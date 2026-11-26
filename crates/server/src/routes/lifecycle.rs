//! Leaving: deleting an account or an organisation, and taking your data with you.
//!
//! - `DELETE /api/me` — body `{current_password?}`. 204. The account is emptied, not removed
//!   (history refers to it): its name becomes `Deleted user`, its address is freed (someone may
//!   register with it again), and its sessions, sign-in providers, memberships and project roles
//!   go; every open document connection of it closes (4401). The work stays: documents, history
//!   and versions belong to their organisations. It takes the current password (or, for an account
//!   that has none, a sign-in from the last ten minutes: 403 otherwise) and is refused with 409
//!   while the account is the only owner of an organisation (`{message, orgs: [{id, name}]}`) or
//!   the server's only admin while other accounts exist.
//! - `GET /api/me/export` — everything the server holds about the caller as a person, as JSON
//!   (their work belongs to their organisations, whose owners export it): `{account,
//!   organisations, projects, sessions, exported_at, versions_you_named, changes_you_made}`.
//!   Tokens are never in it, hashed or not.
//! - `DELETE /api/orgs/{org}` (owner) — body `{current_password?, confirm_name}`. 204. Everything
//!   in the organisation goes: projects, documents, history, versions, invites, and the stored
//!   blobs and snapshots; its open documents close (4404 `the organisation was deleted`). There is
//!   no undo, so it takes the organisation's name typed out (422 if it is not exactly that) as
//!   well as the password. An extension may refuse it
//!   ([`crate::ext::Extensions::before_org_delete`]: 402, or 409 if it does not answer).
//! - `GET /api/orgs/{org}/export` (owner) — a zip of everything in the organisation:
//!   `organisation.json`; for each document that is not deleted
//!   `projects/<project>/<document>/document.json` (the document as it is now, openswx's JSON),
//!   `history.jsonl` (every change, oldest first, one a line, in the shape of
//!   `GET /api/documents/{document}/history`) and `versions.json`; and `blobs/<sha256>` for every
//!   blob the documents or their histories name that storage has.
//!
//! An organisation the caller is not in answers 404 `no such organisation`, exactly as one that
//! does not exist; a member below owner gets 403.
//!
//! # Exports are streamed
//!
//! Both exports are written by a task of their own into a small pipe that the response body reads,
//! so what an export holds at any moment is bounded whatever the organisation holds: one
//! document (its state, then its JSON), one page of history ([`HISTORY_PAGE_BYTES`] of stored
//! operations, [`Exports::new`]'s `page_rows` rows), one chunk of one blob, and the pipe. Nothing
//! is read ahead of the client: when it reads slowly the export waits, and when it reads nothing
//! for [`EXPORT_STALL`] the export is given up. No database transaction or connection is held
//! between reads. At most [`EXPORTS_AT_ONCE`] exports run on a server; one more is answered 429.
//!
//! An export that fails after its answer began cannot change its status: the response is cut
//! short (the connection is closed without the end of the body, so the client sees an error, not
//! a short file) and the failure is logged with the organisation's or account's id.
//!
//! A document is exported at its newest durable change: the state is read from the journal
//! ([`crate::snapshot::state_at`] at the document's `head_seq`), not from its room, so an open
//! document is exported as of the last change the journal made permanent, with that many history
//! lines.

use std::collections::BTreeSet;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::bail;
use async_zip::tokio::write::{EntryStreamWriter, ZipFileWriter};
use async_zip::{Compression, ZipDateTime, ZipDateTimeBuilder, ZipEntryBuilder};
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use chrono::{Datelike, Timelike};
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter, DuplexStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};
use tokio_util::io::ReaderStream;

use crate::access::{CurrentUser, NO_SUCH_ORG, recheck_org, require_org};
use crate::blobs::{Blobs, blob_key, is_sha256};
use crate::content::{blob_refs, json_from_plain, ops_blob_refs};
use crate::error::{ApiError, ApiJson, ApiPath};
use crate::ext::{Event, check_org_delete, emit, notify_member_removed};
use crate::routes::auth::{CHECKED_PASSWORD_MAX, confirm, max_len};
use crate::routes::history::{history_entry_of, rows_within, stored_ops};
use crate::routes::orgs::{NAME_MAX, load_user};
use crate::security::OrgRole;
use crate::snapshot::{LoadError, state_at};
use crate::state::{AppState, Committed};
use crate::store::{Id, Org, User};

/// What a deleted account is called from then on.
pub const DELETED_NAME: &str = "Deleted user";
/// How many exports run at once on a server unless [`Exports`] says otherwise.
pub const EXPORTS_AT_ONCE: usize = 2;
/// How long an export waits for its client to read before it gives up.
pub const EXPORT_STALL: Duration = Duration::from_secs(60);
/// How many rows an export reads from the database at a time.
pub const EXPORT_PAGE_ROWS: i64 = 500;
/// The most stored operation bytes an organisation export reads of a history at a time (a page
/// always holds one change at least; their JSON can be several times larger).
pub const HISTORY_PAGE_BYTES: u64 = 1 << 20;
/// The size of the pipe between an export and its response, and of the buffer before it.
const PIPE_BYTES: usize = 64 << 10;

const ONLY_OWNER: &str = "you are the only owner of these organisations: make someone else an \
     owner, or delete them, first";
const ONLY_ADMIN: &str = "you are this server's only admin; make someone else admin first \
     (openswx-collab-server create-admin)";
const TYPE_THE_NAME: &str = "type the organisation's name exactly to confirm";
const ORG_DELETED: &str = "the organisation was deleted";
const EXPORTS_BUSY: &str = "too many exports are running; try again in a moment";

/// The limits of the exports ([`crate::AppState::exports`]).
#[derive(Debug)]
pub struct Exports {
    permits: Arc<Semaphore>,
    at_once: usize,
    stall: Duration,
    page_rows: i64,
}

impl Exports {
    /// At most `at_once` exports at a time, each given up when its client reads nothing for
    /// `stall`, reading at most `page_rows` rows from the database at a time.
    pub fn new(at_once: usize, stall: Duration, page_rows: i64) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(at_once)),
            at_once,
            stall,
            page_rows: page_rows.max(1),
        }
    }

    /// How many exports are running.
    pub fn running(&self) -> usize {
        self.at_once
            .saturating_sub(self.permits.available_permits())
    }

    /// A place among the running exports, or 429.
    fn place(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        self.permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::Busy(EXPORTS_BUSY))
    }
}

impl Default for Exports {
    fn default() -> Self {
        Self::new(EXPORTS_AT_ONCE, EXPORT_STALL, EXPORT_PAGE_ROWS)
    }
}

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/me", delete(delete_account))
        .route("/api/me/export", get(export_account))
        .route("/api/orgs/{org_id}", delete(delete_org))
        .route("/api/orgs/{org_id}/export", get(export_org))
}

// ------------------------------------------------------------------ your account

#[derive(Debug, Deserialize)]
struct ConfirmIn {
    current_password: Option<String>,
}

/// Deletes the caller's account (see the module documentation). In order: the password (403);
/// then, in one transaction, the account read again (401 if it went meanwhile), the organisations
/// it alone owns (409), the server's last admin (409), and the emptying. After the commit, and
/// also when the commit reported an error (it may have taken effect), its sessions are forgotten
/// and its document connections closed; then the extensions hear of it.
async fn delete_account(
    State(state): State<AppState>,
    current: CurrentUser,
    ApiJson(body): ApiJson<ConfirmIn>,
) -> Result<StatusCode, ApiError> {
    if let Some(password) = &body.current_password {
        max_len("current_password", password, CHECKED_PASSWORD_MAX)?;
    }
    let me = load_user(&state, &current).await?;
    confirm(
        &state,
        &me,
        &current.token_hash,
        body.current_password.as_deref(),
    )
    .await?;

    let mut tx = state.store.begin().await?;
    // TASK 16: lock the account's row and its organisations' rows (FOR UPDATE) before counting
    // owners and admins; SQLite's single writer already serialises this.
    let Some(me) = tx.user_by_id(&current.id).await?.filter(User::active) else {
        return Err(ApiError::Unauthorized);
    };
    // An organisation must not be left without an owner.
    let alone = tx.orgs_owned_alone(&me.id).await?;
    if !alone.is_empty() {
        let orgs: Vec<serde_json::Value> = alone
            .iter()
            .map(|o| json!({"id": o.id, "name": o.name}))
            .collect();
        return Err(ApiError::ConflictWith(
            json!({"message": ONLY_OWNER, "orgs": orgs}),
        ));
    }
    // Nor a server with people on it without an admin (the last person may leave).
    if me.is_server_admin
        && tx.active_admin_count().await? <= 1
        && tx.other_active_user_count(&me.id).await? > 0
    {
        return Err(ApiError::Conflict(ONLY_ADMIN.into()));
    }
    let orgs = tx.user_org_ids(&me.id).await?;
    // (`.invalid` can never be delivered to.)
    let freed = format!("deleted-{}@deleted.invalid", me.id);
    tx.erase_user(&me.id, &freed, DELETED_NAME, state.clock.now())
        .await?;

    let (revoker, revoked) = (state.clone(), me.id.clone());
    let (ext, user) = (state.ext.clone(), me.id.clone());
    state
        .commit_then(
            tx,
            move |committed: Committed| async move {
                let (code, reason) = committed.session_close();
                revoker.sessions.forget_user(&revoked);
                revoker.hub.drop_account_with(&revoked, code, reason).await;
            },
            async move {
                for org in orgs {
                    notify_member_removed(&ext, &org, &user).await;
                    let left = Event::MemberRemoved {
                        org,
                        user: user.clone(),
                        by: user.clone(),
                    };
                    emit(&ext, left).await;
                }
                tracing::info!(user = %user, "account deleted");
                emit(&ext, Event::AccountDeleted { user }).await;
            },
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Everything the server holds about the caller as a person, streamed (see the module
/// documentation): the two lists that grow with use, `versions_you_named` (by id) and
/// `changes_you_made` (by document, then seq), are read a page at a time.
async fn export_account(
    State(state): State<AppState>,
    current: CurrentUser,
) -> Result<Response, ApiError> {
    let me = load_user(&state, &current).await?;
    let place = state.exports.place()?;
    let (writer, id) = (state.clone(), me.id.clone());
    let body = download(&state, place, "account", id, move |pipe| {
        write_account_export(writer, me, pipe)
    });
    Ok(([(header::CONTENT_TYPE, "application/json")], body).into_response())
}

async fn write_account_export(state: AppState, me: User, mut out: Pipe) -> anyhow::Result<()> {
    emit(
        &state.ext,
        Event::AccountExported {
            user: me.id.clone(),
        },
    )
    .await;
    let store = state.store.as_ref();
    let page_rows = state.exports.page_rows;
    let providers: Vec<String> = store
        .user_identities(&me.id)
        .await?
        .into_iter()
        .map(|i| i.provider)
        .collect();
    let organisations: Vec<serde_json::Value> = store
        .user_memberships(&me.id)
        .await?
        .into_iter()
        .map(|o| json!({"id": o.id, "name": o.name, "role": o.role, "since": o.since}))
        .collect();
    let projects: Vec<serde_json::Value> = store
        .user_project_roles(&me.id)
        .await?
        .into_iter()
        .map(|p| json!({"id": p.id, "name": p.name, "org_id": p.org_id, "role": p.role}))
        .collect();
    // (Never the tokens' hashes.)
    let sessions: Vec<serde_json::Value> = store
        .user_sessions(&me.id)
        .await?
        .into_iter()
        .map(|s| json!({"created_at": s.created_at, "expires_at": s.expires_at, "label": s.label}))
        .collect();
    // One JSON object: this part without its closing brace, then the two long lists.
    let mut head = serde_json::to_vec(&json!({
        "account": {
            "id": me.id, "email": me.email, "name": me.name, "created_at": me.created_at,
            "email_verified": me.email_verified, "server_admin": me.is_server_admin,
            "has_password": me.password_hash.is_some(), "sign_in_providers": providers,
        },
        "organisations": organisations,
        "projects": projects,
        "sessions": sessions,
        "exported_at": state.clock.now(),
    }))?;
    head.pop();
    out.write_all(&head).await?;

    out.write_all(b",\"versions_you_named\":[").await?;
    let (mut after, mut first) = (String::new(), true);
    loop {
        let page = store.versions_by_author(&me.id, &after, page_rows).await?;
        let Some(last) = page.last() else { break };
        after.clone_from(&last.id);
        let mut items = Vec::new();
        for v in &page {
            let item = json!({"document_id": v.document_id, "name": v.name, "seq": v.seq,
                              "time": v.created_at});
            push_item(&mut items, &mut first, &item)?;
        }
        out.write_all(&items).await?;
    }

    out.write_all(b"],\"changes_you_made\":[").await?;
    let (mut after, mut first) = ((String::new(), 0i64), true);
    loop {
        let page = store
            .changes_by_author(&me.id, &after.0, after.1, page_rows)
            .await?;
        let Some(last) = page.last() else { break };
        after = (last.document_id.clone(), last.seq);
        let mut items = Vec::new();
        for c in &page {
            let item = json!({"document_id": c.document_id, "seq": c.seq, "time": c.created_at,
                              "command": c.command});
            push_item(&mut items, &mut first, &item)?;
        }
        out.write_all(&items).await?;
    }
    out.write_all(b"]}").await?;
    out.flush().await?;
    out.shutdown().await?;
    Ok(())
}

/// Appends `item` to the JSON list being written in `out`, after a comma unless it is the first.
fn push_item(
    out: &mut Vec<u8>,
    first: &mut bool,
    item: &serde_json::Value,
) -> serde_json::Result<()> {
    if !std::mem::take(first) {
        out.push(b',');
    }
    serde_json::to_writer(out, item)
}

// ------------------------------------------------------------------ organisations

#[derive(Debug, Deserialize)]
struct DeleteOrgIn {
    current_password: Option<String>,
    confirm_name: String,
}

/// Deletes an organisation (see the module documentation). In order: the body's sizes (422); the
/// caller's role (404, 403); the password (403); the name (422); the extension hook, before any
/// transaction (402, 409); then one transaction that reads the role and the name again and
/// deletes the organisation's rows. After the commit, and also when the commit reported an error,
/// every open document of the organisation closes. Only after a commit that took effect, the
/// organisation's blobs and its documents' snapshots are removed from storage (a failure there is
/// logged and leaves files behind: the organisation is gone either way) and the extensions hear
/// of it.
async fn delete_org(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(org_id): ApiPath<String>,
    ApiJson(body): ApiJson<DeleteOrgIn>,
) -> Result<StatusCode, ApiError> {
    if let Some(password) = &body.current_password {
        max_len("current_password", password, CHECKED_PASSWORD_MAX)?;
    }
    max_len("confirm_name", &body.confirm_name, NAME_MAX)?;
    require_org(state.store.as_ref(), &user.id, &org_id, OrgRole::Owner).await?;
    let me = load_user(&state, &user).await?;
    confirm(
        &state,
        &me,
        &user.token_hash,
        body.current_password.as_deref(),
    )
    .await?;
    let named = |org: &Org| body.confirm_name.trim() == org.name;
    let org = state
        .store
        .org_by_id(&org_id)
        .await?
        .ok_or(ApiError::NotFound(NO_SUCH_ORG))?;
    if !named(&org) {
        return Err(ApiError::Unprocessable(TYPE_THE_NAME.into()));
    }
    // Asked before the transaction (never while it holds the writer).
    check_org_delete(&state.ext, &org.id).await?;

    let mut tx = state.store.begin().await?;
    recheck_org(&mut *tx, &user.id, &org.id, OrgRole::Owner).await?;
    let renamed = tx.org_by_id(&org.id).await?.is_none_or(|now| !named(&now));
    if renamed {
        return Err(ApiError::Unprocessable(TYPE_THE_NAME.into()));
    }
    // TASK 16: the cascade locks the organisation's document rows in no particular order; take
    // them in ascending id order first (the lock order rule of `crate::assemblies`).
    let documents = tx.org_document_ids(&org.id).await?;
    let members = tx.org_member_ids(&org.id).await?;
    tx.delete_org(&org.id).await?;

    let (revoker, open) = (state.clone(), documents.clone());
    let (after, org_id, by) = (state.clone(), org.id, user.id);
    state
        .commit_then(
            tx,
            |_| async move {
                for document in &open {
                    revoker.hub.close_document(document, ORG_DELETED).await;
                }
            },
            async move {
                remove_org_files(after.blobs.as_ref(), &org_id, &documents).await;
                for member in &members {
                    notify_member_removed(&after.ext, &org_id, member).await;
                }
                tracing::info!(org = %org_id, "organisation deleted");
                let deleted = Event::OrgDeleted { org: org_id, by };
                emit(&after.ext, deleted).await;
            },
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Removes what a deleted organisation left in storage: its blobs (`orgs/<org>/`; blobs are kept
/// per organisation, so no other organisation's are touched) and the snapshots of its documents
/// (`docs/<document>/`, which also takes snapshots whose rows were never written). Failures are
/// logged, with ids and storage keys only; the files they leave are wasted space, nothing more.
async fn remove_org_files(blobs: &dyn Blobs, org: &str, documents: &[Id]) {
    let prefixes = documents
        .iter()
        .map(|d| format!("docs/{d}/"))
        .chain([format!("orgs/{org}/")]);
    let mut failed = 0usize;
    for prefix in prefixes {
        if let Err(e) = blobs.delete_prefix(&prefix).await {
            if failed == 0 {
                tracing::error!(org, "could not remove {prefix} from storage: {e:#}");
            }
            failed += 1;
        }
    }
    if failed > 0 {
        tracing::error!(
            org,
            "could not remove all stored files of the deleted organisation: {failed} of {} \
             removals failed",
            documents.len() + 1
        );
    }
}

/// A zip of everything in the organisation, streamed (see the module documentation).
async fn export_org(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(org_id): ApiPath<String>,
) -> Result<Response, ApiError> {
    require_org(state.store.as_ref(), &user.id, &org_id, OrgRole::Owner).await?;
    let org = state
        .store
        .org_by_id(&org_id)
        .await?
        .ok_or(ApiError::NotFound(NO_SUCH_ORG))?;
    let place = state.exports.place()?;
    let disposition = HeaderValue::from_str(&content_disposition(&org.name))?;
    let (writer, id, by) = (state.clone(), org.id.clone(), user.id);
    let body = download(&state, place, "organisation", id, move |pipe| {
        write_org_export(writer, org, by, pipe)
    });
    Ok((
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/zip"),
            ),
            (header::CONTENT_DISPOSITION, disposition),
        ],
        body,
    )
        .into_response())
}

/// `attachment` with the file name the Python server gave an export: the organisation's name with
/// every character but letters, digits, `-` and `_` replaced by `-`, cut to 60 characters (or
/// `organisation`), then `-export.zip`; a name that is not ASCII goes in the `filename*` form.
fn content_disposition(org_name: &str) -> String {
    let safe: String = org_name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .take(60)
        .collect();
    let safe = if safe.is_empty() {
        "organisation"
    } else {
        &safe
    };
    let name = format!("{safe}-export.zip");
    if name.is_ascii() {
        return format!("attachment; filename=\"{name}\"");
    }
    let mut quoted = String::new();
    for b in name.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.') {
            quoted.push(char::from(b));
        } else {
            quoted.push_str(&format!("%{b:02X}"));
        }
    }
    format!("attachment; filename*=utf-8''{quoted}")
}

async fn write_org_export(state: AppState, org: Org, by: Id, pipe: Pipe) -> anyhow::Result<()> {
    let exported = Event::OrgExported {
        org: org.id.clone(),
        by,
    };
    emit(&state.ext, exported).await;
    let store = state.store.as_ref();
    let now = state.clock.now();
    let mut archive = Archive::new(pipe, now);

    let members: Vec<serde_json::Value> = store
        .org_members(&org.id)
        .await?
        .into_iter()
        .map(|m| json!({"user": m.name, "email": m.email, "role": m.role}))
        .collect();
    let projects = store.projects_in_org(&org.id).await?;
    let listed: Vec<serde_json::Value> = projects
        .iter()
        .map(|p| json!({"id": p.id, "name": p.name}))
        .collect();
    let about = pretty(&json!({
        "id": org.id, "name": org.name, "created_at": org.created_at, "members": members,
        "exported_at": now, "projects": listed,
    }))?;
    archive.put("organisation.json".to_owned(), &about).await?;
    drop(about);

    // Every blob a document or its history names, in order.
    let mut named = BTreeSet::new();
    for project in &projects {
        for document in store.documents_in_project(&project.id).await? {
            export_document(&state, &mut archive, &project.id, &document.id, &mut named).await?;
        }
    }
    for sha in named.iter().filter(|sha| is_sha256(sha)) {
        // (A blob storage does not have is left out, as the Python server left it out.)
        let Some((_, mut chunks)) = state.blobs.get_stream(&blob_key(&org.id, sha)).await? else {
            continue;
        };
        let mut entry = archive.entry(format!("blobs/{sha}")).await?;
        while let Some(chunk) = chunks.next().await {
            entry.write_all(&chunk?).await?;
        }
        close_entry(entry).await?;
    }
    archive.finish().await
}

/// Writes one document's three entries, and adds the blobs it and its history name to `named`. A
/// document that went since it was listed is left out.
async fn export_document(
    state: &AppState,
    archive: &mut Archive,
    project: &str,
    document: &str,
    named: &mut BTreeSet<String>,
) -> anyhow::Result<()> {
    let store = state.store.as_ref();
    // Read now, not when it was listed: its head is its newest durable change.
    let Some(doc) = store
        .document_by_id(document)
        .await?
        .filter(|d| d.deleted_at.is_none())
    else {
        return Ok(());
    };
    let head = doc.head_seq;
    let content = match state_at(store, state.blobs.as_ref(), &doc.id, head).await {
        Ok(content) => content,
        Err(LoadError::NoDocument) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let base = format!("projects/{project}/{}", doc.id);
    let about = json!({"id": doc.id, "name": doc.name, "kind": doc.kind, "seq": head,
                       "format_version": doc.format_version});
    // Rendered on the blocking pool, one form of the document dropped as the next is made.
    let (bytes, refs) = tokio::task::spawn_blocking(move || {
        let refs = blob_refs(&content);
        let plain = content.to_plain();
        drop(content);
        let rendered = json_from_plain(&plain);
        drop(plain);
        let mut about = about;
        about["content"] = rendered;
        pretty(&about).map(|bytes| (bytes, refs))
    })
    .await??;
    named.extend(refs);
    archive.put(format!("{base}/document.json"), &bytes).await?;
    drop(bytes);

    // The history up to that head, oldest first, a page at a time.
    let page_rows = state.exports.page_rows;
    let mut entry = archive.entry(format!("{base}/history.jsonl")).await?;
    let mut after = 0i64;
    while after < head {
        let upto = after.saturating_add(page_rows).min(head);
        // (The sizes first, without the operations, so that no more than the budget is loaded.)
        let mut sizes = store
            .change_sizes(&doc.id, upto + 1, after, page_rows)
            .await?;
        sizes.reverse();
        let n = rows_within(&sizes, HISTORY_PAGE_BYTES);
        let Some(last) = n.checked_sub(1).and_then(|i| sizes.get(i)).map(|s| s.seq) else {
            bail!(
                "the journal of document {} has no change after {after} (its head is {head})",
                doc.id
            );
        };
        let mut rows = store
            .history(&doc.id, last + 1, after, i64::try_from(n)?)
            .await?;
        rows.reverse();
        let id = doc.id.clone();
        let (lines, refs) = tokio::task::spawn_blocking(move || {
            let (mut lines, mut refs) = (Vec::new(), BTreeSet::new());
            for row in rows {
                let ops = stored_ops(&id, &row);
                if let Some(ops) = &ops {
                    refs.extend(ops_blob_refs(ops));
                }
                serde_json::to_writer(&mut lines, &history_entry_of(row, ops.as_deref()))?;
                lines.push(b'\n');
            }
            serde_json::Result::Ok((lines, refs))
        })
        .await??;
        named.extend(refs);
        entry.write_all(&lines).await?;
        after = last;
    }
    close_entry(entry).await?;

    let versions: Vec<serde_json::Value> = store
        .versions(&doc.id)
        .await?
        .into_iter()
        .map(|v| {
            json!({"name": v.name, "seq": v.seq, "note": v.note, "author": v.author_id,
                   "author_name": v.author_name.unwrap_or_default(), "time": v.created_at})
        })
        .collect();
    archive
        .put(format!("{base}/versions.json"), &pretty(&versions)?)
        .await
}

/// JSON as the Python server's export wrote it: indented by one space, not escaped to ASCII.
fn pretty(value: &impl Serialize) -> serde_json::Result<Vec<u8>> {
    let mut out = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b" ");
    value.serialize(&mut serde_json::Serializer::with_formatter(
        &mut out, formatter,
    ))?;
    Ok(out)
}

// ------------------------------------------------------------------ the zip being written

/// An entry of the zip being written.
type Entry<'a> = Compat<EntryStreamWriter<'a, Pipe>>;

/// A zip written entry after entry into an export's pipe. Every entry is deflated and streamed
/// (its sizes follow its data, in ZIP64 form), so neither an entry nor the archive has a size
/// limit and nothing is held but the entry's current chunk.
struct Archive {
    zip: ZipFileWriter<Pipe>,
    /// When the export was made: the date of every entry.
    date: ZipDateTime,
}

impl Archive {
    fn new(pipe: Pipe, now: f64) -> Self {
        Self {
            zip: ZipFileWriter::with_tokio(pipe),
            date: zip_date(now),
        }
    }

    /// Starts the entry `name`; write its content, then [`close_entry`] it.
    async fn entry(&mut self, name: String) -> anyhow::Result<Entry<'_>> {
        let entry = ZipEntryBuilder::new(name.into(), Compression::Deflate)
            .last_modification_date(self.date);
        Ok(self.zip.write_entry_stream(entry).await?.compat_write())
    }

    /// Writes the entry `name` with `content`.
    async fn put(&mut self, name: String, content: &[u8]) -> anyhow::Result<()> {
        let mut entry = self.entry(name).await?;
        entry.write_all(content).await?;
        close_entry(entry).await
    }

    /// Writes the directory and ends the pipe.
    async fn finish(self) -> anyhow::Result<()> {
        let mut pipe = self.zip.close().await?.into_inner();
        pipe.flush().await?;
        pipe.shutdown().await?;
        Ok(())
    }
}

/// Ends an entry: what is left of its data, then its checksum and sizes.
async fn close_entry(entry: Entry<'_>) -> anyhow::Result<()> {
    Ok(entry.into_inner().close().await?)
}

/// The zip date of `now` (epoch seconds, UTC); the zip epoch for a time a zip cannot say.
fn zip_date(now: f64) -> ZipDateTime {
    // (Whole seconds; a time out of range is no time.)
    let utc = chrono::DateTime::from_timestamp(now as i64, 0);
    match utc.filter(|t| (1980..=2107).contains(&t.year())) {
        Some(t) => ZipDateTimeBuilder::new()
            .year(t.year())
            .month(t.month())
            .day(t.day())
            .hour(t.hour())
            .minute(t.minute())
            .second(t.second())
            .build(),
        None => ZipDateTime::default(),
    }
}

// ------------------------------------------------------------------ the pipe to the response

/// What a running export writes into: a buffer, then the pipe its response body reads from.
type Pipe = BufWriter<Patient<DuplexStream>>;

/// The client read nothing for as long as an export waits.
#[derive(Debug, thiserror::Error)]
#[error("the client stopped reading")]
struct Stalled;

/// A writer that gives up ([`Stalled`], as an `io::Error` of kind `TimedOut`) when one write has
/// waited `patience` for room: whoever reads the other end has stopped.
struct Patient<W> {
    inner: W,
    patience: Duration,
    /// Running while a write waits for room.
    waiting: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for Patient<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(done) => {
                this.waiting = None;
                Poll::Ready(done)
            }
            Poll::Pending => {
                let patience = this.patience;
                let waiting = this
                    .waiting
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(patience)));
                match waiting.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        this.waiting = None;
                        Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, Stalled)))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// An export's task; dropping this (the response went away) stops it.
struct Running(JoinHandle<bool>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The body of a streamed download: `produce` runs in a task of its own, holding `place`, and
/// writes into a pipe of [`PIPE_BYTES`] that the body reads, so it never runs ahead of the client
/// by more than the pipe and its buffer. It must end the pipe (`shutdown`) when it is done. If it
/// fails (logged here, as `what` `id`) or stops, the body ends with an error, which closes the
/// connection without the end of the response. When the response is dropped, the task is stopped.
fn download<F, Fut>(
    state: &AppState,
    place: OwnedSemaphorePermit,
    what: &'static str,
    id: Id,
    produce: F,
) -> Body
where
    F: FnOnce(Pipe) -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let (into, from) = tokio::io::duplex(PIPE_BYTES);
    let patient = Patient {
        inner: into,
        patience: state.exports.stall,
        waiting: None,
    };
    let pipe = BufWriter::with_capacity(PIPE_BYTES, patient);
    let task = tokio::spawn(async move {
        // (Declared first, dropped last: the place is free once everything else is let go.)
        let _place = place;
        match produce(pipe).await {
            Ok(()) => true,
            Err(e) => {
                log_failure(what, &id, &e);
                false
            }
        }
    });
    let chunks = ReaderStream::with_capacity(from, PIPE_BYTES);
    Body::from_stream(stream::unfold(
        Some((chunks, Running(task))),
        |reading| async move {
            let (mut chunks, mut running) = reading?;
            match chunks.next().await {
                Some(Ok(chunk)) => Some((Ok(chunk), Some((chunks, running)))),
                Some(Err(e)) => Some((Err(e), None)),
                // The pipe ended: whole only if the task says so.
                None => match (&mut running.0).await {
                    Ok(true) => None,
                    _ => Some((Err(io::Error::other("the export was cut short")), None)),
                },
            }
        },
    ))
}

/// Logs why an export stopped: a client that went away is no error, one that stopped reading a
/// warning, anything else an error. Only the id is named, never a name or an address.
fn log_failure(what: &str, id: &str, e: &anyhow::Error) {
    let io = e.chain().find_map(|c| c.downcast_ref::<io::Error>());
    let stalled = io.is_some_and(|e| e.get_ref().is_some_and(|inner| inner.is::<Stalled>()));
    if stalled {
        tracing::warn!("the export of {what} {id} was given up: its client stopped reading");
    } else if io.is_some_and(|e| e.kind() == io::ErrorKind::BrokenPipe) {
        tracing::debug!("the export of {what} {id} ended early: its client went away");
    } else {
        tracing::error!("the export of {what} {id} failed: {e:#}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_download_name_is_safe_in_a_header() {
        assert_eq!(
            content_disposition("Robotics"),
            "attachment; filename=\"Robotics-export.zip\""
        );
        assert_eq!(
            content_disposition("a b/c\\d\"e\r\nf;g=h"),
            "attachment; filename=\"a-b-c-d-e--f-g-h-export.zip\""
        );
        assert_eq!(
            content_disposition(&"x".repeat(500)),
            format!("attachment; filename=\"{}-export.zip\"", "x".repeat(60))
        );
        assert_eq!(
            content_disposition("Stra\u{df}e \u{4e2d}"),
            "attachment; filename*=utf-8''Stra%C3%9Fe-%E4%B8%AD-export.zip"
        );
        assert_eq!(
            content_disposition(""),
            "attachment; filename=\"organisation-export.zip\""
        );
    }

    #[test]
    fn a_zip_date_is_the_time_in_utc_or_the_zip_epoch() {
        // 2026-11-26 12:34:56 UTC.
        let at = zip_date(1_795_696_496.0);
        assert_eq!(
            (
                at.year(),
                at.month(),
                at.day(),
                at.hour(),
                at.minute(),
                at.second()
            ),
            (2026, 11, 26, 12, 34, 56)
        );
        for odd in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e18] {
            let at = zip_date(odd);
            assert_eq!((at.year(), at.month(), at.day()), (1980, 1, 1), "{odd}");
        }
    }

    #[tokio::test]
    async fn a_writer_nobody_reads_from_gives_up_and_one_that_is_read_does_not() {
        use tokio::io::AsyncReadExt;
        let (into, mut from) = tokio::io::duplex(16);
        let mut patient = Patient {
            inner: into,
            patience: Duration::from_millis(50),
            waiting: None,
        };
        // Slow, but read: each write waits less than the patience.
        let reader = tokio::spawn(async move {
            let mut got = Vec::new();
            let mut buf = [0u8; 16];
            for _ in 0..4 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let n = from.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
            }
            (from, got)
        });
        patient.write_all(&[7u8; 64]).await.unwrap();
        let (from, got) = reader.await.unwrap();
        assert_eq!(got, [7u8; 64]);
        // Not read at all: the first write fills the pipe, the next gives up.
        patient.write_all(&[8u8; 16]).await.unwrap();
        let e = patient.write_all(&[9u8; 16]).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
        assert!(e.get_ref().unwrap().is::<Stalled>());
        drop(from);
    }
}
