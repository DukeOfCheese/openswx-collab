//! A document's history: its changes, named versions, earlier states, and restoring one.
//!
//! - `GET /api/documents/{document}/history?before=&after=&limit=` (viewer): the changes with
//!   `after < seq < before` (both optional), newest first, at most `limit` (default
//!   [`HISTORY_LIMIT`], clamped to `1..=`[`HISTORY_MAX`]; a value that is not an integer is 422)
//!   and at most [`crate::room::PAGE_BYTES`] of stored operations (always one row at least). The
//!   answer is a plain list, as in the Python server: a client pages on with `before=<the last
//!   seq it got>` until it gets an empty page.
//!   Each: `{seq, author, author_name, client_id, change_id, time, ops, command, reverts,
//!   reverted_by, broke}` — `author` the user id, `client_id` and `change_id` `null` for a change
//!   the server made (a restore, a revert from history), `ops` as [`crate::content::ops_json`]
//!   renders them, `broke` what rebuild reports blamed on it (`{feature id: error}`) or `null`.
//! - `GET /api/documents/{document}/versions` (viewer): `[{id, name, note, seq, created_at,
//!   author: {id, name}}]`, newest seq first.
//! - `POST /api/documents/{document}/versions` (editor): `{name, note?, seq?}` — `name` 1 to 200
//!   characters (trimmed), unique in the document (409); `note` at most 4000; `seq` defaults to the
//!   head and must be within `0..=head` (422). 201 with the version. An open room hears
//!   `version.created`; open assemblies with components pinned to an older state of the document
//!   hear `reference.update`.
//! - `DELETE /api/documents/{document}/versions/{version}` (project owner, as in the Python
//!   server). 204; 404 `no such version`. A version assemblies pin may go: they pin its seq.
//! - `POST /api/documents/{document}/restore` (editor): `{seq}` or `{version}` (a name; it wins
//!   over `seq`; 422 when neither is given). The document becomes, as one new change, what it was
//!   at that state: the target goes to the room ([`crate::room::RoomHandle::restore`]), which works
//!   out `oswx_core::diff(head, target)` against the head it applies it to, so the document at the
//!   answered seq is exactly the target (changes tentative before the restore was judged are
//!   overwritten; later ones apply on top). It is a change the server makes for the caller
//!   (`restore to <seq or version name>`), with no staleness check, refused (409 `{code: "locked",
//!   message, conflicts}`) while anyone but the caller holds a lock. `{seq}` once durable (the
//!   head's seq when nothing differs). Works whether or not the document is open (it opens the
//!   room), and alongside live editing.
//! - `GET /api/documents/{document}/content?seq=N` or `?version=<name>` (viewer; in
//!   `routes::documents`): the document at that state, read with [`crate::snapshot::state_at`].
//!
//! A state that does not exist (below 0, beyond the head) is 404 `the document has no state N`; a
//! version name that does not exist 404 `no version named 'x'`. A document the caller cannot see is
//! 404 `no such document`, as everywhere.

use std::collections::BTreeMap;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use oswx_core::{Conflict, ConflictKind, Refusal, Value};
use serde::Deserialize;
use serde_json::json;

use crate::access::{CurrentUser, recheck_document, require_document};
use crate::content::ops_json;
use crate::error::{ApiError, ApiJson, ApiPath, ApiQuery};
use crate::ext::{Event, emit};
use crate::hub::OpenError;
use crate::record::decode_ops;
use crate::room::{Landed, LockRule, ServerChangeError, ServerChangeOptions};
use crate::routes::orgs::clean_name;
use crate::security::ProjectRole;
use crate::snapshot::{LoadError, state_at};
use crate::state::AppState;
use crate::store::{ChangeSize, Document, HistoryRow, Version, new_id};

/// How many changes a history page has unless `limit` says otherwise.
pub const HISTORY_LIMIT: i64 = 100;
/// The most changes a history page has.
pub const HISTORY_MAX: i64 = 500;
/// The longest note of a version, in characters.
pub const NOTE_MAX: usize = 4000;
/// The longest version name a request may give, in characters.
pub const VERSION_NAME_MAX: usize = 200;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/documents/{document_id}/history", get(history))
        .route(
            "/api/documents/{document_id}/versions",
            get(list_versions).post(create_version),
        )
        .route(
            "/api/documents/{document_id}/versions/{version_id}",
            delete(delete_version),
        )
        .route("/api/documents/{document_id}/restore", post(restore))
}

/// An integer query parameter (422 if it is not one).
fn int_param(name: &str, value: Option<&str>) -> Result<Option<i64>, ApiError> {
    value
        .map(|v| {
            v.trim()
                .parse::<i64>()
                .map_err(|_| ApiError::Unprocessable(format!("{name} must be an integer")))
        })
        .transpose()
}

/// A version name a request gives (422 over [`VERSION_NAME_MAX`] characters).
fn version_name(name: &str) -> Result<&str, ApiError> {
    if name.chars().count() > VERSION_NAME_MAX {
        return Err(ApiError::Unprocessable(format!(
            "version must be at most {VERSION_NAME_MAX} characters"
        )));
    }
    Ok(name)
}

/// 404 for a state the document does not have.
fn no_state(seq: i64) -> ApiError {
    ApiError::NotFoundDetail(format!("the document has no state {seq}"))
}

/// The seq of the version `name` of `doc` (404 `no version named 'x'`); the name is trimmed, as
/// it was when the version was made.
async fn version_seq(state: &AppState, doc: &str, name: &str) -> Result<i64, ApiError> {
    let name = version_name(name)?.trim();
    match state.store.version_by_name(doc, name).await? {
        Some(v) => Ok(v.seq),
        None => Err(ApiError::NotFoundDetail(format!(
            "no version named '{name}'"
        ))),
    }
}

/// The state `?seq=` / `?version=` ask for (`version` wins), or `None` for the head.
pub(crate) async fn requested_seq(
    state: &AppState,
    doc: &Document,
    seq: Option<&str>,
    version: Option<&str>,
) -> Result<Option<i64>, ApiError> {
    if let Some(name) = version {
        return Ok(Some(version_seq(state, &doc.id, name).await?));
    }
    int_param("seq", seq)
}

/// The document at `seq`, JSON-ready content (404 when it has no such state).
pub(crate) async fn content_at(
    state: &AppState,
    doc: &Document,
    seq: i64,
) -> Result<Value, ApiError> {
    match state_at(state.store.as_ref(), state.blobs.as_ref(), &doc.id, seq).await {
        Ok(v) => Ok(v),
        Err(LoadError::NoState(_) | LoadError::NoDocument) => Err(no_state(seq)),
        Err(e) => Err(ApiError::Internal(e.into())),
    }
}

// ------------------------------------------------------------------ history

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    before: Option<String>,
    after: Option<String>,
    limit: Option<String>,
}

/// One history entry.
fn history_entry(doc: &str, row: HistoryRow) -> serde_json::Value {
    let ops = match decode_ops(&row.ops) {
        Ok(ops) => ops_json(&ops),
        Err(e) => {
            tracing::error!("change {} of document {doc} is damaged: {e}", row.seq);
            serde_json::Value::Null
        }
    };
    let broke = row
        .broke
        .as_deref()
        .and_then(|b| serde_json::from_str::<serde_json::Value>(b).ok());
    json!({
        "seq": row.seq, "author": row.author_id, "author_name": row.author_name.unwrap_or_default(),
        "client_id": row.client_id, "change_id": row.change_id, "time": row.created_at,
        "ops": ops, "command": row.command, "reverts": row.reverts,
        "reverted_by": row.reverted_by, "broke": broke,
    })
}

async fn history(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(document_id): ApiPath<String>,
    ApiQuery(query): ApiQuery<HistoryQuery>,
) -> Result<Response, ApiError> {
    let before = int_param("before", query.before.as_deref())?;
    let after = int_param("after", query.after.as_deref())?;
    let limit = int_param("limit", query.limit.as_deref())?
        .unwrap_or(HISTORY_LIMIT)
        .clamp(1, HISTORY_MAX);
    let (doc, _, _) = require_document(
        state.store.as_ref(),
        &user.id,
        &document_id,
        ProjectRole::Viewer,
    )
    .await?;
    // The page stops before the row that would pass the byte budget (one row at least); the sizes
    // are read first, without the operations, so no more than the budget is ever loaded. The
    // budget counts stored (MessagePack) operation bytes: the JSON rendering can be several times
    // larger.
    let sizes = state
        .store
        .change_sizes(
            &doc.id,
            before.unwrap_or(i64::MAX),
            after.unwrap_or(i64::MIN),
            limit,
        )
        .await?;
    let n = rows_within(&sizes, state.room_hooks.page_bytes);
    let rows = match (sizes.first(), n.checked_sub(1).map(|i| &sizes[i])) {
        (Some(top), Some(bottom)) => {
            state
                .store
                .history(&doc.id, top.seq + 1, bottom.seq - 1, i64::try_from(n)?)
                .await?
        }
        _ => Vec::new(),
    };
    // Built and written on the blocking pool.
    let doc_id = doc.id;
    let body = tokio::task::spawn_blocking(move || {
        let page: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|r| history_entry(&doc_id, r))
            .collect();
        serde_json::to_vec(&page)
    })
    .await??;
    Ok((
        [(header::CONTENT_TYPE, "application/json")],
        Body::from(body),
    )
        .into_response())
}

/// How many of `sizes` (newest first) one page takes: rows while their stored operations stay
/// within `budget` bytes, and always the first.
pub(crate) fn rows_within(sizes: &[ChangeSize], budget: u64) -> usize {
    let mut total = 0u64;
    let mut n = 0;
    for s in sizes {
        let size = u64::try_from(s.size).unwrap_or(u64::MAX);
        if n > 0 && total.saturating_add(size) > budget {
            break;
        }
        total = total.saturating_add(size);
        n += 1;
    }
    n
}

// ------------------------------------------------------------------ versions

fn version_out(v: &Version, author_name: &str) -> serde_json::Value {
    json!({
        "id": v.id, "name": v.name, "note": v.note, "seq": v.seq, "created_at": v.created_at,
        "author": {"id": v.author_id, "name": author_name},
    })
}

async fn list_versions(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(document_id): ApiPath<String>,
) -> Result<ApiJson<Vec<serde_json::Value>>, ApiError> {
    let (doc, _, _) = require_document(
        state.store.as_ref(),
        &user.id,
        &document_id,
        ProjectRole::Viewer,
    )
    .await?;
    let rows = state.store.versions(&doc.id).await?;
    Ok(ApiJson(
        rows.into_iter()
            .map(|v| {
                json!({
                    "id": v.id, "name": v.name, "note": v.note, "seq": v.seq,
                    "created_at": v.created_at,
                    "author": {"id": v.author_id, "name": v.author_name.unwrap_or_default()},
                })
            })
            .collect(),
    ))
}

#[derive(Debug, Deserialize)]
struct VersionIn {
    name: String,
    #[serde(default)]
    note: String,
    seq: Option<i64>,
}

/// Names a state: the version is a label on `seq` (the head by default). In order: the caller must
/// be an editor; the name (1 to 200 characters, trimmed) and note (at most 4000) are checked
/// (422); inside the transaction the caller's role is read again and `seq` checked against the
/// head (422); a name the document already has is 409. After the commit an open room hears
/// `version.created`.
async fn create_version(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(document_id): ApiPath<String>,
    ApiJson(body): ApiJson<VersionIn>,
) -> Result<(StatusCode, ApiJson<serde_json::Value>), ApiError> {
    let (doc, _, _) = require_document(
        state.store.as_ref(),
        &user.id,
        &document_id,
        ProjectRole::Editor,
    )
    .await?;
    let name = clean_name(&body.name)?;
    if body.note.chars().count() > NOTE_MAX {
        return Err(ApiError::Unprocessable(format!(
            "note must be at most {NOTE_MAX} characters"
        )));
    }
    let author_name = state
        .store
        .user_by_id(&user.id)
        .await?
        .map(|u| u.name)
        .unwrap_or_default();
    let mut tx = state.store.begin().await?;
    let (doc, project, _) =
        recheck_document(&mut *tx, &user.id, &doc.id, ProjectRole::Editor).await?;
    let seq = body.seq.unwrap_or(doc.head_seq);
    if !(0..=doc.head_seq).contains(&seq) {
        return Err(ApiError::Unprocessable(format!(
            "the document has no state {seq}"
        )));
    }
    let version = Version {
        id: new_id(),
        document_id: doc.id.clone(),
        seq,
        name,
        note: body.note,
        author_id: user.id.clone(),
        created_at: state.clock.now(),
    };
    if !tx.insert_version(&version).await? {
        return Err(ApiError::Conflict(format!(
            "there is already a version named '{}'",
            version.name
        )));
    }
    // After the commit, run to the end even if the client goes away: the hub, then the event.
    let created = oswx_core::wire::ServerMsg::VersionCreated {
        id: version.id.clone(),
        name: version.name.clone(),
        seq: u64::try_from(seq).unwrap_or_default(),
    };
    let event = Event::VersionCreated {
        org: project.org_id,
        project: project.id,
        document: doc.id.clone(),
        version: version.id.clone(),
        by: user.id,
    };
    let after = state.clone();
    state
        .commit_then(tx, |_| async {}, async move {
            // (The assemblies first: that step reads the store, so that once the document's own
            // room has heard of the version, only the event is left.)
            announce_newer(&after, &doc.id, seq).await;
            after.hub.notify(&doc.id, created).await;
            emit(&after.ext, event).await;
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        ApiJson(version_out(&version, &author_name)),
    ))
}

/// Tells every open assembly with components pinned to a state of `part` before `seq` (its new
/// named version) `reference.update {document, components}`: the part's id and those components'
/// ids, nothing more (not everyone in the assembly may open the part; `GET .../references` says
/// the rest to those who can). Rooms that are not open are not opened. A store that cannot answer
/// is logged: the version exists all the same.
async fn announce_newer(state: &AppState, part: &str, seq: i64) {
    let pinned = match state.store.pinned_before(part, seq).await {
        Ok(pinned) => pinned,
        Err(e) => {
            tracing::warn!("could not find the assemblies that use document {part}: {e:#}");
            return;
        }
    };
    let mut by_assembly: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for p in pinned {
        by_assembly
            .entry(p.assembly_id)
            .or_default()
            .push(p.component_id);
    }
    for (assembly, components) in by_assembly {
        let msg = oswx_core::wire::ServerMsg::ReferenceUpdate {
            document: part.to_owned(),
            components,
        };
        state.hub.notify(&assembly, msg).await;
    }
}

async fn delete_version(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath((document_id, version_id)): ApiPath<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let (doc, _, _) = require_document(
        state.store.as_ref(),
        &user.id,
        &document_id,
        ProjectRole::Owner,
    )
    .await?;
    let mut tx = state.store.begin().await?;
    let (doc, project, _) =
        recheck_document(&mut *tx, &user.id, &doc.id, ProjectRole::Owner).await?;
    // A version assemblies pin may go, as in the Python server: a component is pinned to a seq,
    // which never changes; the version name in its `ref` is a label kept for display.
    if !tx.delete_version(&doc.id, &version_id).await? {
        return Err(ApiError::NotFound("no such version"));
    }
    let event = Event::VersionDeleted {
        org: project.org_id,
        project: project.id,
        document: doc.id,
        version: version_id,
        by: user.id,
    };
    let ext = state.ext.clone();
    state
        .commit_then(tx, |_| async {}, async move { emit(&ext, event).await })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ------------------------------------------------------------------ restore

#[derive(Debug, Deserialize)]
struct RestoreIn {
    seq: Option<i64>,
    version: Option<String>,
}

/// What a 409 says about the conflicts that refused a change.
fn conflicts_json(conflicts: &[Conflict]) -> serde_json::Value {
    conflicts
        .iter()
        .map(|c| {
            let kind = match c.kind {
                ConflictKind::Written => "written",
                ConflictKind::Removed => "removed",
                ConflictKind::Read => "read",
                ConflictKind::Locked => "locked",
            };
            json!({"path": c.path.iter().map(|s| &**s).collect::<Vec<_>>(), "seq": c.seq,
                   "by": &*c.by, "kind": kind})
        })
        .collect()
}

/// The 409 for a refused restore.
fn refused(code: &str, message: &str, conflicts: &[Conflict]) -> ApiError {
    ApiError::ConflictWith(
        json!({"code": code, "message": message, "conflicts": conflicts_json(conflicts)}),
    )
}

/// What a change the pipeline refused says about itself.
fn refusal_message(r: &Refusal) -> String {
    match r {
        Refusal::Stale(_) => "someone else changed this meanwhile".to_owned(),
        Refusal::Dependency(id) => format!("it builds on {id}, which was refused"),
        Refusal::NameTaken(name) => format!("the name {name} is taken"),
        Refusal::IdTaken(id) => format!("the id {id} is taken"),
        Refusal::Invalid(why) => why.clone(),
        Refusal::TooOld => "the state is older than the history this server keeps".to_owned(),
    }
}

async fn restore(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(document_id): ApiPath<String>,
    ApiJson(body): ApiJson<RestoreIn>,
) -> Result<ApiJson<serde_json::Value>, ApiError> {
    // The role is checked once, here: the write happens in the room's journal, so there is no REST
    // transaction to read it again in.
    let (doc, project, _) = require_document(
        state.store.as_ref(),
        &user.id,
        &document_id,
        ProjectRole::Editor,
    )
    .await?;
    let (seq, label) = match (&body.version, body.seq) {
        (Some(name), _) => (version_seq(&state, &doc.id, name).await?, name.clone()),
        (None, Some(seq)) => (seq, seq.to_string()),
        (None, None) => return Err(ApiError::Unprocessable("give seq or version".into())),
    };
    let author_name = state
        .store
        .user_by_id(&user.id)
        .await?
        .map(|u| u.name)
        .unwrap_or_default();
    let command = format!("restore to {label}");
    // A room that unloads while the restore waits is opened again, once.
    for attempt in 0..2 {
        let room = match state.hub.open(&state, &doc.id).await {
            Ok(room) => room,
            Err(OpenError::NoDocument) => {
                return Err(ApiError::NotFound(crate::access::NO_SUCH_DOCUMENT));
            }
            Err(e) => return Err(ApiError::Internal(e.into())),
        };
        let Some(head) = room.durable_head().await else {
            continue;
        };
        let head_seq = i64::try_from(head.seq)?;
        if !(0..=head_seq).contains(&seq) {
            return Err(no_state(seq));
        }
        let target = content_at(&state, &doc, seq).await?;
        let options = ServerChangeOptions {
            check_stale: false,
            locks: LockRule::AnyOfOtherUsers,
        };
        // The room works out the operations against the head it applies them to.
        match room
            .restore(target, &command, &user.id, &author_name, options)
            .await
        {
            // Nothing differed: the document was the target as of this durable seq; nothing was
            // written, so there is no event. (The answer has the same shape either way.)
            Ok(Landed::Unchanged(seq)) => return Ok(ApiJson(json!({"seq": seq}))),
            Ok(Landed::Written(new_seq)) => {
                // After the durable answer, run to the end even if the client goes away.
                let event = Event::DocumentRestored {
                    org: project.org_id,
                    project: project.id,
                    document: doc.id,
                    seq: new_seq,
                    by: user.id,
                };
                let ext = state.ext.clone();
                state
                    .after_commit(async move { emit(&ext, event).await })
                    .await;
                return Ok(ApiJson(json!({"seq": new_seq})));
            }
            Err(ServerChangeError::Locked(conflicts)) => {
                return Err(refused(
                    "locked",
                    "other people are editing this document; restore needs their locks released",
                    &conflicts,
                ));
            }
            Err(ServerChangeError::Refused(r)) => {
                let conflicts = match &r {
                    Refusal::Stale(c) => c.clone(),
                    _ => Vec::new(),
                };
                return Err(refused(r.code().as_str(), &refusal_message(&r), &conflicts));
            }
            Err(ServerChangeError::NotWritten) if attempt == 0 && room.is_closed() => continue,
            Err(ServerChangeError::NotWritten) => {
                return Err(ApiError::Internal(anyhow::anyhow!(
                    "the restore of document {} was not written",
                    doc.id
                )));
            }
        }
    }
    Err(ApiError::Internal(anyhow::anyhow!(
        "the room of document {} went away twice during a restore",
        doc.id
    )))
}
