//! Documents: parts and assemblies, reached through their project.
//!
//! - `GET /api/projects/{project}/documents` (viewer): the documents that are not deleted, by name,
//!   each with who has it open (`online`, `online_count`). `?folder=<id>` lists only those directly
//!   in that folder; `?folder=root` only those at the top of the project.
//! - `POST /api/projects/{project}/documents` (editor): `{name, content, kind?, folder_id?}`, at
//!   most [`MAX_DOCUMENT_BODY`]; 201 with the document. The checks, in order, are listed on the
//!   handler, `create_document`.
//! - `GET /api/documents/{document}` (viewer), `PATCH` (editor: `name` and/or `folder_id`, `null`
//!   being the top of the project), `DELETE` (project owner, as in the Python server; 204, a soft
//!   delete: the row stays, marked deleted, and open connections are closed).
//! - `GET /api/documents/{document}/content` (viewer): `{"document", "seq", "content"}`, the
//!   document as it is now; `?seq=N` (an integer, else 422) or `?version=<name>` (which wins) for
//!   an earlier state (see `routes::history`: 404 `the document has no state N` / `no version
//!   named 'x'`).
//!
//! A document answers as `{id, project_id, folder_id, name, kind, format_version, seq, created_by,
//! created_at, updated_at}` (`seq` is its head seq), with `role`, the caller's role, except inside
//! `content`. A document that does not exist, was deleted, or is in a project the caller has no role
//! in answers 404 `no such document`, the same in all three cases ([`require_document`]).

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{FromRequest, Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use oswx_core::{Index, Value, ValueError};
use serde::{Deserialize, Deserializer};
use serde_json::json;

use crate::access::{
    CurrentUser, recheck_document, recheck_project, require_document, require_project,
};
use crate::content::{
    blob_refs, check_shape, format_version, json_from_plain, plain_from_json, with_item_lists,
    without_local_keys,
};
use crate::error::{ApiError, ApiJson, ApiPath, ApiQuery, detail_response};
use crate::ext::{Event, Quota, check_quota, emit};
use crate::routes::history;
use crate::routes::orgs::clean_name;
use crate::security::ProjectRole;
use crate::snapshot::{self, load_head_doc, snapshot_key};
use crate::state::AppState;
use crate::store::{Document, Folder, SnapshotRow, new_id};

/// The largest request body document creation takes: 8 MiB.
pub const MAX_DOCUMENT_BODY: usize = 8 << 20;
/// The most distinct blob references one document may hold.
pub const MAX_BLOB_REFS: usize = 4096;
/// The `?folder=` value that lists the documents at the top of the project.
pub const ROOT_FOLDER: &str = "root";
/// What naming a folder that is not in the project answers (422; it never says whether the folder
/// exists elsewhere).
pub(crate) const NO_FOLDER_HERE: &str = "there is no such folder in this project";

/// The routes whose bodies are at most the default limit.
pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/projects/{project_id}/documents", get(list_documents))
        .route(
            "/api/documents/{document_id}",
            get(get_document)
                .patch(patch_document)
                .delete(delete_document),
        )
        .route("/api/documents/{document_id}/content", get(get_content))
}

/// Document creation, which the caller must wrap in `with_body_limit(_, MAX_DOCUMENT_BODY)`.
pub(crate) fn create_router() -> Router<AppState> {
    Router::new().route(
        "/api/projects/{project_id}/documents",
        post(create_document),
    )
}

/// A document as the client sees it; with `role` when given.
pub(crate) fn doc_out(d: &Document, role: Option<ProjectRole>) -> serde_json::Value {
    let mut out = json!({
        "id": d.id, "project_id": d.project_id, "folder_id": d.folder_id, "name": d.name,
        "kind": d.kind, "format_version": d.format_version, "seq": d.head_seq,
        "created_by": d.created_by, "created_at": d.created_at, "updated_at": d.updated_at,
    });
    if let Some(role) = role {
        out["role"] = json!(role.as_str());
    }
    out
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    folder: Option<String>,
}

async fn list_documents(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(project_id): ApiPath<String>,
    ApiQuery(query): ApiQuery<ListQuery>,
) -> Result<ApiJson<Vec<serde_json::Value>>, ApiError> {
    let (project, role) = require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Viewer,
    )
    .await?;
    let docs = state.store.documents_in_project(&project.id).await?;
    let mut out = Vec::with_capacity(docs.len());
    for d in docs.iter().filter(|d| match query.folder.as_deref() {
        None => true,
        Some(ROOT_FOLDER) => d.folder_id.is_none(),
        Some(folder) => d.folder_id.as_deref() == Some(folder),
    }) {
        let (online, online_count) = state.hub.online(&d.id).await;
        let mut item = doc_out(d, Some(role));
        item["online"] = json!(online);
        item["online_count"] = json!(online_count);
        out.push(item);
    }
    Ok(ApiJson(out))
}

#[derive(Debug, Deserialize)]
struct DocumentIn {
    name: String,
    content: serde_json::Value,
    kind: Option<String>,
    folder_id: Option<String>,
}

/// A creation body once parsed: its fields, and the content converted (or why it could not be).
struct Parsed {
    name: String,
    kind: Option<String>,
    folder_id: Option<String>,
    content: Result<Value, ApiError>,
}

/// What a client is told about content the core refuses: the kind of problem, never the content.
fn content_error(err: &ValueError) -> ApiError {
    let why = match err {
        ValueError::TooDeep => "is nested more than 64 levels deep",
        ValueError::NotFinite => "holds NaN or infinity",
        ValueError::KeyTooLong => "has a key or id longer than 200 characters",
        ValueError::DuplicateId(_) => "has a list with a repeated id",
        ValueError::BadId => "has a malformed list",
        ValueError::Unsupported => {
            "holds something a document cannot (a $list key, or an integer out of range)"
        }
    };
    ApiError::Unprocessable(format!("content {why}"))
}

/// The folder `id`, which must be in `project` (422 [`NO_FOLDER_HERE`] otherwise).
pub(crate) fn folder_in(folder: Option<Folder>, project: &str) -> Result<Folder, ApiError> {
    folder
        .filter(|f| f.project_id == project)
        .ok_or_else(|| ApiError::Unprocessable(NO_FOLDER_HERE.into()))
}

/// Whether the request declares a JSON body (`application/json` or `application/...+json`), as
/// [`ApiJson`] requires.
fn declares_json(request: &Request) -> bool {
    let Some(value) = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let essence = value
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    essence == "application/json"
        || (essence.starts_with("application/") && essence.ends_with("+json"))
}

/// Reads the creation body (413 over the route's limit), then parses it and converts the content
/// on the blocking pool, where the JSON tree is also dropped: only the converted content comes
/// back. Not JSON is 400, the wrong shape 422, with fixed messages (a parser's message can quote
/// the content).
async fn parse_body(request: Request, state: &AppState) -> Result<Parsed, Box<Response>> {
    if !declares_json(&request) {
        return Err(Box::new(detail_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Expected request with `Content-Type: application/json`",
        )));
    }
    let bytes = match Bytes::from_request(request, state).await {
        Ok(bytes) => bytes,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return Err(Box::new(ApiError::TooLarge.into_response()));
        }
        Err(rejection) => {
            return Err(Box::new(detail_response(
                rejection.status(),
                &rejection.body_text(),
            )));
        }
    };
    let parsed = tokio::task::spawn_blocking(move || {
        let body: DocumentIn = serde_json::from_slice(&bytes).map_err(|e| {
            if e.is_data() {
                ApiError::Unprocessable(
                    "the body must be {name, content, kind?, folder_id?}, name a string".into(),
                )
            } else {
                ApiError::BadRequest("the body is not valid JSON".into())
            }
        })?;
        drop(bytes);
        let DocumentIn {
            name,
            content,
            kind,
            folder_id,
        } = body;
        let converted =
            Value::from_plain(&plain_from_json(&content)).map_err(|e| content_error(&e));
        drop(content);
        Ok::<_, ApiError>(Parsed {
            name,
            kind,
            folder_id,
            content: converted,
        })
    })
    .await;
    match parsed {
        Ok(Ok(parsed)) => Ok(parsed),
        Ok(Err(err)) => Err(Box::new(err.into_response())),
        Err(err) => Err(Box::new(ApiError::Internal(err.into()).into_response())),
    }
}

/// Turns collaboration on for a file: its content becomes state 0 of a cloud document.
///
/// In this order: the caller must be an editor; the extensions' quota for one more document is
/// asked (402 if refused); the body is read (413 over [`MAX_DOCUMENT_BODY`]) and converted; the
/// name (1 to 200 characters, trimmed), `kind` (`part`, the default, or `assembly`) and the folder
/// (in the same project) are checked (422); the content must be one the core takes, with an object
/// at the top, and its item lists must be editable ([`check_shape`]; 422); the per-person keys
/// ([`crate::content::LOCAL_ONLY`]) are dropped and missing item lists filled in; at most
/// [`MAX_BLOB_REFS`] distinct blob references (422), each naming a blob this organisation has (409
/// `{"message": "upload these blobs first", "blobs": [...]}` listing the missing and malformed
/// hashes); then the snapshot (seq 0, epoch 0) is written to storage, and the document and its
/// snapshot row are committed. A failure before the commit removes the snapshot file again; see
/// `settle_commit_error` for a commit that reports an error.
async fn create_document(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(project_id): ApiPath<String>,
    request: Request,
) -> Response {
    let found = require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Editor,
    )
    .await;
    let project = match found {
        Ok((project, _)) => project,
        Err(err) => return err.into_response(),
    };
    if let Err(err) = check_quota(&state.ext, &project.org_id, Quota::Documents).await {
        return err.into_response();
    }
    let body = match parse_body(request, &state).await {
        Ok(body) => body,
        Err(rejection) => return *rejection,
    };
    create(state, user, project.id, project.org_id, body)
        .await
        .into_response()
}

async fn create(
    state: AppState,
    user: CurrentUser,
    project_id: String,
    org_id: String,
    body: Parsed,
) -> Result<(StatusCode, ApiJson<serde_json::Value>), ApiError> {
    let name = clean_name(&body.name)?;
    let kind = body.kind.unwrap_or_else(|| "part".into());
    if kind != "part" && kind != "assembly" {
        return Err(ApiError::Unprocessable(
            "kind must be part or assembly".into(),
        ));
    }
    if let Some(folder) = &body.folder_id {
        folder_in(state.store.folder_by_id(folder).await?, &project_id)?;
    }
    let doc = body.content?;
    let shape_kind = kind.clone();
    let (doc, refs) = tokio::task::spawn_blocking(move || {
        if !matches!(doc, Value::Map(_)) {
            return Err(ApiError::Unprocessable(
                "content must be a JSON object".into(),
            ));
        }
        check_shape(&shape_kind, &doc)?;
        let doc = with_item_lists(&shape_kind, without_local_keys(doc));
        let refs = blob_refs(&doc);
        Ok((doc, refs))
    })
    .await??;
    if refs.len() > MAX_BLOB_REFS {
        return Err(ApiError::Unprocessable(format!(
            "content refers to more than {MAX_BLOB_REFS} blobs"
        )));
    }
    // TASK 13: for an assembly, check its components' references (access to every part, no
    // cycles) and record them as assembly_refs rows in the transaction below.
    let missing = missing_blobs(&state, &org_id, refs).await?;
    if !missing.is_empty() {
        return Err(ApiError::ConflictWith(
            json!({"message": "upload these blobs first", "blobs": missing}),
        ));
    }

    let now = state.clock.now();
    let document = Document {
        id: new_id(),
        project_id: project_id.clone(),
        folder_id: body.folder_id,
        name,
        kind,
        format_version: format_version(&doc),
        head_seq: 0,
        created_by: user.id.clone(),
        created_at: now,
        updated_at: now,
        deleted_at: None,
    };
    let key = snapshot_key(&document.id, 0, 0);
    let bytes =
        tokio::task::spawn_blocking(move || snapshot::encode(&doc, &Index::default(), 0)).await?;
    let snapshot = SnapshotRow {
        document_id: document.id.clone(),
        epoch: 0,
        seq: 0,
        key: key.clone(),
        size: i64::try_from(bytes.len())?,
        created_at: now,
    };
    state.blobs.put(&key, bytes).await?;
    let role = match insert_document(&state, &user, &document, &snapshot).await {
        Ok(role) => role,
        Err(InsertError::BeforeCommit(err)) => {
            forget_snapshot(&state, &key).await;
            return Err(err);
        }
        Err(InsertError::Commit(err, role)) => {
            settle_commit_error(&state, &document.id, &key, err).await?;
            role
        }
    };
    emit(
        &state.ext,
        Event::DocumentCreated {
            org: org_id,
            project: project_id,
            document: document.id.clone(),
            by: user.id,
        },
    )
    .await;
    Ok((StatusCode::CREATED, ApiJson(doc_out(&document, Some(role)))))
}

/// Removes a snapshot file whose row will never exist (best effort: an orphan file is harmless).
async fn forget_snapshot(state: &AppState, key: &str) {
    if let Err(e) = state.blobs.delete(key).await {
        tracing::warn!("could not remove the unused snapshot {key}: {e:#}");
    }
}

/// After `commit()` reported an error, which may have come after the commit took effect (a
/// connection lost during `COMMIT`): reads the document back. Absent: the creation did not
/// happen, the snapshot file is removed, 500. Present: it did, and the request succeeds. If the
/// read fails too, the file is kept (an orphan file is harmless; a row without its file is not)
/// and the answer is 500.
async fn settle_commit_error(
    state: &AppState,
    document: &str,
    key: &str,
    err: anyhow::Error,
) -> Result<(), ApiError> {
    match state.store.document_by_id(document).await {
        Ok(Some(_)) => {
            tracing::warn!("a commit reported an error but took effect: {err:#}");
            Ok(())
        }
        Ok(None) => {
            forget_snapshot(state, key).await;
            Err(ApiError::Internal(err))
        }
        Err(read) => {
            tracing::error!(
                "could not tell whether document {document} was created ({read:#}); keeping its \
                 snapshot {key}"
            );
            Err(ApiError::Internal(err))
        }
    }
}

/// The blob references that are malformed or name no blob of the organisation, in the order
/// given. A storage that cannot answer is an error, never "missing".
async fn missing_blobs(
    state: &AppState,
    org_id: &str,
    refs: impl IntoIterator<Item = String>,
) -> Result<Vec<String>, ApiError> {
    Ok(crate::blobs::missing_blobs(state.blobs.as_ref(), org_id, refs).await?)
}

/// How [`insert_document`] failed.
enum InsertError {
    /// Before `commit()` was called: nothing was written.
    BeforeCommit(ApiError),
    /// `commit()` itself reported an error; it may still have taken effect. Carries the role the
    /// transaction read.
    Commit(anyhow::Error, ProjectRole),
}

impl<E: Into<ApiError>> From<E> for InsertError {
    fn from(err: E) -> Self {
        Self::BeforeCommit(err.into())
    }
}

/// The transaction of [`create_document`]: the caller's role and the folder read again, then the
/// document and its snapshot row. Answers the caller's role.
async fn insert_document(
    state: &AppState,
    user: &CurrentUser,
    document: &Document,
    snapshot: &SnapshotRow,
) -> Result<ProjectRole, InsertError> {
    let mut tx = state.store.begin().await?;
    let (_, role) = recheck_project(
        &mut *tx,
        &user.id,
        &document.project_id,
        ProjectRole::Editor,
    )
    .await?;
    if let Some(folder) = &document.folder_id {
        folder_in(tx.folder_by_id(folder).await?, &document.project_id)?;
    }
    tx.insert_document(document).await?;
    tx.insert_snapshot(snapshot).await?;
    if let Err(err) = tx.commit().await {
        return Err(InsertError::Commit(err, role));
    }
    Ok(role)
}

async fn get_document(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(document_id): ApiPath<String>,
) -> Result<ApiJson<serde_json::Value>, ApiError> {
    let (doc, _, role) = require_document(
        state.store.as_ref(),
        &user.id,
        &document_id,
        ProjectRole::Viewer,
    )
    .await?;
    Ok(ApiJson(doc_out(&doc, Some(role))))
}

/// `Some(value)` for a field that is present, `null` included (`Some(None)`); with
/// `#[serde(default)]` an absent field stays `None`.
pub(crate) fn present<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}

#[derive(Debug, Deserialize)]
struct PatchIn {
    name: Option<String>,
    #[serde(default, deserialize_with = "present")]
    folder_id: Option<Option<String>>,
}

/// Renames a document and/or moves it to a folder of its project (`folder_id: null`: the top).
/// Open rooms are not told (as in the Python server); the content does not change.
async fn patch_document(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(document_id): ApiPath<String>,
    ApiJson(body): ApiJson<PatchIn>,
) -> Result<ApiJson<serde_json::Value>, ApiError> {
    if body.name.is_none() && body.folder_id.is_none() {
        return Err(ApiError::Unprocessable("give name or folder_id".into()));
    }
    let name = body.name.as_deref().map(clean_name).transpose()?;
    let (doc, project, _) = require_document(
        state.store.as_ref(),
        &user.id,
        &document_id,
        ProjectRole::Editor,
    )
    .await?;
    if let Some(Some(folder)) = &body.folder_id {
        folder_in(state.store.folder_by_id(folder).await?, &project.id)?;
    }
    let mut tx = state.store.begin().await?;
    let (mut doc, _, role) =
        recheck_document(&mut *tx, &user.id, &doc.id, ProjectRole::Editor).await?;
    if let Some(name) = &name {
        tx.set_document_name(&doc.id, name).await?;
        doc.name.clone_from(name);
    }
    if let Some(folder) = &body.folder_id {
        if let Some(folder) = folder {
            folder_in(tx.folder_by_id(folder).await?, &project.id)?;
        }
        tx.set_document_folder(&doc.id, folder.as_deref()).await?;
        doc.folder_id.clone_from(folder);
    }
    tx.commit().await?;
    if name.is_some() {
        emit(
            &state.ext,
            Event::DocumentRenamed {
                org: project.org_id.clone(),
                project: project.id.clone(),
                document: doc.id.clone(),
                by: user.id.clone(),
            },
        )
        .await;
    }
    if body.folder_id.is_some() {
        emit(
            &state.ext,
            Event::DocumentMoved {
                org: project.org_id.clone(),
                project: project.id.clone(),
                document: doc.id.clone(),
                folder: doc.folder_id.clone(),
                by: user.id.clone(),
            },
        )
        .await;
    }
    Ok(ApiJson(doc_out(&doc, Some(role))))
}

/// Deletes a document (project owners only, as in the Python server): it is marked deleted (the
/// row and its snapshots stay) and every connection to it is closed.
async fn delete_document(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(document_id): ApiPath<String>,
) -> Result<StatusCode, ApiError> {
    let (doc, project, _) = require_document(
        state.store.as_ref(),
        &user.id,
        &document_id,
        ProjectRole::Owner,
    )
    .await?;
    let mut tx = state.store.begin().await?;
    recheck_document(&mut *tx, &user.id, &doc.id, ProjectRole::Owner).await?;
    if !tx.mark_document_deleted(&doc.id, state.clock.now()).await? {
        return Err(ApiError::NotFound(crate::access::NO_SUCH_DOCUMENT));
    }
    // TASK 13: after marking it deleted (which locks the row), refuse with 409 `{"message":
    // "assemblies use this part; remove it from them first", "assemblies": [those the caller can
    // open], "in_projects_you_cannot_open": n}` when assemblies that are not deleted use it
    // (rolling back).
    tx.commit().await?;
    state
        .hub
        .close_document(&doc.id, "the document was deleted")
        .await;
    emit(
        &state.ext,
        Event::DocumentDeleted {
            org: project.org_id,
            project: project.id,
            document: doc.id,
            by: user.id,
        },
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct ContentQuery {
    seq: Option<String>,
    version: Option<String>,
}

/// The document as it is now, or at `?seq=` / `?version=`: `{"document": <document, without
/// role>, "seq": <its seq>, "content": <the content>}`. The JSON is written on the blocking pool.
async fn get_content(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(document_id): ApiPath<String>,
    ApiQuery(query): ApiQuery<ContentQuery>,
) -> Result<Response, ApiError> {
    let (doc, _, _) = require_document(
        state.store.as_ref(),
        &user.id,
        &document_id,
        ProjectRole::Viewer,
    )
    .await?;
    // An earlier state (`seq`) or a named version (`version`, which wins); a seq equal to the
    // head is the head.
    let asked =
        history::requested_seq(&state, &doc, query.seq.as_deref(), query.version.as_deref())
            .await?;
    // An open room has the durable head in memory; otherwise it is read from the store (without
    // opening a room for one request).
    let open = match state.hub.loaded(&doc.id) {
        Some(room) => room.durable_head().await,
        None => None,
    };
    let head_seq = match &open {
        Some(head) => i64::try_from(head.seq)?,
        None => doc.head_seq,
    };
    let (content, seq) = match (asked, open) {
        (Some(seq), _) if seq != head_seq => {
            let content = history::content_at(&state, &doc, seq).await?;
            (content, u64::try_from(seq)?)
        }
        (_, Some(head)) => (head.doc, head.seq),
        (_, None) => {
            let (content, _, seq) =
                load_head_doc(state.store.as_ref(), state.blobs.as_ref(), &doc.id).await?;
            (content, seq)
        }
    };
    let body = tokio::task::spawn_blocking(move || {
        serde_json::to_vec(&json!({
            "document": doc_out(&doc, None),
            "seq": seq,
            "content": json_from_plain(&content.to_plain()),
        }))
    })
    .await??;
    Ok((
        [(header::CONTENT_TYPE, "application/json")],
        Body::from(body),
    )
        .into_response())
}
