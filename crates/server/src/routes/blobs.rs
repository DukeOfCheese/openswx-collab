//! Blobs: large binary data (imported geometry) that documents refer to by SHA-256.
//!
//! - `POST /api/projects/{project}/blobs/missing` (viewer): body a JSON array of at most 2,000
//!   SHA-256s in lower-case hex; answers those the server does not have, in the order given, so a
//!   client uploads only those. More than 2,000 is 400; an entry that is not a SHA-256 is 422.
//! - `PUT /api/projects/{project}/blobs/{sha256}` (editor): the body is the raw bytes, streamed. The
//!   server hashes them as they arrive and refuses with 400 if the digest is not the one in the path
//!   (nothing is stored). Over `max_blob_bytes` is 413: before reading anything when
//!   `Content-Length` says so, and as soon as the streamed size passes it otherwise. 204 when stored.
//!   A blob that is already stored is not written again: the body is still read and checked, and the
//!   answer is the same 204. A path that is not a SHA-256 is 400. The server takes only so many
//!   uploads at once, and so many from one organisation and from one account
//!   ([`crate::uploads`]; an upload of a blob already stored holds no memory and is not counted):
//!   one more is refused before any of it is stored, with 429 (the
//!   account's or the organisation's own limit) or 503 (the server's, after a short wait for a
//!   place). An upload that does not keep up the minimum average rate
//!   ([`crate::blobs::Pace`]) is answered 400 [`TOO_SLOW`], and one that is not done within
//!   [`crate::uploads::time_allowed`] 400 [`TOO_LONG`]; neither leaves anything in storage.
//! - `GET /api/projects/{project}/blobs/{sha256}` (viewer): the raw bytes, streamed, as
//!   `application/octet-stream` with their `Content-Length`, `Cache-Control: private,
//!   max-age=31536000, immutable` and `ETag: "<sha256>"`. `If-None-Match` with that tag answers 304.
//!   Ranges are not supported: a `Range` header is ignored and the whole blob comes back with 200
//!   (`Accept-Ranges: none`). No such blob, or a path that is not a SHA-256, is 404 `no such blob`.
//!
//! A transfer in which no bytes move for `blob_idle_timeout` seconds is given up on: a stalled
//! upload is answered 400 `the upload did not finish` and leaves nothing in storage; a download
//! whose client stops reading has its side of storage (an open file, a connection to the bucket)
//! released, and ends short.
//!
//! Blobs belong to the organisation, not the project: every project of an organisation reaches the
//! same blobs ([`blob_key`]). A project you have no role in is 404 `no such project`.

use std::error::Error as StdError;
use std::io;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{post, put};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use http_body_util::LengthLimitError;
use sha2::{Digest, Sha256};

use crate::access::{CurrentUser, require_project};
use crate::blobs::{
    ByteStream, Pace, PutError, TooSlow, blob_key, drop_when_stalled, fail_when_slow,
    fail_when_stalled, is_sha256,
};
use crate::error::{ApiError, ApiJson, ApiPath};
use crate::routes::{DRAIN_AT_MOST, DRAIN_TIMEOUT};
use crate::security::ProjectRole;
use crate::state::AppState;
use crate::uploads::time_allowed;

/// The most hashes one `blobs/missing` request may ask about.
pub const MAX_MISSING: usize = 2000;
/// How many storage lookups one `blobs/missing` request runs at once.
const LOOKUPS_AT_ONCE: usize = 16;

const NO_SUCH_BLOB: &str = "no such blob";
/// What an upload that did not keep up the minimum rate is answered (400).
pub const TOO_SLOW: &str = "the upload was too slow and was given up on";
/// What an upload that was not done in the time it is allowed is answered (400).
pub const TOO_LONG: &str = "the upload took too long and was given up on";
const CACHE_FOREVER: &str = "private, max-age=31536000, immutable";

/// The routes whose bodies are at most the default limit.
pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/projects/{project_id}/blobs/missing", post(missing))
        .route(
            "/api/projects/{project_id}/blobs/{sha}",
            axum::routing::get(download),
        )
}

/// The upload route, which the caller must wrap in `with_body_limit(_, max_blob_bytes)`.
pub(crate) fn upload_router() -> Router<AppState> {
    Router::new().route("/api/projects/{project_id}/blobs/{sha}", put(upload))
}

/// Which of these blobs the server does not have yet.
async fn missing(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath(project_id): ApiPath<String>,
    ApiJson(shas): ApiJson<Vec<String>>,
) -> Result<ApiJson<Vec<String>>, ApiError> {
    let (project, _) = require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Viewer,
    )
    .await?;
    if shas.len() > MAX_MISSING {
        return Err(ApiError::BadRequest(format!(
            "ask about at most {MAX_MISSING} blobs at a time"
        )));
    }
    if !shas.iter().all(|s| is_sha256(s)) {
        return Err(ApiError::Unprocessable(
            "every blob name must be its sha256 in lower-case hex".into(),
        ));
    }
    let keys: Vec<String> = shas
        .iter()
        .map(|sha| blob_key(&project.org_id, sha))
        .collect();
    let present: Vec<bool> = stream::iter(keys.into_iter().map(|key| {
        let blobs = state.blobs.clone();
        async move { blobs.exists(&key).await }
    }))
    .buffered(LOOKUPS_AT_ONCE)
    .try_collect()
    .await?;
    Ok(ApiJson(
        shas.into_iter()
            .zip(present)
            .filter_map(|(sha, here)| (!here).then_some(sha))
            .collect(),
    ))
}

/// The body's digest is not the one in the path.
#[derive(Debug, thiserror::Error)]
#[error("the content does not match the sha256 in the address")]
struct DigestMismatch;

/// The body went over the route's limit (the body-limit layer's error).
#[derive(Debug, thiserror::Error)]
#[error("too large")]
struct OverLimit;

/// Stores a blob, streamed.
async fn upload(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath((project_id, sha)): ApiPath<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Result<StatusCode, ApiError> {
    let (project, _) = require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Editor,
    )
    .await?;
    if !is_sha256(&sha) {
        return Err(ApiError::BadRequest(
            "the blob name must be its sha256 in lower-case hex".into(),
        ));
    }
    let key = blob_key(&project.org_id, &sha);
    let limit = state.settings.max_blob_bytes as u64;
    // Already stored: the body is read and checked (so the client is answered, not cut off), one
    // chunk at a time, and nothing is written; it holds no memory to speak of, so it takes no
    // place among the uploads.
    let stored = state.blobs.exists(&key).await?;
    // Held until this request ends, however it ends.
    let _place = if stored {
        None
    } else {
        match state.uploads.place(&user.id, &project.org_id).await {
            Ok(place) => Some(place),
            Err(refusal) => {
                unread(&headers, body).await;
                return Err(refusal);
            }
        }
    };
    // The whole upload, storing included, is done in this time or given up on (and whatever it
    // was writing dropped, which removes it).
    let size = declared(&headers).unwrap_or(limit).min(limit);
    let allowed = time_allowed(&state.settings, size);
    let storing = async {
        let idle = Duration::from_secs_f64(state.settings.blob_idle_timeout);
        let body = fail_when_stalled(verified(body, sha), idle);
        let body = fail_when_slow(body, Pace::from_settings(&state.settings));
        if stored {
            drain(body, limit).await.map_err(upload_error)?;
        } else {
            state
                .blobs
                .put_stream(&key, body, limit)
                .await
                .map_err(upload_error)?;
        }
        Ok::<_, ApiError>(StatusCode::NO_CONTENT)
    };
    match tokio::time::timeout(allowed, storing).await {
        Ok(answer) => answer,
        Err(_) => {
            crate::metrics::upload_cut_short();
            Err(ApiError::BadRequest(TOO_LONG.into()))
        }
    }
}

/// The request's `Content-Length`, if it has one.
fn declared(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
}

/// Before an upload is refused unread: a body of modest declared size is read and thrown away
/// (for a few seconds at most), so that a client still sending it reads the refusal rather than a
/// connection reset, as a body over the size limit is treated.
async fn unread(headers: &HeaderMap, body: Body) {
    if declared(headers).is_some_and(|n| n <= DRAIN_AT_MOST) {
        let mut chunks = body.into_data_stream();
        let reading = async { while let Some(Ok(_)) = chunks.next().await {} };
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, reading).await;
    }
}

/// Reads `body` to its end, holding one chunk at a time, refusing it past `limit` bytes.
async fn drain(mut body: ByteStream, limit: u64) -> anyhow::Result<()> {
    let mut seen = 0u64;
    while let Some(chunk) = body.next().await {
        seen += chunk.map_err(PutError::Body)?.len() as u64;
        if seen > limit {
            return Err(PutError::TooLarge.into());
        }
    }
    Ok(())
}

/// The request body as a stream that hashes what passes and, at its end, fails with
/// [`DigestMismatch`] unless the SHA-256 was `expected`. The body-limit layer's error becomes
/// [`OverLimit`].
fn verified(body: Body, expected: String) -> ByteStream {
    let chunks = body.into_data_stream();
    stream::unfold(
        (chunks, Some(Sha256::new())),
        move |(mut chunks, hasher)| {
            let expected = expected.clone();
            async move {
                let mut hasher = hasher?;
                match chunks.next().await {
                    Some(Ok(chunk)) => {
                        hasher.update(&chunk);
                        Some((Ok(chunk), (chunks, Some(hasher))))
                    }
                    Some(Err(err)) => Some((Err(body_error(err)), (chunks, None))),
                    None if hex::encode(hasher.finalize()) == expected => None,
                    None => Some((
                        Err(io::Error::new(io::ErrorKind::InvalidData, DigestMismatch)),
                        (chunks, None),
                    )),
                }
            }
        },
    )
    .boxed()
}

/// A body read error as an `io::Error`: [`OverLimit`] for the body-limit layer's, else as it is.
fn body_error(err: axum::Error) -> io::Error {
    let mut cause: Option<&(dyn StdError + 'static)> = Some(&err);
    while let Some(e) = cause {
        if e.is::<LengthLimitError>() {
            return io::Error::other(OverLimit);
        }
        cause = e.source();
    }
    io::Error::other(err)
}

/// What a failed upload answers.
fn upload_error(err: anyhow::Error) -> ApiError {
    match err.downcast_ref::<PutError>() {
        Some(PutError::TooLarge) => ApiError::TooLarge,
        Some(PutError::Body(e)) => match e.get_ref() {
            Some(inner) if inner.is::<OverLimit>() => ApiError::TooLarge,
            Some(inner) if inner.is::<DigestMismatch>() => {
                ApiError::BadRequest(DigestMismatch.to_string())
            }
            Some(inner) if inner.is::<TooSlow>() => ApiError::BadRequest(TOO_SLOW.into()),
            _ => {
                tracing::debug!("a blob upload ended early: {e}");
                ApiError::BadRequest("the upload did not finish".into())
            }
        },
        None => ApiError::Internal(err),
    }
}

/// Whether `If-None-Match` names this blob's tag (or `*`).
fn not_modified(headers: &HeaderMap, sha: &str) -> bool {
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|tag| tag.trim())
        .any(|tag| {
            let tag = tag.strip_prefix("W/").unwrap_or(tag);
            tag == "*" || tag.strip_prefix('"').and_then(|t| t.strip_suffix('"')) == Some(sha)
        })
}

/// The headers every answer about a stored blob carries.
fn cache_headers(sha: &str) -> Result<[(header::HeaderName, HeaderValue); 2], ApiError> {
    Ok([
        (
            header::CACHE_CONTROL,
            HeaderValue::from_static(CACHE_FOREVER),
        ),
        (header::ETAG, HeaderValue::from_str(&format!("\"{sha}\""))?),
    ])
}

/// Sends a blob, streamed.
async fn download(
    State(state): State<AppState>,
    user: CurrentUser,
    ApiPath((project_id, sha)): ApiPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let (project, _) = require_project(
        state.store.as_ref(),
        &user.id,
        &project_id,
        ProjectRole::Viewer,
    )
    .await?;
    if !is_sha256(&sha) {
        return Err(ApiError::NotFound(NO_SUCH_BLOB));
    }
    let key = blob_key(&project.org_id, &sha);
    if not_modified(&headers, &sha) {
        if !state.blobs.exists(&key).await? {
            return Err(ApiError::NotFound(NO_SUCH_BLOB));
        }
        return Ok((StatusCode::NOT_MODIFIED, cache_headers(&sha)?).into_response());
    }
    let Some((raw_len, chunks)) = state.blobs.get_stream(&key).await? else {
        return Err(ApiError::NotFound(NO_SUCH_BLOB));
    };
    Ok((
        cache_headers(&sha)?,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            ),
            (header::CONTENT_LENGTH, HeaderValue::from(raw_len)),
            (header::ACCEPT_RANGES, HeaderValue::from_static("none")),
        ],
        Body::from_stream(drop_when_stalled(
            chunks,
            Duration::from_secs_f64(state.settings.blob_idle_timeout),
        )),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn if_none_match_names_the_tag_in_any_of_its_forms() {
        let sha = "ab".repeat(32);
        let with = |v: &str| {
            let mut h = HeaderMap::new();
            h.insert(header::IF_NONE_MATCH, HeaderValue::from_str(v).unwrap());
            not_modified(&h, &sha)
        };
        assert!(with(&format!("\"{sha}\"")));
        assert!(with(&format!("W/\"{sha}\"")));
        assert!(with(&format!("\"other\", \"{sha}\"")));
        assert!(with("*"));
        assert!(!with(&sha), "a tag is quoted");
        assert!(!with("\"other\""));
        assert!(!not_modified(&HeaderMap::new(), &sha));
    }
}
