//! The routes, and how they are put together into the app.
//!
//! **Extractors.** Handlers must take their input through [`ApiJson`](crate::ApiJson),
//! [`ApiPath`](crate::ApiPath) and [`ApiQuery`](crate::ApiQuery), never axum's own `Json`, `Path` or
//! `Query`: those answer a bad request in plain text. As a backstop the app rewrites a 4xx or 5xx
//! response with no body or a `text/plain` one into `{"detail": "<status reason>"}`, keeping its other
//! headers (see `json_errors`); a response of any other content type, such as an HTML page, passes
//! through untouched. A handler that panics answers 500 `{"detail": "server error"}` with the panic
//! logged; neither is a reason to rely on the backstops.
//!
//! **Body limits.** Request bodies are refused with 413 over [`DEFAULT_BODY_LIMIT`]. A route that takes
//! bigger bodies goes into the `large` group in the router built by `router` and applies its own limit with
//! [`with_body_limit`]; nothing outside the `standard` group gets the default, so every router merged
//! into `large` must set one. A path may have its methods split between the groups (e.g. `GET` in
//! `standard`, `POST` in `large`).

pub mod auth;
pub mod meta;
pub mod orgs;
pub mod projects;

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use http_body_util::{BodyExt, Limited};
use tower_http::catch_panic::CatchPanicLayer;

use crate::error::{ApiError, detail_response};
use crate::state::AppState;

/// The largest request body a route takes unless it says otherwise: 1 MiB.
pub const DEFAULT_BODY_LIMIT: usize = 1 << 20;

/// A body declared over its route's limit is still read (and thrown away) up to this size, so the
/// client sees the 413; a bigger one is answered at once and the connection closed.
const DRAIN_AT_MOST: u64 = 32 << 20;
/// How long reading such a body may take.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// The app's routes over `state`.
pub(crate) fn router(state: AppState) -> Router {
    // Routes whose bodies are at most DEFAULT_BODY_LIMIT. Merge new routers here.
    let standard = Router::new()
        .merge(meta::router())
        .merge(auth::router())
        .merge(orgs::router())
        .merge(projects::router());

    // Routes with their own body limit, each wrapped in `with_body_limit` (blob uploads, document
    // creation).
    let large: Router<AppState> = Router::new();

    let extensions = with_body_limit(state.ext.routes(state.clone()), DEFAULT_BODY_LIMIT);

    with_body_limit(standard, DEFAULT_BODY_LIMIT)
        .merge(large)
        .with_state(state)
        .merge(extensions)
        .fallback(not_found)
        // After every merge: it applies to the routes registered so far.
        .method_not_allowed_fallback(method_not_allowed)
        // The limits above replace axum's own 2 MB default, which would cap the larger routes.
        .layer(DefaultBodyLimit::disable())
        .layer(middleware::from_fn(json_errors))
        // Outermost: also catches a panic in the layers above.
        .layer(CatchPanicLayer::custom(panicked))
}

/// The backstop for [`ApiError`]'s promise that every error is `{"detail": "..."}`: a 4xx or 5xx
/// response with no body or a `text/plain` body (what axum's own rejections and framework errors
/// produce) becomes `{"detail": "<the status's reason>"}`, keeping its other headers (`Allow`,
/// `Retry-After`, `WWW-Authenticate`, ...). A response of any other content type (an HTML page, JSON,
/// a download's 416) is left alone.
async fn json_errors(req: Request, next: Next) -> Response {
    let response = next.run(req).await;
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error()) {
        return response;
    }
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_ascii_lowercase);
    if content_type
        .as_deref()
        .is_some_and(|v| !v.starts_with("text/plain"))
    {
        return response;
    }
    let (parts, _) = response.into_parts();
    let mut rewritten = detail_response(status, status.canonical_reason().unwrap_or("error"));
    for (name, value) in &parts.headers {
        if name != header::CONTENT_TYPE && name != header::CONTENT_LENGTH {
            rewritten.headers_mut().append(name.clone(), value.clone());
        }
    }
    rewritten
}

/// What a handler that panicked answers: 500 `server error`. The log gets only the kind of payload:
/// a panic message can embed request or document content. (Rust's panic hook still prints the
/// message and location to stderr; nothing here sends them anywhere else.)
fn panicked(panic: Box<dyn std::any::Any + Send + 'static>) -> Response {
    let kind = if panic.is::<&str>() || panic.is::<String>() {
        "message"
    } else {
        "non-string payload"
    };
    tracing::error!("a handler panicked ({kind}; see the panic hook's output for where)");
    detail_response(StatusCode::INTERNAL_SERVER_ERROR, "server error")
}

/// Refuses request bodies over `max` bytes on every route of `router` with 413 `{"detail": "too
/// large"}`: before the handler runs when the declared `Content-Length` is over, and while reading a
/// body sent without one. A handler that reads the body as a stream sees an error once `max` bytes
/// have passed, and should answer `ApiError::TooLarge`.
pub fn with_body_limit<S>(router: Router<S>, max: usize) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.layer(middleware::from_fn_with_state(max, enforce_body_limit))
}

async fn enforce_body_limit(State(max): State<usize>, req: Request, next: Next) -> Response {
    if declared_length(&req).is_some_and(|n| n > max as u64) {
        return too_large(req).await;
    }
    next.run(req.map(|body| Body::new(Limited::new(body, max))))
        .await
}

/// The request's `Content-Length`, if it has one.
fn declared_length(req: &Request) -> Option<u64> {
    req.headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
}

/// 413 for a request whose declared body is over its route's limit.
async fn too_large(req: Request) -> Response {
    if declared_length(&req).is_some_and(|n| n <= DRAIN_AT_MOST) {
        // The client is still sending: read the body and throw it away, so that it reads the 413
        // rather than a connection reset when we close with its data unread.
        let mut body = req.into_body();
        let drain = async { while let Some(Ok(_)) = body.frame().await {} };
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, drain).await;
    }
    ApiError::TooLarge.into_response()
}

async fn not_found() -> ApiError {
    ApiError::NotFound("Not Found")
}

/// 405, or 413 for a body over the default limit (this runs outside the routes' own limits).
async fn method_not_allowed(req: Request) -> Response {
    if declared_length(&req).is_some_and(|n| n > DEFAULT_BODY_LIMIT as u64) {
        return too_large(req).await;
    }
    detail_response(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::{get, post};

    async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        base
    }

    async fn length(body: axum::body::Bytes) -> String {
        body.len().to_string()
    }

    /// The way Tasks 6 and 7 raise the limit: a route in its own router with its own limit, merged
    /// beside default-limited routes on the same path.
    #[tokio::test]
    async fn a_route_can_take_more_than_the_default_on_a_path_shared_with_the_default() {
        let standard = with_body_limit(
            Router::new()
                .route("/x", get(length))
                .route("/y", post(length)),
            DEFAULT_BODY_LIMIT,
        );
        let large = with_body_limit(Router::new().route("/x", post(length)), 4 << 20);
        let app = standard.merge(large).layer(DefaultBodyLimit::disable());
        let base = serve(app).await;
        let http = reqwest::Client::new();
        let two_mib = vec![b'a'; 2 << 20];

        let r = http
            .post(format!("{base}/x"))
            .body(two_mib.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(r.text().await.unwrap(), (2 << 20).to_string());

        let r = http
            .get(format!("{base}/x"))
            .body(two_mib.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 413);
        let r = http
            .post(format!("{base}/y"))
            .body(two_mib)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 413);

        let five_mib = vec![b'a'; 5 << 20];
        let r = http
            .post(format!("{base}/x"))
            .body(five_mib)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 413);
        let body: serde_json::Value = r.json().await.unwrap();
        assert_eq!(body, serde_json::json!({"detail": "too large"}));
    }
}
