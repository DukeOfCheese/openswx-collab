//! Errors that are not ours to rewrite, and the status codes of the error kinds.

mod common;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use common::TestServer;
use oswx_server::ext::Extensions;
use oswx_server::{ApiError, AppState};
use reqwest::StatusCode;
use serde_json::{Value, json};

async fn html_error() -> (StatusCode, [(&'static str, &'static str); 2], &'static str) {
    (
        StatusCode::BAD_REQUEST,
        [
            ("content-type", "text/html; charset=utf-8"),
            ("x-kept", "yes"),
        ],
        "<p>this link has expired</p>",
    )
}

async fn text_405() -> (StatusCode, [(&'static str, &'static str); 3], &'static str) {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [
            ("content-type", "text/plain; charset=utf-8"),
            ("allow", "GET, HEAD"),
            ("retry-after", "7"),
        ],
        "nope",
    )
}

struct Pages;

impl Extensions for Pages {
    fn routes(&self, state: AppState) -> Router {
        Router::new()
            .route("/api/test/html", get(html_error))
            .route("/api/test/text405", get(text_405))
            .with_state(state)
    }
}

#[tokio::test]
async fn a_legitimate_html_error_passes_through_untouched() {
    let server = TestServer::start_custom(|_| {}, |s| s.ext = Arc::new(Pages)).await;
    let r = reqwest::get(server.url("/api/test/html")).await.unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert_eq!(r.headers()["content-type"], "text/html; charset=utf-8");
    assert_eq!(r.headers()["x-kept"], "yes");
    assert_eq!(r.text().await.unwrap(), "<p>this link has expired</p>");
}

#[tokio::test]
async fn a_text_plain_error_becomes_json_and_keeps_its_other_headers() {
    let server = TestServer::start_custom(|_| {}, |s| s.ext = Arc::new(Pages)).await;
    let r = reqwest::get(server.url("/api/test/text405")).await.unwrap();
    assert_eq!(r.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(r.headers()["allow"], "GET, HEAD");
    assert_eq!(r.headers()["retry-after"], "7");
    assert!(
        r.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    assert_eq!(
        r.json::<Value>().await.unwrap(),
        json!({"detail": "Method Not Allowed"})
    );
}

#[test]
fn unprocessable_is_422_with_its_message() {
    let e = ApiError::Unprocessable("bad".into());
    assert_eq!(e.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(e.detail(), "bad");
}
