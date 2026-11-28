//! Every error response is JSON, and a panicking handler does not take the connection down.

mod common;

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query};
use axum::routing::{get, post};
use common::TestServer;
use oswx_server::ext::Extensions;
use oswx_server::{ApiError, ApiPath, ApiQuery, AppState};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};

struct Raw;

#[derive(Deserialize)]
struct Page {
    n: u32,
}

async fn raw_path(Path(n): Path<u32>) -> String {
    n.to_string()
}

async fn raw_query(Query(p): Query<Page>) -> String {
    p.n.to_string()
}

async fn api_path(ApiPath(n): ApiPath<u32>) -> String {
    n.to_string()
}

async fn api_query(ApiQuery(p): ApiQuery<Page>) -> String {
    p.n.to_string()
}

async fn boom() -> Result<(), ApiError> {
    panic!("the handler went wrong");
}

async fn plain_text_error() -> (StatusCode, &'static str) {
    (StatusCode::IM_A_TEAPOT, "plain text")
}

async fn json_error_kept() -> (StatusCode, axum::Json<Value>) {
    (
        StatusCode::CONFLICT,
        axum::Json(json!({"detail": "mine", "extra": 1})),
    )
}

impl Extensions for Raw {
    fn routes(&self, state: AppState) -> Router {
        Router::new()
            .route("/api/test/raw-path/{n}", get(raw_path))
            .route("/api/test/raw-query", get(raw_query))
            .route("/api/test/path/{n}", get(api_path))
            .route("/api/test/query", get(api_query))
            .route("/api/test/boom", post(boom))
            .route("/api/test/teapot", get(plain_text_error))
            .route("/api/test/conflict", get(json_error_kept))
            .with_state(state)
    }
}

async fn server() -> TestServer {
    TestServer::start_custom(|_| {}, |s| s.ext = Arc::new(Raw)).await
}

async fn fetch(
    server: &TestServer,
    method: reqwest::Method,
    path: &str,
) -> (StatusCode, String, Value) {
    let r = reqwest::Client::new()
        .request(method, server.url(path))
        .send()
        .await
        .unwrap();
    let status = r.status();
    let content_type = r
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    let text = r.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or_else(|_| panic!("not JSON: {text:?}"));
    (status, content_type, body)
}

async fn get_json(server: &TestServer, path: &str) -> (StatusCode, String, Value) {
    fetch(server, reqwest::Method::GET, path).await
}

#[tokio::test]
async fn raw_axum_extractors_still_answer_json() {
    let server = server().await;
    for path in [
        "/api/test/raw-path/abc",
        "/api/test/raw-query?n=x",
        "/api/test/raw-query",
    ] {
        let (status, content_type, body) = get_json(&server, path).await;
        assert!(status.is_client_error(), "{path}: {status}");
        assert!(
            content_type.starts_with("application/json"),
            "{path}: {content_type}"
        );
        assert!(body["detail"].is_string(), "{path}: {body}");
    }
    let (status, _, body) = get_json(&server, "/api/test/raw-path/7").await;
    assert_eq!((status, body), (StatusCode::OK, json!(7)));
}

#[tokio::test]
async fn api_path_and_query_refuse_with_400_and_a_detail() {
    let server = server().await;
    for path in [
        "/api/test/path/abc",
        "/api/test/path/-1",
        "/api/test/path/99999999999",
        "/api/test/query?n=x",
        "/api/test/query?n=-1",
        "/api/test/query",
    ] {
        let (status, content_type, body) = get_json(&server, path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
        assert!(content_type.starts_with("application/json"), "{path}");
        assert!(
            body["detail"].as_str().is_some_and(|d| !d.is_empty()),
            "{path}: {body}"
        );
    }
    assert_eq!(
        get_json(&server, "/api/test/path/12").await.0,
        StatusCode::OK
    );
    assert_eq!(
        get_json(&server, "/api/test/query?n=3").await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn a_plain_text_error_is_rewritten_and_a_json_one_is_kept() {
    let server = server().await;
    let (status, content_type, body) = get_json(&server, "/api/test/teapot").await;
    assert_eq!(status, StatusCode::IM_A_TEAPOT);
    assert!(content_type.starts_with("application/json"));
    assert_eq!(body, json!({"detail": "I'm a teapot"}));

    let (status, _, body) = get_json(&server, "/api/test/conflict").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body, json!({"detail": "mine", "extra": 1}));
}

#[tokio::test]
async fn a_panicking_handler_answers_500_and_the_server_carries_on() {
    let server = server().await;
    let (status, content_type, body) =
        fetch(&server, reqwest::Method::POST, "/api/test/boom").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(content_type.starts_with("application/json"));
    assert_eq!(body, json!({"detail": "server error"}));
    // Again, and a normal route still works.
    assert_eq!(
        fetch(&server, reqwest::Method::POST, "/api/test/boom")
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(get_json(&server, "/api/health").await.0, StatusCode::OK);
}
