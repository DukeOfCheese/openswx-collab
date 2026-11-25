//! The routes about the server itself, the error shape every route shares, and body limits.

mod common;

use std::sync::Arc;

use axum::Router;
use axum::routing::post;
use common::TestServer;
use oswx_server::ext::Extensions;
use oswx_server::{ApiError, ApiJson, AppState};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};

async fn get(server: &TestServer, path: &str) -> (StatusCode, Value) {
    let r = reqwest::get(server.url(path)).await.unwrap();
    let status = r.status();
    (status, r.json().await.unwrap())
}

/// Adds `n` accounts straight into the database (accounts get routes in Task 4).
async fn add_users(server: &TestServer, n: usize) {
    for _ in 0..n {
        let id = oswx_server::store::new_id();
        server
            .sql(&format!(
                "INSERT INTO users (id, email, name, created_at) VALUES ('{id}', '{id}@example.com', 'U', 0)"
            ))
            .await;
    }
}

#[tokio::test]
async fn health_says_ok() {
    let server = TestServer::start().await;
    assert_eq!(
        get(&server, "/api/health").await,
        (StatusCode::OK, json!({"ok": true}))
    );
}

#[tokio::test]
async fn ready_is_ok_while_the_store_answers_and_503_when_it_does_not() {
    let (server, store) = TestServer::start_flaky(|_| {}).await;
    assert_eq!(
        get(&server, "/api/ready").await,
        (StatusCode::OK, json!({"ok": true}))
    );

    store.fail_health(true);
    let (status, body) = get(&server, "/api/ready").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, json!({"ok": false}));

    store.fail_health(false);
    assert_eq!(get(&server, "/api/ready").await.0, StatusCode::OK);
}

#[tokio::test]
async fn server_info_has_the_python_shape_with_protocol_2() {
    let server = TestServer::start().await;
    let (status, body) = get(&server, "/api/server").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({
            "name": "openswx-collab",
            "version": oswx_server::VERSION,
            "protocol": 2,
            "registration": "first_user",
            "oauth": [],
            "email": false,
            "edition": "core",
        })
    );
}

#[tokio::test]
async fn registration_is_first_user_then_invite_or_open() {
    let server = TestServer::start().await;
    assert_eq!(
        get(&server, "/api/server").await.1["registration"],
        "first_user"
    );
    add_users(&server, 1).await;
    assert_eq!(
        get(&server, "/api/server").await.1["registration"],
        "invite"
    );

    let server = TestServer::start_with(|s| s.open_registration = true).await;
    assert_eq!(
        get(&server, "/api/server").await.1["registration"],
        "first_user"
    );
    add_users(&server, 3).await;
    assert_eq!(get(&server, "/api/server").await.1["registration"], "open");
}

#[tokio::test]
async fn server_info_lists_configured_providers_and_email() {
    let server = TestServer::start_with(|s| {
        s.github_client_id = "gh".into();
        s.github_client_secret = "ghs".into();
        s.google_client_id = "g".into(); // no secret: not configured
        s.smtp_host = "mail.example.com".into();
        s.smtp_from = "collab@example.com".into();
    })
    .await;
    let (_, body) = get(&server, "/api/server").await;
    assert_eq!(body["oauth"], json!(["github"]));
    assert_eq!(body["email"], true);
}

/// An extension used to reach the app from outside: extra info and routes.
struct TestExtension;

#[derive(Deserialize)]
struct Named {
    #[allow(dead_code)]
    name: String,
    count: u32,
}

async fn echo_count(ApiJson(body): ApiJson<Named>) -> ApiJson<Value> {
    ApiJson(json!({"count": body.count}))
}

async fn fail() -> Result<(), ApiError> {
    Err(anyhow::anyhow!("the secret reason").into())
}

async fn missing() -> Result<(), ApiError> {
    Err(ApiError::NotFound("no such project"))
}

impl Extensions for TestExtension {
    fn server_info(&self) -> Value {
        json!({"edition": "test", "seats": 5})
    }

    fn routes(&self, state: AppState) -> Router {
        Router::new()
            .route("/api/test/echo", post(echo_count))
            .route("/api/test/fail", post(fail))
            .route("/api/test/missing", axum::routing::get(missing))
            .with_state(state)
    }
}

async fn with_extension() -> TestServer {
    TestServer::start_custom(|_| {}, |s| s.ext = Arc::new(TestExtension)).await
}

#[tokio::test]
async fn extension_info_is_merged_into_server_info() {
    let server = with_extension().await;
    let (_, body) = get(&server, "/api/server").await;
    assert_eq!(body["edition"], "test");
    assert_eq!(body["seats"], 5);
    assert_eq!(body["protocol"], 2);
}

#[tokio::test]
async fn an_unknown_route_is_404_with_a_detail() {
    let server = TestServer::start().await;
    assert_eq!(
        get(&server, "/api/no/such/route").await,
        (StatusCode::NOT_FOUND, json!({"detail": "Not Found"}))
    );
}

#[tokio::test]
async fn a_handler_404_carries_its_own_message() {
    let server = with_extension().await;
    assert_eq!(
        get(&server, "/api/test/missing").await,
        (StatusCode::NOT_FOUND, json!({"detail": "no such project"}))
    );
}

#[tokio::test]
async fn a_wrong_method_is_405_with_a_detail() {
    let server = with_extension().await;
    let http = reqwest::Client::new();
    for (method, path) in [
        (reqwest::Method::DELETE, "/api/health"),
        (reqwest::Method::GET, "/api/test/echo"),
    ] {
        let r = http.request(method, server.url(path)).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::METHOD_NOT_ALLOWED, "{path}");
        assert_eq!(
            r.json::<Value>().await.unwrap(),
            json!({"detail": "Method Not Allowed"})
        );
    }
}

#[tokio::test]
async fn a_body_over_1_mib_is_413_on_any_route() {
    let server = with_extension().await;
    let http = reqwest::Client::new();
    let big = json!({"name": "x".repeat(2 << 20), "count": 1}).to_string();
    for path in ["/api/test/echo", "/api/server", "/api/health"] {
        let r = http
            .post(server.url(path))
            .header("content-type", "application/json")
            .body(big.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE, "{path}");
        assert_eq!(
            r.json::<Value>().await.unwrap(),
            json!({"detail": "too large"})
        );
    }
}

#[tokio::test]
async fn a_body_over_1_mib_without_a_length_is_413_too() {
    let server = with_extension().await;
    let chunks: Vec<Result<Vec<u8>, std::io::Error>> =
        (0..32).map(|_| Ok(vec![b' '; 64 * 1024])).collect();
    let r = reqwest::Client::new()
        .post(server.url("/api/test/echo"))
        .header("content-type", "application/json")
        .body(reqwest::Body::wrap_stream(futures_util::stream::iter(
            chunks,
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        r.json::<Value>().await.unwrap(),
        json!({"detail": "too large"})
    );
}

#[tokio::test]
async fn bad_json_bodies_answer_with_a_detail_string() {
    let server = with_extension().await;
    let http = reqwest::Client::new();
    let send = |body: &'static str, content_type: &'static str| {
        http.post(server.url("/api/test/echo"))
            .header("content-type", content_type)
            .body(body)
            .send()
    };

    let r = send(r#"{"name": "a", "count": 2}"#, "application/json")
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.json::<Value>().await.unwrap(), json!({"count": 2}));

    for (body, content_type, want) in [
        ("{not json", "application/json", StatusCode::BAD_REQUEST),
        (
            r#"{"name": "a", "count": "two"}"#,
            "application/json",
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            r#"{"name": "a", "count": -1}"#,
            "application/json",
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            r#"{"name": "a"}"#,
            "application/json",
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            r#"{"name": "a", "count": 2}"#,
            "text/plain",
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
    ] {
        let r = send(body, content_type).await.unwrap();
        assert_eq!(r.status(), want, "{body}");
        let detail: Value = r.json().await.unwrap();
        assert!(detail["detail"].is_string(), "{body}: {detail}");
    }
}

#[tokio::test]
async fn an_internal_error_says_only_server_error() {
    let server = with_extension().await;
    let r = reqwest::Client::new()
        .post(server.url("/api/test/fail"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        r.json::<Value>().await.unwrap(),
        json!({"detail": "server error"})
    );
}

#[tokio::test]
async fn restart_keeps_the_store_and_serves_again() {
    let server = TestServer::start_with(|s| s.open_registration = true).await;
    add_users(&server, 2).await;
    let server = server.restart().await;
    let (status, body) = get(&server, "/api/server").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["registration"], "open");
}
