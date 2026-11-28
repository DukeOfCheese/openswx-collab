//! The routes about the server itself, the error shape every route shares, and body limits.

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::post;
use common::TestServer;
use oswx_server::ext::Extensions;
use oswx_server::{ApiError, ApiJson, AppState};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};

/// `/api/ready` (on a server started with [`flaky_with_ready_cache`] of zero: asked anew).
async fn ready(server: &TestServer) -> (StatusCode, Value) {
    get(server, "/api/ready").await
}

/// A server on a [`common::FlakyStore`] whose `/api/ready` gives an answer again for `cache`.
async fn flaky_with_ready_cache(cache: Duration) -> (TestServer, Arc<common::FlakyStore>) {
    TestServer::start_flaky_full(|_| {}, common::new_journal, |s| s.ready_cache = cache).await
}

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
    let (server, store) = flaky_with_ready_cache(Duration::ZERO).await;
    assert_eq!(
        get(&server, "/api/ready").await,
        (StatusCode::OK, json!({"ok": true}))
    );

    store.fail_health(true);
    let (status, body) = ready(&server).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, json!({"ok": false}));

    store.fail_health(false);
    assert_eq!(ready(&server).await.0, StatusCode::OK);
}

#[tokio::test]
async fn ready_checks_once_at_a_time_gives_its_answer_again_and_warns_once() {
    // An answer is given again for as long as the cache says (a second in a running server;
    // here for longer than the test takes).
    assert_eq!(
        oswx_server::routes::meta::READY_CACHE,
        Duration::from_secs(1)
    );
    let (server, store) = flaky_with_ready_cache(Duration::from_secs(3600)).await;
    let logs = common::LogCapture::start();
    store.fail_health(true);
    // Twenty at once: one check, one warning, the same answer for all.
    let asks = (0..20).map(|_| get(&server, "/api/ready"));
    let answers = futures_util::future::join_all(asks).await;
    assert!(
        answers
            .iter()
            .all(|(s, _)| *s == StatusCode::SERVICE_UNAVAILABLE),
        "{answers:?}"
    );
    // The store mends while the answer is fresh: it is given again.
    store.fail_health(false);
    assert_eq!(
        get(&server, "/api/ready").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        logs.text().matches("not ready").count(),
        1,
        "{}",
        logs.text()
    );
}

#[tokio::test]
async fn ready_says_why_once_when_it_stops_being_ready_and_when_it_is_again() {
    // Every ask is a check.
    let (server, store) = flaky_with_ready_cache(Duration::ZERO).await;
    let logs = common::LogCapture::start();
    store.fail_health(true);
    assert_eq!(ready(&server).await.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        logs.text().matches("not ready").count(),
        1,
        "{}",
        logs.text()
    );
    store.fail_health(false);
    assert_eq!(ready(&server).await.0, StatusCode::OK);
    assert!(logs.text().contains("ready again"), "{}", logs.text());
    // Not ready for a while: said once, not at every check.
    store.fail_health(true);
    for _ in 0..3 {
        assert_eq!(ready(&server).await.0, StatusCode::SERVICE_UNAVAILABLE);
    }
    assert_eq!(
        logs.text().matches("not ready").count(),
        2,
        "{}",
        logs.text()
    );
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
    let sent = reqwest::Client::new()
        .post(server.url("/api/test/echo"))
        .header("content-type", "application/json")
        .body(reqwest::Body::wrap_stream(futures_util::stream::iter(
            chunks,
        )))
        .send()
        .await;
    // The server answers once it has read 1 MiB and closes without reading the rest. Where the
    // client is still writing then, Windows resets the connection and the answer is lost to it:
    // the body was refused all the same.
    let r = match sent {
        Ok(r) => r,
        Err(e) if cfg!(windows) && e.is_request() && format!("{e:?}").contains("BodyWrite") => {
            return;
        }
        Err(e) => panic!("{e:?}"),
    };
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

#[tokio::test]
async fn restart_reopens_the_database_file_as_a_new_process_would() {
    let server = TestServer::start().await;
    add_users(&server, 3).await;
    let before = server.db().clone();
    let server = server.restart().await;
    // The old connections are closed; the new store reads what was written before.
    assert!(before.health().await.is_err());
    assert_eq!(server.state.store.user_count().await.unwrap(), 3);
    assert_eq!(
        get(&server, "/api/server").await.1["registration"],
        "invite"
    );
}

#[tokio::test]
async fn restart_keeps_a_flaky_store_wrapped_around_the_reopened_one() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    add_users(&server, 1).await;
    let server = server.restart().await;
    assert_eq!(server.state.store.user_count().await.unwrap(), 1);
    flaky.fail_health(true);
    // (The first ask of the restarted server: nothing to give again.)
    assert_eq!(
        get(&server, "/api/ready").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn dropping_the_server_closes_the_database() {
    let server = TestServer::start().await;
    let db = server.db().clone();
    drop(server);
    assert!(db.health().await.is_err());
}
