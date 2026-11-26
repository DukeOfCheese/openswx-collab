//! Email: password resets, the reset page, and the SMTP mailer (the Python `test_email.py`, without
//! the invite tests, which come with organisations in Task 5).

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use common::{LogCapture, PASSWORD, TestServer};
use oswx_server::config::SmtpSecurity;
use oswx_server::mail::{Mailer, MemoryMailer, SmtpMailer};
use oswx_server::{AppState, Clock, Settings};
use reqwest::StatusCode;
use serde_json::{Value, json};

const T0: f64 = 1_800_000_000.0;

/// The reset link in a message, and its token.
fn link_in(text: &str) -> (String, String) {
    let start = text
        .find("http://testserver/")
        .expect("a link to this server");
    let link: String = text[start..]
        .chars()
        .take_while(|c| !c.is_whitespace())
        .collect();
    let token = link.rsplit('/').next().unwrap().to_owned();
    (link, token)
}

async fn reset(server: &TestServer, email: &str) -> (StatusCode, Value) {
    server
        .post("/api/auth/password-reset", None, json!({"email": email}))
        .await
}

async fn confirm(server: &TestServer, token: &str, password: &str) -> (StatusCode, Value) {
    server
        .post(
            "/api/auth/password-reset/confirm",
            None,
            json!({"token": token, "password": password}),
        )
        .await
}

#[tokio::test]
async fn test_without_smtp_nothing_is_sent_and_resets_say_so() {
    // (That invites say `emailed: false` is checked with invites, Task 5.)
    let server = TestServer::start().await;
    server.register("ada@example.com").await;
    assert_eq!(server.get("/api/server", None).await.1["email"], false);
    let (s, body) = reset(&server, "ada@example.com").await;
    assert_eq!(s, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        body["detail"],
        "this server cannot send email: ask its operator to reset your password"
    );
}

#[tokio::test]
async fn test_a_forgotten_password() {
    // (That the reset also closes open documents with 4401 is checked with the WebSocket, Task 12.)
    let (server, mailer) = TestServer::start_mailing(|_| {}).await;
    assert_eq!(server.get("/api/server", None).await.1["email"], true);
    let (admin, _) = server.register("ada@example.com").await;
    let other = server.login("ada@example.com", PASSWORD).await;
    // The same answer for an address with no account, and nothing sent.
    for email in ["nobody@example.com", "ada@example.com"] {
        let (s, body) = reset(&server, email).await;
        assert_eq!(s, StatusCode::ACCEPTED);
        assert_eq!(
            body["message"],
            "if there is an account for this address, a link is on its way"
        );
    }
    let sent = mailer.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].to, "ada@example.com");
    assert_eq!(sent[0].subject, "Reset your openswx collaboration password");
    assert!(sent[0].text.contains("within 60 minutes"));
    assert!(sent[0].text.contains("signs you out everywhere"));
    let (url, token) = link_in(&sent[0].text);
    assert_eq!(url, format!("http://testserver/reset-password/{token}"));

    let page = reqwest::get(server.url(&format!("/reset-password/{token}")))
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    assert_eq!(page.headers()["cache-control"], "no-store");
    let page = page.text().await.unwrap();
    assert!(page.contains("Choose a new password"));
    assert!(!page.contains(&token), "the page reads it from its address");
    let hostile = reqwest::get(
        server.url("/reset-password/%22%3E%3C%5Cscript%3E%3Cscript%3Ealert(1)%3C%5Cscript%3E"),
    )
    .await
    .unwrap();
    assert_eq!(hostile.status(), StatusCode::OK);
    let hostile = hostile.text().await.unwrap();
    assert!(!hostile.contains("alert(1)"));
    assert_eq!(hostile, page);

    let (s, _) = confirm(&server, &token, "short").await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    let (s, body) = confirm(&server, &token, "a brand new one").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        body["message"],
        "your password is set; sign in again in openswx"
    );
    // Signed out everywhere; the old password is gone; the link works once.
    assert_eq!(
        server.get("/api/me", Some(&other)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        server.get("/api/me", Some(&admin)).await.0,
        StatusCode::UNAUTHORIZED
    );
    let (s, _) = server
        .post(
            "/api/auth/login",
            None,
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    server.login("ada@example.com", "a brand new one").await;
    let (s, body) = confirm(&server, &token, "again again").await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(
        body["detail"],
        "this link has expired or was already used; ask for a new one"
    );
}

#[tokio::test]
async fn test_only_the_newest_link_works_and_requests_are_limited() {
    let (server, mailer) = TestServer::start_mailing(|_| {}).await;
    server.register("ada@example.com").await;
    for _ in 0..7 {
        assert_eq!(
            reset(&server, "ada@example.com").await.0,
            StatusCode::ACCEPTED
        );
    }
    let sent = mailer.sent();
    assert_eq!(sent.len(), 5, "then quietly limited");
    let tokens: Vec<String> = sent.iter().map(|m| link_in(&m.text).1).collect();
    for old in &tokens[..4] {
        assert_eq!(
            confirm(&server, old, "whatever1").await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        confirm(&server, &tokens[4], "whatever1").await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn reset_requests_are_limited_per_address_whatever_the_client() {
    let (server, mailer) = TestServer::start_mailing(|_| {}).await;
    server.register("ada@example.com").await;
    let http = reqwest::Client::new();
    for i in 0..7 {
        let r = http
            .post(server.url("/api/auth/password-reset"))
            .header("x-forwarded-for", format!("203.0.113.{i}"))
            .json(&json!({"email": "ada@example.com"}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::ACCEPTED);
    }
    assert_eq!(mailer.sent().len(), 5);
}

#[tokio::test]
async fn test_a_reset_makes_the_address_count_as_confirmed() {
    // (Bob registers through open registration here; the Python test invites him, Task 5.)
    let (server, mailer) = TestServer::start_mailing(|s| s.open_registration = true).await;
    server.register("ada@example.com").await;
    let (_, bob_id) = server.register("bob@example.com").await;
    let verified = || async {
        server
            .state
            .store
            .user_by_id(&bob_id)
            .await
            .unwrap()
            .unwrap()
            .email_verified
    };
    assert!(!verified().await);
    reset(&server, "bob@example.com").await;
    let (_, token) = link_in(&mailer.sent().last().unwrap().text);
    assert_eq!(
        confirm(&server, &token, "bobs new one").await.0,
        StatusCode::OK
    );
    assert!(verified().await);
}

/// A mail server that refuses everything.
struct BrokenMailer;

#[async_trait]
impl Mailer for BrokenMailer {
    fn configured(&self) -> bool {
        true
    }

    async fn send(&self, _: &str, _: &str, _: &str, _: Option<&str>) -> anyhow::Result<()> {
        anyhow::bail!("connection refused")
    }
}

#[tokio::test]
async fn test_a_broken_mail_server_does_not_break_the_request() {
    // (The invite half comes with invites, Task 5.)
    let logs = LogCapture::start();
    let server = TestServer::start_custom(
        |s| {
            s.smtp_host = "mail.invalid".into();
            s.smtp_from = "c@example.org".into();
        },
        |s| s.mailer = Arc::new(BrokenMailer),
    )
    .await;
    server.register("ada@example.com").await;
    assert_eq!(
        reset(&server, "ada@example.com").await.0,
        StatusCode::ACCEPTED
    );
    assert!(
        logs.text()
            .contains("could not send email to the mail server mail.invalid"),
        "{}",
        logs.text()
    );
}

#[tokio::test]
async fn reset_tokens_and_passwords_are_never_logged() {
    let logs = LogCapture::start();
    let (server, mailer) = TestServer::start_mailing(|_| {}).await;
    let (session, _) = server.register("ada@example.com").await;
    reset(&server, "ada@example.com").await;
    let (_, token) = link_in(&mailer.sent()[0].text);
    reqwest::get(server.url(&format!("/reset-password/{token}")))
        .await
        .unwrap();
    confirm(&server, &token, "short").await;
    confirm(&server, &token, "the new secret").await;
    confirm(&server, &token, "the new secret").await;
    let text = logs.text();
    assert!(
        text.contains("password reset link sent")
            && text.contains("password set through a reset link"),
        "the capture works: the reset handlers' lines are there: {text}"
    );
    logs.assert_never_logged(&[&token, &session, PASSWORD, "the new secret"]);
}

#[tokio::test]
async fn a_password_change_voids_the_reset_links_sent_before_it() {
    let (server, mailer) = TestServer::start_mailing(|_| {}).await;
    let (session, _) = server.register("ada@example.com").await;
    reset(&server, "ada@example.com").await;
    let (_, token) = link_in(&mailer.sent()[0].text);
    let (s, _) = server
        .patch(
            "/api/me",
            Some(&session),
            json!({"password": "changed it myself", "current_password": PASSWORD}),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    // whoever reads the old mail cannot undo the change
    assert_eq!(
        confirm(&server, &token, "the thief's own").await.0,
        StatusCode::BAD_REQUEST
    );
    server.login("ada@example.com", "changed it myself").await;
    // a rename alone leaves a link alone
    reset(&server, "ada@example.com").await;
    let (_, token) = link_in(&mailer.sent()[1].text);
    let (s, _) = server
        .patch("/api/me", Some(&session), json!({"name": "Ada"}))
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        confirm(&server, &token, "a new one!").await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn an_expired_link_is_refused_and_a_disabled_accounts_link_is_used_up() {
    let mailer = Arc::new(MemoryMailer::new(true));
    let kept = mailer.clone();
    let server = TestServer::start_custom(
        |s| {
            s.smtp_host = "127.0.0.1".into();
            s.smtp_from = "collab@example.org".into();
            s.open_registration = true;
            s.reset_minutes = 10;
        },
        |s| {
            s.mailer = kept;
            s.clock = Clock::manual(T0);
        },
    )
    .await;
    server.register("ada@example.com").await;
    let (_, bob_id) = server.register("bob@example.com").await;

    reset(&server, "ada@example.com").await;
    let (_, token) = link_in(&mailer.sent()[0].text);
    assert!(mailer.sent()[0].text.contains("within 10 minutes"));
    server.state.clock.advance(600.5);
    assert_eq!(
        confirm(&server, &token, "too late now").await.0,
        StatusCode::BAD_REQUEST
    );
    server.login("ada@example.com", PASSWORD).await;

    reset(&server, "bob@example.com").await;
    let (_, token) = link_in(&mailer.sent()[1].text);
    server
        .sql(&format!(
            "UPDATE users SET disabled = TRUE WHERE id = '{bob_id}'"
        ))
        .await;
    assert_eq!(
        confirm(&server, &token, "bobs new one").await.0,
        StatusCode::BAD_REQUEST
    );
    let hash = oswx_server::security::token_hash(&token);
    assert!(
        server
            .state
            .store
            .password_reset_by_hash(&hash)
            .await
            .unwrap()
            .is_none(),
        "used up"
    );
    // And a disabled account is sent nothing.
    reset(&server, "bob@example.com").await;
    assert_eq!(mailer.sent().len(), 2);
}

#[tokio::test]
async fn hostile_reset_bodies_are_refused() {
    let (server, mailer) = TestServer::start_mailing(|_| {}).await;
    server.register("ada@example.com").await;
    for (path, body, want) in [
        (
            "/api/auth/password-reset",
            json!({"email": "a".repeat(321)}),
            422,
        ),
        ("/api/auth/password-reset", json!({}), 422),
        ("/api/auth/password-reset", json!({"email": 5}), 422),
        (
            "/api/auth/password-reset",
            json!({"email": "not an address"}),
            202,
        ),
        (
            "/api/auth/password-reset/confirm",
            json!({"token": "t".repeat(257), "password": "long enough"}),
            422,
        ),
        (
            "/api/auth/password-reset/confirm",
            json!({"token": "t", "password": "p".repeat(201)}),
            422,
        ),
        (
            "/api/auth/password-reset/confirm",
            json!({"token": "t"}),
            422,
        ),
        (
            "/api/auth/password-reset/confirm",
            json!({"token": "made-up", "password": "long enough"}),
            400,
        ),
        (
            "/api/auth/password-reset/confirm",
            json!({"token": "", "password": "long enough"}),
            400,
        ),
    ] {
        let (s, reply) = server.post(path, None, body.clone()).await;
        assert_eq!(s.as_u16(), want, "{path} {body} -> {reply}");
    }
    assert!(mailer.sent().is_empty());
    // A path that is not one page is not the page.
    let (s, body) = server.get("/reset-password/a/b", None).await;
    assert_eq!(
        (s, &body["detail"]),
        (StatusCode::NOT_FOUND, &json!("Not Found"))
    );
}

#[tokio::test]
async fn the_reset_page_is_locked_down() {
    let server = TestServer::start().await;
    let r = reqwest::get(server.url("/reset-password/sometoken"))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let h = r.headers().clone();
    assert_eq!(h["content-type"], "text/html; charset=utf-8");
    assert_eq!(h["cache-control"], "no-store");
    assert_eq!(h["referrer-policy"], "no-referrer");
    assert_eq!(h["x-content-type-options"], "nosniff");
    let csp = h["content-security-policy"].to_str().unwrap();
    for directive in [
        "default-src 'none'",
        "script-src 'sha256-",
        "connect-src 'self'",
        "form-action 'none'",
        "frame-ancestors 'none'",
        "base-uri 'none'",
    ] {
        assert!(csp.contains(directive), "{csp}");
    }
    let page = r.text().await.unwrap();
    assert!(page.contains("<meta name=referrer content=no-referrer>"));
    assert!(page.contains("/api/auth/password-reset/confirm"));
    assert!(!page.contains("sometoken"));
    for external in ["http://", "https://", "src="] {
        assert!(!page.contains(external), "self-contained: {external}");
    }
}

fn smtp_settings() -> Settings {
    Settings {
        smtp_host: "smtp.example.org".into(),
        smtp_port: 2525,
        smtp_security: SmtpSecurity::StartTls,
        smtp_user: "robot".into(),
        smtp_password: "pw".into(),
        smtp_from: "collab@example.org".into(),
        smtp_from_name: "openswx collaboration".into(),
        ..Settings::default()
    }
}

#[test]
fn the_smtp_mailer_is_configured_only_with_a_host_and_a_sender() {
    for security in [
        SmtpSecurity::StartTls,
        SmtpSecurity::Ssl,
        SmtpSecurity::Plain,
    ] {
        let s = Settings {
            smtp_security: security,
            ..smtp_settings()
        };
        assert!(SmtpMailer::new(&s).unwrap().configured(), "{security:?}");
    }
    for s in [
        Settings {
            smtp_host: String::new(),
            ..smtp_settings()
        },
        Settings {
            smtp_from: String::new(),
            ..smtp_settings()
        },
    ] {
        assert!(!SmtpMailer::new(&s).unwrap().configured());
    }
}

#[tokio::test]
async fn an_unconfigured_smtp_mailer_sends_nothing() {
    let mailer = SmtpMailer::new(&Settings::default()).unwrap();
    assert!(mailer.send("a@example.com", "s", "t", None).await.is_err());
}

#[test]
fn the_smtp_message_has_the_python_servers_headers_and_body() {
    let mailer = SmtpMailer::new(&smtp_settings()).unwrap();
    let text = "Someone asked.\n\nhttp://testserver/reset-password/abc\n";
    let message = mailer
        .message(
            "bob@example.com",
            "Reset your openswx collaboration password",
            text,
            None,
        )
        .unwrap();
    let raw = String::from_utf8(message.formatted()).unwrap();
    let headers = raw.split("\r\n\r\n").next().unwrap();
    assert!(
        headers.contains("From: \"openswx collaboration\" <collab@example.org>")
            || headers.contains("From: openswx collaboration <collab@example.org>"),
        "{headers}"
    );
    assert!(headers.contains("To: bob@example.com"), "{headers}");
    assert!(
        headers.contains("Subject: Reset your openswx collaboration password"),
        "{headers}"
    );
    let message_id = headers
        .lines()
        .find(|l| l.starts_with("Message-ID: "))
        .expect("a Message-ID");
    assert!(message_id.ends_with("@example.org>"), "{message_id}");
    assert!(
        headers.contains("Content-Type: text/plain; charset=utf-8"),
        "{headers}"
    );
    assert!(
        raw.contains("http://testserver/reset-password/abc"),
        "{raw}"
    );

    // Without a sender name, the bare address.
    let bare = SmtpMailer::new(&Settings {
        smtp_from_name: String::new(),
        ..smtp_settings()
    })
    .unwrap();
    let raw = String::from_utf8(
        bare.message("bob@example.com", "s", "t", None)
            .unwrap()
            .formatted(),
    )
    .unwrap();
    assert!(raw.contains("From: collab@example.org\r\n"), "{raw}");

    // With HTML, both alternatives.
    let raw = String::from_utf8(
        mailer
            .message(
                "bob@example.com",
                "s",
                "plain words",
                Some("<p>html words</p>"),
            )
            .unwrap()
            .formatted(),
    )
    .unwrap();
    assert!(raw.contains("multipart/alternative"), "{raw}");
    assert!(raw.contains("plain words") && raw.contains("<p>html words</p>"));

    // A recipient that is not an address is an error, not a panic.
    assert!(mailer.message("not an address", "s", "t", None).is_err());
}

#[tokio::test]
async fn the_production_state_opens_the_database_and_mails_through_smtp() {
    let dir = tempfile::TempDir::new().unwrap();
    let settings = Settings {
        database_url: format!(
            "sqlite://{}/collab.db",
            dir.path().to_string_lossy().replace('\\', "/")
        ),
        ..smtp_settings()
    };
    let state = AppState::from_settings(settings).await.unwrap();
    assert!(state.mailer.configured());
    assert_eq!(state.store.user_count().await.unwrap(), 0);
    let unconfigured = AppState::from_settings(Settings {
        smtp_host: String::new(),
        ..state.settings.as_ref().clone()
    })
    .await
    .unwrap();
    assert!(!unconfigured.mailer.configured());
    let postgres = AppState::from_settings(Settings {
        database_url: "postgres://u@localhost/db".into(),
        ..Settings::default()
    })
    .await;
    assert!(postgres.is_err());
}

// ------------------------------------------------------------------ Task 16, fix round 1

/// Two requests for a reset link at the same moment, both past the removal of older links before
/// either commits (on SQLite the second waits for the first's transaction instead): only the
/// newest link works, so one is left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_reset_requests_at_once_leave_one_link() {
    let mailer = Arc::new(MemoryMailer::new(true));
    let kept = mailer.clone();
    let (server, flaky) = TestServer::start_flaky_full(
        |s| {
            s.smtp_host = "127.0.0.1".into();
            s.smtp_from = "collab@example.org".into();
        },
        common::new_journal,
        |s| s.mailer = kept,
    )
    .await;
    server.register("ada@example.com").await;
    flaky.commit_gate.shut();
    let server = Arc::new(server);
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let server = server.clone();
        tasks.push(tokio::spawn(async move {
            server
                .post(
                    "/api/auth/password-reset",
                    None,
                    json!({"email": "ada@example.com"}),
                )
                .await
        }));
    }
    flaky.commit_gate.until_waiting(1).await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    flaky.commit_gate.open();
    for task in tasks {
        assert_eq!(task.await.unwrap().0, StatusCode::ACCEPTED);
    }
    let links: i64 = server.scalar("SELECT COUNT(*) FROM password_resets").await;
    assert_eq!(links, 1);
}
