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

// ------------------------------------------------------------------ the mail server's certificate

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

/// A certificate authority nobody knows but this test.
struct Authority {
    cert: rcgen::Certificate,
    key: rcgen::KeyPair,
}

impl Authority {
    fn new(name: &str) -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        let key = rcgen::KeyPair::generate().unwrap();
        Self {
            cert: params.self_signed(&key).unwrap(),
            key,
        }
    }

    /// A certificate for a server called `host`, and its key.
    fn issue(&self, host: &str) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
        let params = rcgen::CertificateParams::new(vec![host.to_owned()]).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.cert, &self.key).unwrap();
        (
            cert.der().clone(),
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
    }
}

type MailStream = BufStream<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>;

async fn say(stream: &mut MailStream, line: &str) -> std::io::Result<()> {
    stream.write_all(line.as_bytes()).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await
}

/// One SMTP conversation: every message is taken, and who it was for is added to `taken`.
async fn take_mail(mut stream: MailStream, taken: Arc<std::sync::Mutex<Vec<String>>>) {
    if say(&mut stream, "220 localhost ESMTP").await.is_err() {
        return;
    }
    let mut in_data = false;
    loop {
        let mut line = String::new();
        match stream.read_line(&mut line).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        let command = line.trim_end().to_ascii_uppercase();
        let answer = if in_data {
            if command != "." {
                continue;
            }
            in_data = false;
            "250 taken"
        } else if command.starts_with("RCPT TO:") {
            taken.lock().unwrap().push(line.trim_end()[8..].to_owned());
            "250 ok"
        } else if command == "DATA" {
            in_data = true;
            "354 go on"
        } else if command == "QUIT" {
            let _ = say(&mut stream, "221 bye").await;
            return;
        } else {
            "250 ok"
        };
        if say(&mut stream, answer).await.is_err() {
            return;
        }
    }
}

/// A mail server on this machine that speaks TLS from the first byte with a certificate `ca`
/// issued for `localhost`, and takes every message. Answers its port and who it took mail for.
async fn tls_mail_server(ca: &Authority) -> (u16, Arc<std::sync::Mutex<Vec<String>>>) {
    use tokio_rustls::rustls;
    let (cert, key) = ca.issue("localhost");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let recipients = Arc::new(std::sync::Mutex::new(Vec::new()));
    let taken = recipients.clone();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let (acceptor, taken) = (acceptor.clone(), taken.clone());
            tokio::spawn(async move {
                // (A client that does not trust the certificate ends the handshake.)
                if let Ok(tls) = acceptor.accept(socket).await {
                    take_mail(BufStream::new(tls), taken).await;
                }
            });
        }
    });
    (port, recipients)
}

const SMTP_PORT_ENV: &str = "OSWX_TEST_SMTP_PORT";
const SMTP_HOST_ENV: &str = "OSWX_TEST_SMTP_HOST";

/// Run by the test below in a process of its own (the machine's trust store is read from the
/// process's environment): sends one message through the real mailer to the mail server the
/// environment names, and prints how that went.
#[tokio::test]
#[ignore = "run by a_mail_server_is_trusted_by_the_machines_trust_store, in a process of its own"]
async fn send_one_message_to_the_mail_server_in_the_environment() {
    let (Ok(port), Ok(host)) = (std::env::var(SMTP_PORT_ENV), std::env::var(SMTP_HOST_ENV)) else {
        return;
    };
    let settings = Settings {
        smtp_host: host,
        smtp_port: port.parse().unwrap(),
        smtp_security: SmtpSecurity::Ssl,
        smtp_user: String::new(),
        ..smtp_settings()
    };
    let mailer = SmtpMailer::new(&settings).unwrap();
    match mailer
        .send("bob@example.com", "Hello", "A message.\n", None)
        .await
    {
        Ok(()) => println!("MAIL-WAS-SENT"),
        Err(e) => println!("MAIL-WAS-REFUSED: {e:#}"),
    }
}

/// Sends one message in a process whose trust store is the PEM file `trusted`; answers what the
/// process printed.
async fn send_trusting(trusted: &std::path::Path, host: &str, port: u16) -> String {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "send_one_message_to_the_mail_server_in_the_environment",
            "--ignored",
            "--nocapture",
        ])
        .env("SSL_CERT_FILE", trusted)
        .env_remove("SSL_CERT_DIR")
        .env(SMTP_PORT_ENV, port.to_string())
        .env(SMTP_HOST_ENV, host);
    let output = tokio::task::spawn_blocking(move || command.output())
        .await
        .unwrap()
        .expect("the test binary runs again");
    assert!(output.status.success(), "{output:?}");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[tokio::test]
async fn a_mail_server_is_trusted_by_the_machines_trust_store() {
    let dir = tempfile::TempDir::new().unwrap();
    let (ours, theirs) = (Authority::new("Our own CA"), Authority::new("Another CA"));
    let (our_pem, their_pem) = (dir.path().join("ours.pem"), dir.path().join("theirs.pem"));
    std::fs::write(&our_pem, ours.cert.pem()).unwrap();
    std::fs::write(&their_pem, theirs.cert.pem()).unwrap();
    let (port, recipients) = tls_mail_server(&ours).await;

    // The relay's certificate comes from a CA in the machine's trust store: mail goes out.
    let said = send_trusting(&our_pem, "localhost", port).await;
    assert!(said.contains("MAIL-WAS-SENT"), "{said}");
    assert_eq!(*recipients.lock().unwrap(), ["<bob@example.com>"]);

    // From a CA the machine does not trust: refused, and nothing is sent.
    let said = send_trusting(&their_pem, "localhost", port).await;
    assert!(said.contains("MAIL-WAS-REFUSED"), "{said}");
    // A trusted CA, but the certificate is for another name than the one connected to.
    let said = send_trusting(&our_pem, "127.0.0.1", port).await;
    assert!(said.contains("MAIL-WAS-REFUSED"), "{said}");
    assert_eq!(recipients.lock().unwrap().len(), 1, "{said}");
}

// ------------------------------------------------------------------ a slow mail server

/// A mail server that takes its time over every message, then takes it.
struct SlowMailer {
    taken: MemoryMailer,
    delay: std::time::Duration,
}

#[async_trait]
impl Mailer for SlowMailer {
    fn configured(&self) -> bool {
        true
    }

    async fn send(
        &self,
        to: &str,
        subject: &str,
        text: &str,
        html: Option<&str>,
    ) -> anyhow::Result<()> {
        tokio::time::sleep(self.delay).await;
        self.taken.send(to, subject, text, html).await
    }
}

/// A server whose mail goes through `mailer`, whose requests wait `wait` for their mail, and
/// which sends at most `at_once` at a time.
async fn mailing_server(
    mailer: Arc<dyn Mailer>,
    wait: std::time::Duration,
    at_once: usize,
) -> TestServer {
    let hooks = oswx_server::room::Hooks {
        mail_wait: wait,
        ..Default::default()
    };
    TestServer::start_custom(
        |s| {
            s.smtp_host = "127.0.0.1".into();
            s.smtp_from = "collab@example.org".into();
            s.max_mail_sends = at_once;
        },
        |s| {
            s.mailer = mailer;
            s.room_hooks = Arc::new(hooks);
        },
    )
    .await
}

async fn invite(server: &TestServer, world: &common::World, email: &str) -> Value {
    let (status, invite) = server
        .post(
            &format!("/api/orgs/{}/invites", world.org),
            Some(&world.admin),
            json!({"email": email}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{invite}");
    invite
}

async fn sent_at_least(mailer: &MemoryMailer, n: usize) {
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_secs(20);
    while mailer.sent().len() < n {
        assert!(Instant::now() < deadline, "the mail never went");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_slow_mail_server_does_not_hold_a_request_up_and_the_mail_still_goes() {
    use std::time::{Duration, Instant};
    let logs = LogCapture::start();
    // The mail server takes three seconds; a request waits 200 ms for its mail, not the ten
    // seconds of a real server.
    let mailer = Arc::new(SlowMailer {
        taken: MemoryMailer::new(true),
        delay: Duration::from_secs(3),
    });
    let server = mailing_server(mailer.clone(), Duration::from_millis(200), 16).await;
    let world = common::World::new(&server).await.unwrap();

    // A reset link: the answer is the same as ever, and comes without the mail server.
    let started = Instant::now();
    assert_eq!(
        reset(&server, "ada@example.com").await.0,
        StatusCode::ACCEPTED
    );
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(150) && took < Duration::from_millis(2500),
        "the request took {took:?}"
    );
    assert!(mailer.taken.sent().is_empty());
    // The mail is on its way, counted apart from the steps after commits (which are done).
    assert!(server.state.mail.sending() >= 1);
    assert_eq!(server.state.after_commits.running(), 0);
    assert!(
        logs.text()
            .contains("password reset link handed on; the mail server has not answered yet"),
        "{}",
        logs.text()
    );

    // An invite: made, with its link, and `emailed` says only what is known by now.
    let started = Instant::now();
    let invite = invite(&server, &world, "bob@example.com").await;
    assert!(started.elapsed() < Duration::from_millis(2500));
    assert_eq!(invite["emailed"], false, "{invite}");
    assert!(invite["url"].as_str().unwrap().contains("/invite/"));
    assert!(server.state.mail.sending() >= 1);

    // Both messages are still handed over, each when the mail server is done with it.
    sent_at_least(&mailer.taken, 2).await;
    let sent = mailer.taken.sent();
    assert_eq!(sent[0].to, "ada@example.com");
    assert_eq!(sent[1].to, "bob@example.com");
    assert!(sent[1].text.contains(invite["url"].as_str().unwrap()));
    // One invite was made, not two: nothing told the client to try again.
    let invites: i64 = server.scalar("SELECT COUNT(*) FROM invites").await;
    assert_eq!(invites, 1);
    assert_eq!(server.state.http.request_timeouts(), 0);
    let deadline = Instant::now() + Duration::from_secs(20);
    while server.state.mail.sending() > 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn only_so_many_emails_are_on_their_way_at_once() {
    use std::time::{Duration, Instant};
    let logs = LogCapture::start();
    let before = oswx_server::metrics::counters();
    let mailer = Arc::new(SlowMailer {
        taken: MemoryMailer::new(true),
        delay: Duration::from_secs(2),
    });
    let server = mailing_server(mailer.clone(), Duration::from_millis(100), 1).await;
    let world = common::World::new(&server).await.unwrap();
    // One message on its way takes the only place.
    assert_eq!(
        reset(&server, "ada@example.com").await.0,
        StatusCode::ACCEPTED
    );
    assert_eq!(server.state.mail.sending(), 1);
    // The next is not sent, and the request hears so at once, without waiting.
    let started = Instant::now();
    let refused = invite(&server, &world, "bob@example.com").await;
    assert_eq!(refused["emailed"], false, "{refused}");
    assert!(started.elapsed() < Duration::from_secs(1));
    let after = oswx_server::metrics::counters();
    assert!(after.mail_refused > before.mail_refused);
    assert!(
        logs.text().contains("an email was not sent"),
        "{}",
        logs.text()
    );
    assert!(!logs.text().contains("bob@example.com"), "{}", logs.text());
    // The first goes; the second never does.
    sent_at_least(&mailer.taken, 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let sent = mailer.taken.sent();
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(sent[0].to, "ada@example.com");
    // Its place is free again: the next invite's mail goes.
    let deadline = Instant::now() + Duration::from_secs(20);
    while server.state.mail.sending() > 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    invite(&server, &world, "cy@example.com").await;
    sent_at_least(&mailer.taken, 2).await;
    assert_eq!(mailer.taken.sent()[1].to, "cy@example.com");
}

/// A mailer that panics.
struct PanickingMailer;

#[async_trait]
impl Mailer for PanickingMailer {
    fn configured(&self) -> bool {
        true
    }

    async fn send(&self, _: &str, _: &str, _: &str, _: Option<&str>) -> anyhow::Result<()> {
        panic!("a mailer that breaks")
    }
}

/// A mail server that refuses, quoting the address.
struct QuotingMailer;

#[async_trait]
impl Mailer for QuotingMailer {
    fn configured(&self) -> bool {
        true
    }

    async fn send(&self, to: &str, _: &str, _: &str, _: Option<&str>) -> anyhow::Result<()> {
        anyhow::bail!(
            "permanent error (550): <{}>: no such user",
            to.to_uppercase()
        )
    }
}

#[tokio::test]
async fn a_send_that_panics_or_is_refused_is_a_failure_and_the_log_says_what_happened() {
    use std::time::Duration;
    let logs = LogCapture::start();
    let before = oswx_server::metrics::counters();
    let server = mailing_server(Arc::new(PanickingMailer), Duration::from_secs(5), 4).await;
    server.register("ada@example.com").await;
    assert_eq!(
        reset(&server, "ada@example.com").await.0,
        StatusCode::ACCEPTED
    );
    let after = oswx_server::metrics::counters();
    assert!(after.mail_failures > before.mail_failures);
    let said = logs.text();
    assert!(said.contains("the mailer panicked"), "{said}");
    assert!(
        said.contains("password reset link not sent: the mail server did not take it"),
        "{said}"
    );
    assert!(!said.contains("password reset link sent"), "{said}");
    assert_eq!(server.state.mail.sending(), 0);

    // A refusal that quotes the address is logged without it.
    let server = mailing_server(Arc::new(QuotingMailer), Duration::from_secs(5), 4).await;
    server.register("zed.private@example.com").await;
    reset(&server, "zed.private@example.com").await;
    let said = logs.text();
    assert!(said.contains("permanent error (550)"), "{said}");
    assert!(
        !said.to_lowercase().contains("zed.private@example.com"),
        "{said}"
    );
}

#[tokio::test]
async fn a_stop_does_not_wait_for_the_mail_server_before_the_journal() {
    use std::time::{Duration, Instant};
    // Requests would wait 30 s for their mail; the mail server takes 20.
    let mailer = Arc::new(SlowMailer {
        taken: MemoryMailer::new(true),
        delay: Duration::from_secs(20),
    });
    let server = mailing_server(mailer.clone(), Duration::from_secs(30), 4).await;
    server.register("ada@example.com").await;
    let request = {
        let (base, client) = (server.base.clone(), reqwest::Client::new());
        tokio::spawn(async move {
            client
                .post(format!("{base}/api/auth/password-reset"))
                .json(&json!({"email": "ada@example.com"}))
                .send()
                .await
                .map(|r| r.status())
        })
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    while server.state.mail.sending() == 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(server.state.after_commits.running(), 1);
    // The stop: the request stops waiting at once and is answered; the journal is written out;
    // the mail on its way is waited for a tenth of the bound, and left.
    let started = Instant::now();
    server.state.shutdown_within(Duration::from_secs(8)).await;
    let took = started.elapsed();
    assert!(took < Duration::from_secs(3), "the stop took {took:?}");
    assert!(!server.state.journal.healthy());
    assert_eq!(server.state.after_commits.running(), 0);
    let answered = tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .expect("the request was answered")
        .unwrap();
    assert_eq!(answered.unwrap(), StatusCode::ACCEPTED);
    assert!(mailer.taken.sent().is_empty());
}

#[tokio::test]
async fn mail_that_was_sent_and_mail_that_was_not_are_counted() {
    let before = oswx_server::metrics::counters();
    let (server, mailer) = TestServer::start_mailing(|_| {}).await;
    server.register("ada@example.com").await;
    reset(&server, "ada@example.com").await;
    assert_eq!(mailer.sent().len(), 1);
    let broken = TestServer::start_custom(
        |s| {
            s.smtp_host = "mail.invalid".into();
            s.smtp_from = "c@example.org".into();
        },
        |s| s.mailer = Arc::new(BrokenMailer),
    )
    .await;
    broken.register("ada@example.com").await;
    reset(&broken, "ada@example.com").await;
    let after = oswx_server::metrics::counters();
    assert!(after.mail_sent > before.mail_sent);
    assert!(after.mail_failures > before.mail_failures);
}
