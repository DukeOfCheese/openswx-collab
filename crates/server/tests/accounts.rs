//! Accounts: registration, sign-in, sessions and the current user (the Python `test_accounts.py`,
//! without the organisation, invite and OAuth tests, which come with Tasks 5 and 14).

mod common;

use common::{LogCapture, PASSWORD, TestServer};
use oswx_server::Clock;
use oswx_server::sessions::TRUST_SECS;
use reqwest::StatusCode;
use serde_json::{Value, json};

/// A start time for manual clocks.
const T0: f64 = 1_800_000_000.0;

async fn start_at_t0(settings: impl FnOnce(&mut oswx_server::Settings)) -> TestServer {
    TestServer::start_custom(settings, |s| s.clock = Clock::manual(T0)).await
}

fn status(r: (StatusCode, Value)) -> StatusCode {
    r.0
}

async fn login_status(server: &TestServer, email: &str, password: &str) -> StatusCode {
    status(
        server
            .post(
                "/api/auth/login",
                None,
                json!({"email": email, "password": password}),
            )
            .await,
    )
}

#[tokio::test]
async fn test_first_account_is_admin_then_invite_only() {
    let server = TestServer::start().await;
    assert_eq!(
        server.get("/api/server", None).await.1["registration"],
        "first_user"
    );
    let (admin, admin_id) = server.register("ada@example.com").await;
    let (s, me) = server.get("/api/me", Some(&admin)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        me,
        json!({"id": admin_id, "email": "ada@example.com", "name": "ada", "is_server_admin": true})
    );
    assert_eq!(
        server.get("/api/server", None).await.1["registration"],
        "invite"
    );
    let (s, body) = server
        .post(
            "/api/auth/register",
            None,
            json!({"email": "eve@example.com", "password": "password1", "name": "Eve"}),
        )
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(
        body["detail"],
        "this server is invite-only: ask an organisation owner for an invite link"
    );
    // An invite nobody issued is no invite.
    let (s, _) = server
        .post(
            "/api/auth/register",
            None,
            json!({"email": "eve@example.com", "password": "password1", "invite": "made-up"}),
        )
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(
        server.get("/api/me", None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        server.get("/api/me", Some("nonsense")).await.0,
        StatusCode::UNAUTHORIZED
    );

    // What was stored: the operator's own account counts as confirmed; only the hash is kept.
    let user = server
        .state
        .store
        .user_by_email("ada@example.com")
        .await
        .unwrap()
        .unwrap();
    assert!(user.is_server_admin && user.email_verified && user.active());
    assert!(user.password_hash.unwrap().starts_with("scrypt$32768$8$1$"));
    assert_eq!(server.state.store.user_count().await.unwrap(), 1);
}

#[tokio::test]
async fn test_login_logout_and_password_rules() {
    let server = TestServer::start().await;
    let (token, _) = server
        .register_with("Ada@Example.com", "correct horse")
        .await;
    let (s, body) = server
        .post(
            "/api/auth/register",
            None,
            json!({"email": "ada@example.com", "password": "correct horse"}),
        )
        .await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert_eq!(
        body["detail"],
        "there is already an account with this email address"
    );
    let (s, _) = server
        .post(
            "/api/auth/register",
            None,
            json!({"email": "x@example.com", "password": "short"}),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    let (s, body) = server
        .post(
            "/api/auth/login",
            None,
            json!({"email": "ada@example.com", "password": "wrong"}),
        )
        .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    assert_eq!(body["detail"], "wrong email address or password");
    let t2 = server.login("ADA@example.com", "correct horse").await;
    assert_eq!(
        server
            .call(reqwest::Method::POST, "/api/auth/logout", Some(&t2), None)
            .await,
        (StatusCode::NO_CONTENT, Value::Null)
    );
    assert_eq!(
        server.get("/api/me", Some(&t2)).await.0,
        StatusCode::UNAUTHORIZED
    );
    // Changing the password ends the other sessions.
    let t3 = server.login("ada@example.com", "correct horse").await;
    let (s, body) = server
        .patch(
            "/api/me",
            Some(&token),
            json!({"password": "new password", "current_password": "nope"}),
        )
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(body["detail"], "the current password is wrong");
    let (s, _) = server
        .patch(
            "/api/me",
            Some(&token),
            json!({"password": "new password", "current_password": "correct horse"}),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        server.get("/api/me", Some(&t3)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(server.get("/api/me", Some(&token)).await.0, StatusCode::OK);
    assert_eq!(
        login_status(&server, "ada@example.com", "correct horse").await,
        StatusCode::UNAUTHORIZED
    );
    server.login("ada@example.com", "new password").await;
}

#[tokio::test]
async fn test_login_is_rate_limited() {
    let server = TestServer::start().await;
    server.register("ada@example.com").await;
    let mut codes = Vec::new();
    for _ in 0..12 {
        codes.push(login_status(&server, "ada@example.com", "wrong").await);
    }
    assert_eq!(codes[..10], [StatusCode::UNAUTHORIZED; 10]);
    assert_eq!(codes[10..], [StatusCode::TOO_MANY_REQUESTS; 2]);
    // Even the right password, while blocked.
    let (s, body) = server
        .post(
            "/api/auth/login",
            None,
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        body["detail"],
        "too many attempts; try again in a few minutes"
    );
}

#[tokio::test]
async fn test_open_registration() {
    // (Creating an organisation as a non-admin is checked with organisations, Task 5.)
    let server = TestServer::start_with(|s| s.open_registration = true).await;
    server.register("ada@example.com").await;
    assert_eq!(
        server.get("/api/server", None).await.1["registration"],
        "open"
    );
    let (bob, _) = server.register("bob@example.com").await;
    assert_eq!(
        server.get("/api/me", Some(&bob)).await.1["is_server_admin"],
        false
    );
    let stored = server
        .state
        .store
        .user_by_email("bob@example.com")
        .await
        .unwrap()
        .unwrap();
    assert!(!stored.is_server_admin && !stored.email_verified);
}

#[tokio::test]
async fn sign_in_limits_are_per_address_and_per_client() {
    let server = TestServer::start_with(|s| s.open_registration = true).await;
    server.register("ada@example.com").await;
    server
        .register_with("bob@example.com", "bobs password")
        .await;
    let http = reqwest::Client::new();
    let attempt = |email: &'static str, forwarded: Option<&'static str>| {
        let mut r = http
            .post(server.url("/api/auth/login"))
            .json(&json!({"email": email, "password": "wrong"}));
        if let Some(f) = forwarded {
            r = r.header("x-forwarded-for", f);
        }
        async move { r.send().await.unwrap().status() }
    };
    for _ in 0..10 {
        assert_eq!(
            attempt("ada@example.com", None).await,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        attempt("ada@example.com", None).await,
        StatusCode::TOO_MANY_REQUESTS
    );
    // The same address typed differently is the same key.
    assert_eq!(
        attempt(" ADA@example.com ", None).await,
        StatusCode::TOO_MANY_REQUESTS
    );
    // Another address from the same client is not blocked.
    assert_eq!(
        attempt("bob@example.com", None).await,
        StatusCode::UNAUTHORIZED
    );
    // The tests connect from 127.0.0.1, which counts as a proxy on this machine: the client it names
    // is another client.
    assert_eq!(
        attempt("ada@example.com", Some("203.0.113.9")).await,
        StatusCode::UNAUTHORIZED
    );
    // A success clears the key.
    server.login("bob@example.com", "bobs password").await;
    for _ in 0..9 {
        assert_eq!(
            attempt("bob@example.com", None).await,
            StatusCode::UNAUTHORIZED
        );
    }
}

#[tokio::test]
async fn a_401_for_a_missing_malformed_unknown_or_expired_token() {
    let server = start_at_t0(|s| s.session_days = 1).await;
    let (token, _) = server.register("ada@example.com").await;
    let http = reqwest::Client::new();
    for auth in [
        None,
        Some(String::new()),
        Some("Bearer".to_owned()),
        Some("Bearer    ".to_owned()),
        Some(format!("Basic {token}")),
        Some(format!("Bearer{token}")),
        Some(format!("Token {token}")),
        Some("Bearer not-a-token".to_owned()),
        Some(format!("Bearer {}", "x".repeat(4000))),
    ] {
        let mut r = http.get(server.url("/api/me"));
        if let Some(a) = &auth {
            r = r.header("authorization", a);
        }
        let r = r.send().await.unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED, "{auth:?}");
        assert_eq!(
            r.json::<Value>().await.unwrap(),
            json!({"detail": "sign in first"})
        );
    }
    // The scheme in any case, the token trimmed.
    let r = http
        .get(server.url("/api/me"))
        .header("authorization", format!("bEaReR   {token}  "))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    // Expired: a day later, to the second.
    server.state.clock.advance(86400.0);
    assert_eq!(server.get("/api/me", Some(&token)).await.0, StatusCode::OK);
    server.state.clock.advance(0.5);
    assert_eq!(
        server.get("/api/me", Some(&token)).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn a_revoked_session_stops_at_once_and_an_expired_one_at_its_expiry() {
    let server = start_at_t0(|_| {}).await;
    let (token, _) = server.register("ada@example.com").await;
    let other = server.login("ada@example.com", PASSWORD).await;
    // Both are cached now.
    assert_eq!(server.get("/api/me", Some(&token)).await.0, StatusCode::OK);
    assert_eq!(server.get("/api/me", Some(&other)).await.0, StatusCode::OK);
    assert!(server.state.sessions.len() >= 2);
    // Logout: gone at once, although the cache would have trusted it for another 30 s.
    server
        .call(
            reqwest::Method::POST,
            "/api/auth/logout",
            Some(&other),
            None,
        )
        .await;
    assert_eq!(
        server.get("/api/me", Some(&other)).await.0,
        StatusCode::UNAUTHORIZED
    );
    // The row is gone too.
    let hash = oswx_server::security::token_hash(&other);
    assert!(
        server
            .state
            .store
            .session_by_hash(&hash)
            .await
            .unwrap()
            .is_none()
    );
    // Expiry: 30 days, checked on every request, cached or not.
    server.state.clock.advance(30.0 * 86400.0 - 1.0);
    assert_eq!(server.get("/api/me", Some(&token)).await.0, StatusCode::OK);
    server.state.clock.advance(2.0);
    assert_eq!(
        server.get("/api/me", Some(&token)).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn a_cached_session_is_trusted_for_at_most_30_seconds() {
    let server = start_at_t0(|_| {}).await;
    let (token, id) = server.register("ada@example.com").await;
    assert_eq!(server.get("/api/me", Some(&token)).await.0, StatusCode::OK);
    // Ended behind this node's back (as another node would): the cache still trusts it a while.
    server
        .sql(&format!("DELETE FROM sessions WHERE user_id = '{id}'"))
        .await;
    server.state.clock.advance(TRUST_SECS - 1.0);
    assert_eq!(server.get("/api/me", Some(&token)).await.0, StatusCode::OK);
    server.state.clock.advance(1.0);
    assert_eq!(
        server.get("/api/me", Some(&token)).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn a_disabled_or_deleted_account_cannot_sign_in_or_use_its_sessions() {
    let server = start_at_t0(|s| s.open_registration = true).await;
    server.register("ada@example.com").await;
    let (bob, bob_id) = server.register("bob@example.com").await;
    let (cy, cy_id) = server.register("cy@example.com").await;
    assert_eq!(server.get("/api/me", Some(&bob)).await.0, StatusCode::OK);
    assert_eq!(server.get("/api/me", Some(&cy)).await.0, StatusCode::OK);

    server
        .sql(&format!(
            "UPDATE users SET disabled = TRUE WHERE id = '{bob_id}'"
        ))
        .await;
    server
        .sql(&format!(
            "UPDATE users SET deleted_at = {T0} WHERE id = '{cy_id}'"
        ))
        .await;
    // Whoever disables an account forgets its sessions (the lifecycle routes do); without that, the
    // cache notices within 30 s.
    server.state.sessions.forget_user(&bob_id);
    assert_eq!(
        server.get("/api/me", Some(&bob)).await.0,
        StatusCode::UNAUTHORIZED
    );
    server.state.clock.advance(TRUST_SECS);
    assert_eq!(
        server.get("/api/me", Some(&cy)).await.0,
        StatusCode::UNAUTHORIZED
    );
    // Neither signs in, with the same answer as a wrong password.
    for email in ["bob@example.com", "cy@example.com"] {
        let (s, body) = server
            .post(
                "/api/auth/login",
                None,
                json!({"email": email, "password": PASSWORD}),
            )
            .await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        assert_eq!(body["detail"], "wrong email address or password");
    }
}

#[tokio::test]
async fn an_account_without_a_password_cannot_sign_in_with_one() {
    let server = TestServer::start().await;
    let (_, id) = server.register("ada@example.com").await;
    server
        .sql(&format!(
            "UPDATE users SET password_hash = NULL WHERE id = '{id}'"
        ))
        .await;
    for password in [PASSWORD, "", "anything at all"] {
        assert_eq!(
            login_status(&server, "ada@example.com", password).await,
            StatusCode::UNAUTHORIZED
        );
    }
}

#[tokio::test]
async fn logout_ends_only_the_session_it_was_sent_with() {
    let server = TestServer::start().await;
    let (a, _) = server.register("ada@example.com").await;
    let b = server.login("ada@example.com", PASSWORD).await;
    let (s, _) = server
        .call(reqwest::Method::POST, "/api/auth/logout", Some(&b), None)
        .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    assert_eq!(server.get("/api/me", Some(&a)).await.0, StatusCode::OK);
    // A second logout with the same token is a 401: it is no longer a session.
    let (s, _) = server
        .call(reqwest::Method::POST, "/api/auth/logout", Some(&b), None)
        .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    let (s, _) = server
        .call(reqwest::Method::POST, "/api/auth/logout", None, None)
        .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn sessions_are_stored_hashed_with_their_label_and_lifetime() {
    let server = start_at_t0(|s| s.session_days = 2).await;
    server.register("ada@example.com").await;
    let (s, body) = server
        .post(
            "/api/auth/login",
            None,
            json!({"email": "ada@example.com", "password": PASSWORD, "label": "laptop ".repeat(100)}),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    let token = body["token"].as_str().unwrap();
    assert_eq!(token.len(), 43);
    let stored = server
        .state
        .store
        .session_by_hash(&oswx_server::security::token_hash(token))
        .await
        .unwrap()
        .expect("the session is stored under the token's hash");
    assert_eq!(stored.created_at, T0);
    assert_eq!(stored.expires_at, T0 + 2.0 * 86400.0);
    assert_eq!(stored.label.chars().count(), 200, "cut to 200");
    assert_eq!(body["user"]["email"], "ada@example.com");
}

#[tokio::test]
async fn two_first_registrations_at_once_make_one_admin() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let mut tasks = Vec::new();
    for i in 0..4 {
        let r = http
            .post(server.url("/api/auth/register"))
            .json(&json!({"email": format!("p{i}@example.com"), "password": PASSWORD}));
        tasks.push(tokio::spawn(
            async move { r.send().await.unwrap().status() },
        ));
    }
    let mut codes = Vec::new();
    for t in tasks {
        codes.push(t.await.unwrap());
    }
    codes.sort();
    assert_eq!(
        codes,
        [
            StatusCode::CREATED,
            StatusCode::FORBIDDEN,
            StatusCode::FORBIDDEN,
            StatusCode::FORBIDDEN
        ]
    );
    assert_eq!(server.state.store.user_count().await.unwrap(), 1);
}

#[tokio::test]
async fn hostile_registration_bodies_are_refused() {
    let server = TestServer::start().await;
    let long = |n: usize| "a".repeat(n);
    let email_321 = format!("{}@example.com", long(321 - 12));
    for (body, want) in [
        (json!({"email": email_321, "password": PASSWORD}), 422),
        (
            json!({"email": "a@example.com", "password": long(201)}),
            422,
        ),
        (
            json!({"email": "a@example.com", "password": "seven77"}),
            422,
        ),
        (
            json!({"email": "a@example.com", "password": PASSWORD, "name": long(201)}),
            422,
        ),
        (
            json!({"email": "a@example.com", "password": PASSWORD, "invite": long(257)}),
            422,
        ),
        (
            json!({"email": "a@example.com", "password": PASSWORD, "name": null}),
            422,
        ),
        (json!({"email": "a@example.com", "password": 12345678}), 422),
        (
            json!({"email": ["a@example.com"], "password": PASSWORD}),
            422,
        ),
        (json!({"password": PASSWORD}), 422),
        (
            json!({"email": "not an address", "password": PASSWORD}),
            422,
        ),
        (json!({"email": "a@b.c", "password": PASSWORD}), 422),
        (json!([1, 2]), 422),
    ] {
        let (s, reply) = server.post("/api/auth/register", None, body.clone()).await;
        assert_eq!(s.as_u16(), want, "{body} -> {reply}");
        assert!(reply["detail"].is_string(), "{reply}");
    }
    let (s, reply) = server
        .post(
            "/api/auth/register",
            None,
            json!({"email": "x", "password": PASSWORD}),
        )
        .await;
    assert_eq!(
        (s, &reply["detail"]),
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            &json!("not an email address")
        )
    );
    // Not JSON at all.
    let r = reqwest::Client::new()
        .post(server.url("/api/auth/register"))
        .header("content-type", "application/json")
        .body("{\"email\": ")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert!(r.json::<Value>().await.unwrap()["detail"].is_string());
    assert_eq!(server.state.store.user_count().await.unwrap(), 0);

    // At the limits, and with fields nobody asked for, it works; the name is trimmed, and a blank
    // one becomes the address's local part.
    let email_320 = format!("{}@{}.cccc", long(64), "b".repeat(250));
    assert_eq!(email_320.len(), 320);
    let (s, reply) = server
        .post(
            "/api/auth/register",
            None,
            json!({"email": email_320, "password": long(200), "name": format!("  {}  ", long(196)),
                   "is_server_admin": false, "id": "0000"}),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{reply}");
    assert_eq!(reply["user"]["name"], long(196));
    assert_ne!(reply["user"]["id"], "0000");
    let server = TestServer::start_with(|s| s.open_registration = true).await;
    server.register("first@example.com").await;
    let (_, reply) = server
        .post(
            "/api/auth/register",
            None,
            json!({"email": "  Bob@Example.COM ", "password": PASSWORD, "name": "   "}),
        )
        .await;
    assert_eq!(reply["user"]["name"], "bob");
    assert_eq!(reply["user"]["email"], "bob@example.com");
}

#[tokio::test]
async fn hostile_sign_in_bodies_are_refused() {
    let server = TestServer::start().await;
    server.register("ada@example.com").await;
    for (body, want) in [
        (json!({"email": "a".repeat(321), "password": PASSWORD}), 422),
        (
            json!({"email": "ada@example.com", "password": "p".repeat(1025)}),
            422,
        ),
        (json!({"email": "ada@example.com"}), 422),
        (
            json!({"email": "ada@example.com", "password": PASSWORD, "label": 5}),
            422,
        ),
        (json!({"email": "ada@example.com", "password": null}), 422),
        // An address that is not one, or nobody's, is a wrong password.
        (
            json!({"email": "not an address", "password": PASSWORD}),
            401,
        ),
        (json!({"email": "", "password": PASSWORD}), 401),
        (
            json!({"email": "eve@example.com", "password": PASSWORD}),
            401,
        ),
        (json!({"email": "ada@example.com", "password": ""}), 401),
        (
            json!({"email": "ada@example.com", "password": "p".repeat(1024)}),
            401,
        ),
    ] {
        let (s, reply) = server.post("/api/auth/login", None, body.clone()).await;
        assert_eq!(s.as_u16(), want, "{body} -> {reply}");
    }
    // Fields nobody asked for are ignored.
    let (s, _) = server
        .post(
            "/api/auth/login",
            None,
            json!({"email": "ada@example.com", "password": PASSWORD, "admin": true}),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test]
async fn the_name_and_password_change_by_patch() {
    let server = TestServer::start().await;
    let (token, id) = server.register("ada@example.com").await;
    let (s, me) = server
        .patch("/api/me", Some(&token), json!({"name": "  Ada Lovelace  "}))
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(me["name"], "Ada Lovelace");
    assert_eq!(
        server
            .state
            .store
            .user_by_id(&id)
            .await
            .unwrap()
            .unwrap()
            .name,
        "Ada Lovelace"
    );
    // Nothing to change is fine.
    let (s, me) = server.patch("/api/me", Some(&token), json!({})).await;
    assert_eq!((s, &me["name"]), (StatusCode::OK, &json!("Ada Lovelace")));
    for body in [
        json!({"name": ""}),
        json!({"name": "   "}),
        json!({"name": "n".repeat(201)}),
        json!({"name": 7}),
        json!({"password": "short"}),
        json!({"password": "p".repeat(201), "current_password": PASSWORD}),
        json!({"password": "long enough", "current_password": "c".repeat(1025)}),
    ] {
        let (s, reply) = server.patch("/api/me", Some(&token), body.clone()).await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body} -> {reply}");
    }
    // A password change needs the current password, and nothing changes without it (not even the
    // name sent with it).
    let (s, _) = server
        .patch(
            "/api/me",
            Some(&token),
            json!({"name": "Eve", "password": "long enough"}),
        )
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(
        server.get("/api/me", Some(&token)).await.1["name"],
        "Ada Lovelace"
    );
    assert_eq!(
        server
            .patch("/api/me", None, json!({"name": "Eve"}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn an_account_without_a_password_sets_one_only_soon_after_signing_in() {
    let server = start_at_t0(|_| {}).await;
    let (token, id) = server.register("ada@example.com").await;
    server
        .sql(&format!(
            "UPDATE users SET password_hash = NULL WHERE id = '{id}'"
        ))
        .await;
    server.state.clock.advance(601.0);
    let (s, body) = server
        .patch("/api/me", Some(&token), json!({"password": "a first one"}))
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(
        body["detail"],
        "sign in again first (this needs a sign-in from the last ten minutes)"
    );
    // A fresh session (made here directly: this account has no way to sign in yet).
    let fresh = "a-fresh-session-token";
    server
        .sql(&format!(
            "INSERT INTO sessions (token_hash, user_id, created_at, expires_at, label) \
             VALUES ('{}', '{id}', {}, {}, '')",
            oswx_server::security::token_hash(fresh),
            T0 + 601.0,
            T0 + 86400.0
        ))
        .await;
    let (s, _) = server
        .patch("/api/me", Some(fresh), json!({"password": "a first one"}))
        .await;
    assert_eq!(s, StatusCode::OK);
    server.login("ada@example.com", "a first one").await;
}

#[tokio::test]
async fn passwords_and_session_tokens_are_never_logged() {
    let logs = LogCapture::start();
    let server = TestServer::start().await;
    let secret = "a very secret password";
    let (token, id) = server.register_with("ada@example.com", secret).await;
    let second = server.login("ada@example.com", secret).await;
    login_status(&server, "ada@example.com", "a wrong password").await;
    server
        .patch(
            "/api/me",
            Some(&second),
            json!({"password": "the next secret", "current_password": secret}),
        )
        .await;
    server.get("/api/me", Some(&second)).await;
    assert!(
        logs.text().contains(&id),
        "the capture works: registration is logged: {}",
        logs.text()
    );
    logs.assert_never_logged(&[
        secret,
        "a wrong password",
        "the next secret",
        &token,
        &second,
    ]);
}
