//! Sign-in through Google and GitHub (the providers are faked: `common::FakeProviders`), the
//! tests of the Python `test_accounts.py` and what the round trip must refuse.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use common::{LogCapture, PASSWORD, TestServer, Token};
use oswx_server::ext::{Event, Extensions};
use oswx_server::store::Id;
use oswx_server::{Clock, Settings};
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// The secret the app keeps; the server only sees its hash until the exchange.
const VERIFIER: &str = "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv";
/// Another app's secret.
const OTHER_VERIFIER: &str = "wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww";
/// The app's loopback address.
const REDIRECT: &str = "http://127.0.0.1:53111/done";
/// A start time for manual clocks.
const T0: f64 = 1_800_000_000.0;

fn challenge(verifier: &str) -> String {
    hex::encode(Sha256::digest(verifier.as_bytes()))
}

fn google(s: &mut Settings) {
    s.google_client_id = "gid".into();
    s.google_client_secret = "gs".into();
}

fn both(s: &mut Settings) {
    google(s);
    s.github_client_id = "hid".into();
    s.github_client_secret = "hs".into();
}

/// A client that does not follow redirects.
fn browser() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

/// The query of `url` as a map of lists, as Python's `parse_qs`.
fn query(url: &str) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    for (k, v) in url::Url::parse(url).unwrap().query_pairs() {
        out.entry(k.into_owned()).or_default().push(v.into_owned());
    }
    out
}

fn location(r: &reqwest::Response) -> String {
    r.headers()
        .get("location")
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default()
}

async fn get(server: &TestServer, path: &str, params: &[(&str, &str)]) -> reqwest::Response {
    browser()
        .get(server.url(path))
        .query(params)
        .send()
        .await
        .unwrap()
}

/// `GET /start` for `provider` with these query parameters.
async fn start(server: &TestServer, provider: &str, params: &[(&str, &str)]) -> reqwest::Response {
    get(server, &format!("/api/auth/oauth/{provider}/start"), params).await
}

/// The provider sends the browser back.
async fn callback(
    server: &TestServer,
    provider: &str,
    params: &[(&str, &str)],
) -> reqwest::Response {
    get(
        server,
        &format!("/api/auth/oauth/{provider}/callback"),
        params,
    )
    .await
}

/// What came back on the loopback address.
type Back = HashMap<String, Vec<String>>;

/// Starts a sign-in (with [`VERIFIER`]'s challenge, `redirect`, `invite`), checks the provider is
/// told this server's callback, and completes it as the person `code`. Answers what came back and
/// the state.
async fn oauth_to(
    server: &TestServer,
    provider: &str,
    code: &str,
    invite: Option<&str>,
    redirect: &str,
    verifier: &str,
) -> (Back, String) {
    let challenge = challenge(verifier);
    let mut params = vec![
        ("redirect_uri", redirect),
        ("code_challenge", challenge.as_str()),
    ];
    if let Some(invite) = invite {
        params.push(("invite", invite));
    }
    let r = start(server, provider, &params).await;
    assert_eq!(r.status(), StatusCode::FOUND, "{:?}", r.text().await);
    let q = query(&location(&r));
    assert_eq!(
        q["redirect_uri"],
        [format!(
            "http://testserver/api/auth/oauth/{provider}/callback"
        )]
    );
    let state = q["state"][0].clone();
    let r = callback(
        server,
        provider,
        &[("code", code), ("state", state.as_str())],
    )
    .await;
    assert_eq!(r.status(), StatusCode::FOUND);
    let to = location(&r);
    assert!(to.starts_with(redirect), "{to}");
    (query(&to), state)
}

async fn oauth(
    server: &TestServer,
    provider: &str,
    code: &str,
    invite: Option<&str>,
) -> (Back, String) {
    oauth_to(server, provider, code, invite, REDIRECT, VERIFIER).await
}

async fn exchange_with(server: &TestServer, back: &Back, verifier: &str) -> (StatusCode, Value) {
    server
        .post(
            "/api/auth/exchange",
            None,
            json!({"code": back["code"][0], "code_verifier": verifier}),
        )
        .await
}

async fn exchange(server: &TestServer, back: &Back) -> (StatusCode, Value) {
    exchange_with(server, back, VERIFIER).await
}

/// Signs in through `provider` as `code` and answers the token and the account's id.
async fn sign_in(server: &TestServer, provider: &str, code: &str) -> (Token, Id) {
    let (back, _) = oauth(server, provider, code, None).await;
    let (s, body) = exchange(server, &back).await;
    assert_eq!(s, StatusCode::OK, "{body} {back:?}");
    (
        body["token"].as_str().unwrap().to_owned(),
        body["user"]["id"].as_str().unwrap().to_owned(),
    )
}

fn error(back: &Back) -> &str {
    assert_eq!(back.len(), 1, "one key only: {back:?}");
    &back["error"][0]
}

async fn count(server: &TestServer, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(&mut *server.db().reader().await.unwrap())
        .await
        .unwrap()
}

// ------------------------------------------------------------------ ported from test_accounts.py

#[tokio::test]
async fn test_oauth_sign_in() {
    let server = TestServer::start_with(both).await;
    for who in ["ada", "bob", "eve"] {
        server.providers.person(who, &format!("{who}@example.com"));
    }
    assert_eq!(
        server.get("/api/server", None).await.1["oauth"],
        json!(["google", "github"])
    );
    // first person through Google becomes the admin
    let (back, state) = oauth(&server, "google", "ada", None).await;
    let (s, ada) = exchange(&server, &back).await;
    assert_eq!(s, StatusCode::OK, "{ada}");
    assert_eq!(ada["user"]["email"], "ada@example.com");
    assert_eq!(ada["user"]["is_server_admin"], true);
    let ada_token = ada["token"].as_str().unwrap();
    assert_eq!(
        server.get("/api/me", Some(ada_token)).await.0,
        StatusCode::OK
    );
    // the code and the state work once
    assert_eq!(exchange(&server, &back).await.0, StatusCode::BAD_REQUEST);
    let r = callback(
        &server,
        "google",
        &[("code", "ada"), ("state", state.as_str())],
    )
    .await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert_eq!(location(&r), "");
    // the same person through GitHub lands in the same account (verified address matches)
    let (back, _) = oauth(&server, "github", "ada", None).await;
    assert_eq!(
        exchange(&server, &back).await.1["user"]["id"],
        ada["user"]["id"]
    );
    // a stranger needs an invite
    let (back, _) = oauth(&server, "github", "eve", None).await;
    assert_eq!(error(&back), "invite_required");
    let (_, org) = server
        .post("/api/orgs", Some(ada_token), json!({"name": "Robotics"}))
        .await;
    let org = org["id"].as_str().unwrap();
    let (_, inv) = server
        .post(
            &format!("/api/orgs/{org}/invites"),
            Some(ada_token),
            json!({"email": "bob@example.com"}),
        )
        .await;
    let (back, _) = oauth(&server, "google", "bob", inv["token"].as_str()).await;
    // a code is useless to a program that did not start the sign-in, and one wrong try burns it
    assert_eq!(
        exchange_with(&server, &back, OTHER_VERIFIER).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(exchange(&server, &back).await.0, StatusCode::BAD_REQUEST);
    let (bob, _) = sign_in(&server, "google", "bob").await;
    let orgs = server.get("/api/orgs", Some(&bob)).await.1;
    let ids: Vec<&str> = orgs
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, [org]);
    // a provider that refuses the code
    let (back, _) = oauth(&server, "google", "nobody", None).await;
    assert_eq!(error(&back), "provider_refused");
}

#[tokio::test]
async fn test_oauth_only_redirects_to_this_machine() {
    let server = TestServer::start_with(google).await;
    let c = challenge(VERIFIER);
    for bad in [
        "https://evil.example/cb",
        "http://evil.example:80/cb",
        "http://127.0.0.1/cb",
        "http://user@127.0.0.1:5000/cb",
        "myapp://cb",
    ] {
        let r = start(
            &server,
            "google",
            &[("redirect_uri", bad), ("code_challenge", c.as_str())],
        )
        .await;
        assert_eq!(r.status(), StatusCode::BAD_REQUEST, "{bad}");
    }
    let ok = [
        ("redirect_uri", "http://127.0.0.1:5000/"),
        ("code_challenge", c.as_str()),
    ];
    assert_eq!(
        start(&server, "github", &ok).await.status(),
        StatusCode::NOT_FOUND
    ); // not set up
    let short = [
        ("redirect_uri", "http://127.0.0.1:5000/"),
        ("code_challenge", "short"),
    ];
    assert_eq!(
        start(&server, "google", &short).await.status(),
        StatusCode::BAD_REQUEST
    );
    let missing = [("redirect_uri", "http://127.0.0.1:5000/")];
    assert_eq!(
        start(&server, "google", &missing).await.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        start(&server, "google", &ok).await.status(),
        StatusCode::FOUND
    );
}

#[tokio::test]
async fn test_oauth_does_not_join_an_account_nobody_verified() {
    // Someone registers your address with their password and waits for you to arrive through Google.
    let server = TestServer::start_with(|s| {
        google(s);
        s.open_registration = true;
    })
    .await;
    server.providers.person("vic", "victim@example.com");
    server.providers.person("inv", "invited@example.com");
    let (admin, _) = server.register("ada@example.com").await;
    server
        .register_with("victim@example.com", "attacker's own")
        .await;
    let (back, _) = oauth(&server, "google", "vic", None).await;
    assert_eq!(error(&back), "account_exists_use_password");
    // an invite proves nothing either: whoever makes it holds the link and could register the
    // address themselves
    let (_, org) = server
        .post("/api/orgs", Some(&admin), json!({"name": "Robotics"}))
        .await;
    let org = org["id"].as_str().unwrap().to_owned();
    let (_, inv) = server
        .post(
            &format!("/api/orgs/{org}/invites"),
            Some(&admin),
            json!({"email": "invited@example.com"}),
        )
        .await;
    let (invited, invited_id) = server
        .register_invited("invited@example.com", None, inv["token"].as_str().unwrap())
        .await;
    let (back, _) = oauth(&server, "google", "inv", None).await;
    assert_eq!(error(&back), "account_exists_use_password");
    // the account's owner, signed in with the password, adds Google themselves
    let link = json!({"redirect_uri": REDIRECT, "code_challenge": challenge(VERIFIER),
        "current_password": PASSWORD});
    let browser = |token: Token, who: &'static str| {
        let (server, link) = (&server, link.clone());
        async move {
            let (s, body) = server
                .post("/api/auth/oauth/google/link", Some(&token), link)
                .await;
            assert_eq!(s, StatusCode::OK, "{body}");
            let state = query(body["url"].as_str().unwrap())["state"][0].clone();
            let r = callback(
                server,
                "google",
                &[("code", who), ("state", state.as_str())],
            )
            .await;
            query(&location(&r))
        }
    };
    let finish = |token: Token, back: Back, verifier: &'static str| {
        let server = &server;
        async move {
            server
                .post(
                    "/api/auth/oauth/link/finish",
                    Some(&token),
                    json!({"code": back["code"][0], "code_verifier": verifier}),
                )
                .await
        }
    };
    assert_eq!(
        server
            .post("/api/auth/oauth/google/link", None, link.clone())
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let mut wrong = link.clone();
    wrong["current_password"] = json!("wrong");
    // a stolen session alone cannot add a way in
    assert_eq!(
        server
            .post("/api/auth/oauth/google/link", Some(&invited), wrong)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let back = browser(invited.clone(), "inv").await;
    // a link code never signs anyone in
    assert_eq!(exchange(&server, &back).await.0, StatusCode::BAD_REQUEST);
    let back = browser(invited.clone(), "inv").await;
    // ... and only the account that started it can finish it
    assert_eq!(
        finish(admin.clone(), back, VERIFIER).await.0,
        StatusCode::BAD_REQUEST
    );
    let back = browser(invited.clone(), "inv").await;
    assert_eq!(
        finish(invited.clone(), back, OTHER_VERIFIER).await.0,
        StatusCode::BAD_REQUEST
    );
    let back = browser(invited.clone(), "inv").await;
    let (s, body) = finish(invited.clone(), back, VERIFIER).await;
    assert_eq!((s, &body["linked"]), (StatusCode::OK, &json!("google")));
    assert_eq!(
        server.get("/api/me/providers", Some(&invited)).await.1,
        json!({"providers": ["google"], "has_password": true})
    );
    // the provider vouched for the very address the account has
    assert_eq!(
        count(
            &server,
            &format!("SELECT COUNT(*) FROM users WHERE id = '{invited_id}' AND email_verified")
        )
        .await,
        1
    );
    // from now on Google signs them in
    let (back, _) = oauth(&server, "google", "inv", None).await;
    assert_eq!(exchange(&server, &back).await.1["user"]["id"], invited_id);

    // The trick this guards against: Mallory starts a link on HER account and gets the victim to
    // open the address. The victim completes Google in their browser - and nothing is attached to
    // Mallory's account.
    let (mallory, _) = server.register("mallory@example.com").await;
    let back = browser(mallory.clone(), "vic").await;
    assert!(back.contains_key("code"));
    assert_eq!(
        server.get("/api/me/providers", Some(&mallory)).await.1["providers"],
        json!([])
    );
    // the victim's own Google sign-in is still not Mallory's account
    let (back2, _) = oauth(&server, "google", "vic", None).await;
    assert_eq!(error(&back2), "account_exists_use_password");
    // a provider identity that already belongs to one account cannot be moved to another
    let back = browser(admin.clone(), "inv").await;
    assert_eq!(error(&back), "identity_in_use");
    // unlinking needs the password; the last way in cannot be removed
    assert_eq!(
        server
            .post("/api/me/providers/google/unlink", Some(&invited), json!({}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        server
            .post(
                "/api/me/providers/google/unlink",
                Some(&invited),
                json!({"current_password": PASSWORD})
            )
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        server.get("/api/me/providers", Some(&invited)).await.1["providers"],
        json!([])
    );
}

#[tokio::test]
async fn test_an_account_with_only_a_provider_cannot_unlink_it() {
    let server = TestServer::start_custom(google, |s| s.clock = Clock::manual(T0)).await;
    server.providers.person("ada", "ada@example.com");
    let (token, _) = sign_in(&server, "google", "ada").await;
    assert_eq!(
        server.get("/api/me/providers", Some(&token)).await.1,
        json!({"providers": ["google"], "has_password": false})
    );
    assert_eq!(
        server
            .post("/api/me/providers/google/unlink", Some(&token), json!({}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    // with no password to ask for, changing sign-in methods needs a recent sign-in instead
    let link = json!({"redirect_uri": REDIRECT, "code_challenge": challenge(VERIFIER)});
    assert_eq!(
        server
            .post("/api/auth/oauth/google/link", Some(&token), link.clone())
            .await
            .0,
        StatusCode::OK
    );
    server.state.clock.advance(3600.0);
    let (s, body) = server
        .post("/api/auth/oauth/google/link", Some(&token), link)
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert!(body["detail"].as_str().unwrap().contains("sign in again"));
    // ... and so does setting a first password: it would be a way round the rule above
    assert_eq!(
        server
            .patch(
                "/api/me",
                Some(&token),
                json!({"password": "attacker's own"})
            )
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let (s, _) = server
        .post(
            "/api/auth/login",
            None,
            json!({"email": "ada@example.com", "password": "attacker's own"}),
        )
        .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    // a fresh sign-in may set one
    let (fresh, _) = sign_in(&server, "google", "ada").await;
    assert_eq!(
        server
            .patch(
                "/api/me",
                Some(&fresh),
                json!({"password": "my own password"})
            )
            .await
            .0,
        StatusCode::OK
    );
}

// ------------------------------------------------------------------ the redirect address

#[tokio::test]
async fn a_redirect_uri_off_this_machine_is_refused_without_redirecting() {
    let server = TestServer::start_with(google).await;
    let (admin, _) = server.register("ada@example.com").await;
    let c = challenge(VERIFIER);
    let long = format!("http://127.0.0.1:5000/{}", "a".repeat(3000));
    for bad in [
        "http://127.0.0.1.evil.com:5000/cb",
        "http://localhost@evil.com:5000/cb",
        "http://evil.com@localhost:5000/cb",
        "http://user:pw@127.0.0.1:5000/cb",
        "http://127.0.0.1:5000@evil.com/cb",
        "http://127.0.0.1:abc/cb",
        "http://127.0.0.1:/cb",
        "http://127.0.0.1:0/cb",
        "http://127.0.0.1:99999/cb",
        "http://127.0.0.1:5000/cb#fragment",
        "http://127.1:5000/cb",
        "http://0x7f000001:5000/cb",
        "http://[0:0::1]:5000/cb",
        "http://127.0.0.1:5000\\@evil.com/",
        "http://127.0.0.1:5000/a b",
        "http://127.0.0.1:5000/\r\nSet-Cookie: x=1",
        "javascript:alert(1)",
        "",
        &long,
    ] {
        let r = start(
            &server,
            "google",
            &[("redirect_uri", bad), ("code_challenge", c.as_str())],
        )
        .await;
        assert_eq!(r.status(), StatusCode::BAD_REQUEST, "{bad}");
        assert_eq!(location(&r), "", "{bad}: no redirect");
        let (s, _) = server
            .post(
                "/api/auth/oauth/google/link",
                Some(&admin),
                json!({"redirect_uri": bad, "code_challenge": c, "current_password": PASSWORD}),
            )
            .await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{bad}");
    }
    for bad_challenge in [
        c.to_uppercase(),
        format!("{c}0"),
        "g".repeat(64),
        String::new(),
    ] {
        let r = start(
            &server,
            "google",
            &[
                ("redirect_uri", REDIRECT),
                ("code_challenge", bad_challenge.as_str()),
            ],
        )
        .await;
        assert_eq!(r.status(), StatusCode::BAD_REQUEST, "{bad_challenge}");
    }
    assert_eq!(count(&server, "SELECT COUNT(*) FROM oauth_states").await, 0);
    assert_eq!(server.providers.calls(), 0);
}

#[tokio::test]
async fn the_loopback_address_keeps_its_query_and_the_answer_is_not_cached() {
    let server = TestServer::start_with(google).await;
    server.providers.person("ada", "ada@example.com");
    let c = challenge(VERIFIER);
    let r = start(
        &server,
        "google",
        &[
            ("redirect_uri", "http://LOCALHOST:5000/cb?app=1"),
            ("code_challenge", c.as_str()),
        ],
    )
    .await;
    let state = query(&location(&r))["state"][0].clone();
    let r = callback(
        &server,
        "google",
        &[("code", "ada"), ("state", state.as_str())],
    )
    .await;
    assert_eq!(r.status(), StatusCode::FOUND);
    assert_eq!(r.headers()["cache-control"], "no-store");
    assert_eq!(r.headers()["referrer-policy"], "no-referrer");
    let to = location(&r);
    assert!(
        to.starts_with("http://localhost:5000/cb?app=1&code="),
        "{to}"
    );
    // [::1] works too
    let (back, _) = oauth_to(
        &server,
        "google",
        "ada",
        None,
        "http://[::1]:5000/x",
        VERIFIER,
    )
    .await;
    assert!(back.contains_key("code"));
}

// ------------------------------------------------------------------ states and codes

#[tokio::test]
async fn a_state_expires_and_expired_states_are_cleared() {
    let server = TestServer::start_custom(google, |s| s.clock = Clock::manual(T0)).await;
    server.providers.person("ada", "ada@example.com");
    let c = challenge(VERIFIER);
    let params = [("redirect_uri", REDIRECT), ("code_challenge", c.as_str())];
    let first = query(&location(&start(&server, "google", &params).await))["state"][0].clone();
    start(&server, "google", &params).await;
    server.state.clock.advance(600.5);
    let r = callback(
        &server,
        "google",
        &[("code", "ada"), ("state", first.as_str())],
    )
    .await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert_eq!(location(&r), "");
    assert_eq!(server.providers.calls(), 0, "the provider is not asked");
    // a new sign-in clears the expired ones
    let fresh = query(&location(&start(&server, "google", &params).await))["state"][0].clone();
    assert_eq!(count(&server, "SELECT COUNT(*) FROM oauth_states").await, 1);
    server.state.clock.advance(599.0);
    let r = callback(
        &server,
        "google",
        &[("code", "ada"), ("state", fresh.as_str())],
    )
    .await;
    assert!(query(&location(&r)).contains_key("code"));
}

#[tokio::test]
async fn a_state_is_for_its_own_provider_and_is_stored_hashed() {
    let server = TestServer::start_with(both).await;
    server.providers.person("ada", "ada@example.com");
    let c = challenge(VERIFIER);
    let r = start(
        &server,
        "google",
        &[("redirect_uri", REDIRECT), ("code_challenge", c.as_str())],
    )
    .await;
    let state = query(&location(&r))["state"][0].clone();
    let stored = count(
        &server,
        &format!(
            "SELECT COUNT(*) FROM oauth_states WHERE state_hash = '{}'",
            hex::encode(Sha256::digest(state.as_bytes()))
        ),
    )
    .await;
    assert_eq!(stored, 1);
    // GitHub's callback does not take Google's state (which stays for Google)
    let r = callback(
        &server,
        "github",
        &[("code", "ada"), ("state", state.as_str())],
    )
    .await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    let r = callback(
        &server,
        "google",
        &[("code", "ada"), ("state", state.as_str())],
    )
    .await;
    assert!(query(&location(&r)).contains_key("code"));
    // no state, an unknown one, a huge one
    for params in [
        vec![("code", "ada")],
        vec![("code", "ada"), ("state", "nonsense")],
        vec![("code", "ada"), ("state", &*"s".repeat(10_000))],
    ] {
        let r = callback(&server, "google", &params).await;
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        let body: Value = r.json().await.unwrap();
        assert_eq!(
            body["detail"],
            "this sign-in has expired; start again from the app"
        );
    }
}

#[tokio::test]
async fn a_login_code_is_used_once_expires_and_needs_its_own_verifier() {
    let server = TestServer::start_custom(google, |s| {
        s.clock = Clock::manual(T0);
    })
    .await;
    server.providers.person("ada", "ada@example.com");
    sign_in(&server, "google", "ada").await;

    // expires after two minutes, and is gone once presented
    let (back, _) = oauth(&server, "google", "ada", None).await;
    server.state.clock.advance(120.5);
    let (s, body) = exchange(&server, &back).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(
        body["detail"],
        "this code is not valid any more; sign in again"
    );

    // stored hashed
    let (back, _) = oauth(&server, "google", "ada", None).await;
    let hash = hex::encode(Sha256::digest(back["code"][0].as_bytes()));
    assert_eq!(
        count(
            &server,
            &format!("SELECT COUNT(*) FROM login_codes WHERE code_hash = '{hash}'")
        )
        .await,
        1
    );
    // a malformed verifier is refused before the code is touched
    let (s, _) = exchange_with(&server, &back, &"v".repeat(42)).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    let (s, _) = exchange_with(&server, &back, &"v".repeat(201)).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(exchange(&server, &back).await.0, StatusCode::OK);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM login_codes").await, 0);

    // the verifier of another sign-in does not work, and uses the code up
    let (mine, _) = oauth(&server, "google", "ada", None).await;
    let (theirs, _) = oauth_to(&server, "google", "ada", None, REDIRECT, OTHER_VERIFIER).await;
    let (s, body) = exchange_with(&server, &mine, OTHER_VERIFIER).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(
        body["detail"],
        "this code was issued to another app; sign in again"
    );
    assert_eq!(exchange(&server, &mine).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        exchange_with(&server, &theirs, OTHER_VERIFIER).await.0,
        StatusCode::OK
    );
    // hostile bodies
    for body in [
        json!({"code": "x".repeat(300), "code_verifier": VERIFIER}),
        json!({"code": 5, "code_verifier": VERIFIER}),
        json!({"code_verifier": VERIFIER}),
        json!({"code": "x"}),
    ] {
        let (s, _) = server.post("/api/auth/exchange", None, body.clone()).await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_exchanges_of_one_code_at_once_make_one_session() {
    let server = TestServer::start_with(google).await;
    server.providers.person("ada", "ada@example.com");
    let (_, ada) = sign_in(&server, "google", "ada").await;
    let sessions = format!("SELECT COUNT(*) FROM sessions WHERE user_id = '{ada}'");
    for round in 1..=10 {
        let (back, _) = oauth(&server, "google", "ada", None).await;
        let (a, b) = tokio::join!(exchange(&server, &back), exchange(&server, &back));
        let mut statuses = [a.0, b.0];
        statuses.sort();
        assert_eq!(
            statuses,
            [StatusCode::OK, StatusCode::BAD_REQUEST],
            "round {round}"
        );
        assert_eq!(count(&server, &sessions).await, 1 + round);
    }
}

// ------------------------------------------------------------------ what the loopback hears

/// Admits members when invites are made (the first `allow` calls), then refuses.
struct SeatsRunOut(Mutex<usize>);

#[async_trait]
impl Extensions for SeatsRunOut {
    async fn may_add_member(&self, _org: &Id, _current: usize) -> Result<(), String> {
        let mut left = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if *left == 0 {
            return Err("no seats".into());
        }
        *left -= 1;
        Ok(())
    }
}

#[tokio::test]
async fn every_error_code_reaches_the_loopback_address() {
    let server =
        TestServer::start_custom(both, |s| s.ext = Arc::new(SeatsRunOut(Mutex::new(1)))).await;
    let p = &server.providers;
    for who in ["ada", "eve", "pwd", "seat"] {
        p.person(who, &format!("{who}@example.com"));
    }
    p.person_with("unverified", "unverified@example.com", false);
    let c = challenge(VERIFIER);
    let (ada, ada_id) = sign_in(&server, "google", "ada").await;

    // cancelled: the person said no at the provider, or nothing came back
    for params in [vec![("error", "access_denied")], vec![], vec![("code", "")]] {
        let r = start(
            &server,
            "google",
            &[("redirect_uri", REDIRECT), ("code_challenge", c.as_str())],
        )
        .await;
        let state = query(&location(&r))["state"][0].clone();
        let mut params = params.clone();
        params.push(("state", state.as_str()));
        let r = callback(&server, "google", &params).await;
        assert_eq!(error(&query(&location(&r))), "cancelled", "{params:?}");
    }
    // a provider's own error code is passed through; anything else is not
    for (sent, heard) in [
        ("temporarily_unavailable", "temporarily_unavailable"),
        ("x&code=forged", "provider_refused"),
        ("Server Error", "provider_refused"),
    ] {
        let r = start(
            &server,
            "google",
            &[("redirect_uri", REDIRECT), ("code_challenge", c.as_str())],
        )
        .await;
        let state = query(&location(&r))["state"][0].clone();
        let r = callback(
            &server,
            "google",
            &[("error", sent), ("code", "ada"), ("state", state.as_str())],
        )
        .await;
        assert_eq!(error(&query(&location(&r))), heard, "{sent}");
    }
    // provider_refused: a code the provider does not know, or one too long to be real
    let (back, _) = oauth(&server, "google", "nobody", None).await;
    assert_eq!(error(&back), "provider_refused");
    let calls = p.calls();
    let (back, _) = oauth(&server, "google", &"x".repeat(5000), None).await;
    assert_eq!(error(&back), "provider_refused");
    assert_eq!(p.calls(), calls, "not sent to the provider");
    // provider_unreachable
    p.unreachable(true);
    let (back, _) = oauth(&server, "google", "ada", None).await;
    assert_eq!(error(&back), "provider_unreachable");
    p.unreachable(false);
    // no_verified_email, through Google and through GitHub (only unverified addresses)
    for provider in ["google", "github"] {
        let (back, _) = oauth(&server, provider, "unverified", None).await;
        assert_eq!(error(&back), "no_verified_email", "{provider}");
    }
    // invite_required
    let (back, _) = oauth(&server, "google", "eve", None).await;
    assert_eq!(error(&back), "invite_required");
    // account_exists_use_password
    let (s, inv) = server
        .post("/api/orgs", Some(&ada), json!({"name": "Robotics"}))
        .await;
    assert_eq!(s, StatusCode::CREATED);
    let org = inv["id"].as_str().unwrap().to_owned();
    let (s, inv) = server
        .post(&format!("/api/orgs/{org}/invites"), Some(&ada), json!({}))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{inv}");
    let invite = inv["token"].as_str().unwrap().to_owned();
    server
        .sql(&format!(
            "INSERT INTO users (id, email, name, password_hash, is_server_admin, disabled, \
             email_verified, created_at, deleted_at) VALUES ('{}', 'pwd@example.com', 'pwd', \
             'scrypt$x', FALSE, FALSE, FALSE, 0, NULL)",
            "p".repeat(32)
        ))
        .await;
    let (back, _) = oauth(&server, "google", "pwd", None).await;
    assert_eq!(error(&back), "account_exists_use_password");
    // not_allowed: the seat hook refuses (it allowed only the invite itself); nothing is created
    let (back, _) = oauth(&server, "google", "seat", Some(&invite)).await;
    assert_eq!(error(&back), "not_allowed");
    assert_eq!(
        count(
            &server,
            "SELECT COUNT(*) FROM users WHERE email = 'seat@example.com'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &server,
            "SELECT COUNT(*) FROM invites WHERE accepted_by IS NULL"
        )
        .await,
        1
    );
    // identity_in_use: adding Ada's Google identity to another account
    let (s, body) = server
        .post(
            "/api/auth/oauth/google/link",
            Some(&ada),
            json!({"redirect_uri": REDIRECT, "code_challenge": c}),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    server
        .providers
        .answer("someone", "ada", "ada@example.com", true);
    server
        .sql(&format!(
            "UPDATE oauth_states SET link_user_id = '{}'",
            "p".repeat(32)
        ))
        .await;
    let state = query(body["url"].as_str().unwrap())["state"][0].clone();
    let r = callback(
        &server,
        "google",
        &[("code", "someone"), ("state", state.as_str())],
    )
    .await;
    assert_eq!(error(&query(&location(&r))), "identity_in_use");
    // account_disabled: a disabled account with a provider identity cannot sign in
    let (back, _) = oauth(&server, "google", "ada", None).await; // (a code from before)
    server
        .sql(&format!(
            "UPDATE users SET disabled = TRUE WHERE id = '{ada_id}'"
        ))
        .await;
    let (late, _) = oauth(&server, "google", "ada", None).await;
    assert_eq!(error(&late), "account_disabled");
    let (s, body) = exchange(&server, &back).await;
    assert_eq!(
        (s, &body["detail"]),
        (StatusCode::FORBIDDEN, &json!("this account is disabled"))
    );
    // (also through GitHub, which matches the address)
    let (late, _) = oauth(&server, "github", "ada", None).await;
    assert_eq!(error(&late), "account_disabled");
}

#[tokio::test]
async fn a_provider_that_answers_a_code_with_someone_else_is_believed_each_time() {
    let server = TestServer::start_with(|s| {
        google(s);
        s.open_registration = true;
    })
    .await;
    server
        .providers
        .answer("code-1", "ada", "ada@example.com", true);
    let (_, ada) = sign_in(&server, "google", "code-1").await;
    // the same code, replayed to the provider later, now answers for Bob
    server
        .providers
        .answer("code-1", "bob", "bob@example.com", true);
    let (back, _) = oauth(&server, "google", "code-1", None).await;
    let (s, body) = exchange(&server, &back).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["user"]["email"], "bob@example.com");
    assert_ne!(body["user"]["id"], ada);
}

// ------------------------------------------------------------------ accounts made through a provider

#[tokio::test]
async fn the_first_account_made_through_a_provider_is_the_admin_with_a_confirmed_address() {
    let server = TestServer::start_with(google).await;
    server.providers.person("ada", "Ada@Example.com");
    let (token, id) = sign_in(&server, "google", "ada").await;
    let me = server.get("/api/me", Some(&token)).await.1;
    assert_eq!(
        me,
        json!({"id": id, "email": "ada@example.com", "name": "ada", "is_server_admin": true})
    );
    assert_eq!(
        count(
            &server,
            "SELECT COUNT(*) FROM users WHERE email_verified AND password_hash IS NULL"
        )
        .await,
        1
    );
    assert_eq!(
        server.get("/api/server", None).await.1["registration"],
        "invite"
    );
    // the next one is not
    server.providers.person("bob", "bob@example.com");
    let (back, _) = oauth(&server, "google", "bob", None).await;
    assert_eq!(error(&back), "invite_required");
}

#[tokio::test]
async fn an_invite_carried_through_a_provider_gives_its_roles_once() {
    let server = TestServer::start_with(both).await;
    let w = common::World::new(&server).await.unwrap();
    server.providers.person("bob", "bob@example.com");
    server.providers.person("cat", "cat@example.com");
    let (s, inv) = server
        .post(
            &format!("/api/orgs/{}/invites", w.org),
            Some(&w.admin),
            json!({"role": "admin", "projects": [{"project_id": w.project, "role": "viewer"}]}),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED);
    let invite = inv["token"].as_str().unwrap();
    // stored only as its hash while the sign-in is away
    let r = start(
        &server,
        "github",
        &[
            ("redirect_uri", REDIRECT),
            ("code_challenge", challenge(VERIFIER).as_str()),
            ("invite", invite),
        ],
    )
    .await;
    assert_eq!(
        count(
            &server,
            &format!(
                "SELECT COUNT(*) FROM oauth_states WHERE invite = '{}'",
                hex::encode(Sha256::digest(invite.as_bytes()))
            )
        )
        .await,
        1
    );
    let state = query(&location(&r))["state"][0].clone();
    let r = callback(
        &server,
        "github",
        &[("code", "bob"), ("state", state.as_str())],
    )
    .await;
    let (s, bob) = exchange(&server, &query(&location(&r))).await;
    assert_eq!(s, StatusCode::OK, "{bob}");
    let bob = bob["token"].as_str().unwrap();
    let orgs = server.get("/api/orgs", Some(bob)).await.1;
    assert_eq!(orgs[0]["role"], "admin");
    // (an organisation admin owns every project; the grant is kept as given)
    assert_eq!(
        count(
            &server,
            &format!(
                "SELECT COUNT(*) FROM project_members WHERE project_id = '{}' AND role = 'viewer'",
                w.project
            )
        )
        .await,
        1
    );
    // used up
    let (back, _) = oauth(&server, "google", "cat", Some(invite)).await;
    assert_eq!(error(&back), "invite_required");
}

#[tokio::test]
async fn an_unconfigured_or_unknown_provider_is_404() {
    let server = TestServer::start_with(google).await;
    let (admin, _) = server.register("ada@example.com").await;
    let c = challenge(VERIFIER);
    let params = [("redirect_uri", REDIRECT), ("code_challenge", c.as_str())];
    for provider in ["github", "facebook", "Google"] {
        assert_eq!(
            start(&server, provider, &params).await.status(),
            StatusCode::NOT_FOUND
        );
        let r = callback(&server, provider, &[("code", "x"), ("state", "y")]).await;
        assert_eq!(r.status(), StatusCode::NOT_FOUND);
        let (s, _) = server
            .post(
                &format!("/api/auth/oauth/{provider}/link"),
                Some(&admin),
                json!({"redirect_uri": REDIRECT, "code_challenge": c, "current_password": PASSWORD}),
            )
            .await;
        assert_eq!(s, StatusCode::NOT_FOUND);
    }
    // a server with none set up
    let bare = TestServer::start().await;
    assert_eq!(
        start(&bare, "google", &params).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(count(&server, "SELECT COUNT(*) FROM oauth_states").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hanging_provider_is_unreachable_in_time_and_holds_no_writer() {
    let server = TestServer::start_with(google).await;
    server.providers.person("ada", "ada@example.com");
    server.providers.set_timeout(Duration::from_millis(1500));
    server.providers.hang(true);
    let c = challenge(VERIFIER);
    let r = start(
        &server,
        "google",
        &[("redirect_uri", REDIRECT), ("code_challenge", c.as_str())],
    )
    .await;
    let state = query(&location(&r))["state"][0].clone();
    let url = server.url("/api/auth/oauth/google/callback");
    let began = Instant::now();
    let waiting = tokio::spawn(async move {
        browser()
            .get(url)
            .query(&[("code", "ada"), ("state", state.as_str())])
            .send()
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    // another write goes through while the provider is being waited for
    let written =
        tokio::time::timeout(Duration::from_secs(1), server.register("bob@example.com")).await;
    assert!(written.is_ok(), "a write waited for the provider");
    assert!(!waiting.is_finished());
    let r = waiting.await.unwrap();
    let elapsed = began.elapsed();
    assert_eq!(error(&query(&location(&r))), "provider_unreachable");
    assert!(elapsed >= Duration::from_millis(1400), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
}

#[tokio::test]
async fn start_and_exchange_are_limited_per_client() {
    let server = TestServer::start_with(google).await;
    let c = challenge(VERIFIER);
    let params = [("redirect_uri", REDIRECT), ("code_challenge", c.as_str())];
    for _ in 0..60 {
        assert_eq!(
            start(&server, "google", &params).await.status(),
            StatusCode::FOUND
        );
    }
    assert_eq!(
        start(&server, "google", &params).await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    let body = json!({"code": "guess", "code_verifier": VERIFIER});
    for _ in 0..60 {
        let (s, _) = server.post("/api/auth/exchange", None, body.clone()).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
    }
    let (s, _) = server.post("/api/auth/exchange", None, body).await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn link_finish_refuses_an_identity_taken_meanwhile_and_uses_the_code_up() {
    let server = TestServer::start_with(|s| {
        google(s);
        s.open_registration = true;
    })
    .await;
    server.providers.person("zed", "zed@example.com");
    let (admin, _) = server.register("ada@example.com").await;
    let (s, body) = server
        .post(
            "/api/auth/oauth/google/link",
            Some(&admin),
            json!({"redirect_uri": REDIRECT, "code_challenge": challenge(VERIFIER),
                "current_password": PASSWORD}),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    let state = query(body["url"].as_str().unwrap())["state"][0].clone();
    let r = callback(
        &server,
        "google",
        &[("code", "zed"), ("state", state.as_str())],
    )
    .await;
    let back = query(&location(&r));
    // meanwhile Zed signs up with that Google account
    sign_in(&server, "google", "zed").await;
    let finish = json!({"code": back["code"][0], "code_verifier": VERIFIER});
    let (s, _) = server
        .post("/api/auth/oauth/link/finish", Some(&admin), finish.clone())
        .await;
    assert_eq!(s, StatusCode::CONFLICT);
    let (s, _) = server
        .post("/api/auth/oauth/link/finish", Some(&admin), finish)
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(
        server.get("/api/me/providers", Some(&admin)).await.1["providers"],
        json!([])
    );
}

/// Keeps every event.
#[derive(Default)]
struct Events(Mutex<Vec<Event>>);

struct Recording(Arc<Events>);

#[async_trait]
impl Extensions for Recording {
    async fn on_event(&self, event: &Event) {
        self.0
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event.clone());
    }
}

#[tokio::test]
async fn sign_ins_links_and_unlinks_are_events() {
    let events = Arc::new(Events::default());
    let kept = events.clone();
    let server = TestServer::start_custom(both, |s| s.ext = Arc::new(Recording(kept))).await;
    server.providers.person("ada", "ada@example.com");
    let (token, ada) = sign_in(&server, "google", "ada").await;
    sign_in(&server, "github", "ada").await;
    let (s, _) = server
        .post("/api/me/providers/github/unlink", Some(&token), json!({}))
        .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let got = events.0.lock().unwrap().clone();
    let user = || ada.clone();
    assert_eq!(
        got,
        [
            Event::RegisteredWithProvider {
                user: user(),
                provider: "google".into()
            },
            Event::SignedInWithProvider { user: user() },
            Event::ProviderLinked {
                user: user(),
                provider: "github".into()
            },
            Event::SignedInWithProvider { user: user() },
            Event::ProviderUnlinked {
                user: user(),
                provider: "github".into()
            },
        ]
    );
    assert_eq!(
        server.get("/api/me/providers", Some(&token)).await.1["providers"],
        json!(["google"])
    );
}

#[tokio::test]
async fn states_codes_verifiers_tokens_and_addresses_are_never_logged() {
    let logs = LogCapture::start();
    let server = TestServer::start_with(|s| {
        google(s);
        s.open_registration = true;
    })
    .await;
    let code = "provider-code-8c1f2e";
    let email = "ada.secret@example.com";
    server.providers.person(code, email);
    let (back, state) = oauth(&server, "google", code, None).await;
    let login_code = back["code"][0].clone();
    let (s, body) = exchange(&server, &back).await;
    assert_eq!(s, StatusCode::OK);
    let token = body["token"].as_str().unwrap().to_owned();
    let id = body["user"]["id"].as_str().unwrap().to_owned();
    // a wrong verifier, a link and its finish, an error
    let (wrong, wrong_state) = oauth(&server, "google", code, None).await;
    exchange_with(&server, &wrong, OTHER_VERIFIER).await;
    let (_, link) = server
        .post(
            "/api/auth/oauth/google/link",
            Some(&token),
            json!({"redirect_uri": REDIRECT, "code_challenge": challenge(VERIFIER)}),
        )
        .await;
    let link_state = query(link["url"].as_str().unwrap())["state"][0].clone();
    let r = callback(
        &server,
        "google",
        &[("code", code), ("state", link_state.as_str())],
    )
    .await;
    let link_code = query(&location(&r))["code"][0].clone();
    server
        .post(
            "/api/auth/oauth/link/finish",
            Some(&token),
            json!({"code": link_code, "code_verifier": VERIFIER}),
        )
        .await;
    oauth(&server, "google", "nobody", None).await;
    assert!(
        logs.text().contains("browser sign-in code issued") && logs.text().contains(&id),
        "the capture works: {}",
        logs.text()
    );
    logs.assert_never_logged(&[
        code,
        email,
        &state,
        &wrong_state,
        &link_state,
        &login_code,
        &wrong["code"][0],
        &link_code,
        VERIFIER,
        OTHER_VERIFIER,
        &token,
        "at-provider-code",
    ]);
}
