//! Accounts: registration, sign-in and out, the current user, and forgotten passwords.
//!
//! - `POST /api/auth/register`, `POST /api/auth/login`, `POST /api/auth/logout`
//! - `GET /api/me`, `PATCH /api/me`
//! - `POST /api/auth/password-reset`, `POST /api/auth/password-reset/confirm`, and the page a reset
//!   link opens, `GET /reset-password/{token}`
//!
//! Who may register: the first account on a server (it becomes the server admin); after that anyone
//! with a valid invite; or anyone at all when `open_registration` is on.
//!
//! Sign-in failures are limited per email address and client address (10 in 10 minutes); reset
//! requests per address and client and per address alone (5 an hour). The client address is the
//! connection's peer, except that a peer of `127.0.0.1` (a proxy on this machine) is taken at its word
//! in `X-Forwarded-For`, as uvicorn does by default for the Python server.

use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};

use axum::extract::{ConnectInfo, FromRequestParts, State};
use axum::http::header::{
    CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, REFERRER_POLICY, X_CONTENT_TYPE_OPTIONS,
};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::access::CurrentUser;
use crate::config::Settings;
use crate::error::{ApiError, ApiJson};
use crate::ext::{Event, emit};
use crate::mail::{reset_text, send_logged};
use crate::routes::meta::{Registration, registration_for};
use crate::routes::orgs::{accept_invite, approve_invite, find_invite};
use crate::security::{
    RateLimit, check_password_len, clean_email, hash_password, new_token, token_hash, verify_dummy,
    verify_password,
};
use crate::state::AppState;
use crate::store::{PasswordReset, Session, Tx, User, new_id};

/// The longest email address accepted (the column's size).
const EMAIL_MAX: usize = 320;
/// The longest account name (the column's size).
const NAME_MAX: usize = 200;
/// The longest session label kept; longer ones are cut, as the Python server does.
const LABEL_MAX: usize = 200;
/// The longest invite or reset token accepted (real ones are 43 characters).
const TOKEN_MAX: usize = 256;
/// The longest password accepted where one is only checked, never set (sign-in, the current
/// password): longer than any password this server sets, short enough to bound the work.
const CHECKED_PASSWORD_MAX: usize = 1024;
/// How recent a sign-in must be to stand in for the password of an account that has none.
const RECENT_SIGN_IN_SECS: f64 = 600.0;

const INVITE_ONLY: &str =
    "this server is invite-only: ask an organisation owner for an invite link";
const EMAIL_TAKEN: &str = "there is already an account with this email address";
const RESET_ANSWER: &str = "if there is an account for this address, a link is on its way";
const RESET_EXPIRED: &str = "this link has expired or was already used; ask for a new one";
const NO_EMAIL: &str = "this server cannot send email: ask its operator to reset your password";

/// The sign-in and reset limits; one set per app.
#[derive(Debug)]
struct Limits {
    login: RateLimit,
    reset: RateLimit,
}

pub(crate) fn router() -> Router<AppState> {
    let limits = Arc::new(Limits {
        login: RateLimit::new(10, 600),
        reset: RateLimit::new(5, 3600),
    });
    Router::new()
        .route("/api/auth/register", post(register))
        .route("/api/auth/login", post(login))
        .route("/api/auth/logout", post(logout))
        .route("/api/me", get(me).patch(update_me))
        .route("/api/auth/password-reset", post(request_reset))
        .route("/api/auth/password-reset/confirm", post(confirm_reset))
        .route("/reset-password/{token}", get(reset_page))
        .layer(Extension(limits))
}

/// 422 when `value` is longer than `max` characters.
fn max_len(field: &str, value: &str, max: usize) -> Result<(), ApiError> {
    if value.chars().count() > max {
        return Err(ApiError::Unprocessable(format!(
            "{field} must be at most {max} characters"
        )));
    }
    Ok(())
}

/// An account as its owner sees it.
pub fn me_out(u: &User) -> Value {
    json!({"id": u.id, "email": u.email, "name": u.name, "is_server_admin": u.is_server_admin})
}

/// Starts a session for `user_id` and answers its token (only the hash is stored). Does not commit.
pub(crate) async fn start_session(
    tx: &mut dyn Tx,
    settings: &Settings,
    user_id: &str,
    label: &str,
    now: f64,
) -> anyhow::Result<String> {
    let token = new_token();
    tx.insert_session(&Session {
        token_hash: token_hash(&token),
        user_id: user_id.to_owned(),
        created_at: now,
        expires_at: now + f64::from(settings.session_days) * 86400.0,
        label: label.chars().take(LABEL_MAX).collect(),
    })
    .await?;
    Ok(token)
}

/// The client's address, for the limits (see the module documentation).
struct ClientAddr(String);

/// The peer that may speak for the client in `X-Forwarded-For`: uvicorn's default
/// `forwarded_allow_ips`.
const TRUSTED_PROXY: &str = "127.0.0.1";

/// The client's address as uvicorn's proxy-headers middleware works it out with its defaults: the
/// peer's, unless the peer is [`TRUSTED_PROXY`] and sent `X-Forwarded-For`; then the rightmost
/// address there that is not the trusted proxy (or the leftmost if all are). `""` without a peer.
fn client_ip(peer: Option<SocketAddr>, headers: &HeaderMap) -> String {
    let Some(peer) = peer else {
        return String::new();
    };
    let ip = peer.ip().to_canonical().to_string();
    if ip != TRUSTED_PROXY {
        return ip;
    }
    let Some(forwarded) = headers
        .get_all("x-forwarded-for")
        .iter()
        .next_back()
        .and_then(|v| v.to_str().ok())
    else {
        return ip;
    };
    let hosts: Vec<&str> = forwarded.split(',').map(str::trim).collect();
    let host = hosts
        .iter()
        .rev()
        .find(|h| **h != TRUSTED_PROXY)
        .or(hosts.first())
        .copied()
        .unwrap_or_default();
    // A limiter key; an address is at most 45 characters.
    host.chars().take(64).collect()
}

impl<S: Send + Sync> FromRequestParts<S> for ClientAddr {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let peer = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|c| c.0);
        Ok(Self(client_ip(peer, &parts.headers)))
    }
}

// ------------------------------------------------------------------ registration

#[derive(Debug, Deserialize)]
struct RegisterIn {
    email: String,
    password: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    invite: Option<String>,
}

/// Registers an account. In invite mode it needs a usable invite ([`find_invite`]: unknown, used,
/// expired and mismatched invites are all refused with 403); with one, in any mode, the account
/// joins as the invite says, in the same transaction ([`approve_invite`] asks the seat hook first,
/// 402; [`accept_invite`]: 404 `this invite has just been used` or 409 "try again" on a race).
async fn register(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<RegisterIn>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    max_len("email", &body.email, EMAIL_MAX)?;
    check_password_len(&body.password)?;
    max_len("name", &body.name, NAME_MAX)?;
    if let Some(invite) = &body.invite {
        max_len("invite", invite, TOKEN_MAX)?;
    }
    let email = clean_email(&body.email)
        .map_err(|_| ApiError::Unprocessable("not an email address".into()))?;
    // Before the (slow) hash; checked again in the transaction.
    if state.store.user_by_email(&email).await?.is_some() {
        return Err(ApiError::Conflict(EMAIL_TAKEN.into()));
    }
    let password_hash = hash_password(&body.password).await;
    let now = state.clock.now();
    // The seat hook is asked before the transaction (never while it holds the writer).
    let approval = approve_invite(&state, body.invite.as_deref(), &email, None, now).await?;

    let mut tx = state.store.begin().await?;
    if tx.user_by_email(&email).await?.is_some() {
        return Err(ApiError::Conflict(EMAIL_TAKEN.into()));
    }
    // Counted in the transaction: two first registrations at once cannot both become admin.
    let mode = registration_for(tx.user_count().await?, &state.settings);
    let invite = find_invite(&mut *tx, body.invite.as_deref(), &email, now).await?;
    if mode == Registration::Invite && invite.is_none() {
        return Err(ApiError::Forbidden(INVITE_ONLY));
    }
    let name = match body.name.trim() {
        "" => email.split('@').next().unwrap_or_default().to_owned(),
        name => name.to_owned(),
    };
    let first = mode == Registration::FirstUser;
    let user = User {
        id: new_id(),
        email,
        name,
        password_hash: Some(password_hash),
        is_server_admin: first,
        disabled: false,
        // The operator's own account counts as confirmed; anyone else's does not, yet.
        email_verified: first,
        created_at: now,
        deleted_at: None,
    };
    tx.insert_user(&user).await?;
    let accepted = match &invite {
        Some(invite) => {
            Some(accept_invite(&mut *tx, invite, approval.as_ref(), &user.id, now).await?)
        }
        None => None,
    };
    let token = start_session(&mut *tx, &state.settings, &user.id, "", now).await?;
    tx.commit().await?;
    tracing::info!(user = %user.id, server_admin = first, "account created");
    let registered = Event::Registered {
        user: user.id.clone(),
    };
    emit(&state.ext, registered).await;
    if let Some(accepted) = accepted {
        accepted.announce(&state).await;
    }
    Ok((
        StatusCode::CREATED,
        Json(json!({"token": token, "user": me_out(&user)})),
    ))
}

// ------------------------------------------------------------------ sign-in

#[derive(Debug, Deserialize)]
struct LoginIn {
    email: String,
    password: String,
    #[serde(default)]
    label: String,
}

async fn login(
    State(state): State<AppState>,
    Extension(limits): Extension<Arc<Limits>>,
    ClientAddr(ip): ClientAddr,
    ApiJson(body): ApiJson<LoginIn>,
) -> Result<Json<Value>, ApiError> {
    max_len("email", &body.email, EMAIL_MAX)?;
    max_len("password", &body.password, CHECKED_PASSWORD_MAX)?;
    let email = clean_email(&body.email).unwrap_or_default();
    let key = format!("{email}|{ip}");
    let now = state.clock.now();
    if limits.login.blocked(&key, now) {
        return Err(ApiError::RateLimited);
    }
    let user = if email.is_empty() {
        None
    } else {
        state.store.user_by_email(&email).await?
    };
    // Every refusal costs one scrypt run, so the timing does not tell them apart.
    let accepted = match user.as_ref().filter(|u| u.active()) {
        Some(User {
            password_hash: Some(hash),
            ..
        }) => verify_password(&body.password, hash).await,
        _ => verify_dummy(&body.password).await,
    };
    let Some(user) = user.filter(|_| accepted) else {
        limits.login.fail(&key, now);
        return Err(ApiError::WrongCredentials);
    };
    limits.login.reset(&key);
    let mut tx = state.store.begin().await?;
    tx.delete_expired_user_sessions(&user.id, now).await?;
    let token = start_session(&mut *tx, &state.settings, &user.id, &body.label, now).await?;
    tx.commit().await?;
    let signed_in = Event::SignedIn {
        user: user.id.clone(),
    };
    emit(&state.ext, signed_in).await;
    Ok(Json(json!({"token": token, "user": me_out(&user)})))
}

/// Ends the session of the token presented (only that one).
async fn logout(State(state): State<AppState>, user: CurrentUser) -> Result<StatusCode, ApiError> {
    let mut tx = state.store.begin().await?;
    tx.delete_session(&user.token_hash).await?;
    tx.commit().await?;
    state.sessions.forget_token(&user.token_hash);
    emit(&state.ext, Event::SignedOut { user: user.id }).await;
    Ok(StatusCode::NO_CONTENT)
}

// ------------------------------------------------------------------ the current user

/// The signed-in account's row (401 if it vanished since the session was checked).
async fn load(state: &AppState, user: &CurrentUser) -> Result<User, ApiError> {
    state
        .store
        .user_by_id(&user.id)
        .await?
        .ok_or(ApiError::Unauthorized)
}

async fn me(State(state): State<AppState>, user: CurrentUser) -> Result<Json<Value>, ApiError> {
    Ok(Json(me_out(&load(&state, &user).await?)))
}

#[derive(Debug, Deserialize)]
struct MeIn {
    name: Option<String>,
    password: Option<String>,
    current_password: Option<String>,
}

/// Changing how an account is signed in to takes more than a session token that may have leaked:
/// the current password, or for an account that has none, a sign-in from the last ten minutes (403
/// otherwise).
pub(crate) async fn confirm(
    state: &AppState,
    user: &User,
    token_hash: &str,
    password: Option<&str>,
) -> Result<(), ApiError> {
    if let Some(hash) = &user.password_hash {
        if !verify_password(password.unwrap_or(""), hash).await {
            return Err(ApiError::Forbidden("the current password is wrong"));
        }
        return Ok(());
    }
    let now = state.clock.now();
    let recent = state
        .store
        .session_by_hash(token_hash)
        .await?
        .is_some_and(|s| now - s.created_at <= RECENT_SIGN_IN_SECS);
    if !recent {
        return Err(ApiError::Forbidden(
            "sign in again first (this needs a sign-in from the last ten minutes)",
        ));
    }
    Ok(())
}

/// Renames the account and/or changes its password. A new password ends every other session and
/// closes every open document connection of the account (this device's too).
async fn update_me(
    State(state): State<AppState>,
    current: CurrentUser,
    ApiJson(body): ApiJson<MeIn>,
) -> Result<Json<Value>, ApiError> {
    let name = match &body.name {
        Some(name) => {
            max_len("name", name, NAME_MAX)?;
            if name.trim().is_empty() {
                return Err(ApiError::Unprocessable("name must not be empty".into()));
            }
            Some(name.trim())
        }
        None => None,
    };
    if let Some(password) = &body.password {
        check_password_len(password)?;
    }
    if let Some(current_password) = &body.current_password {
        max_len("current_password", current_password, CHECKED_PASSWORD_MAX)?;
    }
    let user = load(&state, &current).await?;
    let password_hash = match &body.password {
        Some(password) => {
            confirm(
                &state,
                &user,
                &current.token_hash,
                body.current_password.as_deref(),
            )
            .await?;
            Some(hash_password(password).await)
        }
        None => None,
    };
    if name.is_none() && password_hash.is_none() {
        return Ok(Json(me_out(&user)));
    }
    let mut tx = state.store.begin().await?;
    if let Some(name) = name {
        tx.set_user_name(&user.id, name).await?;
    }
    if let Some(hash) = &password_hash {
        tx.set_user_password(&user.id, Some(hash)).await?;
        tx.delete_other_user_sessions(&user.id, &current.token_hash)
            .await?;
    }
    tx.commit().await?;
    if password_hash.is_some() {
        state.sessions.forget_user(&user.id);
        state.hub.drop_account(&user.id).await;
        let changed = Event::PasswordChanged {
            user: user.id.clone(),
        };
        emit(&state.ext, changed).await;
    }
    Ok(Json(me_out(&load(&state, &current).await?)))
}

// ------------------------------------------------------------------ forgotten passwords

#[derive(Debug, Deserialize)]
struct ResetIn {
    email: String,
}

/// Emails a link to choose a new password. The answer is the same whether or not there is such an
/// account, whether the request was limited, and whether the mail server took the message.
async fn request_reset(
    State(state): State<AppState>,
    Extension(limits): Extension<Arc<Limits>>,
    ClientAddr(ip): ClientAddr,
    ApiJson(body): ApiJson<ResetIn>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if !state.mailer.configured() {
        return Err(ApiError::NotImplemented(NO_EMAIL.into()));
    }
    max_len("email", &body.email, EMAIL_MAX)?;
    let answer = (
        StatusCode::ACCEPTED,
        Json(json!({ "message": RESET_ANSWER })),
    );
    let email = clean_email(&body.email).unwrap_or_default();
    let key = format!("{email}|{ip}");
    let now = state.clock.now();
    if limits.reset.blocked(&key, now) || limits.reset.blocked(&email, now) {
        return Ok(answer);
    }
    limits.reset.fail(&key, now);
    limits.reset.fail(&email, now);
    let user = if email.is_empty() {
        None
    } else {
        state.store.user_by_email(&email).await?
    };
    let Some(user) = user.filter(User::active) else {
        return Ok(answer);
    };
    let settings = &state.settings;
    let token = new_token();
    let mut tx = state.store.begin().await?;
    // Only the newest link works.
    tx.delete_user_password_resets(&user.id).await?;
    tx.delete_expired_password_resets(now).await?;
    tx.insert_password_reset(&PasswordReset {
        token_hash: token_hash(&token),
        user_id: user.id.clone(),
        expires_at: now + f64::from(settings.reset_minutes) * 60.0,
    })
    .await?;
    tx.commit().await?;
    let url = format!(
        "{}/reset-password/{token}",
        settings.public_url.trim_end_matches('/')
    );
    let (subject, text) = reset_text(&url, settings.reset_minutes);
    send_logged(
        state.mailer.as_ref(),
        settings,
        &user.email,
        &subject,
        &text,
    )
    .await;
    let requested = Event::PasswordResetRequested { user: user.id };
    emit(&state.ext, requested).await;
    Ok(answer)
}

#[derive(Debug, Deserialize)]
struct ResetConfirmIn {
    token: String,
    password: String,
}

/// Sets the password a reset link was sent for. The address counts as confirmed (they read mail
/// sent to it), and the account is signed out everywhere.
async fn confirm_reset(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<ResetConfirmIn>,
) -> Result<Json<Value>, ApiError> {
    max_len("token", &body.token, TOKEN_MAX)?;
    check_password_len(&body.password)?;
    let expired = || ApiError::BadRequest(RESET_EXPIRED.into());
    let hash = token_hash(&body.token);
    let now = state.clock.now();
    // Before the (slow) hash, so that guessing tokens costs us nothing.
    let usable = state
        .store
        .password_reset_by_hash(&hash)
        .await?
        .is_some_and(|r| r.expires_at >= now);
    if !usable {
        return Err(expired());
    }
    let password_hash = hash_password(&body.password).await;

    let mut tx = state.store.begin().await?;
    // Taken in the transaction: of two requests with one link, one wins.
    let Some(reset) = tx.take_password_reset(&hash).await? else {
        return Err(expired());
    };
    if reset.expires_at < now {
        return Err(expired()); // (rolled back: an expired link is left as it was)
    }
    let user = match tx.user_by_id(&reset.user_id).await? {
        Some(user) if user.active() => user,
        _ => {
            tx.commit().await?; // the link is used up
            return Err(expired());
        }
    };
    tx.set_user_password(&user.id, Some(&password_hash)).await?;
    tx.set_user_email_verified(&user.id, true).await?;
    tx.delete_user_sessions(&user.id).await?;
    tx.delete_user_password_resets(&user.id).await?;
    tx.commit().await?;
    state.sessions.forget_user(&user.id);
    state.hub.drop_account(&user.id).await;
    let completed = Event::PasswordResetCompleted { user: user.id };
    emit(&state.ext, completed).await;
    Ok(Json(
        json!({"message": "your password is set; sign in again in openswx"}),
    ))
}

// ------------------------------------------------------------------ the page a reset link opens

const RESET_STYLE: &str = "body{font:16px system-ui;max-width:28em;margin:4em auto;padding:0 1em}\
input{width:100%;font:inherit;padding:.4em}button{font:inherit;padding:.4em 1em}";

/// Reads the token from the page's own address (it is never written into the page), checks the two
/// fields match, and posts them.
const RESET_SCRIPT: &str = r#"
const token = decodeURIComponent(location.pathname.split("/").pop() || "");
document.getElementById("f").onsubmit = async (e) => {
  e.preventDefault();
  const m = document.getElementById("m"), p = document.getElementById("p").value;
  if (p !== document.getElementById("q").value) { m.textContent = "The two passwords differ."; return; }
  const r = await fetch("/api/auth/password-reset/confirm", {method: "POST",
    headers: {"content-type": "application/json"}, body: JSON.stringify({token, password: p})});
  const d = await r.json().catch(() => ({}));
  m.textContent = r.ok ? d.message : (typeof d.detail === "string" ? d.detail : "That did not work.");
  if (r.ok) document.getElementById("f").remove();
};
"#;

/// The page, the same for every link.
static RESET_PAGE: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width">
<meta name=referrer content=no-referrer><title>Choose a new password</title>
<style>{RESET_STYLE}</style>
<body>
<h1>Choose a new password</h1>
<form id=f><p><label>New password<br><input id=p type=password minlength=8 maxlength=200 required autocomplete=new-password></label></p>
<p><label>Again<br><input id=q type=password minlength=8 maxlength=200 required autocomplete=new-password></label></p>
<p><button>Set password</button></p></form><p id=m role=status></p>
<script>{RESET_SCRIPT}</script></body>"#
    )
});

fn csp_hash(source: &str) -> String {
    format!(
        "'sha256-{}'",
        STANDARD.encode(Sha256::digest(source.as_bytes()))
    )
}

/// Nothing but the page's own script and style, requests to this server only, no framing, and no
/// form submission (if the script does not run, the password never goes into a URL).
static RESET_CSP: LazyLock<String> = LazyLock::new(|| {
    format!(
        "default-src 'none'; script-src {}; style-src {}; connect-src 'self'; base-uri 'none'; \
         form-action 'none'; frame-ancestors 'none'",
        csp_hash(RESET_SCRIPT),
        csp_hash(RESET_STYLE)
    )
});

/// The page a reset link opens. The token in the address is not read here: the page reads it.
async fn reset_page() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    match HeaderValue::from_str(&RESET_CSP) {
        Ok(csp) => {
            headers.insert(CONTENT_SECURITY_POLICY, csp);
        }
        Err(err) => return ApiError::Internal(err.into()).into_response(),
    }
    (headers, RESET_PAGE.as_str()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xff(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", HeaderValue::from_str(value).unwrap());
        h
    }

    fn peer(ip: &str) -> Option<SocketAddr> {
        Some(SocketAddr::new(ip.parse().unwrap(), 5000))
    }

    #[test]
    fn the_client_address_is_the_peer_unless_a_local_proxy_forwarded_it() {
        assert_eq!(client_ip(None, &HeaderMap::new()), "");
        assert_eq!(client_ip(peer("10.0.0.7"), &HeaderMap::new()), "10.0.0.7");
        // Only a proxy on 127.0.0.1 is believed.
        assert_eq!(client_ip(peer("10.0.0.7"), &xff("1.2.3.4")), "10.0.0.7");
        assert_eq!(client_ip(peer("::1"), &xff("1.2.3.4")), "::1");
        assert_eq!(client_ip(peer("127.0.0.1"), &xff("1.2.3.4")), "1.2.3.4");
        assert_eq!(
            client_ip(peer("::ffff:127.0.0.1"), &xff("1.2.3.4")),
            "1.2.3.4"
        );
        // The rightmost address that is not the proxy: what the proxy saw, not what the client wrote.
        assert_eq!(
            client_ip(peer("127.0.0.1"), &xff("6.6.6.6, 1.2.3.4")),
            "1.2.3.4"
        );
        assert_eq!(
            client_ip(peer("127.0.0.1"), &xff("1.2.3.4, 127.0.0.1")),
            "1.2.3.4"
        );
        assert_eq!(
            client_ip(peer("127.0.0.1"), &xff("127.0.0.1, 127.0.0.1")),
            "127.0.0.1"
        );
        assert_eq!(client_ip(peer("127.0.0.1"), &HeaderMap::new()), "127.0.0.1");
        let long = "x".repeat(10_000);
        assert_eq!(client_ip(peer("127.0.0.1"), &xff(&long)).len(), 64);
    }

    #[test]
    fn the_reset_page_policy_allows_exactly_its_own_script_and_style() {
        let page = RESET_PAGE.as_str();
        assert!(page.contains(&format!("<script>{RESET_SCRIPT}</script>")));
        assert!(page.contains(&format!("<style>{RESET_STYLE}</style>")));
        assert!(!page.contains("style=\""), "no inline style attributes");
        assert!(RESET_CSP.contains(&csp_hash(RESET_SCRIPT)));
        assert!(RESET_CSP.starts_with("default-src 'none';"));
    }
}
