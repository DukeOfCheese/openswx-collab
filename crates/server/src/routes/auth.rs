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
//! Sign-in attempts are limited per email address and client address (10 in 10 minutes; a
//! success clears the count) and per email address alone (100 in an hour, whatever the client:
//! guessing from many addresses at once); reset requests per address and client and per address
//! alone (5 an hour). Every limit counts the attempt before the password is checked, in one step,
//! so that parallel requests cannot all pass. An IPv6 client is counted by its /64 (what one
//! subscriber is usually given), as the connection limits count it. The client address is the
//! connection's peer, except that a peer listed in `OSWX_COLLAB_TRUSTED_PROXIES` (by default
//! `127.0.0.1`, a proxy on this machine, as uvicorn believes by default for the Python server) is
//! taken at its word in `X-Forwarded-For` (see [`client_ip`]).

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::extract::{ConnectInfo, FromRequestParts, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::access::CurrentUser;
use crate::config::{IpNet, Settings};
use crate::error::{ApiError, ApiJson};
use crate::ext::{Event, emit};
use crate::mail::{Mailed, reset_text, send_after_commit};
use crate::routes::meta::{Registration, registration_for};
use crate::routes::orgs::{accept_invite, approve_invite, find_invite};
use crate::routes::reset_page::reset_page;
use crate::security::{
    RateLimit, check_password_len, clean_email, hash_password, new_token, token_hash, verify_dummy,
    verify_password,
};
use crate::serve::counted_as;
use crate::state::{AppState, Committed};
use crate::store::{PasswordReset, Session, Tx, User, new_id};

/// The longest email address accepted (the column's size).
pub(crate) const EMAIL_MAX: usize = 320;
/// The longest account name (the column's size).
pub(crate) const NAME_MAX: usize = 200;
/// The longest session label kept; longer ones are cut, as the Python server does.
const LABEL_MAX: usize = 200;
/// The longest invite, reset token or login code accepted (real ones are 43 characters).
pub(crate) const TOKEN_MAX: usize = 256;
/// The longest password accepted where one is only checked, never set (sign-in, the current
/// password): longer than any password this server sets, short enough to bound the work.
pub(crate) const CHECKED_PASSWORD_MAX: usize = 1024;
/// How recent a sign-in must be to stand in for the password of an account that has none.
const RECENT_SIGN_IN_SECS: f64 = 600.0;

pub(crate) const INVITE_ONLY: &str =
    "this server is invite-only: ask an organisation owner for an invite link";
const EMAIL_TAKEN: &str = "there is already an account with this email address";
const RESET_ANSWER: &str = "if there is an account for this address, a link is on its way";
const RESET_EXPIRED: &str = "this link has expired or was already used; ask for a new one";
const NO_EMAIL: &str = "this server cannot send email: ask its operator to reset your password";

/// Sign-in attempts per email address and client in [`LOGIN_WINDOW_SECS`].
const LOGIN_MAX: usize = 10;
const LOGIN_WINDOW_SECS: u64 = 600;
/// Sign-in attempts per email address, from any client, in [`LOGIN_EMAIL_WINDOW_SECS`].
pub(crate) const LOGIN_EMAIL_MAX: usize = 100;
const LOGIN_EMAIL_WINDOW_SECS: u64 = 3600;

/// The sign-in and reset limits; one set per app.
#[derive(Debug)]
struct Limits {
    /// Per email address and client.
    login: RateLimit,
    /// Per email address alone.
    login_email: RateLimit,
    reset: RateLimit,
}

pub(crate) fn router() -> Router<AppState> {
    let limits = Arc::new(Limits {
        login: RateLimit::new(LOGIN_MAX, LOGIN_WINDOW_SECS),
        login_email: RateLimit::new(LOGIN_EMAIL_MAX, LOGIN_EMAIL_WINDOW_SECS),
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
pub(crate) fn max_len(field: &str, value: &str, max: usize) -> Result<(), ApiError> {
    if value.contains('\0') {
        return Err(ApiError::Unprocessable(crate::error::ZERO_CHARACTER.into()));
    }
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
        label: label
            .chars()
            .filter(|c| *c != '\0')
            .take(LABEL_MAX)
            .collect(),
    })
    .await?;
    Ok(token)
}

/// The client's address as the limits count it (see the module documentation): [`client_ip`],
/// with an IPv6 address cut to its /64 ([`counted_as`]).
pub(crate) struct ClientAddr(pub(crate) String);

/// The client's address: the connection's peer, unless the peer is one of the `trusted` proxies
/// and sent `X-Forwarded-For`. Then the header's addresses are read from the right (the last one
/// was added by the peer itself, the one before by the proxy before it, and so on; several
/// headers count as one list), and the client is the first that is not a trusted proxy: what the
/// outermost proxy of ours saw, whatever the client itself wrote to the left of it. If every
/// address is a trusted proxy, it is the left-most one.
///
/// An entry that is not an address (a host name, an address with a port, anything a client might
/// forge) ends the reading: the client is then the last address read before it, or the peer. So
/// the answer is always the peer's address or one a trusted proxy vouched for, in its canonical
/// spelling; never text from the header. `""` without a peer (the app was served without
/// connection info).
fn client_ip(peer: Option<SocketAddr>, headers: &HeaderMap, trusted: &[IpNet]) -> String {
    let Some(peer) = peer else {
        return String::new();
    };
    let is_trusted = |ip: IpAddr| trusted.iter().any(|range| range.contains(ip));
    let mut client = peer.ip().to_canonical();
    if !is_trusted(client) {
        return client.to_string();
    }
    'headers: for value in headers.get_all("x-forwarded-for").iter().rev() {
        let Ok(value) = value.to_str() else {
            break;
        };
        for entry in value.rsplit(',') {
            let Ok(ip) = entry.trim().parse::<IpAddr>() else {
                break 'headers;
            };
            client = ip.to_canonical();
            if !is_trusted(client) {
                break 'headers;
            }
        }
    }
    client.to_string()
}

impl FromRequestParts<AppState> for ClientAddr {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let peer = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|c| c.0);
        let ip = client_ip(peer, &parts.headers, &state.settings.trusted_proxies);
        Ok(Self(match ip.parse::<IpAddr>() {
            Ok(ip) => counted_as(ip).to_string(),
            Err(_) => ip,
        }))
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

    // (Joining an organisation changes it: this request takes its turn, as any other does.)
    let turn = match &approval {
        Some(approval) => Some(state.org_turn(approval.org()).await?),
        None => None,
    };
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
    if let Err(e) = tx.insert_user(&user).await {
        // Two registrations with one address at once: the second finds it taken only here.
        return Err(if crate::store::is_unique_violation(&e) {
            ApiError::Conflict(EMAIL_TAKEN.into())
        } else {
            e.into()
        });
    }
    let accepted = match &invite {
        Some(invite) => {
            Some(accept_invite(&mut *tx, invite, approval.as_ref(), &user.id, now).await?)
        }
        None => None,
    };
    let token = start_session(&mut *tx, &state.settings, &user.id, "", now).await?;
    let registered = Event::Registered {
        user: user.id.clone(),
    };
    let ext = state.ext.clone();
    state
        .commit_then((tx, turn), |_| async {}, async move {
            emit(&ext, registered).await;
            if let Some(accepted) = accepted {
                accepted.announce(ext).await;
            }
        })
        .await?;
    tracing::info!(user = %user.id, server_admin = first, "account created");
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
    // The attempt is counted before the (slow) check, in one step per limit: parallel requests
    // cannot all find the count below the limit and then all fail. A success clears the count.
    if !limits.login.check(&key, now)
        || (!email.is_empty() && !limits.login_email.check(&email, now))
    {
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
    // Checked again here rather than trusting that `verify_dummy` is always false.
    let Some(user) = user.filter(|u| accepted && u.active()) else {
        return Err(ApiError::WrongCredentials);
    };
    limits.login.reset(&key);
    limits.login_email.reset(&email);
    let mut tx = state.store.begin().await?;
    tx.delete_expired_user_sessions(&user.id, now).await?;
    let token = start_session(&mut *tx, &state.settings, &user.id, &body.label, now).await?;
    let signed_in = Event::SignedIn {
        user: user.id.clone(),
    };
    let ext = state.ext.clone();
    state
        .commit_then(tx, |_| async {}, async move { emit(&ext, signed_in).await })
        .await?;
    Ok(Json(json!({"token": token, "user": me_out(&user)})))
}

/// Ends the session of the token presented (only that one), and closes the document connections
/// made with it.
async fn logout(State(state): State<AppState>, user: CurrentUser) -> Result<StatusCode, ApiError> {
    let mut tx = state.store.begin().await?;
    tx.delete_session(&user.token_hash).await?;
    let (revoker, hash) = (state.clone(), user.token_hash.clone());
    let ext = state.ext.clone();
    state
        .commit_then(
            tx,
            move |committed: Committed| async move {
                let (code, reason) = committed.session_close();
                revoker.sessions.forget_token(&hash);
                revoker.hub.drop_session_with(&hash, code, reason).await;
            },
            async move { emit(&ext, Event::SignedOut { user: user.id }).await },
        )
        .await?;
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
        // A reset link sent before the change would undo it.
        tx.delete_user_password_resets(&user.id).await?;
    }
    let changed = password_hash.is_some();
    let (revoker, ext) = (state.clone(), state.ext.clone());
    let (revoked, announced) = (user.id.clone(), user.id.clone());
    state
        .commit_then(
            tx,
            move |committed: Committed| async move {
                if changed {
                    let (code, reason) = committed.session_close();
                    revoker.sessions.forget_user(&revoked);
                    revoker.hub.drop_account_with(&revoked, code, reason).await;
                }
            },
            async move {
                if changed {
                    emit(&ext, Event::PasswordChanged { user: announced }).await;
                }
            },
        )
        .await?;
    let user = User {
        name: name.map_or(user.name, str::to_owned),
        password_hash: password_hash.or(user.password_hash),
        ..user
    };
    Ok(Json(me_out(&user)))
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
    // Counted in one step per limit, so that parallel requests cannot all pass.
    if !limits.reset.check(&key, now) || !limits.reset.check(&email, now) {
        return Ok(answer);
    }
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
    // Only the newest link works: one request at a time per account replaces it.
    tx.lock_user(&user.id).await?;
    tx.delete_user_password_resets(&user.id).await?;
    tx.delete_expired_password_resets(now).await?;
    tx.insert_password_reset(&PasswordReset {
        token_hash: token_hash(&token),
        user_id: user.id.clone(),
        expires_at: now + f64::from(settings.reset_minutes) * 60.0,
    })
    .await?;
    let url = format!(
        "{}/reset-password/{token}",
        settings.public_url.trim_end_matches('/')
    );
    let (subject, text) = reset_text(&url, settings.reset_minutes);
    let after = state.clone();
    state
        .commit_then(tx, |_| async {}, async move {
            // (Not held up by a slow mail server: see `send_after_commit`.)
            match send_after_commit(&after, user.email.clone(), subject, text).await {
                Mailed::Sent => tracing::info!(user = %user.id, "password reset link sent"),
                Mailed::Pending => tracing::info!(
                    user = %user.id,
                    "password reset link handed on; the mail server has not answered yet"
                ),
                Mailed::Failed => tracing::warn!(
                    user = %user.id,
                    "password reset link not sent: the mail server did not take it"
                ),
                Mailed::Refused => tracing::warn!(
                    user = %user.id,
                    "password reset link not sent: too many emails were on their way"
                ),
                Mailed::NotConfigured => {
                    tracing::warn!(user = %user.id, "password reset link not sent: no mail server");
                }
            }
            let requested = Event::PasswordResetRequested { user: user.id };
            emit(&after.ext, requested).await;
        })
        .await?;
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
    let (revoker, revoked) = (state.clone(), user.id.clone());
    let ext = state.ext.clone();
    state
        .commit_then(
            tx,
            move |committed: Committed| async move {
                let (code, reason) = committed.session_close();
                revoker.sessions.forget_user(&revoked);
                revoker.hub.drop_account_with(&revoked, code, reason).await;
            },
            async move {
                tracing::info!(user = %user.id, "password set through a reset link");
                let completed = Event::PasswordResetCompleted { user: user.id };
                emit(&ext, completed).await;
            },
        )
        .await?;
    Ok(Json(
        json!({"message": "your password is set; sign in again in openswx"}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn xff(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", HeaderValue::from_str(value).unwrap());
        h
    }

    fn peer(ip: &str) -> Option<SocketAddr> {
        Some(SocketAddr::new(ip.parse().unwrap(), 5000))
    }

    fn ranges(list: &[&str]) -> Vec<IpNet> {
        list.iter().map(|r| r.parse().unwrap()).collect()
    }

    #[test]
    fn the_client_address_is_the_peer_unless_a_trusted_proxy_forwarded_it() {
        let local = ranges(&["127.0.0.1"]);
        let ip = |peer_ip: &str, headers: &HeaderMap| client_ip(peer(peer_ip), headers, &local);
        assert_eq!(client_ip(None, &xff("1.2.3.4"), &local), "");
        assert_eq!(ip("10.0.0.7", &HeaderMap::new()), "10.0.0.7");
        // A peer that is not listed is not believed: a forged header changes nothing.
        assert_eq!(ip("10.0.0.7", &xff("1.2.3.4")), "10.0.0.7");
        assert_eq!(ip("10.0.0.7", &xff("127.0.0.1")), "10.0.0.7");
        assert_eq!(ip("::1", &xff("1.2.3.4")), "::1");
        assert_eq!(ip("127.0.0.1", &xff("1.2.3.4")), "1.2.3.4");
        assert_eq!(ip("::ffff:127.0.0.1", &xff("1.2.3.4")), "1.2.3.4");
        // The rightmost address that is not the proxy: what the proxy saw, not what the client wrote.
        assert_eq!(ip("127.0.0.1", &xff("6.6.6.6, 1.2.3.4")), "1.2.3.4");
        assert_eq!(ip("127.0.0.1", &xff("1.2.3.4, 127.0.0.1")), "1.2.3.4");
        assert_eq!(ip("127.0.0.1", &xff("127.0.0.1, 127.0.0.1")), "127.0.0.1");
        assert_eq!(ip("127.0.0.1", &HeaderMap::new()), "127.0.0.1");
        // Nobody is trusted: the header is never read.
        assert_eq!(
            client_ip(peer("127.0.0.1"), &xff("1.2.3.4"), &[]),
            "127.0.0.1"
        );
    }

    #[test]
    fn a_chain_of_proxies_is_read_from_the_right_up_to_the_first_stranger() {
        // The Docker bridge, and a load balancer in front of it.
        let ours = ranges(&["172.16.0.0/12", "10.0.0.5", "fd00::/8"]);
        let ip = |peer_ip: &str, headers: &HeaderMap| client_ip(peer(peer_ip), headers, &ours);
        assert_eq!(ip("172.18.0.1", &xff("203.0.113.9")), "203.0.113.9");
        assert_eq!(
            ip("172.18.0.1", &xff("203.0.113.9, 10.0.0.5")),
            "203.0.113.9"
        );
        // What the client wrote itself is to the left of what our proxies added, and is never
        // reached: neither an address of its choice, nor one of our own proxies' addresses.
        assert_eq!(
            ip("172.18.0.1", &xff("6.6.6.6, 203.0.113.9, 10.0.0.5")),
            "203.0.113.9"
        );
        assert_eq!(
            ip("172.18.0.1", &xff("172.18.0.1, 10.0.0.5, 203.0.113.9")),
            "203.0.113.9"
        );
        assert_eq!(
            ip("172.18.0.1", &xff("127.0.0.1, 203.0.113.9")),
            "203.0.113.9"
        );
        // Every hop is ours: the first of them.
        assert_eq!(ip("172.18.0.1", &xff("10.0.0.5, 172.20.1.1")), "10.0.0.5");
        // IPv6, and an IPv4 address written as IPv6.
        assert_eq!(ip("fd00::2", &xff("2001:db8::7")), "2001:db8::7");
        assert_eq!(ip("fd00::2", &xff("::ffff:203.0.113.9")), "203.0.113.9");
        assert_eq!(
            ip("172.18.0.1", &xff("203.0.113.9, ::ffff:10.0.0.5")),
            "203.0.113.9"
        );
        // Several headers are one list, in order.
        let mut two = HeaderMap::new();
        two.append("x-forwarded-for", HeaderValue::from_static("6.6.6.6"));
        two.append(
            "x-forwarded-for",
            HeaderValue::from_static("203.0.113.9, 10.0.0.5"),
        );
        assert_eq!(ip("172.18.0.1", &two), "203.0.113.9");
        let mut two = HeaderMap::new();
        two.append("x-forwarded-for", HeaderValue::from_static("203.0.113.9"));
        two.append("x-forwarded-for", HeaderValue::from_static("10.0.0.5"));
        assert_eq!(ip("172.18.0.1", &two), "203.0.113.9");
    }

    #[test]
    fn a_malformed_forwarded_header_never_becomes_the_client_address() {
        let ours = ranges(&["172.16.0.0/12", "10.0.0.5"]);
        let ip = |headers: &HeaderMap| client_ip(peer("172.18.0.1"), headers, &ours);
        let long = "x".repeat(10_000);
        for junk in [
            "",
            " ",
            ",",
            "unknown",
            "localhost",
            "1.2.3.4:5678",
            "[2001:db8::7]",
            "[2001:db8::7]:443",
            "1.2.3",
            "1.2.3.4.5",
            "999.1.1.1",
            "1.2.3.4 5.6.7.8",
            "1.2.3.4;5.6.7.8",
            "0x7f.0.0.1",
            "1.2.3.4/32",
            "_hidden",
            &long,
        ] {
            // Alone: the peer (the proxy) it is.
            assert_eq!(ip(&xff(junk)), "172.18.0.1", "{junk:?}");
            // To the left of a real client: never reached.
            assert_eq!(
                ip(&xff(&format!("{junk}, 203.0.113.9"))),
                "203.0.113.9",
                "{junk:?}"
            );
            // To the right of our own proxy's entry: the last address read before it.
            assert_eq!(
                ip(&xff(&format!("203.0.113.9, {junk}, 10.0.0.5"))),
                "10.0.0.5",
                "{junk:?}"
            );
        }
        // Bytes that are not text.
        let mut h = HeaderMap::new();
        h.insert(
            "x-forwarded-for",
            HeaderValue::from_bytes(b"203.0.113.9, \xff\xfe").unwrap(),
        );
        assert_eq!(ip(&h), "172.18.0.1");
        // Thousands of entries are read without harm.
        let many = vec!["10.0.0.5"; 5000].join(",");
        assert_eq!(ip(&xff(&many)), "10.0.0.5");
        assert_eq!(ip(&xff(&format!("203.0.113.9,{many}"))), "203.0.113.9");
    }
}
