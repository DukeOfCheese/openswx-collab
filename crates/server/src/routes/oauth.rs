//! Sign-in through Google or GitHub, made for a desktop app, and adding or removing those providers
//! on an account.
//!
//! - `GET /api/auth/oauth/{provider}/start?redirect_uri=&code_challenge=&invite=`: 302 to the provider
//! - `GET /api/auth/oauth/{provider}/callback?code=&state=&error=`: where the provider sends the
//!   browser back; 302 to the app's loopback address with `?code=` or `?error=`
//! - `POST /api/auth/exchange {code, code_verifier, label}`: the code for a session, `{token, user}`
//! - `POST /api/auth/oauth/{provider}/link {redirect_uri, code_challenge, current_password}`: signed
//!   in, start adding a provider; `{url}` to open in the browser
//! - `POST /api/auth/oauth/link/finish {code, code_verifier}`: signed in, attach what came back
//! - `GET /api/me/providers`, `POST /api/me/providers/{provider}/unlink {current_password}`
//!
//! # The round trip
//!
//! The app listens on a loopback port, makes up a secret `code_verifier` (43 to 200 characters) and
//! opens `start` in the system browser with `redirect_uri=http://127.0.0.1:<port>/...` and
//! `code_challenge` = the verifier's SHA-256 in lower-case hex. The server remembers the sign-in
//! (`oauth_states`, for [`STATE_SECS`]: the hash of a random `state`, the provider, the redirect
//! address, the hash of the invite, the challenge, and for a link the account) and sends the browser
//! to the provider. The provider sends it back to `callback` with that `state`; the state is taken
//! (deleted, once) and committed **before** the provider is called, and the provider is called with
//! no transaction open. The server then sends the browser to the loopback address with a one-time
//! code (`login_codes`, hashed, for [`CODE_SECS`]) or an error, and the app trades the code and its
//! verifier at `exchange`.
//!
//! Only loopback addresses are accepted as `redirect_uri` ([`loopback`]), so a code is never sent to
//! another machine; a refused address is answered 400 directly, never redirected to. A code picked up
//! by another program on the same machine is useless without the verifier, and one wrong verifier
//! uses the code up.
//!
//! # Who gets in (the Python server's rules)
//!
//! A provider identity already attached to an account signs in to it. Otherwise the provider's
//! **verified** address decides: no verified address, `no_verified_email`; no account with it, one
//! is created when registration allows (the first account, which becomes the server admin; open
//! registration; or an invite carried from `start`, with the seat hook asked before the
//! transaction), else `invite_required`; an account whose address nobody ever confirmed is not
//! joined (`account_exists_use_password`: whoever registered it may not own the address); a
//! confirmed one gets the identity attached. A disabled or deleted account is `account_disabled`.
//!
//! A link (`link_user_id` set) attaches nothing at the callback: whoever sits at that browser may
//! not be the account's owner. The code it ends with only says which identity came back; the app
//! that started it, signed in as that account and holding the verifier, attaches it at
//! `link/finish`.
//!
//! The loopback address gets `?code=` or `?error=` with one of a closed set of codes,
//! [`LOOPBACK_ERRORS`]:
//!
//! - this server's: `cancelled` (the provider said the person declined, `access_denied`, or sent
//!   no code), `provider_refused`, `provider_unreachable`, `identity_in_use`, `no_verified_email`,
//!   `invite_required`, `not_allowed` (the seat hook or the invite refused),
//!   `account_exists_use_password`, `account_disabled`, `server_error` (this server failed after
//!   the sign-in state was used; logged);
//! - the provider's own RFC 6749 §4.1.2.1 codes, passed through: `invalid_request`,
//!   `unauthorized_client`, `unsupported_response_type`, `invalid_scope`, `server_error`,
//!   `temporarily_unavailable`. Any other provider error becomes `provider_refused`.
//!
//! # Limits
//!
//! `start` and `link` together are limited to [`START_LIMIT`] requests per [`START_WINDOW_SECS`]
//! per client address (429, the address worked out as for password sign-in), so they cannot be
//! used to make this server hammer the providers; each `start` allows at most one callback, so the
//! callback needs no limit of its own. `exchange` and `link/finish` are not limited: codes are 32
//! random bytes and used once, so guessing is hopeless, and behind a proxy that hides client
//! addresses a limit would be one budget for everybody.

use std::ops::RangeInclusive;
use std::sync::Arc;

use axum::extract::State;
use axum::http::header::{CACHE_CONTROL, LOCATION, REFERRER_POLICY};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use url::Url;

use crate::access::CurrentUser;
use crate::error::{ApiError, ApiJson, ApiPath, ApiQuery};
use crate::ext::{Event, emit};
use crate::oauth::{Identity, OAuthError, Provider};
use crate::routes::auth::{
    CHECKED_PASSWORD_MAX, ClientAddr, EMAIL_MAX, NAME_MAX, TOKEN_MAX, confirm, max_len, me_out,
    start_session,
};
use crate::routes::meta::{Registration, registration_for};
use crate::routes::orgs::{
    Accepted, Approval, accept_invite, approve_invite_by_hash, find_invite_by_hash,
};
use crate::security::{RateLimit, clean_email, new_token, token_hash};
use crate::state::AppState;
use crate::store::{LoginCode, OAuthIdentity, OAuthState, Tx, User, new_id};

/// How long a started sign-in waits for the browser to come back from the provider.
pub const STATE_SECS: f64 = 600.0;
/// How long the code the browser brings back is valid.
pub const CODE_SECS: f64 = 120.0;
/// The longest `redirect_uri` accepted.
const REDIRECT_MAX: usize = 2048;
/// The length of a `code_verifier`, in characters.
const VERIFIER_LEN: RangeInclusive<usize> = 43..=200;
/// The longest code a provider may send to the callback.
const PROVIDER_CODE_MAX: usize = 2048;
/// `start` and `link` requests per [`START_WINDOW_SECS`] per client address (see the module
/// documentation).
pub const START_LIMIT: usize = 600;
/// The window of [`START_LIMIT`]: ten minutes.
pub const START_WINDOW_SECS: u64 = 600;
/// The provider error codes (RFC 6749 section 4.1.2.1) passed through to the app as they are.
/// (`access_denied` becomes `cancelled`.)
const PROVIDER_ERRORS: [&str; 6] = [
    "invalid_request",
    "unauthorized_client",
    "unsupported_response_type",
    "invalid_scope",
    "server_error",
    "temporarily_unavailable",
];
/// Every `error` the app's loopback address can get (see the module documentation).
pub const LOOPBACK_ERRORS: [&str; 15] = [
    "cancelled",
    "provider_refused",
    "provider_unreachable",
    "identity_in_use",
    "no_verified_email",
    "invite_required",
    "not_allowed",
    "account_exists_use_password",
    "account_disabled",
    "server_error",
    "invalid_request",
    "unauthorized_client",
    "unsupported_response_type",
    "invalid_scope",
    "temporarily_unavailable",
];
/// How many times a sign-in that creates an account with an invite is tried when the
/// organisation's members changed between the seat check and the transaction.
const SEAT_ATTEMPTS: usize = 3;
/// The hosts a `redirect_uri` may name.
const LOOPBACK_HOSTS: [&str; 3] = ["127.0.0.1", "localhost", "[::1]"];

const STATE_EXPIRED: &str = "this sign-in has expired; start again from the app";
const REDIRECT_REFUSED: &str =
    "redirect_uri must be a loopback address (http://127.0.0.1:<port>/...)";
const CHALLENGE_REFUSED: &str =
    "code_challenge must be the SHA-256 (lower-case hex) of the app's code_verifier";
const CODE_INVALID: &str = "this code is not valid any more; sign in again";
const OTHER_APP: &str = "this code was issued to another app; sign in again";
const LINK_CODE: &str = "this code adds a sign-in method; finish it from the signed-in app";
const DISABLED: &str = "this account is disabled";
const LINK_CODE_INVALID: &str = "this code is not valid any more; start again";
const NOT_THIS_LINK: &str = "this code was not issued to this app and account; start again";
const IDENTITY_TAKEN: &str = "that provider account already signs in to another account here";
const ONLY_WAY_IN: &str = "this is the only way into the account; set a password first";

/// The limit on `start` and `link`; one per app.
#[derive(Debug)]
struct Limits {
    start: RateLimit,
}

pub(crate) fn router() -> Router<AppState> {
    let limits = Arc::new(Limits {
        start: RateLimit::new(START_LIMIT, START_WINDOW_SECS),
    });
    Router::new()
        .route("/api/auth/oauth/{name}/start", get(start))
        .route("/api/auth/oauth/{name}/link", post(link))
        .route("/api/auth/oauth/{name}/callback", get(callback))
        .route("/api/auth/exchange", post(exchange))
        .route("/api/auth/oauth/link/finish", post(link_finish))
        .route("/api/me/providers", get(providers))
        .route("/api/me/providers/{name}/unlink", post(unlink))
        .layer(Extension(limits))
}

// ------------------------------------------------------------------ checks

/// The provider called `name` if this server has it set up (a client id and secret); 404 otherwise.
fn configured(state: &AppState, name: &str) -> Result<Provider, ApiError> {
    match Provider::parse(name) {
        Some(provider) if state.settings.oauth_providers().contains(&provider.name()) => {
            Ok(provider)
        }
        Some(provider) => Err(ApiError::NotFoundDetail(format!(
            "sign-in with {} is not set up on this server",
            provider.name()
        ))),
        None => Err(ApiError::NotFound(
            "sign-in with that provider is not set up on this server",
        )),
    }
}

/// `redirect_uri` as it will be redirected to (normalised by a URL parser), if it is an address on
/// this machine: printable ASCII with no `\`, at most 2048 characters, scheme `http`,
/// host exactly `127.0.0.1`, `localhost` or `[::1]` **as written** (`127.1` or `0x7f000001`, which
/// a parser turns into `127.0.0.1`, are refused, as the Python server refuses them), an explicit port
/// of 1 to 65535, no user name or password, no fragment. A path and a query are allowed.
pub fn loopback(redirect_uri: &str) -> Option<String> {
    if redirect_uri.len() > REDIRECT_MAX
        || !redirect_uri
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b'\\')
    {
        return None;
    }
    let url = Url::parse(redirect_uri).ok()?;
    if url.scheme() != "http"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let host = url.host_str()?;
    if !LOOPBACK_HOSTS.contains(&host) {
        return None;
    }
    // The authority as written: "http://" + the host + ":" + digits, then the path, query or end.
    let (scheme, rest) = redirect_uri.split_at_checked(7)?;
    if !scheme.eq_ignore_ascii_case("http://") {
        return None;
    }
    let rest = rest.to_ascii_lowercase();
    let after_host = rest.strip_prefix(host)?.strip_prefix(':')?;
    let port = &after_host[..after_host.find(['/', '?', '#']).unwrap_or(after_host.len())];
    if port.is_empty() || port.len() > 5 || !port.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !(1..=65535).contains(&port.parse::<u32>().ok()?) {
        return None;
    }
    Some(url.into())
}

/// Whether `challenge` is a SHA-256 in lower-case hex.
fn is_challenge(challenge: &str) -> bool {
    challenge.len() == 64
        && challenge
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Whether `verifier`'s SHA-256 is `challenge`, compared in constant time.
fn verifier_matches(verifier: &str, challenge: &str) -> bool {
    let got = hex::encode(Sha256::digest(verifier.as_bytes()));
    got.as_bytes().ct_eq(challenge.as_bytes()).into()
}

/// 422 unless `verifier` has a length in [`VERIFIER_LEN`].
fn check_verifier(verifier: &str) -> Result<(), ApiError> {
    if VERIFIER_LEN.contains(&verifier.chars().count()) {
        Ok(())
    } else {
        Err(ApiError::Unprocessable(
            "code_verifier must be 43 to 200 characters".into(),
        ))
    }
}

/// 429 when the client address is over `limit` (the request counts).
fn limited(limit: &RateLimit, ip: &str, now: f64) -> Result<(), ApiError> {
    if limit.check(ip, now) {
        Ok(())
    } else {
        Err(ApiError::RateLimited)
    }
}

/// This server's callback address for `provider`, as registered with the provider.
fn callback_url(state: &AppState, provider: Provider) -> String {
    format!(
        "{}/api/auth/oauth/{}/callback",
        state.settings.public_url.trim_end_matches('/'),
        provider.name()
    )
}

/// Commits `tx` (a code taken in it stays used up) and answers `error`.
async fn used_up<T>(state: &AppState, tx: Box<dyn Tx>, error: ApiError) -> Result<T, ApiError> {
    commit(state, tx).await?;
    Err(error)
}

/// Commits a transaction that has nothing to announce.
async fn commit(state: &AppState, tx: Box<dyn Tx>) -> Result<(), ApiError> {
    state.commit_then(tx, |_| async {}, async {}).await
}

/// The signed-in account's row (401 if it vanished since the session was checked).
async fn load(state: &AppState, user: &CurrentUser) -> Result<User, ApiError> {
    state
        .store
        .user_by_id(&user.id)
        .await?
        .ok_or(ApiError::Unauthorized)
}

// ------------------------------------------------------------------ starting

/// Remembers a sign-in that is starting and answers the provider's address to send the browser to.
/// 400 for a `redirect_uri` that is not a loopback address or a `code_challenge` that is not a
/// lower-case hex SHA-256.
async fn begin(
    state: &AppState,
    provider: Provider,
    redirect_uri: &str,
    challenge: &str,
    invite_hash: Option<String>,
    link_user_id: Option<&str>,
) -> Result<String, ApiError> {
    let redirect_uri =
        loopback(redirect_uri).ok_or_else(|| ApiError::BadRequest(REDIRECT_REFUSED.into()))?;
    if !is_challenge(challenge) {
        return Err(ApiError::BadRequest(CHALLENGE_REFUSED.into()));
    }
    let client_id = match provider {
        Provider::Google => &state.settings.google_client_id,
        Provider::Github => &state.settings.github_client_id,
    };
    let endpoints = provider.endpoints();
    let mut authorize = Url::parse(endpoints.authorize)?;
    let value = new_token();
    authorize
        .query_pairs_mut()
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", &callback_url(state, provider))
        .append_pair("response_type", "code")
        .append_pair("scope", endpoints.scope)
        .append_pair("state", &value);
    let now = state.clock.now();
    let mut tx = state.store.begin().await?;
    tx.delete_expired_oauth_states(now).await?;
    tx.insert_oauth_state(&OAuthState {
        state_hash: token_hash(&value),
        provider: provider.name().to_owned(),
        redirect_uri,
        invite: invite_hash,
        challenge: challenge.to_owned(),
        link_user_id: link_user_id.map(str::to_owned),
        expires_at: now + STATE_SECS,
    })
    .await?;
    commit(state, tx).await?;
    Ok(authorize.into())
}

#[derive(Debug, Deserialize)]
struct StartQuery {
    redirect_uri: Option<String>,
    code_challenge: Option<String>,
    invite: Option<String>,
}

/// Starts a browser sign-in: 302 to the provider. 422 without `redirect_uri` or `code_challenge`,
/// 404 for a provider that is not set up, 400 for a bad `redirect_uri` or challenge (see [`begin`]).
async fn start(
    State(state): State<AppState>,
    Extension(limits): Extension<Arc<Limits>>,
    ClientAddr(ip): ClientAddr,
    ApiPath(name): ApiPath<String>,
    ApiQuery(query): ApiQuery<StartQuery>,
) -> Result<Response, ApiError> {
    limited(&limits.start, &ip, state.clock.now())?;
    let (Some(redirect_uri), Some(challenge)) = (&query.redirect_uri, &query.code_challenge) else {
        return Err(ApiError::Unprocessable(
            "redirect_uri and code_challenge are required".into(),
        ));
    };
    let invite = query.invite.as_deref().filter(|i| !i.is_empty());
    if let Some(invite) = invite {
        max_len("invite", invite, TOKEN_MAX)?;
    }
    let provider = configured(&state, &name)?;
    let url = begin(
        &state,
        provider,
        redirect_uri,
        challenge,
        invite.map(token_hash),
        None,
    )
    .await?;
    redirect(&url)
}

#[derive(Debug, Deserialize)]
struct LinkIn {
    redirect_uri: String,
    code_challenge: String,
    #[serde(default)]
    current_password: Option<String>,
}

/// Starts adding a provider to the signed-in account: `{url}` to open in the browser. Needs the
/// current password, or for an account without one a sign-in from the last ten minutes (403); then
/// as `start`.
async fn link(
    State(state): State<AppState>,
    Extension(limits): Extension<Arc<Limits>>,
    ClientAddr(ip): ClientAddr,
    current: CurrentUser,
    ApiPath(name): ApiPath<String>,
    ApiJson(body): ApiJson<LinkIn>,
) -> Result<Json<Value>, ApiError> {
    limited(&limits.start, &ip, state.clock.now())?;
    if let Some(password) = &body.current_password {
        max_len("current_password", password, CHECKED_PASSWORD_MAX)?;
    }
    let user = load(&state, &current).await?;
    confirm(
        &state,
        &user,
        &current.token_hash,
        body.current_password.as_deref(),
    )
    .await?;
    let provider = configured(&state, &name)?;
    let url = begin(
        &state,
        provider,
        &body.redirect_uri,
        &body.code_challenge,
        None,
        Some(&user.id),
    )
    .await?;
    Ok(Json(json!({ "url": url })))
}

// ------------------------------------------------------------------ back from the provider

/// A 302 to `url`, not to be cached or passed on as a referrer.
fn redirect(url: &str) -> Result<Response, ApiError> {
    Ok((
        StatusCode::FOUND,
        [
            (LOCATION, HeaderValue::from_str(url)?),
            (CACHE_CONTROL, HeaderValue::from_static("no-store")),
            (REFERRER_POLICY, HeaderValue::from_static("no-referrer")),
        ],
    )
        .into_response())
}

/// How a callback ends, for the app's loopback address.
enum Back {
    /// `?code=`: a one-time code for `exchange` or `link/finish`.
    Code(String),
    /// `?error=`, one of [`LOOPBACK_ERRORS`].
    Error(&'static str),
}

impl Back {
    fn error(code: &'static str) -> Self {
        debug_assert!(LOOPBACK_ERRORS.contains(&code), "{code}");
        Self::Error(code)
    }
}

/// The 302 to the app's address with the outcome in the query (`&` when the address already has
/// one), as the Python server builds it.
fn back(redirect_uri: &str, outcome: &Back) -> Result<Response, ApiError> {
    let (key, value) = match outcome {
        Back::Code(code) => ("code", code.as_str()),
        Back::Error(error) => ("error", *error),
    };
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair(key, value)
        .finish();
    let separator = if redirect_uri.contains('?') { '&' } else { '?' };
    redirect(&format!("{redirect_uri}{separator}{query}"))
}

/// What a provider's own `error` becomes for the app (see the module documentation).
fn provider_error(error: &str) -> &'static str {
    if error == "access_denied" {
        return "cancelled";
    }
    PROVIDER_ERRORS
        .into_iter()
        .find(|known| *known == error)
        .unwrap_or("provider_refused")
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

/// Where the provider sends the browser back. 404 for a provider that is not set up; 400
/// [`STATE_EXPIRED`] (not a redirect: where to is not known) when the state is missing, unknown,
/// used, expired or another provider's. Otherwise always a 302 to the app's loopback address.
async fn callback(
    State(state): State<AppState>,
    ApiPath(name): ApiPath<String>,
    ApiQuery(query): ApiQuery<CallbackQuery>,
) -> Result<Response, ApiError> {
    let provider = configured(&state, &name)?;
    let expired = || ApiError::BadRequest(STATE_EXPIRED.into());
    let Some(value) = query.state.as_deref().filter(|s| s.len() <= TOKEN_MAX) else {
        return Err(expired());
    };
    let now = state.clock.now();
    let mut tx = state.store.begin().await?;
    // Taken (once) and committed before anything else: a replayed callback finds nothing.
    let sign_in = match tx.take_oauth_state(&token_hash(value)).await? {
        None => return Err(expired()),
        // Another provider's state stays (rolled back) for its own callback.
        Some(st) if st.provider != provider.name() => return Err(expired()),
        Some(st) if st.expires_at < now => {
            commit(&state, tx).await?;
            return Err(expired());
        }
        Some(st) => {
            commit(&state, tx).await?;
            st
        }
    };

    let outcome = match (
        query.error.as_deref().filter(|e| !e.is_empty()),
        &query.code,
    ) {
        (Some(error), _) => Back::error(provider_error(error)),
        (None, None) => Back::error("cancelled"),
        (None, Some(code)) if code.is_empty() => Back::error("cancelled"),
        (None, Some(code)) if code.len() > PROVIDER_CODE_MAX => Back::error("provider_refused"),
        (None, Some(code)) => match ask_provider(&state, provider, code).await {
            Err(OAuthError::Refused) => Back::error("provider_refused"),
            Err(OAuthError::Unreachable) => Back::error("provider_unreachable"),
            Ok(identity) => {
                let ended = match &sign_in.link_user_id {
                    Some(link_user_id) => {
                        link_code(
                            &state,
                            provider,
                            &identity,
                            link_user_id,
                            &sign_in.challenge,
                        )
                        .await
                    }
                    None => {
                        sign_in_code(
                            &state,
                            provider,
                            &identity,
                            sign_in.invite.as_deref(),
                            &sign_in.challenge,
                        )
                        .await
                    }
                };
                // The state is used up and the app's address is known: a failure goes back there
                // too, so the app is not left waiting.
                ended.unwrap_or_else(|err| {
                    match &err {
                        ApiError::Internal(e) => tracing::error!("a browser sign-in failed: {e:#}"),
                        // (Not its message: an extension's refusal can carry any text.)
                        other => tracing::error!(
                            status = other.status().as_u16(),
                            kind = other.kind(),
                            "a browser sign-in failed"
                        ),
                    }
                    Back::error("server_error")
                })
            }
        },
    };
    if let Back::Error(error) = &outcome {
        tracing::info!(
            provider = provider.name(),
            error = *error,
            "a browser sign-in ended without a code"
        );
    }
    back(&sign_in.redirect_uri, &outcome)
}

/// Trades the provider's code for an identity, each call bounded by the provider client's timeout
/// (a call that takes longer is [`OAuthError::Unreachable`]). No transaction is open here.
async fn ask_provider(
    state: &AppState,
    provider: Provider,
    code: &str,
) -> Result<Identity, OAuthError> {
    let http = state.oauth.clone();
    let limit = http.timeout();
    let callback = callback_url(state, provider);
    let token = tokio::time::timeout(limit, http.exchange_code(provider, code, &callback))
        .await
        .map_err(|_| OAuthError::Unreachable)??;
    tokio::time::timeout(limit, http.fetch_identity(provider, &token))
        .await
        .map_err(|_| OAuthError::Unreachable)?
}

/// The provider's address, cleaned, if it says the address is the person's.
fn verified_email(identity: &Identity) -> Option<String> {
    if !identity.email_verified {
        return None;
    }
    let email = clean_email(identity.email.as_deref()?).ok()?;
    (email.chars().count() <= EMAIL_MAX).then_some(email)
}

/// Inserts a one-time code (and clears expired ones); answers the code.
async fn issue_code(tx: &mut dyn Tx, mut code: LoginCode, now: f64) -> anyhow::Result<String> {
    tx.delete_expired_login_codes(now).await?;
    let value = new_token();
    code.code_hash = token_hash(&value);
    tx.insert_login_code(&code).await?;
    Ok(value)
}

/// The end of a link at the callback: a code that names the identity, for `link/finish`. Nothing
/// is attached here. `identity_in_use` when the identity signs in to another account.
async fn link_code(
    state: &AppState,
    provider: Provider,
    identity: &Identity,
    link_user_id: &str,
    challenge: &str,
) -> Result<Back, ApiError> {
    let now = state.clock.now();
    let mut tx = state.store.begin().await?;
    let owner = tx
        .oauth_identity(provider.name(), &identity.subject)
        .await?;
    if owner.is_some_and(|o| o.user_id != link_user_id) {
        return Ok(Back::error("identity_in_use"));
    }
    // (The account that started it may have gone meanwhile; a code for it could not be stored.)
    if !tx
        .user_by_id(link_user_id)
        .await?
        .is_some_and(|u| u.active())
    {
        return Ok(Back::error("account_disabled"));
    }
    let code = LoginCode {
        code_hash: String::new(),
        user_id: link_user_id.to_owned(),
        challenge: challenge.to_owned(),
        link_provider: Some(provider.name().to_owned()),
        link_subject: Some(identity.subject.clone()),
        link_email: verified_email(identity),
        expires_at: now + CODE_SECS,
    };
    let value = issue_code(&mut *tx, code, now).await?;
    commit(state, tx).await?;
    Ok(Back::Code(value))
}

/// One try of [`sign_in_code`]: `None` when it should be tried again (the organisation's members
/// changed between the seat check and the transaction).
enum Attempt {
    Done(Back),
    Again,
}

/// The end of a sign-in at the callback: finds, attaches or creates the account (see the module
/// documentation) and answers a code for `exchange`.
async fn sign_in_code(
    state: &AppState,
    provider: Provider,
    identity: &Identity,
    invite_hash: Option<&str>,
    challenge: &str,
) -> Result<Back, ApiError> {
    for attempt in 1..=SEAT_ATTEMPTS {
        match sign_in_once(state, provider, identity, invite_hash, challenge, attempt).await {
            Ok(Attempt::Done(back)) => return Ok(back),
            Ok(Attempt::Again) => {}
            // Another sign-in made the same account, or attached the same identity, at the same
            // moment (the database refused this one's row): the next try finds what the other
            // made. After the last try the app is told, as for any failure here.
            Err(ApiError::Internal(e)) if crate::store::is_unique_violation(&e) => {
                if attempt == SEAT_ATTEMPTS {
                    tracing::error!("a sign-in kept meeting a taken key: {e:#}");
                    return Ok(Back::error("server_error"));
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(Back::error("not_allowed"))
}

async fn sign_in_once(
    state: &AppState,
    provider: Provider,
    identity: &Identity,
    invite_hash: Option<&str>,
    challenge: &str,
    attempt: usize,
) -> Result<Attempt, ApiError> {
    let now = state.clock.now();
    let email = verified_email(identity);
    let done = |code: &'static str| Ok(Attempt::Done(Back::error(code)));

    // Before the transaction: the seat hook, when this sign-in would create an account with an
    // invite (never asked while the transaction holds the writer). Checked again inside.
    let approval = match &email {
        Some(email)
            if state
                .store
                .oauth_identity(provider.name(), &identity.subject)
                .await?
                .is_none()
                && state.store.user_by_email(email).await?.is_none() =>
        {
            match approve_invite_by_hash(state, invite_hash, email, None, now).await {
                Ok(approval) => approval,
                Err(ApiError::PaymentRequired(_) | ApiError::Conflict(_)) => {
                    return done("not_allowed");
                }
                Err(e) => return Err(e),
            }
        }
        _ => None,
    };

    let turn = match &approval {
        Some(approval) => Some(state.org_turn(approval.org()).await?),
        None => None,
    };
    let mut tx = state.store.begin().await?;
    let mut created: Option<(bool, Option<Accepted>)> = None;
    let mut attached = false;
    let user = match tx
        .oauth_identity(provider.name(), &identity.subject)
        .await?
    {
        Some(known) => tx.user_by_id(&known.user_id).await?,
        None => {
            let Some(email) = email else {
                return done("no_verified_email");
            };
            match tx.user_by_email(&email).await? {
                None => {
                    let new = NewAccount {
                        email,
                        invite_hash,
                        approval: approval.as_ref(),
                        attempt,
                        now,
                    };
                    match create_account(&mut *tx, state, provider, identity, new).await? {
                        Ok((user, first, accepted)) => {
                            created = Some((first, accepted));
                            Some(user)
                        }
                        // (Returning drops the transaction: nothing above is kept.)
                        Err(stop) => return Ok(stop),
                    }
                }
                // Nobody ever confirmed that whoever made this account owns the address: joining
                // it now could hand this person's sign-in to a stranger.
                Some(user) if !user.email_verified => return done("account_exists_use_password"),
                Some(user) => {
                    attach(&mut *tx, provider, identity, &user.id).await?;
                    attached = true;
                    // (As it is under the lock `attach` took: it may have been deleted since.)
                    tx.user_by_id(&user.id).await?
                }
            }
        }
    };
    // (Dropping the transaction undoes anything attached above.)
    let Some(user) = user.filter(User::active) else {
        return done("account_disabled");
    };
    let code = LoginCode {
        code_hash: String::new(),
        user_id: user.id.clone(),
        challenge: challenge.to_owned(),
        link_provider: None,
        link_subject: None,
        link_email: None,
        expires_at: now + CODE_SECS,
    };
    let value = issue_code(&mut *tx, code, now).await?;
    let (ext, user_id, name) = (
        state.ext.clone(),
        user.id.clone(),
        provider.name().to_owned(),
    );
    let first = created.as_ref().is_some_and(|(first, _)| *first);
    let made = created.is_some();
    state
        .commit_then((tx, turn), |_| async {}, async move {
            if let Some((_, accepted)) = created {
                let registered = Event::RegisteredWithProvider {
                    user: user_id.clone(),
                    provider: name.clone(),
                };
                emit(&ext, registered).await;
                if let Some(accepted) = accepted {
                    accepted.announce(ext.clone()).await;
                }
            }
            if attached {
                let linked = Event::ProviderLinked {
                    user: user_id,
                    provider: name,
                };
                emit(&ext, linked).await;
            }
        })
        .await?;
    if made {
        tracing::info!(
            user = %user.id,
            provider = provider.name(),
            server_admin = first,
            "account created through a provider"
        );
    }
    tracing::info!(user = %user.id, provider = provider.name(), "browser sign-in code issued");
    Ok(Attempt::Done(Back::Code(value)))
}

/// What [`create_account`] needs besides the identity.
struct NewAccount<'a> {
    /// The provider's verified address, cleaned; no account has it.
    email: String,
    /// The hash of the invite the sign-in carries.
    invite_hash: Option<&'a str>,
    /// What the seat hook approved before the transaction.
    approval: Option<&'a Approval>,
    /// Which try of [`sign_in_code`] this is.
    attempt: usize,
    now: f64,
}

/// Creates the account a first sign-in through a provider makes, with the identity attached and
/// the invite (if any) accepted, in `tx`; does not commit. Answers the account, whether it is the
/// server's first (its admin), and what accepting the invite did. `Err(attempt)` when the sign-in
/// ends here instead (`invite_required`, `not_allowed`) or is to be tried again (the members
/// changed since the seat check): the caller then drops `tx`, which undoes everything written in
/// it.
async fn create_account(
    tx: &mut dyn Tx,
    state: &AppState,
    provider: Provider,
    identity: &Identity,
    new: NewAccount<'_>,
) -> Result<Result<(User, bool, Option<Accepted>), Attempt>, ApiError> {
    let stop = |code: &'static str| Ok(Err(Attempt::Done(Back::error(code))));
    let NewAccount {
        email,
        invite_hash,
        approval,
        attempt,
        now,
    } = new;
    // Counted in the transaction: two first sign-ins at once cannot both be admin.
    let mode = registration_for(tx.user_count().await?, &state.settings);
    let invite = find_invite_by_hash(&mut *tx, invite_hash, &email, now).await?;
    if mode == Registration::Invite && invite.is_none() {
        return stop("invite_required");
    }
    let first = mode == Registration::FirstUser;
    let name: String = match identity.name.trim() {
        "" => email.split('@').next().unwrap_or_default(),
        name => name,
    }
    .chars()
    .filter(|c| *c != '\0')
    .take(NAME_MAX)
    .collect();
    let user = User {
        id: new_id(),
        email,
        name,
        password_hash: None,
        is_server_admin: first,
        disabled: false,
        // The provider vouches for the address.
        email_verified: true,
        created_at: now,
        deleted_at: None,
    };
    tx.insert_user(&user).await?;
    let accepted = match &invite {
        None => None,
        Some(invite) => match accept_invite(&mut *tx, invite, approval, &user.id, now).await {
            Ok(accepted) => Some(accepted),
            // The members changed since the seat check: ask again.
            Err(ApiError::Conflict(_)) if attempt < SEAT_ATTEMPTS => {
                return Ok(Err(Attempt::Again));
            }
            Err(ApiError::Conflict(_) | ApiError::NotFound(_) | ApiError::PaymentRequired(_)) => {
                return stop("not_allowed");
            }
            Err(e) => return Err(e),
        },
    };
    attach(&mut *tx, provider, identity, &user.id).await?;
    Ok(Ok((user, first, accepted)))
}

/// Attaches the identity to the account, whose row it locks first (an account gains a way in
/// under the same lock its deletion and the removal of its ways in take). Does not commit.
async fn attach(
    tx: &mut dyn Tx,
    provider: Provider,
    identity: &Identity,
    user_id: &str,
) -> anyhow::Result<()> {
    tx.lock_user(user_id).await?;
    tx.insert_oauth_identity(&OAuthIdentity {
        provider: provider.name().to_owned(),
        subject: identity.subject.clone(),
        user_id: user_id.to_owned(),
    })
    .await
}

// ------------------------------------------------------------------ the codes

#[derive(Debug, Deserialize)]
struct ExchangeIn {
    code: String,
    code_verifier: String,
    #[serde(default)]
    label: String,
}

/// The code from the browser for a session: `{token, user}`. 400 for an unknown, used or expired
/// code; 400 [`OTHER_APP`] for a verifier that does not match, which uses the code up (one try); 400
/// [`LINK_CODE`] for a code from a link (used up); 403 for a disabled account (used up).
async fn exchange(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<ExchangeIn>,
) -> Result<Json<Value>, ApiError> {
    let now = state.clock.now();
    max_len("code", &body.code, TOKEN_MAX)?;
    check_verifier(&body.code_verifier)?;
    let mut tx = state.store.begin().await?;
    // Taken in the transaction: of two exchanges of one code, one wins.
    let Some(code) = tx.take_login_code(&token_hash(&body.code)).await? else {
        return Err(ApiError::BadRequest(CODE_INVALID.into()));
    };
    if code.expires_at < now {
        return used_up(&state, tx, ApiError::BadRequest(CODE_INVALID.into())).await;
    }
    if !verifier_matches(&body.code_verifier, &code.challenge) {
        return used_up(&state, tx, ApiError::BadRequest(OTHER_APP.into())).await;
    }
    if code.link_provider.is_some() {
        // A code from adding a provider never signs anyone in.
        return used_up(&state, tx, ApiError::BadRequest(LINK_CODE.into())).await;
    }
    exchange_for_session(state, tx, code, &body.label, now).await
}

async fn exchange_for_session(
    state: AppState,
    mut tx: Box<dyn Tx>,
    code: LoginCode,
    label: &str,
    now: f64,
) -> Result<Json<Value>, ApiError> {
    let user = match tx.user_by_id(&code.user_id).await? {
        Some(user) if user.active() => user,
        _ => return used_up(&state, tx, ApiError::Forbidden(DISABLED)).await,
    };
    tx.delete_expired_user_sessions(&user.id, now).await?;
    let token = start_session(&mut *tx, &state.settings, &user.id, label, now).await?;
    let (ext, signed_in) = (
        state.ext.clone(),
        Event::SignedInWithProvider {
            user: user.id.clone(),
        },
    );
    state
        .commit_then(tx, |_| async {}, async move { emit(&ext, signed_in).await })
        .await?;
    Ok(Json(json!({"token": token, "user": me_out(&user)})))
}

#[derive(Debug, Deserialize)]
struct LinkFinishIn {
    code: String,
    code_verifier: String,
}

/// Attaches the identity a link brought back: `{linked, user}`. Needs all three: the code (from
/// the browser), the verifier (kept by the app that started it) and that account's own session.
/// 400 for an unknown, used or expired code; 400 [`NOT_THIS_LINK`] when it is not a link code, is
/// another account's or the verifier does not match (used up); 409 when the identity signs in to
/// another account (used up). The account's address counts as confirmed when the provider vouched
/// for that very address.
async fn link_finish(
    State(state): State<AppState>,
    current: CurrentUser,
    ApiJson(body): ApiJson<LinkFinishIn>,
) -> Result<Json<Value>, ApiError> {
    let now = state.clock.now();
    max_len("code", &body.code, TOKEN_MAX)?;
    check_verifier(&body.code_verifier)?;
    let code_hash = token_hash(&body.code);
    let mut tx = state.store.begin().await?;
    // One at a time per account: a second link of the same identity finds the first.
    tx.lock_user(&current.id).await?;
    let Some(code) = tx.take_login_code(&code_hash).await? else {
        return Err(ApiError::BadRequest(LINK_CODE_INVALID.into()));
    };
    if code.expires_at < now {
        return used_up(&state, tx, ApiError::BadRequest(LINK_CODE_INVALID.into())).await;
    }
    let ours = code.user_id == current.id && verifier_matches(&body.code_verifier, &code.challenge);
    let (Some(provider), Some(subject), true) = (code.link_provider, code.link_subject, ours)
    else {
        return used_up(&state, tx, ApiError::BadRequest(NOT_THIS_LINK.into())).await;
    };
    let owner = tx.oauth_identity(&provider, &subject).await?;
    if owner.as_ref().is_some_and(|o| o.user_id != current.id) {
        return used_up(&state, tx, ApiError::Conflict(IDENTITY_TAKEN.into())).await;
    }
    let Some(mut account) = tx.user_by_id(&current.id).await? else {
        return Err(ApiError::Unauthorized);
    };
    let new = owner.is_none();
    if new {
        let identity = OAuthIdentity {
            provider: provider.clone(),
            subject,
            user_id: current.id.clone(),
        };
        match tx.insert_oauth_identity(&identity).await {
            Ok(()) => {}
            // Another account attached the identity at the same moment: answered as if it had been
            // found attached, with the code used up (in a transaction of its own; this one is
            // undone).
            Err(e) if crate::store::is_unique_violation(&e) => {
                drop(tx);
                let mut tx = state.store.begin().await?;
                tx.take_login_code(&code_hash).await?;
                return used_up(&state, tx, ApiError::Conflict(IDENTITY_TAKEN.into())).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
    if code.link_email.as_deref() == Some(account.email.as_str()) && !account.email_verified {
        // The provider vouches for the very address the account has.
        tx.set_user_email_verified(&account.id, true).await?;
        account.email_verified = true;
    }
    let (ext, linked) = (
        state.ext.clone(),
        Event::ProviderLinked {
            user: current.id.clone(),
            provider: provider.clone(),
        },
    );
    state
        .commit_then(tx, |_| async {}, async move {
            if new {
                emit(&ext, linked).await;
            }
        })
        .await?;
    Ok(Json(json!({"linked": provider, "user": me_out(&account)})))
}

// ------------------------------------------------------------------ an account's providers

/// `{providers, has_password}`: the providers attached to the signed-in account, sorted.
async fn providers(
    State(state): State<AppState>,
    current: CurrentUser,
) -> Result<Json<Value>, ApiError> {
    let user = load(&state, &current).await?;
    let mut names: Vec<String> = state
        .store
        .user_identities(&user.id)
        .await?
        .into_iter()
        .map(|i| i.provider)
        .collect();
    names.sort();
    Ok(Json(
        json!({"providers": names, "has_password": user.password_hash.is_some()}),
    ))
}

#[derive(Debug, Deserialize)]
struct UnlinkIn {
    #[serde(default)]
    current_password: Option<String>,
}

/// Removes a provider from the signed-in account: 204. Needs the current password, or for an
/// account without one a sign-in from the last ten minutes (403). 404 `not linked`; 409 when it is
/// the only way into the account (no password and no other provider).
async fn unlink(
    State(state): State<AppState>,
    current: CurrentUser,
    ApiPath(name): ApiPath<String>,
    ApiJson(body): ApiJson<UnlinkIn>,
) -> Result<StatusCode, ApiError> {
    if let Some(password) = &body.current_password {
        max_len("current_password", password, CHECKED_PASSWORD_MAX)?;
    }
    let user = load(&state, &current).await?;
    confirm(
        &state,
        &user,
        &current.token_hash,
        body.current_password.as_deref(),
    )
    .await?;
    let mut tx = state.store.begin().await?;
    // Read in the transaction, with the account's row locked: two removals at once cannot take
    // away the last way in.
    tx.lock_user(&current.id).await?;
    let Some(user) = tx.user_by_id(&current.id).await? else {
        return Err(ApiError::Unauthorized);
    };
    let identities = tx.user_identities(&user.id).await?;
    let mine = identities.iter().filter(|i| i.provider == name).count();
    if mine == 0 {
        return Err(ApiError::NotFound("not linked"));
    }
    if user.password_hash.is_none() && mine == identities.len() {
        return Err(ApiError::Conflict(ONLY_WAY_IN.into()));
    }
    tx.delete_user_identities(&user.id, &name).await?;
    let (ext, unlinked) = (
        state.ext.clone(),
        Event::ProviderUnlinked {
            user: user.id.clone(),
            provider: name,
        },
    );
    state
        .commit_then(tx, |_| async {}, async move { emit(&ext, unlinked).await })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_addresses_on_this_machine_are_redirect_uris() {
        for ok in [
            "http://127.0.0.1:5000/",
            "http://127.0.0.1:53111/done",
            "http://localhost:8080/callback?x=1",
            "http://LOCALHOST:8080/cb",
            "http://[::1]:5000/cb",
            "http://127.0.0.1:80/cb",
            "http://127.0.0.1:65535",
        ] {
            assert!(loopback(ok).is_some(), "{ok}");
        }
        for bad in [
            "https://evil.example/cb",
            "http://evil.example:80/cb",
            "http://127.0.0.1/cb",
            "http://user@127.0.0.1:5000/cb",
            "http://user:pw@127.0.0.1:5000/cb",
            "http://:pw@127.0.0.1:5000/cb",
            "myapp://cb",
            "http://127.0.0.1.evil.com:5000/cb",
            "http://localhost@evil.com:5000/cb",
            "http://evil.com@localhost:5000/cb",
            "http://127.0.0.1:5000@evil.com/cb",
            "http://127.0.0.1:abc/cb",
            "http://127.0.0.1:/cb",
            "http://127.0.0.1:0/cb",
            "http://127.0.0.1:65536/cb",
            "http://127.0.0.1:005000/cb",
            "http://127.0.0.1:5000/cb#frag",
            "http://127.0.0.1:5000/cb#",
            "http://127.1:5000/cb",
            "http://0x7f000001:5000/cb",
            "http://2130706433:5000/cb",
            "http://[0:0::1]:5000/cb",
            "http://localhost.:5000/cb",
            "http://127.0.0.1:5000\\@evil.com/cb",
            "http://127.0.0.1:5000/c b",
            "http://127.0.0.1:5000/cb\n",
            " http://127.0.0.1:5000/cb",
            "http:/127.0.0.1:5000/cb",
            "http:127.0.0.1:5000/cb",
            "ftp://127.0.0.1:5000/cb",
            "//127.0.0.1:5000/cb",
            "",
        ] {
            assert!(loopback(bad).is_none(), "{bad}");
        }
        let long = format!("http://127.0.0.1:5000/{}", "a".repeat(REDIRECT_MAX));
        assert!(loopback(&long).is_none());
    }

    #[test]
    fn a_challenge_is_lower_case_hex_sha256() {
        let challenge = hex::encode(Sha256::digest(b"v"));
        assert!(is_challenge(&challenge));
        assert!(!is_challenge(&challenge.to_uppercase()));
        assert!(!is_challenge("short"));
        assert!(!is_challenge(&format!("{challenge}0")));
        assert!(verifier_matches("v", &challenge));
        assert!(!verifier_matches("w", &challenge));
        assert!(!verifier_matches("v", ""));
    }

    #[test]
    fn provider_errors_are_passed_through_only_when_they_are_rfc_6749_codes() {
        assert_eq!(provider_error("access_denied"), "cancelled");
        for code in PROVIDER_ERRORS {
            assert_eq!(provider_error(code), code);
            assert!(LOOPBACK_ERRORS.contains(&code));
        }
        assert_eq!(provider_error("some_other_code"), "provider_refused");
        assert_eq!(
            provider_error("temporarily_unavailable"),
            "temporarily_unavailable"
        );
        assert_eq!(provider_error("x&code=forged"), "provider_refused");
        assert_eq!(provider_error(&"a".repeat(65)), "provider_refused");
        assert_eq!(provider_error("Upper"), "provider_refused");
    }
}
