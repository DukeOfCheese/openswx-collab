//! Routes about the server itself: `/api/health`, `/api/ready`, `/api/server`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::Serialize;
use serde_json::{Value, json};

use crate::config::Settings;
use crate::error::ApiError;
use crate::state::AppState;
use crate::store::Store;

/// The wire protocol this server speaks on the document WebSocket.
pub const PROTOCOL: u32 = 2;

/// How long `/api/ready` waits for the database or the storage to answer.
const READY_TIMEOUT: Duration = Duration::from_secs(5);
/// How long an answer of `/api/ready` is given again to those who ask, rather than asking the
/// database and the storage again (on S3 each check is a request).
pub const READY_CACHE: Duration = Duration::from_secs(1);
/// How often at most `/api/ready` logs why the server is not ready, while it stays so.
const READY_WARN_EVERY: Duration = Duration::from_secs(60);

/// The last answer of `/api/ready` and when the log last said why it was not ready. Behind an
/// async lock held through a check: one check at a time, and those who asked meanwhile get its
/// answer.
#[derive(Debug, Default)]
struct Readiness {
    last: Option<(Instant, bool)>,
    warned_at: Option<Instant>,
}

pub(crate) fn router() -> Router<AppState> {
    let readiness = Arc::new(tokio::sync::Mutex::new(Readiness::default()));
    Router::new()
        .route("/api/health", get(health))
        .route("/api/ready", get(ready))
        .route("/api/server", get(server))
        .layer(Extension(readiness))
}

/// Who may create an account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Registration {
    /// There are no accounts yet: the first one becomes the server admin.
    FirstUser,
    /// Only people with an invite.
    Invite,
    /// Anyone.
    Open,
}

/// The registration mode: `first_user` while there are no accounts (deleted and disabled ones count),
/// then `open` or `invite` by `open_registration`.
pub async fn registration_mode(
    store: &dyn Store,
    settings: &Settings,
) -> anyhow::Result<Registration> {
    Ok(registration_for(store.user_count().await?, settings))
}

/// The registration mode on a server with `users` accounts (see [`registration_mode`]).
pub fn registration_for(users: u64, settings: &Settings) -> Registration {
    if users == 0 {
        Registration::FirstUser
    } else if settings.open_registration {
        Registration::Open
    } else {
        Registration::Invite
    }
}

async fn health() -> Json<Value> {
    Json(json!({"ok": true}))
}

/// 200 when the database and the storage answer and the journal writer runs, else 503. Why is
/// logged, not told: when the server stops being ready, then at most every
/// [`READY_WARN_EVERY`] while it stays so. An answer is given again for
/// [`AppState::ready_cache`] ([`READY_CACHE`]), and one check runs at a time.
async fn ready(
    State(state): State<AppState>,
    Extension(readiness): Extension<Arc<tokio::sync::Mutex<Readiness>>>,
) -> (StatusCode, Json<Value>) {
    let mut readiness = readiness.lock().await;
    let ok = match readiness.last {
        Some((at, ok)) if at.elapsed() < state.ready_cache => ok,
        last => {
            let (ok, problems) = check_ready(&state).await;
            let now = Instant::now();
            let was = last.map(|(_, ok)| ok);
            let quiet = readiness
                .warned_at
                .is_some_and(|at| now.duration_since(at) < READY_WARN_EVERY);
            if !ok && (was != Some(false) || !quiet) {
                for problem in &problems {
                    tracing::warn!("not ready: {problem}");
                }
                readiness.warned_at = Some(now);
            }
            if ok && was == Some(false) {
                tracing::info!("ready again");
                readiness.warned_at = None;
            }
            readiness.last = Some((now, ok));
            ok
        }
    };
    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(json!({"ok": ok})))
}

/// Asks the database, the storage and the journal writer: whether all is well, and what is not.
async fn check_ready(state: &AppState) -> (bool, Vec<String>) {
    let store = tokio::time::timeout(READY_TIMEOUT, state.store.health());
    let blobs = tokio::time::timeout(READY_TIMEOUT, state.blobs.health());
    let (store, blobs) = tokio::join!(store, blobs);
    let mut problems = Vec::new();
    if !state.journal.healthy() {
        problems.push("the journal writer is not running".to_owned());
    }
    for (what, outcome) in [("database", store), ("storage", blobs)] {
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(err)) => problems.push(format!("the {what} failed: {err:#}")),
            Err(_) => problems.push(format!("the {what} did not answer in {READY_TIMEOUT:?}")),
        }
    }
    (problems.is_empty(), problems)
}

async fn server(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let registration = registration_mode(state.store.as_ref(), &state.settings).await?;
    let mut info = json!({
        "name": "openswx-collab",
        "version": crate::VERSION,
        "protocol": PROTOCOL,
        "registration": registration,
        "oauth": state.settings.oauth_providers(),
        "email": state.mailer.configured(),
        "edition": "core",
    });
    if let (Some(info), Value::Object(extra)) = (info.as_object_mut(), state.ext.server_info()) {
        info.extend(extra);
    }
    Ok(Json(info))
}
