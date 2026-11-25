//! Routes about the server itself: `/api/health`, `/api/ready`, `/api/server`.

use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
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

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/ready", get(ready))
        .route("/api/server", get(server))
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
    Ok(if store.user_count().await? == 0 {
        Registration::FirstUser
    } else if settings.open_registration {
        Registration::Open
    } else {
        Registration::Invite
    })
}

async fn health() -> Json<Value> {
    Json(json!({"ok": true}))
}

/// 200 when the database and the storage answer, else 503. Why is logged, not told.
async fn ready(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let store = tokio::time::timeout(READY_TIMEOUT, state.store.health());
    let blobs = tokio::time::timeout(READY_TIMEOUT, state.blobs.health());
    let (store, blobs) = tokio::join!(store, blobs);
    let mut ok = true;
    for (what, outcome) in [("database", store), ("storage", blobs)] {
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::warn!("not ready: the {what} failed: {err:#}");
                ok = false;
            }
            Err(_) => {
                tracing::warn!("not ready: the {what} did not answer in {READY_TIMEOUT:?}");
                ok = false;
            }
        }
    }
    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(json!({"ok": ok})))
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
