//! Extension points: paid features (seat limits, audit, extra routes) attach here from outside.
//!
//! Placeholder: Task 5 adds the member, deletion, event and quota hooks.

use async_trait::async_trait;

use crate::state::AppState;

#[async_trait]
pub trait Extensions: Send + Sync + 'static {
    /// Merged into `GET /api/server`; its keys win over the server's own (e.g. `edition`).
    fn server_info(&self) -> serde_json::Value {
        serde_json::json!({})
    }

    /// Extra routes, merged into the app with the default 1 MiB body limit. They must not set a
    /// fallback, and must not repeat a route the server has.
    fn routes(&self, _state: AppState) -> axum::Router {
        axum::Router::new()
    }
}

/// No extensions: the open-source server.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoExtensions;

impl Extensions for NoExtensions {}
