//! The openswx collaboration server: REST routes and (from later tasks) the live document WebSocket.
//!
//! Everything a handler needs is in [`AppState`]; [`app`] builds the router from it. Handlers return
//! `Result<_, ApiError>` and take JSON bodies through [`ApiJson`], so every error a client sees has the
//! shape `{"detail": "..."}`.

pub mod blobs;
pub mod clock;
pub mod config;
pub mod error;
pub mod ext;
pub mod hub;
pub mod mail;
pub mod routes;
pub mod sessions;
pub mod state;
pub mod store;

pub use clock::Clock;
pub use config::Settings;
pub use error::{ApiError, ApiJson};
pub use state::{AppState, app};

/// The server's version, reported by `GET /api/server`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
