//! What every handler can reach.

use std::sync::Arc;

use crate::blobs::Blobs;
use crate::clock::Clock;
use crate::config::Settings;
use crate::ext::{Extensions, NoExtensions};
use crate::hub::Hub;
use crate::mail::{Mailer, MemoryMailer};
use crate::sessions::Sessions;
use crate::store::Store;

/// The server's shared state. Cloning is cheap: every field is shared.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn Store>,
    pub blobs: Arc<dyn Blobs>,
    pub hub: Hub,
    pub mailer: Arc<dyn Mailer>,
    pub ext: Arc<dyn Extensions>,
    pub settings: Arc<Settings>,
    pub sessions: Sessions,
    pub clock: Clock,
}

impl AppState {
    /// State over a store and blob storage, with no extensions and the system clock. Replace any
    /// public field afterwards to change it.
    pub fn new(settings: Settings, store: Arc<dyn Store>, blobs: Arc<dyn Blobs>) -> Self {
        // Until Task 4 brings SmtpMailer, mail is kept in memory; it reports itself as configured
        // exactly when the settings name a mail server and a sender.
        let mailer = Arc::new(MemoryMailer::new(settings.email_configured()));
        Self {
            store,
            blobs,
            hub: Hub::default(),
            mailer,
            ext: Arc::new(NoExtensions),
            settings: Arc::new(settings),
            sessions: Sessions::default(),
            clock: Clock::system(),
        }
    }
}

/// The whole HTTP application for this state.
pub fn app(state: AppState) -> axum::Router {
    crate::routes::router(state)
}
