//! What every handler can reach.

use std::sync::Arc;

use anyhow::bail;

use crate::blobs::{Blobs, LocalBlobs};
use crate::clock::Clock;
use crate::config::{Database, Settings, Storage};
use crate::ext::{Extensions, NoExtensions};
use crate::hub::Hub;
use crate::mail::{Mailer, MemoryMailer, SmtpMailer};
use crate::sessions::Sessions;
use crate::store::{SqliteStore, Store};

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
    /// State over a store and blob storage, with no extensions, the system clock and a
    /// [`MemoryMailer`] (which keeps messages rather than sending them; it reports itself as
    /// configured exactly when the settings name a mail server and a sender). Replace any public
    /// field afterwards to change it. A running server is built with [`AppState::from_settings`].
    pub fn new(settings: Settings, store: Arc<dyn Store>, blobs: Arc<dyn Blobs>) -> Self {
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

    /// The state of a running server: the database `settings.database_url` names (opened, and its
    /// schema brought up to date as [`SqliteStore::open`] does), the blob storage
    /// `settings.storage_url` names (a folder, [`LocalBlobs`]; S3 is refused until Task 17), and mail
    /// through the operator's SMTP server ([`SmtpMailer`]).
    pub async fn from_settings(settings: Settings) -> anyhow::Result<Self> {
        let store: Arc<dyn Store> = match settings.database()? {
            Database::Sqlite(_) => {
                Arc::new(SqliteStore::open(&settings.database_url, settings.auto_migrate).await?)
            }
            // TASK 16: open the PostgreSQL store here.
            Database::Postgres(_) => bail!("this build cannot open PostgreSQL databases yet"),
        };
        let blobs: Arc<dyn Blobs> = match settings.storage()? {
            Storage::Local(folder) => Arc::new(LocalBlobs::new(folder)?),
            // TASK 17: open the S3 storage here.
            Storage::S3 { .. } => {
                bail!("this build cannot use S3 storage yet (S3 storage arrives in Task 17)")
            }
        };
        let mailer = Arc::new(SmtpMailer::new(&settings)?);
        let mut state = Self::new(settings, store, blobs);
        state.mailer = mailer;
        Ok(state)
    }
}

/// The whole HTTP application for this state.
///
/// Serve it with connection info (`into_make_service_with_connect_info::<SocketAddr>()`): sign-in
/// and password reset limits are kept per client address, and without it every client shares one.
pub fn app(state: AppState) -> axum::Router {
    crate::routes::router(state)
}
