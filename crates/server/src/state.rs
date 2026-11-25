//! What every handler can reach.

use std::sync::Arc;

use anyhow::bail;

use crate::blobs::{Blobs, LocalBlobs};
use crate::clock::Clock;
use crate::config::{Database, Settings, Storage};
use crate::ext::{Extensions, NoExtensions};
use crate::hub::Hub;
use crate::journal::{self, JournalWriter};
use crate::mail::{Mailer, MemoryMailer, SmtpMailer};
use crate::sessions::Sessions;
use crate::store::{SqliteStore, Store, new_id};

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
    /// Makes accepted changes durable; started on `store` by [`AppState::new`].
    pub journal: JournalWriter,
    /// This process, as room leases name it: random per process.
    pub node_id: String,
}

impl AppState {
    /// State over a store and blob storage, with no extensions, the system clock and a
    /// [`MemoryMailer`] (which keeps messages rather than sending them; it reports itself as
    /// configured exactly when the settings name a mail server and a sender). Replace any public
    /// field afterwards to change it (after replacing `store`, start a new `journal` on it). A
    /// running server is built with [`AppState::from_settings`].
    ///
    /// Starts the journal writer, so it must be called inside a tokio runtime.
    pub fn new(settings: Settings, store: Arc<dyn Store>, blobs: Arc<dyn Blobs>) -> Self {
        let mailer = Arc::new(MemoryMailer::new(settings.email_configured()));
        let journal = JournalWriter::start(store.clone(), journal::MAX_BATCH, journal::MAX_WAIT);
        Self {
            journal,
            node_id: new_id(),
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

    /// The orderly end of a server, once it has stopped accepting requests: closes the rooms and
    /// connections, then writes what the journal still has queued and stops it.
    pub async fn shutdown(&self) {
        self.hub.shutdown().await;
        self.journal.close().await;
    }
}

/// The whole HTTP application for this state.
///
/// Serve it with connection info (`into_make_service_with_connect_info::<SocketAddr>()`): sign-in
/// and password reset limits are kept per client address, and without it every client shares one.
pub fn app(state: AppState) -> axum::Router {
    crate::routes::router(state)
}
