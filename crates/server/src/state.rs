//! What every handler can reach.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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
    /// Knobs of the rooms that tests turn (defaults otherwise).
    #[doc(hidden)]
    pub room_hooks: Arc<crate::room::Hooks>,
    /// The steps after REST commits still running ([`AppState::after_commit`]).
    pub after_commits: Arc<InFlight>,
}

/// How long shutdown waits for the steps after REST commits that are still running.
const AFTER_COMMIT_WAIT: Duration = Duration::from_secs(10);

/// Counts work that runs detached from whoever started it, so that shutdown can wait for it.
#[derive(Debug, Default)]
pub struct InFlight {
    running: AtomicUsize,
    idle: tokio::sync::Notify,
}

impl InFlight {
    /// How many are running.
    pub fn running(&self) -> usize {
        self.running.load(Ordering::SeqCst)
    }

    fn start(self: &Arc<Self>) -> Running {
        self.running.fetch_add(1, Ordering::SeqCst);
        Running(self.clone())
    }

    /// Waits until none is running.
    pub async fn idle(&self) {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.running() == 0 {
                return;
            }
            idle.await;
        }
    }
}

/// One running piece of [`InFlight`] work; it ends when this is dropped.
struct Running(Arc<InFlight>);

impl Drop for Running {
    fn drop(&mut self) {
        if self.0.running.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
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
            room_hooks: Arc::default(),
            after_commits: Arc::default(),
        }
    }

    /// Runs what a request does after its database commit (forgetting sessions, closing document
    /// connections, mail, extension notifications and events, in that order) to completion in a
    /// task of its own, and answers what it answers. The request waits for it, but if the request
    /// is dropped meanwhile (its client went away) the steps still all run: a commit that took
    /// away someone's access is always followed by the hub closing their connections. Shutdown
    /// waits (a while) for steps still running.
    ///
    /// # Panics
    ///
    /// If the steps panic (the panic is passed on to the request).
    pub async fn after_commit<T, F>(&self, steps: F) -> T
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let running = self.after_commits.start();
        let gate = self.room_hooks.after_commit.clone();
        let task = tokio::spawn(async move {
            let _running = running;
            gate.pass().await;
            steps.await
        });
        match task.await {
            Ok(answer) => answer,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(_) => panic!("the steps after a commit were cancelled"),
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
    /// connections (each room waits briefly for the journal's answers to its changes in flight,
    /// closes its sockets with 1001 and writes a final snapshot of its durable head), waits (a
    /// while) for the steps after REST commits still running, then writes what the journal still
    /// has queued and stops it. Calling it again does no harm.
    pub async fn shutdown(&self) {
        self.hub.shutdown().await;
        if tokio::time::timeout(AFTER_COMMIT_WAIT, self.after_commits.idle())
            .await
            .is_err()
        {
            tracing::warn!(
                "{} requests were still finishing after their commits at shutdown",
                self.after_commits.running()
            );
        }
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
