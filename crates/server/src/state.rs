//! What every handler can reach.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::bail;

use crate::blobs::{Blobs, LocalBlobs};
use crate::clock::Clock;
use crate::config::{Database, Settings, Storage};
use crate::error::ApiError;
use crate::export::Exports;
use crate::ext::{Extensions, NoExtensions};
use crate::hub::Hub;
use crate::journal::{self, JournalWriter};
use crate::mail::{Mailer, MemoryMailer, SmtpMailer};
use crate::oauth::{HttpProviders, NoProviders, OAuthHttp};
use crate::sessions::Sessions;
use crate::store::postgres::PgConfig;
use crate::store::{PgStore, SqliteStore, Store, Tx, new_id};

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
    /// Google and GitHub, for browser sign-in ([`HttpProviders`] in a running server).
    pub oauth: Arc<dyn OAuthHttp>,
    /// Makes accepted changes durable; started on `store` by [`AppState::new`].
    pub journal: JournalWriter,
    /// This process, as room leases name it: random per process.
    pub node_id: String,
    /// Knobs of the rooms that tests turn (defaults otherwise).
    #[doc(hidden)]
    pub room_hooks: Arc<crate::room::Hooks>,
    /// The steps after REST commits still running ([`AppState::after_commit`]).
    pub after_commits: Arc<InFlight>,
    /// How many exports run at once, and how patient they are ([`Exports`]).
    pub exports: Arc<Exports>,
}

/// What the revoking steps of [`AppState::commit_then`] know of the commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Committed {
    /// It took effect.
    Yes,
    /// It reported an error: it may or may not have taken effect. A step that closes connections
    /// then says "reconnect" (4410) rather than "sign in again": the sessions may still be valid.
    Uncertain,
}

impl Committed {
    /// The close code and reason for connections whose sessions the commit ended: 4401 `sign in
    /// again` when it took effect, 4410 `reconnect` when that is uncertain (a reconnect finds out).
    pub fn session_close(self) -> (u16, &'static str) {
        match self {
            Self::Yes => (crate::room::close::SIGN_IN, "sign in again"),
            Self::Uncertain => (crate::room::close::MOVED, "reconnect"),
        }
    }
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
    /// State over a store and blob storage, with no extensions, the system clock, no sign-in
    /// providers ([`NoProviders`]: every call to one fails) and a
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
            oauth: Arc::new(NoProviders),
            room_hooks: Arc::default(),
            after_commits: Arc::default(),
            exports: Arc::default(),
        }
    }

    /// Commits a request's transaction and runs what follows it, all in a task of its own that
    /// the request awaits but does not own: if the request is dropped (its client went away)
    /// once this is called, the commit and every step after it still run. Shutdown waits (a
    /// while) for such tasks.
    ///
    /// The steps come in two kinds, in this order:
    ///
    /// - `revoking`: forgetting sessions and the hub's drops and closes, made from what the commit
    ///   was ([`Committed`]). They run after the commit **whether it succeeded or reported an
    ///   error** (an error can come after the commit
    ///   took effect: a connection lost during `COMMIT`). That is safe: [`Hub::drop_user`] reads
    ///   the roles again and drops nobody whose role did not change, and the other drops cost at
    ///   worst a reconnect.
    /// - `announcing`: mail, extension notifications and events. They run only after a commit
    ///   that succeeded; their answer is the request's.
    ///
    /// A commit error is answered as the error (500) after the revoking steps.
    ///
    /// # Which hub call a route makes
    ///
    /// Every route whose commit takes away access calls one of these among its revoking steps
    /// (after any `sessions.forget_*`):
    ///
    /// | the commit | revoking step |
    /// |---|---|
    /// | ends every session of an account (password changed or reset, account disabled or deleted, signed out everywhere) | `sessions.forget_user`, then [`Hub::drop_account`] |
    /// | ends one session (sign-out) | `sessions.forget_token`, then [`Hub::drop_session`] |
    /// | changes or removes a role in an organisation or one of its projects (member role, member removal, project member set or removed) | [`Hub::drop_user`] for that user and organisation |
    /// | deletes a document, or a project or organisation with its documents | [`Hub::close_document`] for each document |
    ///
    /// Files in storage that a commit orphans (an organisation's blobs and snapshots) are removed
    /// among the `announcing` steps, never the `revoking` ones: only a commit known to have taken
    /// effect may remove them.
    ///
    /// # Panics
    ///
    /// If the steps panic (the panic is passed on to the request).
    pub async fn commit_then<T, F, R, A>(
        &self,
        tx: Box<dyn Tx>,
        revoking: F,
        announcing: A,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(Committed) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
        A: Future<Output = T> + Send + 'static,
    {
        let gate = self.room_hooks.after_commit.clone();
        self.detached(async move {
            let committed = tx.commit().await;
            gate.pass().await;
            let outcome = if committed.is_ok() {
                Committed::Yes
            } else {
                Committed::Uncertain
            };
            revoking(outcome).await;
            match committed {
                Ok(()) => Ok(announcing.await),
                Err(e) => Err(ApiError::Internal(e)),
            }
        })
        .await
    }

    /// Runs what follows a write that has no transaction for the request to commit (a restore,
    /// written by the room's journal; a document creation, whose commit has its own handling of an
    /// uncertain outcome) to completion in a task of its own, as [`AppState::commit_then`] runs
    /// its steps, and answers what it answers.
    ///
    /// # Panics
    ///
    /// If the steps panic (the panic is passed on to the request).
    pub async fn after_commit<T, F>(&self, steps: F) -> T
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let gate = self.room_hooks.after_commit.clone();
        self.detached(async move {
            gate.pass().await;
            steps.await
        })
        .await
    }

    /// Runs `work` in a tracked task of its own, which a dropped caller does not cancel.
    async fn detached<T, F>(&self, work: F) -> T
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let running = self.after_commits.start();
        let task = tokio::spawn(async move {
            let _running = running;
            work.await
        });
        match task.await {
            Ok(answer) => answer,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(_) => panic!("the steps after a commit were cancelled"),
        }
    }

    /// The state of a running server: the database `settings.database_url` names (opened, and its
    /// schema brought up to date as [`SqliteStore::open`] and [`PgStore::open`] do), the blob storage
    /// `settings.storage_url` names (a folder, [`LocalBlobs`]; S3 is refused until Task 17), mail
    /// through the operator's SMTP server ([`SmtpMailer`]) and the real Google and GitHub
    /// ([`HttpProviders`]).
    pub async fn from_settings(settings: Settings) -> anyhow::Result<Self> {
        let store: Arc<dyn Store> = match settings.database()? {
            Database::Sqlite(_) => {
                Arc::new(SqliteStore::open(&settings.database_url, settings.auto_migrate).await?)
            }
            Database::Postgres(_) => Arc::new(
                PgStore::open(
                    &settings.database_url,
                    settings.auto_migrate,
                    PgConfig::from_settings(&settings),
                )
                .await?,
            ),
        };
        let blobs: Arc<dyn Blobs> = match settings.storage()? {
            Storage::Local(folder) => Arc::new(LocalBlobs::new(folder)?),
            // TASK 17: open the S3 storage here.
            Storage::S3 { .. } => {
                bail!("this build cannot use S3 storage yet (S3 storage arrives in Task 17)")
            }
        };
        let mailer = Arc::new(SmtpMailer::new(&settings)?);
        let oauth = Arc::new(HttpProviders::new(&settings)?);
        let mut state = Self::new(settings, store, blobs);
        state.mailer = mailer;
        state.oauth = oauth;
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
