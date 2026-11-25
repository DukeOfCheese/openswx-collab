//! The test harness: a real server per test on a free port, mirroring the Python `conftest.py`.
#![allow(dead_code)] // each test file uses part of the harness

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use oswx_server::blobs::{Blobs, NullBlobs};
use oswx_server::hub::Hub;
use oswx_server::sessions::Sessions;
use oswx_server::store::{Id, PasswordReset, SchemaStatus, Session, SqliteStore, Store, Tx, User};
use oswx_server::{AppState, Settings};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// A session token as the server hands it out.
pub type Token = String;

/// A running server. Dropping it stops it and closes its database.
pub struct TestServer {
    /// `http://127.0.0.1:<port>`, no trailing slash.
    pub base: String,
    pub state: AppState,
    /// The test database, for statements of the test's own ([`TestServer::sql`]).
    db: SqliteStore,
    /// What `state.store` is, so that `restart` can put the reopened database in its place.
    store_kind: StoreKind,
    /// The database and storage live here; kept across `restart`.
    dir: Arc<TempDir>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
enum StoreKind {
    /// `state.store` is `db` itself.
    Plain,
    /// `state.store` is this `FlakyStore` around `db`.
    Flaky(Arc<FlakyStore>),
    /// A test put in a store of its own; `restart` cannot reopen it.
    Custom,
}

/// The settings every test starts from: everything inside the test's own temporary directory.
fn test_settings(dir: &Path) -> Settings {
    let dir = dir.to_string_lossy().replace('\\', "/");
    Settings {
        database_url: format!("sqlite://{dir}/collab.db"),
        storage_url: format!("{dir}/storage"),
        public_url: "http://testserver".into(),
        ..Settings::default()
    }
}

/// Opens the database the settings name. This is the one place that decides what a test runs
/// against: SQLite at `settings.database_url`, or PostgreSQL with an emptied schema when
/// `OSWX_TEST_DATABASE_URL` is set.
async fn open_store(settings: &Settings) -> SqliteStore {
    if std::env::var_os("OSWX_TEST_DATABASE_URL").is_some() {
        // TASK 16: open PgStore on an emptied schema here.
        panic!("OSWX_TEST_DATABASE_URL is set, but the PostgreSQL store arrives in Task 16");
    }
    SqliteStore::open(&settings.database_url, settings.auto_migrate)
        .await
        .expect("the test database opens")
}

/// Opens the blob storage the settings name.
fn open_blobs(_settings: &Settings) -> Arc<dyn Blobs> {
    Arc::new(NullBlobs)
}

impl TestServer {
    pub async fn start() -> Self {
        Self::start_with(|_| {}).await
    }

    /// Starts with the test settings changed by `f`.
    pub async fn start_with(f: impl FnOnce(&mut Settings)) -> Self {
        Self::start_custom(f, |_| {}).await
    }

    /// Starts with the settings changed by `settings`, then the state changed by `state` (to put in
    /// a test double, an extension or a manual clock).
    pub async fn start_custom(
        settings: impl FnOnce(&mut Settings),
        state: impl FnOnce(&mut AppState),
    ) -> Self {
        let dir = Arc::new(TempDir::new().expect("a temporary directory"));
        let mut s = test_settings(dir.path());
        settings(&mut s);
        let db = open_store(&s).await;
        let blobs = open_blobs(&s);
        let plain: Arc<dyn Store> = Arc::new(db.clone());
        let mut app_state = AppState::new(s, plain.clone(), blobs);
        state(&mut app_state);
        let store_kind = if std::ptr::addr_eq(Arc::as_ptr(&app_state.store), Arc::as_ptr(&plain)) {
            StoreKind::Plain
        } else {
            StoreKind::Custom
        };
        Self::serve(app_state, db, store_kind, dir).await
    }

    /// Starts with the test settings changed by `settings` and the store wrapped in a [`FlakyStore`],
    /// which is returned so the test can make it fail. It stays in place across `restart`.
    pub async fn start_flaky(settings: impl FnOnce(&mut Settings)) -> (Self, Arc<FlakyStore>) {
        let mut handle = None;
        let mut server = Self::start_custom(settings, |s| {
            let flaky = Arc::new(FlakyStore::new(s.store.clone()));
            handle = Some(flaky.clone());
            s.store = flaky;
        })
        .await;
        let flaky = handle.expect("the state function ran");
        server.store_kind = StoreKind::Flaky(flaky.clone());
        (server, flaky)
    }

    /// Runs one statement of the test's own against the test database, in its own transaction
    /// (to set up rows no route can make yet).
    pub async fn sql(&self, statement: &str) {
        let mut tx = self.db.begin_tx().await.expect("a transaction");
        sqlx::query(statement)
            .execute(tx.conn())
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
        Box::new(tx).commit().await.expect("the statement commits");
    }

    /// Stops the server and starts it again on the same database file and storage, as a new process
    /// would: the database is closed and opened again; in-memory state (rooms, the session cache)
    /// starts empty; settings, mailer, extensions and clock are kept, and so is a [`FlakyStore`]
    /// (around the reopened database). The port changes; use the new `base`.
    pub async fn restart(mut self) -> Self {
        self.shutdown().await;
        self.db.close().await;
        let mut state = self.state.clone();
        state.hub = Hub::default();
        state.sessions = Sessions::default();
        let db = open_store(&state.settings).await;
        let reopened: Arc<dyn Store> = Arc::new(db.clone());
        match &self.store_kind {
            StoreKind::Plain => state.store = reopened,
            StoreKind::Flaky(flaky) => flaky.set_inner(reopened),
            StoreKind::Custom => panic!("restart() cannot reopen a store the test put in itself"),
        }
        Self::serve(state, db, self.store_kind.clone(), self.dir.clone()).await
    }

    async fn serve(
        state: AppState,
        db: SqliteStore,
        store_kind: StoreKind,
        dir: Arc<TempDir>,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a free port");
        let base = format!("http://{}", listener.local_addr().expect("an address"));
        let (stop, stopped) = oneshot::channel::<()>();
        let app = oswx_server::app(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .expect("the server runs");
        });
        Self {
            base,
            state,
            db,
            store_kind,
            dir,
            stop: Some(stop),
            task: Some(task),
        }
    }

    /// Stops accepting, gives open requests a moment to finish, then closes the hub.
    async fn shutdown(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let abort = task.abort_handle();
            if tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .is_err()
            {
                abort.abort();
            }
        }
        self.state.hub.shutdown().await;
    }

    /// The test database as this server opened it.
    pub fn db(&self) -> &SqliteStore {
        &self.db
    }

    /// `base` + `path`.
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
        // Close the database before the temporary directory is removed (Windows will not delete
        // open files). `Drop` cannot await, and the test's runtime is busy running this, so the
        // closing runs on a runtime of its own on another thread.
        let db = self.db.clone();
        std::thread::scope(|s| {
            s.spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("a runtime to close the database");
                rt.block_on(async {
                    let _ = tokio::time::timeout(Duration::from_secs(5), db.close()).await;
                });
            });
        });
    }
}

/// A store that delegates to a real one and can be told to fail, for the paths a working database
/// never takes. Every [`Store`] method delegates; add yours when you add one to the trait.
pub struct FlakyStore {
    inner: RwLock<Arc<dyn Store>>,
    fail_health: AtomicBool,
    fail_commit: Arc<AtomicBool>,
}

impl FlakyStore {
    pub fn new(inner: Arc<dyn Store>) -> Self {
        Self {
            inner: RwLock::new(inner),
            fail_health: AtomicBool::new(false),
            fail_commit: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The store this one delegates to.
    fn inner(&self) -> Arc<dyn Store> {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Delegates to `inner` from now on (`TestServer::restart` puts the reopened database here).
    pub fn set_inner(&self, inner: Arc<dyn Store>) {
        *self.inner.write().unwrap_or_else(PoisonError::into_inner) = inner;
    }

    /// Makes `health` fail (`true`) or delegate again (`false`).
    pub fn fail_health(&self, fail: bool) {
        self.fail_health.store(fail, Ordering::SeqCst);
    }

    /// Makes every transaction's `commit` roll back and fail (`true`), or delegate again (`false`).
    /// Savepoints are not affected: the enclosing transaction's commit is what fails.
    pub fn fail_commit(&self, fail: bool) {
        self.fail_commit.store(fail, Ordering::SeqCst);
    }
}

#[async_trait]
impl Store for FlakyStore {
    async fn health(&self) -> anyhow::Result<()> {
        if self.fail_health.load(Ordering::SeqCst) {
            anyhow::bail!("the flaky store was told to fail health");
        }
        self.inner().health().await
    }

    async fn schema_status(&self) -> anyhow::Result<SchemaStatus> {
        self.inner().schema_status().await
    }

    async fn begin(&self) -> anyhow::Result<Box<dyn Tx>> {
        Ok(Box::new(FlakyTx {
            inner: self.inner().begin().await?,
            fail_commit: self.fail_commit.clone(),
        }))
    }

    async fn user_count(&self) -> anyhow::Result<u64> {
        self.inner().user_count().await
    }

    async fn user_by_id(&self, id: &str) -> anyhow::Result<Option<User>> {
        self.inner().user_by_id(id).await
    }

    async fn user_by_email(&self, email: &str) -> anyhow::Result<Option<User>> {
        self.inner().user_by_email(email).await
    }

    async fn session_by_hash(&self, token_hash: &str) -> anyhow::Result<Option<Session>> {
        self.inner().session_by_hash(token_hash).await
    }

    async fn password_reset_by_hash(
        &self,
        token_hash: &str,
    ) -> anyhow::Result<Option<PasswordReset>> {
        self.inner().password_reset_by_hash(token_hash).await
    }
}

/// [`FlakyStore`]'s transaction. Every [`Tx`] method delegates; add yours when you add one.
pub struct FlakyTx {
    inner: Box<dyn Tx>,
    fail_commit: Arc<AtomicBool>,
}

#[async_trait]
impl Tx for FlakyTx {
    async fn commit(self: Box<Self>) -> anyhow::Result<()> {
        if self.fail_commit.load(Ordering::SeqCst) {
            self.inner.rollback().await?;
            anyhow::bail!("the flaky store was told to fail commits");
        }
        self.inner.commit().await
    }

    async fn rollback(self: Box<Self>) -> anyhow::Result<()> {
        self.inner.rollback().await
    }

    async fn savepoint(&mut self) -> anyhow::Result<Box<dyn Tx + '_>> {
        self.inner.savepoint().await
    }

    async fn user_count(&mut self) -> anyhow::Result<u64> {
        self.inner.user_count().await
    }

    async fn user_by_id(&mut self, id: &str) -> anyhow::Result<Option<User>> {
        self.inner.user_by_id(id).await
    }

    async fn user_by_email(&mut self, email: &str) -> anyhow::Result<Option<User>> {
        self.inner.user_by_email(email).await
    }

    async fn insert_user(&mut self, user: &User) -> anyhow::Result<()> {
        self.inner.insert_user(user).await
    }

    async fn set_user_name(&mut self, id: &str, name: &str) -> anyhow::Result<()> {
        self.inner.set_user_name(id, name).await
    }

    async fn set_user_password(
        &mut self,
        id: &str,
        password_hash: Option<&str>,
    ) -> anyhow::Result<()> {
        self.inner.set_user_password(id, password_hash).await
    }

    async fn set_user_email_verified(&mut self, id: &str, verified: bool) -> anyhow::Result<()> {
        self.inner.set_user_email_verified(id, verified).await
    }

    async fn set_user_disabled(&mut self, id: &str, disabled: bool) -> anyhow::Result<()> {
        self.inner.set_user_disabled(id, disabled).await
    }

    async fn insert_session(&mut self, session: &Session) -> anyhow::Result<()> {
        self.inner.insert_session(session).await
    }

    async fn delete_session(&mut self, token_hash: &str) -> anyhow::Result<()> {
        self.inner.delete_session(token_hash).await
    }

    async fn delete_user_sessions(&mut self, user_id: &str) -> anyhow::Result<()> {
        self.inner.delete_user_sessions(user_id).await
    }

    async fn delete_other_user_sessions(
        &mut self,
        user_id: &str,
        keep: &str,
    ) -> anyhow::Result<()> {
        self.inner.delete_other_user_sessions(user_id, keep).await
    }

    async fn delete_expired_user_sessions(
        &mut self,
        user_id: &str,
        now: f64,
    ) -> anyhow::Result<()> {
        self.inner.delete_expired_user_sessions(user_id, now).await
    }

    async fn insert_password_reset(&mut self, reset: &PasswordReset) -> anyhow::Result<()> {
        self.inner.insert_password_reset(reset).await
    }

    async fn take_password_reset(
        &mut self,
        token_hash: &str,
    ) -> anyhow::Result<Option<PasswordReset>> {
        self.inner.take_password_reset(token_hash).await
    }

    async fn delete_user_password_resets(&mut self, user_id: &str) -> anyhow::Result<()> {
        self.inner.delete_user_password_resets(user_id).await
    }

    async fn delete_expired_password_resets(&mut self, now: f64) -> anyhow::Result<()> {
        self.inner.delete_expired_password_resets(now).await
    }
}

/// A server admin with an organisation and a project, the starting point of most tests.
pub struct World {
    pub admin: Token,
    pub admin_id: Id,
    pub org: Id,
    pub project: Id,
}

impl World {
    /// Not yet available: accounts arrive with Task 4 and organisations and projects with Task 5,
    /// which gives this its body.
    pub async fn new(_server: &TestServer) -> anyhow::Result<Self> {
        anyhow::bail!("World needs accounts (Task 4) and projects (Task 5); Task 5 provides it")
    }
}

/// A v2 WebSocket client built on `oswx_core::wire`.
pub struct Live {}

impl Live {
    /// Not yet available: the WebSocket arrives with Task 9, which gives `Live` its body.
    pub async fn connect(_server: &TestServer, _token: &str, _doc: &Id) -> anyhow::Result<Self> {
        anyhow::bail!("Live needs the document WebSocket; Task 9 provides it")
    }
}
