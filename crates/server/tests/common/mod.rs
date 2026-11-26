//! The test harness: a real server per test on a free port, mirroring the Python `conftest.py`.
#![allow(dead_code)] // each test file uses part of the harness

mod live;

#[allow(unused_imports)] // (not every test file uses the live client)
pub use live::*;

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use oswx_server::blobs::{Blobs, LocalBlobs};
use oswx_server::hub::Hub;
use oswx_server::journal::{self, JournalWriter};
use oswx_server::mail::MemoryMailer;
use oswx_server::sessions::Sessions;
use oswx_server::store::{
    AssemblyRefRow, AssemblyUse, ChangeEventRow, ChangeRow, ChangeSize, Document, Folder,
    HistoryRow, Id, Invite, Member, NewChange, Org, OrgMember, OrgSummary, PartRow, PasswordReset,
    PinnedComponent, Project, ProjectMember, ProjectWithRole, RoomLease, SchemaStatus, Session,
    SnapshotRow, SqliteStore, Store, Tx, UndoTarget, User, Version, VersionWithAuthor,
};
use oswx_server::{AppState, Settings};
use reqwest::StatusCode;
use serde_json::{Value, json};
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

/// Opens the blob storage the settings name: a folder in the test's temporary directory.
fn open_blobs(settings: &Settings) -> Arc<dyn Blobs> {
    Arc::new(LocalBlobs::new(&settings.storage_url).expect("the test storage folder"))
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
        Self::start_inner(settings, state, new_journal).await
    }

    /// [`TestServer::start_custom`], with the journal (when the test put in a store of its own) made
    /// by `journal` on that store.
    async fn start_inner(
        settings: impl FnOnce(&mut Settings),
        state: impl FnOnce(&mut AppState),
        journal: impl FnOnce(&Arc<dyn Store>) -> JournalWriter,
    ) -> Self {
        let dir = Arc::new(TempDir::new().expect("a temporary directory"));
        let mut s = test_settings(dir.path());
        settings(&mut s);
        let db = open_store(&s).await;
        let blobs = open_blobs(&s);
        let plain: Arc<dyn Store> = Arc::new(db.clone());
        let mut app_state = AppState::new(s, plain.clone(), blobs);
        state(&mut app_state);
        if !std::ptr::addr_eq(Arc::as_ptr(&app_state.store), Arc::as_ptr(&plain)) {
            // The test put in a store of its own: the journal writes through it too.
            app_state.journal = journal(&app_state.store);
        }
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
        Self::start_flaky_journal(settings, new_journal).await
    }

    /// [`TestServer::start_flaky`] with the journal writer `journal` makes on the flaky store (other
    /// batching or a shorter batch timeout). After `restart` the journal is the default one.
    pub async fn start_flaky_journal(
        settings: impl FnOnce(&mut Settings),
        journal: impl FnOnce(&Arc<dyn Store>) -> JournalWriter,
    ) -> (Self, Arc<FlakyStore>) {
        Self::start_flaky_full(settings, journal, |_| {}).await
    }

    /// [`TestServer::start_flaky_journal`], with the state then changed by `state` (room hooks, a
    /// blob double).
    pub async fn start_flaky_full(
        settings: impl FnOnce(&mut Settings),
        journal: impl FnOnce(&Arc<dyn Store>) -> JournalWriter,
        state: impl FnOnce(&mut AppState),
    ) -> (Self, Arc<FlakyStore>) {
        let mut handle = None;
        let mut server = Self::start_inner(
            settings,
            |s| {
                let flaky = Arc::new(FlakyStore::new(s.store.clone()));
                handle = Some(flaky.clone());
                s.store = flaky;
                state(s);
            },
            journal,
        )
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
        state.journal = new_journal(&state.store);
        state.node_id = oswx_server::store::new_id();
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
        let app = oswx_server::app(state.clone())
            .into_make_service_with_connect_info::<std::net::SocketAddr>();
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

    /// Stops accepting, gives open requests a moment to finish, then shuts the state down (the hub,
    /// then the journal, which writes what it has queued).
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
        self.state.shutdown().await;
    }

    /// The test database as this server opened it.
    pub fn db(&self) -> &SqliteStore {
        &self.db
    }

    /// `base` + `path`.
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Starts with a mail server configured, and a [`MemoryMailer`] in its place that the test reads.
    pub async fn start_mailing(settings: impl FnOnce(&mut Settings)) -> (Self, Arc<MemoryMailer>) {
        let mailer = Arc::new(MemoryMailer::new(true));
        let kept = mailer.clone();
        let server = Self::start_custom(
            |s| {
                s.smtp_host = "127.0.0.1".into();
                s.smtp_from = "collab@example.org".into();
                settings(s);
            },
            |s| s.mailer = kept,
        )
        .await;
        (server, mailer)
    }

    /// Sends a request with an optional bearer token and JSON body; answers the status and the JSON
    /// body (`Null` for an empty one).
    pub async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = reqwest::Client::new().request(method, self.url(path));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("the request is answered");
        let status = response.status();
        let text = response.text().await.expect("a body");
        let body = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
        };
        (status, body)
    }

    pub async fn get(&self, path: &str, token: Option<&str>) -> (StatusCode, Value) {
        self.call(reqwest::Method::GET, path, token, None).await
    }

    pub async fn post(&self, path: &str, token: Option<&str>, body: Value) -> (StatusCode, Value) {
        self.call(reqwest::Method::POST, path, token, Some(body))
            .await
    }

    pub async fn patch(&self, path: &str, token: Option<&str>, body: Value) -> (StatusCode, Value) {
        self.call(reqwest::Method::PATCH, path, token, Some(body))
            .await
    }

    pub async fn put(&self, path: &str, token: Option<&str>, body: Value) -> (StatusCode, Value) {
        self.call(reqwest::Method::PUT, path, token, Some(body))
            .await
    }

    pub async fn delete(&self, path: &str, token: Option<&str>) -> (StatusCode, Value) {
        self.call(reqwest::Method::DELETE, path, token, None).await
    }

    /// Registers an account with an invite token (password [`PASSWORD`], named `name` or the
    /// address's local part) and answers its token and id; panics unless the server answers 201.
    pub async fn register_invited(
        &self,
        email: &str,
        name: Option<&str>,
        invite: &str,
    ) -> (Token, Id) {
        let name = name.unwrap_or_else(|| email.split('@').next().unwrap_or_default());
        let (status, body) = self
            .post(
                "/api/auth/register",
                None,
                json!({"email": email, "password": PASSWORD, "name": name, "invite": invite}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        (
            body["token"].as_str().expect("a token").to_owned(),
            body["user"]["id"].as_str().expect("an id").to_owned(),
        )
    }

    /// Registers an account (password [`PASSWORD`], name the address's local part) and answers its
    /// token and id; panics unless the server answers 201.
    pub async fn register(&self, email: &str) -> (Token, Id) {
        self.register_with(email, PASSWORD).await
    }

    /// [`TestServer::register`] with another password.
    pub async fn register_with(&self, email: &str, password: &str) -> (Token, Id) {
        let name = email.split('@').next().unwrap_or_default();
        let (status, body) = self
            .post(
                "/api/auth/register",
                None,
                json!({"email": email, "password": password, "name": name}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        (
            body["token"].as_str().expect("a token").to_owned(),
            body["user"]["id"].as_str().expect("an id").to_owned(),
        )
    }

    /// Signs in and answers the token; panics unless the server answers 200.
    pub async fn login(&self, email: &str, password: &str) -> Token {
        let (status, body) = self
            .post(
                "/api/auth/login",
                None,
                json!({"email": email, "password": password}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["token"].as_str().expect("a token").to_owned()
    }
}

/// A journal writer on `store` with the server's batch settings.
pub fn new_journal(store: &Arc<dyn Store>) -> JournalWriter {
    JournalWriter::start(store.clone(), journal::MAX_BATCH, journal::MAX_WAIT)
}

/// Waits `d`, and not at all when it is zero (a zero-length tokio sleep still waits for the next
/// timer tick, which on the journal's commit path costs every batch milliseconds).
pub async fn pause(d: Duration) {
    if !d.is_zero() {
        tokio::time::sleep(d).await;
    }
}

/// The password [`TestServer::register`] gives accounts.
pub const PASSWORD: &str = "correct horse";

/// What `tracing` wrote on one thread while a [`LogCapture`] was alive there.
type LogBuffer = Arc<Mutex<Vec<u8>>>;

thread_local! {
    /// The buffer of this thread's live [`LogCapture`], if any.
    static CAPTURING: std::cell::RefCell<Option<LogBuffer>> = const { std::cell::RefCell::new(None) };
}

/// The global subscriber's writer: whatever this thread's [`LogCapture`] collects, or nowhere.
struct ThisThread(Option<LogBuffer>);

impl std::io::Write for ThisThread {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(b) = &self.0 {
            b.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend_from_slice(buf);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct ToThisThread;

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ToThisThread {
    type Writer = ThisThread;

    fn make_writer(&'a self) -> ThisThread {
        ThisThread(CAPTURING.with(|c| c.borrow().clone()))
    }
}

/// Captures every `tracing` event, at every level, emitted on this thread while it is alive. A
/// `#[tokio::test]` runs the test, its server and its client on one thread, so that is everything
/// the server logs (except on the blocking pool, which logs nothing).
///
/// The subscriber is global (one per test binary, installed by the first capture) and writes to the
/// capturing thread's buffer. A scoped default subscriber (`tracing::subscriber::set_default`) is
/// not used: `tracing` caches per call site whether anyone listens, and a call site first reached by
/// a test thread without one can stay switched off for the thread that has one.
pub struct LogCapture {
    buffer: LogBuffer,
}

impl LogCapture {
    pub fn start() -> Self {
        static INSTALL: std::sync::Once = std::sync::Once::new();
        INSTALL.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(ToThisThread)
                .with_max_level(tracing::Level::TRACE)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("no other global subscriber in the tests");
        });
        let buffer = LogBuffer::default();
        CAPTURING.with(|c| *c.borrow_mut() = Some(buffer.clone()));
        Self { buffer }
    }

    /// Everything captured so far.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.buffer.lock().unwrap_or_else(PoisonError::into_inner))
            .into_owned()
    }

    /// Panics if any captured line contains one of `secrets` (or if nothing was captured at all,
    /// which would make the check meaningless).
    pub fn assert_never_logged(&self, secrets: &[&str]) {
        let text = self.text();
        assert!(!text.is_empty(), "nothing was captured");
        for secret in secrets {
            assert!(!secret.is_empty());
            if let Some(line) = text.lines().find(|l| l.contains(secret)) {
                panic!("a secret was logged: {line}");
            }
        }
    }
}

impl Drop for LogCapture {
    fn drop(&mut self) {
        CAPTURING.with(|c| *c.borrow_mut() = None);
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
    fail_reverted_by: Arc<AtomicBool>,
    commit_then_fail: Arc<AtomicBool>,
    fail_document_reads: AtomicBool,
    panic_on_begin: AtomicBool,
    hang_commit: Arc<AtomicBool>,
    begins: AtomicUsize,
    refuse_change: Arc<Mutex<Option<String>>>,
    refuse_server_change: Arc<AtomicBool>,
    change_lookups: AtomicUsize,
    lookup_delay: Mutex<Duration>,
    duplicate_inserts: Arc<AtomicUsize>,
    /// The order change-id lookups ended and undo targets were asked for in.
    lookup_log: Mutex<Vec<&'static str>>,
    /// A delay for the next change-id lookup only.
    lookup_delay_once: Mutex<Option<Duration>>,
    preloads: AtomicUsize,
    preload_delay: Mutex<Duration>,
    commit_delay: Arc<Mutex<Duration>>,
    /// Shut: every top-level `commit` parks here (before it commits) until opened.
    pub commit_gate: Arc<oswx_server::room::Gate>,
    /// Shut: every [`Store::parts_by_ids`] parks here, after it has read its answer, until opened.
    pub parts_gate: Arc<oswx_server::room::Gate>,
    /// How many times `replace_assembly_refs` ran (in a transaction or savepoint).
    ref_writes: Arc<AtomicUsize>,
    fail_part_reads: AtomicBool,
    /// How many [`Store`] calls were made (each goes through `inner`; transactions count once).
    calls: AtomicUsize,
}

impl FlakyStore {
    pub fn new(inner: Arc<dyn Store>) -> Self {
        Self {
            inner: RwLock::new(inner),
            fail_health: AtomicBool::new(false),
            fail_commit: Arc::new(AtomicBool::new(false)),
            fail_reverted_by: Arc::new(AtomicBool::new(false)),
            commit_then_fail: Arc::new(AtomicBool::new(false)),
            fail_document_reads: AtomicBool::new(false),
            panic_on_begin: AtomicBool::new(false),
            hang_commit: Arc::new(AtomicBool::new(false)),
            begins: AtomicUsize::new(0),
            refuse_change: Arc::new(Mutex::new(None)),
            refuse_server_change: Arc::new(AtomicBool::new(false)),
            change_lookups: AtomicUsize::new(0),
            lookup_delay: Mutex::new(Duration::ZERO),
            duplicate_inserts: Arc::new(AtomicUsize::new(0)),
            lookup_log: Mutex::new(Vec::new()),
            lookup_delay_once: Mutex::new(None),
            preloads: AtomicUsize::new(0),
            preload_delay: Mutex::new(Duration::ZERO),
            commit_delay: Arc::new(Mutex::new(Duration::ZERO)),
            commit_gate: Arc::default(),
            parts_gate: Arc::default(),
            ref_writes: Arc::new(AtomicUsize::new(0)),
            fail_part_reads: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        }
    }

    /// How many [`Store`] calls (reads, and `begin`) were made so far.
    pub fn store_calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// How many times an assembly's references were rewritten.
    pub fn ref_writes(&self) -> usize {
        self.ref_writes.load(Ordering::SeqCst)
    }

    /// Makes [`Store::parts_by_ids`] fail (`true`) or delegate again (`false`).
    pub fn fail_part_reads(&self, fail: bool) {
        self.fail_part_reads.store(fail, Ordering::SeqCst);
    }

    /// Makes the next journal insert of a change the server made (no change id) insert nothing:
    /// the journal refuses that record.
    pub fn refuse_server_change_once(&self) {
        self.refuse_server_change.store(true, Ordering::SeqCst);
    }

    /// Makes the next journal insert of a change with this `change_id` insert nothing, as if the
    /// journal already had it (the journal then refuses that record as a duplicate).
    pub fn refuse_change_once(&self, change_id: &str) {
        *self
            .refuse_change
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(change_id.to_owned());
    }

    /// Makes every [`Store::change_seq_by_client`] wait this long first.
    pub fn lookup_delay(&self, d: Duration) {
        *self
            .lookup_delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = d;
    }

    /// Makes the next [`Store::change_seq_by_client`] (only) wait this long first.
    pub fn lookup_delay_once(&self, d: Duration) {
        *self
            .lookup_delay_once
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(d);
    }

    /// In which order change-id lookups ended and undo targets were asked for.
    pub fn lookup_log(&self) -> Vec<&'static str> {
        self.lookup_log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn log_lookup(&self, what: &'static str) {
        self.lookup_log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(what);
    }

    /// How many journal inserts found the change id already there (a duplicate reaching the journal).
    pub fn duplicate_inserts(&self) -> usize {
        self.duplicate_inserts.load(Ordering::SeqCst)
    }

    /// Makes every [`Store::recent_change_ids`] wait this long after it has read its answer (so the
    /// answer can be stale by the time the caller gets it).
    pub fn preload_delay(&self, d: Duration) {
        *self
            .preload_delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = d;
    }

    /// Makes every transaction's successful `commit` wait this long before it reports success.
    pub fn commit_delay(&self, d: Duration) {
        *self
            .commit_delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = d;
    }

    /// How many times [`Store::recent_change_ids`] was asked.
    pub fn preloads(&self) -> usize {
        self.preloads.load(Ordering::SeqCst)
    }

    /// How many times [`Store::change_seq_by_client`] was asked.
    pub fn change_lookups(&self) -> usize {
        self.change_lookups.load(Ordering::SeqCst)
    }

    /// Makes [`Store::begin`] panic (`true`), as a bug in the caller's task would, or delegate
    /// again (`false`).
    pub fn panic_on_begin(&self, panic: bool) {
        self.panic_on_begin.store(panic, Ordering::SeqCst);
    }

    /// Makes every transaction's `commit` never return (`true`), as a database that stopped
    /// answering; or delegate again (`false`). A transaction dropped while hanging rolls back.
    pub fn hang_commit(&self, hang: bool) {
        self.hang_commit.store(hang, Ordering::SeqCst);
    }

    /// How many transactions [`Store::begin`] has opened so far.
    pub fn begins(&self) -> usize {
        self.begins.load(Ordering::SeqCst)
    }

    /// The store this one delegates to.
    fn inner(&self) -> Arc<dyn Store> {
        self.calls.fetch_add(1, Ordering::SeqCst);
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

    /// Makes every transaction's `commit` take effect and then report an error anyway (`true`), as
    /// a connection lost during `COMMIT` can; or delegate again (`false`).
    pub fn commit_then_fail(&self, fail: bool) {
        self.commit_then_fail.store(fail, Ordering::SeqCst);
    }

    /// Makes [`Store::document_by_id`] fail (`true`) or delegate again (`false`).
    /// Makes every journal `set_reverted_by` fail (an extra write of an undo or revert).
    pub fn fail_reverted_by(&self, fail: bool) {
        self.fail_reverted_by.store(fail, Ordering::SeqCst);
    }

    pub fn fail_document_reads(&self, fail: bool) {
        self.fail_document_reads.store(fail, Ordering::SeqCst);
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
        if self.panic_on_begin.load(Ordering::SeqCst) {
            panic!("the flaky store was told to panic in begin");
        }
        self.begins.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FlakyTx {
            inner: self.inner().begin().await?,
            savepoint: false,
            fail_commit: self.fail_commit.clone(),
            fail_reverted_by: self.fail_reverted_by.clone(),
            commit_then_fail: self.commit_then_fail.clone(),
            hang_commit: self.hang_commit.clone(),
            refuse_change: self.refuse_change.clone(),
            refuse_server_change: self.refuse_server_change.clone(),
            duplicate_inserts: self.duplicate_inserts.clone(),
            commit_delay: self.commit_delay.clone(),
            commit_gate: self.commit_gate.clone(),
            ref_writes: self.ref_writes.clone(),
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

    async fn org_by_id(&self, id: &str) -> anyhow::Result<Option<Org>> {
        self.inner().org_by_id(id).await
    }

    async fn org_member(&self, org_id: &str, user_id: &str) -> anyhow::Result<Option<OrgMember>> {
        self.inner().org_member(org_id, user_id).await
    }

    async fn orgs_of_user(&self, user_id: &str) -> anyhow::Result<Vec<OrgSummary>> {
        self.inner().orgs_of_user(user_id).await
    }

    async fn org_members(&self, org_id: &str) -> anyhow::Result<Vec<Member>> {
        self.inner().org_members(org_id).await
    }

    async fn org_member_count(&self, org_id: &str) -> anyhow::Result<u64> {
        self.inner().org_member_count(org_id).await
    }

    async fn project_by_id(&self, id: &str) -> anyhow::Result<Option<Project>> {
        self.inner().project_by_id(id).await
    }

    async fn project_member(
        &self,
        project_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Option<ProjectMember>> {
        self.inner().project_member(project_id, user_id).await
    }

    async fn projects_in_org(&self, org_id: &str) -> anyhow::Result<Vec<Project>> {
        self.inner().projects_in_org(org_id).await
    }

    async fn member_projects(
        &self,
        org_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Vec<ProjectWithRole>> {
        self.inner().member_projects(org_id, user_id).await
    }

    async fn project_members(&self, project_id: &str) -> anyhow::Result<Vec<Member>> {
        self.inner().project_members(project_id).await
    }

    async fn invite_by_id(&self, id: &str) -> anyhow::Result<Option<Invite>> {
        self.inner().invite_by_id(id).await
    }

    async fn invite_by_hash(&self, token_hash: &str) -> anyhow::Result<Option<Invite>> {
        self.inner().invite_by_hash(token_hash).await
    }

    async fn open_invites(&self, org_id: &str, now: f64) -> anyhow::Result<Vec<Invite>> {
        self.inner().open_invites(org_id, now).await
    }

    async fn document_by_id(&self, id: &str) -> anyhow::Result<Option<Document>> {
        if self.fail_document_reads.load(Ordering::SeqCst) {
            anyhow::bail!("the flaky store was told to fail document reads");
        }
        self.inner().document_by_id(id).await
    }

    async fn documents_in_project(&self, project_id: &str) -> anyhow::Result<Vec<Document>> {
        self.inner().documents_in_project(project_id).await
    }

    async fn latest_snapshot(&self, document_id: &str) -> anyhow::Result<Option<SnapshotRow>> {
        self.inner().latest_snapshot(document_id).await
    }

    async fn folder_by_id(&self, id: &str) -> anyhow::Result<Option<Folder>> {
        self.inner().folder_by_id(id).await
    }

    async fn folders_in_project(&self, project_id: &str) -> anyhow::Result<Vec<Folder>> {
        self.inner().folders_in_project(project_id).await
    }

    async fn changes_between(
        &self,
        document_id: &str,
        after: i64,
        upto: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ChangeRow>> {
        self.inner()
            .changes_between(document_id, after, upto, limit)
            .await
    }

    async fn change_by_seq(
        &self,
        document_id: &str,
        seq: i64,
    ) -> anyhow::Result<Option<ChangeRow>> {
        self.inner().change_by_seq(document_id, seq).await
    }

    async fn change_events_between(
        &self,
        document_id: &str,
        after: i64,
        upto: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ChangeEventRow>> {
        self.inner()
            .change_events_between(document_id, after, upto, limit)
            .await
    }

    async fn recent_change_ids(
        &self,
        document_id: &str,
        client_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<(String, i64)>> {
        self.preloads.fetch_add(1, Ordering::SeqCst);
        let found = self
            .inner()
            .recent_change_ids(document_id, client_id, limit)
            .await;
        let delay = *self
            .preload_delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        pause(delay).await;
        found
    }

    async fn change_seq_by_client(
        &self,
        document_id: &str,
        client_id: &str,
        change_id: &str,
    ) -> anyhow::Result<Option<i64>> {
        self.change_lookups.fetch_add(1, Ordering::SeqCst);
        let once = self
            .lookup_delay_once
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let delay = once.unwrap_or_else(|| {
            *self
                .lookup_delay
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        });
        pause(delay).await;
        let found = self
            .inner()
            .change_seq_by_client(document_id, client_id, change_id)
            .await;
        self.log_lookup("change id looked up");
        found
    }

    async fn undo_target(
        &self,
        document_id: &str,
        client_id: &str,
        change_id: &str,
    ) -> anyhow::Result<Option<UndoTarget>> {
        self.log_lookup("undo target asked");
        let delay = *self
            .lookup_delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        pause(delay).await;
        self.inner()
            .undo_target(document_id, client_id, change_id)
            .await
    }

    async fn change_sizes(
        &self,
        document_id: &str,
        before: i64,
        after: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ChangeSize>> {
        self.inner()
            .change_sizes(document_id, before, after, limit)
            .await
    }

    async fn history(
        &self,
        document_id: &str,
        before: i64,
        after: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<HistoryRow>> {
        self.inner()
            .history(document_id, before, after, limit)
            .await
    }

    async fn versions(&self, document_id: &str) -> anyhow::Result<Vec<VersionWithAuthor>> {
        self.inner().versions(document_id).await
    }

    async fn version_by_name(
        &self,
        document_id: &str,
        name: &str,
    ) -> anyhow::Result<Option<Version>> {
        self.inner().version_by_name(document_id, name).await
    }

    async fn snapshot_at_or_before(
        &self,
        document_id: &str,
        seq: i64,
    ) -> anyhow::Result<Option<SnapshotRow>> {
        self.inner().snapshot_at_or_before(document_id, seq).await
    }

    async fn snapshot_row(
        &self,
        document_id: &str,
        epoch: i64,
        seq: i64,
    ) -> anyhow::Result<Option<SnapshotRow>> {
        self.inner().snapshot_row(document_id, epoch, seq).await
    }

    async fn room_lease(&self, document_id: &str) -> anyhow::Result<Option<RoomLease>> {
        self.inner().room_lease(document_id).await
    }

    async fn assembly_refs(&self, assembly_id: &str) -> anyhow::Result<Vec<AssemblyRefRow>> {
        self.inner().assembly_refs(assembly_id).await
    }

    async fn parts_by_ids(&self, ids: &[Id]) -> anyhow::Result<Vec<PartRow>> {
        if self.fail_part_reads.load(Ordering::SeqCst) {
            anyhow::bail!("the flaky store was told to fail part reads");
        }
        let found = self.inner().parts_by_ids(ids).await;
        self.parts_gate.pass().await;
        found
    }

    async fn assemblies_using(&self, part_id: &str) -> anyhow::Result<Vec<AssemblyUse>> {
        self.inner().assemblies_using(part_id).await
    }

    async fn pinned_before(&self, part_id: &str, seq: i64) -> anyhow::Result<Vec<PinnedComponent>> {
        self.inner().pinned_before(part_id, seq).await
    }

    async fn versions_of(&self, ids: &[Id]) -> anyhow::Result<Vec<VersionWithAuthor>> {
        self.inner().versions_of(ids).await
    }

    async fn refs_to_part(&self, part_id: &str) -> anyhow::Result<Vec<AssemblyRefRow>> {
        self.inner().refs_to_part(part_id).await
    }
}

/// [`FlakyStore`]'s transaction (or a savepoint in one). Every [`Tx`] method delegates; add yours
/// when you add one.
pub struct FlakyTx<'a> {
    inner: Box<dyn Tx + 'a>,
    /// A savepoint: its commit is not made to fail, hang or wait (only the outer one is).
    savepoint: bool,
    fail_commit: Arc<AtomicBool>,
    fail_reverted_by: Arc<AtomicBool>,
    commit_then_fail: Arc<AtomicBool>,
    hang_commit: Arc<AtomicBool>,
    refuse_change: Arc<Mutex<Option<String>>>,
    refuse_server_change: Arc<AtomicBool>,
    duplicate_inserts: Arc<AtomicUsize>,
    commit_delay: Arc<Mutex<Duration>>,
    commit_gate: Arc<oswx_server::room::Gate>,
    ref_writes: Arc<AtomicUsize>,
}

#[async_trait]
impl Tx for FlakyTx<'_> {
    async fn commit(self: Box<Self>) -> anyhow::Result<()> {
        if self.savepoint {
            return self.inner.commit().await;
        }
        self.commit_gate.pass().await;
        if self.hang_commit.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.fail_commit.load(Ordering::SeqCst) {
            self.inner.rollback().await?;
            anyhow::bail!("the flaky store was told to fail commits");
        }
        if self.commit_then_fail.load(Ordering::SeqCst) {
            self.inner.commit().await?;
            anyhow::bail!("the flaky store committed, then was told to report an error");
        }
        let delay = *self
            .commit_delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.inner.commit().await?;
        pause(delay).await;
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> anyhow::Result<()> {
        self.inner.rollback().await
    }

    async fn savepoint(&mut self) -> anyhow::Result<Box<dyn Tx + '_>> {
        let (fail_commit, fail_reverted_by) =
            (self.fail_commit.clone(), self.fail_reverted_by.clone());
        let (commit_then_fail, hang_commit) =
            (self.commit_then_fail.clone(), self.hang_commit.clone());
        let (refuse_change, duplicate_inserts, commit_delay) = (
            self.refuse_change.clone(),
            self.duplicate_inserts.clone(),
            self.commit_delay.clone(),
        );
        Ok(Box::new(FlakyTx {
            inner: self.inner.savepoint().await?,
            savepoint: true,
            fail_commit,
            fail_reverted_by,
            commit_then_fail,
            hang_commit,
            refuse_change,
            refuse_server_change: self.refuse_server_change.clone(),
            duplicate_inserts,
            commit_delay,
            commit_gate: self.commit_gate.clone(),
            ref_writes: self.ref_writes.clone(),
        }))
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

    async fn insert_org(&mut self, org: &Org) -> anyhow::Result<()> {
        self.inner.insert_org(org).await
    }

    async fn set_org_name(&mut self, id: &str, name: &str) -> anyhow::Result<()> {
        self.inner.set_org_name(id, name).await
    }

    async fn org_by_id(&mut self, id: &str) -> anyhow::Result<Option<Org>> {
        self.inner.org_by_id(id).await
    }

    async fn org_member(
        &mut self,
        org_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Option<OrgMember>> {
        self.inner.org_member(org_id, user_id).await
    }

    async fn insert_org_member(&mut self, member: &OrgMember) -> anyhow::Result<()> {
        self.inner.insert_org_member(member).await
    }

    async fn set_org_member_role(
        &mut self,
        org_id: &str,
        user_id: &str,
        role: &str,
    ) -> anyhow::Result<()> {
        self.inner.set_org_member_role(org_id, user_id, role).await
    }

    async fn delete_org_member(&mut self, org_id: &str, user_id: &str) -> anyhow::Result<()> {
        self.inner.delete_org_member(org_id, user_id).await
    }

    async fn org_member_count(&mut self, org_id: &str) -> anyhow::Result<u64> {
        self.inner.org_member_count(org_id).await
    }

    async fn org_owner_count(&mut self, org_id: &str) -> anyhow::Result<u64> {
        self.inner.org_owner_count(org_id).await
    }

    async fn delete_project_members_in_org(
        &mut self,
        org_id: &str,
        user_id: &str,
    ) -> anyhow::Result<()> {
        self.inner
            .delete_project_members_in_org(org_id, user_id)
            .await
    }

    async fn invite_by_hash(&mut self, token_hash: &str) -> anyhow::Result<Option<Invite>> {
        self.inner.invite_by_hash(token_hash).await
    }

    async fn insert_invite(&mut self, invite: &Invite) -> anyhow::Result<()> {
        self.inner.insert_invite(invite).await
    }

    async fn claim_invite(&mut self, id: &str, user_id: &str) -> anyhow::Result<bool> {
        self.inner.claim_invite(id, user_id).await
    }

    async fn delete_invite(&mut self, org_id: &str, id: &str) -> anyhow::Result<bool> {
        self.inner.delete_invite(org_id, id).await
    }

    async fn insert_project(&mut self, project: &Project) -> anyhow::Result<()> {
        self.inner.insert_project(project).await
    }

    async fn set_project_name(&mut self, id: &str, name: &str) -> anyhow::Result<()> {
        self.inner.set_project_name(id, name).await
    }

    async fn delete_project(&mut self, id: &str) -> anyhow::Result<()> {
        self.inner.delete_project(id).await
    }

    async fn project_by_id(&mut self, id: &str) -> anyhow::Result<Option<Project>> {
        self.inner.project_by_id(id).await
    }

    async fn project_member(
        &mut self,
        project_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Option<ProjectMember>> {
        self.inner.project_member(project_id, user_id).await
    }

    async fn insert_project_member(&mut self, member: &ProjectMember) -> anyhow::Result<()> {
        self.inner.insert_project_member(member).await
    }

    async fn set_project_member_role(
        &mut self,
        project_id: &str,
        user_id: &str,
        role: &str,
    ) -> anyhow::Result<()> {
        self.inner
            .set_project_member_role(project_id, user_id, role)
            .await
    }

    async fn delete_project_member(
        &mut self,
        project_id: &str,
        user_id: &str,
    ) -> anyhow::Result<bool> {
        self.inner.delete_project_member(project_id, user_id).await
    }

    async fn project_document_ids(&mut self, project_id: &str) -> anyhow::Result<Vec<Id>> {
        self.inner.project_document_ids(project_id).await
    }

    async fn mark_project_documents_deleted(
        &mut self,
        project_id: &str,
        now: f64,
    ) -> anyhow::Result<()> {
        self.inner
            .mark_project_documents_deleted(project_id, now)
            .await
    }

    async fn insert_document(&mut self, document: &Document) -> anyhow::Result<()> {
        self.inner.insert_document(document).await
    }

    async fn insert_snapshot(&mut self, snapshot: &SnapshotRow) -> anyhow::Result<()> {
        self.inner.insert_snapshot(snapshot).await
    }

    async fn document_by_id(&mut self, id: &str) -> anyhow::Result<Option<Document>> {
        self.inner.document_by_id(id).await
    }

    async fn set_document_name(&mut self, id: &str, name: &str) -> anyhow::Result<()> {
        self.inner.set_document_name(id, name).await
    }

    async fn set_document_folder(
        &mut self,
        id: &str,
        folder_id: Option<&str>,
    ) -> anyhow::Result<()> {
        self.inner.set_document_folder(id, folder_id).await
    }

    async fn mark_document_deleted(&mut self, id: &str, now: f64) -> anyhow::Result<bool> {
        self.inner.mark_document_deleted(id, now).await
    }

    async fn insert_folder(&mut self, folder: &Folder) -> anyhow::Result<()> {
        self.inner.insert_folder(folder).await
    }

    async fn folder_by_id(&mut self, id: &str) -> anyhow::Result<Option<Folder>> {
        self.inner.folder_by_id(id).await
    }

    async fn folders_in_project(&mut self, project_id: &str) -> anyhow::Result<Vec<Folder>> {
        self.inner.folders_in_project(project_id).await
    }

    async fn set_folder_name(&mut self, id: &str, name: &str) -> anyhow::Result<()> {
        self.inner.set_folder_name(id, name).await
    }

    async fn set_folder_parent(&mut self, id: &str, parent_id: Option<&str>) -> anyhow::Result<()> {
        self.inner.set_folder_parent(id, parent_id).await
    }

    async fn move_child_folders(
        &mut self,
        folder_id: &str,
        parent_id: Option<&str>,
    ) -> anyhow::Result<()> {
        self.inner.move_child_folders(folder_id, parent_id).await
    }

    async fn move_folder_documents(
        &mut self,
        folder_id: &str,
        parent_id: Option<&str>,
    ) -> anyhow::Result<()> {
        self.inner.move_folder_documents(folder_id, parent_id).await
    }

    async fn delete_folder(&mut self, id: &str) -> anyhow::Result<()> {
        self.inner.delete_folder(id).await
    }

    async fn insert_change(&mut self, change: &NewChange<'_>) -> anyhow::Result<bool> {
        {
            let mut refuse = self
                .refuse_change
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if refuse.is_some() && refuse.as_deref() == change.change_id {
                *refuse = None;
                return Ok(false);
            }
        }
        if change.change_id.is_none() && self.refuse_server_change.swap(false, Ordering::SeqCst) {
            return Ok(false);
        }
        let inserted = self.inner.insert_change(change).await?;
        if !inserted {
            self.duplicate_inserts.fetch_add(1, Ordering::SeqCst);
        }
        Ok(inserted)
    }

    async fn set_document_head(
        &mut self,
        id: &str,
        head_seq: i64,
        updated_at: f64,
    ) -> anyhow::Result<()> {
        self.inner.set_document_head(id, head_seq, updated_at).await
    }

    async fn set_reverted_by(
        &mut self,
        document_id: &str,
        seq: i64,
        by: i64,
    ) -> anyhow::Result<bool> {
        if self.fail_reverted_by.load(Ordering::SeqCst) {
            anyhow::bail!("the flaky store was told to fail set_reverted_by");
        }
        self.inner.set_reverted_by(document_id, seq, by).await
    }

    async fn change_broke(
        &mut self,
        document_id: &str,
        seq: i64,
    ) -> anyhow::Result<Option<Option<String>>> {
        self.inner.change_broke(document_id, seq).await
    }

    async fn set_change_broke(
        &mut self,
        document_id: &str,
        seq: i64,
        broke: &str,
    ) -> anyhow::Result<()> {
        self.inner.set_change_broke(document_id, seq, broke).await
    }

    async fn insert_version(&mut self, version: &Version) -> anyhow::Result<bool> {
        self.inner.insert_version(version).await
    }

    async fn delete_version(&mut self, document_id: &str, id: &str) -> anyhow::Result<bool> {
        self.inner.delete_version(document_id, id).await
    }

    async fn replace_assembly_refs(
        &mut self,
        assembly_id: &str,
        rows: &[AssemblyRefRow],
    ) -> anyhow::Result<()> {
        self.ref_writes.fetch_add(1, Ordering::SeqCst);
        self.inner.replace_assembly_refs(assembly_id, rows).await
    }

    async fn lock_live_document(&mut self, id: &str) -> anyhow::Result<bool> {
        self.inner.lock_live_document(id).await
    }

    async fn assemblies_using(&mut self, part_id: &str) -> anyhow::Result<Vec<AssemblyUse>> {
        self.inner.assemblies_using(part_id).await
    }

    async fn assemblies_using_project(
        &mut self,
        project_id: &str,
    ) -> anyhow::Result<Vec<AssemblyUse>> {
        self.inner.assemblies_using_project(project_id).await
    }

    async fn snapshot_row(
        &mut self,
        document_id: &str,
        epoch: i64,
        seq: i64,
    ) -> anyhow::Result<Option<SnapshotRow>> {
        self.inner.snapshot_row(document_id, epoch, seq).await
    }

    async fn room_lease(&mut self, document_id: &str) -> anyhow::Result<Option<RoomLease>> {
        self.inner.room_lease(document_id).await
    }

    async fn lock_room_lease(&mut self, document_id: &str) -> anyhow::Result<Option<i64>> {
        self.inner.lock_room_lease(document_id).await
    }

    async fn insert_room_lease(&mut self, lease: &RoomLease) -> anyhow::Result<()> {
        self.inner.insert_room_lease(lease).await
    }

    async fn take_room_lease(
        &mut self,
        document_id: &str,
        node_id: &str,
        expires_at: f64,
    ) -> anyhow::Result<Option<i64>> {
        self.inner
            .take_room_lease(document_id, node_id, expires_at)
            .await
    }
}

/// A server admin (Ada, `ada@example.com`, the first account) who owns an organisation, "Robotics",
/// with one project, "Rover": the starting point of most tests, as the Python `World`.
pub struct World {
    pub admin: Token,
    pub admin_id: Id,
    pub org: Id,
    pub project: Id,
}

impl World {
    /// Registers the admin on an empty server and creates the organisation and the project.
    pub async fn new(server: &TestServer) -> anyhow::Result<Self> {
        let (status, body) = server
            .post(
                "/api/auth/register",
                None,
                json!({"email": "ada@example.com", "password": PASSWORD, "name": "Ada"}),
            )
            .await;
        anyhow::ensure!(status == StatusCode::CREATED, "register: {status} {body}");
        let admin = body["token"].as_str().unwrap_or_default().to_owned();
        let admin_id = body["user"]["id"].as_str().unwrap_or_default().to_owned();
        let (status, body) = server
            .post("/api/orgs", Some(&admin), json!({"name": "Robotics"}))
            .await;
        anyhow::ensure!(status == StatusCode::CREATED, "create org: {status} {body}");
        let org = body["id"].as_str().unwrap_or_default().to_owned();
        let (status, body) = server
            .post(
                &format!("/api/orgs/{org}/projects"),
                Some(&admin),
                json!({"name": "Rover"}),
            )
            .await;
        anyhow::ensure!(
            status == StatusCode::CREATED,
            "create project: {status} {body}"
        );
        let project = body["id"].as_str().unwrap_or_default().to_owned();
        Ok(Self {
            admin,
            admin_id,
            org,
            project,
        })
    }

    /// Invites `email` into the organisation with the organisation role `org_role` and, unless
    /// `role` is `None`, the project role `role` in the project, then registers the account with the
    /// invite (named `name`, or the address's local part). Answers its token and id; panics if
    /// either step fails.
    pub async fn invite(
        &self,
        server: &TestServer,
        email: &str,
        name: Option<&str>,
        role: Option<&str>,
        org_role: &str,
    ) -> (Token, Id) {
        let grants = match role {
            Some(role) => json!([{"project_id": self.project, "role": role}]),
            None => json!([]),
        };
        let (status, body) = server
            .post(
                &format!("/api/orgs/{}/invites", self.org),
                Some(&self.admin),
                json!({"email": email, "role": org_role, "projects": grants}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let token = body["token"].as_str().expect("an invite token");
        server.register_invited(email, name, token).await
    }

    /// [`World::invite`] as a member of the organisation and an editor of the project.
    pub async fn editor(&self, server: &TestServer, email: &str) -> (Token, Id) {
        self.invite(server, email, None, Some("editor"), "member")
            .await
    }

    /// Creates a document in the project, as the Python `World.document`: `content` (default
    /// [`part`]) named `name` (default "Bracket"), by `token` (default the admin). Answers its id;
    /// panics unless the server answers 201.
    pub async fn document(
        &self,
        server: &TestServer,
        content: Option<Value>,
        name: Option<&str>,
        token: Option<&str>,
    ) -> Id {
        let (status, body) = server
            .post(
                &format!("/api/projects/{}/documents", self.project),
                Some(token.unwrap_or(&self.admin)),
                json!({"name": name.unwrap_or("Bracket"), "content": content.unwrap_or_else(part)}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        body["id"].as_str().expect("an id").to_owned()
    }
}

/// The Python test suite's `PART`: a small openswx part (a sketch and an extrusion).
pub fn part() -> Value {
    json!({
        "format": "openswx-part", "version": 3, "name": "Bracket", "units": "mm",
        "material": {"name": null, "density": 1000.0}, "rollback": null, "active_config": "Default",
        "next_id": 3, "counters": {"Sketch": 1, "Boss-Extrude": 1}, "variables": {},
        "features": [
            {"id": "f1", "name": "Sketch1", "type": "sketch", "suppressed": false,
             "params": {"plane": "Top", "sketch": {"points": {"P1": [0, 0]}, "entities": {}, "counter": 1}}},
            {"id": "f2", "name": "Boss-Extrude1", "type": "extrude", "suppressed": false,
             "params": {"sketch": "Sketch1", "depth": 10}},
        ],
    })
}

/// A server with a [`World`] on it.
pub async fn world() -> (TestServer, World) {
    let server = TestServer::start().await;
    let world = World::new(&server).await.expect("the world is made");
    (server, world)
}

/// Blob storage that delegates to a real one and can be made slow or broken, and counts what it is
/// asked.
pub struct TestBlobs {
    inner: Arc<dyn Blobs>,
    exists_delay: Mutex<Duration>,
    put_delay: Mutex<Duration>,
    fail_exists: AtomicBool,
    fail_put: AtomicBool,
    exists_calls: AtomicUsize,
    exists_now: AtomicUsize,
    exists_most: AtomicUsize,
    puts: AtomicUsize,
    put_failures: AtomicUsize,
}

impl TestBlobs {
    pub fn new(inner: Arc<dyn Blobs>) -> Self {
        Self {
            inner,
            exists_delay: Mutex::new(Duration::ZERO),
            put_delay: Mutex::new(Duration::ZERO),
            fail_exists: AtomicBool::new(false),
            fail_put: AtomicBool::new(false),
            exists_calls: AtomicUsize::new(0),
            exists_now: AtomicUsize::new(0),
            exists_most: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
            put_failures: AtomicUsize::new(0),
        }
    }

    /// Every `exists` waits this long first.
    pub fn exists_delay(&self, d: Duration) {
        *self
            .exists_delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = d;
    }

    /// Every `put` (snapshots) waits this long first.
    pub fn put_delay(&self, d: Duration) {
        *self
            .put_delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = d;
    }

    /// Makes `exists` fail (an outage) or delegate again.
    pub fn fail_exists(&self, fail: bool) {
        self.fail_exists.store(fail, Ordering::SeqCst);
    }

    /// Makes `put` fail or delegate again.
    pub fn fail_put(&self, fail: bool) {
        self.fail_put.store(fail, Ordering::SeqCst);
    }

    /// How many `exists` calls were made.
    pub fn exists_calls(&self) -> usize {
        self.exists_calls.load(Ordering::SeqCst)
    }

    /// The most `exists` calls that were running at once.
    pub fn exists_most(&self) -> usize {
        self.exists_most.load(Ordering::SeqCst)
    }

    /// How many `put` calls were made (failed ones included).
    pub fn puts(&self) -> usize {
        self.puts.load(Ordering::SeqCst)
    }

    /// How many `put` calls failed because the double was told to fail.
    pub fn put_failures(&self) -> usize {
        self.put_failures.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Blobs for TestBlobs {
    async fn put_stream(
        &self,
        key: &str,
        body: oswx_server::blobs::ByteStream,
        limit: u64,
    ) -> anyhow::Result<oswx_server::blobs::PutOutcome> {
        self.inner.put_stream(key, body, limit).await
    }

    async fn get_stream(
        &self,
        key: &str,
    ) -> anyhow::Result<Option<(u64, oswx_server::blobs::ByteStream)>> {
        self.inner.get_stream(key).await
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        self.exists_calls.fetch_add(1, Ordering::SeqCst);
        let now = self.exists_now.fetch_add(1, Ordering::SeqCst) + 1;
        self.exists_most.fetch_max(now, Ordering::SeqCst);
        let delay = *self
            .exists_delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        pause(delay).await;
        self.exists_now.fetch_sub(1, Ordering::SeqCst);
        if self.fail_exists.load(Ordering::SeqCst) {
            anyhow::bail!("the test storage was told to fail exists");
        }
        self.inner.exists(key).await
    }

    async fn put(&self, key: &str, bytes: bytes::Bytes) -> anyhow::Result<()> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        let delay = *self
            .put_delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        pause(delay).await;
        if self.fail_put.load(Ordering::SeqCst) {
            self.put_failures.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("the test storage was told to fail put");
        }
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<bytes::Bytes>> {
        self.inner.get(key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        self.inner.delete_prefix(prefix).await
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }

    async fn health(&self) -> anyhow::Result<()> {
        self.inner.health().await
    }
}

impl TestServer {
    /// Starts with the test settings changed by `settings` and the blob storage wrapped in a
    /// [`TestBlobs`], which is returned so the test can slow it down or break it. It stays in place
    /// across `restart`.
    pub async fn start_blobs(settings: impl FnOnce(&mut Settings)) -> (Self, Arc<TestBlobs>) {
        let mut handle = None;
        let server = Self::start_custom(settings, |s| {
            let blobs = Arc::new(TestBlobs::new(s.blobs.clone()));
            handle = Some(blobs.clone());
            s.blobs = blobs;
        })
        .await;
        (server, handle.expect("the state function ran"))
    }

    /// The open room of a document (panics if there is none).
    pub fn room(&self, doc: &str) -> oswx_server::room::RoomHandle {
        self.state
            .hub
            .loaded(doc)
            .expect("the document's room is open")
    }

    /// What the open room of a document is now.
    pub async fn room_status(&self, doc: &str) -> oswx_server::room::RoomStatus {
        self.room(doc).status().await.expect("the room answers")
    }

    /// Stops the server as a shutdown does (rooms close, the journal writes what it holds), without
    /// starting it again.
    pub async fn stop(&mut self) {
        self.shutdown().await;
    }
}
