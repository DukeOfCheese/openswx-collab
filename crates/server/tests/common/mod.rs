//! The test harness: a real server per test on a free port, mirroring the Python `conftest.py`.
#![allow(dead_code)] // each test file uses part of the harness

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use oswx_server::blobs::{Blobs, LocalBlobs};
use oswx_server::hub::Hub;
use oswx_server::mail::MemoryMailer;
use oswx_server::sessions::Sessions;
use oswx_server::store::{
    Id, Invite, Member, Org, OrgMember, OrgSummary, PasswordReset, Project, ProjectMember,
    ProjectWithRole, SchemaStatus, Session, SqliteStore, Store, Tx, User,
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
}

/// A server with a [`World`] on it.
pub async fn world() -> (TestServer, World) {
    let server = TestServer::start().await;
    let world = World::new(&server).await.expect("the world is made");
    (server, world)
}

/// A v2 WebSocket client built on `oswx_core::wire`.
pub struct Live {}

impl Live {
    /// Not yet available: the WebSocket arrives with Task 9, which gives `Live` its body.
    pub async fn connect(_server: &TestServer, _token: &str, _doc: &Id) -> anyhow::Result<Self> {
        anyhow::bail!("Live needs the document WebSocket; Task 9 provides it")
    }
}
