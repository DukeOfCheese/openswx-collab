//! The test harness: a real server per test on a free port, mirroring the Python `conftest.py`.
//!
//! Every test runs against SQLite in its own temporary directory, or, when
//! `OSWX_TEST_DATABASE_URL` names a PostgreSQL database (`tools/test_postgres_rust.py` starts one),
//! against PostgreSQL: each test server then gets a schema of its own in that database
//! ([`PgSchema`]), dropped when the test ends, so tests run side by side as they do on SQLite.
//!
//! Blobs and snapshots are kept in a folder of that temporary directory, or, when
//! `OSWX_TEST_STORAGE` is `s3`, in S3: an S3-compatible server of the test's own inside the test
//! process ([`FakeS3`]), or the real endpoint `OSWX_TEST_S3_URL` names (each test server under a
//! prefix of its own). Tests look at what is stored through [`TestServer::stored_under`].
#![allow(dead_code)] // each test file uses part of the harness

mod contract;
mod live;
mod providers;
mod s3;

#[allow(unused_imports)] // (only the storage tests run the storage contract)
pub use contract::*;

#[allow(unused_imports)] // (not every test file uses the live client)
pub use live::*;
#[allow(unused_imports)] // (only the sign-in tests use the fake providers)
pub use providers::*;
#[allow(unused_imports)] // (only the storage tests use the fake S3 themselves)
pub use s3::*;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use oswx_server::blobs::{Blobs, LocalBlobs, S3Blobs};
use oswx_server::hub::Hub;
use oswx_server::journal::{self, JournalWriter};
use oswx_server::mail::MemoryMailer;
use oswx_server::sessions::Sessions;
use oswx_server::store::postgres::PgConfig;
use oswx_server::store::{
    AssemblyRefRow, AssemblyUse, AuthoredChange, ChangeEventRow, ChangeRow, ChangeSize, Document,
    Folder, HistoryRow, Id, Invite, LoginCode, Member, Membership, NewChange, OAuthIdentity,
    OAuthState, Org, OrgMember, OrgSummary, PartRow, PasswordReset, PgStore, PinnedComponent,
    Project, ProjectMember, ProjectWithRole, RoomLease, SchemaStatus, Session, SnapshotRow,
    SqliteStore, Store, Tx, UndoTarget, User, Version, VersionWithAuthor,
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
    /// The fake Google and GitHub every server starts with (in `state.oauth`, unless the test put
    /// in something else).
    pub providers: Arc<FakeProviders>,
    /// The test database, for statements of the test's own ([`TestServer::sql`]).
    db: TestDb,
    /// What `state.store` is, so that `restart` can put the reopened database in its place.
    store_kind: StoreKind,
    /// Where blobs and snapshots are kept; kept across `restart`.
    storage: TestStorage,
    /// The database and storage live here; kept across `restart`.
    dir: Arc<TempDir>,
    /// On PostgreSQL: the test's schema, kept across `restart` and dropped with the last server on
    /// it (declared after `db`, whose connections `Drop` closes first).
    schema: Option<Arc<PgSchema>>,
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

/// The environment variable that names the PostgreSQL database the tests run against.
const PG_ENV: &str = "OSWX_TEST_DATABASE_URL";
/// How many connections one test server's PostgreSQL pool for requests holds at most (two for
/// transactions, two for reads; the journal has its default two besides): a few hundred tests,
/// many at once, share one database server's `max_connections`.
const PG_POOL: u32 = 4;

/// Whether the tests run against PostgreSQL (`OSWX_TEST_DATABASE_URL` is set) rather than SQLite.
pub fn on_postgres() -> bool {
    std::env::var_os(PG_ENV).is_some_and(|url| !url.is_empty())
}

/// `bytes` as a literal in a statement of the test's own ([`TestServer::sql`]), in the words of
/// the database the tests run against.
pub fn blob_literal(bytes: &[u8]) -> String {
    if on_postgres() {
        format!("decode('{}', 'hex')", hex::encode(bytes))
    } else {
        format!("X'{}'", hex::encode(bytes))
    }
}

/// A schema of its own in the test PostgreSQL database: created empty, dropped (with everything
/// in it) when the last handle goes.
pub struct PgSchema {
    /// The database as `OSWX_TEST_DATABASE_URL` names it.
    admin: String,
    name: String,
    /// The database URL whose connections work in this schema.
    pub url: String,
}

impl PgSchema {
    pub async fn create() -> Self {
        use sqlx::Connection as _;
        let given = std::env::var(PG_ENV).expect("OSWX_TEST_DATABASE_URL is set");
        let admin = match oswx_server::config::parse_database_url(&given) {
            Ok(oswx_server::config::Database::Postgres(url)) => url,
            _ => panic!("OSWX_TEST_DATABASE_URL must be a PostgreSQL URL"),
        };
        let name = format!("t_{}", oswx_server::store::new_id());
        let mut conn = sqlx::PgConnection::connect(&admin)
            .await
            .expect("the test PostgreSQL database answers");
        sqlx::query(&format!("CREATE SCHEMA {name}"))
            .execute(&mut conn)
            .await
            .expect("the test's schema is created");
        let _ = conn.close().await;
        let mut url = url::Url::parse(&admin).expect("a URL");
        url.query_pairs_mut()
            .append_pair("options", &format!("-c search_path={name}"));
        Self {
            admin,
            name,
            url: url.into(),
        }
    }
}

impl Drop for PgSchema {
    fn drop(&mut self) {
        // (`Drop` cannot await, and the test's runtime is busy running this.)
        std::thread::scope(|s| {
            s.spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("a runtime to drop the schema");
                let dropping = async {
                    use sqlx::Connection as _;
                    let mut conn = sqlx::PgConnection::connect(&self.admin).await?;
                    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.name))
                        .execute(&mut conn)
                        .await?;
                    conn.close().await
                };
                let dropped = rt.block_on(async {
                    tokio::time::timeout(Duration::from_secs(20), dropping).await
                });
                if !matches!(dropped, Ok(Ok(()))) {
                    eprintln!("the test schema {} was not dropped", self.name);
                }
            });
        });
    }
}

/// The database a test server runs on, as the test itself reaches it.
#[derive(Clone)]
pub enum TestDb {
    Sqlite(SqliteStore),
    Postgres(PgStore),
}

impl TestDb {
    /// The database as the server uses it.
    pub fn store(&self) -> Arc<dyn Store> {
        match self {
            Self::Sqlite(db) => Arc::new(db.clone()),
            Self::Postgres(db) => Arc::new(db.clone()),
        }
    }

    pub async fn health(&self) -> anyhow::Result<()> {
        match self {
            Self::Sqlite(db) => db.health().await,
            Self::Postgres(db) => db.health().await,
        }
    }

    pub async fn close(&self) {
        match self {
            Self::Sqlite(db) => db.close().await,
            Self::Postgres(db) => db.close().await,
        }
    }

    /// Runs one statement in a transaction of its own.
    pub async fn execute(&self, statement: &str) -> anyhow::Result<()> {
        match self {
            Self::Sqlite(db) => {
                let mut tx = db.begin_tx().await?;
                sqlx::query(statement).execute(tx.conn()).await?;
                tx.commit_tx().await
            }
            Self::Postgres(db) => {
                let mut tx = db.begin_tx().await?;
                sqlx::query(statement).execute(tx.conn()).await?;
                tx.commit_tx().await
            }
        }
    }

    /// The one value a query answers.
    pub async fn scalar<T>(&self, query: &str) -> T
    where
        T: Send
            + Unpin
            + for<'r> sqlx::Decode<'r, sqlx::Sqlite>
            + sqlx::Type<sqlx::Sqlite>
            + for<'r> sqlx::Decode<'r, sqlx::Postgres>
            + sqlx::Type<sqlx::Postgres>,
    {
        match self {
            Self::Sqlite(db) => sqlx::query_scalar(query)
                .fetch_one(&mut *db.reader().await.expect("a connection"))
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}")),
            Self::Postgres(db) => sqlx::query_scalar(query)
                .fetch_one(&mut *db.reader().await.expect("a connection"))
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}")),
        }
    }
}

/// Opens the database the settings name. This is the one place that decides what a test runs
/// against: SQLite at `settings.database_url`, or the PostgreSQL schema it names.
async fn open_store(settings: &Settings) -> TestDb {
    match settings.database().expect("a database URL") {
        oswx_server::config::Database::Sqlite(_) => TestDb::Sqlite(
            SqliteStore::open(&settings.database_url, settings.auto_migrate)
                .await
                .expect("the test database opens"),
        ),
        oswx_server::config::Database::Postgres(_) => TestDb::Postgres(
            PgStore::open(
                &settings.database_url,
                settings.auto_migrate,
                PgConfig::from_settings(settings),
            )
            .await
            .expect("the test database opens"),
        ),
    }
}

/// The environment variable that says where the tests keep blobs and snapshots: `s3` for S3
/// ([`FakeS3`], or the real endpoint `OSWX_TEST_S3_URL` names); anything else, or unset, for a
/// folder.
const STORAGE_ENV: &str = "OSWX_TEST_STORAGE";

/// Whether the tests keep blobs and snapshots in S3 (`OSWX_TEST_STORAGE=s3`) rather than a folder.
pub fn on_s3() -> bool {
    std::env::var(STORAGE_ENV).is_ok_and(|v| v.trim().eq_ignore_ascii_case("s3"))
}

/// Where a test server keeps blobs and snapshots, as the test itself reaches it.
#[derive(Clone)]
pub enum TestStorage {
    /// A folder in the test's temporary directory.
    Local(PathBuf),
    /// A bucket: of the fake S3 server that is kept alive here, or of a real endpoint.
    S3 {
        blobs: Arc<S3Blobs>,
        fake: Option<Arc<FakeS3>>,
        /// How `blobs` reaches its bucket (to ask a real endpoint for its unfinished uploads).
        config: Arc<oswx_server::blobs::S3Config>,
    },
}

impl TestStorage {
    /// Every key in storage that starts with `prefix`, sorted. In a folder that includes temporary
    /// files (`.tmp-...`); on S3, one `.unfinished-upload/<n>` for what is left of multipart
    /// uploads that were neither completed nor aborted (when `prefix` is empty): on the fake, one
    /// for every part it holds; on a real endpoint, one for every upload under the storage's own
    /// key prefix that `ListMultipartUploads` answers.
    pub async fn stored_under(&self, prefix: &str) -> Vec<String> {
        let mut keys = match self {
            Self::Local(root) => {
                let mut keys = Vec::new();
                let mut todo = vec![root.clone()];
                while let Some(dir) = todo.pop() {
                    for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            todo.push(path);
                        } else if let Ok(inside) = path.strip_prefix(root) {
                            let parts: Vec<_> = inside
                                .components()
                                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                                .collect();
                            keys.push(parts.join("/"));
                        }
                    }
                }
                keys
            }
            Self::S3 {
                blobs,
                fake,
                config,
            } => {
                let mut keys = blobs.keys().await.expect("the test bucket lists its keys");
                let parts = match fake {
                    Some(fake) => fake.parts_held(),
                    None => unfinished_uploads(config)
                        .await
                        .expect("the test bucket lists its unfinished uploads"),
                };
                keys.extend((0..parts).map(|n| format!(".unfinished-upload/{n}")));
                keys
            }
        };
        keys.retain(|k| k.starts_with(prefix));
        keys.sort();
        keys
    }
}

/// Opens the blob storage the tests run against. This is the one place that decides it: the folder
/// the settings name, in the test's temporary directory, or S3 ([`on_s3`]).
async fn open_blobs(settings: &Settings, dir: &Path) -> (Arc<dyn Blobs>, TestStorage) {
    if !on_s3() {
        let local = LocalBlobs::new(&settings.storage_url).expect("the test storage folder");
        let storage = TestStorage::Local(local.root().to_path_buf());
        return (Arc::new(local), storage);
    }
    let (config, fake) = match real_s3() {
        Some(config) => (config, None),
        None => {
            let fake = Arc::new(FakeS3::start(dir).await);
            (fake.config(""), Some(fake))
        }
    };
    let blobs = Arc::new(
        S3Blobs::open(config.clone())
            .await
            .expect("the test bucket works"),
    );
    let storage = TestStorage::S3 {
        blobs: blobs.clone(),
        fake,
        config: Arc::new(config),
    };
    (blobs, storage)
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
        let schema = if on_postgres() {
            let schema = PgSchema::create().await;
            s.database_url.clone_from(&schema.url);
            s.database_pool = PG_POOL;
            Some(Arc::new(schema))
        } else {
            None
        };
        settings(&mut s);
        let db = open_store(&s).await;
        let (blobs, storage) = open_blobs(&s, dir.path()).await;
        let plain: Arc<dyn Store> = db.store();
        let mut app_state = AppState::new(s, plain.clone(), blobs);
        let providers = Arc::new(FakeProviders::default());
        app_state.oauth = providers.clone();
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
        Self::serve(app_state, providers, db, store_kind, storage, dir, schema).await
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
        self.db
            .execute(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }

    /// The one value a query of the test's own answers.
    pub async fn scalar<T>(&self, query: &str) -> T
    where
        T: Send
            + Unpin
            + for<'r> sqlx::Decode<'r, sqlx::Sqlite>
            + sqlx::Type<sqlx::Sqlite>
            + for<'r> sqlx::Decode<'r, sqlx::Postgres>
            + sqlx::Type<sqlx::Postgres>,
    {
        self.db.scalar(query).await
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
        // (A new process has a purger of its own, which starts with what is still marked.)
        state.purger = oswx_server::purge::Purger::new(
            self.state.purger.chunk_rows(),
            oswx_server::purge::RETRY,
        );
        state.org_queue = oswx_server::org_queue::OrgQueue::default();
        let db = open_store(&state.settings).await;
        let reopened: Arc<dyn Store> = db.store();
        match &self.store_kind {
            StoreKind::Plain => state.store = reopened,
            StoreKind::Flaky(flaky) => flaky.set_inner(reopened),
            StoreKind::Custom => panic!("restart() cannot reopen a store the test put in itself"),
        }
        state.journal = new_journal(&state.store);
        state.node_id = oswx_server::store::new_id();
        let providers = self.providers.clone();
        Self::serve(
            state,
            providers,
            db,
            self.store_kind.clone(),
            self.storage.clone(),
            self.dir.clone(),
            self.schema.clone(),
        )
        .await
    }

    async fn serve(
        state: AppState,
        providers: Arc<FakeProviders>,
        db: TestDb,
        store_kind: StoreKind,
        storage: TestStorage,
        dir: Arc<TempDir>,
        schema: Option<Arc<PgSchema>>,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a free port");
        let base = format!("http://{}", listener.local_addr().expect("an address"));
        let (stop, stopped) = oneshot::channel::<()>();
        let serving = state.clone();
        let task = tokio::spawn(async move {
            // As the binary serves: the same loop, limits and orderly stop.
            oswx_server::serve::serve(listener, serving, async {
                let _ = stopped.await;
            })
            .await;
        });
        Self {
            base,
            state,
            providers,
            db,
            store_kind,
            storage,
            dir,
            schema,
            stop: Some(stop),
            task: Some(task),
        }
    }

    /// Stops the server as a signal stops the binary: it stops accepting and shuts the state down
    /// (the hub, then the journal, which writes what it has queued), within the settings'
    /// `shutdown_timeout`. Whatever that left undone is then waited for without a limit.
    async fn shutdown(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let abort = task.abort_handle();
            if tokio::time::timeout(Duration::from_secs(30), task)
                .await
                .is_err()
            {
                abort.abort();
            }
        }
        self.state.shutdown().await;
    }

    /// Waits until everything that was deleted has been removed from the database and storage
    /// (the purger has nothing left to do).
    pub async fn purged(&self) {
        tokio::time::timeout(Duration::from_secs(60), self.state.purger.idle())
            .await
            .expect("the purger finishes");
    }

    /// The test database as this server opened it.
    pub fn db(&self) -> &TestDb {
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
    fail_project_reads: AtomicBool,
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
    /// Set: the next change-id lookup only parks at [`FlakyStore::next_lookup_gate`].
    hold_next_lookup: AtomicBool,
    preloads: AtomicUsize,
    preload_delay: Mutex<Duration>,
    commit_delay: Arc<Mutex<Duration>>,
    /// Shut: every top-level `commit` parks here (before it commits) until opened.
    pub commit_gate: Arc<oswx_server::room::Gate>,
    /// Shut: every `set_org_name` parks here (a request then holds its organisation's row lock).
    pub org_gate: Arc<oswx_server::room::Gate>,
    /// Shut: every `insert_user` parks here (a registration has then counted the accounts).
    pub user_gate: Arc<oswx_server::room::Gate>,
    /// Shut: every `erase_user` parks here (an account's deletion then holds its locks).
    pub erase_gate: Arc<oswx_server::room::Gate>,
    /// Set: every `insert_oauth_identity` inserts its row twice, so that the database refuses the
    /// second as a duplicate key (a sign-in that meets a taken key on every try).
    duplicate_identities: Arc<AtomicBool>,
    /// Shut: every `mark_org_deleted` parks here once it has marked (a deletion then holds the
    /// rows of the organisation's documents).
    pub mark_gate: Arc<oswx_server::room::Gate>,
    /// Shut: every `set_document_head` parks here (the journal has then locked and written what a
    /// batch writes, and not yet committed).
    pub head_gate: Arc<oswx_server::room::Gate>,
    /// How long a top-level `commit` is waited for before an error is reported in its place
    /// (the commit itself goes on in the database).
    commit_patience: Arc<Mutex<Option<Duration>>>,
    /// Shut: every [`Store::parts_by_ids`] parks here, after it has read its answer, until opened.
    pub parts_gate: Arc<oswx_server::room::Gate>,
    /// Shut: reads of a change by seq and of an undo target park here until opened.
    pub history_gate: Arc<oswx_server::room::Gate>,
    /// Shut: every [`Store::deleted_orgs`] (the purger's list of organisations) parks here.
    pub list_orgs_gate: Arc<oswx_server::room::Gate>,
    /// Shut: every [`Store::change_seq_by_client`] (a room's change-id lookup) parks here, once
    /// counted.
    pub lookup_gate: Arc<oswx_server::room::Gate>,
    /// Where the one change-id lookup [`FlakyStore::hold_next_lookup`] picked parks until opened.
    pub next_lookup_gate: Arc<oswx_server::room::Gate>,
    /// Shut: every [`Store::begin_journal`] (a journal batch, a lease) parks here before it opens
    /// its transaction. (REST transactions do not pass it.)
    pub journal_gate: Arc<oswx_server::room::Gate>,
    /// How many times `replace_assembly_refs` ran (in a transaction or savepoint).
    ref_writes: Arc<AtomicUsize>,
    fail_part_reads: AtomicBool,
    fail_deleted_projects: AtomicBool,
    deleted_projects_calls: AtomicUsize,
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
            fail_project_reads: AtomicBool::new(false),
            panic_on_begin: AtomicBool::new(false),
            hang_commit: Arc::new(AtomicBool::new(false)),
            begins: AtomicUsize::new(0),
            refuse_change: Arc::new(Mutex::new(None)),
            refuse_server_change: Arc::new(AtomicBool::new(false)),
            change_lookups: AtomicUsize::new(0),
            lookup_delay: Mutex::new(Duration::ZERO),
            duplicate_inserts: Arc::new(AtomicUsize::new(0)),
            lookup_log: Mutex::new(Vec::new()),
            hold_next_lookup: AtomicBool::new(false),
            preloads: AtomicUsize::new(0),
            preload_delay: Mutex::new(Duration::ZERO),
            commit_delay: Arc::new(Mutex::new(Duration::ZERO)),
            commit_gate: Arc::default(),
            head_gate: Arc::default(),
            user_gate: Arc::default(),
            erase_gate: Arc::default(),
            duplicate_identities: Arc::default(),
            mark_gate: Arc::default(),
            org_gate: Arc::default(),
            commit_patience: Arc::default(),
            parts_gate: Arc::default(),
            history_gate: Arc::default(),
            list_orgs_gate: Arc::default(),
            lookup_gate: Arc::default(),
            next_lookup_gate: Arc::default(),
            journal_gate: Arc::default(),
            ref_writes: Arc::new(AtomicUsize::new(0)),
            fail_part_reads: AtomicBool::new(false),
            fail_deleted_projects: AtomicBool::new(false),
            deleted_projects_calls: AtomicUsize::new(0),
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

    /// Makes [`Store::deleted_projects`] fail at once (`true`) or delegate again (`false`).
    pub fn fail_deleted_projects(&self, fail: bool) {
        self.fail_deleted_projects.store(fail, Ordering::SeqCst);
    }

    /// How many times [`Store::deleted_projects`] was asked (the purger's work list).
    pub fn deleted_projects_calls(&self) -> usize {
        self.deleted_projects_calls.load(Ordering::SeqCst)
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

    /// Makes the next [`Store::change_seq_by_client`] (only) park at
    /// [`FlakyStore::next_lookup_gate`], shut now, until the test opens it.
    pub fn hold_next_lookup(&self) {
        self.next_lookup_gate.shut();
        self.hold_next_lookup.store(true, Ordering::SeqCst);
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

    /// Stops waiting for every transaction's `commit` after `patience` and reports an error in
    /// its place, as a request whose database answer did not come in time: the commit was sent,
    /// and may still take effect afterwards. `None`: wait for it again.
    pub fn give_up_on_commits_after(&self, patience: Option<Duration>) {
        *self
            .commit_patience
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = patience;
    }

    /// What every `begin` (a request's or the journal's) does first.
    fn before_begin(&self) {
        if self.panic_on_begin.load(Ordering::SeqCst) {
            panic!("the flaky store was told to panic in begin");
        }
        self.begins.fetch_add(1, Ordering::SeqCst);
    }

    /// A real transaction wrapped so that it fails, waits and counts as this store was told.
    fn flaky(&self, inner: Box<dyn Tx>) -> Box<dyn Tx> {
        Box::new(FlakyTx {
            inner,
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
            head_gate: self.head_gate.clone(),
            user_gate: self.user_gate.clone(),
            erase_gate: self.erase_gate.clone(),
            duplicate_identities: self.duplicate_identities.clone(),
            mark_gate: self.mark_gate.clone(),
            org_gate: self.org_gate.clone(),
            commit_patience: self.commit_patience.clone(),
            ref_writes: self.ref_writes.clone(),
        })
    }

    /// Makes every `insert_oauth_identity` insert its row twice (`true`): the database refuses
    /// the second as a duplicate key.
    pub fn duplicate_identity_inserts(&self, on: bool) {
        self.duplicate_identities.store(on, Ordering::SeqCst);
    }

    /// How many transactions [`Store::begin`] and [`Store::begin_journal`] have opened so far.
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

    /// Makes [`Store::project_by_id`] fail (`true`) or delegate again (`false`).
    pub fn fail_project_reads(&self, fail: bool) {
        self.fail_project_reads.store(fail, Ordering::SeqCst);
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
        self.before_begin();
        Ok(self.flaky(self.inner().begin().await?))
    }

    async fn begin_journal(&self) -> anyhow::Result<Box<dyn Tx>> {
        self.journal_gate.pass().await;
        self.before_begin();
        Ok(self.flaky(self.inner().begin_journal().await?))
    }

    async fn deleted_projects(&self, after: &str, limit: i64) -> anyhow::Result<Vec<Id>> {
        self.deleted_projects_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_deleted_projects.load(Ordering::SeqCst) {
            anyhow::bail!("the flaky store was told to fail listing deleted projects");
        }
        self.inner().deleted_projects(after, limit).await
    }

    async fn deleted_orgs(&self, after: &str, limit: i64) -> anyhow::Result<Vec<Id>> {
        self.list_orgs_gate.pass().await;
        self.inner().deleted_orgs(after, limit).await
    }

    async fn documents_of_deleted_project(
        &self,
        project_id: &str,
        after: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<Id>> {
        self.inner()
            .documents_of_deleted_project(project_id, after, limit)
            .await
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

    async fn oauth_identity(
        &self,
        provider: &str,
        subject: &str,
    ) -> anyhow::Result<Option<OAuthIdentity>> {
        self.inner().oauth_identity(provider, subject).await
    }

    async fn user_identities(&self, user_id: &str) -> anyhow::Result<Vec<OAuthIdentity>> {
        self.inner().user_identities(user_id).await
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
        if self.fail_project_reads.load(Ordering::SeqCst) {
            anyhow::bail!("the flaky store was told to fail project reads");
        }
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
        self.history_gate.pass().await;
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
        if self.hold_next_lookup.swap(false, Ordering::SeqCst) {
            self.next_lookup_gate.pass().await;
        }
        let delay = *self
            .lookup_delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        pause(delay).await;
        self.lookup_gate.pass().await;
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
        self.history_gate.pass().await;
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

    async fn change_sizes_after(
        &self,
        document_id: &str,
        after: i64,
        upto: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ChangeSize>> {
        self.inner()
            .change_sizes_after(document_id, after, upto, limit)
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

    async fn user_memberships(&self, user_id: &str) -> anyhow::Result<Vec<Membership>> {
        self.inner().user_memberships(user_id).await
    }

    async fn user_project_roles(&self, user_id: &str) -> anyhow::Result<Vec<ProjectWithRole>> {
        self.inner().user_project_roles(user_id).await
    }

    async fn user_sessions(&self, user_id: &str) -> anyhow::Result<Vec<Session>> {
        self.inner().user_sessions(user_id).await
    }

    async fn changes_by_author(
        &self,
        user_id: &str,
        after_document: &str,
        after_seq: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<AuthoredChange>> {
        self.inner()
            .changes_by_author(user_id, after_document, after_seq, limit)
            .await
    }

    async fn versions_by_author(
        &self,
        user_id: &str,
        after: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<Version>> {
        self.inner().versions_by_author(user_id, after, limit).await
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
    head_gate: Arc<oswx_server::room::Gate>,
    user_gate: Arc<oswx_server::room::Gate>,
    erase_gate: Arc<oswx_server::room::Gate>,
    duplicate_identities: Arc<AtomicBool>,
    mark_gate: Arc<oswx_server::room::Gate>,
    org_gate: Arc<oswx_server::room::Gate>,
    commit_patience: Arc<Mutex<Option<Duration>>>,
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
        let patience = *self
            .commit_patience
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match patience {
            None => self.inner.commit().await?,
            Some(patience) => tokio::time::timeout(patience, self.inner.commit())
                .await
                .map_err(|_| anyhow::anyhow!("the flaky store stopped waiting for a commit"))??,
        }
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
            head_gate: self.head_gate.clone(),
            user_gate: self.user_gate.clone(),
            erase_gate: self.erase_gate.clone(),
            duplicate_identities: self.duplicate_identities.clone(),
            mark_gate: self.mark_gate.clone(),
            org_gate: self.org_gate.clone(),
            commit_patience: self.commit_patience.clone(),
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
        self.user_gate.pass().await;
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

    async fn set_user_admin(&mut self, id: &str, admin: bool) -> anyhow::Result<()> {
        self.inner.set_user_admin(id, admin).await
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

    async fn oauth_identity(
        &mut self,
        provider: &str,
        subject: &str,
    ) -> anyhow::Result<Option<OAuthIdentity>> {
        self.inner.oauth_identity(provider, subject).await
    }

    async fn user_identities(&mut self, user_id: &str) -> anyhow::Result<Vec<OAuthIdentity>> {
        self.inner.user_identities(user_id).await
    }

    async fn insert_oauth_identity(&mut self, identity: &OAuthIdentity) -> anyhow::Result<()> {
        self.inner.insert_oauth_identity(identity).await?;
        if self.duplicate_identities.load(Ordering::SeqCst) {
            self.inner.insert_oauth_identity(identity).await?;
        }
        Ok(())
    }

    async fn delete_user_identities(
        &mut self,
        user_id: &str,
        provider: &str,
    ) -> anyhow::Result<()> {
        self.inner.delete_user_identities(user_id, provider).await
    }

    async fn insert_oauth_state(&mut self, state: &OAuthState) -> anyhow::Result<()> {
        self.inner.insert_oauth_state(state).await
    }

    async fn take_oauth_state(&mut self, state_hash: &str) -> anyhow::Result<Option<OAuthState>> {
        self.inner.take_oauth_state(state_hash).await
    }

    async fn delete_expired_oauth_states(&mut self, now: f64) -> anyhow::Result<()> {
        self.inner.delete_expired_oauth_states(now).await
    }

    async fn insert_login_code(&mut self, code: &LoginCode) -> anyhow::Result<()> {
        self.inner.insert_login_code(code).await
    }

    async fn take_login_code(&mut self, code_hash: &str) -> anyhow::Result<Option<LoginCode>> {
        self.inner.take_login_code(code_hash).await
    }

    async fn delete_expired_login_codes(&mut self, now: f64) -> anyhow::Result<()> {
        self.inner.delete_expired_login_codes(now).await
    }

    async fn insert_org(&mut self, org: &Org) -> anyhow::Result<()> {
        self.inner.insert_org(org).await
    }

    async fn set_org_name(&mut self, id: &str, name: &str) -> anyhow::Result<()> {
        self.org_gate.pass().await;
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

    async fn open_invite_count(&mut self, org_id: &str, now: f64) -> anyhow::Result<u64> {
        self.inner.open_invite_count(org_id, now).await
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

    async fn mark_project_deleted(&mut self, id: &str, now: f64) -> anyhow::Result<bool> {
        self.inner.mark_project_deleted(id, now).await
    }

    async fn mark_org_deleted(&mut self, id: &str, now: f64) -> anyhow::Result<bool> {
        let marked = self.inner.mark_org_deleted(id, now).await?;
        self.mark_gate.pass().await;
        Ok(marked)
    }

    async fn purge_document_rows(&mut self, document_id: &str, limit: i64) -> anyhow::Result<u64> {
        self.inner.purge_document_rows(document_id, limit).await
    }

    async fn purge_document(&mut self, id: &str) -> anyhow::Result<bool> {
        self.inner.purge_document(id).await
    }

    async fn purge_project_rows(&mut self, project_id: &str, limit: i64) -> anyhow::Result<u64> {
        self.inner.purge_project_rows(project_id, limit).await
    }

    async fn purge_project(&mut self, id: &str) -> anyhow::Result<bool> {
        self.inner.purge_project(id).await
    }

    async fn purge_org(&mut self, id: &str) -> anyhow::Result<bool> {
        self.inner.purge_org(id).await
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

    async fn insert_changes(&mut self, changes: &[NewChange<'_>]) -> anyhow::Result<u64> {
        // A change this store was told to refuse is refused by `insert_change`: while one is
        // waiting, nothing goes in together (as when a row was left out), and the journal writes
        // the batch again one record at a time.
        let waiting = self
            .refuse_change
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
            || self.refuse_server_change.load(Ordering::SeqCst);
        if waiting {
            return Ok(0);
        }
        self.inner.insert_changes(changes).await
    }

    async fn set_document_head(
        &mut self,
        id: &str,
        head_seq: i64,
        updated_at: f64,
    ) -> anyhow::Result<()> {
        self.head_gate.pass().await;
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

    async fn lock_accounts(&mut self) -> anyhow::Result<()> {
        self.inner.lock_accounts().await
    }

    async fn lock_user(&mut self, id: &str) -> anyhow::Result<bool> {
        self.inner.lock_user(id).await
    }

    async fn lock_org(&mut self, id: &str) -> anyhow::Result<bool> {
        self.inner.lock_org(id).await
    }

    async fn lock_documents(&mut self, ids: &[Id]) -> anyhow::Result<()> {
        self.inner.lock_documents(ids).await
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

    async fn insert_room_lease(&mut self, lease: &RoomLease) -> anyhow::Result<bool> {
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

    async fn orgs_owned_alone(&mut self, user_id: &str) -> anyhow::Result<Vec<Org>> {
        self.inner.orgs_owned_alone(user_id).await
    }

    async fn active_admin_count(&mut self) -> anyhow::Result<u64> {
        self.inner.active_admin_count().await
    }

    async fn other_active_user_count(&mut self, user_id: &str) -> anyhow::Result<u64> {
        self.inner.other_active_user_count(user_id).await
    }

    async fn user_org_ids(&mut self, user_id: &str) -> anyhow::Result<Vec<Id>> {
        self.inner.user_org_ids(user_id).await
    }

    async fn erase_user(
        &mut self,
        id: &str,
        email: &str,
        name: &str,
        now: f64,
    ) -> anyhow::Result<()> {
        self.erase_gate.pass().await;
        self.inner.erase_user(id, email, name, now).await
    }

    async fn org_document_ids(&mut self, org_id: &str) -> anyhow::Result<Vec<Id>> {
        self.inner.org_document_ids(org_id).await
    }

    async fn org_member_ids(&mut self, org_id: &str) -> anyhow::Result<Vec<Id>> {
        self.inner.org_member_ids(org_id).await
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
    fail_delete: AtomicBool,
    /// The one prefix whose removal fails, whatever `fail_delete` says.
    fail_delete_of: Mutex<Option<String>>,
    /// How many removals of a prefix were asked for.
    deletes: AtomicUsize,
    /// Shut: every removal of a prefix parks here, inside the storage call.
    pub delete_gate: oswx_server::room::Gate,
    /// Shut: every `exists` parks here, once counted (it is then running).
    pub exists_gate: oswx_server::room::Gate,
    /// Shut: every `put` (snapshots) parks here, once counted.
    pub put_gate: oswx_server::room::Gate,
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
            fail_delete: AtomicBool::new(false),
            fail_delete_of: Mutex::new(None),
            deletes: AtomicUsize::new(0),
            delete_gate: oswx_server::room::Gate::default(),
            exists_gate: oswx_server::room::Gate::default(),
            put_gate: oswx_server::room::Gate::default(),
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
    /// Makes every removal of a prefix fail (`true`), as a storage that is away; or work again.
    pub fn fail_delete(&self, fail: bool) {
        self.fail_delete.store(fail, Ordering::SeqCst);
    }

    /// Makes the removal of this one prefix fail for good (`None`: of none).
    pub fn fail_delete_of(&self, prefix: Option<&str>) {
        *self
            .fail_delete_of
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = prefix.map(str::to_owned);
    }

    /// How many removals of a prefix were asked for so far.
    pub fn deletes(&self) -> usize {
        self.deletes.load(Ordering::SeqCst)
    }

    pub fn fail_put(&self, fail: bool) {
        self.fail_put.store(fail, Ordering::SeqCst);
    }

    /// How many `exists` calls were made.
    pub fn exists_calls(&self) -> usize {
        self.exists_calls.load(Ordering::SeqCst)
    }

    /// How many `exists` calls are running now.
    pub fn exists_running(&self) -> usize {
        self.exists_now.load(Ordering::SeqCst)
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
        self.exists_gate.pass().await;
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
        self.put_gate.pass().await;
        if self.fail_put.load(Ordering::SeqCst) {
            self.put_failures.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("the test storage was told to fail put");
        }
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<bytes::Bytes>> {
        self.inner.get(key).await
    }

    fn upload_memory(&self) -> u64 {
        self.inner.upload_memory()
    }

    async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.delete_gate.pass().await;
        let this_one = self
            .fail_delete_of
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_deref()
            == Some(prefix);
        if this_one || self.fail_delete.load(Ordering::SeqCst) {
            anyhow::bail!("the test storage was told to fail removals");
        }
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

    /// Every key in the server's storage that starts with `prefix`, sorted
    /// ([`TestStorage::stored_under`]).
    pub async fn stored_under(&self, prefix: &str) -> Vec<String> {
        self.storage.stored_under(prefix).await
    }

    /// The server's storage as the S3 storage it is, if it is ([`on_s3`]).
    pub fn s3_blobs(&self) -> Option<Arc<S3Blobs>> {
        match &self.storage {
            TestStorage::S3 { blobs, .. } => Some(blobs.clone()),
            TestStorage::Local(_) => None,
        }
    }

    /// The fake S3 server this test server keeps its files in, if it does ([`on_s3`] and no real
    /// endpoint).
    pub fn fake_s3(&self) -> Option<Arc<FakeS3>> {
        match &self.storage {
            TestStorage::S3 { fake, .. } => fake.clone(),
            TestStorage::Local(_) => None,
        }
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
