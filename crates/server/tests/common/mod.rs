//! The test harness: a real server per test on a free port, mirroring the Python `conftest.py`.
#![allow(dead_code)] // each test file uses part of the harness

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use oswx_server::blobs::{Blobs, NullBlobs};
use oswx_server::hub::Hub;
use oswx_server::sessions::Sessions;
use oswx_server::store::{Id, MemoryStore, Store};
use oswx_server::{AppState, Settings};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// A session token as the server hands it out.
pub type Token = String;

/// A running server. Dropping it stops it.
pub struct TestServer {
    /// `http://127.0.0.1:<port>`, no trailing slash.
    pub base: String,
    pub state: AppState,
    /// The database and storage live here; kept across `restart`.
    dir: Arc<TempDir>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
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

/// Opens the database and storage the settings name. This is the one place that decides what a
/// test runs against (Task 2: SQLite at `settings.database_url`, or PostgreSQL with an emptied
/// schema when `OSWX_TEST_DATABASE_URL` is set).
async fn open_backing(_settings: &mut Settings) -> (Arc<dyn Store>, Arc<dyn Blobs>) {
    (Arc::new(MemoryStore::new()), Arc::new(NullBlobs))
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
        let (store, blobs) = open_backing(&mut s).await;
        let mut app_state = AppState::new(s, store, blobs);
        state(&mut app_state);
        Self::serve(app_state, dir).await
    }

    /// Stops the server and starts it again on the same database and storage, as a new process
    /// would: in-memory state (rooms, the session cache) starts empty; settings, mailer, extensions
    /// and clock are kept. The port changes; use the new `base`.
    pub async fn restart(mut self) -> Self {
        self.shutdown().await;
        let mut state = self.state.clone();
        state.hub = Hub::default();
        state.sessions = Sessions::default();
        Self::serve(state, self.dir.clone()).await
    }

    async fn serve(state: AppState, dir: Arc<TempDir>) -> Self {
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
