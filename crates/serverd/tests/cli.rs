//! The program itself, run as an operator runs it: each command against a database in a
//! temporary folder, and `serve` started, used and stopped by a signal.
//!
//! The stop is a real one: SIGTERM and SIGINT on Unix (`kill`); on Windows a console control
//! event, Ctrl-Break, sent by a small PowerShell helper to the server's own hidden console. (Not
//! Ctrl-C: a process started in a process group of its own, which is what lets the event be sent
//! to it alone, has Ctrl-C turned off by Windows. The server treats the two alike.)

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use oswx_core::wire::{self, ClientMsg, PROTOCOL_VERSION, ServerMsg};
use oswx_core::{Change, Op, Value as DocValue};
use oswx_server::admin::Db;
use oswx_server::content::plain_from_json;
use oswx_server::{Settings, VERSION};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio_tungstenite::tungstenite::Message;

const BIN: &str = env!("CARGO_BIN_EXE_openswx-collab-server");
const PASSWORD: &str = "a long enough password";
const WAIT: Duration = Duration::from_secs(30);

fn slashes(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// The program with `args`, with no setting but a database and a storage folder in `dir`, and
/// then `env`.
fn program(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> Command {
    let mut command = Command::new(BIN);
    command.args(args).current_dir(dir).stdin(Stdio::null());
    // Whatever the machine running the tests has set is not this test's business.
    for (name, _) in std::env::vars_os() {
        let name = name.to_string_lossy().into_owned();
        if name.starts_with("OSWX_") || name.starts_with("AWS_") {
            command.env_remove(name);
        }
    }
    command
        .env(
            "OSWX_COLLAB_DATABASE_URL",
            format!("sqlite://{}/collab.db", slashes(dir)),
        )
        .env(
            "OSWX_COLLAB_STORAGE_URL",
            format!("{}/storage", slashes(dir)),
        );
    for (name, value) in env {
        command.env(name, value);
    }
    command
}

struct Ran {
    code: Option<i32>,
    out: String,
    err: String,
}

/// Runs the program to its end, which must come within a minute (a command that should have
/// refused its settings and serves instead is killed, and fails the test, rather than hanging
/// it). Answers its exit code and what it wrote.
#[track_caller]
fn run(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> Ran {
    let scratch = TempDir::new().unwrap();
    let (out, err) = (scratch.path().join("out"), scratch.path().join("err"));
    let mut child = program(dir, args, env)
        .stdout(std::fs::File::create(&out).unwrap())
        .stderr(std::fs::File::create(&err).unwrap())
        .spawn()
        .expect("the program runs");
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > Duration::from_secs(60) {
            let _ = child.kill();
            let _ = child.wait();
            let wrote = std::fs::read_to_string(&err).unwrap_or_default();
            panic!("{args:?} with {env:?} did not end: {wrote}");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let read = |path: &Path| {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        text.trim_end().replace("\r\n", "\n")
    };
    Ran {
        code: status.code(),
        out: read(&out),
        err: read(&err),
    }
}

/// Runs a command that must succeed; answers what it printed.
#[track_caller]
fn ok(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> String {
    let ran = run(dir, args, env);
    assert_eq!(ran.code, Some(0), "{args:?}: {}", ran.err);
    assert_eq!(ran.err, "", "{args:?} wrote to standard error");
    ran.out
}

fn settings(dir: &Path) -> Settings {
    Settings {
        database_url: format!("sqlite://{}/collab.db", slashes(dir)),
        storage_url: format!("{}/storage", slashes(dir)),
        ..Settings::default()
    }
}

// ------------------------------------------------------------------ the commands

#[test]
fn help_names_the_commands_and_the_settings_and_version_the_version() {
    let dir = TempDir::new().unwrap();
    let help = ok(dir.path(), &["--help"], &[]);
    for word in [
        "serve",
        "migrate",
        "db-status",
        "create-admin",
        "reset-password",
        "OSWX_COLLAB_DATABASE_URL",
        "OSWX_COLLAB_DATABASE_POOL",
        "OSWX_COLLAB_DATABASE_JOURNAL_POOL",
        "OSWX_COLLAB_DATABASE_LOCK_TIMEOUT",
        "OSWX_COLLAB_DATABASE_JOURNAL_LOCK_TIMEOUT",
        "OSWX_COLLAB_STORAGE_URL",
        "OSWX_COLLAB_S3_ENDPOINT",
        "OSWX_COLLAB_S3_PATH_STYLE",
        "OSWX_COLLAB_BLOB_IDLE_TIMEOUT",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_DEFAULT_REGION",
        "AWS_REGION",
        "OSWX_COLLAB_MAX_BLOB_UPLOADS_PER_ORG",
        "OSWX_COLLAB_MIN_UPLOAD_RATE",
        "OSWX_COLLAB_UPLOAD_RATE_GRACE",
        "OSWX_COLLAB_MAX_CONNECTIONS",
        "OSWX_COLLAB_LOG_LEVEL",
        "OSWX_COLLAB_TRUSTED_PROXIES",
        "OSWX_COLLAB_METRICS_TOKEN",
        "OSWX_COLLAB_LOG",
    ] {
        assert!(help.contains(word), "the help does not say {word}");
    }
    let serve = ok(dir.path(), &["serve", "--help"], &[]);
    assert!(
        serve.contains("--host") && serve.contains("--port"),
        "{serve}"
    );
    assert!(
        serve.contains("127.0.0.1") && serve.contains("8787"),
        "{serve}"
    );
    assert_eq!(
        ok(dir.path(), &["--version"], &[]),
        format!("openswx-collab-server {VERSION}")
    );
    // Asking for help opens nothing.
    assert!(!dir.path().join("collab.db").exists());
    // A command that does not exist, or none: told how, exit code 2.
    for args in [&["frobnicate"][..], &[], &["serve", "--port", "x"]] {
        let ran = run(dir.path(), args, &[]);
        assert_eq!(ran.code, Some(2), "{args:?}");
        assert!(ran.err.contains("--help"), "{}", ran.err);
    }
}

#[test]
fn migrate_and_db_status_say_where_the_schema_is() {
    let dir = TempDir::new().unwrap();
    // Nothing there yet: `db-status` says so, fails, and makes nothing (a command that only
    // looks must not leave an empty database behind).
    let ran = run(dir.path(), &["db-status"], &[]);
    assert_eq!((ran.code, ran.out.as_str()), (Some(1), ""), "{}", ran.err);
    assert!(
        ran.err.starts_with("error: there is no database at ")
            && ran.err.contains("nothing was created"),
        "{}",
        ran.err
    );
    let left: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert!(left.is_empty(), "db-status made {left:?}");
    // The same for a folder that does not exist: it is not made either.
    let elsewhere = format!("sqlite://{}/not/made/collab.db", slashes(dir.path()));
    let elsewhere = [("OSWX_COLLAB_DATABASE_URL", elsewhere.as_str())];
    assert_eq!(run(dir.path(), &["db-status"], &elsewhere).code, Some(1));
    assert!(!dir.path().join("not").exists());
    // (With upgrading at start turned off: `migrate` is the way to do it then.)
    let manual = [("OSWX_COLLAB_AUTO_MIGRATE", "0")];
    assert_eq!(ok(dir.path(), &["migrate"], &manual), "schema none -> 1");
    assert_eq!(
        ok(dir.path(), &["migrate"], &manual),
        "schema 1: up to date"
    );
    assert_eq!(
        ok(dir.path(), &["db-status"], &manual),
        "database at 1; this server needs 1"
    );
    // A database of some other program: refused, with why, and nothing is changed.
    let other = TempDir::new().unwrap();
    std::fs::write(other.path().join("collab.db"), b"this is not a database").unwrap();
    for command in ["migrate", "db-status"] {
        let ran = run(other.path(), &[command], &[]);
        assert_eq!(ran.code, Some(1), "{command}: {}", ran.out);
        assert!(ran.err.starts_with("error: "), "{}", ran.err);
        assert_eq!(ran.out, "");
    }
    assert_eq!(
        std::fs::read(other.path().join("collab.db")).unwrap(),
        b"this is not a database"
    );
}

#[tokio::test]
async fn create_admin_and_reset_password_set_what_they_say() {
    let dir = TempDir::new().unwrap();
    let with = [("OSWX_COLLAB_PASSWORD", PASSWORD)];
    // On a folder with nothing in it: the database is made.
    assert_eq!(
        ok(dir.path(), &["create-admin", "Ada@Example.com"], &with),
        "created the server admin ada@example.com"
    );
    let db = Db::connect(&settings(dir.path())).await.unwrap();
    let ada = db
        .store()
        .user_by_email("ada@example.com")
        .await
        .unwrap()
        .unwrap();
    assert!(ada.is_server_admin && ada.email_verified);
    assert_eq!(ada.name, "ada");
    let first_hash = ada.password_hash.clone().unwrap();
    assert!(oswx_server::security::verify_password(PASSWORD, &first_hash).await);
    db.close().await;

    // Again, with a name and another password: the same account, the new password.
    let other = [("OSWX_COLLAB_PASSWORD", "another password of mine")];
    assert_eq!(
        ok(
            dir.path(),
            &["create-admin", "ada@example.com", "Ada L"],
            &other
        ),
        "ada@example.com is now a server admin; its password is set and its sessions are ended"
    );
    assert_eq!(
        ok(dir.path(), &["reset-password", "ada@example.com"], &with),
        "the password of ada@example.com is set; its sessions are ended"
    );
    let db = Db::connect(&settings(dir.path())).await.unwrap();
    let store = db.store();
    assert_eq!(store.user_count().await.unwrap(), 1);
    let ada = store
        .user_by_email("ada@example.com")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(ada.password_hash.as_deref(), Some(first_hash.as_str()));
    assert!(oswx_server::security::verify_password(PASSWORD, &ada.password_hash.unwrap()).await);
    db.close().await;

    // What is refused, how, and that the password is never repeated.
    let short = [("OSWX_COLLAB_PASSWORD", "2short")];
    for (args, env, message) in [
        (
            &["reset-password", "nobody@example.com"][..],
            &with[..],
            "error: no account with the address nobody@example.com",
        ),
        (
            &["reset-password", "not-an-address"],
            &with,
            "error: not an email address",
        ),
        (
            &["create-admin", "not-an-address"],
            &with,
            "error: not an email address",
        ),
        (
            &["create-admin", "bo@example.com"],
            &short,
            "error: the password must be 8 to 200 characters",
        ),
        (
            &["reset-password", "ada@example.com"],
            &short,
            "error: the password must be 8 to 200 characters",
        ),
        // No password given and no terminal to ask on.
        (
            &["create-admin", "bo@example.com"],
            &[],
            "error: set OSWX_COLLAB_PASSWORD to the password, or run this in a terminal to type it",
        ),
        (
            &["reset-password", "ada@example.com"],
            &[],
            "error: set OSWX_COLLAB_PASSWORD to the password, or run this in a terminal to type it",
        ),
    ] {
        let ran = run(dir.path(), args, env);
        assert_eq!(
            (ran.code, ran.err.as_str(), ran.out.as_str()),
            (Some(1), message, ""),
            "{args:?}"
        );
    }
    for ran in [
        run(dir.path(), &["create-admin", "cy@example.com"], &with),
        run(dir.path(), &["reset-password", "ada@example.com"], &with),
        run(dir.path(), &["create-admin", "bo@example.com"], &short),
    ] {
        assert!(!ran.out.contains(PASSWORD) && !ran.err.contains(PASSWORD));
        assert!(!ran.err.contains("2short"), "{}", ran.err);
    }
}

#[test]
fn a_setting_that_cannot_be_read_stops_every_command_and_is_never_repeated() {
    let dir = TempDir::new().unwrap();
    let secret = "hunter2-very-secret";
    let url_with_password = format!("mysql://collab:{secret}@db.internal/collab");
    for (name, value) in [
        ("OSWX_COLLAB_DATABASE_POOL", "0"),
        ("OSWX_COLLAB_DATABASE_POOL", "many"),
        ("OSWX_COLLAB_DATABASE_JOURNAL_POOL", "-1"),
        ("OSWX_COLLAB_DATABASE_LOCK_TIMEOUT", "21"),
        ("OSWX_COLLAB_DATABASE_LOCK_TIMEOUT", "0"),
        ("OSWX_COLLAB_DATABASE_JOURNAL_LOCK_TIMEOUT", "8"),
        ("OSWX_COLLAB_DATABASE_URL", url_with_password.as_str()),
        ("OSWX_COLLAB_DATABASE_URL", "sqlite::memory:"),
        ("OSWX_COLLAB_STORAGE_URL", "s3:///no-bucket"),
        ("OSWX_COLLAB_STORAGE_URL", ""),
        ("OSWX_COLLAB_S3_PATH_STYLE", "sometimes"),
        ("OSWX_COLLAB_BLOB_IDLE_TIMEOUT", "never"),
        ("OSWX_COLLAB_MAX_BLOB_UPLOADS", "0"),
        ("OSWX_COLLAB_MAX_BLOB_UPLOADS_PER_USER", "x"),
        ("OSWX_COLLAB_REQUEST_TIMEOUT", "0"),
        ("OSWX_COLLAB_WRITE_TIMEOUT", "-3"),
        ("OSWX_COLLAB_SHUTDOWN_TIMEOUT", "soon"),
        ("OSWX_COLLAB_TRUSTED_PROXIES", "the-load-balancer"),
        ("OSWX_COLLAB_METRICS_TOKEN", "short"),
        (
            "OSWX_COLLAB_METRICS_TOKEN",
            "a token with spaces in it, quite long",
        ),
        ("OSWX_COLLAB_SMTP_SECURITY", "tls1.0"),
        ("OSWX_COLLAB_LOG", "xml"),
        ("OSWX_COLLAB_LOG_LEVEL", "chatty"),
        ("OSWX_COLLAB_LOG_LEVEL", "debug,sqlx=loud"),
        // Every address there is, is not a list of proxies.
        ("OSWX_COLLAB_TRUSTED_PROXIES", "0.0.0.0/0"),
        ("OSWX_COLLAB_TRUSTED_PROXIES", "172.16.0.0/12, ::/0"),
        ("OSWX_COLLAB_MAX_BLOB_UPLOADS_PER_ORG", "0"),
        ("OSWX_COLLAB_MIN_UPLOAD_RATE", "slowly"),
        ("OSWX_COLLAB_UPLOAD_RATE_GRACE", "0"),
        ("OSWX_COLLAB_MAX_CONNECTIONS", "0"),
    ] {
        for command in [&["serve", "--port", "0"][..], &["migrate"], &["db-status"]] {
            let ran = run(dir.path(), command, &[(name, value)]);
            assert_eq!(
                ran.code,
                Some(2),
                "{name}={value:?} {command:?}: {}",
                ran.err
            );
            assert!(
                ran.err.starts_with(&format!("error: {name} must be ")),
                "{name}={value:?}: {}",
                ran.err
            );
            assert_eq!(ran.err.lines().count(), 1, "{}", ran.err);
            assert_eq!(ran.out, "");
            assert!(!ran.err.contains(secret), "{}", ran.err);
            if value.len() > 4 {
                assert!(
                    !ran.err.contains(value),
                    "the value is repeated: {}",
                    ran.err
                );
            }
        }
    }
    // For a bucket: the store's address must be one, and the credentials come as a pair (half of
    // one names what is missing). Neither is repeated.
    let bucket = ("OSWX_COLLAB_STORAGE_URL", "s3://bucket");
    let with_password = format!("https://key:{secret}@minio.internal:9000");
    for (given, value, missing) in [
        (
            "OSWX_COLLAB_S3_ENDPOINT",
            "minio.internal:9000",
            "OSWX_COLLAB_S3_ENDPOINT",
        ),
        (
            "OSWX_COLLAB_S3_ENDPOINT",
            with_password.as_str(),
            "OSWX_COLLAB_S3_ENDPOINT",
        ),
        (
            "AWS_ACCESS_KEY_ID",
            "AKIA-the-key-id",
            "AWS_SECRET_ACCESS_KEY",
        ),
        ("AWS_SECRET_ACCESS_KEY", secret, "AWS_ACCESS_KEY_ID"),
        ("AWS_SESSION_TOKEN", secret, "AWS_SESSION_TOKEN"),
    ] {
        let ran = run(
            dir.path(),
            &["serve", "--port", "0"],
            &[bucket, (given, value)],
        );
        assert_eq!(ran.code, Some(2), "{given}: {}", ran.err);
        assert!(
            ran.err.starts_with(&format!("error: {missing} must be ")),
            "{}",
            ran.err
        );
        assert!(!ran.err.contains(value), "{}", ran.err);
    }
    // Nothing was opened or made for any of them.
    assert!(!dir.path().join("collab.db").exists());
    assert!(!dir.path().join("storage").exists());
}

#[test]
fn a_server_that_cannot_reach_its_database_or_storage_says_so_without_its_secrets() {
    let dir = TempDir::new().unwrap();
    let secret = "hunter2-very-secret";
    // A PostgreSQL server nobody listens at (the discard port).
    let database = format!("postgres://collab-user:{secret}@127.0.0.1:9/collab");
    for command in [&["serve", "--port", "0"][..], &["migrate"], &["db-status"]] {
        let ran = run(
            dir.path(),
            command,
            &[("OSWX_COLLAB_DATABASE_URL", &database)],
        );
        assert_eq!(ran.code, Some(1), "{command:?}: {}", ran.err);
        assert!(
            ran.err
                .contains("cannot open the database collab on 127.0.0.1:9"),
            "{}",
            ran.err
        );
        assert!(
            !ran.err.contains(secret) && !ran.err.contains("collab-user"),
            "{}",
            ran.err
        );
        assert_eq!(ran.out, "");
    }
    // A storage folder that cannot be one (its name is taken by a file).
    std::fs::write(dir.path().join("taken"), b"x").unwrap();
    let storage = format!("{}/taken/storage", slashes(dir.path()));
    let ran = run(
        dir.path(),
        &["serve", "--port", "0"],
        &[
            ("OSWX_COLLAB_STORAGE_URL", &storage),
            ("OSWX_COLLAB_SMTP_PASSWORD", secret),
        ],
    );
    assert_eq!(ran.code, Some(1), "{}", ran.err);
    assert!(
        ran.err.contains("error: the server could not start: "),
        "{}",
        ran.err
    );
    assert!(!ran.err.contains(secret), "{}", ran.err);
    // A port that is taken.
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = taken.local_addr().unwrap().port().to_string();
    let ran = run(dir.path(), &["serve", "--port", &port], &[]);
    assert_eq!(ran.code, Some(1), "{}", ran.err);
    assert!(
        ran.err.contains(&format!(
            "the server could not start: cannot listen on 127.0.0.1:{port}"
        )),
        "{}",
        ran.err
    );
}

// ------------------------------------------------------------------ serve

/// A running `serve`. Dropping it kills it.
struct Server {
    child: Child,
    base: String,
    log: PathBuf,
    dir: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    /// Starts `serve` in `dir` on a port the system picks (`--port 0`), reads the address from
    /// the line the server logs once it listens, and checks that it answers `/api/health`.
    async fn start(dir: &Path, env: &[(&str, &str)]) -> Self {
        static STARTED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = STARTED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let log = dir.join(format!("server-{n}.log"));
        let mut command = program(dir, &["serve", "--port", "0"], env);
        command
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap());
        own_console(&mut command);
        let mut server = Self {
            child: command.spawn().expect("the server starts"),
            base: String::new(),
            log,
            dir: dir.to_path_buf(),
        };
        let started = Instant::now();
        loop {
            // Text: `... listening on http://127.0.0.1:50123 version=...`; JSON:
            // `..."message":"listening on http://127.0.0.1:50123",...`.
            let logged = server.logged();
            if let Some((_, rest)) = logged.split_once("listening on http://") {
                let address: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == ':')
                    .collect();
                assert!(
                    address.starts_with("127.0.0.1:") && !address.ends_with(":0"),
                    "{logged}"
                );
                server.base = format!("http://{address}");
                break;
            }
            if let Some(status) = server.child.try_wait().unwrap() {
                panic!("the server ended ({status}): {logged}");
            }
            assert!(
                started.elapsed() < WAIT,
                "the server did not start: {logged}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // It listens before it says so: the first request is answered.
        let health = reqwest::get(format!("{}/api/health", server.base))
            .await
            .unwrap_or_else(|e| panic!("{e}: {}", server.logged()));
        assert_eq!(health.status(), 200);
        assert_eq!(health.json::<Value>().await.unwrap(), json!({"ok": true}));
        server
    }

    fn logged(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    async fn post(&self, path: &str, token: Option<&str>, body: Value) -> Value {
        let mut request = reqwest::Client::new()
            .post(format!("{}{path}", self.base))
            .json(&body);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        assert!(status.is_success(), "{path}: {status} {body}");
        body
    }

    /// Waits for the process to end; answers its exit code and how long that took.
    async fn ended(&mut self, within: Duration) -> (Option<i32>, Duration) {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return (status.code(), started.elapsed());
            }
            assert!(
                started.elapsed() < within,
                "the server was still running after {within:?}: {}",
                self.logged()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Tells the server to stop, as an operator's Ctrl-C does, or `docker stop`.
    fn interrupt(&self, signal: Signal) {
        send_signal(self.child.id(), signal, &self.dir);
    }
}

#[derive(Debug, Clone, Copy)]
enum Signal {
    /// SIGINT; on Windows, Ctrl-Break.
    Interrupt,
    /// SIGTERM, what `docker stop` sends; on Windows (which has none), Ctrl-Break as well.
    Terminate,
    /// SIGHUP: the terminal the server was started from went away.
    #[cfg(unix)]
    Hangup,
}

#[cfg(unix)]
fn own_console(_command: &mut Command) {}

#[cfg(unix)]
fn send_signal(pid: u32, signal: Signal, _dir: &Path) {
    let name = match signal {
        Signal::Interrupt => "-INT",
        Signal::Terminate => "-TERM",
        Signal::Hangup => "-HUP",
    };
    let status = Command::new("kill")
        .arg(name)
        .arg(pid.to_string())
        .status()
        .unwrap();
    assert!(status.success(), "kill {name} {pid}");
}

/// On Windows the server gets a console of its own, without a window, and a process group of its
/// own, for the helper below to send Ctrl-Break to (and to nothing else).
#[cfg(windows)]
fn own_console(command: &mut Command) {
    use std::os::windows::process::CommandExt as _;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
}

/// Sends Ctrl-Break to the process group of `pid`: a PowerShell script attaches to that
/// process's console and generates the event there, for that group.
#[cfg(windows)]
fn send_signal(pid: u32, _signal: Signal, dir: &Path) {
    const SCRIPT: &str = r#"param([uint32]$TargetPid)
$k = Add-Type -PassThru -Name Console -Namespace OswxTest -MemberDefinition @'
[DllImport("kernel32.dll")] public static extern bool FreeConsole();
[DllImport("kernel32.dll")] public static extern bool AttachConsole(uint pid);
[DllImport("kernel32.dll")] public static extern bool GenerateConsoleCtrlEvent(uint ev, uint group);
'@
[void]$k::FreeConsole()
if (-not $k::AttachConsole($TargetPid)) { exit 3 }
if (-not $k::GenerateConsoleCtrlEvent(1, $TargetPid)) { exit 4 }
exit 0
"#;
    let script = dir.join("send-ctrl-break.ps1");
    if !script.exists() {
        std::fs::File::create(&script)
            .unwrap()
            .write_all(SCRIPT.as_bytes())
            .unwrap();
    }
    let output = Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ])
        .arg(&script)
        .arg(pid.to_string())
        .stdin(Stdio::null())
        .output()
        .expect("PowerShell runs");
    assert!(
        output.status.success(),
        "Ctrl-Break could not be sent ({}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A part, as the openswx client stores one.
fn part() -> Value {
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

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn next_message(socket: &mut Socket) -> Option<Result<ServerMsg, u16>> {
    loop {
        match tokio::time::timeout(WAIT, socket.next())
            .await
            .expect("the server says something")
        {
            Some(Ok(Message::Binary(frame))) => {
                return Some(Ok(wire::decode_server(&frame).expect("a server message")));
            }
            Some(Ok(Message::Close(frame))) => {
                return Some(Err(frame.map_or(1005, |f| f.code.into())));
            }
            Some(Ok(_)) => {}
            Some(Err(_)) | None => return None,
        }
    }
}

/// Opens the document's room and makes `changes` changes to it; answers the socket, still open,
/// once every change was acknowledged.
async fn edit(server: &Server, token: &str, document: &str, changes: u64) -> Socket {
    let url = format!(
        "{}/api/documents/{document}/ws",
        server.base.replacen("http", "ws", 1)
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("the WebSocket opens");
    let hello = ClientMsg::Hello {
        protocol: PROTOCOL_VERSION,
        token: token.to_owned(),
        client_id: "cli-test".to_owned(),
        format_version: Some(3),
        last_seq: None,
        app: None,
    };
    socket
        .send(Message::Binary(wire::encode_client(&hello)))
        .await
        .unwrap();
    let mut seq = match next_message(&mut socket).await {
        Some(Ok(ServerMsg::Welcome { seq, .. })) => seq,
        other => panic!("no welcome: {other:?}"),
    };
    for n in 1..=changes {
        let depth = DocValue::from_plain(&plain_from_json(&json!(10 + n))).unwrap();
        let change = Change {
            change_id: Arc::from(format!("cli-test-{n}")),
            base: seq,
            ops: vec![Op::Set {
                path: ["features", "f2", "params", "depth"]
                    .iter()
                    .map(|s| Arc::from(*s))
                    .collect(),
                value: depth,
            }],
            reads: Default::default(),
            depends: Vec::new(),
            undo_of: None,
            command: Arc::from("edit"),
        };
        socket
            .send(Message::Binary(wire::encode_client(&ClientMsg::Change(
                change,
            ))))
            .await
            .unwrap();
        loop {
            match next_message(&mut socket).await {
                Some(Ok(ServerMsg::Accepted { seq: accepted, .. })) => {
                    seq = accepted;
                    break;
                }
                Some(Ok(ServerMsg::Rejected { .. })) | Some(Err(_)) | None => {
                    panic!("change {n} was not accepted")
                }
                Some(Ok(_)) => {}
            }
        }
    }
    assert_eq!(seq, changes);
    socket
}

/// Registers the first account, and makes an organisation, a project and `documents` documents.
async fn world(server: &Server, documents: usize) -> (String, String, Vec<String>) {
    let registered = server
        .post(
            "/api/auth/register",
            None,
            json!({"email": "ada@example.com", "password": PASSWORD, "name": "Ada"}),
        )
        .await;
    let token = registered["token"].as_str().unwrap().to_owned();
    let org = server
        .post("/api/orgs", Some(&token), json!({"name": "Robotics"}))
        .await;
    let project = server
        .post(
            &format!("/api/orgs/{}/projects", org["id"].as_str().unwrap()),
            Some(&token),
            json!({"name": "Rover"}),
        )
        .await;
    let mut ids = Vec::new();
    for n in 0..documents {
        let document = server
            .post(
                &format!(
                    "/api/projects/{}/documents",
                    project["id"].as_str().unwrap()
                ),
                Some(&token),
                json!({"name": format!("Part {n}"), "content": part()}),
            )
            .await;
        ids.push(document["id"].as_str().unwrap().to_owned());
    }
    (token, project["id"].as_str().unwrap().to_owned(), ids)
}

/// The seq of the newest snapshot of each document, read from the database the server left.
async fn snapshot_seqs(dir: &Path, documents: &[String]) -> Vec<i64> {
    let db = Db::connect(&settings(dir)).await.unwrap();
    let store = db.store();
    let mut seqs = Vec::new();
    for document in documents {
        let row = store
            .snapshot_at_or_before(document, i64::MAX)
            .await
            .unwrap();
        seqs.push(row.expect("a snapshot").seq);
    }
    db.close().await;
    seqs
}

async fn serve_answers_and_a_signal_stops_it_with_every_room_snapshotted(
    signal: Signal,
    json_logs: bool,
) {
    let dir = TempDir::new().unwrap();
    let token_for_metrics = "a-metrics-token-0123456789";
    let mut env = vec![("OSWX_COLLAB_METRICS_TOKEN", token_for_metrics)];
    if json_logs {
        env.push(("OSWX_COLLAB_LOG", "json"));
    }
    let mut server = Server::start(dir.path(), &env).await;
    let (token, _, documents) = world(&server, 2).await;
    // Two open rooms with changes that no snapshot holds yet (one is written every 200).
    let mut one = edit(&server, &token, &documents[0], 3).await;
    let mut two = edit(&server, &token, &documents[1], 1).await;
    // `/metrics` shows them, to who has the token.
    let metrics = reqwest::Client::new()
        .get(format!("{}/metrics", server.base))
        .bearer_auth(token_for_metrics)
        .send()
        .await
        .unwrap();
    assert_eq!(metrics.status(), 200);
    let metrics = metrics.text().await.unwrap();
    assert!(metrics.contains("\noswx_collab_rooms 2\n"), "{metrics}");
    assert!(
        metrics.contains("\noswx_collab_changes_accepted_total 4\n"),
        "{metrics}"
    );
    assert_eq!(snapshot_seqs(dir.path(), &documents).await, vec![0, 0]);

    let started = Instant::now();
    server.interrupt(signal);
    // The clients are told the server is going away ...
    for socket in [&mut one, &mut two] {
        loop {
            match next_message(socket).await {
                Some(Err(code)) => {
                    assert_eq!(code, 1001);
                    break;
                }
                Some(Ok(_)) => {}
                None => panic!("the socket ended without a close frame"),
            }
        }
    }
    drop((one, two));
    // ... and the process ends by itself, in order, well inside what `docker stop` allows.
    let (code, _) = server.ended(Duration::from_secs(10)).await;
    assert_eq!(code, Some(0), "{}", server.logged());
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    assert!(
        reqwest::get(format!("{}/api/health", server.base))
            .await
            .is_err()
    );
    // Every room's snapshot is at its head.
    assert_eq!(snapshot_seqs(dir.path(), &documents).await, vec![3, 1]);

    // The log: it says where the server listened and that it stopped, and no secret.
    let log = server.logged();
    assert!(log.contains("listening on http://127.0.0.1:"), "{log}");
    assert!(
        log.contains("stopping (once more to stop at once)"),
        "{log}"
    );
    assert!(log.contains("stopped"), "{log}");
    for secret in [token.as_str(), PASSWORD, token_for_metrics] {
        assert!(!log.contains(secret), "the log holds a secret: {log}");
    }
    if json_logs {
        let mut lines = 0;
        for line in log.lines() {
            let entry: Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("{line:?}: {e}"));
            assert!(
                entry["timestamp"].is_string() && entry["level"].is_string(),
                "{line}"
            );
            assert!(entry["message"].is_string(), "{line}");
            lines += 1;
        }
        assert!(lines >= 3, "{log}");
        let listening = log.lines().find(|l| l.contains("listening on")).unwrap();
        let entry: Value = serde_json::from_str(listening).unwrap();
        assert_eq!(entry["level"], "INFO");
        assert_eq!(entry["version"], VERSION);
        assert_eq!(entry["database"], "sqlite");
        assert_eq!(entry["storage"], "folder");
        assert_eq!(entry["metrics"], true);
    } else {
        assert!(
            log.lines()
                .all(|l| serde_json::from_str::<Value>(l).is_err()),
            "{log}"
        );
    }

    // The same data serves again: the documents are where they were left.
    let again = Server::start(dir.path(), &[]).await;
    let content: Value = reqwest::Client::new()
        .get(format!(
            "{}/api/documents/{}/content",
            again.base, documents[0]
        ))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(content["seq"], 3, "{content}");
    assert_eq!(
        content["content"]["features"][1]["params"]["depth"], 13,
        "{content}"
    );
}

#[tokio::test]
async fn serve_stops_in_order_on_an_interrupt() {
    serve_answers_and_a_signal_stops_it_with_every_room_snapshotted(Signal::Interrupt, false).await;
}

#[tokio::test]
async fn serve_stops_in_order_on_a_termination_and_logs_json_when_asked() {
    serve_answers_and_a_signal_stops_it_with_every_room_snapshotted(Signal::Terminate, true).await;
}

#[cfg(unix)]
#[tokio::test]
async fn serve_stops_in_order_on_a_hangup() {
    serve_answers_and_a_signal_stops_it_with_every_room_snapshotted(Signal::Hangup, false).await;
}

#[tokio::test]
async fn a_second_signal_stops_the_server_at_once() {
    let dir = TempDir::new().unwrap();
    // A stop that would take its whole (long) bound: an upload whose client sends no more.
    let env = [
        ("OSWX_COLLAB_SHUTDOWN_TIMEOUT", "120"),
        ("OSWX_COLLAB_BLOB_IDLE_TIMEOUT", "120"),
    ];
    let mut server = Server::start(dir.path(), &env).await;
    let (token, project, _) = world(&server, 0).await;
    let mut stuck =
        std::net::TcpStream::connect(server.base.trim_start_matches("http://")).unwrap();
    write!(
        stuck,
        "PUT /api/projects/{project}/blobs/{} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\n\
         Content-Length: 1000\r\n\r\nonly the beginning",
        "ab".repeat(32)
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    server.interrupt(Signal::Interrupt);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        server.child.try_wait().unwrap().is_none(),
        "the server ended although a request was in its handler: {}",
        server.logged()
    );
    assert!(
        server
            .logged()
            .contains("stopping (once more to stop at once)")
    );
    // It no longer accepts connections while it stops.
    assert!(
        reqwest::get(format!("{}/api/health", server.base))
            .await
            .is_err()
    );

    server.interrupt(Signal::Interrupt);
    let (code, took) = server.ended(Duration::from_secs(5)).await;
    assert_eq!(code, Some(1), "{}", server.logged());
    assert!(took < Duration::from_secs(5));
    assert!(
        server.logged().contains("again: stopped at once"),
        "{}",
        server.logged()
    );
    drop(stuck);
}
