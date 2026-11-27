//! The `openswx-collab-server` program: its command line, logging, signals and exit codes.
//!
//! [`run`] is the whole program. It takes the extensions as an argument, so that an edition with
//! extensions is a `main` that calls it with its own.
//!
//! ```text
//! openswx-collab-server serve [--host 127.0.0.1] [--port 8787]
//! openswx-collab-server migrate
//! openswx-collab-server db-status
//! openswx-collab-server create-admin <email> [name]
//! openswx-collab-server reset-password <email>
//! ```
//!
//! Settings come from `OSWX_COLLAB_*` environment variables ([`SETTINGS_HELP`] lists them; so
//! does `--help`). A setting that cannot be read ends the program before anything is opened,
//! with a message that names the variable and never repeats its value.
//!
//! **Exit codes.** 0: done (for `serve`: stopped in order). 1: failed (for `serve`: could not
//! start, did not stop in order within `OSWX_COLLAB_SHUTDOWN_TIMEOUT`, or was told twice to
//! stop). 2: the command line or a setting was wrong.
//!
//! **Logs** go to standard error: lines of text, or one JSON object a line when
//! `OSWX_COLLAB_LOG=json`. `OSWX_COLLAB_LOG_LEVEL` (`error`, `warn`, `info`, `debug`, `trace`;
//! `info` unless set) says how much this project's own crates log; every other crate logs
//! warnings and errors only, unless the variable names it (`debug,sqlx=info`). What a command answers goes to standard output.
//!
//! **Panics.** A panic's message can hold what the code was working on (a document's content, a
//! request), so the program's panic hook logs only where the panic was, never its message.
//!
//! **Stopping.** `serve` stops on Ctrl-C (SIGINT), SIGTERM or SIGHUP (on Windows: Ctrl-C,
//! Ctrl-Break, the console closing, the system shutting down): it stops accepting connections, closes every open
//! document with its snapshot written, writes out the journal and ends, all within
//! `OSWX_COLLAB_SHUTDOWN_TIMEOUT` (8 s unless set; `docker stop` kills after 10). Told a second
//! time, it ends at once.

use std::io::IsTerminal as _;
use std::net::{IpAddr, SocketAddr};
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use oswx_server::config::Storage;
use oswx_server::ext::Extensions;
use oswx_server::serve::{Stopped, serve};
use oswx_server::{AppState, Settings, admin};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt as _;

/// Every setting, as `--help` shows them. (A test holds this against what the server reads.)
pub const SETTINGS_HELP: &str = "\
Settings (environment variables; the default is in brackets):

  The database
    OSWX_COLLAB_DATABASE_URL       sqlite://<path> or postgres://user:password@host/database
                                   [sqlite://./data/collab.db]
    OSWX_COLLAB_AUTO_MIGRATE       upgrade the schema at start; off: run `migrate` yourself [on]
    OSWX_COLLAB_DATABASE_POOL      PostgreSQL connections for requests [10]
    OSWX_COLLAB_DATABASE_JOURNAL_POOL
                                   PostgreSQL connections for the journal, besides those [2]
    OSWX_COLLAB_DATABASE_LOCK_TIMEOUT
                                   seconds a request waits for a row lock, at most 20 [5]
    OSWX_COLLAB_DATABASE_JOURNAL_LOCK_TIMEOUT
                                   seconds the journal waits for a row lock, at most 7.5 [2]

  Storage for blobs and snapshots
    OSWX_COLLAB_STORAGE_URL        a folder, or s3://<bucket>/<prefix> [./data/storage]
    OSWX_COLLAB_S3_ENDPOINT        an S3-compatible store's address; empty: AWS S3 []
    OSWX_COLLAB_S3_PATH_STYLE      the bucket in the path, not the host name [on with an endpoint]
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
                                   the S3 credentials; none: the machine's own AWS role
    AWS_DEFAULT_REGION (or AWS_REGION)
                                   the bucket's region [us-east-1]
    OSWX_COLLAB_BLOB_IDLE_TIMEOUT  seconds without a byte before a blob transfer is given up [60]
    OSWX_COLLAB_MAX_BLOB_BYTES     the largest blob [268435456]
    OSWX_COLLAB_UPLOAD_MEMORY      bytes the blob uploads running at once may hold in all; each
                                   is charged what its storage holds (S3: 45 MiB, a folder:
                                   4 MiB); one that does not fit: 503 [754974720]. (It
                                   replaced OSWX_COLLAB_MAX_BLOB_UPLOADS, which must be unset.)
    OSWX_COLLAB_MAX_BLOB_UPLOADS_PER_ORG
                                   blob uploads at once for one organisation; one more: 429 [8]
    OSWX_COLLAB_MAX_BLOB_UPLOADS_PER_USER
                                   blob uploads at once for one account; one more: 429 [4]
    OSWX_COLLAB_MIN_UPLOAD_RATE    bytes a second an upload, or any request body, must keep up
                                   on average; 0: no floor [4096]
    OSWX_COLLAB_UPLOAD_RATE_GRACE  seconds of slack against that rate: how long a body may take
                                   to start, or pause once it has earned the time back [30]
    OSWX_COLLAB_UPLOAD_TIME_LIMIT, OSWX_COLLAB_UPLOAD_TIME_RATE
                                   an upload of n bytes may take at most
                                   max(LIMIT, n / RATE) seconds [600, 65536]

  Serving
    OSWX_COLLAB_PUBLIC_URL         how clients and browsers reach the server [http://localhost:8787]
    OSWX_COLLAB_TRUSTED_PROXIES    addresses and CIDR ranges of the proxies whose X-Forwarded-For
                                   is believed, e.g. `127.0.0.1, 172.16.0.0/12` [127.0.0.1, ::1]
    OSWX_COLLAB_REQUEST_TIMEOUT    seconds from a request's head, or the last of its body, to
                                   the beginning of its answer, before it is answered 503 [60]
    OSWX_COLLAB_MAX_CONNECTIONS    connections held open at once, at most 65536; when full, a new
                                   one takes the place of one that never finished a request, or
                                   the one idle longest; 8 more answer /api/health alone [4096]
    OSWX_COLLAB_MAX_CONNECTIONS_PER_ADDRESS
                                   connections from one address (not a trusted proxy's; an IPv6
                                   address counts as its /64); one more takes the place of the
                                   address's own idle one; 0: no limit [512]
    OSWX_COLLAB_MAX_LARGE_BODIES   request bodies over 1 MiB read at once; one more: 503 [16]
    OSWX_COLLAB_WRITE_TIMEOUT      seconds a client may take nothing of an answer [60]
    OSWX_COLLAB_SHUTDOWN_TIMEOUT   seconds an orderly stop may take [8]
    OSWX_COLLAB_METRICS_TOKEN      the bearer token GET /metrics asks for; empty: no /metrics []
    OSWX_COLLAB_LOG                text or json [text]
    OSWX_COLLAB_LOG_LEVEL          error, warn, info, debug or trace, for the server's own
                                   crates (or `oswx=<level>`) [info]; other crates log warnings
                                   and errors only, unless named: `debug,sqlx=info,hyper=trace`

  Accounts
    OSWX_COLLAB_OPEN_REGISTRATION  anyone may register; off: the first account, then invites [off]
    OSWX_COLLAB_SESSION_DAYS       days a sign-in lasts [30]
    OSWX_COLLAB_RESET_MINUTES      minutes a password reset link works [60]
    OSWX_COLLAB_PASSWORD           the password `create-admin` and `reset-password` set
                                   (asked for on the terminal when unset)
    OSWX_COLLAB_GOOGLE_CLIENT_ID, OSWX_COLLAB_GOOGLE_CLIENT_SECRET,
    OSWX_COLLAB_GITHUB_CLIENT_ID, OSWX_COLLAB_GITHUB_CLIENT_SECRET
                                   sign-in with Google and GitHub (both of a pair) []

  Email
    OSWX_COLLAB_SMTP_HOST, OSWX_COLLAB_SMTP_FROM
                                   the mail server and the sender; both, or no email is sent []
    OSWX_COLLAB_SMTP_PORT [587]    OSWX_COLLAB_SMTP_SECURITY  starttls, ssl or none [starttls]
    OSWX_COLLAB_SMTP_USER, OSWX_COLLAB_SMTP_PASSWORD []
    OSWX_COLLAB_SMTP_FROM_NAME     the sender's name [openswx collaboration]
    OSWX_COLLAB_MAX_MAIL_SENDS     emails on their way at once; one more is not sent [16]

  Documents
    OSWX_COLLAB_SNAPSHOT_EVERY     changes between two stored copies of a document [200]
    OSWX_COLLAB_CATCHUP_LIMIT      changes a reconnecting client is sent before the whole [500]
    OSWX_COLLAB_MAX_CHANGE_BYTES   the largest change [8388608]
    OSWX_COLLAB_MAX_CONNECTIONS_PER_USER
                                   open document connections of one account [64]
    OSWX_COLLAB_PRESENCE_RATE      presence messages a second from one client [60]
    OSWX_COLLAB_LOCK_TTL [45]      OSWX_COLLAB_LOCK_GRACE [60]    OSWX_COLLAB_ROOM_IDLE [120]
    OSWX_COLLAB_SWEEP_INTERVAL [5] OSWX_COLLAB_ACCESS_RECHECK [30]
                                   seconds: a silent client's locks, a gone client's locks, an
                                   unused document in memory, between sweeps, between checks of
                                   open connections against sessions and roles

A setting that cannot be read stops the program before it opens anything.";

#[derive(Debug, Parser)]
#[command(
    name = "openswx-collab-server",
    version = oswx_server::VERSION,
    about = "The openswx collaboration server",
    after_help = SETTINGS_HELP
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the server until it is told to stop (Ctrl-C, SIGTERM)
    Serve {
        /// The address to listen on (0.0.0.0 or :: for every interface)
        #[arg(long, default_value = "127.0.0.1")]
        host: IpAddr,
        /// The port to listen on
        #[arg(long, default_value_t = 8787)]
        port: u16,
    },
    /// Upgrade the database schema (whatever OSWX_COLLAB_AUTO_MIGRATE says)
    Migrate,
    /// Show the database's schema version and the one this server needs
    DbStatus,
    /// Create a server admin, or make an existing account one and set its password
    CreateAdmin {
        /// The account's email address
        email: String,
        /// Its name (the part of the address before the @ unless given)
        name: Option<String>,
    },
    /// Set an account's password and end its sessions
    ResetPassword {
        /// The account's email address
        email: String,
    },
}

/// The program's own environment variables, which are not settings of the server.
const LOG: &str = "OSWX_COLLAB_LOG";
const LOG_LEVEL: &str = "OSWX_COLLAB_LOG_LEVEL";
/// What the names of this project's crates begin with, as log targets.
const OUR_CRATES: &str = "oswx";
const PASSWORD: &str = "OSWX_COLLAB_PASSWORD";

/// A command line or a setting that was wrong.
const USAGE: u8 = 2;

/// The program: reads the command line and the environment, does what was asked with the
/// extensions `ext`, and answers the exit code (see the crate documentation).
pub fn run(ext: Arc<dyn Extensions>) -> ExitCode {
    let cli = Cli::parse();
    let settings = match Settings::from_process_env() {
        Ok(settings) => settings,
        Err(e) => return wrong(&e.to_string()),
    };
    if let Err(e) = start_logging() {
        return wrong(&e);
    }
    log_panics_without_their_messages();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => return failed(&format!("the runtime could not be started: {e}")),
    };
    let code = runtime.block_on(async {
        let answer = match cli.command {
            Command::Serve { host, port } => {
                return run_server(settings, ext, SocketAddr::new(host, port)).await;
            }
            command => command,
        };
        let ext = ext.as_ref();
        let answer = match answer {
            Command::Serve { .. } => unreachable!("served above"),
            Command::Migrate => admin::migrate(&settings, ext).await,
            Command::DbStatus => admin::db_status(&settings, ext).await,
            Command::CreateAdmin { email, name } => match password() {
                Ok(password) => {
                    admin::create_admin(&settings, ext, &email, name.as_deref(), &password).await
                }
                Err(e) => Err(e),
            },
            Command::ResetPassword { email } => match password() {
                Ok(password) => admin::reset_password(&settings, ext, &email, &password).await,
                Err(e) => Err(e),
            },
        };
        match answer {
            Ok(line) => {
                println!("{line}");
                ExitCode::SUCCESS
            }
            Err(e) => failed(&format!("{e:#}")),
        }
    });
    // (Nothing that is still running on the runtime is waited for.)
    runtime.shutdown_background();
    code
}

fn wrong(message: &str) -> ExitCode {
    eprintln!("error: {message}");
    ExitCode::from(USAGE)
}

fn failed(message: &str) -> ExitCode {
    eprintln!("error: {message}");
    ExitCode::FAILURE
}

/// What is logged, from `OSWX_COLLAB_LOG_LEVEL`: a level for this project's own crates (those
/// whose names start `oswx`; `info` unless given), and `warn` for every other crate unless the
/// variable asks for more by name: `debug,sqlx=info,hyper=trace`. (Other crates log at `info`
/// and below what is theirs to say, statements and addresses included; it is there for the
/// asking, not by default.)
fn log_targets(raw: &str) -> Result<Targets, String> {
    let wrong = || {
        format!(
            "{LOG_LEVEL} must be error, warn, info, debug or trace, optionally followed by \
             `<crate>=<level>` entries separated by commas"
        )
    };
    let level = |word: &str| match word.trim().to_ascii_lowercase().as_str() {
        "error" => Ok(tracing::Level::ERROR),
        "warn" | "warning" => Ok(tracing::Level::WARN),
        "info" => Ok(tracing::Level::INFO),
        "debug" => Ok(tracing::Level::DEBUG),
        "trace" => Ok(tracing::Level::TRACE),
        _ => Err(wrong()),
    };
    let mut ours = tracing::Level::INFO;
    let mut ours_named = false;
    let mut targets = Targets::new().with_default(tracing::Level::WARN);
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        match entry.split_once('=') {
            None => ours = level(entry)?,
            Some((name, asked)) => {
                let name = name.trim();
                let named = !name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':');
                if !named {
                    return Err(wrong());
                }
                ours_named |= name == OUR_CRATES;
                targets = targets.with_target(name.to_owned(), level(asked)?);
            }
        }
    }
    // (A target is matched by its beginning: `oswx` is oswx_server, oswx_serverd, oswx_core. An
    // entry that names one of them more exactly wins over this one; one that names `oswx` itself
    // is this one.)
    if ours_named {
        return Ok(targets);
    }
    Ok(targets.with_target(OUR_CRATES, ours))
}

/// Starts logging to standard error as `OSWX_COLLAB_LOG` and `OSWX_COLLAB_LOG_LEVEL` say. The
/// error names the variable that could not be read.
fn start_logging() -> Result<(), String> {
    let var = |name: &str| std::env::var(name).unwrap_or_default();
    let targets = log_targets(&var(LOG_LEVEL))?;
    let lines = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(false);
    // (Set as the default directly, not with `init`: that would also pass on what libraries
    // write through the `log` crate, when some crate of the build turns that feature on.)
    let set = match var(LOG).trim().to_ascii_lowercase().as_str() {
        "" | "text" => tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(targets).with(lines),
        ),
        "json" => tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(targets)
                .with(lines.json().flatten_event(true)),
        ),
        _ => return Err(format!("{LOG} must be text or json")),
    };
    set.map_err(|e| format!("logging could not be started: {e}"))
}

/// Replaces the panic hook with one that logs where a panic was and nothing of its message
/// (which can hold what the code was working on: a document's content, a request).
pub fn log_panics_without_their_messages() {
    std::panic::set_hook(Box::new(|panic| {
        let thread = std::thread::current();
        let thread = thread.name().unwrap_or("unnamed");
        match panic.location() {
            Some(at) => tracing::error!(
                file = at.file(),
                line = at.line(),
                column = at.column(),
                thread,
                "a panic (its message is withheld)"
            ),
            None => tracing::error!(thread, "a panic (its message is withheld)"),
        }
    }));
}

/// The password for `create-admin` and `reset-password`: `OSWX_COLLAB_PASSWORD`, or typed twice
/// on the terminal without being shown. (Its length is checked by the command.)
fn password() -> anyhow::Result<String> {
    if let Ok(password) = std::env::var(PASSWORD) {
        return Ok(password);
    }
    if !std::io::stdin().is_terminal() {
        anyhow::bail!("set {PASSWORD} to the password, or run this in a terminal to type it");
    }
    let first = rpassword::prompt_password("Password: ")?;
    let second = rpassword::prompt_password("Again: ")?;
    anyhow::ensure!(first == second, "the two passwords differ");
    Ok(first)
}

/// `serve`: opens everything, listens on `address`, and serves until told to stop.
async fn run_server(settings: Settings, ext: Arc<dyn Extensions>, address: SocketAddr) -> ExitCode {
    let database = match settings.database() {
        Ok(oswx_server::config::Database::Sqlite(_)) => "sqlite",
        _ => "postgres",
    };
    let storage = match settings.storage() {
        Ok(Storage::S3 { .. }) => "s3",
        _ => "folder",
    };
    let metrics = !settings.metrics_token.is_empty();
    let trusted_proxies = settings.trusted_proxies.len();
    let public_url = settings.public_url.clone();
    // The signals are listened for before anything is opened: a stop during a slow start ends
    // the program rather than being lost.
    let mut signals = match Signals::listen() {
        Ok(signals) => signals,
        Err(e) => return failed(&format!("signals cannot be listened for: {e}")),
    };
    let starting = async {
        let state = AppState::from_settings_with(settings, ext).await?;
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(|e| anyhow::anyhow!("cannot listen on {address}: {e}"))?;
        anyhow::Ok((state, listener))
    };
    let (state, listener) = tokio::select! {
        started = starting => match started {
            Ok(started) => started,
            Err(e) => return failed(&format!("the server could not start: {e:#}")),
        },
        signal = signals.next() => {
            tracing::info!("{signal} while starting: stopped");
            return ExitCode::SUCCESS;
        }
    };
    let address = listener.local_addr().unwrap_or(address);
    tracing::info!(
        version = oswx_server::VERSION,
        %address,
        database,
        storage,
        public_url,
        metrics,
        trusted_proxies,
        "listening on http://{address}"
    );
    let (stop, stopping) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let signal = signals.next().await;
        tracing::info!("{signal}: stopping (once more to stop at once)");
        let _ = stop.send(());
        let signal = signals.next().await;
        tracing::warn!("{signal} again: stopped at once");
        std::process::exit(1);
    });
    let stopped = serve(listener, state, async {
        let _ = stopping.await;
    })
    .await;
    match stopped {
        Stopped::InOrder => {
            tracing::info!("stopped");
            ExitCode::SUCCESS
        }
        Stopped::OutOfTime => ExitCode::FAILURE,
    }
}

/// The signals that tell the server to stop.
struct Signals {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    hangup: tokio::signal::unix::Signal,
    #[cfg(windows)]
    ctrl_c: tokio::signal::windows::CtrlC,
    #[cfg(windows)]
    ctrl_break: tokio::signal::windows::CtrlBreak,
    #[cfg(windows)]
    ctrl_close: tokio::signal::windows::CtrlClose,
    #[cfg(windows)]
    ctrl_shutdown: tokio::signal::windows::CtrlShutdown,
}

impl Signals {
    #[cfg(unix)]
    fn listen() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            terminate: signal(SignalKind::terminate())?,
            interrupt: signal(SignalKind::interrupt())?,
            hangup: signal(SignalKind::hangup())?,
        })
    }

    #[cfg(windows)]
    fn listen() -> std::io::Result<Self> {
        use tokio::signal::windows;
        Ok(Self {
            ctrl_c: windows::ctrl_c()?,
            ctrl_break: windows::ctrl_break()?,
            ctrl_close: windows::ctrl_close()?,
            ctrl_shutdown: windows::ctrl_shutdown()?,
        })
    }

    /// Waits for the next one; answers its name.
    #[cfg(unix)]
    async fn next(&mut self) -> &'static str {
        tokio::select! {
            _ = self.terminate.recv() => "SIGTERM",
            _ = self.interrupt.recv() => "SIGINT",
            _ = self.hangup.recv() => "SIGHUP",
        }
    }

    /// Waits for the next one; answers its name.
    #[cfg(windows)]
    async fn next(&mut self) -> &'static str {
        tokio::select! {
            _ = self.ctrl_c.recv() => "Ctrl-C",
            _ = self.ctrl_break.recv() => "Ctrl-Break",
            _ = self.ctrl_close.recv() => "the console closing",
            _ = self.ctrl_shutdown.recv() => "the system shutting down",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_log_level_is_for_our_crates_and_others_stay_at_warn_unless_named() {
        use tracing::Level;
        let would = |raw: &str, target: &str, level: Level| {
            log_targets(raw).unwrap().would_enable(target, &level)
        };
        // Unset: info for ours, warnings for everyone else.
        for ours in [
            "oswx_server",
            "oswx_server::room::change",
            "oswx_serverd",
            "oswx_core",
        ] {
            assert!(would("", ours, Level::INFO), "{ours}");
            assert!(!would("", ours, Level::DEBUG), "{ours}");
            assert!(would(" Debug ", ours, Level::DEBUG), "{ours}");
            assert!(!would("debug", ours, Level::TRACE), "{ours}");
            assert!(!would("error", ours, Level::WARN), "{ours}");
        }
        for theirs in [
            "sqlx::query",
            "hyper_util::client",
            "object_store::aws",
            "lettre",
        ] {
            assert!(would("", theirs, Level::WARN), "{theirs}");
            assert!(!would("", theirs, Level::INFO), "{theirs}");
            assert!(!would("trace", theirs, Level::INFO), "{theirs}");
        }
        // Named: exactly what was asked, for that crate.
        let asked = "debug, sqlx=info ,hyper_util=TRACE";
        assert!(would(asked, "sqlx::query", Level::INFO));
        assert!(!would(asked, "sqlx::query", Level::DEBUG));
        assert!(would(asked, "hyper_util::client", Level::TRACE));
        assert!(!would(asked, "lettre", Level::INFO));
        assert!(would(asked, "oswx_server", Level::DEBUG));
        // All of ours named as `oswx` is what was asked, whatever the level before it says.
        assert!(would("debug,oswx=trace", "oswx_server", Level::TRACE));
        assert!(would("oswx=trace,debug", "oswx_core", Level::TRACE));
        assert!(!would("debug,oswx=warn", "oswx_server", Level::INFO));
        assert!(!would("oswx=error", "oswx_serverd", Level::WARN));
        // One of ours named more exactly wins over the level for all of ours.
        assert!(would(
            "warn,oswx_server::journal=trace",
            "oswx_server::journal",
            Level::TRACE
        ));
        assert!(!would(
            "warn,oswx_server::journal=trace",
            "oswx_server::hub",
            Level::INFO
        ));
        for bad in [
            "chatty",
            "debug,sqlx",
            "sqlx=",
            "=debug",
            "sqlx=loud",
            "a b=info",
            "info;x",
        ] {
            let said = log_targets(bad).unwrap_err();
            assert!(
                said.starts_with("OSWX_COLLAB_LOG_LEVEL must be "),
                "{bad}: {said}"
            );
            assert!(!said.contains("chatty") && !said.contains("loud"), "{said}");
        }
    }

    #[test]
    fn the_command_line_is_well_formed() {
        use clap::CommandFactory as _;
        Cli::command().debug_assert();
    }

    #[test]
    fn serve_listens_on_the_python_servers_address_unless_told() {
        let cli = Cli::try_parse_from(["x", "serve"]).unwrap();
        let Command::Serve { host, port } = cli.command else {
            panic!("not serve")
        };
        assert_eq!((host.to_string().as_str(), port), ("127.0.0.1", 8787));
        let cli = Cli::try_parse_from(["x", "serve", "--host", "::", "--port", "9000"]).unwrap();
        let Command::Serve { host, port } = cli.command else {
            panic!("not serve")
        };
        assert_eq!((host.to_string().as_str(), port), ("::", 9000));
        for bad in [
            &["x", "serve", "--port", "70000"][..],
            &["x", "serve", "--host", "example.com"],
            &["x", "serve", "extra"],
            &["x", "create-admin"],
            &["x", "create-admin", "a@b.cd", "Name", "extra"],
            &["x", "reset-password"],
            &["x", "run"],
            &["x"],
        ] {
            assert!(Cli::try_parse_from(bad).is_err(), "{bad:?}");
        }
    }

    /// Every `OSWX_COLLAB_*` variable the server's settings read is in the help text.
    #[test]
    fn the_help_lists_every_setting_the_server_reads() {
        let config = include_str!("../../server/src/config.rs");
        let (reading, _) = config
            .split_once("#[cfg(test)]")
            .expect("the settings code, then its tests");
        let mut names = Vec::new();
        for (at, _) in reading.match_indices("(\"") {
            let before = &reading[..at];
            let called = before
                .rsplit(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
                .next()
                .unwrap_or_default();
            if !called.starts_with("e.") {
                continue;
            }
            let name: String = reading[at + 2..]
                .chars()
                .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
                .collect();
            if !name.is_empty() {
                names.push(name);
            }
        }
        names.sort();
        names.dedup();
        assert!(
            names.len() >= 40,
            "only {} settings were found: {names:?}",
            names.len()
        );
        for name in &names {
            let var = format!("OSWX_COLLAB_{name}");
            let listed = SETTINGS_HELP
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .any(|word| word == var);
            assert!(listed, "{var} is not in the help text");
        }
        for var in [
            LOG,
            LOG_LEVEL,
            PASSWORD,
            "AWS_SECRET_ACCESS_KEY",
            "AWS_DEFAULT_REGION",
        ] {
            assert!(SETTINGS_HELP.contains(var), "{var}");
        }
    }
}
