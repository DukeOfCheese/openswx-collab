//! Server settings, read from `OSWX_COLLAB_*` environment variables. The names and defaults are the
//! Python server's (`config.py`); see the parity inventory, section 9.

use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;

/// Every environment variable this server reads starts with this.
pub const PREFIX: &str = "OSWX_COLLAB_";

const MIB: usize = 1 << 20;

/// The region an S3 client names when none is set.
pub const DEFAULT_S3_REGION: &str = "us-east-1";

/// How long a metrics token may be, in characters.
const METRICS_TOKEN_LEN: std::ops::RangeInclusive<usize> = 16..=256;

/// The longest interval a setting in seconds may be: a year.
const MAX_SECONDS: f64 = 365.0 * 86400.0;

/// A setting that could not be read. The message names the variable and never repeats its value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{var} must be {expected}")]
pub struct ConfigError {
    /// The environment variable, e.g. `OSWX_COLLAB_SESSION_DAYS`.
    pub var: String,
    /// What it must be, e.g. `a whole number`.
    pub expected: &'static str,
}

impl ConfigError {
    fn new(name: &str, expected: &'static str) -> Self {
        Self {
            var: format!("{PREFIX}{name}"),
            expected,
        }
    }
}

/// How the mail server connection is secured (`OSWX_COLLAB_SMTP_SECURITY`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmtpSecurity {
    /// `starttls` (the default): connect in plain text, then upgrade.
    StartTls,
    /// `ssl`: TLS from the first byte.
    Ssl,
    /// `none`: no encryption.
    Plain,
}

/// Where the database is, from `OSWX_COLLAB_DATABASE_URL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Database {
    /// A SQLite file. Written `sqlite://<path>` (so `sqlite:///abs/path` is absolute), `sqlite:<path>`,
    /// or in the Python form `sqlite+aiosqlite:///<path>`.
    Sqlite(PathBuf),
    /// A PostgreSQL URL, normalised to start with `postgres://`. Written `postgres://…`,
    /// `postgresql://…` or in the Python form `postgresql+asyncpg://…`.
    Postgres(String),
}

/// Where blobs and snapshots are kept, from `OSWX_COLLAB_STORAGE_URL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Storage {
    /// A folder on this machine.
    Local(PathBuf),
    /// `s3://bucket/prefix`; the prefix is empty or has no leading or trailing `/`.
    S3 { bucket: String, prefix: String },
}

/// A range of addresses, written as one address (`10.0.0.7`, `::1`) or in CIDR form
/// (`172.16.0.0/12`, `fd00::/16`): an entry of `OSWX_COLLAB_TRUSTED_PROXIES`.
///
/// An IPv4 address written as IPv6 (`::ffff:10.0.0.7`) is the IPv4 address, in a range and in an
/// address asked about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpNet {
    /// An address of the range.
    addr: IpAddr,
    /// How many leading bits an address shares with `addr` to be in the range.
    prefix: u8,
}

impl IpNet {
    /// The range of one address.
    pub fn one(addr: IpAddr) -> Self {
        let addr = addr.to_canonical();
        let prefix = if addr.is_ipv4() { 32 } else { 128 };
        Self { addr, prefix }
    }

    /// Whether the range is too wide to be a list of proxies: wider than /8 for IPv4, /16 for
    /// IPv6 (`0.0.0.0/0`, or `0.0.0.0/1` and `128.0.0.0/1`, would let any client name its own
    /// address).
    pub fn is_too_wide(&self) -> bool {
        self.prefix < if self.addr.is_ipv4() { 8 } else { 16 }
    }

    /// Whether `ip` is in the range.
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip.to_canonical()) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                leading(u128::from(net.to_bits()), self.prefix, 32)
                    == leading(u128::from(ip.to_bits()), self.prefix, 32)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                leading(net.to_bits(), self.prefix, 128) == leading(ip.to_bits(), self.prefix, 128)
            }
            _ => false,
        }
    }
}

/// The first `prefix` bits of an address of `width` bits.
fn leading(bits: u128, prefix: u8, width: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        bits >> (width - prefix.min(width))
    }
}

/// (The error says nothing: [`Settings::from_env`] names the variable.)
impl FromStr for IpNet {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        let (addr, prefix) = match s.split_once('/') {
            Some((addr, prefix)) => (addr, Some(prefix)),
            None => (s, None),
        };
        let addr: IpAddr = addr.parse().map_err(|_| ())?;
        let width: u8 = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            // (Digits only: `parse` alone would take `+8`.)
            Some(p) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => {
                p.parse::<u8>().ok().filter(|p| *p <= width).ok_or(())?
            }
            Some(_) => return Err(()),
            None => width,
        };
        // `::ffff:a.b.c.d/n` is the IPv4 range `a.b.c.d/(n - 96)`.
        match (addr, addr.to_canonical()) {
            (IpAddr::V6(_), v4 @ IpAddr::V4(_)) if prefix >= 96 => Ok(Self {
                addr: v4,
                prefix: prefix - 96,
            }),
            _ => Ok(Self { addr, prefix }),
        }
    }
}

/// The server's settings. `Default` gives the Python server's defaults.
#[derive(Clone, PartialEq)]
pub struct Settings {
    pub database_url: String,
    /// The most connections the server holds to a PostgreSQL database (SQLite has its own fixed
    /// few).
    pub database_pool: u32,
    /// The connections to a PostgreSQL database the journal writer has to itself, on top of
    /// `database_pool`.
    pub database_journal_pool: u32,
    /// Seconds a request's transaction waits for another's row lock on PostgreSQL before it is
    /// answered 409 "try again".
    pub database_lock_timeout: f64,
    /// Seconds the journal writer's transaction waits for a row lock on PostgreSQL before it
    /// writes the batch again one document at a time.
    pub database_journal_lock_timeout: f64,
    pub storage_url: String,
    /// For S3-compatible stores (MinIO, Garage, R2), e.g. `http://minio:9000`. Empty: AWS S3.
    pub s3_endpoint: String,
    /// The bucket's region, from `AWS_DEFAULT_REGION` (or `AWS_REGION`), as the Python server's S3
    /// client read it. Stores that have no regions take any name; `us-east-1` when unset.
    pub s3_region: String,
    /// The S3 credentials, from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and (temporary
    /// credentials only) `AWS_SESSION_TOKEN`. With no key set, the machine's own AWS role is used.
    pub s3_access_key_id: String,
    pub s3_secret_access_key: String,
    pub s3_session_token: String,
    /// Whether the bucket goes in the path (`https://host/bucket/key`) rather than the host name
    /// (`https://bucket.host/key`). Unset: in the path exactly when `s3_endpoint` is set, which is
    /// what MinIO and Garage want and what the Python server's S3 client did.
    pub s3_path_style: Option<bool>,
    /// Seconds: a blob upload or download in which no bytes move for this long is given up on.
    pub blob_idle_timeout: f64,
    /// Bytes of memory the blob uploads running at once may hold between them. Each is charged
    /// what its storage holds for one ([`crate::blobs::Blobs::upload_memory`]: about 44 MiB for
    /// S3, 4 MiB for a folder); one that does not fit is answered 503 after a short wait.
    pub upload_memory: u64,
    /// The most blob uploads one organisation may have running at once (at most as many as fit
    /// in `upload_memory`); one more is answered 429.
    pub max_blob_uploads_per_org: usize,
    /// The most blob uploads one account may have running at once; one more is answered 429.
    pub max_blob_uploads_per_user: usize,
    /// Bytes a second a request body (a blob upload, a document being created, any other) must
    /// keep up on average ([`crate::blobs::Pace`]); 0: no floor.
    pub min_upload_rate: u64,
    /// Seconds of slack a body has against `min_upload_rate`: how long it may take to start, or
    /// pause once it has earned the time back, before it is given up on.
    pub upload_rate_grace: f64,
    /// Seconds a blob upload may take in all, at least: an upload of `n` bytes may take
    /// `max(upload_time_limit, n / upload_time_rate)`, then it is answered 400.
    pub upload_time_limit: f64,
    /// Bytes a second, for the time a large upload may take (`upload_time_limit`).
    pub upload_time_rate: u64,
    /// The most connections the server holds open at once (document connections included). When
    /// it has them, a new one takes the place of the one that has waited longest without
    /// finishing its first request (or failing that, the one idle longest); with none of those,
    /// the new one is closed at once. A few more are kept for `GET /api/health` alone.
    pub max_connections: usize,
    /// The most connections one client address may hold open at once (not counting addresses of
    /// `trusted_proxies`); one more is closed at once. 0: no limit.
    pub max_connections_per_address: usize,
    /// The most request bodies larger than the default limit (a document being created, up to
    /// 8 MB) the server reads at once; one more is answered 503 at once.
    pub max_large_bodies: usize,
    /// The most emails being sent at once; one more is not sent (an invite says `emailed:
    /// false`).
    pub max_mail_sends: usize,
    /// Seconds a request may take from its head, or from the last of its body, to the beginning
    /// of its answer; a longer one is answered 503. A body that is arriving at `min_upload_rate`
    /// or faster does not count against it. Blob uploads are not under it (they have
    /// `blob_idle_timeout` and the same minimum rate), and neither is the sending of an answer (a
    /// download, an export) nor an open document connection.
    pub request_timeout: f64,
    /// Seconds: a connection whose client takes nothing of what the server is sending it for this
    /// long is closed (a stalled download or export, a document connection that stopped reading).
    pub write_timeout: f64,
    /// Seconds an orderly stop (Ctrl-C, SIGTERM) may take before the process ends anyway. Below
    /// the ten seconds `docker stop` waits before it kills.
    pub shutdown_timeout: f64,
    /// The proxies whose `X-Forwarded-For` is believed, as addresses and CIDR ranges
    /// (`OSWX_COLLAB_TRUSTED_PROXIES`, separated by commas or spaces). The sign-in limits are kept
    /// per client address: behind a proxy that is not listed here, every client shares one.
    pub trusted_proxies: Vec<IpNet>,
    /// `GET /metrics` answers only requests that carry this as a bearer token; empty (the
    /// default): there is no `/metrics` (404).
    pub metrics_token: String,
    /// How clients and browsers reach this server (OAuth callbacks, invite and reset links).
    pub public_url: String,
    /// False: the first account is the server admin and everyone else needs an invite.
    pub open_registration: bool,
    pub session_days: u32,
    /// Upgrade the database schema on start; off: refuse to start until `migrate` is run.
    pub auto_migrate: bool,
    /// A full copy of the document is stored every this many changes.
    pub snapshot_every: u32,
    /// Seconds: a connected client that stays silent this long loses its locks.
    pub lock_ttl: f64,
    /// Seconds: a disconnected client keeps its locks this long.
    pub lock_grace: f64,
    /// Seconds between sweeps of open rooms.
    pub sweep_interval: f64,
    /// Seconds: how often open connections are checked against sessions and roles.
    pub access_recheck: f64,
    /// Seconds: an open document nobody is connected to is unloaded after this.
    pub room_idle: f64,
    /// Reconnecting clients further behind than this many changes get the whole document.
    pub catchup_limit: usize,
    pub max_change_bytes: usize,
    pub max_blob_bytes: usize,
    /// Presence and draft messages per second per client before they are dropped.
    pub presence_rate: u32,
    /// The most document connections (WebSockets) one account may have open at once, across the
    /// whole server; one more is refused (close 4429).
    pub max_connections_per_user: usize,
    /// Email is sent only when this and `smtp_from` are set.
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_security: SmtpSecurity,
    pub smtp_user: String,
    pub smtp_password: String,
    /// The sender address.
    pub smtp_from: String,
    pub smtp_from_name: String,
    /// Minutes a password reset link works.
    pub reset_minutes: u32,
    pub google_client_id: String,
    pub google_client_secret: String,
    pub github_client_id: String,
    pub github_client_secret: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            database_url: "sqlite+aiosqlite:///./data/collab.db".into(),
            database_pool: crate::store::postgres::DEFAULT_POOL,
            database_journal_pool: crate::store::postgres::DEFAULT_JOURNAL_POOL,
            database_lock_timeout: crate::store::postgres::DEFAULT_LOCK_TIMEOUT,
            database_journal_lock_timeout: crate::store::postgres::DEFAULT_JOURNAL_LOCK_TIMEOUT,
            storage_url: "./data/storage".into(),
            s3_endpoint: String::new(),
            s3_region: DEFAULT_S3_REGION.into(),
            s3_access_key_id: String::new(),
            s3_secret_access_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: None,
            blob_idle_timeout: 60.0,
            // Sixteen uploads to S3 at once (44 MiB each), as many to a folder as people send.
            upload_memory: (704 * MIB) as u64,
            max_blob_uploads_per_org: 8,
            max_blob_uploads_per_user: 4,
            min_upload_rate: 4096,
            upload_rate_grace: 30.0,
            upload_time_limit: 600.0,
            upload_time_rate: 64 << 10,
            max_connections: 4096,
            max_connections_per_address: 512,
            max_large_bodies: 16,
            max_mail_sends: 16,
            request_timeout: 60.0,
            write_timeout: 60.0,
            shutdown_timeout: 8.0,
            // A proxy on this machine, over IPv4 or IPv6. (uvicorn, and so the Python server,
            // believed 127.0.0.1.)
            trusted_proxies: vec![
                IpNet::one(IpAddr::from([127, 0, 0, 1])),
                IpNet::one(IpAddr::from(std::net::Ipv6Addr::LOCALHOST)),
            ],
            metrics_token: String::new(),
            public_url: "http://localhost:8787".into(),
            open_registration: false,
            session_days: 30,
            auto_migrate: true,
            snapshot_every: 200,
            lock_ttl: 45.0,
            lock_grace: 60.0,
            sweep_interval: 5.0,
            access_recheck: 30.0,
            room_idle: 120.0,
            catchup_limit: 500,
            max_change_bytes: 8 * MIB,
            max_blob_bytes: 256 * MIB,
            presence_rate: 60,
            max_connections_per_user: 64,
            smtp_host: String::new(),
            smtp_port: 587,
            smtp_security: SmtpSecurity::StartTls,
            smtp_user: String::new(),
            smtp_password: String::new(),
            smtp_from: String::new(),
            smtp_from_name: "openswx collaboration".into(),
            reset_minutes: 60,
            google_client_id: String::new(),
            google_client_secret: String::new(),
            github_client_id: String::new(),
            github_client_secret: String::new(),
        }
    }
}

impl Settings {
    /// Reads the settings from environment variables, starting from the defaults. Variables that are
    /// not settings are ignored; a setting that cannot be read is an error naming the variable.
    pub fn from_env(env: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let e = Env(env);
        let mut s = Self::default();
        e.string("DATABASE_URL", &mut s.database_url);
        e.positive_whole("DATABASE_POOL", &mut s.database_pool)?;
        e.positive_whole("DATABASE_JOURNAL_POOL", &mut s.database_journal_pool)?;
        e.positive_seconds("DATABASE_LOCK_TIMEOUT", &mut s.database_lock_timeout)?;
        e.positive_seconds(
            "DATABASE_JOURNAL_LOCK_TIMEOUT",
            &mut s.database_journal_lock_timeout,
        )?;
        // A lock wait as long as the statement timeout would never be the one to end; and the
        // journal's must leave a batch time to be written again.
        if s.database_lock_timeout > crate::store::postgres::MAX_LOCK_TIMEOUT {
            return Err(ConfigError::new(
                "DATABASE_LOCK_TIMEOUT",
                "a number of seconds above zero and at most 20 (statements end after 30)",
            ));
        }
        if s.database_journal_lock_timeout > crate::store::postgres::MAX_JOURNAL_LOCK_TIMEOUT {
            return Err(ConfigError::new(
                "DATABASE_JOURNAL_LOCK_TIMEOUT",
                "a number of seconds above zero and at most 7.5 (a quarter of a journal batch's \
                 time)",
            ));
        }
        e.string("STORAGE_URL", &mut s.storage_url);
        e.string("S3_ENDPOINT", &mut s.s3_endpoint);
        if e.get("S3_PATH_STYLE").is_some() {
            let mut path_style = false;
            e.boolean("S3_PATH_STYLE", &mut path_style)?;
            s.s3_path_style = Some(path_style);
        }
        e.positive_seconds("BLOB_IDLE_TIMEOUT", &mut s.blob_idle_timeout)?;
        e.positive_whole("UPLOAD_MEMORY", &mut s.upload_memory)?;
        e.positive_whole(
            "MAX_BLOB_UPLOADS_PER_USER",
            &mut s.max_blob_uploads_per_user,
        )?;
        e.positive_whole("MAX_BLOB_UPLOADS_PER_ORG", &mut s.max_blob_uploads_per_org)?;
        e.whole("MIN_UPLOAD_RATE", &mut s.min_upload_rate)?;
        e.positive_seconds("UPLOAD_RATE_GRACE", &mut s.upload_rate_grace)?;
        e.positive_seconds("UPLOAD_TIME_LIMIT", &mut s.upload_time_limit)?;
        e.positive_whole("UPLOAD_TIME_RATE", &mut s.upload_time_rate)?;
        e.positive_whole("MAX_CONNECTIONS", &mut s.max_connections)?;
        e.whole(
            "MAX_CONNECTIONS_PER_ADDRESS",
            &mut s.max_connections_per_address,
        )?;
        e.positive_whole("MAX_LARGE_BODIES", &mut s.max_large_bodies)?;
        e.positive_whole("MAX_MAIL_SENDS", &mut s.max_mail_sends)?;
        e.positive_seconds("REQUEST_TIMEOUT", &mut s.request_timeout)?;
        e.positive_seconds("WRITE_TIMEOUT", &mut s.write_timeout)?;
        e.positive_seconds("SHUTDOWN_TIMEOUT", &mut s.shutdown_timeout)?;
        if let Some(raw) = e.get("TRUSTED_PROXIES") {
            s.trusted_proxies = raw
                .split(|c: char| c == ',' || c.is_whitespace())
                .filter(|entry| !entry.is_empty())
                .map(str::parse)
                .collect::<Result<_, ()>>()
                .map_err(|()| {
                    ConfigError::new(
                        "TRUSTED_PROXIES",
                        "a list of addresses and CIDR ranges (e.g. `127.0.0.1, 172.16.0.0/12`)",
                    )
                })?;
            // "Everybody is a trusted proxy" lets any client name its own address.
            if s.trusted_proxies.iter().any(IpNet::is_too_wide) {
                return Err(ConfigError::new(
                    "TRUSTED_PROXIES",
                    "a list of your proxies' addresses and ranges, none wider than /8 (IPv4) or \
                     /16 (IPv6)",
                ));
            }
        }
        if let Some(raw) = e.get("METRICS_TOKEN") {
            let token = raw.trim();
            // (It travels in a header; and a short one could be guessed.)
            let fits = token.is_empty()
                || (METRICS_TOKEN_LEN.contains(&token.len())
                    && token.bytes().all(|b| b.is_ascii_graphic()));
            if !fits {
                return Err(ConfigError::new(
                    "METRICS_TOKEN",
                    "16 to 256 printable ASCII characters without spaces (or empty: no /metrics)",
                ));
            }
            token.clone_into(&mut s.metrics_token);
        }
        // The S3 credentials and region have the names every S3 tool reads (and the Python
        // server's client read), without this server's prefix.
        let aws = |name: &str| env.get(name).map(|v| v.trim()).filter(|v| !v.is_empty());
        if let Some(region) = aws("AWS_DEFAULT_REGION").or_else(|| aws("AWS_REGION")) {
            s.s3_region = region.to_owned();
        }
        for (name, out) in [
            ("AWS_ACCESS_KEY_ID", &mut s.s3_access_key_id),
            ("AWS_SECRET_ACCESS_KEY", &mut s.s3_secret_access_key),
            ("AWS_SESSION_TOKEN", &mut s.s3_session_token),
        ] {
            if let Some(value) = aws(name) {
                *out = value.to_owned();
            }
        }
        e.string("PUBLIC_URL", &mut s.public_url);
        e.boolean("OPEN_REGISTRATION", &mut s.open_registration)?;
        e.positive_whole("SESSION_DAYS", &mut s.session_days)?;
        e.boolean("AUTO_MIGRATE", &mut s.auto_migrate)?;
        e.positive_whole("SNAPSHOT_EVERY", &mut s.snapshot_every)?;
        e.positive_seconds("LOCK_TTL", &mut s.lock_ttl)?;
        e.positive_seconds("LOCK_GRACE", &mut s.lock_grace)?;
        e.positive_seconds("SWEEP_INTERVAL", &mut s.sweep_interval)?;
        e.positive_seconds("ACCESS_RECHECK", &mut s.access_recheck)?;
        e.positive_seconds("ROOM_IDLE", &mut s.room_idle)?;
        e.positive_whole("CATCHUP_LIMIT", &mut s.catchup_limit)?;
        e.positive_whole("MAX_CHANGE_BYTES", &mut s.max_change_bytes)?;
        e.positive_whole("MAX_BLOB_BYTES", &mut s.max_blob_bytes)?;
        e.positive_whole("PRESENCE_RATE", &mut s.presence_rate)?;
        e.positive_whole("MAX_CONNECTIONS_PER_USER", &mut s.max_connections_per_user)?;
        e.string("SMTP_HOST", &mut s.smtp_host);
        e.whole("SMTP_PORT", &mut s.smtp_port)?;
        if let Some(raw) = e.get("SMTP_SECURITY") {
            s.smtp_security = match raw.trim().to_ascii_lowercase().as_str() {
                "starttls" => SmtpSecurity::StartTls,
                "ssl" => SmtpSecurity::Ssl,
                "none" => SmtpSecurity::Plain,
                _ => {
                    return Err(ConfigError::new("SMTP_SECURITY", "starttls, ssl or none"));
                }
            };
        }
        e.string("SMTP_USER", &mut s.smtp_user);
        e.string("SMTP_PASSWORD", &mut s.smtp_password);
        e.string("SMTP_FROM", &mut s.smtp_from);
        e.string("SMTP_FROM_NAME", &mut s.smtp_from_name);
        e.positive_whole("RESET_MINUTES", &mut s.reset_minutes)?;
        e.string("GOOGLE_CLIENT_ID", &mut s.google_client_id);
        e.string("GOOGLE_CLIENT_SECRET", &mut s.google_client_secret);
        e.string("GITHUB_CLIENT_ID", &mut s.github_client_id);
        e.string("GITHUB_CLIENT_SECRET", &mut s.github_client_secret);
        s.database()?;
        if let Storage::S3 { .. } = s.storage()? {
            s.check_s3()?;
        }
        Ok(s)
    }

    /// What can be told of the S3 settings without asking the store (the rest is found out when
    /// the server starts and tries the bucket): an endpoint is an `http://` or `https://`
    /// address without credentials in it, and the access key and its secret come together.
    fn check_s3(&self) -> Result<(), ConfigError> {
        let endpoint = self.s3_endpoint.trim();
        if !endpoint.is_empty() {
            let usable = url::Url::parse(endpoint).is_ok_and(|url| {
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
            });
            if !usable {
                return Err(ConfigError::new(
                    "S3_ENDPOINT",
                    "the store's address, as http://host[:port] or https://host[:port]",
                ));
            }
        }
        let other = |var: &str, expected: &'static str| ConfigError {
            var: var.to_owned(),
            expected,
        };
        match (
            self.s3_access_key_id.is_empty(),
            self.s3_secret_access_key.is_empty(),
        ) {
            (false, true) => Err(other(
                "AWS_SECRET_ACCESS_KEY",
                "set when AWS_ACCESS_KEY_ID is (both, or neither to use the machine's own AWS role)",
            )),
            (true, false) => Err(other(
                "AWS_ACCESS_KEY_ID",
                "set when AWS_SECRET_ACCESS_KEY is (both, or neither to use the machine's own AWS \
                 role)",
            )),
            (true, true) if !self.s3_session_token.is_empty() => Err(other(
                "AWS_SESSION_TOKEN",
                "left unset unless AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set",
            )),
            _ => Ok(()),
        }
    }

    /// [`Settings::from_env`] over this process's environment.
    pub fn from_process_env() -> Result<Self, ConfigError> {
        Self::from_env(&std::env::vars().collect())
    }

    /// The configured sign-in providers, in the order clients show them: Google, then GitHub. A
    /// provider counts only when both its client id and secret are set.
    pub fn oauth_providers(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if !self.google_client_id.is_empty() && !self.google_client_secret.is_empty() {
            out.push("google");
        }
        if !self.github_client_id.is_empty() && !self.github_client_secret.is_empty() {
            out.push("github");
        }
        out
    }

    /// Whether email can be sent: a mail server and a sender address are set.
    pub fn email_configured(&self) -> bool {
        !self.smtp_host.is_empty() && !self.smtp_from.is_empty()
    }

    /// The database, parsed from `database_url`.
    pub fn database(&self) -> Result<Database, ConfigError> {
        parse_database_url(&self.database_url)
    }

    /// The blob storage, parsed from `storage_url`.
    pub fn storage(&self) -> Result<Storage, ConfigError> {
        let url = self.storage_url.trim();
        if let Some(rest) = url.strip_prefix("s3://") {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                return Err(ConfigError::new(
                    "STORAGE_URL",
                    "a folder or s3://<bucket>/<prefix>",
                ));
            }
            return Ok(Storage::S3 {
                bucket: bucket.to_owned(),
                prefix: prefix.trim_matches('/').to_owned(),
            });
        }
        if url.is_empty() {
            return Err(ConfigError::new(
                "STORAGE_URL",
                "a folder or s3://<bucket>/<prefix>",
            ));
        }
        Ok(Storage::Local(PathBuf::from(url)))
    }
}

/// Parses a database URL as `OSWX_COLLAB_DATABASE_URL` takes it:
///
/// - `sqlite://<path>` or `sqlite:<path>` (sqlx forms, so `sqlite:///abs/path` is absolute);
/// - `sqlite+aiosqlite:///<path>` (the Python form, so `sqlite+aiosqlite:////abs/path` is absolute);
/// - `postgres://…`, `postgresql://…` or `postgresql+asyncpg://…`, normalised to `postgres://…`.
///
/// An in-memory SQLite database (`sqlite::memory:`) is refused: every pooled connection would see a
/// different, empty database. So is a SQLite path with a query string (`?mode=…`): the store sets
/// its own connection options.
pub fn parse_database_url(url: &str) -> Result<Database, ConfigError> {
    let bad = || ConfigError::new("DATABASE_URL", "sqlite://<path> or postgres://<url>");
    let url = url.trim();
    // The Python form first: after `sqlite+aiosqlite:///` comes the path (`////abs` is absolute).
    let sqlite = url
        .strip_prefix("sqlite+aiosqlite:///")
        .or_else(|| url.strip_prefix("sqlite://"))
        .or_else(|| url.strip_prefix("sqlite:"));
    if let Some(path) = sqlite {
        if path.contains('?') {
            return Err(ConfigError::new(
                "DATABASE_URL",
                "a SQLite path without a query string (`?…` options are not supported)",
            ));
        }
        return if path.is_empty() || path.starts_with(':') {
            Err(bad())
        } else {
            Ok(Database::Sqlite(PathBuf::from(path)))
        };
    }
    let postgres = ["postgresql+asyncpg://", "postgresql://", "postgres://"]
        .iter()
        .find_map(|p| url.strip_prefix(p));
    match postgres {
        Some(rest) if !rest.is_empty() => Ok(Database::Postgres(format!("postgres://{rest}"))),
        _ => Err(bad()),
    }
}

/// Secrets (passwords, client secrets, the database password) are never printed.
impl fmt::Debug for Settings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn secret(s: &str) -> &'static str {
            if s.is_empty() { "" } else { "<redacted>" }
        }
        f.debug_struct("Settings")
            .field("database_url", &redact_url_password(&self.database_url))
            .field("database_pool", &self.database_pool)
            .field("database_journal_pool", &self.database_journal_pool)
            .field("database_lock_timeout", &self.database_lock_timeout)
            .field(
                "database_journal_lock_timeout",
                &self.database_journal_lock_timeout,
            )
            .field("storage_url", &self.storage_url)
            .field("s3_endpoint", &self.s3_endpoint)
            .field("s3_region", &self.s3_region)
            .field("s3_access_key_id", &secret(&self.s3_access_key_id))
            .field("s3_secret_access_key", &secret(&self.s3_secret_access_key))
            .field("s3_session_token", &secret(&self.s3_session_token))
            .field("s3_path_style", &self.s3_path_style)
            .field("blob_idle_timeout", &self.blob_idle_timeout)
            .field("upload_memory", &self.upload_memory)
            .field("max_blob_uploads_per_org", &self.max_blob_uploads_per_org)
            .field("max_blob_uploads_per_user", &self.max_blob_uploads_per_user)
            .field("min_upload_rate", &self.min_upload_rate)
            .field("upload_rate_grace", &self.upload_rate_grace)
            .field("upload_time_limit", &self.upload_time_limit)
            .field("upload_time_rate", &self.upload_time_rate)
            .field("max_connections", &self.max_connections)
            .field(
                "max_connections_per_address",
                &self.max_connections_per_address,
            )
            .field("max_large_bodies", &self.max_large_bodies)
            .field("max_mail_sends", &self.max_mail_sends)
            .field("request_timeout", &self.request_timeout)
            .field("write_timeout", &self.write_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("trusted_proxies", &self.trusted_proxies)
            .field("metrics_token", &secret(&self.metrics_token))
            .field("public_url", &self.public_url)
            .field("open_registration", &self.open_registration)
            .field("session_days", &self.session_days)
            .field("auto_migrate", &self.auto_migrate)
            .field("snapshot_every", &self.snapshot_every)
            .field("lock_ttl", &self.lock_ttl)
            .field("lock_grace", &self.lock_grace)
            .field("sweep_interval", &self.sweep_interval)
            .field("access_recheck", &self.access_recheck)
            .field("room_idle", &self.room_idle)
            .field("catchup_limit", &self.catchup_limit)
            .field("max_change_bytes", &self.max_change_bytes)
            .field("max_blob_bytes", &self.max_blob_bytes)
            .field("presence_rate", &self.presence_rate)
            .field("max_connections_per_user", &self.max_connections_per_user)
            .field("smtp_host", &self.smtp_host)
            .field("smtp_port", &self.smtp_port)
            .field("smtp_security", &self.smtp_security)
            .field("smtp_user", &self.smtp_user)
            .field("smtp_password", &secret(&self.smtp_password))
            .field("smtp_from", &self.smtp_from)
            .field("smtp_from_name", &self.smtp_from_name)
            .field("reset_minutes", &self.reset_minutes)
            .field("google_client_id", &self.google_client_id)
            .field("google_client_secret", &secret(&self.google_client_secret))
            .field("github_client_id", &self.github_client_id)
            .field("github_client_secret", &secret(&self.github_client_secret))
            .finish()
    }
}

/// `scheme://user:password@host/…` → `scheme://user:<redacted>@host/…`; anything else unchanged.
fn redact_url_password(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let Some(at) = rest[..authority_end].rfind('@') else {
        return url.to_owned();
    };
    match rest[..at].split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:<redacted>{}", &rest[at..]),
        None => url.to_owned(),
    }
}

/// Reads `OSWX_COLLAB_<NAME>` variables into settings fields.
struct Env<'a>(&'a HashMap<String, String>);

impl Env<'_> {
    fn get(&self, name: &str) -> Option<&str> {
        self.0.get(&format!("{PREFIX}{name}")).map(String::as_str)
    }

    fn string(&self, name: &str, out: &mut String) {
        if let Some(raw) = self.get(name) {
            raw.clone_into(out);
        }
    }

    /// `1`, `true`, `yes`, `on` are true and `0`, `false`, `no`, `off` false (any case, surrounding
    /// space ignored); anything else is an error.
    fn boolean(&self, name: &str, out: &mut bool) -> Result<(), ConfigError> {
        if let Some(raw) = self.get(name) {
            *out = match raw.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => true,
                "0" | "false" | "no" | "off" => false,
                _ => {
                    return Err(ConfigError::new(
                        name,
                        "true or false (1, 0, yes, no, on, off)",
                    ));
                }
            };
        }
        Ok(())
    }

    fn whole<T: FromStr>(&self, name: &str, out: &mut T) -> Result<(), ConfigError> {
        if let Some(raw) = self.get(name) {
            *out = raw
                .trim()
                .parse()
                .map_err(|_| ConfigError::new(name, "a whole number, not negative"))?;
        }
        Ok(())
    }

    /// A whole number above zero.
    fn positive_whole<T: FromStr + Default + PartialEq>(
        &self,
        name: &str,
        out: &mut T,
    ) -> Result<(), ConfigError> {
        let Some(raw) = self.get(name) else {
            return Ok(());
        };
        match raw.trim().parse::<T>() {
            Ok(v) if v != T::default() => {
                *out = v;
                Ok(())
            }
            _ => Err(ConfigError::new(name, "a whole number above zero")),
        }
    }

    /// A number of seconds above zero and at most [`MAX_SECONDS`] (so converting to a `Duration`
    /// cannot panic).
    fn positive_seconds(&self, name: &str, out: &mut f64) -> Result<(), ConfigError> {
        if let Some(raw) = self.get(name) {
            *out = raw
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|v| *v > 0.0 && *v <= MAX_SECONDS)
                .ok_or_else(|| {
                    ConfigError::new(name, "a number of seconds above zero, at most a year")
                })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (format!("{PREFIX}{k}"), (*v).to_owned()))
            .collect()
    }

    fn read(pairs: &[(&str, &str)]) -> Settings {
        match Settings::from_env(&env(pairs)) {
            Ok(s) => s,
            Err(e) => panic!("{e}"),
        }
    }

    fn refused(pairs: &[(&str, &str)]) -> ConfigError {
        match Settings::from_env(&env(pairs)) {
            Ok(_) => panic!("{pairs:?} was accepted"),
            Err(e) => e,
        }
    }

    #[test]
    fn no_variables_give_the_python_defaults() {
        let s = read(&[]);
        assert!(s == Settings::default());
        assert_eq!(s.database_url, "sqlite+aiosqlite:///./data/collab.db");
        assert_eq!(s.storage_url, "./data/storage");
        assert_eq!(s.public_url, "http://localhost:8787");
        assert!(!s.open_registration && s.auto_migrate);
        assert_eq!(
            (s.session_days, s.snapshot_every, s.catchup_limit),
            (30, 200, 500)
        );
        assert_eq!(
            (s.lock_ttl, s.lock_grace, s.sweep_interval),
            (45.0, 60.0, 5.0)
        );
        assert_eq!((s.access_recheck, s.room_idle), (30.0, 120.0));
        assert_eq!((s.max_change_bytes, s.max_blob_bytes), (8 << 20, 256 << 20));
        assert_eq!(
            (s.presence_rate, s.smtp_port, s.reset_minutes),
            (60, 587, 60)
        );
        assert_eq!(s.smtp_security, SmtpSecurity::StartTls);
        assert_eq!(s.max_connections_per_user, 64);
        assert_eq!(s.smtp_from_name, "openswx collaboration");
        assert_eq!(
            s.database().ok(),
            Some(Database::Sqlite("./data/collab.db".into()))
        );
        assert_eq!(
            s.storage().ok(),
            Some(Storage::Local("./data/storage".into()))
        );
        assert!(s.oauth_providers().is_empty() && !s.email_configured());
    }

    #[test]
    fn every_type_is_parsed() {
        let s = read(&[
            ("PUBLIC_URL", "https://collab.example.com"),
            ("OPEN_REGISTRATION", " Yes "),
            ("AUTO_MIGRATE", "off"),
            ("SESSION_DAYS", " 7 "),
            ("MAX_BLOB_BYTES", "1048576"),
            ("LOCK_TTL", "2.5"),
            ("ROOM_IDLE", "10"),
            ("MAX_CONNECTIONS_PER_USER", "3"),
            ("SMTP_PORT", "465"),
            ("SMTP_SECURITY", "SSL"),
            ("SMTP_PASSWORD", "pw"),
        ]);
        assert_eq!(s.public_url, "https://collab.example.com");
        assert!(s.open_registration);
        assert!(!s.auto_migrate);
        assert_eq!(s.session_days, 7);
        assert_eq!(s.max_blob_bytes, 1 << 20);
        assert_eq!((s.lock_ttl, s.room_idle), (2.5, 10.0));
        assert_eq!(s.max_connections_per_user, 3);
        assert_eq!(s.smtp_port, 465);
        assert_eq!(s.smtp_security, SmtpSecurity::Ssl);
        assert_eq!(s.smtp_password, "pw");
        for (raw, want) in [
            ("1", true),
            ("TRUE", true),
            ("on", true),
            ("0", false),
            (" No ", false),
            ("OFF", false),
            ("false", false),
        ] {
            assert_eq!(
                read(&[("OPEN_REGISTRATION", raw)]).open_registration,
                want,
                "{raw}"
            );
        }
        assert_eq!(
            read(&[("SMTP_SECURITY", "none")]).smtp_security,
            SmtpSecurity::Plain
        );
    }

    #[test]
    fn a_boolean_must_be_one_of_the_known_spellings() {
        for var in ["OPEN_REGISTRATION", "AUTO_MIGRATE"] {
            for bad in ["nah", "", "2", "y", "truee", "enabled"] {
                let e = refused(&[(var, bad)]);
                assert_eq!(e.var, format!("{PREFIX}{var}"), "{bad:?}");
                if bad.len() > 3 {
                    assert!(!e.to_string().contains(bad), "{e}");
                }
            }
        }
    }

    #[test]
    fn limits_and_intervals_must_be_above_zero() {
        for var in [
            "SNAPSHOT_EVERY",
            "SWEEP_INTERVAL",
            "PRESENCE_RATE",
            "CATCHUP_LIMIT",
            "LOCK_TTL",
            "LOCK_GRACE",
            "ROOM_IDLE",
            "ACCESS_RECHECK",
            "SESSION_DAYS",
            "RESET_MINUTES",
            "MAX_CHANGE_BYTES",
            "MAX_BLOB_BYTES",
            "MAX_CONNECTIONS_PER_USER",
        ] {
            for bad in ["0", "0.0", "-1", "-0.5", "", "x"] {
                assert_eq!(
                    refused(&[(var, bad)]).var,
                    format!("{PREFIX}{var}"),
                    "{var}={bad:?}"
                );
            }
            read(&[(var, "1")]);
        }
        for var in [
            "SWEEP_INTERVAL",
            "LOCK_TTL",
            "LOCK_GRACE",
            "ROOM_IDLE",
            "ACCESS_RECHECK",
        ] {
            for bad in ["1e300", "31536001", "inf"] {
                assert_eq!(
                    refused(&[(var, bad)]).var,
                    format!("{PREFIX}{var}"),
                    "{bad}"
                );
            }
            read(&[(var, "31536000")]);
        }
    }

    #[test]
    fn the_s3_settings_are_checked_when_the_storage_is_a_bucket() {
        let with = |extra: &[(&str, &str)]| {
            let mut vars = env(&[("STORAGE_URL", "s3://bucket/prefix")]);
            for (name, value) in extra {
                vars.insert((*name).to_owned(), (*value).to_owned());
            }
            Settings::from_env(&vars)
        };
        assert!(with(&[]).is_ok());
        assert!(
            with(&[
                ("OSWX_COLLAB_S3_ENDPOINT", " https://s3.example.com:9000/ "),
                ("AWS_ACCESS_KEY_ID", "key"),
                ("AWS_SECRET_ACCESS_KEY", "secret"),
                ("AWS_SESSION_TOKEN", "token"),
            ])
            .is_ok()
        );
        assert!(with(&[("OSWX_COLLAB_S3_ENDPOINT", "http://garage:3900")]).is_ok());
        for bad in [
            "garage:3900",
            "s3.example.com",
            "ftp://s3.example.com",
            "https://",
            "https://key:topsecret-in-the-address@s3.example.com",
            "https://s3.example.com/?X-Amz-Signature=topsecret",
            "not an address at all",
        ] {
            let e = with(&[("OSWX_COLLAB_S3_ENDPOINT", bad)]).unwrap_err();
            assert_eq!(e.var, "OSWX_COLLAB_S3_ENDPOINT", "{bad}");
            assert!(!e.to_string().contains("topsecret"), "{e}");
        }
        // Half a pair of credentials is a mistake, not a wish for the machine's own role.
        let e = with(&[("AWS_ACCESS_KEY_ID", "AKIA-the-key-id")]).unwrap_err();
        assert_eq!(e.var, "AWS_SECRET_ACCESS_KEY");
        assert!(!e.to_string().contains("AKIA-the-key-id"), "{e}");
        let e = with(&[("AWS_SECRET_ACCESS_KEY", "topsecret-value")]).unwrap_err();
        assert_eq!(e.var, "AWS_ACCESS_KEY_ID");
        assert!(!e.to_string().contains("topsecret"), "{e}");
        let e = with(&[("AWS_SESSION_TOKEN", "topsecret-token")]).unwrap_err();
        assert_eq!(e.var, "AWS_SESSION_TOKEN");
        assert!(!e.to_string().contains("topsecret"), "{e}");
        // None of that is looked at for a folder: the variables may be there for something else.
        let mut vars = env(&[("S3_ENDPOINT", "nonsense")]);
        vars.insert("AWS_ACCESS_KEY_ID".into(), "key".into());
        assert!(Settings::from_env(&vars).is_ok());
    }

    #[test]
    fn the_serving_limits_have_defaults_and_are_read() {
        let s = read(&[]);
        assert_eq!(
            (s.upload_memory, s.max_blob_uploads_per_user),
            (704 << 20, 4)
        );
        assert_eq!((s.max_blob_uploads_per_org, s.max_connections), (8, 4096));
        assert_eq!((s.min_upload_rate, s.upload_rate_grace), (4096, 30.0));
        assert_eq!((s.upload_time_limit, s.upload_time_rate), (600.0, 65536));
        assert_eq!(
            (
                s.max_connections_per_address,
                s.max_large_bodies,
                s.max_mail_sends
            ),
            (512, 16, 16)
        );
        let s = read(&[
            ("UPLOAD_MEMORY", "1048576"),
            ("UPLOAD_TIME_LIMIT", "2.5"),
            ("UPLOAD_TIME_RATE", "1000"),
            ("MAX_CONNECTIONS_PER_ADDRESS", "0"),
            ("MAX_LARGE_BODIES", "3"),
            ("MAX_MAIL_SENDS", "2"),
        ]);
        assert_eq!(
            (s.upload_memory, s.upload_time_limit, s.upload_time_rate),
            (1 << 20, 2.5, 1000)
        );
        assert_eq!(
            (
                s.max_connections_per_address,
                s.max_large_bodies,
                s.max_mail_sends
            ),
            (0, 3, 2)
        );
        for var in [
            "UPLOAD_MEMORY",
            "UPLOAD_TIME_LIMIT",
            "UPLOAD_TIME_RATE",
            "MAX_LARGE_BODIES",
            "MAX_MAIL_SENDS",
        ] {
            for bad in ["0", "-1", "", "x"] {
                assert_eq!(refused(&[(var, bad)]).var, format!("{PREFIX}{var}"));
            }
        }
        for bad in ["-1", "", "x"] {
            let var = "MAX_CONNECTIONS_PER_ADDRESS";
            assert_eq!(refused(&[(var, bad)]).var, format!("{PREFIX}{var}"));
        }
        assert_eq!(
            (s.request_timeout, s.write_timeout, s.shutdown_timeout),
            (60.0, 60.0, 8.0)
        );
        assert!(s.metrics_token.is_empty());
        let s = read(&[
            ("MAX_BLOB_UPLOADS_PER_ORG", "2"),
            ("MIN_UPLOAD_RATE", "0"),
            ("UPLOAD_RATE_GRACE", "1.5"),
            ("MAX_CONNECTIONS", "10"),
        ]);
        assert_eq!((s.max_blob_uploads_per_org, s.max_connections), (2, 10));
        assert_eq!((s.min_upload_rate, s.upload_rate_grace), (0, 1.5));
        for (var, bad) in [
            ("MAX_BLOB_UPLOADS_PER_ORG", "0"),
            ("MAX_CONNECTIONS", "0"),
            ("MIN_UPLOAD_RATE", "-1"),
            ("MIN_UPLOAD_RATE", "fast"),
            ("UPLOAD_RATE_GRACE", "0"),
        ] {
            assert_eq!(refused(&[(var, bad)]).var, format!("{PREFIX}{var}"));
        }
        let s = read(&[
            ("MAX_BLOB_UPLOADS_PER_USER", "1"),
            ("REQUEST_TIMEOUT", "2.5"),
            ("WRITE_TIMEOUT", "7"),
            ("SHUTDOWN_TIMEOUT", "20"),
            ("METRICS_TOKEN", " 0123456789abcdef-token "),
        ]);
        assert_eq!(s.max_blob_uploads_per_user, 1);
        assert_eq!(
            (s.request_timeout, s.write_timeout, s.shutdown_timeout),
            (2.5, 7.0, 20.0)
        );
        assert_eq!(s.metrics_token, "0123456789abcdef-token");
        for var in [
            "MAX_BLOB_UPLOADS_PER_USER",
            "REQUEST_TIMEOUT",
            "WRITE_TIMEOUT",
            "SHUTDOWN_TIMEOUT",
        ] {
            for bad in ["0", "-1", "", "x"] {
                assert_eq!(refused(&[(var, bad)]).var, format!("{PREFIX}{var}"));
            }
        }
    }

    #[test]
    fn a_metrics_token_is_long_enough_printable_and_never_repeated() {
        assert!(read(&[("METRICS_TOKEN", "")]).metrics_token.is_empty());
        let long = "x".repeat(257);
        for bad in [
            "short",
            "has a space in the middle",
            "tab\there-and-more",
            &long,
        ] {
            let e = refused(&[("METRICS_TOKEN", bad)]);
            assert_eq!(e.var, "OSWX_COLLAB_METRICS_TOKEN");
            assert!(!e.to_string().contains(bad), "{e}");
        }
        assert!(
            refused(&[("METRICS_TOKEN", "caf\u{e9}-0123456789abcdef")])
                .var
                .contains("METRICS")
        );
        let s = read(&[("METRICS_TOKEN", "0123456789abcdef")]);
        assert!(!format!("{s:?}").contains("0123456789abcdef"));
    }

    fn net(s: &str) -> IpNet {
        s.parse().unwrap_or_else(|()| panic!("{s} is a range"))
    }

    #[test]
    fn an_address_range_holds_its_addresses_and_no_others() {
        let has = |range: &str, ip: &str| net(range).contains(ip.parse().unwrap());
        assert!(has("127.0.0.1", "127.0.0.1"));
        assert!(!has("127.0.0.1", "127.0.0.2"));
        assert!(has("172.16.0.0/12", "172.16.0.1"));
        assert!(has("172.16.0.0/12", "172.31.255.255"));
        assert!(!has("172.16.0.0/12", "172.32.0.0"));
        assert!(!has("172.16.0.0/12", "171.255.255.255"));
        // (An address with bits after the prefix names the range it is in.)
        assert!(has("10.1.2.3/8", "10.200.0.1"));
        assert!(has("0.0.0.0/0", "8.8.8.8"));
        assert!(
            !has("0.0.0.0/0", "::1"),
            "an IPv4 range holds no IPv6 address"
        );
        assert!(has("::/0", "2001:db8::1"));
        assert!(!has("::/0", "8.8.8.8"));
        assert!(has("::1", "::1"));
        assert!(!has("::1", "::2"));
        assert!(has("fd00::/8", "fd12:3456::1"));
        assert!(!has("fd00::/8", "fe80::1"));
        assert!(has("2001:db8::/33", "2001:db8:7fff::1"));
        assert!(!has("2001:db8::/33", "2001:db8:8000::1"));
        // An IPv4 address that arrives written as IPv6 is the IPv4 address.
        assert!(has("127.0.0.1", "::ffff:127.0.0.1"));
        assert!(has("::ffff:10.0.0.0/104", "10.9.9.9"));
        assert!(has("::ffff:10.0.0.7", "10.0.0.7"));
        assert!(!has("::ffff:10.0.0.0/104", "11.0.0.1"));
        assert_eq!(
            IpNet::one("::ffff:10.0.0.7".parse().unwrap()),
            net("10.0.0.7")
        );
    }

    #[test]
    fn trusted_proxies_are_a_list_of_addresses_and_ranges() {
        assert_eq!(
            read(&[]).trusted_proxies,
            vec![net("127.0.0.1"), net("::1")]
        );
        // Every address there is, in either family and any spelling, is not a list of proxies;
        // nor is a large part of it (two halves make the whole): nothing wider than /8 for IPv4
        // and /16 for IPv6.
        for too_wide in [
            "0.0.0.0/0",
            "::/0",
            "10.1.2.3/0",
            "127.0.0.1, 0.0.0.0/0",
            "::ffff:0.0.0.0/96",
            "fd00::/00",
            "0.0.0.0/1, 128.0.0.0/1",
            "8000::/1",
            "10.0.0.0/7",
            "::ffff:10.0.0.0/103",
            "fd00::/8",
            "2001::/15",
        ] {
            let e = refused(&[("TRUSTED_PROXIES", too_wide)]);
            assert_eq!(e.var, "OSWX_COLLAB_TRUSTED_PROXIES", "{too_wide}");
            assert!(e.to_string().contains("/8"), "{e}");
        }
        // The widest that are allowed.
        assert_eq!(
            read(&[(
                "TRUSTED_PROXIES",
                "10.0.0.0/8, fd00::/16, ::ffff:10.0.0.0/104"
            )])
            .trusted_proxies,
            vec![net("10.0.0.0/8"), net("fd00::/16"), net("10.0.0.0/8")]
        );
        assert_eq!(read(&[("TRUSTED_PROXIES", "")]).trusted_proxies, vec![]);
        assert_eq!(read(&[("TRUSTED_PROXIES", " , ")]).trusted_proxies, vec![]);
        assert_eq!(
            read(&[("TRUSTED_PROXIES", "127.0.0.1, 172.16.0.0/12 ::1,fd00::/64")]).trusted_proxies,
            vec![
                net("127.0.0.1"),
                net("172.16.0.0/12"),
                net("::1"),
                net("fd00::/64")
            ]
        );
        for bad in [
            "localhost",
            "10.0.0.0/33",
            "::1/129",
            "10.0.0.0/",
            "10.0.0.0/+8",
            "10.0.0.0/-1",
            "10.0.0/8",
            "10.0.0.0/8/8",
            "/8",
            "127.0.0.1;10.0.0.1",
            "127.0.0.1, nonsense-that-must-not-be-echoed",
            "[::1]",
            "127.0.0.1:8080",
            "*",
        ] {
            let e = refused(&[("TRUSTED_PROXIES", bad)]);
            assert_eq!(e.var, "OSWX_COLLAB_TRUSTED_PROXIES", "{bad}");
            assert!(!e.to_string().contains("nonsense"), "{e}");
        }
    }

    #[test]
    fn unknown_variables_are_ignored() {
        let mut vars = env(&[("NOT_A_SETTING", "x"), ("PASSWORD", "secret")]);
        vars.insert("PATH".into(), "/usr/bin".into());
        let s = Settings::from_env(&vars).ok();
        assert!(s == Some(Settings::default()));
    }

    #[test]
    fn a_bad_number_is_an_error_naming_the_variable() {
        let e = refused(&[("SESSION_DAYS", "thirty")]);
        assert_eq!(e.var, "OSWX_COLLAB_SESSION_DAYS");
        assert!(
            e.to_string()
                .starts_with("OSWX_COLLAB_SESSION_DAYS must be"),
            "{e}"
        );
        assert!(!e.to_string().contains("thirty"));
        assert_eq!(
            refused(&[("SESSION_DAYS", "-1")]).var,
            "OSWX_COLLAB_SESSION_DAYS"
        );
        assert_eq!(
            refused(&[("SMTP_PORT", "70000")]).var,
            "OSWX_COLLAB_SMTP_PORT"
        );
        assert_eq!(refused(&[("LOCK_TTL", "soon")]).var, "OSWX_COLLAB_LOCK_TTL");
        assert_eq!(refused(&[("LOCK_TTL", "nan")]).var, "OSWX_COLLAB_LOCK_TTL");
        assert_eq!(refused(&[("LOCK_TTL", "-5")]).var, "OSWX_COLLAB_LOCK_TTL");
        assert_eq!(
            refused(&[("SMTP_SECURITY", "tls")]).var,
            "OSWX_COLLAB_SMTP_SECURITY"
        );
    }

    #[test]
    fn both_database_url_spellings_are_accepted() {
        let db = |url: &str| read(&[("DATABASE_URL", url)]).database().ok();
        let sqlite = |p: &str| Some(Database::Sqlite(PathBuf::from(p)));
        let pg = Some(Database::Postgres("postgres://u:pw@db:5432/collab".into()));
        assert_eq!(db("sqlite://data/collab.db"), sqlite("data/collab.db"));
        assert_eq!(db("sqlite:///srv/collab.db"), sqlite("/srv/collab.db"));
        assert_eq!(db("sqlite:data/collab.db"), sqlite("data/collab.db"));
        assert_eq!(db("sqlite:/srv/collab.db"), sqlite("/srv/collab.db"));
        assert_eq!(
            db("sqlite+aiosqlite:///./data/collab.db"),
            sqlite("./data/collab.db")
        );
        assert_eq!(
            db("sqlite+aiosqlite:////srv/collab.db"),
            sqlite("/srv/collab.db")
        );
        assert_eq!(
            db("sqlite+aiosqlite:///C:/data/collab.db"),
            sqlite("C:/data/collab.db")
        );
        assert_eq!(db("postgres://u:pw@db:5432/collab"), pg);
        assert_eq!(db("postgresql://u:pw@db:5432/collab"), pg);
        assert_eq!(db("postgresql+asyncpg://u:pw@db:5432/collab"), pg);
        for bad in [
            "mysql://db/collab",
            "./collab.db",
            "sqlite://",
            "sqlite:",
            "sqlite::memory:",
            "sqlite://data/collab.db?mode=rwc",
            "sqlite+aiosqlite:///./collab.db?timeout=5",
            "postgres://",
            "",
        ] {
            assert_eq!(
                refused(&[("DATABASE_URL", bad)]).var,
                "OSWX_COLLAB_DATABASE_URL",
                "{bad}"
            );
        }
    }

    #[test]
    fn a_sqlite_query_string_is_refused_with_a_clear_message() {
        let err = refused(&[("DATABASE_URL", "sqlite://data/collab.db?mode=ro")]);
        assert_eq!(err.var, "OSWX_COLLAB_DATABASE_URL");
        assert!(err.expected.contains("query string"), "{err}");
    }

    #[test]
    fn storage_is_a_folder_or_s3() {
        let st = |url: &str| read(&[("STORAGE_URL", url)]).storage().ok();
        assert_eq!(st("/srv/blobs"), Some(Storage::Local("/srv/blobs".into())));
        let s3 = |b: &str, p: &str| {
            Some(Storage::S3 {
                bucket: b.into(),
                prefix: p.into(),
            })
        };
        assert_eq!(st("s3://bucket/some/prefix/"), s3("bucket", "some/prefix"));
        assert_eq!(st("s3://bucket"), s3("bucket", ""));
        assert_eq!(
            refused(&[("STORAGE_URL", "s3:///x")]).var,
            "OSWX_COLLAB_STORAGE_URL"
        );
        assert_eq!(
            refused(&[("STORAGE_URL", " ")]).var,
            "OSWX_COLLAB_STORAGE_URL"
        );
    }

    #[test]
    fn s3_settings_have_the_names_the_python_server_read() {
        let s = read(&[]);
        assert_eq!(s.s3_region, "us-east-1");
        assert_eq!(s.s3_path_style, None);
        assert_eq!(s.blob_idle_timeout, 60.0);
        assert!(s.s3_access_key_id.is_empty() && s.s3_secret_access_key.is_empty());

        let mut vars = env(&[
            ("STORAGE_URL", "s3://collab/prod"),
            ("S3_ENDPOINT", "http://garage:3900"),
            ("S3_PATH_STYLE", "off"),
            ("BLOB_IDLE_TIMEOUT", "2.5"),
        ]);
        for (name, value) in [
            ("AWS_ACCESS_KEY_ID", "GK-key-id"),
            ("AWS_SECRET_ACCESS_KEY", " s3-secret "),
            ("AWS_SESSION_TOKEN", "s3-token"),
            ("AWS_REGION", "eu-west-1"),
            ("AWS_DEFAULT_REGION", "garage"),
        ] {
            vars.insert(name.to_owned(), value.to_owned());
        }
        let s = Settings::from_env(&vars).unwrap();
        assert_eq!(s.s3_endpoint, "http://garage:3900");
        assert_eq!(s.s3_region, "garage", "AWS_DEFAULT_REGION first, as boto3");
        assert_eq!(s.s3_access_key_id, "GK-key-id");
        assert_eq!(s.s3_secret_access_key, "s3-secret");
        assert_eq!(s.s3_session_token, "s3-token");
        assert_eq!(s.s3_path_style, Some(false));
        assert_eq!(s.blob_idle_timeout, 2.5);
        let shown = format!("{s:?}");
        for secret in ["GK-key-id", "s3-secret", "s3-token"] {
            assert!(!shown.contains(secret), "{secret} in {shown}");
        }

        vars.remove("AWS_DEFAULT_REGION");
        assert_eq!(Settings::from_env(&vars).unwrap().s3_region, "eu-west-1");
        assert_eq!(
            refused(&[("S3_PATH_STYLE", "maybe")]).var,
            "OSWX_COLLAB_S3_PATH_STYLE"
        );
        assert_eq!(
            refused(&[("BLOB_IDLE_TIMEOUT", "0")]).var,
            "OSWX_COLLAB_BLOB_IDLE_TIMEOUT"
        );
    }

    #[test]
    fn oauth_providers_need_id_and_secret_and_come_google_first() {
        let s = read(&[
            ("GITHUB_CLIENT_ID", "gh"),
            ("GITHUB_CLIENT_SECRET", "ghs"),
            ("GOOGLE_CLIENT_ID", "g"),
            ("GOOGLE_CLIENT_SECRET", "gs"),
        ]);
        assert_eq!(s.oauth_providers(), ["google", "github"]);
        let half = read(&[("GOOGLE_CLIENT_ID", "g"), ("GITHUB_CLIENT_SECRET", "s")]);
        assert!(half.oauth_providers().is_empty());
    }

    #[test]
    fn the_database_pools_are_ten_and_two_connections_unless_set() {
        let s = read(&[]);
        assert_eq!((s.database_pool, s.database_journal_pool), (10, 2));
        assert_eq!(s.database_lock_timeout, 5.0);
        assert_eq!(s.database_journal_lock_timeout, 2.0);
        // A lock timeout must end before the statement timeout (30 s) does, and the journal's
        // within a quarter of a batch's time.
        for (name, most, over, says) in [
            (
                "DATABASE_LOCK_TIMEOUT",
                "20",
                ["20.5", "30", "86400"],
                "at most 20",
            ),
            (
                "DATABASE_JOURNAL_LOCK_TIMEOUT",
                "7.5",
                ["7.6", "20", "86400"],
                "at most 7.5",
            ),
        ] {
            assert_eq!(
                read(&[(name, most)]).database_pool,
                10,
                "{most} s is allowed"
            );
            for bad in over {
                let err = refused(&[(name, bad)]);
                assert_eq!(err.var, format!("OSWX_COLLAB_{name}"), "{bad}");
                assert!(err.expected.contains(says), "{err}");
            }
        }
        assert_eq!(
            crate::store::postgres::MAX_JOURNAL_LOCK_TIMEOUT,
            crate::journal::BATCH_TIMEOUT.as_secs_f64() / 4.0
        );
        let s = read(&[
            ("DATABASE_POOL", " 32 "),
            ("DATABASE_JOURNAL_POOL", "4"),
            ("DATABASE_LOCK_TIMEOUT", "1.5"),
        ]);
        assert_eq!((s.database_pool, s.database_journal_pool), (32, 4));
        assert_eq!(s.database_lock_timeout, 1.5);
        for name in [
            "DATABASE_POOL",
            "DATABASE_JOURNAL_POOL",
            "DATABASE_LOCK_TIMEOUT",
        ] {
            for bad in ["0", "-1", "many", ""] {
                assert_eq!(
                    refused(&[(name, bad)]).var,
                    format!("OSWX_COLLAB_{name}"),
                    "{bad}"
                );
            }
        }
    }

    #[test]
    fn debug_output_never_shows_secrets() {
        let s = read(&[
            ("DATABASE_URL", "postgres://collab:hunter2@db/collab"),
            ("SMTP_PASSWORD", "smtp-secret"),
            ("GOOGLE_CLIENT_SECRET", "google-secret"),
            ("GITHUB_CLIENT_SECRET", "github-secret"),
        ]);
        let shown = format!("{s:?}");
        for secret in ["hunter2", "smtp-secret", "google-secret", "github-secret"] {
            assert!(!shown.contains(secret), "{secret} in {shown}");
        }
        assert!(
            shown.contains("postgres://collab:<redacted>@db/collab"),
            "{shown}"
        );
    }
}
