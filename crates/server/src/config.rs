//! Server settings, read from `OSWX_COLLAB_*` environment variables. The names and defaults are the
//! Python server's (`config.py`); see the parity inventory, section 9.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

/// Every environment variable this server reads starts with this.
pub const PREFIX: &str = "OSWX_COLLAB_";

const MIB: usize = 1 << 20;

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

/// The server's settings. `Default` gives the Python server's defaults.
#[derive(Clone, PartialEq)]
pub struct Settings {
    pub database_url: String,
    /// The most connections the server holds to a PostgreSQL database (SQLite has its own fixed
    /// few).
    pub database_pool: u32,
    pub storage_url: String,
    /// For S3-compatible stores (MinIO), e.g. `http://minio:9000`.
    pub s3_endpoint: String,
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
            storage_url: "./data/storage".into(),
            s3_endpoint: String::new(),
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
        e.string("STORAGE_URL", &mut s.storage_url);
        e.string("S3_ENDPOINT", &mut s.s3_endpoint);
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
        s.storage()?;
        Ok(s)
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
            .field("storage_url", &self.storage_url)
            .field("s3_endpoint", &self.s3_endpoint)
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
    fn the_database_pool_is_ten_connections_unless_set() {
        assert_eq!(read(&[]).database_pool, 10);
        assert_eq!(read(&[("DATABASE_POOL", " 32 ")]).database_pool, 32);
        for bad in ["0", "-1", "many", ""] {
            assert_eq!(
                refused(&[("DATABASE_POOL", bad)]).var,
                "OSWX_COLLAB_DATABASE_POOL",
                "{bad}"
            );
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
