//! The PostgreSQL store.
//!
//! The connections are in three pools ([`PgConfig`]), so that no kind of work can take the
//! connections another kind needs:
//!
//! - **transactions** of requests ([`Store::begin`]): half of `pool`. These wait for row locks
//!   (every write in an organisation locks its row first), and a waiting transaction holds its
//!   connection; so each of these connections has `lock_timeout` set (5 s unless configured), and
//!   a transaction that waits longer for a lock fails with `55P03`, which a request answers with
//!   409 "try again" ([`super::is_conflict`]);
//! - **reads** ([`PgStore::reader`]): the other half of `pool`. A request that holds a
//!   transaction and then reads through [`Store`] never waits for a connection that only other
//!   transactions could give back (as on SQLite, where the writer and the readers are apart);
//! - **the journal** ([`Store::begin_journal`]): `journal_pool` connections (2 unless
//!   configured) for the journal writer, lease acquisitions, snapshot rows and rebuild reports,
//!   which no request can use up. These have a shorter `lock_timeout` (2 s unless configured):
//!   the journal writes many documents' records in one transaction, and must not let one
//!   document whose row someone holds keep the others waiting. When a batch runs into it
//!   (`55P03`), the writer writes the batch again one document at a time
//!   ([`crate::journal`]): only the records of the blocked document come back `Unknown` (its
//!   room reloads); every other document's are stored.
//!
//! A request's lock timeout is at most 20 s ([`MAX_LOCK_TIMEOUT`]), well below the statement
//! timeout, and the journal's at most 7.5 s ([`MAX_JOURNAL_LOCK_TIMEOUT`], a quarter of the time
//! a journal batch may take), so that a lock wait ends as a lock wait (409, or the journal's
//! fallback) and not as a statement that ran out of time (503) or a batch given up as a whole. [`PgStore::connect`] refuses longer ones, and
//! sets both on the connection it makes first, so that a database that will not take them says
//! so when the server starts.
//!
//! With the defaults the server holds at most **12** connections (5 + 5 + 2), opened as they are
//! needed. A caller waits up to 30 s for a free one ([`super::is_busy`]: 503). Every connection is
//! opened with:
//!
//! - `default_transaction_isolation = read committed`, whatever the database or role is set to:
//!   the server's transactions are written for it (see "Isolation and locks" in [`crate::store`]);
//! - `statement_timeout = 30s`: a statement stuck behind a lock, or runaway, fails instead of
//!   holding its connection for ever;
//! - `idle_in_transaction_session_timeout = 60s`: a transaction nobody is driving any more is
//!   ended by the database, and its row locks with it.
//!
//! Options the operator puts in the URL (`?sslmode=…`, `?options=-c search_path=…`) are kept; the
//! tables live in the first schema of the connection's `search_path` (normally `public`).
//!
//! Migrations are those of `migrations/postgres/`, applied by the same sqlx migrator as SQLite's,
//! on a connection of their own without the statement timeout. sqlx takes an advisory lock for
//! the run, so two servers started at once on an empty database build it once.
//!
//! Unlike SQLite there is no single writer and so no re-entrancy guard: a task that asks for a
//! second transaction gets one (on another connection), and the two can then wait for each other's
//! locks. Use a savepoint, as the rules in [`crate::store`] say.
//!
//! The URL is never logged or put in an error, and neither is what the database says about a
//! failed connection (it names the user): connection errors are a fixed phrase and the host,
//! port and database.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use sqlx::migrate::Migrator;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, PgPoolOptions};
use sqlx::{Acquire, ConnectOptions, Connection, Postgres, Transaction};

use super::{
    AssemblyRefRow, AssemblyUse, AuthoredChange, ChangeEventRow, ChangeRow, ChangeSize, Document,
    Folder, HistoryRow, Id, Invite, LoginCode, Member, Membership, NewChange, OAuthIdentity,
    OAuthState, Org, OrgMember, OrgSummary, PartRow, PasswordReset, PinnedComponent, Project,
    ProjectMember, ProjectWithRole, RoomLease, SchemaError, SchemaStatus, Session, SnapshotRow,
    Store, Tx, UndoTarget, User, Version, VersionWithAuthor, sql,
};
use crate::config::{Database, parse_database_url};

/// The migrations in `migrations/postgres/`, embedded in the binary.
pub static MIGRATOR: Migrator = sqlx::migrate!("migrations/postgres");

/// How many connections requests share (half for transactions, half for reads), unless the
/// settings say otherwise (`OSWX_COLLAB_DATABASE_POOL`).
pub const DEFAULT_POOL: u32 = 10;
/// How many connections the journal has to itself, unless the settings say otherwise
/// (`OSWX_COLLAB_DATABASE_JOURNAL_POOL`).
pub const DEFAULT_JOURNAL_POOL: u32 = 2;
/// How many seconds a request's transaction waits for a row lock, unless the settings say
/// otherwise (`OSWX_COLLAB_DATABASE_LOCK_TIMEOUT`).
pub const DEFAULT_LOCK_TIMEOUT: f64 = 5.0;
/// How many seconds the journal's transaction waits for a row lock, unless the settings say
/// otherwise (`OSWX_COLLAB_DATABASE_JOURNAL_LOCK_TIMEOUT`).
pub const DEFAULT_JOURNAL_LOCK_TIMEOUT: f64 = 2.0;
/// The longest lock timeout, in seconds: well below the statement timeout (30 s), so that a wait
/// for a lock ends as one.
pub const MAX_LOCK_TIMEOUT: f64 = 20.0;
/// The longest lock timeout of the journal's transactions, in seconds: a quarter of the time a
/// journal batch may take ([`crate::journal::BATCH_TIMEOUT`]), so that a batch that waited for a
/// lock, and then once more for the blocked document when it is written again one document at a
/// time, still ends well inside it.
pub const MAX_JOURNAL_LOCK_TIMEOUT: f64 = 7.5;
/// How long a caller waits for a connection.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
/// The longest one statement may run (waiting for a lock included), in milliseconds.
const STATEMENT_TIMEOUT_MS: u32 = 30_000;
/// How long a transaction may sit without a statement before the database ends it, in
/// milliseconds.
const IDLE_IN_TRANSACTION_MS: u32 = 60_000;
/// Prepared statements kept per connection: every fixed statement of [`super::sql`] fits.
const STATEMENT_CACHE: usize = 256;

/// How many connections a [`PgStore`] holds, and how long its requests wait for a lock.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PgConfig {
    /// Connections for requests: half for their transactions, half for reads (at least one each).
    pub pool: u32,
    /// Connections for the journal writer, leases and snapshot rows (at least one).
    pub journal_pool: u32,
    /// How long a request's transaction waits for a row lock before it fails (`55P03`).
    pub lock_timeout: Duration,
    /// How long the journal's transaction waits for a row lock before it fails (`55P03`).
    pub journal_lock_timeout: Duration,
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            pool: DEFAULT_POOL,
            journal_pool: DEFAULT_JOURNAL_POOL,
            lock_timeout: Duration::from_secs_f64(DEFAULT_LOCK_TIMEOUT),
            journal_lock_timeout: Duration::from_secs_f64(DEFAULT_JOURNAL_LOCK_TIMEOUT),
        }
    }
}

impl PgConfig {
    /// As the settings say (`OSWX_COLLAB_DATABASE_POOL`, `_JOURNAL_POOL`, `_LOCK_TIMEOUT`).
    pub fn from_settings(settings: &crate::Settings) -> Self {
        Self {
            pool: settings.database_pool,
            journal_pool: settings.database_journal_pool,
            lock_timeout: Duration::from_secs_f64(settings.database_lock_timeout),
            journal_lock_timeout: Duration::from_secs_f64(settings.database_journal_lock_timeout),
        }
    }
}

/// A PostgreSQL database. Cloning is cheap: clones share the pools.
#[derive(Clone)]
pub struct PgStore {
    /// Connections for requests' transactions ([`PgStore::begin_tx`]).
    transactions: PgPool,
    /// Connections for reads ([`PgStore::reader`]).
    reads: PgPool,
    /// Connections for the journal's transactions ([`PgStore::begin_journal_tx`]).
    journal: PgPool,
    /// How every connection is opened (for the migrations' own connection).
    options: Arc<PgConnectOptions>,
    migrator: &'static Migrator,
}

/// (Never the URL: it holds the password.)
impl std::fmt::Debug for PgStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgStore")
            .field("host", &self.options.get_host())
            .field("database", &self.options.get_database())
            .field(
                "connections",
                &(self.transactions.size() + self.reads.size() + self.journal.size()),
            )
            .finish_non_exhaustive()
    }
}

impl PgStore {
    /// Opens the database at `url` with the pools `config` describes and brings its schema up to
    /// date: an empty database is always built; an older one is upgraded only when `auto_migrate`
    /// is true, and is otherwise an error telling the operator to run `migrate`; a newer one, or
    /// one this server did not write, is always an error ([`SchemaError`]).
    ///
    /// `url` is any PostgreSQL form [`parse_database_url`] accepts.
    pub async fn open(url: &str, auto_migrate: bool, config: PgConfig) -> Result<Self> {
        Self::open_with_migrations(url, auto_migrate, config, &MIGRATOR).await
    }

    /// [`PgStore::open`] with other migrations than the embedded ones (tests of the upgrade path,
    /// which need a "newer binary").
    #[doc(hidden)]
    pub async fn open_with_migrations(
        url: &str,
        auto_migrate: bool,
        config: PgConfig,
        migrator: &'static Migrator,
    ) -> Result<Self> {
        let mut store = Self::connect(url, config).await?;
        store.migrator = migrator;
        store.bring_up_to_date(auto_migrate).await?;
        Ok(store)
    }

    /// Opens the database at `url` without looking at its schema (for `migrate` and tools).
    pub async fn connect(url: &str, config: PgConfig) -> Result<Self> {
        let url = match parse_database_url(url)? {
            Database::Postgres(url) => url,
            Database::Sqlite(_) => bail!("not a PostgreSQL database URL"),
        };
        // (sqlx's error says what is wrong with the URL without repeating it.)
        let options = PgConnectOptions::from_str(&url)
            .map_err(|e| anyhow!("the PostgreSQL database URL cannot be read: {e}"))?
            .statement_cache_capacity(STATEMENT_CACHE)
            // (A space in a startup option's value is written `\ `.)
            .options([("default_transaction_isolation", "read\\ committed")])
            .options([
                ("statement_timeout", STATEMENT_TIMEOUT_MS),
                (
                    "idle_in_transaction_session_timeout",
                    IDLE_IN_TRANSACTION_MS,
                ),
            ]);
        let for_transactions = (config.pool / 2).max(1);
        let for_reads = config.pool.saturating_sub(for_transactions).max(1);
        let pool = |connections: u32| {
            PgPoolOptions::new()
                .max_connections(connections.max(1))
                .acquire_timeout(ACQUIRE_TIMEOUT)
        };
        let longest = Duration::from_secs_f64(MAX_LOCK_TIMEOUT);
        anyhow::ensure!(
            config.lock_timeout <= longest,
            "a lock timeout is at most {MAX_LOCK_TIMEOUT} s (statements end after {} s)",
            STATEMENT_TIMEOUT_MS / 1000
        );
        anyhow::ensure!(
            config.journal_lock_timeout <= Duration::from_secs_f64(MAX_JOURNAL_LOCK_TIMEOUT),
            "the journal's lock timeout is at most {MAX_JOURNAL_LOCK_TIMEOUT} s (a quarter of \
             the time a journal batch may take)"
        );
        let lock_timeout_ms = config.lock_timeout.as_millis().max(1);
        let journal_lock_timeout_ms = config.journal_lock_timeout.as_millis().max(1);
        // One connection made here and now shows whether the database answers, and says why not
        // at once (a pool would try again until its wait ran out). The pools open theirs as they
        // are needed.
        let mut probe = options
            .connect()
            .await
            .map_err(|e| cannot_connect(&options, &e))?;
        // The lock timeouts are tried on it: a database that will not take them says so now.
        for ms in [lock_timeout_ms, journal_lock_timeout_ms] {
            sqlx::query(&format!("SET lock_timeout = {ms}"))
                .execute(&mut probe)
                .await
                .context("the database did not take the lock timeout")?;
        }
        let _ = probe.close().await;
        let transactions = pool(for_transactions)
            .connect_lazy_with(options.clone().options([("lock_timeout", lock_timeout_ms)]));
        let reads = pool(for_reads).connect_lazy_with(options.clone());
        let journal = pool(config.journal_pool).connect_lazy_with(
            options
                .clone()
                .options([("lock_timeout", journal_lock_timeout_ms)]),
        );
        Ok(Self {
            transactions,
            reads,
            journal,
            options: Arc::new(options),
            migrator: &MIGRATOR,
        })
    }

    /// Applies the pending migrations on a connection of their own, without the statement timeout
    /// (building an index on a large table takes as long as it takes).
    async fn run_migrations(&self) -> Result<()> {
        // (The later of two settings of one option wins.)
        let unhurried = PgConnectOptions::clone(&self.options).options([
            ("statement_timeout", 0),
            ("idle_in_transaction_session_timeout", 0),
        ]);
        let mut conn = unhurried
            .connect()
            .await
            .map_err(|e| cannot_connect(&self.options, &e))?;
        let ran = self.migrator.run(&mut conn).await;
        let _ = conn.close().await;
        Ok(ran?)
    }

    /// Nothing to refuse: a second transaction of one task runs on another connection (see the
    /// module documentation).
    #[allow(clippy::unused_self, clippy::unnecessary_wraps)] // (the shape SQLite's guard has)
    fn refuse_nested(&self) -> Result<()> {
        Ok(())
    }

    /// A transaction with direct access to its connection (for tests and tools; handlers use
    /// [`Store::begin`]).
    pub async fn begin_tx(&self) -> Result<PgTx<'static>> {
        Ok(PgTx {
            tx: self.transactions.begin().await?,
        })
    }

    /// [`Store::begin_journal`]: a transaction on the journal's own connections.
    pub async fn begin_journal_tx(&self) -> Result<PgTx<'static>> {
        Ok(PgTx {
            tx: self.journal.begin().await?,
        })
    }

    /// A connection for reads (each statement sees what was committed when it started).
    pub async fn reader(&self) -> Result<PoolConnection<Postgres>> {
        Ok(self.reads.acquire().await?)
    }

    /// Closes every connection.
    pub async fn close(&self) {
        self.reads.close().await;
        self.journal.close().await;
        self.transactions.close().await;
    }
}

/// Where a database is, for an error message: never the user or the password.
fn place(options: &PgConnectOptions) -> String {
    format!(
        "{} on {}:{}",
        options.get_database().unwrap_or("(the default database)"),
        options.get_host(),
        options.get_port()
    )
}

/// The error for a connection that could not be made: a fixed phrase for the kind of failure and
/// where the database is. What the database or the driver said is left out: it names the user
/// (`password authentication failed for user "…"`).
fn cannot_connect(options: &PgConnectOptions, err: &sqlx::Error) -> anyhow::Error {
    let code = err
        .as_database_error()
        .and_then(|e| e.code().map(|c| c.into_owned()));
    let why = match (err, code.as_deref()) {
        (_, Some(code)) if code.starts_with("28") => "the user or password was refused",
        (_, Some("3D000")) => "there is no such database",
        (_, Some("53300")) => "the database has no connection to spare",
        (sqlx::Error::Io(_), _) => "the database server cannot be reached",
        (sqlx::Error::Tls(_), _) => "a secure connection could not be made",
        (sqlx::Error::PoolTimedOut, _) => "the database server did not answer in time",
        _ => "the connection failed",
    };
    anyhow!("cannot open the database {}: {why}", place(options))
}

/// A transaction, or a savepoint within one.
#[derive(Debug)]
pub struct PgTx<'c> {
    tx: Transaction<'c, Postgres>,
}

impl PgTx<'_> {
    /// The transaction's connection. Every query of a [`Tx`] method runs through this.
    pub fn conn(&mut self) -> &mut PgConnection {
        &mut self.tx
    }

    /// A savepoint within this transaction, with direct access to its connection.
    pub async fn savepoint_tx(&mut self) -> Result<PgTx<'_>> {
        Ok(PgTx {
            tx: (&mut self.tx).begin().await?,
        })
    }

    /// Makes the writes permanent (for a savepoint: part of the enclosing transaction).
    pub async fn commit_tx(self) -> Result<()> {
        Ok(self.tx.commit().await?)
    }

    /// Undoes the writes.
    pub async fn rollback_tx(self) -> Result<()> {
        Ok(self.tx.rollback().await?)
    }
}

super::shared::store_impl!(PgStore, PgTx, PgConnection, postgres);
