//! The PostgreSQL store.
//!
//! The connections ([`PgStore::open`]'s `pool_size` of them at most) are in two pools, one for
//! transactions and one for reads, half each: a request that holds a transaction and then reads
//! through [`Store`] can never wait for a connection that only other transactions could give back
//! (as on SQLite, where the writer and the readers are apart). A caller waits up to 30 s for a
//! free connection. Every connection is opened with:
//!
//! - `default_transaction_isolation = read committed`, whatever the database or role is set to:
//!   the server's transactions are written for it (see "Locks" in [`crate::store`]);
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
//! The URL is never logged or put in an error: connection errors name the host, port and database
//! only.

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

/// How many connections the pool holds at most, unless the settings say otherwise
/// (`OSWX_COLLAB_DATABASE_POOL`).
pub const DEFAULT_POOL: u32 = 10;
/// How long a caller waits for a connection.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
/// The longest one statement may run (waiting for a lock included), in milliseconds.
const STATEMENT_TIMEOUT_MS: u32 = 30_000;
/// How long a transaction may sit without a statement before the database ends it, in
/// milliseconds.
const IDLE_IN_TRANSACTION_MS: u32 = 60_000;
/// Prepared statements kept per connection: every fixed statement of [`super::sql`] fits.
const STATEMENT_CACHE: usize = 256;

/// A PostgreSQL database. Cloning is cheap: clones share the pools.
#[derive(Clone)]
pub struct PgStore {
    /// Connections for transactions ([`PgStore::begin_tx`]).
    transactions: PgPool,
    /// Connections for reads ([`PgStore::reader`]).
    reads: PgPool,
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
                &(self.transactions.size() + self.reads.size()),
            )
            .finish_non_exhaustive()
    }
}

impl PgStore {
    /// Opens the database at `url` with at most `pool_size` connections (at least 2: one for
    /// transactions, one for reads) and brings its schema up to date: an empty database is always built; an older one is upgraded only when
    /// `auto_migrate` is true, and is otherwise an error telling the operator to run `migrate`; a
    /// newer one, or one this server did not write, is always an error ([`SchemaError`]).
    ///
    /// `url` is any PostgreSQL form [`parse_database_url`] accepts.
    pub async fn open(url: &str, auto_migrate: bool, pool_size: u32) -> Result<Self> {
        Self::open_with_migrations(url, auto_migrate, pool_size, &MIGRATOR).await
    }

    /// [`PgStore::open`] with other migrations than the embedded ones (tests of the upgrade path,
    /// which need a "newer binary").
    #[doc(hidden)]
    pub async fn open_with_migrations(
        url: &str,
        auto_migrate: bool,
        pool_size: u32,
        migrator: &'static Migrator,
    ) -> Result<Self> {
        let mut store = Self::connect(url, pool_size).await?;
        store.migrator = migrator;
        store.bring_up_to_date(auto_migrate).await?;
        Ok(store)
    }

    /// Opens the database at `url` without looking at its schema (for `migrate` and tools).
    pub async fn connect(url: &str, pool_size: u32) -> Result<Self> {
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
        let for_transactions = (pool_size / 2).max(1);
        let for_reads = pool_size.saturating_sub(for_transactions).max(1);
        let transactions = PgPoolOptions::new()
            .max_connections(for_transactions)
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .connect_with(options.clone())
            .await
            .with_context(|| format!("cannot open the database {}", place(&options)))?;
        // (Opened when first needed: the first pool has shown that the database answers.)
        let reads = PgPoolOptions::new()
            .max_connections(for_reads)
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .connect_lazy_with(options.clone());
        Ok(Self {
            transactions,
            reads,
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
            .with_context(|| format!("cannot open the database {}", place(&self.options)))?;
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

    /// A connection for reads (each statement sees what was committed when it started).
    pub async fn reader(&self) -> Result<PoolConnection<Postgres>> {
        Ok(self.reads.acquire().await?)
    }

    /// Closes every connection.
    pub async fn close(&self) {
        self.reads.close().await;
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
