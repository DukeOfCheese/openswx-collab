//! The SQLite store.
//!
//! The file is opened in WAL mode with foreign keys on and a 30 s busy timeout. SQLite allows one
//! writer at a time, so the store keeps two pools:
//!
//! - **one writer connection**. Every write transaction ([`SqliteStore::begin_tx`], migrations) runs
//!   on it and starts with `BEGIN IMMEDIATE`, so it holds the write lock from its first statement.
//!   Callers that want to write while another transaction is open wait their turn in the pool's
//!   queue, not inside SQLite;
//! - **several reader connections**, opened `query_only`, so a read can never take the write lock.
//!   In WAL mode readers and the writer do not block each other.
//!
//! So within this process two connections never compete for the write lock, and "database is
//! locked" cannot happen. Another process writing the same file (an operator's `migrate`) is waited
//! for by the busy timeout; because the write lock is taken at `BEGIN`, SQLite can always apply that
//! wait (a transaction that upgrades from reading to writing could not).

use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use sqlx::migrate::Migrator;
use sqlx::pool::PoolConnection;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqlitePool, SqlitePoolOptions,
};
use sqlx::{Acquire, Sqlite, Transaction};

use super::{SchemaError, SchemaStatus, Store, Tx, sql};
use crate::config::{Database, parse_database_url};

/// The migrations in `migrations/sqlite/`, embedded in the binary.
pub static MIGRATOR: Migrator = sqlx::migrate!("migrations/sqlite");

/// How long SQLite waits for a lock held by another process.
const BUSY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a caller waits for a connection (for a writer: for the open transaction to end).
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
/// Reader connections.
const READERS: u32 = 4;

/// A SQLite database file. Cloning is cheap: clones share the connections.
#[derive(Clone)]
pub struct SqliteStore {
    writer: SqlitePool,
    readers: SqlitePool,
    migrator: &'static Migrator,
}

impl std::fmt::Debug for SqliteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStore")
            .field("writer", &self.writer)
            .field("readers", &self.readers)
            .finish_non_exhaustive()
    }
}

impl SqliteStore {
    /// Opens the database at `url` and brings its schema up to date: an empty database is always
    /// built; an older one is upgraded only when `auto_migrate` is true, and is otherwise an error
    /// telling the operator to run `migrate`; a newer one, or one this server did not write, is
    /// always an error ([`SchemaError`]).
    ///
    /// `url` is any SQLite form [`parse_database_url`] accepts. The parent directory is created.
    pub async fn open(url: &str, auto_migrate: bool) -> Result<Self> {
        Self::open_with_migrations(url, auto_migrate, &MIGRATOR).await
    }

    /// [`SqliteStore::open`] with other migrations than the embedded ones (tests of the upgrade
    /// path, which need a "newer binary").
    #[doc(hidden)]
    pub async fn open_with_migrations(
        url: &str,
        auto_migrate: bool,
        migrator: &'static Migrator,
    ) -> Result<Self> {
        let mut store = Self::connect(url).await?;
        store.migrator = migrator;
        let status = store.schema_status().await?;
        let upgrade =
            status.current.is_some() && status.unknown.is_empty() && !status.pending.is_empty();
        if upgrade && !auto_migrate {
            bail!(SchemaError::Outdated {
                current: status.current,
                latest: status.latest,
            });
        }
        if !status.is_current() {
            // Builds an empty database; refuses a newer or foreign one.
            store.migrate().await?;
        }
        Ok(store)
    }

    /// Opens the database at `url` without looking at its schema (for `migrate` and tools).
    pub async fn connect(url: &str) -> Result<Self> {
        let path = match parse_database_url(url)? {
            Database::Sqlite(path) => path,
            Database::Postgres(_) => bail!("not a SQLite database URL"),
        };
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("cannot create the folder {}", parent.display()))?;
        }
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .foreign_keys(true)
            .busy_timeout(BUSY_TIMEOUT);
        // The writer stays open for the life of the store, so the WAL files persist between writes.
        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .connect_with(
                options
                    .clone()
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal),
            )
            .await
            .with_context(|| format!("cannot open the database {}", path.display()))?;
        let readers = SqlitePoolOptions::new()
            .max_connections(READERS)
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .connect_with(options.pragma("query_only", "ON"))
            .await
            .with_context(|| format!("cannot open the database {}", path.display()))?;
        Ok(Self {
            writer,
            readers,
            migrator: &MIGRATOR,
        })
    }

    /// Applies the migrations this binary carries that the database has not had, and answers the
    /// schema status afterwards. Refuses a database written by a newer server, or not by this one.
    pub async fn migrate(&self) -> Result<SchemaStatus> {
        let status = self.schema_status().await?;
        if !status.unknown.is_empty() {
            bail!(SchemaError::Newer {
                unknown: status.unknown,
                latest: status.latest,
            });
        }
        if status.current.is_none() && q::has_tables(&mut *self.reader().await?).await? {
            bail!(SchemaError::Foreign);
        }
        if !status.pending.is_empty() {
            self.migrator
                .run(&self.writer)
                .await
                .context("migrating the database")?;
        }
        self.schema_status().await
    }

    /// A write transaction with direct access to its connection (for tests and tools; handlers use
    /// [`Store::begin`]). It waits while another write transaction is open.
    pub async fn begin_tx(&self) -> Result<SqliteTx<'static>> {
        Ok(SqliteTx {
            tx: self.writer.begin_with("BEGIN IMMEDIATE").await?,
        })
    }

    /// A read-only connection (`query_only`), for reads.
    pub async fn reader(&self) -> Result<PoolConnection<Sqlite>> {
        Ok(self.readers.acquire().await?)
    }

    /// Closes every connection.
    pub async fn close(&self) {
        self.readers.close().await;
        self.writer.close().await;
    }
}

#[async_trait]
impl Store for SqliteStore {
    async fn health(&self) -> Result<()> {
        q::health(&mut *self.reader().await?).await
    }

    async fn schema_status(&self) -> Result<SchemaStatus> {
        let applied = q::applied_migrations(&mut *self.reader().await?).await?;
        let known: Vec<i64> = self
            .migrator
            .iter()
            .filter(|m| !m.migration_type.is_down_migration())
            .map(|m| m.version)
            .collect();
        Ok(SchemaStatus::compare(&applied, &known))
    }

    async fn begin(&self) -> Result<Box<dyn Tx>> {
        Ok(Box::new(self.begin_tx().await?))
    }

    async fn user_count(&self) -> Result<u64> {
        q::user_count(&mut *self.reader().await?).await
    }
}

/// A write transaction, or a savepoint within one, on the writer connection.
#[derive(Debug)]
pub struct SqliteTx<'c> {
    tx: Transaction<'c, Sqlite>,
}

impl SqliteTx<'_> {
    /// The transaction's connection, for statements of your own (tests and tools).
    pub fn conn(&mut self) -> &mut SqliteConnection {
        &mut self.tx
    }

    /// A savepoint within this transaction, with direct access to its connection.
    pub async fn savepoint_tx(&mut self) -> Result<SqliteTx<'_>> {
        Ok(SqliteTx {
            tx: (&mut self.tx).begin().await?,
        })
    }
}

#[async_trait]
impl Tx for SqliteTx<'_> {
    async fn commit(self: Box<Self>) -> Result<()> {
        Ok(self.tx.commit().await?)
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(self.tx.rollback().await?)
    }

    async fn savepoint(&mut self) -> Result<Box<dyn Tx + '_>> {
        Ok(Box::new(self.savepoint_tx().await?))
    }
}

/// One function per query; the trait methods call these with a reader or the transaction's
/// connection.
mod q {
    use super::*;

    pub async fn health(c: &mut SqliteConnection) -> Result<()> {
        sqlx::query(sql::HEALTH).execute(c).await?;
        Ok(())
    }

    /// Versions recorded in `_sqlx_migrations`; none when the table does not exist.
    pub async fn applied_migrations(c: &mut SqliteConnection) -> Result<Vec<i64>> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
        )
        .fetch_one(&mut *c)
        .await?;
        if exists == 0 {
            return Ok(Vec::new());
        }
        Ok(sqlx::query_scalar(sql::APPLIED_MIGRATIONS)
            .bind(true)
            .fetch_all(c)
            .await?)
    }

    /// Whether the database has any table besides SQLite's own and the migration record.
    pub async fn has_tables(c: &mut SqliteConnection) -> Result<bool> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' \
             AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name <> '_sqlx_migrations'",
        )
        .fetch_one(c)
        .await?;
        Ok(n > 0)
    }

    pub async fn user_count(c: &mut SqliteConnection) -> Result<u64> {
        let n: i64 = sqlx::query_scalar(sql::USER_COUNT).fetch_one(c).await?;
        Ok(u64::try_from(n)?)
    }
}
