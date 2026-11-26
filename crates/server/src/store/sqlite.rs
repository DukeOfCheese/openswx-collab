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
//!
//! # The re-entrancy guard
//!
//! With one writer connection, a task that holds a transaction and asks for another would wait for
//! itself, and every other writer would wait behind it. So the store remembers which tokio task
//! holds the transaction (for a future driven by `block_on`, which has no task id: which thread),
//! and [`SqliteStore::begin_tx`] (hence [`Store::begin`]) and
//! [`SqliteStore::migrate`] fail at once when called from that task: "begin() called while this task
//! holds a transaction; use savepoint()". The holder is set when the transaction starts, moves to
//! another task when that task uses the transaction (through [`SqliteTx::conn`] or any [`Tx`]
//! method), and is cleared when the transaction ends (commit, rollback or drop), before the
//! connection goes back to the pool.
//!
//! False positives (an error where waiting would have worked):
//! - two independent transactional futures joined within one task (`join!`, `select!`): the second
//!   `begin` fails instead of queueing. Spawn them as tasks instead;
//! - a task that handed its transaction to another task and calls `begin` before that task has used
//!   it.
//!
//! Only `begin_tx` and `migrate` reach the writer connection; every [`Store`] method reads, on a
//! `query_only` connection. A write belongs on [`Tx`], so it is always under the guard.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use sqlx::migrate::Migrator;
use sqlx::pool::PoolConnection;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqlitePool, SqlitePoolOptions,
};
use sqlx::{Acquire, Sqlite, Transaction};

use super::{
    AssemblyRefRow, AssemblyUse, AuthoredChange, ChangeEventRow, ChangeRow, ChangeSize, Document,
    Folder, HistoryRow, Id, Invite, LoginCode, Member, Membership, NewChange, OAuthIdentity,
    OAuthState, Org, OrgMember, OrgSummary, PartRow, PasswordReset, PinnedComponent, Project,
    ProjectMember, ProjectWithRole, RoomLease, SchemaError, SchemaStatus, Session, SnapshotRow,
    Store, Tx, UndoTarget, User, Version, VersionWithAuthor, sql,
};
use crate::config::{Database, parse_database_url};

/// The migrations in `migrations/sqlite/`, embedded in the binary.
pub static MIGRATOR: Migrator = sqlx::migrate!("migrations/sqlite");

/// How long SQLite waits for a lock held by another process.
const BUSY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a caller waits for a connection (for a writer: for the open transaction to end).
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
/// Reader connections.
const READERS: u32 = 4;
/// What a nested `begin` answers.
const NESTED_BEGIN: &str = "begin() called while this task holds a transaction; use savepoint()";

/// Who is running: a spawned tokio task, or a future driven by `block_on` (`#[tokio::main]`,
/// `#[tokio::test]`), which has no task id; at most one of those runs on a thread at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Caller {
    Task(tokio::task::Id),
    Thread(std::thread::ThreadId),
}

impl Caller {
    fn current() -> Self {
        match tokio::task::try_id() {
            Some(id) => Self::Task(id),
            None => Self::Thread(std::thread::current().id()),
        }
    }
}

/// Who holds the write transaction, if anyone (see the module documentation).
type Holder = Arc<Mutex<Option<Caller>>>;

fn holder_get(holder: &Holder) -> Option<Caller> {
    *holder.lock().unwrap_or_else(PoisonError::into_inner)
}

fn holder_set(holder: &Holder, caller: Option<Caller>) {
    *holder.lock().unwrap_or_else(PoisonError::into_inner) = caller;
}

/// A SQLite database file. Cloning is cheap: clones share the connections.
#[derive(Clone)]
pub struct SqliteStore {
    writer: SqlitePool,
    readers: SqlitePool,
    migrator: &'static Migrator,
    holder: Holder,
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
        store.bring_up_to_date(auto_migrate).await?;
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
            holder: Holder::default(),
        })
    }

    /// Applies the pending migrations, on the writer connection.
    async fn run_migrations(&self) -> Result<()> {
        Ok(self.migrator.run(&self.writer).await?)
    }

    /// A write transaction with direct access to its connection (for tests and tools; handlers use
    /// [`Store::begin`]). It waits while another write transaction is open.
    pub async fn begin_tx(&self) -> Result<SqliteTx<'static>> {
        self.refuse_nested()?;
        let tx = self.writer.begin_with("BEGIN IMMEDIATE").await?;
        holder_set(&self.holder, Some(Caller::current()));
        Ok(SqliteTx {
            claim: Some(Claim(self.holder.clone())),
            tx,
        })
    }

    /// [`Store::begin_journal`]: SQLite has one writer for everyone.
    pub async fn begin_journal_tx(&self) -> Result<SqliteTx<'static>> {
        self.begin_tx().await
    }

    /// Fails when the calling task holds the write transaction (it would wait for itself).
    fn refuse_nested(&self) -> Result<()> {
        if holder_get(&self.holder) == Some(Caller::current()) {
            bail!(NESTED_BEGIN);
        }
        Ok(())
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

/// A top-level transaction's hold on the guard: the holder follows the task using the transaction,
/// and is cleared when this is dropped.
#[derive(Debug)]
struct Claim(Holder);

impl Claim {
    /// Records the calling task as the holder (the transaction may have moved to another task).
    fn touch(&self) {
        holder_set(&self.0, Some(Caller::current()));
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        holder_set(&self.0, None);
    }
}

/// A write transaction, or a savepoint within one, on the writer connection.
#[derive(Debug)]
pub struct SqliteTx<'c> {
    // Declared first so that it is dropped first: the guard is cleared before the connection goes
    // back to the pool, where the next transaction sets it. `None` for a savepoint.
    claim: Option<Claim>,
    tx: Transaction<'c, Sqlite>,
}

impl SqliteTx<'_> {
    /// The transaction's connection. Every query of a [`Tx`] method runs through this, so the guard
    /// follows the transaction to the task that uses it.
    pub fn conn(&mut self) -> &mut SqliteConnection {
        self.touch();
        &mut self.tx
    }

    /// A savepoint within this transaction, with direct access to its connection.
    pub async fn savepoint_tx(&mut self) -> Result<SqliteTx<'_>> {
        self.touch();
        Ok(SqliteTx {
            claim: None,
            tx: (&mut self.tx).begin().await?,
        })
    }

    fn touch(&self) {
        if let Some(claim) = &self.claim {
            claim.touch();
        }
    }

    /// Makes the writes permanent (for a savepoint: part of the enclosing transaction).
    pub async fn commit_tx(self) -> Result<()> {
        let SqliteTx { claim, tx } = self;
        drop(claim); // before the connection is released
        Ok(tx.commit().await?)
    }

    /// Undoes the writes.
    pub async fn rollback_tx(self) -> Result<()> {
        let SqliteTx { claim, tx } = self;
        drop(claim);
        Ok(tx.rollback().await?)
    }
}

super::shared::store_impl!(SqliteStore, SqliteTx, SqliteConnection, sqlite);
