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
    AssemblyRefRow, ChangeEventRow, ChangeRow, ChangeSize, Document, Folder, HistoryRow, Id,
    Invite, Member, NewChange, Org, OrgMember, OrgSummary, PasswordReset, Project, ProjectMember,
    ProjectWithRole, RoomLease, SchemaError, SchemaStatus, Session, SnapshotRow, Store, Tx,
    UndoTarget, User, Version, VersionWithAuthor, sql,
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
        store.check_unchanged().await?;
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
            holder: Holder::default(),
        })
    }

    /// Applies the migrations this binary carries that the database has not had, and answers the
    /// schema status afterwards. Refuses a database written by a newer server, or not by this one.
    pub async fn migrate(&self) -> Result<SchemaStatus> {
        self.refuse_nested()?;
        self.check_unchanged().await?;
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
        self.refuse_nested()?;
        let tx = self.writer.begin_with("BEGIN IMMEDIATE").await?;
        holder_set(&self.holder, Some(Caller::current()));
        Ok(SqliteTx {
            claim: Some(Claim(self.holder.clone())),
            tx,
        })
    }

    /// Fails when the calling task holds the write transaction (it would wait for itself).
    fn refuse_nested(&self) -> Result<()> {
        if holder_get(&self.holder) == Some(Caller::current()) {
            bail!(NESTED_BEGIN);
        }
        Ok(())
    }

    /// Fails when a migration the database has had differs from the one this binary carries under
    /// the same version (sqlx checks this only when it has something to apply).
    async fn check_unchanged(&self) -> Result<()> {
        for (version, checksum) in q::applied_checksums(&mut *self.reader().await?).await? {
            let changed = self.migrator.iter().any(|m| {
                m.version == version
                    && !m.migration_type.is_down_migration()
                    && *m.checksum != *checksum
            });
            if changed {
                bail!(SchemaError::Changed { version });
            }
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

#[async_trait]
impl Store for SqliteStore {
    async fn health(&self) -> Result<()> {
        q::health(&mut *self.reader().await?).await
    }

    async fn schema_status(&self) -> Result<SchemaStatus> {
        let applied: Vec<i64> = q::applied_checksums(&mut *self.reader().await?)
            .await?
            .into_iter()
            .map(|(version, _)| version)
            .collect();
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

    async fn user_by_id(&self, id: &str) -> Result<Option<User>> {
        q::user_by_id(&mut *self.reader().await?, id).await
    }

    async fn user_by_email(&self, email: &str) -> Result<Option<User>> {
        q::user_by_email(&mut *self.reader().await?, email).await
    }

    async fn session_by_hash(&self, token_hash: &str) -> Result<Option<Session>> {
        q::session_by_hash(&mut *self.reader().await?, token_hash).await
    }

    async fn password_reset_by_hash(&self, token_hash: &str) -> Result<Option<PasswordReset>> {
        q::password_reset_by_hash(&mut *self.reader().await?, token_hash).await
    }

    async fn org_by_id(&self, id: &str) -> Result<Option<Org>> {
        q::org_by_id(&mut *self.reader().await?, id).await
    }

    async fn org_member(&self, org_id: &str, user_id: &str) -> Result<Option<OrgMember>> {
        q::org_member(&mut *self.reader().await?, org_id, user_id).await
    }

    async fn orgs_of_user(&self, user_id: &str) -> Result<Vec<OrgSummary>> {
        q::orgs_of_user(&mut *self.reader().await?, user_id).await
    }

    async fn org_members(&self, org_id: &str) -> Result<Vec<Member>> {
        q::org_members(&mut *self.reader().await?, org_id).await
    }

    async fn org_member_count(&self, org_id: &str) -> Result<u64> {
        q::org_member_count(&mut *self.reader().await?, org_id).await
    }

    async fn project_by_id(&self, id: &str) -> Result<Option<Project>> {
        q::project_by_id(&mut *self.reader().await?, id).await
    }

    async fn project_member(
        &self,
        project_id: &str,
        user_id: &str,
    ) -> Result<Option<ProjectMember>> {
        q::project_member(&mut *self.reader().await?, project_id, user_id).await
    }

    async fn projects_in_org(&self, org_id: &str) -> Result<Vec<Project>> {
        q::projects_in_org(&mut *self.reader().await?, org_id).await
    }

    async fn member_projects(&self, org_id: &str, user_id: &str) -> Result<Vec<ProjectWithRole>> {
        q::member_projects(&mut *self.reader().await?, org_id, user_id).await
    }

    async fn project_members(&self, project_id: &str) -> Result<Vec<Member>> {
        q::project_members(&mut *self.reader().await?, project_id).await
    }

    async fn invite_by_id(&self, id: &str) -> Result<Option<Invite>> {
        q::invite_by_id(&mut *self.reader().await?, id).await
    }

    async fn invite_by_hash(&self, token_hash: &str) -> Result<Option<Invite>> {
        q::invite_by_hash(&mut *self.reader().await?, token_hash).await
    }

    async fn open_invites(&self, org_id: &str, now: f64) -> Result<Vec<Invite>> {
        q::open_invites(&mut *self.reader().await?, org_id, now).await
    }

    async fn document_by_id(&self, id: &str) -> Result<Option<Document>> {
        q::document_by_id(&mut *self.reader().await?, id).await
    }

    async fn documents_in_project(&self, project_id: &str) -> Result<Vec<Document>> {
        q::documents_in_project(&mut *self.reader().await?, project_id).await
    }

    async fn latest_snapshot(&self, document_id: &str) -> Result<Option<SnapshotRow>> {
        q::latest_snapshot(&mut *self.reader().await?, document_id).await
    }

    async fn folder_by_id(&self, id: &str) -> Result<Option<Folder>> {
        q::folder_by_id(&mut *self.reader().await?, id).await
    }

    async fn folders_in_project(&self, project_id: &str) -> Result<Vec<Folder>> {
        q::folders_in_project(&mut *self.reader().await?, project_id).await
    }

    async fn changes_between(
        &self,
        document_id: &str,
        after: i64,
        upto: i64,
        limit: i64,
    ) -> Result<Vec<ChangeRow>> {
        q::changes_between(&mut *self.reader().await?, document_id, after, upto, limit).await
    }

    async fn change_by_seq(&self, document_id: &str, seq: i64) -> Result<Option<ChangeRow>> {
        q::change_by_seq(&mut *self.reader().await?, document_id, seq).await
    }

    async fn change_events_between(
        &self,
        document_id: &str,
        after: i64,
        upto: i64,
        limit: i64,
    ) -> Result<Vec<ChangeEventRow>> {
        q::change_events_between(&mut *self.reader().await?, document_id, after, upto, limit).await
    }

    async fn change_seq_by_client(
        &self,
        document_id: &str,
        client_id: &str,
        change_id: &str,
    ) -> Result<Option<i64>> {
        q::change_seq_by_client(
            &mut *self.reader().await?,
            document_id,
            client_id,
            change_id,
        )
        .await
    }

    async fn recent_change_ids(
        &self,
        document_id: &str,
        client_id: &str,
        limit: i64,
    ) -> Result<Vec<(String, i64)>> {
        q::recent_change_ids(&mut *self.reader().await?, document_id, client_id, limit).await
    }

    async fn undo_target(
        &self,
        document_id: &str,
        client_id: &str,
        change_id: &str,
    ) -> Result<Option<UndoTarget>> {
        q::undo_target(
            &mut *self.reader().await?,
            document_id,
            client_id,
            change_id,
        )
        .await
    }

    async fn change_sizes(
        &self,
        document_id: &str,
        before: i64,
        after: i64,
        limit: i64,
    ) -> Result<Vec<ChangeSize>> {
        q::change_sizes(
            &mut *self.reader().await?,
            document_id,
            before,
            after,
            limit,
        )
        .await
    }

    async fn history(
        &self,
        document_id: &str,
        before: i64,
        after: i64,
        limit: i64,
    ) -> Result<Vec<HistoryRow>> {
        q::history(
            &mut *self.reader().await?,
            document_id,
            before,
            after,
            limit,
        )
        .await
    }

    async fn versions(&self, document_id: &str) -> Result<Vec<VersionWithAuthor>> {
        q::versions(&mut *self.reader().await?, document_id).await
    }

    async fn version_by_name(&self, document_id: &str, name: &str) -> Result<Option<Version>> {
        q::version_by_name(&mut *self.reader().await?, document_id, name).await
    }

    async fn snapshot_at_or_before(
        &self,
        document_id: &str,
        seq: i64,
    ) -> Result<Option<SnapshotRow>> {
        q::snapshot_at_or_before(&mut *self.reader().await?, document_id, seq).await
    }

    async fn snapshot_row(
        &self,
        document_id: &str,
        epoch: i64,
        seq: i64,
    ) -> Result<Option<SnapshotRow>> {
        q::snapshot_row(&mut *self.reader().await?, document_id, epoch, seq).await
    }

    async fn room_lease(&self, document_id: &str) -> Result<Option<RoomLease>> {
        q::room_lease(&mut *self.reader().await?, document_id).await
    }

    async fn assembly_refs(&self, assembly_id: &str) -> Result<Vec<AssemblyRefRow>> {
        q::assembly_refs(&mut *self.reader().await?, assembly_id).await
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
}

#[async_trait]
impl Tx for SqliteTx<'_> {
    async fn commit(self: Box<Self>) -> Result<()> {
        let SqliteTx { claim, tx } = *self;
        drop(claim); // before the connection is released
        Ok(tx.commit().await?)
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        let SqliteTx { claim, tx } = *self;
        drop(claim);
        Ok(tx.rollback().await?)
    }

    async fn savepoint(&mut self) -> Result<Box<dyn Tx + '_>> {
        Ok(Box::new(self.savepoint_tx().await?))
    }

    async fn user_count(&mut self) -> Result<u64> {
        q::user_count(self.conn()).await
    }

    async fn user_by_id(&mut self, id: &str) -> Result<Option<User>> {
        q::user_by_id(self.conn(), id).await
    }

    async fn user_by_email(&mut self, email: &str) -> Result<Option<User>> {
        q::user_by_email(self.conn(), email).await
    }

    async fn insert_user(&mut self, user: &User) -> Result<()> {
        q::insert_user(self.conn(), user).await
    }

    async fn set_user_name(&mut self, id: &str, name: &str) -> Result<()> {
        q::set_user_name(self.conn(), id, name).await
    }

    async fn set_user_password(&mut self, id: &str, password_hash: Option<&str>) -> Result<()> {
        q::set_user_password(self.conn(), id, password_hash).await
    }

    async fn set_user_email_verified(&mut self, id: &str, verified: bool) -> Result<()> {
        q::set_user_email_verified(self.conn(), id, verified).await
    }

    async fn set_user_disabled(&mut self, id: &str, disabled: bool) -> Result<()> {
        q::set_user_disabled(self.conn(), id, disabled).await
    }

    async fn insert_session(&mut self, session: &Session) -> Result<()> {
        q::insert_session(self.conn(), session).await
    }

    async fn delete_session(&mut self, token_hash: &str) -> Result<()> {
        q::delete_session(self.conn(), token_hash).await
    }

    async fn delete_user_sessions(&mut self, user_id: &str) -> Result<()> {
        q::delete_user_sessions(self.conn(), user_id).await
    }

    async fn delete_other_user_sessions(&mut self, user_id: &str, keep: &str) -> Result<()> {
        q::delete_other_user_sessions(self.conn(), user_id, keep).await
    }

    async fn delete_expired_user_sessions(&mut self, user_id: &str, now: f64) -> Result<()> {
        q::delete_expired_user_sessions(self.conn(), user_id, now).await
    }

    async fn insert_password_reset(&mut self, reset: &PasswordReset) -> Result<()> {
        q::insert_password_reset(self.conn(), reset).await
    }

    async fn take_password_reset(&mut self, token_hash: &str) -> Result<Option<PasswordReset>> {
        q::take_password_reset(self.conn(), token_hash).await
    }

    async fn delete_user_password_resets(&mut self, user_id: &str) -> Result<()> {
        q::delete_user_password_resets(self.conn(), user_id).await
    }

    async fn delete_expired_password_resets(&mut self, now: f64) -> Result<()> {
        q::delete_expired_password_resets(self.conn(), now).await
    }

    async fn insert_org(&mut self, org: &Org) -> Result<()> {
        q::insert_org(self.conn(), org).await
    }

    async fn set_org_name(&mut self, id: &str, name: &str) -> Result<()> {
        q::set_org_name(self.conn(), id, name).await
    }

    async fn org_by_id(&mut self, id: &str) -> Result<Option<Org>> {
        q::org_by_id(self.conn(), id).await
    }

    async fn org_member(&mut self, org_id: &str, user_id: &str) -> Result<Option<OrgMember>> {
        q::org_member(self.conn(), org_id, user_id).await
    }

    async fn insert_org_member(&mut self, member: &OrgMember) -> Result<()> {
        q::insert_org_member(self.conn(), member).await
    }

    async fn set_org_member_role(&mut self, org_id: &str, user_id: &str, role: &str) -> Result<()> {
        q::set_org_member_role(self.conn(), org_id, user_id, role).await
    }

    async fn delete_org_member(&mut self, org_id: &str, user_id: &str) -> Result<()> {
        q::delete_org_member(self.conn(), org_id, user_id).await
    }

    async fn org_member_count(&mut self, org_id: &str) -> Result<u64> {
        q::org_member_count(self.conn(), org_id).await
    }

    async fn org_owner_count(&mut self, org_id: &str) -> Result<u64> {
        q::org_owner_count(self.conn(), org_id).await
    }

    async fn delete_project_members_in_org(&mut self, org_id: &str, user_id: &str) -> Result<()> {
        q::delete_project_members_in_org(self.conn(), org_id, user_id).await
    }

    async fn invite_by_hash(&mut self, token_hash: &str) -> Result<Option<Invite>> {
        q::invite_by_hash(self.conn(), token_hash).await
    }

    async fn insert_invite(&mut self, invite: &Invite) -> Result<()> {
        q::insert_invite(self.conn(), invite).await
    }

    async fn claim_invite(&mut self, id: &str, user_id: &str) -> Result<bool> {
        q::claim_invite(self.conn(), id, user_id).await
    }

    async fn delete_invite(&mut self, org_id: &str, id: &str) -> Result<bool> {
        q::delete_invite(self.conn(), org_id, id).await
    }

    async fn insert_project(&mut self, project: &Project) -> Result<()> {
        q::insert_project(self.conn(), project).await
    }

    async fn set_project_name(&mut self, id: &str, name: &str) -> Result<()> {
        q::set_project_name(self.conn(), id, name).await
    }

    async fn delete_project(&mut self, id: &str) -> Result<()> {
        q::delete_project(self.conn(), id).await
    }

    async fn project_by_id(&mut self, id: &str) -> Result<Option<Project>> {
        q::project_by_id(self.conn(), id).await
    }

    async fn project_member(
        &mut self,
        project_id: &str,
        user_id: &str,
    ) -> Result<Option<ProjectMember>> {
        q::project_member(self.conn(), project_id, user_id).await
    }

    async fn insert_project_member(&mut self, member: &ProjectMember) -> Result<()> {
        q::insert_project_member(self.conn(), member).await
    }

    async fn set_project_member_role(
        &mut self,
        project_id: &str,
        user_id: &str,
        role: &str,
    ) -> Result<()> {
        q::set_project_member_role(self.conn(), project_id, user_id, role).await
    }

    async fn delete_project_member(&mut self, project_id: &str, user_id: &str) -> Result<bool> {
        q::delete_project_member(self.conn(), project_id, user_id).await
    }

    async fn project_document_ids(&mut self, project_id: &str) -> Result<Vec<Id>> {
        q::project_document_ids(self.conn(), project_id).await
    }

    async fn mark_project_documents_deleted(&mut self, project_id: &str, now: f64) -> Result<()> {
        q::mark_project_documents_deleted(self.conn(), project_id, now).await
    }

    async fn insert_document(&mut self, document: &Document) -> Result<()> {
        q::insert_document(self.conn(), document).await
    }

    async fn insert_snapshot(&mut self, snapshot: &SnapshotRow) -> Result<()> {
        q::insert_snapshot(self.conn(), snapshot).await
    }

    async fn document_by_id(&mut self, id: &str) -> Result<Option<Document>> {
        q::document_by_id(self.conn(), id).await
    }

    async fn set_document_name(&mut self, id: &str, name: &str) -> Result<()> {
        q::set_document_name(self.conn(), id, name).await
    }

    async fn set_document_folder(&mut self, id: &str, folder_id: Option<&str>) -> Result<()> {
        q::set_document_folder(self.conn(), id, folder_id).await
    }

    async fn mark_document_deleted(&mut self, id: &str, now: f64) -> Result<bool> {
        q::mark_document_deleted(self.conn(), id, now).await
    }

    async fn insert_folder(&mut self, folder: &Folder) -> Result<()> {
        q::insert_folder(self.conn(), folder).await
    }

    async fn folder_by_id(&mut self, id: &str) -> Result<Option<Folder>> {
        q::folder_by_id(self.conn(), id).await
    }

    async fn folders_in_project(&mut self, project_id: &str) -> Result<Vec<Folder>> {
        q::folders_in_project(self.conn(), project_id).await
    }

    async fn set_folder_name(&mut self, id: &str, name: &str) -> Result<()> {
        q::set_folder_name(self.conn(), id, name).await
    }

    async fn set_folder_parent(&mut self, id: &str, parent_id: Option<&str>) -> Result<()> {
        q::set_folder_parent(self.conn(), id, parent_id).await
    }

    async fn move_child_folders(&mut self, folder_id: &str, parent_id: Option<&str>) -> Result<()> {
        q::move_child_folders(self.conn(), folder_id, parent_id).await
    }

    async fn move_folder_documents(
        &mut self,
        folder_id: &str,
        parent_id: Option<&str>,
    ) -> Result<()> {
        q::move_folder_documents(self.conn(), folder_id, parent_id).await
    }

    async fn delete_folder(&mut self, id: &str) -> Result<()> {
        q::delete_folder(self.conn(), id).await
    }

    async fn insert_change(&mut self, change: &NewChange<'_>) -> Result<bool> {
        q::insert_change(self.conn(), change).await
    }

    async fn set_document_head(&mut self, id: &str, head_seq: i64, updated_at: f64) -> Result<()> {
        q::set_document_head(self.conn(), id, head_seq, updated_at).await
    }

    async fn set_reverted_by(&mut self, document_id: &str, seq: i64, by: i64) -> Result<bool> {
        q::set_reverted_by(self.conn(), document_id, seq, by).await
    }

    async fn change_broke(
        &mut self,
        document_id: &str,
        seq: i64,
    ) -> Result<Option<Option<String>>> {
        q::change_broke(self.conn(), document_id, seq).await
    }

    async fn set_change_broke(&mut self, document_id: &str, seq: i64, broke: &str) -> Result<()> {
        q::set_change_broke(self.conn(), document_id, seq, broke).await
    }

    async fn insert_version(&mut self, version: &Version) -> Result<bool> {
        q::insert_version(self.conn(), version).await
    }

    async fn delete_version(&mut self, document_id: &str, id: &str) -> Result<bool> {
        q::delete_version(self.conn(), document_id, id).await
    }

    async fn replace_assembly_refs(
        &mut self,
        assembly_id: &str,
        rows: &[AssemblyRefRow],
    ) -> Result<()> {
        q::replace_assembly_refs(self.conn(), assembly_id, rows).await
    }

    async fn snapshot_row(
        &mut self,
        document_id: &str,
        epoch: i64,
        seq: i64,
    ) -> Result<Option<SnapshotRow>> {
        q::snapshot_row(self.conn(), document_id, epoch, seq).await
    }

    async fn room_lease(&mut self, document_id: &str) -> Result<Option<RoomLease>> {
        q::room_lease(self.conn(), document_id).await
    }

    async fn lock_room_lease(&mut self, document_id: &str) -> Result<Option<i64>> {
        q::lock_room_lease(self.conn(), document_id).await
    }

    async fn insert_room_lease(&mut self, lease: &RoomLease) -> Result<()> {
        q::insert_room_lease(self.conn(), lease).await
    }

    async fn take_room_lease(
        &mut self,
        document_id: &str,
        node_id: &str,
        expires_at: f64,
    ) -> Result<Option<i64>> {
        q::take_room_lease(self.conn(), document_id, node_id, expires_at).await
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

    /// Versions and checksums recorded in `_sqlx_migrations`; none when the table does not exist.
    pub async fn applied_checksums(c: &mut SqliteConnection) -> Result<Vec<(i64, Vec<u8>)>> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
        )
        .fetch_one(&mut *c)
        .await?;
        if exists == 0 {
            return Ok(Vec::new());
        }
        Ok(sqlx::query_as(sql::APPLIED_MIGRATIONS)
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

    pub async fn user_by_id(c: &mut SqliteConnection, id: &str) -> Result<Option<User>> {
        Ok(sqlx::query_as(sql::USER_BY_ID)
            .bind(id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn user_by_email(c: &mut SqliteConnection, email: &str) -> Result<Option<User>> {
        Ok(sqlx::query_as(sql::USER_BY_EMAIL)
            .bind(email)
            .fetch_optional(c)
            .await?)
    }

    pub async fn insert_user(c: &mut SqliteConnection, u: &User) -> Result<()> {
        sqlx::query(sql::INSERT_USER)
            .bind(&u.id)
            .bind(&u.email)
            .bind(&u.name)
            .bind(&u.password_hash)
            .bind(u.is_server_admin)
            .bind(u.disabled)
            .bind(u.email_verified)
            .bind(u.created_at)
            .bind(u.deleted_at)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn set_user_name(c: &mut SqliteConnection, id: &str, name: &str) -> Result<()> {
        sqlx::query(sql::SET_USER_NAME)
            .bind(id)
            .bind(name)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn set_user_password(
        c: &mut SqliteConnection,
        id: &str,
        password_hash: Option<&str>,
    ) -> Result<()> {
        sqlx::query(sql::SET_USER_PASSWORD)
            .bind(id)
            .bind(password_hash)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn set_user_email_verified(
        c: &mut SqliteConnection,
        id: &str,
        verified: bool,
    ) -> Result<()> {
        sqlx::query(sql::SET_USER_EMAIL_VERIFIED)
            .bind(id)
            .bind(verified)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn set_user_disabled(
        c: &mut SqliteConnection,
        id: &str,
        disabled: bool,
    ) -> Result<()> {
        sqlx::query(sql::SET_USER_DISABLED)
            .bind(id)
            .bind(disabled)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn insert_session(c: &mut SqliteConnection, s: &Session) -> Result<()> {
        sqlx::query(sql::INSERT_SESSION)
            .bind(&s.token_hash)
            .bind(&s.user_id)
            .bind(s.created_at)
            .bind(s.expires_at)
            .bind(&s.label)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn session_by_hash(
        c: &mut SqliteConnection,
        token_hash: &str,
    ) -> Result<Option<Session>> {
        Ok(sqlx::query_as(sql::SESSION_BY_HASH)
            .bind(token_hash)
            .fetch_optional(c)
            .await?)
    }

    pub async fn delete_session(c: &mut SqliteConnection, token_hash: &str) -> Result<()> {
        sqlx::query(sql::DELETE_SESSION)
            .bind(token_hash)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn delete_user_sessions(c: &mut SqliteConnection, user_id: &str) -> Result<()> {
        sqlx::query(sql::DELETE_USER_SESSIONS)
            .bind(user_id)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn delete_other_user_sessions(
        c: &mut SqliteConnection,
        user_id: &str,
        keep: &str,
    ) -> Result<()> {
        sqlx::query(sql::DELETE_OTHER_USER_SESSIONS)
            .bind(user_id)
            .bind(keep)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn delete_expired_user_sessions(
        c: &mut SqliteConnection,
        user_id: &str,
        now: f64,
    ) -> Result<()> {
        sqlx::query(sql::DELETE_EXPIRED_USER_SESSIONS)
            .bind(user_id)
            .bind(now)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn insert_password_reset(c: &mut SqliteConnection, r: &PasswordReset) -> Result<()> {
        sqlx::query(sql::INSERT_PASSWORD_RESET)
            .bind(&r.token_hash)
            .bind(&r.user_id)
            .bind(r.expires_at)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn password_reset_by_hash(
        c: &mut SqliteConnection,
        token_hash: &str,
    ) -> Result<Option<PasswordReset>> {
        Ok(sqlx::query_as(sql::PASSWORD_RESET_BY_HASH)
            .bind(token_hash)
            .fetch_optional(c)
            .await?)
    }

    pub async fn take_password_reset(
        c: &mut SqliteConnection,
        token_hash: &str,
    ) -> Result<Option<PasswordReset>> {
        Ok(sqlx::query_as(sql::TAKE_PASSWORD_RESET)
            .bind(token_hash)
            .fetch_optional(c)
            .await?)
    }

    pub async fn delete_user_password_resets(
        c: &mut SqliteConnection,
        user_id: &str,
    ) -> Result<()> {
        sqlx::query(sql::DELETE_USER_PASSWORD_RESETS)
            .bind(user_id)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn delete_expired_password_resets(c: &mut SqliteConnection, now: f64) -> Result<()> {
        sqlx::query(sql::DELETE_EXPIRED_PASSWORD_RESETS)
            .bind(now)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn insert_org(c: &mut SqliteConnection, o: &Org) -> Result<()> {
        sqlx::query(sql::INSERT_ORG)
            .bind(&o.id)
            .bind(&o.name)
            .bind(o.created_at)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn set_org_name(c: &mut SqliteConnection, id: &str, name: &str) -> Result<()> {
        sqlx::query(sql::SET_ORG_NAME)
            .bind(id)
            .bind(name)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn org_by_id(c: &mut SqliteConnection, id: &str) -> Result<Option<Org>> {
        Ok(sqlx::query_as(sql::ORG_BY_ID)
            .bind(id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn org_member(
        c: &mut SqliteConnection,
        org_id: &str,
        user_id: &str,
    ) -> Result<Option<OrgMember>> {
        Ok(sqlx::query_as(sql::ORG_MEMBER)
            .bind(org_id)
            .bind(user_id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn orgs_of_user(c: &mut SqliteConnection, user_id: &str) -> Result<Vec<OrgSummary>> {
        Ok(sqlx::query_as(sql::ORGS_OF_USER)
            .bind(user_id)
            .fetch_all(c)
            .await?)
    }

    pub async fn org_members(c: &mut SqliteConnection, org_id: &str) -> Result<Vec<Member>> {
        Ok(sqlx::query_as(sql::ORG_MEMBERS)
            .bind(org_id)
            .fetch_all(c)
            .await?)
    }

    pub async fn insert_org_member(c: &mut SqliteConnection, m: &OrgMember) -> Result<()> {
        sqlx::query(sql::INSERT_ORG_MEMBER)
            .bind(&m.org_id)
            .bind(&m.user_id)
            .bind(&m.role)
            .bind(m.created_at)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn set_org_member_role(
        c: &mut SqliteConnection,
        org_id: &str,
        user_id: &str,
        role: &str,
    ) -> Result<()> {
        sqlx::query(sql::SET_ORG_MEMBER_ROLE)
            .bind(org_id)
            .bind(user_id)
            .bind(role)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn delete_org_member(
        c: &mut SqliteConnection,
        org_id: &str,
        user_id: &str,
    ) -> Result<()> {
        sqlx::query(sql::DELETE_ORG_MEMBER)
            .bind(org_id)
            .bind(user_id)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn org_member_count(c: &mut SqliteConnection, org_id: &str) -> Result<u64> {
        let n: i64 = sqlx::query_scalar(sql::ORG_MEMBER_COUNT)
            .bind(org_id)
            .fetch_one(c)
            .await?;
        Ok(u64::try_from(n)?)
    }

    pub async fn org_owner_count(c: &mut SqliteConnection, org_id: &str) -> Result<u64> {
        let n: i64 = sqlx::query_scalar(sql::ORG_OWNER_COUNT)
            .bind(org_id)
            .bind("owner")
            .fetch_one(c)
            .await?;
        Ok(u64::try_from(n)?)
    }

    pub async fn delete_project_members_in_org(
        c: &mut SqliteConnection,
        org_id: &str,
        user_id: &str,
    ) -> Result<()> {
        sqlx::query(sql::DELETE_PROJECT_MEMBERS_IN_ORG)
            .bind(org_id)
            .bind(user_id)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn insert_invite(c: &mut SqliteConnection, i: &Invite) -> Result<()> {
        sqlx::query(sql::INSERT_INVITE)
            .bind(&i.id)
            .bind(&i.token_hash)
            .bind(&i.org_id)
            .bind(&i.email)
            .bind(&i.role)
            .bind(&i.projects)
            .bind(&i.created_by)
            .bind(i.created_at)
            .bind(i.expires_at)
            .bind(&i.accepted_by)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn invite_by_id(c: &mut SqliteConnection, id: &str) -> Result<Option<Invite>> {
        Ok(sqlx::query_as(sql::INVITE_BY_ID)
            .bind(id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn invite_by_hash(
        c: &mut SqliteConnection,
        token_hash: &str,
    ) -> Result<Option<Invite>> {
        Ok(sqlx::query_as(sql::INVITE_BY_HASH)
            .bind(token_hash)
            .fetch_optional(c)
            .await?)
    }

    pub async fn open_invites(
        c: &mut SqliteConnection,
        org_id: &str,
        now: f64,
    ) -> Result<Vec<Invite>> {
        Ok(sqlx::query_as(sql::OPEN_INVITES)
            .bind(org_id)
            .bind(now)
            .fetch_all(c)
            .await?)
    }

    pub async fn claim_invite(c: &mut SqliteConnection, id: &str, user_id: &str) -> Result<bool> {
        let done = sqlx::query(sql::CLAIM_INVITE)
            .bind(id)
            .bind(user_id)
            .execute(c)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    pub async fn delete_invite(c: &mut SqliteConnection, org_id: &str, id: &str) -> Result<bool> {
        let done = sqlx::query(sql::DELETE_INVITE)
            .bind(org_id)
            .bind(id)
            .execute(c)
            .await?;
        Ok(done.rows_affected() > 0)
    }

    pub async fn insert_project(c: &mut SqliteConnection, p: &Project) -> Result<()> {
        sqlx::query(sql::INSERT_PROJECT)
            .bind(&p.id)
            .bind(&p.org_id)
            .bind(&p.name)
            .bind(p.created_at)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn set_project_name(c: &mut SqliteConnection, id: &str, name: &str) -> Result<()> {
        sqlx::query(sql::SET_PROJECT_NAME)
            .bind(id)
            .bind(name)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn delete_project(c: &mut SqliteConnection, id: &str) -> Result<()> {
        sqlx::query(sql::DELETE_PROJECT).bind(id).execute(c).await?;
        Ok(())
    }

    pub async fn project_by_id(c: &mut SqliteConnection, id: &str) -> Result<Option<Project>> {
        Ok(sqlx::query_as(sql::PROJECT_BY_ID)
            .bind(id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn projects_in_org(c: &mut SqliteConnection, org_id: &str) -> Result<Vec<Project>> {
        Ok(sqlx::query_as(sql::PROJECTS_IN_ORG)
            .bind(org_id)
            .fetch_all(c)
            .await?)
    }

    pub async fn member_projects(
        c: &mut SqliteConnection,
        org_id: &str,
        user_id: &str,
    ) -> Result<Vec<ProjectWithRole>> {
        Ok(sqlx::query_as(sql::MEMBER_PROJECTS)
            .bind(org_id)
            .bind(user_id)
            .fetch_all(c)
            .await?)
    }

    pub async fn project_member(
        c: &mut SqliteConnection,
        project_id: &str,
        user_id: &str,
    ) -> Result<Option<ProjectMember>> {
        Ok(sqlx::query_as(sql::PROJECT_MEMBER)
            .bind(project_id)
            .bind(user_id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn project_members(
        c: &mut SqliteConnection,
        project_id: &str,
    ) -> Result<Vec<Member>> {
        Ok(sqlx::query_as(sql::PROJECT_MEMBERS)
            .bind(project_id)
            .fetch_all(c)
            .await?)
    }

    pub async fn insert_project_member(c: &mut SqliteConnection, m: &ProjectMember) -> Result<()> {
        sqlx::query(sql::INSERT_PROJECT_MEMBER)
            .bind(&m.project_id)
            .bind(&m.user_id)
            .bind(&m.role)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn set_project_member_role(
        c: &mut SqliteConnection,
        project_id: &str,
        user_id: &str,
        role: &str,
    ) -> Result<()> {
        sqlx::query(sql::SET_PROJECT_MEMBER_ROLE)
            .bind(project_id)
            .bind(user_id)
            .bind(role)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn delete_project_member(
        c: &mut SqliteConnection,
        project_id: &str,
        user_id: &str,
    ) -> Result<bool> {
        let done = sqlx::query(sql::DELETE_PROJECT_MEMBER)
            .bind(project_id)
            .bind(user_id)
            .execute(c)
            .await?;
        Ok(done.rows_affected() > 0)
    }

    pub async fn project_document_ids(
        c: &mut SqliteConnection,
        project_id: &str,
    ) -> Result<Vec<Id>> {
        Ok(sqlx::query_scalar(sql::PROJECT_DOCUMENT_IDS)
            .bind(project_id)
            .fetch_all(c)
            .await?)
    }

    pub async fn mark_project_documents_deleted(
        c: &mut SqliteConnection,
        project_id: &str,
        now: f64,
    ) -> Result<()> {
        sqlx::query(sql::MARK_PROJECT_DOCUMENTS_DELETED)
            .bind(project_id)
            .bind(now)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn insert_document(c: &mut SqliteConnection, d: &Document) -> Result<()> {
        sqlx::query(sql::INSERT_DOCUMENT)
            .bind(&d.id)
            .bind(&d.project_id)
            .bind(&d.folder_id)
            .bind(&d.name)
            .bind(&d.kind)
            .bind(d.format_version)
            .bind(d.head_seq)
            .bind(&d.created_by)
            .bind(d.created_at)
            .bind(d.updated_at)
            .bind(d.deleted_at)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn document_by_id(c: &mut SqliteConnection, id: &str) -> Result<Option<Document>> {
        Ok(sqlx::query_as(sql::DOCUMENT_BY_ID)
            .bind(id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn documents_in_project(
        c: &mut SqliteConnection,
        project_id: &str,
    ) -> Result<Vec<Document>> {
        Ok(sqlx::query_as(sql::DOCUMENTS_IN_PROJECT)
            .bind(project_id)
            .fetch_all(c)
            .await?)
    }

    pub async fn set_document_name(c: &mut SqliteConnection, id: &str, name: &str) -> Result<()> {
        sqlx::query(sql::SET_DOCUMENT_NAME)
            .bind(id)
            .bind(name)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn set_document_folder(
        c: &mut SqliteConnection,
        id: &str,
        folder_id: Option<&str>,
    ) -> Result<()> {
        sqlx::query(sql::SET_DOCUMENT_FOLDER)
            .bind(id)
            .bind(folder_id)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn mark_document_deleted(
        c: &mut SqliteConnection,
        id: &str,
        now: f64,
    ) -> Result<bool> {
        let done = sqlx::query(sql::MARK_DOCUMENT_DELETED)
            .bind(id)
            .bind(now)
            .execute(c)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    pub async fn insert_snapshot(c: &mut SqliteConnection, s: &SnapshotRow) -> Result<()> {
        sqlx::query(sql::INSERT_SNAPSHOT)
            .bind(&s.document_id)
            .bind(s.epoch)
            .bind(s.seq)
            .bind(&s.key)
            .bind(s.size)
            .bind(s.created_at)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn latest_snapshot(
        c: &mut SqliteConnection,
        document_id: &str,
    ) -> Result<Option<SnapshotRow>> {
        Ok(sqlx::query_as(sql::LATEST_SNAPSHOT)
            .bind(document_id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn insert_folder(c: &mut SqliteConnection, f: &Folder) -> Result<()> {
        sqlx::query(sql::INSERT_FOLDER)
            .bind(&f.id)
            .bind(&f.project_id)
            .bind(&f.parent_id)
            .bind(&f.name)
            .bind(f.created_at)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn folder_by_id(c: &mut SqliteConnection, id: &str) -> Result<Option<Folder>> {
        Ok(sqlx::query_as(sql::FOLDER_BY_ID)
            .bind(id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn folders_in_project(
        c: &mut SqliteConnection,
        project_id: &str,
    ) -> Result<Vec<Folder>> {
        Ok(sqlx::query_as(sql::FOLDERS_IN_PROJECT)
            .bind(project_id)
            .fetch_all(c)
            .await?)
    }

    pub async fn set_folder_name(c: &mut SqliteConnection, id: &str, name: &str) -> Result<()> {
        sqlx::query(sql::SET_FOLDER_NAME)
            .bind(id)
            .bind(name)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn set_folder_parent(
        c: &mut SqliteConnection,
        id: &str,
        parent_id: Option<&str>,
    ) -> Result<()> {
        sqlx::query(sql::SET_FOLDER_PARENT)
            .bind(id)
            .bind(parent_id)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn move_child_folders(
        c: &mut SqliteConnection,
        folder_id: &str,
        parent_id: Option<&str>,
    ) -> Result<()> {
        sqlx::query(sql::MOVE_CHILD_FOLDERS)
            .bind(folder_id)
            .bind(parent_id)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn move_folder_documents(
        c: &mut SqliteConnection,
        folder_id: &str,
        parent_id: Option<&str>,
    ) -> Result<()> {
        sqlx::query(sql::MOVE_FOLDER_DOCUMENTS)
            .bind(folder_id)
            .bind(parent_id)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn delete_folder(c: &mut SqliteConnection, id: &str) -> Result<()> {
        sqlx::query(sql::DELETE_FOLDER).bind(id).execute(c).await?;
        Ok(())
    }

    pub async fn insert_change(c: &mut SqliteConnection, r: &NewChange<'_>) -> Result<bool> {
        let done = sqlx::query(sql::INSERT_CHANGE)
            .bind(r.document_id)
            .bind(r.seq)
            .bind(r.parent_seq)
            .bind(r.branch_id)
            .bind(r.epoch)
            .bind(r.author_id)
            .bind(r.client_id)
            .bind(r.change_id)
            .bind(r.created_at)
            .bind(r.command)
            .bind(r.ops)
            .bind(r.inverse)
            .bind(r.paths)
            .bind(r.reads)
            .bind(r.undo_of)
            .bind(r.reverts)
            .bind(r.reverted_by)
            .bind(r.broke)
            .execute(c)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    pub async fn changes_between(
        c: &mut SqliteConnection,
        document_id: &str,
        after: i64,
        upto: i64,
        limit: i64,
    ) -> Result<Vec<ChangeRow>> {
        Ok(sqlx::query_as(sql::CHANGES_BETWEEN)
            .bind(document_id)
            .bind(after)
            .bind(upto)
            .bind(limit)
            .fetch_all(c)
            .await?)
    }

    pub async fn change_by_seq(
        c: &mut SqliteConnection,
        document_id: &str,
        seq: i64,
    ) -> Result<Option<ChangeRow>> {
        Ok(sqlx::query_as(sql::CHANGE_BY_SEQ)
            .bind(document_id)
            .bind(seq)
            .fetch_optional(c)
            .await?)
    }

    pub async fn change_events_between(
        c: &mut SqliteConnection,
        document_id: &str,
        after: i64,
        upto: i64,
        limit: i64,
    ) -> Result<Vec<ChangeEventRow>> {
        Ok(sqlx::query_as(sql::CHANGE_EVENTS_BETWEEN)
            .bind(document_id)
            .bind(after)
            .bind(upto)
            .bind(limit)
            .fetch_all(c)
            .await?)
    }

    pub async fn change_seq_by_client(
        c: &mut SqliteConnection,
        document_id: &str,
        client_id: &str,
        change_id: &str,
    ) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar(sql::CHANGE_SEQ_BY_CLIENT)
            .bind(document_id)
            .bind(client_id)
            .bind(change_id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn recent_change_ids(
        c: &mut SqliteConnection,
        document_id: &str,
        client_id: &str,
        limit: i64,
    ) -> Result<Vec<(String, i64)>> {
        Ok(sqlx::query_as(sql::RECENT_CHANGE_IDS)
            .bind(document_id)
            .bind(client_id)
            .bind(limit)
            .fetch_all(c)
            .await?)
    }

    pub async fn set_document_head(
        c: &mut SqliteConnection,
        id: &str,
        head_seq: i64,
        updated_at: f64,
    ) -> Result<()> {
        sqlx::query(sql::SET_DOCUMENT_HEAD)
            .bind(id)
            .bind(head_seq)
            .bind(updated_at)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn set_reverted_by(
        c: &mut SqliteConnection,
        document_id: &str,
        seq: i64,
        by: i64,
    ) -> Result<bool> {
        let done = sqlx::query(sql::SET_REVERTED_BY)
            .bind(document_id)
            .bind(seq)
            .bind(by)
            .execute(c)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    pub async fn undo_target(
        c: &mut SqliteConnection,
        document_id: &str,
        client_id: &str,
        change_id: &str,
    ) -> Result<Option<UndoTarget>> {
        Ok(sqlx::query_as(sql::UNDO_TARGET)
            .bind(document_id)
            .bind(client_id)
            .bind(change_id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn change_sizes(
        c: &mut SqliteConnection,
        document_id: &str,
        before: i64,
        after: i64,
        limit: i64,
    ) -> Result<Vec<ChangeSize>> {
        Ok(sqlx::query_as(sql::CHANGE_SIZES)
            .bind(document_id)
            .bind(before)
            .bind(after)
            .bind(limit)
            .fetch_all(c)
            .await?)
    }

    pub async fn history(
        c: &mut SqliteConnection,
        document_id: &str,
        before: i64,
        after: i64,
        limit: i64,
    ) -> Result<Vec<HistoryRow>> {
        Ok(sqlx::query_as(sql::HISTORY)
            .bind(document_id)
            .bind(before)
            .bind(after)
            .bind(limit)
            .fetch_all(c)
            .await?)
    }

    pub async fn change_broke(
        c: &mut SqliteConnection,
        document_id: &str,
        seq: i64,
    ) -> Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar(sql::CHANGE_BROKE)
            .bind(document_id)
            .bind(seq)
            .fetch_optional(c)
            .await?)
    }

    pub async fn set_change_broke(
        c: &mut SqliteConnection,
        document_id: &str,
        seq: i64,
        broke: &str,
    ) -> Result<()> {
        sqlx::query(sql::SET_CHANGE_BROKE)
            .bind(document_id)
            .bind(seq)
            .bind(broke)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn versions(
        c: &mut SqliteConnection,
        document_id: &str,
    ) -> Result<Vec<VersionWithAuthor>> {
        Ok(sqlx::query_as(sql::VERSIONS)
            .bind(document_id)
            .fetch_all(c)
            .await?)
    }

    pub async fn version_by_name(
        c: &mut SqliteConnection,
        document_id: &str,
        name: &str,
    ) -> Result<Option<Version>> {
        Ok(sqlx::query_as(sql::VERSION_BY_NAME)
            .bind(document_id)
            .bind(name)
            .fetch_optional(c)
            .await?)
    }

    pub async fn insert_version(c: &mut SqliteConnection, v: &Version) -> Result<bool> {
        let done = sqlx::query(sql::INSERT_VERSION)
            .bind(&v.id)
            .bind(&v.document_id)
            .bind(v.seq)
            .bind(&v.name)
            .bind(&v.note)
            .bind(&v.author_id)
            .bind(v.created_at)
            .execute(c)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    pub async fn delete_version(
        c: &mut SqliteConnection,
        document_id: &str,
        id: &str,
    ) -> Result<bool> {
        let done = sqlx::query(sql::DELETE_VERSION)
            .bind(document_id)
            .bind(id)
            .execute(c)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    pub async fn replace_assembly_refs(
        c: &mut SqliteConnection,
        assembly_id: &str,
        rows: &[AssemblyRefRow],
    ) -> Result<()> {
        sqlx::query(sql::DELETE_ASSEMBLY_REFS)
            .bind(assembly_id)
            .execute(&mut *c)
            .await?;
        for r in rows {
            sqlx::query(sql::INSERT_ASSEMBLY_REF)
                .bind(&r.assembly_id)
                .bind(&r.component_id)
                .bind(&r.part_id)
                .bind(r.seq)
                .execute(&mut *c)
                .await?;
        }
        Ok(())
    }

    pub async fn assembly_refs(
        c: &mut SqliteConnection,
        assembly_id: &str,
    ) -> Result<Vec<AssemblyRefRow>> {
        Ok(sqlx::query_as(sql::ASSEMBLY_REFS)
            .bind(assembly_id)
            .fetch_all(c)
            .await?)
    }

    pub async fn snapshot_at_or_before(
        c: &mut SqliteConnection,
        document_id: &str,
        seq: i64,
    ) -> Result<Option<SnapshotRow>> {
        Ok(sqlx::query_as(sql::SNAPSHOT_AT_OR_BEFORE)
            .bind(document_id)
            .bind(seq)
            .fetch_optional(c)
            .await?)
    }

    pub async fn snapshot_row(
        c: &mut SqliteConnection,
        document_id: &str,
        epoch: i64,
        seq: i64,
    ) -> Result<Option<SnapshotRow>> {
        Ok(sqlx::query_as(sql::SNAPSHOT_ROW)
            .bind(document_id)
            .bind(epoch)
            .bind(seq)
            .fetch_optional(c)
            .await?)
    }

    pub async fn room_lease(
        c: &mut SqliteConnection,
        document_id: &str,
    ) -> Result<Option<RoomLease>> {
        Ok(sqlx::query_as(sql::ROOM_LEASE)
            .bind(document_id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn lock_room_lease(
        c: &mut SqliteConnection,
        document_id: &str,
    ) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar(sql::LOCK_ROOM_LEASE)
            .bind(document_id)
            .fetch_optional(c)
            .await?)
    }

    pub async fn insert_room_lease(c: &mut SqliteConnection, l: &RoomLease) -> Result<()> {
        sqlx::query(sql::INSERT_ROOM_LEASE)
            .bind(&l.document_id)
            .bind(&l.node_id)
            .bind(l.epoch)
            .bind(l.expires_at)
            .execute(c)
            .await?;
        Ok(())
    }

    pub async fn take_room_lease(
        c: &mut SqliteConnection,
        document_id: &str,
        node_id: &str,
        expires_at: f64,
    ) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar(sql::TAKE_ROOM_LEASE)
            .bind(document_id)
            .bind(node_id)
            .bind(expires_at)
            .fetch_optional(c)
            .await?)
    }
}
