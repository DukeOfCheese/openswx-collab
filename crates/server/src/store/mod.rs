//! The database, behind one trait so SQLite and PostgreSQL share the handlers.
//!
//! Handlers hold an `Arc<dyn Store>` ([`crate::AppState::store`]). Reads go through [`Store`];
//! writes go through a transaction, [`Tx`], which [`Store::begin`] opens.
//!
//! # Adding a query
//!
//! Every query is added the same way, in five places:
//!
//! 1. **SQL text**: a `pub const` in [`sql`]. Portable SQL only (`$1, $2, …` placeholders, no
//!    dialect functions), so the PostgreSQL store runs the same text.
//! 2. **Row type** (for a query that returns rows): a plain struct in this file deriving
//!    `sqlx::FromRow`, one field per selected column, same names. The derive generates the row
//!    mapping for both databases at build time without a database. Column types map one way only:
//!    `VARCHAR`/`TEXT` → `String`, `BIGINT` → `i64` (never declare `INTEGER`: PostgreSQL would decode
//!    it as `i32`), `DOUBLE PRECISION` (epoch seconds) → `f64`, `BOOLEAN` → `bool` (SQLite stores
//!    0/1 and sqlx decodes it as `bool`), MessagePack `BLOB`/`BYTEA` → `Vec<u8>`, JSON → `String`
//!    (parse it with `serde_json` where it is used). `NULL`-able columns are `Option<_>`.
//! 3. **Trait method**, one per query:
//!    - a read used outside a transaction goes on [`Store`] (`&self`);
//!    - a write, or a read that must see or be atomic with a transaction's writes, goes on [`Tx`]
//!      (`&mut self`). Put it on both if both are needed.
//!
//!    Arguments are borrowed (`&str`, `&[u8]`); ids are [`Id`]; times come from
//!    [`crate::Clock::now`], passed in by the caller (the store never reads the system time); new ids
//!    come from [`new_id`].
//! 4. **SQLite body**: one `async fn` in `sqlite.rs`'s `q` module taking `&mut SqliteConnection`,
//!    which binds the arguments with `sqlx::query(sql::X).bind(..)` / `query_as::<_, Row>` /
//!    `query_scalar`. The trait impls are one line each, calling it with a reader connection
//!    (`Store`) or the transaction's connection, always through `self.conn()` (`Tx`), which keeps
//!    the re-entrancy guard on the task using the transaction. (PostgreSQL, from Task 16, mirrors
//!    this in `postgres.rs` with `PgConnection`.)
//!
//!    An `INSERT` lists every column and binds a value for each. The `DEFAULT`s in the schema exist
//!    for imports and hand-written rows, not for handlers: the handler decides every value.
//! 5. **Test double**: add the delegating line to `FlakyStore` / `FlakyTx` in
//!    `tests/common/mod.rs` (the compiler insists).
//!
//! Every test of a write reads the row back through [`Store`] after the request (or after
//! `commit`), so that a write that never reached the database fails the test.
//!
//! # Transactions
//!
//! ```ignore
//! let mut tx = state.store.begin().await?;
//! tx.insert_project(&project_id, &org_id, name, now).await?;
//! tx.add_project_member(&project_id, &user_id, "owner").await?;
//! tx.commit().await?; // dropping `tx` without committing rolls everything back
//! ```
//!
//! A savepoint is a transaction inside a transaction: committing it keeps its writes in the outer
//! transaction, rolling it back (or dropping it) undoes only its writes. The journal writer uses one
//! per document so that one bad record does not lose a whole batch:
//!
//! ```ignore
//! let mut tx = state.store.begin().await?;
//! for record in batch {
//!     let mut sp = tx.savepoint().await?;
//!     match sp.insert_change(&record).await {
//!         Ok(()) => sp.commit().await?,
//!         Err(e) => { sp.rollback().await?; refuse(record, e); }
//!     }
//! }
//! tx.commit().await?;
//! ```
//!
//! Rules:
//! - Only one write transaction runs at a time (SQLite has one writer; the store queues the others),
//!   so keep transactions short and never `.await` anything slow (network, blobs, other rooms)
//!   inside one.
//! - Never call [`Store::begin`] while the same task holds a [`Tx`], directly or in a helper: it
//!   would wait for itself. The store refuses it at once with "begin() called while this task holds
//!   a transaction; use savepoint()". Pass the `Tx` to the helper, or use a savepoint. (One false
//!   positive: two independent transactional futures joined in one task with `join!`/`select!` get
//!   that error instead of queueing; spawn them as tasks. See `sqlite.rs`.)
//! - [`Store`] methods only read. A write is a [`Tx`] method, never a "single statement" `Store`
//!   method that takes the writer behind the caller's back.
//! - A [`Store`] read made while holding a [`Tx`] does not see the transaction's uncommitted writes.
//!   If it must, the read belongs on [`Tx`].

pub mod sql;
pub mod sqlite;

use async_trait::async_trait;

pub use sqlite::{SqliteStore, SqliteTx};

/// A database id: 32 lower-case hex characters.
pub type Id = String;

/// A new random id: 32 lower-case hex characters (a v4 UUID without dashes, as the Python server's
/// `uuid4().hex`).
pub fn new_id() -> Id {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Which migrations a database has had, against the ones this binary carries. Versions are the
/// numbers that start the file names in `migrations/<dialect>/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaStatus {
    /// The highest version applied, or `None` for a database that has none (an empty one).
    pub current: Option<i64>,
    /// The highest version this binary knows.
    pub latest: i64,
    /// Versions this binary knows that the database has not had, in order.
    pub pending: Vec<i64>,
    /// Versions the database has had that this binary does not know: it was written by a newer
    /// version of the server.
    pub unknown: Vec<i64>,
}

impl SchemaStatus {
    /// The status of a database that has had the `applied` versions, for a binary that carries the
    /// `known` ones.
    pub fn compare(applied: &[i64], known: &[i64]) -> Self {
        let mut pending: Vec<i64> = known
            .iter()
            .copied()
            .filter(|v| !applied.contains(v))
            .collect();
        pending.sort_unstable();
        let mut unknown: Vec<i64> = applied
            .iter()
            .copied()
            .filter(|v| !known.contains(v))
            .collect();
        unknown.sort_unstable();
        Self {
            current: applied.iter().copied().max(),
            latest: known.iter().copied().max().unwrap_or(0),
            pending,
            unknown,
        }
    }

    /// The schema is exactly what this binary expects.
    pub fn is_current(&self) -> bool {
        self.pending.is_empty() && self.unknown.is_empty()
    }
}

/// Why a database cannot be used as it is.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    /// The schema is older than this binary and upgrading on start is turned off.
    #[error(
        "the database schema is at version {current:?}, this server needs version {latest}; run \
         `openswx-collab-server migrate` (or set OSWX_COLLAB_AUTO_MIGRATE=1) to upgrade it"
    )]
    Outdated { current: Option<i64>, latest: i64 },
    /// The database has migrations this binary does not know.
    #[error(
        "the database was written by a newer version of the server (it has migration {unknown:?}, \
         this server knows up to {latest}); run that version or a later one"
    )]
    Newer { unknown: Vec<i64>, latest: i64 },
    /// A migration the database has had is not the one this binary carries under that version: a
    /// released migration was edited.
    #[error(
        "migration {version} has changed since it was applied to this database; released \
         migrations must never be edited (add a new one instead)"
    )]
    Changed { version: i64 },
    /// The database has tables but no record of this server's migrations.
    #[error(
        "the database has tables but was not written by this server (no migration record); a \
         database of the Python server must be imported, not opened"
    )]
    Foreign,
}

/// An account (`users`).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct User {
    pub id: Id,
    /// Normalised by [`crate::security::clean_email`]; unique.
    pub email: String,
    pub name: String,
    /// `None` for an account that signs in only through a provider.
    pub password_hash: Option<String>,
    pub is_server_admin: bool,
    pub disabled: bool,
    pub email_verified: bool,
    pub created_at: f64,
    pub deleted_at: Option<f64>,
}

impl User {
    /// Whether this account may sign in at all: it is neither disabled nor deleted.
    pub fn active(&self) -> bool {
        !self.disabled && self.deleted_at.is_none()
    }
}

/// A signed-in device (`sessions`). Only the token's hash is stored.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Session {
    pub token_hash: String,
    pub user_id: Id,
    pub created_at: f64,
    pub expires_at: f64,
    pub label: String,
}

/// A password reset link (`password_resets`). Only the token's hash is stored.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct PasswordReset {
    pub token_hash: String,
    pub user_id: Id,
    pub expires_at: f64,
}

/// The database. One method per query; see the module documentation for how to add one.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// Answers when the database does (`GET /api/ready`).
    async fn health(&self) -> anyhow::Result<()>;

    /// Which migrations the database has had, against the ones this binary carries.
    async fn schema_status(&self) -> anyhow::Result<SchemaStatus>;

    /// Opens a write transaction. It waits while another one is open; see the module documentation.
    async fn begin(&self) -> anyhow::Result<Box<dyn Tx>>;

    /// How many accounts exist, deleted and disabled ones included (the registration mode).
    async fn user_count(&self) -> anyhow::Result<u64>;

    /// The account with this id.
    async fn user_by_id(&self, id: &str) -> anyhow::Result<Option<User>>;

    /// The account with this (normalised) email address.
    async fn user_by_email(&self, email: &str) -> anyhow::Result<Option<User>>;

    /// The session whose token has this hash, expired or not.
    async fn session_by_hash(&self, token_hash: &str) -> anyhow::Result<Option<Session>>;

    /// The password reset whose token has this hash, expired or not.
    async fn password_reset_by_hash(
        &self,
        token_hash: &str,
    ) -> anyhow::Result<Option<PasswordReset>>;
}

/// A write transaction (or a savepoint within one). Dropping it without [`Tx::commit`] rolls it
/// back. One method per query, as on [`Store`].
#[async_trait]
pub trait Tx: Send {
    /// Makes the writes permanent (for a savepoint: part of the enclosing transaction).
    async fn commit(self: Box<Self>) -> anyhow::Result<()>;

    /// Undoes the writes. Dropping the transaction does the same; this also reports a failure.
    async fn rollback(self: Box<Self>) -> anyhow::Result<()>;

    /// Opens a savepoint: a transaction within this one, which can be rolled back on its own.
    async fn savepoint(&mut self) -> anyhow::Result<Box<dyn Tx + '_>>;

    /// [`Store::user_count`], serialised with this transaction's writes (registration decides who
    /// becomes the server admin from it).
    async fn user_count(&mut self) -> anyhow::Result<u64>;

    /// [`Store::user_by_id`], seeing this transaction's writes.
    async fn user_by_id(&mut self, id: &str) -> anyhow::Result<Option<User>>;

    /// [`Store::user_by_email`], seeing this transaction's writes.
    async fn user_by_email(&mut self, email: &str) -> anyhow::Result<Option<User>>;

    /// Adds an account. Fails if the id or the email address is taken.
    async fn insert_user(&mut self, user: &User) -> anyhow::Result<()>;

    async fn set_user_name(&mut self, id: &str, name: &str) -> anyhow::Result<()>;

    async fn set_user_password(
        &mut self,
        id: &str,
        password_hash: Option<&str>,
    ) -> anyhow::Result<()>;

    async fn set_user_email_verified(&mut self, id: &str, verified: bool) -> anyhow::Result<()>;

    async fn set_user_disabled(&mut self, id: &str, disabled: bool) -> anyhow::Result<()>;

    async fn insert_session(&mut self, session: &Session) -> anyhow::Result<()>;

    /// Ends one session.
    async fn delete_session(&mut self, token_hash: &str) -> anyhow::Result<()>;

    /// Ends every session of an account.
    async fn delete_user_sessions(&mut self, user_id: &str) -> anyhow::Result<()>;

    /// Ends every session of an account but the one whose token has the hash `keep`.
    async fn delete_other_user_sessions(&mut self, user_id: &str, keep: &str)
    -> anyhow::Result<()>;

    /// Removes an account's sessions that expired before `now`.
    async fn delete_expired_user_sessions(&mut self, user_id: &str, now: f64)
    -> anyhow::Result<()>;

    async fn insert_password_reset(&mut self, reset: &PasswordReset) -> anyhow::Result<()>;

    /// Removes the password reset whose token has this hash and answers it (`None` if there was
    /// none): a link works once, even when two requests race.
    async fn take_password_reset(
        &mut self,
        token_hash: &str,
    ) -> anyhow::Result<Option<PasswordReset>>;

    async fn delete_user_password_resets(&mut self, user_id: &str) -> anyhow::Result<()>;

    /// Removes every account's password resets that expired before `now`.
    async fn delete_expired_password_resets(&mut self, now: f64) -> anyhow::Result<()>;
}
