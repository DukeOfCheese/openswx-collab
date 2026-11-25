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
//!    (`Store`) or the transaction's connection (`Tx`). (PostgreSQL, from Task 16, mirrors this in
//!    `postgres.rs` with `PgConnection`.)
//! 5. **Test double**: add the delegating line to `FlakyStore` / `FlakyTx` in
//!    `tests/common/mod.rs` (the compiler insists).
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
//!   so keep transactions short, never `.await` anything slow (network, blobs, other rooms) inside
//!   one, and never call [`Store::begin`] while holding a [`Tx`]: that waits for itself. Use a
//!   savepoint instead.
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
    /// The database has tables but no record of this server's migrations.
    #[error(
        "the database has tables but was not written by this server (no migration record); a \
         database of the Python server must be imported, not opened"
    )]
    Foreign,
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
}
