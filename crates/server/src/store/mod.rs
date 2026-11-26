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

/// A Google or GitHub account that signs in to an account here (`oauth_identities`).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct OAuthIdentity {
    /// `google` or `github`.
    pub provider: String,
    /// The provider's id for the person (Google's `sub`, GitHub's numeric `id`).
    pub subject: String,
    pub user_id: Id,
}

/// A browser sign-in that has started and not yet come back from the provider (`oauth_states`).
/// Only the state value's hash is stored.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct OAuthState {
    pub state_hash: String,
    pub provider: String,
    /// The app's loopback address the browser is sent back to.
    pub redirect_uri: String,
    /// The hash of the invite token the sign-in carries, if any.
    pub invite: Option<String>,
    /// The SHA-256 (lower-case hex) of the app's `code_verifier`.
    pub challenge: String,
    /// The account adding this provider, for a link (`None` for a sign-in).
    pub link_user_id: Option<Id>,
    pub expires_at: f64,
}

/// A one-time code the browser brought back to the app (`login_codes`). Only its hash is stored.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct LoginCode {
    pub code_hash: String,
    /// The account it signs in to, or (a link) the account that asked to add the provider.
    pub user_id: Id,
    /// The SHA-256 (lower-case hex) of the app's `code_verifier`.
    pub challenge: String,
    /// For a link: the provider, its subject and the address it gave (cleaned, if verified).
    pub link_provider: Option<String>,
    pub link_subject: Option<String>,
    pub link_email: Option<String>,
    pub expires_at: f64,
}

/// An organisation (`orgs`).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Org {
    pub id: Id,
    pub name: String,
    pub created_at: f64,
}

/// Someone's membership of an organisation (`org_members`).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct OrgMember {
    pub org_id: Id,
    pub user_id: Id,
    /// `owner`, `admin` or `member` ([`crate::security::OrgRole`]).
    pub role: String,
    pub created_at: f64,
}

/// One of an account's organisations, as `GET /api/orgs` lists it.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct OrgSummary {
    pub id: Id,
    pub name: String,
    /// The account's role in it.
    pub role: String,
    /// How many members it has.
    pub members: i64,
}

/// A member of an organisation or a project, with their role there.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Member {
    pub id: Id,
    pub email: String,
    pub name: String,
    pub role: String,
}

/// An invite to an organisation (`invites`). Only the token's hash is stored.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Invite {
    pub id: Id,
    pub token_hash: String,
    pub org_id: Id,
    /// When set, only the account with this (normalised) address can use it.
    pub email: Option<String>,
    /// The organisation role it gives: `admin` or `member`.
    pub role: String,
    /// JSON: the projects it gives a role in, `[{"project_id": .., "role": ..}]`.
    pub projects: String,
    pub created_by: Id,
    pub created_at: f64,
    pub expires_at: f64,
    /// Who used it; `None` while it is unused.
    pub accepted_by: Option<Id>,
}

/// A project (`projects`).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Project {
    pub id: Id,
    pub org_id: Id,
    pub name: String,
    pub created_at: f64,
}

/// Someone's role in a project (`project_members`).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ProjectMember {
    pub project_id: Id,
    pub user_id: Id,
    /// `owner`, `editor` or `viewer` ([`crate::security::ProjectRole`]).
    pub role: String,
}

/// A project and someone's role in it.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ProjectWithRole {
    pub id: Id,
    pub name: String,
    pub org_id: Id,
    pub role: String,
}

/// A document (`documents`): a part or an assembly. Its content lives in snapshots and the journal.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Document {
    pub id: Id,
    pub project_id: Id,
    /// The folder it is in; `None` at the top of the project.
    pub folder_id: Option<Id>,
    pub name: String,
    /// `part` or `assembly`.
    pub kind: String,
    /// The content's top-level `version` when it was created, if that was an integer.
    pub format_version: Option<i64>,
    /// The seq of its newest change (0 for one nobody has changed).
    pub head_seq: i64,
    pub created_by: Id,
    pub created_at: f64,
    /// When its content last changed.
    pub updated_at: f64,
    /// Set when it was deleted (the row stays).
    pub deleted_at: Option<f64>,
}

/// A folder in a project (`folders`). Folders nest; `parent_id` is `None` at the top.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Folder {
    pub id: Id,
    pub project_id: Id,
    pub parent_id: Option<Id>,
    pub name: String,
    pub created_at: f64,
}

/// A stored snapshot of a document (`snapshots`): `key` names it in blob storage.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct SnapshotRow {
    pub document_id: Id,
    /// The room lease epoch it was written under.
    pub epoch: i64,
    pub seq: i64,
    pub key: String,
    /// Its size in storage, in bytes.
    pub size: i64,
    pub created_at: f64,
}

/// One accepted change in a document's journal (`changes`). `ops`, `inverse`, `paths` and `reads`
/// are MessagePack in the forms [`crate::record`] writes.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ChangeRow {
    pub document_id: Id,
    pub seq: i64,
    /// The seq it follows (`seq - 1` on the one branch there is).
    pub parent_seq: i64,
    /// Always `main` for now.
    pub branch_id: String,
    /// The room lease epoch it was written under.
    pub epoch: i64,
    pub author_id: Id,
    /// The connection that sent it (`<user id>.<client id>`, as the conflict index knows it);
    /// `None` for a change the server made (a restore).
    pub client_id: Option<String>,
    /// The sender's id for it; unique per document and `client_id`.
    pub change_id: Option<String>,
    pub created_at: f64,
    pub command: String,
    pub ops: Vec<u8>,
    pub inverse: Vec<u8>,
    pub paths: Vec<u8>,
    pub reads: Vec<u8>,
    /// The seq of the change it undoes.
    pub undo_of: Option<i64>,
    /// The seq of the change it reverts (from history).
    pub reverts: Option<i64>,
    /// The seq of the change that reverted it.
    pub reverted_by: Option<i64>,
    /// JSON: what a rebuild found it broke (later tasks).
    pub broke: Option<String>,
}

/// What a change event (a live broadcast or a catch-up in a welcome) needs of a journal row.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ChangeEventRow {
    pub seq: i64,
    pub author_id: Id,
    /// The author's display name; `None` if the account row is gone.
    pub author_name: Option<String>,
    pub client_id: Option<String>,
    pub change_id: Option<String>,
    pub command: String,
    pub ops: Vec<u8>,
    /// The seq of the change it takes back (an undo or a revert).
    pub reverts: Option<i64>,
    /// For an undo: the change id of the change it undoes.
    pub undo_of: Option<String>,
}

/// What undoing a client's change needs of its journal row ([`Store::undo_target`]).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct UndoTarget {
    pub seq: i64,
    /// MessagePack ([`crate::record::encode_ops`]): what takes the change back out.
    pub inverse: Vec<u8>,
    /// Set once something undid or reverted it.
    pub reverted_by: Option<i64>,
}

/// A journal row as the history lists it ([`Store::history`]).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct HistoryRow {
    pub seq: i64,
    pub author_id: Id,
    /// `None` if the account row is gone.
    pub author_name: Option<String>,
    pub client_id: Option<String>,
    pub change_id: Option<String>,
    pub created_at: f64,
    pub command: String,
    pub ops: Vec<u8>,
    pub reverts: Option<i64>,
    pub reverted_by: Option<i64>,
    /// JSON: `{feature id: error}`, what rebuild reports blamed on it.
    pub broke: Option<String>,
}

/// How many bytes a journal row's stored operations take ([`Store::change_sizes`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct ChangeSize {
    pub seq: i64,
    /// The length of its `ops` column, in bytes.
    pub size: i64,
}

/// A named version of a document (`versions`): a label on a seq.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Version {
    pub id: Id,
    pub document_id: Id,
    pub seq: i64,
    /// 1 to 200 characters, unique per document.
    pub name: String,
    pub note: String,
    pub author_id: Id,
    pub created_at: f64,
}

/// A named version with its author's name ([`Store::versions`]).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct VersionWithAuthor {
    pub id: Id,
    pub document_id: Id,
    pub seq: i64,
    pub name: String,
    pub note: String,
    pub author_id: Id,
    /// `None` if the account row is gone.
    pub author_name: Option<String>,
    pub created_at: f64,
}

/// A journal row to insert: [`ChangeRow`]'s columns, borrowed from the record that carries them
/// (the journal writer binds the MessagePack straight from the record's bytes, without copying them
/// inside its transaction).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NewChange<'a> {
    pub document_id: &'a str,
    pub seq: i64,
    pub parent_seq: i64,
    pub branch_id: &'a str,
    pub epoch: i64,
    pub author_id: &'a str,
    pub client_id: Option<&'a str>,
    pub change_id: Option<&'a str>,
    pub created_at: f64,
    pub command: &'a str,
    pub ops: &'a [u8],
    pub inverse: &'a [u8],
    pub paths: &'a [u8],
    pub reads: &'a [u8],
    pub undo_of: Option<i64>,
    pub reverts: Option<i64>,
    pub reverted_by: Option<i64>,
    pub broke: Option<&'a str>,
}

/// A component of an assembly and the part it refers to (`assembly_refs`).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct AssemblyRefRow {
    pub assembly_id: Id,
    pub component_id: String,
    /// The part document (no foreign key: the row outlives a part deleted since).
    pub part_id: Id,
    /// The state of the part the component is pinned to.
    pub seq: i64,
}

/// A document as an assembly's references need it, with its project's organisation
/// ([`Store::parts_by_ids`]).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct PartRow {
    pub id: Id,
    pub project_id: Id,
    pub org_id: Id,
    pub name: String,
    /// `part` or `assembly`.
    pub kind: String,
    pub head_seq: i64,
    pub deleted_at: Option<f64>,
}

/// An assembly that uses a part ([`Store::assemblies_using`]).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct AssemblyUse {
    pub id: Id,
    pub name: String,
    pub project_id: Id,
    pub org_id: Id,
}

/// A component pinned to a state of a part ([`Store::pinned_before`]).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct PinnedComponent {
    pub assembly_id: Id,
    pub component_id: String,
}

/// Which node holds a document's room, under which epoch (`room_leases`).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct RoomLease {
    pub document_id: Id,
    pub node_id: String,
    /// Bumped by every acquisition; journal writes carry it and are refused when it moved on.
    pub epoch: i64,
    /// A hint for other nodes (project 3), never the guard.
    pub expires_at: f64,
}

/// One of an account's organisations, as its own export lists it ([`Store::user_memberships`]).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Membership {
    pub id: Id,
    pub name: String,
    /// The account's role in it.
    pub role: String,
    /// When the account joined.
    pub since: f64,
}

/// A change as its author's own export lists it ([`Store::changes_by_author`]): where and when,
/// never what.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct AuthoredChange {
    pub document_id: Id,
    pub seq: i64,
    pub created_at: f64,
    pub command: String,
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

    /// The account a provider identity signs in to.
    async fn oauth_identity(
        &self,
        provider: &str,
        subject: &str,
    ) -> anyhow::Result<Option<OAuthIdentity>>;

    /// The provider identities of an account, by provider and subject.
    async fn user_identities(&self, user_id: &str) -> anyhow::Result<Vec<OAuthIdentity>>;

    async fn org_by_id(&self, id: &str) -> anyhow::Result<Option<Org>>;

    /// An account's membership of an organisation.
    async fn org_member(&self, org_id: &str, user_id: &str) -> anyhow::Result<Option<OrgMember>>;

    /// An account's organisations with its role and their member counts, by name then id (one
    /// query, however many organisations).
    async fn orgs_of_user(&self, user_id: &str) -> anyhow::Result<Vec<OrgSummary>>;

    /// An organisation's members, by name then id.
    async fn org_members(&self, org_id: &str) -> anyhow::Result<Vec<Member>>;

    /// How many members an organisation has.
    async fn org_member_count(&self, org_id: &str) -> anyhow::Result<u64>;

    async fn project_by_id(&self, id: &str) -> anyhow::Result<Option<Project>>;

    /// An account's role row in a project.
    async fn project_member(
        &self,
        project_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Option<ProjectMember>>;

    /// Every project of an organisation, by name then id.
    async fn projects_in_org(&self, org_id: &str) -> anyhow::Result<Vec<Project>>;

    /// The projects of an organisation an account has a role row in, with that role, by name then
    /// id.
    async fn member_projects(
        &self,
        org_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Vec<ProjectWithRole>>;

    /// The accounts with a role row in a project, by name then id.
    async fn project_members(&self, project_id: &str) -> anyhow::Result<Vec<Member>>;

    /// The invite with this id (for tests: the routes find invites by their token).
    #[doc(hidden)]
    async fn invite_by_id(&self, id: &str) -> anyhow::Result<Option<Invite>>;

    /// The invite whose token has this hash, used or expired or not.
    async fn invite_by_hash(&self, token_hash: &str) -> anyhow::Result<Option<Invite>>;

    /// An organisation's invites that are unused and expire after `now`, oldest first.
    async fn open_invites(&self, org_id: &str, now: f64) -> anyhow::Result<Vec<Invite>>;

    /// The document with this id, deleted or not.
    async fn document_by_id(&self, id: &str) -> anyhow::Result<Option<Document>>;

    /// A project's documents that are not deleted, by name then id.
    async fn documents_in_project(&self, project_id: &str) -> anyhow::Result<Vec<Document>>;

    /// A document's newest snapshot (highest seq, then highest epoch).
    async fn latest_snapshot(&self, document_id: &str) -> anyhow::Result<Option<SnapshotRow>>;

    /// The folder with this id.
    async fn folder_by_id(&self, id: &str) -> anyhow::Result<Option<Folder>>;

    /// A project's folders, by name then id.
    async fn folders_in_project(&self, project_id: &str) -> anyhow::Result<Vec<Folder>>;

    /// A document's journal rows with `after < seq <= upto`, in seq order, at most `limit` of them.
    async fn changes_between(
        &self,
        document_id: &str,
        after: i64,
        upto: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ChangeRow>>;

    /// One journal row.
    async fn change_by_seq(&self, document_id: &str, seq: i64)
    -> anyhow::Result<Option<ChangeRow>>;

    /// A document's journal rows with `after < seq <= upto` as change events, in seq order, at most
    /// `limit` of them.
    async fn change_events_between(
        &self,
        document_id: &str,
        after: i64,
        upto: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ChangeEventRow>>;

    /// The seq of the change a client sent under `change_id`, if the journal has it (a resend).
    async fn change_seq_by_client(
        &self,
        document_id: &str,
        client_id: &str,
        change_id: &str,
    ) -> anyhow::Result<Option<i64>>;

    /// The seq, inverse and `reverted_by` of the change a client sent under `change_id`, if the
    /// journal has it: what an undo of it needs.
    async fn undo_target(
        &self,
        document_id: &str,
        client_id: &str,
        change_id: &str,
    ) -> anyhow::Result<Option<UndoTarget>>;

    /// The seq and stored operation size of a document's journal rows with `after < seq < before`,
    /// newest first, at most `limit` (without reading the operations).
    async fn change_sizes(
        &self,
        document_id: &str,
        before: i64,
        after: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ChangeSize>>;

    /// A document's journal rows with `after < seq < before`, newest first, at most `limit`, with
    /// their authors' names.
    async fn history(
        &self,
        document_id: &str,
        before: i64,
        after: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<HistoryRow>>;

    /// A document's named versions, newest seq first (then newest first).
    async fn versions(&self, document_id: &str) -> anyhow::Result<Vec<VersionWithAuthor>>;

    /// A document's version with this name.
    async fn version_by_name(
        &self,
        document_id: &str,
        name: &str,
    ) -> anyhow::Result<Option<Version>>;

    /// A client's most recent change ids in a document with their seqs, newest first, at most
    /// `limit`.
    async fn recent_change_ids(
        &self,
        document_id: &str,
        client_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<(String, i64)>>;

    /// A document's snapshot with the highest seq not above `seq` (then the highest epoch).
    async fn snapshot_at_or_before(
        &self,
        document_id: &str,
        seq: i64,
    ) -> anyhow::Result<Option<SnapshotRow>>;

    /// The snapshot row of a document at `(epoch, seq)`.
    async fn snapshot_row(
        &self,
        document_id: &str,
        epoch: i64,
        seq: i64,
    ) -> anyhow::Result<Option<SnapshotRow>>;

    /// A document's room lease.
    async fn room_lease(&self, document_id: &str) -> anyhow::Result<Option<RoomLease>>;

    /// An assembly's component references, by component id.
    async fn assembly_refs(&self, assembly_id: &str) -> anyhow::Result<Vec<AssemblyRefRow>>;

    /// The documents with these ids (deleted ones included) with their organisations, in no
    /// particular order; ids that name no document are left out. Read in batches, however many
    /// ids (the caller bounds them).
    async fn parts_by_ids(&self, ids: &[Id]) -> anyhow::Result<Vec<PartRow>>;

    /// The assemblies, not deleted, whose components use a part, by name then id.
    async fn assemblies_using(&self, part_id: &str) -> anyhow::Result<Vec<AssemblyUse>>;

    /// The named versions of these documents, by document id, then newest seq first (then newest
    /// first), as [`Store::versions`] orders one document's. Read in batches, however many ids.
    async fn versions_of(&self, ids: &[Id]) -> anyhow::Result<Vec<VersionWithAuthor>>;

    /// Every component reference to a part (of any assembly, deleted ones included), by assembly
    /// then component.
    async fn refs_to_part(&self, part_id: &str) -> anyhow::Result<Vec<AssemblyRefRow>>;

    /// The components pinned to a state of a part before `seq`, by assembly then component.
    async fn pinned_before(&self, part_id: &str, seq: i64) -> anyhow::Result<Vec<PinnedComponent>>;

    /// An account's organisations with its role and when it joined, by name then id.
    async fn user_memberships(&self, user_id: &str) -> anyhow::Result<Vec<Membership>>;

    /// The projects an account has a role row in (in any organisation), with that role, by name
    /// then id.
    async fn user_project_roles(&self, user_id: &str) -> anyhow::Result<Vec<ProjectWithRole>>;

    /// An account's sessions, expired ones included, oldest first.
    async fn user_sessions(&self, user_id: &str) -> anyhow::Result<Vec<Session>>;

    /// The changes an account made, by document then seq, after `(after_document, after_seq)`
    /// (`("", 0)` for the first page), at most `limit`.
    async fn changes_by_author(
        &self,
        user_id: &str,
        after_document: &str,
        after_seq: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<AuthoredChange>>;

    /// The versions an account named, by id, after the id `after` (`""` for the first page), at
    /// most `limit`.
    async fn versions_by_author(
        &self,
        user_id: &str,
        after: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<Version>>;
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

    async fn oauth_identity(
        &mut self,
        provider: &str,
        subject: &str,
    ) -> anyhow::Result<Option<OAuthIdentity>>;

    async fn user_identities(&mut self, user_id: &str) -> anyhow::Result<Vec<OAuthIdentity>>;

    async fn insert_oauth_identity(&mut self, identity: &OAuthIdentity) -> anyhow::Result<()>;

    /// Removes an account's identities with this provider.
    async fn delete_user_identities(&mut self, user_id: &str, provider: &str)
    -> anyhow::Result<()>;

    async fn insert_oauth_state(&mut self, state: &OAuthState) -> anyhow::Result<()>;

    /// Removes the sign-in state with this hash and answers it (`None` if there was none): a state
    /// is used once, even when two callbacks race.
    async fn take_oauth_state(&mut self, state_hash: &str) -> anyhow::Result<Option<OAuthState>>;

    /// Removes the sign-in states that expired before `now`.
    async fn delete_expired_oauth_states(&mut self, now: f64) -> anyhow::Result<()>;

    async fn insert_login_code(&mut self, code: &LoginCode) -> anyhow::Result<()>;

    /// Removes the login code with this hash and answers it (`None` if there was none): a code is
    /// used once, even when two exchanges race.
    async fn take_login_code(&mut self, code_hash: &str) -> anyhow::Result<Option<LoginCode>>;

    /// Removes the login codes that expired before `now`.
    async fn delete_expired_login_codes(&mut self, now: f64) -> anyhow::Result<()>;

    async fn insert_org(&mut self, org: &Org) -> anyhow::Result<()>;

    async fn set_org_name(&mut self, id: &str, name: &str) -> anyhow::Result<()>;

    /// [`Store::org_by_id`], seeing this transaction's writes.
    async fn org_by_id(&mut self, id: &str) -> anyhow::Result<Option<Org>>;

    /// [`Store::org_member`], seeing this transaction's writes.
    async fn org_member(
        &mut self,
        org_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Option<OrgMember>>;

    async fn insert_org_member(&mut self, member: &OrgMember) -> anyhow::Result<()>;

    async fn set_org_member_role(
        &mut self,
        org_id: &str,
        user_id: &str,
        role: &str,
    ) -> anyhow::Result<()>;

    async fn delete_org_member(&mut self, org_id: &str, user_id: &str) -> anyhow::Result<()>;

    /// How many members an organisation has.
    async fn org_member_count(&mut self, org_id: &str) -> anyhow::Result<u64>;

    /// How many owners an organisation has.
    async fn org_owner_count(&mut self, org_id: &str) -> anyhow::Result<u64>;

    /// Removes an account's role rows in every project of an organisation.
    async fn delete_project_members_in_org(
        &mut self,
        org_id: &str,
        user_id: &str,
    ) -> anyhow::Result<()>;

    /// The invite whose token has this hash, used or expired or not.
    async fn invite_by_hash(&mut self, token_hash: &str) -> anyhow::Result<Option<Invite>>;

    async fn insert_invite(&mut self, invite: &Invite) -> anyhow::Result<()>;

    /// Marks an unused invite as used by `user_id`; `false` if it was already used (or is gone):
    /// of two people accepting one invite at once, one wins.
    async fn claim_invite(&mut self, id: &str, user_id: &str) -> anyhow::Result<bool>;

    /// Deletes an organisation's invite; `false` if it has no invite with that id.
    async fn delete_invite(&mut self, org_id: &str, id: &str) -> anyhow::Result<bool>;

    async fn insert_project(&mut self, project: &Project) -> anyhow::Result<()>;

    async fn set_project_name(&mut self, id: &str, name: &str) -> anyhow::Result<()>;

    /// Deletes a project; the database removes its documents, their journals, snapshot rows,
    /// versions and references, and its role rows with it.
    async fn delete_project(&mut self, id: &str) -> anyhow::Result<()>;

    /// [`Store::project_by_id`], seeing this transaction's writes.
    async fn project_by_id(&mut self, id: &str) -> anyhow::Result<Option<Project>>;

    /// [`Store::project_member`], seeing this transaction's writes.
    async fn project_member(
        &mut self,
        project_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Option<ProjectMember>>;

    async fn insert_project_member(&mut self, member: &ProjectMember) -> anyhow::Result<()>;

    async fn set_project_member_role(
        &mut self,
        project_id: &str,
        user_id: &str,
        role: &str,
    ) -> anyhow::Result<()>;

    /// Removes an account's role in a project; `false` if it had none.
    async fn delete_project_member(
        &mut self,
        project_id: &str,
        user_id: &str,
    ) -> anyhow::Result<bool>;

    /// The ids of every document in a project, deleted ones included.
    async fn project_document_ids(&mut self, project_id: &str) -> anyhow::Result<Vec<Id>>;

    /// Marks every document of a project that is not yet deleted as deleted at `now`, one row at
    /// a time in ascending id order: part rows are always locked in ascending id order
    /// ([`crate::assemblies`], "Lock order").
    async fn mark_project_documents_deleted(
        &mut self,
        project_id: &str,
        now: f64,
    ) -> anyhow::Result<()>;

    /// Adds a document.
    async fn insert_document(&mut self, document: &Document) -> anyhow::Result<()>;

    /// Records a snapshot written to storage.
    async fn insert_snapshot(&mut self, snapshot: &SnapshotRow) -> anyhow::Result<()>;

    /// [`Store::document_by_id`], seeing this transaction's writes.
    async fn document_by_id(&mut self, id: &str) -> anyhow::Result<Option<Document>>;

    async fn set_document_name(&mut self, id: &str, name: &str) -> anyhow::Result<()>;

    /// Moves a document into a folder (`None`: the top of its project).
    async fn set_document_folder(
        &mut self,
        id: &str,
        folder_id: Option<&str>,
    ) -> anyhow::Result<()>;

    /// Marks a document deleted at `now`; `false` if it was already deleted (or is gone).
    async fn mark_document_deleted(&mut self, id: &str, now: f64) -> anyhow::Result<bool>;

    async fn insert_folder(&mut self, folder: &Folder) -> anyhow::Result<()>;

    /// [`Store::folder_by_id`], seeing this transaction's writes.
    async fn folder_by_id(&mut self, id: &str) -> anyhow::Result<Option<Folder>>;

    /// [`Store::folders_in_project`], seeing this transaction's writes.
    async fn folders_in_project(&mut self, project_id: &str) -> anyhow::Result<Vec<Folder>>;

    async fn set_folder_name(&mut self, id: &str, name: &str) -> anyhow::Result<()>;

    /// Moves a folder into another (`None`: the top of its project).
    async fn set_folder_parent(&mut self, id: &str, parent_id: Option<&str>) -> anyhow::Result<()>;

    /// Moves every folder directly inside `folder_id` into `parent_id` (`None`: the top).
    async fn move_child_folders(
        &mut self,
        folder_id: &str,
        parent_id: Option<&str>,
    ) -> anyhow::Result<()>;

    /// Moves every document directly inside `folder_id` (deleted ones too) into `parent_id`
    /// (`None`: the top).
    async fn move_folder_documents(
        &mut self,
        folder_id: &str,
        parent_id: Option<&str>,
    ) -> anyhow::Result<()>;

    /// Deletes a folder. Move what is inside it first: the database deletes folders inside it with
    /// it and moves documents inside it to the top of the project.
    async fn delete_folder(&mut self, id: &str) -> anyhow::Result<()>;

    /// Adds a journal row; `false` (and nothing written, the transaction still usable) when the
    /// document already has a row with the same `client_id` and `change_id`.
    async fn insert_change(&mut self, change: &NewChange<'_>) -> anyhow::Result<bool>;

    /// Sets a document's head seq and when its content last changed.
    async fn set_document_head(
        &mut self,
        id: &str,
        head_seq: i64,
        updated_at: f64,
    ) -> anyhow::Result<()>;

    /// Marks the journal row `seq` as reverted by `by`; `false` if it is missing or already reverted.
    async fn set_reverted_by(
        &mut self,
        document_id: &str,
        seq: i64,
        by: i64,
    ) -> anyhow::Result<bool>;

    /// Locks the journal row `seq` until the transaction ends and answers its `broke` JSON (`None`:
    /// no such row; `Some(None)`: nothing blamed on it yet).
    async fn change_broke(
        &mut self,
        document_id: &str,
        seq: i64,
    ) -> anyhow::Result<Option<Option<String>>>;

    /// Sets the `broke` JSON of the journal row `seq`.
    async fn set_change_broke(
        &mut self,
        document_id: &str,
        seq: i64,
        broke: &str,
    ) -> anyhow::Result<()>;

    /// Adds a named version; `false` (nothing written) when the document has one of that name.
    async fn insert_version(&mut self, version: &Version) -> anyhow::Result<bool>;

    /// Deletes a document's version; `false` if it has none with that id.
    async fn delete_version(&mut self, document_id: &str, id: &str) -> anyhow::Result<bool>;

    /// Replaces every component reference of an assembly with `rows`.
    async fn replace_assembly_refs(
        &mut self,
        assembly_id: &str,
        rows: &[AssemblyRefRow],
    ) -> anyhow::Result<()>;

    /// Locks a document's row until the transaction ends (PostgreSQL; SQLite's write transaction
    /// already excludes every other writer) and answers whether it exists and is not deleted, as
    /// `SELECT ... FOR UPDATE` would: a deletion either committed before (`false`) or waits for
    /// this transaction.
    async fn lock_live_document(&mut self, id: &str) -> anyhow::Result<bool>;

    /// [`Store::assemblies_using`], seeing this transaction's writes.
    async fn assemblies_using(&mut self, part_id: &str) -> anyhow::Result<Vec<AssemblyUse>>;

    /// The assemblies, not deleted, in other projects than `project_id` whose components use a
    /// document of that project, by name then id.
    async fn assemblies_using_project(
        &mut self,
        project_id: &str,
    ) -> anyhow::Result<Vec<AssemblyUse>>;

    /// [`Store::snapshot_row`], seeing this transaction's writes.
    async fn snapshot_row(
        &mut self,
        document_id: &str,
        epoch: i64,
        seq: i64,
    ) -> anyhow::Result<Option<SnapshotRow>>;

    /// [`Store::room_lease`], seeing this transaction's writes.
    async fn room_lease(&mut self, document_id: &str) -> anyhow::Result<Option<RoomLease>>;

    /// Locks a document's lease row until the transaction ends and answers its epoch (`None`: no
    /// row).
    async fn lock_room_lease(&mut self, document_id: &str) -> anyhow::Result<Option<i64>>;

    async fn insert_room_lease(&mut self, lease: &RoomLease) -> anyhow::Result<()>;

    /// Gives a document's existing lease to `node_id` until `expires_at` under the next epoch, and
    /// answers that epoch (`None`: no row).
    async fn take_room_lease(
        &mut self,
        document_id: &str,
        node_id: &str,
        expires_at: f64,
    ) -> anyhow::Result<Option<i64>>;

    /// The organisations an account owns that have no other owner, by name then id.
    async fn orgs_owned_alone(&mut self, user_id: &str) -> anyhow::Result<Vec<Org>>;

    /// How many server admins are not disabled.
    async fn active_admin_count(&mut self) -> anyhow::Result<u64>;

    /// How many accounts other than `user_id` are not disabled.
    async fn other_active_user_count(&mut self, user_id: &str) -> anyhow::Result<u64>;

    /// The organisations an account is a member of, by id.
    async fn user_org_ids(&mut self, user_id: &str) -> anyhow::Result<Vec<Id>>;

    /// Empties an account (it is not removed: history refers to it): its sessions, sign-in
    /// providers, login codes, password resets, memberships and project roles go; its address
    /// becomes `email` and its name `name`; it has no password, is no admin, is not verified, is
    /// disabled, and is deleted at `now`.
    async fn erase_user(
        &mut self,
        id: &str,
        email: &str,
        name: &str,
        now: f64,
    ) -> anyhow::Result<()>;

    /// The ids of every document of an organisation, deleted ones included, by id.
    async fn org_document_ids(&mut self, org_id: &str) -> anyhow::Result<Vec<Id>>;

    /// The accounts that are members of an organisation, by id.
    async fn org_member_ids(&mut self, org_id: &str) -> anyhow::Result<Vec<Id>>;

    /// Deletes an organisation; the database removes its members, invites and projects with it
    /// (and with those what [`Tx::delete_project`] removes).
    async fn delete_org(&mut self, id: &str) -> anyhow::Result<()>;
}
