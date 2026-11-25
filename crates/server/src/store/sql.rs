//! SQL text shared by the SQLite and PostgreSQL stores: `$n` placeholders and nothing dialect
//! specific. Statements that cannot be portable (the catalogue queries) live with their store.
//!
//! One constant per query, named after the [`super::Store`] / [`super::Tx`] method that runs it.

/// [`super::Store::health`].
pub const HEALTH: &str = "SELECT 1";

/// The versions and checksums recorded by sqlx's migrator, which both stores share (bind `true`).
pub const APPLIED_MIGRATIONS: &str =
    "SELECT version, checksum FROM _sqlx_migrations WHERE success = $1 ORDER BY version";

/// [`super::Store::user_count`]: every account, deleted and disabled ones included.
pub const USER_COUNT: &str = "SELECT COUNT(*) FROM users";
