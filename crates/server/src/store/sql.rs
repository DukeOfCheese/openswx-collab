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

/// The columns of a [`super::User`], in order.
macro_rules! user_columns {
    () => {
        "id, email, name, password_hash, is_server_admin, disabled, email_verified, created_at, deleted_at"
    };
}

/// [`super::Tx::insert_user`].
pub const INSERT_USER: &str = concat!(
    "INSERT INTO users (",
    user_columns!(),
    ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
);

/// [`super::Store::user_by_id`], [`super::Tx::user_by_id`].
pub const USER_BY_ID: &str = concat!("SELECT ", user_columns!(), " FROM users WHERE id = $1");

/// [`super::Store::user_by_email`], [`super::Tx::user_by_email`].
pub const USER_BY_EMAIL: &str = concat!("SELECT ", user_columns!(), " FROM users WHERE email = $1");

/// [`super::Tx::set_user_name`].
pub const SET_USER_NAME: &str = "UPDATE users SET name = $2 WHERE id = $1";

/// [`super::Tx::set_user_password`].
pub const SET_USER_PASSWORD: &str = "UPDATE users SET password_hash = $2 WHERE id = $1";

/// [`super::Tx::set_user_email_verified`].
pub const SET_USER_EMAIL_VERIFIED: &str = "UPDATE users SET email_verified = $2 WHERE id = $1";

/// [`super::Tx::set_user_disabled`].
pub const SET_USER_DISABLED: &str = "UPDATE users SET disabled = $2 WHERE id = $1";

/// [`super::Tx::insert_session`].
pub const INSERT_SESSION: &str = "INSERT INTO sessions (token_hash, user_id, created_at, expires_at, label) \
     VALUES ($1, $2, $3, $4, $5)";

/// [`super::Store::session_by_hash`].
pub const SESSION_BY_HASH: &str = "SELECT token_hash, user_id, created_at, expires_at, label FROM sessions \
     WHERE token_hash = $1";

/// [`super::Tx::delete_session`].
pub const DELETE_SESSION: &str = "DELETE FROM sessions WHERE token_hash = $1";

/// [`super::Tx::delete_user_sessions`].
pub const DELETE_USER_SESSIONS: &str = "DELETE FROM sessions WHERE user_id = $1";

/// [`super::Tx::delete_other_user_sessions`].
pub const DELETE_OTHER_USER_SESSIONS: &str =
    "DELETE FROM sessions WHERE user_id = $1 AND token_hash <> $2";

/// [`super::Tx::delete_expired_user_sessions`].
pub const DELETE_EXPIRED_USER_SESSIONS: &str =
    "DELETE FROM sessions WHERE user_id = $1 AND expires_at < $2";

/// [`super::Tx::insert_password_reset`].
pub const INSERT_PASSWORD_RESET: &str =
    "INSERT INTO password_resets (token_hash, user_id, expires_at) VALUES ($1, $2, $3)";

/// [`super::Store::password_reset_by_hash`].
pub const PASSWORD_RESET_BY_HASH: &str =
    "SELECT token_hash, user_id, expires_at FROM password_resets WHERE token_hash = $1";

/// [`super::Tx::take_password_reset`]: deletes the row and answers it (`RETURNING` is in both
/// SQLite 3.35+ and PostgreSQL).
pub const TAKE_PASSWORD_RESET: &str = "DELETE FROM password_resets WHERE token_hash = $1 \
     RETURNING token_hash, user_id, expires_at";

/// [`super::Tx::delete_user_password_resets`].
pub const DELETE_USER_PASSWORD_RESETS: &str = "DELETE FROM password_resets WHERE user_id = $1";

/// [`super::Tx::delete_expired_password_resets`].
pub const DELETE_EXPIRED_PASSWORD_RESETS: &str =
    "DELETE FROM password_resets WHERE expires_at < $1";
