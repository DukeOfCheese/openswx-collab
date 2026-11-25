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

// ------------------------------------------------------------------ organisations

/// [`super::Tx::insert_org`].
pub const INSERT_ORG: &str = "INSERT INTO orgs (id, name, created_at) VALUES ($1, $2, $3)";

/// [`super::Tx::set_org_name`].
pub const SET_ORG_NAME: &str = "UPDATE orgs SET name = $2 WHERE id = $1";

/// [`super::Store::org_by_id`], [`super::Tx::org_by_id`].
pub const ORG_BY_ID: &str = "SELECT id, name, created_at FROM orgs WHERE id = $1";

/// [`super::Store::org_member`], [`super::Tx::org_member`].
pub const ORG_MEMBER: &str = "SELECT org_id, user_id, role, created_at FROM org_members \
     WHERE org_id = $1 AND user_id = $2";

/// [`super::Store::orgs_of_user`]: one query, whatever the number of organisations.
pub const ORGS_OF_USER: &str = "SELECT o.id, o.name, mine.role, COUNT(everyone.user_id) AS members \
     FROM orgs o \
     JOIN org_members mine ON mine.org_id = o.id \
     JOIN org_members everyone ON everyone.org_id = o.id \
     WHERE mine.user_id = $1 \
     GROUP BY o.id, o.name, mine.role \
     ORDER BY o.name, o.id";

/// [`super::Store::org_members`].
pub const ORG_MEMBERS: &str = "SELECT u.id, u.email, u.name, m.role FROM users u \
     JOIN org_members m ON m.user_id = u.id WHERE m.org_id = $1 ORDER BY u.name, u.id";

/// [`super::Tx::insert_org_member`].
pub const INSERT_ORG_MEMBER: &str =
    "INSERT INTO org_members (org_id, user_id, role, created_at) VALUES ($1, $2, $3, $4)";

/// [`super::Tx::set_org_member_role`].
pub const SET_ORG_MEMBER_ROLE: &str =
    "UPDATE org_members SET role = $3 WHERE org_id = $1 AND user_id = $2";

/// [`super::Tx::delete_org_member`].
pub const DELETE_ORG_MEMBER: &str = "DELETE FROM org_members WHERE org_id = $1 AND user_id = $2";

/// [`super::Store::org_member_count`], [`super::Tx::org_member_count`].
pub const ORG_MEMBER_COUNT: &str = "SELECT COUNT(*) FROM org_members WHERE org_id = $1";

/// [`super::Tx::org_owner_count`] (bind `"owner"`).
pub const ORG_OWNER_COUNT: &str =
    "SELECT COUNT(*) FROM org_members WHERE org_id = $1 AND role = $2";

/// [`super::Tx::delete_project_members_in_org`].
pub const DELETE_PROJECT_MEMBERS_IN_ORG: &str = "DELETE FROM project_members WHERE user_id = $2 \
     AND project_id IN (SELECT id FROM projects WHERE org_id = $1)";

// ------------------------------------------------------------------ invites

/// The columns of a [`super::Invite`], in order.
macro_rules! invite_columns {
    () => {
        "id, token_hash, org_id, email, role, projects, created_by, created_at, expires_at, accepted_by"
    };
}

/// [`super::Tx::insert_invite`].
pub const INSERT_INVITE: &str = concat!(
    "INSERT INTO invites (",
    invite_columns!(),
    ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
);

/// [`super::Store::invite_by_id`].
pub const INVITE_BY_ID: &str = concat!("SELECT ", invite_columns!(), " FROM invites WHERE id = $1");

/// [`super::Tx::invite_by_hash`].
pub const INVITE_BY_HASH: &str = concat!(
    "SELECT ",
    invite_columns!(),
    " FROM invites WHERE token_hash = $1"
);

/// [`super::Store::open_invites`].
pub const OPEN_INVITES: &str = concat!(
    "SELECT ",
    invite_columns!(),
    " FROM invites WHERE org_id = $1 AND accepted_by IS NULL AND expires_at > $2 \
     ORDER BY created_at, id"
);

/// [`super::Tx::claim_invite`].
pub const CLAIM_INVITE: &str =
    "UPDATE invites SET accepted_by = $2 WHERE id = $1 AND accepted_by IS NULL";

/// [`super::Tx::delete_invite`].
pub const DELETE_INVITE: &str = "DELETE FROM invites WHERE org_id = $1 AND id = $2";

// ------------------------------------------------------------------ projects

/// [`super::Tx::insert_project`].
pub const INSERT_PROJECT: &str =
    "INSERT INTO projects (id, org_id, name, created_at) VALUES ($1, $2, $3, $4)";

/// [`super::Tx::set_project_name`].
pub const SET_PROJECT_NAME: &str = "UPDATE projects SET name = $2 WHERE id = $1";

/// [`super::Tx::delete_project`].
pub const DELETE_PROJECT: &str = "DELETE FROM projects WHERE id = $1";

/// [`super::Store::project_by_id`], [`super::Tx::project_by_id`].
pub const PROJECT_BY_ID: &str = "SELECT id, org_id, name, created_at FROM projects WHERE id = $1";

/// [`super::Store::projects_in_org`].
pub const PROJECTS_IN_ORG: &str =
    "SELECT id, org_id, name, created_at FROM projects WHERE org_id = $1 ORDER BY name, id";

/// [`super::Store::member_projects`].
pub const MEMBER_PROJECTS: &str = "SELECT p.id, p.name, p.org_id, pm.role FROM projects p \
     JOIN project_members pm ON pm.project_id = p.id \
     WHERE p.org_id = $1 AND pm.user_id = $2 ORDER BY p.name, p.id";

/// [`super::Store::project_member`], [`super::Tx::project_member`].
pub const PROJECT_MEMBER: &str = "SELECT project_id, user_id, role FROM project_members \
     WHERE project_id = $1 AND user_id = $2";

/// [`super::Store::project_members`].
pub const PROJECT_MEMBERS: &str = "SELECT u.id, u.email, u.name, pm.role FROM users u \
     JOIN project_members pm ON pm.user_id = u.id WHERE pm.project_id = $1 ORDER BY u.name, u.id";

/// [`super::Tx::insert_project_member`].
pub const INSERT_PROJECT_MEMBER: &str =
    "INSERT INTO project_members (project_id, user_id, role) VALUES ($1, $2, $3)";

/// [`super::Tx::set_project_member_role`].
pub const SET_PROJECT_MEMBER_ROLE: &str =
    "UPDATE project_members SET role = $3 WHERE project_id = $1 AND user_id = $2";

/// [`super::Tx::delete_project_member`].
pub const DELETE_PROJECT_MEMBER: &str =
    "DELETE FROM project_members WHERE project_id = $1 AND user_id = $2";

/// [`super::Tx::project_document_ids`].
pub const PROJECT_DOCUMENT_IDS: &str = "SELECT id FROM documents WHERE project_id = $1 ORDER BY id";

/// [`super::Tx::mark_project_documents_deleted`].
pub const MARK_PROJECT_DOCUMENTS_DELETED: &str =
    "UPDATE documents SET deleted_at = $2 WHERE project_id = $1 AND deleted_at IS NULL";
