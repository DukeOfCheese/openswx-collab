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

/// [`super::Store::invite_by_hash`], [`super::Tx::invite_by_hash`].
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

// ------------------------------------------------------------------ documents

/// The columns of [`super::Document`], in order.
macro_rules! document_columns {
    () => {
        "id, project_id, folder_id, name, kind, format_version, head_seq, created_by, created_at, \
         updated_at, deleted_at"
    };
}

/// [`super::Tx::insert_document`].
pub const INSERT_DOCUMENT: &str = concat!(
    "INSERT INTO documents (",
    document_columns!(),
    ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
);

/// [`super::Store::document_by_id`], [`super::Tx::document_by_id`].
pub const DOCUMENT_BY_ID: &str = concat!(
    "SELECT ",
    document_columns!(),
    " FROM documents WHERE id = $1"
);

/// [`super::Store::documents_in_project`].
pub const DOCUMENTS_IN_PROJECT: &str = concat!(
    "SELECT ",
    document_columns!(),
    " FROM documents WHERE project_id = $1 AND deleted_at IS NULL ORDER BY name, id"
);

/// [`super::Tx::set_document_name`].
pub const SET_DOCUMENT_NAME: &str = "UPDATE documents SET name = $2 WHERE id = $1";

/// [`super::Tx::set_document_folder`].
pub const SET_DOCUMENT_FOLDER: &str = "UPDATE documents SET folder_id = $2 WHERE id = $1";

/// [`super::Tx::mark_document_deleted`].
pub const MARK_DOCUMENT_DELETED: &str =
    "UPDATE documents SET deleted_at = $2 WHERE id = $1 AND deleted_at IS NULL";

/// [`super::Tx::insert_snapshot`].
pub const INSERT_SNAPSHOT: &str = "INSERT INTO snapshots (document_id, epoch, seq, key, size, \
     created_at) VALUES ($1, $2, $3, $4, $5, $6)";

/// [`super::Store::latest_snapshot`].
pub const LATEST_SNAPSHOT: &str = "SELECT document_id, epoch, seq, key, size, created_at \
     FROM snapshots WHERE document_id = $1 ORDER BY seq DESC, epoch DESC LIMIT 1";

// ------------------------------------------------------------------ folders

/// [`super::Tx::insert_folder`].
pub const INSERT_FOLDER: &str = "INSERT INTO folders (id, project_id, parent_id, name, created_at) \
     VALUES ($1, $2, $3, $4, $5)";

/// [`super::Store::folder_by_id`], [`super::Tx::folder_by_id`].
pub const FOLDER_BY_ID: &str =
    "SELECT id, project_id, parent_id, name, created_at FROM folders WHERE id = $1";

/// [`super::Store::folders_in_project`], [`super::Tx::folders_in_project`].
pub const FOLDERS_IN_PROJECT: &str = "SELECT id, project_id, parent_id, name, created_at \
     FROM folders WHERE project_id = $1 ORDER BY name, id";

/// [`super::Tx::set_folder_name`].
pub const SET_FOLDER_NAME: &str = "UPDATE folders SET name = $2 WHERE id = $1";

/// [`super::Tx::set_folder_parent`].
pub const SET_FOLDER_PARENT: &str = "UPDATE folders SET parent_id = $2 WHERE id = $1";

/// [`super::Tx::move_child_folders`].
pub const MOVE_CHILD_FOLDERS: &str = "UPDATE folders SET parent_id = $2 WHERE parent_id = $1";

/// [`super::Tx::move_folder_documents`].
pub const MOVE_FOLDER_DOCUMENTS: &str = "UPDATE documents SET folder_id = $2 WHERE folder_id = $1";

/// [`super::Tx::delete_folder`].
pub const DELETE_FOLDER: &str = "DELETE FROM folders WHERE id = $1";

// ------------------------------------------------------------------ journal

/// The columns of [`super::ChangeRow`], in order.
macro_rules! change_columns {
    () => {
        "document_id, seq, parent_seq, branch_id, epoch, author_id, client_id, change_id, \
         created_at, command, ops, inverse, paths, reads, undo_of, reverts, reverted_by, broke"
    };
}

/// [`super::Tx::insert_change`]. A row with the same `(document_id, client_id, change_id)` makes
/// it insert nothing (and say so) instead of failing the transaction.
pub const INSERT_CHANGE: &str = concat!(
    "INSERT INTO changes (",
    change_columns!(),
    ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18) \
     ON CONFLICT (document_id, client_id, change_id) DO NOTHING"
);

/// [`super::Store::changes_between`].
pub const CHANGES_BETWEEN: &str = concat!(
    "SELECT ",
    change_columns!(),
    " FROM changes WHERE document_id = $1 AND seq > $2 AND seq <= $3 ORDER BY seq LIMIT $4"
);

/// [`super::Store::change_by_seq`].
pub const CHANGE_BY_SEQ: &str = concat!(
    "SELECT ",
    change_columns!(),
    " FROM changes WHERE document_id = $1 AND seq = $2"
);

/// [`super::Store::change_events_between`]: what a change event carries, with the author's name.
pub const CHANGE_EVENTS_BETWEEN: &str = "SELECT c.seq, c.author_id, u.name AS author_name, \
     c.client_id, c.change_id, c.command, c.ops FROM changes c LEFT JOIN users u ON u.id = c.author_id \
     WHERE c.document_id = $1 AND c.seq > $2 AND c.seq <= $3 ORDER BY c.seq LIMIT $4";

/// [`super::Store::change_seq_by_client`].
pub const CHANGE_SEQ_BY_CLIENT: &str =
    "SELECT seq FROM changes WHERE document_id = $1 AND client_id = $2 AND change_id = $3";

/// [`super::Tx::set_document_head`].
pub const SET_DOCUMENT_HEAD: &str =
    "UPDATE documents SET head_seq = $2, updated_at = $3 WHERE id = $1";

/// [`super::Tx::set_reverted_by`]: only a row not yet reverted.
pub const SET_REVERTED_BY: &str = "UPDATE changes SET reverted_by = $3 \
     WHERE document_id = $1 AND seq = $2 AND reverted_by IS NULL";

/// [`super::Tx::replace_assembly_refs`], first statement.
pub const DELETE_ASSEMBLY_REFS: &str = "DELETE FROM assembly_refs WHERE assembly_id = $1";

/// [`super::Tx::replace_assembly_refs`], once per row.
pub const INSERT_ASSEMBLY_REF: &str = "INSERT INTO assembly_refs (assembly_id, component_id, \
     part_id, seq) VALUES ($1, $2, $3, $4)";

/// [`super::Store::assembly_refs`].
pub const ASSEMBLY_REFS: &str = "SELECT assembly_id, component_id, part_id, seq FROM assembly_refs \
     WHERE assembly_id = $1 ORDER BY component_id";

/// [`super::Store::snapshot_at_or_before`]: the highest seq not above `$2`, then the highest epoch.
pub const SNAPSHOT_AT_OR_BEFORE: &str = "SELECT document_id, epoch, seq, key, size, created_at \
     FROM snapshots WHERE document_id = $1 AND seq <= $2 ORDER BY seq DESC, epoch DESC LIMIT 1";

/// [`super::Store::snapshot_row`], [`super::Tx::snapshot_row`].
pub const SNAPSHOT_ROW: &str = "SELECT document_id, epoch, seq, key, size, created_at \
     FROM snapshots WHERE document_id = $1 AND epoch = $2 AND seq = $3";

// ------------------------------------------------------------------ room leases

/// [`super::Store::room_lease`], [`super::Tx::room_lease`].
pub const ROOM_LEASE: &str =
    "SELECT document_id, node_id, epoch, expires_at FROM room_leases WHERE document_id = $1";

/// [`super::Tx::insert_room_lease`].
pub const INSERT_ROOM_LEASE: &str = "INSERT INTO room_leases (document_id, node_id, epoch, \
     expires_at) VALUES ($1, $2, $3, $4)";

/// [`super::Tx::lock_room_lease`]: an update that changes nothing, so that the row is locked
/// until the transaction ends (PostgreSQL; SQLite's write transaction already excludes every other
/// writer), answering the epoch.
pub const LOCK_ROOM_LEASE: &str =
    "UPDATE room_leases SET epoch = epoch WHERE document_id = $1 RETURNING epoch";

/// [`super::Tx::take_room_lease`]: the next epoch, for `$2` until `$3`.
pub const TAKE_ROOM_LEASE: &str = "UPDATE room_leases SET node_id = $2, epoch = epoch + 1, \
     expires_at = $3 WHERE document_id = $1 RETURNING epoch";
