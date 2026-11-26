//! SQL text shared by the SQLite and PostgreSQL stores: `$n` placeholders and nothing dialect
//! specific.
//!
//! One constant per query, named after the [`super::Store`] / [`super::Tx`] method that runs it.
//!
//! The statements the two databases cannot share are at the end of this file, each under one name
//! in [`sqlite`] and in [`postgres`]: the catalogue queries, and the row locks (`SELECT … FOR
//! UPDATE` on PostgreSQL; a plain read on SQLite, whose one write transaction at a time already
//! excludes every other writer). Nothing else in the server knows which database it runs on.

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

// ------------------------------------------------------------------ sign-in through a provider

/// [`super::Store::oauth_identity`], [`super::Tx::oauth_identity`].
pub const OAUTH_IDENTITY: &str = "SELECT provider, subject, user_id FROM oauth_identities \
     WHERE provider = $1 AND subject = $2";

/// [`super::Store::user_identities`], [`super::Tx::user_identities`].
pub const USER_IDENTITIES: &str = "SELECT provider, subject, user_id FROM oauth_identities \
     WHERE user_id = $1 ORDER BY provider, subject";

/// [`super::Tx::insert_oauth_identity`].
pub const INSERT_OAUTH_IDENTITY: &str =
    "INSERT INTO oauth_identities (provider, subject, user_id) VALUES ($1, $2, $3)";

/// [`super::Tx::delete_user_identities`].
pub const DELETE_USER_IDENTITIES: &str =
    "DELETE FROM oauth_identities WHERE user_id = $1 AND provider = $2";

/// [`super::Tx::insert_oauth_state`].
pub const INSERT_OAUTH_STATE: &str = "INSERT INTO oauth_states \
     (state_hash, provider, redirect_uri, invite, challenge, link_user_id, expires_at) \
     VALUES ($1, $2, $3, $4, $5, $6, $7)";

/// [`super::Tx::take_oauth_state`].
pub const TAKE_OAUTH_STATE: &str = "DELETE FROM oauth_states WHERE state_hash = $1 \
     RETURNING state_hash, provider, redirect_uri, invite, challenge, link_user_id, expires_at";

/// [`super::Tx::delete_expired_oauth_states`].
pub const DELETE_EXPIRED_OAUTH_STATES: &str = "DELETE FROM oauth_states WHERE expires_at < $1";

/// [`super::Tx::insert_login_code`].
pub const INSERT_LOGIN_CODE: &str = "INSERT INTO login_codes \
     (code_hash, user_id, challenge, link_provider, link_subject, link_email, expires_at) \
     VALUES ($1, $2, $3, $4, $5, $6, $7)";

/// [`super::Tx::take_login_code`].
pub const TAKE_LOGIN_CODE: &str = "DELETE FROM login_codes WHERE code_hash = $1 \
     RETURNING code_hash, user_id, challenge, link_provider, link_subject, link_email, expires_at";

/// [`super::Tx::delete_expired_login_codes`].
pub const DELETE_EXPIRED_LOGIN_CODES: &str = "DELETE FROM login_codes WHERE expires_at < $1";

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

/// [`super::Tx::mark_project_documents_deleted`], first statement: the documents to mark, in the
/// order they are marked (then [`MARK_DOCUMENT_DELETED`] for each).
pub const PROJECT_LIVE_DOCUMENT_IDS: &str =
    "SELECT id FROM documents WHERE project_id = $1 AND deleted_at IS NULL ORDER BY id";

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

/// How many rows one [`insert_changes`] statement carries at most: 18 parameters a row, far below
/// SQLite's 32,766 and PostgreSQL's 65,535 parameters a statement.
pub const CHANGES_PER_INSERT: usize = 512;

/// [`super::Tx::insert_changes`]: [`INSERT_CHANGE`] for `rows` rows in one statement (`rows >= 1`;
/// at most [`CHANGES_PER_INSERT`]). Rows whose `(document_id, client_id, change_id)` is taken are
/// left out; the statement's row count says how many went in.
pub fn insert_changes(rows: usize) -> String {
    const COLUMNS: usize = 18;
    let mut text = String::from(concat!(
        "INSERT INTO changes (",
        change_columns!(),
        ") VALUES "
    ));
    for row in 0..rows.max(1) {
        text.push_str(if row == 0 { "(" } else { ", (" });
        for column in 1..=COLUMNS {
            if column > 1 {
                text.push_str(", ");
            }
            text.push('$');
            text.push_str(&(row * COLUMNS + column).to_string());
        }
        text.push(')');
    }
    text.push_str(" ON CONFLICT (document_id, client_id, change_id) DO NOTHING");
    text
}

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

/// [`super::Store::change_events_between`]: what a change event carries, with the author's name
/// and, for an undo, the change id of the change it undoes.
pub const CHANGE_EVENTS_BETWEEN: &str = "SELECT c.seq, c.author_id, u.name AS author_name, \
     c.client_id, c.change_id, c.command, c.ops, c.reverts, uc.change_id AS undo_of \
     FROM changes c LEFT JOIN users u ON u.id = c.author_id \
     LEFT JOIN changes uc ON uc.document_id = c.document_id AND uc.seq = c.undo_of \
     WHERE c.document_id = $1 AND c.seq > $2 AND c.seq <= $3 ORDER BY c.seq LIMIT $4";

/// [`super::Store::undo_target`].
pub const UNDO_TARGET: &str = "SELECT seq, inverse, reverted_by FROM changes \
     WHERE document_id = $1 AND client_id = $2 AND change_id = $3";

/// [`super::Store::change_sizes`]: newest first, `after < seq < before`, the stored size of each
/// row's `ops` (read without the bytes).
pub const CHANGE_SIZES: &str = "SELECT seq, CAST(LENGTH(ops) AS BIGINT) AS size FROM changes \
     WHERE document_id = $1 AND seq < $2 AND seq > $3 ORDER BY seq DESC LIMIT $4";

/// [`super::Store::history`]: newest first, `after < seq < before`.
pub const HISTORY: &str = "SELECT c.seq, c.author_id, u.name AS author_name, c.client_id, \
     c.change_id, c.created_at, c.command, c.ops, c.reverts, c.reverted_by, c.broke \
     FROM changes c LEFT JOIN users u ON u.id = c.author_id \
     WHERE c.document_id = $1 AND c.seq < $2 AND c.seq > $3 ORDER BY c.seq DESC LIMIT $4";

/// [`super::Tx::set_change_broke`].
pub const SET_CHANGE_BROKE: &str =
    "UPDATE changes SET broke = $3 WHERE document_id = $1 AND seq = $2";

// ------------------------------------------------------------------ versions

/// [`super::Store::versions`]: newest seq first, then newest first.
pub const VERSIONS: &str = "SELECT v.id, v.document_id, v.seq, v.name, v.note, v.author_id, \
     u.name AS author_name, v.created_at FROM versions v LEFT JOIN users u ON u.id = v.author_id \
     WHERE v.document_id = $1 ORDER BY v.seq DESC, v.created_at DESC, v.id";

/// [`super::Store::version_by_name`].
pub const VERSION_BY_NAME: &str = "SELECT id, document_id, seq, name, note, author_id, created_at \
     FROM versions WHERE document_id = $1 AND name = $2";

/// [`super::Tx::insert_version`]: nothing when the document has a version of that name.
pub const INSERT_VERSION: &str = "INSERT INTO versions (id, document_id, seq, name, note, \
     author_id, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7) \
     ON CONFLICT (document_id, name) DO NOTHING";

/// [`super::Tx::delete_version`].
pub const DELETE_VERSION: &str = "DELETE FROM versions WHERE document_id = $1 AND id = $2";

/// [`super::Store::change_seq_by_client`].
pub const CHANGE_SEQ_BY_CLIENT: &str =
    "SELECT seq FROM changes WHERE document_id = $1 AND client_id = $2 AND change_id = $3";

/// [`super::Store::recent_change_ids`].
pub const RECENT_CHANGE_IDS: &str = "SELECT change_id, seq FROM changes \
     WHERE document_id = $1 AND client_id = $2 AND change_id IS NOT NULL ORDER BY seq DESC LIMIT $3";

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

/// [`super::Tx::lock_documents`], completed by [`in_list`] and then the dialect's
/// `IN_ID_ORDER_LOCKED`: the rows are locked in ascending id order.
pub const LOCK_DOCUMENTS: &str = "SELECT id FROM documents WHERE id IN ";

/// [`super::Store::parts_by_ids`], completed by [`in_list`] with one placeholder per id.
pub const PARTS_BY_IDS: &str = "SELECT d.id, d.project_id, p.org_id, d.name, d.kind, d.head_seq, \
     d.deleted_at FROM documents d JOIN projects p ON p.id = d.project_id WHERE d.id IN ";

/// `prefix` followed by `($1, $2, ..., $n)`: an `IN` list of `n` placeholders (`n >= 1`), in
/// portable SQL.
pub fn in_list(prefix: &str, n: usize) -> String {
    let marks: Vec<String> = (1..=n.max(1)).map(|i| format!("${i}")).collect();
    format!("{prefix}({})", marks.join(", "))
}

/// [`super::Store::assemblies_using`], [`super::Tx::assemblies_using`].
pub const ASSEMBLIES_USING: &str = "SELECT d.id, d.name, d.project_id, p.org_id FROM documents d \
     JOIN projects p ON p.id = d.project_id \
     WHERE d.deleted_at IS NULL AND d.id IN \
     (SELECT r.assembly_id FROM assembly_refs r WHERE r.part_id = $1) ORDER BY d.name, d.id";

/// [`super::Tx::assemblies_using_project`].
pub const ASSEMBLIES_USING_PROJECT: &str = "SELECT d.id, d.name, d.project_id, p.org_id \
     FROM documents d JOIN projects p ON p.id = d.project_id \
     WHERE d.deleted_at IS NULL AND d.project_id <> $1 AND d.id IN \
     (SELECT r.assembly_id FROM assembly_refs r JOIN documents part ON part.id = r.part_id \
     WHERE part.project_id = $1) ORDER BY d.name, d.id";

/// [`super::Store::versions_of`], completed by [`in_list`] (the order is the caller's).
pub const VERSIONS_OF: &str = "SELECT v.id, v.document_id, v.seq, v.name, v.note, v.author_id, \
     u.name AS author_name, v.created_at FROM versions v LEFT JOIN users u ON u.id = v.author_id \
     WHERE v.document_id IN ";

/// [`super::Store::refs_to_part`].
pub const REFS_TO_PART: &str = "SELECT assembly_id, component_id, part_id, seq FROM assembly_refs \
     WHERE part_id = $1 ORDER BY assembly_id, component_id";

/// [`super::Store::pinned_before`].
pub const PINNED_BEFORE: &str = "SELECT assembly_id, component_id FROM assembly_refs \
     WHERE part_id = $1 AND seq < $2 ORDER BY assembly_id, component_id";

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

/// [`super::Tx::insert_room_lease`]: nothing when the document already has a lease row (on
/// PostgreSQL the statement first waits for a transaction that is inserting one).
pub const INSERT_ROOM_LEASE: &str = "INSERT INTO room_leases (document_id, node_id, epoch, \
     expires_at) VALUES ($1, $2, $3, $4) ON CONFLICT (document_id) DO NOTHING";

/// [`super::Tx::take_room_lease`]: the next epoch, for `$2` until `$3`.
pub const TAKE_ROOM_LEASE: &str = "UPDATE room_leases SET node_id = $2, epoch = epoch + 1, \
     expires_at = $3 WHERE document_id = $1 RETURNING epoch";

// ------------------------------------------------------------------ leaving

/// [`super::Tx::orgs_owned_alone`] (bind the account, then `"owner"`).
pub const ORGS_OWNED_ALONE: &str = "SELECT o.id, o.name, o.created_at FROM orgs o \
     JOIN org_members m ON m.org_id = o.id \
     WHERE m.user_id = $1 AND m.role = $2 \
     AND (SELECT COUNT(*) FROM org_members w WHERE w.org_id = o.id AND w.role = $2) <= 1 \
     ORDER BY o.name, o.id";

/// [`super::Tx::active_admin_count`] (bind `true`, `false`).
pub const ACTIVE_ADMIN_COUNT: &str =
    "SELECT COUNT(*) FROM users WHERE is_server_admin = $1 AND disabled = $2";

/// [`super::Tx::other_active_user_count`] (bind the account, then `false`).
pub const OTHER_ACTIVE_USER_COUNT: &str =
    "SELECT COUNT(*) FROM users WHERE disabled = $2 AND id <> $1";

/// [`super::Tx::user_org_ids`].
pub const USER_ORG_IDS: &str = "SELECT org_id FROM org_members WHERE user_id = $1 ORDER BY org_id";

/// [`super::Tx::erase_user`]: the account's sign-in providers ...
pub const DELETE_ALL_USER_IDENTITIES: &str = "DELETE FROM oauth_identities WHERE user_id = $1";

/// ... its unused login codes ...
pub const DELETE_USER_LOGIN_CODES: &str = "DELETE FROM login_codes WHERE user_id = $1";

/// ... its memberships ...
pub const DELETE_USER_ORG_MEMBERS: &str = "DELETE FROM org_members WHERE user_id = $1";

/// ... its project roles ...
pub const DELETE_USER_PROJECT_MEMBERS: &str = "DELETE FROM project_members WHERE user_id = $1";

/// ... and the row itself, emptied (bind the id, the new address, the new name, `false`, `true`,
/// the time).
pub const ERASE_USER: &str = "UPDATE users SET email = $2, name = $3, password_hash = NULL, \
     email_verified = $4, is_server_admin = $4, disabled = $5, deleted_at = $6 WHERE id = $1";

/// [`super::Tx::org_document_ids`]: deleted documents included.
pub const ORG_DOCUMENT_IDS: &str = "SELECT d.id FROM documents d \
     JOIN projects p ON p.id = d.project_id WHERE p.org_id = $1 ORDER BY d.id";

/// [`super::Tx::org_member_ids`].
pub const ORG_MEMBER_IDS: &str =
    "SELECT user_id FROM org_members WHERE org_id = $1 ORDER BY user_id";

/// [`super::Tx::delete_org`].
pub const DELETE_ORG: &str = "DELETE FROM orgs WHERE id = $1";

/// [`super::Store::user_memberships`].
pub const USER_MEMBERSHIPS: &str = "SELECT o.id, o.name, m.role, m.created_at AS since \
     FROM orgs o JOIN org_members m ON m.org_id = o.id WHERE m.user_id = $1 ORDER BY o.name, o.id";

/// [`super::Store::user_project_roles`].
pub const USER_PROJECT_ROLES: &str = "SELECT p.id, p.name, p.org_id, pm.role FROM projects p \
     JOIN project_members pm ON pm.project_id = p.id WHERE pm.user_id = $1 ORDER BY p.name, p.id";

/// [`super::Store::user_sessions`].
pub const USER_SESSIONS: &str = "SELECT token_hash, user_id, created_at, expires_at, label \
     FROM sessions WHERE user_id = $1 ORDER BY created_at, token_hash";

/// [`super::Store::changes_by_author`]: in the order of the journal's primary key, from just after
/// `(document, seq)`, so that reading every page walks the journal once.
pub const CHANGES_BY_AUTHOR: &str = "SELECT document_id, seq, created_at, command FROM changes \
     WHERE author_id = $1 AND (document_id, seq) > ($2, $3) ORDER BY document_id, seq LIMIT $4";

/// [`super::Store::versions_by_author`]: by id, from just after `$2`.
pub const VERSIONS_BY_AUTHOR: &str = "SELECT id, document_id, seq, name, note, author_id, \
     created_at FROM versions WHERE author_id = $1 AND id > $2 ORDER BY id LIMIT $3";

// ------------------------------------------------------------------ what the dialects do not share

/// Declares each statement under one name in [`sqlite`] and in [`postgres`], with its text for
/// each.
macro_rules! dialects {
    ($($(#[$doc:meta])* $name:ident: $sqlite:expr, $postgres:expr;)*) => {
        /// The statements with SQLite's text. One write transaction runs at a time there, so a
        /// "lock" is a plain read.
        pub mod sqlite {
            $($(#[$doc])* pub const $name: &str = $sqlite;)*
        }
        /// The statements with PostgreSQL's text.
        pub mod postgres {
            $($(#[$doc])* pub const $name: &str = $postgres;)*
        }
    };
}

dialects! {
    /// Whether sqlx's migration record exists (a count: 0 or 1).
    MIGRATION_RECORD_EXISTS:
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_name = '_sqlx_migrations'";

    /// How many tables there are besides the database's own and the migration record (on
    /// PostgreSQL: in the schema in use).
    OTHER_TABLES:
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name <> '_sqlx_migrations'",
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' \
         AND table_name <> '_sqlx_migrations'";

    /// [`crate::store::Tx::lock_accounts`]: on PostgreSQL an advisory lock held until the transaction
    /// ends (the key is the bytes of `oswxacct`).
    LOCK_ACCOUNTS:
        "SELECT 1",
        "SELECT pg_advisory_xact_lock(8030893919445410676)";

    /// [`crate::store::Tx::lock_user`]. `NO KEY`: a row that only refers to the account (a session, a
    /// membership) can still be inserted meanwhile.
    LOCK_USER:
        "SELECT id FROM users WHERE id = $1",
        "SELECT id FROM users WHERE id = $1 FOR NO KEY UPDATE";

    /// [`crate::store::Tx::lock_org`].
    LOCK_ORG:
        "SELECT id FROM orgs WHERE id = $1",
        "SELECT id FROM orgs WHERE id = $1 FOR UPDATE";

    /// What follows [`LOCK_DOCUMENTS`](super::LOCK_DOCUMENTS)'s id list. `NO KEY`: rows that only
    /// refer to the document (a journal row, a snapshot, a lease) can still be inserted meanwhile.
    IN_ID_ORDER_LOCKED:
        " ORDER BY id",
        " ORDER BY id FOR NO KEY UPDATE";

    /// [`crate::store::Tx::move_folder_documents`], first statement: the documents in the folder, locked
    /// in ascending id order before they are updated together.
    LOCK_FOLDER_DOCUMENTS:
        "SELECT id FROM documents WHERE folder_id = $1 ORDER BY id",
        "SELECT id FROM documents WHERE folder_id = $1 ORDER BY id FOR NO KEY UPDATE";

    /// [`crate::store::Tx::lock_live_document`]: matches only a document that is not deleted (on
    /// PostgreSQL, as it is once any transaction that was changing the row has ended).
    LOCK_LIVE_DOCUMENT:
        "SELECT id FROM documents WHERE id = $1 AND deleted_at IS NULL",
        "SELECT id FROM documents WHERE id = $1 AND deleted_at IS NULL FOR NO KEY UPDATE";

    /// [`crate::store::Tx::lock_room_lease`], answering the epoch.
    LOCK_ROOM_LEASE:
        "SELECT epoch FROM room_leases WHERE document_id = $1",
        "SELECT epoch FROM room_leases WHERE document_id = $1 FOR UPDATE";

    /// [`crate::store::Tx::change_broke`], answering the column.
    CHANGE_BROKE:
        "SELECT broke FROM changes WHERE document_id = $1 AND seq = $2",
        "SELECT broke FROM changes WHERE document_id = $1 AND seq = $2 FOR UPDATE";
}
