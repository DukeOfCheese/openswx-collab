-- The schema of the collaboration server, version 1.
--
-- Kept in step with ../postgres/0001_init.sql: the two files differ only where the dialects must
-- (BLOB here, BYTEA there; COLLATE "C" there, which is how SQLite compares text anyway). Types are spelled so that both databases accept them and the row mapping
-- is the same on both: VARCHAR/TEXT -> String, BIGINT -> i64, DOUBLE PRECISION (epoch seconds) -> f64,
-- BOOLEAN -> bool (SQLite stores 0/1), BLOB/BYTEA (MessagePack) -> Vec<u8>, JSON as TEXT.
--
-- Never edit this file once released: add 0002_<what>.sql instead.

CREATE TABLE users (
    id VARCHAR(32) NOT NULL PRIMARY KEY,
    email VARCHAR(320) NOT NULL,
    name VARCHAR(200) NOT NULL,
    password_hash TEXT NULL,
    is_server_admin BOOLEAN NOT NULL DEFAULT FALSE,
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    email_verified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at DOUBLE PRECISION NOT NULL,
    deleted_at DOUBLE PRECISION NULL
);
CREATE UNIQUE INDEX ix_users_email ON users (email);

CREATE TABLE oauth_identities (
    provider VARCHAR(32) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    user_id VARCHAR(32) NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    PRIMARY KEY (provider, subject)
);
CREATE INDEX ix_oauth_identities_user_id ON oauth_identities (user_id);

CREATE TABLE sessions (
    token_hash VARCHAR(64) NOT NULL PRIMARY KEY,
    user_id VARCHAR(32) NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    created_at DOUBLE PRECISION NOT NULL,
    expires_at DOUBLE PRECISION NOT NULL,
    label VARCHAR(200) NOT NULL DEFAULT ''
);
CREATE INDEX ix_sessions_user_id ON sessions (user_id);

CREATE TABLE oauth_states (
    state_hash VARCHAR(64) NOT NULL PRIMARY KEY,
    provider VARCHAR(32) NOT NULL,
    redirect_uri TEXT NOT NULL,
    invite TEXT NULL,
    challenge VARCHAR(64) NOT NULL DEFAULT '',
    link_user_id VARCHAR(32) NULL,
    expires_at DOUBLE PRECISION NOT NULL
);

CREATE TABLE login_codes (
    code_hash VARCHAR(64) NOT NULL PRIMARY KEY,
    user_id VARCHAR(32) NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    challenge VARCHAR(64) NOT NULL DEFAULT '',
    link_provider VARCHAR(32) NULL,
    link_subject VARCHAR(255) NULL,
    link_email VARCHAR(320) NULL,
    expires_at DOUBLE PRECISION NOT NULL
);

CREATE TABLE password_resets (
    token_hash VARCHAR(64) NOT NULL PRIMARY KEY,
    user_id VARCHAR(32) NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    expires_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX ix_password_resets_user_id ON password_resets (user_id);

-- deleted_at: set when the organisation is deleted. From then on it is gone for everyone (its
-- members and invites go in the same transaction); the server removes what is left of it (its
-- projects, their documents and journals, its files) afterwards, a little at a time, and this row
-- last.
CREATE TABLE orgs (
    id VARCHAR(32) NOT NULL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    deleted_at DOUBLE PRECISION NULL
);

CREATE TABLE org_members (
    org_id VARCHAR(32) NOT NULL REFERENCES orgs (id) ON DELETE CASCADE,
    user_id VARCHAR(32) NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    role VARCHAR(16) NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (org_id, user_id)
);
CREATE INDEX ix_org_members_user_id ON org_members (user_id);

CREATE TABLE invites (
    id VARCHAR(32) NOT NULL PRIMARY KEY,
    token_hash VARCHAR(64) NOT NULL UNIQUE,
    org_id VARCHAR(32) NOT NULL REFERENCES orgs (id) ON DELETE CASCADE,
    email VARCHAR(320) NULL,
    role VARCHAR(16) NOT NULL DEFAULT 'member',
    projects TEXT NOT NULL DEFAULT '[]',
    created_by VARCHAR(32) NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    expires_at DOUBLE PRECISION NOT NULL,
    accepted_by VARCHAR(32) NULL
);
CREATE INDEX ix_invites_org_id ON invites (org_id);

CREATE TABLE projects (
    id VARCHAR(32) NOT NULL PRIMARY KEY,
    org_id VARCHAR(32) NOT NULL REFERENCES orgs (id) ON DELETE CASCADE,
    name VARCHAR(200) NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    -- Set when the project (or its organisation) is deleted: gone for everyone from then on, and
    -- removed afterwards, as an organisation is.
    deleted_at DOUBLE PRECISION NULL
);
CREATE INDEX ix_projects_org_id ON projects (org_id);

CREATE TABLE project_members (
    project_id VARCHAR(32) NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    user_id VARCHAR(32) NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    role VARCHAR(16) NOT NULL,
    PRIMARY KEY (project_id, user_id)
);
CREATE INDEX ix_project_members_user_id ON project_members (user_id);

CREATE TABLE folders (
    id VARCHAR(32) NOT NULL PRIMARY KEY,
    project_id VARCHAR(32) NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    parent_id VARCHAR(32) NULL REFERENCES folders (id) ON DELETE CASCADE,
    name VARCHAR(200) NOT NULL,
    created_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX ix_folders_project_id ON folders (project_id);
CREATE INDEX ix_folders_parent_id ON folders (parent_id);

CREATE TABLE documents (
    id VARCHAR(32) NOT NULL PRIMARY KEY,
    project_id VARCHAR(32) NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    folder_id VARCHAR(32) NULL REFERENCES folders (id) ON DELETE SET NULL,
    name VARCHAR(200) NOT NULL,
    kind VARCHAR(16) NOT NULL DEFAULT 'part',
    format_version BIGINT NULL,
    head_seq BIGINT NOT NULL DEFAULT 0,
    created_by VARCHAR(32) NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL,
    deleted_at DOUBLE PRECISION NULL
);
CREATE INDEX ix_documents_project_id ON documents (project_id);
CREATE INDEX ix_documents_folder_id ON documents (folder_id);

-- The journal: one row per accepted change. ops, inverse, paths and reads are MessagePack
-- (oswx_core wire forms). A NULL change_id never collides, so the unique key only binds a client's
-- own retries.
CREATE TABLE changes (
    document_id VARCHAR(32) NOT NULL REFERENCES documents (id) ON DELETE CASCADE,
    seq BIGINT NOT NULL,
    parent_seq BIGINT NOT NULL,
    branch_id VARCHAR(64) NOT NULL DEFAULT 'main',
    epoch BIGINT NOT NULL,
    author_id VARCHAR(32) NOT NULL,
    client_id VARCHAR(128) NULL,
    change_id VARCHAR(64) NULL,
    created_at DOUBLE PRECISION NOT NULL,
    command VARCHAR(100) NOT NULL DEFAULT '',
    ops BLOB NOT NULL,
    inverse BLOB NOT NULL,
    paths BLOB NOT NULL,
    reads BLOB NOT NULL,
    undo_of BIGINT NULL,
    reverts BIGINT NULL,
    reverted_by BIGINT NULL,
    broke TEXT NULL,
    PRIMARY KEY (document_id, seq),
    CONSTRAINT uq_change_id UNIQUE (document_id, client_id, change_id)
);
-- A joining client's latest change ids, newest first, without sorting the document's history.
CREATE INDEX ix_changes_client_seq ON changes (document_id, client_id, seq);
-- An account's own changes in journal order, a page at a time (its export).
CREATE INDEX ix_changes_author ON changes (author_id, document_id, seq);

CREATE TABLE snapshots (
    document_id VARCHAR(32) NOT NULL REFERENCES documents (id) ON DELETE CASCADE,
    epoch BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    key TEXT NOT NULL,
    size BIGINT NOT NULL DEFAULT 0,
    created_at DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (document_id, epoch, seq)
);

CREATE TABLE versions (
    id VARCHAR(32) NOT NULL PRIMARY KEY,
    document_id VARCHAR(32) NOT NULL REFERENCES documents (id) ON DELETE CASCADE,
    seq BIGINT NOT NULL,
    name VARCHAR(200) NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    author_id VARCHAR(32) NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    CONSTRAINT uq_version_name UNIQUE (document_id, name)
);
CREATE INDEX ix_versions_document_id ON versions (document_id);

CREATE TABLE assembly_refs (
    assembly_id VARCHAR(32) NOT NULL REFERENCES documents (id) ON DELETE CASCADE,
    component_id VARCHAR(200) NOT NULL,
    part_id VARCHAR(32) NOT NULL,
    seq BIGINT NOT NULL,
    PRIMARY KEY (assembly_id, component_id)
);
CREATE INDEX ix_assembly_refs_part_id ON assembly_refs (part_id);

CREATE TABLE room_leases (
    document_id VARCHAR(32) NOT NULL PRIMARY KEY REFERENCES documents (id) ON DELETE CASCADE,
    node_id VARCHAR(64) NOT NULL,
    epoch BIGINT NOT NULL,
    expires_at DOUBLE PRECISION NOT NULL
);
