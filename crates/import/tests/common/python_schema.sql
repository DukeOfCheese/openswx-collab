-- The Python server's SQLite schema at Alembic revision 0003: revisions 0001 (baseline), 0002
-- (users.deleted_at) and 0003 (password_resets), as SQLAlchemy emits them for SQLite
-- (server/openswx_collab_server/db.py, server/openswx_collab_server/migrations/versions/).

CREATE TABLE meta (
	"key" VARCHAR(64) NOT NULL,
	value TEXT NOT NULL,
	PRIMARY KEY ("key")
);

CREATE TABLE oauth_states (
	state_hash VARCHAR(64) NOT NULL,
	provider VARCHAR(32) NOT NULL,
	redirect_uri TEXT NOT NULL,
	invite TEXT,
	challenge VARCHAR(64) NOT NULL,
	link_user_id VARCHAR(32),
	expires_at FLOAT NOT NULL,
	PRIMARY KEY (state_hash)
);

CREATE TABLE orgs (
	id VARCHAR(32) NOT NULL,
	name VARCHAR(200) NOT NULL,
	created_at FLOAT NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE users (
	id VARCHAR(32) NOT NULL,
	email VARCHAR(320) NOT NULL,
	name VARCHAR(200) NOT NULL,
	password_hash TEXT,
	is_server_admin BOOLEAN NOT NULL,
	disabled BOOLEAN NOT NULL,
	email_verified BOOLEAN NOT NULL,
	created_at FLOAT NOT NULL,
	deleted_at FLOAT,
	PRIMARY KEY (id)
);
CREATE UNIQUE INDEX ix_users_email ON users (email);

CREATE TABLE invites (
	id VARCHAR(32) NOT NULL,
	token_hash VARCHAR(64) NOT NULL,
	org_id VARCHAR(32) NOT NULL,
	email VARCHAR(320),
	role VARCHAR(16) NOT NULL,
	projects JSON NOT NULL,
	created_by VARCHAR(32) NOT NULL,
	created_at FLOAT NOT NULL,
	expires_at FLOAT NOT NULL,
	accepted_by VARCHAR(32),
	PRIMARY KEY (id),
	UNIQUE (token_hash),
	FOREIGN KEY(org_id) REFERENCES orgs (id) ON DELETE CASCADE
);
CREATE INDEX ix_invites_org_id ON invites (org_id);

CREATE TABLE login_codes (
	code_hash VARCHAR(64) NOT NULL,
	user_id VARCHAR(32) NOT NULL,
	challenge VARCHAR(64) NOT NULL,
	link_provider VARCHAR(32),
	link_subject VARCHAR(255),
	link_email VARCHAR(320),
	expires_at FLOAT NOT NULL,
	PRIMARY KEY (code_hash),
	FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE
);

CREATE TABLE oauth_identities (
	provider VARCHAR(32) NOT NULL,
	subject VARCHAR(255) NOT NULL,
	user_id VARCHAR(32) NOT NULL,
	PRIMARY KEY (provider, subject),
	FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX ix_oauth_identities_user_id ON oauth_identities (user_id);

CREATE TABLE org_members (
	org_id VARCHAR(32) NOT NULL,
	user_id VARCHAR(32) NOT NULL,
	role VARCHAR(16) NOT NULL,
	created_at FLOAT NOT NULL,
	PRIMARY KEY (org_id, user_id),
	FOREIGN KEY(org_id) REFERENCES orgs (id) ON DELETE CASCADE,
	FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX ix_org_members_user_id ON org_members (user_id);

CREATE TABLE projects (
	id VARCHAR(32) NOT NULL,
	org_id VARCHAR(32) NOT NULL,
	name VARCHAR(200) NOT NULL,
	created_at FLOAT NOT NULL,
	PRIMARY KEY (id),
	FOREIGN KEY(org_id) REFERENCES orgs (id) ON DELETE CASCADE
);
CREATE INDEX ix_projects_org_id ON projects (org_id);

CREATE TABLE sessions (
	token_hash VARCHAR(64) NOT NULL,
	user_id VARCHAR(32) NOT NULL,
	created_at FLOAT NOT NULL,
	expires_at FLOAT NOT NULL,
	label VARCHAR(200) NOT NULL,
	PRIMARY KEY (token_hash),
	FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX ix_sessions_user_id ON sessions (user_id);

CREATE TABLE documents (
	id VARCHAR(32) NOT NULL,
	project_id VARCHAR(32) NOT NULL,
	name VARCHAR(200) NOT NULL,
	kind VARCHAR(16) NOT NULL,
	format_version INTEGER,
	head_seq INTEGER NOT NULL,
	created_by VARCHAR(32) NOT NULL,
	created_at FLOAT NOT NULL,
	updated_at FLOAT NOT NULL,
	deleted_at FLOAT,
	PRIMARY KEY (id),
	FOREIGN KEY(project_id) REFERENCES projects (id) ON DELETE CASCADE
);
CREATE INDEX ix_documents_project_id ON documents (project_id);

CREATE TABLE project_members (
	project_id VARCHAR(32) NOT NULL,
	user_id VARCHAR(32) NOT NULL,
	role VARCHAR(16) NOT NULL,
	PRIMARY KEY (project_id, user_id),
	FOREIGN KEY(project_id) REFERENCES projects (id) ON DELETE CASCADE,
	FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX ix_project_members_user_id ON project_members (user_id);

CREATE TABLE assembly_refs (
	assembly_id VARCHAR(32) NOT NULL,
	component_id VARCHAR(200) NOT NULL,
	part_id VARCHAR(32) NOT NULL,
	seq INTEGER NOT NULL,
	PRIMARY KEY (assembly_id, component_id),
	FOREIGN KEY(assembly_id) REFERENCES documents (id) ON DELETE CASCADE
);
CREATE INDEX ix_assembly_refs_part_id ON assembly_refs (part_id);

CREATE TABLE ops (
	document_id VARCHAR(32) NOT NULL,
	seq INTEGER NOT NULL,
	author_id VARCHAR(32) NOT NULL,
	client_id VARCHAR(128),
	change_id VARCHAR(64),
	created_at FLOAT NOT NULL,
	command VARCHAR(100) NOT NULL,
	args JSON,
	ops JSON NOT NULL,
	inverse_ops JSON NOT NULL,
	targets JSON NOT NULL,
	reverts INTEGER,
	reverted_by INTEGER,
	broke JSON,
	PRIMARY KEY (document_id, seq),
	CONSTRAINT uq_op_change UNIQUE (document_id, client_id, change_id),
	FOREIGN KEY(document_id) REFERENCES documents (id) ON DELETE CASCADE
);
CREATE INDEX ix_ops_doc_seq ON ops (document_id, seq);

CREATE TABLE snapshots (
	document_id VARCHAR(32) NOT NULL,
	seq INTEGER NOT NULL,
	"key" TEXT NOT NULL,
	size INTEGER NOT NULL,
	created_at FLOAT NOT NULL,
	PRIMARY KEY (document_id, seq),
	FOREIGN KEY(document_id) REFERENCES documents (id) ON DELETE CASCADE
);

CREATE TABLE versions (
	id VARCHAR(32) NOT NULL,
	document_id VARCHAR(32) NOT NULL,
	seq INTEGER NOT NULL,
	name VARCHAR(200) NOT NULL,
	note TEXT NOT NULL,
	author_id VARCHAR(32) NOT NULL,
	created_at FLOAT NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT uq_version_name UNIQUE (document_id, name),
	FOREIGN KEY(document_id) REFERENCES documents (id) ON DELETE CASCADE
);
CREATE INDEX ix_versions_document_id ON versions (document_id);

CREATE TABLE password_resets (
	token_hash VARCHAR(64) NOT NULL,
	user_id VARCHAR(32) NOT NULL,
	expires_at FLOAT NOT NULL,
	PRIMARY KEY (token_hash),
	FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX ix_password_resets_user_id ON password_resets (user_id);

CREATE TABLE alembic_version (
	version_num VARCHAR(32) NOT NULL,
	CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);
INSERT INTO alembic_version (version_num) VALUES ('0003');
