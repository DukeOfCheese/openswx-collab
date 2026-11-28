//! Python-shaped source data for the importer's tests: a SQLite database with the Python server's
//! schema at Alembic revision 0003 and a storage folder, filled the way the Python server fills them
//! (`server/openswx_collab_server/documents.py`, `hub.py`, `storage.py`), without running Python.
//!
//! Ids are `uuid4().hex` (32 lower-case hex digits) and times are float epoch seconds.
#![allow(dead_code)] // each test binary uses some of it

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;

const SCHEMA: &str = include_str!("python_schema.sql");

/// A Python server's data: its database (as an `sqlx` URL) and its storage folder.
pub struct PySource {
    pub db_url: String,
    pub storage: PathBuf,
    _dir: TempDir,
}

/// One row of the `ops` table (the columns a test chooses; the rest follow the server's rules).
pub struct PyOp {
    pub author: String,
    pub client_id: Option<String>,
    pub change_id: Option<String>,
    pub command: String,
    pub ops: Value,
    pub inverse_ops: Value,
    pub reverts: Option<i64>,
}

/// An empty Python-schema database at revision `0003` and an empty storage folder.
pub async fn py_source() -> PySource {
    let dir = TempDir::new().unwrap();
    let storage = dir.path().join("storage");
    std::fs::create_dir_all(&storage).unwrap();
    let db_path = dir.path().join("collab.db");
    let src = PySource {
        db_url: format!("sqlite://{}", db_path.to_string_lossy().replace('\\', "/")),
        storage,
        _dir: dir,
    };
    let options = SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    // `raw_sql` runs the whole script (several statements) in one go.
    sqlx::raw_sql(SCHEMA).execute(&pool).await.unwrap();
    pool.close().await;
    src
}

fn new_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    // Not random, but unique and 32 hex digits like `uuid4().hex`.
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut h = Sha256::new();
    h.update(t.to_le_bytes());
    h.update(n.to_le_bytes());
    h.update(std::process::id().to_le_bytes());
    hex::encode(&h.finalize()[..16])
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// `hub.py: pack`: gzip level 6 of compact JSON (`serde_json` writes compact JSON).
fn pack(content: &Value) -> Vec<u8> {
    let mut gz = GzEncoder::new(Vec::new(), Compression::new(6));
    gz.write_all(content.to_string().as_bytes()).unwrap();
    gz.finish().unwrap()
}

/// `storage.py: snapshot_key`.
fn snapshot_key(document: &str, seq: i64) -> String {
    format!("docs/{document}/snapshots/{seq:010}.json.gz")
}

fn put(storage: &Path, key: &str, data: &[u8]) {
    let path = storage.join(key);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, data).unwrap();
}

impl PySource {
    /// A connection to the database, for assertions and extra rows.
    pub async fn pool(&self) -> SqlitePool {
        let options = self
            .db_url
            .parse::<SqliteConnectOptions>()
            .unwrap()
            .foreign_keys(true);
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap()
    }

    /// A user with a lower-case address; returns its id.
    pub async fn user(&self, email: &str) -> String {
        let id = new_id();
        let pool = self.pool().await;
        sqlx::query(
            "INSERT INTO users (id, email, name, password_hash, is_server_admin, disabled, \
             email_verified, created_at, deleted_at) VALUES ($1, $2, $3, NULL, 0, 0, 0, $4, NULL)",
        )
        .bind(&id)
        .bind(email.to_lowercase())
        .bind(email.split('@').next().unwrap())
        .bind(now())
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        id
    }

    /// An organisation with `owner` as its owner; returns its id.
    pub async fn org(&self, name: &str, owner: &str) -> String {
        let id = new_id();
        let pool = self.pool().await;
        let t = now();
        sqlx::query("INSERT INTO orgs (id, name, created_at) VALUES ($1, $2, $3)")
            .bind(&id)
            .bind(name)
            .bind(t)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO org_members (org_id, user_id, role, created_at) VALUES ($1, $2, 'owner', $3)",
        )
        .bind(&id)
        .bind(owner)
        .bind(t)
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        id
    }

    pub async fn project(&self, org: &str, name: &str) -> String {
        let id = new_id();
        let pool = self.pool().await;
        sqlx::query("INSERT INTO projects (id, org_id, name, created_at) VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(org)
            .bind(name)
            .bind(now())
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        id
    }

    /// A document as `documents.py: create_document` makes it: the row (head 0), the seq-0 snapshot
    /// file and its row. Its creator is the earliest member of the project's organisation.
    pub async fn document(&self, project: &str, kind: &str, content: Value) -> String {
        let id = new_id();
        let pool = self.pool().await;
        let creator: String = sqlx::query_scalar(
            "SELECT m.user_id FROM org_members m JOIN projects p ON p.org_id = m.org_id \
             WHERE p.id = $1 ORDER BY m.created_at LIMIT 1",
        )
        .bind(project)
        .fetch_one(&pool)
        .await
        .unwrap();
        let name = content
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("Untitled")
            .to_string();
        let format_version = content.get("version").and_then(Value::as_i64);
        let t = now();
        sqlx::query(
            "INSERT INTO documents (id, project_id, name, kind, format_version, head_seq, created_by, \
             created_at, updated_at, deleted_at) VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $7, NULL)",
        )
        .bind(&id)
        .bind(project)
        .bind(name)
        .bind(kind)
        .bind(format_version)
        .bind(creator)
        .bind(t)
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        self.snapshot(&id, 0, &content).await;
        id
    }

    /// Appends an `ops` row at head + 1 and moves `head_seq` (and `updated_at`) with it, in one
    /// transaction, as `hub.py` does; returns the new seq.
    pub async fn op(&self, doc: &str, row: PyOp) -> i64 {
        let pool = self.pool().await;
        let mut tx = pool.begin().await.unwrap();
        let head: i64 = sqlx::query_scalar("SELECT head_seq FROM documents WHERE id = $1")
            .bind(doc)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let seq = head + 1;
        let t = now();
        sqlx::query(
            "INSERT INTO ops (document_id, seq, author_id, client_id, change_id, created_at, command, \
             args, ops, inverse_ops, targets, reverts, reverted_by, broke) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, $9, '[]', $10, NULL, NULL)",
        )
        .bind(doc)
        .bind(seq)
        .bind(&row.author)
        .bind(&row.client_id)
        .bind(&row.change_id)
        .bind(t)
        .bind(&row.command)
        .bind(row.ops.to_string())
        .bind(row.inverse_ops.to_string())
        .bind(row.reverts)
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query("UPDATE documents SET head_seq = $1, updated_at = $2 WHERE id = $3")
            .bind(seq)
            .bind(t)
            .bind(doc)
            .execute(&mut *tx)
            .await
            .unwrap();
        if let Some(undone) = row.reverts {
            sqlx::query("UPDATE ops SET reverted_by = $1 WHERE document_id = $2 AND seq = $3")
                .bind(seq)
                .bind(doc)
                .bind(undone)
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        pool.close().await;
        seq
    }

    /// A snapshot of the document's state at `seq`: the file and its row.
    pub async fn snapshot(&self, doc: &str, seq: i64, content: &Value) {
        let key = snapshot_key(doc, seq);
        let data = pack(content);
        put(&self.storage, &key, &data);
        let pool = self.pool().await;
        sqlx::query(
            "INSERT INTO snapshots (document_id, seq, key, size, created_at) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(doc)
        .bind(seq)
        .bind(key)
        .bind(data.len() as i64)
        .bind(now())
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
    }

    /// A named version of the document at `seq`, made by the document's creator.
    pub async fn version(&self, doc: &str, seq: i64, name: &str) {
        let pool = self.pool().await;
        let author: String = sqlx::query_scalar("SELECT created_by FROM documents WHERE id = $1")
            .bind(doc)
            .fetch_one(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO versions (id, document_id, seq, name, note, author_id, created_at) \
             VALUES ($1, $2, $3, $4, '', $5, $6)",
        )
        .bind(new_id())
        .bind(doc)
        .bind(seq)
        .bind(name)
        .bind(author)
        .bind(now())
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
    }

    /// Stores `bytes` as an organisation's blob (`storage.py: blob_key`); returns its SHA-256 in
    /// lower-case hex.
    pub fn blob(&self, org: &str, bytes: &[u8]) -> String {
        let sha = hex::encode(Sha256::digest(bytes));
        put(
            &self.storage,
            &format!("orgs/{org}/blobs/{}/{sha}", &sha[..2]),
            bytes,
        );
        sha
    }
}
