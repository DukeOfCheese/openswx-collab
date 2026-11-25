//! The SQLite store: opening, the schema and its migrations, constraints, transactions and
//! concurrency.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::FlakyStore;
use oswx_server::store::{
    PasswordReset, SchemaError, SchemaStatus, Session, SqliteStore, Store, Tx as _, User, new_id,
};
use sqlx::migrate::Migrator;
use tempfile::TempDir;

fn url_in(dir: &TempDir, name: &str) -> String {
    format!("sqlite://{}/{name}", slashes(dir.path()))
}

fn slashes(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

async fn open(url: &str) -> SqliteStore {
    SqliteStore::open(url, true).await.expect("the store opens")
}

fn schema_error(result: anyhow::Result<SqliteStore>) -> SchemaError {
    match result {
        Ok(_) => panic!("the database was accepted"),
        Err(e) => e
            .downcast::<SchemaError>()
            .unwrap_or_else(|e| panic!("not a schema error: {e:#}")),
    }
}

/// Runs one statement in its own transaction.
async fn exec(store: &SqliteStore, statement: &str) -> anyhow::Result<()> {
    let mut tx = store.begin_tx().await?;
    sqlx::query(statement).execute(tx.conn()).await?;
    Box::new(tx).commit().await
}

async fn count(store: &SqliteStore, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(&mut *store.reader().await.unwrap())
        .await
        .unwrap()
}

async fn applied(store: &SqliteStore) -> Vec<i64> {
    sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
        .fetch_all(&mut *store.reader().await.unwrap())
        .await
        .unwrap()
}

const CURRENT: SchemaStatus = SchemaStatus {
    current: Some(1),
    latest: 1,
    pending: Vec::new(),
    unknown: Vec::new(),
};

#[tokio::test]
async fn an_empty_file_is_built_in_a_folder_that_did_not_exist() {
    let dir = TempDir::new().unwrap();
    let url = format!("sqlite:{}/a/b/collab.db", slashes(dir.path()));
    let store = open(&url).await;
    assert_eq!(store.schema_status().await.unwrap(), CURRENT);
    assert_eq!(count(&store, "users").await, 0);
    assert!(dir.path().join("a/b/collab.db").is_file());
}

#[tokio::test]
async fn the_python_url_form_is_accepted() {
    let dir = TempDir::new().unwrap();
    // An absolute path: `sqlite+aiosqlite:////tmp/…` on Unix, `sqlite+aiosqlite:///C:/…` on Windows.
    let url = format!("sqlite+aiosqlite:///{}/py/collab.db", slashes(dir.path()));
    let store = open(&url).await;
    assert_eq!(store.schema_status().await.unwrap(), CURRENT);
    assert!(dir.path().join("py/collab.db").is_file());
}

#[tokio::test]
async fn reopening_is_a_no_op_and_keeps_the_data() {
    let dir = TempDir::new().unwrap();
    let url = url_in(&dir, "collab.db");
    let store = open(&url).await;
    exec(
        &store,
        "INSERT INTO orgs (id, name, created_at) VALUES ('o1', 'Org', 1.5)",
    )
    .await
    .unwrap();
    store.close().await;

    for auto_migrate in [true, false] {
        let store = SqliteStore::open(&url, auto_migrate).await.unwrap();
        assert_eq!(store.schema_status().await.unwrap(), CURRENT);
        assert_eq!(applied(&store).await, vec![1]);
        assert_eq!(count(&store, "orgs").await, 1);
        store.close().await;
    }
}

#[tokio::test]
async fn auto_migrate_off_still_builds_an_empty_file() {
    let dir = TempDir::new().unwrap();
    let store = SqliteStore::open(&url_in(&dir, "collab.db"), false)
        .await
        .unwrap();
    assert_eq!(store.schema_status().await.unwrap(), CURRENT);
}

/// Migrations of a "newer binary": the real ones plus `0002_extra.sql`.
async fn newer_migrator(dir: &TempDir) -> &'static Migrator {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/sqlite/0001_init.sql");
    let to = dir.path().join("migrations");
    std::fs::create_dir_all(&to).unwrap();
    std::fs::copy(src, to.join("0001_init.sql")).unwrap();
    std::fs::write(
        to.join("0002_extra.sql"),
        "CREATE TABLE extra (id BIGINT);\n",
    )
    .unwrap();
    Box::leak(Box::new(Migrator::new(to).await.unwrap()))
}

#[tokio::test]
async fn an_older_schema_is_upgraded_only_when_auto_migrate_is_on() {
    let dir = TempDir::new().unwrap();
    let url = url_in(&dir, "collab.db");
    open(&url).await.close().await;
    let newer = newer_migrator(&dir).await;

    let refused = SqliteStore::open_with_migrations(&url, false, newer).await;
    let err = schema_error(refused);
    assert_eq!(
        err,
        SchemaError::Outdated {
            current: Some(1),
            latest: 2
        }
    );
    assert!(err.to_string().contains("migrate"), "{err}");

    let store = SqliteStore::open_with_migrations(&url, true, newer)
        .await
        .unwrap();
    assert_eq!(
        store.schema_status().await.unwrap(),
        SchemaStatus {
            current: Some(2),
            latest: 2,
            pending: vec![],
            unknown: vec![],
        }
    );
    assert_eq!(count(&store, "extra").await, 0);
    store.close().await;

    // Now this binary (which knows only 0001) meets a database written by the newer one.
    let err = schema_error(SqliteStore::open(&url, true).await);
    assert_eq!(
        err,
        SchemaError::Newer {
            unknown: vec![2],
            latest: 1
        }
    );
    assert!(err.to_string().contains("newer version"), "{err}");
}

#[tokio::test]
async fn connect_then_migrate_builds_the_schema() {
    // What the `migrate` subcommand does: connect without looking at the schema, then migrate.
    let dir = TempDir::new().unwrap();
    let store = SqliteStore::connect(&url_in(&dir, "collab.db"))
        .await
        .unwrap();
    assert_eq!(
        store.schema_status().await.unwrap(),
        SchemaStatus {
            current: None,
            latest: 1,
            pending: vec![1],
            unknown: vec![],
        }
    );
    assert_eq!(store.migrate().await.unwrap(), CURRENT);
    assert_eq!(store.migrate().await.unwrap(), CURRENT);
    assert_eq!(applied(&store).await, vec![1]);
}

#[tokio::test]
async fn a_migration_version_this_binary_does_not_know_is_refused() {
    let dir = TempDir::new().unwrap();
    let url = url_in(&dir, "collab.db");
    let store = open(&url).await;
    exec(
        &store,
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES (9999, 'from the future', TRUE, x'00', 0)",
    )
    .await
    .unwrap();
    assert_eq!(store.schema_status().await.unwrap().unknown, vec![9999]);
    let err = store.migrate().await.unwrap_err().downcast::<SchemaError>();
    assert!(matches!(err, Ok(SchemaError::Newer { .. })), "{err:?}");
    store.close().await;

    for auto_migrate in [true, false] {
        let err = schema_error(SqliteStore::open(&url, auto_migrate).await);
        assert_eq!(
            err,
            SchemaError::Newer {
                unknown: vec![9999],
                latest: 1
            }
        );
    }
}

#[tokio::test]
async fn a_database_with_tables_but_no_migration_record_is_refused() {
    let dir = TempDir::new().unwrap();
    let url = url_in(&dir, "python.db");
    // A Python server's database: its own tables and `alembic_version`, no `_sqlx_migrations`.
    let raw = SqliteStore::connect(&url).await.unwrap();
    exec(
        &raw,
        "CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)",
    )
    .await
    .unwrap();
    raw.close().await;
    assert_eq!(
        schema_error(SqliteStore::open(&url, true).await),
        SchemaError::Foreign
    );
}

#[tokio::test]
async fn the_connection_settings_are_wal_foreign_keys_and_a_30_second_busy_timeout() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    let mut reader = store.reader().await.unwrap();
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut *reader)
        .await
        .unwrap();
    assert_eq!(mode, "wal");
    let fk: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut *reader)
        .await
        .unwrap();
    let busy: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(&mut *reader)
        .await
        .unwrap();
    assert_eq!((fk, busy), (1, 30_000), "reader");
    // A reader cannot write, so it can never hold the write lock.
    let err = sqlx::query("INSERT INTO orgs (id, name, created_at) VALUES ('o', 'O', 0)")
        .execute(&mut *reader)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("readonly"), "{err}");
    drop(reader);

    let mut tx = store.begin_tx().await.unwrap();
    let fk: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(tx.conn())
        .await
        .unwrap();
    let busy: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(tx.conn())
        .await
        .unwrap();
    assert_eq!((fk, busy), (1, 30_000), "writer");
}

/// One row in every table that hangs off an organisation.
async fn fill(store: &SqliteStore) {
    for statement in [
        "INSERT INTO users (id, email, name, created_at) VALUES ('u1', 'a@example.com', 'A', 0)",
        "INSERT INTO orgs (id, name, created_at) VALUES ('o1', 'Org', 0)",
        "INSERT INTO org_members (org_id, user_id, role, created_at) VALUES ('o1', 'u1', 'owner', 0)",
        "INSERT INTO invites (id, token_hash, org_id, created_by, created_at, expires_at) \
         VALUES ('i1', 'h1', 'o1', 'u1', 0, 1)",
        "INSERT INTO projects (id, org_id, name, created_at) VALUES ('p1', 'o1', 'P', 0)",
        "INSERT INTO project_members (project_id, user_id, role) VALUES ('p1', 'u1', 'owner')",
        "INSERT INTO folders (id, project_id, name, created_at) VALUES ('f1', 'p1', 'F', 0)",
        "INSERT INTO folders (id, project_id, parent_id, name, created_at) \
         VALUES ('f2', 'p1', 'f1', 'Sub', 0)",
        "INSERT INTO documents (id, project_id, folder_id, name, created_by, created_at, updated_at) \
         VALUES ('d1', 'p1', 'f2', 'Part', 'u1', 0, 0)",
        "INSERT INTO documents (id, project_id, name, kind, created_by, created_at, updated_at) \
         VALUES ('d2', 'p1', 'Asm', 'assembly', 'u1', 0, 0)",
        "INSERT INTO changes (document_id, seq, parent_seq, epoch, author_id, created_at, ops, \
         inverse, paths, reads) VALUES ('d1', 1, 0, 1, 'u1', 0, x'90', x'90', x'90', x'90')",
        "INSERT INTO snapshots (document_id, epoch, seq, key, created_at) VALUES ('d1', 1, 1, 'k', 0)",
        "INSERT INTO versions (id, document_id, seq, name, author_id, created_at) \
         VALUES ('v1', 'd1', 1, 'V1', 'u1', 0)",
        "INSERT INTO assembly_refs (assembly_id, component_id, part_id, seq) VALUES ('d2', 'c1', 'd1', 1)",
        "INSERT INTO room_leases (document_id, node_id, epoch, expires_at) VALUES ('d1', 'n1', 1, 0)",
    ] {
        exec(store, statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
}

#[tokio::test]
async fn deleting_an_org_deletes_everything_under_it() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    fill(&store).await;
    exec(&store, "DELETE FROM orgs WHERE id = 'o1'")
        .await
        .unwrap();
    for table in [
        "org_members",
        "invites",
        "projects",
        "project_members",
        "folders",
        "documents",
        "changes",
        "snapshots",
        "versions",
        "assembly_refs",
        "room_leases",
    ] {
        assert_eq!(count(&store, table).await, 0, "{table}");
    }
    assert_eq!(count(&store, "users").await, 1);
}

#[tokio::test]
async fn deleting_a_folder_deletes_its_subfolders_and_keeps_their_documents() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    fill(&store).await;
    exec(&store, "DELETE FROM folders WHERE id = 'f1'")
        .await
        .unwrap();
    assert_eq!(count(&store, "folders").await, 0);
    let folder: Option<String> =
        sqlx::query_scalar("SELECT folder_id FROM documents WHERE id = 'd1'")
            .fetch_one(&mut *store.reader().await.unwrap())
            .await
            .unwrap();
    assert_eq!(folder, None);
    assert_eq!(count(&store, "changes").await, 1);
}

#[tokio::test]
async fn a_foreign_key_to_nothing_is_refused() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    let err = exec(
        &store,
        "INSERT INTO projects (id, org_id, name, created_at) VALUES ('p', 'no such org', 'P', 0)",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("FOREIGN KEY"), "{err}");
}

#[tokio::test]
async fn change_ids_are_unique_per_client_and_null_never_collides() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    fill(&store).await;
    let insert = |seq: i64, client: &str, change: &str| {
        format!(
            "INSERT INTO changes (document_id, seq, parent_seq, epoch, author_id, client_id, \
             change_id, created_at, ops, inverse, paths, reads) \
             VALUES ('d1', {seq}, {}, 1, 'u1', {client}, {change}, 0, x'90', x'90', x'90', x'90')",
            seq - 1
        )
    };
    for seq in 2..6 {
        exec(&store, &insert(seq, "'u1.app'", "NULL"))
            .await
            .unwrap();
    }
    exec(&store, &insert(6, "NULL", "NULL")).await.unwrap();
    exec(&store, &insert(7, "NULL", "NULL")).await.unwrap();
    exec(&store, &insert(8, "'u1.app'", "'c1'")).await.unwrap();
    exec(&store, &insert(9, "'u1.other'", "'c1'"))
        .await
        .unwrap();
    let err = exec(&store, &insert(10, "'u1.app'", "'c1'"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("UNIQUE"), "{err}");
    assert_eq!(count(&store, "changes").await, 9);
}

/// A row type as later tasks write them: `sqlx::FromRow`, one field per column.
#[derive(Debug, PartialEq, sqlx::FromRow)]
struct ChangeRow {
    seq: i64,
    client_id: Option<String>,
    created_at: f64,
    ops: Vec<u8>,
}

#[tokio::test]
async fn rows_map_to_plain_structs_and_booleans_read_as_bool() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    fill(&store).await;
    let mut c = store.reader().await.unwrap();
    let row: ChangeRow = sqlx::query_as(
        "SELECT seq, client_id, created_at, ops FROM changes WHERE document_id = $1 AND seq = $2",
    )
    .bind("d1")
    .bind(1_i64)
    .fetch_one(&mut *c)
    .await
    .unwrap();
    assert_eq!(
        row,
        ChangeRow {
            seq: 1,
            client_id: None,
            created_at: 0.0,
            ops: vec![0x90],
        }
    );
    let flags: (bool, bool, bool) =
        sqlx::query_as("SELECT is_server_admin, disabled, email_verified FROM users")
            .fetch_one(&mut *c)
            .await
            .unwrap();
    assert_eq!(flags, (false, false, false));
}

#[tokio::test]
async fn health_and_user_count_read_the_database() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    store.health().await.unwrap();
    assert_eq!(store.user_count().await.unwrap(), 0);
    fill(&store).await;
    // Deleted and disabled accounts count too.
    exec(
        &store,
        "INSERT INTO users (id, email, name, created_at, disabled, deleted_at) \
         VALUES ('u2', 'b@example.com', 'B', 0, TRUE, 5)",
    )
    .await
    .unwrap();
    assert_eq!(store.user_count().await.unwrap(), 2);
    store.close().await;
    assert!(store.health().await.is_err());
}

#[tokio::test]
async fn a_transaction_is_all_or_nothing_and_savepoints_roll_back_alone() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    let org = |id: &str| format!("INSERT INTO orgs (id, name, created_at) VALUES ('{id}', 'O', 0)");

    // Dropped without commit: nothing.
    {
        let mut tx = store.begin_tx().await.unwrap();
        sqlx::query(&org("dropped"))
            .execute(tx.conn())
            .await
            .unwrap();
    }
    // Rolled back: nothing.
    let mut tx = store.begin_tx().await.unwrap();
    sqlx::query(&org("rolled"))
        .execute(tx.conn())
        .await
        .unwrap();
    Box::new(tx).rollback().await.unwrap();
    assert_eq!(count(&store, "orgs").await, 0);

    let mut tx = store.begin_tx().await.unwrap();
    sqlx::query(&org("a")).execute(tx.conn()).await.unwrap();
    {
        let mut sp = tx.savepoint_tx().await.unwrap();
        sqlx::query(&org("b")).execute(sp.conn()).await.unwrap();
        Box::new(sp).rollback().await.unwrap();
    }
    {
        let mut sp = tx.savepoint_tx().await.unwrap();
        sqlx::query(&org("c")).execute(sp.conn()).await.unwrap();
        // A failing statement inside a savepoint, rolled back, leaves the rest intact.
        assert!(sqlx::query(&org("c")).execute(sp.conn()).await.is_err());
        drop(sp); // dropping rolls back too
    }
    {
        let mut sp = tx.savepoint_tx().await.unwrap();
        sqlx::query(&org("d")).execute(sp.conn()).await.unwrap();
        Box::new(sp).commit().await.unwrap();
    }
    // Uncommitted writes are invisible to readers.
    assert_eq!(count(&store, "orgs").await, 0);
    Box::new(tx).commit().await.unwrap();

    let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM orgs ORDER BY id")
        .fetch_all(&mut *store.reader().await.unwrap())
        .await
        .unwrap();
    assert_eq!(ids, ["a", "d"]);
}

#[tokio::test]
async fn the_trait_transaction_and_savepoint_commit() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn Store> = Arc::new(open(&url_in(&dir, "collab.db")).await);
    let mut tx = store.begin().await.unwrap();
    {
        let sp = tx.savepoint().await.unwrap();
        sp.commit().await.unwrap();
    }
    tx.commit().await.unwrap();
    // The writer is free again.
    store.begin().await.unwrap().rollback().await.unwrap();
}

fn user(id: &str, email: &str) -> User {
    User {
        id: id.into(),
        email: email.into(),
        name: "N".into(),
        password_hash: Some("scrypt$x".into()),
        is_server_admin: false,
        disabled: false,
        email_verified: false,
        created_at: 1.5,
        deleted_at: None,
    }
}

fn session(hash: &str, user_id: &str, expires_at: f64) -> Session {
    Session {
        token_hash: hash.into(),
        user_id: user_id.into(),
        created_at: 1.0,
        expires_at,
        label: "laptop".into(),
    }
}

#[tokio::test]
async fn accounts_are_written_and_read_back() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn Store> = Arc::new(open(&url_in(&dir, "collab.db")).await);
    let ada = user(&new_id(), "ada@example.com");
    let mut tx = store.begin().await.unwrap();
    tx.insert_user(&ada).await.unwrap();
    assert_eq!(
        tx.user_count().await.unwrap(),
        1,
        "the transaction sees its own write"
    );
    assert_eq!(
        tx.user_by_email("ada@example.com").await.unwrap(),
        Some(ada.clone())
    );
    assert!(
        store.user_by_id(&ada.id).await.unwrap().is_none(),
        "not committed yet"
    );
    tx.commit().await.unwrap();
    assert_eq!(store.user_by_id(&ada.id).await.unwrap(), Some(ada.clone()));
    assert_eq!(
        store.user_by_email("ada@example.com").await.unwrap(),
        Some(ada.clone())
    );
    assert!(
        store
            .user_by_email("ADA@example.com")
            .await
            .unwrap()
            .is_none()
    );

    // The address is unique.
    let mut tx = store.begin().await.unwrap();
    assert!(
        tx.insert_user(&user(&new_id(), "ada@example.com"))
            .await
            .is_err()
    );
    drop(tx);

    let mut tx = store.begin().await.unwrap();
    tx.set_user_name(&ada.id, "Ada L").await.unwrap();
    tx.set_user_password(&ada.id, None).await.unwrap();
    tx.set_user_email_verified(&ada.id, true).await.unwrap();
    tx.set_user_disabled(&ada.id, true).await.unwrap();
    tx.commit().await.unwrap();
    let got = store.user_by_id(&ada.id).await.unwrap().unwrap();
    assert_eq!(
        got,
        User {
            name: "Ada L".into(),
            password_hash: None,
            email_verified: true,
            disabled: true,
            ..ada.clone()
        }
    );
    assert!(!got.active());
}

#[tokio::test]
async fn sessions_and_password_resets_are_written_read_back_and_deleted() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn Store> = Arc::new(open(&url_in(&dir, "collab.db")).await);
    let (a, b) = (new_id(), new_id());
    let mut tx = store.begin().await.unwrap();
    tx.insert_user(&user(&a, "a@example.com")).await.unwrap();
    tx.insert_user(&user(&b, "b@example.com")).await.unwrap();
    for (hash, owner, expires) in [
        ("s1", &a, 10.0),
        ("s2", &a, 100.0),
        ("s3", &a, 100.0),
        ("s4", &b, 5.0),
    ] {
        tx.insert_session(&session(hash, owner, expires))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(
        store.session_by_hash("s2").await.unwrap(),
        Some(session("s2", &a, 100.0))
    );

    let mut tx = store.begin().await.unwrap();
    tx.delete_expired_user_sessions(&a, 50.0).await.unwrap(); // s1
    tx.delete_session("s3").await.unwrap();
    tx.commit().await.unwrap();
    for (hash, kept) in [("s1", false), ("s2", true), ("s3", false), ("s4", true)] {
        assert_eq!(
            store.session_by_hash(hash).await.unwrap().is_some(),
            kept,
            "{hash}"
        );
    }
    let mut tx = store.begin().await.unwrap();
    tx.insert_session(&session("s5", &a, 100.0)).await.unwrap();
    tx.delete_other_user_sessions(&a, "s5").await.unwrap();
    tx.commit().await.unwrap();
    assert!(store.session_by_hash("s2").await.unwrap().is_none());
    assert!(store.session_by_hash("s5").await.unwrap().is_some());
    let mut tx = store.begin().await.unwrap();
    tx.delete_user_sessions(&a).await.unwrap();
    tx.commit().await.unwrap();
    assert!(store.session_by_hash("s5").await.unwrap().is_none());
    assert!(
        store.session_by_hash("s4").await.unwrap().is_some(),
        "only a's"
    );

    let reset = |hash: &str, owner: &str, expires_at: f64| PasswordReset {
        token_hash: hash.into(),
        user_id: owner.into(),
        expires_at,
    };
    let mut tx = store.begin().await.unwrap();
    tx.insert_password_reset(&reset("r1", &a, 10.0))
        .await
        .unwrap();
    tx.insert_password_reset(&reset("r2", &a, 100.0))
        .await
        .unwrap();
    tx.insert_password_reset(&reset("r3", &b, 10.0))
        .await
        .unwrap();
    tx.insert_password_reset(&reset("r4", &b, 100.0))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        store.password_reset_by_hash("r2").await.unwrap(),
        Some(reset("r2", &a, 100.0))
    );
    let mut tx = store.begin().await.unwrap();
    assert_eq!(
        tx.take_password_reset("r2").await.unwrap(),
        Some(reset("r2", &a, 100.0))
    );
    assert_eq!(
        tx.take_password_reset("r2").await.unwrap(),
        None,
        "a reset is taken once"
    );
    tx.delete_expired_password_resets(50.0).await.unwrap(); // r1, r3
    tx.commit().await.unwrap();
    for (hash, kept) in [("r1", false), ("r2", false), ("r3", false), ("r4", true)] {
        assert_eq!(
            store.password_reset_by_hash(hash).await.unwrap().is_some(),
            kept,
            "{hash}"
        );
    }
    let mut tx = store.begin().await.unwrap();
    tx.delete_user_password_resets(&b).await.unwrap();
    tx.commit().await.unwrap();
    assert!(store.password_reset_by_hash("r4").await.unwrap().is_none());
}

#[tokio::test]
async fn two_level_savepoints_through_the_trait() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    let org = |id: &str| format!("INSERT INTO orgs (id, name, created_at) VALUES ('{id}', 'O', 0)");

    // With data: the writes go through each level's connection, commit and rollback through
    // `Box<dyn Tx>`.
    let mut tx = store.begin_tx().await.unwrap();
    sqlx::query(&org("top")).execute(tx.conn()).await.unwrap();
    {
        let mut outer = tx.savepoint_tx().await.unwrap();
        sqlx::query(&org("outer"))
            .execute(outer.conn())
            .await
            .unwrap();
        {
            let mut inner = outer.savepoint_tx().await.unwrap();
            sqlx::query(&org("inner"))
                .execute(inner.conn())
                .await
                .unwrap();
            let inner: Box<dyn oswx_server::store::Tx + '_> = Box::new(inner);
            inner.rollback().await.unwrap();
        }
        sqlx::query(&org("outer2"))
            .execute(outer.conn())
            .await
            .unwrap();
        let outer: Box<dyn oswx_server::store::Tx + '_> = Box::new(outer);
        outer.commit().await.unwrap();
    }
    let tx: Box<dyn oswx_server::store::Tx> = Box::new(tx);
    tx.commit().await.unwrap();
    let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM orgs ORDER BY id")
        .fetch_all(&mut *store.reader().await.unwrap())
        .await
        .unwrap();
    assert_eq!(ids, ["outer", "outer2", "top"]);

    // Trait objects only, all the way down.
    let store: Arc<dyn Store> = Arc::new(store);
    let mut tx = store.begin().await.unwrap();
    {
        let mut outer = tx.savepoint().await.unwrap();
        outer.savepoint().await.unwrap().rollback().await.unwrap();
        outer.commit().await.unwrap();
    }
    tx.commit().await.unwrap();
}

/// Waits at most a second: the guard answers at once, a self-wait would take 30 s.
async fn at_once<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(1), f)
        .await
        .expect("answered at once")
}

#[tokio::test]
async fn begin_while_this_task_holds_a_transaction_fails_at_once() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    let shared: Arc<dyn Store> = Arc::new(store.clone());

    let tx = store.begin_tx().await.unwrap();
    let err = at_once(store.begin_tx()).await.unwrap_err();
    assert!(err.to_string().contains("savepoint()"), "{err}");
    assert!(at_once(shared.begin()).await.is_err());
    assert!(at_once(store.migrate()).await.is_err());
    // Reads do not need the writer.
    at_once(store.user_count()).await.unwrap();

    // However the transaction ends, this task may begin again.
    Box::new(tx).commit().await.unwrap();
    let tx = at_once(store.begin_tx()).await.unwrap();
    Box::new(tx).rollback().await.unwrap();
    let tx = at_once(store.begin_tx()).await.unwrap();
    drop(tx);
    let tx = at_once(shared.begin()).await.unwrap();
    assert!(at_once(shared.begin()).await.is_err());
    tx.commit().await.unwrap();
    at_once(shared.begin())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
}

#[tokio::test]
async fn another_task_waits_for_the_transaction_instead_of_failing() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    let tx = store.begin_tx().await.unwrap();
    let other = store.clone();
    let waiter = tokio::spawn(async move {
        let tx = other.begin_tx().await?;
        Box::new(tx).commit().await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiter.is_finished());
    Box::new(tx).commit().await.unwrap();
    at_once(waiter).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_transaction_moved_to_another_task_hands_the_guard_over() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;

    // Moved and committed there: this task may begin again.
    let tx = store.begin_tx().await.unwrap();
    tokio::spawn(async move { Box::new(tx).commit().await })
        .await
        .unwrap()
        .unwrap();
    Box::new(at_once(store.begin_tx()).await.unwrap())
        .commit()
        .await
        .unwrap();

    // Moved and dropped there: the same.
    let tx = store.begin_tx().await.unwrap();
    tokio::spawn(async move { drop(tx) }).await.unwrap();
    Box::new(at_once(store.begin_tx()).await.unwrap())
        .commit()
        .await
        .unwrap();

    // Used in the other task: that task now holds it, so its own nested begin fails at once.
    let tx = store.begin_tx().await.unwrap();
    let other = store.clone();
    tokio::spawn(async move {
        let mut tx = tx;
        sqlx::query("SELECT 1").execute(tx.conn()).await.unwrap();
        assert!(at_once(other.begin_tx()).await.is_err());
        Box::new(tx).commit().await.unwrap();
    })
    .await
    .unwrap();
    Box::new(at_once(store.begin_tx()).await.unwrap())
        .commit()
        .await
        .unwrap();
}

#[tokio::test]
async fn an_applied_migration_that_was_edited_is_refused_at_open() {
    let dir = TempDir::new().unwrap();
    let url = url_in(&dir, "collab.db");
    open(&url).await.close().await;

    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/sqlite/0001_init.sql");
    let to = dir.path().join("edited");
    std::fs::create_dir_all(&to).unwrap();
    let mut text = std::fs::read_to_string(src).unwrap();
    text.push_str("\n-- edited after release\n");
    std::fs::write(to.join("0001_init.sql"), text).unwrap();
    let edited: &'static Migrator = Box::leak(Box::new(Migrator::new(to).await.unwrap()));

    for auto_migrate in [true, false] {
        let err = schema_error(SqliteStore::open_with_migrations(&url, auto_migrate, edited).await);
        assert_eq!(err, SchemaError::Changed { version: 1 });
        assert!(err.to_string().contains("changed"), "{err}");
    }
}

#[tokio::test]
async fn the_flaky_store_fails_health_and_commits_on_demand() {
    let dir = TempDir::new().unwrap();
    let flaky = FlakyStore::new(Arc::new(open(&url_in(&dir, "collab.db")).await));
    flaky.health().await.unwrap();
    flaky.fail_health(true);
    assert!(flaky.health().await.is_err());

    flaky.fail_commit(true);
    let tx = flaky.begin().await.unwrap();
    assert!(tx.commit().await.is_err());
    flaky.fail_commit(false);
    // The failed commit released the writer.
    flaky.begin().await.unwrap().commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_concurrent_writers_and_readers_never_see_database_is_locked() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    let mut tasks = Vec::new();
    for task in 0..20 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            for round in 0..10 {
                if task % 2 == 0 {
                    // A writer that holds its transaction across an await, as handlers will.
                    let mut tx = store.begin_tx().await?;
                    let id = new_id();
                    sqlx::query(
                        "INSERT INTO users (id, email, name, created_at) VALUES ($1, $2, $3, $4)",
                    )
                    .bind(&id)
                    .bind(format!("{id}@example.com"))
                    .bind(format!("writer {task} round {round}"))
                    .bind(0.0_f64)
                    .execute(tx.conn())
                    .await?;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    Box::new(tx).commit().await?;
                } else {
                    store.user_count().await?;
                    store.health().await?;
                    store.begin().await?.commit().await?;
                }
            }
            anyhow::Ok(())
        }));
    }
    for task in tasks {
        if let Err(e) = task.await.unwrap() {
            panic!("a task failed: {e:#}");
        }
    }
    assert_eq!(store.user_count().await.unwrap(), 100);
}

#[test]
fn new_ids_are_32_lower_case_hex_characters() {
    let a = new_id();
    assert_eq!(a.len(), 32);
    assert!(
        a.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
    assert_ne!(a, new_id());
}

#[test]
fn schema_status_compares_applied_and_known_versions() {
    assert_eq!(
        SchemaStatus::compare(&[], &[1, 2]),
        SchemaStatus {
            current: None,
            latest: 2,
            pending: vec![1, 2],
            unknown: vec![],
        }
    );
    let s = SchemaStatus::compare(&[1, 3], &[1, 2]);
    assert_eq!(
        (s.current, s.pending, s.unknown),
        (Some(3), vec![2], vec![3])
    );
    assert!(SchemaStatus::compare(&[1, 2], &[1, 2]).is_current());
}

/// Every table, column, index and foreign key, so that the schema cannot drift unnoticed. Update
/// this together with a new migration.
#[tokio::test]
async fn the_schema_is_exactly_as_expected() {
    let dir = TempDir::new().unwrap();
    let store = open(&url_in(&dir, "collab.db")).await;
    let mut c = store.reader().await.unwrap();
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(&mut *c)
    .await
    .unwrap();

    let mut columns = Vec::new();
    let mut indexes = Vec::new();
    let mut keys = Vec::new();
    for table in tables.iter().filter(|t| *t != "_sqlx_migrations") {
        let rows: Vec<(String, String, bool, Option<String>, i64)> = sqlx::query_as(
            "SELECT name, type, \"notnull\", dflt_value, pk FROM pragma_table_info($1) ORDER BY cid",
        )
        .bind(table)
        .fetch_all(&mut *c)
        .await
        .unwrap();
        for (name, ty, not_null, default, pk) in rows {
            let mut line = format!("{table}.{name} {ty}");
            if not_null {
                line.push_str(" NN");
            }
            if let Some(d) = default {
                line.push_str(&format!(" ={d}"));
            }
            if pk > 0 {
                line.push_str(&format!(" pk{pk}"));
            }
            columns.push(line);
        }
        let list: Vec<(String, bool, String)> =
            sqlx::query_as("SELECT name, \"unique\", origin FROM pragma_index_list($1)")
                .bind(table)
                .fetch_all(&mut *c)
                .await
                .unwrap();
        for (name, unique, origin) in list {
            let cols: Vec<String> =
                sqlx::query_scalar("SELECT name FROM pragma_index_info($1) ORDER BY seqno")
                    .bind(&name)
                    .fetch_all(&mut *c)
                    .await
                    .unwrap();
            let label = if origin == "c" { name } else { origin };
            let unique = if unique { "unique " } else { "" };
            indexes.push(format!("{table} {unique}({}) {label}", cols.join(", ")));
        }
        let fks: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT \"from\", \"table\", \"to\", on_delete FROM pragma_foreign_key_list($1)",
        )
        .bind(table)
        .fetch_all(&mut *c)
        .await
        .unwrap();
        for (from, to_table, to, on_delete) in fks {
            keys.push(format!("{table}.{from} -> {to_table}.{to} {on_delete}"));
        }
    }
    indexes.sort();
    keys.sort();

    let expected_tables = [
        "_sqlx_migrations",
        "assembly_refs",
        "changes",
        "documents",
        "folders",
        "invites",
        "login_codes",
        "oauth_identities",
        "oauth_states",
        "org_members",
        "orgs",
        "password_resets",
        "project_members",
        "projects",
        "room_leases",
        "sessions",
        "snapshots",
        "users",
        "versions",
    ];
    assert_eq!(tables, expected_tables);

    let expected_columns = "\
assembly_refs.assembly_id VARCHAR(32) NN pk1
assembly_refs.component_id VARCHAR(200) NN pk2
assembly_refs.part_id VARCHAR(32) NN
assembly_refs.seq BIGINT NN
changes.document_id VARCHAR(32) NN pk1
changes.seq BIGINT NN pk2
changes.parent_seq BIGINT NN
changes.branch_id VARCHAR(64) NN ='main'
changes.epoch BIGINT NN
changes.author_id VARCHAR(32) NN
changes.client_id VARCHAR(128)
changes.change_id VARCHAR(64)
changes.created_at DOUBLE PRECISION NN
changes.command VARCHAR(100) NN =''
changes.ops BLOB NN
changes.inverse BLOB NN
changes.paths BLOB NN
changes.reads BLOB NN
changes.undo_of BIGINT
changes.reverts BIGINT
changes.reverted_by BIGINT
changes.broke TEXT
documents.id VARCHAR(32) NN pk1
documents.project_id VARCHAR(32) NN
documents.folder_id VARCHAR(32)
documents.name VARCHAR(200) NN
documents.kind VARCHAR(16) NN ='part'
documents.format_version BIGINT
documents.head_seq BIGINT NN =0
documents.created_by VARCHAR(32) NN
documents.created_at DOUBLE PRECISION NN
documents.updated_at DOUBLE PRECISION NN
documents.deleted_at DOUBLE PRECISION
folders.id VARCHAR(32) NN pk1
folders.project_id VARCHAR(32) NN
folders.parent_id VARCHAR(32)
folders.name VARCHAR(200) NN
folders.created_at DOUBLE PRECISION NN
invites.id VARCHAR(32) NN pk1
invites.token_hash VARCHAR(64) NN
invites.org_id VARCHAR(32) NN
invites.email VARCHAR(320)
invites.role VARCHAR(16) NN ='member'
invites.projects TEXT NN ='[]'
invites.created_by VARCHAR(32) NN
invites.created_at DOUBLE PRECISION NN
invites.expires_at DOUBLE PRECISION NN
invites.accepted_by VARCHAR(32)
login_codes.code_hash VARCHAR(64) NN pk1
login_codes.user_id VARCHAR(32) NN
login_codes.challenge VARCHAR(64) NN =''
login_codes.link_provider VARCHAR(32)
login_codes.link_subject VARCHAR(255)
login_codes.link_email VARCHAR(320)
login_codes.expires_at DOUBLE PRECISION NN
oauth_identities.provider VARCHAR(32) NN pk1
oauth_identities.subject VARCHAR(255) NN pk2
oauth_identities.user_id VARCHAR(32) NN
oauth_states.state_hash VARCHAR(64) NN pk1
oauth_states.provider VARCHAR(32) NN
oauth_states.redirect_uri TEXT NN
oauth_states.invite TEXT
oauth_states.challenge VARCHAR(64) NN =''
oauth_states.link_user_id VARCHAR(32)
oauth_states.expires_at DOUBLE PRECISION NN
org_members.org_id VARCHAR(32) NN pk1
org_members.user_id VARCHAR(32) NN pk2
org_members.role VARCHAR(16) NN
org_members.created_at DOUBLE PRECISION NN
orgs.id VARCHAR(32) NN pk1
orgs.name VARCHAR(200) NN
orgs.created_at DOUBLE PRECISION NN
password_resets.token_hash VARCHAR(64) NN pk1
password_resets.user_id VARCHAR(32) NN
password_resets.expires_at DOUBLE PRECISION NN
project_members.project_id VARCHAR(32) NN pk1
project_members.user_id VARCHAR(32) NN pk2
project_members.role VARCHAR(16) NN
projects.id VARCHAR(32) NN pk1
projects.org_id VARCHAR(32) NN
projects.name VARCHAR(200) NN
projects.created_at DOUBLE PRECISION NN
room_leases.document_id VARCHAR(32) NN pk1
room_leases.node_id VARCHAR(64) NN
room_leases.epoch BIGINT NN
room_leases.expires_at DOUBLE PRECISION NN
sessions.token_hash VARCHAR(64) NN pk1
sessions.user_id VARCHAR(32) NN
sessions.created_at DOUBLE PRECISION NN
sessions.expires_at DOUBLE PRECISION NN
sessions.label VARCHAR(200) NN =''
snapshots.document_id VARCHAR(32) NN pk1
snapshots.epoch BIGINT NN pk2
snapshots.seq BIGINT NN pk3
snapshots.key TEXT NN
snapshots.size BIGINT NN =0
snapshots.created_at DOUBLE PRECISION NN
users.id VARCHAR(32) NN pk1
users.email VARCHAR(320) NN
users.name VARCHAR(200) NN
users.password_hash TEXT
users.is_server_admin BOOLEAN NN =FALSE
users.disabled BOOLEAN NN =FALSE
users.email_verified BOOLEAN NN =FALSE
users.created_at DOUBLE PRECISION NN
users.deleted_at DOUBLE PRECISION
versions.id VARCHAR(32) NN pk1
versions.document_id VARCHAR(32) NN
versions.seq BIGINT NN
versions.name VARCHAR(200) NN
versions.note TEXT NN =''
versions.author_id VARCHAR(32) NN
versions.created_at DOUBLE PRECISION NN";
    assert_eq!(columns.join("\n"), expected_columns);

    let mut expected_indexes = vec![
        "assembly_refs (part_id) ix_assembly_refs_part_id",
        "assembly_refs unique (assembly_id, component_id) pk",
        "changes (document_id, client_id, seq) ix_changes_client_seq",
        "changes unique (document_id, client_id, change_id) u",
        "changes unique (document_id, seq) pk",
        "documents (folder_id) ix_documents_folder_id",
        "documents (project_id) ix_documents_project_id",
        "documents unique (id) pk",
        "folders (parent_id) ix_folders_parent_id",
        "folders (project_id) ix_folders_project_id",
        "folders unique (id) pk",
        "invites (org_id) ix_invites_org_id",
        "invites unique (id) pk",
        "invites unique (token_hash) u",
        "login_codes unique (code_hash) pk",
        "oauth_identities (user_id) ix_oauth_identities_user_id",
        "oauth_identities unique (provider, subject) pk",
        "oauth_states unique (state_hash) pk",
        "org_members (user_id) ix_org_members_user_id",
        "org_members unique (org_id, user_id) pk",
        "orgs unique (id) pk",
        "password_resets (user_id) ix_password_resets_user_id",
        "password_resets unique (token_hash) pk",
        "project_members (user_id) ix_project_members_user_id",
        "project_members unique (project_id, user_id) pk",
        "projects (org_id) ix_projects_org_id",
        "projects unique (id) pk",
        "room_leases unique (document_id) pk",
        "sessions (user_id) ix_sessions_user_id",
        "sessions unique (token_hash) pk",
        "snapshots unique (document_id, epoch, seq) pk",
        "users unique (email) ix_users_email",
        "users unique (id) pk",
        "versions (document_id) ix_versions_document_id",
        "versions unique (document_id, name) u",
        "versions unique (id) pk",
    ];
    expected_indexes.sort_unstable();
    assert_eq!(indexes, expected_indexes);

    let mut expected_keys = vec![
        "assembly_refs.assembly_id -> documents.id CASCADE",
        "changes.document_id -> documents.id CASCADE",
        "documents.folder_id -> folders.id SET NULL",
        "documents.project_id -> projects.id CASCADE",
        "folders.parent_id -> folders.id CASCADE",
        "folders.project_id -> projects.id CASCADE",
        "invites.org_id -> orgs.id CASCADE",
        "login_codes.user_id -> users.id CASCADE",
        "oauth_identities.user_id -> users.id CASCADE",
        "org_members.org_id -> orgs.id CASCADE",
        "org_members.user_id -> users.id CASCADE",
        "password_resets.user_id -> users.id CASCADE",
        "project_members.project_id -> projects.id CASCADE",
        "project_members.user_id -> users.id CASCADE",
        "projects.org_id -> orgs.id CASCADE",
        "room_leases.document_id -> documents.id CASCADE",
        "sessions.user_id -> users.id CASCADE",
        "snapshots.document_id -> documents.id CASCADE",
        "versions.document_id -> documents.id CASCADE",
    ];
    expected_keys.sort_unstable();
    assert_eq!(keys, expected_keys);
}
