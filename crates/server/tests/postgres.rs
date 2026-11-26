//! The PostgreSQL store itself: its migrations, how it decodes rows, its locks and its errors.
//!
//! Every test here needs a PostgreSQL database (`OSWX_TEST_DATABASE_URL`, which
//! `tools/test_postgres_rust.py` sets) and says so, and passes, when there is none. What the server
//! does on PostgreSQL is tested by the whole suite, which that script runs against it; the SQLite
//! store's own tests are in `store.rs`.

mod common;

use std::path::Path;
use std::time::Duration;

use common::{PgSchema, on_postgres};
use oswx_server::AppState;
use oswx_server::Settings;
use oswx_server::store::postgres::PgConfig;
use oswx_server::store::{
    Document, NewChange, Org, PgStore, Project, RoomLease, SchemaError, SchemaStatus, Store, User,
    is_conflict, is_unique_violation, new_id,
};
use sqlx::migrate::Migrator;
use tempfile::TempDir;

/// Ends the test, saying why, when the suite does not run against PostgreSQL.
macro_rules! needs_postgres {
    () => {
        if !on_postgres() {
            eprintln!("skipped: needs OSWX_TEST_DATABASE_URL (tools/test_postgres_rust.py)");
            return;
        }
    };
}

const CURRENT: SchemaStatus = SchemaStatus {
    current: Some(1),
    latest: 1,
    pending: vec![],
    unknown: vec![],
};

/// `pool` connections for requests (half for transactions, half for reads); the journal's two.
fn pools(pool: u32) -> PgConfig {
    PgConfig {
        pool,
        ..PgConfig::default()
    }
}

/// An empty schema and the store opened in it.
async fn open() -> (PgSchema, PgStore) {
    let schema = PgSchema::create().await;
    let store = PgStore::open(&schema.url, true, pools(4))
        .await
        .expect("the store opens");
    (schema, store)
}

fn schema_error(result: anyhow::Result<PgStore>) -> SchemaError {
    match result {
        Ok(_) => panic!("the database was accepted"),
        Err(e) => e
            .downcast::<SchemaError>()
            .unwrap_or_else(|e| panic!("not a schema error: {e:#}")),
    }
}

async fn exec(store: &PgStore, statement: &str) -> anyhow::Result<()> {
    let mut tx = store.begin_tx().await?;
    sqlx::query(statement).execute(tx.conn()).await?;
    tx.commit_tx().await
}

async fn scalar<T>(store: &PgStore, query: &str) -> T
where
    T: Send + Unpin + for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    sqlx::query_scalar(query)
        .fetch_one(&mut *store.reader().await.unwrap())
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
}

/// An organisation, a project in it, an account and a document, through the store's own methods.
struct Rows {
    org: String,
    user: String,
    doc: String,
}

async fn fill(store: &PgStore) -> Rows {
    let (org, project, user, doc) = (new_id(), new_id(), new_id(), new_id());
    let mut tx = store.begin().await.unwrap();
    tx.insert_user(&User {
        id: user.clone(),
        email: format!("{user}@example.com"),
        name: "Ada".into(),
        password_hash: None,
        is_server_admin: true,
        disabled: false,
        email_verified: true,
        created_at: 1.5,
        deleted_at: None,
    })
    .await
    .unwrap();
    tx.insert_org(&Org {
        id: org.clone(),
        name: "Org".into(),
        created_at: 2.0,
    })
    .await
    .unwrap();
    tx.insert_project(&Project {
        id: project.clone(),
        org_id: org.clone(),
        name: "Project".into(),
        created_at: 3.0,
    })
    .await
    .unwrap();
    tx.insert_document(&Document {
        id: doc.clone(),
        project_id: project,
        folder_id: None,
        name: "Part".into(),
        kind: "part".into(),
        format_version: Some(i64::MAX),
        head_seq: 0,
        created_by: user.clone(),
        created_at: 4.0,
        updated_at: 4.0,
        deleted_at: None,
    })
    .await
    .unwrap();
    tx.commit().await.unwrap();
    Rows { org, user, doc }
}

fn change<'a>(
    doc: &'a str,
    author: &'a str,
    seq: i64,
    id: &'a str,
    ops: &'a [u8],
) -> NewChange<'a> {
    NewChange {
        document_id: doc,
        seq,
        parent_seq: seq - 1,
        branch_id: "main",
        epoch: 1,
        author_id: author,
        client_id: Some("u.c"),
        change_id: Some(id),
        created_at: 10.0 + seq as f64,
        command: "test",
        ops,
        inverse: &[0x90],
        paths: &[0x90],
        reads: &[0x90],
        undo_of: None,
        reverts: None,
        reverted_by: None,
        broke: None,
    }
}

// ------------------------------------------------------------------ opening and migrating

#[tokio::test]
async fn an_empty_schema_is_built_and_opens_again_as_it_is() {
    needs_postgres!();
    let (schema, store) = open().await;
    assert_eq!(store.schema_status().await.unwrap(), CURRENT);
    store.health().await.unwrap();
    assert_eq!(store.user_count().await.unwrap(), 0);
    fill(&store).await;
    store.close().await;
    assert!(store.health().await.is_err());
    for auto_migrate in [true, false] {
        let store = PgStore::open(&schema.url, auto_migrate, pools(2))
            .await
            .unwrap();
        assert_eq!(store.schema_status().await.unwrap(), CURRENT);
        assert_eq!(store.user_count().await.unwrap(), 1);
        store.close().await;
    }
}

#[tokio::test]
async fn connect_then_migrate_builds_the_schema() {
    needs_postgres!();
    let schema = PgSchema::create().await;
    let store = PgStore::connect(&schema.url, pools(2)).await.unwrap();
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_servers_starting_on_an_empty_database_build_it_once() {
    needs_postgres!();
    let schema = PgSchema::create().await;
    let (a, b) = tokio::join!(
        PgStore::open(&schema.url, true, pools(2)),
        PgStore::open(&schema.url, true, pools(2))
    );
    for store in [a.unwrap(), b.unwrap()] {
        assert_eq!(store.schema_status().await.unwrap(), CURRENT);
    }
}

/// Migrations of a "newer binary": the real ones plus `0002_extra.sql`.
async fn newer_migrator(dir: &TempDir) -> &'static Migrator {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/postgres/0001_init.sql");
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
    needs_postgres!();
    let dir = TempDir::new().unwrap();
    let (schema, store) = open().await;
    store.close().await;
    let newer = newer_migrator(&dir).await;

    let refused = PgStore::open_with_migrations(&schema.url, false, pools(2), newer).await;
    assert_eq!(
        schema_error(refused),
        SchemaError::Outdated {
            current: Some(1),
            latest: 2
        }
    );
    let store = PgStore::open_with_migrations(&schema.url, true, pools(2), newer)
        .await
        .unwrap();
    assert_eq!(store.schema_status().await.unwrap().current, Some(2));
    assert_eq!(scalar::<i64>(&store, "SELECT COUNT(*) FROM extra").await, 0);
    store.close().await;

    // Now this binary (which knows only 0001) meets a database written by the newer one.
    assert_eq!(
        schema_error(PgStore::open(&schema.url, true, pools(2)).await),
        SchemaError::Newer {
            unknown: vec![2],
            latest: 1
        }
    );
}

#[tokio::test]
async fn an_applied_migration_that_was_edited_is_refused_at_open() {
    needs_postgres!();
    let dir = TempDir::new().unwrap();
    let (schema, store) = open().await;
    store.close().await;
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/postgres/0001_init.sql");
    let to = dir.path().join("edited");
    std::fs::create_dir_all(&to).unwrap();
    let mut text = std::fs::read_to_string(src).unwrap();
    text.push_str("\n-- edited after release\n");
    std::fs::write(to.join("0001_init.sql"), text).unwrap();
    let edited: &'static Migrator = Box::leak(Box::new(Migrator::new(to).await.unwrap()));
    for auto_migrate in [true, false] {
        let err = schema_error(
            PgStore::open_with_migrations(&schema.url, auto_migrate, pools(2), edited).await,
        );
        assert_eq!(err, SchemaError::Changed { version: 1 });
    }
}

#[tokio::test]
async fn a_schema_with_tables_but_no_migration_record_is_refused() {
    needs_postgres!();
    let schema = PgSchema::create().await;
    // A Python server's database: its own tables and `alembic_version`, no `_sqlx_migrations`.
    let raw = PgStore::connect(&schema.url, pools(2)).await.unwrap();
    exec(
        &raw,
        "CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)",
    )
    .await
    .unwrap();
    raw.close().await;
    assert_eq!(
        schema_error(PgStore::open(&schema.url, true, pools(2)).await),
        SchemaError::Foreign
    );
}

/// A column type as SQLite's migration spells it, for PostgreSQL's name of the same type: the
/// mapping `crate::store`'s module documentation gives (`BLOB` is `bytea`).
fn sqlite_spelling(data_type: &str, length: Option<i64>) -> String {
    match (data_type, length) {
        ("character varying", Some(n)) => format!("VARCHAR({n})"),
        ("text", None) => "TEXT".into(),
        ("bigint", None) => "BIGINT".into(),
        ("double precision", None) => "DOUBLE PRECISION".into(),
        ("boolean", None) => "BOOLEAN".into(),
        ("bytea", None) => "BLOB".into(),
        (other, length) => format!("{other}?{length:?}"),
    }
}

/// A column default without PostgreSQL's cast (`''::character varying`) and in one case.
fn plain_default(default: Option<String>) -> String {
    match default {
        None => String::new(),
        Some(d) => {
            let d = d.split("::").next().unwrap_or_default().to_lowercase();
            format!(" ={d}")
        }
    }
}

/// Everything the PostgreSQL schema declares, one line each: columns (type, NOT NULL, default),
/// indexes and keys (uniqueness, columns, the name of those the migration names), foreign keys
/// (with what a deletion does).
async fn postgres_schema(pg: &PgStore) -> Vec<String> {
    let mut c = pg.reader().await.unwrap();
    let mut out = Vec::new();
    /// A column: its table, name, type, length, whether it is NOT NULL, and its default.
    type Column = (String, String, String, Option<i64>, bool, Option<String>);
    let columns: Vec<Column> = sqlx::query_as(
        "SELECT CAST(table_name AS TEXT), CAST(column_name AS TEXT), CAST(data_type AS TEXT), \
         CAST(character_maximum_length AS BIGINT), is_nullable = 'NO', \
         CAST(column_default AS TEXT) FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name <> '_sqlx_migrations'",
    )
    .fetch_all(&mut *c)
    .await
    .unwrap();
    for (table, column, data_type, length, not_null, default) in columns {
        out.push(format!(
            "column {table}.{column} {}{}{}",
            sqlite_spelling(&data_type, length),
            if not_null { " NN" } else { "" },
            plain_default(default)
        ));
    }
    let indexes: Vec<(String, String, bool, String)> = sqlx::query_as(
        "SELECT CAST(t.relname AS TEXT), CAST(i.relname AS TEXT), x.indisunique, \
         (SELECT string_agg(CAST(a.attname AS TEXT), ', ' ORDER BY k.ord) \
          FROM unnest(CAST(x.indkey AS int2[])) WITH ORDINALITY AS k(attnum, ord) \
          JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum) \
         FROM pg_index x JOIN pg_class t ON t.oid = x.indrelid \
         JOIN pg_class i ON i.oid = x.indexrelid JOIN pg_namespace n ON n.oid = t.relnamespace \
         WHERE n.nspname = current_schema() AND t.relname <> '_sqlx_migrations'",
    )
    .fetch_all(&mut *c)
    .await
    .unwrap();
    for (table, name, unique, columns) in indexes {
        out.push(index_line(&table, &name, unique, &columns));
    }
    let keys: Vec<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT CAST(t.relname AS TEXT), CAST(a.attname AS TEXT), CAST(rt.relname AS TEXT), \
         CAST(ra.attname AS TEXT), CAST(c.confdeltype AS TEXT) \
         FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid \
         JOIN pg_class rt ON rt.oid = c.confrelid JOIN pg_namespace n ON n.oid = t.relnamespace \
         JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = c.conkey[1] \
         JOIN pg_attribute ra ON ra.attrelid = rt.oid AND ra.attnum = c.confkey[1] \
         WHERE c.contype = 'f' AND n.nspname = current_schema() \
         AND cardinality(c.conkey) = 1",
    )
    .fetch_all(&mut *c)
    .await
    .unwrap();
    for (table, column, to_table, to, on_delete) in keys {
        let on_delete = match on_delete.as_str() {
            "a" => "NO ACTION",
            "c" => "CASCADE",
            "n" => "SET NULL",
            "r" => "RESTRICT",
            "d" => "SET DEFAULT",
            other => panic!("an unknown deletion rule {other}"),
        };
        out.push(format!(
            "key {table}.{column} -> {to_table}.{to} {on_delete}"
        ));
    }
    let many: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace \
         WHERE c.contype = 'f' AND n.nspname = current_schema() AND cardinality(c.conkey) <> 1",
    )
    .fetch_one(&mut *c)
    .await
    .unwrap();
    assert_eq!(
        many, 0,
        "this comparison reads one-column foreign keys only"
    );
    out.sort();
    out
}

/// An index or key: its table, uniqueness and columns; and its name if the migration gave it one
/// (each database names the indexes of its keys itself).
fn index_line(table: &str, name: &str, unique: bool, columns: &str) -> String {
    let name = if name.starts_with("ix_") { name } else { "" };
    let unique = if unique { "unique " } else { "" };
    format!("index {table} {unique}({columns}) {name}")
}

/// [`postgres_schema`], of the SQLite schema.
async fn sqlite_schema(lite: &oswx_server::store::SqliteStore) -> Vec<String> {
    let mut c = lite.reader().await.unwrap();
    let mut out = Vec::new();
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
         AND name <> '_sqlx_migrations'",
    )
    .fetch_all(&mut *c)
    .await
    .unwrap();
    for table in tables {
        let columns: Vec<(String, String, bool, Option<String>)> =
            sqlx::query_as("SELECT name, type, \"notnull\", dflt_value FROM pragma_table_info($1)")
                .bind(&table)
                .fetch_all(&mut *c)
                .await
                .unwrap();
        for (column, declared, not_null, default) in columns {
            out.push(format!(
                "column {table}.{column} {declared}{}{}",
                if not_null { " NN" } else { "" },
                plain_default(default)
            ));
        }
        let indexes: Vec<(String, bool)> =
            sqlx::query_as("SELECT name, \"unique\" FROM pragma_index_list($1)")
                .bind(&table)
                .fetch_all(&mut *c)
                .await
                .unwrap();
        for (name, unique) in indexes {
            let columns: Vec<String> =
                sqlx::query_scalar("SELECT name FROM pragma_index_info($1) ORDER BY seqno")
                    .bind(&name)
                    .fetch_all(&mut *c)
                    .await
                    .unwrap();
            out.push(index_line(&table, &name, unique, &columns.join(", ")));
        }
        let keys: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT \"from\", \"table\", \"to\", on_delete FROM pragma_foreign_key_list($1)",
        )
        .bind(&table)
        .fetch_all(&mut *c)
        .await
        .unwrap();
        for (column, to_table, to, on_delete) in keys {
            out.push(format!(
                "key {table}.{column} -> {to_table}.{to} {on_delete}"
            ));
        }
    }
    out.sort();
    out
}

/// The two migration files cannot drift apart: every column (type, NOT NULL, default), every
/// index and key (columns, uniqueness, the names the migrations give) and every foreign key (with
/// its deletion rule) of one is in the other.
#[tokio::test]
async fn the_two_migrations_declare_the_same_schema() {
    needs_postgres!();
    let (_schema, pg) = open().await;
    let dir = TempDir::new().unwrap();
    let lite = oswx_server::store::SqliteStore::open(
        &format!(
            "sqlite://{}/collab.db",
            dir.path().to_string_lossy().replace('\\', "/")
        ),
        true,
    )
    .await
    .unwrap();
    let (of_postgres, of_sqlite) = (postgres_schema(&pg).await, sqlite_schema(&lite).await);
    assert_eq!(
        of_postgres.join("\n"),
        of_sqlite.join("\n"),
        "PostgreSQL's schema (left) and SQLite's (right) differ"
    );
    // (That the comparison sees what it should.)
    for line in [
        "column changes.ops BLOB NN",
        "column users.is_server_admin BOOLEAN NN =false",
        "column invites.projects TEXT NN ='[]'",
        "column documents.format_version BIGINT",
        "index changes (author_id, document_id, seq) ix_changes_author",
        "index changes unique (document_id, client_id, change_id) ",
        "index users unique (email) ix_users_email",
        "key documents.folder_id -> folders.id SET NULL",
        "key changes.document_id -> documents.id CASCADE",
    ] {
        assert!(of_postgres.contains(&line.to_owned()), "no line {line:?}");
    }
    assert!(of_postgres.len() > 150, "{}", of_postgres.len());

    // Every text column compares by bytes, as SQLite does.
    let other: i64 = scalar(
        &pg,
        "SELECT COUNT(*) FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name <> '_sqlx_migrations' \
         AND data_type IN ('character varying', 'text') AND collation_name IS DISTINCT FROM 'C'",
    )
    .await;
    assert_eq!(other, 0);
}

// ------------------------------------------------------------------ URLs and errors

#[tokio::test]
async fn an_unknown_scheme_is_refused_without_repeating_the_url() {
    // (Needs no database.)
    let err = AppState::from_settings(Settings {
        database_url: "mysql://collab:hunter2@db/collab".into(),
        ..Settings::default()
    })
    .await
    .err()
    .expect("refused");
    let shown = format!("{err:#} {err:?}");
    assert!(shown.contains("OSWX_COLLAB_DATABASE_URL"), "{shown}");
    assert!(shown.contains("postgres://"), "it says what is accepted");
    assert!(!shown.contains("hunter2"), "{shown}");
    let err = PgStore::connect("sqlite://data/collab.db", pools(2))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not a PostgreSQL"), "{err}");
}

#[tokio::test]
async fn a_database_that_cannot_be_reached_is_named_without_its_password() {
    // (Needs no database: nothing listens on port 1.)
    for url in [
        "postgres://collab:hunter2@127.0.0.1:1/collab",
        "postgresql+asyncpg://collab:hunter2@127.0.0.1:1/collab",
    ] {
        let err = tokio::time::timeout(Duration::from_secs(60), PgStore::open(url, true, pools(2)))
            .await
            .expect("it gives up")
            .unwrap_err();
        let shown = format!("{err:#} {err:?}");
        assert!(shown.contains("collab on 127.0.0.1:1"), "{shown}");
        assert!(
            shown.contains("the database server cannot be reached"),
            "{shown}"
        );
        assert!(!shown.contains("hunter2"), "{shown}");
    }
    // A URL that cannot be read at all.
    let err = PgStore::connect("postgres://collab:hunter2@127.0.0.1:port/collab", pools(2))
        .await
        .unwrap_err();
    let shown = format!("{err:#} {err:?}");
    assert!(shown.contains("cannot be read"), "{shown}");
    assert!(!shown.contains("hunter2"), "{shown}");
}

#[tokio::test]
async fn a_refused_user_is_not_named_in_the_error() {
    needs_postgres!();
    let schema = PgSchema::create().await;
    let mut url = url::Url::parse(&schema.url).unwrap();
    url.set_username("nobody_of_that_name").unwrap();
    url.set_password(Some("hunter2")).unwrap();
    let err = PgStore::open(url.as_str(), true, pools(2))
        .await
        .unwrap_err();
    let shown = format!("{err:#} {err:?}");
    assert!(
        shown.contains("the user or password was refused"),
        "{shown}"
    );
    assert!(shown.contains("cannot open the database"), "{shown}");
    assert!(!shown.contains("nobody_of_that_name"), "{shown}");
    assert!(!shown.contains("hunter2"), "{shown}");
}

#[tokio::test]
async fn a_statement_out_of_time_or_no_free_connection_is_503_and_a_taken_key_500() {
    use axum::http::StatusCode;
    use oswx_server::ApiError;
    // (This much needs no database.)
    let waited = ApiError::from(sqlx::Error::PoolTimedOut);
    assert_eq!(
        (waited.status(), waited.detail()),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "the server is busy; try again"
        )
    );
    needs_postgres!();
    let (_schema, store) = open().await;
    let rows = fill(&store).await;
    let mut tx = store.begin_tx().await.unwrap();
    sqlx::query("SET LOCAL statement_timeout = 50")
        .execute(tx.conn())
        .await
        .unwrap();
    let slow: anyhow::Error = sqlx::query("SELECT pg_sleep(2)")
        .execute(tx.conn())
        .await
        .unwrap_err()
        .into();
    assert!(oswx_server::store::is_busy(&slow), "{slow:#}");
    assert_eq!(
        ApiError::from(slow).status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(tx);
    // A key nobody expected to be taken is a fault of the server, not something to try again.
    let mut tx = store.begin().await.unwrap();
    let taken = tx
        .insert_org(&Org {
            id: rows.org.clone(),
            name: "Again".into(),
            created_at: 0.0,
        })
        .await
        .unwrap_err();
    assert!(is_unique_violation(&taken) && !is_conflict(&taken));
    assert_eq!(
        ApiError::from(taken).status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_gives_up_on_a_lock_and_the_journal_a_little_later() {
    use axum::http::StatusCode;
    needs_postgres!();
    let schema = PgSchema::create().await;
    let config = PgConfig {
        pool: 4,
        journal_pool: 2,
        lock_timeout: Duration::from_millis(300),
        journal_lock_timeout: Duration::from_millis(1500),
    };
    let store = PgStore::open(&schema.url, true, config).await.unwrap();
    let rows = fill(&store).await;
    let mut holder = store.begin().await.unwrap();
    assert!(holder.lock_org(&rows.org).await.unwrap());
    holder
        .lock_documents(std::slice::from_ref(&rows.doc))
        .await
        .unwrap();
    // A request's transaction gives up after the lock timeout: 409, try again.
    let mut request = store.begin().await.unwrap();
    let started = std::time::Instant::now();
    let err = request.lock_org(&rows.org).await.unwrap_err();
    assert!(is_conflict(&err), "{err:#}");
    assert!(started.elapsed() < Duration::from_secs(5));
    let answer = oswx_server::ApiError::from(err);
    assert_eq!(answer.status(), StatusCode::CONFLICT);
    assert_eq!(
        answer.detail(),
        "another request changed the same thing at the same moment; try again"
    );
    drop(request);
    // The journal's transaction waits its own, longer, time: it goes on when the lock is freed
    // within it ...
    let waiting = |store: PgStore, doc: String| {
        tokio::spawn(async move {
            let mut tx = store.begin_journal().await.unwrap();
            let locked = tx.lock_documents(&[doc]).await;
            if locked.is_ok() {
                tx.commit().await.unwrap();
            }
            locked
        })
    };
    let mut journal = waiting(store.clone(), rows.doc.clone());
    assert!(
        tokio::time::timeout(Duration::from_millis(800), &mut journal)
            .await
            .is_err(),
        "the journal's transaction gave up as early as a request's, or did not wait"
    );
    holder.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), journal)
        .await
        .expect("it goes on once the lock is free")
        .unwrap()
        .unwrap();
    // ... and gives up, as a conflict, when it is not.
    let mut holder = store.begin().await.unwrap();
    holder
        .lock_documents(std::slice::from_ref(&rows.doc))
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let err = waiting(store.clone(), rows.doc.clone())
        .await
        .unwrap()
        .unwrap_err();
    assert!(is_conflict(&err), "{err:#}");
    assert!(started.elapsed() >= Duration::from_millis(1400));
    assert!(started.elapsed() < Duration::from_secs(10));
    drop(holder);
}

#[tokio::test]
async fn a_lock_timeout_as_long_as_a_statement_may_run_is_refused() {
    needs_postgres!();
    let schema = PgSchema::create().await;
    for config in [
        PgConfig {
            lock_timeout: Duration::from_secs(21),
            ..PgConfig::default()
        },
        PgConfig {
            journal_lock_timeout: Duration::from_secs(30),
            ..PgConfig::default()
        },
    ] {
        let err = PgStore::connect(&schema.url, config).await.unwrap_err();
        assert!(err.to_string().contains("at most 20 s"), "{err:#}");
    }
    let longest = PgConfig {
        lock_timeout: Duration::from_secs(20),
        journal_lock_timeout: Duration::from_secs(20),
        ..PgConfig::default()
    };
    PgStore::connect(&schema.url, longest).await.unwrap();
}

#[tokio::test]
async fn every_connection_runs_read_committed_with_timeouts() {
    needs_postgres!();
    let (_schema, store) = open().await;
    assert_eq!(
        scalar::<String>(&store, "SHOW transaction_isolation").await,
        "read committed"
    );
    assert_eq!(
        scalar::<String>(&store, "SHOW statement_timeout").await,
        "30s"
    );
    let mut tx = store.begin_tx().await.unwrap();
    let level: String = sqlx::query_scalar("SHOW transaction_isolation")
        .fetch_one(tx.conn())
        .await
        .unwrap();
    assert_eq!(level, "read committed");
    let idle: String = sqlx::query_scalar("SHOW idle_in_transaction_session_timeout")
        .fetch_one(tx.conn())
        .await
        .unwrap();
    assert_eq!(idle, "1min");
    // A request's transaction waits five seconds for a lock, the journal's two; a read takes none.
    let lock: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(tx.conn())
        .await
        .unwrap();
    assert_eq!(lock, "5s");
    drop(tx);
    let mut tx = store.begin_journal_tx().await.unwrap();
    let lock: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(tx.conn())
        .await
        .unwrap();
    assert_eq!(lock, "2s");
    assert_eq!(scalar::<String>(&store, "SHOW lock_timeout").await, "0");
}

#[tokio::test]
async fn a_taken_key_is_told_apart_from_other_errors() {
    needs_postgres!();
    let (_schema, store) = open().await;
    let rows = fill(&store).await;
    let again = Org {
        id: rows.org.clone(),
        name: "Again".into(),
        created_at: 0.0,
    };
    let mut tx = store.begin().await.unwrap();
    let err = tx.insert_org(&again).await.unwrap_err();
    assert!(is_unique_violation(&err), "{err:#}");
    assert!(
        !is_conflict(&err),
        "a taken key is not something to try again"
    );
    // The transaction is over: PostgreSQL refuses what follows, and that is not a conflict.
    let after = tx.org_by_id(&rows.org).await.unwrap_err();
    assert!(
        !is_unique_violation(&after) && !is_conflict(&after),
        "{after:#}"
    );
    drop(tx);
    // A foreign key is not a taken key.
    let mut tx = store.begin().await.unwrap();
    let err = tx
        .insert_project(&Project {
            id: new_id(),
            org_id: new_id(),
            name: "Nowhere".into(),
            created_at: 0.0,
        })
        .await
        .unwrap_err();
    assert!(!is_unique_violation(&err) && !is_conflict(&err), "{err:#}");
}

// ------------------------------------------------------------------ rows and transactions

#[tokio::test]
async fn every_kind_of_column_comes_back_as_it_went_in() {
    needs_postgres!();
    let (_schema, store) = open().await;
    let rows = fill(&store).await;
    let user = store.user_by_id(&rows.user).await.unwrap().unwrap();
    assert!(user.is_server_admin && user.email_verified && !user.disabled);
    assert_eq!((user.created_at, user.deleted_at), (1.5, None));
    assert_eq!(user.password_hash, None);
    let doc = store.document_by_id(&rows.doc).await.unwrap().unwrap();
    assert_eq!(doc.format_version, Some(i64::MAX));
    assert_eq!((doc.head_seq, doc.folder_id), (0, None));

    let ops: Vec<u8> = (0..=255).collect(); // every byte, the zero byte included
    let mut tx = store.begin().await.unwrap();
    assert!(
        tx.insert_change(&change(&rows.doc, &rows.user, 1, "a", &ops))
            .await
            .unwrap()
    );
    // The same change id again: left out, and the transaction goes on.
    assert!(
        !tx.insert_change(&change(&rows.doc, &rows.user, 2, "a", &ops))
            .await
            .unwrap()
    );
    tx.set_document_head(&rows.doc, 1, 11.0).await.unwrap();
    tx.commit().await.unwrap();
    let row = store.change_by_seq(&rows.doc, 1).await.unwrap().unwrap();
    assert_eq!(row.ops, ops);
    assert_eq!(row.inverse, [0x90]);
    assert_eq!((row.epoch, row.parent_seq, row.created_at), (1, 0, 11.0));
    assert_eq!(row.client_id.as_deref(), Some("u.c"));
    assert_eq!((row.undo_of, row.broke), (None, None));
    let sizes = store
        .change_sizes(&rows.doc, i64::MAX, 0, 10)
        .await
        .unwrap();
    assert_eq!((sizes[0].seq, sizes[0].size), (1, 256));
    let events = store
        .change_events_between(&rows.doc, 0, 9, 10)
        .await
        .unwrap();
    assert_eq!(events[0].author_name.as_deref(), Some("Ada"));
    assert_eq!(store.orgs_of_user(&rows.user).await.unwrap(), []);
}

#[tokio::test]
async fn more_journal_rows_than_one_statement_takes_go_in_and_duplicates_are_counted_out() {
    needs_postgres!();
    let (_schema, store) = open().await;
    let rows = fill(&store).await;
    // 4,000 rows are 72,000 parameters: more than PostgreSQL takes in one statement.
    let ids: Vec<String> = (1..=4000).map(|i| format!("c{i}")).collect();
    let batch: Vec<NewChange<'_>> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| change(&rows.doc, &rows.user, i as i64 + 1, id, &[0x90]))
        .collect();
    let mut tx = store.begin().await.unwrap();
    assert_eq!(tx.insert_changes(&batch).await.unwrap(), 4000);
    tx.commit().await.unwrap();
    assert_eq!(
        scalar::<i64>(&store, "SELECT COUNT(*) FROM changes").await,
        4000
    );
    // Three more, one of them a change id the journal has: two go in.
    let more = [
        change(&rows.doc, &rows.user, 4001, "n1", &[0x90]),
        change(&rows.doc, &rows.user, 4002, "c7", &[0x90]),
        change(&rows.doc, &rows.user, 4003, "n2", &[0x90]),
    ];
    let mut tx = store.begin().await.unwrap();
    assert_eq!(tx.insert_changes(&more).await.unwrap(), 2);
    assert_eq!(tx.insert_changes(&[]).await.unwrap(), 0);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn a_savepoint_undoes_a_failed_statement_and_the_transaction_goes_on() {
    needs_postgres!();
    let (_schema, store) = open().await;
    let rows = fill(&store).await;
    let org = |name: &str| Org {
        id: new_id(),
        name: name.into(),
        created_at: 0.0,
    };
    let mut tx = store.begin().await.unwrap();
    tx.insert_org(&org("kept")).await.unwrap();
    {
        let mut sp = tx.savepoint().await.unwrap();
        sp.insert_org(&org("undone")).await.unwrap();
        let taken = Org {
            id: rows.org.clone(),
            ..org("taken")
        };
        assert!(sp.insert_org(&taken).await.is_err());
        sp.rollback().await.unwrap();
    }
    {
        let mut sp = tx.savepoint().await.unwrap();
        sp.insert_org(&org("dropped")).await.unwrap();
        // (dropped without a commit: undone)
    }
    {
        let mut sp = tx.savepoint().await.unwrap();
        sp.insert_org(&org("inner")).await.unwrap();
        sp.commit().await.unwrap();
    }
    tx.commit().await.unwrap();
    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM orgs ORDER BY name")
        .fetch_all(&mut *store.reader().await.unwrap())
        .await
        .unwrap();
    assert_eq!(names, ["Org", "inner", "kept"]);
    // A transaction dropped without a commit leaves nothing.
    let mut tx = store.begin().await.unwrap();
    tx.insert_org(&org("never")).await.unwrap();
    drop(tx);
    assert_eq!(scalar::<i64>(&store, "SELECT COUNT(*) FROM orgs").await, 3);
}

/// Whether `work` is still not done after a moment (it waits for a lock).
async fn waits<T>(work: &mut tokio::task::JoinHandle<T>) -> bool {
    tokio::time::timeout(Duration::from_millis(400), work)
        .await
        .is_err()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_lock_holds_others_off_until_its_transaction_ends() {
    needs_postgres!();
    let (_schema, store) = open().await;
    let rows = fill(&store).await;
    let mut tx = store.begin().await.unwrap();
    tx.insert_room_lease(&RoomLease {
        document_id: rows.doc.clone(),
        node_id: "n".into(),
        epoch: 3,
        expires_at: 0.0,
    })
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // The holder takes every kind of lock, in the lock order.
    let mut holder = store.begin().await.unwrap();
    holder.lock_accounts().await.unwrap();
    assert!(holder.lock_user(&rows.user).await.unwrap());
    assert!(!holder.lock_user(&new_id()).await.unwrap());
    assert!(holder.lock_org(&rows.org).await.unwrap());
    assert!(!holder.lock_org(&new_id()).await.unwrap());
    holder
        .lock_documents(&[new_id(), rows.doc.clone(), rows.doc.clone()])
        .await
        .unwrap();
    assert!(holder.lock_live_document(&rows.doc).await.unwrap());
    assert_eq!(holder.lock_room_lease(&rows.doc).await.unwrap(), Some(3));
    assert_eq!(holder.lock_room_lease(&new_id()).await.unwrap(), None);

    // Each of these waits for it.
    let spawn = |what: &'static str| {
        let (store, rows) = (
            store.clone(),
            (rows.org.clone(), rows.user.clone(), rows.doc.clone()),
        );
        tokio::spawn(async move {
            let (org, user, doc) = rows;
            let mut tx = store.begin().await.unwrap();
            match what {
                "accounts" => tx.lock_accounts().await.unwrap(),
                "user" => assert!(tx.lock_user(&user).await.unwrap()),
                "org" => assert!(tx.lock_org(&org).await.unwrap()),
                "documents" => tx.lock_documents(&[doc]).await.unwrap(),
                "live document" => assert!(tx.lock_live_document(&doc).await.unwrap()),
                "deleting the document" => {
                    assert!(tx.mark_document_deleted(&doc, 9.0).await.unwrap());
                    tx.rollback().await.unwrap();
                    return;
                }
                "lease" => assert_eq!(tx.lock_room_lease(&doc).await.unwrap(), Some(3)),
                "taking the lease" => {
                    assert_eq!(tx.take_room_lease(&doc, "m", 1.0).await.unwrap(), Some(4));
                    tx.rollback().await.unwrap();
                    return;
                }
                _ => unreachable!(),
            }
            tx.commit().await.unwrap();
        })
    };
    // (Two at a time: the store was opened with two connections for transactions.)
    let mut waiting = Vec::new();
    for what in [
        "accounts",
        "user",
        "org",
        "documents",
        "live document",
        "deleting the document",
        "lease",
        "taking the lease",
    ] {
        let mut task = spawn(what);
        assert!(waits(&mut task).await, "{what} did not wait");
        task.abort();
        let _ = task.await;
        waiting.push(what);
    }
    // Rows that only refer to the locked account and document go in meanwhile.
    let mut other = store.begin().await.unwrap();
    other
        .insert_session(&oswx_server::store::Session {
            token_hash: "h".repeat(64),
            user_id: rows.user.clone(),
            created_at: 0.0,
            expires_at: 9.0,
            label: String::new(),
        })
        .await
        .unwrap();
    assert!(
        other
            .insert_change(&change(&rows.doc, &rows.user, 1, "a", &[0x90]))
            .await
            .unwrap()
    );
    other.rollback().await.unwrap();

    holder.commit().await.unwrap();
    // ... and once it has ended, each goes through.
    for what in waiting {
        tokio::time::timeout(Duration::from_secs(10), spawn(what))
            .await
            .unwrap_or_else(|_| panic!("{what} still waits"))
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_never_waits_for_the_transactions_pool() {
    needs_postgres!();
    let (_schema, store) = open().await;
    let rows = fill(&store).await;
    // Every transaction connection is taken (two of the four) ...
    let a = store.begin().await.unwrap();
    let b = store.begin().await.unwrap();
    // ... and reads go on.
    tokio::time::timeout(Duration::from_secs(5), store.org_by_id(&rows.org))
        .await
        .expect("a read does not wait for a transaction to end")
        .unwrap()
        .unwrap();
    drop((a, b));
}

#[tokio::test]
async fn names_are_ordered_by_their_bytes() {
    needs_postgres!();
    let (_schema, store) = open().await;
    let rows = fill(&store).await;
    let mut tx = store.begin().await.unwrap();
    for name in ["b", "B", "a", "_x", "Z", "é", "10", "9"] {
        tx.insert_project(&Project {
            id: new_id(),
            org_id: rows.org.clone(),
            name: name.into(),
            created_at: 0.0,
        })
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    let names: Vec<String> = store
        .projects_in_org(&rows.org)
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .collect();
    let mut sorted = names.clone();
    sorted.sort(); // (Rust orders strings by their bytes, as SQLite does)
    assert_eq!(names, sorted);
    assert_eq!(names, ["10", "9", "B", "Project", "Z", "_x", "a", "b", "é"]);
}
