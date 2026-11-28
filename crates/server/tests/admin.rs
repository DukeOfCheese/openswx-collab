//! The operator's commands as the binary runs them (`oswx_server::admin`), and an extension's
//! migrations: applied after the server's own by `migrate` and at start, shown by `db-status`.
//! On SQLite, or on PostgreSQL when the tests run against it.

mod common;

use std::sync::Arc;

use common::{PgSchema, TestServer, on_postgres};
use oswx_server::admin::{self, Db};
use oswx_server::ext::{
    Dialect, Extensions, Migration, MigrationsError, NoExtensions, check_migrations,
};
use oswx_server::security::verify_password;
use oswx_server::store::{ExtensionSchema, SchemaError, Session, new_id};
use oswx_server::{AppState, Settings};
use serde_json::json;
use tempfile::TempDir;

/// `list` as migrations named for their place: `one`, `two`, ...
fn by_place(list: &[&'static str]) -> Vec<Migration> {
    const IDS: [&str; 4] = ["one", "two", "three", "four"];
    IDS.iter().copied().zip(list.iter().copied()).collect()
}

/// An extension that brings these migrations.
struct Ext(Vec<&'static str>);

impl Extensions for Ext {
    fn migrations(&self, _dialect: Dialect) -> Vec<Migration> {
        by_place(&self.0)
    }
}

/// An extension whose migrations are these on SQLite and those on PostgreSQL.
struct PerDialect(Vec<&'static str>, Vec<&'static str>);

impl Extensions for PerDialect {
    fn migrations(&self, dialect: Dialect) -> Vec<Migration> {
        match dialect {
            Dialect::Sqlite => by_place(&self.0),
            Dialect::Postgres => by_place(&self.1),
        }
    }
}

/// An extension whose migrations are exactly these, in both dialects.
struct Named(Vec<Migration>, Vec<Migration>);

impl Extensions for Named {
    fn migrations(&self, dialect: Dialect) -> Vec<Migration> {
        match dialect {
            Dialect::Sqlite => self.0.clone(),
            Dialect::Postgres => self.1.clone(),
        }
    }
}

/// Refers to a table of the server's own: fails unless the server's migrations came first.
const FIRST: &str = "CREATE TABLE ext_seats (
        org_id VARCHAR(32) NOT NULL PRIMARY KEY REFERENCES orgs (id) ON DELETE CASCADE,
        seats BIGINT NOT NULL
    );
    CREATE INDEX ix_ext_seats_seats ON ext_seats (seats);";
const SECOND: &str =
    "CREATE TABLE ext_audit (id VARCHAR(32) NOT NULL PRIMARY KEY, what TEXT NOT NULL)";
const THIRD: &str = "ALTER TABLE ext_audit ADD COLUMN at DOUBLE PRECISION NOT NULL DEFAULT 0";
const BROKEN: &str = "CREATE TABLE ext_more (id BIGINT); THIS IS NOT SQL";

/// A database of the test's own, empty: a file in a temporary folder, or a schema in the test
/// PostgreSQL database.
struct Place {
    dir: TempDir,
    schema: Option<PgSchema>,
}

impl Place {
    async fn new() -> Self {
        let schema = if on_postgres() {
            Some(PgSchema::create().await)
        } else {
            None
        };
        Self {
            dir: TempDir::new().unwrap(),
            schema,
        }
    }

    fn settings(&self, auto_migrate: bool) -> Settings {
        let dir = self.dir.path().to_string_lossy().replace('\\', "/");
        Settings {
            database_url: match &self.schema {
                Some(schema) => schema.url.clone(),
                None => format!("sqlite://{dir}/collab.db"),
            },
            database_pool: 4,
            storage_url: format!("{dir}/storage"),
            auto_migrate,
            ..Settings::default()
        }
    }

    /// Checks what `db-status` says of the database while nothing has been made in it: on
    /// PostgreSQL, that it is empty; on SQLite, that there is no file, without making one.
    async fn nothing_yet(&self, ext: &dyn Extensions) {
        let said = admin::db_status(&self.settings(false), ext).await;
        if self.schema.is_some() {
            let said = said.unwrap();
            assert!(
                said.starts_with("database at none (an empty database); this server needs 1"),
                "{said}"
            );
            let ours = ext.migrations(Dialect::Postgres).len();
            assert_eq!(
                said.ends_with(&format!(
                    "extension migrations at 0, this server needs {ours}"
                )),
                ours > 0,
                "{said}"
            );
        } else {
            let said = said.unwrap_err().to_string();
            assert!(said.starts_with("there is no database at "), "{said}");
            assert!(said.contains("nothing was created"), "{said}");
            let left: Vec<_> = std::fs::read_dir(self.dir.path()).unwrap().collect();
            assert!(left.is_empty(), "db-status made {left:?}");
        }
    }

    /// Runs one statement on the database, in its own transaction.
    async fn sql(&self, statement: &str) -> anyhow::Result<()> {
        let db = Db::connect(&self.settings(true)).await?;
        let done = async {
            match &db {
                Db::Sqlite(db) => {
                    let mut tx = db.begin_tx().await?;
                    sqlx::query(statement).execute(tx.conn()).await?;
                    tx.commit_tx().await
                }
                Db::Postgres(db) => {
                    let mut tx = db.begin_tx().await?;
                    sqlx::query(statement).execute(tx.conn()).await?;
                    tx.commit_tx().await
                }
            }
        }
        .await;
        db.close().await;
        done
    }

    /// The extension migrations the database has recorded.
    async fn recorded(&self) -> Vec<i64> {
        let db = Db::connect(&self.settings(true)).await.unwrap();
        let query = "SELECT version FROM extension_migrations ORDER BY version";
        let versions = match &db {
            Db::Sqlite(db) => sqlx::query_scalar(query)
                .fetch_all(&mut *db.reader().await.unwrap())
                .await
                .unwrap(),
            Db::Postgres(db) => sqlx::query_scalar(query)
                .fetch_all(&mut *db.reader().await.unwrap())
                .await
                .unwrap(),
        };
        db.close().await;
        versions
    }
}

/// Opens as a server with `ext` starts, and closes again; the error if it would not start.
async fn start(place: &Place, auto_migrate: bool, ext: &dyn Extensions) -> anyhow::Result<()> {
    let db = Db::open(&place.settings(auto_migrate), ext).await?;
    db.close().await;
    Ok(())
}

fn schema_error(result: anyhow::Result<impl Sized>) -> SchemaError {
    match result {
        Ok(_) => panic!("the database was accepted"),
        Err(e) => e
            .downcast::<SchemaError>()
            .unwrap_or_else(|e| panic!("not a schema error: {e:#}")),
    }
}

// ------------------------------------------------------------------ extension migrations

#[tokio::test]
async fn an_empty_database_gets_the_servers_schema_and_then_the_extensions() {
    for auto_migrate in [true, false] {
        let place = Place::new().await;
        let ext = Ext(vec![FIRST, SECOND]);
        start(&place, auto_migrate, &ext).await.unwrap();
        assert_eq!(place.recorded().await, vec![1, 2]);
        // The extension's tables are there, beside the server's.
        place
            .sql("INSERT INTO orgs (id, name, created_at) VALUES ('o1', 'Org', 1.5)")
            .await
            .unwrap();
        place
            .sql("INSERT INTO ext_seats (org_id, seats) VALUES ('o1', 5)")
            .await
            .unwrap();
        place
            .sql("INSERT INTO ext_audit (id, what) VALUES ('a', 'b')")
            .await
            .unwrap();
        // Starting again changes nothing.
        start(&place, auto_migrate, &ext).await.unwrap();
        assert_eq!(place.recorded().await, vec![1, 2]);
        assert_eq!(
            admin::db_status(&place.settings(auto_migrate), &ext)
                .await
                .unwrap(),
            "database at 1; this server needs 1; extension migrations at 2, this server needs 2"
        );
    }
}

#[tokio::test]
async fn an_older_database_gets_them_from_migrate_or_at_start_when_that_is_allowed() {
    let place = Place::new().await;
    // The open-source server made the database.
    start(&place, true, &NoExtensions).await.unwrap();
    assert_eq!(
        admin::db_status(&place.settings(true), &NoExtensions)
            .await
            .unwrap(),
        "database at 1; this server needs 1"
    );
    let ext = Ext(vec![FIRST, SECOND]);
    assert_eq!(
        admin::db_status(&place.settings(false), &ext)
            .await
            .unwrap(),
        "database at 1; this server needs 1; extension migrations at 0, this server needs 2"
    );
    // A server that may not upgrade by itself does not start, and says what to do.
    let refused = schema_error(start(&place, false, &ext).await);
    assert_eq!(
        refused,
        SchemaError::ExtensionOutdated {
            applied: 0,
            known: 2
        }
    );
    assert!(
        refused
            .to_string()
            .contains("openswx-collab-server migrate"),
        "{refused}"
    );
    assert!(place.recorded().await.is_empty());
    // `migrate` applies them whatever the setting says.
    assert_eq!(
        admin::migrate(&place.settings(false), &ext).await.unwrap(),
        "schema 1: up to date; extension migrations 0 -> 2"
    );
    assert_eq!(place.recorded().await, vec![1, 2]);
    start(&place, false, &ext).await.unwrap();
    assert_eq!(
        admin::migrate(&place.settings(false), &ext).await.unwrap(),
        "schema 1: up to date; extension migrations 2: up to date"
    );

    // A later version of the extension brings a third: only that one is applied (the first two
    // would fail a second time), at start when upgrading is allowed.
    let later = Ext(vec![FIRST, SECOND, THIRD]);
    assert_eq!(
        schema_error(start(&place, false, &later).await),
        SchemaError::ExtensionOutdated {
            applied: 2,
            known: 3
        }
    );
    start(&place, true, &later).await.unwrap();
    assert_eq!(place.recorded().await, vec![1, 2, 3]);
    place
        .sql("INSERT INTO ext_audit (id, what, at) VALUES ('a', 'b', 2.5)")
        .await
        .unwrap();
}

#[tokio::test]
async fn migrate_builds_an_empty_database_with_both() {
    let place = Place::new().await;
    let ext = Ext(vec![FIRST, SECOND]);
    place.nothing_yet(&ext).await;
    assert_eq!(
        admin::migrate(&place.settings(false), &ext).await.unwrap(),
        "schema none -> 1; extension migrations 0 -> 2"
    );
    assert_eq!(place.recorded().await, vec![1, 2]);
    // Without an extension nothing is said of extensions.
    let plain = Place::new().await;
    plain.nothing_yet(&NoExtensions).await;
    assert_eq!(
        admin::migrate(&plain.settings(false), &NoExtensions)
            .await
            .unwrap(),
        "schema none -> 1"
    );
    assert_eq!(
        admin::migrate(&plain.settings(false), &NoExtensions)
            .await
            .unwrap(),
        "schema 1: up to date"
    );
}

#[tokio::test]
async fn an_extension_migration_that_fails_leaves_nothing_of_its_run_behind() {
    let place = Place::new().await;
    start(&place, true, &NoExtensions).await.unwrap();
    // The second of two fails: the first is not kept either.
    let broken = Ext(vec![SECOND, BROKEN]);
    for attempt in [
        start(&place, true, &broken).await.map(|()| String::new()),
        admin::migrate(&place.settings(true), &broken).await,
    ] {
        let failed = attempt.unwrap_err();
        let said = format!("{failed:#}");
        assert!(said.contains("extension migration 2 failed"), "{said}");
        assert!(place.recorded().await.is_empty());
        assert!(
            place
                .sql("INSERT INTO ext_audit (id, what) VALUES ('a', 'b')")
                .await
                .is_err(),
            "the first migration's table was kept"
        );
        assert!(
            place
                .sql("INSERT INTO ext_more (id) VALUES (1)")
                .await
                .is_err()
        );
    }
    // The server's own tables are as they were, and a mended extension goes in whole.
    place
        .sql("INSERT INTO orgs (id, name, created_at) VALUES ('o1', 'Org', 1.5)")
        .await
        .unwrap();
    start(&place, true, &Ext(vec![SECOND, THIRD]))
        .await
        .unwrap();
    assert_eq!(place.recorded().await, vec![1, 2]);
}

#[tokio::test]
async fn a_database_an_extension_cannot_stand_on_is_refused() {
    let place = Place::new().await;
    start(&place, true, &Ext(vec![FIRST, SECOND]))
        .await
        .unwrap();

    // An applied migration was edited.
    let edited = "CREATE TABLE ext_audit (id VARCHAR(64) NOT NULL PRIMARY KEY, what TEXT NOT NULL)";
    for auto_migrate in [true, false] {
        assert_eq!(
            schema_error(start(&place, auto_migrate, &Ext(vec![FIRST, edited])).await),
            SchemaError::ExtensionChanged { version: 2 }
        );
    }
    assert_eq!(
        schema_error(admin::migrate(&place.settings(true), &Ext(vec![FIRST, edited, THIRD])).await),
        SchemaError::ExtensionChanged { version: 2 }
    );
    assert_eq!(
        schema_error(admin::db_status(&place.settings(true), &Ext(vec![edited])).await),
        SchemaError::ExtensionChanged { version: 1 }
    );

    // An older version of the extension than the one that wrote the database.
    let older = Ext(vec![FIRST]);
    let refused = schema_error(start(&place, true, &older).await);
    assert_eq!(
        refused,
        SchemaError::ExtensionNewer {
            applied: 2,
            known: 1
        }
    );
    assert!(refused.to_string().contains("newer version"), "{refused}");
    assert_eq!(
        schema_error(admin::migrate(&place.settings(true), &older).await),
        SchemaError::ExtensionNewer {
            applied: 2,
            known: 1
        }
    );
    // The server without any extension takes the database as it is, and says what it found.
    start(&place, false, &NoExtensions).await.unwrap();
    assert_eq!(
        admin::db_status(&place.settings(false), &NoExtensions)
            .await
            .unwrap(),
        "database at 1; this server needs 1; extension migrations at 2, this server needs 0"
    );

    // A record with a gap, or one somebody wrote by hand.
    place
        .sql("DELETE FROM extension_migrations WHERE version = 1")
        .await
        .unwrap();
    assert_eq!(
        schema_error(start(&place, true, &Ext(vec![FIRST, SECOND])).await),
        SchemaError::ExtensionRecord
    );
    assert_eq!(
        place.recorded().await,
        vec![2],
        "nothing was applied on top of it"
    );
    place.sql("DELETE FROM extension_migrations").await.unwrap();
    place
        .sql("INSERT INTO extension_migrations (version, checksum, applied_at) VALUES (1, 'x', 0)")
        .await
        .unwrap();
    assert_eq!(
        schema_error(start(&place, true, &Ext(vec![FIRST])).await),
        SchemaError::ExtensionChanged { version: 1 }
    );
    for bad in [0, -1, 7] {
        place.sql("DELETE FROM extension_migrations").await.unwrap();
        place
            .sql(&format!(
                "INSERT INTO extension_migrations (version, checksum, applied_at) VALUES ({bad}, \
                 '{}', 0)",
                ExtensionSchema::checksum(FIRST)
            ))
            .await
            .unwrap();
        assert_eq!(
            schema_error(start(&place, true, &Ext(vec![FIRST])).await),
            SchemaError::ExtensionRecord,
            "{bad}"
        );
    }
}

#[test]
fn the_extension_record_is_compared_place_by_place() {
    let sum = |m: &str| ExtensionSchema::checksum(m);
    let had = |n: usize| -> Vec<(i64, String)> {
        [FIRST, SECOND, THIRD][..n]
            .iter()
            .enumerate()
            .map(|(i, m)| (i as i64 + 1, sum(m)))
            .collect()
    };
    assert_eq!(sum("").len(), 64);
    assert_ne!(sum(FIRST), sum(SECOND));
    let status = ExtensionSchema::compare(&had(1), &[FIRST, SECOND, THIRD]).unwrap();
    assert_eq!((status.applied, status.known), (1, 3));
    assert_eq!((status.pending(), status.unknown()), (2, 0));
    let status = ExtensionSchema::compare(&had(3), &[FIRST]).unwrap();
    assert_eq!((status.pending(), status.unknown()), (0, 2));
    let status = ExtensionSchema::compare(&[], &[]).unwrap();
    assert_eq!((status.pending(), status.unknown()), (0, 0));
    assert_eq!(
        ExtensionSchema::compare(&had(2), &[SECOND, FIRST]),
        Err(SchemaError::ExtensionChanged { version: 1 })
    );
    assert_eq!(
        ExtensionSchema::compare(&[(2, sum(SECOND))], &[FIRST, SECOND]),
        Err(SchemaError::ExtensionRecord)
    );
    assert_eq!(
        ExtensionSchema::compare(&[(1, sum(FIRST)), (1, sum(FIRST))], &[FIRST, SECOND]),
        Err(SchemaError::ExtensionRecord)
    );
}

#[tokio::test]
async fn a_server_built_from_settings_starts_with_its_extensions_tables() {
    let place = Place::new().await;
    let ext = Arc::new(Ext(vec![FIRST, SECOND]));
    let state = AppState::from_settings_with(place.settings(true), ext.clone())
        .await
        .unwrap();
    assert_eq!(state.ext.migrations(Dialect::Sqlite).len(), 2);
    assert_eq!(state.store.user_count().await.unwrap(), 0);
    state.shutdown().await;
    assert_eq!(place.recorded().await, vec![1, 2]);
    // An existing database and a server that may not upgrade it: it does not start.
    let later = Arc::new(Ext(vec![FIRST, SECOND, THIRD]));
    let Err(refused) = AppState::from_settings_with(place.settings(false), later).await else {
        panic!("the server started")
    };
    assert_eq!(
        refused.downcast::<SchemaError>().unwrap(),
        SchemaError::ExtensionOutdated {
            applied: 2,
            known: 3
        }
    );
}

// ------------------------------------------------------------------ accounts

const PASSWORD: &str = "a long enough password";

#[tokio::test]
async fn create_admin_makes_the_account_or_takes_an_existing_one_over() {
    let place = Place::new().await;
    let settings = place.settings(true);
    let none = NoExtensions;
    // On a database that does not exist yet: it is built.
    let said = admin::create_admin(&settings, &none, " Ada@Example.COM ", None, PASSWORD)
        .await
        .unwrap();
    assert_eq!(said, "created the server admin ada@example.com");
    assert!(!said.contains(PASSWORD));
    let db = Db::connect(&settings).await.unwrap();
    let store = db.store();
    let ada = store
        .user_by_email("ada@example.com")
        .await
        .unwrap()
        .unwrap();
    assert!(ada.is_server_admin && ada.email_verified && ada.active());
    assert_eq!(ada.name, "ada");
    assert!(verify_password(PASSWORD, ada.password_hash.as_deref().unwrap()).await);
    assert_eq!(store.user_count().await.unwrap(), 1);

    // An account that exists (an ordinary one, signed in, with a reset link out): the password
    // is replaced, it becomes an admin, and its sessions and reset links end.
    let bob_id = new_id();
    let mut tx = store.begin().await.unwrap();
    tx.insert_user(&oswx_server::store::User {
        id: bob_id.clone(),
        email: "bob@example.com".into(),
        name: "Bob B".into(),
        password_hash: Some("not a hash".into()),
        is_server_admin: false,
        disabled: false,
        email_verified: false,
        created_at: 5.0,
        deleted_at: None,
    })
    .await
    .unwrap();
    tx.insert_session(&Session {
        token_hash: "h".repeat(64),
        user_id: bob_id.clone(),
        created_at: 5.0,
        expires_at: 4e9,
        label: String::new(),
    })
    .await
    .unwrap();
    tx.insert_password_reset(&oswx_server::store::PasswordReset {
        token_hash: "r".repeat(64),
        user_id: bob_id.clone(),
        expires_at: 4e9,
    })
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let said = admin::create_admin(
        &settings,
        &none,
        "bob@example.com",
        Some("Robert"),
        PASSWORD,
    )
    .await
    .unwrap();
    assert_eq!(
        said,
        "bob@example.com is now a server admin; its password is set and its sessions are ended"
    );
    let bob = store.user_by_id(&bob_id).await.unwrap().unwrap();
    assert!(bob.is_server_admin);
    assert_eq!(bob.name, "Bob B", "an existing account keeps its name");
    assert!(!bob.email_verified, "and what else it had");
    assert!(verify_password(PASSWORD, bob.password_hash.as_deref().unwrap()).await);
    assert!(store.user_sessions(&bob_id).await.unwrap().is_empty());
    assert!(
        store
            .session_by_hash(&"h".repeat(64))
            .await
            .unwrap()
            .is_none()
    );
    // Ada was not touched.
    assert_eq!(
        store.user_by_email("ada@example.com").await.unwrap(),
        Some(ada)
    );
    assert_eq!(store.user_count().await.unwrap(), 2);
    db.close().await;

    // A name of the operator's choosing.
    admin::create_admin(
        &settings,
        &none,
        "cy@example.com",
        Some("  Cy Young "),
        PASSWORD,
    )
    .await
    .unwrap();
    let db = Db::connect(&settings).await.unwrap();
    let cy = db
        .store()
        .user_by_email("cy@example.com")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cy.name, "Cy Young");
    db.close().await;
}

#[tokio::test]
async fn reset_password_sets_it_and_ends_the_sessions_of_an_account_that_exists() {
    // A running server with an account signed in.
    let server = TestServer::start().await;
    let (token, id) = server.register("ada@example.com").await;
    let settings = (*server.state.settings).clone();
    let none = NoExtensions;

    let unknown = admin::reset_password(&settings, &none, "nobody@example.com", PASSWORD)
        .await
        .unwrap_err();
    assert_eq!(
        unknown.to_string(),
        "no account with the address nobody@example.com"
    );

    let said = admin::reset_password(&settings, &none, "ADA@example.com", PASSWORD)
        .await
        .unwrap();
    assert_eq!(
        said,
        "the password of ada@example.com is set; its sessions are ended"
    );
    assert!(
        server
            .state
            .store
            .user_sessions(&id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        server
            .state
            .store
            .session_by_hash(&oswx_server::security::token_hash(&token))
            .await
            .unwrap()
            .is_none()
    );
    // The new password signs in; the old one does not.
    let (status, _) = server
        .post(
            "/api/auth/login",
            None,
            json!({"email": "ada@example.com", "password": common::PASSWORD}),
        )
        .await;
    assert_eq!(status, 401);
    let again = server.login("ada@example.com", PASSWORD).await;
    assert_eq!(server.get("/api/me", Some(&again)).await.0, 200);
}

#[tokio::test]
async fn the_account_commands_refuse_what_is_not_an_address_or_a_password() {
    let place = Place::new().await;
    let settings = place.settings(true);
    let none = NoExtensions;
    let long = "x".repeat(201);
    for email in [
        "",
        "ada",
        "ada@",
        "@example.com",
        "ada@example",
        "a b@example.com",
        "ada@example.com\0",
    ] {
        for said in [
            admin::create_admin(&settings, &none, email, None, PASSWORD)
                .await
                .unwrap_err(),
            admin::reset_password(&settings, &none, email, PASSWORD)
                .await
                .unwrap_err(),
        ] {
            assert_eq!(said.to_string(), "not an email address", "{email:?}");
        }
    }
    for password in [
        "",
        "short",
        "1234567",
        long.as_str(),
        "with a zero \0 in it",
    ] {
        for said in [
            admin::create_admin(&settings, &none, "ada@example.com", None, password)
                .await
                .unwrap_err(),
            admin::reset_password(&settings, &none, "ada@example.com", password)
                .await
                .unwrap_err(),
        ] {
            let said = said.to_string();
            assert!(said.starts_with("the password must"), "{said}");
            assert!(password.len() < 6 || !said.contains(password), "{said}");
        }
    }
    let said = admin::create_admin(&settings, &none, "ada@example.com", Some(&long), PASSWORD)
        .await
        .unwrap_err();
    assert_eq!(said.to_string(), "the name must be at most 200 characters");
    // Nothing was made by any of that (not even the database, for a bad address or password).
    let db = Db::connect(&settings).await.unwrap();
    assert_eq!(db.status(&none).await.unwrap().0.current, None);
    db.close().await;
}

// ------------------------------------------------------------------ a list for each dialect

/// Only SQLite takes this.
const SQLITE_ONLY: &str =
    "CREATE TABLE ext_counter (id INTEGER PRIMARY KEY AUTOINCREMENT, n BIGINT)";
/// Only PostgreSQL takes this.
const POSTGRES_ONLY: &str =
    "CREATE TABLE ext_counter (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, n BIGINT)";

#[tokio::test]
async fn each_database_gets_the_migrations_written_for_it() {
    let place = Place::new().await;
    let ext = PerDialect(vec![SQLITE_ONLY, SECOND], vec![POSTGRES_ONLY, SECOND]);
    assert_eq!(check_migrations(&ext), Ok(()));
    start(&place, true, &ext).await.unwrap();
    assert_eq!(place.recorded().await, vec![1, 2]);
    place
        .sql("INSERT INTO ext_counter (n) VALUES (5)")
        .await
        .unwrap();
    // The other database's words would not have done: the lists really are apart.
    let other = Place::new().await;
    let swapped = PerDialect(vec![POSTGRES_ONLY, SECOND], vec![SQLITE_ONLY, SECOND]);
    let failed = start(&other, true, &swapped).await.unwrap_err();
    assert!(
        format!("{failed:#}").contains("extension migration 1 failed"),
        "{failed:#}"
    );
    assert!(other.recorded().await.is_empty());
}

#[tokio::test]
async fn an_extension_with_a_different_number_of_migrations_per_dialect_is_refused() {
    let place = Place::new().await;
    // Whichever database this runs on has the right number for itself; the other has one more.
    for uneven in [
        PerDialect(vec![FIRST], vec![FIRST, SECOND]),
        PerDialect(vec![FIRST, SECOND], vec![FIRST]),
    ] {
        let sqlite = uneven.0.len();
        let counts = MigrationsError::Counts {
            sqlite,
            postgres: 3 - sqlite,
        };
        assert_eq!(check_migrations(&uneven), Err(counts.clone()));
        for refused in [
            start(&place, true, &uneven).await.unwrap_err(),
            admin::migrate(&place.settings(true), &uneven)
                .await
                .unwrap_err(),
        ] {
            assert_eq!(refused.downcast::<MigrationsError>().unwrap(), counts);
        }
    }
    // Nothing was applied by any of that.
    start(&place, true, &NoExtensions).await.unwrap();
    assert!(place.recorded().await.is_empty());
}

#[tokio::test]
async fn an_extension_whose_dialects_have_different_steps_is_refused_before_anything_is_opened() {
    // The same number of migrations; the steps are not the same at one place.
    let crossed = Named(
        vec![("seats", FIRST), ("audit", SECOND)],
        vec![("audit", SECOND), ("seats", FIRST)],
    );
    let twice = Named(
        vec![("seats", FIRST), ("seats", SECOND)],
        vec![("seats", FIRST), ("seats", SECOND)],
    );
    let uneven = PerDialect(vec![FIRST], vec![FIRST, SECOND]);
    let place = Place::new().await;
    for (ext, expected) in [
        (
            &crossed as &dyn Extensions,
            MigrationsError::Ids {
                version: 1,
                sqlite: "seats",
                postgres: "audit",
            },
        ),
        (&twice, MigrationsError::Repeated { id: "seats" }),
        (
            &uneven,
            MigrationsError::Counts {
                sqlite: 1,
                postgres: 2,
            },
        ),
    ] {
        assert_eq!(check_migrations(ext), Err(expected.clone()));
        // A database nobody can reach: the check comes first, so that is not what is said ...
        let mut nowhere = place.settings(true);
        nowhere.database_url = "postgres://user:pass@127.0.0.1:1/nothing".into();
        for refused in [
            admin::migrate(&nowhere, ext).await.unwrap_err(),
            admin::db_status(&nowhere, ext).await.unwrap_err(),
            Db::open(&nowhere, ext).await.unwrap_err(),
        ] {
            assert_eq!(refused.downcast::<MigrationsError>().unwrap(), expected);
        }
        // ... and on SQLite nothing is made: no file, no folder.
        for refused in [
            admin::migrate(&place.settings(true), ext)
                .await
                .unwrap_err(),
            start(&place, true, ext).await.unwrap_err(),
        ] {
            assert_eq!(refused.downcast::<MigrationsError>().unwrap(), expected);
        }
        if !on_postgres() {
            let left: Vec<_> = std::fs::read_dir(place.dir.path()).unwrap().collect();
            assert!(left.is_empty(), "{left:?}");
        }
    }
    // The steps the same in both, under ids of their own: taken.
    let named = Named(
        vec![("0001_seats", FIRST), ("0002_audit", SECOND)],
        vec![("0001_seats", FIRST), ("0002_audit", SECOND)],
    );
    start(&place, true, &named).await.unwrap();
    assert_eq!(place.recorded().await, vec![1, 2]);
}
