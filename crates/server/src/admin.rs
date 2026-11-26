//! What the operator's commands do (`openswx-collab-server migrate`, `db-status`,
//! `create-admin`, `reset-password`), and how a database is opened for a server with extensions.
//!
//! Each command answers the line to print, or the error to end with. They work on the database
//! directly, beside a server that may be running: a password set here takes effect at once for
//! sign-ins, and sessions ended here stop working when the running server next looks them up
//! (its cache keeps a session for a short while; `access_recheck` closes open documents).
//!
//! # Extension migrations
//!
//! An extension's migrations ([`Extensions::migrations`]) are applied in its order **after** the
//! server's own, by `migrate` and when a server starts ([`Db::open`], as the server's own are:
//! always to a database that was empty, to an older one only with `auto_migrate`), and are
//! recorded in the table `extension_migrations`. `db-status` shows them. A server whose extension
//! carries fewer than the database has had refuses it, as one whose own migrations are older
//! does; a server with no extension migrations at all (the open-source one) takes it as it is.

use std::sync::Arc;

use anyhow::{Context, bail};

use crate::config::{Database, Settings};
use crate::ext::Extensions;
use crate::security::{PASSWORD_LEN, clean_email, hash_password};
use crate::store::postgres::PgConfig;
use crate::store::{
    ExtensionSchema, PgStore, SchemaError, SchemaStatus, SqliteStore, Store, User, new_id,
};

/// The database the settings name, of either kind, as the commands and the server's start use it.
#[derive(Debug, Clone)]
pub enum Db {
    Sqlite(SqliteStore),
    Postgres(PgStore),
}

impl Db {
    /// Connects without looking at the schema (for `migrate` and `db-status`).
    pub async fn connect(settings: &Settings) -> anyhow::Result<Self> {
        Ok(match settings.database()? {
            Database::Sqlite(_) => {
                Self::Sqlite(SqliteStore::connect(&settings.database_url).await?)
            }
            Database::Postgres(_) => Self::Postgres(
                PgStore::connect(&settings.database_url, PgConfig::from_settings(settings)).await?,
            ),
        })
    }

    /// Opens the database for a server with the extension `ext` and brings its schema up to date
    /// as `settings.auto_migrate` allows: the server's own migrations, then the extension's.
    pub async fn open(settings: &Settings, ext: &dyn Extensions) -> anyhow::Result<Self> {
        let (url, auto, extension) = (
            settings.database_url.as_str(),
            settings.auto_migrate,
            ext.migrations(),
        );
        Ok(match settings.database()? {
            Database::Sqlite(_) => {
                Self::Sqlite(SqliteStore::open_with_extension(url, auto, &extension).await?)
            }
            Database::Postgres(_) => {
                let config = PgConfig::from_settings(settings);
                Self::Postgres(PgStore::open_with_extension(url, auto, config, &extension).await?)
            }
        })
    }

    /// Applies every migration the database has not had: the server's own, then the extension's.
    pub async fn migrate(
        &self,
        ext: &dyn Extensions,
    ) -> anyhow::Result<(SchemaStatus, ExtensionSchema)> {
        let extension = ext.migrations();
        Ok(match self {
            Self::Sqlite(db) => (db.migrate().await?, db.migrate_extension(&extension).await?),
            Self::Postgres(db) => (db.migrate().await?, db.migrate_extension(&extension).await?),
        })
    }

    /// Which migrations the database has had. (The extension's are looked at only once the
    /// server's own schema is there to hold their record.)
    pub async fn status(
        &self,
        ext: &dyn Extensions,
    ) -> anyhow::Result<(SchemaStatus, ExtensionSchema)> {
        let extension = ext.migrations();
        let schema = self.store().schema_status().await?;
        let ours = if schema.current.is_none() {
            ExtensionSchema {
                applied: 0,
                known: extension.len(),
            }
        } else {
            match self {
                Self::Sqlite(db) => db.extension_schema(&extension).await?,
                Self::Postgres(db) => db.extension_schema(&extension).await?,
            }
        };
        Ok((schema, ours))
    }

    /// The database as the server uses it.
    pub fn store(&self) -> Arc<dyn Store> {
        match self {
            Self::Sqlite(db) => Arc::new(db.clone()),
            Self::Postgres(db) => Arc::new(db.clone()),
        }
    }

    /// Closes every connection.
    pub async fn close(&self) {
        match self {
            Self::Sqlite(db) => db.close().await,
            Self::Postgres(db) => db.close().await,
        }
    }
}

fn version(v: Option<i64>) -> String {
    v.map_or_else(|| "none".to_owned(), |v| v.to_string())
}

/// `migrate`: upgrades the schema whatever `auto_migrate` says. Answers `schema A -> B` or
/// `schema B: up to date`, and the same for the extension's migrations if it has any.
pub async fn migrate(settings: &Settings, ext: &dyn Extensions) -> anyhow::Result<String> {
    let db = Db::connect(settings).await?;
    let done = async {
        let (before, ours_before) = db.status(ext).await?;
        let (after, ours) = db.migrate(ext).await?;
        let mut line = if before.current == after.current {
            format!("schema {}: up to date", version(after.current))
        } else {
            format!(
                "schema {} -> {}",
                version(before.current),
                version(after.current)
            )
        };
        if ours.known > 0 || ours.applied > 0 {
            if ours_before.applied == ours.applied {
                line.push_str(&format!(
                    "; extension migrations {}: up to date",
                    ours.applied
                ));
            } else {
                line.push_str(&format!(
                    "; extension migrations {} -> {}",
                    ours_before.applied, ours.applied
                ));
            }
        }
        Ok(line)
    }
    .await;
    db.close().await;
    done
}

/// `db-status`: where the schema is and what this server needs, without changing anything. A
/// database this server could not use (newer, edited, not its own) is an error saying why.
pub async fn db_status(settings: &Settings, ext: &dyn Extensions) -> anyhow::Result<String> {
    let db = Db::connect(settings).await?;
    let told = async {
        let (schema, ours) = db.status(ext).await?;
        let at = match schema.current {
            Some(v) => v.to_string(),
            None => "none (an empty database)".to_owned(),
        };
        if !schema.unknown.is_empty() {
            bail!(SchemaError::Newer {
                unknown: schema.unknown,
                latest: schema.latest,
            });
        }
        let mut line = format!("database at {at}; this server needs {}", schema.latest);
        if ours.known > 0 || ours.applied > 0 {
            line.push_str(&format!(
                "; extension migrations at {}, this server needs {}",
                ours.applied, ours.known
            ));
        }
        Ok(line)
    }
    .await;
    db.close().await;
    told
}

/// A password as a command takes it: 8 to 200 characters.
fn checked_password(password: &str) -> anyhow::Result<()> {
    if !PASSWORD_LEN.contains(&password.chars().count()) {
        bail!(
            "the password must be {} to {} characters",
            PASSWORD_LEN.start(),
            PASSWORD_LEN.end()
        );
    }
    if password.contains('\0') {
        bail!("the password must not contain a zero character");
    }
    Ok(())
}

fn checked_email(email: &str) -> anyhow::Result<String> {
    clean_email(email).map_err(|_| anyhow::anyhow!("not an email address"))
}

/// `create-admin <email> [name]`: makes the account a server admin with this password. A new
/// account is created with its address taken as confirmed and `name` (or the part of the address
/// before the `@`); an existing one gets the password, becomes an admin, and has every session
/// ended and every reset link withdrawn.
pub async fn create_admin(
    settings: &Settings,
    ext: &dyn Extensions,
    email: &str,
    name: Option<&str>,
    password: &str,
) -> anyhow::Result<String> {
    let email = checked_email(email)?;
    checked_password(password)?;
    let name = match name.map(str::trim).filter(|n| !n.is_empty()) {
        Some(name) if name.chars().count() > 200 || name.contains('\0') => {
            bail!("the name must be at most 200 characters")
        }
        Some(name) => name.to_owned(),
        None => email.split('@').next().unwrap_or_default().to_owned(),
    };
    let hash = hash_password(password).await;
    let db = Db::open(settings, ext).await?;
    let done = async {
        let store = db.store();
        let mut tx = store.begin().await?;
        let line = match tx.user_by_email(&email).await? {
            Some(user) => {
                tx.lock_user(&user.id).await?;
                tx.set_user_password(&user.id, Some(&hash)).await?;
                tx.set_user_admin(&user.id, true).await?;
                tx.delete_user_sessions(&user.id).await?;
                tx.delete_user_password_resets(&user.id).await?;
                format!(
                    "{email} is now a server admin; its password is set and its sessions are ended"
                )
            }
            None => {
                tx.insert_user(&User {
                    id: new_id(),
                    email: email.clone(),
                    name,
                    password_hash: Some(hash),
                    is_server_admin: true,
                    disabled: false,
                    email_verified: true,
                    created_at: now(),
                    deleted_at: None,
                })
                .await?;
                format!("created the server admin {email}")
            }
        };
        tx.commit()
            .await
            .context("the change could not be stored")?;
        Ok(line)
    }
    .await;
    db.close().await;
    done
}

/// `reset-password <email>`: sets the account's password, ends every session of it and
/// withdraws every reset link. An error when there is no such account.
pub async fn reset_password(
    settings: &Settings,
    ext: &dyn Extensions,
    email: &str,
    password: &str,
) -> anyhow::Result<String> {
    let email = checked_email(email)?;
    checked_password(password)?;
    let hash = hash_password(password).await;
    let db = Db::open(settings, ext).await?;
    let done = async {
        let store = db.store();
        let mut tx = store.begin().await?;
        let Some(user) = tx.user_by_email(&email).await? else {
            bail!("no account with the address {email}");
        };
        tx.lock_user(&user.id).await?;
        tx.set_user_password(&user.id, Some(&hash)).await?;
        tx.delete_user_sessions(&user.id).await?;
        tx.delete_user_password_resets(&user.id).await?;
        tx.commit()
            .await
            .context("the change could not be stored")?;
        Ok(format!(
            "the password of {email} is set; its sessions are ended"
        ))
    }
    .await;
    db.close().await;
    done
}

fn now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}
