//! The database, behind one trait so SQLite and PostgreSQL share the handlers.
//!
//! Placeholder: Task 2 adds the SQLite store and the transaction helper; each later task adds the
//! methods (one per query) and row types (plain structs) it needs.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;

/// A database id: 32 lower-case hex characters.
pub type Id = String;

#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// Answers when the database does (`GET /api/ready`).
    async fn health(&self) -> anyhow::Result<()>;

    /// How many accounts exist, deleted and disabled ones included (the registration mode).
    async fn user_count(&self) -> anyhow::Result<u64>;
}

/// A store held in memory, for tests: it can be told to report itself unhealthy, and to report a
/// number of users.
#[derive(Debug, Default)]
pub struct MemoryStore {
    unhealthy: AtomicBool,
    users: AtomicU64,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes `health` fail (`false`) or succeed (`true`).
    pub fn set_healthy(&self, healthy: bool) {
        self.unhealthy.store(!healthy, Ordering::SeqCst);
    }

    /// Sets what `user_count` answers.
    pub fn set_user_count(&self, n: u64) {
        self.users.store(n, Ordering::SeqCst);
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn health(&self) -> anyhow::Result<()> {
        if self.unhealthy.load(Ordering::SeqCst) {
            anyhow::bail!("the memory store was told to be unhealthy");
        }
        Ok(())
    }

    async fn user_count(&self) -> anyhow::Result<u64> {
        Ok(self.users.load(Ordering::SeqCst))
    }
}
