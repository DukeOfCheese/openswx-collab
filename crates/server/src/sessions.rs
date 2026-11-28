//! The in-memory session cache.
//!
//! Every authenticated request turns a bearer token into an account. Asking the database each time
//! would cost two reads per request, so [`Sessions`] remembers, per token hash, whose session it is,
//! when it expires and when the database last confirmed it. A remembered session is trusted for
//! [`TRUST_SECS`] without asking again; after that the database is asked (session row, account
//! neither disabled nor deleted) and the entry renewed.
//!
//! Whatever ends sessions on this node calls [`Sessions::forget_token`] or [`Sessions::forget_user`]
//! after its database commit (and then closes the document connections of those sessions through
//! the hub), so a revoked session stops working at once here; another node (none
//! yet: the server is one process) would notice within [`TRUST_SECS`]. An expired session stops at
//! its expiry whatever the cache holds.
//!
//! A lookup that read the database before a `forget` and finishes after it does not put its stale
//! answer back: every `forget` moves a generation counter, and an answer is remembered only if the
//! generation did not move while it was being fetched.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::store::{Id, Store};

/// How long a remembered session is trusted without asking the database, in seconds.
pub const TRUST_SECS: f64 = 30.0;

/// The most sessions remembered; past it the oldest are forgotten.
pub const MAX_ENTRIES: usize = 100_000;

/// A live session, as [`Sessions::session`] answers it.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveSession {
    pub user_id: Id,
    /// When it expires, in the clock's epoch seconds.
    pub expires_at: f64,
}

#[derive(Debug, Clone)]
struct Entry {
    user_id: Id,
    expires_at: f64,
    checked_at: f64,
}

#[derive(Debug, Default)]
struct Inner {
    entries: HashMap<String, Entry>,
    /// Moves on every forget; see the module documentation.
    generation: u64,
}

/// The session cache. Cloning is cheap: clones share the entries.
#[derive(Debug, Clone)]
pub struct Sessions {
    inner: Arc<Mutex<Inner>>,
    capacity: usize,
}

impl Default for Sessions {
    fn default() -> Self {
        Self::with_capacity(MAX_ENTRIES)
    }
}

impl Sessions {
    /// A cache that remembers at most `capacity` sessions (tests use a small one).
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::default(),
            capacity: capacity.max(1),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The account whose live session has this token hash at time `now`, or `None` when there is no
    /// such session, it has expired, or its account is gone, disabled or deleted.
    pub async fn get(
        &self,
        store: &dyn Store,
        token_hash: &str,
        now: f64,
    ) -> anyhow::Result<Option<Id>> {
        Ok(self
            .session(store, token_hash, now)
            .await?
            .map(|s| s.user_id))
    }

    /// [`Sessions::get`], with when the session expires (a document connection is closed then).
    pub async fn session(
        &self,
        store: &dyn Store,
        token_hash: &str,
        now: f64,
    ) -> anyhow::Result<Option<LiveSession>> {
        let generation = {
            let mut inner = self.lock();
            if let Some(e) = inner.entries.get(token_hash) {
                if e.expires_at < now {
                    inner.entries.remove(token_hash);
                    return Ok(None);
                }
                if now - e.checked_at < TRUST_SECS {
                    return Ok(Some(LiveSession {
                        user_id: e.user_id.clone(),
                        expires_at: e.expires_at,
                    }));
                }
            }
            inner.generation
        };
        let found = match store.session_by_hash(token_hash).await? {
            Some(s) if s.expires_at >= now => match store.user_by_id(&s.user_id).await? {
                Some(u) if u.active() => Some(Entry {
                    user_id: u.id,
                    expires_at: s.expires_at,
                    checked_at: now,
                }),
                _ => None,
            },
            _ => None,
        };
        let mut inner = self.lock();
        let Some(entry) = found else {
            inner.entries.remove(token_hash);
            return Ok(None);
        };
        let live = LiveSession {
            user_id: entry.user_id.clone(),
            expires_at: entry.expires_at,
        };
        if inner.generation == generation {
            self.remember(&mut inner, token_hash, entry);
        }
        Ok(Some(live))
    }

    fn remember(&self, inner: &mut Inner, token_hash: &str, entry: Entry) {
        if !inner.entries.contains_key(token_hash) && inner.entries.len() >= self.capacity {
            make_room(&mut inner.entries, self.capacity, entry.checked_at);
        }
        inner.entries.insert(token_hash.to_owned(), entry);
    }

    /// Forgets one session (it was ended).
    pub fn forget_token(&self, token_hash: &str) {
        let mut inner = self.lock();
        inner.generation += 1;
        inner.entries.remove(token_hash);
    }

    /// Forgets every session of an account (its password changed, it was reset, disabled or
    /// deleted).
    pub fn forget_user(&self, user_id: &str) {
        let mut inner = self.lock();
        inner.generation += 1;
        inner.entries.retain(|_, e| e.user_id != user_id);
    }

    /// How many sessions are remembered.
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Whether no session is remembered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Frees space for one more entry: first every entry too old to be trusted anyway, then, if that was
/// not enough, the oldest tenth (at least one).
fn make_room(entries: &mut HashMap<String, Entry>, capacity: usize, now: f64) {
    entries.retain(|_, e| now - e.checked_at < TRUST_SECS && e.expires_at >= now);
    if entries.len() < capacity {
        return;
    }
    let mut by_age: Vec<(f64, String)> = entries
        .iter()
        .map(|(k, e)| (e.checked_at, k.clone()))
        .collect();
    by_age.sort_by(|a, b| a.0.total_cmp(&b.0));
    for (_, key) in by_age.into_iter().take((capacity / 10).max(1)) {
        entries.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(user: &str, checked_at: f64) -> Entry {
        Entry {
            user_id: user.into(),
            expires_at: 1e12,
            checked_at,
        }
    }

    #[test]
    fn the_cache_is_bounded_and_forgets_the_oldest_first() {
        let sessions = Sessions::with_capacity(10);
        {
            let mut inner = sessions.lock();
            for i in 0..10 {
                sessions.remember(&mut inner, &format!("t{i}"), entry("u", 100.0 + i as f64));
            }
            sessions.remember(&mut inner, "new", entry("u", 110.0));
        }
        assert_eq!(sessions.len(), 10);
        let inner = sessions.lock();
        assert!(!inner.entries.contains_key("t0"), "the oldest went");
        assert!(inner.entries.contains_key("t1") && inner.entries.contains_key("new"));
    }

    #[test]
    fn entries_too_old_to_trust_go_before_any_fresh_one() {
        let sessions = Sessions::with_capacity(4);
        {
            let mut inner = sessions.lock();
            sessions.remember(&mut inner, "stale", entry("u", 0.0));
            for i in 0..3 {
                sessions.remember(&mut inner, &format!("t{i}"), entry("u", 100.0));
            }
            sessions.remember(&mut inner, "new", entry("u", 101.0));
        }
        let inner = sessions.lock();
        assert_eq!(inner.entries.len(), 4);
        assert!(!inner.entries.contains_key("stale"));
    }

    #[test]
    fn forgetting_moves_the_generation_and_drops_entries() {
        let sessions = Sessions::default();
        {
            let mut inner = sessions.lock();
            sessions.remember(&mut inner, "a", entry("u1", 1.0));
            sessions.remember(&mut inner, "b", entry("u1", 1.0));
            sessions.remember(&mut inner, "c", entry("u2", 1.0));
        }
        sessions.forget_token("c");
        assert_eq!(sessions.len(), 2);
        sessions.forget_user("u1");
        assert!(sessions.is_empty());
        assert_eq!(sessions.lock().generation, 2);
    }
}
