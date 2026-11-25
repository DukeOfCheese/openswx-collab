//! Locks: advisory leases on document paths ("Ada is editing this sketch") that also stop other
//! clients' changes to what they cover. They live in the room's memory only (a restart or an unload
//! forgets them; clients take them again).
//!
//! # Which paths
//!
//! A path is lockable when it resolves in the durable document (a map key or a keyed-list item at
//! any depth, or a list itself), or when it is a single top-level key, which need not exist yet (a
//! client may lock a key it is about to create). Not lockable: a path with a segment starting `~`
//! (derived data), one under a key the server owns (`oswx_core::PROTECTED`), one of more than
//! `MAX_DEPTH` segments.
//!
//! # Acquiring
//!
//! `lock.acquire` (at most 256 paths, the codec's limit) is answered `lock.result`, path by path:
//! a path the client already holds is granted; otherwise it is denied, with a reason, when the
//! connection may only look (`read_only`), the path is not lockable (`not_lockable`), a lock of
//! another client is in the way (`held`, naming it), or the client ([`MAX_LOCKS`]), its user
//! ([`MAX_USER_LOCKS`]) or the room ([`MAX_ROOM_LOCKS`]) holds as many as it may (`quota`).
//! A lock is in the way when it is **at, under or above** the path, so two clients never hold
//! overlapping locks (one client may hold nested ones), with one exception: a lock on a list itself
//! and locks on its items do not exclude each other (one person may hold `["features"]` to reorder
//! or delete while others hold single features; a lock deeper than an item still conflicts with
//! the list's). Newly granted locks are announced to everyone in `lock.changed`. `lock.release`
//! releases the sender's own locks among those named (no answer).
//!
//! Lock requests are paid for per path from a bucket per connection ([`LOCK_BURST`] tokens,
//! refilling at [`LOCK_RATE`] a second, starting full), acquire and release alike. An acquire the
//! bucket cannot pay for is denied in full (`rate`) and changes nothing. A release is always
//! honoured (a client must always be able to give its locks up), but one the bucket cannot pay for
//! is announced late: what a connection releases beyond its allowance is announced at most once per
//! [`LOCK_NEWS_EVERY`], as one `lock.changed` with the net result.
//!
//! # What a lock stops
//!
//! A change by another client (a client's own locks never stop it) is refused `locked` when one of
//! its writes (`oswx_core::writes`) touches a locked path: `At(p)` touches a lock at `p`, above `p`
//! or under `p`; `Position(list, id)` (a move) a lock at `list + id` or above it. A lock on a list
//! itself (`["features"]`) is narrower: it stops writing the list or above it, and the insertion,
//! removal, replacement or move of its items (`At(list + id)`, `Position(list, id)`), but not edits
//! inside an item (lock the item for that). The refusal's conflicts name each lock in the way
//! (kind `locked`, `by` its holder's client id, seq 0). Changes the server makes are stopped only
//! as their [`super::LockRule`] says. With no lock in the room the check costs one comparison; with
//! some, it reads indexes kept up to date as locks come and go.
//!
//! # Expiry
//!
//! Locks expire on every sweep (the room's timer), before every `lock.acquire`, before every change
//! is judged and before a client's locks are taken up again at a join: an attached lock dies when
//! its client's connection has been silent (no frame at all) for `lock_ttl`; when the connection
//! goes, its locks are detached and die after `lock_grace` unless the same client id comes back,
//! when they are attached again (or released, if it now may only look). A second connection of the
//! same client (the 4409 takeover) keeps them attached. When a change becomes durable, the locks at
//! or under what it wrote or removed whose path no longer resolves are dropped. Every drop is
//! announced in `lock.changed`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use oswx_core::wire::{DenyReason, Lock, LockDenied, ServerMsg};
use oswx_core::{
    ClientId, Conflict, ConflictKind, MAX_DEPTH, PROTECTED, Path, Value, Write, writes,
};

use super::change::Item;
use super::{ConnId, Internal, LockRule, Room};
use crate::metrics;
use crate::store::Id;

/// The most locks one client holds.
pub const MAX_LOCKS: usize = 256;
/// The most locks one user holds, all their clients together.
pub const MAX_USER_LOCKS: usize = 1024;
/// The most locks a room holds.
pub const MAX_ROOM_LOCKS: usize = 8192;
/// A connection's lock allowance: path-tokens when full.
pub const LOCK_BURST: f64 = 512.0;
/// Path-tokens a connection's lock allowance gains a second.
pub const LOCK_RATE: f64 = 64.0;
/// How often releases beyond the allowance are announced, at most, per connection.
pub const LOCK_NEWS_EVERY: Duration = Duration::from_millis(100);
/// The most conflicts a `locked` refusal lists.
const MAX_CONFLICTS: usize = 64;
/// The longest `message` of a `locked` refusal.
const MAX_MESSAGE: usize = 300;

/// Who holds a lock.
#[derive(Debug, Clone)]
pub(super) struct Holder {
    client: ClientId,
    user: Id,
    name: String,
    /// The path was a list when it was locked (see the module documentation).
    list: bool,
}

/// The locks of a room, with the indexes that keep every question about them cheap.
#[derive(Debug, Default)]
pub(super) struct Locks {
    by_path: BTreeMap<Path, Holder>,
    /// Each client's locked paths.
    by_client: HashMap<ClientId, BTreeSet<Path>>,
    /// How many locks each user holds.
    by_user: HashMap<Id, usize>,
    /// How many locks start with each first segment.
    firsts: HashMap<Arc<str>, usize>,
    /// Clients whose connection went, and since when: their locks are detached.
    detached: HashMap<ClientId, f64>,
    /// Releases waiting to be announced, per connection (see the module documentation).
    news: HashMap<ConnId, BTreeSet<Path>>,
}

/// Whether `path` may be locked in `doc`: `Some(is a list)`, or `None`.
pub(super) fn lockable(doc: &Value, path: &Path) -> Option<bool> {
    let first = path.first()?;
    if path.len() > MAX_DEPTH
        || PROTECTED.contains(&&**first)
        || path.iter().any(|s| s.starts_with('~'))
    {
        return None;
    }
    let segments: Vec<&str> = path.iter().map(|s| &**s).collect();
    match doc.at(&segments) {
        Some(v) => Some(matches!(v, Value::Keyed(_) | Value::List(_))),
        None if path.len() == 1 => Some(false),
        None => None,
    }
}

/// Whether a write at `p` (`position`: an item's place, `p` being the item) touches a lock at
/// `lock`.
fn touches(lock: &[Arc<str>], holder: &Holder, p: &[Arc<str>], position: bool) -> bool {
    if p.starts_with(lock) {
        // At or under the lock; under a list's lock, only its items themselves.
        !holder.list || p.len() <= lock.len() + 1
    } else {
        !position && lock.starts_with(p)
    }
}

impl Locks {
    pub(super) fn len(&self) -> usize {
        self.by_path.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.by_path.is_empty()
    }

    /// How many locks are detached.
    pub(super) fn detached_count(&self) -> usize {
        self.detached
            .keys()
            .map(|c| self.by_client.get(c).map_or(0, BTreeSet::len))
            .sum()
    }

    fn wire(&self, path: &Path, h: &Holder) -> Lock {
        Lock {
            path: path.clone(),
            client_id: h.client.clone(),
            user_id: h.user.clone(),
            name: h.name.clone(),
            detached: self.detached.contains_key(&h.client),
        }
    }

    /// Every lock.
    pub(super) fn all(&self) -> Vec<Lock> {
        self.by_path.iter().map(|(p, h)| self.wire(p, h)).collect()
    }

    fn of(&self, client: &ClientId) -> Vec<Path> {
        self.by_client
            .get(client)
            .map(|paths| paths.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn wire_of(&self, client: &ClientId) -> Vec<Lock> {
        self.of(client)
            .iter()
            .filter_map(|p| self.by_path.get(p).map(|h| self.wire(p, h)))
            .collect()
    }

    fn holds(&self, path: &Path, client: &ClientId) -> bool {
        self.by_path.get(path).is_some_and(|h| h.client == *client)
    }

    /// Whether `holder` may take one more lock here.
    fn has_room(&self, holder: &Holder) -> bool {
        self.by_path.len() < MAX_ROOM_LOCKS
            && self.by_client.get(&holder.client).map_or(0, BTreeSet::len) < MAX_LOCKS
            && self.by_user.get(&holder.user).copied().unwrap_or(0) < MAX_USER_LOCKS
    }

    fn insert(&mut self, path: Path, holder: Holder) {
        *self.firsts.entry(path[0].clone()).or_insert(0) += 1;
        *self.by_user.entry(holder.user.clone()).or_insert(0) += 1;
        self.by_client
            .entry(holder.client.clone())
            .or_default()
            .insert(path.clone());
        self.by_path.insert(path, holder);
    }

    fn remove(&mut self, path: &Path) -> Option<Holder> {
        let h = self.by_path.remove(path)?;
        if let Some(n) = self.firsts.get_mut(&path[0]) {
            *n -= 1;
            if *n == 0 {
                self.firsts.remove(&path[0]);
            }
        }
        if let Some(n) = self.by_user.get_mut(&h.user) {
            *n -= 1;
            if *n == 0 {
                self.by_user.remove(&h.user);
            }
        }
        if let Some(paths) = self.by_client.get_mut(&h.client) {
            paths.remove(path);
            if paths.is_empty() {
                self.by_client.remove(&h.client);
                self.detached.remove(&h.client);
            }
        }
        Some(h)
    }

    /// Releases every lock of `client`; answers their paths.
    pub(super) fn release_client(&mut self, client: &ClientId) -> Vec<Path> {
        let paths = self.of(client);
        for p in &paths {
            self.remove(p);
        }
        paths
    }

    /// Marks the client's locks detached (from `now`); answers them, if it had any attached.
    pub(super) fn detach(&mut self, client: &ClientId, now: f64) -> Vec<Lock> {
        if !self.by_client.contains_key(client) || self.detached.contains_key(client) {
            return Vec::new();
        }
        self.detached.insert(client.clone(), now);
        self.wire_of(client)
    }

    /// Attaches the client's detached locks again; answers them, if there were any.
    pub(super) fn attach(&mut self, client: &ClientId) -> Vec<Lock> {
        if self.detached.remove(client).is_none() {
            return Vec::new();
        }
        self.wire_of(client)
    }

    /// The first lock of another client in the way of `path` (`list`: the path is a list).
    fn in_the_way(&self, path: &Path, list: bool, client: &ClientId) -> Option<Lock> {
        for i in 1..=path.len() {
            if let Some((p, h)) = self.by_path.get_key_value(&path[..i])
                && h.client != *client
                // A list's lock and its items' locks do not exclude each other.
                && !(h.list && i + 1 == path.len())
            {
                return Some(self.wire(p, h));
            }
        }
        self.under(path)
            .find(|(p, h)| h.client != *client && !(list && p.len() == path.len() + 1))
            .map(|(p, h)| self.wire(p, h))
    }

    /// The locks strictly under `path`.
    fn under<'a>(&'a self, path: &'a [Arc<str>]) -> impl Iterator<Item = (&'a Path, &'a Holder)> {
        self.by_path
            .range::<[Arc<str>], _>((Bound::Excluded(path), Bound::Unbounded))
            .take_while(move |(p, _)| p.starts_with(path))
    }

    /// The locks the writes touch whose holder `blocks` says count, at most [`MAX_CONFLICTS`].
    fn touched(
        &self,
        writes: &BTreeSet<Write>,
        blocks: impl Fn(&Holder) -> bool,
    ) -> Vec<(Path, Holder)> {
        let mut hit: BTreeMap<Path, Holder> = BTreeMap::new();
        for w in writes {
            if hit.len() >= MAX_CONFLICTS {
                break;
            }
            let (p, position): (std::borrow::Cow<'_, [Arc<str>]>, bool) = match w {
                Write::At(p) => (p.as_slice().into(), false),
                Write::Position(list, id) => {
                    let mut item = list.clone();
                    item.push(id.clone());
                    (item.into(), true)
                }
            };
            if !p.first().is_some_and(|s| self.firsts.contains_key(s)) {
                continue;
            }
            for i in 1..=p.len() {
                if let Some((lp, h)) = self.by_path.get_key_value(&p[..i])
                    && blocks(h)
                    && touches(lp, h, &p, position)
                {
                    hit.insert(lp.clone(), h.clone());
                }
            }
            if !position {
                for (lp, h) in self.under(&p) {
                    if blocks(h) {
                        hit.insert(lp.clone(), h.clone());
                    }
                }
            }
        }
        hit.into_iter().take(MAX_CONFLICTS).collect()
    }

    /// The locks of users other than `user`, at most [`MAX_CONFLICTS`].
    fn of_other_users(&self, user: &str) -> Vec<(Path, Holder)> {
        self.by_path
            .iter()
            .filter(|(_, h)| h.user != user)
            .take(MAX_CONFLICTS)
            .map(|(p, h)| (p.clone(), h.clone()))
            .collect()
    }

    /// Drops the locks (of more than one segment) at or under `roots` whose path is not in `doc`;
    /// answers their paths.
    fn drop_unresolved_under<'a>(
        &mut self,
        roots: impl Iterator<Item = &'a Path>,
        doc: &Value,
    ) -> Vec<Path> {
        let mut gone = BTreeSet::new();
        for root in roots {
            if !self.firsts.contains_key(&root[0]) {
                continue;
            }
            let here = self.by_path.get_key_value(root.as_slice());
            for (p, _) in here.into_iter().chain(self.under(root)) {
                if p.len() > 1 && lockable(doc, p).is_none() {
                    gone.insert(p.clone());
                }
            }
        }
        for p in &gone {
            self.remove(p);
        }
        gone.into_iter().collect()
    }
}

/// What a client sees of a `locked` refusal: the conflicts and the message.
fn refusal(hit: &[(Path, Holder)]) -> (Vec<Conflict>, String) {
    let conflicts = hit
        .iter()
        .map(|(p, h)| Conflict {
            path: p.clone(),
            seq: 0,
            by: h.client.clone(),
            kind: ConflictKind::Locked,
        })
        .collect();
    let names: BTreeSet<&str> = hit.iter().map(|(_, h)| h.name.as_str()).collect();
    let names: Vec<&str> = names.into_iter().collect();
    let message = format!("being edited by {}", names.join(", "))
        .chars()
        .take(MAX_MESSAGE)
        .collect();
    (conflicts, message)
}

impl Room {
    /// Announces locks that are new or changed and paths released, to everyone.
    fn announce_locks(&mut self, locks: Vec<Lock>, released: Vec<Path>) {
        self.shared.locks.store(self.locks.len(), Ordering::Relaxed);
        self.shared
            .detached
            .store(self.locks.detached_count(), Ordering::Relaxed);
        if locks.is_empty() && released.is_empty() {
            return;
        }
        let frame = self.encode(&ServerMsg::LockChanged { locks, released });
        self.broadcast(&frame, &[]);
    }

    /// The periodic sweep.
    pub(super) fn sweep(&mut self) {
        self.expire_locks();
    }

    /// Drops the locks of clients silent too long or gone too long (see the module documentation).
    pub(super) fn expire_locks(&mut self) {
        if self.locks.is_empty() {
            return;
        }
        let now = self.state.clock.now();
        let (ttl, grace) = (self.state.settings.lock_ttl, self.state.settings.lock_grace);
        let dead: Vec<ClientId> = self
            .locks
            .by_client
            .keys()
            .filter(|client| match self.locks.detached.get(*client) {
                Some(since) => now > since + grace,
                None => match self.by_client.get(*client).and_then(|c| self.conns.get(c)) {
                    Some(c) => now > c.heard.at() + ttl,
                    None => true,
                },
            })
            .cloned()
            .collect();
        let mut released = Vec::new();
        for client in dead {
            released.extend(self.locks.release_client(&client));
        }
        self.announce_locks(Vec::new(), released);
    }

    /// After a change became durable: drops the locks at or under what it wrote or removed whose
    /// path no longer resolves.
    pub(super) fn drop_locks_under(
        &mut self,
        wrote: &BTreeSet<Write>,
        removed: &BTreeSet<oswx_core::Path>,
    ) {
        if self.locks.is_empty() {
            return;
        }
        let roots = wrote
            .iter()
            .filter_map(|w| match w {
                Write::At(p) => Some(p),
                Write::Position(..) => None,
            })
            .chain(removed.iter());
        let released = self.locks.drop_unresolved_under(roots, &self.durable.doc);
        if !released.is_empty() {
            self.announce_locks(Vec::new(), released);
        }
    }

    /// At a join (after [`Room::expire_locks`], which the join runs before anything else, so that
    /// nothing dead comes back): the client's detached locks are attached again (released instead
    /// when the connection may only look). Answers what changed, for `lock.changed`.
    pub(super) fn locks_at_join(
        &mut self,
        client: &ClientId,
        read_only: bool,
    ) -> (Vec<Lock>, Vec<Path>) {
        if read_only {
            (Vec::new(), self.locks.release_client(client))
        } else {
            (self.locks.attach(client), Vec::new())
        }
    }

    /// At a leave: the client's locks wait for it (detached), announced after its `peer.left`.
    pub(super) fn locks_at_leave(&mut self, client: &ClientId) {
        let now = self.state.clock.now();
        let detached = self.locks.detach(client, now);
        self.announce_locks(detached, Vec::new());
    }

    /// Announces the changes [`Room::locks_at_join`] made.
    pub(super) fn announce_join_locks(&mut self, back: Vec<Lock>, released: Vec<Path>) {
        self.announce_locks(back, released);
    }

    /// Every lock, for a welcome.
    pub(super) fn all_locks(&self) -> Vec<Lock> {
        self.locks.all()
    }

    /// Announces the releases of a connection that wait (those of paths nobody holds now).
    pub(super) fn flush_lock_news(&mut self, conn: ConnId) {
        let Some(paths) = self.locks.news.remove(&conn) else {
            return;
        };
        let released: Vec<Path> = paths
            .into_iter()
            .filter(|p| !self.locks.by_path.contains_key(p))
            .collect();
        self.announce_locks(Vec::new(), released);
    }

    /// Pays `cost` path-tokens from the connection's lock allowance.
    fn pay_for_locks(&mut self, conn: ConnId, cost: usize) -> bool {
        let now = self.state.clock.now();
        self.conns
            .get_mut(&conn)
            .is_some_and(|c| c.lock_rate.take(cost as f64, LOCK_BURST, LOCK_RATE, now))
    }

    pub(super) fn on_lock_acquire(&mut self, conn: ConnId, request_id: String, paths: Vec<Path>) {
        self.expire_locks();
        let paid = self.pay_for_locks(conn, paths.len());
        let Some(c) = self.conns.get(&conn) else {
            return;
        };
        let me = Holder {
            client: c.client.clone(),
            user: c.user.clone(),
            name: c.name.clone(),
            list: false,
        };
        let read_only = c.read_only.is_some();
        let (mut granted, mut denied, mut acquired) = (Vec::new(), Vec::new(), Vec::new());
        let deny = |path: Path, why: DenyReason| LockDenied {
            path,
            why,
            holder: None,
        };
        for path in paths {
            if !paid {
                denied.push(deny(path, DenyReason::Rate));
                continue;
            }
            if read_only {
                denied.push(deny(path, DenyReason::ReadOnly));
                continue;
            }
            let Some(list) = lockable(&self.durable.doc, &path) else {
                denied.push(deny(path, DenyReason::NotLockable));
                continue;
            };
            if self.locks.holds(&path, &me.client) {
                granted.push(path);
                continue;
            }
            if let Some(holder) = self.locks.in_the_way(&path, list, &me.client) {
                denied.push(LockDenied {
                    path,
                    why: DenyReason::Held,
                    holder: Some(holder),
                });
                continue;
            }
            if !self.locks.has_room(&me) {
                denied.push(deny(path, DenyReason::Quota));
                continue;
            }
            let holder = Holder { list, ..me.clone() };
            if let Some(waiting) = self.locks.news.get_mut(&conn) {
                // Released and taken again: nothing to announce about it any more.
                waiting.remove(&path);
            }
            acquired.push(self.locks.wire(&path, &holder));
            self.locks.insert(path.clone(), holder);
            granted.push(path);
        }
        metrics::locks(acquired.len(), denied.len());
        let frame = self.encode(&ServerMsg::LockResult {
            request_id,
            granted,
            denied,
        });
        self.reply(conn, frame);
        self.announce_locks(acquired, Vec::new());
    }

    pub(super) fn on_lock_release(&mut self, conn: ConnId, paths: &[Path]) {
        let paid = self.pay_for_locks(conn, paths.len());
        let Some(client) = self.conns.get(&conn).map(|c| c.client.clone()) else {
            return;
        };
        let mut released = Vec::new();
        for path in paths {
            if self.locks.holds(path, &client) {
                self.locks.remove(path);
                released.push(path.clone());
            }
        }
        if paid {
            return self.announce_locks(Vec::new(), released);
        }
        // Beyond the allowance: honoured now, announced together a little later.
        self.announce_locks(Vec::new(), Vec::new());
        if released.is_empty() {
            return;
        }
        let first = !self.locks.news.contains_key(&conn);
        self.locks.news.entry(conn).or_default().extend(released);
        if first {
            let internal = self.internal.clone();
            tokio::spawn(async move {
                tokio::time::sleep(LOCK_NEWS_EVERY).await;
                let _ = internal.send(Internal::LockNews(conn));
            });
        }
    }

    /// Refuses a change (`locked`) that touches locks standing in its way, answering whoever waits
    /// for it; otherwise hands it back to be judged.
    pub(super) fn unless_locked(&mut self, item: Item) -> Option<Item> {
        if self.locks.is_empty() {
            return Some(item);
        }
        self.expire_locks();
        if self.locks.is_empty() {
            return Some(item);
        }
        let view = item.lock_view();
        let hit = match view.rule {
            None => self
                .locks
                .touched(&writes(view.ops), |h| h.client != *view.client),
            Some(LockRule::None) => Vec::new(),
            Some(LockRule::Writes) => self.locks.touched(&writes(view.ops), |_| true),
            Some(LockRule::WritesExcept(client)) => self
                .locks
                .touched(&writes(view.ops), |h| h.client != *client),
            Some(LockRule::AnyOfOtherUsers) => self.locks.of_other_users(view.user),
        };
        if hit.is_empty() {
            return Some(item);
        }
        let (conflicts, message) = refusal(&hit);
        self.refuse_locked(item, conflicts, &message);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oswx_core::{Op, Pos};

    fn p(s: &str) -> Path {
        s.split('/').map(Arc::from).collect()
    }

    fn doc() -> Value {
        let j = serde_json::json!({
            "material": {"name": null},
            "features": [
                {"id": "f1", "name": "Sketch1", "params": {"w": 1}},
                {"id": "f2", "name": "Extrude1", "params": {"depth": 10}},
            ],
            "points": [1, 2],
            "rollback": null,
        });
        Value::from_plain(&crate::content::plain_from_json(&j)).unwrap()
    }

    fn holder(client: &str, list: bool) -> Holder {
        Holder {
            client: Arc::from(client),
            user: client.split('.').next().unwrap().to_owned(),
            name: client.to_owned(),
            list,
        }
    }

    fn with(locks: &[(&str, &str, bool)]) -> Locks {
        let mut out = Locks::default();
        for (path, client, list) in locks {
            out.insert(p(path), holder(client, *list));
        }
        out
    }

    fn set(path: &str) -> Op {
        Op::Set {
            path: p(path),
            value: Value::Int(1),
        }
    }

    fn mv(list: &str, id: &str) -> Op {
        Op::Move {
            list: p(list),
            id: Arc::from(id),
            pos: Pos::between(None, None),
        }
    }

    fn remove(list: &str, id: &str) -> Op {
        Op::Remove {
            list: p(list),
            id: Arc::from(id),
        }
    }

    fn hits(locks: &Locks, ops: &[Op]) -> Vec<String> {
        locks
            .touched(&writes(ops), |h| &*h.client != "b.c")
            .into_iter()
            .map(|(path, _)| path.join("/"))
            .collect()
    }

    #[test]
    fn what_is_lockable() {
        let d = doc();
        assert_eq!(lockable(&d, &p("features")), Some(true));
        assert_eq!(lockable(&d, &p("features/f1")), Some(false));
        assert_eq!(lockable(&d, &p("features/f1/params/w")), Some(false));
        assert_eq!(lockable(&d, &p("points")), Some(true));
        assert_eq!(lockable(&d, &p("material/name")), Some(false));
        assert_eq!(
            lockable(&d, &p("display")),
            Some(false),
            "a new top-level key"
        );
        assert_eq!(lockable(&d, &p("features/f9")), None);
        assert_eq!(lockable(&d, &p("material/colour")), None);
        assert_eq!(lockable(&d, &p("features/f1/~cache")), None);
        assert_eq!(lockable(&d, &p("~x")), None);
        assert_eq!(lockable(&d, &Vec::new()), None);
        for key in PROTECTED {
            assert_eq!(lockable(&d, &vec![Arc::from(key)]), None, "{key}");
        }
        let deep: Path = (0..=MAX_DEPTH).map(|_| Arc::from("a")).collect();
        assert_eq!(lockable(&d, &deep), None);
    }

    #[test]
    fn a_lock_stops_writes_at_under_and_above_it() {
        let locks = with(&[("features/f1", "a.c", false)]);
        assert_eq!(hits(&locks, &[set("features/f1")]), ["features/f1"]);
        assert_eq!(
            hits(&locks, &[set("features/f1/params/w")]),
            ["features/f1"]
        );
        assert_eq!(hits(&locks, &[set("features")]), ["features/f1"]);
        assert_eq!(hits(&locks, &[remove("features", "f1")]), ["features/f1"]);
        assert_eq!(hits(&locks, &[mv("features", "f1")]), ["features/f1"]);
        assert!(hits(&locks, &[set("features/f2/params/depth")]).is_empty());
        assert!(hits(&locks, &[mv("features", "f2")]).is_empty());
        assert!(hits(&locks, &[set("material/name")]).is_empty());
        // The holder's own (and anyone `blocks` lets through) never conflict.
        let own = with(&[("features/f1", "b.c", false)]);
        assert!(hits(&own, &[set("features/f1/params/w")]).is_empty());
    }

    #[test]
    fn a_list_lock_stops_its_structure_only() {
        let locks = with(&[("features", "a.c", true)]);
        assert_eq!(hits(&locks, &[remove("features", "f2")]), ["features"]);
        assert_eq!(hits(&locks, &[mv("features", "f2")]), ["features"]);
        assert_eq!(hits(&locks, &[set("features/f9")]), ["features"]);
        assert_eq!(hits(&locks, &[set("features")]), ["features"]);
        assert!(hits(&locks, &[set("features/f2/params/depth")]).is_empty());
        // A move in a list inside an item is an edit inside the item.
        assert!(hits(&locks, &[mv("features/f2/params/list", "x")]).is_empty());
    }

    #[test]
    fn nested_locks_of_one_client_and_the_way_of_another() {
        let locks = with(&[("features/f1", "a.c", false)]);
        let a: ClientId = Arc::from("a.c");
        let b: ClientId = Arc::from("b.c");
        assert!(
            locks
                .in_the_way(&p("features/f1/params"), false, &a)
                .is_none()
        );
        assert!(locks.in_the_way(&p("features"), true, &a).is_none());
        for path in ["features/f1/params", "features/f1"] {
            assert_eq!(
                locks.in_the_way(&p(path), false, &b).map(|l| l.path),
                Some(p("features/f1")),
                "{path}"
            );
        }
        assert!(locks.in_the_way(&p("features/f2"), false, &b).is_none());
        assert!(locks.in_the_way(&p("features/f10"), false, &b).is_none());
    }

    #[test]
    fn a_list_lock_and_its_items_locks_go_together_deeper_ones_do_not() {
        let b: ClientId = Arc::from("b.c");
        let a: ClientId = Arc::from("a.c");
        // Ada holds an item: Bob may lock the list, not a map above it.
        let item = with(&[("features/f1", "a.c", false)]);
        assert!(item.in_the_way(&p("features"), true, &b).is_none());
        assert!(item.in_the_way(&p("features"), false, &b).is_some());
        // Ada holds the list: Bob may lock an item, not deeper.
        let list = with(&[("features", "a.c", true)]);
        assert!(list.in_the_way(&p("features/f1"), false, &b).is_none());
        assert_eq!(
            list.in_the_way(&p("features/f1/params"), false, &b)
                .map(|l| l.path),
            Some(p("features"))
        );
        // Ada holds something deeper than an item: Bob may not lock the list.
        let deep = with(&[("features/f1/params", "a.c", false)]);
        assert!(deep.in_the_way(&p("features"), true, &b).is_some());
        assert!(deep.in_the_way(&p("features"), true, &a).is_none());
    }

    #[test]
    fn the_indexes_follow_every_change() {
        let mut locks = with(&[
            ("features/f1", "a.c", false),
            ("display", "a.c", false),
            ("features/f2", "b.c", false),
        ]);
        let a: ClientId = Arc::from("a.c");
        assert_eq!(locks.firsts.get("features"), Some(&2));
        assert_eq!(locks.by_user.get("a"), Some(&2));
        assert_eq!(locks.detach(&a, 5.0).len(), 2);
        assert!(locks.detach(&a, 6.0).is_empty(), "once");
        assert_eq!(locks.detached_count(), 2);
        assert_eq!(locks.all().iter().filter(|l| l.detached).count(), 2);
        assert_eq!(locks.attach(&a).len(), 2);
        assert!(locks.attach(&a).is_empty());
        assert_eq!(locks.detached_count(), 0);
        // f1 and f2 removed from the document: their locks go; a top-level key stays.
        let mut d = doc();
        if let Value::Map(m) = &mut d {
            m.remove("features");
        }
        let gone = locks.drop_unresolved_under([p("features")].iter(), &d);
        assert_eq!(gone, [p("features/f1"), p("features/f2")]);
        assert_eq!(locks.firsts.get("features"), None);
        assert_eq!(locks.release_client(&a), [p("display")]);
        assert!(locks.is_empty());
        assert!(locks.by_client.is_empty() && locks.by_user.is_empty());
        assert!(locks.firsts.is_empty() && locks.detached.is_empty());
    }

    #[test]
    fn caps_per_client_user_and_room() {
        let mut locks = Locks::default();
        let add = |locks: &mut Locks, client: &str, n: usize| {
            let h = holder(client, false);
            let mut added = 0;
            for i in 0..n {
                if !locks.has_room(&h) {
                    break;
                }
                locks.insert(p(&format!("k-{client}-{i}")), h.clone());
                added += 1;
            }
            added
        };
        assert_eq!(add(&mut locks, "u1.a", 300), MAX_LOCKS);
        assert_eq!(add(&mut locks, "u1.b", 300), MAX_LOCKS);
        assert_eq!(add(&mut locks, "u1.c", 300), MAX_LOCKS);
        assert_eq!(add(&mut locks, "u1.d", 300), MAX_LOCKS);
        assert_eq!(add(&mut locks, "u1.e", 300), 0, "the user's 1,024");
        for u in 2..=8 {
            add(&mut locks, &format!("u{u}.a"), 256);
            add(&mut locks, &format!("u{u}.b"), 256);
            add(&mut locks, &format!("u{u}.c"), 256);
            add(&mut locks, &format!("u{u}.d"), 256);
        }
        assert_eq!(locks.len(), MAX_ROOM_LOCKS);
        assert_eq!(add(&mut locks, "u9.a", 1), 0, "the room's 8,192");
    }
}
