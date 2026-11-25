//! Locks: advisory leases on document paths ("Ada is editing this sketch") that also stop other
//! clients' changes to what they cover. They live in the room's memory only (a restart or an unload
//! forgets them; clients take them again).
//!
//! # Which paths
//!
//! A path is lockable when it resolves in the durable document (a map key or a keyed-list item at
//! any depth, or a list itself), or when it is a single top-level key, which need not exist yet (a
//! client may lock a key it is about to create). A path with a segment starting `~` (derived data)
//! is not lockable, nor one of more than `MAX_DEPTH` segments.
//!
//! # Acquiring
//!
//! `lock.acquire` (at most 256 paths, the codec's limit) is answered `lock.result`, path by path:
//! a read-only connection, a path that is not lockable, or a full quota ([`MAX_LOCKS`] per client)
//! is denied with no holder; a path the client already holds is granted; a path **at, under or
//! above** a lock of another client is denied with that lock (so two clients never hold
//! overlapping locks; one client may hold nested ones). Newly granted locks are announced to
//! everyone in `lock.changed`. `lock.acquire`s are limited to `presence_rate` a second per
//! connection (a token bucket that starts full); one over the limit is answered with every path
//! denied, no holder, and changes nothing. `lock.release` releases the sender's own locks among
//! those named (no answer; `lock.changed` to everyone when any was held).
//!
//! # What a lock stops
//!
//! A change by another client (a client's own locks never stop it) is refused `locked` when one of
//! its writes (`oswx_core::writes`) touches a locked path: `At(p)` touches a lock at `p`, above `p`
//! or under `p`; `Position(list, id)` (a move) a lock at `list + id` or above it. A lock on a list
//! itself (`["features"]`) is narrower: it stops writing the list or above it, and the insertion,
//! removal, replacement or move of its items (`At(list + id)`, `Position(list, id)`), but not edits
//! inside an item (lock the item for that). The refusal's conflicts name each lock in the way
//! (kind `locked`, `by` its holder's client id, seq 0). Changes the server makes follow
//! [`super::LockRule`].
//!
//! # Expiry
//!
//! Locks expire on every sweep (the room's timer), before every `lock.acquire` and before every
//! change is judged: an attached lock dies when its client's connection has been silent (no frame
//! at all) for `lock_ttl`; when the connection goes, its locks are detached and die after
//! `lock_grace` unless the same client id comes back, when they are attached again (or released,
//! if it now may only look). A second connection of the same client (the 4409 takeover) keeps them
//! attached. A lock whose path no longer resolves in the durable document (its item was removed)
//! is dropped as soon as that is durable. Every drop is announced in `lock.changed`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use oswx_core::wire::{Lock, LockDenied, ServerMsg};
use oswx_core::{
    ClientId, Conflict, ConflictKind, MAX_DEPTH, Op, Path, RejectCode, Seq, Value, Write, writes,
};

use super::change::{Item, Origin};
use super::{ConnId, LockRule, Room, ServerChangeError};
use crate::metrics;
use crate::store::Id;

/// The most locks one client holds.
pub const MAX_LOCKS: usize = 256;
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

/// The locks of a room.
#[derive(Debug, Default)]
pub(super) struct Locks {
    by_path: BTreeMap<Path, Holder>,
    /// How many locks each client holds.
    held: HashMap<ClientId, usize>,
    /// Clients whose connection went, and since when: their locks are detached.
    detached: HashMap<ClientId, f64>,
    /// The durable seq the paths were last checked against.
    checked: Seq,
}

/// Whether `path` may be locked in `doc`: `Some(is a list)`, or `None`.
pub(super) fn lockable(doc: &Value, path: &Path) -> Option<bool> {
    if path.is_empty() || path.len() > MAX_DEPTH || path.iter().any(|s| s.starts_with('~')) {
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
    pub(super) fn new(seq: Seq) -> Self {
        Self {
            checked: seq,
            ..Self::default()
        }
    }

    pub(super) fn len(&self) -> usize {
        self.by_path.len()
    }

    /// How many locks are detached.
    pub(super) fn detached_count(&self) -> usize {
        self.detached
            .keys()
            .map(|c| self.held.get(c).copied().unwrap_or(0))
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
        if !self.held.contains_key(client) {
            return Vec::new();
        }
        self.by_path
            .iter()
            .filter(|(_, h)| h.client == *client)
            .map(|(p, _)| p.clone())
            .collect()
    }

    fn remove(&mut self, path: &Path) -> Option<Holder> {
        let h = self.by_path.remove(path)?;
        if let Some(n) = self.held.get_mut(&h.client) {
            *n -= 1;
            if *n == 0 {
                self.held.remove(&h.client);
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
        if !self.held.contains_key(client) || self.detached.contains_key(client) {
            return Vec::new();
        }
        self.detached.insert(client.clone(), now);
        let paths = self.of(client);
        paths
            .iter()
            .map(|p| self.wire(p, &self.by_path[p]))
            .collect()
    }

    /// Attaches the client's detached locks again; answers them, if there were any.
    pub(super) fn attach(&mut self, client: &ClientId) -> Vec<Lock> {
        if self.detached.remove(client).is_none() {
            return Vec::new();
        }
        let paths = self.of(client);
        paths
            .iter()
            .map(|p| self.wire(p, &self.by_path[p]))
            .collect()
    }

    /// The first lock of another client at, above or under `path`.
    fn in_the_way(&self, path: &Path, client: &ClientId) -> Option<Lock> {
        for i in 1..=path.len() {
            if let Some((p, h)) = self.by_path.get_key_value(&path[..i])
                && h.client != *client
            {
                return Some(self.wire(p, h));
            }
        }
        self.under(path)
            .find(|(_, h)| h.client != *client)
            .map(|(p, h)| self.wire(p, h))
    }

    /// The locks strictly under `path`.
    fn under<'a>(&'a self, path: &'a [Arc<str>]) -> impl Iterator<Item = (&'a Path, &'a Holder)> {
        self.by_path
            .range::<[Arc<str>], _>((Bound::Excluded(path), Bound::Unbounded))
            .take_while(move |(p, _)| p.starts_with(path))
    }

    /// The locks `ops` touch whose holder `blocks` says count, at most [`MAX_CONFLICTS`].
    fn touched(&self, ops: &[Op], blocks: impl Fn(&Holder) -> bool) -> Vec<(Path, Holder)> {
        if self.by_path.is_empty() {
            return Vec::new();
        }
        let first: HashSet<&str> = self.by_path.keys().map(|p| &*p[0]).collect();
        let mut hit: BTreeMap<Path, Holder> = BTreeMap::new();
        for w in writes(ops) {
            if hit.len() >= MAX_CONFLICTS {
                break;
            }
            let (p, position) = match w {
                Write::At(p) => (p, false),
                Write::Position(mut list, id) => {
                    list.push(id);
                    (list, true)
                }
            };
            if !p.first().is_some_and(|s| first.contains(&**s)) {
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

    /// Drops the locks (of more than one segment) whose path is not in `doc`; answers their paths.
    fn drop_unresolved(&mut self, doc: &Value) -> Vec<Path> {
        let gone: Vec<Path> = self
            .by_path
            .keys()
            .filter(|p| p.len() > 1 && lockable(doc, p).is_none())
            .cloned()
            .collect();
        for p in &gone {
            self.remove(p);
        }
        gone
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
        if self.locks.held.is_empty() {
            return;
        }
        let now = self.state.clock.now();
        let (ttl, grace) = (self.state.settings.lock_ttl, self.state.settings.lock_grace);
        let dead: Vec<ClientId> = self
            .locks
            .held
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

    /// Drops the locks on paths the durable document no longer has, once per durable change.
    pub(super) fn follow_head_with_locks(&mut self) {
        if self.locks.checked == self.durable.seq {
            return;
        }
        self.locks.checked = self.durable.seq;
        if self.locks.len() == 0 {
            return;
        }
        let released = self.locks.drop_unresolved(&self.durable.doc);
        if !released.is_empty() {
            self.announce_locks(Vec::new(), released);
        }
    }

    /// At a join: the client's detached locks are attached again (released instead when the
    /// connection may only look). Answers what changed, for `lock.changed`.
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

    pub(super) fn on_lock_acquire(&mut self, conn: ConnId, request_id: String, paths: Vec<Path>) {
        self.expire_locks();
        let now = self.state.clock.now();
        let rate = f64::from(self.state.settings.presence_rate);
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        let allowed = c.lock_rate.take(rate, now);
        let (client, user, name) = (c.client.clone(), c.user.clone(), c.name.clone());
        let read_only = c.read_only.is_some();
        let (mut granted, mut denied, mut acquired) = (Vec::new(), Vec::new(), Vec::new());
        let nobody = |path: Path| LockDenied { path, holder: None };
        for path in paths {
            if !allowed || read_only {
                denied.push(nobody(path));
                continue;
            }
            let Some(list) = lockable(&self.durable.doc, &path) else {
                denied.push(nobody(path));
                continue;
            };
            if self
                .locks
                .by_path
                .get(&path)
                .is_some_and(|h| h.client == client)
            {
                granted.push(path);
                continue;
            }
            if let Some(holder) = self.locks.in_the_way(&path, &client) {
                denied.push(LockDenied {
                    path,
                    holder: Some(holder),
                });
                continue;
            }
            if self.locks.held.get(&client).copied().unwrap_or(0) >= MAX_LOCKS {
                denied.push(nobody(path));
                continue;
            }
            let holder = Holder {
                client: client.clone(),
                user: user.clone(),
                name: name.clone(),
                list,
            };
            *self.locks.held.entry(client.clone()).or_insert(0) += 1;
            acquired.push(self.locks.wire(&path, &holder));
            self.locks.by_path.insert(path.clone(), holder);
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
        let Some(client) = self.conns.get(&conn).map(|c| c.client.clone()) else {
            return;
        };
        let mut released = Vec::new();
        for path in paths {
            if self
                .locks
                .by_path
                .get(path)
                .is_some_and(|h| h.client == client)
            {
                self.locks.remove(path);
                released.push(path.clone());
            }
        }
        self.announce_locks(Vec::new(), released);
    }

    /// Refuses a change (`locked`) that touches locks standing in its way, answering whoever waits
    /// for it; otherwise hands it back to be judged.
    pub(super) fn unless_locked(&mut self, item: Item) -> Option<Item> {
        self.expire_locks();
        if self.locks.len() == 0 {
            return Some(item);
        }
        let hit = match &item.origin {
            Origin::Conn(_) => {
                let me = &item.author.client;
                self.locks.touched(&item.change.ops, |h| h.client != *me)
            }
            Origin::Server { locks, .. } => match locks {
                LockRule::Writes => self.locks.touched(&item.change.ops, |_| true),
                LockRule::WritesExcept(client) => self
                    .locks
                    .touched(&item.change.ops, |h| h.client != *client),
                LockRule::AnyOfOtherUsers => self.locks.of_other_users(&item.author.user),
            },
        };
        if hit.is_empty() {
            return Some(item);
        }
        let (conflicts, message) = refusal(&hit);
        match item.origin {
            Origin::Server { reply, .. } => {
                metrics::change_rejected(RejectCode::Locked);
                let _ = reply.send(Err(ServerChangeError::Locked(conflicts)));
            }
            Origin::Conn(_) => {
                self.answer_refused(item, RejectCode::Locked, &message, conflicts, None);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oswx_core::Pos;

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
            let h = holder(client, *list);
            *out.held.entry(h.client.clone()).or_insert(0) += 1;
            out.by_path.insert(p(path), h);
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
            .touched(ops, |h| &*h.client != "b.c")
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
        assert!(locks.in_the_way(&p("features/f1/params"), &a).is_none());
        assert!(locks.in_the_way(&p("features"), &a).is_none());
        for path in ["features/f1/params", "features/f1", "features"] {
            assert_eq!(
                locks.in_the_way(&p(path), &b).map(|l| l.path),
                Some(p("features/f1")),
                "{path}"
            );
        }
        assert!(locks.in_the_way(&p("features/f2"), &b).is_none());
        assert!(locks.in_the_way(&p("features/f10"), &b).is_none());
    }

    #[test]
    fn detaching_attaching_and_dropping() {
        let mut locks = with(&[
            ("features/f1", "a.c", false),
            ("display", "a.c", false),
            ("features/f2", "b.c", false),
        ]);
        let a: ClientId = Arc::from("a.c");
        assert_eq!(locks.detach(&a, 5.0).len(), 2);
        assert!(locks.detach(&a, 6.0).is_empty(), "once");
        assert_eq!(locks.detached_count(), 2);
        assert!(locks.all().iter().filter(|l| l.detached).count() == 2);
        assert_eq!(locks.attach(&a).len(), 2);
        assert!(locks.attach(&a).is_empty());
        assert_eq!(locks.detached_count(), 0);
        // f1 removed from the document: its lock goes; a top-level key stays.
        let mut d = doc();
        if let Value::Map(m) = &mut d {
            m.remove("features");
        }
        let mut gone = locks.drop_unresolved(&d);
        gone.sort();
        assert_eq!(gone, [p("features/f1"), p("features/f2")]);
        assert_eq!(locks.release_client(&a), [p("display")]);
        assert_eq!(locks.len(), 0);
        assert!(locks.held.is_empty() && locks.detached.is_empty());
    }
}
