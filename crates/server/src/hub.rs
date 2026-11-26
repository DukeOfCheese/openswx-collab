//! The open documents (rooms) and the document connections.
//!
//! The hub maps a document id to its room's [`RoomHandle`]. [`Hub::open`] loads a room at most once
//! per document however many connections ask at the same moment (a gate per document); a load that
//! fails is reported to the caller and not remembered. A room takes itself out of the hub when it
//! ends. A room unloading because it is idle stays in the hub until its final snapshot is written,
//! and [`Hub::open`] waits for it to end rather than load the document beside it; it never hands
//! out a room that has begun to unload.
//!
//! [`Hub::online`] reads what each open room publishes (never loading a room or waiting for one).
//!
//! # Connections and drops
//!
//! Every document connection is registered here from the moment its session is known
//! ([`Hub::register`]) until it ends (the [`Registration`] is dropped), with its account, its
//! document, the hash of its session's token and, once settled, its project and role. This is what
//! the drops act on, straight through the connection's [`Outbox`] (closing it at once: whatever is
//! queued is dropped, the close frame goes out, its reader stops reading, and its room processes no
//! more of its frames), so a drop takes effect whatever the room is busy with:
//!
//! | call | which connections | close |
//! |---|---|---|
//! | [`Hub::drop_account`] | every connection of the account | 4401 `sign in again` |
//! | [`Hub::drop_session`] | every connection made with that session | 4401 `sign in again` |
//! | [`Hub::drop_user`] | the account's connections to the organisation's documents whose role, read again now, went or differs | 4403 `your access changed` |
//! | [`Hub::close_document`] | every connection to the document (its room unloads) | 4404 with the reason |
//!
//! A connection whose handshake is still working out its role when a drop concerns it is told to
//! work it out again ([`Registration::settle`] answers false), so a role read before a change is
//! never kept. A session that expires while connected is closed by its room's tick (4401).
//!
//! The registry also counts each account's connections: [`Hub::register`] refuses one more than
//! `max_connections_per_user`.
//!
//! # Sweeper
//!
//! Once a room is open, a sweeper runs every `sweep_interval` seconds and asks each room, without
//! waiting for it, to sweep and to unload itself if it is idle ([`RoomHandle::sweep`]). A room that
//! does not answer is asked again next time; no room can hold up the others.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use serde::Serialize;

use crate::access::role_in_project;
use crate::lease;
use crate::metrics::{self, DropCause};
use crate::room::{self, ConnId, Outbox, RoomHandle, RoomInit, close};
use crate::security::ProjectRole;
use crate::snapshot::{LoadError, load_head};
use crate::state::AppState;
use crate::store::{Id, Store};

/// How long a room lease is said to last (a hint for other nodes; the epoch is the guard).
const LEASE_TTL: f64 = 60.0;
/// How long shutdown waits for the rooms to close.
const SHUTDOWN_WAIT: Duration = Duration::from_secs(60);

/// Someone connected to a document, as the document list shows them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Online {
    pub name: String,
    pub colour: String,
}

/// Why a room could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// There is no such document, or it was deleted.
    #[error("there is no such document")]
    NoDocument,
    /// The server is shutting down and opens no rooms.
    #[error("the server is shutting down")]
    ShuttingDown,
    #[error(transparent)]
    Failed(#[from] anyhow::Error),
}

/// Why a connection could not be registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RegisterError {
    /// The account has `max_connections_per_user` connections open already.
    #[error("too many connections")]
    TooMany,
    /// The server is shutting down.
    #[error("the server is shutting down")]
    ShuttingDown,
}

/// A connection asking to register ([`Hub::register`]).
pub struct Connecting<'a> {
    pub conn: ConnId,
    pub user: &'a Id,
    pub doc: &'a Id,
    /// `<user id>.<client id of the hello>`.
    pub client: &'a Arc<str>,
    /// The hash of its session's token (never the token).
    pub token_hash: &'a str,
    pub outbox: Arc<Outbox>,
}

/// What the hub holds now (for `/metrics`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Gauges {
    /// Rooms open (unloading ones included).
    pub rooms: usize,
    /// Document connections registered (handshakes past the session check included).
    pub connections: usize,
}

/// A registered connection.
struct Entry {
    user: Id,
    doc: Id,
    /// `<user id>.<client id of the hello>`.
    client: Arc<str>,
    token_hash: String,
    outbox: Arc<Outbox>,
    /// The project and role it joined with, once its handshake settled them.
    settled: Option<(Id, ProjectRole)>,
    /// A drop concerned it before it settled: its handshake works its role out again.
    recheck: bool,
    /// The room that took it, once one did.
    room: Option<RoomHandle>,
    /// How a drop closed it, if one did, and whether account-wide (told to its room when the room
    /// takes it later).
    revoked: Option<(u16, String, bool)>,
    /// A newer connection of the same client to the same document was let past the connection
    /// limit to take this one over: no other may use it for that.
    superseded: bool,
    /// It was let past the limit to take over this connection, and has not joined yet (until it
    /// does, no other connection of its client may take over: the limit is passed by one at
    /// most).
    taking_over: Option<ConnId>,
}

/// Everything but the token hash, which is never printed.
impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("user", &self.user)
            .field("doc", &self.doc)
            .field("client", &self.client)
            .field("settled", &self.settled)
            .field("recheck", &self.recheck)
            .field("room", &self.room.as_ref().map(RoomHandle::id))
            .field("revoked", &self.revoked)
            .field("superseded", &self.superseded)
            .field("taking_over", &self.taking_over)
            .finish_non_exhaustive()
    }
}

impl Entry {
    /// Closes the connection for a drop: its outbox at once, and its room (if it has one) is told
    /// to remove it and release its locks.
    /// `user_wide`: the account's access ended (its detached locks in the room go too).
    fn revoke(
        &mut self,
        conn: ConnId,
        code: u16,
        reason: &str,
        user_wide: bool,
    ) -> Option<Revoked> {
        self.outbox.close_now(code, reason);
        self.revoked = Some((code, reason.to_owned(), user_wide));
        self.room
            .clone()
            .map(|room| (room, conn, self.user.clone()))
    }
}

/// A revoked connection's room, the connection and its account.
type Revoked = (RoomHandle, ConnId, Id);

/// Tells the rooms of revoked connections (never under the registry lock).
fn tell_rooms(revoked: Vec<Revoked>, code: u16, reason: &str, user_wide: bool) {
    for (room, conn, user) in revoked {
        room.revoke(Some(conn), &user, code, reason, user_wide);
    }
}

#[derive(Debug, Default)]
struct Registry {
    entries: HashMap<ConnId, Entry>,
    /// The connections of each account (their number is what the limit counts).
    by_user: HashMap<Id, std::collections::HashSet<ConnId>>,
}

impl Registry {
    /// The connections of `user`.
    fn of(&self, user: &str) -> impl Iterator<Item = ConnId> + '_ {
        self.by_user.get(user).into_iter().flatten().copied()
    }
}

#[derive(Debug, Default)]
struct Inner {
    rooms: Mutex<HashMap<Id, RoomHandle>>,
    /// One per document being opened, so that it is loaded once.
    gates: Mutex<HashMap<Id, Arc<tokio::sync::Mutex<()>>>>,
    closing: AtomicBool,
    registry: Mutex<Registry>,
    sweeper: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The drop and close calls made so far (for tests).
    calls: Mutex<Vec<String>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(task) = lock(&self.sweeper).take() {
            task.abort();
        }
    }
}

/// The open rooms and the connections. Cloning is cheap: clones share them.
#[derive(Debug, Clone, Default)]
pub struct Hub {
    inner: Arc<Inner>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A connection's place in the hub's registry; dropping it unregisters the connection (and frees
/// its account's slot).
#[derive(Debug)]
pub struct Registration {
    hub: Hub,
    conn: ConnId,
}

impl Registration {
    /// Records the project and role the connection joins with. Answers false (and forgets the
    /// request) when a drop concerned the connection since it registered or since the last call:
    /// the caller reads the session and the role again and calls again.
    pub fn settle(&self, project: &Id, role: ProjectRole) -> bool {
        let mut registry = lock(&self.hub.inner.registry);
        let Some(entry) = registry.entries.get_mut(&self.conn) else {
            return false;
        };
        if std::mem::take(&mut entry.recheck) {
            return false;
        }
        entry.settled = Some((project.clone(), role));
        true
    }

    /// Records the room that took the connection. If a drop closed the connection before, the
    /// room is told now (so that it removes it and releases its locks at once).
    pub fn joined(&self, room: &RoomHandle) {
        let revoked = {
            let mut registry = lock(&self.hub.inner.registry);
            let Some(entry) = registry.entries.get_mut(&self.conn) else {
                return;
            };
            entry.room = Some(room.clone());
            // Its take-over is done: the next one of its client may come.
            entry.taking_over = None;
            entry.revoked.clone().map(|r| (r, entry.user.clone()))
        };
        if let Some(((code, reason, user_wide), user)) = revoked {
            room.revoke(Some(self.conn), &user, code, &reason, user_wide);
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut registry = lock(&self.hub.inner.registry);
        let Some(entry) = registry.entries.remove(&self.conn) else {
            return;
        };
        // A take-over that never joined gives the connection it was to replace back.
        if let Some(old) = entry.taking_over
            && let Some(e) = registry.entries.get_mut(&old)
        {
            e.superseded = false;
        }
        if let Some(conns) = registry.by_user.get_mut(&entry.user) {
            conns.remove(&self.conn);
            if conns.is_empty() {
                registry.by_user.remove(&entry.user);
            }
        }
    }
}

impl Hub {
    /// A hub with no rooms.
    pub fn new() -> Self {
        Self::default()
    }

    fn record(&self, call: String) {
        let mut calls = lock(&self.inner.calls);
        if calls.len() >= 1000 {
            calls.remove(0); // (a running server keeps only the latest)
        }
        calls.push(call);
    }

    /// The drop and close calls made so far, as `drop_account <user>`, `drop_session`,
    /// `drop_user <user> <org>` and `close_document <doc>` (for tests).
    #[doc(hidden)]
    pub fn recorded(&self) -> Vec<String> {
        lock(&self.inner.calls).clone()
    }

    /// The room of `doc` if it is open (and not unloading).
    pub fn loaded(&self, doc: &str) -> Option<RoomHandle> {
        lock(&self.inner.rooms)
            .get(doc)
            .filter(|r| !r.is_closed() && !r.is_unloading())
            .cloned()
    }

    /// The room of `doc` if it is open, after waiting for one that is unloading to end.
    async fn current(&self, doc: &str) -> Option<RoomHandle> {
        loop {
            let room = lock(&self.inner.rooms).get(doc).cloned()?;
            if room.is_closed() {
                return None;
            }
            if !room.is_unloading() {
                return Some(room);
            }
            room.ended().await;
        }
    }

    /// The room of `doc`, loading it if it is not open: the document (which must exist and not be
    /// deleted) is leased to this process, then its head is loaded from the store.
    pub async fn open(&self, state: &AppState, doc: &str) -> Result<RoomHandle, OpenError> {
        if self.inner.closing.load(Ordering::SeqCst) {
            return Err(OpenError::ShuttingDown);
        }
        if let Some(room) = self.current(doc).await {
            return Ok(room);
        }
        let gate = lock(&self.inner.gates)
            .entry(doc.to_owned())
            .or_default()
            .clone();
        let opened = {
            let _held = gate.lock().await;
            match self.current(doc).await {
                Some(room) => Ok(room),
                None if self.inner.closing.load(Ordering::SeqCst) => Err(OpenError::ShuttingDown),
                None => match load(state, doc).await {
                    Ok(init) => {
                        let room = room::spawn(state.clone(), init);
                        let kept = {
                            let mut rooms = lock(&self.inner.rooms);
                            let closing = self.inner.closing.load(Ordering::SeqCst);
                            if !closing {
                                rooms.insert(doc.to_owned(), room.clone());
                            }
                            !closing
                        };
                        if kept {
                            self.ensure_sweeper(state.settings.sweep_interval);
                            Ok(room)
                        } else {
                            // Shutdown began while it loaded: it closes at once, unseen.
                            room.close(close::GOING_AWAY, "the server is shutting down", false)
                                .await;
                            Err(OpenError::ShuttingDown)
                        }
                    }
                    Err(e) => Err(e),
                },
            }
        };
        let mut gates = lock(&self.inner.gates);
        if gates
            .get(doc)
            .is_some_and(|g| Arc::ptr_eq(g, &gate) && Arc::strong_count(g) == 2)
        {
            gates.remove(doc);
        }
        opened
    }

    /// Takes the room `id` of `doc` out of the hub (a room ending); a newer room of the same
    /// document is left alone.
    pub(crate) fn forget(&self, doc: &str, id: u64) {
        let mut rooms = lock(&self.inner.rooms);
        if rooms.get(doc).is_some_and(|r| r.id() == id) {
            rooms.remove(doc);
        }
    }

    // ------------------------------------------------------------------ connections

    /// Registers a document connection whose session is known (see the module documentation);
    /// `client` is `<user id>.<client id of the hello>`. Refused when the account has `max`
    /// connections already, unless this one takes over a connection of the same client to the same
    /// document (which its room then replaces: no more connections than before); and refused when
    /// the server is shutting down.
    pub fn register(
        &self,
        connecting: Connecting<'_>,
        max: usize,
    ) -> Result<Registration, RegisterError> {
        let Connecting {
            conn,
            user,
            doc,
            client,
            token_hash,
            outbox,
        } = connecting;
        if self.inner.closing.load(Ordering::SeqCst) {
            return Err(RegisterError::ShuttingDown);
        }
        // The limit and the take-over exemption are decided under one lock: of handshakes racing
        // with the same client id, only one may take over a given connection.
        let mut guard = lock(&self.inner.registry);
        let registry = &mut *guard;
        let n = registry.by_user.get(user).map_or(0, |c| c.len());
        let mut taking_over = None;
        if n >= max {
            let same = |e: &Entry| e.doc == *doc && e.client == *client;
            let mut replaced = None;
            for c in registry.of(user) {
                let Some(e) = registry.entries.get(&c) else {
                    continue;
                };
                if same(e) && e.taking_over.is_some() {
                    // Another connection of this client is taking over already.
                    return Err(RegisterError::TooMany);
                }
                if same(e) && !e.superseded {
                    replaced = Some(c);
                }
            }
            let Some(replaced) = replaced else {
                return Err(RegisterError::TooMany);
            };
            if let Some(e) = registry.entries.get_mut(&replaced) {
                e.superseded = true;
            }
            taking_over = Some(replaced);
        }
        registry
            .by_user
            .entry(user.clone())
            .or_default()
            .insert(conn);
        registry.entries.insert(
            conn,
            Entry {
                user: user.clone(),
                doc: doc.clone(),
                client: client.clone(),
                token_hash: token_hash.to_owned(),
                outbox,
                settled: None,
                recheck: false,
                room: None,
                revoked: None,
                superseded: false,
                taking_over,
            },
        );
        Ok(Registration {
            hub: self.clone(),
            conn,
        })
    }

    /// How many document connections the account has registered.
    pub fn connections_of(&self, user: &str) -> usize {
        lock(&self.inner.registry)
            .by_user
            .get(user)
            .map_or(0, |c| c.len())
    }

    /// Open rooms and registered connections.
    pub fn gauges(&self) -> Gauges {
        Gauges {
            rooms: lock(&self.inner.rooms).len(),
            connections: lock(&self.inner.registry).entries.len(),
        }
    }

    /// Closes the settled connections `pick` chooses with `code` and `reason` (and tells their
    /// rooms), and marks the unsettled ones it chooses for a recheck. Answers how many were closed.
    /// `user_wide`: the access of the accounts it closes ended (their detached locks go too).
    fn close_where(
        &self,
        code: u16,
        reason: &str,
        user_wide: bool,
        pick: impl Fn(&Entry) -> bool,
    ) -> usize {
        let (closed, revoked) = {
            let mut registry = lock(&self.inner.registry);
            let mut revoked = Vec::new();
            let mut closed = 0;
            for (conn, entry) in registry.entries.iter_mut().filter(|(_, e)| pick(e)) {
                if entry.settled.is_some() {
                    revoked.extend(entry.revoke(*conn, code, reason, user_wide));
                    closed += 1;
                } else {
                    entry.recheck = true;
                }
            }
            (closed, revoked)
        };
        tell_rooms(revoked, code, reason, user_wide);
        closed
    }

    /// Tells every open room `pick` chooses that the access of `user` there ended: their locks
    /// that wait detached (a client that went away a moment before) are released.
    fn release_detached(
        &self,
        user: &Id,
        code: u16,
        reason: &str,
        pick: impl Fn(&RoomHandle) -> bool,
    ) {
        let rooms: Vec<RoomHandle> = lock(&self.inner.rooms)
            .values()
            .filter(|r| pick(r))
            .cloned()
            .collect();
        for room in rooms {
            room.revoke(None, user, code, reason, true);
        }
    }

    /// Closes (4401) every connection of an account that no longer exists or may no longer sign
    /// in, or whose sessions all ended (password changed or reset, signed out everywhere).
    pub async fn drop_account(&self, user: &Id) {
        self.drop_account_with(user, close::SIGN_IN, "sign in again")
            .await;
    }

    /// [`Hub::drop_account`] with another close code and reason (4410 `reconnect` when it is not
    /// certain the sessions ended: see [`crate::state::Committed`]).
    pub async fn drop_account_with(&self, user: &Id, code: u16, reason: &str) {
        self.record(format!("drop_account {user}"));
        let n = self.close_where(code, reason, true, |e| e.user == *user);
        self.release_detached(user, code, reason, |_| true);
        metrics::dropped(DropCause::Account, n);
    }

    /// Closes (4401) every connection made with the session of this token hash (it was ended).
    pub async fn drop_session(&self, token_hash: &str) {
        self.drop_session_with(token_hash, close::SIGN_IN, "sign in again")
            .await;
    }

    /// [`Hub::drop_session`] with another close code and reason.
    pub async fn drop_session_with(&self, token_hash: &str, code: u16, reason: &str) {
        self.record("drop_session".to_owned());
        // (One session: the other clients of the account keep their detached locks.)
        let n = self.close_where(code, reason, false, |e| e.token_hash == token_hash);
        metrics::dropped(DropCause::Session, n);
    }

    /// After a user's role in an organisation or one of its projects changed: reads the role of
    /// each of their connections to the organisation's documents again, and closes (4403) those
    /// whose role went or is not the one they joined with. Connections whose role did not change
    /// stay. If the role cannot be read, the connection is closed.
    pub async fn drop_user(&self, store: &dyn Store, user: &Id, org: &Id) {
        self.record(format!("drop_user {user} {org}"));
        let settled: Vec<(ConnId, Id, ProjectRole)> = {
            let mut guard = lock(&self.inner.registry);
            let registry = &mut *guard;
            let mut out = Vec::new();
            let theirs: Vec<ConnId> = registry.of(user).collect();
            for conn in theirs {
                let Some(entry) = registry.entries.get_mut(&conn) else {
                    continue;
                };
                match &entry.settled {
                    Some((project, role)) => out.push((conn, project.clone(), *role)),
                    None => entry.recheck = true,
                }
            }
            out
        };
        // The role now, per project (read without holding the registry).
        let mut now: HashMap<Id, Option<Option<ProjectRole>>> = HashMap::new();
        for (_, project, _) in &settled {
            if now.contains_key(project) {
                continue;
            }
            let role = match store.project_by_id(project).await {
                // Another organisation's project: not this change's concern.
                Ok(Some(p)) if p.org_id != *org => None,
                Ok(Some(p)) => Some(role_in_project(store, user, &p).await.unwrap_or_else(|e| {
                    tracing::error!("could not read a role again, closing the connection: {e:?}");
                    None
                })),
                Ok(None) => Some(None),
                Err(e) => {
                    tracing::error!("could not read a role again, closing the connection: {e:#}");
                    Some(None)
                }
            };
            now.insert(project.clone(), role);
        }
        let changed: Vec<ConnId> = settled
            .into_iter()
            .filter(|(_, project, role)| {
                now.get(project)
                    .is_some_and(|r| r.is_some_and(|r| r != Some(*role)))
            })
            .map(|(conn, _, _)| conn)
            .collect();
        let reason = "your access changed";
        let (n, revoked) = {
            let mut registry = lock(&self.inner.registry);
            let mut revoked = Vec::new();
            let mut n = 0;
            for conn in &changed {
                if let Some(entry) = registry.entries.get_mut(conn) {
                    revoked.extend(entry.revoke(*conn, close::ACCESS_CHANGED, reason, false));
                    n += 1;
                }
            }
            (n, revoked)
        };
        tell_rooms(revoked, close::ACCESS_CHANGED, reason, false);
        metrics::dropped(DropCause::Access, n);
        // In the open documents of the organisation where the user may no longer edit, their
        // locks that wait detached go (whether or not they are connected there).
        let rooms: Vec<RoomHandle> = lock(&self.inner.rooms)
            .values()
            .filter(|r| r.org() == org)
            .cloned()
            .collect();
        let mut cannot_edit: HashMap<Id, bool> = HashMap::new();
        for room in rooms {
            let project = room.project().clone();
            if !cannot_edit.contains_key(&project) {
                let role = match store.project_by_id(&project).await {
                    Ok(Some(p)) => role_in_project(store, user, &p).await.unwrap_or(None),
                    _ => None,
                };
                let lost = role.is_none_or(|r| r == ProjectRole::Viewer);
                cannot_edit.insert(project.clone(), lost);
            }
            if cannot_edit.get(&project).copied().unwrap_or(true) {
                room.revoke(None, user, close::ACCESS_CHANGED, reason, true);
            }
        }
    }

    /// Closes every connection to a document (4404, telling the clients why) and unloads its room
    /// without waiting for it and without a snapshot (the document was deleted: a later join is
    /// refused because the document is gone).
    pub async fn close_document(&self, doc: &Id, reason: &str) {
        self.record(format!("close_document {doc}"));
        let n = self.close_where(close::GONE, reason, false, |e| e.doc == *doc);
        metrics::dropped(DropCause::Document, n);
        let room = lock(&self.inner.rooms).remove(doc);
        if let Some(room) = room {
            let reason = reason.to_owned();
            tokio::spawn(async move { room.close(close::GONE, &reason, false).await });
        }
    }

    /// Sends a message to everyone who has a document open; nobody when its room is not open
    /// (never opens one).
    pub async fn notify(&self, doc: &str, msg: oswx_core::wire::ServerMsg) {
        if let Some(room) = self.loaded(doc) {
            room.notify(msg).await;
        }
    }

    /// The first people connected to a document (one entry per person, in the order they joined,
    /// at most [`room::MAX_ONLINE`]), and how many people are connected in all. Nobody when the
    /// document's room is not open; never opens one, never waits for one.
    pub async fn online(&self, doc: &Id) -> (Vec<Online>, usize) {
        self.loaded(doc)
            .map_or_else(|| (Vec::new(), 0), |room| room.online())
    }

    // ------------------------------------------------------------------ sweeper

    /// Starts the sweeper unless it runs already. It holds the hub weakly: it stops when the hub
    /// is gone or shutting down.
    fn ensure_sweeper(&self, every: f64) {
        let mut sweeper = lock(&self.inner.sweeper);
        if sweeper.is_some() {
            return;
        }
        let hub: Weak<Inner> = Arc::downgrade(&self.inner);
        let every = Duration::from_secs_f64(every.max(0.001));
        *sweeper = Some(tokio::spawn(async move {
            let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticks.tick().await;
                let Some(inner) = hub.upgrade() else {
                    return;
                };
                if inner.closing.load(Ordering::SeqCst) {
                    return;
                }
                Hub { inner }.sweep();
            }
        }));
    }

    /// One round of the sweeper: asks every open room to sweep and unload itself if it may,
    /// without waiting for any. Answers how many rooms were asked (a room that could not be asked,
    /// its queue full or its last sweep not yet taken, is skipped this round).
    pub fn sweep(&self) -> usize {
        let rooms: Vec<RoomHandle> = lock(&self.inner.rooms).values().cloned().collect();
        rooms.iter().filter(|r| r.sweep()).count()
    }

    // ------------------------------------------------------------------ shutdown

    /// Whether the hub is shutting down (no new rooms, no new connections).
    pub fn is_closing(&self) -> bool {
        self.inner.closing.load(Ordering::SeqCst)
    }

    /// Closes every room: each stops taking frames, waits briefly for the journal's answers to
    /// changes in flight, closes its connections with `GOING_AWAY` and writes a final snapshot if
    /// it has new changes. No room opens and no connection registers afterwards. Calling it again
    /// does nothing more.
    pub async fn shutdown(&self) {
        self.inner.closing.store(true, Ordering::SeqCst);
        if let Some(task) = lock(&self.inner.sweeper).take() {
            task.abort();
        }
        let rooms: Vec<RoomHandle> = lock(&self.inner.rooms).drain().map(|(_, r)| r).collect();
        let closing = rooms
            .iter()
            .map(|r| r.close(close::GOING_AWAY, "the server is shutting down", true));
        if tokio::time::timeout(SHUTDOWN_WAIT, futures_util::future::join_all(closing))
            .await
            .is_err()
        {
            tracing::warn!("some rooms did not close in time");
        }
    }
}

/// What a room of `doc` starts from: the document and project rows, the lease (acquired before the
/// head is read, so that no write of an older holder can land after the read), and the head.
async fn load(state: &AppState, doc: &str) -> Result<RoomInit, OpenError> {
    let document = state
        .store
        .document_by_id(doc)
        .await?
        .filter(|d| d.deleted_at.is_none())
        .ok_or(OpenError::NoDocument)?;
    let project = state
        .store
        .project_by_id(&document.project_id)
        .await?
        .ok_or(OpenError::NoDocument)?;
    let epoch = lease::acquire(
        state.store.as_ref(),
        doc,
        &state.node_id,
        state.clock.now(),
        LEASE_TTL,
    )
    .await?;
    let (head, _) = load_head(state.store.as_ref(), state.blobs.as_ref(), doc)
        .await
        .map_err(|e| match e {
            LoadError::NoDocument => OpenError::NoDocument,
            other => OpenError::Failed(other.into()),
        })?;
    let snapshot_seq = state
        .store
        .snapshot_at_or_before(doc, i64::try_from(head.seq).unwrap_or(i64::MAX))
        .await?
        .map_or(0, |s| u64::try_from(s.seq).unwrap_or(0));
    Ok(RoomInit {
        doc: doc.to_owned(),
        project: project.id,
        org: project.org_id,
        format_version: document.format_version,
        assembly: document.kind == "assembly",
        head,
        epoch,
        snapshot_seq,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_registry_entry_never_prints_its_token_hash() {
        let entry = Entry {
            user: "u".into(),
            doc: "d".into(),
            client: Arc::from("u.c"),
            token_hash: "5ecre7hash".into(),
            outbox: Outbox::new(1024),
            settled: None,
            recheck: false,
            room: None,
            revoked: None,
            superseded: false,
            taking_over: None,
        };
        let printed = format!("{entry:?}");
        assert!(printed.contains("u.c"), "{printed}");
        assert!(!printed.contains("5ecre7hash"), "{printed}");
    }

    fn connecting<'a>(
        conn: ConnId,
        user: &'a Id,
        doc: &'a Id,
        client: &'a Arc<str>,
    ) -> Connecting<'a> {
        Connecting {
            conn,
            user,
            doc,
            client,
            token_hash: "h",
            outbox: Outbox::new(1024),
        }
    }

    #[test]
    fn at_the_limit_one_take_over_at_a_time_passes() {
        let hub = Hub::new();
        let (user, doc) = ("u".to_owned(), "d".to_owned());
        let (desk, laptop): (Arc<str>, Arc<str>) = (Arc::from("u.desk"), Arc::from("u.laptop"));
        let _a = hub.register(connecting(1, &user, &doc, &desk), 2).unwrap();
        let _b = hub
            .register(connecting(2, &user, &doc, &laptop), 2)
            .unwrap();
        // A new client: refused.
        let other: Arc<str> = Arc::from("u.phone");
        assert!(matches!(
            hub.register(connecting(3, &user, &doc, &other), 2),
            Err(RegisterError::TooMany)
        ));
        // The desk again takes over its connection; a second at the same time does not.
        let first = hub.register(connecting(4, &user, &doc, &desk), 2).unwrap();
        assert!(matches!(
            hub.register(connecting(5, &user, &doc, &desk), 2),
            Err(RegisterError::TooMany)
        ));
        assert_eq!(
            hub.connections_of(&user),
            3,
            "the limit passed by one at most"
        );
        // The take-over never joined: the desk may try again.
        drop(first);
        assert_eq!(hub.connections_of(&user), 2);
        let _again = hub.register(connecting(6, &user, &doc, &desk), 2).unwrap();
        // Another document is not a take-over.
        let elsewhere = "e".to_owned();
        assert!(matches!(
            hub.register(connecting(7, &user, &elsewhere, &laptop), 2),
            Err(RegisterError::TooMany)
        ));
    }
}
