//! A room: one tokio task per open document, owning everything live about it.
//!
//! The task owns the **durable head** (what the journal has made permanent), the **tentative head**
//! (the durable head with the changes decided but not yet durable applied in order), the lease epoch
//! every record is written under, the connections, the recent change ids and the refused ones. It
//! receives commands ([`RoomCmd`]) on bounded channels (frames from readers on one, everything else on
//! another) and the results of work it started (journal outcomes, blob checks, big decisions,
//! welcomes, snapshots) on a third. It never awaits slow I/O while serving: that work runs in tasks of
//! its own and comes back as a message, and the room goes on with other commands meanwhile.
//!
//! # Changes
//!
//! See the `change` module: a change is judged against the tentative head, handed to the journal, and only
//! when the journal reports it durable is the durable head moved, the change broadcast and its sender
//! told `change.accepted`. Nothing that can fail runs after that report. The room decides change
//! `n + 1` while `n` is still being written (pipelining); if the journal refuses `n` for a cause of
//! its own, only `n` is answered, and every change decided after it is judged again, in order,
//! against the durable head (each then written or refused on its merits; its sender sees only the
//! final answer).
//! If the journal cannot say whether `n` was written, or the lease was lost, the room unloads at once
//! and closes every connection with [`close::MOVED`]: clients reconnect (a fresh room loads what the
//! store has) and resend; a resend whose row did land is answered with its original seq.
//!
//! Answers to one connection go out in the order it sent the messages they answer, whatever order
//! they are settled in; every client sees changes in seq order.
//!
//! # Joining
//!
//! A join registers the connection at once, at the durable seq `H` of that moment, and builds its
//! welcome (the document at `H`, or the change events up to `H`) in a task of its own. Until the
//! welcome is ready the connection's writer holds back; what is broadcast meanwhile (every change
//! after `H`) queues behind the welcome. So a joiner misses no change, and gets none twice.
//!
//! # Locks, presence, who is here
//!
//! See the `locks` module (advisory leases on paths that also stop other clients' changes) and the
//! `presence` module (pointers, selections and drafts relayed at once, never stored; the online
//! list the document list reads without asking the room). The room ticks every
//! `sweep_interval` seconds while it is loaded; a tick expires locks, retries a snapshot it owes
//! and closes connections whose session has expired (4401).
//!
//! # Leaving
//!
//! The hub closes connections whose access ended straight through their [`Outbox`] (whatever the
//! room is busy with); the room processes no frame of a connection whose outbox is closed, and
//! removes it when its reader reports it gone. The hub's sweeper asks each room
//! ([`RoomCmd::Sweep`]) whether it may unload: a room with no connections, no locks, nothing in
//! flight and idle for `room_idle` seconds writes a final snapshot (if it has changes since the
//! last), leaves the hub, and ends. A join that reaches a room which has begun to end is handed
//! back to the joiner, which opens the document again.
//!
//! A connection the hub closed because its access ended (4401, 4403, 4404) has its locks released
//! at once, whichever of its `Leave` and the hub's `Revoked` reaches the room first; the hub's
//! `Revoked` also releases the account's locks that already wait detached.
//!
//! A new connection of the same client replaces the old one (4409) and inherits none of its work:
//! what the old one had not yet handed to the journal is dropped where it comes back, what it had
//! handed over completes. The client resends what it has no answer for, and each resend is answered
//! once: by the change still being written (it waits for it), from memory once it is durable, after
//! a duplicate check in flight, or judged afresh under the new connection's access. Every
//! connection receives every durable change as an event, in seq order, except the connections
//! waiting for its answer (they get `accepted`): a new connection that had not resent its client's
//! change yet when it became durable receives it as an event, and its resend is then answered
//! `accepted` with the same seq. Its locks stay with the client (released if the new connection may
//! only look).

mod change;
pub mod conn;
mod history;
mod lifecycle;
mod locks;
mod people;
mod presence;
mod rebuild;
mod util;
mod welcome;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use oswx_core::wire::{self, ClientMsg, Peer, ServerMsg};
use oswx_core::{ClientId, Head, Seq};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::hub::Online;
use crate::security::ProjectRole;
use crate::state::AppState;
use crate::store::Id;
use util::Bounded;

pub use conn::{ConnId, Heard, Outbox};
pub use locks::MAX_LOCKS;
pub use presence::{LIVE_TICK, MAX_ONLINE};
pub use rebuild::{BLAME_KEEP, MAX_BROKE};

/// WebSocket close codes the server uses, in one place.
pub mod close {
    /// The server is shutting down.
    pub const GOING_AWAY: u16 = 1001;
    /// The WebSocket layer could not read a frame (malformed, or far over the size limit).
    pub const PROTOCOL_ERROR: u16 = 1002;
    /// The client kept doing what it must not (repeated duplicate change ids at the journal).
    pub const POLICY: u16 = 1008;
    /// A frame was over the size limit.
    pub const TOO_BIG: u16 = 1009;
    /// Something went wrong on the server; try again.
    pub const SERVER_ERROR: u16 = 1011;
    /// The first frame was not a valid hello (or did not come in time).
    pub const BAD_HELLO: u16 = 4400;
    /// The session is not valid: sign in again.
    pub const SIGN_IN: u16 = 4401;
    /// The account's access to the document changed (its role in the project went or is no
    /// longer the one it connected with): reconnect to get the new one.
    pub const ACCESS_CHANGED: u16 = 4403;
    /// The document does not exist, or no longer does, or is not yours to open.
    pub const GONE: u16 = 4404;
    /// The connection did not read fast enough (or the server could not take it right now).
    pub const TOO_SLOW: u16 = 4408;
    /// The same client connected again; this older connection is replaced.
    pub const REPLACED: u16 = 4409;
    /// The document moved or was reloaded: reconnect (and resend what was not acknowledged).
    pub const MOVED: u16 = 4410;
    /// Whether a close with `code` means the connection's access ended (its locks are released at
    /// once rather than kept for the grace period). A session that expired while connected closes
    /// with 4401 too, and so releases its locks at once: an expired session has no access.
    pub fn ends_access(code: u16) -> bool {
        matches!(code, SIGN_IN | ACCESS_CHANGED | GONE)
    }
    /// The account has as many document connections open as it may
    /// (`max_connections_per_user`): close one first. Not 4408 (`TOO_SLOW`), which a client
    /// answers by reconnecting at once; reconnecting does not help here.
    pub const TOO_MANY: u16 = 4429;
    /// The client speaks another protocol version.
    pub const PROTOCOL: u16 = 4426;
}

/// How many frames may wait for a room before readers wait.
const FRAMES_QUEUE: usize = 256;
/// How many other commands may wait for a room.
const CONTROL_QUEUE: usize = 256;
/// The most changes decided but not yet durable; past it the room stops taking frames until the
/// journal catches up.
const MAX_TENTATIVE: usize = 4096;
/// The most messages of one connection that may wait for its earlier one (a blob check, a journal
/// lookup); past it the connection is closed as too slow.
const MAX_PARKED: usize = 1024;
/// The most answers one connection may have waiting; past it the connection is closed as too slow.
pub const MAX_REPLIES: usize = 2000;
/// How many users' journal duplicates a room counts (the oldest forgotten first).
const DUPLICATE_USERS: usize = 1024;
/// How long closing waits for the journal's answers to changes in flight.
const CLOSE_WAIT: Duration = Duration::from_secs(5);
/// How long a snapshot may take (a final one when a room closes, or one in the background) before
/// it is given up ([`Hooks::snapshot_wait`] by default).
pub const SNAPSHOT_WAIT: Duration = Duration::from_secs(30);
/// How long a handshake waits for a room to take its join before telling the client to reconnect
/// ([`Hooks::join_wait`] by default).
pub const JOIN_WAIT: Duration = Duration::from_secs(10);
/// A snapshot's index keeps this many changes of history to judge late changes against; older
/// marks are pruned just before the snapshot is written ([`Hooks::index_keep`] by default).
pub const INDEX_KEEP: Seq = 100_000;
/// A connection behind on soft frames is caught up (sent `peer.list`) at most this often
/// ([`Hooks::catch_up_every`] by default).
pub const CATCH_UP_EVERY: Duration = Duration::from_secs(1);
/// A connection behind on soft frames whose socket has taken nothing for this long is closed as
/// too slow ([`Hooks::frozen_after`] by default).
pub const FROZEN_AFTER: Duration = Duration::from_secs(30);

/// Knobs of the rooms that tests turn; a running server keeps the defaults.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct Hooks {
    /// How long judging a big change off the task waits first (to make races deterministic).
    pub judge_delay: Duration,
    /// History the index keeps before a snapshot prunes it ([`INDEX_KEEP`]).
    pub index_keep: Seq,
    /// The most change ids a room remembers ([`MAX_REMEMBERED`]).
    pub max_remembered: usize,
    /// The most stored operation bytes one page of history, or one catch-up in a welcome, carries
    /// ([`PAGE_BYTES`]).
    pub page_bytes: u64,
    /// Rooms of the documents named here stop dead when the hub's sweeper reaches them, until
    /// released (a room that does not answer).
    pub stall: Arc<Stall>,
    /// Shut: the steps after a REST commit ([`crate::state::AppState::after_commit`]) wait here
    /// before they start (so that a test can drop the request meanwhile).
    pub after_commit: Arc<Gate>,
    /// How long a handshake waits for a room to take its join ([`JOIN_WAIT`]).
    pub join_wait: Duration,
    /// Shut: a handshake whose join wait ran out stops here before it closes the connection.
    pub after_join_timeout: Arc<Gate>,
    /// Milliseconds the hub's `Revoked` waits before it is sent (0: sent at once), to force the
    /// connection's `Leave` to arrive first.
    pub revoke_delay_ms: Arc<std::sync::atomic::AtomicU64>,
    /// Which of a revoked connection's `Revoked` and `Leave` reached its room first.
    pub revoke_order: Arc<RevokeOrder>,
    /// How long a snapshot may take ([`SNAPSHOT_WAIT`]).
    pub snapshot_wait: Duration,
    /// How long a request waits for the mail it sends ([`crate::mail::MAIL_WAIT`]).
    pub mail_wait: Duration,
    /// How often a connection behind on soft frames may be caught up ([`CATCH_UP_EVERY`]).
    pub catch_up_every: Duration,
    /// How long a connection behind on soft frames may go without its socket taking a frame
    /// ([`FROZEN_AFTER`]).
    pub frozen_after: Duration,
}

/// A gate tests shut to hold work at a point and open to let it go on.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct Gate {
    shut: std::sync::atomic::AtomicBool,
    waiting: AtomicUsize,
    changed: tokio::sync::Notify,
}

impl Gate {
    /// From now on, whoever reaches the gate waits.
    pub fn shut(&self) {
        self.shut.store(true, Ordering::SeqCst);
    }

    /// Lets everyone through, now and later.
    pub fn open(&self) {
        self.shut.store(false, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    /// Waits while the gate is shut.
    pub async fn pass(&self) {
        if !self.shut.load(Ordering::SeqCst) {
            return;
        }
        self.waiting.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.shut.load(Ordering::SeqCst) {
                break;
            }
            changed.await;
        }
        self.waiting.fetch_sub(1, Ordering::SeqCst);
    }

    /// Whether whoever reaches the gate now waits.
    pub fn is_shut(&self) -> bool {
        self.shut.load(Ordering::SeqCst)
    }

    /// How many wait at the gate.
    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::SeqCst)
    }

    /// Waits until at least `n` wait at the gate.
    pub async fn until_waiting(&self, n: usize) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.waiting() >= n {
                return;
            }
            changed.await;
        }
    }
}

/// Counts whether the hub's `Revoked` for a connection found it still in its room (see
/// [`Hooks::revoke_order`]).
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct RevokeOrder {
    /// The hub's `Revoked` found the connection still there.
    pub revoked_first: AtomicUsize,
    /// The connection was gone already: its own `Leave` came first, or a newer connection of its
    /// client had replaced it.
    pub gone_first: AtomicUsize,
}

/// Rooms tests stop dead (see [`Hooks::stall`]).
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct Stall {
    docs: Mutex<std::collections::HashSet<String>>,
    gate: Gate,
}

impl Stall {
    /// The room of `doc` stops at its next sweep by the hub, until [`Stall::release`].
    pub fn stall(&self, doc: &str) {
        self.gate.shut();
        self.docs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(doc.to_owned());
    }

    /// Every stopped room goes on.
    pub fn release(&self) {
        self.docs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.gate.open();
    }

    /// Waits until `n` rooms are stopped.
    pub async fn until_stopped(&self, n: usize) {
        self.gate.until_waiting(n).await;
    }

    async fn hold(&self, doc: &str) {
        let stalled = self
            .docs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(doc);
        if stalled {
            self.gate.pass().await;
        }
    }
}

/// The most stored operation bytes (the journal's `ops` column) one history page or one welcome's
/// catch-up carries: a page stops before the row that would pass it (but always holds one row),
/// and a catch-up that would pass it sends the document instead. It counts stored (MessagePack)
/// bytes: a page's JSON rendering can be several times larger.
pub const PAGE_BYTES: u64 = 16 << 20;

/// The most change ids a room remembers, whatever its snapshots; past it the oldest are forgotten
/// (and their clients' later unknown ids are looked up in the journal).
pub const MAX_REMEMBERED: usize = 100_000;

/// How many of the changes most recently taken back (undone or reverted) a room remembers.
pub const UNDONE_KEEP: usize = 4096;

impl Default for Hooks {
    fn default() -> Self {
        Self {
            judge_delay: Duration::ZERO,
            index_keep: INDEX_KEEP,
            max_remembered: MAX_REMEMBERED,
            page_bytes: PAGE_BYTES,
            stall: Arc::default(),
            after_commit: Arc::default(),
            join_wait: JOIN_WAIT,
            after_join_timeout: Arc::default(),
            revoke_delay_ms: Arc::default(),
            revoke_order: Arc::default(),
            snapshot_wait: SNAPSHOT_WAIT,
            mail_wait: crate::mail::MAIL_WAIT,
            catch_up_every: CATCH_UP_EVERY,
            frozen_after: FROZEN_AFTER,
        }
    }
}

/// Why a change the server made itself ([`RoomHandle::server_change`]) did not land.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ServerChangeError {
    /// It was judged and refused (a conflict with nothing; a name taken, an invalid operation).
    #[error("the change was refused: {0:?}")]
    Refused(oswx_core::Refusal),
    /// It was not written: the room is gone or closing, or the journal failed it.
    #[error("the change was not written")]
    NotWritten,
    /// Locks held by others stand in its way ([`ServerChangeOptions::locks`]); the conflicts name
    /// them (kind `locked`, `by` the holder's client id).
    #[error("the change was refused: locked by others")]
    Locked(Vec<oswx_core::Conflict>),
}

/// How a change the server made landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Landed {
    /// It was written as this seq (durable).
    Written(Seq),
    /// A restore whose target the document already was, as of this durable seq: nothing written.
    Unchanged(Seq),
}

impl Landed {
    /// The seq it answers either way.
    pub fn seq(self) -> Seq {
        match self {
            Landed::Written(seq) | Landed::Unchanged(seq) => seq,
        }
    }
}

/// A change the server makes itself, waiting for the room.
pub struct ServerChange {
    pub ops: Vec<oswx_core::Op>,
    /// For a restore: the state to reach (`ops` is then empty; see [`RoomHandle::restore`]).
    pub target: Option<oswx_core::Value>,
    pub command: String,
    /// The user it is made for (history shows them as its author).
    pub author: Id,
    pub author_name: String,
    pub options: ServerChangeOptions,
    pub reply: oneshot::Sender<Result<Landed, ServerChangeError>>,
}

/// How a change the server makes is judged.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerChangeOptions {
    /// Refuse it (`stale`) if another client changed what it writes since the current head when it
    /// was asked for. Off: it lands whatever happened (a restore); ids are never prefix-checked.
    pub check_stale: bool,
    /// Which locks refuse it (`locked`); by default none do.
    pub locks: LockRule,
}

/// Which locks stand in the way of a change the server makes (it opts in to being stopped).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum LockRule {
    /// No lock stops it.
    #[default]
    None,
    /// The rule for any client's change: a lock on (at, under or above) what it writes. The
    /// server is no client, so every such lock counts.
    Writes,
    /// As [`LockRule::Writes`], except the locks of this client (a change made for it, such as
    /// its revert: its own locks never stop it).
    WritesExcept(ClientId),
    /// Any lock held by a user other than the change's author, whatever it writes (a restore
    /// rewrites anything: it waits until nobody else is editing).
    AnyOfOtherUsers,
}

/// What an open room is: what it and the tests can see of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomStatus {
    /// The durable head's seq.
    pub seq: Seq,
    /// Changes decided and waiting for the journal.
    pub tentative: usize,
    /// The seq of the last snapshot written (or loaded).
    pub snapshot_seq: Seq,
    /// The lease epoch the room writes under.
    pub epoch: u64,
    /// How many frames the room has encoded (each broadcast is encoded once, whoever receives it).
    pub encodes: u64,
    /// Change ids remembered, and the entries of the order they are forgotten in.
    pub remembered: (usize, usize),
    /// Clients whose every change id is in memory.
    pub complete_clients: usize,
    /// Locks held, and how many of them wait for their client to come back.
    pub locks: (usize, usize),
    pub connections: Vec<ConnStatus>,
}

/// One connection of a room.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnStatus {
    pub client_id: String,
    pub queued_frames: usize,
    pub queued_bytes: usize,
    /// Answers it waits for (each counts toward [`MAX_REPLIES`]).
    pub waiting_answers: usize,
    /// It is behind on soft frames (who is here, presence, drafts): they are dropped for it.
    pub behind: bool,
}

/// A connection asking to join, made by the WebSocket handshake once the hello was checked.
pub struct JoinRequest {
    pub conn: ConnId,
    pub outbox: Arc<Outbox>,
    /// When the connection's reader last read a frame.
    pub heard: Arc<Heard>,
    pub user: Id,
    pub name: String,
    pub role: ProjectRole,
    /// `<user id>.<client id from the hello>`.
    pub client: ClientId,
    pub format_version: Option<i64>,
    pub last_seq: Option<Seq>,
    /// This client's most recent change ids in the document and their seqs (from the journal), so
    /// that its resends are recognised in memory whatever base they carry.
    pub recent_ids: Vec<(String, Seq)>,
    /// `recent_ids` are all the ids this client ever journaled here (the journal had fewer than a
    /// join loads).
    pub ids_complete: bool,
    /// The room (`RoomHandle::id`) whose lease was held when `recent_ids` were read; another room
    /// refuses the join (the client reconnects and they are read again).
    pub room: u64,
    /// When the session the connection signed in with expires (the room closes it then, 4401).
    pub session_expires: f64,
}

/// What became of a join a room was sent.
pub enum JoinAnswer {
    /// The room took it (and either registered the connection or closed it).
    Taken,
    /// The room had begun to end: open the document again.
    Bounced(Box<JoinRequest>),
}

/// Why [`RoomHandle::join`] did not join.
pub enum JoinError {
    /// The room is gone or ending: here is the request back, to try another.
    Gone(Box<JoinRequest>),
    /// The room ended without answering (it failed): the connection should reconnect.
    Lost,
}

/// A command for a room.
pub enum RoomCmd {
    /// A join, and where to say what became of it.
    Join(Box<JoinRequest>, Option<oneshot::Sender<JoinAnswer>>),
    /// The connection's reader stopped: the client went away.
    Leave(ConnId),
    /// A decoded message from a connection; `size` is the frame's length in bytes.
    Frame {
        conn: ConnId,
        msg: Box<ClientMsg>,
        size: usize,
    },
    /// A frame of a connection that could not be read: answered `error` in its turn.
    Malformed {
        conn: ConnId,
        code: &'static str,
        message: String,
    },
    /// A change the server makes itself.
    ServerChange(Box<ServerChange>),
    /// A message for every connection (`version.created`, and `reference.update` from Task 13).
    Notify(Box<ServerMsg>),
    /// A sweep, as the room's own timer makes every `sweep_interval` seconds: locks expire, a
    /// snapshot still owed (one that failed) is tried again, and connections whose session expired
    /// close.
    Tick,
    /// The hub's sweeper: a tick, then the room unloads itself if it may (see the module
    /// documentation).
    Sweep,
    /// The hub revoked access (it has closed the connection's outbox with `code` already): the
    /// room removes the connection `conn` (if any) at once, releasing its locks, and when
    /// `user_wide` the locks of `user` that wait detached here too.
    Revoked {
        conn: Option<ConnId>,
        user: Id,
        code: u16,
        reason: String,
        user_wide: bool,
    },
    /// Stop: wait (briefly) for the journal's answers to changes in flight, close every connection
    /// with `code`, write a final snapshot if `snapshot` and there is anything new, then answer
    /// `done`.
    Close {
        code: u16,
        reason: String,
        snapshot: bool,
        done: oneshot::Sender<()>,
    },
    /// The durable head.
    Head(oneshot::Sender<Head>),
    /// For tests: forget every remembered change id but keep believing memory complete, as a room
    /// that lost entries in a race would.
    #[doc(hidden)]
    ForgetChangeIds,
    Status(oneshot::Sender<RoomStatus>),
}

/// The room is gone (unloaded or stopping); open it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the room is gone")]
pub struct RoomGone;

/// What a room publishes for others to read without asking it (and so without waiting for it).
#[derive(Debug, Default)]
struct Shared {
    /// The online list: the first people (one entry per person, in the order they joined) and
    /// how many there are.
    online: Mutex<(Vec<Online>, usize)>,
    /// Locks held, and how many of those are detached.
    locks: AtomicUsize,
    detached: AtomicUsize,
    /// The room has begun to unload because it was idle: the hub opens no join into it.
    unloading: std::sync::atomic::AtomicBool,
    /// A [`RoomCmd::Sweep`] waits in its queue (the sweeper sends no second one).
    sweep_queued: std::sync::atomic::AtomicBool,
}

/// How the rest of the server reaches a room. Cloning is cheap.
#[derive(Clone, Debug)]
pub struct RoomHandle {
    id: u64,
    control: mpsc::Sender<RoomCmd>,
    frames: mpsc::Sender<RoomCmd>,
    shared: Arc<Shared>,
    /// The document's organisation and project.
    org: Id,
    project: Id,
    hooks: Arc<Hooks>,
}

impl RoomHandle {
    /// Which room this is: unique within the process (a document reopened gets a new one).
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Whether the room's task has ended.
    pub fn is_closed(&self) -> bool {
        self.control.is_closed()
    }

    /// Sends a command (waiting while the room's queue is full). The command comes back if the
    /// room is gone.
    pub async fn send(&self, cmd: RoomCmd) -> Result<(), Box<RoomCmd>> {
        let channel = match &cmd {
            // Presence and drafts take the other way in, so that a room busy with changes (not
            // taking frames) still relays them at once.
            RoomCmd::Frame { msg, .. } if presence::is_live(msg) => &self.control,
            RoomCmd::Frame { .. } | RoomCmd::Malformed { .. } => &self.frames,
            _ => &self.control,
        };
        channel.send(cmd).await.map_err(|e| Box::new(e.0))
    }

    /// The document's organisation.
    pub fn org(&self) -> &Id {
        &self.org
    }

    /// The document's project.
    pub fn project(&self) -> &Id {
        &self.project
    }

    /// Whether the room has begun to unload (idle): it takes no more joins.
    pub fn is_unloading(&self) -> bool {
        self.shared.unloading.load(Ordering::SeqCst)
    }

    /// Waits until the room's task has ended (or no longer takes commands).
    pub async fn ended(&self) {
        self.control.closed().await;
    }

    /// Asks the room to take a connection, and waits until it has (or has closed it). The request
    /// comes back if the room is gone or ending.
    pub async fn join(&self, req: JoinRequest) -> Result<(), JoinError> {
        let (answer, answered) = oneshot::channel();
        if let Err(cmd) = self.send(RoomCmd::Join(Box::new(req), Some(answer))).await {
            return match *cmd {
                RoomCmd::Join(req, _) => Err(JoinError::Gone(req)),
                _ => unreachable!("send hands back what it was given"),
            };
        }
        match answered.await {
            Ok(JoinAnswer::Taken) => Ok(()),
            Ok(JoinAnswer::Bounced(req)) => Err(JoinError::Gone(req)),
            Err(_) => Err(JoinError::Lost),
        }
    }

    /// Tells the room, without waiting, that the hub revoked a connection's access (and closed
    /// its outbox with `code`): the room removes it at once and releases its locks. Taken even
    /// while the room judges a big change or waits for the journal; if the room's queue is full it
    /// is sent from a task of its own.
    ///
    /// With no `conn`, only the user's detached locks (when `user_wide`) are released: the hub
    /// tells every room the account's access may have ended in, not only those it is connected to.
    pub fn revoke(
        &self,
        conn: Option<ConnId>,
        user: &Id,
        code: u16,
        reason: &str,
        user_wide: bool,
    ) {
        let cmd = RoomCmd::Revoked {
            conn,
            user: user.clone(),
            code,
            reason: reason.to_owned(),
            user_wide,
        };
        let delay = self.hooks.revoke_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            let control = self.control.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let _ = control.send(cmd).await;
            });
            return;
        }
        if let Err(mpsc::error::TrySendError::Full(cmd)) = self.control.try_send(cmd) {
            let control = self.control.clone();
            tokio::spawn(async move {
                let _ = control.send(cmd).await;
            });
        }
    }

    /// The hub's sweeper: asks the room, without waiting, to sweep and unload itself if it may.
    /// Answers false when the room could not be asked this time (gone, its queue full, or a sweep
    /// already waiting in it).
    pub fn sweep(&self) -> bool {
        if self.shared.sweep_queued.swap(true, Ordering::SeqCst) {
            return false;
        }
        if self.control.try_send(RoomCmd::Sweep).is_err() {
            self.shared.sweep_queued.store(false, Ordering::SeqCst);
            return false;
        }
        true
    }

    pub(crate) async fn frame(
        &self,
        conn: ConnId,
        msg: ClientMsg,
        size: usize,
    ) -> Result<(), RoomGone> {
        let cmd = RoomCmd::Frame {
            conn,
            msg: Box::new(msg),
            size,
        };
        self.send(cmd).await.map_err(|_| RoomGone)
    }

    pub(crate) async fn malformed(
        &self,
        conn: ConnId,
        code: &'static str,
        message: String,
    ) -> Result<(), RoomGone> {
        let cmd = RoomCmd::Malformed {
            conn,
            code,
            message,
        };
        self.send(cmd).await.map_err(|_| RoomGone)
    }

    /// Makes a change on behalf of the server (a restore, a revert): judged against the current
    /// head under [`crate::snapshot::SERVER_CLIENT`], journaled without a client id, broadcast to
    /// every connection. Answers its seq once it is durable.
    pub async fn server_change(
        &self,
        ops: Vec<oswx_core::Op>,
        command: &str,
        author: &str,
        author_name: &str,
        options: ServerChangeOptions,
    ) -> Result<Seq, ServerChangeError> {
        self.submit(ops, None, command, author, author_name, options)
            .await
            .map(Landed::seq)
    }

    /// Puts the document back to `target` as one change the server makes (a restore). The
    /// operations are `diff(head, target)` worked out when the room judges it, off its task,
    /// against the very head they are applied to: the document at the answered seq is exactly
    /// `target`. Every change that became tentative before the restore was judged is overwritten
    /// by it; changes judged after it apply on top. When the head is `target` already, nothing is
    /// written: [`Landed::Unchanged`] with that head's seq, answered only once it is durable (if
    /// the journal refuses one of the changes it waits for, the restore is judged again, as any
    /// change decided after a refused one is). Locks are judged at the same moment.
    pub async fn restore(
        &self,
        target: oswx_core::Value,
        command: &str,
        author: &str,
        author_name: &str,
        options: ServerChangeOptions,
    ) -> Result<Landed, ServerChangeError> {
        self.submit(
            Vec::new(),
            Some(target),
            command,
            author,
            author_name,
            options,
        )
        .await
    }

    async fn submit(
        &self,
        ops: Vec<oswx_core::Op>,
        target: Option<oswx_core::Value>,
        command: &str,
        author: &str,
        author_name: &str,
        options: ServerChangeOptions,
    ) -> Result<Landed, ServerChangeError> {
        let (reply, answer) = oneshot::channel();
        let change = ServerChange {
            ops,
            target,
            command: command.to_owned(),
            author: author.to_owned(),
            author_name: author_name.to_owned(),
            options,
            reply,
        };
        self.send(RoomCmd::ServerChange(Box::new(change)))
            .await
            .map_err(|_| ServerChangeError::NotWritten)?;
        answer.await.unwrap_or(Err(ServerChangeError::NotWritten))
    }

    /// Sends a message to every connection of the room (none if the room is gone).
    pub async fn notify(&self, msg: ServerMsg) {
        let _ = self.send(RoomCmd::Notify(Box::new(msg))).await;
    }

    pub(crate) async fn leave(&self, conn: ConnId) {
        let _ = self.send(RoomCmd::Leave(conn)).await;
    }

    /// The durable head, or `None` if the room is gone.
    pub async fn durable_head(&self) -> Option<Head> {
        let (tx, rx) = oneshot::channel();
        self.send(RoomCmd::Head(tx)).await.ok()?;
        rx.await.ok()
    }

    /// Who has the document open: the first [`MAX_ONLINE`] people (one entry per person, in the
    /// order they joined) and how many there are. Read from what the room published at its last
    /// join or leave; never waits for the room.
    pub fn online(&self) -> (Vec<Online>, usize) {
        self.shared
            .online
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Whether the room holds any lock (attached or waiting for its client): a room that does must
    /// stay loaded until they expire. Never waits for the room.
    pub fn holds_locks(&self) -> bool {
        self.shared.locks.load(Ordering::Relaxed) > 0
    }

    /// How many locks are detached (their client gone, within the grace period).
    pub fn detached_locks(&self) -> usize {
        self.shared.detached.load(Ordering::Relaxed)
    }

    /// Asks the room to sweep now (as its timer does).
    pub async fn tick(&self) {
        let _ = self.send(RoomCmd::Tick).await;
    }

    /// For tests: makes the room forget every change id it remembers (as if trimmed).
    #[doc(hidden)]
    pub async fn forget_change_ids(&self) {
        let _ = self.send(RoomCmd::ForgetChangeIds).await;
    }

    /// What the room is now, or `None` if it is gone.
    pub async fn status(&self) -> Option<RoomStatus> {
        let (tx, rx) = oneshot::channel();
        self.send(RoomCmd::Status(tx)).await.ok()?;
        rx.await.ok()
    }

    /// Closes the room (see [`RoomCmd::Close`]) and waits until it has.
    pub async fn close(&self, code: u16, reason: &str, snapshot: bool) {
        let (done, wait) = oneshot::channel();
        let cmd = RoomCmd::Close {
            code,
            reason: reason.to_owned(),
            snapshot,
            done,
        };
        if self.send(cmd).await.is_ok() {
            let _ = wait.await;
        }
    }
}

/// What a room starts from, loaded by the hub.
pub(crate) struct RoomInit {
    pub doc: Id,
    pub project: Id,
    pub org: Id,
    /// The document's `format_version`; a client with another one may only look.
    pub format_version: Option<i64>,
    /// The document is an assembly (its components' references are checked and recorded).
    pub assembly: bool,
    pub head: Head,
    /// The epoch [`crate::lease::acquire`] gave for this load.
    pub epoch: u64,
    /// The seq of the snapshot the head was loaded from.
    pub snapshot_seq: Seq,
}

/// An answer queued for a connection: ready, or waiting for the outcome of a change in the
/// pipeline (named by its ticket, which stays the same if the change is judged again).
enum Reply {
    Ready(Bytes),
    Waiting(change::Ticket),
}

/// A connection, as the room keeps it.
struct Conn {
    client: ClientId,
    user: Id,
    name: String,
    colour: String,
    /// Why the connection may only look, if it may.
    read_only: Option<String>,
    outbox: Arc<Outbox>,
    /// Answers not yet sent, in the order of the messages they answer.
    replies: VecDeque<Reply>,
    /// Messages waiting for an earlier one of this connection to be settled.
    parked: VecDeque<change::Incoming>,
    /// The frame bytes of `parked`.
    parked_bytes: usize,
    /// A message of this connection waits for work off the task (a blob check, a journal lookup,
    /// a big change being judged).
    busy: bool,
    /// When its reader last read a frame.
    heard: Arc<Heard>,
    /// The last presence and draft it sent, for the welcomes of those who join later.
    presence: Option<oswx_core::Plain>,
    draft: Option<oswx_core::Plain>,
    /// Its presence and draft not yet relayed to the others (the latest of each), encoded once.
    presence_news: Option<oswx_core::wire::LiveEntry>,
    draft_news: Option<oswx_core::wire::LiveEntry>,
    /// Its draft as a `draft` frame, made when a connection that caught up is first owed it, and
    /// shared by every such connection (forgotten when the draft changes, and once nobody is owed
    /// drafts).
    draft_frame: Option<Bytes>,
    /// Since it last caught up: what it is owed of the peers, in order (the presence `peer.list`
    /// had no room for, then every draft: it carries none), and how much of it it has been sent.
    owed: Option<(presence::OwedList, usize)>,
    /// Its allowance of presence and draft messages, and of `lock.acquire`s.
    live_rate: presence::Bucket,
    lock_rate: presence::Bucket,
    /// When it joined the room, in join order.
    joined: u64,
    /// When its session expires (clock seconds).
    expires: f64,
}

impl Conn {
    fn peer(&self) -> Peer {
        Peer {
            client_id: self.client.clone(),
            user_id: self.user.clone(),
            name: self.name.clone(),
            colour: self.colour.clone(),
            read_only: self.read_only.is_some(),
            presence: self.presence.clone(),
            draft: self.draft.clone(),
        }
    }
}

/// The work a room started, coming back.
enum Internal {
    Journal(change::Outcome),
    Lookup(change::Lookup),
    // (Boxed: the channel's blocks are sized by its largest message, and these are rare.)
    Blobs(Box<change::BlobCheck>),
    References(Box<change::RefCheck>),
    Decided(Box<change::Offloaded>),
    Duplicate(Box<change::DuplicateCheck>),
    Undo(history::UndoLookup),
    Revert(Box<history::RevertLookup>),
    Report(rebuild::ReportLookup),
    Welcome {
        conn: ConnId,
        result: anyhow::Result<Bytes>,
    },
    Snapshot {
        seq: Seq,
        ok: bool,
    },
    /// Time to announce the releases a connection made beyond its lock allowance.
    LockNews(ConnId),
}

struct Closing {
    code: u16,
    reason: String,
    snapshot: bool,
    /// Unloading because idle: the final snapshot is written while the room is still in the hub
    /// (which keeps joiners waiting for it to end), and it leaves the hub only after.
    idle: bool,
    done: Vec<oneshot::Sender<()>>,
    /// Stop waiting for the journal then.
    deadline: Instant,
}

struct Room {
    state: AppState,
    id: u64,
    doc: Id,
    project: Id,
    org: Id,
    format_version: Option<i64>,
    assembly: bool,
    epoch: u64,
    durable: Head,
    tentative: Head,
    /// Moves whenever `tentative` does, so that work judged against an older one is judged again.
    tentative_version: u64,
    pending: VecDeque<change::Pending>,
    /// Moves when pending changes are discarded: outcomes of the older generation are ignored.
    generation: u64,
    journal: mpsc::UnboundedSender<change::Submit>,
    internal: mpsc::UnboundedSender<Internal>,
    conns: HashMap<ConnId, Conn>,
    by_client: HashMap<ClientId, ConnId>,
    recent: change::Recent,
    refused: HashMap<ClientId, change::RefusedIds>,
    known_blobs: change::KnownBlobs,
    snapshot_seq: Seq,
    snapshot_task: Option<tokio::task::JoinHandle<()>>,
    /// Big changes being judged off the task: no frame is taken until they are back.
    paused: u32,
    /// Changes the server makes itself, waiting for their turn.
    server_queue: VecDeque<change::Item>,
    next_ticket: change::Ticket,
    /// Connections that sent a change again while the journal's seq for it is looked up.
    checking: HashMap<change::Ticket, Vec<ConnId>>,
    /// Journal duplicates per user (surviving reconnects), bounded.
    duplicates: util::Tally,
    /// Connections with parked messages that may go on.
    ready: VecDeque<ConnId>,
    /// Connections that broke their queue's limits, to be removed.
    slow: Vec<ConnId>,
    closing: Option<Closing>,
    encodes: Arc<AtomicU64>,
    locks: locks::Locks,
    /// What the room publishes for its handles.
    shared: Arc<Shared>,
    /// The join order of the next connection.
    next_join: u64,
    /// The most recent changes taken back (undone or reverted) by changes made durable here (the
    /// journal has every older one, and a lookup in flight takes far less time than [`UNDONE_KEEP`]
    /// more of them).
    undone: Bounded<Seq>,
    /// What does not rebuild, and what blaming needs.
    rebuilds: rebuild::Rebuilds,
    /// Restores found unchanged against a tentative head, waiting for that head's seq to become
    /// durable before they are answered.
    held_unchanged: Vec<(Seq, change::Item)>,
    /// Undos whose target's seq is being checked (a journal duplicate), by that change's ticket.
    undo_waiting: HashMap<change::Ticket, Vec<(ConnId, Arc<oswx_core::Change>, usize)>>,
    /// Since when nobody has been connected (clock seconds); `None` while someone is.
    idle_since: Option<f64>,
    /// No connection's session expires before this (clock seconds).
    next_expiry: f64,
    /// The last snapshot attempt failed: further failures are not logged as warnings until one
    /// succeeds (one warning per outage).
    snapshot_failing: bool,
    /// Connections with presence or drafts not yet relayed, each once.
    live_news: Vec<ConnId>,
    /// When presence and drafts were last relayed, and when they will be next.
    live_sent: Instant,
    live_due: Option<Instant>,
    /// Connections behind on soft frames, to be sent the peers once they have written theirs.
    behind: std::collections::HashSet<ConnId>,
    /// Connections that caught up and are still owed presence or drafts.
    owing: std::collections::HashSet<ConnId>,
}

fn next_room_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// After a room has finished: nobody can send it anything any more, and what was sent but not
/// taken is answered: joins go back to their joiners (who open the document again), a close is told
/// it is done, and everything else is dropped (its sender sees the room gone).
fn wind_up(control: &mut mpsc::Receiver<RoomCmd>, frames: &mut mpsc::Receiver<RoomCmd>) {
    control.close();
    frames.close();
    while let Ok(cmd) = control.try_recv() {
        match cmd {
            RoomCmd::Join(req, Some(answer)) => {
                let _ = answer.send(JoinAnswer::Bounced(req));
            }
            RoomCmd::Close { done, .. } => {
                let _ = done.send(());
            }
            _ => {}
        }
    }
    while frames.try_recv().is_ok() {}
}

/// Starts a room's task and answers its handle.
pub(crate) fn spawn(state: AppState, init: RoomInit) -> RoomHandle {
    let (control_tx, control_rx) = mpsc::channel(CONTROL_QUEUE);
    let (frames_tx, frames_rx) = mpsc::channel(FRAMES_QUEUE);
    let (internal_tx, internal_rx) = mpsc::unbounded_channel();
    let journal = change::pump(state.journal.clone(), internal_tx.clone());
    let (org, project) = (init.org.clone(), init.project.clone());
    let hooks = state.room_hooks.clone();
    let id = next_room_id();
    let shared = Arc::new(Shared::default());
    let room = Room {
        id,
        doc: init.doc,
        project: init.project,
        org: init.org,
        format_version: init.format_version,
        assembly: init.assembly,
        epoch: init.epoch,
        tentative: init.head.clone(),
        durable: init.head.clone(),
        tentative_version: 0,
        pending: VecDeque::new(),
        generation: 0,
        journal,
        internal: internal_tx,
        conns: HashMap::new(),
        by_client: HashMap::new(),
        recent: change::Recent::new(state.room_hooks.max_remembered),
        refused: HashMap::new(),
        known_blobs: change::KnownBlobs::default(),
        snapshot_seq: init.snapshot_seq,
        snapshot_task: None,
        paused: 0,
        server_queue: VecDeque::new(),
        next_ticket: 0,
        checking: HashMap::new(),
        duplicates: util::Tally::new(DUPLICATE_USERS),
        ready: VecDeque::new(),
        slow: Vec::new(),
        closing: None,
        encodes: Arc::new(AtomicU64::new(0)),
        locks: locks::Locks::default(),
        shared: shared.clone(),
        next_join: 0,
        undone: Bounded::default(),
        rebuilds: rebuild::Rebuilds::default(),
        undo_waiting: HashMap::new(),
        held_unchanged: Vec::new(),
        idle_since: Some(state.clock.now()),
        next_expiry: f64::INFINITY,
        snapshot_failing: false,
        live_news: Vec::new(),
        // The first presence is relayed at once.
        live_sent: Instant::now()
            .checked_sub(presence::LIVE_TICK)
            .unwrap_or_else(Instant::now),
        live_due: None,
        behind: std::collections::HashSet::new(),
        owing: std::collections::HashSet::new(),
        state,
    };
    crate::metrics::room_loaded();
    tokio::spawn(room.run(control_rx, frames_rx, internal_rx));
    RoomHandle {
        id,
        control: control_tx,
        frames: frames_tx,
        shared,
        org,
        project,
        hooks,
    }
}

impl Room {
    async fn run(
        mut self,
        mut control: mpsc::Receiver<RoomCmd>,
        mut frames: mpsc::Receiver<RoomCmd>,
        mut internal: mpsc::UnboundedReceiver<Internal>,
    ) {
        let mut control_open = true;
        let every = Duration::from_secs_f64(self.state.settings.sweep_interval.max(0.001));
        let mut sweep = tokio::time::interval_at(Instant::now() + every, every);
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let deadline = self.closing.as_ref().map(|c| c.deadline);
            let far = Instant::now() + Duration::from_secs(86400);
            tokio::select! {
                biased;
                Some(event) = internal.recv() => self.on_internal(event),
                _ = sweep.tick() => self.on_tick(),
                cmd = control.recv(), if control_open => match cmd {
                    Some(RoomCmd::Sweep) => {
                        let hooks = self.state.room_hooks.clone();
                        hooks.stall.hold(&self.doc).await;
                        self.on_command(RoomCmd::Sweep);
                    }
                    Some(cmd) => self.on_command(cmd),
                    // Nobody can reach the room any more (the hub is gone).
                    None => {
                        control_open = false;
                        self.begin_close(close::GOING_AWAY, "the server is shutting down", true, None);
                    }
                },
                Some(cmd) = frames.recv(), if self.takes_frames() => self.on_command(cmd),
                () = tokio::time::sleep_until(deadline.unwrap_or(far)), if deadline.is_some() => {
                    if let Some(c) = &mut self.closing {
                        // Stop waiting for the journal: what is still tentative goes unanswered.
                        c.deadline = Instant::now();
                    }
                }
                // (Relayed just below.)
                () = tokio::time::sleep_until(self.live_due.unwrap_or(far)),
                    if self.live_due.is_some() => {}
            }
            self.run_parked();
            self.relay_live_when_due();
            self.reap();
            if self
                .closing
                .as_ref()
                .is_some_and(|c| self.pending.is_empty() || c.deadline <= Instant::now())
            {
                self.finish().await;
                wind_up(&mut control, &mut frames);
                crate::metrics::room_unloaded();
                return;
            }
        }
    }

    /// The room's periodic work: locks expire, connections whose session expired close, and a
    /// snapshot that failed (with no change since to try it again) is tried again, so a room never
    /// sits on a snapshot it owes. Not when the room could unload (idle, nothing else keeping it):
    /// the sweeper's unload writes the final snapshot then, and a retry started here (which with
    /// storage hanging lasts until its timeout) would keep the room loaded.
    fn on_tick(&mut self) {
        self.sweep();
        self.close_expired_sessions();
        if !self.idle_enough() {
            self.maybe_snapshot();
        }
    }

    /// The hub's sweep: a tick, or an unload if the room may (whose final snapshot then stands
    /// for the snapshot a tick would try again).
    fn on_sweep(&mut self) {
        self.shared.sweep_queued.store(false, Ordering::SeqCst);
        self.sweep();
        self.close_expired_sessions();
        if self.may_unload() {
            self.begin_unload();
        } else {
            self.maybe_snapshot();
        }
    }

    /// Closes (4401) the connections whose session has expired. The boundary is REST's
    /// ([`crate::sessions::Sessions`]): a session is expired once `expires_at < now`; at its
    /// expiry second it still works.
    fn close_expired_sessions(&mut self) {
        let now = self.state.clock.now();
        if now <= self.next_expiry {
            return;
        }
        let expired: Vec<ConnId> = self
            .conns
            .iter()
            .filter(|(_, c)| c.expires < now)
            .map(|(id, _)| *id)
            .collect();
        for conn in &expired {
            if let Some(c) = self.conns.get(conn) {
                c.outbox.close_now(close::SIGN_IN, "sign in again");
            }
            self.remove_conn(*conn, None, true);
        }
        crate::metrics::dropped(crate::metrics::DropCause::Expired, expired.len());
        self.next_expiry = self
            .conns
            .values()
            .map(|c| c.expires)
            .fold(f64::INFINITY, f64::min);
    }

    /// Whether the room takes the next frame now.
    fn takes_frames(&self) -> bool {
        self.closing.is_none() && self.paused == 0 && self.pending.len() < MAX_TENTATIVE
    }

    fn encode(&self, msg: &ServerMsg) -> Bytes {
        self.encodes.fetch_add(1, Ordering::Relaxed);
        wire::encode_server(msg)
    }

    fn on_command(&mut self, cmd: RoomCmd) {
        match cmd {
            RoomCmd::Join(req, answer) => {
                self.join(*req);
                if let Some(answer) = answer {
                    let _ = answer.send(JoinAnswer::Taken);
                }
            }
            // (A socket no longer in the room, replaced say, finds nothing: its successor has a
            // connection id of its own.)
            RoomCmd::Leave(conn) => self.remove_conn(conn, None, true),
            RoomCmd::Frame { conn, msg, size } => {
                self.on_frame(conn, change::Incoming::Msg(*msg, size));
            }
            RoomCmd::Malformed {
                conn,
                code,
                message,
            } => self.on_frame(conn, change::Incoming::Malformed(code, message)),
            RoomCmd::ServerChange(sc) => {
                // A room nobody is connected to is idle from its last change by REST.
                if self.conns.is_empty() {
                    self.idle_since = Some(self.state.clock.now());
                }
                self.on_server_change(*sc);
            }
            RoomCmd::Notify(msg) => {
                let frame = self.encode(&msg);
                self.broadcast(&frame, &[]);
            }
            RoomCmd::Tick => self.on_tick(),
            RoomCmd::Sweep => self.on_sweep(),
            RoomCmd::Revoked {
                conn,
                user,
                code,
                reason,
                user_wide,
            } => self.revoke(conn, &user, code, &reason, user_wide),
            RoomCmd::Close {
                code,
                reason,
                snapshot,
                done,
            } => self.begin_close(code, &reason, snapshot, Some(done)),
            RoomCmd::Head(reply) => {
                let _ = reply.send(self.durable.clone());
            }
            RoomCmd::ForgetChangeIds => self.recent.forget_entries(),
            RoomCmd::Status(reply) => {
                let _ = reply.send(self.status());
            }
        }
    }

    fn on_internal(&mut self, event: Internal) {
        match event {
            Internal::Journal(outcome) => self.on_journal(outcome),
            Internal::Lookup(lookup) => self.on_lookup(lookup),
            Internal::Blobs(check) => self.on_blobs(*check),
            Internal::References(check) => self.on_references(*check),
            Internal::Decided(decided) => self.on_offloaded(*decided),
            Internal::Duplicate(check) => self.on_duplicate(*check),
            Internal::Undo(lookup) => self.on_undo_lookup(lookup),
            Internal::Revert(lookup) => self.on_revert_lookup(*lookup),
            Internal::Report(lookup) => self.on_report_lookup(lookup),
            Internal::Welcome { conn, result } => self.on_welcome(conn, result),
            Internal::LockNews(conn) => self.flush_lock_news(conn),
            Internal::Snapshot { seq, ok } => {
                self.snapshot_task = None;
                self.snapshot_failing = !ok;
                if ok {
                    self.snapshot_seq = self.snapshot_seq.max(seq);
                    self.recent.trim();
                }
                // Changes that became durable while it was written were not snapshotted: look
                // again now (a failed one is tried again only once something new is durable).
                if self.durable.seq > seq {
                    self.maybe_snapshot();
                }
            }
        }
    }

    fn status(&self) -> RoomStatus {
        let mut connections: Vec<ConnStatus> = self
            .conns
            .values()
            .map(|c| ConnStatus {
                client_id: c.client.to_string(),
                queued_frames: c.outbox.queued_frames(),
                queued_bytes: c.outbox.queued_bytes(),
                waiting_answers: c.replies.len(),
                behind: c.outbox.is_behind(),
            })
            .collect();
        connections.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        RoomStatus {
            seq: self.durable.seq,
            tentative: self.pending.len(),
            snapshot_seq: self.snapshot_seq,
            epoch: self.epoch,
            encodes: self.encodes.load(Ordering::Relaxed),
            remembered: self.recent.sizes(),
            complete_clients: self.recent.complete_clients(),
            locks: (self.locks.len(), self.locks.detached_count()),
            connections,
        }
    }

    // ------------------------------------------------------------------ frames

    /// The connection, if the room still works for it: present, and its outbox not closed. A
    /// closed outbox means its access ended (the hub closes it at once, before the room hears) or
    /// it broke a limit: nothing more of it is processed. Every path that resumes work for a
    /// connection (a judge, a lookup, a blob check coming back; a re-judge after a journal refusal;
    /// the hand-over to the journal itself) asks this first.
    ///
    /// The rule for a revoked connection's changes: those already handed to the journal before the
    /// drop were accepted while it had access, and complete (they are durable, or about to be,
    /// and broadcast to the others); nothing more is sent to the revoked connection, whose outbox
    /// is closed. Everything not yet handed to the journal is discarded.
    pub(super) fn live_conn(&self, conn: ConnId) -> Option<&Conn> {
        self.conns.get(&conn).filter(|c| !c.outbox.is_closed())
    }

    /// The hub revoked a connection's access (`code`: 4401, 4403 or 4404): it is removed at once,
    /// whatever the room is busy with (its parked messages and work not yet handed to the journal
    /// are discarded; see [`Room::live_conn`]), the others hear `peer.left` once, and its locks are
    /// released now rather than kept for the grace period: a revoked user must not go on blocking
    /// others. (An ordinary disconnect keeps its locks for the grace period.)
    ///
    /// The user's locks that already wait detached in this room (a client of theirs that went away
    /// a moment before) are released too. Whichever of the connection's `Leave` and this arrives
    /// first, the locks go at once: `remove_conn` releases them for a connection closed with an
    /// access code.
    fn revoke(
        &mut self,
        socket: Option<ConnId>,
        user: &Id,
        code: u16,
        reason: &str,
        user_wide: bool,
    ) {
        if let Some(socket) = socket {
            // (A connection no longer in the room, replaced or gone, is not touched.)
            let found = Some(socket).filter(|c| self.conns.contains_key(c));
            let order = &self.state.room_hooks.revoke_order;
            match found {
                Some(conn) => {
                    order.revoked_first.fetch_add(1, Ordering::Relaxed);
                    if let Some(c) = self.conns.get(&conn) {
                        c.outbox.close_now(code, reason);
                    }
                    self.remove_conn(conn, None, true);
                }
                None => {
                    order.gone_first.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        self.release_if(user_wide, code, user);
    }

    fn release_if(&mut self, user_wide: bool, code: u16, user: &Id) {
        if user_wide && close::ends_access(code) {
            self.release_detached_of(user);
        }
    }

    fn on_frame(&mut self, conn: ConnId, incoming: change::Incoming) {
        if self.live_conn(conn).is_none() {
            return; // (see `live_conn`)
        }
        // Presence and drafts are not answered and wait for nothing: relayed now, whatever the
        // connection's other messages wait for.
        let incoming = match incoming {
            change::Incoming::Msg(msg, _) if presence::is_live(&msg) => {
                return self.on_live(conn, msg);
            }
            other => other,
        };
        let max_bytes = 4 * self.state.settings.max_change_bytes;
        let Some(c) = self.conns.get_mut(&conn) else {
            return; // (a connection the room already dropped)
        };
        if c.busy || !c.parked.is_empty() {
            let size = incoming.size();
            if c.parked.len() >= MAX_PARKED || c.parked_bytes + size > max_bytes {
                c.outbox.close_now(close::TOO_SLOW, "too slow");
                self.slow.push(conn);
            } else {
                c.parked_bytes += size;
                c.parked.push_back(incoming);
            }
            return;
        }
        self.dispatch(conn, incoming);
    }

    /// Puts a message back first in its connection's queue, to go on when the room takes frames.
    fn park_first(&mut self, conn: ConnId, incoming: change::Incoming) {
        if let Some(c) = self.conns.get_mut(&conn) {
            c.parked_bytes += incoming.size();
            c.parked.push_front(incoming);
            if !c.busy {
                self.ready.push_front(conn);
            }
        }
    }

    /// Messages of connections whose earlier message has been settled, in order; and the server's
    /// own changes.
    fn run_parked(&mut self) {
        while self.takes_frames() {
            if let Some(item) = self.server_queue.pop_front() {
                self.judge_item(item);
                continue;
            }
            let Some(conn) = self.ready.pop_front() else {
                return;
            };
            loop {
                if !self.takes_frames() {
                    self.ready.push_front(conn);
                    return;
                }
                let Some(c) = self.conns.get_mut(&conn) else {
                    break;
                };
                if c.busy {
                    break;
                }
                let Some(incoming) = c.parked.pop_front() else {
                    break;
                };
                c.parked_bytes = c.parked_bytes.saturating_sub(incoming.size());
                self.dispatch(conn, incoming);
            }
        }
    }

    /// A connection's message wait is over: its parked messages may go on.
    fn unpark(&mut self, conn: ConnId) {
        if let Some(c) = self.conns.get_mut(&conn) {
            c.busy = false;
            if !c.parked.is_empty() {
                self.ready.push_back(conn);
            }
        }
    }

    /// What every message a client sends leads to. Later tasks add their messages here.
    fn dispatch(&mut self, conn: ConnId, incoming: change::Incoming) {
        if self.conns.contains_key(&conn) && self.live_conn(conn).is_none() {
            // Closed (its access ended, or it broke a limit): nothing more of it is processed.
            return;
        }
        let (msg, size) = match incoming {
            change::Incoming::Msg(msg, size) => (msg, size),
            change::Incoming::Judge(change, size, reverts) => {
                return self.judge_conn_change(conn, change, size, reverts);
            }
            change::Incoming::Malformed(code, message) => {
                let frame = self.encode(&ServerMsg::Error {
                    code: code.to_owned(),
                    message,
                    request_id: None,
                });
                return self.reply(conn, frame);
            }
        };
        match msg {
            ClientMsg::Change(change) => self.on_change(conn, change, size),
            ClientMsg::Ping => {
                let pong = self.encode(&ServerMsg::Pong {
                    seq: self.durable.seq,
                });
                self.reply(conn, pong);
            }
            ClientMsg::LockAcquire { request_id, paths } => {
                self.on_lock_acquire(conn, request_id, paths);
            }
            ClientMsg::LockRelease { paths } => self.on_lock_release(conn, &paths),
            msg @ (ClientMsg::Presence { .. } | ClientMsg::Draft { .. }) => self.on_live(conn, msg),
            ClientMsg::Revert { request_id, seq } => self.on_revert(conn, request_id, seq),
            ClientMsg::RebuildReport { seq, errors } => self.on_report(conn, seq, errors),
            ClientMsg::Hello { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::close;

    /// Every open document has these channels, whose memory comes in blocks of 32 messages sized by
    /// the largest message: rare big ones are boxed.
    #[test]
    fn the_messages_of_a_rooms_channels_stay_small() {
        assert!(std::mem::size_of::<super::Internal>() <= 128);
        assert!(std::mem::size_of::<super::change::Submit>() <= 32);
        assert!(std::mem::size_of::<super::RoomCmd>() <= 128);
    }

    #[test]
    fn only_codes_that_end_access_release_locks_at_once() {
        for code in [close::SIGN_IN, close::ACCESS_CHANGED, close::GONE] {
            assert!(close::ends_access(code), "{code}");
        }
        for code in [
            1000,
            close::GOING_AWAY,
            close::TOO_SLOW,
            close::REPLACED,
            close::MOVED,
            close::POLICY,
            close::SERVER_ERROR,
        ] {
            assert!(!close::ends_access(code), "{code}");
        }
    }
}
