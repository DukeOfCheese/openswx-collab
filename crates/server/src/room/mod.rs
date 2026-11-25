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
//! `n + 1` while `n` is still being written (pipelining); if the journal refuses `n`, every change
//! decided after it is refused with `server_error` and the tentative head returns to the durable one.
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

mod change;
pub mod conn;
mod welcome;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use oswx_core::wire::{self, ClientMsg, Peer, ServerMsg};
use oswx_core::{ClientId, Head, Seq};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::security::{ProjectRole, colour_for};
use crate::snapshot::write_snapshot;
use crate::state::AppState;
use crate::store::Id;
use welcome::Welcome;

pub use conn::{ConnId, Outbox};

/// WebSocket close codes the server uses, in one place.
pub mod close {
    /// The server is shutting down.
    pub const GOING_AWAY: u16 = 1001;
    /// A frame was over the size limit.
    pub const TOO_BIG: u16 = 1009;
    /// Something went wrong on the server; try again.
    pub const SERVER_ERROR: u16 = 1011;
    /// The first frame was not a valid hello (or did not come in time).
    pub const BAD_HELLO: u16 = 4400;
    /// The session is not valid: sign in again.
    pub const SIGN_IN: u16 = 4401;
    /// The account's access to the document changed (Task 12).
    pub const ACCESS_CHANGED: u16 = 4403;
    /// The document does not exist, or no longer does, or is not yours to open.
    pub const GONE: u16 = 4404;
    /// The connection did not read fast enough (or the server could not take it right now).
    pub const TOO_SLOW: u16 = 4408;
    /// The same client connected again; this older connection is replaced.
    pub const REPLACED: u16 = 4409;
    /// The document moved or was reloaded: reconnect (and resend what was not acknowledged).
    pub const MOVED: u16 = 4410;
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
/// How long closing waits for the journal's answers to changes in flight.
const CLOSE_WAIT: Duration = Duration::from_secs(5);
/// How long a final snapshot may take when a room closes.
const FINAL_SNAPSHOT_WAIT: Duration = Duration::from_secs(30);
/// A snapshot's index keeps this many changes of history to judge late changes against; older
/// marks are pruned just before the snapshot is written.
const INDEX_KEEP: Seq = 100_000;

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
    pub connections: Vec<ConnStatus>,
}

/// One connection of a room.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnStatus {
    pub client_id: String,
    pub queued_frames: usize,
    pub queued_bytes: usize,
}

/// A connection asking to join, made by the WebSocket handshake once the hello was checked.
pub struct JoinRequest {
    pub conn: ConnId,
    pub outbox: Arc<Outbox>,
    pub user: Id,
    pub name: String,
    pub role: ProjectRole,
    /// `<user id>.<client id from the hello>`.
    pub client: ClientId,
    pub format_version: Option<i64>,
    pub last_seq: Option<Seq>,
}

/// A command for a room.
pub enum RoomCmd {
    Join(Box<JoinRequest>),
    /// The connection's reader stopped: the client went away.
    Leave(ConnId),
    /// A decoded message from a connection; `size` is the frame's length in bytes.
    Frame {
        conn: ConnId,
        msg: Box<ClientMsg>,
        size: usize,
    },
    /// A periodic nudge (TASK 10/12: lock expiry and idle checks hang off it); nothing yet.
    Tick,
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
    Status(oneshot::Sender<RoomStatus>),
}

/// The room is gone (unloaded or stopping); open it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the room is gone")]
pub struct RoomGone;

/// How the rest of the server reaches a room. Cloning is cheap.
#[derive(Clone, Debug)]
pub struct RoomHandle {
    id: u64,
    control: mpsc::Sender<RoomCmd>,
    frames: mpsc::Sender<RoomCmd>,
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
        let channel = match cmd {
            RoomCmd::Frame { .. } => &self.frames,
            _ => &self.control,
        };
        channel.send(cmd).await.map_err(|e| Box::new(e.0))
    }

    /// Asks the room to take a connection. The request comes back if the room is gone.
    pub async fn join(&self, req: JoinRequest) -> Result<(), Box<JoinRequest>> {
        match self.send(RoomCmd::Join(Box::new(req))).await {
            Ok(()) => Ok(()),
            Err(cmd) => match *cmd {
                RoomCmd::Join(req) => Err(req),
                _ => unreachable!("send hands back what it was given"),
            },
        }
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

    pub(crate) async fn leave(&self, conn: ConnId) {
        let _ = self.send(RoomCmd::Leave(conn)).await;
    }

    /// The durable head, or `None` if the room is gone.
    pub async fn durable_head(&self) -> Option<Head> {
        let (tx, rx) = oneshot::channel();
        self.send(RoomCmd::Head(tx)).await.ok()?;
        rx.await.ok()
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
    pub head: Head,
    /// The epoch [`crate::lease::acquire`] gave for this load.
    pub epoch: u64,
    /// The seq of the snapshot the head was loaded from.
    pub snapshot_seq: Seq,
}

/// An answer queued for a connection: ready, or waiting for the journal's outcome of a change.
enum Reply {
    Ready(Bytes),
    Waiting(Seq),
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
    parked: VecDeque<(ClientMsg, usize)>,
    /// A message of this connection waits for a storage answer (a blob check, a journal lookup).
    busy: bool,
}

impl Conn {
    fn peer(&self) -> Peer {
        Peer {
            client_id: self.client.clone(),
            user_id: self.user.clone(),
            name: self.name.clone(),
            colour: self.colour.clone(),
            // TASK 10: presence and drafts.
            presence: None,
            draft: None,
        }
    }
}

/// The work a room started, coming back.
enum Internal {
    Journal(change::Outcome),
    Lookup(change::Lookup),
    Blobs(change::BlobCheck),
    Decided(change::Offloaded),
    Welcome {
        conn: ConnId,
        result: anyhow::Result<Bytes>,
    },
    Snapshot {
        seq: Seq,
        ok: bool,
    },
}

struct Closing {
    code: u16,
    reason: String,
    snapshot: bool,
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
    /// A big change is being judged off the task: no frame is taken until it is back.
    paused: bool,
    /// Connections with parked messages that may go on.
    ready: VecDeque<ConnId>,
    /// Connections that broke their queue's limits, to be removed.
    slow: Vec<ConnId>,
    closing: Option<Closing>,
    encodes: Arc<AtomicU64>,
}

fn next_room_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Starts a room's task and answers its handle.
pub(crate) fn spawn(state: AppState, init: RoomInit) -> RoomHandle {
    let (control_tx, control_rx) = mpsc::channel(CONTROL_QUEUE);
    let (frames_tx, frames_rx) = mpsc::channel(FRAMES_QUEUE);
    let (internal_tx, internal_rx) = mpsc::unbounded_channel();
    let journal = change::pump(state.journal.clone(), internal_tx.clone());
    let id = next_room_id();
    let room = Room {
        id,
        doc: init.doc,
        project: init.project,
        org: init.org,
        format_version: init.format_version,
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
        recent: change::Recent::new(init.head.seq),
        refused: HashMap::new(),
        known_blobs: change::KnownBlobs::default(),
        snapshot_seq: init.snapshot_seq,
        snapshot_task: None,
        paused: false,
        ready: VecDeque::new(),
        slow: Vec::new(),
        closing: None,
        encodes: Arc::new(AtomicU64::new(0)),
        state,
    };
    tokio::spawn(room.run(control_rx, frames_rx, internal_rx));
    RoomHandle {
        id,
        control: control_tx,
        frames: frames_tx,
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
        loop {
            let deadline = self.closing.as_ref().map(|c| c.deadline);
            let far = Instant::now() + Duration::from_secs(86400);
            tokio::select! {
                biased;
                Some(event) = internal.recv() => self.on_internal(event),
                cmd = control.recv(), if control_open => match cmd {
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
            }
            self.run_parked();
            self.reap();
            if self
                .closing
                .as_ref()
                .is_some_and(|c| self.pending.is_empty() || c.deadline <= Instant::now())
            {
                self.finish().await;
                return;
            }
        }
    }

    /// Whether the room takes the next frame now.
    fn takes_frames(&self) -> bool {
        self.closing.is_none() && !self.paused && self.pending.len() < MAX_TENTATIVE
    }

    fn encode(&self, msg: &ServerMsg) -> Bytes {
        self.encodes.fetch_add(1, Ordering::Relaxed);
        wire::encode_server(msg)
    }

    fn on_command(&mut self, cmd: RoomCmd) {
        match cmd {
            RoomCmd::Join(req) => self.join(*req),
            RoomCmd::Leave(conn) => self.remove_conn(conn, None, true),
            RoomCmd::Frame { conn, msg, size } => self.on_frame(conn, *msg, size),
            RoomCmd::Tick => {}
            RoomCmd::Close {
                code,
                reason,
                snapshot,
                done,
            } => self.begin_close(code, &reason, snapshot, Some(done)),
            RoomCmd::Head(reply) => {
                let _ = reply.send(self.durable.clone());
            }
            RoomCmd::Status(reply) => {
                let _ = reply.send(self.status());
            }
        }
    }

    fn on_internal(&mut self, event: Internal) {
        match event {
            Internal::Journal(outcome) => self.on_journal(outcome),
            Internal::Lookup(lookup) => self.on_lookup(lookup),
            Internal::Blobs(check) => self.on_blobs(check),
            Internal::Decided(decided) => self.on_offloaded(decided),
            Internal::Welcome { conn, result } => self.on_welcome(conn, result),
            Internal::Snapshot { seq, ok } => {
                self.snapshot_task = None;
                if ok {
                    self.snapshot_seq = self.snapshot_seq.max(seq);
                    self.recent.trim(self.snapshot_seq);
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
            })
            .collect();
        connections.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        RoomStatus {
            seq: self.durable.seq,
            tentative: self.pending.len(),
            snapshot_seq: self.snapshot_seq,
            epoch: self.epoch,
            encodes: self.encodes.load(Ordering::Relaxed),
            connections,
        }
    }

    // ------------------------------------------------------------------ frames

    fn on_frame(&mut self, conn: ConnId, msg: ClientMsg, size: usize) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return; // (a connection the room already dropped)
        };
        if c.busy || !c.parked.is_empty() {
            if c.parked.len() >= MAX_PARKED {
                c.outbox.close_now(close::TOO_SLOW, "too slow");
                self.slow.push(conn);
            } else {
                c.parked.push_back((msg, size));
            }
            return;
        }
        self.dispatch(conn, msg, size);
    }

    /// Messages of connections whose earlier message has been settled, in order.
    fn run_parked(&mut self) {
        while self.takes_frames() {
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
                let Some((msg, size)) = c.parked.pop_front() else {
                    break;
                };
                self.dispatch(conn, msg, size);
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
    fn dispatch(&mut self, conn: ConnId, msg: ClientMsg, size: usize) {
        match msg {
            ClientMsg::Change(change) => self.on_change(conn, change, size),
            ClientMsg::Ping => {
                let pong = self.encode(&ServerMsg::Pong {
                    seq: self.durable.seq,
                });
                self.reply(conn, pong);
            }
            // TASK 10: locks, presence and drafts.
            ClientMsg::LockAcquire { .. }
            | ClientMsg::LockRelease { .. }
            | ClientMsg::Presence { .. }
            | ClientMsg::Draft { .. } => {
                tracing::debug!("a lock, presence or draft message is ignored until Task 10");
            }
            // TASK 11: reverting from history and rebuild reports.
            ClientMsg::Revert { .. } | ClientMsg::RebuildReport { .. } => {
                tracing::debug!("a revert or rebuild report is ignored until Task 11");
            }
            ClientMsg::Hello { .. } => {}
        }
    }

    // ------------------------------------------------------------------ sending

    /// Queues a frame for one connection; one that breaks its limits is dropped afterwards.
    fn send_to(&mut self, conn: ConnId, frame: Bytes) {
        if let Some(c) = self.conns.get(&conn)
            && c.outbox.push(frame) == conn::Pushed::TooSlow
        {
            self.slow.push(conn);
        }
    }

    /// One frame to every connection but `except`.
    fn broadcast(&mut self, frame: &Bytes, except: &[ConnId]) {
        let mut recipients = 0;
        for (id, c) in &self.conns {
            if except.contains(id) {
                continue;
            }
            recipients += 1;
            if c.outbox.push(frame.clone()) == conn::Pushed::TooSlow {
                self.slow.push(*id);
            }
        }
        crate::metrics::broadcast(recipients);
    }

    /// An answer to one of the connection's messages: sent at once unless an earlier answer is
    /// still waiting.
    fn reply(&mut self, conn: ConnId, frame: Bytes) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        if c.replies.is_empty() {
            self.send_to(conn, frame);
        } else {
            c.replies.push_back(Reply::Ready(frame));
        }
    }

    /// Settles the connection's answer that waits for `seq`, and sends what is now first in line.
    fn resolve(&mut self, conn: ConnId, seq: Seq, frame: Bytes) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        if let Some(slot) = c
            .replies
            .iter_mut()
            .find(|r| matches!(r, Reply::Waiting(s) if *s == seq))
        {
            *slot = Reply::Ready(frame);
        }
        let mut out = Vec::new();
        while let Some(Reply::Ready(_)) = c.replies.front() {
            if let Some(Reply::Ready(f)) = c.replies.pop_front() {
                out.push(f);
            }
        }
        for f in out {
            self.send_to(conn, f);
        }
    }

    /// Removes the connections that broke their limits (each removal is announced, which may break
    /// another's).
    fn reap(&mut self) {
        while let Some(conn) = self.slow.pop() {
            if self.conns.contains_key(&conn) {
                crate::metrics::slow_consumer();
                self.remove_conn(conn, None, true);
            }
        }
    }

    // ------------------------------------------------------------------ people

    fn join(&mut self, req: JoinRequest) {
        if let Some(c) = &self.closing {
            req.outbox.close(c.code, &c.reason);
            return;
        }
        if let Some(old) = self.by_client.get(&req.client).copied() {
            // Popped first: its leaving is not announced.
            self.remove_conn(
                old,
                Some((close::REPLACED, "connected again from the same client")),
                false,
            );
        }
        let read_only = if req.role == ProjectRole::Viewer {
            Some("you have view access to this project".to_owned())
        } else if let Some(theirs) = self
            .format_version
            .filter(|fv| req.format_version != Some(*fv))
        {
            let mine = req
                .format_version
                .map_or_else(|| "none".to_owned(), |v| v.to_string());
            Some(format!(
                "this file uses openswx file format {theirs} and this app uses {mine}; update \
                 openswx to edit it"
            ))
        } else {
            None
        };
        let same_user = self.conns.values().find(|c| c.user == req.user);
        let colour = match same_user {
            Some(c) => c.colour.clone(),
            None => {
                let taken: Vec<String> = self.conns.values().map(|c| c.colour.clone()).collect();
                colour_for(&req.user, &taken)
            }
        };
        let conn = Conn {
            client: req.client.clone(),
            user: req.user,
            name: req.name,
            colour,
            read_only,
            outbox: req.outbox,
            replies: VecDeque::new(),
            parked: VecDeque::new(),
            busy: false,
        };
        let you = conn.peer();
        let peers: Vec<Peer> = self.conns.values().map(Conn::peer).collect();
        let joined = self.encode(&ServerMsg::PeerJoined(you.clone()));
        self.broadcast(&joined, &[]);
        let welcome = Welcome {
            store: self.state.store.clone(),
            doc: self.doc.clone(),
            head: self.durable.clone(),
            last_seq: req.last_seq,
            catchup_limit: self.state.settings.catchup_limit as Seq,
            project: self.project.clone(),
            role: req.role,
            read_only: conn.read_only.clone(),
            you,
            peers,
            encodes: self.encodes.clone(),
        };
        self.by_client.insert(req.client, req.conn);
        self.conns.insert(req.conn, conn);
        let internal = self.internal.clone();
        let conn_id = req.conn;
        tokio::spawn(async move {
            let result = welcome.build().await;
            let _ = internal.send(Internal::Welcome {
                conn: conn_id,
                result,
            });
        });
    }

    fn on_welcome(&mut self, conn: ConnId, result: anyhow::Result<Bytes>) {
        let Some(c) = self.conns.get(&conn) else {
            return;
        };
        match result {
            Ok(frame) => c.outbox.release(frame),
            Err(e) => {
                tracing::error!("could not make the welcome of document {}: {e:#}", self.doc);
                self.remove_conn(conn, Some((close::SERVER_ERROR, "try again")), true);
            }
        }
    }

    /// Takes a connection out of the room, closing it (with `close`, or a normal close), and tells
    /// the others it left when `announce`.
    fn remove_conn(&mut self, conn: ConnId, close: Option<(u16, &str)>, announce: bool) {
        let Some(c) = self.conns.remove(&conn) else {
            return;
        };
        if self.by_client.get(&c.client) == Some(&conn) {
            self.by_client.remove(&c.client);
        }
        // For this task the refused ids are forgotten when the connection goes (TASK 12: after a
        // grace period instead).
        self.refused.remove(&c.client);
        let (code, reason) = close.unwrap_or((1000, ""));
        c.outbox.close(code, reason);
        if announce {
            let left = self.encode(&ServerMsg::PeerLeft {
                client_id: c.client.clone(),
            });
            self.broadcast(&left, &[]);
        }
    }

    // ------------------------------------------------------------------ closing

    fn begin_close(
        &mut self,
        code: u16,
        reason: &str,
        snapshot: bool,
        done: Option<oneshot::Sender<()>>,
    ) {
        match &mut self.closing {
            Some(c) => c.done.extend(done),
            None => {
                self.closing = Some(Closing {
                    code,
                    reason: reason.to_owned(),
                    snapshot,
                    done: done.into_iter().collect(),
                    deadline: Instant::now() + CLOSE_WAIT,
                });
            }
        }
    }

    /// The document is no longer this room's to write (the lease moved, or the journal could not
    /// say whether a change was written): forget everything tentative and unload at once, telling
    /// every client to reconnect.
    fn lose(&mut self, why: &str) {
        tracing::warn!(
            "document {} is unloaded and its clients told to reconnect: {why}",
            self.doc
        );
        self.pending.clear();
        self.generation += 1;
        self.tentative = self.durable.clone();
        self.state.hub.forget(&self.doc, self.id);
        match &mut self.closing {
            Some(c) => {
                c.deadline = Instant::now();
                c.snapshot = false;
                c.code = close::MOVED;
                c.reason = "reconnect".into();
            }
            None => {
                self.closing = Some(Closing {
                    code: close::MOVED,
                    reason: "reconnect".into(),
                    snapshot: false,
                    done: Vec::new(),
                    deadline: Instant::now(),
                });
            }
        }
    }

    async fn finish(&mut self) {
        let Some(c) = self.closing.take() else {
            return;
        };
        // Out of the hub first, so that a client reconnecting at once gets a fresh room.
        self.state.hub.forget(&self.doc, self.id);
        for (_, conn) in self.conns.drain() {
            conn.outbox.close(c.code, &c.reason);
        }
        if c.snapshot {
            if let Some(task) = self.snapshot_task.take() {
                let _ = tokio::time::timeout(FINAL_SNAPSHOT_WAIT, task).await;
            }
            if self.durable.seq > self.snapshot_seq {
                self.prune_index();
                let head = self.durable.clone();
                let written = tokio::time::timeout(
                    FINAL_SNAPSHOT_WAIT,
                    write_snapshot(
                        self.state.store.as_ref(),
                        self.state.blobs.as_ref(),
                        &self.doc,
                        self.epoch,
                        &head,
                        self.state.clock.now(),
                    ),
                )
                .await;
                match written {
                    Ok(Ok(())) => self.snapshot_seq = head.seq,
                    Ok(Err(e)) => {
                        tracing::warn!("the final snapshot of document {} failed: {e:#}", self.doc)
                    }
                    Err(_) => {
                        tracing::warn!("the final snapshot of document {} timed out", self.doc)
                    }
                }
            }
        }
        for done in c.done {
            let _ = done.send(());
        }
    }

    // ------------------------------------------------------------------ snapshots

    /// Drops index marks older than [`INDEX_KEEP`] changes from both heads. Only ever called right
    /// before a snapshot of the durable head is written, so that a reload judges as memory does.
    fn prune_index(&mut self) {
        let floor = self.durable.seq.saturating_sub(INDEX_KEEP);
        if floor > self.durable.index.floor() {
            self.durable.index = self.durable.index.prune(floor);
            self.tentative.index = self.tentative.index.prune(floor);
        }
    }

    /// Writes a snapshot in the background once enough durable changes have built up since the last
    /// one (a failed one is tried again after the next change).
    fn maybe_snapshot(&mut self) {
        let every = Seq::from(self.state.settings.snapshot_every.max(1));
        if self.snapshot_task.is_some()
            || self.closing.is_some()
            || self.durable.seq - self.snapshot_seq < every
        {
            return;
        }
        self.prune_index();
        let head = self.durable.clone();
        let (store, blobs, doc, epoch) = (
            self.state.store.clone(),
            self.state.blobs.clone(),
            self.doc.clone(),
            self.epoch,
        );
        let now = self.state.clock.now();
        let internal = self.internal.clone();
        self.snapshot_task = Some(tokio::spawn(async move {
            let result =
                write_snapshot(store.as_ref(), blobs.as_ref(), &doc, epoch, &head, now).await;
            if let Err(e) = &result {
                tracing::warn!("a snapshot of document {doc} failed (tried again later): {e:#}");
            }
            let _ = internal.send(Internal::Snapshot {
                seq: head.seq,
                ok: result.is_ok(),
            });
        }));
    }
}

/// A bounded set of hashes, for [`change::KnownBlobs`] and [`change::RefusedIds`].
#[derive(Debug, Default)]
struct Bounded<T: std::hash::Hash + Eq + Clone> {
    set: HashSet<T>,
    order: VecDeque<T>,
}

impl<T: std::hash::Hash + Eq + Clone> Bounded<T> {
    fn insert(&mut self, item: T, cap: usize) {
        if self.set.insert(item.clone()) {
            self.order.push_back(item);
            while self.order.len() > cap {
                if let Some(old) = self.order.pop_front() {
                    self.set.remove(&old);
                }
            }
        }
    }

    fn contains(&self, item: &T) -> bool {
        self.set.contains(item)
    }
}
