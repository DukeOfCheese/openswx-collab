//! The change pipeline of a room.
//!
//! For each `change` a connection sends, in this order:
//!
//! 1. **Resend?** A change id this client already used is answered with the seq it got
//!    (`change.accepted`), or, while that change is still being written, answered when it is. The
//!    room remembers, in memory, every change decided but not yet durable, every change made
//!    durable since it loaded, and, when a connection joins, that client's most recent change ids
//!    in the document (read from the journal by the handshake). A client whose join found fewer ids
//!    than a join loads, and none of whose entries has been forgotten since, is **complete**: an id
//!    memory does not hold is new, and is judged at once (the hot path asks the store nothing). For
//!    any other client a miss is looked up by `(document, client, change id)` first, off the task.
//!    So a duplicate reaches the journal (whose unique key is the backstop) only through a race.
//!    Entries are forgotten, oldest first, only past the room's maximum
//!    ([`super::MAX_REMEMBERED`]), and their clients stop being complete; a client also stops being
//!    complete when it leaves (worked out again at its next join) and whenever the journal refuses
//!    one of its changes as a duplicate.
//! 2. **Read-only** connections are refused (`read_only`), after the memory check, and are never
//!    looked up in the journal. Then **locks** (the `locks` module): a change touching what another
//!    client holds a lock on is refused (`locked`), against the lock table of the moment it is
//!    judged (and again whenever it is judged again).
//! 3. **Judged** by [`oswx_core::decide`] against the tentative head, under the sender's client id,
//!    with `Rules::openswx()`. `depends` are answered from what the room knows of the client's own
//!    changes: accepted (tentatively or durably) if memory holds it, refused if the client's
//!    refused ids hold it, unknown otherwise. A big change (more than [`BIG_OPS`] operations or a
//!    frame over [`BIG_FRAME`]) is judged on the blocking pool against a copy of the head (a
//!    persistent value, so the copy is cheap) while the room takes no other frame but goes on with
//!    everything else; the sender's later messages wait for it.
//! 4. **Blobs**: at most [`MAX_BLOB_REFS`] distinct `{"$blob": sha}` references (more is
//!    `invalid`); those the room has not seen are looked up in the organisation's storage (16 at a
//!    time) off the task, the sender's later messages waiting. A missing one is `missing_blob` (and
//!    remembered as missing for [`MISSING_BLOB_SECS`]); a storage error is `server_error`.
//! 5. **Written**: the change becomes tentative and its record goes to the journal. When the journal
//!    reports it durable the durable head moves, the change is broadcast to every other connection
//!    and the sender is told `change.accepted`.
//!
//! If the journal refuses a record for a cause of its own (a duplicate change id, a failed extra
//! write, a broken limit, or being out of order), only that record is answered: a duplicate with
//! the seq the journal holds for it (counting as accepted for the changes that build on it until
//! then), anything else `server_error`. The changes decided after it are kept and judged again, in
//! their order, against the durable head; each is then written or refused on its merits, and its
//! sender sees only the final answer. A change judged again [`MAX_REJUDGE`] times for a systematic
//! cause is answered `server_error` (a duplicate refusal does not count). A user whose changes the
//! journal refuses as duplicates more than [`MAX_DUPLICATES`] times in a room is closed there
//! (1008, misbehaving client). A lost
//! lease or an unknown outcome unloads the room (see the room module).
//!
//! The server's own changes ([`super::RoomHandle::server_change`]) go through the same pipeline
//! under [`SERVER_CLIENT`], with no change id, and are broadcast to every connection.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use bytes::Bytes;
use futures_util::FutureExt;
use oswx_core::wire::{self, ChangeEvent, ClientMsg, ServerMsg};
use oswx_core::{
    Accepted, Change, ClientId, Conflict, Head, Known, Op, Refusal, RejectCode, Rules, Seq, decide,
    decide_server,
};
use tokio::sync::{mpsc, oneshot};

use super::{ConnId, Internal, Reply, Room, ServerChange, ServerChangeError, close};
use crate::blobs::missing_blobs;
use crate::content::blob_refs;
use crate::journal::{JournalError, JournalWriter, Receipt, Record, RefusedWhy};
use crate::metrics;
use crate::record::{encode_ops, encode_paths, encode_reads};
use crate::snapshot::SERVER_CLIENT;

/// A change with more operations than this is judged off the room's task.
pub const BIG_OPS: usize = 256;
/// A change in a frame longer than this is judged off the room's task.
pub const BIG_FRAME: usize = 256 * 1024;

/// The most distinct blob references one change may hold.
pub const MAX_BLOB_REFS: usize = 4096;
/// How long a blob found missing is answered missing without asking storage again, in seconds.
pub const MISSING_BLOB_SECS: f64 = 30.0;
/// How many times a change may be judged again after a record before it was refused.
pub const MAX_REJUDGE: u8 = 3;
/// How many duplicate refusals from the journal one user may cause in a room before their
/// connections to it are closed.
pub const MAX_DUPLICATES: u32 = 3;
/// Refused change ids remembered per client.
const REFUSED_KEEP: usize = 1024;
/// Blob hashes remembered per room, found or missing.
const BLOBS_KEEP: usize = 10_000;
/// The longest `command` the journal keeps.
const MAX_COMMAND: usize = 100;
/// The longest `message` of a rejection.
const MAX_MESSAGE: usize = 300;
/// What the sender of a change that could not be written is told.
const NOT_STORED: &str = "the server could not store the change; try again";

/// Names a change in the pipeline from its first adoption on; answers wait on it.
pub(super) type Ticket = u64;

// ------------------------------------------------------------------ memory

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    /// Decided, not yet durable.
    Tentative(Ticket),
    /// The journal refused it as a duplicate; the seq it holds is being looked up. Counts as
    /// accepted for the changes that build on it.
    Checking(Ticket),
    Durable(Seq),
}

type Key = (ClientId, Arc<str>);

/// The change ids the room holds in memory; see the module documentation.
pub(super) struct Recent {
    map: HashMap<Key, Seen>,
    /// Durable entries, oldest first, each remembered key once (a key forgotten and remembered
    /// again may leave an older entry behind, which only forgets early).
    order: VecDeque<(Seq, Key)>,
    /// Clients for which memory holds every id they ever journaled in this document: a miss is a
    /// new id. Set at join when the journal had fewer ids than a join loads; cleared whenever one of
    /// the client's entries is forgotten.
    complete: HashSet<ClientId>,
    /// The most durable entries kept, whatever the snapshots.
    max: usize,
}

impl Recent {
    /// Empty memory keeping at most `max` durable entries.
    pub(super) fn new(max: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            complete: HashSet::new(),
            max: max.max(1),
        }
    }

    fn get(&self, client: &ClientId, change_id: &Arc<str>) -> Option<Seen> {
        self.map.get(&(client.clone(), change_id.clone())).copied()
    }

    /// Whether a change id of `client` that memory does not hold is new.
    fn is_complete(&self, client: &ClientId) -> bool {
        self.complete.contains(client)
    }

    /// Says whether memory now holds every id `client` journaled here.
    pub(super) fn set_complete(&mut self, client: &ClientId, complete: bool) {
        if complete {
            self.complete.insert(client.clone());
        } else {
            self.complete.remove(client);
        }
    }

    /// Remembers that the client's change `change_id` is durable as `seq`.
    pub(super) fn remember(&mut self, client: &ClientId, change_id: &str, seq: Seq) {
        let key = (client.clone(), Arc::from(change_id));
        // A change still being written (or whose seq is being looked up) is settled by its own
        // outcome, never by a row read meanwhile.
        if matches!(
            self.map.get(&key),
            Some(Seen::Tentative(_) | Seen::Checking(_))
        ) {
            return;
        }
        self.durable(key, seq);
    }

    fn durable(&mut self, key: Key, seq: Seq) {
        let before = self.map.insert(key.clone(), Seen::Durable(seq));
        if !matches!(before, Some(Seen::Durable(_))) {
            self.order.push_back((seq, key));
        }
    }

    /// For tests: forgets every entry, but not which clients are complete.
    pub(super) fn forget_entries(&mut self) {
        self.map.clear();
        self.order.clear();
    }

    /// How many clients are complete.
    pub(super) fn complete_clients(&self) -> usize {
        self.complete.len()
    }

    /// How many ids are remembered, and how long the order of forgetting is.
    pub(super) fn sizes(&self) -> (usize, usize) {
        (self.map.len(), self.order.len())
    }

    /// Forgets the oldest durable entries past the room's maximum (and only then: forgetting makes
    /// the entry's client incomplete, and its later unknown ids cost a journal lookup).
    pub(super) fn trim(&mut self) {
        while self.order.len() > self.max {
            let Some((seq, key)) = self.order.pop_front() else {
                break;
            };
            if self.map.get(&key) == Some(&Seen::Durable(seq)) {
                self.map.remove(&key);
            }
            self.complete.remove(&key.0);
        }
    }
}

/// The change ids of one client that were refused (bounded).
pub(super) type RefusedIds = super::Bounded<Arc<str>>;

/// Blob hashes the room has asked storage about: found ones (bounded), and missing ones with when
/// that answer stops counting.
#[derive(Debug, Default)]
pub(super) struct KnownBlobs {
    found: super::Bounded<String>,
    missing: HashMap<String, f64>,
}

impl KnownBlobs {
    fn note_missing(&mut self, sha: String, now: f64) {
        if self.missing.len() >= BLOBS_KEEP {
            self.missing.retain(|_, until| *until > now);
            if self.missing.len() >= BLOBS_KEEP {
                self.missing.clear();
            }
        }
        self.missing.insert(sha, now + MISSING_BLOB_SECS);
    }

    fn is_missing(&self, sha: &str, now: f64) -> bool {
        self.missing.get(sha).is_some_and(|until| *until > now)
    }
}

// ------------------------------------------------------------------ in flight

/// A message of a connection, as the room queues it behind an earlier one.
pub(super) enum Incoming {
    /// A decoded message and the size of its frame.
    Msg(ClientMsg, usize),
    /// A change whose memory and journal checks are done: judge it.
    Judge(Arc<Change>, usize),
    /// A frame that could not be read: answer `error` with this code and message.
    Malformed(&'static str, String),
}

impl Incoming {
    pub(super) fn size(&self) -> usize {
        match self {
            Incoming::Msg(_, size) | Incoming::Judge(_, size) => *size,
            Incoming::Malformed(..) => 0,
        }
    }
}

/// Where a change came from, and so who is answered.
pub(super) enum Origin {
    Conn(ConnId),
    Server {
        reply: oneshot::Sender<Result<Seq, ServerChangeError>>,
        /// Refuse it as stale if someone changed what it writes since its base.
        check_stale: bool,
        /// Which locks refuse it.
        locks: super::LockRule,
    },
}

impl Origin {
    /// `Some(check_stale)` for the server's own change.
    fn server_stale(&self) -> Option<bool> {
        match self {
            Origin::Server { check_stale, .. } => Some(*check_stale),
            Origin::Conn(_) => None,
        }
    }
}

/// Who a change is from, for its index marks, record and broadcast.
#[derive(Debug, Clone)]
pub(super) struct Author {
    /// `<user>.<client>`, or [`SERVER_CLIENT`].
    client: ClientId,
    user: String,
    name: String,
}

/// A change going through the pipeline.
pub(super) struct Item {
    origin: Origin,
    author: Author,
    change: Arc<Change>,
    size: usize,
    /// Set when it is first made tentative; answers wait on it, and keep waiting on it if the
    /// change is judged again.
    ticket: Option<Ticket>,
    /// Connections waiting for its answer (the sender, and any that resent it meanwhile).
    waiters: Vec<ConnId>,
    rejudged: u8,
    /// Its blob references were checked.
    blobs_checked: bool,
}

impl Item {
    fn conn(&self) -> Option<ConnId> {
        match self.origin {
            Origin::Conn(conn) => Some(conn),
            Origin::Server { .. } => None,
        }
    }

    /// The key of its id in [`Recent`], for a client's change.
    fn key(&self) -> Option<Key> {
        self.conn()
            .map(|_| (self.author.client.clone(), self.change.change_id.clone()))
    }

    /// What the lock check needs to know of it.
    pub(super) fn lock_view(&self) -> LockView<'_> {
        LockView {
            ops: &self.change.ops,
            client: &self.author.client,
            user: &self.author.user,
            rule: match &self.origin {
                Origin::Conn(_) => None,
                Origin::Server { locks, .. } => Some(locks),
            },
        }
    }
}

/// A change as the lock check sees it.
pub(super) struct LockView<'a> {
    pub(super) ops: &'a [Op],
    /// Its author's client ([`SERVER_CLIENT`] for the server's own change).
    pub(super) client: &'a ClientId,
    pub(super) user: &'a str,
    /// `None` for a client's change; the server's own says which locks count.
    pub(super) rule: Option<&'a super::LockRule>,
}

/// A change decided and handed to the journal, not yet durable.
pub(super) struct Pending {
    seq: Seq,
    item: Item,
    accepted: Accepted,
    /// The change event, encoded once for every recipient.
    event: Bytes,
}

/// A record on its way to the journal.
pub(super) struct Submit {
    generation: u64,
    seq: Seq,
    record: Record,
}

/// The journal's answer for one record.
pub(super) struct Outcome {
    generation: u64,
    seq: Seq,
    result: Result<(), JournalError>,
}

/// A journal lookup of a change id, coming back.
pub(super) struct Lookup {
    conn: ConnId,
    change: Arc<Change>,
    size: usize,
    result: anyhow::Result<Option<i64>>,
}

/// The journal's seq of a change it refused as a duplicate, coming back.
pub(super) struct DuplicateCheck {
    item: Item,
    result: anyhow::Result<Option<i64>>,
}

/// A blob check, coming back with what was decided before it.
pub(super) struct BlobCheck {
    item: Item,
    version: u64,
    decision: Decision,
    asked: Vec<String>,
    result: anyhow::Result<Vec<String>>,
}

/// A big change judged off the task, coming back (`None`: the judging panicked).
pub(super) struct Offloaded {
    item: Item,
    version: u64,
    result: Option<Result<Decision, Refusal>>,
}

/// An accepted change with everything its record and broadcast need, made where it was judged.
pub(super) struct Decision {
    accepted: Accepted,
    blobs: BTreeSet<String>,
    ops: Bytes,
    inverse: Bytes,
    paths: Bytes,
    reads: Bytes,
    event: Bytes,
}

/// Starts the two tasks that hand a room's records to the journal and bring the outcomes back, in
/// the order the room submitted them, and answers where to send records.
pub(super) fn pump(
    journal: JournalWriter,
    internal: mpsc::UnboundedSender<Internal>,
) -> mpsc::UnboundedSender<Submit> {
    let (submit_tx, mut submit_rx) = mpsc::unbounded_channel::<Submit>();
    let (receipt_tx, mut receipt_rx) = mpsc::unbounded_channel::<(u64, Seq, Instant, Receipt)>();
    tokio::spawn(async move {
        while let Some(s) = submit_rx.recv().await {
            let at = Instant::now();
            let receipt = journal.submit(s.record).await;
            if receipt_tx.send((s.generation, s.seq, at, receipt)).is_err() {
                break;
            }
        }
    });
    tokio::spawn(async move {
        while let Some((generation, seq, at, receipt)) = receipt_rx.recv().await {
            let result = receipt.outcome().await;
            metrics::journal_waited(at.elapsed());
            let outcome = Outcome {
                generation,
                seq,
                result,
            };
            if internal.send(Internal::Journal(outcome)).is_err() {
                break;
            }
        }
    });
    submit_tx
}

/// Runs `work` and turns a panic in it into an error, so that whoever waits for its answer gets one.
async fn caught<T>(
    work: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    match AssertUnwindSafe(work).catch_unwind().await {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!("the work panicked")),
    }
}

/// The blob references in what the operations set or insert.
fn blob_refs_of(ops: &[Op]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for op in ops {
        match op {
            Op::Set { value, .. } => out.extend(blob_refs(value)),
            Op::Insert { item, .. } => out.extend(blob_refs(item)),
            Op::Unset { .. } | Op::Remove { .. } | Op::Move { .. } => {}
        }
    }
    out
}

/// Judges a change against `head` and, if it is accepted, makes everything its record and broadcast
/// need. `server_stale` is `Some(check_stale)` for a change the server makes itself
/// ([`decide_server`]). Pure and `Send`, so it can run on the blocking pool.
fn judge(
    head: &Head,
    author: &Author,
    change: &Change,
    known: &HashMap<Arc<str>, Known>,
    server_stale: Option<bool>,
) -> Result<Decision, Refusal> {
    let blobs = blob_refs_of(&change.ops);
    if blobs.len() > MAX_BLOB_REFS {
        return Err(Refusal::Invalid(format!(
            "a change may refer to at most {MAX_BLOB_REFS} blobs"
        )));
    }
    let accepted = match server_stale {
        // The server's own change: no id prefix, staleness only when asked.
        Some(check_stale) => decide_server(head, change, &Rules::openswx(), check_stale)?,
        None => {
            let known = |id: &str| known.get(id).copied().unwrap_or(Known::Unknown);
            decide(
                head,
                &author.client,
                change,
                &known,
                None,
                &Rules::openswx(),
            )?
        }
    };
    let server = &*author.client == SERVER_CLIENT;
    let event = wire::encode_server(&ServerMsg::Change(ChangeEvent {
        seq: accepted.seq,
        client_id: author.client.clone(),
        author: author.user.clone(),
        author_name: author.name.clone(),
        // (As catch-up reads it from a row without one.)
        change_id: if server {
            Arc::from("")
        } else {
            change.change_id.clone()
        },
        ops: change.ops.clone(),
        command: change.command.clone(),
        // TASK 11: undo (the room resolves `undo_of` and hands `decide` the kept inverse).
        undo_of: None,
        reverts: None,
    }));
    Ok(Decision {
        blobs,
        ops: encode_ops(&change.ops),
        inverse: encode_ops(&accepted.inverse),
        paths: encode_paths(&accepted.writes, &accepted.removed),
        reads: encode_reads(&accepted.reads),
        event,
        accepted,
    })
}

/// What a refused sender is told.
fn refusal_message(refusal: &Refusal) -> String {
    let message = match refusal {
        Refusal::Stale(_) => "someone else changed this since your base".to_owned(),
        Refusal::Dependency(id) => format!("it builds on {id}, which was refused"),
        Refusal::NameTaken(name) => format!("the name {name} is taken"),
        Refusal::IdTaken(id) => format!("the id {id} is taken"),
        Refusal::Invalid(why) => why.clone(),
        Refusal::TooOld => {
            "its base is older than the history this server keeps; reload the document".to_owned()
        }
    };
    message.chars().take(MAX_MESSAGE).collect()
}

impl Room {
    // ------------------------------------------------------------------ arriving

    pub(super) fn on_change(&mut self, conn: ConnId, change: Change, size: usize) {
        let Some(c) = self.conns.get(&conn) else {
            return;
        };
        let seen = self.recent.get(&c.client, &change.change_id);
        if matches!(seen, Some(Seen::Tentative(_) | Seen::Checking(_))) && self.replies_full(conn) {
            return; // (resent over and over while it is written: closed as too slow)
        }
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        match seen {
            Some(Seen::Durable(seq)) => return self.accept_again(conn, &change.change_id, seq),
            Some(Seen::Tentative(ticket)) => {
                // Still being written: answered with the change itself, when it is.
                if let Some(p) = self
                    .pending
                    .iter_mut()
                    .find(|p| p.item.ticket == Some(ticket))
                {
                    p.item.waiters.push(conn);
                    c.replies.push_back(Reply::Waiting(ticket));
                    return;
                }
            }
            Some(Seen::Checking(ticket)) => {
                // Its seq is being looked up: answered with it.
                c.replies.push_back(Reply::Waiting(ticket));
                self.checking.entry(ticket).or_default().push(conn);
                return;
            }
            None => {}
        }
        if let Some(reason) = c.read_only.clone() {
            return self.reject(conn, &change.change_id, RejectCode::ReadOnly, &reason);
        }
        let change = Arc::new(change);
        if !self.recent.is_complete(&c.client) {
            // Memory may not hold every id this client journaled: ask the journal first.
            c.busy = true;
            let (store, doc, client) =
                (self.state.store.clone(), self.doc.clone(), c.client.clone());
            let internal = self.internal.clone();
            tokio::spawn(async move {
                let result =
                    caught(store.change_seq_by_client(&doc, &client, &change.change_id)).await;
                let lookup = Lookup {
                    conn,
                    change,
                    size,
                    result,
                };
                let _ = internal.send(Internal::Lookup(lookup));
            });
            return;
        }
        self.judge_conn_change(conn, change, size);
    }

    pub(super) fn on_lookup(&mut self, lookup: Lookup) {
        let Lookup {
            conn,
            change,
            size,
            result,
        } = lookup;
        self.unpark(conn);
        if self.closing.is_some() || !self.conns.contains_key(&conn) {
            return;
        }
        match result {
            Ok(Some(seq)) => {
                let seq = Seq::try_from(seq).unwrap_or_default();
                if let Some(c) = self.conns.get(&conn) {
                    // Remembered, so that a further resend needs no lookup.
                    let client = c.client.clone();
                    self.recent.remember(&client, &change.change_id, seq);
                    self.recent.trim();
                }
                self.accept_again(conn, &change.change_id, seq);
            }
            Ok(None) if self.paused > 0 => self.park_first(conn, Incoming::Judge(change, size)),
            Ok(None) => self.judge_conn_change(conn, change, size),
            Err(e) => {
                tracing::error!(
                    "could not look a change id up in document {}: {e:#}",
                    self.doc
                );
                self.reject(
                    conn,
                    &change.change_id,
                    RejectCode::ServerError,
                    "the server could not check the change; try again",
                );
            }
        }
    }

    pub(super) fn on_server_change(&mut self, sc: ServerChange) {
        if self.closing.is_some() {
            let _ = sc.reply.send(Err(ServerChangeError::NotWritten));
            return;
        }
        let change = Change {
            // `decide` needs an id; it is neither journaled nor broadcast.
            change_id: Arc::from(SERVER_CLIENT),
            base: self.tentative.seq,
            ops: sc.ops,
            reads: Default::default(),
            depends: Vec::new(),
            undo_of: None,
            command: Arc::from(sc.command.as_str()),
        };
        self.server_queue.push_back(Item {
            origin: Origin::Server {
                reply: sc.reply,
                check_stale: sc.options.check_stale,
                locks: sc.options.locks,
            },
            author: Author {
                client: Arc::from(SERVER_CLIENT),
                user: sc.author,
                name: sc.author_name,
            },
            change: Arc::new(change),
            size: 0,
            ticket: None,
            waiters: Vec::new(),
            rejudged: 0,
            blobs_checked: true,
        });
    }

    fn accept_again(&mut self, conn: ConnId, change_id: &Arc<str>, seq: Seq) {
        let frame = self.encode(&ServerMsg::Accepted {
            change_id: change_id.clone(),
            seq,
        });
        self.reply(conn, frame);
    }

    // ------------------------------------------------------------------ judging

    /// What the room knows of the client's changes named in `depends`.
    fn known(&self, client: &ClientId, depends: &[Arc<str>]) -> HashMap<Arc<str>, Known> {
        depends
            .iter()
            .map(|d| {
                let k = if self.recent.get(client, d).is_some() {
                    Known::Accepted
                } else if self.refused.get(client).is_some_and(|r| r.contains(d)) {
                    Known::Refused
                } else {
                    Known::Unknown
                };
                (d.clone(), k)
            })
            .collect()
    }

    pub(super) fn judge_conn_change(&mut self, conn: ConnId, change: Arc<Change>, size: usize) {
        let Some(c) = self.conns.get(&conn) else {
            return;
        };
        let item = Item {
            origin: Origin::Conn(conn),
            author: Author {
                client: c.client.clone(),
                user: c.user.clone(),
                name: c.name.clone(),
            },
            change,
            size,
            ticket: None,
            waiters: Vec::new(),
            rejudged: 0,
            blobs_checked: false,
        };
        self.judge_item(item);
    }

    /// Judges a change (off the task when it is big) and goes on with the decision.
    pub(super) fn judge_item(&mut self, item: Item) {
        let Some(mut item) = self.unless_locked(item) else {
            return;
        };
        if matches!(item.origin, Origin::Server { .. }) {
            // The server's change is made on whatever is current.
            Arc::make_mut(&mut item.change).base = self.tentative.seq;
        }
        let known = self.known(&item.author.client, &item.change.depends);
        let version = self.tentative_version;
        if item.change.ops.len() > BIG_OPS || item.size > BIG_FRAME {
            // Judged off the task; no other frame is taken meanwhile, and the sender's later
            // messages wait; everything else goes on.
            self.paused += 1;
            if let Some(c) = item.conn().and_then(|conn| self.conns.get_mut(&conn)) {
                c.busy = true;
            }
            let head = self.tentative.clone();
            let stale = item.origin.server_stale();
            let (author, change) = (item.author.clone(), item.change.clone());
            let (internal, encodes) = (self.internal.clone(), self.encodes.clone());
            let delay = self.state.room_hooks.judge_delay;
            tokio::spawn(async move {
                let judging = tokio::task::spawn_blocking(move || {
                    std::thread::sleep(delay);
                    let started = Instant::now();
                    let result = judge(&head, &author, &change, &known, stale);
                    metrics::decided(started.elapsed());
                    result
                });
                let result = judging.await.ok();
                if matches!(result, Some(Ok(_))) {
                    encodes.fetch_add(1, Ordering::Relaxed);
                }
                let decided = Offloaded {
                    item,
                    version,
                    result,
                };
                let _ = internal.send(Internal::Decided(decided));
            });
            return;
        }
        let started = Instant::now();
        let stale = item.origin.server_stale();
        let result = judge(&self.tentative, &item.author, &item.change, &known, stale);
        metrics::decided(started.elapsed());
        if result.is_ok() {
            self.encodes.fetch_add(1, Ordering::Relaxed);
        }
        self.decided(item, version, result);
    }

    pub(super) fn on_offloaded(&mut self, decided: Offloaded) {
        self.paused = self.paused.saturating_sub(1);
        let Offloaded {
            item,
            version,
            result,
        } = decided;
        if let Some(conn) = item.conn() {
            self.unpark(conn);
            if !self.conns.contains_key(&conn) {
                return;
            }
        }
        if self.closing.is_some() {
            return self.not_written(item);
        }
        match result {
            None => {
                tracing::error!("judging a change of document {} panicked", self.doc);
                self.answer_refused(
                    item,
                    RejectCode::ServerError,
                    "the server could not judge the change; try again",
                    Vec::new(),
                    None,
                );
            }
            // The head it was judged against changed meanwhile.
            Some(_) if version != self.tentative_version => self.judge_again(item),
            Some(result) => self.decided(item, version, result),
        }
    }

    /// Judges a change once more when the room takes frames, before anything its sender sent later.
    fn judge_again(&mut self, item: Item) {
        if self.paused == 0 {
            return self.judge_item(item);
        }
        match item.origin {
            Origin::Conn(conn) => self.park_first(conn, Incoming::Judge(item.change, item.size)),
            Origin::Server { .. } => self.server_queue.push_front(item),
        }
    }

    fn decided(&mut self, item: Item, version: u64, result: Result<Decision, Refusal>) {
        let decision = match result {
            Ok(decision) => decision,
            Err(refusal) => return self.refuse_item(item, refusal),
        };
        if item.blobs_checked {
            return self.adopt(item, decision);
        }
        let now = self.state.clock.now();
        let missing: Vec<String> = decision
            .blobs
            .iter()
            .filter(|h| self.known_blobs.is_missing(h, now))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return self.missing_blob(item, &missing);
        }
        let unknown: Vec<String> = decision
            .blobs
            .iter()
            .filter(|h| !self.known_blobs.found.contains(*h))
            .cloned()
            .collect();
        if unknown.is_empty() {
            return self.adopt(item, decision);
        }
        if let Some(c) = item.conn().and_then(|conn| self.conns.get_mut(&conn)) {
            c.busy = true;
        }
        let (blobs, org) = (self.state.blobs.clone(), self.org.clone());
        let internal = self.internal.clone();
        tokio::spawn(async move {
            let result = caught(missing_blobs(blobs.as_ref(), &org, unknown.iter().cloned())).await;
            let check = BlobCheck {
                item,
                version,
                decision,
                asked: unknown,
                result,
            };
            let _ = internal.send(Internal::Blobs(check));
        });
    }

    pub(super) fn on_blobs(&mut self, check: BlobCheck) {
        let BlobCheck {
            mut item,
            version,
            decision,
            asked,
            result,
        } = check;
        if let Some(conn) = item.conn() {
            self.unpark(conn);
            if !self.conns.contains_key(&conn) {
                return;
            }
        }
        if self.closing.is_some() {
            return self.not_written(item);
        }
        let missing = match result {
            Ok(missing) => missing,
            Err(e) => {
                tracing::error!("could not check blobs for document {}: {e:#}", self.doc);
                return self.answer_refused(
                    item,
                    RejectCode::ServerError,
                    "the server could not check the blobs; try again",
                    Vec::new(),
                    None,
                );
            }
        };
        let now = self.state.clock.now();
        for sha in asked {
            if missing.contains(&sha) {
                self.known_blobs.note_missing(sha, now);
            } else {
                self.known_blobs.found.insert(sha, BLOBS_KEEP);
            }
        }
        if !missing.is_empty() {
            return self.missing_blob(item, &missing);
        }
        item.blobs_checked = true;
        if version == self.tentative_version {
            self.adopt(item, decision);
        } else {
            // The head moved while storage was asked: judge again (every blob is known now).
            self.judge_again(item);
        }
    }

    fn missing_blob(&mut self, item: Item, missing: &[String]) {
        let message: String = format!("upload these blobs first: {}", missing.join(", "))
            .chars()
            .take(MAX_MESSAGE)
            .collect();
        self.answer_refused(item, RejectCode::MissingBlob, &message, Vec::new(), None);
    }

    // ------------------------------------------------------------------ writing

    /// Makes an accepted change tentative and hands its record to the journal.
    fn adopt(&mut self, mut item: Item, d: Decision) {
        let seq = d.accepted.seq;
        debug_assert_eq!(seq, self.tentative.seq + 1, "decided on the tentative head");
        let ticket = match item.ticket {
            Some(ticket) => ticket,
            None => {
                let ticket = self.next_ticket;
                self.next_ticket += 1;
                item.ticket = Some(ticket);
                if let Some(conn) = item.conn()
                    && let Some(c) = self.conns.get_mut(&conn)
                {
                    c.replies.push_back(Reply::Waiting(ticket));
                    item.waiters.push(conn);
                }
                ticket
            }
        };
        let client = item.conn().map(|_| item.author.client.to_string());
        let record = Record {
            document: self.doc.clone(),
            seq,
            parent_seq: seq - 1,
            epoch: self.epoch,
            author: item.author.user.clone(),
            change_id: client.as_ref().map(|_| item.change.change_id.to_string()),
            client_id: client,
            command: item.change.command.chars().take(MAX_COMMAND).collect(),
            ops: d.ops,
            inverse: d.inverse,
            paths: d.paths,
            reads: d.reads,
            // TASK 11: the seq of the change `undo_of` names.
            undo_of: None,
            reverts: None,
            created_at: self.state.clock.now(),
            // TASK 13: an assembly's component references.
            extra: Vec::new(),
        };
        self.tentative = Head {
            seq,
            doc: d.accepted.doc.clone(),
            index: d.accepted.index.clone(),
        };
        self.tentative_version += 1;
        if let Some(key) = item.key() {
            self.recent.map.insert(key, Seen::Tentative(ticket));
        }
        self.pending.push_back(Pending {
            seq,
            item,
            accepted: d.accepted,
            event: d.event,
        });
        let submit = Submit {
            generation: self.generation,
            seq,
            record,
        };
        if self.journal.send(submit).is_err() {
            self.lose("the journal pump stopped");
        }
    }

    // ------------------------------------------------------------------ answering

    /// Refuses a message of a connection that never entered the pipeline.
    fn reject(&mut self, conn: ConnId, change_id: &Arc<str>, code: RejectCode, message: &str) {
        metrics::change_rejected(code);
        self.note_refused(conn, change_id);
        let frame = self.encode(&ServerMsg::Rejected {
            change_id: change_id.clone(),
            code,
            message: message.to_owned(),
            conflicts: Vec::new(),
        });
        self.reply(conn, frame);
    }

    /// Remembers a refused id of the connection's client, only while it is connected.
    fn note_refused(&mut self, conn: ConnId, change_id: &Arc<str>) {
        if let Some(c) = self.conns.get(&conn) {
            self.refused
                .entry(c.client.clone())
                .or_default()
                .insert(change_id.clone(), REFUSED_KEEP);
        }
    }

    fn refuse_item(&mut self, item: Item, refusal: Refusal) {
        let conflicts = match &refusal {
            Refusal::Stale(conflicts) => conflicts.clone(),
            _ => Vec::new(),
        };
        let message = refusal_message(&refusal);
        let code = refusal.code();
        self.answer_refused(item, code, &message, conflicts, Some(refusal));
    }

    /// Refuses a change because of locks (`locked`), answering whoever waits for it.
    pub(super) fn refuse_locked(&mut self, item: Item, conflicts: Vec<Conflict>, message: &str) {
        match item.origin {
            Origin::Server { reply, .. } => {
                metrics::change_rejected(RejectCode::Locked);
                let _ = reply.send(Err(ServerChangeError::Locked(conflicts)));
            }
            Origin::Conn(_) => {
                self.answer_refused(item, RejectCode::Locked, message, conflicts, None);
            }
        }
    }

    /// Tells everyone waiting for a change that it was refused.
    fn answer_refused(
        &mut self,
        item: Item,
        code: RejectCode,
        message: &str,
        conflicts: Vec<Conflict>,
        refusal: Option<Refusal>,
    ) {
        metrics::change_rejected(code);
        match item.origin {
            Origin::Server { reply, .. } => {
                let err = refusal.map_or(ServerChangeError::NotWritten, ServerChangeError::Refused);
                let _ = reply.send(Err(err));
            }
            Origin::Conn(conn) => {
                self.note_refused(conn, &item.change.change_id);
                let frame = self.encode(&ServerMsg::Rejected {
                    change_id: item.change.change_id.clone(),
                    code,
                    message: message.to_owned(),
                    conflicts,
                });
                match item.ticket {
                    Some(ticket) => {
                        for w in &item.waiters {
                            self.resolve(*w, ticket, frame.clone());
                        }
                    }
                    None => self.reply(conn, frame),
                }
            }
        }
    }

    /// Tells everyone waiting for a change that it is durable as `seq`.
    fn answer_accepted(&mut self, item: Item, seq: Seq) {
        match item.origin {
            Origin::Server { reply, .. } => {
                let _ = reply.send(Ok(seq));
            }
            Origin::Conn(conn) => {
                let frame = self.encode(&ServerMsg::Accepted {
                    change_id: item.change.change_id.clone(),
                    seq,
                });
                match item.ticket {
                    Some(ticket) => {
                        for w in &item.waiters {
                            self.resolve(*w, ticket, frame.clone());
                        }
                    }
                    None => self.reply(conn, frame),
                }
            }
        }
    }

    /// A change that will not be written because the room is closing: the server is told; clients
    /// resend after reconnecting.
    fn not_written(&mut self, item: Item) {
        if let Origin::Server { reply, .. } = item.origin {
            let _ = reply.send(Err(ServerChangeError::NotWritten));
        }
    }

    // ------------------------------------------------------------------ journal outcomes

    pub(super) fn on_journal(&mut self, outcome: Outcome) {
        let Outcome {
            generation,
            seq,
            result,
        } = outcome;
        if self
            .closing
            .as_ref()
            .is_some_and(|c| c.code == super::close::MOVED)
        {
            return; // (unloading: nothing more is settled here)
        }
        if generation != self.generation {
            // A record decided on a head that was given up: the journal refuses it.
            if result.is_ok() {
                self.lose("the journal wrote a change the room had given up");
            }
            return;
        }
        match result {
            Ok(()) => self.made_durable(seq),
            Err(JournalError::Refused(RefusedWhy::Lease)) => {
                self.lose("the room's lease was taken over");
            }
            Err(JournalError::Refused(why)) => self.refused_record(seq, &why),
            Err(JournalError::Unknown) => {
                self.lose("the journal could not say whether a change was written");
            }
        }
    }

    fn made_durable(&mut self, seq: Seq) {
        let front = self.pending.front().map(|p| p.seq);
        if front != Some(seq) {
            return self.lose("the journal answered out of order");
        }
        let Some(p) = self.pending.pop_front() else {
            return;
        };
        let a = &p.accepted;
        self.durable = Head {
            seq,
            doc: a.doc.clone(),
            index: self.durable.index.record(
                seq,
                &p.item.author.client,
                &a.writes,
                &a.removed,
                &a.reads,
            ),
        };
        if self.pending.is_empty() {
            self.tentative = self.durable.clone();
        }
        // Everyone but those waiting for this change's answer hears of it, in seq order.
        self.broadcast(&p.event, &p.item.waiters);
        // Locks on what this change took out of the document go with it.
        self.drop_locks_under(&a.writes, &a.removed);
        if let Some(key) = p.item.key() {
            self.recent.durable(key, seq);
            self.recent.trim();
        }
        self.answer_accepted(p.item, seq);
        metrics::change_accepted();
        self.maybe_snapshot();
    }

    /// The journal refused the first pending record for a cause of its own: answer it, and judge
    /// the changes decided after it again, in their order, against the durable head.
    fn refused_record(&mut self, seq: Seq, why: &RefusedWhy) {
        let mut rest = std::mem::take(&mut self.pending);
        let Some(front) = rest.pop_front() else {
            return;
        };
        if front.seq != seq {
            return self.lose("the journal answered out of order");
        }
        self.generation += 1;
        self.tentative = self.durable.clone();
        self.tentative_version += 1;
        for p in &rest {
            if let Some(key) = p.item.key() {
                self.recent.map.remove(&key);
            }
        }
        let duplicate = matches!(why, RefusedWhy::Duplicate);
        let mut misbehaving = None;
        match (duplicate, front.item.key(), front.item.ticket) {
            (true, Some(key), Some(ticket)) => {
                // Find the seq the journal holds for it, and answer that. Meanwhile it counts as
                // accepted for the changes that build on it. Memory clearly missed one of this
                // client's ids: its unknown ids are looked up from now on.
                self.recent.set_complete(&key.0, false);
                self.recent.map.insert(key, Seen::Checking(ticket));
                let user = front.item.author.user.clone();
                if self.duplicates.add(&user) > MAX_DUPLICATES {
                    misbehaving = Some(user);
                }
                let (store, doc) = (self.state.store.clone(), self.doc.clone());
                let internal = self.internal.clone();
                let item = front.item;
                tokio::spawn(async move {
                    let result = caught(store.change_seq_by_client(
                        &doc,
                        &item.author.client,
                        &item.change.change_id,
                    ))
                    .await;
                    let _ = internal.send(Internal::Duplicate(DuplicateCheck { item, result }));
                });
            }
            _ => {
                if let Some(key) = front.item.key() {
                    self.recent.map.remove(&key);
                }
                tracing::warn!(
                    "the journal refused change {seq} of document {}: {why}",
                    self.doc
                );
                self.answer_refused(
                    front.item,
                    RejectCode::ServerError,
                    NOT_STORED,
                    Vec::new(),
                    None,
                );
            }
        }
        for p in rest {
            let mut item = p.item;
            // A duplicate refusal is the other record's fault (and should not happen at all: see
            // the module documentation); only systematic causes count toward the cap.
            if !duplicate {
                if item.rejudged >= MAX_REJUDGE {
                    self.answer_refused(
                        item,
                        RejectCode::ServerError,
                        NOT_STORED,
                        Vec::new(),
                        None,
                    );
                    continue;
                }
                item.rejudged += 1;
            }
            let Some(mut item) = self.unless_locked(item) else {
                continue;
            };
            if matches!(item.origin, Origin::Server { .. }) {
                Arc::make_mut(&mut item.change).base = self.tentative.seq;
            }
            // Judged here and now, so that the order of the changes is kept (their blobs were
            // checked already).
            let known = self.known(&item.author.client, &item.change.depends);
            let started = Instant::now();
            let stale = item.origin.server_stale();
            let result = judge(&self.tentative, &item.author, &item.change, &known, stale);
            metrics::decided(started.elapsed());
            match result {
                Ok(decision) => {
                    self.encodes.fetch_add(1, Ordering::Relaxed);
                    self.adopt(item, decision);
                }
                Err(refusal) => self.refuse_item(item, refusal),
            }
        }
        if let Some(user) = misbehaving {
            let theirs: Vec<ConnId> = self
                .conns
                .iter()
                .filter(|(_, c)| c.user == user)
                .map(|(id, _)| *id)
                .collect();
            for conn in theirs {
                self.remove_conn(conn, Some((close::POLICY, "misbehaving client")), true);
            }
        }
    }

    pub(super) fn on_duplicate(&mut self, check: DuplicateCheck) {
        let DuplicateCheck { mut item, result } = check;
        if let Some(ticket) = item.ticket {
            // (Connections that sent it again while it was looked up.)
            item.waiters
                .extend(self.checking.remove(&ticket).unwrap_or_default());
        }
        match result {
            Ok(Some(seq)) => {
                let seq = Seq::try_from(seq).unwrap_or_default();
                if let Some((client, id)) = item.key() {
                    self.recent.remember(&client, &id, seq);
                    self.recent.trim();
                }
                self.answer_accepted(item, seq);
            }
            Ok(None) | Err(_) => {
                if let Some(key) = item.key() {
                    if matches!(self.recent.map.get(&key), Some(Seen::Checking(_))) {
                        self.recent.map.remove(&key);
                    }
                    // (The journal may hold it after all: look this client's ids up.)
                    self.recent.set_complete(&key.0, false);
                }
                self.answer_refused(item, RejectCode::ServerError, NOT_STORED, Vec::new(), None);
            }
        }
    }
}
