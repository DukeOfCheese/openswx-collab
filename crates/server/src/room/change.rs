//! The change pipeline of a room.
//!
//! For each `change` a connection sends, in this order:
//!
//! 1. **Resend?** A change id this client already used is answered with the seq it got
//!    (`change.accepted`), or, while that change is still being written, answered when it is. The
//!    room remembers, in memory, every change decided but not yet durable and every change made
//!    durable since it loaded, back to a floor: the older ones it forgets (keeping at least those
//!    since the last snapshot, and [`RECENT_KEEP`] in all) raise the floor. A change made on a base
//!    below the floor may be a resend of a change memory no longer holds (an accepted change's seq
//!    is above its base), so only then is the journal asked, by `(document, client, change id)`.
//!    The journal's unique key is a backstop, not the mechanism.
//! 2. **Read-only** connections are refused (`read_only`).
//! 3. **Judged** by [`oswx_core::decide`] against the tentative head, under the sender's client id,
//!    with `Rules::openswx()`. `depends` are answered from what the room knows of the client's own
//!    changes: accepted (tentatively or durably) if memory holds it, refused if the client's
//!    refused ids hold it, unknown otherwise. A big change (more than [`BIG_OPS`] operations or a
//!    frame over [`BIG_FRAME`]) is judged on the blocking pool against a copy of the head (a
//!    persistent value, so the copy is cheap) while the room takes no other frame but goes on with
//!    everything else.
//! 4. **Blobs**: the `{"$blob": sha}` references in what it sets or inserts that the room has not
//!    seen yet are looked up in the organisation's storage (16 at a time) off the task, the sender's
//!    later messages waiting; a missing one is `missing_blob`, a storage error `server_error`.
//! 5. **Written**: the change becomes tentative and its record goes to the journal. When the journal
//!    reports it durable the durable head moves, the change is broadcast to every other connection
//!    and the sender is told `change.accepted`.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use bytes::Bytes;
use oswx_core::wire::{self, ChangeEvent, ServerMsg};
use oswx_core::{
    Accepted, Change, ClientId, Head, Known, Op, Refusal, RejectCode, Rules, Seq, decide,
};
use tokio::sync::mpsc;

use super::{ConnId, Internal, Reply, Room};
use crate::blobs::missing_blobs;
use crate::content::blob_refs;
use crate::journal::{JournalError, JournalWriter, Receipt, Record, RefusedWhy};
use crate::metrics;
use crate::record::{encode_ops, encode_paths, encode_reads};

/// A change with more operations than this is judged off the room's task.
pub const BIG_OPS: usize = 256;
/// A change in a frame longer than this is judged off the room's task.
pub const BIG_FRAME: usize = 256 * 1024;
/// Durable change ids kept in memory beyond those since the last snapshot.
pub const RECENT_KEEP: usize = 5000;
/// Refused change ids remembered per client.
const REFUSED_KEEP: usize = 1024;
/// Blob hashes known to exist, remembered per room.
const KNOWN_BLOBS_KEEP: usize = 10_000;
/// The longest `command` the journal keeps.
const MAX_COMMAND: usize = 100;
/// The longest `message` of a rejection.
const MAX_MESSAGE: usize = 300;

// ------------------------------------------------------------------ memory

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    /// Decided, not yet durable.
    Tentative(Seq),
    Durable(Seq),
}

type Key = (ClientId, Arc<str>);

/// The change ids the room holds in memory; see the module documentation.
pub(super) struct Recent {
    map: HashMap<Key, Seen>,
    /// Durable entries, oldest first.
    order: VecDeque<(Seq, Key)>,
    /// Every durable change with a seq above this is in `map`.
    floor: Seq,
}

impl Recent {
    /// Memory as a room loaded at `head` has it: nothing, every change at or below `head` being
    /// asked of the journal.
    pub(super) fn new(head: Seq) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            floor: head,
        }
    }

    fn get(&self, client: &ClientId, change_id: &Arc<str>) -> Option<Seen> {
        self.map.get(&(client.clone(), change_id.clone())).copied()
    }

    /// Forgets the oldest durable entries past [`RECENT_KEEP`], never one after `snapshot_seq`.
    pub(super) fn trim(&mut self, snapshot_seq: Seq) {
        while self.order.len() > RECENT_KEEP {
            let Some((seq, _)) = self.order.front() else {
                break;
            };
            if *seq > snapshot_seq {
                break;
            }
            if let Some((seq, key)) = self.order.pop_front() {
                if self.map.get(&key) == Some(&Seen::Durable(seq)) {
                    self.map.remove(&key);
                }
                self.floor = self.floor.max(seq);
            }
        }
    }
}

/// The change ids of one client that were refused (bounded).
pub(super) type RefusedIds = super::Bounded<Arc<str>>;

/// Blob hashes known to exist in the organisation's storage (bounded).
#[derive(Debug, Default)]
pub(super) struct KnownBlobs(super::Bounded<String>);

// ------------------------------------------------------------------ in flight

/// A change decided and handed to the journal, not yet durable.
pub(super) struct Pending {
    seq: Seq,
    client: ClientId,
    change_id: Arc<str>,
    accepted: Accepted,
    /// The change event, encoded once for every recipient.
    event: Bytes,
    /// Connections waiting for its answer (the sender, and any that resent it meanwhile).
    waiters: Vec<ConnId>,
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
    change: Change,
    size: usize,
    result: anyhow::Result<Option<i64>>,
}

/// A blob check, coming back with what was decided before it.
pub(super) struct BlobCheck {
    conn: ConnId,
    change: Change,
    size: usize,
    version: u64,
    decision: Decision,
    asked: Vec<String>,
    result: anyhow::Result<Vec<String>>,
}

/// A big change judged off the task, coming back.
pub(super) struct Offloaded {
    conn: ConnId,
    change: Change,
    size: usize,
    version: u64,
    result: Result<Decision, Refusal>,
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

/// Who a change is from, for its broadcast.
struct Author {
    client: ClientId,
    user: String,
    name: String,
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
/// need. Pure and `Send`, so it can run on the blocking pool.
fn judge(
    head: &Head,
    author: &Author,
    change: &Change,
    known: &HashMap<Arc<str>, Known>,
) -> Result<Decision, Refusal> {
    let known = |id: &str| known.get(id).copied().unwrap_or(Known::Unknown);
    let accepted = decide(
        head,
        &author.client,
        change,
        &known,
        None,
        &Rules::openswx(),
    )?;
    let event = wire::encode_server(&ServerMsg::Change(ChangeEvent {
        seq: accepted.seq,
        client_id: author.client.clone(),
        author: author.user.clone(),
        author_name: author.name.clone(),
        change_id: change.change_id.clone(),
        ops: change.ops.clone(),
        command: change.command.clone(),
        // TASK 11: undo (the room resolves `undo_of` and hands `decide` the kept inverse).
        undo_of: None,
    }));
    Ok(Decision {
        blobs: blob_refs_of(&change.ops),
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
    pub(super) fn on_change(&mut self, conn: ConnId, change: Change, size: usize) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        match self.recent.get(&c.client, &change.change_id) {
            Some(Seen::Durable(seq)) => return self.accept_again(conn, &change.change_id, seq),
            Some(Seen::Tentative(seq)) => {
                // Still being written: answered with the change itself, when it is.
                if let Some(p) = self.pending.iter_mut().find(|p| p.seq == seq) {
                    p.waiters.push(conn);
                    c.replies.push_back(Reply::Waiting(seq));
                    return;
                }
            }
            None => {}
        }
        if change.base < self.recent.floor {
            // Possibly a resend of a change memory no longer holds: ask the journal.
            c.busy = true;
            let (store, doc, client) =
                (self.state.store.clone(), self.doc.clone(), c.client.clone());
            let internal = self.internal.clone();
            tokio::spawn(async move {
                let result = store
                    .change_seq_by_client(&doc, &client, &change.change_id)
                    .await;
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
        self.judge_change(conn, change, size);
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
                    let key = (c.client.clone(), change.change_id.clone());
                    self.recent.map.insert(key.clone(), Seen::Durable(seq));
                    self.recent.order.push_back((seq, key));
                }
                self.accept_again(conn, &change.change_id, seq);
            }
            Ok(None) => self.judge_change(conn, change, size),
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
                    Vec::new(),
                );
            }
        }
    }

    fn accept_again(&mut self, conn: ConnId, change_id: &Arc<str>, seq: Seq) {
        let frame = self.encode(&ServerMsg::Accepted {
            change_id: change_id.clone(),
            seq,
        });
        self.reply(conn, frame);
    }

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

    fn judge_change(&mut self, conn: ConnId, change: Change, size: usize) {
        let Some(c) = self.conns.get(&conn) else {
            return;
        };
        if let Some(reason) = c.read_only.clone() {
            return self.reject(
                conn,
                &change.change_id,
                RejectCode::ReadOnly,
                &reason,
                Vec::new(),
            );
        }
        let author = Author {
            client: c.client.clone(),
            user: c.user.clone(),
            name: c.name.clone(),
        };
        let known = self.known(&author.client, &change.depends);
        let version = self.tentative_version;
        if change.ops.len() > BIG_OPS || size > BIG_FRAME {
            // Judged off the task; no other frame is taken meanwhile, everything else goes on.
            self.paused = true;
            let head = self.tentative.clone();
            let internal = self.internal.clone();
            let encodes = self.encodes.clone();
            tokio::task::spawn_blocking(move || {
                let started = Instant::now();
                let result = judge(&head, &author, &change, &known);
                metrics::decided(started.elapsed());
                if result.is_ok() {
                    encodes.fetch_add(1, Ordering::Relaxed);
                }
                let decided = Offloaded {
                    conn,
                    change,
                    size,
                    version,
                    result,
                };
                let _ = internal.send(Internal::Decided(decided));
            });
            return;
        }
        let started = Instant::now();
        let result = judge(&self.tentative, &author, &change, &known);
        metrics::decided(started.elapsed());
        if result.is_ok() {
            self.encodes.fetch_add(1, Ordering::Relaxed);
        }
        self.decided(conn, change, size, version, result);
    }

    pub(super) fn on_offloaded(&mut self, decided: Offloaded) {
        self.paused = false;
        let Offloaded {
            conn,
            change,
            size,
            version,
            result,
        } = decided;
        if self.closing.is_some() || !self.conns.contains_key(&conn) {
            return;
        }
        if version != self.tentative_version {
            // The head it was judged against was discarded meanwhile.
            return self.judge_change(conn, change, size);
        }
        self.decided(conn, change, size, version, result);
    }

    fn decided(
        &mut self,
        conn: ConnId,
        change: Change,
        size: usize,
        version: u64,
        result: Result<Decision, Refusal>,
    ) {
        let decision = match result {
            Ok(decision) => decision,
            Err(refusal) => {
                let conflicts = match &refusal {
                    Refusal::Stale(conflicts) => conflicts.clone(),
                    _ => Vec::new(),
                };
                let message = refusal_message(&refusal);
                return self.reject(conn, &change.change_id, refusal.code(), &message, conflicts);
            }
        };
        let unknown: Vec<String> = decision
            .blobs
            .iter()
            .filter(|h| !self.known_blobs.0.contains(*h))
            .cloned()
            .collect();
        if unknown.is_empty() {
            return self.adopt(conn, change, decision);
        }
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        c.busy = true;
        let (blobs, org) = (self.state.blobs.clone(), self.org.clone());
        let internal = self.internal.clone();
        tokio::spawn(async move {
            let result = missing_blobs(blobs.as_ref(), &org, unknown.iter().cloned()).await;
            let check = BlobCheck {
                conn,
                change,
                size,
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
            conn,
            change,
            size,
            version,
            decision,
            asked,
            result,
        } = check;
        self.unpark(conn);
        if self.closing.is_some() || !self.conns.contains_key(&conn) {
            return;
        }
        let missing = match result {
            Ok(missing) => missing,
            Err(e) => {
                tracing::error!("could not check blobs for document {}: {e:#}", self.doc);
                return self.reject(
                    conn,
                    &change.change_id,
                    RejectCode::ServerError,
                    "the server could not check the blobs; try again",
                    Vec::new(),
                );
            }
        };
        for sha in asked.into_iter().filter(|s| !missing.contains(s)) {
            self.known_blobs.0.insert(sha, KNOWN_BLOBS_KEEP);
        }
        if !missing.is_empty() {
            let message: String = format!("upload these blobs first: {}", missing.join(", "))
                .chars()
                .take(MAX_MESSAGE)
                .collect();
            return self.reject(
                conn,
                &change.change_id,
                RejectCode::MissingBlob,
                &message,
                Vec::new(),
            );
        }
        if version == self.tentative_version {
            self.adopt(conn, change, decision);
        } else if !self.paused {
            // The head moved while storage was asked: judge again (every blob is known now).
            self.judge_change(conn, change, size);
        } else if let Some(c) = self.conns.get_mut(&conn) {
            // A big change is being judged: judge this one after it, before anything later.
            c.parked
                .push_front((oswx_core::wire::ClientMsg::Change(change), size));
            self.ready.push_front(conn);
        }
    }

    /// Makes an accepted change tentative and hands its record to the journal.
    fn adopt(&mut self, conn: ConnId, change: Change, d: Decision) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        let seq = d.accepted.seq;
        debug_assert_eq!(seq, self.tentative.seq + 1, "decided on the tentative head");
        c.replies.push_back(Reply::Waiting(seq));
        let record = Record {
            document: self.doc.clone(),
            seq,
            parent_seq: seq - 1,
            epoch: self.epoch,
            author: c.user.clone(),
            client_id: Some(c.client.to_string()),
            change_id: Some(change.change_id.to_string()),
            command: change.command.chars().take(MAX_COMMAND).collect(),
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
        let client = c.client.clone();
        self.tentative = Head {
            seq,
            doc: d.accepted.doc.clone(),
            index: d.accepted.index.clone(),
        };
        self.tentative_version += 1;
        self.recent.map.insert(
            (client.clone(), change.change_id.clone()),
            Seen::Tentative(seq),
        );
        self.pending.push_back(Pending {
            seq,
            client,
            change_id: change.change_id,
            accepted: d.accepted,
            event: d.event,
            waiters: vec![conn],
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

    /// Refuses a change: tells the sender (in turn) and remembers the id as refused.
    fn reject(
        &mut self,
        conn: ConnId,
        change_id: &Arc<str>,
        code: RejectCode,
        message: &str,
        conflicts: Vec<oswx_core::Conflict>,
    ) {
        metrics::change_rejected(code);
        if let Some(c) = self.conns.get(&conn) {
            self.refused
                .entry(c.client.clone())
                .or_default()
                .insert(change_id.clone(), REFUSED_KEEP);
        }
        let frame = self.encode(&ServerMsg::Rejected {
            change_id: change_id.clone(),
            code,
            message: message.to_owned(),
            conflicts,
        });
        self.reply(conn, frame);
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
            // A record decided on a head that was discarded: the journal refuses it.
            if result.is_ok() {
                self.lose("the journal wrote a change the room had discarded");
            }
            return;
        }
        match result {
            Ok(()) => self.made_durable(seq),
            Err(JournalError::Refused(RefusedWhy::Lease)) => {
                self.lose("the room's lease was taken over");
            }
            Err(JournalError::Refused(why)) => {
                tracing::warn!(
                    "the journal refused change {seq} of document {}: {why}",
                    self.doc
                );
                self.discard_tentative();
            }
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
            index: self
                .durable
                .index
                .record(seq, &p.client, &a.writes, &a.removed, &a.reads),
        };
        if self.pending.is_empty() {
            self.tentative = self.durable.clone();
        }
        // Everyone but those waiting for this change's answer hears of it, in seq order.
        self.broadcast(&p.event, &p.waiters);
        let ack = self.encode(&ServerMsg::Accepted {
            change_id: p.change_id.clone(),
            seq,
        });
        for w in &p.waiters {
            self.resolve(*w, seq, ack.clone());
        }
        let key = (p.client, p.change_id);
        self.recent.map.insert(key.clone(), Seen::Durable(seq));
        self.recent.order.push_back((seq, key));
        self.recent.trim(self.snapshot_seq);
        metrics::change_accepted();
        self.maybe_snapshot();
    }

    /// The journal refused a change: it and every change decided after it are refused with
    /// `server_error` (their senders may resend), and the tentative head is the durable one again.
    fn discard_tentative(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        self.generation += 1;
        self.tentative = self.durable.clone();
        self.tentative_version += 1;
        for p in pending {
            self.recent
                .map
                .remove(&(p.client.clone(), p.change_id.clone()));
            metrics::change_rejected(RejectCode::ServerError);
            self.refused
                .entry(p.client.clone())
                .or_default()
                .insert(p.change_id.clone(), REFUSED_KEEP);
            let frame = self.encode(&ServerMsg::Rejected {
                change_id: p.change_id.clone(),
                code: RejectCode::ServerError,
                message: "the server could not store the change; try again".into(),
                conflicts: Vec::new(),
            });
            for w in &p.waiters {
                self.resolve(*w, p.seq, frame.clone());
            }
        }
    }
}
