//! The journal writer: every accepted change of every open document is made durable here, in
//! batches, before anyone is told it was accepted.
//!
//! One background task per store collects [`Record`]s from all rooms. It takes the first record
//! that arrives, keeps collecting until it has `max_batch` records or `max_wait` has passed since
//! the first, and writes the batch in **one transaction**:
//!
//! - per document in the batch, the room lease row is locked and its epoch compared with each
//!   record's; a record carrying another epoch is `Refused(Lease)` (a document without a lease
//!   row gets one, at epoch 0, only for records of epoch 0: a document nobody has acquired yet);
//! - the document's records are inserted in order, each having to be the next seq
//!   (`seq == head_seq + 1`, `parent_seq == head_seq`); one that is not is `Refused(OutOfOrder)`,
//!   and since a refused record does not move the head, the records after it in the batch
//!   are refused the same way (a refused record never lets its successors in out of order, in this
//!   batch or a later one);
//! - a record whose `(document, client_id, change_id)` is already in the journal is
//!   `Refused(Duplicate)` without failing the batch (rooms detect duplicates before this; this is
//!   the backstop). The records after it in the batch then fail the seq check above;
//! - a record carrying [`Extra`] writes runs in its own savepoint, and a failure there refuses that
//!   record only (and, through the seq check, the ones after it);
//! - `documents.head_seq` and `updated_at` are updated once per document.
//!
//! Any error from `begin`, from a statement outside a savepoint, or from `commit`, or a batch that
//! takes longer than [`BATCH_TIMEOUT`], gives **every** record of the batch
//! [`JournalError::Unknown`]: the commit may or may not have taken effect, and the room must reload
//! from the store ([`crate::snapshot::load_head`]) to find out. After a successful commit every
//! record that was not refused is `Ok(())`. Results are delivered in the order the records were
//! submitted, so the results of one document arrive in its seq order.
//!
//! Nothing but [`crate::store::Tx`] calls is awaited while the transaction is open.
//!
//! If the task stops (a panic, or [`JournalWriter::close`]), every record still queued and every
//! later one is `Unknown`, and [`JournalWriter::healthy`] says false (`/api/ready` reports it).
//!
//! # Cost bounds
//!
//! Records arrive here already judged and encoded; the writer does no per-operation work. Bounding
//! the work one change may cost (`Index::check`, large decodes) belongs where changes are judged,
//! in the room (Task 9).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

pub use crate::record::{AssemblyRef, ChosenPart, Extra, Record};
pub use crate::snapshot::{
    LoadError, SERVER_CLIENT, WriteSnapshotError, load_head, state_at, write_snapshot,
};
use crate::store::{AssemblyRefRow, NewChange, RoomLease, Store, Tx};

/// The most records written in one transaction, unless [`JournalWriter::start`] is told otherwise.
pub const MAX_BATCH: usize = 256;
/// How long the writer waits for more records after the first of a batch.
pub const MAX_WAIT: Duration = Duration::from_millis(2);
/// How many records may wait for the writer; [`JournalWriter::submit`] waits while it is full.
pub const QUEUE: usize = 4096;
/// A batch that has not committed by then is abandoned with every record `Unknown` (unless
/// [`JournalWriter::start_with_timeout`] says otherwise).
pub const BATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Column limits of `changes` a record must respect (checked before it is queued, so that one bad
/// record cannot fail a whole batch).
const MAX_CLIENT_ID: usize = 128;
const MAX_CHANGE_ID: usize = 64;
const MAX_COMMAND: usize = 100;
const MAX_ID: usize = 32;
const MAX_COMPONENT_ID: usize = 200;
/// The most rows one `Extra::AssemblyRefs` may write: far more components than an assembly
/// anyone edits live has (a change over it is refused, and its sender told to try again).
pub const MAX_ASSEMBLY_ROWS: usize = 100_000;
/// The most parts one `Extra::AssemblyRefs` may lock, as the room allows a change to choose.
const MAX_CHOSEN: usize = crate::assemblies::MAX_CHOSEN_PARTS;

/// Why a record was not made durable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JournalError {
    /// This record was not written, for the reason given; the rest of its batch was not affected
    /// (though the records after it of the same document are refused too, being out of order).
    #[error("the journal refused the change: {0}")]
    Refused(RefusedWhy),
    /// Whether the record was written is not known (the commit failed or timed out, or the writer
    /// stopped). Reload the document from the store.
    #[error("whether the change was written is not known")]
    Unknown,
}

/// Why the journal refused one record. Match on the variant, never on the text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RefusedWhy {
    /// The record's epoch is not the document's lease epoch: the room has lost the document (another
    /// acquisition took it). Stop writing; reload or unload.
    #[error("lease")]
    Lease,
    /// Its seq is not the document's head plus one (or its parent is not the head): a gap, a record
    /// after a refused one, or a seq already taken.
    #[error("out of order")]
    OutOfOrder,
    /// The document already has a change with this `client_id` and `change_id`.
    #[error("duplicate")]
    Duplicate,
    /// `Extra::SetRevertedBy` named a change that is missing or already reverted.
    #[error("already reverted")]
    AlreadyReverted,
    /// An extra write failed in the database (the cause is logged).
    #[error("extra writes failed")]
    ExtraFailed,
    /// The record breaks a column limit; it was never queued.
    #[error("invalid record: {0}")]
    Invalid(String),
    /// The document does not exist.
    #[error("no such document")]
    NoDocument,
    /// `Extra::AssemblyRefs` chose a part that no longer exists or was deleted since the room
    /// checked it; `component` is the first component pointed at it.
    #[error("a chosen part is gone")]
    PartGone { component: String },
}

type Reply = oneshot::Sender<Result<(), JournalError>>;

struct Pending {
    record: Record,
    reply: Reply,
}

enum Msg {
    Record(Box<Pending>),
    Close(oneshot::Sender<()>),
}

/// The handle rooms write through. Cloning is cheap: clones share the one writer task.
#[derive(Clone)]
pub struct JournalWriter {
    queue: mpsc::Sender<Msg>,
    alive: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
}

/// How the writer task batches.
#[derive(Debug, Clone, Copy)]
struct Batching {
    max_batch: usize,
    max_wait: Duration,
    timeout: Duration,
}

impl std::fmt::Debug for JournalWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalWriter")
            .field("healthy", &self.healthy())
            .finish_non_exhaustive()
    }
}

/// What [`JournalWriter::submit`] hands back: the record's outcome, once its batch is settled.
#[derive(Debug)]
pub struct Receipt(oneshot::Receiver<Result<(), JournalError>>);

impl Receipt {
    /// Resolves when the record is durable (`Ok`), refused, or its outcome is unknown.
    pub async fn outcome(self) -> Result<(), JournalError> {
        self.0.await.unwrap_or(Err(JournalError::Unknown))
    }
}

/// Clears the alive flag when the writer task ends, however it ends (a panic included).
struct Alive(Arc<AtomicBool>);

impl Drop for Alive {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
        if std::thread::panicking() {
            tracing::error!("the journal writer stopped: no change can be made durable");
        }
    }
}

impl JournalWriter {
    /// Starts the writer task on the current tokio runtime (it must be called inside one).
    /// `max_batch` is at least 1.
    pub fn start(store: Arc<dyn Store>, max_batch: usize, max_wait: Duration) -> Self {
        Self::start_with_timeout(store, max_batch, max_wait, BATCH_TIMEOUT)
    }

    /// [`JournalWriter::start`] with another limit than [`BATCH_TIMEOUT`] on how long one batch may
    /// take before every record of it is `Unknown`.
    pub fn start_with_timeout(
        store: Arc<dyn Store>,
        max_batch: usize,
        max_wait: Duration,
        timeout: Duration,
    ) -> Self {
        let (queue, rx) = mpsc::channel(QUEUE);
        let alive = Arc::new(AtomicBool::new(true));
        let guard = Alive(alive.clone());
        let batching = Batching {
            max_batch: max_batch.max(1),
            max_wait,
            timeout,
        };
        tokio::spawn(run(store, rx, batching, guard));
        Self {
            queue,
            alive,
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether the writer is running and accepting records.
    pub fn healthy(&self) -> bool {
        self.alive.load(Ordering::SeqCst) && !self.closed.load(Ordering::SeqCst)
    }

    /// Queues a record (waiting while the queue is full) and answers a [`Receipt`] for its outcome.
    /// Records are written in the order they are submitted: a room submits its records one after
    /// another and awaits the receipts later, so that it can go on with the next message meanwhile.
    pub async fn submit(&self, record: Record) -> Receipt {
        let (reply, rx) = oneshot::channel();
        if let Err(why) = check(&record) {
            let _ = reply.send(Err(JournalError::Refused(RefusedWhy::Invalid(why))));
        } else if self.healthy() {
            // Fails only when the task has stopped; the reply is then dropped: `Unknown`.
            let _ = self
                .queue
                .send(Msg::Record(Box::new(Pending { record, reply })))
                .await;
        }
        // Otherwise `reply` is dropped here: `Unknown`.
        Receipt(rx)
    }

    /// Writes one record and resolves when it is durable: [`JournalWriter::submit`], then the
    /// receipt's outcome.
    pub async fn append(&self, record: Record) -> Result<(), JournalError> {
        self.submit(record).await.outcome().await
    }

    /// Writes what is queued, then stops the writer. Records submitted afterwards are `Unknown`.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let (done, wait) = oneshot::channel();
        if self.queue.send(Msg::Close(done)).await.is_ok() {
            let _ = wait.await;
        }
    }
}

/// Refuses a record that would break a column limit (so it can never fail a statement).
fn check(r: &Record) -> Result<(), String> {
    let chars = |s: &str| s.chars().count();
    let fits = |n: u64| i64::try_from(n).is_ok();
    let bad = if r.seq == 0 || !fits(r.seq) || !fits(r.parent_seq) || !fits(r.epoch) {
        Some("seq, parent_seq and epoch must be positive 63-bit numbers")
    } else if r.undo_of.is_some_and(|s| !fits(s)) || r.reverts.is_some_and(|s| !fits(s)) {
        Some("undo_of and reverts must be 63-bit numbers")
    } else if chars(&r.document) > MAX_ID || chars(&r.author) > MAX_ID {
        Some("an id is too long")
    } else if r
        .client_id
        .as_deref()
        .is_some_and(|c| chars(c) > MAX_CLIENT_ID)
    {
        Some("client_id is too long")
    } else if r
        .change_id
        .as_deref()
        .is_some_and(|c| c.is_empty() || chars(c) > MAX_CHANGE_ID)
    {
        Some("change_id must be 1 to 64 characters")
    } else if chars(&r.command) > MAX_COMMAND {
        Some("command is too long")
    } else if !r.created_at.is_finite() {
        Some("created_at is not a time")
    } else if r.extra.iter().any(|e| match e {
        Extra::SetRevertedBy { seq } => !fits(*seq),
        Extra::AssemblyRefs { rows, chosen } => {
            rows.len() > MAX_ASSEMBLY_ROWS
                || chosen.len() > MAX_CHOSEN
                || rows.iter().any(|a| {
                    chars(&a.component_id) > MAX_COMPONENT_ID
                        || chars(&a.part_id) > MAX_ID
                        || !fits(a.seq)
                })
                || chosen.iter().any(|c| {
                    chars(&c.component_id) > MAX_COMPONENT_ID || chars(&c.part_id) > MAX_ID
                })
        }
    }) {
        Some("an extra write breaks a limit")
    } else {
        None
    };
    match bad {
        Some(why) => Err(why.to_owned()),
        None => Ok(()),
    }
}

/// A number [`check`] has already bounded.
fn int(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// The writer task.
async fn run(store: Arc<dyn Store>, mut rx: mpsc::Receiver<Msg>, batching: Batching, alive: Alive) {
    let mut closing = None;
    loop {
        let first = match rx.recv().await {
            Some(Msg::Record(p)) => *p,
            Some(Msg::Close(done)) => {
                closing = Some(done);
                break;
            }
            None => break,
        };
        let deadline = Instant::now() + batching.max_wait;
        let mut batch = vec![first];
        let mut stop = false;
        while batch.len() < batching.max_batch {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(Msg::Record(p))) => batch.push(*p),
                Ok(Some(Msg::Close(done))) => {
                    closing = Some(done);
                    stop = true;
                    break;
                }
                Ok(None) => {
                    stop = true;
                    break;
                }
                Err(_) => break,
            }
        }
        write(store.as_ref(), batch, batching.timeout).await;
        if stop {
            break;
        }
    }
    // Whatever is still queued (submitted while closing) is dropped: `Unknown`.
    rx.close();
    while rx.try_recv().is_ok() {}
    drop(alive);
    if let Some(done) = closing {
        let _ = done.send(());
    }
}

/// Writes one batch and answers every record, in order.
async fn write(store: &dyn Store, batch: Vec<Pending>, timeout: Duration) {
    let (records, replies): (Vec<Record>, Vec<Reply>) =
        batch.into_iter().map(|p| (p.record, p.reply)).unzip();
    let n = records.len();
    crate::metrics::journal_batch(n);
    let results = match tokio::time::timeout(timeout, write_batch(store, &records)).await {
        Ok(Ok(results)) => results,
        Ok(Err(e)) => {
            tracing::error!("a journal batch of {n} records failed: {e:#}");
            vec![Err(JournalError::Unknown); n]
        }
        Err(_) => {
            tracing::error!("a journal batch of {n} records took longer than {timeout:?}");
            vec![Err(JournalError::Unknown); n]
        }
    };
    for (reply, result) in replies.into_iter().zip(results) {
        let _ = reply.send(result);
    }
}

/// The batch's transaction. An `Err` is an error outside any savepoint: nothing is known.
async fn write_batch(
    store: &dyn Store,
    records: &[Record],
) -> anyhow::Result<Vec<Result<(), JournalError>>> {
    let refused = |why: RefusedWhy| Err(JournalError::Refused(why));
    let mut results: Vec<Result<(), JournalError>> =
        vec![Err(JournalError::Unknown); records.len()];
    // The records of each document, in submission order; documents in order of first appearance.
    let mut docs: Vec<&str> = Vec::new();
    let mut of_doc: HashMap<&str, Vec<usize>> = HashMap::new();
    for (i, r) in records.iter().enumerate() {
        of_doc
            .entry(r.document.as_str())
            .or_insert_with(|| {
                docs.push(r.document.as_str());
                Vec::new()
            })
            .push(i);
    }

    let mut tx = store.begin().await?;
    for doc in docs {
        let indices = &of_doc[doc];
        let refuse_all = |results: &mut Vec<Result<(), JournalError>>, why: RefusedWhy| {
            for &i in indices {
                results[i] = refused(why.clone());
            }
        };
        let first_epoch = records[indices[0]].epoch;
        let lease = match tx.lock_room_lease(doc).await? {
            Some(epoch) => Some(epoch),
            None if first_epoch == 0 => {
                // A document nobody acquired yet: its first writer, at epoch 0, creates the row;
                // the first `lease::acquire` then moves it to epoch 1. The document is checked
                // first, so that a missing one never reaches the insert (whose foreign key would
                // fail the whole batch).
                if tx.document_by_id(doc).await?.is_none() {
                    refuse_all(&mut results, RefusedWhy::NoDocument);
                    continue;
                }
                tx.insert_room_lease(&RoomLease {
                    document_id: doc.to_owned(),
                    node_id: String::new(),
                    epoch: 0,
                    expires_at: 0.0,
                })
                .await?;
                Some(0)
            }
            None => None,
        };
        // The head is read only now, with the lease row held: on PostgreSQL a batch that waited on
        // that lock for another writer's transaction must not use a head read before it waited.
        let Some(document) = tx.document_by_id(doc).await? else {
            refuse_all(&mut results, RefusedWhy::NoDocument);
            continue;
        };
        let mut head = document.head_seq;
        let mut updated_at = None;
        for &i in indices {
            let r = &records[i];
            let result = if lease != Some(int(r.epoch)) {
                refused(RefusedWhy::Lease)
            } else if int(r.seq) != head + 1 || int(r.parent_seq) != head {
                refused(RefusedWhy::OutOfOrder)
            } else {
                insert(tx.as_mut(), r).await?
            };
            if result.is_ok() {
                head += 1;
                updated_at = Some(r.created_at);
            }
            results[i] = result;
        }
        if let Some(updated_at) = updated_at {
            tx.set_document_head(doc, head, updated_at).await?;
        }
    }
    tx.commit().await?;
    Ok(results)
}

/// The row of a record, borrowing its strings and bytes.
fn change_row(r: &Record) -> NewChange<'_> {
    NewChange {
        document_id: &r.document,
        seq: int(r.seq),
        parent_seq: int(r.parent_seq),
        branch_id: "main",
        epoch: int(r.epoch),
        author_id: &r.author,
        client_id: r.client_id.as_deref(),
        change_id: r.change_id.as_deref(),
        created_at: r.created_at,
        command: &r.command,
        ops: &r.ops,
        inverse: &r.inverse,
        paths: &r.paths,
        reads: &r.reads,
        undo_of: r.undo_of.map(int),
        reverts: r.reverts.map(int),
        reverted_by: None,
        broke: None,
    }
}

/// Inserts one record that has passed the lease and seq checks. The outer `Err` fails the batch.
async fn insert(tx: &mut dyn Tx, r: &Record) -> anyhow::Result<Result<(), JournalError>> {
    let row = change_row(r);
    if r.extra.is_empty() {
        return Ok(if tx.insert_change(&row).await? {
            Ok(())
        } else {
            Err(JournalError::Refused(RefusedWhy::Duplicate))
        });
    }
    let mut sp = tx.savepoint().await?;
    match with_extra(sp.as_mut(), r, &row).await {
        Ok(Ok(())) => {
            sp.commit().await?;
            Ok(Ok(()))
        }
        Ok(Err(refusal)) => {
            sp.rollback().await?;
            Ok(Err(refusal))
        }
        Err(e) => {
            sp.rollback().await?;
            tracing::warn!(
                "a journal record of document {} with extra writes failed: {e:#}",
                r.document
            );
            Ok(Err(JournalError::Refused(RefusedWhy::ExtraFailed)))
        }
    }
}

/// The record and its extra writes, inside its savepoint.
async fn with_extra(
    sp: &mut dyn Tx,
    r: &Record,
    row: &NewChange<'_>,
) -> anyhow::Result<Result<(), JournalError>> {
    let refused = |why: RefusedWhy| Ok(Err(JournalError::Refused(why)));
    if !sp.insert_change(row).await? {
        return refused(RefusedWhy::Duplicate);
    }
    for extra in &r.extra {
        match extra {
            Extra::SetRevertedBy { seq } => {
                if !sp.set_reverted_by(&r.document, int(*seq), row.seq).await? {
                    return refused(RefusedWhy::AlreadyReverted);
                }
            }
            Extra::AssemblyRefs { rows, chosen } => {
                // Every part the change newly chose must still be there: its row is locked (on
                // PostgreSQL; SQLite has one writer), so a deletion either committed before this
                // (refused here) or waits and then sees this reference. Part rows are always locked
                // in ascending id order, each once (`crate::assemblies`, "Lock order").
                let mut chosen: Vec<&ChosenPart> = chosen.iter().collect();
                chosen.sort_by(|a, b| a.part_id.cmp(&b.part_id));
                chosen.dedup_by(|a, b| a.part_id == b.part_id);
                for c in chosen {
                    if !sp.lock_live_document(&c.part_id).await? {
                        return refused(RefusedWhy::PartGone {
                            component: c.component_id.clone(),
                        });
                    }
                }
                let rows: Vec<AssemblyRefRow> = rows
                    .iter()
                    .map(|a| AssemblyRefRow {
                        assembly_id: r.document.clone(),
                        component_id: a.component_id.clone(),
                        part_id: a.part_id.clone(),
                        seq: int(a.seq),
                    })
                    .collect();
                sp.replace_assembly_refs(&r.document, &rows).await?;
            }
        }
    }
    Ok(Ok(()))
}
