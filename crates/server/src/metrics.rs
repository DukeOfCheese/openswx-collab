//! What the server counts about itself, and `GET /metrics`, which tells it in the Prometheus text
//! format ([`render`]).
//!
//! The counters and histograms are plain process-wide atomics, so that counting costs a few
//! relaxed adds and the server's logic only calls the functions below. The gauges (open rooms and
//! connections, running exports and uploads, waiting requests) are read from the state when
//! `/metrics` is asked; nothing here reads the database or the storage.
//!
//! **No label is ever made from a request.** Every label value is one of a fixed list written in
//! this file (a reject code, a direction, a pool, a cause), so no user, document, project or
//! organisation id, name or address can appear in the output, and the number of series never
//! grows.
//!
//! `/metrics` is off unless `OSWX_COLLAB_METRICS_TOKEN` is set (404, as any unknown path), and
//! then answers only requests that carry the token as `Authorization: Bearer <token>` (401
//! otherwise): what a server is doing is not for everyone who can reach its address.

use std::fmt::Write as _;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use oswx_core::RejectCode;
use subtle::ConstantTimeEq;

use crate::error::ApiError;
use crate::state::AppState;

/// The reject codes, in the order [`Counters::rejected`] counts them.
pub const REJECT_CODES: [RejectCode; 10] = [
    RejectCode::Stale,
    RejectCode::Locked,
    RejectCode::Dependency,
    RejectCode::NameTaken,
    RejectCode::IdTaken,
    RejectCode::Invalid,
    RejectCode::MissingBlob,
    RejectCode::ReadOnly,
    RejectCode::TooOld,
    RejectCode::ServerError,
];

static ACCEPTED: AtomicU64 = AtomicU64::new(0);
static REJECTED: [AtomicU64; 10] = [const { AtomicU64::new(0) }; 10];
static DECIDES: AtomicU64 = AtomicU64::new(0);
static DECIDE_NANOS: AtomicU64 = AtomicU64::new(0);
static JOURNAL_WAITS: AtomicU64 = AtomicU64::new(0);
static JOURNAL_WAIT_NANOS: AtomicU64 = AtomicU64::new(0);
static BROADCASTS: AtomicU64 = AtomicU64::new(0);
static BROADCAST_RECIPIENTS: AtomicU64 = AtomicU64::new(0);
static SLOW_CONSUMERS: AtomicU64 = AtomicU64::new(0);
static JOURNAL_BATCHES: AtomicU64 = AtomicU64::new(0);
static JOURNAL_RECORDS: AtomicU64 = AtomicU64::new(0);
static LOCKS_GRANTED: AtomicU64 = AtomicU64::new(0);
static LOCKS_DENIED: AtomicU64 = AtomicU64::new(0);
static PRESENCE_RELAYED: AtomicU64 = AtomicU64::new(0);
static PRESENCE_DROPPED: AtomicU64 = AtomicU64::new(0);
static DROPS: [AtomicU64; 5] = [const { AtomicU64::new(0) }; 5];
static ROOMS_LOADED: AtomicU64 = AtomicU64::new(0);
static ROOMS_UNLOADED: AtomicU64 = AtomicU64::new(0);
static CONNECTIONS_REFUSED: AtomicU64 = AtomicU64::new(0);
static JOURNAL_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static JOURNAL_FALLBACK_DOCUMENTS_FAILED: AtomicU64 = AtomicU64::new(0);
static PURGE_REMOVED: AtomicU64 = AtomicU64::new(0);
static PURGE_FAILURES: AtomicU64 = AtomicU64::new(0);
static ORG_QUEUE_REFUSALS: AtomicU64 = AtomicU64::new(0);
static DB_ACQUIRE_TIMEOUTS: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
static UPLOADS_REFUSED: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
static MAIL_SENT: AtomicU64 = AtomicU64::new(0);
static MAIL_FAILURES: AtomicU64 = AtomicU64::new(0);
static MAIL_REFUSED: AtomicU64 = AtomicU64::new(0);
static UPLOADS_CUT_SHORT: AtomicU64 = AtomicU64::new(0);
static LARGE_BODIES_REFUSED: AtomicU64 = AtomicU64::new(0);

/// Seconds, as the nanoseconds the histograms of durations count in.
const fn secs(s: f64) -> u64 {
    (s * 1e9) as u64
}

static DECIDE_SECONDS: Histogram = Histogram::new(
    &[
        secs(0.0001),
        secs(0.0005),
        secs(0.001),
        secs(0.005),
        secs(0.01),
        secs(0.05),
        secs(0.1),
        secs(0.5),
        secs(1.0),
        secs(5.0),
    ],
    1e9,
);
static JOURNAL_WAIT_SECONDS: Histogram = Histogram::new(
    &[
        secs(0.001),
        secs(0.005),
        secs(0.01),
        secs(0.025),
        secs(0.05),
        secs(0.1),
        secs(0.25),
        secs(0.5),
        secs(1.0),
        secs(5.0),
        secs(30.0),
    ],
    1e9,
);
static JOURNAL_BATCH_RECORDS: Histogram =
    Histogram::new(&[1, 2, 5, 10, 25, 50, 100, 250, 500], 1.0);
const FRAME_BOUNDS: &[u64] = &[
    64,
    256,
    1 << 10,
    4 << 10,
    16 << 10,
    64 << 10,
    256 << 10,
    1 << 20,
    8 << 20,
];
static FRAME_BYTES_IN: Histogram = Histogram::new(FRAME_BOUNDS, 1.0);
static FRAME_BYTES_OUT: Histogram = Histogram::new(FRAME_BOUNDS, 1.0);
const ACQUIRE_BOUNDS: &[u64] = &[
    secs(0.001),
    secs(0.01),
    secs(0.1),
    secs(1.0),
    secs(5.0),
    secs(30.0),
];
static DB_ACQUIRE_SECONDS: [Histogram; 3] = [
    Histogram::new(ACQUIRE_BOUNDS, 1e9),
    Histogram::new(ACQUIRE_BOUNDS, 1e9),
    Histogram::new(ACQUIRE_BOUNDS, 1e9),
];

/// The most buckets a [`Histogram`] has (besides the one for everything above the last bound).
const MAX_BUCKETS: usize = 12;

/// A histogram with fixed upper bounds, counted in whole base units (nanoseconds, bytes, records).
struct Histogram {
    /// The buckets' upper bounds (inclusive), ascending, in base units.
    bounds: &'static [u64],
    /// How many base units make one unit of the output (1e9: counted in nanoseconds, told in
    /// seconds).
    per_unit: f64,
    /// How many observations were at most each bound and above the one before (not cumulative);
    /// the last used entry is for everything above the last bound.
    buckets: [AtomicU64; MAX_BUCKETS + 1],
    sum: AtomicU64,
}

/// What a [`Histogram`] held when it was read.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramReading {
    /// Each upper bound in output units, and how many observations were at most that
    /// (cumulative).
    pub buckets: Vec<(f64, u64)>,
    /// The sum of the observations, in output units.
    pub sum: f64,
    pub count: u64,
}

impl Histogram {
    const fn new(bounds: &'static [u64], per_unit: f64) -> Self {
        assert!(bounds.len() <= MAX_BUCKETS);
        Self {
            bounds,
            per_unit,
            buckets: [const { AtomicU64::new(0) }; MAX_BUCKETS + 1],
            sum: AtomicU64::new(0),
        }
    }

    fn observe(&self, value: u64) {
        let i = self.bounds.partition_point(|bound| *bound < value);
        self.buckets[i].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
    }

    fn read(&self) -> HistogramReading {
        let mut count = 0;
        let mut buckets = Vec::with_capacity(self.bounds.len());
        for (i, bound) in self.bounds.iter().enumerate() {
            count += self.buckets[i].load(Ordering::Relaxed);
            buckets.push((*bound as f64 / self.per_unit, count));
        }
        count += self.buckets[self.bounds.len()].load(Ordering::Relaxed);
        HistogramReading {
            buckets,
            sum: self.sum.load(Ordering::Relaxed) as f64 / self.per_unit,
            count,
        }
    }
}

/// The database connections a caller waits for, in the order [`Counters::db_acquire_timeouts`]
/// counts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbPool {
    /// For requests' transactions (on SQLite: the one writer, which the journal shares).
    Transactions = 0,
    /// For reads.
    Reads = 1,
    /// For the journal writer, leases and snapshot rows (PostgreSQL only).
    Journal = 2,
}

const DB_POOLS: [(DbPool, &str); 3] = [
    (DbPool::Transactions, "transactions"),
    (DbPool::Reads, "reads"),
    (DbPool::Journal, "journal"),
];

/// Which limit refused a blob upload, in the order [`Counters::uploads_refused`] counts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadLimit {
    /// The account's own (429).
    Account = 0,
    /// The server's (503).
    Server = 1,
    /// The organisation's (429).
    Organisation = 2,
}

const DROP_CAUSES: [&str; 5] = ["account", "session", "expired", "access", "document"];

/// Why the server closed a document connection on its own (not counting slow consumers, takeovers
/// and shutdown), in the order [`Counters::drops`] counts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropCause {
    /// Every session of the account ended (password changed or reset, account disabled or
    /// deleted): 4401.
    Account = 0,
    /// The session the connection joined with ended (signed out): 4401.
    Session = 1,
    /// The session expired while connected: 4401.
    Expired = 2,
    /// The account's role in the project changed or went: 4403.
    Access = 3,
    /// The document (or its project) was deleted: 4404.
    Document = 4,
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// A change was accepted (made durable and acknowledged).
pub fn change_accepted() {
    ACCEPTED.fetch_add(1, Ordering::Relaxed);
}

/// A change was rejected with `code`.
pub fn change_rejected(code: RejectCode) {
    if let Some(i) = REJECT_CODES.iter().position(|c| *c == code) {
        REJECTED[i].fetch_add(1, Ordering::Relaxed);
    }
}

/// One `decide` took `took`.
pub fn decided(took: Duration) {
    DECIDES.fetch_add(1, Ordering::Relaxed);
    DECIDE_NANOS.fetch_add(nanos(took), Ordering::Relaxed);
    DECIDE_SECONDS.observe(nanos(took));
}

/// A record waited `took` between being handed to the journal and its outcome.
pub fn journal_waited(took: Duration) {
    JOURNAL_WAITS.fetch_add(1, Ordering::Relaxed);
    JOURNAL_WAIT_NANOS.fetch_add(nanos(took), Ordering::Relaxed);
    JOURNAL_WAIT_SECONDS.observe(nanos(took));
}

/// A client sent a frame of `bytes` bytes on a document connection.
pub fn frame_in(bytes: usize) {
    FRAME_BYTES_IN.observe(bytes as u64);
}

/// The server sent a frame of `bytes` bytes on a document connection.
pub fn frame_out(bytes: usize) {
    FRAME_BYTES_OUT.observe(bytes as u64);
}

/// A journal batch could not have a lock in time and was written again one document at a time.
pub fn journal_fallback() {
    JOURNAL_FALLBACKS.fetch_add(1, Ordering::Relaxed);
}

/// In such a batch, one document's records could not be written (its room reloads).
pub fn journal_fallback_document_failed() {
    JOURNAL_FALLBACK_DOCUMENTS_FAILED.fetch_add(1, Ordering::Relaxed);
}

/// The purger removed a deleted document, project or organisation.
pub fn purge_removed() {
    PURGE_REMOVED.fetch_add(1, Ordering::Relaxed);
}

/// The purger could not remove something (it tries again later), or could not list what is left.
pub fn purge_failed() {
    PURGE_FAILURES.fetch_add(1, Ordering::Relaxed);
}

/// A request was answered 503 instead of waiting (any longer) for its organisation's turn.
pub fn org_queue_refused() {
    ORG_QUEUE_REFUSALS.fetch_add(1, Ordering::Relaxed);
}

/// A caller waited `took` for a database connection of `pool` and got one.
pub fn db_acquired(pool: DbPool, took: Duration) {
    DB_ACQUIRE_SECONDS[pool as usize].observe(nanos(took));
}

/// A caller waited for a database connection of `pool` until the wait ran out.
pub fn db_acquire_timed_out(pool: DbPool) {
    DB_ACQUIRE_TIMEOUTS[pool as usize].fetch_add(1, Ordering::Relaxed);
}

/// Runs `acquiring` (getting a connection of `pool`, or a transaction on one) and counts how
/// long it took, or that the wait ran out.
pub async fn acquiring<T>(
    pool: DbPool,
    acquiring: impl Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, sqlx::Error> {
    let started = std::time::Instant::now();
    let outcome = acquiring.await;
    match &outcome {
        Ok(_) => db_acquired(pool, started.elapsed()),
        Err(sqlx::Error::PoolTimedOut) => db_acquire_timed_out(pool),
        Err(_) => {}
    }
    outcome
}

/// A message was handed to the mail server.
pub fn mail_sent() {
    MAIL_SENT.fetch_add(1, Ordering::Relaxed);
}

/// A message could not be handed to the mail server.
pub fn mail_failed() {
    MAIL_FAILURES.fetch_add(1, Ordering::Relaxed);
}

/// A message was not sent because as many were being sent as the server sends at once.
pub fn mail_refused() {
    MAIL_REFUSED.fetch_add(1, Ordering::Relaxed);
}

/// A blob upload was refused because as many were running as `limit` allows.
pub fn upload_refused(limit: UploadLimit) {
    UPLOADS_REFUSED[limit as usize].fetch_add(1, Ordering::Relaxed);
}

/// A blob upload was given up on because it was not done in the time it is allowed.
pub fn upload_cut_short() {
    UPLOADS_CUT_SHORT.fetch_add(1, Ordering::Relaxed);
}

/// A request with a large body was refused because as many were being read as the server reads
/// at once.
pub fn large_body_refused() {
    LARGE_BODIES_REFUSED.fetch_add(1, Ordering::Relaxed);
}

/// One frame was broadcast to `recipients` connections.
pub fn broadcast(recipients: usize) {
    BROADCASTS.fetch_add(1, Ordering::Relaxed);
    BROADCAST_RECIPIENTS.fetch_add(recipients as u64, Ordering::Relaxed);
}

/// The journal wrote one batch (one transaction) of `records` records.
pub fn journal_batch(records: usize) {
    JOURNAL_BATCHES.fetch_add(1, Ordering::Relaxed);
    JOURNAL_RECORDS.fetch_add(records as u64, Ordering::Relaxed);
    JOURNAL_BATCH_RECORDS.observe(records as u64);
}

/// A connection was closed because it did not read fast enough.
pub fn slow_consumer() {
    SLOW_CONSUMERS.fetch_add(1, Ordering::Relaxed);
}

/// A `lock.acquire` newly granted `granted` paths and denied `denied`.
pub fn locks(granted: usize, denied: usize) {
    LOCKS_GRANTED.fetch_add(granted as u64, Ordering::Relaxed);
    LOCKS_DENIED.fetch_add(denied as u64, Ordering::Relaxed);
}

/// A presence or draft message was relayed.
pub fn presence_relayed() {
    PRESENCE_RELAYED.fetch_add(1, Ordering::Relaxed);
}

/// A presence or draft message was dropped (over the rate, from a viewer, or refused by the codec).
pub fn presence_dropped() {
    PRESENCE_DROPPED.fetch_add(1, Ordering::Relaxed);
}

/// The server closed `n` connections for `cause`.
pub fn dropped(cause: DropCause, n: usize) {
    DROPS[cause as usize].fetch_add(n as u64, Ordering::Relaxed);
}

/// A room was loaded.
pub fn room_loaded() {
    ROOMS_LOADED.fetch_add(1, Ordering::Relaxed);
}

/// A room was unloaded (idle, closed, lost or stopped).
pub fn room_unloaded() {
    ROOMS_UNLOADED.fetch_add(1, Ordering::Relaxed);
}

/// A connection was refused because its account had as many open as it may.
pub fn connection_refused() {
    CONNECTIONS_REFUSED.fetch_add(1, Ordering::Relaxed);
}

/// The counters as they are now.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Counters {
    pub accepted: u64,
    /// By code, in the order of [`REJECT_CODES`].
    pub rejected: [u64; 10],
    pub decides: u64,
    pub decide_nanos: u64,
    pub journal_waits: u64,
    pub journal_wait_nanos: u64,
    pub broadcasts: u64,
    pub broadcast_recipients: u64,
    pub slow_consumers: u64,
    /// Journal transactions (batches) written, and the records in them.
    pub journal_batches: u64,
    pub journal_records: u64,
    pub locks_granted: u64,
    pub locks_denied: u64,
    pub presence_relayed: u64,
    pub presence_dropped: u64,
    /// Connections the server closed, by [`DropCause`] (account, session, expired, access,
    /// document).
    pub drops: [u64; 5],
    pub rooms_loaded: u64,
    pub rooms_unloaded: u64,
    /// Connections refused at the handshake for the per-account limit.
    pub connections_refused: u64,
    /// Journal batches written again one document at a time, and the documents whose records
    /// could not be written then.
    pub journal_fallbacks: u64,
    pub journal_fallback_documents_failed: u64,
    /// Deleted things the purger removed, and removals (or listings) that failed.
    pub purge_removed: u64,
    pub purge_failures: u64,
    /// Requests answered 503 for their organisation's queue.
    pub org_queue_refusals: u64,
    /// Waits for a database connection that ran out, by [`DbPool`].
    pub db_acquire_timeouts: [u64; 3],
    /// Blob uploads refused, by [`UploadLimit`].
    pub uploads_refused: [u64; 3],
    /// Messages handed to the mail server, and messages it did not take.
    pub mail_sent: u64,
    pub mail_failures: u64,
    /// Messages not sent because as many were on their way as the server sends at once.
    pub mail_refused: u64,
    /// Blob uploads given up on for taking longer than they are allowed.
    pub uploads_cut_short: u64,
    /// Requests with large bodies refused because as many were being read as the server reads.
    pub large_bodies_refused: u64,
}

/// Reads every counter.
pub fn counters() -> Counters {
    let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
    Counters {
        accepted: get(&ACCEPTED),
        rejected: std::array::from_fn(|i| get(&REJECTED[i])),
        decides: get(&DECIDES),
        decide_nanos: get(&DECIDE_NANOS),
        journal_waits: get(&JOURNAL_WAITS),
        journal_wait_nanos: get(&JOURNAL_WAIT_NANOS),
        broadcasts: get(&BROADCASTS),
        broadcast_recipients: get(&BROADCAST_RECIPIENTS),
        slow_consumers: get(&SLOW_CONSUMERS),
        journal_batches: get(&JOURNAL_BATCHES),
        journal_records: get(&JOURNAL_RECORDS),
        locks_granted: get(&LOCKS_GRANTED),
        locks_denied: get(&LOCKS_DENIED),
        presence_relayed: get(&PRESENCE_RELAYED),
        presence_dropped: get(&PRESENCE_DROPPED),
        drops: std::array::from_fn(|i| get(&DROPS[i])),
        rooms_loaded: get(&ROOMS_LOADED),
        rooms_unloaded: get(&ROOMS_UNLOADED),
        connections_refused: get(&CONNECTIONS_REFUSED),
        journal_fallbacks: get(&JOURNAL_FALLBACKS),
        journal_fallback_documents_failed: get(&JOURNAL_FALLBACK_DOCUMENTS_FAILED),
        purge_removed: get(&PURGE_REMOVED),
        purge_failures: get(&PURGE_FAILURES),
        org_queue_refusals: get(&ORG_QUEUE_REFUSALS),
        db_acquire_timeouts: std::array::from_fn(|i| get(&DB_ACQUIRE_TIMEOUTS[i])),
        uploads_refused: std::array::from_fn(|i| get(&UPLOADS_REFUSED[i])),
        mail_sent: get(&MAIL_SENT),
        mail_failures: get(&MAIL_FAILURES),
        mail_refused: get(&MAIL_REFUSED),
        uploads_cut_short: get(&UPLOADS_CUT_SHORT),
        large_bodies_refused: get(&LARGE_BODIES_REFUSED),
    }
}

/// What the serving loop ([`crate::serve`]) counts, per server ([`AppState::http`]).
#[derive(Debug, Default)]
pub struct HttpStats {
    open: AtomicUsize,
    handling: AtomicUsize,
    refused: AtomicU64,
    refused_for_address: AtomicU64,
    evicted: AtomicU64,
    request_timeouts: AtomicU64,
    write_timeouts: AtomicU64,
}

/// One request in its handler in [`HttpStats`]; it is counted until this is dropped.
#[derive(Debug)]
pub struct InHandler(std::sync::Arc<HttpStats>);

impl Drop for InHandler {
    fn drop(&mut self) {
        self.0.handling.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One open connection in [`HttpStats`]; it is counted until this is dropped.
#[derive(Debug)]
pub struct OpenConnection(std::sync::Arc<HttpStats>);

impl Drop for OpenConnection {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::SeqCst);
    }
}

impl HttpStats {
    /// Counts a connection as open until what this answers is dropped.
    pub fn opened(self: &std::sync::Arc<Self>) -> OpenConnection {
        self.open.fetch_add(1, Ordering::SeqCst);
        OpenConnection(self.clone())
    }

    /// How many connections (HTTP and document connections alike) are open.
    pub fn open(&self) -> usize {
        self.open.load(Ordering::SeqCst)
    }

    /// Counts a request as in its handler until what this answers is dropped.
    pub fn handling(self: &std::sync::Arc<Self>) -> InHandler {
        self.handling.fetch_add(1, Ordering::SeqCst);
        InHandler(self.clone())
    }

    /// How many requests are in their handlers (an answer being sent is not).
    pub fn in_handlers(&self) -> usize {
        self.handling.load(Ordering::SeqCst)
    }

    /// A connection was closed as soon as it was accepted: the server had `max_connections`, and
    /// its reserve, and none could make room.
    pub fn refused(&self) {
        self.refused.fetch_add(1, Ordering::Relaxed);
    }

    /// How many connections were closed as soon as they were accepted for the server being full.
    pub fn refusals(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// A connection was closed as soon as it was accepted: its address had
    /// `max_connections_per_address`.
    pub fn refused_for_address(&self) {
        self.refused_for_address.fetch_add(1, Ordering::Relaxed);
    }

    /// How many connections were closed as soon as they were accepted for their address.
    pub fn refusals_for_address(&self) -> u64 {
        self.refused_for_address.load(Ordering::Relaxed)
    }

    /// A connection was closed to make room for a new one.
    pub fn evicted(&self) {
        self.evicted.fetch_add(1, Ordering::Relaxed);
    }

    /// How many connections were closed to make room for new ones.
    pub fn evictions(&self) -> u64 {
        self.evicted.load(Ordering::Relaxed)
    }

    /// A request took longer than `request_timeout` and was answered 503.
    pub fn request_timed_out(&self) {
        self.request_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// How many requests were answered 503 for taking too long.
    pub fn request_timeouts(&self) -> u64 {
        self.request_timeouts.load(Ordering::Relaxed)
    }

    /// A connection was closed because its client took nothing for `write_timeout`.
    pub fn write_timed_out(&self) {
        self.write_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// How many connections were closed because their client took nothing.
    pub fn write_timeouts(&self) -> u64 {
        self.write_timeouts.load(Ordering::Relaxed)
    }
}

// ------------------------------------------------------------------ the Prometheus text

/// The Prometheus text being written.
struct Text(String);

impl Text {
    fn head(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP oswx_collab_{name} {help}");
        let _ = writeln!(self.0, "# TYPE oswx_collab_{name} {kind}");
    }

    fn sample(&mut self, name: &str, labels: &str, value: impl std::fmt::Display) {
        let _ = writeln!(self.0, "oswx_collab_{name}{labels} {value}");
    }

    fn counter(&mut self, name: &str, help: &str, value: u64) {
        self.head(name, "counter", help);
        self.sample(name, "", value);
    }

    fn gauge(&mut self, name: &str, help: &str, value: usize) {
        self.head(name, "gauge", help);
        self.sample(name, "", value);
    }

    /// One counter for each value of the label `label`.
    fn counters<'a>(
        &mut self,
        name: &str,
        help: &str,
        label: &str,
        values: impl IntoIterator<Item = (&'a str, u64)>,
    ) {
        self.head(name, "counter", help);
        for (value_of_label, n) in values {
            self.sample(name, &format!("{{{label}=\"{value_of_label}\"}}"), n);
        }
    }

    /// One histogram for each `(labels, reading)`; `labels` is empty or `key="value"`.
    fn histograms(&mut self, name: &str, help: &str, series: &[(&str, HistogramReading)]) {
        self.head(name, "histogram", help);
        for (labels, reading) in series {
            let (with, alone) = if labels.is_empty() {
                (String::new(), String::new())
            } else {
                (format!("{labels},"), format!("{{{labels}}}"))
            };
            for (bound, count) in &reading.buckets {
                let labels = format!("{{{with}le=\"{bound}\"}}");
                self.sample(&format!("{name}_bucket"), &labels, count);
            }
            let labels = format!("{{{with}le=\"+Inf\"}}");
            self.sample(&format!("{name}_bucket"), &labels, reading.count);
            self.sample(&format!("{name}_sum"), &alone, reading.sum);
            self.sample(&format!("{name}_count"), &alone, reading.count);
        }
    }
}

/// Everything the server counts, in the Prometheus text exposition format (version 0.0.4). Every
/// name starts with `oswx_collab_`; see the module documentation for what a label can be.
pub fn render(state: &AppState) -> String {
    let c = counters();
    let hub = state.hub.gauges();
    let mut t = Text(String::with_capacity(8 << 10));
    t.head("build_info", "gauge", "The server's version (always 1).");
    t.sample(
        "build_info",
        &format!("{{version=\"{}\"}}", crate::VERSION),
        1,
    );

    t.gauge("rooms", "Documents open in memory.", hub.rooms);
    t.gauge("connections", "Open document connections.", hub.connections);
    t.counter("rooms_loaded_total", "Documents opened.", c.rooms_loaded);
    t.counter(
        "rooms_unloaded_total",
        "Documents closed.",
        c.rooms_unloaded,
    );
    t.counter(
        "connections_refused_total",
        "Document connections refused because their account had as many open as it may.",
        c.connections_refused,
    );
    t.counters(
        "connections_closed_total",
        "Document connections the server closed because access ended, by cause.",
        "cause",
        DROP_CAUSES.iter().copied().zip(c.drops),
    );
    t.counter(
        "slow_consumer_disconnects_total",
        "Document connections closed because their client did not read fast enough.",
        c.slow_consumers,
    );

    t.counter(
        "changes_accepted_total",
        "Changes made durable and acknowledged.",
        c.accepted,
    );
    t.counters(
        "changes_rejected_total",
        "Changes refused, by reject code.",
        "code",
        REJECT_CODES
            .iter()
            .map(|code| code.as_str())
            .zip(c.rejected),
    );
    t.histograms(
        "decide_seconds",
        "How long deciding whether a change is accepted took.",
        &[("", DECIDE_SECONDS.read())],
    );
    t.histograms(
        "journal_batch_records",
        "Records in each journal batch (one transaction).",
        &[("", JOURNAL_BATCH_RECORDS.read())],
    );
    t.histograms(
        "journal_wait_seconds",
        "How long a record took from being handed to the journal to its outcome.",
        &[("", JOURNAL_WAIT_SECONDS.read())],
    );
    t.counter(
        "journal_fallbacks_total",
        "Journal batches written again one document at a time after waiting too long for a lock.",
        c.journal_fallbacks,
    );
    t.counter(
        "journal_fallback_documents_failed_total",
        "Documents whose records could not be written in such a batch.",
        c.journal_fallback_documents_failed,
    );
    t.histograms(
        "frame_bytes",
        "Sizes of the frames on document connections, by direction.",
        &[
            ("direction=\"in\"", FRAME_BYTES_IN.read()),
            ("direction=\"out\"", FRAME_BYTES_OUT.read()),
        ],
    );
    t.counter("broadcasts_total", "Frames broadcast.", c.broadcasts);
    t.counter(
        "broadcast_recipients_total",
        "Connections those broadcasts went to.",
        c.broadcast_recipients,
    );
    t.counter("locks_granted_total", "Locks granted.", c.locks_granted);
    t.counter("locks_denied_total", "Locks denied.", c.locks_denied);
    t.counter(
        "presence_relayed_total",
        "Presence and draft messages relayed.",
        c.presence_relayed,
    );
    t.counter(
        "presence_dropped_total",
        "Presence and draft messages dropped.",
        c.presence_dropped,
    );

    t.gauge(
        "purge_running",
        "Whether the purger has, or may have, something left to remove (1) or not (0).",
        usize::from(state.purger.busy()),
    );
    t.gauge(
        "purge_waiting",
        "Deleted things whose removal failed and is tried again later.",
        state.purger.waiting(),
    );
    t.counter(
        "purge_removed_total",
        "Deleted documents, projects and organisations removed for good.",
        c.purge_removed,
    );
    t.counter(
        "purge_failures_total",
        "Removals of deleted things that failed.",
        c.purge_failures,
    );

    t.gauge(
        "org_queue_waiting",
        "Requests waiting for their organisation's turn.",
        state.org_queue.waiters(),
    );
    t.counter(
        "org_queue_refusals_total",
        "Requests answered 503 instead of waiting (any longer) for their organisation's turn.",
        c.org_queue_refusals,
    );
    let acquired: Vec<(String, HistogramReading)> = DB_POOLS
        .iter()
        .map(|(pool, name)| {
            (
                format!("pool=\"{name}\""),
                DB_ACQUIRE_SECONDS[*pool as usize].read(),
            )
        })
        .collect();
    let acquired: Vec<(&str, HistogramReading)> = acquired
        .iter()
        .map(|(labels, reading)| (labels.as_str(), reading.clone()))
        .collect();
    t.histograms(
        "db_acquire_seconds",
        "How long callers waited for a database connection, by pool.",
        &acquired,
    );
    t.counters(
        "db_acquire_timeouts_total",
        "Waits for a database connection that ran out, by pool.",
        "pool",
        DB_POOLS
            .iter()
            .map(|(pool, name)| (*name, c.db_acquire_timeouts[*pool as usize])),
    );

    t.gauge(
        "exports_running",
        "Exports being downloaded.",
        state.exports.running(),
    );
    t.gauge(
        "blob_uploads_running",
        "Blob uploads being received.",
        state.uploads.running(),
    );
    t.counters(
        "blob_uploads_refused_total",
        "Blob uploads refused because too many were running, by the limit that refused.",
        "limit",
        [
            ("account", c.uploads_refused[UploadLimit::Account as usize]),
            (
                "organisation",
                c.uploads_refused[UploadLimit::Organisation as usize],
            ),
            ("server", c.uploads_refused[UploadLimit::Server as usize]),
        ],
    );
    t.gauge(
        "blob_uploads_queued",
        "Blob uploads waiting for a place on the server.",
        state.uploads.queued(),
    );
    t.gauge(
        "blob_uploads_memory_bytes",
        "The memory the running blob uploads are charged.",
        usize::try_from(state.uploads.memory_held()).unwrap_or(usize::MAX),
    );
    t.counter(
        "blob_uploads_cut_short_total",
        "Blob uploads given up on for taking longer than they are allowed.",
        c.uploads_cut_short,
    );
    t.counter(
        "large_bodies_refused_total",
        "Requests with bodies over 1 MiB refused because as many were being read as the server \
         reads at once.",
        c.large_bodies_refused,
    );
    t.counter(
        "mail_sent_total",
        "Messages handed to the mail server.",
        c.mail_sent,
    );
    t.counter(
        "mail_failures_total",
        "Messages the mail server did not take (or whose sending failed).",
        c.mail_failures,
    );
    t.counter(
        "mail_refused_total",
        "Messages not sent because as many were on their way as the server sends at once.",
        c.mail_refused,
    );
    t.gauge(
        "mail_sending",
        "Messages on their way to the mail server.",
        state.mail.sending(),
    );

    t.gauge(
        "http_connections",
        "Open connections to the server (document connections included).",
        state.http.open(),
    );
    t.gauge(
        "http_requests_in_handlers",
        "Requests whose answer has not begun.",
        state.http.in_handlers(),
    );
    t.counter(
        "http_connections_refused_total",
        "Connections closed as soon as they were accepted: the server had as many as it holds \
         and none could make room.",
        state.http.refusals(),
    );
    t.counter(
        "http_connections_refused_for_address_total",
        "Connections closed as soon as they were accepted: their address had as many as one \
         address may hold.",
        state.http.refusals_for_address(),
    );
    t.counter(
        "http_connections_evicted_total",
        "Connections closed to make room for new ones (no request finished, or idle longest).",
        state.http.evictions(),
    );
    t.counter(
        "http_request_timeouts_total",
        "Requests answered 503 because they took too long.",
        state.http.request_timeouts(),
    );
    t.counter(
        "http_write_timeouts_total",
        "Connections closed because their client stopped taking what was sent.",
        state.http.write_timeouts(),
    );
    t.0
}

/// The route's path.
pub const PATH: &str = "/metrics";

/// `GET /metrics`: 404 when no token is set; 401 without the token; else [`render`].
pub(crate) async fn metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let expected = state.settings.metrics_token.as_bytes();
    if expected.is_empty() {
        return Err(ApiError::NotFound("Not Found"));
    }
    // `Bearer`, one space, the token and nothing else: no second space, nothing after it.
    let given = headers
        .get(header::AUTHORIZATION)
        .map(HeaderValue::as_bytes)
        .and_then(|v| {
            let (scheme, token) = v.split_at_checked(7)?;
            scheme.eq_ignore_ascii_case(b"bearer ").then_some(token)
        })
        .unwrap_or_default();
    // (In constant time for tokens of the right length; the length itself is no secret.)
    if !bool::from(given.ct_eq(expected)) {
        return Err(ApiError::Unauthorized);
    }
    Ok((
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
            ),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
        ],
        render(&state),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_histogram_counts_each_observation_in_the_first_bucket_that_holds_it() {
        static H: Histogram = Histogram::new(&[10, 100, 1000], 10.0);
        for value in [0, 10, 11, 100, 1000, 1001, 50_000] {
            H.observe(value);
        }
        let r = H.read();
        assert_eq!(r.buckets, vec![(1.0, 2), (10.0, 4), (100.0, 5)]);
        assert_eq!(r.count, 7);
        assert!((r.sum - 5212.2).abs() < 1e-9, "{}", r.sum);
    }

    #[test]
    fn the_text_has_a_head_for_every_family_and_cumulative_buckets() {
        let mut t = Text(String::new());
        t.counters("things_total", "Things.", "kind", [("a", 1), ("b", 2)]);
        let reading = HistogramReading {
            buckets: vec![(0.5, 1), (2.0, 3)],
            sum: 4.25,
            count: 4,
        };
        t.histograms(
            "size",
            "Sizes.",
            &[("", reading.clone()), ("way=\"in\"", reading)],
        );
        assert_eq!(
            t.0,
            "# HELP oswx_collab_things_total Things.\n\
             # TYPE oswx_collab_things_total counter\n\
             oswx_collab_things_total{kind=\"a\"} 1\n\
             oswx_collab_things_total{kind=\"b\"} 2\n\
             # HELP oswx_collab_size Sizes.\n\
             # TYPE oswx_collab_size histogram\n\
             oswx_collab_size_bucket{le=\"0.5\"} 1\n\
             oswx_collab_size_bucket{le=\"2\"} 3\n\
             oswx_collab_size_bucket{le=\"+Inf\"} 4\n\
             oswx_collab_size_sum 4.25\n\
             oswx_collab_size_count 4\n\
             oswx_collab_size_bucket{way=\"in\",le=\"0.5\"} 1\n\
             oswx_collab_size_bucket{way=\"in\",le=\"2\"} 3\n\
             oswx_collab_size_bucket{way=\"in\",le=\"+Inf\"} 4\n\
             oswx_collab_size_sum{way=\"in\"} 4.25\n\
             oswx_collab_size_count{way=\"in\"} 4\n"
        );
    }
}
