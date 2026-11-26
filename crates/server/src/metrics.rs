//! Counters the live-editing path keeps about itself, for `/metrics` (Task 18 exports them).
//!
//! Plain process-wide atomics, so that counting costs one relaxed add and the room's logic only calls
//! the functions below. No counter is keyed by a user, document or organisation. The gauges (open
//! rooms, open connections) are read from the hub ([`crate::hub::Hub::gauges`]).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use oswx_core::RejectCode;

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
}

/// A record waited `took` between being handed to the journal and its outcome.
pub fn journal_waited(took: Duration) {
    JOURNAL_WAITS.fetch_add(1, Ordering::Relaxed);
    JOURNAL_WAIT_NANOS.fetch_add(nanos(took), Ordering::Relaxed);
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
    }
}
