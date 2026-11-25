//! Counters the live-editing path keeps about itself, for `/metrics` (Task 18 exports them).
//!
//! Plain process-wide atomics, so that counting costs one relaxed add and the room's logic only calls
//! the functions below. No counter is keyed by a user, document or organisation.

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
static LOCKS_GRANTED: AtomicU64 = AtomicU64::new(0);
static LOCKS_DENIED: AtomicU64 = AtomicU64::new(0);
static PRESENCE_RELAYED: AtomicU64 = AtomicU64::new(0);
static PRESENCE_DROPPED: AtomicU64 = AtomicU64::new(0);

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
    pub locks_granted: u64,
    pub locks_denied: u64,
    pub presence_relayed: u64,
    pub presence_dropped: u64,
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
        locks_granted: get(&LOCKS_GRANTED),
        locks_denied: get(&LOCKS_DENIED),
        presence_relayed: get(&PRESENCE_RELAYED),
        presence_dropped: get(&PRESENCE_DROPPED),
    }
}
