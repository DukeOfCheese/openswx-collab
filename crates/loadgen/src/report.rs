//! Measurements and the JSON report.

use std::collections::BTreeMap;

use hdrhistogram::Histogram;
use serde::Serialize;

/// Counters and latencies of one run (or of one editor, before merging).
pub struct Metrics {
    latency_us: Histogram<u64>,
    schedule_us: Histogram<u64>,
    /// `open` only: the first sample, kept out of the histogram (cold: the server loads the document).
    pub first_open: Option<std::time::Duration>,
    /// Smallest and largest `welcome` frame seen, in bytes.
    pub welcome_bytes: Option<(u64, u64)>,
    /// Edits sent. Always `accepted + rejected + timeouts + errors`.
    pub sent: u64,
    pub accepted: u64,
    pub rejected: BTreeMap<String, u64>,
    pub timeouts: u64,
    /// Edits lost because the connection dropped before the answer.
    pub errors: u64,
    /// Frames that were not valid messages.
    pub protocol_errors: u64,
    pub dropped: u64,
    pub connect_failed: u64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            latency_us: new_histogram(),
            schedule_us: new_histogram(),
            first_open: None,
            welcome_bytes: None,
            sent: 0,
            accepted: 0,
            rejected: BTreeMap::new(),
            timeouts: 0,
            errors: 0,
            protocol_errors: 0,
            dropped: 0,
            connect_failed: 0,
        }
    }
}

fn new_histogram() -> Histogram<u64> {
    // 1 microsecond to 10 minutes, three significant digits.
    Histogram::new_with_bounds(1, 600_000_000, 3).expect("valid histogram bounds")
}

fn record(h: &mut Histogram<u64>, d: std::time::Duration) {
    h.saturating_record(u64::try_from(d.as_micros()).unwrap_or(u64::MAX).max(1));
}

fn summarize(h: &Histogram<u64>) -> Option<Latency> {
    if h.is_empty() {
        return None;
    }
    let ms = |us: u64| us as f64 / 1000.0;
    Some(Latency {
        count: h.len(),
        p50: ms(h.value_at_quantile(0.50)),
        p90: ms(h.value_at_quantile(0.90)),
        p99: ms(h.value_at_quantile(0.99)),
        max: ms(h.max()),
    })
}

/// A timed-out edit enters both histograms with this value, so a stall cannot hide the worst samples.
pub const TIMEOUT_SAMPLE: std::time::Duration = std::time::Duration::from_secs(30);

pub const LATENCY_NOTE: &str = "latency_ms is service time: from just before an edit is sent until its answer arrives. latency_from_schedule_ms is measured from the slot the edit was scheduled for (a fixed interval per editor), so a stalled server also shows as waiting for the edits queued behind the stall; no slot is skipped. An edit with no answer after 30 s counts as a timeout and enters both as 30000 ms. For `open`, first_open_ms is the first, cold sample and latency_ms covers the remaining ones.";

impl Metrics {
    /// Service time of one answered edit (or one open).
    pub fn record_latency(&mut self, d: std::time::Duration) {
        record(&mut self.latency_us, d);
    }

    /// Time from the edit's scheduled slot to its answer.
    pub fn record_schedule(&mut self, d: std::time::Duration) {
        record(&mut self.schedule_us, d);
    }

    pub fn record_timeout(&mut self) {
        self.timeouts += 1;
        self.record_latency(TIMEOUT_SAMPLE);
        self.record_schedule(TIMEOUT_SAMPLE);
    }

    pub fn note_welcome(&mut self, bytes: u64) {
        self.welcome_bytes = Some(match self.welcome_bytes {
            Some((lo, hi)) => (lo.min(bytes), hi.max(bytes)),
            None => (bytes, bytes),
        });
    }

    pub fn merge(&mut self, other: Metrics) {
        // Same bounds on both sides, so adding cannot fail.
        let _ = self.latency_us.add(&other.latency_us);
        let _ = self.schedule_us.add(&other.schedule_us);
        if let Some((lo, hi)) = other.welcome_bytes {
            self.note_welcome(lo);
            self.note_welcome(hi);
        }
        self.first_open = self.first_open.or(other.first_open);
        self.sent += other.sent;
        self.accepted += other.accepted;
        for (code, n) in other.rejected {
            *self.rejected.entry(code).or_default() += n;
        }
        self.timeouts += other.timeouts;
        self.errors += other.errors;
        self.protocol_errors += other.protocol_errors;
        self.dropped += other.dropped;
        self.connect_failed += other.connect_failed;
    }

    pub fn rejected_total(&self) -> u64 {
        self.rejected.values().sum()
    }

    pub fn latency(&self) -> Option<Latency> {
        summarize(&self.latency_us)
    }

    pub fn latency_from_schedule(&self) -> Option<Latency> {
        summarize(&self.schedule_us)
    }
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Latency {
    pub count: u64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
}

#[derive(Debug, Serialize)]
pub struct Host {
    pub os: String,
    pub cpu: String,
    pub cpu_threads: usize,
    pub ram_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct WelcomeBytes {
    pub min: u64,
    pub max: u64,
}

#[derive(Debug, Serialize)]
pub struct Rss {
    pub before_connect: Option<u64>,
    pub after_connect: Option<u64>,
    pub end: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub scenario: String,
    pub params: serde_json::Value,
    pub dialect: String,
    pub server: String,
    pub database: Option<String>,
    pub loadgen_commit: Option<String>,
    pub started_at: String,
    pub note: Option<String>,
    pub host: Host,
    pub duration_s: f64,
    /// Size of each generated document (JSON bytes).
    pub document_bytes: usize,
    pub changes_sent: u64,
    pub accepted: u64,
    pub rejected: BTreeMap<String, u64>,
    pub timeouts: u64,
    /// Edits lost to a dropped connection. `changes_sent = accepted + rejected + timeouts + errors`.
    pub errors: u64,
    pub protocol_errors: u64,
    pub dropped_connections: u64,
    /// Connections closed under their peers, by WebSocket close code (`1006`: none was sent). `null` for
    /// `v1`, which does not record it.
    pub close_codes: Option<BTreeMap<String, u64>>,
    pub connect_failed: u64,
    /// What `latency_ms` measures: `edit` (send to `change.accepted`) or `open` (`hello` to `welcome`).
    pub latency_kind: String,
    pub latency_ms: Option<Latency>,
    pub latency_from_schedule_ms: Option<Latency>,
    pub latency_note: String,
    /// `open` only: the cold first sample.
    pub first_open_ms: Option<f64>,
    /// Smallest and largest `welcome` frame, bytes.
    pub welcome_bytes: Option<WelcomeBytes>,
    pub accepted_per_s: f64,
    pub server_rss_bytes: Option<Rss>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn percentiles_of_known_samples() {
        let mut m = Metrics::default();
        for ms in 1..=100u64 {
            m.record_latency(Duration::from_millis(ms));
        }
        let l = m.latency().unwrap();
        assert_eq!(l.count, 100);
        for (got, want) in [(l.p50, 50.0), (l.p90, 90.0), (l.p99, 99.0), (l.max, 100.0)] {
            assert!((got - want).abs() <= 0.2, "got {got}, want {want}");
        }
    }

    #[test]
    fn no_samples_no_latency() {
        assert_eq!(Metrics::default().latency(), None);
    }

    #[test]
    fn merge_adds_everything() {
        let mut a = Metrics::default();
        a.record_latency(Duration::from_millis(10));
        a.sent = 2;
        a.accepted = 1;
        a.rejected.insert("stale".into(), 1);
        let mut b = Metrics::default();
        b.record_latency(Duration::from_millis(30));
        b.sent = 3;
        b.accepted = 2;
        b.rejected.insert("stale".into(), 2);
        b.rejected.insert("locked".into(), 1);
        b.dropped = 1;
        a.merge(b);
        assert_eq!((a.sent, a.accepted, a.dropped), (5, 3, 1));
        assert_eq!(a.rejected["stale"], 3);
        assert_eq!(a.rejected["locked"], 1);
        let l = a.latency().unwrap();
        assert_eq!(l.count, 2);
        assert!((l.max - 30.0).abs() < 0.1);
    }

    #[test]
    fn timeouts_enter_both_histograms() {
        let mut m = Metrics::default();
        m.record_timeout();
        assert_eq!(m.timeouts, 1);
        for l in [m.latency().unwrap(), m.latency_from_schedule().unwrap()] {
            assert_eq!(l.count, 1);
            assert!((l.max - 30_000.0).abs() < 30.0, "max {}", l.max);
        }
    }

    #[test]
    fn welcome_sizes_keep_min_and_max() {
        let mut a = Metrics::default();
        a.note_welcome(500);
        let mut b = Metrics::default();
        b.note_welcome(100);
        b.note_welcome(900);
        a.merge(b);
        assert_eq!(a.welcome_bytes, Some((100, 900)));
    }

    #[test]
    fn huge_latency_is_clamped_not_a_panic() {
        let mut m = Metrics::default();
        m.record_latency(Duration::from_secs(100_000));
        assert!(m.latency().is_some());
    }
}
