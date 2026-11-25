//! Measurements and the JSON report.

use std::collections::BTreeMap;

use hdrhistogram::Histogram;
use serde::Serialize;

/// Counters and latencies of one run (or of one editor, before merging).
pub struct Metrics {
    latency_us: Histogram<u64>,
    pub sent: u64,
    pub accepted: u64,
    pub rejected: BTreeMap<String, u64>,
    pub timeouts: u64,
    pub protocol_errors: u64,
    pub dropped: u64,
    pub connect_failed: u64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            // 1 microsecond to 10 minutes, three significant digits.
            latency_us: Histogram::new_with_bounds(1, 600_000_000, 3)
                .expect("valid histogram bounds"),
            sent: 0,
            accepted: 0,
            rejected: BTreeMap::new(),
            timeouts: 0,
            protocol_errors: 0,
            dropped: 0,
            connect_failed: 0,
        }
    }
}

impl Metrics {
    pub fn record_latency(&mut self, d: std::time::Duration) {
        let us = u64::try_from(d.as_micros()).unwrap_or(u64::MAX).max(1);
        self.latency_us.saturating_record(us);
    }

    pub fn merge(&mut self, other: Metrics) {
        // Same bounds on both sides, so adding cannot fail.
        let _ = self.latency_us.add(&other.latency_us);
        self.sent += other.sent;
        self.accepted += other.accepted;
        for (code, n) in other.rejected {
            *self.rejected.entry(code).or_default() += n;
        }
        self.timeouts += other.timeouts;
        self.protocol_errors += other.protocol_errors;
        self.dropped += other.dropped;
        self.connect_failed += other.connect_failed;
    }

    pub fn latency(&self) -> Option<Latency> {
        if self.latency_us.is_empty() {
            return None;
        }
        let ms = |us: u64| us as f64 / 1000.0;
        Some(Latency {
            count: self.latency_us.len(),
            p50: ms(self.latency_us.value_at_quantile(0.50)),
            p90: ms(self.latency_us.value_at_quantile(0.90)),
            p99: ms(self.latency_us.value_at_quantile(0.99)),
            max: ms(self.latency_us.max()),
        })
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
    pub changes_sent: u64,
    pub accepted: u64,
    pub rejected: BTreeMap<String, u64>,
    pub timeouts: u64,
    pub protocol_errors: u64,
    pub dropped_connections: u64,
    pub connect_failed: u64,
    /// What `latency_ms` measures: `edit` (send to `change.accepted`) or `open` (`hello` to `welcome`).
    pub latency_kind: String,
    pub latency_ms: Option<Latency>,
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
    fn huge_latency_is_clamped_not_a_panic() {
        let mut m = Metrics::default();
        m.record_latency(Duration::from_secs(100_000));
        assert!(m.latency().is_some());
    }
}
