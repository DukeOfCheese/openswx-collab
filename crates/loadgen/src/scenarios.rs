//! The four scenarios. Each connects its peers first, then runs for the requested time.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{StreamExt, stream};
use tokio::time::Instant;

use crate::dialect::{Dialect, Outcome, Peer, Target};
use crate::report::{Metrics, Rss};

/// A number in [0, 1) for start offsets; quality does not matter, spread does.
fn unit_random() -> f64 {
    (uuid::Uuid::new_v4().as_u128() >> 75) as f64 / (1u64 << 53) as f64
}

/// An edit unanswered for this long counts as a timeout; the run goes on.
pub const EDIT_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_PARALLELISM: usize = 32;
const PRESENCE_EVERY: Duration = Duration::from_secs(5);

pub struct RunResult {
    pub metrics: Metrics,
    pub elapsed: Duration,
    pub rss: Option<Rss>,
}

/// Opens `count` connections, a few at a time. Failures are counted, not fatal.
async fn connect_all(
    dialect: &Arc<dyn Dialect>,
    target: &Arc<Target>,
    count: usize,
    metrics: &mut Metrics,
) -> Vec<(usize, Box<dyn Peer>)> {
    let results: Vec<_> = stream::iter(0..count)
        .map(|n| {
            let (d, t) = (dialect.clone(), target.clone());
            async move { (n, d.connect(&t, n).await) }
        })
        .buffer_unordered(CONNECT_PARALLELISM)
        .collect()
        .await;
    let mut peers = Vec::with_capacity(count);
    for (n, r) in results {
        match r {
            Ok(p) => {
                metrics.note_welcome(p.welcome_bytes());
                peers.push((n, p));
            }
            Err(e) => {
                eprintln!("connection {n} failed: {e:#}");
                metrics.connect_failed += 1;
            }
        }
    }
    peers.sort_by_key(|(n, _)| *n);
    peers
}

/// `editors` connections spread over the target's documents, each editing its own feature `rate` times a second.
pub async fn edit_run(
    dialect: Arc<dyn Dialect>,
    target: Target,
    editors: usize,
    rate: f64,
    duration: Duration,
) -> RunResult {
    let target = Arc::new(target);
    let mut metrics = Metrics::default();
    let peers = connect_all(&dialect, &target, editors, &mut metrics).await;
    let docs = target.documents.len().max(1);
    let features = target.features.max(1);
    let period = Duration::from_secs_f64(1.0 / rate);

    let started = Instant::now();
    let deadline = started + duration;
    let tasks: Vec<_> = peers
        .into_iter()
        .map(|(n, peer)| {
            // Editors of one document take its features in turn, so each edits a feature of its own.
            let feature = (n / docs) % features;
            let offset = period.mul_f64(unit_random());
            tokio::spawn(editor(peer, feature, period, started + offset, deadline))
        })
        .collect();
    for t in tasks {
        match t.await {
            Ok(m) => metrics.merge(m),
            Err(e) => eprintln!("an editor task failed: {e}"),
        }
    }
    RunResult {
        metrics,
        elapsed: started.elapsed(),
        rss: None,
    }
}

/// One editor: edit slots at a fixed interval starting at `first`. A slot is never skipped: an editor that
/// is behind schedule sends at once, and the lateness shows in the from-schedule latency.
async fn editor(
    mut peer: Box<dyn Peer>,
    feature: usize,
    period: Duration,
    first: Instant,
    deadline: Instant,
) -> Metrics {
    let mut m = Metrics::default();
    m.note_welcome(peer.welcome_bytes());
    let mut slot = first;
    while slot < deadline {
        tokio::time::sleep_until(slot).await;
        if !peer.is_open() {
            m.dropped += 1;
            break;
        }
        m.sent += 1;
        let sent_at = Instant::now();
        match tokio::time::timeout(EDIT_TIMEOUT, peer.edit(feature)).await {
            Ok(Ok(Outcome::Accepted)) => {
                let done = Instant::now();
                m.accepted += 1;
                m.record_latency(done - sent_at);
                m.record_schedule(done - slot);
            }
            Ok(Ok(Outcome::Rejected(code))) => *m.rejected.entry(code).or_default() += 1,
            Ok(Err(_)) => {
                m.errors += 1;
                m.dropped += 1;
                break;
            }
            Err(_) => m.record_timeout(),
        }
        slot += period;
    }
    if !peer.is_open() && m.dropped == 0 {
        m.dropped += 1;
    }
    m.protocol_errors += peer.protocol_errors();
    m
}

/// Time from `hello` to `welcome`, `times` times, one connection after another.
pub async fn open_run(
    dialect: Arc<dyn Dialect>,
    target: Target,
    times: usize,
) -> anyhow::Result<RunResult> {
    let mut metrics = Metrics::default();
    let started = Instant::now();
    for n in 0..times {
        match dialect.connect(&target, n).await {
            Ok(peer) => {
                metrics.note_welcome(peer.welcome_bytes());
                if metrics.first_open.is_none() {
                    // Cold: the server may still be loading the document.
                    metrics.first_open = Some(peer.open_time());
                } else {
                    metrics.record_latency(peer.open_time());
                }
                metrics.protocol_errors += peer.protocol_errors();
            }
            Err(e) => {
                eprintln!("open {n} failed: {e:#}");
                metrics.connect_failed += 1;
            }
        }
    }
    if metrics.connect_failed as usize == times && times > 0 {
        anyhow::bail!("every connection failed");
    }
    Ok(RunResult {
        metrics,
        elapsed: started.elapsed(),
        rss: None,
    })
}

/// `connections` peers that only ping (the peer does it itself) and send presence.
pub async fn idle_run(
    dialect: Arc<dyn Dialect>,
    target: Target,
    connections: usize,
    duration: Duration,
    sample_rss: &dyn Fn() -> Option<u64>,
) -> RunResult {
    let target = Arc::new(target);
    let mut metrics = Metrics::default();
    let before_connect = sample_rss();
    let peers = connect_all(&dialect, &target, connections, &mut metrics).await;
    let after_connect = sample_rss();

    let started = Instant::now();
    let deadline = started + duration;
    let tasks: Vec<_> = peers
        .into_iter()
        .map(|(_, mut peer)| {
            let offset = PRESENCE_EVERY.mul_f64(unit_random());
            tokio::spawn(async move {
                let mut m = Metrics::default();
                let mut next = started + offset;
                while next < deadline {
                    tokio::time::sleep_until(next).await;
                    if peer.presence().await.is_err() || peer.drain().await.is_err() {
                        break;
                    }
                    next += PRESENCE_EVERY;
                }
                tokio::time::sleep_until(deadline).await;
                if !peer.is_open() {
                    m.dropped += 1;
                }
                m.protocol_errors += peer.protocol_errors();
                m
            })
        })
        .collect();
    for t in tasks {
        match t.await {
            Ok(m) => metrics.merge(m),
            Err(e) => eprintln!("a peer task failed: {e}"),
        }
    }
    let end = sample_rss();
    RunResult {
        metrics,
        elapsed: started.elapsed(),
        rss: Some(Rss {
            before_connect,
            after_connect,
            end,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use async_trait::async_trait;

    use super::*;

    enum Step {
        /// Answer after this many milliseconds.
        Answer(u64, Outcome),
        /// Never answer.
        Hang,
        /// The connection drops.
        Fail,
    }

    struct Fake(VecDeque<Step>);

    #[async_trait]
    impl Peer for Fake {
        async fn edit(&mut self, _feature: usize) -> anyhow::Result<Outcome> {
            match self.0.pop_front() {
                Some(Step::Answer(ms, o)) => {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(o)
                }
                Some(Step::Hang) => std::future::pending().await,
                Some(Step::Fail) | None => anyhow::bail!("closed"),
            }
        }
        async fn presence(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn drain(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
        fn open_time(&self) -> Duration {
            Duration::ZERO
        }
        fn welcome_bytes(&self) -> u64 {
            1000
        }
        fn is_open(&self) -> bool {
            true
        }
        fn protocol_errors(&self) -> u64 {
            0
        }
    }

    async fn run(steps: Vec<Step>, period_ms: u64, duration_ms: u64) -> Metrics {
        let now = Instant::now();
        editor(
            Box::new(Fake(steps.into())),
            0,
            Duration::from_millis(period_ms),
            now,
            now + Duration::from_millis(duration_ms),
        )
        .await
    }

    fn acc(ms: u64) -> Step {
        Step::Answer(ms, Outcome::Accepted)
    }

    #[tokio::test(start_paused = true)]
    async fn a_stall_shows_as_lateness_in_the_following_slots_and_skips_none() {
        // Slots at 0, 100, ..., 900. The first answer takes 1 s; the rest take 1 ms.
        let mut steps = vec![acc(1000)];
        steps.extend((0..9).map(|_| acc(1)));
        let m = run(steps, 100, 1000).await;
        assert_eq!(m.sent, 10, "no slot is skipped");
        assert_eq!(m.accepted, 10);
        let service = m.latency().unwrap();
        let sched = m.latency_from_schedule().unwrap();
        assert!(
            service.max < 1001.0 + 1.0 && service.p50 < 2.0,
            "{service:?}"
        );
        // The second slot (100 ms) is answered at 1001 ms: 901 ms from its schedule; the last slot (900 ms)
        // at 1009 ms: 109 ms from its schedule.
        assert!(sched.max >= 1000.0, "{sched:?}");
        assert!(sched.p50 > 100.0, "{sched:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_timeout_enters_both_histograms_and_is_counted() {
        let m = run(vec![Step::Hang], 1000, 500).await;
        assert_eq!((m.sent, m.timeouts, m.accepted), (1, 1, 0));
        for l in [m.latency().unwrap(), m.latency_from_schedule().unwrap()] {
            assert!((l.max - 30_000.0).abs() < 30.0, "{l:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn counters_reconcile() {
        let steps = vec![
            acc(1),
            Step::Answer(1, Outcome::Rejected("stale".into())),
            Step::Hang,
            acc(1),
            Step::Fail,
            acc(1),
        ];
        let m = run(steps, 100, 10_000).await;
        assert_eq!(
            (m.accepted, m.rejected_total(), m.timeouts, m.errors),
            (2, 1, 1, 1)
        );
        assert_eq!(
            m.sent,
            m.accepted + m.rejected_total() + m.timeouts + m.errors
        );
        assert_eq!(m.dropped, 1);
    }
}
