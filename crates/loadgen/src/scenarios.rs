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
            Ok(p) => peers.push((n, p)),
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

async fn editor(
    mut peer: Box<dyn Peer>,
    feature: usize,
    period: Duration,
    first: Instant,
    deadline: Instant,
) -> Metrics {
    let mut m = Metrics::default();
    let mut next = first;
    loop {
        tokio::time::sleep_until(next).await;
        if Instant::now() >= deadline {
            break;
        }
        if !peer.is_open() {
            m.dropped += 1;
            break;
        }
        m.sent += 1;
        let sent_at = Instant::now();
        match tokio::time::timeout(EDIT_TIMEOUT, peer.edit(feature)).await {
            Ok(Ok(Outcome::Accepted)) => {
                m.accepted += 1;
                m.record_latency(sent_at.elapsed());
            }
            Ok(Ok(Outcome::Rejected(code))) => *m.rejected.entry(code).or_default() += 1,
            Ok(Err(_)) => {
                m.dropped += 1;
                break;
            }
            Err(_) => m.timeouts += 1,
        }
        // A fixed schedule, but a slow answer does not cause a burst afterwards.
        next = (next + period).max(Instant::now());
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
                metrics.record_latency(peer.open_time());
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
