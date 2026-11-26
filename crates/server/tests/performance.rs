//! Work the server avoids, and waits it never makes the room do, without changing what anyone
//! receives. (Replaces `tests/test_performance.py` with structural assertions: a broadcast is
//! encoded once; the room is never held up by storage; blob checks run together and are remembered;
//! catch-up carries the live events. The queue accounting of `Conn` is unit-tested in
//! `room/conn.rs`.) The scale check at the end is `#[ignore]`d: run it by hand in a release build.

mod common;

use std::time::{Duration, Instant};

use common::*;
use oswx_core::wire::{ChangeEvent, RejectCode, ServerMsg};
use oswx_core::{Op, Seq};
use serde_json::json;
use sha2::{Digest, Sha256};

#[tokio::test]
async fn a_broadcast_is_encoded_once_for_everyone() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut others = Vec::new();
    for _ in 0..50 {
        others.push(Live::connect(&server, &doc, &world.admin).await);
    }
    a.drain().await;
    for o in &mut others {
        o.drain().await;
    }
    let before = server.room_status(&doc).await.encodes;
    assert_eq!(a.change(vec![set_depth(4)], vec![], vec![]).await.seq(), 1);
    for o in &mut others {
        let ev: ChangeEvent = o.expect().await;
        assert_eq!((ev.seq, ev.ops.clone()), (1, vec![set_depth(4)]));
    }
    let after = server.room_status(&doc).await.encodes;
    // One change event for the fifty, one acknowledgement for the sender.
    assert_eq!(after - before, 2);
}

/// Presence in a busy room goes out once per tick, each peer's latest only and many peers to a
/// frame, and nobody gets their own back. In a quiet room a presence goes out at once, as it came.
#[tokio::test]
async fn presence_is_relayed_as_each_peers_latest_many_to_a_frame() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut senders = Vec::new();
    for _ in 0..4 {
        senders.push(Live::connect(&server, &doc, &world.admin).await);
    }
    let mut watcher = Live::connect(&server, &doc, &world.admin).await;
    for s in &mut senders {
        s.drain().await;
    }
    watcher.drain().await;
    senders[0].presence(json!({"n": -1})).await;
    let first: Relayed = watcher.expect().await;
    assert_eq!(first.data, Some(plain(json!({"n": -1}))));
    assert_eq!(watcher.batches, 0, "alone, it goes as it came");
    let before = server.room_status(&doc).await.encodes;
    for n in 0..30 {
        for s in &mut senders {
            s.presence(json!({"n": n})).await;
        }
    }
    let last = Some(plain(json!({"n": 29})));
    let mut latest = std::collections::HashMap::new();
    let mut relayed = 0;
    while latest.len() < 4 || latest.values().any(|d| *d != last) {
        let r: Relayed = watcher.expect().await;
        relayed += 1;
        latest.insert(r.client_id, r.data);
    }
    assert!(relayed < 60, "{relayed} relayed of 120 sent");
    assert!(watcher.batches > 0, "several peers to a frame");
    // Encoded once per frame for everyone, and once more for each sender in it (without itself).
    let encodes = server.room_status(&doc).await.encodes - before;
    assert!(encodes < 30, "{encodes} frames encoded");
    tokio::time::sleep(Duration::from_millis(200)).await;
    for s in &mut senders {
        let own = s.client_id.clone();
        let seen = s.drain().await;
        assert!(
            seen.iter()
                .all(|m| !matches!(m, ServerMsg::Presence { client_id, .. }
                if **client_id == *own)),
            "{own} got its own presence back"
        );
        assert!(seen.iter().any(|m| matches!(m, ServerMsg::Presence { .. })));
    }
}

async fn upload(server: &TestServer, world: &World, body: &[u8]) -> String {
    let sha = hex::encode(Sha256::digest(body));
    let r = reqwest::Client::new()
        .put(server.url(&format!("/api/projects/{}/blobs/{sha}", world.project)))
        .bearer_auth(&world.admin)
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    sha
}

fn blob_ref(key: &str, sha: &str) -> Op {
    set(
        &["features", "f2", "params", key],
        json!({"$blob": sha, "len": 1}),
    )
}

#[tokio::test]
async fn a_slow_blob_lookup_does_not_hold_up_other_changes() {
    let (server, blobs) = TestServer::start_blobs(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    blobs.exists_delay(Duration::from_secs(2));
    let slow = a.make_change(0, vec![blob_ref("brep", &"1".repeat(64))]);
    a.send_change(&slow).await;
    // Ada's next change waits behind her first (answers come in the order she sent them) ...
    let behind = a.make_change(0, vec![set(&["variables", "x"], json!(1))]);
    a.send_change(&behind).await;
    // ... but nobody else waits.
    let started = Instant::now();
    for n in 0..5 {
        let r = b
            .change(
                vec![set(&["variables", &format!("b{n}")], json!(n))],
                vec![],
                vec![],
            )
            .await;
        assert_eq!(r.seq(), n + 1);
    }
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        a.outcome(&slow.change_id).await.code(),
        RejectCode::MissingBlob
    );
    assert!(started.elapsed() >= Duration::from_millis(1500));
    assert_eq!(a.outcome(&behind.change_id).await.seq(), 6);
}

#[tokio::test]
async fn a_slow_snapshot_write_does_not_hold_up_changes() {
    let (server, blobs) = TestServer::start_blobs(|s| s.snapshot_every = 2).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    blobs.put_delay(Duration::from_secs(2));
    let started = Instant::now();
    for n in 0..8 {
        assert_eq!(
            a.change(vec![set_depth(n)], vec![], vec![]).await.seq(),
            n + 1
        );
    }
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    let status = server.room_status(&doc).await;
    assert_eq!(
        status.snapshot_seq, 0,
        "the snapshot at 2 is still being written"
    );
    let deadline = tokio::time::Instant::now() + WAIT;
    while server.room_status(&doc).await.snapshot_seq < 2 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn blob_checks_run_together_and_are_remembered() {
    let (server, blobs) = TestServer::start_blobs(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let mut shas = Vec::new();
    for n in 0..5 {
        shas.push(upload(&server, &world, format!("body {n}").repeat(100).as_bytes()).await);
    }
    shas.push(hex::encode(Sha256::digest(b"never uploaded")));
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    blobs.exists_delay(Duration::from_millis(50));
    let using = |shas: &[String]| -> Vec<Op> {
        shas.iter()
            .enumerate()
            .map(|(n, sha)| blob_ref(&format!("b{n}"), sha))
            .collect()
    };
    let calls = blobs.exists_calls();
    let Outcome::Rejected { code, message, .. } = a.change_on(0, using(&shas)).await else {
        panic!("expected a refusal")
    };
    assert_eq!(code, RejectCode::MissingBlob);
    assert!(
        message.contains(&shas[5]) && !message.contains(&shas[0]),
        "{message}"
    );
    assert_eq!(blobs.exists_calls() - calls, 6);
    assert!(
        blobs.exists_most() > 1,
        "asked together, not one after another"
    );
    let calls = blobs.exists_calls();
    assert_eq!(a.change_on(0, using(&shas[..5])).await.seq(), 1);
    assert_eq!(
        blobs.exists_calls(),
        calls,
        "the five found were remembered"
    );
    // The missing one is remembered as missing for a while: asking again costs storage nothing.
    let again = a.change_on(1, using(&shas)).await;
    assert_eq!(again.code(), RejectCode::MissingBlob);
    assert_eq!(
        blobs.exists_calls(),
        calls,
        "the missing one was remembered"
    );
    blobs.fail_exists(true);
    let unseen = hex::encode(Sha256::digest(b"never seen"));
    assert_eq!(
        a.change_on(1, using(&[unseen])).await.code(),
        RejectCode::ServerError
    );
    assert_eq!(server.room_status(&doc).await.seq, 1);
}

#[tokio::test]
async fn catch_up_carries_the_same_change_events_as_live() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let (op, _) = a.add_feature("Fillet1");
    let mut change = a.make_change(0, vec![set_depth(8), op]);
    change.command = "edit_feature".into();
    assert_eq!(a.resend(&change).await.seq(), 1);
    assert_eq!(
        a.change(vec![remove(&["features"], "f1")], vec![], vec![])
            .await
            .seq(),
        2
    );
    let live: Vec<ChangeEvent> = b
        .drain()
        .await
        .into_iter()
        .filter_map(|m| match m {
            ServerMsg::Change(ev) => Some(ev),
            _ => None,
        })
        .collect();
    let late = Live::connect_with(&server, &doc, &world.admin, Hello::default().last_seq(0)).await;
    assert_eq!(late.welcome.changes.unwrap(), live);
    assert_eq!(live.len(), 2);
}

#[tokio::test]
async fn a_big_change_is_judged_off_the_room_task_and_lands_like_any_other() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let ops: Vec<Op> = (0..400)
        .map(|n| set(&["variables", &format!("v{n}")], json!(n)))
        .collect();
    let big = a.make_change(0, ops.clone());
    a.send_change(&big).await;
    // Another client's change is judged before or after it, never against half of it.
    let small = b.change(vec![set_depth(1)], vec![], vec![]).await;
    assert!(matches!(small, Outcome::Accepted(_)));
    let seq = a.outcome(&big.change_id).await.seq();
    b.drain().await;
    let ev = b
        .seen
        .iter()
        .find_map(|m| match m {
            ServerMsg::Change(ev) if ev.seq == seq => Some(ev.clone()),
            _ => None,
        })
        .expect("the big change reached the other client");
    assert_eq!(ev.ops, ops);
    assert_eq!(server.room_status(&doc).await.seq, 2);
}

/// 100 clients on one document, each sending 2 changes a second for 10 seconds, against SQLite.
/// Prints the median and p99 time from send to `change.accepted`. Not a benchmark and not a CI
/// gate: run it by hand, in release:
///
/// `cargo test --release -p oswx-server --test performance -- --ignored --nocapture scale`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn scale_check_one_hundred_editors() {
    const CLIENTS: usize = 100;
    const SECONDS: u64 = 10;
    // (Every client is the same account: above the default 64 connections per account.)
    let (server, flaky) = TestServer::start_flaky(|s| {
        s.max_connections_per_user = CLIENTS + 1;
        // (To compare runs on PostgreSQL: `OSWX_SCALE_JOURNAL_POOL=4`.)
        if let Some(pool) = std::env::var("OSWX_SCALE_JOURNAL_POOL")
            .ok()
            .and_then(|n| n.parse().ok())
        {
            s.database_journal_pool = pool;
        }
    })
    .await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    // What the run costs the database, for comparing runs: transactions (all of them) and journal
    // batches (process-wide counters: run this test alone).
    let (begins, journal) = (flaky.begins(), oswx_server::metrics::counters());
    let mut tasks = Vec::new();
    let server = std::sync::Arc::new(server);
    for i in 0..CLIENTS {
        let server = server.clone();
        let (doc, token) = (doc.clone(), world.admin.clone());
        tasks.push(tokio::spawn(async move {
            let mut live = Live::connect(&server, &doc, &token).await;
            let mut latencies = Vec::new();
            let mut rejected = Vec::new();
            let mut tick = tokio::time::interval(Duration::from_millis(500));
            let stop = Instant::now() + Duration::from_secs(SECONDS);
            let mut n = 0;
            while Instant::now() < stop {
                tick.tick().await;
                n += 1;
                // Every client edits its own value: no conflicts expected.
                let op = set(&["variables", &format!("c{i}")], json!(n));
                let started = Instant::now();
                match live.change(vec![op], vec![], vec![]).await {
                    Outcome::Accepted(_) => latencies.push(started.elapsed()),
                    Outcome::Rejected { code, .. } => rejected.push(code),
                }
                live.seen.clear();
            }
            (latencies, rejected)
        }));
    }
    let mut latencies = Vec::new();
    let mut rejected = Vec::new();
    for t in tasks {
        let (l, r) = t.await.unwrap();
        latencies.extend(l);
        rejected.extend(r);
    }
    let transactions = flaky.begins() - begins;
    let after = oswx_server::metrics::counters();
    let batches = after.journal_batches - journal.journal_batches;
    let records = after.journal_records - journal.journal_records;
    latencies.sort();
    let at = |q: f64| latencies[((latencies.len() as f64 - 1.0) * q) as usize];
    println!(
        "scale: {} changes accepted, {} rejected {:?}; median {:?}, p99 {:?}, max {:?}; {} store lookups; \
         {} journal batches of {:.1} records on average; {} transactions",
        latencies.len(),
        rejected.len(),
        rejected,
        at(0.5),
        at(0.99),
        latencies.last().unwrap(),
        flaky.change_lookups(),
        batches,
        records as f64 / batches.max(1) as f64,
        transactions
    );
    assert_eq!(
        flaky.change_lookups(),
        0,
        "the hot path asks the store nothing"
    );
    assert!(rejected.is_empty());
    let head: Seq = server.room_status(&doc).await.seq;
    assert_eq!(head as usize, latencies.len());
}
