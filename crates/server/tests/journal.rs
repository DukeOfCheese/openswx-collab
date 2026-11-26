//! The journal writer, room leases, and loading a document back from its snapshot and journal.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{FlakyStore, TestServer, World, new_journal};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use oswx_core::{
    Accepted, Change, ClientId, Head, Known, Op, Path, Pos, Rules, Value, Write, decide,
};
use oswx_server::journal::{
    AssemblyRef, ChosenPart, Extra, JournalError, JournalWriter, LoadError, Record, RefusedWhy,
    SERVER_CLIENT, WriteSnapshotError, load_head, state_at, write_snapshot,
};
use oswx_server::lease::acquire;
use oswx_server::record::{encode_ops, encode_paths, encode_reads};
use oswx_server::store::Store;
use reqwest::StatusCode;

fn p(segs: &[&str]) -> Path {
    segs.iter().map(|s| Arc::from(*s)).collect()
}

async fn setup() -> (TestServer, Arc<FlakyStore>, World) {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.expect("the world is made");
    (server, flaky, world)
}

async fn new_doc(server: &TestServer, world: &World, name: &str) -> String {
    world.document(server, None, Some(name), None).await
}

fn store(server: &TestServer) -> &dyn Store {
    server.state.store.as_ref()
}

async fn lease(server: &TestServer, doc: &str) -> u64 {
    acquire(store(server), doc, &server.state.node_id, 1000.0, 60.0)
        .await
        .expect("the lease is taken")
}

/// A record setting the top-level key `n` to `seq`, by client `u.c` with change id `c<seq>`.
fn simple(doc: &str, seq: u64, epoch: u64) -> Record {
    let ops = vec![Op::Set {
        path: p(&["n"]),
        value: Value::Int(seq as i64),
    }];
    let writes = BTreeSet::from([Write::At(p(&["n"]))]);
    Record {
        document: doc.to_owned(),
        seq,
        parent_seq: seq - 1,
        epoch,
        author: "0123456789abcdef0123456789abcdef".into(),
        client_id: Some("u.c".into()),
        change_id: Some(format!("c{seq}")),
        command: "test".into(),
        ops: encode_ops(&ops),
        inverse: encode_ops(&[]),
        paths: encode_paths(&writes, &BTreeSet::new()),
        reads: encode_reads(&BTreeSet::new()),
        undo_of: None,
        reverts: None,
        created_at: 2000.0 + seq as f64,
        extra: Vec::new(),
    }
}

async fn head_seq(server: &TestServer, doc: &str) -> i64 {
    store(server)
        .document_by_id(doc)
        .await
        .unwrap()
        .unwrap()
        .head_seq
}

async fn row_count(server: &TestServer, doc: &str) -> usize {
    store(server)
        .changes_between(doc, 0, i64::MAX, 100_000)
        .await
        .unwrap()
        .len()
}

/// A writer that waits long enough for everything a test submits at once to share a batch.
fn slow_writer(server: &TestServer) -> JournalWriter {
    JournalWriter::start(server.state.store.clone(), 256, Duration::from_millis(200))
}

fn refused(why: RefusedWhy) -> Result<(), JournalError> {
    Err(JournalError::Refused(why))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_thousand_appends_from_fifty_tasks_land_in_order_in_few_transactions() {
    let (server, flaky, world) = setup().await;
    let writer = new_journal(&server.state.store);
    let mut docs = Vec::new();
    for i in 0..10 {
        let doc = new_doc(&server, &world, &format!("D{i}")).await;
        let epoch = lease(&server, &doc).await;
        // Per document: the next seq, taken and submitted under the lock (as a room orders them).
        docs.push(Arc::new((doc, epoch, tokio::sync::Mutex::new(1u64))));
    }
    let begins = flaky.begins();
    let mut tasks = Vec::new();
    for t in 0..50 {
        let d = docs[t % docs.len()].clone();
        let writer = writer.clone();
        tasks.push(tokio::spawn(async move {
            let mut mine = Vec::new();
            for _ in 0..20 {
                let mut next = d.2.lock().await;
                let seq = *next;
                *next += 1;
                mine.push((seq, writer.submit(simple(&d.0, seq, d.1)).await));
            }
            // This task's own results arrive in the order it submitted them (its seqs ascend):
            // FuturesUnordered yields each as soon as it is answered.
            let mut pending: FuturesUnordered<_> = mine
                .into_iter()
                .map(|(seq, receipt)| async move { (seq, receipt.outcome().await) })
                .collect();
            let mut arrived = Vec::new();
            while let Some((seq, outcome)) = pending.next().await {
                assert_eq!(outcome, Ok(()), "seq {seq}");
                arrived.push(seq);
            }
            let mut sorted = arrived.clone();
            sorted.sort_unstable();
            assert_eq!(arrived, sorted, "results arrive in seq order");
            arrived.len()
        }));
    }
    let mut total = 0;
    for t in tasks {
        total += t.await.unwrap();
    }
    assert_eq!(total, 1000);
    let transactions = flaky.begins() - begins;
    assert!(
        transactions < 1000 / 5,
        "{transactions} transactions for 1000 records"
    );
    for d in &docs {
        assert_eq!(head_seq(&server, &d.0).await, 100);
        let rows = store(&server)
            .changes_between(&d.0, 0, 100, 1000)
            .await
            .unwrap();
        let seqs: Vec<i64> = rows.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, (1..=100).collect::<Vec<_>>());
        for r in &rows {
            assert_eq!(r.change_id.as_deref(), Some(format!("c{}", r.seq).as_str()));
            assert_eq!(r.parent_seq, r.seq - 1);
            assert_eq!(r.epoch, d.1 as i64);
            assert_eq!(r.branch_id, "main");
        }
        let (head, _) = load_head(store(&server), server.state.blobs.as_ref(), &d.0)
            .await
            .unwrap();
        assert_eq!(head.seq, 100);
        assert_eq!(head.doc.at(&["n"]), Some(&Value::Int(100)));
    }
}

#[tokio::test]
async fn every_acquisition_bumps_the_epoch_and_the_old_epoch_is_refused() {
    let (server, _flaky, world) = setup().await;
    let (a, b) = (
        new_doc(&server, &world, "A").await,
        new_doc(&server, &world, "B").await,
    );
    assert_eq!(store(&server).room_lease(&a).await.unwrap(), None);
    assert_eq!(lease(&server, &a).await, 1);
    assert_eq!(lease(&server, &a).await, 2);
    let row = store(&server).room_lease(&a).await.unwrap().unwrap();
    assert_eq!(
        (row.epoch, row.node_id.as_str()),
        (2, &*server.state.node_id)
    );
    assert_eq!(row.expires_at, 1060.0);
    // Another node (a later run of the server) takes it too, bumping again.
    assert_eq!(
        acquire(store(&server), &a, "other", 1.0, 1.0)
            .await
            .unwrap(),
        3
    );
    let eb = lease(&server, &b).await;

    // A writer holding epoch 2 for A is refused; B in the same batch commits.
    let writer = slow_writer(&server);
    let ra = writer.submit(simple(&a, 1, 2)).await;
    let rb = writer.submit(simple(&b, 1, eb)).await;
    assert_eq!(ra.outcome().await, refused(RefusedWhy::Lease));
    assert_eq!(rb.outcome().await, Ok(()));
    assert_eq!(
        (head_seq(&server, &a).await, row_count(&server, &a).await),
        (0, 0)
    );
    assert_eq!(head_seq(&server, &b).await, 1);
    // With the current epoch, A's first change goes in.
    assert_eq!(writer.append(simple(&a, 1, 3)).await, Ok(()));
    assert_eq!(head_seq(&server, &a).await, 1);
}

#[tokio::test]
async fn a_document_never_acquired_takes_epoch_zero_records_only() {
    let (server, _flaky, world) = setup().await;
    let (a, b) = (
        new_doc(&server, &world, "A").await,
        new_doc(&server, &world, "B").await,
    );
    let writer = new_journal(&server.state.store);
    assert_eq!(writer.append(simple(&a, 1, 0)).await, Ok(()));
    let row = store(&server).room_lease(&a).await.unwrap().unwrap();
    assert_eq!(row.epoch, 0);
    // The first acquisition then moves it on, and epoch 0 is refused.
    assert_eq!(lease(&server, &a).await, 1);
    assert_eq!(
        writer.append(simple(&a, 2, 0)).await,
        refused(RefusedWhy::Lease)
    );
    // Without a row, any other epoch is refused.
    assert_eq!(
        writer.append(simple(&b, 1, 5)).await,
        refused(RefusedWhy::Lease)
    );
    assert_eq!(store(&server).room_lease(&b).await.unwrap(), None);
}

#[tokio::test]
async fn a_gap_or_wrong_parent_refuses_that_record_and_its_successors_only() {
    let (server, _flaky, world) = setup().await;
    let (a, b) = (
        new_doc(&server, &world, "A").await,
        new_doc(&server, &world, "B").await,
    );
    let (ea, eb) = (lease(&server, &a).await, lease(&server, &b).await);
    let writer = slow_writer(&server);
    let mut wrong_parent = simple(&a, 2, ea);
    wrong_parent.parent_seq = 0;
    let receipts = vec![
        writer.submit(simple(&a, 1, ea)).await,
        writer.submit(wrong_parent).await,
        writer.submit(simple(&a, 3, ea)).await,
        writer.submit(simple(&b, 1, eb)).await,
        writer.submit(simple(&b, 3, eb)).await, // a gap
        writer.submit(simple(&b, 4, eb)).await,
    ];
    let mut outcomes = Vec::new();
    for r in receipts {
        outcomes.push(r.outcome().await);
    }
    assert_eq!(
        outcomes,
        vec![
            Ok(()),
            refused(RefusedWhy::OutOfOrder),
            refused(RefusedWhy::OutOfOrder),
            Ok(()),
            refused(RefusedWhy::OutOfOrder),
            refused(RefusedWhy::OutOfOrder),
        ]
    );
    assert_eq!(head_seq(&server, &a).await, 1);
    assert_eq!(head_seq(&server, &b).await, 1);
    assert_eq!(row_count(&server, &a).await, 1);
    // A later batch cannot slip them in out of order either: seq 3 is still refused, 2 goes in.
    assert_eq!(
        writer.append(simple(&a, 3, ea)).await,
        refused(RefusedWhy::OutOfOrder)
    );
    assert_eq!(writer.append(simple(&a, 2, ea)).await, Ok(()));
}

#[tokio::test]
async fn a_duplicate_change_id_is_refused_without_failing_the_batch() {
    let (server, _flaky, world) = setup().await;
    let (a, b) = (
        new_doc(&server, &world, "A").await,
        new_doc(&server, &world, "B").await,
    );
    let (ea, eb) = (lease(&server, &a).await, lease(&server, &b).await);
    let writer = slow_writer(&server);
    assert_eq!(writer.append(simple(&a, 1, ea)).await, Ok(()));
    let mut again = simple(&a, 2, ea);
    again.change_id = Some("c1".into());
    let mut other_client = simple(&a, 2, ea);
    other_client.change_id = Some("c1".into());
    other_client.client_id = Some("v.c".into());
    let ra = writer.submit(again).await;
    let rb = writer.submit(simple(&b, 1, eb)).await;
    assert_eq!(ra.outcome().await, refused(RefusedWhy::Duplicate));
    assert_eq!(rb.outcome().await, Ok(()));
    // The same change id from another client is another change.
    assert_eq!(writer.append(other_client).await, Ok(()));
    // Server changes (no client id) never collide.
    let mut s3 = simple(&a, 3, ea);
    s3.client_id = None;
    s3.change_id = None;
    let mut s4 = simple(&a, 4, ea);
    s4.client_id = None;
    s4.change_id = None;
    assert_eq!(writer.append(s3).await, Ok(()));
    assert_eq!(writer.append(s4).await, Ok(()));
    assert_eq!(head_seq(&server, &a).await, 4);
}

#[tokio::test]
async fn extra_writes_run_in_their_own_savepoint() {
    let (server, _flaky, world) = setup().await;
    let (a, b) = (
        new_doc(&server, &world, "A").await,
        new_doc(&server, &world, "B").await,
    );
    let (ea, eb) = (lease(&server, &a).await, lease(&server, &b).await);
    let writer = slow_writer(&server);
    assert_eq!(writer.append(simple(&a, 1, ea)).await, Ok(()));
    let mut revert = simple(&a, 2, ea);
    revert.reverts = Some(1);
    revert.extra = vec![Extra::SetRevertedBy { seq: 1 }];
    assert_eq!(writer.append(revert).await, Ok(()));
    let first = store(&server).change_by_seq(&a, 1).await.unwrap().unwrap();
    assert_eq!(first.reverted_by, Some(2));
    let second = store(&server).change_by_seq(&a, 2).await.unwrap().unwrap();
    assert_eq!((second.reverts, second.reverted_by), (Some(1), None));

    // Reverting it again is refused, that record only: B in the same batch commits.
    let mut twice = simple(&a, 3, ea);
    twice.extra = vec![Extra::SetRevertedBy { seq: 1 }];
    let ra = writer.submit(twice).await;
    let rb = writer.submit(simple(&b, 1, eb)).await;
    assert_eq!(ra.outcome().await, refused(RefusedWhy::AlreadyReverted));
    assert_eq!(rb.outcome().await, Ok(()));
    assert_eq!(head_seq(&server, &a).await, 2);
    assert_eq!(row_count(&server, &a).await, 2);
    // A record reverting a seq that does not exist is refused the same way.
    let mut missing = simple(&a, 3, ea);
    missing.extra = vec![Extra::SetRevertedBy { seq: 99 }];
    assert_eq!(
        writer.append(missing).await,
        refused(RefusedWhy::AlreadyReverted)
    );

    // Assembly references are replaced whole, each with the state of the part it is pinned to.
    let refs = |ids: &[(&str, &str)]| Extra::AssemblyRefs {
        rows: ids
            .iter()
            .map(|(c, p)| AssemblyRef {
                component_id: (*c).into(),
                part_id: (*p).into(),
                seq: 7,
            })
            .collect(),
        chosen: Vec::new(),
    };
    let mut r3 = simple(&a, 3, ea);
    r3.extra = vec![refs(&[("c1", &b), ("c2", &b)])];
    assert_eq!(writer.append(r3).await, Ok(()));
    let mut r4 = simple(&a, 4, ea);
    r4.extra = vec![refs(&[("c2", &b)])];
    assert_eq!(writer.append(r4).await, Ok(()));
    let rows = store(&server).assembly_refs(&a).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].component_id.as_str(), rows[0].seq), ("c2", 7));
}

#[tokio::test]
async fn a_failed_commit_makes_every_waiter_unknown_and_stores_nothing() {
    let (server, flaky, world) = setup().await;
    let (a, b) = (
        new_doc(&server, &world, "A").await,
        new_doc(&server, &world, "B").await,
    );
    let (ea, eb) = (lease(&server, &a).await, lease(&server, &b).await);
    let writer = slow_writer(&server);
    flaky.fail_commit(true);
    let mut bad_epoch = simple(&b, 2, eb + 7);
    bad_epoch.change_id = Some("x".into());
    let receipts = [
        writer.submit(simple(&a, 1, ea)).await,
        writer.submit(simple(&b, 1, eb)).await,
        writer.submit(bad_epoch).await,
    ];
    for r in receipts {
        assert_eq!(r.outcome().await, Err(JournalError::Unknown));
    }
    flaky.fail_commit(false);
    assert_eq!(
        (head_seq(&server, &a).await, row_count(&server, &a).await),
        (0, 0)
    );
    assert_eq!(
        (head_seq(&server, &b).await, row_count(&server, &b).await),
        (0, 0)
    );
    assert!(writer.healthy());
    // The next batch works, from the same seqs.
    assert_eq!(writer.append(simple(&a, 1, ea)).await, Ok(()));
    assert_eq!(writer.append(simple(&b, 1, eb)).await, Ok(()));
}

#[tokio::test]
async fn a_commit_that_reports_an_error_after_taking_effect_is_unknown_and_reload_sees_it() {
    let (server, flaky, world) = setup().await;
    let a = new_doc(&server, &world, "A").await;
    let ea = lease(&server, &a).await;
    let writer = new_journal(&server.state.store);
    flaky.commit_then_fail(true);
    assert_eq!(
        writer.append(simple(&a, 1, ea)).await,
        Err(JournalError::Unknown)
    );
    flaky.commit_then_fail(false);
    // The caller reloads: the change is there.
    let (head, epoch) = load_head(store(&server), server.state.blobs.as_ref(), &a)
        .await
        .unwrap();
    assert_eq!((head.seq, epoch), (1, 0));
    assert_eq!(head.doc.at(&["n"]), Some(&Value::Int(1)));
    assert_eq!(row_count(&server, &a).await, 1);
    // Writing it again is refused (its seq is taken); the next one goes in.
    assert_eq!(
        writer.append(simple(&a, 1, ea)).await,
        refused(RefusedWhy::OutOfOrder)
    );
    assert_eq!(
        writer.append(simple(&a, 2, ea)).await,
        Ok(()),
        "seq 2 follows the stored seq 1"
    );
}

#[tokio::test]
async fn a_writer_that_dies_is_unhealthy_answers_unknown_and_fails_readiness() {
    let (server, flaky, world) = setup().await;
    let a = new_doc(&server, &world, "A").await;
    let ea = lease(&server, &a).await;
    assert_eq!(server.get("/api/ready", None).await.0, StatusCode::OK);
    let writer = server.state.journal.clone();
    assert!(writer.healthy());
    flaky.panic_on_begin(true);
    assert_eq!(
        writer.append(simple(&a, 1, ea)).await,
        Err(JournalError::Unknown)
    );
    flaky.panic_on_begin(false);
    assert!(!writer.healthy());
    assert_eq!(
        writer.append(simple(&a, 1, ea)).await,
        Err(JournalError::Unknown)
    );
    assert_eq!(row_count(&server, &a).await, 0);
    assert_eq!(
        server.get("/api/ready", None).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn close_flushes_what_is_queued_and_later_appends_are_unknown() {
    let (server, _flaky, world) = setup().await;
    let a = new_doc(&server, &world, "A").await;
    let ea = lease(&server, &a).await;
    let writer = JournalWriter::start(server.state.store.clone(), 256, Duration::from_secs(5));
    let mut receipts = Vec::new();
    for seq in 1..=5 {
        receipts.push(writer.submit(simple(&a, seq, ea)).await);
    }
    // The batch would wait 5 s for more; closing writes it at once.
    tokio::time::timeout(Duration::from_secs(2), writer.close())
        .await
        .expect("close does not wait out the batch");
    for r in receipts {
        assert_eq!(r.outcome().await, Ok(()));
    }
    assert_eq!(head_seq(&server, &a).await, 5);
    assert!(!writer.healthy());
    assert_eq!(
        writer.append(simple(&a, 6, ea)).await,
        Err(JournalError::Unknown)
    );
    writer.close().await; // twice is fine
}

#[tokio::test]
async fn a_record_breaking_a_column_limit_is_refused_before_it_is_queued() {
    let (server, _flaky, world) = setup().await;
    let a = new_doc(&server, &world, "A").await;
    let ea = lease(&server, &a).await;
    let writer = new_journal(&server.state.store);
    let mut cases = Vec::new();
    let mut r = simple(&a, 1, ea);
    r.command = "x".repeat(101);
    cases.push(r);
    let mut r = simple(&a, 1, ea);
    r.change_id = Some("x".repeat(65));
    cases.push(r);
    let mut r = simple(&a, 1, ea);
    r.change_id = Some(String::new());
    cases.push(r);
    let mut r = simple(&a, 1, ea);
    r.client_id = Some("x".repeat(129));
    cases.push(r);
    let mut r = simple(&a, 1, ea);
    r.seq = u64::MAX;
    cases.push(r);
    let mut r = simple(&a, 1, ea);
    r.created_at = f64::NAN;
    cases.push(r);
    let mut r = simple(&a, 1, ea);
    r.extra = vec![Extra::AssemblyRefs {
        rows: vec![AssemblyRef {
            component_id: "c".repeat(201),
            part_id: a.clone(),
            seq: 0,
        }],
        chosen: Vec::new(),
    }];
    cases.push(r);
    let mut r = simple(&a, 1, ea);
    r.extra = vec![Extra::AssemblyRefs {
        rows: Vec::new(),
        chosen: vec![ChosenPart {
            component_id: "c".into(),
            part_id: "p".repeat(33),
        }],
    }];
    cases.push(r);
    // More parts to lock than a change may choose, more rows than an assembly may have.
    let mut r = simple(&a, 1, ea);
    r.extra = vec![Extra::AssemblyRefs {
        rows: Vec::new(),
        chosen: (0..=oswx_server::assemblies::MAX_CHOSEN_PARTS)
            .map(|n| ChosenPart {
                component_id: format!("c{n}"),
                part_id: format!("{n:032x}"),
            })
            .collect(),
    }];
    cases.push(r);
    let mut r = simple(&a, 1, ea);
    r.extra = vec![Extra::AssemblyRefs {
        rows: (0..=oswx_server::journal::MAX_ASSEMBLY_ROWS)
            .map(|n| AssemblyRef {
                component_id: format!("c{n}"),
                part_id: a.clone(),
                seq: 0,
            })
            .collect(),
        chosen: Vec::new(),
    }];
    cases.push(r);
    for r in cases {
        assert!(
            matches!(
                writer.append(r.clone()).await,
                Err(JournalError::Refused(RefusedWhy::Invalid(_)))
            ),
            "{r:?}"
        );
    }
    // A command of exactly 100 characters is fine.
    let mut r = simple(&a, 1, ea);
    r.command = "é".repeat(100);
    assert_eq!(writer.append(r).await, Ok(()));
}

// ------------------------------------------------------------------ loading

/// SplitMix64, so that a run is the same everywhere.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// The features of a document: (id, position).
fn features(doc: &Value) -> Vec<(Arc<str>, Pos)> {
    match doc.at(&["features"]) {
        Some(Value::Keyed(k)) => k
            .iter()
            .map(|(id, pos, _)| (id.clone(), pos.clone()))
            .collect(),
        _ => Vec::new(),
    }
}

/// A random edit of `doc` by `client`: operations and reads; `None` when it picked nothing doable.
fn random_edit(
    rng: &mut Rng,
    doc: &Value,
    client: &str,
    n: usize,
) -> Option<(Vec<Op>, BTreeSet<Path>)> {
    let feats = features(doc);
    let pick = |rng: &mut Rng| feats[rng.below(feats.len())].clone();
    let gap = |rng: &mut Rng| {
        let mut positions: Vec<Pos> = feats.iter().map(|(_, p)| p.clone()).collect();
        positions.sort();
        positions.dedup();
        let g = rng.below(positions.len() + 1);
        Pos::try_between(g.checked_sub(1).map(|i| &positions[i]), positions.get(g))
    };
    let roll = rng.below(100);
    let mut reads = BTreeSet::new();
    let ops = if feats.is_empty() || roll < 15 {
        let id = format!("{client}.{n}");
        let item = Value::from_plain(&rmpv::Value::Map(vec![
            ("id".into(), id.as_str().into()),
            ("name".into(), format!("F{n}").into()),
            (
                "params".into(),
                rmpv::Value::Map(vec![("depth".into(), 1.into())]),
            ),
        ]))
        .ok()?;
        if !feats.is_empty() && rng.below(2) == 0 {
            reads.insert(p(&["features", &pick(rng).0]));
        }
        vec![Op::Insert {
            list: p(&["features"]),
            item,
            pos: gap(rng)?,
        }]
    } else if roll < 50 {
        let (id, _) = pick(rng);
        vec![Op::Set {
            path: p(&["features", &id, "params", "depth"]),
            value: Value::Int(rng.below(1000) as i64),
        }]
    } else if roll < 62 && feats.len() > 3 {
        vec![Op::Remove {
            list: p(&["features"]),
            id: pick(rng).0,
        }]
    } else if roll < 75 {
        let (id, _) = pick(rng);
        vec![Op::Move {
            list: p(&["features"]),
            id,
            pos: gap(rng)?,
        }]
    } else if roll < 88 {
        vec![Op::Set {
            path: p(&["variables", ["x", "y", "z"][rng.below(3)]]),
            value: Value::Int(rng.below(100) as i64),
        }]
    } else {
        let (id, _) = pick(rng);
        let (other, _) = pick(rng);
        reads.insert(p(&["features", &other]));
        vec![
            Op::Set {
                path: p(&["features", &id, "params", "angle"]),
                value: Value::Float(rng.below(360) as f64 / 2.0),
            },
            Op::Unset {
                path: p(&["features", &id, "params", "angle"]),
            },
            Op::Set {
                path: p(&["features", &id, "suppressed"]),
                value: Value::Bool(rng.below(2) == 0),
            },
        ]
    };
    Some((ops, reads))
}

fn record_of(doc: &str, epoch: u64, client: Option<&str>, change: &Change, a: &Accepted) -> Record {
    Record {
        document: doc.to_owned(),
        seq: a.seq,
        parent_seq: a.seq - 1,
        epoch,
        author: "0123456789abcdef0123456789abcdef".into(),
        client_id: client.map(str::to_owned),
        change_id: client.map(|_| change.change_id.to_string()),
        command: change.command.to_string(),
        ops: encode_ops(&change.ops),
        inverse: encode_ops(&a.inverse),
        paths: encode_paths(&a.writes, &a.removed),
        reads: encode_reads(&a.reads),
        undo_of: None,
        reverts: None,
        created_at: 3000.0 + a.seq as f64,
        extra: Vec::new(),
    }
}

/// Plays a room: `n` random changes by two clients and the server, each decided against the head,
/// journaled, and adopted once durable. Answers every head it had, from seq 0.
async fn play_room(server: &TestServer, doc: &str, n: usize, seed: u64) -> (Vec<Head>, u64) {
    let blobs = server.state.blobs.as_ref();
    let (head, epoch0) = load_head(store(server), blobs, doc).await.unwrap();
    assert_eq!((head.seq, epoch0), (0, 0), "a new document");
    let epoch = lease(server, doc).await;
    let writer = &server.state.journal;
    let mut rng = Rng(seed);
    let mut heads = vec![head];
    let mut tries = 0;
    while heads.len() <= n {
        tries += 1;
        assert!(tries < n * 10, "too many edits refused");
        let head = heads.last().unwrap();
        // Every tenth change is the server's own (no client id), which only sets values.
        let server_made = tries % 10 == 0;
        let client = ["u1.c1", "u2.c1"][rng.below(2)];
        let (ops, reads) = if server_made {
            (
                vec![Op::Set {
                    path: p(&["variables", "s"]),
                    value: Value::Int(tries as i64),
                }],
                BTreeSet::new(),
            )
        } else {
            match random_edit(&mut rng, &head.doc, client, tries) {
                Some(edit) => edit,
                None => continue,
            }
        };
        let change = Change {
            change_id: Arc::from(format!("ch{tries}")),
            base: head.seq,
            ops,
            reads,
            depends: Vec::new(),
            undo_of: None,
            command: Arc::from("edit"),
        };
        let as_client: ClientId = Arc::from(if server_made { SERVER_CLIENT } else { client });
        let Ok(accepted) = decide(
            head,
            &as_client,
            &change,
            &|_| Known::Accepted,
            None,
            &Rules::openswx(),
        ) else {
            continue;
        };
        let record = record_of(
            doc,
            epoch,
            (!server_made).then_some(client),
            &change,
            &accepted,
        );
        assert_eq!(writer.append(record).await, Ok(()), "seq {}", accepted.seq);
        heads.push(Head {
            seq: accepted.seq,
            doc: accepted.doc,
            index: accepted.index,
        });
    }
    (heads, epoch)
}

fn assert_same_head(loaded: &Head, expected: &Head) {
    assert_eq!(loaded.seq, expected.seq);
    assert_eq!(loaded.doc, expected.doc, "document at seq {}", expected.seq);
    assert_eq!(
        loaded.index.to_wire(),
        expected.index.to_wire(),
        "index at seq {}",
        expected.seq
    );
}

#[tokio::test]
async fn load_head_after_300_changes_is_exactly_the_rooms_head_and_state_at_every_seq() {
    let (server, _flaky, world) = setup().await;
    let doc = new_doc(&server, &world, "Room").await;
    let (heads, epoch) = play_room(&server, &doc, 300, 7).await;
    let blobs = server.state.blobs.as_ref();
    let last = heads.last().unwrap();
    assert_eq!(last.seq, 300);
    assert!(features(&last.doc).len() > 3, "the room did real work");

    // From the creation snapshot (epoch 0) and the whole journal.
    let (loaded, snapshot_epoch) = load_head(store(&server), blobs, &doc).await.unwrap();
    assert_eq!(snapshot_epoch, 0);
    assert_same_head(&loaded, last);

    // From a snapshot in the middle, written under the room's epoch, and the tail.
    write_snapshot(store(&server), blobs, &doc, epoch, &heads[150], 5000.0)
        .await
        .unwrap();
    // Writing it again does nothing.
    write_snapshot(store(&server), blobs, &doc, epoch, &heads[150], 6000.0)
        .await
        .unwrap();
    let row = store(&server)
        .snapshot_at_or_before(&doc, 300)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (row.epoch, row.seq, row.created_at),
        (epoch as i64, 150, 5000.0)
    );
    let (loaded, snapshot_epoch) = load_head(store(&server), blobs, &doc).await.unwrap();
    assert_eq!(snapshot_epoch, epoch);
    assert_same_head(&loaded, last);
    // The older snapshot is still there.
    assert!(
        store(&server)
            .snapshot_at_or_before(&doc, 149)
            .await
            .unwrap()
            .is_some_and(|r| r.seq == 0)
    );

    // Every earlier state, on both sides of the middle snapshot.
    for h in &heads {
        let at = state_at(store(&server), blobs, &doc, h.seq as i64)
            .await
            .unwrap();
        assert_eq!(at, h.doc, "state at seq {}", h.seq);
    }
    for bad in [-1, 301] {
        assert!(matches!(
            state_at(store(&server), blobs, &doc, bad).await,
            Err(LoadError::NoState(s)) if s == bad
        ));
    }

    // GET content reads the head through the same path.
    let (status, body) = server
        .get(&format!("/api/documents/{doc}/content"), Some(&world.admin))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["seq"], 300);
}

/// A document with `n` simple changes journaled under a lease.
async fn journaled(server: &TestServer, world: &World, name: &str, n: u64) -> String {
    let doc = new_doc(server, world, name).await;
    let epoch = lease(server, &doc).await;
    for seq in 1..=n {
        assert_eq!(
            server.state.journal.append(simple(&doc, seq, epoch)).await,
            Ok(())
        );
    }
    doc
}

async fn load_err(server: &TestServer, doc: &str) -> LoadError {
    load_head(store(server), server.state.blobs.as_ref(), doc)
        .await
        .expect_err("loading fails")
}

#[tokio::test]
async fn damage_to_a_snapshot_or_the_journal_is_a_distinct_error() {
    let (server, _flaky, world) = setup().await;
    let blobs = server.state.blobs.clone();
    let key_of = async |doc: &str| {
        store(&server)
            .snapshot_at_or_before(doc, 0)
            .await
            .unwrap()
            .unwrap()
            .key
    };

    let corrupt = journaled(&server, &world, "Corrupt", 3).await;
    blobs
        .put(
            &key_of(&corrupt).await,
            Bytes::from_static(b"not a snapshot"),
        )
        .await
        .unwrap();
    assert!(matches!(
        load_err(&server, &corrupt).await,
        LoadError::Snapshot { .. }
    ));

    let missing = journaled(&server, &world, "Missing", 3).await;
    blobs.delete(&key_of(&missing).await).await.unwrap();
    assert!(matches!(
        load_err(&server, &missing).await,
        LoadError::SnapshotMissing(_)
    ));

    let no_row = journaled(&server, &world, "NoRow", 1).await;
    server
        .sql(&format!(
            "DELETE FROM snapshots WHERE document_id = '{no_row}'"
        ))
        .await;
    assert!(matches!(
        load_err(&server, &no_row).await,
        LoadError::NoSnapshot(1)
    ));

    let gap = journaled(&server, &world, "Gap", 4).await;
    server
        .sql(&format!(
            "DELETE FROM changes WHERE document_id = '{gap}' AND seq = 2"
        ))
        .await;
    assert!(matches!(load_err(&server, &gap).await, LoadError::Gap(2)));

    // The journal ends before the head.
    let short = journaled(&server, &world, "Short", 4).await;
    server
        .sql(&format!(
            "DELETE FROM changes WHERE document_id = '{short}' AND seq = 4"
        ))
        .await;
    assert!(matches!(load_err(&server, &short).await, LoadError::Gap(4)));

    let stale_op = journaled(&server, &world, "StaleOp", 4).await;
    let bad = encode_ops(&[Op::Set {
        path: p(&["features", "nope", "x"]),
        value: Value::Int(1),
    }]);
    server
        .sql(&format!(
            "UPDATE changes SET ops = {} WHERE document_id = '{stale_op}' AND seq = 3",
            common::blob_literal(&bad)
        ))
        .await;
    assert!(matches!(
        load_err(&server, &stale_op).await,
        LoadError::DoesNotApply { seq: 3, .. }
    ));

    let damaged = journaled(&server, &world, "Damaged", 2).await;
    server
        .sql(&format!(
            "UPDATE changes SET paths = {} WHERE document_id = '{damaged}' AND seq = 2",
            common::blob_literal(&[0xc1])
        ))
        .await;
    assert!(matches!(
        load_err(&server, &damaged).await,
        LoadError::Damaged { seq: 2, .. }
    ));

    assert!(matches!(
        load_err(&server, "0123456789abcdef0123456789abcdef").await,
        LoadError::NoDocument
    ));
    // GET content of a damaged document is a server error, not a shorter document.
    let (status, _) = server
        .get(&format!("/api/documents/{gap}/content"), Some(&world.admin))
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn a_journaled_document_survives_a_restart() {
    let (server, _flaky, world) = setup().await;
    let doc = new_doc(&server, &world, "Room").await;
    let (heads, _) = play_room(&server, &doc, 40, 11).await;
    let server = server.restart().await;
    let (loaded, _) = load_head(store(&server), server.state.blobs.as_ref(), &doc)
        .await
        .unwrap();
    assert_same_head(&loaded, heads.last().unwrap());
    // The new process takes the lease under the next epoch and goes on writing.
    let epoch = lease(&server, &doc).await;
    assert_eq!(epoch, 2);
    let mut r = simple(&doc, 41, epoch);
    r.change_id = Some("after-restart".into());
    assert_eq!(server.state.journal.append(r).await, Ok(()));
}

#[tokio::test]
async fn a_record_for_a_document_that_does_not_exist_is_refused_alone() {
    let (server, _flaky, world) = setup().await;
    let a = new_doc(&server, &world, "A").await;
    let ea = lease(&server, &a).await;
    let writer = slow_writer(&server);
    let ghost = "0123456789abcdef0123456789abcdef";
    let rg = writer.submit(simple(ghost, 1, 0)).await;
    let ra = writer.submit(simple(&a, 1, ea)).await;
    assert_eq!(rg.outcome().await, refused(RefusedWhy::NoDocument));
    assert_eq!(ra.outcome().await, Ok(()));
    assert_eq!(store(&server).room_lease(ghost).await.unwrap(), None);
}

#[tokio::test]
async fn a_batch_that_never_commits_times_out_unknown_and_the_next_batch_works() {
    let (server, flaky, world) = setup().await;
    let (a, b) = (
        new_doc(&server, &world, "A").await,
        new_doc(&server, &world, "B").await,
    );
    let (ea, eb) = (lease(&server, &a).await, lease(&server, &b).await);
    let writer = JournalWriter::start_with_timeout(
        server.state.store.clone(),
        256,
        Duration::from_millis(50),
        Duration::from_millis(300),
    );
    flaky.hang_commit(true);
    let ra = writer.submit(simple(&a, 1, ea)).await;
    let rb = writer.submit(simple(&b, 1, eb)).await;
    let outcomes = tokio::time::timeout(Duration::from_secs(10), async {
        (ra.outcome().await, rb.outcome().await)
    })
    .await
    .expect("the batch times out");
    assert_eq!(
        outcomes,
        (Err(JournalError::Unknown), Err(JournalError::Unknown))
    );
    flaky.hang_commit(false);
    assert_eq!(
        (head_seq(&server, &a).await, row_count(&server, &a).await),
        (0, 0)
    );
    assert!(writer.healthy());
    assert_eq!(writer.append(simple(&a, 1, ea)).await, Ok(()));
    assert_eq!(writer.append(simple(&b, 1, eb)).await, Ok(()));
}

#[tokio::test]
async fn a_snapshot_of_a_head_the_journal_has_not_reached_is_refused() {
    let (server, _flaky, world) = setup().await;
    let doc = new_doc(&server, &world, "Room").await;
    let (heads, epoch) = play_room(&server, &doc, 5, 3).await;
    let blobs = server.state.blobs.as_ref();
    let durable = heads.last().unwrap().clone();
    // A tentative head: decided, not journaled.
    let change = Change {
        change_id: Arc::from("tentative"),
        base: durable.seq,
        ops: vec![Op::Set {
            path: p(&["variables", "t"]),
            value: Value::Int(1),
        }],
        reads: BTreeSet::new(),
        depends: Vec::new(),
        undo_of: None,
        command: Arc::from("edit"),
    };
    let a = decide(
        &durable,
        &Arc::from("u1.c1"),
        &change,
        &|_| Known::Accepted,
        None,
        &Rules::openswx(),
    )
    .unwrap();
    let tentative = Head {
        seq: a.seq,
        doc: a.doc,
        index: a.index,
    };
    let err = write_snapshot(store(&server), blobs, &doc, epoch, &tentative, 9.0)
        .await
        .expect_err("refused");
    assert!(
        matches!(
            err,
            WriteSnapshotError::AheadOfJournal {
                seq: 6,
                head_seq: 5
            }
        ),
        "{err}"
    );
    assert!(
        store(&server)
            .snapshot_row(&doc, epoch as i64, 6)
            .await
            .unwrap()
            .is_none()
    );
    let (loaded, snapshot_epoch) = load_head(store(&server), blobs, &doc).await.unwrap();
    assert_eq!(snapshot_epoch, 0);
    assert_same_head(&loaded, &durable);
    // The durable head is accepted, once.
    write_snapshot(store(&server), blobs, &doc, epoch, &durable, 9.0)
        .await
        .unwrap();
    write_snapshot(store(&server), blobs, &doc, epoch, &durable, 10.0)
        .await
        .unwrap();
    let row = store(&server)
        .snapshot_row(&doc, epoch as i64, 5)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.created_at, 9.0);
    assert!(matches!(
        write_snapshot(
            store(&server),
            blobs,
            "0123456789abcdef0123456789abcdef",
            1,
            &durable,
            9.0
        )
        .await,
        Err(WriteSnapshotError::NoDocument)
    ));
}

#[tokio::test]
async fn loading_replays_a_journal_longer_than_one_page() {
    let (server, _flaky, world) = setup().await;
    let doc = new_doc(&server, &world, "Long").await;
    let (heads, _) = play_room(&server, &doc, 1100, 5).await;
    let blobs = server.state.blobs.as_ref();
    let (loaded, _) = load_head(store(&server), blobs, &doc).await.unwrap();
    assert_same_head(&loaded, heads.last().unwrap());
    for seq in [0, 499, 500, 501, 999, 1000, 1001, 1100] {
        let at = state_at(store(&server), blobs, &doc, seq as i64)
            .await
            .unwrap();
        assert_eq!(at, heads[seq].doc, "state at seq {seq}");
    }
}

#[tokio::test]
async fn a_record_submitted_just_before_shutdown_is_durable_after_it() {
    let (server, _flaky, world) = setup().await;
    let doc = new_doc(&server, &world, "A").await;
    let epoch = lease(&server, &doc).await;
    let receipt = server.state.journal.submit(simple(&doc, 1, epoch)).await;
    // An orderly stop: the hub, then the journal, which writes what it has queued.
    let server = server.restart().await;
    assert_eq!(receipt.outcome().await, Ok(()));
    assert_eq!(head_seq(&server, &doc).await, 1);
    let (head, _) = load_head(store(&server), server.state.blobs.as_ref(), &doc)
        .await
        .unwrap();
    assert_eq!(head.doc.at(&["n"]), Some(&Value::Int(1)));
}

#[test]
fn refusals_read_as_before() {
    let cases = [
        (RefusedWhy::Lease, "lease"),
        (RefusedWhy::OutOfOrder, "out of order"),
        (RefusedWhy::Duplicate, "duplicate"),
        (RefusedWhy::AlreadyReverted, "already reverted"),
        (RefusedWhy::ExtraFailed, "extra writes failed"),
        (RefusedWhy::NoDocument, "no such document"),
        (
            RefusedWhy::Invalid("command is too long".into()),
            "invalid record: command is too long",
        ),
    ];
    for (why, text) in cases {
        assert_eq!(why.to_string(), text);
    }
    assert_eq!(
        JournalError::Refused(RefusedWhy::Lease).to_string(),
        "the journal refused the change: lease"
    );
}

// ------------------------------------------------------------------ Task 16: leases under concurrency

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_writers_with_different_epochs_cannot_both_write() {
    let (server, _flaky, world) = setup().await;
    let doc = new_doc(&server, &world, "D").await;
    // Two rooms of one document, each with a journal writer of its own (as two nodes have) and
    // the epoch its acquisition gave it.
    let old = new_journal(&server.state.store);
    let new = new_journal(&server.state.store);
    let first = lease(&server, &doc).await;
    assert_eq!(old.append(simple(&doc, 1, first)).await, Ok(()));
    let second = lease(&server, &doc).await;
    assert_eq!(second, first + 1);
    let stale = |seq: u64| Record {
        change_id: Some(format!("stale{seq}")),
        ..simple(&doc, seq, first)
    };
    // The writer that lost the lease is refused from then on: alone ...
    assert_eq!(old.append(stale(2)).await, refused(RefusedWhy::Lease));
    // ... and racing the holder for the same seq, each in a transaction of its own.
    for seq in 2..22 {
        let (lost, held) = tokio::join!(
            old.append(stale(seq)),
            new.append(simple(&doc, seq, second))
        );
        assert_eq!(lost, refused(RefusedWhy::Lease), "seq {seq}");
        assert_eq!(held, Ok(()), "seq {seq}");
    }
    let rows = store(&server)
        .changes_between(&doc, 0, i64::MAX, 100)
        .await
        .unwrap();
    assert_eq!(rows.len(), 21);
    assert_eq!(rows[0].epoch, first as i64);
    assert!(
        rows[1..]
            .iter()
            .all(|r| r.epoch == second as i64
                && !r.change_id.as_deref().unwrap().starts_with("stale"))
    );
    assert_eq!(head_seq(&server, &doc).await, 21);
    // The holder's own epoch moving on stops it too (a room loaded twice in one process).
    let third = lease(&server, &doc).await;
    assert_eq!(
        new.append(simple(&doc, 22, second)).await,
        refused(RefusedWhy::Lease)
    );
    assert_eq!(new.append(simple(&doc, 22, third)).await, Ok(()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_in_the_middle_of_a_stream_cuts_it_at_one_point() {
    let (server, _flaky, world) = setup().await;
    let doc = new_doc(&server, &world, "D").await;
    let writer = new_journal(&server.state.store);
    let first = lease(&server, &doc).await;
    // 300 records in a row from the old holder, while the lease is taken from under it.
    let mut receipts = Vec::new();
    for seq in 1..=150 {
        receipts.push(writer.submit(simple(&doc, seq, first)).await);
    }
    let taking = {
        let (store, doc) = (server.state.store.clone(), doc.clone());
        tokio::spawn(async move { acquire(store.as_ref(), &doc, "other", 1000.0, 60.0).await })
    };
    for seq in 151..=300 {
        receipts.push(writer.submit(simple(&doc, seq, first)).await);
    }
    assert_eq!(taking.await.unwrap().unwrap(), first + 1);
    let mut outcomes = Vec::new();
    for receipt in receipts {
        outcomes.push(receipt.outcome().await);
    }
    // Written up to some point, refused for the lease from there on: never one after a refusal.
    let written = outcomes.iter().take_while(|o| o.is_ok()).count();
    assert!(
        outcomes[written..]
            .iter()
            .all(|o| *o == refused(RefusedWhy::Lease)),
        "{:?}",
        &outcomes[written..]
    );
    assert_eq!(head_seq(&server, &doc).await, written as i64);
    assert_eq!(row_count(&server, &doc).await, written);
    // And whatever it sends after the takeover is refused.
    assert_eq!(
        writer.append(simple(&doc, written as u64 + 1, first)).await,
        refused(RefusedWhy::Lease)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rooms_opening_a_new_document_at_once_each_get_an_epoch_of_their_own() {
    let (server, _flaky, world) = setup().await;
    for round in 0..5 {
        let doc = new_doc(&server, &world, &format!("D{round}")).await;
        let mut tasks = FuturesUnordered::new();
        for node in 0..6 {
            let (store, doc) = (server.state.store.clone(), doc.clone());
            tasks.push(tokio::spawn(async move {
                acquire(store.as_ref(), &doc, &format!("node{node}"), 1000.0, 60.0).await
            }));
        }
        let mut epochs = Vec::new();
        while let Some(taken) = tasks.next().await {
            epochs.push(taken.unwrap().expect("every acquisition gets the lease"));
        }
        epochs.sort_unstable();
        assert_eq!(epochs, [1, 2, 3, 4, 5, 6], "round {round}");
        let row = store(&server).room_lease(&doc).await.unwrap().unwrap();
        assert_eq!(row.epoch, 6);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_write_racing_the_first_acquisition_is_settled_by_the_lease_row() {
    let (server, _flaky, world) = setup().await;
    let writer = new_journal(&server.state.store);
    for round in 0..10 {
        let doc = new_doc(&server, &world, &format!("D{round}")).await;
        // A write under epoch 0 (a document nobody acquired) and a room's first acquisition.
        let (written, taken) = tokio::join!(
            writer.append(simple(&doc, 1, 0)),
            acquire(store(&server), &doc, "node", 1000.0, 60.0)
        );
        assert_eq!(
            taken.expect("the acquisition gets the lease"),
            1,
            "round {round}"
        );
        // Before the acquisition it is written; after it, refused. Never an error.
        match written {
            Ok(()) => assert_eq!(head_seq(&server, &doc).await, 1),
            Err(why) => {
                assert_eq!(Err(why), refused(RefusedWhy::Lease), "round {round}");
                assert_eq!(head_seq(&server, &doc).await, 0);
            }
        }
        let row = store(&server).room_lease(&doc).await.unwrap().unwrap();
        assert_eq!((row.epoch, row.node_id.as_str()), (1, "node"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_goes_in_with_one_insert_and_a_duplicate_in_it_costs_only_itself() {
    let (server, flaky, world) = setup().await;
    let (a, b) = (
        new_doc(&server, &world, "A").await,
        new_doc(&server, &world, "B").await,
    );
    let (ea, eb) = (lease(&server, &a).await, lease(&server, &b).await);
    let writer = slow_writer(&server);
    writer.append(simple(&a, 1, ea)).await.unwrap();
    // One batch: two documents' records, one of them a change id the journal already has.
    let again = Record {
        change_id: Some("c1".into()),
        ..simple(&a, 2, ea)
    };
    let begins = flaky.begins();
    let receipts = [
        writer.submit(simple(&b, 1, eb)).await,
        writer.submit(again).await,
        writer.submit(simple(&b, 2, eb)).await,
        writer.submit(simple(&a, 3, ea)).await,
    ];
    let mut outcomes = Vec::new();
    for receipt in receipts {
        outcomes.push(receipt.outcome().await);
    }
    assert_eq!(
        outcomes,
        [
            Ok(()),
            refused(RefusedWhy::Duplicate),
            Ok(()),
            refused(RefusedWhy::OutOfOrder)
        ]
    );
    // The insert of all four left one out, so the batch was written again record by record.
    assert_eq!(flaky.begins() - begins, 2);
    assert_eq!(flaky.duplicate_inserts(), 1);
    assert_eq!(
        (head_seq(&server, &a).await, head_seq(&server, &b).await),
        (1, 2)
    );
    assert_eq!(
        (row_count(&server, &a).await, row_count(&server, &b).await),
        (1, 2)
    );
    // Without a duplicate: one transaction.
    let begins = flaky.begins();
    let receipts = [
        writer.submit(simple(&a, 2, ea)).await,
        writer.submit(simple(&b, 3, eb)).await,
        writer.submit(simple(&a, 3, ea)).await,
    ];
    for receipt in receipts {
        assert_eq!(receipt.outcome().await, Ok(()));
    }
    assert_eq!(flaky.begins() - begins, 1);
    assert_eq!(
        (head_seq(&server, &a).await, head_seq(&server, &b).await),
        (3, 3)
    );
}

#[tokio::test]
async fn a_record_with_the_zero_character_is_refused_alone() {
    let (server, _flaky, world) = setup().await;
    let doc = new_doc(&server, &world, "D").await;
    let epoch = lease(&server, &doc).await;
    let writer = slow_writer(&server);
    let bad_command = Record {
        command: "te\0st".into(),
        ..simple(&doc, 1, epoch)
    };
    let bad_change_id = Record {
        change_id: Some("c\0".into()),
        ..simple(&doc, 1, epoch)
    };
    let bad_client = Record {
        client_id: Some("u.\0".into()),
        ..simple(&doc, 1, epoch)
    };
    // One batch: the three are refused before they reach the database, the fourth is written.
    let receipts = [
        writer.submit(bad_command).await,
        writer.submit(bad_change_id).await,
        writer.submit(bad_client).await,
        writer.submit(simple(&doc, 1, epoch)).await,
    ];
    let mut outcomes = Vec::new();
    for receipt in receipts {
        outcomes.push(receipt.outcome().await);
    }
    let invalid = refused(RefusedWhy::Invalid(
        "text must not contain the zero character".into(),
    ));
    assert_eq!(
        outcomes,
        [invalid.clone(), invalid.clone(), invalid, Ok(())]
    );
    assert_eq!(head_seq(&server, &doc).await, 1);
    // Asking for such a change answers nothing, on either database.
    let s = store(&server);
    assert_eq!(
        s.change_seq_by_client(&doc, "u.c", "c\0").await.unwrap(),
        None
    );
    assert_eq!(s.undo_target(&doc, "u.\0", "c1").await.unwrap(), None);
    assert_eq!(s.recent_change_ids(&doc, "u.\0", 10).await.unwrap(), []);
}

// ------------------------------------------------------------------ Task 16, fix round 1

/// The lease row is locked by the batch that checked its epoch, until that batch commits: an
/// acquisition cannot move the epoch on between the check and the write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_acquisition_waits_for_the_batch_that_checked_the_lease() {
    let (server, flaky, world) = setup().await;
    let doc = new_doc(&server, &world, "D").await;
    let epoch = lease(&server, &doc).await;
    let writer = new_journal(&server.state.store);
    // The batch has checked the lease and inserted its row; it is held before it commits.
    flaky.head_gate.shut();
    let receipt = writer.submit(simple(&doc, 1, epoch)).await;
    flaky.head_gate.until_waiting(1).await;
    let mut taking = {
        let (store, doc) = (server.state.store.clone(), doc.clone());
        tokio::spawn(async move { acquire(store.as_ref(), &doc, "other", 1000.0, 60.0).await })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(600), &mut taking)
            .await
            .is_err(),
        "the lease was taken from under a batch that had checked it"
    );
    flaky.head_gate.open();
    // The record was written under the epoch it was checked against; the acquisition follows.
    assert_eq!(receipt.outcome().await, Ok(()));
    assert_eq!(taking.await.unwrap().unwrap(), epoch + 1);
    let rows = store(&server)
        .changes_between(&doc, 0, i64::MAX, 10)
        .await
        .unwrap();
    assert_eq!((rows.len(), rows[0].epoch), (1, epoch as i64));
}

/// One organisation whose row lock is held for a long time, and requests queued on it, must not
/// take the connections the journal and other organisations need: the queued requests wait in
/// the server (holding no connection), the journal has connections of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_busy_organisation_holds_up_neither_the_journal_nor_other_organisations() {
    // Four connections for requests (two of them for transactions).
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (status, other) = server
        .post(
            "/api/orgs",
            Some(&world.admin),
            serde_json::json!({"name": "Other"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let other = other["id"].as_str().unwrap().to_owned();
    let (status, elsewhere) = server
        .post(
            &format!("/api/orgs/{other}/projects"),
            Some(&world.admin),
            serde_json::json!({"name": "Elsewhere"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, doc) = server
        .post(
            &format!(
                "/api/projects/{}/documents",
                elsewhere["id"].as_str().unwrap()
            ),
            Some(&world.admin),
            serde_json::json!({"name": "D", "content": common::part()}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let doc = doc["id"].as_str().unwrap().to_owned();
    let epoch = lease(&server, &doc).await;
    let server = Arc::new(server);

    // A request holds the organisation's row lock (it is parked in the middle of renaming it) ...
    flaky.org_gate.shut();
    let holder = {
        let (server, w) = (server.clone(), (world.org.clone(), world.admin.clone()));
        tokio::spawn(async move {
            server
                .patch(
                    &format!("/api/orgs/{}", w.0),
                    Some(&w.1),
                    serde_json::json!({"name": "Held"}),
                )
                .await
        })
    };
    flaky.org_gate.until_waiting(1).await;
    // ... and three more writes to that organisation queue up behind it.
    let mut queued = Vec::new();
    for n in 0..3 {
        let (server, w) = (server.clone(), (world.org.clone(), world.admin.clone()));
        queued.push(tokio::spawn(async move {
            server
                .post(
                    &format!("/api/orgs/{}/projects", w.0),
                    Some(&w.1),
                    serde_json::json!({"name": format!("Queued {n}")}),
                )
                .await
        }));
    }
    while server.state.org_queue.waiting(&world.org) < 3 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // The journal writes at once, on PostgreSQL (on SQLite the parked request holds the one
    // writer: there the journal waits for it, as every write does).
    if common::on_postgres() {
        let written = tokio::time::timeout(
            Duration::from_secs(3),
            server.state.journal.append(simple(&doc, 1, epoch)),
        )
        .await
        .expect("the journal is not held up by requests waiting for a lock");
        assert_eq!(written, Ok(()));
        // And so does another organisation's write.
        let (status, body) = tokio::time::timeout(
            Duration::from_secs(3),
            server.post(
                &format!("/api/orgs/{other}/projects"),
                Some(&world.admin),
                serde_json::json!({"name": "Meanwhile"}),
            ),
        )
        .await
        .expect("another organisation is not held up");
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }
    assert!(!holder.is_finished());
    assert!(queued.iter().all(|task| !task.is_finished()));
    flaky.org_gate.open();
    assert_eq!(holder.await.unwrap().0, StatusCode::OK);
    // Each queued write then has its turn.
    for task in queued {
        assert_eq!(task.await.unwrap().0, StatusCode::CREATED);
    }
}

/// Someone holds the row of one document of a batch for longer than the journal waits for a
/// lock. Only that document's records are not written (they come back `Unknown`, and its room
/// reloads); the other document's, in the same batch, are stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_locked_document_costs_only_its_own_changes() {
    if !common::on_postgres() {
        eprintln!("skipped on SQLite: it has one writer, and no row lock to wait for");
        return;
    }
    let (server, _flaky) = TestServer::start_flaky(|s| s.database_journal_lock_timeout = 0.5).await;
    let world = World::new(&server).await.unwrap();
    let (a, b) = (
        new_doc(&server, &world, "A").await,
        new_doc(&server, &world, "B").await,
    );
    let (ea, eb) = (lease(&server, &a).await, lease(&server, &b).await);
    let writer = slow_writer(&server);
    // A transaction of a request holds A's row.
    let mut holder = server.state.store.begin().await.unwrap();
    holder
        .lock_documents(std::slice::from_ref(&a))
        .await
        .unwrap();
    // One batch: records of both documents.
    let started = std::time::Instant::now();
    let receipts = [
        writer.submit(simple(&b, 1, eb)).await,
        writer.submit(simple(&a, 1, ea)).await,
        writer.submit(simple(&b, 2, eb)).await,
        writer.submit(simple(&a, 2, ea)).await,
    ];
    let mut outcomes = Vec::new();
    for receipt in receipts {
        outcomes.push(receipt.outcome().await);
    }
    assert_eq!(
        outcomes,
        [
            Ok(()),
            Err(JournalError::Unknown),
            Ok(()),
            Err(JournalError::Unknown)
        ]
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        (head_seq(&server, &a).await, head_seq(&server, &b).await),
        (0, 2)
    );
    assert_eq!(row_count(&server, &a).await, 0);
    // Once the row is free the blocked document is written to again.
    holder.rollback().await.unwrap();
    assert_eq!(writer.append(simple(&a, 1, ea)).await, Ok(()));
    assert_eq!(head_seq(&server, &a).await, 1);
}

// ------------------------------------------------------------------ Task 16, fix round 3

/// The scenario the two-step deletion and the journal's fallback were made for, through the real
/// path: an organisation's deletion is held inside its transaction (it has marked, and holds the
/// rows of the organisation's documents), and a journal batch mixes a change to one of those
/// documents with changes to another organisation's. Only the blocked document's change is not
/// written; the other organisation's are; and a writer of the other organisation that lost its
/// lease is still refused for that, in the fallback as in any batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deletion_held_in_its_transaction_costs_other_organisations_nothing() {
    if !common::on_postgres() {
        eprintln!("skipped on SQLite: a held transaction holds the one writer there");
        return;
    }
    let (server, flaky) = TestServer::start_flaky(|s| {
        s.open_registration = true;
        s.database_journal_lock_timeout = 0.5;
    })
    .await;
    let world = World::new(&server).await.unwrap();
    let theirs = new_doc(&server, &world, "Doomed").await;
    let (status, other) = server
        .post(
            "/api/orgs",
            Some(&world.admin),
            serde_json::json!({"name": "Other"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, elsewhere) = server
        .post(
            &format!("/api/orgs/{}/projects", other["id"].as_str().unwrap()),
            Some(&world.admin),
            serde_json::json!({"name": "Elsewhere"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let mut ours = Vec::new();
    for name in ["B", "C"] {
        let (status, doc) = server
            .post(
                &format!(
                    "/api/projects/{}/documents",
                    elsewhere["id"].as_str().unwrap()
                ),
                Some(&world.admin),
                serde_json::json!({"name": name, "content": common::part()}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        ours.push(doc["id"].as_str().unwrap().to_owned());
    }
    let (b, c) = (ours[0].clone(), ours[1].clone());
    let (ea, eb) = (lease(&server, &theirs).await, lease(&server, &b).await);
    // (C's writer holds an epoch the lease has moved on from.)
    let stale = lease(&server, &c).await;
    let ec = lease(&server, &c).await;
    assert!(ec > stale);
    let writer = slow_writer(&server);
    let server = Arc::new(server);

    // The deletion has marked the organisation and is held before its commit.
    flaky.mark_gate.shut();
    let deletion = {
        let (server, w) = (server.clone(), (world.org.clone(), world.admin.clone()));
        tokio::spawn(async move {
            server
                .call(
                    reqwest::Method::DELETE,
                    &format!("/api/orgs/{}", w.0),
                    Some(&w.1),
                    Some(serde_json::json!({
                        "confirm_name": "Robotics",
                        "current_password": common::PASSWORD,
                    })),
                )
                .await
        })
    };
    flaky.mark_gate.until_waiting(1).await;

    // One batch: the deleted organisation's document, and two of another organisation.
    let receipts = [
        writer.submit(simple(&b, 1, eb)).await,
        writer.submit(simple(&theirs, 1, ea)).await,
        writer.submit(simple(&c, 1, stale)).await,
        writer.submit(simple(&b, 2, eb)).await,
    ];
    let mut outcomes = Vec::new();
    for receipt in receipts {
        outcomes.push(receipt.outcome().await);
    }
    assert_eq!(
        outcomes,
        [
            Ok(()),
            Err(JournalError::Unknown),
            refused(RefusedWhy::Lease),
            Ok(())
        ]
    );
    assert!(!deletion.is_finished(), "the deletion is still held");
    assert_eq!(head_seq(&server, &b).await, 2);
    assert_eq!(head_seq(&server, &c).await, 0);
    // The other organisation goes on writing while the deletion is held.
    assert_eq!(writer.append(simple(&c, 1, ec)).await, Ok(()));

    flaky.mark_gate.open();
    assert_eq!(deletion.await.unwrap().0, StatusCode::NO_CONTENT);
    // And once it has committed, the deleted document takes nothing more.
    assert_eq!(
        writer.append(simple(&theirs, 1, ea)).await,
        refused(RefusedWhy::NoDocument)
    );
}

/// A room that slipped in around a deletion can write nothing for the document any more: it
/// gets no lease, and a snapshot of it is refused and its file not left behind (the purger may
/// already have emptied the document's folder).
#[tokio::test]
async fn a_deleted_document_gets_no_lease_and_no_snapshot() {
    let (server, _flaky, world) = setup().await;
    let blobs = server.state.blobs.as_ref();
    let doc = new_doc(&server, &world, "D").await;
    let epoch = lease(&server, &doc).await;
    let writer = new_journal(&server.state.store);
    writer.append(simple(&doc, 1, epoch)).await.unwrap();
    let (head, _) = load_head(store(&server), blobs, &doc).await.unwrap();
    assert_eq!(head.seq, 1);

    // The project is deleted; the purger is held, so every row is still there.
    server.state.purger.gate().shut();
    let (status, _) = server
        .delete(
            &format!("/api/projects/{}", world.project),
            Some(&world.admin),
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let files = server.stored_under(&format!("docs/{doc}/")).await;
    assert_eq!(files.len(), 1, "{files:?}");

    let refused_lease = acquire(store(&server), &doc, "node", 1000.0, 60.0)
        .await
        .unwrap_err();
    assert!(
        refused_lease.is::<oswx_server::lease::NoDocument>(),
        "{refused_lease:#}"
    );
    assert_eq!(
        store(&server)
            .room_lease(&doc)
            .await
            .unwrap()
            .unwrap()
            .epoch,
        epoch as i64,
        "the lease did not move"
    );
    let refused_snapshot = write_snapshot(store(&server), blobs, &doc, epoch, &head, 9.0)
        .await
        .unwrap_err();
    assert!(
        matches!(refused_snapshot, WriteSnapshotError::NoDocument),
        "{refused_snapshot:?}"
    );
    assert_eq!(server.stored_under(&format!("docs/{doc}/")).await, files);

    // And once the purger has removed the document: nothing is put back.
    server.state.purger.gate().open();
    server.purged().await;
    assert!(
        server
            .stored_under(&format!("docs/{doc}/"))
            .await
            .is_empty()
    );
    for _ in 0..2 {
        assert!(
            acquire(store(&server), &doc, "node", 1000.0, 60.0)
                .await
                .is_err()
        );
        assert!(matches!(
            write_snapshot(store(&server), blobs, &doc, epoch, &head, 9.0).await,
            Err(WriteSnapshotError::NoDocument)
        ));
    }
    assert!(
        server
            .stored_under(&format!("docs/{doc}/"))
            .await
            .is_empty()
    );
    assert!(store(&server).room_lease(&doc).await.unwrap().is_none());
    // A document deleted on its own is refused the same way.
    let alone = {
        let (status, body) = server
            .post(
                "/api/orgs",
                Some(&world.admin),
                serde_json::json!({"name": "Other"}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (_, p) = server
            .post(
                &format!("/api/orgs/{}/projects", body["id"].as_str().unwrap()),
                Some(&world.admin),
                serde_json::json!({"name": "P"}),
            )
            .await;
        let (status, d) = server
            .post(
                &format!("/api/projects/{}/documents", p["id"].as_str().unwrap()),
                Some(&world.admin),
                serde_json::json!({"name": "Alone", "content": common::part()}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        d["id"].as_str().unwrap().to_owned()
    };
    let (head, _) = load_head(store(&server), blobs, &alone).await.unwrap();
    let (status, _) = server
        .delete(&format!("/api/documents/{alone}"), Some(&world.admin))
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(
        acquire(store(&server), &alone, "node", 1000.0, 60.0)
            .await
            .is_err()
    );
    assert!(matches!(
        write_snapshot(store(&server), blobs, &alone, 7, &head, 9.0).await,
        Err(WriteSnapshotError::NoDocument)
    ));
}

/// A snapshot being stored while its document is deleted and removed: when its row is about to
/// be written the document is gone, so nothing is recorded, and the file that was just stored
/// (after the purger emptied the document's folder) is taken away again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_stored_around_a_deletion_leaves_no_file() {
    let (server, blobs) = TestServer::start_blobs(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = new_doc(&server, &world, "D").await;
    let epoch = lease(&server, &doc).await;
    let writer = new_journal(&server.state.store);
    writer.append(simple(&doc, 1, epoch)).await.unwrap();
    let (head, _) = load_head(store(&server), server.state.blobs.as_ref(), &doc)
        .await
        .unwrap();
    // The snapshot's file takes a second to store ...
    blobs.put_delay(Duration::from_secs(1));
    let snapshot = {
        let (store, storage, doc) = (
            server.state.store.clone(),
            server.state.blobs.clone(),
            doc.clone(),
        );
        tokio::spawn(async move {
            write_snapshot(store.as_ref(), storage.as_ref(), &doc, epoch, &head, 9.0).await
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    // ... and meanwhile the project is deleted, and everything of it removed.
    let (status, _) = server
        .delete(
            &format!("/api/projects/{}", world.project),
            Some(&world.admin),
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    server.purged().await;
    assert!(
        server
            .stored_under(&format!("docs/{doc}/"))
            .await
            .is_empty()
    );
    assert!(
        !snapshot.is_finished(),
        "the snapshot is still being stored"
    );
    let refused = snapshot.await.unwrap().unwrap_err();
    assert!(
        matches!(refused, WriteSnapshotError::NoDocument),
        "{refused:?}"
    );
    let left = server.stored_under(&format!("docs/{doc}/")).await;
    assert!(left.is_empty(), "left behind for good: {left:?}");
}
