//! The journal writer, room leases, and loading a document back from its snapshot and journal.

mod common;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use common::{FlakyStore, TestServer, World, new_journal};
use oswx_core::{
    Accepted, Change, ClientId, Head, Known, Op, Path, Pos, Rules, Value, Write, decide,
};
use oswx_server::journal::{
    AssemblyRef, Extra, JournalError, JournalWriter, LoadError, Record, SERVER_CLIENT, load_head,
    state_at, write_snapshot,
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

fn refused(why: &str) -> Result<(), JournalError> {
    Err(JournalError::Refused(why.into()))
}

#[tokio::test]
async fn a_thousand_appends_from_fifty_tasks_land_in_order_in_few_transactions() {
    let (server, flaky, world) = setup().await;
    let writer = new_journal(&server.state.store);
    let mut docs = Vec::new();
    for i in 0..10 {
        let doc = new_doc(&server, &world, &format!("D{i}")).await;
        let epoch = lease(&server, &doc).await;
        // Per document: the next seq, and the receipts in the order they were submitted.
        docs.push(Arc::new((
            doc,
            epoch,
            tokio::sync::Mutex::new((1u64, Vec::new())),
        )));
    }
    let begins = flaky.begins();
    let mut tasks = Vec::new();
    for t in 0..50 {
        let d = docs[t % docs.len()].clone();
        let writer = writer.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..20 {
                // As a room does: take the seq and submit under the room's own order.
                let mut state = d.2.lock().await;
                let seq = state.0;
                state.0 += 1;
                let receipt = writer.submit(simple(&d.0, seq, d.1)).await;
                state.1.push((seq, receipt));
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    // Await every receipt in its own task; results of one document must arrive in seq order.
    let arrivals: Arc<Mutex<Vec<(usize, u64)>>> = Arc::default();
    let mut waiters = Vec::new();
    for (i, d) in docs.iter().enumerate() {
        let receipts = std::mem::take(&mut d.2.lock().await.1);
        for (seq, receipt) in receipts {
            let arrivals = arrivals.clone();
            waiters.push(tokio::spawn(async move {
                assert_eq!(receipt.outcome().await, Ok(()), "seq {seq}");
                arrivals.lock().unwrap().push((i, seq));
            }));
        }
    }
    for w in waiters {
        w.await.unwrap();
    }
    let arrivals = arrivals.lock().unwrap().clone();
    assert_eq!(arrivals.len(), 1000);
    for i in 0..docs.len() {
        let seqs: Vec<u64> = arrivals
            .iter()
            .filter(|(d, _)| *d == i)
            .map(|(_, s)| *s)
            .collect();
        assert_eq!(seqs, (1..=100).collect::<Vec<_>>(), "document {i}");
    }
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
    assert_eq!(ra.outcome().await, refused("lease"));
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
    assert_eq!(writer.append(simple(&a, 2, 0)).await, refused("lease"));
    // Without a row, any other epoch is refused.
    assert_eq!(writer.append(simple(&b, 1, 5)).await, refused("lease"));
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
            refused("out of order"),
            refused("out of order"),
            Ok(()),
            refused("out of order"),
            refused("out of order"),
        ]
    );
    assert_eq!(head_seq(&server, &a).await, 1);
    assert_eq!(head_seq(&server, &b).await, 1);
    assert_eq!(row_count(&server, &a).await, 1);
    // A later batch cannot slip them in out of order either: seq 3 is still refused, 2 goes in.
    assert_eq!(
        writer.append(simple(&a, 3, ea)).await,
        refused("out of order")
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
    assert_eq!(ra.outcome().await, refused("duplicate"));
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
    assert_eq!(ra.outcome().await, refused("already reverted"));
    assert_eq!(rb.outcome().await, Ok(()));
    assert_eq!(head_seq(&server, &a).await, 2);
    assert_eq!(row_count(&server, &a).await, 2);
    // A record reverting a seq that does not exist is refused the same way.
    let mut missing = simple(&a, 3, ea);
    missing.extra = vec![Extra::SetRevertedBy { seq: 99 }];
    assert_eq!(writer.append(missing).await, refused("already reverted"));

    // Assembly references are replaced whole, with the record's seq.
    let refs = |ids: &[(&str, &str)]| Extra::AssemblyRefs {
        rows: ids
            .iter()
            .map(|(c, p)| AssemblyRef {
                component_id: (*c).into(),
                part_id: (*p).into(),
            })
            .collect(),
    };
    let mut r3 = simple(&a, 3, ea);
    r3.extra = vec![refs(&[("c1", &b), ("c2", &b)])];
    assert_eq!(writer.append(r3).await, Ok(()));
    let mut r4 = simple(&a, 4, ea);
    r4.extra = vec![refs(&[("c2", &b)])];
    assert_eq!(writer.append(r4).await, Ok(()));
    let rows = store(&server).assembly_refs(&a).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].component_id.as_str(), rows[0].seq), ("c2", 4));
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
        refused("out of order")
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
        }],
    }];
    cases.push(r);
    for r in cases {
        assert!(
            matches!(writer.append(r.clone()).await, Err(JournalError::Refused(why)) if why.starts_with("invalid record")),
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
            "UPDATE changes SET ops = X'{}' WHERE document_id = '{stale_op}' AND seq = 3",
            hex::encode(&bad)
        ))
        .await;
    assert!(matches!(
        load_err(&server, &stale_op).await,
        LoadError::DoesNotApply { seq: 3, .. }
    ));

    let damaged = journaled(&server, &world, "Damaged", 2).await;
    server
        .sql(&format!(
            "UPDATE changes SET paths = X'c1' WHERE document_id = '{damaged}' AND seq = 2"
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
    assert_eq!(rg.outcome().await, refused("no such document"));
    assert_eq!(ra.outcome().await, Ok(()));
    assert_eq!(store(&server).room_lease(ghost).await.unwrap(), None);
}
