//! The change pipeline's harder paths: a record the journal refuses (judging the rest again),
//! resends the room's memory no longer holds, resends while a change is being written, answer order
//! when big changes are judged off the task, the bounds on what one connection may queue, changes
//! the server makes itself, and what snapshots and pruning leave behind.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use oswx_core::wire::{ChangeEvent, RejectCode, ServerMsg};
use oswx_core::{Op, Refusal, Seq};
use oswx_server::journal::{self, JournalWriter};
use oswx_server::room::{Hooks, ServerChangeError};
use oswx_server::store::Store;
use serde_json::json;

/// A journal that waits `ms` for more records after the first of a batch, so that a burst lands in
/// one batch.
fn slow(ms: u64) -> impl FnOnce(&Arc<dyn Store>) -> JournalWriter {
    move |store| JournalWriter::start(store.clone(), journal::MAX_BATCH, Duration::from_millis(ms))
}

fn var(name: &str, v: impl Into<serde_json::Value>) -> Op {
    set(&["variables", name], v.into())
}

async fn head_seq(server: &TestServer, doc: &str) -> i64 {
    let doc = server.state.store.document_by_id(doc).await.unwrap();
    doc.unwrap().head_seq
}

/// The change ids in the journal, in seq order.
async fn journal_ids(server: &TestServer, doc: &str) -> Vec<Option<String>> {
    let rows = server
        .state
        .store
        .changes_between(doc, 0, i64::MAX, 100_000)
        .await
        .unwrap();
    rows.into_iter().map(|r| r.change_id).collect()
}

fn rejections(msgs: &[ServerMsg]) -> Vec<&ServerMsg> {
    msgs.iter()
        .filter(|m| matches!(m, ServerMsg::Rejected { .. }))
        .collect()
}

#[tokio::test]
async fn a_refused_record_is_answered_alone_and_the_rest_are_judged_again() {
    let (server, flaky) = TestServer::start_flaky_journal(|_| {}, slow(300)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    // Fifty changes from two clients, interleaved; the journal refuses the tenth.
    let mut sent = Vec::new();
    for n in 0..25 {
        for (who, live) in [(0, &mut a), (1, &mut b)] {
            let change = live.make_change(0, vec![var(&format!("c{who}x{n}"), n)]);
            if sent.len() == 9 {
                flaky.refuse_change_once(&change.change_id);
            }
            live.send_change(&change).await;
            sent.push((who, change));
        }
    }
    let mut seqs = [Vec::new(), Vec::new()];
    for (i, (who, change)) in sent.iter().enumerate() {
        let live = if *who == 0 { &mut a } else { &mut b };
        let outcome = live.outcome(&change.change_id).await;
        if i == 9 {
            assert_eq!(outcome.code(), RejectCode::ServerError, "the refused one");
        } else {
            seqs[*who].push(outcome.seq());
        }
    }
    for s in &seqs {
        assert!(
            s.windows(2).all(|w| w[0] < w[1]),
            "in each client's order: {s:?}"
        );
    }
    assert!(rejections(&a.seen).is_empty() && rejections(&b.seen).is_empty());
    let all: BTreeSet<Seq> = seqs.iter().flatten().copied().collect();
    assert_eq!(all, (1..=49).collect(), "one seq each, none lost");
    assert_eq!(head_seq(&server, &doc).await, 49);
    let ids = journal_ids(&server, &doc).await;
    assert_eq!(
        ids.iter().collect::<BTreeSet<_>>().len(),
        49,
        "one row per change"
    );
}

#[tokio::test]
async fn a_resend_of_an_id_memory_forgot_is_answered_and_hurts_nobody() {
    let (server, _) = TestServer::start_flaky_journal(|_| {}, slow(300)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let mut c = Live::connect(&server, &doc, &world.admin).await;
    let x = a.make_change(0, vec![var("a", 1)]);
    assert_eq!(a.resend(&x).await.seq(), 1);
    b.drain().await;
    c.drain().await;
    // The room no longer remembers it; A sends it again on the current head.
    server.room(&doc).forget_change_ids().await;
    let b1 = b.make_change(1, vec![var("b", 1)]);
    b.send_change(&b1).await;
    let mut again = x.clone();
    again.base = 1;
    a.send_change(&again).await;
    let c1 = c.make_change(1, vec![var("c", 1)]);
    c.send_change(&c1).await;
    let b2 = b.make_change(1, vec![var("b2", 2)]);
    b.send_change(&b2).await;
    let c2 = c.make_change(1, vec![var("c2", 2)]);
    c.send_change(&c2).await;
    assert_eq!(
        a.outcome(&x.change_id).await,
        Outcome::Accepted(1),
        "the original seq"
    );
    for (live, changes) in [(&mut b, [&b1, &b2]), (&mut c, [&c1, &c2])] {
        for ch in changes {
            assert!(matches!(
                live.outcome(&ch.change_id).await,
                Outcome::Accepted(_)
            ));
        }
        assert!(rejections(&live.seen).is_empty(), "{:?}", live.seen);
    }
    let ids = journal_ids(&server, &doc).await;
    assert_eq!(ids.len(), 5);
    assert_eq!(
        ids.iter().collect::<BTreeSet<_>>().len(),
        5,
        "one row per change id"
    );
    // Everyone converges on the same document.
    let head = server.room(&doc).durable_head().await.unwrap();
    for live in [&mut a, &mut b, &mut c] {
        live.drain().await;
        assert_eq!(live.seq, head.seq);
    }
    let fresh = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(fresh.welcome.content, Some(head.doc));
}

#[tokio::test]
async fn after_a_restart_a_resend_with_a_new_base_is_known_from_the_join() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-laptop")).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let x = a.make_change(0, vec![var("a", 1)]);
    assert_eq!(a.resend(&x).await.seq(), 1);
    assert_eq!(b.change(vec![var("b", 1)], vec![], vec![]).await.seq(), 2);
    assert_eq!(b.change(vec![var("b", 2)], vec![], vec![]).await.seq(), 3);
    a.close().await;
    let server = server.restart().await;
    let mut a2 = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-laptop")).await;
    let lookups = flaky.change_lookups();
    let mut again = x.clone();
    again.base = 3;
    assert_eq!(a2.resend(&again).await, Outcome::Accepted(1));
    assert_eq!(flaky.change_lookups(), lookups, "answered from memory");
    assert_eq!(head_seq(&server, &doc).await, 3);
}

#[tokio::test]
async fn a_client_that_keeps_causing_duplicates_is_closed() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    for n in 0..3 {
        let change = a.make_change(a.seq, vec![var("a", n)]);
        flaky.refuse_change_once(&change.change_id);
        // (The journal says duplicate, but has no such row: it was not written.)
        assert_eq!(a.resend(&change).await.code(), RejectCode::ServerError);
    }
    let change = a.make_change(a.seq, vec![var("a", 4)]);
    flaky.refuse_change_once(&change.change_id);
    a.send_change(&change).await;
    assert_eq!(a.closed().await, (1008, "misbehaving client".into()));
}

#[tokio::test]
async fn a_resend_while_the_change_is_being_written_gets_its_seq_once() {
    let (server, _) = TestServer::start_flaky_journal(|_| {}, slow(400)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    let x = a.make_change(0, vec![var("a", 1)]);
    a.send_change(&x).await;
    // It reconnects while X is in the journal's batch, and sends X again.
    let mut a2 =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    assert_eq!(a.closed().await.0, 4409);
    assert_eq!(a2.resend(&x).await, Outcome::Accepted(1));
    let rest = a2.drain().await;
    assert!(
        rest.iter()
            .all(|m| !matches!(m, ServerMsg::Accepted { .. } | ServerMsg::Change(_))),
        "answered once: {rest:?}"
    );
    assert_eq!(
        journal_ids(&server, &doc).await,
        [Some(x.change_id.to_string())]
    );
}

#[tokio::test]
async fn a_clients_answers_stay_in_order_when_big_changes_are_judged_off_the_task() {
    let hooks = Hooks {
        judge_delay: Duration::from_millis(800),
        ..Hooks::default()
    };
    let (server, flaky) =
        TestServer::start_flaky_full(|_| {}, new_journal, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut first = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(
        first
            .change(vec![var("first", 1)], vec![], vec![])
            .await
            .seq(),
        1
    );
    first.close().await;
    // After a restart the room's memory starts at 1: a change made on 0 is looked up first.
    let server = server.restart().await;
    flaky.lookup_delay(Duration::from_millis(300));
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let big = |who: &str| -> Vec<Op> { (0..300).map(|n| var(&format!("{who}{n}"), n)).collect() };
    // B's big change waits for the journal lookup ...
    let b1 = b.make_change(0, big("b"));
    b.send_change(&b1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    // ... while A's big change is being judged off the task, and B sends one that builds on B1.
    let a1 = a.make_change(1, big("a"));
    a.send_change(&a1).await;
    let mut b2 = b.make_change(1, vec![var("b-after", 1)]);
    b2.depends = vec![b1.change_id.clone()];
    b.send_change(&b2).await;
    let first_answer: Answer = b.expect().await;
    assert_eq!(
        first_answer.change_id, &*b1.change_id,
        "B1 is answered first"
    );
    let s1 = first_answer.outcome.seq();
    let s2 = b.outcome(&b2.change_id).await.seq();
    assert!(s1 < s2, "{s1} {s2}");
    assert!(matches!(
        a.outcome(&a1.change_id).await,
        Outcome::Accepted(_)
    ));
}

#[tokio::test]
async fn what_a_connection_may_queue_is_bounded() {
    let (server, flaky) = TestServer::start_flaky(|s| s.max_change_bytes = 20000).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut first = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(
        first
            .change(vec![var("first", 1)], vec![], vec![])
            .await
            .seq(),
        1
    );
    first.close().await;
    let server = server.restart().await;
    flaky.lookup_delay(Duration::from_secs(3));
    // Bytes: behind a change that waits for the journal, 96 kB of changes (the limit is 80 kB).
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let wait = a.make_change(0, vec![var("wait", 1)]);
    a.send_change(&wait).await;
    let pad = "p".repeat(16_000);
    for n in 0..6 {
        let change = a.make_change(1, vec![var(&format!("pad{n}"), pad.clone())]);
        a.send_change(&change).await;
    }
    assert_eq!(a.closed().await.0, 4408);
    // Count: 1,100 small messages behind one that waits.
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let wait = b.make_change(0, vec![var("wait", 2)]);
    b.send_change(&wait).await;
    for _ in 0..1100 {
        b.send(&oswx_core::wire::ClientMsg::Ping).await;
    }
    assert_eq!(b.closed().await.0, 4408);
}

#[tokio::test]
async fn an_unreadable_frame_is_answered_after_the_answers_before_it() {
    let (server, _) = TestServer::start_flaky_journal(|_| {}, slow(300)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let change = a.make_change(0, vec![var("a", 1)]);
    a.send_change(&change).await;
    a.send_raw(vec![0xc1]).await;
    a.send_text("text").await;
    match a.recv().await {
        ServerMsg::Accepted { change_id, seq } => {
            assert_eq!((&*change_id, seq), (&*change.change_id, 1));
        }
        other => panic!("the change's answer comes first, got {other:?}"),
    }
    assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
    assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
}

#[tokio::test]
async fn a_read_only_connection_is_never_looked_up_in_the_journal() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (viewer, _) = world
        .invite(&server, "vi@example.com", None, Some("viewer"), "member")
        .await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.change(vec![var("a", 1)], vec![], vec![]).await.seq(), 1);
    a.close().await;
    let server = server.restart().await;
    let mut v = Live::connect(&server, &doc, &viewer).await;
    let lookups = flaky.change_lookups();
    let old_base = v.change_on(0, vec![var("v", 1)]).await;
    assert_eq!(old_base.code(), RejectCode::ReadOnly);
    assert_eq!(flaky.change_lookups(), lookups);
}

#[tokio::test]
async fn a_change_may_refer_to_at_most_4096_blobs() {
    let (server, blobs) = TestServer::start_blobs(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let refs: serde_json::Map<String, serde_json::Value> = (0..4097)
        .map(|n| (format!("r{n}"), json!({"$blob": format!("{n:064x}")})))
        .collect();
    let outcome = a.change(vec![var("refs", refs)], vec![], vec![]).await;
    assert_eq!(outcome.code(), RejectCode::Invalid);
    assert_eq!(blobs.exists_calls(), 0, "storage was not asked");
}

#[tokio::test]
async fn the_server_can_make_a_change_that_everyone_hears_and_a_reload_judges_alike() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.change(vec![set_depth(5)], vec![], vec![]).await.seq(), 1);
    let room = server.room(&doc);
    let seq = room
        .server_change(
            vec![set_depth(42), var("restored", true)],
            "restore to 0",
            &world.admin_id,
            "Ada",
        )
        .await
        .unwrap();
    assert_eq!(seq, 2);
    for live in [&mut a, &mut b] {
        let ev = loop {
            let ev: ChangeEvent = live.expect().await;
            if ev.seq == 2 {
                break ev;
            }
        };
        assert_eq!((&*ev.client_id, &*ev.change_id), ("~server", ""));
        assert_eq!(
            (&*ev.command, ev.author_name.as_str()),
            ("restore to 0", "Ada")
        );
        assert_eq!(ev.ops, vec![set_depth(42), var("restored", true)]);
    }
    // Refused on its merits like any change.
    let refused = room
        .server_change(
            vec![set(&["features", "gone", "x"], json!(1))],
            "restore",
            &world.admin_id,
            "Ada",
        )
        .await;
    assert!(
        matches!(
            refused,
            Err(ServerChangeError::Refused(Refusal::Invalid(_)))
        ),
        "{refused:?}"
    );
    let catch_up =
        Live::connect_with(&server, &doc, &world.admin, Hello::default().last_seq(0)).await;
    let events = catch_up.welcome.changes.unwrap();
    assert_eq!(
        (&*events[1].client_id, &*events[1].change_id),
        ("~server", "")
    );
    let memory = room.durable_head().await.unwrap();
    a.close().await;
    b.close().await;
    let server = server.restart().await;
    let (loaded, _) = oswx_server::snapshot::load_head(
        server.state.store.as_ref(),
        server.state.blobs.as_ref(),
        &doc,
    )
    .await
    .unwrap();
    assert_eq!(loaded.seq, memory.seq);
    assert_eq!(loaded.doc, memory.doc);
    assert_eq!(loaded.index.to_wire(), memory.index.to_wire());
}

#[tokio::test]
async fn a_snapshot_taken_while_changes_are_in_flight_holds_only_durable_state() {
    let (server, _) = TestServer::start_flaky_journal(|s| s.snapshot_every = 2, slow(100)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut changes = Vec::new();
    for n in 0..20 {
        let change = a.make_change(0, vec![var(&format!("v{n}"), n)]);
        a.send_change(&change).await;
        changes.push(change);
    }
    for change in &changes {
        a.outcome(&change.change_id).await.seq();
    }
    // (A burst lands in one batch, and one snapshot is written at a time: a few more changes give
    // the next one.)
    for n in 0..4 {
        a.change(vec![var("after", n)], vec![], vec![]).await.seq();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let deadline = tokio::time::Instant::now() + WAIT;
    while server.room_status(&doc).await.snapshot_seq < 20 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let store = server.state.store.as_ref();
    let mut rows = BTreeSet::new();
    for s in 1..=24 {
        if let Some(row) = store.snapshot_at_or_before(&doc, s).await.unwrap() {
            rows.insert((row.seq, row.key));
        }
    }
    assert!(rows.len() >= 3, "{rows:?}"); // (with the one the document was created with)
    for (seq, key) in rows {
        let bytes = server.state.blobs.get(&key).await.unwrap().unwrap();
        let (snap, _, at) = oswx_server::snapshot::decode(&bytes).unwrap();
        assert_eq!(at as i64, seq);
        let replayed =
            oswx_server::snapshot::state_at(store, server.state.blobs.as_ref(), &doc, seq)
                .await
                .unwrap();
        assert_eq!(
            snap, replayed,
            "the snapshot at {seq} is the durable state at {seq}"
        );
    }
}

#[tokio::test]
async fn the_index_is_pruned_before_a_snapshot_and_a_reload_judges_the_same() {
    let hooks = Hooks {
        index_keep: 5,
        ..Hooks::default()
    };
    let server =
        TestServer::start_custom(|s| s.snapshot_every = 5, |s| s.room_hooks = Arc::new(hooks))
            .await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    for n in 0..25 {
        a.change(vec![var(&format!("v{n}"), n)], vec![], vec![])
            .await
            .seq();
    }
    let deadline = tokio::time::Instant::now() + WAIT;
    while server.room_status(&doc).await.snapshot_seq < 25 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let memory = server.room(&doc).durable_head().await.unwrap();
    assert_eq!(memory.index.floor(), 20, "pruned to 25 - 5");
    // A change on a base below the floor is too old now.
    assert_eq!(
        a.change_on(3, vec![var("late", 1)]).await.code(),
        RejectCode::TooOld
    );
    a.close().await;
    let server = server.restart().await;
    let (loaded, _) = oswx_server::snapshot::load_head(
        server.state.store.as_ref(),
        server.state.blobs.as_ref(),
        &doc,
    )
    .await
    .unwrap();
    assert_eq!(loaded.seq, 25);
    assert_eq!(loaded.index.to_wire(), memory.index.to_wire());
}
