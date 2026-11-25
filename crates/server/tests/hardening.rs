//! What a hostile or broken client (or a failing database or storage) can do to live editing, and
//! what must not happen when it does. (A port of the connection and change tests of
//! `tests/test_hardening.py`; its lock, reservation, undo, rebuild-report and access-drop tests
//! arrive with Tasks 10, 11 and 12.)

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use futures_util::SinkExt;
use oswx_core::wire::{ChangeEvent, ClientMsg, Peer, RejectCode, ServerMsg};
use oswx_core::{Plain, Value};
use oswx_server::journal::{self, JournalWriter};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_tungstenite::tungstenite::Message;

async fn content(server: &TestServer, doc: &str, token: &str) -> serde_json::Value {
    let (status, body) = server
        .get(&format!("/api/documents/{doc}/content"), Some(token))
        .await;
    assert_eq!(status, 200, "{body}");
    body
}

async fn head_seq(server: &TestServer, doc: &str) -> i64 {
    let doc = server.state.store.document_by_id(doc).await.unwrap();
    doc.unwrap().head_seq
}

fn s(v: &str) -> Plain {
    Plain::from(v)
}

fn map(pairs: Vec<(&str, Plain)>) -> Plain {
    Plain::Map(pairs.into_iter().map(|(k, v)| (s(k), v)).collect())
}

fn pack(p: &Plain) -> Vec<u8> {
    let mut out = Vec::new();
    rmpv::encode::write_value(&mut out, p).unwrap();
    out
}

fn path_plain(segments: &[&str]) -> Plain {
    Plain::Array(segments.iter().map(|x| s(x)).collect())
}

/// A change frame built by hand, with whatever operations (as MessagePack) the test wants.
fn raw_change(change_id: &str, base: u64, ops: Vec<Plain>) -> Vec<u8> {
    pack(&map(vec![
        ("t", s("change")),
        ("change_id", s(change_id)),
        ("base_seq", Plain::from(base)),
        ("ops", Plain::Array(ops)),
    ]))
}

fn raw_set(segments: &[&str], value: Plain) -> Plain {
    map(vec![
        ("op", s("set")),
        ("path", path_plain(segments)),
        ("value", value),
    ])
}

/// The answer of a socket that has sent only a first frame: the `error` code and the close code.
async fn refusal_of(ws: &mut Socket) -> (String, u16) {
    let mut code = String::new();
    loop {
        let next = tokio::time::timeout(WAIT, futures_util::StreamExt::next(ws))
            .await
            .expect("an answer");
        match next {
            Some(Ok(Message::Binary(frame))) => {
                if let ServerMsg::Error { code: c, .. } =
                    oswx_core::wire::decode_server(&frame).unwrap()
                {
                    code = c;
                }
            }
            Some(Ok(Message::Close(frame))) => {
                return (code, frame.map_or(1005, |f| u16::from(f.code)));
            }
            Some(Ok(_)) => continue,
            Some(Err(_)) | None => return (code, 1006),
        }
    }
}

// ------------------------------------------------------------------ the handshake

#[tokio::test]
async fn a_first_frame_that_is_not_a_hello_is_refused() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let change = ClientMsg::Change(oswx_core::Change {
        change_id: Arc::from("x"),
        base: 0,
        ops: vec![set_depth(1)],
        reads: Default::default(),
        depends: Vec::new(),
        undo_of: None,
        command: Arc::from(""),
    });
    let firsts = [
        Message::Binary(oswx_core::wire::encode_client(&change)),
        Message::Binary(vec![0xc1, 0x00].into()),
        Message::Text(r#"{"type":"hello","protocol":2}"#.into()),
    ];
    for first in firsts {
        let mut ws = ws_connect(&server, &doc).await;
        ws.send(first.clone()).await.unwrap();
        assert_eq!(
            refusal_of(&mut ws).await,
            ("bad_hello".into(), 4400),
            "{first:?}"
        );
    }
    assert_eq!(head_seq(&server, &doc).await, 0);
}

#[tokio::test]
async fn a_hello_must_come_within_fifteen_seconds() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut ws = ws_connect(&server, &doc).await;
    let started = Instant::now();
    assert_eq!(refusal_of(&mut ws).await, ("bad_hello".into(), 4400));
    let waited = started.elapsed();
    assert!(waited >= Duration::from_secs(14), "{waited:?}");
}

#[tokio::test]
async fn another_protocol_version_is_refused() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let hello = Hello {
        protocol: 1,
        ..Hello::default()
    };
    let refused = Live::try_connect(&server, &doc, &world.admin, hello)
        .await
        .err()
        .unwrap();
    assert_eq!(refused.code(), "protocol");
    assert_eq!(refused.close, Some(4426));
    assert!(refused.error.unwrap().1.contains("protocol 2"));
}

#[tokio::test]
async fn a_text_frame_after_the_hello_is_answered_and_the_connection_kept() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    a.send_text(r#"{"type":"change"}"#).await;
    let err: ErrorMsg = a.expect().await;
    assert_eq!(err.code, "bad_message");
    // garbage after a valid hello: answered, kept
    a.send_raw(vec![0x92, 0x01]).await;
    assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
    // a second hello is read and ignored
    let hello = hello_frame(&world.admin, &Hello::default(), "another-client");
    a.send_raw(hello).await;
    assert_eq!(a.change(vec![set_depth(2)], vec![], vec![]).await.seq(), 1);
    assert!(a.seen.is_empty(), "{:?}", a.seen);
}

#[tokio::test]
async fn frames_are_bounded() {
    // The largest frame is max_change_bytes + 64 KiB; one byte more is read and refused with 1009,
    // anything longer is refused unread by the WebSocket layer (1002, which a client still sending
    // usually loses to the reset). Within the limit, what is too big for its kind is answered.
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let max = server.state.settings.max_change_bytes + 65536;
    let mut watcher = Live::connect(&server, &doc, &world.admin).await;

    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let _: Peer = watcher.expect().await;
    // A presence frame over 512 KiB: `too_large`, connection kept.
    let pad = "p".repeat(600 * 1024);
    let presence = pack(&map(vec![
        ("t", s("presence")),
        ("data", map(vec![("pad", s(&pad))])),
    ]));
    a.send_raw(presence).await;
    let err: ErrorMsg = a.expect().await;
    assert_eq!(err.code, "too_large");
    // A change of nearly the whole limit is read and judged.
    let big = "b".repeat(max - 200);
    let change = raw_change("big", 0, vec![raw_set(&["variables", "big"], s(&big))]);
    assert!(change.len() <= max && change.len() > max - 1000);
    a.send_raw(change).await;
    assert_eq!(a.outcome("big").await.seq(), 1);
    let ev: ChangeEvent = watcher.expect().await;
    assert_eq!(ev.seq, 1);

    // One byte over the limit: closed with 1009.
    let mut over = vec![0u8; max + 1];
    over[0] = 0x80;
    a.send_raw(over).await;
    assert_eq!(a.closed().await, (1009, "frame too large".into()));
    let left: PeerLeft = watcher.expect().await;
    assert_eq!(left.0, a.client_id);
    // Far over: the connection ends (the WebSocket layer does not read it).
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let mut far = vec![0u8; max + 100_000];
    far[0] = 0x80;
    b.send_raw_lossy(far).await;
    let (code, _) = b.closed().await;
    assert!(code == 1002 || code == 1006, "closed with {code}");
    assert_eq!(head_seq(&server, &doc).await, 1);
}

// ------------------------------------------------------------------ what a change may hold

#[tokio::test]
async fn nan_and_infinity_never_enter_a_document() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    for (n, bad) in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY]
        .into_iter()
        .enumerate()
    {
        let set = raw_set(&["features", "f2", "params", "depth"], Plain::F64(bad));
        a.send_raw(raw_change(&format!("n{n}"), 0, vec![set])).await;
        assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
        let deep = Plain::Array(vec![map(vec![("v", Plain::F64(bad))])]);
        let insert = map(vec![
            ("op", s("insert")),
            ("list", path_plain(&["features"])),
            (
                "item",
                map(vec![("id", s("x.y.1")), ("name", s("N1")), ("deep", deep)]),
            ),
            ("pos", s("V")),
        ]);
        a.send_raw(raw_change(&format!("m{n}"), 0, vec![insert]))
            .await;
        assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
    }
    assert_eq!(a.change_on(0, vec![set_depth(12.5)]).await.seq(), 1);
    let ev: ChangeEvent = b.expect().await;
    assert_eq!(ev.seq, 1, "nothing before it reached anyone");
    let late = Live::connect(&server, &doc, &world.admin).await;
    let doc_now = late.welcome.content.unwrap();
    assert_eq!(
        doc_now.at(&["features", "f2", "params", "depth"]),
        Some(&Value::Float(12.5))
    );
}

#[tokio::test]
async fn nesting_is_bounded() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut deep = map(vec![]);
    for _ in 0..80 {
        deep = map(vec![("n", deep)]);
    }
    a.send_raw(raw_change(
        "deep",
        0,
        vec![raw_set(&["variables", "deep"], deep)],
    ))
    .await;
    assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
    // ... nor by digging a little deeper each time
    let mut seq = 0;
    let mut at: Vec<String> = vec!["variables".into()];
    let mut refused = false;
    for _ in 0..12 {
        let mut nest = json!({});
        for _ in 0..9 {
            nest = json!({ "n": nest });
        }
        at.push("d".into());
        let segments: Vec<&str> = at.iter().map(String::as_str).collect();
        let change = a.make_change(seq, vec![set(&segments, nest)]);
        a.send_change(&change).await;
        match a.recv().await {
            ServerMsg::Accepted { seq: s, .. } => seq = s,
            ServerMsg::Rejected { code, .. } => {
                assert_eq!(code, RejectCode::Invalid);
                refused = true;
                break;
            }
            ServerMsg::Error { code, .. } => {
                assert_eq!(code, "bad_message");
                refused = true;
                break;
            }
            other => panic!("{other:?}"),
        }
        at.extend(std::iter::repeat_n("n".to_owned(), 9));
    }
    assert!(refused && seq >= 1);
    // The document still loads and still takes edits.
    let mut again = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(again.welcome.seq, seq);
    assert_eq!(
        again
            .change(vec![set_depth(123)], vec![], vec![])
            .await
            .seq(),
        seq + 1
    );
}

#[tokio::test]
async fn huge_or_pathological_ids_and_keys_cannot_break_a_document() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let long_id = format!("{}.{}", a.client_id, "7".repeat(5000));
    let bad_ops = [
        insert(&["features"], feature("placeholder", "Fillet1"), after_f2()),
        set(&["variables", &"k".repeat(201)], json!(1)),
    ];
    // An item id over 200 characters, and a key over 200 characters, never get past the wire.
    let mut long = bad_ops[0].to_wire();
    if let Plain::Map(pairs) = &mut long
        && let Some((_, Plain::Map(item))) =
            pairs.iter_mut().find(|(k, _)| k.as_str() == Some("item"))
        && let Some((_, id)) = item.iter_mut().find(|(k, _)| k.as_str() == Some("id"))
    {
        *id = s(&long_id);
    }
    a.send_raw(raw_change("long-id", 0, vec![long])).await;
    assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
    let long_key = raw_set(&["variables", &"k".repeat(201)], Plain::from(1));
    a.send_raw(raw_change("long-key", 0, vec![long_key])).await;
    assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
    // Long but legal names are fine.
    let (op, _) = a.add_feature(&format!("Part{}", "9".repeat(30)));
    assert_eq!(a.change_on(0, vec![op]).await.seq(), 1);
    a.close().await;
    let server = server.restart().await; // (a restart reloads from the journal)
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(b.welcome.seq, 1);
    assert_eq!(
        b.change(vec![set_depth(123)], vec![], vec![]).await.seq(),
        2
    );
}

#[tokio::test]
async fn extra_keys_on_an_operation_never_reach_the_journal_or_other_clients() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let with_junk = |depth: i64, junk: Plain| {
        let mut op = set_depth(depth).to_wire();
        if let Plain::Map(pairs) = &mut op {
            pairs.push((s("junk"), junk));
            pairs.push((s("more"), map(vec![("fine", Plain::from(1))])));
        }
        op
    };
    a.send_raw(raw_change("j1", 0, vec![with_junk(11, s("x"))]))
        .await;
    assert_eq!(a.outcome("j1").await.seq(), 1);
    assert_eq!(b.expect::<ChangeEvent>().await.ops, vec![set_depth(11)]);
    let mut nested = Plain::Nil;
    for _ in 0..20 {
        nested = Plain::Array(vec![nested]);
    }
    a.send_raw(raw_change("j2", 1, vec![with_junk(12, nested)]))
        .await;
    assert_eq!(a.outcome("j2").await.seq(), 2);
    assert_eq!(b.expect::<ChangeEvent>().await.ops, vec![set_depth(12)]);
    let bads = [
        map(vec![
            ("op", s("set")),
            ("path", Plain::from(7)),
            ("value", Plain::from(1)),
        ]),
        map(vec![
            ("op", s("move")),
            ("list", path_plain(&["features"])),
            ("id", s("f1")),
            ("pos", Plain::from(3)),
        ]),
        map(vec![("op", s("nope"))]),
    ];
    for (n, bad) in bads.into_iter().enumerate() {
        a.send_raw(raw_change(&format!("bad{n}"), 2, vec![bad]))
            .await;
        assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
    }
    // Catching up from the journal gives the clean operations.
    let late = Live::connect_with(&server, &doc, &world.admin, Hello::default().last_seq(0)).await;
    let ops: Vec<_> = late
        .welcome
        .changes
        .unwrap()
        .into_iter()
        .map(|c| c.ops)
        .collect();
    assert_eq!(ops, vec![vec![set_depth(11)], vec![set_depth(12)]]);
}

#[tokio::test]
async fn replacing_a_feature_under_its_own_id_is_still_a_conflict() {
    let (server, world) = world().await;
    let (bob, _) = world
        .invite(
            &server,
            "bob@example.com",
            Some("Bob"),
            Some("editor"),
            "member",
        )
        .await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(a.change_on(0, vec![set_depth(50)]).await.seq(), 1);
    let mut f2 = feature("f2", "Boss-Extrude1");
    f2["type"] = json!("extrude");
    f2["params"] = json!({"depth": 1});
    let swap = vec![
        remove(&["features"], "f2"),
        insert(&["features"], f2, after_f2()),
    ];
    // Built before Ada's edit: it would silently throw her edit away.
    let Outcome::Rejected {
        code, conflicts, ..
    } = b.change_on(0, swap.clone()).await
    else {
        panic!("expected a refusal")
    };
    assert_eq!(code, RejectCode::Stale);
    assert_eq!(&*conflicts[0].by, a.client_id.as_str());
    // With her edit seen, it is Bob's call.
    assert_eq!(b.change_on(1, swap).await.seq(), 2);
}

#[tokio::test]
async fn nobody_can_connect_as_someone_elses_client() {
    let (server, world) = world().await;
    let (bob, bob_id) = world
        .invite(
            &server,
            "bob@example.com",
            Some("Bob"),
            Some("editor"),
            "member",
        )
        .await;
    let (viewer, _) = world
        .invite(&server, "vi@example.com", None, Some("viewer"), "member")
        .await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    let known = a.make_change(0, vec![set_depth(20)]);
    assert_eq!(a.resend(&known).await.seq(), 1);
    // A viewer and an editor both try Ada's client id (which every peer can see).
    let v = Live::connect_with(&server, &doc, &viewer, Hello::client("ada-desktop")).await;
    let mut b = Live::connect_with(&server, &doc, &bob, Hello::client("ada-desktop")).await;
    assert_eq!(b.client_id, format!("{bob_id}.ada-desktop"));
    let ids = [&a.client_id, &b.client_id, &v.client_id];
    assert_eq!(
        ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
        3
    );
    a.drain().await; // Ada is still connected
    assert!(a.closed.is_none());
    // Her change id is hers: Bob using it is a new change of his own, not a replay of hers.
    let mut his = b.make_change(1, vec![set(&["features", "f1", "params", "x"], json!(1))]);
    his.change_id = known.change_id.clone();
    assert_eq!(b.resend(&his).await.seq(), 2);
    // Her ids are hers.
    let mine = format!("{}.1", a.client_id);
    let op = insert(&["features"], feature(&mine, "Mine1"), after_f2());
    assert_eq!(b.change_on(2, vec![op]).await.code(), RejectCode::Invalid);
    // And Bob gets no exemption from the conflict check for what Ada changed.
    assert_eq!(a.change_on(2, vec![set_depth(30)]).await.seq(), 3);
    assert_eq!(
        b.change_on(2, vec![set_depth(31)]).await.code(),
        RejectCode::Stale
    );
}

#[tokio::test]
async fn names_can_be_swapped_or_reused_within_one_change() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let ren = |id: &str, name: &str| set(&["features", id, "name"], json!(name));
    assert_eq!(
        a.change_on(0, vec![ren("f1", "Boss-Extrude1"), ren("f2", "Sketch1")])
            .await
            .seq(),
        1,
        "a swap, in one change"
    );
    let clash = a.change_on(1, vec![ren("f1", "Sketch1")]).await;
    assert_eq!(
        clash.code(),
        RejectCode::NameTaken,
        "no duplicates at the end of a change"
    );
    assert_eq!(
        a.change_on(1, vec![remove(&["features"], "f1")])
            .await
            .seq(),
        2
    );
    assert_eq!(
        a.change_on(2, vec![ren("f2", "Boss-Extrude1")]).await.seq(),
        3
    );
    let got = content(&server, &doc, &world.admin).await;
    assert_eq!(
        got["content"]["features"],
        json!([{"id": "f2", "name": "Boss-Extrude1",
        "type": "extrude", "suppressed": false, "params": {"sketch": "Sketch1", "depth": 10}}])
    );
}

// ------------------------------------------------------------------ slow consumers

#[tokio::test]
async fn a_reader_that_never_reads_is_dropped_before_it_costs_memory() {
    // (so the per-socket limit is 80 kB behind the frame being written)
    let server = TestServer::start_with(|s| s.max_change_bytes = 20000).await;
    let w = World::new(&server).await.unwrap();
    let doc = w.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &w.admin).await;
    let mut watcher = Live::connect(&server, &doc, &w.admin).await;
    let mut stuck = Live::connect_with(&server, &doc, &w.admin, Hello::client("never-reads")).await;
    let stuck_id = stuck.client_id.clone();
    let pad = "x".repeat(15000);
    let mut latencies = Vec::new();
    let mut dropped_after = None;
    for n in 0..3000 {
        let started = Instant::now();
        let outcome = a
            .change(
                vec![set(&["variables", "pad"], json!(format!("{n}{pad}")))],
                vec![],
                vec![],
            )
            .await;
        latencies.push(started.elapsed());
        assert!(matches!(outcome, Outcome::Accepted(_)));
        let status = server.room_status(&doc).await;
        if let Some(c) = status.connections.iter().find(|c| c.client_id == stuck_id) {
            assert!(
                c.queued_bytes <= 80_000 + 2 * 16_000,
                "{} bytes queued",
                c.queued_bytes
            );
        } else {
            dropped_after = Some(n);
            break;
        }
        watcher.drain().await;
    }
    let n = dropped_after.expect("the reader that never reads was dropped");
    // Everyone else carried on: the others' acknowledgements did not slow down while it was stuck.
    let mut sorted = latencies.clone();
    sorted.sort();
    let median = sorted[sorted.len() / 2];
    let worst = *sorted.last().unwrap();
    assert!(
        worst < Duration::from_secs(2) && median < Duration::from_millis(200),
        "median {median:?}, worst {worst:?} over {n} changes"
    );
    let left: PeerLeft = watcher.expect().await;
    assert_eq!(left.0, stuck_id);
    assert_eq!(
        a.change(vec![set_depth(7)], vec![], vec![]).await.seq() as usize,
        n + 2
    );
    // It reads again: what the socket held, then the close.
    assert_eq!(stuck.closed().await.0, 4408);
}

// ------------------------------------------------------------------ storage and database trouble

/// A change sent while the journal cannot commit: nobody is told it was accepted, every client is
/// told to reconnect, and after reconnecting the resend is answered with the seq it has (if the
/// commit happened after all) or written now.
async fn a_failed_batch(
    fail: impl Fn(&FlakyStore, bool),
    landed: bool,
    flaky: Arc<FlakyStore>,
    server: TestServer,
) {
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let _: Peer = a.expect().await;
    let change = a.make_change(0, vec![set_depth(77)]);
    fail(&flaky, true);
    a.send_change(&change).await;
    assert_eq!(a.closed().await.0, 4410, "told to reconnect");
    assert_eq!(b.closed().await.0, 4410);
    for m in a.seen.iter().chain(&b.seen) {
        assert!(
            !matches!(m, ServerMsg::Accepted { .. } | ServerMsg::Change(_)),
            "nobody is told it was accepted: {m:?}"
        );
    }
    fail(&flaky, false);
    assert_eq!(head_seq(&server, &doc).await, i64::from(landed));
    let mut a2 =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    assert_eq!(a2.welcome.seq, u64::from(landed));
    let lookups = flaky.change_lookups();
    assert_eq!(a2.resend(&change).await, Outcome::Accepted(1));
    // (Its id came with the client's recent ids when it joined: no lookup by change id.)
    assert_eq!(flaky.change_lookups(), lookups);
    assert_eq!(head_seq(&server, &doc).await, 1, "written once");
    // The next change works.
    assert_eq!(
        a2.change(vec![set_depth(78)], vec![], vec![]).await.seq(),
        2
    );
    let got = content(&server, &doc, &world.admin).await;
    assert_eq!(got["content"]["features"][1]["params"]["depth"], 78);
}

#[tokio::test]
async fn a_batch_that_fails_to_commit_tells_nobody_and_the_resend_is_written() {
    // Review Focus 2.
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    a_failed_batch(FlakyStore::fail_commit, false, flaky, server).await;
}

#[tokio::test]
async fn a_batch_that_commits_but_reports_an_error_is_found_on_resend() {
    // Review Focus 2: the commit took effect; the resend is answered with the original seq.
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    a_failed_batch(FlakyStore::commit_then_fail, true, flaky, server).await;
}

#[tokio::test]
async fn a_batch_that_times_out_tells_nobody_and_the_resend_is_written() {
    // Review Focus 2: a commit that never returns.
    let short = |store: &Arc<dyn oswx_server::store::Store>| {
        JournalWriter::start_with_timeout(
            store.clone(),
            journal::MAX_BATCH,
            journal::MAX_WAIT,
            Duration::from_millis(300),
        )
    };
    let (server, flaky) = TestServer::start_flaky_journal(|_| {}, short).await;
    a_failed_batch(FlakyStore::hang_commit, false, flaky, server).await;
}

#[tokio::test]
async fn a_room_that_lost_its_lease_stops_and_its_clients_reconnect() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    assert_eq!(a.change(vec![set_depth(1)], vec![], vec![]).await.seq(), 1);
    let epoch = server.room_status(&doc).await.epoch;
    // Someone else takes the document (another node, or a second load).
    let taken = oswx_server::lease::acquire(
        server.state.store.as_ref(),
        &doc,
        "another-node",
        server.state.clock.now(),
        60.0,
    )
    .await
    .unwrap();
    assert_eq!(taken, epoch + 1);
    let change = a.make_change(1, vec![set_depth(2)]);
    a.send_change(&change).await;
    assert_eq!(a.closed().await.0, 4410);
    assert!(
        a.seen
            .iter()
            .all(|m| !matches!(m, ServerMsg::Accepted { .. })),
        "{:?}",
        a.seen
    );
    assert_eq!(
        head_seq(&server, &doc).await,
        1,
        "the old room wrote nothing more"
    );
    // A fresh room takes the lease back, and the resend is written.
    let mut a2 =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    assert_eq!(server.room_status(&doc).await.epoch, taken + 1);
    assert_eq!(a2.resend(&change).await, Outcome::Accepted(2));
}

#[tokio::test]
async fn blobs_must_be_uploaded_before_they_are_referenced() {
    let (server, world) = world().await;
    let body = b"B".repeat(40000);
    let sha = hex::encode(Sha256::digest(&body));
    let (status, _) = upload(&server, &world, &sha, body).await;
    assert_eq!(status, 204);
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let missing = "0".repeat(64);
    let r = a
        .change_on(
            0,
            vec![set(
                &["features", "f2", "params", "brep"],
                json!({"$blob": missing, "len": 5}),
            )],
        )
        .await;
    let Outcome::Rejected { code, message, .. } = r else {
        panic!("expected a refusal")
    };
    assert_eq!(code, RejectCode::MissingBlob);
    assert!(message.contains(&missing), "{message}");
    let malformed = a
        .change_on(0, vec![set(&["variables", "b"], json!({"$blob": "zz"}))])
        .await;
    assert_eq!(malformed.code(), RejectCode::MissingBlob);
    assert_eq!(head_seq(&server, &doc).await, 0, "nothing was written");
    let ok = a
        .change_on(
            0,
            vec![set(
                &["features", "f2", "params", "brep"],
                json!({"$blob": sha, "len": 40000}),
            )],
        )
        .await;
    assert_eq!(ok.seq(), 1);
}

async fn upload(
    server: &TestServer,
    world: &World,
    sha: &str,
    body: Vec<u8>,
) -> (reqwest::StatusCode, String) {
    let r = reqwest::Client::new()
        .put(server.url(&format!("/api/projects/{}/blobs/{sha}", world.project)))
        .bearer_auth(&world.admin)
        .body(body)
        .send()
        .await
        .unwrap();
    (r.status(), r.text().await.unwrap())
}

#[tokio::test]
async fn storage_trouble_never_drops_the_connection_or_half_applies() {
    let (server, blobs) = TestServer::start_blobs(|s| s.snapshot_every = 2).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    blobs.fail_put(true);
    assert_eq!(a.change_on(0, vec![set_depth(1)]).await.seq(), 1);
    // A snapshot was due and failed: the change still stands.
    assert_eq!(a.change_on(1, vec![set_depth(2)]).await.seq(), 2);
    let heard: Vec<u64> = b
        .drain()
        .await
        .iter()
        .filter_map(|m| match m {
            ServerMsg::Change(ev) => Some(ev.seq),
            _ => None,
        })
        .collect();
    assert_eq!(heard, [1, 2]);
    blobs.fail_exists(true);
    let r = a
        .change_on(
            2,
            vec![set(
                &["features", "f2", "params", "body"],
                json!({"$blob": "a".repeat(64), "len": 9}),
            )],
        )
        .await;
    assert_eq!(r.code(), RejectCode::ServerError);
    assert_eq!(head_seq(&server, &doc).await, 2);
    blobs.fail_put(false);
    blobs.fail_exists(false);
    // Storage is back: the overdue snapshot is written after the next change.
    assert_eq!(a.change_on(2, vec![set_depth(3)]).await.seq(), 3);
    let deadline = tokio::time::Instant::now() + WAIT;
    while server.room_status(&doc).await.snapshot_seq != 3 {
        assert!(tokio::time::Instant::now() < deadline, "no snapshot at 3");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(a.closed.is_none() && b.closed.is_none());
}

#[tokio::test]
async fn connections_racing_to_open_a_document_share_one_room() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let connects = (0..8).map(|_| Live::connect(&server, &doc, &world.admin));
    let mut lives = futures_util::future::join_all(connects).await;
    assert_eq!(server.room_status(&doc).await.connections.len(), 8);
    assert_eq!(
        lives[0]
            .change(vec![set_depth(3)], vec![], vec![])
            .await
            .seq(),
        1
    );
    for live in &mut lives[1..] {
        assert_eq!(live.expect::<ChangeEvent>().await.seq, 1);
    }
    let epoch = server.room_status(&doc).await.epoch;
    assert_eq!(epoch, 1, "the document was leased once");
}
