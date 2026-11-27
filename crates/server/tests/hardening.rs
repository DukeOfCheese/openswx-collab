//! What a hostile or broken client (or a failing database or storage) can do to live editing, and
//! what must not happen when it does. (A port of the connection and change tests of
//! `tests/test_hardening.py`, and of its lock and presence tests; its undo, rebuild-report and
//! access-drop tests arrive with Tasks 11 and 12; v2 has no reservations.)

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

/// The status of one connection of the document.
async fn conn_status(
    server: &TestServer,
    doc: &str,
    client_id: &str,
) -> Option<oswx_server::room::ConnStatus> {
    server
        .room_status(doc)
        .await
        .connections
        .into_iter()
        .find(|c| c.client_id == client_id)
}

/// Big drafts from `flooders`, as fast as each may send them (each counting on from `n`), until
/// the connection `stuck` is behind on soft frames; its queue stays bounded meanwhile. With
/// `to_a_standstill`, until its socket takes nothing more either (it stays behind with the same
/// frames queued: a reader that is behind but whose socket still has room writes them, and is
/// caught up without reading).
async fn flood_until_behind(
    server: &TestServer,
    doc: &str,
    stuck: &str,
    flooders: &mut [Live],
    n: &mut u64,
    within: Duration,
    to_a_standstill: bool,
) {
    let pad = "x".repeat(120_000);
    let deadline = Instant::now() + within;
    let mut still = (0, 0);
    loop {
        *n += 1;
        for f in flooders.iter_mut() {
            f.draft(Some(json!({"n": *n, "pad": pad}))).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        for f in flooders.iter_mut() {
            f.drain().await;
        }
        let c = conn_status(server, doc, stuck)
            .await
            .expect("the slow reader is still there");
        assert!(
            c.queued_bytes <= 1024 * 1024 + 2 * 130_000,
            "{} bytes queued",
            c.queued_bytes
        );
        if c.behind && !to_a_standstill {
            return;
        }
        still = match (c.behind, c.queued_bytes) {
            (true, b) if b > 0 && b == still.1 => (still.0 + 1, b),
            (true, b) => (1, b),
            _ => (0, 0),
        };
        if still.0 >= 5 {
            return;
        }
        assert!(Instant::now() < deadline, "the reader never fell behind");
    }
}

/// A reader that does not keep up with presence and drafts (and who comes and goes) falls behind on
/// them instead of being closed: they are dropped for it, and once it has written everything it
/// had it is sent everyone as they are now, itself included, in one small list without drafts,
/// however large the drafts (here more than 1 MiB in all). Who joined while it was behind is in
/// it and who left is not. Then every peer's current draft follows, without it falling behind
/// again. What must arrive (a change) still does, in order.
#[tokio::test]
async fn a_reader_behind_is_caught_up_with_a_small_list_and_then_the_drafts() {
    let hooks = oswx_server::room::Hooks {
        // (Those that never read below stay.)
        frozen_after: Duration::from_secs(3600),
        ..Default::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut stuck =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("slow-reader")).await;
    let stuck_id = stuck.client_id.clone();
    // Ten that each leave a big draft and then never read (the list would be over 1 MiB with them).
    let big = |i: usize| json!({"sitter": i, "pad": "y".repeat(120_000)});
    let mut sitters = Vec::new();
    for i in 0..10 {
        let mut s = Live::connect(&server, &doc, &world.admin).await;
        s.draft(Some(big(i))).await;
        sitters.push(s);
    }
    let mut flooders = vec![
        Live::connect(&server, &doc, &world.admin).await,
        Live::connect(&server, &doc, &world.admin).await,
    ];
    let mut n = 0;
    flood_until_behind(
        &server,
        &doc,
        &stuck_id,
        &mut flooders,
        &mut n,
        WAIT * 3,
        true,
    )
    .await;
    let b_last = n;
    // More of them while it is behind cost it nothing.
    let pad = "x".repeat(120_000);
    for _ in 0..5 {
        n += 1;
        flooders[0].draft(Some(json!({"n": n, "pad": pad}))).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    flooders[1].presence(json!({"tool": "last"})).await;
    // One joins and one leaves while it is behind.
    let joiner = Live::connect(&server, &doc, &world.admin).await;
    let leaver = sitters.pop().unwrap();
    let leaver_id = leaver.client_id.clone();
    leaver.close().await;
    let seq = flooders[0]
        .change(vec![set_depth(3)], vec![], vec![])
        .await
        .seq();
    let c = conn_status(&server, &doc, &stuck_id)
        .await
        .expect("still there");
    assert!(
        c.behind && c.queued_bytes <= 1024 * 1024 + 3 * 130_000,
        "{c:?}"
    );
    // It reads again: what its socket held (lists of earlier catch-ups among it, while its
    // socket still had room), the change in its place, and then the peers as they are now; never
    // a close.
    let mut change_seen = false;
    let peers = loop {
        match stuck.recv().await {
            ServerMsg::Change(ev) => {
                assert_eq!(ev.seq, seq);
                change_seen = true;
            }
            ServerMsg::PeerList { peers } if change_seen => break peers,
            _ => {}
        }
    };
    let ids: Vec<&str> = peers.iter().map(|p| &*p.client_id).collect();
    assert_eq!(
        peers.len(),
        1 + 9 + 2 + 1,
        "everyone, itself included: {ids:?}"
    );
    assert!(
        ids.contains(&stuck_id.as_str()),
        "itself (it skips its own id)"
    );
    assert!(ids.contains(&joiner.client_id.as_str()), "who joined");
    assert!(!ids.contains(&leaver_id.as_str()), "not who left");
    assert!(peers.iter().all(|p| p.draft.is_none()), "no drafts");
    let peer = |id: &str| peers.iter().find(|p| &*p.client_id == id).unwrap().clone();
    assert_eq!(
        peer(&flooders[1].client_id).presence,
        Some(plain(json!({"tool": "last"})))
    );
    let list = oswx_core::wire::encode_server(&ServerMsg::PeerList {
        peers: peers.clone(),
    });
    assert!(list.len() < 16 * 1024, "{} bytes", list.len());
    // The drafts follow: every peer's current one.
    let mut want: std::collections::HashMap<String, Plain> = sitters
        .iter()
        .enumerate()
        .map(|(i, s)| (s.client_id.clone(), plain(big(i))))
        .collect();
    want.insert(
        flooders[0].client_id.clone(),
        plain(json!({"n": n, "pad": pad})),
    );
    want.insert(
        flooders[1].client_id.clone(),
        plain(json!({"n": b_last, "pad": pad})),
    );
    while !want.is_empty() {
        match stuck.recv().await {
            ServerMsg::Draft {
                client_id,
                data: Some(data),
            } => {
                if want.get(&*client_id) == Some(&data) {
                    want.remove(&*client_id);
                }
            }
            ServerMsg::PeerList { .. } => panic!("caught up again"),
            _ => {}
        }
    }
    assert!(stuck.closed.is_none());
    assert!(!conn_status(&server, &doc, &stuck_id).await.unwrap().behind);
    // And it takes presence again as it comes.
    let a_id = flooders[0].client_id.clone();
    flooders[0].presence(json!({"tool": "again"})).await;
    loop {
        let r: Relayed = stuck.expect().await;
        if r.kind == "presence" && r.client_id == a_id {
            assert_eq!(r.data, Some(plain(json!({"tool": "again"}))));
            break;
        }
    }
    assert!(!conn_status(&server, &doc, &stuck_id).await.unwrap().behind);
}

/// A reader behind is caught up at most once a period, even when it has written everything
/// again sooner.
#[tokio::test]
async fn a_reader_is_caught_up_at_most_once_a_period() {
    let every = Duration::from_secs(5);
    let hooks = oswx_server::room::Hooks {
        catch_up_every: every,
        ..Default::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut stuck =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("slow-reader")).await;
    let stuck_id = stuck.client_id.clone();
    let mut flooders = vec![
        Live::connect(&server, &doc, &world.admin).await,
        Live::connect(&server, &doc, &world.admin).await,
    ];
    let mut n = 0;
    flood_until_behind(
        &server,
        &doc,
        &stuck_id,
        &mut flooders,
        &mut n,
        WAIT * 3,
        false,
    )
    .await;
    while !matches!(stuck.recv().await, ServerMsg::PeerList { .. }) {}
    let first = Instant::now();
    // Behind again at once, and it reads everything it can.
    flood_until_behind(
        &server,
        &doc,
        &stuck_id,
        &mut flooders,
        &mut n,
        every / 2,
        false,
    )
    .await;
    let lists = stuck
        .drain()
        .await
        .iter()
        .filter(|m| matches!(m, ServerMsg::PeerList { .. }))
        .count();
    assert_eq!(lists, 0, "not caught up again yet");
    // It has written everything, and is still behind until the period is over.
    let c = conn_status(&server, &doc, &stuck_id).await.unwrap();
    assert!(
        first.elapsed() < every - Duration::from_secs(1),
        "too slow to tell"
    );
    assert!(c.behind, "{c:?}");
    while !matches!(stuck.recv().await, ServerMsg::PeerList { .. }) {}
    let gap = first.elapsed();
    assert!(
        gap >= every - Duration::from_millis(100),
        "caught up again after {gap:?}"
    );
    assert!(stuck.closed.is_none());
}

/// A reader that stops reading while presence and drafts keep coming is behind on them, never
/// too slow for what must arrive: it is closed once it has been behind a while with its socket
/// taking nothing, and leaves the others' peer lists.
#[tokio::test]
async fn a_reader_frozen_behind_is_closed_and_leaves() {
    let hooks = oswx_server::room::Hooks {
        frozen_after: Duration::from_secs(1),
        ..Default::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut stuck =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("frozen-one")).await;
    let stuck_id = stuck.client_id.clone();
    let mut flooders = vec![
        Live::connect(&server, &doc, &world.admin).await,
        Live::connect(&server, &doc, &world.admin).await,
    ];
    let mut n = 0;
    flood_until_behind(
        &server,
        &doc,
        &stuck_id,
        &mut flooders,
        &mut n,
        WAIT * 3,
        true,
    )
    .await;
    // Presence only from now on.
    let deadline = Instant::now() + Duration::from_secs(15);
    let left = 'wait: loop {
        flooders[0].presence(json!({"n": n})).await;
        n += 1;
        for m in flooders[1].drain().await {
            if let ServerMsg::PeerLeft { client_id } = m {
                break 'wait client_id;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the frozen reader was never closed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(&*left, stuck_id.as_str());
    assert!(conn_status(&server, &doc, &stuck_id).await.is_none());
    assert_eq!(stuck.closed().await.0, 4408);
}

/// A TCP stream that reads at most `rate` bytes a second (in bursts of at most 16 KiB), on a
/// socket with a small receive buffer: a client on a slow link.
struct Throttled {
    tcp: tokio::net::TcpStream,
    rate: f64,
    tokens: f64,
    at: tokio::time::Instant,
    sleep: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl Throttled {
    const BURST: f64 = 16.0 * 1024.0;

    async fn connect(server: &TestServer, rate: f64) -> Self {
        let addr: std::net::SocketAddr = server.base.trim_start_matches("http://").parse().unwrap();
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(8 * 1024).unwrap();
        let tcp = socket.connect(addr).await.unwrap();
        Self {
            tcp,
            rate,
            tokens: 0.0,
            at: tokio::time::Instant::now(),
            sleep: None,
        }
    }
}

impl tokio::io::AsyncRead for Throttled {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::future::Future;
        use std::task::Poll;
        loop {
            if let Some(sleep) = self.sleep.as_mut() {
                if sleep.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                self.sleep = None;
            }
            let now = tokio::time::Instant::now();
            let refill = now.duration_since(self.at).as_secs_f64() * self.rate;
            self.tokens = (self.tokens + refill).min(Self::BURST);
            self.at = now;
            if self.tokens < 1024.0 {
                let wait = (1024.0 - self.tokens) / self.rate;
                self.sleep = Some(Box::pin(tokio::time::sleep(Duration::from_secs_f64(wait))));
                continue;
            }
            let mut chunk = vec![0u8; (self.tokens as usize).min(buf.remaining())];
            let mut read = tokio::io::ReadBuf::new(&mut chunk);
            let this = &mut *self;
            match std::pin::Pin::new(&mut this.tcp).poll_read(cx, &mut read) {
                Poll::Ready(Ok(())) => {
                    this.tokens -= read.filled().len() as f64;
                    buf.put_slice(read.filled());
                    return Poll::Ready(Ok(()));
                }
                other => return other,
            }
        }
    }
}

impl tokio::io::AsyncWrite for Throttled {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.tcp).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.tcp).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.tcp).poll_shutdown(cx)
    }
}

/// What a slow reader got, and when: a message, or how the socket closed.
type Got = (Instant, Result<ServerMsg, (u16, String)>);

/// A client reading at `rate` bytes a second, joined with the welcome read: its client id, and
/// everything it reads after the welcome as it reads it.
async fn slow_reader(
    server: &TestServer,
    doc: &str,
    token: &str,
    rate: f64,
) -> (String, tokio::sync::mpsc::UnboundedReceiver<Got>) {
    use futures_util::StreamExt;
    let stream = Throttled::connect(server, rate).await;
    let (mut ws, _) = tokio_tungstenite::client_async(ws_url(server, doc), stream)
        .await
        .expect("the WebSocket opens");
    let hello = Hello::client("slow-link");
    ws.send(Message::Binary(
        hello_frame(token, &hello, "slow-link").into(),
    ))
    .await
    .unwrap();
    let decode = |frame: &[u8]| oswx_core::wire::decode_server(frame).expect("a valid frame");
    let welcome = loop {
        match tokio::time::timeout(WAIT, ws.next())
            .await
            .expect("a welcome")
        {
            Some(Ok(Message::Binary(frame))) => break decode(&frame),
            Some(Ok(_)) => continue,
            other => panic!("no welcome: {other:?}"),
        }
    };
    let ServerMsg::Welcome { you, .. } = welcome else {
        panic!("expected a welcome, got {welcome:?}");
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let got = match ws.next().await {
                Some(Ok(Message::Binary(frame))) => Ok(decode(&frame)),
                Some(Ok(Message::Close(frame))) => Err(frame.map_or((1005, String::new()), |f| {
                    (u16::from(f.code), f.reason.to_string())
                })),
                Some(Ok(_)) => continue,
                Some(Err(_)) | None => Err((1006, String::new())),
            };
            let end = got.is_err();
            if tx.send((Instant::now(), got)).is_err() || end {
                return;
            }
        }
    });
    (you.client_id.to_string(), rx)
}

/// A healthy reader on a slow link that is being written a big frame (here a change of 2 MiB at
/// 512 KiB/s, four times what it reads in `frozen_after`) while presence and drafts flood is
/// behind on those, but never frozen: the socket takes the frame as fast as the link goes. It
/// gets the change and then the peers, and is never closed.
#[tokio::test]
async fn a_slow_reader_taking_a_big_frame_while_presence_floods_is_not_frozen() {
    let after = Duration::from_secs(1);
    let hooks = oswx_server::room::Hooks {
        frozen_after: after,
        ..Default::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut writer = Live::connect(&server, &doc, &world.admin).await;
    let mut flooders = [
        Live::connect(&server, &doc, &world.admin).await,
        Live::connect(&server, &doc, &world.admin).await,
    ];
    let (slow_id, mut got) = slow_reader(&server, &doc, &world.admin, 512.0 * 1024.0).await;
    let big = "z".repeat(2 << 20);
    let sent = Instant::now();
    let seq = writer
        .change(
            vec![set(&["features", "f2", "params", "note"], json!(big))],
            vec![],
            vec![],
        )
        .await
        .seq();
    // Drafts flood while the change is being written to it: it falls behind on them.
    let pad = "x".repeat(120_000);
    let mut n = 0;
    let mut behind_late = false;
    while sent.elapsed() < 3 * after {
        n += 1;
        for f in flooders.iter_mut() {
            f.draft(Some(json!({"n": n, "pad": pad}))).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        for f in flooders.iter_mut().chain([&mut writer]) {
            f.drain().await;
        }
        let c = conn_status(&server, &doc, &slow_id)
            .await
            .expect("the slow reader is still there");
        behind_late |= c.behind && sent.elapsed() > 2 * after;
    }
    assert!(behind_late, "it never fell behind long enough to tell");
    // It reads the change, then the peers; never a close.
    let mut change_at = None;
    loop {
        let (at, msg) = tokio::time::timeout(WAIT, got.recv())
            .await
            .expect("the slow reader reads on")
            .expect("its socket is open");
        match msg {
            Ok(ServerMsg::Change(ev)) => {
                assert_eq!(ev.seq, seq);
                change_at = Some(at);
            }
            Ok(ServerMsg::PeerList { .. }) if change_at.is_some() => break,
            Ok(_) => {}
            Err(close) => panic!("the slow reader was closed: {close:?}"),
        }
    }
    let took = change_at.unwrap() - sent;
    assert!(
        took > 2 * after,
        "the change was read in {took:?}: too fast to tell"
    );
    assert!(conn_status(&server, &doc, &slow_id).await.is_some());
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
    // (Wait until that attempt has really failed: one still on its way to storage when storage
    // comes back would simply succeed, and the next snapshot would then be due at 4.)
    let deadline = tokio::time::Instant::now() + WAIT;
    while blobs.put_failures() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the snapshot at 2 was not tried"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
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

// ------------------------------------------------------------------ locks and presence

#[tokio::test]
async fn someone_elses_client_id_gets_none_of_their_locks() {
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
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    assert_eq!(a.lock(&[&["features", "f2"]]).await.granted.len(), 1);
    let mut b = Live::connect_with(&server, &doc, &bob, Hello::client("ada-desktop")).await;
    a.drain().await;
    assert!(a.closed.is_none(), "Ada is still connected");
    assert_eq!(b.welcome.locks.len(), 1);
    assert_eq!(&*b.welcome.locks[0].client_id, a.client_id.as_str());
    assert_eq!(
        b.change_on(0, vec![set_depth(1)]).await.code(),
        RejectCode::Locked
    );
    // Nor can Bob release them.
    b.unlock(&[&["features", "f2"]]).await;
    assert_eq!(
        b.change_on(0, vec![set_depth(1)]).await.code(),
        RejectCode::Locked
    );
}

fn raw_live(t: &str, data: Plain) -> Vec<u8> {
    pack(&map(vec![("t", s(t)), ("data", data)]))
}

#[tokio::test]
async fn presence_and_drafts_the_codec_refuses_are_dropped_and_the_connection_kept() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let big = |n: usize| map(vec![("pad", s(&"x".repeat(n)))]);
    let refused = [
        raw_live("presence", big(17 * 1024)),
        raw_live("draft", big(129 * 1024)),
        // (A frame over the 512 KiB any message but a change may be is answered `too_large`:
        // `frames_are_bounded`.)
        raw_live(
            "presence",
            map(vec![("pointer", Plain::Array(vec![Plain::F64(f64::NAN)]))]),
        ),
        raw_live("draft", map(vec![("x", Plain::F64(f64::INFINITY))])),
        raw_live("presence", map(vec![("x", Plain::Binary(vec![1, 2]))])),
        raw_live("presence", s("not a map")),
    ];
    for frame in refused {
        a.send_raw(frame).await;
    }
    let answers = a.drain().await;
    assert!(
        answers
            .iter()
            .all(|m| matches!(m, ServerMsg::PeerJoined(_))),
        "nothing answered: {answers:?}"
    );
    assert!(a.closed.is_none());
    a.draft(Some(json!({"x": 1}))).await;
    a.presence(json!({"pointer": [1, 2, 3]})).await;
    let mut got = Vec::new();
    while got.len() < 2 {
        match b.recv().await {
            ServerMsg::Presence { data, .. } => got.push(("presence", data)),
            ServerMsg::Draft { data: Some(d), .. } => got.push(("draft", d)),
            other => panic!("only what was valid comes: {other:?}"),
        }
    }
    assert_eq!(
        got,
        vec![
            ("draft", plain(json!({"x": 1}))),
            ("presence", plain(json!({"pointer": [1, 2, 3]})))
        ]
    );
    // What a later joiner sees was never poisoned.
    let late = Live::connect(&server, &doc, &world.admin).await;
    let ada = late
        .welcome
        .peers
        .iter()
        .find(|p| p.client_id.as_ref() == a.client_id)
        .unwrap();
    assert_eq!(ada.draft, Some(plain(json!({"x": 1}))));
    assert_eq!(ada.presence, Some(plain(json!({"pointer": [1, 2, 3]}))));
}

#[tokio::test]
async fn a_failed_snapshot_is_tried_again_without_waiting_for_another_change() {
    let (server, blobs) = TestServer::start_blobs(|s| {
        s.snapshot_every = 2;
        s.sweep_interval = 0.1;
    })
    .await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    blobs.fail_put(true);
    assert_eq!(a.change_on(0, vec![set_depth(1)]).await.seq(), 1);
    assert_eq!(a.change_on(1, vec![set_depth(2)]).await.seq(), 2);
    let deadline = tokio::time::Instant::now() + WAIT;
    while blobs.put_failures() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the snapshot at 2 was not tried"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Storage comes back, and nobody changes anything: the room still writes what it owes.
    blobs.fail_put(false);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while server.room_status(&doc).await.snapshot_seq != 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the owed snapshot was never written"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
