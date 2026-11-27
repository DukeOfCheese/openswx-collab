//! Live editing over the WebSocket: joining, changes and their broadcast, conflicts, resends,
//! catch-up, read-only connections, pipelining, snapshots, shutdown, presence and drafts. (A port
//! of `tests/test_live.py`; its lock tests are in `locks.rs`, its access tests arrive with Task 12.)

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::*;
use oswx_core::wire::{ChangeEvent, Peer, RejectCode, ServerMsg};
use oswx_core::{ConflictKind, Seq, Value};
use serde_json::json;

fn depth_of(doc: &Value) -> Option<&Value> {
    doc.at(&["features", "f2", "params", "depth"])
}

async fn content(server: &TestServer, doc: &str, token: &str) -> serde_json::Value {
    let (status, body) = server
        .get(&format!("/api/documents/{doc}/content"), Some(token))
        .await;
    assert_eq!(status, 200, "{body}");
    body
}

async fn head_seq(server: &TestServer, doc: &str) -> i64 {
    server
        .state
        .store
        .document_by_id(doc)
        .await
        .unwrap()
        .unwrap()
        .head_seq
}

#[tokio::test]
async fn join_change_and_broadcast() {
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
    assert_eq!(a.welcome.seq, 0);
    assert_eq!(a.welcome.role, "owner");
    assert!(a.welcome.peers.is_empty());
    assert_eq!(a.welcome.project_id, world.project);
    let content0 = a.welcome.content.clone().expect("the document");
    assert_eq!(depth_of(&content0), Some(&Value::Int(10)));

    let mut b = Live::connect(&server, &doc, &bob).await;
    let names: Vec<&str> = b.welcome.peers.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, ["Ada"]);
    assert_eq!(b.welcome.role, "editor");
    let joined: Peer = a.expect().await;
    assert_eq!(joined.name, "Bob");
    assert_ne!(a.welcome.you.colour, b.welcome.you.colour);

    assert_eq!(
        a.change(vec![set_depth(25)], vec![], vec![]).await,
        Outcome::Accepted(1)
    );
    let ev: ChangeEvent = b.expect().await;
    assert_eq!(
        (
            ev.seq,
            ev.author.as_str(),
            ev.author_name.as_str(),
            &*ev.command
        ),
        (1, world.admin_id.as_str(), "Ada", "edit")
    );
    assert_eq!(ev.ops, vec![set_depth(25)]);
    assert_eq!(&*ev.client_id, a.client_id.as_str());
    // the author gets the acknowledgement, not an echo
    let rest = a.drain().await;
    assert!(rest.iter().all(|m| !matches!(m, ServerMsg::Change(_))));

    let (op, id) = b.add_feature("Fillet1");
    assert_eq!(b.change(vec![op], vec![], vec![]).await.seq(), 2);
    assert_eq!(a.expect::<ChangeEvent>().await.seq, 2);
    let got = content(&server, &doc, &bob).await;
    assert_eq!(got["seq"], 2);
    let ids: Vec<&str> = got["content"]["features"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["f1", "f2", &*id]);
    assert_eq!(got["content"]["features"][1]["params"]["depth"], 25);
    a.close().await;
    b.close().await;
    let (_, body) = server
        .get(&format!("/api/documents/{doc}"), Some(&bob))
        .await;
    assert_eq!(body["seq"], 2);
}

#[tokio::test]
async fn who_may_connect() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let (outsider, _) = world
        .invite(&server, "out@example.com", None, None, "member")
        .await;
    let refused = |r: Result<Live, Refused>| match r {
        Ok(_) => panic!("the connection was accepted"),
        Err(refused) => refused,
    };
    let bad_token =
        refused(Live::try_connect(&server, &doc, "not a token", Hello::default()).await);
    assert_eq!(bad_token.code(), "unauthorized");
    assert_eq!(bad_token.close, Some(4401));
    let no_role = refused(Live::try_connect(&server, &doc, &outsider, Hello::default()).await);
    let missing = refused(Live::try_connect(&server, "nope", &world.admin, Hello::default()).await);
    let deleted_id = "0".repeat(32);
    let unknown =
        refused(Live::try_connect(&server, &deleted_id, &world.admin, Hello::default()).await);
    // A document you cannot open is refused exactly as one that does not exist.
    assert_eq!(no_role, missing);
    assert_eq!(no_role, unknown);
    assert_eq!(no_role.code(), "not_found");
    assert_eq!(no_role.close, Some(4404));
    // A deleted document too.
    let (status, _) = server
        .delete(&format!("/api/documents/{doc}"), Some(&world.admin))
        .await;
    assert_eq!(status, 204);
    let deleted = refused(Live::try_connect(&server, &doc, &world.admin, Hello::default()).await);
    assert_eq!(deleted, missing);
}

#[tokio::test]
async fn viewers_and_other_file_formats_are_read_only() {
    let (server, world) = world().await;
    let (viewer, _) = world
        .invite(&server, "vi@example.com", None, Some("viewer"), "member")
        .await;
    let doc = world.document(&server, None, None, None).await;
    let mut v = Live::connect(&server, &doc, &viewer).await;
    assert!(v.welcome.read_only);
    assert!(
        v.welcome
            .read_only_reason
            .as_deref()
            .unwrap()
            .contains("view access")
    );
    assert_eq!(v.welcome.role, "viewer");
    let refused = v.change(vec![set_depth(1)], vec![], vec![]).await;
    assert_eq!(refused.code(), RejectCode::ReadOnly);
    let old = Hello::default().format_version(Some(2));
    let mut o = Live::connect_with(&server, &doc, &world.admin, old).await;
    assert!(o.welcome.read_only);
    let reason = o.welcome.read_only_reason.clone().unwrap();
    assert!(
        reason.contains("update openswx") && reason.contains("format 3"),
        "{reason}"
    );
    assert_eq!(
        o.change(vec![set_depth(1)], vec![], vec![]).await.code(),
        RejectCode::ReadOnly
    );
    let none = Hello::default().format_version(None);
    let n = Live::connect_with(&server, &doc, &world.admin, none).await;
    assert!(n.welcome.read_only, "no format version is another format");
    assert_eq!(content(&server, &doc, &viewer).await["seq"], 0);
}

#[tokio::test]
async fn stale_change_is_rejected_but_unrelated_ones_merge() {
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
    assert_eq!(a.change_on(0, vec![set_depth(20)]).await.seq(), 1);
    // Bob has not seen change 1 and edits the same value: refused, and told where and by whom.
    let r = b.change_on(0, vec![set_depth(40)]).await;
    let Outcome::Rejected {
        code, conflicts, ..
    } = r
    else {
        panic!("expected a refusal")
    };
    assert_eq!(code, RejectCode::Stale);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(
        conflicts[0].path,
        path(&["features", "f2", "params", "depth"])
    );
    assert_eq!(
        (conflicts[0].seq, &*conflicts[0].by),
        (1, a.client_id.as_str())
    );
    assert_eq!(conflicts[0].kind, ConflictKind::Written);
    // ... a different feature from the same old base is fine
    let sketch = set(
        &["features", "f1", "params", "sketch", "points", "P2"],
        json!([5, 5]),
    );
    assert_eq!(b.change_on(0, vec![sketch]).await.seq(), 2);
    // ... and so are top-level values nobody else touched
    let steel = set(&["material", "name"], json!("Steel"));
    assert_eq!(b.change_on(0, vec![steel]).await.seq(), 3);
    // v2: a sibling value is not a conflict (v1 refused the whole `material` key)
    let density = set(&["material", "density"], json!(7850));
    assert_eq!(a.change_on(0, vec![density]).await.seq(), 4);
    // ... but the map someone wrote inside is
    let replace = set(&["material"], json!({"name": "Oak"}));
    assert_eq!(
        a.change_on(2, vec![replace]).await.code(),
        RejectCode::Stale
    );
    let got = content(&server, &doc, &bob).await["content"].clone();
    assert_eq!(got["features"][1]["params"]["depth"], 20);
    assert_eq!(
        got["features"][0]["params"]["sketch"]["points"]["P2"],
        json!([5, 5])
    );
    assert_eq!(got["material"], json!({"name": "Steel", "density": 7850}));
}

#[tokio::test]
async fn own_changes_in_a_row_do_not_conflict_with_each_other() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    // Three sketch lines before the first acknowledgement arrives: all built on base 0.
    let mut ids = Vec::new();
    for n in 0..3 {
        let op = set(
            &[
                "features",
                "f1",
                "params",
                "sketch",
                "entities",
                &format!("Line{n}"),
            ],
            json!({"n": n}),
        );
        let change = a.make_change(0, vec![op]);
        a.send_change(&change).await;
        ids.push(change.change_id);
    }
    let mut seqs = Vec::new();
    for id in &ids {
        seqs.push(a.outcome(id).await.seq());
    }
    assert_eq!(seqs, [1, 2, 3]);
    let got = content(&server, &doc, &world.admin).await;
    let entities = &got["content"]["features"][0]["params"]["sketch"]["entities"];
    assert_eq!(entities.as_object().unwrap().len(), 3);
}

#[tokio::test]
async fn invalid_changes_leave_the_document_alone() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let dup = insert(&["features"], feature("f2", "Dup1"), after_f2());
    let mine = a.new_id();
    let cases = [
        // nothing there to set into
        (
            vec![set(&["features", "gone", "params", "depth"], json!(1))],
            RejectCode::Invalid,
        ),
        // an id the list already has
        (vec![dup], RejectCode::IdTaken),
        // a name another feature has
        (
            vec![insert(&["features"], feature(&mine, "Sketch1"), after_f2())],
            RejectCode::NameTaken,
        ),
        // an id that is not this client's
        (
            vec![insert(
                &["features"],
                feature("someone.else.1", "Mine1"),
                after_f2(),
            )],
            RejectCode::Invalid,
        ),
    ];
    for (ops, code) in cases {
        assert_eq!(a.change_on(0, ops.clone()).await.code(), code, "{ops:?}");
    }
    // a base that does not exist yet
    assert_eq!(
        a.change_on(5, vec![set_depth(1)]).await.code(),
        RejectCode::Invalid
    );
    // what the wire refuses is answered as a bad message
    let mut frame = rmpv_map(vec![("t", "change".into()), ("change_id", "x".into())]);
    frame.push(0xc1);
    a.send_raw(frame).await;
    assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
    a.send_raw(rmpv_map(vec![("t", "no-such-type".into())]))
        .await;
    assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
    a.send_text("not msgpack").await;
    assert_eq!(a.expect::<ErrorMsg>().await.code, "bad_message");
    // the connection survives, and nothing moved
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 0);
    assert_eq!(a.change_on(0, vec![set_depth(11)]).await.seq(), 1);
}

/// A MessagePack map of string keys.
fn rmpv_map(pairs: Vec<(&str, rmpv::Value)>) -> Vec<u8> {
    let map = rmpv::Value::Map(pairs.into_iter().map(|(k, v)| (k.into(), v)).collect());
    let mut out = Vec::new();
    rmpv::encode::write_value(&mut out, &map).unwrap();
    out
}

#[tokio::test]
async fn new_ids_belong_to_their_client_and_names_stay_unique() {
    // (v2 replaces `test_ids_and_names_come_from_the_server`: there is no id reservation.)
    let (server, world) = world().await;
    let (bob, _) = world.editor(&server, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    // Both create from the same base: no collision, both accepted.
    let (fa, ida) = a.add_feature("Sketch2");
    let (fb, idb) = b.add_feature("Sketch3");
    assert_eq!(a.change_on(0, vec![fa]).await.seq(), 1);
    assert_eq!(b.change_on(0, vec![fb]).await.seq(), 2);
    // Nobody can make an item under another client's prefix.
    let theirs = format!("{}.99", a.client_id);
    let op = insert(&["features"], feature(&theirs, "Whatever1"), after_f2());
    assert_eq!(b.change_on(2, vec![op]).await.code(), RejectCode::Invalid);
    // A name taken at commit is refused, so the client can rename and resend.
    let (clash, _) = b.add_feature("Sketch2");
    let refused = b.change_on(2, vec![clash]).await;
    assert_eq!(refused.code(), RejectCode::NameTaken);
    let got = content(&server, &doc, &bob).await;
    let feats: Vec<(String, String)> = got["content"]["features"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                f["id"].as_str().unwrap().to_owned(),
                f["name"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    let names: BTreeSet<&str> = feats.iter().map(|(_, n)| n.as_str()).collect();
    assert_eq!(
        names,
        BTreeSet::from(["Sketch1", "Boss-Extrude1", "Sketch2", "Sketch3"])
    );
    assert!(feats.iter().any(|(id, _)| id == &*ida));
    assert!(feats.iter().any(|(id, _)| id == &*idb));
}

#[tokio::test]
async fn resending_a_change_is_harmless() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("desktop-ada-1")).await;
    let change = a.make_change(0, vec![set_depth(20)]);
    assert_eq!(a.resend(&change).await.seq(), 1);
    assert_eq!(
        a.resend(&change).await.seq(),
        1,
        "same answer, not applied twice"
    );
    assert_eq!(head_seq(&server, &doc).await, 1);
    a.close().await;
    // (Review Focus 1) The server restarts: the journal remembers the change id.
    let server = server.restart().await;
    let hello = Hello::client("desktop-ada-1").last_seq(1);
    let mut a2 = Live::connect_with(&server, &doc, &world.admin, hello).await;
    assert_eq!(a2.welcome.changes.as_deref(), Some(&[][..]));
    assert_eq!(a2.welcome.seq, 1);
    assert_eq!(a2.resend(&change).await, Outcome::Accepted(1));
    assert_eq!(head_seq(&server, &doc).await, 1);
    let next = a2.make_change(1, vec![set_depth(21)]);
    assert_eq!(a2.resend(&next).await.seq(), 2);
    // Another client's change with the same id is its own change.
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let mut same_id = b.make_change(2, vec![set(&["variables", "x"], json!(1))]);
    same_id.change_id = change.change_id.clone();
    assert_eq!(b.resend(&same_id).await.seq(), 3);
}

#[tokio::test]
async fn reconnect_catches_up() {
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
    let a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let ada = a.client_id.clone();
    a.close().await; // Ada's wifi drops
    let left: PeerLeft = b.expect().await;
    assert_eq!(left.0, format!("{}.ada-desktop", world.admin_id));
    assert_eq!(b.change(vec![set_depth(15)], vec![], vec![]).await.seq(), 1);
    let (op, _) = b.add_feature("Fillet1");
    assert_eq!(b.change(vec![op.clone()], vec![], vec![]).await.seq(), 2);
    let hello = Hello::client("ada-desktop").last_seq(0);
    let mut a2 = Live::connect_with(&server, &doc, &world.admin, hello).await;
    assert_eq!(a2.client_id, ada);
    assert!(a2.welcome.content.is_none());
    let changes = a2.welcome.changes.clone().unwrap();
    assert_eq!(changes.iter().map(|c| c.seq).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(changes[0].ops, vec![set_depth(15)]);
    assert_eq!(changes[0].author_name, "Bob");
    assert_eq!(changes[1].ops, vec![op]);
    // What she drew while offline goes in on top of the server's head ...
    let sketch = set(
        &["features", "f1", "params", "sketch", "points", "P9"],
        json!([1, 2]),
    );
    assert_eq!(a2.change_on(0, vec![sketch]).await.seq(), 3);
    // ... and what she did offline to something Bob changed meanwhile is refused, naming Bob.
    let Outcome::Rejected {
        code, conflicts, ..
    } = a2.change_on(0, vec![set_depth(77)]).await
    else {
        panic!("expected a refusal")
    };
    assert_eq!(code, RejectCode::Stale);
    assert_eq!(&*conflicts[0].by, b.client_id.as_str());
}

#[tokio::test]
async fn far_behind_or_unknown_position_gets_the_whole_document() {
    let server = TestServer::start_with(|s| s.catchup_limit = 2).await;
    let w = World::new(&server).await.unwrap();
    let doc = w.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &w.admin).await;
    for n in 0..4 {
        assert_eq!(a.change_on(n, vec![set_depth(n + 1)]).await.seq(), n + 1);
    }
    let caught = Live::connect_with(&server, &doc, &w.admin, Hello::default().last_seq(2)).await;
    let seqs: Vec<Seq> = caught
        .welcome
        .changes
        .unwrap()
        .iter()
        .map(|c| c.seq)
        .collect();
    assert_eq!(seqs, [3, 4]);
    let late = Live::connect_with(&server, &doc, &w.admin, Hello::default().last_seq(1)).await;
    assert!(late.welcome.changes.is_none());
    assert_eq!(
        depth_of(&late.welcome.content.unwrap()),
        Some(&Value::Int(4))
    );
    let ahead = Live::connect_with(&server, &doc, &w.admin, Hello::default().last_seq(99)).await;
    assert!(ahead.welcome.content.is_some());
    let now = Live::connect_with(&server, &doc, &w.admin, Hello::default().last_seq(4)).await;
    assert_eq!(now.welcome.changes.as_deref(), Some(&[][..]));
}

#[tokio::test]
async fn two_clients_of_one_user_conflict_but_one_client_never_with_itself() {
    // Review Focus 3.
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut laptop =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("laptop-1")).await;
    let mut desk =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("desktop-1")).await;
    assert_eq!(laptop.change_on(0, vec![set_depth(1)]).await.seq(), 1);
    let refused = desk.change_on(0, vec![set_depth(2)]).await;
    assert_eq!(
        refused.code(),
        RejectCode::Stale,
        "another client of the same user"
    );
    laptop.close().await;
    // The same client reconnecting does not conflict with what it did before.
    let mut again =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("laptop-1")).await;
    assert_eq!(again.change_on(0, vec![set_depth(3)]).await.seq(), 2);
    assert_eq!(again.change_on(0, vec![set_depth(4)]).await.seq(), 3);
}

#[tokio::test]
async fn a_second_connection_of_a_client_replaces_the_first() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut watcher = Live::connect(&server, &doc, &world.admin).await;
    let mut first =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("same-client")).await;
    let _: Peer = watcher.expect().await;
    let mut second =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("same-client")).await;
    assert_eq!(
        first.closed().await,
        (4409, "connected again from the same client".into())
    );
    assert_eq!(first.client_id, second.client_id);
    // The replaced connection's leaving is not announced; the new one joins.
    let joined: Peer = watcher.expect().await;
    assert_eq!(&*joined.client_id, second.client_id.as_str());
    let rest = watcher.drain().await;
    assert!(
        rest.iter()
            .all(|m| !matches!(m, ServerMsg::PeerLeft { .. })),
        "{rest:?}"
    );
    assert_eq!(
        second
            .change(vec![set_depth(5)], vec![], vec![])
            .await
            .seq(),
        1
    );
    assert_eq!(server.room_status(&doc).await.connections.len(), 2);
}

#[tokio::test]
async fn a_client_can_send_many_changes_without_waiting() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut changes = Vec::new();
    for n in 0..50 {
        let change = a.make_change(0, vec![set_depth(n)]);
        a.send_change(&change).await;
        changes.push(change);
    }
    let mut seqs = Vec::new();
    for change in &changes {
        seqs.push(a.outcome(&change.change_id).await.seq());
    }
    assert_eq!(
        seqs,
        (1..=50).collect::<Vec<Seq>>(),
        "in order, consecutive"
    );
    assert_eq!(head_seq(&server, &doc).await, 50);
    assert!(a.seen.is_empty(), "nothing but the answers: {:?}", a.seen);
}

#[tokio::test]
async fn a_client_joining_during_a_burst_gets_every_change_once() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut changes = Vec::new();
    for n in 0..200 {
        let change = a.make_change(0, vec![set_depth(n)]);
        a.send_change(&change).await;
        changes.push(change);
        if n == 60 {
            break;
        }
    }
    // Join while the rest are being sent and written.
    let joining = Live::connect(&server, &doc, &world.admin);
    let sending = async {
        for n in 61..200 {
            let change = a.make_change(0, vec![set_depth(n)]);
            a.send_change(&change).await;
            changes.push(change);
        }
    };
    let (mut b, ()) = tokio::join!(joining, sending);
    for change in &changes {
        a.outcome(&change.change_id).await.seq();
    }
    let mut seen: Vec<Seq> = Vec::new();
    while b.seq < 200 {
        if let ServerMsg::Change(ev) = b.recv().await {
            seen.push(ev.seq);
        }
    }
    let expected: Vec<Seq> = (b.welcome.seq + 1..=200).collect();
    assert_eq!(seen, expected, "welcome at {}", b.welcome.seq);
    assert_eq!(
        depth_of(&server.room(&doc).durable_head().await.unwrap().doc),
        Some(&Value::Int(199))
    );
}

#[tokio::test]
async fn content_comes_from_the_open_room_and_from_the_store_after() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.change(vec![set_depth(6)], vec![], vec![]).await.seq(), 1);
    assert_eq!(a.change(vec![set_depth(7)], vec![], vec![]).await.seq(), 2);
    let open = content(&server, &doc, &world.admin).await;
    assert_eq!(
        (open["seq"].clone(), open["document"]["seq"].clone()),
        (json!(2), json!(2))
    );
    assert_eq!(open["content"]["features"][1]["params"]["depth"], 7);
    a.close().await;
    let server = server.restart().await;
    assert!(
        server.state.hub.loaded(&doc).is_none(),
        "nothing opened it yet"
    );
    let closed = content(&server, &doc, &world.admin).await;
    assert_eq!(closed["content"], open["content"]);
    assert_eq!(closed["seq"], 2);
    assert!(
        server.state.hub.loaded(&doc).is_none(),
        "reading content opens no room"
    );
}

#[tokio::test]
async fn a_snapshot_is_written_every_so_many_changes_and_loading_starts_there() {
    let server = TestServer::start_with(|s| s.snapshot_every = 10).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    for n in 0..25 {
        assert_eq!(
            a.change(vec![set_depth(n)], vec![], vec![]).await.seq(),
            n + 1
        );
    }
    let deadline = tokio::time::Instant::now() + WAIT;
    let snap = loop {
        let status = server.room_status(&doc).await;
        if status.snapshot_seq >= 20 {
            break status.snapshot_seq;
        }
        assert!(tokio::time::Instant::now() < deadline, "no snapshot at 20");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let row = server
        .state
        .store
        .snapshot_at_or_before(&doc, 25)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.seq as Seq, snap);
    // Without the journal before it, the document still loads: from the snapshot.
    server
        .sql(&format!(
            "DELETE FROM changes WHERE document_id = '{doc}' AND seq <= {snap}"
        ))
        .await;
    let room_head = server.room(&doc).durable_head().await.unwrap();
    let (loaded, _) = oswx_server::snapshot::load_head(
        server.state.store.as_ref(),
        server.state.blobs.as_ref(),
        &doc,
    )
    .await
    .unwrap();
    assert_eq!(loaded.seq, 25);
    assert_eq!(loaded.doc, room_head.doc);
    assert_eq!(loaded.index.to_wire(), room_head.index.to_wire());
    a.close().await;
    let server = server.restart().await;
    let got = content(&server, &doc, &world.admin).await;
    assert_eq!(got["seq"], 25);
    assert_eq!(got["content"]["features"][1]["params"]["depth"], 24);
}

#[tokio::test]
async fn shutdown_closes_sockets_after_what_was_accepted_is_durable() {
    let (mut server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    for n in 0..5 {
        assert_eq!(
            a.change(vec![set_depth(n)], vec![], vec![]).await.seq(),
            n + 1
        );
    }
    // More in flight as the server stops: whatever is acknowledged is durable.
    let mut changes = Vec::new();
    for n in 0..20 {
        let change = a.make_change(5, vec![set(&["variables", &format!("v{n}")], json!(n))]);
        a.send_change(&change).await;
        changes.push(change);
    }
    server.stop().await;
    let (code, _) = a.closed().await;
    assert_eq!(code, 1001);
    assert_eq!(b.closed().await.0, 1001);
    let acked: Vec<Seq> = a
        .seen
        .iter()
        .filter_map(|m| match m {
            ServerMsg::Accepted { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect();
    assert_eq!(acked, (6..6 + acked.len() as Seq).collect::<Vec<_>>());
    let durable = head_seq(&server, &doc).await;
    assert!(durable >= 5 + acked.len() as i64, "{durable} {acked:?}");
    // A snapshot of the durable head was written as the room closed.
    let row = server
        .state
        .store
        .snapshot_at_or_before(&doc, durable)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.seq, durable);
}

// ------------------------------------------------------------------ presence and drafts

fn relayed(msgs: &[ServerMsg]) -> Vec<Relayed> {
    msgs.iter()
        .filter_map(|m| <Relayed as FromServer>::pick(m.clone()).ok())
        .collect()
}

#[tokio::test]
async fn presence_and_drafts_are_relayed_not_stored() {
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
    let (viewer, _) = world
        .invite(
            &server,
            "vi@example.com",
            Some("Vi"),
            Some("viewer"),
            "member",
        )
        .await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let pointer = json!({"pointer": {"point": [1, 2, 3], "face": "Boss-Extrude1:cap+"},
                         "tool": "fillet", "selection": ["Boss-Extrude1:cap+"],
                         "camera": {"eye": [0, 0, 100]}});
    a.presence(pointer.clone()).await;
    let got: Relayed = b.expect().await;
    assert_eq!(
        (got.kind, got.client_id.as_str(), got.data),
        (
            "presence",
            a.client_id.as_str(),
            Some(plain(pointer.clone()))
        )
    );
    let sketch = json!({"sketch": "f1", "entities": {"Line9": {"p1": [0, 0], "p2": [4, 4]}}});
    a.draft(Some(sketch.clone())).await;
    let got: Relayed = b.expect().await;
    assert_eq!((got.kind, got.data), ("draft", Some(plain(sketch.clone()))));
    assert!(relayed(&a.drain().await).is_empty(), "no echo");

    // Someone who joins later sees where everyone is and what they are drawing.
    let mut v = Live::connect(&server, &doc, &viewer).await;
    let ada = v.welcome.peers.iter().find(|p| p.name == "Ada").unwrap();
    assert_eq!(ada.presence, Some(plain(pointer)));
    assert_eq!(ada.draft, Some(plain(sketch)));
    assert!(!ada.read_only);
    // Viewers have a cursor too ...
    v.presence(json!({"tool": "measure"})).await;
    let got: Relayed = b.expect().await;
    assert_eq!(got.data, Some(plain(json!({"tool": "measure"}))));
    // ... but nothing to preview.
    v.draft(Some(json!({"x": 1}))).await;
    v.drain().await;
    a.draft(None).await;
    let got: Relayed = b.expect().await;
    assert_eq!(
        (got.kind, got.client_id.as_str(), got.data),
        ("draft", a.client_id.as_str(), None)
    );
    assert!(
        b.seen
            .iter()
            .all(|m| !matches!(m, ServerMsg::Draft { data: Some(d), .. }
        if *d == plain(json!({"x": 1}))))
    );
    let late = Live::connect(&server, &doc, &bob).await;
    let ada = late.welcome.peers.iter().find(|p| p.name == "Ada").unwrap();
    assert_eq!(ada.draft, None, "cleared");
    let vi = late.welcome.peers.iter().find(|p| p.name == "Vi").unwrap();
    assert!(vi.read_only);
    assert_eq!(vi.draft, None);
    assert_eq!(head_seq(&server, &doc).await, 0, "nothing was stored");
}

#[tokio::test]
async fn the_61st_presence_in_a_second_is_dropped() {
    let clock = oswx_server::clock::Clock::manual(1_800_000_000.0);
    let c = clock.clone();
    let server = TestServer::start_custom(|_| {}, move |s| s.clock = c).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    for n in 0..61 {
        if n % 2 == 0 {
            a.presence(json!({"n": n})).await;
        } else {
            a.draft(Some(json!({"n": n}))).await;
        }
    }
    a.drain().await;
    // (Relayed at the next tick, each kind's latest.)
    tokio::time::sleep(Duration::from_millis(200)).await;
    let got = relayed(&b.drain().await);
    let last = |kind: &str| {
        got.iter()
            .rev()
            .find(|r| r.kind == kind)
            .unwrap()
            .data
            .clone()
    };
    assert_eq!(last("presence"), Some(plain(json!({"n": 58}))));
    assert_eq!(last("draft"), Some(plain(json!({"n": 59}))));
    assert!(
        got.iter().all(|r| r.data != Some(plain(json!({"n": 60})))),
        "presence and drafts share the allowance: the 61st is dropped"
    );
    a.presence(json!({"n": "late"})).await;
    a.drain().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(relayed(&b.drain().await).is_empty());
    clock.advance(1.0);
    a.presence(json!({"n": "next second"})).await;
    let got: Relayed = b.expect().await;
    assert_eq!(got.data, Some(plain(json!({"n": "next second"}))));
}

#[tokio::test]
async fn a_presence_flood_is_dropped() {
    let clock = oswx_server::clock::Clock::manual(1_800_000_000.0);
    let c = clock.clone();
    let server = TestServer::start_custom(|s| s.presence_rate = 5, move |s| s.clock = c).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    // Two hundred in each of two seconds: five a second get through (and of those, each tick
    // relays the latest).
    let mut got = Vec::new();
    for second in 0..2 {
        for n in 0..200 {
            a.presence(json!({"n": [second, n]})).await;
        }
        a.drain().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        got.extend(relayed(&b.drain().await));
        clock.advance(1.0);
    }
    let allowed: Vec<_> = (0..2)
        .flat_map(|s| (0..5).map(move |n| Some(plain(json!({"n": [s, n]})))))
        .collect();
    assert!(got.iter().all(|r| allowed.contains(&r.data)), "{got:?}");
    assert_eq!(got.last().unwrap().data, Some(plain(json!({"n": [1, 4]}))));
    assert!(
        got.iter()
            .any(|r| r.data == Some(plain(json!({"n": [0, 4]}))))
    );
}

/// Waits until `live` is relayed a presence of client `who`.
async fn presence_arrives(live: &mut Live, who: &str) {
    let wait = async {
        loop {
            let got: Relayed = live.expect().await;
            if got.kind == "presence" && got.client_id == who {
                break;
            }
        }
    };
    tokio::time::timeout(WAIT, wait)
        .await
        .expect("the presence came");
}

#[tokio::test]
async fn presence_goes_on_while_a_big_change_is_judged_off_the_task() {
    let hooks = std::sync::Arc::new(oswx_server::room::Hooks::default());
    let h = hooks.clone();
    let server = TestServer::start_custom(|_| {}, move |s| s.room_hooks = h).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let mut c = Live::connect(&server, &doc, &world.admin).await;
    // More than 256 operations: judged off the room's task, which takes no frame meanwhile.
    let ops = (0..300)
        .map(|i| set(&["variables", &format!("v{i}")], json!(i)))
        .collect();
    let big = a.make_change(0, ops);
    hooks.judge_gate.shut();
    a.send_change(&big).await;
    tokio::time::timeout(WAIT, hooks.judge_gate.until_waiting(1))
        .await
        .expect("the big change is being judged");
    // From the sender itself, whose later messages wait for it, and from someone else.
    a.presence(json!({"tool": "a"})).await;
    let a_id = a.client_id.clone();
    presence_arrives(&mut b, &a_id).await;
    b.presence(json!({"tool": "b"})).await;
    let b_id = b.client_id.clone();
    presence_arrives(&mut c, &b_id).await;
    assert_eq!(hooks.judge_gate.waiting(), 1, "relayed while it was judged");
    hooks.judge_gate.open();
    assert_eq!(a.outcome(&big.change_id).await.seq(), 1);
}

#[tokio::test]
async fn presence_goes_on_while_the_senders_blobs_are_looked_up() {
    let (server, blobs) = TestServer::start_blobs(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let missing = "0".repeat(64);
    let change = a.make_change(
        0,
        vec![set(&["variables", "brep"], json!({"$blob": missing}))],
    );
    blobs.exists_gate.shut();
    a.send_change(&change).await;
    tokio::time::timeout(WAIT, blobs.exists_gate.until_waiting(1))
        .await
        .expect("the blob is being looked up");
    a.presence(json!({"tool": "line"})).await;
    let a_id = a.client_id.clone();
    presence_arrives(&mut b, &a_id).await;
    assert_eq!(
        blobs.exists_gate.waiting(),
        1,
        "relayed while it was looked up"
    );
    blobs.exists_gate.open();
    assert_eq!(
        a.outcome(&change.change_id).await.code(),
        RejectCode::MissingBlob
    );
}

#[tokio::test]
async fn presence_does_not_wait_for_the_journal() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    // The journal is held: the change is tentative until it is opened.
    flaky.journal_gate.shut();
    let change = a.make_change(0, vec![set_depth(3)]);
    a.send_change(&change).await;
    tokio::time::timeout(WAIT, flaky.journal_gate.until_waiting(1))
        .await
        .expect("the journal is about to write the change");
    a.presence(json!({"tool": "line"})).await;
    let first = b.recv().await;
    assert!(
        matches!(first, ServerMsg::Presence { .. }),
        "the presence comes before the change: {first:?}"
    );
    assert_eq!(
        flaky.journal_gate.waiting(),
        1,
        "the journal was still held"
    );
    flaky.journal_gate.open();
    assert_eq!(a.outcome(&change.change_id).await.seq(), 1);
}
