//! History: undo (a change with `undo_of`), reverts from history, rebuild reports, the history
//! list, content at any state, named versions and restore. (A port of `tests/test_history.py`,
//! adapted to protocol v2: undo is a change marked `undo_of`, and an undo of something someone
//! else changed since is `stale`.)

mod common;

use std::sync::Arc;

use common::*;
use oswx_core::wire::{ChangeEvent, ClientMsg, RejectCode, ServerMsg};
use oswx_core::{Op, Rules, Seq, Value};
use oswx_server::store::Id;
use serde_json::{Value as Json, json};

// ------------------------------------------------------------------ helpers

async fn editor(server: &TestServer, world: &World, email: &str, name: &str) -> (Token, Id) {
    world
        .invite(server, email, Some(name), Some("editor"), "member")
        .await
}

async fn viewer(server: &TestServer, world: &World) -> (Token, Id) {
    world
        .invite(
            server,
            "vi@example.com",
            Some("Vi"),
            Some("viewer"),
            "member",
        )
        .await
}

async fn outsider(server: &TestServer, world: &World) -> Token {
    world
        .invite(server, "out@example.com", None, None, "member")
        .await
        .0
}

/// `GET /content` with a query (`""` for the head).
async fn content_q(server: &TestServer, doc: &str, token: &str, q: &str) -> (u16, Json) {
    let (status, body) = server
        .get(&format!("/api/documents/{doc}/content{q}"), Some(token))
        .await;
    (status.as_u16(), body)
}

/// The content at `q` (panics unless 200).
async fn content(server: &TestServer, doc: &str, token: &str, q: &str) -> Json {
    let (status, body) = content_q(server, doc, token, q).await;
    assert_eq!(status, 200, "{q}: {body}");
    body["content"].clone()
}

fn depth(content: &Json) -> &Json {
    &content["features"][1]["params"]["depth"]
}

fn ids(content: &Json) -> Vec<String> {
    content["features"]
        .as_array()
        .expect("features")
        .iter()
        .map(|f| f["id"].as_str().unwrap().to_owned())
        .collect()
}

/// The inverse the journal kept of change `seq` (what a client keeps for its own changes).
async fn kept_inverse(server: &TestServer, doc: &str, seq: Seq) -> Vec<Op> {
    let row = server
        .state
        .store
        .change_by_seq(doc, seq as i64)
        .await
        .unwrap()
        .expect("the change");
    oswx_server::record::decode_ops(&row.inverse).unwrap()
}

/// The change event with this seq (earlier ones are skipped).
async fn event(live: &mut Live, seq: Seq) -> ChangeEvent {
    loop {
        let ev: ChangeEvent = live.expect().await;
        if ev.seq == seq {
            return ev;
        }
        assert!(ev.seq < seq, "event {} came before {seq}", ev.seq);
    }
}

async fn history(server: &TestServer, doc: &str, token: &str, q: &str) -> (u16, Json) {
    let (status, body) = server
        .get(&format!("/api/documents/{doc}/history{q}"), Some(token))
        .await;
    (status.as_u16(), body)
}

fn seqs(page: &Json) -> Vec<i64> {
    page.as_array()
        .expect("a list")
        .iter()
        .map(|h| h["seq"].as_i64().unwrap())
        .collect()
}

async fn restore(server: &TestServer, doc: &str, token: &str, body: Json) -> (u16, Json) {
    let (status, body) = server
        .post(&format!("/api/documents/{doc}/restore"), Some(token), body)
        .await;
    (status.as_u16(), body)
}

async fn new_version(server: &TestServer, doc: &str, token: &str, body: Json) -> (u16, Json) {
    let (status, body) = server
        .post(&format!("/api/documents/{doc}/versions"), Some(token), body)
        .await;
    (status.as_u16(), body)
}

fn rejected(outcome: &Outcome) -> (RejectCode, &str) {
    match outcome {
        Outcome::Rejected { code, message, .. } => (*code, message.as_str()),
        other => panic!("expected a rejection, got {other:?}"),
    }
}

fn path(segments: &[&str]) -> oswx_core::Path {
    common::path(segments)
}

// ------------------------------------------------------------------ undo

#[tokio::test]
async fn undo_takes_back_only_your_own_change() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let first = a.make_change(0, vec![set_depth(20)]);
    a.send_change(&first).await;
    assert_eq!(a.outcome(&first.change_id).await.seq(), 1);
    let (fillet, fillet_id) = b.add_feature("Fillet1");
    event(&mut b, 1).await;
    assert_eq!(b.change(vec![fillet], vec![], vec![]).await.seq(), 2);

    // Bob cannot undo Ada's change: it is not one of his.
    let inverse = kept_inverse(&server, &doc, 1).await;
    let refused = b.undo(&first.change_id, inverse.clone()).await;
    assert_eq!(rejected(&refused).0, RejectCode::Invalid);
    // Ada can: a new change, and Bob's fillet stays.
    event(&mut a, 2).await;
    let undo = a.undo_change(a.seq, &first.change_id, inverse.clone());
    a.send_change(&undo).await;
    assert_eq!(a.outcome(&undo.change_id).await.seq(), 3);
    let ev = event(&mut b, 3).await;
    assert_eq!(ev.reverts, Some(1));
    assert_eq!(ev.undo_of.as_deref(), Some(&*first.change_id));
    assert_eq!(&*ev.command, "undo");
    assert_eq!(ev.ops, vec![set_depth(10)]);
    let got = content(&server, &doc, &bob, "").await;
    assert_eq!(depth(&got), 10);
    assert_eq!(ids(&got), ["f1", "f2", &*fillet_id]);
    // Each change is undone once.
    let twice = a.undo(&first.change_id, inverse).await;
    let (code, message) = rejected(&twice);
    assert_eq!(code, RejectCode::Invalid);
    assert!(message.contains("already undone"), "{message}");
    // Redo is an undo of the undo.
    let redo_ops = kept_inverse(&server, &doc, 3).await;
    let redo = a.undo_change(a.seq, &undo.change_id, redo_ops);
    a.send_change(&redo).await;
    assert_eq!(a.outcome(&redo.change_id).await.seq(), 4);
    assert_eq!(depth(&content(&server, &doc, &bob, "").await), 20);
    // Once someone else has changed the same thing, the undo is stale (even made on a base that
    // has seen that change) and says who.
    event(&mut b, 4).await;
    assert_eq!(b.change(vec![set_depth(33)], vec![], vec![]).await.seq(), 5);
    event(&mut a, 5).await;
    let late = a
        .undo(&redo.change_id, kept_inverse(&server, &doc, 4).await)
        .await;
    match &late {
        Outcome::Rejected {
            code, conflicts, ..
        } => {
            assert_eq!(*code, RejectCode::Stale);
            assert_eq!(conflicts[0].seq, 5);
            assert_eq!(&*conflicts[0].by, b.client_id.as_str());
        }
        other => panic!("expected stale, got {other:?}"),
    }
    // The history shows who took back what.
    let (_, page) = history(&server, &doc, &bob, "").await;
    let row = |seq: i64| {
        page.as_array()
            .unwrap()
            .iter()
            .find(|h| h["seq"] == seq)
            .unwrap()
            .clone()
    };
    assert_eq!(
        (row(1)["reverted_by"].clone(), row(3)["reverts"].clone()),
        (json!(3), json!(1))
    );
    assert_eq!(
        (row(3)["reverted_by"].clone(), row(4)["reverts"].clone()),
        (json!(4), json!(3))
    );
    assert_eq!(row(5)["reverts"], Json::Null);
}

#[tokio::test]
async fn your_own_later_changes_do_not_stop_an_undo() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let first = a.make_change(0, vec![set_depth(20)]);
    a.send_change(&first).await;
    assert_eq!(a.outcome(&first.change_id).await.seq(), 1);
    assert_eq!(a.change(vec![set_depth(25)], vec![], vec![]).await.seq(), 2);
    let undo = a
        .undo(&first.change_id, kept_inverse(&server, &doc, 1).await)
        .await;
    assert_eq!(undo.seq(), 3);
    assert_eq!(depth(&content(&server, &doc, &world.admin, "").await), 10);
}

#[tokio::test]
async fn another_client_of_the_same_person_cannot_undo_it() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let mut desktop =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    let mut laptop =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-laptop1")).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let first = desktop.make_change(0, vec![set_depth(20)]);
    desktop.send_change(&first).await;
    assert_eq!(desktop.outcome(&first.change_id).await.seq(), 1);
    let inverse = kept_inverse(&server, &doc, 1).await;
    for live in [&mut laptop, &mut b] {
        event(live, 1).await;
        let out = live.undo(&first.change_id, inverse.clone()).await;
        assert_eq!(rejected(&out).0, RejectCode::Invalid);
    }
    // An id that names nothing at all is refused the same way.
    let out = desktop.undo("no-such-change", inverse.clone()).await;
    assert_eq!(rejected(&out).0, RejectCode::Invalid);
    assert_eq!(desktop.undo(&first.change_id, inverse).await.seq(), 2);
}

#[tokio::test]
async fn undo_of_a_change_in_another_document_is_refused() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let other = world.document(&server, None, Some("Other"), None).await;
    let hello = || Hello::client("ada-desktop");
    let mut there = Live::connect_with(&server, &other, &world.admin, hello()).await;
    let first = there.make_change(0, vec![set_depth(20)]);
    there.send_change(&first).await;
    assert_eq!(there.outcome(&first.change_id).await.seq(), 1);
    let inverse = kept_inverse(&server, &other, 1).await;
    // The same client, in another document.
    let mut here = Live::connect_with(&server, &doc, &world.admin, hello()).await;
    let out = here.undo(&first.change_id, inverse).await;
    assert_eq!(rejected(&out).0, RejectCode::Invalid);
    assert_eq!(depth(&content(&server, &doc, &world.admin, "").await), 10);
}

#[tokio::test]
async fn undoing_an_add_removes_it_and_respects_locks() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let (op, id) = a.add_feature("Fillet1");
    let add = a.make_change(0, vec![op]);
    a.send_change(&add).await;
    assert_eq!(a.outcome(&add.change_id).await.seq(), 1);
    event(&mut b, 1).await;
    assert_eq!(b.lock(&[&["features", &id]]).await.granted.len(), 1);
    let inverse = kept_inverse(&server, &doc, 1).await;
    assert_eq!(inverse, vec![remove(&["features"], &id)]);
    assert_eq!(
        rejected(&a.undo(&add.change_id, inverse.clone()).await).0,
        RejectCode::Locked
    );
    b.unlock(&[&["features", &id]]).await;
    b.drain().await;
    assert_eq!(a.undo(&add.change_id, inverse).await.seq(), 2);
    assert_eq!(ids(&content(&server, &doc, &bob, "").await), ["f1", "f2"]);
}

#[tokio::test]
async fn undoing_a_removal_puts_the_item_back_under_its_own_id() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let before = content(&server, &doc, &world.admin, "").await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    // A legacy id from the Python fixture.
    let gone = a.make_change(0, vec![remove(&["features"], "f1")]);
    a.send_change(&gone).await;
    assert_eq!(a.outcome(&gone.change_id).await.seq(), 1);
    assert_eq!(
        a.undo(&gone.change_id, kept_inverse(&server, &doc, 1).await)
            .await
            .seq(),
        2
    );
    let back = content(&server, &doc, &bob, "").await;
    assert_eq!(back["features"], before["features"]);
    // An item another client made, removed by Ada.
    event(&mut b, 2).await;
    let (op, bobs) = b.add_feature("Fillet1");
    assert_eq!(b.change(vec![op], vec![], vec![]).await.seq(), 3);
    event(&mut a, 3).await;
    let theirs = a.make_change(3, vec![remove(&["features"], &bobs)]);
    a.send_change(&theirs).await;
    assert_eq!(a.outcome(&theirs.change_id).await.seq(), 4);
    assert_eq!(
        a.undo(&theirs.change_id, kept_inverse(&server, &doc, 4).await)
            .await
            .seq(),
        5
    );
    assert_eq!(
        ids(&content(&server, &doc, &bob, "").await),
        ["f1", "f2", &*bobs]
    );
    // Everyone converges.
    let ev = event(&mut b, 5).await;
    assert_eq!(ev.reverts, Some(4));
}

#[tokio::test]
async fn undoing_a_set_that_replaced_someone_elses_items_brings_them_back() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    // Bob makes a feature with items of his own inside it.
    let (fid, part) = (b.new_id(), b.new_id());
    let item = json!({"id": &*fid, "name": "Pattern1", "type": "pattern",
                      "params": {"parts": [{"id": &*part, "v": 1}]}});
    let add = insert(&["features"], item, after_f2());
    assert_eq!(b.change(vec![add], vec![], vec![]).await.seq(), 1);
    event(&mut a, 1).await;
    // Ada replaces the whole list (his ids go with it), then something else.
    let parts = ["features", &*fid, "params", "parts"];
    let replace = a.make_change(1, vec![set(&parts, json!([]))]);
    a.send_change(&replace).await;
    assert_eq!(a.outcome(&replace.change_id).await.seq(), 2);
    let other = a.make_change(2, vec![set_depth(12)]);
    a.send_change(&other).await;
    assert_eq!(a.outcome(&other.change_id).await.seq(), 3);
    let inverse = kept_inverse(&server, &doc, 2).await;

    // The same operations without `undo_of` would bring back ids that are not Ada's.
    let plain = a.change(inverse.clone(), vec![], vec![]).await;
    assert_eq!(rejected(&plain).0, RejectCode::Invalid);
    // Naming another change of hers does not exempt them either.
    let wrong = a.undo(&other.change_id, inverse.clone()).await;
    assert_eq!(rejected(&wrong).0, RejectCode::Invalid);
    // Operations beyond the kept inverse are judged on their own: no foreign id slips in.
    let mut smuggled = inverse.clone();
    smuggled.push(insert(
        &["features"],
        feature(&format!("{}.77", b.client_id), "Smuggled"),
        after_f2(),
    ));
    let sneaky = a.undo(&replace.change_id, smuggled).await;
    assert_eq!(rejected(&sneaky).0, RejectCode::Invalid);
    // The undo itself is accepted, and his item is back under his id.
    assert_eq!(a.undo(&replace.change_id, inverse).await.seq(), 4);
    let got = content(&server, &doc, &bob, "").await;
    assert_eq!(
        got["features"][2]["params"]["parts"],
        json!([{"id": &*part, "v": 1}])
    );
    let ev = event(&mut b, 4).await;
    assert_eq!(ev.undo_of.as_deref(), Some(&*replace.change_id));
}

#[tokio::test]
async fn an_undo_of_a_change_whose_seq_is_being_checked_waits_for_the_check() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let x = a.make_change(0, vec![set_depth(20)]);
    assert_eq!(a.resend(&x).await.seq(), 1);
    // The room forgets X; X sent again reaches the journal, which refuses it as a duplicate, and
    // the room looks its seq up (slowly).
    server.room(&doc).forget_change_ids().await;
    // The duplicate check is slow; the undo's own resend check (the client is no longer complete
    // then) is quick, so the undo arrives while X's seq is still being checked.
    flaky.lookup_delay_once(std::time::Duration::from_millis(800));
    let mut again = x.clone();
    again.base = 1;
    a.send_change(&again).await;
    pause(std::time::Duration::from_millis(200)).await;
    let undo = a.undo_change(1, &x.change_id, vec![set_depth(10)]);
    a.send_change(&undo).await;
    assert_eq!(a.outcome(&x.change_id).await, Outcome::Accepted(1));
    assert_eq!(a.outcome(&undo.change_id).await.seq(), 2);
    // The undo's target was read only once the check had answered.
    let log = flaky.lookup_log();
    let asked = log
        .iter()
        .position(|e| *e == "undo target asked")
        .expect("asked");
    // (Lookups: X's duplicate check, which ends last of the two, and the undo's resend check.)
    assert_eq!(log[..asked].len(), 2, "{log:?}");
}

#[tokio::test]
async fn an_undo_sent_before_its_change_was_acknowledged_works() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let first = a.make_change(0, vec![set_depth(20)]);
    // The client's own inverse, kept before any answer.
    let undo = a.undo_change(0, &first.change_id, vec![set_depth(10)]);
    a.send_change(&first).await;
    a.send_change(&undo).await;
    assert_eq!(a.outcome(&first.change_id).await.seq(), 1);
    assert_eq!(a.outcome(&undo.change_id).await.seq(), 2);
    assert_eq!(event(&mut b, 2).await.reverts, Some(1));
    assert_eq!(depth(&content(&server, &doc, &bob, "").await), 10);
}

#[tokio::test]
async fn of_two_undos_of_one_change_exactly_one_is_accepted() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    for round in 0..2u64 {
        let base = round * 2;
        let first = a.make_change(base, vec![set_depth(20 + round)]);
        a.send_change(&first).await;
        assert_eq!(a.outcome(&first.change_id).await.seq(), base + 1);
        let inverse = kept_inverse(&server, &doc, base + 1).await;
        let one = a.undo_change(base + 1, &first.change_id, inverse.clone());
        let two = a.undo_change(base + 1, &first.change_id, inverse);
        if round == 0 {
            // Back to back from the client.
            a.send_change(&one).await;
            a.send_change(&two).await;
            let answers = [
                a.outcome(&one.change_id).await,
                a.outcome(&two.change_id).await,
            ];
            let accepted = answers
                .iter()
                .filter(|o| matches!(o, Outcome::Accepted(_)))
                .count();
            assert_eq!(accepted, 1, "{answers:?}");
        } else {
            // An undo and a revert from history at once.
            event(&mut b, base + 1).await;
            a.send_change(&one).await;
            b.send(&ClientMsg::Revert {
                request_id: "r".into(),
                seq: base + 1,
            })
            .await;
            let undone = a.outcome(&one.change_id).await;
            let reverted: RevertResult = b.expect().await;
            assert!(
                matches!(undone, Outcome::Accepted(_)) != reverted.ok,
                "{undone:?} {reverted:?}"
            );
        }
        let (_, page) = history(&server, &doc, &world.admin, "").await;
        let takes_back = page
            .as_array()
            .unwrap()
            .iter()
            .filter(|h| h["reverts"] == json!(base + 1))
            .count();
        assert_eq!(takes_back, 1);
        a.drain().await;
    }
}

#[tokio::test]
async fn an_undo_works_after_a_restart_and_for_a_change_made_before_the_room_loaded() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let hello = || Hello::client("ada-desktop");
    let mut a = Live::connect_with(&server, &doc, &world.admin, hello()).await;
    let first = a.make_change(0, vec![set_depth(20)]);
    a.send_change(&first).await;
    assert_eq!(a.outcome(&first.change_id).await.seq(), 1);
    let second = a.make_change(1, vec![set(&["features", "f1", "name"], json!("Base"))]);
    a.send_change(&second).await;
    assert_eq!(a.outcome(&second.change_id).await.seq(), 2);
    let inverses = [
        kept_inverse(&server, &doc, 1).await,
        kept_inverse(&server, &doc, 2).await,
    ];
    a.close().await;

    // The room unloads; a new one loads from the store.
    server
        .room(&doc)
        .close(oswx_server::room::close::GOING_AWAY, "test", true)
        .await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, hello().last_seq(2)).await;
    assert_eq!(
        a.undo(&second.change_id, inverses[1].clone()).await.seq(),
        3
    );
    a.close().await;

    // The server restarts: the inverse comes from the store.
    let server = server.restart().await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, hello().last_seq(3)).await;
    assert_eq!(a.undo(&first.change_id, inverses[0].clone()).await.seq(), 4);
    let got = content(&server, &doc, &world.admin, "").await;
    assert_eq!(depth(&got), 10);
    assert_eq!(got["features"][0]["name"], "Sketch1");
    // A catch-up shows the same undo events as the live broadcast did.
    let late = Live::connect_with(
        &server,
        &doc,
        &world.admin,
        Hello::client("ada-laptop1").last_seq(2),
    )
    .await;
    let changes = late.welcome.changes.clone().expect("a catch-up");
    let undos: Vec<(Seq, Option<&str>, Option<Seq>)> = changes
        .iter()
        .map(|c| (c.seq, c.undo_of.as_deref(), c.reverts))
        .collect();
    assert_eq!(
        undos,
        [
            (3, Some(&*second.change_id), Some(2)),
            (4, Some(&*first.change_id), Some(1))
        ]
    );
}

#[tokio::test]
async fn catch_up_and_live_change_events_are_the_same() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let first = a.make_change(0, vec![set_depth(20)]);
    a.send_change(&first).await;
    assert_eq!(a.outcome(&first.change_id).await.seq(), 1);
    assert_eq!(
        a.undo(&first.change_id, kept_inverse(&server, &doc, 1).await)
            .await
            .seq(),
        2
    );
    let live: Vec<ChangeEvent> = vec![event(&mut b, 1).await, event(&mut b, 2).await];
    assert_eq!(b.revert(2).await.seq(), 3);
    let third = event(&mut a, 3).await;
    let late = Live::connect_with(&server, &doc, &bob, Hello::default().last_seq(0)).await;
    let caught_up = late.welcome.changes.clone().expect("a catch-up");
    assert_eq!(caught_up, [live[0].clone(), live[1].clone(), third.clone()]);
    assert_eq!((third.reverts, third.undo_of.clone()), (Some(2), None));
    assert_eq!(&*third.client_id, oswx_core::SERVER_CLIENT);
}

// ------------------------------------------------------------------ revert

#[tokio::test]
async fn editors_can_revert_someone_elses_change() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let (vi, _) = viewer(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut v = Live::connect(&server, &doc, &vi).await;
    assert_eq!(
        a.change(vec![set_depth(500)], vec![], vec![]).await.seq(),
        1
    );
    let refused = v.revert(1).await;
    assert_eq!(
        (refused.code(), refused.request_id.as_str()),
        ("read_only", "rv1")
    );
    let done = b.revert(1).await;
    assert_eq!((done.seq(), done.ops.clone()), (2, vec![set_depth(10)]));
    // The requester applies the result itself; everyone else gets the change.
    let ev = event(&mut a, 2).await;
    assert_eq!(
        (ev.reverts, ev.author_name.as_str(), &*ev.command),
        (Some(1), "Bob", "revert edit")
    );
    assert_eq!(event(&mut v, 2).await, ev);
    assert!(
        b.drain()
            .await
            .iter()
            .all(|m| !matches!(m, ServerMsg::Change(c) if c.seq == 2))
    );
    let (status, page) = history(&server, &doc, &vi, "").await;
    assert_eq!(status, 200);
    let rows: Vec<(i64, &str, &str, Json, Json)> = page
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            (
                h["seq"].as_i64().unwrap(),
                h["author_name"].as_str().unwrap(),
                h["command"].as_str().unwrap(),
                h["reverts"].clone(),
                h["reverted_by"].clone(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            (2, "Bob", "revert edit", json!(1), Json::Null),
            (1, "Ada", "edit", Json::Null, json!(2)),
        ]
    );
    assert_eq!(page[0]["client_id"], Json::Null);
    assert_eq!(page[1]["client_id"], json!(a.client_id));
}

#[tokio::test]
async fn what_a_revert_is_refused_for() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let (cy, _) = editor(&server, &world, "cy@example.com", "Cy").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut c = Live::connect(&server, &doc, &cy).await;
    assert_eq!(a.change(vec![set_depth(20)], vec![], vec![]).await.seq(), 1);
    for seq in [0, 2, 99, u64::MAX, i64::MAX as u64 + 1] {
        assert_eq!(b.revert(seq).await.code(), "not_found", "{seq}");
    }
    // Someone else wrote the same thing since: stale. (Bob has heard of change 1 by now.)
    assert_eq!(b.seq, 1);
    assert_eq!(b.change(vec![set_depth(30)], vec![], vec![]).await.seq(), 2);
    let stale = a.revert(1).await;
    assert_eq!(stale.code(), "stale");
    assert!(stale.message.is_some());
    // Locked by another client; the requester's own lock does not count.
    let name = ["features", "f1", "name"];
    assert_eq!(
        a.change(vec![set(&name, json!("Base"))], vec![], vec![])
            .await
            .seq(),
        3
    );
    assert_eq!(c.lock(&[&["features", "f1"]]).await.granted.len(), 1);
    assert_eq!(b.revert(3).await.code(), "locked");
    c.unlock(&[&["features", "f1"]]).await;
    c.drain().await;
    assert_eq!(b.lock(&[&["features", "f1"]]).await.granted.len(), 1);
    assert_eq!(b.revert(3).await.seq(), 4);
    assert_eq!(b.revert(3).await.code(), "already_reverted");
    // A change the server made (the revert itself) can be reverted too.
    b.unlock(&[&["features", "f1"]]).await;
    assert_eq!(b.revert(4).await.seq(), 5);
    assert_eq!(
        content(&server, &doc, &bob, "").await["features"][0]["name"],
        "Base"
    );
}

#[tokio::test]
async fn a_revert_does_not_silently_undo_a_restore_or_another_revert() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(a.change(vec![set_depth(20)], vec![], vec![]).await.seq(), 1);
    // A restore (a server-made change) writes the same path after change 1.
    assert_eq!(
        restore(&server, &doc, &bob, json!({"seq": 0})).await,
        (200, json!({"seq": 2}))
    );
    let stale = b.revert(1).await;
    assert_eq!(stale.code(), "stale");
    assert_eq!(depth(&content(&server, &doc, &bob, "").await), 10);

    // Two reverts that touch one path: the older one is stale once the newer has landed.
    event(&mut a, 2).await;
    assert_eq!(a.change(vec![set_depth(30)], vec![], vec![]).await.seq(), 3);
    assert_eq!(a.change(vec![set_depth(40)], vec![], vec![]).await.seq(), 4);
    assert_eq!(b.revert(4).await.seq(), 5);
    assert_eq!(b.revert(3).await.code(), "stale");
    assert_eq!(b.revert(4).await.code(), "already_reverted");
    assert_eq!(depth(&content(&server, &doc, &bob, "").await), 30);
}

#[tokio::test]
async fn a_change_older_than_the_history_kept_cannot_be_taken_back_and_says_so() {
    // The index keeps no history before the latest snapshot (every 2 changes).
    let hooks = oswx_server::room::Hooks {
        index_keep: 0,
        ..oswx_server::room::Hooks::default()
    };
    let server =
        TestServer::start_custom(|s| s.snapshot_every = 2, |s| s.room_hooks = Arc::new(hooks))
            .await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let first = a.make_change(0, vec![set_depth(20)]);
    a.send_change(&first).await;
    assert_eq!(a.outcome(&first.change_id).await.seq(), 1);
    let name = ["features", "f1", "name"];
    assert_eq!(
        a.change(vec![set(&name, json!("Base"))], vec![], vec![])
            .await
            .seq(),
        2
    );
    // Not `too_old` (which tells a client to reload and try again): it never will work.
    let undo = a
        .undo(&first.change_id, kept_inverse(&server, &doc, 1).await)
        .await;
    let (code, message) = rejected(&undo);
    assert_eq!(code, RejectCode::Invalid);
    assert!(message.contains("too old"), "{message}");
    let revert = a.revert(1).await;
    assert_eq!(revert.code(), "invalid");
    assert!(revert.message.unwrap().contains("too old"));
}

#[tokio::test]
async fn a_revert_or_undo_the_journal_cannot_write_is_a_server_error_and_changes_nothing() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let first = a.make_change(0, vec![set_depth(20)]);
    a.send_change(&first).await;
    assert_eq!(a.outcome(&first.change_id).await.seq(), 1);
    flaky.fail_reverted_by(true);
    let failed = a.revert(1).await;
    assert_eq!(failed.code(), "server_error");
    let undo = a
        .undo(&first.change_id, kept_inverse(&server, &doc, 1).await)
        .await;
    assert_eq!(rejected(&undo).0, RejectCode::ServerError);
    assert_eq!(depth(&content(&server, &doc, &world.admin, "").await), 20);
    // Once the journal works again, so does the revert.
    flaky.fail_reverted_by(false);
    assert_eq!(a.revert(1).await.seq(), 2);
    assert_eq!(depth(&content(&server, &doc, &world.admin, "").await), 10);
}

#[tokio::test]
async fn reverting_inserts_and_removals() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let (op, id) = a.add_feature("Fillet1");
    assert_eq!(a.change(vec![op], vec![], vec![]).await.seq(), 1);
    assert_eq!(
        a.change(vec![remove(&["features"], "f1")], vec![], vec![])
            .await
            .seq(),
        2
    );
    // Bob takes Ada's insert out and puts her removal back (the original ids, whoever made them).
    let out = b.revert(1).await;
    assert_eq!(out.ops, vec![remove(&["features"], &id)]);
    assert_eq!(b.revert(2).await.seq(), 4);
    let got = content(&server, &doc, &world.admin, "").await;
    assert_eq!(ids(&got), ["f1", "f2"]);
    assert_eq!(got["features"][0]["name"], "Sketch1");
}

#[tokio::test]
async fn one_and_true_and_one_point_zero_are_different_and_come_back_as_they_were() {
    // (Deferred here from test_hardening.py: undo and revert put back exactly what was there.)
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let before = content(&server, &doc, &world.admin, "").await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let ops = vec![
        set_depth(10.0),
        set(&["features", "f2", "suppressed"], json!(0)),
    ];
    let first = a.make_change(0, ops);
    a.send_change(&first).await;
    assert_eq!(a.outcome(&first.change_id).await.seq(), 1);
    let changed = content(&server, &doc, &world.admin, "").await;
    assert_eq!(changed["features"][1]["params"]["depth"], json!(10.0));
    assert!(changed["features"][1]["params"]["depth"].is_f64());
    assert_eq!(changed["features"][1]["suppressed"], json!(0));
    assert_eq!(a.revert(1).await.seq(), 2);
    let back = content(&server, &doc, &world.admin, "").await;
    assert_eq!(back, before);
    assert!(back["features"][1]["params"]["depth"].is_i64());
    assert_eq!(back["features"][1]["suppressed"], json!(false));
    // And the undo of the revert (a redo) brings the float and the integer back.
    assert_eq!(a.revert(2).await.seq(), 3);
    assert_eq!(content(&server, &doc, &world.admin, "").await, changed);
}

// ------------------------------------------------------------------ rebuild reports

#[tokio::test]
async fn rebuild_reports_pin_the_break_on_the_change_that_caused_it() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let (op, fillet) = b.add_feature("Fillet1");
    assert_eq!(b.change(vec![op], vec![], vec![]).await.seq(), 1);
    event(&mut a, 1).await;
    a.report(1, &[]).await; // all good so far: the baseline
    let mut thin = a.make_change(1, vec![set_depth(0.5)]);
    thin.command = Arc::from("edit_feature");
    a.send_change(&thin).await;
    assert_eq!(a.outcome(&thin.change_id).await.seq(), 2);
    event(&mut b, 2).await;
    b.report(2, &[(&fillet, "fillet radius too large")]).await;
    for live in [&mut a, &mut b] {
        let st: RebuildStatus = live.expect().await;
        assert_eq!(st.seq, 2);
        let entry = &st.broken[&*fillet];
        assert_eq!(entry.error, "fillet radius too large");
        assert_eq!(
            (
                entry.by_seq,
                entry.by_user.as_deref(),
                entry.by_name.as_deref()
            ),
            (Some(2), Some(world.admin_id.as_str()), Some("Ada"))
        );
        assert_eq!(
            (entry.command.as_deref(), entry.since_seq),
            (Some("edit_feature"), Some(2))
        );
    }
    // A second report on the same state changes nothing.
    a.report(2, &[(&fillet, "again")]).await;
    assert!(
        a.drain()
            .await
            .iter()
            .all(|m| !matches!(m, ServerMsg::RebuildStatus { .. }))
    );
    // The blamed change says what it broke (written off the room's task: wait for it).
    let mut broke = Json::Null;
    for _ in 0..200 {
        broke = history(&server, &doc, &bob, "").await.1[0]["broke"].clone();
        if !broke.is_null() {
            break;
        }
        pause(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(broke, json!({&*fillet: "fillet radius too large"}));
    // A newcomer sees it; a revert of change 2 and a clean report fix it.
    let late = Live::connect(&server, &doc, &bob).await;
    assert_eq!(late.welcome.broken[&*fillet].by_seq, Some(2));
    assert_eq!(b.revert(2).await.seq(), 3);
    b.report(3, &[]).await;
    let st: RebuildStatus = a.expect().await;
    assert_eq!((st.seq, st.broken.len()), (3, 0));
}

#[tokio::test]
async fn which_rebuild_reports_count() {
    let (server, world) = world().await;
    let (vi, _) = viewer(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut v = Live::connect(&server, &doc, &vi).await;
    assert_eq!(a.change(vec![set_depth(20)], vec![], vec![]).await.seq(), 1);
    assert_eq!(a.change(vec![set_depth(21)], vec![], vec![]).await.seq(), 2);
    // Someone who may only look can neither blame nor clear.
    v.report(1, &[("f2", "broken")]).await;
    // A state the document has not reached.
    a.report(3, &[("f2", "broken")]).await;
    assert!(
        v.drain()
            .await
            .iter()
            .all(|m| !matches!(m, ServerMsg::RebuildStatus { .. }))
    );
    // The baseline (already broken: nobody is blamed).
    a.report(1, &[("f2", "broken")]).await;
    let st: RebuildStatus = v.expect().await;
    assert_eq!((st.seq, st.broken["f2"].by_seq), (1, None));
    // Not newer than the last report.
    a.report(1, &[]).await;
    assert!(
        v.drain()
            .await
            .iter()
            .all(|m| !matches!(m, ServerMsg::RebuildStatus { .. }))
    );
    // Fixed entries go; features that do not exist are reported as given, bounded by the codec.
    let made_up: Vec<String> = (0..500).map(|i| format!("ghost-{i}")).collect();
    let errors: Vec<(&str, &str)> = made_up.iter().map(|g| (g.as_str(), "nope")).collect();
    a.report(2, &errors).await;
    let st: RebuildStatus = v.expect().await;
    assert_eq!(st.broken.len(), 500);
    assert!(!st.broken.contains_key("f2"));
    assert_eq!(st.broken["ghost-7"].by_seq, Some(2));
    assert_eq!(st.broken["ghost-7"].since_seq, Some(2));
    // Over the codec's limit: an error, and nothing changes.
    let too_many: Vec<String> = (0..501).map(|i| format!("g{i}")).collect();
    let errors: Vec<(&str, &str)> = too_many.iter().map(|g| (g.as_str(), "x")).collect();
    assert_eq!(a.change(vec![set_depth(22)], vec![], vec![]).await.seq(), 3);
    a.report(3, &errors).await;
    let err: ErrorMsg = a.expect().await;
    assert_eq!(err.code, "bad_message");
    let late = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(late.welcome.broken.len(), 500);
}

// ------------------------------------------------------------------ history

#[tokio::test]
async fn history_pages_and_content_at_any_point() {
    let (server, world) = world().await;
    let (vi, _) = viewer(&server, &world).await;
    let out = outsider(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    for n in 1..=7u64 {
        let mut change = a.make_change(n - 1, vec![set_depth(n * 10)]);
        change.command = Arc::from("edit_feature");
        a.send_change(&change).await;
        assert_eq!(a.outcome(&change.change_id).await.seq(), n);
    }
    let admin = world.admin.as_str();
    let (_, page) = history(&server, &doc, admin, "?limit=3").await;
    assert_eq!(seqs(&page), [7, 6, 5]);
    let top = &page[0];
    assert_eq!(
        top["ops"],
        json!([{"op": "set", "path": ["features", "f2", "params", "depth"], "value": 70}])
    );
    assert_eq!(
        (
            top["author"].as_str(),
            top["author_name"].as_str(),
            top["command"].as_str()
        ),
        (
            Some(world.admin_id.as_str()),
            Some("Ada"),
            Some("edit_feature")
        )
    );
    assert_eq!(top["client_id"], json!(a.client_id));
    assert!(top["change_id"].is_string() && top["time"].is_f64());
    assert_eq!(
        (
            top["reverts"].clone(),
            top["reverted_by"].clone(),
            top["broke"].clone()
        ),
        (Json::Null, Json::Null, Json::Null)
    );
    assert_eq!(
        seqs(&history(&server, &doc, admin, "?limit=3&before=5").await.1),
        [4, 3, 2]
    );
    assert_eq!(
        seqs(&history(&server, &doc, admin, "?after=4").await.1),
        [7, 6, 5]
    );
    assert_eq!(
        seqs(&history(&server, &doc, admin, "?after=2&before=5").await.1),
        [4, 3]
    );
    // Limits are clamped; anything but an integer is refused.
    for (q, n) in [("?limit=0", 1), ("?limit=-5", 1), ("?limit=100000", 7)] {
        assert_eq!(
            seqs(&history(&server, &doc, admin, q).await.1).len(),
            n,
            "{q}"
        );
    }
    for q in [
        "?limit=x",
        "?before=1.5",
        "?after=",
        "?limit=99999999999999999999",
    ] {
        assert_eq!(history(&server, &doc, admin, q).await.0, 422, "{q}");
    }
    // Viewers read it; nobody else sees the document.
    assert_eq!(history(&server, &doc, &vi, "").await.0, 200);
    let (status, body) = history(&server, &doc, &out, "").await;
    assert_eq!((status, body), (404, json!({"detail": "no such document"})));
    // The content at any point.
    for (seq, d) in [(0, 10), (3, 30), (7, 70)] {
        assert_eq!(
            depth(&content(&server, &doc, &vi, &format!("?seq={seq}")).await),
            d
        );
    }
    let (status, body) = content_q(&server, &doc, admin, "?seq=8").await;
    assert_eq!(
        (status, body),
        (404, json!({"detail": "the document has no state 8"}))
    );
    assert_eq!(content_q(&server, &doc, &out, "?seq=1").await.0, 404);
}

#[tokio::test]
async fn a_history_page_holds_at_most_500_changes() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut last = None;
    for n in 1..=505u64 {
        let change = a.make_change(n - 1, vec![set_depth(n)]);
        a.send_change(&change).await;
        last = Some(change.change_id.clone());
    }
    assert_eq!(a.outcome(&last.unwrap()).await.seq(), 505);
    let (_, page) = history(&server, &doc, &world.admin, "?limit=100000").await;
    let got = seqs(&page);
    assert_eq!((got.len(), got[0], got[499]), (500, 505, 6));
    assert_eq!(
        seqs(&history(&server, &doc, &world.admin, "").await.1).len(),
        100
    );
}

/// A server whose history pages and catch-ups carry at most `bytes` of stored operations.
async fn with_page_bytes(bytes: u64) -> (TestServer, World) {
    let hooks = oswx_server::room::Hooks {
        page_bytes: bytes,
        ..oswx_server::room::Hooks::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    (server, world)
}

/// Six changes of about 730 stored bytes each.
async fn six_big_changes(live: &mut Live) {
    for n in 1..=6u64 {
        let value = format!("{n}{}", "x".repeat(700));
        let out = live.change(vec![set(&["variables", "v"], json!(value))], vec![], vec![]);
        assert_eq!(out.await.seq(), n);
    }
}

#[tokio::test]
async fn a_history_page_is_bounded_in_bytes_too() {
    let (server, world) = with_page_bytes(2000).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    six_big_changes(&mut a).await;
    let admin = world.admin.as_str();
    // Two rows fit in 2000 bytes; the client pages on with `before` until a page is empty.
    let mut pages = Vec::new();
    let mut q = "?limit=500".to_owned();
    loop {
        let page = seqs(&history(&server, &doc, admin, &q).await.1);
        if page.is_empty() {
            break;
        }
        q = format!("?limit=500&before={}", page.last().unwrap());
        pages.push(page);
    }
    assert_eq!(pages, [vec![6, 5], vec![4, 3], vec![2, 1]]);
    // A row bigger than the whole budget still comes, alone.
    let huge = json!("y".repeat(3000));
    assert_eq!(
        a.change(vec![set(&["variables", "w"], huge)], vec![], vec![])
            .await
            .seq(),
        7
    );
    assert_eq!(seqs(&history(&server, &doc, admin, "").await.1), [7]);
    assert_eq!(
        seqs(&history(&server, &doc, admin, "?before=7").await.1),
        [6, 5]
    );
}

#[tokio::test]
async fn a_catch_up_over_the_byte_budget_sends_the_document() {
    let (server, world) = with_page_bytes(2000).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    six_big_changes(&mut a).await;
    let near = Live::connect_with(&server, &doc, &world.admin, Hello::default().last_seq(4)).await;
    let seqs: Vec<Seq> = near
        .welcome
        .changes
        .expect("a catch-up")
        .iter()
        .map(|c| c.seq)
        .collect();
    assert_eq!((seqs, near.welcome.content.is_none()), (vec![5, 6], true));
    let far = Live::connect_with(&server, &doc, &world.admin, Hello::default().last_seq(2)).await;
    assert!(far.welcome.changes.is_none());
    let content = far.welcome.content.expect("the document");
    assert_eq!(
        content.at(&["variables", "v"]).and_then(|v| match v {
            Value::Str(s) => s.chars().next(),
            _ => None,
        }),
        Some('6')
    );
}

#[tokio::test]
async fn the_content_at_every_seq_is_what_clients_held_then() {
    // Snapshots every 7 changes, and a restart: every state is still reproduced.
    let server = TestServer::start_with(|s| s.snapshot_every = 7).await;
    let world = World::new(&server).await.unwrap();
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut mirror = a.welcome.content.clone().expect("the document");
    let mut held: Vec<Value> = vec![mirror.clone()];
    let mut added: Vec<Arc<str>> = Vec::new();
    for n in 1..=60u64 {
        let (live, other) = if n % 3 == 0 {
            (&mut b, &mut a)
        } else {
            (&mut a, &mut b)
        };
        let ops = match n % 5 {
            0 if !added.is_empty() => vec![remove(&["features"], &added.remove(0))],
            1 => {
                let (op, id) = live.add_feature(&format!("Fillet{n}"));
                added.push(id);
                vec![op]
            }
            2 => vec![set(
                &["features", "f1", "params", "plane"],
                json!(format!("P{n}")),
            )],
            _ => vec![set_depth(n as f64 / 2.0)],
        };
        assert_eq!(live.change(ops.clone(), vec![], vec![]).await.seq(), n);
        event(other, n).await;
        mirror = oswx_core::apply(&mirror, &ops, &Rules::default(), None)
            .unwrap()
            .doc;
        held.push(mirror.clone());
    }
    let (held, doc_id, token) = (&held, doc.as_str(), bob.as_str());
    let check = |server: TestServer| async move {
        for (seq, doc_then) in held.iter().enumerate() {
            let got = content(&server, doc_id, token, &format!("?seq={seq}")).await;
            let expected = oswx_server::content::json_from_plain(&doc_then.to_plain());
            assert_eq!(got, expected, "seq {seq}");
        }
        server
    };
    a.close().await;
    b.close().await;
    let server = check(server).await;
    let server = check(server.restart().await).await;
    let snapshot = server
        .state
        .store
        .latest_snapshot(&doc)
        .await
        .unwrap()
        .unwrap();
    assert!(snapshot.seq > 0, "{snapshot:?}");
    assert_eq!(content_q(&server, &doc, &bob, "?seq=61").await.0, 404);
    assert_eq!(content_q(&server, &doc, &bob, "?seq=-1").await.0, 404);
}

#[tokio::test]
async fn the_history_queries_write_and_read_back() {
    use oswx_server::store::Version;
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let first = a.make_change(0, vec![set_depth(20)]);
    a.send_change(&first).await;
    assert_eq!(a.outcome(&first.change_id).await.seq(), 1);
    let store = &server.state.store;
    // The change an undo names: by document, client and change id only.
    let target = store
        .undo_target(&doc, &a.client_id, &first.change_id)
        .await
        .unwrap()
        .expect("the change");
    assert_eq!((target.seq, target.reverted_by), (1, None));
    assert!(
        store
            .undo_target(&doc, "x.y", &first.change_id)
            .await
            .unwrap()
            .is_none()
    );
    // `broke`: locked and read, then written.
    let mut tx = store.begin().await.unwrap();
    assert_eq!(tx.change_broke(&doc, 1).await.unwrap(), Some(None));
    assert_eq!(tx.change_broke(&doc, 2).await.unwrap(), None);
    tx.set_change_broke(&doc, 1, r#"{"f2":"x"}"#).await.unwrap();
    tx.commit().await.unwrap();
    let row = store.history(&doc, i64::MAX, 0, 10).await.unwrap();
    assert_eq!(
        (row.len(), row[0].broke.as_deref()),
        (1, Some(r#"{"f2":"x"}"#))
    );
    assert_eq!(row[0].author_name.as_deref(), Some("Ada"));
    assert!(store.history(&doc, 1, 0, 10).await.unwrap().is_empty());
    // Versions: a name once per document; deleted once.
    let v = Version {
        id: oswx_server::store::new_id(),
        document_id: doc.clone(),
        seq: 1,
        name: "A".into(),
        note: String::new(),
        author_id: world.admin_id.clone(),
        created_at: 1.0,
    };
    let mut tx = store.begin().await.unwrap();
    assert!(tx.insert_version(&v).await.unwrap());
    let again = Version {
        id: oswx_server::store::new_id(),
        ..v.clone()
    };
    assert!(!tx.insert_version(&again).await.unwrap());
    tx.commit().await.unwrap();
    assert_eq!(
        store.version_by_name(&doc, "A").await.unwrap(),
        Some(v.clone())
    );
    let listed = store.versions(&doc).await.unwrap();
    assert_eq!(
        (listed.len(), listed[0].author_name.as_deref()),
        (1, Some("Ada"))
    );
    let mut tx = store.begin().await.unwrap();
    assert!(
        !tx.delete_version("0123456789abcdef0123456789abcdef", &v.id)
            .await
            .unwrap()
    );
    assert!(tx.delete_version(&doc, &v.id).await.unwrap());
    assert!(!tx.delete_version(&doc, &v.id).await.unwrap());
    tx.commit().await.unwrap();
    assert!(store.versions(&doc).await.unwrap().is_empty());
}

// ------------------------------------------------------------------ versions and restore

#[tokio::test]
async fn named_versions_and_restore() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let (vi, _) = viewer(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.change(vec![set_depth(20)], vec![], vec![]).await.seq(), 1);
    let (status, v) = new_version(
        &server,
        &doc,
        &bob,
        json!({"name": "Rev A", "note": "for review"}),
    )
    .await;
    assert_eq!(status, 201, "{v}");
    assert_eq!(
        (
            v["seq"].clone(),
            v["author"]["name"].clone(),
            v["note"].clone()
        ),
        (json!(1), json!("Bob"), json!("for review"))
    );
    let heard: VersionCreated = a.expect().await;
    assert_eq!(
        (heard.name.as_str(), heard.seq, heard.id.as_str()),
        ("Rev A", 1, v["id"].as_str().unwrap())
    );
    let (status, body) = new_version(&server, &doc, &bob, json!({"name": " Rev A "})).await;
    assert_eq!(
        (status, body),
        (
            409,
            json!({"detail": "there is already a version named 'Rev A'"})
        )
    );
    assert_eq!(
        new_version(&server, &doc, &vi, json!({"name": "X"}))
            .await
            .0,
        403
    );
    for seq in [9, -1, 2] {
        assert_eq!(
            new_version(&server, &doc, &bob, json!({"name": "Future", "seq": seq}))
                .await
                .0,
            422,
            "{seq}"
        );
    }
    for body in [
        json!({"name": ""}),
        json!({"name": "x".repeat(201)}),
        json!({"name": "N", "note": "n".repeat(4001)}),
        json!({"name": 5}),
    ] {
        assert_eq!(
            new_version(&server, &doc, &bob, body.clone()).await.0,
            422,
            "{body}"
        );
    }
    let (op, _) = a.add_feature("Fillet1");
    assert_eq!(a.change(vec![op], vec![], vec![]).await.seq(), 2);
    assert_eq!(a.change(vec![set_depth(99)], vec![], vec![]).await.seq(), 3);
    assert_eq!(
        new_version(&server, &doc, &bob, json!({"name": "Start", "seq": 0}))
            .await
            .0,
        201
    );
    let (_, list) = server
        .get(&format!("/api/documents/{doc}/versions"), Some(&vi))
        .await;
    let names: Vec<(&str, i64)> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|x| (x["name"].as_str().unwrap(), x["seq"].as_i64().unwrap()))
        .collect();
    assert_eq!(names, [("Rev A", 1), ("Start", 0)]);
    // What an assembly pinned to "Rev A" would load.
    assert_eq!(
        depth(&content(&server, &doc, &vi, "?version=Rev%20A").await),
        20
    );
    assert_eq!(
        depth(&content(&server, &doc, &vi, "?version=Rev%20A&seq=3").await),
        20
    );
    let (status, body) = content_q(&server, &doc, &vi, "?version=nope").await;
    assert_eq!(
        (status, body),
        (404, json!({"detail": "no version named 'nope'"}))
    );

    // A restore waits for other people's locks, then goes in as one change everyone receives.
    assert_eq!(a.lock(&[&["features", "f1"]]).await.granted.len(), 1);
    let (status, body) = restore(&server, &doc, &bob, json!({"version": "Rev A"})).await;
    assert_eq!(
        (status, body["detail"]["code"].clone()),
        (409, json!("locked"))
    );
    assert_eq!(
        body["detail"]["conflicts"][0]["path"],
        json!(["features", "f1"])
    );
    a.unlock(&[&["features", "f1"]]).await;
    a.drain().await;
    assert_eq!(
        restore(&server, &doc, &vi, json!({"version": "Rev A"}))
            .await
            .0,
        403
    );
    let (status, body) = restore(&server, &doc, &bob, json!({"version": "Rev A"})).await;
    assert_eq!((status, body), (200, json!({"seq": 4})));
    let ev = event(&mut a, 4).await;
    assert_eq!(
        (&*ev.command, ev.author_name.as_str()),
        ("restore to Rev A", "Bob")
    );
    // A client applying the event ends up at Rev A.
    let at3 = a_value(&content(&server, &doc, &bob, "?seq=3").await);
    let mirror = oswx_core::apply(&at3, &ev.ops, &Rules::default(), None)
        .unwrap()
        .doc;
    let rev_a = content(&server, &doc, &bob, "?seq=1").await;
    assert_eq!(
        oswx_server::content::json_from_plain(&mirror.to_plain()),
        rev_a
    );
    assert_eq!(content(&server, &doc, &bob, "").await, rev_a);
    // Nothing differs: the head.
    assert_eq!(
        restore(&server, &doc, &bob, json!({"seq": 4})).await,
        (200, json!({"seq": 4}))
    );
    // Nothing was lost: the restore itself can be reverted.
    assert_eq!(a.revert(4).await.seq(), 5);
    assert_eq!(depth(&content(&server, &doc, &bob, "").await), 99);
    // Only project owners delete versions.
    let vid = v["id"].as_str().unwrap();
    let path = format!("/api/documents/{doc}/versions/{vid}");
    assert_eq!(server.delete(&path, Some(&bob)).await.0, 403);
    assert_eq!(server.delete(&path, Some(&world.admin)).await.0, 204);
    let (status, body) = server.delete(&path, Some(&world.admin)).await;
    assert_eq!(
        (status.as_u16(), body),
        (404, json!({"detail": "no such version"}))
    );
    let (_, list) = server
        .get(&format!("/api/documents/{doc}/versions"), Some(&vi))
        .await;
    assert_eq!(list.as_array().unwrap().len(), 1);
}

/// JSON content back to a document value.
fn a_value(j: &Json) -> Value {
    value(j.clone())
}

#[tokio::test]
async fn versions_belong_to_their_document() {
    let (server, world) = world().await;
    let out = outsider(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let other = world.document(&server, None, Some("Other"), None).await;
    let (status, v) = new_version(&server, &other, &world.admin, json!({"name": "Theirs"})).await;
    assert_eq!(status, 201);
    // A version of another document is no version here.
    assert_eq!(
        restore(&server, &doc, &world.admin, json!({"version": "Theirs"}))
            .await
            .0,
        404
    );
    assert_eq!(
        content_q(&server, &doc, &world.admin, "?version=Theirs")
            .await
            .0,
        404
    );
    let vid = v["id"].as_str().unwrap();
    assert_eq!(
        server
            .delete(
                &format!("/api/documents/{doc}/versions/{vid}"),
                Some(&world.admin)
            )
            .await
            .0,
        404
    );
    // People outside the project see nothing.
    let (status, body) = server
        .get(&format!("/api/documents/{other}/versions"), Some(&out))
        .await;
    assert_eq!(
        (status.as_u16(), body),
        (404, json!({"detail": "no such document"}))
    );
    assert_eq!(
        new_version(&server, &other, &out, json!({"name": "Mine"}))
            .await
            .0,
        404
    );
    assert_eq!(
        server
            .delete(
                &format!("/api/documents/{other}/versions/{vid}"),
                Some(&out)
            )
            .await
            .0,
        404
    );
    assert_eq!(
        restore(&server, &other, &out, json!({"seq": 0})).await.0,
        404
    );
}

#[tokio::test]
async fn version_names_are_trimmed_when_looked_up_as_when_made() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.change(vec![set_depth(20)], vec![], vec![]).await.seq(), 1);
    let (status, v) = new_version(&server, &doc, &world.admin, json!({"name": "  Rev A "})).await;
    assert_eq!((status, v["name"].clone()), (201, json!("Rev A")));
    assert_eq!(
        depth(&content(&server, &doc, &world.admin, "?version=%20Rev%20A%20%20").await),
        20
    );
    assert_eq!(a.change(vec![set_depth(30)], vec![], vec![]).await.seq(), 2);
    let (status, body) = restore(&server, &doc, &world.admin, json!({"version": " Rev A\t"})).await;
    assert_eq!((status, body), (200, json!({"seq": 3})));
    assert_eq!(depth(&content(&server, &doc, &world.admin, "").await), 20);
}

#[tokio::test]
async fn what_a_restore_needs_to_be_told() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.change(vec![set_depth(20)], vec![], vec![]).await.seq(), 1);
    assert_eq!(
        new_version(
            &server,
            &doc,
            &world.admin,
            json!({"name": "Start", "seq": 0})
        )
        .await
        .0,
        201
    );
    let admin = world.admin.as_str();
    let (status, body) = restore(&server, &doc, admin, json!({})).await;
    assert_eq!(
        (status, body),
        (422, json!({"detail": "give seq or version"}))
    );
    for body in [
        json!({"seq": "1"}),
        json!({"version": 3}),
        json!({"seq": 1.5}),
        json!({"version": "v".repeat(201)}),
    ] {
        assert_eq!(
            restore(&server, &doc, admin, body.clone()).await.0,
            422,
            "{body}"
        );
    }
    for seq in [2, -1, i64::MAX] {
        let (status, body) = restore(&server, &doc, admin, json!({"seq": seq})).await;
        assert_eq!(
            (status, body),
            (
                404,
                json!({"detail": format!("the document has no state {seq}")})
            )
        );
    }
    // Both: the version wins (as in the Python server).
    let (status, body) = restore(&server, &doc, admin, json!({"seq": 1, "version": "Start"})).await;
    assert_eq!((status, body), (200, json!({"seq": 2})));
    assert_eq!(depth(&content(&server, &doc, admin, "").await), 10);
}

#[tokio::test]
async fn restoring_a_python_shaped_document_after_edits() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    let start = content(&server, &doc, &world.admin, "").await;
    assert_eq!(
        (start["next_id"].clone(), start["counters"].clone()),
        (json!(3), json!({"Sketch": 1, "Boss-Extrude": 1}))
    );
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let (op, _) = b.add_feature("Fillet1");
    assert_eq!(b.change(vec![op], vec![], vec![]).await.seq(), 1);
    event(&mut a, 1).await;
    assert_eq!(
        a.change(vec![remove(&["features"], "f1")], vec![], vec![])
            .await
            .seq(),
        2
    );
    assert_eq!(
        a.change(
            vec![set_depth(55), set(&["material", "name"], json!("Steel"))],
            vec![],
            vec![]
        )
        .await
        .seq(),
        3
    );
    let moved = oswx_core::Op::Move {
        list: path(&["features"]),
        id: Arc::from("f2"),
        pos: oswx_core::Pos::between(None, Some(&oswx_core::Pos::spread(2)[0])),
    };
    assert_eq!(a.change(vec![moved], vec![], vec![]).await.seq(), 4);
    let (status, body) = restore(&server, &doc, &bob, json!({"seq": 0})).await;
    assert_eq!((status, body), (200, json!({"seq": 5})));
    let got = content(&server, &doc, &bob, "").await;
    assert_eq!(got, start);
    // Every client converges on it.
    let ev = event(&mut a, 5).await;
    let at4 = a_value(&content(&server, &doc, &bob, "?seq=4").await);
    let applied = oswx_core::apply(&at4, &ev.ops, &Rules::default(), None)
        .unwrap()
        .doc;
    assert_eq!(
        oswx_server::content::json_from_plain(&applied.to_plain()),
        start
    );
}

/// Waits until the open room of `doc` has at least one change being written.
async fn until_tentative(server: &TestServer, doc: &str) {
    for _ in 0..500 {
        if server.room_status(doc).await.tentative > 0 {
            return;
        }
        pause(std::time::Duration::from_millis(10)).await;
    }
    panic!("no change became tentative");
}

#[tokio::test]
async fn a_restore_overwrites_changes_that_were_tentative_when_it_was_judged() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let target = content(&server, &doc, &world.admin, "").await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.change(vec![set_depth(20)], vec![], vec![]).await.seq(), 1);
    // Ada's insert is being written (its commit is reported late) when the restore comes.
    flaky.commit_delay(std::time::Duration::from_millis(800));
    let (op, _) = a.add_feature("Fillet1");
    let insert = a.make_change(1, vec![op]);
    a.send_change(&insert).await;
    until_tentative(&server, &doc).await;
    let (status, body) = restore(&server, &doc, &world.admin, json!({"seq": 0})).await;
    flaky.commit_delay(std::time::Duration::ZERO);
    assert_eq!((status, body.clone()), (200, json!({"seq": 3})), "{body}");
    assert_eq!(a.outcome(&insert.change_id).await.seq(), 2);
    // The document at the restore's seq is exactly the target: the insert did not survive.
    assert_eq!(content(&server, &doc, &world.admin, "?seq=3").await, target);
}

#[tokio::test]
async fn a_restore_puts_back_an_item_removed_while_it_waited() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let target = content(&server, &doc, &world.admin, "").await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let name = ["features", "f1", "name"];
    assert_eq!(
        a.change(vec![set(&name, json!("Base"))], vec![], vec![])
            .await
            .seq(),
        1
    );
    flaky.commit_delay(std::time::Duration::from_millis(800));
    let gone = a.make_change(1, vec![remove(&["features"], "f1")]);
    a.send_change(&gone).await;
    until_tentative(&server, &doc).await;
    let (status, body) = restore(&server, &doc, &world.admin, json!({"seq": 0})).await;
    flaky.commit_delay(std::time::Duration::ZERO);
    assert_eq!((status, body.clone()), (200, json!({"seq": 3})), "{body}");
    assert_eq!(a.outcome(&gone.change_id).await.seq(), 2);
    assert_eq!(content(&server, &doc, &world.admin, "?seq=3").await, target);
}

/// Keeps every event.
#[derive(Default)]
struct Recorder(std::sync::Mutex<Vec<Json>>);

#[async_trait::async_trait]
impl oswx_server::ext::Extensions for Recorder {
    async fn on_event(&self, event: &oswx_server::ext::Event) {
        self.0
            .lock()
            .unwrap()
            .push(serde_json::to_value(event).unwrap());
    }
}

impl Recorder {
    fn restores(&self) -> Vec<Json> {
        let events = self.0.lock().unwrap();
        events
            .iter()
            .filter(|e| e["type"] == "document_restored")
            .cloned()
            .collect()
    }
}

/// A server whose journal waits `ms` before writing each batch, with a flaky store and an event
/// recorder.
async fn slow_journal_server(
    ms: u64,
) -> (TestServer, Arc<FlakyStore>, Arc<Recorder>, World, String) {
    let recorder = Arc::new(Recorder::default());
    let ext = recorder.clone();
    let (server, flaky) = TestServer::start_flaky_full(
        |_| {},
        move |store| {
            oswx_server::journal::JournalWriter::start(
                store.clone(),
                oswx_server::journal::MAX_BATCH,
                std::time::Duration::from_millis(ms),
            )
        },
        move |s| s.ext = ext,
    )
    .await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    (server, flaky, recorder, world, doc)
}

/// Two restores to the same state sent at once ("a double click"); each answer with the
/// document's head seq in the store at the moment it came.
async fn double_restore(server: &TestServer, doc: &str, token: &str) -> Vec<(u16, Json, i64)> {
    let one = |body: Json| {
        let (base, token, doc) = (server.base.clone(), token.to_owned(), doc.to_owned());
        let store = server.state.store.clone();
        tokio::spawn(async move {
            let r = reqwest::Client::new()
                .post(format!("{base}/api/documents/{doc}/restore"))
                .bearer_auth(token)
                .json(&body)
                .send()
                .await
                .unwrap();
            let head = store.document_by_id(&doc).await.unwrap().unwrap().head_seq;
            let status = r.status().as_u16();
            (status, r.json::<Json>().await.unwrap(), head)
        })
    };
    let (a, b) = (one(json!({"seq": 0})), one(json!({"seq": 0})));
    vec![a.await.unwrap(), b.await.unwrap()]
}

#[tokio::test]
async fn a_restore_that_finds_the_target_being_written_answers_once_it_is_durable() {
    let (server, _flaky, recorder, world, doc) = slow_journal_server(500).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.change(vec![set_depth(20)], vec![], vec![]).await.seq(), 1);
    let answers = double_restore(&server, &doc, &world.admin).await;
    // Both are told seq 2, and only once it is durable; one restore was written, so one event.
    for (status, body, head) in &answers {
        assert_eq!((*status, body.clone()), (200, json!({"seq": 2})));
        assert!(*head >= 2, "answered before seq 2 was durable: {answers:?}");
    }
    let events = recorder.restores();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["seq"], 2);
    let (_, page) = history(&server, &doc, &world.admin, "").await;
    assert_eq!(seqs(&page), [2, 1]);
}

#[tokio::test]
async fn when_the_restore_being_waited_for_is_refused_the_other_one_is_written() {
    let (server, flaky, recorder, world, doc) = slow_journal_server(500).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.change(vec![set_depth(20)], vec![], vec![]).await.seq(), 1);
    flaky.refuse_server_change_once();
    let mut answers = double_restore(&server, &doc, &world.admin).await;
    answers.sort_by_key(|(status, _, _)| *status);
    // The first written is refused by the journal (500); the other is not told a seq that was
    // never written: it is judged again and written itself.
    assert_eq!(
        (answers[0].0, answers[0].1.clone()),
        (200, json!({"seq": 2}))
    );
    assert_eq!(answers[1].0, 500);
    assert!(answers[0].2 >= 2);
    let events = recorder.restores();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(
        depth(&content(&server, &doc, &world.admin, "?seq=2").await),
        10
    );
}

#[tokio::test]
async fn presence_goes_on_while_a_restore_is_judged() {
    let hooks = Arc::new(oswx_server::room::Hooks::default());
    let h = hooks.clone();
    let server = TestServer::start_custom(|_| {}, move |s| s.room_hooks = h).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    let ops = (0..300)
        .map(|i| set(&["variables", &format!("v{i}")], json!(i)))
        .collect();
    assert_eq!(a.change(ops, vec![], vec![]).await.seq(), 1);
    hooks.judge_gate.shut();
    let restoring = {
        let (base, token, doc) = (server.base.clone(), world.admin.clone(), doc.clone());
        tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("{base}/api/documents/{doc}/restore"))
                .bearer_auth(token)
                .json(&json!({"seq": 0}))
                .send()
                .await
                .unwrap()
                .status()
        })
    };
    tokio::time::timeout(WAIT, hooks.judge_gate.until_waiting(1))
        .await
        .expect("the restore is being judged");
    a.presence(json!({"tool": "a"})).await;
    let a_id = a.client_id.clone();
    tokio::time::timeout(WAIT, async {
        loop {
            let got: Relayed = b.expect().await;
            if got.kind == "presence" && got.client_id == a_id {
                break;
            }
        }
    })
    .await
    .expect("the presence came");
    assert_eq!(
        hooks.judge_gate.waiting(),
        1,
        "the restore was still being judged"
    );
    assert!(!restoring.is_finished());
    hooks.judge_gate.open();
    assert_eq!(restoring.await.unwrap(), 200);
}

#[tokio::test]
async fn a_restore_during_live_editing_and_without_an_open_room() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com", "Bob").await;
    let doc = world.document(&server, None, None, None).await;
    // Nobody has the document open: the restore opens it.
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.change(vec![set_depth(20)], vec![], vec![]).await.seq(), 1);
    a.close().await;
    server
        .room(&doc)
        .close(oswx_server::room::close::GOING_AWAY, "test", true)
        .await;
    assert!(server.state.hub.loaded(&doc).is_none());
    assert_eq!(
        restore(&server, &doc, &world.admin, json!({"seq": 0})).await,
        (200, json!({"seq": 2}))
    );
    assert_eq!(depth(&content(&server, &doc, &bob, "").await), 10);

    // Bob edits other things while Ada restores: everyone converges.
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut sent = Vec::new();
    for n in 0..20u64 {
        let change = b.make_change(
            b.seq,
            vec![set(
                &["features", "f1", "params", "plane"],
                json!(format!("P{n}")),
            )],
        );
        b.send_change(&change).await;
        sent.push(change.change_id.clone());
    }
    let restoring = {
        let server_base = server.base.clone();
        let token = world.admin.clone();
        let doc = doc.clone();
        tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("{server_base}/api/documents/{doc}/restore"))
                .bearer_auth(token)
                .json(&json!({"seq": 1}))
                .send()
                .await
                .unwrap()
                .status()
        })
    };
    for id in &sent {
        let _ = b.outcome(id).await;
    }
    assert_eq!(restoring.await.unwrap(), 200);
    let head = content_q(&server, &doc, &bob, "").await.1;
    let head_seq = head["seq"].as_u64().unwrap();
    // Ada, who only watched, holds from what she was sent what the server holds.
    let mut mirror = a.welcome.content.clone().unwrap();
    let mut seen = a.welcome.seq;
    while seen < head_seq {
        let ev: ChangeEvent = a.expect().await;
        assert_eq!(ev.seq, seen + 1);
        mirror = oswx_core::apply(&mirror, &ev.ops, &Rules::default(), None)
            .unwrap()
            .doc;
        seen = ev.seq;
    }
    assert_eq!(
        oswx_server::content::json_from_plain(&mirror.to_plain()),
        head["content"]
    );
    assert_eq!(depth(&head["content"]), 20);
    let restores = history(&server, &doc, &bob, "?limit=500")
        .await
        .1
        .as_array()
        .unwrap()
        .iter()
        .filter(|h| h["command"] == "restore to 1")
        .count();
    assert_eq!(restores, 1);
}
