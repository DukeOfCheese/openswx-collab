//! Locks: what they stop, who may take them, how they expire and come back. (The lock tests of
//! `tests/test_live.py` and `tests/test_hardening.py`, with paths for targets, and the v2 rules.)

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use oswx_core::wire::{ClientMsg, DenyReason, Lock, RejectCode, ServerMsg};
use oswx_core::{ConflictKind, Path};
use oswx_server::clock::Clock;
use oswx_server::journal::{self, JournalWriter};
use oswx_server::room::{LockRule, MAX_LOCKS, ServerChangeError, ServerChangeOptions};
use oswx_server::store::Store;
use serde_json::json;

const T0: f64 = 1_800_000_000.0;
const F1: &[&str] = &["features", "f1"];
const F2: &[&str] = &["features", "f2"];

async fn bob(server: &TestServer, world: &World) -> Token {
    world
        .invite(
            server,
            "bob@example.com",
            Some("Bob"),
            Some("editor"),
            "member",
        )
        .await
        .0
}

async fn viewer(server: &TestServer, world: &World) -> Token {
    world
        .invite(
            server,
            "vi@example.com",
            Some("Vi"),
            Some("viewer"),
            "member",
        )
        .await
        .0
}

/// A server on a manual clock, with a world and a document.
async fn on_a_clock() -> (TestServer, World, Clock, String) {
    let clock = Clock::manual(T0);
    let c = clock.clone();
    let server = TestServer::start_custom(|_| {}, move |s| s.clock = c).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    (server, world, clock, doc)
}

fn paths_of(locks: &[Lock]) -> Vec<Path> {
    let mut out: Vec<Path> = locks.iter().map(|l| l.path.clone()).collect();
    out.sort();
    out
}

fn set_name(id: &str, name: &str) -> oswx_core::Op {
    set(&["features", id, "name"], json!(name))
}

fn mv(id: &str) -> oswx_core::Op {
    oswx_core::Op::Move {
        list: path(&["features"]),
        id: Arc::from(id),
        pos: oswx_core::Pos::between(None, Some(&oswx_core::Pos::spread(2)[0])),
    }
}

/// Locks the 256 top-level keys from `k<from>` on; answers the result.
async fn lock_keys(live: &mut Live, from: usize) -> LockResult {
    let keys: Vec<String> = (from..from + 256).map(|n| format!("k{n}")).collect();
    let paths: Vec<[&str; 1]> = keys.iter().map(|k| [k.as_str()]).collect();
    let refs: Vec<&[&str]> = paths.iter().map(|p| &p[..]).collect();
    live.lock(&refs).await
}

/// The next `lock.changed`, and no second one soon after.
async fn expect_one_lock_news(live: &mut Live) -> LockChanged {
    let news: LockChanged = live.expect().await;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            break;
        }
        match live.try_recv(left).await {
            Some(ServerMsg::LockChanged { .. }) => panic!("a second lock.changed came"),
            Some(other) => live.seen.push(other),
            None => break,
        }
    }
    news
}

/// No `lock.changed` came to `live` up to now.
async fn no_lock_news(live: &mut Live) {
    let seen = live.drain().await;
    assert!(
        !seen
            .iter()
            .any(|m| matches!(m, ServerMsg::LockChanged { .. })),
        "{seen:?}"
    );
}

#[tokio::test]
async fn locks_block_others_until_released() {
    let (server, world) = world().await;
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;

    let got = a.lock(&[F2, &["features", "missing"]]).await;
    assert_eq!(got.request_id, "r");
    assert_eq!(got.granted, vec![path(F2)]);
    assert_eq!(got.holder(&["features", "missing"]), None);
    let news = expect_one_lock_news(&mut b).await;
    assert_eq!(news.locks.len(), 1);
    let held = &news.locks[0];
    assert_eq!(
        (
            &held.path,
            &*held.client_id,
            held.user_id.as_str(),
            held.name.as_str(),
            held.detached
        ),
        (
            &path(F2),
            a.client_id.as_str(),
            world.admin_id.as_str(),
            "Ada",
            false
        )
    );
    assert!(news.released.is_empty());
    // (The holder hears of its own lock too.)
    let _: LockChanged = a.expect().await;

    let denied = b.lock(&[F2, F1]).await;
    assert_eq!(denied.granted, vec![path(F1)]);
    assert_eq!(denied.holder(F2).map(|l| l.name.as_str()), Some("Ada"));

    let refused = b.change_on(0, vec![set_depth(99)]).await;
    let Outcome::Rejected {
        code,
        message,
        conflicts,
    } = refused
    else {
        panic!("refused")
    };
    assert_eq!(code, RejectCode::Locked);
    assert!(message.contains("Ada"), "{message}");
    assert_eq!(conflicts.len(), 1);
    assert_eq!(
        (&conflicts[0].path, &*conflicts[0].by, conflicts[0].kind),
        (&path(F2), a.client_id.as_str(), ConflictKind::Locked)
    );
    assert_eq!(
        b.change_on(0, vec![remove(&["features"], "f2")])
            .await
            .code(),
        RejectCode::Locked
    );
    // The holder edits freely.
    assert_eq!(a.change_on(0, vec![set_depth(30)]).await.seq(), 1);

    // A newcomer is told who holds what.
    let c = Live::connect(&server, &doc, &bob).await;
    assert_eq!(paths_of(&c.welcome.locks), vec![path(F1), path(F2)]);

    a.unlock(&[F2]).await;
    let news: LockChanged = loop {
        let n: LockChanged = b.expect().await;
        if !n.released.is_empty() {
            break n;
        }
    };
    assert_eq!(news.released, vec![path(F2)]);
    assert_eq!(b.change_on(1, vec![set_depth(99)]).await.seq(), 2);
}

#[tokio::test]
async fn a_list_lock_stops_inserts_removals_and_moves_only() {
    let (server, world) = world().await;
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(
        a.lock(&[&["features"]]).await.granted,
        vec![path(&["features"])]
    );
    assert_eq!(
        b.change_on(0, vec![mv("f2")]).await.code(),
        RejectCode::Locked
    );
    assert_eq!(
        b.change_on(0, vec![remove(&["features"], "f2")])
            .await
            .code(),
        RejectCode::Locked
    );
    let (add, _) = b.add_feature("Fillet1");
    assert_eq!(b.change_on(0, vec![add]).await.code(), RejectCode::Locked);
    // Edits inside an item are not the list's structure.
    assert_eq!(b.change_on(0, vec![set_depth(12)]).await.seq(), 1);
    assert_eq!(b.change_on(1, vec![set_name("f1", "Base")]).await.seq(), 2);
    // A list's lock and its items' locks go together: Bob may lock a feature (not deeper) ...
    assert_eq!(b.lock(&[F1]).await.granted, vec![path(F1)]);
    let deeper = b.lock(&[&["features", "f2", "params"]]).await;
    assert_eq!(deeper.why(&["features", "f2", "params"]), DenyReason::Held);
    // ... and then Ada, holding the list, may not remove or move it, nor Bob replace it whole.
    assert_eq!(
        a.change_on(2, vec![remove(&["features"], "f1")])
            .await
            .code(),
        RejectCode::Locked
    );
    assert_eq!(
        a.change_on(2, vec![mv("f1")]).await.code(),
        RejectCode::Locked
    );
    let whole = set(F1, json!({"id": "f1", "name": "Sketch1", "type": "sketch"}));
    assert_eq!(b.change_on(2, vec![whole]).await.code(), RejectCode::Locked);
    // Edits inside it are Bob's to make.
    assert_eq!(b.change_on(2, vec![set_name("f1", "Mine")]).await.seq(), 3);
    // The list's holder reorders what nobody else holds.
    assert_eq!(a.change_on(3, vec![mv("f2")]).await.seq(), 4);
}

#[tokio::test]
async fn a_list_lock_and_a_lock_deeper_than_an_item_exclude_each_other() {
    let (server, world) = world().await;
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let params: &[&str] = &["features", "f1", "params"];
    assert_eq!(a.lock(&[params]).await.granted.len(), 1);
    let got = b.lock(&[&["features"]]).await;
    assert_eq!(got.holder(&["features"]).unwrap().path, path(params));
    a.unlock(&[params]).await;
    assert_eq!(a.lock(&[&["features"]]).await.granted.len(), 1);
    let got = b.lock(&[params]).await;
    assert_eq!(got.holder(params).unwrap().path, path(&["features"]));
}

#[tokio::test]
async fn a_change_at_under_or_above_a_lock_is_locked() {
    let (server, world) = world().await;
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let params: &[&str] = &["features", "f2", "params"];
    assert_eq!(a.lock(&[params]).await.granted.len(), 1);
    for (what, op) in [
        ("under", set_depth(1)),
        ("at", set(params, json!({"depth": 1}))),
        (
            "above",
            set(F2, json!({"id": "f2", "name": "X", "type": "t"})),
        ),
        ("above, removed", remove(&["features"], "f2")),
    ] {
        let Outcome::Rejected {
            code, conflicts, ..
        } = b.change_on(0, vec![op]).await
        else {
            panic!("{what}: accepted")
        };
        assert_eq!(code, RejectCode::Locked, "{what}");
        assert_eq!(conflicts[0].path, path(params), "{what}");
    }
    // A sibling, and the item's place in its list, are free.
    assert_eq!(b.change_on(0, vec![set_name("f2", "Boss")]).await.seq(), 1);
    assert_eq!(b.change_on(1, vec![mv("f2")]).await.seq(), 2);
    // The holder's own changes pass.
    assert_eq!(a.change_on(2, vec![set_depth(7)]).await.seq(), 3);
}

#[tokio::test]
async fn nested_locks_one_client_may_hold_another_may_not_take() {
    let (server, world) = world().await;
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(a.lock(&[F1]).await.granted.len(), 1);
    let got = b
        .lock(&[
            &["features", "f1", "params"],
            F1,
            &["features", "f1", "name"],
        ])
        .await;
    assert!(got.granted.is_empty());
    for p in [
        &["features", "f1", "params"][..],
        F1,
        &["features", "f1", "name"],
    ] {
        assert_eq!(got.holder(p).unwrap().path, path(F1), "{p:?}");
    }
    // Ada may nest hers; asking again for one she holds is granted.
    let got = a
        .lock(&[&["features", "f1", "params"], &["features"], F1])
        .await;
    assert_eq!(got.granted.len(), 3);
    assert!(got.denied.is_empty());
}

#[tokio::test]
async fn what_may_be_locked_and_by_whom() {
    let (server, world) = world().await;
    let vi = viewer(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut v = Live::connect(&server, &doc, &vi).await;
    let got = v.lock(&[F1, &["display"]]).await;
    assert!(got.granted.is_empty());
    assert_eq!((got.holder(F1), got.holder(&["display"])), (None, None));
    assert_eq!(got.why(F1), DenyReason::ReadOnly);
    let got = a
        .lock(&[
            &["features", "f9"],
            &["features", "f1", "~cache"],
            &["~derived"],
            &["material", "colour"],
            &["display"],
            &["material", "density"],
            // Keys the server owns.
            &["rollback"],
            &["journal"],
            &["active_config"],
        ])
        .await;
    assert_eq!(
        got.granted,
        vec![path(&["display"]), path(&["material", "density"])],
        "a new top-level key and an existing value"
    );
    for p in [
        &["features", "f9"][..],
        &["features", "f1", "~cache"],
        &["~derived"],
        &["material", "colour"],
        &["rollback"],
        &["journal"],
        &["active_config"],
    ] {
        assert_eq!(got.holder(p), None, "{p:?}");
        assert_eq!(got.why(p), DenyReason::NotLockable, "{p:?}");
    }
}

#[tokio::test]
async fn a_key_locked_before_it_exists_stops_others_creating_it() {
    let (server, world) = world().await;
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(a.lock(&[&["display"]]).await.granted.len(), 1);
    let make = set(&["display"], json!({"mode": "shaded"}));
    assert_eq!(
        b.change_on(0, vec![make.clone()]).await.code(),
        RejectCode::Locked
    );
    assert_eq!(a.change_on(0, vec![make]).await.seq(), 1);
}

#[tokio::test]
async fn server_changes_are_stopped_by_locks_only_when_they_say_so() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    a.lock(&[F2]).await;
    let room = server.room(&doc);
    let seq = room
        .server_change(
            vec![set_depth(4)],
            "test",
            &world.admin_id,
            "Ada",
            ServerChangeOptions::default(),
        )
        .await;
    assert_eq!(seq, Ok(1));
}

#[tokio::test]
async fn locks_are_bounded_per_client() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let keys: Vec<String> = (0..MAX_LOCKS).map(|n| format!("key{n}")).collect();
    let paths: Vec<[&str; 1]> = keys.iter().map(|k| [k.as_str()]).collect();
    let refs: Vec<&[&str]> = paths.iter().map(|p| &p[..]).collect();
    let got = a.lock(&refs).await;
    assert_eq!((got.granted.len(), got.denied.len()), (256, 0));
    let got = a.lock(&[&["one-more"]]).await;
    assert!(got.granted.is_empty());
    assert_eq!(got.holder(&["one-more"]), None);
    assert_eq!(got.why(&["one-more"]), DenyReason::Quota);
    // Holding one again is still fine.
    assert_eq!(a.lock(&[&["key7"]]).await.granted.len(), 1);
    assert_eq!(server.room_status(&doc).await.locks, (256, 0));
}

#[tokio::test]
async fn the_quota_holds_across_a_disconnect_and_per_user() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    assert_eq!(lock_keys(&mut a, 0).await.granted.len(), 256);
    a.close().await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    assert_eq!(a.welcome.locks.len(), 256);
    assert_eq!(
        a.lock(&[&["one-more"]]).await.why(&["one-more"]),
        DenyReason::Quota
    );
    // Three more apps of Ada's take 256 each; a fifth gets none: 1,024 per person.
    for (n, app) in ["ada-laptop", "ada-tablet", "ada-phone", "ada-spare"]
        .iter()
        .enumerate()
    {
        let mut other = Live::connect_with(&server, &doc, &world.admin, Hello::client(app)).await;
        let got = lock_keys(&mut other, 256 * (n + 1)).await;
        if n < 3 {
            assert_eq!(got.granted.len(), 256, "{app}");
        } else {
            assert!(got.granted.is_empty(), "{app}");
            assert!(got.denied.iter().all(|d| d.why == DenyReason::Quota));
        }
    }
    // (The other apps went at the end of each turn: their locks wait for them.)
    assert_eq!(server.room_status(&doc).await.locks.0, 1024);
}

#[tokio::test]
async fn a_release_by_someone_else_is_ignored() {
    let (server, world) = world().await;
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    a.lock(&[F2]).await;
    expect_one_lock_news(&mut b).await;
    b.unlock(&[F2, &["nothing"]]).await;
    no_lock_news(&mut b).await;
    assert_eq!(
        b.change_on(0, vec![set_depth(3)]).await.code(),
        RejectCode::Locked
    );
}

#[tokio::test]
async fn a_lock_dies_after_45_seconds_of_silence_and_a_ping_keeps_it() {
    let (server, world, clock, doc) = on_a_clock().await;
    let bob = bob(&server, &world).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    a.lock(&[F1]).await;
    let _: LockChanged = b.expect().await;
    let room = server.room(&doc);
    clock.advance(30.0);
    a.drain().await; // (a ping)
    clock.advance(30.0);
    room.tick().await;
    no_lock_news(&mut b).await;
    assert_eq!(server.room_status(&doc).await.locks, (1, 0));
    clock.advance(16.0); // 46 s since the ping
    room.tick().await;
    let news = expect_one_lock_news(&mut b).await;
    assert_eq!(news.released, vec![path(F1)]);
    assert!(!room.holds_locks());
}

#[tokio::test]
async fn a_lapsed_lock_is_announced_when_someone_else_edits() {
    // (No sweep: only the edit notices.)
    let clock = Clock::manual(T0);
    let c = clock.clone();
    let server =
        TestServer::start_custom(|s| s.sweep_interval = 3600.0, move |s| s.clock = c).await;
    let world = World::new(&server).await.unwrap();
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    a.lock(&[F2]).await;
    let _: LockChanged = b.expect().await;
    clock.advance(46.0); // Ada goes silent
    assert_eq!(b.change_on(0, vec![set_depth(5)]).await.seq(), 1);
    assert!(b.seen.iter().any(|m| matches!(
        m,
        ServerMsg::LockChanged { released, .. } if *released == vec![path(F2)]
    )));
}

#[tokio::test]
async fn a_disconnected_clients_locks_wait_for_it_then_go() {
    let (server, world, clock, doc) = on_a_clock().await;
    let bob = bob(&server, &world).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    a.lock(&[F1]).await;
    let _: LockChanged = b.expect().await;
    let ada = a.client_id.clone();
    a.close().await; // her wifi drops
    let left: PeerLeft = b.expect().await;
    assert_eq!(left.0, ada);
    let news = expect_one_lock_news(&mut b).await;
    assert_eq!(paths_of(&news.locks), vec![path(F1)]);
    assert!(news.locks[0].detached);
    let room = server.room(&doc);
    assert_eq!(room.detached_locks(), 1);
    // Her sketch is still hers during the grace period.
    let got = b.lock(&[F1]).await;
    let holder = got.holder(F1).unwrap();
    assert_eq!((holder.name.as_str(), holder.detached), ("Ada", true));
    assert_eq!(
        b.change_on(0, vec![set_name("f1", "Mine")]).await.code(),
        RejectCode::Locked
    );

    clock.advance(50.0);
    room.tick().await;
    let a2 = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    assert_eq!(a2.welcome.locks.len(), 1);
    assert!(!a2.welcome.locks[0].detached);
    let news = expect_one_lock_news(&mut b).await;
    assert_eq!(
        (paths_of(&news.locks), news.locks[0].detached),
        (vec![path(F1)], false)
    );
    assert_eq!(room.detached_locks(), 0);

    // Gone again, and for longer than the grace period this time.
    a2.close().await;
    let _: PeerLeft = b.expect().await;
    let _: LockChanged = b.expect().await;
    clock.advance(59.0);
    room.tick().await;
    no_lock_news(&mut b).await;
    assert!(room.holds_locks(), "a room with a waiting lock keeps it");
    clock.advance(2.0);
    room.tick().await;
    let news = expect_one_lock_news(&mut b).await;
    assert_eq!(news.released, vec![path(F1)]);
    assert!(!room.holds_locks());
    let c = Live::connect(&server, &doc, &world.admin).await;
    assert!(c.welcome.locks.is_empty());
}

#[tokio::test]
async fn a_client_back_with_only_view_access_loses_its_locks_at_once() {
    let (server, world) = world().await;
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    a.lock(&[F1]).await;
    let _: LockChanged = b.expect().await;
    a.close().await;
    let _: PeerLeft = b.expect().await;
    let _: LockChanged = b.expect().await;
    // Back with an app of another file format: it may only look.
    let hello = Hello::client("ada-desktop").format_version(Some(2));
    let a2 = Live::connect_with(&server, &doc, &world.admin, hello).await;
    assert!(a2.welcome.read_only);
    assert!(a2.welcome.locks.is_empty());
    let news = expect_one_lock_news(&mut b).await;
    assert_eq!(news.released, vec![path(F1)]);
}

#[tokio::test]
async fn a_lock_on_a_removed_item_goes() {
    let (server, world) = world().await;
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    a.lock(&[F2, &["features", "f2", "params"], &["display"]])
        .await;
    let _: LockChanged = b.expect().await;
    assert_eq!(
        a.change_on(0, vec![remove(&["features"], "f2")])
            .await
            .seq(),
        1
    );
    let _: oswx_core::wire::ChangeEvent = b.expect().await;
    let news = expect_one_lock_news(&mut b).await;
    assert_eq!(
        news.released,
        vec![path(F2), path(&["features", "f2", "params"])]
    );
    assert_eq!(server.room_status(&doc).await.locks, (1, 0));
}

#[tokio::test]
async fn a_second_connection_of_a_client_keeps_its_locks_without_a_flicker() {
    let (server, world) = world().await;
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    a.lock(&[F1]).await;
    let _: LockChanged = b.expect().await;
    let a2 = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    assert_eq!(a.closed().await.0, 4409);
    assert_eq!(a2.welcome.locks.len(), 1);
    assert!(!a2.welcome.locks[0].detached);
    let seen = b.drain().await;
    assert!(
        seen.iter().all(|m| !matches!(
            m,
            ServerMsg::PeerLeft { .. } | ServerMsg::LockChanged { .. }
        )),
        "{seen:?}"
    );
    assert_eq!(server.room_status(&doc).await.locks, (1, 0));
    assert_eq!(
        b.change_on(0, vec![set_name("f1", "Mine")]).await.code(),
        RejectCode::Locked
    );
}

#[tokio::test]
async fn a_change_judged_again_after_a_refused_record_meets_the_locks_of_then() {
    let slow = |store: &Arc<dyn Store>| {
        JournalWriter::start(
            store.clone(),
            journal::MAX_BATCH,
            Duration::from_millis(400),
        )
    };
    let (server, flaky) = TestServer::start_flaky_journal(|_| {}, slow).await;
    let world = World::new(&server).await.unwrap();
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    // Bob's first change will be refused by the journal; his second, decided on top of it, is
    // judged again then.
    let first = b.make_change(0, vec![set(&["variables", "x"], json!(1))]);
    let second = b.make_change(0, vec![set_name("f1", "Base")]);
    flaky.refuse_change_once(&first.change_id);
    b.send_change(&first).await;
    b.send_change(&second).await;
    // Meanwhile (both are still in the journal's batch) Ada locks what the second writes.
    assert_eq!(a.lock(&[F1]).await.granted.len(), 1);
    assert_eq!(
        b.outcome(&first.change_id).await.code(),
        RejectCode::ServerError
    );
    assert_eq!(
        b.outcome(&second.change_id).await.code(),
        RejectCode::Locked
    );
}

#[tokio::test]
async fn the_server_says_which_locks_stop_its_own_changes() {
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
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let _b = Live::connect(&server, &doc, &bob).await;
    a.lock(&[F2]).await;
    let room = server.room(&doc);
    let change = |rule: LockRule, author: String, op: oswx_core::Op| {
        let room = room.clone();
        async move {
            let options = ServerChangeOptions {
                check_stale: false,
                locks: rule,
            };
            room.server_change(vec![op], "test", &author, "X", options)
                .await
        }
    };
    let admin = world.admin_id.clone();
    // The rule for any change: Ada's lock is in the way.
    let Err(ServerChangeError::Locked(conflicts)) =
        change(LockRule::Writes, admin.clone(), set_depth(1)).await
    else {
        panic!("locked")
    };
    assert_eq!(
        (&conflicts[0].path, &*conflicts[0].by),
        (&path(F2), a.client_id.as_str())
    );
    assert!(
        change(LockRule::Writes, admin.clone(), set_name("f1", "A"))
            .await
            .is_ok()
    );
    // Made for Ada's client: hers do not count.
    let mine = LockRule::WritesExcept(Arc::from(a.client_id.as_str()));
    assert!(change(mine, admin.clone(), set_depth(2)).await.is_ok());
    // A restore waits for everyone else's locks, wherever they are.
    assert!(matches!(
        change(LockRule::AnyOfOtherUsers, bob_id, set_name("f1", "B")).await,
        Err(ServerChangeError::Locked(_))
    ));
    assert!(
        change(LockRule::AnyOfOtherUsers, admin, set_name("f1", "C"))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn hostile_lock_requests_are_refused_by_the_codec_and_the_connection_kept() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let many: Vec<Path> = (0..257).map(|n| path(&[&format!("k{n}")])).collect();
    let deep: Path = (0..65).map(|_| Arc::from("a")).collect();
    let long: Path = vec![Arc::from("x".repeat(100 * 1024))];
    for paths in [many, vec![deep], vec![long]] {
        a.send(&ClientMsg::LockAcquire {
            request_id: "r".into(),
            paths,
        })
        .await;
        let err: ErrorMsg = a.expect().await;
        assert_eq!(err.code, "bad_message");
    }
    assert!(a.lock(&[F1]).await.granted.len() == 1, "still connected");
    assert_eq!(server.room_status(&doc).await.locks, (1, 0));
}

#[tokio::test]
async fn lock_requests_are_paid_for_per_path() {
    let (server, world, _clock, doc) = on_a_clock().await;
    let bob = bob(&server, &world).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    // A thousand one-path acquire/release pairs at one moment: the 512 path-tokens pay for 256.
    let n = 1000;
    for i in 0..n {
        let key = format!("k{i}");
        a.send_lock(&[&[key.as_str()]]).await;
        a.unlock(&[&[key.as_str()]]).await;
    }
    let mut results = Vec::new();
    while results.len() < n {
        let r: LockResult = a.expect().await;
        results.push(r);
    }
    assert!(results[..256].iter().all(|r| r.granted.len() == 1));
    assert!(
        results[256..]
            .iter()
            .all(|r| r.denied.len() == 1 && r.denied[0].why == DenyReason::Rate)
    );
    let news = b
        .drain()
        .await
        .into_iter()
        .filter(|m| matches!(m, ServerMsg::LockChanged { .. }))
        .count();
    assert_eq!(news, 512, "one announcement per grant and per paid release");
    assert_eq!(server.room_status(&doc).await.locks, (0, 0));
}

#[tokio::test]
async fn releases_beyond_the_allowance_are_honoured_and_announced_together() {
    let (server, world, _clock, doc) = on_a_clock().await;
    let bob = bob(&server, &world).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(lock_keys(&mut a, 0).await.granted.len(), 256);
    expect_one_lock_news(&mut b).await;
    // Asking again for one it holds, 256 times over, spends the rest of the allowance.
    let again: Vec<&[&str]> = vec![&["k0"][..]; 256];
    assert_eq!(a.lock(&again).await.granted.len(), 256);
    let keys: Vec<String> = (0..256).map(|n| format!("k{n}")).collect();
    let paths: Vec<[&str; 1]> = keys.iter().map(|k| [k.as_str()]).collect();
    let refs: Vec<&[&str]> = paths.iter().map(|p| &p[..]).collect();
    a.unlock(&refs).await;
    a.drain().await;
    assert_eq!(
        server.room_status(&doc).await.locks,
        (0, 0),
        "honoured at once"
    );
    let news = expect_one_lock_news(&mut b).await;
    assert_eq!(news.released.len(), 256, "announced together");
    // Out of tokens, an acquire is refused for the rate, distinctly.
    let got = a.lock(&[&["k1"]]).await;
    assert_eq!(got.why(&["k1"]), DenyReason::Rate);
}

#[tokio::test]
async fn one_editor_cycling_locks_cannot_flood_the_others() {
    let (server, world, clock, doc) = on_a_clock().await;
    let bob = bob(&server, &world).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let keys: Vec<String> = (0..256).map(|n| format!("k{n}")).collect();
    let paths: Vec<[&str; 1]> = keys.iter().map(|k| [k.as_str()]).collect();
    let refs: Vec<&[&str]> = paths.iter().map(|p| &p[..]).collect();
    // Sixty 256-path acquire/release cycles a second, for two seconds.
    for _ in 0..120 {
        a.send_lock(&refs).await;
        a.unlock(&refs).await;
        clock.advance(1.0 / 60.0);
    }
    a.drain().await;
    tokio::time::sleep(Duration::from_millis(300)).await; // (any late announcement)
    let msgs = b.drain().await;
    assert!(b.closed.is_none(), "the peer is not dropped as too slow");
    let news: Vec<&ServerMsg> = msgs
        .iter()
        .filter(|m| matches!(m, ServerMsg::LockChanged { .. }))
        .collect();
    let bytes: usize = news
        .iter()
        .map(|m| oswx_core::wire::encode_server(m).len())
        .sum();
    assert!(news.len() <= 4, "{} announcements", news.len());
    assert!(bytes < 100_000, "{bytes} bytes of lock news");
    assert_eq!(server.room_status(&doc).await.locks, (0, 0));
}

#[tokio::test]
async fn a_client_back_after_the_grace_period_gets_nothing_back() {
    // (No sweep: only the join itself can notice the grace period is over.)
    let clock = Clock::manual(T0);
    let c = clock.clone();
    let server =
        TestServer::start_custom(|s| s.sweep_interval = 3600.0, move |s| s.clock = c).await;
    let world = World::new(&server).await.unwrap();
    let bob = bob(&server, &world).await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    a.lock(&[F1]).await;
    let _: LockChanged = b.expect().await;
    a.close().await;
    let _: PeerLeft = b.expect().await;
    let _: LockChanged = b.expect().await;
    clock.advance(61.0);
    let a2 = Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-desktop")).await;
    assert!(a2.welcome.locks.is_empty(), "{:?}", a2.welcome.locks);
    let news = expect_one_lock_news(&mut b).await;
    assert_eq!((news.locks.len(), news.released), (0, vec![path(F1)]));
}
