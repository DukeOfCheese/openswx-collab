//! What takes people out of documents and documents out of memory: the immediate drops when access
//! changes (inventory §5 "Hub", "Immediate drops"), a session expiring while connected, the
//! per-account connection limit, the sweeper's idle unload, and shutdown. Also the steps after a
//! REST commit, which run to the end even when the request is dropped.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::*;
use oswx_core::Seq;
use oswx_core::wire::ServerMsg;
use oswx_server::clock::Clock;
use oswx_server::room::Hooks;
use oswx_server::store::Id;
use serde_json::{Value, json};

const T0: f64 = 1_800_000_000.0;

/// A server on a manual clock whose own timers never fire (the test sweeps and ticks by hand).
async fn on_a_clock(settings: impl FnOnce(&mut oswx_server::Settings)) -> (TestServer, Clock) {
    let clock = Clock::manual(T0);
    let c = clock.clone();
    let server = TestServer::start_custom(
        |s| {
            s.sweep_interval = 3600.0;
            settings(s);
        },
        move |s| s.clock = c,
    )
    .await;
    (server, clock)
}

async fn editor(server: &TestServer, world: &World, email: &str) -> (Token, Id) {
    world.editor(server, email).await
}

async fn content(server: &TestServer, doc: &str, token: &str) -> Value {
    let (status, body) = server
        .get(&format!("/api/documents/{doc}/content"), Some(token))
        .await;
    assert_eq!(status, 200, "{body}");
    body
}

/// Asserts the socket is closed with `code` and `reason`, and that nothing at all came before the
/// close (call [`quiet`] before the drop).
async fn closed_with_nothing_before(live: &mut Live, code: u16, reason: &str) {
    let closed = live.closed().await;
    assert_eq!(closed, (code, reason.to_owned()));
    assert!(
        live.seen.is_empty(),
        "nothing but the close after a drop: {:?}",
        live.seen
    );
}

/// Reads everything the server has sent so far, then forgets it.
async fn quiet(live: &mut Live) {
    live.drain().await;
    live.seen.clear();
}

/// Whether the connection is still answered.
async fn alive(live: &mut Live) -> bool {
    live.send(&oswx_core::wire::ClientMsg::Ping).await;
    loop {
        match live.try_recv(WAIT).await {
            Some(ServerMsg::Pong { .. }) => return true,
            Some(other) => live.seen.push(other),
            None => return false,
        }
    }
}

/// A change made by `live` and accepted.
async fn edit(live: &mut Live, n: i64) -> Seq {
    live.change(vec![set_depth(n)], vec![], vec![]).await.seq()
}

/// Asks the hub's sweeper to go round once, and answers whether `doc`'s room unloaded.
async fn sweep_unloads(server: &TestServer, doc: &str) -> bool {
    let room = server.room(doc);
    assert!(server.state.hub.sweep() >= 1);
    // (Commands are taken in order: a room that unloaded answers nothing.)
    room.status().await.is_none()
}

/// Waits until the account has `n` registered connections.
async fn connections_become(server: &TestServer, user: &str, n: usize) {
    let deadline = Instant::now() + WAIT;
    while server.state.hub.connections_of(user) != n {
        assert!(
            Instant::now() < deadline,
            "{} connections, not {n}",
            server.state.hub.connections_of(user)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn set_project_role(server: &TestServer, world: &World, user: &str, role: &str) {
    let (status, body) = server
        .put(
            &format!("/api/projects/{}/members/{user}", world.project),
            Some(&world.admin),
            json!({"role": role}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
}

async fn set_org_role(server: &TestServer, world: &World, user: &str, role: &str) {
    let (status, body) = server
        .patch(
            &format!("/api/orgs/{}/members/{user}", world.org),
            Some(&world.admin),
            json!({"role": role}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
}

async fn change_password(server: &TestServer, token: &str) {
    let (status, body) = server
        .patch(
            "/api/me",
            Some(token),
            json!({"password": "a new password", "current_password": PASSWORD}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
}

fn refused_with(refused: &Result<Live, Refused>, code: u16) {
    match refused {
        Ok(_) => panic!("the connection was not refused"),
        Err(r) => assert_eq!(r.close, Some(code), "{r:?}"),
    }
}

// ------------------------------------------------------------------ immediate drops

#[tokio::test]
async fn a_password_change_closes_every_connection_of_the_account_and_no_one_elses() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com").await;
    let bob_laptop = server.login("bob@example.com", PASSWORD).await;
    let one = world.document(&server, None, Some("One"), None).await;
    let two = world.document(&server, None, Some("Two"), None).await;
    let mut b1 = Live::connect(&server, &one, &bob).await;
    let mut b2 = Live::connect(&server, &two, &bob_laptop).await;
    let mut a = Live::connect(&server, &one, &world.admin).await;
    quiet(&mut b1).await;
    quiet(&mut b2).await;
    change_password(&server, &bob).await;
    // Ada edits right after: the dropped sockets see none of it.
    assert_eq!(edit(&mut a, 7).await, 1);
    closed_with_nothing_before(&mut b1, 4401, "sign in again").await;
    closed_with_nothing_before(&mut b2, 4401, "sign in again").await;
    assert!(alive(&mut a).await);
    // Both of Bob's sessions are over (the one that changed it was kept for REST, but reconnecting
    // the documents takes the new password's sign-in on the other device).
    refused_with(
        &Live::try_connect(&server, &one, &bob_laptop, Hello::default()).await,
        4401,
    );
}

#[tokio::test]
async fn a_password_reset_closes_the_accounts_connections() {
    let (server, mailer) = TestServer::start_mailing(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, _) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    quiet(&mut b).await;
    let (status, _) = server
        .post(
            "/api/auth/password-reset",
            None,
            json!({"email": "bob@example.com"}),
        )
        .await;
    assert_eq!(status, 202);
    let text = mailer.sent().last().expect("the reset mail").text.clone();
    let token = text
        .split_whitespace()
        .find(|w| w.contains("/reset-password/"))
        .and_then(|link| link.rsplit('/').next())
        .expect("a reset link")
        .to_owned();
    let (status, body) = server
        .post(
            "/api/auth/password-reset/confirm",
            None,
            json!({"token": token, "password": "another password"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(edit(&mut a, 3).await, 1);
    closed_with_nothing_before(&mut b, 4401, "sign in again").await;
    assert!(alive(&mut a).await);
}

#[tokio::test]
async fn signing_out_closes_only_the_connections_of_that_session() {
    let (server, world) = world().await;
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let bob_laptop = server.login("bob@example.com", PASSWORD).await;
    let doc = world.document(&server, None, None, None).await;
    let mut desk = Live::connect(&server, &doc, &bob).await;
    let mut laptop = Live::connect(&server, &doc, &bob_laptop).await;
    quiet(&mut desk).await;
    let (status, _) = server.post("/api/auth/logout", Some(&bob), json!({})).await;
    assert_eq!(status, 204);
    closed_with_nothing_before(&mut desk, 4401, "sign in again").await;
    assert!(alive(&mut laptop).await);
    connections_become(&server, &bob_id, 1).await;
}

#[tokio::test]
async fn an_org_role_change_closes_only_connections_whose_role_changed() {
    let (server, world) = world().await;
    // Bob edits as a member; Cy owns every project as an admin.
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let (cy, cy_id) = world
        .invite(&server, "cy@example.com", None, None, "admin")
        .await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut c = Live::connect(&server, &doc, &cy).await;
    assert_eq!(c.welcome.role, "owner");
    quiet(&mut b).await;
    // Admin to owner: still the project's owner. Cy stays.
    set_org_role(&server, &world, &cy_id, "owner").await;
    assert!(alive(&mut c).await);
    // Member to admin: Bob's role in the project becomes owner. Bob goes.
    set_org_role(&server, &world, &bob_id, "admin").await;
    closed_with_nothing_before(&mut b, 4403, "your access changed").await;
    assert!(alive(&mut c).await);
    let b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(b.welcome.role, "owner");
}

#[tokio::test]
async fn removal_from_the_organisation_closes_the_members_connections() {
    let (server, world) = world().await;
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    quiet(&mut b).await;
    let (status, _) = server
        .delete(
            &format!("/api/orgs/{}/members/{bob_id}", world.org),
            Some(&world.admin),
        )
        .await;
    assert_eq!(status, 204);
    assert_eq!(edit(&mut a, 4).await, 1);
    closed_with_nothing_before(&mut b, 4403, "your access changed").await;
    refused_with(
        &Live::try_connect(&server, &doc, &bob, Hello::default()).await,
        4404,
    );
}

#[tokio::test]
async fn a_viewer_promoted_to_editor_is_dropped_once_and_edits_after_reconnecting() {
    let (server, world) = world().await;
    let (vi, vi_id) = world
        .invite(&server, "vi@example.com", None, Some("viewer"), "member")
        .await;
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut v = Live::connect(&server, &doc, &vi).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert!(v.welcome.read_only);
    quiet(&mut v).await;
    // Bob's role is set to what it is already: nothing changes for him.
    set_project_role(&server, &world, &bob_id, "editor").await;
    assert!(alive(&mut b).await);
    set_project_role(&server, &world, &vi_id, "editor").await;
    closed_with_nothing_before(&mut v, 4403, "your access changed").await;
    let mut v = Live::connect(&server, &doc, &vi).await;
    assert_eq!(v.welcome.role, "editor");
    assert!(!v.welcome.read_only);
    assert_eq!(edit(&mut v, 12).await, 1);
    // Dropped once: the new connection stays.
    assert!(alive(&mut v).await);
    assert!(alive(&mut b).await);
}

#[tokio::test]
async fn removal_from_the_project_closes_and_reconnecting_is_refused() {
    let (server, world) = world().await;
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    quiet(&mut b).await;
    let (status, _) = server
        .delete(
            &format!("/api/projects/{}/members/{bob_id}", world.project),
            Some(&world.admin),
        )
        .await;
    assert_eq!(status, 204);
    assert_eq!(edit(&mut a, 4).await, 1);
    closed_with_nothing_before(&mut b, 4403, "your access changed").await;
    refused_with(
        &Live::try_connect(&server, &doc, &bob, Hello::default()).await,
        4404,
    );
    assert!(alive(&mut a).await);
}

#[tokio::test]
async fn deleting_a_document_closes_it_with_the_reason_and_it_cannot_be_reopened() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let other = world.document(&server, None, Some("Other"), None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut elsewhere = Live::connect(&server, &other, &bob).await;
    quiet(&mut a).await;
    quiet(&mut b).await;
    let (status, _) = server
        .delete(&format!("/api/documents/{doc}"), Some(&world.admin))
        .await;
    assert_eq!(status, 204);
    closed_with_nothing_before(&mut a, 4404, "the document was deleted").await;
    closed_with_nothing_before(&mut b, 4404, "the document was deleted").await;
    assert!(server.state.hub.loaded(&doc).is_none());
    refused_with(
        &Live::try_connect(&server, &doc, &world.admin, Hello::default()).await,
        4404,
    );
    assert!(alive(&mut elsewhere).await);
}

#[tokio::test]
async fn deleting_a_project_closes_its_documents() {
    let (server, world) = world().await;
    let one = world.document(&server, None, Some("One"), None).await;
    let two = world.document(&server, None, Some("Two"), None).await;
    let mut a1 = Live::connect(&server, &one, &world.admin).await;
    let mut a2 = Live::connect(&server, &two, &world.admin).await;
    quiet(&mut a1).await;
    quiet(&mut a2).await;
    let (status, _) = server
        .delete(
            &format!("/api/projects/{}", world.project),
            Some(&world.admin),
        )
        .await;
    assert_eq!(status, 204);
    closed_with_nothing_before(&mut a1, 4404, "the project was deleted").await;
    closed_with_nothing_before(&mut a2, 4404, "the project was deleted").await;
    refused_with(
        &Live::try_connect(&server, &one, &world.admin, Hello::default()).await,
        4404,
    );
}

#[tokio::test]
async fn drops_are_counted_by_cause() {
    let (server, world) = world().await;
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let before = oswx_server::metrics::counters();
    set_project_role(&server, &world, &bob_id, "viewer").await;
    b.closed().await;
    let after = oswx_server::metrics::counters();
    assert!(after.drops[3] > before.drops[3], "access drops counted");
    let gauges = server.state.hub.gauges();
    assert_eq!(gauges.rooms, 1);
}

// ------------------------------------------------------------------ drops while the room is busy

#[tokio::test]
async fn a_drop_takes_effect_while_the_room_judges_a_big_change() {
    let hooks = Hooks {
        judge_delay: Duration::from_millis(3000),
        ..Hooks::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    // More than 256 operations: judged off the room's task, which takes no frame meanwhile.
    let ops = (0..300)
        .map(|i| set(&["variables", &format!("v{i}")], json!(i)))
        .collect();
    let big = b.make_change(0, ops);
    b.send_change(&big).await;
    // And one more behind it, which waits for the big one.
    let small = b.make_change(0, vec![set_depth(2)]);
    b.send_change(&small).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    set_project_role(&server, &world, &bob_id, "viewer").await;
    assert_eq!(b.closed().await.0, 4403);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    // Neither of Bob's changes is answered or applied: the room processes nothing more of his.
    assert!(
        !b.seen
            .iter()
            .any(|m| matches!(m, ServerMsg::Accepted { .. } | ServerMsg::Rejected { .. })),
        "{:?}",
        b.seen
    );
    tokio::time::sleep(Duration::from_millis(3200)).await;
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 0);
}

#[tokio::test]
async fn a_drop_takes_effect_while_the_journal_is_slow() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, _) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    flaky.commit_delay(Duration::from_millis(1500));
    let change = b.make_change(0, vec![set_depth(2)]);
    b.send_change(&change).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    // (The password change commits through the slow store too: the drop is timed from its answer.)
    change_password(&server, &bob).await;
    let answered = Instant::now();
    assert_eq!(b.closed().await.0, 4401);
    assert!(
        answered.elapsed() < Duration::from_millis(700),
        "{:?}",
        started.elapsed()
    );
    flaky.commit_delay(Duration::ZERO);
}

// ------------------------------------------------------------------ the steps after a commit

/// Sends a request in a task of its own, waits until its steps after the commit are held at the
/// gate, drops the request (and its connection), then lets the steps go on.
async fn dropped_after_commit(
    server: &TestServer,
    method: reqwest::Method,
    path: &str,
    token: &str,
    body: Option<Value>,
) {
    let gate = server.state.room_hooks.after_commit.clone();
    gate.shut();
    let mut request = reqwest::Client::new()
        .request(method, server.url(path))
        .bearer_auth(token);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let mut sent = tokio::spawn(async move { request.send().await.map(|r| r.status()) });
    tokio::select! {
        () = gate.until_waiting(1) => {}
        answer = &mut sent => panic!("answered before its steps after the commit: {answer:?}"),
        () = tokio::time::sleep(WAIT) => panic!("the request never committed"),
    }
    sent.abort();
    let _ = sent.await;
    // Long enough for the server to see the client gone and drop the handler.
    tokio::time::sleep(Duration::from_millis(300)).await;
    gate.open();
}

#[tokio::test]
async fn a_request_dropped_after_its_commit_still_closes_connections() {
    let (server, world) = world().await;
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let (cy, cy_id) = editor(&server, &world, "cy@example.com").await;
    let (dee, _) = editor(&server, &world, "dee@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let doomed = world.document(&server, None, Some("Doomed"), None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut c = Live::connect(&server, &doc, &cy).await;
    let mut d = Live::connect(&server, &doc, &dee).await;
    let mut gone = Live::connect(&server, &doomed, &world.admin).await;

    // Organisation member removal.
    let path = format!("/api/orgs/{}/members/{bob_id}", world.org);
    dropped_after_commit(&server, reqwest::Method::DELETE, &path, &world.admin, None).await;
    assert_eq!(b.closed().await.0, 4403);
    // Project member change.
    let path = format!("/api/projects/{}/members/{cy_id}", world.project);
    let body = Some(json!({"role": "viewer"}));
    dropped_after_commit(&server, reqwest::Method::PUT, &path, &world.admin, body).await;
    assert_eq!(c.closed().await.0, 4403);
    // Password change.
    let body = Some(json!({"password": "a new password", "current_password": PASSWORD}));
    dropped_after_commit(&server, reqwest::Method::PATCH, "/api/me", &dee, body).await;
    assert_eq!(d.closed().await.0, 4401);
    // Document deletion.
    let path = format!("/api/documents/{doomed}");
    dropped_after_commit(&server, reqwest::Method::DELETE, &path, &world.admin, None).await;
    assert_eq!(gone.closed().await.0, 4404);
    // A named version: the open room still hears of it.
    let mut watcher = Live::connect(&server, &doc, &world.admin).await;
    let path = format!("/api/documents/{doc}/versions");
    let body = Some(json!({"name": "Dropped"}));
    dropped_after_commit(&server, reqwest::Method::POST, &path, &world.admin, body).await;
    let heard: VersionCreated = watcher.expect().await;
    assert_eq!(heard.name, "Dropped");

    let recorded = server.state.hub.recorded();
    for call in [
        format!("drop_user {bob_id} {}", world.org),
        format!("drop_user {cy_id} {}", world.org),
        format!("close_document {doomed}"),
    ] {
        assert!(recorded.contains(&call), "{call} in {recorded:?}");
    }
    // Every request's steps after its commit run to the end although its client went away
    // (hearing `version.created` does not mean the steps after it are done: wait for them).
    tokio::time::timeout(WAIT, server.state.after_commits.idle())
        .await
        .expect("the steps after the commits finish");
}

// ------------------------------------------------------------------ sessions expiring

#[tokio::test]
async fn a_session_that_expires_while_connected_is_closed_at_its_expiry() {
    let (server, clock) = on_a_clock(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, _) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    clock.advance(3600.0);
    // Ada signs in again later: her session ends an hour after Bob's.
    let ada_later = server.login("ada@example.com", PASSWORD).await;
    let mut a = Live::connect(&server, &doc, &ada_later).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let room = server.room(&doc);
    let days = 30.0 * 86400.0;
    // At its expiry a session still works (REST takes it until `expires_at < now`), and so does
    // the connection; a second later neither does.
    clock.advance(days - 3600.0);
    room.tick().await;
    assert!(alive(&mut b).await);
    quiet(&mut b).await;
    clock.advance(1.0);
    room.tick().await;
    closed_with_nothing_before(&mut b, 4401, "sign in again").await;
    assert!(alive(&mut a).await);
    clock.advance(3600.0);
    room.tick().await;
    assert_eq!(a.closed().await.0, 4401);
}

// ------------------------------------------------------------------ connections per account

#[tokio::test]
async fn the_sixty_fifth_connection_of_an_account_is_refused_until_one_closes() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut open = Vec::new();
    for _ in 0..64 {
        open.push(Live::connect(&server, &doc, &world.admin).await);
    }
    match Live::try_connect(&server, &doc, &world.admin, Hello::default()).await {
        Ok(_) => panic!("the 65th connection was taken"),
        Err(refused) => {
            assert_eq!(refused.close, Some(4429));
            assert_eq!(
                refused.error.map(|e| e.0).as_deref(),
                Some("too_many_connections")
            );
        }
    }
    // Someone else is not limited by it.
    let (bob, _) = editor(&server, &world, "bob@example.com").await;
    Live::connect(&server, &doc, &bob).await;
    open.pop().expect("a connection").close().await;
    connections_become(&server, &world.admin_id, 63).await;
    Live::connect(&server, &doc, &world.admin).await;
}

#[tokio::test]
async fn connection_counts_survive_takeovers_drops_and_unloads() {
    let (server, clock) = on_a_clock(|s| s.max_connections_per_user = 3).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    // A takeover (same client id) replaces, it does not add.
    let mut first = Live::connect_with(&server, &doc, &bob, Hello::client("desk-computer")).await;
    let _second = Live::connect_with(&server, &doc, &bob, Hello::client("desk-computer")).await;
    assert_eq!(first.closed().await.0, 4409);
    connections_become(&server, &bob_id, 1).await;
    let _third = Live::connect(&server, &doc, &bob).await;
    let _fourth = Live::connect(&server, &doc, &bob).await;
    refused_with(
        &Live::try_connect(&server, &doc, &bob, Hello::default()).await,
        4429,
    );
    // A drop frees every slot.
    set_project_role(&server, &world, &bob_id, "viewer").await;
    connections_become(&server, &bob_id, 0).await;
    // So does a room unloading (nobody left in it), and the next load counts again.
    let mut v = Live::connect(&server, &doc, &bob).await;
    connections_become(&server, &bob_id, 1).await;
    quiet(&mut v).await;
    v.close().await;
    connections_become(&server, &bob_id, 0).await;
    clock.advance(121.0);
    assert!(sweep_unloads(&server, &doc).await);
    let _v = Live::connect(&server, &doc, &bob).await;
    connections_become(&server, &bob_id, 1).await;
    assert_eq!(server.state.hub.gauges().connections, 1);
}

// ------------------------------------------------------------------ idle unload

#[tokio::test]
async fn an_idle_room_unloads_with_a_snapshot_at_its_head_and_comes_back_the_same() {
    let (server, clock) = on_a_clock(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    for n in 1..=3 {
        assert_eq!(edit(&mut a, n).await, n as Seq);
    }
    let before = content(&server, &doc, &world.admin).await;
    a.close().await;
    // Someone left a moment ago: not idle long enough.
    clock.advance(60.0);
    assert!(!sweep_unloads(&server, &doc).await);
    clock.advance(61.0);
    assert!(sweep_unloads(&server, &doc).await);
    assert!(server.state.hub.loaded(&doc).is_none());
    let snapshot = server
        .state
        .store
        .snapshot_at_or_before(&doc, 3)
        .await
        .unwrap()
        .expect("a snapshot");
    assert_eq!(snapshot.seq, 3, "the snapshot is at the head");
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.welcome.seq, 3);
    assert_eq!(content(&server, &doc, &world.admin).await, before);
    assert_eq!(edit(&mut a, 9).await, 4);
}

#[tokio::test]
async fn a_room_with_a_detached_lock_stays_until_the_grace_passes() {
    let (server, clock) = on_a_clock(|s| s.lock_grace = 300.0).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    a.lock(&[&["features", "f1"]]).await;
    a.close().await;
    let room = server.room(&doc);
    // (Wait for the leave to reach the room.)
    let deadline = Instant::now() + WAIT;
    while room.detached_locks() != 1 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    clock.advance(200.0);
    assert!(!sweep_unloads(&server, &doc).await, "its lock waits");
    assert!(room.holds_locks());
    clock.advance(101.0);
    assert!(sweep_unloads(&server, &doc).await, "the lock lapsed");
}

#[tokio::test]
async fn a_room_with_a_change_in_flight_does_not_unload() {
    let clock = Clock::manual(T0);
    let c = clock.clone();
    let (server, flaky) = TestServer::start_flaky_full(
        |s| s.sweep_interval = 3600.0,
        new_journal,
        move |s| s.clock = c,
    )
    .await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    flaky.commit_delay(Duration::from_millis(1500));
    let change = a.make_change(0, vec![set_depth(5)]);
    a.send_change(&change).await;
    let deadline = Instant::now() + WAIT;
    while server.room_status(&doc).await.tentative == 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    a.close().await;
    clock.advance(121.0);
    assert!(!sweep_unloads(&server, &doc).await, "a change is in flight");
    flaky.commit_delay(Duration::ZERO);
    let deadline = Instant::now() + WAIT;
    while server.room_status(&doc).await.seq != 1 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(sweep_unloads(&server, &doc).await);
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 1);
}

#[tokio::test]
async fn a_room_that_cannot_write_its_snapshot_unloads_anyway_and_says_so_once() {
    let logs = LogCapture::start();
    let clock = Clock::manual(T0);
    let c = clock.clone();
    let mut kept = None;
    let server = TestServer::start_custom(
        |s| {
            s.sweep_interval = 3600.0;
            s.snapshot_every = 1;
        },
        |s| {
            let blobs = Arc::new(TestBlobs::new(s.blobs.clone()));
            kept = Some(blobs.clone());
            s.blobs = blobs;
            s.clock = c;
        },
    )
    .await;
    let blobs = kept.unwrap();
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    blobs.fail_put(true);
    assert_eq!(edit(&mut a, 2).await, 1);
    assert_eq!(edit(&mut a, 3).await, 2);
    a.close().await;
    let room = server.room(&doc);
    for _ in 0..3 {
        room.tick().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    clock.advance(121.0);
    assert!(
        sweep_unloads(&server, &doc).await,
        "it unloads all the same"
    );
    let warnings = logs
        .text()
        .lines()
        .filter(|l| l.contains("WARN") && l.contains("snapshot") && l.contains(doc.as_str()))
        .count();
    assert_eq!(warnings, 1, "{}", logs.text());
    // The journal has everything: the next load replays it.
    blobs.fail_put(false);
    let a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(a.welcome.seq, 2);
    assert_eq!(
        content(&server, &doc, &world.admin).await["content"]["features"][1]["params"]["depth"],
        3
    );
}

#[tokio::test]
async fn a_join_racing_an_unload_gets_the_old_room_or_a_new_one() {
    let clock = Clock::manual(T0);
    let c = clock.clone();
    let mut kept = None;
    let server = TestServer::start_custom(
        |s| s.sweep_interval = 3600.0,
        |s| {
            let blobs = Arc::new(TestBlobs::new(s.blobs.clone()));
            kept = Some(blobs.clone());
            s.blobs = blobs;
            s.clock = c;
        },
    )
    .await;
    let blobs = kept.unwrap();
    // The final snapshot takes a while: a join comes while the room is unloading.
    blobs.put_delay(Duration::from_millis(150));
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let (mut same, mut fresh) = (0, 0);
    for round in 0..12 {
        let mut a = Live::connect(&server, &doc, &world.admin).await;
        let seq = edit(&mut a, round).await;
        a.close().await;
        let room = server.room(&doc);
        while room
            .status()
            .await
            .is_some_and(|s| !s.connections.is_empty())
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        clock.advance(121.0);
        let joined = if round % 2 == 0 {
            server.state.hub.sweep();
            Live::connect(&server, &doc, &world.admin).await
        } else {
            let joining = Live::connect(&server, &doc, &world.admin);
            let (live, _) = tokio::join!(joining, async {
                tokio::time::sleep(Duration::from_millis(2)).await;
                server.state.hub.sweep();
            });
            live
        };
        assert_eq!(joined.welcome.seq, seq);
        let now = server.room(&doc);
        if now.id() == room.id() {
            same += 1;
        } else {
            fresh += 1;
            assert!(room.is_closed());
        }
        joined.close().await;
    }
    assert!(
        fresh > 0,
        "a join met an unloading room ({same} same, {fresh} fresh)"
    );
    assert_eq!(server.state.hub.gauges().rooms, 1);
}

#[tokio::test]
async fn a_room_opened_only_by_a_restore_unloads_when_idle() {
    let (server, clock) = on_a_clock(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(edit(&mut a, 20).await, 1);
    a.close().await;
    clock.advance(121.0);
    assert!(sweep_unloads(&server, &doc).await);
    let (status, body) = server
        .post(
            &format!("/api/documents/{doc}/restore"),
            Some(&world.admin),
            json!({"seq": 0}),
        )
        .await;
    assert_eq!((status, body), (reqwest::StatusCode::OK, json!({"seq": 2})));
    assert!(
        server.state.hub.loaded(&doc).is_some(),
        "the restore opened it"
    );
    clock.advance(60.0);
    assert!(!sweep_unloads(&server, &doc).await);
    clock.advance(61.0);
    assert!(sweep_unloads(&server, &doc).await);
}

#[tokio::test]
async fn a_room_that_does_not_answer_does_not_stop_the_others_unloading() {
    let clock = Clock::manual(T0);
    let c = clock.clone();
    let hooks = Arc::new(Hooks::default());
    let h = hooks.clone();
    // The sweeper itself, every 50 ms.
    let server = TestServer::start_custom(
        |s| s.sweep_interval = 0.05,
        move |s| {
            s.clock = c;
            s.room_hooks = h;
        },
    )
    .await;
    let world = World::new(&server).await.unwrap();
    let stuck = world.document(&server, None, Some("Stuck"), None).await;
    let fine = world.document(&server, None, Some("Fine"), None).await;
    for doc in [&stuck, &fine] {
        Live::connect(&server, doc, &world.admin)
            .await
            .close()
            .await;
    }
    let stuck_room = server.room(&stuck);
    hooks.stall.stall(&stuck);
    clock.advance(121.0);
    tokio::time::timeout(WAIT, hooks.stall.until_stopped(1))
        .await
        .expect("the stuck room stopped");
    let deadline = Instant::now() + WAIT;
    while server.state.hub.loaded(&fine).is_some() {
        assert!(Instant::now() < deadline, "the other room did not unload");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!stuck_room.is_closed());
    hooks.stall.release();
    tokio::time::timeout(WAIT, stuck_room.ended())
        .await
        .expect("the stuck room unloads once it goes on");
    assert_eq!(server.state.hub.gauges().rooms, 0);
}

/// A room whose task panics: its connections, even idle ones that send nothing, are closed with
/// "reconnect" (4410), and a reconnect loads a fresh room that works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_room_that_panics_tells_its_connections_to_reconnect() {
    let hooks = Arc::new(Hooks::default());
    let h = hooks.clone();
    let server =
        TestServer::start_custom(|s| s.sweep_interval = 3600.0, move |s| s.room_hooks = h).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(edit(&mut a, 1).await, 1);
    quiet(&mut a).await;
    let room = server.room(&doc);
    hooks.panic_on_sweep.lock().unwrap().insert(doc.clone());
    server.state.hub.sweep();
    tokio::time::timeout(WAIT, room.ended())
        .await
        .expect("the room's task ended");
    assert_eq!(a.closed().await.0, 4410);
    hooks.panic_on_sweep.lock().unwrap().clear();
    let mut b = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(edit(&mut b, 2).await, 2);
}

// ------------------------------------------------------------------ shutdown

#[tokio::test]
async fn shutdown_leaves_every_open_room_snapshotted_at_its_head() {
    let (server, world) = world().await;
    let (bob, _) = editor(&server, &world, "bob@example.com").await;
    let one = world.document(&server, None, Some("One"), None).await;
    let two = world.document(&server, None, Some("Two"), None).await;
    let mut a = Live::connect(&server, &one, &world.admin).await;
    let mut b = Live::connect(&server, &two, &bob).await;
    for n in 1..=4 {
        assert_eq!(edit(&mut a, n).await, n as Seq);
    }
    assert_eq!(edit(&mut b, 9).await, 1);
    // A request whose steps after the commit are still running when the server stops.
    let gate = server.state.room_hooks.after_commit.clone();
    gate.shut();
    let renaming = {
        let (base, token) = (server.base.clone(), world.admin.clone());
        let project = world.project.clone();
        tokio::spawn(async move {
            reqwest::Client::new()
                .patch(format!("{base}/api/projects/{project}"))
                .bearer_auth(token)
                .json(&json!({"name": "Renamed"}))
                .send()
                .await
                .map(|r| r.status())
        })
    };
    tokio::time::timeout(WAIT, gate.until_waiting(1))
        .await
        .expect("the rename committed");
    let stopping = {
        let state = server.state.clone();
        tokio::spawn(async move { state.shutdown().await })
    };
    assert_eq!(a.closed().await.0, 1001);
    assert_eq!(b.closed().await.0, 1001);
    // A handshake now is refused.
    refused_with(
        &Live::try_connect(&server, &one, &world.admin, Hello::default()).await,
        1001,
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !stopping.is_finished(),
        "shutdown waits for the rename's steps"
    );
    gate.open();
    tokio::time::timeout(WAIT, stopping)
        .await
        .expect("shutdown ends")
        .unwrap();
    assert_eq!(renaming.await.unwrap().ok(), Some(reqwest::StatusCode::OK));
    assert_eq!(server.state.after_commits.running(), 0);
    // Again: nothing happens.
    server.state.shutdown().await;
    for (doc, head) in [(&one, 4), (&two, 1)] {
        let row = server
            .state
            .store
            .snapshot_at_or_before(doc, head)
            .await
            .unwrap()
            .expect("a snapshot");
        assert_eq!(row.seq, head, "the snapshot is at the head");
    }
    let server = server.restart().await;
    assert_eq!(content(&server, &one, &world.admin).await["seq"], 4);
    assert_eq!(content(&server, &two, &world.admin).await["seq"], 1);
}

// ------------------------------------------------------------------ fix round 1: revocation

/// Records the events extensions hear.
#[derive(Default)]
struct Heard(std::sync::Mutex<Vec<String>>);

#[async_trait::async_trait]
impl oswx_server::ext::Extensions for Heard {
    async fn on_event(&self, event: &oswx_server::ext::Event) {
        self.0.lock().unwrap().push(format!("{event:?}"));
    }
}

impl Heard {
    fn any(&self, prefix: &str) -> bool {
        self.0.lock().unwrap().iter().any(|e| e.starts_with(prefix))
    }
}

/// A flaky server whose extensions record the events.
async fn flaky_hearing() -> (TestServer, Arc<FlakyStore>, Arc<Heard>) {
    let heard = Arc::new(Heard::default());
    let h = heard.clone();
    let (server, flaky) =
        TestServer::start_flaky_full(|_| {}, new_journal, move |s| s.ext = h).await;
    (server, flaky, heard)
}

/// Sends a request, waits until its transaction is parked inside `commit()`, drops the request (and
/// its connection), then lets the commit go on.
async fn dropped_in_commit(
    server: &TestServer,
    flaky: &FlakyStore,
    method: reqwest::Method,
    path: &str,
    token: &str,
    body: Option<Value>,
) {
    let gate = flaky.commit_gate.clone();
    gate.shut();
    let mut request = reqwest::Client::new()
        .request(method, server.url(path))
        .bearer_auth(token);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let mut sent = tokio::spawn(async move { request.send().await.map(|r| r.status()) });
    tokio::select! {
        () = gate.until_waiting(1) => {}
        answer = &mut sent => panic!("answered before it committed: {answer:?}"),
        () = tokio::time::sleep(WAIT) => panic!("the request never reached its commit"),
    }
    sent.abort();
    let _ = sent.await;
    // Long enough for the server to see the client gone and drop the handler.
    tokio::time::sleep(Duration::from_millis(300)).await;
    gate.open();
}

#[tokio::test]
async fn a_request_dropped_while_its_commit_is_in_flight_still_closes_connections() {
    let (server, flaky, _) = flaky_hearing().await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let (cy, cy_id) = editor(&server, &world, "cy@example.com").await;
    let (dee, _) = editor(&server, &world, "dee@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let doomed = world.document(&server, None, Some("Doomed"), None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut c = Live::connect(&server, &doc, &cy).await;
    let mut d = Live::connect(&server, &doc, &dee).await;
    let mut gone = Live::connect(&server, &doomed, &world.admin).await;
    let delete = reqwest::Method::DELETE;

    let path = format!("/api/orgs/{}/members/{bob_id}", world.org);
    dropped_in_commit(&server, &flaky, delete.clone(), &path, &world.admin, None).await;
    assert_eq!(b.closed().await.0, 4403);
    let path = format!("/api/projects/{}/members/{cy_id}", world.project);
    let body = Some(json!({"role": "viewer"}));
    dropped_in_commit(
        &server,
        &flaky,
        reqwest::Method::PUT,
        &path,
        &world.admin,
        body,
    )
    .await;
    assert_eq!(c.closed().await.0, 4403);
    let body = Some(json!({"password": "a new password", "current_password": PASSWORD}));
    dropped_in_commit(
        &server,
        &flaky,
        reqwest::Method::PATCH,
        "/api/me",
        &dee,
        body,
    )
    .await;
    assert_eq!(d.closed().await.0, 4401);
    let path = format!("/api/documents/{doomed}");
    dropped_in_commit(&server, &flaky, delete, &path, &world.admin, None).await;
    assert_eq!(gone.closed().await.0, 4404);
    // Each change was committed.
    assert!(
        server
            .state
            .store
            .org_member(&world.org, &bob_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(server.state.after_commits.running(), 0);
}

#[tokio::test]
async fn a_commit_that_lands_but_reports_an_error_still_closes_connections() {
    let (server, flaky, heard) = flaky_hearing().await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    flaky.commit_then_fail(true);
    let (status, _) = server
        .delete(
            &format!("/api/projects/{}/members/{bob_id}", world.project),
            Some(&world.admin),
        )
        .await;
    flaky.commit_then_fail(false);
    assert_eq!(status, 500, "the outcome is reported as an error");
    assert_eq!(b.closed().await.0, 4403, "the removal landed: Bob goes");
    assert!(
        !heard.any("ProjectMemberRemoved"),
        "nothing is announced for an uncertain commit"
    );
}

#[tokio::test]
async fn a_commit_that_fails_closes_nobody_and_announces_nothing() {
    let (server, flaky, heard) = flaky_hearing().await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    flaky.fail_commit(true);
    let (status, _) = server
        .delete(
            &format!("/api/projects/{}/members/{bob_id}", world.project),
            Some(&world.admin),
        )
        .await;
    flaky.fail_commit(false);
    assert_eq!(status, 500);
    // The revoking step ran (the role was read again) and found nothing changed.
    assert!(
        server
            .state
            .hub
            .recorded()
            .contains(&format!("drop_user {bob_id} {}", world.org))
    );
    assert!(alive(&mut b).await, "the role did not change: Bob stays");
    assert!(!heard.any("ProjectMemberRemoved"));
}

/// Whether `live` heard a change event.
fn heard_a_change(live: &Live) -> bool {
    live.seen.iter().any(|m| matches!(m, ServerMsg::Change(_)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoked_change_being_judged_is_never_written_even_behind_a_full_queue() {
    let hooks = Hooks {
        judge_delay: Duration::from_millis(2000),
        ..Hooks::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    quiet(&mut a).await;
    let ops = (0..300)
        .map(|i| set(&["variables", &format!("v{i}")], json!(i)))
        .collect();
    let big = b.make_change(0, ops);
    b.send_change(&big).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The room takes no frame while it judges: the queue fills and the reader waits on it.
    let ping = oswx_core::wire::encode_client(&oswx_core::wire::ClientMsg::Ping).to_vec();
    for _ in 0..400 {
        b.send_raw_lossy(ping.clone()).await;
    }
    set_project_role(&server, &world, &bob_id, "viewer").await;
    assert_eq!(b.closed().await.0, 4403);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 0);
    a.drain().await;
    assert!(!heard_a_change(&a), "{:?}", a.seen);
}

#[tokio::test]
async fn a_revoked_change_waiting_for_a_journal_lookup_is_never_written() {
    let hooks = Hooks {
        // (So that the ids are not all in memory, and the change is looked up first.)
        max_remembered: 1,
        ..Hooks::default()
    };
    let (server, flaky) =
        TestServer::start_flaky_full(|_| {}, new_journal, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut first = Live::connect_with(&server, &doc, &bob, Hello::client("bob-lookup")).await;
    assert_eq!(edit(&mut first, 1).await, 1);
    assert_eq!(edit(&mut first, 2).await, 2);
    first.close().await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect_with(&server, &doc, &bob, Hello::client("bob-lookup")).await;
    quiet(&mut a).await;
    flaky.lookup_delay(Duration::from_millis(1500));
    let lookups = flaky.change_lookups();
    let change = b.make_change(2, vec![set_depth(9)]);
    b.send_change(&change).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        flaky.change_lookups() > lookups,
        "the change is being looked up"
    );
    set_project_role(&server, &world, &bob_id, "viewer").await;
    assert_eq!(b.closed().await.0, 4403);
    tokio::time::sleep(Duration::from_millis(1800)).await;
    flaky.lookup_delay(Duration::ZERO);
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 2);
    a.drain().await;
    assert!(!heard_a_change(&a), "{:?}", a.seen);
}

#[tokio::test]
async fn a_revoked_undo_waiting_for_its_target_is_never_written() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let x = b.make_change(0, vec![set_depth(20)]);
    assert_eq!(b.resend(&x).await.seq(), 1);
    quiet(&mut a).await;
    // The undone change is durable: its inverse is read from the (slow) journal.
    flaky.lookup_delay(Duration::from_millis(1500));
    let undo = b.undo_change(1, &x.change_id, vec![set_depth(10)]);
    b.send_change(&undo).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    set_project_role(&server, &world, &bob_id, "viewer").await;
    assert_eq!(b.closed().await.0, 4403);
    tokio::time::sleep(Duration::from_millis(1800)).await;
    flaky.lookup_delay(Duration::ZERO);
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 1);
    a.drain().await;
    assert!(!heard_a_change(&a), "{:?}", a.seen);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoked_users_locks_are_released_at_once() {
    let (server, world) = world().await;
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let f1: &[&str] = &["features", "f1"];
    b.lock(&[f1]).await;
    let _: LockChanged = a.expect().await;
    set_project_role(&server, &world, &bob_id, "viewer").await;
    assert_eq!(b.closed().await.0, 4403);
    let news: LockChanged = a.expect().await;
    assert_eq!(
        news.released,
        vec![path(f1)],
        "released, not kept for the grace period: {news:?}"
    );
    assert!(!server.room(&doc).holds_locks());
    let mine = a.lock(&[f1]).await;
    assert_eq!(mine.granted, vec![path(f1)], "{mine:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_already_in_the_journal_lands_but_the_revoked_socket_hears_only_the_close() {
    // The journal holds a record 1.5 s before writing it (REST commits are not delayed).
    let slow = |store: &Arc<dyn oswx_server::store::Store>| {
        oswx_server::journal::JournalWriter::start(
            store.clone(),
            oswx_server::journal::MAX_BATCH,
            Duration::from_millis(1500),
        )
    };
    let (server, _) = TestServer::start_flaky_journal(|_| {}, slow).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    quiet(&mut a).await;
    quiet(&mut b).await;
    let change = b.make_change(0, vec![set_depth(4)]);
    b.send_change(&change).await;
    let deadline = Instant::now() + WAIT;
    while server.room_status(&doc).await.tentative == 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (status, _) = server
        .delete(
            &format!("/api/projects/{}/members/{bob_id}", world.project),
            Some(&world.admin),
        )
        .await;
    assert_eq!(status, 204);
    closed_with_nothing_before(&mut b, 4403, "your access changed").await;
    let event = loop {
        if let ServerMsg::Change(ev) = a.recv().await {
            break ev;
        }
    };
    assert_eq!(event.seq, 1, "accepted while the access lasted: it lands");
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 1);
}

// ------------------------------------------------------------------ fix round 1: smaller items

#[tokio::test]
async fn a_room_whose_snapshot_hangs_still_unloads() {
    let clock = Clock::manual(T0);
    let c = clock.clone();
    let mut kept = None;
    let hooks = Hooks {
        snapshot_wait: Duration::from_millis(300),
        ..Hooks::default()
    };
    let server = TestServer::start_custom(
        |s| {
            s.sweep_interval = 3600.0;
            s.snapshot_every = 1;
        },
        |s| {
            let blobs = Arc::new(TestBlobs::new(s.blobs.clone()));
            kept = Some(blobs.clone());
            s.blobs = blobs;
            s.clock = c;
            s.room_hooks = Arc::new(hooks);
        },
    )
    .await;
    let blobs = kept.unwrap();
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    blobs.put_delay(Duration::from_secs(3600)); // storage hangs
    assert_eq!(edit(&mut a, 2).await, 1);
    a.close().await;
    // The snapshot the change started gives up.
    tokio::time::sleep(Duration::from_millis(600)).await;
    clock.advance(121.0);
    // The room's own tick does not start another one now that the room could unload ...
    server.room(&doc).tick().await;
    // ... so the sweeper's unload wins.
    assert!(sweep_unloads(&server, &doc).await);
    blobs.put_delay(Duration::ZERO);
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 1);
}

#[tokio::test]
async fn a_takeover_at_the_connection_limit_is_let_through() {
    let server = TestServer::start_with(|s| s.max_connections_per_user = 2).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let desk = Hello::client("desk-computer");
    let mut first = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    let _other = Live::connect(&server, &doc, &world.admin).await;
    refused_with(
        &Live::try_connect(&server, &doc, &world.admin, Hello::default()).await,
        4429,
    );
    // The same client connecting again replaces its old connection: no more than before.
    let _again = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(first.closed().await.0, 4409);
    connections_become(&server, &world.admin_id, 2).await;
}

#[tokio::test]
async fn a_join_a_stuck_room_never_takes_is_told_to_reconnect() {
    let hooks = Arc::new(Hooks {
        join_wait: Duration::from_millis(300),
        ..Hooks::default()
    });
    let h = hooks.clone();
    let server =
        TestServer::start_custom(|s| s.sweep_interval = 3600.0, move |s| s.room_hooks = h).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let a = Live::connect(&server, &doc, &world.admin).await;
    hooks.stall.stall(&doc);
    server.state.hub.sweep();
    tokio::time::timeout(WAIT, hooks.stall.until_stopped(1))
        .await
        .expect("the room stopped");
    let started = Instant::now();
    refused_with(
        &Live::try_connect(&server, &doc, &world.admin, Hello::default()).await,
        4410,
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    hooks.stall.release();
    a.close().await;
    connections_become(&server, &world.admin_id, 0).await;
}

// ------------------------------------------------------------------ fix round 2

const F1: &[&str] = &["features", "f1"];

/// Waits for the next `lock.changed` at `live` and answers it.
async fn lock_news(live: &mut Live) -> LockChanged {
    live.expect().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoked_users_locks_go_at_once_whichever_message_reaches_the_room_first() {
    let hooks = Arc::new(Hooks::default());
    let h = hooks.clone();
    let server = TestServer::start_custom(|_| {}, move |s| s.room_hooks = h).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    for round in 0..200u64 {
        // Every other round the hub's `Revoked` is held back: the connection's `Leave` comes first.
        hooks
            .revoke_delay_ms
            .store(if round % 2 == 1 { 30 } else { 0 }, Ordering::SeqCst);
        let mut b = Live::connect(&server, &doc, &bob).await;
        b.lock(&[F1]).await;
        let taken = lock_news(&mut a).await;
        assert_eq!(taken.locks.len(), 1, "round {round}: {taken:?}");
        server.state.hub.drop_account(&bob_id).await;
        assert_eq!(b.closed().await.0, 4401, "round {round}");
        let news = lock_news(&mut a).await;
        assert!(
            news.locks.is_empty() && news.released == vec![path(F1)],
            "round {round}: released at once, never detached: {news:?}"
        );
        let mine = a.lock(&[F1]).await;
        assert_eq!(mine.granted, vec![path(F1)], "round {round}: {mine:?}");
        a.unlock(&[F1]).await;
    }
    let order = &hooks.revoke_order;
    let (revoked, leave) = (
        order.revoked_first.load(Ordering::SeqCst),
        order.gone_first.load(Ordering::SeqCst),
    );
    assert!(revoked > 0 && leave > 0, "both orders: {revoked} {leave}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_detached_locks_of_someone_removed_just_after_leaving_go_at_once() {
    let (server, world) = world().await;
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    b.lock(&[F1]).await;
    lock_news(&mut a).await;
    // An ordinary disconnect: the lock waits for its client (detached).
    b.close().await;
    let waiting = lock_news(&mut a).await;
    assert!(
        waiting.locks.len() == 1 && waiting.locks[0].detached,
        "{waiting:?}"
    );
    assert_eq!(server.room(&doc).detached_locks(), 1);
    // Then Bob loses his access: the lock goes now, not when the grace period ends.
    let (status, _) = server
        .delete(
            &format!("/api/projects/{}/members/{bob_id}", world.project),
            Some(&world.admin),
        )
        .await;
    assert_eq!(status, 204);
    let news = lock_news(&mut a).await;
    assert_eq!(news.released, vec![path(F1)], "{news:?}");
    let mine = a.lock(&[F1]).await;
    assert_eq!(mine.granted, vec![path(F1)], "{mine:?}");
}

/// A journal that holds every record `ms` milliseconds before writing it.
fn slow_journal(
    ms: u64,
) -> impl FnOnce(&Arc<dyn oswx_server::store::Store>) -> oswx_server::journal::JournalWriter {
    move |store| {
        oswx_server::journal::JournalWriter::start(
            store.clone(),
            oswx_server::journal::MAX_BATCH,
            Duration::from_millis(ms),
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_taken_just_as_its_wait_ran_out_leaves_no_ghost() {
    let hooks = Arc::new(Hooks {
        join_wait: Duration::from_millis(300),
        ..Hooks::default()
    });
    let h = hooks.clone();
    let server =
        TestServer::start_custom(|s| s.sweep_interval = 3600.0, move |s| s.room_hooks = h).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let _a = Live::connect(&server, &doc, &world.admin).await;
    let room = server.room(&doc);
    // The room stops; a join waits for it until its wait runs out, then holds there ...
    hooks.stall.stall(&doc);
    server.state.hub.sweep();
    tokio::time::timeout(WAIT, hooks.stall.until_stopped(1))
        .await
        .expect("the room stopped");
    hooks.after_join_timeout.shut();
    let joining = {
        let (base, token, doc) = (server.base.clone(), world.admin.clone(), doc.clone());
        tokio::spawn(async move {
            let ws_url = format!("{}/api/documents/{doc}/ws", base.replacen("http", "ws", 1));
            let (mut ws, _) = tokio_tungstenite::connect_async(ws_url).await.unwrap();
            let hello = hello_frame(&token, &Hello::default(), "ghost-client");
            futures_util::SinkExt::send(
                &mut ws,
                tokio_tungstenite::tungstenite::Message::Binary(hello.into()),
            )
            .await
            .unwrap();
            // Read until the socket closes.
            while let Some(Ok(_)) = futures_util::StreamExt::next(&mut ws).await {}
        })
    };
    tokio::time::timeout(WAIT, hooks.after_join_timeout.until_waiting(1))
        .await
        .expect("the join wait ran out");
    // ... while the room goes on and takes the join after all.
    hooks.stall.release();
    let deadline = Instant::now() + WAIT;
    while room.status().await.expect("open").connections.len() < 2 {
        assert!(Instant::now() < deadline, "the room took the join");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    hooks.after_join_timeout.open();
    tokio::time::timeout(WAIT, joining)
        .await
        .expect("the connection closed")
        .unwrap();
    let deadline = Instant::now() + WAIT;
    while room.status().await.expect("open").connections.len() != 1 {
        assert!(Instant::now() < deadline, "the ghost connection went");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ------------------------------------------------------------------ fix round 3

/// Demotes Bob to viewer while holding the steps after the commit (so the hub has not closed his
/// old socket yet), reconnects him with `hello` meanwhile (his handshake reads the new role), then
/// lets the steps go on. Answers the new connection.
async fn demote_and_reconnect(
    server: &TestServer,
    world: &World,
    bob: &str,
    bob_id: &str,
    doc: &str,
    hello: Hello,
    role: &str,
) -> Live {
    let gate = server.state.room_hooks.after_commit.clone();
    gate.shut();
    let demoting = {
        let url = server.url(&format!("/api/projects/{}/members/{bob_id}", world.project));
        let token = world.admin.clone();
        let role = role.to_owned();
        tokio::spawn(async move {
            reqwest::Client::new()
                .put(url)
                .bearer_auth(token)
                .json(&json!({"role": role}))
                .send()
                .await
                .unwrap()
                .status()
        })
    };
    tokio::time::timeout(WAIT, gate.until_waiting(1))
        .await
        .expect("the role change committed");
    let successor = Live::connect_with(server, doc, bob, hello).await;
    gate.open();
    assert_eq!(demoting.await.unwrap(), 200);
    successor
}

/// The change came back after the demotion: nothing of it was written, and the new connection
/// resending it is refused `read_only`.
async fn nothing_written_and_told(
    server: &TestServer,
    world: &World,
    doc: &str,
    successor: &mut Live,
    change: &oswx_core::Change,
    seq: u64,
) {
    assert_eq!(
        content(server, doc, &world.admin).await["seq"],
        json!(seq),
        "nothing written after the demotion"
    );
    assert_eq!(
        successor.resend(change).await.code(),
        oswx_core::wire::RejectCode::ReadOnly
    );
    assert_eq!(content(server, doc, &world.admin).await["seq"], json!(seq));
}

fn big_ops() -> Vec<oswx_core::Op> {
    (0..300)
        .map(|i| set(&["variables", &format!("v{i}")], json!(i)))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_demoted_client_reconnecting_inherits_nothing_being_judged() {
    let hooks = Hooks {
        judge_delay: Duration::from_millis(1500),
        ..Hooks::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let desk = Hello::client("demoted-judge");
    let mut a = Live::connect_with(&server, &doc, &bob, desk.clone()).await;
    let big = a.make_change(0, big_ops());
    a.send_change(&big).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut b = demote_and_reconnect(&server, &world, &bob, &bob_id, &doc, desk, "viewer").await;
    assert!(b.welcome.read_only);
    tokio::time::sleep(Duration::from_millis(1800)).await;
    nothing_written_and_told(&server, &world, &doc, &mut b, &big, 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_demoted_client_reconnecting_inherits_nothing_being_looked_up() {
    let hooks = Hooks {
        max_remembered: 1,
        ..Hooks::default()
    };
    let (server, flaky) =
        TestServer::start_flaky_full(|_| {}, new_journal, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let desk = Hello::client("demoted-lookup");
    let mut first = Live::connect_with(&server, &doc, &bob, desk.clone()).await;
    assert_eq!(edit(&mut first, 1).await, 1);
    assert_eq!(edit(&mut first, 2).await, 2);
    first.close().await;
    let mut a = Live::connect_with(&server, &doc, &bob, desk.clone()).await;
    flaky.lookup_delay(Duration::from_millis(1500));
    let change = a.make_change(2, vec![set_depth(9)]);
    a.send_change(&change).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut b = demote_and_reconnect(&server, &world, &bob, &bob_id, &doc, desk, "viewer").await;
    tokio::time::sleep(Duration::from_millis(1800)).await;
    flaky.lookup_delay(Duration::ZERO);
    nothing_written_and_told(&server, &world, &doc, &mut b, &change, 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_demoted_client_reconnecting_inherits_nothing_waiting_for_its_blobs() {
    let (server, blobs) = TestServer::start_blobs(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let body = b"a stored shape";
    let sha = {
        use sha2::Digest;
        hex::encode(sha2::Sha256::digest(body))
    };
    let r = reqwest::Client::new()
        .put(server.url(&format!("/api/projects/{}/blobs/{sha}", world.project)))
        .bearer_auth(&world.admin)
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let desk = Hello::client("demoted-blobs");
    let mut a = Live::connect_with(&server, &doc, &bob, desk.clone()).await;
    blobs.exists_delay(Duration::from_millis(1500));
    let change = a.make_change(
        0,
        vec![set(
            &["variables", "brep"],
            json!({"$blob": sha, "len": body.len()}),
        )],
    );
    a.send_change(&change).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut b = demote_and_reconnect(&server, &world, &bob, &bob_id, &doc, desk, "viewer").await;
    tokio::time::sleep(Duration::from_millis(1800)).await;
    blobs.exists_delay(Duration::ZERO);
    nothing_written_and_told(&server, &world, &doc, &mut b, &change, 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_demoted_client_reconnecting_inherits_nothing_judged_again_after_a_refusal() {
    let (server, flaky) = TestServer::start_flaky_journal(|_| {}, slow_journal(800)).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let desk = Hello::client("demoted-rejudge");
    let mut a = Live::connect_with(&server, &doc, &bob, desk.clone()).await;
    // The journal refuses the first; the second, already handed over, is judged again after it.
    let first = a.make_change(0, vec![set(&["variables", "x"], json!(1))]);
    flaky.refuse_change_once(&first.change_id);
    a.send_change(&first).await;
    let second = a.make_change(0, vec![set_depth(7)]);
    a.send_change(&second).await;
    let deadline = Instant::now() + WAIT;
    while server.room_status(&doc).await.tentative < 2 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut b = demote_and_reconnect(&server, &world, &bob, &bob_id, &doc, desk, "viewer").await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    nothing_written_and_told(&server, &world, &doc, &mut b, &second, 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_promoted_client_reconnecting_inherits_nothing_and_is_answered_when_it_resends() {
    let hooks = Hooks {
        judge_delay: Duration::from_millis(1500),
        ..Hooks::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let desk = Hello::client("promoted-desk");
    let mut a = Live::connect_with(&server, &doc, &bob, desk.clone()).await;
    let big = a.make_change(0, big_ops());
    a.send_change(&big).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Editor to owner: another role, so the old connection's work is not inherited.
    let mut b = demote_and_reconnect(&server, &world, &bob, &bob_id, &doc, desk, "owner").await;
    assert_eq!(b.welcome.role, "owner");
    tokio::time::sleep(Duration::from_millis(1800)).await;
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 0);
    assert!(
        !b.seen
            .iter()
            .any(|m| matches!(m, ServerMsg::Accepted { .. })),
        "{:?}",
        b.seen
    );
    // Sent again, it is judged under the new role and answered (once).
    assert_eq!(b.resend(&big).await.seq(), 1);
    assert!(no_answer_within(&mut b, &big.change_id, Duration::from_millis(500)).await);
}

/// Whether no answer for `change_id` comes within `wait`.
async fn no_answer_within(live: &mut Live, change_id: &str, wait: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return true;
        }
        match live.try_recv(left).await {
            Some(
                ServerMsg::Accepted { change_id: id, .. }
                | ServerMsg::Rejected { change_id: id, .. },
            ) if &*id == change_id => {
                return false;
            }
            Some(_) => continue,
            None => return true,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_user_whose_role_did_not_change_keeps_their_detached_locks_and_connection() {
    let (server, world) = world().await;
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let other = world.document(&server, None, Some("Other"), None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut elsewhere = Live::connect(&server, &other, &bob).await;
    b.lock(&[F1]).await;
    lock_news(&mut a).await;
    b.close().await;
    lock_news(&mut a).await;
    assert_eq!(server.room(&doc).detached_locks(), 1);
    // His role is set to what it is already.
    set_project_role(&server, &world, &bob_id, "editor").await;
    assert!(alive(&mut elsewhere).await, "his connection stays");
    a.drain().await;
    assert_eq!(
        server.room(&doc).detached_locks(),
        1,
        "his lock still waits"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_that_cannot_be_read_closes_connections_but_keeps_detached_locks() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let other = world.document(&server, None, Some("Other"), None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    let mut elsewhere = Live::connect(&server, &other, &bob).await;
    b.lock(&[F1]).await;
    lock_news(&mut a).await;
    b.close().await;
    lock_news(&mut a).await;
    flaky.fail_project_reads(true);
    server
        .state
        .hub
        .drop_user(server.state.store.as_ref(), &bob_id, &world.org)
        .await;
    flaky.fail_project_reads(false);
    // Failing closed for connections ...
    assert_eq!(elsewhere.closed().await.0, 4403);
    // ... but a lock is not taken from someone whose role may not have changed.
    a.drain().await;
    assert_eq!(server.room(&doc).detached_locks(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_change_releases_locks_at_once_whichever_message_reaches_the_room_first() {
    let hooks = Arc::new(Hooks::default());
    let h = hooks.clone();
    let server = TestServer::start_custom(|_| {}, move |s| s.room_hooks = h).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    for round in 0..200u64 {
        hooks
            .revoke_delay_ms
            .store(if round % 2 == 1 { 30 } else { 0 }, Ordering::SeqCst);
        let mut b = Live::connect(&server, &doc, &bob).await;
        b.lock(&[F1]).await;
        lock_news(&mut a).await;
        set_project_role(&server, &world, &bob_id, "viewer").await;
        assert_eq!(b.closed().await.0, 4403, "round {round}");
        let news = lock_news(&mut a).await;
        assert!(
            news.locks.is_empty() && news.released == vec![path(F1)],
            "round {round}: released at once, never detached: {news:?}"
        );
        let mine = a.lock(&[F1]).await;
        assert_eq!(mine.granted, vec![path(F1)], "round {round}: {mine:?}");
        a.unlock(&[F1]).await;
        set_project_role(&server, &world, &bob_id, "editor").await;
    }
    let order = &hooks.revoke_order;
    let (revoked, leave) = (
        order.revoked_first.load(Ordering::SeqCst),
        order.gone_first.load(Ordering::SeqCst),
    );
    assert!(revoked > 0 && leave > 0, "both orders: {revoked} {leave}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_closed_as_too_slow_keeps_its_locks_for_the_grace_period() {
    let hooks = Hooks {
        // (So that the ids are not all in memory, and each change is looked up first.)
        max_remembered: 1,
        ..Hooks::default()
    };
    let (server, flaky) =
        TestServer::start_flaky_full(|_| {}, new_journal, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let desk = Hello::client("slow-desk");
    let mut first = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    assert_eq!(edit(&mut first, 1).await, 1);
    assert_eq!(edit(&mut first, 2).await, 2);
    first.close().await;
    let mut watcher = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect_with(&server, &doc, &world.admin, desk).await;
    b.lock(&[F1]).await;
    lock_news(&mut watcher).await;
    flaky.lookup_delay(Duration::from_secs(3));
    let wait = b.make_change(2, vec![set_depth(3)]);
    b.send_change(&wait).await;
    for _ in 0..1100 {
        b.send(&oswx_core::wire::ClientMsg::Ping).await;
    }
    assert_eq!(b.closed().await.0, 4408);
    let news = lock_news(&mut watcher).await;
    assert!(
        news.locks.len() == 1 && news.locks[0].detached && news.released.is_empty(),
        "detached, not released: {news:?}"
    );
    flaky.lookup_delay(Duration::ZERO);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_connection_that_passes_nothing_on_keeps_its_clients_locks() {
    let (server, world) = world().await;
    let (bob, bob_id) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut watcher = Live::connect(&server, &doc, &world.admin).await;
    let desk = Hello::client("replaced-desk");
    let mut a = Live::connect_with(&server, &doc, &bob, desk.clone()).await;
    a.lock(&[F1]).await;
    lock_news(&mut watcher).await;
    // Promoted, he reconnects: no inheritance (another role), but the lock is his client's still.
    let _b = demote_and_reconnect(&server, &world, &bob, &bob_id, &doc, desk, "owner").await;
    // (Replaced, 4409; or, if the hub got there before the close frame went, 4403.)
    let code = a.closed().await.0;
    assert!(code == 4409 || code == 4403, "{code}");
    watcher.drain().await;
    let mine = watcher.lock(&[F1]).await;
    assert!(
        mine.granted.is_empty(),
        "the lock was not released: {mine:?}"
    );
}

// ------------------------------------------------------------------ fix round 4: takeovers start clean

/// How many journal rows of `doc` carry the change id `id`.
async fn rows_with(server: &TestServer, doc: &str, id: &str) -> usize {
    server
        .state
        .store
        .changes_between(doc, 0, i64::MAX, 100_000)
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.change_id.as_deref() == Some(id))
        .count()
}

/// How many `change` events for `seq` `live` has been sent so far.
async fn events_for(live: &mut Live, seq: Seq) -> usize {
    live.drain().await;
    live.seen
        .iter()
        .filter(|m| matches!(m, ServerMsg::Change(ev) if ev.seq == seq))
        .count()
}

/// Waits until the connection `client` of `doc` waits for an answer (its resend reached the room).
async fn until_waiting_for_an_answer(server: &TestServer, doc: &str, client: &str) {
    let deadline = Instant::now() + WAIT;
    loop {
        let status = server.room_status(doc).await;
        if status
            .connections
            .iter()
            .any(|c| c.client_id == client && c.waiting_answers > 0)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the resend did not reach the room"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// `live`, which sent `change` (or was waiting for it) when it became durable as `seq`, was
/// answered `accepted`: nothing more comes for it, the journal holds it once, the observer saw its
/// event once, and `live` (a waiter gets `accepted`, not the event) saw none.
async fn settled_once(
    server: &TestServer,
    doc: &str,
    live: &mut Live,
    observer: &mut Live,
    change: &oswx_core::Change,
    seq: Seq,
) {
    assert!(
        no_answer_within(live, &change.change_id, Duration::from_millis(500)).await,
        "one answer"
    );
    assert_eq!(
        rows_with(server, doc, &change.change_id).await,
        1,
        "written once"
    );
    assert_eq!(events_for(observer, seq).await, 1, "others hear of it once");
    assert_eq!(
        events_for(live, seq).await,
        0,
        "a waiter is answered, not sent the event"
    );
}

/// The successor resends `change` and is the one it is settled for ([`settled_once`]). Answers
/// its seq.
async fn resent_once(
    server: &TestServer,
    doc: &str,
    successor: &mut Live,
    observer: &mut Live,
    change: &oswx_core::Change,
) -> Seq {
    let seq = successor.resend(change).await.seq();
    settled_once(server, doc, successor, observer, change, seq).await;
    seq
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resend_after_a_chain_of_takeovers_is_answered_once() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut observer = Live::connect(&server, &doc, &world.admin).await;
    let desk = Hello::client("chain-desk");
    // P1 sends C, which is held at the journal's commit, and goes away.
    flaky.commit_gate.shut();
    let mut p1 = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    let c = p1.make_change(0, vec![set_depth(4)]);
    p1.send_change(&c).await;
    tokio::time::timeout(WAIT, flaky.commit_gate.until_waiting(1))
        .await
        .expect("C is being written");
    p1.close().await;
    // P2 connects and is replaced by P3 before it resends anything; P3 resends, then C lands.
    let mut p2 = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    let mut p3 = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(p2.closed().await.0, 4409);
    p3.send_change(&c).await;
    until_waiting_for_an_answer(&server, &doc, &p3.client_id).await;
    flaky.commit_gate.open();
    assert_eq!(p3.outcome(&c.change_id).await.seq(), 1);
    settled_once(&server, &doc, &mut p3, &mut observer, &c, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chain_of_takeovers_each_resending_gets_one_answer_per_live_send() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut observer = Live::connect(&server, &doc, &world.admin).await;
    let desk = Hello::client("chain-each");
    flaky.commit_gate.shut();
    let mut a = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    let c = a.make_change(0, vec![set_depth(5)]);
    a.send_change(&c).await;
    tokio::time::timeout(WAIT, flaky.commit_gate.until_waiting(1))
        .await
        .expect("C is being written");
    let mut b = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    assert_eq!(a.closed().await.0, 4409);
    b.send_change(&c).await;
    until_waiting_for_an_answer(&server, &doc, &b.client_id).await;
    let mut last = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(b.closed().await.0, 4409);
    last.send_change(&c).await;
    until_waiting_for_an_answer(&server, &doc, &last.client_id).await;
    flaky.commit_gate.open();
    assert_eq!(last.outcome(&c.change_id).await.seq(), 1);
    settled_once(&server, &doc, &mut last, &mut observer, &c, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_while_the_change_waits_behind_another_is_answered_once() {
    let hooks = Hooks {
        judge_delay: Duration::from_millis(1000),
        ..Hooks::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut observer = Live::connect(&server, &doc, &world.admin).await;
    let desk = Hello::client("parked-desk");
    let mut a = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    let big = a.make_change(0, big_ops());
    a.send_change(&big).await;
    // Parked behind the big one (judged off the task).
    let small = a.make_change(0, vec![set_depth(6)]);
    a.send_change(&small).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut b = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(a.closed().await.0, 4409);
    assert_eq!(
        resent_once(&server, &doc, &mut b, &mut observer, &big).await,
        1
    );
    assert_eq!(
        resent_once(&server, &doc, &mut b, &mut observer, &small).await,
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_while_the_change_is_judged_off_the_task_is_answered_once() {
    let hooks = Hooks {
        judge_delay: Duration::from_millis(1000),
        ..Hooks::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut observer = Live::connect(&server, &doc, &world.admin).await;
    let desk = Hello::client("judged-desk");
    let mut a = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    let big = a.make_change(0, big_ops());
    a.send_change(&big).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The successor resends at once, while the old copy is still being judged: the old copy is
    // dropped when it comes back, the resend is judged afresh.
    let mut b = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(a.closed().await.0, 4409);
    assert_eq!(
        resent_once(&server, &doc, &mut b, &mut observer, &big).await,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_while_the_change_is_looked_up_is_answered_once() {
    let hooks = Hooks {
        max_remembered: 1,
        ..Hooks::default()
    };
    let (server, flaky) =
        TestServer::start_flaky_full(|_| {}, new_journal, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let desk = Hello::client("lookup-desk");
    let mut first = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    assert_eq!(edit(&mut first, 1).await, 1);
    assert_eq!(edit(&mut first, 2).await, 2);
    first.close().await;
    let mut observer = Live::connect(&server, &doc, &world.admin).await;
    let mut a = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    flaky.lookup_delay(Duration::from_millis(1000));
    let change = a.make_change(2, vec![set_depth(9)]);
    a.send_change(&change).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut b = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(a.closed().await.0, 4409);
    assert_eq!(
        resent_once(&server, &doc, &mut b, &mut observer, &change).await,
        3
    );
    flaky.lookup_delay(Duration::ZERO);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_while_the_changes_blobs_are_checked_is_answered_once() {
    let (server, blobs) = TestServer::start_blobs(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let body = b"another stored shape";
    let sha = {
        use sha2::Digest;
        hex::encode(sha2::Sha256::digest(body))
    };
    let r = reqwest::Client::new()
        .put(server.url(&format!("/api/projects/{}/blobs/{sha}", world.project)))
        .bearer_auth(&world.admin)
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let mut observer = Live::connect(&server, &doc, &world.admin).await;
    let desk = Hello::client("blob-desk");
    let mut a = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    blobs.exists_delay(Duration::from_millis(1000));
    let change = a.make_change(
        0,
        vec![set(
            &["variables", "brep"],
            json!({"$blob": sha, "len": body.len()}),
        )],
    );
    a.send_change(&change).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut b = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(a.closed().await.0, 4409);
    assert_eq!(
        resent_once(&server, &doc, &mut b, &mut observer, &change).await,
        1
    );
    blobs.exists_delay(Duration::ZERO);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_while_the_change_waits_to_be_judged_again_is_answered_once_per_send() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut observer = Live::connect(&server, &doc, &world.admin).await;
    let desk = Hello::client("rejudge-desk");
    let mut a = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    // The journal is held: the first (which it will refuse) and the second wait in it.
    flaky.commit_gate.shut();
    let first = a.make_change(0, vec![set(&["variables", "x"], json!(1))]);
    flaky.refuse_change_once(&first.change_id);
    a.send_change(&first).await;
    let second = a.make_change(0, vec![set_depth(7)]);
    a.send_change(&second).await;
    let deadline = Instant::now() + WAIT;
    while server.room_status(&doc).await.tentative < 2 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut b = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(a.closed().await.0, 4409);
    // The second is resent while it waits ...
    b.send_change(&second).await;
    until_waiting_for_an_answer(&server, &doc, &b.client_id).await;
    assert_eq!(server.room_status(&doc).await.tentative, 2, "still held");
    flaky.commit_gate.open();
    // ... the refusal of the first makes the room judge it again; its sender is gone, so the
    // waiting resend is told it was not stored (once) ...
    assert_eq!(
        b.outcome(&second.change_id).await.code(),
        oswx_core::wire::RejectCode::ServerError
    );
    assert!(no_answer_within(&mut b, &second.change_id, Duration::from_millis(300)).await);
    // ... and sent again, it is judged afresh and written once; so is the first.
    let s2 = resent_once(&server, &doc, &mut b, &mut observer, &second).await;
    let s1 = resent_once(&server, &doc, &mut b, &mut observer, &first).await;
    assert_eq!((s2, s1), (1, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_while_the_changes_seq_is_checked_is_answered_once() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut observer = Live::connect(&server, &doc, &world.admin).await;
    let desk = Hello::client("checked-desk");
    let mut a = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    let x = a.make_change(0, vec![set_depth(20)]);
    assert_eq!(a.resend(&x).await.seq(), 1);
    // The room forgets X; sent again it reaches the journal, which says duplicate, and its seq
    // is looked up (slowly).
    server.room(&doc).forget_change_ids().await;
    flaky.lookup_delay_once(Duration::from_millis(1000));
    let mut again = x.clone();
    again.base = 1;
    a.send_change(&again).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut b = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(a.closed().await.0, 4409);
    assert_eq!(b.resend(&again).await, Outcome::Accepted(1));
    assert!(no_answer_within(&mut b, &x.change_id, Duration::from_millis(500)).await);
    assert_eq!(rows_with(&server, &doc, &x.change_id).await, 1);
    assert_eq!(events_for(&mut observer, 1).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_that_had_not_resent_gets_its_clients_change_as_an_event_in_order() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, _) = editor(&server, &world, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut other = Live::connect(&server, &doc, &bob).await;
    let desk = Hello::client("order-desk");
    let mut p = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    // C is held at the journal's commit (pending as seq 1).
    flaky.commit_gate.shut();
    let c = p.make_change(0, vec![set_depth(8)]);
    p.send_change(&c).await;
    tokio::time::timeout(WAIT, flaky.commit_gate.until_waiting(1))
        .await
        .expect("C is being written");
    // S replaces P; its welcome ends at seq 0. Another client's change follows C.
    let mut s = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(p.closed().await.0, 4409);
    assert_eq!(s.welcome.seq, 0);
    let e = other.make_change(0, vec![set(&["variables", "e"], json!(2))]);
    other.send_change(&e).await;
    let deadline = Instant::now() + WAIT;
    while server.room_status(&doc).await.tentative < 2 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    flaky.commit_gate.open();
    assert_eq!(other.outcome(&e.change_id).await.seq(), 2);
    // S sees C (as seq 1, its client's change) before seq 2, and no answer.
    let mut seen = Vec::new();
    while seen.len() < 2 {
        match s.recv().await {
            ServerMsg::Change(ev) => {
                seen.push((ev.seq, ev.client_id.clone(), ev.change_id.clone()))
            }
            ServerMsg::Accepted { .. } | ServerMsg::Rejected { .. } => {
                panic!("no answer before a resend")
            }
            _ => {}
        }
    }
    assert_eq!(seen[0].0, 1);
    assert_eq!(&*seen[0].1, s.client_id.as_str());
    assert_eq!(seen[0].2, c.change_id);
    assert_eq!(seen[1].0, 2);
    // Its resend is answered from memory with C's seq, once.
    assert_eq!(s.resend(&c).await, Outcome::Accepted(1));
    assert!(no_answer_within(&mut s, &c.change_id, Duration::from_millis(500)).await);
    assert_eq!(rows_with(&server, &doc, &c.change_id).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_a_takeover_revoking_the_new_one_writes_nothing_and_the_old_one_changes_nothing() {
    let hooks = Hooks {
        judge_delay: Duration::from_millis(1000),
        ..Hooks::default()
    };
    let server = TestServer::start_custom(|_| {}, |s| s.room_hooks = Arc::new(hooks)).await;
    let world = World::new(&server).await.unwrap();
    let (desk_token, bob_id) = editor(&server, &world, "bob@example.com").await;
    let laptop_token = server.login("bob@example.com", PASSWORD).await;
    let doc = world.document(&server, None, None, None).await;
    let desk = Hello::client("revoke-after");
    // The old socket's session ends after the takeover: the new connection is not touched.
    let mut a = Live::connect_with(&server, &doc, &desk_token, desk.clone()).await;
    let mut b = Live::connect_with(&server, &doc, &laptop_token, desk.clone()).await;
    assert_eq!(a.closed().await.0, 4409);
    let (status, _) = server
        .post("/api/auth/logout", Some(&desk_token), json!({}))
        .await;
    assert_eq!(status, 204);
    assert!(alive(&mut b).await, "the new connection is unaffected");
    assert_eq!(edit(&mut b, 3).await, 1);
    // A change in the works, a takeover, then the new connection loses its access: nothing more.
    let big = b.make_change(1, big_ops());
    b.send_change(&big).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut c = Live::connect_with(&server, &doc, &laptop_token, desk).await;
    assert_eq!(b.closed().await.0, 4409);
    c.send_change(&big).await;
    server.state.hub.drop_account(&bob_id).await;
    assert_eq!(c.closed().await.0, 4401);
    tokio::time::sleep(Duration::from_millis(2300)).await;
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 1);
}

// ------------------------------------------------------------------ fix round 5: history lookups

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_while_an_undo_target_is_read_is_answered_once() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut observer = Live::connect(&server, &doc, &world.admin).await;
    let desk = Hello::client("undo-desk");
    let mut a = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    let x = a.make_change(0, vec![set_depth(20)]);
    assert_eq!(a.resend(&x).await.seq(), 1);
    // The undone change is durable: its inverse is read from the journal (held).
    flaky.history_gate.shut();
    let undo = a.undo_change(1, &x.change_id, vec![set_depth(10)]);
    a.send_change(&undo).await;
    tokio::time::timeout(WAIT, flaky.history_gate.until_waiting(1))
        .await
        .expect("the undo target is being read");
    let mut b = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(a.closed().await.0, 4409);
    b.send_change(&undo).await;
    tokio::time::timeout(WAIT, flaky.history_gate.until_waiting(2))
        .await
        .expect("the resend reads it too");
    flaky.history_gate.open();
    assert_eq!(b.outcome(&undo.change_id).await.seq(), 2);
    settled_once(&server, &doc, &mut b, &mut observer, &undo, 2).await;
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_while_a_revert_reads_its_change_is_answered_once() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut observer = Live::connect(&server, &doc, &world.admin).await;
    let desk = Hello::client("revert-desk");
    let mut a = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    assert_eq!(edit(&mut a, 20).await, 1);
    flaky.history_gate.shut();
    a.send(&oswx_core::wire::ClientMsg::Revert {
        request_id: "rv1".into(),
        seq: 1,
    })
    .await;
    tokio::time::timeout(WAIT, flaky.history_gate.until_waiting(1))
        .await
        .expect("the change is being read");
    let mut b = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(a.closed().await.0, 4409);
    b.send(&oswx_core::wire::ClientMsg::Revert {
        request_id: "rv1".into(),
        seq: 1,
    })
    .await;
    tokio::time::timeout(WAIT, flaky.history_gate.until_waiting(2))
        .await
        .expect("the repeat reads it too");
    flaky.history_gate.open();
    let result: RevertResult = b.expect().await;
    assert_eq!(result.seq(), 2);
    // One result, one revert written, heard of once.
    let more = b.drain().await;
    assert!(
        !more
            .iter()
            .any(|m| matches!(m, ServerMsg::RevertResult { .. })),
        "{more:?}"
    );
    assert_eq!(content(&server, &doc, &world.admin).await["seq"], 2);
    assert_eq!(events_for(&mut observer, 2).await, 1);
    assert_eq!(events_for(&mut observer, 3).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_while_a_rebuild_report_reads_its_change_is_heard_once() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let (op, fillet) = a.add_feature("Fillet1");
    assert_eq!(a.change(vec![op], vec![], vec![]).await.seq(), 1);
    assert_eq!(edit(&mut a, 3).await, 2);
    a.close().await;
    // A fresh room knows nothing of these changes: blaming one reads it from the journal.
    server
        .room(&doc)
        .close(oswx_server::room::close::GOING_AWAY, "test", true)
        .await;
    let mut observer = Live::connect(&server, &doc, &world.admin).await;
    let desk = Hello::client("report-desk");
    let mut p = Live::connect_with(&server, &doc, &world.admin, desk.clone()).await;
    p.report(1, &[]).await; // the baseline
    p.drain().await;
    observer.drain().await;
    observer.seen.clear();
    flaky.history_gate.shut();
    p.report(2, &[(&fillet, "fillet radius too large")]).await;
    tokio::time::timeout(WAIT, flaky.history_gate.until_waiting(1))
        .await
        .expect("the blamed change is being read");
    let mut s = Live::connect_with(&server, &doc, &world.admin, desk).await;
    assert_eq!(p.closed().await.0, 4409);
    s.report(2, &[(&fillet, "fillet radius too large")]).await;
    tokio::time::timeout(WAIT, flaky.history_gate.until_waiting(2))
        .await
        .expect("the repeat reads it too");
    flaky.history_gate.open();
    let st: RebuildStatus = observer.expect().await;
    assert_eq!(st.seq, 2);
    assert_eq!(st.broken[&*fillet].by_seq, Some(2));
    let more = observer.drain().await;
    assert!(
        !more
            .iter()
            .any(|m| matches!(m, ServerMsg::RebuildStatus { .. })),
        "heard once: {more:?}"
    );
}
