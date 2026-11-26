//! What takes people out of documents and documents out of memory: the immediate drops when access
//! changes (inventory §5 "Hub", "Immediate drops"), a session expiring while connected, the
//! per-account connection limit, the sweeper's idle unload, and shutdown. Also the steps after a
//! REST commit, which run to the end even when the request is dropped.

mod common;

use std::sync::Arc;
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
    assert_eq!(server.state.after_commits.running(), 0);
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
    clock.advance(days - 3600.0 - 1.0); // a second before Bob's expires
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
