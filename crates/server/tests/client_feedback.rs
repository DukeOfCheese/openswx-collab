//! What the first real client (openswx) needed from the protocol. (A port of
//! `tests/test_client_feedback.py`; v2 has no `ids.reserve`, so its reservation tests become the
//! same checks on what replaced them: answers that say which request they answer, and a welcome
//! that says where the client is.)

mod common;

use common::*;
use serde_json::json;

async fn restore(
    server: &TestServer,
    doc: &str,
    token: &str,
    seq: i64,
) -> (u16, serde_json::Value) {
    let (status, body) = server
        .post(
            &format!("/api/documents/{doc}/restore"),
            Some(token),
            json!({"seq": seq}),
        )
        .await;
    (status.as_u16(), body)
}

#[tokio::test]
async fn a_refused_revert_says_which_request_it_answers() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let got = a.revert(7).await;
    assert_eq!((got.request_id.as_str(), got.code()), ("rv7", "not_found"));
    let (vi, _) = world
        .invite(&server, "vi@example.com", None, Some("viewer"), "member")
        .await;
    let mut v = Live::connect(&server, &doc, &vi).await;
    let got = v.revert(1).await;
    assert_eq!(
        (got.request_id.as_str(), got.code(), got.message.as_deref()),
        (
            "rv1",
            "read_only",
            Some("you have view access to this project")
        )
    );
}

#[tokio::test]
async fn the_welcome_says_which_project_and_what_does_not_rebuild() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let hello = || Hello::client("ada-desktop-run-1");
    let mut a = Live::connect_with(&server, &doc, &world.admin, hello()).await;
    assert_eq!(a.welcome.project_id, world.project);
    assert!(a.welcome.broken.is_empty());
    a.report(0, &[("f2", "no sketch")]).await;
    a.drain().await;
    a.close().await;
    // A short drop: it comes back to the same state, and so does anyone else.
    let again = Live::connect_with(&server, &doc, &world.admin, hello().last_seq(0)).await;
    assert_eq!(again.welcome.broken["f2"].error, "no sketch");
    assert_eq!(again.welcome.changes.as_deref(), Some(&[][..]));
    let other = Live::connect_with(
        &server,
        &doc,
        &world.admin,
        Hello::client("ada-laptop-run-9"),
    )
    .await;
    assert_eq!(other.welcome.broken.len(), 1);
}

#[tokio::test]
async fn your_own_locks_do_not_stop_your_restore() {
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
    assert_eq!(a.change(vec![set_depth(30)], vec![], vec![]).await.seq(), 1);
    assert_eq!(a.lock(&[&["features", "f2"]]).await.granted.len(), 1);
    assert_eq!(
        restore(&server, &doc, &world.admin, 0).await,
        (200, json!({"seq": 2}))
    );
    // Another connection of the same person does not stop it either.
    let mut laptop =
        Live::connect_with(&server, &doc, &world.admin, Hello::client("ada-laptop1")).await;
    assert_eq!(laptop.lock(&[&["features", "f1"]]).await.granted.len(), 1);
    assert_eq!(
        restore(&server, &doc, &world.admin, 1).await,
        (200, json!({"seq": 3}))
    );
    laptop.unlock(&[&["features", "f1"]]).await;
    laptop.drain().await;
    // Someone else's does, and says which.
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(b.lock(&[&["features", "f1"]]).await.granted.len(), 1);
    let (status, body) = restore(&server, &doc, &world.admin, 0).await;
    assert_eq!(status, 409);
    assert_eq!(body["detail"]["code"], "locked");
    let paths: Vec<&serde_json::Value> = body["detail"]["conflicts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| &c["path"])
        .collect();
    assert_eq!(paths, [&json!(["features", "f1"])]);
    assert_eq!(body["detail"]["conflicts"][0]["by"], json!(b.client_id));
}
