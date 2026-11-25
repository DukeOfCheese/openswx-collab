//! Who is in each document: `online` and `online_count` in `GET /api/projects/{id}/documents`.
//! (A port of `tests/test_online.py`.)

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::*;
use serde_json::{Value, json};

async fn listing(server: &TestServer, project: &str, token: &str) -> HashMap<String, Value> {
    let (status, body) = server
        .get(&format!("/api/projects/{project}/documents"), Some(token))
        .await;
    assert_eq!(status, 200, "{body}");
    body.as_array()
        .unwrap()
        .iter()
        .map(|d| (d["id"].as_str().unwrap().to_owned(), d.clone()))
        .collect()
}

async fn wait_for_connections(server: &TestServer, doc: &str, n: usize) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while server.room_status(doc).await.connections.len() != n {
        assert!(
            tokio::time::Instant::now() < deadline,
            "never {n} connections"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_member_sees_who_is_in_an_open_document() {
    let (server, world) = world().await;
    let open_doc = world.document(&server, None, Some("Bracket"), None).await;
    let closed_doc = world.document(&server, None, Some("Cover"), None).await;
    let (bob, _) = world
        .invite(
            &server,
            "bob@example.com",
            Some("Bob"),
            Some("editor"),
            "member",
        )
        .await;
    let a = Live::connect(&server, &open_doc, &world.admin).await;
    // A second app of Ada's: she is still one person.
    let _a2 = Live::connect(&server, &open_doc, &world.admin).await;
    let b = Live::connect(&server, &open_doc, &bob).await;
    let docs = listing(&server, &world.project, &bob).await;
    assert_eq!(
        docs[&open_doc]["online"],
        json!([
            {"name": "Ada", "colour": a.welcome.you.colour},
            {"name": "Bob", "colour": b.welcome.you.colour},
        ])
    );
    assert_eq!(docs[&open_doc]["online_count"], 2);
    assert_eq!(docs[&closed_doc]["online"], json!([]));
    assert_eq!(docs[&closed_doc]["online_count"], 0);
}

#[tokio::test]
async fn someone_who_left_is_not_listed() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let (bob, _) = world
        .invite(
            &server,
            "bob@example.com",
            Some("Bob"),
            Some("editor"),
            "member",
        )
        .await;
    let _a = Live::connect(&server, &doc, &world.admin).await;
    let b = Live::connect(&server, &doc, &bob).await;
    b.close().await;
    wait_for_connections(&server, &doc, 1).await;
    let d = &listing(&server, &world.project, &world.admin).await[&doc];
    assert_eq!(
        d["online"],
        json!([{"name": "Ada", "colour": d["online"][0]["colour"]}])
    );
    assert_eq!(d["online_count"], 1);
}

#[tokio::test]
async fn a_closed_or_unloaded_room_lists_no_one() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let live = Live::connect(&server, &doc, &world.admin).await;
    let room = server.room(&doc);
    live.close().await;
    wait_for_connections(&server, &doc, 0).await;
    let d = &listing(&server, &world.project, &world.admin).await[&doc];
    assert_eq!(
        (&d["online"], &d["online_count"]),
        (&json!([]), &json!(0)),
        "open, but empty"
    );
    room.close(1001, "unloaded", true).await;
    assert!(server.state.hub.loaded(&doc).is_none());
    let d = &listing(&server, &world.project, &world.admin).await[&doc];
    assert_eq!((&d["online"], &d["online_count"]), (&json!([]), &json!(0)));
}

#[tokio::test]
async fn the_list_is_capped_and_the_count_is_not() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut people = Vec::new();
    for i in 0..10 {
        let (token, _) = world
            .invite(
                &server,
                &format!("p{i}@example.com"),
                Some(&format!("P{i}")),
                Some("editor"),
                "member",
            )
            .await;
        people.push(Live::connect(&server, &doc, &token).await);
    }
    let d = &listing(&server, &world.project, &world.admin).await[&doc];
    let names: Vec<&str> = d["online"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, (0..8).map(|i| format!("P{i}")).collect::<Vec<_>>());
    assert_eq!(d["online_count"], 10);
    // The first to join leaves: the ninth comes into the list.
    people.remove(0).close().await;
    wait_for_connections(&server, &doc, 9).await;
    let d = &listing(&server, &world.project, &world.admin).await[&doc];
    assert_eq!(d["online"][0]["name"], "P1");
    assert_eq!(d["online"][7]["name"], "P8");
    assert_eq!(d["online_count"], 9);
}

#[tokio::test]
async fn someone_outside_the_project_gets_404_not_the_list() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let _a = Live::connect(&server, &doc, &world.admin).await;
    let (eve, _) = world
        .invite(&server, "eve@example.com", Some("Eve"), None, "member")
        .await;
    let (status, body) = server
        .get(
            &format!("/api/projects/{}/documents", world.project),
            Some(&eve),
        )
        .await;
    assert_eq!(status, 404);
    assert!(!body.to_string().contains("Ada"), "{body}");
}

#[tokio::test]
async fn the_list_never_opens_a_room() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let d = &listing(&server, &world.project, &world.admin).await[&doc];
    assert_eq!(d["online_count"], 0);
    assert!(server.state.hub.loaded(&doc).is_none());
}
