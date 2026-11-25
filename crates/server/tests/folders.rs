//! Folders inside projects (new in v2; no Python equivalent), with the hostile cases.

mod common;

use common::{TestServer, Token, World, world};
use oswx_server::store::Id;
use reqwest::StatusCode;
use serde_json::{Value, json};

async fn new_folder(
    server: &TestServer,
    w: &World,
    token: &str,
    name: &str,
    parent: Option<&str>,
) -> (StatusCode, Value) {
    server
        .post(
            &format!("/api/projects/{}/folders", w.project),
            Some(token),
            json!({"name": name, "parent_id": parent}),
        )
        .await
}

async fn folder(server: &TestServer, w: &World, name: &str, parent: Option<&str>) -> Id {
    let (status, body) = new_folder(server, w, &w.admin, name, parent).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body["id"].as_str().unwrap().to_owned()
}

async fn move_to(
    server: &TestServer,
    w: &World,
    folder: &str,
    parent: Option<&str>,
) -> (StatusCode, Value) {
    server
        .patch(
            &format!("/api/folders/{folder}"),
            Some(&w.admin),
            json!({ "parent_id": parent }),
        )
        .await
}

/// Eve: an admin of another organisation, with no role in the world's.
async fn outsider(server: &TestServer, w: &World) -> Token {
    let (_, org) = server
        .post("/api/orgs", Some(&w.admin), json!({"name": "Elsewhere"}))
        .await;
    let (_, inv) = server
        .post(
            &format!("/api/orgs/{}/invites", org["id"].as_str().unwrap()),
            Some(&w.admin),
            json!({"email": "eve@example.com", "role": "admin"}),
        )
        .await;
    server
        .register_invited("eve@example.com", None, inv["token"].as_str().unwrap())
        .await
        .0
}

#[tokio::test]
async fn folders_are_created_listed_renamed_moved_and_deleted() {
    let (server, w) = world().await;
    let (status, top) = new_folder(&server, &w, &w.admin, "  Top  ", None).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(top["name"], "Top");
    assert_eq!(top["parent_id"], Value::Null);
    assert_eq!(top["project_id"], w.project.as_str());
    let top = top["id"].as_str().unwrap().to_owned();
    let mid = folder(&server, &w, "Mid", Some(&top)).await;
    let low = folder(&server, &w, "Low", Some(&mid)).await;
    // Sibling names need not be unique.
    folder(&server, &w, "Mid", Some(&top)).await;

    let (status, list) = server
        .get(
            &format!("/api/projects/{}/folders", w.project),
            Some(&w.admin),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["Low", "Mid", "Mid", "Top"]);

    // Rename and move together.
    let (status, got) = server
        .patch(
            &format!("/api/folders/{low}"),
            Some(&w.admin),
            json!({"name": "Bottom", "parent_id": null}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(
        (&got["name"], &got["parent_id"]),
        (&json!("Bottom"), &Value::Null)
    );
    let row = server
        .state
        .store
        .folder_by_id(&low)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((row.name.as_str(), row.parent_id), ("Bottom", None));
    assert_eq!(
        move_to(&server, &w, &low, Some(&mid)).await.0,
        StatusCode::OK
    );

    // Deleting Mid moves its document and its folder up to Top; nothing else goes.
    let doc = w.document(&server, None, None, None).await;
    let (status, _) = server
        .patch(
            &format!("/api/documents/{doc}"),
            Some(&w.admin),
            json!({"folder_id": mid}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        server
            .delete(&format!("/api/folders/{mid}"), Some(&w.admin))
            .await,
        (StatusCode::NO_CONTENT, Value::Null)
    );
    let store = &server.state.store;
    assert!(store.folder_by_id(&mid).await.unwrap().is_none());
    assert_eq!(
        store.folder_by_id(&low).await.unwrap().unwrap().parent_id,
        Some(top.clone())
    );
    let row = store.document_by_id(&doc).await.unwrap().unwrap();
    assert_eq!(row.folder_id, Some(top.clone()));
    assert!(row.deleted_at.is_none());
    // A folder at the top: what it held goes to the top.
    assert_eq!(
        server
            .delete(&format!("/api/folders/{top}"), Some(&w.admin))
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        store.folder_by_id(&low).await.unwrap().unwrap().parent_id,
        None
    );
    assert_eq!(
        store.document_by_id(&doc).await.unwrap().unwrap().folder_id,
        None
    );
    assert_eq!(
        server
            .delete(&format!("/api/folders/{top}"), Some(&w.admin))
            .await,
        (StatusCode::NOT_FOUND, json!({"detail": "no such folder"}))
    );
}

#[tokio::test]
async fn a_folder_cannot_go_inside_itself() {
    let (server, w) = world().await;
    let a = folder(&server, &w, "A", None).await;
    let b = folder(&server, &w, "B", Some(&a)).await;
    let c = folder(&server, &w, "C", Some(&b)).await;
    let cycle = (
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({"detail": "a folder cannot be moved into itself or a folder inside it"}),
    );
    assert_eq!(move_to(&server, &w, &a, Some(&a)).await, cycle);
    assert_eq!(move_to(&server, &w, &a, Some(&b)).await, cycle);
    assert_eq!(move_to(&server, &w, &a, Some(&c)).await, cycle);
    assert_eq!(
        server
            .state
            .store
            .folder_by_id(&a)
            .await
            .unwrap()
            .unwrap()
            .parent_id,
        None
    );
    // Sideways and up is fine.
    assert_eq!(move_to(&server, &w, &c, Some(&a)).await.0, StatusCode::OK);
    assert_eq!(move_to(&server, &w, &b, None).await.0, StatusCode::OK);
}

#[tokio::test]
async fn folders_nest_at_most_32_deep() {
    let (server, w) = world().await;
    let mut chain = vec![folder(&server, &w, "1", None).await];
    for i in 2..=32 {
        let parent = chain.last().unwrap().clone();
        chain.push(folder(&server, &w, &i.to_string(), Some(&parent)).await);
    }
    let too_deep = (
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({"detail": "folders nest at most 32 deep"}),
    );
    assert_eq!(
        new_folder(&server, &w, &w.admin, "33", Some(chain.last().unwrap())).await,
        too_deep
    );
    // A two-level subtree under folder 31 would reach 33.
    let other = folder(&server, &w, "x", None).await;
    folder(&server, &w, "y", Some(&other)).await;
    assert_eq!(
        move_to(&server, &w, &other, Some(&chain[30])).await,
        too_deep
    );
    assert_eq!(
        move_to(&server, &w, &other, Some(&chain[29])).await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn folder_roles_and_hostile_requests() {
    let (server, w) = world().await;
    let (val, _) = w
        .invite(&server, "val@example.com", None, Some("viewer"), "member")
        .await;
    let (bob, _) = w.editor(&server, "bob@example.com").await;
    let eve = outsider(&server, &w).await;
    let mine = folder(&server, &w, "Mine", None).await;

    // Viewers list, editors write.
    assert_eq!(
        server
            .get(&format!("/api/projects/{}/folders", w.project), Some(&val))
            .await
            .0,
        StatusCode::OK
    );
    let editor_needed = (
        StatusCode::FORBIDDEN,
        json!({"detail": "needs the project role editor"}),
    );
    assert_eq!(
        new_folder(&server, &w, &val, "V", None).await,
        editor_needed
    );
    assert_eq!(
        server
            .patch(
                &format!("/api/folders/{mine}"),
                Some(&val),
                json!({"name": "V"})
            )
            .await,
        editor_needed
    );
    assert_eq!(
        server
            .delete(&format!("/api/folders/{mine}"), Some(&val))
            .await,
        editor_needed
    );
    assert_eq!(
        new_folder(&server, &w, &bob, "B", None).await.0,
        StatusCode::CREATED
    );

    // Someone without a role sees no folder and no project.
    let gone = (StatusCode::NOT_FOUND, json!({"detail": "no such folder"}));
    for f in [mine.clone(), "0".repeat(32)] {
        assert_eq!(
            server
                .patch(
                    &format!("/api/folders/{f}"),
                    Some(&eve),
                    json!({"name": "E"})
                )
                .await,
            gone
        );
        assert_eq!(
            server
                .delete(&format!("/api/folders/{f}"), Some(&eve))
                .await,
            gone
        );
    }
    let no_project = (StatusCode::NOT_FOUND, json!({"detail": "no such project"}));
    assert_eq!(
        server
            .get(&format!("/api/projects/{}/folders", w.project), Some(&eve))
            .await,
        no_project
    );
    assert_eq!(new_folder(&server, &w, &eve, "E", None).await, no_project);

    // Names, and parents in another project.
    for (name, detail) in [
        ("   ", "name must not be empty"),
        (
            &"n".repeat(201) as &str,
            "name must be at most 200 characters",
        ),
    ] {
        assert_eq!(
            new_folder(&server, &w, &w.admin, name, None).await,
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({ "detail": detail })
            )
        );
    }
    let (_, other) = server
        .post(
            &format!("/api/orgs/{}/projects", w.org),
            Some(&w.admin),
            json!({"name": "Other"}),
        )
        .await;
    let (_, theirs) = server
        .post(
            &format!("/api/projects/{}/folders", other["id"].as_str().unwrap()),
            Some(&w.admin),
            json!({"name": "Theirs"}),
        )
        .await;
    let theirs = theirs["id"].as_str().unwrap().to_owned();
    let elsewhere = (
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({"detail": "there is no such folder in this project"}),
    );
    assert_eq!(
        new_folder(&server, &w, &w.admin, "X", Some(&theirs)).await,
        elsewhere
    );
    assert_eq!(move_to(&server, &w, &mine, Some(&theirs)).await, elsewhere);
    assert_eq!(
        server
            .patch(&format!("/api/folders/{mine}"), Some(&w.admin), json!({}))
            .await,
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"detail": "give name or parent_id"})
        )
    );
}
