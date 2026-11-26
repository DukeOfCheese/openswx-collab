//! Documents over REST: create, list, get, rename and move, delete, and the content at the head
//! (the document parts of the Python `test_accounts.py`, `test_live.py` and `test_hardening.py`
//! that need no WebSocket), with the hostile cases.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use common::{TestServer, Token, World, on_postgres, part, world};
use oswx_server::blobs::{Blobs, ByteStream, PutOutcome};
use oswx_server::ext::{Extensions, Quota};
use oswx_server::snapshot::{decode, snapshot_key};
use oswx_server::store::Id;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// The PART fixture as the server keeps it: without the per-person keys.
fn stored_part() -> Value {
    let mut p = part();
    let m = p.as_object_mut().unwrap();
    for key in [
        "name",
        "rollback",
        "active_config",
        "display_state",
        "journal",
    ] {
        m.remove(key);
    }
    p
}

fn docs_url(w: &World) -> String {
    format!("/api/projects/{}/documents", w.project)
}

async fn create(server: &TestServer, w: &World, token: &str, body: Value) -> (StatusCode, Value) {
    server.post(&docs_url(w), Some(token), body).await
}

async fn content(server: &TestServer, doc: &str, token: &str) -> (StatusCode, Value) {
    server
        .get(&format!("/api/documents/{doc}/content"), Some(token))
        .await
}

fn storage(server: &TestServer) -> PathBuf {
    PathBuf::from(&server.state.settings.storage_url)
}

/// Every file under `dir` (none if it does not exist).
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(files_under(&path));
        } else {
            out.push(path);
        }
    }
    out
}

async fn viewer(server: &TestServer, w: &World, email: &str) -> (Token, Id) {
    w.invite(server, email, None, Some("viewer"), "member")
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
async fn documents_are_created_listed_read_renamed_and_deleted_by_role() {
    let (server, w) = world().await;
    let (bob, bob_id) = w.editor(&server, "bob@example.com").await;
    let (val, _) = viewer(&server, &w, "val@example.com").await;

    // An editor creates.
    let (status, doc) = create(
        &server,
        &w,
        &bob,
        json!({"name": "  Bracket  ", "content": part()}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{doc}");
    let id = doc["id"].as_str().unwrap().to_owned();
    assert_eq!(id.len(), 32);
    assert_eq!(doc["name"], "Bracket");
    assert_eq!(doc["kind"], "part");
    assert_eq!(doc["format_version"], 3);
    assert_eq!(doc["seq"], 0);
    assert_eq!(doc["project_id"], w.project.as_str());
    assert_eq!(doc["folder_id"], Value::Null);
    assert_eq!(doc["created_by"], bob_id.as_str());
    assert_eq!(doc["role"], "editor");
    assert!(doc["created_at"].as_f64().unwrap() > 0.0);
    assert_eq!(doc["created_at"], doc["updated_at"]);

    // Everyone with a role lists and reads it; the list says who has it open.
    for token in [&w.admin, &bob, &val] {
        let (status, list) = server.get(&docs_url(&w), Some(token)).await;
        assert_eq!(status, StatusCode::OK);
        let list = list.as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["id"], id.as_str());
        assert_eq!(list[0]["online"], json!([]));
        assert_eq!(list[0]["online_count"], 0);
        let (status, got) = server
            .get(&format!("/api/documents/{id}"), Some(token))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(got["name"], "Bracket");
        let (status, got) = content(&server, &id, token).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(got["content"]["features"][0]["id"], "f1");
    }
    let (_, got) = server
        .get(&format!("/api/documents/{id}"), Some(&val))
        .await;
    assert_eq!(got["role"], "viewer");
    let (_, got) = server
        .get(&format!("/api/documents/{id}"), Some(&w.admin))
        .await;
    assert_eq!(got["role"], "owner");

    // A viewer cannot create, rename or delete.
    let editor_needed = json!({"detail": "needs the project role editor"});
    assert_eq!(
        create(&server, &w, &val, json!({"name": "X", "content": part()})).await,
        (StatusCode::FORBIDDEN, editor_needed.clone())
    );
    assert_eq!(
        server
            .patch(
                &format!("/api/documents/{id}"),
                Some(&val),
                json!({"name": "Y"})
            )
            .await,
        (StatusCode::FORBIDDEN, editor_needed)
    );
    assert_eq!(
        server
            .delete(&format!("/api/documents/{id}"), Some(&val))
            .await,
        (
            StatusCode::FORBIDDEN,
            json!({"detail": "needs the project role owner"})
        )
    );

    // An editor renames; only a project owner deletes (as in the Python server).
    let (status, got) = server
        .patch(
            &format!("/api/documents/{id}"),
            Some(&bob),
            json!({"name": " Plate "}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(got["name"], "Plate");
    assert_eq!(got["role"], "editor");
    let row = server
        .state
        .store
        .document_by_id(&id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.name, "Plate");
    assert_eq!(
        server
            .delete(&format!("/api/documents/{id}"), Some(&bob))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        server
            .delete(&format!("/api/documents/{id}"), Some(&w.admin))
            .await,
        (StatusCode::NO_CONTENT, Value::Null)
    );
    assert!(
        server
            .state
            .hub
            .recorded()
            .contains(&format!("close_document {id}"))
    );

    // Deleted: gone from the list and 404 everywhere, but the row stays, marked.
    let gone = (StatusCode::NOT_FOUND, json!({"detail": "no such document"}));
    assert_eq!(server.get(&docs_url(&w), Some(&w.admin)).await.1, json!([]));
    assert_eq!(
        server
            .get(&format!("/api/documents/{id}"), Some(&w.admin))
            .await,
        gone
    );
    assert_eq!(content(&server, &id, &w.admin).await, gone);
    assert_eq!(
        server
            .patch(
                &format!("/api/documents/{id}"),
                Some(&w.admin),
                json!({"name": "Z"})
            )
            .await,
        gone
    );
    assert_eq!(
        server
            .delete(&format!("/api/documents/{id}"), Some(&w.admin))
            .await,
        gone
    );
    let row = server
        .state
        .store
        .document_by_id(&id)
        .await
        .unwrap()
        .unwrap();
    assert!(row.deleted_at.is_some());
}

#[tokio::test]
async fn the_python_part_comes_back_as_it_was_created() {
    let (server, w) = world().await;
    let id = w.document(&server, None, None, None).await;
    let (status, got) = content(&server, &id, &w.admin).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(got["content"], stored_part());
    assert_eq!(got["seq"], 0);
    assert_eq!(got["document"]["id"], id.as_str());
    assert_eq!(got["document"]["seq"], 0);
    assert!(got["document"].get("role").is_none());
    // next_id and counters are kept as given; the per-person keys are not.
    assert_eq!(got["content"]["next_id"], 3);
    assert!(got["content"].get("rollback").is_none());

    // The snapshot is seq 0 of epoch 0, in storage and in the database.
    let row = server
        .state
        .store
        .latest_snapshot(&id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((row.epoch, row.seq), (0, 0));
    assert_eq!(row.key, snapshot_key(&id, 0, 0));
    let bytes = server.state.blobs.get(&row.key).await.unwrap().unwrap();
    assert_eq!(row.size, bytes.len() as i64);
    let (doc, _, seq) = decode(&bytes).unwrap();
    assert_eq!(seq, 0);
    assert_eq!(
        oswx_server::content::json_from_plain(&doc.to_plain()),
        stored_part()
    );

    // And again after a restart.
    let server = server.restart().await;
    assert_eq!(
        content(&server, &id, &w.admin).await.1["content"],
        stored_part()
    );
}

#[tokio::test]
async fn a_document_you_cannot_open_answers_exactly_as_a_missing_one() {
    let (server, w) = world().await;
    let id = w.document(&server, None, None, None).await;
    let eve = outsider(&server, &w).await;
    // Carol is in the organisation but not the project.
    let (carol, _) = w
        .invite(&server, "carol@example.com", None, None, "member")
        .await;
    let missing = "0".repeat(32);
    for token in [&eve, &carol] {
        for doc in [id.as_str(), missing.as_str()] {
            let gone = (StatusCode::NOT_FOUND, json!({"detail": "no such document"}));
            assert_eq!(
                server
                    .get(&format!("/api/documents/{doc}"), Some(token))
                    .await,
                gone
            );
            assert_eq!(content(&server, doc, token).await, gone);
            assert_eq!(
                server
                    .patch(
                        &format!("/api/documents/{doc}"),
                        Some(token),
                        json!({"name": "X"})
                    )
                    .await,
                gone
            );
            assert_eq!(
                server
                    .delete(&format!("/api/documents/{doc}"), Some(token))
                    .await,
                gone
            );
        }
        let no_project = (StatusCode::NOT_FOUND, json!({"detail": "no such project"}));
        assert_eq!(server.get(&docs_url(&w), Some(token)).await, no_project);
        assert_eq!(
            create(&server, &w, token, json!({"name": "X", "content": part()})).await,
            no_project
        );
    }
    assert_eq!(
        server.get(&format!("/api/documents/{id}"), None).await,
        (StatusCode::UNAUTHORIZED, json!({"detail": "sign in first"}))
    );
}

/// `{"a": {"a": ... 1 ...}}` with `levels` maps.
fn nested(levels: usize) -> Value {
    (0..levels).fold(json!(1), |inner, _| json!({ "a": inner }))
}

#[tokio::test]
async fn content_the_core_refuses_is_422_and_nothing_is_written() {
    let (server, w) = world().await;
    let refused = |content: Value| json!({"name": "X", "content": content});
    let cases = [
        (
            refused(nested(65)),
            "content is nested more than 64 levels deep",
        ),
        (
            refused(json!({"features": [], "x": {"$list": []}})),
            "content holds something a document cannot (a $list key, or an integer out of range)",
        ),
        (
            refused(json!({"n": u64::MAX})),
            "content holds something a document cannot (a $list key, or an integer out of range)",
        ),
        (
            refused(json!({"k".repeat(201): 1})),
            "content has a key or id longer than 200 characters",
        ),
        (refused(json!([1, 2])), "content must be a JSON object"),
        (refused(json!("text")), "content must be a JSON object"),
        (refused(json!(7)), "content must be a JSON object"),
        (refused(Value::Null), "content must be a JSON object"),
        (
            json!({"name": "X", "content": part(), "kind": "drawing"}),
            "kind must be part or assembly",
        ),
        (
            json!({"name": "  ", "content": part()}),
            "name must not be empty",
        ),
        (
            json!({"name": "n".repeat(201), "content": part()}),
            "name must be at most 200 characters",
        ),
    ];
    for (body, detail) in cases {
        assert_eq!(
            create(&server, &w, &w.admin, body).await,
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({ "detail": detail })
            )
        );
    }
    // Missing or mistyped fields are the extractor's 422.
    for body in [
        json!({"name": "X"}),
        json!({"content": part()}),
        json!({"name": 5, "content": {}}),
    ] {
        assert_eq!(
            create(&server, &w, &w.admin, body).await.0,
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }
    // NaN is not JSON: 400.
    let r = reqwest::Client::new()
        .post(server.url(&docs_url(&w)))
        .bearer_auth(&w.admin)
        .header("content-type", "application/json")
        .body(r#"{"name": "X", "content": {"x": NaN}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert!(
        server
            .state
            .store
            .documents_in_project(&w.project)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(files_under(&storage(&server).join("docs")).is_empty());

    // 64 levels is fine.
    w.document(&server, Some(nested(64)), None, None).await;
}

#[tokio::test]
async fn format_version_is_the_integer_version_only() {
    let (server, w) = world().await;
    for (version, want) in [
        (json!(3), json!(3)),
        (json!(true), Value::Null),
        (json!(3.0), Value::Null),
        (json!("3"), Value::Null),
    ] {
        let (status, doc) = create(
            &server,
            &w,
            &w.admin,
            json!({"name": "X", "content": {"version": version}}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(doc["format_version"], want, "version {version}");
    }
    let (_, doc) = create(&server, &w, &w.admin, json!({"name": "X", "content": {}})).await;
    assert_eq!(doc["format_version"], Value::Null);
    let (_, doc) = create(
        &server,
        &w,
        &w.admin,
        json!({"name": "A", "content": {}, "kind": "assembly"}),
    )
    .await;
    assert_eq!(doc["kind"], "assembly");
}

fn sha256(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

#[tokio::test]
async fn blob_references_must_name_uploaded_blobs() {
    let (server, w) = world().await;
    let data = b"an imported body".to_vec();
    let sha = sha256(&data);
    let r = reqwest::Client::new()
        .put(server.url(&format!("/api/projects/{}/blobs/{sha}", w.project)))
        .bearer_auth(&w.admin)
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);

    let with_body = |body: Value| {
        let mut p = part();
        p["features"][1]["params"]["body"] = body;
        json!({"name": "X", "content": p})
    };
    let (status, doc) = create(
        &server,
        &w,
        &w.admin,
        with_body(json!({"$blob": sha, "len": data.len()})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{doc}");

    let absent = sha256(b"never uploaded");
    let malformed = "not-a-hash";
    let mut p = part();
    p["features"][1]["params"]["body"] = json!({"$blob": absent});
    p["extra"] = json!([{"$blob": malformed, "len": 3}, {"$blob": sha}]);
    let (status, body) = create(&server, &w, &w.admin, json!({"name": "Y", "content": p})).await;
    let mut want = vec![absent.clone(), malformed.to_owned()];
    want.sort();
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body,
        json!({"detail": {"message": "upload these blobs first", "blobs": want}})
    );

    // A blob of another organisation does not count.
    let (_, other) = server
        .post("/api/orgs", Some(&w.admin), json!({"name": "Other"}))
        .await;
    let (_, project) = server
        .post(
            &format!("/api/orgs/{}/projects", other["id"].as_str().unwrap()),
            Some(&w.admin),
            json!({"name": "P"}),
        )
        .await;
    let (status, body) = server
        .post(
            &format!(
                "/api/projects/{}/documents",
                project["id"].as_str().unwrap()
            ),
            Some(&w.admin),
            with_body(json!({"$blob": sha})),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["detail"]["blobs"], json!([sha]));
    assert_eq!(
        server
            .state
            .store
            .documents_in_project(&w.project)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// A creation body of about `mib` MiB.
fn body_of(mib: f64) -> String {
    let target = (mib * f64::from(1 << 20)) as usize;
    let shell = r#"{"name":"Big","content":{"features":[],"big":""}}"#;
    let filler = "x".repeat(target - shell.len());
    format!(r#"{{"name":"Big","content":{{"features":[],"big":"{filler}"}}}}"#)
}

#[tokio::test]
async fn creation_takes_up_to_eight_mib() {
    let (server, w) = world().await;
    let http = reqwest::Client::new();
    let send = |body: String| {
        http.post(server.url(&docs_url(&w)))
            .bearer_auth(&w.admin)
            .header("content-type", "application/json")
            .body(body)
            .send()
    };
    let small = body_of(7.9);
    let r = send(small.clone()).await.unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);
    let id = r.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (status, got) = content(&server, &id, &w.admin).await;
    assert_eq!(status, StatusCode::OK);
    let sent: Value = serde_json::from_str(&small).unwrap();
    assert_eq!(got["content"], sent["content"]);

    let r = send(body_of(8.1)).await.unwrap();
    assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        r.json::<Value>().await.unwrap(),
        json!({"detail": "too large"})
    );
    // Other routes keep the 1 MiB default.
    let (status, _) = server
        .patch(
            &format!("/api/documents/{id}"),
            Some(&w.admin),
            json!({"name": "x".repeat(2 << 20)}),
        )
        .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

struct NoDocuments;

#[async_trait]
impl Extensions for NoDocuments {
    async fn quota(&self, _org: &Id, what: Quota) -> Result<(), String> {
        match what {
            Quota::Documents => Err("this plan has no documents".into()),
            _ => Ok(()),
        }
    }
}

#[tokio::test]
async fn the_quota_hook_can_refuse_a_document_before_anything_is_written() {
    let server = TestServer::start_custom(|_| {}, |s| s.ext = Arc::new(NoDocuments)).await;
    let w = World::new(&server).await.unwrap();
    assert_eq!(
        create(
            &server,
            &w,
            &w.admin,
            json!({"name": "X", "content": part()})
        )
        .await,
        (
            StatusCode::PAYMENT_REQUIRED,
            json!({"detail": "this plan has no documents"})
        )
    );
    assert!(
        server
            .state
            .store
            .documents_in_project(&w.project)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(files_under(&storage(&server).join("docs")).is_empty());
}

#[tokio::test]
async fn a_failed_commit_leaves_no_snapshot_behind() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let w = World::new(&server).await.unwrap();
    flaky.fail_commit(true);
    assert_eq!(
        create(
            &server,
            &w,
            &w.admin,
            json!({"name": "X", "content": part()})
        )
        .await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"detail": "server error"})
        )
    );
    flaky.fail_commit(false);
    assert!(
        server
            .state
            .store
            .documents_in_project(&w.project)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(files_under(&storage(&server).join("docs")).is_empty());
}

#[tokio::test]
async fn earlier_states_are_asked_for_by_seq_or_version() {
    // (The states themselves are tested in history.rs.)
    let (server, w) = world().await;
    let id = w.document(&server, None, None, None).await;
    for (query, expected) in [
        ("?seq=0", StatusCode::OK),
        ("?seq=1", StatusCode::NOT_FOUND),
        ("?seq=-1", StatusCode::NOT_FOUND),
        ("?version=first", StatusCode::NOT_FOUND),
        ("?seq=x", StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        let (status, _) = server
            .get(
                &format!("/api/documents/{id}/content{query}"),
                Some(&w.admin),
            )
            .await;
        assert_eq!(status, expected, "{query}");
    }
}

#[tokio::test]
async fn documents_live_in_folders_of_their_project() {
    let (server, w) = world().await;
    let (_, folder) = server
        .post(
            &format!("/api/projects/{}/folders", w.project),
            Some(&w.admin),
            json!({"name": "Brackets"}),
        )
        .await;
    let folder = folder["id"].as_str().unwrap().to_owned();
    let (status, a) = create(
        &server,
        &w,
        &w.admin,
        json!({"name": "A", "content": part(), "folder_id": folder}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(a["folder_id"], folder.as_str());
    let a = a["id"].as_str().unwrap().to_owned();
    let b = w.document(&server, None, Some("B"), None).await;

    let names = |list: Value| -> Vec<String> {
        list.as_array()
            .unwrap()
            .iter()
            .map(|d| d["name"].as_str().unwrap().to_owned())
            .collect()
    };
    let list = |query: String| {
        let server = &server;
        let w = &w;
        async move {
            server
                .get(&format!("{}{query}", docs_url(w)), Some(&w.admin))
                .await
                .1
        }
    };
    assert_eq!(names(list(String::new()).await), ["A", "B"]);
    assert_eq!(names(list(format!("?folder={folder}")).await), ["A"]);
    assert_eq!(names(list("?folder=root".into()).await), ["B"]);
    assert_eq!(
        names(list(format!("?folder={}", "0".repeat(32))).await),
        Vec::<String>::new()
    );

    // Move B into the folder, A out of it.
    let (status, got) = server
        .patch(
            &format!("/api/documents/{b}"),
            Some(&w.admin),
            json!({"folder_id": folder}),
        )
        .await;
    assert_eq!(
        (status, &got["folder_id"]),
        (StatusCode::OK, &json!(folder))
    );
    let (status, got) = server
        .patch(
            &format!("/api/documents/{a}"),
            Some(&w.admin),
            json!({"folder_id": null, "name": "A2"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        (&got["folder_id"], &got["name"]),
        (&Value::Null, &json!("A2"))
    );
    let row = server
        .state
        .store
        .document_by_id(&a)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((row.folder_id, row.name.as_str()), (None, "A2"));
    assert_eq!(names(list(format!("?folder={folder}")).await), ["B"]);

    // A folder of another project, or none at all, is refused without saying which.
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
    let refused = (
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({"detail": "there is no such folder in this project"}),
    );
    for bad in [theirs.clone(), "0".repeat(32), "garbage".into()] {
        assert_eq!(
            server
                .patch(
                    &format!("/api/documents/{b}"),
                    Some(&w.admin),
                    json!({"folder_id": bad})
                )
                .await,
            refused
        );
        assert_eq!(
            create(
                &server,
                &w,
                &w.admin,
                json!({"name": "C", "content": part(), "folder_id": bad}),
            )
            .await,
            refused
        );
    }
    assert_eq!(
        server
            .patch(&format!("/api/documents/{b}"), Some(&w.admin), json!({}))
            .await,
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"detail": "give name or folder_id"})
        )
    );
}

/// Keeps the type of every event.
#[derive(Default)]
struct Recorder(std::sync::Mutex<Vec<Value>>);

#[async_trait]
impl Extensions for Recorder {
    async fn on_event(&self, event: &oswx_server::ext::Event) {
        self.0
            .lock()
            .unwrap()
            .push(serde_json::to_value(event).unwrap());
    }
}

#[tokio::test]
async fn document_and_folder_changes_are_events_with_ids_only() {
    let recorder = Arc::new(Recorder::default());
    let ext = recorder.clone();
    let server = TestServer::start_custom(|_| {}, move |s| s.ext = ext).await;
    let w = World::new(&server).await.unwrap();
    let (_, f) = server
        .post(
            &format!("/api/projects/{}/folders", w.project),
            Some(&w.admin),
            json!({"name": "F"}),
        )
        .await;
    let f = f["id"].as_str().unwrap().to_owned();
    let doc = w.document(&server, None, None, None).await;
    let url = format!("/api/documents/{doc}");
    server
        .patch(&url, Some(&w.admin), json!({"name": "N", "folder_id": f}))
        .await;
    server.delete(&url, Some(&w.admin)).await;
    server
        .patch(
            &format!("/api/folders/{f}"),
            Some(&w.admin),
            json!({"name": "G", "parent_id": null}),
        )
        .await;
    server
        .delete(&format!("/api/folders/{f}"), Some(&w.admin))
        .await;
    let events = recorder.0.lock().unwrap().clone();
    let types: Vec<&str> = events
        .iter()
        .map(|e| e["type"].as_str().unwrap())
        .filter(|t| t.starts_with("document_") || t.starts_with("folder_"))
        .collect();
    assert_eq!(
        types,
        [
            "folder_created",
            "document_created",
            "document_renamed",
            "document_moved",
            "document_deleted",
            "folder_renamed",
            "folder_moved",
            "folder_deleted",
        ]
    );
    let moved = events
        .iter()
        .find(|e| e["type"] == "document_moved")
        .unwrap();
    assert_eq!(
        moved,
        &json!({"type": "document_moved", "org": w.org, "project": w.project,
                "document": doc, "folder": f, "by": w.admin_id})
    );
}

#[tokio::test]
async fn lists_of_items_must_have_distinct_ids_so_they_can_be_edited() {
    let (server, w) = world().await;
    let feats_msg = "content.features must be a list of objects with distinct ids";
    let part_with = |features: Value| {
        let mut p = part();
        p["features"] = features;
        json!({"name": "X", "content": p})
    };
    let cases = [
        (
            part_with(json!([{"id": "f1", "name": "A"}, {"id": "f1", "name": "B"}])),
            feats_msg,
        ),
        (
            part_with(json!([{"id": "f1"}, {"name": "no id"}])),
            feats_msg,
        ),
        (part_with(json!([{"id": "f1"}, 3])), feats_msg),
        (part_with(json!([{"id": ""}])), feats_msg),
        (part_with(json!([{"id": 7}])), feats_msg),
        (part_with(json!({"f1": {}})), feats_msg),
        (part_with(json!("features")), feats_msg),
        (
            json!({"name": "X", "content": {"features": [], "components": []}}),
            "a part has no components",
        ),
        (
            json!({"name": "X", "content": {"mates": []}}),
            "a part has no mates",
        ),
        (
            json!({"name": "A", "kind": "assembly",
                   "content": {"components": [{"id": "c1"}, {"id": "c1"}]}}),
            "content.components must be a list of objects with distinct ids",
        ),
        (
            json!({"name": "A", "kind": "assembly", "content": {"mates": [{"name": "m"}]}}),
            "content.mates must be a list of objects with distinct ids",
        ),
    ];
    for (body, detail) in cases {
        assert_eq!(
            create(&server, &w, &w.admin, body.clone()).await,
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({ "detail": detail })
            ),
            "{body}"
        );
    }
    assert!(
        server
            .state
            .store
            .documents_in_project(&w.project)
            .await
            .unwrap()
            .is_empty()
    );

    // Repeated feature names stay legal: names are settled at commit, not here.
    let id = w
        .document(
            &server,
            Some(json!({"features": [{"id": "f1", "name": "Same"}, {"id": "f2", "name": "Same"}]})),
            None,
            None,
        )
        .await;
    assert_eq!(
        content(&server, &id, &w.admin).await.1["content"]["features"][1]["id"],
        "f2"
    );

    // Missing lists are filled in as empty ones.
    let id = w
        .document(&server, Some(json!({"version": 3})), None, None)
        .await;
    assert_eq!(
        content(&server, &id, &w.admin).await.1["content"],
        json!({"version": 3, "features": []})
    );
    let (status, asm) = create(
        &server,
        &w,
        &w.admin,
        json!({"name": "A", "kind": "assembly", "content": {}}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        content(&server, asm["id"].as_str().unwrap(), &w.admin)
            .await
            .1["content"],
        json!({"features": [], "components": [], "mates": []})
    );
}

#[tokio::test]
async fn a_document_may_refer_to_at_most_4096_blobs() {
    let (server, w) = world().await;
    let refs: Vec<Value> = (0..4097)
        .map(|i| json!({"$blob": sha256(i.to_string().as_bytes())}))
        .collect();
    assert_eq!(
        create(
            &server,
            &w,
            &w.admin,
            json!({"name": "X", "content": {"refs": refs}})
        )
        .await,
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"detail": "content refers to more than 4096 blobs"})
        )
    );
    // 4096 are looked up (and here all missing).
    let (status, body) = create(
        &server,
        &w,
        &w.admin,
        json!({"name": "X", "content": {"refs": refs[..4096]}}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["detail"]["blobs"].as_array().unwrap().len(), 4096);
}

#[tokio::test]
async fn a_commit_that_reports_an_error_but_took_effect_is_a_success() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let w = World::new(&server).await.unwrap();
    flaky.commit_then_fail(true);
    let (status, doc) = create(
        &server,
        &w,
        &w.admin,
        json!({"name": "X", "content": part()}),
    )
    .await;
    flaky.commit_then_fail(false);
    assert_eq!(status, StatusCode::CREATED, "{doc}");
    let id = doc["id"].as_str().unwrap();
    assert_eq!(doc["role"], "owner");
    // The row and its snapshot are both there: the document opens.
    assert_eq!(
        content(&server, id, &w.admin).await.1["content"],
        stored_part()
    );
}

#[tokio::test]
async fn when_a_failed_commit_cannot_be_settled_the_snapshot_is_kept() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let w = World::new(&server).await.unwrap();
    flaky.commit_then_fail(true);
    flaky.fail_document_reads(true);
    assert_eq!(
        create(
            &server,
            &w,
            &w.admin,
            json!({"name": "X", "content": part()})
        )
        .await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"detail": "server error"})
        )
    );
    flaky.commit_then_fail(false);
    flaky.fail_document_reads(false);
    // The commit did take effect; its snapshot file was not removed, so the document opens.
    let docs = server
        .state
        .store
        .documents_in_project(&w.project)
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(files_under(&storage(&server).join("docs")).len(), 1);
    assert_eq!(
        content(&server, &docs[0].id, &w.admin).await.1["content"],
        stored_part()
    );
}

/// Blob storage whose `exists` fails (an outage), and everything else works.
struct NoLookups(Arc<dyn Blobs>);

#[async_trait]
impl Blobs for NoLookups {
    async fn put_stream(
        &self,
        key: &str,
        body: ByteStream,
        limit: u64,
    ) -> anyhow::Result<PutOutcome> {
        self.0.put_stream(key, body, limit).await
    }
    async fn get_stream(&self, key: &str) -> anyhow::Result<Option<(u64, ByteStream)>> {
        self.0.get_stream(key).await
    }
    async fn exists(&self, _key: &str) -> anyhow::Result<bool> {
        anyhow::bail!("storage is down")
    }
    async fn put(&self, key: &str, bytes: bytes::Bytes) -> anyhow::Result<()> {
        self.0.put(key, bytes).await
    }
    async fn get(&self, key: &str) -> anyhow::Result<Option<bytes::Bytes>> {
        self.0.get(key).await
    }
    async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        self.0.delete_prefix(prefix).await
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.0.delete(key).await
    }
    async fn health(&self) -> anyhow::Result<()> {
        self.0.health().await
    }
}

#[tokio::test]
async fn a_storage_outage_during_the_blob_check_is_a_server_error() {
    let server =
        TestServer::start_custom(|_| {}, |s| s.blobs = Arc::new(NoLookups(s.blobs.clone()))).await;
    let w = World::new(&server).await.unwrap();
    let mut p = part();
    p["body"] = json!({"$blob": sha256(b"x")});
    assert_eq!(
        create(&server, &w, &w.admin, json!({"name": "X", "content": p})).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"detail": "server error"})
        )
    );
    assert!(
        server
            .state
            .store
            .documents_in_project(&w.project)
            .await
            .unwrap()
            .is_empty()
    );
    // A document without blob references needs no lookup.
    w.document(&server, None, None, None).await;
}

#[tokio::test]
async fn a_creation_body_must_be_declared_as_json() {
    let (server, w) = world().await;
    let r = reqwest::Client::new()
        .post(server.url(&docs_url(&w)))
        .bearer_auth(&w.admin)
        .header("content-type", "text/plain")
        .body(json!({"name": "X", "content": part()}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert!(r.json::<Value>().await.unwrap()["detail"].is_string());
}

// ------------------------------------------------------------------ Task 16: PostgreSQL

/// On PostgreSQL a commit whose answer was not waited for can still take effect afterwards. The
/// document must not be told apart from one that was never created by a read made too early: its
/// snapshot would be removed, and the row would then appear without it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_that_lands_after_its_error_keeps_its_snapshot() {
    if !on_postgres() {
        eprintln!(
            "skipped on SQLite: a commit cannot be made slow inside SQLite, and nothing else can \
             write while one runs"
        );
        return;
    }
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let w = World::new(&server).await.unwrap();
    // Committing a new document takes a second and a half in the database ...
    server
        .sql(
            "CREATE FUNCTION slow_commit() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN PERFORM pg_sleep(1.5); RETURN NULL; END $$",
        )
        .await;
    server
        .sql(
            "CREATE CONSTRAINT TRIGGER slow_commit AFTER INSERT ON documents \
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION slow_commit()",
        )
        .await;
    // ... and the server stops waiting for it after 200 ms.
    flaky.give_up_on_commits_after(Some(std::time::Duration::from_millis(200)));
    let (status, doc) = create(
        &server,
        &w,
        &w.admin,
        json!({"name": "X", "content": part()}),
    )
    .await;
    flaky.give_up_on_commits_after(None);
    // The creation is seen to have taken effect, and is answered as the success it is.
    assert_eq!(status, StatusCode::CREATED, "{doc}");
    let id = doc["id"].as_str().unwrap();
    let docs = server
        .state
        .store
        .documents_in_project(&w.project)
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].id, id);
    // The row has its snapshot: the document opens.
    let (status, body) = content(&server, id, &w.admin).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["content"], stored_part());
}
