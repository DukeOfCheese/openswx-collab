//! Deleting a project or an organisation in two steps (the request marks, the purger removes), and
//! the per-organisation queue of requests. See `crate::purge` and `crate::org_queue`.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{FlakyStore, Live, PASSWORD, TestServer, World, new_journal, part};
use oswx_server::journal::{JournalError, Record, RefusedWhy};
use oswx_server::lease::acquire;
use oswx_server::org_queue::OrgQueue;
use oswx_server::purge::{Purger, RETRY};
use oswx_server::record::{encode_ops, encode_paths, encode_reads};
use oswx_server::store::{NewChange, Store};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

/// A server whose purger removes `chunk` rows a transaction.
async fn server_purging(chunk: i64) -> (TestServer, Arc<FlakyStore>) {
    TestServer::start_flaky_full(
        |s| s.open_registration = true,
        new_journal,
        move |s| s.purger = Purger::new(chunk, RETRY),
    )
    .await
}

async fn created(server: &TestServer, path: &str, token: &str, body: Value) -> String {
    let (status, answer) = server.post(path, Some(token), body).await;
    assert_eq!(status, StatusCode::CREATED, "{path}: {answer}");
    answer["id"].as_str().expect("an id").to_owned()
}

async fn org(server: &TestServer, token: &str, name: &str) -> String {
    created(server, "/api/orgs", token, json!({"name": name})).await
}

async fn project(server: &TestServer, token: &str, org: &str, name: &str) -> String {
    created(
        server,
        &format!("/api/orgs/{org}/projects"),
        token,
        json!({"name": name}),
    )
    .await
}

async fn document(server: &TestServer, token: &str, project: &str, name: &str) -> String {
    created(
        server,
        &format!("/api/projects/{project}/documents"),
        token,
        json!({"name": name, "content": part()}),
    )
    .await
}

async fn delete_org(server: &TestServer, token: &str, org: &str, name: &str) -> StatusCode {
    server
        .call(
            Method::DELETE,
            &format!("/api/orgs/{org}"),
            Some(token),
            Some(json!({"confirm_name": name, "current_password": PASSWORD})),
        )
        .await
        .0
}

/// `n` journal rows written straight into the store for `doc` (as its journal would hold them),
/// and the head moved.
async fn fill_journal(store: &dyn Store, doc: &str, author: &str, n: i64) {
    let ids: Vec<String> = (1..=n).map(|i| format!("c{i}")).collect();
    let ops = encode_ops(&[]);
    let rows: Vec<NewChange<'_>> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| NewChange {
            document_id: doc,
            seq: i as i64 + 1,
            parent_seq: i as i64,
            branch_id: "main",
            epoch: 0,
            author_id: author,
            client_id: Some("u.c"),
            change_id: Some(id),
            created_at: 1.0,
            command: "fill",
            ops: &ops,
            inverse: &ops,
            paths: &ops,
            reads: &ops,
            undo_of: None,
            reverts: None,
            reverted_by: None,
            broke: None,
        })
        .collect();
    let mut tx = store.begin().await.unwrap();
    assert_eq!(tx.insert_changes(&rows).await.unwrap(), n as u64);
    tx.set_document_head(doc, n, 2.0).await.unwrap();
    tx.commit().await.unwrap();
}

/// A record setting nothing, by `u.c`, as the journal tests make them.
fn record(doc: &str, author: &str, seq: u64, epoch: u64) -> Record {
    let none = std::collections::BTreeSet::new();
    Record {
        document: doc.to_owned(),
        seq,
        parent_seq: seq - 1,
        epoch,
        author: author.to_owned(),
        client_id: Some("u.j".into()),
        change_id: Some(format!("j{seq}")),
        command: "test".into(),
        ops: encode_ops(&[]),
        inverse: encode_ops(&[]),
        paths: encode_paths(&none, &std::collections::BTreeSet::new()),
        reads: encode_reads(&std::collections::BTreeSet::new()),
        undo_of: None,
        reverts: None,
        created_at: 3000.0 + seq as f64,
        extra: Vec::new(),
    }
}

async fn count(server: &TestServer, query: &str) -> i64 {
    server.scalar(query).await
}

// ------------------------------------------------------------------ nobody else waits

/// An organisation with thousands of journal rows is deleted. The request answers at once, and
/// while its rows are removed (a chunk a transaction) another organisation's changes are written
/// as fast as ever: no append waits for more than a chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_big_deletion_delays_nobody_elses_journal() {
    let (server, _flaky) = server_purging(250).await;
    let w = World::new(&server).await.unwrap();
    let store = server.state.store.clone();
    let mut doomed = Vec::new();
    for n in 0..3 {
        let doc = w
            .document(&server, None, Some(&format!("D{n}")), None)
            .await;
        fill_journal(store.as_ref(), &doc, &w.admin_id, 2000).await;
        doomed.push(doc);
    }
    let other = org(&server, &w.admin, "Other").await;
    let elsewhere = project(&server, &w.admin, &other, "Elsewhere").await;
    let kept = document(&server, &w.admin, &elsewhere, "Kept").await;
    let epoch = acquire(store.as_ref(), &kept, "node", 1000.0, 60.0)
        .await
        .unwrap();

    // The purger is held before its first chunk: the request itself must not be the slow part.
    server.state.purger.gate().shut();
    let started = Instant::now();
    assert_eq!(
        delete_org(&server, &w.admin, &w.org, "Robotics").await,
        StatusCode::NO_CONTENT
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "marking took {:?}",
        started.elapsed()
    );
    // Gone for everyone at once, with every row still there.
    let (status, orgs) = server.get("/api/orgs", Some(&w.admin)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(orgs.as_array().unwrap().len(), 1, "{orgs}");
    assert_eq!(
        server
            .get(&format!("/api/documents/{}", doomed[0]), Some(&w.admin))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(count(&server, "SELECT COUNT(*) FROM changes").await, 6000);

    // The purger works through the rows while the other organisation's document is written to.
    server.state.purger.gate().open();
    let mut slowest = Duration::ZERO;
    let mut seq = 0;
    while server.state.purger.busy() || seq < 20 {
        seq += 1;
        let started = Instant::now();
        let written = server
            .state
            .journal
            .append(record(&kept, &w.admin_id, seq, epoch))
            .await;
        assert_eq!(written, Ok(()), "append {seq}");
        slowest = slowest.max(started.elapsed());
        assert!(seq < 100_000, "the purger never finished");
    }
    server.purged().await;
    assert!(
        server.state.purger.chunks() >= 24,
        "6,000 rows went in {} chunks of 250",
        server.state.purger.chunks()
    );
    assert!(
        slowest < Duration::from_secs(1),
        "an append waited {slowest:?} for the deletion"
    );
    // Nothing of the deleted organisation is left, in the database or in storage ...
    assert_eq!(
        count(&server, "SELECT COUNT(*) FROM changes").await,
        seq as i64
    );
    assert_eq!(count(&server, "SELECT COUNT(*) FROM orgs").await, 1);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM projects").await, 1);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM documents").await, 1);
    for doc in &doomed {
        assert!(
            server
                .stored_under(&format!("docs/{doc}/"))
                .await
                .is_empty()
        );
    }
    // ... and the other organisation has everything it had.
    assert_eq!(
        store.document_by_id(&kept).await.unwrap().unwrap().head_seq,
        seq as i64
    );
    assert!(
        !server
            .stored_under(&format!("docs/{kept}/"))
            .await
            .is_empty()
    );
}

/// The same for a project: marked in one short transaction, removed in the background, with its
/// documents' snapshots.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_project_is_removed_with_its_rows_folders_and_snapshots() {
    let (server, _flaky) = server_purging(100).await;
    let w = World::new(&server).await.unwrap();
    let doomed = project(&server, &w.admin, &w.org, "Doomed").await;
    let mut docs = Vec::new();
    for n in 0..2 {
        let doc = document(&server, &w.admin, &doomed, &format!("D{n}")).await;
        fill_journal(server.state.store.as_ref(), &doc, &w.admin_id, 450).await;
        docs.push(doc);
    }
    let mut parent: Option<String> = None;
    for n in 0..5 {
        let id = created(
            &server,
            &format!("/api/projects/{doomed}/folders"),
            &w.admin,
            json!({"name": format!("F{n}"), "parent_id": parent}),
        )
        .await;
        parent = Some(id);
    }
    let (status, _) = server
        .post(
            &format!("/api/documents/{}/versions", docs[0]),
            Some(&w.admin),
            json!({"name": "Rev A"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let kept = w.document(&server, None, Some("Kept"), None).await;

    let (status, _) = server
        .delete(&format!("/api/projects/{doomed}"), Some(&w.admin))
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    server.purged().await;
    assert!(server.state.purger.chunks() >= 9);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM projects").await, 1);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM documents").await, 1);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM changes").await, 0);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM folders").await, 0);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM versions").await, 0);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM snapshots").await, 1);
    for doc in &docs {
        assert!(
            server
                .stored_under(&format!("docs/{doc}/"))
                .await
                .is_empty()
        );
    }
    assert!(
        !server
            .stored_under(&format!("docs/{kept}/"))
            .await
            .is_empty()
    );
    // The organisation is as it was but for the project.
    assert_eq!(count(&server, "SELECT COUNT(*) FROM orgs").await, 1);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM org_members").await, 1);
}

// ------------------------------------------------------------------ gone at once

/// Every route that names a document, with the method and body it takes.
fn document_routes(doc: &str) -> Vec<(Method, String, Option<Value>)> {
    let at = |rest: &str| format!("/api/documents/{doc}{rest}");
    vec![
        (Method::GET, at(""), None),
        (Method::PATCH, at(""), Some(json!({"name": "Renamed"}))),
        (Method::DELETE, at(""), None),
        (Method::GET, at("/content"), None),
        (Method::GET, at("/content?seq=0"), None),
        (Method::GET, at("/history"), None),
        (Method::GET, at("/versions"), None),
        (
            Method::POST,
            at("/versions"),
            Some(json!({"name": "Rev B"})),
        ),
        (
            Method::DELETE,
            at("/versions/0123456789abcdef0123456789abcdef"),
            None,
        ),
        (Method::POST, at("/restore"), Some(json!({"seq": 0}))),
        (Method::GET, at("/references"), None),
        (Method::GET, at("/used-in"), None),
    ]
}

/// A document deleted on its own, with its project, or with its organisation is gone from the
/// moment the request is answered, while every row of it is still in the database: 404 on every
/// route that names it, no connection to it, no change written to it, and its name free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_is_deleted_is_gone_at_once_on_every_route() {
    let (server, _flaky) = server_purging(100).await;
    let w = World::new(&server).await.unwrap();
    // Nothing is removed until the end of the test: whatever is refused is refused by the mark.
    server.state.purger.gate().shut();
    let store = server.state.store.clone();

    // One document deleted alone; one in a project that is deleted; one in an organisation that
    // is.
    let alone = w.document(&server, None, Some("Alone"), None).await;
    let doomed_project = project(&server, &w.admin, &w.org, "Doomed").await;
    let in_project = document(&server, &w.admin, &doomed_project, "InProject").await;
    let doomed_org = org(&server, &w.admin, "Doomed Org").await;
    let its_project = project(&server, &w.admin, &doomed_org, "Its").await;
    let in_org = document(&server, &w.admin, &its_project, "InOrg").await;
    let mut epochs = Vec::new();
    for doc in [&alone, &in_project, &in_org] {
        epochs.push(
            acquire(store.as_ref(), doc, "node", 1000.0, 60.0)
                .await
                .unwrap(),
        );
        let written = server
            .state
            .journal
            .append(record(doc, &w.admin_id, 1, *epochs.last().unwrap()))
            .await;
        assert_eq!(written, Ok(()));
    }
    let mut open = Vec::new();
    for doc in [&alone, &in_project, &in_org] {
        open.push(Live::connect(&server, doc, &w.admin).await);
    }

    let (status, _) = server
        .delete(&format!("/api/documents/{alone}"), Some(&w.admin))
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = server
        .delete(&format!("/api/projects/{doomed_project}"), Some(&w.admin))
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        delete_org(&server, &w.admin, &doomed_org, "Doomed Org").await,
        StatusCode::NO_CONTENT
    );
    // Every row is still there.
    assert_eq!(count(&server, "SELECT COUNT(*) FROM documents").await, 3);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM changes").await, 3);
    assert!(server.state.purger.busy());

    for live in &mut open {
        assert_eq!(live.closed().await.0, 4404);
    }
    for (doc, epoch) in [&alone, &in_project, &in_org].into_iter().zip(&epochs) {
        for (method, path, body) in document_routes(doc) {
            let (status, answer) = server
                .call(method.clone(), &path, Some(&w.admin), body)
                .await;
            assert_eq!(
                (status, &answer),
                (
                    StatusCode::NOT_FOUND,
                    &json!({"detail": "no such document"})
                ),
                "{method} {path}"
            );
        }
        // No connection, and nothing more in its journal: a writer that still holds the lease is
        // told there is no such document.
        match Live::try_connect(&server, doc, &w.admin, common::Hello::default()).await {
            Ok(_) => panic!("a deleted document opened"),
            Err(refused) => assert_eq!(refused.close, Some(4404), "{refused:?}"),
        }
        // (Under the lease as it is now, which its room took: a record that would be written
        // were the document not deleted. And under the older one.)
        let now = store.room_lease(doc).await.unwrap().unwrap().epoch as u64;
        assert!(now > *epoch);
        for held in [now, *epoch] {
            let written = server
                .state
                .journal
                .append(record(doc, &w.admin_id, 2, held))
                .await;
            assert_eq!(
                written,
                Err(JournalError::Refused(RefusedWhy::NoDocument)),
                "{doc}"
            );
        }
        assert_eq!(
            store.document_by_id(doc).await.unwrap().unwrap().head_seq,
            1
        );
    }

    // The deleted project: every route that names it, and it is in no list.
    let p = |rest: &str| format!("/api/projects/{doomed_project}{rest}");
    let sha = "0".repeat(64);
    for (method, path, body) in [
        (Method::GET, p("/documents"), None),
        (
            Method::POST,
            p("/documents"),
            Some(json!({"name": "New", "content": part()})),
        ),
        (Method::GET, p("/folders"), None),
        (Method::POST, p("/folders"), Some(json!({"name": "F"}))),
        (Method::PATCH, p(""), Some(json!({"name": "Renamed"}))),
        (Method::DELETE, p(""), None),
        (Method::GET, p("/members"), None),
        (
            Method::PUT,
            p(&format!("/members/{}", w.admin_id)),
            Some(json!({"role": "editor"})),
        ),
        (Method::DELETE, p(&format!("/members/{}", w.admin_id)), None),
        (Method::POST, p("/blobs/missing"), Some(json!([sha]))),
        (Method::GET, p(&format!("/blobs/{sha}")), None),
    ] {
        let (status, answer) = server
            .call(method.clone(), &path, Some(&w.admin), body)
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}: {answer}");
    }
    let (_, listed) = server
        .get(&format!("/api/orgs/{}/projects", w.org), Some(&w.admin))
        .await;
    let names: Vec<&str> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(!names.contains(&"Doomed"), "{names:?}");

    // The deleted organisation: 404 as one that never was, in no list, and nobody's.
    let o = |rest: &str| format!("/api/orgs/{doomed_org}{rest}");
    for (method, path, body) in [
        (Method::GET, o("/members"), None),
        (Method::GET, o("/projects"), None),
        (Method::POST, o("/projects"), Some(json!({"name": "P"}))),
        (Method::PATCH, o(""), Some(json!({"name": "Renamed"}))),
        (Method::GET, o("/invites"), None),
        (Method::POST, o("/invites"), Some(json!({}))),
        (Method::GET, o("/export"), None),
        (
            Method::DELETE,
            o(""),
            Some(json!({"confirm_name": "Doomed Org", "current_password": PASSWORD})),
        ),
    ] {
        let (status, answer) = server
            .call(method.clone(), &path, Some(&w.admin), body)
            .await;
        assert_eq!(
            (status, &answer),
            (
                StatusCode::NOT_FOUND,
                &json!({"detail": "no such organisation"})
            ),
            "{method} {path}"
        );
    }
    let (_, orgs) = server.get("/api/orgs", Some(&w.admin)).await;
    assert_eq!(orgs.as_array().unwrap().len(), 1, "{orgs}");
    let (status, _) = server
        .get(
            &format!("/api/projects/{its_project}/documents"),
            Some(&w.admin),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The names are free at once.
    let again = org(&server, &w.admin, "Doomed Org").await;
    let project_again = project(&server, &w.admin, &w.org, "Doomed").await;
    let doc_again = document(&server, &w.admin, &project_again, "InProject").await;
    let _ = w.document(&server, None, Some("Alone"), None).await;
    assert_ne!(again, doomed_org);

    // Then the rows go; what was made since stays, and so does the document deleted on its own
    // (its row is kept, as it always was: assemblies may still say what they used).
    server.state.purger.gate().open();
    server.purged().await;
    assert_eq!(count(&server, "SELECT COUNT(*) FROM orgs").await, 2);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM projects").await, 2);
    let left: Vec<String> = {
        let mut ids = Vec::new();
        for doc in [&alone, &in_project, &in_org, &doc_again] {
            if store.document_by_id(doc).await.unwrap().is_some() {
                ids.push(doc.clone());
            }
        }
        ids
    };
    assert_eq!(left, [alone.clone(), doc_again]);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM changes").await, 1);
    assert!(
        store
            .document_by_id(&alone)
            .await
            .unwrap()
            .unwrap()
            .deleted_at
            .is_some()
    );
}

/// Someone who cannot delete marks nothing: the hostile side of the routes above.
#[tokio::test]
async fn only_who_may_delete_marks_anything() {
    let server = TestServer::start_with(|s| s.open_registration = true).await;
    let w = World::new(&server).await.unwrap();
    let (bob, _) = w.editor(&server, "bob@example.com").await;
    let (eve, _) = server.register("eve@example.com").await;
    let doc = w.document(&server, None, None, None).await;
    for (token, want) in [(&bob, StatusCode::FORBIDDEN), (&eve, StatusCode::NOT_FOUND)] {
        let (status, _) = server
            .delete(&format!("/api/projects/{}", w.project), Some(token))
            .await;
        assert_eq!(status, want);
        assert_eq!(delete_org(&server, token, &w.org, "Robotics").await, want);
    }
    // A wrong name, or a wrong password, marks nothing either.
    let (status, _) = server
        .call(
            Method::DELETE,
            &format!("/api/orgs/{}", w.org),
            Some(&w.admin),
            Some(json!({"confirm_name": "robotics", "current_password": PASSWORD})),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    server.purged().await;
    assert_eq!(
        count(
            &server,
            "SELECT COUNT(*) FROM projects WHERE deleted_at IS NOT NULL"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &server,
            "SELECT COUNT(*) FROM orgs WHERE deleted_at IS NOT NULL"
        )
        .await,
        0
    );
    let (status, _) = server
        .get(&format!("/api/documents/{doc}"), Some(&w.admin))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!server.state.purger.busy());
}

// ------------------------------------------------------------------ starting again

/// The server stops while the purger is in the middle of an organisation. The next start finds
/// what is still marked and removes it: no row and no file is left (in a folder, and in S3 when
/// the suite runs with `OSWX_TEST_STORAGE=s3`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_purger_goes_on_after_a_restart_and_leaves_nothing() {
    let (server, _flaky) = server_purging(10).await;
    let w = World::new(&server).await.unwrap();
    let store = server.state.store.clone();
    let mut docs = Vec::new();
    for n in 0..3 {
        let doc = w
            .document(&server, None, Some(&format!("D{n}")), None)
            .await;
        fill_journal(store.as_ref(), &doc, &w.admin_id, 600).await;
        docs.push(doc);
    }
    // A blob of the organisation, and a second snapshot of a document.
    let body = b"a blob of the organisation".to_vec();
    let sha = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&body));
    let put = reqwest::Client::new()
        .put(server.url(&format!("/api/projects/{}/blobs/{sha}", w.project)))
        .bearer_auth(&w.admin)
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(put.status().is_success(), "{}", put.status());
    let blobs = format!("orgs/{}/", w.org);
    assert_eq!(server.stored_under(&blobs).await.len(), 1);
    let other = org(&server, &w.admin, "Other").await;
    let elsewhere = project(&server, &w.admin, &other, "Elsewhere").await;
    let kept = document(&server, &w.admin, &elsewhere, "Kept").await;

    // The purger removes a few chunks, then the server stops under it.
    assert_eq!(
        delete_org(&server, &w.admin, &w.org, "Robotics").await,
        StatusCode::NO_CONTENT
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while server.state.purger.chunks() < 3 {
        assert!(Instant::now() < deadline, "the purger did not start");
        tokio::task::yield_now().await;
    }
    server.state.purger.gate().shut();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = count(&server, "SELECT COUNT(*) FROM changes").await;
    assert!(
        (1..1800).contains(&before),
        "stopped in the middle: {before} of 1,800 rows left"
    );
    assert_eq!(
        count(
            &server,
            "SELECT COUNT(*) FROM orgs WHERE deleted_at IS NOT NULL"
        )
        .await,
        1
    );

    let server = server.restart().await;
    // Still gone for everyone, before and after the new server has finished the work.
    let (_, orgs) = server.get("/api/orgs", Some(&w.admin)).await;
    assert_eq!(orgs.as_array().unwrap().len(), 1, "{orgs}");
    server.purged().await;
    assert_eq!(count(&server, "SELECT COUNT(*) FROM changes").await, 0);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM snapshots").await, 1);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM documents").await, 1);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM projects").await, 1);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM orgs").await, 1);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM org_members").await, 1);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM room_leases").await, 0);
    for doc in &docs {
        assert!(
            server
                .stored_under(&format!("docs/{doc}/"))
                .await
                .is_empty()
        );
    }
    assert!(server.stored_under(&blobs).await.is_empty());
    // The other organisation's files are where they were.
    assert_eq!(server.stored_under(&format!("docs/{kept}/")).await.len(), 1);
    let (status, _) = server
        .get(&format!("/api/documents/{kept}/content"), Some(&w.admin))
        .await;
    assert_eq!(status, StatusCode::OK);
}

/// A storage that cannot remove a document's files keeps its rows: the purger tries again, and
/// nothing is left once the storage works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_wait_for_their_files_when_storage_fails() {
    let mut handle = None;
    let server = TestServer::start_custom(
        |s| s.open_registration = true,
        |s| {
            let blobs = Arc::new(common::TestBlobs::new(s.blobs.clone()));
            handle = Some(blobs.clone());
            s.blobs = blobs;
            s.purger = Purger::new(100, Duration::from_millis(200));
        },
    )
    .await;
    let blobs = handle.unwrap();
    let w = World::new(&server).await.unwrap();
    let doc = w.document(&server, None, None, None).await;
    blobs.fail_delete(true);
    assert_eq!(
        delete_org(&server, &w.admin, &w.org, "Robotics").await,
        StatusCode::NO_CONTENT
    );
    tokio::time::sleep(Duration::from_millis(700)).await;
    // Gone for everyone; its rows and files wait.
    assert!(server.state.purger.busy());
    assert_eq!(count(&server, "SELECT COUNT(*) FROM documents").await, 1);
    assert_eq!(server.stored_under(&format!("docs/{doc}/")).await.len(), 1);
    let (_, orgs) = server.get("/api/orgs", Some(&w.admin)).await;
    assert_eq!(orgs, json!([]));
    blobs.fail_delete(false);
    server.purged().await;
    assert_eq!(count(&server, "SELECT COUNT(*) FROM documents").await, 0);
    assert_eq!(count(&server, "SELECT COUNT(*) FROM orgs").await, 0);
    assert!(
        server
            .stored_under(&format!("docs/{doc}/"))
            .await
            .is_empty()
    );
}

// ------------------------------------------------------------------ the organisation's queue

/// Many requests that change one organisation, while one of them is held in its transaction: the
/// others wait in the server, not in the database (one transaction is open, whatever their
/// number), another organisation is not held up, and one waiter too many is told at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writers_to_one_organisation_queue_without_a_connection_each() {
    let (server, flaky) = TestServer::start_flaky_full(
        |s| s.open_registration = true,
        new_journal,
        |s| s.org_queue = OrgQueue::new(6, Duration::from_secs(30)),
    )
    .await;
    let w = World::new(&server).await.unwrap();
    let other = org(&server, &w.admin, "Other").await;
    let server = Arc::new(server);
    let write = |path: String, body: Value| {
        let (server, token) = (server.clone(), w.admin.clone());
        tokio::spawn(async move { server.post(&path, Some(&token), body).await })
    };

    // One request is parked in its transaction, holding the organisation's turn and row lock.
    flaky.org_gate.shut();
    let holder = {
        let (server, w) = (server.clone(), (w.org.clone(), w.admin.clone()));
        tokio::spawn(async move {
            server
                .patch(
                    &format!("/api/orgs/{}", w.0),
                    Some(&w.1),
                    json!({"name": "Held"}),
                )
                .await
        })
    };
    flaky.org_gate.until_waiting(1).await;
    let begins = flaky.begins();
    // Six more queue behind it (as many as may wait) ...
    let queued: Vec<_> = (0..6)
        .map(|n| {
            write(
                format!("/api/orgs/{}/projects", w.org),
                json!({"name": format!("Queued {n}")}),
            )
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    while server.state.org_queue.waiting(&w.org) < 6 {
        assert!(Instant::now() < deadline, "the requests did not queue");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // ... and the seventh is answered at once: busy, nothing done.
    let started = Instant::now();
    let (status, answer) = server
        .post(
            &format!("/api/orgs/{}/projects", w.org),
            Some(&w.admin),
            json!({"name": "One too many"}),
        )
        .await;
    assert_eq!(
        (status, answer),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"detail": "the server is busy; try again"})
        )
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    // None of the waiters has opened a transaction.
    assert_eq!(flaky.begins(), begins);

    // Another organisation's write goes through meanwhile, at once: on PostgreSQL. (On SQLite
    // the parked request holds the one writer, and every write waits for it.)
    if common::on_postgres() {
        let started = Instant::now();
        let (status, _) = write(
            format!("/api/orgs/{other}/projects"),
            json!({"name": "Elsewhere"}),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(flaky.begins(), begins + 1, "its one transaction");
    }

    // The holder ends; the queued ones follow, each in its turn, and all succeed.
    flaky.org_gate.open();
    assert_eq!(holder.await.unwrap().0, StatusCode::OK);
    for task in queued {
        let (status, answer) = task.await.unwrap();
        assert_eq!(status, StatusCode::CREATED, "{answer}");
    }
    let names: Vec<String> = server
        .state
        .store
        .projects_in_org(&w.org)
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .filter(|n| n.starts_with("Queued") || n == "One too many")
        .collect();
    assert_eq!(names.len(), 6, "{names:?}");
    // No entry is kept for an organisation nobody is changing.
    assert_eq!(server.state.org_queue.organisations(), 0);
}

/// A request that waits too long for its organisation's turn is answered busy, and wrote nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_turn_that_does_not_come_is_answered_busy() {
    let (server, flaky) = TestServer::start_flaky_full(
        |_| {},
        new_journal,
        |s| s.org_queue = OrgQueue::new(8, Duration::from_millis(400)),
    )
    .await;
    let w = World::new(&server).await.unwrap();
    let server = Arc::new(server);
    flaky.org_gate.shut();
    let holder = {
        let (server, w) = (server.clone(), (w.org.clone(), w.admin.clone()));
        tokio::spawn(async move {
            server
                .patch(
                    &format!("/api/orgs/{}", w.0),
                    Some(&w.1),
                    json!({"name": "Held"}),
                )
                .await
        })
    };
    flaky.org_gate.until_waiting(1).await;
    // Every kind of write to the organisation waits for the same turn.
    let doc = format!("/api/projects/{}/documents", w.project);
    let folder = format!("/api/projects/{}/folders", w.project);
    let invites = format!("/api/orgs/{}/invites", w.org);
    for (path, body) in [
        (doc.as_str(), json!({"name": "D", "content": part()})),
        (folder.as_str(), json!({"name": "F"})),
        (invites.as_str(), json!({})),
    ] {
        let (status, answer) = server.post(path, Some(&w.admin), body).await;
        assert_eq!(
            (status, answer),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"detail": "the server is busy; try again"})
            ),
            "{path}"
        );
    }
    flaky.org_gate.open();
    assert_eq!(holder.await.unwrap().0, StatusCode::OK);
    assert!(
        server
            .state
            .store
            .documents_in_project(&w.project)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(server.state.org_queue.organisations(), 0);
}
