//! Assemblies: components that use parts by reference, pinned to a state of the part. (A port of
//! `tests/test_assemblies.py`, adapted to protocol v2, with the v2 rules: every failed reference
//! check says the same, whatever failed; what assemblies use cannot be deleted, and the 409 names
//! only assemblies the caller can open, without counting the others.)

mod common;

use std::collections::BTreeMap;

use common::*;
use oswx_core::Op;
use oswx_core::wire::{ChangeEvent, RejectCode, ServerMsg};
use oswx_server::assemblies::no_part;
use oswx_server::store::Id;
use serde_json::{Value as Json, json};

// ------------------------------------------------------------------ helpers

/// Creates an assembly with `content` named `name` in `project`, by `token`.
async fn make(
    server: &TestServer,
    content: Json,
    name: &str,
    token: &str,
    project: &str,
) -> (u16, Json) {
    let (status, body) = server
        .post(
            &format!("/api/projects/{project}/documents"),
            Some(token),
            json!({"name": name, "kind": "assembly", "content": content}),
        )
        .await;
    (status.as_u16(), body)
}

/// [`make`] in the world's project by the admin; panics unless 201.
async fn make_ok(server: &TestServer, world: &World, content: Json, name: &str) -> Id {
    let (status, body) = make(server, content, name, &world.admin, &world.project).await;
    assert_eq!(status, 201, "{body}");
    body["id"].as_str().unwrap().to_owned()
}

/// `GET /references`, by component id (panics unless 200).
async fn refs(server: &TestServer, doc: &str, token: &str) -> BTreeMap<String, Json> {
    let (status, body) = server
        .get(&format!("/api/documents/{doc}/references"), Some(token))
        .await;
    assert_eq!(status, 200, "{body}");
    body.as_array()
        .unwrap()
        .iter()
        .map(|r| (r["component"].as_str().unwrap().to_owned(), r.clone()))
        .collect()
}

fn keys(refs: &BTreeMap<String, Json>) -> Vec<&str> {
    refs.keys().map(String::as_str).collect()
}

/// A project of the world's organisation, made by the admin.
async fn project(server: &TestServer, world: &World, name: &str) -> Id {
    let (status, body) = server
        .post(
            &format!("/api/orgs/{}/projects", world.org),
            Some(&world.admin),
            json!({"name": name}),
        )
        .await;
    assert_eq!(status, 201, "{body}");
    body["id"].as_str().unwrap().to_owned()
}

/// A part with no features in `project`, by `token`.
async fn part_in(server: &TestServer, token: &str, project: &str, name: &str) -> Id {
    let (status, body) = server
        .post(
            &format!("/api/projects/{project}/documents"),
            Some(token),
            json!({"name": name, "content": {"features": []}}),
        )
        .await;
    assert_eq!(status, 201, "{body}");
    body["id"].as_str().unwrap().to_owned()
}

async fn content(server: &TestServer, doc: &str, token: &str) -> Json {
    let (status, body) = server
        .get(&format!("/api/documents/{doc}/content"), Some(token))
        .await;
    assert_eq!(status, 200, "{body}");
    body["content"].clone()
}

async fn delete(server: &TestServer, path: &str, token: &str) -> (u16, Json) {
    let (status, body) = server.delete(path, Some(token)).await;
    (status.as_u16(), body)
}

async fn version(server: &TestServer, doc: &str, token: &str, name: &str) {
    let (status, body) = server
        .post(
            &format!("/api/documents/{doc}/versions"),
            Some(token),
            json!({"name": name}),
        )
        .await;
    assert_eq!(status, 201, "{body}");
}

async fn restore(server: &TestServer, doc: &str, token: &str, seq: u64) -> (u16, Json) {
    let (status, body) = server
        .post(
            &format!("/api/documents/{doc}/restore"),
            Some(token),
            json!({"seq": seq}),
        )
        .await;
    (status.as_u16(), body)
}

type Row = (String, String, i64);

/// The `assembly_refs` rows of an assembly.
async fn table(server: &TestServer, asm: &str) -> Vec<Row> {
    server
        .state
        .store
        .assembly_refs(asm)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.component_id, r.part_id, r.seq))
        .collect()
}

/// What the rows of an assembly with this content must be.
fn rows_of(content: &Json) -> Vec<Row> {
    let mut rows: Vec<Row> = content["components"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["id"].as_str().unwrap().to_owned(),
                c["ref"]["document"].as_str().unwrap().to_owned(),
                c["ref"]["seq"].as_i64().unwrap(),
            )
        })
        .collect();
    rows.sort();
    rows
}

/// Asserts the table holds exactly what the assembly's content says.
async fn table_matches(server: &TestServer, asm: &str, token: &str) {
    let content = content(server, asm, token).await;
    assert_eq!(table(server, asm).await, rows_of(&content));
}

async fn bob(server: &TestServer, world: &World) -> (Token, Id) {
    world
        .invite(
            server,
            "bob@example.com",
            Some("Bob"),
            Some("editor"),
            "member",
        )
        .await
}

fn rejected_message(outcome: &Outcome) -> &str {
    match outcome {
        Outcome::Rejected { message, .. } => message,
        other => panic!("expected a rejection, got {other:?}"),
    }
}

// ------------------------------------------------------------------ the Python suite

#[tokio::test]
async fn an_assembly_pins_parts_and_checks_what_it_points_at() {
    let (server, world) = world().await;
    let part = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &part, &world.admin).await;
    assert_eq!(a.change(vec![set_depth(20)], vec![], vec![]).await.seq(), 1);
    let other_kind = make_ok(&server, &world, assembly(vec![], vec![]), "Other").await;
    let cases = [
        (
            assembly(vec![component("c1", "nope", 0, None)], vec![]),
            "there is no part",
        ),
        (
            assembly(vec![component("c1", &part, 9, None)], vec![]),
            "there is no part",
        ),
        (
            assembly(vec![component("c1", &other_kind, 0, None)], vec![]),
            "there is no part",
        ),
        (
            assembly(vec![json!({"id": "c1", "part": {"features": []}})], vec![]),
            "does not carry copies",
        ),
        (
            assembly(
                vec![json!({"id": "c1", "ref": {"document": part, "seq": "1"}})],
                vec![],
            ),
            "needs ref",
        ),
        (
            assembly(
                vec![json!({"id": "c1", "ref": {"document": part, "seq": true}})],
                vec![],
            ),
            "needs ref",
        ),
        (
            assembly(
                vec![
                    component("c1", &part, 0, None),
                    component("c1", &part, 0, None),
                ],
                vec![],
            ),
            "distinct ids",
        ),
    ];
    for (bad, why) in cases {
        let (status, body) = make(&server, bad, "Bad", &world.admin, &world.project).await;
        assert_eq!(status, 422, "{why}: {body}");
        assert!(
            body["detail"].as_str().unwrap().contains(why),
            "{why}: {body}"
        );
    }
    // The three failures that depend on the part say exactly the same.
    let message = |part: &str, seq: i64| {
        let server = &server;
        let world = &world;
        let content = assembly(vec![component("c1", part, seq, None)], vec![]);
        async move {
            make(server, content, "Bad", &world.admin, &world.project)
                .await
                .1
        }
    };
    let nope = message("nope", 0).await;
    assert_eq!(message(&part, 9).await, nope);
    assert_eq!(message(&other_kind, 0).await, nope);
    assert_eq!(nope["detail"], no_part("c1"));

    let asm = make_ok(
        &server,
        &world,
        assembly(
            vec![
                component("c1", &part, 1, None),
                component("c2", &part, 0, None),
            ],
            vec![],
        ),
        "Rover",
    )
    .await;
    let got = refs(&server, &asm, &world.admin).await;
    assert_eq!(got["c1"]["pinned"], json!({"seq": 1, "versions": []}));
    assert_eq!(got["c1"]["newer_edits"], false);
    assert_eq!(got["c2"]["newer_edits"], true);
    assert_eq!(got["c2"]["changes_since_pinned"], 1);
    assert_eq!(got["c2"]["update_available"], false);
    assert_eq!(got["c2"]["latest_version"], Json::Null);
    assert_eq!(got["c2"]["document"]["name"], "Bracket");
    assert_eq!(got["c2"]["document"]["accessible"], true);
    let (status, body) = server
        .get(
            &format!("/api/documents/{part}/references"),
            Some(&world.admin),
        )
        .await;
    assert_eq!(status, 422);
    assert_eq!(body["detail"], "only assemblies have references");
    // Parts stay parts.
    let (status, _) = server
        .post(
            &format!("/api/projects/{}/documents", world.project),
            Some(&world.admin),
            json!({"name": "P", "content": {"features": [], "components": []}}),
        )
        .await;
    assert_eq!(status, 422);
    table_matches(&server, &asm, &world.admin).await;
}

#[tokio::test]
async fn editing_a_part_never_changes_an_assembly_until_someone_updates_it() {
    let (server, world) = world().await;
    let (bob, _) = bob(&server, &world).await;
    let part = world.document(&server, None, None, None).await;
    let asm = make_ok(
        &server,
        &world,
        assembly(
            vec![
                component("c1", &part, 0, None),
                component("c2", &part, 0, None),
            ],
            vec![json!({"id": "m1", "type": "coincident", "a": "c1", "b": "c2"})],
        ),
        "Rover",
    )
    .await;
    let mut p = Live::connect(&server, &part, &world.admin).await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    let mut b = Live::connect(&server, &asm, &bob).await;
    let welcome = a.welcome.content.clone().unwrap();
    assert_eq!(
        welcome.at(&["components", "c1", "ref"]),
        Some(&value(json!({"document": part, "seq": 0, "version": null})))
    );
    // Ada reworks the bracket: three edits, then names it.
    for n in 0..3 {
        assert_eq!(
            p.change(vec![set_depth(20 + n)], vec![], vec![])
                .await
                .seq(),
            n as u64 + 1
        );
    }
    let heard = b.drain().await;
    assert!(
        !heard
            .iter()
            .any(|m| matches!(m, ServerMsg::Change(_) | ServerMsg::ReferenceUpdate { .. })),
        "{heard:?}"
    );
    // Edits alone are not an "update".
    assert_eq!(
        refs(&server, &asm, &bob).await["c1"]["update_available"],
        false
    );
    version(&server, &part, &world.admin, "Rev B").await;
    let ev = loop {
        match b.recv().await {
            m @ ServerMsg::ReferenceUpdate { .. } => break m,
            _ => continue,
        }
    };
    assert_eq!(
        ev,
        ServerMsg::ReferenceUpdate {
            document: part.clone(),
            components: vec!["c1".into(), "c2".into()],
        }
    );
    let got = &refs(&server, &asm, &bob).await["c1"];
    assert_eq!(got["update_available"], true);
    assert_eq!(got["latest_version"]["name"], "Rev B");
    assert_eq!(got["changes_since_pinned"], 3);
    // What changed in between, for the client's "which mates would break" check.
    let (_, between) = server
        .get(
            &format!("/api/documents/{part}/history?after=0&before=4"),
            Some(&bob),
        )
        .await;
    let seqs: Vec<i64> = between
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(seqs, [3, 2, 1]);
    let (_, old) = server
        .get(&format!("/api/documents/{part}/content?seq=0"), Some(&bob))
        .await;
    assert_eq!(old["content"]["features"][1]["params"]["depth"], 10);
    // Bob moves a component to Rev B: an ordinary change, locked and conflict-checked like any.
    let lock = a.lock(&[&["components", "c2"]]).await;
    assert_eq!(lock.granted.len(), 1);
    assert_eq!(
        b.change(set_ref("c2", 3, Some("Rev B")), vec![], vec![])
            .await
            .code(),
        RejectCode::Locked
    );
    let repin = b.make_change(b.seq, set_ref("c1", 3, Some("Rev B")));
    b.send_change(&repin).await;
    assert_eq!(b.outcome(&repin.change_id).await.seq(), 1);
    let ev: ChangeEvent = a.expect().await;
    assert_eq!(ev.ops, set_ref("c1", 3, Some("Rev B")));
    let got = refs(&server, &asm, &bob).await;
    assert_eq!(
        got["c1"]["pinned"],
        json!({"seq": 3, "versions": ["Rev B"]})
    );
    assert_eq!(got["c1"]["update_available"], false);
    assert_eq!(got["c2"]["update_available"], true);
    // ... and cannot point it at a state the part does not have, or at nothing.
    assert_eq!(
        b.change(set_ref("c1", 99, None), vec![], vec![])
            .await
            .code(),
        RejectCode::Invalid
    );
    let unset = Op::Unset {
        path: path(&["components", "c1", "ref"]),
    };
    assert_eq!(
        b.change(vec![unset], vec![], vec![]).await.code(),
        RejectCode::Invalid
    );
    assert_eq!(refs(&server, &asm, &bob).await["c1"]["pinned"]["seq"], 3);
    // Undo puts the pin back.
    assert_eq!(
        b.undo(&repin.change_id, set_ref("c1", 0, None)).await.seq(),
        2
    );
    assert_eq!(refs(&server, &asm, &bob).await["c1"]["pinned"]["seq"], 0);
    table_matches(&server, &asm, &bob).await;
}

#[tokio::test]
async fn components_mates_and_positions_are_edited_live() {
    let (server, world) = world().await;
    let (bob, _) = bob(&server, &world).await;
    let part = world.document(&server, None, None, None).await;
    let asm = make_ok(
        &server,
        &world,
        assembly(vec![component("c1", &part, 0, None)], vec![]),
        "Rover",
    )
    .await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    let mut b = Live::connect(&server, &asm, &bob).await;
    let (c2, m1) = (a.new_id(), a.new_id());
    let add = vec![
        add_component(component(&c2, &part, 0, None)),
        add_mate(json!({"id": m1, "type": "coincident", "a": "c1", "b": c2})),
    ];
    assert_eq!(a.change(add.clone(), vec![], vec![]).await.seq(), 1);
    let ev: ChangeEvent = b.expect().await;
    assert_eq!(ev.ops, add);
    // Both move different components at the same moment: both go in.
    assert_eq!(
        a.change_on(1, vec![move_component("c1", 1.0)]).await.seq(),
        2
    );
    assert_eq!(
        b.change_on(1, vec![move_component(&c2, 2.0)]).await.seq(),
        3
    );
    // ... the same component: the second is told someone got there first.
    assert_eq!(
        b.change_on(1, vec![move_component("c1", 9.0)]).await.code(),
        RejectCode::Stale
    );
    // A mate someone holds a lock on cannot be removed.
    assert_eq!(a.lock(&[&["mates", &m1]]).await.granted.len(), 1);
    let take_out = vec![remove(&["mates"], &m1), remove(&["components"], &c2)];
    assert_eq!(
        b.change_on(3, take_out.clone()).await.code(),
        RejectCode::Locked
    );
    a.unlock(&[&["mates", &m1]]).await;
    a.drain().await;
    assert_eq!(b.change_on(3, take_out).await.seq(), 4);
    let got = content(&server, &asm, &bob).await;
    assert_eq!(got["components"].as_array().unwrap().len(), 1);
    assert_eq!(got["mates"], json!([]));
    assert_eq!(got["components"][0]["x"][0], 1.0);
    assert_eq!(keys(&refs(&server, &asm, &bob).await), ["c1"]);
    // Restore brings the component, its mate and its reference back.
    assert_eq!(
        restore(&server, &asm, &bob, 3).await,
        (200, json!({"seq": 5}))
    );
    let got = refs(&server, &asm, &bob).await;
    assert_eq!(got.len(), 2);
    assert!(got.contains_key(c2.as_ref()));
    table_matches(&server, &asm, &bob).await;
}

#[tokio::test]
async fn where_a_part_is_used_and_why_it_cannot_be_deleted() {
    let (server, world) = world().await;
    let part = world.document(&server, None, Some("Bracket"), None).await;
    let loose = world.document(&server, None, Some("Loose"), None).await;
    let one = make_ok(
        &server,
        &world,
        assembly(
            vec![
                component("c1", &part, 0, None),
                component("c2", &part, 0, None),
            ],
            vec![],
        ),
        "Rover",
    )
    .await;
    let two = make_ok(
        &server,
        &world,
        assembly(vec![component("k1", &part, 0, None)], vec![]),
        "Arm",
    )
    .await;
    let (_, used) = server
        .get(
            &format!("/api/documents/{part}/used-in"),
            Some(&world.admin),
        )
        .await;
    assert_eq!(
        used,
        json!([
            {"document": {"id": two, "name": "Arm", "project_id": world.project},
             "components": [{"component": "k1", "seq": 0}]},
            {"document": {"id": one, "name": "Rover", "project_id": world.project},
             "components": [{"component": "c1", "seq": 0}, {"component": "c2", "seq": 0}]},
        ])
    );
    let (_, none) = server
        .get(
            &format!("/api/documents/{loose}/used-in"),
            Some(&world.admin),
        )
        .await;
    assert_eq!(none, json!([]));
    let (status, body) = delete(&server, &format!("/api/documents/{part}"), &world.admin).await;
    assert_eq!(status, 409);
    assert_eq!(
        body["detail"],
        json!({"message": "assemblies use this part; remove it from them first",
               "assemblies": [{"id": two, "name": "Arm"}, {"id": one, "name": "Rover"}],
               "in_projects_you_cannot_open": false})
    );
    // Refused: the part is still there.
    let (status, _) = server
        .get(&format!("/api/documents/{part}"), Some(&world.admin))
        .await;
    assert_eq!(status, 200);
    let (status, _) = delete(&server, &format!("/api/documents/{loose}"), &world.admin).await;
    assert_eq!(status, 204);
    // Once nothing uses it (assemblies deleted, or the component removed), it can go.
    let (status, _) = delete(&server, &format!("/api/documents/{two}"), &world.admin).await;
    assert_eq!(status, 204);
    let mut a = Live::connect(&server, &one, &world.admin).await;
    let out = vec![remove(&["components"], "c1"), remove(&["components"], "c2")];
    assert_eq!(a.change(out, vec![], vec![]).await.seq(), 1);
    assert!(table(&server, &one).await.is_empty());
    a.close().await;
    let (status, _) = delete(&server, &format!("/api/documents/{part}"), &world.admin).await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn references_respect_projects_and_organisations() {
    let (server, world) = world().await;
    // A second project in the same organisation, which Bob cannot see.
    let secret = project(&server, &world, "Secret").await;
    let hidden = part_in(&server, &world.admin, &secret, "Gearbox").await;
    let (bob, _) = bob(&server, &world).await;
    let shared = world.document(&server, None, None, None).await;
    // Bob cannot build on a part he cannot open, and cannot tell it from one that does not exist.
    let (status, body) = make(
        &server,
        assembly(vec![component("c1", &hidden, 0, None)], vec![]),
        "Rover",
        &bob,
        &world.project,
    )
    .await;
    assert_eq!((status, &body["detail"]), (422, &json!(no_part("c1"))));
    let (_, nothing) = make(
        &server,
        assembly(vec![component("c1", &"0".repeat(32), 0, None)], vec![]),
        "Rover",
        &bob,
        &world.project,
    )
    .await;
    assert_eq!(nothing, body);
    let (status, body) = make(
        &server,
        assembly(vec![component("c1", &shared, 0, None)], vec![]),
        "Rover",
        &bob,
        &world.project,
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let asm = body["id"].as_str().unwrap().to_owned();
    let mut b = Live::connect(&server, &asm, &bob).await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    let c2 = a.new_id();
    let add = vec![add_component(component(&c2, &hidden, 0, None))];
    assert_eq!(
        b.change(add.clone(), vec![], vec![]).await.code(),
        RejectCode::Invalid
    );
    // Ada can (she owns both projects); Bob then sees that the component exists and nothing
    // about its part.
    assert_eq!(a.change(add, vec![], vec![]).await.seq(), 1);
    let got = refs(&server, &asm, &bob).await;
    assert_eq!(
        got[c2.as_ref()],
        json!({"component": c2.as_ref(), "document": {"id": hidden, "accessible": false},
               "pinned": {"seq": 0}})
    );
    assert_eq!(got["c1"]["document"]["name"], "Bracket");
    let (status, _) = server
        .get(&format!("/api/documents/{hidden}/content"), Some(&bob))
        .await;
    assert_eq!(status, 404);
    // Where-used lists only assemblies the asker can open.
    make(
        &server,
        assembly(vec![component("z1", &shared, 0, None)], vec![]),
        "Private",
        &world.admin,
        &secret,
    )
    .await;
    let names = |token: &str| {
        let (server, shared, token) = (&server, &shared, token.to_owned());
        async move {
            let (_, used) = server
                .get(&format!("/api/documents/{shared}/used-in"), Some(&token))
                .await;
            used.as_array()
                .unwrap()
                .iter()
                .map(|u| u["document"]["name"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(names(&world.admin).await, ["Private", "Rover"]);
    assert_eq!(names(&bob).await, ["Rover"]);
    // Another organisation's part is out of reach, even for someone who owns both.
    let (_, other_org) = server
        .post("/api/orgs", Some(&world.admin), json!({"name": "Other"}))
        .await;
    let (_, other_project) = server
        .post(
            &format!("/api/orgs/{}/projects", other_org["id"].as_str().unwrap()),
            Some(&world.admin),
            json!({"name": "X"}),
        )
        .await;
    let foreign = part_in(
        &server,
        &world.admin,
        other_project["id"].as_str().unwrap(),
        "Foreign",
    )
    .await;
    let (status, body) = make(
        &server,
        assembly(vec![component("c1", &foreign, 0, None)], vec![]),
        "Rover",
        &world.admin,
        &world.project,
    )
    .await;
    assert_eq!((status, &body["detail"]), (422, &json!(no_part("c1"))));
}

#[tokio::test]
async fn an_assembly_survives_a_restart_with_its_references() {
    let server = TestServer::start_with(|s| s.snapshot_every = 2).await;
    let world = World::new(&server).await.unwrap();
    let part = world.document(&server, None, None, None).await;
    let asm = make_ok(
        &server,
        &world,
        assembly(vec![component("c1", &part, 0, None)], vec![]),
        "Rover",
    )
    .await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    let mut added = Vec::new();
    for n in 1..6u64 {
        let ops = if n % 2 == 1 {
            let id = a.new_id();
            added.push(id.to_string());
            vec![add_component(component(&id, &part, 0, None))]
        } else {
            vec![move_component("c1", n as f64)]
        };
        assert_eq!(a.change_on(n - 1, ops).await.seq(), n);
    }
    a.close().await;
    let before = content(&server, &asm, &world.admin).await;
    let server = server.restart().await;
    let (_, got) = server
        .get(&format!("/api/documents/{asm}/content"), Some(&world.admin))
        .await;
    assert_eq!(got["seq"], 5);
    assert_eq!(got["content"], before);
    let mut expected: Vec<&str> = added.iter().map(String::as_str).collect();
    expected.push("c1");
    expected.sort();
    assert_eq!(keys(&refs(&server, &asm, &world.admin).await), expected);
    table_matches(&server, &asm, &world.admin).await;
    let mut live = Live::connect(&server, &asm, &world.admin).await;
    assert_eq!(
        live.change(vec![remove(&["components"], &added[1])], vec![], vec![])
            .await
            .seq(),
        6
    );
    assert_eq!(refs(&server, &asm, &world.admin).await.len(), 3);
    table_matches(&server, &asm, &world.admin).await;
}

#[tokio::test]
async fn parts_have_no_components() {
    let (server, world) = world().await;
    let part = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &part, &world.admin).await;
    let id = a.new_id();
    for op in [
        add_component(json!({"id": id, "part": {"big": "copy"}})),
        add_mate(json!({"id": id, "type": "coincident"})),
        set(&["components"], json!([])),
    ] {
        let outcome = a.change(vec![op], vec![], vec![]).await;
        assert_eq!(outcome.code(), RejectCode::Invalid);
        assert!(rejected_message(&outcome).contains("only assemblies"));
    }
    assert_eq!(a.change(vec![set_depth(3)], vec![], vec![]).await.seq(), 1);
}

#[tokio::test]
async fn working_in_an_assembly_whose_parts_you_cannot_open() {
    let (server, world) = world().await;
    let secret = project(&server, &world, "Secret").await;
    let hidden = part_in(&server, &world.admin, &secret, "SecretGearbox").await;
    let other_hidden = part_in(&server, &world.admin, &secret, "Other").await;
    let (bob, _) = bob(&server, &world).await;
    let shared = world.document(&server, None, None, None).await;
    let asm = make_ok(
        &server,
        &world,
        assembly(
            vec![
                component("c1", &hidden, 0, None),
                component("c2", &hidden, 0, None),
                component("c3", &shared, 0, None),
            ],
            vec![],
        ),
        "Rover",
    )
    .await;
    let mut b = Live::connect(&server, &asm, &bob).await;
    // Moving, renaming and removing a component are fine ...
    assert_eq!(
        b.change(vec![move_component("c1", 7.0)], vec![], vec![])
            .await
            .seq(),
        1
    );
    assert_eq!(
        b.change(
            vec![set(&["components", "c1", "name"], json!("Gears"))],
            vec![],
            vec![]
        )
        .await
        .seq(),
        2
    );
    let removal = b.make_change(b.seq, vec![remove(&["components"], "c2")]);
    b.send_change(&removal).await;
    assert_eq!(b.outcome(&removal.change_id).await.seq(), 3);
    // ... and so is taking the removal back, and restoring.
    let back = vec![add_component(component("c2", &hidden, 0, None))];
    assert_eq!(b.undo(&removal.change_id, back).await.seq(), 4);
    assert_eq!(restore(&server, &asm, &bob, 0).await.0, 200);
    b.drain().await;
    // What he cannot do is choose that part: a new component of it, or pointing another at it.
    let c9 = b.new_id();
    let base = b.seq;
    let attempts = [
        vec![add_component(component(&c9, &hidden, 0, None))],
        vec![set(&["components", "c3", "ref", "document"], json!(hidden))],
        // ... or choose another state of it, which would also say how long its history is.
        set_ref("c1", 1, None),
        set_ref("c1", 999, None),
        vec![repin("c1", &hidden, 1)],
        vec![
            remove(&["components"], "c1"),
            add_component(component("c1", &hidden, 1, None)),
        ],
    ];
    for ops in attempts {
        let outcome = b.change_on(base, ops).await;
        assert_eq!(outcome.code(), RejectCode::Invalid);
        let message = rejected_message(&outcome);
        assert!(message.contains("there is no part"), "{message}");
        assert!(!message.contains("SecretGearbox"));
    }
    // An undo sends its own operations: one that would bring the component back pointing at
    // another part than the undone change took away is a choice like any other.
    let removal = b.make_change(b.seq, vec![remove(&["components"], "c2")]);
    b.send_change(&removal).await;
    b.outcome(&removal.change_id).await.seq();
    let smuggled = vec![add_component(component("c2", &other_hidden, 0, None))];
    assert_eq!(
        b.undo(&removal.change_id, smuggled).await.code(),
        RejectCode::Invalid
    );
    let back = vec![add_component(component("c2", &hidden, 0, None))];
    b.undo(&removal.change_id, back).await.seq();
    assert_eq!(keys(&refs(&server, &asm, &bob).await), ["c1", "c2", "c3"]);
    table_matches(&server, &asm, &world.admin).await;
    // A new version of the secret part is announced without its name.
    let mut owner = Live::connect(&server, &hidden, &world.admin).await;
    assert_eq!(
        owner
            .change(
                vec![set(&["material"], json!({"name": "Steel"}))],
                vec![],
                vec![]
            )
            .await
            .seq(),
        1
    );
    version(&server, &hidden, &world.admin, "Classified Rev 2").await;
    let ev = loop {
        match b.recv().await {
            m @ ServerMsg::ReferenceUpdate { .. } => break m,
            _ => continue,
        }
    };
    let text = format!("{ev:?}");
    assert!(!text.contains("SecretGearbox") && !text.contains("Classified"));
    assert_eq!(
        ev,
        ServerMsg::ReferenceUpdate {
            document: hidden.clone(),
            components: vec!["c1".into(), "c2".into()],
        }
    );
    let shown = format!("{:?}", refs(&server, &asm, &bob).await);
    assert!(!shown.contains("SecretGearbox") && !shown.contains("Classified"));
}

#[tokio::test]
async fn a_project_cannot_be_deleted_from_under_other_projects_assemblies() {
    let (server, world) = world().await;
    let library = project(&server, &world, "Library").await;
    let bolt = part_in(&server, &world.admin, &library, "Bolt").await;
    let content = assembly(vec![component("c1", &bolt, 0, None)], vec![]);
    let (_, inside) = make(&server, content.clone(), "Own", &world.admin, &library).await;
    let inside = inside["id"].as_str().unwrap().to_owned();
    let rover = make_ok(&server, &world, content, "Rover").await;
    let (status, body) = delete(&server, &format!("/api/projects/{library}"), &world.admin).await;
    assert_eq!(status, 409);
    assert_eq!(
        body["detail"],
        json!({"message": "assemblies in other projects use parts of this project",
               "assemblies": [{"id": rover, "name": "Rover"}],
               "in_projects_you_cannot_open": false})
    );
    // Rolled back: the project and its documents are still there.
    let (status, _) = server
        .get(&format!("/api/documents/{bolt}"), Some(&world.admin))
        .await;
    assert_eq!(status, 200);
    let (status, _) = delete(&server, &format!("/api/documents/{rover}"), &world.admin).await;
    assert_eq!(status, 204);
    // Its own assemblies go with it.
    let (status, _) = delete(&server, &format!("/api/projects/{library}"), &world.admin).await;
    assert_eq!(status, 204);
    let (status, _) = server
        .get(&format!("/api/documents/{inside}"), Some(&world.admin))
        .await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn the_delete_guard_does_not_name_assemblies_you_cannot_open() {
    let (server, world) = world().await;
    let (carol, _) = world
        .invite(&server, "carol@example.com", Some("Carol"), None, "member")
        .await;
    let (status, mine) = server
        .post(
            &format!("/api/orgs/{}/projects", world.org),
            Some(&carol),
            json!({"name": "Carols"}),
        )
        .await;
    assert_eq!(status, 201);
    let part = part_in(&server, &carol, mine["id"].as_str().unwrap(), "Hinge").await;
    // The organisation owner (who can open everything) builds on it where Carol cannot look.
    make_ok(
        &server,
        &world,
        assembly(vec![component("c1", &part, 0, None)], vec![]),
        "TopSecretRover",
    )
    .await;
    let (status, body) = delete(&server, &format!("/api/documents/{part}"), &carol).await;
    assert_eq!(status, 409);
    assert!(!body.to_string().contains("TopSecretRover"));
    assert_eq!(body["detail"]["assemblies"], json!([]));
    assert_eq!(body["detail"]["in_projects_you_cannot_open"], true);
    // The same for her project.
    let (status, body) = delete(
        &server,
        &format!("/api/projects/{}", mine["id"].as_str().unwrap()),
        &carol,
    )
    .await;
    assert_eq!(status, 409);
    assert!(!body.to_string().contains("TopSecretRover"));
    assert_eq!(body["detail"]["in_projects_you_cannot_open"], true);
}

#[tokio::test]
async fn undo_and_restore_still_work_after_a_part_they_bring_back_was_deleted() {
    let (server, world) = world().await;
    let keep = world.document(&server, None, Some("Keep"), None).await;
    let doomed = world.document(&server, None, Some("Doomed"), None).await;
    let asm = make_ok(
        &server,
        &world,
        assembly(
            vec![
                component("c1", &keep, 0, None),
                component("c2", &doomed, 0, None),
            ],
            vec![],
        ),
        "Rover",
    )
    .await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    assert_eq!(
        a.change(vec![remove(&["components"], "c2")], vec![], vec![])
            .await
            .seq(),
        1
    );
    // Nothing uses it now.
    let (status, _) = delete(&server, &format!("/api/documents/{doomed}"), &world.admin).await;
    assert_eq!(status, 204);
    assert_eq!(
        a.change(vec![move_component("c1", 3.0)], vec![], vec![])
            .await
            .seq(),
        2
    );
    // The whole restore is not held hostage by the one component whose part is gone.
    assert_eq!(
        restore(&server, &asm, &world.admin, 0).await,
        (200, json!({"seq": 3}))
    );
    a.drain().await;
    let got = refs(&server, &asm, &world.admin).await;
    assert_eq!(keys(&got), ["c1", "c2"]);
    assert_eq!(got["c2"]["document"]["deleted"], true);
    table_matches(&server, &asm, &world.admin).await;
    // It can still be moved, as long as it keeps pointing where it did ...
    assert_eq!(
        a.change(vec![move_component("c2", 5.0)], vec![], vec![])
            .await
            .seq(),
        4
    );
    // ... and relabelled: same part, same state.
    assert_eq!(
        a.change(set_ref("c2", 0, Some("relabelled")), vec![], vec![])
            .await
            .seq(),
        5
    );
    // But nobody can newly choose a deleted part, for a new component or by repointing one.
    let c9 = a.new_id();
    assert_eq!(
        a.change(
            vec![add_component(component(&c9, &doomed, 0, None))],
            vec![],
            vec![]
        )
        .await
        .code(),
        RejectCode::Invalid
    );
    assert_eq!(
        a.change(
            vec![set(&["components", "c1", "ref", "document"], json!(doomed))],
            vec![],
            vec![]
        )
        .await
        .code(),
        RejectCode::Invalid
    );
}

// ------------------------------------------------------------------ v2: reference checks

#[tokio::test]
async fn every_part_one_cannot_use_is_refused_with_the_same_message() {
    let (server, world) = world().await;
    let (bob, _) = bob(&server, &world).await;
    let usable = world.document(&server, None, Some("Usable"), None).await;
    let secret = project(&server, &world, "Secret").await;
    let unseen = part_in(&server, &world.admin, &secret, "Unseen").await;
    let gone = world.document(&server, None, Some("Gone"), None).await;
    let (status, _) = delete(&server, &format!("/api/documents/{gone}"), &world.admin).await;
    assert_eq!(status, 204);
    let (_, other_org) = server
        .post("/api/orgs", Some(&world.admin), json!({"name": "Other"}))
        .await;
    let (_, other_project) = server
        .post(
            &format!("/api/orgs/{}/projects", other_org["id"].as_str().unwrap()),
            Some(&world.admin),
            json!({"name": "X"}),
        )
        .await;
    let foreign = part_in(
        &server,
        &world.admin,
        other_project["id"].as_str().unwrap(),
        "Foreign",
    )
    .await;
    let asm = make_ok(&server, &world, assembly(vec![], vec![]), "Rover").await;
    let other_asm = make_ok(&server, &world, assembly(vec![], vec![]), "Arm").await;
    let mut b = Live::connect(&server, &asm, &bob).await;
    let id = b.new_id();
    let try_part = |part: &str, seq: i64| vec![add_component(component(&id, part, seq, None))];
    let mut messages = Vec::new();
    for (part, seq) in [
        (foreign.as_str(), 0),
        (gone.as_str(), 0),
        (other_asm.as_str(), 0),
        (asm.as_str(), 0),
        ("0123456789abcdef0123456789abcdef", 0),
        (unseen.as_str(), 0),
        (usable.as_str(), 1),
        (usable.as_str(), i64::MAX),
    ] {
        let outcome = b.change_on(0, try_part(part, seq)).await;
        assert_eq!(outcome.code(), RejectCode::Invalid, "{part}@{seq}");
        messages.push(rejected_message(&outcome).to_owned());
    }
    assert!(messages.iter().all(|m| *m == no_part(&id)), "{messages:?}");
    // The one it can use, at a state it has, goes in; a version label is a label (not checked
    // against the part's versions, as in the Python server).
    assert_eq!(b.change_on(0, try_part(&usable, 0)).await.seq(), 1);
    assert_eq!(
        b.change(set_ref(&id, 0, Some("no such version")), vec![], vec![])
            .await
            .seq(),
        2
    );
    table_matches(&server, &asm, &bob).await;
}

#[tokio::test]
async fn hostile_references_are_refused() {
    let (server, world) = world().await;
    let part = world.document(&server, None, None, None).await;
    let asm = make_ok(
        &server,
        &world,
        assembly(vec![component("c1", &part, 0, None)], vec![]),
        "Rover",
    )
    .await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    for bad in [
        json!("a string"),
        json!([part, 0]),
        json!({"document": 5, "seq": 0}),
        json!({"document": part, "seq": -1}),
        json!({"document": part, "seq": 0.5}),
        json!({"document": part}),
        json!({"document": part, "seq": 0, "version": 3}),
        json!({"document": part, "seq": 0, "version": "v".repeat(201)}),
        json!({"document": "p".repeat(500), "seq": 0}),
    ] {
        let set_ref = set(&["components", "c1", "ref"], bad.clone());
        assert_eq!(
            a.change(vec![set_ref], vec![], vec![]).await.code(),
            RejectCode::Invalid,
            "{bad}"
        );
        let id = a.new_id();
        let mut item = component(&id, &part, 0, None);
        item["ref"] = bad.clone();
        assert_eq!(
            a.change(vec![add_component(item)], vec![], vec![])
                .await
                .code(),
            RejectCode::Invalid,
            "{bad}"
        );
    }
    // Replacing the whole list with something that is not one.
    assert_eq!(
        a.change(vec![set(&["components"], json!("nope"))], vec![], vec![])
            .await
            .code(),
        RejectCode::Invalid
    );
    // A component referring to the assembly itself.
    let id = a.new_id();
    let outcome = a
        .change(
            vec![add_component(component(&id, &asm, 0, None))],
            vec![],
            vec![],
        )
        .await;
    assert_eq!(rejected_message(&outcome), no_part(&id));
    // Ten thousand components choosing as many parts: refused, without asking the store about
    // each.
    let ops: Vec<Op> = (0..10_000)
        .map(|n| {
            let id = a.new_id();
            add_component(component(&id, &format!("{n:032x}"), 0, None))
        })
        .collect();
    let outcome = a.change(ops, vec![], vec![]).await;
    assert_eq!(outcome.code(), RejectCode::Invalid);
    assert!(rejected_message(&outcome).contains("at most 1024"));
    // The assembly is as it was.
    assert_eq!(table(&server, &asm).await.len(), 1);
    assert_eq!(
        a.change(vec![move_component("c1", 1.0)], vec![], vec![])
            .await
            .seq(),
        1
    );
    // The same at creation.
    let many: Vec<Json> = (0..1025)
        .map(|n| component(&format!("c{n}"), &format!("{n:032x}"), 0, None))
        .collect();
    let (status, body) = make(
        &server,
        assembly(many, vec![]),
        "Many",
        &world.admin,
        &world.project,
    )
    .await;
    assert_eq!(status, 422, "{body}");
}

// ------------------------------------------------------------------ v2: the table

#[tokio::test]
async fn the_table_follows_the_document_and_is_written_only_when_it_changes() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let part = world.document(&server, None, Some("P"), None).await;
    let other = world.document(&server, None, Some("Q"), None).await;
    let mut p = Live::connect(&server, &part, &world.admin).await;
    assert_eq!(p.change(vec![set_depth(11)], vec![], vec![]).await.seq(), 1);
    let writes0 = flaky.ref_writes();
    let asm = make_ok(
        &server,
        &world,
        assembly(vec![component("c1", &part, 0, None)], vec![]),
        "Rover",
    )
    .await;
    assert_eq!(flaky.ref_writes(), writes0 + 1, "written with the document");
    table_matches(&server, &asm, &world.admin).await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    let step = |expected_writes: usize| {
        let (server, flaky, asm, token) = (&server, &flaky, &asm, world.admin.clone());
        async move {
            table_matches(server, asm, &token).await;
            assert_eq!(flaky.ref_writes(), expected_writes);
        }
    };
    let w = flaky.ref_writes();
    // Moving, renaming, relabelling: nothing to write.
    a.change(vec![move_component("c1", 1.0)], vec![], vec![])
        .await
        .seq();
    a.change(
        vec![set(&["components", "c1", "name"], json!("N"))],
        vec![],
        vec![],
    )
    .await
    .seq();
    a.change(set_ref("c1", 0, Some("label")), vec![], vec![])
        .await
        .seq();
    step(w).await;
    // Insert, re-pin, re-point, remove: written each time.
    let c2 = a.new_id();
    a.change(
        vec![add_component(component(&c2, &part, 0, None))],
        vec![],
        vec![],
    )
    .await
    .seq();
    step(w + 1).await;
    a.change(set_ref("c1", 1, None), vec![], vec![]).await.seq();
    step(w + 2).await;
    let repinned = a
        .change(vec![repin(&c2, &other, 0)], vec![], vec![])
        .await
        .seq();
    step(w + 3).await;
    let removal = a.make_change(a.seq, vec![remove(&["components"], "c1")]);
    a.send_change(&removal).await;
    a.outcome(&removal.change_id).await.seq();
    step(w + 4).await;
    // Undo, revert from history, restore.
    let back = vec![add_component(component("c1", &part, 1, None))];
    a.undo(&removal.change_id, back).await.seq();
    step(w + 5).await;
    assert!(a.revert(repinned).await.ok);
    step(w + 6).await;
    let (status, _) = restore(&server, &asm, &world.admin, 0).await;
    assert_eq!(status, 200);
    step(w + 7).await;
    assert_eq!(
        table(&server, &asm).await,
        vec![("c1".to_owned(), part.clone(), 0)]
    );
    // A restore that changes nothing writes nothing.
    let (status, _) = restore(&server, &asm, &world.admin, 0).await;
    assert_eq!(status, 200);
    step(w + 7).await;
}

#[tokio::test]
async fn a_restore_that_would_repin_to_a_part_deleted_since_is_refused() {
    let (server, world) = world().await;
    let first = world.document(&server, None, Some("First"), None).await;
    let second = world.document(&server, None, Some("Second"), None).await;
    let asm = make_ok(
        &server,
        &world,
        assembly(vec![component("c1", &first, 0, None)], vec![]),
        "Rover",
    )
    .await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    assert_eq!(
        a.change(vec![repin("c1", &second, 0)], vec![], vec![])
            .await
            .seq(),
        1
    );
    let (status, _) = delete(&server, &format!("/api/documents/{first}"), &world.admin).await;
    assert_eq!(status, 204);
    let (status, body) = restore(&server, &asm, &world.admin, 0).await;
    assert_eq!(status, 409);
    assert_eq!(
        body["detail"],
        json!({"code": "invalid", "message": no_part("c1"), "conflicts": []})
    );
    // Undoing the re-pin from history is refused the same way.
    let result = a.revert(1).await;
    assert_eq!(result.code.as_deref(), Some("invalid"));
    assert_eq!(result.message.as_deref(), Some(no_part("c1").as_str()));
    assert_eq!(
        table(&server, &asm).await,
        vec![("c1".to_owned(), second.clone(), 0)]
    );
}

#[tokio::test]
async fn a_store_that_cannot_check_the_parts_is_a_server_error() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let part = world.document(&server, None, None, None).await;
    let asm = make_ok(&server, &world, assembly(vec![], vec![]), "Rover").await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    flaky.fail_part_reads(true);
    let id = a.new_id();
    let add = vec![add_component(component(&id, &part, 0, None))];
    let outcome = a.change(add.clone(), vec![], vec![]).await;
    assert_eq!(outcome.code(), RejectCode::ServerError);
    // Creating an assembly fails the same way; once the store answers, the change goes in.
    let (status, _) = make(
        &server,
        assembly(vec![component("c1", &part, 0, None)], vec![]),
        "Arm",
        &world.admin,
        &world.project,
    )
    .await;
    assert_eq!(status, 500);
    flaky.fail_part_reads(false);
    assert_eq!(a.change(add, vec![], vec![]).await.seq(), 1);
    table_matches(&server, &asm, &world.admin).await;
}

// ------------------------------------------------------------------ v2: races

#[tokio::test]
async fn a_part_deleted_after_the_check_refuses_the_change_and_spares_the_ones_after_it() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, _) = bob(&server, &world).await;
    let doomed = world.document(&server, None, Some("Doomed"), None).await;
    let keep = world.document(&server, None, Some("Keep"), None).await;
    let asm = make_ok(
        &server,
        &world,
        assembly(vec![component("c1", &keep, 0, None)], vec![]),
        "Rover",
    )
    .await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    let mut b = Live::connect(&server, &asm, &bob).await;
    // Ada's change is checked; the answer waits while the part is deleted.
    flaky.parts_gate.shut();
    let id = a.new_id();
    let change = a.make_change(0, vec![add_component(component(&id, &doomed, 0, None))]);
    a.send_change(&change).await;
    flaky.parts_gate.until_waiting(1).await;
    let (status, _) = delete(&server, &format!("/api/documents/{doomed}"), &world.admin).await;
    assert_eq!(status, 204, "nothing used it yet");
    // Her record reaches the journal, which holds it; Bob's change is decided after it.
    flaky.commit_gate.shut();
    flaky.parts_gate.open();
    flaky.commit_gate.until_waiting(1).await;
    let move_c1 = b.make_change(0, vec![move_component("c1", 4.0)]);
    b.send_change(&move_c1).await;
    let deadline = tokio::time::Instant::now() + WAIT;
    while server.room_status(&asm).await.tentative < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Bob's change was not decided"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    flaky.commit_gate.open();
    let outcome = a.outcome(&change.change_id).await;
    assert_eq!(outcome.code(), RejectCode::Invalid);
    assert_eq!(rejected_message(&outcome), no_part(&id));
    // Bob's is judged again without it, and goes in.
    assert_eq!(b.outcome(&move_c1.change_id).await.seq(), 1);
    table_matches(&server, &asm, &world.admin).await;
    assert_eq!(table(&server, &asm).await.len(), 1);
}

#[tokio::test]
async fn a_reference_committed_before_a_delete_stops_the_delete() {
    let (server, world) = world().await;
    let part = world.document(&server, None, Some("Hinge"), None).await;
    let asm = make_ok(&server, &world, assembly(vec![], vec![]), "Rover").await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    let id = a.new_id();
    assert_eq!(
        a.change(
            vec![add_component(component(&id, &part, 0, None))],
            vec![],
            vec![]
        )
        .await
        .seq(),
        1
    );
    let (status, body) = delete(&server, &format!("/api/documents/{part}"), &world.admin).await;
    assert_eq!(status, 409);
    assert_eq!(
        body["detail"]["assemblies"],
        json!([{"id": asm, "name": "Rover"}])
    );
    let (status, _) = server
        .get(&format!("/api/documents/{part}"), Some(&world.admin))
        .await;
    assert_eq!(status, 200);
}

// ------------------------------------------------------------------ v2: reference.update

#[tokio::test]
async fn reference_update_reaches_only_open_assemblies_pinned_to_an_older_state() {
    let (server, world) = world().await;
    let part = world.document(&server, None, None, None).await;
    let mut p = Live::connect(&server, &part, &world.admin).await;
    assert_eq!(p.change(vec![set_depth(12)], vec![], vec![]).await.seq(), 1);
    let older = make_ok(
        &server,
        &world,
        assembly(
            vec![
                component("c1", &part, 0, None),
                component("c2", &part, 1, None),
            ],
            vec![],
        ),
        "Older",
    )
    .await;
    let newest = make_ok(
        &server,
        &world,
        assembly(vec![component("c1", &part, 1, None)], vec![]),
        "Newest",
    )
    .await;
    let closed = make_ok(
        &server,
        &world,
        assembly(vec![component("c1", &part, 0, None)], vec![]),
        "Closed",
    )
    .await;
    let mut o = Live::connect(&server, &older, &world.admin).await;
    let mut n = Live::connect(&server, &newest, &world.admin).await;
    version(&server, &part, &world.admin, "Rev 1").await;
    let ev = loop {
        match o.recv().await {
            m @ ServerMsg::ReferenceUpdate { .. } => break m,
            _ => continue,
        }
    };
    assert_eq!(
        ev,
        ServerMsg::ReferenceUpdate {
            document: part.clone(),
            components: vec!["c1".into()],
        }
    );
    // The part's own room hears `version.created` after every `reference.update` was sent: the
    // announcing step is done by then.
    let created: VersionCreated = p.expect().await;
    assert_eq!(created.name, "Rev 1");
    let heard = n.drain().await;
    assert!(
        !heard
            .iter()
            .any(|m| matches!(m, ServerMsg::ReferenceUpdate { .. })),
        "{heard:?}"
    );
    assert!(
        server.state.hub.loaded(&closed).is_none(),
        "not opened for it"
    );
}

// ------------------------------------------------------------------ v2: bounded reads

#[tokio::test]
async fn references_and_used_in_read_the_store_a_bounded_number_of_times() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    // 300 parts at state 3, each with a version there (rows written directly: only their rows
    // matter here).
    let parts: Vec<String> = (0..300).map(|n| format!("{n:032x}")).collect();
    let docs: Vec<String> = parts
        .iter()
        .map(|id| {
            format!(
                "('{id}', '{}', 'P{id}', 'part', 3, '{}', 0, 0)",
                world.project, world.admin_id
            )
        })
        .collect();
    server
        .sql(&format!(
            "INSERT INTO documents (id, project_id, name, kind, head_seq, created_by, created_at, \
             updated_at) VALUES {}",
            docs.join(", ")
        ))
        .await;
    let versions: Vec<String> = parts
        .iter()
        .map(|id| {
            format!(
                "('v{}', '{id}', 3, 'Rev 3', '', '{}', 1)",
                &id[1..],
                world.admin_id
            )
        })
        .collect();
    server
        .sql(&format!(
            "INSERT INTO versions (id, document_id, seq, name, note, author_id, created_at) \
             VALUES {}",
            versions.join(", ")
        ))
        .await;
    // 600 components over them.
    let components: Vec<Json> = (0..600)
        .map(|n| component(&format!("c{n:03}"), &parts[n % 300], 0, None))
        .collect();
    let asm = make_ok(&server, &world, assembly(components, vec![]), "Big").await;
    let before = flaky.store_calls();
    let got = refs(&server, &asm, &world.admin).await;
    let reads = flaky.store_calls() - before;
    assert_eq!(got.len(), 600);
    assert_eq!(got["c000"]["update_available"], true);
    assert_eq!(got["c000"]["latest_version"]["name"], "Rev 3");
    assert_eq!(got["c599"]["changes_since_pinned"], 3);
    assert!(reads <= 12, "{reads} store calls for 600 components");
    // Thirty assemblies use one part: one read for all their references.
    for n in 0..30 {
        make_ok(
            &server,
            &world,
            assembly(vec![component("k1", &parts[0], 0, None)], vec![]),
            &format!("A{n:02}"),
        )
        .await;
    }
    let before = flaky.store_calls();
    let (status, used) = server
        .get(
            &format!("/api/documents/{}/used-in", parts[0]),
            Some(&world.admin),
        )
        .await;
    let reads = flaky.store_calls() - before;
    assert_eq!(status, 200);
    assert_eq!(used.as_array().unwrap().len(), 31);
    assert_eq!(
        used[0]["components"],
        json!([{"component": "k1", "seq": 0}])
    );
    assert_eq!(used[30]["document"]["name"], "Big");
    assert_eq!(used[30]["components"].as_array().unwrap().len(), 2);
    assert!(reads <= 12, "{reads} store calls for 31 assemblies");
}

// ------------------------------------------------------------------ v2: connections

#[tokio::test]
async fn a_revocation_while_the_parts_are_checked_writes_nothing() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = bob(&server, &world).await;
    let part = world.document(&server, None, None, None).await;
    let asm = make_ok(&server, &world, assembly(vec![], vec![]), "Rover").await;
    let mut b = Live::connect(&server, &asm, &bob).await;
    flaky.parts_gate.shut();
    let id = b.new_id();
    let change = b.make_change(0, vec![add_component(component(&id, &part, 0, None))]);
    b.send_change(&change).await;
    flaky.parts_gate.until_waiting(1).await;
    server.state.hub.drop_account(&bob_id).await;
    assert_eq!(b.closed().await.0, 4401);
    flaky.parts_gate.open();
    // The check comes back to a room that no longer works for him: nothing is written.
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    a.drain().await;
    assert_eq!(
        a.change(vec![set(&["note"], json!("x"))], vec![], vec![])
            .await
            .seq(),
        1
    );
    let (_, got) = server
        .get(&format!("/api/documents/{asm}/content"), Some(&world.admin))
        .await;
    assert_eq!(got["content"]["components"], json!([]));
    assert!(table(&server, &asm).await.is_empty());
}

// ------------------------------------------------------------------ v2: the re-judge corner

#[tokio::test]
async fn a_change_a_rejudge_cannot_check_is_answered_server_error_and_can_be_sent_again() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let first = world.document(&server, None, Some("First"), None).await;
    let second = world.document(&server, None, Some("Second"), None).await;
    for part in [&first, &second] {
        let mut p = Live::connect(&server, part, &world.admin).await;
        assert_eq!(p.change(vec![set_depth(12)], vec![], vec![]).await.seq(), 1);
    }
    let asm = make_ok(
        &server,
        &world,
        assembly(vec![component("c1", &first, 0, None)], vec![]),
        "Rover",
    )
    .await;
    let mut a = Live::connect(&server, &asm, &world.admin).await;
    // One change points c1 at Second (checked: Second at 0); the next sets only its seq (checked:
    // Second at 1, on the head the room judged it on).
    let repoint = a.make_change(0, vec![repin("c1", &second, 0)]);
    let seq_only = a.make_change(0, vec![set(&["components", "c1", "ref", "seq"], json!(1))]);
    // ... but the journal refuses the first: judged again without it, the second would pin First
    // at 1, which it was never checked for.
    // (The journal is held until both are tentative: the refusal comes with the second handed
    // over already.)
    flaky.refuse_change_once(&repoint.change_id);
    flaky.journal_gate.shut();
    a.send_change(&repoint).await;
    a.send_change(&seq_only).await;
    let deadline = std::time::Instant::now() + WAIT;
    while server.room_status(&asm).await.tentative < 2 {
        assert!(std::time::Instant::now() < deadline, "both are tentative");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    tokio::time::timeout(WAIT, flaky.journal_gate.until_waiting(1))
        .await
        .expect("the journal is held");
    flaky.journal_gate.open();
    a.outcome(&repoint.change_id).await;
    assert_eq!(
        a.outcome(&seq_only.change_id).await.code(),
        RejectCode::ServerError
    );
    // A change depending on it, sent now, is refused: the room remembers the id as refused.
    let mut dependent = a.make_change(0, vec![move_component("c1", 2.0)]);
    dependent.depends = vec![seq_only.change_id.clone()];
    a.send_change(&dependent).await;
    assert_eq!(
        a.outcome(&dependent.change_id).await.code(),
        RejectCode::Dependency
    );
    // Sent again, the same change is judged afresh (and checked: First has state 1).
    assert_eq!(a.resend(&seq_only).await.seq(), 1);
    // Once accepted, what depends on it goes in.
    let mut after = a.make_change(1, vec![move_component("c1", 3.0)]);
    after.depends = vec![seq_only.change_id.clone()];
    a.send_change(&after).await;
    assert_eq!(a.outcome(&after.change_id).await.seq(), 2);
    assert_eq!(
        table(&server, &asm).await,
        vec![("c1".to_owned(), first.clone(), 1)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_demoted_client_reconnecting_inherits_nothing_whose_parts_are_checked() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = bob(&server, &world).await;
    let part = world.document(&server, None, None, None).await;
    let asm = make_ok(&server, &world, assembly(vec![], vec![]), "Rover").await;
    let desk = Hello::client("demoted-parts");
    let mut a = Live::connect_with(&server, &asm, &bob, desk.clone()).await;
    flaky.parts_gate.shut();
    let id = a.new_id();
    let change = a.make_change(0, vec![add_component(component(&id, &part, 0, None))]);
    a.send_change(&change).await;
    flaky.parts_gate.until_waiting(1).await;
    // Bob is made a viewer; his client reconnects before the hub has closed the old socket.
    let gate = server.state.room_hooks.after_commit.clone();
    gate.shut();
    let demoting = {
        let url = server.url(&format!("/api/projects/{}/members/{bob_id}", world.project));
        let token = world.admin.clone();
        tokio::spawn(async move {
            reqwest::Client::new()
                .put(url)
                .bearer_auth(token)
                .json(&json!({"role": "viewer"}))
                .send()
                .await
                .unwrap()
                .status()
        })
    };
    gate.until_waiting(1).await;
    let mut b = Live::connect_with(&server, &asm, &bob, desk).await;
    gate.open();
    assert_eq!(demoting.await.unwrap(), 200);
    flaky.parts_gate.open();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let (_, got) = server
        .get(&format!("/api/documents/{asm}/content"), Some(&world.admin))
        .await;
    assert_eq!(got["seq"], 0, "nothing written after the demotion");
    assert_eq!(got["content"]["components"], json!([]));
    assert_eq!(b.resend(&change).await.code(), RejectCode::ReadOnly);
    assert!(table(&server, &asm).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_while_the_parts_are_checked_is_answered_once_when_resent() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let part = world.document(&server, None, None, None).await;
    let asm = make_ok(&server, &world, assembly(vec![], vec![]), "Rover").await;
    let desk = Hello::client("assembly-desk");
    let mut a = Live::connect_with(&server, &asm, &world.admin, desk.clone()).await;
    flaky.parts_gate.shut();
    let id = a.new_id();
    let change = a.make_change(0, vec![add_component(component(&id, &part, 0, None))]);
    a.send_change(&change).await;
    flaky.parts_gate.until_waiting(1).await;
    // The same client connects again: the old socket is replaced and its copy is dropped when
    // the check comes back; the new one resends and is answered once.
    let mut b = Live::connect_with(&server, &asm, &world.admin, desk).await;
    assert_eq!(a.closed().await.0, 4409);
    b.send_change(&change).await;
    flaky.parts_gate.open();
    assert_eq!(b.outcome(&change.change_id).await.seq(), 1);
    assert!(
        b.try_recv(std::time::Duration::from_millis(500))
            .await
            .is_none_or(|m| !matches!(m, ServerMsg::Accepted { .. } | ServerMsg::Rejected { .. })),
        "one answer"
    );
    table_matches(&server, &asm, &world.admin).await;
    assert_eq!(table(&server, &asm).await.len(), 1);
    let (_, got) = server
        .get(&format!("/api/documents/{asm}/content"), Some(&world.admin))
        .await;
    assert_eq!(got["seq"], 1);
}

// ------------------------------------------------------------------ Task 16: the lock order

/// A change that makes an assembly use a part locks two document rows (the part's, and the
/// assembly's own for its head); deleting their project locks the same two. Both take them in
/// ascending id order, whichever of the two documents has the lower id, so neither waits for the
/// other in a cycle (which PostgreSQL would end by failing one of them).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_project_deletion_and_an_assembly_change_never_wait_for_each_other() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let server = std::sync::Arc::new(server);
    // Both orders of the two ids: the assembly's row below the part's, and above it.
    let mut below = false;
    let mut above = false;
    let mut round = 0;
    while !(below && above) {
        round += 1;
        assert!(
            round <= 40,
            "forty pairs of random ids were all ordered one way"
        );
        let project = project(&server, &world, &format!("P{round}")).await;
        let (status, body) = make(
            &server,
            assembly(vec![], vec![]),
            "Rover",
            &world.admin,
            &project,
        )
        .await;
        assert_eq!(status, 201, "{body}");
        let asm = body["id"].as_str().unwrap().to_owned();
        let part = part_in(&server, &world.admin, &project, "Hinge").await;
        if (asm < part && below) || (asm > part && above) {
            continue;
        }
        below |= asm < part;
        above |= asm > part;

        let mut a = Live::connect(&server, &asm, &world.admin).await;
        // The journal has locked what its batch needs and written it; before its commit ...
        flaky.head_gate.shut();
        let id = a.new_id();
        let change = a.make_change(0, vec![add_component(component(&id, &part, 0, None))]);
        a.send_change(&change).await;
        flaky.head_gate.until_waiting(1).await;
        // ... the project's deletion starts, and has to wait for it.
        let deleting = {
            let (server, admin, path) = (
                server.clone(),
                world.admin.clone(),
                format!("/api/projects/{project}"),
            );
            tokio::spawn(async move { delete(&server, &path, &admin).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert!(
            !deleting.is_finished(),
            "the deletion waits for the journal"
        );
        flaky.head_gate.open();
        // Both end well: the change was written, then the project went.
        assert_eq!(a.outcome(&change.change_id).await.seq(), 1, "round {round}");
        let (status, body) = tokio::time::timeout(WAIT, deleting)
            .await
            .expect("the deletion ends")
            .unwrap();
        assert_eq!(status, 204, "round {round}: {body}");
    }
}
