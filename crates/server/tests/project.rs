//! Projects and project roles (the project parts of the Python `test_accounts.py`; its document
//! assertions come with documents in Task 7), the quota hook, and the hostile cases.
//!
//! (The Python `test_project.py` is about the repository, not about projects: versions, licences,
//! issue labels. It is not ported here.)

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use common::{TestServer, Token, World, world};
use oswx_server::ext::{Extensions, Quota};
use oswx_server::store::{Id, Store};
use reqwest::StatusCode;
use serde_json::{Value, json};

async fn new_project(
    server: &TestServer,
    w: &World,
    token: &str,
    name: &str,
) -> (StatusCode, Value) {
    server
        .post(
            &format!("/api/orgs/{}/projects", w.org),
            Some(token),
            json!({ "name": name }),
        )
        .await
}

fn names_and_roles(list: &Value) -> Vec<(String, String)> {
    list.as_array()
        .unwrap()
        .iter()
        .map(|p| {
            (
                p["name"].as_str().unwrap().to_owned(),
                p["role"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

#[tokio::test]
async fn test_project_roles_gate_everything() {
    let (server, w) = world().await;
    let (viewer, viewer_id) = w
        .invite(&server, "vi@example.com", None, Some("viewer"), "member")
        .await;
    let (editor, _) = w.editor(&server, "ed@example.com").await;
    let (outsider, _) = w
        .invite(&server, "out@example.com", None, None, "member")
        .await;
    let members = format!("/api/projects/{}/members", w.project);
    // a project you are not in does not exist for you
    assert_eq!(
        server.get(&members, Some(&outsider)).await,
        (StatusCode::NOT_FOUND, json!({"detail": "no such project"}))
    );
    assert_eq!(
        server
            .get(&format!("/api/orgs/{}/projects", w.org), Some(&outsider))
            .await
            .1,
        json!([])
    );
    // viewers read; only owners manage
    assert_eq!(server.get(&members, Some(&viewer)).await.0, StatusCode::OK);
    let (s, body) = server
        .put(
            &format!("{members}/{viewer_id}"),
            Some(&editor),
            json!({"role": "owner"}),
        )
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(body["detail"], "needs the project role owner");
    // promote the viewer; organisation admins own every project
    let (s, body) = server
        .put(
            &format!("{members}/{viewer_id}"),
            Some(&w.admin),
            json!({"role": "editor"}),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, json!({"user_id": viewer_id, "role": "editor"}));
    let projects = server
        .get(&format!("/api/orgs/{}/projects", w.org), Some(&viewer))
        .await
        .1;
    assert_eq!(
        names_and_roles(&projects),
        [("Rover".into(), "editor".into())]
    );
    // someone outside the organisation cannot be added to a project
    let (s, body) = server
        .put(
            &format!("{members}/nobody"),
            Some(&w.admin),
            json!({"role": "viewer"}),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        body["detail"],
        "that person is not in the organisation; invite them first"
    );
}

#[tokio::test]
async fn any_member_starts_a_project_and_owns_it() {
    let (server, w) = world().await;
    let (bob, bob_id) = w
        .invite(&server, "bob@example.com", None, None, "member")
        .await;
    let (s, body) = new_project(&server, &w, &bob, "  Arm ").await;
    assert_eq!(s, StatusCode::CREATED);
    let arm = body["id"].as_str().unwrap().to_owned();
    assert_eq!(
        body,
        json!({"id": arm, "name": "Arm", "org_id": w.org, "role": "owner"})
    );
    let stored = server
        .state
        .store
        .project_by_id(&arm)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (stored.name.as_str(), stored.org_id.as_str()),
        ("Arm", w.org.as_str())
    );
    assert_eq!(
        server
            .state
            .store
            .project_member(&arm, &bob_id)
            .await
            .unwrap()
            .unwrap()
            .role,
        "owner"
    );
    // Bob sees his project only; the owner of the organisation sees both, as owner, by name
    let list = |t: Token| {
        let server = &server;
        let org = w.org.clone();
        async move {
            server
                .get(&format!("/api/orgs/{org}/projects"), Some(&t))
                .await
                .1
        }
    };
    assert_eq!(
        names_and_roles(&list(bob.clone()).await),
        [("Arm".into(), "owner".into())]
    );
    assert_eq!(
        names_and_roles(&list(w.admin.clone()).await),
        [
            ("Arm".into(), "owner".into()),
            ("Rover".into(), "owner".into())
        ]
    );
    // the members list shows role rows only: Ada owns Arm through the organisation, without one
    let members = server
        .get(&format!("/api/projects/{arm}/members"), Some(&w.admin))
        .await
        .1;
    assert_eq!(
        members,
        json!([{"id": bob_id, "email": "bob@example.com", "name": "bob", "role": "owner"}])
    );
}

#[tokio::test]
async fn only_project_owners_rename_or_delete_a_project() {
    let (server, w) = world().await;
    let (viewer, _) = w
        .invite(&server, "vi@example.com", None, Some("viewer"), "member")
        .await;
    let (editor, editor_id) = w.editor(&server, "ed@example.com").await;
    let path = format!("/api/projects/{}", w.project);
    for token in [&viewer, &editor] {
        assert_eq!(
            server
                .patch(&path, Some(token), json!({"name": "Mine"}))
                .await,
            (
                StatusCode::FORBIDDEN,
                json!({"detail": "needs the project role owner"})
            )
        );
        assert_eq!(
            server.delete(&path, Some(token)).await.0,
            StatusCode::FORBIDDEN
        );
    }
    // an editor made owner of the project can
    server
        .put(
            &format!("{path}/members/{editor_id}"),
            Some(&w.admin),
            json!({"role": "owner"}),
        )
        .await;
    let (s, body) = server
        .patch(&path, Some(&editor), json!({"name": " Rover II "}))
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        body,
        json!({"id": w.project, "name": "Rover II", "org_id": w.org})
    );
    assert_eq!(
        server
            .state
            .store
            .project_by_id(&w.project)
            .await
            .unwrap()
            .unwrap()
            .name,
        "Rover II"
    );
    for bad in [
        json!(""),
        json!("  "),
        json!("x".repeat(201)),
        json!(null),
        json!(1),
    ] {
        let (s, _) = server
            .patch(&path, Some(&editor), json!({ "name": bad }))
            .await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{bad}");
        let (s, _) = new_project(&server, &w, &editor, bad.as_str().unwrap_or("")).await;
        if bad.is_string() {
            assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{bad}");
        }
    }
    assert_eq!(
        server.delete(&path, Some(&editor)).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        server.delete(&path, Some(&editor)).await,
        (StatusCode::NOT_FOUND, json!({"detail": "no such project"}))
    );
}

#[tokio::test]
async fn deleting_a_project_removes_everything_in_it() {
    let (server, w) = world().await;
    let (_, bob_id) = w.editor(&server, "bob@example.com").await;
    // (documents arrive with Task 7; a row of one stands in for it)
    server
        .sql(&format!(
            "INSERT INTO documents (id, project_id, folder_id, name, kind, format_version, head_seq, \
             created_by, created_at, updated_at, deleted_at) VALUES ('{}', '{}', NULL, 'Bracket', \
             'part', 3, 0, '{}', 1.0, 1.0, NULL)",
            "d".repeat(32),
            w.project,
            w.admin_id
        ))
        .await;
    assert_eq!(
        server
            .delete(&format!("/api/projects/{}", w.project), Some(&w.admin))
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    let store = &server.state.store;
    assert!(store.project_by_id(&w.project).await.unwrap().is_none());
    assert!(
        store
            .project_member(&w.project, &bob_id)
            .await
            .unwrap()
            .is_none()
    );
    let mut tx = store.begin().await.unwrap();
    assert!(
        tx.project_document_ids(&w.project)
            .await
            .unwrap()
            .is_empty()
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        server
            .get(&format!("/api/orgs/{}/projects", w.org), Some(&w.admin))
            .await
            .1,
        json!([])
    );
    // Bob is still in the organisation
    assert!(store.org_member(&w.org, &bob_id).await.unwrap().is_some());
}

#[tokio::test]
async fn project_members_are_set_changed_and_removed() {
    let (server, w) = world().await;
    let (bob, bob_id) = w
        .invite(&server, "bob@example.com", None, None, "member")
        .await;
    let path = format!("/api/projects/{}/members/{bob_id}", w.project);
    for bad in [
        json!("admin"),
        json!("Owner"),
        json!(""),
        json!(null),
        json!(2),
    ] {
        let (s, body) = server
            .put(&path, Some(&w.admin), json!({ "role": bad }))
            .await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{bad}: {body}");
    }
    assert!(
        server
            .state
            .store
            .project_member(&w.project, &bob_id)
            .await
            .unwrap()
            .is_none()
    );
    server
        .put(&path, Some(&w.admin), json!({"role": "viewer"}))
        .await;
    server
        .put(&path, Some(&w.admin), json!({"role": "editor"}))
        .await;
    let row = server
        .state
        .store
        .project_member(&w.project, &bob_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.role, "editor");
    // a viewer or editor cannot take anyone's role away
    assert_eq!(
        server
            .delete(
                &format!("/api/projects/{}/members/{}", w.project, w.admin_id),
                Some(&bob)
            )
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        server.delete(&path, Some(&w.admin)).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        server.delete(&path, Some(&w.admin)).await,
        (
            StatusCode::NOT_FOUND,
            json!({"detail": "not a member of the project"})
        )
    );
    assert_eq!(
        server
            .get(&format!("/api/projects/{}/members", w.project), Some(&bob))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}

/// Eve: an admin of an organisation of her own with a project; not in Robotics.
async fn eve_elsewhere(server: &TestServer, w: &World) -> (Token, Id, Id) {
    let (_, org) = server
        .post("/api/orgs", Some(&w.admin), json!({"name": "Elsewhere"}))
        .await;
    let org = org["id"].as_str().unwrap().to_owned();
    let (_, project) = server
        .post(
            &format!("/api/orgs/{org}/projects"),
            Some(&w.admin),
            json!({"name": "Theirs"}),
        )
        .await;
    let project = project["id"].as_str().unwrap().to_owned();
    let (_, inv) = server
        .post(
            &format!("/api/orgs/{org}/invites"),
            Some(&w.admin),
            json!({"email": "eve@example.com", "role": "admin"}),
        )
        .await;
    let (eve, eve_id) = server
        .register_invited("eve@example.com", None, inv["token"].as_str().unwrap())
        .await;
    let _ = eve_id;
    (eve, org, project)
}

#[tokio::test]
async fn another_organisations_projects_answer_exactly_as_missing_ones() {
    let (server, w) = world().await;
    let (eve, _, eves_project) = eve_elsewhere(&server, &w).await;
    let (_, bob_id) = w.editor(&server, "bob@example.com").await;
    let missing = "0".repeat(32);
    let not_found = (StatusCode::NOT_FOUND, json!({"detail": "no such project"}));
    for project in [w.project.as_str(), missing.as_str()] {
        let calls = [
            server
                .patch(
                    &format!("/api/projects/{project}"),
                    Some(&eve),
                    json!({"name": "x"}),
                )
                .await,
            server
                .delete(&format!("/api/projects/{project}"), Some(&eve))
                .await,
            server
                .get(&format!("/api/projects/{project}/members"), Some(&eve))
                .await,
            server
                .put(
                    &format!("/api/projects/{project}/members/{bob_id}"),
                    Some(&eve),
                    json!({"role": "viewer"}),
                )
                .await,
            server
                .delete(
                    &format!("/api/projects/{project}/members/{bob_id}"),
                    Some(&eve),
                )
                .await,
        ];
        for call in calls {
            assert_eq!(call, not_found, "{project}");
        }
    }
    // Robotics' people cannot be given roles in Eve's project: the same answer as nobody
    for user in [bob_id.as_str(), missing.as_str()] {
        assert_eq!(
            server
                .put(
                    &format!("/api/projects/{eves_project}/members/{user}"),
                    Some(&eve),
                    json!({"role": "viewer"}),
                )
                .await,
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({"detail": "that person is not in the organisation; invite them first"})
            )
        );
        assert_eq!(
            server
                .delete(
                    &format!("/api/projects/{eves_project}/members/{user}"),
                    Some(&eve)
                )
                .await,
            (
                StatusCode::NOT_FOUND,
                json!({"detail": "not a member of the project"})
            )
        );
    }
    // Rover is untouched
    assert_eq!(
        server
            .state
            .store
            .project_by_id(&w.project)
            .await
            .unwrap()
            .unwrap()
            .name,
        "Rover"
    );
    // a project role row alone, without organisation membership, gives nothing
    server
        .sql(&format!(
            "INSERT INTO project_members (project_id, user_id, role) VALUES ('{}', \
             (SELECT id FROM users WHERE email = 'eve@example.com'), 'owner')",
            w.project
        ))
        .await;
    assert_eq!(
        server
            .get(&format!("/api/projects/{}/members", w.project), Some(&eve))
            .await,
        not_found
    );
}

/// No more projects.
struct NoProjects;

#[async_trait]
impl Extensions for NoProjects {
    async fn quota(&self, _org: &Id, what: Quota) -> Result<(), String> {
        match what {
            Quota::Projects => Err("this plan has one project".into()),
            _ => Ok(()),
        }
    }
}

#[tokio::test]
async fn the_quota_hook_can_refuse_a_project() {
    let server = TestServer::start_custom(|_| {}, |s| s.ext = Arc::new(NoProjects)).await;
    let (admin, _) = server.register("ada@example.com").await;
    let (_, org) = server
        .post("/api/orgs", Some(&admin), json!({"name": "Robotics"}))
        .await;
    let org = org["id"].as_str().unwrap();
    assert_eq!(
        server
            .post(
                &format!("/api/orgs/{org}/projects"),
                Some(&admin),
                json!({"name": "Rover"}),
            )
            .await,
        (
            StatusCode::PAYMENT_REQUIRED,
            json!({"detail": "this plan has one project"})
        )
    );
    assert!(
        server
            .state
            .store
            .projects_in_org(org)
            .await
            .unwrap()
            .is_empty()
    );
}

/// While the quota hook is being asked, removes an account from the organisation (as a concurrent
/// request could).
struct RemovesTheCaller(Arc<dyn Store>, std::sync::Mutex<Option<(Id, Id)>>);

#[async_trait]
impl Extensions for RemovesTheCaller {
    async fn quota(&self, _org: &Id, _what: Quota) -> Result<(), String> {
        let target = self.1.lock().unwrap().clone();
        if let Some((org, user)) = target {
            let mut tx = self.0.begin().await.unwrap();
            tx.delete_org_member(&org, &user).await.unwrap();
            tx.commit().await.unwrap();
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_membership_lost_after_the_first_check_still_refuses_a_new_project() {
    let holder = Arc::new(std::sync::Mutex::new(None::<Arc<RemovesTheCaller>>));
    let kept = holder.clone();
    let server = TestServer::start_custom(
        |_| {},
        move |s| {
            let ext = Arc::new(RemovesTheCaller(
                s.store.clone(),
                std::sync::Mutex::new(None),
            ));
            *kept.lock().unwrap() = Some(ext.clone());
            s.ext = ext;
        },
    )
    .await;
    let w = World::new(&server).await.unwrap();
    let (bob, bob_id) = w.editor(&server, "bob@example.com").await;
    let ext = holder.lock().unwrap().clone().unwrap();
    *ext.1.lock().unwrap() = Some((w.org.clone(), bob_id));
    assert_eq!(
        new_project(&server, &w, &bob, "Arm").await,
        (
            StatusCode::NOT_FOUND,
            json!({"detail": "no such organisation"})
        )
    );
    assert_eq!(
        server
            .state
            .store
            .projects_in_org(&w.org)
            .await
            .unwrap()
            .len(),
        1
    );
}
