//! Organisations, members and invites (the organisation and invite tests of the Python
//! `test_accounts.py`, the invite halves of `test_email.py`, the seat hook), the extension hooks and
//! events, and the hostile cases.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use common::{LogCapture, PASSWORD, TestServer, Token, World, world};
use oswx_server::ext::{Event, Extensions, Quota};
use oswx_server::hub::Hub;
use oswx_server::mail::{Mailer, MemoryMailer};
use oswx_server::security::token_hash;
use oswx_server::store::{Id, Store};
use oswx_server::{AppState, Clock};
use reqwest::StatusCode;
use serde_json::{Value, json};

const T0: f64 = 1_800_000_000.0;

async fn invite(server: &TestServer, w: &World, body: Value) -> (StatusCode, Value) {
    server
        .post(
            &format!("/api/orgs/{}/invites", w.org),
            Some(&w.admin),
            body,
        )
        .await
}

async fn register(server: &TestServer, email: &str, invite: Option<&str>) -> (StatusCode, Value) {
    server
        .post(
            "/api/auth/register",
            None,
            json!({"email": email, "password": PASSWORD, "invite": invite}),
        )
        .await
}

async fn accept(server: &TestServer, token: &str, invite: &str) -> (StatusCode, Value) {
    server
        .post(
            "/api/invites/accept",
            Some(token),
            json!({ "token": invite }),
        )
        .await
}

fn roles(list: &Value) -> Vec<&str> {
    list.as_array()
        .unwrap()
        .iter()
        .map(|o| o["role"].as_str().unwrap())
        .collect()
}

// ------------------------------------------------------------------ ported

#[tokio::test]
async fn test_invite_joins_org_and_project_once() {
    let (server, w) = world().await;
    let (s, body) = invite(
        &server,
        &w,
        json!({"email": "bob@example.com", "projects": [{"project_id": w.project, "role": "viewer"}]}),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let token = body["token"].as_str().unwrap().to_owned();
    // an invite for one address does not let another person in
    assert_eq!(
        register(&server, "eve@example.com", Some(&token)).await.0,
        StatusCode::FORBIDDEN
    );
    let (bob, _) = server
        .register_invited("bob@example.com", None, &token)
        .await;
    assert_eq!(
        roles(&server.get("/api/orgs", Some(&bob)).await.1),
        ["member"]
    );
    let projects = server
        .get(&format!("/api/orgs/{}/projects", w.org), Some(&bob))
        .await
        .1;
    assert_eq!(roles(&projects), ["viewer"]);
    // used up
    assert_eq!(
        register(&server, "bob2@example.com", Some(&token)).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        server
            .get(&format!("/api/orgs/{}/invites", w.org), Some(&w.admin))
            .await
            .1,
        json!([])
    );
    // members cannot invite or create organisations
    assert_eq!(
        server
            .post(
                &format!("/api/orgs/{}/invites", w.org),
                Some(&bob),
                json!({})
            )
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        server
            .post("/api/orgs", Some(&bob), json!({"name": "Mine"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn test_existing_account_accepts_a_link_invite() {
    let (server, w) = world().await;
    let (s, body) = server
        .post("/api/orgs", Some(&w.admin), json!({"name": "Second"}))
        .await;
    assert_eq!(s, StatusCode::CREATED);
    let other = body["id"].as_str().unwrap().to_owned();
    let (bob, _) = w.editor(&server, "bob@example.com").await;
    let (_, link) = server
        .post(
            &format!("/api/orgs/{other}/invites"),
            Some(&w.admin),
            json!({"role": "admin"}),
        )
        .await;
    let link_token = link["token"].as_str().unwrap();
    assert!(link["url"].as_str().unwrap().ends_with(link_token));
    let (s, body) = accept(&server, &bob, link_token).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({"org": {"id": other, "name": "Second"}, "role": "admin"})
    );
    assert_eq!(
        accept(&server, &bob, link_token).await.0,
        StatusCode::NOT_FOUND
    );
    let orgs = server.get("/api/orgs", Some(&bob)).await.1;
    let by_name: Vec<(&str, &str)> = orgs
        .as_array()
        .unwrap()
        .iter()
        .map(|o| (o["name"].as_str().unwrap(), o["role"].as_str().unwrap()))
        .collect();
    assert_eq!(by_name, [("Robotics", "member"), ("Second", "admin")]);
}

#[tokio::test]
async fn test_members_roles_and_last_owner() {
    let (server, w) = world().await;
    let (bob, bob_id) = w.editor(&server, "bob@example.com").await;
    let member = |id: &str| format!("/api/orgs/{}/members/{id}", w.org);
    let (s, body) = server
        .patch(&member(&bob_id), Some(&w.admin), json!({"role": "admin"}))
        .await;
    assert_eq!((s, body["role"].as_str()), (StatusCode::OK, Some("admin")));
    // an admin cannot make owners or touch them
    let (s, body) = server
        .patch(&member(&bob_id), Some(&bob), json!({"role": "owner"}))
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(body["detail"], "only an owner can change who is an owner");
    let (s, body) = server.delete(&member(&w.admin_id), Some(&bob)).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(body["detail"], "only an owner can remove an owner");
    // the only owner cannot step down or leave
    let (s, body) = server
        .patch(
            &member(&w.admin_id),
            Some(&w.admin),
            json!({"role": "member"}),
        )
        .await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert_eq!(body["detail"], "an organisation needs at least one owner");
    assert_eq!(
        server.delete(&member(&w.admin_id), Some(&w.admin)).await.0,
        StatusCode::CONFLICT
    );
    // removing a member takes their project access with it
    assert_eq!(
        server.delete(&member(&bob_id), Some(&w.admin)).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(server.get("/api/orgs", Some(&bob)).await.1, json!([]));
    assert_eq!(
        server
            .get(&format!("/api/projects/{}/members", w.project), Some(&bob))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let store = &server.state.store;
    assert!(store.org_member(&w.org, &bob_id).await.unwrap().is_none());
    assert!(
        store
            .project_member(&w.project, &bob_id)
            .await
            .unwrap()
            .is_none()
    );
}

/// Two seats per organisation, and an edition name.
struct TwoSeats;

#[async_trait]
impl Extensions for TwoSeats {
    async fn may_add_member(&self, _org: &Id, current_members: usize) -> Result<(), String> {
        if current_members < 2 {
            Ok(())
        } else {
            Err("this organisation has 2 seats".into())
        }
    }

    fn server_info(&self) -> Value {
        json!({"edition": "test"})
    }
}

#[tokio::test]
async fn test_seat_hook_can_refuse() {
    let server = TestServer::start_custom(|_| {}, |s| s.ext = Arc::new(TwoSeats)).await;
    let w = World::new(&server).await.unwrap();
    w.editor(&server, "bob@example.com").await;
    let (s, body) = invite(&server, &w, json!({"email": "cy@example.com"})).await;
    assert_eq!(s, StatusCode::PAYMENT_REQUIRED);
    assert!(body["detail"].as_str().unwrap().contains("2 seats"));
    assert_eq!(server.get("/api/server", None).await.1["edition"], "test");
}

#[tokio::test]
async fn test_open_registration_lets_anyone_create_an_organisation() {
    let server = TestServer::start_with(|s| s.open_registration = true).await;
    server.register("ada@example.com").await;
    let (bob, _) = server.register("bob@example.com").await;
    let (s, body) = server
        .post("/api/orgs", Some(&bob), json!({"name": "Bob's"}))
        .await;
    assert_eq!(s, StatusCode::CREATED);
    assert_eq!(body["role"], "owner");
    assert_eq!(body["members"], 1);
}

#[tokio::test]
async fn test_without_smtp_invites_are_not_emailed() {
    let (server, w) = world().await;
    assert_eq!(server.get("/api/server", None).await.1["email"], false);
    let (s, body) = invite(&server, &w, json!({"email": "bob@example.com"})).await;
    assert_eq!(s, StatusCode::CREATED);
    assert_eq!(body["emailed"], false);
    assert!(
        body["url"]
            .as_str()
            .unwrap()
            .starts_with("http://testserver/invite/")
    );
}

#[tokio::test]
async fn test_an_invite_is_emailed() {
    let (server, mailer) = TestServer::start_mailing(|_| {}).await;
    assert_eq!(server.get("/api/server", None).await.1["email"], true);
    let w = World::new(&server).await.unwrap();
    let (_, r) = invite(&server, &w, json!({"email": "bob@example.com"})).await;
    assert_eq!(r["emailed"], true);
    let sent = mailer.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].to, "bob@example.com");
    assert_eq!(sent[0].subject, "Ada invited you to Robotics on openswx");
    assert!(sent[0].text.contains("expires in 7 days"));
    let url = r["url"].as_str().unwrap();
    assert!(sent[0].text.contains(&format!("\n\n{url}\n\n")));
    // the link in the email works
    let token = url.rsplit('/').next().unwrap();
    server
        .register_invited("bob@example.com", None, token)
        .await;
    // link invites (no address) and send_email=false send nothing
    let (_, r) = invite(&server, &w, json!({})).await;
    assert_eq!(r["emailed"], false);
    let (_, r) = invite(
        &server,
        &w,
        json!({"email": "cy@example.com", "send_email": false}),
    )
    .await;
    assert_eq!(r["emailed"], false);
    assert_eq!(mailer.sent().len(), 1);
}

/// A mail server that refuses everything.
struct BrokenMailer;

#[async_trait]
impl Mailer for BrokenMailer {
    fn configured(&self) -> bool {
        true
    }

    async fn send(&self, _: &str, _: &str, _: &str, _: Option<&str>) -> anyhow::Result<()> {
        anyhow::bail!("connection refused")
    }
}

#[tokio::test]
async fn test_a_broken_mail_server_does_not_break_an_invite() {
    let logs = LogCapture::start();
    let server = TestServer::start_custom(
        |s| {
            s.smtp_host = "mail.invalid".into();
            s.smtp_from = "c@example.org".into();
        },
        |s| s.mailer = Arc::new(BrokenMailer),
    )
    .await;
    let w = World::new(&server).await.unwrap();
    let (s, body) = invite(&server, &w, json!({"email": "bob@example.com"})).await;
    assert_eq!((s, &body["emailed"]), (StatusCode::CREATED, &json!(false)));
    assert!(
        logs.text()
            .contains("could not send email to the mail server mail.invalid")
    );
}

// ------------------------------------------------------------------ organisations

#[tokio::test]
async fn organisations_are_listed_with_their_members_and_only_owners_rename_them() {
    let (server, w) = world().await;
    let (bob, bob_id) = w.editor(&server, "bob@example.com").await;
    let (_, second) = server
        .post("/api/orgs", Some(&w.admin), json!({"name": "  Alpha  "}))
        .await;
    assert_eq!(second["name"], "Alpha", "the name is trimmed");
    let list = server.get("/api/orgs", Some(&w.admin)).await.1;
    assert_eq!(
        list,
        json!([
            {"id": second["id"], "name": "Alpha", "role": "owner", "members": 1},
            {"id": w.org, "name": "Robotics", "role": "owner", "members": 2},
        ])
    );
    let members = server
        .get(&format!("/api/orgs/{}/members", w.org), Some(&bob))
        .await
        .1;
    assert_eq!(
        members,
        json!([
            {"id": w.admin_id, "email": "ada@example.com", "name": "Ada", "role": "owner"},
            {"id": bob_id, "email": "bob@example.com", "name": "bob", "role": "member"},
        ])
    );
    // an admin is not an owner
    server
        .patch(
            &format!("/api/orgs/{}/members/{bob_id}", w.org),
            Some(&w.admin),
            json!({"role": "admin"}),
        )
        .await;
    let (s, body) = server
        .patch(
            &format!("/api/orgs/{}", w.org),
            Some(&bob),
            json!({"name": "Mine"}),
        )
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(body["detail"], "needs the organisation role owner");
    let (s, body) = server
        .patch(
            &format!("/api/orgs/{}", w.org),
            Some(&w.admin),
            json!({"name": " Rovers "}),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, json!({"id": w.org, "name": "Rovers"}));
    let stored = server.state.store.org_by_id(&w.org).await.unwrap().unwrap();
    assert_eq!(stored.name, "Rovers");
}

#[tokio::test]
async fn only_the_server_admin_creates_organisations_and_it_sees_no_others() {
    let server = TestServer::start().await;
    let w = World::new(&server).await.unwrap();
    let (bob, _) = w.editor(&server, "bob@example.com").await;
    let (s, body) = server
        .post("/api/orgs", Some(&bob), json!({"name": "Bob's"}))
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(
        body["detail"],
        "only the server admin can create organisations on this server"
    );
    // being the server admin gives no role in an organisation one is not a member of
    let server = TestServer::start_with(|s| s.open_registration = true).await;
    let (admin, _) = server.register("ada@example.com").await;
    let (cy, _) = server.register("cy@example.com").await;
    let (_, cys) = server
        .post("/api/orgs", Some(&cy), json!({"name": "Cy's"}))
        .await;
    let cys = cys["id"].as_str().unwrap();
    assert_eq!(
        server
            .get(&format!("/api/orgs/{cys}/members"), Some(&admin))
            .await,
        (
            StatusCode::NOT_FOUND,
            json!({"detail": "no such organisation"})
        )
    );
    assert_eq!(server.get("/api/orgs", Some(&admin)).await.1, json!([]));
}

#[tokio::test]
async fn a_member_cannot_manage_invites_or_roles() {
    let (server, w) = world().await;
    let (bob, bob_id) = w.editor(&server, "bob@example.com").await;
    let (_, cy_id) = w.editor(&server, "cy@example.com").await;
    let (_, inv) = invite(&server, &w, json!({})).await;
    let org = &w.org;
    let admin_only = [
        server
            .get(&format!("/api/orgs/{org}/invites"), Some(&bob))
            .await,
        server
            .post(&format!("/api/orgs/{org}/invites"), Some(&bob), json!({}))
            .await,
        server
            .delete(
                &format!("/api/orgs/{org}/invites/{}", inv["id"].as_str().unwrap()),
                Some(&bob),
            )
            .await,
        server
            .patch(
                &format!("/api/orgs/{org}/members/{cy_id}"),
                Some(&bob),
                json!({"role": "member"}),
            )
            .await,
        server
            .delete(&format!("/api/orgs/{org}/members/{cy_id}"), Some(&bob))
            .await,
    ];
    for (s, body) in admin_only {
        assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["detail"], "needs the organisation role admin");
    }
    // nor raise themselves
    let (s, _) = server
        .patch(
            &format!("/api/orgs/{org}/members/{bob_id}"),
            Some(&bob),
            json!({"role": "admin"}),
        )
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    // but anyone may leave
    assert_eq!(
        server
            .delete(&format!("/api/orgs/{org}/members/{bob_id}"), Some(&bob))
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    // and the invite is still there
    assert_eq!(
        server
            .get(&format!("/api/orgs/{org}/invites"), Some(&w.admin))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn names_are_one_to_two_hundred_characters() {
    let (server, w) = world().await;
    for bad in [
        json!(""),
        json!("   "),
        json!("x".repeat(201)),
        json!(5),
        json!(null),
    ] {
        let (s, body) = server
            .post("/api/orgs", Some(&w.admin), json!({ "name": bad }))
            .await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{bad}: {body}");
        let (s, _) = server
            .patch(
                &format!("/api/orgs/{}", w.org),
                Some(&w.admin),
                json!({ "name": bad }),
            )
            .await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{bad}");
    }
    assert_eq!(
        server.post("/api/orgs", Some(&w.admin), json!({})).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let longest = "é".repeat(200);
    let (s, body) = server
        .post("/api/orgs", Some(&w.admin), json!({ "name": longest }))
        .await;
    assert_eq!(
        (s, body["name"].as_str()),
        (StatusCode::CREATED, Some(longest.as_str()))
    );
}

#[tokio::test]
async fn role_strings_outside_the_allowed_sets_are_refused() {
    let (server, w) = world().await;
    let (_, bob_id) = w.editor(&server, "bob@example.com").await;
    for bad in [
        json!("Owner"),
        json!("superuser"),
        json!(""),
        json!(3),
        json!(null),
        json!("viewer"),
    ] {
        let (s, body) = server
            .patch(
                &format!("/api/orgs/{}/members/{bob_id}", w.org),
                Some(&w.admin),
                json!({ "role": bad }),
            )
            .await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{bad}: {body}");
    }
    let stored = server
        .state
        .store
        .org_member(&w.org, &bob_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.role, "member");
}

#[tokio::test]
async fn owners_can_hand_over_and_step_down() {
    let (server, w) = world().await;
    let (bob, bob_id) = w.editor(&server, "bob@example.com").await;
    let member = |id: &str| format!("/api/orgs/{}/members/{id}", w.org);
    server
        .patch(&member(&bob_id), Some(&w.admin), json!({"role": "owner"}))
        .await;
    // now there are two owners: either may step down or leave
    let (s, _) = server
        .patch(
            &member(&w.admin_id),
            Some(&w.admin),
            json!({"role": "admin"}),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    // Bob is the only owner again
    assert_eq!(
        server.delete(&member(&bob_id), Some(&bob)).await.0,
        StatusCode::CONFLICT
    );
    // an admin can demote themselves
    let (s, _) = server
        .patch(
            &member(&w.admin_id),
            Some(&w.admin),
            json!({"role": "member"}),
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        server
            .state
            .store
            .org_member(&w.org, &w.admin_id)
            .await
            .unwrap()
            .unwrap()
            .role,
        "member"
    );
}

// ------------------------------------------------------------------ invites

#[tokio::test]
async fn an_invite_works_once_until_it_expires_and_only_for_its_address() {
    let clock = Clock::manual(T0);
    let kept = clock.clone();
    let server = TestServer::start_custom(|_| {}, |s| s.clock = kept).await;
    let w = World::new(&server).await.unwrap();
    let (cy, _) = w.editor(&server, "cy@example.com").await;

    // bound to an address: nobody else can use it, signed in or registering
    let (_, bound) = invite(&server, &w, json!({"email": "Bob@Example.com"})).await;
    assert_eq!(bound["email"], "bob@example.com", "stored normalised");
    let bound = bound["token"].as_str().unwrap().to_owned();
    let invalid =
        json!({"detail": "this invite is not valid (used, expired, or for another email address)"});
    assert_eq!(
        accept(&server, &cy, &bound).await,
        (StatusCode::NOT_FOUND, invalid.clone())
    );
    // unknown tokens answer the same
    assert_eq!(
        accept(&server, &cy, "nonsense").await,
        (StatusCode::NOT_FOUND, invalid.clone())
    );
    assert_eq!(
        accept(&server, &cy, "").await,
        (StatusCode::NOT_FOUND, invalid.clone())
    );

    // a link invite expires after its days
    let (_, link) = invite(&server, &w, json!({"days": 1})).await;
    let link_token = link["token"].as_str().unwrap().to_owned();
    assert_eq!(link["expires_at"], json!(T0 + 86400.0));
    clock.advance(86400.0 + 1.0);
    assert_eq!(
        accept(&server, &cy, &link_token).await,
        (StatusCode::NOT_FOUND, invalid.clone())
    );
    assert_eq!(
        register(&server, "dee@example.com", Some(&link_token))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    // the list shows the 7-day one, not the expired one
    let open = server
        .get(&format!("/api/orgs/{}/invites", w.org), Some(&w.admin))
        .await
        .1;
    assert_eq!(open.as_array().unwrap().len(), 1);
    assert_eq!(open[0]["email"], "bob@example.com");
    assert!(open[0].get("token").is_none(), "{open}");

    // used once, by the right address
    server
        .register_invited("bob@example.com", None, &bound)
        .await;
    let bob = server.login("bob@example.com", PASSWORD).await;
    assert_eq!(
        accept(&server, &bob, &bound).await,
        (StatusCode::NOT_FOUND, invalid)
    );
}

#[tokio::test]
async fn invites_are_stored_hashed_and_their_tokens_never_logged() {
    let logs = LogCapture::start();
    let (server, w) = world().await;
    let (_, body) = invite(&server, &w, json!({"email": "bob@example.com"})).await;
    let token = body["token"].as_str().unwrap().to_owned();
    let id = body["id"].as_str().unwrap();
    let stored = server.state.store.invite_by_id(id).await.unwrap().unwrap();
    assert_eq!(stored.token_hash, token_hash(&token));
    assert_ne!(stored.token_hash, token);
    assert_eq!(stored.created_by, w.admin_id);
    assert_eq!(stored.role, "member");
    assert_eq!(stored.projects, "[]");
    server
        .register_invited("bob@example.com", None, &token)
        .await;
    let bob = server.login("bob@example.com", PASSWORD).await;
    accept(&server, &bob, &token).await; // (used: refused, and not logged either)
    let bob_id = server
        .state
        .store
        .user_by_email("bob@example.com")
        .await
        .unwrap()
        .unwrap()
        .id;
    let stored = server.state.store.invite_by_id(id).await.unwrap().unwrap();
    assert_eq!(stored.accepted_by, Some(bob_id));
    logs.assert_never_logged(&[&token]);
}

#[tokio::test]
async fn a_revoked_invite_no_longer_works() {
    let (server, w) = world().await;
    let (_, inv) = invite(&server, &w, json!({"email": "bob@example.com"})).await;
    let path = format!(
        "/api/orgs/{}/invites/{}",
        w.org,
        inv["id"].as_str().unwrap()
    );
    assert_eq!(
        server.delete(&path, Some(&w.admin)).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        server.delete(&path, Some(&w.admin)).await,
        (StatusCode::NOT_FOUND, json!({"detail": "no such invite"}))
    );
    assert_eq!(
        register(&server, "bob@example.com", inv["token"].as_str())
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert!(
        server
            .state
            .store
            .invite_by_id(inv["id"].as_str().unwrap())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn an_invite_gives_its_project_roles_and_keeps_existing_ones() {
    let (server, w) = world().await;
    let (_, p2) = server
        .post(
            &format!("/api/orgs/{}/projects", w.org),
            Some(&w.admin),
            json!({"name": "Arm"}),
        )
        .await;
    let p2 = p2["id"].as_str().unwrap().to_owned();
    let (bob, bob_id) = w
        .invite(
            &server,
            "bob@example.com",
            Some("Bob"),
            Some("viewer"),
            "member",
        )
        .await;
    // a second invite: the organisation role stays, the Rover role stays, the Arm role is added
    // (a new grant without a role means editor)
    let (_, inv) = invite(
        &server,
        &w,
        json!({"role": "admin", "projects": [
            {"project_id": w.project, "role": "owner"},
            {"project_id": p2},
        ]}),
    )
    .await;
    assert_eq!(
        inv["projects"],
        json!([{"project_id": w.project, "role": "owner"}, {"project_id": p2, "role": "editor"}])
    );
    let (s, body) = accept(&server, &bob, inv["token"].as_str().unwrap()).await;
    assert_eq!((s, &body["role"]), (StatusCode::OK, &json!("member")));
    let store = &server.state.store;
    assert_eq!(
        store
            .project_member(&w.project, &bob_id)
            .await
            .unwrap()
            .unwrap()
            .role,
        "viewer"
    );
    assert_eq!(
        store
            .project_member(&p2, &bob_id)
            .await
            .unwrap()
            .unwrap()
            .role,
        "editor"
    );
}

#[tokio::test]
async fn a_grant_for_a_project_deleted_since_is_skipped() {
    let (server, w) = world().await;
    let (_, p2) = server
        .post(
            &format!("/api/orgs/{}/projects", w.org),
            Some(&w.admin),
            json!({"name": "Arm"}),
        )
        .await;
    let p2 = p2["id"].as_str().unwrap().to_owned();
    let (_, inv) = invite(
        &server,
        &w,
        json!({"projects": [{"project_id": p2, "role": "editor"}, {"project_id": w.project, "role": "viewer"}]}),
    )
    .await;
    server
        .delete(&format!("/api/projects/{p2}"), Some(&w.admin))
        .await;
    let (bob, _) = server
        .register_invited("bob@example.com", None, inv["token"].as_str().unwrap())
        .await;
    let projects = server
        .get(&format!("/api/orgs/{}/projects", w.org), Some(&bob))
        .await
        .1;
    assert_eq!(projects.as_array().unwrap().len(), 1);
    assert_eq!(projects[0]["id"], json!(w.project));
}

#[tokio::test]
async fn in_open_registration_an_invite_for_another_address_is_ignored() {
    let server = TestServer::start_with(|s| s.open_registration = true).await;
    let w = World::new(&server).await.unwrap();
    let (_, inv) = invite(&server, &w, json!({"email": "bob@example.com"})).await;
    let (s, body) = register(&server, "eve@example.com", inv["token"].as_str()).await;
    assert_eq!(s, StatusCode::CREATED);
    let eve = body["token"].as_str().unwrap();
    assert_eq!(server.get("/api/orgs", Some(eve)).await.1, json!([]));
    // and it still works for Bob
    let (bob, _) = server
        .register_invited("bob@example.com", None, inv["token"].as_str().unwrap())
        .await;
    assert_eq!(
        roles(&server.get("/api/orgs", Some(&bob)).await.1),
        ["member"]
    );
}

#[tokio::test]
async fn hostile_invite_bodies_are_refused() {
    let (server, w) = world().await;
    let (_, other) = server
        .post("/api/orgs", Some(&w.admin), json!({"name": "Second"}))
        .await;
    let (_, foreign) = server
        .post(
            &format!("/api/orgs/{}/projects", other["id"].as_str().unwrap()),
            Some(&w.admin),
            json!({"name": "Elsewhere"}),
        )
        .await;
    let foreign = foreign["id"].as_str().unwrap();
    let many: Vec<Value> = (0..101)
        .map(|_| json!({"project_id": w.project, "role": "viewer"}))
        .collect();
    let hundred: Vec<Value> = (0..100)
        .map(|_| json!({"project_id": w.project, "role": "viewer"}))
        .collect();
    let bad = [
        json!({"role": "owner"}),
        json!({"role": "boss"}),
        json!({"role": null}),
        json!({"days": 0}),
        json!({"days": 91}),
        json!({"days": "7"}),
        json!({"days": 1.5}),
        json!({"days": -1}),
        json!({"email": "not an address"}),
        json!({"email": format!("{}@example.com", "a".repeat(320))}),
        json!({"email": 5}),
        json!({"send_email": "yes"}),
        json!({"projects": "all"}),
        json!({"projects": [{"role": "viewer"}]}),
        json!({"projects": [{"project_id": w.project, "role": "god"}]}),
        json!({"projects": [{"project_id": "x".repeat(65)}]}),
        json!({"projects": [{"project_id": "nonexistent"}]}),
        json!({"projects": [{"project_id": foreign}]}),
        json!({ "projects": many }),
    ];
    for body in bad {
        let (s, answer) = invite(&server, &w, body.clone()).await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}: {answer}");
    }
    // another organisation's project gets the same answer as one that does not exist
    let (_, a) = invite(&server, &w, json!({"projects": [{"project_id": foreign}]})).await;
    assert_eq!(
        a["detail"],
        format!("no project {foreign} in this organisation")
    );
    // nothing was made
    assert_eq!(
        server
            .get(&format!("/api/orgs/{}/invites", w.org), Some(&w.admin))
            .await
            .1,
        json!([])
    );
    // the limits themselves are accepted
    let (s, _) = invite(
        &server,
        &w,
        json!({ "projects": hundred, "days": 90, "email": "" }),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    // a token over the limit
    let (bob, _) = w.editor(&server, "bob@example.com").await;
    assert_eq!(
        accept(&server, &bob, &"t".repeat(257)).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        server
            .post("/api/invites/accept", Some(&bob), json!({}))
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        server
            .post("/api/invites/accept", None, json!({"token": "x"}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
}

/// Admits one more member when asked at invite time, then none.
struct SeatsRunOut(Mutex<usize>);

#[async_trait]
impl Extensions for SeatsRunOut {
    async fn may_add_member(&self, _org: &Id, _current: usize) -> Result<(), String> {
        let mut left = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if *left == 0 {
            return Err(String::new());
        }
        *left -= 1;
        Ok(())
    }
}

#[tokio::test]
async fn a_refused_seat_at_acceptance_changes_nothing() {
    let server = TestServer::start_custom(
        |s| s.open_registration = true,
        |s| s.ext = Arc::new(SeatsRunOut(Mutex::new(1))),
    )
    .await;
    let w = World::new(&server).await.unwrap();
    let (s, inv) = invite(
        &server,
        &w,
        json!({"projects": [{"project_id": w.project, "role": "editor"}]}),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    let token = inv["token"].as_str().unwrap();
    let (bob, bob_id) = server.register("bob@example.com").await;
    let (s, body) = accept(&server, &bob, token).await;
    assert_eq!(
        (s, body),
        (
            StatusCode::PAYMENT_REQUIRED,
            json!({"detail": "this organisation has no free seats"})
        )
    );
    let store = &server.state.store;
    assert!(store.org_member(&w.org, &bob_id).await.unwrap().is_none());
    assert!(
        store
            .project_member(&w.project, &bob_id)
            .await
            .unwrap()
            .is_none()
    );
    let stored = store
        .invite_by_id(inv["id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.accepted_by, None, "the invite is not used up");
    // registering with it is refused the same way, and makes no account
    let (s, _) = register(&server, "cy@example.com", Some(token)).await;
    assert_eq!(s, StatusCode::PAYMENT_REQUIRED);
    assert!(
        store
            .user_by_email("cy@example.com")
            .await
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------------------------------ another organisation's ids

/// Eve, an admin of an organisation of her own with a project, who is not in Robotics; Ada owns
/// both.
async fn eve_elsewhere(server: &TestServer, w: &World) -> (Token, Id, Id, Id) {
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
    (eve, eve_id, org, project)
}

#[tokio::test]
async fn another_organisations_ids_answer_exactly_as_missing_ones() {
    let (server, w) = world().await;
    let (eve, eve_id, eves_org, _) = eve_elsewhere(&server, &w).await;
    let (_, bob_id) = w.editor(&server, "bob@example.com").await;
    let (_, robotics_invite) = invite(&server, &w, json!({})).await;
    let robotics_invite = robotics_invite["id"].as_str().unwrap();
    let missing = "0".repeat(32);

    // Robotics, as Eve, against an organisation that does not exist
    for org in [w.org.as_str(), missing.as_str()] {
        let calls = [
            server
                .get(&format!("/api/orgs/{org}/members"), Some(&eve))
                .await,
            server
                .patch(
                    &format!("/api/orgs/{org}"),
                    Some(&eve),
                    json!({"name": "Mine"}),
                )
                .await,
            server
                .patch(
                    &format!("/api/orgs/{org}/members/{bob_id}"),
                    Some(&eve),
                    json!({"role": "admin"}),
                )
                .await,
            server
                .delete(&format!("/api/orgs/{org}/members/{bob_id}"), Some(&eve))
                .await,
            server
                .delete(&format!("/api/orgs/{org}/members/{eve_id}"), Some(&eve))
                .await,
            server
                .get(&format!("/api/orgs/{org}/invites"), Some(&eve))
                .await,
            server
                .post(&format!("/api/orgs/{org}/invites"), Some(&eve), json!({}))
                .await,
            server
                .delete(
                    &format!("/api/orgs/{org}/invites/{robotics_invite}"),
                    Some(&eve),
                )
                .await,
            server
                .get(&format!("/api/orgs/{org}/projects"), Some(&eve))
                .await,
            server
                .post(
                    &format!("/api/orgs/{org}/projects"),
                    Some(&eve),
                    json!({"name": "Mine"}),
                )
                .await,
        ];
        for (s, body) in calls {
            assert_eq!(
                (s, body),
                (
                    StatusCode::NOT_FOUND,
                    json!({"detail": "no such organisation"})
                ),
                "{org}"
            );
        }
    }

    // Robotics' invite and member ids inside Eve's own organisation, where she is admin
    for invite_id in [robotics_invite, missing.as_str()] {
        assert_eq!(
            server
                .delete(
                    &format!("/api/orgs/{eves_org}/invites/{invite_id}"),
                    Some(&eve)
                )
                .await,
            (StatusCode::NOT_FOUND, json!({"detail": "no such invite"}))
        );
    }
    for user in [bob_id.as_str(), missing.as_str()] {
        assert_eq!(
            server
                .patch(
                    &format!("/api/orgs/{eves_org}/members/{user}"),
                    Some(&eve),
                    json!({"role": "member"}),
                )
                .await,
            (StatusCode::NOT_FOUND, json!({"detail": "not a member"}))
        );
        assert_eq!(
            server
                .delete(&format!("/api/orgs/{eves_org}/members/{user}"), Some(&eve))
                .await,
            (StatusCode::NOT_FOUND, json!({"detail": "not a member"}))
        );
    }
    // Robotics' invite is untouched, and Eve learned nothing about Robotics
    assert!(
        server
            .state
            .store
            .invite_by_id(robotics_invite)
            .await
            .unwrap()
            .is_some()
    );
    let orgs = server.get("/api/orgs", Some(&eve)).await.1;
    assert_eq!(orgs.as_array().unwrap().len(), 1);
}

// ------------------------------------------------------------------ hooks and events

/// Records every hook call (prefixed `hook`) and every event (its JSON).
#[derive(Default)]
struct Recorder {
    calls: Mutex<Vec<String>>,
    events: Mutex<Vec<Value>>,
}

impl Recorder {
    fn push(&self, s: String) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(s);
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.calls.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

struct Recording(Arc<Recorder>);

#[async_trait]
impl Extensions for Recording {
    async fn may_add_member(&self, _org: &Id, current: usize) -> Result<(), String> {
        self.0.push(format!("hook may_add_member {current}"));
        Ok(())
    }

    async fn member_added(&self, _org: &Id, _user: &Id) {
        self.0.push("hook member_added".into());
    }

    async fn member_removed(&self, _org: &Id, _user: &Id) {
        self.0.push("hook member_removed".into());
    }

    async fn on_event(&self, event: &Event) {
        let v = serde_json::to_value(event).unwrap();
        self.0.push(v["type"].as_str().unwrap().to_owned());
        self.0
            .events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(v);
    }

    async fn quota(&self, _org: &Id, what: Quota) -> Result<(), String> {
        self.0.push(format!("hook quota {what:?}"));
        Ok(())
    }
}

#[tokio::test]
async fn the_hooks_and_events_are_called_where_things_happen() {
    let recorder = Arc::new(Recorder::default());
    let ext = Arc::new(Recording(recorder.clone()));
    let mailer = Arc::new(MemoryMailer::new(true));
    let kept = mailer.clone();
    let server = TestServer::start_custom(
        |s| {
            s.smtp_host = "127.0.0.1".into();
            s.smtp_from = "collab@example.org".into();
        },
        |s: &mut AppState| {
            s.ext = ext;
            s.mailer = kept;
        },
    )
    .await;
    let w = World::new(&server).await.unwrap();
    assert_eq!(
        recorder.take(),
        [
            "registered",
            "org_created",
            "hook quota Projects",
            "project_created"
        ]
    );

    let (bob, bob_id) = w.editor(&server, "bob@example.com").await;
    assert_eq!(
        recorder.take(),
        [
            "hook may_add_member 1",
            "invite_created",
            "hook may_add_member 1",
            "registered",
            "hook member_added",
            "member_added",
            "invite_accepted",
        ]
    );
    let org = &w.org;
    let project = &w.project;
    server
        .patch(
            &format!("/api/orgs/{org}/members/{bob_id}"),
            Some(&w.admin),
            json!({"role": "admin"}),
        )
        .await;
    server
        .put(
            &format!("/api/projects/{project}/members/{bob_id}"),
            Some(&w.admin),
            json!({"role": "viewer"}),
        )
        .await;
    server
        .delete(
            &format!("/api/projects/{project}/members/{bob_id}"),
            Some(&w.admin),
        )
        .await;
    server
        .patch(
            &format!("/api/projects/{project}"),
            Some(&w.admin),
            json!({"name": "Rover 2"}),
        )
        .await;
    server
        .patch(
            &format!("/api/orgs/{org}"),
            Some(&w.admin),
            json!({"name": "R"}),
        )
        .await;
    let (_, inv) = server
        .post(&format!("/api/orgs/{org}/invites"), Some(&bob), json!({}))
        .await;
    server
        .delete(
            &format!("/api/orgs/{org}/invites/{}", inv["id"].as_str().unwrap()),
            Some(&bob),
        )
        .await;
    server
        .delete(&format!("/api/orgs/{org}/members/{bob_id}"), Some(&w.admin))
        .await;
    server
        .delete(&format!("/api/projects/{project}"), Some(&w.admin))
        .await;
    assert_eq!(
        recorder.take(),
        [
            "member_role_changed",
            "project_member_set",
            "project_member_removed",
            "project_renamed",
            "org_renamed",
            "hook may_add_member 2",
            "invite_created",
            "invite_revoked",
            "hook member_removed",
            "member_removed",
            "project_deleted",
        ]
    );

    // the account events of Task 4
    let token = server.login("ada@example.com", PASSWORD).await;
    server
        .call(
            reqwest::Method::POST,
            "/api/auth/logout",
            Some(&token),
            None,
        )
        .await;
    server
        .patch(
            "/api/me",
            Some(&w.admin),
            json!({"password": "another one", "current_password": PASSWORD}),
        )
        .await;
    server
        .post(
            "/api/auth/password-reset",
            None,
            json!({"email": "ada@example.com"}),
        )
        .await;
    server
        .post(
            "/api/auth/password-reset",
            None,
            json!({"email": "nobody@example.com"}),
        )
        .await;
    let text = mailer.sent().last().unwrap().text.clone();
    let reset = text
        .split_whitespace()
        .find(|w| w.contains("/reset-password/"))
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned();
    server
        .post(
            "/api/auth/password-reset/confirm",
            None,
            json!({"token": reset, "password": "a third one"}),
        )
        .await;
    assert_eq!(
        recorder.take(),
        [
            "signed_in",
            "signed_out",
            "password_changed",
            "password_reset_requested",
            "password_reset_completed",
        ]
    );

    // ids only: no names, addresses or tokens
    let events = serde_json::to_string(&*recorder.events.lock().unwrap()).unwrap();
    for leak in ["example.com", "Robotics", "Rover", "Ada", &w.admin, &reset] {
        assert!(!events.contains(leak), "{leak} in {events}");
    }
    assert!(events.contains(&bob_id));
    let _ = bob;
}

#[tokio::test]
async fn a_stored_grant_without_a_role_gives_viewer() {
    // (as an invite written by the Python server can read)
    let (server, w) = world().await;
    let (_, inv) = invite(&server, &w, json!({})).await;
    server
        .sql(&format!(
            r#"UPDATE invites SET projects = '[{{"project_id": "{}"}}]' WHERE id = '{}'"#,
            w.project,
            inv["id"].as_str().unwrap()
        ))
        .await;
    let (_, bob_id) = server
        .register_invited("bob@example.com", None, inv["token"].as_str().unwrap())
        .await;
    let row = server
        .state
        .store
        .project_member(&w.project, &bob_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.role, "viewer");
}

// ------------------------------------------------------------------ the store's queries

#[tokio::test]
async fn the_organisation_queries_read_back_what_they_wrote() {
    use oswx_server::store::{Invite, Org, OrgMember, Project, ProjectMember, Tx};
    let server = TestServer::start_with(|s| s.open_registration = true).await;
    let (_, ada) = server.register("ada@example.com").await;
    let (_, bob) = server.register("bob@example.com").await;
    let store = &server.state.store;
    let org = |id: &str, name: &str| Org {
        id: id.repeat(32),
        name: name.into(),
        created_at: 1.0,
    };
    let member = |o: &str, user: &str, role: &str| OrgMember {
        org_id: o.repeat(32),
        user_id: user.to_owned(),
        role: role.into(),
        created_at: 2.0,
    };
    let project = |id: &str, o: &str| Project {
        id: id.repeat(32),
        org_id: o.repeat(32),
        name: format!("P{id}"),
        created_at: 3.0,
    };
    let (a, b) = ("a".repeat(32), "b".repeat(32));
    let mut tx: Box<dyn Tx> = store.begin().await.unwrap();
    tx.insert_org(&org("a", "Alpha")).await.unwrap();
    tx.insert_org(&org("b", "Beta")).await.unwrap();
    tx.insert_org_member(&member("a", &ada, "owner"))
        .await
        .unwrap();
    tx.insert_org_member(&member("a", &bob, "member"))
        .await
        .unwrap();
    tx.insert_org_member(&member("b", &bob, "owner"))
        .await
        .unwrap();
    tx.insert_project(&project("1", "a")).await.unwrap();
    tx.insert_project(&project("2", "b")).await.unwrap();
    for p in ["1", "2"] {
        tx.insert_project_member(&ProjectMember {
            project_id: p.repeat(32),
            user_id: bob.clone(),
            role: "editor".into(),
        })
        .await
        .unwrap();
    }
    assert_eq!(tx.org_member_count(&a).await.unwrap(), 2);
    assert_eq!(tx.org_owner_count(&a).await.unwrap(), 1);
    tx.commit().await.unwrap();

    let orgs = store.orgs_of_user(&bob).await.unwrap();
    let summary: Vec<_> = orgs
        .iter()
        .map(|o| (o.name.as_str(), o.role.as_str(), o.members))
        .collect();
    assert_eq!(summary, [("Alpha", "member", 2), ("Beta", "owner", 1)]);
    assert_eq!(store.org_member_count(&b).await.unwrap(), 1);
    assert_eq!(
        store.org_by_id(&a).await.unwrap().unwrap(),
        org("a", "Alpha")
    );
    assert_eq!(
        store.org_member(&a, &bob).await.unwrap().unwrap(),
        member("a", &bob, "member")
    );
    assert_eq!(
        store.projects_in_org(&a).await.unwrap(),
        [project("1", "a")]
    );
    let mine = store.member_projects(&b, &bob).await.unwrap();
    assert_eq!(mine.len(), 1);
    assert_eq!(
        (mine[0].id.as_str(), mine[0].role.as_str()),
        ("2".repeat(32).as_str(), "editor")
    );

    // removing Bob's project roles in one organisation leaves the other's
    let mut tx = store.begin().await.unwrap();
    tx.delete_project_members_in_org(&a, &bob).await.unwrap();
    tx.set_org_member_role(&a, &bob, "admin").await.unwrap();
    tx.set_org_name(&a, "Alpha 2").await.unwrap();
    tx.set_project_name(&"1".repeat(32), "One").await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        store
            .project_member(&"1".repeat(32), &bob)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .project_member(&"2".repeat(32), &bob)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store.org_member(&a, &bob).await.unwrap().unwrap().role,
        "admin"
    );
    assert_eq!(store.org_by_id(&a).await.unwrap().unwrap().name, "Alpha 2");
    assert_eq!(
        store
            .project_by_id(&"1".repeat(32))
            .await
            .unwrap()
            .unwrap()
            .name,
        "One"
    );

    // invites: claimed once; deleted only through their own organisation
    let inv = Invite {
        id: "i".repeat(32),
        token_hash: "h".repeat(64),
        org_id: a.clone(),
        email: Some("cy@example.com".into()),
        role: "member".into(),
        projects: "[]".into(),
        created_by: ada.clone(),
        created_at: 10.0,
        expires_at: 20.0,
        accepted_by: None,
    };
    let mut tx = store.begin().await.unwrap();
    tx.insert_invite(&inv).await.unwrap();
    assert_eq!(
        tx.invite_by_hash(&inv.token_hash).await.unwrap().unwrap(),
        inv
    );
    tx.commit().await.unwrap();
    assert_eq!(
        store.open_invites(&a, 19.0).await.unwrap(),
        std::slice::from_ref(&inv)
    );
    assert!(store.open_invites(&a, 20.0).await.unwrap().is_empty());
    let mut tx = store.begin().await.unwrap();
    assert!(tx.claim_invite(&inv.id, &bob).await.unwrap());
    assert!(!tx.claim_invite(&inv.id, &ada).await.unwrap());
    assert!(!tx.delete_invite(&b, &inv.id).await.unwrap());
    tx.commit().await.unwrap();
    assert_eq!(
        store
            .invite_by_id(&inv.id)
            .await
            .unwrap()
            .unwrap()
            .accepted_by,
        Some(bob.clone())
    );
    assert!(store.open_invites(&a, 0.0).await.unwrap().is_empty());
    let mut tx = store.begin().await.unwrap();
    assert!(tx.delete_invite(&a, &inv.id).await.unwrap());
    tx.delete_org_member(&a, &bob).await.unwrap();
    assert!(
        !tx.delete_project_member(&"1".repeat(32), &bob)
            .await
            .unwrap()
    );
    assert!(
        tx.delete_project_member(&"2".repeat(32), &bob)
            .await
            .unwrap()
    );
    tx.commit().await.unwrap();
    assert!(store.invite_by_id(&inv.id).await.unwrap().is_none());
    assert!(store.org_member(&a, &bob).await.unwrap().is_none());
    assert!(
        store
            .project_members(&"2".repeat(32))
            .await
            .unwrap()
            .is_empty()
    );
    // a project's documents are marked deleted, then go with it
    server
        .sql(&format!(
            "INSERT INTO documents (id, project_id, folder_id, name, kind, format_version, head_seq, \
             created_by, created_at, updated_at, deleted_at) VALUES ('{}', '{}', NULL, 'D', 'part', \
             3, 0, '{ada}', 1.0, 1.0, NULL)",
            "d".repeat(32),
            "1".repeat(32)
        ))
        .await;
    let mut tx = store.begin().await.unwrap();
    assert_eq!(
        tx.project_document_ids(&"1".repeat(32)).await.unwrap(),
        ["d".repeat(32)]
    );
    tx.mark_project_documents_deleted(&"1".repeat(32), 99.0)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let deleted_at: Option<f64> = sqlx::query_scalar("SELECT deleted_at FROM documents")
        .fetch_one(&mut *server.db().reader().await.unwrap())
        .await
        .unwrap();
    assert_eq!(deleted_at, Some(99.0));
    let mut tx = store.begin().await.unwrap();
    tx.delete_project(&"1".repeat(32)).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        store
            .project_by_id(&"1".repeat(32))
            .await
            .unwrap()
            .is_none()
    );
    let documents: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM documents")
        .fetch_one(&mut *server.db().reader().await.unwrap())
        .await
        .unwrap();
    assert_eq!(documents, 0, "the documents went with the project");
    // (ending on a request gives the writer connection time to go back to its pool before the
    // server is dropped)
    server.get("/api/health", None).await;
}

// ------------------------------------------------------------------ fix round 1: hooks, order, races

/// A seat hook that, once `slow` is set, never answers for `slow_org`; waits 1 s at most.
struct HangingSeats {
    slow: AtomicBool,
    slow_org: Mutex<Id>,
}

#[async_trait]
impl Extensions for HangingSeats {
    fn hook_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }

    async fn may_add_member(&self, org: &Id, _current: usize) -> Result<(), String> {
        let slow = self.slow.load(Ordering::SeqCst)
            && *org == *self.slow_org.lock().unwrap_or_else(PoisonError::into_inner);
        if slow {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seat_hook_that_hangs_blocks_nobody_else_and_its_request_ends_with_409() {
    let ext = Arc::new(HangingSeats {
        slow: false.into(),
        slow_org: Mutex::new(String::new()),
    });
    let kept = ext.clone();
    let server = TestServer::start_custom(|s| s.open_registration = true, |s| s.ext = kept).await;
    let w = World::new(&server).await.unwrap();
    let (_, other) = server
        .post("/api/orgs", Some(&w.admin), json!({"name": "Other"}))
        .await;
    let other = other["id"].as_str().unwrap().to_owned();
    let (_, inv) = invite(&server, &w, json!({})).await;
    let (bob, bob_id) = server.register("bob@example.com").await;
    *ext.slow_org.lock().unwrap() = w.org.clone();
    ext.slow.store(true, Ordering::SeqCst);

    let server = Arc::new(server);
    let started = Instant::now();
    let stuck = {
        let server = server.clone();
        let token = inv["token"].as_str().unwrap().to_owned();
        tokio::spawn(async move { accept(&server, &bob, &token).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    // another organisation's writes go on while the hook hangs
    let (s, _) = server
        .post(
            &format!("/api/orgs/{other}/projects"),
            Some(&w.admin),
            json!({"name": "Meanwhile"}),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED);
    assert!(!stuck.is_finished(), "the stuck request is still waiting");
    let (s, body) = stuck.await.unwrap();
    assert_eq!(
        (s, body),
        (
            StatusCode::CONFLICT,
            json!({"detail": "the server could not check this with its extensions in time; try again"})
        )
    );
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(
        server
            .state
            .store
            .org_member(&w.org, &bob_id)
            .await
            .unwrap()
            .is_none()
    );
}

/// Two seats; once `gate` is set, each seat check waits until two of them are waiting.
struct TwoSeatsTogether {
    gate: AtomicBool,
    both: tokio::sync::Barrier,
}

#[async_trait]
impl Extensions for TwoSeatsTogether {
    async fn may_add_member(&self, _org: &Id, current: usize) -> Result<(), String> {
        if self.gate.load(Ordering::SeqCst) {
            self.both.wait().await;
        }
        if current < 2 {
            Ok(())
        } else {
            Err("this organisation has 2 seats".into())
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_acceptances_for_the_last_seat_add_exactly_one_member() {
    let ext = Arc::new(TwoSeatsTogether {
        gate: false.into(),
        both: tokio::sync::Barrier::new(2),
    });
    let kept = ext.clone();
    let server = TestServer::start_custom(|s| s.open_registration = true, |s| s.ext = kept).await;
    let w = World::new(&server).await.unwrap();
    let (_, one) = invite(&server, &w, json!({})).await;
    let (_, two) = invite(&server, &w, json!({})).await;
    let (bob, _) = server.register("bob@example.com").await;
    let (cy, _) = server.register("cy@example.com").await;
    ext.gate.store(true, Ordering::SeqCst);
    let (a, b) = tokio::join!(
        accept(&server, &bob, one["token"].as_str().unwrap()),
        accept(&server, &cy, two["token"].as_str().unwrap()),
    );
    let mut codes = [a.0, b.0];
    codes.sort();
    assert_eq!(codes, [StatusCode::OK, StatusCode::CONFLICT], "{a:?} {b:?}");
    let refused = if a.0 == StatusCode::CONFLICT {
        a.1
    } else {
        b.1
    };
    assert!(
        refused["detail"].as_str().unwrap().ends_with("try again"),
        "{refused}"
    );
    assert_eq!(
        server.state.store.org_member_count(&w.org).await.unwrap(),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_use_invite_accepted_three_times_at_once_works_once() {
    let server = TestServer::start_with(|s| s.open_registration = true).await;
    let w = World::new(&server).await.unwrap();
    let (_, inv) = invite(&server, &w, json!({})).await;
    let token = inv["token"].as_str().unwrap();
    let (bob, _) = server.register("bob@example.com").await;
    let (cy, _) = server.register("cy@example.com").await;
    let (a, b, c) = tokio::join!(
        accept(&server, &bob, token),
        accept(&server, &cy, token),
        accept(&server, &bob, token),
    );
    let answers = [&a, &b, &c];
    assert_eq!(
        answers.iter().filter(|r| r.0.is_success()).count(),
        1,
        "{answers:?}"
    );
    for r in answers {
        assert!(
            r.0 == StatusCode::OK || r.0 == StatusCode::NOT_FOUND,
            "{r:?}"
        );
    }
    assert_eq!(
        server.state.store.org_member_count(&w.org).await.unwrap(),
        2
    );
}

/// Hooks that panic, and note whether the hub had already acted when they were called.
struct PanicsAfterHub(Hub, Arc<Mutex<Vec<String>>>);

#[async_trait]
impl Extensions for PanicsAfterHub {
    async fn member_removed(&self, _org: &Id, user: &Id) {
        let dropped = self
            .0
            .recorded()
            .iter()
            .any(|c| c.starts_with(&format!("drop_user {user}")));
        self.1
            .lock()
            .unwrap()
            .push(format!("member_removed after drop: {dropped}"));
        panic!("a broken member_removed hook");
    }

    async fn on_event(&self, _event: &Event) {
        panic!("a broken audit sink");
    }
}

#[tokio::test]
async fn the_hub_closes_access_before_the_hooks_and_whatever_they_do() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let kept = seen.clone();
    let server = TestServer::start_custom(
        |_| {},
        |s| s.ext = Arc::new(PanicsAfterHub(s.hub.clone(), kept)),
    )
    .await;
    let w = World::new(&server).await.unwrap();
    let (_, bob_id) = w.editor(&server, "bob@example.com").await;
    let (_, cy_id) = w.editor(&server, "cy@example.com").await;
    let org = &w.org;
    let p = &w.project;
    let calls = [
        server
            .patch(
                &format!("/api/orgs/{org}/members/{cy_id}"),
                Some(&w.admin),
                json!({"role": "admin"}),
            )
            .await,
        server
            .put(
                &format!("/api/projects/{p}/members/{bob_id}"),
                Some(&w.admin),
                json!({"role": "viewer"}),
            )
            .await,
        server
            .delete(
                &format!("/api/projects/{p}/members/{bob_id}"),
                Some(&w.admin),
            )
            .await,
        server
            .delete(&format!("/api/orgs/{org}/members/{bob_id}"), Some(&w.admin))
            .await,
    ];
    for (s, body) in calls {
        assert!(s.is_success(), "{s} {body}");
    }
    assert_eq!(
        server.state.hub.recorded(),
        [
            format!("drop_user {cy_id} {org}"),
            format!("drop_user {bob_id} {org}"),
            format!("drop_user {bob_id} {org}"),
            format!("drop_user {bob_id} {org}"),
        ]
    );
    assert_eq!(*seen.lock().unwrap(), ["member_removed after drop: true"]);
    assert!(
        server
            .state
            .store
            .org_member(org, &bob_id)
            .await
            .unwrap()
            .is_none()
    );
}

/// While the seat hook is being asked, lowers an account's role (as a concurrent request could).
struct DemotesTheCaller(Arc<dyn Store>, Mutex<Option<(Id, Id)>>);

#[async_trait]
impl Extensions for DemotesTheCaller {
    async fn may_add_member(&self, _org: &Id, _current: usize) -> Result<(), String> {
        let target = self.1.lock().unwrap().clone();
        if let Some((org, user)) = target {
            let mut tx = self.0.begin().await.unwrap();
            tx.set_org_member_role(&org, &user, "member").await.unwrap();
            tx.commit().await.unwrap();
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_role_lost_after_the_first_check_still_refuses_the_change() {
    let holder = Arc::new(Mutex::new(None::<Arc<DemotesTheCaller>>));
    let kept = holder.clone();
    let server = TestServer::start_custom(
        |_| {},
        move |s| {
            let ext = Arc::new(DemotesTheCaller(s.store.clone(), Mutex::new(None)));
            *kept.lock().unwrap() = Some(ext.clone());
            s.ext = ext;
        },
    )
    .await;
    let w = World::new(&server).await.unwrap();
    let (bob, bob_id) = w.editor(&server, "bob@example.com").await;
    server
        .patch(
            &format!("/api/orgs/{}/members/{bob_id}", w.org),
            Some(&w.admin),
            json!({"role": "admin"}),
        )
        .await;
    let ext = holder.lock().unwrap().clone().unwrap();
    *ext.1.lock().unwrap() = Some((w.org.clone(), bob_id.clone()));
    // Bob is an admin when the invite is asked for, and a member by the time it would be written
    let (s, body) = server
        .post(
            &format!("/api/orgs/{}/invites", w.org),
            Some(&bob),
            json!({}),
        )
        .await;
    assert_eq!(
        (s, body),
        (
            StatusCode::FORBIDDEN,
            json!({"detail": "needs the organisation role admin"})
        )
    );
    assert_eq!(
        server
            .get(&format!("/api/orgs/{}/invites", w.org), Some(&w.admin))
            .await
            .1,
        json!([])
    );
}
