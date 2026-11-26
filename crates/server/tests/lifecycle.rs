//! Leaving: deleting an account or an organisation, and exporting (the Python `test_lifecycle.py`,
//! and what the Rust server adds: streamed exports, their limits, and storage clean-up).

mod common;

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::*;
use futures_util::stream::{self, StreamExt};
use oswx_server::blobs::{Blobs, ByteStream, PutOutcome, blob_key};
use oswx_server::clock::Clock;
use oswx_server::export::Exports;
use oswx_server::ext::{Event, Extensions};
use oswx_server::security::token_hash;
use oswx_server::store::Id;
use reqwest::{Method, StatusCode, header};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const DELETED: &str = "Deleted user";
const MINUTE: Duration = Duration::from_secs(60);
const HOUR: Duration = Duration::from_secs(3600);

// ------------------------------------------------------------------ helpers

async fn delete_me(server: &TestServer, token: &str, body: Value) -> (StatusCode, Value) {
    server
        .call(Method::DELETE, "/api/me", Some(token), Some(body))
        .await
}

fn confirmed() -> Value {
    json!({"current_password": PASSWORD})
}

async fn delete_org(
    server: &TestServer,
    org: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    server
        .call(
            Method::DELETE,
            &format!("/api/orgs/{org}"),
            Some(token),
            Some(body),
        )
        .await
}

fn robotics() -> Value {
    json!({"confirm_name": "Robotics", "current_password": PASSWORD})
}

fn sha256(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

async fn upload(server: &TestServer, project: &str, token: &str, body: &[u8]) -> String {
    let sha = sha256(body);
    let r = reqwest::Client::new()
        .put(server.url(&format!("/api/projects/{project}/blobs/{sha}")))
        .bearer_auth(token)
        .body(body.to_vec())
        .send()
        .await
        .expect("an answer");
    assert_eq!(r.status(), 204);
    sha
}

/// [`part`] with an imported feature that refers to a blob.
fn part_with_blob(sha: &str, len: u64) -> Value {
    let mut p = part();
    p["features"].as_array_mut().unwrap().push(json!({
        "id": "f3", "name": "Imported1", "type": "import", "suppressed": false,
        "params": {"brep": {"$blob": sha, "len": len}},
    }));
    p
}

async fn export(server: &TestServer, org: &str, token: Option<&str>) -> reqwest::Response {
    let mut request = reqwest::Client::new().get(server.url(&format!("/api/orgs/{org}/export")));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    request.send().await.expect("an answer")
}

type Zip = zip::ZipArchive<io::Cursor<Vec<u8>>>;

/// The organisation's export, opened with a standard zip reader.
async fn export_zip(server: &TestServer, org: &str, token: &str) -> Zip {
    let r = export(server, org, Some(token)).await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()[header::CONTENT_TYPE], "application/zip");
    let bytes = r.bytes().await.expect("the whole export").to_vec();
    zip::ZipArchive::new(io::Cursor::new(bytes)).expect("a zip a standard reader opens")
}

fn names(zip: &Zip) -> Vec<String> {
    let mut names: Vec<String> = zip
        .file_names()
        .map(|n| n.expect("a name").into_owned())
        .collect();
    names.sort();
    names
}

fn entry(zip: &mut Zip, name: &str) -> Vec<u8> {
    let mut file = zip
        .by_name(name)
        .unwrap_or_else(|e| panic!("no entry {name}: {e}"));
    let mut out = Vec::new();
    file.read_to_end(&mut out).expect("the entry reads");
    out
}

fn entry_json(zip: &mut Zip, name: &str) -> Value {
    serde_json::from_slice(&entry(zip, name)).unwrap_or_else(|e| panic!("{name} is not JSON: {e}"))
}

fn entry_lines(zip: &mut Zip, name: &str) -> Vec<Value> {
    String::from_utf8(entry(zip, name))
        .expect("text")
        .lines()
        .map(|l| serde_json::from_str(l).expect("a JSON line"))
        .collect()
}

/// The storage folder of the test server.
fn storage(server: &TestServer) -> PathBuf {
    PathBuf::from(&server.state.settings.storage_url)
}

/// Waits (up to five seconds) for `done` to hold.
async fn eventually(mut done: impl FnMut() -> bool) -> bool {
    for _ in 0..100 {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    done()
}

async fn edit(live: &mut Live, depth: i64) -> u64 {
    live.change(vec![set_depth(depth)], vec![], vec![])
        .await
        .seq()
}

/// The frame that carries `change`.
fn change_frame(change: oswx_core::Change) -> Vec<u8> {
    oswx_core::wire::encode_client(&oswx_core::wire::ClientMsg::Change(change)).to_vec()
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Records hook calls and events; refuses or hangs `before_org_delete` when told to.
#[derive(Default)]
struct Recorder {
    calls: Mutex<Vec<String>>,
    events: Mutex<Vec<Value>>,
    refuse: Mutex<Option<String>>,
    hang: AtomicBool,
}

impl Recorder {
    fn calls(&self) -> Vec<String> {
        lock(&self.calls).clone()
    }

    fn events(&self) -> Vec<Value> {
        lock(&self.events).clone()
    }

    fn event_types(&self) -> Vec<String> {
        self.events()
            .iter()
            .map(|e| e["type"].as_str().unwrap().to_owned())
            .collect()
    }
}

struct Recording(Arc<Recorder>);

#[async_trait]
impl Extensions for Recording {
    fn hook_timeout(&self) -> Duration {
        Duration::from_millis(200)
    }

    async fn member_removed(&self, org: &Id, user: &Id) {
        lock(&self.0.calls).push(format!("member_removed {org} {user}"));
    }

    async fn before_org_delete(&self, org: &Id) -> Result<(), String> {
        lock(&self.0.calls).push(format!("before_org_delete {org}"));
        if self.0.hang.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        match lock(&self.0.refuse).clone() {
            Some(why) => Err(why),
            None => Ok(()),
        }
    }

    async fn on_event(&self, event: &Event) {
        lock(&self.0.events).push(serde_json::to_value(event).unwrap());
    }
}

async fn recording() -> (TestServer, Arc<Recorder>) {
    let recorder = Arc::new(Recorder::default());
    let ext = Arc::new(Recording(recorder.clone()));
    let server = TestServer::start_custom(|_| {}, |s| s.ext = ext).await;
    (server, recorder)
}

/// A chunk that counts itself out when it is dropped.
struct Tracked {
    data: Bytes,
    outstanding: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.outstanding
            .fetch_sub(self.data.len(), Ordering::SeqCst);
    }
}

/// Blob storage for the export and clean-up tests. It delegates to a real one, and:
/// - serves blobs that were never stored, made up chunk by chunk as they are read ([`noise`]);
/// - counts the bytes blob reads handed out, and the most bytes of chunks alive at once;
/// - can hold blob reads back, fail them part-way, and fail removals;
/// - records what was removed.
struct ExportBlobs {
    inner: Arc<dyn Blobs>,
    made_up: Mutex<HashMap<String, u64>>,
    pulled: Arc<AtomicU64>,
    outstanding: Arc<AtomicUsize>,
    most_outstanding: Arc<AtomicUsize>,
    held: AtomicBool,
    released: tokio::sync::Notify,
    reads_started: AtomicUsize,
    /// A blob read fails once it has handed out this many bytes.
    fail_reads_after: AtomicU64,
    fail_removals: AtomicBool,
    removed: Mutex<Vec<String>>,
}

/// The size of the chunks a made-up blob comes in.
const NOISE_CHUNK: usize = 64 << 10;

/// The chunk of a made-up blob that starts at `offset`: bytes that do not compress.
fn noise(offset: u64, len: usize) -> Vec<u8> {
    let mut x = offset ^ 0x9e37_79b9_7f4a_7c15;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// The chunk lengths of a made-up blob of `len` bytes, with their offsets.
fn noise_chunks(len: u64) -> impl Iterator<Item = (u64, usize)> {
    (0..len.div_ceil(NOISE_CHUNK as u64)).map(move |i| {
        let offset = i * NOISE_CHUNK as u64;
        (
            offset,
            usize::try_from((len - offset).min(NOISE_CHUNK as u64)).unwrap(),
        )
    })
}

/// The SHA-256 of the made-up blob of `len` bytes.
fn noise_sha(len: u64) -> String {
    let mut hasher = Sha256::new();
    for (offset, n) in noise_chunks(len) {
        hasher.update(noise(offset, n));
    }
    hex::encode(hasher.finalize())
}

impl ExportBlobs {
    fn new(inner: Arc<dyn Blobs>) -> Self {
        Self {
            inner,
            made_up: Mutex::default(),
            pulled: Arc::default(),
            outstanding: Arc::default(),
            most_outstanding: Arc::default(),
            held: AtomicBool::new(false),
            released: tokio::sync::Notify::new(),
            reads_started: AtomicUsize::new(0),
            fail_reads_after: AtomicU64::new(u64::MAX),
            fail_removals: AtomicBool::new(false),
            removed: Mutex::default(),
        }
    }

    /// Makes up a blob of `len` bytes in `org` (nothing is stored) and answers its SHA-256.
    fn make_up(&self, org: &str, len: u64) -> String {
        let sha = noise_sha(len);
        lock(&self.made_up).insert(blob_key(org, &sha), len);
        sha
    }

    fn pulled(&self) -> u64 {
        self.pulled.load(Ordering::SeqCst)
    }

    fn most_outstanding(&self) -> usize {
        self.most_outstanding.load(Ordering::SeqCst)
    }

    /// Blob reads wait from now on.
    fn hold(&self) {
        self.held.store(true, Ordering::SeqCst);
    }

    /// Lets held blob reads go.
    fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
        self.released.notify_waiters();
    }

    fn reads_started(&self) -> usize {
        self.reads_started.load(Ordering::SeqCst)
    }

    fn removed(&self) -> Vec<String> {
        lock(&self.removed).clone()
    }

    /// `chunks`, counted, and failing once `fail_reads_after` bytes have passed.
    fn metered(&self, chunks: ByteStream) -> ByteStream {
        let (pulled, outstanding, most) = (
            self.pulled.clone(),
            self.outstanding.clone(),
            self.most_outstanding.clone(),
        );
        let fail_after = self.fail_reads_after.load(Ordering::SeqCst);
        let mut passed = 0u64;
        chunks
            .map(move |chunk| {
                let chunk = chunk?;
                passed += chunk.len() as u64;
                if passed > fail_after {
                    return Err(io::Error::other("the test storage was told to fail reads"));
                }
                pulled.fetch_add(chunk.len() as u64, Ordering::SeqCst);
                let now = outstanding.fetch_add(chunk.len(), Ordering::SeqCst) + chunk.len();
                most.fetch_max(now, Ordering::SeqCst);
                Ok(Bytes::from_owner(Tracked {
                    data: chunk,
                    outstanding: outstanding.clone(),
                }))
            })
            .boxed()
    }
}

#[async_trait]
impl Blobs for ExportBlobs {
    async fn put_stream(
        &self,
        key: &str,
        body: ByteStream,
        limit: u64,
    ) -> anyhow::Result<PutOutcome> {
        self.inner.put_stream(key, body, limit).await
    }

    async fn get_stream(&self, key: &str) -> anyhow::Result<Option<(u64, ByteStream)>> {
        self.reads_started.fetch_add(1, Ordering::SeqCst);
        loop {
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if !self.held.load(Ordering::SeqCst) {
                break;
            }
            released.await;
        }
        if let Some(len) = lock(&self.made_up).get(key).copied() {
            let chunks = stream::iter(noise_chunks(len))
                .map(|(offset, n)| Ok(Bytes::from(noise(offset, n))))
                .boxed();
            return Ok(Some((len, self.metered(chunks))));
        }
        Ok(self
            .inner
            .get_stream(key)
            .await?
            .map(|(len, chunks)| (len, self.metered(chunks))))
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        if lock(&self.made_up).contains_key(key) {
            return Ok(true);
        }
        self.inner.exists(key).await
    }

    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        if self.fail_removals.load(Ordering::SeqCst) {
            anyhow::bail!("the test storage was told to fail removals");
        }
        lock(&self.removed).push(prefix.to_owned());
        self.inner.delete_prefix(prefix).await
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        if self.fail_removals.load(Ordering::SeqCst) {
            anyhow::bail!("the test storage was told to fail removals");
        }
        lock(&self.removed).push(key.to_owned());
        self.inner.delete(key).await
    }

    async fn health(&self) -> anyhow::Result<()> {
        self.inner.health().await
    }
}

/// A server whose blob storage is an [`ExportBlobs`], with these export limits.
async fn with_export_blobs(exports: Exports) -> (TestServer, Arc<ExportBlobs>) {
    let mut handle = None;
    let server = TestServer::start_custom(
        |_| {},
        |s| {
            let blobs = Arc::new(ExportBlobs::new(s.blobs.clone()));
            handle = Some(blobs.clone());
            s.blobs = blobs;
            s.exports = Arc::new(exports);
        },
    )
    .await;
    (server, handle.expect("the state function ran"))
}

// ------------------------------------------------------------------ deleting an account

#[tokio::test]
async fn deleting_an_account_keeps_the_work_and_frees_the_address() {
    let (server, world) = world().await;
    let (bob, bob_id) = world
        .invite(
            &server,
            "bob@example.com",
            Some("Bob"),
            Some("editor"),
            "member",
        )
        .await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(edit(&mut b, 20).await, 1);
    let (status, _) = server
        .post(
            &format!("/api/documents/{doc}/versions"),
            Some(&bob),
            json!({"name": "Bob's rev"}),
        )
        .await;
    assert_eq!(status, 201);
    b.drain().await;
    b.seen.clear();

    assert_eq!(
        delete_me(&server, &bob, json!({"current_password": "wrong"})).await,
        (
            StatusCode::FORBIDDEN,
            json!({"detail": "the current password is wrong"})
        )
    );
    assert_eq!(
        delete_me(&server, &bob, confirmed()).await,
        (StatusCode::NO_CONTENT, Value::Null)
    );
    // signed out everywhere, open documents included
    assert_eq!(server.get("/api/me", Some(&bob)).await.0, 401);
    assert_eq!(b.closed().await, (4401, "sign in again".to_owned()));
    let (status, _) = server
        .post(
            "/api/auth/login",
            None,
            json!({"email": "bob@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(status, 401);

    // the work stays, credited to "Deleted user"
    let admin = Some(world.admin.as_str());
    let (_, history) = server
        .get(&format!("/api/documents/{doc}/history"), admin)
        .await;
    assert_eq!(history[0]["author_name"], DELETED);
    assert_eq!(history[0]["author"], bob_id.as_str());
    assert_eq!(history[0]["seq"], 1);
    let (_, versions) = server
        .get(&format!("/api/documents/{doc}/versions"), admin)
        .await;
    assert_eq!(versions[0]["author"]["name"], DELETED);
    let (_, content) = server
        .get(&format!("/api/documents/{doc}/content"), admin)
        .await;
    assert_eq!(content["content"]["features"][1]["params"]["depth"], 20);
    let (_, members) = server
        .get(&format!("/api/orgs/{}/members", world.org), admin)
        .await;
    let member_names: Vec<&str> = members
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert_eq!(member_names, ["Ada"]);
    // open documents show the new name too
    let a = Live::connect_with(&server, &doc, &world.admin, Hello::default().last_seq(0)).await;
    assert_eq!(a.welcome.changes.as_ref().unwrap()[0].author_name, DELETED);

    // the row is emptied, not removed
    let store = &server.state.store;
    let row = store
        .user_by_id(&bob_id)
        .await
        .unwrap()
        .expect("the row stays");
    assert_eq!(row.email, format!("deleted-{bob_id}@deleted.invalid"));
    assert_eq!(row.name, DELETED);
    assert_eq!(row.password_hash, None);
    assert!(row.disabled && row.deleted_at.is_some());
    assert!(!row.is_server_admin && !row.email_verified);
    assert!(
        store
            .session_by_hash(&token_hash(&bob))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .org_member(&world.org, &bob_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .project_member(&world.project, &bob_id)
            .await
            .unwrap()
            .is_none()
    );

    // the address is free again
    let (new_bob, new_id) = world
        .invite(
            &server,
            "bob@example.com",
            Some("Bob again"),
            None,
            "member",
        )
        .await;
    assert_ne!(new_id, bob_id);
    assert_eq!(server.get("/api/me", Some(&new_bob)).await.0, 200);
}

#[tokio::test]
async fn an_account_cannot_leave_an_organisation_or_the_server_without_an_owner() {
    let (server, world) = world().await;
    assert_eq!(
        delete_me(&server, &world.admin, confirmed()).await,
        (
            StatusCode::CONFLICT,
            json!({"detail": {
                "message": "you are the only owner of these organisations: make someone else an \
                            owner, or delete them, first",
                "orgs": [{"id": world.org, "name": "Robotics"}],
            }})
        )
    );
    let (_, bob_id) = world
        .invite(&server, "bob@example.com", None, None, "admin")
        .await;
    let (status, _) = server
        .patch(
            &format!("/api/orgs/{}/members/{bob_id}", world.org),
            Some(&world.admin),
            json!({"role": "owner"}),
        )
        .await;
    assert_eq!(status, 200);
    // the server's only admin, while others exist
    assert_eq!(
        delete_me(&server, &world.admin, confirmed()).await,
        (
            StatusCode::CONFLICT,
            json!({"detail": "you are this server's only admin; make someone else admin first \
                              (openswx-collab-server create-admin)"})
        )
    );
    // nothing was changed by the refusals
    assert_eq!(server.get("/api/me", Some(&world.admin)).await.0, 200);
    let row = server
        .state
        .store
        .user_by_id(&world.admin_id)
        .await
        .unwrap()
        .unwrap();
    assert!(row.active() && row.is_server_admin);
    assert_eq!(row.email, "ada@example.com");
}

#[tokio::test]
async fn the_last_person_on_a_server_can_leave() {
    let server = TestServer::start().await;
    let (ada, _) = server.register("ada@example.com").await;
    assert_eq!(delete_me(&server, &ada, confirmed()).await.0, 204);
    // the account row is still there
    assert_eq!(
        server.get("/api/server", None).await.1["registration"],
        "invite"
    );
}

#[tokio::test]
async fn a_deleted_account_cannot_be_deleted_again_or_used() {
    let (server, world) = world().await;
    let (bob, _) = world.editor(&server, "bob@example.com").await;
    let laptop = server.login("bob@example.com", PASSWORD).await;
    assert_eq!(delete_me(&server, &bob, confirmed()).await.0, 204);
    for token in [&bob, &laptop] {
        assert_eq!(
            delete_me(&server, token, confirmed()).await,
            (StatusCode::UNAUTHORIZED, json!({"detail": "sign in first"}))
        );
        assert_eq!(server.get("/api/me/export", Some(token)).await.0, 401);
        assert_eq!(server.get("/api/orgs", Some(token)).await.0, 401);
    }
    assert_eq!(server.state.hub.recorded().len(), 1, "one drop_account");
}

#[tokio::test]
async fn deleting_an_account_closes_its_connections_and_nothing_more_is_written() {
    let (server, world) = world().await;
    let (bob, bob_id) = world.editor(&server, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(edit(&mut b, 20).await, 1);
    a.drain().await;
    assert_eq!(delete_me(&server, &bob, confirmed()).await.0, 204);
    assert_eq!(b.closed().await, (4401, "sign in again".to_owned()));
    assert_eq!(
        server.state.hub.recorded(),
        [format!("drop_account {bob_id}")]
    );
    // A change sent on the closed connection goes nowhere, and the account cannot connect again.
    let late = b.make_change(1, vec![set_depth(99)]);
    b.send_raw_lossy(change_frame(late)).await;
    match Live::try_connect(&server, &doc, &bob, Hello::default()).await {
        Ok(_) => panic!("a deleted account connected"),
        Err(refused) => assert_eq!(refused.close, Some(4401), "{refused:?}"),
    }
    // The other person is still there, and sees only their own change next.
    assert_eq!(edit(&mut a, 30).await, 2);
    let store = &server.state.store;
    assert_eq!(
        store.document_by_id(&doc).await.unwrap().unwrap().head_seq,
        2
    );
    let second = store.change_by_seq(&doc, 2).await.unwrap().unwrap();
    assert_eq!(second.author_id, world.admin_id);
    assert!(store.change_by_seq(&doc, 3).await.unwrap().is_none());
}

#[tokio::test]
async fn account_deletion_refuses_what_it_should_not_trust() {
    let (server, world) = world().await;
    let (bob, bob_id) = world.editor(&server, "bob@example.com").await;
    let url = server.url("/api/me");
    let http = reqwest::Client::new();
    // nobody
    let r = http.delete(&url).json(&confirmed()).send().await.unwrap();
    assert_eq!(r.status(), 401);
    // no body at all, and a body that is not JSON
    let r = http.delete(&url).bearer_auth(&bob).send().await.unwrap();
    assert!(r.status().is_client_error(), "{}", r.status());
    let r = http
        .delete(&url)
        .bearer_auth(&bob)
        .header(header::CONTENT_TYPE, "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    // wrong types and sizes
    for body in [
        json!({"current_password": 7}),
        json!({"current_password": ["correct horse"]}),
        json!({"current_password": "x".repeat(1025)}),
        json!([]),
    ] {
        assert_eq!(
            delete_me(&server, &bob, body.clone()).await.0,
            422,
            "{body}"
        );
    }
    // no password given, or none at all
    for body in [
        json!({}),
        json!({"current_password": null}),
        json!({"current_password": ""}),
    ] {
        assert_eq!(
            delete_me(&server, &bob, body.clone()).await.0,
            403,
            "{body}"
        );
    }
    // a body over the limit
    let r = http
        .delete(&url)
        .bearer_auth(&bob)
        .json(&json!({"current_password": "x", "padding": "y".repeat(2 << 20)}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 413);
    // none of it deleted anything
    assert!(
        server
            .state
            .store
            .user_by_id(&bob_id)
            .await
            .unwrap()
            .unwrap()
            .active()
    );
    assert!(server.state.hub.recorded().is_empty());
}

#[tokio::test]
async fn an_account_without_a_password_needs_a_recent_sign_in_to_be_deleted() {
    let clock = Clock::manual(1_800_000_000.0);
    let c = clock.clone();
    let server = TestServer::start_custom(|_| {}, move |s| s.clock = c).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = world.editor(&server, "bob@example.com").await;
    server
        .sql(&format!(
            "UPDATE users SET password_hash = NULL WHERE id = '{bob_id}'"
        ))
        .await;
    clock.advance(601.0);
    assert_eq!(
        delete_me(&server, &bob, confirmed()).await,
        (
            StatusCode::FORBIDDEN,
            json!({"detail": "sign in again first (this needs a sign-in from the last ten minutes)"})
        )
    );
    // A session of the last ten minutes will do (made by hand: the account has no password).
    let fresh = "a-fresh-session-token";
    server
        .sql(&format!(
            "INSERT INTO sessions (token_hash, user_id, created_at, expires_at, label) \
             VALUES ('{}', '{bob_id}', {}, {}, '')",
            token_hash(fresh),
            clock.now(),
            clock.now() + 3600.0
        ))
        .await;
    assert_eq!(delete_me(&server, fresh, json!({})).await.0, 204);
}

#[tokio::test]
async fn deleting_an_account_tells_the_extensions_with_ids_only() {
    let (server, recorder) = recording().await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = world
        .invite(
            &server,
            "bob@example.com",
            Some("Bob Builder"),
            Some("editor"),
            "member",
        )
        .await;
    lock(&recorder.events).clear();
    assert_eq!(delete_me(&server, &bob, confirmed()).await.0, 204);
    assert_eq!(
        recorder.calls(),
        [format!("member_removed {} {bob_id}", world.org)]
    );
    assert_eq!(
        recorder.events(),
        [
            json!({"type": "member_removed", "org": world.org, "user": bob_id, "by": bob_id}),
            json!({"type": "account_deleted", "user": bob_id}),
        ]
    );
    let told = serde_json::to_string(&recorder.events()).unwrap();
    assert!(
        !told.contains("Bob") && !told.contains("example.com"),
        "{told}"
    );
}

#[tokio::test]
async fn a_commit_that_may_have_failed_still_closes_the_accounts_connections() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = world.editor(&server, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    b.drain().await;
    flaky.fail_commit(true);
    assert_eq!(delete_me(&server, &bob, confirmed()).await.0, 500);
    flaky.fail_commit(false);
    // Whether the deletion took effect is not known: the connection is told to reconnect.
    assert_eq!(b.closed().await, (4410, "reconnect".to_owned()));
    // It did not: the account is whole and signs in.
    let row = server
        .state
        .store
        .user_by_id(&bob_id)
        .await
        .unwrap()
        .unwrap();
    assert!(row.active());
    assert_eq!(row.email, "bob@example.com");
    assert_eq!(server.get("/api/me", Some(&bob)).await.0, 200);
}

// ------------------------------------------------------------------ your own data

#[tokio::test]
async fn your_own_data_export() {
    let (server, world) = world().await;
    let (bob, bob_id) = world
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
    assert_eq!(edit(&mut a, 4).await, 1);
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(edit(&mut b, 5).await, 2);
    let (status, _) = server
        .post(
            &format!("/api/documents/{doc}/versions"),
            Some(&bob),
            json!({"name": "Rev B"}),
        )
        .await;
    assert_eq!(status, 201);

    let (status, data) = server.get("/api/me/export", Some(&bob)).await;
    assert_eq!(status, 200, "{data}");
    assert_eq!(data["account"]["id"], bob_id.as_str());
    assert_eq!(data["account"]["email"], "bob@example.com");
    assert_eq!(data["account"]["name"], "Bob");
    assert_eq!(data["account"]["has_password"], true);
    assert_eq!(data["account"]["server_admin"], false);
    assert_eq!(data["account"]["email_verified"], false);
    assert_eq!(data["account"]["sign_in_providers"], json!([]));
    assert!(data["account"]["created_at"].is_f64());
    assert_eq!(
        data["organisations"],
        json!([{"id": world.org, "name": "Robotics", "role": "member",
                "since": data["organisations"][0]["since"]}])
    );
    assert!(data["organisations"][0]["since"].is_f64());
    assert_eq!(
        data["projects"],
        json!([{"id": world.project, "name": "Rover", "org_id": world.org, "role": "editor"}])
    );
    // only what Bob did
    let changes = data["changes_you_made"].as_array().unwrap();
    assert_eq!(changes.len(), 1, "{changes:?}");
    assert_eq!(changes[0]["document_id"], doc.as_str());
    assert_eq!(changes[0]["seq"], 2);
    assert!(changes[0]["time"].is_f64() && changes[0]["command"].is_string());
    let versions = data["versions_you_named"].as_array().unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0]["name"], "Rev B");
    assert_eq!(versions[0]["document_id"], doc.as_str());
    assert_eq!(versions[0]["seq"], 2);
    assert!(data["exported_at"].is_f64());
    // tokens are never handed back, not even hashed
    let sessions = data["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1);
    let mut keys: Vec<&String> = sessions[0].as_object().unwrap().keys().collect();
    keys.sort();
    assert_eq!(keys, ["created_at", "expires_at", "label"]);
    let text = data.to_string();
    assert!(!text.contains(&bob) && !text.contains(&token_hash(&bob)));
    assert!(!text.contains("password_hash"));

    assert_eq!(
        server.get("/api/me/export", None).await,
        (StatusCode::UNAUTHORIZED, json!({"detail": "sign in first"}))
    );
}

#[tokio::test]
async fn your_own_data_export_is_read_a_page_at_a_time_and_misses_nothing() {
    let (server, _) = with_export_blobs(Exports::new(2, MINUTE, HOUR, 2)).await;
    let world = World::new(&server).await.unwrap();
    let one = world.document(&server, None, Some("One"), None).await;
    let two = world.document(&server, None, Some("Two"), None).await;
    let mut expected = Vec::new();
    for doc in [&one, &two] {
        let mut live = Live::connect(&server, doc, &world.admin).await;
        for n in 1..=5 {
            assert_eq!(edit(&mut live, n).await, n as u64);
            expected.push((doc.clone(), n));
        }
        for n in 0..3 {
            let (status, _) = server
                .post(
                    &format!("/api/documents/{doc}/versions"),
                    Some(&world.admin),
                    json!({"name": format!("V{n}"), "seq": n}),
                )
                .await;
            assert_eq!(status, 201);
        }
    }
    expected.sort();
    let (status, data) = server.get("/api/me/export", Some(&world.admin)).await;
    assert_eq!(status, 200);
    // by document, then seq
    let got: Vec<(String, i64)> = data["changes_you_made"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["document_id"].as_str().unwrap().to_owned(),
                c["seq"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(got, expected);
    assert_eq!(data["versions_you_named"].as_array().unwrap().len(), 6);
}

// ------------------------------------------------------------------ exporting an organisation

#[tokio::test]
async fn an_organisation_export_has_everything_in_it() {
    let (server, world) = world().await;
    let body = "Z".repeat(30_000).into_bytes();
    let sha = upload(&server, &world.project, &world.admin, &body).await;
    let doc = world
        .document(
            &server,
            Some(part_with_blob(&sha, body.len() as u64)),
            None,
            None,
        )
        .await;
    let (bob, bob_id) = world
        .invite(
            &server,
            "bob@example.com",
            Some("Bob"),
            Some("editor"),
            "member",
        )
        .await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(edit(&mut b, 42).await, 1);
    let (status, _) = server
        .post(
            &format!("/api/documents/{doc}/versions"),
            Some(&bob),
            json!({"name": "Rev A", "note": "first"}),
        )
        .await;
    assert_eq!(status, 201);
    // a deleted document is left out
    let gone = world.document(&server, None, Some("Gone"), None).await;
    assert_eq!(
        server
            .delete(&format!("/api/documents/{gone}"), Some(&world.admin))
            .await
            .0,
        204
    );

    // owners only
    let r = export(&server, &world.org, Some(&bob)).await;
    assert_eq!(r.status(), 403);
    assert_eq!(
        r.json::<Value>().await.unwrap(),
        json!({"detail": "needs the organisation role owner"})
    );

    let r = export(&server, &world.org, Some(&world.admin)).await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()[header::CONTENT_TYPE], "application/zip");
    assert_eq!(
        r.headers()[header::CONTENT_DISPOSITION],
        "attachment; filename=\"Robotics-export.zip\""
    );
    let bytes = r.bytes().await.unwrap().to_vec();
    let mut z = zip::ZipArchive::new(io::Cursor::new(bytes)).expect("a zip");
    let base = format!("projects/{}/{doc}", world.project);
    assert_eq!(
        names(&z),
        [
            format!("blobs/{sha}"),
            "organisation.json".to_owned(),
            format!("{base}/document.json"),
            format!("{base}/history.jsonl"),
            format!("{base}/versions.json"),
        ]
    );
    let d = entry_json(&mut z, &format!("{base}/document.json"));
    assert_eq!(d["id"], doc.as_str());
    assert_eq!(d["name"], "Bracket");
    assert_eq!(d["kind"], "part");
    assert_eq!(d["seq"], 1);
    assert_eq!(d["format_version"], 3);
    assert_eq!(d["content"]["features"][1]["params"]["depth"], 42);
    assert_eq!(
        d["content"]["features"][2]["params"]["brep"]["$blob"],
        sha.as_str()
    );
    let mut keys: Vec<&String> = d.as_object().unwrap().keys().collect();
    keys.sort();
    assert_eq!(
        keys,
        ["content", "format_version", "id", "kind", "name", "seq"]
    );

    // history: one line per change, in the shape of GET .../history
    let lines = entry_lines(&mut z, &format!("{base}/history.jsonl"));
    let (_, listed) = server
        .get(&format!("/api/documents/{doc}/history"), Some(&world.admin))
        .await;
    assert_eq!(Value::Array(lines.clone()), listed);
    assert_eq!(lines[0]["author_name"], "Bob");
    assert_eq!(lines[0]["author"], bob_id.as_str());

    let versions = entry_json(&mut z, &format!("{base}/versions.json"));
    assert_eq!(
        versions,
        json!([{"name": "Rev A", "seq": 1, "note": "first", "author": bob_id,
                "author_name": "Bob", "time": versions[0]["time"]}])
    );
    assert!(versions[0]["time"].is_f64());
    assert_eq!(entry(&mut z, &format!("blobs/{sha}")), body);

    let org = entry_json(&mut z, "organisation.json");
    assert_eq!(org["id"], world.org.as_str());
    assert_eq!(org["name"], "Robotics");
    assert!(org["created_at"].is_f64() && org["exported_at"].is_f64());
    assert_eq!(
        org["members"],
        json!([
            {"user": "Ada", "email": "ada@example.com", "role": "owner"},
            {"user": "Bob", "email": "bob@example.com", "role": "member"},
        ])
    );
    assert_eq!(
        org["projects"],
        json!([{"id": world.project, "name": "Rover"}])
    );
    // as the Python server wrote it: indented by one space
    let raw = String::from_utf8(entry(&mut z, "organisation.json")).unwrap();
    assert!(raw.starts_with("{\n \""), "{raw}");
}

#[tokio::test]
async fn an_export_of_an_open_document_has_its_latest_durable_change() {
    let (server, world) = world().await;
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    for n in 1..=3 {
        assert_eq!(edit(&mut a, 10 * n).await, n as u64);
    }
    // The room is open and has written no snapshot since the document was made.
    assert!(server.state.hub.loaded(&doc).is_some());
    let mut z = export_zip(&server, &world.org, &world.admin).await;
    let base = format!("projects/{}/{doc}", world.project);
    let d = entry_json(&mut z, &format!("{base}/document.json"));
    assert_eq!(d["seq"], 3);
    assert_eq!(d["content"]["features"][1]["params"]["depth"], 30);
    let seqs: Vec<i64> = entry_lines(&mut z, &format!("{base}/history.jsonl"))
        .iter()
        .map(|l| l["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(seqs, [1, 2, 3]);
    // The export did not disturb the room.
    assert_eq!(edit(&mut a, 77).await, 4);
}

#[tokio::test]
async fn an_export_reads_history_a_page_at_a_time_and_finds_blobs_only_history_names() {
    let (server, _) = with_export_blobs(Exports::new(2, MINUTE, HOUR, 2)).await;
    let world = World::new(&server).await.unwrap();
    let body = b"only the history knows this one".to_vec();
    let sha = upload(&server, &world.project, &world.admin, &body).await;
    let doc = world.document(&server, None, None, None).await;
    let empty = world.document(&server, None, Some("Untouched"), None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    // A reference set, then overwritten: the document no longer names the blob.
    let op = set(
        &["features", "f2", "params", "brep"],
        json!({"$blob": sha, "len": body.len()}),
    );
    assert_eq!(a.change(vec![op], vec![], vec![]).await.seq(), 1);
    let op = set(&["features", "f2", "params", "brep"], json!(null));
    assert_eq!(a.change(vec![op], vec![], vec![]).await.seq(), 2);
    for n in 3..=7 {
        assert_eq!(edit(&mut a, n).await, n as u64);
    }
    let mut z = export_zip(&server, &world.org, &world.admin).await;
    let base = format!("projects/{}", world.project);
    let seqs: Vec<i64> = entry_lines(&mut z, &format!("{base}/{doc}/history.jsonl"))
        .iter()
        .map(|l| l["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(seqs, [1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(entry(&mut z, &format!("blobs/{sha}")), body);
    // A document nobody changed has an empty history and no versions.
    assert_eq!(entry(&mut z, &format!("{base}/{empty}/history.jsonl")), b"");
    assert_eq!(
        entry_json(&mut z, &format!("{base}/{empty}/versions.json")),
        json!([])
    );
    assert_eq!(
        entry_json(&mut z, &format!("{base}/{empty}/document.json"))["seq"],
        0
    );
}

#[tokio::test]
async fn an_export_is_for_owners_of_that_organisation_only() {
    let (server, recorder) = recording().await;
    let world = World::new(&server).await.unwrap();
    let (admin, _) = world
        .invite(&server, "cy@example.com", None, None, "admin")
        .await;
    let (member, _) = world.editor(&server, "bob@example.com").await;
    // Someone else's organisation.
    let (status, other) = server
        .post("/api/orgs", Some(&world.admin), json!({"name": "Other"}))
        .await;
    assert_eq!(status, 201);
    let other = other["id"].as_str().unwrap();
    lock(&recorder.events).clear();

    let no_such = json!({"detail": "no such organisation"});
    let r = export(&server, &world.org, None).await;
    assert_eq!(r.status(), 401);
    for token in [&admin, &member] {
        let r = export(&server, &world.org, Some(token)).await;
        assert_eq!(r.status(), 403);
        // An organisation they are not in does not exist for them.
        let r = export(&server, other, Some(token)).await;
        assert_eq!(r.status(), 404);
        assert_eq!(r.json::<Value>().await.unwrap(), no_such);
    }
    for id in ["0".repeat(32), "nope".to_owned(), "x".repeat(5000)] {
        let r = export(&server, &id, Some(&world.admin)).await;
        assert_eq!(r.status(), 404, "{id}");
        assert_eq!(r.json::<Value>().await.unwrap(), no_such);
    }
    assert!(recorder.events().is_empty(), "no export began");

    let z = export_zip(&server, &world.org, &world.admin).await;
    assert_eq!(names(&z), ["organisation.json"]);
    assert_eq!(
        recorder.events(),
        [json!({"type": "org_exported", "org": world.org, "by": world.admin_id})]
    );
}

/// Renames the organisation and answers the `Content-Disposition` of its export.
async fn disposition(server: &TestServer, world: &World, name: &str) -> String {
    let (status, _) = server
        .patch(
            &format!("/api/orgs/{}", world.org),
            Some(&world.admin),
            json!({"name": name}),
        )
        .await;
    assert_eq!(status, 200);
    let r = export(server, &world.org, Some(&world.admin)).await;
    assert_eq!(r.status(), 200);
    let value = r.headers()[header::CONTENT_DISPOSITION]
        .to_str()
        .unwrap()
        .to_owned();
    r.bytes().await.unwrap();
    value
}

#[tokio::test]
async fn an_export_names_its_file_after_the_organisation_safely() {
    let (server, world) = world().await;
    assert_eq!(
        disposition(&server, &world, "R&D \"lab\"/..\\x\r\n").await,
        "attachment; filename=\"R-D--lab-----x-export.zip\""
    );
    assert_eq!(
        disposition(&server, &world, "Caf\u{e9} 9").await,
        "attachment; filename*=utf-8''Caf%C3%A9-9-export.zip"
    );
    assert_eq!(
        disposition(&server, &world, &"a".repeat(200)).await,
        format!("attachment; filename=\"{}-export.zip\"", "a".repeat(60))
    );
}

#[tokio::test]
async fn an_export_of_a_64_mb_blob_never_holds_the_blob() {
    const LEN: u64 = 64 << 20;
    let (server, blobs) = with_export_blobs(Exports::default()).await;
    let world = World::new(&server).await.unwrap();
    let sha = blobs.make_up(&world.org, LEN);
    world
        .document(&server, Some(part_with_blob(&sha, LEN)), None, None)
        .await;

    let mut r = export(&server, &world.org, Some(&world.admin)).await;
    assert_eq!(r.status(), 200);
    // The client reads nothing yet: the export stops taking the blob from storage once the
    // buffers between them are full, long before the blob has passed.
    let mut pulled = blobs.pulled();
    loop {
        pause(Duration::from_millis(300)).await;
        let now = blobs.pulled();
        if now == pulled && now > 0 {
            break;
        }
        pulled = now;
    }
    assert!(
        pulled < 16 << 20,
        "{pulled} bytes were taken from storage while the client read none"
    );

    // Now it reads, a piece at a time, into a file.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("export.zip");
    let mut file = std::fs::File::create(&path).unwrap();
    while let Some(chunk) = r.chunk().await.expect("the export continues") {
        file.write_all(&chunk).unwrap();
    }
    drop(file);
    assert_eq!(blobs.pulled(), LEN);
    // By construction: of the chunks storage handed out, never more than a few were alive at once.
    assert!(
        blobs.most_outstanding() <= 4 * NOISE_CHUNK,
        "{} bytes of the blob were held at once",
        blobs.most_outstanding()
    );

    let mut z = zip::ZipArchive::new(std::fs::File::open(&path).unwrap()).expect("a zip");
    let mut blob = z
        .by_name(&format!("blobs/{sha}"))
        .expect("the blob's entry");
    assert_eq!(blob.size(), LEN);
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = blob.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    assert_eq!(hex::encode(hasher.finalize()), sha);
}

#[tokio::test]
async fn only_so_many_exports_run_at_once() {
    let (server, blobs) = with_export_blobs(Exports::new(1, MINUTE, HOUR, 500)).await;
    let world = World::new(&server).await.unwrap();
    let body = b"held back".to_vec();
    let sha = upload(&server, &world.project, &world.admin, &body).await;
    world
        .document(
            &server,
            Some(part_with_blob(&sha, body.len() as u64)),
            None,
            None,
        )
        .await;
    let (bob, _) = world.editor(&server, "bob@example.com").await;
    blobs.hold();
    let first = export(&server, &world.org, Some(&world.admin)).await;
    assert_eq!(first.status(), 200);
    assert!(eventually(|| blobs.reads_started() == 1).await);
    // One is running: the next, someone else's, is refused and holds nothing.
    assert_eq!(
        server.get("/api/me/export", Some(&bob)).await,
        (
            StatusCode::TOO_MANY_REQUESTS,
            json!({"detail": "too many exports are running; try again in a moment"})
        )
    );
    assert_eq!(server.state.exports.running(), 1);
    // Someone who may not export is told so, not that the server is busy.
    assert_eq!(export(&server, &world.org, Some(&bob)).await.status(), 403);

    blobs.release();
    let bytes = first.bytes().await.expect("the first export finishes");
    let mut z = zip::ZipArchive::new(io::Cursor::new(bytes.to_vec())).unwrap();
    assert_eq!(entry(&mut z, &format!("blobs/{sha}")), body);
    // Its place is free again.
    let mut free = false;
    for _ in 0..100 {
        let (status, _) = server.get("/api/me/export", Some(&world.admin)).await;
        if status == 200 {
            free = true;
            break;
        }
        pause(Duration::from_millis(50)).await;
    }
    assert!(free, "the finished export gave its place back");
    export_zip(&server, &world.org, &world.admin).await;
}

#[tokio::test]
async fn a_client_that_goes_away_frees_its_export() {
    let (server, blobs) = with_export_blobs(Exports::new(1, MINUTE, HOUR, 500)).await;
    let world = World::new(&server).await.unwrap();
    let body = b"never sent".to_vec();
    let sha = upload(&server, &world.project, &world.admin, &body).await;
    world
        .document(
            &server,
            Some(part_with_blob(&sha, body.len() as u64)),
            None,
            None,
        )
        .await;
    blobs.hold();
    let r = export(&server, &world.org, Some(&world.admin)).await;
    assert_eq!(r.status(), 200);
    assert!(eventually(|| blobs.reads_started() == 1).await);
    assert_eq!(server.state.exports.running(), 1);
    // The client goes away while the export waits for storage: it is stopped where it is.
    drop(r);
    assert!(
        eventually(|| server.state.exports.running() == 0).await,
        "the abandoned export gave its place back"
    );
    blobs.release();
    export_zip(&server, &world.org, &world.admin).await;
}

#[tokio::test]
async fn a_client_that_stops_reading_an_export_loses_it() {
    const LEN: u64 = 32 << 20;
    let (server, blobs) =
        with_export_blobs(Exports::new(1, Duration::from_millis(300), HOUR, 500)).await;
    let world = World::new(&server).await.unwrap();
    let sha = blobs.make_up(&world.org, LEN);
    world
        .document(&server, Some(part_with_blob(&sha, LEN)), None, None)
        .await;
    let logs = LogCapture::start();
    let mut stuck = export(&server, &world.org, Some(&world.admin)).await;
    assert_eq!(stuck.status(), 200);
    // It reads nothing. The export gives up, and its place is free for another.
    let mut freed = false;
    for _ in 0..100 {
        if server.get("/api/me/export", Some(&world.admin)).await.0 == 200 {
            freed = true;
            break;
        }
        pause(Duration::from_millis(50)).await;
    }
    assert!(freed, "the stalled export gave its place back");
    assert!(blobs.pulled() < LEN, "the blob was not read to its end");
    assert_eq!(
        blobs.outstanding.load(Ordering::SeqCst),
        0,
        "nothing is kept"
    );
    // What the client finds when it reads again is an export cut short, not a whole one.
    let mut got = 0u64;
    let ended = loop {
        match stuck.chunk().await {
            Ok(Some(chunk)) => got += chunk.len() as u64,
            Ok(None) => break Ok(()),
            Err(e) => break Err(e),
        }
    };
    assert!(
        ended.is_err(),
        "the response ended cleanly after {got} bytes"
    );
    assert!(got < LEN);
    let text = logs.text();
    assert!(text.contains("stopped reading"), "{text}");
    logs.assert_never_logged(&["Robotics", "ada@example.com", "Ada"]);
}

#[tokio::test]
async fn an_export_that_fails_part_way_is_cut_short_and_logged() {
    let (server, blobs) = with_export_blobs(Exports::new(1, MINUTE, HOUR, 500)).await;
    let world = World::new(&server).await.unwrap();
    let sha = blobs.make_up(&world.org, 4 << 20);
    world
        .document(&server, Some(part_with_blob(&sha, 4 << 20)), None, None)
        .await;
    blobs.fail_reads_after.store(1 << 20, Ordering::SeqCst);
    let logs = LogCapture::start();
    let r = export(&server, &world.org, Some(&world.admin)).await;
    assert_eq!(r.status(), 200, "the failure comes after the answer began");
    let whole = r.bytes().await;
    assert!(whole.is_err(), "the response ended as if it were whole");
    let text = logs.text();
    assert!(
        text.contains("failed") && text.contains(&world.org),
        "{text}"
    );
    logs.assert_never_logged(&["Robotics", "ada@example.com", "Ada", "Bracket"]);
    // Its place is free again, and a later export works.
    blobs.fail_reads_after.store(u64::MAX, Ordering::SeqCst);
    assert!(eventually(|| server.state.exports.running() == 0).await);
    let z = export_zip(&server, &world.org, &world.admin).await;
    assert!(names(&z).contains(&format!("blobs/{sha}")));
}

#[tokio::test]
async fn an_export_leaves_out_blobs_the_storage_does_not_have() {
    let (server, world) = world().await;
    let body = b"here today".to_vec();
    let sha = upload(&server, &world.project, &world.admin, &body).await;
    world
        .document(
            &server,
            Some(part_with_blob(&sha, body.len() as u64)),
            None,
            None,
        )
        .await;
    server
        .state
        .blobs
        .delete(&blob_key(&world.org, &sha))
        .await
        .unwrap();
    let z = export_zip(&server, &world.org, &world.admin).await;
    assert!(!names(&z).iter().any(|n| n.starts_with("blobs/")));
}

// ------------------------------------------------------------------ deleting an organisation

#[tokio::test]
async fn deleting_an_organisation_removes_everything_in_it() {
    let (server, world) = world().await;
    let body = "Q".repeat(30_000).into_bytes();
    upload(&server, &world.project, &world.admin, &body).await;
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
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(edit(&mut b, 12).await, 1);
    b.drain().await;
    b.seen.clear();
    let (_, other) = server
        .post("/api/orgs", Some(&world.admin), json!({"name": "Other"}))
        .await;
    let other = other["id"].as_str().unwrap().to_owned();
    let org_files = storage(&server).join("orgs").join(&world.org);
    let doc_files = storage(&server).join("docs").join(&doc);
    assert!(org_files.is_dir() && doc_files.is_dir());

    let bad = [
        (
            json!({"confirm_name": "Robotics"}),
            &bob,
            403,
            "needs the organisation role owner",
        ),
        (
            json!({"confirm_name": "robotics", "current_password": PASSWORD}),
            &world.admin,
            422,
            "type the organisation's name exactly to confirm",
        ),
        (
            json!({"confirm_name": "Robotics", "current_password": "nope"}),
            &world.admin,
            403,
            "the current password is wrong",
        ),
    ];
    for (payload, token, code, detail) in bad {
        let (status, answer) = delete_org(&server, &world.org, token, payload).await;
        assert_eq!((status.as_u16(), answer), (code, json!({"detail": detail})));
    }
    assert!(server.state.hub.recorded().is_empty());

    // (the name may have spaces around it)
    let (status, _) = delete_org(
        &server,
        &world.org,
        &world.admin,
        json!({"confirm_name": " Robotics ", "current_password": PASSWORD}),
    )
    .await;
    assert_eq!(status, 204);
    assert_eq!(
        b.closed().await,
        (4404, "the organisation was deleted".to_owned())
    );
    assert!(b.seen.is_empty(), "nothing but the close: {:?}", b.seen);
    let admin = Some(world.admin.as_str());
    assert_eq!(
        server.get(&format!("/api/documents/{doc}"), admin).await.0,
        404
    );
    let (_, orgs) = server.get("/api/orgs", admin).await;
    assert_eq!(orgs.as_array().unwrap().len(), 1);
    assert_eq!(orgs[0]["id"], other.as_str());
    assert_eq!(server.get("/api/orgs", Some(&bob)).await.1, json!([]));
    assert!(!org_files.exists(), "the blobs are gone");
    assert!(!doc_files.exists(), "the snapshots are gone");
    // people keep their accounts
    assert_eq!(server.get("/api/me", Some(&bob)).await.0, 200);

    // every row went with it
    let store = &server.state.store;
    assert!(store.org_by_id(&world.org).await.unwrap().is_none());
    assert!(store.project_by_id(&world.project).await.unwrap().is_none());
    assert!(store.document_by_id(&doc).await.unwrap().is_none());
    assert!(store.change_by_seq(&doc, 1).await.unwrap().is_none());
    assert!(store.latest_snapshot(&doc).await.unwrap().is_none());
    assert!(store.org_members(&world.org).await.unwrap().is_empty());
    assert!(
        store
            .open_invites(&world.org, 0.0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn deleting_an_organisation_closes_its_open_documents_and_nothing_more_is_written() {
    let (server, world) = world().await;
    let (bob, _) = world.editor(&server, "bob@example.com").await;
    let one = world.document(&server, None, Some("One"), None).await;
    let two = world.document(&server, None, Some("Two"), None).await;
    // Another organisation's open document stays open.
    let (_, other) = server
        .post("/api/orgs", Some(&world.admin), json!({"name": "Other"}))
        .await;
    let other = other["id"].as_str().unwrap().to_owned();
    let (_, project) = server
        .post(
            &format!("/api/orgs/{other}/projects"),
            Some(&world.admin),
            json!({"name": "Elsewhere"}),
        )
        .await;
    let (_, elsewhere) = server
        .post(
            &format!(
                "/api/projects/{}/documents",
                project["id"].as_str().unwrap()
            ),
            Some(&world.admin),
            json!({"name": "Kept", "content": part()}),
        )
        .await;
    let elsewhere = elsewhere["id"].as_str().unwrap().to_owned();

    let mut b1 = Live::connect(&server, &one, &bob).await;
    let mut a2 = Live::connect(&server, &two, &world.admin).await;
    let mut kept = Live::connect(&server, &elsewhere, &world.admin).await;
    assert_eq!(edit(&mut b1, 5).await, 1);
    b1.drain().await;
    b1.seen.clear();

    assert_eq!(
        delete_org(&server, &world.org, &world.admin, robotics())
            .await
            .0,
        204
    );
    for live in [&mut b1, &mut a2] {
        assert_eq!(
            live.closed().await,
            (4404, "the organisation was deleted".to_owned())
        );
    }
    let mut closes = server.state.hub.recorded();
    closes.sort();
    let mut expected = [
        format!("close_document {one}"),
        format!("close_document {two}"),
    ];
    expected.sort();
    assert_eq!(closes, expected);
    assert!(server.state.hub.loaded(&one).is_none() && server.state.hub.loaded(&two).is_none());

    // A change sent after the close is written nowhere, and the documents cannot be opened.
    let late = b1.make_change(1, vec![set_depth(99)]);
    b1.send_raw_lossy(change_frame(late)).await;
    for doc in [&one, &two] {
        match Live::try_connect(&server, doc, &world.admin, Hello::default()).await {
            Ok(_) => panic!("a deleted document opened"),
            Err(refused) => assert_eq!(refused.close, Some(4404), "{refused:?}"),
        }
        assert!(
            server
                .state
                .store
                .document_by_id(doc)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            server
                .state
                .store
                .change_by_seq(doc, 1)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            server
                .state
                .store
                .change_by_seq(doc, 2)
                .await
                .unwrap()
                .is_none()
        );
        assert!(!storage(&server).join("docs").join(doc).exists());
    }
    // The other organisation's document is still open and takes changes.
    assert_eq!(edit(&mut kept, 8).await, 1);
}

#[tokio::test]
async fn deleting_an_organisation_removes_its_files_and_leaves_another_organisations_alone() {
    let (server, blobs) = with_export_blobs(Exports::default()).await;
    let world = World::new(&server).await.unwrap();
    // Two organisations hold the same bytes: the same hash, each its own copy.
    let body = b"the very same bytes in two organisations".to_vec();
    let sha = upload(&server, &world.project, &world.admin, &body).await;
    let doc = world
        .document(
            &server,
            Some(part_with_blob(&sha, body.len() as u64)),
            None,
            None,
        )
        .await;
    let (_, other) = server
        .post("/api/orgs", Some(&world.admin), json!({"name": "Other"}))
        .await;
    let other = other["id"].as_str().unwrap().to_owned();
    let (_, project) = server
        .post(
            &format!("/api/orgs/{other}/projects"),
            Some(&world.admin),
            json!({"name": "Elsewhere"}),
        )
        .await;
    let project = project["id"].as_str().unwrap().to_owned();
    assert_eq!(upload(&server, &project, &world.admin, &body).await, sha);
    let (status, kept) = server
        .post(
            &format!("/api/projects/{project}/documents"),
            Some(&world.admin),
            json!({"name": "Kept", "content": part_with_blob(&sha, body.len() as u64)}),
        )
        .await;
    assert_eq!(status, 201, "{kept}");
    let kept = kept["id"].as_str().unwrap().to_owned();
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(edit(&mut a, 3).await, 1);
    drop(a);

    let root = storage(&server);
    let ours = [
        root.join("orgs").join(&world.org),
        root.join("docs").join(&doc),
    ];
    let theirs = [
        root.join("orgs").join(&other),
        root.join("docs").join(&kept),
    ];
    assert!(ours.iter().chain(&theirs).all(|p| p.is_dir()));

    assert_eq!(
        delete_org(&server, &world.org, &world.admin, robotics())
            .await
            .0,
        204
    );
    for path in &ours {
        assert!(!path.exists(), "{} is gone", path.display());
    }
    for path in &theirs {
        assert!(path.is_dir(), "{} is untouched", path.display());
    }
    let mut removed = blobs.removed();
    removed.sort();
    assert_eq!(
        removed,
        [format!("docs/{doc}/"), format!("orgs/{}/", world.org)],
        "only the organisation's own prefixes were removed"
    );
    // The other organisation's blob and document are whole.
    let r = reqwest::Client::new()
        .get(server.url(&format!("/api/projects/{project}/blobs/{sha}")))
        .bearer_auth(&world.admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.bytes().await.unwrap().to_vec(), body);
    let (status, content) = server
        .get(
            &format!("/api/documents/{kept}/content"),
            Some(&world.admin),
        )
        .await;
    assert_eq!(status, 200, "{content}");
    let mut z = export_zip(&server, &other, &world.admin).await;
    assert_eq!(entry(&mut z, &format!("blobs/{sha}")), body);
}

#[tokio::test]
async fn files_that_cannot_be_removed_do_not_fail_the_deletion() {
    let (server, blobs) = with_export_blobs(Exports::default()).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    blobs.fail_removals.store(true, Ordering::SeqCst);
    let logs = LogCapture::start();
    assert_eq!(
        delete_org(&server, &world.org, &world.admin, robotics()).await,
        (StatusCode::NO_CONTENT, Value::Null)
    );
    assert!(
        server
            .state
            .store
            .org_by_id(&world.org)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage(&server).join("docs").join(&doc).exists(),
        "left behind"
    );
    let text = logs.text();
    assert!(
        text.contains("could not remove") && text.contains(&world.org),
        "{text}"
    );
    logs.assert_never_logged(&["Robotics", "ada@example.com", "Ada", "Bracket", PASSWORD]);
}

#[tokio::test]
async fn a_deletion_that_was_not_committed_removes_no_files() {
    let mut handle = None;
    let (server, flaky) = TestServer::start_flaky_full(
        |_| {},
        new_journal,
        |s| {
            let blobs = Arc::new(ExportBlobs::new(s.blobs.clone()));
            handle = Some(blobs.clone());
            s.blobs = blobs;
        },
    )
    .await;
    let blobs = handle.unwrap();
    let world = World::new(&server).await.unwrap();
    let body = b"still wanted".to_vec();
    let sha = upload(&server, &world.project, &world.admin, &body).await;
    let doc = world
        .document(
            &server,
            Some(part_with_blob(&sha, body.len() as u64)),
            None,
            None,
        )
        .await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    a.drain().await;
    flaky.fail_commit(true);
    assert_eq!(
        delete_org(&server, &world.org, &world.admin, robotics())
            .await
            .0,
        500
    );
    flaky.fail_commit(false);
    // The open document was closed (the commit might have happened), but nothing was removed.
    assert_eq!(a.closed().await.0, 4404);
    assert!(blobs.removed().is_empty(), "{:?}", blobs.removed());
    let (status, content) = server
        .get(&format!("/api/documents/{doc}/content"), Some(&world.admin))
        .await;
    assert_eq!(status, 200, "{content}");
    let mut z = export_zip(&server, &world.org, &world.admin).await;
    assert_eq!(entry(&mut z, &format!("blobs/{sha}")), body);
    Live::connect(&server, &doc, &world.admin).await;
}

#[tokio::test]
async fn organisation_deletion_refuses_what_it_should_not_trust() {
    let (server, world) = world().await;
    let (admin, _) = world
        .invite(&server, "cy@example.com", None, None, "admin")
        .await;
    // Eve has an account here and owns an organisation of her own, with the same name; she is
    // not in this one. (Only the server admin makes organisations here: made for her, then
    // handed over.)
    let (eve, eve_id) = world.editor(&server, "eve@example.com").await;
    let (status, _) = server
        .delete(
            &format!("/api/orgs/{}/members/{eve_id}", world.org),
            Some(&world.admin),
        )
        .await;
    assert_eq!(status, 204);
    let (status, hers) = server
        .post("/api/orgs", Some(&world.admin), json!({"name": "Robotics"}))
        .await;
    assert_eq!(status, 201);
    let hers = hers["id"].as_str().unwrap().to_owned();
    server
        .sql(&format!(
            "UPDATE org_members SET user_id = '{eve_id}' WHERE org_id = '{hers}'"
        ))
        .await;
    let closes = |server: &TestServer| {
        server
            .state
            .hub
            .recorded()
            .iter()
            .filter(|c| c.starts_with("close_document"))
            .count()
    };
    let no_such = json!({"detail": "no such organisation"});

    // wrong types, sizes and shapes, and names that are not the organisation's: 422
    for body in [
        json!({}),
        json!({"current_password": PASSWORD}),
        json!({"confirm_name": 7, "current_password": PASSWORD}),
        json!({"confirm_name": ["Robotics"], "current_password": PASSWORD}),
        json!({"confirm_name": "Robotics", "current_password": {"is": PASSWORD}}),
        json!({"confirm_name": "R".repeat(201), "current_password": PASSWORD}),
        json!({"confirm_name": "Robotics", "current_password": "p".repeat(1025)}),
        json!({"confirm_name": "", "current_password": PASSWORD}),
        json!({"confirm_name": "Robotics\u{0}", "current_password": PASSWORD}),
    ] {
        let (status, _) = delete_org(&server, &world.org, &world.admin, body.clone()).await;
        assert_eq!(status, 422, "{body}");
    }
    // nobody, and a body over the limit
    let url = server.url(&format!("/api/orgs/{}", world.org));
    let http = reqwest::Client::new();
    let r = http.delete(&url).json(&robotics()).send().await.unwrap();
    assert_eq!(r.status(), 401);
    let r = http
        .delete(&url)
        .bearer_auth(&world.admin)
        .json(
            &json!({"confirm_name": "Robotics", "current_password": PASSWORD,
                      "padding": "y".repeat(2 << 20)}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 413);
    // an admin is not an owner
    assert_eq!(
        delete_org(&server, &world.org, &admin, robotics()).await,
        (
            StatusCode::FORBIDDEN,
            json!({"detail": "needs the organisation role owner"})
        )
    );
    // an organisation the caller is not in does not exist, whatever they know about it
    assert_eq!(
        delete_org(&server, &world.org, &eve, robotics()).await,
        (StatusCode::NOT_FOUND, no_such.clone())
    );
    assert_eq!(
        delete_org(&server, &hers, &world.admin, robotics()).await,
        (StatusCode::NOT_FOUND, no_such.clone())
    );
    for id in ["0".repeat(32), "robotics".to_owned(), "x".repeat(5000)] {
        assert_eq!(
            delete_org(&server, &id, &world.admin, robotics()).await,
            (StatusCode::NOT_FOUND, no_such.clone()),
            "{id}"
        );
    }
    // none of it deleted anything
    let store = &server.state.store;
    assert!(store.org_by_id(&world.org).await.unwrap().is_some());
    assert!(store.org_by_id(&hers).await.unwrap().is_some());
    assert_eq!(closes(&server), 0);

    // and once it is deleted, it is gone for its owner too
    assert_eq!(
        delete_org(&server, &world.org, &world.admin, robotics())
            .await
            .0,
        204
    );
    assert_eq!(
        delete_org(&server, &world.org, &world.admin, robotics()).await,
        (StatusCode::NOT_FOUND, no_such.clone())
    );
    let r = export(&server, &world.org, Some(&world.admin)).await;
    assert_eq!(r.status(), 404);
    assert!(store.org_by_id(&hers).await.unwrap().is_some());
}

#[tokio::test]
async fn an_extension_can_refuse_deleting_an_organisation() {
    let (server, recorder) = recording().await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    *lock(&recorder.refuse) = Some("Robotics still has a paid subscription".into());
    assert_eq!(
        delete_org(&server, &world.org, &world.admin, robotics()).await,
        (
            StatusCode::PAYMENT_REQUIRED,
            json!({"detail": "Robotics still has a paid subscription"})
        )
    );
    // an extension that does not answer refuses too
    *lock(&recorder.refuse) = None;
    recorder.hang.store(true, Ordering::SeqCst);
    let (status, _) = delete_org(&server, &world.org, &world.admin, robotics()).await;
    assert_eq!(status, 409);
    recorder.hang.store(false, Ordering::SeqCst);
    let (_, orgs) = server.get("/api/orgs", Some(&world.admin)).await;
    assert_eq!(orgs[0]["id"], world.org.as_str());
    assert!(server.state.hub.recorded().is_empty());
    assert!(storage(&server).join("docs").join(&doc).exists());
    // The hook is not asked by someone who may not delete the organisation anyway.
    let (bob, _) = world.editor(&server, "bob@example.com").await;
    let asked = recorder.calls().len();
    assert_eq!(
        delete_org(&server, &world.org, &bob, robotics()).await.0,
        403
    );
    let wrong = json!({"confirm_name": "Robotics", "current_password": "wrong"});
    assert_eq!(
        delete_org(&server, &world.org, &world.admin, wrong).await.0,
        403
    );
    assert_eq!(recorder.calls().len(), asked);
}

#[tokio::test]
async fn deleting_an_organisation_tells_the_extensions_with_ids_only() {
    let (server, recorder) = recording().await;
    let world = World::new(&server).await.unwrap();
    let (_, bob_id) = world.editor(&server, "bob@example.com").await;
    lock(&recorder.events).clear();
    lock(&recorder.calls).clear();
    assert_eq!(
        delete_org(&server, &world.org, &world.admin, robotics())
            .await
            .0,
        204
    );
    let mut removed = [
        format!("member_removed {} {}", world.org, world.admin_id),
        format!("member_removed {} {bob_id}", world.org),
    ];
    removed.sort();
    let mut calls = recorder.calls();
    assert_eq!(calls.remove(0), format!("before_org_delete {}", world.org));
    calls.sort();
    assert_eq!(calls, removed);
    assert_eq!(recorder.event_types(), ["org_deleted"]);
    assert_eq!(
        recorder.events()[0],
        json!({"type": "org_deleted", "org": world.org, "by": world.admin_id})
    );
}

#[tokio::test]
async fn an_export_of_a_document_whose_early_journal_is_gone_holds_what_is_left() {
    let mut handle = None;
    let server = TestServer::start_custom(
        |s| s.snapshot_every = 5,
        |s| {
            let blobs = Arc::new(ExportBlobs::new(s.blobs.clone()));
            handle = Some(blobs.clone());
            s.blobs = blobs;
            s.exports = Arc::new(Exports::new(2, MINUTE, HOUR, 2));
        },
    )
    .await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    for n in 1..=12 {
        assert_eq!(edit(&mut a, n).await, n as u64);
    }
    let mut snap = 0;
    for _ in 0..250 {
        snap = server.room_status(&doc).await.snapshot_seq;
        if snap >= 10 {
            break;
        }
        pause(Duration::from_millis(20)).await;
    }
    assert!((10..=12).contains(&snap), "a snapshot at {snap}");
    // (Two more changes, so that there is journal after the snapshot wherever it was taken.)
    for n in 13..=14 {
        assert_eq!(edit(&mut a, n).await, n as u64);
    }
    // The journal before the snapshot goes (a gap several pages wide): the document still loads,
    // and its export holds the rows that exist.
    server
        .sql(&format!(
            "DELETE FROM changes WHERE document_id = '{doc}' AND seq <= {snap}"
        ))
        .await;
    let mut z = export_zip(&server, &world.org, &world.admin).await;
    let base = format!("projects/{}/{doc}", world.project);
    let d = entry_json(&mut z, &format!("{base}/document.json"));
    assert_eq!(d["seq"], 14);
    assert_eq!(d["content"]["features"][1]["params"]["depth"], 14);
    let seqs: Vec<u64> = entry_lines(&mut z, &format!("{base}/history.jsonl"))
        .iter()
        .map(|l| l["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, ((snap + 1)..=14).collect::<Vec<_>>());
}

#[tokio::test]
async fn an_account_has_one_export_at_a_time() {
    let (server, blobs) = with_export_blobs(Exports::new(3, MINUTE, HOUR, 500)).await;
    let world = World::new(&server).await.unwrap();
    let body = b"held back".to_vec();
    let sha = upload(&server, &world.project, &world.admin, &body).await;
    world
        .document(
            &server,
            Some(part_with_blob(&sha, body.len() as u64)),
            None,
            None,
        )
        .await;
    let (bob, _) = world.editor(&server, "bob@example.com").await;
    blobs.hold();
    let first = export(&server, &world.org, Some(&world.admin)).await;
    assert_eq!(first.status(), 200);
    assert!(eventually(|| blobs.reads_started() == 1).await);
    // The server has places left, but this account has its one: either kind is refused.
    let mine =
        json!({"detail": "you have an export running already; try again when it has finished"});
    let r = export(&server, &world.org, Some(&world.admin)).await;
    assert_eq!(r.status(), 429);
    assert_eq!(r.json::<Value>().await.unwrap(), mine);
    assert_eq!(
        server.get("/api/me/export", Some(&world.admin)).await,
        (StatusCode::TOO_MANY_REQUESTS, mine)
    );
    assert_eq!(
        server.state.exports.running(),
        1,
        "a refusal takes no place"
    );
    // Someone else is not held up.
    assert_eq!(server.get("/api/me/export", Some(&bob)).await.0, 200);
    blobs.release();
    first.bytes().await.expect("the first export finishes");
    export_zip(&server, &world.org, &world.admin).await;
}

#[tokio::test]
async fn a_client_that_reads_a_little_now_and_then_is_cut_off_at_the_time_limit() {
    const LEN: u64 = 32 << 20;
    // No single write waits long (the client keeps reading), but the export may take 600 ms.
    let limits = Exports::new(1, MINUTE, Duration::from_millis(600), 500);
    let (server, blobs) = with_export_blobs(limits).await;
    let world = World::new(&server).await.unwrap();
    let sha = blobs.make_up(&world.org, LEN);
    world
        .document(&server, Some(part_with_blob(&sha, LEN)), None, None)
        .await;
    let logs = LogCapture::start();
    let started = std::time::Instant::now();
    let mut slow = export(&server, &world.org, Some(&world.admin)).await;
    assert_eq!(slow.status(), 200);
    let mut got = 0u64;
    let ended = loop {
        match slow.chunk().await {
            Ok(Some(chunk)) => got += chunk.len() as u64,
            Ok(None) => break Ok(()),
            Err(e) => break Err(e),
        }
        pause(Duration::from_millis(50)).await;
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "the export went on after its time limit ({got} bytes so far)"
        );
    };
    assert!(
        ended.is_err(),
        "the response ended cleanly after {got} bytes"
    );
    assert!(got < LEN && blobs.pulled() < LEN);
    assert!(eventually(|| server.state.exports.running() == 0).await);
    assert_eq!(
        blobs.outstanding.load(Ordering::SeqCst),
        0,
        "nothing is kept"
    );
    assert!(logs.text().contains("took longer than"), "{}", logs.text());
    logs.assert_never_logged(&["Robotics", "ada@example.com", "Ada"]);
    // The account's place is free too.
    assert_eq!(
        server.get("/api/me/export", Some(&world.admin)).await.0,
        200
    );
}

#[tokio::test]
async fn exports_are_not_to_be_cached_and_head_starts_none() {
    let (server, recorder) = recording().await;
    let world = World::new(&server).await.unwrap();
    world.document(&server, None, None, None).await;
    let http = reqwest::Client::new();
    let paths = [
        "/api/me/export".to_owned(),
        format!("/api/orgs/{}/export", world.org),
    ];
    lock(&recorder.events).clear();
    for path in &paths {
        // HEAD would have to make the export to know its headers: it is not offered.
        let r = http
            .head(server.url(path))
            .bearer_auth(&world.admin)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 405, "{path}");
        for method in [Method::POST, Method::PUT] {
            let r = http
                .request(method, server.url(path))
                .bearer_auth(&world.admin)
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), 405, "{path}");
        }
    }
    pause(Duration::from_millis(100)).await;
    assert!(recorder.events().is_empty(), "{:?}", recorder.events());
    assert_eq!(server.state.exports.running(), 0);
    for path in &paths {
        let r = http
            .get(server.url(path))
            .bearer_auth(&world.admin)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(r.headers()[header::CACHE_CONTROL], "no-store", "{path}");
        r.bytes().await.unwrap();
    }
    assert_eq!(recorder.event_types(), ["account_exported", "org_exported"]);
}

#[tokio::test]
async fn names_in_the_organisation_never_reach_the_names_in_the_zip() {
    let (server, world) = world().await;
    let admin = Some(world.admin.as_str());
    let hostile = [
        "../x",
        "C:\\x",
        "/etc/passwd",
        "a\u{1}b\tc",
        "..",
        "x/../../y",
    ];
    let body = b"a blob".to_vec();
    let sha = upload(&server, &world.project, &world.admin, &body).await;
    let mut documents = Vec::new();
    for name in hostile {
        let (status, project) = server
            .post(
                &format!("/api/orgs/{}/projects", world.org),
                admin,
                json!({"name": name}),
            )
            .await;
        assert_eq!(status, 201, "{name:?}: {project}");
        let project = project["id"].as_str().unwrap().to_owned();
        let (status, folder) = server
            .post(
                &format!("/api/projects/{project}/folders"),
                admin,
                json!({"name": name}),
            )
            .await;
        assert_eq!(status, 201, "{name:?}: {folder}");
        let (status, doc) = server
            .post(
                &format!("/api/projects/{project}/documents"),
                admin,
                json!({"name": name, "folder_id": folder["id"],
                       "content": part_with_blob(&sha, body.len() as u64)}),
            )
            .await;
        assert_eq!(status, 201, "{name:?}: {doc}");
        let doc = doc["id"].as_str().unwrap().to_owned();
        let (status, _) = server
            .post(
                &format!("/api/documents/{doc}/versions"),
                admin,
                json!({"name": name}),
            )
            .await;
        assert_eq!(status, 201);
        documents.push((project, doc, name));
    }
    let (status, _) = server
        .patch(
            &format!("/api/orgs/{}", world.org),
            admin,
            json!({"name": "../../org"}),
        )
        .await;
    assert_eq!(status, 200);

    let mut z = export_zip(&server, &world.org, &world.admin).await;
    let is_id = |s: &str, n: usize| {
        s.len() == n && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    };
    let all = names(&z);
    assert_eq!(all.len(), 2 + 3 * hostile.len());
    for name in &all {
        let parts: Vec<&str> = name.split('/').collect();
        let fine = match parts.as_slice() {
            ["organisation.json"] => true,
            ["blobs", sha] => is_id(sha, 64),
            ["projects", project, doc, file] => {
                is_id(project, 32)
                    && is_id(doc, 32)
                    && ["document.json", "history.jsonl", "versions.json"].contains(file)
            }
            _ => false,
        };
        assert!(fine, "an entry named {name:?}");
        assert!(!name.contains("..") && !name.contains('\\') && !name.starts_with('/'));
        assert!(name.is_ascii() && !name.bytes().any(|b| b.is_ascii_control()));
    }
    // The names are inside, where they are data.
    for (project, doc, name) in documents {
        let d = entry_json(&mut z, &format!("projects/{project}/{doc}/document.json"));
        assert_eq!(d["name"], name);
        let v = entry_json(&mut z, &format!("projects/{project}/{doc}/versions.json"));
        assert_eq!(v[0]["name"], name);
    }
    assert_eq!(entry_json(&mut z, "organisation.json")["name"], "../../org");
}

/// The organisation is deleted, as a request of its own; answers its status.
fn deleting(server: &TestServer, world: &World) -> tokio::task::JoinHandle<StatusCode> {
    let url = server.url(&format!("/api/orgs/{}", world.org));
    let token = world.admin.clone();
    tokio::spawn(async move {
        reqwest::Client::new()
            .delete(url)
            .bearer_auth(token)
            .json(&robotics())
            .send()
            .await
            .expect("an answer")
            .status()
    })
}

/// Asserts that nothing of `doc` is left in the database: no row, and no journal row.
async fn nothing_left_of(server: &TestServer, doc: &str) {
    let store = &server.state.store;
    assert!(store.document_by_id(doc).await.unwrap().is_none());
    let rows = store.changes_between(doc, 0, i64::MAX, 100).await.unwrap();
    assert!(
        rows.is_empty(),
        "journal rows of a deleted document: {rows:?}"
    );
    assert!(store.latest_snapshot(doc).await.unwrap().is_none());
    assert!(server.state.hub.loaded(doc).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_being_written_when_its_organisation_is_deleted_leaves_no_row() {
    let (server, flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, _) = world.editor(&server, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(edit(&mut b, 1).await, 1);
    // Bob's next change is held at the journal's commit; the deletion arrives meanwhile.
    flaky.commit_gate.shut();
    let change = b.make_change(1, vec![set_depth(2)]);
    b.send_change(&change).await;
    tokio::time::timeout(Duration::from_secs(10), flaky.commit_gate.until_waiting(1))
        .await
        .expect("the change is being written");
    let deletion = deleting(&server, &world);
    // (Long enough for the deletion to reach its own transaction or its commit.)
    pause(Duration::from_millis(500)).await;
    flaky.commit_gate.open();
    assert_eq!(deletion.await.unwrap(), 204);
    // Whichever of the two was written first, Bob ends up closed and no row is left.
    assert_eq!(b.closed().await.0, 4404);
    nothing_left_of(&server, &doc).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_sent_between_the_deletions_commit_and_the_close_is_written_nowhere() {
    let (server, _flaky) = TestServer::start_flaky(|_| {}).await;
    let world = World::new(&server).await.unwrap();
    let (bob, _) = world.editor(&server, "bob@example.com").await;
    let doc = world.document(&server, None, None, None).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(edit(&mut b, 1).await, 1);
    // The deletion commits, and is held before it closes anything: the room is still open.
    let gate = server.state.room_hooks.after_commit.clone();
    gate.shut();
    let deletion = deleting(&server, &world);
    tokio::time::timeout(Duration::from_secs(10), gate.until_waiting(1))
        .await
        .expect("the deletion committed");
    assert!(
        server
            .state
            .store
            .document_by_id(&doc)
            .await
            .unwrap()
            .is_none()
    );
    assert!(server.state.hub.loaded(&doc).is_some());
    // Bob, still connected, sends changes into the room of a document that is gone.
    for depth in [2, 3] {
        let change = b.make_change(1, vec![set_depth(depth)]);
        b.send_change(&change).await;
    }
    pause(Duration::from_millis(300)).await;
    nothing_left_of_rows(&server, &doc).await;
    gate.open();
    assert_eq!(deletion.await.unwrap(), 204);
    // The room could not store them and said so; then Bob is closed. No row was written.
    assert_eq!(
        b.closed().await,
        (4404, "the organisation was deleted".to_owned())
    );
    assert!(
        b.seen
            .iter()
            .all(|m| matches!(m, oswx_core::wire::ServerMsg::Rejected { .. })),
        "{:?}",
        b.seen
    );
    nothing_left_of(&server, &doc).await;
    match Live::try_connect(&server, &doc, &bob, Hello::default()).await {
        Ok(_) => panic!("a deleted document opened"),
        Err(refused) => assert_eq!(refused.close, Some(4404), "{refused:?}"),
    }
}

/// Asserts that the journal has no row of `doc`.
async fn nothing_left_of_rows(server: &TestServer, doc: &str) {
    let rows = server
        .state
        .store
        .changes_between(doc, 0, i64::MAX, 100)
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "journal rows of a deleted document: {rows:?}"
    );
}
