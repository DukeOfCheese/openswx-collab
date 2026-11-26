//! What the serving loop adds to the app (`oswx_server::serve`): time limits that cut what
//! stopped and spare what moves, the limit on blob uploads at once, the client address behind
//! proxies, `/metrics`, and the orderly stop.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::*;
use futures_util::StreamExt;
use oswx_core::Seq;
use oswx_core::wire::RejectCode;
use oswx_server::blobs::blob_key;
use oswx_server::serve::{Stopped, TOO_SLOW, serve};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

const MIB: usize = 1 << 20;
const WAIT: Duration = Duration::from_secs(20);
const METRICS_TOKEN: &str = "a-metrics-token-0123456789";

async fn eventually(mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + WAIT;
    while Instant::now() < deadline {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    check()
}

fn addr(server: &TestServer) -> String {
    server.base.trim_start_matches("http://").to_owned()
}

/// Reads from `socket` until `end` appears (or the peer closes); answers what was read.
async fn read_until(socket: &mut TcpStream, end: &[u8]) -> Vec<u8> {
    let mut got = Vec::new();
    let mut buf = vec![0u8; 16 << 10];
    while !got.windows(end.len()).any(|w| w == end) {
        match socket.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
        }
    }
    got
}

/// Reads to the end of the connection (a close or a reset); answers how many bytes came.
async fn read_to_end(socket: &mut TcpStream) -> usize {
    let mut total = 0;
    let mut buf = vec![0u8; 64 << 10];
    loop {
        match socket.read(&mut buf).await {
            Ok(0) | Err(_) => return total,
            Ok(n) => total += n,
        }
    }
}

async fn store_blob(server: &TestServer, w: &World, body: &[u8]) -> String {
    let sha = sha_of(body);
    server
        .state
        .blobs
        .put_stream(&blob_key(&w.org, &sha), chunked(body, MIB), u64::MAX)
        .await
        .unwrap();
    sha
}

fn blob_path(w: &World, sha: &str) -> String {
    format!("/api/projects/{}/blobs/{sha}", w.project)
}

// ------------------------------------------------------------------ time limits

#[tokio::test]
async fn a_download_that_moves_slowly_survives_and_one_that_stalls_is_cut() {
    // Both limits far below how long the slow download takes; the storage's own idle limit far
    // above, so that it is not what ends the stalled one.
    let server = TestServer::start_with(|s| {
        s.write_timeout = 1.0;
        s.request_timeout = 1.0;
        s.blob_idle_timeout = 300.0;
    })
    .await;
    let w = World::new(&server).await.unwrap();
    // Far more than the connection's buffers hold.
    let body = noise(48 * MIB, 23);
    let sha = store_blob(&server, &w, &body).await;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
        blob_path(&w, &sha),
        addr(&server),
        w.admin
    );

    // Slow, but it moves: a megabyte every 100 ms, for some five seconds.
    let started = Instant::now();
    let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
    socket.write_all(request.as_bytes()).await.unwrap();
    let head = read_until(&mut socket, b"\r\n\r\n").await;
    let split = head.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    assert!(head.starts_with(b"HTTP/1.1 200"), "{:?}", &head[..split]);
    let mut got = head[split..].to_vec();
    let mut buf = vec![0u8; MIB];
    while got.len() < body.len() {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut filled = 0;
        while filled < buf.len() {
            match socket.read(&mut buf[filled..]).await.unwrap() {
                0 => break,
                n => filled += n,
            }
        }
        assert!(filled > 0, "the download ended after {} bytes", got.len());
        got.extend_from_slice(&buf[..filled]);
    }
    assert!(
        started.elapsed() > Duration::from_secs(3),
        "the download was not slow enough to show anything: {:?}",
        started.elapsed()
    );
    assert!(got == body, "the slow download came back changed");
    assert_eq!(server.state.http.write_timeouts(), 0);
    assert_eq!(server.state.http.request_timeouts(), 0);
    drop(socket);

    // Stalled: the client reads the head and then nothing, and keeps the connection open.
    let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut first = vec![0u8; 4096];
    let n = socket.read(&mut first).await.unwrap();
    assert!(String::from_utf8_lossy(&first[..n]).starts_with("HTTP/1.1 200"));
    assert!(
        eventually(|| server.state.http.write_timeouts() == 1).await,
        "the stalled download was not cut"
    );
    // The server closed its end: what its buffers held still comes, the rest never does.
    let rest = tokio::time::timeout(WAIT, read_to_end(&mut socket))
        .await
        .expect("the connection ends");
    assert!(n + rest < body.len(), "the whole blob came after all");
    drop(socket);
    // ... and the connection is gone from the server, with whatever it held.
    assert!(
        eventually(|| server.state.http.open() == 0).await,
        "{} connections are still open",
        server.state.http.open()
    );
    assert_eq!(server.state.http.write_timeouts(), 1);
}

#[tokio::test]
async fn an_upload_that_keeps_moving_outlives_the_request_timeout() {
    let server = TestServer::start_with(|s| s.request_timeout = 1.0).await;
    let w = World::new(&server).await.unwrap();
    let body = noise(2 * MIB, 5);
    let sha = sha_of(&body);
    // Ten chunks, 300 ms apart: three seconds.
    let chunks: Vec<Bytes> = body
        .chunks(body.len() / 10)
        .map(Bytes::copy_from_slice)
        .collect();
    let slow = futures_util::stream::iter(chunks).then(|chunk| async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok::<_, io::Error>(chunk)
    });
    let started = Instant::now();
    let r = reqwest::Client::new()
        .put(server.url(&blob_path(&w, &sha)))
        .bearer_auth(&w.admin)
        .body(reqwest::Body::wrap_stream(slow))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert!(started.elapsed() > Duration::from_secs(2));
    assert_eq!(server.state.http.request_timeouts(), 0);
    let back = reqwest::Client::new()
        .get(server.url(&blob_path(&w, &sha)))
        .bearer_auth(&w.admin)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(back == body);
}

#[tokio::test]
async fn a_request_that_takes_too_long_is_answered_503() {
    let server = TestServer::start_with(|s| s.request_timeout = 0.5).await;
    let w = World::new(&server).await.unwrap();
    // A body that is announced and never finished, on a route that reads it whole.
    let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
    let head = format!(
        "PATCH /api/projects/{} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\n\
         Content-Type: application/json\r\nContent-Length: 200\r\n\r\n{{\"name\":",
        w.project,
        addr(&server),
        w.admin
    );
    socket.write_all(head.as_bytes()).await.unwrap();
    let started = Instant::now();
    let answer = tokio::time::timeout(WAIT, read_until(&mut socket, b"}"))
        .await
        .expect("the request is answered");
    let answer = String::from_utf8_lossy(&answer).into_owned();
    assert!(answer.starts_with("HTTP/1.1 503"), "{answer}");
    assert!(
        answer.contains(&format!("{{\"detail\":\"{TOO_SLOW}\"}}")),
        "{answer}"
    );
    assert!(started.elapsed() >= Duration::from_millis(400));
    assert_eq!(server.state.http.request_timeouts(), 1);

    // The same on a document creation (the other route with a large body).
    let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
    let head = format!(
        "POST /api/projects/{}/documents HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\n\
         Content-Type: application/json\r\nContent-Length: 2000000\r\n\r\n{{\"name\":",
        w.project,
        addr(&server),
        w.admin
    );
    socket.write_all(head.as_bytes()).await.unwrap();
    let answer = tokio::time::timeout(WAIT, read_until(&mut socket, b"}"))
        .await
        .expect("the request is answered");
    assert!(
        answer.starts_with(b"HTTP/1.1 503"),
        "{}",
        String::from_utf8_lossy(&answer)
    );
    assert_eq!(server.state.http.request_timeouts(), 2);
}

#[tokio::test]
async fn a_client_that_never_finishes_its_request_head_is_cut_off() {
    let server = TestServer::start_with(|s| s.request_timeout = 0.5).await;
    for unfinished in ["", "GET /api/health HTTP/1.1\r\nHost: x\r\nX-Slow: lo"] {
        let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
        socket.write_all(unfinished.as_bytes()).await.unwrap();
        assert!(eventually(|| server.state.http.open() >= 1).await);
        let started = Instant::now();
        // The server ends the connection by itself (whatever it says first, it is no answer to
        // the request).
        let got = tokio::time::timeout(WAIT, read_until(&mut socket, b"\0never\0"))
            .await
            .expect("the connection is closed");
        assert!(
            !got.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&got)
        );
        assert!(started.elapsed() < Duration::from_secs(10));
        assert!(
            eventually(|| server.state.http.open() == 0).await,
            "the connection is still counted as open"
        );
    }
    // A client that does finish is served as ever.
    assert_eq!(server.get("/api/health", None).await.0, 200);
}

// ------------------------------------------------------------------ uploads at once

/// An upload of `body` that sends its first half and then waits for `finish` before the rest.
fn held_upload(
    server: &TestServer,
    w: &World,
    token: &str,
    body: &[u8],
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<StatusCode>) {
    let (finish, finished) = oneshot::channel::<()>();
    let (first, second) = body.split_at(body.len() / 2);
    let (first, second) = (
        Bytes::copy_from_slice(first),
        Bytes::copy_from_slice(second),
    );
    let parts = futures_util::stream::once(async move { Ok::<_, io::Error>(first) }).chain(
        futures_util::stream::once(async move {
            let _ = finished.await;
            Ok(second)
        }),
    );
    let request = reqwest::Client::new()
        .put(server.url(&blob_path(w, &sha_of(body))))
        .bearer_auth(token)
        .body(reqwest::Body::wrap_stream(parts));
    let task = tokio::spawn(async move { request.send().await.unwrap().status() });
    (finish, task)
}

async fn upload_now(
    server: &TestServer,
    w: &World,
    token: &str,
    body: &[u8],
) -> (StatusCode, Value) {
    let r = reqwest::Client::new()
        .put(server.url(&blob_path(w, &sha_of(body))))
        .bearer_auth(token)
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    let status = r.status();
    let text = r.text().await.unwrap();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}

#[tokio::test]
async fn only_so_many_uploads_run_at_once_per_account_and_on_the_server() {
    let server = TestServer::start_with(|s| {
        s.max_blob_uploads = 2;
        s.max_blob_uploads_per_user = 1;
    })
    .await;
    let w = World::new(&server).await.unwrap();
    let (bob, _) = w.editor(&server, "bob@example.com").await;
    let (cy, _) = w.editor(&server, "cy@example.com").await;
    let uploads = server.state.uploads.clone();
    let (one, two, three) = (noise(MIB, 1), noise(MIB, 2), noise(MIB, 3));

    let (finish_admin, admin_upload) = held_upload(&server, &w, &w.admin, &one);
    assert!(eventually(|| uploads.running() == 1).await);
    // One more from the same account: 429, at once, and nothing of it is stored.
    let (status, body) = upload_now(&server, &w, &w.admin, &two).await;
    assert_eq!(status, 429, "{body}");
    assert!(
        body["detail"]
            .as_str()
            .unwrap()
            .contains("too many uploads running"),
        "{body}"
    );
    assert_eq!(uploads.running(), 1);
    // Another account is not held up by it...
    let (finish_bob, bob_upload) = held_upload(&server, &w, &bob, &two);
    assert!(eventually(|| uploads.running() == 2).await);
    // ... until the server has as many as it takes: 503 for a third account.
    let (status, body) = upload_now(&server, &w, &cy, &three).await;
    assert_eq!(status, 503, "{body}");
    assert!(
        body["detail"]
            .as_str()
            .unwrap()
            .contains("as many uploads as it takes"),
        "{body}"
    );
    // Asking what is missing, downloading and everything else go on meanwhile.
    let (status, missing) = server
        .post(
            &format!("/api/projects/{}/blobs/missing", w.project),
            Some(&cy),
            json!([sha_of(&three)]),
        )
        .await;
    assert_eq!((status, missing), (StatusCode::OK, json!([sha_of(&three)])));
    // Someone who may not upload at all takes no place and hears nothing of the limit.
    let (status, _) = upload_now(&server, &w, "not-a-token", b"x").await;
    assert_eq!(status, 401);
    assert_eq!(uploads.running(), 2);

    // The held uploads finish, and the places are free again.
    finish_admin.send(()).unwrap();
    finish_bob.send(()).unwrap();
    assert_eq!(admin_upload.await.unwrap(), 204);
    assert_eq!(bob_upload.await.unwrap(), 204);
    assert!(eventually(|| uploads.running() == 0).await);
    assert_eq!(upload_now(&server, &w, &cy, &three).await.0, 204);
    assert_eq!(upload_now(&server, &w, &w.admin, &three).await.0, 204);
    assert!(eventually(|| uploads.running() == 0).await);
    let counted = oswx_server::metrics::counters().uploads_refused;
    assert!(counted[0] >= 1 && counted[1] >= 1, "{counted:?}");
}

#[tokio::test]
async fn an_upload_whose_client_goes_away_or_fails_gives_its_place_back() {
    let server = TestServer::start_with(|s| {
        s.max_blob_uploads = 1;
        s.blob_idle_timeout = 0.5;
    })
    .await;
    let w = World::new(&server).await.unwrap();
    let uploads = server.state.uploads.clone();
    let body = noise(MIB, 4);
    // The client hangs up in the middle.
    let (finish, upload) = held_upload(&server, &w, &w.admin, &body);
    assert!(eventually(|| uploads.running() == 1).await);
    upload.abort();
    drop(finish);
    assert!(
        eventually(|| uploads.running() == 0).await,
        "the place was kept"
    );
    // The client stays, and sends nothing more: given up on when no bytes move.
    let (_finish, upload) = held_upload(&server, &w, &w.admin, &body);
    assert_eq!(upload.await.unwrap(), 400);
    assert!(eventually(|| uploads.running() == 0).await);
    // A wrong digest, a blob that is there already: each gives its place back.
    let wrong = reqwest::Client::new()
        .put(server.url(&blob_path(&w, &sha_of(b"something else"))))
        .bearer_auth(&w.admin)
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 400);
    assert_eq!(upload_now(&server, &w, &w.admin, &body).await.0, 204);
    assert_eq!(upload_now(&server, &w, &w.admin, &body).await.0, 204);
    assert_eq!(uploads.running(), 0);
}

// ------------------------------------------------------------------ the client address

async fn failed_sign_in(server: &TestServer, email: &str, forwarded: Option<&str>) -> StatusCode {
    let mut r = reqwest::Client::new()
        .post(server.url("/api/auth/login"))
        .json(&json!({"email": email, "password": "not the password"}));
    if let Some(f) = forwarded {
        r = r.header("x-forwarded-for", f);
    }
    r.send().await.unwrap().status()
}

/// How many of twelve failed sign-ins for `email` are refused as wrong (401) before the limit
/// answers 429, when try `i` carries the header `forwarded(i)`.
async fn tries_before_the_limit(
    server: &TestServer,
    email: &str,
    forwarded: impl Fn(usize) -> Option<String>,
) -> usize {
    for i in 0..12 {
        match failed_sign_in(server, email, forwarded(i).as_deref()).await {
            StatusCode::UNAUTHORIZED => {}
            StatusCode::TOO_MANY_REQUESTS => return i,
            other => panic!("try {i} answered {other}"),
        }
    }
    12
}

#[tokio::test]
async fn a_forged_forwarded_header_does_not_get_round_the_sign_in_limit() {
    // Nobody is trusted: the tests' own address (127.0.0.1) is every client's, whatever it sends.
    let server = TestServer::start_with(|s| s.trusted_proxies = vec![]).await;
    World::new(&server).await.unwrap();
    let forged = |i: usize| Some(format!("203.0.113.{i}"));
    assert_eq!(
        tries_before_the_limit(&server, "a@example.com", forged).await,
        10
    );

    // A proxy that is trusted, somewhere else: the tests' address still is not.
    let server = TestServer::start_with(|s| {
        s.trusted_proxies = vec!["172.16.0.0/12".parse().unwrap(), "::1".parse().unwrap()];
    })
    .await;
    World::new(&server).await.unwrap();
    assert_eq!(
        tries_before_the_limit(&server, "a@example.com", forged).await,
        10
    );
}

#[tokio::test]
async fn behind_a_trusted_proxy_each_client_has_its_own_limit_and_cannot_forge_another() {
    // The tests connect from 127.0.0.1, as a proxy on this machine does.
    let server = TestServer::start_with(|s| {
        s.trusted_proxies = vec!["127.0.0.0/8".parse().unwrap(), "10.0.0.5".parse().unwrap()];
    })
    .await;
    World::new(&server).await.unwrap();
    // Each client the proxy vouches for is counted by itself.
    let honest = |i: usize| Some(format!("203.0.113.{i}"));
    assert_eq!(
        tries_before_the_limit(&server, "a@example.com", honest).await,
        12
    );
    // What a client writes itself is to the left of what the proxies add: changing it changes
    // nothing, whether it is an address of its choice, a proxy's own address or rubbish.
    let chained = |i: usize| Some(format!("198.51.100.{i}, 203.0.113.99, 10.0.0.5"));
    assert_eq!(
        tries_before_the_limit(&server, "b@example.com", chained).await,
        10
    );
    let posing = |i: usize| {
        let left = ["127.0.0.1", "10.0.0.5", "unknown", "", "::1", "1.2.3.4:80"][i % 6];
        Some(format!("{left}, 203.0.113.98"))
    };
    assert_eq!(
        tries_before_the_limit(&server, "c@example.com", posing).await,
        10
    );
    // A header that is no list of addresses at all counts for the proxy's own address: one
    // limit for all such, never one per spelling.
    let rubbish = |i: usize| {
        let spellings = [
            "unknown".to_owned(),
            format!("host-{i}.example"),
            format!("203.0.113.{i}:4000"),
            format!("[2001:db8::{i}]"),
            format!("203.0.113.{i}/32"),
            "x".repeat(4000 + i),
        ];
        Some(spellings[i % 6].clone())
    };
    assert_eq!(
        tries_before_the_limit(&server, "d@example.com", rubbish).await,
        10
    );
}

// ------------------------------------------------------------------ /metrics

async fn metrics(server: &TestServer, bearer: Option<&str>) -> (StatusCode, String, String) {
    let mut r = reqwest::Client::new().get(server.url("/metrics"));
    if let Some(token) = bearer {
        r = r.bearer_auth(token);
    }
    let r = r.send().await.unwrap();
    let content_type = r
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    (r.status(), content_type, r.text().await.unwrap())
}

/// The value of the sample `name` (with its labels, exactly as written) in a metrics text.
fn sample(text: &str, name: &str) -> f64 {
    let line = text
        .lines()
        .find(|l| {
            l.strip_prefix(name)
                .is_some_and(|rest| rest.starts_with(' '))
        })
        .unwrap_or_else(|| panic!("no sample {name} in:\n{text}"));
    line.rsplit(' ').next().unwrap().parse().unwrap()
}

#[tokio::test]
async fn there_is_no_metrics_route_unless_a_token_is_set_and_then_only_with_it() {
    let (server, world) = world().await;
    for bearer in [
        None,
        Some(world.admin.as_str()),
        Some(METRICS_TOKEN),
        Some(""),
    ] {
        let (status, _, body) = metrics(&server, bearer).await;
        assert_eq!(status, 404, "{bearer:?}");
        assert_eq!(body, r#"{"detail":"Not Found"}"#);
    }

    let server = TestServer::start_with(|s| s.metrics_token = METRICS_TOKEN.into()).await;
    let world = World::new(&server).await.unwrap();
    let longer = format!("{METRICS_TOKEN}x");
    for bearer in [
        None,
        Some(""),
        // The server admin's session is not the metrics token.
        Some(world.admin.as_str()),
        Some(&METRICS_TOKEN[1..]),
        Some(&METRICS_TOKEN[..METRICS_TOKEN.len() - 1]),
        Some(longer.as_str()),
        Some("A-METRICS-TOKEN-0123456789"),
    ] {
        let (status, _, body) = metrics(&server, bearer).await;
        assert_eq!(status, 401, "{bearer:?}");
        assert!(!body.contains("oswx_collab"), "{body}");
    }
    // Not as a query, not as another scheme.
    for request in [
        reqwest::Client::new().get(server.url(&format!("/metrics?token={METRICS_TOKEN}"))),
        reqwest::Client::new()
            .get(server.url("/metrics"))
            .header("authorization", format!("Basic {METRICS_TOKEN}")),
        reqwest::Client::new()
            .get(server.url("/metrics"))
            .header("authorization", METRICS_TOKEN),
    ] {
        assert_eq!(request.send().await.unwrap().status(), 401);
    }
    let (status, content_type, text) = metrics(&server, Some(METRICS_TOKEN)).await;
    assert_eq!(status, 200);
    assert_eq!(content_type, "text/plain; version=0.0.4; charset=utf-8");
    assert!(text.contains("# TYPE oswx_collab_changes_accepted_total counter\n"));
    // (The scheme's name in any case, as HTTP has it.)
    let r = reqwest::Client::new()
        .get(server.url("/metrics"))
        .header("authorization", format!("bearer {METRICS_TOKEN}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let (status, _) = server
        .post("/metrics", Some(METRICS_TOKEN), json!({}))
        .await;
    assert_eq!(status, 405);
}

#[tokio::test]
async fn the_metrics_change_with_what_the_server_does() {
    let server = TestServer::start_with(|s| s.metrics_token = METRICS_TOKEN.into()).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let (_, _, before) = metrics(&server, Some(METRICS_TOKEN)).await;
    assert_eq!(sample(&before, "oswx_collab_rooms"), 0.0);
    assert_eq!(sample(&before, "oswx_collab_connections"), 0.0);

    let (bob, _) = world.editor(&server, "bob@example.com").await;
    let mut live = Live::connect(&server, &doc, &world.admin).await;
    let mut other = Live::connect(&server, &doc, &bob).await;
    assert_eq!(live.change_on(0, vec![set_depth(4)]).await.seq(), 1);
    // Bob has not seen that change and edits the same value: refused.
    assert_eq!(
        other.change_on(0, vec![set_depth(5)]).await.code(),
        RejectCode::Stale
    );

    let (_, _, after) = metrics(&server, Some(METRICS_TOKEN)).await;
    // (The counters are the process's: other tests of this file add to them meanwhile.)
    let grew = |name: &str| {
        let (was, is) = (sample(&before, name), sample(&after, name));
        assert!(is >= was + 1.0, "{name}: {was} -> {is}");
    };
    grew("oswx_collab_changes_accepted_total");
    grew("oswx_collab_decide_seconds_count");
    grew("oswx_collab_journal_batch_records_count");
    grew("oswx_collab_journal_batch_records_sum");
    grew("oswx_collab_journal_wait_seconds_count");
    grew(r#"oswx_collab_journal_wait_seconds_bucket{le="+Inf"}"#);
    grew(r#"oswx_collab_frame_bytes_count{direction="in"}"#);
    grew(r#"oswx_collab_frame_bytes_count{direction="out"}"#);
    grew(r#"oswx_collab_frame_bytes_sum{direction="in"}"#);
    grew("oswx_collab_rooms_loaded_total");
    grew(r#"oswx_collab_changes_rejected_total{code="stale"}"#);
    // The gauges are this server's own.
    assert_eq!(sample(&after, "oswx_collab_rooms"), 1.0);
    assert_eq!(sample(&after, "oswx_collab_connections"), 2.0);
    assert_eq!(sample(&after, "oswx_collab_exports_running"), 0.0);
    assert_eq!(sample(&after, "oswx_collab_blob_uploads_running"), 0.0);
    assert_eq!(sample(&after, "oswx_collab_org_queue_waiting"), 0.0);
    // (The two document connections, and the one that asks.)
    assert!(sample(&after, "oswx_collab_http_connections") >= 3.0);
    assert_eq!(sample(&after, "oswx_collab_http_write_timeouts_total"), 0.0);
    live.close().await;
    other.close().await;
}

#[tokio::test]
async fn no_metric_names_a_user_document_project_or_organisation() {
    let server = TestServer::start_with(|s| s.metrics_token = METRICS_TOKEN.into()).await;
    let world = World::new(&server).await.unwrap();
    let (bob, bob_id) = world.editor(&server, "bob@example.com").await;
    let doc = world
        .document(&server, None, Some("A secret name"), None)
        .await;
    let mut a = Live::connect(&server, &doc, &world.admin).await;
    let mut b = Live::connect(&server, &doc, &bob).await;
    assert_eq!(a.change(vec![set_depth(4)], vec![], vec![]).await.seq(), 1);
    b.drain().await;
    assert_eq!(
        b.change_on(0, vec![set_depth(5)]).await.code(),
        RejectCode::Stale
    );
    let blob = noise(1000, 3);
    assert_eq!(upload_now(&server, &world, &bob, &blob).await.0, 204);
    // A deletion, for the purger's counters.
    let gone = world.document(&server, None, Some("Gone"), None).await;
    assert_eq!(
        server
            .delete(&format!("/api/documents/{gone}"), Some(&world.admin))
            .await
            .0,
        204
    );

    let (status, _, text) = metrics(&server, Some(METRICS_TOKEN)).await;
    assert_eq!(status, 200);
    for secret in [
        world.admin_id.as_str(),
        bob_id.as_str(),
        world.org.as_str(),
        world.project.as_str(),
        doc.as_str(),
        gone.as_str(),
        "secret",
        "bob",
        "example.com",
        METRICS_TOKEN,
        bob.as_str(),
        "127.0.0.1",
    ] {
        assert!(!text.contains(secret), "the metrics say {secret:?}");
    }

    // Every line is a comment or `name{label="value",...} number`; every family is announced;
    // and every label value is one of the few this server can say.
    let allowed: BTreeSet<&str> = [
        // version
        oswx_server::VERSION,
        // code
        "stale",
        "locked",
        "dependency",
        "name_taken",
        "id_taken",
        "invalid",
        "missing_blob",
        "read_only",
        "too_old",
        "server_error",
        // cause
        "account",
        "session",
        "expired",
        "access",
        "document",
        // direction, pool, limit
        "in",
        "out",
        "transactions",
        "reads",
        "journal",
        "server",
    ]
    .into();
    let mut announced = BTreeSet::new();
    let mut samples = 0;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').unwrap();
            assert!(["counter", "gauge", "histogram"].contains(&kind), "{line}");
            assert!(
                announced.insert(name.to_owned()),
                "{name} is announced twice"
            );
            continue;
        }
        if line.starts_with("# HELP ") {
            continue;
        }
        samples += 1;
        let (series, value) = line.rsplit_once(' ').unwrap_or_else(|| panic!("{line:?}"));
        value.parse::<f64>().unwrap_or_else(|_| panic!("{line:?}"));
        let (name, labels) = match series.split_once('{') {
            Some((name, labels)) => (name, labels.strip_suffix('}').expect(line)),
            None => (series, ""),
        };
        assert!(
            name.starts_with("oswx_collab_")
                && name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
            "{line:?}"
        );
        let family = ["_bucket", "_sum", "_count"]
            .iter()
            .find_map(|suffix| name.strip_suffix(suffix))
            .filter(|family| announced.contains(*family))
            .unwrap_or(name);
        assert!(announced.contains(family), "{name} was never announced");
        for label in labels.split(',').filter(|l| !l.is_empty()) {
            let (key, value) = label.split_once('=').unwrap_or_else(|| panic!("{line:?}"));
            let value = value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .unwrap_or_else(|| panic!("{line:?}"));
            if key == "le" {
                assert!(value == "+Inf" || value.parse::<f64>().is_ok(), "{line:?}");
            } else {
                assert!(
                    allowed.contains(value),
                    "an unexpected label value in {line:?}"
                );
            }
        }
    }
    assert!(samples > 100, "only {samples} samples");
    // No run of 32 hex digits (an id, a token's hash) anywhere.
    let longest_hex = text
        .split(|c: char| !c.is_ascii_hexdigit())
        .map(str::len)
        .max()
        .unwrap_or(0);
    assert!(longest_hex < 32, "a run of {longest_hex} hex digits");
    a.close().await;
    b.close().await;
}

// ------------------------------------------------------------------ stopping

/// The seq of the document's newest snapshot at or before `head`.
async fn snapshot_seq(server: &TestServer, doc: &str, head: i64) -> i64 {
    let row = server
        .state
        .store
        .snapshot_at_or_before(doc, head)
        .await
        .unwrap();
    row.expect("a snapshot").seq
}

async fn edit(live: &mut Live, n: i64) -> Seq {
    live.change(vec![set_depth(n)], vec![], vec![]).await.seq()
}

/// Serves `server`'s state on a listener of the test's own, so that the test holds the end of
/// `serve` in its hands; `server.base` is pointed at it.
async fn serve_again(
    server: &mut TestServer,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<Stopped>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    server.base = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = oneshot::channel::<()>();
    let state = server.state.clone();
    let serving = tokio::spawn(async move {
        serve(listener, state, async {
            let _ = stopped.await;
        })
        .await
    });
    (stop, serving)
}

#[tokio::test]
async fn a_stop_closes_every_room_snapshotted_and_every_connection_in_order() {
    let (mut server, world) = world().await;
    let (stop, serving) = serve_again(&mut server).await;
    let one = world.document(&server, None, Some("One"), None).await;
    let two = world.document(&server, None, Some("Two"), None).await;
    let mut a = Live::connect(&server, &one, &world.admin).await;
    let mut b = Live::connect(&server, &two, &world.admin).await;
    for n in 1..=3 {
        assert_eq!(edit(&mut a, n).await, n as Seq);
    }
    assert_eq!(edit(&mut b, 9).await, 1);
    // An idle kept-alive connection, and one in the middle of its request head.
    let idle = reqwest::Client::new();
    assert_eq!(
        idle.get(server.url("/api/health"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let mut half = TcpStream::connect(addr(&server)).await.unwrap();
    half.write_all(b"GET /api/health HTTP/1.1\r\nHost: x\r\n")
        .await
        .unwrap();
    assert!(eventually(|| server.state.http.open() >= 4).await);

    let started = Instant::now();
    stop.send(()).unwrap();
    assert_eq!(a.closed().await.0, 1001);
    assert_eq!(b.closed().await.0, 1001);
    let stopped = tokio::time::timeout(WAIT, serving)
        .await
        .expect("serve ends")
        .unwrap();
    assert_eq!(stopped, Stopped::InOrder);
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "{:?}",
        started.elapsed()
    );
    // Every HTTP connection is gone (the half-sent request included); the two document
    // connections go when their clients have answered the close, or a moment later.
    assert!(server.state.http.open() <= 2, "connections were left open");
    drop((a, b, half));
    assert!(
        eventually(|| server.state.http.open() == 0).await,
        "connections were left open"
    );
    // Nobody is listening any more.
    assert!(TcpStream::connect(addr(&server)).await.is_err());
    assert!(idle.get(server.url("/api/health")).send().await.is_err());
    // Every room's snapshot is at its head, and the journal is stopped.
    for (doc, head) in [(&one, 3), (&two, 1)] {
        let row = server
            .state
            .store
            .snapshot_at_or_before(doc, head)
            .await
            .unwrap()
            .expect("a snapshot");
        assert_eq!(row.seq, head, "the snapshot is at the head");
    }
    assert!(!server.state.journal.healthy());
    assert_eq!(server.state.hub.gauges().rooms, 0);
}

#[tokio::test]
async fn a_stop_that_cannot_finish_ends_at_its_bound_with_the_rooms_closed_first() {
    let mut server = TestServer::start_with(|s| s.shutdown_timeout = 1.5).await;
    let world = World::new(&server).await.unwrap();
    let (stop, serving) = serve_again(&mut server).await;
    let doc = world.document(&server, None, None, None).await;
    let mut live = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(edit(&mut live, 7).await, 1);
    // A request whose steps after its commit never end.
    let gate = server.state.room_hooks.after_commit.clone();
    gate.shut();
    let renaming = {
        let (base, token, project) = (
            server.base.clone(),
            world.admin.clone(),
            world.project.clone(),
        );
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

    let started = Instant::now();
    stop.send(()).unwrap();
    // The room is closed and snapshotted at once, whatever else is stuck.
    assert_eq!(live.closed().await.0, 1001);
    while snapshot_seq(&server, &doc, 1).await != 1 {
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "no snapshot at the head {:?} after the stop",
            started.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let stopped = tokio::time::timeout(WAIT, serving)
        .await
        .expect("serve ends")
        .unwrap();
    assert_eq!(stopped, Stopped::OutOfTime);
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(1400) && took < Duration::from_secs(4),
        "the stop took {took:?}"
    );
    // The journal was still written out and stopped inside the bound.
    assert!(!server.state.journal.healthy());
    gate.open();
    let _ = renaming.await;
}
