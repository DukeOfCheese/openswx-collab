//! What the serving loop adds to the app (`oswx_server::serve`): time limits that cut what
//! stopped and spare what moves, the limit on blob uploads at once, the client address behind
//! proxies, `/metrics`, and the orderly stop.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use common::*;
use futures_util::StreamExt;
use oswx_core::Seq;
use oswx_core::wire::RejectCode;
use oswx_server::blobs::{Blobs, ByteStream, PutOutcome, blob_key};
use oswx_server::serve::{Stopped, TOO_SLOW, serve};
use oswx_server::uploads::Uploads;
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
    // A client that does finish is served as ever; and when it then keeps its connection open
    // and asks nothing more, the connection is closed after the same time.
    assert_eq!(server.get("/api/health", None).await.0, 200);
    let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
    socket
        .write_all(b"GET /api/health HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let answer = read_until(&mut socket, b"}").await;
    assert!(
        answer.starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&answer)
    );
    let started = Instant::now();
    let more = tokio::time::timeout(WAIT, read_to_end(&mut socket))
        .await
        .expect("the idle connection is closed");
    assert_eq!(more, 0);
    // After the limit (half a second here), not before and not long after.
    let idle = started.elapsed();
    assert!(
        idle >= Duration::from_millis(400) && idle < Duration::from_secs(5),
        "the idle connection was closed after {idle:?}"
    );
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

/// A server whose uploads have these limits (server, organisation, account) and wait `queue`
/// for a place; anyone may register and make an organisation.
async fn server_with_uploads(at_once: usize, per_org: usize, per_account: usize) -> TestServer {
    TestServer::start_custom(
        |s| s.open_registration = true,
        |s| {
            let uploads = Uploads::places(at_once, per_org, per_account);
            s.uploads = Arc::new(uploads.queue_wait(Duration::from_millis(300)));
        },
    )
    .await
}

/// Another account with an organisation and a project of its own: its token and a [`World`] to
/// upload into.
async fn another_world(server: &TestServer, email: &str) -> World {
    let (token, id) = server.register(email).await;
    let (status, org) = server
        .post("/api/orgs", Some(&token), json!({"name": email}))
        .await;
    assert_eq!(status, 201, "{org}");
    let org = org["id"].as_str().unwrap().to_owned();
    let (status, project) = server
        .post(
            &format!("/api/orgs/{org}/projects"),
            Some(&token),
            json!({"name": "Theirs"}),
        )
        .await;
    assert_eq!(status, 201, "{project}");
    World {
        admin: token,
        admin_id: id,
        org,
        project: project["id"].as_str().unwrap().to_owned(),
    }
}

#[tokio::test]
async fn only_so_many_uploads_run_at_once_per_account_organisation_and_server() {
    let server = server_with_uploads(2, 2, 1).await;
    let w = World::new(&server).await.unwrap();
    let (bob, _) = w.editor(&server, "bob@example.com").await;
    let (cy, _) = w.editor(&server, "cy@example.com").await;
    let other = another_world(&server, "dee@example.org").await;
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
            .starts_with("you have too many uploads running"),
        "{body}"
    );
    assert_eq!(uploads.running(), 1);
    // Another account of the organisation is not held up by it...
    let (finish_bob, bob_upload) = held_upload(&server, &w, &bob, &two);
    assert!(eventually(|| uploads.running() == 2).await);
    // ... until the organisation has as many as it may: 429 for a third member.
    let (status, body) = upload_now(&server, &w, &cy, &three).await;
    assert_eq!(status, 429, "{body}");
    assert!(
        body["detail"]
            .as_str()
            .unwrap()
            .starts_with("your organisation has too many uploads running"),
        "{body}"
    );
    // And the server has as many as it takes: 503 for someone else's organisation, after a
    // short wait for a place.
    let started = Instant::now();
    let (status, body) = upload_now(&server, &other, &other.admin, &three).await;
    assert_eq!(status, 503, "{body}");
    assert!(
        body["detail"]
            .as_str()
            .unwrap()
            .contains("as many uploads as it takes"),
        "{body}"
    );
    assert!(started.elapsed() >= Duration::from_millis(250));
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
    assert_eq!((uploads.running(), uploads.queued()), (2, 0));

    // The held uploads finish, and the places are free again.
    finish_admin.send(()).unwrap();
    finish_bob.send(()).unwrap();
    assert_eq!(admin_upload.await.unwrap(), 204);
    assert_eq!(bob_upload.await.unwrap(), 204);
    assert!(eventually(|| uploads.running() == 0).await);
    assert_eq!(upload_now(&server, &w, &cy, &three).await.0, 204);
    assert_eq!(upload_now(&server, &w, &w.admin, &three).await.0, 204);
    assert_eq!(
        upload_now(&server, &other, &other.admin, &three).await.0,
        204
    );
    assert!(eventually(|| uploads.running() == 0).await);
    let counted = oswx_server::metrics::counters().uploads_refused;
    assert!(counted.iter().all(|n| *n >= 1), "{counted:?}");
}

#[tokio::test]
async fn an_upload_waits_for_a_place_on_the_server_and_gets_one_that_comes_free() {
    let server = TestServer::start_custom(
        |s| s.open_registration = true,
        |s| s.uploads = Arc::new(Uploads::places(1, 1, 1).queue_wait(Duration::from_secs(20))),
    )
    .await;
    let w = World::new(&server).await.unwrap();
    let other = another_world(&server, "dee@example.org").await;
    let uploads = server.state.uploads.clone();
    let (one, two) = (noise(MIB, 1), noise(MIB, 2));
    let (finish, held) = held_upload(&server, &w, &w.admin, &one);
    assert!(eventually(|| uploads.running() == 1).await);
    let waiting = {
        let request = reqwest::Client::new()
            .put(server.url(&blob_path(&other, &sha_of(&two))))
            .bearer_auth(&other.admin)
            .body(two.clone());
        tokio::spawn(async move { request.send().await.unwrap().status() })
    };
    assert!(eventually(|| uploads.queued() == 1).await);
    assert!(!waiting.is_finished());
    finish.send(()).unwrap();
    assert_eq!(held.await.unwrap(), 204);
    assert_eq!(waiting.await.unwrap(), 204);
    assert!(eventually(|| (uploads.running(), uploads.queued()) == (0, 0)).await);
}

#[tokio::test]
async fn two_sessions_of_one_account_share_its_uploads() {
    let server = server_with_uploads(8, 8, 1).await;
    let w = World::new(&server).await.unwrap();
    // The same person, signed in on another machine.
    let laptop = server.login("ada@example.com", PASSWORD).await;
    assert_ne!(laptop, w.admin);
    let (one, two) = (noise(MIB, 1), noise(MIB, 2));
    let (finish, held) = held_upload(&server, &w, &w.admin, &one);
    assert!(eventually(|| server.state.uploads.running() == 1).await);
    let (status, body) = upload_now(&server, &w, &laptop, &two).await;
    assert_eq!(status, 429, "{body}");
    finish.send(()).unwrap();
    assert_eq!(held.await.unwrap(), 204);
    assert!(eventually(|| server.state.uploads.running() == 0).await);
    assert_eq!(upload_now(&server, &w, &laptop, &two).await.0, 204);
}

#[tokio::test]
async fn an_organisation_at_its_limit_does_not_stop_anothers_upload() {
    let server = server_with_uploads(4, 1, 4).await;
    let w = World::new(&server).await.unwrap();
    let (bob, _) = w.editor(&server, "bob@example.com").await;
    let other = another_world(&server, "dee@example.org").await;
    let (one, two) = (noise(MIB, 1), noise(MIB, 2));
    let (finish, held) = held_upload(&server, &w, &w.admin, &one);
    assert!(eventually(|| server.state.uploads.running() == 1).await);
    // The organisation has its one: a colleague is told so ...
    let (status, body) = upload_now(&server, &w, &bob, &two).await;
    assert_eq!(status, 429, "{body}");
    assert!(
        body["detail"].as_str().unwrap().contains("organisation"),
        "{body}"
    );
    // ... and another organisation uploads as if nothing were (the same blob, even: blobs
    // belong to the organisation).
    assert_eq!(upload_now(&server, &other, &other.admin, &two).await.0, 204);
    assert_eq!(upload_now(&server, &other, &other.admin, &one).await.0, 204);
    finish.send(()).unwrap();
    assert_eq!(held.await.unwrap(), 204);
    assert_eq!(upload_now(&server, &w, &bob, &two).await.0, 204);
}

#[tokio::test]
async fn an_upload_of_a_blob_already_stored_takes_no_place() {
    let server = server_with_uploads(1, 1, 1).await;
    let w = World::new(&server).await.unwrap();
    let uploads = server.state.uploads.clone();
    let (stored, other) = (noise(MIB, 41), noise(MIB, 42));
    assert_eq!(upload_now(&server, &w, &w.admin, &stored).await.0, 204);
    // The one place, and the account's one, taken by an upload that is running.
    let (finish, held) = held_upload(&server, &w, &w.admin, &other);
    assert!(eventually(|| uploads.running() == 1).await);
    let charged = uploads.memory_held();
    // The blob already stored is sent again: read, checked and answered, charged nothing.
    assert_eq!(upload_now(&server, &w, &w.admin, &stored).await.0, 204);
    assert_eq!((uploads.running(), uploads.memory_held()), (1, charged));
    // Checked all the same: a body that is not the blob is refused.
    let wrong = reqwest::Client::new()
        .put(server.url(&blob_path(&w, &sha_of(&stored))))
        .bearer_auth(&w.admin)
        .body(other.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 400);
    finish.send(()).unwrap();
    assert_eq!(held.await.unwrap(), 204);
}

/// An upload of `body` by `w`'s admin in pieces of `piece` bytes, `gap` apart, over a socket of
/// the test's own (so that an answer that comes before the body is all sent is read, whatever the
/// server then does with the connection): its status and its body's `detail`.
async fn paced_upload(
    server: &TestServer,
    w: &World,
    body: &[u8],
    piece: usize,
    gap: Duration,
) -> (u16, String) {
    paced_upload_framed(server, w, body, piece, gap, false).await
}

/// [`paced_upload`], `chunked` (no `Content-Length`: the server cannot know the size).
async fn paced_upload_framed(
    server: &TestServer,
    w: &World,
    body: &[u8],
    piece: usize,
    gap: Duration,
    chunked: bool,
) -> (u16, String) {
    let mut socket = TcpStream::connect(addr(server)).await.unwrap();
    let framing = if chunked {
        "Transfer-Encoding: chunked".to_owned()
    } else {
        format!("Content-Length: {}", body.len())
    };
    let head = format!(
        "PUT {} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\n{framing}\r\n\
         Connection: close\r\n\r\n",
        blob_path(w, &sha_of(body)),
        addr(server),
        w.admin,
    );
    socket.write_all(head.as_bytes()).await.unwrap();
    let mut answer = Vec::new();
    let mut buf = vec![0u8; 4096];
    let pieces = body.chunks(piece).count();
    for (i, piece) in body.chunks(piece).enumerate() {
        let framed = if chunked {
            let mut framed = format!("{:x}\r\n", piece.len()).into_bytes();
            framed.extend_from_slice(piece);
            framed.extend_from_slice(b"\r\n");
            if i + 1 == pieces {
                framed.extend_from_slice(b"0\r\n\r\n");
            }
            framed
        } else {
            piece.to_vec()
        };
        if socket.write_all(&framed).await.is_err() {
            break;
        }
        // The gap, spent listening: an answer now means the server has given the upload up.
        if let Ok(Ok(n)) = tokio::time::timeout(gap, socket.read(&mut buf)).await {
            answer.extend_from_slice(&buf[..n]);
            break;
        }
    }
    let rest = tokio::time::timeout(WAIT, async {
        let mut rest = Vec::new();
        let _ = socket.read_to_end(&mut rest).await;
        rest
    })
    .await
    .expect("the server answers");
    answer.extend_from_slice(&rest);
    let answer = String::from_utf8_lossy(&answer).into_owned();
    let status = answer
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status in {answer:?}"));
    let detail = answer
        .split_once("{\"detail\":\"")
        .map(|(_, rest)| rest.split('"').next().unwrap_or_default().to_owned())
        .unwrap_or_default();
    (status, detail)
}

#[tokio::test]
async fn an_upload_that_drips_is_given_up_on_and_frees_its_place() {
    // No idle limit to speak of: only the rate can end a drip. 2,000 bytes a second, with a
    // second to spare.
    let server = TestServer::start_custom(
        |s| {
            s.blob_idle_timeout = 300.0;
            s.min_upload_rate = 2000;
            s.upload_rate_grace = 1.0;
        },
        |s| s.uploads = Arc::new(Uploads::places(2, 2, 2).queue_wait(Duration::from_millis(100))),
    )
    .await;
    let w = World::new(&server).await.unwrap();
    let uploads = server.state.uploads.clone();
    // The attack: every place taken by an upload that sends ten bytes five times a second
    // (never idle, 50 bytes a second).
    let body = noise(64 << 10, 7);
    let started = Instant::now();
    let (first, second) = (noise(64 << 10, 70), noise(64 << 10, 71));
    let gap = Duration::from_millis(200);
    let drips = async {
        tokio::join!(
            paced_upload(&server, &w, &first, 10, gap),
            paced_upload(&server, &w, &second, 10, gap),
        )
    };
    let others = async {
        assert!(eventually(|| uploads.running() == 2).await);
        // While they hold the places nobody else gets in ...
        assert_eq!(upload_now(&server, &w, &w.admin, &body).await.0, 429);
    };
    // ... but not for long: each is given up on about a second after it began.
    let ((one, two), ()) = tokio::join!(drips, others);
    for (status, detail) in [one, two] {
        assert_eq!(
            (status, detail.as_str()),
            (400, "the upload was too slow and was given up on")
        );
    }
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(900) && took < Duration::from_secs(8),
        "{took:?}"
    );
    assert!(
        eventually(|| uploads.running() == 0).await,
        "the places were kept"
    );
    // Nothing of them is in storage, and an honest upload goes through.
    assert_eq!(upload_now(&server, &w, &w.admin, &body).await.0, 204);
    let stored = server.stored_under("").await;
    assert_eq!(stored.len(), 1, "{stored:?}");
}

#[tokio::test]
async fn an_honest_slow_upload_is_not_cut_by_the_minimum_rate() {
    let server = TestServer::start_with(|s| {
        s.min_upload_rate = 2000;
        s.upload_rate_grace = 1.0;
        s.request_timeout = 1.0;
    })
    .await;
    let w = World::new(&server).await.unwrap();
    // 4,000 bytes every 250 ms: 16,000 bytes a second, for five seconds, five times the grace.
    let body = noise(80_000, 11);
    let started = Instant::now();
    let (status, answer) = paced_upload(&server, &w, &body, 4000, Duration::from_millis(250)).await;
    assert_eq!(status, 204, "{answer}");
    assert!(started.elapsed() > Duration::from_secs(4));
    // A pause shorter than the grace, once the sender has earned it, is forgiven: 700 ms
    // between pieces of 4,000 bytes (two seconds' worth each).
    let body = noise(24_000, 12);
    let (status, answer) = paced_upload(&server, &w, &body, 4000, Duration::from_millis(700)).await;
    assert_eq!(status, 204, "{answer}");
    // A little above the floor is enough: 600 bytes every 250 ms, 2,400 bytes a second. (Not
    // right at it: a scheduler that is late by a few milliseconds a gap would put it below.)
    let body = noise(6000, 13);
    let (status, answer) = paced_upload(&server, &w, &body, 600, Duration::from_millis(250)).await;
    assert_eq!(status, 204, "{answer}");
    // Below it is not: 100 bytes every 250 ms (400 bytes a second).
    let body = noise(6000, 14);
    let (status, answer) = paced_upload(&server, &w, &body, 100, Duration::from_millis(250)).await;
    assert_eq!(
        (status, answer.as_str()),
        (400, "the upload was too slow and was given up on")
    );
    // And with no floor set, even that is taken (the idle limit is all there is).
    let server = TestServer::start_with(|s| s.min_upload_rate = 0).await;
    let w = World::new(&server).await.unwrap();
    let body = noise(1000, 15);
    let (status, answer) = paced_upload(&server, &w, &body, 100, Duration::from_millis(250)).await;
    assert_eq!(status, 204, "{answer}");
}

#[tokio::test]
async fn an_upload_that_takes_longer_than_it_is_allowed_is_given_up_on() {
    // At least a second, or the size at 20,000 bytes a second; a floor the uploads below keep up.
    let server = TestServer::start_with(|s| {
        s.upload_time_limit = 1.0;
        s.upload_time_rate = 20_000;
        s.min_upload_rate = 500;
        s.upload_rate_grace = 1.0;
    })
    .await;
    let w = World::new(&server).await.unwrap();
    let uploads = server.state.uploads.clone();
    // 60,000 bytes may take three seconds: at 30,000 bytes a second (two) it is stored, well
    // past the one-second least.
    let body = noise(60_000, 21);
    let started = Instant::now();
    let (status, answer) = paced_upload(&server, &w, &body, 6000, Duration::from_millis(200)).await;
    assert_eq!(status, 204, "{answer}");
    assert!(started.elapsed() > Duration::from_millis(1500));
    // At 15,000 bytes a second (four seconds) it is given up on at three, though it never
    // stopped and never fell below the floor; its place comes free and nothing is stored.
    let body = noise(60_000, 22);
    let started = Instant::now();
    let (status, answer) = paced_upload(&server, &w, &body, 3000, Duration::from_millis(200)).await;
    assert_eq!(
        (status, answer.as_str()),
        (400, "the upload took too long and was given up on")
    );
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(2800) && took < Duration::from_millis(3900),
        "{took:?}"
    );
    // A small one has the least: 2,000 bytes at 800 bytes a second take 2.5 s, over the second.
    let body = noise(2000, 23);
    let (status, answer) = paced_upload(&server, &w, &body, 200, Duration::from_millis(250)).await;
    assert_eq!(
        (status, answer.as_str()),
        (400, "the upload took too long and was given up on")
    );
    assert!(
        eventually(|| uploads.running() == 0).await,
        "a place was kept"
    );
    let stored = server.stored_under("").await;
    assert_eq!(stored.len(), 1, "{stored:?}");
    assert!(oswx_server::metrics::counters().uploads_cut_short >= 2);
}

#[tokio::test]
async fn an_upload_that_does_not_say_its_size_has_the_time_of_the_largest_blob_and_no_more() {
    // The largest blob 40,000 bytes: an upload of unknown size may take four seconds (at 10,000
    // bytes a second), whatever it really holds.
    let server = TestServer::start_with(|s| {
        s.max_blob_bytes = 40_000;
        s.upload_time_limit = 1.0;
        s.upload_time_rate = 10_000;
        s.min_upload_rate = 500;
        s.upload_rate_grace = 1.0;
    })
    .await;
    let w = World::new(&server).await.unwrap();
    // 12,000 bytes, chunked, at 7,500 bytes a second (1.6 s): stored, past the one-second least
    // and well within the four seconds.
    let body = noise(12_000, 51);
    let started = Instant::now();
    let (status, answer) =
        paced_upload_framed(&server, &w, &body, 1500, Duration::from_millis(200), true).await;
    assert_eq!(status, 204, "{answer}");
    assert!(started.elapsed() > Duration::from_millis(1200));
    // 36,000 bytes, chunked, at 5,000 bytes a second (seven seconds): given up on at four, though
    // it never stopped and kept well above the floor; nothing is stored.
    let body = noise(36_000, 52);
    let started = Instant::now();
    let (status, answer) =
        paced_upload_framed(&server, &w, &body, 1000, Duration::from_millis(200), true).await;
    assert_eq!(
        (status, answer.as_str()),
        (400, "the upload took too long and was given up on")
    );
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(3800) && took < Duration::from_millis(6000),
        "{took:?}"
    );
    assert!(eventually(|| server.state.uploads.running() == 0).await);
    assert_eq!(server.stored_under("").await.len(), 1);
}

/// Storage that takes a whole body and then does not answer for a minute before it stores it.
struct SlowStore(Arc<dyn Blobs>);

#[async_trait]
impl Blobs for SlowStore {
    async fn put_stream(
        &self,
        key: &str,
        mut body: ByteStream,
        limit: u64,
    ) -> anyhow::Result<PutOutcome> {
        let mut whole = Vec::new();
        while let Some(chunk) = body.next().await {
            whole.extend_from_slice(&chunk?);
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
        self.0.put_stream(key, chunked(&whole, MIB), limit).await
    }
    async fn get_stream(&self, key: &str) -> anyhow::Result<Option<(u64, ByteStream)>> {
        self.0.get_stream(key).await
    }
    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        self.0.exists(key).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.0.put(key, bytes).await
    }
    async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
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
    fn upload_memory(&self) -> u64 {
        self.0.upload_memory()
    }
}

#[tokio::test]
async fn an_upload_whose_storage_does_not_answer_is_given_up_on_in_its_time() {
    let server = TestServer::start_custom(
        |s| {
            s.upload_time_limit = 1.0;
            s.request_timeout = 30.0;
        },
        |s| {
            s.blobs = Arc::new(SlowStore(s.blobs.clone()));
            s.uploads = Arc::new(Uploads::places(1, 1, 1));
        },
    )
    .await;
    let w = World::new(&server).await.unwrap();
    let uploads = server.state.uploads.clone();
    // The body is all sent at once; the storage then holds the upload up.
    let body = noise(64 << 10, 53);
    let started = Instant::now();
    let (status, answer) = upload_now(&server, &w, &w.admin, &body).await;
    assert_eq!(
        (status, answer["detail"].as_str()),
        (
            StatusCode::BAD_REQUEST,
            Some("the upload took too long and was given up on")
        )
    );
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(900) && took < Duration::from_secs(5),
        "{took:?}"
    );
    // Its place came back, and nothing was stored.
    assert!(eventually(|| uploads.running() == 0).await);
    assert!(server.stored_under("").await.is_empty());
}

#[tokio::test]
async fn the_servers_room_for_uploads_is_memory_charged_by_what_its_storage_holds() {
    // 12 MiB of room. The test storage (a folder, or a bucket when the tests run on S3) says
    // what one upload holds; the server fits as many as that allows, and no more.
    let server = TestServer::start_custom(
        |s| {
            s.upload_memory = 12 << 20;
            s.open_registration = true;
        },
        |s| {
            let uploads = Uploads::new(12 << 20, s.blobs.upload_memory(), 8, 8);
            s.uploads = Arc::new(uploads.queue_wait(Duration::from_millis(100)));
        },
    )
    .await;
    let held = server.state.blobs.upload_memory();
    let fit = server.state.uploads.fit();
    assert_eq!(fit as u64, ((12u64 << 20) / held).max(1), "{held}");
    // As the state builds it from the settings, too.
    let built = oswx_server::AppState::new(
        (*server.state.settings).clone(),
        server.state.store.clone(),
        server.state.blobs.clone(),
    );
    assert_eq!(built.uploads.fit(), fit);
    built.shutdown().await;
    let w = World::new(&server).await.unwrap();
    let mut finishers = Vec::new();
    let mut tasks = Vec::new();
    let mut members = Vec::new();
    for i in 0..fit {
        let (token, _) = w.editor(&server, &format!("m{i}@example.com")).await;
        members.push(token);
    }
    for (i, token) in members.iter().enumerate() {
        let (finish, task) = held_upload(&server, &w, token, &noise(MIB, 30 + i as u64));
        finishers.push(finish);
        tasks.push(task);
    }
    let uploads = server.state.uploads.clone();
    assert!(eventually(|| uploads.running() == fit).await);
    assert_eq!(
        uploads.memory_held(),
        fit as u64 * held.div_ceil(1024) * 1024
    );
    let other = another_world(&server, "dee@example.org").await;
    let (status, body) = upload_now(&server, &other, &other.admin, &noise(MIB, 99)).await;
    assert_eq!(status, 503, "{body}");
    for finish in finishers {
        finish.send(()).unwrap();
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), 204);
    }
    assert_eq!(
        upload_now(&server, &other, &other.admin, &noise(MIB, 99))
            .await
            .0,
        204
    );
}

#[tokio::test]
async fn an_upload_whose_client_goes_away_or_fails_gives_its_place_back() {
    let server = TestServer::start_custom(
        |s| s.blob_idle_timeout = 0.5,
        |s| s.uploads = Arc::new(Uploads::places(1, 1, 1)),
    )
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

/// The status of `n` failed sign-ins for `email` sent at once, the try `i` carrying the header
/// `forwarded(i)`, as (refused as wrong, refused by a limit).
async fn failed_sign_ins_at_once(
    server: &TestServer,
    email: &str,
    n: usize,
    forwarded: impl Fn(usize) -> Option<String>,
) -> (usize, usize) {
    let tries = (0..n).map(|i| {
        let header = forwarded(i);
        async move { failed_sign_in(server, email, header.as_deref()).await }
    });
    let statuses = futures_util::future::join_all(tries).await;
    let wrong = statuses
        .iter()
        .filter(|s| **s == StatusCode::UNAUTHORIZED)
        .count();
    let limited = statuses
        .iter()
        .filter(|s| **s == StatusCode::TOO_MANY_REQUESTS)
        .count();
    assert_eq!(wrong + limited, n, "{statuses:?}");
    (wrong, limited)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sign_ins_sent_at_once_cannot_all_get_past_the_limit() {
    let server = TestServer::start_with(|s| s.trusted_proxies = vec![]).await;
    World::new(&server).await.unwrap();
    // Each is counted before its password is checked: of thirty at once, ten are checked.
    let (wrong, limited) = failed_sign_ins_at_once(&server, "a@example.com", 30, |_| None).await;
    assert!(wrong <= 10, "{wrong} were checked");
    assert_eq!(wrong, 10);
    assert_eq!(limited, 20);
}

#[tokio::test]
async fn an_ipv6_client_is_counted_by_its_slash_64() {
    let server = TestServer::start_with(|s| {
        s.trusted_proxies = vec!["127.0.0.0/8".parse().unwrap()];
    })
    .await;
    World::new(&server).await.unwrap();
    // Another address in the same /64 on every try: one client.
    let rotating = |i: usize| Some(format!("2001:db8:1:2::{:x}", i + 1));
    assert_eq!(
        tries_before_the_limit(&server, "a@example.com", rotating).await,
        10
    );
    // Clients in other /64s are counted apart.
    let apart = |i: usize| Some(format!("2001:db8:1:{:x}::1", i + 100));
    assert_eq!(
        tries_before_the_limit(&server, "b@example.com", apart).await,
        12
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_email_address_is_limited_whatever_the_client() {
    let server = TestServer::start_with(|s| {
        s.trusted_proxies = vec!["127.0.0.0/8".parse().unwrap()];
    })
    .await;
    World::new(&server).await.unwrap();
    // A hundred clients, a guess each: all checked.
    let client = |i: usize| Some(format!("203.0.{}.{}", i / 200, i % 200 + 1));
    let (wrong, limited) = failed_sign_ins_at_once(&server, "a@example.com", 100, client).await;
    assert_eq!((wrong, limited), (100, 0));
    // The hundred and first, from a client never seen before: refused unchecked.
    assert_eq!(
        failed_sign_in(&server, "a@example.com", Some("198.51.100.7")).await,
        StatusCode::TOO_MANY_REQUESTS
    );
    // Another address is not.
    assert_eq!(
        failed_sign_in(&server, "b@example.com", Some("198.51.100.7")).await,
        StatusCode::UNAUTHORIZED
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
    // Not as a query, not as another scheme, not with anything but one space before it.
    for request in [
        reqwest::Client::new()
            .get(server.url("/metrics"))
            .header("authorization", format!("Bearer  {METRICS_TOKEN}")),
        reqwest::Client::new()
            .get(server.url("/metrics"))
            .header("authorization", format!("Bearer\t{METRICS_TOKEN}")),
        reqwest::Client::new()
            .get(server.url("/metrics"))
            .header("authorization", format!("Bearer {METRICS_TOKEN} x")),
        reqwest::Client::new()
            .get(server.url("/metrics"))
            .header("authorization", format!("Bearer {METRICS_TOKEN},")),
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
    // (White space after the header's value is not part of it: HTTP takes it off before the
    // server sees it, so a trailing space changes nothing.)
    let r = reqwest::Client::new()
        .get(server.url("/metrics"))
        .header("authorization", format!("Bearer {METRICS_TOKEN} "))
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
        "organisation",
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
    let mut server = TestServer::start_with(|s| s.shutdown_timeout = 3.0).await;
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
    // The request gets half the bound to finish (it never does); then the room is closed and
    // snapshotted, whatever else is stuck.
    assert_eq!(live.closed().await.0, 1001);
    assert!(
        started.elapsed() >= Duration::from_millis(1400),
        "the room was closed {:?} after the stop, before the request had its time",
        started.elapsed()
    );
    while snapshot_seq(&server, &doc, 1).await != 1 {
        assert!(
            started.elapsed() < Duration::from_millis(2500),
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
        took >= Duration::from_millis(2900) && took < Duration::from_secs(6),
        "the stop took {took:?}"
    );
    // The journal was still written out and stopped inside the bound.
    assert!(!server.state.journal.healthy());
    gate.open();
    let _ = renaming.await;
}

// ------------------------------------------------------------------ fix round 1

/// Sends a document creation whose body arrives in pieces of `piece` bytes, `gap` apart; answers
/// the status line and body of the answer.
async fn paced_creation(
    server: &TestServer,
    w: &World,
    body: &[u8],
    piece: usize,
    gap: Duration,
) -> String {
    let mut socket = TcpStream::connect(addr(server)).await.unwrap();
    let head = format!(
        "POST /api/projects/{}/documents HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        w.project,
        addr(server),
        w.admin,
        body.len()
    );
    socket.write_all(head.as_bytes()).await.unwrap();
    let mut answer = Vec::new();
    let mut buf = vec![0u8; 4096];
    for piece in body.chunks(piece) {
        if socket.write_all(piece).await.is_err() {
            break;
        }
        if let Ok(Ok(n)) = tokio::time::timeout(gap, socket.read(&mut buf)).await {
            answer.extend_from_slice(&buf[..n]);
            break;
        }
    }
    let rest = tokio::time::timeout(WAIT, async {
        let mut rest = Vec::new();
        let _ = socket.read_to_end(&mut rest).await;
        rest
    })
    .await
    .expect("the server answers");
    answer.extend_from_slice(&rest);
    String::from_utf8_lossy(&answer).into_owned()
}

#[tokio::test]
async fn a_body_that_keeps_coming_does_not_count_against_the_request_limit() {
    // Half a second to answer; 2,000 bytes a second is fast enough.
    let server = TestServer::start_with(|s| {
        s.request_timeout = 0.5;
        s.min_upload_rate = 2000;
    })
    .await;
    let w = World::new(&server).await.unwrap();
    let mut content = part();
    content["note"] = json!("x".repeat(30_000));
    let body = serde_json::to_vec(&json!({"name": "Slow but sure", "content": content})).unwrap();
    assert!(body.len() > 30_000);
    // 2,000 bytes every 100 ms: 20,000 bytes a second, a body that takes over 1.5 s to arrive,
    // three times the limit.
    let started = Instant::now();
    let answer = paced_creation(&server, &w, &body, 2000, Duration::from_millis(100)).await;
    assert!(answer.starts_with("HTTP/1.1 201"), "{answer}");
    assert!(started.elapsed() > Duration::from_millis(1400));
    assert_eq!(server.state.http.request_timeouts(), 0);

    // The same body at 200 bytes a second (20 bytes every 100 ms) is below the rate: the clock
    // is hardly put forward, and the request is answered 503 a little after the limit.
    let started = Instant::now();
    let answer = paced_creation(&server, &w, &body, 20, Duration::from_millis(100)).await;
    assert!(answer.starts_with("HTTP/1.1 503"), "{answer}");
    assert!(answer.contains(TOO_SLOW), "{answer}");
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(450) && took < Duration::from_secs(3),
        "{took:?}"
    );
    assert_eq!(server.state.http.request_timeouts(), 1);
    // Once the body is all there, the handler has the whole limit again, however long the body
    // took: nothing was cut in the first request, which ended 1.5 s after its head.
    let documents: i64 = server.scalar("SELECT COUNT(*) FROM documents").await;
    assert_eq!(documents, 1);
}

#[tokio::test]
async fn only_so_many_large_bodies_are_read_at_once() {
    let server = TestServer::start_with(|s| s.max_large_bodies = 1).await;
    let w = World::new(&server).await.unwrap();
    let large = |name: &str| {
        let mut content = part();
        content["note"] = json!("x".repeat(1_200_000));
        serde_json::to_vec(&json!({"name": name, "content": content})).unwrap()
    };
    let small = |name: &str| serde_json::to_vec(&json!({"name": name, "content": part()})).unwrap();
    let create = |body: Vec<u8>| {
        reqwest::Client::new()
            .post(server.url(&format!("/api/projects/{}/documents", w.project)))
            .bearer_auth(&w.admin)
            .header("content-type", "application/json")
            .body(body)
            .send()
    };
    // A large body on its way (half of it sent) holds the only place.
    let first = large("First");
    let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
    let head = format!(
        "POST /api/projects/{}/documents HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        w.project,
        addr(&server),
        w.admin,
        first.len()
    );
    socket.write_all(head.as_bytes()).await.unwrap();
    let (sent, rest) = first.split_at(first.len() / 2);
    socket.write_all(sent).await.unwrap();
    assert!(eventually(|| server.state.http.in_handlers() == 1).await);
    // Another large one is answered 503 at once; so is one sent without a length.
    let refused = create(large("Second")).await.unwrap();
    assert_eq!(refused.status(), 503);
    let detail: Value = refused.json().await.unwrap();
    assert_eq!(detail["detail"], oswx_server::routes::LARGE_BODIES_BUSY);
    let chunk = Bytes::from(small("Chunked"));
    let unsized_body = reqwest::Body::wrap_stream(futures_util::stream::once(async move {
        Ok::<_, io::Error>(chunk)
    }));
    let chunked = reqwest::Client::new()
        .post(server.url(&format!("/api/projects/{}/documents", w.project)))
        .bearer_auth(&w.admin)
        .header("content-type", "application/json")
        .body(unsized_body)
        .send()
        .await
        .unwrap();
    assert_eq!(chunked.status(), 503);
    // A small one is not held up.
    assert_eq!(create(small("Small")).await.unwrap().status(), 201);
    // The first ends, and the place is free again.
    socket.write_all(rest).await.unwrap();
    let mut answer = Vec::new();
    tokio::time::timeout(WAIT, socket.read_to_end(&mut answer))
        .await
        .expect("answered")
        .unwrap();
    assert!(
        answer.starts_with(b"HTTP/1.1 201"),
        "{}",
        String::from_utf8_lossy(&answer)
    );
    assert_eq!(create(large("Third")).await.unwrap().status(), 201);
    assert!(oswx_server::metrics::counters().large_bodies_refused >= 2);
}

const HEALTH: &[u8] = b"GET /api/health HTTP/1.1\r\nHost: x\r\n\r\n";

/// Asks for `/api/health` on `socket` (kept alive); answers the status, or `None` when the
/// connection was closed instead.
async fn health_on(socket: &mut TcpStream) -> Option<u16> {
    socket.write_all(HEALTH).await.ok()?;
    let answer = tokio::time::timeout(WAIT, read_until(socket, b"}"))
        .await
        .expect("an answer or the end of the connection");
    let answer = String::from_utf8_lossy(&answer).into_owned();
    answer.split(' ').nth(1).and_then(|code| code.parse().ok())
}

/// Whether the server closed `socket` (it reads the end, not an answer).
async fn closed_by_server(socket: &mut TcpStream) -> bool {
    tokio::time::timeout(WAIT, read_to_end(socket))
        .await
        .expect("the connection is closed")
        == 0
}

#[tokio::test]
async fn a_new_connection_takes_the_place_of_one_that_never_finished_its_head() {
    let server = TestServer::start_with(|s| s.max_connections = 3).await;
    let stats = server.state.http.clone();
    assert!(eventually(|| stats.open() == 0).await);
    // The attack: sockets that are opened and sent a byte now and then, never a whole head.
    let mut held = Vec::new();
    for _ in 0..3 {
        let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
        socket.write_all(b"GET /api/he").await.unwrap();
        held.push(socket);
        // (So that they are taken in this order.)
        assert!(eventually(|| stats.open() == held.len()).await);
    }
    // Someone else still gets in: the oldest of them makes room, and is closed.
    let mut newcomer = TcpStream::connect(addr(&server)).await.unwrap();
    assert_eq!(health_on(&mut newcomer).await, Some(200));
    assert!(closed_by_server(&mut held[0]).await);
    assert_eq!((stats.evictions(), stats.refusals()), (1, 0));
    // And again: the next oldest.
    let mut another = TcpStream::connect(addr(&server)).await.unwrap();
    assert_eq!(health_on(&mut another).await, Some(200));
    assert!(closed_by_server(&mut held[1]).await);
    assert!(eventually(|| stats.open() == 3).await);
    // Those that asked for something are kept: the third trickler goes before either.
    let mut third = TcpStream::connect(addr(&server)).await.unwrap();
    assert_eq!(health_on(&mut third).await, Some(200));
    assert!(closed_by_server(&mut held[2]).await);
    assert_eq!(health_on(&mut newcomer).await, Some(200));
    assert_eq!(health_on(&mut another).await, Some(200));
    assert_eq!(stats.evictions(), 3);
}

#[tokio::test]
async fn failing_that_the_connection_idle_longest_makes_room() {
    let server = TestServer::start_with(|s| s.max_connections = 3).await;
    let stats = server.state.http.clone();
    assert!(eventually(|| stats.open() == 0).await);
    let mut kept = Vec::new();
    for _ in 0..3 {
        let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
        assert_eq!(health_on(&mut socket).await, Some(200));
        kept.push(socket);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    // The first is used again: the second is now the one idle longest.
    assert_eq!(health_on(&mut kept[0]).await, Some(200));
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut newcomer = TcpStream::connect(addr(&server)).await.unwrap();
    assert_eq!(health_on(&mut newcomer).await, Some(200));
    assert!(closed_by_server(&mut kept[1]).await);
    assert_eq!(health_on(&mut kept[0]).await, Some(200));
    assert_eq!(health_on(&mut kept[2]).await, Some(200));
    assert_eq!((stats.evictions(), stats.refusals()), (1, 0));
}

#[tokio::test]
async fn a_download_its_client_has_stopped_reading_is_not_closed_to_make_room() {
    // The write limit far above the test, so that only eviction could end the download; idle
    // kept-alive connections of the set-up close after a second.
    let server = TestServer::start_with(|s| {
        s.max_connections = 2;
        s.write_timeout = 120.0;
        s.blob_idle_timeout = 300.0;
        s.request_timeout = 1.0;
    })
    .await;
    let w = World::new(&server).await.unwrap();
    let body = noise(48 * MIB, 29);
    let sha = store_blob(&server, &w, &body).await;
    let stats = server.state.http.clone();
    assert!(eventually(|| stats.open() == 0).await);
    // A download whose client reads the head and then nothing for now: once the buffers are
    // full nothing moves on it.
    let mut download = TcpStream::connect(addr(&server)).await.unwrap();
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
        blob_path(&w, &sha),
        w.admin
    );
    download.write_all(request.as_bytes()).await.unwrap();
    let head = read_until(&mut download, b"\r\n\r\n").await;
    assert!(head.starts_with(b"HTTP/1.1 200"));
    let mut got = head.len() - (head.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4);
    tokio::time::sleep(Duration::from_millis(500)).await;
    // A connection used after it: so the download is the one idle longest by its last write.
    let mut used = TcpStream::connect(addr(&server)).await.unwrap();
    assert_eq!(health_on(&mut used).await, Some(200));
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The server is full: a newcomer takes the place of the one used, not of the download.
    let mut newcomer = TcpStream::connect(addr(&server)).await.unwrap();
    assert_eq!(health_on(&mut newcomer).await, Some(200));
    assert!(closed_by_server(&mut used).await);
    assert_eq!(stats.evictions(), 1);
    // The client reads again, and gets the whole blob.
    got += tokio::time::timeout(WAIT, read_to_end(&mut download))
        .await
        .expect("the download ends");
    assert_eq!(got, body.len(), "the download was cut short");
    assert_eq!(stats.write_timeouts(), 0);
}

#[tokio::test]
async fn with_every_connection_busy_the_health_check_still_gets_through_and_nothing_else() {
    let server = TestServer::start_with(|s| s.max_connections = 2).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let other_doc = world.document(&server, None, None, None).await;
    let stats = server.state.http.clone();
    // Two document connections: the server holds no more, and neither may be closed for another.
    let mut first = Live::connect(&server, &doc, &world.admin).await;
    let mut second = Live::connect(&server, &other_doc, &world.admin).await;
    assert!(eventually(|| stats.open() == 2).await);
    let evicted = stats.evictions();
    // A health check is answered, on a connection kept for it, which is then closed.
    let mut check = TcpStream::connect(addr(&server)).await.unwrap();
    check.write_all(HEALTH).await.unwrap();
    let answer = tokio::time::timeout(WAIT, async {
        let mut all = Vec::new();
        let _ = check.read_to_end(&mut all).await;
        all
    })
    .await
    .expect("answered and closed");
    assert!(answer.starts_with(b"HTTP/1.1 200"), "{answer:?}");
    // Anything else on such a connection is 503.
    let mut other = TcpStream::connect(addr(&server)).await.unwrap();
    other
        .write_all(b"GET /api/server HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let answer = String::from_utf8_lossy(&read_until(&mut other, b"}").await).into_owned();
    assert!(answer.starts_with("HTTP/1.1 503"), "{answer}");
    assert!(answer.contains(oswx_server::serve::FULL), "{answer}");
    // The reserve filled by sockets that send nothing: the oldest of them makes room.
    let mut idle = Vec::new();
    for _ in 0..oswx_server::serve::HEALTH_RESERVE {
        idle.push(TcpStream::connect(addr(&server)).await.unwrap());
        assert!(eventually(|| stats.open() == 2 + idle.len()).await);
    }
    let mut check = TcpStream::connect(addr(&server)).await.unwrap();
    assert_eq!(health_on(&mut check).await, Some(200));
    assert!(closed_by_server(&mut idle[0]).await);
    assert_eq!(stats.evictions(), evicted + 1);
    // The document connections were never touched.
    assert_eq!(edit(&mut first, 1).await, 1);
    assert_eq!(edit(&mut second, 2).await, 1);
}

#[tokio::test]
async fn an_address_full_of_sockets_that_send_nothing_still_lets_a_newcomer_from_it_in() {
    let server = TestServer::start_with(|s| {
        s.trusted_proxies = vec![];
        s.max_connections_per_address = 2;
    })
    .await;
    let stats = server.state.http.clone();
    assert!(eventually(|| stats.open() == 0).await);
    // The address's limit taken by sockets that never finish a head (the server is far from
    // full).
    let mut held = Vec::new();
    for _ in 0..2 {
        let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
        socket.write_all(b"GET /api/he").await.unwrap();
        held.push(socket);
        assert!(eventually(|| stats.open() == held.len()).await);
    }
    // Someone else at that address still gets in: the oldest of them makes room.
    let mut newcomer = TcpStream::connect(addr(&server)).await.unwrap();
    assert_eq!(health_on(&mut newcomer).await, Some(200));
    assert!(closed_by_server(&mut held[0]).await);
    // And again: the other one goes before the newcomer, which asked for something.
    let mut another = TcpStream::connect(addr(&server)).await.unwrap();
    assert_eq!(health_on(&mut another).await, Some(200));
    assert!(closed_by_server(&mut held[1]).await);
    assert_eq!(health_on(&mut newcomer).await, Some(200));
    assert_eq!(
        (
            stats.evictions(),
            stats.refusals_for_address(),
            stats.refusals()
        ),
        (2, 0, 0)
    );
}

#[tokio::test]
async fn one_address_holds_only_so_many_unless_it_is_a_trusted_proxy() {
    let server = TestServer::start_with(|s| {
        s.trusted_proxies = vec![];
        s.max_connections_per_address = 2;
    })
    .await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let other_doc = world.document(&server, None, None, None).await;
    let stats = server.state.http.clone();
    // Two document connections from the address: neither may be closed for another.
    let mut first = Live::connect(&server, &doc, &world.admin).await;
    let mut second = Live::connect(&server, &other_doc, &world.admin).await;
    assert!(eventually(|| stats.open() == 2).await);
    // So one more from there is closed at once, whatever it asks.
    let refused = stats.refusals_for_address();
    let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
    assert_eq!(health_on(&mut socket).await, None);
    assert_eq!(stats.refusals_for_address(), refused + 1);
    assert_eq!(edit(&mut first, 1).await, 1);
    assert_eq!(edit(&mut second, 2).await, 1);
    // One goes: there is room for another from there.
    drop(first);
    assert!(eventually(|| stats.open() == 1).await);
    let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
    assert_eq!(health_on(&mut socket).await, Some(200));

    // A trusted proxy's address is not limited: every client behind it shares it.
    let server = TestServer::start_with(|s| s.max_connections_per_address = 2).await;
    let mut many = Vec::new();
    for _ in 0..5 {
        let mut socket = TcpStream::connect(addr(&server)).await.unwrap();
        assert_eq!(health_on(&mut socket).await, Some(200));
        many.push(socket);
    }
    assert_eq!(server.state.http.refusals_for_address(), 0);
}

#[tokio::test]
async fn a_stop_lets_the_requests_in_their_handlers_finish_before_the_rooms_close() {
    let mut server = TestServer::start_with(|s| s.shutdown_timeout = 6.0).await;
    let world = World::new(&server).await.unwrap();
    let (stop, serving) = serve_again(&mut server).await;
    let doc = world.document(&server, None, None, None).await;
    let mut live = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(edit(&mut live, 7).await, 1);
    // A request in its handler when the stop comes (parked after its commit).
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
    assert!(eventually(|| server.state.http.in_handlers() == 1).await);

    let started = Instant::now();
    stop.send(()).unwrap();
    // Nobody new is taken ...
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(TcpStream::connect(addr(&server)).await.is_err());
    // ... but while that request finishes, the room is still open and takes changes.
    assert_eq!(edit(&mut live, 8).await, 2);
    assert!(!server.state.hub.is_closing());
    gate.open();
    assert_eq!(renaming.await.unwrap().ok(), Some(StatusCode::OK));
    // Then the rooms close, snapshotted, and the stop is in order, long before its bound.
    assert_eq!(live.closed().await.0, 1001);
    let stopped = tokio::time::timeout(WAIT, serving)
        .await
        .expect("serve ends")
        .unwrap();
    assert_eq!(stopped, Stopped::InOrder);
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(snapshot_seq(&server, &doc, 2).await, 2);
}
