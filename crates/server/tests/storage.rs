//! Blob storage and the blob routes: the Python `test_storage.py` (its S3 cases come with S3 in
//! Task 17, and the document half of `test_the_whole_server_on_s3` with documents in Task 7), the
//! v2 streaming promises, and the hostile cases.

mod common;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{TestServer, World, world};
use futures_util::stream::{self, StreamExt};
use oswx_server::blobs::{Blobs, ByteStream, LocalBlobs, PutError, blob_key};
use oswx_server::{AppState, Settings};
use reqwest::StatusCode;
use reqwest::header;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MIB: usize = 1 << 20;

fn sha_of(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// `n` bytes that do not compress to nothing.
fn noise(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(n + 8);
    while out.len() < n {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(n);
    out
}

fn body_of(chunks: Vec<Bytes>) -> ByteStream {
    stream::iter(chunks.into_iter().map(Ok::<_, io::Error>)).boxed()
}

/// Every file under `dir`, recursively (empty when it does not exist).
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut todo = vec![dir.to_path_buf()];
    while let Some(d) = todo.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                todo.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

fn storage_dir(server: &TestServer) -> PathBuf {
    PathBuf::from(&server.state.settings.storage_url)
}

fn blob_path(server: &TestServer, org: &str, sha: &str) -> PathBuf {
    blob_key(org, sha)
        .split('/')
        .fold(storage_dir(server), |p, c| p.join(c))
}

fn blob_url(server: &TestServer, w: &World, sha: &str) -> String {
    server.url(&format!("/api/projects/{}/blobs/{sha}", w.project))
}

async fn upload(
    server: &TestServer,
    w: &World,
    token: &str,
    sha: &str,
    body: Vec<u8>,
) -> StatusCode {
    reqwest::Client::new()
        .put(blob_url(server, w, sha))
        .bearer_auth(token)
        .body(body)
        .send()
        .await
        .expect("an answer")
        .status()
}

async fn missing(server: &TestServer, w: &World, token: &str, shas: Value) -> (StatusCode, Value) {
    server
        .post(
            &format!("/api/projects/{}/blobs/missing", w.project),
            Some(token),
            shas,
        )
        .await
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

// ------------------------------------------------------------------ the storage itself

#[tokio::test]
async fn local_folder() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobs::new(dir.path().join("files")).unwrap();
    let key = blob_key("org1", &"ab".repeat(32));
    assert_eq!(store.get(&key).await.unwrap(), None);
    assert!(!store.exists(&key).await.unwrap());
    store.put(&key, Bytes::from_static(b"hello")).await.unwrap();
    store
        .put(&key, Bytes::from_static(b"hello again"))
        .await
        .unwrap(); // (overwrite)
    assert!(store.exists(&key).await.unwrap());
    assert_eq!(
        store.get(&key).await.unwrap().as_deref(),
        Some(&b"hello again"[..])
    );
    let left = files_under(&dir.path().join("files"));
    assert_eq!(left.len(), 1, "no half-written files left behind: {left:?}");

    for bad in [
        "../outside",
        "/abs",
        "a/../../b",
        "sp ace",
        "",
        "a\\b",
        "C:/x",
    ] {
        assert!(
            store.put(bad, Bytes::from_static(b"x")).await.is_err(),
            "{bad:?}"
        );
        assert!(store.get(bad).await.is_err(), "{bad:?}");
        assert!(store.exists(bad).await.is_err(), "{bad:?}");
        assert!(store.get_stream(bad).await.is_err(), "{bad:?}");
        assert!(
            store
                .put_stream(bad, body_of(vec![Bytes::from_static(b"x")]), 10)
                .await
                .is_err(),
            "{bad:?}"
        );
        assert!(
            store.delete_prefix(&format!("{bad}/")).await.is_err(),
            "{bad:?}"
        );
    }
    assert!(!dir.path().join("outside").exists());
}

#[tokio::test]
async fn removing_everything_under_a_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobs::new(dir.path()).unwrap();
    let keys = [
        "orgs/a/blobs/1",
        "orgs/a/blobs/sub/2",
        "orgs/ab/blobs/3",
        "orgs/b/x",
    ];
    for key in keys {
        store.put(key, Bytes::from_static(b"x")).await.unwrap();
    }
    store.delete_prefix("orgs/a/").await.unwrap();
    store.delete_prefix("orgs/nothing-here/").await.unwrap(); // (nothing to remove is fine)
    let mut left = Vec::new();
    for key in keys {
        left.push(store.exists(key).await.unwrap());
    }
    assert_eq!(left, [false, false, true, true]);
    // Only a folder-like prefix, and never the root.
    assert!(store.delete_prefix("orgs/b").await.is_err());
    assert!(store.delete_prefix("/").await.is_err());
    assert!(store.delete_prefix("").await.is_err());
    assert!(store.exists("orgs/b/x").await.unwrap());
}

#[tokio::test]
async fn a_streamed_blob_is_compressed_self_describing_and_comes_back_raw() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobs::new(dir.path()).unwrap();
    let data = b"0123456789".repeat(100_000); // a million bytes that compress well
    let key = blob_key("org1", &sha_of(&data));
    let chunks: Vec<Bytes> = data.chunks(7_777).map(Bytes::copy_from_slice).collect();
    let outcome = store
        .put_stream(&key, body_of(chunks), data.len() as u64)
        .await
        .unwrap();
    assert_eq!(outcome.raw_len, data.len() as u64);
    assert_eq!(outcome.sha256, sha_of(&data));
    assert!(store.exists(&key).await.unwrap());

    let on_disk = files_under(dir.path());
    assert_eq!(on_disk.len(), 1, "{on_disk:?}");
    let stored = std::fs::read(&on_disk[0]).unwrap();
    assert!(
        stored.len() < data.len() / 10,
        "stored {} bytes",
        stored.len()
    );
    assert_eq!(&stored[..8], b"oswxblb1");
    assert_eq!(&stored[8..16], &(data.len() as u64).to_le_bytes());

    // Another storage over the same folder (a restart) reads it with no other record.
    let again = LocalBlobs::new(dir.path()).unwrap();
    let (len, mut chunks) = again.get_stream(&key).await.unwrap().unwrap();
    assert_eq!(len, data.len() as u64);
    let mut back = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.unwrap();
        assert!(chunk.len() <= 64 << 10);
        back.extend_from_slice(&chunk);
    }
    assert_eq!(back, data);
    assert!(
        again
            .get_stream("orgs/org1/blobs/00/none")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_damaged_blob_file_fails_rather_than_answering_short() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobs::new(dir.path()).unwrap();
    let data = noise(300_000, 7);
    let key = blob_key("org1", &sha_of(&data));
    store
        .put_stream(&key, body_of(vec![Bytes::from(data.clone())]), u64::MAX)
        .await
        .unwrap();
    let path = files_under(dir.path()).pop().unwrap();
    let mut stored = std::fs::read(&path).unwrap();
    stored[8..16].copy_from_slice(&(data.len() as u64 + 5).to_le_bytes());
    std::fs::write(&path, &stored).unwrap();
    let (_, chunks) = store.get_stream(&key).await.unwrap().unwrap();
    let results: Vec<_> = chunks.collect().await;
    assert!(
        results.last().unwrap().is_err(),
        "a short blob ends in an error"
    );

    std::fs::write(&path, b"not a blob").unwrap();
    assert!(store.get_stream(&key).await.is_err());
}

#[tokio::test]
async fn a_failed_streamed_write_leaves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobs::new(dir.path()).unwrap();
    let key = blob_key("org1", &"cd".repeat(32));

    let err = store
        .put_stream(&key, body_of(vec![Bytes::from(vec![1u8; 600])]), 500)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<PutError>(),
        Some(PutError::TooLarge)
    ));

    let broken: ByteStream = stream::iter(vec![
        Ok(Bytes::from_static(b"some")),
        Err(io::Error::other("the client went away")),
    ])
    .boxed();
    let err = store.put_stream(&key, broken, 500).await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<PutError>(),
        Some(PutError::Body(_))
    ));

    assert!(!store.exists(&key).await.unwrap());
    assert_eq!(files_under(dir.path()), Vec::<PathBuf>::new());
}

#[tokio::test]
async fn health_needs_a_writable_folder() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("files");
    let store = LocalBlobs::new(&root).unwrap();
    store.health().await.unwrap();
    assert_eq!(
        files_under(&root),
        Vec::<PathBuf>::new(),
        "the probe is removed"
    );
    std::fs::remove_dir_all(&root).unwrap();
    std::fs::write(&root, b"a file where the folder was").unwrap();
    assert!(store.health().await.is_err());
}

#[tokio::test]
async fn a_folder_is_local_storage_and_s3_waits_for_task_17() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_string_lossy().replace('\\', "/");
    let settings = Settings {
        database_url: format!("sqlite://{base}/collab.db"),
        storage_url: format!("{base}/files"),
        ..Settings::default()
    };
    let state = AppState::from_settings(settings.clone()).await.unwrap();
    state.blobs.health().await.unwrap();
    assert!(dir.path().join("files").is_dir());
    drop(state);

    let s3 = Settings {
        storage_url: "s3://collab/prod".into(),
        ..settings
    };
    let err = AppState::from_settings(s3).await.err().expect("refused");
    assert!(err.to_string().contains("Task 17"), "{err}");
}

// ------------------------------------------------------------------ the routes

#[tokio::test]
async fn upload_check_and_download_a_blob_across_a_restart() {
    let (server, w) = world().await;
    let body = b"C".repeat(30_000);
    let sha = sha_of(&body);
    assert_eq!(
        missing(&server, &w, &w.admin, json!([sha])).await,
        (StatusCode::OK, json!([sha]))
    );
    assert_eq!(
        upload(&server, &w, &w.admin, &sha, body.clone()).await,
        StatusCode::NO_CONTENT
    );
    let other = "0".repeat(64);
    assert_eq!(
        missing(&server, &w, &w.admin, json!([other, sha, other])).await,
        (StatusCode::OK, json!([other, other]))
    );
    assert_eq!(
        missing(&server, &w, &w.admin, json!([])).await,
        (StatusCode::OK, json!([]))
    );

    let r = reqwest::Client::new()
        .get(blob_url(&server, &w, &sha))
        .bearer_auth(&w.admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let h = r.headers();
    assert_eq!(h[header::CONTENT_TYPE], "application/octet-stream");
    assert_eq!(h[header::CONTENT_LENGTH], body.len().to_string().as_str());
    assert_eq!(
        h[header::CACHE_CONTROL],
        "private, max-age=31536000, immutable"
    );
    assert_eq!(h[header::ETAG], format!("\"{sha}\"").as_str());
    assert_eq!(r.bytes().await.unwrap(), body);

    // Stored compressed, under the organisation's key.
    let stored = std::fs::read(blob_path(&server, &w.org, &sha)).unwrap();
    assert!(stored.len() < 1000, "{} bytes on disk", stored.len());

    // Every project of the organisation reaches it.
    let (status, p2) = server
        .post(
            &format!("/api/orgs/{}/projects", w.org),
            Some(&w.admin),
            json!({"name": "Arm"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, list) = server
        .post(
            &format!("/api/projects/{}/blobs/missing", p2["id"].as_str().unwrap()),
            Some(&w.admin),
            json!([sha]),
        )
        .await;
    assert_eq!((status, list), (StatusCode::OK, json!([])));

    let server = server.restart().await;
    let r = reqwest::Client::new()
        .get(blob_url(&server, &w, &sha))
        .bearer_auth(&w.admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.bytes().await.unwrap(), body);
}

#[tokio::test]
async fn content_that_does_not_match_its_hash_is_refused_and_not_stored() {
    let (server, w) = world().await;
    let sha = sha_of(b"what I said I would send");
    let r = reqwest::Client::new()
        .put(blob_url(&server, &w, &sha))
        .bearer_auth(&w.admin)
        .body(b"something else".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        r.json::<Value>().await.unwrap(),
        json!({"detail": "the content does not match the sha256 in the address"})
    );
    assert_eq!(files_under(&storage_dir(&server)), Vec::<PathBuf>::new());
    assert_eq!(
        missing(&server, &w, &w.admin, json!([sha])).await,
        (StatusCode::OK, json!([sha]))
    );
}

#[tokio::test]
async fn a_blob_name_must_be_a_lower_case_sha256() {
    let (server, w) = world().await;
    let body = b"hello".to_vec();
    let sha = sha_of(&body);
    for bad in [
        sha.to_uppercase(),
        sha[..63].to_owned(),
        format!("{sha}0"),
        format!("{}g", &sha[..63]),
        "a%2Fb".into(),
        "..%2F..%2Fx".into(),
    ] {
        assert_eq!(
            upload(&server, &w, &w.admin, &bad, body.clone()).await,
            StatusCode::BAD_REQUEST,
            "{bad}"
        );
        let (status, detail) = server
            .get(
                &format!("/api/projects/{}/blobs/{bad}", w.project),
                Some(&w.admin),
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{bad}");
        assert_eq!(detail, json!({"detail": "no such blob"}));
    }
    let (status, detail) = server
        .get(
            &format!("/api/projects/{}/blobs/{sha}", w.project),
            Some(&w.admin),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(detail, json!({"detail": "no such blob"}));
    assert_eq!(files_under(&storage_dir(&server)), Vec::<PathBuf>::new());
}

#[tokio::test]
async fn missing_takes_at_most_2000_well_formed_hashes() {
    let (server, w) = world().await;
    let many: Vec<String> = (0..2001).map(|n| format!("{n:064x}")).collect();
    let (status, body) = missing(&server, &w, &w.admin, json!(many)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, body) = missing(&server, &w, &w.admin, json!(many[..2000])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().unwrap().len(), 2000);

    let good = "a".repeat(64);
    for bad in [
        json!([good, "A".repeat(64)]),
        json!([good, "a".repeat(63)]),
        json!([good, "z".repeat(64)]),
        json!([good, ""]),
        json!([good, 7]),
        json!([good, null]),
        json!({"shas": [good]}),
        json!(good),
    ] {
        let (status, body) = missing(&server, &w, &w.admin, bad.clone()).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{bad} -> {body}");
        assert!(body["detail"].is_string());
    }
}

#[tokio::test]
async fn a_viewer_may_read_but_not_write_and_an_outsider_sees_nothing() {
    let (server, w) = world().await;
    let body = b"geometry".to_vec();
    let sha = sha_of(&body);
    assert_eq!(
        upload(&server, &w, &w.admin, &sha, body.clone()).await,
        StatusCode::NO_CONTENT
    );
    let (viewer, _) = w
        .invite(&server, "vic@example.com", None, Some("viewer"), "member")
        .await;
    let (editor, _) = w.editor(&server, "eve@example.com").await;
    let (outsider, _) = w
        .invite(&server, "olga@example.com", None, None, "member")
        .await;

    assert_eq!(
        missing(&server, &w, &viewer, json!([sha])).await,
        (StatusCode::OK, json!([]))
    );
    let r = reqwest::Client::new()
        .get(blob_url(&server, &w, &sha))
        .bearer_auth(&viewer)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.bytes().await.unwrap(), body);
    let other = b"other".to_vec();
    let r = reqwest::Client::new()
        .put(blob_url(&server, &w, &sha_of(&other)))
        .bearer_auth(&viewer)
        .body(other.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        r.json::<Value>().await.unwrap(),
        json!({"detail": "needs the project role editor"})
    );
    assert_eq!(
        upload(&server, &w, &editor, &sha_of(&other), other.clone()).await,
        StatusCode::NO_CONTENT
    );

    let no_such = json!({"detail": "no such project"});
    assert_eq!(
        missing(&server, &w, &outsider, json!([sha])).await,
        (StatusCode::NOT_FOUND, no_such.clone())
    );
    assert_eq!(
        server
            .get(
                &format!("/api/projects/{}/blobs/{sha}", w.project),
                Some(&outsider)
            )
            .await,
        (StatusCode::NOT_FOUND, no_such.clone())
    );
    assert_eq!(
        upload(&server, &w, &outsider, &sha, body.clone()).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        server
            .get(&format!("/api/projects/{}/blobs/{sha}", w.project), None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        upload(&server, &w, "nonsense", &sha, body).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn a_blob_already_stored_is_not_written_again() {
    let (server, w) = world().await;
    let body = noise(200_000, 3);
    let sha = sha_of(&body);
    assert_eq!(
        upload(&server, &w, &w.admin, &sha, body.clone()).await,
        StatusCode::NO_CONTENT
    );
    let path = blob_path(&server, &w.org, &sha);
    let before = std::fs::metadata(&path).unwrap().modified().unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        upload(&server, &w, &w.admin, &sha, body.clone()).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().modified().unwrap(),
        before
    );
    // A wrong body for a stored blob is still refused (and the stored one untouched).
    assert_eq!(
        upload(&server, &w, &w.admin, &sha, b"wrong".to_vec()).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().modified().unwrap(),
        before
    );
    assert_eq!(files_under(&storage_dir(&server)), vec![path]);
}

#[tokio::test]
async fn a_blob_one_byte_over_256_mb_is_refused_and_leaves_nothing() {
    let (server, w) = world().await;
    assert_eq!(server.state.settings.max_blob_bytes, 256 * MIB);
    let block = Bytes::from(vec![b'x'; MIB]);
    let chunks: Vec<Bytes> = std::iter::repeat_n(block, 256)
        .chain([Bytes::from_static(b"!")])
        .collect();
    // Streamed without a Content-Length: refused once the size passes the limit.
    let r = reqwest::Client::new()
        .put(blob_url(&server, &w, &"ab".repeat(32)))
        .bearer_auth(&w.admin)
        .body(reqwest::Body::wrap_stream(stream::iter(
            chunks.into_iter().map(Ok::<_, io::Error>),
        )))
        .send()
        .await
        .expect("the client has sent all but the last byte, and reads the answer");
    assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        r.json::<Value>().await.unwrap(),
        json!({"detail": "too large"})
    );
    let dir = storage_dir(&server);
    assert!(
        eventually(|| files_under(&dir).is_empty()).await,
        "left behind: {:?}",
        files_under(&dir)
    );
}

#[tokio::test]
async fn a_declared_length_over_the_limit_is_refused_before_the_body_is_read() {
    let (server, w) = world().await;
    let addr = server.base.trim_start_matches("http://").to_owned();
    let mut socket = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let request = format!(
        "PUT /api/projects/{}/blobs/{} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {}\r\n\
         Content-Length: {}\r\n\r\n",
        w.project,
        "ab".repeat(32),
        w.admin,
        256 * MIB + 1
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    // Not one byte of the body is sent: the answer must come anyway.
    let mut answer = vec![0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut answer))
        .await
        .expect("answered without waiting for the body")
        .unwrap();
    let answer = String::from_utf8_lossy(&answer[..n]);
    assert!(answer.starts_with("HTTP/1.1 413"), "{answer}");
    assert!(answer.contains(r#"{"detail":"too large"}"#), "{answer}");
}

#[tokio::test]
async fn a_64_mb_upload_is_streamed_not_buffered() {
    let mut local = None;
    let server = TestServer::start_custom(
        |_| {},
        |s| {
            let blobs = Arc::new(LocalBlobs::new(&s.settings.storage_url).unwrap());
            local = Some(blobs.clone());
            s.blobs = blobs;
        },
    )
    .await;
    let local = local.unwrap();
    let w = World::new(&server).await.unwrap();
    let block = Bytes::from(noise(MIB, 11));
    let mut hasher = Sha256::new();
    for _ in 0..64 {
        hasher.update(&block);
    }
    let sha = hex::encode(hasher.finalize());

    let r = reqwest::Client::new()
        .put(blob_url(&server, &w, &sha))
        .bearer_auth(&w.admin)
        .header(header::CONTENT_LENGTH, 64 * MIB)
        .body(reqwest::Body::wrap_stream(stream::iter(
            std::iter::repeat_n(block.clone(), 64).map(Ok::<_, io::Error>),
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    let largest = local.largest_chunk_held();
    assert!(
        largest > 0 && largest < 8 * MIB,
        "largest chunk held: {largest}"
    );

    // And it comes back whole, streamed.
    let r = reqwest::Client::new()
        .get(blob_url(&server, &w, &sha))
        .bearer_auth(&w.admin)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.headers()[header::CONTENT_LENGTH],
        (64 * MIB).to_string().as_str()
    );
    let mut hasher = Sha256::new();
    let mut total = 0;
    let mut chunks = r.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.unwrap();
        total += chunk.len();
        hasher.update(&chunk);
    }
    assert_eq!(total, 64 * MIB);
    assert_eq!(hex::encode(hasher.finalize()), sha);
}

#[tokio::test]
async fn a_client_that_hangs_up_mid_upload_leaves_no_file() {
    let (server, w) = world().await;
    let dir = storage_dir(&server);
    let addr = server.base.trim_start_matches("http://").to_owned();
    let mut socket = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let request = format!(
        "PUT /api/projects/{}/blobs/{} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {}\r\n\
         Content-Length: {}\r\n\r\n",
        w.project,
        "ab".repeat(32),
        w.admin,
        4 * MIB
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    socket.write_all(&noise(MIB, 5)).await.unwrap();
    assert!(
        eventually(|| !files_under(&dir).is_empty()).await,
        "the upload started writing"
    );
    drop(socket);
    assert!(
        eventually(|| files_under(&dir).is_empty()).await,
        "left behind: {:?}",
        files_under(&dir)
    );
    assert_eq!(
        missing(&server, &w, &w.admin, json!(["ab".repeat(32)])).await,
        (StatusCode::OK, json!(["ab".repeat(32)]))
    );
}

#[tokio::test]
async fn two_uploads_of_one_blob_at_once_both_succeed() {
    let (server, w) = world().await;
    let body = noise(8 * MIB, 9);
    let sha = sha_of(&body);
    let (a, b) = tokio::join!(
        upload(&server, &w, &w.admin, &sha, body.clone()),
        upload(&server, &w, &w.admin, &sha, body.clone()),
    );
    assert_eq!((a, b), (StatusCode::NO_CONTENT, StatusCode::NO_CONTENT));
    let back = reqwest::Client::new()
        .get(blob_url(&server, &w, &sha))
        .bearer_auth(&w.admin)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(back == body, "the stored blob is intact");
    assert_eq!(
        files_under(&storage_dir(&server)),
        vec![blob_path(&server, &w.org, &sha)]
    );
}

#[tokio::test]
async fn ranges_are_ignored_and_the_etag_answers_304() {
    let (server, w) = world().await;
    let body = noise(10_000, 13);
    let sha = sha_of(&body);
    assert_eq!(
        upload(&server, &w, &w.admin, &sha, body.clone()).await,
        StatusCode::NO_CONTENT
    );
    let http = reqwest::Client::new();
    let r = http
        .get(blob_url(&server, &w, &sha))
        .bearer_auth(&w.admin)
        .header(header::RANGE, "bytes=0-9")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers()[header::ACCEPT_RANGES], "none");
    assert_eq!(r.bytes().await.unwrap(), body);

    let r = http
        .get(blob_url(&server, &w, &sha))
        .bearer_auth(&w.admin)
        .header(header::IF_NONE_MATCH, format!("\"{sha}\""))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(r.headers()[header::ETAG], format!("\"{sha}\"").as_str());
    assert!(r.bytes().await.unwrap().is_empty());

    let r = http
        .get(blob_url(&server, &w, &sha))
        .bearer_auth(&w.admin)
        .header(header::IF_NONE_MATCH, "\"something-else\"")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.bytes().await.unwrap(), body);

    // A tag for a blob that is not there is still 404, and so is one without access.
    let absent = "e".repeat(64);
    let r = http
        .get(blob_url(&server, &w, &absent))
        .bearer_auth(&w.admin)
        .header(header::IF_NONE_MATCH, format!("\"{absent}\""))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    let (outsider, _) = w
        .invite(&server, "olga@example.com", None, None, "member")
        .await;
    let r = http
        .get(blob_url(&server, &w, &sha))
        .bearer_auth(&outsider)
        .header(header::IF_NONE_MATCH, format!("\"{sha}\""))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
}
