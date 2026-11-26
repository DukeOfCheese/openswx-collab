//! Blob storage in a folder and the blob routes: the Python `test_storage.py`, the v2 streaming
//! promises, and the hostile cases. What every storage promises is one suite
//! (`common::blobs_contract`), run here against a folder and in `storage_s3.rs` against S3; the
//! route tests run against whichever storage the tests are on (`OSWX_TEST_STORAGE`).

mod common;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::{
    TestServer, TestStorage, World, blobs_contract, body_of, noise, nothing_stored, sha_of, world,
};
use futures_util::stream::{self, StreamExt};
use oswx_server::blobs::{Blobs, ByteStream, LocalBlobs, PutOutcome, blob_key};
use oswx_server::{AppState, Settings};
use reqwest::StatusCode;
use reqwest::header;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MIB: usize = 1 << 20;

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

/// Waits (up to ten seconds) for nothing to be in the server's storage.
async fn nothing_left(server: &TestServer) -> Vec<String> {
    let mut left = server.stored_under("").await;
    for _ in 0..200 {
        if left.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        left = server.stored_under("").await;
    }
    left
}

// ------------------------------------------------------------------ a folder

#[tokio::test]
async fn a_folder_keeps_the_storage_contract() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobs::new(dir.path().join("files")).unwrap();
    let storage = TestStorage::Local(store.root().to_path_buf());
    blobs_contract(&store, &storage, 20 * MIB + 3).await;
    assert!(!dir.path().join("outside").exists());
    assert_eq!(nothing_stored(&storage, "").await, Vec::<String>::new());
}

#[tokio::test]
async fn a_blob_is_one_file_that_another_process_reads_with_no_other_record() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobs::new(dir.path()).unwrap();
    let data = b"0123456789".repeat(100_000);
    let key = blob_key("org1", &sha_of(&data));
    let chunks: Vec<Bytes> = data.chunks(7_777).map(Bytes::copy_from_slice).collect();
    store
        .put_stream(&key, body_of(chunks), data.len() as u64)
        .await
        .unwrap();
    let on_disk = files_under(dir.path());
    assert_eq!(on_disk.len(), 1, "{on_disk:?}");
    let stored = std::fs::read(&on_disk[0]).unwrap();
    assert_eq!(&stored[..8], b"oswxblb1");
    assert_eq!(&stored[8..16], &(data.len() as u64).to_le_bytes());

    // Another storage over the same folder (a restart) reads it.
    let again = LocalBlobs::new(dir.path()).unwrap();
    assert!(common::read_back(&again, &key).await.unwrap() == data);
}

#[tokio::test]
async fn health_needs_a_folder_that_is_there_and_can_be_written() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("files");
    let store = LocalBlobs::new(&root).unwrap();
    store.health().await.unwrap();
    assert_eq!(
        files_under(&root),
        Vec::<PathBuf>::new(),
        "the probe is removed"
    );

    // The folder is gone (a volume that was unmounted): not healthy, and not made again.
    std::fs::remove_dir_all(&root).unwrap();
    assert!(store.health().await.is_err());
    assert!(!root.exists(), "health made the folder again");

    std::fs::write(&root, b"a file where the folder was").unwrap();
    assert!(store.health().await.is_err());
}

#[tokio::test]
async fn a_folder_that_is_gone_is_an_outage_not_an_empty_storage() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("files");
    let store = LocalBlobs::new(&root).unwrap();
    let key = blob_key("org1", &"ab".repeat(32));
    let snapshot = "docs/d1/snapshots/0000000001-0000000007.mpk.zst";
    store.put(snapshot, Bytes::from_static(b"x")).await.unwrap();
    assert!(!store.exists(&key).await.unwrap());

    std::fs::remove_dir_all(&root).unwrap();
    assert!(
        store.exists(&key).await.is_err(),
        "a storage that is gone made a blob look missing"
    );
    assert!(
        store.get(snapshot).await.is_err(),
        "a storage that is gone made a snapshot look absent"
    );
    assert!(store.get_stream(&key).await.is_err());
    // Nor is anything written to a folder made again where the storage was.
    assert!(store.put(snapshot, Bytes::from_static(b"x")).await.is_err());
    let body = body_of(vec![Bytes::from_static(b"x")]);
    assert!(store.put_stream(&key, body, 10).await.is_err());
    assert!(!root.exists());
    assert!(
        oswx_server::blobs::missing_blobs(&store, "org1", ["ab".repeat(32)])
            .await
            .is_err()
    );

    // The folder is back (empty): absent is absent again.
    std::fs::create_dir_all(&root).unwrap();
    assert!(!store.exists(&key).await.unwrap());
    assert_eq!(store.get(snapshot).await.unwrap(), None);
}

#[tokio::test]
async fn abandoned_temporary_files_are_swept_but_never_one_in_use() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalBlobs::new(dir.path()).unwrap());
    let key = blob_key("org1", &"ab".repeat(32));
    store.put(&key, Bytes::from_static(b"kept")).await.unwrap();
    // What a process that was killed mid-upload left: one at the root, one beside a blob.
    let folder = dir
        .path()
        .join("orgs")
        .join("org1")
        .join("blobs")
        .join("ab");
    let abandoned = [dir.path().join(".tmp-0123"), folder.join(".tmp-4567")];
    for path in &abandoned {
        std::fs::write(path, b"half an upload").unwrap();
    }

    // An upload under way holds a temporary file of its own.
    let (feed, body) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(1);
    let body: ByteStream = stream::unfold(body, |mut body| async move {
        body.recv().await.map(|item| (item, body))
    })
    .boxed();
    let uploading = {
        let (store, key) = (store.clone(), blob_key("org1", &"cd".repeat(32)));
        tokio::spawn(async move { store.put_stream(&key, body, u64::MAX).await })
    };
    feed.send(Ok(Bytes::from_static(b"first half")))
        .await
        .unwrap();
    assert!(eventually(|| files_under(dir.path()).len() == 4).await);

    // Too young: nothing is removed.
    assert_eq!(
        store
            .sweep_temp_files(Duration::from_secs(3600))
            .await
            .unwrap(),
        0
    );
    assert_eq!(files_under(dir.path()).len(), 4);

    // Old enough (any age): the abandoned ones go; the one in use and the blob stay.
    assert_eq!(store.sweep_temp_files(Duration::ZERO).await.unwrap(), 2);
    assert!(abandoned.iter().all(|p| !p.exists()));
    assert_eq!(files_under(dir.path()).len(), 2);

    // The upload is unharmed.
    feed.send(Ok(Bytes::from_static(b", second half")))
        .await
        .unwrap();
    drop(feed);
    assert_eq!(uploading.await.unwrap().unwrap().raw_len, 23);
    assert_eq!(
        files_under(dir.path()).len(),
        2,
        "two blobs, no temporary file"
    );
    assert_eq!(store.sweep_temp_files(Duration::ZERO).await.unwrap(), 0);
    assert_eq!(
        store.get(&key).await.unwrap().as_deref(),
        Some(&b"kept"[..])
    );
}

#[tokio::test]
async fn the_sweeper_sweeps_at_once_and_ends_with_the_storage() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(".tmp-left"), b"x").unwrap();
    let store = Arc::new(LocalBlobs::new(dir.path()).unwrap());
    let sweeper = store.start_sweeper(Duration::from_millis(20), Duration::ZERO);
    assert!(eventually(|| files_under(dir.path()).is_empty()).await);
    // And again later.
    std::fs::write(dir.path().join(".tmp-later"), b"x").unwrap();
    assert!(eventually(|| files_under(dir.path()).is_empty()).await);
    drop(store);
    tokio::time::timeout(Duration::from_secs(5), sweeper)
        .await
        .expect("the sweeper ends once the storage is dropped")
        .unwrap();
}

#[tokio::test]
async fn a_running_server_sweeps_its_storage_folder_when_it_starts() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_string_lossy().replace('\\', "/");
    let files = dir.path().join("files");
    std::fs::create_dir_all(&files).unwrap();
    let fresh = files.join(".tmp-fresh");
    std::fs::write(&fresh, b"an upload of a moment ago").unwrap();
    let settings = Settings {
        database_url: format!("sqlite://{base}/collab.db"),
        storage_url: format!("{base}/files"),
        ..Settings::default()
    };
    let state = AppState::from_settings(settings).await.unwrap();
    state.blobs.health().await.unwrap();
    // (A day must pass before a temporary file is taken for abandoned.)
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(fresh.exists(), "a fresh temporary file was swept");
}

#[tokio::test]
async fn settings_choose_a_folder_or_a_bucket() {
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

    // A bucket: it must take a write, a read and a removal before the server starts.
    let fake = common::FakeS3::start(dir.path()).await;
    let s3 = Settings {
        storage_url: format!("s3://{}/some/prefix", common::FAKE_BUCKET),
        s3_endpoint: fake.endpoint.clone(),
        s3_access_key_id: common::FAKE_ACCESS_KEY.into(),
        s3_secret_access_key: common::FAKE_SECRET_KEY.into(),
        ..settings.clone()
    };
    let state = AppState::from_settings(s3.clone()).await.unwrap();
    state.blobs.health().await.unwrap();
    state
        .blobs
        .put("docs/d/x", Bytes::from_static(b"in the bucket"))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(fake.object_file("some/prefix/docs/d/x")).unwrap(),
        b"in the bucket"
    );
    assert!(
        fake.count(|r| r.method == "PUT") >= 2,
        "the probe was written"
    );
    drop(state);

    // The wrong secret, or a bucket that is not there: the server does not start, and the error
    // names neither key.
    for wrong in [
        Settings {
            s3_secret_access_key: "not/the/secret".into(),
            ..s3.clone()
        },
        Settings {
            storage_url: "s3://no-such-bucket".into(),
            ..s3.clone()
        },
    ] {
        let err = AppState::from_settings(wrong).await.err().expect("refused");
        let text = format!("{err:#}");
        assert!(text.contains("cannot be used"), "{text}");
        for secret in [
            common::FAKE_ACCESS_KEY,
            common::FAKE_SECRET_KEY,
            "not/the/secret",
        ] {
            assert!(!text.contains(secret), "{secret} in {text}");
        }
    }
}

/// Storage that counts the streamed writes it is asked for.
struct CountingBlobs {
    inner: Arc<dyn Blobs>,
    stream_puts: AtomicUsize,
}

#[async_trait]
impl Blobs for CountingBlobs {
    async fn put_stream(
        &self,
        key: &str,
        body: ByteStream,
        limit: u64,
    ) -> anyhow::Result<PutOutcome> {
        self.stream_puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put_stream(key, body, limit).await
    }

    async fn get_stream(&self, key: &str) -> anyhow::Result<Option<(u64, ByteStream)>> {
        self.inner.get_stream(key).await
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        self.inner.exists(key).await
    }

    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        self.inner.delete_prefix(prefix).await
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }

    async fn health(&self) -> anyhow::Result<()> {
        self.inner.health().await
    }
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
    let key = blob_key(&w.org, &sha);
    assert_eq!(server.stored_under("orgs/").await, [key.as_str()]);
    let stored = server.state.blobs.get(&key).await.unwrap().unwrap();
    assert!(stored.len() < 1000, "{} bytes stored", stored.len());

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
    assert_eq!(nothing_left(&server).await, Vec::<String>::new());
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
    assert_eq!(server.stored_under("").await, Vec::<String>::new());
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
    let mut counting = None;
    let server = TestServer::start_custom(
        |_| {},
        |s| {
            let blobs = Arc::new(CountingBlobs {
                inner: s.blobs.clone(),
                stream_puts: AtomicUsize::new(0),
            });
            counting = Some(blobs.clone());
            s.blobs = blobs;
        },
    )
    .await;
    let counting = counting.unwrap();
    let w = World::new(&server).await.unwrap();
    let body = noise(200_000, 3);
    let sha = sha_of(&body);
    assert_eq!(
        upload(&server, &w, &w.admin, &sha, body.clone()).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(counting.stream_puts.load(Ordering::SeqCst), 1);
    let key = blob_key(&w.org, &sha);
    let before = server.state.blobs.get(&key).await.unwrap().unwrap();
    assert_eq!(
        upload(&server, &w, &w.admin, &sha, body.clone()).await,
        StatusCode::NO_CONTENT
    );
    // A wrong body for a stored blob is still refused (and the stored one untouched).
    assert_eq!(
        upload(&server, &w, &w.admin, &sha, b"wrong".to_vec()).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        counting.stream_puts.load(Ordering::SeqCst),
        1,
        "a stored blob was written again"
    );
    assert!(server.state.blobs.get(&key).await.unwrap().unwrap() == before);
    assert_eq!(server.stored_under("").await, [key]);
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
    assert_eq!(nothing_left(&server).await, Vec::<String>::new());
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
    // (On a folder the test keeps the storage as what it is, to ask it what it held.)
    let mut local = None;
    let server = TestServer::start_custom(
        |_| {},
        |s| {
            if !common::on_s3() {
                let blobs = Arc::new(LocalBlobs::new(&s.settings.storage_url).unwrap());
                local = Some(blobs.clone());
                s.blobs = blobs;
            }
        },
    )
    .await;
    let w = World::new(&server).await.unwrap();
    // (Every megabyte different: zstd would fold repeats of one block into nothing, and nothing
    // would be held however the storage was written.)
    let block = |n: usize| Bytes::from(noise(MIB, 11 + 2 * n as u64));
    let mut hasher = Sha256::new();
    for n in 0..64 {
        hasher.update(block(n));
    }
    let sha = hex::encode(hasher.finalize());

    let r = reqwest::Client::new()
        .put(blob_url(&server, &w, &sha))
        .bearer_auth(&w.admin)
        .header(header::CONTENT_LENGTH, 64 * MIB)
        .body(reqwest::Body::wrap_stream(stream::iter(
            (0..64).map(move |n| Ok::<_, io::Error>(block(n))),
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    if let Some(local) = local {
        let largest = local.largest_chunk_held();
        assert!(
            largest > 0 && largest < 8 * MIB,
            "largest chunk held: {largest}"
        );
    }
    if let Some(s3) = server.s3_blobs() {
        // The first part (kept back for its header), the one being filled and three on their way.
        let held = s3.peak_buffered();
        assert!(
            held > 8 * MIB && held <= 5 * 8 * MIB + MIB,
            "most bytes of the upload held at once: {held}"
        );
    }

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

/// Starts an upload of 28 MiB by hand and sends the first 20, so that storage has begun to take
/// it (a temporary file in a folder; a part of a multipart upload in a bucket). Answers the
/// connection, which waits for the rest.
async fn start_an_upload(server: &TestServer, w: &World, sha: &str) -> tokio::net::TcpStream {
    let addr = server.base.trim_start_matches("http://").to_owned();
    let mut socket = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let request = format!(
        "PUT /api/projects/{}/blobs/{sha} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {}\r\n\
         Content-Length: {}\r\n\r\n",
        w.project,
        w.admin,
        28 * MIB
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    socket.write_all(&noise(20 * MIB, 5)).await.unwrap();
    if common::on_s3() && server.fake_s3().is_none() {
        // (A real bucket does not show its unfinished uploads.)
        tokio::time::sleep(Duration::from_secs(2)).await;
        return socket;
    }
    let mut stored = server.stored_under("").await;
    for _ in 0..200 {
        if !stored.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        stored = server.stored_under("").await;
    }
    assert!(!stored.is_empty(), "the upload started writing");
    socket
}

#[tokio::test]
async fn a_client_that_hangs_up_mid_upload_leaves_nothing() {
    let (server, w) = world().await;
    let sha = "ab".repeat(32);
    let socket = start_an_upload(&server, &w, &sha).await;
    drop(socket);
    assert_eq!(nothing_left(&server).await, Vec::<String>::new());
    assert_eq!(
        missing(&server, &w, &w.admin, json!([sha])).await,
        (StatusCode::OK, json!([sha]))
    );
}

#[tokio::test]
async fn an_upload_that_stalls_is_given_up_on_and_leaves_nothing() {
    let server = TestServer::start_with(|s| s.blob_idle_timeout = 0.4).await;
    let w = World::new(&server).await.unwrap();
    let sha = "ab".repeat(32);
    let mut socket = start_an_upload(&server, &w, &sha).await;
    // Not another byte is sent, and the connection stays open: the server answers anyway.
    let mut answer = vec![0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(10), socket.read(&mut answer))
        .await
        .expect("answered without waiting for the rest of the body")
        .unwrap();
    let answer = String::from_utf8_lossy(&answer[..n]);
    assert!(answer.starts_with("HTTP/1.1 400"), "{answer}");
    assert!(
        answer.contains(r#"{"detail":"the upload did not finish"}"#),
        "{answer}"
    );
    assert_eq!(nothing_left(&server).await, Vec::<String>::new());
    assert_eq!(
        missing(&server, &w, &w.admin, json!([sha])).await,
        (StatusCode::OK, json!([sha]))
    );
    drop(socket);

    // An upload that keeps moving is not cut short by so short a timeout.
    let body = noise(3 * MIB, 6);
    assert_eq!(
        upload(&server, &w, &w.admin, &sha_of(&body), body).await,
        StatusCode::NO_CONTENT
    );
}

/// Storage that counts the blob reads it has handed out and that are still held.
struct WatchedBlobs {
    inner: Arc<dyn Blobs>,
    reads_held: Arc<AtomicUsize>,
    reads: AtomicUsize,
}

struct Held(Arc<AtomicUsize>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Blobs for WatchedBlobs {
    async fn put_stream(
        &self,
        key: &str,
        body: ByteStream,
        limit: u64,
    ) -> anyhow::Result<PutOutcome> {
        self.inner.put_stream(key, body, limit).await
    }

    async fn get_stream(&self, key: &str) -> anyhow::Result<Option<(u64, ByteStream)>> {
        let Some((len, chunks)) = self.inner.get_stream(key).await? else {
            return Ok(None);
        };
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.reads_held.fetch_add(1, Ordering::SeqCst);
        let held = Held(self.reads_held.clone());
        let chunks = chunks
            .map(move |chunk| {
                let _ = &held;
                chunk
            })
            .boxed();
        Ok(Some((len, chunks)))
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        self.inner.exists(key).await
    }

    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        self.inner.delete_prefix(prefix).await
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }

    async fn health(&self) -> anyhow::Result<()> {
        self.inner.health().await
    }
}

#[tokio::test]
async fn a_download_whose_client_stops_reading_lets_go_of_storage() {
    let mut watched = None;
    let server = TestServer::start_custom(
        |s| s.blob_idle_timeout = 0.4,
        |s| {
            let blobs = Arc::new(WatchedBlobs {
                inner: s.blobs.clone(),
                reads_held: Arc::default(),
                reads: AtomicUsize::new(0),
            });
            watched = Some(blobs.clone());
            s.blobs = blobs;
        },
    )
    .await;
    let watched = watched.unwrap();
    let w = World::new(&server).await.unwrap();
    // Far more than the connection's buffers hold.
    let body = noise(48 * MIB, 17);
    let sha = sha_of(&body);
    server
        .state
        .blobs
        .put_stream(
            &blob_key(&w.org, &sha),
            common::chunked(&body, MIB),
            u64::MAX,
        )
        .await
        .unwrap();

    let addr = server.base.trim_start_matches("http://").to_owned();
    let mut socket = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let request = format!(
        "GET /api/projects/{}/blobs/{sha} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {}\r\n\r\n",
        w.project, w.admin
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut first = vec![0u8; 4096];
    let n = socket.read(&mut first).await.unwrap();
    assert!(String::from_utf8_lossy(&first[..n]).starts_with("HTTP/1.1 200"));
    assert_eq!(watched.reads.load(Ordering::SeqCst), 1);
    // The client reads no more, and keeps the connection open: the read of storage is let go.
    assert!(
        eventually(|| watched.reads_held.load(Ordering::SeqCst) == 0).await,
        "the stalled download still holds its read of storage"
    );
    drop(socket);

    // A client that keeps reading gets all of it under the same timeout.
    let back = reqwest::Client::new()
        .get(blob_url(&server, &w, &sha))
        .bearer_auth(&w.admin)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(back == body, "the blob came back changed");
    assert!(eventually(|| watched.reads_held.load(Ordering::SeqCst) == 0).await);
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
    assert_eq!(nothing_but(&server, &blob_key(&w.org, &sha)).await, None);
}

/// Waits (up to ten seconds) for `key` to be all there is in the server's storage; answers what
/// is there instead if it does not come to that.
async fn nothing_but(server: &TestServer, key: &str) -> Option<Vec<String>> {
    let mut stored = server.stored_under("").await;
    for _ in 0..200 {
        if stored == [key] {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        stored = server.stored_under("").await;
    }
    Some(stored)
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

/// The switch itself: `OSWX_TEST_STORAGE=s3` really puts a test server's files in S3 (else a run
/// "on S3" could pass while testing folders).
#[tokio::test]
async fn the_test_servers_keep_their_files_where_the_environment_says() {
    let (server, w) = world().await;
    assert_eq!(server.s3_blobs().is_some(), common::on_s3());
    let body = b"where does this go?".to_vec();
    let sha = sha_of(&body);
    assert_eq!(
        upload(&server, &w, &w.admin, &sha, body).await,
        StatusCode::NO_CONTENT
    );
    let key = blob_key(&w.org, &sha);
    let in_the_folder = key
        .split('/')
        .fold(PathBuf::from(&server.state.settings.storage_url), |p, c| {
            p.join(c)
        })
        .is_file();
    assert_eq!(in_the_folder, !common::on_s3());
    if let Some(fake) = server.fake_s3() {
        assert!(fake.object_file(&key).is_file());
        assert!(fake.count(|r| r.method == "PUT" && r.path.ends_with(&sha)) == 1);
    }
}
