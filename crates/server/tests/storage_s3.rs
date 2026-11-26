//! Blob storage in S3: the storage contract (`common::blobs_contract`, the suite a folder keeps in
//! `storage.rs`) against an S3-compatible server inside this process ([`FakeS3`]) and, when
//! `OSWX_TEST_S3_URL` names one, against a real endpoint; then what only a remote store can do to
//! the server: be unreachable, refuse, fail, hang and stall, and lose its bucket.

mod common;

use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{
    FAKE_ACCESS_KEY, FAKE_SECRET_KEY, FakeS3, Fault, Live, Seen, TestServer, TestStorage, World,
    blobs_contract, body_of, chunked, noise, nothing_stored, read_back, real_s3, set, set_depth,
    sha_of, wrong_body,
};
use futures_util::stream::{self, StreamExt};
use oswx_core::wire::RejectCode;
use oswx_server::blobs::{
    Blobs, ByteStream, PutError, S3Blobs, S3Tuning, Stalled, blob_key, missing_blobs,
};
use oswx_server::snapshot::{LoadError, load_head};
use reqwest::StatusCode;
use serde_json::json;
use tempfile::TempDir;

const MIB: usize = 1 << 20;
const KIB: usize = 1 << 10;
/// Every request, for [`FakeS3::inject`].
const ALWAYS: usize = usize::MAX;

/// A fake S3 server and a storage on its bucket, under `prefix`, tuned by `tune` (from
/// `common::test_tuning`). Nothing has been asked of the bucket yet.
struct Bucket {
    fake: Arc<FakeS3>,
    blobs: Arc<S3Blobs>,
    storage: TestStorage,
    /// (Dropped last: the server's files are here.)
    _dir: TempDir,
}

async fn bucket_with(prefix: &str, tune: impl FnOnce(&mut S3Tuning)) -> Bucket {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeS3::start(dir.path()).await);
    let mut config = fake.config(prefix);
    tune(&mut config.tuning);
    let blobs = Arc::new(S3Blobs::new(config).unwrap());
    let storage = TestStorage::S3 {
        blobs: blobs.clone(),
        fake: Some(fake.clone()),
        config: Arc::new(fake.config(prefix)),
    };
    Bucket {
        fake,
        blobs,
        storage,
        _dir: dir,
    }
}

async fn bucket() -> Bucket {
    bucket_with("", |_| {}).await
}

/// The smallest parts S3 takes (5 MiB; the fake refuses smaller ones as S3 does), two at a time.
fn small_parts(tuning: &mut S3Tuning) {
    tuning.part_size = PART;
    tuning.parts_at_once = 2;
}

/// The part size of [`small_parts`].
const PART: usize = oswx_server::blobs::s3::MIN_PART;

/// Waits (up to ten seconds) for `done` to hold.
async fn eventually(mut done: impl FnMut() -> bool) -> bool {
    for _ in 0..200 {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    done()
}

fn text(err: &anyhow::Error) -> String {
    format!("{err:#}")
}

/// Fails if `shown` holds a credential of the fake.
fn no_secrets(shown: &str) {
    for secret in [FAKE_ACCESS_KEY, FAKE_SECRET_KEY] {
        assert!(!shown.contains(secret), "a credential in {shown}");
    }
}

// ------------------------------------------------------------------ the contract

#[tokio::test]
async fn a_bucket_keeps_the_storage_contract() {
    let b = bucket().await;
    b.blobs.verify().await.unwrap();
    blobs_contract(b.blobs.as_ref(), &b.storage, 20 * MIB + 3).await;
    assert_eq!(
        b.fake.parts_held(),
        0,
        "parts of unfinished uploads are left"
    );
    assert!(
        b.fake.count(Seen::completes_upload) > 0,
        "nothing was a multipart upload"
    );
    assert!(
        b.fake.count(Seen::aborts_upload) > 0,
        "no failed upload was aborted"
    );
}

#[tokio::test]
async fn a_bucket_keeps_the_storage_contract_under_a_prefix() {
    let b = bucket_with("some/prefix", |_| {}).await;
    // What is in the bucket outside the prefix is not this storage's, and is left alone.
    let outside = Arc::new(S3Blobs::new(b.fake.config("")).unwrap());
    let beside = Arc::new(S3Blobs::new(b.fake.config("some/prefixed")).unwrap());
    for other in [&outside, &beside] {
        other
            .put("orgs/1/blobs/x", Bytes::from_static(b"theirs"))
            .await
            .unwrap();
        other
            .put("docs/1/s", Bytes::from_static(b"theirs"))
            .await
            .unwrap();
    }
    blobs_contract(b.blobs.as_ref(), &b.storage, 9 * MIB).await;
    assert_eq!(
        beside.keys().await.unwrap(),
        ["docs/1/s", "orgs/1/blobs/x"],
        "a prefix reached a longer one"
    );
    assert_eq!(
        outside.keys().await.unwrap(),
        [
            "docs/1/s",
            "orgs/1/blobs/x",
            "some/prefixed/docs/1/s",
            "some/prefixed/orgs/1/blobs/x"
        ],
        "a storage under a prefix reached outside it"
    );
    assert!(b.fake.object_file("some/prefixed/docs/1/s").is_file());
}

/// The same suite against the endpoint `OSWX_TEST_S3_URL` names (Garage in CI). Skipped when it
/// is not set. There the suite sees unfinished multipart uploads through `ListMultipartUploads`.
#[tokio::test]
async fn a_real_s3_endpoint_keeps_the_storage_contract() {
    let Some(config) = real_s3() else {
        eprintln!("skipped: {} is not set", common::S3_ENV);
        return;
    };
    let blobs = Arc::new(
        S3Blobs::open(config.clone())
            .await
            .expect("the bucket can be used"),
    );
    let storage = TestStorage::S3 {
        blobs: blobs.clone(),
        fake: None,
        config: Arc::new(config),
    };
    assert_eq!(storage.stored_under("").await, Vec::<String>::new());
    blobs_contract(blobs.as_ref(), &storage, 20 * MIB + 3).await;

    // More than one page of a listing, removed in more than one batch.
    let keys: Vec<String> = (0..1005).map(|n| format!("orgs/1/blobs/{n:04}")).collect();
    stream::iter(&keys)
        .for_each_concurrent(32, |key| {
            let blobs = blobs.clone();
            async move { blobs.put(key, Bytes::from_static(b"x")).await.unwrap() }
        })
        .await;
    blobs
        .put("orgs/10/x", Bytes::from_static(b"x"))
        .await
        .unwrap();
    assert_eq!(blobs.keys().await.unwrap().len(), 1006);
    blobs.delete_prefix("orgs/1/").await.unwrap();
    assert_eq!(blobs.keys().await.unwrap(), ["orgs/10/x"]);
    blobs.delete_prefix("orgs/").await.unwrap();
    assert_eq!(blobs.keys().await.unwrap(), Vec::<String>::new());
}

// ------------------------------------------------------------------ outages

/// A blob and a snapshot in the bucket, for the tests that then break the store.
async fn stored(b: &Bucket) -> (String, &'static str) {
    let data = noise(300_000, 3);
    let key = blob_key("org1", &sha_of(&data));
    b.blobs
        .put_stream(&key, chunked(&data, 50_000), u64::MAX)
        .await
        .unwrap();
    let snapshot = "docs/d1/snapshots/0000000001-0000000007.mpk.zst";
    b.blobs
        .put(snapshot, Bytes::from_static(b"a snapshot"))
        .await
        .unwrap();
    (key, snapshot)
}

/// Every way of asking the storage fails, and none of the errors holds a credential.
async fn everything_is_an_error(blobs: &S3Blobs, key: &str, snapshot: &str) {
    let absent = blob_key("org1", &"0".repeat(64));
    for key in [key, absent.as_str()] {
        let err = blobs.exists(key).await.expect_err("exists answered");
        no_secrets(&text(&err));
        let err = blobs.get_stream(key).await.err().expect("a read started");
        no_secrets(&text(&err));
    }
    for key in [snapshot, "docs/d1/snapshots/none"] {
        let err = blobs.get(key).await.expect_err("a value was read");
        no_secrets(&text(&err));
    }
    let err = missing_blobs(blobs, "org1", ["0".repeat(64)])
        .await
        .expect_err("a blob was called missing");
    no_secrets(&text(&err));
    no_secrets(&text(&blobs.health().await.expect_err("healthy")));
    let put = blobs.put(snapshot, Bytes::from_static(b"x")).await;
    no_secrets(&text(&put.expect_err("a value was written")));
    let put = blobs
        .put_stream(&absent, body_of(vec![Bytes::from_static(b"x")]), 10)
        .await
        .expect_err("a blob was written");
    assert!(
        put.downcast_ref::<PutError>().is_none(),
        "the body is blamed"
    );
    no_secrets(&text(&put));
    no_secrets(&text(&blobs.delete(snapshot).await.expect_err("removed")));
    let removed = blobs.delete_prefix("orgs/org1/").await;
    no_secrets(&text(&removed.expect_err("a prefix was removed")));
}

#[tokio::test]
async fn an_unreachable_store_is_an_error_never_absent() {
    // (No retries: on Windows every refused connection takes two seconds to be refused.)
    let b = bucket_with("", |t| t.max_retries = 0).await;
    let (key, snapshot) = stored(&b).await;
    assert!(b.blobs.exists(&key).await.unwrap());
    b.fake.stop().await;
    let started = Instant::now();
    everything_is_an_error(&b.blobs, &key, snapshot).await;
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "an unreachable store took {:?} to fail",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_store_that_refuses_the_credentials_is_an_error_never_absent() {
    let b = bucket().await;
    let (key, snapshot) = stored(&b).await;
    b.fake.inject(ALWAYS, Fault::Status(403), |_| true);
    everything_is_an_error(&b.blobs, &key, snapshot).await;
    // (The fake's 403 repeats the key id, as AWS does: it is taken out of the error.)
    let err = text(&b.blobs.get(snapshot).await.unwrap_err());
    assert!(err.contains("403") && err.contains("<redacted>"), "{err}");
    b.fake.heal();

    // And the real thing: a secret that is not the bucket's.
    let mut wrong = b.fake.config("");
    wrong.secret_access_key = "not/the/secret".into();
    let wrong = S3Blobs::new(wrong).unwrap();
    everything_is_an_error(&wrong, &key, snapshot).await;
    assert!(wrong.verify().await.is_err());
    assert!(
        b.blobs.exists(&key).await.unwrap(),
        "the blob is still there"
    );
}

#[tokio::test]
async fn a_store_that_keeps_failing_is_an_error_never_absent() {
    let b = bucket().await;
    let (key, snapshot) = stored(&b).await;
    for status in [500, 503, 429] {
        b.fake.inject(ALWAYS, Fault::Status(status), |_| true);
        everything_is_an_error(&b.blobs, &key, snapshot).await;
        b.fake.heal();
    }
    // Each was tried again as often as the tuning says, and no more.
    let heads = b.fake.count(Seen::is_head);
    b.fake.inject(ALWAYS, Fault::Status(500), Seen::is_head);
    assert!(b.blobs.exists(&key).await.is_err());
    assert_eq!(
        b.fake.count(Seen::is_head) - heads,
        1 + 3,
        "one try and three more"
    );
    b.fake.heal();
    assert!(b.blobs.exists(&key).await.unwrap());
    assert_eq!(
        nothing_stored(&b.storage, ".unfinished").await,
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn a_failure_that_passes_within_the_retries_is_not_seen() {
    let b = bucket_with("", small_parts).await;
    let (key, snapshot) = stored(&b).await;
    let absent = blob_key("org1", &"0".repeat(64));

    b.fake.inject(2, Fault::Status(500), Seen::is_head);
    assert!(b.blobs.exists(&key).await.unwrap());
    b.fake.inject(2, Fault::Status(503), Seen::is_head);
    assert!(!b.blobs.exists(&absent).await.unwrap());

    b.fake.inject(2, Fault::Status(500), Seen::is_get);
    assert_eq!(
        b.blobs.get(snapshot).await.unwrap().as_deref(),
        Some(&b"a snapshot"[..])
    );
    b.fake.inject(3, Fault::Status(429), Seen::is_get);
    assert!(read_back(b.blobs.as_ref(), &key).await.is_some());

    b.fake.inject(2, Fault::Status(500), |r| r.method == "PUT");
    b.blobs
        .put(snapshot, Bytes::from_static(b"again"))
        .await
        .unwrap();

    // A multipart upload whose parts, start and end each fail at first.
    let data = noise(2 * PART + 5, 8);
    let large = blob_key("org1", &sha_of(&data));
    b.fake.inject(2, Fault::Status(500), Seen::starts_upload);
    b.fake.inject(3, Fault::Status(503), Seen::is_part);
    b.fake.inject(2, Fault::Status(500), Seen::completes_upload);
    b.blobs
        .put_stream(&large, chunked(&data, 70_000), u64::MAX)
        .await
        .unwrap();
    assert!(read_back(b.blobs.as_ref(), &large).await.unwrap() == data);
    assert_eq!(b.fake.count(Seen::aborts_upload), 0);

    b.fake.inject(2, Fault::Status(500), Seen::is_list);
    b.fake.inject(2, Fault::Status(500), Seen::is_batch_delete);
    b.blobs.delete_prefix("orgs/org1/").await.unwrap();
    b.fake
        .inject(2, Fault::Status(500), |r| r.method == "DELETE");
    b.blobs.delete(snapshot).await.unwrap();
    b.fake.inject(2, Fault::Status(500), Seen::is_list);
    b.blobs.health().await.unwrap();
    assert_eq!(nothing_stored(&b.storage, "").await, Vec::<String>::new());
}

#[tokio::test]
async fn a_store_that_does_not_answer_fails_a_lookup_within_its_bound() {
    let b = bucket_with("", |t| {
        t.lookup_timeout = Duration::from_millis(400);
        t.first_byte_timeout = Duration::from_millis(400);
    })
    .await;
    let (key, snapshot) = stored(&b).await;

    b.fake.inject(ALWAYS, Fault::Hang, Seen::is_head);
    let started = Instant::now();
    let err = text(&b.blobs.exists(&key).await.unwrap_err());
    assert!(err.contains("did not answer within"), "{err}");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );

    // A read that never starts.
    b.fake.inject(ALWAYS, Fault::Hang, Seen::is_get);
    let started = Instant::now();
    assert!(b.blobs.get(snapshot).await.is_err());
    assert!(b.blobs.get_stream(&key).await.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    b.fake.heal();
    assert!(b.blobs.exists(&key).await.unwrap());
}

/// With the tuning a server runs with: a store that does not answer fails a change's blob check
/// after five seconds, not after the minute one request may take.
#[tokio::test]
async fn a_lookup_fails_after_five_seconds_with_the_tuning_a_server_runs_with() {
    let dir = tempfile::tempdir().unwrap();
    let fake = FakeS3::start(dir.path()).await;
    let mut config = fake.config("");
    config.tuning = S3Tuning::default();
    let blobs = S3Blobs::new(config).unwrap();
    fake.inject(ALWAYS, Fault::Hang, Seen::is_head);
    let started = Instant::now();
    let checked = missing_blobs(&blobs, "org1", ["0".repeat(64), "1".repeat(64)]).await;
    let took = started.elapsed();
    assert!(checked.is_err(), "a blob was called missing or present");
    assert!(
        took >= Duration::from_millis(4500) && took < Duration::from_secs(9),
        "the lookups took {took:?}"
    );
}

/// The request the harness signs by hand (for a real endpoint's unfinished uploads) is one the
/// store takes as signed: the fake checks the signature before it says it has no such listing.
#[tokio::test]
async fn the_hand_signed_listing_of_unfinished_uploads_is_properly_signed() {
    let b = bucket_with("some/prefix", |_| {}).await;
    let right = common::list_uploads(&b.fake.config("some/prefix"))
        .await
        .unwrap();
    let mut wrong = b.fake.config("some/prefix");
    wrong.secret_access_key = "not/the/secret".into();
    let wrong = common::list_uploads(&wrong).await.unwrap();
    assert_eq!(wrong.status(), StatusCode::FORBIDDEN);
    assert_ne!(
        right.status(),
        StatusCode::FORBIDDEN,
        "the signature was refused"
    );
    let asked = b.fake.seen();
    assert!(
        asked[0].query.contains("uploads") && asked[0].query.contains("prefix=some%2Fprefix%2F"),
        "{asked:?}"
    );
}

#[tokio::test]
async fn a_bucket_that_is_gone_is_an_error_never_absent() {
    let b = bucket().await;
    let (key, snapshot) = stored(&b).await;
    b.fake.remove_bucket();
    // A storage that has not heard from the bucket lately asks it before it says "absent".
    let fresh = S3Blobs::new(b.fake.config("")).unwrap();
    let err = text(&fresh.exists(&key).await.unwrap_err());
    assert!(
        err.contains("the S3 bucket") && err.contains("404"),
        "{err}"
    );
    assert!(fresh.get(snapshot).await.is_err());
    assert!(fresh.get_stream(&key).await.is_err());
    assert!(fresh.health().await.is_err());
    assert!(fresh.verify().await.is_err());
    assert!(fresh.put(snapshot, Bytes::from_static(b"x")).await.is_err());
    assert!(fresh.delete_prefix("orgs/org1/").await.is_err());
    assert!(
        missing_blobs(&fresh, "org1", ["0".repeat(64)])
            .await
            .is_err()
    );
    // The one that had just heard from it finds out at its next health check at the latest.
    assert!(b.blobs.health().await.is_err());
}

#[tokio::test]
async fn absent_is_absent_once_the_bucket_has_answered() {
    let b = bucket().await;
    let absent = blob_key("org1", &"0".repeat(64));
    // The first "no such object" is checked against the bucket; those after it are not.
    assert!(!b.blobs.exists(&absent).await.unwrap());
    assert_eq!(b.fake.count(Seen::is_list), 1);
    assert!(!b.blobs.exists(&absent).await.unwrap());
    assert_eq!(b.blobs.get("docs/d1/none").await.unwrap(), None);
    assert!(b.blobs.get_stream(&absent).await.unwrap().is_none());
    assert_eq!(b.fake.count(Seen::is_list), 1);
    assert_eq!(
        missing_blobs(
            b.blobs.as_ref(),
            "org1",
            ["0".repeat(64), "not a hash".into()]
        )
        .await
        .unwrap(),
        ["0".repeat(64), "not a hash".to_owned()]
    );
}

// ------------------------------------------------------------------ multipart uploads

fn part_number(seen: &Seen) -> Option<usize> {
    seen.is_part().then(|| {
        seen.query
            .split('&')
            .find_map(|q| q.strip_prefix("partNumber="))
            .and_then(|n| n.parse().ok())
            .expect("a part number")
    })
}

#[tokio::test]
async fn a_large_blob_goes_up_in_bounded_parts_and_shows_only_when_complete() {
    let b = bucket_with("", small_parts).await;
    let data = noise(4 * PART + 100_000, 4);
    let key = blob_key("org1", &sha_of(&data));
    // The body is fed by hand, and held back just before its end.
    let (feed, body) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(1);
    let body: ByteStream = stream::unfold(body, |mut body| async move {
        body.recv().await.map(|item| (item, body))
    })
    .boxed();
    let uploading = {
        let (blobs, key) = (b.blobs.clone(), key.clone());
        tokio::spawn(async move { blobs.put_stream(&key, body, u64::MAX).await })
    };
    for chunk in data.chunks(100_000) {
        feed.send(Ok(Bytes::copy_from_slice(chunk))).await.unwrap();
    }
    // Three whole parts are at the store, and nothing is visible under the address.
    assert!(
        eventually(|| b.fake.parts_held() >= 3).await,
        "no parts went up"
    );
    assert!(!b.fake.object_file(&key).exists());
    assert!(!b.blobs.exists(&key).await.unwrap());
    assert!(b.blobs.get_stream(&key).await.unwrap().is_none());
    assert_eq!(b.fake.count(Seen::completes_upload), 0);
    // The body ends (well): now it is.
    drop(feed);
    let outcome = uploading.await.unwrap().unwrap();
    assert_eq!(outcome.sha256, sha_of(&data));
    assert!(b.blobs.exists(&key).await.unwrap());

    let seen = b.fake.seen();
    assert_eq!(b.fake.count(Seen::starts_upload), 1);
    assert_eq!(b.fake.count(Seen::completes_upload), 1);
    assert_eq!(b.fake.count(Seen::aborts_upload), 0);
    let parts: Vec<usize> = seen.iter().filter_map(part_number).collect();
    let mut sorted = parts.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, [1, 2, 3, 4, 5], "four full parts and the rest");
    assert_ne!(
        parts[0], 1,
        "the first part, which holds the length, goes once the body has ended: {parts:?}"
    );
    assert_eq!(b.fake.parts_held(), 0, "nothing is left of the upload");

    // At most the first part, the one being filled and two on their way, and a chunk.
    let held = b.blobs.peak_buffered();
    assert!(
        held > PART && held <= 4 * PART + 256 * KIB,
        "most bytes held at once: {held}"
    );
    let stored = std::fs::read(b.fake.object_file(&key)).unwrap();
    assert_eq!(&stored[..8], b"oswxblb1");
    assert_eq!(&stored[8..16], &(data.len() as u64).to_le_bytes());
    assert!(read_back(b.blobs.as_ref(), &key).await.unwrap() == data);

    // A large whole value goes the same way, and comes back as it was.
    let value = Bytes::from(noise(2 * PART + 7, 6));
    b.blobs.put("docs/d1/large", value.clone()).await.unwrap();
    assert_eq!(b.fake.count(Seen::completes_upload), 2);
    assert!(b.blobs.get("docs/d1/large").await.unwrap() == Some(value));
}

#[tokio::test]
async fn an_upload_that_fails_or_is_abandoned_is_aborted_and_never_shows() {
    let b = bucket_with("", small_parts).await;
    let data = noise(3 * PART, 4);
    let key = blob_key("org1", &sha_of(&data));
    let aborted = |n: usize| {
        let fake = b.fake.clone();
        eventually(move || fake.count(Seen::aborts_upload) == n && fake.parts_held() == 0)
    };

    // The body turns out wrong at its end (the digest does not match).
    let wrong = wrong_body(data.clone(), 100_000, Duration::ZERO);
    let err = b.blobs.put_stream(&key, wrong, u64::MAX).await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<PutError>(),
        Some(PutError::Body(_))
    ));
    assert!(b.fake.count(Seen::is_part) > 0, "parts had gone up");
    assert!(aborted(1).await, "the upload was not aborted");

    // The request is dropped mid-upload (the client went away).
    let (feed, body) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(1);
    let body: ByteStream = stream::unfold(body, |mut body| async move {
        body.recv().await.map(|item| (item, body))
    })
    .boxed();
    let uploading = {
        let (blobs, key) = (b.blobs.clone(), key.clone());
        tokio::spawn(async move { blobs.put_stream(&key, body, u64::MAX).await })
    };
    let parts = b.fake.count(Seen::is_part);
    for chunk in data.chunks(100_000).take(120) {
        feed.send(Ok(Bytes::copy_from_slice(chunk))).await.unwrap();
    }
    assert!(eventually(|| b.fake.count(Seen::is_part) > parts).await);
    uploading.abort();
    assert!(uploading.await.unwrap_err().is_cancelled());
    assert!(aborted(2).await, "the abandoned upload was not aborted");

    // The store fails a part for good, then the completion for good.
    b.fake.inject(ALWAYS, Fault::Status(500), Seen::is_part);
    let err = b
        .blobs
        .put_stream(&key, chunked(&data, 100_000), u64::MAX)
        .await
        .unwrap_err();
    assert!(
        err.downcast_ref::<PutError>().is_none(),
        "the body is blamed"
    );
    b.fake.heal();
    assert!(aborted(3).await);
    b.fake
        .inject(ALWAYS, Fault::Status(500), Seen::completes_upload);
    assert!(
        b.blobs
            .put_stream(&key, chunked(&data, 100_000), u64::MAX)
            .await
            .is_err()
    );
    b.fake.heal();
    assert!(aborted(4).await);

    // A part that hangs is given up on (and tried again, and given up on).
    let slow = bucket_with("", |t| {
        small_parts(t);
        t.request_timeout = Duration::from_millis(300);
        t.max_retries = 1;
    })
    .await;
    slow.fake.inject(ALWAYS, Fault::Hang, Seen::is_part);
    let started = Instant::now();
    assert!(
        slow.blobs
            .put_stream(&key, chunked(&data, 100_000), u64::MAX)
            .await
            .is_err()
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    slow.fake.heal();
    assert!(eventually(|| slow.fake.count(Seen::aborts_upload) == 1).await);

    // Through all of it the address never held anything.
    assert_eq!(
        b.fake.count(Seen::completes_upload),
        1 + 3,
        "only the completion that failed, tried four times"
    );
    assert!(!b.blobs.exists(&key).await.unwrap());
    assert!(b.blobs.get_stream(&key).await.unwrap().is_none());
    assert_eq!(nothing_stored(&b.storage, "").await, Vec::<String>::new());
    assert_eq!(
        nothing_stored(&slow.storage, "").await,
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn a_64_mb_upload_holds_a_few_parts_at_most() {
    let b = bucket().await;
    // (Every megabyte different: zstd would fold repeats of one block into nothing.)
    let block = |n: usize| Bytes::from(noise(MIB, 11 + 2 * n as u64));
    let body: ByteStream = stream::iter((0..64).map(move |n| Ok::<_, io::Error>(block(n)))).boxed();
    let key = blob_key("org1", &"ab".repeat(32));
    let outcome = b.blobs.put_stream(&key, body, u64::MAX).await.unwrap();
    assert_eq!(outcome.raw_len, 64 * MIB as u64);
    // The first part (8 MiB), the one being filled and three on their way; never the 64 MiB.
    let held = b.blobs.peak_buffered();
    assert!(
        held > 8 * MIB && held <= 5 * 8 * MIB + MIB,
        "most bytes held at once: {held}"
    );
    let parts = b.fake.count(Seen::is_part);
    assert!((8..=9).contains(&parts), "64 MiB in parts of 8: {parts}");
    let (len, mut chunks) = b.blobs.get_stream(&key).await.unwrap().unwrap();
    assert_eq!(len, 64 * MIB as u64);
    let mut back = Vec::with_capacity(2 * MIB);
    let mut blocks = 0;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.unwrap();
        assert!(chunk.len() <= 64 * KIB);
        back.extend_from_slice(&chunk);
        while back.len() >= MIB {
            assert!(
                back[..MIB] == block(blocks)[..],
                "megabyte {blocks} changed"
            );
            back.drain(..MIB);
            blocks += 1;
        }
    }
    assert_eq!((blocks, back.len()), (64, 0));
}

// ------------------------------------------------------------------ reads that stall

#[tokio::test]
async fn a_read_the_store_stops_feeding_is_given_up_on() {
    let b = bucket_with("", |t| t.idle_timeout = Duration::from_millis(400)).await;
    let data = noise(2 * MIB, 5);
    let key = blob_key("org1", &sha_of(&data));
    b.blobs
        .put_stream(&key, chunked(&data, 100_000), u64::MAX)
        .await
        .unwrap();
    b.blobs
        .put("docs/d1/s", Bytes::from(data.clone()))
        .await
        .unwrap();

    b.fake
        .inject(1, Fault::StallBodyAfter(300_000), Seen::is_get);
    let started = Instant::now();
    let (len, mut chunks) = b.blobs.get_stream(&key).await.unwrap().unwrap();
    assert_eq!(len, data.len() as u64);
    let mut got = 0;
    let err = loop {
        match chunks
            .next()
            .await
            .expect("the stream ended without an error")
        {
            Ok(chunk) => got += chunk.len(),
            Err(err) => break err,
        }
    };
    assert_eq!(err.kind(), io::ErrorKind::TimedOut, "{err}");
    assert!(err.get_ref().is_some_and(|e| e.is::<Stalled>()), "{err}");
    assert!(got < data.len());
    assert!(chunks.next().await.is_none(), "nothing follows the error");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    // A whole value (a snapshot) that stalls is an error, not a short value and not "absent".
    b.fake
        .inject(1, Fault::StallBodyAfter(300_000), Seen::is_get);
    assert!(b.blobs.get("docs/d1/s").await.is_err());
    // The store is fine again: both read whole.
    assert!(read_back(b.blobs.as_ref(), &key).await.unwrap() == data);
    assert!(b.blobs.get("docs/d1/s").await.unwrap().unwrap() == data);
}

// ------------------------------------------------------------------ removing a prefix

/// 1,005 objects under `orgs/1/` (more than one page of a listing, more than one batch of
/// deletes) and four beside it.
async fn many_objects(blobs: &Arc<S3Blobs>) -> Vec<&'static str> {
    let keys: Vec<String> = (0..1005).map(|n| format!("orgs/1/blobs/{n:04}")).collect();
    stream::iter(&keys)
        .for_each_concurrent(32, |key| {
            let blobs = blobs.clone();
            async move { blobs.put(key, Bytes::from_static(b"x")).await.unwrap() }
        })
        .await;
    let beside = ["docs/1/s", "orgs/10/x", "orgs/1x", "orgs/2/y"];
    for key in beside {
        blobs.put(key, Bytes::from_static(b"x")).await.unwrap();
    }
    beside.to_vec()
}

#[tokio::test]
async fn removing_a_prefix_pages_through_it_and_touches_nothing_beside_it() {
    let b = bucket().await;
    let beside = many_objects(&b.blobs).await;
    assert_eq!(b.blobs.keys().await.unwrap().len(), 1009);
    let (lists, batches) = (
        b.fake.count(Seen::is_list),
        b.fake.count(Seen::is_batch_delete),
    );
    b.blobs.delete_prefix("orgs/1/").await.unwrap();
    let asked: Vec<Seen> = b.fake.seen().into_iter().filter(Seen::is_list).collect();
    assert_eq!(b.blobs.keys().await.unwrap(), beside);
    assert!(
        asked[lists..]
            .iter()
            .all(|r| r.query.contains("prefix=orgs%2F1%2F")),
        "{:?}",
        &asked[lists..]
    );
    assert!(asked.len() - lists >= 2, "the listing came in one page");
    assert_eq!(
        b.fake.count(Seen::is_batch_delete) - batches,
        2,
        "1,005 keys go in two batches"
    );
}

#[tokio::test]
async fn a_prefix_that_is_only_partly_removed_is_an_error_and_the_rest_is_gone() {
    let b = bucket().await;
    let beside = many_objects(&b.blobs).await;
    // The first batch of deletes fails for good (one try and three more); the second works.
    b.fake.inject(4, Fault::Status(500), Seen::is_batch_delete);
    let err = text(&b.blobs.delete_prefix("orgs/1/").await.unwrap_err());
    assert!(err.contains("1000 objects could not be removed"), "{err}");
    no_secrets(&err);
    assert_eq!(b.blobs.keys().await.unwrap().len(), 1000 + beside.len());

    // The listing fails for good: an error, and nothing beside the prefix is touched.
    b.fake.inject(ALWAYS, Fault::Status(500), Seen::is_list);
    assert!(b.blobs.delete_prefix("orgs/1/").await.is_err());
    b.fake.heal();

    // Asked again, it finishes.
    b.blobs.delete_prefix("orgs/1/").await.unwrap();
    assert_eq!(b.blobs.keys().await.unwrap(), beside);
}

// ------------------------------------------------------------------ the whole server

/// A server whose storage is a bucket of a fake S3 of the test's own.
async fn server_on(b: &Bucket, settings: impl FnOnce(&mut oswx_server::Settings)) -> TestServer {
    let blobs = b.blobs.clone();
    TestServer::start_custom(settings, move |s| s.blobs = blobs).await
}

async fn ready(server: &TestServer) -> StatusCode {
    server.get("/api/ready", None).await.0
}

#[tokio::test]
async fn ready_says_whether_the_bucket_answers() {
    let b = bucket().await;
    let server = server_on(&b, |_| {}).await;
    assert_eq!(ready(&server).await, StatusCode::OK);
    b.fake.inject(ALWAYS, Fault::Status(403), |_| true);
    assert_eq!(ready(&server).await, StatusCode::SERVICE_UNAVAILABLE);
    b.fake.heal();
    assert_eq!(ready(&server).await, StatusCode::OK);
    b.fake.remove_bucket();
    assert_eq!(ready(&server).await, StatusCode::SERVICE_UNAVAILABLE);
    b.fake.stop().await;
    assert_eq!(ready(&server).await, StatusCode::SERVICE_UNAVAILABLE);
}

fn blob_ref(sha: &str) -> oswx_core::Op {
    set(
        &["features", "f2", "params", "body"],
        json!({"$blob": sha, "len": 9}),
    )
}

#[tokio::test]
async fn a_change_that_names_a_blob_is_a_server_error_while_the_store_is_out() {
    let b = bucket().await;
    let server = server_on(&b, |_| {}).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut live = Live::connect(&server, &doc, &world.admin).await;
    assert_eq!(live.change_on(0, vec![set_depth(1)]).await.seq(), 1);

    // A blob that is there, and one that is not, while the store answers.
    let body = b"geometry!".to_vec();
    let sha = sha_of(&body);
    let uploaded = reqwest::Client::new()
        .put(server.url(&format!("/api/projects/{}/blobs/{sha}", world.project)))
        .bearer_auth(&world.admin)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::NO_CONTENT);
    let absent = "a".repeat(64);
    let refused = live.change_on(1, vec![blob_ref(&absent)]).await;
    assert_eq!(refused.code(), RejectCode::MissingBlob);

    // The store refuses, fails, then is unreachable: `server_error` each time, never
    // `missing_blob`, and the document does not move.
    let other = "b".repeat(64);
    b.fake.inject(ALWAYS, Fault::Status(403), |_| true);
    let refused = live.change_on(1, vec![blob_ref(&other)]).await;
    assert_eq!(refused.code(), RejectCode::ServerError);
    b.fake.heal();
    b.fake.inject(ALWAYS, Fault::Status(500), |_| true);
    let refused = live.change_on(1, vec![blob_ref(&"c".repeat(64))]).await;
    assert_eq!(refused.code(), RejectCode::ServerError);
    b.fake.heal();
    b.fake.stop().await;
    let refused = live.change_on(1, vec![blob_ref(&"d".repeat(64))]).await;
    assert_eq!(refused.code(), RejectCode::ServerError);
    let (status, body) = server
        .post(
            &format!("/api/projects/{}/blobs/missing", world.project),
            Some(&world.admin),
            json!(["e".repeat(64)]),
        )
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");

    // Changes that name no blob go on, and the connection was never dropped.
    assert_eq!(live.change_on(1, vec![set_depth(2)]).await.seq(), 2);
    assert!(live.closed.is_none());
    let head = server.state.store.document_by_id(&doc).await.unwrap();
    assert_eq!(head.unwrap().head_seq, 2);
}

#[tokio::test]
async fn a_document_is_not_loaded_while_its_snapshot_cannot_be_read() {
    let b = bucket_with("", |t| t.first_byte_timeout = Duration::from_millis(400)).await;
    let server = server_on(&b, |s| s.snapshot_every = 2).await;
    let world = World::new(&server).await.unwrap();
    let doc = world.document(&server, None, None, None).await;
    let mut live = Live::connect(&server, &doc, &world.admin).await;
    for n in 0..5 {
        assert_eq!(
            live.change_on(n, vec![set_depth(n as i64 + 1)]).await.seq(),
            n + 1
        );
    }
    drop(live);
    // Stopping the server writes a final snapshot: at 5, after those at 0, 2 and 4.
    let server = server.restart().await;
    let snapshots = b.storage.stored_under(&format!("docs/{doc}/")).await;
    assert!(snapshots.len() >= 2, "{snapshots:?}");
    let store = server.state.store.as_ref();
    let blobs = server.state.blobs.as_ref();
    assert_eq!(load_head(store, blobs, &doc).await.unwrap().0.seq, 5);

    // The store refuses, fails, stalls: an error each time, never an older snapshot and never a
    // document from scratch.
    let open = async || {
        let (status, _) = server
            .get(&format!("/api/documents/{doc}/content"), Some(&world.admin))
            .await;
        status
    };
    for fault in [Fault::Status(403), Fault::Status(500), Fault::Hang] {
        b.fake.inject(ALWAYS, fault, Seen::is_get);
        let loaded = tokio::time::timeout(Duration::from_secs(10), load_head(store, blobs, &doc))
            .await
            .expect("loading ended");
        assert!(
            matches!(loaded, Err(LoadError::Store(_))),
            "{:?}",
            loaded.map(|(head, _)| head.seq)
        );
        b.fake.heal();
    }
    b.fake.inject(ALWAYS, Fault::Status(500), Seen::is_get);
    assert_eq!(open().await, StatusCode::INTERNAL_SERVER_ERROR);
    let connecting =
        Live::try_connect(&server, &doc, &world.admin, common::Hello::client("c1")).await;
    assert!(connecting.is_err(), "a room opened without its snapshot");
    b.fake.heal();

    // The newest snapshot is gone from the bucket: the document does not load (from the one
    // before it, or at all) until someone looks into it.
    let newest = snapshots.last().unwrap();
    blobs.delete(newest).await.unwrap();
    let loaded = load_head(store, blobs, &doc).await;
    assert!(
        matches!(&loaded, Err(LoadError::SnapshotMissing(key)) if key == newest),
        "{:?}",
        loaded.map(|(head, _)| head.seq)
    );
    assert_eq!(open().await, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn a_server_starts_only_on_a_bucket_it_can_fully_use() {
    let b = bucket().await;
    b.blobs.verify().await.unwrap();
    assert_eq!(b.storage.stored_under("").await, Vec::<String>::new());

    // A key that may write, read and remove but not list: S3 then answers 403, not 404, for an
    // object that is not there, and every missing blob would look like an outage.
    b.fake.inject(ALWAYS, Fault::Status(403), Seen::is_list);
    let err = text(&b.blobs.verify().await.unwrap_err());
    assert!(err.contains("cannot be listed"), "{err}");
    no_secrets(&err);
    b.fake.heal();
    b.fake.inject(ALWAYS, Fault::Status(403), Seen::is_head);
    let err = text(&b.blobs.verify().await.unwrap_err());
    assert!(err.contains("s3:ListBucket"), "{err}");
    b.fake.heal();

    // A key that may not write, or not remove.
    b.fake
        .inject(ALWAYS, Fault::Status(403), |r| r.method == "PUT");
    assert!(b.blobs.verify().await.is_err());
    b.fake.heal();
    b.fake
        .inject(ALWAYS, Fault::Status(403), |r| r.method == "DELETE");
    assert!(b.blobs.verify().await.is_err());
    b.fake.heal();
    b.blobs.verify().await.unwrap();
}
