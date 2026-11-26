//! What every [`Blobs`] promises, as one suite: [`blobs_contract`] runs against a folder
//! (`tests/storage.rs`) and against S3 (`tests/storage_s3.rs`). It keeps to keys under `orgs/` and
//! `docs/` and removes them when it is done.

use std::io;
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use oswx_server::blobs::{Blobs, ByteStream, PutError, blob_key};
use sha2::{Digest, Sha256};

use super::TestStorage;

const MIB: usize = 1 << 20;

pub fn sha_of(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// `n` bytes that do not compress to nothing.
pub fn noise(n: usize, seed: u64) -> Vec<u8> {
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

pub fn body_of(chunks: Vec<Bytes>) -> ByteStream {
    stream::iter(chunks.into_iter().map(Ok::<_, io::Error>)).boxed()
}

/// `data` in chunks of `chunk` bytes.
pub fn chunked(data: &[u8], chunk: usize) -> ByteStream {
    body_of(data.chunks(chunk).map(Bytes::copy_from_slice).collect())
}

/// A body that sends `data` in chunks of `chunk` bytes, `pause` apart, and then fails as the
/// upload route's body does when the digest is not the one in the address.
pub fn wrong_body(data: Vec<u8>, chunk: usize, pause: Duration) -> ByteStream {
    let chunks: Vec<Bytes> = data.chunks(chunk).map(Bytes::copy_from_slice).collect();
    stream::iter(chunks.into_iter().map(Ok).chain([Err(())]))
        .then(move |item| async move {
            tokio::time::sleep(pause).await;
            item.map_err(|()| io::Error::new(io::ErrorKind::InvalidData, "the digest is wrong"))
        })
        .boxed()
}

/// A blob read back whole.
pub async fn read_back(blobs: &dyn Blobs, key: &str) -> Option<Vec<u8>> {
    let (len, mut chunks) = blobs.get_stream(key).await.expect("the blob is read")?;
    let mut back = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.expect("a chunk of the blob");
        assert!(chunk.len() <= 64 << 10, "a chunk of {} bytes", chunk.len());
        back.extend_from_slice(&chunk);
    }
    assert_eq!(
        len,
        back.len() as u64,
        "the length said first is the length"
    );
    Some(back)
}

/// Waits (up to ten seconds) for nothing to be stored under `prefix`.
pub async fn nothing_stored(storage: &TestStorage, prefix: &str) -> Vec<String> {
    let mut left = storage.stored_under(prefix).await;
    for _ in 0..200 {
        if left.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        left = storage.stored_under(prefix).await;
    }
    left
}

/// Everything a [`Blobs`] promises. `storage` is the same storage as the test reaches it (with
/// nothing in it); `big` is the size of the largest blob written (make it span several parts of a
/// multipart upload on S3).
pub async fn blobs_contract(blobs: &dyn Blobs, storage: &TestStorage, big: usize) {
    blobs.health().await.expect("the storage is healthy");
    whole_values(blobs, storage).await;
    bad_keys(blobs, storage).await;
    removing(blobs, storage).await;
    streamed_blobs(blobs, storage, big).await;
    failed_writes_leave_nothing(blobs, storage).await;
    a_wrong_upload_never_shows(blobs, storage).await;
    damaged_blobs_fail(blobs).await;
    blobs.delete_prefix("orgs/").await.unwrap();
    blobs.delete_prefix("docs/").await.unwrap();
    assert_eq!(nothing_stored(storage, "").await, Vec::<String>::new());
    blobs.health().await.expect("the storage is still healthy");
}

async fn whole_values(blobs: &dyn Blobs, storage: &TestStorage) {
    let key = "docs/d1/snapshots/0000000001-0000000007.mpk.zst";
    assert_eq!(blobs.get(key).await.unwrap(), None);
    assert!(!blobs.exists(key).await.unwrap());
    assert!(blobs.get_stream(key).await.unwrap().is_none());
    blobs.put(key, Bytes::from_static(b"hello")).await.unwrap();
    blobs
        .put(key, Bytes::from_static(b"hello again"))
        .await
        .unwrap(); // (overwrite)
    assert!(blobs.exists(key).await.unwrap());
    assert_eq!(
        blobs.get(key).await.unwrap().as_deref(),
        Some(&b"hello again"[..])
    );
    blobs.put("docs/d1/empty", Bytes::new()).await.unwrap();
    assert_eq!(
        blobs.get("docs/d1/empty").await.unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(
        storage.stored_under("").await,
        ["docs/d1/empty", key],
        "no half-written files left behind"
    );

    // A value far larger than one request should carry (a snapshot of a large document).
    let large = Bytes::from(noise(20 * MIB + 17, 21));
    blobs.put("docs/d1/large", large.clone()).await.unwrap();
    assert!(blobs.get("docs/d1/large").await.unwrap() == Some(large));

    blobs.delete(key).await.unwrap();
    blobs.delete(key).await.unwrap(); // (removing nothing is fine)
    assert!(!blobs.exists(key).await.unwrap());
    assert_eq!(blobs.get(key).await.unwrap(), None);
    blobs.delete_prefix("docs/").await.unwrap();
}

async fn bad_keys(blobs: &dyn Blobs, storage: &TestStorage) {
    for bad in [
        "../outside",
        "/abs",
        "a/../../b",
        "sp ace",
        "",
        "a\\b",
        "C:/x",
        "a//b",
        ".tmp-x",
        ".oswx-probe/x",
        "caf\u{e9}",
    ] {
        assert!(
            blobs.put(bad, Bytes::from_static(b"x")).await.is_err(),
            "{bad:?}"
        );
        assert!(blobs.get(bad).await.is_err(), "{bad:?}");
        assert!(blobs.exists(bad).await.is_err(), "{bad:?}");
        assert!(blobs.get_stream(bad).await.is_err(), "{bad:?}");
        assert!(blobs.delete(bad).await.is_err(), "{bad:?}");
        assert!(
            blobs
                .put_stream(bad, body_of(vec![Bytes::from_static(b"x")]), 10)
                .await
                .is_err(),
            "{bad:?}"
        );
        assert!(
            blobs.delete_prefix(&format!("{bad}/")).await.is_err(),
            "{bad:?}"
        );
    }
    assert_eq!(storage.stored_under("").await, Vec::<String>::new());
}

async fn removing(blobs: &dyn Blobs, storage: &TestStorage) {
    let keys = [
        "orgs/1/blobs/x",
        "orgs/1/blobs/sub/y",
        "orgs/10/blobs/z",
        "orgs/1x/w",
        "orgs/2/v",
        "docs/1/s",
    ];
    for key in keys {
        blobs.put(key, Bytes::from_static(b"x")).await.unwrap();
    }
    blobs.delete_prefix("orgs/1/").await.unwrap();
    blobs.delete_prefix("orgs/nothing-here/").await.unwrap(); // (nothing to remove is fine)
    assert_eq!(
        storage.stored_under("").await,
        ["docs/1/s", "orgs/10/blobs/z", "orgs/1x/w", "orgs/2/v"],
        "orgs/1/ is not orgs/10/ nor orgs/1x"
    );
    // Only a folder-like prefix, and never the root.
    assert!(blobs.delete_prefix("orgs/2").await.is_err());
    assert!(blobs.delete_prefix("/").await.is_err());
    assert!(blobs.delete_prefix("").await.is_err());
    assert!(blobs.delete_prefix("orgs//").await.is_err());
    assert!(blobs.delete_prefix("../").await.is_err());
    assert!(blobs.exists("orgs/2/v").await.unwrap());
    blobs.delete_prefix("orgs/").await.unwrap();
    blobs.delete_prefix("docs/").await.unwrap();
    assert_eq!(storage.stored_under("").await, Vec::<String>::new());
}

async fn streamed_blobs(blobs: &dyn Blobs, storage: &TestStorage, big: usize) {
    // A million bytes that compress well.
    let data = b"0123456789".repeat(100_000);
    let key = blob_key("org1", &sha_of(&data));
    let outcome = blobs
        .put_stream(&key, chunked(&data, 7_777), data.len() as u64)
        .await
        .unwrap();
    assert_eq!(outcome.raw_len, data.len() as u64);
    assert_eq!(outcome.sha256, sha_of(&data));
    assert!(blobs.exists(&key).await.unwrap());
    assert_eq!(storage.stored_under("").await, [key.as_str()]);

    // Stored compressed and self-describing: the same bytes whatever the storage.
    let stored = blobs.get(&key).await.unwrap().unwrap();
    assert!(
        stored.len() < data.len() / 10,
        "stored {} bytes",
        stored.len()
    );
    assert_eq!(&stored[..8], b"oswxblb1");
    assert_eq!(&stored[8..16], &(data.len() as u64).to_le_bytes());
    assert!(read_back(blobs, &key).await.unwrap() == data);

    // Nothing at all is a blob too.
    let empty = blob_key("org1", &sha_of(b""));
    let outcome = blobs.put_stream(&empty, body_of(vec![]), 0).await.unwrap();
    assert_eq!((outcome.raw_len, outcome.sha256), (0, sha_of(b"")));
    assert_eq!(read_back(blobs, &empty).await.unwrap(), b"");

    // One that does not compress, larger than a part, in chunks of an awkward size; and exactly
    // at its limit.
    let large = noise(big, 5);
    let large_key = blob_key("org1", &sha_of(&large));
    let outcome = blobs
        .put_stream(&large_key, chunked(&large, 300_007), big as u64)
        .await
        .unwrap();
    assert_eq!(outcome.raw_len, big as u64);
    assert_eq!(outcome.sha256, sha_of(&large));
    let back = read_back(blobs, &large_key).await.unwrap();
    assert!(back == large, "the large blob came back changed");

    // Two uploads of one blob at once: both succeed and the blob is intact. (`big` is more than
    // a part on S3: two multipart uploads of one key, each completed.)
    let twice = noise(big + 1, 9);
    let twice_key = blob_key("org1", &sha_of(&twice));
    let (a, b) = tokio::join!(
        blobs.put_stream(&twice_key, chunked(&twice, 64_000), u64::MAX),
        blobs.put_stream(&twice_key, chunked(&twice, 50_000), u64::MAX),
    );
    assert_eq!(a.unwrap().sha256, sha_of(&twice));
    assert_eq!(b.unwrap().sha256, sha_of(&twice));
    assert!(read_back(blobs, &twice_key).await.unwrap() == twice);

    assert!(
        blobs
            .get_stream("orgs/org1/blobs/00/none")
            .await
            .unwrap()
            .is_none()
    );
    blobs.delete_prefix("orgs/org1/").await.unwrap();
    assert_eq!(nothing_stored(storage, "").await, Vec::<String>::new());
}

async fn failed_writes_leave_nothing(blobs: &dyn Blobs, storage: &TestStorage) {
    let key = blob_key("org1", &"cd".repeat(32));

    let err = blobs
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
    let err = blobs.put_stream(&key, broken, 500).await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<PutError>(),
        Some(PutError::Body(_))
    ));

    // A large one that goes over its limit late, and one that breaks late: by then parts of it
    // have gone to storage.
    let large = noise(20 * MIB, 3);
    let err = blobs
        .put_stream(&key, chunked(&large, MIB), (20 * MIB - 1) as u64)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<PutError>(),
        Some(PutError::TooLarge)
    ));
    let err = blobs
        .put_stream(&key, wrong_body(large, MIB, Duration::ZERO), u64::MAX)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<PutError>(),
        Some(PutError::Body(_))
    ));

    assert!(!blobs.exists(&key).await.unwrap());
    assert!(blobs.get_stream(&key).await.unwrap().is_none());
    assert_eq!(nothing_stored(storage, "").await, Vec::<String>::new());
}

/// Two uploads of one address at once, one whose content is right and one whose content is wrong:
/// whichever ends first, the address holds the right content or nothing, never the wrong one.
async fn a_wrong_upload_never_shows(blobs: &dyn Blobs, storage: &TestStorage) {
    let right = noise(18 * MIB, 31);
    let wrong = noise(18 * MIB, 32);
    let key = blob_key("org1", &sha_of(&right));
    let slow = Duration::from_millis(15);

    // The wrong one alone: nothing.
    let err = blobs
        .put_stream(
            &key,
            wrong_body(wrong.clone(), MIB, Duration::ZERO),
            u64::MAX,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<PutError>(),
        Some(PutError::Body(_))
    ));
    assert!(!blobs.exists(&key).await.unwrap());

    // The wrong one ends last, then first.
    for (pause_right, pause_wrong) in [(Duration::ZERO, slow), (slow, Duration::ZERO)] {
        let right_body = stream::iter(
            right
                .chunks(MIB)
                .map(Bytes::copy_from_slice)
                .collect::<Vec<_>>(),
        )
        .then(move |chunk| async move {
            tokio::time::sleep(pause_right).await;
            Ok::<_, io::Error>(chunk)
        })
        .boxed();
        let seen_meanwhile = async {
            // While both are on their way, the address is empty or right.
            for _ in 0..20 {
                if let Some(back) = read_back(blobs, &key).await {
                    assert!(back == right, "the address showed wrong content");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let (a, b, ()) = tokio::join!(
            blobs.put_stream(&key, right_body, u64::MAX),
            blobs.put_stream(&key, wrong_body(wrong.clone(), MIB, pause_wrong), u64::MAX),
            seen_meanwhile,
        );
        assert_eq!(a.unwrap().sha256, sha_of(&right));
        assert!(b.is_err());
        let back = read_back(blobs, &key)
            .await
            .expect("the right upload stored it");
        assert!(back == right, "the address holds wrong content");
        blobs.delete(&key).await.unwrap();
        assert_eq!(nothing_stored(storage, "").await, Vec::<String>::new());
    }
}

async fn damaged_blobs_fail(blobs: &dyn Blobs) {
    let data = noise(300_000, 7);
    let key = blob_key("org1", &sha_of(&data));
    blobs
        .put_stream(&key, body_of(vec![Bytes::from(data.clone())]), u64::MAX)
        .await
        .unwrap();
    // (`put` stores bytes as they are, so it can stand in for damage to the stored form.)
    let mut stored = blobs.get(&key).await.unwrap().unwrap().to_vec();
    stored[8..16].copy_from_slice(&(data.len() as u64 + 5).to_le_bytes());
    blobs.put(&key, Bytes::from(stored.clone())).await.unwrap();
    let (_, chunks) = blobs.get_stream(&key).await.unwrap().unwrap();
    let results: Vec<_> = chunks.collect().await;
    assert!(
        results.last().unwrap().is_err(),
        "a blob shorter than its header says ends in an error"
    );

    stored[8..16].copy_from_slice(&(data.len() as u64 - 5).to_le_bytes());
    blobs.put(&key, Bytes::from(stored)).await.unwrap();
    let (_, chunks) = blobs.get_stream(&key).await.unwrap().unwrap();
    let results: Vec<_> = chunks.collect().await;
    assert!(
        results.last().unwrap().is_err(),
        "a blob longer than its header says ends in an error"
    );

    blobs
        .put(&key, Bytes::from_static(b"not a blob"))
        .await
        .unwrap();
    assert!(blobs.get_stream(&key).await.is_err());
    blobs.delete(&key).await.unwrap();
}
