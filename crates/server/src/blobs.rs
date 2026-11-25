//! Blob and snapshot storage: a local folder ([`LocalBlobs`]) or, from Task 17, an S3 bucket.
//!
//! Storage is a flat map from keys to bytes. A key is a `/`-separated path of components made of
//! ASCII letters, digits, `.`, `_` and `-`, none empty, none starting or ending with `.`
//! ([`check_key`]); no key can name anything outside the storage root.
//!
//! There are two kinds of value, written and read through different methods:
//!
//! - **Blobs** ([`Blobs::put_stream`], [`Blobs::get_stream`]): large binary data that documents refer
//!   to by SHA-256 (imported geometry, up to `max_blob_bytes`). They are streamed in both directions,
//!   never held whole in memory, and stored zstd-compressed by this layer; readers get the raw bytes
//!   back and learn their length before reading any.
//! - **Whole values** ([`Blobs::put`], [`Blobs::get`]): small values kept exactly as given (document
//!   snapshots, which their own encoder compresses). This layer does not compress them.
//!
//! Do not mix them: [`Blobs::get`] on a key written by [`Blobs::put_stream`] answers the stored
//! (compressed, framed) bytes, and [`Blobs::get_stream`] on a key written by [`Blobs::put`] fails.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, bail, ensure};
use async_compression::tokio::bufread::ZstdDecoder;
use async_compression::tokio::write::ZstdEncoder;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, BoxStream, Stream, StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio_util::io::ReaderStream;

/// A stream of bytes, as a request body or a blob being read.
pub type ByteStream = BoxStream<'static, io::Result<Bytes>>;

/// What [`Blobs::put_stream`] stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOutcome {
    /// How many raw (uncompressed) bytes the body had.
    pub raw_len: u64,
    /// The SHA-256 of the raw bytes, in lower-case hex.
    pub sha256: String,
}

/// Why [`Blobs::put_stream`] stored nothing, when it was the body's fault rather than the storage's.
/// It comes back inside the `anyhow::Error`; find it with `downcast_ref::<PutError>()`. Any other
/// error is the storage's.
#[derive(Debug, thiserror::Error)]
pub enum PutError {
    /// The body was longer than the limit. Reading stopped at the chunk that went over.
    #[error("the body is over the limit")]
    TooLarge,
    /// The body stream answered this error (a client that went away, or an error the caller's
    /// stream chose to raise, such as a digest that does not match).
    #[error("reading the body failed")]
    Body(#[source] io::Error),
}

/// Where blobs and snapshots are kept.
#[async_trait]
pub trait Blobs: Send + Sync + 'static {
    /// Stores the blob `body` under `key`, zstd-compressed, and answers its raw length and the
    /// SHA-256 of its raw bytes.
    ///
    /// The body is read one chunk at a time and never held whole. Once more than `limit` raw bytes
    /// have arrived, reading stops and the error is [`PutError::TooLarge`]; an error from the body
    /// itself is [`PutError::Body`]. On any error nothing is stored under `key`, and nothing is left
    /// behind. A reader of `key` sees the old value or the whole new one, never part of one.
    ///
    /// Blob keys are content-addressed, so if `key` was written meanwhile by someone else the
    /// value already there may be kept.
    async fn put_stream(
        &self,
        key: &str,
        body: ByteStream,
        limit: u64,
    ) -> anyhow::Result<PutOutcome>;

    /// The blob under `key` as written by [`Blobs::put_stream`]: its raw length and a stream of its
    /// raw (decompressed) bytes, or `None` if there is none. The stream fails if the stored data
    /// turns out shorter or longer than the length.
    async fn get_stream(&self, key: &str) -> anyhow::Result<Option<(u64, ByteStream)>>;

    /// Whether anything is stored under `key`. An outage is an error, never `false`: a storage that
    /// cannot answer must not make a blob look missing.
    async fn exists(&self, key: &str) -> anyhow::Result<bool>;

    /// Stores `bytes` under `key` exactly as given, replacing what was there, all or nothing.
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()>;

    /// The bytes [`Blobs::put`] stored under `key`, or `None` if there are none.
    async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>>;

    /// Removes everything under `prefix`, which must end with `/` (`orgs/<id>/`; used when an
    /// organisation is deleted). Removing nothing is fine.
    async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()>;

    /// Removes the value under `key` (a snapshot whose database row was never committed). Removing
    /// nothing is fine.
    async fn delete(&self, key: &str) -> anyhow::Result<()>;

    /// Answers when the storage can be written (`GET /api/ready`).
    async fn health(&self) -> anyhow::Result<()>;
}

/// The key of a blob of an organisation: `orgs/<org>/blobs/<sha[:2]>/<sha>`. Blobs belong to the
/// organisation: every project in it reaches the same blob. `sha256_hex` must already be checked
/// ([`is_sha256`]).
pub fn blob_key(org: &str, sha256_hex: &str) -> String {
    let fan = sha256_hex.get(..2).unwrap_or(sha256_hex);
    format!("orgs/{org}/blobs/{fan}/{sha256_hex}")
}

/// Whether `s` is a SHA-256 in lower-case hex (64 characters of `0-9a-f`).
pub fn is_sha256(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// The longest key [`check_key`] accepts.
const MAX_KEY: usize = 512;
/// The longest key component [`check_key`] accepts.
const MAX_COMPONENT: usize = 128;

/// Refuses a key that is not a `/`-separated path of safe components: each non-empty, at most 128
/// characters of ASCII letters, digits, `.`, `_` and `-`, not starting or ending with `.` (so never
/// `.` or `..`, and never one of this storage's own temporary files), and not a Windows device name
/// (`CON`, `NUL`, `COM1`, ...). So a key is never absolute, never climbs out of the root and never
/// holds a separator other than `/`.
pub fn check_key(key: &str) -> anyhow::Result<()> {
    let ok = !key.is_empty() && key.len() <= MAX_KEY && key.split('/').all(valid_component);
    ensure!(ok, "bad storage key {key:?}");
    Ok(())
}

fn valid_component(c: &str) -> bool {
    !c.is_empty()
        && c.len() <= MAX_COMPONENT
        && !c.starts_with('.')
        && !c.ends_with('.')
        && c.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        && !windows_device(c)
}

/// `CON`, `PRN`, `AUX`, `NUL`, `COM0`-`COM9`, `LPT0`-`LPT9`, in any case and with any extension:
/// names Windows opens as devices rather than files.
fn windows_device(c: &str) -> bool {
    let stem = c.split('.').next().unwrap_or(c).to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.as_bytes()[3].is_ascii_digit())
}

/// How many storage lookups [`missing_blobs`] runs at once.
pub const LOOKUPS_AT_ONCE: usize = 16;

/// The blob references (hashes) that are malformed or name no blob of the organisation, in the order
/// given, asking storage [`LOOKUPS_AT_ONCE`] at a time. A storage that cannot answer is an error,
/// never "missing".
pub async fn missing_blobs(
    blobs: &dyn Blobs,
    org_id: &str,
    refs: impl IntoIterator<Item = String>,
) -> anyhow::Result<Vec<String>> {
    let checked: Vec<Option<String>> = stream::iter(refs.into_iter().map(|sha| {
        let key = is_sha256(&sha).then(|| blob_key(org_id, &sha));
        async move {
            let here = match key {
                Some(key) => blobs.exists(&key).await?,
                None => false,
            };
            anyhow::Ok((!here).then_some(sha))
        }
    }))
    .buffered(LOOKUPS_AT_ONCE)
    .try_collect()
    .await?;
    Ok(checked.into_iter().flatten().collect())
}

// ------------------------------------------------------------------ local folder

/// The first bytes of every file [`Blobs::put_stream`] writes. The next 8 are the raw length
/// (little-endian), then the zstd frame.
const MAGIC: [u8; 8] = *b"oswxblb1";
/// The magic and the raw length.
const HEADER_LEN: usize = 16;
/// How much a blob read hands on at a time.
const READ_CHUNK: usize = 64 << 10;

/// Storage in a folder on this machine. Every write goes to a temporary file in the destination's
/// folder, is flushed to disk, then renamed over the destination; the temporary file is removed on
/// any failure, including a request dropped mid-upload.
///
/// A blob file is self-describing: a 16-byte header (a magic number and the raw length), then the
/// zstd-compressed bytes. No database row is needed to read one.
#[derive(Debug)]
pub struct LocalBlobs {
    root: PathBuf,
    /// The longest body chunk [`Blobs::put_stream`] has held at once (for the streaming tests).
    largest_chunk: AtomicUsize,
}

impl LocalBlobs {
    /// Storage in the folder `root`, which is created if it does not exist.
    pub fn new(root: impl AsRef<Path>) -> anyhow::Result<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)
            .with_context(|| format!("creating the storage folder {}", root.display()))?;
        let root = std::path::absolute(root)?;
        Ok(Self {
            root,
            largest_chunk: AtomicUsize::new(0),
        })
    }

    /// The folder everything is kept in.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The longest single body chunk any [`Blobs::put_stream`] call has held in memory: a test hook
    /// for the promise that uploads are streamed.
    #[doc(hidden)]
    pub fn largest_chunk_held(&self) -> usize {
        self.largest_chunk.load(Ordering::Relaxed)
    }

    fn path(&self, key: &str) -> anyhow::Result<PathBuf> {
        check_key(key)?;
        Ok(key.split('/').fold(self.root.clone(), |p, c| p.join(c)))
    }

    /// The folder `path` goes in, created.
    async fn parent_of(path: &Path) -> anyhow::Result<&Path> {
        let dir = path.parent().context("a storage path has a parent")?;
        tokio::fs::create_dir_all(dir).await?;
        Ok(dir)
    }
}

/// A temporary file that is removed when this is dropped, unless it was renamed into place.
struct TempFile {
    path: PathBuf,
    armed: bool,
}

impl TempFile {
    /// A new, empty temporary file in `dir`, named so that no key can name it.
    async fn create(dir: &Path) -> io::Result<(Self, File)> {
        let path = dir.join(format!(".tmp-{}", uuid::Uuid::new_v4().simple()));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        Ok((Self { path, armed: true }, file))
    }

    /// Flushes `file` to disk and renames it to `dest`.
    async fn persist(mut self, mut file: File, dest: &Path) -> io::Result<()> {
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&self.path, dest).await?;
        self.armed = false;
        sync_dir(dest).await;
        Ok(())
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if self.armed
            && let Err(err) = std::fs::remove_file(&self.path)
            && err.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(
                "could not remove the temporary file {}: {err}",
                self.path.display()
            );
        }
    }
}

/// Makes a rename into `path`'s folder durable (on Unix; Windows has no such step).
async fn sync_dir(path: &Path) {
    #[cfg(unix)]
    if let Some(dir) = path.parent()
        && let Ok(d) = File::open(dir).await
    {
        let _ = d.sync_all().await;
    }
    #[cfg(not(unix))]
    let _ = path;
}

/// Whether `err` means "there is nothing there" (rather than "the storage failed").
fn is_absent(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
    )
}

/// `inner`, failing if it yields more or fewer than `expected` bytes in all.
fn exact_length(
    inner: impl Stream<Item = io::Result<Bytes>> + Send + Unpin + 'static,
    expected: u64,
) -> ByteStream {
    let wrong = move |seen: u64| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the stored blob has {seen}+ bytes, not the {expected} its header says"),
        )
    };
    stream::unfold(
        (inner, 0u64, false),
        move |(mut inner, seen, done)| async move {
            if done {
                return None;
            }
            match inner.next().await {
                Some(Ok(chunk)) => {
                    let seen = seen + chunk.len() as u64;
                    if seen > expected {
                        Some((Err(wrong(seen)), (inner, seen, true)))
                    } else {
                        Some((Ok(chunk), (inner, seen, false)))
                    }
                }
                Some(Err(err)) => Some((Err(err), (inner, seen, true))),
                None if seen != expected => Some((Err(wrong(seen)), (inner, seen, true))),
                None => None,
            }
        },
    )
    .boxed()
}

#[async_trait]
impl Blobs for LocalBlobs {
    async fn put_stream(
        &self,
        key: &str,
        mut body: ByteStream,
        limit: u64,
    ) -> anyhow::Result<PutOutcome> {
        let dest = self.path(key)?;
        let dir = Self::parent_of(&dest).await?;
        // `tmp` is declared first so it is dropped last, after the file is closed.
        let (tmp, mut file) = TempFile::create(dir).await?;
        // The header is written last, once the length is known; until then the file has no magic.
        file.write_all(&[0; HEADER_LEN]).await?;
        let mut encoder = ZstdEncoder::new(file);
        let mut hasher = Sha256::new();
        let mut raw_len = 0u64;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(PutError::Body)?;
            self.largest_chunk.fetch_max(chunk.len(), Ordering::Relaxed);
            raw_len += chunk.len() as u64;
            if raw_len > limit {
                return Err(PutError::TooLarge.into());
            }
            hasher.update(&chunk);
            encoder.write_all(&chunk).await?;
        }
        drop(body);
        encoder.shutdown().await?;
        let mut file = encoder.into_inner();
        let mut header = [0u8; HEADER_LEN];
        header[..8].copy_from_slice(&MAGIC);
        header[8..].copy_from_slice(&raw_len.to_le_bytes());
        file.seek(io::SeekFrom::Start(0)).await?;
        file.write_all(&header).await?;
        let tmp_path = tmp.path.clone();
        if let Err(err) = tmp.persist(file, &dest).await {
            // Someone else stored this content-addressed key meanwhile (Windows refuses to rename
            // over a file in some states): theirs is as good as ours. `persist` failing dropped
            // and so removed the temporary file.
            let theirs = tokio::fs::metadata(&dest).await.is_ok_and(|m| m.is_file());
            if !theirs {
                return Err(anyhow::Error::new(err).context(format!(
                    "storing {} as {}",
                    tmp_path.display(),
                    dest.display()
                )));
            }
        }
        Ok(PutOutcome {
            raw_len,
            sha256: hex::encode(hasher.finalize()),
        })
    }

    async fn get_stream(&self, key: &str) -> anyhow::Result<Option<(u64, ByteStream)>> {
        let path = self.path(key)?;
        let mut file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) if is_absent(&err) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let mut header = [0u8; HEADER_LEN];
        file.read_exact(&mut header)
            .await
            .with_context(|| format!("reading the header of {key}"))?;
        if header[..8] != MAGIC {
            bail!("{key} is not a blob file");
        }
        let raw_len = u64::from_le_bytes(header[8..].try_into()?);
        let decoder = ZstdDecoder::new(BufReader::new(file));
        let chunks = ReaderStream::with_capacity(decoder, READ_CHUNK);
        Ok(Some((raw_len, exact_length(chunks, raw_len))))
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let path = self.path(key)?;
        match tokio::fs::metadata(&path).await {
            Ok(meta) => Ok(meta.is_file()),
            Err(err) if is_absent(&err) => Ok(false),
            Err(err) => Err(anyhow::Error::new(err).context(format!("looking for {key}"))),
        }
    }

    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        let dest = self.path(key)?;
        let dir = Self::parent_of(&dest).await?;
        let (tmp, mut file) = TempFile::create(dir).await?;
        file.write_all(&bytes).await?;
        tmp.persist(file, &dest)
            .await
            .with_context(|| format!("storing {key}"))
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        let path = self.path(key)?;
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(Bytes::from(data))),
            Err(err) if is_absent(&err) => Ok(None),
            Err(err) => Err(anyhow::Error::new(err).context(format!("reading {key}"))),
        }
    }

    async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        let Some(folder) = prefix.strip_suffix('/') else {
            bail!("a prefix to delete must end with /: {prefix:?}");
        };
        let path = self.path(folder)?;
        match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => Ok(()),
            Err(err) if is_absent(&err) => Ok(()),
            Err(err) => Err(anyhow::Error::new(err).context(format!("removing {prefix}"))),
        }
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let path = self.path(key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(err) if is_absent(&err) => Ok(()),
            Err(err) => Err(anyhow::Error::new(err).context(format!("removing {key}"))),
        }
    }

    async fn health(&self) -> anyhow::Result<()> {
        tokio::fs::create_dir_all(&self.root).await?;
        let (tmp, mut file) = TempFile::create(&self.root)
            .await
            .context("the storage folder cannot be written")?;
        file.write_all(b"ok").await?;
        file.flush().await?;
        drop(file);
        drop(tmp);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_keys_fan_out_by_the_first_two_characters() {
        let sha = "ab".repeat(32);
        assert_eq!(
            blob_key("0123456789abcdef0123456789abcdef", &sha),
            format!("orgs/0123456789abcdef0123456789abcdef/blobs/ab/{sha}")
        );
    }

    #[test]
    fn only_lower_case_hex_of_the_right_length_is_a_sha256() {
        assert!(is_sha256(&"0f".repeat(32)));
        assert!(!is_sha256(&"0F".repeat(32)));
        assert!(!is_sha256(&"0f".repeat(31)));
        assert!(!is_sha256(&format!("{}g", "0".repeat(63))));
        assert!(!is_sha256(""));
    }

    #[test]
    fn keys_cannot_leave_the_root() {
        for good in [
            "orgs/a/blobs/ab/abc",
            "docs/d/snapshots/0000000007.json.gz",
            "x",
            "a-b_c.d",
        ] {
            assert!(check_key(good).is_ok(), "{good}");
        }
        let long = "a".repeat(MAX_COMPONENT + 1);
        for bad in [
            "",
            "../outside",
            "/abs",
            "a/../../b",
            "a/./b",
            "..",
            ".",
            "a//b",
            "a/",
            "sp ace",
            "a\\b",
            "C:/x",
            "C:",
            ".tmp-x",
            "a/.hidden",
            "trailing.",
            "nul",
            "CON.txt",
            "a/com1",
            "lpt9.json",
            "caf\u{e9}",
            long.as_str(),
        ] {
            assert!(check_key(bad).is_err(), "{bad:?}");
        }
        assert!(check_key("console").is_ok());
        assert!(check_key("com").is_ok());
    }
}
