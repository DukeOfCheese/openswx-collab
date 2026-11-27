//! Blob and snapshot storage: a local folder ([`LocalBlobs`]) or a bucket of an S3-compatible
//! store ([`S3Blobs`]).
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

pub mod s3;

pub use s3::{S3Blobs, S3Config, S3Tuning};

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use async_compression::tokio::bufread::ZstdDecoder;
use async_compression::tokio::write::ZstdEncoder;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, BoxStream, Stream, StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
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
    /// cannot answer (unreachable, too slow, refusing the server's credentials, failing) must not
    /// make a blob look missing.
    async fn exists(&self, key: &str) -> anyhow::Result<bool>;

    /// Stores `bytes` under `key` exactly as given, replacing what was there, all or nothing.
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()>;

    /// The bytes [`Blobs::put`] stored under `key`, or `None` if there are none. As with
    /// [`Blobs::exists`], only a storage that answered "there is nothing under this key" is `None`;
    /// any other failure is an error (a snapshot that cannot be read must not look absent).
    async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>>;

    /// Removes everything under `prefix`, which must end with `/` (`orgs/<id>/`; used when an
    /// organisation is deleted). Removing nothing is fine. Only keys that start with the whole
    /// prefix are touched (`orgs/1/` never reaches `orgs/10/...`). If some of it could not be
    /// removed, the rest still is, and the answer is an error saying how much was left.
    async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()>;

    /// Removes the value under `key` (a snapshot whose database row was never committed). Removing
    /// nothing is fine.
    async fn delete(&self, key: &str) -> anyhow::Result<()>;

    /// Answers when the storage is there and can be used (`GET /api/ready`): the folder exists and
    /// can be written, or the bucket exists and answers this server's credentials. A storage that is
    /// missing or unreachable is an error.
    async fn health(&self) -> anyhow::Result<()>;

    /// About the most memory one [`Blobs::put_stream`] holds while it runs, in bytes: what an
    /// upload is charged against `OSWX_COLLAB_UPLOAD_MEMORY` ([`crate::uploads`]). The default,
    /// [`COMPRESSOR_MEMORY`], is for a storage that writes what it reads as it reads it (a folder);
    /// one that gathers parts before sending them says how much it gathers.
    fn upload_memory(&self) -> u64 {
        COMPRESSOR_MEMORY
    }
}

/// About what the compressor of one blob being stored holds (zstd at its default level: a 2 MiB
/// window and its tables), with a body chunk in hand: what an upload to a folder costs in memory.
pub const COMPRESSOR_MEMORY: u64 = 4 << 20;

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

/// How long [`missing_blobs`] waits for storage to say whether one blob is there. A change waits
/// for this check, so a storage that is slow must fail it rather than hold the change.
pub const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// The blob references (hashes) that are malformed or name no blob of the organisation, in the order
/// given, asking storage [`LOOKUPS_AT_ONCE`] at a time. A storage that cannot answer, or does not
/// within [`LOOKUP_TIMEOUT`] per blob, is an error, never "missing".
pub async fn missing_blobs(
    blobs: &dyn Blobs,
    org_id: &str,
    refs: impl IntoIterator<Item = String>,
) -> anyhow::Result<Vec<String>> {
    let checked: Vec<Option<String>> = stream::iter(refs.into_iter().map(|sha| {
        let key = is_sha256(&sha).then(|| blob_key(org_id, &sha));
        async move {
            let here = match key {
                Some(key) => tokio::time::timeout(LOOKUP_TIMEOUT, blobs.exists(&key))
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!(
                            "storage did not say within {LOOKUP_TIMEOUT:?} whether a blob is there"
                        )
                    })??,
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

// ------------------------------------------------------------------ streams that stall

/// The error of a stream that was given up on because no bytes moved for too long. It is the
/// source of an [`io::Error`] of kind [`io::ErrorKind::TimedOut`].
#[derive(Debug, thiserror::Error)]
#[error("no bytes for {0:?}")]
pub struct Stalled(pub Duration);

fn stalled(idle: Duration) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, Stalled(idle))
}

/// `source`, for a reader this server drives (an upload's body, an object being read from a
/// bucket): it fails with [`Stalled`] once no chunk has arrived for `idle`.
pub fn fail_when_stalled(source: ByteStream, idle: Duration) -> ByteStream {
    stream::unfold((source, false), move |(mut source, done)| async move {
        if done {
            return None;
        }
        match tokio::time::timeout(idle, source.next()).await {
            Ok(Some(Ok(chunk))) => Some((Ok(chunk), (source, false))),
            Ok(Some(Err(err))) => Some((Err(err), (source, true))),
            Ok(None) => None,
            Err(_) => Some((Err(stalled(idle)), (source, true))),
        }
    })
    .boxed()
}

/// The error of a body that was given up on because it came too slowly ([`fail_when_slow`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the sender did not keep up {0} bytes a second")]
pub struct TooSlow(pub u64);

/// How fast a body must come, on average: `floor` bytes a second, with `grace` to spare.
///
/// The sender starts with an allowance of `grace`. Waiting for it uses the allowance up; every
/// byte it sends earns `1 / floor` seconds back, up to `grace` again. When the allowance is gone
/// the body is given up on. So a sender at or above the floor is never cut, however long its
/// body takes; a pause of up to `grace` is forgiven once the sender has earned it back; and one
/// that sends a byte now and then (which no idle limit catches) is cut `grace` after it began.
/// Only the time spent waiting for the sender counts, never the time the server took to store
/// what it sent.
///
/// The default floor is 4,096 bytes a second (`OSWX_COLLAB_MIN_UPLOAD_RATE`; 0: none): below
/// what a 56k modem or a poor 2G connection carries, so no honest uplink is cut, and enough
/// that the largest blob (256 MiB) cannot hold a place for more than about 18 hours.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pace {
    /// Bytes a second; 0: no floor.
    pub floor: u64,
    pub grace: Duration,
}

impl Pace {
    /// As the settings say (`min_upload_rate`, `upload_rate_grace`).
    pub fn from_settings(settings: &crate::Settings) -> Self {
        Self {
            floor: settings.min_upload_rate,
            grace: Duration::from_secs_f64(settings.upload_rate_grace),
        }
    }

    /// The allowance left after waiting `waited` for `bytes` bytes with `allowance` in hand.
    pub fn after(&self, allowance: Duration, waited: Duration, bytes: usize) -> Duration {
        if self.floor == 0 {
            return self.grace;
        }
        let earned = Duration::from_secs_f64(bytes as f64 / self.floor as f64);
        (allowance.saturating_sub(waited) + earned).min(self.grace)
    }
}

/// `source`, for a body a client sends: it fails with [`TooSlow`] when the sender does not keep
/// up `pace` (see [`Pace`]).
pub fn fail_when_slow(source: ByteStream, pace: Pace) -> ByteStream {
    if pace.floor == 0 {
        return source;
    }
    let slow = move || io::Error::new(io::ErrorKind::TimedOut, TooSlow(pace.floor));
    stream::unfold(
        (source, pace.grace, false),
        move |(mut source, allowance, done)| async move {
            if done {
                return None;
            }
            let asked = tokio::time::Instant::now();
            match tokio::time::timeout(allowance, source.next()).await {
                Ok(Some(Ok(chunk))) => {
                    let left = pace.after(allowance, asked.elapsed(), chunk.len());
                    Some((Ok(chunk), (source, left, false)))
                }
                Ok(Some(Err(err))) => Some((Err(err), (source, allowance, true))),
                Ok(None) => None,
                Err(_) => Some((Err(slow()), (source, allowance, true))),
            }
        },
    )
    .boxed()
}

/// `source`, for a reader this server does not drive (a download a client pulls at its own pace):
/// `source` is dropped, and whatever it holds released (an open file, a connection to the bucket),
/// once the reader has taken nothing for `idle` or `source` has produced nothing for `idle`. The
/// reader then sees a [`Stalled`] error, or the stream ending early.
///
/// A task of its own pulls `source`, at most two chunks ahead of the reader. It ends when the
/// stream does, when the reader is dropped, or at a stall. Must be called inside a tokio runtime.
pub fn drop_when_stalled(source: ByteStream, idle: Duration) -> ByteStream {
    let (tx, rx) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(1);
    tokio::spawn(async move {
        let mut source = source;
        loop {
            let item = match tokio::time::timeout(idle, source.next()).await {
                Ok(Some(item)) => item,
                Ok(None) => return,
                Err(_) => Err(stalled(idle)),
            };
            let last = item.is_err();
            // The reader went away (the send fails) or has not taken the chunk before this one
            // for `idle` (the wait for room times out): either way `source` is dropped here.
            match tokio::time::timeout(idle, tx.send(item)).await {
                Ok(Ok(())) if !last => {}
                _ => return,
            }
        }
    });
    stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
    .boxed()
}

// ------------------------------------------------------------------ the stored form of a blob

/// The first bytes of every blob [`Blobs::put_stream`] stores. The next 8 are the raw length
/// (little-endian), then the zstd frame. Both implementations store blobs in this form, so a folder
/// can be copied into a bucket (or back) as it is.
const MAGIC: [u8; 8] = *b"oswxblb1";
/// The magic and the raw length.
const HEADER_LEN: usize = 16;
/// How much a blob read hands on at a time.
const READ_CHUNK: usize = 64 << 10;

/// The header of a stored blob of `raw_len` raw bytes.
fn blob_header(raw_len: u64) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[..8].copy_from_slice(&MAGIC);
    header[8..].copy_from_slice(&raw_len.to_le_bytes());
    header
}

/// The raw length and the raw bytes of the blob stored as `stored` (under `key`, for messages).
async fn decode_blob(
    mut stored: impl AsyncRead + Send + Unpin + 'static,
    key: &str,
) -> anyhow::Result<(u64, ByteStream)> {
    let mut header = [0u8; HEADER_LEN];
    stored
        .read_exact(&mut header)
        .await
        .with_context(|| format!("reading the header of {key}"))?;
    if header[..8] != MAGIC {
        bail!("{key} is not a blob file");
    }
    let raw_len = u64::from_le_bytes(header[8..].try_into()?);
    let decoder = ZstdDecoder::new(BufReader::new(stored));
    let chunks = ReaderStream::with_capacity(decoder, READ_CHUNK);
    Ok((raw_len, exact_length(chunks, raw_len)))
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

// ------------------------------------------------------------------ local folder

/// How often a running server removes temporary upload files nothing owns any more
/// ([`LocalBlobs::start_sweeper`]).
pub const TEMP_SWEEP_EVERY: Duration = Duration::from_secs(3600);
/// How long a temporary upload file must have been untouched before the sweep removes it. An
/// upload that stalls is given up on within the blob idle timeout, so a file this old has no
/// writer (and the sweep never takes a file its own process is writing, whatever its age).
pub const TEMP_MAX_AGE: Duration = Duration::from_secs(24 * 3600);

/// What every temporary file's name starts with. No key can name one ([`check_key`]).
const TEMP_PREFIX: &str = ".tmp-";

/// The temporary files this process is writing.
type InUse = Arc<Mutex<HashSet<PathBuf>>>;

/// Storage in a folder on this machine. Every write goes to a temporary file in the destination's
/// folder, is flushed to disk, then renamed over the destination; the temporary file is removed on
/// any failure, including a request dropped mid-upload. One left by a process that was killed is
/// removed by the sweep ([`LocalBlobs::sweep_temp_files`]).
///
/// A blob file is self-describing: a 16-byte header (a magic number and the raw length), then the
/// zstd-compressed bytes. No database row is needed to read one.
///
/// The folder must stay where it is: once it is gone (a volume that was unmounted), every write
/// and [`Blobs::health`] fail rather than creating it again somewhere that will not last.
#[derive(Debug)]
pub struct LocalBlobs {
    root: PathBuf,
    /// The longest body chunk [`Blobs::put_stream`] has held at once (for the streaming tests).
    largest_chunk: AtomicUsize,
    in_use: InUse,
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
            in_use: InUse::default(),
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

    /// Fails unless the storage folder is (still) there.
    async fn root_is_there(&self) -> anyhow::Result<()> {
        let there = tokio::fs::metadata(&self.root)
            .await
            .is_ok_and(|m| m.is_dir());
        ensure!(
            there,
            "the storage folder {} is missing or cannot be reached",
            self.root.display()
        );
        Ok(())
    }

    /// `nothing`, as the answer for a key whose file is not there, unless the storage folder itself
    /// is gone: then nothing in it can be said to be absent, and the answer is an error.
    async fn absent<T>(&self, nothing: T) -> anyhow::Result<T> {
        self.root_is_there().await?;
        Ok(nothing)
    }

    /// The folder `path` goes in, created (inside a storage folder that is there).
    async fn parent_of<'p>(&self, path: &'p Path) -> anyhow::Result<&'p Path> {
        self.root_is_there().await?;
        let dir = path.parent().context("a storage path has a parent")?;
        tokio::fs::create_dir_all(dir).await?;
        Ok(dir)
    }

    /// Removes the temporary upload files under the storage folder that were last written more
    /// than `older_than` ago and that this process is not writing, and answers how many it removed.
    /// These are what a process that was killed mid-upload left behind. A file that cannot be
    /// examined or removed is logged and skipped.
    pub async fn sweep_temp_files(&self, older_than: Duration) -> anyhow::Result<usize> {
        self.root_is_there().await?;
        let (root, in_use) = (self.root.clone(), self.in_use.clone());
        tokio::task::spawn_blocking(move || sweep(&root, &in_use, older_than))
            .await
            .context("sweeping temporary files")
    }

    /// Sweeps now and then every `every` ([`LocalBlobs::sweep_temp_files`] with `older_than`), in a
    /// task that ends once this storage has been dropped. Must be called inside a tokio runtime.
    pub fn start_sweeper(
        self: &Arc<Self>,
        every: Duration,
        older_than: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let weak: Weak<Self> = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let Some(blobs) = weak.upgrade() else { return };
                match blobs.sweep_temp_files(older_than).await {
                    Ok(0) => {}
                    Ok(n) => tracing::info!("removed {n} abandoned temporary upload files"),
                    Err(err) => tracing::warn!("could not sweep temporary upload files: {err:#}"),
                }
                drop(blobs);
                tokio::time::sleep(every).await;
            }
        })
    }
}

/// The body of [`LocalBlobs::sweep_temp_files`], on the blocking pool.
fn sweep(root: &Path, in_use: &InUse, older_than: Duration) -> usize {
    let mut removed = 0;
    let mut todo = vec![root.to_path_buf()];
    while let Some(dir) = todo.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) => {
                if err.kind() != io::ErrorKind::NotFound {
                    tracing::warn!("could not read the folder {}: {err}", dir.display());
                }
                continue;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() {
                todo.push(path);
                continue;
            }
            let temporary = entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(TEMP_PREFIX));
            if !temporary {
                continue;
            }
            let age = entry
                .metadata()
                .and_then(|m| m.modified())
                .map(|at| at.elapsed().unwrap_or(Duration::ZERO));
            let old = match age {
                Ok(age) => age >= older_than,
                Err(err) if err.kind() == io::ErrorKind::NotFound => false,
                Err(err) => {
                    tracing::warn!("could not examine {}: {err}", path.display());
                    false
                }
            };
            // Held while removing, so that the file cannot become one in use in between.
            let writing = in_use.lock().unwrap_or_else(PoisonError::into_inner);
            if !old || writing.contains(&path) {
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => tracing::warn!("could not remove {}: {err}", path.display()),
            }
        }
    }
    removed
}

/// A temporary file that is removed when this is dropped, unless it was renamed into place. For as
/// long as this lives the sweep leaves the file alone.
struct TempFile {
    path: PathBuf,
    armed: bool,
    in_use: InUse,
}

impl TempFile {
    /// A new, empty temporary file in `dir`, named so that no key can name it.
    async fn create(dir: &Path, in_use: &InUse) -> io::Result<(Self, File)> {
        let path = dir.join(format!("{TEMP_PREFIX}{}", uuid::Uuid::new_v4().simple()));
        in_use
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(path.clone());
        // From here on dropping `tmp` takes the name out of `in_use` again.
        let mut tmp = Self {
            path,
            armed: false,
            in_use: in_use.clone(),
        };
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp.path)
            .await?;
        tmp.armed = true;
        Ok((tmp, file))
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
        self.in_use
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.path);
    }
}

/// Makes a rename into `path`'s folder durable (on Unix; Windows has no such step). A failure is
/// logged: the file is in place, but a power cut could still lose its name.
async fn sync_dir(path: &Path) {
    #[cfg(unix)]
    if let Some(dir) = path.parent() {
        let synced: io::Result<()> = async { File::open(dir).await?.sync_all().await }.await;
        if let Err(err) = synced {
            tracing::warn!(
                "could not flush the folder {} to disk: {err}",
                dir.display()
            );
        }
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

#[async_trait]
impl Blobs for LocalBlobs {
    async fn put_stream(
        &self,
        key: &str,
        mut body: ByteStream,
        limit: u64,
    ) -> anyhow::Result<PutOutcome> {
        let dest = self.path(key)?;
        let dir = self.parent_of(&dest).await?;
        // `tmp` is declared first so it is dropped last, after the file is closed.
        let (tmp, mut file) = TempFile::create(dir, &self.in_use).await?;
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
        file.seek(io::SeekFrom::Start(0)).await?;
        file.write_all(&blob_header(raw_len)).await?;
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
        let file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) if is_absent(&err) => return self.absent(None).await,
            Err(err) => return Err(err.into()),
        };
        Ok(Some(decode_blob(file, key).await?))
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let path = self.path(key)?;
        match tokio::fs::metadata(&path).await {
            Ok(meta) => Ok(meta.is_file()),
            Err(err) if is_absent(&err) => self.absent(false).await,
            Err(err) => Err(anyhow::Error::new(err).context(format!("looking for {key}"))),
        }
    }

    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        let dest = self.path(key)?;
        let dir = self.parent_of(&dest).await?;
        let (tmp, mut file) = TempFile::create(dir, &self.in_use).await?;
        file.write_all(&bytes).await?;
        tmp.persist(file, &dest)
            .await
            .with_context(|| format!("storing {key}"))
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        let path = self.path(key)?;
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(Bytes::from(data))),
            Err(err) if is_absent(&err) => self.absent(None).await,
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
        self.root_is_there().await?;
        let (tmp, mut file) = TempFile::create(&self.root, &self.in_use)
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
