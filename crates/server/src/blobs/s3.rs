//! Blob and snapshot storage in a bucket of an S3-compatible store (AWS S3, Cloudflare R2, MinIO,
//! Garage, Backblaze B2): [`S3Blobs`]. Nothing here is specific to one provider: it uses object
//! PUT, GET, HEAD and DELETE, multipart uploads, `ListObjectsV2` and `DeleteObjects`, signed with
//! Signature Version 4.
//!
//! # A blob is never visible unless it is whole and is what its address says
//!
//! [`Blobs::put_stream`] is handed a body that fails at its end unless its SHA-256 and size are
//! right (the upload route's check). A blob that fits one part is sent in one `PutObject` after the
//! body has ended well. A larger one goes up as a multipart upload **to its final key**, and
//! `CompleteMultipartUpload` is sent only after the body has ended well; a failed, refused or
//! abandoned upload is aborted instead. S3 shows nothing under a key until a multipart upload of it
//! is completed, and completes atomically, so a reader sees the old object, or the whole new one,
//! or none. Two uploads of one key have upload ids of their own: a wrong one aborts without
//! touching a right one.
//!
//! The other way to do it (upload to a temporary key, check, then copy to the address and delete)
//! was not taken: S3 has no rename, so it costs a second full write of every blob, leaves a window
//! in which a crash strands a whole temporary object where lifecycle rules for incomplete uploads
//! do not reach, and `CopyObject` is the operation the compatible stores differ on most.
//!
//! An upload whose process dies is not aborted by anyone: its parts stay, invisible but billed,
//! until the bucket's lifecycle rule for incomplete multipart uploads removes them. Every bucket
//! this server uses should have one (a day is plenty).
//!
//! # Memory
//!
//! An upload holds at most `(parts_at_once + 2)` parts ([`S3Tuning`]): the first part, kept back
//! until the length that goes in its header is known, the part being filled, and those on their
//! way. A download holds what one read hands on (64 KiB) and the HTTP client's buffers. Removing
//! a prefix holds one page of the listing (1,000 keys).
//!
//! # Errors
//!
//! Only the store's own "there is no such object" (404) is "absent"; even then the bucket must
//! have answered something recently, or is asked now, since a bucket that is gone also answers 404.
//! Everything else (unreachable, timed out, 403, 5xx once the retries are spent) is an error.
//! Error text and `Debug` never hold the credentials.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail, ensure};
use async_compression::tokio::write::ZstdEncoder;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::stream::{self, StreamExt};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path as ObjectPath;
use object_store::{
    BackoffConfig, ClientOptions, Error as StoreError, GetResult, ObjectStore, PutPayload,
    RetryConfig,
};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;

use super::{
    Blobs, ByteStream, HEADER_LEN, PutError, PutOutcome, blob_header, check_key, decode_blob,
    fail_when_stalled,
};
use crate::config::{Settings, Storage};

/// The smallest part S3 takes in a multipart upload (but for the last).
pub const MIN_PART: usize = 5 << 20;
/// How many keys one `DeleteObjects` request names, and one page of a listing holds.
const DELETE_BATCH: usize = 1000;
/// Where the probes of [`Blobs::health`] and [`S3Blobs::verify`] look and write. No key can name
/// anything under it ([`check_key`] refuses a component that starts with `.`).
const PROBE: &str = ".oswx-probe";
/// How long an answer from the bucket vouches for the bucket being there.
const BUCKET_SEEN: Duration = Duration::from_secs(30);

/// The timeouts, retries and sizes of [`S3Blobs`]. `Default` gives what a server runs with.
#[derive(Debug, Clone, PartialEq)]
pub struct S3Tuning {
    /// How long opening a connection to the store may take.
    pub connect_timeout: Duration,
    /// How long one attempt of one request may take from start to finish. It covers every request
    /// but the streamed reads: HEAD, DELETE, a page of a listing, a batch of deletes, a whole
    /// value of at most one part, one part of a multipart upload, and starting, completing and
    /// aborting one.
    pub request_timeout: Duration,
    /// How long [`Blobs::exists`] may take in all, retries included.
    pub lookup_timeout: Duration,
    /// How long a read may wait for the store to start answering.
    pub first_byte_timeout: Duration,
    /// How long a read may go without a byte from the store before it is given up on.
    pub idle_timeout: Duration,
    /// How many times a request that failed in a way worth trying again (a connection error, a
    /// timeout, 5xx, 429) is sent again. Every request this storage makes is safe to repeat.
    pub max_retries: usize,
    /// No retry starts later than this after the first attempt.
    pub retry_timeout: Duration,
    /// The wait before the first retry; it doubles each time, up to `backoff_max`.
    pub backoff_init: Duration,
    pub backoff_max: Duration,
    /// The size of every part of a multipart upload but the last; a blob or value of at most this
    /// goes in one request. At least [`MIN_PART`] on a real store.
    pub part_size: usize,
    /// How many parts of one upload are on their way at once.
    pub parts_at_once: usize,
}

impl Default for S3Tuning {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(60),
            lookup_timeout: Duration::from_secs(5),
            first_byte_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_timeout: Duration::from_secs(60),
            backoff_init: Duration::from_millis(100),
            backoff_max: Duration::from_secs(5),
            part_size: 8 << 20,
            parts_at_once: 3,
        }
    }
}

/// Which bucket [`S3Blobs`] uses, and how it reaches it.
#[derive(Clone, PartialEq)]
pub struct S3Config {
    pub bucket: String,
    /// What every key is put under in the bucket: empty, or a key ([`check_key`]) with no
    /// leading or trailing `/`.
    pub prefix: String,
    /// The store's address (`https://host[:port]`, or `http://` for a store on a private network).
    /// Empty: AWS S3 in `region`.
    pub endpoint: String,
    pub region: String,
    /// Empty (with an empty secret): the credentials of the machine's own AWS role.
    pub access_key_id: String,
    pub secret_access_key: String,
    /// For temporary credentials; else empty.
    pub session_token: String,
    /// The bucket in the path (`endpoint/bucket/key`) rather than the host name
    /// (`bucket.endpoint/key`).
    pub path_style: bool,
    pub tuning: S3Tuning,
}

impl S3Config {
    /// The bucket the settings name, or `None` when they name a folder. The bucket goes in the
    /// path exactly when an endpoint is set, unless `s3_path_style` says otherwise.
    pub fn from_settings(settings: &Settings) -> anyhow::Result<Option<Self>> {
        let Storage::S3 { bucket, prefix } = settings.storage()? else {
            return Ok(None);
        };
        let endpoint = settings.s3_endpoint.trim().trim_end_matches('/').to_owned();
        Ok(Some(Self {
            bucket,
            prefix,
            path_style: settings.s3_path_style.unwrap_or(!endpoint.is_empty()),
            endpoint,
            region: settings.s3_region.clone(),
            access_key_id: settings.s3_access_key_id.clone(),
            secret_access_key: settings.s3_secret_access_key.clone(),
            session_token: settings.s3_session_token.clone(),
            tuning: S3Tuning::default(),
        }))
    }
}

/// The credentials are never printed.
impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn secret(s: &str) -> &'static str {
            if s.is_empty() { "" } else { "<redacted>" }
        }
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key_id", &secret(&self.access_key_id))
            .field("secret_access_key", &secret(&self.secret_access_key))
            .field("session_token", &secret(&self.session_token))
            .field("path_style", &self.path_style)
            .field("tuning", &self.tuning)
            .finish()
    }
}

/// The credentials, to keep them out of error text: a store may repeat the key id in its answer.
struct Secrets(Vec<String>);

impl Secrets {
    fn scrub(&self, mut text: String) -> String {
        for secret in &self.0 {
            if text.contains(secret.as_str()) {
                text = text.replace(secret.as_str(), "<redacted>");
            }
        }
        text
    }

    /// What went wrong with `key`, in words that are safe to log.
    fn error(&self, doing: &str, key: &str, err: &StoreError) -> anyhow::Error {
        anyhow!("{doing} {key}: {}", self.scrub(err.to_string()))
    }
}

/// Storage in a bucket of an S3-compatible store. See the [module](self) for what it promises.
pub struct S3Blobs {
    /// For every request with a deadline of its own (`request_timeout` per attempt).
    quick: Arc<AmazonS3>,
    /// For streamed reads, which may take as long as their reader does: no deadline per request,
    /// but `first_byte_timeout` and `idle_timeout`.
    streams: AmazonS3,
    bucket: String,
    prefix: String,
    endpoint: String,
    tuning: S3Tuning,
    secrets: Arc<Secrets>,
    started: Instant,
    /// Milliseconds after `started` at which the bucket last answered something that only a bucket
    /// that exists answers, plus one (zero: never).
    bucket_seen: AtomicU64,
    /// The most bytes of one upload held at once (for the streaming tests).
    peak_buffered: AtomicUsize,
}

impl fmt::Debug for S3Blobs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Blobs")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl S3Blobs {
    /// Storage in the bucket `config` names. Nothing is asked of the store yet: see
    /// [`S3Blobs::verify`].
    pub fn new(config: S3Config) -> anyhow::Result<Self> {
        ensure!(!config.bucket.is_empty(), "the S3 bucket has no name");
        if !config.prefix.is_empty() {
            check_key(&config.prefix).context("the prefix in the S3 storage URL")?;
        }
        ensure!(
            config.access_key_id.is_empty() == config.secret_access_key.is_empty(),
            "set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither"
        );
        let tuning = config.tuning.clone();
        ensure!(
            tuning.part_size > HEADER_LEN && tuning.parts_at_once > 0,
            "the S3 part size and the number of parts at once must be above zero"
        );
        let secrets = Arc::new(Secrets(
            [
                &config.secret_access_key,
                &config.session_token,
                &config.access_key_id,
            ]
            .into_iter()
            .filter(|s| !s.is_empty())
            .cloned()
            .collect(),
        ));
        let quick = Self::client(&config, Some(tuning.request_timeout))
            .map_err(|e| anyhow!("the S3 settings: {}", secrets.scrub(e.to_string())))?;
        let streams = Self::client(&config, None)
            .map_err(|e| anyhow!("the S3 settings: {}", secrets.scrub(e.to_string())))?;
        Ok(Self {
            quick: Arc::new(quick),
            streams,
            bucket: config.bucket,
            prefix: config.prefix,
            endpoint: config.endpoint,
            tuning,
            secrets,
            started: Instant::now(),
            bucket_seen: AtomicU64::new(0),
            peak_buffered: AtomicUsize::new(0),
        })
    }

    /// [`S3Blobs::new`], then [`S3Blobs::verify`]: what a server does when it starts.
    pub async fn open(config: S3Config) -> anyhow::Result<Self> {
        let blobs = Self::new(config)?;
        blobs.verify().await?;
        Ok(blobs)
    }

    fn client(config: &S3Config, timeout: Option<Duration>) -> object_store::Result<AmazonS3> {
        let tuning = &config.tuning;
        let mut options = ClientOptions::new().with_connect_timeout(tuning.connect_timeout);
        options = match timeout {
            Some(timeout) => options.with_timeout(timeout),
            None => options.with_timeout_disabled(),
        };
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_virtual_hosted_style_request(!config.path_style)
            .with_retry(RetryConfig {
                backoff: BackoffConfig {
                    init_backoff: tuning.backoff_init,
                    max_backoff: tuning.backoff_max,
                    base: 2.0,
                },
                max_retries: tuning.max_retries,
                retry_timeout: tuning.retry_timeout,
            });
        if !config.endpoint.is_empty() {
            // Plain HTTP only where the operator wrote it (a store on a private network).
            options = options.with_allow_http(config.endpoint.starts_with("http://"));
            let endpoint = if config.path_style {
                config.endpoint.clone()
            } else {
                bucket_in_host(&config.endpoint, &config.bucket)
            };
            builder = builder.with_endpoint(endpoint);
        }
        if !config.access_key_id.is_empty() {
            builder = builder
                .with_access_key_id(&config.access_key_id)
                .with_secret_access_key(&config.secret_access_key);
            if !config.session_token.is_empty() {
                builder = builder.with_token(&config.session_token);
            }
        }
        builder.with_client_options(options).build()
    }

    /// Fails unless this server can write, read, remove and list in the bucket (under its prefix,
    /// at a key no blob or snapshot can have) and is told "no such object" for one that is not
    /// there (S3 answers 403 instead to a key that may not list the bucket, which would make every
    /// missing blob look like an outage). A server does this once when it starts, so that a wrong
    /// bucket, key or permission is found then and not at the first upload.
    pub async fn verify(&self) -> anyhow::Result<()> {
        let key = format!("{PROBE}/{}", uuid::Uuid::new_v4().simple());
        let path = self.object_path(&key)?;
        let wrong = |doing: &str, e: StoreError| {
            self.secrets
                .error(doing, &key, &e)
                .context(format!("the S3 bucket {} cannot be used", self.bucket))
        };
        self.quick
            .put(&path, PutPayload::from_static(b"ok"))
            .await
            .map_err(|e| wrong("writing", e))?;
        let read = async { self.quick.get(&path).await?.bytes().await };
        let back = read.await.map_err(|e| wrong("reading", e))?;
        ensure!(
            back.as_ref() == b"ok",
            "the S3 bucket {} answered something other than what was written to it",
            self.bucket
        );
        self.quick
            .delete(&path)
            .await
            .map_err(|e| wrong("removing", e))?;
        self.probe()
            .await
            .with_context(|| format!("the S3 bucket {} cannot be listed", self.bucket))?;
        match self.quick.head(&path).await {
            // (`Ok`: a store that still shows what was just removed. Nothing to object to.)
            Ok(_) | Err(StoreError::NotFound { .. }) => Ok(()),
            Err(e) => Err(wrong("looking for (it is not there)", e).context(
                "the S3 store does not say that an object is missing (on AWS the key needs \
                 s3:ListBucket for that)",
            )),
        }
    }

    /// The most bytes any one [`Blobs::put_stream`] call has held at once: a test hook for the
    /// promise that uploads are streamed.
    #[doc(hidden)]
    pub fn peak_buffered(&self) -> usize {
        self.peak_buffered.load(Ordering::Relaxed)
    }

    /// Every key under this storage's prefix, in the store's order: a test hook. It reads the
    /// whole listing.
    #[doc(hidden)]
    pub async fn keys(&self) -> anyhow::Result<Vec<String>> {
        let (within, strip) = if self.prefix.is_empty() {
            (None, String::new())
        } else {
            (
                Some(ObjectPath::parse(&self.prefix)?),
                format!("{}/", self.prefix),
            )
        };
        let mut listing = self.quick.list(within.as_ref());
        let mut keys = Vec::new();
        while let Some(found) = listing.next().await {
            let found = found.map_err(|e| self.secrets.error("listing", &self.bucket, &e))?;
            let key = found.location.as_ref();
            keys.push(key.strip_prefix(&strip).unwrap_or(key).to_owned());
        }
        Ok(keys)
    }

    /// The object path of a key that has been checked.
    fn path(&self, key: &str) -> anyhow::Result<ObjectPath> {
        check_key(key)?;
        self.object_path(key)
    }

    fn object_path(&self, key: &str) -> anyhow::Result<ObjectPath> {
        let full = if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.prefix)
        };
        Ok(ObjectPath::parse(full)?)
    }

    fn saw_bucket(&self) {
        let now = self.started.elapsed().as_millis() as u64 + 1;
        self.bucket_seen.fetch_max(now, Ordering::Relaxed);
    }

    /// Asks the bucket something only a bucket that exists and takes this server's credentials
    /// answers: a listing of the probe prefix (which is empty).
    async fn probe(&self) -> anyhow::Result<()> {
        let at = self.object_path(PROBE)?;
        let mut listing = self.quick.list(Some(&at));
        match listing.next().await {
            None | Some(Ok(_)) => {
                self.saw_bucket();
                Ok(())
            }
            Some(Err(StoreError::NotFound { .. })) => {
                bail!("the S3 bucket {} does not exist", self.bucket)
            }
            // (A bucket that does not exist comes here too: the store's client keeps "not found"
            // for objects.)
            Some(Err(e)) => Err(self
                .secrets
                .error("reaching the S3 bucket", &self.bucket, &e)),
        }
    }

    /// Called when the store answered 404 for an object: succeeds if that means "no such object".
    /// A bucket that does not exist answers 404 too, so unless the bucket answered something else
    /// in the last [`BUCKET_SEEN`], it is asked now; a bucket that is gone is an error.
    async fn really_absent(&self) -> anyhow::Result<()> {
        let seen = self.bucket_seen.load(Ordering::Relaxed);
        let now = self.started.elapsed().as_millis() as u64 + 1;
        if seen != 0 && now.saturating_sub(seen) <= BUCKET_SEEN.as_millis() as u64 {
            return Ok(());
        }
        let wait = self.tuning.first_byte_timeout;
        match tokio::time::timeout(wait, self.probe()).await {
            Ok(answer) => answer,
            Err(_) => bail!(
                "the S3 bucket {} did not answer within {wait:?}",
                self.bucket
            ),
        }
    }

    /// Starts reading the object at `path`; `None` when there is none.
    async fn open_read(&self, key: &str, path: &ObjectPath) -> anyhow::Result<Option<ByteStream>> {
        let wait = self.tuning.first_byte_timeout;
        let result: GetResult = match tokio::time::timeout(wait, self.streams.get(path)).await {
            Err(_) => bail!("reading {key}: the store did not answer within {wait:?}"),
            Ok(Err(StoreError::NotFound { .. })) => {
                self.really_absent().await?;
                return Ok(None);
            }
            Ok(Err(e)) => return Err(self.secrets.error("reading", key, &e)),
            Ok(Ok(result)) => result,
        };
        self.saw_bucket();
        let secrets = self.secrets.clone();
        let chunks = result
            .into_stream()
            .map(move |chunk| {
                chunk.map_err(|e| std::io::Error::other(secrets.scrub(e.to_string())))
            })
            .boxed();
        Ok(Some(fail_when_stalled(chunks, self.tuning.idle_timeout)))
    }

    /// Stores `bytes` at `path` in one request or, past one part, as a multipart upload.
    async fn put_whole(&self, key: &str, path: &ObjectPath, bytes: Bytes) -> anyhow::Result<()> {
        let part = self.tuning.part_size;
        let failed = |e: StoreError| self.secrets.error("storing", key, &e);
        if bytes.len() <= part {
            self.quick
                .put(path, PutPayload::from(bytes))
                .await
                .map_err(failed)?;
        } else {
            let mut upload = Upload::start(self, path).await.map_err(failed)?;
            for (index, start) in (0..bytes.len()).step_by(part).enumerate() {
                let end = bytes.len().min(start + part);
                upload
                    .send(index, bytes.slice(start..end))
                    .await
                    .map_err(failed)?;
            }
            upload.finish().await.map_err(failed)?;
        }
        self.saw_bucket();
        Ok(())
    }

    /// Removes `batch` (and empties it); answers how many could not be removed, and why the first
    /// of them could not. (One request names the whole batch: when the request itself fails, the
    /// store's client reports that once, and none of the batch is known to be removed.)
    async fn remove_batch(&self, batch: &mut Vec<ObjectPath>) -> (usize, Option<String>) {
        let asked = batch.len();
        let paths = stream::iter(std::mem::take(batch).into_iter().map(Ok)).boxed();
        let mut results = self.quick.delete_stream(paths);
        let (mut removed, mut first) = (0, None);
        while let Some(result) = results.next().await {
            match result {
                Ok(_) | Err(StoreError::NotFound { .. }) => removed += 1,
                Err(e) => {
                    first.get_or_insert_with(|| self.secrets.scrub(e.to_string()));
                }
            }
        }
        (asked.saturating_sub(removed), first)
    }
}

/// `scheme://host[:port]` with `bucket.` put before the host.
fn bucket_in_host(endpoint: &str, bucket: &str) -> String {
    match endpoint.split_once("://") {
        Some((scheme, rest)) => format!("{scheme}://{bucket}.{rest}"),
        None => format!("{bucket}.{endpoint}"),
    }
}

/// A multipart upload under way. Dropping it before [`Upload::finish`] succeeded stops the parts
/// still on their way and aborts the upload at the store (in a task of its own, since `Drop` cannot
/// wait), so that nothing of it stays behind; nothing was ever visible under the key.
struct Upload {
    store: Arc<AmazonS3>,
    path: ObjectPath,
    id: String,
    parts_at_once: usize,
    abort_timeout: Duration,
    in_flight: JoinSet<object_store::Result<(usize, PartId)>>,
    done: Vec<Option<PartId>>,
    secrets: Arc<Secrets>,
    completed: bool,
}

impl Upload {
    async fn start(blobs: &S3Blobs, path: &ObjectPath) -> object_store::Result<Self> {
        let id = blobs.quick.create_multipart(path).await?;
        Ok(Self {
            store: blobs.quick.clone(),
            path: path.clone(),
            id,
            parts_at_once: blobs.tuning.parts_at_once,
            abort_timeout: blobs.tuning.retry_timeout + blobs.tuning.request_timeout,
            in_flight: JoinSet::new(),
            done: Vec::new(),
            secrets: blobs.secrets.clone(),
            completed: false,
        })
    }

    /// How many parts are on their way.
    fn on_their_way(&self) -> usize {
        self.in_flight.len()
    }

    /// Sends part `index` (from 0, in any order), first waiting until fewer than `parts_at_once`
    /// are on their way. An error is that of a part sent earlier, or of this one's wait.
    async fn send(&mut self, index: usize, data: Bytes) -> object_store::Result<()> {
        while self.in_flight.len() >= self.parts_at_once {
            self.reap().await?;
        }
        let (store, path, id) = (self.store.clone(), self.path.clone(), self.id.clone());
        self.in_flight.spawn(async move {
            let part = store
                .put_part(&path, &id, index, PutPayload::from(data))
                .await?;
            Ok((index, part))
        });
        Ok(())
    }

    /// Waits for one part on its way to arrive.
    async fn reap(&mut self) -> object_store::Result<()> {
        if let Some(joined) = self.in_flight.join_next().await {
            let (index, part) = joined??;
            if self.done.len() <= index {
                self.done.resize_with(index + 1, || None);
            }
            self.done[index] = Some(part);
        }
        Ok(())
    }

    /// Waits for every part, then completes the upload: only now is the object visible.
    async fn finish(mut self) -> object_store::Result<()> {
        while !self.in_flight.is_empty() {
            self.reap().await?;
        }
        let parts: Option<Vec<PartId>> = std::mem::take(&mut self.done).into_iter().collect();
        let parts = parts.ok_or_else(|| StoreError::Generic {
            store: "S3",
            source: "a part of the upload was never sent".into(),
        })?;
        self.store
            .complete_multipart(&self.path, &self.id, parts)
            .await?;
        self.completed = true;
        Ok(())
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut in_flight = std::mem::take(&mut self.in_flight);
        let (store, path, id) = (self.store.clone(), self.path.clone(), self.id.clone());
        let (secrets, wait) = (self.secrets.clone(), self.abort_timeout);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!("a multipart upload of {path} was left for the bucket's lifecycle rule");
            return;
        };
        runtime.spawn(async move {
            // The parts on their way are stopped first: a part that lands after the abort would
            // stay behind.
            in_flight.shutdown().await;
            let aborted = tokio::time::timeout(wait, store.abort_multipart(&path, &id)).await;
            let why = match aborted {
                Ok(Ok(())) | Ok(Err(StoreError::NotFound { .. })) => return,
                Ok(Err(e)) => secrets.scrub(e.to_string()),
                Err(_) => format!("no answer within {wait:?}"),
            };
            tracing::warn!(
                "could not abort a multipart upload of {path} (the bucket's lifecycle rule for \
                 incomplete uploads will remove its parts): {why}"
            );
        });
    }
}

#[async_trait]
impl Blobs for S3Blobs {
    async fn put_stream(
        &self,
        key: &str,
        mut body: ByteStream,
        limit: u64,
    ) -> anyhow::Result<PutOutcome> {
        let path = self.path(key)?;
        let part_size = self.tuning.part_size;
        let failed = |e: StoreError| self.secrets.error("storing", key, &e);
        // The compressed bytes gather here, after room for the header; whole parts are cut off the
        // front as they fill.
        let mut encoder = ZstdEncoder::new(vec![0u8; HEADER_LEN]);
        let mut hasher = Sha256::new();
        let mut raw_len = 0u64;
        // The first part is kept back: the header in it holds the length, known at the end.
        let mut first: Option<Vec<u8>> = None;
        let mut upload: Option<Upload> = None;
        let mut next_index = 1;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(PutError::Body)?;
            raw_len += chunk.len() as u64;
            if raw_len > limit {
                return Err(PutError::TooLarge.into());
            }
            hasher.update(&chunk);
            encoder.write_all(&chunk).await?;
            while encoder.get_ref().len() >= part_size {
                let buffer = encoder.get_mut();
                let rest = buffer.split_off(part_size);
                let part = std::mem::replace(buffer, rest);
                match &mut upload {
                    None => {
                        upload = Some(Upload::start(self, &path).await.map_err(failed)?);
                        first = Some(part);
                    }
                    Some(upload) => {
                        upload
                            .send(next_index, Bytes::from(part))
                            .await
                            .map_err(failed)?;
                        next_index += 1;
                    }
                }
            }
            let held = encoder.get_ref().len()
                + first.as_ref().map_or(0, Vec::len)
                + upload.as_ref().map_or(0, Upload::on_their_way) * part_size;
            self.peak_buffered.fetch_max(held, Ordering::Relaxed);
        }
        drop(body);
        encoder.shutdown().await?;
        let mut last = encoder.into_inner();
        // The body ended well (its digest and size are right, or it would have failed above): only
        // now does anything become visible under the key.
        match (upload, first) {
            (Some(mut upload), Some(mut first)) => {
                first[..HEADER_LEN].copy_from_slice(&blob_header(raw_len));
                upload.send(0, Bytes::from(first)).await.map_err(failed)?;
                if !last.is_empty() {
                    upload
                        .send(next_index, Bytes::from(last))
                        .await
                        .map_err(failed)?;
                }
                upload.finish().await.map_err(failed)?;
            }
            _ => {
                last[..HEADER_LEN].copy_from_slice(&blob_header(raw_len));
                self.quick
                    .put(&path, PutPayload::from(Bytes::from(last)))
                    .await
                    .map_err(failed)?;
            }
        }
        self.saw_bucket();
        Ok(PutOutcome {
            raw_len,
            sha256: hex::encode(hasher.finalize()),
        })
    }

    async fn get_stream(&self, key: &str) -> anyhow::Result<Option<(u64, ByteStream)>> {
        let path = self.path(key)?;
        let Some(stored) = self.open_read(key, &path).await? else {
            return Ok(None);
        };
        Ok(Some(decode_blob(StreamReader::new(stored), key).await?))
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let path = self.path(key)?;
        let wait = self.tuning.lookup_timeout;
        let lookup = async {
            match self.quick.head(&path).await {
                Ok(_) => {
                    self.saw_bucket();
                    Ok(true)
                }
                Err(StoreError::NotFound { .. }) => {
                    self.really_absent().await?;
                    Ok(false)
                }
                Err(e) => Err(self.secrets.error("looking for", key, &e)),
            }
        };
        match tokio::time::timeout(wait, lookup).await {
            Ok(answer) => answer,
            Err(_) => bail!("looking for {key}: the store did not answer within {wait:?}"),
        }
    }

    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        let path = self.path(key)?;
        self.put_whole(key, &path, bytes).await
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        let path = self.path(key)?;
        let Some(mut chunks) = self.open_read(key, &path).await? else {
            return Ok(None);
        };
        let mut value = BytesMut::new();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.with_context(|| format!("reading {key}"))?;
            value.extend_from_slice(&chunk);
        }
        Ok(Some(value.freeze()))
    }

    async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        let Some(folder) = prefix.strip_suffix('/') else {
            bail!("a prefix to delete must end with /: {prefix:?}");
        };
        let within = self.path(folder)?;
        // The listing is asked for `<folder>/` (the store's client adds the `/`); every key it
        // answers is checked again, so that nothing outside the prefix can be removed whatever
        // the store answers.
        let inside = format!("{within}/");
        let mut listing = self.quick.list(Some(&within));
        let mut batch = Vec::with_capacity(DELETE_BATCH);
        let (mut left, mut why) = (0usize, None);
        let mut note = |(failed, first): (usize, Option<String>)| {
            left += failed;
            why = why.take().or(first);
        };
        let mut listing_failed = None;
        while let Some(found) = listing.next().await {
            let found = match found {
                Ok(found) => found,
                Err(e) => {
                    listing_failed = Some(self.secrets.error("listing", prefix, &e));
                    break;
                }
            };
            ensure!(
                found.location.as_ref().starts_with(&inside),
                "the store listed {} under {prefix}",
                found.location
            );
            batch.push(found.location);
            if batch.len() == DELETE_BATCH {
                note(self.remove_batch(&mut batch).await);
            }
        }
        if !batch.is_empty() {
            note(self.remove_batch(&mut batch).await);
        }
        if let Some(e) = listing_failed {
            return Err(e.context(format!(
                "removing {prefix} (what was listed before is removed)"
            )));
        }
        if left > 0 {
            bail!(
                "removing {prefix}: {left} objects could not be removed; the first: {}",
                why.unwrap_or_default()
            );
        }
        self.saw_bucket();
        Ok(())
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let path = self.path(key)?;
        match self.quick.delete(&path).await {
            Ok(()) | Err(StoreError::NotFound { .. }) => Ok(()),
            Err(e) => Err(self.secrets.error("removing", key, &e)),
        }
    }

    async fn health(&self) -> anyhow::Result<()> {
        self.probe().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> S3Config {
        S3Config {
            bucket: "collab".into(),
            prefix: "prod".into(),
            endpoint: "http://127.0.0.1:9".into(),
            region: "garage".into(),
            access_key_id: "GK-the-key-id".into(),
            secret_access_key: "the-secret-key".into(),
            session_token: "the-session-token".into(),
            path_style: true,
            tuning: S3Tuning::default(),
        }
    }

    #[test]
    fn the_bucket_goes_before_the_host_for_virtual_hosting() {
        assert_eq!(
            bucket_in_host("https://s3.example.com:9000", "collab"),
            "https://collab.s3.example.com:9000"
        );
        assert_eq!(bucket_in_host("s3.example.com", "b"), "b.s3.example.com");
    }

    #[test]
    fn debug_and_errors_never_show_the_credentials() {
        let config = config();
        let blobs = S3Blobs::new(config.clone()).unwrap();
        let shown = format!("{config:?} {blobs:?}");
        for secret in ["GK-the-key-id", "the-secret-key", "the-session-token"] {
            assert!(!shown.contains(secret), "{secret} in {shown}");
        }
        assert!(shown.contains("collab") && shown.contains("http://127.0.0.1:9"));

        let said = StoreError::Generic {
            store: "S3",
            source: "<AWSAccessKeyId>GK-the-key-id</AWSAccessKeyId> the-secret-key".into(),
        };
        let text = format!("{:#}", blobs.secrets.error("reading", "a/b", &said));
        assert!(
            text.contains("reading a/b") && text.contains("<redacted>"),
            "{text}"
        );
        assert!(
            !text.contains("GK-the-key-id") && !text.contains("the-secret-key"),
            "{text}"
        );
    }

    #[test]
    fn settings_choose_the_bucket_and_how_it_is_addressed() {
        let folder = Settings::default();
        assert_eq!(S3Config::from_settings(&folder).unwrap(), None);

        let aws = Settings {
            storage_url: "s3://collab/some/prefix/".into(),
            ..Settings::default()
        };
        let config = S3Config::from_settings(&aws).unwrap().unwrap();
        assert_eq!(
            (config.bucket.as_str(), config.prefix.as_str()),
            ("collab", "some/prefix")
        );
        assert_eq!(config.region, "us-east-1");
        assert!(
            config.endpoint.is_empty() && !config.path_style,
            "AWS: bucket in the host"
        );

        let garage = Settings {
            s3_endpoint: "http://garage:3900/".into(),
            ..aws.clone()
        };
        let config = S3Config::from_settings(&garage).unwrap().unwrap();
        assert_eq!(config.endpoint, "http://garage:3900");
        assert!(config.path_style, "an endpoint: bucket in the path");

        let r2 = Settings {
            s3_path_style: Some(false),
            ..garage
        };
        assert!(!S3Config::from_settings(&r2).unwrap().unwrap().path_style);
    }

    #[test]
    fn half_a_credential_and_a_bad_prefix_are_refused() {
        let half = S3Config {
            secret_access_key: String::new(),
            ..config()
        };
        let err = S3Blobs::new(half).unwrap_err().to_string();
        assert!(err.contains("AWS_SECRET_ACCESS_KEY"), "{err}");
        assert!(!err.contains("GK-the-key-id"), "{err}");

        let bad = S3Config {
            prefix: "../up".into(),
            ..config()
        };
        assert!(S3Blobs::new(bad).is_err());
        let none = S3Config {
            prefix: String::new(),
            ..config()
        };
        assert!(S3Blobs::new(none).is_ok());
    }
}
