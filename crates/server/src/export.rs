//! How an export is streamed: its limits, the pipe between the task that writes it and the response
//! that reads it, and the zip written into that pipe. What goes into an export is in
//! [`crate::routes::lifecycle`].
//!
//! An export is written by a task of its own into a small pipe (`download`). The task holds a
//! place (`Place`): one of `max_exports` ([`EXPORTS_AT_ONCE`] by default) of its kind on the
//! server (organisation exports and account exports have a pool each, so that accounts exporting
//! themselves cannot keep an owner from exporting the organisation), and the only one of its
//! account. It is given up when its client reads nothing for [`EXPORT_STALL`], when its client
//! reads more slowly than the minimum rate on average ([`Pace`], the floor and grace of uploads:
//! `min_upload_rate`, `upload_rate_grace`), or when it has run for [`EXPORT_LIMIT`]; and stopped
//! when its response goes away.

use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use async_zip::tokio::write::{EntryStreamWriter, ZipFileWriter};
use async_zip::{Compression, ZipDateTime, ZipDateTimeBuilder, ZipEntryBuilder};
use axum::body::Body;
use chrono::{Datelike, Timelike};
use futures_util::stream::{self, StreamExt};
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter, DuplexStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};
use tokio_util::io::ReaderStream;

use crate::blobs::Pace;
use crate::error::ApiError;
use crate::store::Id;

/// How many exports of each kind run at once on a server unless [`Exports`] says otherwise
/// (`OSWX_COLLAB_MAX_EXPORTS`).
pub const EXPORTS_AT_ONCE: usize = 2;
/// How long an export waits for its client to read before it gives up.
pub const EXPORT_STALL: Duration = Duration::from_secs(60);
/// How long an export may take in all.
pub const EXPORT_LIMIT: Duration = Duration::from_secs(30 * 60);
/// How many rows an export reads from the database at a time.
pub const EXPORT_PAGE_ROWS: i64 = 500;
/// The size of the pipe between an export and its response, and of the buffer before it.
const PIPE_BYTES: usize = 64 << 10;

const EXPORTS_BUSY: &str = "too many exports are running; try again in a moment";
const EXPORT_RUNNING: &str = "you have an export running already; try again when it has finished";

/// What is being exported: each kind has its own pool of places.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportKind {
    /// An organisation (`GET /api/orgs/{org}/export`).
    Org,
    /// An account (`GET /api/me/export`).
    Account,
}

/// The limits of the exports ([`crate::AppState::exports`]).
#[derive(Debug)]
pub struct Exports {
    orgs: Arc<Semaphore>,
    account_exports: Arc<Semaphore>,
    at_once: usize,
    /// The accounts that have an export running.
    accounts: Mutex<HashSet<Id>>,
    stall: Duration,
    limit: Duration,
    pace: Pace,
    page_rows: i64,
}

impl Exports {
    /// At most `at_once` exports of each kind at a time on the server and one per account, each
    /// given up when its client reads nothing for `stall` or when it has run for `limit`, reading
    /// at most `page_rows` rows from the database at a time. No minimum rate
    /// ([`Exports::with_pace`]).
    pub fn new(at_once: usize, stall: Duration, limit: Duration, page_rows: i64) -> Self {
        Self {
            orgs: Arc::new(Semaphore::new(at_once)),
            account_exports: Arc::new(Semaphore::new(at_once)),
            at_once,
            accounts: Mutex::default(),
            stall,
            limit,
            pace: Pace {
                floor: 0,
                grace: stall,
            },
            page_rows: page_rows.max(1),
        }
    }

    /// As the settings say: `max_exports` of each kind, the minimum rate of uploads
    /// ([`Pace::from_settings`]), and the default stall and time limits.
    pub fn from_settings(settings: &crate::Settings) -> Self {
        Self::new(
            settings.max_exports,
            EXPORT_STALL,
            EXPORT_LIMIT,
            EXPORT_PAGE_ROWS,
        )
        .with_pace(Pace::from_settings(settings))
    }

    /// Gives up an export whose client reads more slowly than `pace` on average: only the time
    /// the export waits for its client to take what it wrote counts (see [`Pace`]).
    pub fn with_pace(mut self, pace: Pace) -> Self {
        self.pace = pace;
        self
    }

    fn pool(&self, kind: ExportKind) -> &Arc<Semaphore> {
        match kind {
            ExportKind::Org => &self.orgs,
            ExportKind::Account => &self.account_exports,
        }
    }

    /// How many exports are running, of both kinds.
    pub fn running(&self) -> usize {
        self.running_of(ExportKind::Org) + self.running_of(ExportKind::Account)
    }

    /// How many exports of `kind` are running.
    pub fn running_of(&self, kind: ExportKind) -> usize {
        self.at_once
            .saturating_sub(self.pool(kind).available_permits())
    }

    /// How many rows an export reads from the database at a time.
    pub(crate) fn page_rows(&self) -> i64 {
        self.page_rows
    }

    /// A place among the running exports of `kind` for an export by `account`; 429 when that
    /// account has one running (of either kind), or the server has as many of `kind` as it runs
    /// at once.
    pub(crate) fn place(
        self: &Arc<Self>,
        account: &Id,
        kind: ExportKind,
    ) -> Result<Place, ApiError> {
        let mut accounts = self.accounts.lock().unwrap_or_else(PoisonError::into_inner);
        if accounts.contains(account) {
            return Err(ApiError::Busy(EXPORT_RUNNING));
        }
        let permit = self
            .pool(kind)
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::Busy(EXPORTS_BUSY))?;
        accounts.insert(account.clone());
        Ok(Place {
            exports: self.clone(),
            account: account.clone(),
            _permit: permit,
        })
    }
}

impl Default for Exports {
    fn default() -> Self {
        Self::new(
            EXPORTS_AT_ONCE,
            EXPORT_STALL,
            EXPORT_LIMIT,
            EXPORT_PAGE_ROWS,
        )
    }
}

/// A running export's place ([`Exports::place`]); dropping it gives the place back.
#[derive(Debug)]
pub(crate) struct Place {
    exports: Arc<Exports>,
    account: Id,
    _permit: OwnedSemaphorePermit,
}

impl Drop for Place {
    fn drop(&mut self) {
        self.exports
            .accounts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.account);
    }
}

// ------------------------------------------------------------------ the zip being written

/// An entry of the zip being written.
pub(crate) type Entry<'a> = Compat<EntryStreamWriter<'a, Pipe>>;

/// A zip written entry after entry into an export's pipe. Every entry is deflated and streamed
/// (its sizes follow its data, in ZIP64 form), so neither an entry nor the archive has a size
/// limit and nothing is held but the entry's current chunk.
pub(crate) struct Archive {
    zip: ZipFileWriter<Pipe>,
    /// When the export was made: the date of every entry.
    date: ZipDateTime,
}

impl Archive {
    pub(crate) fn new(pipe: Pipe, now: f64) -> Self {
        Self {
            zip: ZipFileWriter::with_tokio(pipe),
            date: zip_date(now),
        }
    }

    /// Starts the entry `name`; write its content, then [`close_entry`] it.
    pub(crate) async fn entry(&mut self, name: String) -> anyhow::Result<Entry<'_>> {
        let entry = ZipEntryBuilder::new(name.into(), Compression::Deflate)
            .last_modification_date(self.date);
        Ok(self.zip.write_entry_stream(entry).await?.compat_write())
    }

    /// Writes the entry `name` with `content`.
    pub(crate) async fn put(&mut self, name: String, content: &[u8]) -> anyhow::Result<()> {
        let mut entry = self.entry(name).await?;
        entry.write_all(content).await?;
        close_entry(entry).await
    }

    /// Writes the directory and ends the pipe.
    pub(crate) async fn finish(self) -> anyhow::Result<()> {
        let mut pipe = self.zip.close().await?.into_inner();
        pipe.flush().await?;
        pipe.shutdown().await?;
        Ok(())
    }
}

/// Ends an entry: what is left of its data, then its checksum and sizes.
pub(crate) async fn close_entry(entry: Entry<'_>) -> anyhow::Result<()> {
    Ok(entry.into_inner().close().await?)
}

/// The zip date of `now` (epoch seconds, UTC); the zip epoch for a time a zip cannot say.
fn zip_date(now: f64) -> ZipDateTime {
    // (Whole seconds; a time out of range is no time.)
    let utc = chrono::DateTime::from_timestamp(now as i64, 0);
    match utc.filter(|t| (1980..=2107).contains(&t.year())) {
        Some(t) => ZipDateTimeBuilder::new()
            .year(t.year())
            .month(t.month())
            .day(t.day())
            .hour(t.hour())
            .minute(t.minute())
            .second(t.second())
            .build(),
        None => ZipDateTime::default(),
    }
}

// ------------------------------------------------------------------ the pipe to the response

/// What a running export writes into: a buffer, then the pipe its response body reads from.
pub(crate) type Pipe = BufWriter<Patient<DuplexStream>>;

/// The client read nothing for as long as an export waits.
#[derive(Debug, thiserror::Error)]
#[error("the client stopped reading")]
struct Stalled;

/// The client read more slowly than the minimum rate.
#[derive(Debug, thiserror::Error)]
#[error("the client read too slowly")]
struct ReadTooSlowly;

/// A writer that gives up when whoever reads the other end has stopped, or reads too slowly
/// (each as an `io::Error` of kind `TimedOut`): [`Stalled`] when one write has waited `patience`
/// for room; [`ReadTooSlowly`] when the writes have waited for room longer than `pace` allows
/// (the allowance starts at its grace; waiting uses it up, every byte taken earns `1 / floor`
/// seconds back).
pub(crate) struct Patient<W> {
    inner: W,
    patience: Duration,
    pace: Pace,
    /// What is left of the allowance ([`Pace::after`]).
    allowance: Duration,
    /// Running while a write waits for room: since when, until when it may wait, and whether that
    /// limit is the allowance's.
    waiting: Option<Waiting>,
}

struct Waiting {
    since: tokio::time::Instant,
    until: Pin<Box<tokio::time::Sleep>>,
    by_pace: bool,
}

impl<W> Patient<W> {
    pub(crate) fn new(inner: W, patience: Duration, pace: Pace) -> Self {
        Self {
            inner,
            patience,
            pace,
            allowance: pace.grace,
            waiting: None,
        }
    }

    /// A write begins to wait: for `patience`, or less when the allowance is shorter.
    fn start_waiting(&self) -> Waiting {
        let by_pace = self.pace.floor > 0 && self.allowance < self.patience;
        let limit = if by_pace {
            self.allowance
        } else {
            self.patience
        };
        Waiting {
            since: tokio::time::Instant::now(),
            until: Box::pin(tokio::time::sleep(limit)),
            by_pace,
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for Patient<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(done) => {
                let waited = this
                    .waiting
                    .take()
                    .map_or(Duration::ZERO, |w| w.since.elapsed());
                if let Ok(n) = &done {
                    this.allowance = this.pace.after(this.allowance, waited, *n);
                }
                Poll::Ready(done)
            }
            Poll::Pending => {
                if this.waiting.is_none() {
                    this.waiting = Some(this.start_waiting());
                }
                let waiting = this.waiting.as_mut().expect("just set");
                match waiting.until.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        let by_pace = waiting.by_pace;
                        this.waiting = None;
                        let e = if by_pace {
                            io::Error::new(io::ErrorKind::TimedOut, ReadTooSlowly)
                        } else {
                            io::Error::new(io::ErrorKind::TimedOut, Stalled)
                        };
                        Poll::Ready(Err(e))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// An export's task; dropping this (the response went away) stops it.
struct Running(JoinHandle<bool>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The body of a streamed download: `produce` runs in a task of its own, holding `place`, and
/// writes into a pipe of [`PIPE_BYTES`] that the body reads, so it never runs ahead of the client
/// by more than the pipe and its buffer. It must end the pipe (`shutdown`) when it is done. If it
/// fails (logged here, as `what` `id`), stops, or takes longer than the exports' time limit, the
/// body ends with an error, which closes the connection without the end of the response. When
/// the response is dropped, the task is stopped.
pub(crate) fn download<F, Fut>(
    exports: &Exports,
    place: Place,
    what: &'static str,
    id: Id,
    produce: F,
) -> Body
where
    F: FnOnce(Pipe) -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let (into, from) = tokio::io::duplex(PIPE_BYTES);
    let patient = Patient::new(into, exports.stall, exports.pace);
    let pipe = BufWriter::with_capacity(PIPE_BYTES, patient);
    let limit = exports.limit;
    let task = tokio::spawn(async move {
        // (Declared first, dropped last: the place is free once everything else is let go.)
        let _place = place;
        match tokio::time::timeout(limit, produce(pipe)).await {
            Ok(Ok(())) => true,
            Ok(Err(e)) => {
                log_failure(what, &id, &e);
                false
            }
            Err(_) => {
                tracing::warn!(
                    "the export of {what} {id} was given up: it took longer than {limit:?}"
                );
                false
            }
        }
    });
    let chunks = ReaderStream::with_capacity(from, PIPE_BYTES);
    Body::from_stream(stream::unfold(
        Some((chunks, Running(task))),
        |reading| async move {
            let (mut chunks, mut running) = reading?;
            match chunks.next().await {
                Some(Ok(chunk)) => Some((Ok(chunk), Some((chunks, running)))),
                Some(Err(e)) => Some((Err(e), None)),
                // The pipe ended: whole only if the task says so.
                None => match (&mut running.0).await {
                    Ok(true) => None,
                    _ => Some((Err(io::Error::other("the export was cut short")), None)),
                },
            }
        },
    ))
}

/// Logs why an export stopped: a client that went away is no error, one that stopped reading a
/// warning, anything else an error. Only the id is named, never a name or an address.
fn log_failure(what: &str, id: &str, e: &anyhow::Error) {
    let io = e.chain().find_map(|c| c.downcast_ref::<io::Error>());
    let inner = io.and_then(|e| e.get_ref());
    if inner.is_some_and(|inner| inner.is::<Stalled>()) {
        tracing::warn!("the export of {what} {id} was given up: its client stopped reading");
    } else if inner.is_some_and(|inner| inner.is::<ReadTooSlowly>()) {
        tracing::warn!(
            "the export of {what} {id} was given up: its client read more slowly than the \
             minimum rate"
        );
    } else if io.is_some_and(|e| e.kind() == io::ErrorKind::BrokenPipe) {
        tracing::debug!("the export of {what} {id} ended early: its client went away");
    } else {
        tracing::error!("the export of {what} {id} failed: {e:#}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_settings_say_how_many_exports_of_each_kind_run_at_once() {
        let settings = crate::Settings {
            max_exports: 3,
            ..crate::Settings::default()
        };
        let exports = Arc::new(Exports::from_settings(&settings));
        for kind in [ExportKind::Org, ExportKind::Account] {
            let places: Vec<_> = ["a", "b", "c"]
                .map(|who| exports.place(&format!("{who}-{kind:?}"), kind).unwrap())
                .into();
            assert_eq!(exports.running_of(kind), 3);
            let refused = exports.place(&format!("d-{kind:?}"), kind).unwrap_err();
            assert_eq!(refused.detail(), EXPORTS_BUSY);
            drop(places);
        }
    }

    #[test]
    fn a_place_is_one_per_account_and_so_many_of_each_kind_per_server() {
        use ExportKind::{Account, Org};
        let exports = Arc::new(Exports::new(2, EXPORT_STALL, EXPORT_LIMIT, 1));
        let [a, b, c, d, e] = ["a", "b", "c", "d", "e"].map(str::to_owned);
        let first = exports.place(&a, Org).unwrap();
        assert_eq!(exports.place(&a, Org).unwrap_err().detail(), EXPORT_RUNNING);
        assert_eq!(
            exports.place(&a, Account).unwrap_err().detail(),
            EXPORT_RUNNING
        );
        assert_eq!(exports.running(), 1, "a refusal takes no place");
        let second = exports.place(&b, Org).unwrap();
        assert_eq!(exports.place(&c, Org).unwrap_err().detail(), EXPORTS_BUSY);
        // Accounts exporting themselves have places of their own, and cannot take the others'.
        let mine = exports.place(&c, Account).unwrap();
        let yours = exports.place(&d, Account).unwrap();
        assert_eq!(
            exports.place(&e, Account).unwrap_err().detail(),
            EXPORTS_BUSY
        );
        assert_eq!(
            (exports.running_of(Org), exports.running_of(Account)),
            (2, 2)
        );
        drop((mine, yours));
        assert_eq!(exports.running_of(Org), 2);
        // (A refusal for the server's sake does not mark the account as exporting.)
        drop(first);
        let third = exports.place(&e, Org).unwrap();
        assert_eq!(exports.running(), 2);
        drop((second, third));
        assert_eq!(exports.running(), 0);
        exports.place(&a, Org).unwrap();
    }

    #[test]
    fn a_zip_date_is_the_time_in_utc_or_the_zip_epoch() {
        // 2026-11-26 12:34:56 UTC.
        let at = zip_date(1_795_696_496.0);
        assert_eq!(
            (
                at.year(),
                at.month(),
                at.day(),
                at.hour(),
                at.minute(),
                at.second()
            ),
            (2026, 11, 26, 12, 34, 56)
        );
        for odd in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e18] {
            let at = zip_date(odd);
            assert_eq!((at.year(), at.month(), at.day()), (1980, 1, 1), "{odd}");
        }
    }

    /// No minimum rate.
    fn no_floor() -> Pace {
        Pace {
            floor: 0,
            grace: Duration::from_secs(30),
        }
    }

    #[tokio::test]
    async fn a_writer_nobody_reads_from_gives_up_and_one_that_is_read_does_not() {
        use tokio::io::AsyncReadExt;
        let (into, mut from) = tokio::io::duplex(16);
        // A patience fifty times the reader's pause: far above the timer's granularity (about
        // 16 ms on Windows).
        let mut patient = Patient::new(into, Duration::from_secs(1), no_floor());
        // Slow, but read: each write waits less than the patience.
        let reader = tokio::spawn(async move {
            let mut got = Vec::new();
            let mut buf = [0u8; 16];
            for _ in 0..4 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let n = from.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
            }
            (from, got)
        });
        patient.write_all(&[7u8; 64]).await.unwrap();
        let (from, got) = reader.await.unwrap();
        assert_eq!(got, [7u8; 64]);
        // Not read at all: the first write fills the pipe, the next gives up.
        patient.write_all(&[8u8; 16]).await.unwrap();
        let e = patient.write_all(&[9u8; 16]).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
        assert!(e.get_ref().unwrap().is::<Stalled>());
        drop(from);
    }

    /// Reads `from` 1 KiB at a time, pausing `pause` before each read, to its end.
    async fn read_every(mut from: DuplexStream, pause: Duration) {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 1024];
        loop {
            tokio::time::sleep(pause).await;
            if from.read(&mut buf).await.unwrap_or(0) == 0 {
                return;
            }
        }
    }

    #[tokio::test]
    async fn a_writer_read_more_slowly_than_the_floor_gives_up_and_one_read_fast_enough_does_not() {
        // 16 KiB a second, with a second of grace; the patience far longer.
        let pace = Pace {
            floor: 16 << 10,
            grace: Duration::from_secs(1),
        };
        // 1 KiB every 10 ms (about 100 KiB a second): kept, however long it takes.
        let (into, from) = tokio::io::duplex(1024);
        let mut fast = Patient::new(into, Duration::from_secs(60), pace);
        let reader = tokio::spawn(read_every(from, Duration::from_millis(10)));
        fast.write_all(&vec![1u8; 160 << 10]).await.unwrap();
        drop(fast);
        reader.await.unwrap();
        // 1 KiB every 250 ms (4 KiB a second): given up once the grace is used up, long before
        // the patience runs out.
        let (into, from) = tokio::io::duplex(1024);
        let mut slow = Patient::new(into, Duration::from_secs(60), pace);
        let reader = tokio::spawn(read_every(from, Duration::from_millis(250)));
        let started = tokio::time::Instant::now();
        let e = slow.write_all(&vec![2u8; 64 << 10]).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
        assert!(e.get_ref().unwrap().is::<ReadTooSlowly>(), "{e}");
        assert!(started.elapsed() < Duration::from_secs(30));
        drop(slow);
        reader.await.unwrap();
    }
}
