//! How many blob uploads run at once ([`crate::AppState::uploads`]), and how fast and how long
//! one may take.
//!
//! An upload holds memory while it runs: about 45 MiB for one to S3 (the parts it gathers before
//! sending them), about 4 MiB for one to a folder (the compressor). So the server's own limit is
//! on the **memory** the running uploads hold between them (`UPLOAD_MEMORY`), each charged what
//! its storage says one holds ([`crate::blobs::Blobs::upload_memory`]): sixteen to S3 at once by
//! default, and many more to a folder, which needs no tight limit. So that no one party can take
//! all of it, only so many run at once from one organisation and from one account (whatever
//! session it uses):
//!
//! | limit | setting (`OSWX_COLLAB_`) | default | one more is answered |
//! |---|---|---|---|
//! | one account | `MAX_BLOB_UPLOADS_PER_USER` | 4 | **429** at once |
//! | one organisation | `MAX_BLOB_UPLOADS_PER_ORG` | 8 | **429** at once |
//! | the server | `UPLOAD_MEMORY` | 720 MiB | **503**, after waiting [`QUEUE_WAIT`] for room |
//!
//! (An organisation's limit above the number of uploads that fit counts as that number.) The
//! account's and the organisation's own limits answer 429: it is their own doing, and they send
//! again when one of their uploads has finished. The server's answers 503: it is busy with other
//! people's. Before that an upload waits a few seconds for room (as many may wait as fit), since
//! room usually comes free quickly; its body is not read meanwhile.
//!
//! A place is held for as long as the upload runs, so an upload must not be able to run for
//! ever. Besides the idle limit (`BLOB_IDLE_TIMEOUT`: no bytes at all), a body must keep up an
//! average rate ([`crate::blobs::Pace`], `MIN_UPLOAD_RATE`), and the whole upload must be done
//! within [`time_allowed`] (`UPLOAD_TIME_LIMIT`, `UPLOAD_TIME_RATE`: ten minutes, or longer for
//! a blob larger than 64 KiB a second fills in that time). So the longest anyone can hold a place
//! is about 68 minutes, with the largest blob.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::ApiError;
use crate::metrics::{self, UploadLimit};
use crate::store::Id;

/// How long an upload waits for room on the server before it is answered 503.
pub const QUEUE_WAIT: Duration = Duration::from_secs(5);

/// The unit the server's memory is counted in.
const KIB: u64 = 1 << 10;

const ACCOUNT_BUSY: &str =
    "you have too many uploads running; send this one again when one of them has finished";
const ORG_BUSY: &str = "your organisation has too many uploads running; send this one again when \
                        one of them has finished";
const SERVER_BUSY: &str = "the server is receiving as many uploads as it takes at once; try again \
                           in a moment";

/// How long an upload of `size` bytes may take in all (from when it has its place to its end):
/// `max(settings.upload_time_limit, size / settings.upload_time_rate)`. `size` is what the
/// request declared, or the largest blob when it declared nothing.
pub fn time_allowed(settings: &crate::Settings, size: u64) -> Duration {
    let least = settings.upload_time_limit;
    let for_size = size as f64 / settings.upload_time_rate.max(1) as f64;
    Duration::from_secs_f64(least.max(for_size))
}

/// The limits of the blob uploads.
#[derive(Debug)]
pub struct Uploads {
    /// The server's room, in KiB.
    room: Arc<Semaphore>,
    room_kib: u64,
    /// What one upload is charged, in KiB (at most `room_kib`).
    cost_kib: u32,
    /// How many uploads fit at once (`room_kib / cost_kib`).
    fit: usize,
    per_org: usize,
    per_account: usize,
    queue_wait: Duration,
    /// How many uploads hold room on the server.
    running: AtomicUsize,
    /// How many uploads wait for room on the server.
    queued: AtomicUsize,
    /// How many uploads each account and each organisation has running or waiting (no entry:
    /// none).
    counts: Mutex<Counts>,
}

#[derive(Debug, Default)]
struct Counts {
    accounts: HashMap<Id, usize>,
    orgs: HashMap<Id, usize>,
}

fn less(counts: &mut HashMap<Id, usize>, id: &Id) {
    match counts.get_mut(id) {
        Some(n) if *n > 1 => *n -= 1,
        _ => {
            counts.remove(id);
        }
    }
}

impl Uploads {
    /// Uploads that hold `held` bytes each, in `memory` bytes in all on the server; at most
    /// `per_org` from one organisation (at most as many as fit) and `per_account` from one
    /// account. At least one of each fits, whatever the numbers. An upload waits
    /// [`QUEUE_WAIT`] for room on the server.
    pub fn new(memory: u64, held: u64, per_org: usize, per_account: usize) -> Self {
        let room_kib = (memory / KIB).clamp(1, u64::from(u32::MAX));
        let cost_kib = held.div_ceil(KIB).clamp(1, room_kib);
        let fit = usize::try_from(room_kib / cost_kib).unwrap_or(usize::MAX);
        Self {
            room: Arc::new(Semaphore::new(
                usize::try_from(room_kib).unwrap_or(usize::MAX),
            )),
            room_kib,
            cost_kib: u32::try_from(cost_kib).unwrap_or(u32::MAX),
            fit,
            per_org: per_org.clamp(1, fit),
            per_account: per_account.max(1),
            queue_wait: QUEUE_WAIT,
            running: AtomicUsize::new(0),
            queued: AtomicUsize::new(0),
            counts: Mutex::default(),
        }
    }

    /// At most `at_once` uploads on the server, whatever they hold (for tests, and for a
    /// storage whose uploads all cost the same).
    pub fn places(at_once: usize, per_org: usize, per_account: usize) -> Self {
        Self::new(at_once.max(1) as u64 * KIB, KIB, per_org, per_account)
    }

    /// The same limits, with another wait for room on the server.
    #[must_use]
    pub fn queue_wait(mut self, wait: Duration) -> Self {
        self.queue_wait = wait;
        self
    }

    /// How many uploads are running (not counting those that wait for room).
    pub fn running(&self) -> usize {
        self.running.load(Ordering::SeqCst)
    }

    /// How many uploads fit on the server at once.
    pub fn fit(&self) -> usize {
        self.fit
    }

    /// The memory the running uploads are charged, in bytes.
    pub fn memory_held(&self) -> u64 {
        let free = self.room.available_permits() as u64;
        self.room_kib.saturating_sub(free) * KIB
    }

    /// How many uploads wait for room on the server.
    pub fn queued(&self) -> usize {
        self.queued.load(Ordering::SeqCst)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Counts> {
        self.counts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A place among the running uploads for one by `account` into `org`, kept until what this
    /// answers is dropped. 429 at once when the account or the organisation has as many as it
    /// may; 503 when the server has no room and none came free in the queue's wait (or as many
    /// wait already as fit).
    pub(crate) async fn place(self: &Arc<Self>, account: &Id, org: &Id) -> Result<Place, ApiError> {
        let counted = {
            let mut counts = self.lock();
            if counts.accounts.get(account).copied().unwrap_or(0) >= self.per_account {
                metrics::upload_refused(UploadLimit::Account);
                return Err(ApiError::Busy(ACCOUNT_BUSY));
            }
            if counts.orgs.get(org).copied().unwrap_or(0) >= self.per_org {
                metrics::upload_refused(UploadLimit::Organisation);
                return Err(ApiError::Busy(ORG_BUSY));
            }
            *counts.accounts.entry(account.clone()).or_default() += 1;
            *counts.orgs.entry(org.clone()).or_default() += 1;
            Counted {
                uploads: self.clone(),
                account: account.clone(),
                org: org.clone(),
            }
        };
        let room = match self.room.clone().try_acquire_many_owned(self.cost_kib) {
            Ok(room) => room,
            Err(_) => self.queue().await?,
        };
        self.running.fetch_add(1, Ordering::SeqCst);
        Ok(Place {
            _room: Room {
                uploads: self.clone(),
                _permit: room,
            },
            _counted: counted,
        })
    }

    /// Waits in the queue for room on the server; 503 when the queue is full or the wait runs
    /// out.
    async fn queue(self: &Arc<Self>) -> Result<OwnedSemaphorePermit, ApiError> {
        let refused = || {
            metrics::upload_refused(UploadLimit::Server);
            ApiError::Unavailable(SERVER_BUSY)
        };
        if self.queued.fetch_add(1, Ordering::SeqCst) >= self.fit {
            self.queued.fetch_sub(1, Ordering::SeqCst);
            return Err(refused());
        }
        let _queued = Queued(self.clone());
        let waiting = self.room.clone().acquire_many_owned(self.cost_kib);
        match tokio::time::timeout(self.queue_wait, waiting).await {
            Ok(Ok(room)) => Ok(room),
            // (The semaphore is never closed.)
            _ => Err(refused()),
        }
    }
}

/// An upload in the queue, until this is dropped.
struct Queued(Arc<Uploads>);

impl Drop for Queued {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::SeqCst);
    }
}

/// An upload counted for its account and its organisation, until this is dropped.
#[derive(Debug)]
struct Counted {
    uploads: Arc<Uploads>,
    account: Id,
    org: Id,
}

impl Drop for Counted {
    fn drop(&mut self) {
        let mut counts = self.uploads.lock();
        less(&mut counts.accounts, &self.account);
        less(&mut counts.orgs, &self.org);
    }
}

/// An upload's room on the server, until this is dropped.
#[derive(Debug)]
struct Room {
    uploads: Arc<Uploads>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for Room {
    fn drop(&mut self) {
        self.uploads.running.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A running upload's place ([`Uploads::place`]); dropping it gives the place back.
#[derive(Debug)]
pub(crate) struct Place {
    // (In this order: the room on the server is free before the counts say so.)
    _room: Room,
    _counted: Counted,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> [Id; 6] {
        ["ada", "bo", "cy", "dee", "org one", "org two"].map(Id::from)
    }

    #[tokio::test]
    async fn so_many_per_account_per_organisation_and_on_the_server() {
        let [ada, bo, cy, dee, one, two] = ids();
        let uploads = Arc::new(Uploads::places(4, 3, 2).queue_wait(Duration::from_millis(50)));
        let first = uploads.place(&ada, &one).await.unwrap();
        let second = uploads.place(&ada, &one).await.unwrap();
        // The account's own limit: 429 at once, in whatever organisation.
        for org in [&one, &two] {
            let refused = uploads.place(&ada, org).await.unwrap_err();
            assert_eq!(
                (refused.status().as_u16(), refused.detail()),
                (429, ACCOUNT_BUSY)
            );
        }
        // The organisation's limit: 429 for a member who has none running.
        let third = uploads.place(&bo, &one).await.unwrap();
        let refused = uploads.place(&cy, &one).await.unwrap_err();
        assert_eq!(
            (refused.status().as_u16(), refused.detail()),
            (429, ORG_BUSY)
        );
        // Another organisation is not held up by it ...
        let fourth = uploads.place(&cy, &two).await.unwrap();
        assert_eq!(uploads.running(), 4);
        // ... until the server has as many as it takes: 503, after the queue's wait.
        let started = std::time::Instant::now();
        let refused = uploads.place(&dee, &two).await.unwrap_err();
        assert_eq!(
            (refused.status().as_u16(), refused.detail()),
            (503, SERVER_BUSY)
        );
        assert!(started.elapsed() >= Duration::from_millis(40));
        assert_eq!((uploads.running(), uploads.queued()), (4, 0));
        drop((first, second, third, fourth));
        assert_eq!((uploads.running(), uploads.memory_held()), (0, 0));
        let counts = uploads.lock();
        assert!(counts.accounts.is_empty() && counts.orgs.is_empty());
    }

    #[tokio::test]
    async fn the_server_counts_the_memory_uploads_hold_not_how_many_there_are() {
        let ids: Vec<Id> = (0..40).map(|i| format!("account {i}")).collect();
        let orgs: Vec<Id> = (0..40).map(|i| format!("org {i}")).collect();
        // 100 MiB of room. Uploads to a bucket hold 45 MiB each: two fit, the third waits and is
        // refused.
        let to_s3 =
            Arc::new(Uploads::new(100 << 20, 45 << 20, 8, 4).queue_wait(Duration::from_millis(20)));
        assert_eq!(to_s3.fit(), 2);
        let held = [
            to_s3.place(&ids[0], &orgs[0]).await.unwrap(),
            to_s3.place(&ids[1], &orgs[1]).await.unwrap(),
        ];
        assert_eq!(to_s3.memory_held(), 90 << 20);
        let refused = to_s3.place(&ids[2], &orgs[2]).await.unwrap_err();
        assert_eq!(refused.status(), 503);
        drop(held);
        // The same room, uploads to a folder holding 4 MiB each: twenty-five run at once.
        let to_folder =
            Arc::new(Uploads::new(100 << 20, 4 << 20, 8, 4).queue_wait(Duration::from_millis(20)));
        assert_eq!(to_folder.fit(), 25);
        let mut held = Vec::new();
        for i in 0..25 {
            held.push(to_folder.place(&ids[i], &orgs[i]).await.unwrap());
        }
        assert_eq!(
            (to_folder.running(), to_folder.memory_held()),
            (25, 100 << 20)
        );
        assert_eq!(
            to_folder
                .place(&ids[30], &orgs[30])
                .await
                .unwrap_err()
                .status(),
            503
        );
        drop(held);
        assert_eq!((to_folder.running(), to_folder.memory_held()), (0, 0));
        // An upload that holds more than all the room still runs, alone.
        let huge = Arc::new(Uploads::new(1 << 20, 1 << 30, 8, 4).queue_wait(Duration::ZERO));
        assert_eq!(huge.fit(), 1);
        let one = huge.place(&ids[0], &orgs[0]).await.unwrap();
        assert_eq!(
            huge.place(&ids[1], &orgs[1]).await.unwrap_err().status(),
            503
        );
        drop(one);
    }

    #[tokio::test]
    async fn an_upload_waits_a_moment_for_a_place_on_the_server() {
        let [ada, bo, cy, dee, one, two] = ids();
        let uploads = Arc::new(Uploads::places(1, 1, 1).queue_wait(Duration::from_secs(20)));
        let held = uploads.place(&ada, &one).await.unwrap();
        let waiting = {
            let uploads = uploads.clone();
            let (bo, two) = (bo.clone(), two.clone());
            tokio::spawn(async move { uploads.place(&bo, &two).await.map(|_| ()) })
        };
        while uploads.queued() < 1 {
            tokio::task::yield_now().await;
        }
        // While it waits it counts for its account and organisation ...
        assert_eq!(uploads.place(&bo, &one).await.unwrap_err().status(), 429);
        assert_eq!(uploads.place(&cy, &two).await.unwrap_err().status(), 429);
        // ... and no more wait than fit: the next is answered at once.
        let started = std::time::Instant::now();
        let [three] = ["org three"].map(Id::from);
        assert_eq!(uploads.place(&dee, &three).await.unwrap_err().status(), 503);
        assert!(started.elapsed() < Duration::from_secs(5));
        // The place comes free: the one that waited has it.
        drop(held);
        waiting.await.unwrap().unwrap();
        assert_eq!((uploads.running(), uploads.queued()), (0, 0));
        // One that gives up waiting (its client went away) leaves nothing behind.
        let held = uploads.place(&ada, &one).await.unwrap();
        let waiting = {
            let uploads = uploads.clone();
            tokio::spawn(async move { uploads.place(&bo, &two).await.map(|_| ()) })
        };
        while uploads.queued() < 1 {
            tokio::task::yield_now().await;
        }
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(uploads.queued(), 0);
        drop(held);
        let counts = uploads.lock();
        assert!(counts.accounts.is_empty() && counts.orgs.is_empty());
    }

    #[tokio::test]
    async fn limits_of_zero_count_as_one_and_an_organisation_has_at_most_what_fits() {
        let [ada, bo, cy, _, one, two] = ids();
        let uploads = Arc::new(Uploads::places(0, 0, 0).queue_wait(Duration::ZERO));
        let held = uploads.place(&ada, &one).await.unwrap();
        assert_eq!(uploads.place(&bo, &two).await.unwrap_err().status(), 503);
        drop(held);
        let uploads = Arc::new(Uploads::places(2, 50, 1).queue_wait(Duration::ZERO));
        let _a = uploads.place(&ada, &one).await.unwrap();
        let _b = uploads.place(&bo, &one).await.unwrap();
        assert_eq!(uploads.place(&cy, &one).await.unwrap_err().status(), 429);
    }

    #[test]
    fn an_upload_has_ten_minutes_or_what_its_size_needs_at_64_kib_a_second() {
        let settings = crate::Settings::default();
        assert_eq!(time_allowed(&settings, 0), Duration::from_secs(600));
        assert_eq!(time_allowed(&settings, 1 << 20), Duration::from_secs(600));
        // 256 MiB at 64 KiB a second: 4,096 s.
        assert_eq!(
            time_allowed(&settings, 256 << 20),
            Duration::from_secs(4096)
        );
        let settings = crate::Settings {
            upload_time_limit: 1.5,
            upload_time_rate: 1000,
            ..Default::default()
        };
        assert_eq!(time_allowed(&settings, 1000), Duration::from_secs_f64(1.5));
        assert_eq!(time_allowed(&settings, 3000), Duration::from_secs(3));
    }
}
