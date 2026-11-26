//! How many blob uploads run at once ([`crate::AppState::uploads`]), and how fast one must come.
//!
//! An upload to S3 holds up to about 40 MiB in memory while it runs, so the server takes only so
//! many at once, and so that no one party can take all of those places, only so many from one
//! organisation and from one account (whatever session it uses):
//!
//! | limit | setting (`OSWX_COLLAB_`) | default | one more is answered |
//! |---|---|---|---|
//! | one account | `MAX_BLOB_UPLOADS_PER_USER` | 4 | **429** at once |
//! | one organisation | `MAX_BLOB_UPLOADS_PER_ORG` | 8 | **429** at once |
//! | the server | `MAX_BLOB_UPLOADS` | 16 | **503**, after waiting [`QUEUE_WAIT`] for a place |
//!
//! (An organisation's limit above the server's counts as the server's.) The account's and the
//! organisation's own limits answer 429: it is their own doing, and they send again when one of
//! their uploads has finished. The server's answers 503: it is busy with other people's. Before
//! that an upload waits a few seconds for a place (as many may wait as there are places), since
//! places usually come free quickly; its body is not read meanwhile.
//!
//! A place is held for as long as the upload runs, so an upload must not be able to run for
//! ever on a trickle. Besides the idle limit (`BLOB_IDLE_TIMEOUT`: no bytes at all), a body must
//! keep up an average rate ([`crate::blobs::Pace`], `MIN_UPLOAD_RATE`): a client that sends a
//! byte now and then is given up on, and its place comes free.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::ApiError;
use crate::metrics::{self, UploadLimit};
use crate::store::Id;

/// How long an upload waits for a place on the server before it is answered 503.
pub const QUEUE_WAIT: Duration = Duration::from_secs(5);

const ACCOUNT_BUSY: &str =
    "you have too many uploads running; send this one again when one of them has finished";
const ORG_BUSY: &str = "your organisation has too many uploads running; send this one again when \
                        one of them has finished";
const SERVER_BUSY: &str = "the server is receiving as many uploads as it takes at once; try again \
                           in a moment";

/// The limits of the blob uploads.
#[derive(Debug)]
pub struct Uploads {
    permits: Arc<Semaphore>,
    at_once: usize,
    per_org: usize,
    per_account: usize,
    queue_wait: Duration,
    /// How many uploads wait for a place on the server.
    queued: AtomicUsize,
    /// How many uploads each account and each organisation has running or waiting (no entry:
    /// none).
    running: Mutex<Running>,
}

#[derive(Debug, Default)]
struct Running {
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
    /// At most `at_once` uploads at a time on the server, `per_org` from one organisation (at
    /// most `at_once`) and `per_account` from one account; at least one of each. An upload waits
    /// [`QUEUE_WAIT`] for a place on the server.
    pub fn new(at_once: usize, per_org: usize, per_account: usize) -> Self {
        let at_once = at_once.max(1);
        Self {
            permits: Arc::new(Semaphore::new(at_once)),
            at_once,
            per_org: per_org.clamp(1, at_once),
            per_account: per_account.max(1),
            queue_wait: QUEUE_WAIT,
            queued: AtomicUsize::new(0),
            running: Mutex::default(),
        }
    }

    /// The same limits, with another wait for a place on the server.
    #[must_use]
    pub fn queue_wait(mut self, wait: Duration) -> Self {
        self.queue_wait = wait;
        self
    }

    /// How many uploads are running (not counting those that wait for a place).
    pub fn running(&self) -> usize {
        self.at_once
            .saturating_sub(self.permits.available_permits())
    }

    /// How many uploads wait for a place on the server.
    pub fn queued(&self) -> usize {
        self.queued.load(Ordering::SeqCst)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Running> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A place among the running uploads for one by `account` into `org`, kept until what this
    /// answers is dropped. 429 at once when the account or the organisation has as many as it
    /// may; 503 when the server has and no place came free in the queue's wait (or as many wait
    /// already as there are places).
    pub(crate) async fn place(self: &Arc<Self>, account: &Id, org: &Id) -> Result<Place, ApiError> {
        let counted = {
            let mut running = self.lock();
            if running.accounts.get(account).copied().unwrap_or(0) >= self.per_account {
                metrics::upload_refused(UploadLimit::Account);
                return Err(ApiError::Busy(ACCOUNT_BUSY));
            }
            if running.orgs.get(org).copied().unwrap_or(0) >= self.per_org {
                metrics::upload_refused(UploadLimit::Organisation);
                return Err(ApiError::Busy(ORG_BUSY));
            }
            *running.accounts.entry(account.clone()).or_default() += 1;
            *running.orgs.entry(org.clone()).or_default() += 1;
            Counted {
                uploads: self.clone(),
                account: account.clone(),
                org: org.clone(),
            }
        };
        let permit = match self.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => self.queue().await?,
        };
        Ok(Place {
            _permit: permit,
            _counted: counted,
        })
    }

    /// Waits in the queue for a place on the server; 503 when the queue is full or the wait
    /// runs out.
    async fn queue(self: &Arc<Self>) -> Result<OwnedSemaphorePermit, ApiError> {
        let refused = || {
            metrics::upload_refused(UploadLimit::Server);
            ApiError::Unavailable(SERVER_BUSY)
        };
        if self.queued.fetch_add(1, Ordering::SeqCst) >= self.at_once {
            self.queued.fetch_sub(1, Ordering::SeqCst);
            return Err(refused());
        }
        let _queued = Queued(self.clone());
        match tokio::time::timeout(self.queue_wait, self.permits.clone().acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
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
        let mut running = self.uploads.lock();
        less(&mut running.accounts, &self.account);
        less(&mut running.orgs, &self.org);
    }
}

/// A running upload's place ([`Uploads::place`]); dropping it gives the place back.
#[derive(Debug)]
pub(crate) struct Place {
    // (In this order: the place on the server is free before the counts say so.)
    _permit: OwnedSemaphorePermit,
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
        let uploads = Arc::new(Uploads::new(4, 3, 2).queue_wait(Duration::from_millis(50)));
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
        assert_eq!(uploads.running(), 0);
        let running = uploads.lock();
        assert!(running.accounts.is_empty() && running.orgs.is_empty());
    }

    #[tokio::test]
    async fn an_upload_waits_a_moment_for_a_place_on_the_server() {
        let [ada, bo, cy, dee, one, two] = ids();
        let uploads = Arc::new(Uploads::new(1, 1, 1).queue_wait(Duration::from_secs(20)));
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
        // ... and no more wait than there are places: the next is answered at once.
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
        let running = uploads.lock();
        assert!(running.accounts.is_empty() && running.orgs.is_empty());
    }

    #[tokio::test]
    async fn limits_of_zero_count_as_one_and_an_organisation_has_at_most_the_servers() {
        let [ada, bo, cy, _, one, two] = ids();
        let uploads = Arc::new(Uploads::new(0, 0, 0).queue_wait(Duration::ZERO));
        let held = uploads.place(&ada, &one).await.unwrap();
        assert_eq!(uploads.place(&bo, &two).await.unwrap_err().status(), 503);
        drop(held);
        let uploads = Arc::new(Uploads::new(2, 50, 1).queue_wait(Duration::ZERO));
        let _a = uploads.place(&ada, &one).await.unwrap();
        let _b = uploads.place(&bo, &one).await.unwrap();
        assert_eq!(uploads.place(&cy, &one).await.unwrap_err().status(), 429);
    }
}
