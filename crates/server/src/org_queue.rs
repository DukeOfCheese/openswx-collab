//! One write at a time per organisation, queued in this process.
//!
//! Every request that changes something in an organisation locks that organisation's row first
//! (the lock order of [`crate::store`]), so such requests run one after another anyway. Waiting
//! for that lock in the database costs a connection per waiter; a handful of requests queued on
//! one busy organisation would hold every connection the server has for requests. So they queue
//! here instead, **before** they open a transaction: a request takes its organisation's turn
//! ([`OrgQueue::turn`]), and only the one whose turn it is holds a connection.
//!
//! The queue is bounded twice: at most `waiters` requests wait for one organisation (one more
//! is answered at once), and none waits longer than `wait`. Both answer **503**
//! [`crate::error::BUSY`]: nothing was done, and the request can be sent again.
//!
//! An organisation has an entry only while a request holds or waits for its turn.
//!
//! The turn is held from before `begin` until right after the transaction commits: what follows
//! the commit (revoking connections, announcing, extension hooks) runs without it, so one
//! organisation's write rate does not depend on how long those take.
//!
//! This is per process, and so is not what keeps the data right: that is the row lock, which a
//! request still takes. (A request whose client went away gives its turn up while its commit may
//! still be running; the next one then waits for the row lock, briefly, as before.)

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{ApiError, BUSY};
use crate::store::Id;

/// How many requests may wait for one organisation's turn, unless the queue is made otherwise.
pub const WAITERS: usize = 64;
/// How long a request waits for its organisation's turn, unless the queue is made otherwise.
pub const WAIT: Duration = Duration::from_secs(10);

/// The queues, one per organisation with a request in it. Cloning is cheap: clones are the same.
#[derive(Clone)]
pub struct OrgQueue {
    inner: Arc<Inner>,
}

struct Inner {
    waiters: usize,
    wait: Duration,
    /// Per organisation: its turn, and how many requests hold or wait for it.
    orgs: Mutex<HashMap<Id, (Arc<Semaphore>, usize)>>,
}

impl std::fmt::Debug for OrgQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrgQueue")
            .field("organisations", &self.organisations())
            .finish_non_exhaustive()
    }
}

impl Default for OrgQueue {
    fn default() -> Self {
        Self::new(WAITERS, WAIT)
    }
}

/// An organisation's turn: while this lives, no other request has it. Dropping it passes the turn
/// on.
#[derive(Debug)]
pub struct OrgTurn {
    // (Dropped in this order: the turn is passed on, then the entry is left.)
    _permit: OwnedSemaphorePermit,
    _place: Place,
}

/// A request's place in an organisation's entry, given up when dropped.
#[derive(Debug)]
struct Place {
    queue: OrgQueue,
    org: Id,
}

impl Drop for Place {
    fn drop(&mut self) {
        let mut orgs = self.queue.orgs();
        if let Some((_, users)) = orgs.get_mut(&self.org) {
            *users -= 1;
            if *users == 0 {
                orgs.remove(&self.org);
            }
        }
    }
}

impl OrgQueue {
    /// Queues in which at most `waiters` requests wait for one organisation, each for at most
    /// `wait`.
    pub fn new(waiters: usize, wait: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                waiters,
                wait,
                orgs: Mutex::default(),
            }),
        }
    }

    fn orgs(&self) -> std::sync::MutexGuard<'_, HashMap<Id, (Arc<Semaphore>, usize)>> {
        self.inner
            .orgs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// How many organisations have a request holding or waiting for their turn.
    pub fn organisations(&self) -> usize {
        self.orgs().len()
    }

    /// How many requests wait for a turn, over every organisation (not counting those that have
    /// one).
    pub fn waiters(&self) -> usize {
        self.orgs()
            .values()
            .map(|(_, users)| users.saturating_sub(1))
            .sum()
    }

    /// How many requests wait for the turn of `org` (not counting the one that has it).
    pub fn waiting(&self, org: &str) -> usize {
        self.orgs()
            .get(org)
            .map_or(0, |(_, users)| users.saturating_sub(1))
    }

    /// Waits for the turn of `org` and takes it. 503 at once when as many requests as the queue
    /// allows already wait for it, and after the queue's wait when the turn did not come.
    pub async fn turn(&self, org: &str) -> Result<OrgTurn, ApiError> {
        let (turn, place) = {
            let mut orgs = self.orgs();
            let (turn, users) = orgs
                .entry(org.to_owned())
                .or_insert_with(|| (Arc::new(Semaphore::new(1)), 0));
            // (One has the turn; the rest wait.)
            if *users > self.inner.waiters {
                crate::metrics::org_queue_refused();
                return Err(ApiError::Unavailable(BUSY));
            }
            *users += 1;
            let place = Place {
                queue: self.clone(),
                org: org.to_owned(),
            };
            (turn.clone(), place)
        };
        match tokio::time::timeout(self.inner.wait, turn.acquire_owned()).await {
            Ok(Ok(permit)) => Ok(OrgTurn {
                _permit: permit,
                _place: place,
            }),
            // (The semaphore is never closed; a wait that ran out gives its place up.)
            _ => {
                crate::metrics::org_queue_refused();
                Err(ApiError::Unavailable(BUSY))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn one_at_a_time_per_organisation_in_order_and_others_meanwhile() {
        let queue = OrgQueue::new(8, Duration::from_secs(5));
        let first = queue.turn("a").await.unwrap();
        // Another organisation does not wait.
        let other = queue.turn("b").await.unwrap();
        assert_eq!(queue.organisations(), 2);
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for n in 0..3 {
            let (waiter, order) = (queue.clone(), order.clone());
            tasks.push(tokio::spawn(async move {
                let _turn = waiter.turn("a").await.unwrap();
                order.lock().unwrap().push(n);
                tokio::task::yield_now().await;
            }));
            // (So that they queue in this order.)
            while queue.waiting("a") <= n {
                tokio::task::yield_now().await;
            }
        }
        assert!(order.lock().unwrap().is_empty(), "nobody has the turn yet");
        drop(first);
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), [0, 1, 2]);
        drop(other);
        assert_eq!(queue.organisations(), 0, "no entry is kept for an idle one");
    }

    #[tokio::test]
    async fn one_waiter_too_many_is_refused_at_once_and_a_long_wait_ends() {
        let queue = OrgQueue::new(2, Duration::from_millis(200));
        let held = queue.turn("a").await.unwrap();
        let waiting: Vec<_> = (0..2)
            .map(|_| {
                let queue = queue.clone();
                tokio::spawn(async move { queue.turn("a").await.map(drop) })
            })
            .collect();
        while queue.waiting("a") < 2 {
            tokio::task::yield_now().await;
        }
        // The third waiter is told at once.
        let started = std::time::Instant::now();
        let refused = queue.turn("a").await.unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(
            refused.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(refused.detail(), BUSY);
        // The two that wait give up after the queue's wait, with the same answer.
        for task in waiting {
            let gave_up = task.await.unwrap().unwrap_err();
            assert_eq!(
                gave_up.status(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            );
        }
        assert_eq!(queue.waiting("a"), 0);
        drop(held);
        assert_eq!(queue.organisations(), 0);
        // And the turn is free again.
        drop(queue.turn("a").await.unwrap());
    }
}
