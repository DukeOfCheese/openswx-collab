//! Removing what was deleted, in the background.
//!
//! Deleting a project or an organisation is two steps. The request does the first, in one short
//! transaction: it **marks** the project (or the organisation and its projects) and their
//! documents deleted ([`crate::store::Tx::mark_project_deleted`],
//! [`crate::store::Tx::mark_org_deleted`]). From that commit they are gone for everyone: no query
//! finds them, their rooms are closed, their names are free, and nothing more is written for such
//! a document: the journal refuses it as [`crate::journal::RefusedWhy::NoDocument`], and so do a
//! lease acquisition and a snapshot. What is still in the database and in storage is only space.
//!
//! The [`Purger`] gives that space back: one task a server, which looks for marked projects and
//! organisations ([`crate::store::Store::deleted_projects`], `deleted_orgs`) and removes what
//! hangs on them **a chunk at a time, each chunk a short transaction of its own**
//! ([`CHUNK_ROWS`] rows): a document's journal rows, snapshot rows, versions and references, then
//! its row; a project's folders, then its row; last the organisation's row. No transaction of it
//! removes more than a chunk, so other writers get in between the chunks whatever the size of
//! what was deleted (`tests/deletion.rs` watches journal appends land between them).
//!
//! # Order, and starting again
//!
//! The marks are the work list, and they are in the database: a server that stops in the middle
//! finds them again when it starts ([`crate::app`] wakes the purger) and goes on. For that to
//! leave nothing behind, every step can be done twice and a row goes only after what it points at:
//!
//! 1. a document's stored snapshots (`docs/<document>/`) are removed **before** its rows, and an
//!    organisation's blobs (`orgs/<org>/`) before its row. The commit that makes this safe is the
//!    one that marked them, which the purger knows happened (it read the mark): storage is removed
//!    only after a known commit, never on the strength of a request that may have failed;
//! 2. then the rows, children first, each statement removing only what belongs to something
//!    marked.
//!
//! # When a step fails
//!
//! A document, project or organisation whose removal fails (the database busy, the storage
//! refusing its prefix) is left as it is, marked and invisible, and **the rest goes on**: one
//! thing that cannot be removed holds up only itself (and what must go after it: its project, its
//! organisation). It is tried again after [`RETRY`], then after twice that, and so on up to
//! [`MAX_RETRY`]; each failure is logged once, so a permanent one is a warning an hour, not a
//! stream of them.
//!
//! # With the journal
//!
//! A chunk removes journal rows of a document the journal no longer writes to (it refuses marked
//! documents, and the mark waited for any batch that had the document's row locked). The one
//! document-row lock the purger takes is the single-row delete at the end, in a transaction that
//! holds nothing else: level 4 of the lock order of [`crate::store`], alone.
//!
//! A single document deleted on its own is only marked, as it always was: its row and snapshots
//! stay (assemblies may still show what they used, and an undo may bring a reference back).

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::blobs::Blobs;
use crate::room::Gate;
use crate::store::Store;

/// The most rows one purge transaction removes.
pub const CHUNK_ROWS: i64 = 1000;
/// How long a document, project or organisation whose removal failed waits for its next try the
/// first time; twice as long after every further failure.
pub const RETRY: Duration = Duration::from_secs(30);
/// The longest wait between two tries of one thing.
pub const MAX_RETRY: Duration = Duration::from_secs(3600);
/// How many marked projects, organisations or documents are listed at a time.
const LIST: i64 = 100;

/// The background task that removes what was marked deleted (see the module documentation).
/// Cloning is cheap: clones are the same purger.
#[derive(Clone)]
pub struct Purger {
    inner: Arc<Inner>,
}

struct Inner {
    chunk_rows: i64,
    retry: Duration,
    /// Whether the task runs.
    busy: watch::Sender<bool>,
    /// Something was marked since the running sweep looked.
    again: AtomicBool,
    stopped: AtomicBool,
    /// Wakes a task that waits for a next try.
    nudge: Notify,
    task: Mutex<Option<JoinHandle<()>>>,
    /// What failed and waits for its next try, by what it is (`document <id>`, …).
    waiting: Mutex<HashMap<String, Waiting>>,
    gate: Gate,
    chunks: AtomicUsize,
}

/// A thing whose removal failed.
#[derive(Debug, Clone, Copy)]
struct Waiting {
    failures: u32,
    /// When it is tried again.
    next: Instant,
}

impl std::fmt::Debug for Purger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Purger")
            .field("busy", &self.busy())
            .field("waiting", &self.waiting())
            .finish_non_exhaustive()
    }
}

impl Default for Purger {
    fn default() -> Self {
        Self::new(CHUNK_ROWS, RETRY)
    }
}

/// The purger was told to stop in the middle of a sweep.
#[derive(Debug, thiserror::Error)]
#[error("the purger was stopped")]
struct Stopped;

/// Clears the busy flag when the task ends, however it ends (a panic, an abort).
struct Done(Arc<Inner>);

impl Drop for Done {
    fn drop(&mut self) {
        self.0.busy.send_replace(false);
    }
}

impl Purger {
    /// A purger that removes `chunk_rows` rows a transaction (at least 1) and tries what failed
    /// again after `retry` (then twice that, and so on). It does nothing until [`Purger::wake`].
    pub fn new(chunk_rows: i64, retry: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                chunk_rows: chunk_rows.max(1),
                retry,
                busy: watch::Sender::new(false),
                again: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                nudge: Notify::new(),
                task: Mutex::default(),
                waiting: Mutex::default(),
                gate: Gate::default(),
                chunks: AtomicUsize::new(0),
            }),
        }
    }

    /// Whether the task is running: there is, or may be, something left to remove (something
    /// that failed and waits for its next try included).
    pub fn busy(&self) -> bool {
        *self.inner.busy.borrow()
    }

    /// Waits until the task has nothing left to do (or was stopped).
    pub async fn idle(&self) {
        let mut busy = self.inner.busy.subscribe();
        // (The sender lives as long as `self`: this cannot fail.)
        let _ = busy.wait_for(|busy| !busy).await;
    }

    /// How many rows one of its transactions removes at most.
    pub fn chunk_rows(&self) -> i64 {
        self.inner.chunk_rows
    }

    /// How many documents, projects and organisations failed and wait for their next try.
    pub fn waiting(&self) -> usize {
        self.lock_waiting().len()
    }

    /// For tests: shut, the task parks here before every chunk.
    #[doc(hidden)]
    pub fn gate(&self) -> &Gate {
        &self.inner.gate
    }

    /// For tests: how many chunk transactions were committed so far.
    #[doc(hidden)]
    pub fn chunks(&self) -> usize {
        self.inner.chunks.load(Ordering::SeqCst)
    }

    fn lock_waiting(&self) -> std::sync::MutexGuard<'_, HashMap<String, Waiting>> {
        self.inner
            .waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Says that something was marked deleted (or that the server has just started): the task is
    /// started on the current tokio runtime unless it runs already, in which case it looks again
    /// when its sweep ends. Nothing happens after the purger was told to stop.
    pub fn wake(&self, store: Arc<dyn Store>, blobs: Arc<dyn Blobs>) {
        let inner = &self.inner;
        inner.again.store(true, Ordering::SeqCst);
        inner.nudge.notify_one();
        if inner.stopped.load(Ordering::SeqCst) || inner.busy.send_replace(true) {
            return;
        }
        let purger = self.clone();
        let task = tokio::spawn(async move { purger.run(store, blobs).await });
        *inner.task.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
    }

    /// Tells the task to stop at its next step, without waiting for it. What it has not removed
    /// stays marked, for the next start.
    pub fn signal_stop(&self) {
        self.inner.stopped.store(true, Ordering::SeqCst);
        self.inner.nudge.notify_one();
        self.inner.gate.open();
    }

    /// [`Purger::signal_stop`], then waits for the task to end; one that has not ended after
    /// `patience` (it is inside a slow storage call, say) is aborted. Either way nothing is lost:
    /// the marks say what is left.
    pub async fn stop(&self, patience: Duration) {
        self.signal_stop();
        if tokio::time::timeout(patience, self.idle()).await.is_err() {
            let task = self
                .inner
                .task
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(task) = task {
                task.abort();
                let _ = task.await;
            }
            // (An aborted task dropped its `Done`; this is for one that was never started.)
            self.inner.busy.send_replace(false);
        }
    }

    async fn run(self, store: Arc<dyn Store>, blobs: Arc<dyn Blobs>) {
        let inner = &self.inner;
        let done = Done(inner.clone());
        loop {
            inner.again.store(false, Ordering::SeqCst);
            if self.sweep(store.as_ref(), blobs.as_ref()).await.is_err()
                || inner.stopped.load(Ordering::SeqCst)
            {
                return;
            }
            if inner.again.load(Ordering::SeqCst) {
                continue;
            }
            let next = self.lock_waiting().values().map(|w| w.next).min();
            if let Some(next) = next {
                // Something failed: its next try, or a new deletion or a stop, ends the wait.
                tokio::select! {
                    () = tokio::time::sleep_until(next) => {}
                    () = inner.nudge.notified() => {}
                }
                continue;
            }
            // Not busy any more; unless something was marked just now, whose `wake` saw the task
            // still running: then it goes on.
            inner.busy.send_replace(false);
            if !inner.again.load(Ordering::SeqCst) || inner.busy.send_replace(true) {
                // (Nothing came, or the `wake` that came started a task of its own.)
                std::mem::forget(done);
                return;
            }
        }
    }

    /// Before every step: parks at the test gate, and ends the sweep if the purger was stopped.
    async fn step(&self) -> Result<(), Stopped> {
        self.inner.gate.pass().await;
        if self.inner.stopped.load(Ordering::SeqCst) {
            return Err(Stopped);
        }
        Ok(())
    }

    /// Does the removal `work` of the thing called `what`, unless it failed before and its next
    /// try is not due. Answers whether it is removed now. A failure is logged, and the thing
    /// waits (twice as long as the time before) while the sweep goes on with the rest.
    async fn remove<F>(&self, what: &str, work: F) -> Result<bool, Stopped>
    where
        F: Future<Output = anyhow::Result<()>>,
    {
        let failed_before = self.lock_waiting().get(what).copied();
        if failed_before.is_some_and(|w| w.next > Instant::now()) {
            return Ok(false);
        }
        match work.await {
            Ok(()) => {
                self.lock_waiting().remove(what);
                Ok(true)
            }
            Err(e) if e.is::<Stopped>() => Err(Stopped),
            Err(e) => {
                let failures = failed_before.map_or(0, |w| w.failures) + 1;
                let wait = self
                    .inner
                    .retry
                    .saturating_mul(1 << (failures - 1).min(16))
                    .min(MAX_RETRY);
                self.lock_waiting().insert(
                    what.to_owned(),
                    Waiting {
                        failures,
                        next: Instant::now() + wait,
                    },
                );
                tracing::warn!(
                    "the deleted {what} could not be removed (try {failures}); it is tried again \
                     in {wait:?} and the rest goes on meanwhile: {e:#}"
                );
                Ok(false)
            }
        }
    }

    /// Goes once through everything that is marked now.
    async fn sweep(&self, store: &dyn Store, blobs: &dyn Blobs) -> Result<(), Stopped> {
        self.lock_waiting().remove("the list");
        let mut after = String::new();
        loop {
            self.step().await?;
            let Some(projects) = self.list(store.deleted_projects(&after, LIST).await) else {
                break;
            };
            for project in &projects {
                self.project(store, blobs, project).await?;
            }
            match projects.last() {
                Some(last) => after.clone_from(last),
                None => break,
            }
        }
        let mut after = String::new();
        loop {
            self.step().await?;
            let Some(orgs) = self.list(store.deleted_orgs(&after, LIST).await) else {
                break;
            };
            for org in &orgs {
                // (One whose projects wait for a next try waits with them, without a word.)
                let held_up = self.waiting() > 0;
                let what = format!("organisation {org}");
                let removed = self.remove(&what, async {
                    self.step().await?;
                    // Its blobs, then its row (which nothing hangs on any more).
                    blobs.delete_prefix(&format!("orgs/{org}/")).await?;
                    let mut tx = store.begin().await?;
                    let gone = tx.purge_org(org).await?;
                    tx.commit().await?;
                    anyhow::ensure!(gone || held_up, "it still has projects");
                    if gone {
                        tracing::info!(org = %org, "a deleted organisation was removed");
                    }
                    Ok(())
                });
                removed.await?;
            }
            match orgs.last() {
                Some(last) => after.clone_from(last),
                None => break,
            }
        }
        Ok(())
    }

    /// A page of the work list; `None` when the database could not say (logged; the sweep ends
    /// here and is made again at the next wake or try).
    fn list(&self, page: anyhow::Result<Vec<String>>) -> Option<Vec<String>> {
        match page {
            Ok(page) => Some(page),
            Err(e) => {
                tracing::warn!("what was deleted could not be listed: {e:#}");
                // (So that the task looks again: as a thing that failed.)
                let _ = self
                    .lock_waiting()
                    .entry("the list".to_owned())
                    .or_insert(Waiting {
                        failures: 1,
                        next: Instant::now() + self.inner.retry,
                    });
                None
            }
        }
    }

    async fn project(
        &self,
        store: &dyn Store,
        blobs: &dyn Blobs,
        project: &str,
    ) -> Result<(), Stopped> {
        let mut all = true;
        let mut after = String::new();
        loop {
            let Some(documents) = self.list(
                store
                    .documents_of_deleted_project(project, &after, LIST)
                    .await,
            ) else {
                return Ok(());
            };
            for document in &documents {
                all &= self
                    .remove(
                        &format!("document {document}"),
                        self.document(store, blobs, document),
                    )
                    .await?;
            }
            match documents.last() {
                Some(last) => after.clone_from(last),
                None => break,
            }
        }
        if !all {
            // (It goes when its documents have gone: at the sweep their next try makes.)
            return Ok(());
        }
        self.remove(&format!("project {project}"), async {
            loop {
                self.step().await?;
                let mut tx = store.begin().await?;
                let gone = tx
                    .purge_project_rows(project, self.inner.chunk_rows)
                    .await?;
                tx.commit().await?;
                if gone == 0 {
                    break;
                }
                self.inner.chunks.fetch_add(1, Ordering::SeqCst);
            }
            let mut tx = store.begin().await?;
            let gone = tx.purge_project(project).await?;
            tx.commit().await?;
            anyhow::ensure!(gone, "it still has documents");
            tracing::info!(project = %project, "a deleted project was removed");
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn document(
        &self,
        store: &dyn Store,
        blobs: &dyn Blobs,
        document: &str,
    ) -> anyhow::Result<()> {
        self.step().await?;
        // Its snapshots first: were the rows to go first and the server to stop here, nothing
        // would say that these files are still to be removed.
        blobs.delete_prefix(&format!("docs/{document}/")).await?;
        loop {
            self.step().await?;
            let mut tx = store.begin().await?;
            let gone = tx
                .purge_document_rows(document, self.inner.chunk_rows)
                .await?;
            tx.commit().await?;
            if gone == 0 {
                break;
            }
            self.inner.chunks.fetch_add(1, Ordering::SeqCst);
            // (Lets whoever waits for the writer in: on SQLite there is one.)
            tokio::task::yield_now().await;
        }
        self.step().await?;
        let mut tx = store.begin().await?;
        let gone = tx.purge_document(document).await?;
        tx.commit().await?;
        anyhow::ensure!(
            gone,
            "its row was not removed (its project is not marked deleted)"
        );
        Ok(())
    }
}
