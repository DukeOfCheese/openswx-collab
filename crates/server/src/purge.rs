//! Removing what was deleted, in the background.
//!
//! Deleting a project or an organisation is two steps. The request does the first, in one short
//! transaction: it **marks** the project (or the organisation and its projects) and their
//! documents deleted ([`crate::store::Tx::mark_project_deleted`],
//! [`crate::store::Tx::mark_org_deleted`]). From that commit they are gone for everyone: no query
//! finds them, their rooms are closed, their names are free, and the journal refuses a write to
//! such a document as [`crate::journal::RefusedWhy::NoDocument`]. What is still in the database and
//! in storage is only space.
//!
//! The [`Purger`] gives that space back: one task a server, which looks for marked projects and
//! organisations ([`crate::store::Store::deleted_projects`], `deleted_orgs`) and removes what
//! hangs on them **a chunk at a time, each chunk a short transaction of its own**
//! ([`CHUNK_ROWS`] rows): a document's journal rows, snapshot rows, versions and references, then
//! its row; a project's folders, then its row; last the organisation's row. It never holds a lock
//! across a long cascade, so a deletion of any size delays nobody else's writes by more than one
//! chunk (on SQLite, where there is one writer) or not at all (PostgreSQL).
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
//! A step that fails (the database busy, the storage away) is logged and the whole pass tried
//! again after [`RETRY`]; the marked rows stay invisible meanwhile.
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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, watch};

use crate::blobs::Blobs;
use crate::room::Gate;
use crate::store::Store;

/// The most rows one purge transaction removes.
pub const CHUNK_ROWS: i64 = 1000;
/// How long the purger waits after a pass that failed before it tries again.
pub const RETRY: Duration = Duration::from_secs(30);
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
    /// Something was marked since the running pass looked.
    again: AtomicBool,
    stopped: AtomicBool,
    /// Wakes a task that waits to try again.
    nudge: Notify,
    gate: Gate,
    chunks: AtomicUsize,
}

impl std::fmt::Debug for Purger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Purger")
            .field("busy", &self.busy())
            .finish_non_exhaustive()
    }
}

impl Default for Purger {
    fn default() -> Self {
        Self::new(CHUNK_ROWS, RETRY)
    }
}

/// The purger was told to stop in the middle of a pass.
#[derive(Debug, thiserror::Error)]
#[error("the purger was stopped")]
struct Stopped;

impl Purger {
    /// A purger that removes `chunk_rows` rows a transaction (at least 1) and tries a failed pass
    /// again after `retry`. It does nothing until [`Purger::wake`].
    pub fn new(chunk_rows: i64, retry: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                chunk_rows: chunk_rows.max(1),
                retry,
                busy: watch::Sender::new(false),
                again: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                nudge: Notify::new(),
                gate: Gate::default(),
                chunks: AtomicUsize::new(0),
            }),
        }
    }

    /// Whether the task is running (there is, or may be, something left to remove).
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

    /// Says that something was marked deleted (or that the server has just started): the task is
    /// started on the current tokio runtime unless it runs already, in which case it looks again
    /// when its pass ends. Nothing happens after [`Purger::stop`].
    pub fn wake(&self, store: Arc<dyn Store>, blobs: Arc<dyn Blobs>) {
        let inner = &self.inner;
        inner.again.store(true, Ordering::SeqCst);
        inner.nudge.notify_one();
        if inner.stopped.load(Ordering::SeqCst) || inner.busy.send_replace(true) {
            return;
        }
        let purger = self.clone();
        tokio::spawn(async move { purger.run(store, blobs).await });
    }

    /// Stops the task at its next chunk and waits for it to end. What it had not removed stays
    /// marked, for the next start.
    pub async fn stop(&self) {
        self.inner.stopped.store(true, Ordering::SeqCst);
        self.inner.nudge.notify_one();
        self.inner.gate.open();
        self.idle().await;
    }

    async fn run(self, store: Arc<dyn Store>, blobs: Arc<dyn Blobs>) {
        let inner = &self.inner;
        // (However the task ends, a panic included, it is no longer busy.)
        struct Done<'a>(&'a Inner);
        impl Drop for Done<'_> {
            fn drop(&mut self) {
                self.0.busy.send_replace(false);
            }
        }
        let done = Done(inner);
        loop {
            inner.again.store(false, Ordering::SeqCst);
            match self.pass(store.as_ref(), blobs.as_ref()).await {
                Ok(()) => {}
                Err(e) if e.is::<Stopped>() => return,
                Err(e) => {
                    tracing::warn!(
                        "removing what was deleted failed, and will be tried again: {e:#}"
                    );
                    // (A new deletion, or a stop, ends the wait early.)
                    let _ = tokio::time::timeout(inner.retry, inner.nudge.notified()).await;
                    inner.again.store(true, Ordering::SeqCst);
                }
            }
            if inner.stopped.load(Ordering::SeqCst) {
                return;
            }
            if !inner.again.load(Ordering::SeqCst) {
                // Not busy any more; unless something was marked just now, whose `wake` saw the
                // task still running: then it goes on.
                inner.busy.send_replace(false);
                if !inner.again.load(Ordering::SeqCst) || inner.busy.send_replace(true) {
                    // (Nothing came, or the `wake` that came started a task of its own.)
                    std::mem::forget(done);
                    return;
                }
            }
        }
    }

    /// Before every step: parks at the test gate, and ends the pass if the purger was stopped.
    async fn step(&self) -> anyhow::Result<()> {
        self.inner.gate.pass().await;
        if self.inner.stopped.load(Ordering::SeqCst) {
            return Err(Stopped.into());
        }
        Ok(())
    }

    /// Removes everything that is marked now.
    async fn pass(&self, store: &dyn Store, blobs: &dyn Blobs) -> anyhow::Result<()> {
        loop {
            let projects = store.deleted_projects(LIST).await?;
            if projects.is_empty() {
                break;
            }
            for project in &projects {
                self.project(store, blobs, project).await?;
            }
        }
        loop {
            let orgs = store.deleted_orgs(LIST).await?;
            if orgs.is_empty() {
                break;
            }
            for org in &orgs {
                self.step().await?;
                // Its blobs, then its row (which nothing hangs on any more).
                blobs.delete_prefix(&format!("orgs/{org}/")).await?;
                let mut tx = store.begin().await?;
                let gone = tx.purge_org(org).await?;
                tx.commit().await?;
                anyhow::ensure!(gone, "the deleted organisation {org} still has projects");
                tracing::info!(org = %org, "a deleted organisation was removed");
            }
        }
        Ok(())
    }

    async fn project(
        &self,
        store: &dyn Store,
        blobs: &dyn Blobs,
        project: &str,
    ) -> anyhow::Result<()> {
        loop {
            let documents = store.documents_of_deleted_project(project, LIST).await?;
            if documents.is_empty() {
                break;
            }
            for document in &documents {
                self.document(store, blobs, document).await?;
            }
        }
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
        anyhow::ensure!(gone, "the deleted project {project} still has documents");
        tracing::info!(project = %project, "a deleted project was removed");
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
        tx.purge_document(document).await?;
        tx.commit().await?;
        Ok(())
    }
}
