//! A room's end (closing, unloading when the document is lost) and its snapshots.

use oswx_core::Seq;
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::{CLOSE_WAIT, Closing, FINAL_SNAPSHOT_WAIT, Internal, Room, close};
use crate::snapshot::write_snapshot;

impl Room {
    // ------------------------------------------------------------------ closing

    pub(super) fn begin_close(
        &mut self,
        code: u16,
        reason: &str,
        snapshot: bool,
        done: Option<oneshot::Sender<()>>,
    ) {
        match &mut self.closing {
            Some(c) => c.done.extend(done),
            None => {
                self.closing = Some(Closing {
                    code,
                    reason: reason.to_owned(),
                    snapshot,
                    done: done.into_iter().collect(),
                    deadline: Instant::now() + CLOSE_WAIT,
                });
            }
        }
    }

    /// The document is no longer this room's to write (the lease moved, or the journal could not
    /// say whether a change was written): forget everything tentative and unload at once, telling
    /// every client to reconnect.
    pub(super) fn lose(&mut self, why: &str) {
        tracing::warn!(
            "document {} is unloaded and its clients told to reconnect: {why}",
            self.doc
        );
        self.pending.clear();
        self.generation += 1;
        self.tentative = self.durable.clone();
        self.state.hub.forget(&self.doc, self.id);
        match &mut self.closing {
            Some(c) => {
                c.deadline = Instant::now();
                c.snapshot = false;
                c.code = close::MOVED;
                c.reason = "reconnect".into();
            }
            None => {
                self.closing = Some(Closing {
                    code: close::MOVED,
                    reason: "reconnect".into(),
                    snapshot: false,
                    done: Vec::new(),
                    deadline: Instant::now(),
                });
            }
        }
    }

    pub(super) async fn finish(&mut self) {
        let Some(c) = self.closing.take() else {
            return;
        };
        // Out of the hub first, so that a client reconnecting at once gets a fresh room.
        self.state.hub.forget(&self.doc, self.id);
        for (_, conn) in self.conns.drain() {
            conn.outbox.close(c.code, &c.reason);
        }
        if c.snapshot {
            if let Some(task) = self.snapshot_task.take() {
                let _ = tokio::time::timeout(FINAL_SNAPSHOT_WAIT, task).await;
            }
            if self.durable.seq > self.snapshot_seq {
                self.prune_index();
                let head = self.durable.clone();
                let written = tokio::time::timeout(
                    FINAL_SNAPSHOT_WAIT,
                    write_snapshot(
                        self.state.store.as_ref(),
                        self.state.blobs.as_ref(),
                        &self.doc,
                        self.epoch,
                        &head,
                        self.state.clock.now(),
                    ),
                )
                .await;
                match written {
                    Ok(Ok(())) => self.snapshot_seq = head.seq,
                    Ok(Err(e)) => {
                        tracing::warn!("the final snapshot of document {} failed: {e:#}", self.doc)
                    }
                    Err(_) => {
                        tracing::warn!("the final snapshot of document {} timed out", self.doc)
                    }
                }
            }
        }
        for done in c.done {
            let _ = done.send(());
        }
    }

    // ------------------------------------------------------------------ snapshots

    /// Drops index marks older than [`super::Hooks::index_keep`] changes from both heads. Only ever called right
    /// before a snapshot of the durable head is written, so that a reload judges as memory does.
    pub(super) fn prune_index(&mut self) {
        let floor = self
            .durable
            .seq
            .saturating_sub(self.state.room_hooks.index_keep);
        if floor > self.durable.index.floor() {
            self.durable.index = self.durable.index.prune(floor);
            self.tentative.index = self.tentative.index.prune(floor);
        }
    }

    /// Writes a snapshot in the background once enough durable changes have built up since the last
    /// one (a failed one is tried again after the next change).
    pub(super) fn maybe_snapshot(&mut self) {
        let every = Seq::from(self.state.settings.snapshot_every.max(1));
        if self.snapshot_task.is_some()
            || self.closing.is_some()
            || self.durable.seq - self.snapshot_seq < every
        {
            return;
        }
        self.prune_index();
        let head = self.durable.clone();
        let (store, blobs, doc, epoch) = (
            self.state.store.clone(),
            self.state.blobs.clone(),
            self.doc.clone(),
            self.epoch,
        );
        let now = self.state.clock.now();
        let internal = self.internal.clone();
        self.snapshot_task = Some(tokio::spawn(async move {
            let result =
                write_snapshot(store.as_ref(), blobs.as_ref(), &doc, epoch, &head, now).await;
            if let Err(e) = &result {
                tracing::warn!("a snapshot of document {doc} failed (tried again later): {e:#}");
            }
            let _ = internal.send(Internal::Snapshot {
                seq: head.seq,
                ok: result.is_ok(),
            });
        }));
    }
}
