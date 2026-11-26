//! A room's end (closing, unloading when idle or when the document is lost) and its snapshots.

use std::sync::atomic::Ordering;

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
                    idle: false,
                    done: done.into_iter().collect(),
                    deadline: Instant::now() + CLOSE_WAIT,
                });
            }
        }
    }

    /// Whether the room may unload because it is idle: nobody connected for `room_idle` seconds,
    /// no lock (attached or waiting for its client), nothing decided but not durable, no change of
    /// the server's waiting, nothing being judged or looked up, no snapshot being written, and not
    /// closing already.
    pub(super) fn may_unload(&self) -> bool {
        let idle_long = self
            .idle_since
            .is_some_and(|t| self.state.clock.now() - t >= self.state.settings.room_idle);
        idle_long
            && self.closing.is_none()
            && self.conns.is_empty()
            && self.locks.is_empty()
            && self.pending.is_empty()
            && self.server_queue.is_empty()
            && self.paused == 0
            && self.checking.is_empty()
            && self.undo_waiting.is_empty()
            && self.snapshot_task.is_none()
    }

    /// Unloads the room because it is idle: it stops being offered to joiners at once, writes a
    /// final snapshot if it has changes since the last (an unwritable one is given up: the journal
    /// has every change, and the next load replays them), then leaves the hub and ends.
    pub(super) fn begin_unload(&mut self) {
        self.shared.unloading.store(true, Ordering::SeqCst);
        self.closing = Some(Closing {
            code: close::GOING_AWAY,
            reason: "the document was unloaded".into(),
            snapshot: true,
            idle: true,
            done: Vec::new(),
            deadline: Instant::now(),
        });
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
                    idle: false,
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
        if !c.idle {
            // Out of the hub first, so that a client reconnecting at once gets a fresh room. (An
            // idle room has nobody to reconnect; it stays until its snapshot is written, so that
            // a joiner meanwhile waits for it rather than loading the document beside it.)
            self.state.hub.forget(&self.doc, self.id);
        }
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
                let failed = match written {
                    Ok(Ok(())) => {
                        self.snapshot_seq = head.seq;
                        None
                    }
                    Ok(Err(e)) => Some(format!("failed: {e:#}")),
                    Err(_) => Some("timed out".to_owned()),
                };
                match failed {
                    // Already reported for this outage.
                    Some(why) if self.snapshot_failing => {
                        tracing::debug!("the final snapshot of document {} {why}", self.doc);
                    }
                    Some(why) => {
                        tracing::warn!(
                            "the final snapshot of document {} {why} (the journal has every \
                             change; the next load replays them)",
                            self.doc
                        );
                    }
                    None => {}
                }
            }
        }
        if c.idle {
            self.state.hub.forget(&self.doc, self.id);
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
    /// one (a failed one is tried again after the next change, or at the next tick). A failure is
    /// logged as a warning once per outage (until a snapshot succeeds again), not at every try.
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
        let failing = self.snapshot_failing;
        self.snapshot_task = Some(tokio::spawn(async move {
            let written = tokio::time::timeout(
                FINAL_SNAPSHOT_WAIT,
                write_snapshot(store.as_ref(), blobs.as_ref(), &doc, epoch, &head, now),
            )
            .await;
            let failed = match written {
                Ok(Ok(())) => None,
                Ok(Err(e)) => Some(format!("failed: {e:#}")),
                Err(_) => Some("timed out".to_owned()),
            };
            match &failed {
                Some(why) if failing => {
                    tracing::debug!("a snapshot of document {doc} {why} (tried again later)");
                }
                Some(why) => {
                    tracing::warn!(
                        "a snapshot of document {doc} {why} (tried again later; not reported \
                         again until one succeeds)"
                    );
                }
                None => {}
            }
            let _ = internal.send(Internal::Snapshot {
                seq: head.seq,
                ok: failed.is_none(),
            });
        }));
    }
}
