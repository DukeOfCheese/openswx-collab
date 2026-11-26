//! The open documents (rooms).
//!
//! The hub maps a document id to its room's [`RoomHandle`]. [`Hub::open`] loads a room at most once
//! per document however many connections ask at the same moment (a gate per document); a load that
//! fails is reported to the caller and not remembered. A room takes itself out of the hub when it
//! unloads.
//!
//! [`Hub::online`] reads what each open room publishes (never loading a room or waiting for one);
//! Task 12 gives the hub the drops (`drop_account`, `drop_user` only record the call for now) and
//! the sweeper.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde::Serialize;

use crate::lease;
use crate::room::{self, RoomHandle, RoomInit, close};
use crate::snapshot::{LoadError, load_head};
use crate::state::AppState;
use crate::store::Id;

/// How long a room lease is said to last (a hint for other nodes; the epoch is the guard).
const LEASE_TTL: f64 = 60.0;
/// How long shutdown waits for the rooms to close.
const SHUTDOWN_WAIT: Duration = Duration::from_secs(60);

/// Someone connected to a document, as the document list shows them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Online {
    pub name: String,
    pub colour: String,
}

/// Why a room could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// There is no such document, or it was deleted.
    #[error("there is no such document")]
    NoDocument,
    /// The server is shutting down and opens no rooms.
    #[error("the server is shutting down")]
    ShuttingDown,
    #[error(transparent)]
    Failed(#[from] anyhow::Error),
}

#[derive(Debug, Default)]
struct Inner {
    rooms: Mutex<HashMap<Id, RoomHandle>>,
    /// One per document being opened, so that it is loaded once.
    gates: Mutex<HashMap<Id, Arc<tokio::sync::Mutex<()>>>>,
    closing: AtomicBool,
    /// The drop and close calls made so far (for tests; Task 12 gives drops their behaviour).
    calls: Mutex<Vec<String>>,
}

/// The open rooms. Cloning is cheap: clones share them.
#[derive(Debug, Clone, Default)]
pub struct Hub {
    inner: Arc<Inner>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Hub {
    /// A hub with no rooms.
    pub fn new() -> Self {
        Self::default()
    }

    fn record(&self, call: String) {
        let mut calls = lock(&self.inner.calls);
        if calls.len() >= 1000 {
            calls.remove(0); // (a running server keeps only the latest)
        }
        calls.push(call);
    }

    /// The drop and close calls made so far, as `drop_account <user>`, `drop_user <user> <org>` and
    /// `close_document <doc>` (for tests).
    #[doc(hidden)]
    pub fn recorded(&self) -> Vec<String> {
        lock(&self.inner.calls).clone()
    }

    /// The room of `doc` if it is open.
    pub fn loaded(&self, doc: &str) -> Option<RoomHandle> {
        lock(&self.inner.rooms)
            .get(doc)
            .filter(|r| !r.is_closed())
            .cloned()
    }

    /// The room of `doc`, loading it if it is not open: the document (which must exist and not be
    /// deleted) is leased to this process, then its head is loaded from the store.
    pub async fn open(&self, state: &AppState, doc: &str) -> Result<RoomHandle, OpenError> {
        if self.inner.closing.load(Ordering::SeqCst) {
            return Err(OpenError::ShuttingDown);
        }
        if let Some(room) = self.loaded(doc) {
            return Ok(room);
        }
        let gate = lock(&self.inner.gates)
            .entry(doc.to_owned())
            .or_default()
            .clone();
        let opened = {
            let _held = gate.lock().await;
            match self.loaded(doc) {
                Some(room) => Ok(room),
                None if self.inner.closing.load(Ordering::SeqCst) => Err(OpenError::ShuttingDown),
                None => match load(state, doc).await {
                    Ok(init) => {
                        let room = room::spawn(state.clone(), init);
                        let kept = {
                            let mut rooms = lock(&self.inner.rooms);
                            let closing = self.inner.closing.load(Ordering::SeqCst);
                            if !closing {
                                rooms.insert(doc.to_owned(), room.clone());
                            }
                            !closing
                        };
                        if kept {
                            Ok(room)
                        } else {
                            // Shutdown began while it loaded: it closes at once, unseen.
                            room.close(close::GOING_AWAY, "the server is shutting down", false)
                                .await;
                            Err(OpenError::ShuttingDown)
                        }
                    }
                    Err(e) => Err(e),
                },
            }
        };
        let mut gates = lock(&self.inner.gates);
        if gates
            .get(doc)
            .is_some_and(|g| Arc::ptr_eq(g, &gate) && Arc::strong_count(g) == 2)
        {
            gates.remove(doc);
        }
        opened
    }

    /// Takes the room `id` of `doc` out of the hub (a room unloading itself); a newer room of the
    /// same document is left alone.
    pub(crate) fn forget(&self, doc: &str, id: u64) {
        let mut rooms = lock(&self.inner.rooms);
        if rooms.get(doc).is_some_and(|r| r.id() == id) {
            rooms.remove(doc);
        }
    }

    /// Closes every connection of an account that no longer exists or may no longer sign in.
    pub async fn drop_account(&self, user: &Id) {
        self.record(format!("drop_account {user}"));
    }

    /// Closes the connections a user has to an organisation's documents after their role changed.
    pub async fn drop_user(&self, user: &Id, org: &Id) {
        self.record(format!("drop_user {user} {org}"));
    }

    /// Closes every connection to a document, telling the clients why (`GONE`), and unloads its
    /// room without a snapshot (the document was deleted).
    pub async fn close_document(&self, doc: &Id, reason: &str) {
        self.record(format!("close_document {doc}"));
        let room = lock(&self.inner.rooms).remove(doc);
        if let Some(room) = room {
            room.close(close::GONE, reason, false).await;
        }
    }

    /// Sends a message to everyone who has a document open; nobody when its room is not open
    /// (never opens one).
    pub async fn notify(&self, doc: &str, msg: oswx_core::wire::ServerMsg) {
        if let Some(room) = self.loaded(doc) {
            room.notify(msg).await;
        }
    }

    /// The first people connected to a document (one entry per person, in the order they joined,
    /// at most [`room::MAX_ONLINE`]), and how many people are connected in all. Nobody when the
    /// document's room is not open; never opens one, never waits for one.
    pub async fn online(&self, doc: &Id) -> (Vec<Online>, usize) {
        self.loaded(doc)
            .map_or_else(|| (Vec::new(), 0), |room| room.online())
    }

    /// Closes every room: each waits briefly for the journal's answers to changes in flight, closes
    /// its connections with `GOING_AWAY` and writes a final snapshot if it has new changes. No room
    /// opens afterwards.
    pub async fn shutdown(&self) {
        self.inner.closing.store(true, Ordering::SeqCst);
        let rooms: Vec<RoomHandle> = lock(&self.inner.rooms).drain().map(|(_, r)| r).collect();
        let closing = rooms
            .iter()
            .map(|r| r.close(close::GOING_AWAY, "the server is shutting down", true));
        if tokio::time::timeout(SHUTDOWN_WAIT, futures_util::future::join_all(closing))
            .await
            .is_err()
        {
            tracing::warn!("some rooms did not close in time");
        }
    }
}

/// What a room of `doc` starts from: the document and project rows, the lease (acquired before the
/// head is read, so that no write of an older holder can land after the read), and the head.
async fn load(state: &AppState, doc: &str) -> Result<RoomInit, OpenError> {
    let document = state
        .store
        .document_by_id(doc)
        .await?
        .filter(|d| d.deleted_at.is_none())
        .ok_or(OpenError::NoDocument)?;
    let project = state
        .store
        .project_by_id(&document.project_id)
        .await?
        .ok_or(OpenError::NoDocument)?;
    let epoch = lease::acquire(
        state.store.as_ref(),
        doc,
        &state.node_id,
        state.clock.now(),
        LEASE_TTL,
    )
    .await?;
    let (head, _) = load_head(state.store.as_ref(), state.blobs.as_ref(), doc)
        .await
        .map_err(|e| match e {
            LoadError::NoDocument => OpenError::NoDocument,
            other => OpenError::Failed(other.into()),
        })?;
    let snapshot_seq = state
        .store
        .snapshot_at_or_before(doc, i64::try_from(head.seq).unwrap_or(i64::MAX))
        .await?
        .map_or(0, |s| u64::try_from(s.seq).unwrap_or(0));
    Ok(RoomInit {
        doc: doc.to_owned(),
        project: project.id,
        org: project.org_id,
        format_version: document.format_version,
        head,
        epoch,
        snapshot_seq,
    })
}
