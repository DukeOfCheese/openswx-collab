//! The welcome a joining connection gets: built and encoded off the room's task, from what the room
//! had at the moment of the join.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use oswx_core::wire::{self, Broken, ChangeEvent, Lock, Peer, ServerMsg};
use oswx_core::{Head, Seq};

use crate::record::decode_ops;
use crate::security::ProjectRole;
use crate::snapshot::SERVER_CLIENT;
use crate::store::{Id, Store};

/// Everything a welcome needs, taken from the room at the moment of the join.
pub(super) struct Welcome {
    pub store: Arc<dyn Store>,
    pub doc: Id,
    /// The durable head at the join: the welcome's seq.
    pub head: Head,
    pub last_seq: Option<Seq>,
    pub catchup_limit: Seq,
    pub project: Id,
    pub role: ProjectRole,
    pub read_only: Option<String>,
    pub you: Peer,
    /// The other connections, in the order they joined (with their last presence and draft).
    pub peers: Vec<Peer>,
    /// Every lock.
    pub locks: Vec<Lock>,
    /// What does not rebuild.
    pub broken: BTreeMap<String, Broken>,
    pub encodes: Arc<AtomicU64>,
}

impl Welcome {
    /// The encoded welcome: the change events since `last_seq` when the client gave one within the
    /// catch-up window (`0 <= last_seq <= head`, at most `catchup_limit` behind), otherwise the
    /// document. If the journal cannot give the events, the document is sent instead.
    pub async fn build(self) -> anyhow::Result<Bytes> {
        let head_seq = self.head.seq;
        let changes = match self.last_seq {
            Some(last) if last <= head_seq && head_seq - last <= self.catchup_limit => {
                match change_events(self.store.as_ref(), &self.doc, last, head_seq).await {
                    Ok(events) => Some(events),
                    Err(e) => {
                        tracing::warn!(
                            "catching a client of document {} up failed; it gets the document: \
                             {e:#}",
                            self.doc
                        );
                        None
                    }
                }
            }
            _ => None,
        };
        let content = changes.is_none().then_some(self.head.doc);
        let msg = ServerMsg::Welcome {
            seq: head_seq,
            project_id: self.project,
            role: self.role.as_str().to_owned(),
            read_only: self.read_only.is_some(),
            read_only_reason: self.read_only,
            you: self.you,
            peers: self.peers,
            locks: self.locks,
            broken: self.broken,
            content,
            changes,
        };
        self.encodes.fetch_add(1, Ordering::Relaxed);
        Ok(tokio::task::spawn_blocking(move || wire::encode_server(&msg)).await?)
    }
}

/// The change events `after < seq <= upto`, read from the journal: the same events the live
/// broadcast carried.
async fn change_events(
    store: &dyn Store,
    doc: &str,
    after: Seq,
    upto: Seq,
) -> anyhow::Result<Vec<ChangeEvent>> {
    let (after_i, upto_i) = (i64::try_from(after)?, i64::try_from(upto)?);
    let rows = store
        .change_events_between(doc, after_i, upto_i, upto_i - after_i)
        .await?;
    anyhow::ensure!(
        rows.len() as Seq == upto - after,
        "the journal of document {doc} is missing changes between {after} and {upto}"
    );
    tokio::task::spawn_blocking(move || {
        rows.into_iter()
            .map(|row| {
                Ok(ChangeEvent {
                    seq: Seq::try_from(row.seq)?,
                    client_id: Arc::from(row.client_id.as_deref().unwrap_or(SERVER_CLIENT)),
                    author: row.author_id,
                    author_name: row.author_name.unwrap_or_default(),
                    change_id: Arc::from(row.change_id.as_deref().unwrap_or("")),
                    ops: decode_ops(&row.ops)?,
                    command: Arc::from(row.command.as_str()),
                    undo_of: row.undo_of.as_deref().map(Arc::from),
                    reverts: row.reverts.map(Seq::try_from).transpose()?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()
    })
    .await?
}
