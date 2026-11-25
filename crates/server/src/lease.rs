//! Room leases: which node holds a document's room, under which epoch.
//!
//! A room takes the lease with [`acquire`] before it loads the document, and puts the epoch it got
//! in every [`crate::record::Record`]. The journal writer compares that epoch with the lease row
//! inside its transaction, so a room whose lease has moved on can no longer write.
//!
//! Every acquisition bumps the epoch, even by the node that holds it: a room loaded twice in one
//! process (an unload racing a reload) cannot have two writers sharing an epoch, and the older one
//! is refused from its next write. This plan runs one node: every lease row was written by this
//! process or an earlier run of the server, so [`acquire`] always takes it. Refusing a live lease
//! of another node (and forwarding the connection there) is project 3's.

use anyhow::Context;

use crate::store::{RoomLease, Store};

/// Takes the room lease of `doc` for `node_id` until `now + ttl`, in one transaction, and answers
/// the new epoch: 1 for a document that never had a lease (a new document's snapshot was written
/// under epoch 0), the previous epoch plus one otherwise.
pub async fn acquire(
    store: &dyn Store,
    doc: &str,
    node_id: &str,
    now: f64,
    ttl: f64,
) -> anyhow::Result<u64> {
    let expires_at = now + ttl;
    let mut tx = store.begin().await?;
    let epoch = match tx.take_room_lease(doc, node_id, expires_at).await? {
        Some(epoch) => epoch,
        None => {
            tx.insert_room_lease(&RoomLease {
                document_id: doc.to_owned(),
                node_id: node_id.to_owned(),
                epoch: 1,
                expires_at,
            })
            .await?;
            1
        }
    };
    tx.commit().await.context("taking a room lease")?;
    Ok(u64::try_from(epoch)?)
}
