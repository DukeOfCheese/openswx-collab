//! Presence (pointers, selections, cameras), drafts (previews of unfinished work) and who is here.
//!
//! `presence` and `draft` are relayed to everyone else as they came (opaque data the codec checked
//! and bounded: 16 KB and 128 KB), encoded once per broadcast, and never stored in the database. The
//! last of each connection is kept for the welcomes of later joiners; `draft` with no data clears
//! it. A draft from a connection that may only look is dropped. Each connection may send
//! `presence_rate` of them a second, together (a token bucket that starts full); more are dropped
//! silently. They reach the room by the way commands do, not behind the frames it may be holding
//! back, and are handled at once, ahead of anything the connection's earlier messages wait for:
//! they are never answered and never wait for the journal. A slow reader is still closed (4408) by
//! its queue's limits.
//!
//! The room also publishes who has the document open ([`super::RoomHandle::online`]): one entry per
//! person, in the order they joined, at most [`MAX_ONLINE`], and how many people in all. It is
//! updated at every join and leave, so the document list reads it without asking the room.

use oswx_core::wire::{ClientMsg, ServerMsg};
use std::sync::PoisonError;

use super::{ConnId, Room};
use crate::hub::Online;
use crate::metrics;

/// The most people the online list names (the count has them all).
pub const MAX_ONLINE: usize = 8;

/// Whether a message is presence or a draft.
pub(super) fn is_live(msg: &ClientMsg) -> bool {
    matches!(msg, ClientMsg::Presence { .. } | ClientMsg::Draft { .. })
}

/// A token bucket that starts full: it holds at most `capacity` tokens and refills at `rate` a
/// second.
#[derive(Debug, Clone)]
pub(super) struct Bucket {
    tokens: f64,
    at: Option<f64>,
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            tokens: f64::INFINITY,
            at: None,
        }
    }
}

impl Bucket {
    /// Takes `cost` tokens at `now` if there are that many; otherwise takes none.
    pub(super) fn take(&mut self, cost: f64, capacity: f64, rate: f64, now: f64) -> bool {
        let refill = self.at.map_or(capacity, |at| (now - at).max(0.0) * rate);
        self.tokens = (self.tokens + refill).min(capacity);
        self.at = Some(now);
        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }
}

impl Room {
    /// A presence or draft message of a connection.
    pub(super) fn on_live(&mut self, conn: ConnId, msg: ClientMsg) {
        let now = self.state.clock.now();
        let rate = f64::from(self.state.settings.presence_rate);
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        if !c.live_rate.take(1.0, rate, rate, now) {
            return metrics::presence_dropped();
        }
        let client_id = c.client.clone();
        let out = match msg {
            ClientMsg::Presence { data } => {
                c.presence = Some(data.clone());
                ServerMsg::Presence { client_id, data }
            }
            ClientMsg::Draft { .. } if c.read_only.is_some() => {
                return metrics::presence_dropped();
            }
            ClientMsg::Draft { data } => {
                c.draft.clone_from(&data);
                ServerMsg::Draft { client_id, data }
            }
            _ => return,
        };
        let frame = self.encode(&out);
        self.broadcast(&frame, &[conn]);
        metrics::presence_relayed();
    }

    /// Publishes the online list for [`super::RoomHandle::online`].
    pub(super) fn publish_online(&self) {
        let mut conns: Vec<_> = self.conns.values().collect();
        conns.sort_by_key(|c| c.joined);
        let mut seen = std::collections::HashSet::new();
        let mut people = Vec::new();
        for c in conns {
            if seen.insert(c.user.as_str()) {
                people.push(Online {
                    name: c.name.clone(),
                    colour: c.colour.clone(),
                });
            }
        }
        let count = people.len();
        people.truncate(MAX_ONLINE);
        *self
            .shared
            .online
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = (people, count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_starts_full_and_refills_by_the_second() {
        let take = |b: &mut Bucket, now: f64| b.take(1.0, 60.0, 60.0, now);
        let mut b = Bucket::default();
        assert!((0..60).all(|_| take(&mut b, 100.0)));
        assert!(!take(&mut b, 100.0), "the 61st in the same second");
        assert!(take(&mut b, 101.0), "the next second");
        assert!((0..59).all(|_| take(&mut b, 101.0)));
        assert!(!take(&mut b, 101.0));
        assert!(take(&mut b, 101.5), "half a second is thirty");
        // It never holds more than its capacity.
        let mut b = Bucket::default();
        b.take(1.0, 5.0, 5.0, 0.0);
        assert_eq!(
            (0..100).filter(|_| b.take(1.0, 5.0, 5.0, 1000.0)).count(),
            5
        );
    }

    #[test]
    fn a_costly_request_is_paid_in_full_or_not_at_all() {
        let mut b = Bucket::default();
        assert!(b.take(256.0, 512.0, 64.0, 0.0));
        assert!(b.take(256.0, 512.0, 64.0, 0.0));
        assert!(!b.take(1.0, 512.0, 64.0, 0.0), "empty");
        assert!(!b.take(256.0, 512.0, 64.0, 3.9), "249.6 tokens");
        assert!(b.take(256.0, 512.0, 64.0, 4.0), "256 after 4 s");
        assert!(b.take(3.0, 512.0, 64.0, 4.1), "a few paths again soon");
    }
}
