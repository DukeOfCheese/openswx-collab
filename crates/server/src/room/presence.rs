//! Presence (pointers, selections, cameras), drafts (previews of unfinished work) and who is here.
//!
//! `presence` and `draft` are relayed to everyone else as they came (opaque data the codec checked
//! and bounded: 16 KB and 128 KB), and never stored in the database. The last of each connection is
//! kept for the welcomes of later joiners; `draft` with no data clears it. A draft from a
//! connection that may only look is dropped. Each connection may send `presence_rate` of them a
//! second, together (a token bucket that starts full); more are dropped silently. They reach the
//! room by the way commands do, not behind the frames it may be holding back, and are handled at
//! once, ahead of anything the connection's earlier messages wait for: they are never answered and
//! never wait for the journal.
//!
//! The room relays them at most every [`LIVE_TICK`]: what came in between goes out together, each
//! connection's latest presence and latest draft only, in one frame per [`MAX_LIVE_ENTRIES`]
//! (`presence.batch`; a lone entry goes as the `presence` or `draft` message it was). Each frame is
//! encoded once for everyone but the senders in it, who get it without their own entries; entries
//! are encoded once, when they come. Relayed frames, and `peer.joined` and `peer.left`, are soft
//! ([`super::conn::Outbox::push_soft`]): a reader that does not keep up falls behind on them instead
//! of being closed, and once it has written what it had it is sent `peer.list`, everyone as they
//! are now.
//!
//! The room also publishes who has the document open ([`super::RoomHandle::online`]): one entry per
//! person, in the order they joined, at most [`MAX_ONLINE`], and how many people in all. It is
//! updated at every join and leave, so the document list reads it without asking the room.

use std::sync::PoisonError;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use oswx_core::wire::{self, ClientMsg, LiveEntry};
use tokio::time::Instant;

use super::conn::Soft;
use super::{ConnId, Room};
use crate::hub::Online;
use crate::metrics;

/// The most people the online list names (the count has them all).
pub const MAX_ONLINE: usize = 8;
/// How often a room relays presence and drafts at most: what comes in between is relayed together
/// at the next tick, each connection's latest only. A presence that comes after a quiet tick is
/// relayed at once.
pub const LIVE_TICK: Duration = Duration::from_millis(50);
/// The most entries one relayed frame carries.
pub const MAX_LIVE_ENTRIES: usize = 64;
/// A relayed frame takes no more entries once it has this many bytes of them.
pub const MAX_LIVE_BYTES: usize = 64 * 1024;

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
    /// A presence or draft message of a connection: kept as its latest, and relayed at the next
    /// relay ([`Room::relay_live_when_due`]).
    pub(super) fn on_live(&mut self, conn: ConnId, msg: ClientMsg) {
        let now = self.state.clock.now();
        let rate = f64::from(self.state.settings.presence_rate);
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        if !c.live_rate.take(1.0, rate, rate, now) {
            return metrics::presence_dropped();
        }
        let had_news = c.presence_news.is_some() || c.draft_news.is_some();
        match msg {
            ClientMsg::Presence { data } => {
                c.presence_news = Some(LiveEntry::new(&c.client, Some(&data)));
                c.presence = Some(data);
            }
            ClientMsg::Draft { .. } if c.read_only.is_some() => {
                return metrics::presence_dropped();
            }
            ClientMsg::Draft { data } => {
                c.draft_news = Some(LiveEntry::new(&c.client, data.as_ref()));
                c.draft = data;
            }
            _ => return,
        }
        if !had_news {
            self.live_news.push(conn);
        }
        metrics::presence_relayed();
    }

    /// Relays the presence and drafts that came since the last relay, if that was [`LIVE_TICK`]
    /// ago (otherwise sets when the next is due), and sends the peers to connections that were
    /// behind on soft frames and have written theirs. Called after every event the room handles.
    pub(super) fn relay_live_when_due(&mut self) {
        if self.live_news.is_empty() && self.behind.is_empty() {
            self.live_due = None;
            return;
        }
        let now = Instant::now();
        let due = self.live_sent + LIVE_TICK;
        if now < due {
            self.live_due = Some(due);
            return;
        }
        self.live_sent = now;
        self.relay_live();
        self.catch_up_behind();
        // Those still behind are looked at again a tick later.
        self.live_due = (!self.behind.is_empty()).then_some(now + LIVE_TICK);
    }

    /// Every connection's latest presence and draft since the last relay, to everyone else: one
    /// frame per [`MAX_LIVE_ENTRIES`] entries ([`MAX_LIVE_BYTES`]), encoded once for all but the
    /// senders in it (who get it without their own entries).
    fn relay_live(&mut self) {
        let news = std::mem::take(&mut self.live_news);
        // (connection, is a draft, entry)
        let mut entries: Vec<(ConnId, bool, LiveEntry)> = Vec::new();
        for id in news {
            if let Some(c) = self.conns.get_mut(&id) {
                entries.extend(c.presence_news.take().map(|e| (id, false, e)));
                entries.extend(c.draft_news.take().map(|e| (id, true, e)));
            }
        }
        let mut rest = entries.as_slice();
        while !rest.is_empty() {
            let mut n = 0;
            let mut bytes = 0;
            while n < rest.len() && n < MAX_LIVE_ENTRIES && (n == 0 || bytes < MAX_LIVE_BYTES) {
                bytes += rest[n].2.encoded_len();
                n += 1;
            }
            let (chunk, after) = rest.split_at(n);
            rest = after;
            self.relay_chunk(chunk);
        }
    }

    fn relay_chunk(&mut self, chunk: &[(ConnId, bool, LiveEntry)]) {
        let frame = |without: Option<ConnId>| {
            let pick = |drafts: bool| -> Vec<&LiveEntry> {
                chunk
                    .iter()
                    .filter(|(id, d, _)| *d == drafts && Some(*id) != without)
                    .map(|(_, _, e)| e)
                    .collect()
            };
            let (presence, drafts) = (pick(false), pick(true));
            if presence.is_empty() && drafts.is_empty() {
                return None;
            }
            self.encodes.fetch_add(1, Ordering::Relaxed);
            Some(wire::encode_live(&presence, &drafts))
        };
        let Some(shared) = frame(None) else {
            return;
        };
        let mut behind = Vec::new();
        let mut recipients = 0;
        for (id, c) in &self.conns {
            let sent_some = chunk.iter().any(|(sender, _, _)| sender == id);
            let out = if sent_some {
                match frame(Some(*id)) {
                    Some(f) => f,
                    None => continue,
                }
            } else {
                shared.clone()
            };
            recipients += 1;
            if c.outbox.push_soft(out) == Soft::Behind {
                behind.push(*id);
            }
        }
        self.behind.extend(behind);
        metrics::broadcast(recipients);
    }

    /// One soft frame (who is here) to every connection.
    pub(super) fn broadcast_soft(&mut self, frame: &Bytes) {
        let mut behind = Vec::new();
        for (id, c) in &self.conns {
            if c.outbox.push_soft(frame.clone()) == Soft::Behind {
                behind.push(*id);
            }
        }
        metrics::broadcast(self.conns.len());
        self.behind.extend(behind);
    }

    /// Sends `peer.list` to every connection behind on soft frames that has written the ones it
    /// had: the peers as they are now, presence and drafts included, which is what the frames it
    /// missed came to. Each peer is encoded once, whoever receives it.
    fn catch_up_behind(&mut self) {
        let mut encoded: Option<Vec<(ConnId, Bytes)>> = None;
        let mut still = std::collections::HashSet::new();
        for id in std::mem::take(&mut self.behind) {
            let Some(c) = self.conns.get(&id) else {
                continue;
            };
            let conns = &self.conns;
            let peers = encoded.get_or_insert_with(|| {
                let mut all: Vec<(&ConnId, &super::Conn)> = conns.iter().collect();
                all.sort_by_key(|(_, c)| c.joined);
                all.into_iter()
                    .map(|(id, c)| (*id, wire::encode_peer(&c.peer())))
                    .collect()
            });
            let caught_up = c.outbox.catch_up(|| {
                let others: Vec<&Bytes> = peers
                    .iter()
                    .filter(|(p, _)| *p != id)
                    .map(|(_, b)| b)
                    .collect();
                self.encodes.fetch_add(1, Ordering::Relaxed);
                wire::encode_peer_list(&others)
            });
            if !caught_up && c.outbox.is_behind() && !c.outbox.is_closed() {
                still.insert(id);
            }
        }
        self.behind = still;
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
