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
//! of being closed.
//!
//! Once a reader behind has written everything it had, it is caught up at the next tick (at most
//! once a [`super::Hooks::catch_up_every`]): it is sent `peer.list`, everyone as they are now with
//! **no draft**, and with their presence up to [`PEER_LIST_PRESENCE`] of it in all (in the order
//! they joined; the peers past it come without). One list is made per tick for every reader caught
//! up in it, encoded from what the room holds without copying it first, so it names the recipient
//! too (a client skips its own `client_id`), and costs one allocation however many readers share
//! it; it carries the peers' ids and names and at most 1 MiB more, however large their drafts
//! and presence are. What it left out follows: the presence it had no room for, in frames of up to
//! [`MAX_LIVE_ENTRIES`] (made once per tick for every reader at the same place: a frame ends just
//! before the reader's own entry and the next begins after it, so that none is made for one reader
//! alone), then each peer's
//! current draft as a `draft` frame (made once per draft and shared by every reader owed it),
//! queued a few at a time while the reader has room to spare
//! ([`super::conn::Outbox::push_soft_with_room`]), so that they never make it fall behind again. A
//! reader that stays behind for [`super::Hooks::frozen_after`] while its socket takes nothing (and
//! that long after the slowest link would have read everything it was handed:
//! [`super::conn::Outbox::is_frozen`]) is closed as too slow (and leaves the others' peer lists).
//!
//! The room also publishes who has the document open ([`super::RoomHandle::online`]): one entry per
//! person, in the order they joined, at most [`MAX_ONLINE`], and how many people in all. It is
//! updated at every join and leave, so the document list reads it without asking the room.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use oswx_core::wire::{self, ClientMsg, LiveEntry, PeerRef};
use tokio::time::Instant;

use super::conn::{Outbox, Soft};
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
/// The most bytes of presence one `peer.list` carries: the peers past it (in the order they joined)
/// come without, and their presence follows the list.
pub const PEER_LIST_PRESENCE: usize = 1024 * 1024;

/// What a connection that caught up is owed of a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Owed {
    /// Its presence (the list had no room for it).
    Presence,
    /// Its draft (the list carries none).
    Draft,
}

/// What connections caught up at one tick are owed, in order; shared by all of them.
pub(super) type OwedList = Arc<[(ConnId, Owed)]>;

/// Whether `owed` names `conn`'s own presence.
fn names(owed: &[(ConnId, Owed)], conn: ConnId) -> bool {
    owed.iter()
        .any(|&(peer, kind)| peer == conn && kind == Owed::Presence)
}

/// Owed presence frames made at one tick, by the place they start at in a list (the list's address
/// and the place): the place after each, the frame (none if it carries nothing) and how many
/// entries it has. Several may start at one place, each ending before a different reader's own
/// entry.
type MadeFrames = HashMap<(usize, usize), Vec<(usize, Option<Bytes>, usize)>>;

/// The owed presence frame for `to` at place `next` of `owed`: one already made at that place that
/// does not name `to`, or else `make()`, which is then kept for the readers that come to that
/// place after it.
fn shared_frame(
    made: &mut MadeFrames,
    owed: &OwedList,
    next: usize,
    to: ConnId,
    make: impl FnOnce() -> (usize, Option<Bytes>, usize),
) -> (usize, Option<Bytes>, usize) {
    // Its own entry: nothing, and on to the place after it.
    if owed.get(next) == Some(&(to, Owed::Presence)) {
        return (next + 1, None, 0);
    }
    let key = (Arc::as_ptr(owed).cast::<u8>() as usize, next);
    let at = made.entry(key).or_default();
    if let Some(frame) = at
        .iter()
        .find(|(after, _, _)| !names(&owed[next..*after], to))
    {
        return frame.clone();
    }
    let frame = make();
    debug_assert!(!names(&owed[next..frame.0], to));
    at.push(frame.clone());
    frame
}

/// The presence entries one owed frame carries for `to` from place `from` of `owed`: up to the
/// next draft owed or `to`'s own entry (a frame ends just before it; one that starts at it is that
/// entry alone, and carries nothing), at most [`MAX_LIVE_ENTRIES`] ([`MAX_LIVE_BYTES`] of
/// `len`), leaving out the peers `entry` has nothing for (gone, or no presence any more). Answers
/// the place after them and the entries.
fn presence_range<E>(
    owed: &[(ConnId, Owed)],
    from: usize,
    to: ConnId,
    entry: impl Fn(ConnId) -> Option<E>,
    len: impl Fn(&E) -> usize,
) -> (usize, Vec<E>) {
    if owed.get(from) == Some(&(to, Owed::Presence)) {
        return (from + 1, Vec::new());
    }
    let mut entries = Vec::new();
    let mut bytes = 0;
    let mut at = from;
    while let Some(&(peer, Owed::Presence)) = owed.get(at) {
        let full =
            entries.len() >= MAX_LIVE_ENTRIES || (!entries.is_empty() && bytes >= MAX_LIVE_BYTES);
        if full || peer == to {
            break;
        }
        at += 1;
        if let Some(e) = entry(peer) {
            bytes += len(&e);
            entries.push(e);
        }
    }
    (at, entries)
}

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

/// What became of the connections behind on soft frames at one tick.
#[derive(Debug, Default)]
pub(super) struct CatchUps {
    /// Sent the list.
    pub caught_up: Vec<ConnId>,
    /// Still behind: not yet written what they had, or caught up too recently.
    pub still: Vec<ConnId>,
    /// Behind for too long with the socket taking nothing: to be closed.
    pub frozen: Vec<ConnId>,
    /// Whether the list was made (at most once, for all of them).
    pub list_made: bool,
}

/// Catches up each of `behind` that may be ([`Outbox::catch_up`]), every one with the same list:
/// `make` is called at most once, and only if one of them is caught up.
pub(super) fn catch_up_all<'a>(
    behind: impl IntoIterator<Item = (ConnId, &'a Outbox)>,
    now: Instant,
    every: Duration,
    frozen_after: Duration,
    make: impl FnOnce() -> Bytes,
) -> CatchUps {
    let mut make = Some(make);
    let mut list: Option<Bytes> = None;
    let mut done = CatchUps::default();
    for (id, outbox) in behind {
        if outbox.is_frozen(now, frozen_after) {
            done.frozen.push(id);
            continue;
        }
        let caught_up = outbox.catch_up(now, every, || {
            list.get_or_insert_with(|| make.take().map_or_else(Bytes::new, |make| make()))
                .clone()
        });
        if caught_up {
            done.caught_up.push(id);
        } else if outbox.is_behind() && !outbox.is_closed() {
            done.still.push(id);
        }
    }
    done.list_made = list.is_some();
    done
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
        let replaced = match msg {
            ClientMsg::Presence { data } => {
                let old = c
                    .presence_news
                    .replace(LiveEntry::new(&c.client, Some(&data)));
                c.presence = Some(data);
                old.is_some()
            }
            ClientMsg::Draft { .. } if c.read_only.is_some() => {
                return metrics::presence_dropped();
            }
            ClientMsg::Draft { data } => {
                let old = c
                    .draft_news
                    .replace(LiveEntry::new(&c.client, data.as_ref()));
                c.draft = data;
                c.draft_frame = None;
                old.is_some()
            }
            _ => return,
        };
        if replaced {
            // (Relayed is counted when it goes out.)
            metrics::presence_coalesced();
        }
        if !had_news {
            self.live_news.push(conn);
        }
    }

    /// Relays the presence and drafts that came since the last relay, if that was a tick
    /// ([`super::Hooks::live_tick`], [`LIVE_TICK`]) ago or there was none (otherwise sets when the
    /// next is due), catches up connections that were behind on soft frames and have written
    /// theirs, and sends drafts to those that caught up. Called after every event the room handles.
    pub(super) fn relay_live_when_due(&mut self) {
        if self.live_news.is_empty() && self.behind.is_empty() && self.owing.is_empty() {
            self.live_due = None;
            return;
        }
        let tick = self.state.room_hooks.live_tick;
        let now = Instant::now();
        if let Some(due) = self.live_sent.map(|sent| sent + tick)
            && now < due
        {
            self.live_due = Some(due);
            return;
        }
        self.live_sent = Some(now);
        self.relay_live();
        self.catch_up_behind(now);
        self.send_owed();
        // Those still behind, or still owed presence or drafts, are looked at again a tick later.
        let again = !self.behind.is_empty() || !self.owing.is_empty();
        self.live_due = again.then_some(now + tick);
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
        if recipients > 0 {
            metrics::presence_relayed(chunk.len());
        }
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

    /// Catches up every connection behind on soft frames that has written everything it had and
    /// was not caught up in the last [`super::Hooks::catch_up_every`]: one `peer.list` for all of
    /// them, which is what the frames they missed came to (drafts aside: they are owed, and follow
    /// in [`Room::send_owed`]). Closes those frozen behind.
    fn catch_up_behind(&mut self, now: Instant) {
        if self.behind.is_empty() {
            return;
        }
        let hooks = &self.state.room_hooks;
        let behind: Vec<ConnId> = self.behind.drain().collect();
        let conns = &self.conns;
        let outboxes = behind
            .iter()
            .filter_map(|id| conns.get(id).map(|c| (*id, &*c.outbox)));
        let mut without_presence = Vec::new();
        let done = catch_up_all(
            outboxes,
            now,
            hooks.catch_up_every,
            hooks.frozen_after,
            || {
                let (list, without) = self.peer_list();
                without_presence = without;
                list
            },
        );
        self.behind = done.still.into_iter().collect();
        for id in done.frozen {
            if let Some(c) = self.conns.get(&id) {
                c.outbox.close_now(super::close::TOO_SLOW, "too slow");
                self.slow.push(id);
            }
        }
        if done.caught_up.is_empty() {
            return;
        }
        // Owed to everyone caught up now: the presence the list had no room for, then every
        // draft, each in the order the peers joined.
        let mut drafting: Vec<(&ConnId, &super::Conn)> = self
            .conns
            .iter()
            .filter(|(_, c)| c.draft.is_some())
            .collect();
        drafting.sort_by_key(|(_, c)| c.joined);
        let owed: OwedList = without_presence
            .into_iter()
            .map(|id| (id, Owed::Presence))
            .chain(drafting.into_iter().map(|(id, _)| (*id, Owed::Draft)))
            .collect();
        if owed.is_empty() {
            return;
        }
        for id in done.caught_up {
            if let Some(c) = self.conns.get_mut(&id) {
                c.owed = Some((owed.clone(), 0));
                self.owing.insert(id);
            }
        }
    }

    /// Everyone as they are now, the recipients included, with no draft and with their presence
    /// up to [`PEER_LIST_PRESENCE`] of it (in the order they joined), encoded from what the room
    /// holds; and the peers past that whose presence it does not carry.
    fn peer_list(&self) -> (Bytes, Vec<ConnId>) {
        let mut all: Vec<(&ConnId, &super::Conn)> = self.conns.iter().collect();
        all.sort_by_key(|(_, c)| c.joined);
        let peers: Vec<PeerRef<'_>> = all
            .iter()
            .map(|(_, c)| PeerRef {
                client_id: &c.client,
                user_id: &c.user,
                name: &c.name,
                colour: &c.colour,
                read_only: c.read_only.is_some(),
                presence: c.presence.as_ref(),
            })
            .collect();
        self.encodes.fetch_add(1, Ordering::Relaxed);
        let (list, with_presence) = wire::encode_peer_list_within(&peers, PEER_LIST_PRESENCE);
        let without = all[with_presence..]
            .iter()
            .filter(|(_, c)| c.presence.is_some())
            .map(|(id, _)| **id)
            .collect();
        (list, without)
    }

    /// Sends the connections that caught up what they are owed, each peer's as it is now, as much
    /// as each has room for; the rest at a later tick. Presence goes in frames of up to
    /// [`MAX_LIVE_ENTRIES`] ([`MAX_LIVE_BYTES`]), made once per tick for every connection at the
    /// same place in the same list (a frame ends just before the connection's own entry, and the
    /// next begins after it, so every frame made may be sent to any connection at that place it
    /// does not name); each draft as a `draft` frame made once while anyone is owed it. A
    /// connection that fell behind again is owed nothing more (its next catch-up owes it again),
    /// and nobody is sent its own.
    fn send_owed(&mut self) {
        if self.owing.is_empty() {
            return;
        }
        let mut made = MadeFrames::new();
        for id in self.owing.drain().collect::<Vec<_>>() {
            let Some(c) = self.conns.get_mut(&id) else {
                continue;
            };
            let Some((owed, mut next)) = c.owed.take() else {
                continue;
            };
            let outbox = c.outbox.clone();
            if outbox.is_behind() || outbox.is_closed() {
                continue;
            }
            let mut sent = 0;
            while let Some(&(peer, kind)) = owed.get(next) {
                let (after, frame, entries) = match kind {
                    Owed::Presence => shared_frame(&mut made, &owed, next, id, || {
                        self.presence_frame(&owed, next, id)
                    }),
                    Owed::Draft => (next + 1, self.draft_frame(peer, id), 1),
                };
                let Some(frame) = frame else {
                    // Gone, itself, or nothing any more (it never knew the old one).
                    next = after;
                    continue;
                };
                if !outbox.push_soft_with_room(frame) {
                    break;
                }
                sent += entries;
                next = after;
            }
            metrics::presence_owed_sent(sent);
            if next < owed.len()
                && let Some(c) = self.conns.get_mut(&id)
            {
                c.owed = Some((owed, next));
                self.owing.insert(id);
            }
        }
        // Nobody is owed a draft any more: the frames made for them go.
        if self.owing.is_empty() {
            for c in self.conns.values_mut() {
                c.draft_frame = None;
            }
        }
    }

    /// A frame of the presence owed from place `from` of `owed`, as it is now
    /// ([`presence_range`]: never `to`'s own, nor that of peers gone). Answers the place after it,
    /// the frame (none if it carries nothing) and how many entries it has.
    fn presence_frame(
        &self,
        owed: &[(ConnId, Owed)],
        from: usize,
        to: ConnId,
    ) -> (usize, Option<Bytes>, usize) {
        let entry = |peer| {
            let p = self.conns.get(&peer)?;
            Some(LiveEntry::new(&p.client, Some(p.presence.as_ref()?)))
        };
        let (at, entries) = presence_range(owed, from, to, entry, LiveEntry::encoded_len);
        if entries.is_empty() {
            return (at, None, 0);
        }
        self.encodes.fetch_add(1, Ordering::Relaxed);
        let refs: Vec<&LiveEntry> = entries.iter().collect();
        (at, Some(wire::encode_live(&refs, &[])), entries.len())
    }

    /// `peer`'s draft as it is now, as a `draft` frame for `to` (made once while anyone is owed
    /// it); none for `to` itself, a peer gone or one with no draft.
    fn draft_frame(&mut self, peer: ConnId, to: ConnId) -> Option<Bytes> {
        if peer == to {
            return None;
        }
        let p = self.conns.get_mut(&peer)?;
        let draft = p.draft.as_ref()?;
        let client = &p.client;
        let encodes = &self.encodes;
        Some(
            p.draft_frame
                .get_or_insert_with(|| {
                    encodes.fetch_add(1, Ordering::Relaxed);
                    wire::encode_live(&[], &[&LiveEntry::new(client, Some(draft))])
                })
                .clone(),
        )
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

    /// Many readers behind at once: one list, made once, the same bytes in every queue.
    #[test]
    fn readers_caught_up_at_once_share_one_list() {
        let behind_and_drained = |outbox: &Outbox| {
            outbox.release(Bytes::from_static(b"welcome"));
            for _ in 0..=super::super::conn::MAX_SOFT {
                outbox.push_soft(Bytes::from_static(b"soft"));
            }
            assert!(outbox.is_behind());
            outbox.drain_for_tests();
        };
        let outboxes: Vec<_> = (0..1000).map(|_| Outbox::new(1000)).collect();
        for o in &outboxes {
            behind_and_drained(o);
        }
        // One more, still writing what it had.
        let late = Outbox::new(1000);
        late.release(Bytes::from_static(b"welcome"));
        for _ in 0..=super::super::conn::MAX_SOFT {
            late.push_soft(Bytes::from_static(b"soft"));
        }
        let (now, every) = (Instant::now(), Duration::from_secs(1));
        let mut made = 0;
        let list = Bytes::from(vec![7u8; 2 << 20]);
        let all = outboxes
            .iter()
            .chain([&late])
            .enumerate()
            .map(|(i, o)| (i as ConnId, &**o));
        let done = catch_up_all(all, now, every, Duration::from_secs(30), || {
            made += 1;
            list.clone()
        });
        assert_eq!(made, 1);
        assert!(done.list_made);
        assert_eq!(done.caught_up.len(), 1000);
        assert_eq!(done.still, vec![1000]);
        assert!(done.frozen.is_empty());
        for o in &outboxes {
            let queued = o.take_for_tests().expect("the list");
            assert_eq!(queued.as_ptr(), list.as_ptr(), "the same allocation");
        }
        // Nobody to catch up: nothing made.
        let done = catch_up_all(
            [(1000, &*late)],
            now,
            every,
            Duration::from_secs(30),
            || panic!("not made"),
        );
        assert!(!done.list_made && done.caught_up.is_empty());
    }

    /// Readers owed the same presence, some of them in it: each is sent every entry but its own, in
    /// order, in frames shared with every reader at the same place that they do not name; a frame
    /// ends just before a reader's own entry, and none is made for one reader alone.
    #[test]
    fn owed_presence_frames_end_before_a_readers_own_entry_and_are_shared() {
        // Peers 1 to 10 owed their presence, then two drafts.
        let owed: OwedList = (1..=10)
            .map(|p| (p, Owed::Presence))
            .chain([(1, Owed::Draft), (2, Owed::Draft)])
            .collect();
        let mut made = MadeFrames::new();
        let mut makes = 0;
        let mut first = Vec::new();
        for to in [3, 100, 7] {
            let mut got = Vec::new();
            let mut next = 0;
            while let Some(&(_, Owed::Presence)) = owed.get(next) {
                let (after, frame, n) = shared_frame(&mut made, &owed, next, to, || {
                    makes += 1;
                    let (after, peers) = presence_range(&owed, next, to, Some, |_| 1);
                    let frame = Bytes::from(peers.iter().map(|&p| p as u8).collect::<Vec<_>>());
                    (after, Some(frame).filter(|f| !f.is_empty()), peers.len())
                });
                if let Some(frame) = frame {
                    assert_eq!(frame.len(), n);
                    assert!(!frame.contains(&(to as u8)), "{to} sent its own");
                    if next == 0 {
                        first.push(frame.as_ptr());
                    }
                    got.extend_from_slice(&frame);
                }
                next = after;
            }
            assert_eq!(next, 10, "the drafts are not presence");
            let want: Vec<u8> = (1..=10).filter(|&p| p != to).map(|p| p as u8).collect();
            assert_eq!(got, want, "{to}");
        }
        // 3: 1-2, 4-10. 100: 1-2 (shared), 3-10. 7: 1-2 (shared), 3-6, 8-10.
        assert_eq!(makes, 5);
        assert!(first.iter().all(|&p| p == first[0]), "one frame for 1-2");
        // Within one entry's room, a frame is cut at the most it may carry.
        let many: OwedList = (1..=200).map(|p| (p, Owed::Presence)).collect();
        let (after, peers) = presence_range(&many, 0, 0, Some, |_| 1);
        assert_eq!((after, peers.len()), (MAX_LIVE_ENTRIES, MAX_LIVE_ENTRIES));
        // Peers with nothing (gone) are passed over.
        let (after, peers) = presence_range(&owed, 0, 0, |p| (p % 2 == 0).then_some(p), |_| 1);
        assert_eq!((after, peers), (10, vec![2, 4, 6, 8, 10]));
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
