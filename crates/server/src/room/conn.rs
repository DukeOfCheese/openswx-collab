//! One WebSocket connection: its outgoing queue, the task that writes it and the loop that reads it.
//!
//! The room never awaits a socket. It pushes already encoded frames ([`Bytes`], shared between every
//! recipient of a broadcast) into the connection's [`Outbox`], and a writer task per connection
//! sends them. A connection that does not keep up is closed with
//! [`close::TOO_SLOW`] as soon as its queue breaks a limit (inventory Â§5
//! "Conn"):
//!
//! - more than [`MAX_QUEUED`] frames waiting; or
//! - the bytes waiting **behind** the oldest unwritten frame, plus the new frame, over
//!   `4 × max_change_bytes`. The oldest frame may be any size (a welcome carries the whole document),
//!   so one big frame is never too slow by itself.
//!
//! A frame waits until the socket has flushed it: frames the writer has handed the WebSocket layer
//! but not yet flushed (up to [`MAX_PER_WRITE`] at once) still count. So what one connection holds
//! of frames that must arrive is at most its oldest frame plus `4 × max_change_bytes`; with the soft
//! frames below (at most [`MAX_SOFT_BYTES`] plus one frame of any size) and one catch-up, that is
//! all the queue holds. (The WebSocket layer's own buffer is not counted twice: it holds frames
//! that are still counted here.)
//!
//! Frames the client can do without, who is here (`peer.joined`, `peer.left`), `presence` and
//! `draft`, are **soft** ([`Outbox::push_soft`]): they count for neither limit, and a connection
//! with more than [`MAX_SOFT`] of them (or [`MAX_SOFT_BYTES`]) waiting falls behind on them instead
//! of being closed. Soft frames are then dropped until it has written everything it had (what must
//! arrive included) and the room has sent it the peers as they are now (`peer.list`,
//! [`Outbox::catch_up`], at most once a [`super::CATCH_UP_EVERY`]), which counts for no limit. One
//! that has been behind for [`super::FROZEN_AFTER`] is closed with [`close::TOO_SLOW`] once the
//! socket has not finished what it was handed for that long, and that long has also passed since
//! a link of [`SLOWEST_READER`] bytes a second would have read every frame it was handed (what
//! must arrive, soft frames and catch-ups alike), flushed or not (the socket's own buffers may
//! take a big frame at once, and then nothing while the client reads it): its client stopped
//! reading. A reader on a slow link taking a big welcome or change, or a full batch of drafts, is
//! not frozen. Otherwise a connection is only closed for what must arrive (answers, changes,
//! locks); frames are written in the order they were queued, whatever their kind.
//!
//! A known residual: because that allowance counts what was handed, flushed or not, a client that
//! read a burst quickly keeps the unused part of its allowance, and may then stop reading for that
//! long beyond [`super::Hooks::frozen_after`] before it is taken for frozen. Each time a flush
//! returns, what is left is cut to the time the slowest link takes over
//! [`MAX_UNREAD_AFTER_FLUSH`] (4 MiB, 256 seconds at 16 KiB a second); frames still being written
//! keep their full time. So the allowance is never more than 256 s after the client stopped
//! taking frames (its last flush returned), plus the time of what it was handed since, and a
//! client that stops reading is taken for frozen at most `frozen_after + 4 MiB / 16 KiB/s` (under
//! 5 minutes at the defaults) after it stopped taking frames, plus the time of what it was handed
//! since. It holds nothing more meanwhile: its queues keep their limits, and a frame that must
//! arrive still closes it when they are broken.
//!
//! A connection already closing gracefully ([`Outbox::close`]: its queued frames, then the close
//! frame) is never taken for frozen: a client that stops reading then is ended by the server's
//! socket write timeout (`write_timeout`, 60 s by default), which closes any connection whose
//! client takes nothing for that long.
//!
//! The reader decodes frames (on the blocking pool when they are large) and hands messages to the
//! room; malformed ones are answered in their turn through the room. Every frame read counts as
//! hearing from the client ([`Heard`]: a connection silent for `lock_ttl` loses its locks). A
//! `presence` or `draft` frame within the 512 KiB any non-change frame may be, whose content the
//! codec refuses (over 16 KB or 128 KB, NaN or binary inside, not a map), is dropped silently:
//! presence is never answered (the Python server dropped oversize and unencodable ones silently
//! too). A longer one is answered `too_large`, like any frame too long for its kind (as Python
//! answered a frame over its frame limit).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use oswx_core::wire::{self, ClientMsg, ServerMsg, WireError};
use tokio::sync::{Notify, watch};

use super::{RoomHandle, close};
use crate::clock::Clock;

/// The most frames a connection may have waiting, the one being written included (soft frames
/// not counted).
pub const MAX_QUEUED: usize = 2000;
/// The most soft frames a connection may have waiting before it is behind on them.
pub const MAX_SOFT: usize = 64;
/// The most bytes of soft frames a connection may have waiting before it is behind on them (one
/// frame of any size may always wait alone).
pub const MAX_SOFT_BYTES: usize = 1024 * 1024;
/// The most frames the writer hands the socket with one flush.
const MAX_PER_WRITE: usize = 64;
/// The slowest link (bytes a second) a reader may be on and never be taken for frozen: every frame
/// handed to the socket (soft ones too) is given this long to be read, over and above
/// [`super::Hooks::frozen_after`] ([`super::Hooks::slowest_reader`] by default).
pub const SLOWEST_READER: usize = 16 * 1024;
/// The most bytes a reader is given time for once the socket has flushed what it was handed: a
/// reader that read a burst quickly does not bank the rest of its allowance beyond this.
pub const MAX_UNREAD_AFTER_FLUSH: usize = 4 * 1024 * 1024;
/// How long closing may wait for the last frames and the close frame to be written.
const CLOSE_WAIT: Duration = Duration::from_secs(5);
/// After the server closed a connection, how long the reader keeps reading (and dropping) what the
/// client still sends, so that its close frame is not lost to a reset.
const LINGER: Duration = Duration::from_secs(5);
/// Frames longer than this are decoded on the blocking pool.
pub const DECODE_OFF_TASK: usize = 256 * 1024;
/// The longest `message` of an `error` about a malformed frame.
const MAX_ERROR_MESSAGE: usize = 300;

/// A connection, as rooms and their commands name it. Unique within the process.
pub type ConnId = u64;

/// A new connection id.
pub fn next_id() -> ConnId {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// When a connection was last heard from (any frame), in the clock's epoch seconds. Shared by its
/// reader, which notes every frame, and the room, which expires the locks of silent clients.
#[derive(Debug)]
pub struct Heard {
    clock: Clock,
    at: AtomicU64,
}

impl Heard {
    /// Heard from now.
    pub fn new(clock: Clock) -> Arc<Self> {
        let at = AtomicU64::new(clock.now().to_bits());
        Arc::new(Self { clock, at })
    }

    /// Notes a frame.
    pub fn touch(&self) {
        self.at.store(self.clock.now().to_bits(), Ordering::Relaxed);
    }

    /// When the last frame came.
    pub fn at(&self) -> f64 {
        f64::from_bits(self.at.load(Ordering::Relaxed))
    }
}

/// Where a connection is in its life, as its reader and writer see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Life {
    Open,
    /// Closing: the frames already queued are written, then the close frame.
    Closing,
    /// Closing now: what was queued is dropped, and a frame being written may be abandoned.
    Aborting,
}

/// What [`Outbox::push`] did with a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pushed {
    Queued,
    /// The connection is closing or closed; the frame was dropped.
    Closed,
    /// The frame broke a limit: the connection is now closing with `TOO_SLOW` and the caller should
    /// treat it as gone.
    TooSlow,
}

/// What [`Outbox::push_soft`] did with a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Soft {
    Queued,
    /// The connection is closing or closed; the frame was dropped.
    Closed,
    /// The connection is behind on soft frames: this one was dropped, and so will every soft frame
    /// be until [`Outbox::catch_up`] says it may catch up.
    Behind,
}

/// The queues a frame waits in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// What must arrive.
    Hard,
    /// What may be dropped.
    Soft,
    /// The catch-up (`peer.list`).
    List,
}

#[derive(Debug)]
struct State {
    /// Frames that must arrive (answers, changes, locks), each with its place in the order of all
    /// frames. The first `handed.0` of them are with the socket, not yet flushed.
    queue: VecDeque<(u64, Bytes)>,
    /// Bytes in `queue`.
    queued: usize,
    /// Frames that may be dropped for a connection that does not keep up (who is here, presence
    /// and drafts), in the same order as `queue`. The first `handed.1` are with the socket.
    soft: VecDeque<(u64, Bytes)>,
    /// Bytes in `soft`.
    soft_queued: usize,
    /// The catch-up of a connection that was behind (`peer.list`), which counts for neither limit,
    /// and whether it is with the socket.
    list: Option<(u64, Bytes)>,
    /// How many frames of `queue` and of `soft` the writer has handed the socket since its last
    /// flush, and whether `list` is one of them. They stay counted until the flush returns.
    handed: (usize, usize, bool),
    /// Since when the writer has been writing what it handed (it has not flushed since).
    writing_since: Option<Instant>,
    /// When a reader on a link of [`SLOWEST_READER`] would have read every frame handed to the
    /// socket so far (soft ones and catch-ups too), flushed or not: the socket's own buffers may
    /// take a big frame at once, and then nothing more while the client reads it.
    read_by: Option<Instant>,
    /// The link `read_by` is reckoned at, bytes a second ([`SLOWEST_READER`]).
    slowest_reader: usize,
    /// Soft frames were dropped: none is queued until the room has sent what replaces them.
    behind: bool,
    /// Since when it has been behind.
    behind_since: Option<Instant>,
    /// When it last caught up.
    caught_up_at: Option<Instant>,
    /// The place of the next frame queued.
    next: u64,
    /// The writer waits: the welcome, which must go first, is not ready yet.
    held: bool,
    /// The close frame to send once the queue is written (or at once, when aborting).
    close: Option<(u16, String)>,
    /// The code the connection was last closed with (kept after the close frame is sent).
    closed_with: Option<u16>,
    life: Life,
}

/// A connection's outgoing queue. The room pushes into it without waiting; the writer task drains
/// it.
#[derive(Debug)]
pub struct Outbox {
    state: Mutex<State>,
    wake: Notify,
    life: watch::Sender<Life>,
    max_behind: usize,
}

/// What the writer does next.
enum Next {
    Frame(Bytes),
    Close(u16, String),
}

impl Outbox {
    /// A queue whose writer waits for [`Outbox::release`] (the welcome) before it sends anything.
    /// `max_behind` is the byte limit described in the module documentation.
    pub fn new(max_behind: usize) -> Arc<Self> {
        Self::with_slowest_reader(max_behind, SLOWEST_READER)
    }

    /// [`Outbox::new`], giving a reader the time a link of `slowest_reader` bytes a second takes
    /// to read what it is handed (instead of [`SLOWEST_READER`]).
    pub fn with_slowest_reader(max_behind: usize, slowest_reader: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                queued: 0,
                soft: VecDeque::new(),
                soft_queued: 0,
                list: None,
                handed: (0, 0, false),
                writing_since: None,
                read_by: None,
                slowest_reader: slowest_reader.max(1),
                behind: false,
                behind_since: None,
                caught_up_at: None,
                // 0 is the welcome's, ahead of everything.
                next: 1,
                held: true,
                close: None,
                closed_with: None,
                life: Life::Open,
            }),
            wake: Notify::new(),
            life: watch::Sender::new(Life::Open),
            max_behind,
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues a frame, or closes the connection with `TOO_SLOW` if the frame would break a limit.
    pub fn push(&self, frame: Bytes) -> Pushed {
        let mut s = self.lock();
        if s.life != Life::Open {
            return Pushed::Closed;
        }
        // Soft frames count for nothing here: they have limits of their own. Frames handed to the
        // socket stay in the queue until its flush returns, so they count.
        let waiting = s.queue.len();
        // The bytes behind the oldest unwritten frame (being written or not): everything queued
        // but the first.
        let behind = s.queue.front().map(|(_, first)| s.queued - first.len());
        let over = behind.is_some_and(|b| b + frame.len() > self.max_behind);
        if waiting >= MAX_QUEUED || over {
            drop(s);
            self.close_now(close::TOO_SLOW, "too slow");
            return Pushed::TooSlow;
        }
        s.queued += frame.len();
        let at = s.next;
        s.next += 1;
        s.queue.push_back((at, frame));
        drop(s);
        self.wake.notify_one();
        Pushed::Queued
    }

    /// Queues a frame that may be dropped (who is here, presence, drafts) unless the connection is
    /// behind on them: more than [`MAX_SOFT`] soft frames or [`MAX_SOFT_BYTES`] waiting. Then this
    /// one and every later one is dropped, never closing the connection, until the room sends what
    /// replaces them ([`Outbox::catch_up`]).
    pub fn push_soft(&self, frame: Bytes) -> Soft {
        let mut s = self.lock();
        if s.life != Life::Open {
            return Soft::Closed;
        }
        if s.behind {
            return Soft::Behind;
        }
        if !Self::soft_fits(&s, frame.len(), MAX_SOFT, MAX_SOFT_BYTES) {
            s.behind = true;
            s.behind_since = Some(Instant::now());
            return Soft::Behind;
        }
        Self::queue_soft(&mut s, frame);
        drop(s);
        self.wake.notify_one();
        Soft::Queued
    }

    /// Queues a soft frame only if it leaves the connection room to spare: fewer than half of
    /// [`MAX_SOFT`] soft frames waiting, and at most half of [`MAX_SOFT_BYTES`] with it (or none
    /// waiting). It never makes the connection behind. Answers whether it queued the frame.
    pub fn push_soft_with_room(&self, frame: Bytes) -> bool {
        let mut s = self.lock();
        if s.life != Life::Open
            || s.behind
            || !Self::soft_fits(&s, frame.len(), MAX_SOFT / 2, MAX_SOFT_BYTES / 2)
        {
            return false;
        }
        Self::queue_soft(&mut s, frame);
        drop(s);
        self.wake.notify_one();
        true
    }

    /// Whether a soft frame of `len` bytes may join those waiting: fewer than `count` wait, and
    /// they and it come to at most `bytes`. The frame being written (the oldest handed to the
    /// socket) does not count toward the bytes, and one frame waiting alone may be any size.
    fn soft_fits(s: &State, len: usize, count: usize, bytes: usize) -> bool {
        let waiting = s.soft.len();
        let being_written = match (s.handed.1, s.soft.front()) {
            (1.., Some((_, f))) => f.len(),
            _ => 0,
        };
        waiting < count && (waiting == 0 || s.soft_queued - being_written + len <= bytes)
    }

    fn queue_soft(s: &mut State, frame: Bytes) {
        s.soft_queued += frame.len();
        let at = s.next;
        s.next += 1;
        s.soft.push_back((at, frame));
    }

    /// Whether the connection is behind on soft frames.
    pub fn is_behind(&self) -> bool {
        self.lock().behind
    }

    /// For a connection behind on soft frames that has written everything it had queued (what
    /// must arrive included), and has not caught up in the last `every`: queues `make()`, what
    /// replaces the frames dropped (the peers as they are now), and takes soft frames again.
    /// The catch-up counts for neither limit. Answers whether it did.
    pub fn catch_up(&self, now: Instant, every: Duration, make: impl FnOnce() -> Bytes) -> bool {
        let mut s = self.lock();
        let drained = s.queue.is_empty() && s.soft.is_empty() && s.list.is_none();
        let rested = s.caught_up_at.is_none_or(|at| now >= at + every);
        if s.life != Life::Open || !s.behind || !drained || !rested {
            return false;
        }
        let at = s.next;
        s.next += 1;
        s.list = Some((at, make()));
        s.behind = false;
        s.behind_since = None;
        s.caught_up_at = Some(now);
        drop(s);
        self.wake.notify_one();
        true
    }

    /// Whether the connection has been behind for `after` while the writer has not finished what
    /// it handed the socket in `after`, and `after` has also passed since a link of
    /// [`SLOWEST_READER`] would have read every frame it was handed, soft or not: a client that
    /// stopped reading (a frame stuck on its socket), not one on a slow link taking a big frame
    /// (a welcome, a change) or a full batch of drafts. Never for a connection already closing
    /// (the server's `write_timeout` ends one whose client stops reading then).
    pub fn is_frozen(&self, now: Instant, after: Duration) -> bool {
        let s = self.lock();
        let long_ago = |at: Option<Instant>| at.is_some_and(|at| now >= at + after);
        s.life == Life::Open
            && s.behind
            && long_ago(s.behind_since)
            && long_ago(s.writing_since)
            && s.read_by.is_none_or(|at| now >= at + after)
    }

    /// Puts `first` (the welcome) ahead of everything queued and lets the writer start.
    pub fn release(&self, first: Bytes) {
        let mut s = self.lock();
        if s.life == Life::Open {
            s.queued += first.len();
            s.queue.push_front((0, first));
        }
        s.held = false;
        drop(s);
        self.wake.notify_one();
    }

    /// Closes after the frames already queued are written. Later pushes are dropped.
    pub fn close(&self, code: u16, reason: &str) {
        let mut s = self.lock();
        if s.life != Life::Open {
            return;
        }
        s.life = Life::Closing;
        s.close = Some((code, reason.to_owned()));
        s.closed_with = Some(code);
        drop(s);
        self.life.send_replace(Life::Closing);
        self.wake.notify_one();
    }

    /// Closes at once: what is queued is dropped.
    pub fn close_now(&self, code: u16, reason: &str) {
        let mut s = self.lock();
        if s.life == Life::Aborting {
            return;
        }
        s.life = Life::Aborting;
        Self::drop_queued(&mut s);
        s.close = Some((code, reason.to_owned()));
        s.closed_with = Some(code);
        drop(s);
        self.life.send_replace(Life::Aborting);
        self.wake.notify_one();
    }

    fn drop_queued(s: &mut State) {
        s.queue.clear();
        s.queued = 0;
        s.soft.clear();
        s.soft_queued = 0;
        s.list = None;
        s.handed = (0, 0, false);
    }

    /// The close code the server closed the connection with, if it did.
    pub fn close_code(&self) -> Option<u16> {
        self.lock().closed_with
    }

    /// Whether the connection is closing or closed.
    pub fn is_closed(&self) -> bool {
        self.lock().life != Life::Open
    }

    /// Bytes waiting to be written: soft frames, the catch-up, and those handed to the socket
    /// but not yet flushed included.
    pub fn queued_bytes(&self) -> usize {
        let s = self.lock();
        s.queued + s.soft_queued + s.list.as_ref().map_or(0, |(_, f)| f.len())
    }

    /// Frames waiting to be written: soft frames, the catch-up, and those handed to the socket
    /// but not yet flushed included.
    pub fn queued_frames(&self) -> usize {
        let s = self.lock();
        s.queue.len() + s.soft.len() + usize::from(s.list.is_some())
    }

    /// Hands the socket the next frame not yet handed, in the order they were queued, whatever
    /// its kind. It stays queued (and counted) until [`Outbox::written`].
    fn hand(s: &mut State) -> Option<Bytes> {
        let (hard, soft, list) = s.handed;
        let place = |q: &VecDeque<(u64, Bytes)>, i: usize| q.get(i).map(|(at, _)| *at);
        let candidates = [
            (place(&s.queue, hard), Kind::Hard),
            (place(&s.soft, soft), Kind::Soft),
            (
                s.list.as_ref().filter(|_| !list).map(|(at, _)| *at),
                Kind::List,
            ),
        ];
        let (_, kind) = candidates
            .into_iter()
            .filter_map(|(at, kind)| at.map(|at| (at, kind)))
            .min_by_key(|(at, _)| *at)?;
        let frame = match kind {
            Kind::Hard => s.queue.get(hard).map(|(_, f)| f.clone()),
            Kind::Soft => s.soft.get(soft).map(|(_, f)| f.clone()),
            Kind::List => s.list.as_ref().map(|(_, f)| f.clone()),
        }?;
        match kind {
            Kind::Hard => s.handed.0 += 1,
            Kind::Soft => s.handed.1 += 1,
            Kind::List => s.handed.2 = true,
        }
        // Every frame handed, soft ones too: a reader must read a batch of drafts before anything
        // behind it, so a slow link is as slow on those.
        let now = Instant::now();
        let reading = Self::reading_time(frame.len(), s.slowest_reader);
        s.read_by = Some(s.read_by.map_or(now, |at| at.max(now)) + reading);
        s.writing_since.get_or_insert(now);
        Some(frame)
    }

    /// The next frame if one is ready to be written now.
    fn try_next(&self) -> Option<Bytes> {
        let mut s = self.lock();
        if s.held || s.life == Life::Aborting {
            return None;
        }
        Self::hand(&mut s)
    }

    async fn next(&self) -> Next {
        loop {
            {
                let mut s = self.lock();
                if !s.held
                    && s.life != Life::Aborting
                    && let Some(frame) = Self::hand(&mut s)
                {
                    return Next::Frame(frame);
                }
                if let Some((code, reason)) = s.close.take() {
                    return Next::Close(code, reason);
                }
            }
            self.wake.notified().await;
        }
    }

    /// The socket has flushed what it was handed: it leaves the queues.
    fn written(&self) {
        let mut s = self.lock();
        let (hard, soft, list) = std::mem::take(&mut s.handed);
        for _ in 0..hard {
            if let Some((_, f)) = s.queue.pop_front() {
                s.queued -= f.len();
            }
        }
        for _ in 0..soft {
            if let Some((_, f)) = s.soft.pop_front() {
                s.soft_queued -= f.len();
            }
        }
        if list {
            s.list = None;
        }
        s.writing_since = None;
        // Everything handed has been flushed (frames are handed only between flushes, so none is
        // still being written): the reader keeps at most the time the slowest link takes over
        // MAX_UNREAD_AFTER_FLUSH, rather than banking a busy session's allowance for hours.
        let now = Instant::now();
        let cap = now + Self::reading_time(MAX_UNREAD_AFTER_FLUSH, s.slowest_reader);
        s.read_by = s.read_by.map(|at| at.min(cap));
    }

    /// How long a link of `per_second` bytes a second takes to read `len` bytes.
    fn reading_time(len: usize, per_second: usize) -> Duration {
        Duration::from_secs_f64(len as f64 / per_second as f64)
    }

    /// Writes everything queued, as the writer would.
    #[cfg(test)]
    pub(super) fn drain_for_tests(&self) {
        while self.take_for_tests().is_some() {}
    }

    /// Writes the next frame, as the writer would, and answers it.
    #[cfg(test)]
    pub(super) fn take_for_tests(&self) -> Option<Bytes> {
        let frame = Self::hand(&mut self.lock());
        self.written();
        frame
    }

    /// The writer could not write: nothing more will be.
    fn dead(&self) {
        let mut s = self.lock();
        s.life = Life::Aborting;
        Self::drop_queued(&mut s);
        s.writing_since = None;
        drop(s);
        self.life.send_replace(Life::Aborting);
    }
}

/// The writer task of a connection: sends what the room queues, then the close frame.
pub async fn write(mut sink: SplitSink<WebSocket, Message>, outbox: Arc<Outbox>) {
    let mut life = outbox.life.subscribe();
    loop {
        match outbox.next().await {
            Next::Frame(frame) => {
                // A close that drops the queue also abandons a frame stuck on a socket nobody reads.
                let sent = tokio::select! {
                    sent = write_queued(&mut sink, &outbox, frame) => sent.is_ok(),
                    _ = life.wait_for(|l| *l == Life::Aborting) => true,
                };
                outbox.written();
                if !sent {
                    outbox.dead();
                    return;
                }
            }
            Next::Close(code, reason) => {
                let frame = CloseFrame {
                    code,
                    reason: reason.into(),
                };
                let _ =
                    tokio::time::timeout(CLOSE_WAIT, sink.send(Message::Close(Some(frame)))).await;
                return;
            }
        }
    }
}

/// Writes `first` and whatever else is queued by then (up to [`MAX_PER_WRITE`] frames) with one
/// flush: a burst of frames costs the socket one write, not one each. The frames stay counted
/// (queued) until the flush returns, so that what waits in the WebSocket layer's buffer counts
/// toward the connection's limits; the caller then says they are [`Outbox::written`].
async fn write_queued<S>(sink: &mut S, outbox: &Outbox, first: Bytes) -> Result<(), axum::Error>
where
    S: futures_util::Sink<Message, Error = axum::Error> + Unpin,
{
    crate::metrics::frame_out(first.len());
    sink.feed(Message::Binary(first)).await?;
    for _ in 1..MAX_PER_WRITE {
        let Some(frame) = outbox.try_next() else {
            break;
        };
        crate::metrics::frame_out(frame.len());
        sink.feed(Message::Binary(frame)).await?;
    }
    sink.flush().await
}

/// An `error` frame.
pub fn error_frame(code: &str, message: &str) -> Bytes {
    wire::encode_server(&ServerMsg::Error {
        code: code.to_owned(),
        message: message.chars().take(MAX_ERROR_MESSAGE).collect(),
        request_id: None,
    })
}

/// Decodes a client frame; on failure, also says whether it was a `presence` or `draft` frame.
fn decode(frame: &[u8], max: usize) -> Result<ClientMsg, (WireError, bool)> {
    wire::decode_client(frame, max).map_err(|e| {
        let live = matches!(
            wire::scan(frame, Some(wire::MAX_NODES)).map(|s| s.t),
            Ok(Some("presence" | "draft"))
        );
        (e, live)
    })
}

/// Reads a connection's frames and hands its messages to the room, until the client goes away or the
/// server closes the connection; then tells the room it left.
pub async fn read(
    mut stream: SplitStream<WebSocket>,
    conn: ConnId,
    outbox: Arc<Outbox>,
    heard: Arc<Heard>,
    room: RoomHandle,
    max: usize,
) {
    let mut life = outbox.life.subscribe();
    loop {
        let next = tokio::select! {
            next = stream.next() => next,
            _ = life.wait_for(|l| *l != Life::Open) => break,
            // The room's task ended without closing this connection (it panicked): the client
            // reconnects, and a fresh room loads what is durable.
            () = room.ended() => {
                outbox.close(close::MOVED, "reconnect");
                break;
            }
        };
        if matches!(next, Some(Ok(Message::Binary(_) | Message::Text(_)))) {
            heard.touch();
        }
        let frame = match next {
            Some(Ok(Message::Binary(frame))) => {
                crate::metrics::frame_in(frame.len());
                frame
            }
            Some(Ok(Message::Text(_))) => {
                let message = "frames must be binary MessagePack".to_owned();
                if malformed(&room, conn, &outbox, &mut life, "bad_message", message).await {
                    continue;
                }
                break;
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            Some(Err(_)) => {
                // The WebSocket layer could not read a frame: malformed, or longer than the one
                // byte over the limit it lets through (see [`read_limit`]).
                outbox.close(close::PROTOCOL_ERROR, "unreadable frame");
                break;
            }
            Some(Ok(Message::Close(_))) | None => break,
        };
        let size = frame.len();
        if size > max {
            // Read whole (so the close is not lost to a reset), then refused.
            outbox.close(close::TOO_BIG, "frame too large");
            break;
        }
        let decoded = if size > DECODE_OFF_TASK {
            match tokio::task::spawn_blocking(move || decode(&frame, max)).await {
                Ok(decoded) => decoded,
                Err(_) => Err((
                    WireError::Invalid("the frame could not be read".into()),
                    false,
                )),
            }
        } else {
            decode(&frame, max)
        };
        match decoded {
            // Presence is never answered: one whose content the codec refuses is dropped.
            Err((WireError::Invalid(_), true)) => crate::metrics::presence_dropped(),
            Err((WireError::TooLarge, _)) => {
                let message = "send large data as blobs".to_owned();
                if !malformed(&room, conn, &outbox, &mut life, "too_large", message).await {
                    break;
                }
            }
            Err((WireError::Invalid(why), false)) => {
                if !malformed(&room, conn, &outbox, &mut life, "bad_message", why).await {
                    break;
                }
            }
            // A second hello is read and ignored.
            Ok(ClientMsg::Hello { .. }) => {}
            Ok(msg) => {
                // Raced against the connection's life: a reader waiting on a full room queue
                // still stops at once when the connection is closed (its access ended).
                let handed = tokio::select! {
                    handed = room.frame(conn, msg, size) => handed,
                    _ = life.wait_for(|l| *l != Life::Open) => break,
                };
                if handed.is_err() {
                    outbox.close(close::MOVED, "reconnect");
                    break;
                }
            }
        }
    }
    room.leave(conn).await;
    // Let the close frame reach the client before the socket goes: read (and drop) what it still
    // sends until it answers the close or a few seconds pass.
    if outbox.is_closed() {
        let _ = tokio::time::timeout(LINGER, async {
            while let Some(Ok(msg)) = stream.next().await {
                if matches!(msg, Message::Close(_)) {
                    break;
                }
            }
        })
        .await;
    }
}

/// The largest frame the WebSocket layer is told to read: one byte over the protocol's limit, so
/// that a frame just over it is read whole and refused here with `TOO_BIG` (whose close frame then
/// reaches the client), while anything longer is refused by the layer without being read.
pub fn read_limit(max: usize) -> usize {
    max.saturating_add(1)
}

/// Hands an unreadable frame to the room, to be answered `error` in its turn (after the answers
/// to the connection's earlier messages); `false` if the room is gone.
async fn malformed(
    room: &RoomHandle,
    conn: ConnId,
    outbox: &Outbox,
    life: &mut watch::Receiver<Life>,
    code: &'static str,
    message: String,
) -> bool {
    let message: String = message.chars().take(MAX_ERROR_MESSAGE).collect();
    // (Raced against the connection's life, as a frame is.)
    let handed = tokio::select! {
        handed = room.malformed(conn, code, message) => handed,
        _ = life.wait_for(|l| *l != Life::Open) => return false,
    };
    if handed.is_err() {
        outbox.close(close::MOVED, "reconnect");
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(n: usize) -> Bytes {
        Bytes::from(vec![b'x'; n])
    }

    /// Takes the next frame as the writer would, leaving it in flight.
    fn start_writing(outbox: &Outbox) -> Option<usize> {
        Outbox::hand(&mut outbox.lock()).map(|f| f.len())
    }

    /// Puts a connection behind on soft frames, then writes everything it had.
    fn behind_and_drained(outbox: &Outbox) {
        for _ in 0..MAX_SOFT {
            outbox.push_soft(frame(2));
        }
        assert_eq!(outbox.push_soft(frame(2)), Soft::Behind);
        while start_writing(outbox).is_some() {
            outbox.written();
        }
    }

    /// A socket that takes every frame and never finishes flushing until `open`.
    #[derive(Default)]
    struct Stuck {
        fed: Vec<usize>,
        open: bool,
    }

    impl futures_util::Sink<Message> for Stuck {
        type Error = axum::Error;
        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), axum::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn start_send(mut self: std::pin::Pin<&mut Self>, m: Message) -> Result<(), axum::Error> {
            if let Message::Binary(b) = m {
                self.fed.push(b.len());
            }
            Ok(())
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), axum::Error>> {
            if self.open {
                std::task::Poll::Ready(Ok(()))
            } else {
                std::task::Poll::Pending
            }
        }
        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), axum::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn frames_handed_to_the_socket_count_until_its_flush_returns() {
        let outbox = Outbox::new(50);
        outbox.release(frame(10));
        assert_eq!(outbox.push(frame(20)), Pushed::Queued);
        assert_eq!(outbox.push(frame(30)), Pushed::Queued); // 50 behind the welcome
        outbox.push_soft(frame(5));
        let Next::Frame(first) = outbox.next().await else {
            panic!("a frame first");
        };
        let mut sink = Stuck::default();
        let stuck = tokio::time::timeout(
            Duration::from_millis(50),
            write_queued(&mut sink, &outbox, first),
        )
        .await;
        assert!(stuck.is_err(), "the flush never returns");
        assert_eq!(sink.fed, vec![10, 20, 30, 5], "all handed to the socket");
        // Nothing is flushed: everything still counts, toward the limits too.
        assert_eq!((outbox.queued_bytes(), outbox.queued_frames()), (65, 4));
        assert_eq!(
            outbox.push(frame(1)),
            Pushed::TooSlow,
            "51 behind the welcome"
        );
        // Once flushed they are gone.
        let outbox = Outbox::new(1000);
        outbox.release(frame(10));
        outbox.push(frame(20));
        let Next::Frame(first) = outbox.next().await else {
            panic!("a frame first");
        };
        let mut sink = Stuck {
            open: true,
            ..Stuck::default()
        };
        assert!(write_queued(&mut sink, &outbox, first).await.is_ok());
        assert_eq!(
            outbox.queued_frames(),
            2,
            "counted until the writer says so"
        );
        outbox.written();
        assert_eq!((outbox.queued_bytes(), outbox.queued_frames()), (0, 0));
    }

    #[test]
    fn one_big_frame_is_not_too_slow_but_a_backlog_behind_it_is() {
        let outbox = Outbox::new(1000);
        outbox.release(frame(5000)); // a big welcome: nothing behind it
        assert!(!outbox.is_closed());
        for _ in 0..9 {
            assert_eq!(outbox.push(frame(100)), Pushed::Queued); // 900 behind it
        }
        assert_eq!(outbox.push(frame(101)), Pushed::TooSlow); // 1001 behind it
        assert!(outbox.is_closed());
        assert_eq!(outbox.queued_bytes(), 0, "a too-slow close drops the queue");
        assert_eq!(outbox.push(frame(1)), Pushed::Closed);
    }

    #[test]
    fn a_big_frame_behind_others_is_still_too_slow() {
        let outbox = Outbox::new(1000);
        outbox.release(frame(10));
        assert_eq!(start_writing(&outbox), Some(10));
        assert_eq!(outbox.push(frame(1000)), Pushed::Queued);
        assert_eq!(outbox.push(frame(1)), Pushed::TooSlow);
        let outbox = Outbox::new(1000);
        outbox.release(frame(10));
        assert_eq!(outbox.push(frame(1001)), Pushed::TooSlow);
    }

    #[test]
    fn the_frame_count_is_bounded() {
        let outbox = Outbox::new(usize::MAX);
        outbox.release(frame(1));
        for _ in 1..MAX_QUEUED {
            assert_eq!(outbox.push(frame(1)), Pushed::Queued);
        }
        assert_eq!(outbox.queued_frames(), MAX_QUEUED);
        assert_eq!(outbox.push(frame(1)), Pushed::TooSlow);
    }

    #[test]
    fn the_count_returns_to_zero_once_written() {
        let outbox = Outbox::new(1000);
        outbox.release(frame(10));
        outbox.push(frame(20));
        while start_writing(&outbox).is_some() {
            outbox.written();
        }
        assert_eq!((outbox.queued_bytes(), outbox.queued_frames()), (0, 0));
    }

    #[test]
    fn soft_frames_never_close_a_connection_and_count_for_no_limit() {
        let outbox = Outbox::new(usize::MAX);
        outbox.release(frame(1));
        for _ in 1..MAX_QUEUED {
            assert_eq!(outbox.push(frame(1)), Pushed::Queued);
        }
        // The frame limit is reached by what must arrive only.
        for _ in 0..MAX_SOFT {
            assert_eq!(outbox.push_soft(frame(10)), Soft::Queued);
        }
        assert!(!outbox.is_behind());
        // One more soft frame: behind, not closed; it and the later ones are dropped.
        assert_eq!(outbox.push_soft(frame(10)), Soft::Behind);
        assert_eq!(outbox.push_soft(frame(1)), Soft::Behind);
        assert!(outbox.is_behind() && !outbox.is_closed());
        assert_eq!(outbox.queued_frames(), MAX_QUEUED + MAX_SOFT);
        // The byte limit of what must arrive counts no soft byte either.
        let outbox = Outbox::new(1000);
        outbox.release(frame(1));
        assert_eq!(outbox.push_soft(frame(900)), Soft::Queued);
        assert_eq!(outbox.push(frame(1000)), Pushed::Queued);
        assert!(!outbox.is_closed());
    }

    #[test]
    fn soft_bytes_are_bounded_but_one_frame_may_be_any_size() {
        let outbox = Outbox::new(1000);
        outbox.release(frame(1));
        assert_eq!(outbox.push_soft(frame(MAX_SOFT_BYTES * 3)), Soft::Queued);
        assert_eq!(outbox.push_soft(frame(1)), Soft::Behind);
        let outbox = Outbox::new(1000);
        outbox.release(frame(1));
        assert_eq!(outbox.push_soft(frame(MAX_SOFT_BYTES - 10)), Soft::Queued);
        assert_eq!(outbox.push_soft(frame(10)), Soft::Queued);
        assert_eq!(outbox.push_soft(frame(1)), Soft::Behind);
    }

    #[test]
    fn a_connection_behind_catches_up_once_it_has_written_everything() {
        let (now, every) = (Instant::now(), Duration::from_secs(1));
        let outbox = Outbox::new(1000);
        outbox.release(frame(1));
        assert_eq!(start_writing(&outbox), Some(1));
        outbox.written();
        assert!(!outbox.catch_up(now, every, || frame(5)), "not behind");
        for _ in 0..MAX_SOFT {
            outbox.push_soft(frame(2));
        }
        assert_eq!(outbox.push_soft(frame(2)), Soft::Behind);
        assert!(
            !outbox.catch_up(now, every, || frame(5)),
            "soft frames wait"
        );
        for _ in 0..MAX_SOFT - 1 {
            start_writing(&outbox);
            outbox.written();
        }
        assert_eq!(start_writing(&outbox), Some(2));
        assert!(
            !outbox.catch_up(now, every, || frame(5)),
            "one is being written"
        );
        outbox.written();
        // What must arrive must be written too.
        assert_eq!(outbox.push(frame(7)), Pushed::Queued);
        assert!(
            !outbox.catch_up(now, every, || frame(5)),
            "a change still waits"
        );
        assert_eq!(start_writing(&outbox), Some(7));
        assert!(!outbox.catch_up(now, every, || frame(5)), "and is written");
        outbox.written();
        assert!(outbox.catch_up(now, every, || frame(5)));
        assert!(!outbox.is_behind());
        assert_eq!(outbox.push_soft(frame(3)), Soft::Queued);
        assert_eq!(start_writing(&outbox), Some(5), "the catch-up goes first");
        outbox.written();
        assert_eq!(start_writing(&outbox), Some(3));
    }

    #[test]
    fn the_catch_up_counts_for_no_soft_limit() {
        let (now, every) = (Instant::now(), Duration::from_secs(1));
        let outbox = Outbox::new(1000);
        outbox.release(frame(1));
        behind_and_drained(&outbox);
        // A list far over the soft byte limit ...
        assert!(outbox.catch_up(now, every, || frame(3 * MAX_SOFT_BYTES)));
        // ... leaves the soft limits whole: as many soft frames as ever may wait behind it.
        for _ in 0..MAX_SOFT {
            assert_eq!(outbox.push_soft(frame(2)), Soft::Queued);
        }
        assert_eq!(outbox.push_soft(frame(2)), Soft::Behind);
        assert_eq!(outbox.queued_bytes(), 3 * MAX_SOFT_BYTES + 2 * MAX_SOFT);
        let outbox = Outbox::new(1000);
        outbox.release(frame(1));
        behind_and_drained(&outbox);
        assert!(outbox.catch_up(now, every, || frame(3 * MAX_SOFT_BYTES)));
        // Nor while it is being written.
        assert_eq!(start_writing(&outbox), Some(3 * MAX_SOFT_BYTES));
        assert_eq!(outbox.push_soft(frame(MAX_SOFT_BYTES - 1)), Soft::Queued);
        assert_eq!(outbox.push_soft(frame(1)), Soft::Queued);
        assert_eq!(outbox.push_soft(frame(1)), Soft::Behind);
    }

    #[test]
    fn a_connection_catches_up_at_most_once_a_period() {
        let (now, every) = (Instant::now(), Duration::from_secs(1));
        let outbox = Outbox::new(1000);
        outbox.release(frame(1));
        behind_and_drained(&outbox);
        assert!(outbox.catch_up(now, every, || frame(5)));
        behind_and_drained(&outbox);
        let soon = now + every - Duration::from_millis(1);
        assert!(!outbox.catch_up(soon, every, || frame(5)), "too soon");
        assert!(outbox.is_behind());
        assert!(outbox.catch_up(now + every, every, || frame(5)));
    }

    #[test]
    fn a_connection_behind_whose_socket_takes_nothing_is_frozen() {
        let after = Duration::from_secs(30);
        let outbox = Outbox::new(1000);
        outbox.release(frame(1));
        assert_eq!(start_writing(&outbox), Some(1)); // stuck on the socket
        let t0 = Instant::now();
        assert!(!outbox.is_frozen(t0 + 2 * after, after), "not behind");
        for _ in 0..=MAX_SOFT {
            outbox.push_soft(frame(2));
        }
        // (The welcome's one byte is read in no time even at the slowest link.)
        let since = Instant::now().max(outbox.lock().read_by.unwrap());
        assert!(outbox.is_behind());
        assert!(!outbox.is_frozen(since + after / 2, after));
        assert!(outbox.is_frozen(since + after, after));
        // A socket that takes frames is not frozen, however long the connection stays behind.
        outbox.written();
        assert!(!outbox.is_frozen(since + 2 * after, after), "it wrote");
        assert_eq!(start_writing(&outbox), Some(2));
        let again = Instant::now().max(outbox.lock().read_by.unwrap());
        assert!(!outbox.is_frozen(again + after / 2, after));
        assert!(outbox.is_frozen(again + after, after));
        outbox.close_now(close::TOO_SLOW, "too slow");
        assert!(!outbox.is_frozen(again + after, after), "closed");
    }

    /// A reader on a slow link taking a big frame is given the time the slowest link takes to read
    /// it, over and above `after`, even once the socket's buffers have taken it (flushed).
    #[test]
    fn a_big_frame_being_written_gives_the_reader_time_by_its_size() {
        let after = Duration::from_secs(1);
        let outbox = Outbox::new(usize::MAX);
        // A welcome four seconds long at the slowest link, and a change behind it.
        outbox.release(frame(4 * SLOWEST_READER));
        outbox.push(frame(SLOWEST_READER / 2));
        assert_eq!(start_writing(&outbox), Some(4 * SLOWEST_READER));
        assert_eq!(start_writing(&outbox), Some(SLOWEST_READER / 2));
        // A soft frame handed with them adds its own time.
        outbox.push_soft(frame(8 * SLOWEST_READER));
        assert_eq!(start_writing(&outbox), Some(8 * SLOWEST_READER));
        for _ in 0..=MAX_SOFT {
            outbox.push_soft(frame(2));
        }
        assert!(outbox.is_behind());
        let since = outbox.lock().writing_since.unwrap();
        let behind = outbox.lock().behind_since.unwrap();
        let read_by = outbox.lock().read_by.unwrap();
        assert!(read_by >= since + Duration::from_millis(12_500));
        assert!(!outbox.is_frozen(behind + after, after));
        let just_before = read_by + after - Duration::from_millis(1);
        assert!(!outbox.is_frozen(just_before, after));
        assert!(outbox.is_frozen(behind.max(read_by + after), after));
        // The socket's buffers took them all at once, and now take nothing more: the slowest
        // link is still reading them.
        outbox.written();
        assert_eq!(start_writing(&outbox), Some(2));
        let again = outbox.lock().writing_since.unwrap();
        assert!(!outbox.is_frozen(again + after, after));
        assert!(!outbox.is_frozen(just_before, after));
        // (The two bytes handed since add next to nothing.)
        let read_by = outbox.lock().read_by.unwrap();
        assert!(outbox.is_frozen(again.max(read_by) + after, after));
    }

    /// A reader on a slow link taking a full batch of soft frames (heavy drafts) is given the time
    /// the slowest link takes to read it too: soft bytes are bytes it must read before it can read
    /// anything else.
    #[test]
    fn a_soft_batch_being_written_gives_the_reader_time_by_its_size() {
        let after = Duration::from_secs(30);
        let outbox = Outbox::new(usize::MAX);
        outbox.release(frame(1));
        outbox.drain_for_tests();
        // 1 MiB of drafts: 64 seconds at the slowest link.
        outbox.push_soft(frame(MAX_SOFT_BYTES));
        assert_eq!(start_writing(&outbox), Some(MAX_SOFT_BYTES));
        for _ in 0..=MAX_SOFT {
            outbox.push_soft(frame(2));
        }
        assert!(outbox.is_behind());
        let since = outbox.lock().writing_since.unwrap();
        let behind = outbox.lock().behind_since.unwrap();
        let start = since.max(behind);
        assert!(!outbox.is_frozen(start + after, after), "still reading");
        let reading = Duration::from_secs(MAX_SOFT_BYTES as u64 / SLOWEST_READER as u64);
        assert!(!outbox.is_frozen(since + reading + after / 2, after));
        let read_by = outbox.lock().read_by.unwrap();
        assert!(read_by >= since + reading);
        assert!(outbox.is_frozen(start.max(read_by) + after, after));
    }

    /// A reader does not bank allowance without limit: once a flush returns, it keeps at most the
    /// time the slowest link takes over [`MAX_UNREAD_AFTER_FLUSH`], however much it was handed.
    #[test]
    fn a_flushed_burst_leaves_the_reader_at_most_the_cap() {
        let after = Duration::from_secs(30);
        let outbox = Outbox::new(usize::MAX);
        // 64 MiB handed: over an hour at the slowest link.
        outbox.release(frame(64 * 1024 * 1024));
        assert_eq!(start_writing(&outbox), Some(64 * 1024 * 1024));
        let flushed = Instant::now();
        outbox.written();
        // Then it stops reading: a frame stuck on the socket, and it falls behind.
        outbox.push(frame(2));
        assert_eq!(start_writing(&outbox), Some(2));
        for _ in 0..=MAX_SOFT {
            outbox.push_soft(frame(2));
        }
        assert!(outbox.is_behind());
        let cap = Duration::from_secs_f64(MAX_UNREAD_AFTER_FLUSH as f64 / SLOWEST_READER as f64);
        // It still has the whole cap's time to read what the socket's buffers took.
        let ms = Duration::from_millis(1);
        assert!(
            !outbox.is_frozen(flushed + cap + after - ms, after),
            "within the cap"
        );
        // (The two bytes handed since add next to nothing.)
        let by = Instant::now() + cap + after + Duration::from_millis(5);
        assert!(outbox.is_frozen(by, after), "frozen by now + cap + after");
    }

    /// A frame handed after the flush keeps its full reading time on top of the cap: the cap
    /// bounds only what was flushed already.
    #[test]
    fn a_frame_handed_after_the_flush_keeps_its_full_time() {
        let after = Duration::from_secs(30);
        let outbox = Outbox::new(usize::MAX);
        outbox.release(frame(64 * 1024 * 1024));
        assert_eq!(start_writing(&outbox), Some(64 * 1024 * 1024));
        let flushed = Instant::now();
        outbox.written();
        // 8 MiB handed after the flush: 512 s at the slowest link. Then it stops reading.
        let big = 8 * 1024 * 1024;
        outbox.push(frame(big));
        assert_eq!(start_writing(&outbox), Some(big));
        for _ in 0..=MAX_SOFT {
            outbox.push_soft(frame(2));
        }
        assert!(outbox.is_behind());
        let cap = Duration::from_secs_f64(MAX_UNREAD_AFTER_FLUSH as f64 / SLOWEST_READER as f64);
        let big_time = Duration::from_secs(512);
        assert_eq!(big_time, Outbox::reading_time(big, SLOWEST_READER));
        let ms = Duration::from_millis(1);
        assert!(
            !outbox.is_frozen(flushed + cap + big_time + after - ms, after),
            "the frame handed since keeps its time"
        );
        let by = Instant::now() + cap + big_time + after + Duration::from_millis(5);
        assert!(
            outbox.is_frozen(by, after),
            "frozen by now + cap + 512 s + after"
        );
    }

    #[test]
    fn a_frame_pushed_with_room_never_puts_a_connection_behind() {
        let outbox = Outbox::new(1000);
        outbox.release(frame(1));
        let mut queued = 0;
        while outbox.push_soft_with_room(frame(10)) {
            queued += 1;
        }
        assert_eq!(queued, MAX_SOFT / 2);
        assert!(!outbox.is_behind());
        // Room for the others: as many again may come before it is behind.
        for _ in queued..MAX_SOFT {
            assert_eq!(outbox.push_soft(frame(10)), Soft::Queued);
        }
        // Bytes: half the limit, but one frame alone may be any size.
        let outbox = Outbox::new(1000);
        assert!(outbox.push_soft_with_room(frame(3 * MAX_SOFT_BYTES)));
        assert!(!outbox.push_soft_with_room(frame(1)));
        let outbox = Outbox::new(1000);
        assert!(outbox.push_soft_with_room(frame(MAX_SOFT_BYTES / 2 - 1)));
        assert!(outbox.push_soft_with_room(frame(1)));
        assert!(!outbox.push_soft_with_room(frame(1)));
        assert!(!outbox.is_behind());
        behind_and_drained(&outbox);
        assert!(!outbox.push_soft_with_room(frame(1)), "behind");
    }

    #[test]
    fn frames_go_out_in_the_order_they_were_queued_soft_or_not() {
        let outbox = Outbox::new(1000);
        outbox.push_soft(frame(2));
        outbox.push(frame(3));
        outbox.push_soft(frame(4));
        outbox.push_soft(frame(5));
        outbox.push(frame(6));
        outbox.release(frame(1));
        let mut order = Vec::new();
        while let Some(n) = start_writing(&outbox) {
            outbox.written();
            order.push(n);
        }
        assert_eq!(order, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!((outbox.queued_bytes(), outbox.queued_frames()), (0, 0));
        // A close that drops the queue drops soft frames too.
        outbox.push_soft(frame(7));
        outbox.close_now(close::TOO_SLOW, "too slow");
        assert_eq!(outbox.queued_frames(), 0);
        assert_eq!(outbox.push_soft(frame(1)), Soft::Closed);
    }

    #[tokio::test]
    async fn the_writer_waits_for_the_welcome_and_sends_it_first() {
        let outbox = Outbox::new(1000);
        assert_eq!(outbox.push(frame(1)), Pushed::Queued);
        let early = tokio::time::timeout(Duration::from_millis(20), outbox.next()).await;
        assert!(early.is_err(), "nothing is written before the welcome");
        outbox.release(frame(7));
        match outbox.next().await {
            Next::Frame(f) => assert_eq!(f.len(), 7),
            Next::Close(..) => panic!("the welcome comes first"),
        }
        outbox.written();
        outbox.close(1000, "");
        assert!(matches!(outbox.next().await, Next::Frame(f) if f.len() == 1));
        outbox.written();
        assert!(matches!(outbox.next().await, Next::Close(1000, _)));
    }
}
