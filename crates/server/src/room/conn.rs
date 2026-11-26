//! One WebSocket connection: its outgoing queue, the task that writes it and the loop that reads it.
//!
//! The room never awaits a socket. It pushes already encoded frames ([`Bytes`], shared between every
//! recipient of a broadcast) into the connection's [`Outbox`], and a writer task per connection
//! sends them. A connection that does not keep up is closed with
//! [`close::TOO_SLOW`] as soon as its queue breaks a limit (inventory Â§5
//! "Conn"):
//!
//! - more than [`MAX_QUEUED`] frames waiting, the one being written included; or
//! - the bytes waiting **behind** the oldest unwritten frame, plus the new frame, over
//!   `4 Ã— max_change_bytes`. The oldest frame may be any size (a welcome carries the whole document),
//!   so one big frame is never too slow by itself, and what a socket can hold is that frame plus the
//!   limit.
//!
//! Frames the client can do without, who is here (`peer.joined`, `peer.left`), `presence` and
//! `draft`, are **soft** ([`Outbox::push_soft`]): they count for neither limit, and a connection
//! with more than [`MAX_SOFT`] of them (or [`MAX_SOFT_BYTES`]) waiting falls behind on them instead
//! of being closed. Soft frames are then dropped until it has written what it had and the room has
//! sent it the peers as they are now (`peer.list`, [`Outbox::catch_up`]). A connection is only
//! closed for what must arrive (answers, changes, locks); frames are written in the order they were
//! queued, soft or not.
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

#[derive(Debug)]
struct State {
    /// Frames that must arrive (answers, changes, locks), each with its place in the order of all
    /// frames.
    queue: VecDeque<(u64, Bytes)>,
    /// Bytes in `queue`.
    queued: usize,
    /// Frames that may be dropped for a connection that does not keep up (who is here, presence
    /// and drafts), in the same order as `queue`.
    soft: VecDeque<(u64, Bytes)>,
    /// Bytes in `soft`.
    soft_queued: usize,
    /// Soft frames were dropped: none is queued until the room has sent what replaces them.
    behind: bool,
    /// The place of the next frame queued.
    next: u64,
    /// The length of the frame the writer is sending, if any, and whether it is a soft one.
    in_flight: Option<(usize, bool)>,
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
        Arc::new(Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                queued: 0,
                soft: VecDeque::new(),
                soft_queued: 0,
                behind: false,
                // 0 is the welcome's, ahead of everything.
                next: 1,
                in_flight: None,
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
        // Soft frames count for nothing here: they have limits of their own.
        let writing = s.in_flight.filter(|(_, soft)| !soft).is_some();
        let waiting = s.queue.len() + usize::from(writing);
        // The bytes behind the oldest unwritten frame: everything queued when a frame is being
        // written, otherwise everything but the first queued one.
        let behind = match (writing, s.queue.front()) {
            (true, _) => Some(s.queued),
            (false, Some((_, first))) => Some(s.queued - first.len()),
            (false, None) => None,
        };
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
        let writing = s.in_flight.filter(|(_, soft)| *soft).is_some();
        let waiting = s.soft.len() + usize::from(writing);
        if waiting >= MAX_SOFT || (waiting > 0 && s.soft_queued + frame.len() > MAX_SOFT_BYTES) {
            s.behind = true;
            return Soft::Behind;
        }
        Self::queue_soft(&mut s, frame);
        drop(s);
        self.wake.notify_one();
        Soft::Queued
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

    /// For a connection behind on soft frames that has written every one it had queued: queues
    /// `make()`, what replaces the ones dropped (the peers as they are now), and takes soft frames
    /// again. Answers whether it did.
    pub fn catch_up(&self, make: impl FnOnce() -> Bytes) -> bool {
        let mut s = self.lock();
        let writing = s.in_flight.filter(|(_, soft)| *soft).is_some();
        if s.life != Life::Open || !s.behind || writing || !s.soft.is_empty() {
            return false;
        }
        Self::queue_soft(&mut s, make());
        s.behind = false;
        drop(s);
        self.wake.notify_one();
        true
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
    }

    /// The close code the server closed the connection with, if it did.
    pub fn close_code(&self) -> Option<u16> {
        self.lock().closed_with
    }

    /// Whether the connection is closing or closed.
    pub fn is_closed(&self) -> bool {
        self.lock().life != Life::Open
    }

    /// Bytes waiting to be written, soft frames and the frame being written included.
    pub fn queued_bytes(&self) -> usize {
        let s = self.lock();
        s.queued + s.soft_queued + s.in_flight.map_or(0, |(n, _)| n)
    }

    /// Frames waiting to be written, soft frames and the one being written included.
    pub fn queued_frames(&self) -> usize {
        let s = self.lock();
        s.queue.len() + s.soft.len() + usize::from(s.in_flight.is_some())
    }

    /// The next frame to write, in the order they were queued, soft or not.
    fn pop(s: &mut State) -> Option<Bytes> {
        let soft = match (s.queue.front(), s.soft.front()) {
            (Some((a, _)), Some((b, _))) => b < a,
            (None, Some(_)) => true,
            (_, None) => false,
        };
        let (_, frame) = if soft {
            let f = s.soft.pop_front()?;
            s.soft_queued -= f.1.len();
            f
        } else {
            let f = s.queue.pop_front()?;
            s.queued -= f.1.len();
            f
        };
        s.in_flight = Some((frame.len(), soft));
        Some(frame)
    }

    /// The next frame if one is ready to be written now.
    fn try_next(&self) -> Option<Bytes> {
        let mut s = self.lock();
        if s.held || s.life == Life::Aborting {
            return None;
        }
        Self::pop(&mut s)
    }

    async fn next(&self) -> Next {
        loop {
            {
                let mut s = self.lock();
                if !s.held
                    && s.life != Life::Aborting
                    && let Some(frame) = Self::pop(&mut s)
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

    fn written(&self) {
        self.lock().in_flight = None;
    }

    /// The writer could not write: nothing more will be.
    fn dead(&self) {
        let mut s = self.lock();
        s.life = Life::Aborting;
        Self::drop_queued(&mut s);
        s.in_flight = None;
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
/// flush: a burst of frames costs the socket one write, not one each. A frame counts as written
/// once the WebSocket layer has it.
async fn write_queued(
    sink: &mut SplitSink<WebSocket, Message>,
    outbox: &Outbox,
    first: Bytes,
) -> Result<(), axum::Error> {
    crate::metrics::frame_out(first.len());
    sink.feed(Message::Binary(first)).await?;
    for _ in 1..MAX_PER_WRITE {
        outbox.written();
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
        Outbox::pop(&mut outbox.lock()).map(|f| f.len())
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
    fn a_connection_behind_catches_up_once_it_has_written_its_soft_frames() {
        let outbox = Outbox::new(1000);
        outbox.release(frame(1));
        assert_eq!(start_writing(&outbox), Some(1));
        outbox.written();
        assert!(!outbox.catch_up(|| frame(5)), "not behind");
        for _ in 0..MAX_SOFT {
            outbox.push_soft(frame(2));
        }
        assert_eq!(outbox.push_soft(frame(2)), Soft::Behind);
        assert!(!outbox.catch_up(|| frame(5)), "soft frames still wait");
        for _ in 0..MAX_SOFT - 1 {
            start_writing(&outbox);
            outbox.written();
        }
        assert_eq!(start_writing(&outbox), Some(2));
        assert!(!outbox.catch_up(|| frame(5)), "one is being written");
        outbox.written();
        assert!(outbox.catch_up(|| frame(5)));
        assert!(!outbox.is_behind());
        assert_eq!(outbox.push_soft(frame(3)), Soft::Queued);
        assert_eq!(start_writing(&outbox), Some(5), "the catch-up goes first");
        outbox.written();
        assert_eq!(start_writing(&outbox), Some(3));
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
