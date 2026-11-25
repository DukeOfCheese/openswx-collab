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
//! The reader decodes frames (on the blocking pool when they are large) and hands messages to the
//! room; malformed ones are answered here, without the room.

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

/// The most frames a connection may have waiting, the one being written included.
pub const MAX_QUEUED: usize = 2000;
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

#[derive(Debug)]
struct State {
    queue: VecDeque<Bytes>,
    /// Bytes in `queue`.
    queued: usize,
    /// The length of the frame the writer is sending, if any.
    in_flight: Option<usize>,
    /// The writer waits: the welcome, which must go first, is not ready yet.
    held: bool,
    /// The close frame to send once the queue is written (or at once, when aborting).
    close: Option<(u16, String)>,
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
                in_flight: None,
                held: true,
                close: None,
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
        let waiting = s.queue.len() + usize::from(s.in_flight.is_some());
        // The bytes behind the oldest unwritten frame: everything queued when a frame is being
        // written, otherwise everything but the first queued one.
        let behind = match (s.in_flight, s.queue.front()) {
            (Some(_), _) => Some(s.queued),
            (None, Some(first)) => Some(s.queued - first.len()),
            (None, None) => None,
        };
        let over = behind.is_some_and(|b| b + frame.len() > self.max_behind);
        if waiting >= MAX_QUEUED || over {
            drop(s);
            self.close_now(close::TOO_SLOW, "too slow");
            return Pushed::TooSlow;
        }
        s.queued += frame.len();
        s.queue.push_back(frame);
        drop(s);
        self.wake.notify_one();
        Pushed::Queued
    }

    /// Puts `first` (the welcome) ahead of everything queued and lets the writer start.
    pub fn release(&self, first: Bytes) {
        let mut s = self.lock();
        if s.life == Life::Open {
            s.queued += first.len();
            s.queue.push_front(first);
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
        s.queue.clear();
        s.queued = 0;
        s.close = Some((code, reason.to_owned()));
        drop(s);
        self.life.send_replace(Life::Aborting);
        self.wake.notify_one();
    }

    /// Whether the connection is closing or closed.
    pub fn is_closed(&self) -> bool {
        self.lock().life != Life::Open
    }

    /// Bytes waiting to be written, the frame being written included.
    pub fn queued_bytes(&self) -> usize {
        let s = self.lock();
        s.queued + s.in_flight.unwrap_or(0)
    }

    /// Frames waiting to be written, the one being written included.
    pub fn queued_frames(&self) -> usize {
        let s = self.lock();
        s.queue.len() + usize::from(s.in_flight.is_some())
    }

    async fn next(&self) -> Next {
        loop {
            {
                let mut s = self.lock();
                if !s.held
                    && s.life != Life::Aborting
                    && let Some(frame) = s.queue.pop_front()
                {
                    s.queued -= frame.len();
                    s.in_flight = Some(frame.len());
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
        s.queue.clear();
        s.queued = 0;
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
                    sent = sink.send(Message::Binary(frame)) => sent.is_ok(),
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

/// An `error` frame.
pub fn error_frame(code: &str, message: &str) -> Bytes {
    wire::encode_server(&ServerMsg::Error {
        code: code.to_owned(),
        message: message.chars().take(MAX_ERROR_MESSAGE).collect(),
        request_id: None,
    })
}

/// Reads a connection's frames and hands its messages to the room, until the client goes away or the
/// server closes the connection; then tells the room it left.
pub async fn read(
    mut stream: SplitStream<WebSocket>,
    conn: ConnId,
    outbox: Arc<Outbox>,
    room: RoomHandle,
    max: usize,
) {
    let mut life = outbox.life.subscribe();
    loop {
        let next = tokio::select! {
            next = stream.next() => next,
            _ = life.wait_for(|l| *l != Life::Open) => break,
        };
        let frame = match next {
            Some(Ok(Message::Binary(frame))) => frame,
            Some(Ok(Message::Text(_))) => {
                let message = "frames must be binary MessagePack".to_owned();
                if malformed(&room, conn, &outbox, "bad_message", message).await {
                    continue;
                }
                break;
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            Some(Err(_)) => {
                // The WebSocket layer could not read a frame: malformed, or longer than the one
                // byte over the limit it lets through (see [ead_limit]).
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
            match tokio::task::spawn_blocking(move || wire::decode_client(&frame, max)).await {
                Ok(decoded) => decoded,
                Err(_) => Err(WireError::Invalid("the frame could not be read".into())),
            }
        } else {
            wire::decode_client(&frame, max)
        };
        match decoded {
            Err(WireError::TooLarge) => {
                let message = "send large data as blobs".to_owned();
                if !malformed(&room, conn, &outbox, "too_large", message).await {
                    break;
                }
            }
            Err(WireError::Invalid(why)) => {
                if !malformed(&room, conn, &outbox, "bad_message", why).await {
                    break;
                }
            }
            // A second hello is read and ignored.
            Ok(ClientMsg::Hello { .. }) => {}
            Ok(msg) => {
                if room.frame(conn, msg, size).await.is_err() {
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
/// to the connection's earlier messages); alse if the room is gone.
async fn malformed(
    room: &RoomHandle,
    conn: ConnId,
    outbox: &Outbox,
    code: &'static str,
    message: String,
) -> bool {
    let message: String = message.chars().take(MAX_ERROR_MESSAGE).collect();
    if room.malformed(conn, code, message).await.is_err() {
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
        let mut s = outbox.lock();
        let f = s.queue.pop_front()?;
        s.queued -= f.len();
        s.in_flight = Some(f.len());
        Some(f.len())
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
