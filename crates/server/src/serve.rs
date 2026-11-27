//! The serving loop: accepts connections, serves the app on them, and stops in order.
//!
//! [`serve`] is what the binary and the tests run. It differs from `axum::serve` in what a
//! server on the open internet needs:
//!
//! - **Every request knows its peer's address** (`ConnectInfo<SocketAddr>`), which the sign-in
//!   limits are kept by.
//! - **A request's head must arrive in time**: 30 s, or `request_timeout` if that is shorter. A
//!   client that opens a connection and sends nothing, or its headers a byte at a time, is cut
//!   off. (An idle kept-alive connection is closed after the same time.)
//! - **A request must be answered in time**: `request_timeout` (60 s unless set) from its head,
//!   or from the last of its body, to the beginning of its answer, or it is answered 503
//!   ([`within_time`], which the routes apply). A body that is arriving does not count against
//!   it, as long as it keeps up the minimum rate ([`crate::blobs::Pace`]): a document of 8 MB on
//!   a slow uplink is read for as long as it takes, a body that stops or trickles is not. What
//!   follows a commit still runs ([`crate::AppState::commit_then`]).
//! - **A client must take what it is sent**: when nothing at all could be written to a connection
//!   for `write_timeout` (60 s unless set), the connection is closed, and with it whatever the
//!   answer held (a file, a connection to the bucket, an export's place).
//! - **There are only so many connections**: `max_connections` (4,096 unless set), document
//!   connections included, counted from the moment one is accepted. When the server has them, a
//!   new one takes the place of the connection that has waited longest without finishing its
//!   first request head (someone holding sockets open by sending a byte now and then), or
//!   failing that the one idle longest; never one with a request in its handler or an answer
//!   still being sent (a download read slowly is busy, not idle), never a document connection.
//!   (A request that arrives on a connection just as it is chosen never reaches its handler: it
//!   is answered 503 [`FULL`], if the connection lasts long enough.) With none of those, it goes in a small reserve ([`HEALTH_RESERVE`])
//!   that answers `GET /api/health` alone (anything else: 503 [`FULL`]), so that a health check
//!   always gets through; with that full too, it is accepted and closed at once, and counted.
//!   (Not left unaccepted: the kernel's queue would fill and every later client would wait for a
//!   time it cannot know; a closed connection is an answer.) Keep the process's limit on open
//!   files above `max_connections` and its reserve.
//! - **One address holds only so many**: `max_connections_per_address` (512 unless set; 0: no
//!   limit; an IPv6 address counts as its /64). One more from it takes the place of that
//!   address's own connection that has waited longest for its first request head, or failing
//!   that its one idle longest (as above); it is closed at once only when none of the address's
//!   connections can go. A trusted proxy's address (`trusted_proxies`) is not limited: every
//!   client behind it shares it. Its connections are not counted for the addresses beside it in
//!   its /64 either, and are never closed to make room for them.
//!
//! # Long transfers
//!
//! The limits are made so that a transfer that moves is never cut for being long, and one that
//! stopped always is:
//!
//! | | while it moves | when it stops |
//! |---|---|---|
//! | blob upload | not under `request_timeout`; only so many at once, in `upload_memory` ([`crate::uploads`]); done within `max(upload_time_limit, size / upload_time_rate)` | no bytes for `blob_idle_timeout`, or slower than the minimum rate: 400, nothing stored |
//! | any other request body | does not count against `request_timeout` while it keeps up the minimum rate | 503 `request_timeout` after it stopped, or after it fell behind the rate |
//! | blob download, export | the handler has answered long before; the body is sent for as long as the client reads, and the connection counts as busy (never closed to make room) until it has been | nothing written for `write_timeout`: the connection is closed (and the storage's side after `blob_idle_timeout`, an export after its own stall time) |
//! | document connection | the upgrade is answered at once | the room closes a slow consumer; a socket that takes nothing for `write_timeout` is closed |
//!
//! # Stopping
//!
//! When `stop` resolves, [`serve`]:
//!
//! 1. stops accepting, and tells every connection to take no further request;
//! 2. lets the requests that are in their handlers finish, for up to half of `shutdown_timeout`,
//!    while everything still works (so a restart does not turn the edits in flight into
//!    errors); a connection without a request in its handler (idle, in the middle of sending
//!    one, or still being sent a long answer) is closed a moment later ([`LINGER`]);
//! 3. runs [`crate::AppState::shutdown_within`] with what is left: rooms are closed and
//!    snapshotted first, then the journal is written out;
//! 4. waits for the requests still in their handlers and for the document sockets to close.
//!
//! All of that is given `shutdown_timeout` (8 s unless set: `docker stop` kills after 10); what
//! has not finished by then is abandoned, and [`serve`] says so.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes, HttpBody};
use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use futures_util::StreamExt as _;
use hyper::body::{Frame, Incoming, SizeHint};
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, Sleep};
use tower_service::Service as _;

use crate::error::detail_response;
use crate::metrics::{HttpStats, OpenConnection};
use crate::state::AppState;

/// The longest a request's head may take to arrive (`request_timeout` if that is shorter).
pub const HEADER_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a connection with no request in its handler is kept once the server is stopping (to
/// finish sending an answer).
pub const LINGER: Duration = Duration::from_secs(1);

/// What a request that took longer than `request_timeout` is answered (503).
pub const TOO_SLOW: &str = "the request took too long; try again";

/// How [`serve`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stopped {
    /// Every room was closed, the journal written out and every connection ended.
    InOrder,
    /// `shutdown_timeout` ran out first: something was abandoned. (Nothing a client was told is
    /// durable is lost by that: a change is acknowledged only once it is stored.)
    OutOfTime,
}

/// Serves the app of `state` on `listener` until `stop` resolves, then stops in order (see the
/// module documentation). Must run inside a tokio runtime.
pub async fn serve(
    listener: TcpListener,
    state: AppState,
    stop: impl Future<Output = ()>,
) -> Stopped {
    let app = crate::app(state.clone());
    let settings = state.settings.clone();
    let limits = Limits {
        header: HEADER_TIMEOUT.min(Duration::from_secs_f64(settings.request_timeout)),
        write: Duration::from_secs_f64(settings.write_timeout),
    };
    let (closing, _) = watch::channel(false);
    let mut connections = JoinSet::new();
    let open = Arc::new(Conns::default());
    let mut trouble = AcceptTrouble::default();
    let mut stop = pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    // Counted from here, before anything else of it runs.
                    let trusted = settings
                        .trusted_proxies
                        .iter()
                        .any(|proxy| proxy.contains(peer.ip()));
                    let admitted = open.admit(
                        peer.ip(),
                        trusted,
                        settings.max_connections,
                        settings.max_connections_per_address,
                        &state.http,
                    );
                    let Some(conn) = admitted else {
                        // Closed at once (see the module documentation).
                        drop(stream);
                        continue;
                    };
                    connections.spawn(connection(
                        stream,
                        peer,
                        conn,
                        app.clone(),
                        limits,
                        state.http.clone(),
                        closing.subscribe(),
                    ));
                }
                Err(e) => accept_failed(&e, &open, &state.http, &mut trouble).await,
            },
            // (So that the set holds only the connections still open.)
            Some(_) = connections.join_next() => {}
        }
    }
    drop(listener);
    closing.send_replace(true);
    let bound = Duration::from_secs_f64(settings.shutdown_timeout);
    let stopping_at = Instant::now();
    let ending = async {
        // The requests in their handlers finish while the rooms, the journal and everything
        // they need still run.
        let handlers_done = async {
            while state.http.in_handlers() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        if tokio::time::timeout(bound / 2, handlers_done)
            .await
            .is_err()
        {
            tracing::warn!(
                requests = state.http.in_handlers(),
                "requests were still in their handlers {:?} after the stop; going on without them",
                bound / 2
            );
        }
        state
            .shutdown_within(bound.saturating_sub(stopping_at.elapsed()))
            .await;
        // Requests in their handlers are answered.
        while connections.join_next().await.is_some() {}
        // Document connections were closed by their rooms: a moment for their close frames to
        // go out and their sockets to close.
        let sockets_closed = async {
            while state.http.open() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let _ = tokio::time::timeout(LINGER, sockets_closed).await;
    };
    if tokio::time::timeout(bound, ending).await.is_ok() {
        Stopped::InOrder
    } else {
        tracing::warn!(
            connections = connections.len(),
            "the server did not stop in order within {bound:?}; ending anyway"
        );
        connections.abort_all();
        Stopped::OutOfTime
    }
}

/// What an `accept` that failed costs: nothing for a connection that went away before it was
/// accepted. Anything else is the system refusing a file descriptor (or memory) for one more
/// socket: one idle connection is closed to give its descriptor back, so that what waits to be
/// accepted (a health check, say) gets in, and the loop pauses a moment (a second when nothing
/// could be closed) rather than spin. The log says so at most once a minute ([`AcceptTrouble`]).
async fn accept_failed(
    e: &io::Error,
    open: &Conns,
    stats: &HttpStats,
    trouble: &mut AcceptTrouble,
) {
    use io::ErrorKind::{ConnectionAborted, ConnectionRefused, ConnectionReset};
    if matches!(
        e.kind(),
        ConnectionAborted | ConnectionRefused | ConnectionReset
    ) {
        return;
    }
    let freed = open.lock().make_room(|_| true, true, stats);
    if let Some(more) = trouble.note(Instant::now()) {
        tracing::error!(
            more,
            freed,
            "a connection could not be accepted: {e} (is the limit on open files, `ulimit -n`,              below max_connections?)"
        );
    }
    let pause = if freed { FREED_PAUSE } else { ACCEPT_PAUSE };
    tokio::time::sleep(pause).await;
}

/// How long the loop waits after an `accept` failed and nothing could be closed.
const ACCEPT_PAUSE: Duration = Duration::from_secs(1);
/// ... and after one was closed, for its socket to go.
const FREED_PAUSE: Duration = Duration::from_millis(100);
/// How often at most a failing `accept` is logged.
const ACCEPT_LOG_EVERY: Duration = Duration::from_secs(60);

/// Failed `accept`s, so that the log is not written once a second while they last.
#[derive(Debug, Default)]
struct AcceptTrouble {
    logged_at: Option<Instant>,
    unlogged: u64,
}

impl AcceptTrouble {
    /// One more failure at `now`: `Some(n)` when it is to be logged, `n` the failures not logged
    /// since the last line; `None` within [`ACCEPT_LOG_EVERY`] of it.
    fn note(&mut self, now: Instant) -> Option<u64> {
        if self
            .logged_at
            .is_some_and(|at| now.duration_since(at) < ACCEPT_LOG_EVERY)
        {
            self.unlogged += 1;
            return None;
        }
        self.logged_at = Some(now);
        Some(std::mem::take(&mut self.unlogged))
    }
}

#[derive(Debug, Clone, Copy)]
struct Limits {
    header: Duration,
    write: Duration,
}

/// Serves one connection to its end (or until it is evicted to make room for another).
async fn connection(
    stream: TcpStream,
    peer: SocketAddr,
    conn: Admitted,
    app: Router,
    limits: Limits,
    stats: Arc<HttpStats>,
    mut closing: watch::Receiver<bool>,
) {
    let (open, conn) = (conn.open, conn.registered);
    let this = conn.0.clone();
    let in_handlers = stats.clone();
    let io = TokioIo::new(TakenInTime::new(stream, limits.write, stats, open, conn));
    let reserved = this.reserved;
    let seen = this.clone();
    let service = hyper::service::service_fn(move |mut request: Request<Incoming>| {
        request.extensions_mut().insert(ConnectInfo(peer));
        let mut app = app.clone();
        // `None`: the connection was just chosen to make room for another, and is closing.
        let handling = Handling::begin(&seen, &in_handlers);
        // A connection taken in the reserve is for the health check alone.
        let refused = reserved && request.uri().path() != HEALTH;
        let seen = seen.clone();
        async move {
            let Some((handling, in_handler)) = handling else {
                return Ok(detail_response(StatusCode::SERVICE_UNAVAILABLE, FULL));
            };
            if refused {
                return Ok(detail_response(StatusCode::SERVICE_UNAVAILABLE, FULL));
            }
            let response = app.call(request).await?;
            if response.status() == StatusCode::SWITCHING_PROTOCOLS {
                seen.upgraded.store(true, Ordering::SeqCst);
            }
            drop(in_handler);
            // Busy until the answer's body has been sent (or dropped): a long download is not
            // idle, whatever its pace.
            Ok::<_, std::convert::Infallible>(response.map(|body| handling.carried_by(body)))
        }
    });
    let mut served = pin!(
        http1::Builder::new()
            .timer(TokioTimer::new())
            .header_read_timeout(limits.header)
            .keep_alive(!reserved)
            .serve_connection(io, service)
            .with_upgrades()
    );
    // (A connection's end is its client's business: a reset, a timeout above, a bad request.)
    tokio::select! {
        _ = served.as_mut() => return,
        // Made room for another (see `Conns::admit`): dropping `served` closes the socket.
        () = this.evicted.notified() => return,
        _ = closing.wait_for(|closing| *closing) => {}
    }
    // Stopping: the request in its handler is answered; a connection without one is closed once
    // it has had a moment to be sent what was being written.
    served.as_mut().graceful_shutdown();
    loop {
        tokio::select! {
            _ = served.as_mut() => return,
            () = tokio::time::sleep(LINGER) => {
                if this.in_handler.load(Ordering::SeqCst) == 0 {
                    return;
                }
            }
        }
    }
}

/// The path that connections in the reserve may ask for.
const HEALTH: &str = "/api/health";

/// What a connection in the reserve that asks for anything but [`HEALTH`] is answered (503).
pub const FULL: &str = "the server has as many connections as it holds; try again in a moment";

/// How many connections beyond `max_connections` the server takes for `GET /api/health` alone
/// (each is closed after one answer), so that a health check gets through when every connection
/// is busy.
pub const HEALTH_RESERVE: usize = 8;

/// A request on a connection, from its head to the end of its answer's body: while one is held
/// the connection is busy and is never closed to make room for another ([`Conns::admit`]). It is
/// carried in the answer's body ([`Handling::carried_by`]), so a download or an export that the
/// client reads slowly, or not at all, stays busy until the body has been sent or dropped; one
/// that stopped is ended by `write_timeout` ([`TakenInTime`]), which drops the body.
struct Handling(Arc<Conn>);

/// A request in its handler: counted on its connection (a stop waits for it) and in
/// [`HttpStats::in_handlers`], until this is dropped.
struct HandlerRunning {
    conn: Arc<Conn>,
    _counted: crate::metrics::InHandler,
}

impl Drop for HandlerRunning {
    fn drop(&mut self) {
        self.conn.in_handler.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Handling {
    /// A request whose head has arrived on `conn`; `None` when the connection has just been
    /// chosen to make room for another (it is closing: the request is answered 503 [`FULL`]
    /// rather than cut in the middle of its handler).
    fn begin(conn: &Arc<Conn>, stats: &Arc<HttpStats>) -> Option<(Self, HandlerRunning)> {
        conn.headed.store(true, Ordering::SeqCst);
        // One atomic step against `Conns::claim`'s: either the eviction sees this request and
        // passes the connection by, or this sees the eviction.
        if conn.busy.fetch_add(1, Ordering::SeqCst) & EVICTING != 0 {
            conn.busy.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        conn.in_handler.fetch_add(1, Ordering::SeqCst);
        let running = HandlerRunning {
            conn: conn.clone(),
            _counted: stats.handling(),
        };
        Some((Self(conn.clone()), running))
    }

    /// `body`, holding this until it has ended or is dropped.
    fn carried_by(self, body: Body) -> Body {
        if body.is_end_stream() {
            return body;
        }
        Body::new(Answering {
            body,
            handling: Some(self),
        })
    }
}

impl Drop for Handling {
    fn drop(&mut self) {
        self.0.busy.fetch_sub(1, Ordering::SeqCst);
        self.0.touch();
    }
}

/// An answer's body with its request's [`Handling`], let go at the body's end.
struct Answering {
    body: Body,
    handling: Option<Handling>,
}

impl HttpBody for Answering {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let polled = Pin::new(&mut self.body).poll_frame(cx);
        if matches!(polled, Poll::Ready(None | Some(Err(_)))) {
            self.handling = None;
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

/// The bit of [`Conn::busy`] that says the connection was chosen to make room for another.
const EVICTING: usize = 1 << (usize::BITS - 1);

/// What one address is counted as for `max_connections_per_address`: an IPv4 address, or an
/// IPv6 address's /64 (what one subscriber is usually given; counting each of its addresses
/// apart would make the limit no limit). The sign-in, reset and OAuth limits count clients the
/// same way.
pub(crate) fn counted_as(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V6(v6) => IpAddr::V6(Ipv6Addr::from(u128::from(v6) & !(u128::MAX >> 64))),
        v4 => v4,
    }
}

// ------------------------------------------------------------------ how many connections

/// One open connection, as [`Conns`] sees it.
#[derive(Debug)]
struct Conn {
    id: u64,
    /// Its address as the per-address limit counts it ([`counted_as`]), or `None` for a trusted
    /// proxy: not counted, and never closed to make room for its address.
    address: Option<IpAddr>,
    accepted: Instant,
    /// Taken in the reserve: it may ask for [`HEALTH`] alone.
    reserved: bool,
    /// A request head has arrived on it.
    headed: AtomicBool,
    /// How many of its requests are being handled or answered ([`Handling`]; one at most, on
    /// HTTP/1), with [`EVICTING`] set once it is chosen to make room.
    busy: AtomicUsize,
    /// How many of its requests are in their handlers.
    in_handler: AtomicUsize,
    /// It became a document connection (a WebSocket): never evicted.
    upgraded: AtomicBool,
    /// When anything last moved on it, in milliseconds after `base`.
    active_ms: AtomicU64,
    base: Instant,
    /// Chosen to make room for another: its task closes it.
    evicted: Notify,
}

impl Conn {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.base.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Something moved on it.
    fn touch(&self) {
        if !self.upgraded.load(Ordering::Relaxed) {
            self.active_ms.store(self.now_ms(), Ordering::Relaxed);
        }
    }

    /// Whether it may be closed to make room for another: no request being handled or answered,
    /// not a document connection, not already chosen.
    fn evictable(&self) -> bool {
        self.busy.load(Ordering::SeqCst) == 0 && !self.upgraded.load(Ordering::SeqCst)
    }
}

/// The open connections, for the limits on how many ([`Conns::admit`]).
#[derive(Debug)]
struct Conns {
    base: Instant,
    inner: Mutex<ConnsInner>,
}

impl Default for Conns {
    fn default() -> Self {
        Self {
            base: Instant::now(),
            inner: Mutex::default(),
        }
    }
}

#[derive(Debug, Default)]
struct ConnsInner {
    next: u64,
    all: HashMap<u64, Arc<Conn>>,
    by_address: HashMap<IpAddr, usize>,
    /// Those chosen to make room, still closing: no longer counted.
    evicted: HashSet<u64>,
    /// How many are counted (not chosen to make room), outside the reserve and in it.
    regular: usize,
    reserve: usize,
}

impl ConnsInner {
    /// One fewer counted, of the kind `conn` is and from its address.
    fn uncount(&mut self, conn: &Conn) {
        if conn.reserved {
            self.reserve -= 1;
        } else {
            self.regular -= 1;
        }
        if let Some(address) = conn.address
            && let Some(n) = self.by_address.get_mut(&address)
        {
            *n -= 1;
            if *n == 0 {
                self.by_address.remove(&address);
            }
        }
    }
}

/// A connection [`Conns::admit`] took: counted until its socket closes.
struct Admitted {
    open: OpenConnection,
    registered: Registered,
}

/// A connection in [`Conns`], until this is dropped (with its socket).
struct Registered(Arc<Conn>, Arc<Conns>);

impl Drop for Registered {
    fn drop(&mut self) {
        let mut inner = self.1.lock();
        let conn = &self.0;
        inner.all.remove(&conn.id);
        // (One chosen to make room was uncounted then.)
        if !inner.evicted.remove(&conn.id) {
            inner.uncount(conn);
        }
    }
}

impl Conns {
    fn lock(&self) -> std::sync::MutexGuard<'_, ConnsInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes a connection just accepted from `ip`, or answers `None` when it must be closed at
    /// once. A connection may be closed to make room for it (never one with a request being
    /// handled or answered, never a document connection): the one that has waited longest
    /// without finishing its first request head, or failing that the one idle longest.
    ///
    /// - An address that is not a `trusted` proxy holds at most `per_address` (0: any number;
    ///   an IPv6 address counts as its /64). At that, one of the address's own connections makes
    ///   room; with none that can, the new one is closed. A trusted proxy's connections are not
    ///   counted for its address (or its /64), and never make room for it.
    /// - The server holds `max` and [`HEALTH_RESERVE`] more for the health check. When it has
    ///   `max`, one of the others makes room. With none that can it goes in the reserve; with
    ///   the reserve full, it takes the place of a reserve connection that has sent nothing, or
    ///   is closed.
    fn admit(
        self: &Arc<Self>,
        ip: IpAddr,
        trusted: bool,
        max: usize,
        per_address: usize,
        stats: &Arc<HttpStats>,
    ) -> Option<Admitted> {
        let address = (!trusted).then(|| counted_as(ip));
        let mut inner = self.lock();
        if let Some(address) = address
            && per_address > 0
            && inner.by_address.get(&address).copied().unwrap_or(0) >= per_address
            && !inner.make_room(|c| c.address == Some(address), true, stats)
        {
            stats.refused_for_address();
            return None;
        }
        let reserved = if inner.regular < max || inner.make_room(|c| !c.reserved, true, stats) {
            false
        } else if inner.reserve < HEALTH_RESERVE || inner.make_room(|c| c.reserved, false, stats) {
            true
        } else {
            stats.refused();
            return None;
        };
        let conn = Arc::new(Conn {
            id: inner.next,
            address,
            accepted: Instant::now(),
            reserved,
            headed: AtomicBool::new(false),
            busy: AtomicUsize::new(0),
            in_handler: AtomicUsize::new(0),
            upgraded: AtomicBool::new(false),
            active_ms: AtomicU64::new(0),
            base: self.base,
            evicted: Notify::new(),
        });
        conn.touch();
        inner.next += 1;
        if reserved {
            inner.reserve += 1;
        } else {
            inner.regular += 1;
        }
        inner.all.insert(conn.id, conn.clone());
        if let Some(address) = address {
            *inner.by_address.entry(address).or_default() += 1;
        }
        drop(inner);
        Some(Admitted {
            open: stats.opened(),
            registered: Registered(conn, self.clone()),
        })
    }
}

impl ConnsInner {
    /// Closes one of the connections `among` to make room, and answers whether there was one:
    /// the one that has waited longest for its first request head; failing that, if `idle_too`,
    /// the one idle longest.
    fn make_room(
        &mut self,
        among: impl Fn(&Conn) -> bool,
        idle_too: bool,
        stats: &HttpStats,
    ) -> bool {
        // (A connection whose request began between the choice and the claim is passed by.)
        while let Some(victim) = self.victim(&among, idle_too) {
            if victim
                .busy
                .compare_exchange(0, EVICTING, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.evicted.insert(victim.id);
                self.uncount(&victim);
                victim.evicted.notify_one();
                stats.evicted();
                return true;
            }
        }
        false
    }

    /// The connection [`ConnsInner::make_room`] would close.
    fn victim(&self, among: &impl Fn(&Conn) -> bool, idle_too: bool) -> Option<Arc<Conn>> {
        let candidates = || {
            self.all
                .values()
                .filter(|c| among(c) && c.evictable() && !self.evicted.contains(&c.id))
        };
        let fresh = candidates()
            .filter(|c| !c.headed.load(Ordering::SeqCst))
            .min_by_key(|c| c.accepted);
        if fresh.is_some() || !idle_too {
            return fresh.cloned();
        }
        candidates()
            .min_by_key(|c| c.active_ms.load(Ordering::Relaxed))
            .cloned()
    }
}

/// The routes' time limit: a request whose answer has not begun `request_timeout` after its
/// head arrived, or after the last of its body did, is answered 503 [`TOO_SLOW`], and whatever
/// it was doing is dropped (what follows a commit goes on by itself). The log names the route,
/// never the address asked for (which can hold a reset token).
///
/// While the body arrives the clock is put forward: every byte earns `1 / min_upload_rate`
/// seconds, up to a full `request_timeout` from now ([`BodyClock`]). So a large body on a slow
/// uplink is not cut for being slow, and one that stops, or sends a byte now and then, is.
pub(crate) async fn within_time(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let limit = Duration::from_secs_f64(state.settings.request_timeout);
    let method = req.method().clone();
    let route = req.extensions().get::<MatchedPath>().cloned();
    let clock = Arc::new(BodyClock::new(limit, state.settings.min_upload_rate));
    let req = req.map(|body| clock.clone().watch(body));
    let mut answering = pin!(next.run(req));
    loop {
        tokio::select! {
            response = answering.as_mut() => return response,
            () = tokio::time::sleep_until(clock.deadline()) => {
                // (The body may have put the clock forward meanwhile.)
                if clock.deadline() > Instant::now() {
                    continue;
                }
            }
        }
        state.http.request_timed_out();
        tracing::warn!(
            %method,
            route = route.as_ref().map_or("", MatchedPath::as_str),
            "a request was not answered within {limit:?} of its head or its body and was given \
             up on"
        );
        return detail_response(StatusCode::SERVICE_UNAVAILABLE, TOO_SLOW);
    }
}

/// When a request must be answered by: `limit` after its head; put forward by its body as that
/// arrives; `limit` after the body's end.
struct BodyClock {
    limit: Duration,
    /// Bytes a second the body must keep up to put the clock forward; 0: any byte gives a full
    /// `limit`.
    floor: u64,
    deadline: Mutex<Instant>,
}

impl BodyClock {
    fn new(limit: Duration, floor: u64) -> Self {
        Self {
            limit,
            floor,
            deadline: Mutex::new(Instant::now() + limit),
        }
    }

    fn deadline(&self) -> Instant {
        *self.deadline.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `bytes` more of the body arrived (`None`: its end).
    fn arrived(&self, bytes: Option<usize>) {
        let latest = Instant::now() + self.limit;
        let mut deadline = self.deadline.lock().unwrap_or_else(PoisonError::into_inner);
        *deadline = match bytes {
            Some(bytes) if self.floor > 0 => {
                let earned = Duration::from_secs_f64(bytes as f64 / self.floor as f64);
                (*deadline + earned).min(latest)
            }
            _ => latest,
        };
    }

    /// `body`, telling this clock of every chunk that arrives and of its end.
    fn watch(self: Arc<Self>, body: Body) -> Body {
        let chunks = body.into_data_stream();
        Body::from_stream(futures_util::stream::unfold(
            (chunks, self, false),
            |(mut chunks, clock, ended)| async move {
                if ended {
                    return None;
                }
                let next = chunks.next().await;
                match &next {
                    Some(Ok(chunk)) => clock.arrived(Some(chunk.len())),
                    // (An error ends the body as well: the handler has what it will get.)
                    Some(Err(_)) | None => clock.arrived(None),
                }
                let ended = !matches!(next, Some(Ok(_)));
                next.map(|item| (item, (chunks, clock, ended)))
            },
        ))
    }
}

/// A connection whose peer must take what is written to it: when a write has been waiting for
/// the peer for `timeout` with nothing taken at all, it fails with `TimedOut` (and every write
/// after it), which ends the connection. A write that gets anything through starts the time
/// again, so a slow reader is never cut, only one that stopped.
struct TakenInTime {
    stream: TcpStream,
    timeout: Duration,
    /// Runs while a write waits for the peer.
    waiting: Option<Pin<Box<Sleep>>>,
    /// A write waited for the whole of `timeout`: every write from then on fails.
    timed_out: bool,
    stats: Arc<HttpStats>,
    /// Counted as open for as long as the socket lives: as a document connection too, which
    /// outlives the HTTP connection it began as.
    _open: OpenConnection,
    /// The same, for the limits on how many ([`Conns`]); told when anything moves.
    conn: Registered,
}

impl TakenInTime {
    fn new(
        stream: TcpStream,
        timeout: Duration,
        stats: Arc<HttpStats>,
        open: OpenConnection,
        conn: Registered,
    ) -> Self {
        Self {
            stream,
            timeout,
            waiting: None,
            timed_out: false,
            _open: open,
            conn,
            stats,
        }
    }

    /// What a write that the stream answered with `outcome` answers.
    fn timed<T>(
        &mut self,
        cx: &mut Context<'_>,
        outcome: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        let failed = || {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "the peer took nothing of what was sent in time",
            )))
        };
        if self.timed_out {
            return failed();
        }
        if outcome.is_ready() {
            self.waiting = None;
            self.conn.0.touch();
            return outcome;
        }
        let timeout = self.timeout;
        let waiting = self
            .waiting
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(Instant::now() + timeout)));
        if waiting.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        // Counted once; the flag (not the timer, which must not be polled again once it has
        // fired) fails whatever is written after this.
        self.timed_out = true;
        self.waiting = None;
        self.stats.write_timed_out();
        failed()
    }
}

impl AsyncRead for TakenInTime {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let outcome = Pin::new(&mut self.stream).poll_read(cx, buf);
        if buf.filled().len() > before {
            self.conn.0.touch();
        }
        outcome
    }
}

impl AsyncWrite for TakenInTime {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let outcome = Pin::new(&mut self.stream).poll_write(cx, buf);
        self.timed(cx, outcome)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let outcome = Pin::new(&mut self.stream).poll_write_vectored(cx, bufs);
        self.timed(cx, outcome)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    /// Admits a connection from `from` to a server of `max` with `per_address` from one address.
    fn admit(
        conns: &Arc<Conns>,
        stats: &Arc<HttpStats>,
        from: &str,
        max: usize,
        per_address: usize,
    ) -> Option<Admitted> {
        conns.admit(ip(from), false, max, per_address, stats)
    }

    fn conn(admitted: &Admitted) -> &Arc<Conn> {
        &admitted.registered.0
    }

    fn chosen(admitted: &Admitted) -> bool {
        conn(admitted).busy.load(Ordering::SeqCst) & EVICTING != 0
    }

    #[tokio::test]
    async fn a_failing_accept_closes_an_idle_connection_to_free_its_descriptor() {
        let (conns, stats) = (Arc::new(Conns::default()), Arc::new(HttpStats::default()));
        let idle = admit(&conns, &stats, "203.0.113.7", 100, 0).unwrap();
        let busy = admit(&conns, &stats, "203.0.113.8", 100, 0).unwrap();
        let (_handling, _running) = Handling::begin(conn(&busy), &stats).unwrap();
        let mut trouble = AcceptTrouble::default();
        let out_of_files = io::Error::other("Too many open files (os error 24)");
        accept_failed(&out_of_files, &conns, &stats, &mut trouble).await;
        assert!(chosen(&idle) && !chosen(&busy));
        // A connection that went away before it was accepted closes nothing.
        let other_idle = admit(&conns, &stats, "203.0.113.9", 100, 0).unwrap();
        let reset = io::Error::from(io::ErrorKind::ConnectionReset);
        accept_failed(&reset, &conns, &stats, &mut trouble).await;
        assert!(!chosen(&other_idle));
    }

    #[test]
    fn a_failing_accept_is_logged_at_most_once_a_minute_with_what_was_not() {
        let mut trouble = AcceptTrouble::default();
        let start = Instant::now();
        assert_eq!(trouble.note(start), Some(0));
        for s in 1..60 {
            assert_eq!(trouble.note(start + Duration::from_secs(s)), None);
        }
        assert_eq!(trouble.note(start + Duration::from_secs(60)), Some(59));
        assert_eq!(trouble.note(start + Duration::from_secs(61)), None);
    }

    #[tokio::test]
    async fn an_address_at_its_limit_makes_room_from_its_own_and_is_refused_only_when_none_can_go()
    {
        let (conns, stats) = (Arc::new(Conns::default()), Arc::new(HttpStats::default()));
        let first = admit(&conns, &stats, "203.0.113.7", 100, 2).unwrap();
        let second = admit(&conns, &stats, "203.0.113.7", 100, 2).unwrap();
        let elsewhere = admit(&conns, &stats, "198.51.100.1", 100, 2).unwrap();
        // Both sent a head; the first was idle longer.
        conn(&first).headed.store(true, Ordering::SeqCst);
        conn(&second).headed.store(true, Ordering::SeqCst);
        conn(&first).active_ms.store(0, Ordering::SeqCst);
        conn(&second).active_ms.store(10, Ordering::SeqCst);
        // A third from there: the first makes room, nobody else's is touched.
        let third = admit(&conns, &stats, "203.0.113.7", 100, 2).unwrap();
        assert!(chosen(&first) && !chosen(&second) && !chosen(&elsewhere));
        // The newcomer, which has sent nothing, goes before the one that has.
        let fourth = admit(&conns, &stats, "203.0.113.7", 100, 2).unwrap();
        assert!(chosen(&third) && !chosen(&second));
        assert_eq!((stats.evictions(), stats.refusals_for_address()), (2, 0));
        // Both left busy: one more from there is refused, as nothing of the address can go.
        let busy = [&second, &fourth].map(|a| Handling::begin(conn(a), &stats).unwrap());
        assert!(admit(&conns, &stats, "203.0.113.7", 100, 2).is_none());
        assert_eq!((stats.evictions(), stats.refusals_for_address()), (2, 1));
        // A document connection cannot go either.
        drop(busy);
        conn(&second).upgraded.store(true, Ordering::SeqCst);
        conn(&fourth).upgraded.store(true, Ordering::SeqCst);
        assert!(admit(&conns, &stats, "203.0.113.7", 100, 2).is_none());
        assert_eq!(stats.refusals_for_address(), 2);
        // Another address is not held up by it; and a trusted proxy is not limited.
        assert!(admit(&conns, &stats, "198.51.100.1", 100, 2).is_some());
        let proxy = conns.admit(ip("203.0.113.7"), true, 100, 2, &stats);
        assert!(proxy.is_some());
        assert_eq!(stats.evictions(), 2);
    }

    #[tokio::test]
    async fn a_trusted_proxy_neither_counts_against_its_neighbours_nor_makes_room_for_them() {
        let (conns, stats) = (Arc::new(Conns::default()), Arc::new(HttpStats::default()));
        // A trusted proxy and an address beside it, in one /64.
        let proxy: Vec<Admitted> = (0..3)
            .map(|_| {
                conns
                    .admit(ip("2001:db8:1:2::1"), true, 100, 1, &stats)
                    .unwrap()
            })
            .collect();
        // The proxy's connections do not use up the neighbour's one place ...
        let first = admit(&conns, &stats, "2001:db8:1:2::9", 100, 1).unwrap();
        assert!(!proxy.iter().any(chosen) && !chosen(&first));
        assert_eq!(stats.evictions(), 0);
        // ... and at its limit the neighbour makes room from its own, never from the proxy's.
        let second = admit(&conns, &stats, "2001:db8:1:2::9", 100, 1).unwrap();
        assert!(chosen(&first) && !chosen(&second));
        assert!(!proxy.iter().any(chosen));
        // With nothing of its own that can go, the neighbour is refused; the proxy is untouched.
        let busy = Handling::begin(conn(&second), &stats).unwrap();
        assert!(admit(&conns, &stats, "2001:db8:1:2::9", 100, 1).is_none());
        assert!(!proxy.iter().any(chosen));
        assert_eq!((stats.evictions(), stats.refusals_for_address()), (1, 1));
        drop(busy);
    }

    #[tokio::test]
    async fn an_ipv6_address_counts_as_its_64() {
        let (conns, stats) = (Arc::new(Conns::default()), Arc::new(HttpStats::default()));
        let first = admit(&conns, &stats, "2001:db8:1:2::1", 100, 1).unwrap();
        // Another address of the same /64 shares its limit ...
        let second = admit(&conns, &stats, "2001:db8:1:2:ffff::9", 100, 1).unwrap();
        assert!(chosen(&first));
        // ... another /64 does not; and an IPv4 address written as IPv6 is that IPv4 address.
        let other = admit(&conns, &stats, "2001:db8:1:3::1", 100, 1).unwrap();
        let v4 = admit(&conns, &stats, "192.0.2.1", 100, 1).unwrap();
        let mapped = admit(&conns, &stats, "::ffff:192.0.2.1", 100, 1).unwrap();
        assert!(!chosen(&second) && !chosen(&other) && chosen(&v4) && !chosen(&mapped));
        assert_eq!(counted_as(ip("2001:db8:1:2:3:4:5:6")), ip("2001:db8:1:2::"));
    }

    #[tokio::test]
    async fn a_connection_chosen_to_make_room_takes_no_request_and_a_busy_one_is_never_chosen() {
        let (conns, stats) = (Arc::new(Conns::default()), Arc::new(HttpStats::default()));
        let busy = admit(&conns, &stats, "192.0.2.1", 2, 0).unwrap();
        let idle = admit(&conns, &stats, "192.0.2.2", 2, 0).unwrap();
        let (handling, running) = Handling::begin(conn(&busy), &stats).unwrap();
        // The handler has returned; the answer is still being sent: busy all the same.
        drop(running);
        assert_eq!(conn(&busy).in_handler.load(Ordering::SeqCst), 0);
        conn(&busy).active_ms.store(0, Ordering::SeqCst);
        conn(&idle).headed.store(true, Ordering::SeqCst);
        conn(&idle).active_ms.store(1_000_000, Ordering::SeqCst);
        let newcomer = admit(&conns, &stats, "192.0.2.3", 2, 0).unwrap();
        assert!(chosen(&idle) && !chosen(&busy) && !newcomer.registered.0.reserved);
        // A request that arrives on the chosen one is not begun (it is answered 503).
        assert!(Handling::begin(conn(&idle), &stats).is_none());
        assert_eq!(conn(&idle).in_handler.load(Ordering::SeqCst), 0);
        assert_eq!(stats.in_handlers(), 0);
        // Once its answer is sent, the busy one can make room.
        drop(handling);
        conn(&newcomer).headed.store(true, Ordering::SeqCst);
        conn(&newcomer).active_ms.store(u64::MAX, Ordering::SeqCst);
        let _later = admit(&conns, &stats, "192.0.2.4", 2, 0).unwrap();
        assert!(chosen(&busy));
    }

    #[tokio::test]
    async fn with_the_reserve_full_of_connections_that_asked_for_something_one_more_is_refused() {
        let (conns, stats) = (Arc::new(Conns::default()), Arc::new(HttpStats::default()));
        let only = admit(&conns, &stats, "192.0.2.1", 1, 0).unwrap();
        let _busy = Handling::begin(conn(&only), &stats).unwrap();
        let mut reserve = Vec::new();
        for i in 0..HEALTH_RESERVE {
            let taken = admit(&conns, &stats, &format!("192.0.2.{}", 10 + i), 1, 0).unwrap();
            assert!(conn(&taken).reserved);
            reserve.push(taken);
        }
        // One in the reserve that has sent nothing makes room ...
        let newcomer = admit(&conns, &stats, "192.0.2.100", 1, 0).unwrap();
        assert!(chosen(&reserve[0]) && conn(&newcomer).reserved);
        assert_eq!((stats.evictions(), stats.refusals()), (1, 0));
        // ... but not one that has asked for something, even idle: then one more is refused.
        for taken in reserve.iter().skip(1).chain([&newcomer]) {
            conn(taken).headed.store(true, Ordering::SeqCst);
        }
        assert!(admit(&conns, &stats, "192.0.2.101", 1, 0).is_none());
        assert_eq!((stats.evictions(), stats.refusals()), (1, 1));
    }
}
