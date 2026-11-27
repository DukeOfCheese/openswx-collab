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
//!   failing that the one idle longest; never one with a request in its handler, never a
//!   document connection. With none of those, it goes in a small reserve ([`HEALTH_RESERVE`])
//!   that answers `GET /api/health` alone (anything else: 503 [`FULL`]), so that a health check
//!   always gets through; with that full too, it is accepted and closed at once, and counted.
//!   (Not left unaccepted: the kernel's queue would fill and every later client would wait for a
//!   time it cannot know; a closed connection is an answer.) Keep the process's limit on open
//!   files above `max_connections` and its reserve.
//! - **One address holds only so many**: `max_connections_per_address` (512 unless set; 0: no
//!   limit). One more from it is closed at once. A trusted proxy's address (`trusted_proxies`)
//!   is not limited: every client behind it shares it.
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
//! | blob download, export | the handler has answered long before; the body is sent for as long as the client reads | nothing written for `write_timeout`: the connection is closed (and the storage's side after `blob_idle_timeout`, an export after its own stall time) |
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
use std::net::{IpAddr, SocketAddr};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use futures_util::StreamExt as _;
use hyper::body::Incoming;
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
                Err(e) => accept_failed(&e).await,
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
/// accepted; a second's pause for anything else (out of file descriptors, say), so that the loop
/// does not spin.
async fn accept_failed(e: &io::Error) {
    use io::ErrorKind::{ConnectionAborted, ConnectionRefused, ConnectionReset};
    if matches!(
        e.kind(),
        ConnectionAborted | ConnectionRefused | ConnectionReset
    ) {
        return;
    }
    tracing::error!("a connection could not be accepted: {e}");
    tokio::time::sleep(Duration::from_secs(1)).await;
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
        let handled = (Handling::begin(&seen), in_handlers.handling());
        // A connection taken in the reserve is for the health check alone.
        let refused = reserved && request.uri().path() != HEALTH;
        let seen = seen.clone();
        async move {
            if refused {
                return Ok(detail_response(StatusCode::SERVICE_UNAVAILABLE, FULL));
            }
            let response = app.call(request).await;
            if matches!(&response, Ok(r) if r.status() == StatusCode::SWITCHING_PROTOCOLS) {
                seen.upgraded.store(true, Ordering::SeqCst);
            }
            drop(handled);
            response
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
                if this.handling.load(Ordering::SeqCst) == 0 {
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

/// A request in its handler on a connection, counted until this is dropped.
struct Handling(Arc<Conn>);

impl Handling {
    fn begin(conn: &Arc<Conn>) -> Self {
        conn.headed.store(true, Ordering::SeqCst);
        conn.handling.fetch_add(1, Ordering::SeqCst);
        Self(conn.clone())
    }
}

impl Drop for Handling {
    fn drop(&mut self) {
        self.0.handling.fetch_sub(1, Ordering::SeqCst);
        self.0.touch();
    }
}

// ------------------------------------------------------------------ how many connections

/// One open connection, as [`Conns`] sees it.
#[derive(Debug)]
struct Conn {
    id: u64,
    ip: IpAddr,
    accepted: Instant,
    /// Taken in the reserve: it may ask for [`HEALTH`] alone.
    reserved: bool,
    /// A request head has arrived on it.
    headed: AtomicBool,
    /// How many of its requests are in their handlers (one at most, on HTTP/1).
    handling: AtomicUsize,
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

    /// Whether it may be closed to make room for another: no request in its handler, not a
    /// document connection, not already chosen.
    fn evictable(&self, chosen: &HashSet<u64>) -> bool {
        self.handling.load(Ordering::SeqCst) == 0
            && !self.upgraded.load(Ordering::SeqCst)
            && !chosen.contains(&self.id)
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
    /// One fewer counted, of the kind `conn` is.
    fn uncount(&mut self, conn: &Conn) {
        if conn.reserved {
            self.reserve -= 1;
        } else {
            self.regular -= 1;
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
        if !inner.evicted.remove(&conn.id) {
            inner.uncount(conn);
        }
        if let Some(n) = inner.by_address.get_mut(&conn.ip) {
            *n -= 1;
            if *n == 0 {
                inner.by_address.remove(&conn.ip);
            }
        }
    }
}

impl Conns {
    fn lock(&self) -> std::sync::MutexGuard<'_, ConnsInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes a connection just accepted from `ip`, or answers `None` when it must be closed at
    /// once:
    ///
    /// - an address that is not a `trusted` proxy holds at most `per_address` (0: any number);
    /// - the server holds `max` and [`HEALTH_RESERVE`] more for the health check. When it has
    ///   `max`, the new one takes the place of the one that has waited longest without
    ///   finishing its first request head, or failing that the one idle longest (never one with
    ///   a request in its handler, never a document connection), which is closed. With none of
    ///   those it goes in the reserve; with the reserve full, it takes the place of a reserve
    ///   connection that has sent nothing, or is closed.
    fn admit(
        self: &Arc<Self>,
        ip: IpAddr,
        trusted: bool,
        max: usize,
        per_address: usize,
        stats: &Arc<HttpStats>,
    ) -> Option<Admitted> {
        let ip = ip.to_canonical();
        let mut inner = self.lock();
        let from_there = inner.by_address.get(&ip).copied().unwrap_or(0);
        if !trusted && per_address > 0 && from_there >= per_address {
            stats.refused_for_address();
            return None;
        }
        let (regular, reserve) = (inner.regular, inner.reserve);
        let reserved = if regular < max {
            false
        } else if let Some(victim) = inner.victim(false) {
            inner.evict(victim, stats);
            false
        } else if reserve < HEALTH_RESERVE {
            true
        } else if let Some(victim) = inner.victim(true) {
            inner.evict(victim, stats);
            true
        } else {
            stats.refused();
            return None;
        };
        let conn = Arc::new(Conn {
            id: inner.next,
            ip,
            accepted: Instant::now(),
            reserved,
            headed: AtomicBool::new(false),
            handling: AtomicUsize::new(0),
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
        *inner.by_address.entry(ip).or_default() += 1;
        drop(inner);
        Some(Admitted {
            open: stats.opened(),
            registered: Registered(conn, self.clone()),
        })
    }
}

impl ConnsInner {
    /// The connection to close to make room, among those in the reserve or the others: the one
    /// that has waited longest for its first request head; failing that (not in the reserve,
    /// whose connections take one request), the one idle longest.
    fn victim(&self, in_reserve: bool) -> Option<Arc<Conn>> {
        let candidates = || {
            self.all
                .values()
                .filter(move |c| c.reserved == in_reserve && c.evictable(&self.evicted))
        };
        let fresh = candidates()
            .filter(|c| !c.headed.load(Ordering::SeqCst))
            .min_by_key(|c| c.accepted);
        if fresh.is_some() || in_reserve {
            return fresh.cloned();
        }
        candidates()
            .min_by_key(|c| c.active_ms.load(Ordering::Relaxed))
            .cloned()
    }

    fn evict(&mut self, victim: Arc<Conn>, stats: &HttpStats) {
        self.evicted.insert(victim.id);
        self.uncount(&victim);
        victim.evicted.notify_one();
        stats.evicted();
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
