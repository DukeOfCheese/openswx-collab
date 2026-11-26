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
//! - **A request must be answered in time**: `request_timeout` (60 s unless set) from its head to
//!   the beginning of its answer, body read and handler run, or it is answered 503
//!   ([`within_time`], which the routes apply). What follows a commit still runs
//!   ([`crate::AppState::commit_then`]).
//! - **A client must take what it is sent**: when nothing at all could be written to a connection
//!   for `write_timeout` (60 s unless set), the connection is closed, and with it whatever the
//!   answer held (a file, a connection to the bucket, an export's place).
//!
//! # Long transfers
//!
//! The limits are made so that a transfer that moves is never cut for being long, and one that
//! stopped always is:
//!
//! | | while it moves | when it stops |
//! |---|---|---|
//! | blob upload | not under `request_timeout`; at most `max_blob_uploads` at once ([`crate::uploads`]) | no bytes for `blob_idle_timeout`: 400, nothing stored |
//! | blob download, export | the handler has answered long before; the body is sent for as long as the client reads | nothing written for `write_timeout`: the connection is closed (and the storage's side after `blob_idle_timeout`, an export after its own stall time) |
//! | document connection | the upgrade is answered at once | the room closes a slow consumer; a socket that takes nothing for `write_timeout` is closed |
//!
//! # Stopping
//!
//! When `stop` resolves, [`serve`] stops accepting, tells every connection to finish, and runs
//! [`crate::AppState::shutdown_within`]: rooms are closed and snapshotted first, then the journal
//! is written out. A connection with a request in its handler is waited for; one without (idle,
//! in the middle of sending a request, or still being sent a long answer) is closed a moment
//! later ([`LINGER`]). All of that is given `shutdown_timeout` (8 s unless set: `docker stop`
//! kills after 10); what has not finished by then is abandoned, and [`serve`] says so.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
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
    let mut stop = pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    connections.spawn(connection(
                        stream,
                        peer,
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
    let ending = async {
        state.shutdown_within(bound).await;
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

/// Serves one connection to its end.
async fn connection(
    stream: TcpStream,
    peer: SocketAddr,
    app: Router,
    limits: Limits,
    stats: Arc<HttpStats>,
    mut closing: watch::Receiver<bool>,
) {
    let io = TokioIo::new(TakenInTime::new(stream, limits.write, stats));
    // How many of the connection's requests are in their handlers (one at most, on HTTP/1).
    let handling = Arc::new(AtomicUsize::new(0));
    let counted = handling.clone();
    let service = hyper::service::service_fn(move |mut request: Request<Incoming>| {
        request.extensions_mut().insert(ConnectInfo(peer));
        let mut app = app.clone();
        let handled = Handling::begin(&counted);
        async move {
            let response = app.call(request).await;
            drop(handled);
            response
        }
    });
    let mut served = pin!(
        http1::Builder::new()
            .timer(TokioTimer::new())
            .header_read_timeout(limits.header)
            .serve_connection(io, service)
            .with_upgrades()
    );
    // (A connection's end is its client's business: a reset, a timeout above, a bad request.)
    tokio::select! {
        _ = served.as_mut() => return,
        _ = closing.wait_for(|closing| *closing) => {}
    }
    // Stopping: the request in its handler is answered; a connection without one is closed once
    // it has had a moment to be sent what was being written.
    served.as_mut().graceful_shutdown();
    loop {
        tokio::select! {
            _ = served.as_mut() => return,
            () = tokio::time::sleep(LINGER) => {
                if handling.load(Ordering::SeqCst) == 0 {
                    return;
                }
            }
        }
    }
}

/// A request in its handler, counted until this is dropped.
struct Handling(Arc<AtomicUsize>);

impl Handling {
    fn begin(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count.clone())
    }
}

impl Drop for Handling {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The routes' time limit: a request whose answer has not begun `request_timeout` after its head
/// arrived is answered 503 [`TOO_SLOW`], and whatever it was doing is dropped (what follows a
/// commit goes on by itself). The log names the route, never the address asked for (which can
/// hold a reset token).
pub(crate) async fn within_time(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let limit = Duration::from_secs_f64(state.settings.request_timeout);
    let method = req.method().clone();
    let route = req.extensions().get::<MatchedPath>().cloned();
    match tokio::time::timeout(limit, next.run(req)).await {
        Ok(response) => response,
        Err(_) => {
            state.http.request_timed_out();
            tracing::warn!(
                %method,
                route = route.as_ref().map_or("", MatchedPath::as_str),
                "a request took longer than {limit:?} and was given up on"
            );
            detail_response(StatusCode::SERVICE_UNAVAILABLE, TOO_SLOW)
        }
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
    stats: Arc<HttpStats>,
    /// Counted as open for as long as the socket lives: as a document connection too, which
    /// outlives the HTTP connection it began as.
    _open: OpenConnection,
}

impl TakenInTime {
    fn new(stream: TcpStream, timeout: Duration, stats: Arc<HttpStats>) -> Self {
        Self {
            stream,
            timeout,
            waiting: None,
            _open: stats.opened(),
            stats,
        }
    }

    /// What a write that the stream answered with `outcome` answers.
    fn timed<T>(
        &mut self,
        cx: &mut Context<'_>,
        outcome: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        if outcome.is_ready() {
            self.waiting = None;
            return outcome;
        }
        let timeout = self.timeout;
        let waiting = self
            .waiting
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(Instant::now() + timeout)));
        if waiting.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        // (Left in place: it stays elapsed, so every later write fails as well.)
        self.stats.write_timed_out();
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "the peer took nothing of what was sent in time",
        )))
    }
}

impl AsyncRead for TakenInTime {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
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
