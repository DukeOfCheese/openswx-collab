//! `GET /api/documents/{document}/ws`: the live document WebSocket (protocol v2).
//!
//! Frames are binary MessagePack ([`oswx_core::wire`]); the largest frame taken is
//! `max_change_bytes + 65536` bytes ([`frame_limit`]). The WebSocket layer reads one byte more
//! ([`conn::read_limit`]): a frame just over the limit is read whole and refused with close 1009;
//! a longer one is refused by the layer without being read, and closed 1002 (which a client still
//! sending the rest of that frame usually loses to the reset, seeing the connection drop). A frame
//! within the limit that is not a `change` and is over 512 KiB is answered with an `error`
//! (`too_large`) in its turn and the connection kept. On joining, once the room is open (and so holds
//! the document's lease), the client's 1,000 most recent change ids in the document are read from the
//! journal and handed to that room (another room refuses them), so that its resends are recognised
//! whatever base they carry; this is repeated if the join has to be tried again.
//!
//! The handshake (inventory §5 "Handshake", in v2): the first frame must come within
//! [`HELLO_TIMEOUT`] and be a `hello`; then, each refusal being an `error` frame and a close:
//!
//! | what | `error` code | close |
//! |---|---|---|
//! | the first frame is late, text, not a hello, or malformed | `bad_hello` | 4400 |
//! | `protocol` is not [`PROTOCOL_VERSION`] | `protocol` | 4426 |
//! | the token is not a live session | `unauthorized` | 4401 |
//! | the server holds as many document connections as it may ([`crate::Settings::max_document_connections`]; checked at once, before the hello) | `server_full` | 4429 |
//! | the account has `max_connections_per_user` document connections open already | `too_many_connections` | 4429 |
//! | the document is missing or deleted, or the account has no role in its project | `not_found` | 4404 |
//! | the server is stopping | `shutting_down` | 1001 |
//! | the server failed | `server_error` | 1011 |
//!
//! A refused document is refused the same way whether it does not exist or is not yours. The
//! connection's client id is `<user id>.<client id of the hello>`; a second connection with the same
//! one replaces the first (4409). After the welcome, a text frame or a malformed one is answered with
//! `error` (`bad_message`) and the connection kept; a second hello is ignored.
//!
//! From the session check on, the connection is registered with the hub ([`crate::hub`]), which
//! closes it when its access ends; if that happens while its role is being read, the session and
//! the role are read again.

use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::get;
use futures_util::StreamExt;
use oswx_core::wire::{self, ClientMsg, PROTOCOL_VERSION};
use std::sync::Arc;

use crate::access::project_role;
use crate::error::ApiPath;
use crate::hub::{Connecting, OpenError, RegisterError};
use crate::room::conn::{self, Outbox, error_frame};
use crate::room::{JoinError, JoinRequest, close};
use crate::security::{ProjectRole, token_hash};
use crate::state::AppState;

/// How long a client has to send its hello.
pub const HELLO_TIMEOUT: Duration = Duration::from_secs(15);
/// How many times a join is tried when the room unloads under it.
const JOIN_TRIES: usize = 3;
/// How many of a client's latest change ids a join loads into the room's memory.
const RECENT_IDS: i64 = 1000;
/// How many times the session and role are read when drops keep coming during the handshake.
const SETTLE_TRIES: usize = 3;
/// The longest close reason sent.
const MAX_REASON: usize = 120;
/// How much a connection reads from its socket at once, and what its read buffer holds when no
/// frame larger than that is coming in. The WebSocket layer's default, 128 KiB, is allocated and
/// filled with zeros for every connection at its first read: with it an idle connection cost the
/// server about 170 KB, with this about 25 KB (`bench/results/README.md`). Messages here are
/// mostly far smaller; a larger frame grows the buffer while it is read.
const READ_BUFFER: usize = 8 * 1024;

/// The largest frame a client may send: `max_change_bytes` plus room for the envelope.
pub fn frame_limit(settings: &crate::Settings) -> usize {
    settings.max_change_bytes.saturating_add(65536)
}

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/api/documents/{document_id}/ws", get(document_ws))
}

async fn document_ws(
    State(state): State<AppState>,
    ApiPath(document_id): ApiPath<String>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let max = frame_limit(&state.settings);
    upgrade
        .max_message_size(conn::read_limit(max))
        .max_frame_size(conn::read_limit(max))
        .read_buffer_size(READ_BUFFER)
        .on_upgrade(move |socket| serve(state, document_id, socket))
}

/// Sends an `error` and closes.
async fn refuse(mut socket: WebSocket, code: &str, message: &str, close_code: u16) {
    let _ = socket
        .send(Message::Binary(error_frame(code, message)))
        .await;
    let reason: String = message.chars().take(MAX_REASON).collect();
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code,
            reason: reason.into(),
        })))
        .await;
    // Let the close frame arrive before the socket goes (read until the client answers it).
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(Ok(msg)) = socket.next().await {
            if matches!(msg, Message::Close(_)) {
                break;
            }
        }
    })
    .await;
}

/// The first frame's message; `None` if it was not a usable hello (or the client went away).
async fn first_frame(socket: &mut WebSocket, max: usize) -> Option<Result<ClientMsg, ()>> {
    loop {
        match socket.next().await? {
            Ok(Message::Binary(frame)) => {
                return Some(wire::decode_client(&frame, max).map_err(|_| ()));
            }
            Ok(Message::Text(_)) => return Some(Err(())),
            Ok(Message::Ping(_) | Message::Pong(_)) => continue,
            Ok(Message::Close(_)) | Err(_) => return None,
        }
    }
}

/// What a document connection over the server's ceiling is told.
const SERVER_FULL: &str =
    "the server holds as many document connections as it can; try again later";

async fn serve(state: AppState, document_id: String, mut socket: WebSocket) {
    // Counted from the upgrade, before the hello: a socket that never says hello holds a place.
    let Some(_place) = state
        .hub
        .take_socket(state.settings.max_document_connections())
    else {
        return refuse(socket, "server_full", SERVER_FULL, close::TOO_MANY).await;
    };
    let max = frame_limit(&state.settings);
    let bad_hello = "the first message must be a valid hello";
    let hello = match tokio::time::timeout(HELLO_TIMEOUT, first_frame(&mut socket, max)).await {
        Ok(Some(Ok(hello @ ClientMsg::Hello { .. }))) => hello,
        Ok(None) => return,
        Ok(Some(_)) | Err(_) => {
            return refuse(socket, "bad_hello", bad_hello, close::BAD_HELLO).await;
        }
    };
    let ClientMsg::Hello {
        protocol,
        token,
        client_id,
        format_version,
        last_seq,
        ..
    } = hello
    else {
        return;
    };
    if client_id.contains('\0') {
        // (No database text holds the zero character; the client id is part of journal rows.)
        return refuse(socket, "bad_hello", bad_hello, close::BAD_HELLO).await;
    }
    if protocol != PROTOCOL_VERSION {
        let message = format!("this server speaks protocol {PROTOCOL_VERSION}; update openswx");
        return refuse(socket, "protocol", &message, close::PROTOCOL).await;
    }
    let server_error = |e: anyhow::Error| {
        tracing::error!("a document connection failed: {e:#}");
    };
    let hash = token_hash(&token);
    drop(token);
    let session = match state
        .sessions
        .session(state.store.as_ref(), &hash, state.clock.now())
        .await
    {
        Ok(Some(session)) => session,
        Ok(None) => return refuse(socket, "unauthorized", "sign in again", close::SIGN_IN).await,
        Err(e) => {
            server_error(e);
            return refuse(socket, "server_error", "try again", close::SERVER_ERROR).await;
        }
    };
    let user_id = session.user_id.clone();
    let mut session_expires = session.expires_at;
    let outbox = Outbox::with_slowest_reader(
        4 * state.settings.max_change_bytes,
        state.room_hooks.slowest_reader,
    );
    let conn = conn::next_id();
    let client: Arc<str> = Arc::from(format!("{user_id}.{client_id}"));
    let connecting = Connecting {
        conn,
        user: &user_id,
        doc: &document_id,
        client: &client,
        token_hash: &hash,
        outbox: outbox.clone(),
    };
    let registration = match state
        .hub
        .register(connecting, state.settings.max_connections_per_user)
    {
        Ok(registration) => registration,
        Err(RegisterError::TooMany) => {
            crate::metrics::connection_refused();
            let message = "too many connections";
            return refuse(socket, "too_many_connections", message, close::TOO_MANY).await;
        }
        Err(RegisterError::ShuttingDown) => {
            let message = "the server is shutting down";
            return refuse(socket, "shutting_down", message, close::GOING_AWAY).await;
        }
    };
    let not_found = "no such document";
    let found = || async {
        let Some(doc) = state
            .store
            .document_by_id(&document_id)
            .await?
            .filter(|d| d.deleted_at.is_none())
        else {
            return anyhow::Ok(None);
        };
        let role = project_role(state.store.as_ref(), &user_id, &doc.project_id)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        let name = state
            .store
            .user_by_id(&user_id)
            .await?
            .map(|u| u.name)
            .unwrap_or_default();
        Ok(role.map(|role| (role, name, doc.project_id)))
    };
    let mut settled = None;
    for attempt in 0..SETTLE_TRIES {
        if attempt > 0 {
            // A drop concerned this connection while its role was read: is the session still
            // live?
            match state
                .sessions
                .session(state.store.as_ref(), &hash, state.clock.now())
                .await
            {
                Ok(Some(s)) if s.user_id == user_id => session_expires = s.expires_at,
                Ok(_) => {
                    drop(registration);
                    return refuse(socket, "unauthorized", "sign in again", close::SIGN_IN).await;
                }
                Err(e) => {
                    server_error(e);
                    drop(registration);
                    return refuse(socket, "server_error", "try again", close::SERVER_ERROR).await;
                }
            }
        }
        match found().await {
            Ok(Some((role, name, project))) => {
                if registration.settle(&project, role) {
                    settled = Some((role, name));
                    break;
                }
            }
            Ok(None) => {
                drop(registration);
                return refuse(socket, "not_found", not_found, close::GONE).await;
            }
            Err(e) => {
                server_error(e);
                drop(registration);
                return refuse(socket, "server_error", "try again", close::SERVER_ERROR).await;
            }
        }
    }
    let Some((role, name)) = settled else {
        drop(registration);
        return refuse(socket, "busy", "try again", close::TOO_SLOW).await;
    };

    let (sink, stream) = socket.split();
    tokio::spawn(conn::write(sink, outbox.clone()));
    let heard = conn::Heard::new(state.clock.clone());
    let mut request = JoinRequest {
        conn,
        outbox: outbox.clone(),
        heard: heard.clone(),
        user: user_id.clone(),
        name,
        role,
        client,
        format_version,
        last_seq,
        recent_ids: Vec::new(),
        ids_complete: false,
        room: 0,
        session_expires,
    };
    let mut joined = None;
    for _ in 0..JOIN_TRIES {
        let room = match state.hub.open(&state, &document_id).await {
            Ok(room) => room,
            Err(OpenError::NoDocument) => {
                send_refusal(&outbox, "not_found", not_found, close::GONE);
                drop(registration);
                return drain(stream).await;
            }
            Err(OpenError::ShuttingDown) => {
                let message = "the server is shutting down";
                send_refusal(&outbox, "shutting_down", message, close::GOING_AWAY);
                drop(registration);
                return drain(stream).await;
            }
            Err(OpenError::Failed(e)) => {
                server_error(e);
                send_refusal(&outbox, "server_error", "try again", close::SERVER_ERROR);
                drop(registration);
                return drain(stream).await;
            }
        };
        // What this client already sent to the document, read only now that this room holds the
        // lease (no earlier room can commit any more), so that the room recognises its resends. A
        // viewer cannot send changes: nothing to load, and the room never looks its ids up.
        if role != ProjectRole::Viewer {
            match state
                .store
                .recent_change_ids(&document_id, &request.client, RECENT_IDS)
                .await
            {
                Ok(ids) => {
                    // Fewer than asked for: these are all of them.
                    request.ids_complete = (ids.len() as i64) < RECENT_IDS;
                    request.recent_ids = ids
                        .into_iter()
                        .filter_map(|(id, seq)| Some((id, u64::try_from(seq).ok()?)))
                        .collect();
                }
                Err(e) => {
                    server_error(e);
                    send_refusal(&outbox, "server_error", "try again", close::SERVER_ERROR);
                    drop(registration);
                    return drain(stream).await;
                }
            }
        }
        request.room = room.id();
        // Bounded: a room that does not take the join (stuck) cannot hold the handshake forever.
        // (If it takes the join later, the connection is closed by then and never registers.)
        match tokio::time::timeout(state.room_hooks.join_wait, room.join(request)).await {
            Ok(Ok(())) => {
                registration.joined(&room);
                joined = Some(room);
                break;
            }
            // The room was ending at that moment: open it again.
            Ok(Err(JoinError::Gone(back))) => request = *back,
            Ok(Err(JoinError::Lost)) => {
                outbox.close(close::MOVED, "reconnect");
                drop(registration);
                return drain(stream).await;
            }
            Err(_) => {
                state.room_hooks.after_join_timeout.pass().await;
                outbox.close(close::MOVED, "reconnect");
                // The room may have taken the join just as the wait ran out: no reader will ever
                // tell it the connection went, so it is told now (nothing, if it never joined).
                let gone = room.clone();
                tokio::spawn(async move { gone.leave(conn).await });
                drop(registration);
                return drain(stream).await;
            }
        }
    }
    let Some(room) = joined else {
        send_refusal(&outbox, "busy", "try again", close::TOO_SLOW);
        drop(registration);
        return drain(stream).await;
    };
    conn::read(stream, conn, outbox, heard, room, max).await;
    drop(registration);
}

/// An `error` and a close, through a connection's queue (whose writer is already running).
fn send_refusal(outbox: &Outbox, code: &str, message: &str, close_code: u16) {
    outbox.release(error_frame(code, message));
    let reason: String = message.chars().take(MAX_REASON).collect();
    outbox.close(close_code, &reason);
}

/// Reads (and drops) what a refused client still sends until it answers the close.
async fn drain(mut stream: futures_util::stream::SplitStream<WebSocket>) {
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(Ok(msg)) = stream.next().await {
            if matches!(msg, Message::Close(_)) {
                break;
            }
        }
    })
    .await;
}
