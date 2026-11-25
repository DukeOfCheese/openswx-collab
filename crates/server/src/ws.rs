//! `GET /api/documents/{document}/ws`: the live document WebSocket (protocol v2).
//!
//! Frames are binary MessagePack ([`oswx_core::wire`]); the largest frame taken is
//! `max_change_bytes + 65536` bytes ([`frame_limit`]). A longer one ends the connection: the
//! WebSocket layer refuses to read it, and the server closes with 1009 (which a client still sending
//! the rest of that frame usually loses to the reset, seeing the connection drop). A frame within the
//! limit that is not a `change` and is over 512 KiB is answered with an `error` (`too_large`) and the
//! connection kept.
//!
//! The handshake (inventory §5 "Handshake", in v2): the first frame must come within
//! [`HELLO_TIMEOUT`] and be a `hello`; then, each refusal being an `error` frame and a close:
//!
//! | what | `error` code | close |
//! |---|---|---|
//! | the first frame is late, text, not a hello, or malformed | `bad_hello` | 4400 |
//! | `protocol` is not [`PROTOCOL_VERSION`] | `protocol` | 4426 |
//! | the token is not a live session | `unauthorized` | 4401 |
//! | the document is missing or deleted, or the account has no role in its project | `not_found` | 4404 |
//! | the server is stopping | `shutting_down` | 1001 |
//! | the server failed | `server_error` | 1011 |
//!
//! A refused document is refused the same way whether it does not exist or is not yours. The
//! connection's client id is `<user id>.<client id of the hello>`; a second connection with the same
//! one replaces the first (4409). After the welcome, a text frame or a malformed one is answered with
//! `error` (`bad_message`) and the connection kept; a second hello is ignored.

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
use crate::hub::OpenError;
use crate::room::conn::{self, Outbox, error_frame};
use crate::room::{JoinRequest, close};
use crate::security::token_hash;
use crate::state::AppState;

/// How long a client has to send its hello.
pub const HELLO_TIMEOUT: Duration = Duration::from_secs(15);
/// How many times a join is tried when the room unloads under it.
const JOIN_TRIES: usize = 3;
/// The longest close reason sent.
const MAX_REASON: usize = 120;

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
        .max_message_size(max)
        .max_frame_size(max)
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

async fn serve(state: AppState, document_id: String, mut socket: WebSocket) {
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
    if protocol != PROTOCOL_VERSION {
        let message = format!("this server speaks protocol {PROTOCOL_VERSION}; update openswx");
        return refuse(socket, "protocol", &message, close::PROTOCOL).await;
    }
    let server_error = |e: anyhow::Error| {
        tracing::error!("a document connection failed: {e:#}");
    };
    let user_id = match state
        .sessions
        .get(state.store.as_ref(), &token_hash(&token), state.clock.now())
        .await
    {
        Ok(Some(id)) => id,
        Ok(None) => return refuse(socket, "unauthorized", "sign in again", close::SIGN_IN).await,
        Err(e) => {
            server_error(e);
            return refuse(socket, "server_error", "try again", close::SERVER_ERROR).await;
        }
    };
    drop(token);
    let not_found = "no such document";
    let found = async {
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
        Ok(role.map(|role| (role, name)))
    };
    let (role, name) = match found.await {
        Ok(Some(found)) => found,
        Ok(None) => return refuse(socket, "not_found", not_found, close::GONE).await,
        Err(e) => {
            server_error(e);
            return refuse(socket, "server_error", "try again", close::SERVER_ERROR).await;
        }
    };

    let (sink, stream) = socket.split();
    let outbox = Outbox::new(4 * state.settings.max_change_bytes);
    tokio::spawn(conn::write(sink, outbox.clone()));
    let conn = conn::next_id();
    let mut request = JoinRequest {
        conn,
        outbox: outbox.clone(),
        user: user_id.clone(),
        name,
        role,
        client: Arc::from(format!("{user_id}.{client_id}")),
        format_version,
        last_seq,
    };
    let mut joined = None;
    for _ in 0..JOIN_TRIES {
        let room = match state.hub.open(&state, &document_id).await {
            Ok(room) => room,
            Err(OpenError::NoDocument) => {
                send_refusal(&outbox, "not_found", not_found, close::GONE);
                return drain(stream).await;
            }
            Err(OpenError::ShuttingDown) => {
                let message = "the server is shutting down";
                send_refusal(&outbox, "shutting_down", message, close::GOING_AWAY);
                return drain(stream).await;
            }
            Err(OpenError::Failed(e)) => {
                server_error(e);
                send_refusal(&outbox, "server_error", "try again", close::SERVER_ERROR);
                return drain(stream).await;
            }
        };
        match room.join(request).await {
            Ok(()) => {
                joined = Some(room);
                break;
            }
            // The room unloaded at that moment: open it again.
            Err(back) => request = *back,
        }
    }
    let Some(room) = joined else {
        send_refusal(&outbox, "busy", "try again", close::TOO_SLOW);
        return drain(stream).await;
    };
    conn::read(stream, conn, outbox, room, max).await;
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
