//! `Live`: a protocol v2 client on a document's WebSocket, built on `oswx_core::wire`, for tests.
//!
//! ```ignore
//! let mut a = Live::connect(&server, &doc, &world.admin).await;
//! assert_eq!(a.change(vec![set_depth(25)], vec![], vec![]).await.seq(), 1);
//! let ev: ChangeEvent = b.expect().await;
//! ```
//!
//! Every wait has a timeout ([`WAIT`]) and panics when it passes, so a test never hangs.

// A message not wanted is handed back whole, to be kept in `seen`.
#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use oswx_core::wire::{
    self, ChangeEvent, ClientMsg, Lock, LockDenied, PROTOCOL_VERSION, Peer, RejectCode, ServerMsg,
};
use oswx_core::{Change, Conflict, Op, Path, Plain, Pos, Seq, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::TestServer;

/// How long any wait for the server lasts before the test fails.
pub const WAIT: Duration = Duration::from_secs(20);

/// The raw socket.
pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The URL of a document's WebSocket.
pub fn ws_url(server: &TestServer, doc: &str) -> String {
    format!(
        "{}/api/documents/{doc}/ws",
        server.base.replacen("http", "ws", 1)
    )
}

/// A socket on a document's WebSocket, nothing sent yet.
pub async fn ws_connect(server: &TestServer, doc: &str) -> Socket {
    let (ws, _) = tokio_tungstenite::connect_async(ws_url(server, doc))
        .await
        .expect("the WebSocket opens");
    ws
}

/// What a client puts in its hello besides the token.
#[derive(Debug, Clone)]
pub struct Hello {
    /// `None`: a random one.
    pub client_id: Option<String>,
    pub format_version: Option<i64>,
    pub last_seq: Option<Seq>,
    pub protocol: u32,
}

impl Default for Hello {
    fn default() -> Self {
        Self {
            client_id: None,
            format_version: Some(3),
            last_seq: None,
            protocol: PROTOCOL_VERSION,
        }
    }
}

impl Hello {
    pub fn client(id: &str) -> Self {
        Self {
            client_id: Some(id.to_owned()),
            ..Self::default()
        }
    }

    pub fn last_seq(mut self, seq: Seq) -> Self {
        self.last_seq = Some(seq);
        self
    }

    pub fn format_version(mut self, v: Option<i64>) -> Self {
        self.format_version = v;
        self
    }
}

/// The hello frame.
pub fn hello_frame(token: &str, hello: &Hello, client_id: &str) -> Vec<u8> {
    wire::encode_client(&ClientMsg::Hello {
        protocol: hello.protocol,
        token: token.to_owned(),
        client_id: client_id.to_owned(),
        format_version: hello.format_version,
        last_seq: hello.last_seq,
        app: None,
    })
    .to_vec()
}

/// The welcome a client got.
#[derive(Debug, Clone)]
pub struct Welcome {
    pub seq: Seq,
    pub project_id: String,
    pub role: String,
    pub read_only: bool,
    pub read_only_reason: Option<String>,
    pub you: Peer,
    pub peers: Vec<Peer>,
    pub locks: Vec<Lock>,
    pub content: Option<Value>,
    pub changes: Option<Vec<ChangeEvent>>,
}

/// How a connection was refused: the `error` frame (code, message) and the close code.
#[derive(Debug, Clone, PartialEq)]
pub struct Refused {
    pub error: Option<(String, String)>,
    pub close: Option<u16>,
}

impl Refused {
    /// The `error` code.
    pub fn code(&self) -> &str {
        self.error.as_ref().map_or("", |(c, _)| c)
    }
}

/// How a change ended.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Accepted(Seq),
    Rejected {
        code: RejectCode,
        message: String,
        conflicts: Vec<Conflict>,
    },
}

impl Outcome {
    /// The seq it was accepted as (panics if it was rejected).
    #[track_caller]
    pub fn seq(&self) -> Seq {
        match self {
            Outcome::Accepted(seq) => *seq,
            other => panic!("expected an acceptance, got {other:?}"),
        }
    }

    /// The code it was rejected with (panics if it was accepted).
    #[track_caller]
    pub fn code(&self) -> RejectCode {
        match self {
            Outcome::Rejected { code, .. } => *code,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }
}

/// A server message `expect` can wait for.
pub trait FromServer: Sized {
    fn pick(msg: ServerMsg) -> Result<Self, ServerMsg>;
}

impl FromServer for ServerMsg {
    fn pick(msg: ServerMsg) -> Result<Self, ServerMsg> {
        Ok(msg)
    }
}

impl FromServer for ChangeEvent {
    fn pick(msg: ServerMsg) -> Result<Self, ServerMsg> {
        match msg {
            ServerMsg::Change(ev) => Ok(ev),
            other => Err(other),
        }
    }
}

/// `peer.joined`.
impl FromServer for Peer {
    fn pick(msg: ServerMsg) -> Result<Self, ServerMsg> {
        match msg {
            ServerMsg::PeerJoined(p) => Ok(p),
            other => Err(other),
        }
    }
}

/// `peer.left`: the client id.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerLeft(pub String);

impl FromServer for PeerLeft {
    fn pick(msg: ServerMsg) -> Result<Self, ServerMsg> {
        match msg {
            ServerMsg::PeerLeft { client_id } => Ok(PeerLeft(client_id.to_string())),
            other => Err(other),
        }
    }
}

/// `pong`: the seq.
#[derive(Debug, Clone, PartialEq)]
pub struct Pong(pub Seq);

impl FromServer for Pong {
    fn pick(msg: ServerMsg) -> Result<Self, ServerMsg> {
        match msg {
            ServerMsg::Pong { seq } => Ok(Pong(seq)),
            other => Err(other),
        }
    }
}

/// `error`.
#[derive(Debug, Clone, PartialEq)]
pub struct ErrorMsg {
    pub code: String,
    pub message: String,
}

impl FromServer for ErrorMsg {
    fn pick(msg: ServerMsg) -> Result<Self, ServerMsg> {
        match msg {
            ServerMsg::Error { code, message, .. } => Ok(ErrorMsg { code, message }),
            other => Err(other),
        }
    }
}

/// `lock.result`.
#[derive(Debug, Clone, PartialEq)]
pub struct LockResult {
    pub request_id: String,
    pub granted: Vec<Path>,
    pub denied: Vec<LockDenied>,
}

impl LockResult {
    /// The holder named for a denied path (`None`: denied with no holder; panics if it was not
    /// denied).
    #[track_caller]
    pub fn holder(&self, p: &[&str]) -> Option<&Lock> {
        let p = path(p);
        self.denied
            .iter()
            .find(|d| d.path == p)
            .unwrap_or_else(|| panic!("{p:?} was not denied: {self:?}"))
            .holder
            .as_ref()
    }
}

impl FromServer for LockResult {
    fn pick(msg: ServerMsg) -> Result<Self, ServerMsg> {
        match msg {
            ServerMsg::LockResult {
                request_id,
                granted,
                denied,
            } => Ok(LockResult {
                request_id,
                granted,
                denied,
            }),
            other => Err(other),
        }
    }
}

/// `lock.changed`.
#[derive(Debug, Clone, PartialEq)]
pub struct LockChanged {
    pub locks: Vec<Lock>,
    pub released: Vec<Path>,
}

impl FromServer for LockChanged {
    fn pick(msg: ServerMsg) -> Result<Self, ServerMsg> {
        match msg {
            ServerMsg::LockChanged { locks, released } => Ok(LockChanged { locks, released }),
            other => Err(other),
        }
    }
}

/// `presence` or `draft` from another client: (`"presence"` or `"draft"`, client id, data).
#[derive(Debug, Clone, PartialEq)]
pub struct Relayed {
    pub kind: &'static str,
    pub client_id: String,
    pub data: Option<Plain>,
}

impl FromServer for Relayed {
    fn pick(msg: ServerMsg) -> Result<Self, ServerMsg> {
        match msg {
            ServerMsg::Presence { client_id, data } => Ok(Relayed {
                kind: "presence",
                client_id: client_id.to_string(),
                data: Some(data),
            }),
            ServerMsg::Draft { client_id, data } => Ok(Relayed {
                kind: "draft",
                client_id: client_id.to_string(),
                data,
            }),
            other => Err(other),
        }
    }
}

/// Opaque data (presence, a draft) from JSON.
pub fn plain(j: serde_json::Value) -> Plain {
    oswx_server::content::plain_from_json(&j)
}

/// `change.accepted` or `change.rejected`, for any change id.
#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    pub change_id: String,
    pub outcome: Outcome,
}

impl FromServer for Answer {
    fn pick(msg: ServerMsg) -> Result<Self, ServerMsg> {
        match msg {
            ServerMsg::Accepted { change_id, seq } => Ok(Answer {
                change_id: change_id.to_string(),
                outcome: Outcome::Accepted(seq),
            }),
            ServerMsg::Rejected {
                change_id,
                code,
                message,
                conflicts,
            } => Ok(Answer {
                change_id: change_id.to_string(),
                outcome: Outcome::Rejected {
                    code,
                    message,
                    conflicts,
                },
            }),
            other => Err(other),
        }
    }
}

/// One client on a document.
pub struct Live {
    ws: Socket,
    pub welcome: Welcome,
    /// The id the server knows this client by: `<user id>.<client id of the hello>`.
    pub client_id: String,
    /// The newest seq this client has heard of (welcome, change events, its own acceptances).
    pub seq: Seq,
    /// Messages `expect`, `outcome` and `drain` read past.
    pub seen: Vec<ServerMsg>,
    /// How the server closed the socket, once it did: the close code (1006 for none) and reason.
    pub closed: Option<(u16, String)>,
    changes: u64,
    items: u64,
    tag: String,
}

fn random_client_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Reads the next binary frame of a socket as a server message; `Err` with the close code (1006
/// when the socket ended without one) when it closes instead.
async fn next_frame(ws: &mut Socket, wait: Duration) -> Option<Result<ServerMsg, (u16, String)>> {
    let read = async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Binary(frame))) => {
                    return Ok(wire::decode_server(&frame).expect("the server sends valid frames"));
                }
                Some(Ok(Message::Text(t))) => panic!("the server sent a text frame: {t}"),
                Some(Ok(Message::Close(frame))) => {
                    return Err(frame.map_or((1005, String::new()), |f| {
                        (u16::from(f.code), f.reason.to_string())
                    }));
                }
                Some(Ok(_)) => continue,
                Some(Err(_)) | None => return Err((1006, String::new())),
            }
        }
    };
    tokio::time::timeout(wait, read).await.ok()
}

impl Live {
    /// Connects with a random client id, format version 3 and no `last_seq` (panics if refused).
    pub async fn connect(server: &TestServer, doc: &str, token: &str) -> Live {
        Self::connect_with(server, doc, token, Hello::default()).await
    }

    /// Connects with the given hello (panics if refused).
    pub async fn connect_with(server: &TestServer, doc: &str, token: &str, hello: Hello) -> Live {
        match Self::try_connect(server, doc, token, hello).await {
            Ok(live) => live,
            Err(refused) => panic!("the connection was refused: {refused:?}"),
        }
    }

    /// Connects, or answers how the server refused.
    pub async fn try_connect(
        server: &TestServer,
        doc: &str,
        token: &str,
        hello: Hello,
    ) -> Result<Live, Refused> {
        let mut ws = ws_connect(server, doc).await;
        let asked = hello.client_id.clone().unwrap_or_else(random_client_id);
        ws.send(Message::Binary(hello_frame(token, &hello, &asked).into()))
            .await
            .expect("the hello is sent");
        let first = next_frame(&mut ws, WAIT)
            .await
            .expect("an answer to the hello");
        match first {
            Ok(ServerMsg::Welcome {
                seq,
                project_id,
                role,
                read_only,
                read_only_reason,
                you,
                peers,
                locks,
                broken: _,
                content,
                changes,
            }) => Ok(Live {
                ws,
                client_id: you.client_id.to_string(),
                welcome: Welcome {
                    seq,
                    project_id,
                    role,
                    read_only,
                    read_only_reason,
                    you,
                    peers,
                    locks,
                    content,
                    changes,
                },
                seq,
                seen: Vec::new(),
                closed: None,
                changes: 0,
                items: 0,
                tag: random_client_id()[..6].to_owned(),
            }),
            Ok(ServerMsg::Error { code, message, .. }) => {
                let close = next_frame(&mut ws, WAIT)
                    .await
                    .and_then(|f| f.err())
                    .map(|c| c.0);
                Err(Refused {
                    error: Some((code, message)),
                    close,
                })
            }
            Ok(other) => panic!("expected a welcome, got {other:?}"),
            Err((code, _)) => Err(Refused {
                error: None,
                close: Some(code),
            }),
        }
    }

    fn note(&mut self, msg: &ServerMsg) {
        match msg {
            ServerMsg::Change(ev) => self.seq = self.seq.max(ev.seq),
            ServerMsg::Accepted { seq, .. } => self.seq = self.seq.max(*seq),
            _ => {}
        }
    }

    /// The next message, or `None` if none comes within `wait` (or the socket closed: see
    /// [`Live::closed`]).
    pub async fn try_recv(&mut self, wait: Duration) -> Option<ServerMsg> {
        if self.closed.is_some() {
            return None;
        }
        match next_frame(&mut self.ws, wait).await? {
            Ok(msg) => {
                self.note(&msg);
                Some(msg)
            }
            Err(close) => {
                self.closed = Some(close);
                None
            }
        }
    }

    /// The next message (panics if none comes).
    pub async fn recv(&mut self) -> ServerMsg {
        match self.try_recv(WAIT).await {
            Some(msg) => msg,
            None => panic!("no message came; closed: {:?}", self.closed),
        }
    }

    /// The next message of kind `T`; the ones before it are kept in `seen`.
    pub async fn expect<T: FromServer>(&mut self) -> T {
        loop {
            let msg = self.recv().await;
            match T::pick(msg) {
                Ok(t) => return t,
                Err(other) => self.seen.push(other),
            }
        }
    }

    /// Everything the server has sent so far: a ping, then every message up to its pong.
    pub async fn drain(&mut self) -> Vec<ServerMsg> {
        self.send(&ClientMsg::Ping).await;
        let mut out = Vec::new();
        loop {
            match self.recv().await {
                ServerMsg::Pong { .. } => break,
                other => out.push(other),
            }
        }
        self.seen.extend(out.iter().cloned());
        out
    }

    /// How the server closed the socket (waiting for it, reading past anything it still sends).
    pub async fn closed(&mut self) -> (u16, String) {
        let deadline = tokio::time::Instant::now() + WAIT;
        while self.closed.is_none() {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(!left.is_zero(), "the server did not close the socket");
            if let Some(msg) = self.try_recv(left).await {
                self.seen.push(msg);
            }
        }
        self.closed.clone().unwrap_or_default()
    }

    pub async fn send(&mut self, msg: &ClientMsg) {
        self.send_raw(wire::encode_client(msg).to_vec()).await;
    }

    pub async fn send_raw(&mut self, frame: Vec<u8>) {
        self.ws
            .send(Message::Binary(frame.into()))
            .await
            .expect("the frame is sent");
    }

    /// Sends a frame the server may cut off halfway (an error sending it is not a failure).
    pub async fn send_raw_lossy(&mut self, frame: Vec<u8>) {
        let _ = self.ws.send(Message::Binary(frame.into())).await;
    }

    pub async fn send_text(&mut self, text: &str) {
        self.ws
            .send(Message::Text(text.into()))
            .await
            .expect("the frame is sent");
    }

    /// A fresh change id: [`Live::change_id`] of 1, 2, ...
    pub fn next_change_id(&mut self) -> Arc<str> {
        self.changes += 1;
        self.change_id(self.changes)
    }

    /// The id of this connection's `n`th change made with [`Live::next_change_id`]: `<tag>c<n>`,
    /// the tag being random per connection, so that a client that reconnects does not resend by
    /// accident.
    pub fn change_id(&self, n: u64) -> Arc<str> {
        Arc::from(format!("{}c{n}", self.tag))
    }

    /// A fresh item id of this client: `<user>.<client>.<n>`.
    pub fn new_id(&mut self) -> Arc<str> {
        self.items += 1;
        Arc::from(format!("{}.{}", self.client_id, self.items))
    }

    /// A change on `base` with a fresh id.
    pub fn make_change(&mut self, base: Seq, ops: Vec<Op>) -> Change {
        Change {
            change_id: self.next_change_id(),
            base,
            ops,
            reads: Default::default(),
            depends: Vec::new(),
            undo_of: None,
            command: Arc::from("edit"),
        }
    }

    pub async fn send_change(&mut self, change: &Change) {
        self.send(&ClientMsg::Change(change.clone())).await;
    }

    /// The answer to the change `change_id`; what comes before it is kept in `seen`.
    pub async fn outcome(&mut self, change_id: &str) -> Outcome {
        loop {
            let msg = self.recv().await;
            match Answer::pick(msg) {
                Ok(a) if a.change_id == change_id => return a.outcome,
                Ok(a) => self.seen.push(answer_msg(a)),
                Err(other) => self.seen.push(other),
            }
        }
    }

    /// Sends a change made on the newest seq this client knows, and waits for its answer.
    pub async fn change(
        &mut self,
        ops: Vec<Op>,
        reads: Vec<Path>,
        depends: Vec<Arc<str>>,
    ) -> Outcome {
        let mut change = self.make_change(self.seq, ops);
        change.reads = reads.into_iter().collect();
        change.depends = depends;
        self.send_change(&change).await;
        self.outcome(&change.change_id).await
    }

    /// Sends a change made on `base`, and waits for its answer.
    pub async fn change_on(&mut self, base: Seq, ops: Vec<Op>) -> Outcome {
        let change = self.make_change(base, ops);
        self.send_change(&change).await;
        self.outcome(&change.change_id).await
    }

    /// Sends `change` again (same id), and waits for its answer.
    pub async fn resend(&mut self, change: &Change) -> Outcome {
        self.send_change(change).await;
        self.outcome(&change.change_id).await
    }

    /// An `Insert` of a new feature named `name` after `f2` of the [`super::part`] fixture, with a
    /// fresh id of this client; answers the operation and the id.
    pub fn add_feature(&mut self, name: &str) -> (Op, Arc<str>) {
        let id = self.new_id();
        (insert(&["features"], feature(&id, name), after_f2()), id)
    }

    /// Asks for locks on `paths` (each given as its segments) and waits for the `lock.result`.
    pub async fn lock(&mut self, paths: &[&[&str]]) -> LockResult {
        self.send_lock(paths).await;
        self.expect().await
    }

    /// Sends a `lock.acquire` (request id `r`) without waiting.
    pub async fn send_lock(&mut self, paths: &[&[&str]]) {
        self.send(&ClientMsg::LockAcquire {
            request_id: "r".into(),
            paths: paths.iter().map(|p| path(p)).collect(),
        })
        .await;
    }

    /// Releases locks (no answer comes).
    pub async fn unlock(&mut self, paths: &[&[&str]]) {
        self.send(&ClientMsg::LockRelease {
            paths: paths.iter().map(|p| path(p)).collect(),
        })
        .await;
    }

    /// Sends presence data (JSON).
    pub async fn presence(&mut self, data: serde_json::Value) {
        self.send(&ClientMsg::Presence { data: plain(data) }).await;
    }

    /// Sends a draft (JSON; `None` clears it).
    pub async fn draft(&mut self, data: Option<serde_json::Value>) {
        self.send(&ClientMsg::Draft {
            data: data.map(plain),
        })
        .await;
    }

    /// Closes the socket from this side.
    pub async fn close(mut self) {
        let _ = self.ws.close(None).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(Ok(_)) = self.ws.next().await {}
        })
        .await;
    }
}

fn answer_msg(a: Answer) -> ServerMsg {
    match a.outcome {
        Outcome::Accepted(seq) => ServerMsg::Accepted {
            change_id: Arc::from(a.change_id),
            seq,
        },
        Outcome::Rejected {
            code,
            message,
            conflicts,
        } => ServerMsg::Rejected {
            change_id: Arc::from(a.change_id),
            code,
            message,
            conflicts,
        },
    }
}

// ------------------------------------------------------------------ operations

/// A path from its segments.
pub fn path(segments: &[&str]) -> Path {
    segments.iter().map(|s| Arc::from(*s)).collect()
}

/// A document value from JSON.
pub fn value(j: serde_json::Value) -> Value {
    Value::from_plain(&oswx_server::content::plain_from_json(&j)).expect("a valid value")
}

/// `Set` at `segments` to the JSON value `j`.
pub fn set(segments: &[&str], j: serde_json::Value) -> Op {
    Op::Set {
        path: path(segments),
        value: value(j),
    }
}

/// The Python suite's `set_depth`: `features.f2.params.depth = depth`.
pub fn set_depth(depth: impl Into<serde_json::Value>) -> Op {
    set(&["features", "f2", "params", "depth"], depth.into())
}

/// A position after `f2` of the [`super::part`] fixture (the last of its two features).
pub fn after_f2() -> Pos {
    Pos::between(Some(&Pos::spread(2)[1]), None)
}

/// A feature as the Python suite's `add_feature` makes it.
pub fn feature(id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({"id": id, "name": name, "type": "fillet", "params": {"radius": 2},
                       "suppressed": false})
}

/// `Insert` of `item` (JSON) into the keyed list at `list`.
pub fn insert(list: &[&str], item: serde_json::Value, pos: Pos) -> Op {
    Op::Insert {
        list: path(list),
        item: value(item),
        pos,
    }
}

/// `Remove` of `id` from the keyed list at `list`.
pub fn remove(list: &[&str], id: &str) -> Op {
    Op::Remove {
        list: path(list),
        id: Arc::from(id),
    }
}
