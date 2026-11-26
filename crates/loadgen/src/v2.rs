//! The v2 dialect: the Rust server. REST is the Python server's (so setup is shared with `v1`); the
//! WebSocket carries binary MessagePack frames ([`oswx_core::wire`]).
//!
//! What an edit is, next to v1's `feature.update` of `params.depth`: one `Set` of
//! `features.<id>.params.depth`, made on the newest seq this connection has heard of. v1 judged a conflict
//! per feature, v2 judges it per path; two editors of one feature write the same path here, so the two
//! agree for this load.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use oswx_core::wire::{self, ClientMsg, PROTOCOL_VERSION, ServerMsg};
use oswx_core::{Change, Op, Plain, Value};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::dialect::{Dialect, Outcome, Peer, Target};
use crate::doc::feature_id;
use crate::v1::{PING_EVERY, PendingGuard, Shared, open_socket, rest_setup};

/// Connections closed under their peers, by close code.
type Closes = Arc<Mutex<BTreeMap<String, u64>>>;

pub struct V2 {
    /// Makes client ids unique per run, so two runs never disconnect each other (4409).
    nonce: String,
    closes: Closes,
}

impl V2 {
    pub fn new() -> Self {
        Self {
            nonce: uuid::Uuid::new_v4().simple().to_string()[..8].to_owned(),
            closes: Closes::default(),
        }
    }
}

fn note_close(closes: &Closes, code: &str) {
    *closes
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(code.to_owned())
        .or_default() += 1;
}

#[async_trait]
impl Dialect for V2 {
    fn name(&self) -> &'static str {
        "v2"
    }

    fn closes(&self) -> Option<BTreeMap<String, u64>> {
        Some(
            self.closes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        )
    }

    async fn setup(
        &self,
        base_url: &str,
        docs: usize,
        features: usize,
        doc_bytes: usize,
    ) -> Result<Target> {
        rest_setup(&self.nonce, base_url, docs, features, doc_bytes).await
    }

    async fn connect(&self, target: &Target, n: usize) -> Result<Box<dyn Peer>> {
        let mut ws = open_socket(target, n).await?;
        let hello = wire::encode_client(&ClientMsg::Hello {
            protocol: PROTOCOL_VERSION,
            token: target.token.clone(),
            client_id: format!("lg{}-{n}", self.nonce),
            format_version: Some(3),
            last_seq: None,
            app: Some("loadgen".to_owned()),
        });
        let started = Instant::now();
        ws.send(Message::Binary(hello))
            .await
            .context("sending hello")?;
        let (seq, open_time, welcome_bytes) = loop {
            let frame = ws
                .next()
                .await
                .context("the server closed the socket before welcome")??;
            let frame = match frame {
                Message::Binary(frame) => frame,
                Message::Close(c) => bail!("the server closed the socket before welcome: {c:?}"),
                Message::Text(_) => bail!("the server answered hello with a text frame"),
                _ => continue,
            };
            // Like v1, the clock stops once the welcome is read into a document, not when its bytes arrive.
            match wire::decode_server(&frame).context("the answer to hello was not a message")? {
                ServerMsg::Welcome { seq, content, .. } => {
                    if content.is_none() {
                        bail!(
                            "the welcome carried no document content (the server sent changes or nothing)"
                        );
                    }
                    break (seq, started.elapsed(), frame.len() as u64);
                }
                ServerMsg::Error { code, message, .. } => {
                    bail!("the server refused hello: {code}: {message}")
                }
                _ => {}
            }
        };

        let shared = Arc::new(Shared::new(seq));
        let (mut sink, mut stream) = ws.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

        let writer_shared = shared.clone();
        let writer = tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                if sink.send(m).await.is_err() {
                    writer_shared.close();
                    return;
                }
            }
            let _ = sink.close().await;
        });
        let reader_shared = shared.clone();
        let closes = self.closes.clone();
        let reader = tokio::spawn(async move {
            // 1006: the socket ended with no close frame.
            let mut code = "1006".to_owned();
            while let Some(frame) = stream.next().await {
                match frame {
                    Ok(Message::Binary(frame)) => on_frame(&reader_shared, &frame),
                    Ok(Message::Text(_)) => {
                        reader_shared
                            .protocol_errors
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Message::Close(c)) => {
                        code = c.map_or(1005, |c| u16::from(c.code)).to_string();
                        break;
                    }
                    Err(_) => break,
                    Ok(_) => {}
                }
            }
            // (This task is stopped when loadgen itself ends a peer, so only other closes get here.)
            note_close(&closes, &code);
            reader_shared.close();
        });
        let ping_tx = tx.clone();
        let ping = tokio::spawn(async move {
            let frame = wire::encode_client(&ClientMsg::Ping);
            let mut tick = tokio::time::interval(PING_EVERY);
            tick.tick().await;
            loop {
                tick.tick().await;
                if ping_tx.send(Message::Binary(frame.clone())).is_err() {
                    return;
                }
            }
        });

        Ok(Box::new(V2Peer {
            shared,
            tx,
            features: target.features.max(1),
            counter: 0,
            open_time,
            welcome_bytes,
            tasks: vec![reader, ping, writer],
        }))
    }
}

/// One frame from the server after the welcome. A frame that is not a message counts as a protocol error,
/// and so does an `error` message: this load sends nothing the server should object to.
fn on_frame(shared: &Shared, frame: &[u8]) {
    match wire::decode_server(frame) {
        Ok(ServerMsg::Change(ev)) => shared.bump_seq(ev.seq),
        Ok(ServerMsg::Accepted { change_id, seq }) => {
            shared.bump_seq(seq);
            shared.answer(&change_id, Outcome::Accepted);
        }
        Ok(ServerMsg::Rejected {
            change_id, code, ..
        }) => shared.answer(&change_id, Outcome::Rejected(code.as_str().to_owned())),
        Ok(ServerMsg::Error { .. }) | Err(_) => {
            shared.protocol_errors.fetch_add(1, Ordering::Relaxed);
        }
        Ok(_) => {}
    }
}

/// The change an edit sends: `features.<feature>.params.depth = depth`, made on `base`.
fn depth_change(change_id: &str, base: u64, feature: usize, depth: u64) -> Change {
    let path = ["features", &feature_id(feature), "params", "depth"]
        .iter()
        .map(|s| Arc::from(*s))
        .collect();
    Change {
        change_id: Arc::from(change_id),
        base,
        ops: vec![Op::Set {
            path,
            value: Value::Int(i64::try_from(depth).unwrap_or(i64::MAX)),
        }],
        reads: BTreeSet::new(),
        depends: Vec::new(),
        undo_of: None,
        command: Arc::from("bench"),
    }
}

struct V2Peer {
    shared: Arc<Shared>,
    tx: mpsc::UnboundedSender<Message>,
    features: usize,
    counter: u64,
    open_time: Duration,
    welcome_bytes: u64,
    tasks: Vec<JoinHandle<()>>,
}

impl V2Peer {
    fn send(&self, msg: &ClientMsg) -> Result<()> {
        if !self.shared.open.load(Ordering::Relaxed) {
            bail!("the connection is closed");
        }
        self.tx
            .send(Message::Binary(wire::encode_client(msg)))
            .map_err(|_| anyhow!("the connection is closed"))
    }
}

impl Drop for V2Peer {
    fn drop(&mut self) {
        // The writer ends (and closes the socket) once the senders are gone, so only the readers are stopped.
        for t in self.tasks.iter().take(2) {
            t.abort();
        }
    }
}

#[async_trait]
impl Peer for V2Peer {
    async fn edit(&mut self, feature: usize) -> Result<Outcome> {
        self.counter += 1;
        let change_id = uuid::Uuid::new_v4().simple().to_string();
        let (done, answer) = oneshot::channel();
        let _guard = PendingGuard {
            shared: &self.shared,
            id: change_id.clone(),
        };
        self.shared
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(change_id.clone(), done);
        let change = depth_change(
            &change_id,
            self.shared.seq.load(Ordering::Relaxed),
            feature % self.features,
            self.counter,
        );
        self.send(&ClientMsg::Change(change))?;
        answer
            .await
            .map_err(|_| anyhow!("the connection closed before the answer"))
    }

    async fn presence(&mut self) -> Result<()> {
        self.send(&ClientMsg::Presence {
            data: Plain::Map(vec![(Plain::from("tool"), Plain::from("bench"))]),
        })
    }

    async fn drain(&mut self) -> Result<()> {
        if self.shared.open.load(Ordering::Relaxed) {
            Ok(())
        } else {
            bail!("the connection is closed")
        }
    }

    fn open_time(&self) -> Duration {
        self.open_time
    }

    fn welcome_bytes(&self) -> u64 {
        self.welcome_bytes
    }

    fn is_open(&self) -> bool {
        self.shared.open.load(Ordering::Relaxed)
    }

    fn protocol_errors(&self) -> u64 {
        self.shared.protocol_errors.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use oswx_core::wire::{ChangeEvent, RejectCode};
    use oswx_server::{AppState, Settings};
    use tokio::net::TcpListener;

    use super::*;
    use crate::scenarios;

    // ------------------------------------------------------------------ frames, no server

    fn frame(msg: &ServerMsg) -> Vec<u8> {
        wire::encode_server(msg).to_vec()
    }

    fn waiting(shared: &Shared, id: &str) -> oneshot::Receiver<Outcome> {
        let (tx, rx) = oneshot::channel();
        shared.pending.lock().unwrap().insert(id.into(), tx);
        rx
    }

    #[test]
    fn answers_reach_the_waiting_edit_and_move_seq() {
        let s = Shared::new(0);
        let mut accepted = waiting(&s, "a");
        let mut rejected = waiting(&s, "r");
        on_frame(
            &s,
            &frame(&ServerMsg::Change(ChangeEvent {
                seq: 4,
                client_id: Arc::from("u.other"),
                author: "u".into(),
                author_name: "U".into(),
                change_id: Arc::from("x"),
                ops: depth_change("x", 3, 0, 1).ops,
                command: Arc::from("bench"),
                undo_of: None,
                reverts: None,
            })),
        );
        on_frame(
            &s,
            &frame(&ServerMsg::Accepted {
                change_id: Arc::from("a"),
                seq: 5,
            }),
        );
        on_frame(
            &s,
            &frame(&ServerMsg::Rejected {
                change_id: Arc::from("r"),
                code: RejectCode::Stale,
                message: "someone else changed it".into(),
                conflicts: Vec::new(),
            }),
        );
        assert_eq!(accepted.try_recv().unwrap(), Outcome::Accepted);
        assert_eq!(
            rejected.try_recv().unwrap(),
            Outcome::Rejected("stale".into())
        );
        assert_eq!(s.seq.load(Ordering::Relaxed), 5);
        assert_eq!(s.protocol_errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn garbage_and_error_messages_are_protocol_errors_not_panics() {
        let s = Shared::new(0);
        on_frame(&s, b"not messagepack at all");
        on_frame(&s, &[]);
        on_frame(&s, &[0xc1]); // a byte MessagePack never uses
        on_frame(&s, &[0x80]); // a map with no `t`
        on_frame(
            &s,
            &frame(&ServerMsg::Error {
                code: "bad_message".into(),
                message: "no".into(),
                request_id: None,
            }),
        );
        // An answer nobody waits for is not an error.
        on_frame(
            &s,
            &frame(&ServerMsg::Accepted {
                change_id: Arc::from("unknown"),
                seq: 1,
            }),
        );
        on_frame(&s, &frame(&ServerMsg::Pong { seq: 1 }));
        assert_eq!(s.protocol_errors.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn an_edit_is_one_set_of_the_features_depth_and_the_server_takes_it() {
        let change = depth_change("c1", 7, 3, 12);
        assert_eq!(change.base, 7);
        let sent = wire::encode_client(&ClientMsg::Change(change.clone()));
        // What the server reads back is what was meant (and it passes the server's own checks).
        let read = wire::decode_client(&sent, 1 << 20).unwrap();
        assert_eq!(read, ClientMsg::Change(change.clone()));
        let [Op::Set { path, value }] = &change.ops[..] else {
            panic!("one set, got {:?}", change.ops)
        };
        let path: Vec<&str> = path.iter().map(|s| &**s).collect();
        assert_eq!(path, ["features", "f5", "params", "depth"]);
        assert_eq!(value, &Value::Int(12));
    }

    // ------------------------------------------------------------------ against a server in this process

    struct Server {
        base: String,
        stop: Option<oneshot::Sender<()>>,
        task: Option<JoinHandle<()>>,
        _dir: tempfile::TempDir,
    }

    impl Server {
        async fn start(change: impl FnOnce(&mut Settings)) -> Server {
            let dir = tempfile::tempdir().unwrap();
            let folder = dir.path().to_string_lossy().replace('\\', "/");
            let mut settings = Settings {
                database_url: format!("sqlite://{folder}/collab.db"),
                storage_url: format!("{folder}/storage"),
                ..Settings::default()
            };
            change(&mut settings);
            let state = AppState::from_settings(settings).await.unwrap();
            let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let (stop, stopping) = oneshot::channel::<()>();
            let task = tokio::spawn(async move {
                oswx_server::serve::serve(listener, state, async {
                    let _ = stopping.await;
                })
                .await;
            });
            Server {
                base,
                stop: Some(stop),
                task: Some(task),
                _dir: dir,
            }
        }

        /// Stops the server in order (its database is closed before the folder goes).
        async fn stop(mut self) {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
            if let Some(task) = self.task.take() {
                let _ = task.await;
            }
        }
    }

    fn v2() -> (Arc<V2>, Arc<dyn Dialect>) {
        let v2 = Arc::new(V2::new());
        (v2.clone(), v2)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn editors_of_their_own_features_have_every_edit_accepted() {
        let server = Server::start(|_| {}).await;
        let (_, dialect) = v2();
        let target = dialect.setup(&server.base, 1, 4, 0).await.unwrap();
        assert!(
            target.server.contains("(protocol 2)"),
            "the server says what it is: {}",
            target.server
        );
        // 3 editors, 10 slots each: every 100 ms for a second.
        let run = scenarios::edit_run(
            dialect.clone(),
            target.clone(),
            3,
            10.0,
            Duration::from_secs(1),
        )
        .await;
        let m = &run.metrics;
        assert_eq!((m.sent, m.accepted), (30, 30));
        assert_eq!(
            (
                m.rejected_total(),
                m.timeouts,
                m.errors,
                m.dropped,
                m.connect_failed
            ),
            (0, 0, 0, 0, 0)
        );
        assert_eq!(m.protocol_errors, 0);
        assert_eq!(m.latency().unwrap().count, 30);
        assert_eq!(dialect.closes(), Some(BTreeMap::new()));

        // The edits are in the document: a new connection is welcomed at seq 30.
        let mut ws = open_socket(&target, 0).await.unwrap();
        let hello = ClientMsg::Hello {
            protocol: PROTOCOL_VERSION,
            token: target.token.clone(),
            client_id: "checker-1".into(),
            format_version: Some(3),
            last_seq: None,
            app: None,
        };
        ws.send(Message::Binary(wire::encode_client(&hello)))
            .await
            .unwrap();
        let Some(Ok(Message::Binary(welcome))) = ws.next().await else {
            panic!("no welcome")
        };
        let ServerMsg::Welcome { seq, content, .. } = wire::decode_server(&welcome).unwrap() else {
            panic!("not a welcome")
        };
        assert_eq!(seq, 30);
        // Each editor counted its own feature's depth up to 10.
        let content = content.unwrap();
        for feature in ["f2", "f3", "f4"] {
            assert_eq!(
                content.at(&["features", feature, "params", "depth"]),
                Some(&Value::Int(10))
            );
        }
        drop(ws);
        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn editors_of_one_feature_are_accepted_or_refused_as_stale_and_nothing_is_lost() {
        let server = Server::start(|_| {}).await;
        let (_, dialect) = v2();
        // One feature, four editors: all write the same path.
        let target = dialect.setup(&server.base, 1, 1, 0).await.unwrap();
        let run =
            scenarios::edit_run(dialect.clone(), target, 4, 20.0, Duration::from_secs(1)).await;
        let m = &run.metrics;
        assert_eq!(m.sent, 80);
        assert_eq!(m.sent, m.accepted + m.rejected_total());
        assert!(m.accepted > 0);
        assert!(
            m.rejected.keys().all(|code| code == "stale"),
            "{:?}",
            m.rejected
        );
        assert_eq!((m.timeouts, m.errors, m.dropped), (0, 0, 0));
        assert_eq!(m.protocol_errors, 0);
        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_connection_the_server_refuses_is_counted_and_the_others_run() {
        // The account may hold two connections; the third is refused (`too_many_connections`).
        let server = Server::start(|s| s.max_connections_per_user = 2).await;
        let (_, dialect) = v2();
        let target = dialect.setup(&server.base, 1, 4, 0).await.unwrap();
        let run =
            scenarios::edit_run(dialect.clone(), target, 3, 10.0, Duration::from_secs(1)).await;
        let m = &run.metrics;
        assert_eq!(m.connect_failed, 1);
        assert_eq!((m.sent, m.accepted), (20, 20));
        assert_eq!(m.dropped, 0);
        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn opening_a_document_brings_all_of_it() {
        let server = Server::start(|_| {}).await;
        let (_, dialect) = v2();
        let target = dialect.setup(&server.base, 1, 10, 200_000).await.unwrap();
        assert!(target.document_bytes.abs_diff(200_000) < 10_000);
        let run = scenarios::open_run(dialect.clone(), target.clone(), 4)
            .await
            .unwrap();
        let m = &run.metrics;
        assert_eq!(m.connect_failed, 0);
        assert!(m.first_open.is_some());
        assert_eq!(m.latency().unwrap().count, 3);
        // MessagePack is a little smaller than JSON, but the padding strings are all there.
        let (smallest, _) = m.welcome_bytes.unwrap();
        assert!(smallest > 190_000, "welcome of {smallest} bytes");
        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_connections_stay_open_and_a_stopped_server_is_seen() {
        let server = Server::start(|_| {}).await;
        let (v2, dialect) = v2();
        let target = dialect.setup(&server.base, 1, 1, 0).await.unwrap();
        let run = scenarios::idle_run(
            dialect.clone(),
            target.clone(),
            5,
            Duration::from_millis(300),
            &|| Some(1),
        )
        .await;
        let m = &run.metrics;
        assert_eq!((m.connect_failed, m.dropped, m.protocol_errors), (0, 0, 0));
        assert_eq!(v2.closes(), Some(BTreeMap::new()));

        // A peer whose server stops sees the close, its code is recorded, and its edit fails.
        let mut peer = dialect.connect(&target, 100).await.unwrap();
        peer.presence().await.unwrap();
        assert_eq!(peer.edit(0).await.unwrap(), Outcome::Accepted);
        server.stop().await;
        let closed = async {
            while peer.is_open() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(20), closed)
            .await
            .expect("the peer sees the server go");
        assert!(peer.edit(0).await.is_err());
        assert!(peer.drain().await.is_err());
        // 1001: the server said it was going away.
        assert_eq!(v2.closes(), Some(BTreeMap::from([("1001".to_owned(), 1)])));
        assert!(dialect.connect(&target, 101).await.is_err());
    }
}
