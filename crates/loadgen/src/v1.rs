//! The v1 dialect: REST plus the JSON WebSocket protocol of the Python server (docs/protocol.md).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::dialect::{Dialect, Outcome, Peer, Target};
use crate::doc::{build_document, feature_id};

const EMAIL: &str = "loadgen@example.com";
const PASSWORD: &str = "loadgen password 1";
/// The server wants a ping at least every 45 s; 15 s is what docs/protocol.md says.
const PING_EVERY: Duration = Duration::from_secs(15);

pub struct V1 {
    /// Makes client ids unique per run, so two runs never disconnect each other (4409).
    nonce: String,
}

impl V1 {
    pub fn new() -> Self {
        Self {
            nonce: uuid::Uuid::new_v4().simple().to_string()[..8].to_owned(),
        }
    }
}

async fn json_or_fail(what: &str, resp: reqwest::Response) -> Result<Value> {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!(
            "{what}: the server answered {status}: {}",
            body.chars().take(300).collect::<String>()
        );
    }
    serde_json::from_str(&body).with_context(|| format!("{what}: the answer was not JSON"))
}

#[async_trait]
impl Dialect for V1 {
    async fn setup(
        &self,
        base_url: &str,
        docs: usize,
        features: usize,
        doc_bytes: usize,
    ) -> Result<Target> {
        if features == 0 {
            bail!("--features must be at least 1");
        }
        let base = base_url.trim_end_matches('/');
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;

        let info = http
            .get(format!("{base}/api/server"))
            .send()
            .await
            .map_err(|e| anyhow!("cannot reach the server at {base}: {e}"))?;
        let info = json_or_fail("GET /api/server", info).await?;
        let server = format!(
            "{} {} (protocol {})",
            info["name"].as_str().unwrap_or("unknown server"),
            info["version"].as_str().unwrap_or("?"),
            info["protocol"]
        );

        let creds = json!({"email": EMAIL, "password": PASSWORD, "name": "Loadgen"});
        let resp = http
            .post(format!("{base}/api/auth/register"))
            .json(&creds)
            .send()
            .await?;
        let token = if resp.status().is_success() {
            json_or_fail("register", resp).await?
        } else {
            // The first user exists already (an earlier scenario on the same server): sign in as it.
            let login = http
                .post(format!("{base}/api/auth/login"))
                .json(&json!({"email": EMAIL, "password": PASSWORD}))
                .send()
                .await?;
            if !login.status().is_success() {
                bail!(
                    "the server has users but not the loadgen account; start it on a fresh database \
                     (registration answered {})",
                    resp.status()
                );
            }
            json_or_fail("login", login).await?
        };
        let token = token["token"]
            .as_str()
            .context("no token in the sign-in answer")?
            .to_owned();

        let auth = format!("Bearer {token}");
        let run = &self.nonce;
        let org = http
            .post(format!("{base}/api/orgs"))
            .header("Authorization", &auth)
            .json(&json!({"name": format!("Loadgen {run}")}))
            .send()
            .await?;
        let org = json_or_fail("create organisation", org).await?;
        let org = org["id"].as_str().context("no organisation id")?;
        let project = http
            .post(format!("{base}/api/orgs/{org}/projects"))
            .header("Authorization", &auth)
            .json(&json!({"name": format!("Bench {run}")}))
            .send()
            .await?;
        let project = json_or_fail("create project", project).await?;
        let project = project["id"].as_str().context("no project id")?;

        let content = build_document(features, doc_bytes);
        let mut documents = Vec::with_capacity(docs);
        for i in 0..docs {
            let doc = http
                .post(format!("{base}/api/projects/{project}/documents"))
                .header("Authorization", &auth)
                .json(&json!({"name": format!("Bench-{i}"), "content": content}))
                .send()
                .await?;
            let doc = json_or_fail("create document", doc).await?;
            documents.push(doc["id"].as_str().context("no document id")?.to_owned());
        }
        Ok(Target {
            base_url: base.to_owned(),
            token,
            documents,
            features,
            server,
        })
    }

    async fn connect(&self, target: &Target, n: usize) -> Result<Box<dyn Peer>> {
        if target.documents.is_empty() {
            bail!("the target has no documents");
        }
        let doc = &target.documents[n % target.documents.len()];
        let ws_base = if let Some(rest) = target.base_url.strip_prefix("https://") {
            format!("wss://{rest}")
        } else if let Some(rest) = target.base_url.strip_prefix("http://") {
            format!("ws://{rest}")
        } else {
            bail!("the url must start with http:// or https://");
        };
        let config = WebSocketConfig::default()
            .max_message_size(Some(1 << 30))
            .max_frame_size(None);
        let (mut ws, _) = tokio_tungstenite::connect_async_with_config(
            format!("{ws_base}/api/documents/{doc}/ws"),
            Some(config),
            false,
        )
        .await
        .context("WebSocket handshake")?;

        let hello = json!({
            "type": "hello", "protocol": 1, "token": target.token,
            "client_id": format!("lg{}-{n}", self.nonce), "format_version": 3, "last_seq": null,
            "app": "loadgen",
        });
        let started = Instant::now();
        ws.send(Message::text(hello.to_string()))
            .await
            .context("sending hello")?;
        let (seq, open_time) = loop {
            let frame = ws
                .next()
                .await
                .context("the server closed the socket before welcome")??;
            let Message::Text(text) = frame else {
                if let Message::Close(c) = frame {
                    bail!("the server closed the socket before welcome: {c:?}");
                }
                continue;
            };
            let v: Value = serde_json::from_str(text.as_str()).context("welcome was not JSON")?;
            match v["type"].as_str() {
                Some("welcome") => break (v["seq"].as_u64().unwrap_or(0), started.elapsed()),
                Some("error") => bail!("the server refused hello: {v}"),
                _ => {}
            }
        };

        let shared = Arc::new(Shared {
            seq: AtomicU64::new(seq),
            open: AtomicBool::new(true),
            protocol_errors: AtomicU64::new(0),
            pending: Mutex::new(HashMap::new()),
        });
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
        let reader = tokio::spawn(async move {
            while let Some(frame) = stream.next().await {
                match frame {
                    Ok(Message::Text(text)) => reader_shared.on_text(text.as_str()),
                    Ok(Message::Binary(_)) => {
                        reader_shared
                            .protocol_errors
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            reader_shared.close();
        });
        let ping_tx = tx.clone();
        let ping = tokio::spawn(async move {
            let mut tick = tokio::time::interval(PING_EVERY);
            tick.tick().await;
            loop {
                tick.tick().await;
                if ping_tx
                    .send(Message::text(json!({"type": "ping"}).to_string()))
                    .is_err()
                {
                    return;
                }
            }
        });

        Ok(Box::new(V1Peer {
            shared,
            tx,
            features: target.features.max(1),
            counter: 0,
            open_time,
            tasks: vec![reader, ping, writer],
        }))
    }
}

struct Shared {
    seq: AtomicU64,
    open: AtomicBool,
    protocol_errors: AtomicU64,
    pending: Mutex<HashMap<String, oneshot::Sender<Outcome>>>,
}

impl Shared {
    fn close(&self) {
        self.open.store(false, Ordering::Relaxed);
        // Dropping the senders wakes every waiting edit with an error.
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    fn bump_seq(&self, seq: u64) {
        self.seq.fetch_max(seq, Ordering::Relaxed);
    }

    fn answer(&self, change_id: &str, outcome: Outcome) {
        let waiter = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(change_id);
        if let Some(w) = waiter {
            let _ = w.send(outcome);
        }
    }

    fn on_text(&self, text: &str) {
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            self.protocol_errors.fetch_add(1, Ordering::Relaxed);
            return;
        };
        match v["type"].as_str() {
            Some("change") => self.bump_seq(v["seq"].as_u64().unwrap_or(0)),
            Some("change.accepted") => {
                self.bump_seq(v["seq"].as_u64().unwrap_or(0));
                if let Some(id) = v["change_id"].as_str() {
                    self.answer(id, Outcome::Accepted);
                }
            }
            Some("change.rejected") => {
                if let Some(id) = v["change_id"].as_str() {
                    self.answer(
                        id,
                        Outcome::Rejected(v["code"].as_str().unwrap_or("unknown").to_owned()),
                    );
                }
            }
            Some(_) => {}
            None => {
                self.protocol_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

struct V1Peer {
    shared: Arc<Shared>,
    tx: mpsc::UnboundedSender<Message>,
    features: usize,
    counter: u64,
    open_time: Duration,
    tasks: Vec<JoinHandle<()>>,
}

impl V1Peer {
    fn send(&self, v: &Value) -> Result<()> {
        if !self.shared.open.load(Ordering::Relaxed) {
            bail!("the connection is closed");
        }
        self.tx
            .send(Message::text(v.to_string()))
            .map_err(|_| anyhow!("the connection is closed"))
    }
}

impl Drop for V1Peer {
    fn drop(&mut self) {
        // The writer ends (and closes the socket) once the senders are gone, so only the readers are stopped.
        for t in self.tasks.iter().take(2) {
            t.abort();
        }
    }
}

#[async_trait]
impl Peer for V1Peer {
    async fn edit(&mut self, feature: usize) -> Result<Outcome> {
        self.counter += 1;
        let change_id = uuid::Uuid::new_v4().to_string();
        let (done, answer) = oneshot::channel();
        self.shared
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(change_id.clone(), done);
        let msg = json!({
            "type": "change", "change_id": change_id,
            "base_seq": self.shared.seq.load(Ordering::Relaxed),
            "ops": [{
                "op": "feature.update", "id": feature_id(feature % self.features),
                "set": [[["params", "depth"], self.counter]], "unset": [],
            }],
            "command": "bench",
        });
        if let Err(e) = self.send(&msg) {
            self.shared
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&change_id);
            return Err(e);
        }
        answer
            .await
            .map_err(|_| anyhow!("the connection closed before the answer"))
    }

    async fn presence(&mut self) -> Result<()> {
        self.send(&json!({"type": "presence", "data": {"tool": "bench"}}))
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

    fn is_open(&self) -> bool {
        self.shared.open.load(Ordering::Relaxed)
    }

    fn protocol_errors(&self) -> u64 {
        self.shared.protocol_errors.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared() -> Shared {
        Shared {
            seq: AtomicU64::new(0),
            open: AtomicBool::new(true),
            protocol_errors: AtomicU64::new(0),
            pending: Mutex::new(HashMap::new()),
        }
    }

    #[test]
    fn answers_reach_the_waiting_edit_and_move_seq() {
        let s = shared();
        let (a, mut a_rx) = oneshot::channel();
        let (r, mut r_rx) = oneshot::channel();
        s.pending.lock().unwrap().insert("a".into(), a);
        s.pending.lock().unwrap().insert("r".into(), r);
        s.on_text(r#"{"type":"change","seq":4,"author":"u","ops":[]}"#);
        s.on_text(r#"{"type":"change.accepted","change_id":"a","seq":5}"#);
        s.on_text(r#"{"type":"change.rejected","change_id":"r","code":"stale"}"#);
        assert_eq!(a_rx.try_recv().unwrap(), Outcome::Accepted);
        assert_eq!(r_rx.try_recv().unwrap(), Outcome::Rejected("stale".into()));
        assert_eq!(s.seq.load(Ordering::Relaxed), 5);
        assert_eq!(s.protocol_errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn seq_never_moves_backwards() {
        let s = shared();
        s.on_text(r#"{"type":"change","seq":9}"#);
        s.on_text(r#"{"type":"change","seq":3}"#);
        assert_eq!(s.seq.load(Ordering::Relaxed), 9);
    }

    #[test]
    fn garbage_is_a_protocol_error_not_a_panic() {
        let s = shared();
        s.on_text("not json at all");
        s.on_text("[1, 2]");
        s.on_text(r#"{"type":"change.accepted"}"#); // no change_id: ignored
        s.on_text(r#"{"type":"change.accepted","change_id":"unknown","seq":1}"#);
        assert_eq!(s.protocol_errors.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn closing_wakes_waiting_edits_with_an_error() {
        let s = shared();
        let (tx, mut rx) = oneshot::channel::<Outcome>();
        s.pending.lock().unwrap().insert("x".into(), tx);
        s.close();
        assert!(rx.try_recv().is_err());
        assert!(!s.open.load(Ordering::Relaxed));
    }
}
