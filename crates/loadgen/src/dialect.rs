//! What a load run needs from a server, independent of its wire format. `v1` is the JSON protocol of the
//! Python server, `v2` the MessagePack protocol of the Rust server.

use std::time::Duration;

use async_trait::async_trait;

#[async_trait]
pub trait Dialect: Send + Sync + 'static {
    /// `v1` or `v2`, for the report.
    fn name(&self) -> &'static str;

    /// How many connections the server (or the network) closed while their peers were still in use, by
    /// WebSocket close code (`1006`: the socket ended without one). `None`: this dialect does not record it.
    fn closes(&self) -> Option<std::collections::BTreeMap<String, u64>> {
        None
    }

    /// Registers (or signs in) the first user and creates an organisation, a project and `docs` documents
    /// of `features` features, about `doc_bytes` bytes each.
    async fn setup(
        &self,
        base_url: &str,
        docs: usize,
        features: usize,
        doc_bytes: usize,
    ) -> anyhow::Result<Target>;

    /// Opens one connection on document `target.documents[n % documents.len()]`. `n` also makes the
    /// connection's client id unique.
    async fn connect(&self, target: &Target, n: usize) -> anyhow::Result<Box<dyn Peer>>;
}

#[async_trait]
pub trait Peer: Send {
    /// One small change to `feature` (taken modulo the document's feature count); waits for the answer.
    async fn edit(&mut self, feature: usize) -> anyhow::Result<Outcome>;
    async fn presence(&mut self) -> anyhow::Result<()>;
    /// Read and discard broadcasts. The peer reads its socket in the background anyway, so this only
    /// reports whether the connection is still usable.
    async fn drain(&mut self) -> anyhow::Result<()>;
    /// Time from sending `hello` until `welcome` arrived.
    fn open_time(&self) -> Duration;
    /// Size of the `welcome` frame in bytes.
    fn welcome_bytes(&self) -> u64;
    /// False once the server (or the network) closed the connection.
    fn is_open(&self) -> bool;
    /// Frames received that were not valid messages.
    fn protocol_errors(&self) -> u64;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Accepted,
    /// The server's rejection code (`stale`, `locked`, ...).
    Rejected(String),
}

#[derive(Clone)]
pub struct Target {
    pub base_url: String,
    pub token: String,
    pub documents: Vec<String>,
    pub features: usize,
    /// Size of each created document, JSON bytes.
    pub document_bytes: usize,
    /// Human-readable description of the server, for the report.
    pub server: String,
}

/// Written by hand so that printing a target never prints its token.
impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Target")
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .field("documents", &self.documents)
            .field("features", &self.features)
            .field("document_bytes", &self.document_bytes)
            .field("server", &self.server)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn printing_a_target_does_not_print_its_token() {
        let t = Target {
            base_url: "http://host:1".into(),
            token: "s3cret-token".into(),
            documents: vec!["d1".into()],
            features: 3,
            document_bytes: 100,
            server: "test".into(),
        };
        let shown = format!("{t:?} {t:#?}");
        assert!(!shown.contains("s3cret-token"), "{shown}");
        assert!(shown.contains("<redacted>") && shown.contains("http://host:1"));
    }
}
