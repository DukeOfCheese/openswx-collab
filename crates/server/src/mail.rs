//! Email, when the operator has set up SMTP.
//!
//! Placeholder: Task 4 adds `SmtpMailer` and the messages; until then the server uses `MemoryMailer`.

use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;

#[async_trait]
pub trait Mailer: Send + Sync + 'static {
    /// Whether this mailer can send anything (`GET /api/server` reports it as `email`).
    fn configured(&self) -> bool;

    /// Sends one message.
    async fn send(
        &self,
        to: &str,
        subject: &str,
        text: &str,
        html: Option<&str>,
    ) -> anyhow::Result<()>;
}

/// A message `MemoryMailer` was asked to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sent {
    pub to: String,
    pub subject: String,
    pub text: String,
    pub html: Option<String>,
}

/// A mailer that keeps what it is asked to send, for tests to read.
#[derive(Debug, Default)]
pub struct MemoryMailer {
    configured: bool,
    sent: Mutex<Vec<Sent>>,
}

impl MemoryMailer {
    /// A mailer that says it is `configured` or not.
    pub fn new(configured: bool) -> Self {
        Self {
            configured,
            sent: Mutex::new(Vec::new()),
        }
    }

    /// Every message sent so far, oldest first.
    pub fn sent(&self) -> Vec<Sent> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Mailer for MemoryMailer {
    fn configured(&self) -> bool {
        self.configured
    }

    async fn send(
        &self,
        to: &str,
        subject: &str,
        text: &str,
        html: Option<&str>,
    ) -> anyhow::Result<()> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Sent {
                to: to.to_owned(),
                subject: subject.to_owned(),
                text: text.to_owned(),
                html: html.map(str::to_owned),
            });
        Ok(())
    }
}
