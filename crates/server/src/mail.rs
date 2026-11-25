//! Email, when the operator has set up SMTP (`OSWX_COLLAB_SMTP_*`). Without it the server sends
//! nothing and works as before: invites are links people pass on, forgotten passwords are reset by
//! the operator.
//!
//! As in the Python server, a handler awaits the send and then answers the same whatever happened: a
//! failure is logged for the operator, never shown to the person (someone asking for a reset link is
//! told the same thing whether or not one was sent).

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use lettre::message::header::ContentType;
use lettre::message::{Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use crate::config::{Settings, SmtpSecurity};

#[async_trait]
pub trait Mailer: Send + Sync + 'static {
    /// Whether this mailer can send anything (`GET /api/server` reports it as `email`).
    fn configured(&self) -> bool;

    /// Sends one message: plain text, with an HTML alternative if `html` is given.
    async fn send(
        &self,
        to: &str,
        subject: &str,
        text: &str,
        html: Option<&str>,
    ) -> anyhow::Result<()>;
}

/// Sends through `mailer` and logs a failure (with the mail server's name, never the message);
/// answers whether the message was handed to the mail server. What handlers call.
pub async fn send_logged(
    mailer: &dyn Mailer,
    settings: &Settings,
    to: &str,
    subject: &str,
    text: &str,
) -> bool {
    if !mailer.configured() {
        return false;
    }
    match mailer.send(to, subject, text, None).await {
        Ok(()) => true,
        Err(err) => {
            tracing::warn!(
                "could not send email to the mail server {}: {err:#}",
                settings.smtp_host
            );
            false
        }
    }
}

/// The invitation email: subject and body.
pub fn invite_text(org: &str, inviter: &str, url: &str, days: u32) -> (String, String) {
    (
        format!("{inviter} invited you to {org} on openswx"),
        format!(
            "{inviter} has invited you to collaborate in {org} on openswx.\n\n\
             To accept, open openswx, choose Collaborate > Join with invite, and paste this link:\n\n{url}\n\n\
             You can create your account there. The invite works once and expires in {days} days.\n\n\
             If you were not expecting this, you can ignore this email.\n"
        ),
    )
}

/// The password reset email: subject and body.
pub fn reset_text(url: &str, minutes: u32) -> (String, String) {
    (
        "Reset your openswx collaboration password".to_owned(),
        format!(
            "Someone (hopefully you) asked to reset the password of the openswx collaboration account \
             for this address.\n\nTo choose a new password, open this link within {minutes} minutes:\n\n\
             {url}\n\nSetting a new password signs you out everywhere. If you did not ask for this, \
             ignore this email: nothing changes.\n"
        ),
    )
}

/// How long a connection to the mail server may take, per step (the Python server's 20 s).
const SMTP_TIMEOUT: Duration = Duration::from_secs(20);

/// Sends through the operator's mail server, one connection per message.
pub struct SmtpMailer {
    host: String,
    from: String,
    from_name: String,
    /// `None` when no mail server is configured.
    transport: Option<AsyncSmtpTransport<Tokio1Executor>>,
}

impl std::fmt::Debug for SmtpMailer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmtpMailer")
            .field("host", &self.host)
            .field("from", &self.from)
            .finish_non_exhaustive()
    }
}

impl SmtpMailer {
    /// A mailer for the `smtp_*` settings. It is [`Mailer::configured`] when `smtp_host` and
    /// `smtp_from` are set; `smtp_security` is `ssl` (TLS from the first byte), `starttls` (plain,
    /// then upgraded; required) or `none`; credentials are sent when `smtp_user` is set.
    pub fn new(settings: &Settings) -> anyhow::Result<Self> {
        let transport = if settings.email_configured() {
            let host = settings.smtp_host.as_str();
            let builder = match settings.smtp_security {
                SmtpSecurity::Ssl => AsyncSmtpTransport::<Tokio1Executor>::relay(host)?,
                SmtpSecurity::StartTls => {
                    AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)?
                }
                SmtpSecurity::Plain => {
                    AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host)
                }
            };
            let mut builder = builder.port(settings.smtp_port).timeout(Some(SMTP_TIMEOUT));
            if !settings.smtp_user.is_empty() {
                builder = builder.credentials(Credentials::new(
                    settings.smtp_user.clone(),
                    settings.smtp_password.clone(),
                ));
            }
            Some(builder.build())
        } else {
            None
        };
        Ok(Self {
            host: settings.smtp_host.clone(),
            from: settings.smtp_from.clone(),
            from_name: settings.smtp_from_name.clone(),
            transport,
        })
    }

    /// The message as it is sent: `From` is the sender with its name (if one is set), `Message-ID`
    /// uses the sender's domain, and the body is plain text, or plain text and HTML alternatives.
    pub fn message(
        &self,
        to: &str,
        subject: &str,
        text: &str,
        html: Option<&str>,
    ) -> anyhow::Result<Message> {
        let address = self
            .from
            .parse()
            .context("OSWX_COLLAB_SMTP_FROM is not an email address")?;
        let name = (!self.from_name.is_empty()).then(|| self.from_name.clone());
        let domain = self.from.rsplit('@').next().unwrap_or_default();
        let builder = Message::builder()
            .from(Mailbox::new(name, address))
            .to(to.parse::<Mailbox>().context("not an email address")?)
            .subject(subject)
            .message_id(Some(format!(
                "<{}@{domain}>",
                uuid::Uuid::new_v4().simple()
            )));
        let message = match html {
            None => builder.singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_PLAIN)
                    .body(text.to_owned()),
            )?,
            Some(html) => builder.multipart(MultiPart::alternative_plain_html(
                text.to_owned(),
                html.to_owned(),
            ))?,
        };
        Ok(message)
    }
}

#[async_trait]
impl Mailer for SmtpMailer {
    fn configured(&self) -> bool {
        self.transport.is_some()
    }

    async fn send(
        &self,
        to: &str,
        subject: &str,
        text: &str,
        html: Option<&str>,
    ) -> anyhow::Result<()> {
        let Some(transport) = &self.transport else {
            bail!("no mail server is configured");
        };
        let message = self.message(to, subject, text, html)?;
        transport.send(message).await?;
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_emails_say_what_the_python_server_says() {
        let (subject, body) = invite_text("Robotics", "Ada", "http://x/invite/t", 7);
        assert_eq!(subject, "Ada invited you to Robotics on openswx");
        assert!(body.contains("Ada has invited you to collaborate in Robotics on openswx."));
        assert!(body.contains("\n\nhttp://x/invite/t\n\n"));
        assert!(body.contains("The invite works once and expires in 7 days."));
        assert!(body.contains("If you were not expecting this, you can ignore this email."));

        let (subject, body) = reset_text("http://x/reset-password/t", 60);
        assert_eq!(subject, "Reset your openswx collaboration password");
        assert!(
            body.contains("open this link within 60 minutes:\n\nhttp://x/reset-password/t\n\n")
        );
        assert!(body.contains("Setting a new password signs you out everywhere."));
        assert!(!body.contains("  "), "no doubled spaces from line joins");
    }
}
