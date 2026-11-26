//! Email, when the operator has set up SMTP (`OSWX_COLLAB_SMTP_*`). Without it the server sends
//! nothing and works as before: invites are links people pass on, forgotten passwords are reset by
//! the operator.
//!
//! As in the Python server, a handler awaits the send and then answers the same whatever happened: a
//! failure is logged for the operator, never shown to the person (someone asking for a reset link is
//! told the same thing whether or not one was sent).
//!
//! **Whose certificates are trusted.** The mail server's certificate must lead to one of Mozilla's
//! roots (built into the binary, so a container without a certificate bundle can still reach a
//! public mail service) or to a root in the machine's own trust store (so a self-hosted relay with
//! a certificate from a private authority works once that authority is installed there; on Linux
//! `SSL_CERT_FILE` and `SSL_CERT_DIR` say where the store is). The certificate must be for the
//! name in `OSWX_COLLAB_SMTP_HOST`. There is no setting that turns the check off.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use lettre::message::header::ContentType;
use lettre::message::{Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::client::{Certificate, CertificateStore, Tls, TlsParameters};
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use crate::config::{Settings, SmtpSecurity};
use crate::state::AppState;

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
        Ok(()) => {
            crate::metrics::mail_sent();
            true
        }
        Err(err) => {
            crate::metrics::mail_failed();
            tracing::warn!(
                "could not send email to the mail server {}: {err:#}",
                settings.smtp_host
            );
            false
        }
    }
}

/// How long a request waits for the mail it sends before it answers without knowing.
pub const MAIL_WAIT: Duration = Duration::from_secs(10);

/// Sends a message for a request that has already committed, without letting the mail server
/// hold the request up: the send runs in a task of its own ([`send_logged`]: a failure is logged
/// and counted), and the request waits for it for [`MAIL_WAIT`] at most. Answers whether the
/// message was handed to the mail server **within that time**; `false` also when it is still
/// being sent (it goes on, and shutdown waits a while for it).
///
/// A mail server can take a minute to refuse or accept (20 s a step). Awaited in the handler,
/// that passed the request limit and answered 503 "try again" for something already stored.
pub async fn send_after_commit(
    state: &AppState,
    to: String,
    subject: String,
    text: String,
) -> bool {
    let wait = state.room_hooks.mail_wait;
    let sending = {
        let state = state.clone();
        state.clone().in_background(async move {
            send_logged(state.mailer.as_ref(), &state.settings, &to, &subject, &text).await
        })
    };
    matches!(tokio::time::timeout(wait, sending).await, Ok(Ok(true)))
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

/// How a mail server called `host` is verified: its certificate must be for that name and lead
/// to one of Mozilla's roots or to a root of the machine's trust store (see the module
/// documentation).
fn tls_parameters(host: &str) -> anyhow::Result<TlsParameters> {
    let mut parameters =
        TlsParameters::builder(host.to_owned()).certificate_store(CertificateStore::WebpkiRoots);
    for root in machine_roots() {
        parameters = parameters.add_root_certificate(Certificate::from_der(root)?);
    }
    parameters
        .build_rustls()
        .context("the TLS settings for the mail server could not be made")
}

/// The roots of the machine's trust store that can be used, in DER. One that cannot be read is
/// left out (a store often holds a few), and a store that cannot be read at all gives none:
/// Mozilla's roots are still there, so neither stops the server.
fn machine_roots() -> Vec<Vec<u8>> {
    let found = rustls_native_certs::load_native_certs();
    let mut usable = rustls::RootCertStore::empty();
    let roots: Vec<Vec<u8>> = found
        .certs
        .into_iter()
        .filter(|root| usable.add(root.clone()).is_ok())
        .map(|root| root.to_vec())
        .collect();
    if !found.errors.is_empty() {
        tracing::warn!(
            "part of the machine's trust store could not be read ({} errors); {} of its roots \
             are trusted for the mail server",
            found.errors.len(),
            roots.len()
        );
    }
    roots
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
            // (`builder_dangerous` only means "no TLS unless one is set", which is done here.)
            let builder = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host);
            let builder = match settings.smtp_security {
                SmtpSecurity::Ssl => builder.tls(Tls::Wrapper(tls_parameters(host)?)),
                SmtpSecurity::StartTls => builder.tls(Tls::Required(tls_parameters(host)?)),
                SmtpSecurity::Plain => builder,
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
