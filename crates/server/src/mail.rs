//! Email, when the operator has set up SMTP (`OSWX_COLLAB_SMTP_*`). Without it the server sends
//! nothing and works as before: invites are links people pass on, forgotten passwords are reset by
//! the operator.
//!
//! As in the Python server, a handler answers the same whatever happened to the send: a failure is
//! logged for the operator (without the address), never shown to the person (someone asking for a
//! reset link is told the same thing whether or not one was sent). Unlike it, a handler waits for
//! its send for a few seconds at most ([`send_after_commit`]), and only so many messages are on
//! their way at once ([`MailSends`]).
//!
//! **Whose certificates are trusted.** The mail server's certificate must lead to one of Mozilla's
//! roots (built into the binary, so a container without a certificate bundle can still reach a
//! public mail service) or to a root in the machine's own trust store (so a self-hosted relay with
//! a certificate from a private authority works once that authority is installed there; on Linux
//! `SSL_CERT_FILE` and `SSL_CERT_DIR` say where the store is). The certificate must be for the
//! name in `OSWX_COLLAB_SMTP_HOST`. There is no setting that turns the check off.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use lettre::message::header::ContentType;
use lettre::message::{Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::client::{Certificate, CertificateStore, Tls, TlsParameters};
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use tokio::sync::{Semaphore, watch};

use crate::config::{Settings, SmtpSecurity};
use crate::state::{AppState, InFlight};

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

/// Sends through `mailer` and logs a failure (with the mail server's name, never the message nor
/// the address it was for); answers whether the message was handed to the mail server. A send
/// that panics counts as one that failed.
pub async fn send_logged(
    mailer: &dyn Mailer,
    settings: &Settings,
    to: &str,
    subject: &str,
    text: &str,
) -> bool {
    use futures_util::FutureExt as _;
    if !mailer.configured() {
        return false;
    }
    let sending = std::panic::AssertUnwindSafe(mailer.send(to, subject, text, None));
    let why = match sending.catch_unwind().await {
        Ok(Ok(())) => {
            crate::metrics::mail_sent();
            return true;
        }
        Ok(Err(err)) => without_address(&format!("{err:#}"), to, failure_class(&err)),
        Err(_) => "the mailer panicked".to_owned(),
    };
    crate::metrics::mail_failed();
    tracing::warn!(
        "could not send email to the mail server {}: {why}",
        settings.smtp_host
    );
    false
}

/// What a failure is logged as: `text` with every spelling of the address `to` (in any case)
/// replaced by `<the recipient>`, since a mail server's refusal often quotes the address it
/// refused. When that cannot be sure of leaving nothing of the address (`to` is not ASCII, or
/// its local part or its domain, in any case or, for an internationalised domain, in either of
/// its forms, is still in what is left), only a generic reason, the kind of failure (`class`,
/// [`failure_class`]) and the SMTP code are logged.
fn without_address(text: &str, to: &str, class: Option<&str>) -> String {
    let to = to.trim();
    if to.is_empty() {
        return text.to_owned();
    }
    let generic = || match (smtp_code(text), class) {
        (Some(code), Some(class)) => format!(
            "the mail server's answer is left out, as it may name the recipient ({class} \
             error, SMTP {code})"
        ),
        (Some(code), None) => format!(
            "the mail server's answer is left out, as it may name the recipient (SMTP {code})"
        ),
        (None, Some(class)) => format!(
            "the send failed ({class} error); what was said is left out, as it may name the \
             recipient"
        ),
        (None, None) => {
            "the send failed; what was said is left out, as it may name the recipient".to_owned()
        }
    };
    if !to.is_ascii() {
        return generic();
    }
    // (ASCII lower case keeps every byte where it was, so the places found are places in `text`.)
    let (lower, needle) = (text.to_ascii_lowercase(), to.to_ascii_lowercase());
    let (mut out, mut left) = (String::with_capacity(text.len()), String::new());
    let mut from = 0;
    while let Some(at) = lower[from..].find(&needle) {
        out.push_str(&text[from..from + at]);
        out.push_str("<the recipient>");
        left.push_str(&text[from..from + at]);
        left.push('\n');
        from += at + needle.len();
    }
    out.push_str(&text[from..]);
    left.push_str(&text[from..]);
    // What is left of the text, as a reader would compare it: every letter in lower case (not
    // only ASCII's).
    let left = left.to_lowercase();
    let (local, domain) = needle.rsplit_once('@').unwrap_or((needle.as_str(), ""));
    let unicode_domain = url::quirks::domain_to_unicode(domain).to_lowercase();
    let traces = [local, domain, unicode_domain.as_str()];
    if traces.iter().any(|t| !t.is_empty() && left.contains(t)) {
        return generic();
    }
    out
}

/// The kind of failure `err` is, in a word, when the mail client says (never anything of the
/// address): a refusal for good (`permanent`, 5xx) or for now (`transient`, 4xx), no answer in
/// time (`timeout`), the encryption failed (`TLS`), the mail server's answer could not be read
/// (`response`), the message could not be made (`client`), or the mail server could not be reached
/// or the connection broke (`connection`).
fn failure_class(err: &anyhow::Error) -> Option<&'static str> {
    let smtp = err
        .chain()
        .find_map(|e| e.downcast_ref::<lettre::transport::smtp::Error>())?;
    Some(if smtp.is_permanent() {
        "permanent"
    } else if smtp.is_transient() {
        "transient"
    } else if smtp.is_timeout() {
        "timeout"
    } else if smtp.is_tls() {
        "TLS"
    } else if smtp.is_response() {
        "response"
    } else if smtp.is_client() {
        "client"
    } else {
        "connection"
    })
}

/// The SMTP reply code a failure's text gives (`... (550): ...`, as the mail client words a
/// refusal), if it gives one.
fn smtp_code(text: &str) -> Option<&str> {
    text.match_indices('(').find_map(|(at, _)| {
        let code = text.get(at + 1..at + 4)?;
        let closed = text.get(at + 4..at + 5) == Some(")");
        (closed
            && code.bytes().all(|b| b.is_ascii_digit())
            && (b'2'..=b'5').contains(&code.as_bytes()[0]))
        .then_some(code)
    })
}

/// How long a request waits for the mail it sends before it answers without knowing.
pub const MAIL_WAIT: Duration = Duration::from_secs(10);

/// What became of a message a request sent ([`send_after_commit`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mailed {
    /// The mail server took it.
    Sent,
    /// The mail server refused it, could not be reached, or the send failed (logged, counted).
    Failed,
    /// Still being sent when the request stopped waiting (it goes on by itself).
    Pending,
    /// Not sent: as many messages were on their way as the server sends at once
    /// (`OSWX_COLLAB_MAX_MAIL_SENDS`; counted).
    Refused,
    /// No mail server is configured.
    NotConfigured,
}

/// The messages on their way to the mail server ([`AppState::mail`]): only so many at once, and
/// counted apart from the steps after commits, so that a stop does not hold the journal for a
/// slow mail server.
#[derive(Debug)]
pub struct MailSends {
    slots: Arc<Semaphore>,
    sending: Arc<InFlight>,
    /// Set when the server is stopping: requests stop waiting for their mail.
    stopping: watch::Sender<bool>,
}

impl MailSends {
    /// At most `at_once` messages on their way at a time (at least one).
    pub fn new(at_once: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(at_once.max(1))),
            sending: Arc::default(),
            stopping: watch::Sender::new(false),
        }
    }

    /// How many messages are on their way.
    pub fn sending(&self) -> usize {
        self.sending.running()
    }

    /// The server is stopping: a request waiting for its mail stops waiting (and answers
    /// [`Mailed::Pending`]).
    pub fn stop_waiting(&self) {
        self.stopping.send_replace(true);
    }

    /// Waits until no message is on its way.
    pub async fn idle(&self) {
        self.sending.idle().await;
    }
}

/// Sends a message for a request that has already committed, without letting the mail server
/// hold the request up: the send runs in a task of its own ([`send_logged`]: a failure is logged
/// and counted), and the request waits for it for [`MAIL_WAIT`] at most, or until the server
/// begins to stop. When as many messages are on their way as the server sends at once, this one
/// is not sent ([`Mailed::Refused`]).
///
/// A mail server can take a minute to refuse or accept (20 s a step). Awaited in the handler,
/// that passed the request limit and answered 503 "try again" for something already stored.
pub async fn send_after_commit(
    state: &AppState,
    to: String,
    subject: String,
    text: String,
) -> Mailed {
    if !state.mailer.configured() {
        return Mailed::NotConfigured;
    }
    let Ok(slot) = state.mail.slots.clone().try_acquire_owned() else {
        crate::metrics::mail_refused();
        tracing::warn!(
            "an email was not sent: as many are on their way to the mail server as are sent at \
             once (OSWX_COLLAB_MAX_MAIL_SENDS)"
        );
        return Mailed::Refused;
    };
    let mut stopping = state.mail.stopping.subscribe();
    let running = state.mail.sending.start();
    let (mailer, settings) = (state.mailer.clone(), state.settings.clone());
    let sending = tokio::spawn(async move {
        let (_slot, _running) = (slot, running);
        send_logged(mailer.as_ref(), &settings, &to, &subject, &text).await
    });
    tokio::select! {
        sent = tokio::time::timeout(state.room_hooks.mail_wait, sending) => match sent {
            Ok(Ok(true)) => Mailed::Sent,
            // (A panic was caught and counted in the task; a join error is only a cancel.)
            Ok(Ok(false) | Err(_)) => Mailed::Failed,
            Err(_) => Mailed::Pending,
        },
        _ = stopping.wait_for(|stopping| *stopping) => Mailed::Pending,
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

    /// [`without_address`] of a failure the mail client did not class.
    fn scrubbed(text: &str, to: &str) -> String {
        without_address(text, to, None)
    }

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

    #[test]
    fn a_refusal_is_logged_without_the_address_it_quotes() {
        let said = "permanent error (550): 5.1.1 <Bob@Example.com>: user unknown; bob@example.COM";
        assert_eq!(
            scrubbed(said, "bob@example.com"),
            "permanent error (550): 5.1.1 <<the recipient>>: user unknown; <the recipient>"
        );
        assert_eq!(scrubbed("timed out", "bob@example.com"), "timed out");
        assert_eq!(scrubbed("x é y", ""), "x é y");

        // Whatever cannot be scrubbed for sure is left out, but for the SMTP code.
        let generic = |code: &str| {
            format!(
                "the mail server's answer is left out, as it may name the recipient (SMTP {code})"
            )
        };
        let quoted = |address: &str| format!("permanent error (550): 5.1.1 <{address}>: unknown");
        // An address that is not ASCII: in its own spelling, in upper case, or as punycode.
        for said in [
            quoted("zoë@bücher.example"),
            quoted("ZOË@BÜCHER.EXAMPLE"),
            quoted("zoë@xn--bcher-kva.example"),
        ] {
            let logged = scrubbed(&said, "zoë@bücher.example");
            assert_eq!(logged, generic("550"), "{said}");
        }
        // An ASCII address with an internationalised domain, quoted in its Unicode form.
        let said = quoted("bob@BÜCHER.example");
        assert_eq!(scrubbed(&said, "bob@xn--bcher-kva.example"), generic("550"));
        // A domain quoted on its own: in ASCII, or in its Unicode form.
        let said = "transient error (451): no route to xn--bcher-kva.example";
        assert_eq!(scrubbed(said, "ann@xn--bcher-kva.example"), generic("451"));
        let said = "transient error (421): bücher.example is not answering";
        assert_eq!(scrubbed(said, "ann@xn--bcher-kva.example"), generic("421"));
        // The local part quoted on its own, or a letter only Unicode lower-cases (the Kelvin
        // sign) hiding the whole address from an ASCII comparison.
        let said = "permanent error (550): mailbox Zed.Private unknown";
        assert_eq!(scrubbed(said, "zed.private@example.com"), generic("550"));
        let said = "permanent error (553): <\u{212A}im@example.com> refused";
        assert_eq!(scrubbed(said, "kim@example.com"), generic("553"));
        // No code to keep.
        assert_eq!(
            scrubbed(
                "connection to zoë@bücher.example failed",
                "zoë@bücher.example"
            ),
            "the send failed; what was said is left out, as it may name the recipient"
        );
        // Nothing of the address left: the answer is kept as it is.
        assert_eq!(
            scrubbed(
                &quoted("ann@xn--bcher-kva.example"),
                "ann@xn--bcher-kva.example"
            ),
            "permanent error (550): 5.1.1 <<the recipient>>: unknown"
        );
    }

    /// A mail server on this machine (plain SMTP) that refuses every recipient with `refusal`.
    async fn refusing_mail_server(refusal: &'static str) -> u16 {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut stream = BufReader::new(socket);
                    let mut answer = "220 localhost ESMTP".to_owned();
                    loop {
                        answer.push_str("\r\n");
                        if stream.write_all(answer.as_bytes()).await.is_err() {
                            return;
                        }
                        let mut line = String::new();
                        if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                            return;
                        }
                        let command = line.trim_end().to_ascii_uppercase();
                        answer = if command.starts_with("RCPT TO:") {
                            refusal.to_owned()
                        } else if command == "QUIT" {
                            "221 bye".to_owned()
                        } else {
                            "250 ok".to_owned()
                        };
                    }
                });
            }
        });
        port
    }

    fn mailer_on(port: u16) -> SmtpMailer {
        SmtpMailer::new(&Settings {
            smtp_host: "127.0.0.1".into(),
            smtp_port: port,
            smtp_security: SmtpSecurity::Plain,
            smtp_from: "collab@example.com".into(),
            ..Settings::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn a_failure_that_cannot_be_scrubbed_is_logged_with_its_kind_and_code() {
        let to = "zed.private@example.com";
        // A refusal that names the mailbox on its own: for good, then for now.
        for (refusal, kind, code) in [
            ("550 5.1.1 mailbox Zed.Private unknown", "permanent", "550"),
            ("451 4.3.0 Zed.Private is over quota", "transient", "451"),
        ] {
            let port = refusing_mail_server(refusal).await;
            let err = mailer_on(port).send(to, "s", "t", None).await.unwrap_err();
            assert_eq!(failure_class(&err), Some(kind), "{err:#}");
            let logged = without_address(&format!("{err:#}"), to, failure_class(&err));
            assert_eq!(
                logged,
                format!(
                    "the mail server's answer is left out, as it may name the recipient ({kind} \
                     error, SMTP {code})"
                )
            );
        }
        // A mail server that is not there.
        let closed = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let err = mailer_on(closed)
            .send(to, "s", "t", None)
            .await
            .unwrap_err();
        assert_eq!(failure_class(&err), Some("connection"), "{err:#}");
        // Without a code, the kind alone; an error that is not the mail client's has none.
        assert_eq!(
            without_address("it failed for zed.private", to, Some("timeout")),
            "the send failed (timeout error); what was said is left out, as it may name the \
             recipient"
        );
        assert_eq!(
            failure_class(&anyhow::anyhow!("not an email address")),
            None
        );
    }
}
