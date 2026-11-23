"""Email, when the operator has set up SMTP (OSWX_COLLAB_SMTP_*). Without it the server sends nothing and
works as before: invites are links people pass on, forgotten passwords are reset by the operator.

Sending happens on a worker thread after the request has been answered: a slow or broken mail server
never holds up the person, and failures are logged rather than shown (a person asking for a reset link
is told the same thing whether or not one was sent).
"""
from __future__ import annotations

import asyncio
import logging
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr, make_msgid

log = logging.getLogger("openswx_collab")


class Mailer:
    def __init__(self, settings):
        self.s = settings

    @property
    def enabled(self) -> bool:
        return bool(self.s.smtp_host and self.s.smtp_from)

    def _send(self, to: str, subject: str, text: str) -> None:
        msg = EmailMessage()
        msg["From"] = formataddr((self.s.smtp_from_name, self.s.smtp_from)) if self.s.smtp_from_name else self.s.smtp_from
        msg["To"] = to
        msg["Subject"] = subject
        msg["Message-ID"] = make_msgid(domain=self.s.smtp_from.rsplit("@", 1)[-1])
        msg.set_content(text)
        security = self.s.smtp_security
        context = ssl.create_default_context()
        if security == "ssl":
            server = smtplib.SMTP_SSL(self.s.smtp_host, self.s.smtp_port, timeout=20, context=context)
        else:
            server = smtplib.SMTP(self.s.smtp_host, self.s.smtp_port, timeout=20)
        with server:
            if security == "starttls":
                server.starttls(context=context)
            if self.s.smtp_user:
                server.login(self.s.smtp_user, self.s.smtp_password)
            server.send_message(msg)

    async def send(self, to: str, subject: str, text: str) -> bool:
        """Send one plain-text message. Returns whether it was handed to the mail server."""
        if not self.enabled:
            return False
        try:
            await asyncio.to_thread(self._send, to, subject, text)
            return True
        except Exception:  # noqa: BLE001 - logged for the operator; the person is not told about our mail server
            log.exception("could not send email to the mail server %s", self.s.smtp_host)
            return False


def invite_text(org: str, inviter: str, url: str, days: int) -> tuple[str, str]:
    return (f"{inviter} invited you to {org} on openswx",
            f"{inviter} has invited you to collaborate in {org} on openswx.\n\n"
            f"To accept, open openswx, choose Collaborate > Join with invite, and paste this link:\n\n{url}\n\n"
            f"You can create your account there. The invite works once and expires in {days} days.\n\n"
            "If you were not expecting this, you can ignore this email.\n")


def reset_text(url: str, minutes: int) -> tuple[str, str]:
    return ("Reset your openswx collaboration password",
            "Someone (hopefully you) asked to reset the password of the openswx collaboration account for this "
            f"address.\n\nTo choose a new password, open this link within {minutes} minutes:\n\n{url}\n\n"
            "Setting a new password signs you out everywhere. If you did not ask for this, ignore this email: "
            "nothing changes.\n")
