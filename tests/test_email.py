"""Email: invites and password resets, sent through a small real SMTP server so the actual sending code runs."""
import re
import socket
import socketserver
import threading
from email import message_from_bytes
from email.policy import default

import pytest

from conftest import H, Live, World, register


class _SMTPHandler(socketserver.StreamRequestHandler):
    """Just enough SMTP for smtplib: EHLO, AUTH PLAIN/LOGIN, MAIL, RCPT, DATA, QUIT (no TLS)."""

    def reply(self, line: str) -> None:
        self.wfile.write((line + "\r\n").encode())

    def handle(self):
        box = self.server.box
        self.reply("220 fake ESMTP")
        mail_from, rcpts = None, []
        while True:
            line = self.rfile.readline()
            if not line:
                return
            cmd = line.decode().strip()
            verb = cmd.split(" ", 1)[0].upper()
            if verb in ("EHLO", "HELO"):
                self.wfile.write(b"250-fake\r\n250-AUTH PLAIN LOGIN\r\n250 OK\r\n")
            elif verb == "AUTH":
                box["auth"] = cmd
                self.reply("235 ok")
            elif verb == "MAIL":
                mail_from, rcpts = cmd, []
                self.reply("250 ok")
            elif verb == "RCPT":
                rcpts.append(re.search(r"<(.*)>", cmd).group(1))
                self.reply("250 ok")
            elif verb == "DATA":
                self.reply("354 go on")
                data = b""
                while True:
                    chunk = self.rfile.readline()
                    if chunk in (b".\r\n", b".\n"):
                        break
                    data += chunk[1:] if chunk.startswith(b"..") else chunk
                box["messages"].append({"from": mail_from, "to": rcpts,
                                        "message": message_from_bytes(data, policy=default)})
                self.reply("250 queued")
            elif verb == "QUIT":
                self.reply("221 bye")
                return
            else:
                self.reply("250 ok")


@pytest.fixture
def smtp():
    box = {"messages": []}
    server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), _SMTPHandler)
    server.daemon_threads = True
    server.box = box
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server.server_address[1], box
    server.shutdown()
    server.server_close()


@pytest.fixture
def mailing(make_client, smtp):
    port, box = smtp
    client = make_client(smtp_host="127.0.0.1", smtp_port=port, smtp_security="none", smtp_user="robot",
                         smtp_password="pw", smtp_from="collab@example.org")
    return client, box


def link_in(msg) -> str:
    return re.search(r"(http://testserver/\S+)", msg.get_content()).group(1)


def test_without_smtp_nothing_is_sent_and_resets_say_so(world, client):
    assert client.get("/api/server").json()["email"] is False
    r = client.post(f"/api/orgs/{world.org}/invites", json={"email": "bob@example.com"}, headers=H(world.admin))
    assert r.json()["emailed"] is False and r.json()["url"].startswith("http://testserver/invite/")
    assert client.post("/api/auth/password-reset", json={"email": "ada@example.com"}).status_code == 501


def test_an_invite_is_emailed(mailing):
    client, box = mailing
    assert client.get("/api/server").json()["email"] is True
    w = World(client)
    r = client.post(f"/api/orgs/{w.org}/invites", json={"email": "bob@example.com"}, headers=H(w.admin)).json()
    assert r["emailed"] is True
    (sent,) = box["messages"]
    msg = sent["message"]
    assert sent["to"] == ["bob@example.com"] and msg["To"] == "bob@example.com"
    assert msg["Subject"] == "Ada invited you to Robotics on openswx"
    assert "collab@example.org" in msg["From"] and box["auth"].startswith("AUTH")
    assert link_in(msg) == r["url"]
    # the link in the email works
    register(client, "bob@example.com", invite=link_in(msg).rsplit("/", 1)[1])
    # link invites (no address) and send_email=false send nothing
    client.post(f"/api/orgs/{w.org}/invites", json={}, headers=H(w.admin))
    client.post(f"/api/orgs/{w.org}/invites", json={"email": "cy@example.com", "send_email": False}, headers=H(w.admin))
    assert len(box["messages"]) == 1


def test_a_forgotten_password(mailing):
    client, box = mailing
    w = World(client)
    other = client.post("/api/auth/login", json={"email": "ada@example.com", "password": "correct horse"}).json()["token"]
    doc = w.document()
    live = Live(client, doc, w.admin)
    # the same answer for an address with no account, and nothing sent
    for email in ("nobody@example.com", "ada@example.com"):
        r = client.post("/api/auth/password-reset", json={"email": email})
        assert r.status_code == 202 and "if there is an account" in r.json()["message"]
    (sent,) = box["messages"]
    assert sent["to"] == ["ada@example.com"]
    url = link_in(sent["message"])
    token = url.rsplit("/", 1)[1]
    page = client.get(f"/reset-password/{token}")
    assert page.status_code == 200 and "Choose a new password" in page.text and token in page.text
    assert client.post("/api/auth/password-reset/confirm", json={"token": token, "password": "short"}).status_code == 422
    r = client.post("/api/auth/password-reset/confirm", json={"token": token, "password": "a brand new one"})
    assert r.status_code == 200
    # signed out everywhere, open documents included; the old password is gone; the link works once
    assert client.get("/api/me", headers=H(other)).status_code == 401
    assert client.get("/api/me", headers=H(w.admin)).status_code == 401
    try:
        while True:
            live.ws.receive_json()
    except Exception as exc:  # noqa: BLE001
        assert getattr(exc, "code", None) == 4401
    assert client.post("/api/auth/login", json={"email": "ada@example.com", "password": "correct horse"}).status_code == 401
    assert client.post("/api/auth/login", json={"email": "ada@example.com", "password": "a brand new one"}).status_code == 200
    assert client.post("/api/auth/password-reset/confirm", json={"token": token, "password": "again again"}).status_code == 400


def test_only_the_newest_link_works_and_requests_are_limited(mailing):
    client, box = mailing
    World(client)
    for _ in range(7):
        assert client.post("/api/auth/password-reset", json={"email": "ada@example.com"}).status_code == 202
    assert len(box["messages"]) == 5  # (then quietly limited)
    tokens = [link_in(m["message"]).rsplit("/", 1)[1] for m in box["messages"]]
    for old in tokens[:-1]:
        assert client.post("/api/auth/password-reset/confirm", json={"token": old, "password": "whatever1"}).status_code == 400
    assert client.post("/api/auth/password-reset/confirm", json={"token": tokens[-1], "password": "whatever1"}).status_code == 200


def test_a_reset_makes_the_address_count_as_confirmed(mailing):
    client, box = mailing
    w = World(client)
    bob, bob_id = w.invite("bob@example.com")

    async def verified():
        from openswx_collab_server.db import User
        async with client.app.state.db() as s:
            return (await s.get(User, bob_id)).email_verified
    assert client.portal.call(verified) is False
    client.post("/api/auth/password-reset", json={"email": "bob@example.com"})
    token = link_in(box["messages"][-1]["message"]).rsplit("/", 1)[1]
    client.post("/api/auth/password-reset/confirm", json={"token": token, "password": "bobs new one"})
    assert client.portal.call(verified) is True


def test_a_broken_mail_server_does_not_break_the_request(make_client):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        dead_port = s.getsockname()[1]  # (nothing listens there once this closes)
    client = make_client(smtp_host="127.0.0.1", smtp_port=dead_port, smtp_security="none", smtp_from="c@example.org")
    w = World(client)
    r = client.post(f"/api/orgs/{w.org}/invites", json={"email": "bob@example.com"}, headers=H(w.admin))
    assert r.status_code == 201 and r.json()["emailed"] is False
    assert client.post("/api/auth/password-reset", json={"email": "ada@example.com"}).status_code == 202
