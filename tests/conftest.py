import copy
import os
import uuid

import pytest
from fastapi.testclient import TestClient

from openswx_collab_server.app import create_app
from openswx_collab_server.config import Settings
from openswx_collab_server.hooks import Hooks

PART = {
    "format": "openswx-part", "version": 3, "name": "Bracket", "units": "mm",
    "material": {"name": None, "density": 1000.0}, "rollback": None, "active_config": "Default",
    "next_id": 3, "counters": {"Sketch": 1, "Boss-Extrude": 1}, "variables": {},
    "features": [
        {"id": "f1", "name": "Sketch1", "type": "sketch", "suppressed": False,
         "params": {"plane": "Top", "sketch": {"points": {"P1": [0, 0]}, "entities": {}, "counter": 1}}},
        {"id": "f2", "name": "Boss-Extrude1", "type": "extrude", "suppressed": False,
         "params": {"sketch": "Sketch1", "depth": 10}},
    ],
}


def part() -> dict:
    return copy.deepcopy(PART)


def _empty_postgres(url: str) -> None:
    """Each test starts with an empty database (tools/test_postgres.py runs the suite this way)."""
    import asyncio

    import asyncpg

    async def reset():
        conn = await asyncpg.connect(url.replace("+asyncpg", ""))
        try:
            await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        finally:
            await conn.close()
    asyncio.run(reset())


@pytest.fixture
def settings(tmp_path):
    url = os.environ.get("OSWX_TEST_DATABASE_URL")  # e.g. postgresql+asyncpg://postgres:@127.0.0.1:5432/postgres
    if url:
        _empty_postgres(url)
    return Settings(database_url=url or f"sqlite+aiosqlite:///{(tmp_path / 'collab.db').as_posix()}",
                    storage_url=str(tmp_path / "storage"), public_url="http://testserver")


@pytest.fixture
def make_client(settings):
    """A running server; call again (after closing) for a 'restart' on the same database."""
    open_clients = []

    def make(http_client=None, hooks=None, **over):
        for k, v in over.items():
            setattr(settings, k, v)
        c = TestClient(create_app(settings, hooks=hooks or Hooks(), http_client=http_client), follow_redirects=False)
        c.__enter__()
        open_clients.append(c)
        return c

    yield make
    for live in Live.open:
        live.close()
    Live.open.clear()
    for c in open_clients:
        try:
            c.__exit__(None, None, None)
        except Exception:  # noqa: BLE001
            pass


@pytest.fixture
def client(make_client):
    return make_client()


def H(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def register(client, email, name=None, invite=None, password="correct horse"):
    r = client.post("/api/auth/register", json={"email": email, "password": password, "name": name or email.split("@")[0],
                                                "invite": invite})
    assert r.status_code == 201, r.text
    return r.json()["token"], r.json()["user"]["id"]


class World:
    """An admin (org owner) with one organisation, one project and helpers to add people and documents."""

    def __init__(self, client):
        self.c = client
        self.admin, self.admin_id = register(client, "ada@example.com", "Ada")
        self.org = client.post("/api/orgs", json={"name": "Robotics"}, headers=H(self.admin)).json()["id"]
        self.project = client.post(f"/api/orgs/{self.org}/projects", json={"name": "Rover"},
                                   headers=H(self.admin)).json()["id"]

    def invite(self, email, name=None, role="editor", org_role="member"):
        grants = [{"project_id": self.project, "role": role}] if role else []
        r = self.c.post(f"/api/orgs/{self.org}/invites", json={"email": email, "role": org_role, "projects": grants},
                        headers=H(self.admin))
        assert r.status_code == 201, r.text
        return register(self.c, email, name, invite=r.json()["token"])

    def document(self, content=None, name="Bracket", token=None):
        r = self.c.post(f"/api/projects/{self.project}/documents", json={"name": name, "content": content or part()},
                        headers=H(token or self.admin))
        assert r.status_code == 201, r.text
        return r.json()["id"]


@pytest.fixture
def world(client):
    return World(client)


class Live:
    """One client on a document's WebSocket."""
    open: list = []  # closed by the make_client fixture: an open socket would keep the test server alive

    def __init__(self, client, doc_id, token, client_id=None, format_version=3, last_seq=None):
        self.client_id = client_id or uuid.uuid4().hex
        self._cm = client.websocket_connect(f"/api/documents/{doc_id}/ws")
        self.ws = self._cm.__enter__()
        self.seen: list[dict] = []
        self.n = 0
        Live.open.append(self)
        self.ws.send_json({"type": "hello", "token": token, "client_id": self.client_id,
                           "format_version": format_version, "last_seq": last_seq})
        first = self.ws.receive_json()
        self.error = first if first["type"] == "error" else None
        self.welcome = first if first["type"] == "welcome" else None
        if self.welcome:  # the id the server knows this client by: "<user id>.<what the app sent>"
            self.client_id = self.welcome["you"]["client_id"]

    def send(self, type, **kw):
        self.ws.send_json({"type": type, **kw})

    def recv(self, type):
        """The next message of this type; everything before it is kept in .seen."""
        while True:
            m = self.ws.receive_json()
            if m["type"] == type:
                return m
            self.seen.append(m)

    def drain(self):
        """Everything the server has sent so far (a ping marks the end)."""
        self.send("ping")
        out = []
        while True:
            m = self.ws.receive_json()
            if m["type"] == "pong":
                self.seen.extend(out)
                return out
            out.append(m)

    def change(self, ops, base_seq, command="edit", change_id=None):
        self.n += 1
        cid = change_id or f"c{self.n}"
        self.send("change", change_id=cid, base_seq=base_seq, ops=ops, command=command)
        while True:
            m = self.ws.receive_json()
            if m["type"] in ("change.accepted", "change.rejected") and m["change_id"] == cid:
                return m
            self.seen.append(m)

    def lock(self, *targets):
        self.send("lock.acquire", request_id="r", targets=list(targets))
        return self.recv("lock.result")

    def close(self):
        try:
            self._cm.__exit__(None, None, None)
        except Exception:  # noqa: BLE001
            pass


def set_depth(depth, fid="f2"):
    return [{"op": "feature.update", "id": fid, "set": [[["params", "depth"], depth]], "unset": []}]


def add_feature(fid, name, after="f2", ftype="fillet"):
    return [{"op": "feature.add", "after": after,
             "feature": {"id": fid, "name": name, "type": ftype, "params": {"radius": 2}, "suppressed": False}}]
