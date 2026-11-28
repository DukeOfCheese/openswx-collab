"""Make a real Python-server data set for the importer's CI test.

    python tools/make_import_fixture.py <out dir>

Drives the released Python server in-process (the way tests/conftest.py does) and leaves its database
and storage folder behind: `<out>/collab.db`, `<out>/storage/` and `<out>/summary.json`
(`{"users": n, "documents": n, "head": {document id: head seq}}`).

What it makes: two users (one later deleted), an organisation with a pending invite, two projects, a part
with 450 changes (including an undo, a revert and a restore), a named version, a part that references a
blob, a soft-deleted document, and an assembly that pins the first part.

Needs the Python server installed (`pip install server/ protocol/`), nothing else.
"""
from __future__ import annotations

import json
import shutil
import sys
import uuid
from pathlib import Path

from fastapi.testclient import TestClient

from openswx_collab_protocol.ops import externalize
from openswx_collab_server.app import create_app
from openswx_collab_server.config import Settings
from openswx_collab_server.hooks import Hooks

PASSWORD = "correct horse"
CHANGES = 450  # the first part's head after everything below

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


def bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def check(r, status: int):
    assert r.status_code == status, f"{r.request.method} {r.request.url}: {r.status_code} {r.text}"
    return r


def register(client, email: str, name: str, invite: str | None = None) -> tuple[str, str]:
    r = check(client.post("/api/auth/register", json={"email": email, "password": PASSWORD, "name": name,
                                                      "invite": invite}), 201)
    return r.json()["token"], r.json()["user"]["id"]


class Live:
    """One client on a document's WebSocket (as in tests/conftest.py)."""

    def __init__(self, client, doc_id: str, token: str):
        self.client_id = uuid.uuid4().hex
        self._cm = client.websocket_connect(f"/api/documents/{doc_id}/ws")
        self.ws = self._cm.__enter__()
        self.n = 0
        self.ws.send_json({"type": "hello", "token": token, "client_id": self.client_id, "format_version": 3,
                           "last_seq": None})
        first = self.ws.receive_json()
        assert first["type"] == "welcome", first

    def _until(self, types: tuple[str, ...], key: str, value: str) -> dict:
        while True:
            m = self.ws.receive_json()
            if m["type"] in types and m.get(key) == value:
                return m

    def change(self, ops: list, base_seq: int, command: str = "edit") -> int:
        self.n += 1
        cid = f"c{self.n}"
        self.ws.send_json({"type": "change", "change_id": cid, "base_seq": base_seq, "ops": ops,
                           "command": command})
        m = self._until(("change.accepted", "change.rejected"), "change_id", cid)
        assert m["type"] == "change.accepted", m
        return m["seq"]

    def revert(self, seq: int, mode: str) -> int:
        self.ws.send_json({"type": "change.revert", "request_id": "r", "seq": seq, "mode": mode})
        m = self._until(("change.revert.result",), "request_id", "r")
        assert m["ok"], m
        return m["seq"]

    def close(self) -> None:
        self._cm.__exit__(None, None, None)


def set_depth(depth: int) -> list:
    return [{"op": "feature.update", "id": "f2", "set": [[["params", "depth"], depth]], "unset": []}]


def add_feature(fid: str, name: str) -> list:
    return [{"op": "feature.add", "after": "f2",
             "feature": {"id": fid, "name": name, "type": "fillet", "params": {"radius": 2}, "suppressed": False}}]


def make(out: Path) -> dict:
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    settings = Settings(database_url=f"sqlite+aiosqlite:///{(out / 'collab.db').as_posix()}",
                        storage_url=str(out / "storage"), public_url="http://testserver")
    with TestClient(create_app(settings, hooks=Hooks()), follow_redirects=False) as client:
        ada, ada_id = register(client, "ada@example.com", "Ada")
        org = check(client.post("/api/orgs", json={"name": "Robotics"}, headers=bearer(ada)), 201).json()["id"]
        rover = check(client.post(f"/api/orgs/{org}/projects", json={"name": "Rover"}, headers=bearer(ada)),
                      201).json()["id"]
        arm = check(client.post(f"/api/orgs/{org}/projects", json={"name": "Arm"}, headers=bearer(ada)),
                    201).json()["id"]
        # Bob joins through an invite (with a grant on Rover) and is deleted at the end; a second invite
        # stays pending.
        grant = [{"project_id": rover, "role": "editor"}]
        invite = check(client.post(f"/api/orgs/{org}/invites", json={"email": "bob@example.com", "projects": grant},
                                   headers=bearer(ada)), 201).json()
        bob, bob_id = register(client, "bob@example.com", "Bob", invite=invite["token"])
        check(client.post(f"/api/orgs/{org}/invites", json={"email": "cy@example.com", "send_email": False},
                          headers=bearer(ada)), 201)

        def create(project: str, name: str, kind: str, content: dict) -> str:
            r = check(client.post(f"/api/projects/{project}/documents",
                                  json={"name": name, "kind": kind, "content": content}, headers=bearer(ada)), 201)
            return r.json()["id"]

        part = create(rover, "Bracket", "part", json.loads(json.dumps(PART)))

        # 450 changes: Bob adds a fillet (1), Ada adds another (2) and changes the depth 445 times, then
        # undoes her fillet (448), reverts Bob's (449) and restores the state at change 100 (450).
        live_bob = Live(client, part, bob)
        bobs = live_bob.change(add_feature("f3", "Fillet1"), 0, "add fillet")
        live_bob.close()
        live = Live(client, part, ada)
        head = bobs
        head = live.change(add_feature("f4", "Fillet2"), head, "add fillet")
        for n in range(3, CHANGES - 2):
            head = live.change(set_depth(10 + n), head, "set depth")
        assert head == CHANGES - 3, head
        head = live.revert(2, "undo")
        head = live.revert(1, "revert")
        live.close()
        assert head == CHANGES - 1, head
        check(client.post(f"/api/documents/{part}/versions", json={"name": "v1", "note": "first release",
                                                                    "seq": 100}, headers=bearer(ada)), 201)
        check(client.post(f"/api/documents/{part}/restore", json={"version": "v1"}, headers=bearer(ada)), 200)
        assert check(client.get(f"/api/documents/{part}", headers=bearer(ada)), 200).json()["head_seq"] == CHANGES

        # a part with a blob reference (a long string travels as a separately uploaded blob)
        blobs: dict[str, bytes] = {}
        body = json.loads(json.dumps(PART))
        body["name"] = "Plate"
        body["features"][0]["params"]["note"] = "x" * 20000
        body = externalize(body, blobs)
        for sha, data in blobs.items():
            check(client.put(f"/api/projects/{rover}/blobs/{sha}", content=data, headers=bearer(ada)), 204)
        plate = create(rover, "Plate", "part", body)

        # a document in the other project, deleted (the row stays, soft-deleted)
        gone = create(arm, "Old arm", "part", json.loads(json.dumps(PART)))
        check(client.delete(f"/api/documents/{gone}", headers=bearer(ada)), 204)

        # an assembly that pins the first part at its head
        component = {"id": "c1", "name": "Bracket-1", "x": [0, 0, 0, 0, 0, 0], "fixed": False,
                     "ref": {"document": part, "seq": CHANGES, "version": None}}
        assembly = create(rover, "Rover", "assembly", {
            "format": "openswx-assembly", "version": 3, "components": [component], "mates": [],
            "features": [], "display_states": {}})

        # Bob deletes his account (his work stays, credited to "Deleted user")
        check(client.request("DELETE", "/api/me", json={"current_password": PASSWORD}, headers=bearer(bob)), 204)

        heads = {}
        for doc in (part, plate, gone, assembly):
            heads[doc] = check(client.get(f"/api/documents/{doc}", headers=bearer(ada)), 200).json()["head_seq"] \
                if doc != gone else 0
    summary = {"users": 2, "documents": 4, "head": heads}
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: python tools/make_import_fixture.py <out dir>")
    print(json.dumps(make(Path(sys.argv[1]))))


if __name__ == "__main__":
    main()
