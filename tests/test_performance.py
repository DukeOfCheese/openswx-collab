"""Work the server avoids (encoding once, no copies, fewer queries) without changing what anyone receives."""
import asyncio
import hashlib
import threading

from sqlalchemy import event

import openswx_collab_server.hub as hub_module
from openswx_collab_server.hub import Conn, Room

from conftest import H, Live, World, add_feature, set_depth


# ------------------------------------------------------------------ 1. broadcasts
def test_a_broadcast_is_encoded_once_for_everyone(world, client, monkeypatch):
    doc = world.document()
    a, b, c = (Live(client, doc, world.admin) for _ in range(3))
    sent = []
    real = Conn.send_text

    def record(self, text):
        sent.append((self.client_id, text))
        return real(self, text)
    monkeypatch.setattr(Conn, "send_text", record)
    assert a.change(set_depth(4), 0)["seq"] == 1
    a.send("presence", data={"pointer": [1, 2, 3]})
    assert b.recv("presence")["data"] == {"pointer": [1, 2, 3]} and c.recv("presence")
    changes = [(cid, t) for cid, t in sent if '"type":"change"' in t]
    presences = [(cid, t) for cid, t in sent if '"type":"presence"' in t]
    for frames in (changes, presences):
        assert {cid for cid, _ in frames} == {b.client_id, c.client_id}  # (not the sender)
        assert frames[0][1] is frames[1][1]  # one string, the same object, for every peer
    for peer in (b, c):  # (what they received is unchanged)
        got = [m for m in peer.seen if m["type"] == "change"]
        assert [(m["seq"], m["ops"]) for m in got] == [(1, set_depth(4))]


def test_a_draft_that_is_not_json_is_neither_relayed_nor_kept(world, client):
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, world.admin)
    a.ws.send_text('{"type":"draft","data":{"x":NaN}}')
    a.send("draft", data={"x": 1})
    assert b.recv("draft")["data"] == {"x": 1}  # (the NaN one never arrived)
    late = Live(client, doc, world.admin).welcome
    assert next(p for p in late["peers"] if p["client_id"] == a.client_id)["draft"] == {"x": 1}


def test_one_big_frame_is_not_too_slow_but_a_backlog_behind_it_is(world, client):
    doc = world.document()
    a = Live(client, doc, world.admin)
    stuck = Live(client, doc, world.admin, client_id="slow-reader")
    conn = client.app.state.hub.rooms[doc].conns[stuck.client_id]
    seen = {}

    async def run():
        conn.task.cancel()  # (nothing is written from here on: everything stays in the buffer)
        await asyncio.sleep(0)
        conn.max_buffer = 1000
        conn.send_text("x" * 5000)  # nothing waits: a frame bigger than the limit goes (a big welcome)
        seen["big"] = not conn.closed
        for _ in range(9):
            conn.send_text("y" * 100)  # 900 behind it: within the limit
        seen["behind"] = not conn.closed
        conn.send_text("z" * 101)  # 1001 behind it: too slow
        seen["over"] = conn.closed
    client.portal.call(run)
    assert seen == {"big": True, "behind": True, "over": True}
    assert a.recv("peer.left")["client_id"] == stuck.client_id


def test_a_big_frame_behind_others_is_still_too_slow(world, client):
    doc = world.document()
    stuck = Live(client, doc, world.admin, client_id="slow-reader")
    conn = client.app.state.hub.rooms[doc].conns[stuck.client_id]
    seen = {}

    async def run():
        conn.task.cancel()
        await asyncio.sleep(0)
        conn.max_buffer = 1000
        conn.send_text("a" * 10)
        seen["small"] = not conn.closed
        conn.send_text("b" * 1001)
        seen["big"] = conn.closed
    client.portal.call(run)
    assert seen == {"small": True, "big": True}


def test_the_buffer_count_returns_to_zero_once_written(world, client):
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, world.admin)
    conn = client.app.state.hub.rooms[doc].conns[b.client_id]
    for n in range(5):
        assert a.change(set_depth(n + 1), n)["seq"] == n + 1
    assert [m["seq"] for m in b.drain() if m["type"] == "change"] == [1, 2, 3, 4, 5]
    assert conn.buffered == 0 and not conn._sizes


# ------------------------------------------------------------------ 2. snapshots
def test_snapshots_are_packed_off_the_event_loop_from_a_copy(make_client, monkeypatch):
    client = make_client(snapshot_every=2)
    w = World(client)
    doc = w.document()
    loop_thread = client.portal.call(lambda: asyncio.sleep(0, threading.get_ident()))
    packed = []
    real = hub_module.pack

    def spy(state):
        packed.append((threading.get_ident(), state))
        return real(state)
    monkeypatch.setattr(hub_module, "pack", spy)
    a = Live(client, doc, w.admin)
    a.send("ids.reserve", features=2, names=[])
    assert a.recv("ids.reserved")["ids"] == ["f3", "f4"]
    assert a.change(set_depth(5), 0)["seq"] == 1
    assert a.change(add_feature("f3", "Fillet1"), 1)["seq"] == 2
    room = client.app.state.hub.rooms[doc]
    assert room.snap_seq == 2 and len(packed) == 1
    thread, state = packed[0]
    assert thread != loop_thread and state is not room.state and state == room.state
    before = client.get(f"/api/documents/{doc}/content", headers=H(w.admin)).json()
    a.close()
    client.__exit__(None, None, None)
    again = make_client(snapshot_every=2)  # (a restart: the document loads from that snapshot)
    after = again.get(f"/api/documents/{doc}/content", headers=H(w.admin)).json()
    assert after["content"] == before["content"] and after["seq"] == 2
    assert after["content"]["next_id"] == before["content"]["next_id"] >= 5


# ------------------------------------------------------------------ 3. content
def test_content_now_is_not_copied_and_reads_the_same(world, client, monkeypatch):
    doc = world.document(name="Équerre")
    a = Live(client, doc, world.admin)
    assert a.change(set_depth(6), 0)["seq"] == 1
    assert a.change(set_depth(7), 1)["seq"] == 2
    client.post(f"/api/documents/{doc}/versions", json={"name": "first", "seq": 1}, headers=H(world.admin))
    real = Room.state_at

    async def no_copy_at_head(self, seq):
        assert seq != self.head, "the current state is read without a copy"
        return await real(self, seq)
    monkeypatch.setattr(Room, "state_at", no_copy_at_head)
    for query in ("", "?seq=2"):
        r = client.get(f"/api/documents/{doc}/content{query}", headers=H(world.admin))
        assert r.status_code == 200 and r.headers["content-type"] == "application/json"
        body = r.json()
        assert body["seq"] == 2 and body["document"]["name"] == "Équerre" and body["document"]["seq"] == 2
        assert body["content"]["features"][1]["params"]["depth"] == 7
    old = client.get(f"/api/documents/{doc}/content?version=first", headers=H(world.admin)).json()
    assert old["seq"] == 1 and old["content"]["features"][1]["params"]["depth"] == 6
    assert client.get(f"/api/documents/{doc}/content?seq=3", headers=H(world.admin)).status_code == 404
    outsider, _ = world.invite("out@example.com", role=None)  # (in the organisation, not in the project)
    assert client.get(f"/api/documents/{doc}/content", headers=H(outsider)).status_code == 404


# ------------------------------------------------------------------ 4. catch-up
def test_catch_up_carries_the_same_change_events_as_live(world, client):
    doc = world.document()
    a = Live(client, doc, world.admin, client_id="ada-desktop")
    b = Live(client, doc, world.admin)
    a.send("change", change_id="c1", base_seq=0, ops=set_depth(8), command="edit", args={"depth": 8})
    assert a.recv("change.accepted")["seq"] == 1
    a.send("change.revert", request_id="r", seq=1, mode="undo")
    assert a.recv("change.revert.result")["ok"]
    live = [m for m in b.drain() if m["type"] == "change"]
    late = Live(client, doc, world.admin, last_seq=0).welcome["changes"]
    assert late == live and late[1]["reverts"] == 1
    assert all(set(m) == {"type", "seq", "author", "author_name", "client_id", "change_id", "time", "ops",
                          "command", "reverts"} for m in late)  # (no inverses, arguments or rebuild failures)


# ------------------------------------------------------------------ 5. blobs
def test_blob_checks_run_together_and_are_remembered(world, client, monkeypatch):
    p = world.project
    bodies = [f"body {n}".encode() * 100 for n in range(6)]
    shas = [hashlib.sha256(x).hexdigest() for x in bodies]
    for sha, body in zip(shas[:5], bodies[:5]):
        assert client.put(f"/api/projects/{p}/blobs/{sha}", content=body, headers=H(world.admin)).status_code == 204
    doc = world.document()
    a = Live(client, doc, world.admin)
    store = client.app.state.storage
    real = store.exists
    calls, now_in, most = [], [0], [0]

    async def slow(key):
        calls.append(key)
        now_in[0] += 1
        most[0] = max(most[0], now_in[0])
        await asyncio.sleep(0.05)
        now_in[0] -= 1
        return await real(key)
    monkeypatch.setattr(store, "exists", slow)

    def use(blobs, base):
        sets = [[["params", f"b{n}"], {"$blob": s, "len": 1}] for n, s in enumerate(blobs)]
        return a.change([{"op": "feature.update", "id": "f2", "set": sets, "unset": []}], base)
    r = use(shas, 0)  # (the sixth was never uploaded)
    assert r["code"] == "missing_blob" and r["conflicts"]["blobs"] == [shas[5]]
    assert len(calls) == 6 and most[0] > 1  # asked together, not one after another
    calls.clear()
    assert use(shas[:5], 0)["seq"] == 1
    assert calls == []  # (the five found were remembered)

    async def down(key):
        raise OSError("storage is down")
    monkeypatch.setattr(store, "exists", down)
    r = use(shas, 1)
    assert r["code"] == "server_error"
    room = client.app.state.hub.rooms[doc]
    assert room.head == 1 and shas[5] not in room.known_blobs


# ------------------------------------------------------------------ 6. organisations
def test_the_organisation_list_counts_members_in_one_query(world, client):
    engine = client.app.state.db.kw["bind"].sync_engine
    statements = []

    def count(*args):
        statements.append(args[2])

    def listed():
        statements.clear()
        event.listen(engine, "before_cursor_execute", count)
        try:
            r = client.get("/api/orgs", headers=H(world.admin))
        finally:
            event.remove(engine, "before_cursor_execute", count)
        assert r.status_code == 200
        return r.json(), len(statements)

    one, queries = listed()
    assert [(o["name"], o["role"], o["members"]) for o in one] == [("Robotics", "owner", 1)]
    world.invite("bob@example.com", "Bob")
    world.invite("cy@example.com", "Cy")
    for name in ("Aero", "Zeta"):
        assert client.post("/api/orgs", json={"name": name}, headers=H(world.admin)).status_code == 201
    three, more = listed()
    assert [(o["name"], o["role"], o["members"]) for o in three] == [
        ("Aero", "owner", 1), ("Robotics", "owner", 3), ("Zeta", "owner", 1)]
    assert more == queries  # (not one more query per organisation)
    # someone else sees their own organisations only, with the same counts
    bob = client.post("/api/auth/login", json={"email": "bob@example.com", "password": "correct horse"})
    assert bob.status_code == 200, bob.text
    mine = client.get("/api/orgs", headers=H(bob.json()["token"])).json()
    assert [(o["name"], o["role"], o["members"]) for o in mine] == [("Robotics", "member", 3)]
