"""What a hostile or broken client can send, and what must not happen when it does.
Each test is a problem found in review."""
import time
from types import SimpleNamespace

import pytest

from openswx_collab_protocol.ops import OpError, absorb_ids, apply, diff, fingerprint

from conftest import H, Live, World, add_feature, part, set_depth


def content(client, doc, token):
    r = client.get(f"/api/documents/{doc}/content", headers=H(token))
    assert r.status_code == 200, r.text
    return r.json()["content"]


def healthy(client, doc, token, seq):
    """The document still loads, still takes edits, and still hands out ids."""
    live = Live(client, doc, token)
    assert live.welcome["seq"] == seq
    live.send("ids.reserve", features=1, names=["Sketch"])
    assert live.recv("ids.reserved")["ids"]
    assert live.change(set_depth(123), seq)["seq"] == seq + 1
    live.close()


def test_huge_or_pathological_names_cannot_break_a_document(world, client, make_client):
    doc = world.document()
    a = Live(client, doc, world.admin)
    for name in ("Fillet" + "9" * 5000, "1" * 40000 + "a", "x" * 201):
        assert a.change(add_feature("f3", name), 0)["code"] == "invalid"
    assert a.change(add_feature("f" + "7" * 5000, "Fillet1"), 0)["code"] == "invalid"
    # long but legal names with long numbers are simply not counted
    assert a.change(add_feature("f3", "Part" + "9" * 30), 0)["seq"] == 1
    a.close()
    client.__exit__(None, None, None)
    healthy(make_client(), doc, world.admin, 1)  # (a restart reloads from the history)
    t = time.time()
    absorb_ids({"features": [{"id": "f" + "1" * 9, "name": "1" * 150 + "a"}, {"id": "f" + "1" * 12, "name": "N" + "8" * 40}]})
    assert time.time() - t < 0.5


def test_a_feature_stays_a_feature_whatever_is_patched(world, client):
    doc = world.document()
    a = Live(client, doc, world.admin)
    upd = lambda sets=(), unsets=(): [{"op": "feature.update", "id": "f2", "set": list(sets), "unset": list(unsets)}]  # noqa: E731
    bad = [
        upd([[["name"], "Unique1"], [["name"], "Sketch1"]]),  # the second entry is the one that lands
        upd(unsets=[["name"]]), upd(unsets=[["type"]]), upd(unsets=[["id"]]),
        upd([[["name"], ["a", "list"]]]), upd([[["name"], ""]]), upd([[["type"], 5]]), upd([[["id"], "f1"]]),
        [{"op": "feature.order", "ids": [["f1"], "f2"]}],
    ]
    for ops in bad:
        assert a.change(ops, 0)["code"] == "invalid", ops
    assert a.change(upd([[["name"], "Renamed1"]]), 0)["seq"] == 1
    assert [f["name"] for f in content(client, doc, world.admin)["features"]] == ["Sketch1", "Renamed1"]
    healthy(client, doc, world.admin, 1)


def test_nesting_is_bounded(world, client):
    doc = world.document()
    a = Live(client, doc, world.admin)
    deep = cur = {}
    for _ in range(80):
        cur["n"] = {}
        cur = cur["n"]
    assert a.change([{"op": "doc.patch", "set": [[["variables", "deep"], deep]], "unset": []}], 0)["code"] == "invalid"
    # ... nor by digging a little deeper each time
    seq, path, refused = 0, ["variables"], False
    for step in range(12):
        nest = cur = {}
        for _ in range(9):
            cur["n"] = {}
            cur = cur["n"]
        path = path + ["d"]
        r = a.change([{"op": "doc.patch", "set": [[path, nest]], "unset": []}], seq)
        if r["type"] == "change.rejected":
            refused = True
            break
        seq = r["seq"]
        path = path + ["n"] * 9
    assert refused and seq >= 1
    healthy(client, doc, world.admin, seq)
    too = part()
    cur = too["variables"]
    for _ in range(80):
        cur["n"] = {}
        cur = cur["n"]
    r = client.post(f"/api/projects/{world.project}/documents", json={"name": "Deep", "content": too},
                    headers=H(world.admin))
    assert r.status_code == 422


def test_one_and_true_and_one_point_zero_are_different(world, client):
    a, b = part(), part()
    b["features"][1]["params"]["depth"] = 10.0
    b["features"][1]["suppressed"] = 0
    ops = diff(a, b)
    assert ops and fingerprint(apply(part(), ops)) == fingerprint(b) != fingerprint(a)
    assert diff(b, a)  # ... and the way back exists, so undo and restore work
    doc = world.document()
    live = Live(client, doc, world.admin)
    assert live.change(ops, 0)["seq"] == 1
    live.send("change.revert", request_id="u", seq=1)
    assert live.recv("change.revert.result")["ok"]
    assert fingerprint(content(client, doc, world.admin)) == fingerprint({k: v for k, v in a.items()})


def test_nobody_can_connect_as_someone_elses_client(world, client):
    bob, bob_id = world.invite("bob@example.com", "Bob")
    viewer, _ = world.invite("vi@example.com", role="viewer")
    doc = world.document()
    a = Live(client, doc, world.admin, client_id="ada-desktop")
    assert a.lock("f:f2")["granted"] == ["f:f2"]
    a.send("ids.reserve", features=1, names=["Fillet"])
    reserved = a.recv("ids.reserved")
    assert a.change(set_depth(20), 0, change_id="known")["seq"] == 1
    # a viewer and an editor both try Ada's id (which every peer can see)
    v = Live(client, doc, viewer, client_id="ada-desktop")
    b = Live(client, doc, bob, client_id="ada-desktop")
    assert {v.client_id, b.client_id, a.client_id} == {f"{x}.ada-desktop" for x in (world.admin_id, bob_id)} | {v.client_id}
    assert len({v.client_id, b.client_id, a.client_id}) == 3
    assert a.drain() is not None  # Ada is still connected ...
    assert b.welcome["locks"]["f:f2"]["client_id"] == a.client_id  # ... still holds her lock ...
    assert b.change(set_depth(1), 1)["code"] == "locked"
    assert b.change(add_feature(reserved["ids"][0], "Mine1"), 1)["code"] == "id_taken"  # ... and her reserved id
    # her change id is hers: Bob using it is a new change of his own, not a replay of hers
    sk = [{"op": "feature.update", "id": "f1", "set": [[["params", "x"], 1]], "unset": []}]
    assert b.change(sk, 1, change_id="known")["seq"] == 2
    # and Bob gets no exemption from the conflict check for things Ada changed
    a.send("lock.release", targets=["f:f2"])
    assert a.change(set_depth(30), 2)["seq"] == 3
    assert b.change(set_depth(31), 2)["code"] == "stale"


def test_viewers_cannot_blame_or_clear_rebuild_errors(world, client):
    viewer, _ = world.invite("vi@example.com", role="viewer")
    doc = world.document()
    a, v = Live(client, doc, world.admin), Live(client, doc, viewer)
    a.send("rebuild.report", seq=0, errors={})
    assert a.change(set_depth(0.1), 0)["seq"] == 1
    v.recv("change")
    v.send("rebuild.report", seq=1, errors={"f2": "pwned"})
    v.drain()
    assert all(m["type"] != "rebuild.status" for m in a.drain())
    assert client.get(f"/api/documents/{doc}/history", headers=H(world.admin)).json()[0]["broke"] is None
    a.send("rebuild.report", seq=1, errors={"f2": "really broken"})  # the honest report still lands
    assert v.recv("rebuild.status")["broken"]["f2"]["error"] == "really broken"


def test_a_changed_organisation_role_reaches_open_documents(world, client):
    bob, bob_id = world.invite("bob@example.com", role=None, org_role="admin")  # an org admin: owns every project
    doc = world.document()
    b = Live(client, doc, bob)
    assert b.welcome["role"] == "owner"
    assert client.patch(f"/api/orgs/{world.org}/members/{bob_id}", json={"role": "member"},
                        headers=H(world.admin)).status_code == 200
    with pytest.raises(Exception) as exc:
        for _ in range(10):
            b.send("change", change_id="late", base_seq=0, ops=set_depth(666))
            b.ws.receive_json()
    assert getattr(exc.value, "code", 4403) == 4403
    assert Live(client, doc, bob).error["code"] == "not_found"
    assert client.get(f"/api/documents/{doc}", headers=H(world.admin)).json()["seq"] == 0  # nothing got in


def test_reservations_and_locks_are_bounded_and_released(make_client):
    client = make_client(lock_grace=0.3, sweep_interval=0.1)
    world = World(client)
    doc = world.document()
    a = Live(client, doc, world.admin)
    for bad in (["Sketch9"], ["x" * 60], [" Sketch"], ["tab\there"], [""]):
        a.send("ids.reserve", names=bad)
        assert a.recv("error")["code"] == "bad_reserve", bad
    # every stem openswx uses is fine, including the ones that start with a digit or hold a slash
    a.send("ids.reserve", names=["3DSketch", "Helix/Spiral", "Boss-Extrude", "Skizze-Ä"])
    assert a.recv("ids.reserved")["names"] == ["3DSketch1", "Helix/Spiral1", "Boss-Extrude2", "Skizze-Ä1"]
    for _ in range(4):
        a.send("ids.reserve", features=64)
        assert len(a.recv("ids.reserved")["ids"]) == 64
    a.send("ids.reserve", features=1)
    assert a.recv("error")["code"] == "bad_reserve"
    got = a.lock(*[f"k:key{n}" for n in range(300)])
    assert len(got["granted"]) == 256 and len(got["denied"]) == 0  # (the request itself is cut at 256)
    assert a.lock("k:one-more")["granted"] == []
    room = client.app.state.hub.rooms[doc]
    assert len(room.reserved_ids) == 256
    a.close()
    deadline = time.time() + 5
    while room.reserved_ids and time.time() < deadline:
        time.sleep(0.05)
    assert not room.reserved_ids  # gone once the grace period after the disconnect is over


def test_a_lock_that_expires_during_someone_elses_edit_is_announced(make_client):
    client = make_client(lock_ttl=0.3, lock_grace=0.3, sweep_interval=60)  # (no sweeper: only edits notice)
    w = World(client)
    bob, _ = w.invite("bob@example.com")
    doc = w.document()
    a, b = Live(client, doc, w.admin), Live(client, doc, bob)
    assert a.lock("f:f2")["granted"]
    b.recv("lock.changed")
    time.sleep(0.5)  # Ada goes silent
    assert b.change(set_depth(5), 0)["seq"] == 1  # her lock has lapsed ...
    assert any(m["type"] == "lock.changed" and m["released"] == ["f:f2"] for m in b.seen)  # ... and Bob is told


def test_a_reader_that_never_reads_is_dropped_before_it_costs_memory(make_client):
    client = make_client(max_change_bytes=20000)  # (so the per-socket buffer limit is 80 kB)
    w = World(client)
    doc = w.document()
    a = Live(client, doc, w.admin)
    stuck = Live(client, doc, w.admin, client_id="never-reads")
    room = client.app.state.hub.rooms[doc]
    conn = room.conns[stuck.client_id]

    async def stall():  # stand in for a socket whose other end stopped reading
        conn.task.cancel()
    client.portal.call(stall)
    blob = "x" * 5000
    for n in range(60):
        a.send("presence", data={"n": n, "pad": blob})
        time.sleep(0.02)
    a.drain()
    assert conn.closed and conn.buffered <= 80000 + 6000
    assert a.change(set_depth(7), 0)["seq"] == 1  # everyone else carries on


def test_eviction_cannot_leave_two_rooms_for_one_document(make_client):
    client = make_client(room_idle=0.0, sweep_interval=60)
    w = World(client)
    doc = w.document()
    a = Live(client, doc, w.admin)
    assert a.change(set_depth(2), 0)["seq"] == 1
    hub = client.app.state.hub
    stale = hub.rooms[doc]
    a.close()
    deadline = time.time() + 5
    while stale.conns and time.time() < deadline:
        time.sleep(0.02)

    async def evict_then_join_the_stale_room():
        await hub.sweep()
        assert stale.evicted and doc not in hub.rooms
        from openswx_collab_server.hub import RoomGone
        for call in (stale.join(None, SimpleNamespace(id="u"), "owner", SimpleNamespace(client_id="c")),
                     stale.restore(None, 0)):
            try:
                await call
            except RoomGone:
                continue
            raise AssertionError("an unloaded room was used")
    client.portal.call(evict_then_join_the_stale_room)
    b, c = Live(client, doc, w.admin), Live(client, doc, w.admin)  # both land in one fresh room
    assert hub.rooms[doc] is not stale and len(hub.rooms[doc].conns) == 2
    assert b.change(set_depth(3), 1)["seq"] == 2 and c.recv("change")["seq"] == 2


def test_apply_checks_are_in_the_shared_library_too():
    for ops in ([{"op": "feature.update", "id": "f2", "set": [], "unset": [["name"]]}],
                [{"op": "feature.add", "after": None, "feature": {"id": "f9", "name": "n" * 201, "type": "t"}}],
                [{"op": "doc.patch", "set": [[["a"] * 65, 1]], "unset": []}]):
        with pytest.raises(OpError):
            apply(part(), ops)


def test_reserved_names_that_are_never_used_leave_nothing_behind(world, client):
    """Reconnecting over and over with new name stems must not fill the document's counters."""
    doc = world.document()
    for round_ in range(3):
        a = Live(client, doc, world.admin, client_id=f"reconnect-{round_}")
        a.send("ids.reserve", names=[f"Stem{round_}-{n}-" for n in range(60)])
        assert len(a.recv("ids.reserved")["names"]) == 60
        a.close()
    got = content(client, doc, world.admin)
    assert set(got["counters"]) == {"Sketch", "Boss-Extrude"}
    b = Live(client, doc, world.admin)
    b.send("ids.reserve", features=1, names=["Fillet", "Sketch"])
    r = b.recv("ids.reserved")
    assert r["names"] == ["Fillet1", "Sketch2"]
    assert b.change(add_feature(r["ids"][0], "Fillet1"), 0)["seq"] == 1
    assert content(client, doc, world.admin)["counters"]["Fillet"] == 1  # counted once it exists


def test_names_can_be_swapped_or_reused_within_one_change_and_restored(world, client):
    doc = world.document()
    a = Live(client, doc, world.admin)
    ren = lambda fid, name: {"op": "feature.update", "id": fid, "set": [[["name"], name]], "unset": []}  # noqa: E731
    assert a.change([ren("f1", "Boss-Extrude1"), ren("f2", "Sketch1")], 0)["seq"] == 1  # a swap, in one change
    assert a.change([ren("f1", "Sketch1")], 1)["code"] == "invalid"  # still no duplicates at the end of a change
    assert a.change([{"op": "feature.remove", "id": "f1"}], 1)["seq"] == 2
    assert a.change([ren("f2", "Boss-Extrude1")], 2)["seq"] == 3  # the deleted feature's name, taken over
    # every one of those can be undone, and the first state restored
    r = client.post(f"/api/documents/{doc}/restore", json={"seq": 0}, headers=H(world.admin))
    assert r.status_code == 200 and r.json() == {"seq": 4}, r.text
    got = content(client, doc, world.admin)
    assert [(f["id"], f["name"]) for f in got["features"]] == [("f1", "Sketch1"), ("f2", "Boss-Extrude1")]
    a.drain()
    a.send("change.revert", request_id="u", seq=4, mode="revert")
    assert a.recv("change.revert.result")["ok"]
    assert [(f["id"], f["name"]) for f in content(client, doc, world.admin)["features"]] == [("f2", "Boss-Extrude1")]


def test_nan_and_infinity_never_enter_a_document(world, client):
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, world.admin)
    for bad in ("NaN", "Infinity", "-Infinity"):
        a.ws.send_text('{"type":"change","change_id":"n%s","base_seq":0,"command":"x","ops":[{"op":"feature.update",'
                       '"id":"f2","set":[[["params","depth"],%s]],"unset":[]}]}' % (bad, bad))
        assert a.recv("change.rejected")["code"] == "invalid"
        a.ws.send_text('{"type":"change","change_id":"m%s","base_seq":0,"ops":[{"op":"feature.add","after":null,'
                       '"feature":{"id":"f9","name":"N1","type":"t","params":{"deep":[{"v":%s}]}}}]}' % (bad, bad))
        assert a.recv("change.rejected")["code"] == "invalid"
    # in the command's arguments it is dropped from the history instead of breaking the commit
    a.ws.send_text('{"type":"change","change_id":"args","base_seq":0,"command":"edit","args":{"x":NaN},"ops":'
                   '[{"op":"feature.update","id":"f2","set":[[["params","depth"],12.5]],"unset":[]}]}')
    assert a.recv("change.accepted")["seq"] == 1
    assert client.get(f"/api/documents/{doc}/history", headers=H(world.admin)).json()[0]["args"] is None
    # a cursor with NaN in it is not relayed (other clients could not read the frame) and does not poison late joiners
    a.ws.send_text('{"type":"presence","data":{"pointer":[NaN,0,0]}}')
    a.send("presence", data={"pointer": [1, 2, 3]})
    assert b.recv("presence")["data"] == {"pointer": [1, 2, 3]}
    assert Live(client, doc, world.admin).welcome["content"]["features"][1]["params"]["depth"] == 12.5
    r = client.post(f"/api/projects/{world.project}/documents", headers={**H(world.admin), "content-type": "application/json"},
                    content='{"name":"Bad","content":{"features":[],"variables":{"x":NaN}}}')
    assert r.status_code in (400, 422)


def test_minus_zero_is_a_change():
    a, b = part(), part()
    a["features"][1]["params"]["depth"], b["features"][1]["params"]["depth"] = 0.0, -0.0
    a["variables"]["z"], b["variables"]["z"] = 0.0, -0.0
    ops = diff(a, b)
    assert [o["op"] for o in ops] == ["feature.update", "doc.patch"] and ops[0]["set"] and ops[1]["set"]
    assert fingerprint(apply(a, ops)) == fingerprint(b)


def test_changing_the_password_closes_open_documents(world, client):
    bob, _ = world.invite("bob@example.com")
    doc = world.document()
    old = Live(client, doc, bob)
    r = client.patch("/api/me", json={"password": "a new password", "current_password": "correct horse"}, headers=H(bob))
    assert r.status_code == 200
    with pytest.raises(Exception) as exc:
        for _ in range(10):
            old.send("change", change_id="late", base_seq=0, ops=set_depth(666))
            old.ws.receive_json()
    assert getattr(exc.value, "code", 4401) == 4401
    assert client.get(f"/api/documents/{doc}", headers=H(world.admin)).json()["seq"] == 0


def test_open_connections_are_checked_against_sessions_and_roles(make_client):
    client = make_client(sweep_interval=0.1, access_recheck=0.2)
    w = World(client)
    bob, bob_id = w.invite("bob@example.com")
    cy, cy_id = w.invite("cy@example.com")
    doc = w.document()
    a, b, c = Live(client, doc, w.admin), Live(client, doc, bob), Live(client, doc, cy)
    a.recv("peer.joined"), a.recv("peer.joined")
    time.sleep(0.6)
    assert all(m["type"] != "peer.left" for m in a.drain())  # nobody is dropped while nothing changed
    # Bob signs out on that device; Cy's role is changed behind the hub's back (as if during his connect)
    assert client.post("/api/auth/logout", headers=H(bob)).status_code == 204

    async def demote():
        from openswx_collab_server.db import ProjectMember
        async with client.app.state.db() as s:
            pm = await s.get(ProjectMember, (w.project, cy_id))
            pm.role = "viewer"
            await s.commit()
    client.portal.call(demote)
    left = {a.recv("peer.left")["client_id"], a.recv("peer.left")["client_id"]}
    assert left == {b.client_id, c.client_id}
    assert Live(client, doc, cy).welcome["you"]["read_only"]  # reconnecting gives the new role
    assert a.change(set_depth(4), 0)["seq"] == 1  # the owner was left alone


def test_a_full_queue_still_closes_the_socket(world, client):
    doc = world.document()
    a = Live(client, doc, world.admin)
    stuck = Live(client, doc, world.admin, client_id="never-reads")
    conn = client.app.state.hub.rooms[doc].conns[stuck.client_id]
    closed = []

    async def stall_and_flood():
        conn.task.cancel()
        real = conn.ws.close

        async def close(code=1000, reason=""):
            closed.append(code)
            await real(code, reason)
        conn.ws.close = close
        for n in range(2100):
            conn.send({"type": "pong", "seq": n})
    client.portal.call(stall_and_flood)
    deadline = time.time() + 5
    while not closed and time.time() < deadline:
        time.sleep(0.05)
    assert conn.closed and closed == [4408]
    assert a.recv("peer.left")["client_id"] == stuck.client_id


def test_extra_keys_on_an_operation_never_reach_the_history_or_other_clients(world, client):
    """An operation is rebuilt from the keys it is made of: whatever else was on it (NaN, deep junk) is gone
    before it is stored or sent on, so it cannot break the history or other clients' parsers."""
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, world.admin)
    a.ws.send_text('{"type":"change","change_id":"j1","base_seq":0,"ops":[{"op":"feature.update","id":"f2",'
                   '"set":[[["params","depth"],11]],"unset":[],"junk":NaN}]}')
    assert a.recv("change.accepted")["seq"] == 1
    assert b.recv("change")["ops"] == set_depth(11)
    deep = "[" * 150 + "]" * 150
    a.ws.send_text('{"type":"change","change_id":"j2","base_seq":1,"ops":[{"op":"feature.update","id":"f2",'
                   '"set":[[["params","depth"],12]],"unset":[],"junk":{"fine":1},"more":%s}]}' % deep)
    assert a.recv("change.accepted")["seq"] == 2
    assert b.recv("change")["ops"] == set_depth(12)
    for n, bad in enumerate(('{"op":"feature.update","id":7,"set":[],"unset":[]}',
                             '{"op":"feature.add","after":3,"feature":{"id":"f9","name":"N1","type":"t"}}',
                             '{"op":"feature.order","ids":[NaN]}', '{"op":"nope"}')):
        a.ws.send_text('{"type":"change","change_id":"bad%d","base_seq":2,"ops":[%s]}' % (n, bad))
        assert a.recv("change.rejected")["code"] == "invalid", bad
    late = Live(client, doc, world.admin, last_seq=0)  # catching up from the history works
    assert [c["ops"] for c in late.welcome["changes"]] == [set_depth(11), set_depth(12)]


def test_replacing_a_feature_under_its_own_id_is_still_a_conflict(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, bob)
    assert a.change(set_depth(50), 0)["seq"] == 1
    swap = [{"op": "feature.remove", "id": "f2"},
            {"op": "feature.add", "after": "f1", "feature": {"id": "f2", "name": "Boss-Extrude1", "type": "extrude",
                                                             "params": {"depth": 1}, "suppressed": False}}]
    r = b.change(swap, 0)  # built before Ada's edit: it would silently throw her edit away
    assert r["code"] == "stale" and r["conflicts"]["f:f2"]["name"] == "Ada"
    assert b.change(swap, 1)["seq"] == 2  # with her edit seen, it is Bob's call


def test_reserved_names_wait_for_a_client_that_dropped(make_client):
    client = make_client(lock_grace=30)
    w = World(client)
    bob, _ = w.invite("bob@example.com")
    doc = w.document()
    a = Live(client, doc, w.admin, client_id="ada-desktop")
    a.send("ids.reserve", features=1, names=["Fillet"])
    mine = a.recv("ids.reserved")
    a.close()  # wifi drops with the fillet built locally and not yet sent
    b = Live(client, doc, bob)
    b.send("ids.reserve", features=1, names=["Fillet"])
    theirs = b.recv("ids.reserved")
    assert theirs["names"] == ["Fillet2"] and theirs["ids"] != mine["ids"]
    a2 = Live(client, doc, w.admin, client_id="ada-desktop", last_seq=0)
    assert a2.change(add_feature(mine["ids"][0], mine["names"][0]), 0)["seq"] == 1
    assert b.change(add_feature(theirs["ids"][0], theirs["names"][0]), 0)["seq"] == 2


def test_a_change_that_cannot_be_stored_is_refused_and_nothing_moves(world, client):
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, world.admin)
    room = client.app.state.hub.rooms[doc]
    real = room.hub.db

    def broken():
        raise RuntimeError("database is down")
    room.hub.db = broken
    try:
        assert a.change(set_depth(77), 0)["code"] == "server_error"
    finally:
        room.hub.db = real
    assert room.head == 0 and room.state["features"][1]["params"]["depth"] == 10
    assert all(m["type"] != "change" for m in b.drain())
    assert a.change(set_depth(78), 0, change_id="again")["seq"] == 1


def test_one_rooms_failing_snapshot_does_not_stop_the_sweep(make_client):
    client = make_client(room_idle=0.0, sweep_interval=60)
    w = World(client)
    first, second = w.document(name="A"), w.document(name="B")
    for d in (first, second):
        live = Live(client, d, w.admin)
        assert live.change(set_depth(3), 0)["seq"] == 1
        live.close()
    hub = client.app.state.hub
    deadline = time.time() + 5
    while any(r.conns for r in hub.rooms.values()) and time.time() < deadline:
        time.sleep(0.02)

    async def sweep_with_one_bad_room():
        async def fail():
            raise OSError("disk full")
        hub.rooms[first].snapshot = fail
        await hub.sweep()
    client.portal.call(sweep_with_one_bad_room)
    assert first in hub.rooms and second not in hub.rooms  # the healthy room was still unloaded


def test_storage_trouble_never_drops_the_connection_or_half_applies(make_client):
    client = make_client(snapshot_every=2)
    w = World(client)
    doc = w.document()
    a, b = Live(client, doc, w.admin), Live(client, doc, w.admin)
    store = client.app.state.storage
    real_put, real_exists = store.put, store.exists

    async def down(*args, **kw):
        raise OSError("storage is down")
    store.put = down
    try:
        assert a.change(set_depth(1), 0)["seq"] == 1
        assert a.change(set_depth(2), 1)["seq"] == 2  # a snapshot was due and failed: the change still stands
        assert [m["seq"] for m in b.drain() if m["type"] == "change"] == [1, 2]
        store.exists = down
        blob = {"$blob": "a" * 64, "len": 9}
        r = a.change([{"op": "feature.update", "id": "f2", "set": [[["params", "body"], blob]], "unset": []}], 2)
        assert r["code"] == "server_error"
    finally:
        store.put, store.exists = real_put, real_exists
    assert a.change(set_depth(3), 2)["seq"] == 3  # storage is back: the overdue snapshot is written
    room = client.app.state.hub.rooms[doc]
    assert room.snap_seq == 3


def test_two_people_cannot_both_use_one_invite(world, client):
    import threading
    link = client.post(f"/api/orgs/{world.org}/invites", json={}, headers=H(world.admin)).json()["token"]
    results = []

    def join(n):
        r = client.post("/api/auth/register", json={"email": f"racer{n}@example.com", "password": "password1",
                                                    "name": f"R{n}", "invite": link})
        results.append(r.status_code)
    threads = [threading.Thread(target=join, args=(n,)) for n in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results).count(201) == 1, results
    assert len(client.get(f"/api/orgs/{world.org}/members", headers=H(world.admin)).json()) == 2
