"""Live editing: ordering, locks, id hand-out, conflicts, reconnect, presence."""
import hashlib
import time

from conftest import H, Live, World, add_feature, part, set_depth


def content(client, doc, token, **q):
    return client.get(f"/api/documents/{doc}/content", params=q, headers=H(token)).json()


def test_join_change_and_broadcast(world, client):
    bob, bob_id = world.invite("bob@example.com", "Bob")
    doc = world.document()
    a = Live(client, doc, world.admin)
    assert a.welcome["seq"] == 0 and a.welcome["role"] == "owner" and a.welcome["peers"] == []
    assert a.welcome["content"]["features"][1]["params"]["depth"] == 10
    b = Live(client, doc, bob)
    assert [p["name"] for p in b.welcome["peers"]] == ["Ada"] and b.welcome["role"] == "editor"
    assert a.recv("peer.joined")["peer"]["name"] == "Bob"
    assert a.welcome["you"]["colour"] != b.welcome["you"]["colour"]

    assert a.change(set_depth(25), 0, "edit_feature") == {"type": "change.accepted", "change_id": "c1", "seq": 1}
    ev = b.recv("change")
    assert (ev["seq"], ev["author"], ev["author_name"], ev["command"]) == (1, world.admin_id, "Ada", "edit_feature")
    assert ev["ops"] == set_depth(25)
    # the author gets the acknowledgement, not an echo
    assert all(m["type"] != "change" for m in a.drain())
    assert b.change(add_feature("f3", "Fillet1"), 1)["seq"] == 2
    assert a.recv("change")["seq"] == 2
    got = content(client, doc, bob)
    assert got["seq"] == 2 and [f["id"] for f in got["content"]["features"]] == ["f1", "f2", "f3"]
    assert got["content"]["features"][1]["params"]["depth"] == 25
    assert got["content"]["next_id"] == 4 and got["content"]["counters"]["Fillet"] == 1
    a.close(); b.close()
    assert client.get(f"/api/documents/{doc}", headers=H(bob)).json()["seq"] == 2


def test_who_may_connect(world, client):
    doc = world.document()
    outsider, _ = world.invite("out@example.com", role=None)
    assert Live(client, doc, "not a token").error["code"] == "unauthorized"
    assert Live(client, doc, outsider).error["code"] == "not_found"
    assert Live(client, "nope", world.admin).error["code"] == "not_found"
    with client.websocket_connect(f"/api/documents/{doc}/ws") as ws:
        ws.send_json({"type": "change", "change_id": "x", "base_seq": 0, "ops": []})
        assert ws.receive_json()["code"] == "bad_hello"


def test_viewers_and_other_file_formats_are_read_only(world, client):
    viewer, _ = world.invite("vi@example.com", role="viewer")
    doc = world.document()
    v = Live(client, doc, viewer)
    assert v.welcome["you"]["read_only"] and "view access" in v.welcome["read_only_reason"]
    assert v.change(set_depth(1), 0)["code"] == "read_only"
    assert v.lock("f:f2")["granted"] == []
    old = Live(client, doc, world.admin, format_version=2)
    assert old.welcome["you"]["read_only"] and "update openswx" in old.welcome["read_only_reason"]
    assert old.change(set_depth(1), 0)["code"] == "read_only"
    assert content(client, doc, viewer)["seq"] == 0


def test_locks_block_others_until_released(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, bob)
    assert a.lock("f:f2", "f:missing") == {"type": "lock.result", "request_id": "r", "granted": ["f:f2"],
                                          "denied": {"f:missing": {"client_id": "", "user_id": "", "name": "",
                                                                   "detached": False}}}
    assert b.recv("lock.changed")["acquired"]["f:f2"]["name"] == "Ada"
    denied = b.lock("f:f2", "f:f1")
    assert denied["granted"] == ["f:f1"] and denied["denied"]["f:f2"]["name"] == "Ada"
    r = b.change(set_depth(99), 0)
    assert r["code"] == "locked" and "Ada" in r["message"] and "f:f2" in r["conflicts"]
    assert b.change([{"op": "feature.remove", "id": "f2"}], 0)["code"] == "locked"
    assert a.change(set_depth(30), 0)["seq"] == 1  # the holder edits freely
    # a newcomer is told who holds what
    c = Live(client, doc, bob)
    assert set(c.welcome["locks"]) == {"f:f1", "f:f2"}
    a.send("lock.release", targets=["f:f2"])
    assert b.recv("lock.changed")["released"] == ["f:f2"]
    assert b.change(set_depth(99), 1)["seq"] == 2


def test_structure_lock_stops_deletes_and_reorders_only(world, client):
    bob, _ = world.invite("bob@example.com")
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, bob)
    assert a.lock("structure")["granted"] == ["structure"]
    assert b.change([{"op": "feature.order", "ids": ["f2", "f1"]}], 0)["code"] == "locked"
    assert b.change([{"op": "feature.remove", "id": "f2"}], 0)["code"] == "locked"
    assert b.change(set_depth(12), 0)["seq"] == 1
    assert b.change(add_feature("f3", "Fillet1"), 1)["seq"] == 2
    assert a.change([{"op": "feature.order", "ids": ["f1", "f3", "f2"]}], 2)["seq"] == 3


def test_stale_change_is_rejected_but_unrelated_ones_merge(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, bob)
    assert a.change(set_depth(20), 0)["seq"] == 1
    # Bob has not seen change 1 and edits the same feature: refused, and told who
    r = b.change(set_depth(40), 0)
    assert r["code"] == "stale" and r["conflicts"]["f:f2"]["name"] == "Ada" and r["conflicts"]["f:f2"]["seq"] == 1
    # ... a different feature from the same old base is fine
    sk = [{"op": "feature.update", "id": "f1", "set": [[["params", "sketch", "points", "P2"], [5, 5]]], "unset": []}]
    assert b.change(sk, 0)["seq"] == 2
    # ... and so are top-level keys nobody else touched
    assert b.change([{"op": "doc.patch", "set": [[["material", "name"], "Steel"]], "unset": []}], 0)["seq"] == 3
    assert a.change([{"op": "doc.patch", "set": [[["material", "density"], 7850]], "unset": []}], 0)["code"] == "stale"
    got = content(client, doc, bob)["content"]
    assert got["features"][1]["params"]["depth"] == 20 and got["features"][0]["params"]["sketch"]["points"]["P2"] == [5, 5]
    assert got["material"] == {"name": "Steel", "density": 1000.0}


def test_own_changes_in_a_row_do_not_conflict_with_each_other(world, client):
    """A client draws three sketch lines before the first acknowledgement arrives: all built on base 0."""
    doc = world.document()
    a = Live(client, doc, world.admin)
    for n in range(3):
        op = [{"op": "feature.update", "id": "f1", "set": [[["params", "sketch", "entities", f"Line{n}"], {"n": n}]],
               "unset": []}]
        a.send("change", change_id=f"q{n}", base_seq=0, ops=op, command="sketch_line")
    assert [a.recv("change.accepted")["seq"] for _ in range(3)] == [1, 2, 3]
    assert len(content(client, doc, world.admin)["content"]["features"][0]["params"]["sketch"]["entities"]) == 3


def test_invalid_changes_leave_the_document_alone(world, client):
    doc = world.document()
    a = Live(client, doc, world.admin)
    for ops in ([], [{"op": "nope"}], set_depth(1, "gone"), [{"op": "doc.patch", "set": [[["next_id"], 1]], "unset": []}],
                [{"op": "feature.add"}], add_feature("f2", "Dup1"), add_feature("f9", "Sketch1")):
        assert a.change(ops, 0)["code"] == "invalid", ops
    assert a.change(set_depth(1), 5)["code"] == "invalid"  # a base that does not exist
    a.send("no-such-type")
    assert a.recv("error")["code"] == "bad_message"
    a.ws.send_text("not json")
    assert a.recv("error")["code"] == "bad_message"
    assert a.change(set_depth(11), 0)["seq"] == 1  # the connection survives


def test_ids_and_names_come_from_the_server(world, client):
    bob, _ = world.invite("bob@example.com")
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, bob)
    a.send("ids.reserve", request_id="1", features=2, names=["Sketch", "Fillet"])
    ra = a.recv("ids.reserved")
    b.send("ids.reserve", request_id="2", features=1, names=["Sketch"])
    rb = b.recv("ids.reserved")
    assert (ra["ids"], ra["names"]) == (["f3", "f4"], ["Sketch2", "Fillet1"])
    assert (rb["ids"], rb["names"]) == (["f5"], ["Sketch3"])
    # nobody can use what another client reserved
    assert b.change(add_feature("f3", "Whatever1"), 0)["code"] == "id_taken"
    assert b.change(add_feature("f5", "Sketch2"), 0)["code"] == "id_taken"
    # both create at once from the same base: no collision, both accepted
    assert a.change(add_feature("f3", "Sketch2", ftype="sketch"), 0)["seq"] == 1
    assert b.change(add_feature("f5", "Sketch3", ftype="sketch"), 0)["seq"] == 2
    feats = content(client, doc, bob)["content"]["features"]
    assert [f["name"] for f in feats] == ["Sketch1", "Boss-Extrude1", "Sketch3", "Sketch2"]
    # an unreserved id that happens to be free is still accepted; the counters move past it
    assert a.change(add_feature("f40", "Fillet7"), 2)["seq"] == 3
    a.send("ids.reserve", request_id="3", features=1, names=["Fillet"])
    assert a.recv("ids.reserved")["ids"] == ["f41"] and a.seen is not None
    viewer, _ = world.invite("vi@example.com", role="viewer")
    v = Live(client, doc, viewer)
    v.send("ids.reserve", features=1)
    assert v.recv("error")["code"] == "read_only"


def test_resending_a_change_is_harmless(world, client, make_client):
    doc = world.document()
    a = Live(client, doc, world.admin, client_id="desktop-ada-1")
    assert a.change(set_depth(20), 0, change_id="abc")["seq"] == 1
    assert a.change(set_depth(20), 0, change_id="abc")["seq"] == 1  # same answer, not applied twice
    assert client.get(f"/api/documents/{doc}", headers=H(world.admin)).json()["seq"] == 1
    a.close()
    client.__exit__(None, None, None)
    again = make_client()  # the server restarted: the history remembers the change id
    a2 = Live(again, doc, world.admin, client_id="desktop-ada-1", last_seq=1)
    assert a2.welcome["changes"] == [] and a2.welcome["seq"] == 1
    assert a2.change(set_depth(20), 0, change_id="abc")["seq"] == 1
    assert a2.change(set_depth(21), 1, change_id="abd")["seq"] == 2


def test_reconnect_catches_up_and_keeps_locks(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    doc = world.document()
    a, b = Live(client, doc, world.admin, client_id="ada-desktop"), Live(client, doc, bob)
    assert a.lock("f:f1")["granted"] == ["f:f1"]
    b.recv("lock.changed")
    a.close()  # Ada's wifi drops
    assert b.recv("peer.left")["client_id"] == f"{world.admin_id}.ada-desktop"
    assert b.recv("lock.changed")["acquired"]["f:f1"]["detached"] is True
    assert b.change(set_depth(15), 0)["seq"] == 1
    assert b.change(add_feature("f3", "Fillet1"), 1)["seq"] == 2
    # her sketch is still hers during the grace period
    assert b.lock("f:f1")["denied"]["f:f1"]["name"] == "Ada"
    a2 = Live(client, doc, world.admin, client_id="ada-desktop", last_seq=0)
    assert "content" not in a2.welcome and [c["seq"] for c in a2.welcome["changes"]] == [1, 2]
    assert a2.welcome["changes"][0]["ops"] == set_depth(15) and a2.welcome["changes"][0]["author_name"] == "Bob"
    assert a2.welcome["locks"]["f:f1"]["detached"] is False
    # what she drew while offline goes in on top of the server's head
    sk = [{"op": "feature.update", "id": "f1", "set": [[["params", "sketch", "points", "P9"], [1, 2]]], "unset": []}]
    assert a2.change(sk, 0)["seq"] == 3
    # what she did offline to something Bob changed meanwhile is refused, with Bob's name
    r = a2.change(set_depth(77), 0)
    assert r["code"] == "stale" and r["conflicts"]["f:f2"]["name"] == "Bob"


def test_far_behind_or_unknown_position_gets_the_whole_document(make_client):
    client = make_client(catchup_limit=2)
    w = World(client)
    doc = w.document()
    a = Live(client, doc, w.admin)
    for n in range(4):
        assert a.change(set_depth(n + 1), n)["seq"] == n + 1
    assert [c["seq"] for c in Live(client, doc, w.admin, last_seq=2).welcome["changes"]] == [3, 4]
    late = Live(client, doc, w.admin, last_seq=1).welcome
    assert "changes" not in late and late["content"]["features"][1]["params"]["depth"] == 4
    assert "content" in Live(client, doc, w.admin, last_seq=99).welcome


def test_locks_expire_after_the_grace_period_and_when_a_client_goes_silent(make_client):
    client = make_client(lock_ttl=0.4, lock_grace=0.4, sweep_interval=0.1)
    w = World(client)
    bob, _ = w.invite("bob@example.com")
    doc = w.document()
    a, b = Live(client, doc, w.admin), Live(client, doc, bob)
    a.lock("f:f1")
    b.lock("f:f2")
    a.recv("lock.changed"); b.recv("lock.changed")
    a.close()
    assert b.recv("lock.changed")["acquired"]["f:f1"]["detached"]
    assert "f:f1" in b.recv("lock.changed")["released"]  # grace over
    # Bob stays connected but silent: his lock goes too (a real client pings)
    time.sleep(0.8)
    c = Live(client, doc, w.admin)
    assert c.welcome["locks"] == {}
    assert c.change(set_depth(3), 0)["seq"] == 1


def test_presence_and_drafts_are_relayed_not_stored(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    viewer, _ = world.invite("vi@example.com", role="viewer")
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, bob)
    pointer = {"pointer": {"point": [1, 2, 3], "face": "Boss-Extrude1:cap+"}, "tool": "fillet",
               "selection": ["Boss-Extrude1:cap+"], "camera": {"eye": [0, 0, 100]}}
    a.send("presence", data=pointer)
    got = b.recv("presence")
    assert got["client_id"] == a.client_id and got["data"] == pointer
    a.send("draft", data={"sketch": "f1", "entities": {"Line9": {"p1": [0, 0], "p2": [4, 4]}}})
    assert b.recv("draft")["data"]["entities"]["Line9"]["p2"] == [4, 4]
    assert all(m["type"] not in ("presence", "draft") for m in a.drain())  # no echo
    # someone who joins later sees where everyone is and what they are drawing
    v = Live(client, doc, viewer)
    ada = next(p for p in v.welcome["peers"] if p["name"] == "Ada")
    assert ada["presence"]["tool"] == "fillet" and "Line9" in ada["draft"]["entities"]
    v.send("presence", data={"tool": "measure"})  # viewers have a cursor too
    assert b.recv("presence")["data"] == {"tool": "measure"}
    v.send("draft", data={"x": 1})  # ... but nothing to preview
    a.send("draft", data=None)
    assert b.recv("draft") == {"type": "draft", "client_id": a.client_id, "data": None}
    assert client.get(f"/api/documents/{doc}", headers=H(bob)).json()["seq"] == 0


def test_presence_flood_is_dropped(make_client):
    client = make_client(presence_rate=5)
    w = World(client)
    bob, _ = w.invite("bob@example.com")
    doc = w.document()
    a, b = Live(client, doc, w.admin), Live(client, doc, bob)
    time.sleep(1.1)  # (a full bucket)
    for n in range(200):
        a.send("presence", data={"n": n})
    a.drain()
    assert 1 <= sum(1 for m in b.drain() if m["type"] == "presence") < 50


def test_losing_access_disconnects(world, client):
    bob, bob_id = world.invite("bob@example.com")
    doc = world.document()
    b = Live(client, doc, bob)
    assert client.delete(f"/api/projects/{world.project}/members/{bob_id}", headers=H(world.admin)).status_code == 204
    try:
        while True:
            b.ws.receive_json()
    except Exception as exc:  # noqa: BLE001 - the server closed the socket
        assert getattr(exc, "code", None) == 4403
    assert Live(client, doc, bob).error["code"] == "not_found"


def test_blobs_must_be_uploaded_before_they_are_referenced(world, client):
    body = ("B" * 40000).encode()
    sha = hashlib.sha256(body).hexdigest()
    ref = {"$blob": sha, "len": len(body)}
    imported = part()
    imported["features"].append({"id": "f3", "name": "Imported1", "type": "import", "params": {"brep": ref},
                                 "suppressed": False})
    p = world.project
    r = client.post(f"/api/projects/{p}/documents", json={"name": "Imp", "content": imported}, headers=H(world.admin))
    assert r.status_code == 409 and r.json()["detail"]["blobs"] == [sha]
    assert client.post(f"/api/projects/{p}/blobs/missing", json=[sha], headers=H(world.admin)).json() == [sha]
    assert client.put(f"/api/projects/{p}/blobs/{sha}", content=b"something else", headers=H(world.admin)).status_code == 422
    assert client.put(f"/api/projects/{p}/blobs/{sha}", content=body, headers=H(world.admin)).status_code == 204
    assert client.post(f"/api/projects/{p}/blobs/missing", json=[sha], headers=H(world.admin)).json() == []
    assert client.get(f"/api/projects/{p}/blobs/{sha}", headers=H(world.admin)).content == body
    doc = world.document(imported, "Imp")
    a = Live(client, doc, world.admin)
    other = {"$blob": "0" * 64, "len": 5}
    r = a.change([{"op": "feature.update", "id": "f3", "set": [[["params", "brep"], other]], "unset": []}], 0)
    assert r["code"] == "missing_blob" and r["conflicts"]["blobs"] == ["0" * 64]
    viewer, _ = world.invite("vi@example.com", role="viewer")
    assert client.get(f"/api/projects/{p}/blobs/{sha}", headers=H(viewer)).status_code == 200
    assert client.put(f"/api/projects/{p}/blobs/{sha}", content=body, headers=H(viewer)).status_code == 403
