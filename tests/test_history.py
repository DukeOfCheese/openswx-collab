"""History: undo and revert, who broke what, versions, restore, snapshots and restarts."""
import time

from openswx_collab_protocol.ops import apply, fingerprint

from conftest import H, Live, World, add_feature, part, set_depth


def content(client, doc, token, **q):
    r = client.get(f"/api/documents/{doc}/content", params=q, headers=H(token))
    assert r.status_code == 200, r.text
    return r.json()["content"]


def revert(live, seq, mode="undo"):
    live.send("change.revert", request_id="u", seq=seq, mode=mode)
    return live.recv("change.revert.result")


def test_undo_takes_back_only_your_own_untouched_change(world, client):
    bob, bob_id = world.invite("bob@example.com", "Bob")
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, bob)
    assert a.change(set_depth(20), 0)["seq"] == 1
    assert b.change(add_feature("f3", "Fillet1"), 1)["seq"] == 2
    # Bob cannot undo Ada's change ...
    assert revert(b, 1)["code"] == "not_yours"
    # ... Ada can: it becomes a new change, and Bob's fillet stays
    r = revert(a, 1)
    assert r["ok"] and r["seq"] == 3 and r["ops"] == set_depth(10)
    ev = b.recv("change")
    while ev["seq"] != 3:
        ev = b.recv("change")
    assert ev["reverts"] == 1 and ev["command"] == "undo edit" and ev["ops"] == set_depth(10)
    got = content(client, doc, bob)
    assert got["features"][1]["params"]["depth"] == 10 and [f["id"] for f in got["features"]] == ["f1", "f2", "f3"]
    assert revert(a, 1)["code"] == "already_reverted"
    # redo is undoing the undo
    assert revert(a, 3)["seq"] == 4
    assert content(client, doc, bob)["features"][1]["params"]["depth"] == 20
    # once someone else has changed the same feature, undo is refused and says who
    assert b.change(set_depth(33), 4)["seq"] == 5
    r = revert(a, 4)
    assert r["code"] == "changed_since" and "Bob" in r["message"] and r["conflicts"]["f:f2"]["seq"] == 5
    assert revert(a, 99)["code"] == "not_found"


def test_undoing_an_add_removes_it_and_respects_locks(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, bob)
    assert a.change(add_feature("f3", "Fillet1"), 0)["seq"] == 1
    assert b.lock("structure")["granted"] == ["structure"]
    assert revert(a, 1)["code"] == "locked"
    b.send("lock.release", targets=["structure"])
    r = revert(a, 1)
    assert r["ok"] and r["ops"] == [{"op": "feature.remove", "id": "f3"}]
    assert [f["id"] for f in content(client, doc, bob)["features"]] == ["f1", "f2"]


def test_editors_can_revert_someone_elses_change(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    viewer, _ = world.invite("vi@example.com", role="viewer")
    doc = world.document()
    a, b, v = Live(client, doc, world.admin), Live(client, doc, bob), Live(client, doc, viewer)
    assert a.change(set_depth(500), 0)["seq"] == 1
    assert revert(v, 1, "revert")["code"] == "read_only"
    assert revert(b, 1, "revert")["ok"]
    hist = client.get(f"/api/documents/{doc}/history", headers=H(viewer)).json()
    assert [(h["seq"], h["author"]["name"], h["command"], h["reverts"], h["reverted_by"]) for h in hist] == \
        [(2, "Bob", "revert edit", 1, None), (1, "Ada", "edit", None, 2)]


def test_rebuild_reports_pin_the_break_on_the_change_that_caused_it(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, bob)
    assert b.change(add_feature("f3", "Fillet1"), 0)["seq"] == 1
    a.recv("change")
    a.send("rebuild.report", seq=1, errors={})  # all good so far
    assert a.change(set_depth(0.5), 1, "edit_feature")["seq"] == 2  # Ada thins the plate under Bob's fillet
    b.recv("change")
    b.send("rebuild.report", seq=2, errors={"f3": "fillet radius too large"})
    for live in (a, b):
        st = live.recv("rebuild.status")
        assert st["broken"] == {"f3": {"error": "fillet radius too large", "by_seq": 2, "by_user": world.admin_id,
                                       "by_name": "Ada", "command": "edit_feature", "since_seq": 2}}
    a.send("rebuild.report", seq=2, errors={"f3": "again"})  # a second report on the same state changes nothing
    assert all(m["type"] != "rebuild.status" for m in a.drain())
    assert client.get(f"/api/documents/{doc}/history", headers=H(bob)).json()[0]["broke"] == \
        {"f3": "fillet radius too large"}
    # a newcomer sees it; a one-click revert of change 2 fixes it
    assert "f3" in Live(client, doc, bob).welcome["broken"]
    assert revert(b, 2, "revert")["seq"] == 3
    b.send("rebuild.report", seq=3, errors={})
    assert a.recv("rebuild.status")["broken"] == {}


def test_history_pages_and_content_at_any_point(world, client):
    doc = world.document()
    a = Live(client, doc, world.admin)
    for n in range(1, 8):
        assert a.change(set_depth(n * 10), n - 1, "edit_feature")["seq"] == n
    page = client.get(f"/api/documents/{doc}/history", params={"limit": 3}, headers=H(world.admin)).json()
    assert [h["seq"] for h in page] == [7, 6, 5] and page[0]["targets"] == ["f:f2"]
    page = client.get(f"/api/documents/{doc}/history", params={"limit": 3, "before": 5}, headers=H(world.admin)).json()
    assert [h["seq"] for h in page] == [4, 3, 2]
    for seq, depth in ((0, 10), (3, 30), (7, 70)):
        assert content(client, doc, world.admin, seq=seq)["features"][1]["params"]["depth"] == depth
    assert client.get(f"/api/documents/{doc}/content", params={"seq": 8}, headers=H(world.admin)).status_code == 404


def test_named_versions_and_restore(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    viewer, _ = world.invite("vi@example.com", role="viewer")
    doc = world.document()
    a, b = Live(client, doc, world.admin), Live(client, doc, bob)
    assert a.change(set_depth(20), 0)["seq"] == 1
    v = client.post(f"/api/documents/{doc}/versions", json={"name": "Rev A", "note": "for review"}, headers=H(bob))
    assert v.status_code == 201 and v.json()["seq"] == 1 and v.json()["author"]["name"] == "Bob"
    assert a.recv("version.created")["name"] == "Rev A"
    assert client.post(f"/api/documents/{doc}/versions", json={"name": "Rev A"}, headers=H(bob)).status_code == 409
    assert client.post(f"/api/documents/{doc}/versions", json={"name": "X"}, headers=H(viewer)).status_code == 403
    assert client.post(f"/api/documents/{doc}/versions", json={"name": "Future", "seq": 9}, headers=H(bob)).status_code == 422
    assert a.change(add_feature("f3", "Fillet1"), 1)["seq"] == 2
    assert a.change(set_depth(99), 2)["seq"] == 3
    assert client.post(f"/api/documents/{doc}/versions", json={"name": "Start", "seq": 0}, headers=H(bob)).status_code == 201
    assert [(x["name"], x["seq"]) for x in client.get(f"/api/documents/{doc}/versions", headers=H(viewer)).json()] == \
        [("Rev A", 1), ("Start", 0)]
    # what an assembly pinned to "Rev A" would load
    assert content(client, doc, viewer, version="Rev A")["features"][1]["params"]["depth"] == 20

    # restore needs every lock released, then goes in as one change that everyone receives
    assert a.lock("f:f1")["granted"]
    r = client.post(f"/api/documents/{doc}/restore", json={"version": "Rev A"}, headers=H(bob))
    assert r.status_code == 409 and r.json()["detail"]["code"] == "locked"
    a.send("lock.release", targets=["f:f1"])
    assert client.post(f"/api/documents/{doc}/restore", json={"version": "Rev A"}, headers=H(viewer)).status_code == 403
    assert client.post(f"/api/documents/{doc}/restore", json={"version": "Rev A"}, headers=H(bob)).json() == {"seq": 4}
    ev = a.recv("change")
    assert ev["seq"] == 4 and ev["command"] == "restore to 1" and ev["author_name"] == "Bob"
    mirror = apply(content(client, doc, bob, seq=3), ev["ops"])  # a client applying the event ends up at Rev A
    assert fingerprint(mirror) == fingerprint(content(client, doc, bob, seq=1)) == fingerprint(content(client, doc, bob))
    assert client.post(f"/api/documents/{doc}/restore", json={"seq": 4}, headers=H(bob)).json() == {"seq": 4}  # no-op
    # nothing was lost: the restore itself can be reverted
    assert revert(a, 4, "revert")["seq"] == 5
    assert content(client, doc, bob)["features"][1]["params"]["depth"] == 99
    # only project owners delete versions
    vid = v.json()["id"]
    assert client.delete(f"/api/documents/{doc}/versions/{vid}", headers=H(bob)).status_code == 403
    assert client.delete(f"/api/documents/{doc}/versions/{vid}", headers=H(world.admin)).status_code == 204


def test_snapshots_and_a_restart_reproduce_every_state(make_client, settings, tmp_path):
    client = make_client(snapshot_every=5)
    w = World(client)
    doc = w.document()
    a = Live(client, doc, w.admin, client_id="ada-desktop")
    mirror, prints = part(), {}
    for k in ("rollback", "active_config", "name"):
        mirror.pop(k, None)
    prints[0] = fingerprint(mirror)
    a.send("ids.reserve", features=13, names=["Fillet"] * 13)
    ids = a.recv("ids.reserved")
    for n in range(1, 14):
        ops = add_feature(ids["ids"][n - 1], ids["names"][n - 1]) if n % 2 else set_depth(n)
        assert a.change(ops, n - 1)["seq"] == n
        apply(mirror, ops)
        prints[n] = fingerprint(mirror)
    assert a.lock("f:f2")["granted"]
    snaps = sorted(p.name for p in (tmp_path / "storage" / "docs" / doc / "snapshots").iterdir())
    assert snaps == ["0000000000.json.gz", "0000000005.json.gz", "0000000010.json.gz"]
    a.close()
    client.__exit__(None, None, None)  # shutting down stores the tail

    again = make_client()
    assert again.get(f"/api/documents/{doc}", headers=H(w.admin)).json()["seq"] == 13
    for seq in (0, 4, 5, 6, 10, 12, 13):
        assert fingerprint(content(again, doc, w.admin, seq=seq)) == prints[seq], seq
    a2 = Live(again, doc, w.admin, client_id="ada-desktop", last_seq=11)
    assert [c["seq"] for c in a2.welcome["changes"]] == [12, 13]
    assert a2.welcome["locks"] == {}  # locks live in memory: a restart releases them
    # the restarted server still knows who changed what last, and where the counters were
    assert a2.change(set_depth(1), 11, change_id="after-restart")["seq"] == 14  # own earlier change: no conflict
    b = Live(again, doc, w.admin, client_id="ada-laptop")
    assert b.change(set_depth(2), 13)["code"] == "stale"
    b.send("ids.reserve", features=1, names=["Fillet"])
    got = b.recv("ids.reserved")
    assert got["ids"] == ["f16"] and got["names"] == ["Fillet14"]


def test_idle_documents_are_unloaded_and_come_back(make_client):
    client = make_client(room_idle=0.2, sweep_interval=0.1, snapshot_every=1000)
    w = World(client)
    doc = w.document()
    a = Live(client, doc, w.admin)
    assert a.change(set_depth(42), 0)["seq"] == 1
    hub = client.app.state.hub
    assert doc in hub.rooms
    a.close()
    deadline = time.time() + 5
    while doc in hub.rooms and time.time() < deadline:
        time.sleep(0.05)
    assert doc not in hub.rooms
    b = Live(client, doc, w.admin)
    assert b.welcome["seq"] == 1 and b.welcome["content"]["features"][1]["params"]["depth"] == 42


def test_deleting_a_document_ends_the_session(world, client):
    doc = world.document()
    a = Live(client, doc, world.admin)
    assert client.delete(f"/api/documents/{doc}", headers=H(world.admin)).status_code == 204
    try:
        while True:
            a.ws.receive_json()
    except Exception as exc:  # noqa: BLE001
        assert getattr(exc, "code", None) == 4404
    assert Live(client, doc, world.admin).error["code"] == "not_found"
    assert client.get(f"/api/projects/{world.project}/documents", headers=H(world.admin)).json() == []


def test_document_kinds_and_content_are_checked(world, client):
    p = f"/api/projects/{world.project}/documents"
    assert client.post(p, json={"name": "A", "kind": "drawing", "content": {}}, headers=H(world.admin)).status_code == 422
    dup = part()
    dup["features"][1]["id"] = "f1"
    assert client.post(p, json={"name": "A", "content": dup}, headers=H(world.admin)).status_code == 422
    assert client.post(p, json={"name": "A", "content": {"features": [{"id": "f1"}]}}, headers=H(world.admin)).status_code == 422
    doc = client.post(p, json={"name": "Empty", "content": {"version": 3}}, headers=H(world.admin)).json()
    assert doc["format_version"] == 3 and doc["seq"] == 0
    got = content(client, doc["id"], world.admin)
    assert got["features"] == [] and got["next_id"] == 1 and "rollback" not in got
