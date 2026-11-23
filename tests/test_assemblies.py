"""Assemblies: components that use parts by reference, pinned to a point of the part's history."""
import copy

import pytest

from openswx_collab_protocol.ops import OpError, apply, diff, fingerprint, is_structural, targets

from conftest import H, Live, World, set_depth


def comp(cid, part, seq, version=None, **more):
    return {"id": cid, "name": f"Bracket-{cid}", "x": [0, 0, 0, 0, 0, 0], "fixed": False,
            "ref": {"document": part, "seq": seq, "version": version}, **more}


def assembly(*components, mates=()):
    return {"format": "openswx-assembly", "version": 3, "components": list(components), "mates": list(mates),
            "features": [], "display_states": {}}


def make(client, w, content, name="Rover", token=None, project=None):
    return client.post(f"/api/projects/{project or w.project}/documents", headers=H(token or w.admin),
                       json={"name": name, "kind": "assembly", "content": content})


def set_ref(cid, seq, version=None):
    return [{"op": "item.update", "in": "components", "id": cid, "unset": [],
             "set": [[["ref", "seq"], seq], [["ref", "version"], version]]}]


def refs(client, doc, token):
    r = client.get(f"/api/documents/{doc}/references", headers=H(token))
    assert r.status_code == 200, r.text
    return {x["component"]: x for x in r.json()}


# ------------------------------------------------------------------ the shared operations
def test_components_and_mates_diff_and_apply_like_features():
    a = assembly(comp("c1", "p", 0), comp("c2", "p", 0), mates=[{"id": "m1", "type": "coincident", "a": "c1", "b": "c2"}])
    b = copy.deepcopy(a)
    b["components"][1]["x"] = [5, 0, 0, 0, 0, 0]
    b["components"].append(comp("c3", "q", 4))
    b["components"] = [b["components"][i] for i in (2, 1, 0)]  # the new one first, the old two swapped
    del b["mates"][0]
    b["mates"].append({"id": "m2", "type": "distance", "d": 4.5})
    b["display_states"]["hidden"] = ["c2"]
    ops = diff(a, b)
    assert [(o["op"], o.get("in")) for o in ops] == [
        ("item.add", "components"), ("item.update", "components"), ("item.order", "components"),
        ("item.remove", "mates"), ("item.add", "mates"), ("doc.patch", None)]
    assert targets(ops) == {"i:components:c3", "i:components:c2", "order:components", "i:mates:m1", "i:mates:m2",
                            "k:display_states"}
    assert is_structural(ops)
    assert fingerprint(apply(copy.deepcopy(a), ops)) == fingerprint(b)
    assert fingerprint(apply(copy.deepcopy(b), diff(b, a))) == fingerprint(a)
    c = copy.deepcopy(a)
    c["components"].insert(0, comp("c0", "p", 0))  # adding alone needs no order operation
    assert [o["op"] for o in diff(a, c)] == ["item.add"] and not is_structural(diff(a, c))
    for bad in ([{"op": "item.add", "in": "widgets", "item": {"id": "x"}}],
                [{"op": "item.add", "in": "components", "item": {"name": "no id"}}],
                [{"op": "item.add", "in": "components", "item": {"id": "c1"}}],
                [{"op": "item.update", "in": "mates", "id": "m1", "set": [[["id"], "m9"]], "unset": []}],
                [{"op": "item.order", "in": "components", "ids": ["c1"]}],
                [{"op": "doc.patch", "set": [[["components"], []]], "unset": []}]):
        with pytest.raises(OpError):
            apply(copy.deepcopy(a), bad)


# ------------------------------------------------------------------ on the server
def test_an_assembly_pins_parts_and_checks_what_it_points_at(world, client):
    part = world.document()
    a = Live(client, part, world.admin)
    assert a.change(set_depth(20), 0)["seq"] == 1
    other_kind = make(client, world, assembly()).json()["id"]
    for bad, why in ((assembly(comp("c1", "nope", 0)), "no part"),
                     (assembly(comp("c1", part, 9)), "no state 9"),
                     (assembly(comp("c1", other_kind, 0)), "not a part"),
                     (assembly({"id": "c1", "part": {"features": []}}), "does not carry copies"),
                     (assembly({"id": "c1", "ref": {"document": part, "seq": "1"}}), "needs ref"),
                     (assembly({"id": "c1", "ref": {"document": part, "seq": True}}), "needs ref"),
                     (assembly(comp("c1", part, 0), comp("c1", part, 0)), "distinct ids")):
        r = make(client, world, bad)
        assert r.status_code == 422 and why in r.text, (why, r.text)
    asm = make(client, world, assembly(comp("c1", part, 1), comp("c2", part, 0))).json()
    assert asm["kind"] == "assembly"
    got = refs(client, asm["id"], world.admin)
    assert got["c1"]["pinned"] == {"seq": 1, "versions": []} and got["c1"]["newer_edits"] is False
    assert got["c2"]["newer_edits"] is True and got["c2"]["changes_since_pinned"] == 1
    assert got["c2"]["update_available"] is False and got["c2"]["latest_version"] is None
    assert client.get(f"/api/documents/{part}/references", headers=H(world.admin)).status_code == 422
    # parts stay parts
    r = client.post(f"/api/projects/{world.project}/documents", headers=H(world.admin),
                    json={"name": "P", "content": {"features": [], "components": []}})
    assert r.status_code == 422


def test_editing_a_part_never_changes_an_assembly_until_someone_updates_it(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    part = world.document()
    asm = make(client, world, assembly(comp("c1", part, 0), comp("c2", part, 0),
                                       mates=[{"id": "m1", "type": "coincident", "a": "c1", "b": "c2"}])).json()["id"]
    p = Live(client, part, world.admin)
    a, b = Live(client, asm, world.admin), Live(client, asm, bob)
    assert a.welcome["content"]["components"][0]["ref"] == {"document": part, "seq": 0, "version": None}
    # Ada reworks the bracket: three edits, then names it
    for n in range(3):
        assert p.change(set_depth(20 + n), n)["seq"] == n + 1
    assert all(m["type"] not in ("change", "reference.update") for m in b.drain())  # the assembly is untouched
    assert refs(client, asm, bob)["c1"]["update_available"] is False  # edits alone are not an "update"
    client.post(f"/api/documents/{part}/versions", json={"name": "Rev B"}, headers=H(world.admin))
    ev = b.recv("reference.update")
    assert ev == {"type": "reference.update", "document": part, "components": ["c1", "c2"]}  # /references says more
    got = refs(client, asm, bob)["c1"]
    assert got["update_available"] and got["latest_version"]["name"] == "Rev B" and got["changes_since_pinned"] == 3
    # what changed in between, for the client's "which mates would break" check
    between = client.get(f"/api/documents/{part}/history", params={"after": 0, "before": 4}, headers=H(bob)).json()
    assert [h["seq"] for h in between] == [3, 2, 1] and between[0]["targets"] == ["f:f2"]
    old = client.get(f"/api/documents/{part}/content", params={"seq": 0}, headers=H(bob)).json()["content"]
    assert old["features"][1]["params"]["depth"] == 10  # the pinned state is still there to load
    # Bob moves one component to Rev B: an ordinary change, locked and conflict-checked like any other
    assert a.lock("i:components:c2")["granted"] == ["i:components:c2"]
    assert b.change(set_ref("c2", 3, "Rev B"), 0)["code"] == "locked"
    assert b.change(set_ref("c1", 3, "Rev B"), 0)["seq"] == 1
    assert a.recv("change")["ops"] == set_ref("c1", 3, "Rev B")
    got = refs(client, asm, bob)
    assert got["c1"]["pinned"] == {"seq": 3, "versions": ["Rev B"]} and not got["c1"]["update_available"]
    assert got["c2"]["update_available"]
    # ... and cannot point it at a state the part does not have, or at nothing
    assert b.change(set_ref("c1", 99), 1)["code"] == "invalid"
    assert b.change([{"op": "item.update", "in": "components", "id": "c1", "set": [], "unset": [["ref"]]}], 1)["code"] == "invalid"
    assert refs(client, asm, bob)["c1"]["pinned"]["seq"] == 3
    # undo puts the pin back
    b.send("change.revert", request_id="u", seq=1)
    assert b.recv("change.revert.result")["ok"]
    assert refs(client, asm, bob)["c1"]["pinned"]["seq"] == 0


def test_components_mates_and_positions_are_edited_live(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    part = world.document()
    asm = make(client, world, assembly(comp("c1", part, 0))).json()["id"]
    a, b = Live(client, asm, world.admin), Live(client, asm, bob)
    add = [{"op": "item.add", "in": "components", "after": "c1", "item": comp("c2", part, 0)},
           {"op": "item.add", "in": "mates", "after": None, "item": {"id": "m1", "type": "coincident", "a": "c1", "b": "c2"}}]
    assert a.change(add, 0)["seq"] == 1
    assert b.recv("change")["ops"] == add
    # both move different components at the same moment: both go in
    move = lambda cid, x: [{"op": "item.update", "in": "components", "id": cid, "set": [[["x"], x]], "unset": []}]  # noqa: E731
    assert a.change(move("c1", [1, 0, 0, 0, 0, 0]), 1)["seq"] == 2
    assert b.change(move("c2", [0, 2, 0, 0, 0, 0]), 1)["seq"] == 3
    # ... the same component: the second is told who got there first
    r = b.change(move("c1", [9, 9, 9, 0, 0, 0]), 1)
    assert r["code"] == "stale" and r["conflicts"]["i:components:c1"]["name"] == "Ada"
    # deleting needs the structure lock to be free, and a lock on something that is gone is refused
    assert a.lock("structure", "i:mates:m1", "i:mates:gone")["granted"] == ["structure", "i:mates:m1"]
    assert b.change([{"op": "item.remove", "in": "components", "id": "c2"}], 3)["code"] == "locked"
    a.send("lock.release", targets=["structure", "i:mates:m1"])
    assert b.change([{"op": "item.remove", "in": "mates", "id": "m1"}, {"op": "item.remove", "in": "components", "id": "c2"}], 3)["seq"] == 4
    got = client.get(f"/api/documents/{asm}/content", headers=H(bob)).json()["content"]
    assert [c["id"] for c in got["components"]] == ["c1"] and got["mates"] == [] and got["components"][0]["x"][0] == 1
    assert set(refs(client, asm, bob)) == {"c1"}
    # restore brings the component, its mate and its reference back
    assert client.post(f"/api/documents/{asm}/restore", json={"seq": 3}, headers=H(bob)).json() == {"seq": 5}
    assert set(refs(client, asm, bob)) == {"c1", "c2"}
    assert client.get(f"/api/documents/{asm}/history", headers=H(bob)).json()[0]["targets"] == \
        ["i:components:c2", "i:mates:m1"]


def test_where_a_part_is_used_and_why_it_cannot_be_deleted(world, client):
    part, loose = world.document(name="Bracket"), world.document(name="Loose")
    one = make(client, world, assembly(comp("c1", part, 0), comp("c2", part, 0)), "Rover").json()["id"]
    two = make(client, world, assembly(comp("k1", part, 0)), "Arm").json()["id"]
    used = client.get(f"/api/documents/{part}/used-in", headers=H(world.admin)).json()
    assert [(u["document"]["name"], [c["component"] for c in u["components"]]) for u in used] == \
        [("Arm", ["k1"]), ("Rover", ["c1", "c2"])]
    assert client.get(f"/api/documents/{loose}/used-in", headers=H(world.admin)).json() == []
    r = client.delete(f"/api/documents/{part}", headers=H(world.admin))
    assert r.status_code == 409 and [a["name"] for a in r.json()["detail"]["assemblies"]] == ["Arm", "Rover"]
    assert r.json()["detail"]["in_projects_you_cannot_open"] == 0
    assert client.delete(f"/api/documents/{loose}", headers=H(world.admin)).status_code == 204
    # once nothing uses it (assemblies deleted, or the component removed), it can go
    assert client.delete(f"/api/documents/{two}", headers=H(world.admin)).status_code == 204
    a = Live(client, one, world.admin)
    assert a.change([{"op": "item.remove", "in": "components", "id": "c1"},
                     {"op": "item.remove", "in": "components", "id": "c2"}], 0)["seq"] == 1
    a.close()
    assert client.delete(f"/api/documents/{part}", headers=H(world.admin)).status_code == 204


def test_references_respect_projects_and_organisations(world, client, make_client):
    # a second project in the same organisation, which Bob cannot see
    secret = client.post(f"/api/orgs/{world.org}/projects", json={"name": "Secret"}, headers=H(world.admin)).json()["id"]
    hidden = client.post(f"/api/projects/{secret}/documents", json={"name": "Gearbox", "content": {"features": []}},
                         headers=H(world.admin)).json()["id"]
    bob, _ = world.invite("bob@example.com", "Bob")
    shared = world.document()
    # Bob cannot build on a part he cannot open, and cannot tell it from one that does not exist
    r = make(client, world, assembly(comp("c1", hidden, 0)), token=bob)
    assert r.status_code == 422 and "there is no part" in r.text
    asm = make(client, world, assembly(comp("c1", shared, 0)), token=bob).json()["id"]
    b = Live(client, asm, bob)
    add = [{"op": "item.add", "in": "components", "after": "c1", "item": comp("c2", hidden, 0)}]
    assert b.change(add, 0)["code"] == "invalid"
    # Ada can (she owns both projects); Bob then sees that the component exists and nothing about its part
    a = Live(client, asm, world.admin)
    assert a.change(add, 0)["seq"] == 1
    got = refs(client, asm, bob)
    assert got["c2"] == {"component": "c2", "document": {"id": hidden, "accessible": False}, "pinned": {"seq": 0}}
    assert got["c1"]["document"]["name"] == "Bracket"
    assert client.get(f"/api/documents/{hidden}/content", headers=H(bob)).status_code == 404
    # where-used only lists assemblies the asker can open
    private = make(client, world, assembly(comp("z1", shared, 0)), "Private", project=secret).json()["id"]
    names = lambda t: [u["document"]["name"] for u in client.get(f"/api/documents/{shared}/used-in", headers=H(t)).json()]  # noqa: E731
    assert names(world.admin) == ["Private", "Rover"] and names(bob) == ["Rover"]
    # another organisation's part is out of reach even for a server admin's own second organisation
    other_org = client.post("/api/orgs", json={"name": "Other"}, headers=H(world.admin)).json()["id"]
    other_project = client.post(f"/api/orgs/{other_org}/projects", json={"name": "X"}, headers=H(world.admin)).json()["id"]
    foreign = client.post(f"/api/projects/{other_project}/documents", json={"name": "Foreign", "content": {"features": []}},
                          headers=H(world.admin)).json()["id"]
    assert make(client, world, assembly(comp("c1", foreign, 0))).status_code == 422
    del private


def test_an_assembly_survives_a_restart_with_its_references(make_client):
    client = make_client(snapshot_every=2)
    w = World(client)
    part = w.document()
    asm = make(client, w, assembly(comp("c1", part, 0))).json()["id"]
    a = Live(client, asm, w.admin)
    mirror = copy.deepcopy(a.welcome["content"])
    for n in range(1, 6):
        ops = [{"op": "item.add", "in": "components", "after": "c1", "item": comp(f"n{n}", part, 0)}] if n % 2 else \
            [{"op": "item.update", "in": "components", "id": "c1", "set": [[["x"], [n, 0, 0, 0, 0, 0]]], "unset": []}]
        assert a.change(ops, n - 1)["seq"] == n
        apply(mirror, ops)
    a.close()
    client.__exit__(None, None, None)
    again = make_client()
    got = again.get(f"/api/documents/{asm}/content", headers=H(w.admin)).json()
    assert got["seq"] == 5 and fingerprint(got["content"]) == fingerprint(mirror)
    assert set(refs(again, asm, w.admin)) == {"c1", "n1", "n3", "n5"}
    live = Live(again, asm, w.admin)
    assert live.change([{"op": "item.remove", "in": "components", "id": "n3"}], 5)["seq"] == 6
    assert set(refs(again, asm, w.admin)) == {"c1", "n1", "n5"}


def test_parts_have_no_components(world, client):
    part = world.document()
    a = Live(client, part, world.admin)
    r = a.change([{"op": "item.add", "in": "components", "after": None, "item": {"id": "c1", "part": {"big": "copy"}}}], 0)
    assert r["code"] == "invalid" and "only assemblies" in r["message"]
    assert a.change(set_depth(3), 0)["seq"] == 1


def test_working_in_an_assembly_whose_parts_you_cannot_open(world, client):
    """Bob edits the assembly but has no access to the project one of its parts lives in."""
    secret = client.post(f"/api/orgs/{world.org}/projects", json={"name": "Secret"}, headers=H(world.admin)).json()["id"]
    hidden = client.post(f"/api/projects/{secret}/documents", json={"name": "SecretGearbox", "content": {"features": []}},
                         headers=H(world.admin)).json()["id"]
    bob, _ = world.invite("bob@example.com", "Bob")
    shared = world.document()
    asm = make(client, world, assembly(comp("c1", hidden, 0), comp("c2", hidden, 0), comp("c3", shared, 0))).json()["id"]
    b = Live(client, asm, bob)
    move = [{"op": "item.update", "in": "components", "id": "c1", "set": [[["x"], [7, 0, 0, 0, 0, 0]]], "unset": []}]
    assert b.change(move, 0)["seq"] == 1  # moving it is fine
    assert b.change([{"op": "item.remove", "in": "components", "id": "c2"}], 1)["seq"] == 2
    b.send("change.revert", request_id="u", seq=2)
    assert b.recv("change.revert.result")["ok"]  # ... and so is taking the removal back
    assert client.post(f"/api/documents/{asm}/restore", json={"seq": 0}, headers=H(bob)).status_code == 200
    # what he cannot do is choose that part: a new component of it, or pointing another component at it
    assert b.change([{"op": "item.add", "in": "components", "after": None, "item": comp("c9", hidden, 0)}], 4)["code"] == "invalid"
    swap = [{"op": "item.update", "in": "components", "id": "c3", "set": [[["ref", "document"], hidden]], "unset": []}]
    assert b.change(swap, 4)["code"] == "invalid"
    # ... or choose another state of it, which would also tell him how long its history is and what it is called
    for seq in (1, 999):
        r = b.change(set_ref("c1", seq), 4)
        assert r["code"] == "invalid" and "SecretGearbox" not in r["message"] and "there is no part" in r["message"]
    whole = [{"op": "item.update", "in": "components", "id": "c1", "unset": [],
              "set": [[["ref"], {"document": hidden, "seq": 1, "version": None}]]}]
    assert b.change(whole, 4)["code"] == "invalid"
    readd = [{"op": "item.remove", "in": "components", "id": "c1"},
             {"op": "item.add", "in": "components", "after": None, "item": comp("c1", hidden, 1)}]
    assert b.change(readd, 4)["code"] == "invalid"
    assert set(refs(client, asm, bob)) == {"c1", "c2", "c3"}
    # a new version of the secret part is announced without its name
    owner = Live(client, hidden, world.admin)
    assert owner.change([{"op": "doc.patch", "set": [[["material"], {"name": "Steel"}]], "unset": []}], 0)["seq"] == 1
    v = client.post(f"/api/documents/{hidden}/versions", json={"name": "Classified Rev 2"}, headers=H(world.admin))
    assert v.status_code == 201
    ev = b.recv("reference.update")
    assert "SecretGearbox" not in str(ev) and "Classified" not in str(ev) and ev["components"] == ["c1", "c2"]
    assert "SecretGearbox" not in str(refs(client, asm, bob))


def test_a_project_cannot_be_deleted_from_under_other_projects_assemblies(world, client):
    library = client.post(f"/api/orgs/{world.org}/projects", json={"name": "Library"}, headers=H(world.admin)).json()["id"]
    bolt = client.post(f"/api/projects/{library}/documents", json={"name": "Bolt", "content": {"features": []}},
                       headers=H(world.admin)).json()["id"]
    inside = make(client, world, assembly(comp("c1", bolt, 0)), "Own", project=library).json()["id"]
    rover = make(client, world, assembly(comp("c1", bolt, 0)), "Rover").json()["id"]
    r = client.delete(f"/api/projects/{library}", headers=H(world.admin))
    assert r.status_code == 409 and r.json()["detail"]["assemblies"] == [{"id": rover, "name": "Rover"}]
    assert client.delete(f"/api/documents/{rover}", headers=H(world.admin)).status_code == 204
    assert client.delete(f"/api/projects/{library}", headers=H(world.admin)).status_code == 204  # own assemblies go with it
    assert client.get(f"/api/documents/{inside}", headers=H(world.admin)).status_code == 404


def test_the_delete_guard_does_not_name_assemblies_you_cannot_open(world, client):
    carol, carol_id = world.invite("carol@example.com", "Carol", role=None)
    mine = client.post(f"/api/orgs/{world.org}/projects", json={"name": "Carols"}, headers=H(carol)).json()["id"]
    part = client.post(f"/api/projects/{mine}/documents", json={"name": "Hinge", "content": {"features": []}},
                       headers=H(carol)).json()["id"]
    # the organisation owner (who can open everything) builds on it in a project Carol is not in
    make(client, world, assembly(comp("c1", part, 0)), "TopSecretRover")
    r = client.delete(f"/api/documents/{part}", headers=H(carol))
    assert r.status_code == 409 and "TopSecretRover" not in r.text
    assert r.json()["detail"]["assemblies"] == [] and r.json()["detail"]["in_projects_you_cannot_open"] == 1
    del carol_id


def test_undo_and_restore_still_work_after_a_part_they_bring_back_was_deleted(world, client):
    keep, doomed = world.document(name="Keep"), world.document(name="Doomed")
    asm = make(client, world, assembly(comp("c1", keep, 0), comp("c2", doomed, 0))).json()["id"]
    a = Live(client, asm, world.admin)
    assert a.change([{"op": "item.remove", "in": "components", "id": "c2"}], 0)["seq"] == 1
    assert client.delete(f"/api/documents/{doomed}", headers=H(world.admin)).status_code == 204  # nothing uses it now
    move = [{"op": "item.update", "in": "components", "id": "c1", "set": [[["x"], [3, 0, 0, 0, 0, 0]]], "unset": []}]
    assert a.change(move, 1)["seq"] == 2
    # the whole restore is not held hostage by the one component whose part is gone
    assert client.post(f"/api/documents/{asm}/restore", json={"seq": 0}, headers=H(world.admin)).json() == {"seq": 3}
    got = refs(client, asm, world.admin)
    assert set(got) == {"c1", "c2"} and got["c2"]["document"]["deleted"] is True
    # it can still be moved, as long as it keeps pointing where it did ...
    move2 = [{"op": "item.update", "in": "components", "id": "c2", "set": [[["x"], [0, 5, 0, 0, 0, 0]]], "unset": []}]
    assert a.change(move2, 3)["seq"] == 4
    # (and relabelled: same part, same state)
    assert a.change(set_ref("c2", 0, "relabelled"), 4)["seq"] == 5
    # ... but nobody can newly choose a deleted part, for a new component or by repointing one
    add = [{"op": "item.add", "in": "components", "after": None, "item": comp("c9", doomed, 0)}]
    assert a.change(add, 5)["code"] == "invalid"
    repoint = [{"op": "item.update", "in": "components", "id": "c1", "set": [[["ref", "document"], doomed]], "unset": []}]
    assert a.change(repoint, 5)["code"] == "invalid"


def test_a_delete_locks_before_it_looks_for_users(world, client, monkeypatch):
    """A part deleted at the very moment someone adds it to an assembly: the delete marks the part deleted
    (taking the row lock) before it asks whether anything uses it, and the change locks the same row before
    it records the use, so one always sees the other. Checked here by looking at what the delete has
    already written when it asks."""
    from openswx_collab_server import assemblies as A
    from openswx_collab_server import documents as D
    from openswx_collab_server import orgs as O
    from openswx_collab_server.db import Document
    part = world.document()
    seen = {}

    async def watching_used_in(db, part_id):
        seen["deleted_when_checked"] = (await db.get(Document, part_id)).deleted_at is not None
        return await A.used_in(db, part_id)
    monkeypatch.setattr(D, "used_in", watching_used_in)
    asm = make(client, world, assembly(comp("c1", part, 0))).json()["id"]
    assert client.delete(f"/api/documents/{part}", headers=H(world.admin)).status_code == 409
    assert seen["deleted_when_checked"] is True
    assert client.get(f"/api/documents/{part}", headers=H(world.admin)).status_code == 200  # rolled back
    lib = client.post(f"/api/orgs/{world.org}/projects", json={"name": "Lib"}, headers=H(world.admin)).json()["id"]
    bolt = client.post(f"/api/projects/{lib}/documents", json={"name": "Bolt", "content": {"features": []}},
                       headers=H(world.admin)).json()["id"]
    a = Live(client, asm, world.admin)
    assert a.change([{"op": "item.add", "in": "components", "after": "c1", "item": comp("c2", bolt, 0)}], 0)["seq"] == 1

    original_used_outside = A.used_outside

    async def watching_used_outside(db, project_id):
        seen["project_parts_deleted_when_checked"] = (await db.get(Document, bolt)).deleted_at is not None
        return await original_used_outside(db, project_id)
    monkeypatch.setattr(A, "used_outside", watching_used_outside)
    assert client.delete(f"/api/projects/{lib}", headers=H(world.admin)).status_code == 409
    assert seen["project_parts_deleted_when_checked"] is True
    assert client.get(f"/api/documents/{bolt}", headers=H(world.admin)).status_code == 200  # rolled back
    del O
