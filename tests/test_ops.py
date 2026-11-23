"""The shared operations: diff, apply, inverse, targets, blobs."""
import copy
import random

import pytest

from openswx_collab_protocol import messages as M
from openswx_collab_protocol.ops import (OpError, apply, blob_refs, dict_diff, dict_patch, diff, externalize,
                                         fingerprint, internalize, is_structural, targets)

from conftest import part


def roundtrip(before, after):
    ops = diff(before, after)
    got = apply(copy.deepcopy(before), ops)
    assert fingerprint(got) == fingerprint(after)
    back = apply(copy.deepcopy(after), diff(after, before))  # the inverse
    assert fingerprint(back) == fingerprint(before)
    return ops


def test_no_change_is_no_ops():
    assert diff(part(), part()) == []


def test_param_edit_sends_only_the_changed_value():
    a, b = part(), part()
    b["features"][1]["params"]["depth"] = 25
    ops = roundtrip(a, b)
    assert ops == [{"op": "feature.update", "id": "f2", "set": [[["params", "depth"], 25]], "unset": []}]
    assert targets(ops) == {"f:f2"} and not is_structural(ops)


def test_sketch_entity_added_does_not_resend_the_sketch():
    a, b = part(), part()
    b["features"][0]["params"]["sketch"]["entities"]["Line1"] = {"type": "line", "p1": "P1", "p2": "P2"}
    b["features"][0]["params"]["sketch"]["points"]["P2"] = [10, 0]
    ops = roundtrip(a, b)
    assert len(ops) == 1 and sorted(p[0][-1] for p in ops[0]["set"]) == ["Line1", "P2"]


def test_add_remove_reorder_and_top_level_keys():
    a, b = part(), part()
    b["features"].insert(1, {"id": "f3", "name": "Sketch2", "type": "sketch", "params": {}, "suppressed": False})
    b["features"].append({"id": "f4", "name": "Fillet1", "type": "fillet", "params": {}, "suppressed": False})
    b["material"] = {"name": "Steel", "density": 7850.0}
    b["variables"]["w"] = 4
    ops = roundtrip(a, b)
    assert [o["op"] for o in ops] == ["feature.add", "feature.add", "doc.patch"]
    assert targets(ops) == {"f:f3", "f:f4", "k:material", "k:variables"}

    c = copy.deepcopy(b)
    c["features"] = [c["features"][i] for i in (0, 3, 1, 2)]
    ops = roundtrip(b, c)
    assert [o["op"] for o in ops] == ["feature.order"] and is_structural(ops)

    d = copy.deepcopy(c)
    del d["features"][1]
    ops = roundtrip(c, d)
    assert ops == [{"op": "feature.remove", "id": "f4"}] and is_structural(ops)


def test_local_and_server_owned_keys_never_travel():
    a, b = part(), part()
    b["rollback"], b["active_config"], b["next_id"], b["counters"] = 1, "Small", 99, {"Sketch": 50}
    b["journal"] = [{"command": "x"}]
    assert diff(a, b) == [] and fingerprint(a) == fingerprint(b)
    for key in ("next_id", "rollback", "features"):
        with pytest.raises(OpError):
            apply(part(), [{"op": "doc.patch", "set": [[[key], 1]], "unset": []}])


def test_apply_refuses_what_does_not_fit():
    bad = [
        [{"op": "feature.add", "after": "f2", "feature": {"id": "f2", "name": "X1", "type": "t"}}],  # id in use
        [{"op": "feature.add", "after": "f2", "feature": {"id": "f9", "name": "Sketch1", "type": "t"}}],  # name in use
        [{"op": "feature.add", "after": "gone", "feature": {"id": "f9", "name": "X1", "type": "t"}}],
        [{"op": "feature.update", "id": "gone", "set": [], "unset": []}],
        [{"op": "feature.update", "id": "f2", "set": [[["id"], "f7"]], "unset": []}],
        [{"op": "feature.update", "id": "f2", "set": [[["name"], "Sketch1"]], "unset": []}],
        [{"op": "feature.remove", "id": "gone"}],
        [{"op": "feature.order", "ids": ["f1"]}],
        [{"op": "feature.update", "id": "f2", "set": [[["params", "nowhere", "x"], 1]], "unset": []}],
        [{"op": "explode"}],
    ]
    for ops in bad:
        with pytest.raises(OpError):
            apply(part(), ops)


def test_random_edits_round_trip():
    rng = random.Random(7)
    doc = part()
    for step in range(300):
        new = copy.deepcopy(doc)
        feats = new["features"]
        roll = rng.random()
        if roll < 0.3 or not feats:
            n = new["next_id"]
            new["next_id"] += 1
            feats.insert(rng.randint(0, len(feats)), {"id": f"f{n}", "name": f"Feat{n}", "type": "x",
                                                      "params": {"a": rng.random()}, "suppressed": False})
        elif roll < 0.45:
            del feats[rng.randrange(len(feats))]
        elif roll < 0.6:
            rng.shuffle(feats)
        elif roll < 0.9:
            f = rng.choice(feats)
            f["params"][rng.choice("abc")] = {"v": rng.random(), "list": [rng.randint(0, 3)]}
            if rng.random() < 0.3:
                f["params"].pop(rng.choice(list(f["params"])), None)
        else:
            new["variables"][rng.choice("xyz")] = rng.random()
        roundtrip(doc, new)
        doc = new


def test_dict_diff_and_patch():
    a = {"x": 1, "deep": {"keep": 1, "drop": 2, "list": [1, 2]}, "gone": True}
    b = {"x": 1, "deep": {"keep": 1, "list": [1, 2, 3], "new": {"k": 1}}}
    sets, unsets = dict_diff(a, b)
    assert sorted(map(tuple, unsets)) == [("deep", "drop"), ("gone",)]
    dict_patch(a, sets, unsets)
    assert a == b


def test_long_strings_become_blobs_and_back():
    body = "A" * 50000
    doc = part()
    doc["features"].append({"id": "f3", "name": "Imported1", "type": "import", "params": {"brep": body}})
    sink = {}
    out = externalize(doc, sink)
    (sha, data), = sink.items()
    assert out["features"][2]["params"]["brep"] == {"$blob": sha, "len": 50000} and data == body.encode()
    assert blob_refs(out) == {sha} and blob_refs(diff(part(), out)) == {sha}
    assert internalize(out, sink.__getitem__) == doc


def test_messages_parse_by_type():
    m = M.client_message.validate_python({"type": "change", "change_id": "c1", "base_seq": 3, "ops": []})
    assert isinstance(m, M.Change)
    assert isinstance(M.client_message.validate_python({"type": "ping"}), M.Ping)
    with pytest.raises(Exception):
        M.client_message.validate_python({"type": "nope"})
    assert isinstance(M.server_message.validate_python({"type": "change.accepted", "change_id": "c", "seq": 1}),
                      M.ChangeAccepted)
