"""THROWAWAY SPIKE (Stage 0). Turn a before/after document dict into id-keyed operations, and apply them.

Not the real protocol: whole-feature and whole-key replacement, no locks, no server-owned ids.
"""
from __future__ import annotations

import copy
import hashlib
import json

# Per-user view state: never sent. `name` is the local document name.
UNSYNCED = {"rollback", "active_config", "display_state", "journal", "name"}


def _order_after(ids: list[str], present: list[str]) -> list[str]:
    """`present` reordered to follow `ids` (ids not in `ids` keep their relative place at the end)."""
    rank = {i: n for n, i in enumerate(ids)}
    return sorted(present, key=lambda i: rank.get(i, len(rank)))


def diff(before: dict, after: dict) -> list[dict]:
    ops: list[dict] = []
    b = {f["id"]: f for f in before.get("features", [])}
    a = {f["id"]: f for f in after.get("features", [])}
    a_ids = [f["id"] for f in after.get("features", [])]
    for fid in b:
        if fid not in a:
            ops.append({"op": "feature.remove", "id": fid})
    for n, fid in enumerate(a_ids):
        if fid not in b:
            ops.append({"op": "feature.add", "feature": a[fid], "after": a_ids[n - 1] if n else None})
        elif a[fid] != b[fid]:
            ops.append({"op": "feature.update", "id": fid, "feature": a[fid]})
    # order: what the receiver would have after removes and adds, against what the sender has
    survivors = [f["id"] for f in before.get("features", []) if f["id"] in a]
    if _order_after(a_ids, survivors) != survivors:
        ops.append({"op": "feature.order", "ids": a_ids})
    for key in sorted(set(before) | set(after)):
        if key == "features" or key in UNSYNCED:
            continue
        if before.get(key) != after.get(key):
            ops.append({"op": "doc.set", "key": key, "value": after.get(key), "absent": key not in after})
    return ops


def apply(doc: dict, ops: list[dict]) -> dict:
    d = copy.deepcopy(doc)
    feats: list[dict] = d.setdefault("features", [])
    for op in ops:
        kind = op["op"]
        if kind == "feature.remove":
            feats[:] = [f for f in feats if f["id"] != op["id"]]
        elif kind == "feature.add":
            if any(f["id"] == op["feature"]["id"] for f in feats):
                raise ValueError(f"id collision: {op['feature']['id']} already exists")
            at = 0 if op["after"] is None else next(n for n, f in enumerate(feats) if f["id"] == op["after"]) + 1
            feats.insert(at, copy.deepcopy(op["feature"]))
        elif kind == "feature.update":
            n = next(n for n, f in enumerate(feats) if f["id"] == op["id"])
            feats[n] = copy.deepcopy(op["feature"])
        elif kind == "feature.order":
            rank = {i: n for n, i in enumerate(op["ids"])}
            feats.sort(key=lambda f: rank[f["id"]])
        elif kind == "doc.set":
            if op.get("absent"):
                d.pop(op["key"], None)
            else:
                d[op["key"]] = copy.deepcopy(op["value"])
        else:
            raise ValueError(f"unknown op {kind!r}")
    return d


def synced_fingerprint(doc: dict) -> str:
    shared = {k: v for k, v in doc.items() if k not in UNSYNCED}
    return hashlib.sha1(json.dumps(shared, sort_keys=True, default=str).encode("utf-8")).hexdigest()
