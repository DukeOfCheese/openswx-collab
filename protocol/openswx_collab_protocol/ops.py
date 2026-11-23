"""Document operations: how a change to an openswx document is described, compared and applied.

A document is the JSON dict openswx saves (`PartDocument.to_dict()`): top-level keys plus an ordered
`features` list of `{"id", "name", "type", "params", "suppressed"}`. A change is a list of operations
that address features by id, never by position, so two changes to different features commute.

    {"op": "feature.add",    "feature": {...}, "after": "<id>" | None}
    {"op": "feature.remove", "id": "<id>"}
    {"op": "feature.update", "id": "<id>", "set": [[path, value], ...], "unset": [path, ...]}
    {"op": "feature.order",  "ids": ["<id>", ...]}            # the complete new order
    {"op": "doc.patch",      "set": [[path, value], ...], "unset": [path, ...]}

A path is a list of dict keys. Dicts are compared key by key; lists and scalars are replaced whole.

Assemblies have two more id-keyed lists, `components` and `mates`. They work exactly like features, with
the list named in the operation:

    {"op": "item.add",    "in": "components", "item": {"id": ...}, "after": "<id>" | None}
    {"op": "item.remove", "in": "mates", "id": "<id>"}
    {"op": "item.update", "in": "components", "id": "<id>", "set": [...], "unset": [...]}
    {"op": "item.order",  "in": "components", "ids": [...]}

Used by both the server (which has no geometry kernel) and the openswx client.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Callable

# Per-user view state: stays on each machine, never sent.
LOCAL_ONLY = frozenset({"rollback", "active_config", "display_state", "journal", "name"})
# Owned by the server (it hands out feature ids and names): clients never send changes to these.
SERVER_OWNED = frozenset({"next_id", "counters"})
UNSYNCED = LOCAL_ONLY | SERVER_OWNED

STRUCTURE = "structure"  # lock target for changes to the shape of the feature tree
ORDER = "order"          # change target of feature.order
ITEM_LISTS = ("components", "mates")       # id-keyed lists besides features (assemblies)
LISTS = ("features",) + ITEM_LISTS


def target_of(coll: str, item_id: Any) -> str:
    """'f:<id>' for a feature, 'i:<list>:<id>' for a component or mate."""
    return f"f:{item_id}" if coll == "features" else f"i:{coll}:{item_id}"


def order_target(coll: str) -> str:
    return ORDER if coll == "features" else f"{ORDER}:{coll}"


class OpError(ValueError):
    """An operation that cannot be applied to the document it was sent for."""


MAX_DEPTH = 64   # how deeply nested a document may be (real openswx documents stay far below)
MAX_NAME = 200   # longest feature id, name or type


def same(a: Any, b: Any) -> bool:
    """Equal as JSON. Python calls 1, 1.0 and True equal; a document that changes one into another has
    changed (its fingerprint has), so the change must travel."""
    if a is b:  # (the server shares untouched features between versions: most comparisons end here)
        return True
    if a != b:
        return False
    if isinstance(a, (dict, list)):
        return json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
    if isinstance(a, float) and isinstance(b, float):
        return math.copysign(1.0, a) == math.copysign(1.0, b)  # (0.0 and -0.0 are written differently)
    return type(a) is type(b)


def check_value(v: Any, room: int = MAX_DEPTH) -> None:
    """Refuse what cannot live in a document: nesting deeper than `room` levels, and numbers that are not
    JSON (NaN, Infinity: every reader of the document would choke on them)."""
    if room < 0:
        raise OpError("the value is nested too deeply")
    if isinstance(v, float):
        if not math.isfinite(v):
            raise OpError("NaN and Infinity cannot be stored in a document")
    elif isinstance(v, dict):
        for k, x in v.items():
            if not isinstance(k, str):
                raise OpError("object keys must be text")
            check_value(x, room - 1)
    elif isinstance(v, list):
        for x in v:
            check_value(x, room - 1)


# ------------------------------------------------------------------ dict diff / patch
def dict_diff(a: dict, b: dict) -> tuple[list, list]:
    """What turns dict `a` into dict `b`: ([[path, value], ...], [path, ...])."""
    sets: list = []
    unsets: list = []

    def walk(x: dict, y: dict, path: list) -> None:
        for k in x:
            if k not in y:
                unsets.append(path + [k])
        for k, v in y.items():
            if k not in x:
                sets.append([path + [k], v])
            elif not same(x[k], v):
                if isinstance(x[k], dict) and isinstance(v, dict):
                    walk(x[k], v, path + [k])
                else:
                    sets.append([path + [k], v])

    walk(a, b, [])
    return sets, unsets


def _parent(d: dict, path: list) -> dict:
    if not isinstance(path, list) or not path or len(path) > MAX_DEPTH or not all(isinstance(k, str) for k in path):
        raise OpError("bad path")
    cur: Any = d
    for k in path[:-1]:
        cur = cur.get(k) if isinstance(cur, dict) else None
        if not isinstance(cur, dict):
            raise OpError(f"nothing to change at {'/'.join(path)}")
    return cur


def dict_patch(d: dict, sets: list, unsets: list) -> None:
    """Apply a dict_diff result to `d`, in place."""
    for path in unsets or []:
        _parent(d, path).pop(path[-1], None)
    for item in sets or []:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise OpError("bad set entry")
        path, value = item
        parent = _parent(d, path)
        check_value(value, MAX_DEPTH - len(path))
        parent[path[-1]] = _copy(value)


def _copy(v: Any) -> Any:
    return json.loads(json.dumps(v)) if isinstance(v, (dict, list)) else v


# ------------------------------------------------------------------ diff
def _shared_top(doc: dict) -> dict:
    return {k: v for k, v in doc.items() if k not in LISTS and k not in UNSYNCED}


def _items(doc: dict, coll: str) -> list[dict]:
    items = doc.get(coll)
    if items is None:
        return []
    if not isinstance(items, list) or not all(isinstance(x, dict) and isinstance(x.get("id"), str) for x in items):
        raise OpError(f"{coll} must be a list of objects that each have an id")
    return items


def _op(coll: str, action: str, **kw) -> dict:
    if coll == "features":
        if "item" in kw:
            kw["feature"] = kw.pop("item")
        return {"op": f"feature.{action}", **kw}
    return {"op": f"item.{action}", "in": coll, **kw}


def diff(before: dict, after: dict) -> list[dict]:
    """The operations that turn document `before` into `after` (local-only and server-owned keys ignored)."""
    ops: list[dict] = []
    for coll in LISTS:
        b_list, a_list = _items(before, coll), _items(after, coll)
        b = {f["id"]: f for f in b_list}
        a = {f["id"]: f for f in a_list}
        a_ids = [f["id"] for f in a_list]
        for fid in b:
            if fid not in a:
                ops.append(_op(coll, "remove", id=fid))
        for n, fid in enumerate(a_ids):
            if fid not in b:
                ops.append(_op(coll, "add", item=_copy(a[fid]), after=a_ids[n - 1] if n else None))
            elif not same(a[fid], b[fid]):
                sets, unsets = dict_diff(b[fid], a[fid])
                ops.append(_op(coll, "update", id=fid, set=_copy(sets), unset=unsets))
        # removes and adds alone reproduce the order unless items that stayed changed places
        if [f["id"] for f in b_list if f["id"] in a] != [i for i in a_ids if i in b]:
            ops.append(_op(coll, "order", ids=a_ids))
    sets, unsets = dict_diff(_shared_top(before), _shared_top(after))
    if sets or unsets:
        ops.append({"op": "doc.patch", "set": _copy(sets), "unset": unsets})
    return ops


# ------------------------------------------------------------------ apply
def _check_doc_path(path: Any) -> None:
    if not isinstance(path, list) or not path or path[0] in LISTS or path[0] in UNSYNCED:
        raise OpError(f"doc.patch may not change {path[0] if isinstance(path, list) and path else path!r}")


_ACTION_KEYS = {"add": ("after",), "remove": ("id",), "update": ("id", "set", "unset"), "order": ("ids",)}


def parse(op: Any) -> tuple[str, str]:
    """(action, list) of an operation: ('update', 'features'), ('add', 'components'), ('patch', '')."""
    if not isinstance(op, dict) or not isinstance(op.get("op"), str):
        raise OpError("an operation must be an object with an op")
    kind = op["op"]
    if kind == "doc.patch":
        return "patch", ""
    family, _, action = kind.partition(".")
    if action in _ACTION_KEYS:
        if family == "feature":
            return action, "features"
        if family == "item" and op.get("in") in ITEM_LISTS:
            return action, op["in"]
    raise OpError(f"unknown operation {kind!r}")


def _payload(op: dict, coll: str) -> Any:
    return op.get("feature") if coll == "features" else op.get("item")


def clean(ops: Any) -> list[dict]:
    """The operations with nothing in them but what an operation is made of: anything else a sender put
    there (and everything that is not valid JSON data, like NaN) is refused or dropped before the change is
    stored and sent on to others."""
    if not isinstance(ops, list):
        raise OpError("a change is a list of operations")
    out = []
    for op in ops:
        action, coll = parse(op)
        if action == "patch":
            keys: tuple = ("set", "unset")
        else:
            keys = _ACTION_KEYS[action] + ((("feature",) if coll == "features" else ("item",)) if action == "add" else ())
        kept = {"op": op["op"], **({"in": coll} if coll in ITEM_LISTS else {}), **{k: op[k] for k in keys if k in op}}
        check_value(kept, MAX_DEPTH + 4)
        if "after" in kept and kept["after"] is not None and not isinstance(kept["after"], str):
            raise OpError("after must be an id")
        if "id" in kept and not isinstance(kept["id"], str):
            raise OpError("id must be text")
        out.append(kept)
    return out


def check_feature(f: Any) -> None:
    """A feature must keep a usable id, name and type whatever an operation did to it."""
    if not isinstance(f, dict):
        raise OpError("a feature must be an object")
    for k in ("id", "name", "type"):
        v = f.get(k)
        if not isinstance(v, str) or not v or len(v) > MAX_NAME:
            raise OpError(f"a feature needs a {k} (text, at most {MAX_NAME} characters)")
    check_value(f, MAX_DEPTH - 2)


def check_item(coll: str, x: Any) -> None:
    if coll == "features":
        return check_feature(x)
    if not isinstance(x, dict) or not isinstance(x.get("id"), str) or not 0 < len(x["id"]) <= MAX_NAME:
        raise OpError(f"every entry of {coll} needs an id (text, at most {MAX_NAME} characters)")
    check_value(x, MAX_DEPTH - 2)


def apply(doc: dict, ops: list[dict]) -> dict:
    """Apply operations to `doc` in place and return it. Raises OpError and may leave `doc` half-changed:
    apply to a copy when the result must be all-or-nothing.

    Feature names must be unique when the whole change has been applied, not after each operation: a
    change may swap two names, or delete a feature and give its name to another."""
    renamed = False
    for op in ops:
        action, coll = parse(op)
        if action == "patch":
            for path in list(op.get("unset") or []):
                _check_doc_path(path)
            for s in op.get("set") or []:
                _check_doc_path(s[0] if isinstance(s, list) and s else None)
            dict_patch(doc, op.get("set") or [], op.get("unset") or [])
            continue
        items = doc.setdefault(coll, [])
        if not isinstance(items, list):
            raise OpError(f"{coll} is not a list")
        renamed = renamed or (coll == "features" and action in ("add", "update"))
        if action == "add":
            x = _payload(op, coll)
            check_item(coll, x)
            if any(i["id"] == x["id"] for i in items):
                raise OpError(f"an entry with id {x['id']} already exists in {coll}")
            after = op.get("after")
            if after is None:
                at = 0
            else:
                at = next((n for n, i in enumerate(items) if i["id"] == after), -1) + 1
                if at == 0:
                    raise OpError(f"{after} (to insert after) no longer exists in {coll}")
            items.insert(at, _copy(x))
        elif action == "remove":
            del items[_index(items, op.get("id"), coll)]
        elif action == "update":
            n = _index(items, op.get("id"), coll)
            dict_patch(items[n], op.get("set") or [], op.get("unset") or [])
            check_item(coll, items[n])  # (checked as it ends up: whatever the patch did to it)
            if items[n]["id"] != op.get("id"):
                raise OpError("an id cannot change")
        else:  # order
            ids = op.get("ids")
            cur = [i["id"] for i in items]
            if not isinstance(ids, list) or len(ids) != len(cur) or not all(isinstance(i, str) for i in ids) \
                    or set(ids) != set(cur):
                raise OpError(f"an order must list every entry of {coll} exactly once")
            rank = {i: n for n, i in enumerate(ids)}
            items.sort(key=lambda i: rank[i["id"]])
    if renamed:
        seen: set[str] = set()
        for x in doc.get("features") or []:
            if x["name"] in seen:
                raise OpError(f"a feature named {x['name']} already exists")
            seen.add(x["name"])
    return doc


def _index(items: list[dict], item_id: Any, coll: str = "features") -> int:
    for n, x in enumerate(items):
        if x["id"] == item_id:
            return n
    raise OpError(f"{'feature' if coll == 'features' else coll[:-1]} {item_id} no longer exists")


# ------------------------------------------------------------------ what a change touches
def targets(ops: list[dict]) -> set[str]:
    """What a change touches, as strings two changes can be compared by: 'f:<feature id>',
    'i:<list>:<id>', 'k:<top-level key>', 'order', 'order:<list>'. Two changes with no target in common
    do not conflict."""
    out: set[str] = set()
    for op in ops:
        action, coll = parse(op)
        if action == "patch":
            for path in list(op.get("unset") or []) + [s[0] for s in op.get("set") or []]:
                out.add(f"k:{path[0]}")
        elif action == "add":
            out.add(target_of(coll, _payload(op, coll)["id"]))
        elif action == "order":
            out.add(order_target(coll))
        else:
            out.add(target_of(coll, op.get("id")))
    return out


def added_targets(ops: list[dict]) -> set[str]:
    """The targets of everything a change adds."""
    return {target_of(coll, _payload(op, coll)["id"]) for op in ops for action, coll in [parse(op)] if action == "add"}


def added_ids(ops: list[dict]) -> set[str]:
    """Ids of the features a change adds."""
    return {op["feature"]["id"] for op in ops if op.get("op") == "feature.add"}


def is_structural(ops: list[dict]) -> bool:
    """Does the change alter the shape of a tree (so the structure lock applies)?"""
    return any(parse(op)[0] in ("remove", "order") for op in ops)


def fingerprint(doc: dict) -> str:
    """A hash of the shared part of a document: equal on every client that is in sync."""
    shared = {k: v for k, v in doc.items() if k not in UNSYNCED}
    return hashlib.sha256(json.dumps(shared, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


# ------------------------------------------------------------------ ids and names
def split_number(text: str) -> tuple[str, int] | None:
    """'Boss-Extrude12' -> ('Boss-Extrude', 12); None when there is no trailing number (of at most 9 digits)."""
    stem = text.rstrip("0123456789")
    digits = text[len(stem):]
    return (stem, int(digits)) if 0 < len(digits) <= 9 else None


def absorb_ids(state: dict, feats: list[dict] | None = None) -> None:
    """Move `next_id` and `counters` past every feature id and name in use (all features, or just `feats`).
    The server does this for every accepted change; a client does it after applying changes from others, so
    that what it creates next does not reuse an id it has just been sent."""
    try:
        nid = int(state.get("next_id") or 1)
    except (TypeError, ValueError):
        nid = 1
    counters = state.get("counters")
    if not isinstance(counters, dict):
        counters = state["counters"] = {}
    for f in (state.get("features") or []) if feats is None else feats:
        got = split_number(str(f.get("id", "")))
        if got and got[0] == "f":
            nid = max(nid, got[1] + 1)
        got = split_number(str(f.get("name", "")))
        if got and got[0]:
            have = counters.get(got[0], 0)
            counters[got[0]] = max(have if isinstance(have, int) else 0, got[1])
    state["next_id"] = nid


# ------------------------------------------------------------------ large strings (imported bodies)
BLOB_MIN = 16384  # strings at least this long travel as separately uploaded blobs


def externalize(obj: Any, sink: dict[str, bytes], min_len: int = BLOB_MIN) -> Any:
    """A copy of `obj` with every long string replaced by {"$blob": sha256, "len": n}; the bytes go to `sink`."""
    if isinstance(obj, str) and len(obj) >= min_len:
        data = obj.encode("utf-8")
        sha = hashlib.sha256(data).hexdigest()
        sink[sha] = data
        return {"$blob": sha, "len": len(obj)}
    if isinstance(obj, dict):
        return {k: externalize(v, sink, min_len) for k, v in obj.items()}
    if isinstance(obj, list):
        return [externalize(v, sink, min_len) for v in obj]
    return obj


def blob_refs(obj: Any) -> set[str]:
    out: set[str] = set()
    if isinstance(obj, dict):
        if isinstance(obj.get("$blob"), str) and set(obj) <= {"$blob", "len"}:
            out.add(obj["$blob"])
        else:
            for v in obj.values():
                out |= blob_refs(v)
    elif isinstance(obj, list):
        for v in obj:
            out |= blob_refs(v)
    return out


def internalize(obj: Any, fetch: Callable[[str], bytes]) -> Any:
    """The reverse of externalize: `fetch(sha256)` returns the bytes of a blob."""
    if isinstance(obj, dict):
        if isinstance(obj.get("$blob"), str) and set(obj) <= {"$blob", "len"}:
            return fetch(obj["$blob"]).decode("utf-8")
        return {k: internalize(v, fetch) for k, v in obj.items()}
    if isinstance(obj, list):
        return [internalize(v, fetch) for v in obj]
    return obj
