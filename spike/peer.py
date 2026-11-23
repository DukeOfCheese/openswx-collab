"""THROWAWAY SPIKE (Stage 0). One collaborator: a real openswx session in its own process.

Reads one JSON request per line on stdin, writes one JSON reply per line on stdout:
  {"do": command, "args": {...}}   run a command locally; reply carries the ops it produced
  {"apply": [ops]}                 apply ops that came from the other peer, and rebuild
  {"query": command, "args": {}}   run a read-only command and return its result
Every reply carries a digest of the document and its rebuilt geometry.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile

os.environ.setdefault("OPENSWX_HOME", tempfile.mkdtemp(prefix="openswx-spike-"))
for _k in ("OPENSWX_PLUGINS", "OPENSWX_AUTOSAVE", "OPENSWX_LOG", "OPENSWX_CACHE"):
    os.environ.setdefault(_k, "off")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sync  # noqa: E402
from openswx import session as S  # noqa: E402

OUT = sys.stdout
sys.stdout = sys.stderr  # anything openswx prints must not corrupt the reply stream


def digest() -> dict:
    doc = S.APP.active_doc
    topo = doc.topology()
    names = sorted(str(f.get("name")) for f in topo["faces"]) + sorted(str(e.get("name")) for e in topo["edges"])
    vol = S.call("get_mass_properties")["volume"] if doc.body is not None else 0.0
    return {
        "doc": sync.synced_fingerprint(doc.to_dict()),
        "names": hashlib.sha1("\n".join(names).encode()).hexdigest(),
        "faces": len(topo["faces"]), "edges": len(topo["edges"]),
        "unnamed": sum(1 for f in topo["faces"] if not f.get("name")),
        "volume": round(vol, 6),
        "features": [f["name"] for f in doc.features],
        "errors": {k: v["error"] for k, v in getattr(doc, "state", {}).items() if isinstance(v, dict) and v.get("error")},
    }


def handle(req: dict) -> dict:
    if "do" in req:
        before = json.loads(json.dumps(S.APP.active_doc.to_dict(), default=str))
        result = S.call(req["do"], req.get("args") or {}, origin="ui")
        after = json.loads(json.dumps(S.APP.active_doc.to_dict(), default=str))
        return {"result": result, "ops": sync.diff(before, after), "digest": digest()}
    if "apply" in req:
        doc = S.APP.active_doc
        with S.APP.lock:
            local = doc.to_dict()
            merged = sync.apply(json.loads(json.dumps(local, default=str)), req["apply"])
            doc.load_dict(merged)  # (rebuilds)
        return {"digest": digest()}
    if "query" in req:
        return {"result": S.call(req["query"], req.get("args") or {}), "digest": digest()}
    raise ValueError("bad request")


def main() -> None:
    S.call("new_part", {"name": "Spike"})
    OUT.write(json.dumps({"ready": True, "seed": os.environ.get("PYTHONHASHSEED")}) + "\n")
    OUT.flush()
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            reply = handle(json.loads(line))
        except Exception as exc:  # noqa: BLE001 - the driver reports it
            reply = {"error": f"{type(exc).__name__}: {exc}"}
        OUT.write(json.dumps(reply, default=str) + "\n")
        OUT.flush()


if __name__ == "__main__":
    main()
