"""End-to-end check, one collaborator: a real openswx session connected to a real collaboration server.

Run by tools/e2e_two_peers.py with the openswx checkout's Python. Not the openswx client (that is Stage 2):
the smallest thing that speaks the protocol, to prove the server and real geometry agree.

It keeps `confirmed`, a copy of the document as the server has it, next to the live openswx document
(confirmed + changes not yet acknowledged). A rejected change is undone by reloading `confirmed`.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import uuid

os.environ.setdefault("OPENSWX_HOME", tempfile.mkdtemp(prefix="openswx-e2e-"))
for _k in ("OPENSWX_PLUGINS", "OPENSWX_AUTOSAVE", "OPENSWX_LOG", "OPENSWX_CACHE"):
    os.environ.setdefault(_k, "off")

from websockets.sync.client import connect  # noqa: E402

from openswx import session as S  # noqa: E402
from openswx_collab_protocol import ops as O  # noqa: E402

OUT = sys.stdout
sys.stdout = sys.stderr


class Peer:
    def __init__(self):
        self.ws = None
        self.seq = 0
        self.confirmed: dict = {}
        self.pending: dict[str, list] = {}  # change id -> ops, sent and not yet answered
        self.rejected: list[dict] = []
        self.client_id = uuid.uuid4().hex
        self.n = 0

    # -- document
    def local(self) -> dict:
        return json.loads(json.dumps(S.APP.active_doc.to_dict(), default=str))

    def load(self, shared: dict) -> None:
        """Show `shared` (the server's keys) in openswx, keeping this user's own view state."""
        doc = S.APP.active_doc
        mine = {k: v for k, v in doc.to_dict().items() if k in O.LOCAL_ONLY}
        with S.APP.lock:
            doc.load_dict({**json.loads(json.dumps(shared)), **mine})

    def rebuild_view(self) -> None:
        view = json.loads(json.dumps(self.confirmed))
        for ops in self.pending.values():
            O.apply(view, ops)
        self.load(view)

    # -- network
    def join(self, url: str, token: str) -> dict:
        self.ws = connect(url, max_size=64 * 2**20)
        version = S.APP.active_doc.to_dict().get("version")
        self.ws.send(json.dumps({"type": "hello", "token": token, "client_id": self.client_id,
                                 "format_version": version, "last_seq": None}))
        w = json.loads(self.ws.recv(timeout=30))
        if w["type"] != "welcome":
            raise RuntimeError(f"refused: {w}")
        self.seq, self.confirmed = w["seq"], w["content"]
        self.load(self.confirmed)
        return {"role": w["role"], "read_only": w["you"]["read_only"], "peers": len(w["peers"])}

    def handle(self, m: dict) -> None:
        t = m["type"]
        if t == "change":
            O.apply(self.confirmed, m["ops"])
            O.absorb_ids(self.confirmed)  # (what this client creates next must not reuse an id it was just sent)
            self.seq = m["seq"]
            self.rebuild_view()
        elif t == "change.accepted":
            O.apply(self.confirmed, self.pending.pop(m["change_id"]))
            self.seq = m["seq"]
        elif t == "change.rejected":
            self.pending.pop(m["change_id"])
            self.rejected.append({"code": m["code"], "message": m["message"], "conflicts": m["conflicts"]})
            self.rebuild_view()  # (back to what the server has, plus anything else still pending)

    def pump(self, until) -> None:
        while not until():
            self.handle(json.loads(self.ws.recv(timeout=30)))

    # -- actions
    def do(self, command: str, args: dict, wait: bool) -> dict:
        before = self.local()
        result = S.call(command, args, origin="ui")
        ops = O.diff(before, self.local())
        if ops and self.ws is not None:
            self.n += 1
            cid = f"{self.client_id[:6]}-{self.n}"
            self.pending[cid] = ops
            self.ws.send(json.dumps({"type": "change", "change_id": cid, "base_seq": self.seq, "ops": ops,
                                     "command": command}))
            if wait:
                self.pump(lambda: cid not in self.pending)
        return {"result": result, "ops": [o["op"] for o in ops], "bytes": len(json.dumps(ops))}

    def report(self) -> None:
        doc = S.APP.active_doc
        by_name = {f["name"]: f["id"] for f in doc.features}
        errors = {by_name[k]: v["error"] for k, v in getattr(doc, "state", {}).items()
                  if isinstance(v, dict) and v.get("error") and k in by_name}
        self.ws.send(json.dumps({"type": "rebuild.report", "seq": self.seq, "errors": errors}))

    def digest(self) -> dict:
        doc = S.APP.active_doc
        topo = doc.topology()
        names = sorted(str(f.get("name")) for f in topo["faces"]) + sorted(str(e.get("name")) for e in topo["edges"])
        return {"seq": self.seq, "doc": O.fingerprint(self.local()), "confirmed": O.fingerprint(self.confirmed),
                "names": hashlib.sha1("\n".join(names).encode()).hexdigest(), "faces": len(topo["faces"]),
                "volume": round(S.call("get_mass_properties")["volume"], 6) if doc.body is not None else 0.0,
                "features": [f["name"] for f in doc.features], "pending": len(self.pending)}


def main() -> None:
    S.call("new_part", {"name": "E2E"})
    p = Peer()
    OUT.write(json.dumps({"ready": True}) + "\n")
    OUT.flush()
    for line in sys.stdin:
        req = json.loads(line)
        try:
            out: dict = {}
            if "content" in req:
                out["content"] = p.local()
            if "join" in req:
                out.update(p.join(req["join"], req["token"]))
            if "do" in req:
                out.update(p.do(req["do"], req.get("args") or {}, req.get("wait", True)))
            if "query" in req:
                out["result"] = S.call(req["query"], req.get("args") or {})
            if "until_seq" in req:
                p.pump(lambda: p.seq >= req["until_seq"] and not p.pending)
            if req.get("report"):
                p.report()
            if "expect" in req:  # read until a message of this type arrives; return it
                got = {}
                while got.get("type") != req["expect"]:
                    got = json.loads(p.ws.recv(timeout=30))
                    p.handle(got)
                out["message"] = got
            out["rejected"], p.rejected = p.rejected, []
            out["digest"] = p.digest()
        except Exception as exc:  # noqa: BLE001
            import traceback
            traceback.print_exc()
            out = {"error": f"{type(exc).__name__}: {exc}"}
        OUT.write(json.dumps(out, default=str) + "\n")
        OUT.flush()


if __name__ == "__main__":
    main()
