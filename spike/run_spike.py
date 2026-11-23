"""THROWAWAY SPIKE (Stage 0). The relay: two openswx processes, edits alternate between them, only ops cross.

Usage:  <openswx venv python> spike/run_spike.py
Passes when, after every step, both peers hold the same document and the same rebuilt geometry
(volume, face/edge counts and persistent face/edge names).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OPENSWX = Path(os.environ.get("OPENSWX_REPO", HERE.parent.parent / "openswx"))


class Peer:
    def __init__(self, label: str, seed: str):
        env = dict(os.environ, PYTHONHASHSEED=seed, PYTHONPATH=str(OPENSWX))
        self.label = label
        self.p = subprocess.Popen([sys.executable, str(HERE / "peer.py")], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.DEVNULL, text=True, env=env, cwd=str(HERE))
        assert json.loads(self.p.stdout.readline())["ready"]

    def ask(self, req: dict) -> dict:
        self.p.stdin.write(json.dumps(req) + "\n")
        self.p.stdin.flush()
        reply = json.loads(self.p.stdout.readline())
        if "error" in reply:
            raise RuntimeError(f"{self.label}: {reply['error']}")
        return reply

    def close(self):
        self.p.stdin.close()
        self.p.wait(timeout=30)


COMPARE = ("doc", "names", "faces", "edges", "volume", "features", "errors")
failures: list[str] = []
wire_bytes = 0


def step(editor: Peer, other: Peer, command: str, args: dict, note: str = "") -> dict:
    """Run a command on `editor`, relay only its ops to `other`, and compare the two."""
    global wire_bytes
    r = editor.ask({"do": command, "args": args})
    wire = json.dumps(r["ops"])  # what would cross the network
    wire_bytes += len(wire)
    o = other.ask({"apply": json.loads(wire)})
    same = all(r["digest"][k] == o["digest"][k] for k in COMPARE)
    kinds = ",".join(op["op"] + (":" + op["key"] if op["op"] == "doc.set" else "") for op in r["ops"])
    print(f"{'ok  ' if same else 'DIFF'} {editor.label} {command:<18} vol={r['digest']['volume']:<12} "
          f"faces={r['digest']['faces']:<3} ops=[{kinds}] {note}")
    if not same:
        failures.append(command)
        for k in COMPARE:
            if r["digest"][k] != o["digest"][k]:
                print(f"       {k}: {editor.label}={r['digest'][k]!r}  {other.label}={o['digest'][k]!r}")
    return r


def edge_name(peer: Peer, pick) -> str:
    edges = peer.ask({"query": "get_topology"})["result"]["edges"]
    e = pick(edges)
    return e.get("name") or e["id"]


def top_face(peer: Peer) -> str:
    faces = peer.ask({"query": "get_topology"})["result"]["faces"]
    f = max((f for f in faces if f["normal"][1] > 0.99), key=lambda f: f["point"][1])
    return f.get("name") or f["id"]


def main() -> int:
    a, b = Peer("A", "1"), Peer("B", "4242")  # different hash seeds: set/dict order must not leak into geometry
    try:
        sk = step(a, b, "create_sketch", {"plane": "Top"})["result"]["sketch"]
        step(a, b, "sketch_rectangle", {"sketch": sk, "corner1": [-50, -30], "corner2": [50, 30]})
        ext = step(a, b, "extrude", {"sketch": sk, "depth": 20})["result"]["feature"]

        # B picks an edge from ITS OWN rebuilt copy, by persistent name
        e = edge_name(b, lambda es: max(es, key=lambda x: x.get("length", 0)))
        fil = step(b, a, "fillet", {"radius": 3, "edges": [e]}, f"(edge {e})")["result"]["feature"]

        step(a, b, "edit_feature", {"name": ext, "params": {"depth": 32}}, "(upstream edit under B's fillet)")

        f = top_face(b)
        sk2 = step(b, a, "create_sketch", {"plane": f}, f"(on {f})")["result"]["sketch"]
        step(b, a, "sketch_circle", {"sketch": sk2, "center": [10, 5], "radius": 8})
        cut = step(b, a, "cut_extrude", {"sketch": sk2, "depth": 10})["result"]["feature"]

        step(a, b, "rename_feature", {"name": fil, "new_name": "EdgeRound"})
        step(a, b, "suppress_feature", {"name": cut, "suppressed": True})
        step(b, a, "suppress_feature", {"name": cut, "suppressed": False})
        step(b, a, "reorder_feature", {"name": "EdgeRound", "after": cut}, "(fillet moved after the cut)")
        step(a, b, "delete_feature", {"name": "EdgeRound"})
        step(b, a, "undo", {}, "(whole-snapshot undo of B's own state)")

        # Concurrent creation from the same base, no server handing out ids: the collision the design predicts.
        ra = a.ask({"do": "create_sketch", "args": {"plane": "Front"}})
        rb = b.ask({"do": "create_sketch", "args": {"plane": "Right"}})
        ida = [op["feature"]["id"] + "/" + op["feature"]["name"] for op in ra["ops"] if op["op"] == "feature.add"]
        idb = [op["feature"]["id"] + "/" + op["feature"]["name"] for op in rb["ops"] if op["op"] == "feature.add"]
        try:
            b.ask({"apply": ra["ops"]})
            collided = False
        except RuntimeError as exc:
            collided = "collision" in str(exc)
        print(f"\nconcurrent create without id reservation: A made {ida}, B made {idb} -> "
              f"{'collision detected (expected)' if collided else 'NO collision detected'}")
    finally:
        a.close(); b.close()
    print(f"\nops sent: {wire_bytes} bytes in total")
    print("RESULT:", "PASS - peers identical after every step" if not failures else f"FAIL at {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
