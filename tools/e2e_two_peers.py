"""End-to-end check: a real server process, two real openswx processes, real geometry.

    .venv/Scripts/python tools/e2e_two_peers.py [path to the openswx checkout]

Starts the collaboration server on a spare port with a temporary database, signs two people up, turns a
part into a cloud document and has both edit it: in turn, at the same moment on different features, and at
the same moment on the same feature. Passes when both openswx sessions and the server agree after every
step, and the conflict is refused for exactly one of them.

Needs an openswx checkout with its virtual environment (default: ../openswx). The peers are
tools/e2e_peer.py, a minimal stand-in for the openswx client.

To run it against a server that is already up (a Docker deployment, say) instead of starting one, set
OSWX_E2E_SERVER=http://host:port. That server must be empty: the check creates its first account.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

from openswx_collab_protocol.ops import fingerprint

ROOT = Path(__file__).resolve().parent.parent
OPENSWX = Path(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("OPENSWX_REPO", ROOT.parent / "openswx")).resolve()
OPENSWX_PY = OPENSWX / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
SAME = ("doc", "names", "faces", "volume", "features")
failures: list[str] = []


def check(ok: bool, what: str) -> None:
    print(("ok   " if ok else "FAIL ") + what, flush=True)
    if not ok:
        failures.append(what)


class Peer:
    def __init__(self, label: str, seed: str):
        env = dict(os.environ, PYTHONHASHSEED=seed,
                   PYTHONPATH=os.pathsep.join([str(OPENSWX), str(ROOT / "protocol")]))
        self.label = label
        self.err = tempfile.TemporaryFile("w+")
        self.p = subprocess.Popen([str(OPENSWX_PY), str(ROOT / "tools" / "e2e_peer.py")], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=self.err, text=True, env=env)
        if not self.p.stdout.readline():
            raise RuntimeError(f"peer {label} did not start:\n{self.stderr()}")

    def stderr(self) -> str:
        self.err.seek(0)
        return self.err.read()[-3000:]

    def send(self, **req) -> None:
        self.p.stdin.write(json.dumps(req) + "\n")
        self.p.stdin.flush()

    def read(self) -> dict:
        line = self.p.stdout.readline()
        out = json.loads(line) if line else {"error": "the peer process died"}
        if "error" in out:
            raise RuntimeError(f"{self.label}: {out['error']}\n{self.stderr()}")
        return out

    def ask(self, **req) -> dict:
        self.send(**req)
        return self.read()

    def close(self) -> None:
        try:
            self.p.stdin.close()
            self.p.wait(timeout=20)
        except Exception:  # noqa: BLE001
            self.p.kill()


def agree(a: dict, b: dict, what: str, server_doc: str | None = None) -> None:
    da, db = a["digest"], b["digest"]
    ok = all(da[k] == db[k] for k in SAME) and da["seq"] == db["seq"] and da["doc"] == da["confirmed"] \
        and db["doc"] == db["confirmed"] and (server_doc is None or server_doc == da["doc"])
    check(ok, f"{what}: seq={da['seq']} volume={da['volume']} faces={da['faces']} features={len(da['features'])}")
    if not ok:
        for k in SAME + ("seq", "confirmed"):
            print(f"       {k}: A={da[k]!r} B={db[k]!r}")
        print(f"       server: {server_doc}")


def scenario(a: Peer, b: Peer, http: httpx.Client, base: str) -> None:
    def H(t):
        return {"Authorization": f"Bearer {t}"}

    ada = http.post("/api/auth/register", json={"email": "ada@example.com", "password": "correct horse",
                                                "name": "Ada"}).json()["token"]
    org = http.post("/api/orgs", json={"name": "Robotics"}, headers=H(ada)).json()["id"]
    project = http.post(f"/api/orgs/{org}/projects", json={"name": "Rover"}, headers=H(ada)).json()["id"]
    inv = http.post(f"/api/orgs/{org}/invites", headers=H(ada), json={
        "email": "bob@example.com", "projects": [{"project_id": project, "role": "editor"}]}).json()["token"]
    bob = http.post("/api/auth/register", json={"email": "bob@example.com", "password": "correct horse",
                                                "name": "Bob", "invite": inv}).json()["token"]

    # Ada models on her own machine, then turns collaboration on: the file becomes a cloud document
    sk = a.ask(do="create_sketch", args={"plane": "Top"})["result"]["sketch"]
    a.ask(do="sketch_rectangle", args={"sketch": sk, "corner1": [-50, -30], "corner2": [50, 30]})
    ext = a.ask(do="extrude", args={"sketch": sk, "depth": 20})["result"]["feature"]
    local = a.ask(content=True)["content"]
    r = http.post(f"/api/projects/{project}/documents", json={"name": "Bracket", "content": local}, headers=H(ada))
    check(r.status_code == 201, f"collaboration turned on for the part (document created, HTTP {r.status_code})")
    doc = r.json()["id"]
    ws = f"{base.replace('http', 'ws')}/api/documents/{doc}/ws"

    def server() -> str:
        return fingerprint(http.get(f"/api/documents/{doc}/content", headers=H(ada)).json()["content"])

    def both(seq: int, what: str) -> tuple[dict, dict]:
        ra, rb = a.ask(until_seq=seq), b.ask(until_seq=seq)
        agree(ra, rb, what, server())
        return ra, rb

    ja, jb = a.ask(join=ws, token=ada), b.ask(join=ws, token=bob)
    check(ja["role"] == "owner" and jb["role"] == "editor" and jb["peers"] == 1, "both joined (owner and editor)")
    agree(ja, jb, "Bob opens the cloud file and rebuilds the same body", server())

    # in turn
    edges = b.ask(query="get_topology")["result"]["edges"]
    e = max(edges, key=lambda x: x.get("length", 0))
    fil = b.ask(do="fillet", args={"radius": 3, "edges": [e.get("name") or e["id"]]})["result"]["feature"]
    both(1, "Bob fillets an edge of Ada's block")
    a.ask(do="edit_feature", args={"name": ext, "params": {"depth": 32}})
    both(2, "Ada changes the extrude under Bob's fillet")
    faces = b.ask(query="get_topology")["result"]["faces"]
    top = max((f for f in faces if f["normal"][1] > 0.99), key=lambda f: f["point"][1])
    sk2 = b.ask(do="create_sketch", args={"plane": top.get("name") or top["id"]})["result"]["sketch"]
    b.ask(do="sketch_circle", args={"sketch": sk2, "center": [10, 5], "radius": 8})
    b.ask(do="cut_extrude", args={"sketch": sk2, "depth": 10})
    both(5, "Bob sketches on the top face and cuts a hole")

    # at the same moment, different features: both go in
    a.send(do="edit_feature", args={"name": ext, "params": {"depth": 40}}, wait=False)
    b.send(do="edit_feature", args={"name": fil, "params": {"radius": 2}}, wait=False)
    a.read(), b.read()
    ra, rb = both(7, "Ada and Bob edit different features at the same moment")
    check(not ra["rejected"] and not rb["rejected"], "neither edit was refused")

    # at the same moment, the same feature: one wins, the other is told who beat them and rolls back
    a.send(do="edit_feature", args={"name": fil, "params": {"radius": 4}}, wait=False)
    b.send(do="edit_feature", args={"name": fil, "params": {"radius": 1}}, wait=False)
    a.read(), b.read()
    ra, rb = both(8, "Ada and Bob edit the same fillet at the same moment")
    rej = ra["rejected"] + rb["rejected"]
    check(len(rej) == 1 and rej[0]["code"] == "stale", f"exactly one was refused as a conflict ({rej[0]['code'] if rej else 'none'}"
          f", beaten by {next(iter(rej[0]['conflicts'].values()))['name'] if rej else '-'})")

    # named version, more work, restore
    v = http.post(f"/api/documents/{doc}/versions", json={"name": "Rev A"}, headers=H(bob))
    check(v.status_code == 201 and v.json()["seq"] == 8, "Bob names the current state 'Rev A'")
    a.ask(do="edit_feature", args={"name": ext, "params": {"depth": 5}})
    a.ask(do="suppress_feature", args={"name": fil, "suppressed": True})
    ra, rb = both(10, "Ada keeps editing (thinner plate, fillet suppressed)")
    r = http.post(f"/api/documents/{doc}/restore", json={"version": "Rev A"}, headers=H(ada))
    check(r.status_code == 200 and r.json() == {"seq": 11}, "Ada restores 'Rev A' (one new change in the history)")
    ra2, rb2 = both(11, "both screens are back at Rev A")
    pinned = fingerprint(http.get(f"/api/documents/{doc}/content", params={"version": "Rev A"}, headers=H(bob)).json()["content"])
    check(pinned == ra2["digest"]["doc"] and ra2["digest"]["volume"] != ra["digest"]["volume"],
          "the restored model is exactly the named version")
    hist = http.get(f"/api/documents/{doc}/history", headers=H(bob)).json()
    check([h["seq"] for h in hist] == list(range(11, 0, -1)) and {h["author"]["name"] for h in hist} == {"Ada", "Bob"},
          f"history lists all 11 changes with their authors ({', '.join(h['command'] for h in reversed(hist))})")


def main() -> int:
    work = tempfile.mkdtemp(prefix="oswx-collab-e2e-")
    external = os.environ.get("OSWX_E2E_SERVER", "").rstrip("/")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    base = external or f"http://127.0.0.1:{port}"
    env = dict(os.environ, OSWX_COLLAB_DATABASE_URL=f"sqlite+aiosqlite:///{Path(work, 'collab.db').as_posix()}",
               OSWX_COLLAB_STORAGE_URL=str(Path(work, "storage")), OSWX_COLLAB_PUBLIC_URL=base)
    log = open(Path(work, "server.log"), "w")
    # (with a server given, a harmless stand-in process keeps the rest of this function the same)
    cmd = [sys.executable, "-c", "import time; time.sleep(3600)"] if external else \
        [sys.executable, "-m", "openswx_collab_server", "serve", "--port", str(port)]
    server = subprocess.Popen(cmd, env=env, stdout=log, stderr=log)
    http = httpx.Client(base_url=base, timeout=30)
    peers: list[Peer] = []
    try:
        for _ in range(150):
            try:
                if http.get("/api/health").status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.1)
        else:
            raise RuntimeError(f"the server did not start; see {Path(work, 'server.log')}")
        peers = [Peer("A", "1"), Peer("B", "4242")]  # (different hash seeds: set order must not reach geometry)
        scenario(peers[0], peers[1], http, base)
    finally:
        for p in peers:
            p.close()
        http.close()
        server.terminate()
        server.wait(timeout=20)
        log.close()
    print("\nRESULT:", "PASS" if not failures else f"FAIL ({len(failures)}): {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
