"""A real openswx app in a load test (started by tools/load_test.py with the openswx checkout's Python).

    load_probe.py SERVER EMAIL PASSWORD DOCUMENT_ID MODE

Signs in with openswx's own collaboration commands, opens the cloud document, then waits for one line on stdin:
{"start": t0, "end": t1} (time.time() values). Between them it sends presence 20 times a second the way the
openswx UI does (send_presence, outside the command lock), times `collab_status` once a second (a command: it
waits behind whatever the app is doing, such as rebuilding for changes from the others) and, unless MODE is
"presence", changes the depth of the extrude every 3 s (a real command: rebuild, change, wait for the server's
answer). Afterwards it waits until everything it sent is answered and checks that its copy of the
document is the server's. Prints one JSON line.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time

os.environ.setdefault("OPENSWX_HOME", tempfile.mkdtemp(prefix="openswx-probe-"))
for _k in ("OPENSWX_PLUGINS", "OPENSWX_AUTOSAVE", "OPENSWX_LOG", "OPENSWX_CACHE"):
    os.environ.setdefault(_k, "off")

import httpx  # noqa: E402

from openswx import session as S  # noqa: E402
from openswx_collab_protocol import ops as O  # noqa: E402

OUT = sys.stdout
sys.stdout = sys.stderr


def pct(xs: list[float]) -> dict:
    if not xs:
        return {"n": 0}
    xs = sorted(xs)

    def q(p):
        return round(1000 * xs[min(len(xs) - 1, int(p / 100 * len(xs)))], 1)
    return {"n": len(xs), "p50": q(50), "p95": q(95), "max": round(1000 * xs[-1], 1)}


def main() -> None:
    server, email, password, doc_id, mode = sys.argv[1:6]
    out: dict = {"in_sync": False}
    try:
        S.call("collab_sign_in", {"server": server, "email": email, "password": password})
        t = time.time()
        S.call("collab_open", {"document_id": doc_id})
        out["open_s"] = round(time.time() - t, 2)
        from openswx.collab import client as C
        from openswx.collab.commands_collab import send_presence
        c = C.CLIENTS[doc_id]
        window = json.loads(sys.stdin.readline())
        t0, t1 = window["start"], window["end"]
        time.sleep(max(0.0, t0 - time.time()))
        pres, status, edits, failed = [], [], [], []

        def presence() -> None:
            k = 0
            while time.time() < t1:
                k += 1
                a = time.time()
                send_presence(pointer={"point": [k % 50, 20.0, 0.0], "face": "Face<f2:top>"},
                              camera={"q": [0, 0, 0, 1], "t": [0, 0, 0], "h": 150.0, "perspective": False})
                pres.append(time.time() - a)
                time.sleep(max(0.0, 0.05 - (time.time() - a)))

        def poll() -> None:
            while time.time() < t1:
                a = time.time()
                S.call("collab_status", {})
                status.append(time.time() - a)
                time.sleep(max(0.0, 1.0 - (time.time() - a)))

        threads = [threading.Thread(target=presence), threading.Thread(target=poll)]
        for th in threads:
            th.start()
        k = 0
        while mode != "presence" and time.time() < t1 - 1:
            k += 1
            a = time.time()
            try:
                S.call("edit_feature", {"name": "Boss-Extrude1", "params": {"depth": 20 + k % 7}})
                edits.append(time.time() - a)
            except Exception as exc:  # noqa: BLE001
                failed.append(f"{type(exc).__name__}: {exc}"[:200])
            time.sleep(max(0.0, 3.0 - (time.time() - a)))
        for th in threads:
            th.join()
        out.update(presence_ms=pct(pres), status_ms=pct(status), edit_ms=pct(edits), edit_failures=failed[:5])
        # in step: everything answered, and the same document as the server's
        token = httpx.post(f"{server}/api/auth/login", json={"email": email, "password": password},
                           timeout=60).json()["token"]
        h = {"Authorization": f"Bearer {token}"}
        end = time.time() + 60
        while time.time() < end:
            head = httpx.get(f"{server}/api/documents/{doc_id}", headers=h, timeout=60).json()["seq"]
            st = c.status()
            if st["up_to_date"] and st["seq"] == head:
                break
            time.sleep(0.5)
        content = httpx.get(f"{server}/api/documents/{doc_id}/content", headers=h, timeout=60).json()["content"]
        st = c.status()
        with c.mu:
            confirmed = O.fingerprint(c.confirmed)
        shown = [(f["id"], f["name"]) for f in c.doc.to_dict()["features"]]
        # (the shown document also holds what openswx computes, such as a sketch's solve status: compare the
        # replicated copy byte for byte, and the features on screen by id and name)
        out.update(seq=st["seq"], head=head, connected=st["connected"], conflicts=len(st["conflicts"]),
                   in_sync=st["up_to_date"] and st["seq"] == head and confirmed == O.fingerprint(content)
                   and shown == [(f["id"], f["name"]) for f in content["features"]],
                   features=len(shown))
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"[:300]
    OUT.write(json.dumps(out) + "\n")
    OUT.flush()
    os._exit(0)  # (the collaboration threads need not wind down)


if __name__ == "__main__":
    main()
