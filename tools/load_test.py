"""Load test: how many people can work live on one part before the server breaks.

    .venv/Scripts/python tools/load_test.py run --backend sqlite --mode edit --steps 2,5,10,25,50,100
    .venv/Scripts/python tools/load_test.py run --backend postgres --mode presence
    .venv/Scripts/python tools/load_test.py report results/*.jsonl > table.md

`run` starts its own server (a temporary SQLite file, or a throwaway PostgreSQL from pixeltable-pgserver, as
tools/test_postgres.py does), makes up accounts (loadNNN@example.com), and then, for each N in --steps, opens a
new document and has N simulated people work in it for --measure seconds. It never connects to a server it did
not start.

Each simulated person speaks the real wire protocol over a WebSocket (with permessage-deflate, as openswx
does), with the message shapes of openswx_collab_protocol:
  * presence: a pointer, selection, tool and camera like the openswx UI's, 20 times a second (--presence-hz);
  * in --mode edit, also an edit every --edit-every seconds (randomised by +-50 %): the first one reserves an id
    and adds a sketch (ids.reserve, change with feature.add); later ones take a lock on that sketch, move its
    rectangle (feature.update, made with ops.diff) and release the lock;
  * ping every second from one person per worker process (its round trip shows how late the event loop is).
--mode presence is people watching and moving pointers only; --mode edit-only edits without presence.

The people are spread over --workers processes so the load generator is not the bottleneck; each worker
reports its own CPU use, which the report shows next to the server's.

With --openswx PATH (an openswx checkout with its .venv) a real openswx app also joins every step
(tools/load_probe.py): it edits a feature of its own every few seconds and sends presence, and at the end its
document must match the server's.

What is measured, per step (latencies are histograms, merged across workers):
  edit fan-out   change sent by one person -> `change` received by each other person (change_id carries the
                 send time; all processes are on one machine and share the clock)
  edit accept    change sent -> `change.accepted` back to the sender (one database write)
  lock           lock.acquire -> lock.result
  presence       presence sent -> received by each other person
  ping           ping -> pong, from an otherwise ordinary connection
  delivery       every person must see every change exactly once and in order (seq) and every other person's
                 presence frames in order with no gaps (a counter in presence.data); rejected changes, error
                 frames and connections that fail or close are counted
  server         CPU (% of one core; the server is one process and one event loop) and RSS, sampled every
                 0.5 s; ERROR lines and tracebacks in its log; database size; the step's rows in the history
                 table and their size

A step "breaks" when any of: p95 edit fan-out > 1 s (presence p95 in presence mode), anything lost or out of
order, a connection failing, an error in the server log, the server above 95 % of a core on average, or its
memory above --max-rss MB. The ramp of a mode stops after the first broken step (--keep-going: run them all).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import multiprocessing as mp
import os
import random
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PASSWORD = "load-test-password"

# The document every step starts from: what openswx sends when collaboration is turned on for a part with one
# sketch (a 100 x 60 mm rectangle on Top) and one extrude (20 mm). Made with openswx; no geometry is stored.
BASE_PART = {
    "format": "openswx-part", "version": 1, "units": "mm", "material": {"name": None, "density": 1000.0},
    "appearance": {"part": None, "features": {}, "faces": []}, "properties": {}, "drawing": {},
    "study": {"fixtures": [], "loads": [], "element_size": None}, "counters": {"Sketch": 1, "Boss-Extrude": 1},
    "next_id": 3, "variables": {}, "configurations": {"Default": {"suppressed": {}, "values": {}}}, "folders": [],
    "design_table": None, "named_views": {}, "pmi": [], "render": {}, "tolerances": {}, "requirements": [],
    "blocks": {}, "display_states": {},
    "features": [
        {"id": "f1", "name": "Sketch1", "type": "sketch", "suppressed": False, "params": {"plane": "Top", "sketch": {
            "points": {"P1": [-50.0, -30.0], "P2": [50.0, -30.0], "P3": [50.0, 30.0], "P4": [-50.0, 30.0]},
            "entities": {"Line5": {"type": "line", "p1": "P1", "p2": "P2", "construction": False},
                         "Line6": {"type": "line", "p1": "P2", "p2": "P3", "construction": False},
                         "Line7": {"type": "line", "p1": "P3", "p2": "P4", "construction": False},
                         "Line8": {"type": "line", "p1": "P4", "p2": "P1", "construction": False}},
            "relations": [{"id": "R9", "type": "horizontal", "refs": ["Line5"]},
                          {"id": "R10", "type": "horizontal", "refs": ["Line7"]},
                          {"id": "R11", "type": "vertical", "refs": ["Line6"]},
                          {"id": "R12", "type": "vertical", "refs": ["Line8"]}],
            "dimensions": [], "counter": 12}}},
        {"id": "f2", "name": "Boss-Extrude1", "type": "extrude", "suppressed": False, "params": {
            "sketch": "Sketch1", "depth": 20, "end_condition": "Blind", "reverse": False, "draft": 0.0,
            "draft_outward": False, "merge": True, "start_offset": 0.0}},
    ],
}


def sketch_feature(fid: str, name: str, i: int, w: float) -> dict:
    """A rectangle sketch on Front, one per person, placed so that nobody's overlaps."""
    x, y = (i % 20) * 200.0, (i // 20) * 200.0 + 100.0
    pts = {"P1": [x, y], "P2": [x + w, y], "P3": [x + w, y + 40.0], "P4": [x, y + 40.0]}
    ents = {f"Line{k + 5}": {"type": "line", "p1": f"P{k + 1}", "p2": f"P{(k + 1) % 4 + 1}", "construction": False}
            for k in range(4)}
    return {"id": fid, "name": name, "type": "sketch", "suppressed": False, "params": {"plane": "Front", "sketch": {
        "points": pts, "entities": ents, "relations": [], "dimensions": [], "counter": 8}}}


# ------------------------------------------------------------------ histograms
# Latencies go into log-spaced buckets (2 % wide, 10 us .. 100 s), so workers can be merged exactly.
_LO, _STEP, _NB = 1e-5, math.log(1.02), 820


def hbin(seconds: float) -> int:
    if seconds <= _LO:
        return 0
    return min(_NB - 1, int(math.log(seconds / _LO) / _STEP))


def hnew() -> list[int]:
    return [0] * _NB


def hmerge(a: list[int], b: list[int]) -> list[int]:
    return [x + y for x, y in zip(a, b)]


def hpct(h: list[int], p: float) -> float | None:
    n = sum(h)
    if not n:
        return None
    want, run = p / 100.0 * n, 0
    for i, c in enumerate(h):
        run += c
        if run >= want:
            return _LO * math.exp((i + 0.5) * _STEP)
    return None


# ------------------------------------------------------------------ one simulated person
class Person:
    def __init__(self, w: Worker, idx: int, token: str):
        self.w, self.idx, self.token = w, idx, token
        self.client_id = uuid.uuid4().hex
        self.me = ""  # the server's name for this connection (<user id>.<client_id>)
        self.ws = None
        self.seq = 0  # last change seen
        self.start_seq = 0
        self.seen: set[int] = set()
        self.max_seen = 0
        self.last_n: dict[str, int] = {}
        self.n = 0
        self.waiting: dict[str, asyncio.Future] = {}
        self.fid: str | None = None
        self.feature: dict | None = None
        self.edits = 0
        self.alive = False

    async def run(self) -> None:
        import websockets
        w = self.w
        try:
            self.ws = await websockets.connect(w.url, max_size=64 * 2**20, open_timeout=60, close_timeout=2,
                                               compression=None if w.args["no_compression"] else "deflate",
                                               ping_interval=None)
            await self.ws.send(json.dumps({"type": "hello", "token": self.token, "client_id": self.client_id,
                                           "format_version": 1, "last_seq": None, "app": "load-test"}))
            welcome = json.loads(await asyncio.wait_for(self.ws.recv(), 60))
            if welcome.get("type") != "welcome":
                raise RuntimeError(f"refused: {welcome.get('code')}")
        except Exception as exc:  # noqa: BLE001
            w.count("connect_failed")
            w.note(f"connect: {type(exc).__name__}: {exc}"[:200])
            return
        self.me = welcome["you"]["client_id"]
        self.seq = self.start_seq = self.max_seen = welcome["seq"]
        self.alive = True
        w.connected += 1
        tasks = [asyncio.create_task(self.reader())]
        if w.args["mode"] in ("edit", "presence"):
            tasks.append(asyncio.create_task(self.presence_loop()))
        if w.args["mode"] in ("edit", "edit-only"):
            tasks.append(asyncio.create_task(self.edit_loop()))
        tasks.append(asyncio.create_task(self.ping_loop()))
        await w.stop.wait()
        for t in tasks[1:]:
            t.cancel()
        await asyncio.sleep(w.args["drain"])  # (what is on its way still arrives and counts)
        tasks[0].cancel()
        self.alive = False
        try:
            await self.ws.close()
        except Exception:  # noqa: BLE001
            pass

    async def send(self, msg: dict) -> None:
        await self.ws.send(json.dumps(msg, separators=(",", ":")))

    async def reader(self) -> None:
        w = self.w
        try:
            async for raw in self.ws:
                self.handle(raw, time.time())
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if not w.stop.is_set():
                w.count("connection_lost")
                w.note(f"lost: {type(exc).__name__}: {exc}"[:200])
            return
        if not w.stop.is_set():  # (the server closed it)
            w.count("connection_lost")
            rc = getattr(self.ws, "close_code", None)
            w.note(f"closed by server: {rc} {getattr(self.ws, 'close_reason', '')}")

    def handle(self, raw: str, t: float) -> None:
        w = self.w
        m = json.loads(raw)
        typ = m.get("type")
        w.received += 1
        if typ == "presence":
            d = m.get("data") or {}
            sender, n, ts = m.get("client_id"), d.get("n"), d.get("ts")
            last = self.last_n.get(sender)
            if last is not None and isinstance(n, int):
                if n <= last:
                    w.count("presence_out_of_order")
                elif n > last + 1:
                    w.count("presence_gaps", n - last - 1)
            if isinstance(n, int):
                self.last_n[sender] = n
            if isinstance(ts, float) and w.in_window(ts):
                w.hist("presence", t - ts)
        elif typ == "change":
            seq = m["seq"]
            if seq <= self.max_seen or seq in self.seen:
                w.count("change_out_of_order")
            self.seen.add(seq)
            self.max_seen = max(self.max_seen, seq)
            self.seq = max(self.seq, seq)
            cid = m.get("change_id") or ""
            if m.get("client_id") == self.me:
                w.count("own_change_echoed")
            try:
                ts = int(cid.split(".", 1)[0]) / 1e9
            except ValueError:
                ts = None
            if ts and w.in_window(ts):
                w.hist("edit_fanout", t - ts)
        elif typ == "change.accepted":
            seq = m["seq"]
            self.seen.add(seq)
            self.max_seen = max(self.max_seen, seq)
            self.seq = max(self.seq, seq)
            self._answer(m["change_id"], m)
        elif typ == "change.rejected":
            w.count(f"rejected_{m.get('code')}")
            self._answer(m["change_id"], m)
        elif typ in ("lock.result", "ids.reserved"):
            self._answer(m.get("request_id") or "", m)
        elif typ == "pong":
            self._answer("ping", m)
        elif typ == "error":
            w.count(f"error_{m.get('code')}")
            self._answer(m.get("request_id") or "", m)

    def _answer(self, key: str, m: dict) -> None:
        f = self.waiting.pop(key, None)
        if f is not None and not f.done():
            f.set_result(m)

    async def ask(self, key: str, msg: dict, timeout: float = 30.0) -> dict | None:
        f = asyncio.get_running_loop().create_future()
        self.waiting[key] = f
        await self.send(msg)
        try:
            return await asyncio.wait_for(f, timeout)
        except TimeoutError:
            self.waiting.pop(key, None)
            self.w.count(f"timeout_{msg['type']}")
            return None

    async def presence_loop(self) -> None:
        w, rnd = self.w, random.Random(self.idx)
        period = 1.0 / w.args["presence_hz"]
        await asyncio.sleep(rnd.random() * period)
        nxt = time.monotonic()
        x, y = rnd.uniform(-50, 50), rnd.uniform(-30, 30)
        while True:
            self.n += 1
            x, y = x + rnd.uniform(-2, 2), y + rnd.uniform(-2, 2)
            ts = time.time()
            data = {"pointer": {"point": [round(x, 3), 20.0, round(y, 3)], "face": "Face<f2:top>"},
                    "selection": ["Face<f2:top>", "Edge<f2:3>"], "tool": "Smart Dimension",
                    "camera": {"q": [-0.27984, 0.36471, 0.11591, 0.88047], "t": [0.0, 10.0, 0.0], "h": 180.0,
                               "perspective": False},
                    "ts": ts, "n": self.n}
            await self.send({"type": "presence", "data": data})
            if w.in_window(ts):
                w.sent_presence += 1
            nxt += period
            delay = nxt - time.monotonic()
            if delay < -1.0:  # (this process fell behind: do not send a burst to catch up)
                nxt, delay = time.monotonic(), 0
            await asyncio.sleep(max(0.0, delay))

    async def edit_loop(self) -> None:
        w, rnd = self.w, random.Random(1000 + self.idx)
        from openswx_collab_protocol import ops as O
        every = w.args["edit_every"]
        await asyncio.sleep(rnd.random() * every)
        while True:
            try:
                await self.edit_once(O, rnd)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                w.count("edit_failed")
                w.note(f"edit: {type(exc).__name__}: {exc}"[:200])
            await asyncio.sleep(every * rnd.uniform(0.5, 1.5))

    def change_id(self) -> str:
        self.edits += 1
        return f"{time.time_ns()}.{self.client_id[:12]}.{self.edits}"

    async def edit_once(self, O, rnd: random.Random) -> None:
        w = self.w
        if self.fid is None:  # the first edit makes this person's sketch
            rid = uuid.uuid4().hex[:12]
            r = await self.ask(rid, {"type": "ids.reserve", "request_id": rid, "features": 1, "names": ["Sketch"]})
            if not r or r.get("type") != "ids.reserved":
                return
            feat = sketch_feature(r["ids"][0], r["names"][0], self.idx, 60.0)
            ops = O.diff({"features": [BASE_PART["features"][1]]}, {"features": [BASE_PART["features"][1], feat]})
            if await self.commit(ops, "create_sketch"):
                self.fid, self.feature = feat["id"], feat
            return
        target = f"f:{self.fid}"
        rid = uuid.uuid4().hex[:12]
        t0 = time.time()
        r = await self.ask(rid, {"type": "lock.acquire", "request_id": rid, "targets": [target]})
        if r is None:
            return
        if w.in_window(t0):
            w.hist("lock", time.time() - t0)
        if target not in r.get("granted", []):
            w.count("lock_denied")
            return
        try:
            new = json.loads(json.dumps(self.feature))
            wd = rnd.uniform(20.0, 120.0)
            pts = new["params"]["sketch"]["points"]
            pts["P2"][0] = pts["P3"][0] = round(pts["P1"][0] + wd, 3)
            ops = O.diff({"features": [self.feature]}, {"features": [new]})
            if await self.commit(ops, "sketch_dimension"):
                self.feature = new
        finally:
            await self.send({"type": "lock.release", "targets": [target]})

    async def commit(self, ops: list, command: str) -> bool:
        w = self.w
        cid = self.change_id()
        t0 = time.time()
        r = await self.ask(cid, {"type": "change", "change_id": cid, "base_seq": self.seq, "ops": ops,
                                 "command": command})
        if r is None:
            return False
        if w.in_window(t0):
            w.sent_edits += 1
            if r.get("type") == "change.accepted":
                w.hist("edit_accept", time.time() - t0)
        return r.get("type") == "change.accepted"

    async def ping_loop(self) -> None:
        w = self.w
        timed = self.idx == w.first  # (one person per worker measures; the rest ping like openswx: every 15 s)
        await asyncio.sleep(random.random())
        while True:
            t0 = time.time()
            r = await self.ask("ping", {"type": "ping"}, timeout=60)
            if timed and r is not None and w.in_window(t0):
                w.hist("ping", time.time() - t0)
            await asyncio.sleep(1.0 if timed else 15.0)


class Worker:
    def __init__(self, args: dict, url: str, people: list[tuple[int, str]]):
        self.args, self.url, self.people = args, url, people
        self.first = people[0][0] if people else -1
        self.t0 = self.t1 = 0.0
        self.stop: asyncio.Event | None = None
        self.counts: dict[str, int] = {}
        self.notes: list[str] = []
        self.hists: dict[str, list[int]] = {}
        self.connected = self.received = self.sent_presence = self.sent_edits = 0

    def in_window(self, ts: float) -> bool:
        return self.t0 <= ts < self.t1

    def count(self, key: str, n: int = 1) -> None:
        self.counts[key] = self.counts.get(key, 0) + n

    def note(self, text: str) -> None:
        if len(self.notes) < 20:
            self.notes.append(text)

    def hist(self, key: str, seconds: float) -> None:
        h = self.hists.get(key)
        if h is None:
            h = self.hists[key] = hnew()
        h[hbin(seconds)] += 1

    async def main(self, ctl_in, ctl_out) -> dict:
        import psutil
        self.stop = asyncio.Event()
        people = [Person(self, i, tok) for i, tok in self.people]
        ramp = self.args["ramp"]
        tasks = []
        for k, p in enumerate(people):
            tasks.append(asyncio.create_task(p.run()))
            await asyncio.sleep(ramp / max(1, len(people)))
        for _ in range(600):
            if self.connected + self.counts.get("connect_failed", 0) >= len(people):
                break
            await asyncio.sleep(0.1)
        ctl_out.put(("ready", self.connected))
        loop = asyncio.get_running_loop()
        self.t0, self.t1 = await loop.run_in_executor(None, ctl_in.get)
        proc = psutil.Process()
        await asyncio.sleep(max(0.0, self.t0 - time.time()))
        cpu0 = proc.cpu_times()
        w0, r0 = time.time(), self.received
        await asyncio.sleep(max(0.0, self.t1 - time.time()))
        cpu1 = proc.cpu_times()
        cpu = 100.0 * ((cpu1.user - cpu0.user) + (cpu1.system - cpu0.system)) / max(1e-6, time.time() - w0)
        received_in_window = self.received - r0
        self.stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        missing = 0
        for p in people:
            if p.me:
                missing += (p.max_seen - p.start_seq) - len([s for s in p.seen if s > p.start_seq])
        return {"counts": self.counts, "notes": self.notes, "hists": self.hists, "connected": self.connected,
                "sent_presence": self.sent_presence, "sent_edits": self.sent_edits, "received": received_in_window,
                "cpu": cpu, "change_gaps": missing,
                "max_seen": [p.max_seen for p in people if p.me]}


def worker_main(args: dict, url: str, people: list, ctl_in, ctl_out) -> None:
    try:
        out = asyncio.run(Worker(args, url, people).main(ctl_in, ctl_out))
    except Exception as exc:  # noqa: BLE001
        out = {"crash": f"{type(exc).__name__}: {exc}"}
    ctl_out.put(("done", out))


# ------------------------------------------------------------------ the server under test
class Server:
    def __init__(self, backend: str, work: Path, profile: bool = False):
        import httpx
        self.backend, self.work = backend, work
        self.pg = None
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.base = f"http://127.0.0.1:{self.port}"
        if backend == "postgres":
            import pixeltable_pgserver as pgserver
            self.pg = pgserver.get_server(str(work / "pg"))
            self.db_url = self.pg.get_uri().replace("postgresql://", "postgresql+asyncpg://", 1)
        else:
            self.db_url = f"sqlite+aiosqlite:///{(work / 'collab.db').as_posix()}"
        env = dict(os.environ, OSWX_COLLAB_DATABASE_URL=self.db_url, OSWX_COLLAB_STORAGE_URL=str(work / "storage"),
                   OSWX_COLLAB_PUBLIC_URL=self.base, OSWX_COLLAB_OPEN_REGISTRATION="false")
        self.log_path = work / "server.log"
        self.log = open(self.log_path, "w")
        cmd = [sys.executable, "-m", "openswx_collab_server", "serve", "--port", str(self.port)]
        if profile:
            cmd = [sys.executable, str(Path(__file__).with_name("load_profile_server.py")), str(work / "profile"),
                   "--port", str(self.port), *(["--no-cprofile"] if profile == "timing" else [])]
        self.p = subprocess.Popen(cmd, env=env, stdout=self.log, stderr=subprocess.STDOUT)
        self.http = httpx.Client(base_url=self.base, timeout=120)
        for _ in range(300):
            try:
                if self.http.get("/api/health").status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.1)
        else:
            raise RuntimeError(f"the server did not start; see {self.log_path}")
        self.log_pos = 0

    def procs(self):
        import psutil
        root = psutil.Process(self.p.pid)
        return [root, *root.children(recursive=True)]

    def new_log_errors(self) -> list[str]:
        with open(self.log_path, encoding="utf-8", errors="replace") as f:
            f.seek(self.log_pos)
            text = f.read()
            self.log_pos = f.tell()
        return [ln for ln in text.splitlines() if re.search(r"\bERROR\b|Traceback|Exception", ln)]

    def db_size(self) -> int:
        if self.backend == "sqlite":
            return sum(p.stat().st_size for p in self.work.glob("collab.db*"))
        out = self.pg.psql("select pg_database_size(current_database());")
        return int(re.findall(r"^\s*(\d+)\s*$", out, re.MULTILINE)[0])

    def history_size(self, doc: str) -> tuple[int, int]:
        """Rows in the history table for one document, and their stored size (ops + inverses, as JSON text)."""
        sql = ("select count(*), coalesce(sum(length(cast(ops as text)) + length(cast(inverse_ops as text))), 0) "
               f"from ops where document_id = '{doc}'")
        if self.backend == "sqlite":
            import sqlite3
            with sqlite3.connect(self.work / "collab.db") as c:
                n, size = c.execute(sql).fetchone()
            return int(n), int(size)
        out = self.pg.psql(sql + ";")
        n, size = re.findall(r"^\s*(\d+)\s*\|\s*(\d+)\s*$", out, re.MULTILINE)[0]
        return int(n), int(size)

    def close(self) -> None:
        try:
            self.p.terminate()
            self.p.wait(30)
        except Exception:  # noqa: BLE001
            self.p.kill()
        self.log.close()
        if self.pg is not None:
            self.pg.cleanup()


def headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def make_accounts(srv: Server, n: int, have: list[str], org: dict) -> None:
    """Invite and register people until there are n (each has editor access to the project)."""
    http = srv.http
    if not have:
        r = http.post("/api/auth/register", json={"email": "load000@example.com", "password": PASSWORD,
                                                  "name": "Load 0"})
        r.raise_for_status()
        admin = r.json()["token"]
        have.append(admin)
        org["id"] = http.post("/api/orgs", json={"name": "Load test"}, headers=headers(admin)).json()["id"]
        org["project"] = http.post(f"/api/orgs/{org['id']}/projects", json={"name": "Load"},
                                   headers=headers(admin)).json()["id"]
    admin = have[0]
    while len(have) < n:
        i = len(have)
        email = f"load{i:03d}@example.com"
        inv = http.post(f"/api/orgs/{org['id']}/invites", headers=headers(admin), json={
            "email": email, "send_email": False, "projects": [{"project_id": org["project"], "role": "editor"}]})
        inv.raise_for_status()
        r = http.post("/api/auth/register", json={"email": email, "password": PASSWORD, "name": f"Load {i}",
                                                  "invite": inv.json()["token"]})
        r.raise_for_status()
        have.append(r.json()["token"])


def sample_server(srv: Server, until: float, out: list) -> None:
    procs = srv.procs()
    for p in procs:
        p.cpu_percent(None)
    while time.time() < until:
        time.sleep(0.5)
        cpu = rss = 0.0
        for p in procs:
            try:
                cpu += p.cpu_percent(None)
                rss += p.memory_info().rss
            except Exception:  # noqa: BLE001
                pass
        out.append((cpu, rss / 2**20))


def run_step(srv: Server, args, tokens: list[str], org: dict, n: int, mode: str) -> dict:
    import threading
    http = srv.http
    admin = tokens[0]
    r = http.post(f"/api/projects/{org['project']}/documents",
                  json={"name": f"{mode}-{n}-{uuid.uuid4().hex[:6]}", "content": BASE_PART}, headers=headers(admin))
    r.raise_for_status()
    doc = r.json()["id"]
    url = f"ws://127.0.0.1:{srv.port}/api/documents/{doc}/ws"
    people = list(enumerate(tokens[1:n + 1], start=1))  # (tokens[0], the owner, is the openswx probe's account)
    workers = max(1, min(args.workers, math.ceil(n / 2)))
    wargs = {"mode": mode, "presence_hz": args.presence_hz, "edit_every": args.edit_every, "drain": args.drain,
             "ramp": min(args.ramp_max, 0.05 * n), "no_compression": args.no_compression}
    ctx = mp.get_context("spawn")
    procs, ins, out = [], [], ctx.Queue()
    for k in range(workers):
        share = people[k::workers]
        q = ctx.Queue()
        p = ctx.Process(target=worker_main, args=(wargs, url, share, q, out), daemon=True)
        p.start()
        procs.append(p)
        ins.append(q)
    probe = None
    if args.openswx:
        probe = start_probe(args, srv, doc, n, mode)
    ready = 0
    for _ in range(workers):
        _, connected = out.get(timeout=900)
        ready += connected
    srv.new_log_errors()  # (only errors from the measured window count)
    t0 = time.time() + 2.0
    t1 = t0 + args.measure
    if probe is not None:
        probe["proc"].stdin.write(json.dumps({"start": t0, "end": t1}) + "\n")
        probe["proc"].stdin.flush()
    for q in ins:
        q.put((t0, t1))
    samples: list = []
    while time.time() < t0:
        time.sleep(0.05)
    th = threading.Thread(target=sample_server, args=(srv, t1, samples))
    th.start()
    th.join()
    results = []
    for _ in range(workers):
        _, res = out.get(timeout=600)
        results.append(res)
    for p in procs:
        p.join(30)
    probe_out = finish_probe(probe) if probe is not None else None
    errors = srv.new_log_errors()
    head = http.get(f"/api/documents/{doc}", headers=headers(admin)).json().get("seq")
    rows, rows_bytes = srv.history_size(doc)
    return summarise(args, srv, mode, n, ready, results, samples, errors, head, rows, rows_bytes, probe_out,
                     workers)


def summarise(args, srv, mode, n, ready, results, samples, errors, head, rows, rows_bytes, probe, workers) -> dict:
    counts: dict[str, int] = {}
    hists: dict[str, list[int]] = {}
    notes, crashes = [], []
    sent_presence = sent_edits = received = gaps = 0
    behind = 0
    wcpu = []
    for r in results:
        if "crash" in r:
            crashes.append(r["crash"])
            continue
        for k, v in r["counts"].items():
            counts[k] = counts.get(k, 0) + v
        for k, h in r["hists"].items():
            hists[k] = hmerge(hists.get(k, hnew()), h)
        notes += r["notes"]
        sent_presence += r["sent_presence"]
        sent_edits += r["sent_edits"]
        received += r["received"]
        gaps += r["change_gaps"]
        behind += sum(1 for m in r["max_seen"] if head is not None and m < head)
        wcpu.append(r["cpu"])
    lat = {}
    for k, h in hists.items():
        lat[k] = {"n": sum(h), **{f"p{p}": round(1000 * hpct(h, p), 2) for p in (50, 95, 99)}}
    cpu = [c for c, _ in samples]
    rss = [m for _, m in samples]
    secs = args.measure
    expected_presence = sent_presence * (n - 1)
    got_presence = lat.get("presence", {}).get("n", 0)
    res = {
        "backend": srv.backend, "mode": mode, "n": n, "connected": ready, "workers": workers,
        "measure_s": secs, "presence_hz": args.presence_hz, "edit_every_s": args.edit_every,
        "compression": not args.no_compression,
        "latency_ms": lat,
        "edits_per_s": round(sent_edits / secs, 2),
        "presence_sent_per_s": round(sent_presence / secs, 1),
        "presence_delivered_ratio": round(got_presence / expected_presence, 4) if expected_presence else None,
        "frames_received_per_s": round(received / secs, 1),
        "server_cpu_pct": {"mean": round(sum(cpu) / len(cpu), 1) if cpu else None, "max": max(cpu) if cpu else None},
        "server_rss_mb": {"mean": round(sum(rss) / len(rss), 1) if rss else None,
                          "max": round(max(rss), 1) if rss else None},
        "worker_cpu_pct_max": round(max(wcpu), 1) if wcpu else None,
        "db_bytes": srv.db_size(), "history_rows": rows, "history_bytes": rows_bytes, "head_seq": head,
        "counts": counts, "change_gaps": gaps, "people_behind_head": behind,
        "server_log_errors": errors[:10], "server_log_error_count": len(errors), "notes": notes[:10],
        "worker_crashes": crashes, "probe": probe,
    }
    res["broken"] = breaks(args, res)
    return res


BAD = ("connect_failed", "connection_lost", "presence_gaps", "presence_out_of_order", "change_out_of_order",
       "own_change_echoed", "edit_failed", "lock_denied")


def breaks(args, r: dict) -> list[str]:
    why = []
    key = "presence" if r["mode"] == "presence" else "edit_fanout"
    p95 = r["latency_ms"].get(key, {}).get("p95")
    if p95 is not None and p95 > 1000:
        why.append(f"{key} p95 {p95:.0f} ms")
    if r["mode"] != "presence" and not r["latency_ms"].get("edit_fanout", {}).get("n") and r["n"] > 1:
        why.append("no edits arrived")
    for k, v in r["counts"].items():
        if v and (k in BAD or k.startswith(("rejected_", "error_", "timeout_"))):
            why.append(f"{k}={v}")
    if r["change_gaps"]:
        why.append(f"changes missed={r['change_gaps']}")
    if r["people_behind_head"]:
        why.append(f"{r['people_behind_head']} people behind the head")
    if r["connected"] < r["n"]:
        why.append(f"only {r['connected']}/{r['n']} connected")
    if r["server_log_error_count"]:
        why.append(f"server log errors={r['server_log_error_count']}")
    if (r["server_cpu_pct"]["mean"] or 0) > 95:
        why.append(f"server CPU {r['server_cpu_pct']['mean']:.0f} %")
    if (r["server_rss_mb"]["max"] or 0) > args.max_rss:
        why.append(f"server RSS {r['server_rss_mb']['max']:.0f} MB")
    if r["worker_crashes"]:
        why.append("load generator crashed")
    pr = r.get("probe") or {}
    if pr and not pr.get("in_sync", False):
        why.append("openswx client out of sync")
    return why


# ------------------------------------------------------------------ a real openswx app (tools/load_probe.py)
def start_probe(args, srv: Server, doc: str, n: int, mode: str) -> dict:
    repo = Path(args.openswx).resolve()
    py = repo / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    home = Path(tempfile.mkdtemp(prefix="oswx-probe-", dir=srv.work))
    env = dict(os.environ, OPENSWX_HOME=str(home), OPENSWX_LOG="off", OPENSWX_AUTOSAVE="off",
               OPENSWX_PLUGINS="off", OPENSWX_CACHE="off", OPENSWX_ISOLATE="off",
               PYTHONPATH=os.pathsep.join([str(repo), str(ROOT / "protocol")]))
    err = open(srv.work / f"probe-{mode}-{n}.log", "w")
    p = subprocess.Popen([str(py), str(Path(__file__).with_name("load_probe.py")), srv.base, "load000@example.com",
                          PASSWORD, doc, mode], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err, text=True,
                         env=env)
    return {"proc": p, "err": err}


def finish_probe(probe: dict) -> dict:
    p = probe["proc"]
    try:
        line = p.stdout.readline()
        p.wait(120)
    except Exception as exc:  # noqa: BLE001
        p.kill()
        return {"in_sync": False, "error": f"probe: {exc}"}
    probe["err"].close()
    try:
        return json.loads(line)
    except ValueError:
        return {"in_sync": False, "error": "the probe died (see probe-*.log)"}


# ------------------------------------------------------------------ commands
def cmd_run(args) -> int:
    work = Path(tempfile.mkdtemp(prefix="oswx-load-"))
    out = Path(args.out or f"load-{args.backend}-{'-'.join(args.mode)}-{time.strftime('%Y%m%d-%H%M%S')}.jsonl")
    steps = [int(x) for x in args.steps.split(",")]
    print(f"server: {args.backend}, work folder {work}, results -> {out}", flush=True)
    srv = Server(args.backend, work, profile=args.profile)
    tokens: list[str] = []
    org: dict = {}
    try:
        for mode in args.mode:
            for n in steps:
                make_accounts(srv, n + 1, tokens, org)
                r = run_step(srv, args, tokens, org, n, mode)
                r["hardware"] = hardware()
                with open(out, "a") as f:
                    f.write(json.dumps(r) + "\n")
                print(line(r), flush=True)
                if r["broken"] and not args.keep_going:
                    break
                time.sleep(args.pause)
    finally:
        srv.close()
        if not args.keep_work:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"kept {work}")
    return 0


def line(r: dict) -> str:
    lat = r["latency_ms"]

    def p(k, q="p95"):
        v = lat.get(k, {}).get(q)
        return "-" if v is None else f"{v:.0f}"
    pr = r.get("probe") or {}
    return (f"{r['backend']:8} {r['mode']:9} N={r['n']:<4} fanout p50/p95/p99={p('edit_fanout', 'p50')}/"
            f"{p('edit_fanout')}/{p('edit_fanout', 'p99')} ms  accept p95={p('edit_accept')}  "
            f"presence p50/p95={p('presence', 'p50')}/{p('presence')}  ping p95={p('ping')}  "
            f"cpu={r['server_cpu_pct']['mean']}% rss={r['server_rss_mb']['max']}MB gen-cpu={r['worker_cpu_pct_max']}%"
            f"  frames/s={r['frames_received_per_s']:.0f}"
            + (f"  openswx: sync={pr.get('in_sync')} edit p95={pr.get('edit_ms', {}).get('p95')}" if pr else "")
            + ("  BROKEN: " + "; ".join(r["broken"]) if r["broken"] else "  ok"))


def hardware() -> dict:
    import platform

    import psutil
    return {"cpu": platform.processor(), "cores": psutil.cpu_count(logical=False),
            "threads": psutil.cpu_count(), "ram_gb": round(psutil.virtual_memory().total / 2**30, 1),
            "os": f"{platform.system()} {platform.release()}", "python": platform.python_version()}


def cmd_report(args) -> int:
    rows = []
    for path in args.files:
        with open(path) as f:
            rows += [json.loads(x) for x in f if x.strip()]

    def g(r, k, q):
        v = r["latency_ms"].get(k, {}).get(q)
        return "-" if v is None else (f"{v:.0f}" if v >= 10 else f"{v:.1f}")
    print("| backend | mode | deflate | N | edit fan-out p50 / p95 / p99 (ms) | accept p95 | presence p50 / p95 / p99 (ms) | "
          "ping p95 | frames out/s | server CPU % | RSS MB | DB MB | history rows / KB | verdict |")
    print("|---|---|---|--:|---|--:|---|--:|--:|--:|--:|--:|---|---|")
    for r in rows:
        verdict = "ok" if not r["broken"] else "**breaks**: " + "; ".join(r["broken"])
        print(f"| {r['backend']} | {r['mode']} | {'yes' if r.get('compression', True) else 'no'} | {r['n']} | {g(r, 'edit_fanout', 'p50')} / {g(r, 'edit_fanout', 'p95')}"
              f" / {g(r, 'edit_fanout', 'p99')} | {g(r, 'edit_accept', 'p95')} | {g(r, 'presence', 'p50')} / "
              f"{g(r, 'presence', 'p95')} / {g(r, 'presence', 'p99')} | {g(r, 'ping', 'p95')} | "
              f"{r['frames_received_per_s']:.0f} | {r['server_cpu_pct']['mean']} | {r['server_rss_mb']['max']} | "
              f"{r['db_bytes'] / 2**20:.1f} | {r['history_rows']} / {r['history_bytes'] / 1024:.0f} | {verdict} |")
    if args.chart:
        chart(rows, args.chart)
    return 0


def chart(rows: list[dict], path: str) -> None:
    """p95 latency against people, one line per backend and mode, log scale, with the 1 s limit drawn in."""
    series: dict[str, list[tuple[int, float]]] = {}
    for r in rows:
        key = "presence" if r["mode"] == "presence" else "edit_fanout"
        v = r["latency_ms"].get(key, {}).get("p95")
        if v is not None:
            label = f"{r['mode']} ({r['backend']}{'' if r.get('compression', True) else ', no deflate'})"
            series.setdefault(label, []).append((r["n"], v))
    W, H, L, B, T, R = 760, 420, 60, 50, 20, 230
    xmax = max(n for pts in series.values() for n, _ in pts)
    lo, hi = 1.0, 20000.0

    def x(n):
        return L + (W - L - R) * math.log(n) / math.log(xmax)

    def y(ms):
        ms = min(max(ms, lo), hi)
        return T + (H - T - B) * (1 - math.log(ms / lo) / math.log(hi / lo))
    colours = ["#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c", "#0891b2", "#4b5563", "#ca8a04"]
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="sans-serif" font-size="12">',
           f'<rect width="{W}" height="{H}" fill="white"/>']
    for ms in (1, 10, 100, 1000, 10000):
        out.append(f'<line x1="{L}" x2="{W - R}" y1="{y(ms):.1f}" y2="{y(ms):.1f}" stroke="#e5e7eb"/>'
                   f'<text x="{L - 6}" y="{y(ms) + 4:.1f}" text-anchor="end">{ms if ms < 1000 else f"{ms // 1000} s"}'
                   f'{" ms" if ms < 1000 else ""}</text>')
    out.append(f'<line x1="{L}" x2="{W - R}" y1="{y(1000):.1f}" y2="{y(1000):.1f}" stroke="#111" '
               f'stroke-dasharray="5,4"/><text x="{W - R - 4}" y="{y(1000) - 5:.1f}" text-anchor="end">limit: '
               f'p95 1 s</text>')
    for n in sorted({n for pts in series.values() for n, _ in pts}):
        out.append(f'<text x="{x(n):.1f}" y="{H - B + 16}" text-anchor="middle">{n}</text>')
    out.append(f'<text x="{(L + W - R) / 2}" y="{H - 10}" text-anchor="middle">people in one part</text>')
    out.append(f'<text x="14" y="{(T + H - B) / 2}" transform="rotate(-90 14 {(T + H - B) / 2})" '
               f'text-anchor="middle">p95 latency to the others</text>')
    for k, (label, pts) in enumerate(sorted(series.items())):
        c = colours[k % len(colours)]
        pts = sorted(pts)
        out.append(f'<polyline fill="none" stroke="{c}" stroke-width="2" points="'
                   + " ".join(f"{x(n):.1f},{y(v):.1f}" for n, v in pts) + '"/>')
        out += [f'<circle cx="{x(n):.1f}" cy="{y(v):.1f}" r="3" fill="{c}"/>' for n, v in pts]
        out.append(f'<rect x="{W - R + 14}" y="{T + 18 * k}" width="12" height="3" fill="{c}"/>'
                   f'<text x="{W - R + 32}" y="{T + 18 * k + 5}">{label}</text>')
    out.append("</svg>")
    Path(path).write_text("\n".join(out), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--backend", choices=("sqlite", "postgres"), default="sqlite")
    r.add_argument("--mode", nargs="+", choices=("edit", "presence", "edit-only"), default=["edit"])
    r.add_argument("--steps", default="2,5,10,25,50,100,200,500")
    r.add_argument("--measure", type=float, default=20.0, help="seconds measured per step")
    r.add_argument("--drain", type=float, default=3.0, help="seconds to keep reading after the window")
    r.add_argument("--presence-hz", type=float, default=20.0)
    r.add_argument("--edit-every", type=float, default=5.0, help="seconds between one person's edits (mean)")
    r.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) // 2), help="load processes")
    r.add_argument("--ramp-max", type=float, default=10.0, help="connections are spread over at most this long")
    r.add_argument("--pause", type=float, default=3.0)
    r.add_argument("--max-rss", type=float, default=2048.0, help="server memory (MB) that counts as exhausted")
    r.add_argument("--no-compression", action="store_true", help="no permessage-deflate (openswx uses it)")
    r.add_argument("--openswx", help="an openswx checkout: a real openswx app joins every step")
    r.add_argument("--profile", nargs="?", const="cprofile", choices=("cprofile", "timing"),
                   help="run the server under tools/load_profile_server.py: cProfile and timings, or timings only")
    r.add_argument("--keep-going", action="store_true")
    r.add_argument("--keep-work", action="store_true")
    r.add_argument("--out")
    rp = sub.add_parser("report")
    rp.add_argument("files", nargs="+")
    rp.add_argument("--chart", help="also draw p95 latency against people as an SVG file")
    args = ap.parse_args()
    return cmd_run(args) if args.cmd == "run" else cmd_report(args)


if __name__ == "__main__":
    sys.exit(main())
