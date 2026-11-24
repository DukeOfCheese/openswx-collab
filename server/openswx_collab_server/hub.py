"""Open documents ("rooms"): the one place where changes to a document are checked, ordered and stored.

The server keeps the current JSON of every open document in memory and applies each accepted change to
it. It never rebuilds geometry: clients do, and tell the server what failed (rebuild.report).

A change is accepted when
  * the sender may edit,
  * nothing it touches is locked by another client (and, for deletes and reorders, nobody else holds
    the structure lock),
  * nothing it touches was changed by another client after the change's base_seq,
  * ids and names it adds are not reserved by someone else, and
  * it applies cleanly to the server's copy.
It then gets the next sequence number, is written to the edit history together with its inverse, and is
sent to everyone else in the document.

One process holds all rooms: run a single server process per database.
"""
from __future__ import annotations

import asyncio
import copy
import gzip
import json
import logging
import re
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from fastapi import WebSocket
from sqlalchemy import delete, select, update

from openswx_collab_protocol import messages as M
from openswx_collab_protocol.ops import (LISTS, STRUCTURE, OpError, absorb_ids, added_ids, added_targets, apply,
                                         blob_refs, clean, diff, is_structural, parse, targets)

from .access import project_role
from .assemblies import check_refs, ref_rows
from .db import AssemblyRef, Document, Op, Project, Session, Snapshot, User, now
from .security import colour_for, token_hash
from .storage import blob_key, snapshot_key

_TARGET = re.compile(r"^(f:.{1,200}|i:(components|mates):.{1,200}|k:.{1,64}|structure)$")
MAX_LOCKS = 256      # per client
MAX_RESERVED = 256   # ids, and names, a client may hold unused
BLOB_CHECKS = 16     # storage look-ups a change may have in flight at once


def _stem_ok(x: object) -> bool:
    """A name stem ("Boss-Extrude", "3DSketch", "Helix/Spiral"): short text that does not end in a digit."""
    return isinstance(x, str) and 0 < len(x) <= 40 and not x[-1].isdigit() and x.isprintable() and x == x.strip()


class RoomGone(Exception):
    """The room was unloaded while the caller waited for it: get the document's room from the hub again."""


def pack(state: dict) -> bytes:
    return gzip.compress(json.dumps(state, separators=(",", ":")).encode("utf-8"), compresslevel=6)


def unpack(data: bytes) -> dict:
    return json.loads(gzip.decompress(data).decode("utf-8"))


def encode(msg: dict) -> str | None:
    """A message as the text every recipient gets, or None when it is not JSON (NaN, infinity)."""
    try:
        return json.dumps(msg, separators=(",", ":"), allow_nan=False)
    except ValueError:
        return None


@dataclass
class Lock:
    client_id: str
    user_id: str
    name: str
    detached_at: float | None = None  # set when the holder disconnects: the grace period starts

    def holder(self) -> dict:
        return {"client_id": self.client_id, "user_id": self.user_id, "name": self.name,
                "detached": self.detached_at is not None}


class Conn:
    def __init__(self, ws: WebSocket, user: User, client_id: str, role: str, read_only: bool, reason: str | None,
                 colour: str, max_buffer: int = 32 * 2**20):
        self.ws, self.user, self.client_id, self.role = ws, user, client_id, role
        self.max_buffer, self.buffered = max_buffer, 0  # bytes waiting to be written to this socket
        self._sizes: deque[int] = deque()  # the size of each of those frames, oldest first
        self.read_only, self.reason, self.colour = read_only, reason, colour
        self.presence: dict | None = None
        self.draft: dict | None = None
        self.last_seen = time.time()
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self.closed = False
        self._tokens, self._refill = -1.0, time.time()  # (-1: the bucket starts full, see allow)
        self.task = asyncio.create_task(self._writer())

    def peer(self) -> dict:
        return {"client_id": self.client_id, "user_id": self.user.id, "name": self.user.name, "colour": self.colour,
                "read_only": self.read_only, "presence": self.presence, "draft": self.draft}

    def send(self, msg: dict) -> None:
        if self.closed:
            return
        text = encode(msg)
        if text is None:
            # Something that is not JSON got this far. Dropping the frame would leave this client out of step
            # without knowing: closing makes it reconnect and catch up.
            return self.close(4408, "resync")
        self.send_text(text)

    def send_text(self, text: str) -> None:
        """Queue an already encoded message (a broadcast encodes once for everyone)."""
        if self.closed:
            return
        # The oldest frame waiting may be any size (a welcome carries the whole document); what queues up
        # behind it is held to max_buffer. So one big frame is never "too slow" by itself, and the memory a
        # socket can hold is that frame plus max_buffer.
        behind = self.buffered - self._sizes[0] if self._sizes else None
        if behind is not None and behind + len(text) > self.max_buffer:
            return self.close(4408, "too slow")  # (it reconnects and catches up from the history)
        try:
            self.queue.put_nowait(text)
        except asyncio.QueueFull:
            return self.close(4408, "too slow")
        self.buffered += len(text)
        self._sizes.append(len(text))

    def close(self, code: int = 1000, reason: str = "") -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.queue.put_nowait(None)
        except asyncio.QueueFull:
            self.task.cancel()
        asyncio.create_task(self._close(code, reason))

    async def _close(self, code: int, reason: str) -> None:
        try:
            await asyncio.wait_for(self.task, 5)
        except (Exception, asyncio.CancelledError):  # noqa: BLE001 - a cancelled writer must not stop the close
            pass
        try:
            await self.ws.close(code, reason)
        except Exception:  # noqa: BLE001 - already gone
            pass
        # A connection the server closed is out of the document now, not when (or if) the other end answers.
        room = getattr(self, "room", None)
        if room is not None:
            await room.leave(self)

    async def _writer(self) -> None:
        try:
            while True:
                item = await self.queue.get()
                if item is None:
                    return
                await self.ws.send_text(item)
                self.buffered -= self._sizes.popleft()
        except Exception:  # noqa: BLE001 - the socket died: the reader notices and leaves the room
            pass

    def allow(self, rate: int) -> bool:
        """Token bucket for presence and draft messages."""
        t = time.time()
        if self._tokens < 0:
            self._tokens = float(rate)
        self._tokens = min(float(rate), self._tokens + (t - self._refill) * rate)
        self._refill = t
        if self._tokens < 1.0:
            return False
        self._tokens -= 1.0
        return True


class Room:
    def __init__(self, hub: "Hub", doc: Document, org_id: str):
        self.hub, self.doc_id, self.org_id, self.project_id = hub, doc.id, org_id, doc.project_id
        self.kind = doc.kind
        self.format_version = doc.format_version
        self.state: dict = {}
        self.head = 0
        self.snap_seq = 0
        self.last_changed: dict[str, tuple[int, str | None, str]] = {}  # target -> (seq, client id, author id)
        self.locks: dict[str, Lock] = {}
        self.conns: dict[str, Conn] = {}
        self.reserved_ids: dict[str, str] = {}
        self.reserved_names: dict[str, str] = {}
        self.recent: dict[tuple[str, str], int] = {}  # (client id, change id) -> seq
        self.broken: dict[str, dict] = {}
        self.last_report_seq: int | None = None
        self.known_blobs: set[str] = set()
        self.names: dict[str, str] = {}  # user id -> display name
        self.lock = asyncio.Lock()
        self.idle_since: float | None = time.time()
        self.left_at: dict[str, float] = {}  # client id -> when it disconnected (reservations are kept a while)
        self.evicted = False  # set (under the lock) when the hub unloads this room: nothing may use it after

    # ------------------------------------------------------------------ loading
    async def load(self) -> None:
        async with self.hub.db() as s:
            snap = (await s.execute(select(Snapshot).where(Snapshot.document_id == self.doc_id)
                                    .order_by(Snapshot.seq.desc()).limit(1))).scalar_one_or_none()
            if snap is None:
                raise RuntimeError(f"document {self.doc_id} has no snapshot")
            data = await self.hub.storage.get(snap.key)
            if data is None:
                raise RuntimeError(f"snapshot {snap.key} is missing from storage")
            self.state, self.snap_seq, self.head = await asyncio.to_thread(unpack, data), snap.seq, snap.seq
            for seq, tg, client_id, author in (await s.execute(
                    select(Op.seq, Op.targets, Op.client_id, Op.author_id).where(Op.document_id == self.doc_id)
                    .order_by(Op.seq))).all():
                for t in tg:
                    self.last_changed[t] = (seq, client_id, author)
            for seq, ops in (await s.execute(select(Op.seq, Op.ops).where(
                    Op.document_id == self.doc_id, Op.seq > snap.seq).order_by(Op.seq))).all():
                apply(self.state, ops)
                self.head = seq
        absorb_ids(self.state)

    async def state_at(self, seq: int) -> dict:
        """The document as it was after change `seq`."""
        if seq == self.head:
            return copy.deepcopy(self.state)
        if seq < 0 or seq > self.head:
            raise LookupError(f"no state {seq}")
        async with self.hub.db() as s:
            snap = (await s.execute(select(Snapshot).where(Snapshot.document_id == self.doc_id, Snapshot.seq <= seq)
                                    .order_by(Snapshot.seq.desc()).limit(1))).scalar_one()
            state = await asyncio.to_thread(unpack, await self.hub.storage.get(snap.key))
            for (ops,) in (await s.execute(select(Op.ops).where(
                    Op.document_id == self.doc_id, Op.seq > snap.seq, Op.seq <= seq).order_by(Op.seq))).all():
                apply(state, ops)
        absorb_ids(state)
        return state

    async def snapshot(self) -> None:
        """Store a full copy of the current state (call with the room lock held)."""
        if self.head <= self.snap_seq:
            return
        head = self.head
        # Packed in a thread (it is the whole document), from a copy of the top level: the room's state is
        # replaced, never altered, so nothing below that can change while the thread reads it.
        key, data = snapshot_key(self.doc_id, head), await asyncio.to_thread(pack, dict(self.state))
        await self.hub.storage.put(key, data)
        async with self.hub.db() as s:
            s.add(Snapshot(document_id=self.doc_id, seq=head, key=key, size=len(data)))
            await s.commit()
        self.snap_seq = head

    # ------------------------------------------------------------------ people
    def broadcast(self, msg: dict, skip: str | None = None) -> None:
        self.broadcast_text(encode(msg), skip)

    def broadcast_text(self, text: str | None, skip: str | None = None) -> None:
        """Send one encoded message to everyone (None: it was not JSON, and everyone resyncs, as send does)."""
        for cid, c in list(self.conns.items()):
            if cid != skip:
                if text is None:
                    c.close(4408, "resync")
                else:
                    c.send_text(text)

    async def join(self, ws: WebSocket, user: User, role: str, hello: M.Hello) -> Conn:
        # The id everything is keyed by (locks, reserved ids, resent changes) starts with the user's id:
        # nobody can connect "as" another person's client, whatever client_id they send.
        cid = f"{user.id}.{hello.client_id}"
        async with self.lock:
            if self.evicted:
                raise RoomGone()
            old = self.conns.pop(cid, None)
            if old is not None:
                old.close(4409, "connected again from the same client")
            read_only, reason = role == "viewer", None
            if read_only:
                reason = "you have view access to this project"
            elif self.format_version is not None and hello.format_version != self.format_version:
                read_only = True  # a different openswx could rebuild the model differently: it may look, not edit
                reason = (f"this file uses openswx file format {self.format_version} and this app uses "
                          f"{hello.format_version}; update openswx to edit it")
            taken = {c.colour for c in self.conns.values() if c.user.id != user.id}
            mine = next((c.colour for c in self.conns.values() if c.user.id == user.id), None)
            conn = Conn(ws, user, cid, role, read_only, reason, mine or colour_for(user.id, taken),
                        max_buffer=4 * self.hub.settings.max_change_bytes)
            conn.room = self
            conn.session = token_hash(hello.token)  # (checked again from time to time: see Hub.recheck_access)
            self.names[user.id] = user.name
            released = []
            for t, lk in list(self.locks.items()):  # locks kept through a disconnect: taken up again
                if lk.client_id == cid:
                    if not read_only:
                        lk.detached_at = None
                    else:
                        del self.locks[t]
                        released.append(t)
            welcome: dict[str, Any] = {
                "type": "welcome", "protocol": M.PROTOCOL_VERSION, "document_id": self.doc_id, "seq": self.head,
                "format_version": self.format_version, "you": conn.peer(), "role": role,
                "read_only_reason": reason, "peers": [c.peer() for c in self.conns.values()],
                "locks": {t: lk.holder() for t, lk in self.locks.items()}, "broken": self.broken,
                "project_id": self.project_id,
                # what this client still has from before a disconnect (kept for the grace period; a server
                # restart forgets them): ids and names it may still use
                "reserved": {"ids": sorted(i for i, o in self.reserved_ids.items() if o == cid),
                             "names": sorted(n for n, o in self.reserved_names.items() if o == cid)},
            }
            last = hello.last_seq
            if last is not None and 0 <= last <= self.head and self.head - last <= self.hub.settings.catchup_limit:
                welcome["changes"] = await self._changes_since(last)
            else:
                welcome["content"] = self.state
            conn.send(welcome)
            self.broadcast({"type": "peer.joined", "peer": conn.peer()})
            mine_back = {t: lk.holder() for t, lk in self.locks.items() if lk.client_id == cid}
            if mine_back or released:
                self.broadcast({"type": "lock.changed", "acquired": mine_back, "released": released})
            self.conns[cid] = conn
            self.idle_since = None
            return conn

    async def leave(self, conn: Conn) -> None:
        async with self.lock:
            if self.conns.get(conn.client_id) is conn:
                del self.conns[conn.client_id]
                t = time.time()
                detached = {}
                for target, lk in self.locks.items():
                    if lk.client_id == conn.client_id and lk.detached_at is None:
                        lk.detached_at = t
                        detached[target] = lk.holder()
                self.left_at[conn.client_id] = t  # (its reserved ids and names wait for it, as its locks do)
                self.broadcast({"type": "peer.left", "client_id": conn.client_id})
                if detached:
                    self.broadcast({"type": "lock.changed", "acquired": detached, "released": []})
            conn.close()
            if not self.conns:
                self.idle_since = time.time()

    async def _changes_since(self, seq: int) -> list[dict]:
        async with self.hub.db() as s:
            # (only what a change event carries: not the inverses, arguments or rebuild failures)
            rows = (await s.execute(select(Op.seq, Op.author_id, Op.client_id, Op.change_id, Op.created_at, Op.ops,
                                           Op.command, Op.reverts).where(Op.document_id == self.doc_id, Op.seq > seq)
                                    .order_by(Op.seq))).all()
            await self._load_names(s, {r.author_id for r in rows})
        return [self._event(r.seq, r.author_id, r.client_id, r.change_id, r.created_at, r.ops, r.command, r.reverts)
                for r in rows]

    async def _load_names(self, s, user_ids: set[str]) -> None:
        missing = [u for u in user_ids if u not in self.names]
        if missing:
            for uid, name in (await s.execute(select(User.id, User.name).where(User.id.in_(missing)))).all():
                self.names[uid] = name

    def _event(self, seq, author, client_id, change_id, t, ops, command, reverts) -> dict:
        return {"type": "change", "seq": seq, "author": author, "author_name": self.names.get(author, ""),
                "client_id": client_id, "change_id": change_id, "time": t, "ops": ops, "command": command,
                "reverts": reverts}

    # ------------------------------------------------------------------ locks
    def _expire_locks(self) -> list[str]:
        """Drop locks whose holder has gone, and tell everyone (so no screen keeps showing a dead lock)."""
        t, s = time.time(), self.hub.settings
        gone = []
        for target, lk in list(self.locks.items()):
            if lk.detached_at is not None:
                dead = t > lk.detached_at + s.lock_grace
            else:
                conn = self.conns.get(lk.client_id)
                dead = conn is None or t > conn.last_seen + s.lock_ttl
            if dead:
                del self.locks[target]
                gone.append(target)
        if gone:
            self.broadcast({"type": "lock.changed", "acquired": {}, "released": gone})
        for cid, when in list(self.left_at.items()):
            if cid in self.conns:
                del self.left_at[cid]
            elif t > when + s.lock_grace:
                del self.left_at[cid]
                for table in (self.reserved_ids, self.reserved_names):
                    for k in [k for k, owner in table.items() if owner == cid]:
                        del table[k]
        return gone

    async def sweep(self) -> None:
        async with self.lock:
            self._expire_locks()

    async def acquire(self, conn: Conn, msg: M.LockAcquire) -> None:
        async with self.lock:
            if self.evicted:
                return
            self._expire_locks()
            granted, denied, acquired = [], {}, {}
            held = sum(1 for lk in self.locks.values() if lk.client_id == conn.client_id)
            for t in msg.targets[:MAX_LOCKS]:
                if conn.read_only or not _TARGET.match(t) or not self._exists(t) \
                        or (t not in self.locks and held >= MAX_LOCKS):
                    denied[t] = {"client_id": "", "user_id": "", "name": "", "detached": False}
                    continue
                lk = self.locks.get(t)
                if lk is None:
                    lk = self.locks[t] = Lock(conn.client_id, conn.user.id, conn.user.name)
                    acquired[t] = lk.holder()
                    held += 1
                if lk.client_id == conn.client_id:
                    granted.append(t)
                else:
                    denied[t] = lk.holder()
            conn.send({"type": "lock.result", "request_id": msg.request_id, "granted": granted, "denied": denied})
            if acquired:
                self.broadcast({"type": "lock.changed", "acquired": acquired, "released": []})

    async def release(self, conn: Conn, msg: M.LockRelease) -> None:
        async with self.lock:
            if self.evicted:
                return
            gone = [t for t in msg.targets if t in self.locks and self.locks[t].client_id == conn.client_id]
            for t in gone:
                del self.locks[t]
            if gone:
                self.broadcast({"type": "lock.changed", "acquired": {}, "released": gone})

    # ------------------------------------------------------------------ ids
    async def reserve(self, conn: Conn, msg: M.Reserve) -> None:
        async with self.lock:
            if self.evicted:
                return
            if conn.read_only:
                conn.send({"type": "error", "code": "read_only", "message": conn.reason or "",
                           "request_id": msg.request_id})
                return
            counters = self.state.get("counters") or {}
            mine_ids = sum(1 for o in self.reserved_ids.values() if o == conn.client_id)
            mine_names = sum(1 for o in self.reserved_names.values() if o == conn.client_id)
            if mine_ids + msg.features > MAX_RESERVED or mine_names + len(msg.names) > MAX_RESERVED \
                    or not all(_stem_ok(x) for x in msg.names):
                conn.send({"type": "error", "code": "bad_reserve", "request_id": msg.request_id,
                           "message": "too many reserved ids or names, or a "
                           "name stem that is empty, longer than 40 characters or ends in a digit"})
                return
            feats = self.state.get("features") or []
            have_ids, have_names = {f["id"] for f in feats}, {f["name"] for f in feats}
            nid = int(self.state.get("next_id") or 1)
            ids, names = [], []
            for _ in range(msg.features):
                while f"f{nid}" in have_ids or f"f{nid}" in self.reserved_ids:
                    nid += 1
                ids.append(f"f{nid}")
                self.reserved_ids[f"f{nid}"] = conn.client_id
                nid += 1
            self.state = {**self.state, "next_id": nid}  # (replaced, not altered: a snapshot may be reading it)
            for prefix in msg.names:
                have = counters.get(prefix, 0)
                n = (have if isinstance(have, int) else 0) + 1
                while f"{prefix}{n}" in have_names or f"{prefix}{n}" in self.reserved_names:
                    n += 1
                names.append(f"{prefix}{n}")
                self.reserved_names[f"{prefix}{n}"] = conn.client_id
            conn.send({"type": "ids.reserved", "request_id": msg.request_id, "ids": ids, "names": names})

    # ------------------------------------------------------------------ changes
    def _exists(self, target: str) -> bool:
        """Is there a feature, component or mate behind this target right now?"""
        if target.startswith("f:"):
            coll, item_id = "features", target[2:]
        elif target.startswith("i:"):
            _, coll, item_id = target.split(":", 2)
        else:
            return True
        return any(x["id"] == item_id for x in self.state.get(coll) or [])

    def _who(self, entry: tuple[int, str | None, str]) -> dict:
        seq, client_id, author = entry
        return {"seq": seq, "client_id": client_id, "user_id": author, "name": self.names.get(author, "")}

    def _writable_copy(self, tg: set[str]) -> dict:
        """A copy of the state in which everything a change with these targets may alter is its own object;
        the rest is shared with the current state (so a change costs what it touches, not the document).
        Nothing ever alters a state in place once it is current."""
        new = dict(self.state)
        for coll in LISTS:
            if isinstance(self.state.get(coll), list) or coll == "features":
                prefix = "f:" if coll == "features" else f"i:{coll}:"
                touched = {t[len(prefix):] for t in tg if t.startswith(prefix)}
                new[coll] = [copy.deepcopy(x) if x["id"] in touched else x for x in self.state.get(coll) or []]
        for key in {t[2:] for t in tg if t.startswith("k:")}:
            if key in new:
                new[key] = copy.deepcopy(new[key])
        new["counters"] = dict(new["counters"]) if isinstance(new.get("counters"), dict) else {}
        return new

    async def _commit(self, *, user: User, client_id: str | None, change_id: str | None, base_seq: int,
                      ops: list[dict], command: str, args: dict | None, reverts: int | None = None,
                      check_stale: bool = True, from_history: bool = False) -> tuple[bool, dict]:
        """Check, store and broadcast one change. The room lock must be held."""
        def no(code: str, message: str, conflicts: dict | None = None) -> tuple[bool, dict]:
            return False, {"code": code, "message": message, "conflicts": conflicts or {}}

        if self.evicted:
            raise RoomGone()
        if not ops:
            return no("invalid", "the change is empty")
        if base_seq > self.head or base_seq < 0:
            return no("invalid", f"base_seq {base_seq} is not a state of this document")
        try:
            ops = clean(ops)
            if self.kind != "assembly" and any(parse(op)[1] not in ("", "features") for op in ops):
                return no("invalid", "only assemblies have components and mates")
            tg = targets(ops)
            new_targets = added_targets(ops)
            added = added_ids(ops)
            added_names = {op["feature"]["name"] for op in ops if op.get("op") == "feature.add"}
        except OpError as exc:
            return no("invalid", str(exc))
        except (KeyError, TypeError, AttributeError, IndexError):
            return no("invalid", "malformed operations")
        self._expire_locks()
        check = set(tg) | ({STRUCTURE} if is_structural(ops) else set())
        def blocks(lk: Lock) -> bool:  # (a change made outside any connection - a restore - is its author's)
            return lk.client_id != client_id and not (client_id is None and lk.user_id == user.id)
        locked = {t: self.locks[t].holder() for t in check if t in self.locks and blocks(self.locks[t])}
        if locked:
            who = ", ".join(sorted({h["name"] for h in locked.values()}))
            return no("locked", f"being edited by {who}", locked)
        if check_stale:
            stale = {}
            for t in tg:
                if t in new_targets and not self._exists(t):
                    continue  # (what did not exist cannot have been changed by someone else)
                entry = self.last_changed.get(t)
                if entry and entry[0] > base_seq and (client_id is None or entry[1] != client_id):
                    stale[t] = self._who(entry)
            if stale:
                return no("stale", "someone changed the same thing while you were editing", stale)
        taken = {f"f:{i}": self.reserved_ids[i] for i in added
                 if self.reserved_ids.get(i, client_id) != client_id}
        taken.update({f"name:{n}": self.reserved_names[n] for n in added_names
                      if self.reserved_names.get(n, client_id) != client_id})
        if taken:
            return no("id_taken", "a feature id or name in this change is reserved by another client", taken)
        # Everything that can fail happens here, on a copy, before anything is stored: a change is either in
        # the history, in memory and sent to everyone, or it is none of those.
        try:
            new_state = self._writable_copy(tg)
            apply(new_state, ops)
            absorb_ids(new_state, [op["feature"] for op in ops if op.get("op") == "feature.add"])
            inverse = diff(new_state, self.state)
        except OpError as exc:
            return no("invalid", str(exc))
        except Exception as exc:  # noqa: BLE001 - malformed in a way apply did not name
            return no("invalid", f"cannot be applied ({type(exc).__name__})")
        refs = None
        if self.kind == "assembly" and any(t.startswith("i:components:") for t in tg):
            # every component the change touched must point at a part its author may use, at a state it has
            changed = [c for c in new_state.get("components") or [] if f"i:components:{c['id']}" in tg]
            # Choosing a part takes access to it. Moving, removing or putting back a component that already
            # uses it does not, and neither does undo or restore (they only bring back what was there).
            def pin(c: dict) -> tuple:
                ref = c.get("ref") if isinstance(c.get("ref"), dict) else {}
                return ref.get("document"), ref.get("seq")
            was = {c["id"]: pin(c) for c in self.state.get("components") or []}
            # (which part, and which state of it: pointing a component at another state is choosing too)
            chosen = set() if from_history else {c["id"] for c in changed if was.get(c["id"]) != pin(c)}
            try:
                async with self.hub.db() as s:
                    # (a component that keeps pointing where it did may be moved even if its part has gone)
                    kept = {c["id"] for c in changed if c["id"] in was and was[c["id"]] == pin(c)}
                    await check_refs(s, self.org_id, user, changed, access_for=chosen,
                                     allow_deleted=True if from_history else kept)
                refs = ref_rows(new_state)
            except OpError as exc:
                return no("invalid", str(exc))
            except Exception:  # noqa: BLE001
                logging.getLogger("openswx_collab").exception("could not check references of %s", self.doc_id)
                return no("server_error", "the server could not check the change; try again")
        unknown = sorted(blob_refs(ops) - self.known_blobs)
        missing = [sha for sha in unknown if not re.fullmatch(r"[0-9a-f]{64}", sha)]
        asks = [sha for sha in unknown if sha not in missing]
        if asks:  # (asked all at once, a few at a time, rather than one after another under the room lock)
            gate = asyncio.Semaphore(BLOB_CHECKS)

            async def there(sha: str) -> bool:
                async with gate:
                    return await self.hub.storage.exists(blob_key(self.org_id, sha))
            found = await asyncio.gather(*(there(sha) for sha in asks), return_exceptions=True)
            failed = [r for r in found if isinstance(r, BaseException)]
            if failed:
                logging.getLogger("openswx_collab").error("could not reach storage for %s", self.doc_id,
                                                          exc_info=failed[0])
                return no("server_error", "the server could not check the change; try again")
            for sha, ok in zip(asks, found):
                if ok:
                    self.known_blobs.add(sha)
                else:
                    missing.append(sha)
        if missing:
            return no("missing_blob", "upload these blobs first", {"blobs": missing})
        seq, t = self.head + 1, now()
        try:  # (kept for the history when small and storable)
            small_args = args if args is not None and len(json.dumps(args, allow_nan=False)) <= 4096 else None
        except (TypeError, ValueError):
            small_args = None
        try:
            async with self.hub.db() as s:
                s.add(Op(document_id=self.doc_id, seq=seq, author_id=user.id, client_id=client_id,
                         change_id=change_id, created_at=t, command=command[:100], args=small_args, ops=ops,
                         inverse_ops=inverse, targets=sorted(tg), reverts=reverts))
                await s.execute(update(Document).where(Document.id == self.doc_id).values(head_seq=seq, updated_at=t))
                if reverts is not None:
                    await s.execute(update(Op).where(Op.document_id == self.doc_id, Op.seq == reverts)
                                    .values(reverted_by=seq))
                if refs is not None:  # (in the same transaction: "where used" never disagrees with the content)
                    if not from_history:
                        # the parts are looked at once more, locked, inside the transaction that records the
                        # use: a part deleted between the check above and here must not gain a user
                        wanted = {part_id for part_id, _ in refs.values()}
                        alive = set((await s.execute(select(Document.id).where(
                            Document.id.in_(wanted), Document.deleted_at.is_(None)).with_for_update())).scalars())
                        gone = wanted - alive
                        if any(refs[c][0] in gone for c in chosen if c in refs):  # (newly pointed at it)
                            await s.rollback()
                            return no("invalid", "a part this change uses has just been deleted")
                    await s.execute(delete(AssemblyRef).where(AssemblyRef.assembly_id == self.doc_id))
                    for component_id, (part_id, pinned) in refs.items():
                        s.add(AssemblyRef(assembly_id=self.doc_id, component_id=component_id, part_id=part_id,
                                          seq=pinned))
                await s.commit()
        except Exception:  # noqa: BLE001 - not stored: not applied, not sent, and the sender is told
            logging.getLogger("openswx_collab").exception("could not store change %s of %s", seq, self.doc_id)
            return no("server_error", "the server could not store the change; try again")
        self.state, self.head = new_state, seq
        self.names[user.id] = user.name
        for tname in tg:
            self.last_changed[tname] = (seq, client_id, user.id)
        for i in added:
            self.reserved_ids.pop(i, None)
        for n in added_names:
            self.reserved_names.pop(n, None)
        if client_id and change_id:
            self.recent[(client_id, change_id)] = seq
            if len(self.recent) > 5000:
                for k in list(self.recent)[:2500]:
                    del self.recent[k]
        self.broadcast(self._event(seq, user.id, client_id, change_id, t, ops, command[:100], reverts), skip=client_id)
        if self.head - self.snap_seq >= self.hub.settings.snapshot_every:
            try:
                await self.snapshot()
            except Exception:  # noqa: BLE001 - the change is safe in the history; the next change tries again
                logging.getLogger("openswx_collab").exception("could not store a snapshot of %s", self.doc_id)
        return True, {"seq": seq}

    async def change(self, conn: Conn, msg: M.Change) -> None:
        async with self.lock:
            if self.evicted or conn.closed:
                return
            seq = self.recent.get((conn.client_id, msg.change_id))
            if seq is None:  # sent before a disconnect or a server restart, and sent again: already in the history?
                try:
                    async with self.hub.db() as s:
                        seq = (await s.execute(select(Op.seq).where(
                            Op.document_id == self.doc_id, Op.client_id == conn.client_id,
                            Op.change_id == msg.change_id))).scalar_one_or_none()
                except Exception:  # noqa: BLE001 - the database is unreachable: say so, keep the connection
                    logging.getLogger("openswx_collab").exception("could not look up change %s", msg.change_id)
                    conn.send({"type": "change.rejected", "change_id": msg.change_id, "code": "server_error",
                               "message": "the server could not store the change; try again", "conflicts": {}})
                    return
            if seq is not None:
                conn.send({"type": "change.accepted", "change_id": msg.change_id, "seq": seq})
                return
            if conn.read_only:
                ok, res = False, {"code": "read_only", "message": conn.reason or "read-only", "conflicts": {}}
            else:
                ok, res = await self._commit(user=conn.user, client_id=conn.client_id, change_id=msg.change_id,
                                             base_seq=msg.base_seq, ops=msg.ops, command=msg.command, args=msg.args)
            if ok:
                conn.send({"type": "change.accepted", "change_id": msg.change_id, "seq": res["seq"]})
            else:
                conn.send({"type": "change.rejected", "change_id": msg.change_id, **res})

    async def revert(self, conn: Conn, msg: M.Revert) -> None:
        """Undo one earlier change by applying its stored inverse as a new change."""
        def fail(code: str, message: str, conflicts: dict | None = None) -> None:
            conn.send({"type": "change.revert.result", "request_id": msg.request_id, "ok": False, "code": code,
                       "message": message, "conflicts": conflicts or {}})

        async with self.lock:
            if self.evicted or conn.closed:
                return None
            if conn.read_only:
                return fail("read_only", conn.reason or "read-only")
            try:
                async with self.hub.db() as s:
                    row = await s.get(Op, (self.doc_id, msg.seq))
            except Exception:  # noqa: BLE001
                logging.getLogger("openswx_collab").exception("could not read change %s of %s", msg.seq, self.doc_id)
                return fail("server_error", "the server could not read the history; try again")
            if row is None:
                return fail("not_found", f"there is no change {msg.seq}")
            if msg.mode == "undo" and row.author_id != conn.user.id:
                return fail("not_yours", "undo only takes back your own changes")
            if row.reverted_by is not None:
                return fail("already_reverted", "this change was already undone")
            later = {t: self._who(self.last_changed[t]) for t in row.targets
                     if t in self.last_changed and self.last_changed[t][0] > msg.seq}
            if later:
                who = ", ".join(sorted({w["name"] or "someone" for w in later.values()}))
                return fail("changed_since", f"cannot undo: {who} changed the same thing afterwards", later)
            ok, res = await self._commit(user=conn.user, client_id=conn.client_id, change_id=None,
                                         base_seq=self.head, ops=row.inverse_ops,
                                         command=f"{msg.mode} {row.command}".strip(), args=None, reverts=msg.seq,
                                         from_history=True)
            if not ok:
                return fail(res["code"], res["message"], res["conflicts"])
            # the sender is skipped by the broadcast: it gets the inverse here to apply itself
            conn.send({"type": "change.revert.result", "request_id": msg.request_id, "ok": True, "seq": res["seq"],
                       "ops": row.inverse_ops})

    async def restore(self, user: User, seq: int) -> tuple[bool, dict]:  # noqa: C901
        """Put the document back to how it was after change `seq`, as one new change."""
        async with self.lock:
            if self.evicted:
                raise RoomGone()
            target = await self.state_at(seq)
            try:
                ops = diff(self.state, target)
            except OpError as exc:
                return False, {"code": "invalid", "message": str(exc), "conflicts": {}}
            if not ops:
                return True, {"seq": self.head}
            self._expire_locks()
            others = {t: lk.holder() for t, lk in self.locks.items() if lk.user_id != user.id}
            if others:  # (your own locks do not stop you)
                return False, {"code": "locked", "message": "other people are editing this document; restore needs "
                               "their locks released", "conflicts": others}
            return await self._commit(user=user, client_id=None, change_id=None, base_seq=self.head, ops=ops,
                                      command=f"restore to {seq}", args=None, check_stale=False,
                                      from_history=True)

    # ------------------------------------------------------------------ rebuild reports
    async def report(self, conn: Conn, msg: M.RebuildReport) -> None:
        """A client says which features failed to rebuild at a state: new failures are pinned on the change
        that came in since the last report."""
        async with self.lock:
            if self.evicted or conn.read_only:  # (someone who cannot edit cannot blame an edit either)
                return
            if msg.seq > self.head or (self.last_report_seq is not None and msg.seq <= self.last_report_seq):
                return
            errors = {str(k): str(v)[:500] for k, v in list(msg.errors.items())[:500]}
            baseline = self.last_report_seq
            fresh = {f: e for f, e in errors.items() if f not in self.broken}
            fixed = [f for f in self.broken if f not in errors]
            cause: dict = {}
            if fresh and baseline is not None:
                async with self.hub.db() as s:
                    row = await s.get(Op, (self.doc_id, msg.seq))
                    if row is not None:
                        await self._load_names(s, {row.author_id})
                        cause = {"by_seq": row.seq, "by_user": row.author_id,
                                 "by_name": self.names.get(row.author_id, ""), "command": row.command,
                                 "since_seq": baseline + 1}
                        row.broke = {**(row.broke or {}), **fresh}
                        await s.commit()
            for f, e in errors.items():
                self.broken[f] = {**self.broken.get(f, cause if f in fresh else {}), "error": e}
            for f in fixed:
                del self.broken[f]
            self.last_report_seq = msg.seq
            if fresh or fixed:
                self.broadcast({"type": "rebuild.status", "seq": msg.seq, "broken": self.broken})

    # ------------------------------------------------------------------ presence
    def presence(self, conn: Conn, msg: M.Presence | M.Draft) -> None:
        if not conn.allow(self.hub.settings.presence_rate):
            return
        presence = isinstance(msg, M.Presence)
        if not presence and conn.read_only:
            return
        # Encoded once for everyone; what cannot be encoded (NaN: other clients could not parse it) is dropped
        # here, and never kept for the welcome either.
        text = encode({"type": "presence" if presence else "draft", "client_id": conn.client_id, "data": msg.data})
        if text is None:
            return
        if presence:
            conn.presence = msg.data
        else:
            conn.draft = msg.data
        self.broadcast_text(text, skip=conn.client_id)


class Hub:
    def __init__(self, settings, db_maker, storage):
        self.settings, self.db, self.storage = settings, db_maker, storage
        self.rooms: dict[str, Room] = {}
        self._opening: dict[str, asyncio.Lock] = {}
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._sweeper())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        for room in list(self.rooms.values()):
            for c in list(room.conns.values()):
                c.close(1001, "the server is shutting down")
            async with room.lock:
                await room.snapshot()
        self.rooms.clear()

    async def room(self, document_id: str) -> Room:
        room = self.rooms.get(document_id)
        if room is not None:
            return room
        gate = self._opening.setdefault(document_id, asyncio.Lock())
        async with gate:
            room = self.rooms.get(document_id)
            if room is None:
                async with self.db() as s:
                    doc = await s.get(Document, document_id)
                    if doc is None or doc.deleted_at is not None:
                        raise LookupError("no such document")
                    project = await s.get(Project, doc.project_id)
                room = Room(self, doc, project.org_id)
                await room.load()
                self.rooms[document_id] = room
        self._opening.pop(document_id, None)
        return room

    async def _sweeper(self) -> None:
        checked = time.time()
        while True:
            await asyncio.sleep(self.settings.sweep_interval)
            await self.sweep()
            if time.time() - checked >= self.settings.access_recheck:
                checked = time.time()
                try:
                    await self.recheck_access()
                except Exception:  # noqa: BLE001 - tried again next time
                    logging.getLogger("openswx_collab").exception("access re-check failed")

    async def sweep(self) -> None:
        t = time.time()
        for doc_id, room in list(self.rooms.items()):
            try:
                await self._sweep_room(doc_id, room, t)
            except Exception:  # noqa: BLE001 - one room's trouble (storage down) is not the others'
                logging.getLogger("openswx_collab").exception("sweep of %s failed", doc_id)

    async def _sweep_room(self, doc_id: str, room: Room, t: float) -> None:
        await room.sweep()
        if not room.conns and not room.locks and room.idle_since is not None \
                and t - room.idle_since > self.settings.room_idle:
            async with room.lock:
                if room.conns:
                    return
                await room.snapshot()
                room.evicted = True  # (anyone who was waiting for this room's lock asks the hub again)
                self.rooms.pop(doc_id, None)

    async def recheck_access(self) -> None:
        """Whoever is connected must still be signed in and still have the role they connected with. Role
        changes drop people at once (drop_user); this catches everything else: a session ended by a password
        reset or the operator, a disabled account, a role that changed while the socket was being opened."""
        for room in list(self.rooms.values()):
            conns = list(room.conns.values())
            if not conns:
                continue
            async with self.db() as s:
                project = await s.get(Project, room.project_id)
                for c in conns:
                    sess = await s.get(Session, getattr(c, "session", ""))
                    user = await s.get(User, c.user.id) if sess is not None and sess.expires_at > now() else None
                    ok = user is not None and not user.disabled and project is not None
                    if not ok or await project_role(s, user, project) != c.role:
                        c.close(4403, "your access changed")

    def forget_user(self, user_id: str, name: str) -> None:
        """An account was deleted: open documents show its changes under `name` from now on."""
        for room in self.rooms.values():
            if user_id in room.names:
                room.names[user_id] = name

    def drop_account(self, user_id: str) -> None:
        """Disconnect every open document of an account whose sessions were just ended."""
        for room in list(self.rooms.values()):
            for c in list(room.conns.values()):
                if c.user.id == user_id:
                    c.close(4401, "sign in again")

    async def drop_user(self, user_id: str, org_id: str) -> None:
        """Disconnect a user whose access changed: reconnecting applies the new role."""
        for room in list(self.rooms.values()):
            if room.org_id == org_id:
                for c in list(room.conns.values()):
                    if c.user.id == user_id:
                        c.close(4403, "your access changed")

    async def close_document(self, document_id: str, reason: str) -> None:
        room = self.rooms.pop(document_id, None)
        if room is not None:
            room.evicted = True
            for c in list(room.conns.values()):
                c.close(4404, reason)

    def notify(self, document_id: str, msg: dict) -> None:
        room = self.rooms.get(document_id)
        if room is not None:
            room.broadcast(msg)
