"""Cloud documents: create, list, content, history, named versions, restore, blobs, and the live WebSocket."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from openswx_collab_protocol import messages as M
from openswx_collab_protocol.ops import LOCAL_ONLY, MAX_DEPTH, MAX_NAME, OpError, absorb_ids, blob_refs, check_value

from .access import current_user, get_db, need_document, need_project, project_role, user_for_token
from .assemblies import check_refs, ref_rows, used_in, visible, where_pinned_older
from .db import AssemblyRef, Document, Op, Project, Snapshot, User, Version, now
from .hub import RoomGone, pack
from .storage import blob_key, snapshot_key

router = APIRouter(prefix="/api")
_SHA = re.compile(r"^[0-9a-f]{64}$")
KINDS = {"part", "assembly"}


def doc_out(d: Document, role: str | None = None) -> dict:
    out = {"id": d.id, "project_id": d.project_id, "name": d.name, "kind": d.kind,
           "format_version": d.format_version, "seq": d.head_seq, "created_by": d.created_by,
           "created_at": d.created_at, "updated_at": d.updated_at}
    if role:
        out["role"] = role
    return out


# ------------------------------------------------------------------ blobs (per organisation, reached through a project)
@router.put("/projects/{project_id}/blobs/{sha}", status_code=204)
async def put_blob(project_id: str, sha: str, request: Request, user: User = Depends(current_user),
                   db: AsyncSession = Depends(get_db)):
    project, _ = await need_project(db, user, project_id, "editor")
    if not _SHA.match(sha):
        raise HTTPException(422, "the blob name must be its sha256 in lower-case hex")
    limit = request.app.state.settings.max_blob_bytes
    if int(request.headers.get("content-length") or 0) > limit:
        raise HTTPException(413, "too large")
    h, chunks, size = hashlib.sha256(), [], 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise HTTPException(413, "too large")
        h.update(chunk)
        chunks.append(chunk)
    if h.hexdigest() != sha:
        raise HTTPException(422, "the content does not match the sha256 in the address")
    await request.app.state.storage.put(blob_key(project.org_id, sha), b"".join(chunks))


@router.get("/projects/{project_id}/blobs/{sha}")
async def get_blob(project_id: str, sha: str, request: Request, user: User = Depends(current_user),
                   db: AsyncSession = Depends(get_db)):
    project, _ = await need_project(db, user, project_id)
    data = await request.app.state.storage.get(blob_key(project.org_id, sha)) if _SHA.match(sha) else None
    if data is None:
        raise HTTPException(404, "no such blob")
    return Response(data, media_type="application/octet-stream",
                    headers={"Cache-Control": "private, max-age=31536000, immutable"})


@router.post("/projects/{project_id}/blobs/missing")
async def missing_blobs(project_id: str, shas: list[str], request: Request, user: User = Depends(current_user),
                        db: AsyncSession = Depends(get_db)):
    """Which of these blobs the server does not have yet (so a client uploads only those)."""
    project, _ = await need_project(db, user, project_id)
    store = request.app.state.storage
    return [s for s in shas[:2000] if not _SHA.match(s) or not await store.exists(blob_key(project.org_id, s))]


# ------------------------------------------------------------------ documents
class DocumentIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    kind: str = "part"
    content: dict[str, Any]  # the openswx document (PartDocument.to_dict()), long strings as {"$blob": sha256}


def _check_content(content: dict, kind: str = "part") -> dict:
    feats = content.get("features")
    if feats is None:
        feats = content["features"] = []
    if not isinstance(feats, list) or not all(
            isinstance(f, dict) and all(isinstance(f.get(k), str) and 0 < len(f[k]) <= MAX_NAME
                                        for k in ("id", "name", "type")) for f in feats):
        raise HTTPException(422, "content.features must be a list of features with id, name and type")
    try:
        check_value(content, MAX_DEPTH)
    except OpError as exc:
        raise HTTPException(422, str(exc)) from exc
    for key in ("id", "name"):
        seen = [f[key] for f in feats]
        if len(seen) != len(set(seen)):
            raise HTTPException(422, f"two features share the same {key}")
    for coll in ("components", "mates"):
        items = content.get(coll)
        if kind != "assembly":
            if items is not None:
                raise HTTPException(422, f"a {kind} has no {coll}")
            continue
        if items is None:
            items = content[coll] = []
        if not isinstance(items, list) or not all(isinstance(x, dict) and isinstance(x.get("id"), str)
                                                  and 0 < len(x["id"]) <= MAX_NAME for x in items) \
                or len({x["id"] for x in items}) != len(items):
            raise HTTPException(422, f"content.{coll} must be a list of objects with distinct ids")
    for k in LOCAL_ONLY:
        content.pop(k, None)
    absorb_ids(content)
    return content


@router.get("/projects/{project_id}/documents")
async def list_documents(project_id: str, user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    _, role = await need_project(db, user, project_id)
    rows = (await db.execute(select(Document).where(Document.project_id == project_id, Document.deleted_at.is_(None))
                             .order_by(Document.name))).scalars()
    return [doc_out(d, role) for d in rows]


@router.post("/projects/{project_id}/documents", status_code=201)
async def create_document(project_id: str, body: DocumentIn, request: Request, user: User = Depends(current_user),
                          db: AsyncSession = Depends(get_db)):
    """Turn collaboration on for a file: its current content becomes state 0 of a cloud document."""
    project, role = await need_project(db, user, project_id, "editor")
    if body.kind not in KINDS:
        raise HTTPException(422, f"there is no document kind {body.kind!r} (part or assembly)")
    content = _check_content(body.content, body.kind)
    refs = {}
    if body.kind == "assembly":
        try:
            await check_refs(db, project.org_id, user, content["components"])
            refs = ref_rows(content)
        except OpError as exc:
            raise HTTPException(422, str(exc)) from exc
    store = request.app.state.storage
    missing = [s for s in blob_refs(content) if not _SHA.match(s) or not await store.exists(blob_key(project.org_id, s))]
    if missing:
        raise HTTPException(409, {"message": "upload these blobs first", "blobs": missing})
    version = content.get("version")
    doc = Document(project_id=project_id, name=body.name.strip(), kind=body.kind, created_by=user.id,
                   format_version=version if isinstance(version, int) else None)
    db.add(doc)
    await db.flush()
    key, data = snapshot_key(doc.id, 0), pack(content)
    await store.put(key, data)
    db.add(Snapshot(document_id=doc.id, seq=0, key=key, size=len(data)))
    for component_id, (part_id, pinned) in refs.items():
        db.add(AssemblyRef(assembly_id=doc.id, component_id=component_id, part_id=part_id, seq=pinned))
    await db.commit()
    return doc_out(doc, role)


@router.get("/documents/{document_id}")
async def get_document(document_id: str, user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    doc, _, role = await need_document(db, user, document_id)
    return doc_out(doc, role)


class RenameIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


@router.patch("/documents/{document_id}")
async def rename_document(document_id: str, body: RenameIn, user: User = Depends(current_user),
                          db: AsyncSession = Depends(get_db)):
    doc, _, role = await need_document(db, user, document_id, "editor")
    doc.name = body.name.strip()
    await db.commit()
    return doc_out(doc, role)


@router.delete("/documents/{document_id}", status_code=204)
async def delete_document(document_id: str, request: Request, user: User = Depends(current_user),
                          db: AsyncSession = Depends(get_db)):
    doc, _, _ = await need_document(db, user, document_id, "owner")
    # Mark it deleted first, then look for users, in one transaction. The write locks the part's row (the
    # whole database on SQLite), and a change that adds a use of the part locks that row too before it
    # records the use (hub.Room._commit): one of the two waits for the other, so a part can never end up
    # deleted with a new user that this check did not see.
    doc.deleted_at = now()  # (kept in the database: a deleted document can be brought back by the operator)
    await db.flush()
    users = await used_in(db, document_id)
    if users:  # (assemblies pinned to it would lose the part they are built from)
        shown, hidden = await visible(db, user, users)  # (not the names of assemblies the asker cannot open)
        await db.rollback()
        raise HTTPException(409, {"message": "assemblies use this part; remove it from them first",
                                  "assemblies": shown, "in_projects_you_cannot_open": hidden})
    await db.commit()
    await request.app.state.hub.close_document(document_id, "the document was deleted")


@router.get("/documents/{document_id}/content")
async def get_content(document_id: str, request: Request, seq: int | None = None, version: str | None = None,
                      user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    """The document at a point in its history (default: now). Used for backups, history and pinned versions."""
    doc, _, _ = await need_document(db, user, document_id)
    if version is not None:
        v = (await db.execute(select(Version).where(Version.document_id == document_id, Version.name == version))
             ).scalar_one_or_none()
        if v is None:
            raise HTTPException(404, f"no version named {version!r}")
        seq = v.seq
    room = await request.app.state.hub.room(document_id)
    try:
        state = await room.state_at(room.head if seq is None else seq)
    except LookupError as exc:
        raise HTTPException(404, f"the document has no state {seq}") from exc
    return {"document": doc_out(doc), "seq": room.head if seq is None else seq, "content": state}


@router.get("/documents/{document_id}/history")
async def history(document_id: str, before: int | None = None, after: int | None = None, limit: int = 100,
                  user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    """Newest first. `after` and `before` bound it: what changed in a part between the state an assembly is
    pinned to and a newer one is history?after=<pinned seq>&before=<newer seq + 1>."""
    await need_document(db, user, document_id)
    q = select(Op).where(Op.document_id == document_id)
    if before is not None:
        q = q.where(Op.seq < before)
    if after is not None:
        q = q.where(Op.seq > after)
    rows = (await db.execute(q.order_by(Op.seq.desc()).limit(max(1, min(limit, 500))))).scalars().all()
    names = dict((await db.execute(select(User.id, User.name).where(
        User.id.in_({r.author_id for r in rows})))).all()) if rows else {}
    return [{"seq": r.seq, "author": {"id": r.author_id, "name": names.get(r.author_id, "")}, "time": r.created_at,
             "command": r.command, "args": r.args, "targets": r.targets, "reverts": r.reverts,
             "reverted_by": r.reverted_by, "broke": r.broke} for r in rows]


class VersionIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    note: str = Field(default="", max_length=4000)
    seq: int | None = None  # default: the current state


def _version_out(v: Version, names: dict | None = None) -> dict:
    return {"id": v.id, "name": v.name, "note": v.note, "seq": v.seq, "created_at": v.created_at,
            "author": {"id": v.author_id, "name": (names or {}).get(v.author_id, "")}}


@router.get("/documents/{document_id}/versions")
async def list_versions(document_id: str, user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    await need_document(db, user, document_id)
    rows = (await db.execute(select(Version).where(Version.document_id == document_id).order_by(Version.seq.desc(),
                                                                                              Version.created_at.desc()))).scalars().all()
    names = dict((await db.execute(select(User.id, User.name).where(
        User.id.in_({r.author_id for r in rows})))).all()) if rows else {}
    return [_version_out(v, names) for v in rows]


@router.post("/documents/{document_id}/versions", status_code=201)
async def create_version(document_id: str, body: VersionIn, request: Request, user: User = Depends(current_user),
                         db: AsyncSession = Depends(get_db)):
    doc, _, _ = await need_document(db, user, document_id, "editor")
    seq = doc.head_seq if body.seq is None else body.seq
    if not 0 <= seq <= doc.head_seq:
        raise HTTPException(422, f"the document has no state {seq}")
    v = Version(document_id=document_id, seq=seq, name=body.name.strip(), note=body.note, author_id=user.id)
    db.add(v)
    try:
        await db.commit()
    except IntegrityError as exc:
        raise HTTPException(409, f"there is already a version named {body.name.strip()!r}") from exc
    hub = request.app.state.hub
    hub.notify(document_id, {"type": "version.created", "id": v.id, "seq": seq, "name": v.name, "author": user.id})
    # everyone with an assembly open that uses an older state of this part hears about it
    for assembly_id, components in (await where_pinned_older(db, document_id, seq)).items():
        # ids only: someone in the assembly may not be allowed to open the part, or know what it is called
        hub.notify(assembly_id, {"type": "reference.update", "document": document_id, "components": components})
    return _version_out(v, {user.id: user.name})


@router.delete("/documents/{document_id}/versions/{version_id}", status_code=204)
async def delete_version(document_id: str, version_id: str, user: User = Depends(current_user),
                         db: AsyncSession = Depends(get_db)):
    await need_document(db, user, document_id, "owner")  # (versions are what others pin to: owners only)
    v = await db.get(Version, version_id)
    if v is None or v.document_id != document_id:
        raise HTTPException(404, "no such version")
    await db.delete(v)
    await db.commit()


class RestoreIn(BaseModel):
    seq: int | None = None
    version: str | None = None


@router.post("/documents/{document_id}/restore")
async def restore(document_id: str, body: RestoreIn, request: Request, user: User = Depends(current_user),
                  db: AsyncSession = Depends(get_db)):
    """Put the document back to an earlier state. Nothing is lost: the restore is itself a change in the history."""
    await need_document(db, user, document_id, "editor")
    seq = body.seq
    if body.version is not None:
        v = (await db.execute(select(Version).where(Version.document_id == document_id, Version.name == body.version))
             ).scalar_one_or_none()
        if v is None:
            raise HTTPException(404, f"no version named {body.version!r}")
        seq = v.seq
    if seq is None:
        raise HTTPException(422, "give seq or version")
    hub = request.app.state.hub
    try:
        try:
            ok, res = await (await hub.room(document_id)).restore(user, seq)
        except RoomGone:  # (unloaded while we waited: load it again)
            ok, res = await (await hub.room(document_id)).restore(user, seq)
    except LookupError as exc:
        raise HTTPException(404, f"the document has no state {seq}") from exc
    if not ok:
        raise HTTPException(409, res)
    return res


# ------------------------------------------------------------------ live
@router.websocket("/documents/{document_id}/ws")
async def document_ws(ws: WebSocket, document_id: str):
    app = ws.app
    await ws.accept()

    async def refuse(code: str, message: str, close: int) -> None:
        try:
            await ws.send_text(json.dumps({"type": "error", "code": code, "message": message}))
            await ws.close(close, message[:120])
        except Exception:  # noqa: BLE001
            pass

    try:
        hello = M.Hello.model_validate_json(await asyncio.wait_for(ws.receive_text(), 15))
    except (asyncio.TimeoutError, ValidationError, WebSocketDisconnect, KeyError):
        return await refuse("bad_hello", "the first message must be a valid hello", 4400)
    if hello.protocol != M.PROTOCOL_VERSION:
        return await refuse("protocol", f"this server speaks protocol {M.PROTOCOL_VERSION}; update openswx", 4426)
    async with app.state.db() as db:
        user = await user_for_token(db, hello.token)
        if user is None:
            return await refuse("unauthorized", "sign in again", 4401)
        doc = await db.get(Document, document_id)
        project = await db.get(Project, doc.project_id) if doc is not None and doc.deleted_at is None else None
        role = await project_role(db, user, project) if project is not None else None
    if role is None:
        return await refuse("not_found", "no such document", 4404)
    conn = None
    for _ in range(3):
        try:
            room = await app.state.hub.room(document_id)
            conn = await room.join(ws, user, role, hello)
            break
        except LookupError:
            return await refuse("not_found", "no such document", 4404)
        except RoomGone:  # (the room was unloaded at that moment: open it again)
            continue
    if conn is None:
        return await refuse("busy", "try again", 4408)
    limit = app.state.settings.max_change_bytes
    try:
        while True:
            raw = await ws.receive_text()
            if conn.closed:  # (dropped by the server - access changed, replaced, too slow: nothing more is taken)
                break
            conn.last_seen = now()
            if len(raw) > limit:
                conn.send({"type": "error", "code": "too_large", "message": "send large data as blobs"})
                continue
            try:
                msg = M.client_message.validate_json(raw)
            except ValidationError as exc:
                conn.send({"type": "error", "code": "bad_message", "message": str(exc)[:300]})
                continue
            if isinstance(msg, (M.Presence, M.Draft)):
                # cursors are tiny and previews small: anything bigger is not presence
                if len(raw) <= (16384 if isinstance(msg, M.Presence) else 131072):
                    room.presence(conn, msg)
            elif isinstance(msg, M.Change):
                await room.change(conn, msg)
            elif isinstance(msg, M.LockAcquire):
                await room.acquire(conn, msg)
            elif isinstance(msg, M.LockRelease):
                await room.release(conn, msg)
            elif isinstance(msg, M.Reserve):
                await room.reserve(conn, msg)
            elif isinstance(msg, M.Revert):
                await room.revert(conn, msg)
            elif isinstance(msg, M.RebuildReport):
                await room.report(conn, msg)
            elif isinstance(msg, M.Ping):
                conn.send({"type": "pong", "seq": room.head})
    except (WebSocketDisconnect, RuntimeError, RoomGone):
        pass
    finally:
        await room.leave(conn)
