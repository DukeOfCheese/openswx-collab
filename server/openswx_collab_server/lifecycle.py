"""Leaving: deleting an account or an organisation, and taking your data with you.

Deleting an account keeps the work: documents, history and versions belong to their organisation. The
account is emptied, not removed, because history refers to it: its name becomes "Deleted user", its email
address is freed (someone may sign up with it again), and its sessions, sign-in methods and memberships
go. It is refused while the person is the only owner of an organisation (hand it over or delete it first)
or the server's last admin.

Deleting an organisation removes everything in it: projects, documents, history, versions, invites and the
stored files. There is no undo, so it takes the organisation's name typed out as well as the password.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import zipfile
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.background import BackgroundTask

from openswx_collab_protocol.ops import blob_refs

from .access import current_user, get_db, need_org
from .auth import _confirm
from .db import (Document, LoginCode, OAuthIdentity, Op, Org, OrgMember, PasswordReset, Project, ProjectMember,
                 Session, Snapshot,
                 User, Version, now)
from .storage import blob_key

router = APIRouter(prefix="/api")
log = logging.getLogger("openswx_collab")
DELETED_NAME = "Deleted user"


class ConfirmIn(BaseModel):
    current_password: str | None = None


# ------------------------------------------------------------------ your account
@router.delete("/me", status_code=204)
async def delete_account(body: ConfirmIn, request: Request, user: User = Depends(current_user),
                         db: AsyncSession = Depends(get_db)):
    me = await db.get(User, user.id)
    await _confirm(request, db, me, body.current_password)
    # an organisation must not be left without an owner
    alone = []
    for org_id, in (await db.execute(select(OrgMember.org_id).where(OrgMember.user_id == me.id,
                                                                   OrgMember.role == "owner"))).all():
        owners = (await db.execute(select(func.count()).select_from(OrgMember).where(
            OrgMember.org_id == org_id, OrgMember.role == "owner"))).scalar_one()
        if owners <= 1:
            org = await db.get(Org, org_id)
            alone.append({"id": org.id, "name": org.name})
    if alone:
        raise HTTPException(409, {"message": "you are the only owner of these organisations: make someone else "
                                  "an owner, or delete them, first", "orgs": alone})
    if me.is_server_admin:
        admins = (await db.execute(select(func.count()).select_from(User).where(
            User.is_server_admin.is_(True), User.disabled.is_(False)))).scalar_one()
        others = (await db.execute(select(func.count()).select_from(User).where(
            User.disabled.is_(False), User.id != me.id))).scalar_one()
        if admins <= 1 and others > 0:
            raise HTTPException(409, "you are this server's only admin; make someone else admin first "
                                "(openswx-collab-server create-admin)")
    orgs = [o for o, in (await db.execute(select(OrgMember.org_id).where(OrgMember.user_id == me.id))).all()]
    for table in (Session, OAuthIdentity, LoginCode, PasswordReset, OrgMember, ProjectMember):
        await db.execute(delete(table).where(table.user_id == me.id))
    me.email = f"deleted-{me.id}@deleted.invalid"  # (frees the address; .invalid can never be delivered to)
    me.name = DELETED_NAME
    me.password_hash = None
    me.email_verified = False
    me.is_server_admin = False
    me.disabled = True
    me.deleted_at = now()
    hooks = request.app.state.hooks
    for org_id in orgs:
        await hooks.member_removed(db, await db.get(Org, org_id), me.id)
    await db.commit()
    hub = request.app.state.hub
    hub.drop_account(me.id)
    hub.forget_user(me.id, DELETED_NAME)


@router.get("/me/export")
async def export_account(user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    """Everything the server holds about you as a person (your work belongs to your organisations: their
    owners export it)."""
    me = await db.get(User, user.id)
    orgs = [{"id": o.id, "name": o.name, "role": role, "since": since} for o, role, since in (await db.execute(
        select(Org, OrgMember.role, OrgMember.created_at).join(OrgMember, OrgMember.org_id == Org.id)
        .where(OrgMember.user_id == me.id))).all()]
    projects = [{"id": p.id, "name": p.name, "org_id": p.org_id, "role": role} for p, role in (await db.execute(
        select(Project, ProjectMember.role).join(ProjectMember, ProjectMember.project_id == Project.id)
        .where(ProjectMember.user_id == me.id))).all()]
    sessions = [{"created_at": s.created_at, "expires_at": s.expires_at, "label": s.label} for s in (
        await db.execute(select(Session).where(Session.user_id == me.id))).scalars()]
    providers = [p for p, in (await db.execute(select(OAuthIdentity.provider).where(OAuthIdentity.user_id == me.id))).all()]
    changes = [{"document_id": d, "seq": s, "time": t, "command": c} for d, s, t, c in (await db.execute(
        select(Op.document_id, Op.seq, Op.created_at, Op.command).where(Op.author_id == me.id)
        .order_by(Op.created_at))).all()]
    versions = [{"document_id": v.document_id, "name": v.name, "seq": v.seq, "time": v.created_at} for v in (
        await db.execute(select(Version).where(Version.author_id == me.id))).scalars()]
    return {"account": {"id": me.id, "email": me.email, "name": me.name, "created_at": me.created_at,
                        "email_verified": me.email_verified, "server_admin": me.is_server_admin,
                        "has_password": me.password_hash is not None, "sign_in_providers": providers},
            "organisations": orgs, "projects": projects, "sessions": sessions,
            "changes_you_made": changes, "versions_you_named": versions, "exported_at": now()}


# ------------------------------------------------------------------ organisations
class DeleteOrgIn(ConfirmIn):
    confirm_name: str


@router.delete("/orgs/{org_id}", status_code=204)
async def delete_org(org_id: str, body: DeleteOrgIn, request: Request, user: User = Depends(current_user),
                     db: AsyncSession = Depends(get_db)):
    await need_org(db, user, org_id, "owner")
    await _confirm(request, db, await db.get(User, user.id), body.current_password)
    org = await db.get(Org, org_id)
    if body.confirm_name.strip() != org.name:
        raise HTTPException(422, "type the organisation's name exactly to confirm")
    await request.app.state.hooks.before_org_delete(db, org)
    docs = list((await db.execute(select(Document.id).join(Project, Project.id == Document.project_id)
                                  .where(Project.org_id == org_id))).scalars())
    snapshots = list((await db.execute(select(Snapshot.key).where(Snapshot.document_id.in_(docs)))).scalars()) \
        if docs else []
    members = [u for u, in (await db.execute(select(OrgMember.user_id).where(OrgMember.org_id == org_id))).all()]
    await db.execute(delete(Org).where(Org.id == org_id))  # (projects, documents, history ... go with it)
    await db.commit()
    hub = request.app.state.hub
    for d in docs:
        await hub.close_document(d, "the organisation was deleted")
    store = request.app.state.storage
    try:  # (after the commit: a file left behind is wasted space, a row without its file is a broken document)
        for key in snapshots:
            await store.delete(key)
        await store.delete_prefix(f"orgs/{org_id}/")
    except Exception:  # noqa: BLE001 - the organisation is gone either way
        log.exception("could not remove all stored files of deleted organisation %s", org_id)
    for uid in members:
        await request.app.state.hooks.member_removed(db, org, uid)


@router.get("/orgs/{org_id}/export")
async def export_org(org_id: str, request: Request, user: User = Depends(current_user),
                     db: AsyncSession = Depends(get_db)):
    """A zip of everything in the organisation: each document as it is now (openswx's JSON), its full
    history and named versions, the projects and members, and the stored blobs. Owners only."""
    await need_org(db, user, org_id, "owner")
    org = await db.get(Org, org_id)
    hub, store = request.app.state.hub, request.app.state.storage
    names = dict((await db.execute(select(User.id, User.name))).all())
    fd, path = tempfile.mkstemp(prefix="oswx-export-", suffix=".zip")
    os.close(fd)
    try:
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            def put(name: str, value: Any) -> None:
                z.writestr(name, json.dumps(value, indent=1, ensure_ascii=False))
            members = [{"user": names.get(u), "email": e, "role": r} for u, e, r in (await db.execute(
                select(OrgMember.user_id, User.email, OrgMember.role).join(User, User.id == OrgMember.user_id)
                .where(OrgMember.org_id == org_id))).all()]
            projects = (await db.execute(select(Project).where(Project.org_id == org_id))).scalars().all()
            put("organisation.json", {"id": org.id, "name": org.name, "created_at": org.created_at,
                                      "members": members, "exported_at": now(),
                                      "projects": [{"id": p.id, "name": p.name} for p in projects]})
            blobs: set[str] = set()
            for p in projects:
                docs = (await db.execute(select(Document).where(Document.project_id == p.id,
                                                                Document.deleted_at.is_(None)))).scalars().all()
                for d in docs:
                    base = f"projects/{p.id}/{d.id}"
                    room = await hub.room(d.id)
                    content = await room.state_at(room.head)
                    blobs |= blob_refs(content)
                    put(f"{base}/document.json", {"id": d.id, "name": d.name, "kind": d.kind, "seq": room.head,
                                                  "format_version": d.format_version, "content": content})
                    ops = (await db.execute(select(Op).where(Op.document_id == d.id).order_by(Op.seq))).scalars()
                    z.writestr(f"{base}/history.jsonl", "".join(json.dumps({
                        "seq": o.seq, "author": names.get(o.author_id), "time": o.created_at, "command": o.command,
                        "ops": o.ops, "reverts": o.reverts}, ensure_ascii=False) + "\n" for o in ops))
                    for o_ops, in (await db.execute(select(Op.ops).where(Op.document_id == d.id))).all():
                        blobs |= blob_refs(o_ops)
                    put(f"{base}/versions.json", [{"name": v.name, "seq": v.seq, "note": v.note,
                                                   "author": names.get(v.author_id), "time": v.created_at}
                                                  for v in (await db.execute(select(Version).where(
                                                      Version.document_id == d.id))).scalars()])
            for sha in sorted(blobs):
                data = await store.get(blob_key(org_id, sha))
                if data is not None:
                    z.writestr(f"blobs/{sha}", data)
    except BaseException:
        os.unlink(path)
        raise
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in org.name)[:60] or "organisation"
    return FileResponse(path, media_type="application/zip", filename=f"{safe}-export.zip",
                        background=BackgroundTask(os.unlink, path))
