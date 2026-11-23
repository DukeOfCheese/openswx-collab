"""Assemblies: components that use parts by reference.

A cloud assembly does not carry copies of its parts. Each component says which part document it uses
and at which point of that part's history:

    {"id": "c1", ..., "ref": {"document": "<part document id>", "seq": 42, "version": "Rev A"}}

`seq` is what counts (a state of a document never changes); `version` is the label the person picked,
kept for display. Someone editing the part therefore never changes an assembly. The assembly's users see
that something newer exists (GET .../references, and a `reference.update` message when a part gets a new
named version) and move a component to it by changing its `ref` like any other edit. Checking which mates
would break is the client's job, since it needs geometry; the server says what changed in between
(GET /documents/{part}/history?after=<pinned>&before=<newer + 1>).
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from openswx_collab_protocol.ops import OpError

from .access import current_user, get_db, need_document, project_role
from .db import AssemblyRef, Document, Op, Project, User, Version

router = APIRouter(prefix="/api")


def _ref(component: dict) -> tuple[str, int]:
    """(part document id, seq) of a component, or OpError."""
    cid = component.get("id")
    if "part" in component:
        raise OpError(f"component {cid}: a cloud assembly refers to its parts ('ref'); it does not carry copies")
    ref = component.get("ref")
    if not isinstance(ref, dict) or not isinstance(ref.get("document"), str) or not ref["document"] \
            or isinstance(ref.get("seq"), bool) or not isinstance(ref.get("seq"), int) or ref["seq"] < 0:
        raise OpError(f"component {cid} needs ref: {{document: <part document id>, seq: <number>}}")
    if ref.get("version") is not None and not isinstance(ref["version"], str):
        raise OpError(f"component {cid}: ref.version is a version name")
    return ref["document"], ref["seq"]


def ref_rows(content: dict) -> dict[str, tuple[str, int]]:
    """component id -> (part document id, seq) for a whole assembly."""
    return {c["id"]: _ref(c) for c in content.get("components") or []}


async def check_refs(db: AsyncSession, org_id: str, user: User, components: list[dict],
                     access_for: set[str] | None = None, allow_deleted: bool = False) -> None:
    """Each of these components must use a part of the same organisation, at a state that part has.
    For the components in `access_for` (all of them when None) `user` must also be able to open the part:
    that is asked when someone chooses a part or a state of it, not every time a component that already
    uses it is moved, removed or put back. `allow_deleted`: for undo and restore, which may bring back a
    component whose part has since been deleted. Raises OpError; its text names a part only to someone
    who can open it."""
    seen: dict[tuple[str, bool], Document | None] = {}
    for c in components:
        part_id, seq = _ref(c)
        need = access_for is None or c["id"] in access_for
        if (part_id, need) not in seen:
            doc = await db.get(Document, part_id)
            live = doc is not None and (doc.deleted_at is None or allow_deleted)
            project = await db.get(Project, doc.project_id) if live else None
            ok = project is not None and project.org_id == org_id
            if ok and need:
                ok = await project_role(db, user, project) is not None
            seen[(part_id, need)] = doc if ok else None  # (one answer for "missing" and "not yours": no probing)
        doc = seen[(part_id, need)]
        if doc is None:
            raise OpError(f"component {c['id']}: there is no part {part_id} you can use")
        label = doc.name if need else "that document"
        if doc.kind != "part":
            raise OpError(f"component {c['id']}: {label} is not a part")
        if seq > doc.head_seq:
            raise OpError(f"component {c['id']}: {label} has no state {seq}" if need else
                          f"component {c['id']}: that state does not exist")


async def used_outside(db: AsyncSession, project_id: str) -> list[Document]:
    """Assemblies in other projects that use a part of this project."""
    parts = select(Document.id).where(Document.project_id == project_id)
    ids = select(AssemblyRef.assembly_id).where(AssemblyRef.part_id.in_(parts)).distinct()
    return list((await db.execute(select(Document).where(
        Document.id.in_(ids), Document.deleted_at.is_(None), Document.project_id != project_id)
        .order_by(Document.name))).scalars())


async def visible(db: AsyncSession, user: User, docs: list[Document]) -> tuple[list[dict], int]:
    """(the documents `user` can open as {id, name}, how many they cannot)."""
    shown, hidden = [], 0
    for d in docs:
        project = await db.get(Project, d.project_id)
        if project is not None and await project_role(db, user, project) is not None:
            shown.append({"id": d.id, "name": d.name})
        else:
            hidden += 1
    return shown, hidden


async def used_in(db: AsyncSession, part_id: str) -> list[Document]:
    """The (not deleted) assemblies that use a part."""
    ids = select(AssemblyRef.assembly_id).where(AssemblyRef.part_id == part_id).distinct()
    return list((await db.execute(select(Document).where(Document.id.in_(ids), Document.deleted_at.is_(None))
                                  .order_by(Document.name))).scalars())


async def where_pinned_older(db: AsyncSession, part_id: str, seq: int) -> dict[str, list[str]]:
    """assembly id -> components that use `part_id` at a state before `seq`."""
    out: dict[str, list[str]] = {}
    for assembly_id, component_id in (await db.execute(
            select(AssemblyRef.assembly_id, AssemblyRef.component_id)
            .where(AssemblyRef.part_id == part_id, AssemblyRef.seq < seq).order_by(AssemblyRef.component_id))).all():
        out.setdefault(assembly_id, []).append(component_id)
    return out


@router.get("/documents/{document_id}/references")
async def references(document_id: str, user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    """What each component of an assembly is pinned to, and whether its part has moved on.

    `update_available`: the part has a named version newer than the pinned state (`latest_version`).
    `newer_edits`: the part has been edited since (possibly without anyone naming a version).
    """
    doc, _, _ = await need_document(db, user, document_id)
    if doc.kind != "assembly":
        raise HTTPException(422, "only assemblies have references")
    rows = (await db.execute(select(AssemblyRef).where(AssemblyRef.assembly_id == document_id)
                             .order_by(AssemblyRef.component_id))).scalars().all()
    parts: dict[str, dict] = {}
    for part_id in {r.part_id for r in rows}:
        part = await db.get(Document, part_id)
        project = await db.get(Project, part.project_id) if part is not None else None
        versions = (await db.execute(select(Version).where(Version.document_id == part_id)
                                     .order_by(Version.seq.desc(), Version.created_at.desc()))).scalars().all() \
            if part is not None else []
        parts[part_id] = {"part": part, "versions": versions,
                          "accessible": project is not None and await project_role(db, user, project) is not None}
    out = []
    for r in rows:
        info = parts[r.part_id]
        part, versions = info["part"], info["versions"]
        if part is None or not info["accessible"]:
            # in a project this person cannot open: they learn that it exists and nothing about it
            out.append({"component": r.component_id, "document": {"id": r.part_id, "accessible": False},
                        "pinned": {"seq": r.seq}})
            continue
        latest = versions[0] if versions else None
        target = latest.seq if latest is not None and latest.seq > r.seq else part.head_seq
        changes = (await db.execute(select(func.count()).select_from(Op).where(
            Op.document_id == part.id, Op.seq > r.seq, Op.seq <= target))).scalar_one()
        out.append({
            "component": r.component_id,
            "document": {"id": part.id, "name": part.name, "project_id": part.project_id, "accessible": True,
                         "deleted": part.deleted_at is not None},
            "pinned": {"seq": r.seq, "versions": [v.name for v in versions if v.seq == r.seq]},
            "head_seq": part.head_seq,
            "latest_version": {"id": latest.id, "name": latest.name, "seq": latest.seq} if latest else None,
            "update_available": latest is not None and latest.seq > r.seq,
            "newer_edits": part.head_seq > r.seq,
            "changes_since_pinned": changes,
        })
    return out


@router.get("/documents/{document_id}/used-in")
async def where_used(document_id: str, user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    """The assemblies that use this part and what each is pinned to (only assemblies the asker can open)."""
    await need_document(db, user, document_id)
    out = []
    for asm in await used_in(db, document_id):
        project = await db.get(Project, asm.project_id)
        if project is None or await project_role(db, user, project) is None:
            continue
        comps = (await db.execute(select(AssemblyRef.component_id, AssemblyRef.seq).where(
            AssemblyRef.assembly_id == asm.id, AssemblyRef.part_id == document_id)
            .order_by(AssemblyRef.component_id))).all()
        out.append({"document": {"id": asm.id, "name": asm.name, "project_id": asm.project_id},
                    "components": [{"component": c, "seq": s} for c, s in comps]})
    return out
