"""Who is asking, and what they may do.

Organisation roles: owner > admin > member. Project roles: owner > editor > viewer.
Organisation owners and admins are owners of every project; everyone else has the role they were given
in a project, or cannot see it at all. Files inherit from their project.
"""
from __future__ import annotations

from typing import AsyncIterator

from fastapi import Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .db import Document, OrgMember, Project, ProjectMember, Session, User, now
from .security import ORG_ROLES, PROJECT_ROLES, token_hash


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.db() as s:
        yield s


async def user_for_token(db: AsyncSession, token: str) -> User | None:
    if not token:
        return None
    sess = await db.get(Session, token_hash(token))
    if sess is None or sess.expires_at < now():
        return None
    user = await db.get(User, sess.user_id)
    return None if user is None or user.disabled else user


def bearer(request: Request) -> str:
    auth = request.headers.get("authorization", "")
    return auth[7:].strip() if auth.lower().startswith("bearer ") else ""


async def current_user(request: Request, db: AsyncSession = Depends(get_db)) -> User:
    user = await user_for_token(db, bearer(request))
    if user is None:
        raise HTTPException(401, "sign in first")
    return user


async def org_role(db: AsyncSession, user: User, org_id: str) -> str | None:
    m = await db.get(OrgMember, (org_id, user.id))
    return m.role if m else None


async def need_org(db: AsyncSession, user: User, org_id: str, minimum: str = "member") -> str:
    role = await org_role(db, user, org_id)
    if role is None:
        raise HTTPException(404, "no such organisation")
    if ORG_ROLES[role] < ORG_ROLES[minimum]:
        raise HTTPException(403, f"needs the organisation role {minimum}")
    return role


async def project_role(db: AsyncSession, user: User, project: Project) -> str | None:
    role = await org_role(db, user, project.org_id)
    if role is None:
        return None
    if ORG_ROLES[role] >= ORG_ROLES["admin"]:
        return "owner"
    pm = await db.get(ProjectMember, (project.id, user.id))
    return pm.role if pm else None


async def need_project(db: AsyncSession, user: User, project_id: str, minimum: str = "viewer") -> tuple[Project, str]:
    project = await db.get(Project, project_id)
    role = await project_role(db, user, project) if project else None
    if role is None:
        raise HTTPException(404, "no such project")  # (not 403: a project you cannot see does not exist for you)
    if PROJECT_ROLES[role] < PROJECT_ROLES[minimum]:
        raise HTTPException(403, f"needs the project role {minimum}")
    return project, role


async def need_document(db: AsyncSession, user: User, document_id: str,
                        minimum: str = "viewer") -> tuple[Document, Project, str]:
    doc = await db.get(Document, document_id)
    if doc is None or doc.deleted_at is not None:
        raise HTTPException(404, "no such document")
    try:
        project, role = await need_project(db, user, doc.project_id, minimum)
    except HTTPException as exc:
        raise HTTPException(exc.status_code, "no such document" if exc.status_code == 404 else exc.detail) from exc
    return doc, project, role


async def visible_projects(db: AsyncSession, user: User, org_id: str, role: str) -> list[tuple[Project, str]]:
    projects = (await db.execute(select(Project).where(Project.org_id == org_id).order_by(Project.name))).scalars().all()
    if ORG_ROLES[role] >= ORG_ROLES["admin"]:
        return [(p, "owner") for p in projects]
    mine = {pm.project_id: pm.role for pm in (await db.execute(
        select(ProjectMember).where(ProjectMember.user_id == user.id))).scalars()}
    return [(p, mine[p.id]) for p in projects if p.id in mine]
