"""Organisations, members, invites, projects and project members."""
from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .access import current_user, get_db, need_org, need_project, org_role, visible_projects
from .db import Invite, Org, OrgMember, Project, ProjectMember, User, now
from .security import ORG_ROLES, clean_email, new_token, token_hash

router = APIRouter(prefix="/api")


def user_out(u: User) -> dict:
    return {"id": u.id, "email": u.email, "name": u.name}


async def member_count(db: AsyncSession, org_id: str) -> int:
    return (await db.execute(select(func.count()).select_from(OrgMember).where(OrgMember.org_id == org_id))).scalar_one()


async def _owners(db: AsyncSession, org_id: str) -> int:
    return (await db.execute(select(func.count()).select_from(OrgMember).where(
        OrgMember.org_id == org_id, OrgMember.role == "owner"))).scalar_one()


# ------------------------------------------------------------------ invites (shared with sign-up)
async def find_invite(db: AsyncSession, token: str | None, email: str | None = None) -> Invite | None:
    """A usable invite for this token (and, where the invite names one, this email address)."""
    if not token:
        return None
    inv = (await db.execute(select(Invite).where(Invite.token_hash == token_hash(token)))).scalar_one_or_none()
    if inv is None or inv.accepted_by is not None or inv.expires_at < now():
        return None
    if inv.email and email is not None and inv.email != email:
        return None
    return inv


async def accept_invite(request: Request, db: AsyncSession, inv: Invite, user: User) -> None:
    """Make `user` a member as the invite says. Does not commit.

    Two people accepting at the same moment must not both get in on one invite, or both take the last
    seat: the invite is claimed in one statement, and the organisation's row is locked while its members
    are counted (SQLite has one writer at a time, which the claim already made us)."""
    claimed = await db.execute(update(Invite).where(Invite.id == inv.id, Invite.accepted_by.is_(None))
                               .values(accepted_by=user.id))
    if claimed.rowcount != 1:
        raise HTTPException(404, "this invite has just been used")
    org = (await db.execute(select(Org).where(Org.id == inv.org_id).with_for_update())).scalar_one()
    existing = await db.get(OrgMember, (inv.org_id, user.id))
    if existing is None:
        ok, why = await request.app.state.hooks.may_add_member(db, org, await member_count(db, inv.org_id))
        if not ok:
            raise HTTPException(402, why or "this organisation has no free seats")
        db.add(OrgMember(org_id=inv.org_id, user_id=user.id, role=inv.role))
        await request.app.state.hooks.member_added(db, org, user)
    for grant in inv.projects or []:
        project = await db.get(Project, grant.get("project_id"))
        if project is None or project.org_id != inv.org_id:
            continue
        pm = await db.get(ProjectMember, (project.id, user.id))
        if pm is None:
            db.add(ProjectMember(project_id=project.id, user_id=user.id, role=grant.get("role", "viewer")))
    inv.accepted_by = user.id


# ------------------------------------------------------------------ organisations
class OrgIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


@router.get("/orgs")
async def list_orgs(user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    rows = (await db.execute(select(Org, OrgMember.role).join(OrgMember, OrgMember.org_id == Org.id)
                             .where(OrgMember.user_id == user.id).order_by(Org.name))).all()
    return [{"id": o.id, "name": o.name, "role": role, "members": await member_count(db, o.id)} for o, role in rows]


@router.post("/orgs", status_code=201)
async def create_org(body: OrgIn, request: Request, user: User = Depends(current_user),
                     db: AsyncSession = Depends(get_db)):
    if not (user.is_server_admin or request.app.state.settings.open_registration):
        raise HTTPException(403, "only the server admin can create organisations on this server")
    org = Org(name=body.name.strip())
    db.add(org)
    await db.flush()
    db.add(OrgMember(org_id=org.id, user_id=user.id, role="owner"))
    await db.commit()
    return {"id": org.id, "name": org.name, "role": "owner", "members": 1}


@router.patch("/orgs/{org_id}")
async def rename_org(org_id: str, body: OrgIn, user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    await need_org(db, user, org_id, "owner")
    org = await db.get(Org, org_id)
    org.name = body.name.strip()
    await db.commit()
    return {"id": org.id, "name": org.name}


@router.get("/orgs/{org_id}/members")
async def list_members(org_id: str, user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    await need_org(db, user, org_id)
    rows = (await db.execute(select(User, OrgMember.role).join(OrgMember, OrgMember.user_id == User.id)
                             .where(OrgMember.org_id == org_id).order_by(User.name))).all()
    return [{**user_out(u), "role": role} for u, role in rows]


class RoleIn(BaseModel):
    role: Literal["owner", "admin", "member"]


@router.patch("/orgs/{org_id}/members/{user_id}")
async def set_member_role(org_id: str, user_id: str, body: RoleIn, request: Request,
                          user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    mine = await need_org(db, user, org_id, "admin")
    m = await db.get(OrgMember, (org_id, user_id))
    if m is None:
        raise HTTPException(404, "not a member")
    if (body.role == "owner" or m.role == "owner") and mine != "owner":
        raise HTTPException(403, "only an owner can change who is an owner")
    if m.role == "owner" and body.role != "owner" and await _owners(db, org_id) <= 1:
        raise HTTPException(409, "an organisation needs at least one owner")
    m.role = body.role
    await db.commit()
    await request.app.state.hub.drop_user(user_id, org_id)  # (reconnecting picks up the new role)
    return {"user_id": user_id, "role": m.role}


@router.delete("/orgs/{org_id}/members/{user_id}", status_code=204)
async def remove_member(org_id: str, user_id: str, request: Request, user: User = Depends(current_user),
                        db: AsyncSession = Depends(get_db)):
    mine = await need_org(db, user, org_id)
    m = await db.get(OrgMember, (org_id, user_id))
    if m is None:
        raise HTTPException(404, "not a member")
    if user_id != user.id and ORG_ROLES[mine] < ORG_ROLES["admin"]:
        raise HTTPException(403, "needs the organisation role admin")
    if m.role == "owner":
        if mine != "owner":
            raise HTTPException(403, "only an owner can remove an owner")
        if await _owners(db, org_id) <= 1:
            raise HTTPException(409, "an organisation needs at least one owner")
    project_ids = select(Project.id).where(Project.org_id == org_id)
    await db.execute(delete(ProjectMember).where(ProjectMember.user_id == user_id,
                                                 ProjectMember.project_id.in_(project_ids)))
    await db.delete(m)
    await request.app.state.hooks.member_removed(db, await db.get(Org, org_id), user_id)
    await db.commit()
    await request.app.state.hub.drop_user(user_id, org_id)  # (they lose their open documents at once)


# ------------------------------------------------------------------ invites
class Grant(BaseModel):
    project_id: str
    role: Literal["owner", "editor", "viewer"] = "editor"


class InviteIn(BaseModel):
    email: str | None = None
    role: Literal["admin", "member"] = "member"
    projects: list[Grant] = Field(default_factory=list)
    days: int = Field(default=7, ge=1, le=90)


def _invite_out(inv: Invite) -> dict:
    return {"id": inv.id, "email": inv.email, "role": inv.role, "projects": inv.projects,
            "expires_at": inv.expires_at, "accepted_by": inv.accepted_by}


@router.post("/orgs/{org_id}/invites", status_code=201)
async def create_invite(org_id: str, body: InviteIn, request: Request, user: User = Depends(current_user),
                        db: AsyncSession = Depends(get_db)):
    await need_org(db, user, org_id, "admin")
    email = None
    if body.email:
        email = clean_email(body.email)
        if email is None:
            raise HTTPException(422, "not an email address")
    for g in body.projects:
        p = await db.get(Project, g.project_id)
        if p is None or p.org_id != org_id:
            raise HTTPException(422, f"no project {g.project_id} in this organisation")
    org = await db.get(Org, org_id)
    ok, why = await request.app.state.hooks.may_add_member(db, org, await member_count(db, org_id))
    if not ok:
        raise HTTPException(402, why or "this organisation has no free seats")
    token = new_token()
    inv = Invite(token_hash=token_hash(token), org_id=org_id, email=email, role=body.role,
                 projects=[g.model_dump() for g in body.projects], created_by=user.id,
                 expires_at=now() + body.days * 86400)
    db.add(inv)
    await db.commit()
    base = request.app.state.settings.public_url.rstrip("/")
    return {**_invite_out(inv), "token": token, "url": f"{base}/invite/{token}"}  # (the token is shown once)


@router.get("/orgs/{org_id}/invites")
async def list_invites(org_id: str, user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    await need_org(db, user, org_id, "admin")
    rows = (await db.execute(select(Invite).where(Invite.org_id == org_id, Invite.accepted_by.is_(None),
                                                  Invite.expires_at > now()).order_by(Invite.created_at))).scalars()
    return [_invite_out(i) for i in rows]


@router.delete("/orgs/{org_id}/invites/{invite_id}", status_code=204)
async def revoke_invite(org_id: str, invite_id: str, user: User = Depends(current_user),
                        db: AsyncSession = Depends(get_db)):
    await need_org(db, user, org_id, "admin")
    inv = await db.get(Invite, invite_id)
    if inv is None or inv.org_id != org_id:
        raise HTTPException(404, "no such invite")
    await db.delete(inv)
    await db.commit()


class AcceptIn(BaseModel):
    token: str


@router.post("/invites/accept")
async def accept(body: AcceptIn, request: Request, user: User = Depends(current_user),
                 db: AsyncSession = Depends(get_db)):
    inv = await find_invite(db, body.token, user.email)
    if inv is None:
        raise HTTPException(404, "this invite is not valid (used, expired, or for another email address)")
    await accept_invite(request, db, inv, user)
    await db.commit()
    org = await db.get(Org, inv.org_id)
    return {"org": {"id": org.id, "name": org.name}, "role": await org_role(db, user, org.id)}


# ------------------------------------------------------------------ projects
class ProjectIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


@router.get("/orgs/{org_id}/projects")
async def list_projects(org_id: str, user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    role = await need_org(db, user, org_id)
    return [{"id": p.id, "name": p.name, "org_id": p.org_id, "role": r}
            for p, r in await visible_projects(db, user, org_id, role)]


@router.post("/orgs/{org_id}/projects", status_code=201)
async def create_project(org_id: str, body: ProjectIn, user: User = Depends(current_user),
                         db: AsyncSession = Depends(get_db)):
    await need_org(db, user, org_id)  # (any member can start a project, and owns it)
    p = Project(org_id=org_id, name=body.name.strip())
    db.add(p)
    await db.flush()
    db.add(ProjectMember(project_id=p.id, user_id=user.id, role="owner"))
    await db.commit()
    return {"id": p.id, "name": p.name, "org_id": org_id, "role": "owner"}


@router.patch("/projects/{project_id}")
async def rename_project(project_id: str, body: ProjectIn, user: User = Depends(current_user),
                         db: AsyncSession = Depends(get_db)):
    p, _ = await need_project(db, user, project_id, "owner")
    p.name = body.name.strip()
    await db.commit()
    return {"id": p.id, "name": p.name, "org_id": p.org_id}


@router.delete("/projects/{project_id}", status_code=204)
async def delete_project(project_id: str, request: Request, user: User = Depends(current_user),
                         db: AsyncSession = Depends(get_db)):
    p, _ = await need_project(db, user, project_id, "owner")
    from .assemblies import used_outside, visible  # noqa: PLC0415
    from .db import Document  # noqa: PLC0415
    # As for one document (documents.delete_document): take the locks by marking its documents deleted,
    # then look for users outside, in one transaction.
    doc_ids = list((await db.execute(select(Document.id).where(Document.project_id == p.id))).scalars())
    await db.execute(update(Document).where(Document.project_id == p.id, Document.deleted_at.is_(None))
                     .values(deleted_at=now()))
    outside = await used_outside(db, p.id)
    if outside:  # (assemblies elsewhere are built from parts in here)
        shown, hidden = await visible(db, user, outside)
        await db.rollback()
        raise HTTPException(409, {"message": "assemblies in other projects use parts of this project",
                                  "assemblies": shown, "in_projects_you_cannot_open": hidden})
    await db.delete(p)
    await db.commit()
    for d in doc_ids:
        await request.app.state.hub.close_document(d, "the project was deleted")


@router.get("/projects/{project_id}/members")
async def list_project_members(project_id: str, user: User = Depends(current_user),
                               db: AsyncSession = Depends(get_db)):
    await need_project(db, user, project_id)
    rows = (await db.execute(select(User, ProjectMember.role).join(ProjectMember, ProjectMember.user_id == User.id)
                             .where(ProjectMember.project_id == project_id).order_by(User.name))).all()
    return [{**user_out(u), "role": role} for u, role in rows]


class ProjectRoleIn(BaseModel):
    role: Literal["owner", "editor", "viewer"]


@router.put("/projects/{project_id}/members/{user_id}")
async def set_project_member(project_id: str, user_id: str, body: ProjectRoleIn, request: Request,
                             user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    p, _ = await need_project(db, user, project_id, "owner")
    if await db.get(OrgMember, (p.org_id, user_id)) is None:
        raise HTTPException(422, "that person is not in the organisation; invite them first")
    pm = await db.get(ProjectMember, (project_id, user_id))
    if pm is None:
        db.add(ProjectMember(project_id=project_id, user_id=user_id, role=body.role))
    else:
        pm.role = body.role
    await db.commit()
    await request.app.state.hub.drop_user(user_id, p.org_id)  # (reconnecting picks up the new role)
    return {"user_id": user_id, "role": body.role}


@router.delete("/projects/{project_id}/members/{user_id}", status_code=204)
async def remove_project_member(project_id: str, user_id: str, request: Request, user: User = Depends(current_user),
                                db: AsyncSession = Depends(get_db)):
    p, _ = await need_project(db, user, project_id, "owner")
    pm = await db.get(ProjectMember, (project_id, user_id))
    if pm is None:
        raise HTTPException(404, "not a member of the project")
    await db.delete(pm)
    await db.commit()
    await request.app.state.hub.drop_user(user_id, p.org_id)
