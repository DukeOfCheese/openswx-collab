"""Database tables (SQLAlchemy, async). SQLite for small self-hosted servers, Postgres otherwise.

Times are seconds since the epoch (float). Ids are random hex strings.
"""
from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import JSON, Boolean, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, event
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SCHEMA_VERSION = 1


def new_id() -> str:
    return uuid.uuid4().hex


def now() -> float:
    return time.time()


class Base(DeclarativeBase):
    type_annotation_map = {dict[str, Any]: JSON, list[Any]: JSON}


class Meta(Base):
    __tablename__ = "meta"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(Text)


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)  # lower case
    name: Mapped[str] = mapped_column(String(200))
    password_hash: Mapped[str | None] = mapped_column(Text, nullable=True)  # None: signs in with Google/GitHub only
    is_server_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    disabled: Mapped[bool] = mapped_column(Boolean, default=False)
    # True when something other than the person's own word says the address is theirs: a provider vouched
    # for it, or the operator made the account. Only then may a provider sign-in join the account by address.
    # (An invite proves nothing: whoever makes an invite also holds its link.)
    email_verified: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[float] = mapped_column(Float, default=now)


class OAuthIdentity(Base):
    __tablename__ = "oauth_identities"
    provider: Mapped[str] = mapped_column(String(32), primary_key=True)
    subject: Mapped[str] = mapped_column(String(255), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)


class Session(Base):
    __tablename__ = "sessions"
    token_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[float] = mapped_column(Float, default=now)
    expires_at: Mapped[float] = mapped_column(Float)
    label: Mapped[str] = mapped_column(String(200), default="")


class OAuthState(Base):
    """A sign-in through Google/GitHub that has been started and not yet come back."""
    __tablename__ = "oauth_states"
    state_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    provider: Mapped[str] = mapped_column(String(32))
    redirect_uri: Mapped[str] = mapped_column(Text)
    invite: Mapped[str | None] = mapped_column(Text, nullable=True)
    challenge: Mapped[str] = mapped_column(String(64), default="")
    link_user_id: Mapped[str | None] = mapped_column(String(32), nullable=True)  # set: attach to this account
    expires_at: Mapped[float] = mapped_column(Float)


class LoginCode(Base):
    """A one-time code handed to the desktop app after a browser sign-in; exchanged for a session token."""
    __tablename__ = "login_codes"
    code_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    challenge: Mapped[str] = mapped_column(String(64), default="")  # sha256 of the secret the app kept
    # set when the code is for adding a provider to user_id (not for signing in): what to attach
    link_provider: Mapped[str | None] = mapped_column(String(32), nullable=True)
    link_subject: Mapped[str | None] = mapped_column(String(255), nullable=True)
    link_email: Mapped[str | None] = mapped_column(String(320), nullable=True)
    expires_at: Mapped[float] = mapped_column(Float)


class Org(Base):
    __tablename__ = "orgs"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(200))
    created_at: Mapped[float] = mapped_column(Float, default=now)


class OrgMember(Base):
    __tablename__ = "org_members"
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id", ondelete="CASCADE"), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True, index=True)
    role: Mapped[str] = mapped_column(String(16))  # owner | admin | member
    created_at: Mapped[float] = mapped_column(Float, default=now)


class Invite(Base):
    __tablename__ = "invites"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id", ondelete="CASCADE"), index=True)
    email: Mapped[str | None] = mapped_column(String(320), nullable=True)  # None: whoever has the link
    role: Mapped[str] = mapped_column(String(16), default="member")
    projects: Mapped[list[Any]] = mapped_column(default=list)  # [{"project_id", "role"}]
    created_by: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[float] = mapped_column(Float, default=now)
    expires_at: Mapped[float] = mapped_column(Float)
    accepted_by: Mapped[str | None] = mapped_column(String(32), nullable=True)


class Project(Base):
    __tablename__ = "projects"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(200))
    created_at: Mapped[float] = mapped_column(Float, default=now)


class ProjectMember(Base):
    __tablename__ = "project_members"
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True, index=True)
    role: Mapped[str] = mapped_column(String(16))  # owner | editor | viewer


class Document(Base):
    __tablename__ = "documents"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(200))
    kind: Mapped[str] = mapped_column(String(16), default="part")
    format_version: Mapped[int | None] = mapped_column(Integer, nullable=True)  # openswx file-format version
    head_seq: Mapped[int] = mapped_column(Integer, default=0)
    created_by: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[float] = mapped_column(Float, default=now)
    updated_at: Mapped[float] = mapped_column(Float, default=now)
    deleted_at: Mapped[float | None] = mapped_column(Float, nullable=True)


class Op(Base):
    """One accepted change: the edit history of a document."""
    __tablename__ = "ops"
    document_id: Mapped[str] = mapped_column(ForeignKey("documents.id", ondelete="CASCADE"), primary_key=True)
    seq: Mapped[int] = mapped_column(Integer, primary_key=True)
    author_id: Mapped[str] = mapped_column(String(32))
    client_id: Mapped[str | None] = mapped_column(String(128), nullable=True)  # "<user id>.<the app's id>"
    change_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[float] = mapped_column(Float, default=now)
    command: Mapped[str] = mapped_column(String(100), default="")
    args: Mapped[dict[str, Any] | None] = mapped_column(nullable=True)
    ops: Mapped[list[Any]] = mapped_column()
    inverse_ops: Mapped[list[Any]] = mapped_column()
    targets: Mapped[list[Any]] = mapped_column()
    reverts: Mapped[int | None] = mapped_column(Integer, nullable=True)  # the seq this change undid
    reverted_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    broke: Mapped[dict[str, Any] | None] = mapped_column(nullable=True)  # feature id -> rebuild error it caused
    __table_args__ = (UniqueConstraint("document_id", "client_id", "change_id", name="uq_op_change"),)


class Snapshot(Base):
    __tablename__ = "snapshots"
    document_id: Mapped[str] = mapped_column(ForeignKey("documents.id", ondelete="CASCADE"), primary_key=True)
    seq: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(Text)  # where the gzipped JSON is in the blob store
    size: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[float] = mapped_column(Float, default=now)


class Version(Base):
    """A named point in a document's history."""
    __tablename__ = "versions"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    document_id: Mapped[str] = mapped_column(ForeignKey("documents.id", ondelete="CASCADE"), index=True)
    seq: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String(200))
    note: Mapped[str] = mapped_column(Text, default="")
    author_id: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[float] = mapped_column(Float, default=now)
    __table_args__ = (UniqueConstraint("document_id", "name", name="uq_version_name"),)


class AssemblyRef(Base):
    """Which part, at which point of its history, a component of an assembly uses. Kept in step with the
    assembly's content on every change, so "where is this part used" needs no document to be opened."""
    __tablename__ = "assembly_refs"
    assembly_id: Mapped[str] = mapped_column(ForeignKey("documents.id", ondelete="CASCADE"), primary_key=True)
    component_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    part_id: Mapped[str] = mapped_column(String(32), index=True)
    seq: Mapped[int] = mapped_column(Integer)


Index("ix_ops_doc_seq", Op.document_id, Op.seq)


def make_engine(url: str) -> AsyncEngine:
    if url.startswith("sqlite"):
        path = url.split("///", 1)[-1]
        if path and path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        engine = create_async_engine(url, connect_args={"timeout": 30})

        @event.listens_for(engine.sync_engine, "connect")
        def _pragmas(dbapi_conn, _record):  # (foreign keys are off by default in SQLite; WAL lets readers run)
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute("PRAGMA journal_mode=WAL")
            cur.close()
        return engine
    return create_async_engine(url, pool_pre_ping=True)


async def init_db(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        row = await s.get(Meta, "schema_version")
        if row is None:
            s.add(Meta(key="schema_version", value=str(SCHEMA_VERSION)))
            await s.commit()
        elif int(row.value) > SCHEMA_VERSION:
            raise RuntimeError(f"the database was written by a newer server (schema {row.value})")
