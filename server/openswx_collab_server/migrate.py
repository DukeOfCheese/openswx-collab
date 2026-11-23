"""Database schema: created and upgraded with Alembic migrations (in `migrations/versions/`).

The server upgrades the database when it starts (OSWX_COLLAB_AUTO_MIGRATE, on by default). With it off,
a server that finds an older database refuses to start and says to run `openswx-collab-server migrate`
(take a backup first). A database written by a newer server is always refused.

Databases made before migrations existed (they have the tables but no `alembic_version`; only ever made
by development versions before the first release) are taken to be at the first revision.
"""
from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncEngine

BASELINE = "0001"


class SchemaError(RuntimeError):
    pass


def config(connection=None) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(Path(__file__).with_name("migrations")))
    if connection is not None:
        cfg.attributes["connection"] = connection
    return cfg


def head() -> str:
    return ScriptDirectory.from_config(config()).get_current_head()


def _state(connection) -> tuple[str | None, bool]:
    """(revision the database is at, whether it has the server's tables at all)."""
    current = MigrationContext.configure(connection).get_current_revision()
    has_tables = inspect(connection).has_table("users")
    return current, has_tables


def _upgrade(connection, allow: bool) -> str:
    current, has_tables = _state(connection)
    known = {r.revision for r in ScriptDirectory.from_config(config()).walk_revisions()}
    if current is not None and current not in known:
        raise SchemaError(f"the database is at schema revision {current}, which this server does not know: "
                          "it was written by a newer version of openswx-collab")
    target = head()
    if current == target:
        return target
    if not allow and has_tables:
        raise SchemaError(f"the database schema is at {current or BASELINE} and this server needs {target}: "
                          "back the database up, then run `openswx-collab-server migrate` "
                          "(or set OSWX_COLLAB_AUTO_MIGRATE=true)")
    cfg = config(connection)
    if current is None and has_tables:  # (made before migrations existed: it is the first revision)
        command.stamp(cfg, BASELINE)
    command.upgrade(cfg, "head")
    return target


async def upgrade(engine: AsyncEngine, allow: bool = True) -> str:
    """Bring the database to the newest schema; returns the revision it is at."""
    async with engine.begin() as conn:
        return await conn.run_sync(_upgrade, allow)


async def current(engine: AsyncEngine) -> tuple[str | None, str]:
    """(the revision the database is at, the newest revision this server knows)."""
    async with engine.connect() as conn:
        rev, _ = await conn.run_sync(_state)
    return rev, head()
