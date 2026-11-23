"""The database schema: migrations agree with the models, and every way a server meets a database works."""
import asyncio

import pytest
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import text

from openswx_collab_server import __main__ as cli
from openswx_collab_server.db import Base, init_db, make_engine
from openswx_collab_server.hooks import Hooks
from openswx_collab_server.migrate import BASELINE, SchemaError, current, head, upgrade

from conftest import H, World


def run(coro):
    return asyncio.run(coro)


def test_the_migrations_build_exactly_the_tables_the_code_uses(settings):
    """Fails when db.py changed without a migration (or a migration without db.py)."""
    async def go():
        engine = make_engine(settings.database_url)
        try:
            assert await upgrade(engine) == head()
            async with engine.connect() as conn:
                return await conn.run_sync(
                    lambda c: compare_metadata(MigrationContext.configure(c, opts={"compare_type": True}), Base.metadata))
        finally:
            await engine.dispose()
    assert run(go()) == []


def test_a_database_made_before_migrations_is_adopted_with_its_data(settings, make_client):
    async def old_style():
        engine = make_engine(settings.database_url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)  # (how development versions made it)
            await conn.execute(text("INSERT INTO orgs (id, name, created_at) VALUES ('o1', 'Kept', 0)"))
        await engine.dispose()
    run(old_style())

    async def state():
        engine = make_engine(settings.database_url)
        try:
            return await current(engine)
        finally:
            await engine.dispose()
    assert run(state()) == (None, head())
    client = make_client()  # starting the server adopts it
    assert run(state()) == (head(), head())
    w = World(client)
    assert w.org  # (and it works)
    assert BASELINE == "0001"

    async def kept():
        engine = make_engine(settings.database_url)
        async with engine.connect() as conn:
            name = (await conn.execute(text("SELECT name FROM orgs WHERE id = 'o1'"))).scalar_one()
        await engine.dispose()
        return name
    assert run(kept()) == "Kept"


def test_with_automatic_upgrades_off_an_older_database_is_refused(settings):
    async def go():
        engine = make_engine(settings.database_url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        try:
            with pytest.raises(SchemaError, match="openswx-collab-server migrate"):
                await init_db(engine, auto_migrate=False)
            assert await init_db(engine, auto_migrate=True) == head()
            assert await init_db(engine, auto_migrate=False) == head()  # (up to date: fine either way)
        finally:
            await engine.dispose()
    run(go())


def test_a_database_from_a_newer_server_is_refused(settings):
    async def go():
        engine = make_engine(settings.database_url)
        try:
            await upgrade(engine)
            async with engine.begin() as conn:
                await conn.execute(text("UPDATE alembic_version SET version_num = '9999'"))
            with pytest.raises(SchemaError, match="newer version"):
                await upgrade(engine)
        finally:
            await engine.dispose()
    run(go())


def test_an_empty_database_is_built_by_the_migrations(settings, make_client):
    client = make_client()
    w = World(client)
    assert client.get(f"/api/orgs/{w.org}/members", headers=H(w.admin)).status_code == 200


def test_extensions_set_up_their_own_tables_after_the_migrations(settings, make_client):
    made = []

    class WithTable(Hooks):
        def prepare_db(self, connection):
            connection.execute(text("CREATE TABLE IF NOT EXISTS ext_things (id INTEGER PRIMARY KEY)"))
            made.append(True)
    make_client(hooks=WithTable())
    assert made == [True]

    async def tables():
        engine = make_engine(settings.database_url)
        async with engine.connect() as conn:
            n = (await conn.execute(text("SELECT COUNT(*) FROM ext_things"))).scalar_one()
        await engine.dispose()
        return n
    assert run(tables()) == 0
    run(upgrade(make_engine(settings.database_url)))  # (upgrading again leaves the extension's table alone)
    assert run(tables()) == 0


def test_command_line(settings, monkeypatch, capsys):
    for k, v in (("DATABASE_URL", settings.database_url), ("STORAGE_URL", settings.storage_url)):
        monkeypatch.setenv(f"OSWX_COLLAB_{k}", v)
    cli.main(["db-status"])
    assert "database at none" in capsys.readouterr().out
    cli.main(["migrate"])
    assert f"-> {head()}" in capsys.readouterr().out
    cli.main(["migrate"])
    assert "up to date" in capsys.readouterr().out
    cli.main(["db-status"])
    assert f"database at {head()}; this server needs {head()}" in capsys.readouterr().out
