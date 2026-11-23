"""Command line: `openswx-collab-server serve`, and account tools for the operator.

  serve [--host 0.0.0.0] [--port 8787]
  migrate                          upgrade the database schema (take a backup first)
  db-status                        which schema the database is at, and which this server needs
  create-admin <email> [name]      make (or promote) a server admin; asks for a password
  reset-password <email>           set a new password and end the account's sessions
"""
from __future__ import annotations

import argparse
import asyncio
import getpass
import os
import sys

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from .config import Settings
from .db import Session, User, init_db, make_engine
from .hooks import load_hooks
from .migrate import SchemaError, current, upgrade
from .security import clean_email, hash_password


def _ask_password() -> str:
    pw = os.environ.get("OSWX_COLLAB_PASSWORD") or getpass.getpass("New password: ")
    if len(pw) < 8:
        sys.exit("the password needs at least 8 characters")
    return pw


async def _account(settings: Settings, email: str, name: str | None, admin: bool) -> str:
    email = clean_email(email) or sys.exit("not an email address")
    engine = make_engine(settings.database_url)
    await init_db(engine, settings.auto_migrate, load_hooks())
    async with async_sessionmaker(engine, expire_on_commit=False)() as s:
        user = (await s.execute(select(User).where(User.email == email))).scalar_one_or_none()
        if user is None and not admin:
            sys.exit(f"no account {email}")
        pw = hash_password(_ask_password())
        if user is None:
            user = User(email=email, name=name or email.split("@")[0], password_hash=pw, is_server_admin=True,
                        email_verified=True)
            s.add(user)
            done = f"created server admin {email}"
        else:
            user.password_hash = pw
            user.is_server_admin = user.is_server_admin or admin
            await s.execute(delete(Session).where(Session.user_id == user.id))
            done = f"password set for {email}" + (" (server admin)" if user.is_server_admin else "")
        await s.commit()
    await engine.dispose()
    return done


async def _schema(settings: Settings, do_upgrade: bool) -> str:
    engine = make_engine(settings.database_url)
    try:
        if do_upgrade:
            before, _ = await current(engine)
            after = await upgrade(engine, allow=True)
            async with engine.begin() as conn:
                await conn.run_sync(load_hooks().prepare_db)
            return f"schema {before or 'none'} -> {after}" if before != after else f"schema {after}: up to date"
        at, head = await current(engine)
        return f"database at {at or 'none (or made before migrations)'}; this server needs {head}"
    except SchemaError as exc:
        sys.exit(str(exc))
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="openswx-collab-server", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("serve")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8787)
    a = sub.add_parser("create-admin")
    a.add_argument("email")
    a.add_argument("name", nargs="?")
    r = sub.add_parser("reset-password")
    r.add_argument("email")
    sub.add_parser("migrate")
    sub.add_parser("db-status")
    args = p.parse_args(argv)
    settings = Settings.from_env()
    if args.cmd in ("migrate", "db-status"):
        print(asyncio.run(_schema(settings, args.cmd == "migrate")))
        return
    if args.cmd == "serve":
        import uvicorn

        from .app import create_app
        # one process: open documents live in this process's memory
        uvicorn.run(create_app(settings), host=args.host, port=args.port, workers=1, ws_max_size=settings.max_change_bytes + 65536)
    elif args.cmd == "create-admin":
        print(asyncio.run(_account(settings, args.email, args.name, True)))
    else:
        print(asyncio.run(_account(settings, args.email, None, False)))


if __name__ == "__main__":
    main()
