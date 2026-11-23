"""The collaboration server application."""
from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from openswx_collab_protocol import PROTOCOL_VERSION

from . import __version__, assemblies, auth, documents, orgs
from .access import get_db
from .config import Settings
from .db import init_db, make_engine
from .hooks import Hooks, load_hooks
from .hub import Hub
from .security import Limiter
from .storage import make_storage


def create_app(settings: Settings | None = None, hooks: Hooks | None = None, http_client=None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = make_engine(settings.database_url)
        await init_db(engine)
        app.state.db = async_sessionmaker(engine, expire_on_commit=False)
        app.state.hub = Hub(settings, app.state.db, app.state.storage)
        await app.state.hub.start()
        try:
            yield
        finally:
            await app.state.hub.stop()
            await engine.dispose()

    app = FastAPI(title="openswx collaboration server", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.state.hooks = hooks or load_hooks()
    app.state.storage = make_storage(settings.storage_url, settings.s3_endpoint)
    app.state.login_limiter = Limiter()
    app.state.http_client = http_client or (lambda: httpx.AsyncClient(timeout=15))

    @app.get("/api/health")
    async def health():
        return {"ok": True}

    @app.get("/api/server")
    async def server_info(db: AsyncSession = Depends(get_db)):
        """What a client needs to draw its sign-in dialog."""
        return {"name": "openswx-collab", "version": __version__, "protocol": PROTOCOL_VERSION,
                "registration": await auth.registration_mode(db, settings),
                "oauth": settings.oauth_providers(), "edition": app.state.hooks.name,
                **app.state.hooks.server_info()}

    @app.get("/invite/{token}", response_class=HTMLResponse)
    async def invite_page(token: str):
        """Where an invite link lands in a browser: the invite is used from inside openswx."""
        return ("<!doctype html><meta charset=utf-8><title>openswx invite</title>"
                "<body style='font:16px system-ui;max-width:34em;margin:4em auto;padding:0 1em'>"
                "<h1>You have been invited to collaborate in openswx</h1>"
                "<p>Open openswx, choose <b>Collaborate &rsaquo; Join with invite</b>, and paste this page's "
                "address. You can create your account there.</p></body>")

    app.include_router(auth.router)
    app.include_router(orgs.router)
    app.include_router(documents.router)
    app.include_router(assemblies.router)
    for r in app.state.hooks.routers():
        app.include_router(r)
    return app
