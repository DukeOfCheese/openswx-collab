"""The collaboration server application."""
from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from openswx_collab_protocol import PROTOCOL_VERSION

from . import __version__, assemblies, auth, documents, lifecycle, orgs
from .access import get_db
from .config import Settings
from .db import init_db, make_engine
from .hooks import Hooks, load_hooks
from .hub import Hub
from .mail import Mailer
from .security import Limiter
from .storage import make_storage


_RESET_PAGE = """<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width">
<meta name=referrer content=no-referrer><title>Choose a new password</title>
<body style="font:16px system-ui;max-width:28em;margin:4em auto;padding:0 1em">
<h1>Choose a new password</h1>
<form id=f><p><label>New password<br><input id=p type=password minlength=8 required autocomplete=new-password
style="width:100%;font:inherit;padding:.4em"></label></p>
<p><label>Again<br><input id=q type=password minlength=8 required autocomplete=new-password
style="width:100%;font:inherit;padding:.4em"></label></p>
<p><button style="font:inherit;padding:.4em 1em">Set password</button></p></form><p id=m role=status></p>
<script>
const token = decodeURIComponent(location.pathname.split("/").pop() || "");  // (never written into the page)
document.getElementById("f").onsubmit = async (e) => {
  e.preventDefault();
  const m = document.getElementById("m"), p = document.getElementById("p").value;
  if (p !== document.getElementById("q").value) { m.textContent = "The two passwords differ."; return; }
  const r = await fetch("/api/auth/password-reset/confirm", {method: "POST",
    headers: {"content-type": "application/json"}, body: JSON.stringify({token, password: p})});
  const d = await r.json().catch(() => ({}));
  m.textContent = r.ok ? d.message : (typeof d.detail === "string" ? d.detail : "That did not work.");
  if (r.ok) document.getElementById("f").remove();
};
</script></body>"""


def create_app(settings: Settings | None = None, hooks: Hooks | None = None, http_client=None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = make_engine(settings.database_url)
        await init_db(engine, settings.auto_migrate, app.state.hooks)
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
    app.state.reset_limiter = Limiter(limit=5, window=3600)
    app.state.mailer = Mailer(settings)
    app.state.http_client = http_client or (lambda: httpx.AsyncClient(timeout=15))

    @app.get("/api/health")
    async def health():
        return {"ok": True}

    @app.get("/api/server")
    async def server_info(db: AsyncSession = Depends(get_db)):
        """What a client needs to draw its sign-in dialog."""
        return {"name": "openswx-collab", "version": __version__, "protocol": PROTOCOL_VERSION,
                "registration": await auth.registration_mode(db, settings),
                "oauth": settings.oauth_providers(), "email": app.state.mailer.enabled,
                "edition": app.state.hooks.name,
                **app.state.hooks.server_info()}

    @app.get("/invite/{token}", response_class=HTMLResponse)
    async def invite_page(token: str):
        """Where an invite link lands in a browser: the invite is used from inside openswx."""
        return ("<!doctype html><meta charset=utf-8><title>openswx invite</title>"
                "<body style='font:16px system-ui;max-width:34em;margin:4em auto;padding:0 1em'>"
                "<h1>You have been invited to collaborate in openswx</h1>"
                "<p>Open openswx, choose <b>Collaborate &rsaquo; Join with invite</b>, and paste this page's "
                "address. You can create your account there.</p></body>")

    @app.get("/reset-password/{token}", response_class=HTMLResponse)
    async def reset_page(token: str):  # (unused: the page reads it from its own address)
        """Where a reset link lands: a small form that posts to /api/auth/password-reset/confirm. The page is the
        same for every link: nothing from the address is written into it."""
        return HTMLResponse(_RESET_PAGE, headers={"Cache-Control": "no-store"})

    app.include_router(auth.router)
    app.include_router(orgs.router)
    app.include_router(documents.router)
    app.include_router(assemblies.router)
    app.include_router(lifecycle.router)
    for r in app.state.hooks.routers():
        app.include_router(r)
    return app
