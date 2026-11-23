"""Accounts: email and password, sessions, and sign-in through Google or GitHub.

Who may create an account: the first account on a server (it becomes the server admin); after that, anyone
with a valid invite; or anyone at all when OSWX_COLLAB_OPEN_REGISTRATION is on.

Sign-in through a provider is made for a desktop app: the app listens on a loopback port and opens
  GET /api/auth/oauth/<provider>/start?redirect_uri=http://127.0.0.1:<port>/...
in the system browser. After the provider, the browser lands on that loopback address with ?code=...,
and the app trades the code for a session token at POST /api/auth/exchange. Only loopback addresses are
accepted as redirect_uri, so a code can never be sent to another machine.

The app also makes up a secret (43+ characters), sends its SHA-256 as `code_challenge` to /start and the
secret itself as `code_verifier` to /exchange: a code picked up by another program on the same machine is
useless without it.

A provider sign-in joins an existing account with the same address only when that address was vouched
for (email_verified: the account came from a provider, or the operator made it). Otherwise someone could
register your address first and wait for you to arrive. An invite does not vouch for an address: whoever
makes an invite holds its link. People with a password account add a provider while signed in:
POST /api/auth/oauth/<provider>/link (with their password) returns the address to open in the browser, and
the code that comes back is handed to POST /api/auth/oauth/link/finish by the same signed-in app. Nothing
is attached by the browser alone: a link someone else started and talked you into opening does nothing.
"""
from __future__ import annotations

from urllib.parse import urlencode, urlsplit

import hashlib
import re

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .access import bearer, current_user, get_db
from .db import LoginCode, OAuthIdentity, OAuthState, PasswordReset, Session, User, now
from .orgs import accept_invite, find_invite, user_out
from .security import clean_email, hash_password, new_token, token_hash, verify_password

router = APIRouter(prefix="/api")

PROVIDERS = {
    "google": {
        "authorize": "https://accounts.google.com/o/oauth2/v2/auth",
        "token": "https://oauth2.googleapis.com/token",
        "userinfo": "https://openidconnect.googleapis.com/v1/userinfo",
        "scope": "openid email profile",
    },
    "github": {
        "authorize": "https://github.com/login/oauth/authorize",
        "token": "https://github.com/login/oauth/access_token",
        "userinfo": "https://api.github.com/user",
        "emails": "https://api.github.com/user/emails",
        "scope": "read:user user:email",
    },
}


async def registration_mode(db: AsyncSession, settings) -> str:
    users = (await db.execute(select(func.count()).select_from(User))).scalar_one()
    if users == 0:
        return "first_user"
    return "open" if settings.open_registration else "invite"


async def start_session(db: AsyncSession, user: User, settings, label: str = "") -> str:
    token = new_token()
    db.add(Session(token_hash=token_hash(token), user_id=user.id, label=label[:200],
                   expires_at=now() + settings.session_days * 86400))
    return token


def me_out(u: User) -> dict:
    return {**user_out(u), "is_server_admin": u.is_server_admin}


async def create_user(request: Request, db: AsyncSession, email: str, name: str, password_hash: str | None,
                      invite_token: str | None, verified: bool = False) -> User:
    """Create an account if this server lets this person have one. Does not commit."""
    mode = await registration_mode(db, request.app.state.settings)
    inv = await find_invite(db, invite_token, email)
    if mode == "invite" and inv is None:
        raise HTTPException(403, "this server is invite-only: ask an organisation owner for an invite link")
    user = User(email=email, name=name.strip() or email.split("@")[0], password_hash=password_hash,
                is_server_admin=(mode == "first_user"),
                email_verified=verified or mode == "first_user")  # a provider's word, or the operator's own account
    db.add(user)
    await db.flush()
    if inv is not None:
        await accept_invite(request, db, inv, user)
    return user


# ------------------------------------------------------------------ email and password
class RegisterIn(BaseModel):
    email: str
    password: str = Field(min_length=8, max_length=200)
    name: str = Field(default="", max_length=200)
    invite: str | None = None


class LoginIn(BaseModel):
    email: str
    password: str
    label: str = ""


@router.post("/auth/register", status_code=201)
async def register(body: RegisterIn, request: Request, db: AsyncSession = Depends(get_db)):
    email = clean_email(body.email)
    if email is None:
        raise HTTPException(422, "not an email address")
    if (await db.execute(select(User).where(User.email == email))).scalar_one_or_none() is not None:
        raise HTTPException(409, "there is already an account with this email address")
    user = await create_user(request, db, email, body.name, hash_password(body.password), body.invite)
    token = await start_session(db, user, request.app.state.settings)
    await db.commit()
    return {"token": token, "user": me_out(user)}


@router.post("/auth/login")
async def login(body: LoginIn, request: Request, db: AsyncSession = Depends(get_db)):
    limiter = request.app.state.login_limiter
    email = clean_email(body.email) or ""
    key = f"{email}|{request.client.host if request.client else ''}"
    if limiter.blocked(key):
        raise HTTPException(429, "too many attempts; try again in a few minutes")
    user = (await db.execute(select(User).where(User.email == email))).scalar_one_or_none()
    if user is None or user.disabled or not verify_password(body.password, user.password_hash):
        limiter.fail(key)
        raise HTTPException(401, "wrong email address or password")
    limiter.clear(key)
    token = await start_session(db, user, request.app.state.settings, body.label)
    await db.commit()
    return {"token": token, "user": me_out(user)}


@router.post("/auth/logout", status_code=204)
async def logout(request: Request, user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    await db.execute(delete(Session).where(Session.token_hash == token_hash(bearer(request))))
    await db.commit()


@router.get("/me")
async def me(user: User = Depends(current_user)):
    return me_out(user)


class MeIn(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    password: str | None = Field(default=None, min_length=8, max_length=200)
    current_password: str | None = None


@router.patch("/me")
async def update_me(body: MeIn, request: Request, user: User = Depends(current_user),
                    db: AsyncSession = Depends(get_db)):
    user = await db.get(User, user.id)
    if body.name:
        user.name = body.name.strip()
    if body.password:
        # the current password - or, for an account that has none yet, a sign-in from the last ten minutes:
        # a first password is a new way in, like adding a provider
        await _confirm(request, db, user, body.current_password)
        user.password_hash = hash_password(body.password)
        # every other session ends: a changed password signs out whoever else had it
        await db.execute(delete(Session).where(Session.user_id == user.id,
                                               Session.token_hash != token_hash(bearer(request))))
    await db.commit()
    if body.password:
        request.app.state.hub.drop_account(user.id)  # (open documents too: every device signs in again)
    return me_out(user)


# ------------------------------------------------------------------ Google / GitHub
def _loopback(uri: str) -> bool:
    try:
        u = urlsplit(uri)
    except ValueError:
        return False
    return u.scheme == "http" and u.hostname in ("127.0.0.1", "localhost", "::1") and u.port is not None \
        and not u.username and not u.fragment


def _provider(request: Request, name: str) -> tuple[dict, str, str]:
    s = request.app.state.settings
    if name not in PROVIDERS or name not in s.oauth_providers():
        raise HTTPException(404, f"sign-in with {name} is not set up on this server")
    return PROVIDERS[name], getattr(s, f"{name}_client_id"), getattr(s, f"{name}_client_secret")


def _callback_url(request: Request, name: str) -> str:
    return f"{request.app.state.settings.public_url.rstrip('/')}/api/auth/oauth/{name}/callback"


_CHALLENGE = re.compile(r"^[0-9a-f]{64}$")


async def _begin(request: Request, db: AsyncSession, name: str, redirect_uri: str, code_challenge: str,
                 invite: str | None, link_user_id: str | None) -> str:
    """Remember a sign-in that is starting; the provider's address to send the browser to."""
    cfg, client_id, _ = _provider(request, name)
    if not _loopback(redirect_uri):
        raise HTTPException(400, "redirect_uri must be a loopback address (http://127.0.0.1:<port>/...)")
    if not _CHALLENGE.match(code_challenge):
        raise HTTPException(400, "code_challenge must be the SHA-256 (lower-case hex) of the app's code_verifier")
    state = new_token()
    await db.execute(delete(OAuthState).where(OAuthState.expires_at < now()))
    db.add(OAuthState(state_hash=token_hash(state), provider=name, redirect_uri=redirect_uri, invite=invite,
                      challenge=code_challenge, link_user_id=link_user_id, expires_at=now() + 600))
    await db.commit()
    q = {"client_id": client_id, "redirect_uri": _callback_url(request, name), "response_type": "code",
         "scope": cfg["scope"], "state": state}
    return f"{cfg['authorize']}?{urlencode(q)}"


@router.get("/auth/oauth/{name}/start")
async def oauth_start(name: str, redirect_uri: str, code_challenge: str, request: Request,
                      invite: str | None = None, db: AsyncSession = Depends(get_db)):
    return RedirectResponse(await _begin(request, db, name, redirect_uri, code_challenge, invite, None),
                            status_code=302)


class LinkIn(BaseModel):
    redirect_uri: str
    code_challenge: str
    current_password: str | None = None


async def _confirm(request: Request, db: AsyncSession, user: User, password: str | None) -> None:
    """Changing how an account is signed in to takes more than a session token that may have leaked: the
    password, or for an account that has none, a sign-in within the last ten minutes."""
    if user.password_hash:
        if not verify_password(password or "", user.password_hash):
            raise HTTPException(403, "the current password is wrong")
        return
    sess = await db.get(Session, token_hash(bearer(request)))
    if sess is None or now() - sess.created_at > 600:
        raise HTTPException(403, "sign in again first (this needs a sign-in from the last ten minutes)")


@router.post("/auth/oauth/{name}/link")
async def oauth_link(name: str, body: LinkIn, request: Request, user: User = Depends(current_user),
                     db: AsyncSession = Depends(get_db)):
    """Add Google or GitHub to the account that is signed in. Open the returned address in the browser;
    it ends with a code on the loopback address, which goes to /auth/oauth/link/finish."""
    await _confirm(request, db, user, body.current_password)
    return {"url": await _begin(request, db, name, body.redirect_uri, body.code_challenge, None, user.id)}


async def _identity(client: httpx.AsyncClient, name: str, cfg: dict, access_token: str) -> tuple[str, str | None, str]:
    """(subject, verified email or None, display name) from the provider."""
    h = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    info = (await client.get(cfg["userinfo"], headers=h)).raise_for_status().json()
    if name == "google":
        email = info.get("email") if info.get("email_verified") in (True, "true") else None
        return str(info["sub"]), email, info.get("name") or ""
    emails = (await client.get(cfg["emails"], headers=h)).raise_for_status().json()
    primary = next((e["email"] for e in emails if e.get("primary") and e.get("verified")), None)
    return str(info["id"]), primary, info.get("name") or info.get("login") or ""


@router.get("/auth/oauth/{name}/callback")
async def oauth_callback(name: str, request: Request, code: str | None = None, state: str | None = None,
                         error: str | None = None, db: AsyncSession = Depends(get_db)):
    cfg, client_id, client_secret = _provider(request, name)
    st = await db.get(OAuthState, token_hash(state or ""))
    if st is None or st.provider != name or st.expires_at < now():
        raise HTTPException(400, "this sign-in has expired; start again from the app")
    redirect_uri, invite, challenge, link_user_id = st.redirect_uri, st.invite, st.challenge, st.link_user_id
    await db.delete(st)  # (single use)
    await db.commit()

    def back(**q) -> RedirectResponse:
        sep = "&" if "?" in redirect_uri else "?"
        return RedirectResponse(f"{redirect_uri}{sep}{urlencode(q)}", status_code=302)

    if error or not code:
        return back(error=error or "cancelled")
    try:
        async with request.app.state.http_client() as client:
            tok = (await client.post(cfg["token"], headers={"Accept": "application/json"}, data={
                "client_id": client_id, "client_secret": client_secret, "code": code,
                "redirect_uri": _callback_url(request, name), "grant_type": "authorization_code",
            })).raise_for_status().json()
            if not tok.get("access_token"):
                return back(error="provider_refused")
            subject, email, display = await _identity(client, name, cfg, tok["access_token"])
    except (httpx.HTTPError, KeyError, ValueError):
        return back(error="provider_unreachable")

    ident = await db.get(OAuthIdentity, (name, subject))
    if link_user_id is not None:
        # Someone signed in asked to add this provider. Nothing is attached here: whoever sits at this
        # browser may not be them. The code only says which provider identity came back; the app that
        # started it finishes, signed in, at /auth/oauth/link/finish.
        if ident is not None and ident.user_id != link_user_id:
            return back(error="identity_in_use")
        login_code = new_token()
        db.add(LoginCode(code_hash=token_hash(login_code), user_id=link_user_id, challenge=challenge,
                         link_provider=name, link_subject=subject, link_email=clean_email(email or ""),
                         expires_at=now() + 120))
        await db.commit()
        return back(code=login_code)
    if ident is not None:
        user = await db.get(User, ident.user_id)
    else:
        email = clean_email(email or "")
        if email is None:
            return back(error="no_verified_email")
        user = (await db.execute(select(User).where(User.email == email))).scalar_one_or_none()
        if user is None:
            try:
                user = await create_user(request, db, email, display, None, invite, verified=True)
            except HTTPException as exc:
                await db.rollback()
                return back(error="invite_required" if exc.status_code == 403 else "not_allowed")
        elif not user.email_verified:
            # Nobody ever checked that whoever made this account owns the address: joining it now could hand
            # this person's sign-in to a stranger. They sign in with the password instead.
            return back(error="account_exists_use_password")
        db.add(OAuthIdentity(provider=name, subject=subject, user_id=user.id))
    if user is None or user.disabled:
        return back(error="account_disabled")
    login_code = new_token()
    db.add(LoginCode(code_hash=token_hash(login_code), user_id=user.id, challenge=challenge,
                     expires_at=now() + 120))
    await db.commit()
    return back(code=login_code)


class ExchangeIn(BaseModel):
    code: str
    code_verifier: str = Field(min_length=43, max_length=200)
    label: str = ""


@router.post("/auth/exchange")
async def exchange(body: ExchangeIn, request: Request, db: AsyncSession = Depends(get_db)):
    lc = await db.get(LoginCode, token_hash(body.code))
    if lc is None or lc.expires_at < now():
        raise HTTPException(400, "this code is not valid any more; sign in again")
    user = await db.get(User, lc.user_id)
    await db.delete(lc)  # (one try: a wrong verifier burns the code)
    if hashlib.sha256(body.code_verifier.encode("utf-8")).hexdigest() != lc.challenge:
        await db.commit()
        raise HTTPException(400, "this code was issued to another app; sign in again")
    if lc.link_provider is not None:  # (a code from adding a provider never signs anyone in)
        await db.commit()
        raise HTTPException(400, "this code adds a sign-in method; finish it from the signed-in app")
    if user is None or user.disabled:
        await db.commit()
        raise HTTPException(403, "this account is disabled")
    token = await start_session(db, user, request.app.state.settings, body.label)
    await db.commit()
    return {"token": token, "user": me_out(user)}


class LinkFinishIn(BaseModel):
    code: str
    code_verifier: str = Field(min_length=43, max_length=200)


@router.post("/auth/oauth/link/finish")
async def oauth_link_finish(body: LinkFinishIn, user: User = Depends(current_user),
                            db: AsyncSession = Depends(get_db)):
    """Attach the provider identity a link flow brought back. Needs all three: the code (from the browser),
    the verifier (kept by the app that started it) and that account's own session."""
    lc = await db.get(LoginCode, token_hash(body.code))
    if lc is None or lc.expires_at < now():
        raise HTTPException(400, "this code is not valid any more; start again")
    await db.delete(lc)
    ok = lc.link_provider is not None and lc.user_id == user.id \
        and hashlib.sha256(body.code_verifier.encode("utf-8")).hexdigest() == lc.challenge
    if not ok:
        await db.commit()
        raise HTTPException(400, "this code was not issued to this app and account; start again")
    ident = await db.get(OAuthIdentity, (lc.link_provider, lc.link_subject))
    if ident is not None and ident.user_id != user.id:
        await db.commit()
        raise HTTPException(409, "that provider account already signs in to another account here")
    if ident is None:
        db.add(OAuthIdentity(provider=lc.link_provider, subject=lc.link_subject, user_id=user.id))
    account = await db.get(User, user.id)
    if lc.link_email and lc.link_email == account.email:
        account.email_verified = True  # (the provider vouches for the very address the account has)
    await db.commit()
    return {"linked": lc.link_provider, "user": me_out(account)}


@router.get("/me/providers")
async def my_providers(user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    rows = (await db.execute(select(OAuthIdentity.provider).where(OAuthIdentity.user_id == user.id))).scalars()
    return {"providers": sorted(rows), "has_password": user.password_hash is not None}


class UnlinkIn(BaseModel):
    current_password: str | None = None


@router.post("/me/providers/{name}/unlink", status_code=204)
async def unlink_provider(name: str, body: UnlinkIn, request: Request, user: User = Depends(current_user),
                          db: AsyncSession = Depends(get_db)):
    await _confirm(request, db, user, body.current_password)
    rows = (await db.execute(select(OAuthIdentity).where(OAuthIdentity.user_id == user.id))).scalars().all()
    mine = [r for r in rows if r.provider == name]
    if not mine:
        raise HTTPException(404, "not linked")
    if user.password_hash is None and len(rows) == len(mine):
        raise HTTPException(409, "this is the only way into the account; set a password first")
    for r in mine:
        await db.delete(r)
    await db.commit()


# ------------------------------------------------------------------ forgotten passwords (needs email)
class ResetIn(BaseModel):
    email: str


@router.post("/auth/password-reset", status_code=202)
async def request_reset(body: ResetIn, request: Request, db: AsyncSession = Depends(get_db)):
    """Email a link to choose a new password. The answer is the same whether or not there is such an
    account (so it cannot be used to find out who has one)."""
    mailer, settings = request.app.state.mailer, request.app.state.settings
    if not mailer.enabled:
        raise HTTPException(501, "this server cannot send email: ask its operator to reset your password")
    email = clean_email(body.email) or ""
    limiter = request.app.state.reset_limiter
    key = f"{email}|{request.client.host if request.client else ''}"
    answer = {"message": "if there is an account for this address, a link is on its way"}
    if limiter.blocked(key) or limiter.blocked(email):
        return answer
    limiter.fail(key)
    limiter.fail(email)
    user = (await db.execute(select(User).where(User.email == email))).scalar_one_or_none()
    if user is None or user.disabled:
        return answer
    await db.execute(delete(PasswordReset).where((PasswordReset.user_id == user.id) | (PasswordReset.expires_at < now())))
    token = new_token()
    db.add(PasswordReset(token_hash=token_hash(token), user_id=user.id,
                         expires_at=now() + settings.reset_minutes * 60))
    await db.commit()
    from .mail import reset_text  # noqa: PLC0415
    subject, text = reset_text(f"{settings.public_url.rstrip('/')}/reset-password/{token}", settings.reset_minutes)
    await mailer.send(user.email, subject, text)
    return answer


class ResetConfirmIn(BaseModel):
    token: str
    password: str = Field(min_length=8, max_length=200)


@router.post("/auth/password-reset/confirm")
async def confirm_reset(body: ResetConfirmIn, request: Request, db: AsyncSession = Depends(get_db)):
    reset = await db.get(PasswordReset, token_hash(body.token))
    if reset is None or reset.expires_at < now():
        raise HTTPException(400, "this link has expired or was already used; ask for a new one")
    user = await db.get(User, reset.user_id)
    await db.delete(reset)
    if user is None or user.disabled:
        await db.commit()
        raise HTTPException(400, "this link has expired or was already used; ask for a new one")
    user.password_hash = hash_password(body.password)
    user.email_verified = True  # (they read mail sent to it: the address is theirs)
    await db.execute(delete(Session).where(Session.user_id == user.id))  # (signed out everywhere)
    await db.execute(delete(PasswordReset).where(PasswordReset.user_id == user.id))
    await db.commit()
    request.app.state.hub.drop_account(user.id)
    return {"message": "your password is set; sign in again in openswx"}
