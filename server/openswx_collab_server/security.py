"""Passwords, tokens and the roles table."""
from __future__ import annotations

import base64
import hashlib
import hmac
import re
import secrets
import time
from collections import defaultdict, deque

_N, _R, _P = 2**15, 8, 1


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=_N, r=_R, p=_P, maxmem=2**27, dklen=32)
    return f"scrypt${_N}${_R}${_P}${base64.b64encode(salt).decode()}${base64.b64encode(dk).decode()}"


def verify_password(password: str, stored: str | None) -> bool:
    try:
        scheme, n, r, p, salt, dk = (stored or "").split("$")
        if scheme != "scrypt":
            return False
        got = hashlib.scrypt(password.encode("utf-8"), salt=base64.b64decode(salt), n=int(n), r=int(r), p=int(p),
                             maxmem=2**27, dklen=32)
        return hmac.compare_digest(got, base64.b64decode(dk))
    except (ValueError, TypeError):
        return False


def new_token() -> str:
    return secrets.token_urlsafe(32)


def token_hash(token: str) -> str:
    """Tokens are stored hashed: a copy of the database does not sign anyone in."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


_EMAIL = re.compile(r"^[^@\s]{1,64}@[^@\s]{1,255}\.[^@\s.]{2,}$")


def clean_email(email: str) -> str | None:
    e = (email or "").strip().lower()
    return e if _EMAIL.match(e) else None


ORG_ROLES = {"member": 1, "admin": 2, "owner": 3}
PROJECT_ROLES = {"viewer": 1, "editor": 2, "owner": 3}


class Limiter:
    """A small in-memory limit on repeated failures (sign-in attempts), per key."""

    def __init__(self, limit: int = 10, window: float = 600.0):
        self.limit, self.window = limit, window
        self.hits: dict[str, deque] = defaultdict(deque)

    def blocked(self, key: str) -> bool:
        q, t = self.hits[key], time.time()
        while q and q[0] < t - self.window:
            q.popleft()
        return len(q) >= self.limit

    def fail(self, key: str) -> None:
        self.hits[key].append(time.time())

    def clear(self, key: str) -> None:
        self.hits.pop(key, None)


_PALETTE = ["#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4", "#008080", "#f032e6", "#9a6324", "#800000",
            "#469990", "#000075", "#808000"]


def colour_for(user_id: str, taken: set[str] = frozenset()) -> str:
    """A stable colour per user, moved along the palette when someone in the document already has it."""
    start = int(hashlib.sha256(user_id.encode()).hexdigest(), 16) % len(_PALETTE)
    for n in range(len(_PALETTE)):
        c = _PALETTE[(start + n) % len(_PALETTE)]
        if c not in taken:
            return c
    return _PALETTE[start]
