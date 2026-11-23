"""Extension points of the open core.

The core server has organisations, members and roles and no limits. A separate package (the commercial
layer, which lives in its own repository, or anything else) can register a Hooks subclass under the entry-point group
`openswx_collab_server.hooks` to add seat limits, single sign-on, billing or an admin console.
"""
from __future__ import annotations

from importlib.metadata import entry_points
from typing import Any


class Hooks:
    name = "core"

    async def may_add_member(self, db: Any, org: Any, current_members: int) -> tuple[bool, str]:
        """May this organisation take one more member? (False, reason) refuses the invite."""
        return True, ""

    async def member_added(self, db: Any, org: Any, user: Any) -> None:
        pass

    async def member_removed(self, db: Any, org: Any, user_id: str) -> None:
        pass

    def prepare_db(self, connection: Any) -> None:
        """Create or upgrade the extension's own tables (a synchronous SQLAlchemy connection, inside a
        transaction), after the server's migrations have run. The server's migrations never touch tables
        they do not know."""

    def routers(self) -> list:
        """Extra FastAPI routers to mount (admin console, billing webhooks, SSO)."""
        return []

    def server_info(self) -> dict:
        """Extra keys for GET /api/server."""
        return {}


def load_hooks() -> Hooks:
    for ep in entry_points(group="openswx_collab_server.hooks"):
        return ep.load()()
    return Hooks()
