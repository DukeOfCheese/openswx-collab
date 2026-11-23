"""Server settings, read from OSWX_COLLAB_* environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, fields

PREFIX = "OSWX_COLLAB_"


@dataclass
class Settings:
    database_url: str = "sqlite+aiosqlite:///./data/collab.db"  # or postgresql+asyncpg://user:pw@host/db
    storage_url: str = "./data/storage"  # a folder, or s3://bucket/prefix
    s3_endpoint: str = ""  # for S3-compatible stores (MinIO): http://minio:9000
    public_url: str = "http://localhost:8787"  # how clients and browsers reach this server (OAuth callbacks)
    open_registration: bool = False  # False: the first account is the server admin, everyone else needs an invite
    session_days: int = 30
    auto_migrate: bool = True  # upgrade the database schema on start (off: refuse to start until `migrate` is run)
    snapshot_every: int = 200  # a full copy of the document is stored every this many changes
    lock_ttl: float = 45.0  # a connected client that stays silent this long loses its locks
    lock_grace: float = 60.0  # a disconnected client keeps its locks this long
    sweep_interval: float = 5.0
    access_recheck: float = 30.0  # how often open connections are checked against sessions and roles
    room_idle: float = 120.0  # an open document nobody is connected to is unloaded after this
    catchup_limit: int = 500  # reconnecting clients further behind than this get the whole document
    max_change_bytes: int = 8 * 2**20
    max_blob_bytes: int = 256 * 2**20
    presence_rate: int = 60  # presence/draft messages per second per client before they are dropped
    google_client_id: str = ""
    google_client_secret: str = ""
    github_client_id: str = ""
    github_client_secret: str = ""

    @classmethod
    def from_env(cls, env: dict | None = None) -> "Settings":
        env = os.environ if env is None else env
        kw = {}
        for f in fields(cls):
            raw = env.get(PREFIX + f.name.upper())
            if raw is None:
                continue
            if f.type in ("bool", bool):
                kw[f.name] = raw.strip().lower() in ("1", "true", "yes", "on")
            elif f.type in ("int", int):
                kw[f.name] = int(raw)
            elif f.type in ("float", float):
                kw[f.name] = float(raw)
            else:
                kw[f.name] = raw
        return cls(**kw)

    def oauth_providers(self) -> list[str]:
        out = []
        if self.google_client_id and self.google_client_secret:
            out.append("google")
        if self.github_client_id and self.github_client_secret:
            out.append("github")
        return out
