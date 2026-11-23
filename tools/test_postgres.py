"""Run the test suite against a real PostgreSQL instead of SQLite.

    .venv/Scripts/python -m pip install pixeltable-pgserver asyncpg
    .venv/Scripts/python tools/test_postgres.py [pytest arguments]

Starts a throwaway PostgreSQL (the pixeltable-pgserver package carries the binaries; no Docker needed),
points the tests at it and removes it afterwards. To use a server you already have, skip this script:

    OSWX_TEST_DATABASE_URL=postgresql+asyncpg://user:pw@host/db python -m pytest     # the database is wiped
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile

import pixeltable_pgserver as pgserver


def main() -> int:
    data = tempfile.mkdtemp(prefix="oswx-collab-pg-")
    server = pgserver.get_server(data)
    try:
        uri = server.get_uri()
        version = server.psql("show server_version;").strip().splitlines()[-2].strip()
        print(f"PostgreSQL {version} at {uri}", flush=True)
        env = dict(os.environ, OSWX_TEST_DATABASE_URL=uri.replace("postgresql://", "postgresql+asyncpg://", 1))
        return subprocess.call([sys.executable, "-m", "pytest", "-q", "-W", "ignore", *sys.argv[1:]], env=env)
    finally:
        server.cleanup()
        shutil.rmtree(data, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
