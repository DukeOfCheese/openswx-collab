"""Run the Rust server's test suite against a real PostgreSQL instead of SQLite.

    .venv/Scripts/python -m pip install pixeltable-pgserver
    .venv/Scripts/python tools/test_postgres_rust.py [cargo test arguments]

Starts a throwaway PostgreSQL (the pixeltable-pgserver package carries the binaries; no Docker needed),
points the tests at it with OSWX_TEST_DATABASE_URL and removes it afterwards. Every test makes a schema of
its own in that database and drops it when it ends, so the tests run side by side as they do on SQLite.

Arguments go to `cargo test -p oswx-server -j 2`, for example:

    tools/test_postgres_rust.py --test journal                 # one test file
    tools/test_postgres_rust.py --test orgs -- two_first       # tests whose names contain "two_first"

To use a server you already have, skip this script (the database is only added to: schemas named t_<id>):

    OSWX_TEST_DATABASE_URL=postgres://user:pw@host/db cargo test -p oswx-server
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile

import pixeltable_pgserver as pgserver

# Tests run many at once, each with a few connections of its own.
MAX_CONNECTIONS = 400


def setting(server: pgserver.PostgresServer, name: str) -> str:
    return server.psql(f"show {name};").strip().splitlines()[-2].strip()


def main() -> int:
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data = tempfile.mkdtemp(prefix="oswx-collab-pg-")
    server = pgserver.get_server(data)
    try:
        # max_connections is read when the server starts.
        server.psql(f"ALTER SYSTEM SET max_connections = {MAX_CONNECTIONS};")
        server.stop()
        server.ensure_postgres_running()
        uri = server.get_uri()
        print(
            f"PostgreSQL {setting(server, 'server_version')} at {uri} "
            f"(max_connections {setting(server, 'max_connections')})",
            flush=True,
        )
        env = dict(os.environ, OSWX_TEST_DATABASE_URL=uri)
        return subprocess.call(["cargo", "test", "-p", "oswx-server", "-j", "2", *sys.argv[1:]], env=env, cwd=repo)
    finally:
        server.cleanup()
        shutil.rmtree(data, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
