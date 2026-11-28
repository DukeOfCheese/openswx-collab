"""Release notes for a version: its CHANGELOG.md section plus how to run it.

    python tools/release_notes.py 0.1.0 > notes.md      (the release workflow does this for each v* tag)

0.1.x is the Python server; 0.2 and later the Rust server, each with its own "Run it". OSWX_CHANGELOG
names another CHANGELOG file to read (to try a section before it is in CHANGELOG.md).
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

RUN_RUST = """
## Run it

```
docker pull ghcr.io/dukeofcheese/openswx-collab-server:{v}
```

The image has version tags only ({tags}); `latest` stays on the Python server 0.1.x.

Or the program alone: download the archive for your machine below (Linux x86_64 and aarch64, static;
Windows x86_64; macOS arm64 and x86_64), check it against its `.sha256`, unpack it and run
`openswx-collab-server serve`. Or with Compose, from a checkout of this tag: copy `deploy/.env.example` to
`deploy/.env`, fill it in, and `docker compose -f deploy/docker-compose.rust.yml --env-file deploy/.env up -d
--build` (SQLite), adding `-f deploy/docker-compose.rust-postgres.yml` for PostgreSQL. The full guide is
[docs/self-hosting.md](https://github.com/DukeOfCheese/openswx-collab/blob/v{v}/docs/self-hosting.md).

Upgrading: back the database up first; the server upgrades its schema when it starts.

openswx-collab is an independent open-source project, not affiliated with Dassault Systemes.
"""

RUN = """
## Run it

```
docker pull ghcr.io/dukeofcheese/openswx-collab-server:{v}
```

or with PostgreSQL, from a checkout of this tag: copy `deploy/.env.example` to `deploy/.env`, fill it in, and
`docker compose -f deploy/docker-compose.yml --env-file deploy/.env up -d`. Without Docker:
`pip install openswx-collab-server=={v}` then `openswx-collab-server serve`. The full guide is
[docs/self-hosting.md](https://github.com/DukeOfCheese/openswx-collab/blob/v{v}/docs/self-hosting.md).

Upgrading: back the database up first; the server upgrades its schema when it starts.

openswx-collab is an independent open-source project, not affiliated with Dassault Systemes.
"""


def section(version: str, changelog: str) -> str:
    """The CHANGELOG section headed `## <version>` (up to the next `## `), without its heading."""
    m = re.search(rf"^## {re.escape(version)}\b[^\n]*\n(.*?)(?=^## |\Z)", changelog, re.M | re.S)
    if not m:
        raise SystemExit(f"CHANGELOG.md has no section for {version}")
    return m.group(1).strip()


def notes(version: str) -> str:
    version = version.lstrip("v")
    path = os.environ.get("OSWX_CHANGELOG") or ROOT / "CHANGELOG.md"
    changelog = Path(path).read_text(encoding="utf-8")
    m = re.match(r"(\d+)\.(\d+)\.", version)
    if not m:
        raise SystemExit(f"{version} is not a version (major.minor.patch)")
    if (int(m.group(1)), int(m.group(2))) <= (0, 1):
        run = RUN.format(v=version)
    else:
        # (a pre-release, 0.2.0-rc.1, gets no `<major>.<minor>` tag)
        tags = f"`{version}`" if "-" in version else f"`{version}` and `{m.group(1)}.{m.group(2)}`"
        run = RUN_RUST.format(v=version, tags=tags)
    return section(version, changelog) + "\n" + run


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    sys.stdout.write(notes(sys.argv[1]))
