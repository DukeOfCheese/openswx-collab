"""Release notes for a version: its CHANGELOG.md section plus how to run it.

    python tools/release_notes.py 0.1.0 > notes.md      (the release workflow does this for each v* tag)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

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
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    return section(version, changelog) + "\n" + RUN.format(v=version)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    sys.stdout.write(notes(sys.argv[1]))
