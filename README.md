# openswx-collab

Live collaboration for [openswx](https://github.com/DukeOfCheese/openswx): several people in the same part at once, with
cursors, feature locks, full edit history and named versions. This repository holds the server and the
wire protocol; the in-app toggle and sync client live in openswx (branch `dev/openswx-collab`).

> **Not affiliated with Dassault Systèmes.** openswx and openswx-collab are independent open-source projects.
> They are not affiliated with, endorsed by, sponsored by or connected to Dassault Systèmes or SOLIDWORKS
> Corporation. SOLIDWORKS is a registered trademark of Dassault Systèmes SolidWorks Corporation. openswx is a
> clean-room implementation, and this repository contains no SOLIDWORKS code, icons, artwork or files. See
> [NOTICE](NOTICE).

## Status

**0.1.0 is released** (2026-11-23): `pip install openswx-collab-server`, or the Docker image
`ghcr.io/dukeofcheese/openswx-collab-server` (amd64 and arm64). See the
[release](https://github.com/DukeOfCheese/openswx-collab/releases/tag/v0.1.0) and [CHANGELOG.md](CHANGELOG.md).

| Piece | State |
|---|---|
| Shared protocol (`protocol/`, on PyPI) | Done, tested |
| Accounts: email + password, Google/GitHub sign-in, organisations, invites, project roles | Done, tested. Providers are faked in tests; no real Google or GitHub sign-in has been tried yet |
| Live documents: ordering, locks, id hand-out, conflicts, reconnect, presence | Done, tested |
| History: undo/revert, who-broke-what, named versions, restore, snapshots | Done, tested |
| Assembly documents (components pinned to a part's version, where-used, update notices) | Done, tested on the server. The openswx side of assemblies is not designed yet |
| Leaving: account and organisation deletion, data export | Done, tested |
| Email (invites, password reset), optional SMTP | Done, tested against a local SMTP server |
| Database migrations (Alembic) | Done; the server upgrades on start |
| Storage: SQLite or PostgreSQL 16; files in a folder or S3 | Whole suite passes on both databases; S3 tested with a fake and with Garage |
| Docker deployment (`deploy/`) | Published image; Compose stack run on a Raspberry Pi (arm64) and in CI on every change |
| openswx client (toggle, sync, cursors, previews) | In progress on openswx's `dev/openswx-collab` branch: sync and commands done and reviewed, UI being built |
| Hosted service extras (seat limits, Stripe billing) | In a separate private package; billing not yet tried against Stripe's test mode |
| Assemblies in openswx, company SSO, browser-only viewers | Later |

## How it works

Each person's openswx applies an edit locally and sends the resulting change to the document, not the
command. The server checks locks and conflicts, gives the change the next sequence number, stores it and
sends it to everyone else, who apply it and rebuild locally. The server never runs a geometry kernel, so
it is small and cheap to host. Design: `docs/2026-11-23-live-collaboration-design.md`. Protocol:
`docs/protocol.md`.

## Run it

```
python -m venv .venv
.venv/Scripts/python -m pip install -e protocol -e "server[dev]"      # bin/ instead of Scripts/ on Linux and macOS
.venv/Scripts/python -m openswx_collab_server serve                   # http://127.0.0.1:8787, SQLite in ./data
.venv/Scripts/python -m pytest                                        # the whole suite on SQLite
.venv/Scripts/python -m pip install -e "server[test]"                 # extra tools for the three checks below
.venv/Scripts/python tools/test_postgres.py                           # the same tests on a throwaway PostgreSQL
.venv/Scripts/python tools/check_deploy.py                            # the Dockerfile and Compose settings, without Docker
.venv/Scripts/python tools/e2e_two_peers.py ../openswx                # needs an openswx checkout with its .venv
```

The first account created on a server is its admin; everyone else joins through an invite link.
Self-hosting with Docker, Postgres, TLS and Google/GitHub sign-in: `docs/self-hosting.md`.

## Layout

- `protocol/` — `openswx_collab_protocol`: document operations (diff, apply) and message formats, used by
  the server and the client
- `server/` — `openswx_collab_server`: the FastAPI app
- `deploy/` — Dockerfile and Compose files
- `tools/` — the end-to-end check, the PostgreSQL test runner and the deployment check
- `spike/` — the throwaway experiment that proved the approach; kept for reference
- `tests/`

## Contributing and security

[CONTRIBUTING.md](CONTRIBUTING.md) has the rules (the clean-room rule first) and how to report a problem so it
can be acted on. Vulnerabilities are reported privately: [SECURITY.md](SECURITY.md). Notes for coding agents:
[AGENTS.md](AGENTS.md). Changes: [CHANGELOG.md](CHANGELOG.md).

## Licence

Apache-2.0 (see [LICENSE](LICENSE) and [NOTICE](NOTICE)). The server is complete as it is here. What a
hosted service adds on top (seat limits so far) is a separate package that plugs in through
`server/openswx_collab_server/hooks.py`.
