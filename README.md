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

| Piece | State |
|---|---|
| Shared protocol (`protocol/`) | Done, tested |
| Server: accounts, Google/GitHub sign-in, organisations, invites, project roles | Done, tested. Providers are faked in tests; the endpoints match what Google and GitHub publish, but no real sign-in has been done (needs OAuth app credentials) |
| Server: live documents — ordering, locks, id hand-out, conflicts, reconnect, presence | Done, tested |
| Server: history, undo/revert, who-broke-what, named versions, restore, snapshots | Done, tested |
| Server: assembly documents (components pinned to a part's version, where-used, update notices) | Done, tested. The openswx side of assemblies is not designed yet |
| Seat limits | Done, tested, in a separate private package (not part of the open server). Set by the server admin; no billing |
| End to end with two real openswx processes (`tools/e2e_two_peers.py`) | Passes |
| PostgreSQL | Whole suite passes on PostgreSQL 16 (`tools/test_postgres.py`) |
| S3 storage | Tested against a fake S3 (`tests/test_storage.py`) and against a real S3 server (Garage 2.5, in the Compose stack) |
| Docker deployment (`deploy/`) | Image built and the Compose stack run on a Raspberry Pi (arm64, Debian 13): PostgreSQL alone, and PostgreSQL + Garage. The two-openswx check passes against it and data survives a restart. CI builds and starts it on every push |
| openswx client (toggle, sync, cursors, previews) | Not started |
| Assemblies in openswx, SSO, billing, browser-only viewers | Later; see `docs/` |

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
