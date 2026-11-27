# openswx-collab

Live collaboration for [openswx](https://github.com/DukeOfCheese/openswx): several people in the same part at once, with
cursors, feature locks, full edit history and named versions. This repository holds the server and the
wire protocol; the client shipped in openswx 0.4.0 (refined in 0.4.1): it is in the desktop installers, and from
source it is `pip install openswx[collab]`.

> **Not affiliated with Dassault Systèmes.** openswx and openswx-collab are independent open-source projects.
> They are not affiliated with, endorsed by, sponsored by or connected to Dassault Systèmes or SOLIDWORKS
> Corporation. SOLIDWORKS is a registered trademark of Dassault Systèmes SolidWorks Corporation. openswx is a
> clean-room implementation, and this repository contains no SOLIDWORKS code, icons, artwork or files. See
> [NOTICE](NOTICE).

## Status

**0.1.2 is released** (2026-11-24), the Python server: `pip install openswx-collab-server`, or the Docker
image `ghcr.io/dukeofcheese/openswx-collab-server` (amd64 and arm64). It speaks protocol 1, which openswx
0.4 uses. See the [release](https://github.com/DukeOfCheese/openswx-collab/releases/tag/v0.1.2) and
[CHANGELOG.md](CHANGELOG.md).

**0.2 is the Rust server** (`crates/`), not released yet: the same features, rebuilt so that a change costs
work in proportion to the change rather than the document, with protocol 2 (MessagePack over the
WebSocket, edits at any depth, offline-friendly ids, undo as an ordinary change). openswx's client moves to
it in its next rework; until then the Python server stays here for openswx 0.4.

| Piece | State |
|---|---|
| Document model and protocol 2 (`crates/core`) | Done, tested: operations, conflict index, wire codec with test vectors, convergence tests |
| Accounts: email + password, Google/GitHub sign-in, organisations, invites, project roles, folders | Done, tested (Rust); Google and GitHub checked by hand with real OAuth apps against the Python server |
| Live documents: ordering, locks, conflicts, reconnect, presence | Done, tested; under load (`bench/results/README.md`) 200 editors in one document p99 10 to 18 ms, Python 3.2 s |
| History: undo/revert, who-broke-what, named versions, restore, snapshots | Done, tested |
| Assembly documents (components pinned to a part's version, where-used, update notices) | Done, tested on the server. The openswx side of assemblies is not designed yet |
| Leaving: account and organisation deletion (two-step), data export | Done, tested |
| Email (invites, password reset), optional SMTP | Done, tested against a local SMTP server |
| Storage: SQLite or PostgreSQL 16; files in a folder or S3 | Whole suite passes on both databases; S3 tested with an in-process server and with Garage |
| Operations: `/metrics`, JSON logs, health and readiness, orderly stop, connection and upload limits | Done, tested |
| Docker deployment (`deploy/`) | Rust image and Compose files (SQLite; PostgreSQL; + Garage) run on a Raspberry Pi (arm64) and in CI; the published image is still the Python server |
| openswx client (toggle, sync, cursors, previews) | Shipped in openswx 0.4.0, refined in 0.4.1, against the Python server (protocol 1); the protocol 2 client is the next project |
| Hosted service extras (seat limits, Stripe billing) | In a separate private crate through the `Extensions` trait; billing not yet tried against Stripe's test mode |
| Several server machines, assemblies in openswx, company SSO | Later |

## How it works

Each person's openswx applies an edit locally and sends the resulting change to the document, not the
command. The server checks locks and conflicts, gives the change the next sequence number, stores it and
sends it to everyone else, who apply it and rebuild locally. The server never runs a geometry kernel, so
it is small and cheap to host. Design: `docs/superpowers/specs/2026-11-25-collab-rebuild-design.md` (and,
for the Python server, `docs/2026-11-23-live-collaboration-design.md`). Protocol: `docs/protocol.md`.

## Run it

```
cargo run --release -p oswx-serverd -- serve                         # http://127.0.0.1:8787, SQLite and files in ./data
cargo test --workspace -j 2                                         # the whole suite on SQLite
.venv/Scripts/python tools/test_postgres_rust.py                    # the server's suite on a throwaway PostgreSQL
.venv/Scripts/python tools/check_deploy_rust.py                     # the Rust Dockerfile and Compose files
docker compose -f deploy/docker-compose.rust.yml --env-file deploy/.env up -d --build    # in Docker
```

The Rust server (`crates/server`) tests its S3 storage against an S3 server inside the test process, so
`cargo test` needs nothing else. To run its tests with every file kept in S3, or against a real store:

```
OSWX_TEST_STORAGE=s3 cargo test -p oswx-server                        # every test server on S3 (in-process)
# Against a store you have. The bucket must exist; each test works under a prefix of its own (t-<id>).
# For Garage as deploy/docker-compose.s3.yml sets it up, start just Garage with its S3 port published:
#   docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.s3.yml --env-file deploy/.env #     run -d -p 127.0.0.1:3900:3900 garage
OSWX_TEST_S3_URL=http://127.0.0.1:3900 OSWX_TEST_S3_BUCKET=openswx-collab OSWX_TEST_S3_REGION=garage OSWX_TEST_S3_ACCESS_KEY=<S3_ACCESS_KEY> OSWX_TEST_S3_SECRET_KEY=<S3_SECRET_KEY>   cargo test -p oswx-server --test storage_s3                         # add OSWX_TEST_STORAGE=s3 for the other tests
```

The Python server (0.1.x):

```
python -m venv .venv
.venv/Scripts/python -m pip install -e protocol -e "server[dev]"      # bin/ instead of Scripts/ on Linux and macOS
.venv/Scripts/python -m openswx_collab_server serve                   # http://127.0.0.1:8787, SQLite in ./data
.venv/Scripts/python -m pytest                                        # its suite on SQLite
.venv/Scripts/python tools/check_deploy.py                            # its Dockerfile and Compose settings
.venv/Scripts/python tools/e2e_two_peers.py ../openswx                # needs an openswx checkout with its .venv
```

The first account created on a server is its admin; everyone else joins through an invite link.
Self-hosting with Docker, PostgreSQL, S3, TLS and Google/GitHub sign-in: `docs/self-hosting.md`.

## Layout

- `crates/core/` — the document model, operations and the protocol 2 wire codec (no I/O)
- `crates/server/` — the server as a library: rooms, journal, store, storage, accounts, REST and WebSocket
- `crates/serverd/` — the program `openswx-collab-server`
- `crates/loadgen/` — the load generator; results in `bench/results/`
- `deploy/` — Dockerfiles and Compose files (Rust: `*.rust*`; Python: the others)
- `tools/` — the PostgreSQL test runners, the deployment checks and the end-to-end check
- `protocol/`, `server/`, `tests/` — the Python server 0.1.x and its protocol 1 library
- `spike/` — the throwaway experiment that proved the approach; kept for reference

## Contributing and security

[CONTRIBUTING.md](CONTRIBUTING.md) has the rules (the clean-room rule first) and how to report a problem so it
can be acted on. Vulnerabilities are reported privately: [SECURITY.md](SECURITY.md). Notes for coding agents:
[AGENTS.md](AGENTS.md). Changes: [CHANGELOG.md](CHANGELOG.md).

## Licence

Apache-2.0 (see [LICENSE](LICENSE) and [NOTICE](NOTICE)). The server is complete as it is here. What a
hosted service adds on top (seat limits so far) is a separate crate that plugs in through the
`Extensions` trait (`crates/server/src/ext.rs`).
