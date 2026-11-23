# Notes for coding agents

Read [CONTRIBUTING.md](CONTRIBUTING.md) first; its rules apply to you as to anyone. This file is what you
need to know to change this repository without breaking it.

## What this is

The collaboration server and wire protocol for [openswx](https://github.com/DukeOfCheese/openswx). The
openswx client lives in the openswx repository, not here.

- `protocol/openswx_collab_protocol/` — `ops.py` (operations: `diff`, `apply`, `clean`, `targets`) and
  `messages.py` (WebSocket messages). Installed by both the server and the client. Depends on pydantic only.
- `server/openswx_collab_server/` — the FastAPI app.
  - `hub.py`: one `Room` per open document. **Every change to a document goes through `Room._commit`.**
  - `documents.py`: REST for documents, blobs, history, versions; the WebSocket endpoint.
  - `assemblies.py`: components that reference parts.
  - `auth.py`, `orgs.py`, `access.py`, `security.py`: accounts, organisations, roles.
  - `db.py`: tables; `migrate.py` + `migrations/versions/`: how the schema gets there. `storage.py`: files. `hooks.py`: extension points (seat limits and the like plug in from outside).
- `tests/` — pytest, with a real app per test (`conftest.py`: `World`, `Live`).
- `tools/` — `e2e_two_peers.py`, `test_postgres.py`, `check_deploy.py`.
- `docs/protocol.md` is the contract with the client. Keep it true.

## Rules that are easy to break

1. **The server has no geometry kernel and does not import openswx.** It treats documents as JSON.
2. **A change is all or nothing.** In `_commit`, everything that can fail happens on a copy before the
   database write; the in-memory state changes only after the commit; the broadcast comes last. Do not add
   a step after the commit that can raise.
3. **State is shared between versions.** `_writable_copy` copies only what a change's `targets()` name.
   Never mutate `Room.state`, or anything reachable from it, in place. If you add an operation, `targets()`
   must name everything `apply()` touches for it.
4. **Never trust a field a client sent.** Operations are rebuilt from known keys by `ops.clean`. New
   message fields need a size limit and a type; anything stored or relayed must survive
   `json.dumps(allow_nan=False)`.
5. **Ids are bound to the user.** The server knows a connection as `<user id>.<client_id>`. Key new
   per-client state on `Conn.client_id`, never on what `hello` carried.
6. **A project you cannot open does not exist for you**: answer 404, not 403, and do not put its name in a
   message that other people receive.
7. **Tokens are stored hashed.** Never log a token, a password, an invite or a login code.
8. **Every table change comes with a migration.** Change `db.py`, then generate a revision against a
   database at the current head and read what it wrote (`tests/test_migrations.py` fails while the models
   and the migrations disagree). Never edit a revision that has been released.
9. **One process.** Rooms and locks live in memory. Do not add anything that assumes several workers.

## Before you finish

```bash
python -m pytest -q                 # SQLite
python tools/test_postgres.py       # PostgreSQL
```

A change to the sync core (`hub.py`, `ops.py`) also needs `tests/test_convergence.py` to pass and, where you
can, `tools/e2e_two_peers.py` against an openswx checkout. A change to `deploy/` needs
`python tools/check_deploy.py`.

For everything a client can send, write the hostile test as well as the friendly one
(`tests/test_hardening.py` has the pattern). Update `docs/protocol.md` and `CHANGELOG.md` with the change.
