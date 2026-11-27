# Notes for coding agents

Read [CONTRIBUTING.md](CONTRIBUTING.md) first; its rules apply to you as to anyone. This file is what you
need to know to change this repository without breaking it.

## What this is

The collaboration server and wire protocol for [openswx](https://github.com/DukeOfCheese/openswx). The
openswx client lives in the openswx repository, not here. The server is a Cargo workspace (Rust, edition
2024); the Python server (`protocol/`, `server/`, `tests/`) is the released 0.1.x line, kept until openswx's
client speaks protocol 2, and is changed only for fixes to that line.

- `crates/core/` (`oswx-core`): the document model. No async, no I/O.
  - `value.rs` (documents: maps, plain lists, keyed lists; persistent, so a copy is O(1)), `path.rs`,
    `pos.rs` (positions in keyed lists), `limits.rs`.
  - `op.rs` (the five operations, `writes`, `removed`), `apply.rs` (apply with its inverse and the rules),
    `diff.rs`, `index.rs` (who last wrote what: the conflict index), `log.rs` (`decide`: judges a change).
  - `wire.rs`: protocol v2 messages (MessagePack), every field typed and bounded. `vectors/`: wire test
    vectors. `tests/convergence.rs` and `tests/common/client.rs` (the reference client behaviour).
- `crates/server/` (`oswx-server`): the server as a library.
  - `state.rs`: `AppState`, `app`, and `commit_then` (a commit and what follows it, in a detached task).
  - `room/`: one task per open document. **Every change to a document goes through `room/change.rs`**
    (judge, write to the journal, then broadcast). `conn.rs` (a connection's queue and writer; soft
    frames), `locks.rs`, `presence.rs`, `history.rs` (undo, revert), `rebuild.rs`, `people.rs`,
    `welcome.rs`, `lifecycle.rs` (unload, snapshots).
  - `journal.rs` (the one writer that makes changes durable, in batches), `lease.rs` (who holds a
    document, fenced by epoch), `hub.rs` (open rooms; drops connections whose access ended), `ws.rs` (the
    WebSocket handshake), `snapshot.rs`, `record.rs`.
  - `routes/`: REST (accounts, organisations, projects, folders, documents, blobs, history, assemblies,
    leaving). `access.rs`, `security.rs`, `sessions.rs`, `oauth.rs`, `mail.rs`: accounts and roles.
    `org_queue.rs` (writes one at a time per organisation), `purge.rs` (deletion's second step),
    `export.rs`, `uploads.rs`, `assemblies.rs`, `content.rs`.
  - `store/` (the `Store` trait; `sqlite.rs`, `postgres.rs`, shared SQL in `sql.rs`), `blobs.rs` and
    `blobs/s3.rs` (files), `migrations/{sqlite,postgres}/` (the schema, embedded in the binary).
  - `serve.rs` (the serving loop: time limits, connection limits, stopping), `config.rs` (settings),
    `metrics.rs`, `admin.rs` (`migrate`, `db-status`, `create-admin`), `ext.rs` (`Extensions`: what a paid
    edition plugs in), `error.rs` (`ApiError`), `clock.rs`.
  - `tests/`: integration tests, each with a real server (`tests/common/mod.rs`: `TestServer`, `World`,
    `Live`, a v2 client). `hardening.rs` has the hostile-input pattern.
- `crates/serverd/` (`oswx-serverd`): the program `openswx-collab-server` (command line, logging, signals,
  exit codes). `run(ext)` is the whole program, for an edition's own `main`.
- `crates/loadgen/`: the load generator. Results: `bench/results/README.md`.
- `deploy/`: `Dockerfile.rust` and `docker-compose.rust*.yml` (the Rust server); `Dockerfile` and
  `docker-compose.yml` (the Python server); `docker-compose.s3.yml` (Garage, for either).
- `tools/`: `test_postgres_rust.py` (the server's suite on a throwaway PostgreSQL), `check_deploy_rust.py`
  (the Rust deployment files; `--docker` builds and runs them), and the Python line's tools.
- `docs/protocol.md` is the contract with the client. Keep it true: `crates/core/tests/protocol_doc.rs`
  fails when a message, field or code of `wire.rs` is missing from it.

## Rules that are easy to break

1. **The server has no geometry kernel and does not depend on openswx.** It treats documents as data and
   never interprets a feature's type or parameters.
2. **A change is all or nothing.** In the room, a change is judged against the tentative head and handed
   to the journal; only when the journal reports it durable does the durable head move, then the change is
   broadcast and its sender told `change.accepted`. Nothing that can fail runs after that report. In REST,
   everything that can fail happens before the commit; what follows it (revoking connections, events,
   mail) goes in `commit_then` and must not be able to undo or fail the request.
3. **State is shared between versions.** Documents, the conflict index and the room's heads are
   persistent values: a change makes a new version that shares what it did not touch. Never mutate one in
   place. If you add or change an operation, `writes()`, `removed()` and `apply()` must agree about
   everything it touches, or conflicts go undetected.
4. **Never trust a field a client sent.** Messages are rebuilt from known fields by `wire.rs`, operations
   by `op.rs`, REST bodies through `ApiJson`/`ApiPath`/`ApiQuery`; every field has a type and a size limit,
   and NaN, infinity, binary data and the zero character are refused. A new field needs both, and a hostile
   test beside the friendly one.
5. **Ids are bound to the user.** The server knows a connection as `<user id>.<client id>`, and new ids
   must start with it. Key per-client state on the connection's key, never on what `hello` carried.
6. **A project you cannot open does not exist for you**: answer 404, not 403, and never put its name in a
   message that other people receive.
7. **Tokens are stored hashed** (SHA-256). Never log a token, a password, an invite, a login code or
   document content; a panic is logged with its location only. `Debug` of anything holding a secret
   redacts it.
8. **Every table change comes with a migration**, written for SQLite and for PostgreSQL in
   `crates/server/migrations/`, in SQL both read alike (`$n` placeholders, no dialect functions) unless the
   two files must differ. The tests compare the two schemas. Never edit a migration that has been
   released; add a new one. An extension's migrations are `(id, sql)` pairs, the same ids for both
   databases.
9. **One node holds a document.** An open document, its locks, presence and recent change ids live in the
   memory of the node that holds its lease (`room_leases`, checked by epoch in every journal write). Do not
   add anything that assumes another process can see or change that memory, and do not run several server
   processes against one database (several machines are a later plan).

## Before you finish

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -j 2 -- -D warnings
cargo test --workspace -j 2                     # SQLite; files in a folder and in an in-process S3
python tools/test_postgres_rust.py              # the server's suite on PostgreSQL
```

(`-j 2` on a machine with 16 GB: a full parallel test build can run the compiler out of memory.) A change
to the sync core (`crates/core`, `room/`, `journal.rs`) also needs `crates/core/tests/convergence.rs` and
`crates/server/tests/convergence.rs` to pass, and `crates/server/tests/performance.rs` (ignored by default)
or a loadgen run when it could change speed. A change to `deploy/` needs `python tools/check_deploy_rust.py`
(and `--docker` where Docker is available).

For everything a client can send, write the hostile test as well as the friendly one. Update
`docs/protocol.md` and `CHANGELOG.md` with the change; `docs/self-hosting.md` with a setting.
