# Collaboration rebuild: design

Date: 2026-11-25. Status: approved direction; details marked *builder's call* are open.

## Intent

Collaboration in openswx should be a first-class part of the product, not an add-on. One codebase has to
serve two audiences:

- **Personal / self-hosted:** download one program, run it, done. SQLite and local files, no other services.
- **Cloud / Enterprise (paid):** tens of thousands of users, many server machines, no lost work, and the
  controls a company expects.

For the person using openswx it should feel like a synced drive: the file lives in the Cloud, opens like
any other file, saves itself, works offline, and sharing is a button on the file.

## Why now

The current server is a single Python process. Its model is right, but each change costs work in
proportion to the whole document, it cannot run on a second machine, and the client blocks on the server
for every command. Details are in "What exists today".

## Goals

1. A Rust server whose cost per change depends on the size of the change, not of the document.
2. The same binary self-hosts on SQLite and runs as many machines on PostgreSQL + S3-compatible storage.
3. No acknowledged change is ever lost, including when a machine dies.
4. Two people editing different things in the same feature or sketch do not conflict.
5. openswx applies its own edits at once and other people's edits without reloading the document.
6. Cloud parts open and can be edited offline once they have been opened before.
7. Numbers: every claim about speed is backed by the load generator, before and after.

## Non-goals

- Branching and merging (history is shaped to allow it later; see "History").
- Keeping the current wire protocol or old clients working. This is a clean break.
- A geometry kernel on the server. The server still treats documents as data.
- A browser-only client.

## Anti-goals

- A server that can accept a change which breaks the feature tree (dangling reference, duplicate name,
  cycle in an assembly). This is why a CRDT core was rejected.
- Cloud and self-hosted behaving differently in anything a user can see.
- Paid code in the public repository.
- A rewrite that ships without measurements showing it is faster.

## What exists today

Server (`server/openswx_collab_server/`, about 3,000 lines of Python, FastAPI):

- One `Room` per open document, one `asyncio.Lock` per room, every change through `Room._commit`.
- A change is accepted if nobody else changed any of its targets (`f:<id>`, `k:<key>`, `order`) after its
  `base_seq`; otherwise it is refused. No transformation.
- Per change it copies the feature list (`_writable_copy`), runs a whole-document `diff` to get the
  inverse, scans all names, and holds the lock across the database commit and blob checks. Every 200th
  change snapshots the whole document under the lock. Loading a room reads the targets of its entire
  history. Password hashing runs on the event loop. Access is rechecked by polling every 30 s.
- Rooms, locks, id reservations and rate limits are in process memory. One process only.
- No metrics, audit log, SSO, quotas or admin UI. The paid layer (`openswx-collab-ee`, private) plugs in
  through `hooks.py`.

Client (`openswx/collab/` in the openswx repository):

- Each command serialises the whole document before and after, diffs, then waits up to 15 s for the
  server while holding the application lock.
- Each remote change replaces the document (`load_dict`) and rebuilds, under the same lock.
- Cloud parts cannot be opened offline; new features need a connection (ids come from the server);
  assemblies cannot be shared.

Only one measurement exists: 40 editors on one part, 89 ms median and 250 ms p95, laptop, SQLite.

## Approaches considered

| | How it works | Trade-off |
|---|---|---|
| **A. One server machine per open document** (chosen) | The open document is held in memory by one machine, which orders and stores its changes; an ownership record lets other machines forward connections to it and take over if it dies. | Fastest per change; same code self-hosts. Takeover and deploy hand-off need careful engineering. |
| B. Any machine writes, database orders | Each change is a conditional insert; machines notify each other over a bus. | Simple failover, but a database round trip per change and a bus that only Cloud has. |
| C. CRDT core | Everything merges automatically. | Cannot refuse invalid states; history grows without bound. |

A was chosen because speed and headroom are the stated goal and it is the model Figma, tldraw and Onshape
run in production. A full Rust server (rather than Python with a Rust hot path) was chosen for the same
reason and because a single static binary is the best self-hosting experience.

## Decisions made with the owner

- Full Rust server; clean break in protocol and storage, with a one-off import tool.
- Paid features: the private repository is a Rust crate that depends on the public server library,
  implements its extension points, and builds the Cloud/Enterprise binary.
- Hosting stays portable: PostgreSQL, S3-compatible object storage, a load balancer. No Kubernetes
  requirement, no vendor-specific service.
- The openswx client may be reworked as far as needed, provided it is fast and ships on Linux, Windows
  and macOS.
- Synced-drive feel, including moving local parts and folders into the Cloud and back out.
- Offline: keep editing, sync on return, clashes listed for the user.
- Order: (1) Rust server and protocol v2, (2) client rework, (3) multi-machine, (4) enterprise features.

## Design

### Repository layout

```
openswx-collab (public, Cargo workspace)
  crates/core      document model, ops, wire codec. No async, no I/O.
  crates/core-py   PyO3 abi3 wheel "openswx-collab-core" (maturin)
  crates/server    library: rooms, store, blobs, accounts, orgs, access, HTTP + WebSocket API, Extensions
  crates/serverd   binary "openswx-collab-server": self-host
  crates/loadgen   binary: load generator and benchmarks
  crates/import    binary: converts a Python-server database and snapshots
```

The Python `protocol/` and `server/` stay in place until the Rust server passes the ported tests, then
are removed in one change (the last Python release is tagged).

### Document model and operations (`crates/core`)

- A document is a tree of maps, lists and scalars, as today. A **keyed list** is any list whose elements
  are all maps with a unique string `id`; `features`, `components`, `mates`, and lists inside feature
  params such as sketch entities and dimensions are keyed lists. Keyed lists are addressed by id, at any
  depth. Other lists are replaced whole.
- A **path** is a sequence of map keys and keyed-list ids from the document root.
- Operations (names are *builder's call*; the set is):
  - `set(path, value)`, `unset(path)` on map entries;
  - `insert(list_path, item, after_id)`, `remove(list_path, id)`, `move(list_path, id, after_id)` on
    keyed lists. `move` replaces today's whole-list `order` op.
- `apply` returns the new document and the inverse operations. Documents are structurally shared
  (`Arc`), so applying copies only the spine of what changed.
- `clean` rebuilds every operation from known fields and enforces the limits (depth 64, id and name
  length 200, no NaN or infinity).
- `writes(ops)` gives the set of paths a change writes. Structural writes (`insert`, `remove`, `move`)
  also write the list's order.
- `diff(before, after)` produces operations; used by the client and by restore.
- Invariants checked on apply: feature names unique; ids unique within a list; local-only and
  server-owned keys never written by clients.
- Feature ids are generated by the client as `<user id>.<client id>.<counter>`; the server checks the
  prefix against the connection (rule: ids are bound to the user). `ids.reserve` goes away.
- Names: the client proposes a name; if it is taken at commit, the server substitutes the next free
  number for that stem and reports the rename in the acceptance and in the broadcast change.

### Conflict rule

A change with base `b` is accepted if, for every path `p` it writes, no accepted change by another client
with `seq > b` wrote `p`, a prefix of `p`, or a path under `p`. Otherwise it is refused with the
conflicting paths and who wrote them. Accepted changes are applied to the current state (not the base
state); if they do not apply (for example the item was removed) they are refused as invalid.

Advisory locks ("X is editing this") stay, at path granularity, and remain in memory; clients re-take
them after reconnecting.

### Wire protocol v2

- WebSocket binary frames, MessagePack maps with a `t` field for the message type. REST stays JSON.
- `hello` carries the protocol version, client id, document format version and `last_seq`; the server
  answers `welcome` with either the changes since `last_seq` or a snapshot (zstd MessagePack), plus peers.
- Message set carried over from v1: change / accepted / rejected / change broadcast, revert (undo, redo),
  lock / unlock / lock.changed, presence, draft, rebuild.report / rebuild.status, version.created,
  reference.update, ping / pong, peer.joined / peer.left. Removed: `ids.reserve`.
- New: `moved` close code telling the client to reconnect (drain and takeover).
- Limits as today (8 MB per change, 256 MB per blob, 16 KB presence, 128 KB draft, 60 presence/s).
- Blobs: raw bytes, content-addressed by SHA-256 of the raw bytes, stored zstd-compressed, streamed both
  ways. The client stops base64-wrapping geometry before hashing. *Builder's call:* direct-to-S3
  presigned transfer for Cloud.
- `docs/protocol.md` is rewritten as the contract, with test vectors (bytes in, bytes out) that both the
  Rust tests and the wheel's tests run.

### Server (`crates/server`)

Stack (*builder's call*, proposed): tokio, axum, tokio-tungstenite, sqlx, object_store, argon2.

- **Room task.** One tokio task per open document with a bounded inbox. It owns the current document
  version, the last-writer index (path → seq, client), locks, and the connection list.
- **Commit pipeline.** clean → access, locks, conflict rule → apply to a new version → blob existence
  check (cached) → hand the record to the journal writer and continue with the next message, treating the
  new version as tentative → when the writer reports the batch durable: publish the version, broadcast
  the pre-encoded frame, acknowledge. If the write fails, tentative versions are discarded and their
  senders get `server_error`. Nothing that can fail happens after durability.
- **Journal writer.** One per store. Collects records from all rooms and commits them in one transaction
  every few milliseconds or when a batch fills (*builder's call* on the numbers; start at 2 ms / 256).
- **Store trait** with `SqliteStore` and `PgStore`. Operation records are stored as MessagePack bytes
  with `document_id, seq, parent_seq, branch_id, author_id, client_id, change_id, created_at, command,
  paths`. Unique on `(document_id, client_id, change_id)` for idempotent resends; the room keeps recent
  change ids in memory so the common case needs no lookup.
- **Snapshots.** Written in the background from an immutable version when a document has accumulated N
  changes or bytes, when it unloads, and at shutdown. A snapshot contains the document and the
  last-writer index, so loading reads one snapshot and the journal tail only.
- **Retention.** Per-org policy for how long old snapshots and journal records are kept; default keeps
  everything, as today. Deleted documents are purged after a grace period.
- **Accounts, orgs, projects, roles, invites, OAuth (Google, GitHub), password reset, account and org
  export/deletion:** carried over with the same behaviour and the same rules (404 not 403, hashed tokens,
  never log secrets). Password hashing runs on the blocking pool. Sessions and roles are cached in
  memory and invalidated by events, replacing the 30 s poll.
- **Folders** inside projects (new): a document has an optional parent folder; folders nest.
- **Assemblies, named versions, history, restore, undo/redo, rebuild reports:** carried over.
- **Extensions trait** (replaces `hooks.py`): `may_add_member`, `member_added`, `member_removed`,
  `before_org_delete`, `routes`, `migrations`, `authenticators`, `on_event` (audit sink), `quota`,
  `server_info`. `serverd` uses the no-op implementation.
- **Operations:** `/metrics` (Prometheus), structured logs, `/api/health`, `/api/ready` (database and
  storage reachable), graceful shutdown that flushes and snapshots.
- **Migrations** are embedded and run at start; every table change comes with one.

### History

Every journal record stores `parent_seq` and `branch_id`. Only one branch exists per document for now.
Named versions are labels on a seq. This is enough to add branches later without rewriting stored data.

### Multi-machine (project 3)

- Table `room_leases(document_id, node_id, epoch, expires_at)`. A node takes or renews a lease before
  loading a room. Journal writes carry the epoch and fail if it is no longer current.
- A node that receives a connection for a document it does not hold forwards the WebSocket to the holder
  over an internal connection. Nodes register their internal address in a `nodes` table.
- Drain (deploy): stop accepting, snapshot, release leases, close clients with `moved`.
- Crash: the lease expires; the next node to receive a connection takes it, loads the snapshot and
  journal tail. Clients resend unacknowledged changes with their original change ids.
- Cross-document events (access changed, version created, online lists) go through a `Bus` trait:
  in-process for one node, PostgreSQL notifications for many.
- With SQLite there is exactly one node and the lease is always held.

### Client (openswx repository, project 2)

- `openswx-collab-core` wheel replaces `openswx_collab_protocol`: abi3 for CPython 3.11+, built for
  Windows x86_64, macOS arm64 and x86_64, Linux x86_64 and aarch64. PyInstaller bundles are checked on
  all three systems.
- New sync engine replacing `openswx/collab/client.py`:
  - local edits apply at once; acceptance arrives in the background; a refusal rolls back that change
    and puts it in a "sync issues" list where it can be applied again or kept as a copy;
  - changes are captured by a diff in Rust against the engine's copy of the document; per-feature dirty
    tracking is added only if measurement shows the diff still matters;
  - remote changes patch the live document operation by operation and rebuild from the first affected
    feature; non-geometric keys do not rebuild;
  - solver output that each client recomputes stops being synced if it is shown to be deterministic
    across platforms; otherwise it is synced but excluded from the conflict rule;
  - a local store per cloud document (snapshot and pending queue) makes offline open and edit possible;
  - presence goes straight to the WebSocket; collab status reaches the page as deltas.
- Synced-drive UI: Cloud as a location in the file browser with folders; move local parts and folders
  in and out; autosave; sync status and pending count per file; Share on the file; viewer mode that
  hides edit tools. Cloud assemblies come last in this project.

### Enterprise (project 4, private repository)

SSO (OIDC and SAML) with enforcement, SCIM, exportable audit log, admin console, per-org quotas and
retention, Stripe billing and seats, region choice. Designed in detail after project 3.

## Targets

To be checked with `loadgen` on a stated machine; they are proposals until the baseline run exists.

- One node: 10,000 connections and 2,000 open documents, p99 time from change received to acknowledged
  under 20 ms on PostgreSQL.
- 200 editors in one document: p99 under 50 ms.
- Opening a 5 MB document: under 200 ms on the server.
- Killing a node: no acknowledged change lost; clients reconnected within 5 s.

## Verification

- `cargo test --workspace` on SQLite, and the same integration suite against PostgreSQL.
- Every scenario in today's `tests/` has a Rust counterpart, hostile cases included.
- Convergence test: simulated clients making random concurrent edits, then restart and replay.
- `loadgen` results recorded in the repository for the Python server (baseline) and the Rust server.
- Two real openswx processes end to end, including offline edit, reconnect and a refused change.
- Container and compose files checked on a real host.

## Amendments after review

An independent read found gaps. These rulings replace the text above where they differ. All are the
author's calls, not owner decisions.

1. **Dependent changes.** A change lists `depends`: the ids of the sender's own unanswered changes whose
   writes or reads overlap its own (or which created something it touches). The server refuses a change
   whose dependency was refused (`dependency`). The client rolls back a refused change together with
   everything that depends on it, newest first, using the inverses it kept.
2. **What list operations write.** `insert` writes only the new item's path. `remove` writes the item's
   path (so it conflicts with any concurrent edit inside the item). `move` writes the item's position
   only. Two inserts never conflict. Order is kept by a **position key** per item (a fractional index,
   as Figma does): `insert` and `move` carry a position between two neighbours, so there is no anchor
   that can go missing; equal positions are ordered by id. "Structural writes also write the list's
   order" is withdrawn.
3. **References.** The server has no schema of which fields are references. Instead a change carries
   `reads`: paths of items it relies on existing (the client knows them: a feature's sketch, a mate's
   components). The conflict rule gains a second half: a change is refused if another client removed
   anything in its `reads` after its base, and a `remove` is refused if another client's accepted change
   since the remover's base read the item. Assembly cycles and component access stay explicit checks as
   today. References the client does not declare are not protected; rebuild reports ("broken by") remain
   the backstop.
4. **Ids.** Every new id, at any depth (features, sketch entities, dimensions, components), is
   `<user>.<client>.<counter>`. The client id is per installation and stored with its counter, which
   never goes backwards. Ids that do not have this form are legacy ids: allowed only in a document's
   initial content and in imports, never in a new `insert`. The client shows names, not ids; where
   openswx shows entity ids today (`Line3`), it gains a per-sketch display label (project 2).

   An id can be re-inserted by anyone, at the path where a removal of that id is still remembered
   (the conflict index holds its tombstone), together with everything inside the item: that is how
   undoing a removal, or restoring someone else's deleted item, gets past the id rule. Once the
   tombstone has been pruned, the re-insert is refused like any other foreign id. Ids already present
   under a Set's path may be rearranged within that Set; an item moved by Remove and Insert to a
   different list must be given new ids at every depth by the client.
5. **Names.** The server does not substitute names, because documents refer to features by name
   (`D1@Sketch1`). A clash at commit is refused with `name_taken`; the client engine renames (rewriting
   its queued changes) and resends without involving the user. Explicit renames by a user are refused
   the same way and shown. `next_id` and `counters` stop being server-owned and stop being synced.
6. **Journal writer.**
   - A record must not be able to fail on its own: duplicates are detected in the room before the
     tentative apply (in memory for everything since the last snapshot, by lookup for older bases).
     Records that touch assembly references run in their own savepoint and can be refused individually.
   - If a commit's outcome is unknown (timeout, lost connection), affected rooms discard tentative
     versions, reload from the store, and tell clients to reconnect.
   - Fencing: the batch transaction locks each room's lease row and checks the epoch inside the
     transaction; takeover bumps the epoch under the same lock. Expiry clocks are a hint, never the
     guard.
   - Snapshots are written only from published versions and keyed by `(document, epoch, seq)`.
   - A change refused because of a tentative change that is later discarded is accepted as a rare
     spurious refusal; the client resends it once automatically.
7. **Undo.** Undo is a local inverse sent as an ordinary change marked `undo_of`, so it works offline and
   follows the normal conflict rule. Journal records also keep `inverse` so an editor can revert anyone's
   change from history, as today.
8. **Import** converts full history (v1 operations map mechanically to v2), so restore and named
   versions keep working.
9. **Derived data.** Any map key beginning `~` holds data each client can recompute (solver output).
   It is synced last-writer-wins and never causes a conflict. The client decides what goes there.

## Left to the builder

Crate choices, operation and message field names, batch sizes, snapshot thresholds, table layout, test
layout, and the order of work inside each project.
