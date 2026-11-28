# Changelog

User-visible changes, newest first. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/) (before 1.0, a minor version may change the
protocol or the database; the change is listed here).

## Unreleased

### 0.2.0 (not yet released): the Rust server and protocol version 2

The server is rewritten in Rust (`crates/`): one program, `openswx-collab-server`, on SQLite and files in a
folder for a self-hosted install, or on PostgreSQL and an S3-compatible bucket. A change now costs work in
proportion to the change, not to the document, and the server keeps up where the Python server fell
behind. **Protocol version 2 is a clean break**: version 1 clients (openswx 0.4) are refused with close
code 4426 and keep using the Python server (0.1.x, still in this repository) until openswx's client moves
to version 2. A Python server's database is not opened by the Rust server, and there is no import.
`docs/protocol.md` is rewritten as the contract; `docs/self-hosting.md` describes the new settings.

#### Protocol 2
- WebSocket frames are binary MessagePack maps with a `t` field. REST stays JSON. Wire test vectors are
  in `crates/core/vectors/`.
- Documents are edited at any depth: a **keyed list** (any list of maps with distinct ids: `features`,
  `components`, `mates` and lists inside parameters alike) travels as `{"$list": [[pos, item], ...]}` and
  is addressed by id. Five operations, `set`, `unset`, `insert`, `remove` and `move`, with positions
  instead of whole-list reordering. Two edits conflict only when one writes what the other wrote, above or
  under it, or removes what the other `reads`; edits to different parameters of one feature both go in.
- Ids are made by the client, `<user id>.<client id>.<counter>`, so features can be created offline;
  `ids.reserve` is gone. Feature names are checked at the end of a change (`name_taken`).
- `change` carries `reads` and `depends`. Rejection codes: `stale`, `locked`, `dependency`, `name_taken`,
  `id_taken`, `invalid`, `missing_blob`, `read_only`, `too_old`, `server_error` (not stored: send it
  again). A resend of a stored change id is answered with its seq, however old; an `accepted` for a change
  already settled is a no-op and never moves the client's seq backwards. A client receives its own change
  as an event in a catch-up, or when it became durable before it was resent after a reconnect or takeover.
- Undo is an ordinary change with `undo_of`, judged with the inverse the server stored; revert of anyone's
  change from history is `change.revert`.
- Locks on any path (`lock.acquire`, `lock.result`, `lock.release`, `lock.changed`), with quotas and a
  rate per connection; a lock on a list and locks on its items do not exclude each other. Lock frames may
  name peers a client fell behind on.
- Presence and drafts are relayed at most every 50 ms per document, each peer's latest, several at once
  in `presence.batch`. For a client that does not read fast enough, peer, presence and draft frames are
  dropped instead of closing it, and it is then sent `peer.list` (every peer as now, the recipient
  included, no drafts, presence up to 1 MiB; the rest follows). A reader that stops reading altogether is
  closed (4408): at most 30 s + 4 MiB / 16 KiB/s (under 5 minutes) after it stopped taking frames, plus
  the time of anything it was handed since. A draft may arrive up to a tick after the change that ends it.
- New close codes: 4410 (the document was reloaded or moved, or its room stopped unexpectedly: reconnect
  and resend) and 4429 (too many document connections for the account, or `server_full` for the
  server). A connection may have at most
  2,000 unanswered changes (4408).
  A client frame may hold at most 1,000,000 values; only a `change` may be longer than 512 KiB. Messages
  waiting behind one of the connection's own that the server is still working on: at most 1,024, or
  4 × `max_change_bytes` (4408).
- Assemblies: `GET .../references` reports the versions at a pinned seq (`pinned.versions`, to be
  preferred over a component's own `ref.version` label).

#### Added
- Folders in projects (`/api/projects/{id}/folders`, `/api/folders/{id}`; documents get `folder_id`).
- `GET /api/ready` (database, storage and journal), `GET /metrics` (Prometheus, behind
  `OSWX_COLLAB_METRICS_TOKEN`), JSON logs (`OSWX_COLLAB_LOG=json`), `OSWX_COLLAB_LOG_LEVEL`.
- `GET /api/me/providers`, `POST /api/me/providers/{name}/unlink`, `POST /api/auth/oauth/link/finish`.
- Settings (all `OSWX_COLLAB_*`; `--help` lists every setting with its default): `DATABASE_POOL`,
  `DATABASE_JOURNAL_POOL`, `DATABASE_LOCK_TIMEOUT`, `DATABASE_JOURNAL_LOCK_TIMEOUT`, `S3_PATH_STYLE`,
  `BLOB_IDLE_TIMEOUT`, `UPLOAD_MEMORY`, `MAX_BLOB_UPLOADS_PER_ORG`, `MAX_BLOB_UPLOADS_PER_USER`,
  `MIN_UPLOAD_RATE`, `UPLOAD_RATE_GRACE`, `UPLOAD_TIME_LIMIT`, `UPLOAD_TIME_RATE`, `TRUSTED_PROXIES`,
  `REQUEST_TIMEOUT`, `WRITE_TIMEOUT`, `SHUTDOWN_TIMEOUT`, `MAX_CONNECTIONS` (at most 65,536),
  `MAX_CONNECTIONS_PER_ADDRESS`, `MAX_LARGE_BODIES`, `MAX_MAIL_SENDS`, `MAX_EXPORTS`, `METRICS_TOKEN`,
  `LOG`, `LOG_LEVEL`, `PASSWORD`.
- `deploy/Dockerfile.rust` and `deploy/docker-compose.rust.yml` (SQLite), with
  `docker-compose.rust-postgres.yml` (PostgreSQL) and the existing `docker-compose.s3.yml` (Garage) on top;
  `tools/check_deploy_rust.py` checks them, and with `--docker` builds and runs each. The image's base
  images are pinned by digest. Building needs Rust 1.89 or later (`rust-version`).
- Extensions (`crates/server/src/ext.rs`) replace `hooks.py`: a paid edition is a crate that depends on the
  server library and calls `oswx_serverd::run`; its migrations are `(id, sql)` pairs for each database. An
  extension's refusal answers 402; one that does not answer within 5 s, 409.
- The load generator (`crates/loadgen`) speaks both protocols; results in `bench/results/README.md`.

#### Changed
- Every change is written by one journal writer, in batches, before it is acknowledged; a document judges
  its next change while the previous one is being written. A document is held by one node under a lease
  fenced by epoch, ready for several machines later. If the outcome of a write is unknown, the document
  reloads and its clients reconnect (4410) and resend; nothing acknowledged is lost.
- Snapshots are written in the background, from durable versions only; loading a document reads one
  snapshot and the journal after it.
- Blobs are hashed and stored as their raw bytes (zstd-compressed in storage), streamed both ways.
- Requests that change an organisation's data run one at a time per organisation, queued in the server
  (at most 64 waiting, 10 s each; then 503 "the server is busy; try again"). On PostgreSQL a request that
  loses to another, or waits 5 s for its lock, answers 409 "try again"; one that finds no connection, 503.
- Deleting a project or an organisation is two steps: it is gone for everyone at once, and its rows and
  files are removed in the background, a chunk at a time, resuming after a restart; what cannot be
  removed yet, or listed, is tried again later, waiting twice as long after each failure.
- Email is sent after the commit; `emailed: false` in an invite's answer may also mean "still on its way",
  or that the organisation (50 an hour) or the account (200 a day) has had as many invites emailed as it
  may. An organisation has at most 1,000 open invites (409 beyond).
- Exports: organisation exports and account exports have a pool of places each (`MAX_EXPORTS` of each,
  default 2), and a client that reads an export more slowly than `MIN_UPLOAD_RATE` on average (after
  `UPLOAD_RATE_GRACE`) loses it, as one that stops reading does.
- An extension's quota hook is asked for `Members` (with the seat hook) and for `BlobBytes` before a blob
  that is not stored yet is uploaded (its declared size, or `MAX_BLOB_BYTES` without one).
- `GET /api/ready` gives its answer again for a second and checks once at a time; it logs why the server
  is not ready when that starts, then at most once a minute.
- Sessions and roles are dropped on the event that ends them (sign-out, password change, role change,
  deletion) as well as by a 30 s check; a session that expires while connected closes and releases its
  locks at once.
- Settings are checked at start: a value that cannot be read stops the program with a message that names
  the variable; booleans must be one of the known spellings. `db-status` exits 1 on a missing SQLite file
  (it creates none), a database written by a newer version or one it cannot reach; an empty database
  exits 0 with "database at none". `serve` migrates before it listens. SIGHUP stops
  the server like SIGTERM.

#### Removed
- `OSWX_COLLAB_MAX_BLOB_UPLOADS`: replaced by `UPLOAD_MEMORY`; the server refuses to start while it is set.
- `ids.reserve` and server-issued feature ids and names; `next_id` and `counters` are no longer
  server-owned.

#### Security
- A `$blob` reference in a change that is not a SHA-256 is refused `missing_blob` before any storage
  lookup, and the document's room does not remember it.
- Every REST body and WebSocket field has a type and a size limit; requests have time limits from head to
  answer, bodies a minimum rate, uploads a memory budget and a duration limit; connections are limited in
  all and per address (a full server replaces idle and never-finished connections, and keeps a reserve
  for `/api/health`).
- `X-Forwarded-For` is believed only from `OSWX_COLLAB_TRUSTED_PROXIES` and is read from the right; the
  Rust Compose file trusts its own fixed subnet, so a TLS proxy on the host gives each client its own
  sign-in limits.
- Sign-in attempts are counted before the password is checked, so parallel requests cannot get past the
  limit (10 in 10 minutes per email address and client); one email address also has 100 attempts an hour
  from anywhere. A completed password reset lifts both limits on the address, from every client, so
  that nobody can keep an owner who reads their mail from signing in; a reset whose commit failed with an
  unknown outcome leaves them as they were. Sign-in, reset and OAuth limits
  count an IPv6 client by its /64.
- Document connections may take at most `MAX_CONNECTIONS` less an eighth (and less at least 256), so that
  they cannot leave REST without connections; one more is closed 4429 with the `error` code
  `server_full`. A document whose room stops unexpectedly gives back the places of all its connections,
  those of clients leaving at that moment included.
- The Rust Compose file raises the container's limit on open files to 65,536 (Docker's soft default has
  been 1,024 since version 25); the server raises its soft limit to the hard limit at start and warns
  when that is below what `MAX_CONNECTIONS` needs, and when out of descriptors closes an idle connection
  for each one it cannot accept (logged at most once a minute).
- Invites are limited as a mail relay: 50 emailed an hour per organisation, 200 a day per account.
- The cost of stored password hashes is bounded and hashing runs off the server's main threads; logs never
  hold a token, password, invite, login code or document content, and a panic is logged without its
  message.

#### Performance
Measured with `loadgen` on one laptop (Windows 11, i7-11800H), with the scenarios of the Python baseline
(`bench/results/README.md`; service time p50 / p99 in ms, median runs):

| Scenario | Python 0.1.2 (SQLite) | Rust, first runs (SQLite · PostgreSQL) | Rust after the follow-up (SQLite · PostgreSQL) |
|---|---|---|---|
| 10 editors in one document | 5.1 / 9.7 | 18.6 / 21.6 · 17.2 / 18.8 | 1.7 / 3.4 · 1.8 / 3.4 |
| 100 editors in one document | 532 / 590 | 19.2 / 34.6 · 17.7 / 20.0 | 2.3 / 9.7 · 2.2 / 4.6 |
| 200 editors in one document | 1,851 / 3,152 (104 of 400 edits/s) | 16.7 / 39.6 · 15.7 / 23.2 | 3.7 / 18.0 · 4.2 / 10.5 (all 400/s) |
| 200 documents x 3 editors | 1,637 / 4,497 (337 of 600 edits/s) | 19.1 / 48.8 · 18.7 / 21.9 | 3.1 / 23.8 · 2.8 / 7.1 (all 600/s) |
| 2,000 idle peers in one document | failed (all dropped) | 1 of 3 runs dropped peers; about 690 CPU-s, 700 MB | none dropped; 12 to 15 CPU-s, about 390 MB |
| 10,000 connections on 2,000 documents | not run | p99 1,165 · 186; about 1.8 GB | p99 66 · 105; about 520 MB |

The follow-up removed a timer wait from the journal writer (about 15 ms on Windows), relays presence once
per tick with soft frames for slow readers, cut a connection's memory from about 170 KB to about 21 KB
(an 8 KiB WebSocket read buffer), and opens the PostgreSQL pools' first connections at start (the first
open of a document on PostgreSQL takes 16 to 64 ms instead of 52 to 109). The proposed 20 ms p99 at 10,000
connections is still missed on this machine, and Linux was not measured.

## 0.1.2 - 2026-11-24

A faster server: each message is encoded once for everyone, big documents no longer hold up the others, and a
client is no longer dropped after receiving one very large message. The protocol is unchanged (version 1).

### Changed
- The server does less work per message: a change, cursor or preview is encoded once for everyone in the
  document instead of once per person.
- Large documents no longer hold up everyone else in them: saving a snapshot, loading one, and downloading a
  document's current content happen off the server's main loop, and the current content is no longer copied
  first.
- Reconnecting reads only what it needs from the history, a change that uses several imported bodies checks
  them all at once, and the list of organisations counts members in one query however many there are.

### Fixed
- A client that had just received one very large message (a welcome with a big document) could be dropped
  as "too slow" by the next small message. Only what waits behind a message counts now.

## 0.1.1 - 2026-11-24

The document list says who is in each document right now, for openswx's Home page, and the password-reset page
is fixed. The protocol is unchanged (version 1); openswx 0.3 and 0.4 work with this server.

### Added
- `GET /api/projects/{id}/documents` says who is in each document right now: `online` (name and
  colour, one entry per person, at most 8) and `online_count`.

### Security
- The password-reset page no longer writes the token from its address into the page (a crafted link
  could run script there). The page reads the token itself, and is never cached.

## 0.1.0 - 2026-11-23

The first release: the server and the protocol. The openswx side (the in-app toggle, sync and live
cursors) is not released yet; it is being built on openswx's `dev/openswx-collab` branch.

### Added
- The wire protocol (`openswx-collab-protocol`): id-keyed operations on features, components and mates,
  `diff` / `apply`, and the WebSocket messages. Protocol version 1.
- Accounts: email and password, sign-in with Google or GitHub for desktop apps, sessions, invites.
  The first account on a server is its admin; everyone else joins by invite unless registration is opened.
- Organisations with owner / admin / member roles, projects with owner / editor / viewer roles.
- Cloud documents: turn a part or an assembly into a shared document; large imported bodies travel as
  content-addressed blobs.
- Live editing: the server orders changes, refuses conflicting ones and says who got there first; feature
  locks with a grace period across short disconnects; server-issued feature ids and names; catch-up after a
  reconnect; cursors, selections, active tools and in-progress previews relayed to everyone in the file.
- History: every change with its author; per-user undo and revert of any change; rebuild failures pinned on
  the change that caused them; named versions; restore to any point.
- Assemblies: components use parts by reference, pinned to a point of the part's history; where-used;
  a notice when a part gets a newer named version.
- Storage on SQLite or PostgreSQL, with files in a folder or an S3-compatible bucket; Dockerfile and
  Compose files.
- Extension hooks (`hooks.py`) for what a hosted service adds on top, such as seat limits.
- Database migrations (Alembic): the server upgrades its database on start; `openswx-collab-server migrate`
  and `db-status` for operators who turn that off.
- Deleting your account (your work stays with the organisation) and exporting your data; deleting an
  organisation with everything in it, and exporting it as a zip.
- Optional email over SMTP: invites made for an address are emailed, and people can reset a forgotten
  password themselves.
