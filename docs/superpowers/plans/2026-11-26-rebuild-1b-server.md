# Rebuild 1b: Rust Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `crates/server` and the `openswx-collab-server` binary: a Rust server with everything the Python
server does today, speaking protocol v2 on the WebSocket, on SQLite and PostgreSQL, local files and S3.

**Architecture:** axum on tokio. One task per open document owns its `oswx_core::Head` and decides
changes with `oswx_core::decide`; a single journal-writer task groups records from all documents into
one database transaction every few milliseconds and only then are changes published and acknowledged.
Accounts, organisations, projects and documents are plain REST handlers over a `Store` trait. Paid
features attach through an `Extensions` trait.

**Tech Stack:** Rust stable, tokio, axum 0.8 (WebSocket), sqlx 0.8 (sqlite, postgres, migrate),
object_store, zstd, scrypt, sha2, rand, uuid, serde/serde_json, rmpv, lettre, reqwest (rustls), zip,
tracing, metrics + metrics-exporter-prometheus; `oswx-core`.

**Spec:** `docs/superpowers/specs/2026-11-25-collab-rebuild-design.md` (amendments override earlier
text). **Parity checklist:** `docs/superpowers/specs/2026-11-25-python-server-inventory.md` — the
Python server's behaviour, section by section. Where a task says "inventory §N", that section is part of
the task's requirements, except where this plan or the spec says v2 differs.
**Carried in:** `docs/superpowers/plans/2026-11-25-rebuild-1a-carry-forward.md`, "For the server".

Not in this plan (1c): the import tool, removal of the Python server, release packaging.
Multi-machine operation (leases across nodes, proxying) is project 3; this plan writes the epoch into
the journal and snapshots so that project 3 changes no stored data.

## Global Constraints

- The server has no geometry kernel and never interprets feature types.
- A change is all or nothing: nothing that can fail runs after the journal commit; memory changes only
  after the commit; the broadcast comes last.
- Never trust a client field: every REST body field and every message field has a type and a size
  limit; REST bodies are refused over 1 MB except blob uploads and document creation (8 MB).
- Ids are bound to the user: a connection is `<user id>.<client id>`; per-client state is keyed on that.
- A project you cannot open does not exist for you: 404, not 403, and its name never reaches others.
- Tokens, passwords, invites, login codes and document content are never logged. Tokens are stored as
  SHA-256 hex.
- Every table comes from a migration embedded in the binary; SQL text is portable between SQLite and
  PostgreSQL (`$n` placeholders, no dialect functions) unless a task says otherwise.
- REST request and response shapes, status codes and error bodies are those of inventory §1, unless a
  task says v2 differs. Times in JSON are float epoch seconds. Ids are 32 lower-case hex characters.
- Limits: one change 8 MB; blob 256 MB; presence 16 KB; draft 128 KB; 60 presence or draft messages a
  second per connection; 256 locks per client; catch-up window 500 changes.
- Settings are read from `OSWX_COLLAB_*` environment variables with the names and defaults of
  inventory §9 (database and storage URLs take the forms `sqlite://path`, `postgres://…`, a folder
  path, `s3://bucket/prefix`; the Python `sqlite+aiosqlite:///` and `postgresql+asyncpg://` forms are
  also accepted).
- No `unsafe`. `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings` pass at
  every commit. No Claude or AI attribution in commits or pull requests. Work on a branch; never push
  to `main`.
- Python files are not modified. For everything a client can send there is a hostile test as well as a
  friendly one.

## Review Focus

1. A client that disconnects between sending a change and its acknowledgement resends it with the same
   `change_id` after a server restart: it must get the original seq, not a second application. Task 9.
2. The database fails or times out during a journal batch: no client is told "accepted", rooms reload
   from the store, and the next change works. Task 8.
3. Two connections of one user edit the same field: they are different clients and must conflict; the
   same client reconnecting must not conflict with itself. Task 9.
4. A member is removed from a project while connected: the socket closes at once with 4403 and nothing
   about the project reaches them afterwards. Task 12.
5. A document created by the Python server's shape of content (plain lists, legacy ids, `next_id`,
   `counters`) is created, opened, edited and restored. Tasks 7 and 11.

---

## File structure

```
crates/server/Cargo.toml
crates/server/migrations/{sqlite,postgres}/0001_init.sql     identical text where possible
crates/server/src/lib.rs            pub fn app(state) -> Router; re-exports
crates/server/src/config.rs         Settings::from_env
crates/server/src/error.rs          ApiError -> (status, {"detail": ...})
crates/server/src/state.rs          AppState (store, blobs, hub, mailer, ext, settings, limits)
crates/server/src/store/mod.rs      trait Store and row types
crates/server/src/store/sql.rs      shared SQL text and row mapping
crates/server/src/store/sqlite.rs   SqliteStore
crates/server/src/store/postgres.rs PgStore (Task 16)
crates/server/src/blobs.rs          trait Blobs, LocalBlobs, S3Blobs (Task 17)
crates/server/src/security.rs       passwords, tokens, rate limits, email, colours
crates/server/src/access.rs         current user, roles, project_role
crates/server/src/sessions.rs       session and role cache, invalidation events
crates/server/src/mail.rs           trait Mailer, SmtpMailer, MemoryMailer
crates/server/src/ext.rs            trait Extensions, NoExtensions
crates/server/src/routes/{meta,auth,oauth,orgs,projects,documents,blobs,history,assemblies,lifecycle}.rs
crates/server/src/journal.rs        JournalWriter
crates/server/src/snapshot.rs       encode/decode, load
crates/server/src/room/{mod,conn,locks,presence,history,rebuild}.rs
crates/server/src/hub.rs            Hub: open rooms, sweeper, drops, notify
crates/server/src/ws.rs             handshake and frame loop
crates/server/src/metrics.rs
crates/server/tests/common/mod.rs   TestServer, World, Live
crates/server/tests/*.rs            one file per area, mirroring tests/ of the Python server
crates/serverd/src/main.rs          binary and subcommands
crates/loadgen/src/v2.rs            v2 dialect (Task 19)
```

## Seams every task shares

```rust
// state.rs
#[derive(Clone)]
pub struct AppState { pub store: Arc<dyn Store>, pub blobs: Arc<dyn Blobs>, pub hub: Hub,
    pub mailer: Arc<dyn Mailer>, pub ext: Arc<dyn Extensions>, pub settings: Arc<Settings>,
    pub sessions: Sessions, pub clock: Clock }
pub fn app(state: AppState) -> axum::Router;

// error.rs — every handler returns Result<_, ApiError>
pub enum ApiError { NotFound, Unauthorized, Forbidden(&'static str), Conflict(String),
    BadRequest(String), PaymentRequired(String), TooLarge, RateLimited, NotImplemented(String),
    Internal(anyhow::Error) }          // body: {"detail": "..."}; Internal logs and says "server error"

// store/mod.rs — async_trait; one method per query; row types are plain structs
pub type Id = String;                   // 32 hex
pub trait Store: Send + Sync + 'static { /* grows task by task; see each task's Interfaces */ }

// tests/common/mod.rs
pub struct TestServer { pub base: String, pub state: AppState, /* task handle, temp dir */ }
impl TestServer { pub async fn start() -> Self; pub async fn start_with(f: impl FnOnce(&mut Settings)) -> Self;
    pub async fn restart(self) -> Self; }          // same database and storage
pub struct World { pub admin: Token, pub admin_id: Id, pub org: Id, pub project: Id }
pub struct Live { /* a v2 WebSocket client built on oswx_core::wire */ }
```

`TestServer::start` runs against SQLite in a temp directory, or against PostgreSQL when
`OSWX_TEST_DATABASE_URL` is set (each test gets an empty schema), exactly as the Python `conftest.py`
does. A `Clock` that tests can advance replaces every direct use of system time in logic that has a
timeout (sessions, invites, locks, idle unload, rate limits).

---

### Task 1: Crate, settings, errors, meta routes, test harness

**Files:** `crates/server/{Cargo.toml,src/lib.rs,config.rs,error.rs,state.rs,routes/meta.rs}`,
`tests/common/mod.rs`, `tests/meta.rs`.

**Interfaces — produces:** `Settings` (every field of inventory §9 with the same default),
`Settings::from_env(&HashMap<String,String>)`, `ApiError`, `AppState` (store, blobs, hub fields may be
placeholders that later tasks replace), `app()`, `TestServer`, `Clock`.
Routes: `GET /api/health` → `{"ok": true}`; `GET /api/ready` → 200 `{"ok": true}` when the database and
storage answer, else 503; `GET /api/server` as inventory §1 "App-level routes" with `protocol: 2`.

- [ ] Tests: settings parse every type, both URL spellings, unknown variables ignored, a bad number is
  an error naming the variable; health; ready fails when the store reports unhealthy (test double);
  server info shape; a 2 MB JSON body to any route is 413; unknown route is 404 with `{"detail": …}`.
- [ ] Implement; `cargo test -p oswx-server`; commit `server: crate, settings, errors, meta routes`.

### Task 2: Store, schema and migrations (SQLite)

**Files:** `migrations/sqlite/0001_init.sql`, `store/{mod,sql,sqlite}.rs`, `tests/store.rs`.

Schema: the tables of inventory §4 that the spec carries over, with the same columns, plus:

```sql
folders(id PK, project_id FK projects CASCADE, parent_id NULL FK folders CASCADE, name, created_at)
documents: + folder_id NULL FK folders SET NULL; head_seq kept
changes(document_id FK documents CASCADE, seq, parent_seq, branch_id default 'main', epoch,
        author_id, client_id NULL, change_id NULL, created_at, command, ops BLOB, inverse BLOB,
        paths BLOB, reads BLOB, undo_of NULL, reverts NULL, reverted_by NULL, broke TEXT NULL,
        PRIMARY KEY(document_id, seq), UNIQUE(document_id, client_id, change_id))
snapshots(document_id FK documents CASCADE, epoch, seq, key, size, created_at,
          PRIMARY KEY(document_id, epoch, seq))
room_leases(document_id PK FK documents CASCADE, node_id, epoch, expires_at)
```

`ops`, `inverse`, `paths`, `reads` are MessagePack (`oswx_core` wire forms). No `meta` table, no `ops`
table. SQLite is opened WAL, `foreign_keys=ON`, busy timeout 30 s, parent directory created.

**Interfaces — produces:** `SqliteStore::open(url) -> Result<Self>` (runs migrations unless
`auto_migrate` is false, in which case an older schema is an error telling the operator to run
`migrate`; a newer schema is always an error), `Store::health()`, `Store::schema_status()`, and the
transaction helper the other tasks use. Each later task adds its own `Store` methods and their SQL.

- [ ] Tests: an empty file is built; reopening is a no-op; `auto_migrate=false` on an empty file still
  builds it; a database with a migration version unknown to this binary is refused; foreign keys
  cascade (delete an org → its projects, documents, changes go); the unique constraint on
  `(document_id, client_id, change_id)` allows many rows with NULL `change_id`.
- [ ] Implement; commit `server: store and schema`.

### Task 3: Security primitives

**Files:** `security.rs`, `tests/security.rs` (unit tests may live in the module).

**Interfaces — produces** (inventory §2 "Passwords", "Tokens", "Email normalisation", "Rate limits",
and the colour rule in §5 "Join"):

```rust
pub async fn hash_password(pw: &str) -> String;          // on the blocking pool; same format and scrypt parameters as Python
pub async fn verify_password(pw: &str, stored: &str) -> bool;   // constant time; malformed stored -> false
pub fn new_token() -> String;  pub fn token_hash(t: &str) -> String;
pub fn clean_email(s: &str) -> Result<String, ApiError>;
pub struct RateLimit { /* key -> window */ }  impl RateLimit { pub fn check(&self, key: &str, now: f64) -> bool; pub fn reset(&self, key: &str); }
pub fn colour_for(user_id: &str, taken: &[String]) -> String;
pub fn role_at_least(have: &str, need: &str) -> bool;    // org and project orders
```

- [ ] Tests: a hash produced by the Python server verifies (put one fixture hash and its password in the
  test, generated with `server/openswx_collab_server/security.py`); wrong password fails; password
  length limits as in the inventory; the login limit (10 failures per 10 minutes per email+IP) and the
  reset limit (5 an hour); colours match the Python palette for three fixed user ids.
- [ ] Implement; commit `server: passwords, tokens, rate limits`.

### Task 4: Accounts — register, login, sessions, me, password reset, mail

**Files:** `access.rs`, `sessions.rs`, `mail.rs`, `routes/auth.rs`, store methods, `tests/accounts.rs`,
`tests/email.rs`.

Requirements: inventory §1 "Auth" (all routes except the OAuth ones), §2 "Sessions", "Registration
modes", "Password reset", "Admin rules", §10. v2 difference: sessions are cached in memory
(`Sessions`), with entries dropped on logout, password change, reset and account deletion; a cached
session is trusted for at most 30 s without a store check.

**Interfaces — produces:** `access::CurrentUser` (axum extractor from `Authorization: Bearer`),
`Sessions::{get, forget_token, forget_user}`, `trait Mailer { async fn send(&self, to, subject, text,
html) -> Result<()> }`, `MemoryMailer` (tests read what was sent), `SmtpMailer`.

- [ ] Port every test in inventory §11 for `test_accounts.py` and `test_email.py` that does not involve
  OAuth, invites or organisations (those arrive in Tasks 5 and 14), keeping the names. Add: a token is
  never present in any log line captured during the test run; a 401 for a missing, malformed, unknown
  and expired token; the reset page at `/reset-password/<token>` never contains the token in its HTML.
- [ ] Implement; commit `server: accounts and sessions`.

### Task 5: Organisations, invites, projects, members, extensions

**Files:** `ext.rs`, `routes/{orgs,projects}.rs`, store methods, `tests/orgs.rs`, `tests/project.rs`.

Requirements: inventory §1 "Organisations, invites, projects", §2 "Invites", §3, §8.

**Interfaces — produces:**

```rust
#[async_trait] pub trait Extensions: Send + Sync + 'static {
    async fn may_add_member(&self, org: &Id, adding: usize) -> Result<(), String> { Ok(()) }   // Err -> 402
    async fn member_added(&self, org: &Id, user: &Id) {}
    async fn member_removed(&self, org: &Id, user: &Id) {}
    async fn before_org_delete(&self, org: &Id) -> Result<(), String> { Ok(()) }
    fn routes(&self, state: AppState) -> axum::Router { axum::Router::new() }
    fn migrations(&self) -> Vec<&'static str> { vec![] }
    async fn on_event(&self, event: &Event) {}                 // audit sink
    async fn quota(&self, org: &Id, what: Quota) -> Result<(), String> { Ok(()) }
    fn server_info(&self) -> serde_json::Value { serde_json::json!({}) }
}
pub struct NoExtensions;
pub async fn project_role(store: &dyn Store, user: &User, project: &Id) -> Result<Option<Role>, ApiError>;
pub async fn require_project(store, user, project, need: Role) -> Result<(Project, Role), ApiError>;  // 404 when no role
```

Role and membership changes call `state.hub.drop_user(user, org)` (a no-op until Task 12).

- [ ] Port `test_project.py` and the organisation and invite tests of `test_accounts.py`, and the hook
  tests (seat limit → 402). Add hostile tests: every route with another organisation's ids answers 404;
  names over 200 characters are 400; an invite token is single use and expires.
- [ ] Implement; commit `server: organisations, invites, projects`.

### Task 6: Blobs

**Files:** `blobs.rs`, `routes/blobs.rs`, `tests/storage.rs`.

Requirements: inventory §7. v2 differences: the body is raw bytes hashed as received (SHA-256 of the
raw bytes must equal the path's hash, else 400); stored zstd-compressed; upload and download are
streamed (never the whole blob in memory); `GET` sets `Content-Length` of the raw size.

```rust
#[async_trait] pub trait Blobs: Send + Sync + 'static {
    async fn put_stream(&self, key: &str, body: BoxStream<'static, io::Result<Bytes>>, limit: u64) -> Result<PutOutcome>;
    async fn get_stream(&self, key: &str) -> Result<Option<(u64, BoxStream<'static, io::Result<Bytes>>)>>;
    async fn exists(&self, key: &str) -> Result<bool>;        // an outage is Err, not Ok(false)
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()>;  async fn get(&self, key: &str) -> Result<Option<Bytes>>;
    async fn delete_prefix(&self, prefix: &str) -> Result<()>;   async fn health(&self) -> Result<()>;
}
pub fn blob_key(org: &Id, sha256_hex: &str) -> String;   // orgs/<org>/blobs/<sha[:2]>/<sha>
```

- [ ] Port `test_storage.py`. Add: wrong hash refused and nothing stored; 256 MB + 1 byte refused with
  413 and no partial file left; `blobs/missing` with 2,001 hashes is 400; a malformed hash is 400; a
  viewer may read but not write; a 64 MB upload keeps the process's peak allocation for that request
  under 8 MB (assert by counting the largest chunk the handler ever holds).
- [ ] Implement `LocalBlobs`; commit `server: blobs`.

### Task 7: Documents and folders (REST)

**Files:** `routes/documents.rs`, `snapshot.rs`, store methods, `tests/documents.rs`.

Requirements: inventory §1 "Documents" for create, list, get, patch, delete and `content` at head.
Creation: `content` (JSON) → `oswx_core::Value::from_plain`; protected keys kept as given; blob
references in the content (`{"$blob": sha, "len": n}`) must exist; the result is snapshot seq 0, epoch
0, written before the row is committed. New: `folders` CRUD under a project
(`GET/POST /api/projects/{id}/folders`, `PATCH/DELETE /api/folders/{id}`; a folder may not be moved
into its own descendant; deleting a folder moves its documents to the parent) and `folder_id` on
documents (create, patch, list filter `?folder=`).

**Interfaces — produces:** `snapshot::{encode(doc: &Value, index: &Index, seq) -> Bytes,
decode(&[u8]) -> Result<(Value, Index, Seq)>}` (zstd MessagePack `{v:1, seq, doc, index}`),
`snapshot_key(doc, epoch, seq)`.

- [ ] Tests: create/list/get/patch/delete with roles (viewer cannot create; non-member gets 404); the
  Python `PART` fixture content is accepted and `GET content` returns it equal as JSON; content over
  8 MB is 413; content with depth 65, NaN, a `$list` key is 400; missing blob is 409 listing the
  hashes; folder cycle refused; soft-deleted documents disappear from lists and answer 404.
- [ ] Implement; commit `server: documents and folders`.

### Task 8: Journal writer and room loading

**Files:** `journal.rs`, `snapshot.rs`, `oswx-core` additions, store methods, `tests/journal.rs`.

Core additions (in `crates/core`, with tests there): `Accepted` gains `writes: BTreeSet<Write>`,
`removed: BTreeSet<Path>`, `reads: BTreeSet<Path>`; `decide` takes
`refused: &dyn Fn(&str) -> Known` with `enum Known { Accepted, Refused, Unknown }` and refuses
`Unknown` as `Dependency`; `decide` takes an optional `undo: Option<&[Op]>` (the kept inverse of the
change named by `undo_of`) and exempts ids that occur under the same path in it from the prefix check.

```rust
pub struct Record { pub document: Id, pub seq: Seq, pub parent_seq: Seq, pub epoch: u64, pub author: Id,
    pub client_id: Option<String>, pub change_id: Option<String>, pub command: String, pub ops: Bytes,
    pub inverse: Bytes, pub paths: Bytes, pub reads: Bytes, pub undo_of: Option<String>,
    pub reverts: Option<Seq>, pub created_at: f64, pub extra: Vec<Extra> }   // Extra: SetRevertedBy, AssemblyRefs
#[derive(Clone)] pub struct JournalWriter { /* mpsc sender */ }
impl JournalWriter {
    pub fn start(store: Arc<dyn Store>, max_batch: usize /*256*/, max_wait: Duration /*2 ms*/) -> Self;
    pub async fn append(&self, r: Record) -> Result<(), JournalError>;   // resolves when durable
}
pub enum JournalError { Refused(String) /* this record only */, Unknown /* outcome not known: reload */ }
pub async fn load_head(store: &dyn Store, blobs: &dyn Blobs, doc: &Id) -> Result<(Head, u64 /*epoch*/)>;
pub async fn state_at(store, blobs, doc, seq) -> Result<Value>;     // nearest snapshot at or before + replay
```

One transaction per batch; within it each document's lease row is locked and its epoch compared with
the record's (a mismatch refuses that document's records only); records that carry `Extra` run in
their own savepoint; `documents.head_seq` and `updated_at` are updated in the same transaction. On a
commit error or timeout every waiter gets `Unknown`.

- [ ] Tests: 1,000 appends from 50 tasks all land, in order per document, in far fewer than 1,000
  transactions (count them through a store double); a record with a stale epoch is `Refused` while
  others in the batch succeed; a store double that fails the commit makes every waiter `Unknown` and
  nothing is stored; `load_head` equals replaying everything from seq 0; `state_at` for every seq of a
  300-change document; duplicate `(document, client_id, change_id)` is `Refused`.
- [ ] Implement; commit `server: journal writer, snapshots, room loading`.

### Task 9: Rooms and the WebSocket — changes

**Files:** `room/{mod,conn}.rs`, `hub.rs`, `ws.rs`, `tests/common` (`Live`), `tests/live.rs`,
`tests/hardening.rs`.

Requirements: inventory §5 "Handshake", "Main loop", "Conn", "Join" (without locks), "Leave", "Change
commit" and `Room.change`, as changed by v2: binary frames decoded with
`oswx_core::wire::decode_client(frame, 8 MB + 64 KB)`; a text frame is `bad_message`; the hello must
have `protocol == 2` (else 4426); decisions by `oswx_core::decide` (so no `ids.reserve`, conflicts by
path, `dependency`, `name_taken`, `too_old`); reject codes via `Refusal::code()`; `welcome` and all
other server messages encoded once with `encode_server` and the same `Bytes` sent to every recipient.

Room task: owns `Head`, the epoch, the connection table, recent change ids (all since the last
snapshot, plus a store lookup for older ones), and the refused-id set per client. Commit pipeline:
decide → blob check (unknown hashes via `Blobs::exists`, at most 16 at once, an outage is
`server_error`) → `JournalWriter::append` → on `Ok` adopt the head, broadcast, acknowledge; on
`Refused` reject with `server_error`; on `Unknown` drop tentative state, reload with `load_head`, close
every connection with `moved` (4410) so clients reconnect. While an append is in flight the room keeps
deciding later changes against the tentative head; a refusal of an earlier one refuses those that were
decided on top of it (`server_error`) and the room reverts to the last durable head.

Snapshots: after `snapshot_every` (200) changes since the last one, encode from the durable head on the
blocking pool and write in the background; the index is pruned to `head.seq - 100_000` first.

**Interfaces — produces:** `Hub::{new, open(doc) -> Result<RoomHandle>, loaded(doc) -> Option<RoomHandle>}`,
`RoomHandle::{join, send(cmd)}`, `Live` test client (`connect`, `change(ops, reads) -> Outcome`,
`expect::<Msg>()`, `close`).

- [ ] Port `test_live.py` (without lock, presence and undo tests), the change-related tests of
  `test_hardening.py`, `test_convergence.py` driven through real sockets against one server (4 clients,
  40 rounds, 3 seeds, using `crates/core/tests/common/client.rs` as the client), and
  `test_performance.py`'s structural assertions that still apply (encoded once; snapshot off the room
  task). Add the Review Focus tests 1 and 3; a store double failing the batch (Review Focus 2); 4409
  for a duplicate client id; 4408 for a client that stops reading (limits of inventory §5 "Conn"); a
  frame of 8 MB + 64 KB + 1; garbage after a valid hello keeps the connection and answers
  `bad_message`.
- [ ] Implement; commit `server: rooms and live changes`.

### Task 10: Locks, presence, drafts, who is online

**Files:** `room/{locks,presence}.rs`, `routes/documents.rs` (online list), `tests/locks.rs`,
`tests/online.rs`.

Requirements: inventory §5 "Locks", "Presence and draft", "Join" (lock re-attachment), "Leave", and
the `online` and `online_count` fields of the document list. v2 differences: locks are on paths
(`Vec<Path>`); a path is lockable when it resolves in the document; a change is `locked` when any of
its writes is at, under or above a path locked by another client; a lock on a keyed list's own path
(for example `["features"]`) also blocks `insert`, `remove` and `move` in that list (this replaces the
`structure` target). The `lock.result` and `lock.changed` shapes follow `oswx_core::wire`.

- [ ] Port `test_online.py` and the lock and presence tests of `test_live.py` and `test_hardening.py`.
  Add: lock TTL (45 s of silence) and grace (60 s after disconnect) with the test clock; 257th lock
  refused; viewers cannot lock or send drafts; presence over 16 KB and the 61st message in a second
  are dropped silently.
- [ ] Implement; commit `server: locks and presence`.

### Task 11: History, versions, undo, revert, restore, rebuild reports

**Files:** `room/{history,rebuild}.rs`, `routes/history.rs`, `tests/history.rs`,
`tests/client_feedback.rs`.

Requirements: inventory §1 routes for `content?seq=|version=`, `history`, `versions`, `restore`; §5
"Undo and revert", "Restore", "Rebuild reports". v2: undo is a `change` with `undo_of` (the room hands
`decide` the kept inverse of that change; the journal row records `reverts`); `change.revert` remains
for reverting anyone's change from history (editors), computed from the stored inverse; restore is
`oswx_core::diff(current, state_at(seq).to_plain())` applied with no id prefix and no staleness check,
refused while another user holds a lock. History events carry `seq, author, author_name, client_id,
change_id, time, ops, command, reverts, reverted_by, broke`.

- [ ] Port `test_history.py` and `test_client_feedback.py`. Add Review Focus 5 (restore of a
  Python-shaped document); undo after someone else changed the same field is `stale`; undo of a removal
  restores the item with its original id; undo of a replaced subtree holding another client's ids is
  accepted (carry-forward item); history paging limits (500).
- [ ] Implement; commit `server: history, versions, undo, restore, rebuild reports`.

### Task 12: Drops, sweeper, idle unload, shutdown

**Files:** `hub.rs`, `sessions.rs`, `tests/lifecycle_rooms.rs`.

Requirements: inventory §5 "Hub" and the "Immediate drops" table. v2: no periodic recheck; instead
`Hub::drop_account(user)` (4401), `Hub::drop_user(user, org)` (4403; only connections whose role
actually changed or vanished, re-evaluated per connection), `Hub::close_document(doc, reason)` (4404),
called from the handlers that change passwords, roles, memberships and delete things; an expired
session is noticed when its expiry time passes (a timer per room for the earliest expiry) and closes
4401. Sweeper every `sweep_interval`: lock expiry; unload a room with no connections and no locks
after `room_idle` (snapshot first); one room's failure never stops the others. Shutdown: stop
accepting, close sockets 1001, flush the journal, snapshot every open room.

- [ ] Tests: each row of the "Immediate drops" table (Review Focus 4 among them); a viewer promoted to
  editor is dropped once and can edit after reconnecting; idle unload then reopen gives the same
  content; a room with a detached lock stays until the grace passes; shutdown leaves a snapshot at the
  head for every open room and a restart serves the same seq.
- [ ] Implement; commit `server: access drops, sweeper, shutdown`.

### Task 13: Assemblies

**Files:** `routes/assemblies.rs`, room changes, store methods, `tests/assemblies.rs`.

Requirements: inventory §6 and §1 "Assemblies". Component reference checks run before the journal
append; the `assembly_refs` rewrite travels with the record as `Extra::AssemblyRefs` (its own
savepoint; a part deleted in the meantime refuses that record as `invalid`).

- [ ] Port `test_assemblies.py`. Add: a reference to a part in another organisation is `invalid` and
  never reveals whether the part exists; deleting a part in use is 409 listing only assemblies the
  caller can open.
- [ ] Implement; commit `server: assemblies`.

### Task 14: Sign-in with Google and GitHub

**Files:** `routes/oauth.rs`, store methods, `tests/oauth.rs`.

Requirements: inventory §2 "OAuth" in full (routes, state handling, the challenge and verifier
exchange, linking rules, every error code sent to the loopback address). The provider HTTP calls go
through a `trait OAuthHttp` so tests supply the provider's answers.

- [ ] Port the OAuth tests of `test_accounts.py`. Add: a redirect URI that is not a loopback address is
  refused; a state or login code is single use and expires; the verifier must match the challenge.
- [ ] Implement; commit `server: Google and GitHub sign-in`.

### Task 15: Account and organisation deletion and export

**Files:** `routes/lifecycle.rs`, `tests/lifecycle.rs`.

Requirements: inventory §1 "Lifecycle", §2 "Account deletion", "Org deletion", "Org export". The org
export is a streamed zip; each document's content is JSON (`to_plain`), history is JSON lines of the
history event shape.

- [ ] Port `test_lifecycle.py`. Add: the export of an organisation with a 64 MB blob does not hold the
  blob in memory; deleting an organisation removes its blobs and snapshots from storage.
- [ ] Implement; commit `server: deletion and export`.

### Task 16: PostgreSQL

**Files:** `store/postgres.rs`, `migrations/postgres/0001_init.sql`, `tools/test_postgres_rust.py` (or a
shell script), CI job.

`PgStore` implements `Store` with the shared SQL text; journal batches use one multi-row insert; lease
rows are locked with `SELECT … FOR UPDATE`. CI gains a job that runs `cargo test -p oswx-server` with
`OSWX_TEST_DATABASE_URL` against a PostgreSQL 16 service.

- [ ] The whole integration suite passes against PostgreSQL. Add a test that two `JournalWriter`s with
  different epochs for one document cannot both write (the stale one is `Refused`).
- [ ] Implement; commit `server: PostgreSQL store`.

### Task 17: S3 storage

**Files:** `blobs.rs` (`S3Blobs` via `object_store`), `tests/storage_s3.rs` (runs only when
`OSWX_TEST_S3_URL` is set), README note for running it against the Garage compose file in `deploy/`.

- [ ] The `Blobs` contract tests of Task 6 run against both implementations through one generic test
  function; an unreachable endpoint makes `exists` an error (so a change is `server_error`, not
  `missing_blob`).
- [ ] Implement; commit `server: S3 storage`.

### Task 18: The binary, logging and metrics

**Files:** `crates/serverd/{Cargo.toml,src/main.rs}`, `crates/server/src/metrics.rs`, `tests/cli.rs`.

Binary `openswx-collab-server` with the subcommands of inventory §9 (`serve [--host] [--port]`,
`create-admin`, `reset-password`, `migrate`, `db-status`). Structured logs (`tracing`, JSON when
`OSWX_COLLAB_LOG=json`). `GET /metrics` (Prometheus text): open rooms, connections, changes accepted
and rejected by code, journal batch size and latency, decide time, frame sizes, slow-consumer
disconnects; no label ever carries a user, document or organisation id.

- [ ] Tests: each subcommand against a temp database; `serve` starts, answers `/api/health`, and stops
  cleanly on Ctrl-C with every open room snapshotted; `/metrics` changes after one accepted change.
- [ ] Implement; commit `server: binary, logs, metrics`.

### Task 19: Load generator v2 dialect and the comparison run

**Files:** `crates/loadgen/src/v2.rs`, `bench/results/rust-<version>-sqlite/*.json`,
`bench/results/README.md`.

A v2 dialect for `loadgen` using `oswx_core::wire`. Run the same scenarios as the baseline on the same
machine against the Rust server (release build, SQLite; PostgreSQL too if a local instance is
available), one fresh server per scenario, and add a second table and a short comparison to
`bench/results/README.md` with the same caveats. A scenario the server fails is recorded as failed.

- [ ] Run, record, commit `bench: Rust server compared with the Python baseline`.

### Task 20: Documents that describe the server

**Files:** `docs/protocol.md` (rewritten for v2, using the list in the carry-forward file),
`docs/self-hosting.md`, `AGENTS.md`, `CONTRIBUTING.md`, `CHANGELOG.md`, `README.md`,
`deploy/Dockerfile.rust` and a compose file for the Rust server.

- [ ] `docs/protocol.md` matches `oswx_core::wire` field for field (a test in `crates/core` reads the
  message names from the doc and fails if one is missing); `AGENTS.md` rules restated for the Rust
  server (rule 9 becomes "one node holds a document"); the container builds and `tools/check_deploy.py`
  (or its successor) passes on a real host.
- [ ] Commit `docs: protocol v2 and the Rust server`.

---

## Self-review notes

- Spec coverage: server section, amendments 1–9 on the server side (journal fencing, unknown commit
  outcome, undo through the kept inverse), folders, extensions, operations. Multi-machine, import and
  removal of the Python server are other plans.
- Every Python test file in inventory §11 is ported by a named task: accounts (4, 5, 14), email (4),
  project (5), storage (6), live (9, 10), hardening (9, 10), convergence (9), performance (9), online
  (10), history (11), client_feedback (11), assemblies (13), lifecycle (15), migrations (2), ops (in
  `crates/core` already).
