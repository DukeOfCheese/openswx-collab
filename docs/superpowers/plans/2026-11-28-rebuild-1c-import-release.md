# Rebuild 1c: Import, Release and 1b Follow-ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Self-hosters of the Python server (0.1.x) can move everything to the Rust server with one
command; the Rust server can be released as binaries and a Docker image; the follow-ups 1b deferred are
done.

**Architecture:** A new library crate `crates/import` (`oswx-import`) reads a Python-server database
(SQLite or PostgreSQL, Alembic revision `0003`) and its storage (folder or S3), converts each
document's v1 history into v2 changes one-to-one by seq, and writes them through the existing `Store`
and `Blobs`. `openswx-collab-server import` runs it. Conversion is pure (no I/O) and verified against
every Python snapshot. The release workflow gains Rust jobs routed by tag, beside the Python ones.

**Tech Stack:** Rust (workspace as in 1b), sqlx (reading the Python schema with raw queries),
serde_json, flate2 (already in `Cargo.lock` through `zip`), `oswx-core`, `oswx-server`; GitHub Actions
(native `ubuntu-24.04-arm` runners, musl-tools), cargo-deny.

**Spec:** `docs/superpowers/specs/2026-11-25-collab-rebuild-design.md` (amendment 8: import converts
full history; repository layout). **Carried in:** `docs/superpowers/plans/2026-11-25-rebuild-1a-carry-forward.md`
("For docs and import (1c)": empty-string keys and ids; "Small test and tidy items"), and the 1b
deferrals listed in Tasks 5–8. **Python storage reference:** `docs/superpowers/specs/2026-11-25-python-server-inventory.md`
§4 and `server/openswx_collab_server/db.py`, `hub.py`, `storage.py`, `protocol/openswx_collab_protocol/ops.py`.

Not in this plan: removing the Python server, `protocol/` and their CI (after the v2 client ships);
`spike/` (removed with them); the `crates/core-py` wheel (project 2); multi-machine (project 3).
Ruled out: folding the store constructors in `sqlite.rs`/`postgres.rs` (`shared.rs` is already the
`store_impl!` macro; what is left is a few short functions).

## Decisions made with the owner (2026-11-28)

- The import is a subcommand of the one binary: `openswx-collab-server import`.
- The Rust image is published as `ghcr.io/dukeofcheese/openswx-collab-server` with version tags only
  (`0.2.3`, `0.2`); `latest` stays on the Python 0.1.x image until the v2 client ships.
- A Rust release attaches binaries for Linux x86_64 and aarch64 (static, musl), Windows x86_64 and
  macOS arm64 and x86_64.

## Global Constraints

- All AGENTS.md rules hold. `cargo fmt --check`, `cargo clippy --workspace --all-targets -j 2 -- -D warnings`
  and `cargo test --workspace -j 2` pass at every commit; `python tools/test_postgres_rust.py` before
  each task's last commit when it touches `crates/server` or `crates/import`.
- No `unsafe`. No new crate dependency that is not already in `Cargo.lock` (no downloads); CI-only tools
  (cargo-deny, musl-tools) are installed in workflows.
- No Claude or AI attribution in commits or pull requests. Work on branch `rebuild-1c`; never push to
  `main`.
- Files under `server/`, `protocol/` and `tests/` (the Python line) are not modified.
- The import never writes to the source database or source storage; it opens the source SQLite file
  read-only.
- Never log a token, password hash, invite or document content, also from the import (rule 7). Ids,
  counts, seqs and document names may be printed.
- Seqs are kept: v1 op row `seq = n` becomes v2 change `seq = n`, so named versions, assembly refs,
  `reverts` and `reverted_by` keep their meaning.
- Update `CHANGELOG.md` (the 0.2.0 section) and `docs/self-hosting.md` with each user-visible change.

## Review Focus

1. **The Python server's SQLite file has un-checkpointed WAL content** (the server was stopped without a
   checkpoint): the import must read it all. Task 3 test `a_source_with_unmerged_wal_is_read_whole`.
2. **The destination already has data** (someone started the Rust server and signed up first): the
   import refuses before writing anything and says why. Task 3 test
   `a_destination_with_any_user_or_org_is_refused_untouched`.
3. **One document cannot be converted** (an integer beyond i64, a key over 200 characters, a renamed
   key colliding): `--dry-run` lists every such document by id and project; a real run stops before
   that document with the same message. Task 4 test `a_document_that_cannot_convert_is_named`.
4. **A blob a document refers to is missing from the source storage:** a warning names the sha and
   document; the import goes on; the document opens in the Rust server and reports the missing blob as
   it does today. Task 4 test `a_missing_source_blob_is_a_warning_not_a_failure`.
5. **A document with a long history** (more than the 500-change catch-up window and several Python
   snapshots): it loads in the Rust server, and its head and every named version match the Python
   server's. Task 4 test `a_long_history_loads_and_its_versions_match`.

---

### Task 1: Python-shaped source fixtures

The Rust tests need Python-server data without running Python (nothing is installed locally). A Rust
test helper builds the Python schema from SQL transcribed from the Alembic migrations; a Python tool
makes real data from the real server, used by one CI job.

**Files:**
- Create: `crates/import/Cargo.toml`, `crates/import/src/lib.rs` (crate doc and `pub mod` lines only)
- Create: `crates/import/tests/common/mod.rs`, `crates/import/tests/common/python_schema.sql`
- Create: `crates/import/tests/fixture.rs`
- Create: `tools/make_import_fixture.py`
- Modify: `Cargo.toml` (workspace `members`)

**Interfaces:**
- Produces (test helper, `tests/common/mod.rs`):
  - `pub struct PySource { pub db_url: String, pub storage: PathBuf, _dir: TempDir }`
  - `pub async fn py_source() -> PySource` — an empty Python-schema SQLite database at revision `0003`
    (with `alembic_version`) and an empty storage folder.
  - `impl PySource`: `async fn user(&self, email: &str) -> String` (id), `async fn org(&self, name, owner) -> String`,
    `async fn project(&self, org, name) -> String`, `async fn document(&self, project, kind: &str, content: serde_json::Value) -> String`
    (writes the `documents` row and the seq-0 snapshot file and row exactly as `server/.../documents.py`
    does), `async fn op(&self, doc, row: PyOp) -> i64` (appends an `ops` row at head+1, updating
    `head_seq`), `async fn snapshot(&self, doc, seq, content)`, `async fn version(&self, doc, seq, name)`,
    `fn blob(&self, org, bytes: &[u8]) -> String` (sha; raw bytes at `orgs/<org>/blobs/<sha[:2]>/<sha>`).
  - `pub struct PyOp { pub author: String, pub client_id: Option<String>, pub change_id: Option<String>,
    pub command: String, pub ops: serde_json::Value, pub inverse_ops: serde_json::Value, pub reverts: Option<i64> }`
- Produces (tool): `python tools/make_import_fixture.py <out dir>` writes `<out>/collab.db` and
  `<out>/storage/` by driving the real Python server in-process (the way `tests/conftest.py` does):
  two users (one later deleted), an org with an invite, two projects, a part with 450 changes including
  an undo, a revert and a restore, a named version, a blob reference, a soft-deleted document, and an
  assembly pinning the part. It prints a JSON summary (`{"users": n, "documents": n, "head": {doc: seq}}`)
  to `<out>/summary.json`.

- [ ] **Step 1: Write `python_schema.sql`** — the `CREATE TABLE`/`CREATE INDEX` statements of revisions
  `0001`+`0002`+`0003` as SQLAlchemy emits them for SQLite (`server/openswx_collab_server/db.py:30-212`),
  plus `CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL PRIMARY KEY); INSERT … '0003'`.
- [ ] **Step 2: Write the failing test** `crates/import/tests/fixture.rs::the_fixture_matches_the_python_models`:
  build `py_source()`, then assert the table list equals `["alembic_version","assembly_refs","documents",
  "invites","login_codes","meta","oauth_identities","oauth_states","ops","org_members","orgs",
  "password_resets","project_members","projects","sessions","snapshots","users","versions"]` and that
  `document()` + two `op()` calls give `head_seq = 2` and a file at the snapshot row's `key` that gunzips
  to the content.
- [ ] **Step 3: Run it** — `cargo test -p oswx-import --test fixture -j 2`; expected FAIL (no crate).
- [ ] **Step 4: Add the crate and the helper.** Dependencies: `oswx-core`, `oswx-server`, `sqlx`,
  `serde_json`, `flate2`, `anyhow`, `tokio`, `futures-util`; dev: `tempfile`, `sha2`, `hex`. Snapshot
  files: gzip level 6 of compact JSON (`separators=(",",":")`, UTF-8) at the key
  `docs/{id}/snapshots/{seq:010}.json.gz`.
- [ ] **Step 5: Run it** — expected PASS.
- [ ] **Step 6: Write `tools/make_import_fixture.py`** (stdlib plus the installed Python server only).
  It is run in CI only (Task 4); check it with `python -m py_compile tools/make_import_fixture.py`.
- [ ] **Step 7: Commit** — `git add Cargo.toml Cargo.lock crates/import tools/make_import_fixture.py`;
  message `import: Python-shaped source fixtures`.

### Task 2: Converting a v1 document history to v2 (pure)

**Files:**
- Modify: `crates/core/src/diff.rs` (add `diff_at`), `crates/core/tests/diff.rs`
- Create: `crates/import/src/sanitize.rs`, `crates/import/src/convert.rs`
- Test: `crates/import/tests/convert.rs`

**Interfaces:**
- Produces (core): `pub fn diff_at(doc: &Value, at: &[String], after: Option<&Plain>) -> Result<Vec<Op>, ValueError>`
  — the ops that turn whatever is at path `at` in `doc` into `after` (`None`: absent). Recurses when
  both sides are maps or keyed lists (inserts, removes, moves by id with positions, as `diff` does for
  lists); otherwise one `Set` or `Unset` at `at`. Unlike `diff`, it never skips `PROTECTED` keys below
  the top level (a feature's `name` is data). `at` empty means the whole document and behaves as `diff`.
- Produces (import):
  - `pub struct Sanitizer` with `pub fn key(&self, k: &str) -> Result<String, ConvertError>` and
    `pub fn value(&self, v: &Plain) -> Result<Plain, ConvertError>` (recursive) and
    `pub fn content(&self, v: &Plain) -> Result<Plain, ConvertError>` (`value` plus dropping top-level
    `next_id`, `counters`, `rollback`, `active_config`, `display_state`, `journal`, `name`).
    Rules: key `""` → `"_empty"`; key `"$list"` → `"_list"`; an `id` value `""` → `"_empty"`; a renamed
    key that collides with an existing key of the same map, a key over 200 characters, an integer
    outside i64, or depth over 64 → `ConvertError`. Every rename is counted in `Sanitizer::renamed()`.
  - `pub enum V1Op` parsed from one JSON op with `pub fn parse(v: &serde_json::Value) -> Result<V1Op, ConvertError>`:
    `Add { list: String, item: Plain, after: Option<String> }`, `Remove { list, id }`,
    `Update { list, id, set: Vec<(Vec<String>, Plain)>, unset: Vec<Vec<String>> }`,
    `Order { list, ids: Vec<String> }`, `Patch { set, unset }`. `list` is `"features"` for `feature.*`
    and the `in` field (`components` or `mates`) for `item.*`. Missing optional fields take v1's
    defaults (`after` absent = `null`, `set`/`unset`/`ids` absent = empty).
  - `pub struct Converted { pub ops: Vec<Op>, pub inverse: Vec<Op> }` and
    `pub struct History { pub head: Value, sanitizer: Sanitizer }` with
    `pub fn start(initial: &Plain) -> Result<History, ConvertError>` (sanitized content, `Value::from_plain`)
    and `pub fn step(&mut self, v1_ops: &[V1Op]) -> Result<Converted, ConvertError>` and
    `pub fn matches(&self, snapshot: &Plain) -> Result<bool, ConvertError>` (sanitized snapshot content
    equals `head.to_plain()`).
  - `pub enum ConvertError` (thiserror-free, `Display` + `std::error::Error`), variants naming the cause
    and the path.

Mapping (v1 semantics from `protocol/openswx_collab_protocol/ops.py:116-137, 273-317`): each v1 op is
applied to a plain copy of the subtree it touches, and `diff_at` gives the v2 ops for that subtree:
`Add`/`Remove`/`Order` touch `[list]` (`after: None` inserts at index 0; `after: id` right after `id`);
`Update` touches `[list, id]` (a `set` of `id` to its current value and an `unset` of an absent key are
no-ops; a `set` of `id` to another value is a `ConvertError`); `Patch` touches each top-level key named
in its paths. Set values and path segments pass through the `Sanitizer`. The step's ops are applied with
`oswx_core::apply(&head, &ops, &Rules::default(), None)`, which gives the next head and the inverse.
An op that v1 would have refused (unknown id, `after` not in the list) is a `ConvertError`.

- [ ] **Step 1: Write the failing core test** `crates/core/tests/diff.rs::diff_at_changes_only_the_subtree`:
  for a document with keyed `features` `[a, b]`, `diff_at(doc, ["features","a"], Some(a'))` where `a'`
  changes `params.depth` and `name` gives exactly `[Set ["features","a","params","depth"], Set ["features","a","name"]]`
  (sorted by path), and applying them gives `a'`; `diff_at(doc, ["units"], None)` gives `[Unset ["units"]]`.
  Add the property `diff_at_then_apply_reaches_the_target` (proptest, existing generators): for any doc
  and any keyed item `id` in it and any target item with the same `id`, applying `diff_at` reaches it.
- [ ] **Step 2: Run** `cargo test -p oswx-core --test diff -j 2 diff_at` — FAIL.
- [ ] **Step 3: Implement `diff_at` in `crates/core/src/diff.rs`**, reusing `diff`'s list and map code
  with a path prefix.
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Write the failing import tests** in `crates/import/tests/convert.rs`:
  - `each_v1_operation_converts`: one case per `V1Op` variant, including `Add` with `after: null` into a
    non-empty list (lands first) and into an empty `features: []`, `Order` that swaps two of five, and an
    assembly's `item.add` into `components`; after `step`, `head.to_plain()` equals the v1 result
    written out by hand.
  - `inverses_undo_each_step`: applying `Converted::inverse` to the new head gives the previous head.
  - `v1_no_ops_are_no_ops`: an `unset` of an absent key and a `set` of `id` to itself give empty `ops`.
  - `empty_keys_and_ids_are_renamed_the_same_way_everywhere`: initial content with a `""` key in a
    feature's params and a component with `id: ""`; a later `Update` setting `["params",""]` writes
    `["features",<id>,"params","_empty"]`; `renamed() == 2` kinds counted.
  - `unconvertible_content_is_an_error`: integer `2**63`, a 201-character key, a map with both `""` and
    `"_empty"` keys each give `ConvertError`.
  - `random_v1_histories_match_a_v1_reference` (proptest, 200 cases, up to 40 ops): a minimal v1
    applier written in the test (on `serde_json::Value`, following `ops.py`) and `History::step` reach
    the same plain document after every op.
- [ ] **Step 6: Run** `cargo test -p oswx-import --test convert -j 2` — FAIL.
- [ ] **Step 7: Implement `sanitize.rs` and `convert.rs`.**
- [ ] **Step 8: Run** — PASS; then the whole workspace tests.
- [ ] **Step 9: Commit** — `core: diff_at; import: converting v1 histories to v2`.

### Task 3: Reading the source, accounts, and the `import` command

**Files:**
- Create: `crates/import/src/source.rs` (reading the Python database and storage), `crates/import/src/accounts.rs`,
  `crates/import/src/run.rs` (`ImportOptions`, `import`, the report)
- Modify: `crates/serverd/Cargo.toml`, `crates/serverd/src/lib.rs` (subcommand, doc block, command-line tests)
- Test: `crates/import/tests/accounts.rs`

**Interfaces:**
- Consumes: `oswx_server::admin::Db` (`Db::open(settings, ext)`, `db.store()`), `Settings`
  (`from_process_env`), the blob setup of `AppState::from_settings_with` (`state.rs:354-381`).
- Produces:
  - `pub struct ImportOptions { pub from_database: String, pub from_storage: String,
    pub from_s3_endpoint: Option<String>, pub dry_run: bool }`
  - `pub async fn import(settings: &Settings, ext: Arc<dyn Extensions>, opts: &ImportOptions, out: &mut dyn std::io::Write) -> anyhow::Result<Report>`
  - `pub struct Report { pub users: usize, pub orgs: usize, pub projects: usize, pub documents: usize,
    pub changes: u64, pub blobs: usize, pub warnings: Vec<String> }` with `Display` (one line of counts,
    then one line per warning).
  - `source::Source` with `open(opts, settings) -> anyhow::Result<Source>` and row readers returning
    plain structs per table (`users`, `oauth_identities`, `sessions`, `password_resets`, `orgs`,
    `org_members`, `invites`, `projects`, `project_members`, `documents`, `ops` streamed by document in
    seq order, `snapshots`, `versions`, `assembly_refs`), and `storage: Arc<dyn Blobs>` (source storage
    through the existing `LocalBlobs`/`S3Blobs`, read with `Blobs::get`, which returns stored bytes as
    they are).
- Command line: `openswx-collab-server import --from-database <url> --from-storage <folder|s3://bucket/prefix>
  [--from-s3-endpoint <url>] [--dry-run]`. The destination is the usual `OSWX_COLLAB_*` settings.
  S3 credentials for both sides come from the same `AWS_*` variables. Database URLs take the Python
  forms (`sqlite+aiosqlite:///path`, `postgresql+asyncpg://…`) and the Rust forms.

Rules: the source must be at revision `0003` (`alembic_version`), else refuse with "the source is at
revision X; run the Python server 0.1.2's migrate first". SQLite is opened read-only (`mode=ro`), WAL
content included. The destination is opened with `Db::open` (migrations as usual) and must have no row
in `users` and `orgs`, else refuse before writing. `--dry-run` never opens the destination for writing
(it opens it only to check emptiness when reachable) and does everything else. Accounts go in one
transaction: users (verbatim: password hashes and token hashes are the same format, `security.rs`),
OAuth identities, unexpired sessions and password resets, orgs, members, invites (verbatim `projects`
JSON), projects, project members. `oauth_states` and `login_codes` are not imported. User ids and
every other id are kept.

- [ ] **Step 1: Write the failing tests** in `crates/import/tests/accounts.rs` (each runs a real
  `TestServer`-style destination on SQLite; reuse `oswx_server` test helpers only if public, otherwise
  open the destination with `Db::open` on a temp file):
  - `accounts_come_over_and_can_sign_in`: a Python user with a password hash made by the Python
    algorithm (compute it in the test with `oswx_server::security::hash_password`, same format) signs in
    to a Rust server started on the imported database with that password; their session token from the
    source still authenticates `GET /api/me`; org, roles, invite and project grants match.
  - `a_destination_with_any_user_or_org_is_refused_untouched` (Review Focus 2).
  - `a_source_at_another_revision_is_refused` (`alembic_version` = `0002`).
  - `a_source_with_unmerged_wal_is_read_whole` (Review Focus 1): write the source with WAL on and
    `wal_autocheckpoint=0`, keep the `-wal` file, import, all rows present.
  - `a_dry_run_writes_nothing`: destination tables stay empty, the report has the same counts.
  - `the_report_holds_no_secret`: the printed report and warnings contain no password hash or token
    hash from the source.
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement `source.rs`, `accounts.rs`, `run.rs`** (documents left as an empty step for
  Task 4).
- [ ] **Step 4: Add the `import` subcommand** in `crates/serverd/src/lib.rs`: a `Command::Import` variant,
  a match arm calling `oswx_import::import`, the doc block lines, and cases in
  `the_command_line_is_well_formed` (missing `--from-storage` is a usage error, exit 2).
- [ ] **Step 5: Run** the import and serverd tests — PASS.
- [ ] **Step 6: Commit** — `import: the import command; accounts, organisations and projects`.

### Task 4: Documents, history, blobs, versions and assembly refs

**Files:**
- Create: `crates/import/src/documents.rs`
- Modify: `crates/import/src/run.rs`, `.github/workflows/rust.yml` (job `import-from-python`),
  `docs/self-hosting.md` (section "Moving from the Python server"), `CHANGELOG.md`, `AGENTS.md`
  (crate list: `crates/import/`)
- Test: `crates/import/tests/documents.rs`, `crates/import/tests/python_data.rs`

**Interfaces:**
- Consumes: `History`, `V1Op`, `Sanitizer` (Task 2); `Source` (Task 3); `oswx_server::record::{encode_ops,
  encode_paths, encode_reads}`, `oswx_core::{writes, removed}`, `oswx_server::snapshot::{encode,
  snapshot_key, load_head, write_snapshot}`, `Tx::{insert_document, insert_snapshot, insert_changes,
  set_document_head, insert_version, replace_assembly_refs}`, `oswx_server::content::{plain_from_json,
  blob_refs}`, `blob_key`.
- Produces: `documents::import_document(src: &Source, dest: &dyn Store, blobs: &dyn Blobs, doc: &PyDocument,
  dry_run: bool, report: &mut Report) -> anyhow::Result<()>`.

Per document, in seq order: start `History` from the seq-0 snapshot; for each `ops` row parse its
`ops` JSON into `V1Op`s and `step`; whenever the source has a snapshot at that seq, `matches` must be
true, else fail with "document <id> differs from its Python snapshot at seq <n>". Each row becomes a
`NewChange` with the same `seq`, `parent_seq = seq − 1`, `branch_id = "main"`, `epoch = 0`, the same
`author_id`, `client_id`, `change_id`, `created_at`, `command`, `reverts`, `reverted_by` and `broke`;
`undo_of = reverts` when `command` starts with `"undo "`, else `None`; `ops`/`inverse` from `Converted`;
`paths = encode_paths(&writes(&ops), &removed(&ops))`; `reads = encode_reads(&[])`. The v1 `args`,
`targets` and `inverse_ops` are dropped (the inverse is recomputed). Write the seq-0 snapshot (epoch 0,
empty index) to the destination blobs, then in one transaction the document row (same id, project,
name, kind, format_version, created_by, times, deleted_at; `folder_id` NULL), the snapshot row, the
changes (`insert_changes`), `set_document_head`, the versions and the assembly refs (copied; seqs are
kept). After the commit, if head > 0, `load_head` from the destination and `write_snapshot` at epoch 0.
Blobs: every `$blob` sha referenced by the document's content or its v1 ops is copied once per org from
`orgs/<org>/blobs/<sha[:2]>/<sha>` with `put_stream` (bytes unchanged, so the sha and the refs stay
valid; the stored form becomes the v2 header + zstd). A sha whose stored digest differs is a failure; a
missing one is a warning (Review Focus 4). Documents are imported one by one, each in its own
transaction; a failure stops the import with "empty the destination and run the import again", after
`--dry-run` has been recommended in the docs.

- [ ] **Step 1: Write the failing tests** in `crates/import/tests/documents.rs`:
  - `a_part_with_history_opens_at_the_same_head`: 30 v1 changes including undo and revert; a Rust
    server on the destination opens the document over WebSocket and its `welcome` content equals the
    sanitized Python head; `GET …/history` lists 30 entries with the same seqs, commands, and
    `reverts`/`reverted_by`.
  - `a_long_history_loads_and_its_versions_match` (Review Focus 5): 1,200 changes, Python snapshots at
    200, 400, … ; a version at seq 777; `GET` of the version's content equals the Python state at 777.
  - `a_document_that_cannot_convert_is_named` (Review Focus 3): one document with an integer beyond
    i64 in a later change; `--dry-run` report lists it with its project; a real run fails naming it,
    and the documents before it are complete.
  - `a_missing_source_blob_is_a_warning_not_a_failure` (Review Focus 4).
  - `blobs_come_over_unchanged`: a 70 KB blob referenced by a part reads back through the Rust blob
    endpoint byte for byte.
  - `an_assembly_keeps_its_pins` and `a_deleted_document_stays_deleted`.
  - `a_snapshot_that_disagrees_is_a_failure`: tamper a Python snapshot; the import names doc and seq.
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement `documents.rs`** and call it from `run.rs` after the accounts.
- [ ] **Step 4: Run** — PASS. Then the same tests on PostgreSQL (destination) through
  `python tools/test_postgres_rust.py` and on S3 storage (source and destination) through the fake S3
  (`crates/server/tests/common/s3.rs`; expose what is needed via a `test-util` feature of `oswx-server`
  if it is not public).
- [ ] **Step 5: Write `crates/import/tests/python_data.rs::real_python_data_imports`**, ignored unless
  `OSWX_TEST_PYTHON_DATA` names a folder from `tools/make_import_fixture.py`: dry run, real run, then
  every document's head seq equals `summary.json`'s and opens.
- [ ] **Step 6: Add CI job `import-from-python`** to `.github/workflows/rust.yml` (ubuntu-latest):
  set up Python 3.13, `pip install -e protocol -e server`, run the tool into `$RUNNER_TEMP/py`, then
  `OSWX_TEST_PYTHON_DATA=$RUNNER_TEMP/py cargo test -p oswx-import --test python_data -- --ignored`.
- [ ] **Step 7: Docs.** `docs/self-hosting.md` "Moving from the Python server": stop the Python server;
  back up; run `import --dry-run`; run `import` into an empty database; what is not carried
  (sign-in-in-progress state, the v1 `args` field); the renamed-key rule. Also the Docker form
  (`docker compose run --rm server import …` with both volumes). CHANGELOG 0.2.0: the import command.
- [ ] **Step 8: Commit** — `import: documents with full history, blobs, versions and assembly refs`.

### Task 5: A connection whose room ends before its Leave arrives

1b deferral. Today `RoomHandle::leave` (`room/mod.rs:837-839`) ignores a failed send; when the room
task has ended (a panic) or ends before handling the `Leave`, the outbox is never closed, the writer
(`conn.rs:607-635`) waits forever, and the socket's `SocketPlace` (`hub.rs:238-245`) is never given back.

**Files:**
- Modify: `crates/server/src/room/mod.rs` (`leave`), `crates/server/src/room/conn.rs` (reader end),
  test hook in the room's `Hooks` if none can end a room task abruptly
- Test: `crates/server/tests/lifecycle_rooms.rs`

**Interfaces:**
- Produces: `RoomHandle::leave(&self, conn) -> bool` (true when the room took the message).

- [ ] **Step 1: Write the failing test** `a_connection_whose_room_died_gives_back_its_place`: with
  `max_connections` small, connect a client, make the room task end abruptly (the hook), close the
  client; the server's open-socket count returns to its previous value within 2 s, and a new client can
  connect to the same document.
- [ ] **Step 2: Run** — FAIL (the count stays up).
- [ ] **Step 3: Implement.** `leave` returns whether the send succeeded; after `leave` the reader
  closes its outbox (`MOVED` when `leave` was not delivered, otherwise as today), so the writer always
  ends when the reader does.
- [ ] **Step 4: Run** the test, then `cargo test -p oswx-server --test lifecycle_rooms --test convergence -j 2` — PASS.
- [ ] **Step 5: Commit** — `room: a connection whose room ended gives back its place`.

### Task 6: Sign-in counts on an uncertain reset; two test properties

1b deferrals.

**Files:**
- Modify: `crates/server/src/routes/auth.rs:678-686`
- Test: `crates/server/tests/accounts.rs`, `crates/server/tests/serving.rs:1703-1765`,
  `crates/server/tests/performance.rs:42-43,126-163`

- [ ] **Step 1: Write the failing test** `an_uncertain_reset_keeps_the_sign_in_counts`: lock an account
  by failed sign-ins, confirm a password reset while the store's commit fails (flaky store; outcome
  `Committed::Uncertain`); the account is still locked (the next sign-in with the new password is
  429), and a later successful reset unlocks it.
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement:** call `forget_sign_in_failures` only for `Committed::Yes`.
- [ ] **Step 4: Restore the stop bound.** In
  `a_stop_that_cannot_finish_ends_at_its_bound_with_the_rooms_closed_first`, assert the room closes no
  later than half the bound plus 700 ms (2,200 ms for `shutdown_timeout = 3.0`) as well as no earlier
  than 1,400 ms.
- [ ] **Step 5: Quiet-spell tests.** Make `performance.rs`'s doc comment at 42-43 name the tests that
  exist, and give `a_presence_after_a_quiet_spell_past_the_tick_goes_at_once` its limit as a named
  constant derived from the tick (arrives within one tick of being sent).
- [ ] **Step 6: Run** the three test files — PASS (run the serving test 20 times in a loop, paced 1 s,
  to check for flakiness).
- [ ] **Step 7: Commit** — `auth: an uncertain reset keeps the sign-in counts; stop and presence test bounds`.

### Task 7: Releasing the Rust server

**Files:**
- Modify: `.github/workflows/release.yml`, `tools/release_notes.py`, `Cargo.toml` (`[profile.release]`),
  `.github/dependabot.yml`, `.github/workflows/rust.yml` (job `deny`), `CONTRIBUTING.md` (how to release)
- Create: `deny.toml`

Tag routing: `v0.1.*` runs the existing Python jobs unchanged (PyPI and the Python image, which keeps
`latest`); any other `v*` tag runs the Rust jobs. Rust jobs:
- `rust-check`: the tag equals `version` in `crates/server/Cargo.toml` and `crates/serverd/Cargo.toml`;
  `tools/release_notes.py <version>` finds the section.
- `rust-binaries` (matrix, native runners, `cargo build --release --locked -p oswx-serverd`):
  `x86_64-unknown-linux-musl` (ubuntu-latest, `apt-get install musl-tools`), `aarch64-unknown-linux-musl`
  (`ubuntu-24.04-arm`, musl-tools), `x86_64-pc-windows-msvc` (windows-latest), `aarch64-apple-darwin`
  (macos-latest), `x86_64-apple-darwin` (macos-13). Each archive (`.tar.gz`; `.zip` for Windows) holds
  the binary, `LICENSE`, `NOTICE`, `README.md`, and gets a `.sha256`. Each job runs the built binary's
  `--version` (and on Linux `serve` against a temp folder with a `/health` check).
- `rust-image`: `deploy/Dockerfile.rust` built natively per arch (amd64 on ubuntu-latest, arm64 on
  `ubuntu-24.04-arm`), pushed by digest, then one manifest list with tags `<version>` and
  `<major>.<minor>` only — never `latest`.
- `rust-release`: `gh release create` with the archives and checksums and the notes.
`release_notes.py`: for a version ≥ 0.2 the "Run it" block names the image tag, the binaries and
`deploy/docker-compose.rust.yml`/`docker-compose.rust-postgres.yml`, and the import command. Release
profile: `lto = "thin"`, `codegen-units = 1`, `strip = true`; never `panic = "abort"` (room panic
recovery unwinds). `deny.toml`: advisories (deny vulnerabilities, warn unmaintained), licenses (allow
the licenses in today's tree, listed explicitly), bans (warn on duplicates), sources (crates.io only).
CI job `deny` runs `cargo-deny check` (EmbarkStudios/cargo-deny-action pinned by commit). Dependabot
gains a monthly `cargo` entry for `/`.

- [ ] **Step 1: Write `deny.toml`**; run `cargo deny check` in CI only (not installed locally): push the
  branch and read the `deny` job; fix by adjusting the license list, not by ignoring advisories (an
  advisory that cannot be fixed is listed with its id and reason, and reported to the owner).
- [ ] **Step 2: Add the release profile**; check `cargo build --release -p oswx-serverd -j 2` and that
  the server still recovers from a room panic (`cargo test -p oswx-server --release --test lifecycle_rooms -j 2`).
- [ ] **Step 3: Split `release.yml`** as above; extend `release_notes.py`; check it with
  `python tools/release_notes.py 0.1.2` (unchanged output) and a temporary `## 0.2.0` heading in a
  scratch copy of the CHANGELOG (`OSWX_CHANGELOG` variable or argument, if the tool lacks one, add it).
- [ ] **Step 4: Dry run on GitHub:** `gh workflow run release.yml --ref rebuild-1c` (a manual run
  builds without publishing); every Rust build job passes and uploads its archive. (Ask the owner before
  this run if the plan is executed without standing approval to run CI on the branch.)
- [ ] **Step 5: Docs.** CONTRIBUTING "Releasing": bump both crate versions, move the CHANGELOG section to
  `## <version> (<date>)`, tag `v<version>`; `v0.1.*` tags release the Python line.
- [ ] **Step 6: Commit** — `release: Rust binaries and image; cargo-deny; dependabot for cargo`.

### Task 8: Small core and loadgen items

From the carry-forward's "Small test and tidy items" (all still apply).

**Files:**
- Modify: `crates/core/src/value.rs:53-77` (`KeyedList` keeps the value with its order entry, so `iter`
  does no lookup), `crates/core/src/path.rs:47-57` (`from_wire` calls `check`), `crates/core/src/op.rs:106-114`
  (`item_id` calls `check_name`, so `$list` is refused there too), `crates/loadgen/src/main.rs:141-148`
  (`loadgen_commit` gets `-dirty` when `git status --porcelain` is non-empty), `bench/results/README.md`
  (prose lines wrapped at 110)
- Test: `crates/core/tests/pos.rs`, `crates/core/tests/diff.rs:442-482`, `crates/core/tests/wire.rs`,
  `crates/core/tests/op.rs` (or where `item_id` is tested)

- [ ] **Step 1: Write the tests:** `between_any_well_formed_bounds_is_strictly_between` (proptest over
  arbitrary well-formed `Pos` pairs `a < b`, and `None` ends); in `reorder_case_holds`, assert
  `moves == bound` when no positions are tied; `nesting_at_max_is_read_and_one_more_is_refused`
  (depth `MAX_NESTING` read, `MAX_NESTING + 1` refused); the generator also emits array16/array32,
  map16/map32 and str32 forms; `an_item_id_of_dollar_list_is_refused`.
- [ ] **Step 2: Run** — the `$list` test fails; the others pass or fail as the code stands (record which).
- [ ] **Step 3: Implement** the four code items.
- [ ] **Step 4: Run** `cargo test -p oswx-core -j 2` and the convergence tests — PASS.
- [ ] **Step 5: Commit** — `core: tidy items from 1a; loadgen marks a dirty tree`.

---

## Verification (before the pull request)

- The four commands in AGENTS.md "Before you finish", plus `cargo test -p oswx-core --test convergence`
  and `cargo test -p oswx-server --test convergence`.
- The `import-from-python` and `deny` CI jobs and the manual release run pass on the branch.
- `python tools/check_deploy_rust.py` (the image still builds and runs with the release profile; the Pi
  `duke@dukerpi` for `--docker`, avoiding port 8787).
