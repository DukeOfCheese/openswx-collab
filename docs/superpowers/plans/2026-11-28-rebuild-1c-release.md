# Rebuild 1c: Release and 1b Follow-ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The Rust server can be released as binaries and a Docker image, and the follow-ups 1b
deferred are done. (The import tool first planned here was dropped: see below.)

**Architecture:** The release workflow gains Rust jobs routed by tag, beside the Python ones. The
follow-ups are local fixes in `crates/server` and `crates/core`.

**Tech Stack:** Rust (workspace as in 1b); GitHub Actions (native `ubuntu-24.04-arm` runners,
musl-tools), cargo-deny.

**Spec:** `docs/superpowers/specs/2026-11-25-collab-rebuild-design.md` (its import tool and amendment 8
are withdrawn by the owner's decision below). **Carried in:** `docs/superpowers/plans/2026-11-25-rebuild-1a-carry-forward.md`
("Small test and tidy items") and the 1b deferrals listed in Tasks 5–8.

Not in this plan: removing the Python server, `protocol/` and their CI (after the v2 client ships);
`spike/` (removed with them); the `crates/core-py` wheel (project 2); multi-machine (project 3).
Ruled out: folding the store constructors in `sqlite.rs`/`postgres.rs` (`shared.rs` is already the
`store_impl!` macro; what is left is a few short functions).

## Decisions made with the owner (2026-11-28)

- No import from the Python server (nobody runs it).
- The Rust image is published as `ghcr.io/dukeofcheese/openswx-collab-server` with version tags only
  (`0.2.3`, `0.2`); `latest` stays on the Python 0.1.x image until the v2 client ships.
- A Rust release attaches binaries for Linux x86_64 and aarch64 (static, musl), Windows x86_64 and
  macOS arm64 and x86_64.

## Global Constraints

- All AGENTS.md rules hold. `cargo fmt --check`, `cargo clippy --workspace --all-targets -j 2 -- -D warnings`
  and `cargo test --workspace -j 2` pass at every commit; `python tools/test_postgres_rust.py` before
  each task's last commit when it touches `crates/server`.
- No `unsafe`. No new crate dependency that is not already in `Cargo.lock` (no downloads); CI-only tools
  (cargo-deny, musl-tools) are installed in workflows.
- No Claude or AI attribution in commits or pull requests. Work on branch `rebuild-1c`; never push to
  `main`.
- Files under `server/`, `protocol/` and `tests/` (the Python line) are not modified.
- Update `CHANGELOG.md` (the 0.2.0 section) and `docs/self-hosting.md` with each user-visible change.

## Review Focus

1. **A room task panics while clients are connected and some are closing:** every socket place and
   writer is given back; new clients can join. Task 5 test.
2. **A password reset whose commit outcome is unknown:** the account's sign-in failure counts stay;
   a later reset that commits clears them. Task 6 test.
3. **The release build (`lto`, `strip`) still recovers from a room panic:** never `panic = "abort"`.
   Task 7 step 2.
4. **A `v0.1.*` tag** still releases the Python line exactly as today, and **a Rust tag** never moves
   the image's `latest`. Task 7 YAML review.
5. **An item id or path segment of `$list`** is refused at every entry point. Task 8 test.

---

Tasks 1–4 (the import tool) were dropped on 2026-11-28 by the owner: nobody runs the Python server, so
there is nothing to import. The task numbers below are kept.

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
`deploy/docker-compose.rust.yml`/`docker-compose.rust-postgres.yml`. Release
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
- The `deny` CI job and the manual release run pass on the branch.
- `python tools/check_deploy_rust.py` (the image still builds and runs with the release profile; the Pi
  `duke@dukerpi` for `--docker`, avoiding port 8787).
