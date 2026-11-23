# openswx live collaboration — design and first-version plan

## Context

openswx is a standalone parametric CAD app (Python + FastAPI backend, three.js web
UI, desktop via pywebview). Today it is single-user: collaboration is a shared-folder
vault and a view-only share link. The goal is real-time multi-user editing, surfaced
as a **toggle on a file inside openswx** (not a separate app). Toggling makes the file
cloud-served through a middleware server that can be self-hosted or hosted by the
project owner as a paid, per-organisation, seat-limited service.

## Decisions (user's, unless marked "my call")

| Topic | Decision |
|---|---|
| Users | Student teams, small firms and self-hosters all matter |
| Server design | **Relay document changes**: each user's openswx runs the edit locally and sends the resulting change; the server checks locks, orders, stores, rebroadcasts. No geometry kernel on the server |
| Same part, two editors | Fine-grained locks per feature/sketch; others watch live |
| In-progress work | Live previews: sketch lines as drawn, feature ghost before OK |
| Presence | Pointer on the model, selection highlights, active tool label, follow-their-view |
| Connectivity | Short drops tolerated, reconcile on reconnect; no long offline work |
| Part used in an assembly | Assembly pins a part version; "update available" prompt previews broken mates (later stage) |
| File home | Cloud is the source of truth; automatic local backup snapshots |
| History | Full edit history (author, time, restore) + named versions |
| Sign-in | Email/password + Google/GitHub in core; company SSO in paid layer |
| Permissions | Organisation → project roles (owner / editor / viewer); files inherit |
| Licensing | Open core: server core Apache-2.0; `ee/` (seats, SSO, billing, admin console) commercial |
| Repos | Server + shared protocol in `openswx-collab`; client module in `openswx` |
| openswx branch | `dev/openswx-collab` (git forbids the `:` in `dev:openswx-collab`) |
| First version | Parts only, live, self-hosted, simple accounts |
| Stack (my call) | Python, FastAPI + WebSockets, SQLAlchemy on Postgres (SQLite for small self-hosts), S3-compatible or local-disk blob store, Docker Compose |

## How it works

```
 browser UI ──WS──► local openswx backend ──WSS──► collab server ──► Postgres + blobs
 (presence,          (kernel, applies edits,        (auth, locks, ordering,
  cursors, tree)      diffs, applies remote ops)     op log, snapshots, fan-out)
```

**Edit path.** Every mutating command passes through the `@command` wrapper
(`openswx/session.py:109-172`). For a cloud document the collab module takes
`doc.to_dict()` before and after the outermost command, diffs them into
**id-keyed operations** (`feature.add{after}`, `feature.update{id, params-diff}`,
`feature.remove`, `feature.move`, `doc.set{path}`), and sends
`{base_seq, ops, inverse_ops, command, args}`. The server checks role and lock
ownership for every touched id, assigns the next sequence number, appends to the op
log and broadcasts. Other clients apply the ops to their copy and rebuild (the
incremental rebuild cache in `document.py:719-784` keeps this cheap).

**Not synced (per-user view state, my call):** `rollback`, `active_config`,
`display_state`, `journal`, camera. These stay local so one person rolling back the
tree does not roll back everyone.

**IDs.** `next_id`/`counters` (`document.py:204-219`) collide across clients. The
server owns them: a client reserves a feature id and display name from the server
before creating a feature. Consequence: creating new features needs a connection;
during a drop you can only keep editing what you already hold a lock on.

**Locks.** Opening a sketch or a feature's property panel requests a lease on that
feature id (heartbeat-renewed; held for a 60 s grace period after a disconnect).
Tree-structure changes (reorder, delete) take a short document-wide structure lock.
The wrapper rejects a local command whose diff touches someone else's lock and rolls
it back via the existing `_checkpoint` snapshot.

**Presence and previews.** A separate unsaved channel at ~20 Hz carries: pointer hit
point + persistent face name (`naming.py`), selection as persistent names, active
tool / open panel, camera (for follow), and drafts (in-progress sketch entities,
uncommitted panel parameters). Receivers draw sketch drafts as an overlay and run
feature drafts through the existing throwaway `preview` path
(`session.py:289-309`), debounced. The local backend gains a WebSocket endpoint
(today the UI only has SSE polling, `web_server.py:104-116`).

**Storage.** Op log rows (seq, author, time, command label, ops, inverse ops), a
full snapshot every N ops, and content-addressed blobs for imported BREP bodies so
they are uploaded once. Named versions are labelled sequence numbers.

**Local backup.** A `.oswprt` snapshot written periodically to
`~/.openswx/cloud-backups/<org>/<project>/`, reusing the autosave thread pattern in
`recovery.py`.

## Conflict handling

1. **Same feature, two people** — prevented by locks.
2. **Upstream edit breaks someone's downstream feature** (e.g. a sketch change kills
   a fillet's edges) — the change is accepted; clients report rebuild failures, and
   the tree shows "Fillet2 broken by Sam's change to Sketch1" with one-click revert
   of that change (its stored inverse ops) or "edit to fix".
3. **Reconnect after a drop** — queued local changes are replayed on top of the
   server head. If a touched feature changed meanwhile or the lock was lost, a
   dialog offers: keep mine as a new copy of the feature, discard mine, or view the
   difference (built on `compare_documents`, `commands_review.py:115-140`).
4. **Undo** — per-user: undo sends the inverse of your own last change, and is
   refused with an explanation if someone has since changed the same feature.
   Whole-snapshot undo is disabled on cloud documents.
5. **Imported bodies** — opaque; replace-whole only, always under a lock.
6. **Part → assembly (later stage)** — assemblies reference `{document id, version}`
   instead of embedding a copy (`assembly.py:128`). Updating runs a dry-run that
   lists mates whose face names no longer resolve before applying.
7. **Version mismatch** — clients on a different openswx file-format version than
   the document join read-only, so rebuild results cannot diverge.

## Layout

`openswx-collab` (this repo):
- `protocol/` — `openswx_collab_protocol`: pydantic message and op schemas, shared by both sides
- `server/` — `openswx_collab_server`: `auth`, `orgs`, `projects`, `documents`, `hub` (WebSocket rooms), `locks`, `oplog`, `snapshots`, `storage`
- `ee/` — commercial layer (empty placeholder in first version)
- `deploy/` — Dockerfile, docker-compose (server + Postgres + MinIO)
- `docs/` — this design, protocol reference, self-hosting guide

`openswx` (branch `dev/openswx-collab`):
- `openswx/collab/` — `client.py` (connection, reconnect, queue), `diff.py` (dict → ops, apply ops), `locks.py`, `presence.py`, `backup.py`, `commands_collab.py` (`collab_sign_in`, `collab_enable`, `collab_open`, `collab_lock`, `collab_history`, `collab_name_version`)
- `openswx/session.py` — small change-hook registry called from the `@command` wrapper; apply-remote-ops entry point addressed by document, not `APP.active`
- `openswx/web_server.py` — WebSocket endpoint for presence
- `openswx/web/collab.js` + edits to `app.js`, `viewport.js`, `sketcher.js`, `propman.js` — toggle, sign-in, avatar bar, tree lock badges, remote pointers/highlights, draft overlays, follow view, history panel
- `pyproject.toml` — `collab` extra; `packaging/openswx.spec` hiddenimports
- With the extra absent or the toggle off, behaviour is unchanged

## Stages

0. **Spike (throwaway).** Two local openswx sessions and a 50-line relay: capture a
   diff from the wrapper, apply it to the other session, assert equal document
   fingerprints across a scripted set of commands (sketch, extrude, fillet, edit,
   delete, reorder). This proves the central idea before anything is built on it.
1. **Protocol + server core.** Accounts (email/password), projects, documents,
   WebSocket hub, id reservation, locks, op log, snapshots, Docker Compose.
2. **openswx sync client.** Hook, diff/apply, enable-collaboration toggle, open
   cloud file, locks, reconnect and replay, local backup.
3. **Presence and live previews.** Local WebSocket, pointers, highlights, tool
   labels, follow view, sketch and feature drafts.
4. **History.** History panel, restore to point, named versions, per-user undo,
   broken-by attribution.
5. **Accounts.** Google/GitHub sign-in, organisation → project roles.

Later, each its own design round: assemblies with pinned references; `ee/` seats,
SSO, billing and admin console for the paid hosting; browser-only viewers.

First implementation steps: save this design to
`openswx-collab/docs/2026-11-23-live-collaboration-design.md`, `git init` here,
create `dev/openswx-collab` in openswx, then run Stage 0 and report before Stage 1.

## Verification

- **Spike / sync:** pytest that drives two `Application` instances through the
  relay and compares `_fingerprint` after each command; includes a deliberately
  concurrent case and a lock-violation case.
- **Server:** pytest with two fake WebSocket clients covering ordering, lock
  leases and expiry, id reservation, reconnect replay, role checks; runs on SQLite.
- **End to end:** start the server with Docker Compose, launch two openswx
  instances, enable collaboration on a part in one, open it in the other; edit in
  both; kill one's network for 30 s and reconnect; confirm equal documents, history
  entries with authors, and a local backup file.
- **UI:** Playwright test with two browser contexts (pattern in `tests/ui`)
  asserting remote pointer, selection highlight, tool label and a sketch draft
  appear; follow view moves the camera.
- **Regression:** full existing openswx test suite passes with the `collab` extra
  absent and with it installed but the toggle off.

## Risks

- Rebuilds must give the same face names on every client; the spike and the
  version-match rule address this, and it is the first thing to test.
- Some helpers mutate documents outside commands (`session.py:905-913`); diffing
  whole-document state catches these as long as they run inside a command.
- Feature previews for collaborators cost local compute; debounce and allow
  turning previews off per user.
- openswx's own roadmap lists cloud collaboration as out of scope
  (`PLAN.md:716`); the module stays optional and off by default.
