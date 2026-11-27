# Protocol (version 2)

For whoever writes a client (the openswx module, first of all). This is the contract with the Rust server
(`crates/server`). The messages are defined in `crates/core/src/wire.rs` (`oswx_core::wire`) and the
operations in `crates/core/src/op.rs`. `crates/core/tests/protocol_doc.rs` fails when a message, a field, a
structure, an operation or a code is missing from the tables here (it does not notice one the tables name
that the code no longer has), and a test in the server (`crates/server/src/protocol_doc.rs`) fails when a
close code or an `error` code it sends has no row. Wire test vectors (bytes in, bytes out) are in
`crates/core/vectors/`.

Version 2 is a clean break from version 1 (the Python server's JSON messages): a version 1 client is closed
with 4426, and `GET /api/server` says `"protocol": 2`.

## REST (JSON, `Authorization: Bearer <token>`)

Every error answers `{"detail": "<message>"}` (a few 409s carry an object in `detail`; they say so below).
Times are float seconds since the epoch; ids are 32 lower-case hex characters.

| | |
|---|---|
| `GET /api/health` | `{"ok": true}` while the process serves; a few connections are kept for it alone when the server is full |
| `GET /api/ready` | 200 `{"ok": true}` when the database and the storage answer and the journal writer runs, else 503 `{"ok": false}` |
| `GET /api/server` | `{name, version, protocol, registration, oauth, email, edition}`: registration mode (`first_user`, `invite`, `open`), sign-in providers, whether it can send email, protocol version (2); an extension may add fields |
| `GET /metrics` | Prometheus text, only with `Authorization: Bearer <OSWX_COLLAB_METRICS_TOKEN>` (401 without it); 404 when no token is set |
| `POST /api/auth/register` `{email, password, name, invite?}` | → `{token, user}` |
| `POST /api/auth/login` `{email, password}` · `POST /api/auth/logout` · `GET/PATCH /api/me` | |
| `DELETE /api/me` `{current_password?}` | delete your account (password, or a sign-in from the last 10 minutes); refused (409) while you are an organisation's only owner or the server's last admin |
| `GET /api/me/export` | everything the server holds about you, as JSON |
| `POST /api/auth/password-reset` `{email}` | emails a link to `/reset-password/<token>` (a page that sets the password); 202 for any well-formed request (whether or not there is such an account, and when the request was limited), 501 when the server cannot send email |
| `POST /api/auth/password-reset/confirm` `{token, password}` | sets it, signs out everywhere |
| `GET /api/auth/oauth/{google\|github}/start?redirect_uri=http://127.0.0.1:<port>/…&code_challenge=&invite=` | browser sign-in; ends on the loopback address with `?code=` or `?error=` |
| `POST /api/auth/exchange` `{code, code_verifier}` | → `{token, user}` |
| `POST /api/auth/oauth/{google\|github}/link` `{redirect_uri, code_challenge}` | signed in: add a provider to this account; returns `{url}` to open in the browser |
| `POST /api/auth/oauth/link/finish` `{code, code_verifier}` | finishes a `link` |
| `GET /api/me/providers` · `POST /api/me/providers/{name}/unlink` | the providers on your account; remove one |
| `GET/POST /api/orgs` · `PATCH /api/orgs/{id}` · `GET /api/orgs/{id}/members` · `PATCH/DELETE …/members/{user}` | |
| `DELETE /api/orgs/{id}` `{confirm_name, current_password?}` | owners: delete the organisation and everything in it |
| `GET /api/orgs/{id}/export` | owners: a zip of every document (content, history, versions), the members and the blobs (`HEAD` is 405) |
| `GET/POST /api/orgs/{id}/invites` · `DELETE …/invites/{id}` · `POST /api/invites/accept` `{token}` | an invite with an `email` is emailed when the server can (`send_email: false` to skip); the answer says `emailed` (see below). An organisation has at most 1,000 open (unused, unexpired) invites: one more is 409 |
| `GET/POST /api/orgs/{id}/projects` · `PATCH/DELETE /api/projects/{id}` · `GET …/members` · `PUT/DELETE …/members/{user}` | |
| `GET/POST /api/projects/{id}/folders` · `PATCH/DELETE /api/folders/{id}` | folders, nested at most 32 deep; `{name, parent_id?}`; deleting one moves what was in it to its parent |
| `POST /api/projects/{id}/blobs/missing` `[sha256…]` · `PUT/GET /api/projects/{id}/blobs/{sha256}` | large data, by the SHA-256 of its raw bytes; at most 2,000 hashes a call (400 beyond, 422 for one that is not a hash) |
| `GET/POST /api/projects/{id}/documents` `?folder=<id>\|root` | create = turn collaboration on for a file (`{name, content, kind?, folder_id?}`, at most 8 MB); the list gives each document `online: [{name, colour}]` (who has it open now, one entry per person, in the order they joined, at most 8) and `online_count` (all of them) |
| `GET/PATCH/DELETE /api/documents/{id}` | `PATCH` takes `name` and/or `folder_id` (`null`: the top of the project) |
| `GET /api/documents/{id}/content?seq=\|version=` | `{document, seq, content}`: the document at any point (backups, pinned versions) |
| `GET /api/documents/{id}/history?before=&after=&limit=` | newest first, at most `limit` and 16 MiB of operations a page; page on with `before=<the last seq>` until a page is empty |
| `GET/POST /api/documents/{id}/versions` · `DELETE …/versions/{id}` | named versions |
| `POST /api/documents/{id}/restore` `{seq \| version}` | one new change that makes the document what it was; 409 `{code: "locked", message, conflicts}` while someone else holds a lock |
| `GET /api/documents/{id}/references` | assembly: what each component is pinned to and whether its part has moved on |
| `GET /api/documents/{id}/used-in` | part: the assemblies that use it |
| `WS /api/documents/{id}/ws` | live editing (below) |

Browser sign-in: the app makes up a random `code_verifier` (43+ characters), sends its SHA-256 in lower-case
hex as `code_challenge`, and sends the verifier itself with the code. Errors it can get back on the loopback
address: `invite_required`, `account_exists_use_password` (an account with that address exists and nobody
ever confirmed the address is its owner's: sign in with the password, then add the provider with `link`),
`identity_in_use`, `no_verified_email`, `provider_refused`, `provider_unreachable`, `cancelled`,
`server_error`.

Roles. Organisation: owner > admin > member. Project: owner > editor > viewer. Organisation owners and
admins own every project. **A project you are not in answers 404**, exactly like one that does not exist
(so do its documents, folders and blobs).

### What the status codes mean

| | |
|---|---|
| 400 | the request is wrong; for a blob upload also: the bytes do not match the hash, the upload stalled (no byte for 60 s), it was slower than the minimum rate (`the upload was too slow and was given up on`), or it ran past its time limit (`the upload took too long and was given up on`). Nothing was stored |
| 401 | sign in first, or a wrong address or password |
| 402 | an extension refused (a seat limit, a plan's quota); `detail` says why. Show it; trying again will not help |
| 403 | you are in the organisation or project but your role is too low |
| 404 | not there, or not yours to see (the same answer) |
| 409 | a conflict the message names (a name taken, the last owner leaving, a part still used by assemblies). Also **`another request changed the same thing at the same moment; try again`**: the database put this request behind another one (or waited 5 s for its lock) and nothing was written; send it again. And 409 `the server could not check this with its extensions in time; try again` when an extension did not answer within 5 s |
| 413 | the body is too large (1 MiB for most routes, 8 MB to create a document, `max_blob_bytes` for a blob) |
| 422 | the body has the wrong shape or fails a check (text holding the zero character is refused everywhere) |
| 429 | too many attempts (sign-in: 10 in 10 minutes for one email address from one client address, 100 in an hour for one email address from anywhere; an IPv6 client counts as its /64), or as many of this as the server does at once for you: blob uploads of your account (4) or organisation (8), exports (one per account; two organisation exports and two account exports per server, `max_exports`) |
| 500 | the server failed; logged on the server. Rare; something unexpected, such as a database refusing a duplicate the route did not foresee |
| 503 | the server could not get to the request; **nothing was done**, send it again later. `the server is busy; try again` (no database connection came free, the organisation already has 64 requests queued or one waited 10 s for its turn, a statement ran out of time), `the request took too long; try again` (no answer within the request time limit, 60 s), a blob upload that found no room in the server's upload memory, or too many large bodies being read at once |

Requests that change an organisation's data run one at a time for that organisation, in the order they
arrive.

`emailed: false` in an invite's answer means the email was not confirmed sent within 10 seconds: it may
have failed, the server may have been sending as many emails as it sends at once, it may still be on its
way, or it was not sent because the organisation (50 an hour) or the account (200 a day) has had as many
invites emailed as it may. The invite exists either way and its link is in the answer: show the link when `emailed` is
false.

Deleting a project or an organisation answers at once: from that moment it is gone for everyone (404, its
open documents closed with 4404, its name free). The server removes its rows and files in the background
afterwards.

## Turning collaboration on

1. Take `doc.to_dict()`. Turn long strings (imported bodies) into blob references,
   `{"$blob": "<sha256 of the raw bytes>", "len": n}` (the server does not check `len`); ask `blobs/missing`
   which to upload; `PUT` those (raw bytes, streamed).
2. `POST /api/projects/{id}/documents` with `{name, content}` (and `kind: "assembly"` for an assembly).
   That content is state 0. In it, every non-empty list of maps with distinct string `id`s, at any depth,
   becomes a **keyed list** (below); `features` (and `components`, `mates` in an assembly) must be such
   lists or absent (missing ones are filled in as empty).
3. Open the WebSocket.

Per-user keys (`rollback`, `active_config`, `display_state`, `journal`, `name`) are stripped when a document
is created; clients never write them, nor `next_id` and `counters` (all seven are refused in operations).
REST shows content as plain JSON: keyed lists come back as ordinary lists in their order.

## The WebSocket

Frames are **binary MessagePack maps**; the string field `t` says which message it is. A text frame is
answered `error` (`bad_message`). Unknown fields are ignored; a known field given twice is refused. An
optional field may be absent or `nil`; the server leaves out optional fields it has no value for. NaN and
infinity are refused everywhere, and so are binary and extension values.

A client frame may hold at most 1,000,000 values (every scalar, key and container counts) and be nested at
most 136 containers deep. Only a `change` may be longer than 512 KiB (another one is answered `error`
`too_large`, and the connection kept); no frame may be longer than `max_change_bytes` + 64 KiB (8 MiB +
64 KiB by default): just over that it is closed with 1009, far over it with 1002.

### Values, paths and operations

A document is a tree of maps, lists and scalars. A **keyed list** is a list whose items are maps with
distinct string ids; it is addressed by id at any depth, and its order is kept by a **position** per item.
On the wire a keyed list is `{"$list": [[pos, item], ...]}`; a plain list of maps with distinct ids sent
in an operation is refused (send `$list`), and `$list` is never a map key, path segment or id. Other lists
are plain lists, replaced whole. Integers, floats and booleans are different values (`1`, `1.0`, `true`).
Map keys beginning `~` hold derived data each client recomputes (solver output): written last-writer-wins,
never a conflict, never lockable.

A **path** is a list of 1 to 64 segments, each a map key or a keyed-list item id: 1 to 200 characters, not
`$list`. A **position** is 1 to 1,024 base-62 digits (`0-9A-Za-z`, ordered as bytes) not ending in `0`;
equal positions are ordered by id. Values nest at most 64 levels.

| `op` | fields | |
|---|---|---|
| `set` | `path`, `value` | put a value at a map key (creating it) |
| `unset` | `path` | remove a map key |
| `insert` | `list`, `item`, `pos` | add `item` (a map with a string `id`) to the keyed list at `list`, at `pos` |
| `remove` | `list`, `id` | remove an item |
| `move` | `list`, `id`, `pos` | give an item a new position |

What an operation **writes**, for conflicts: `set`/`unset` their path; `insert` the new item's path;
`remove` the item's path (so it conflicts with any concurrent edit inside it); `move` the item's position
only. Two inserts never conflict.

**Ids.** Every new id, at any depth (features, sketch entities, dimensions, components, mates), is
`<user id>.<client id>.<counter>`, where `<user id>.<client id>` is `welcome.you.client_id` and the counter
never goes backwards for that client id. Ids of another form are legacy ids, allowed only in a document's
initial content. An id may be put back by anyone at the path where its removal is still remembered (undoing
a removal, restoring someone's deleted item); an item moved to another list by `remove` and `insert` needs
new ids at every depth; ids already under a `set`'s path may be rearranged within that `set`.

**Names.** Feature names must be unique at the end of a change (not after each operation, so one change can
swap two names). A clash is refused `name_taken`; the client renames and resends.

### Messages a client sends

| `t` | fields | |
|---|---|---|
| `hello` | `protocol`, `token`, `client_id`, `format_version`?, `last_seq`?, `app`? | the first frame, within 15 s. `protocol` is 2; `token` 1 to 512 characters; `client_id` 8 to 64 characters of `[A-Za-z0-9_-]`; `app` at most 100 characters |
| `change` | `change_id`, `base_seq`, `ops`, `reads`?, `depends`?, `undo_of`?, `command`? | `change_id` 1 to 64 characters; 1 to 10,000 `ops`; at most 1,000 `reads` (paths); at most 64 distinct `depends` (change ids); `command` at most 200 characters (history keeps 100) |
| `change.revert` | `request_id`, `seq` | revert anyone's change from history (editors); `request_id` at most 64 characters |
| `lock.acquire` | `request_id`, `paths` | 1 to 256 paths |
| `lock.release` | `paths` | 1 to 256 paths; no answer |
| `presence` | `data` | a map of at most 16 KB, relayed as it is |
| `draft` | `data` | a map of at most 128 KB, or `nil` to clear |
| `rebuild.report` | `seq`, `errors` | `errors`: feature id → message, at most 500 entries, keys at most 200 characters, messages at most 500 |
| `ping` | | answered `pong` |

### Messages the server sends

| `t` | fields | |
|---|---|---|
| `welcome` | `seq`, `project_id`, `role`, `read_only`, `read_only_reason`?, `you`, `peers`, `locks`, `broken`, `content`?, `changes`? | see "Joining" |
| `error` | `code`, `message`, `request_id`? | a refusal that is not an answer to a change |
| `change.accepted` | `change_id`, `seq` | your change is durable at `seq` |
| `change.rejected` | `change_id`, `code`, `message`, `conflicts` | your change was not stored; `conflicts` lists what was in the way |
| `change` | `seq`, `client_id`, `author`, `author_name`, `change_id`, `ops`, `command`, `undo_of`?, `reverts`? | a durable change (see "Changes from others") |
| `change.revert.result` | `request_id`, `ok`, `seq`?, `ops`, `code`?, `message`? | the answer to `change.revert` |
| `lock.result` | `request_id`, `granted`, `denied` | the answer to `lock.acquire`, path by path |
| `lock.changed` | `locks`, `released` | locks new or changed (detached, attached again), and paths no longer locked |
| `peer.joined` | `peer` | |
| `peer.left` | `client_id` | |
| `presence` | `client_id`, `data` | |
| `draft` | `client_id`, `data` | `data` `nil`: the peer's draft is cleared |
| `presence.batch` | `presence`, `drafts` | several peers' latest presence and drafts in one frame |
| `peer.list` | `peers` | every peer as it is now (see "Falling behind") |
| `rebuild.status` | `seq`, `broken` | |
| `version.created` | `id`, `name`, `seq` | |
| `reference.update` | `document`, `components` | |
| `pong` | `seq` | the durable seq |

Structures inside them:

| | fields | |
|---|---|---|
| peer | `client_id`, `user_id`, `name`, `colour`, `read_only`?, `presence`?, `draft`? | `read_only` only when true |
| lock | `path`, `client_id`, `user_id`, `name`, `detached` | `detached`: the holder's connection is gone; the lock waits for it |
| denied | `path`, `why`, `holder`? | `holder` (a lock) exactly when `why` is `held` |
| broken | `error`, `by_seq`?, `by_user`?, `by_name`?, `command`?, `since_seq`? | the blame fields all present or all absent; the break came in somewhere in `since_seq..=by_seq` |
| conflict | `path`, `seq`, `by`, `kind` | `kind`: `written`, `removed`, `read`, `locked` (`by` is the holder's client id, `seq` 0) |

`presence.batch`: `presence` is a map client id → data, `drafts` client id → data or `nil`; 2 to 256 entries
in all, each client at most once per kind, client ids at most 129 characters (user id, a dot, the hello's
client id). Apply each entry as the `presence` or `draft` message it stands for. The server relays presence
and drafts at most every 50 ms per document, each peer's latest only: a lone entry comes as `presence` or
`draft`, several as `presence.batch`. A client never gets its own back.

`peer.list` and `welcome.peers` name at most 65,536 peers, each `client_id` once.

### Joining

`client_id` is one random string per running app (make a new one each time the app starts; keep it across
reconnects of that run). The server knows you as `<your user id>.<client_id>`: that is
`welcome.you.client_id`, and the form every other message uses (peers, locks, presence, the `client_id` on
changes). Use it to recognise yourself. Two connections with the same `client_id` replace each other: the
older one is closed with 4409.

A refused `hello` is answered `error` and closed; after the `welcome`, the last two codes below are answered
with the connection kept. Every `code` an `error` frame carries:

| `code` | close | |
|---|---|---|
| `bad_hello` | 4400 | the first frame was late, text, not a `hello`, or malformed |
| `protocol` | 4426 | `protocol` is not 2 |
| `unauthorized` | 4401 | the token is not a live session |
| `too_many_connections` | 4429 | your account has 64 document connections open already (`max_connections_per_user`); close one, reconnecting will not help |
| `server_full` | 4429 | the server holds as many document connections as it may (`max_connections` less an eighth, and less at least 256, kept for REST); sent at once, before the `hello` is read. Try again later, with back-off |
| `not_found` | 4404 | the document does not exist, was deleted, or is not yours to open (the same answer) |
| `busy` | 4408 | the server could not take the join in time; reconnect |
| `shutting_down` | 1001 | |
| `server_error` | 1011 | try again |
| `bad_message` | none | after the `welcome`: a text frame, or one that is not a valid message; it was ignored |
| `too_large` | none | after the `welcome`: a frame other than a `change` over 512 KiB; it was ignored |

`welcome` carries `seq` (the durable head), the `project_id` (blobs are fetched through it), your `role`,
whether you are `read_only` and why (`read_only_reason`), `you`, the other `peers` (with their last presence
and draft), all `locks`, `broken` features, and either `content` (the whole document, in the wire form) or,
when `last_seq` was within the last 500 changes (`catchup_limit`), `changes` since then (change events,
including your own: `client_id` + `change_id` identify them). You are read-only when you are a viewer, or
when your `format_version` differs from the document's.

After the `welcome`, a malformed frame is answered `error` (`bad_message`) and the connection kept; a second
`hello` is ignored.

### Making a change

Keep two things: `confirmed`, the document as the server has it, and the live document (confirmed plus your
changes the server has not answered yet, in order).

1. Apply the command locally; `diff(before, after)` gives the operations, and keep their inverse.
2. Send `change {change_id, base_seq, ops, reads, depends, command}`. `base_seq` is the last seq you have
   applied. Do not wait: further changes can go out straight away with the same `base_seq`.
   - `reads`: paths the change relies on existing without writing them (a feature's sketch, a mate's
     components).
   - `depends`: your own unanswered changes whose writes or removals overlap this change's writes,
     removals or reads (or which created something it touches).
3. `change.accepted {change_id, seq}` → apply the ops to `confirmed`, set your seq.
4. `change.rejected {change_id, code, message, conflicts}` → the change was not stored. Roll it back
   together with every pending change that depends on it, newest first, using the inverses you kept. Codes:
   - `stale` — another client wrote what this change writes (or removed something it reads) after its
     `base_seq`; `conflicts` says what and who
   - `locked` — another client holds a lock on it; `conflicts` names the locks
   - `dependency` — a change in `depends` was refused (or answered `server_error`)
   - `name_taken`, `id_taken` — a feature name or an item id would repeat
   - `invalid` — it does not apply, breaks a rule (an id not yours, a protected key, `base_seq` ahead of
     the server, a component pinned to a part or state you cannot use, …), or is too large for the server
   - `missing_blob` — upload the blob first, then resend
   - `read_only` — you may only look
   - `too_old` — `base_seq` is older than the history the server keeps for judging; reload and redo it
   - `server_error` — **the server did not store it; send it again** (with the same `change_id`). A change
     that depends on it is refused `dependency` meanwhile and rolled back with it.

A change answered `server_error` is judged afresh when it is resent.

Two changes conflict only if one writes a path that the other wrote, a prefix of it, or a path under it
(after the other's base), or removes what the other reads. Edits to different features, or to different
parameters of one feature, from the same base both go in. Accepted changes are applied to the current
document, not their base: one that no longer applies is `invalid`.

**Resending is safe.** A `change_id` the server already holds is answered with the seq it got
(`change.accepted`), however old. So a `change_id` must never be used twice for different changes by the
same client id: the server keeps them in the history for good. Use a UUID.

**Answers are idempotent on your side too.** An `accepted` for a change id you have already settled (a
resend after a reconnect, or one whose event you already applied) is a no-op: never apply it again, and
never move your seq backwards (an `accepted` whose `seq` is at or below the seq you have applied changes
nothing).

### Changes from others

`change {seq, client_id, author, author_name, change_id, ops, command, undo_of?, reverts?}` → apply to
`confirmed`, rebuild the live document (confirmed plus what is still pending). Events arrive in seq order.
`author` is the user id; a change the server made (a restore, a revert from history) has `client_id`
`~server` and an empty `change_id`.

You do not receive your own change back while you wait for its answer: you get `change.accepted`. You do
receive it as an event in a reconnect catch-up (`welcome.changes`), and also live when it became durable
before you resent it on a new connection (after a reconnect or a takeover, the change of your client id
that the old connection had sent arrives as an ordinary `change` event, in seq order, carrying your
`client_id` and its `change_id`): mark that change answered when it arrives, and treat the later `accepted`
for its resend as already settled.

When a peer's change arrives, clear that peer's draft yourself (see "Presence and previews").

### Limits

Feature ids, names and types: at most 200 characters. Nesting: at most 64 levels. One change: 8 MB (bigger
data goes in blobs); at most 4,096 distinct blob references in it. A connection may have at most **2,000
unanswered changes** in flight: one more closes it with 4408. When you replay an offline queue, keep at most
about 1,000 in flight and send the next as answers come. While one of a connection's messages waits for work
the server does off to the side (a blob check, a journal lookup for an old change id, a large change being
judged, a revert), its later messages wait behind it in order; more than **1,024** waiting messages, or
more than 4 × `max_change_bytes` of them (32 MiB by default), also close it with 4408. Presence and drafts
(which never wait): at most 60 a second per connection together (`presence_rate`), more are dropped
silently.

### Locks

`lock.acquire {request_id, paths}` when a sketch or a feature's panel opens; `lock.release {paths}` when it
closes. Any path that resolves in the document can be locked (a map key, an item, a list), and a single
top-level key that does not exist yet; not a path with a `~` segment or under a key the server owns. A lock
is in the way of another client's lock **at, under or above** it, with one exception: a lock on a list
itself and locks on its items do not exclude each other (hold `["features"]` to delete or reorder while
others hold single features).

`lock.result` grants path by path; a denied path says `why`: `held` (with the `holder`), `not_lockable`,
`quota` (256 locks a client, 1,024 a user, 8,192 a document), `rate` (lock requests are paid per path from a
bucket of 512 refilling at 64 a second; an acquire it cannot pay is denied whole), `read_only`. Everyone gets
`lock.changed`. A lock stops other clients' changes to what it covers (`locked`); a lock on a list stops
inserting, removing, replacing or moving its items but not edits inside one.

Send `ping` every 15 s: a connected client silent for `lock_ttl` (45 s) loses its locks. After a disconnect
locks are kept, `detached`, for `lock_grace` (60 s) and handed back when the same client id returns; a
takeover by the same client id keeps them. A connection closed because its access ended (4401, 4403, 4404),
including a session that expired while connected, has its locks released at once. A server restart, or the
document unloading, drops all locks: take them again after reconnecting.

A `lock.changed` or `lock.result` may name a `client_id` you were never told about, or one that has left,
when you fell behind on peer messages (see "Falling behind"): show the lock anyway, with the name the lock
carries, and do not expect the peer to be in your list.

### Undo and revert

**Undo** of your own change is an ordinary `change` with `undo_of` set to the change id being undone and the
inverse you kept as its `ops`. The server judges it with the inverse it stored itself (never what you sent
for ids), against the older of your base and the undone change's seq: it is `stale` when someone else has
written the same thing since. A change is undone once (`invalid` the second time); redo is an undo of the
undo. Its event carries `undo_of` (the change id) and `reverts` (the undone seq). openswx's own
whole-document undo must be off for cloud documents.

**Revert** from history, `change.revert {request_id, seq}` (editors): the server applies the change's stored
inverse as a change it makes for you and answers `change.revert.result {request_id, ok: true, seq, ops}`;
apply `ops` yourself (you get no `change` event for it; everyone else does). Failure: `ok: false` with `code`
one of `read_only`, `not_found`, `already_reverted`, `stale`, `locked`, `invalid`, `name_taken`, `id_taken`,
`server_error` (try again), and a `message`.

### Presence and previews

`presence {data}` and `draft {data}` are relayed to everyone else (the server checks their size and that
they are plain data, and does not look inside), at most every 50 ms per document with each peer's latest
only, and never stored. Suggested `presence.data`: `pointer` (3D point + persistent face name), `selection`
(persistent names), `tool`, `camera`. `draft {data: nil}` clears a preview. A draft from a connection that
may only look is dropped. openswx also sends `pointer.plane: true` when the pointer is over empty
background: the point is then where the ray meets a plane through the view's centre, facing the viewer, and
is drawn without hiding it behind the model.

**Late drafts.** Presence and drafts can arrive up to a tick (50 ms) after a later message of the same peer
that is never delayed: a draft cleared just after its change was committed may come after that change, and
so may a draft the peer sent just before it. When a peer's `change` arrives, clear that peer's draft
yourself; a draft that still comes within the tick is applied as it says, and the peer's own clear follows
it.

**Treat presence and drafts as hostile input.** They come from other people (any project member, viewers
included) and the server passes them on unchecked. A client must never run what a draft names: to preview
someone's unfinished feature, accept only the commands that create features, with plain data arguments,
and nothing that reads or writes files, runs scripts or acts on documents. Check every number before using
it (a camera or point can be enormous), cap how much work previews cause (one in flight per person, a time
limit), and escape every string before it reaches a page. The same goes for names, messages and anything
else other people chose, in every message type.

### Falling behind

What the server sends is of two kinds:

- **Soft frames**: `peer.joined`, `peer.left`, `presence`, `draft` and `presence.batch`. For a client that
  does not read fast enough (more than 64 of them, or 1 MiB, waiting) they are **dropped**, and it is not
  closed for them. Once it has read everything the server had for it, it is sent `peer.list {peers}` (at
  most once a second), which **replaces** what it knew of the others:
  - it names **every** peer as it is now, **the recipient included** (one list is shared by every client
    caught up at once): skip the entry whose `client_id` is your own;
  - it carries **no drafts** (`draft` is absent): forget the drafts you had; each peer's current draft
    follows, as a `draft` message, after the list;
  - it carries the peers' presence up to **1 MiB** of it in all, in the order they joined; a peer past that
    comes **without** `presence` (absent does not mean it has none), and its presence follows after the
    list, in `presence` or `presence.batch` messages like any other.
- **Everything else** (answers, changes, lock messages, `welcome`, `error`, `rebuild.status`, …) is never
  dropped. A client too slow for those is closed with 4408: more than 2,000 frames waiting, or more than
  four times `max_change_bytes` (32 MiB) waiting behind the frame being written (one big frame by itself,
  such as a welcome with a large document, never is).

A client that stays behind on the soft frames while it reads nothing at all is **frozen** and closed with
4408: when it has been behind for 30 s, its socket has not finished what it was handed for that long, and
that long has also passed since a 16 KiB/s link would have read everything it was sent (soft frames
included). A slow link taking a big welcome, a big change or a batch of drafts is not closed for it.
That allowance is never more than 256 s (4 MiB at 16 KiB/s) after it stopped taking frames, plus the time
of what it was handed since: a client that read quickly and then stopped reading is closed at most
30 s + 4 MiB / 16 KiB/s (under 5 minutes) after it stopped taking frames, plus the time of anything it was
handed since.

### Rebuild reports

After applying a change and rebuilding, send `rebuild.report {seq, errors: {feature id: message}}`. The
server pins new failures on the change at `seq` and tells everyone
`rebuild.status {seq, broken: {feature id: broken}}`. The first report after the document is opened on the
server only sets the baseline: failures already there are listed without a cause. Reports from read-only
connections, for a `seq` beyond the head, or not newer than the last one processed, are ignored.

### Access while connected

The server closes a connection when its access ends: 4401 when the session ends or the account is
deleted or disabled (a password change closes that account's connections at once, and a session that
expires while connected is closed by the document's next sweep), 4403 when the account's role in the
project went or changed (reconnect to get the new one), 4404 when the document, its project or its
organisation was deleted. Sessions and roles are also checked against the database every 30 s.

### Reconnecting

Reconnect with `last_seq`. Apply `welcome.changes` (or replace everything with `welcome.content`), then
resend every unanswered change with its original `change_id`, `base_seq` and `depends` (keeping at most
about 1,000 in flight). Each resend is answered once: `accepted` with its seq if it was stored (or when it
is), otherwise judged now. Some may come back `stale`: show the conflict (keep mine as a copy / discard /
compare).

## Assemblies

Create one with `kind: "assembly"`. Besides `features`, an assembly has two more keyed lists, `components`
and `mates`, edited and locked like any other (`["components", "<id>"]`). Every entry needs an `id` the
client makes (in the id form above). In a part, a change that writes `components` or `mates` is `invalid`.

A component does not carry a copy of its part. It points at one:

```
{"id": "...", ..., "ref": {"document": "<part document id>", "seq": 42, "version": "Rev A"}}
```

`seq` is what counts; `version` is a label for display that the server does not check (`GET
.../references` reports the real versions, in `pinned.versions`: **prefer `pinned.versions` over
`ref.version`**). The part must be in the same organisation, the person making the change must be able to
open it, and it must have that state; otherwise the change is `invalid`, with one message whatever failed.
To show the assembly, load each part with `GET /api/documents/{part}/content?seq=<seq>`.

Editing a part never changes an assembly. When a part gets a new named version, everyone in an assembly
that uses an older state receives `reference.update {document, components}` (nothing more: not everyone in
the assembly may be allowed to know about the part), and `GET .../references` reports `update_available`,
`latest_version`, `newer_edits` and `changes_since_pinned` per component. To update, change the component's
`ref` like any other edit (it can be undone); choosing a part, or another state of one, needs access to that
part, while moving, removing or restoring a component that already uses it does not. Before doing so the
client should load the newer part, check which mates still resolve, and show the result;
`GET /api/documents/{part}/history?after=<pinned>&before=<newer + 1>` lists what changed in between. A part
that assemblies use cannot be deleted (409 with the list).

## Close codes

| code | meaning | what the client does |
|---|---|---|
| 1000 | a normal close: the connection left the room (its client closed it) | nothing; reconnect if the app did not mean to close |
| 1001 | the server is shutting down | reconnect after a moment |
| 1002 | a frame far over the size limit, or not a WebSocket frame | fix the client; do not resend the same frame |
| 1008 | the client kept sending change ids the journal already held under other changes | fix the client |
| 1009 | a frame just over the size limit | put the data in blobs |
| 1011 | the server failed | reconnect and resend |
| 4400 | bad hello | fix the client |
| 4401 | sign in again | ask for a sign-in; locks were released |
| 4403 | your access changed | reconnect to get the new role; locks were released |
| 4404 | the document is gone (or not yours) | close it; locks were released |
| 4408 | too slow, or the server could not take you now | reconnect at once and resend |
| 4409 | the same client id connected again | do not reconnect from this connection (another one of yours has the document) |
| 4410 | **moved / reconnect**: the document was reloaded (the journal could not say whether a change was stored, the server lost the document's lease, its room stopped unexpectedly, or it had to start the join again) | reconnect at once and resend everything unanswered; resends that were stored are answered with their seq |
| 4426 | protocol version | update the app |
| 4429 | too many document connections for this account (`error` `too_many_connections`), or on this server (`server_full`) | for the account: close another; reconnecting at once will not help (a reconnect that races its own old connection at the cap can get it too: try again after a moment). For the server: try again later, with back-off |

The close reason is a short text (at most 120 bytes) for logs; branch on the code.
