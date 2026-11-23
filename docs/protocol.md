# Protocol

For whoever writes a client (the openswx module, first of all). Message and operation definitions are in
`protocol/openswx_collab_protocol/`; this page is the flow and the rules around them.

## REST (JSON, `Authorization: Bearer <token>`)

| | |
|---|---|
| `GET /api/server` | registration mode (`first_user`, `invite`, `open`), sign-in providers, whether it can send `email`, protocol version |
| `POST /api/auth/register` `{email, password, name, invite?}` | → `{token, user}` |
| `DELETE /api/me` `{current_password?}` | delete your account (password, or a sign-in from the last 10 minutes); refused (409) while you are an organisation's only owner or the server's last admin |
| `GET /api/me/export` | everything the server holds about you, as JSON |
| `POST /api/auth/password-reset` `{email}` | emails a link to `/reset-password/<token>` (a page that sets the password); always 202, 501 when the server cannot send email |
| `POST /api/auth/password-reset/confirm` `{token, password}` | sets it, signs out everywhere |
| `POST /api/auth/login` `{email, password}` · `POST /api/auth/logout` · `GET/PATCH /api/me` | |
| `GET /api/auth/oauth/{google\|github}/start?redirect_uri=http://127.0.0.1:<port>/…&code_challenge=&invite=` | browser sign-in; ends on the loopback address with `?code=` or `?error=` |
| `POST /api/auth/exchange` `{code, code_verifier}` | → `{token, user}` |
| `POST /api/auth/oauth/{google\|github}/link` `{redirect_uri, code_challenge}` | signed in: add a provider to this account; returns `{url}` to open in the browser |
| `GET/POST /api/orgs` · `GET /api/orgs/{id}/members` · `PATCH/DELETE …/members/{user}` | |
| `DELETE /api/orgs/{id}` `{confirm_name, current_password?}` | owners: delete the organisation and everything in it |
| `GET /api/orgs/{id}/export` | owners: a zip of every document (content, history, versions), the members and the blobs |
| `GET/POST /api/orgs/{id}/invites` · `DELETE …/invites/{id}` · `POST /api/invites/accept` `{token}` | an invite with an `email` is emailed when the server can (`send_email: false` to skip); the answer says `emailed` |
| `GET/POST /api/orgs/{id}/projects` · `PATCH/DELETE /api/projects/{id}` · `GET …/members` · `PUT/DELETE …/members/{user}` | |
| `POST /api/projects/{id}/blobs/missing` `[sha256…]` · `PUT/GET /api/projects/{id}/blobs/{sha256}` | large strings |
| `GET/POST /api/projects/{id}/documents` | create = turn collaboration on for a file; the list gives each document `online: [{name, colour}]` (who has it open now, one entry per person, in the order they joined, at most 8) and `online_count` (all of them); `[]` and `0` when nobody has it open |
| `GET/PATCH/DELETE /api/documents/{id}` | |
| `GET /api/documents/{id}/content?seq=\|version=` | the document at any point (backups, pinned versions) |
| `GET /api/documents/{id}/history?before=&after=&limit=` | newest first |
| `GET/POST /api/documents/{id}/versions` · `DELETE …/versions/{id}` | named versions |
| `POST /api/documents/{id}/restore` `{seq \| version}` | |
| `GET /api/documents/{id}/references` | assembly: what each component is pinned to and whether its part has moved on |
| `GET /api/documents/{id}/used-in` | part: the assemblies that use it |
| `WS /api/documents/{id}/ws` | live editing |

Browser sign-in: the app makes up a random `code_verifier` (43+ characters), sends its SHA-256 in lower-case
hex as `code_challenge`, and sends the verifier itself with the code. Errors it can get back on the loopback
address: `invite_required`, `account_exists_use_password` (an account with that address exists and nobody
ever confirmed the address is its owner's: sign in with the password, then add the provider with `link`),
`identity_in_use`, `no_verified_email`,
`provider_refused`, `provider_unreachable`, `cancelled`.

Roles. Organisation: owner > admin > member. Project: owner > editor > viewer. Organisation owners and
admins own every project. A project you are not in answers 404.

## Turning collaboration on

1. Take `doc.to_dict()`. Run it through `ops.externalize` so long strings (imported bodies) become
   `{"$blob": sha256, "len": n}`; ask `blobs/missing` which to upload; `PUT` those.
2. `POST /api/projects/{id}/documents` with `{name, content}`. That content is state 0.
3. Open the WebSocket.

Per-user keys (`rollback`, `active_config`, `display_state`, `journal`, `name`) are stripped and never
synced. `next_id` and `counters` belong to the server.

## The WebSocket

First frame: `hello {token, client_id, format_version, last_seq}`. `client_id` is one random string per
running app (make a new one each time the app starts; keep it across reconnects of that run). Two apps
connecting with the same `client_id` keep disconnecting each other (4409). The answer is `welcome` (or
`error` and a close).

The server knows you as `<your user id>.<client_id>`: that is `welcome.you.client_id`, and the form every
other message uses (peers, locks, presence, the `client_id` on changes). Use it to recognise yourself.

`welcome` carries `seq`, the `project_id` (blobs are fetched through it), your role, whether you are read-only and why, the other `peers` (with their last
presence and draft), all `locks`, `broken` features, the ids and names you still have `reserved` (after a
short disconnect; a server restart forgets them), and either `content` (the whole document) or, when
`last_seq` was recent enough, `changes` since then.

You are read-only when you are a viewer, or when your `format_version` differs from the document's.

### Making a change

Keep two things: `confirmed`, the document as the server has it, and the live document (confirmed plus
your changes the server has not answered yet).

1. Before the command, note the document; after it, `ops.diff(before, after)`.
2. Send `change {change_id, base_seq, ops, command}` where `base_seq` is the last sequence number you
   have applied. Do not wait: further changes can go out straight away with the same `base_seq`.
3. `change.accepted {change_id, seq}` → apply the ops to `confirmed`, set your seq.
4. `change.rejected {change_id, code, message, conflicts}` → drop the change and reload the live
   document from `confirmed` plus whatever is still pending. Codes:
   - `stale` — someone else changed the same feature or key after your `base_seq`; `conflicts` says who
   - `locked` — someone else holds a lock on it; `conflicts` says who
   - `id_taken`, `invalid`, `missing_blob`, `read_only`
5. `change {seq, author, author_name, ops, …}` from others → apply to `confirmed`, call
   `ops.absorb_ids(confirmed)`, rebuild the live document. You never receive your own changes back,
   except in a reconnect catch-up, where `client_id` + `change_id` identify them.

Resending a `change_id` is safe: the server answers with the sequence number it already has. That also
means a `change_id` must never be used twice for different changes by the same client and user, ever:
the server remembers them in the history for good. Use a UUID.

Two changes conflict only if they share a target (`ops.targets`): a feature id, a top-level key, or the
feature order. Edits to different features from the same base both go in.

### Limits

Feature ids, names and types: text, at most 200 characters. Nesting: at most 64 levels. One change: 8 MB
(bigger data goes in blobs). `1`, `1.0` and `true` are different values. A client may hold 256 locks and
256 unused reserved ids and names (released when it disconnects); a name stem is at most 40 characters and
does not end in a digit. NaN and Infinity are refused everywhere. Feature names must be unique at the end
of a change, not after each operation, so one change can swap two names. A client that stops reading is disconnected (4408) once 32 MB is waiting for it.

### Ids and names

Before a command that creates features, ask `ids.reserve {features: n, names: ["Sketch", …]}` and make
openswx use the returned ids and names. Reserved ids cannot be used by anyone else. Creating a feature
therefore needs a connection. A refused reservation is an `error` (`bad_reserve`, or `read_only`) carrying
the `request_id` you sent. Reservations survive a disconnect for the lock grace period, and `welcome` lists
the ones you still have; reserving ahead skips numbers in names when ids go unused.

### Locks

`lock.acquire {targets}` when a sketch or a feature's panel opens; `lock.release` when it closes. Targets:
`f:<feature id>`, `k:<top-level key>`, `structure` (hold it while deleting or reordering). Everyone gets
`lock.changed`. Send `ping` every 15 s: a connected client silent for `lock_ttl` (45 s) loses its locks.
After a disconnect locks are kept for `lock_grace` (60 s) and handed back when the same `client_id`
returns. A server restart drops all locks.

### Reconnecting

Reconnect with `last_seq`. Apply `welcome.changes` (or replace everything with `welcome.content`), then
resend every unanswered change with its original `change_id` and `base_seq`. Some may come back `stale`:
show the conflict (keep mine as a copy / discard / compare).

### Undo

`change.revert {seq, mode}`. `undo` takes back your own change, `revert` anyone's (editors). The server
applies the stored inverse as a new change and answers `change.revert.result {ok, seq, ops}`; apply `ops`
yourself. Refused with `changed_since` when someone has changed the same thing afterwards. Redo is
undoing the undo. openswx's own whole-document undo must be off for cloud documents.

### Presence and previews

`presence {data}` and `draft {data}` are relayed to everyone else as they are (the server does not look
inside), limited to 60 a second, 16 KB per presence and 128 KB per draft, and never stored. Suggested `presence.data`: `pointer`
(3D point + persistent face name), `selection` (persistent names), `tool`, `camera`. `draft {data: null}`
clears a preview.

**Treat presence and drafts as hostile input.** They come from other people (any project member, viewers
included) and the server passes them on unchecked. A client must never run what a draft names: to preview
someone's unfinished feature, accept only the commands that create features, with plain data arguments,
and nothing that reads or writes files, runs scripts or acts on documents. Check every number before using
it (a camera or point can be NaN or enormous), cap how much work previews cause (one in flight per person,
a time limit), and escape every string before it reaches a page. The same goes for names, messages and
anything else other people chose, in every message type.

### Rebuild reports

After applying a change and rebuilding, send `rebuild.report {seq, errors: {feature id: message}}`. The
server pins new failures on the change that came in since the previous report and tells everyone:
`rebuild.status {broken: {feature id: {error, by_seq, by_name, command}}}`. Failures already there in the
first report after the document is opened on the server are listed without a cause (there is nothing to
compare them with).

The server checks every 30 s that each open connection still has a valid session and the role it joined
with, and closes it (4401 or 4403) otherwise. A password change closes that account's connections at once.

## Assemblies

Create one with `kind: "assembly"`. Besides `features`, an assembly has two more id-keyed lists,
`components` and `mates`; `ops.diff` and `ops.apply` handle them with `item.add`, `item.remove`,
`item.update` and `item.order` (each carrying `"in": "components"` or `"mates"`). Their targets are
`i:components:<id>` and `i:mates:<id>`, which is also what you lock. Every entry needs an `id` the client
makes up (a UUID is fine); the server does not hand these out.

A component does not carry a copy of its part. It points at one:

```
{"id": "c1", ..., "ref": {"document": "<part document id>", "seq": 42, "version": "Rev A"}}
```

`seq` is what counts; `version` is the label for display. The part must be in the same organisation, the
person making the change must be able to open it, and it must have that state. To show the assembly, load
each part with `GET /api/documents/{part}/content?seq=<seq>`.

Editing a part never changes an assembly. When a part gets a new named version, everyone in an assembly
that uses an older state receives `reference.update {document, components}` (nothing more: not everyone in
the assembly may be allowed to know about the part), and
`GET .../references` reports `update_available`, `latest_version`, `newer_edits` and
`changes_since_pinned` per component. To update, change the component's `ref` like any other edit (it can
be undone); choosing a part, or another state of one, needs access to that part, while moving, removing or
restoring a component that already uses it does not. Before doing so the client should load the newer part, check which mates still resolve, and
show the result; `GET /api/documents/{part}/history?after=<pinned>&before=<newer + 1>` lists what changed
in between. A part that assemblies use cannot be deleted (409 with the list).

### Close codes

4400 bad hello · 4401 sign in again · 4403 your access changed · 4404 document gone · 4408 too slow,
reconnect · 4409 the same client connected again · 4426 protocol version.
