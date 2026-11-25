
# Python collab server: behavioural inventory (parity checklist)

All paths are relative to `C:\Users\sjsev\Documents\GitHub\openswx-collab\`. Server code is in `server/openswx_collab_server/` (`S/` below). The wire models and op library are in `protocol/openswx_collab_protocol/` (`P/` below). Tests are in `tests/`.

## 0. Context from the spec and the carry-forward plan

**What is carried over unchanged** (spec "Server", line 188): accounts, orgs, projects, roles, invites, OAuth (Google, GitHub), password reset, and account/org export and deletion. The rules stay the same: 404 not 403, hashed tokens, secrets never logged.

**What changes:**
- Password hashing moves to the blocking pool.
- Sessions and roles are cached and invalidated by events, replacing the 30 s poll.
- `hooks.py` becomes an `Extensions` trait with: may_add_member, member_added, member_removed, before_org_delete, routes, migrations, authenticators, on_event, quota, server_info.
- New: `/metrics`, `/api/ready`, and folders.
- `ids.reserve` is removed. Under amendment 5, `next_id` and `counters` stop being server-owned and stop being synced.
- Ops become a journal: MessagePack, `parent_seq`, `branch_id`, `paths`, and `inverse` kept (amendment 7), unique on `(document_id, client_id, change_id)`.
- Snapshots hold the document plus the last-writer index, zstd MessagePack, keyed `(document, epoch, seq)` (amendment 6).
- Reject codes in v2: stale, locked, dependency, name_taken, id_taken, invalid, missing_blob, read_only, too_old, server_error.
- Limits kept: 8 MB per change, 256 MB per blob, 16 KB presence, 128 KB draft, 60 presence/s.

**Server items from the carry-forward plan:**
- Undo of a replaced subtree uses the journal's kept inverse.
- An unknown dependency is refused (three-state answer).
- Bound the cost per change.
- `Accepted` lacks the writes/removals/reads needed for the `paths` column.
- Pre-encoded snapshot path for welcomes.
- Token redaction in logs.
- Harden `Index::from_wire`.
- Decide the `~`-key edge cases.
- Only `try_between` may be called on client positions.
- PostgreSQL baseline still to be recorded.

---

## 1. REST routes

### Shared behaviour

- **Auth:** `Authorization: Bearer <token>`. `bearer()` strips `"bearer "` case-insensitively (`S/access.py:34-36`).
- **`current_user`:** 401 `"sign in first"` when the session row is missing, expired (`expires_at < now`), the user is missing, or `user.disabled` (`S/access.py:24-43`).
- **Error body:** FastAPI's `{"detail": ...}`. Some 409s carry a dict detail.
- **Validation:** pydantic errors are 422.
- **Prefix:** every router except the app-level pages uses `/api`.

### App-level routes (`S/app.py`)

| Method / path | Auth | Behaviour |
|---|---|---|
| GET `/api/health` (73) | none | `{"ok": true}` |
| GET `/api/server` (77) | none | `{"name":"openswx-collab","version":__version__ ("0.1.2"),"protocol":1,"registration":"first_user"\|"invite"\|"open","oauth":[...configured, order google then github],"email":mailer.enabled,"edition":hooks.name ("core"), **hooks.server_info()}` |
| GET `/invite/{token}` (86) | none | Static HTML: "You have been invited…; open openswx, Collaborate › Join with invite, paste this page's address". The token is not echoed. |
| GET `/reset-password/{token}` (95) | none | Static HTML `_RESET_PAGE` (24-45), header `Cache-Control: no-store`. The page's JS reads the token from `location.pathname` (never written into the page), checks the two fields match (minlength 8), POSTs `{token,password}` to `/api/auth/password-reset/confirm`, shows `d.message` or `d.detail`, and removes the form on success. Meta `referrer=no-referrer`. |

- Startup (lifespan, 51-62): `make_engine` → `init_db(auto_migrate, hooks)` → sessionmaker → `Hub(...)` → `hub.start()`. On exit: `hub.stop()`, then engine dispose.
- App state (65-71):
  - storage from `storage_url` and `s3_endpoint`
  - `login_limiter = Limiter()` (10 per 600 s)
  - `reset_limiter = Limiter(5, 3600)`
  - `Mailer(settings)`
  - `http_client` factory, default `httpx.AsyncClient(timeout=15)`
- Routers mounted: auth, orgs, documents, assemblies, lifecycle, then `hooks.routers()` (101-107).

### Auth (`S/auth.py`)

| Route | Auth | Body / params | Result |
|---|---|---|---|
| POST `/api/auth/register` (111) | none | `email:str`, `password:str 8..200`, `name:str ≤200 default ""`, `invite:str\|None` | **201** `{token, user: me_out}`. 422 `"not an email address"` (after `clean_email`). 409 when the email is taken. 403 `"this server is invite-only…"` when mode is invite and there is no usable invite. 402 from the seat hook via `accept_invite`. 404 `"this invite has just been used"` on a race. |
| POST `/api/auth/login` (124) | none | `email`, `password`, `label:str ""` | 200 `{token,user}`. 429 `"too many attempts…"` if blocked. 401 `"wrong email address or password"` for an unknown user, a disabled user, a wrong password, or a password-less account. |
| POST `/api/auth/logout` (141) | user | — | **204**. Deletes only the session of the presented token. |
| GET `/api/me` (147) | user | — | `me_out = {id,email,name,is_server_admin}` (76) |
| PATCH `/api/me` (158) | user | `name:str 1..200 \|None`, `password:str 8..200\|None`, `current_password\|None` | 200 `me_out`. See notes below. |
| GET `/api/auth/oauth/{name}/start` (220) | none | query `redirect_uri` (required), `code_challenge` (required → 422 if missing), `invite` (optional) | **302** to the provider. 404 if the provider is unknown or not configured. 400 for a non-loopback `redirect_uri`. 400 if the challenge does not match `^[0-9a-f]{64}$`. |
| POST `/api/auth/oauth/{name}/link` (245) | user | `redirect_uri`, `code_challenge`, `current_password\|None` | 200 `{"url": authorize_url}`. Same errors as start, plus 403 from `_confirm`. Stores `link_user_id`. |
| GET `/api/auth/oauth/{name}/callback` (266) | none | query `code`, `state`, `error` | See section 2. 404 if the provider is not configured. 400 `"this sign-in has expired; start again from the app"` if the state is missing, expired, or for another provider (plain HTTP error, not a redirect). Otherwise always a 302 back to the loopback. |
| POST `/api/auth/exchange` (341) | none | `code`, `code_verifier:str 43..200`, `label:""` | 200 `{token,user}`. 400 for an invalid/expired code. 400 `"issued to another app"` for a wrong verifier, which burns the code. 400 `"this code adds a sign-in method…"` for a link code. 403 `"this account is disabled"`. |
| POST `/api/auth/oauth/link/finish` (367) | user | `code`, `code_verifier 43..200` | 200 `{"linked":provider,"user":me_out}`. 400 for an invalid/expired code. 400 `"not issued to this app and account"` when it is not a link code, the user does not match, or the verifier is wrong (code burned). 409 when the identity belongs to another account. Sets `email_verified=True` if `link_email == account.email`. |
| GET `/api/me/providers` (394) | user | — | `{"providers": sorted[...], "has_password": bool}` |
| POST `/api/me/providers/{name}/unlink` (404) | user | `current_password\|None` | **204**. 403 from `_confirm`. 404 `"not linked"`. 409 `"this is the only way into the account; set a password first"` when there is no password and every identity is this provider. |
| POST `/api/auth/password-reset` (424) | none | `email` | **202** `{"message":"if there is an account for this address, a link is on its way"}`. 501 if email is not configured. See section 2. |
| POST `/api/auth/password-reset/confirm` (458) | none | `token`, `password 8..200` | 200 `{"message":"your password is set; sign in again in openswx"}`. 400 `"this link has expired or was already used; ask for a new one"` for an unknown/expired token, and also for a disabled user (token deleted). |

PATCH `/api/me` notes:
- The name is stripped.
- A password change needs `_confirm`. It deletes every session except the current one, commits, then calls `hub.drop_account(user.id)`, which closes **all** of that user's WebSockets (including the current device's) with 4401 (`S/auth.py:158-175`).

Password-reset confirm side effects (468-473):
- sets `password_hash`
- `email_verified=True`
- deletes all sessions
- deletes all of the user's resets
- `hub.drop_account` → 4401

### Organisations, invites, projects (`S/orgs.py`)

| Route | Min role | Body | Result / side effects |
|---|---|---|---|
| GET `/api/orgs` (78) | user | — | `[{id,name,role,members}]` for the caller's orgs, ordered by name then id. One query (tested). |
| POST `/api/orgs` (88) | server admin, or `open_registration` | `name 1..200` (stripped) | **201** `{id,name,role:"owner",members:1}`. 403 `"only the server admin can create organisations on this server"`. Creator becomes owner. |
| PATCH `/api/orgs/{org_id}` (101) | org owner | `name 1..200` | `{id,name}` |
| GET `/api/orgs/{org_id}/members` (110) | org member | — | `[{id,email,name,role}]` ordered by user name |
| PATCH `/api/orgs/{org_id}/members/{user_id}` (122) | org admin | `role: owner\|admin\|member` | `{user_id,role}`. See rules below. |
| DELETE `/api/orgs/{org_id}/members/{user_id}` (139) | member (self) or admin (others) | — | **204**. See rules below. |
| POST `/api/orgs/{org_id}/invites` (181) | org admin | see below | **201**. See below. |
| GET `/api/orgs/{org_id}/invites` (214) | org admin | — | Unaccepted and unexpired invites, ordered by `created_at`, as `_invite_out` |
| DELETE `/api/orgs/{org_id}/invites/{invite_id}` (222) | org admin | — | **204**. Hard delete. 404 `"no such invite"` if missing or in another org. |
| POST `/api/invites/accept` (237) | user | `token` | `{"org":{id,name},"role":<org role now>}`. 404 `"this invite is not valid (used, expired, or for another email address)"`. 402 seat. 404 race. |
| GET `/api/orgs/{org_id}/projects` (254) | org member | — | `[{id,name,org_id,role}]` from `visible_projects`, ordered by project name |
| POST `/api/orgs/{org_id}/projects` (261) | org member | `name 1..200` | **201** `{id,name,org_id,role:"owner"}`. Creator gets a ProjectMember row with role owner. |
| PATCH `/api/projects/{project_id}` (273) | project owner | `name 1..200` | `{id,name,org_id}` |
| DELETE `/api/projects/{project_id}` (282) | project owner | — | **204**. See below. |
| GET `/api/projects/{project_id}/members` (305) | project viewer | — | `[{id,email,name,role}]` from ProjectMember rows only (org admins do not appear unless they have a row), ordered by name |
| PUT `/api/projects/{project_id}/members/{user_id}` (318) | project owner | `role: owner\|editor\|viewer` | `{user_id,role}`. 422 `"that person is not in the organisation; invite them first"`. Upserts the row. `hub.drop_user(user_id, org_id)` (4403). |
| DELETE `/api/projects/{project_id}/members/{user_id}` (334) | project owner | — | **204**. 404 `"not a member of the project"`. `hub.drop_user` (4403). |

PATCH member role rules:
- 404 `"not a member"`.
- 403 `"only an owner can change who is an owner"` if the new or old role is owner and the caller is not an owner.
- 409 `"an organisation needs at least one owner"` when demoting the last owner.
- After commit, `hub.drop_user(user_id, org_id)` closes that user's sockets in every room of the org with 4403.

DELETE member rules:
- 404 `"not a member"`.
- 403 `"needs the organisation role admin"` when removing someone else without admin.
- Removing an owner needs the caller to be an owner (403) and must not remove the last owner (409).
- Deletes the user's ProjectMember rows in the org's projects.
- `hooks.member_removed` runs before commit.
- After commit, `hub.drop_user` (4403).

Create invite:
- Body:
  - `email: str|None`
  - `role: admin|member` (default member; owner not allowed)
  - `projects: [{project_id, role: owner|editor|viewer = editor}]`
  - `days: int 1..90 = 7`
  - `send_email: bool = True`
- 422 `"not an email address"`.
- 422 `"no project X in this organisation"`.
- 402 from `may_add_member(db, org, member_count)`; the reason, or `"this organisation has no free seats"`.
- Response: `{id,email,role,projects,expires_at,accepted_by, token, url: "{public_url}/invite/{token}", emailed}`. The token is shown once.
- Emails `invite_text` if an email is given, `send_email` is true, and the mailer is enabled.

Delete project (`S/orgs.py:282-302`):
1. Marks every not-yet-deleted document in the project `deleted_at=now` (takes the row locks).
2. `used_outside(project)`: assemblies in other projects, not deleted, that reference parts of this project. If any, rollback and 409 `{"message":"assemblies in other projects use parts of this project","assemblies":[{id,name} the caller can open],"in_projects_you_cannot_open":count}`.
3. Otherwise hard-deletes the project. FK cascade removes documents, ops, snapshots rows, versions, assembly_refs and project_members.
4. **Snapshot files in storage are not deleted.**
5. `hub.close_document(d, "the project was deleted")` for every document id, which closes with 4404.

### Documents, history, versions, blobs, WebSocket (`S/documents.py`)

`doc_out` (31): `{id,project_id,name,kind,format_version,seq(head_seq),created_by,created_at,updated_at}`, plus `role` when given.

| Route | Min role | Details |
|---|---|---|
| PUT `/api/projects/{pid}/blobs/{sha}` (41) | project editor | Raw body. 422 if `sha` does not match `^[0-9a-f]{64}$`. 413 `"too large"` if `Content-Length > max_blob_bytes`, or if the streamed size exceeds it. 422 `"the content does not match the sha256 in the address"`. Stores the raw bytes at `blob_key(org_id, sha)`. **204**. Blobs belong to the org (any project in the org reaches the same blob). |
| GET `/api/projects/{pid}/blobs/{sha}` (62) | project viewer | 404 `"no such blob"` (including a malformed sha). 200 `application/octet-stream`, `Cache-Control: private, max-age=31536000, immutable`. |
| POST `/api/projects/{pid}/blobs/missing` (73) | project viewer | Body: JSON list of strings. Only the first 2000 are considered. Returns those that are malformed or not in storage. |
| GET `/api/projects/{pid}/documents` (138) | project viewer | Non-deleted documents ordered by name: `doc_out(d, role) \| online(hub, id)`. `online` (126-135) reads `{"online":[{name,colour}] first 8 distinct users in join order,"online_count":n}` from the in-memory room. Skips closed connections. An evicted or absent room lists nobody. `MAX_ONLINE=8` (123). |
| POST `/api/projects/{pid}/documents` (148) | project editor | See below. |
| GET `/api/documents/{id}` (181) | viewer | `doc_out + role` |
| PATCH `/api/documents/{id}` (191) | editor | `name 1..200` (stripped) → `doc_out + role`. Open rooms are not notified. |
| DELETE `/api/documents/{id}` (200) | project owner | See below. |
| GET `/api/documents/{id}/content` (220) | viewer | See below. |
| GET `/api/documents/{id}/history` (246) | viewer | Query `before`, `after` (exclusive bounds), `limit` default 100, clamped to 1..500. Newest first. Each entry: `{seq, author:{id,name}, time, command, args, targets, reverts, reverted_by, broke}`. |
| GET `/api/documents/{id}/versions` (276) | viewer | Ordered by seq desc, then created_at desc. Each: `{id,name,note,seq,created_at,author:{id,name}}`. |
| POST `/api/documents/{id}/versions` (286) | editor | See below. |
| DELETE `/api/documents/{id}/versions/{vid}` (308) | owner | 404 `"no such version"`. **204**. |
| POST `/api/documents/{id}/restore` (324) | editor | See below. |
| WS `/api/documents/{id}/ws` (352) | token in hello | Section 5. |

Create document:
- Body: `name 1..200`, `kind: str = "part"`, `content: dict`.
- 422 if `kind` is not part or assembly.
- `_check_content` (89-120), all failures 422:
  - `features` defaults to `[]`; it must be a list of dicts whose `id`, `name` and `type` are strings of length 1..200 (MAX_NAME)
  - `check_value(content, 64)` (depth, NaN, etc.)
  - feature ids unique and feature names unique
  - for a part, `components` or `mates` present → 422 `"a part has no components"`
  - for an assembly, `components` and `mates` default to `[]` and must be lists of dicts with distinct string ids of 1..200 characters
  - removes LOCAL_ONLY keys (`rollback, active_config, display_state, journal, name`)
  - `absorb_ids` (sets `next_id` and `counters`)
- For an assembly, `check_refs(org, user, components)` (access checked for all components) → 422 with the OpError text.
- Missing blobs (malformed or absent from storage) → 409 `{"message":"upload these blobs first","blobs":[...]}`.
- `format_version = content["version"]` if it is an int, else null.
- Writes snapshot 0 to storage *before* the DB commit, then the Snapshot row and the AssemblyRef rows.
- **201** `doc_out + role`. A body containing NaN is refused 400/422 (JSON parse).

Delete document:
- Sets `deleted_at=now` and flushes (locks the row) before checking `used_in(part)`, all in one transaction.
- If non-deleted assemblies reference the document: rollback and 409 `{"message":"assemblies use this part; remove it from them first","assemblies":shown,"in_projects_you_cannot_open":hidden}`.
- Otherwise commit (a soft delete; the row stays) and `hub.close_document(id, "the document was deleted")` → 4404.
- **204**.

Get content:
- Query `seq`, `version` (a name; 404 `"no version named 'x'"`). `version` overrides `seq`.
- Always loads the room (`hub.room`).
- If `seq` is null or equals the head: `{"document":doc_out (no role),"seq":head,"content":state}`, JSON-encoded in a thread with `ensure_ascii=False`, `allow_nan=False`, compact separators.
- Otherwise `room.state_at(seq)`; LookupError → 404 `"the document has no state N"`.
- The content includes `next_id` and `counters`.

Create version:
- Body: `name 1..200`, `note ≤4000 = ""`, `seq|None` (default head).
- 422 unless `0 ≤ seq ≤ head_seq`.
- Duplicate name (UniqueConstraint) → 409 `"there is already a version named 'x'"`.
- **201** `_version_out`.
- Side effects:
  - `hub.notify(doc, {"type":"version.created","id","seq","name","author":user_id})`
  - for each `(assembly, components)` in `where_pinned_older(part, seq)`, i.e. components pinned at `AssemblyRef.seq < seq`: `hub.notify(assembly, {"type":"reference.update","document":part_id,"components":[ids sorted]})`
  - `notify` reaches only rooms that are loaded

Restore:
- Body: `seq|None`, `version|None` (a name; 404 if unknown; overrides `seq`).
- 422 `"give seq or version"`.
- `room.restore(user, seq)`, retried once on RoomGone. LookupError → 404.
- Failure → 409 with `{code,message,conflicts}`. Success → `{"seq": n}`. A no-op restore returns the current head.

### Assemblies (`S/assemblies.py`)

| Route | Min role | Result |
|---|---|---|
| GET `/api/documents/{id}/references` (119) | viewer | 422 `"only assemblies have references"`. Per AssemblyRef row, ordered by `component_id`; see below. |
| GET `/api/documents/{id}/used-in` (167) | viewer (of the part) | `[{"document":{id,name,project_id},"components":[{component,seq}]}]` for non-deleted assemblies the caller can open. Others are silently omitted. |

References row shapes:
- Part inaccessible or missing: `{"component","document":{"id":part_id,"accessible":false},"pinned":{"seq"}}`.
- Otherwise:
  ```
  {component, document:{id,name,project_id,accessible:true,deleted:bool},
   pinned:{seq, versions:[names at exactly that seq]}, head_seq,
   latest_version:{id,name,seq}|null, update_available: latest.seq>pinned,
   newer_edits: head_seq>pinned, changes_since_pinned:count}
  ```
  `changes_since_pinned` counts Op rows with `pinned < seq ≤ target`, where `target` is the latest version's seq if it is newer than the pin, else `head_seq`.

### Lifecycle (`S/lifecycle.py`)

| Route | Auth | Result |
|---|---|---|
| DELETE `/api/me` (47) | user | Body `{current_password\|None}`. **204**. Rules in section 2. |
| GET `/api/me/export` (91) | user | JSON (shape below) |
| DELETE `/api/orgs/{org_id}` (122) | org owner | Body `{current_password\|None, confirm_name:str}`. **204**. Rules in section 2. |
| GET `/api/orgs/{org_id}/export` (152) | org owner | A zip file (section 2) |

Account export shape:
```
{"account":{id,email,name,created_at,email_verified,server_admin,has_password,sign_in_providers:[...]},
 "organisations":[{id,name,role,since}],
 "projects":[{id,name,org_id,role}] (ProjectMember rows only),
 "sessions":[{created_at,expires_at,label}] (no hashes),
 "changes_you_made":[{document_id,seq,time,command}] ordered by created_at,
 "versions_you_named":[{document_id,name,seq,time}],
 "exported_at"}
```

---

## 2. Auth and security details

### Passwords (`S/security.py:12-30`)

- scrypt with N=2^15, r=8, p=1, maxmem 2^27, dklen 32, 16-byte random salt.
- Stored format: `scrypt$32768$8$1$<b64 salt>$<b64 dk>` (standard base64).
- Verify parses N, r and p from the stored string and compares with `hmac.compare_digest`.
- Any parse error, a non-scrypt scheme, or a `None` hash → `False`.
- Verification runs on the event loop (synchronous).
- Length rules: 8..200 for register, PATCH /me, reset confirm and the CLI (minimum 8).

### Tokens (`S/security.py:33-39`)

- `secrets.token_urlsafe(32)`, about 43 characters.
- Stored as `sha256(token).hexdigest()` (64 hex) for sessions, OAuth states, login codes, password resets and invites.

### Sessions

- Table `sessions(token_hash PK, user_id, created_at, expires_at, label≤200)`.
- Lifetime: `session_days*86400`, default 30 days (`S/auth.py:69-73`).
- No sliding renewal.
- Expired rows are never purged.
- Logout deletes one row.
- Password change deletes all other rows.
- Reset confirm, the CLI `reset-password`/`create-admin` on an existing user, and account deletion delete all rows.

### Email normalisation (`S/security.py:42-47`)

- `strip().lower()`, then it must match `^[^@\s]{1,64}@[^@\s]{1,255}\.[^@\s.]{2,}$`, else `None`.
- Used for register, login (falls back to `""`), invites, reset requests, OAuth emails and the CLI.

### Rate limits (`S/security.py:54-71`)

- In-memory sliding window per key.
- `blocked` = at least `limit` hits within `window`.
- Login (`S/auth.py:126-135`):
  - key `"{email}|{client ip}"`, 10 failures per 600 s
  - blocked → 429 checked before the DB lookup
  - every failure increments
  - success clears the key
- Reset (`S/auth.py:432-438`):
  - `Limiter(5, 3600)`, keys `"{email}|{ip}"` and `"{email}"`
  - if either is blocked, the request silently returns the same 202
  - otherwise both keys are counted on **every** request, successful or not

### Registration modes (`S/auth.py:62-66`)

- `first_user` if the `users` table has 0 rows. Deleted and disabled accounts still count.
- Otherwise `open` if `open_registration`, else `invite`.

`create_user` (80-94):
- `find_invite(token, email)`.
- Invite mode without a usable invite → 403.
- Name = stripped name, or the email's local part.
- `is_server_admin = (mode == first_user)`.
- `email_verified = verified or first_user`.
- Flushes, then `accept_invite` if an invite was found. This also happens in open mode; a mismatched email-bound invite is simply ignored there.

Create org: server admin or open registration (`S/orgs.py:91`).

### Invites (`S/orgs.py:33-70, 181-246`)

- Expiry: `days*86400`, 1..90, default 7.
- Email binding: an invite with an email only matches a user or registration with exactly that (cleaned) email. For registration and accept, the email is passed in.
- A usable invite has `accepted_by IS NULL` and `expires_at ≥ now`.
- Single use: an atomic `UPDATE invites SET accepted_by=? WHERE id=? AND accepted_by IS NULL`. `rowcount != 1` → 404 `"this invite has just been used"`.
- The org row is then locked `FOR UPDATE`.
- If the user is not already a member: `may_add_member` (402 on refusal), then add an OrgMember with the invite's role, then `member_added`. Existing members keep their current role.
- Project grants: for each `{project_id, role}`, skip if the project is missing or in another org; add a ProjectMember (default role `viewer` if the grant lacks one; creation defaults to `editor`) only if none exists.
- An invite does **not** set `email_verified`.
- Revoke is a hard delete.
- The list shows unaccepted, unexpired invites only.

### OAuth (`S/auth.py:45-59, 179-391`)

Provider URLs:
- Google:
  - authorize `https://accounts.google.com/o/oauth2/v2/auth`
  - token `https://oauth2.googleapis.com/token`
  - userinfo `https://openidconnect.googleapis.com/v1/userinfo`
  - scope `openid email profile`
- GitHub:
  - authorize `https://github.com/login/oauth/authorize`
  - token `https://github.com/login/oauth/access_token`
  - userinfo `https://api.github.com/user`
  - emails `https://api.github.com/user/emails`
  - scope `read:user user:email`

Configuration and redirect checks:
- A provider is enabled only if both its client id and secret are non-empty (`S/config.py:60-66`).
- Loopback check (179-185): scheme `http`; host one of `127.0.0.1`, `localhost`, `::1`; an explicit port; no userinfo; no fragment. Paths and query are allowed.
- Callback URL is `{public_url without trailing /}/api/auth/oauth/{name}/callback`.

`_begin` (202-217):
- Deletes expired `oauth_states`.
- Stores `OAuthState(state_hash, provider, redirect_uri, invite, challenge, link_user_id, expires_at=now+600)`.
- Commits.
- Authorize URL query: `client_id`, `redirect_uri=<callback>`, `response_type=code`, `scope`, `state`. No PKCE is sent to the provider.

PKCE between the app and the server:
- The app sends `code_challenge = sha256(verifier)` in lower-case hex, 64 characters, at start.
- `code_verifier` (43..200) is sent at exchange or link/finish.
- The server compares `sha256(verifier).hexdigest() == challenge`.

Callback steps (266-332):
1. Provider must be configured (404).
2. State lookup; it is deleted and committed before anything else (single use).
3. `back(**q)` redirects 302 to `redirect_uri` + (`&` if a `?` is already present, else `?`) + urlencoded query.
4. `error` present or no `code` → `error=<provider's error or "cancelled">`.
5. Token POST with form fields `client_id, client_secret, code, redirect_uri(callback), grant_type=authorization_code` and `Accept: application/json`.
6. No `access_token` → `error=provider_refused`.
7. `httpx.HTTPError`, KeyError or ValueError (non-2xx statuses included) → `error=provider_unreachable`.
8. Identity:
   - Google: `sub`; the email only if `email_verified in (True,"true")`; name.
   - GitHub: `id`; the email of the entry with `primary && verified` from `/user/emails`; name or login.
   - Both use `Authorization: Bearer`.

Link flow (`link_user_id` set):
- The identity exists for another user → `error=identity_in_use`.
- Otherwise a LoginCode (120 s) with `link_provider`, `link_subject`, `link_email=clean_email(email)` and the challenge → `code=...`.
- Nothing is attached at the callback.

Sign-in flow:
- If the identity exists, use its user.
- Otherwise:
  - no cleaned verified email → `no_verified_email`
  - no user with that email → `create_user(verified=True)`; a 403 → `invite_required`, any other HTTPException → `not_allowed` (rolled back)
  - a user exists but `email_verified` is false → `account_exists_use_password`
  - otherwise insert an OAuthIdentity, which links automatically
- User missing or disabled → `account_disabled`.
- Otherwise a LoginCode (120 s, challenge) → `code=`.

Complete list of loopback error codes: the provider's own `error` string passed through, `cancelled`, `provider_refused`, `provider_unreachable`, `identity_in_use`, `no_verified_email`, `invite_required`, `not_allowed`, `account_exists_use_password`, `account_disabled`.

Account linking rules:
- Joining by email requires `email_verified`.
- `email_verified` becomes true for: the first user; a CLI-created admin; a provider-created account; link/finish when the provider email equals the account email; reset confirm.
- Account deletion sets it to false.

`_confirm` (233-242), used for PATCH /me (password), link, unlink, DELETE /me and DELETE /orgs:
- An account with a password needs the correct `current_password`, else 403 `"the current password is wrong"`.
- An account without one needs the current session's `created_at` within 600 s, else 403 `"sign in again first (this needs a sign-in from the last ten minutes)"`.

Housekeeping gaps: expired login codes are deleted only when presented; there is no sweep.

### Password reset (`S/auth.py:424-474`)

- 501 if mail is not enabled, checked first.
- If the limiter allows and the user exists and is not disabled:
  - delete that user's resets and **all** expired resets
  - insert a new reset (expires `reset_minutes*60`, default 60 min)
  - commit
  - email `{public_url}/reset-password/{token}`
- Only the newest link works.
- The mail send is awaited inside the handler. Failure is swallowed and the response is the same.

### Admin rules

- `is_server_admin` is only set by the first registration or the CLI `create-admin`.
- The admin flag alone gives: creating orgs (when not open) and the deletion guard below.
- A server admin is not automatically a member of any org.

### Account deletion (`S/lifecycle.py:47-88`)

Refusals:
- 409 `{"message":"you are the only owner of these organisations…","orgs":[{id,name}]}` for every org where the user is an owner and the owner count is ≤1.
- If the user is a server admin: count non-disabled admins and other non-disabled users. If `admins ≤ 1 and others > 0` → 409 `"you are this server's only admin; make someone else admin first (openswx-collab-server create-admin)"`. The last person on a server may leave.

Effects:
- Deletes the user's rows in sessions, oauth_identities, login_codes, password_resets, org_members and project_members.
- Anonymises the user:
  - email `deleted-<id>@deleted.invalid`
  - name `"Deleted user"`
  - password_hash null
  - email_verified false
  - is_server_admin false
  - disabled true
  - deleted_at now
- `hooks.member_removed` per former org, before commit.
- Commit.
- `hub.drop_account` (4401), then `hub.forget_user(id, "Deleted user")`, which renames the user in the `names` cache of open rooms.
- Ops, versions, documents and invites keep the user id.

### Org deletion (`S/lifecycle.py:122-149`)

1. Owner only.
2. `_confirm`.
3. `confirm_name.strip() != org.name` → 422 `"type the organisation's name exactly to confirm"`.
4. `hooks.before_org_delete(db, org)`; raising an HTTPException refuses.
5. Collect: document ids of every document in the org (deleted ones included), snapshot keys, member ids.
6. `DELETE FROM orgs` (cascade), commit.
7. `close_document(d, "the organisation was deleted")` → 4404.
8. Delete each snapshot key and `storage.delete_prefix("orgs/<org>/")`, which removes the blobs. Errors are logged.
9. `hooks.member_removed` per member, **after** commit and never committed.

### Org export (`S/lifecycle.py:152-204`)

ZIP_DEFLATED, written to a temporary file:
- `organisation.json`: `{id,name,created_at,members:[{user:name,email,role}],exported_at,projects:[{id,name}]}`, JSON with indent=1 and `ensure_ascii=False`.
- Per non-deleted document `projects/<pid>/<did>/`:
  - `document.json` `{id,name,kind,seq,format_version,content}`; content comes from a loaded room at its head
  - `history.jsonl` lines `{seq,author:name,time,command,ops,reverts}` in seq order
  - `versions.json` `[{name,seq,note,author:name,time}]`
- `blobs/<sha>` for every blob referenced by current content or any op's `ops` (not `inverse_ops`) that exists in storage.

Download: filename = org name with characters other than alphanumerics, `-` and `_` replaced by `-`, cut to 60, or `organisation` if empty, plus `-export.zip`; `application/zip`. The temporary file is deleted after sending, or on error.

---

## 3. Roles and access (`S/access.py`, `S/security.py:50-51`)

- `ORG_ROLES = member 1, admin 2, owner 3`. `PROJECT_ROLES = viewer 1, editor 2, owner 3`.
- `project_role` (60-67): not an org member → `None`. Org admin or owner → `"owner"` of every project in the org. Otherwise the ProjectMember role or `None`.
- `need_org` (51-57): no membership → **404** `"no such organisation"`. Role too low → 403 `"needs the organisation role X"`.
- `need_project` (70-77): project missing or role `None` → **404** `"no such project"`. Role too low → 403 `"needs the project role X"`.
- `need_document` (80-89): document missing or `deleted_at` set → 404 `"no such document"`. A 404 from the project → 404 `"no such document"`. A 403 passes through.
- `visible_projects` (92-98): admins and owners see every project as owner; others see only projects with a membership.

The 404-not-403 rule applies:
- to every org, project and document route
- to the WebSocket (role `None` or a deleted document → `not_found` / 4404)
- to invite accept (404 for every failure)
- to `check_refs`, which gives one message for missing and not yours: `"component X: there is no part P you can use"` (`S/assemblies.py:69-72`); names are hidden when access was not asked (`"that document"`)
- to delete guards (count of hidden assemblies)
- to references (`accessible:false`)
- to used-in (silent omission)
- to `reference.update`, which carries ids only

A 403 appears only when the thing is visible but the role is too low.

Viewers **can**:
- get a document, its content, history, versions, references and used-in
- list documents and project members
- GET blobs and POST `blobs/missing`
- connect to the WebSocket read-only (`welcome.you.read_only=true`, reason `"you have view access to this project"`)
- send presence (relayed)

Viewers **cannot**:
- PUT blobs, create, rename or delete documents, create or delete versions, restore
- send changes (`change.rejected` `read_only`)
- take locks (denied with an empty holder)
- reserve ids (`error` `read_only` with request_id)
- revert (`read_only`)
- send drafts (silently dropped)
- send rebuild reports (silently ignored)

Role minimums:
- Editor: create document, PUT blob, rename document, create version, restore, edit live, revert anyone's change.
- Project owner: delete document, delete version, rename or delete project, set or remove project members.
- Org member: create projects (and own them), list members.
- Org admin: invites, member roles (not owner), removing others.
- Org owner: rename, delete or export the org; change owner roles.

---

## 4. Database schema (`S/db.py`; migrations `S/migrations/versions/0001_baseline.py`, `0002_users_deleted_at.py`, `0003_password_resets.py`)

- Times are float epoch seconds. Ids are `uuid4().hex` (32 characters).
- SQLite settings (`S/db.py:215-229`): `connect_args timeout=30`, `PRAGMA foreign_keys=ON`, `journal_mode=WAL`, the parent directory is created.
- PostgreSQL: `pool_pre_ping`.
- Migrations: Alembic `render_as_batch`. Reflected tables unknown to the metadata (an extension's) are ignored (`migrations/env.py`).

| Table | Columns (type, null, default) | Keys / indexes |
|---|---|---|
| `meta` | key String(64) PK; value Text NN | Unused (kept for old databases) |
| `users` | id S32 PK; email S320 NN (lower case); name S200 NN; password_hash Text NULL; is_server_admin Bool NN default False; disabled Bool NN False; email_verified Bool NN False; created_at Float NN now; deleted_at Float NULL (0002) | unique index `ix_users_email` |
| `oauth_identities` | provider S32; subject S255; user_id FK users CASCADE NN | PK(provider, subject); ix_oauth_identities_user_id |
| `sessions` | token_hash S64 PK; user_id FK users CASCADE; created_at Float now; expires_at Float NN; label S200 default "" | ix_sessions_user_id |
| `oauth_states` | state_hash S64 PK; provider S32; redirect_uri Text; invite Text NULL; challenge S64 default ""; link_user_id S32 NULL (no FK); expires_at Float | — |
| `login_codes` | code_hash S64 PK; user_id FK users CASCADE; challenge S64 ""; link_provider S32 NULL; link_subject S255 NULL; link_email S320 NULL; expires_at Float | no index |
| `password_resets` (0003) | token_hash S64 PK; user_id FK users CASCADE; expires_at Float | ix_password_resets_user_id |
| `orgs` | id S32 PK; name S200; created_at Float | — |
| `org_members` | org_id FK orgs CASCADE; user_id FK users CASCADE; role S16 (owner\|admin\|member); created_at Float | PK(org_id, user_id); ix_org_members_user_id |
| `invites` | id S32 PK; token_hash S64 UNIQUE; org_id FK orgs CASCADE; email S320 NULL; role S16 default "member"; projects JSON NN default []; created_by S32 (no FK); created_at; expires_at; accepted_by S32 NULL (no FK) | ix_invites_org_id |
| `projects` | id S32 PK; org_id FK orgs CASCADE; name S200; created_at | ix_projects_org_id |
| `project_members` | project_id FK projects CASCADE; user_id FK users CASCADE; role S16 | PK(project_id, user_id); ix_project_members_user_id |
| `documents` | id S32 PK; project_id FK projects CASCADE; name S200; kind S16 default "part"; format_version Int NULL; head_seq Int default 0; created_by S32 (no FK); created_at; updated_at; deleted_at NULL | ix_documents_project_id |
| `ops` | document_id FK documents CASCADE; seq Int; author_id S32; client_id S128 NULL (`"<user id>.<app client id>"`); change_id S64 NULL; created_at; command S100 default ""; args JSON NULL; ops JSON NN; inverse_ops JSON NN; targets JSON NN; reverts Int NULL; reverted_by Int NULL; broke JSON NULL | PK(document_id, seq); UNIQUE `uq_op_change`(document_id, client_id, change_id); index `ix_ops_doc_seq`(document_id, seq) |
| `snapshots` | document_id FK documents CASCADE; seq Int; key Text (storage key); size Int default 0; created_at | PK(document_id, seq) |
| `versions` | id S32 PK; document_id FK documents CASCADE; seq Int; name S200; note Text default ""; author_id S32; created_at | UNIQUE `uq_version_name`(document_id, name); ix_versions_document_id |
| `assembly_refs` | assembly_id FK documents CASCADE; component_id S200; part_id S32 (no FK); seq Int | PK(assembly_id, component_id); ix_assembly_refs_part_id |

Notes:
- `alembic_version` is managed by Alembic.
- `S/migrate.py`:
  - the BASELINE `0001` is stamped when tables exist with no `alembic_version`
  - an unknown revision raises SchemaError ("written by a newer version")
  - with `auto_migrate` off and an older database, SchemaError tells the operator to run `migrate`
  - an empty database is always built
- After migrations, `hooks.prepare_db(sync_conn)` runs in a transaction (`S/db.py:232-241`).

Changed or kept per the spec:
- **`ops` changes:** MessagePack records with `parent_seq, branch_id, paths` (instead of `targets`), `inverse` kept, `depends/reads/undo_of` semantics, unique `(document_id, client_id, change_id)`.
- **`snapshots` change:** document plus last-writer index, zstd MessagePack, keyed `(document, epoch, seq)`.
- `documents.head_seq` is implied by the journal.
- **New** in the spec: folders (documents gain a parent folder), `room_leases`, `nodes`.
- **Carried over by the spec:** users, oauth_identities, sessions, oauth_states, login_codes, password_resets, orgs, org_members, invites, projects, project_members, documents, versions, assembly_refs.
- `meta` is obsolete.

---

## 5. WebSocket and room behaviour (`S/documents.py:352-427`, `S/hub.py`, `P/messages.py`)

### Handshake

1. `ws.accept()`, then the first text frame within **15 s**.
2. It must validate as `Hello`:
   - `type:"hello"`
   - `protocol:int=1`
   - `token:str`
   - `client_id:str 8..64` (any characters in v1)
   - `format_version:int|None`
   - `last_seq:int|None`
   - `app:str|None` (ignored)
   - extra keys ignored
3. Refusal sends `{"type":"error","code","message"}` then closes with `(code, message[:120])`:
   - timeout, invalid JSON or model, disconnect, or a binary frame (KeyError) → `bad_hello` 4400
   - `protocol != 1` → `protocol` 4426 `"this server speaks protocol 1; update openswx"`
   - token invalid → `unauthorized` 4401 `"sign in again"`
   - document missing or deleted, project missing, or role `None` → `not_found` 4404
   - join raising LookupError → 4404
   - RoomGone three times → `busy` 4408 `"try again"`

### Main loop

- `receive_text`. If `conn.closed` (the server dropped it), break.
- `last_seen = now` on **every** frame.
- `len(raw) > max_change_bytes` (characters) → `error too_large "send large data as blobs"`; the connection is kept. Uvicorn's `ws_max_size = max_change_bytes + 65536` (`S/__main__.py:99`).
- Invalid message → `error bad_message` with the pydantic text cut to 300.
- Dispatch:
  - presence: relayed only if `len(raw) ≤ 16384`, otherwise silently dropped
  - draft: relayed only if `len(raw) ≤ 131072`, otherwise silently dropped
  - change, lock.acquire, lock.release, ids.reserve, change.revert, rebuild.report
  - ping → `{"type":"pong","seq":head}`
  - a second hello parses and is ignored
- Exits on WebSocketDisconnect, RuntimeError or RoomGone; `finally: room.leave(conn)`. A binary frame after the hello raises KeyError, which is not caught here.

### Conn (`S/hub.py:87-178`)

- `client_id = f"{user.id}.{hello.client_id}"`, which prevents connecting as another person's client (`S/hub.py:272`).
- Writer task with a queue of maxsize **2000**.
- `max_buffer = 4*max_change_bytes` (32 MB by default).
- Back-pressure: the oldest queued frame may be any size. If the bytes queued behind it plus the new frame exceed `max_buffer`, or the queue is full → `close(4408,"too slow")`.
- `buffered` returns to 0 once frames are written.
- `send` of a message that cannot be JSON-encoded (NaN) → `close(4408,"resync")`. A broadcast of an unencodable message closes every recipient with 4408 `"resync"`.
- `close` marks closed, puts the sentinel (or cancels the writer if the queue is full), and in a task waits up to 5 s for the writer, then `ws.close(code, reason)`, then `room.leave(self)`.
- Presence token bucket: `allow(rate)`, starts full, capacity = rate, refill rate per second.

### Join (`S/hub.py:269-324`), under the room lock

- Evicted → RoomGone.
- Duplicate cid: the old connection is popped and closed **4409** `"connected again from the same client"`. Because it was popped first, its leave produces no `peer.left` and its locks are not detached.
- Read-only:
  - viewer → `"you have view access to this project"`
  - or the document's `format_version` is set and differs from `hello.format_version` → `"this file uses openswx file format X and this app uses Y; update openswx to edit it"`
- Colour: reuse the colour of the same user's other connection; otherwise `colour_for(user_id, taken by other users)`.
  - palette of 12: `#e6194b #3cb44b #4363d8 #f58231 #911eb4 #008080 #f032e6 #9a6324 #800000 #469990 #000075 #808000`
  - start index `int(sha256(user_id),16) % 12`, moving to the next free colour, falling back to the start (`S/security.py:74-85`)
- `conn.session = token_hash(hello.token)` for the periodic recheck.
- Locks held under this cid are re-attached (`detached_at=None`). If the client is now read-only they are released instead.

Welcome contents:
```
{type:"welcome", protocol:1, document_id, seq:head, format_version, you:peer, role,
 read_only_reason, peers:[peer of every other conn], locks:{target:holder}, broken,
 project_id, reserved:{ids:[sorted ids this cid holds], names:[...]}}
```
- Plus `changes` (a list of change events, possibly empty) **if** `last_seq` is not null and `0 ≤ last_seq ≤ head` and `head-last_seq ≤ catchup_limit` (500).
- Otherwise `content: state`, the whole document including `next_id` and `counters`.
- `peer = {client_id,user_id,name,colour,read_only,presence,draft}`.
- Then broadcast `peer.joined {peer}` to the others (the joiner is not yet in `conns`). If locks were re-attached or released: `lock.changed {acquired: mine_back, released}`. Then add the connection and clear `idle_since`.

Catch-up change events (`S/hub.py:344-363`) have the same shape as live events:
```
{type:"change",seq,author,author_name,client_id,change_id,time,ops,command,reverts}
```
No inverses, args or `broke`.

### Leave (326-342)

If the connection is still the registered one:
- remove it
- mark its locks detached (`detached_at=now`)
- `left_at[cid]=now` (reservations wait)
- broadcast `peer.left {client_id}`, then `lock.changed {acquired: detached holders, released:[]}`

Then `conn.close()` (1000). If there are no connections left, `idle_since=now`.

### Locks (365-428)

- Targets must match `^(f:.{1,200}|i:(components|mates):.{1,200}|k:.{1,64}|structure)$` (`S/hub.py:44`) and must exist (`f:`/`i:` items present; `k:` and `structure` always exist).
- `MAX_LOCKS=256` per client. A request is cut to its first 256 targets.
- Denied entries for read-only, an invalid target, a missing item, or a full quota carry the empty holder `{client_id:"",user_id:"",name:"",detached:false}`.
- Already held by self → granted. Held by another → denied with that holder.
- The requester gets `lock.result {request_id,granted,denied}`. Newly acquired locks are broadcast to everyone (including the requester) as `lock.changed {acquired,released:[]}`.
- Release: only the requester's own targets; broadcasts `lock.changed released`; no reply.
- Holder: `{client_id,user_id,name,detached}`.
- Expiry (`_expire_locks`, 366-389), run in acquire, `_commit`, restore and every sweep:
  - a detached lock dies after `lock_grace` (60 s)
  - an attached lock dies if its connection is gone or `now > conn.last_seen + lock_ttl` (45 s of silence)
  - dropped locks are broadcast in `lock.changed released`
  - reservations of a client that left are purged after `lock_grace`
- The `structure` lock blocks changes containing remove or order ops (`is_structural`).

### Id reservation (`ids.reserve`, 431-466; removed in v2)

- Read-only → `error read_only` with request_id.
- Limits: `features` 0..64, `names` ≤64 items. Per client ≤256 ids and ≤256 names (`MAX_RESERVED`). A stem must be 1..40 printable characters, not end in a digit, and have no surrounding whitespace. Failure → `error bad_reserve` with request_id.
- Ids are `f<n>` from `next_id`, skipping ids in use or reserved; `state.next_id` is advanced.
- Names are `prefix + (counters[prefix]+1…)`, skipping names in use or reserved.
- Reply: `ids.reserved {request_id,ids,names}`.

### Change commit (`_commit`, 499-657)

Checks, in order:
1. empty ops → `invalid`
2. `base_seq` outside `0..head` → `invalid`
3. `clean`, then `parse` → `invalid`; a non-assembly with components or mates ops → `invalid "only assemblies have components and mates"`
4. `_expire_locks`
5. **locked**: over the targets plus `structure` if the change is structural, any lock whose `client_id` differs (for `client_id=None`, i.e. restore, a lock by the same user does not block) → `locked "being edited by <names>"` with conflicts `{target: holder}`
6. **stale** (if `check_stale`): per target, skip added targets that do not currently exist; stale if `last_changed[t].seq > base_seq` and (`client_id` is None or the writer's client differs) → `stale` with conflicts `{target:{seq,client_id,user_id,name}}`. Own changes never conflict.
7. **id_taken**: an added feature id, or an added feature name, reserved by another client → `id_taken` with conflicts `{"f:<id>"|"name:<n>": owner cid}`
8. apply on `_writable_copy`; `absorb_ids` for added features; `inverse = diff(new, old)` → `invalid` on failure
9. assembly refs check (section 6)
10. blob check: blob refs in the ops not in `known_blobs`; a malformed sha counts as missing; storage `exists` with at most 16 lookups in parallel; a storage exception → `server_error`; found shas are cached → `missing_blob` with conflicts `{"blobs":[...]}`
11. store in one transaction:
    - Op row; `args` kept only if their JSON is ≤4096 bytes and has no NaN, else null; `command[:100]`; targets sorted
    - Document `head_seq` and `updated_at`
    - `reverted_by` on the reverted op
    - assembly_refs rewrite
    - any exception → `server_error "the server could not store the change; try again"`, with nothing changed in memory
12. publish:
    - `state` and `head`, `names`, `last_changed`
    - pop the used reservations
    - `recent[(cid,change_id)]=seq` (above 5000 entries, drop the oldest 2500)
    - broadcast the change event to everyone except the sender
    - snapshot if `head - snap_seq ≥ snapshot_every` (200); failure is logged and ignored

The broadcast is skipped for the sender, who gets `change.accepted {change_id,seq}`.

`Room.change` (659-686):
- idempotent resend: look up `recent`, then the DB `(doc, cid, change_id)`
- a DB error → `change.rejected server_error`
- found → `change.accepted` with the original seq; this check runs before the read-only check
- read-only → `change.rejected {code:"read_only",message:reason,conflicts:{}}`
- otherwise commit
- reply `change.accepted` or `change.rejected {change_id,code,message,conflicts}`

### Undo and revert (`change.revert`, 688-724)

- Message: `{request_id, seq, mode: "undo"|"revert"}`.
- Failures, in order:
  - read-only → `read_only`
  - DB error → `server_error`
  - no op → `not_found`
  - undo of someone else's change → `not_yours`
  - `reverted_by` set → `already_reverted`
  - **changed_since**: any of the op's targets with `last_changed.seq > msg.seq` by anyone, including self → `"cannot undo: <names> changed the same thing afterwards"` with a conflicts map
- Then commit `inverse_ops` with:
  - `base_seq=head`, `change_id=None`
  - `command="undo <cmd>"` or `"revert <cmd>"`
  - `reverts=seq`, `from_history=True`
  - lock checks still apply
- Reply `change.revert.result {request_id, ok:true, seq, ops: inverse}`; the sender gets no change event. Failure: `{ok:false,code,message,conflicts}`.
- Redo means reverting the undo change.

### Restore (726-745)

1. `state_at(seq)` (LookupError → 404).
2. `diff(current, target)`; empty → `{seq: head}`.
3. `_expire_locks`. Any lock held by **another user** → `locked "other people are editing this document; restore needs their locks released"`; your own locks do not block.
4. Commit with `client_id=None`, `check_stale=False` (staleness ignored), `from_history=True`, `command="restore to N"`.

### Rebuild reports (748-777)

- Ignored if the client is read-only, `msg.seq > head`, or `msg.seq ≤ last_report_seq`.
- `errors` are cut to 500 entries with values of ≤500 characters.
- `fresh` = errors not already in `broken`. `fixed` = entries of `broken` not in errors.
- If `fresh` is non-empty and a baseline exists, the Op at `msg.seq` is blamed:
  - `cause={by_seq,by_user,by_name,command,since_seq:baseline+1}`
  - `op.broke` is merged with `fresh`
- The first report only sets the baseline.
- `broken[f] = {...cause (only for fresh), error}`; fixed entries are removed.
- If anything is fresh or fixed: broadcast `rebuild.status {seq,broken}` to everyone.
- `broken` lives in memory only; it is sent in the welcome and lost on unload.

### Presence and draft (780-795)

- Rate check: `presence_rate` per second per client (default 60).
- A draft from a read-only client is dropped.
- A message that cannot be JSON-encoded is dropped and not stored.
- `conn.presence` / `conn.draft` are stored for later welcomes. `draft:null` clears.
- Broadcast to everyone except the sender: `{type:"presence"|"draft",client_id,data}`.

### Hub (798-916)

- `room()` uses a per-document opening gate; a missing or deleted document → LookupError.
- `Room.load`:
  - newest snapshot (missing snapshot or file → RuntimeError)
  - reads **all** ops' targets to build `last_changed`
  - applies the ops after the snapshot
  - `absorb_ids`
- `state_at(seq)`: the nearest snapshot at or before `seq` plus ops (each snapshot is unpacked in a thread).
- Sweeper every `sweep_interval` (5 s):
  - `room.sweep()` (lock expiry)
  - unload when there are no connections, no locks, and idle longer than `room_idle` (120 s): snapshot, `evicted=True`, pop
  - a room with detached locks stays loaded until they expire
  - `idle_since` is set at creation, so rooms loaded by REST also unload
  - one room's failure does not stop the others
- `recheck_access`, every `access_recheck` (30 s): for each connection, the session row exists and is unexpired, the user exists and is not disabled, the project exists, and `project_role == conn.role`. Otherwise close **4403** `"your access changed"`. An expired or deleted session also gets 4403.

Immediate drops:

| Trigger | Close |
|---|---|
| Password change (PATCH /me), reset confirm, account deletion (`drop_account`) | **4401** `"sign in again"` |
| Org role change, org member removal, project member set or remove (`drop_user(user, org)`, every room in the org) | **4403** `"your access changed"` |
| Document, project or org deletion (`close_document`: room popped and evicted, no snapshot) | **4404** with reason |
| Shutdown (`stop`: then each room snapshots under its lock) | **1001** `"the server is shutting down"` |

- `notify(document, msg)` broadcasts to a loaded room only.
- RoomGone handling: the WebSocket retries the join 3 times; REST restore retries once.

---

## 6. Assemblies (`S/assemblies.py`, `S/hub.py:559-582, 619-633`)

- A component is `{"id", ..., "ref": {"document": part_id, "seq": int ≥0 (not bool), "version": str|None}}`. A `"part"` key (an embedded copy) is refused (`_ref`, 29-40).
- `ref_rows(content)` maps component id → `(part_id, seq)`.

`check_refs(db, org, user, components, access_for=None|set, allow_deleted=False|True|set)` (48-78):
- The part must exist, be not deleted (unless allowed), be in the same org, and (for components in `access_for`, or all of them when it is None) the user must have a project role on its project.
- Then `kind == "part"`, else `"… is not a part"`.
- Then `seq ≤ head_seq`, else `"has no state N"` (or `"that state does not exist"` when access was not asked).
- OpError text names a part only to someone with access.

In `_commit`, for an assembly when any `i:components:` target is touched:
- `changed` = the new state's components among the targets.
- `chosen` = components whose `(document, seq)` changed (empty for undo or restore).
- `kept` = components that kept the same pin, which may point at a deleted part.
- `check_refs(access_for=chosen, allow_deleted=True if from_history else kept)` → `invalid`. Other exceptions → `server_error`.
- In the store transaction, unless `from_history`: `SELECT ... FOR UPDATE` on the wanted parts that are not deleted; a newly chosen part that was deleted → rollback and `invalid "a part this change uses has just been deleted"`.
- Then `DELETE` and re-`INSERT` every AssemblyRef row for the assembly.

Other assembly behaviour:
- Create (REST) checks refs with access for all components and inserts the rows.
- Parts reject component and mate ops.
- `used_in(part)`: non-deleted assemblies referencing it. `used_outside(project)`: assemblies outside the project referencing its parts. `visible`: splits documents into shown `{id,name}` and a hidden count.
- Deleting a part used by an assembly → 409. Deleting a project used from outside → 409 (section 1).
- `reference.update` fan-out happens on version creation only (section 1). Editing a part never changes an assembly.

---

## 7. Blobs and storage (`S/storage.py`, `S/documents.py:41-79`)

- Blob refs in content are `{"$blob": sha256, "len": n}`; a dict whose keys are a subset of `{$blob,len}` (`P/ops.py:417-428`). Strings of ≥16384 characters are externalised by clients (`BLOB_MIN`); the hash is of the UTF-8 bytes.
- Blob key: `orgs/{org_id}/blobs/{sha[:2]}/{sha}`. Snapshot key: `docs/{document_id}/snapshots/{seq:010d}.json.gz`, holding gzip level 6 of compact JSON (`S/hub.py:59-64`).
- Missing-blob checks: document create → 409 with blobs; change → `missing_blob` (with the `known_blobs` cache per room).
- Key validation: `^[A-Za-z0-9._\-/]+$`, no `..`, no leading `/` (11-17).
- **Local** (`LocalStorage`):
  - the root is resolved and created
  - put is atomic (temporary file in the same directory, then `os.replace`)
  - get returns None when missing
  - `exists` is `is_file`
  - delete uses `missing_ok`
  - `delete_prefix` is `rmtree`
  - all in threads
- **S3** (`S3Storage`):
  - URL form `s3://bucket/prefix`
  - optional endpoint
  - boto3 credentials from the environment
  - get: NoSuchKey → None
  - `exists`: `head_object`; **any** ClientError → False
  - `delete_prefix`: paginated `list_objects_v2`, then `delete_objects` in batches of 1000 (Quiet)
- `make_storage` picks S3 for URLs starting `s3://`, else a local folder.
- Blobs are never deleted except by org deletion. Snapshots are deleted only on org deletion (not on project or document deletion).

---

## 8. hooks.py (`S/hooks.py`)

`Hooks.name = "core"`. `load_hooks()` instantiates the first entry point in group `openswx_collab_server.hooks`, else `Hooks()` (43-46). Tests pass `hooks=` to `create_app`.

| Hook | Default | Called at |
|---|---|---|
| `may_add_member(db, org, current_members) -> (ok, reason)` | `(True,"")` | `S/orgs.py:58` (`accept_invite`, only if not already a member) and `S/orgs.py:195` (`create_invite`); refusal → 402 |
| `member_added(db, org, user)` | no-op | `S/orgs.py:62` |
| `member_removed(db, org, user_id)` | no-op | `S/orgs.py:157` (before commit); `S/lifecycle.py:84` (account deletion, per org, before commit); `S/lifecycle.py:149` (org deletion, per member, after commit) |
| `before_org_delete(db, org)` | no-op (raise HTTPException to refuse) | `S/lifecycle.py:130` |
| `prepare_db(sync_connection)` | no-op | `S/db.py:238-240` (after migrations, inside a transaction); `S/__main__.py:64-65` (CLI `migrate`) |
| `routers() -> list` | `[]` | `S/app.py:106` |
| `server_info() -> dict` | `{}` | `S/app.py:84` (merged into `/api/server`); `name` → `"edition"` |

---

## 9. Configuration (`S/config.py`) and CLI (`S/__main__.py`)

Environment: `OSWX_COLLAB_<FIELD UPPER>`. Booleans are true for `1/true/yes/on` (case-insensitive, stripped); ints and floats are parsed (`from_env`, 42-58).

| Setting | Default | Used in |
|---|---|---|
| database_url | `sqlite+aiosqlite:///./data/collab.db` | `make_engine` (app lifespan, CLI) |
| storage_url | `./data/storage` | `make_storage` (`S/app.py:67`) |
| s3_endpoint | `""` | `S3Storage` |
| public_url | `http://localhost:8787` | OAuth callback (`S/auth.py:196`), invite URL (`S/orgs.py:204`), reset URL (`S/auth.py:448`) |
| open_registration | False | `registration_mode`, `create_org` |
| session_days | 30 | `start_session` |
| auto_migrate | True | `init_db` / `migrate.upgrade(allow)` |
| snapshot_every | 200 | `_commit` (`S/hub.py:652`) |
| lock_ttl | 45.0 | `_expire_locks` |
| lock_grace | 60.0 | `_expire_locks` (locks and reservations) |
| sweep_interval | 5.0 | `_sweeper` |
| access_recheck | 30.0 | `_sweeper` |
| room_idle | 120.0 | `_sweep_room` |
| catchup_limit | 500 | `join` |
| max_change_bytes | 8 MiB | WebSocket frame limit (`S/documents.py:391`), `Conn.max_buffer` = 4×, uvicorn `ws_max_size` + 65536 |
| max_blob_bytes | 256 MiB | `put_blob` |
| presence_rate | 60 | `Room.presence` |
| smtp_host, smtp_port (587), smtp_security (`starttls`\|`ssl`\|`none`), smtp_user, smtp_password, smtp_from, smtp_from_name (`"openswx collaboration"`) | — | `Mailer` |
| reset_minutes | 60 | `request_reset` and the email text |
| google_client_id/secret, github_client_id/secret | `""` | `oauth_providers`, `_provider` |

CLI `openswx-collab-server`:
- `serve [--host 127.0.0.1] [--port 8787]`: uvicorn with `workers=1` and `ws_max_size=max_change_bytes+65536`.
- `migrate`: upgrade with `allow=True`, then `hooks.prepare_db`. Prints `"schema A -> B"` or `"schema B: up to date"`.
- `db-status`: prints `"database at X (or none…); this server needs HEAD"`. SchemaError → exit with its message.
- `create-admin <email> [name]`:
  - runs `init_db`
  - the password comes from `OSWX_COLLAB_PASSWORD` or getpass (≥8, else exit)
  - creates the user (`is_server_admin=True`, `email_verified=True`, name = argument or local part) or, if it exists, sets the password, promotes to admin and deletes all sessions
- `reset-password <email>`: exits `"no account …"` if missing; sets the password and deletes all sessions. It does not drop open sockets; the 30 s recheck closes them with 4403.
- Invalid email → exit `"not an email address"`.

---

## 10. Email (`S/mail.py`)

- Enabled iff `smtp_host` and `smtp_from` are set.
- Message:
  - `From` is `formataddr((from_name, from))`, or the bare address if there is no name
  - `Message-ID` uses the domain of `smtp_from`
  - plain text
- Transport: `ssl` uses `SMTP_SSL` with the default TLS context; otherwise `SMTP`, with `starttls()` if `starttls`, and `login()` if a user is set; timeout 20.
- `send` runs in a thread but is **awaited inside the request handler** (the docstring says after the response; the code does not). Returns True if the message was handed off. Any exception is logged (`"could not send email to the mail server %s"`) and returns False.
- Emails:
  1. **Invite** (`invite_text`, 60-65), triggered by POST invites when an email is given, `send_email` is true and mail is enabled. Subject: `"{inviter} invited you to {org} on openswx"`. The body has the URL, says the invite works once and expires in `{days}` days, and ignore-if-unexpected text. The result is the `emailed` flag in the response.
  2. **Password reset** (`reset_text`, 68-73), triggered by POST password-reset when the limiter allows and the account exists and is enabled. Subject: `"Reset your openswx collaboration password"`. The body has the link, the `{minutes}` validity, and says setting a password signs you out everywhere. Failure does not change the 202.
- Without mail: reset → 501; invites → `emailed:false`; `/api/server` `email:false`.

---

## 11. Test suite (`tests/`)

Helpers in `tests/conftest.py`:
- `PART` fixture document (format_version 3, `next_id` 3, counters, f1 Sketch1, f2 Boss-Extrude1)
- settings fixture (SQLite in a temporary directory, or `OSWX_TEST_DATABASE_URL` for PostgreSQL with the schema dropped)
- `make_client(**overrides)` for restarts
- `World` (admin Ada, org Robotics, project Rover, `invite`, `document`)
- `Live` WebSocket client (`send`, `recv`, `drain` via ping/pong, `change`, `lock`)
- `set_depth`, `add_feature`

**test_accounts.py:**
- test_first_account_is_admin_then_invite_only
- test_login_logout_and_password_rules
- test_login_is_rate_limited
- test_invite_joins_org_and_project_once
- test_existing_account_accepts_a_link_invite
- test_project_roles_gate_everything
- test_members_roles_and_last_owner
- test_seat_hook_can_refuse
- test_open_registration
- test_oauth_sign_in
- test_oauth_only_redirects_to_this_machine
- test_oauth_does_not_join_an_account_nobody_verified
- test_an_account_with_only_a_provider_cannot_unlink_it

**test_assemblies.py:**
- test_components_and_mates_diff_and_apply_like_features
- test_an_assembly_pins_parts_and_checks_what_it_points_at
- test_editing_a_part_never_changes_an_assembly_until_someone_updates_it
- test_components_mates_and_positions_are_edited_live
- test_where_a_part_is_used_and_why_it_cannot_be_deleted
- test_references_respect_projects_and_organisations
- test_an_assembly_survives_a_restart_with_its_references
- test_parts_have_no_components
- test_working_in_an_assembly_whose_parts_you_cannot_open
- test_a_project_cannot_be_deleted_from_under_other_projects_assemblies
- test_the_delete_guard_does_not_name_assemblies_you_cannot_open
- test_undo_and_restore_still_work_after_a_part_they_bring_back_was_deleted
- test_a_delete_locks_before_it_looks_for_users

**test_client_feedback.py:**
- test_a_refused_reservation_says_which_request_it_answers
- test_welcome_says_what_this_client_still_holds_and_where_blobs_are
- test_your_own_locks_do_not_stop_your_restore

**test_convergence.py:**
- test_random_concurrent_editing_converges (parametrized; uses the `Sim` class)

**test_email.py:**
- test_without_smtp_nothing_is_sent_and_resets_say_so
- test_an_invite_is_emailed
- test_a_forgotten_password
- test_only_the_newest_link_works_and_requests_are_limited
- test_a_reset_makes_the_address_count_as_confirmed
- test_a_broken_mail_server_does_not_break_the_request

**test_hardening.py:**
- test_huge_or_pathological_names_cannot_break_a_document
- test_a_feature_stays_a_feature_whatever_is_patched
- test_nesting_is_bounded
- test_one_and_true_and_one_point_zero_are_different
- test_nobody_can_connect_as_someone_elses_client
- test_viewers_cannot_blame_or_clear_rebuild_errors
- test_a_changed_organisation_role_reaches_open_documents
- test_reservations_and_locks_are_bounded_and_released
- test_a_lock_that_expires_during_someone_elses_edit_is_announced
- test_a_reader_that_never_reads_is_dropped_before_it_costs_memory
- test_eviction_cannot_leave_two_rooms_for_one_document
- test_apply_checks_are_in_the_shared_library_too
- test_reserved_names_that_are_never_used_leave_nothing_behind
- test_names_can_be_swapped_or_reused_within_one_change_and_restored
- test_nan_and_infinity_never_enter_a_document
- test_minus_zero_is_a_change
- test_changing_the_password_closes_open_documents
- test_open_connections_are_checked_against_sessions_and_roles
- test_a_full_queue_still_closes_the_socket
- test_extra_keys_on_an_operation_never_reach_the_history_or_other_clients
- test_replacing_a_feature_under_its_own_id_is_still_a_conflict
- test_reserved_names_wait_for_a_client_that_dropped
- test_a_change_that_cannot_be_stored_is_refused_and_nothing_moves
- test_one_rooms_failing_snapshot_does_not_stop_the_sweep
- test_storage_trouble_never_drops_the_connection_or_half_applies
- test_two_people_cannot_both_use_one_invite

**test_history.py:**
- test_undo_takes_back_only_your_own_untouched_change
- test_undoing_an_add_removes_it_and_respects_locks
- test_editors_can_revert_someone_elses_change
- test_rebuild_reports_pin_the_break_on_the_change_that_caused_it
- test_history_pages_and_content_at_any_point
- test_named_versions_and_restore
- test_snapshots_and_a_restart_reproduce_every_state
- test_idle_documents_are_unloaded_and_come_back
- test_deleting_a_document_ends_the_session
- test_document_kinds_and_content_are_checked

**test_lifecycle.py:**
- test_deleting_an_account_keeps_the_work_and_frees_the_address
- test_an_account_cannot_leave_an_organisation_or_the_server_without_an_owner
- test_the_last_person_on_a_server_can_leave
- test_your_own_data_export
- test_an_organisation_export_has_everything_in_it
- test_deleting_an_organisation_removes_everything_in_it
- test_an_extension_can_refuse_deleting_an_organisation

**test_live.py:**
- test_join_change_and_broadcast
- test_who_may_connect
- test_viewers_and_other_file_formats_are_read_only
- test_locks_block_others_until_released
- test_structure_lock_stops_deletes_and_reorders_only
- test_stale_change_is_rejected_but_unrelated_ones_merge
- test_own_changes_in_a_row_do_not_conflict_with_each_other
- test_invalid_changes_leave_the_document_alone
- test_ids_and_names_come_from_the_server
- test_resending_a_change_is_harmless
- test_reconnect_catches_up_and_keeps_locks
- test_far_behind_or_unknown_position_gets_the_whole_document
- test_locks_expire_after_the_grace_period_and_when_a_client_goes_silent
- test_presence_and_drafts_are_relayed_not_stored
- test_presence_flood_is_dropped
- test_losing_access_disconnects
- test_blobs_must_be_uploaded_before_they_are_referenced

**test_migrations.py:**
- test_the_migrations_build_exactly_the_tables_the_code_uses
- test_a_database_made_before_migrations_is_adopted_with_its_data
- test_with_automatic_upgrades_off_an_older_database_is_refused
- test_a_database_from_a_newer_server_is_refused
- test_an_empty_database_is_built_by_the_migrations
- test_extensions_set_up_their_own_tables_after_the_migrations
- test_command_line

**test_online.py:**
- test_a_member_sees_who_is_in_an_open_document
- test_someone_who_left_is_not_listed
- test_a_closed_or_evicted_room_lists_no_one
- test_an_evicted_room_still_in_hand_lists_no_one
- test_the_list_is_capped_and_the_count_is_not
- test_someone_outside_the_project_gets_404_not_the_list

**test_ops.py** (Python protocol library):
- test_no_change_is_no_ops
- test_param_edit_sends_only_the_changed_value
- test_sketch_entity_added_does_not_resend_the_sketch
- test_add_remove_reorder_and_top_level_keys
- test_local_and_server_owned_keys_never_travel
- test_apply_refuses_what_does_not_fit
- test_random_edits_round_trip
- test_dict_diff_and_patch
- test_long_strings_become_blobs_and_back
- test_messages_parse_by_type

**test_performance.py:**
- test_a_broadcast_is_encoded_once_for_everyone
- test_a_draft_that_is_not_json_is_neither_relayed_nor_kept
- test_one_big_frame_is_not_too_slow_but_a_backlog_behind_it_is
- test_a_big_frame_behind_others_is_still_too_slow
- test_the_buffer_count_returns_to_zero_once_written
- test_snapshots_are_packed_off_the_event_loop_from_a_copy
- test_content_now_is_not_copied_and_reads_the_same
- test_catch_up_carries_the_same_change_events_as_live
- test_blob_checks_run_together_and_are_remembered
- test_the_organisation_list_counts_members_in_one_query

**test_project.py** (repository hygiene):
- test_one_version_everywhere
- test_issue_forms_only_use_labels_that_exist
- test_everything_here_is_apache_and_the_public_files_exist
- test_the_protocol_package_depends_on_nothing_but_pydantic
- test_the_release_notes_come_from_the_changelog

**test_storage.py:**
- test_local_folder
- test_s3_bucket_with_and_without_a_prefix
- test_the_whole_server_on_s3
- test_removing_everything_under_a_prefix

Specific assertions worth porting:
- OAuth loopback rejects: `https://evil.example/cb`, `http://evil.example:80/cb`, `http://127.0.0.1/cb` (no port), `http://user@127.0.0.1:5000/cb`, `myapp://cb`.
- A missing `code_challenge` → 422.
- Locks: a request of 300 targets → 256 granted, 0 denied; the next one granted none.
- Reserve stems such as `3DSketch`, `Helix/Spiral`, `Skizze-Ä` are fine.
- NaN in `args` is accepted; `args` are stored as null.
- Restore after a name swap returns `{"seq":4}`.

---

## 12. deploy/ and tools/

**deploy/:**
- `deploy/Dockerfile`:
  - `python:3.15-slim`; installs `/app/protocol` and `/app/server[postgres,s3]`
  - non-root user `collab`; `/data` volume
  - `OSWX_COLLAB_DATABASE_URL=sqlite+aiosqlite:////data/collab.db`, `OSWX_COLLAB_STORAGE_URL=/data/storage`
  - EXPOSE 8787; HEALTHCHECK every 30 s against `GET http://127.0.0.1:8787/api/health`
  - `CMD openswx-collab-server serve --host 0.0.0.0 --port 8787` (one process)
  - published as `ghcr.io/dukeofcheese/openswx-collab-server` (amd64, arm64)
- `deploy/docker-compose.yml`:
  - `server` runs the published image (`OSWX_COLLAB_VERSION`, default latest)
  - depends on `db` (`postgres:16`, `pg_isready` healthcheck)
  - environment: `DATABASE_URL=postgresql+asyncpg://collab:${POSTGRES_PASSWORD}@db/collab`, `STORAGE_URL=/data/storage`, `PUBLIC_URL` (required), `OPEN_REGISTRATION`, `SMTP_*` (no `SMTP_FROM_NAME`), Google/GitHub ids
  - volumes `storage:/data` and `db`
  - port `127.0.0.1:${HOST_PORT:-8787}:8787`; a TLS proxy is expected in front
- `deploy/docker-compose.build.yml`: overrides the image with a build from context `..`, Dockerfile `deploy/Dockerfile`.
- `deploy/docker-compose.s3.yml`:
  - adds Garage `dxflrs/garage:v2.5.0` (single node, default bucket)
  - server environment: `STORAGE_URL=s3://${S3_BUCKET:-openswx-collab}`, `S3_ENDPOINT=http://garage:3900`, `AWS_ACCESS_KEY_ID`/`SECRET` from `S3_ACCESS_KEY`/`S3_SECRET_KEY`, `AWS_DEFAULT_REGION=garage`
  - Garage environment: RPC secret, default key and bucket
  - volumes `garage-meta` and `garage-data`; no published ports
- `deploy/garage.toml`: metadata and data directories, sqlite db engine, replication 1, RPC `[::]:3901`, S3 API `[::]:3900` region `garage`, root domain `.s3.garage.localhost`.
- `deploy/.env.example`: `OSWX_COLLAB_VERSION`, `PUBLIC_URL`, `POSTGRES_PASSWORD`, `OPEN_REGISTRATION`, `HOST_PORT`, `SMTP_*`, Google/GitHub ids, `S3_BUCKET`/`S3_ACCESS_KEY`/`S3_SECRET_KEY`/`GARAGE_RPC_SECRET` (with openssl recipes to make them).

**tools/:**
- `tools/check_deploy.py`:
  - static checks: every `OSWX_COLLAB_*` variable in the Dockerfile and Compose files is a real Settings field; `.env.example` offers every `${VAR}` the Compose files read; the build context and every COPY source exist; Compose uses the ghcr image; the named volumes are declared; the image CMD is a console script
  - then installs the server with the Dockerfile's pip line into a fresh venv
  - runs it on PostgreSQL (pixeltable-pgserver) plus S3 (moto) with the Compose environment, and runs the HEALTHCHECK command
  - checks `/api/server` (`registration first_user`, `oauth []` when ids are empty)
  - register → create org → project → document → WebSocket hello and change (expects `{"type":"change.accepted","change_id":"c1","seq":1}`)
  - restarts the server: content seq 1 survives, seq 0 is unchanged (`10.0` still a float), snapshot keys are in the bucket, and the DB has `ops=1` and `users=1`
- `tools/e2e_two_peers.py`: starts the server (or uses `OSWX_E2E_SERVER`), runs two real openswx processes (`tools/e2e_peer.py`) editing in turn, concurrently on different features, and concurrently on the same feature. It compares fingerprints, document, names, faces, volume and features, and expects exactly one refusal on the conflict.
- `tools/e2e_peer.py`: a minimal protocol peer inside an openswx session. It keeps a `confirmed` copy and reloads it on rejection.
- `tools/test_postgres.py`: starts a throwaway PostgreSQL and runs pytest with `OSWX_TEST_DATABASE_URL`.
- `tools/release_notes.py`: prints the `CHANGELOG.md` section for a version plus the "Run it" boilerplate (docker pull, compose, pip, an upgrade note, the non-affiliation line).

---

## Code facts that differ from the docs, or edge cases worth pinning

- Mail is awaited inside the handler despite `S/mail.py`'s docstring.
- Project and document deletion leave snapshot files in storage. Org deletion removes them.
- `member_removed` after org deletion runs after commit and is never committed.
- S3 `exists` turns any ClientError (outages included) into "missing", which gives `missing_blob`, not `server_error`.
- An expired session found by the access recheck closes 4403, not 4401.
- `drop_user` drops the user from every document in the org, not only the affected project.
- Expired sessions and login codes are never purged.
- A binary frame after the hello is uncaught (KeyError ends the socket).
- `format_version` uses `isinstance(version,int)`, so a JSON `true` would be accepted.
- `registration_mode` counts deleted and disabled accounts.
- A room with detached locks is not unloaded until they expire.
- `broken` and rebuild baselines are in memory only.
- `undo` refuses `changed_since` even when the later change was your own.
