# The Extensions seam: gaps before the paid edition (planning note for project 4)

Written 2026-11-27, from the final whole-branch review of plan 1b (reviewer B), for the plan of project 4.
Nothing here is built yet; it lists what the seam in `crates/server/src/ext.rs` lacks for the paid
edition, so that project 4 starts from a known list rather than finding these one at a time.

## What the paid edition is

Decided with the user on 2026-11-27:

- **Cloud plus licensed self-host**: the same paid crate runs on our servers and on a customer's, under a
  licence.
- **The first paid release** has: SSO (OIDC and SAML) with SCIM provisioning; an audit log and an admin
  console; Stripe billing with seats; and shared part libraries (a free basic version, paid extras).
- The order of the projects is unchanged: project 2 (the client on protocol 2), then 3, then 4.

The paid crate depends on the server library and calls `oswx_serverd::run(ext)` with its own
`Extensions`. Today that seam gives it: refusing hooks asked before a transaction (`may_add_member`,
`quota`, `before_org_delete`), notifying hooks after the commit (`member_added`, `member_removed`,
`on_event`), extra routes (`routes`), its own migrations for each database (`migrations`, `(id, sql)`
pairs), `server_info`, and `hook_timeout`. A refusal answers 402 with the hook's text; a hook that does
not answer in time answers 409.

## The gaps

Each gap says what is missing, what in the paid release needs it, and a first idea of the shape. The
shapes are suggestions for the plan, not decisions.

### 1. Database access for the extension's own tables

The extension can create tables (`migrations`) but has no way to use them: `Store` is a trait of the
server's own queries, the pools are private, and `AppState` holds only `Arc<dyn Store>`. Billing (Stripe
customers, subscriptions, seat counts), SSO (connections, SAML certificates), SCIM (external ids), the
audit log and the part libraries all need tables of their own.

Shape: a narrow handle on the database for extensions (a pool, or a `begin()` that returns a transaction
the extension can run its own SQL in, on either dialect), plus a way to join the server's transaction
where a write must be atomic with the server's (e.g. a member and its seat).

### 2. Sign-in and registration hooks (SSO enforcement)

There is no hook on password sign-in, registration, password reset or provider sign-in. An organisation
that requires SSO must be able to refuse a password sign-in or registration for its domain, and to send
the person to its identity provider instead.

Shape: refusing hooks `before_sign_in(email, method)`, `before_register(email, invite)` and
`before_password_reset(email)`, asked before the transaction like the others, with a refusal that can carry
a URL (see gap 5).

### 3. A public way to issue a session, and a sign-in method plug-in

`start_session` and the login-code machinery in `routes/oauth.rs` are crate-private, and the provider list
is a closed enum (`Provider`: Google, GitHub). OIDC (per organisation, any issuer) and SAML need to create
or find the account, issue a session or a login code, and hand back to the app through the same loopback
flow as the built-in providers.

Shape: a public `sessions` API (issue a session or a login code for an account, with the same rules as the
built-in flows), and either a provider trait the extension can implement or a documented route pattern
that ends in the existing `exchange`.

### 4. A request filter, and a handshake hook, for licence and suspension

There is no layer an extension can put in front of every route, and no hook in the WebSocket handshake. A
suspended organisation (unpaid, or over its licence) must be refused everywhere, live editing included,
and a self-hosted server without a valid licence must fall back to the free edition's limits.

Shape: `fn layer(&self) -> Option<tower::Layer>` (or a `before_request(org, user, route)` refusing hook)
and `before_join(user, document, project, role)` in the handshake, with refusals mapped to a documented
status and close code.

### 5. A richer refusal

A refusal is `Result<(), String>` and always answers 402 with that text. The paid release needs other
statuses (403 for SSO required, 423 or 402 for a suspended organisation), a machine-readable code the
client can branch on, and a URL (where to sign in, where to pay).

Shape: `Refusal { status, code, message, url }`, with `From<String>` keeping today's hooks working.
(Plan 1b ruled that refusals stay 402 until the paid crate needs more; this is that need.)

### 6. The actor and the target in hooks

The refusing and notifying hooks get ids only, and not always who is acting: `may_add_member(org,
count)` does not say who is being added or by whom; `quota(org, what)` does not say who asks. Seats
billed per person, SSO-only domains and the audit log all need the actor (account, session, client
address) and the target.

Shape: a `Context { actor: Option<Id>, session: Option<…>, address: Option<IpAddr> }` passed to every
hook, and the target's id where there is one (the invitee's email, the member being added).

### 7. More hook points

Organisation creation, project membership and the admin commands have no hooks: a plan that limits
organisations per account, project members, or that must record a server admin's `create-admin` or
`reset-password`, cannot.

Shape: `before_org_create(user)`, `before_project_member(project, user, role)`, and events (or hooks) from
the admin commands in `serverd`.

### 8. A lossless audit outbox, with context, and the missing events

`on_event` is a notification after the commit, with a timeout: an event is lost when the hook is slow or
panics, and when the process stops between the commit and the call. An audit log must not lose events. The
events carry no context (who, from where, which session), and some actions have none: failed sign-ins,
sessions ended by an admin or by expiry, account disabled, server-admin changes, blob uploads, exports
started and refused, invites not emailed, the admin commands.

Shape: an outbox table written in the same transaction as the change (the server's `commit_then` is the
place), drained to the extension at least once with an id it can de-duplicate on; `Event` gains a
`Context` (gap 6) and the missing variants.

### 9. A service API (SCIM and the admin console)

SCIM and an admin console must act on accounts and memberships: create a user, deactivate or delete one,
add and remove members, set roles, list accounts and organisations with their usage. Today these exist
only as route handlers (with their checks, transactions, revocations and events inside them), so an
extension would have to copy them or call the HTTP API on itself.

Shape: a `Service` with `create_user`, `revoke_account` (disable, end sessions, close connections),
`remove_member`, `set_role`, and listing and usage queries, each doing what the matching route does
(checks, the organisation's turn, `commit_then`, events); the routes then call it too.

### 10. A storage quota, and a sweeper for blobs nobody uses

Plans limit storage. The server keeps no count of an organisation's stored bytes, and blobs that no
document refers to any more are never removed (a deleted document's blobs stay until its organisation
goes; plan 1c has the sweeper). A quota on stored bytes needs both a count and the sweeper, or customers
pay for storage they cannot free.

Partly closed in this wave (see below): `Quota::BlobBytes` is now asked before a new blob is stored.

Shape: a stored-bytes count per organisation kept by the upload path and the purger, exposed to the
extension; the unreferenced-blob sweeper of plan 1c.

### 11. Licence checks while running

A licence is checked, at best, when the extension starts. A licence that expires, or is revoked, while the
server runs is never noticed until a restart.

Shape: a periodic hook (`tick` every few minutes) the extension can use to recheck and switch the server's
behaviour (through gap 4's filter), and a way to show the state in `/api/server` (already possible with
`server_info`, if it is read live).

### 12. Body limits for extension routes

Extension routes go through the server's body limit for most routes (1 MiB), with no way to ask for
another. SAML responses, SCIM bulk requests and part-library uploads can need more, and a few routes would
want less.

Shape: `routes()` returns routes with a per-route body limit (or the extension applies its own
`DefaultBodyLimit` and the server's layer leaves extension routes alone), documented.

## What this wave (final fix wave 2) closed

- **Gap 10, in part**: `Quota::BlobBytes(size)` is asked before a blob that is not stored yet is uploaded
  (its declared size, or `MAX_BLOB_BYTES` when it declares none), and a refusal answers 402 before any
  upload place is taken. The count of stored bytes and the sweeper are still missing.
- **Quota::Members** is now asked (with the seat hook) when an invite is made and when one is used, so a
  plan can limit members through `quota` as well as `may_add_member`. This does not close gap 6 (the hooks
  still do not say who is added, or by whom).
- The `ext.rs` module documentation now lists every place `quota` is asked (projects, documents, members,
  blob bytes).

Everything else above is open.
