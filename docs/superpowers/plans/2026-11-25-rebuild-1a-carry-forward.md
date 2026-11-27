# Rebuild 1a: what the next plans must pick up

Plan 1a (`2026-11-25-rebuild-1a-baseline-and-core.md`) is complete. Reviews left these items for the
server plan (1b), the import/docs plan (1c) and the client project. Nothing here blocks 1a.

## For the server (1b)

- **Undo of a replaced subtree.** Undoing a Set or Unset that dropped a subtree holding other clients' or
  legacy ids is refused (`ForeignId`), because no removal is remembered for those ids. Rule to implement:
  when `undo_of` names an accepted change of the same client, `decide` receives that change's kept
  inverse from the journal and exempts ids found under the same path in it.
- **Refuse a base below the index floor before calling `Index::check`** (already done inside `decide`;
  keep it that way if the server ever calls `check` directly).
- **Unknown dependencies.** `decide`'s `refused` callback treats an unknown change id as not refused.
  Give the server a three-state answer (accepted, refused, unknown) and refuse unknown.
- **Cost bounds.** `Index::check` can cost about ops × size of the index subtree before `apply` refuses;
  bound work per change. Cap concurrent large decodes: at the 1,000,000-value frame limit `rmpv` builds
  roughly 50–100 MB transiently.
- **Journal needs.** `Accepted` does not carry the change's writes, removals and reads; the journal's
  `paths` column needs them. `Value::to_wire` builds a whole tree per welcome; add a pre-encoded
  snapshot path.
- **Logging.** `ClientMsg`'s `Debug` redacts the token; keep every other log line free of tokens too.
- **Index snapshots.** `Index::from_wire` does not limit segment length or refuse `~` segments; harden
  it where snapshots are read.
- **Derived keys.** A `~` write inside an item someone else removed comes back `Invalid`, not a
  conflict; `~` also matches item ids that begin with it. Decide and pin.
- **Position cap.** `Pos::between` panics on equal bounds and when the result would exceed 1024
  characters; only `try_between` may be called on client-controlled positions.
- **PostgreSQL baseline.** Not recorded in 1a (the only host was reachable over a network link that would
  have dominated the numbers). Record one like-for-like when the Rust server has its PostgreSQL store.

## For docs and import (1c)

`docs/protocol.md` still describes v1. The v2 text must state:

- `hello.client_id` is 8–64 characters of `[A-Za-z0-9_-]`.
- A client frame may hold at most 1,000,000 values; frames over 512 KB must be `change`; larger data
  goes in blobs.
- A change whose `base_seq` is ahead of the server is refused `invalid`; below the floor, `too_old`.
- Keyed lists travel as `{"$list": [[pos, item], ...]}`; a plain list of maps with distinct ids is
  refused; `$list` is never a key, path segment or id.
- The `depends` rule: a change lists its sender's own unanswered changes whose writes or removals
  overlap its writes, removals or reads.
- Ids: `<user>.<client>.<counter>` for anything new; an id may be put back by anyone while its removal
  is remembered; an item moved to another list by Remove and Insert needs new ids at every depth; ids
  already under a Set's path may be rearranged within that Set.
- Reject codes: `stale`, `locked`, `dependency`, `name_taken`, `id_taken`, `invalid`, `missing_blob`,
  `read_only`, `too_old`, `server_error`.
- Presence and drafts (from 1b Task 19b): the server relays them at most every 50 ms per document,
  each peer's latest only. A lone entry comes as `presence` / `draft`; several come as
  `presence.batch {presence: {client_id: data}, drafts: {client_id: data | nil}}` (2 to 256 entries,
  each client at most once per kind; apply each entry as the message it stands for). A client never
  gets its own back.
- `peer.joined`, `peer.left`, `presence`, `draft` and `presence.batch` may be dropped for a client
  that does not read fast enough (it is not closed for them). Once it has read everything the
  server had for it, it is sent `peer.list {peers: [peer]}` (at most once a second), which
  replaces what it knew of the others:
  - it names **every** peer as it is now, **the recipient included** (one list is shared by every
    client caught up at once): skip the entry whose `client_id` is your own;
  - it carries each peer's presence but **no drafts** (`draft` is absent): forget the drafts you
    had; each peer's current draft follows, as a `draft` message, after the list.
  At most 65,536 peers, each `client_id` once; a `presence.batch` client id is at most 129
  characters (user id, a dot, the hello's client id).
- Changes, answers and lock messages are never dropped; a client too slow for those is closed with
  4408. So is one that stays behind on the soft messages for 30 s while it reads nothing at all.
- Until a `peer.list` arrives, a client that fell behind may get `lock.changed` / `lock.result`
  frames naming a `client_id` it was never told about (a `peer.joined` it missed) or one that has
  left (a `peer.left` it missed): show the lock anyway, with the name the lock carries, and do not
  expect the peer to be in your list.
- Presence and drafts can arrive up to a tick (50 ms) after a later message of the same peer that
  is never delayed: a draft cleared just after its change was committed may come after that
  change, and so may a draft the peer sent just before it. When a peer's change arrives
  (`change`), clear that peer's draft yourself; a draft that still comes within the tick is applied
  as it says, and the peer's own clear follows it.

`CHANGELOG.md` needs the same list when v2 ships. The import tool must give legacy documents with an
empty-string key or id a valid name (no client operation can address `""`).

## For the client (project 2)

- Expose `try_between`, never `between`, from the wheel.
- The simulated client in `crates/core/tests/common/client.rs` is the reference for pending-queue,
  dependency and rollback behaviour. Automatic renaming on `name_taken` is not in it yet.
- Wire vectors in `crates/core/vectors/` are to be read by the wheel's tests.

## Small test and tidy items

- `KeyedList::iter` looks each item up; store the value alongside the order entry.
- Property test for `Pos` midpoints over arbitrary well-formed bounds.
- A distinct-positions branch in the diff reorder property, where the Move bound is exact.
- Boundary test for `MAX_NESTING` at the scan; generate 16- and 32-bit container and string formats.
- `path::from_wire` and `op::item_id` restate the length rule instead of calling `path::check`.
- Loadgen reads its commit from the working directory at run time, with no dirty flag.
- `bench/results/README.md` has a few over-long lines and one sentence about the timing of two memory
  readings that the files do not show.
