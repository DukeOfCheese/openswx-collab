# Rebuild 1a: Baseline Load Generator and Core Crate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Cargo workspace containing a load generator that records how the current Python server
performs, and `crates/core`: the document model, operations, conflict index and protocol v2 codec that
the Rust server and the openswx wheel will both use.

**Architecture:** `core` is pure (no async, no I/O). Documents are persistent trees (`imbl`), so applying
a change shares everything it does not touch. Keyed lists carry a fractional position per item. The
conflict index is a path trie of last writers and last readers. `loadgen` drives a server through a
`Dialect` trait; this plan implements the v1 JSON dialect only.

**Tech Stack:** Rust stable (1.99), `imbl`, `serde`, `rmp-serde`, `rmpv`, `thiserror`, `proptest`;
loadgen: `tokio`, `tokio-tungstenite`, `reqwest`, `clap`, `hdrhistogram`, `serde_json`.

**Spec:** `docs/superpowers/specs/2026-11-25-collab-rebuild-design.md` (the "Amendments after review"
section overrides earlier text).

This is the first of three plans for project 1. 1b is the server (`crates/server`, `crates/serverd`);
1c is import, deploy files, docs and removal of the Python server. The Python code is not touched here.

## Global Constraints

- Nothing in `crates/core` may depend on tokio, a database, the filesystem or the network.
- The server has no geometry kernel; `core` treats documents as data and knows nothing about openswx
  feature types.
- Limits, copied from the spec: nesting depth 64; ids, names and types at most 200 characters; one change
  at most 8 MB; NaN and infinity refused everywhere; `1`, `1.0` and `true` are different values.
- New ids at any depth have the form `<user>.<client>.<counter>`; anything else is a legacy id, legal
  only in initial content.
- Keys never written by clients (top level): `rollback`, `active_config`, `display_state`, `journal`,
  `name`, `next_id`, `counters`.
- Map keys beginning `~` are derived data: synced, never a conflict.
- No Claude attribution in commits or pull requests. `main` takes pull requests only; work on a branch.
- `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings` pass at every commit.

## Review Focus

1. A change stacked on a refused change of the same client must be refused too (`depends`), or the server
   ends up in a state nobody saw. Pinned in Task 7.
2. Two clients inserting between the same neighbours get the same position; order must still be
   identical everywhere (tie broken by id). Pinned in Task 3.
3. A document from the Python server (plain lists, legacy ids `f1`, no positions) must load, and edits to
   its existing items must work. Pinned in Tasks 2 and 5.
4. An edit inside an item that someone else removed must be a conflict naming the remover, not `invalid`.
   Pinned in Task 6.
5. Hostile values: depth 65, a 201-character id, NaN, a duplicate id inside an inserted item, a `set` on
   a protected key, an insert carrying someone else's id prefix. Pinned in Tasks 4 and 5.

---

## File structure

```
Cargo.toml                      workspace: members crates/*, shared lints, resolver 3
rust-toolchain.toml             channel = "stable"
crates/core/Cargo.toml
crates/core/src/lib.rs          re-exports
crates/core/src/value.rs        Value, Map, KeyedList, Plain conversion
crates/core/src/pos.rs          Pos (fractional index)
crates/core/src/path.rs         Path
crates/core/src/op.rs           Op, Change, limits and cleaning
crates/core/src/apply.rs        apply, Applied, Rules, OpError
crates/core/src/diff.rs         diff
crates/core/src/index.rs        Index (last writers/readers), Conflict
crates/core/src/wire.rs         protocol v2 messages and codec
crates/core/tests/              one file per module, plus convergence.rs and vectors.rs
crates/core/vectors/*.json      wire test vectors (hex in, JSON meaning)
crates/loadgen/Cargo.toml
crates/loadgen/src/main.rs      CLI
crates/loadgen/src/dialect.rs   trait Dialect
crates/loadgen/src/v1.rs        v1 JSON dialect (Python server)
crates/loadgen/src/scenario.rs  scenarios and report
bench/README.md                 how to run; machine description
bench/results/                  recorded JSON reports
.github/workflows/rust.yml      fmt, clippy, test on ubuntu, windows, macos
```

---

### Task 1: Workspace and CI

**Files:** Create `Cargo.toml`, `rust-toolchain.toml`, `crates/core/Cargo.toml`,
`crates/core/src/lib.rs`, `.github/workflows/rust.yml`; modify `.gitignore` (add `target/`).

**Interfaces:** Produces the workspace; crate name `oswx-core` (lib name `oswx_core`), edition 2024,
licence field copied from `server/pyproject.toml`.

- [ ] **Step 1:** Write the workspace manifest with `[workspace.lints.rust] unsafe_code = "forbid"` and
  `[workspace.lints.clippy] all = "warn"`; `crates/core` with an empty `lib.rs` and one test
  `#[test] fn builds() {}`.
- [ ] **Step 2:** Run `cargo test --workspace`. Expected: 1 passed.
- [ ] **Step 3:** Add `rust.yml`: on pull_request and push to main; matrix ubuntu-latest,
  windows-latest, macos-latest; steps `cargo fmt --all --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`. The existing
  `test.yml` is left as it is.
- [ ] **Step 4:** Commit: `Rust workspace and CI`.

---

### Task 2: Values and plain conversion

**Files:** Create `crates/core/src/value.rs`, `crates/core/src/pos.rs` (type only),
`crates/core/tests/value.rs`.

**Interfaces — produces:**

```rust
pub type Id = Arc<str>;
pub type Plain = rmpv::Value;            // what arrives from JSON/MessagePack with ordinary lists

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null, Bool(bool), Int(i64), Float(f64), Str(Arc<str>),
    List(Arc<Vec<Value>>),               // replaced whole
    Map(Map),                            // imbl::OrdMap<Arc<str>, Value>
    Keyed(KeyedList),
}
pub struct KeyedList { /* imbl::OrdMap<Id,(Pos,Value)> + imbl::OrdSet<(Pos,Id)> */ }
impl KeyedList {
    pub fn get(&self, id: &str) -> Option<&Value>;
    pub fn pos(&self, id: &str) -> Option<&Pos>;
    pub fn iter(&self) -> impl Iterator<Item = (&Id, &Pos, &Value)>;   // in order: (pos, id)
    pub fn len(&self) -> usize;
}
pub enum ValueError { TooDeep, NotFinite, KeyTooLong, DuplicateId(Id), BadId }
impl Value {
    pub fn from_plain(p: &Plain) -> Result<Value, ValueError>;  // assigns positions to keyed lists
    pub fn to_plain(&self) -> Plain;                             // keyed lists become ordinary lists
    pub fn from_wire(p: &Plain) -> Result<Value, ValueError>;   // keyed list = {"$list": [[pos, item], ...]}
    pub fn to_wire(&self) -> Plain;
}
```

Rules the tests fix: a plain list is keyed when it is non-empty and every element is a map with a string
`id`, all ids distinct; `Int` and `Float` never compare equal; `from_plain` assigns evenly spread
positions with `Pos::spread(n)`; depth over 64 is `TooDeep`; a map key or an `id` over 200 characters is
`KeyTooLong`; NaN or infinity is `NotFinite`; binary and extension MessagePack values, and map keys that
are not strings, are `Unsupported` (add that variant to `ValueError`; `BadId` is for a malformed
`$list` entry in `from_wire`).

- [ ] **Step 1: failing tests** in `tests/value.rs`:

```rust
#[test] fn plain_round_trip_keeps_order() {
    let p = json!({"features":[{"id":"f1","name":"Sketch1"},{"id":"f2","name":"Boss-Extrude1"}],"units":"mm"});
    let v = Value::from_plain(&to_plain(p.clone())).unwrap();
    assert!(matches!(v.at(&["features"]), Some(Value::Keyed(k)) if k.len() == 2));
    assert_eq!(v.to_plain(), to_plain(p));
}
#[test] fn wire_round_trip_keeps_positions() { /* from_wire(to_wire(v)) == v, positions equal */ }
#[test] fn list_of_scalars_is_not_keyed() { /* [1,2,3] -> Value::List */ }
#[test] fn duplicate_ids_make_a_plain_list() { /* [{"id":"a"},{"id":"a"}] -> Value::List */ }
#[test] fn one_and_one_point_zero_differ() { assert_ne!(Value::Int(1), Value::Float(1.0)); }
#[test] fn depth_65_is_refused() { /* 65 nested maps -> Err(TooDeep); 64 -> Ok */ }
#[test] fn nan_is_refused() { /* Err(NotFinite) */ }
#[test] fn id_of_201_chars_is_refused() { /* Err(KeyTooLong) */ }
#[test] fn clone_shares_structure() { /* clone of a 10_000-feature doc is O(1): Arc::ptr_eq on a feature */ }
```

  (`to_plain(serde_json::Value) -> Plain` is a test helper; `Value::at(&[&str])` is a small public
  lookup used by later tasks.)
- [ ] **Step 2:** Run `cargo test -p oswx-core --test value`. Expected: compile failure.
- [ ] **Step 3:** Implement. `Pos` in this task needs only `Ord`, `Clone`, a string form, and
  `Pos::spread(n: usize) -> Vec<Pos>`.
- [ ] **Step 4:** Run the tests. Expected: all pass.
- [ ] **Step 5:** Commit: `core: values, keyed lists, plain and wire forms`.

---

### Task 3: Positions

**Files:** Modify `crates/core/src/pos.rs`; create `crates/core/tests/pos.rs`.

**Interfaces — produces:**

```rust
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pos(Arc<str>);               // base-62 digits, ordered as byte strings, never ends in '0'
impl Pos {
    pub fn between(a: Option<&Pos>, b: Option<&Pos>) -> Pos;   // a < result < b; panics if a >= b
    pub fn spread(n: usize) -> Vec<Pos>;
    pub fn parse(s: &str) -> Option<Pos>;                       // digits only, 1..=200 chars
    pub fn as_str(&self) -> &str;
}
```

- [ ] **Step 1: failing tests**:

```rust
proptest! {
    #[test] fn between_is_strictly_between(seed in any::<u64>()) {
        // start with spread(3); 500 times pick a random gap (including before first / after last),
        // insert Pos::between; assert the list stays strictly sorted and every Pos::parse round-trips
    }
}
#[test] fn repeated_insert_at_front_stays_short() {
    // 1_000 inserts before the first element: longest position is under 200 characters
}
#[test] fn same_gap_gives_same_position() {
    let p = Pos::spread(2);
    assert_eq!(Pos::between(Some(&p[0]), Some(&p[1])), Pos::between(Some(&p[0]), Some(&p[1])));
}
#[test] fn equal_positions_order_by_id() {
    // KeyedList with two items at the same Pos iterates in id order ("a.1.1" before "b.1.1")
}
```

- [ ] **Step 2:** Run; expected FAIL.
- [ ] **Step 3:** Implement the standard fractional-index midpoint on base-62 digit strings.
- [ ] **Step 4:** Run; expected PASS.
- [ ] **Step 5:** Commit: `core: fractional positions`.

---

### Task 4: Operations and cleaning

**Files:** Create `crates/core/src/path.rs`, `crates/core/src/op.rs`, `crates/core/tests/op.rs`.

**Interfaces — produces:**

```rust
pub type Path = Vec<Arc<str>>;          // each segment is a map key or a keyed-list id

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Set    { path: Path, value: Value },
    Unset  { path: Path },
    Insert { list: Path, item: Value, pos: Pos },   // item is a Map with a string "id"
    Remove { list: Path, id: Id },
    Move   { list: Path, id: Id, pos: Pos },
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Write { At(Path), Position(Path, Id) }     // Position: the item's place in its list

impl Op {
    pub fn from_wire(p: &Plain) -> Result<Op, OpError>;   // rebuilt from known fields only
    pub fn to_wire(&self) -> Plain;
}
pub fn writes(ops: &[Op]) -> BTreeSet<Write>;
pub fn removed(ops: &[Op]) -> BTreeSet<Path>;             // item paths removed
pub const MAX_DEPTH: usize = 64;
pub const MAX_NAME: usize = 200;
pub const PROTECTED: [&str; 7] = ["rollback","active_config","display_state","journal","name","next_id","counters"];
```

Wire form: `{"op":"set","path":[..],"value":..}`, `{"op":"unset","path":[..]}`,
`{"op":"insert","list":[..],"item":{..},"pos":".."}`, `{"op":"remove","list":[..],"id":".."}`,
`{"op":"move","list":[..],"id":"..","pos":".."}`. Unknown fields are dropped; an unknown `op` is
`OpError::Invalid`.

`writes`: `Set`/`Unset` → `At(path)`; `Insert` → `At(list + id)`; `Remove` → `At(list + id)`;
`Move` → `Position(list, id)`.

- [ ] **Step 1: failing tests**: round trip of each op through `to_wire`/`from_wire`; extra field
  `{"op":"set","path":["a"],"value":1,"evil":true}` round-trips without `evil`; empty path refused; path
  of 65 segments refused; a segment of 201 characters refused; `set` whose first segment is in
  `PROTECTED` refused; `insert` whose item has no string `id` refused; `pos` that fails `Pos::parse`
  refused; `writes` of one of each op equals the set described above.
- [ ] **Step 2:** Run; FAIL.
- [ ] **Step 3:** Implement. `OpError` lives in `apply.rs` in Task 5; declare it here as
  `pub enum OpError { Invalid(String), Missing(Path), NameTaken(Arc<str>), IdTaken(Id), ForeignId(Id) }`
  and move nothing later.
- [ ] **Step 4:** Run; PASS.
- [ ] **Step 5:** Commit: `core: operations and cleaning`.

---

### Task 5: Apply, inverse, rules

**Files:** Create `crates/core/src/apply.rs`, `crates/core/tests/apply.rs`.

**Interfaces — produces:**

```rust
pub struct Rules {
    pub unique: Vec<(Path, Arc<str>)>,      // (keyed list, field): values distinct across items
}
impl Rules { pub fn openswx() -> Rules; }    // unique: (["features"], "name")

pub struct Applied { pub doc: Value, pub inverse: Vec<Op> }

/// Applies all ops or none. `id_prefix`: Some("<user>.<client>.") for client changes, None for the server.
pub fn apply(doc: &Value, ops: &[Op], rules: &Rules, id_prefix: Option<&str>) -> Result<Applied, OpError>;
```

Semantics the tests fix:

- `inverse` applied to `Applied.doc` gives back `doc` exactly, positions included (the inverse of
  `Remove` is `Insert` at the old position; of `Move`, `Move` to the old position; of `Set` over an
  existing value, `Set` of the old value; of `Set` creating a key, `Unset`).
- `Set`/`Unset` create no intermediate maps: every segment but the last must exist (`Missing`).
- `Set` may not target a keyed-list item itself (path ending on an item id) → `Invalid`; use
  `Insert`/`Remove`.
- `Insert` into a missing key or an empty `List` creates a keyed list; into a non-empty plain `List` →
  `Invalid`; an id already present → `IdTaken`.
- With `id_prefix = Some(p)`: every id that the change introduces (the inserted item's, and every item
  of every keyed list inside inserted or set values, unless that id already existed at the same path
  before the change) must start with `p`, else `ForeignId`.
- Uniqueness is checked once, after all ops, so one change can swap two names; a clash → `NameTaken`.
- Untouched subtrees are shared: after editing `features/f2/params/depth` in a 1,000-feature document,
  `Arc::ptr_eq` holds for feature `f1` before and after.

- [ ] **Step 1: failing tests**, using this fixture (the Python tests' `PART` from `tests/conftest.py`):

```rust
fn part() -> Value { /* from_plain of conftest.PART */ }
#[test] fn set_and_inverse() {
    let a = apply(&part(), &[set(&["features","f2","params","depth"], 25)], &Rules::openswx(), None).unwrap();
    assert_eq!(a.doc.at(&["features","f2","params","depth"]), Some(&Value::Int(25)));
    assert_eq!(apply(&a.doc, &a.inverse, &Rules::openswx(), None).unwrap().doc, part());
}
#[test] fn legacy_ids_can_be_edited_but_not_introduced() {
    // set inside f1 with prefix Some("u1.c1.") -> Ok; insert item id "f9" -> Err(ForeignId)
    // insert item id "u1.c1.1" -> Ok; insert item id "u2.c1.1" -> Err(ForeignId)
}
#[test] fn nested_new_ids_are_checked() {
    // set features/f1/params/sketch/dims = [{"id":"u2.c9.1"}] with prefix "u1.c1." -> Err(ForeignId)
}
#[test] fn swap_two_names_in_one_change() { /* Ok */ }
#[test] fn duplicate_name_is_refused() { /* Err(NameTaken("Sketch1")) and doc unchanged */ }
#[test] fn all_or_nothing() { /* [valid set, remove of missing id] -> Err(Missing); nothing applied */ }
#[test] fn set_through_missing_parent() { /* Err(Missing(path of the missing segment)) */ }
#[test] fn insert_remove_move_inverse() { /* each, then inverse, equals the original with positions */ }
#[test] fn sharing() { /* Arc::ptr_eq as described */ }
proptest! { #[test] fn inverse_restores(ops in arb_ops()) { /* any applicable op list */ } }
```

- [ ] **Step 2:** Run; FAIL.
- [ ] **Step 3:** Implement on a working copy (cheap: it is persistent), returning it only on success.
- [ ] **Step 4:** Run; PASS.
- [ ] **Step 5:** Commit: `core: apply with inverse and rules`.

---

### Task 6: Conflict index

**Files:** Create `crates/core/src/index.rs`, `crates/core/tests/index.rs`.

**Interfaces — produces:**

```rust
pub type Seq = u64;
pub type ClientId = Arc<str>;            // "<user>.<client>"

#[derive(Clone, Debug, PartialEq)]
pub struct Conflict { pub path: Path, pub seq: Seq, pub by: ClientId, pub kind: ConflictKind }
pub enum ConflictKind { Written, Removed, Read }

#[derive(Clone, Default)]
pub struct Index { /* persistent trie: per node last write, last removal, last read, subtree max seq */ }
impl Index {
    /// Conflicts for a change by `client` made on `base`: empty means acceptable.
    pub fn check(&self, base: Seq, client: &str, writes: &BTreeSet<Write>, removed: &BTreeSet<Path>,
                 reads: &BTreeSet<Path>) -> Vec<Conflict>;
    /// Record an accepted change.
    pub fn record(&self, seq: Seq, client: &ClientId, writes: &BTreeSet<Write>, removed: &BTreeSet<Path>,
                  reads: &BTreeSet<Path>) -> Index;
    pub fn floor(&self) -> Seq;                               // bases below this are too old to judge
    pub fn prune(&self, floor: Seq) -> Index;                 // drop removals and reads at or below floor
    pub fn to_wire(&self) -> Plain;  pub fn from_wire(p: &Plain) -> Option<Index>;   // for snapshots
}
```

Rule (`other` = a different client, `after` = seq greater than `base`):

- a `Write::At(p)` conflicts if another client after base wrote `p`, any prefix of `p`, or anything
  under `p` (`Written`), or removed `p` or a prefix (`Removed`);
- a `Write::Position(list, id)` conflicts with another `Position` write of the same item, or a removal
  of the item or a prefix;
- a path in `reads` conflicts if another client after base removed it or a prefix (`Removed`);
- a path in `removed` conflicts if another client after base read it or anything under it (`Read`);
- paths with any segment starting `~` never conflict and are not recorded;
- the sender's own earlier writes never conflict.

- [ ] **Step 1: failing tests**:

```rust
#[test] fn different_fields_of_one_feature_do_not_conflict()  // A: f2/params/depth, B: f2/params/draft
#[test] fn same_field_conflicts_and_names_the_writer()        // Conflict{by:"uA.c1", seq:5, kind:Written}
#[test] fn edit_inside_removed_item_is_a_removal_conflict()   // kind: Removed, path: features/f2
#[test] fn replacing_a_map_conflicts_with_edits_below_it()    // A sets f2/params, B set f2/params/depth
#[test] fn two_inserts_do_not_conflict()
#[test] fn two_moves_of_one_item_conflict()
#[test] fn move_and_edit_of_one_item_do_not_conflict()
#[test] fn removing_something_another_change_read_conflicts() // B reads features/f1; A removes f1 -> Read
#[test] fn reading_something_removed_conflicts()
#[test] fn own_writes_never_conflict()
#[test] fn derived_keys_never_conflict()                       // .../~solved/...
#[test] fn before_base_is_ignored()
#[test] fn wire_round_trip()
#[test] fn prune_keeps_writes_drops_old_removals()
```

- [ ] **Step 2:** Run; FAIL.
- [ ] **Step 3:** Implement the trie with `imbl::OrdMap` children so `record` shares structure.
- [ ] **Step 4:** Run; PASS.
- [ ] **Step 5:** Commit: `core: conflict index by path`.

---

### Task 7: Change log (the pure room core)

The part of a room that is logic, with no I/O, so the server (1b), the convergence test and later the
client engine all use the same code.

**Files:** Create `crates/core/src/log.rs`, `crates/core/tests/log.rs`, `crates/core/tests/convergence.rs`.

**Interfaces — produces:**

```rust
pub struct Change {
    pub change_id: Arc<str>,             // 1..=64 chars
    pub base: Seq,
    pub ops: Vec<Op>,
    pub reads: BTreeSet<Path>,
    pub depends: Vec<Arc<str>>,          // the sender's own earlier change ids this one builds on
    pub undo_of: Option<Arc<str>>,
    pub command: Arc<str>,
}
pub struct Accepted { pub seq: Seq, pub doc: Value, pub index: Index, pub inverse: Vec<Op> }
pub enum Refusal {
    Stale(Vec<Conflict>), Dependency(Arc<str>), NameTaken(Arc<str>), IdTaken(Id), Invalid(String),
    TooOld,
}
pub struct Head { pub seq: Seq, pub doc: Value, pub index: Index }

/// Decide one change against the head. Pure; the caller persists and then adopts `Accepted`.
pub fn decide(head: &Head, client: &ClientId, change: &Change, refused: &dyn Fn(&str) -> bool,
              rules: &Rules) -> Result<Accepted, Refusal>;
```

`refused(change_id)` says whether that earlier change of the same client was refused. Order of checks:
`base < head.index.floor()` → `TooOld`; any `depends` refused → `Dependency(id)`; index conflicts →
`Stale`; `apply` with prefix `"<client>."` → `OpError` mapped to `NameTaken`/`IdTaken`/`Invalid`
(`Missing` and `ForeignId` are `Invalid`).

- [ ] **Step 1: failing tests** in `tests/log.rs`:

```rust
#[test] fn disjoint_changes_from_one_base_are_both_accepted()
#[test] fn second_writer_of_a_field_is_stale()
#[test] fn change_depending_on_a_refused_change_is_refused() {
    // c1 (set depth) is Stale; c2 depends:[c1] sets draft -> Err(Dependency("c1")) though it would apply
}
#[test] fn independent_later_change_survives_a_refusal()   // c2 without depends -> accepted
#[test] fn name_clash_is_name_taken()
#[test] fn base_below_floor_is_too_old()
#[test] fn accepted_is_not_adopted_until_caller_says()     // head unchanged after decide
```

- [ ] **Step 2: failing convergence test** in `tests/convergence.rs` (port of
  `tests/test_convergence.py`): 4 simulated clients, 40 rounds, 3 seeds. Each round every client makes
  0–2 random edits (set a param, insert a feature with its own id prefix, remove, move, rename) against
  its own view with `depends` filled from overlapping pending changes; the test delivers them to
  `decide` in a random interleaving, adopts accepted ones, sends results back in order; clients roll
  back refused changes and their dependents with the kept inverses. Assert at the end: every client's
  document equals the head; replaying the accepted ops from the initial document equals the head;
  feature names are unique.
- [ ] **Step 3:** Run both; FAIL.
- [ ] **Step 4:** Implement `decide`; put the simulated client in `tests/common/client.rs` (it is the
  reference for the real client engine in project 2, so keep it readable).
- [ ] **Step 5:** Run; PASS for all three seeds. Run the proptest suites once with
  `PROPTEST_CASES=2000`.
- [ ] **Step 6:** Commit: `core: change decisions, dependency chains, convergence test`.

---

### Task 8: Diff

**Files:** Create `crates/core/src/diff.rs`, `crates/core/tests/diff.rs`.

**Interfaces — produces:**

```rust
/// Ops that turn `before` into a document whose plain form equals `after`.
/// New positions are chosen between surviving neighbours of `before`.
pub fn diff(before: &Value, after: &Plain) -> Result<Vec<Op>, ValueError>;
```

Rules: maps are diffed key by key (recursively); keyed lists by id (`Remove`, `Insert`, `Move` for the
smallest set of items that are out of order, found by longest increasing subsequence, and recursive
diff inside items present on both sides); plain lists and scalars are replaced whole with `Set`;
top-level `PROTECTED` keys are skipped; `Int(1)` to `Float(1.0)` is a change.

- [ ] **Step 1: failing tests**: no change → empty; one param changed → exactly one `Set` at the full
  path; feature appended → one `Insert` with a position after the last; feature removed; two features
  swapped → exactly one `Move`; a change inside `params.sketch.entities.<id>` gives a `Set` under that
  entity, not a replacement of `entities`; protected keys ignored; and

```rust
proptest! { #[test] fn diff_then_apply_reaches_target(a in arb_doc(), b in arb_doc()) {
    let ops = diff(&a, &b.to_plain()).unwrap();
    let got = apply(&a, &ops, &Rules { unique: vec![] }, None).unwrap().doc;
    prop_assert_eq!(got.to_plain(), strip_protected(b.to_plain()));
} }
```

- [ ] **Step 2:** Run; FAIL. **Step 3:** Implement. **Step 4:** Run; PASS.
- [ ] **Step 5:** Commit: `core: diff`.

---

### Task 9: Protocol v2 messages and vectors

**Files:** Create `crates/core/src/wire.rs`, `crates/core/tests/wire.rs`, `crates/core/tests/vectors.rs`,
`crates/core/vectors/*.json`; rewrite the WebSocket half of nothing yet (docs change in 1b).

**Interfaces — produces:**

```rust
pub const PROTOCOL_VERSION: u32 = 2;
pub enum ClientMsg {
    Hello { protocol: u32, token: String, client_id: String, format_version: Option<i64>, last_seq: Option<Seq>, app: Option<String> },
    Change(Change),
    Revert { request_id: String, seq: Seq },
    LockAcquire { request_id: String, paths: Vec<Path> }, LockRelease { paths: Vec<Path> },
    Presence { data: Plain }, Draft { data: Option<Plain> },
    RebuildReport { seq: Seq, errors: BTreeMap<String, String> },
    Ping,
}
pub enum ServerMsg { /* Welcome, Error, Accepted, Rejected, Change, RevertResult, LockChanged,
                       PeerJoined, PeerLeft, Presence, Draft, RebuildStatus, VersionCreated,
                       ReferenceUpdate, Pong */ }
pub fn decode_client(frame: &[u8], max: usize) -> Result<ClientMsg, WireError>;
pub fn encode_server(msg: &ServerMsg) -> bytes::Bytes;
pub fn encode_client(msg: &ClientMsg) -> bytes::Bytes;      // for the wheel and loadgen
pub fn decode_server(frame: &[u8]) -> Result<ServerMsg, WireError>;
```

Frames are MessagePack maps with a string field `t` (`hello`, `change`, `change.accepted`,
`change.rejected`, `change.revert`, `change.revert.result`, `lock.acquire`, `lock.release`,
`lock.changed`, `presence`, `draft`, `rebuild.report`, `rebuild.status`, `peer.joined`, `peer.left`,
`version.created`, `reference.update`, `welcome`, `error`, `ping`, `pong`). Field names for messages
that exist in v1 are the v1 names from `protocol/openswx_collab_protocol/messages.py`, with these
differences: `change` gains `reads`, `depends`, `undo_of` and loses `args`; lock `targets` become
`paths`; `welcome` loses `reserved`, and its `content` is the wire form of the document (zstd is applied
by the server, not here); `change.rejected.code` gains `dependency`, `name_taken`, `too_old`.
Size limits enforced in `decode_client`: frame over `max` → `WireError::TooLarge`; `client_id` 8..=64;
`change_id` 1..=64; presence 16 KB; draft 128 KB; at most 256 lock paths; at most 500 rebuild errors of
500 characters.

- [ ] **Step 1: failing tests**: round trip of every message; each limit just under and just over; a
  frame that is not a map, has no `t`, or an unknown `t` → `WireError::Invalid`; unknown fields ignored;
  a text (JSON) frame is not accepted.
- [ ] **Step 2:** Vector test: each `vectors/*.json` is `{"name":..,"hex":..,"direction":"c2s"|"s2c",
  "json":..}`; the test decodes `hex` and compares with `json` (through a debug-JSON rendering of the
  message), and re-encodes to the same bytes. Write vectors for `hello`, a `change` with all five op
  kinds, `change.rejected` with one conflict, and `welcome` with a two-feature document. Generate the
  hex with a `#[ignore]` test that prints it, then paste; the wheel's tests will read the same files.
- [ ] **Step 3:** Run; FAIL. **Step 4:** Implement with `rmp-serde` (struct-as-map) and hand-written
  conversion for `Value`/`Op`. **Step 5:** Run; PASS.
- [ ] **Step 6:** Commit: `core: protocol v2 messages and test vectors`.

---

### Task 10: Load generator against the Python server

**Files:** Create `crates/loadgen/**`, `bench/README.md`.

**Interfaces — produces:**

```rust
#[async_trait] pub trait Dialect: Send + Sync + 'static {
    async fn setup(&self, base_url: &str, features: usize, doc_bytes: usize) -> anyhow::Result<Target>; // registers the first user, org, project, document
    async fn connect(&self, target: &Target, n: usize) -> anyhow::Result<Box<dyn Peer>>;
}
#[async_trait] pub trait Peer: Send {
    async fn edit(&mut self, feature: usize) -> anyhow::Result<Outcome>;   // one small change; waits for the answer
    async fn presence(&mut self) -> anyhow::Result<()>;
    async fn drain(&mut self) -> anyhow::Result<()>;                       // read and discard broadcasts
}
pub enum Outcome { Accepted, Rejected(String) }
pub struct Target { pub base_url: String, pub token: String, pub documents: Vec<String>, pub features: usize }
```

CLI: `loadgen --dialect v1 --url http://127.0.0.1:8791 <scenario> [--duration 30s] [--out file.json]`
with scenarios:

| Scenario | Meaning |
|---|---|
| `one-doc --editors N --rate R --features F` | N connections on one document; each edits its own feature (`feature = n % F`) R times a second |
| `many-docs --docs D --editors-per-doc E --rate R` | D documents, E editors each |
| `open --doc-bytes B --times T` | time from `hello` to `welcome` for a document of about B bytes |
| `idle --connections N` | N connections sending only pings and presence; reports server RSS if `--pid` is given |

v1 dialect: all peers use the admin's token with distinct `client_id`s (the server knows each as
`<user>.<client_id>`); the document is `tests/conftest.py`'s `PART` extended to F extrude features with
padding strings to reach `doc_bytes`; an edit is
`{"type":"change","change_id":uuid,"base_seq":last,"ops":[{"op":"feature.update","id":fid,"set":[[["params","depth"],n]],"unset":[]}],"command":"bench"}`
(check the exact op shape against `protocol/openswx_collab_protocol/ops.py` before relying on it);
latency is the time from send to `change.accepted`; each peer applies incoming `change` seqs to keep
`base_seq` current.

Report (JSON): scenario and its parameters, server description string, duration, changes sent,
accepted, rejected by code, latency p50/p90/p99/max in ms (hdrhistogram), throughput per second,
connections that were dropped.

- [ ] **Step 1:** Unit tests for the report (percentiles from known samples) and the document builder
  (`--features 500 --doc-bytes 5_000_000` gives a document within 5% of the size, 500 features).
- [ ] **Step 2:** Implement the CLI, scenarios and v1 dialect.
- [ ] **Step 3: integration check.** In a scratch virtualenv:
  `pip install -e protocol -e server`, then with `OSWX_COLLAB_DATABASE_URL` pointing at a new SQLite
  file and `OSWX_COLLAB_STORAGE_URL` at a new folder, `python -m openswx_collab_server serve --port 8791`.
  Run `loadgen --dialect v1 --url http://127.0.0.1:8791 one-doc --editors 4 --rate 5 --features 20 --duration 10s`.
  Expected: about 200 changes, all accepted, none rejected, no dropped connections.
- [ ] **Step 4:** Write `bench/README.md`: the commands above, and that each result file records CPU,
  RAM, OS, database and commit.
- [ ] **Step 5:** Commit: `loadgen: scenarios and v1 dialect`.

---

### Task 11: Record the baseline

**Files:** Create `bench/results/2026-11-python-0.1.2-sqlite.json` (and `-postgres.json` if PostgreSQL
is reachable; the Pi at `duke@dukerpi` has Docker, avoid port 8787), `bench/results/README.md`.

- [ ] **Step 1:** With a fresh database each run, record: `one-doc` with 10, 40, 100 and 200 editors at
  rate 2, features 200; `many-docs --docs 200 --editors-per-doc 3 --rate 1`; `open` with 100 KB, 1 MB
  and 5 MB, 20 times each; `idle --connections 2000`. Duration 60 s each. A scenario the server cannot
  survive is recorded as failed with what happened, not dropped.
- [ ] **Step 2:** `bench/results/README.md`: one table of the numbers, the machine, and one paragraph
  on where it fell over.
- [ ] **Step 3:** Commit: `bench: baseline for the Python server 0.1.2`.

---

## Self-review notes

- Spec coverage for this slice: document model, operations, positions (amendment 2), reads
  (amendment 3), ids (4), names refused (5), dependencies (1), derived keys (9), undo marker (7, the
  field only; behaviour is the server's and client's), wire v2, baseline numbers. Everything else in the
  spec belongs to plans 1b, 1c and projects 2–4.
- `counters` and `next_id` stay in `PROTECTED` so documents from the Python server do not sync them.
