//! Applying operations to a document: all or none, with the operations that undo them.
//!
//! Every edit rebuilds only the path it changes; everything else is shared with the input.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::op::{MAX_DEPTH, Op, OpError};
use crate::path::{self, Path};
use crate::value::{Id, KeyedList, Value};

/// Invariants every change must leave intact.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Rules {
    /// (keyed list, field): the field's string values are distinct across the list's items.
    pub unique: Vec<(Path, Arc<str>)>,
}

impl Rules {
    /// The rules of an openswx part: feature names are unique.
    pub fn openswx() -> Rules {
        Rules {
            unique: vec![(vec![Arc::from("features")], Arc::from("name"))],
        }
    }
}

/// A changed document and the operations that change it back.
#[derive(Clone, Debug, PartialEq)]
pub struct Applied {
    pub doc: Value,
    /// `inverse` applied to `doc` with `Rules::default()` and `id_prefix = None` gives back the
    /// original document exactly. Applied under rules, an inverse is judged like any other change
    /// and may be refused with `NameTaken` when it would bring back a name that is now taken;
    /// callers treat that as an ordinary name clash.
    pub inverse: Vec<Op>,
}

/// Applies all ops or none. `id_prefix`: Some("<user>.<client>.") for client changes, None for the server.
///
/// Contract: `inverse` applied to `Applied.doc` with `Rules::default()` and `id_prefix = None`
/// gives back `doc` exactly. Applied under rules, an inverse is judged like any other change and
/// may be refused with `NameTaken` when it would bring back a name that is now taken; callers
/// treat that as an ordinary name clash.
pub fn apply(
    doc: &Value,
    ops: &[Op],
    rules: &Rules,
    id_prefix: Option<&str>,
) -> Result<Applied, OpError> {
    apply_with(doc, ops, rules, id_prefix, &|_| false)
}

/// `apply`, told which removed items may come back. `existed(path)` (a keyed list's path plus an
/// item id) says whether a removal of exactly that item is still remembered; any client may then
/// put an item with that id back at that path, and everything inside it, whatever its ids. This
/// is how an undo of a removal, or a restore of someone else's deleted item, passes the prefix
/// check. Once the removal is forgotten (the index pruned past it) the id is foreign again.
/// `apply` is `apply_with` with nothing remembered. Only consulted when `id_prefix` is given.
pub fn apply_with(
    doc: &Value,
    ops: &[Op],
    rules: &Rules,
    id_prefix: Option<&str>,
    existed: &dyn Fn(&Path) -> bool,
) -> Result<Applied, OpError> {
    apply_judged(doc, ops, rules, id_prefix, existed, &|_| false)
}

/// `apply_with`, told also which items a kept inverse carries. `kept(path)` (a keyed list's path
/// plus an item id, built the way the prefix check walks values: see [`carried_items`]) says that
/// the change being undone took an item with that id away from that path; the id may come back
/// there whoever made it. Unlike `existed`, it exempts that one id only: the items inside it are
/// judged one by one, so an id the inverse did not carry is still refused.
pub(crate) fn apply_judged(
    doc: &Value,
    ops: &[Op],
    rules: &Rules,
    id_prefix: Option<&str>,
    existed: &dyn Fn(&Path) -> bool,
    kept: &dyn Fn(&Path) -> bool,
) -> Result<Applied, OpError> {
    let mut out = doc.clone();
    let mut inverse = Vec::with_capacity(ops.len());
    let mut introduced = Introduced::default();
    let mut touched: Vec<Touched> = rules.unique.iter().map(|_| Touched::default()).collect();
    for op in ops {
        let (next, undo) = apply_one(&out, op)?;
        out = next;
        inverse.push(undo);
        if id_prefix.is_some() {
            introduced.note(doc, op, existed, kept);
        }
        for ((list, field), t) in rules.unique.iter().zip(&mut touched) {
            t.note(op, list, field);
        }
    }
    inverse.reverse();
    if let Some(prefix) = id_prefix {
        introduced.check(doc, &out, prefix)?;
    }
    for ((list, field), t) in rules.unique.iter().zip(&touched) {
        t.check(doc, &out, list, field)?;
    }
    Ok(Applied { doc: out, inverse })
}

fn invalid(reason: &str) -> OpError {
    OpError::Invalid(reason.to_string())
}

fn child(list: &Path, id: &Id) -> Path {
    let mut p = list.clone();
    p.push(id.clone());
    p
}

/// The value at `path`, through maps (by key) and keyed lists (by item id).
fn get<'a>(v: &'a Value, path: &[Id]) -> Option<&'a Value> {
    path.iter().try_fold(v, |cur, seg| match cur {
        Value::Map(m) => m.get(seg),
        Value::Keyed(k) => k.get(seg),
        _ => None,
    })
}

fn has_item(v: &Value, list: &[Id], id: &str) -> bool {
    matches!(get(v, list), Some(Value::Keyed(k)) if k.get(id).is_some())
}

thread_local! {
    /// Values `ids_under` has visited on this thread: lets tests bound the work a change costs.
    static HELD_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many values the prefix check has walked on this thread to learn which ids a Set's path
/// already held. For tests: take the difference across a call.
#[doc(hidden)]
pub fn held_values_visited() -> usize {
    HELD_VISITS.with(|c| c.get())
}

/// Adds to `out` the id of every item under `v`: keyed-list items, and maps with a string `id`
/// that are elements of plain lists (an item of a list that might become keyed).
fn ids_under(v: &Value, out: &mut HashSet<Id>) {
    HELD_VISITS.with(|c| c.set(c.get() + 1));
    match v {
        Value::Map(m) => m.values().for_each(|c| ids_under(c, out)),
        Value::Keyed(k) => {
            for (id, _, item) in k.iter() {
                out.insert(id.clone());
                ids_under(item, out);
            }
        }
        Value::List(items) => {
            for item in items.iter() {
                if let Some(id) = item_id(item) {
                    out.insert(id.clone());
                }
                ids_under(item, out);
            }
        }
        _ => {}
    }
}

/// The id of an item: a map with a non-empty string `id`.
fn item_id(item: &Value) -> Option<&Id> {
    match item {
        Value::Map(m) => match m.get("id") {
            Some(Value::Str(id)) if !id.is_empty() => Some(id),
            _ => None,
        },
        _ => None,
    }
}

/// `cur` with the value at `path[at..end]` replaced by what `f` makes of it. `f` also learns
/// whether that value is a keyed-list item. Every segment must exist: a missing one, or a step
/// into anything but a map or keyed list, is `Missing` with the path up to that segment.
fn edit_at<T>(
    cur: &Value,
    path: &[Id],
    at: usize,
    end: usize,
    in_item: bool,
    f: impl FnOnce(&Value, bool) -> Result<(Value, T), OpError>,
) -> Result<(Value, T), OpError> {
    if at == end {
        return f(cur, in_item);
    }
    let seg = &path[at];
    let missing = || OpError::Missing(path[..=at].to_vec());
    match cur {
        Value::Map(m) => {
            let next = m.get(seg).ok_or_else(missing)?;
            let (new, t) = edit_at(next, path, at + 1, end, false, f)?;
            Ok((Value::Map(m.update(seg.clone(), new)), t))
        }
        Value::Keyed(k) => {
            let next = k.get(seg).ok_or_else(missing)?;
            let (new, t) = edit_at(next, path, at + 1, end, true, f)?;
            let k = k.updated(seg, new).expect("the item is present");
            Ok((Value::Keyed(k), t))
        }
        _ => Err(missing()),
    }
}

/// Edits the parent of `path` (all of `path` but its last segment), handing `f` the parent, the
/// last segment and whether the parent is a keyed-list item.
fn edit_parent<T>(
    doc: &Value,
    path: &Path,
    f: impl FnOnce(&Value, &Id, bool) -> Result<(Value, T), OpError>,
) -> Result<(Value, T), OpError> {
    let Some(last) = path.last() else {
        return Err(invalid("empty path"));
    };
    edit_at(doc, path, 0, path.len() - 1, false, |parent, in_item| {
        f(parent, last, in_item)
    })
}

/// One operation on `doc`: the new document and the operation that undoes it.
fn apply_one(doc: &Value, op: &Op) -> Result<(Value, Op), OpError> {
    check_names(op)?;
    match op {
        Op::Set { path, value } => {
            check_depth(path.len(), value)?;
            check_wire_form(value)?;
            edit_parent(doc, path, |parent, last, in_item| match parent {
                Value::Map(m) => {
                    if in_item && &**last == "id" {
                        return Err(invalid("an item's id cannot be changed"));
                    }
                    let undo = match m.get(last) {
                        Some(old) => Op::Set {
                            path: path.clone(),
                            value: old.clone(),
                        },
                        None => Op::Unset { path: path.clone() },
                    };
                    Ok((Value::Map(m.update(last.clone(), value.clone())), undo))
                }
                Value::Keyed(_) => Err(invalid("set cannot replace a list item; use insert")),
                _ => Err(OpError::Missing(path.clone())),
            })
        }
        Op::Unset { path } => edit_parent(doc, path, |parent, last, in_item| match parent {
            Value::Map(m) => {
                if in_item && &**last == "id" {
                    return Err(invalid("an item's id cannot be removed"));
                }
                let mut m = m.clone();
                let old = m
                    .remove(last)
                    .ok_or_else(|| OpError::Missing(path.clone()))?;
                let undo = Op::Set {
                    path: path.clone(),
                    value: old,
                };
                Ok((Value::Map(m), undo))
            }
            Value::Keyed(_) => Err(invalid("unset cannot remove a list item; use remove")),
            _ => Err(OpError::Missing(path.clone())),
        }),
        Op::Insert { list, item, pos } => {
            let id = item_id(item).ok_or_else(|| invalid("item needs a string id"))?;
            check_depth(list.len() + 1, item)?;
            check_wire_form(item)?;
            let with_item =
                |k: &KeyedList| Value::Keyed(k.inserted(id.clone(), pos.clone(), item.clone()));
            edit_parent(doc, list, |parent, last, _| match parent {
                Value::Map(m) => {
                    let (new, undo) = match m.get(last) {
                        None => (
                            with_item(&KeyedList::new()),
                            Op::Unset { path: list.clone() },
                        ),
                        Some(Value::List(l)) if l.is_empty() => (
                            with_item(&KeyedList::new()),
                            Op::Set {
                                path: list.clone(),
                                value: Value::List(l.clone()),
                            },
                        ),
                        Some(Value::Keyed(k)) => {
                            if k.get(id).is_some() {
                                return Err(OpError::IdTaken(id.clone()));
                            }
                            let undo = Op::Remove {
                                list: list.clone(),
                                id: id.clone(),
                            };
                            (with_item(k), undo)
                        }
                        Some(_) => return Err(invalid("insert needs a keyed list")),
                    };
                    Ok((Value::Map(m.update(last.clone(), new)), undo))
                }
                Value::Keyed(k) if k.get(last).is_some() => {
                    Err(invalid("insert needs a keyed list"))
                }
                _ => Err(OpError::Missing(list.clone())),
            })
        }
        Op::Remove { list, id } => edit_at(doc, list, 0, list.len(), false, |l, _| {
            let Value::Keyed(k) = l else {
                return Err(invalid("remove needs a keyed list"));
            };
            let (rest, pos, item) = k
                .removed(id)
                .ok_or_else(|| OpError::Missing(child(list, id)))?;
            let undo = Op::Insert {
                list: list.clone(),
                item,
                pos,
            };
            Ok((Value::Keyed(rest), undo))
        }),
        Op::Move { list, id, pos } => edit_at(doc, list, 0, list.len(), false, |l, _| {
            let Value::Keyed(k) = l else {
                return Err(invalid("move needs a keyed list"));
            };
            let old = k
                .pos(id)
                .cloned()
                .ok_or_else(|| OpError::Missing(child(list, id)))?;
            let moved = k.moved(id, pos.clone()).expect("the item is present");
            let undo = Op::Move {
                list: list.clone(),
                id: id.clone(),
                pos: old,
            };
            Ok((Value::Keyed(moved), undo))
        }),
    }
}

/// Item ids a change may introduce, gathered from what it inserts and sets: walking only those
/// values (and, for a Set, the value it replaces) keeps the check proportional to the change.
#[derive(Default)]
struct Introduced {
    /// (keyed list, id).
    at: BTreeSet<(Path, Id)>,
    /// Ids in keyed lists inside plain lists. No path reaches them, so they are treated as new
    /// unless the Set that writes them replaces a value that already held them.
    unreachable: Vec<Id>,
    /// Per Set path, the ids under it in the document before the change. Learned only when a Set
    /// brings in an id, and once per path for the whole change, so that many Sets at one path cost
    /// one walk of what it held (at most `MAX_DEPTH` walks of any one value in all).
    held: HashMap<Path, HashSet<Id>>,
}

impl Introduced {
    /// `before` is the document before the whole change; `existed` as for `apply_with`.
    fn note(
        &mut self,
        before: &Value,
        op: &Op,
        existed: &dyn Fn(&Path) -> bool,
        kept: &dyn Fn(&Path) -> bool,
    ) {
        match op {
            Op::Set { path, value } => {
                // Ids anywhere under the Set's path before the change are not new for it, wherever
                // the new value puts them: an edit inside a plain list sets the whole list, and a
                // list may turn from plain to keyed or back. So a Set may also rearrange those ids
                // freely within its path, even into another list under it: telling a move from an
                // edit would need per-list bookkeeping. An item moved by Remove and Insert into a
                // different list is judged by the Insert and needs new ids (spec, amendment 4).
                let set = Held { before, path };
                let judge = Judge { existed, kept };
                self.walk(value, &mut path.clone(), true, Some(&set), &judge);
            }
            Op::Insert { list, item, .. } => {
                if let Some(id) = item_id(item) {
                    let at = child(list, id);
                    if existed(&at) {
                        return; // A remembered removal: the item, and all in it, may come back.
                    }
                    if !kept(&at) {
                        self.at.insert((list.clone(), id.clone()));
                    }
                    let judge = Judge { existed, kept };
                    self.walk(item, &mut at.clone(), true, None, &judge);
                }
            }
            Op::Unset { .. } | Op::Remove { .. } | Op::Move { .. } => {}
        }
    }

    /// Whether `id` was under the Set's path before the change.
    fn was_held(&mut self, set: &Held, id: &Id) -> bool {
        if !self.held.contains_key(set.path) {
            let mut ids = HashSet::new();
            if let Some(old) = get(set.before, set.path) {
                ids_under(old, &mut ids);
            }
            self.held.insert(set.path.clone(), ids);
        }
        self.held[set.path].contains(id)
    }

    /// Notes the item ids in `v`, which sits at `path`, except those the Set (if `v` is written by
    /// one) already held, those the kept inverse carried at the same path, and the items (with
    /// everything in them) whose removal `existed` remembers.
    fn walk(
        &mut self,
        v: &Value,
        path: &mut Path,
        reachable: bool,
        held: Option<&Held>,
        judge: &Judge,
    ) {
        match v {
            Value::Map(m) => {
                for (k, child) in m.iter() {
                    path.push(k.clone());
                    self.walk(child, path, reachable, held, judge);
                    path.pop();
                }
            }
            Value::Keyed(k) => {
                for (id, _, item) in k.iter() {
                    let list = path.clone();
                    path.push(id.clone());
                    if !(reachable && (judge.existed)(path)) {
                        if (judge.kept)(path) || held.is_some_and(|set| self.was_held(set, id)) {
                            // Was there before, or the change being undone took it from here.
                        } else if reachable {
                            self.at.insert((list, id.clone()));
                        } else {
                            self.unreachable.push(id.clone());
                        }
                        self.walk(item, path, reachable, held, judge);
                    }
                    path.pop();
                }
            }
            Value::List(items) => {
                for item in items.iter() {
                    self.walk(item, path, false, held, judge);
                }
            }
            _ => {}
        }
    }

    /// Every id present after the change but not at the same path before it starts with `prefix`.
    fn check(&self, before: &Value, after: &Value, prefix: &str) -> Result<(), OpError> {
        let foreign = |id: &Id| !id.starts_with(prefix);
        let new_at = self
            .at
            .iter()
            .filter(|(list, id)| has_item(after, list, id) && !has_item(before, list, id))
            .map(|(_, id)| id);
        match self.unreachable.iter().chain(new_at).find(|id| foreign(id)) {
            Some(id) => Err(OpError::ForeignId(id.clone())),
            None => Ok(()),
        }
    }
}

/// What lets an id in without the client's prefix: a remembered removal (`existed`, which lets in
/// the whole item) or the kept inverse of the change being undone (`kept`, that id only).
struct Judge<'a> {
    existed: &'a dyn Fn(&Path) -> bool,
    kept: &'a dyn Fn(&Path) -> bool,
}

/// The paths of the items `ops` carry in their values: for every Set value and Insert item, each
/// keyed-list item in it at the path (list path plus id) where the prefix check finds it. Items in
/// keyed lists inside plain lists get the path the check builds for them too (map keys and item ids
/// from the nearest path above, the plain list itself adding nothing), so a value carried back the
/// way it was matches item for item.
pub(crate) fn carried_items(ops: &[Op]) -> HashSet<Path> {
    fn walk(v: &Value, path: &mut Path, out: &mut HashSet<Path>) {
        match v {
            Value::Map(m) => {
                for (k, child) in m.iter() {
                    path.push(k.clone());
                    walk(child, path, out);
                    path.pop();
                }
            }
            Value::Keyed(k) => {
                for (id, _, item) in k.iter() {
                    path.push(id.clone());
                    out.insert(path.clone());
                    walk(item, path, out);
                    path.pop();
                }
            }
            Value::List(items) => items.iter().for_each(|item| walk(item, path, out)),
            _ => {}
        }
    }
    let mut out = HashSet::new();
    for op in ops {
        match op {
            Op::Set { path, value } => walk(value, &mut path.clone(), &mut out),
            Op::Insert { list, item, .. } => {
                if let Some(id) = item_id(item) {
                    let mut at = child(list, id);
                    out.insert(at.clone());
                    walk(item, &mut at, &mut out);
                }
            }
            Op::Unset { .. } | Op::Remove { .. } | Op::Move { .. } => {}
        }
    }
    out
}

/// A Set being judged: its path, in the document before the change.
struct Held<'a> {
    before: &'a Value,
    path: &'a Path,
}

/// The items of one unique rule's list whose field a change may have written.
#[derive(Default)]
enum Touched {
    #[default]
    Nothing,
    Items(BTreeSet<Id>),
    /// The list itself, or something above it, was replaced.
    All,
}

impl Touched {
    /// Records what `op` writes: an inserted item, a write at or under an item's `field`, or a
    /// write at or above the list. Removes and moves write no field.
    fn note(&mut self, op: &Op, list: &[Id], field: &str) {
        let target: Path = match op {
            Op::Set { path, .. } | Op::Unset { path } => path.clone(),
            Op::Insert { list: l, item, .. } => match item_id(item) {
                Some(id) => child(l, id),
                None => return,
            },
            Op::Remove { .. } | Op::Move { .. } => return,
        };
        if target.len() <= list.len() {
            if target[..] == list[..target.len()] {
                *self = Touched::All;
            }
            return;
        }
        let writes_field = target.get(list.len() + 1).is_none_or(|seg| &**seg == field);
        if !target.starts_with(list) || !writes_field {
            return;
        }
        let id = target[list.len()].clone();
        match self {
            Touched::All => {}
            Touched::Items(ids) => {
                ids.insert(id);
            }
            Touched::Nothing => *self = Touched::Items(BTreeSet::from([id])),
        }
    }

    /// A change may not give a touched item a value of `field` that another item of the list
    /// has, unless the item had that same value before the change (same id, same list). So
    /// duplicates already in the document are left alone, and an item put back exactly as it was
    /// passes. The first offending value in list order is `NameTaken`; items without the field,
    /// or with a value that is not a string, are ignored. Free when nothing is touched; otherwise
    /// O(touched) to find the items to judge and, if there are any, one O(n) pass over the list.
    fn check(
        &self,
        before: &Value,
        after: &Value,
        list: &[Id],
        field: &str,
    ) -> Result<(), OpError> {
        if matches!(self, Touched::Nothing) {
            return Ok(());
        }
        let Some(Value::Keyed(now)) = get(after, list) else {
            return Ok(());
        };
        let was = match get(before, list) {
            Some(Value::Keyed(k)) => Some(k),
            _ => None,
        };
        let value_of = |item: &Value| match item {
            Value::Map(m) => match m.get(field) {
                Some(Value::Str(s)) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        };
        // A touched item still in the list with a string value it did not have before.
        let judged = |id: &Id| {
            let value = now.get(id).and_then(value_of)?;
            let old = was.and_then(|k| k.get(id)).and_then(value_of);
            (old.as_ref() != Some(&value)).then_some(value)
        };
        let judged: HashSet<Id> = match self {
            Touched::Nothing => return Ok(()),
            Touched::Items(ids) => ids
                .iter()
                .filter(|id| judged(id).is_some())
                .cloned()
                .collect(),
            Touched::All => now
                .iter()
                .map(|(id, _, _)| id)
                .filter(|id| judged(id).is_some())
                .cloned()
                .collect(),
        };
        if judged.is_empty() {
            return Ok(());
        }
        let mut counts: HashMap<Arc<str>, usize> = HashMap::new();
        for (_, _, item) in now.iter() {
            if let Some(v) = value_of(item) {
                *counts.entry(v).or_default() += 1;
            }
        }
        for (id, _, item) in now.iter() {
            if judged.contains(id)
                && let Some(v) = value_of(item)
                && counts[&v] > 1
            {
                return Err(OpError::NameTaken(v));
            }
        }
        Ok(())
    }
}

/// Whether `v` nests at most `budget` containers deep (as `Value::from_plain` counts: a scalar is
/// 0, a map, list or keyed list one more than its deepest child). Stops early.
fn fits(v: &Value, budget: usize) -> bool {
    let inside = |c: &Value| fits(c, budget - 1);
    match v {
        Value::Map(m) => budget > 0 && m.values().all(inside),
        Value::List(l) => budget > 0 && l.iter().all(inside),
        Value::Keyed(k) => budget > 0 && k.iter().all(|(_, _, c)| inside(c)),
        _ => true,
    }
}

/// A value placed `segments` deep (the final key or item id counted) keeps the document within
/// `MAX_DEPTH` levels: `segments` plus the value's own depth is at most `MAX_DEPTH`.
fn check_depth(segments: usize, value: &Value) -> Result<(), OpError> {
    if segments <= MAX_DEPTH && fits(value, MAX_DEPTH - segments) {
        Ok(())
    } else {
        Err(OpError::Invalid(format!(
            "nesting is deeper than {MAX_DEPTH} levels"
        )))
    }
}

/// Paths, list paths and ids follow the same rule as on the wire (`path::check`,
/// `path::check_name`): `Op::from_wire` enforces it, and hand-built operations skip that, so it is
/// checked here for all five kinds. An over-long or `$list` key or id would leave a document whose
/// wire form cannot be read back; an empty path names nothing.
fn check_names(op: &Op) -> Result<(), OpError> {
    match op {
        Op::Set { path, .. } | Op::Unset { path } => path::check(path, "path"),
        Op::Insert { list, item, .. } => {
            path::check(list, "list")?;
            match item_id(item) {
                Some(id) => path::check_name(id, "id"),
                None => Ok(()), // `apply_one` refuses an item without an id.
            }
        }
        Op::Remove { list, id } | Op::Move { list, id, .. } => {
            path::check(list, "list")?;
            path::check_name(id, "id")
        }
    }
}

/// A value whose wire form does not read back the same (`Value::is_canonical`: a keyed item
/// without its own id, a plain list that reads as keyed, `$list` as a key, an over-long key, ...)
/// is refused. With `check_names`, this means a snapshot of any document `apply` produced from a
/// document that could be read back can be read back too.
fn check_wire_form(value: &Value) -> Result<(), OpError> {
    if value.is_canonical() {
        Ok(())
    } else {
        Err(invalid(
            "the value has no wire form that reads back the same",
        ))
    }
}
