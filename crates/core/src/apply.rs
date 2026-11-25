//! Applying operations to a document: all or none, with the operations that undo them.
//!
//! Every edit rebuilds only the path it changes; everything else is shared with the input.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use crate::op::{Op, OpError};
use crate::path::Path;
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
    /// Undoes the change when applied in order (with no id prefix).
    pub inverse: Vec<Op>,
}

/// Applies all ops or none. `id_prefix`: Some("<user>.<client>.") for client changes, None for the server.
pub fn apply(
    doc: &Value,
    ops: &[Op],
    rules: &Rules,
    id_prefix: Option<&str>,
) -> Result<Applied, OpError> {
    let mut out = doc.clone();
    let mut inverse = Vec::with_capacity(ops.len());
    let mut introduced = Introduced::default();
    for op in ops {
        let (next, undo) = apply_one(&out, op)?;
        out = next;
        inverse.push(undo);
        if id_prefix.is_some() {
            introduced.note(op);
        }
    }
    inverse.reverse();
    if let Some(prefix) = id_prefix {
        introduced.check(doc, &out, prefix)?;
    }
    for (list, field) in &rules.unique {
        if ops.iter().any(|op| may_clash(op, list, field)) {
            check_unique(&out, list, field)?;
        }
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
    match op {
        Op::Set { path, value } => edit_parent(doc, path, |parent, last, in_item| match parent {
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
        }),
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
/// values keeps the check proportional to the change.
#[derive(Default)]
struct Introduced {
    /// (keyed list, id).
    at: BTreeSet<(Path, Id)>,
    /// Ids in keyed lists inside plain lists, which no path reaches: always new.
    unreachable: Vec<Id>,
}

impl Introduced {
    fn note(&mut self, op: &Op) {
        match op {
            Op::Set { path, value } => self.walk(value, &mut path.clone(), true),
            Op::Insert { list, item, .. } => {
                if let Some(id) = item_id(item) {
                    self.at.insert((list.clone(), id.clone()));
                    self.walk(item, &mut child(list, id), true);
                }
            }
            Op::Unset { .. } | Op::Remove { .. } | Op::Move { .. } => {}
        }
    }

    fn walk(&mut self, v: &Value, path: &mut Path, reachable: bool) {
        match v {
            Value::Map(m) => {
                for (k, child) in m.iter() {
                    path.push(k.clone());
                    self.walk(child, path, reachable);
                    path.pop();
                }
            }
            Value::Keyed(k) => {
                for (id, _, item) in k.iter() {
                    if reachable {
                        self.at.insert((path.clone(), id.clone()));
                    } else {
                        self.unreachable.push(id.clone());
                    }
                    path.push(id.clone());
                    self.walk(item, path, reachable);
                    path.pop();
                }
            }
            Value::List(items) => {
                for item in items.iter() {
                    self.walk(item, path, false);
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

/// Whether `op` can make two items of `list` share a value of `field`: it replaces the list or
/// something above it, inserts or replaces an item, or writes at or under an item's `field`.
/// Removes and moves never can.
fn may_clash(op: &Op, list: &[Id], field: &str) -> bool {
    let target: Path = match op {
        Op::Set { path, .. } | Op::Unset { path } => path.clone(),
        Op::Insert { list: l, item, .. } => match item_id(item) {
            Some(id) => child(l, id),
            None => l.clone(),
        },
        Op::Remove { .. } | Op::Move { .. } => return false,
    };
    if target.len() <= list.len() {
        return target[..] == list[..target.len()];
    }
    target.starts_with(list) && target.get(list.len() + 1).is_none_or(|seg| &**seg == field)
}

/// The string values of `field` across the items of the keyed list at `list` are distinct; the
/// first repeat in list order is `NameTaken`. Items without the field, or with a value that is
/// not a string, are ignored. Costs O(n) in the length of the list.
fn check_unique(doc: &Value, list: &[Id], field: &str) -> Result<(), OpError> {
    let Some(Value::Keyed(k)) = get(doc, list) else {
        return Ok(());
    };
    let mut seen = HashSet::new();
    for (_, _, item) in k.iter() {
        if let Value::Map(m) = item
            && let Some(Value::Str(name)) = m.get(field)
            && !seen.insert(name.clone())
        {
            return Err(OpError::NameTaken(name.clone()));
        }
    }
    Ok(())
}
