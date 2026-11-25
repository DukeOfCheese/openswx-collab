//! Document values: JSON-like data whose lists of `{"id": ...}` maps are keyed lists.
//!
//! Every container is persistent (`imbl`) or behind an `Arc`, so cloning a value is O(1) and
//! a changed copy shares everything it did not touch.

use std::collections::BTreeSet;
use std::sync::Arc;

use imbl::{OrdMap, OrdSet};

use crate::limits::{MAX_DEPTH, MAX_NAME};
use crate::pos::Pos;

pub type Id = Arc<str>;
/// What arrives from JSON or MessagePack, with ordinary lists.
pub type Plain = rmpv::Value;
pub type Map = OrdMap<Arc<str>, Value>;

/// The one key of a keyed list's wire form; never a map key or a path segment in a document.
pub(crate) const LIST_KEY: &str = "$list";

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(Arc<str>),
    /// An ordinary list, replaced whole.
    List(Arc<Vec<Value>>),
    Map(Map),
    Keyed(KeyedList),
}

#[derive(thiserror::Error, Clone, Debug, PartialEq)]
pub enum ValueError {
    #[error("nesting is deeper than {MAX_DEPTH} levels")]
    TooDeep,
    #[error("NaN and infinity are not allowed")]
    NotFinite,
    #[error("a key or id is longer than {MAX_NAME} characters")]
    KeyTooLong,
    #[error("duplicate id {0}")]
    DuplicateId(Id),
    #[error("malformed keyed list")]
    BadId,
    #[error("unsupported value")]
    Unsupported,
}

/// Items with ids, in the order of their positions.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KeyedList {
    items: OrdMap<Id, (Pos, Value)>,
    order: OrdSet<(Pos, Id)>,
}

impl KeyedList {
    pub fn new() -> KeyedList {
        KeyedList::default()
    }

    pub fn get(&self, id: &str) -> Option<&Value> {
        self.items.get(id).map(|(_, v)| v)
    }

    pub fn pos(&self, id: &str) -> Option<&Pos> {
        self.items.get(id).map(|(p, _)| p)
    }

    /// In order: by position, then id.
    pub fn iter(&self) -> impl Iterator<Item = (&Id, &Pos, &Value)> {
        self.order.iter().filter_map(|(_, id)| {
            let (k, (p, v)) = self.items.get_key_value(id)?;
            Some((k, p, v))
        })
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// A list with `id` at `pos`; an item already under `id` is replaced.
    pub fn inserted(&self, id: Id, pos: Pos, value: Value) -> KeyedList {
        let mut out = match self.removed(&id) {
            Some((rest, _, _)) => rest,
            None => self.clone(),
        };
        out.order.insert((pos.clone(), id.clone()));
        out.items.insert(id, (pos, value));
        out
    }

    /// The list without `id`, with the position and value it had.
    pub fn removed(&self, id: &str) -> Option<(KeyedList, Pos, Value)> {
        let mut out = self.clone();
        let (key, (pos, value)) = out.items.remove_with_key(id)?;
        out.order.remove(&(pos.clone(), key));
        Some((out, pos, value))
    }

    pub fn moved(&self, id: &str, pos: Pos) -> Option<KeyedList> {
        let (rest, _, value) = self.removed(id)?;
        Some(rest.inserted(Arc::from(id), pos, value))
    }

    pub fn updated(&self, id: &str, value: Value) -> Option<KeyedList> {
        let mut out = self.clone();
        out.items.get_mut(id)?.1 = value;
        Some(out)
    }
}

impl Value {
    /// Follows `path` through maps (by key) and keyed lists (by item id).
    pub fn at(&self, path: &[&str]) -> Option<&Value> {
        let Some((first, rest)) = path.split_first() else {
            return Some(self);
        };
        let next = match self {
            Value::Map(m) => m.get(*first)?,
            Value::Keyed(k) => k.get(first)?,
            _ => return None,
        };
        next.at(rest)
    }

    /// Converts ordinary data. A non-empty list of maps with distinct string `id`s becomes a keyed
    /// list with evenly spread positions; such a list with the id `$list` is refused.
    pub fn from_plain(p: &Plain) -> Result<Value, ValueError> {
        from_plain_at(p, 0)
    }

    /// Back to ordinary data: keyed lists become lists.
    pub fn to_plain(&self) -> Plain {
        match self {
            Value::Keyed(k) => Plain::Array(k.iter().map(|(_, _, v)| v.to_plain()).collect()),
            other => other.encode(Value::to_plain),
        }
    }

    /// Whether the wire form gives this value back exactly: `Value::from_wire(&v.to_wire())` is
    /// `Ok(v)`. Values made by `from_wire` and `from_plain` always are; values built by hand may
    /// not be. One walk, nothing allocated. It checks that numbers are finite, nesting is at most
    /// `MAX_DEPTH`, keys and ids are at most `MAX_NAME` characters and never `$list`, every keyed
    /// item is a map whose `id` is its key, and no plain list reads as keyed (`from_plain`'s test).
    pub fn is_canonical(&self) -> bool {
        canonical(self, 0)
    }

    /// Reads the wire form, in which a keyed list is `{"$list": [[pos, item], ...]}`.
    pub fn from_wire(p: &Plain) -> Result<Value, ValueError> {
        from_wire_at(p, 0)
    }

    pub fn to_wire(&self) -> Plain {
        match self {
            Value::Keyed(k) => {
                let entries = k
                    .iter()
                    .map(|(_, pos, v)| Plain::Array(vec![Plain::from(pos.as_str()), v.to_wire()]))
                    .collect();
                Plain::Map(vec![(Plain::from(LIST_KEY), Plain::Array(entries))])
            }
            other => other.encode(Value::to_wire),
        }
    }

    /// Everything but a keyed list; `rec` encodes the children.
    fn encode(&self, rec: fn(&Value) -> Plain) -> Plain {
        match self {
            Value::Null => Plain::Nil,
            Value::Bool(b) => Plain::Boolean(*b),
            Value::Int(i) => Plain::from(*i),
            Value::Float(f) => Plain::F64(*f),
            Value::Str(s) => Plain::from(&**s),
            Value::List(items) => Plain::Array(items.iter().map(rec).collect()),
            Value::Map(m) => {
                Plain::Map(m.iter().map(|(k, v)| (Plain::from(&**k), rec(v))).collect())
            }
            Value::Keyed(_) => unreachable!("keyed lists are encoded by the caller"),
        }
    }
}

fn name_ok(s: &str) -> bool {
    s != LIST_KEY && s.chars().count() <= MAX_NAME
}

/// `Value::is_canonical` for a value inside `depth` containers.
fn canonical(v: &Value, depth: usize) -> bool {
    let room = depth < MAX_DEPTH;
    match v {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Str(_) => true,
        Value::Float(f) => f.is_finite(),
        Value::Map(m) => room && m.iter().all(|(k, c)| name_ok(k) && canonical(c, depth + 1)),
        Value::List(items) => {
            room && !reads_as_keyed(items) && items.iter().all(|i| canonical(i, depth + 1))
        }
        Value::Keyed(k) => {
            room && k.iter().all(|(id, pos, item)| {
                let own_id = matches!(item, Value::Map(m)
                    if matches!(m.get("id"), Some(Value::Str(s)) if s == id));
                name_ok(id)
                    && own_id
                    && Pos::parse(pos.as_str()).is_some()
                    && canonical(item, depth + 1)
            })
        }
    }
}

/// Whether `from_wire` would refuse these plain-list items for looking like a keyed list: all
/// maps with a string `id`, and those ids distinct or one of them too long.
fn reads_as_keyed(items: &[Value]) -> bool {
    let ids: Option<Vec<&str>> = items
        .iter()
        .map(|i| match i {
            Value::Map(m) => match m.get("id") {
                Some(Value::Str(s)) => Some(&**s),
                _ => None,
            },
            _ => None,
        })
        .collect();
    match ids {
        Some(ids) if !ids.is_empty() => {
            ids.iter().any(|id| id.chars().count() > MAX_NAME)
                || ids.iter().collect::<BTreeSet<_>>().len() == ids.len()
        }
        _ => false,
    }
}

fn enter(depth: usize) -> Result<usize, ValueError> {
    if depth + 1 > MAX_DEPTH {
        Err(ValueError::TooDeep)
    } else {
        Ok(depth + 1)
    }
}

fn check_name(s: &str) -> Result<(), ValueError> {
    if s.chars().count() > MAX_NAME {
        Err(ValueError::KeyTooLong)
    } else {
        Ok(())
    }
}

fn plain_str(p: &Plain) -> Result<&str, ValueError> {
    p.as_str().ok_or(ValueError::Unsupported)
}

fn finite(f: f64) -> Result<Value, ValueError> {
    if f.is_finite() {
        Ok(Value::Float(f))
    } else {
        Err(ValueError::NotFinite)
    }
}

/// A scalar's value; `None` for a list or map.
fn scalar(p: &Plain) -> Result<Option<Value>, ValueError> {
    Ok(Some(match p {
        Plain::Nil => Value::Null,
        Plain::Boolean(b) => Value::Bool(*b),
        Plain::Integer(i) => Value::Int(i.as_i64().ok_or(ValueError::Unsupported)?),
        Plain::F32(f) => finite(f64::from(*f))?,
        Plain::F64(f) => finite(*f)?,
        Plain::String(_) => Value::Str(Arc::from(plain_str(p)?)),
        Plain::Binary(_) | Plain::Ext(..) => return Err(ValueError::Unsupported),
        Plain::Array(_) | Plain::Map(_) => return Ok(None),
    }))
}

/// A map's entries with checked keys. Repeated keys are refused rather than resolved.
fn entries(pairs: &[(Plain, Plain)]) -> Result<Vec<(&str, &Plain)>, ValueError> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(pairs.len());
    for (k, v) in pairs {
        let key = plain_str(k)?;
        check_name(key)?;
        if !seen.insert(key) {
            return Err(ValueError::Unsupported);
        }
        out.push((key, v));
    }
    Ok(out)
}

fn to_map(
    pairs: &[(&str, &Plain)],
    mut conv: impl FnMut(&Plain) -> Result<Value, ValueError>,
) -> Result<Map, ValueError> {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(Arc::from(*k), conv(v)?);
    }
    Ok(m)
}

/// The `id` of a plain item, if it is a map with a string `id`.
fn item_id(p: &Plain) -> Option<&str> {
    let Plain::Map(pairs) = p else { return None };
    pairs
        .iter()
        .find(|(k, _)| k.as_str() == Some("id"))
        .and_then(|(_, v)| v.as_str())
}

fn from_plain_at(p: &Plain, depth: usize) -> Result<Value, ValueError> {
    if let Some(v) = scalar(p)? {
        return Ok(v);
    }
    let depth = enter(depth)?;
    match p {
        Plain::Map(pairs) => {
            let pairs = entries(pairs)?;
            if pairs.iter().any(|(k, _)| *k == LIST_KEY) {
                return Err(ValueError::Unsupported);
            }
            Ok(Value::Map(to_map(&pairs, |v| from_plain_at(v, depth))?))
        }
        Plain::Array(items) => {
            let ids: Option<Vec<&str>> = items.iter().map(item_id).collect();
            if let Some(ids) = ids.filter(|ids| !ids.is_empty()) {
                for id in &ids {
                    check_name(id)?;
                }
                if ids.iter().collect::<BTreeSet<_>>().len() == ids.len() {
                    if ids.contains(&LIST_KEY) {
                        return Err(ValueError::Unsupported);
                    }
                    let mut list = KeyedList::new();
                    for ((id, item), pos) in ids.iter().zip(items).zip(Pos::spread(items.len())) {
                        list = list.inserted(Arc::from(*id), pos, from_plain_at(item, depth)?);
                    }
                    return Ok(Value::Keyed(list));
                }
            }
            let items = items
                .iter()
                .map(|i| from_plain_at(i, depth))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::List(Arc::new(items)))
        }
        _ => unreachable!("scalars returned above"),
    }
}

fn from_wire_at(p: &Plain, depth: usize) -> Result<Value, ValueError> {
    if let Some(v) = scalar(p)? {
        return Ok(v);
    }
    let depth = enter(depth)?;
    match p {
        Plain::Map(pairs) => {
            let pairs = entries(pairs)?;
            if let [(LIST_KEY, list)] = pairs.as_slice() {
                return keyed_from_wire(list, depth);
            }
            // A map never holds `$list` beside other keys; it would read back as something else.
            if pairs.iter().any(|(k, _)| *k == LIST_KEY) {
                return Err(ValueError::Unsupported);
            }
            Ok(Value::Map(to_map(&pairs, |v| from_wire_at(v, depth))?))
        }
        Plain::Array(items) => {
            // A list `from_plain` would make keyed is only ever sent in keyed form, so that the
            // wire form and the plain form of a document always agree.
            let ids: Option<Vec<&str>> = items.iter().map(item_id).collect();
            if let Some(ids) = ids.filter(|ids| !ids.is_empty()) {
                for id in &ids {
                    check_name(id)?;
                }
                if ids.iter().collect::<BTreeSet<_>>().len() == ids.len() {
                    return Err(ValueError::Unsupported);
                }
            }
            let items = items
                .iter()
                .map(|i| from_wire_at(i, depth))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::List(Arc::new(items)))
        }
        _ => unreachable!("scalars returned above"),
    }
}

fn keyed_from_wire(list: &Plain, depth: usize) -> Result<Value, ValueError> {
    let Plain::Array(entries) = list else {
        return Err(ValueError::BadId);
    };
    let mut out = KeyedList::new();
    let mut last: Option<(Pos, Arc<str>)> = None;
    for entry in entries {
        let Plain::Array(pair) = entry else {
            return Err(ValueError::BadId);
        };
        let [pos, item] = pair.as_slice() else {
            return Err(ValueError::BadId);
        };
        let pos = pos.as_str().and_then(Pos::parse).ok_or(ValueError::BadId)?;
        let id: Id = Arc::from(item_id(item).ok_or(ValueError::BadId)?);
        check_name(&id)?;
        if &*id == LIST_KEY {
            return Err(ValueError::Unsupported);
        }
        if out.get(&id).is_some() {
            return Err(ValueError::DuplicateId(id));
        }
        // The wire form is written in order; anything else is ambiguous.
        let key = (pos, id);
        if last.as_ref().is_some_and(|l| *l >= key) {
            return Err(ValueError::BadId);
        }
        let (pos, id) = key.clone();
        last = Some(key);
        out = out.inserted(id, pos, from_wire_at(item, depth)?);
    }
    Ok(Value::Keyed(out))
}
