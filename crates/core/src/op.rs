//! Operations: what a client sends to change a document, rebuilt from known fields only.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::path::{self, Path};
use crate::pos::Pos;
use crate::value::{Id, LIST_KEY, Plain, Value};

pub use crate::limits::{MAX_DEPTH, MAX_NAME};
/// Top-level keys the server owns; clients never write them.
pub const PROTECTED: [&str; 7] = [
    "rollback",
    "active_config",
    "display_state",
    "journal",
    "name",
    "next_id",
    "counters",
];

#[derive(thiserror::Error, Clone, Debug, PartialEq)]
pub enum OpError {
    #[error("invalid operation: {0}")]
    Invalid(String),
    #[error("nothing at {0:?}")]
    Missing(Path),
    #[error("name {0} is taken")]
    NameTaken(Arc<str>),
    #[error("id {0} is taken")]
    IdTaken(Id),
    #[error("id {0} was not made by this client")]
    ForeignId(Id),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Set {
        path: Path,
        value: Value,
    },
    Unset {
        path: Path,
    },
    /// `item` is a map with a string `id`.
    Insert {
        list: Path,
        item: Value,
        pos: Pos,
    },
    Remove {
        list: Path,
        id: Id,
    },
    Move {
        list: Path,
        id: Id,
        pos: Pos,
    },
}

/// What an operation writes, for conflict detection.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Write {
    At(Path),
    /// An item's place in its list.
    Position(Path, Id),
}

fn invalid(reason: &str) -> OpError {
    OpError::Invalid(reason.to_string())
}

/// The value of a known field; a field given twice is refused.
fn field<'a>(pairs: &'a [(Plain, Plain)], key: &str) -> Result<&'a Plain, OpError> {
    let mut found = pairs.iter().filter(|(k, _)| k.as_str() == Some(key));
    let first = found
        .next()
        .ok_or_else(|| OpError::Invalid(format!("missing field {key}")))?;
    if found.next().is_some() {
        return Err(OpError::Invalid(format!("repeated field {key}")));
    }
    Ok(&first.1)
}

fn protected(path: &Path) -> Result<(), OpError> {
    match path.first() {
        Some(first) if PROTECTED.contains(&&**first) => {
            Err(OpError::Invalid(format!("{first} cannot be changed")))
        }
        _ => Ok(()),
    }
}

fn parse_pos(p: &Plain) -> Result<Pos, OpError> {
    p.as_str()
        .and_then(Pos::parse)
        .ok_or_else(|| invalid("pos is not a position"))
}

fn parse_value(p: &Plain) -> Result<Value, OpError> {
    Value::from_wire(p).map_err(|e| OpError::Invalid(format!("bad value: {e}")))
}

/// The id of an inserted item, if it is a map with a string `id` that is a name (`check_name`).
fn item_id(item: &Value) -> Option<&Id> {
    match item {
        Value::Map(m) => match m.get("id") {
            Some(Value::Str(id)) if path::check_name(id, "an item id").is_ok() => Some(id),
            _ => None,
        },
        _ => None,
    }
}

impl Op {
    pub fn from_wire(p: &Plain) -> Result<Op, OpError> {
        let Plain::Map(pairs) = p else {
            return Err(invalid("an operation must be a map"));
        };
        let kind = field(pairs, "op")?
            .as_str()
            .ok_or_else(|| invalid("op must be a string"))?;
        match kind {
            "set" => {
                let path = path::from_wire(field(pairs, "path")?, "path")?;
                protected(&path)?;
                let value = parse_value(field(pairs, "value")?)?;
                Ok(Op::Set { path, value })
            }
            "unset" => {
                let path = path::from_wire(field(pairs, "path")?, "path")?;
                protected(&path)?;
                Ok(Op::Unset { path })
            }
            "insert" => {
                let list = path::from_wire(field(pairs, "list")?, "list")?;
                protected(&list)?;
                let item = parse_value(field(pairs, "item")?)?;
                if !matches!(item, Value::Map(_)) {
                    return Err(invalid("item must be a map"));
                }
                match item_id(&item) {
                    None => return Err(invalid("item needs a string id")),
                    Some(id) if &**id == LIST_KEY => {
                        return Err(invalid("an item id cannot be $list"));
                    }
                    Some(_) => {}
                }
                let pos = parse_pos(field(pairs, "pos")?)?;
                Ok(Op::Insert { list, item, pos })
            }
            "remove" => {
                let list = path::from_wire(field(pairs, "list")?, "list")?;
                protected(&list)?;
                let id = path::name(field(pairs, "id")?, "id")?;
                Ok(Op::Remove { list, id })
            }
            "move" => {
                let list = path::from_wire(field(pairs, "list")?, "list")?;
                protected(&list)?;
                let id = path::name(field(pairs, "id")?, "id")?;
                let pos = parse_pos(field(pairs, "pos")?)?;
                Ok(Op::Move { list, id, pos })
            }
            _ => Err(invalid("unknown op")),
        }
    }

    pub fn to_wire(&self) -> Plain {
        fn entry(key: &str, v: Plain) -> (Plain, Plain) {
            (Plain::from(key), v)
        }
        let s = Plain::from;
        let pairs = match self {
            Op::Set { path, value } => vec![
                entry("op", s("set")),
                entry("path", path::to_wire(path)),
                entry("value", value.to_wire()),
            ],
            Op::Unset { path } => vec![entry("op", s("unset")), entry("path", path::to_wire(path))],
            Op::Insert { list, item, pos } => vec![
                entry("op", s("insert")),
                entry("list", path::to_wire(list)),
                entry("item", item.to_wire()),
                entry("pos", s(pos.as_str())),
            ],
            Op::Remove { list, id } => vec![
                entry("op", s("remove")),
                entry("list", path::to_wire(list)),
                entry("id", s(id)),
            ],
            Op::Move { list, id, pos } => vec![
                entry("op", s("move")),
                entry("list", path::to_wire(list)),
                entry("id", s(id)),
                entry("pos", s(pos.as_str())),
            ],
        };
        Plain::Map(pairs)
    }
}

fn child(list: &Path, id: &Id) -> Path {
    let mut p = list.clone();
    p.push(id.clone());
    p
}

pub fn writes(ops: &[Op]) -> BTreeSet<Write> {
    let mut out = BTreeSet::new();
    for op in ops {
        match op {
            Op::Set { path, .. } | Op::Unset { path } => {
                out.insert(Write::At(path.clone()));
            }
            Op::Insert { list, item, .. } => {
                if let Some(id) = item_id(item) {
                    out.insert(Write::At(child(list, id)));
                }
            }
            Op::Remove { list, id } => {
                out.insert(Write::At(child(list, id)));
            }
            Op::Move { list, id, .. } => {
                out.insert(Write::Position(list.clone(), id.clone()));
            }
        }
    }
    out
}

/// Paths of the items the operations remove.
pub fn removed(ops: &[Op]) -> BTreeSet<Path> {
    ops.iter()
        .filter_map(|op| match op {
            Op::Remove { list, id } => Some(child(list, id)),
            _ => None,
        })
        .collect()
}
