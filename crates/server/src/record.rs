//! A journal record: one accepted change as the journal writer stores it, and the MessagePack forms
//! of its operations, paths and reads in the `changes` row.
//!
//! # Stored forms
//!
//! - `ops`, `inverse`: an array of [`Op::to_wire`] maps.
//! - `paths`: a map `{"w": [write, ...], "r": [path, ...]}`, where `w` holds what the change writes
//!   and `r` the items it removes. A path is an array of strings (map keys and keyed-list item ids
//!   from the document root, never empty). A write is either a path (`Write::At`) or a map
//!   `{"pos": [list path, item id]}` (`Write::Position`: the item's place in its list).
//! - `reads`: an array of paths.
//!
//! The decoders are strict: anything but exactly these shapes (another type, a missing, extra or
//! repeated key, an empty or over-deep path, an over-long segment, bytes after the value) is a
//! [`RecordError`], never a panic. Rows are written only by the journal writer, so an error here
//! means the stored row is damaged.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use oswx_core::{MAX_DEPTH, MAX_NAME, Op, Path, Plain, Seq, Write};

use crate::store::Id;

/// How far the MessagePack reader may recurse in a stored value (as the core's frame reader).
const PARSE_DEPTH: usize = 2 * oswx_core::wire::MAX_NESTING + 4;

/// One accepted change, handed to [`crate::journal::JournalWriter::append`].
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub document: Id,
    /// Its seq: the document's head seq plus one when it is written.
    pub seq: Seq,
    /// The seq it follows: `seq - 1`.
    pub parent_seq: Seq,
    /// The room lease epoch the room holds ([`crate::lease::acquire`]).
    pub epoch: u64,
    pub author: Id,
    /// The connection as the conflict index knows it (`<user id>.<client id>`); `None` for a change
    /// the server made. At most 128 characters.
    pub client_id: Option<String>,
    /// At most 64 characters.
    pub change_id: Option<String>,
    /// At most 100 characters.
    pub command: String,
    /// [`encode_ops`] of the operations.
    pub ops: Bytes,
    /// [`encode_ops`] of the inverse.
    pub inverse: Bytes,
    /// [`encode_paths`] of what it writes and removes.
    pub paths: Bytes,
    /// [`encode_reads`] of what it reads.
    pub reads: Bytes,
    /// The seq of the change it undoes (the room resolves the client's `undo_of` change id to it).
    pub undo_of: Option<Seq>,
    /// The seq of the change it reverts from history.
    pub reverts: Option<Seq>,
    pub created_at: f64,
    /// More writes that belong with it, each judged in the record's own savepoint.
    pub extra: Vec<Extra>,
}

/// A write that goes with a record and may be refused on its own.
#[derive(Debug, Clone, PartialEq)]
pub enum Extra {
    /// Marks the earlier change `seq` of the same document as reverted by this record; refused if it
    /// is already reverted (or missing).
    SetRevertedBy { seq: Seq },
    /// The assembly's component references after this change, replacing all it had.
    AssemblyRefs { rows: Vec<AssemblyRef> },
}

/// A component of an assembly and the part it refers to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssemblyRef {
    pub component_id: String,
    pub part_id: Id,
}

/// Why a stored form could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("damaged {what}: {why}")]
pub struct RecordError {
    /// `ops`, `paths` or `reads`.
    pub what: &'static str,
    pub why: String,
}

fn err<T>(what: &'static str, why: impl Into<String>) -> Result<T, RecordError> {
    Err(RecordError {
        what,
        why: why.into(),
    })
}

fn pack(p: &Plain) -> Bytes {
    let mut out = Vec::new();
    rmpv::encode::write_value(&mut out, p).expect("writing to a Vec cannot fail");
    Bytes::from(out)
}

fn unpack(bytes: &[u8], what: &'static str) -> Result<Plain, RecordError> {
    let mut rest = bytes;
    let Ok(p) = rmpv::decode::read_value_with_max_depth(&mut rest, PARSE_DEPTH) else {
        return err(what, "not MessagePack");
    };
    if !rest.is_empty() {
        return err(what, "bytes after the value");
    }
    Ok(p)
}

fn path_to_wire(path: &[Arc<str>]) -> Plain {
    Plain::Array(path.iter().map(|s| Plain::from(&**s)).collect())
}

fn path_from_wire(p: &Plain, what: &'static str) -> Result<Path, RecordError> {
    let Some(items) = p.as_array() else {
        return err(what, "a path is not an array");
    };
    if items.is_empty() || items.len() > MAX_DEPTH {
        return err(what, "a path is empty or too deep");
    }
    items
        .iter()
        .map(|s| match s.as_str() {
            Some(s) if s.chars().count() <= MAX_NAME => Ok(Arc::from(s)),
            _ => err(what, "a path segment is not a short string"),
        })
        .collect()
}

/// The stored form of operations (`ops` and `inverse`).
pub fn encode_ops(ops: &[Op]) -> Bytes {
    pack(&Plain::Array(ops.iter().map(Op::to_wire).collect()))
}

/// The operations [`encode_ops`] wrote.
pub fn decode_ops(bytes: &[u8]) -> Result<Vec<Op>, RecordError> {
    let p = unpack(bytes, "ops")?;
    let Some(items) = p.as_array() else {
        return err("ops", "not an array");
    };
    items
        .iter()
        .map(|op| Op::from_wire(op).or_else(|e| err("ops", e.to_string())))
        .collect()
}

/// The stored form of what a change writes and removes (`paths`).
pub fn encode_paths(writes: &BTreeSet<Write>, removed: &BTreeSet<Path>) -> Bytes {
    let w = writes
        .iter()
        .map(|w| match w {
            Write::At(p) => path_to_wire(p),
            Write::Position(list, id) => Plain::Map(vec![(
                Plain::from("pos"),
                Plain::Array(vec![path_to_wire(list), Plain::from(&**id)]),
            )]),
        })
        .collect();
    let r = removed.iter().map(|p| path_to_wire(p)).collect();
    pack(&Plain::Map(vec![
        (Plain::from("w"), Plain::Array(w)),
        (Plain::from("r"), Plain::Array(r)),
    ]))
}

/// What [`encode_paths`] wrote: the writes and the removed items.
pub fn decode_paths(bytes: &[u8]) -> Result<(BTreeSet<Write>, BTreeSet<Path>), RecordError> {
    const WHAT: &str = "paths";
    let Plain::Map(pairs) = unpack(bytes, WHAT)? else {
        return err(WHAT, "not a map");
    };
    let (mut w, mut r) = (None, None);
    for (k, v) in pairs {
        let slot = match k.as_str() {
            Some("w") => &mut w,
            Some("r") => &mut r,
            _ => return err(WHAT, "an unknown key"),
        };
        if slot.replace(v).is_some() {
            return err(WHAT, "a repeated key");
        }
    }
    let (Some(Plain::Array(w)), Some(Plain::Array(r))) = (w, r) else {
        return err(WHAT, "w and r must both be arrays");
    };
    let mut writes = BTreeSet::new();
    for item in &w {
        let write = match item {
            Plain::Array(_) => Write::At(path_from_wire(item, WHAT)?),
            Plain::Map(pairs) => match pairs.as_slice() {
                [(k, Plain::Array(pos))] if k.as_str() == Some("pos") => match pos.as_slice() {
                    [list, id] => {
                        let list = path_from_wire(list, WHAT)?;
                        let id = match id.as_str() {
                            Some(id) if !id.is_empty() && id.chars().count() <= MAX_NAME => id,
                            _ => return err(WHAT, "a position's id is not a short string"),
                        };
                        Write::Position(list, Arc::from(id))
                    }
                    _ => return err(WHAT, "a position is not [list, id]"),
                },
                _ => return err(WHAT, "a write map is not {\"pos\": [list, id]}"),
            },
            _ => return err(WHAT, "a write is neither a path nor a position"),
        };
        writes.insert(write);
    }
    let removed = r
        .iter()
        .map(|p| path_from_wire(p, WHAT))
        .collect::<Result<_, _>>()?;
    Ok((writes, removed))
}

/// The stored form of what a change reads (`reads`).
pub fn encode_reads(reads: &BTreeSet<Path>) -> Bytes {
    pack(&Plain::Array(
        reads.iter().map(|p| path_to_wire(p)).collect(),
    ))
}

/// What [`encode_reads`] wrote.
pub fn decode_reads(bytes: &[u8]) -> Result<BTreeSet<Path>, RecordError> {
    let p = unpack(bytes, "reads")?;
    let Some(items) = p.as_array() else {
        return err("reads", "not an array");
    };
    items.iter().map(|p| path_from_wire(p, "reads")).collect()
}

#[cfg(test)]
mod tests {
    use oswx_core::{Pos, Value};

    use super::*;

    fn path(segs: &[&str]) -> Path {
        segs.iter().map(|s| Arc::from(*s)).collect()
    }

    fn sample() -> (BTreeSet<Write>, BTreeSet<Path>) {
        let writes = BTreeSet::from([
            Write::At(path(&["features", "f1", "params", "depth"])),
            Write::At(path(&["features", "f2"])),
            Write::Position(path(&["features"]), Arc::from("f1")),
            Write::Position(path(&["a", "list"]), Arc::from("u.c.9")),
        ]);
        let removed = BTreeSet::from([path(&["features", "f2"])]);
        (writes, removed)
    }

    #[test]
    fn paths_round_trip() {
        let (writes, removed) = sample();
        let bytes = encode_paths(&writes, &removed);
        assert_eq!(decode_paths(&bytes).unwrap(), (writes, removed));
        let empty = encode_paths(&BTreeSet::new(), &BTreeSet::new());
        assert_eq!(
            decode_paths(&empty).unwrap(),
            (BTreeSet::new(), BTreeSet::new())
        );
    }

    #[test]
    fn the_paths_encoding_is_exactly_the_documented_one() {
        let writes = BTreeSet::from([
            Write::At(path(&["a", "b"])),
            Write::Position(path(&["l"]), Arc::from("i")),
        ]);
        let removed = BTreeSet::from([path(&["l", "j"])]);
        let p = unpack(&encode_paths(&writes, &removed), "paths").unwrap();
        let s = |v: &str| Plain::from(v);
        let expected = Plain::Map(vec![
            (
                s("w"),
                Plain::Array(vec![
                    Plain::Array(vec![s("a"), s("b")]),
                    Plain::Map(vec![(
                        s("pos"),
                        Plain::Array(vec![Plain::Array(vec![s("l")]), s("i")]),
                    )]),
                ]),
            ),
            (
                s("r"),
                Plain::Array(vec![Plain::Array(vec![s("l"), s("j")])]),
            ),
        ]);
        assert_eq!(p, expected);
    }

    #[test]
    fn reads_and_ops_round_trip() {
        let reads = BTreeSet::from([path(&["features", "f1"]), path(&["x"])]);
        assert_eq!(decode_reads(&encode_reads(&reads)).unwrap(), reads);
        let ops = vec![
            Op::Set {
                path: path(&["features", "f1", "params", "depth"]),
                value: Value::Float(2.5),
            },
            Op::Unset { path: path(&["x"]) },
            Op::Move {
                list: path(&["features"]),
                id: Arc::from("f1"),
                pos: Pos::between(None, None),
            },
        ];
        assert_eq!(decode_ops(&encode_ops(&ops)).unwrap(), ops);
    }

    fn bytes_of(p: Plain) -> Bytes {
        pack(&p)
    }

    #[test]
    fn damaged_paths_are_errors_never_panics() {
        let s = |v: &str| Plain::from(v);
        let arr = Plain::Array;
        let map = |pairs: Vec<(&str, Plain)>| {
            Plain::Map(pairs.into_iter().map(|(k, v)| (s(k), v)).collect())
        };
        let ok_w = arr(vec![arr(vec![s("a")])]);
        let bad = [
            Plain::Nil,
            arr(vec![]),
            map(vec![("w", ok_w.clone())]),
            map(vec![("r", arr(vec![]))]),
            map(vec![
                ("w", ok_w.clone()),
                ("r", arr(vec![])),
                ("x", arr(vec![])),
            ]),
            map(vec![
                ("w", ok_w.clone()),
                ("w", ok_w.clone()),
                ("r", arr(vec![])),
            ]),
            map(vec![("w", s("a")), ("r", arr(vec![]))]),
            map(vec![("w", arr(vec![arr(vec![])])), ("r", arr(vec![]))]),
            map(vec![
                ("w", arr(vec![arr(vec![Plain::from(1)])])),
                ("r", arr(vec![])),
            ]),
            map(vec![
                ("w", arr(vec![arr(vec![s(&"k".repeat(201))])])),
                ("r", arr(vec![])),
            ]),
            map(vec![
                ("w", arr(vec![arr(vec![s("a"); 65])])),
                ("r", arr(vec![])),
            ]),
            map(vec![("w", arr(vec![s("a")])), ("r", arr(vec![]))]),
            map(vec![
                (
                    "w",
                    arr(vec![map(vec![("pos", arr(vec![arr(vec![s("l")])]))])]),
                ),
                ("r", arr(vec![])),
            ]),
            map(vec![
                (
                    "w",
                    arr(vec![map(vec![(
                        "pos",
                        arr(vec![arr(vec![s("l")]), s("")]),
                    )])]),
                ),
                ("r", arr(vec![])),
            ]),
            map(vec![
                (
                    "w",
                    arr(vec![map(vec![(
                        "at",
                        arr(vec![arr(vec![s("l")]), s("i")]),
                    )])]),
                ),
                ("r", arr(vec![])),
            ]),
            map(vec![("w", arr(vec![])), ("r", arr(vec![s("a")]))]),
        ];
        for p in bad {
            assert!(decode_paths(&bytes_of(p.clone())).is_err(), "{p:?}");
        }
        let (writes, removed) = sample();
        let good = encode_paths(&writes, &removed).to_vec();
        let mut trailing = good.clone();
        trailing.push(0xc0);
        assert!(decode_paths(&trailing).is_err());
        for cut in 0..good.len() {
            assert!(decode_paths(&good[..cut]).is_err(), "cut at {cut}");
        }
        for i in 0..good.len() {
            let mut flipped = good.clone();
            flipped[i] ^= 0x5a;
            let _ = decode_paths(&flipped); // any answer but a panic
        }
        assert!(decode_reads(&[0xc1]).is_err());
        assert!(decode_reads(&bytes_of(map(vec![]))).is_err());
        assert!(decode_ops(&bytes_of(arr(vec![map(vec![("op", s("nope"))])]))).is_err());
        assert!(decode_ops(&bytes_of(s("x"))).is_err());
    }
}
