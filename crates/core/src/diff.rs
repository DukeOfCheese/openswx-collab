//! Diff: the smallest list of operations that turns the last synchronised document into the
//! client's current one.
//!
//! The client holds its document as ordinary data with plain lists; `after` is that data. It is
//! classified exactly as `Value::from_plain` does, so a list of maps with distinct string `id`s is
//! a keyed list and is diffed by id.
//!
//! Order of the result: at every level, `Unset`s for keys that went away come first; then, key by
//! key in key order, whole-value `Set`s for new or replaced keys and the diffs of children present
//! on both sides. In a keyed list, `Remove`s come first, then one `Insert` or `Move` for every item
//! that is new or out of place, in final list order, then the recursive diffs of the items present
//! on both sides. Applied in this order every operation finds what it needs.
//!
//! Complexity: one pass over both documents, O(n log n) in the size of the keyed lists (map
//! lookups and a longest increasing subsequence), plus the size of what has to be sent. Unchanged
//! parts are compared by walking them; nothing is shared between the two sides to skip on.

use std::collections::HashMap;

use crate::op::{Op, PROTECTED};
use crate::path::Path;
use crate::pos::Pos;
use crate::value::{Id, KeyedList, Map, Plain, Value, ValueError};

/// Ops that turn `before` into a document whose plain form equals `after`.
/// New positions are chosen between surviving neighbours of `before`.
///
/// Both sides must be maps (a document is). Top-level keys in `PROTECTED` are left alone on both
/// sides. `after` is checked with the limits of `Value::from_plain`, whose error is returned.
pub fn diff(before: &Value, after: &Plain) -> Result<Vec<Op>, ValueError> {
    let after = Value::from_plain(after)?;
    let (Value::Map(b), Value::Map(a)) = (before, &after) else {
        return Err(ValueError::Unsupported);
    };
    let mut ops = Vec::new();
    diff_maps(b, a, &mut Vec::new(), true, &mut ops);
    Ok(ops)
}

fn child(path: &Path, seg: &Id) -> Path {
    let mut p = path.clone();
    p.push(seg.clone());
    p
}

fn diff_maps(before: &Map, after: &Map, path: &mut Path, top: bool, ops: &mut Vec<Op>) {
    let skip = |k: &str| top && PROTECTED.contains(&k);
    for k in before.keys() {
        if !skip(k) && !after.contains_key(k) {
            ops.push(Op::Unset {
                path: child(path, k),
            });
        }
    }
    for (k, a) in after.iter() {
        if skip(k) {
            continue;
        }
        path.push(k.clone());
        match before.get(k) {
            None => ops.push(Op::Set {
                path: path.clone(),
                value: a.clone(),
            }),
            Some(b) => diff_value(b, a, path, ops),
        }
        path.pop();
    }
}

fn diff_value(before: &Value, after: &Value, path: &mut Path, ops: &mut Vec<Op>) {
    match (before, after) {
        (Value::Map(b), Value::Map(a)) => diff_maps(b, a, path, false, ops),
        (Value::Keyed(b), Value::Keyed(a)) => diff_keyed(b, a, path, ops),
        // Everything else (scalars, plain lists, a change of type, a plain list becoming keyed or
        // the reverse) is one whole-value `Set`. `==` tells `Int(1)` from `Float(1.0)`.
        _ => {
            if before != after {
                ops.push(Op::Set {
                    path: path.clone(),
                    value: after.clone(),
                });
            }
        }
    }
}

/// Indices into `seq` of one longest strictly increasing subsequence.
fn longest_increasing(seq: &[usize]) -> Vec<usize> {
    let mut tails: Vec<usize> = Vec::new();
    let mut prev: Vec<Option<usize>> = vec![None; seq.len()];
    for (i, &v) in seq.iter().enumerate() {
        let k = tails.partition_point(|&t| seq[t] < v);
        prev[i] = k.checked_sub(1).map(|j| tails[j]);
        match tails.get_mut(k) {
            Some(slot) => *slot = i,
            None => tails.push(i),
        }
    }
    let mut out = Vec::with_capacity(tails.len());
    let mut at = tails.last().copied();
    while let Some(i) = at {
        out.push(i);
        at = prev[i];
    }
    out.reverse();
    out
}

fn diff_keyed(before: &KeyedList, after: &KeyedList, path: &mut Path, ops: &mut Vec<Op>) {
    // The operations carry ids that `Op::from_wire` would refuse when empty; send such a list whole.
    let has_empty_id = |k: &KeyedList| k.iter().any(|(id, _, _)| id.is_empty());
    if has_empty_id(before) || has_empty_id(after) {
        ops.push(Op::Set {
            path: path.clone(),
            value: Value::Keyed(after.clone()),
        });
        return;
    }

    for (id, _, _) in before.iter() {
        if after.get(id).is_none() {
            ops.push(Op::Remove {
                list: path.clone(),
                id: id.clone(),
            });
        }
    }

    // Survivors ranked by their place in `before` (position, then id: the order `iter` gives).
    let rank: HashMap<&str, usize> = before
        .iter()
        .filter(|(id, _, _)| after.get(id).is_some())
        .enumerate()
        .map(|(r, (id, _, _))| (&**id, r))
        .collect();
    // The final order, and for each survivor in it the rank it had.
    let fin: Vec<(&Id, &Value)> = after.iter().map(|(id, _, v)| (id, v)).collect();
    let survivors: Vec<usize> = (0..fin.len())
        .filter(|&i| rank.contains_key(&**fin[i].0))
        .collect();
    let ranks: Vec<usize> = survivors.iter().map(|&i| rank[&**fin[i].0]).collect();
    // Items on a longest increasing subsequence keep their place.
    let mut stable = vec![false; fin.len()];
    for k in longest_increasing(&ranks) {
        stable[survivors[k]] = true;
    }

    let before_pos = |i: usize| before.pos(fin[i].0).expect("a survivor").clone();
    let mut prev: Option<Pos> = None;
    let mut next = 0; // the nearest stable index after the current one; only ever moves right
    for i in 0..fin.len() {
        if stable[i] {
            prev = Some(before_pos(i));
            continue;
        }
        let place = loop {
            while next < fin.len() && (next <= i || !stable[next]) {
                next += 1;
            }
            let upper = (next < fin.len()).then(|| before_pos(next));
            // Stable neighbours may share a position (concurrent inserts do that); nothing fits
            // between them, so the right-hand one moves too.
            if let (Some(p), Some(q)) = (&prev, &upper)
                && p >= q
            {
                stable[next] = false;
                continue;
            }
            break Pos::between(prev.as_ref(), upper.as_ref());
        };
        let (id, item) = fin[i];
        ops.push(if rank.contains_key(&**id) {
            Op::Move {
                list: path.clone(),
                id: id.clone(),
                pos: place.clone(),
            }
        } else {
            Op::Insert {
                list: path.clone(),
                item: item.clone(),
                pos: place.clone(),
            }
        });
        prev = Some(place);
    }

    // Items on both sides are maps; an item's `id` is what identifies it, so it never differs.
    for (id, _, a) in after.iter() {
        if let (Some(Value::Map(b)), Value::Map(a)) = (before.get(id), a) {
            path.push(id.clone());
            diff_maps(b, a, path, false, ops);
            path.pop();
        }
    }
}
