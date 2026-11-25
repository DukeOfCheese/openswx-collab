//! What the convergence tests share: a seeded random generator, the starting document, and random
//! edits a simulated client ([`super::client::Client`]) makes on its live document. Used by the core's
//! `tests/convergence.rs` and, through `#[path]`, by the server's socket-driven convergence test.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use oswx_core::{Op, Path, Plain, Pos, Value};
use serde_json::json;

use super::client::Client;

/// A small seeded generator (SplitMix64), so that a run is the same on every machine and the
/// crates need no extra dependency.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`; `n` > 0.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    /// Uniform in [0, 1).
    pub fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }
}

fn to_plain(j: serde_json::Value) -> Plain {
    match j {
        serde_json::Value::Null => Plain::Nil,
        serde_json::Value::Bool(b) => Plain::Boolean(b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => Plain::from(i),
            None => Plain::F64(n.as_f64().unwrap()),
        },
        serde_json::Value::String(s) => Plain::from(s),
        serde_json::Value::Array(a) => Plain::Array(a.into_iter().map(to_plain).collect()),
        serde_json::Value::Object(o) => Plain::Map(
            o.into_iter()
                .map(|(k, v)| (Plain::from(k), to_plain(v)))
                .collect(),
        ),
    }
}

fn load(j: serde_json::Value) -> Value {
    Value::from_plain(&to_plain(j)).unwrap()
}

fn path(segments: &[&str]) -> Path {
    segments.iter().map(|s| Arc::from(*s)).collect()
}

/// The starting document, as JSON. (The server drops its `name` when the document is created: a
/// client there starts from the welcome.)
pub fn initial_json() -> serde_json::Value {
    json!({
        "name": "Part1",
        "variables": {"x": 1, "y": 2},
        "features": [
            {"id": "f1", "name": "Sketch1", "type": "Sketch", "params": {"a": 1, "b": 2}},
            {"id": "f2", "name": "Extrude1", "type": "Extrude", "params": {"a": 3, "b": 4}},
            {"id": "f3", "name": "Fillet1", "type": "Fillet", "params": {"a": 5}},
        ],
    })
}

/// The starting document.
pub fn initial() -> Value {
    load(initial_json())
}

/// The features of a document: (id, position, name), in order.
pub fn features(doc: &Value) -> Vec<(Arc<str>, Pos, Arc<str>)> {
    let Some(Value::Keyed(list)) = doc.at(&["features"]) else {
        return Vec::new();
    };
    list.iter()
        .map(|(id, pos, item)| {
            let name = match item.at(&["name"]) {
                Some(Value::Str(s)) => s.clone(),
                _ => Arc::from(""),
            };
            (id.clone(), pos.clone(), name)
        })
        .collect()
}

/// A position in a random gap between the given positions (any order, repeats allowed).
fn random_pos(rng: &mut Rng, positions: impl Iterator<Item = Pos>) -> Pos {
    let distinct: Vec<Pos> = positions.collect::<BTreeSet<_>>().into_iter().collect();
    let gap = rng.below(distinct.len() + 1);
    Pos::between(gap.checked_sub(1).map(|i| &distinct[i]), distinct.get(gap))
}

/// The first of `Sketch1`, `Sketch2`, ... (for `kind` Sketch) that no feature of `doc` has.
fn free_name(doc: &Value, kind: &str) -> Arc<str> {
    let taken: HashSet<Arc<str>> = features(doc).into_iter().map(|(_, _, n)| n).collect();
    (1..)
        .map(|n| Arc::from(format!("{kind}{n}")))
        .find(|n| !taken.contains(n))
        .unwrap()
}

fn feature_path(id: &str, rest: &[&str]) -> Path {
    let mut p = path(&["features", id]);
    p.extend(rest.iter().map(|s| Arc::from(*s)));
    p
}

/// A random edit of the client's live document: its operations and what it reads. Edits cluster
/// on a few features and fields so that clients get in each other's way.
pub fn random_edit(rng: &mut Rng, client: &mut Client) -> (Vec<Op>, BTreeSet<Path>) {
    let live = client.live.clone();
    let feats = features(&live);
    let pick = |rng: &mut Rng| feats[rng.below(feats.len())].0.clone();
    let roll = rng.unit();
    let kinds = ["Sketch", "Extrude", "Fillet"];
    if feats.len() < 2 || (0.55..0.7).contains(&roll) {
        // Insert a feature, now and then one that refers to another.
        let id = client.new_item_id();
        let kind = kinds[rng.below(kinds.len())];
        let mut reads = BTreeSet::new();
        let mut item =
            json!({"id": &*id, "name": &*free_name(&live, kind), "type": kind, "params": {"a": 0}});
        if !feats.is_empty() && rng.chance(0.5) {
            let other = pick(rng);
            item["ref"] = json!(&*other);
            reads.insert(path(&["features", &other]));
        }
        let pos = random_pos(rng, feats.iter().map(|(_, p, _)| p.clone()));
        let op = Op::Insert {
            list: path(&["features"]),
            item: load(item),
            pos,
        };
        return (vec![op], reads);
    }
    let ops = if roll < 0.45 {
        // Set a parameter, sometimes suppressing the feature in the same change.
        let id = pick(rng);
        let key = ["a", "b"][rng.below(2)];
        let mut ops = vec![Op::Set {
            path: feature_path(&id, &["params", key]),
            value: Value::Int(rng.below(10) as i64),
        }];
        if rng.chance(0.2) {
            ops.push(Op::Set {
                path: feature_path(&id, &["suppressed"]),
                value: Value::Bool(rng.chance(0.5)),
            });
        }
        ops
    } else if roll < 0.55 {
        // Rename to a free name.
        let id = pick(rng);
        let kind = kinds[rng.below(kinds.len())];
        vec![Op::Set {
            path: feature_path(&id, &["name"]),
            value: Value::Str(free_name(&live, kind)),
        }]
    } else if roll < 0.8 && feats.len() >= 3 {
        vec![Op::Remove {
            list: path(&["features"]),
            id: pick(rng),
        }]
    } else if roll < 0.9 {
        let id = pick(rng);
        let others = feats
            .iter()
            .filter(|(f, _, _)| *f != id)
            .map(|(_, p, _)| p.clone());
        let pos = random_pos(rng, others);
        vec![Op::Move {
            list: path(&["features"]),
            id,
            pos,
        }]
    } else {
        let var = ["x", "y", "z"][rng.below(3)];
        vec![Op::Set {
            path: path(&["variables", var]),
            value: Value::Int(rng.below(100) as i64),
        }]
    };
    (ops, BTreeSet::new())
}
