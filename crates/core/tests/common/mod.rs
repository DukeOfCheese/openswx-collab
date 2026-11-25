//! Shared test helpers: fixtures, and proptest generators for documents and operations.
#![allow(dead_code)]

pub mod client;

use std::sync::Arc;

use oswx_core::{KeyedList, Op, Path, Plain, Pos, Value};
use proptest::prelude::*;
use proptest::sample::Index;

pub fn to_plain(j: serde_json::Value) -> Plain {
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

/// A document from JSON, through `Value::from_plain`.
pub fn load(j: serde_json::Value) -> Value {
    Value::from_plain(&to_plain(j)).unwrap()
}

pub fn path(segments: &[&str]) -> Path {
    segments.iter().map(|s| Arc::from(*s)).collect()
}

pub fn map(pairs: Vec<(&str, Value)>) -> Value {
    Value::Map(pairs.into_iter().map(|(k, v)| (Arc::from(k), v)).collect())
}

/// A keyed list of `items` (maps with a string `id`), with evenly spread positions.
pub fn keyed(items: Vec<Value>) -> Value {
    let mut list = KeyedList::new();
    let n = items.len();
    for (item, pos) in items.into_iter().zip(Pos::spread(n)) {
        let Value::Map(m) = &item else {
            panic!("an item is a map")
        };
        let Some(Value::Str(id)) = m.get("id") else {
            panic!("an item has a string id")
        };
        list = list.inserted(id.clone(), pos, item.clone());
    }
    Value::Keyed(list)
}

fn str_value(s: &str) -> Value {
    Value::Str(Arc::from(s))
}

pub fn arb_scalar() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        (-100i64..100).prop_map(Value::Int),
        (-1e6f64..1e6).prop_map(Value::Float),
        "[a-z]{0,6}".prop_map(|s| str_value(&s)),
    ]
}

/// Any value, keyed lists included; nested ids are drawn from `u1.c1.*` and a few legacy ones.
pub fn arb_value() -> impl Strategy<Value = Value> {
    arb_scalar().prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(|v| Value::List(Arc::new(v))),
            prop::collection::btree_map("[a-c]", inner.clone(), 0..4).prop_map(|m| {
                Value::Map(m.into_iter().map(|(k, v)| (Arc::from(k), v)).collect())
            }),
            (
                prop::collection::vec(inner, 1..4),
                prop_oneof![Just("u1.c1.n"), Just("legacy")]
            )
                .prop_map(|(vals, stem)| {
                    keyed(
                        vals.into_iter()
                            .enumerate()
                            .map(|(i, v)| {
                                map(vec![("id", str_value(&format!("{stem}{i}"))), ("v", v)])
                            })
                            .collect(),
                    )
                }),
        ]
    })
}

/// A part-like document: scalars, a map, a plain list, and a keyed list `features` of 0..6 items
/// with legacy ids `f<i>`, names `Feature<i>` (in a quarter of the documents pairs share a name,
/// as legacy content may), and a nested keyed (or empty plain) list `params/sketch/dims`.
pub fn arb_doc() -> impl Strategy<Value = Value> {
    let feature = (
        -100i64..100,
        0usize..4,
        arb_value(),
        prop::option::of(arb_scalar()),
    );
    (
        prop::collection::vec(feature, 0..6),
        arb_scalar(),
        arb_scalar(),
        prop::collection::vec(arb_scalar(), 0..3),
        prop::bool::weighted(0.25),
    )
        .prop_map(|(features, units, density, notes, legacy_duplicates)| {
            // Now and then pairs of features share a name, as legacy content may.
            let name = |i: usize| match legacy_duplicates {
                true => format!("Feature{}", i / 2),
                false => format!("Feature{i}"),
            };
            let features = features
                .into_iter()
                .enumerate()
                .map(|(i, (depth, ndims, extra, flag))| {
                    let dims: Vec<Value> = (0..ndims)
                        .map(|j| {
                            map(vec![
                                ("id", str_value(&format!("d{i}_{j}"))),
                                ("v", Value::Int(j as i64)),
                            ])
                        })
                        .collect();
                    let dims = if dims.is_empty() {
                        Value::List(Arc::new(vec![]))
                    } else {
                        keyed(dims)
                    };
                    let mut pairs = vec![
                        ("id", str_value(&format!("f{i}"))),
                        ("name", str_value(&name(i))),
                        (
                            "params",
                            map(vec![
                                ("depth", Value::Int(depth)),
                                ("sketch", map(vec![("dims", dims)])),
                            ]),
                        ),
                        ("extra", extra),
                    ];
                    if let Some(flag) = flag {
                        pairs.push(("flag", flag));
                    }
                    map(pairs)
                })
                .collect::<Vec<_>>();
            let features = if features.is_empty() {
                Value::List(Arc::new(vec![]))
            } else {
                keyed(features)
            };
            map(vec![
                ("units", units),
                ("material", map(vec![("density", density)])),
                ("notes", Value::List(Arc::new(notes))),
                ("features", features),
            ])
        })
}

/// What an operation can aim at in a document.
#[derive(Default)]
struct Targets {
    /// Existing map entries (never an item's `id`).
    keys: Vec<Path>,
    /// Maps, the root included.
    maps: Vec<Path>,
    /// Keyed lists, with their ids and their distinct positions in order.
    lists: Vec<(Path, Vec<Arc<str>>, Vec<Pos>)>,
    /// Empty plain lists.
    empties: Vec<Path>,
}

fn walk(v: &Value, at: &mut Path, in_item: bool, t: &mut Targets) {
    match v {
        Value::Map(m) => {
            t.maps.push(at.clone());
            for (k, child) in m.iter() {
                if in_item && &**k == "id" {
                    continue;
                }
                at.push(k.clone());
                t.keys.push(at.clone());
                if matches!(child, Value::List(l) if l.is_empty()) {
                    t.empties.push(at.clone());
                }
                walk(child, at, false, t);
                at.pop();
            }
        }
        Value::Keyed(list) => {
            let ids = list.iter().map(|(id, _, _)| id.clone()).collect();
            let mut poss: Vec<Pos> = list.iter().map(|(_, p, _)| p.clone()).collect();
            poss.dedup();
            t.lists.push((at.clone(), ids, poss));
            for (id, _, item) in list.iter() {
                at.push(id.clone());
                walk(item, at, true, t);
                at.pop();
            }
        }
        _ => {}
    }
}

/// A position in a list with the given distinct, ordered positions: between neighbours, or now and
/// then on an existing position.
fn pick_pos(poss: &[Pos], a: &Index, b: &Index) -> Pos {
    if !poss.is_empty() && b.index(4) == 0 {
        return poss[a.index(poss.len())].clone();
    }
    let i = a.index(poss.len() + 1);
    let prev = i.checked_sub(1).map(|j| &poss[j]);
    Pos::between(prev, poss.get(i))
}

fn child(p: &Path, seg: &str) -> Path {
    let mut out = p.clone();
    out.push(Arc::from(seg));
    out
}

type Choice = (u8, Index, Index, Index, Value);

fn build_ops(doc: &Value, choices: Vec<Choice>) -> Vec<Op> {
    let mut t = Targets::default();
    walk(doc, &mut Vec::new(), false, &mut t);
    let fresh_key = |i: usize, a: &Index| child(&t.maps[a.index(t.maps.len())], &format!("k{i}"));
    choices
        .into_iter()
        .enumerate()
        .map(|(i, (kind, a, b, c, value))| match kind {
            // Replace an existing entry; a name is drawn from a small pool so that names clash
            // or swap now and then.
            0..=2 if !t.keys.is_empty() => {
                let path = t.keys[a.index(t.keys.len())].clone();
                let value = if path.last().is_some_and(|k| &**k == "name") {
                    str_value(&format!("Feature{}", c.index(6)))
                } else {
                    value
                };
                Op::Set { path, value }
            }
            4 if !t.keys.is_empty() => Op::Unset {
                path: t.keys[a.index(t.keys.len())].clone(),
            },
            5 | 6 => {
                let id = format!("u1.c1.{i}");
                let item = map(vec![("id", str_value(&id)), ("v", value)]);
                let (list, pos) = match c.index(4) {
                    0 if !t.empties.is_empty() => (
                        t.empties[a.index(t.empties.len())].clone(),
                        Pos::between(None, None),
                    ),
                    1 => (fresh_key(i, &a), Pos::between(None, None)),
                    _ if !t.lists.is_empty() => {
                        let (list, _, poss) = &t.lists[a.index(t.lists.len())];
                        (list.clone(), pick_pos(poss, &b, &c))
                    }
                    _ => (fresh_key(i, &a), Pos::between(None, None)),
                };
                Op::Insert { list, item, pos }
            }
            7 if !t.lists.is_empty() => {
                let (list, ids, _) = &t.lists[a.index(t.lists.len())];
                Op::Remove {
                    list: list.clone(),
                    id: ids[b.index(ids.len())].clone(),
                }
            }
            8 | 9 if !t.lists.is_empty() => {
                let (list, ids, poss) = &t.lists[a.index(t.lists.len())];
                Op::Move {
                    list: list.clone(),
                    id: ids[b.index(ids.len())].clone(),
                    pos: pick_pos(poss, &c, &b),
                }
            }
            // A new key (kind 3, or nothing else to aim at); now and then the wire form's list
            // key, which apply must refuse.
            _ => Op::Set {
                path: match c.index(10) {
                    0 => child(&t.maps[a.index(t.maps.len())], "$list"),
                    _ => fresh_key(i, &a),
                },
                value,
            },
        })
        .collect()
}

/// 0..8 operations against `doc`, of all five kinds, mostly applicable in sequence (an op can
/// still fail, for instance by editing inside an item an earlier op removed).
pub fn arb_ops(doc: Value) -> impl Strategy<Value = Vec<Op>> {
    let choice = (
        0u8..10,
        any::<Index>(),
        any::<Index>(),
        any::<Index>(),
        arb_value(),
    );
    prop::collection::vec(choice, 0..8).prop_map(move |choices| build_ops(&doc, choices))
}

/// A document and operations against it.
pub fn arb_doc_and_ops() -> impl Strategy<Value = (Value, Vec<Op>)> {
    arb_doc().prop_flat_map(|doc| (Just(doc.clone()), arb_ops(doc)))
}

/// A document `{"l": ...}` and a target for it, for exercising reordering in a keyed list.
///
/// `before` holds a random subset of the ids `i0..i11`, with positions drawn from only three
/// values so that ties occur. `after` holds another subset (so some ids are fresh, some gone) in
/// a different order, and every item's `v` changes. With `nested`, items also carry a keyed list
/// `sub` of up to three items, reordered or shrunk between the two sides.
pub fn arb_reorder_case(nested: bool) -> impl Strategy<Value = (Value, Plain)> {
    let flags = || prop::collection::vec(prop::bool::weighted(0.75), 12);
    let small = |n: usize| prop::collection::vec(0..n, 12);
    (
        flags(),
        small(3),
        flags(),
        Just((0..12usize).collect::<Vec<_>>()).prop_shuffle(),
        small(4),
        small(8),
        small(8),
        flags(),
    )
        .prop_map(
            move |(in_before, pos_idx, in_after, order, v_after, sub_b, sub_a, rev)| {
                let positions = Pos::spread(3);
                let subs = |mask: usize, reversed: bool| -> Vec<usize> {
                    let mut v: Vec<usize> = (0..3).filter(|j| (mask >> j) & 1 == 1).collect();
                    if reversed {
                        v.reverse();
                    }
                    v
                };
                let mut list = KeyedList::new();
                for i in (0..12).filter(|&i| in_before[i]) {
                    let mut pairs = vec![
                        ("id", str_value(&format!("i{i}"))),
                        ("v", Value::Int(i as i64)),
                    ];
                    if nested {
                        let items: Vec<Value> = subs(sub_b[i], false)
                            .into_iter()
                            .map(|j| {
                                map(vec![
                                    ("id", str_value(&format!("s{j}"))),
                                    ("n", Value::Int(j as i64)),
                                ])
                            })
                            .collect();
                        pairs.push((
                            "sub",
                            if items.is_empty() {
                                Value::List(Arc::new(vec![]))
                            } else {
                                keyed(items)
                            },
                        ));
                    }
                    list = list.inserted(
                        Arc::from(format!("i{i}")),
                        positions[pos_idx[i]].clone(),
                        map(pairs),
                    );
                }
                let before_list = if list.is_empty() {
                    Value::List(Arc::new(vec![]))
                } else {
                    Value::Keyed(list)
                };
                let items: Vec<serde_json::Value> = order
                    .iter()
                    .filter(|&&i| in_after[i])
                    .map(|&i| {
                        let mut item = serde_json::json!({"id": format!("i{i}"), "v": v_after[i] as i64 + 100});
                        if nested {
                            item["sub"] = subs(sub_a[i], rev[i])
                                .into_iter()
                                .map(|j| serde_json::json!({"id": format!("s{j}"), "n": j}))
                                .collect();
                        }
                        item
                    })
                    .collect();
                (
                    map(vec![("l", before_list)]),
                    to_plain(serde_json::json!({ "l": items })),
                )
            },
        )
}
