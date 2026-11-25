use std::collections::BTreeSet;
use std::sync::Arc;

use oswx_core::{Op, OpError, PROTECTED, Plain, Pos, Value, Write, removed, writes};

fn s(t: &str) -> Plain {
    Plain::from(t)
}
fn m(pairs: Vec<(&str, Plain)>) -> Plain {
    Plain::Map(pairs.into_iter().map(|(k, v)| (s(k), v)).collect())
}
fn arr(items: &[&str]) -> Plain {
    Plain::Array(items.iter().map(|i| s(i)).collect())
}
fn p(items: &[&str]) -> Vec<Arc<str>> {
    items.iter().map(|i| Arc::from(*i)).collect()
}
fn item(id: &str) -> Plain {
    m(vec![("id", s(id)), ("n", Plain::from(1))])
}
fn set(path: Plain, value: Plain) -> Plain {
    m(vec![("op", s("set")), ("path", path), ("value", value)])
}
fn unset(path: Plain) -> Plain {
    m(vec![("op", s("unset")), ("path", path)])
}
fn insert(list: Plain, it: Plain, pos: Plain) -> Plain {
    m(vec![
        ("op", s("insert")),
        ("list", list),
        ("item", it),
        ("pos", pos),
    ])
}
fn remove(list: Plain, id: Plain) -> Plain {
    m(vec![("op", s("remove")), ("list", list), ("id", id)])
}
fn mv(list: Plain, id: Plain, pos: Plain) -> Plain {
    m(vec![
        ("op", s("move")),
        ("list", list),
        ("id", id),
        ("pos", pos),
    ])
}
fn invalid(r: Result<Op, OpError>) {
    assert!(matches!(r, Err(OpError::Invalid(_))), "got {r:?}");
}
fn nested(depth: usize) -> Plain {
    let mut v = Plain::from(1);
    for _ in 0..depth {
        v = Plain::Array(vec![v]);
    }
    v
}

fn samples() -> Vec<Plain> {
    vec![
        set(arr(&["a", "b"]), Plain::from(1)),
        unset(arr(&["a"])),
        insert(arr(&["feats"]), item("u.c.1"), s("V")),
        remove(arr(&["feats"]), s("u.c.1")),
        mv(arr(&["feats"]), s("u.c.1"), s("V")),
    ]
}

fn ops() -> Vec<Op> {
    samples()
        .iter()
        .map(|w| Op::from_wire(w).unwrap())
        .collect()
}

#[test]
fn round_trips() {
    for w in samples() {
        let op = Op::from_wire(&w).unwrap();
        assert_eq!(op.to_wire(), w);
        assert_eq!(Op::from_wire(&op.to_wire()).unwrap(), op);
    }
}

#[test]
fn keyed_value_round_trips() {
    let v = m(vec![(
        "$list",
        Plain::Array(vec![Plain::Array(vec![s("V"), item("x")])]),
    )]);
    let w = set(arr(&["a"]), v);
    let op = Op::from_wire(&w).unwrap();
    assert!(matches!(
        &op,
        Op::Set {
            value: Value::Keyed(_),
            ..
        }
    ));
    assert_eq!(op.to_wire(), w);
}

#[test]
fn unknown_fields_are_dropped() {
    let w = m(vec![
        ("op", s("set")),
        ("path", arr(&["a"])),
        ("value", Plain::from(1)),
        ("evil", Plain::Boolean(true)),
    ]);
    let op = Op::from_wire(&w).unwrap();
    assert_eq!(op.to_wire(), set(arr(&["a"]), Plain::from(1)));
}

#[test]
fn tilde_segments_are_legal() {
    Op::from_wire(&set(arr(&["~derived"]), Plain::from(1))).unwrap();
}

#[test]
fn bad_paths() {
    invalid(Op::from_wire(&set(Plain::Array(vec![]), Plain::from(1))));
    let long: Vec<String> = (0..65).map(|i| i.to_string()).collect();
    let refs: Vec<&str> = long.iter().map(String::as_str).collect();
    invalid(Op::from_wire(&set(arr(&refs), Plain::from(1))));
    Op::from_wire(&set(arr(&refs[..64]), Plain::from(1))).unwrap();
    invalid(Op::from_wire(&set(
        arr(&[&"x".repeat(201)]),
        Plain::from(1),
    )));
    Op::from_wire(&set(arr(&[&"x".repeat(200)]), Plain::from(1))).unwrap();
    invalid(Op::from_wire(&set(arr(&[""]), Plain::from(1))));
    invalid(Op::from_wire(&set(
        Plain::Array(vec![Plain::from(1)]),
        Plain::from(1),
    )));
    invalid(Op::from_wire(&set(s("a"), Plain::from(1))));
    invalid(Op::from_wire(&set(Plain::Nil, Plain::from(1))));
    // List paths and ids obey the same limits.
    invalid(Op::from_wire(&remove(arr(&[]), s("a"))));
    invalid(Op::from_wire(&remove(arr(&["l"]), s(""))));
    invalid(Op::from_wire(&remove(arr(&["l"]), s(&"x".repeat(201)))));
    invalid(Op::from_wire(&remove(arr(&refs), s("a"))));
}

#[test]
fn the_wire_list_key_is_never_a_segment() {
    // `$list` as a map key would make the document unreadable in its own wire form.
    let l = |segs: &[&str]| arr(segs);
    for path in [&["$list"][..], &["a", "$list"], &["$list", "b"]] {
        invalid(Op::from_wire(&set(l(path), Plain::from(1))));
        invalid(Op::from_wire(&unset(l(path))));
        invalid(Op::from_wire(&insert(l(path), item("u.c.1"), s("V"))));
        invalid(Op::from_wire(&remove(l(path), s("u.c.1"))));
        invalid(Op::from_wire(&mv(l(path), s("u.c.1"), s("V"))));
    }
    invalid(Op::from_wire(&remove(l(&["l"]), s("$list"))));
    invalid(Op::from_wire(&mv(l(&["l"]), s("$list"), s("V"))));
    invalid(Op::from_wire(&insert(l(&["l"]), item("$list"), s("V"))));
    // Similar names are ordinary.
    for seg in ["$lists", "list", "$", "x$list"] {
        Op::from_wire(&set(l(&["a", seg]), Plain::from(1))).unwrap();
        Op::from_wire(&insert(l(&[seg]), item(seg), s("V"))).unwrap();
    }
}

#[test]
fn protected_first_segment() {
    for k in PROTECTED {
        invalid(Op::from_wire(&set(arr(&[k, "x"]), Plain::from(1))));
        invalid(Op::from_wire(&unset(arr(&[k]))));
        invalid(Op::from_wire(&insert(arr(&[k]), item("a"), s("V"))));
        invalid(Op::from_wire(&remove(arr(&[k]), s("a"))));
        invalid(Op::from_wire(&mv(arr(&[k]), s("a"), s("V"))));
    }
    // Only the first segment counts.
    Op::from_wire(&set(arr(&["a", "name"]), Plain::from(1))).unwrap();
}

#[test]
fn bad_inserts() {
    let l = || arr(&["l"]);
    invalid(Op::from_wire(&insert(
        l(),
        m(vec![("n", Plain::from(1))]),
        s("V"),
    )));
    invalid(Op::from_wire(&insert(
        l(),
        m(vec![("id", Plain::from(1))]),
        s("V"),
    )));
    invalid(Op::from_wire(&insert(l(), m(vec![("id", s(""))]), s("V"))));
    invalid(Op::from_wire(&insert(
        l(),
        m(vec![("id", s(&"x".repeat(201)))]),
        s("V"),
    )));
    invalid(Op::from_wire(&insert(l(), s("not a map"), s("V"))));
    invalid(Op::from_wire(&insert(
        l(),
        Plain::Array(vec![item("a")]),
        s("V"),
    )));
}

#[test]
fn bad_positions() {
    let bad = [
        s(""),
        s("a0"),
        s("a-b"),
        s(&"a".repeat(1025)),
        Plain::from(1),
        Plain::Nil,
    ];
    for b in bad {
        invalid(Op::from_wire(&insert(arr(&["l"]), item("a"), b.clone())));
        invalid(Op::from_wire(&mv(arr(&["l"]), s("a"), b)));
    }
}

#[test]
fn wrong_types_and_missing_fields() {
    invalid(Op::from_wire(&Plain::Nil));
    invalid(Op::from_wire(&arr(&["set"])));
    invalid(Op::from_wire(&m(vec![])));
    invalid(Op::from_wire(&m(vec![("op", Plain::from(1))])));
    invalid(Op::from_wire(&m(vec![("op", Plain::Nil)])));
    invalid(Op::from_wire(&m(vec![("op", s("bogus"))])));
    invalid(Op::from_wire(&m(vec![
        ("op", s("SET")),
        ("path", arr(&["a"])),
        ("value", Plain::Nil),
    ])));
    // Each op with each field missing, and with each field the wrong type.
    for w in samples() {
        let Plain::Map(pairs) = &w else {
            unreachable!()
        };
        for i in 0..pairs.len() {
            let mut missing = pairs.clone();
            missing.remove(i);
            invalid(Op::from_wire(&Plain::Map(missing)));
            if pairs[i].0.as_str() != Some("value") {
                let mut wrong = pairs.clone();
                wrong[i].1 = Plain::Boolean(true);
                invalid(Op::from_wire(&Plain::Map(wrong)));
            }
        }
    }
    // A field given twice is ambiguous.
    invalid(Op::from_wire(&m(vec![
        ("op", s("unset")),
        ("path", arr(&["a"])),
        ("path", arr(&["b"])),
    ])));
    // Non-string keys are not known fields.
    invalid(Op::from_wire(&Plain::Map(vec![(Plain::from(1), s("set"))])));
}

#[test]
fn hostile_values() {
    let a = || arr(&["a"]);
    invalid(Op::from_wire(&set(a(), Plain::Binary(vec![1, 2]))));
    invalid(Op::from_wire(&set(a(), Plain::Ext(1, vec![1]))));
    invalid(Op::from_wire(&set(a(), Plain::F64(f64::NAN))));
    invalid(Op::from_wire(&set(a(), Plain::F64(f64::INFINITY))));
    invalid(Op::from_wire(&set(
        a(),
        Plain::Array(vec![Plain::F32(f32::NAN)]),
    )));
    invalid(Op::from_wire(&set(a(), nested(65))));
    Op::from_wire(&set(a(), nested(64))).unwrap();
    // A plain list that plain data would read as keyed is refused; its keyed form is not.
    invalid(Op::from_wire(&set(
        a(),
        Plain::Array(vec![item("u2.c1.1")]),
    )));
    let keyed = m(vec![(
        "$list",
        Plain::Array(vec![Plain::Array(vec![s("V"), item("u2.c1.1")])]),
    )]);
    Op::from_wire(&set(a(), keyed)).unwrap();
    // The inserted item is checked as a value too.
    let nan = m(vec![("id", s("a")), ("x", Plain::F64(f64::NAN))]);
    invalid(Op::from_wire(&insert(arr(&["l"]), nan, s("V"))));
    let bin = m(vec![("id", s("a")), ("b", Plain::Binary(vec![]))]);
    invalid(Op::from_wire(&insert(arr(&["l"]), bin, s("V"))));
}

#[test]
fn writes_of_each_op() {
    let expected: BTreeSet<Write> = [
        Write::At(p(&["a", "b"])),
        Write::At(p(&["a"])),
        Write::At(p(&["feats", "u.c.1"])),
        Write::Position(p(&["feats"]), Arc::from("u.c.1")),
    ]
    .into_iter()
    .collect();
    assert_eq!(writes(&ops()), expected);
    assert!(writes(&[]).is_empty());
}

#[test]
fn writes_per_op() {
    let all = ops();
    let at = BTreeSet::from([Write::At(p(&["feats", "u.c.1"]))]);
    assert_eq!(writes(&all[2..3]), at);
    assert_eq!(writes(&all[3..4]), at);
    assert_eq!(
        writes(&all[4..5]),
        BTreeSet::from([Write::Position(p(&["feats"]), Arc::from("u.c.1"))])
    );
}

#[test]
fn removed_item_paths() {
    assert_eq!(removed(&ops()), BTreeSet::from([p(&["feats", "u.c.1"])]));
    assert!(removed(&[]).is_empty());
}

#[test]
fn pos_is_kept() {
    let Op::Insert { pos, .. } = Op::from_wire(&samples()[2]).unwrap() else {
        panic!()
    };
    assert_eq!(pos, Pos::parse("V").unwrap());
}
