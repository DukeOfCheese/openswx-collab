mod common;

use std::sync::Arc;

use common::{arb_doc, keyed, load, map, path, to_plain};
use oswx_core::{KeyedList, Op, Plain, Pos, Rules, Value, ValueError, apply, diff};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use serde_json::json;

fn reach(before: &Value, ops: &[Op]) -> Value {
    apply(before, ops, &Rules::default(), None).unwrap().doc
}

/// Diffs `before` against `after` (JSON), checks the ops reach `after`, and returns them.
fn check(before: serde_json::Value, after: serde_json::Value) -> Vec<Op> {
    let (b, a) = (load(before), to_plain(after));
    let ops = diff(&b, &a).unwrap();
    assert_eq!(reach(&b, &ops).to_plain(), a);
    ops
}

fn features(ids: &[&str]) -> serde_json::Value {
    let items: Vec<_> = ids
        .iter()
        .map(|id| json!({"id": id, "params": {"depth": 1}}))
        .collect();
    json!({"units": "mm", "features": items})
}

fn count(ops: &[Op], pick: fn(&Op) -> bool) -> usize {
    ops.iter().filter(|o| pick(o)).count()
}

fn is_move(o: &Op) -> bool {
    matches!(o, Op::Move { .. })
}

#[test]
fn no_change_is_empty() {
    let doc = load(features(&["a", "b", "c"]));
    assert!(diff(&doc, &doc.to_plain()).unwrap().is_empty());
}

#[test]
fn one_param_changed_is_one_set_at_the_full_path() {
    let mut after = features(&["a", "b"]);
    after["features"][1]["params"]["depth"] = json!(7);
    let ops = check(features(&["a", "b"]), after);
    assert_eq!(
        ops,
        vec![Op::Set {
            path: path(&["features", "b", "params", "depth"]),
            value: Value::Int(7)
        }]
    );
}

#[test]
fn appended_feature_is_one_insert_after_the_last() {
    let before = load(features(&["a", "b"]));
    let last = match before.at(&["features"]) {
        Some(Value::Keyed(k)) => k.pos("b").unwrap().clone(),
        _ => panic!("keyed"),
    };
    let ops = check(features(&["a", "b"]), features(&["a", "b", "c"]));
    let [Op::Insert { list, item, pos }] = ops.as_slice() else {
        panic!("one insert, got {ops:?}");
    };
    assert_eq!(list, &path(&["features"]));
    assert!(pos > &last);
    assert!(matches!(item, Value::Map(m) if m.get("id") == Some(&Value::Str(Arc::from("c")))));
}

#[test]
fn removed_feature_is_one_remove() {
    let ops = check(features(&["a", "b", "c"]), features(&["a", "c"]));
    assert_eq!(
        ops,
        vec![Op::Remove {
            list: path(&["features"]),
            id: Arc::from("b")
        }]
    );
}

#[test]
fn two_features_swapped_is_one_move() {
    let ops = check(features(&["a", "b"]), features(&["b", "a"]));
    assert_eq!(ops.len(), 1);
    assert!(is_move(&ops[0]));
}

#[test]
fn reversed_list_moves_all_but_one() {
    let ids = ["a", "b", "c", "d", "e", "f"];
    let mut rev = ids;
    rev.reverse();
    let ops = check(features(&ids), features(&rev));
    assert_eq!(ops.len(), ids.len() - 1);
    assert_eq!(count(&ops, is_move), ids.len() - 1);
}

#[test]
fn change_inside_an_entity_sets_under_the_entity() {
    let doc = |x: i64| {
        json!({"features": [{"id": "f1", "params": {"sketch": {"entities": [
            {"id": "e1", "x": x}, {"id": "e2", "x": 0}]}}}]})
    };
    let ops = check(doc(1), doc(2));
    assert_eq!(
        ops,
        vec![Op::Set {
            path: path(&["features", "f1", "params", "sketch", "entities", "e1", "x"]),
            value: Value::Int(2)
        }]
    );
}

#[test]
fn protected_keys_are_ignored() {
    let before = load(json!({"name": "a", "next_id": 3, "journal": [1], "units": "mm"}));
    let after = to_plain(json!({"name": "b", "counters": {"x": 1}, "rollback": 2, "units": "mm"}));
    assert!(diff(&before, &after).unwrap().is_empty());
}

#[test]
fn int_to_float_is_a_change() {
    let ops = check(json!({"k": 1}), json!({"k": 1.0}));
    assert_eq!(
        ops,
        vec![Op::Set {
            path: path(&["k"]),
            value: Value::Float(1.0)
        }]
    );
}

#[test]
fn removed_and_new_keys() {
    let ops = check(
        json!({"a": 1, "b": 2}),
        json!({"b": 2, "c": {"d": {"e": 1}}}),
    );
    assert_eq!(ops.len(), 2);
    assert!(matches!(&ops[0], Op::Unset { path: p } if p == &path(&["a"])));
    // A new nested map is one Set of the whole subtree at the first missing key.
    assert!(matches!(&ops[1], Op::Set { path: p, .. } if p == &path(&["c"])));
}

#[test]
fn type_changes_at_a_key() {
    let ops = check(json!({"k": {"a": 1}}), json!({"k": 5}));
    assert_eq!(ops.len(), 1);
    let ops = check(json!({"k": 5}), json!({"k": {"a": [{"id": "x"}]}}));
    assert_eq!(ops.len(), 1);
    assert!(matches!(&ops[0], Op::Set { path: p, .. } if p == &path(&["k"])));
}

#[test]
fn empty_to_keyed_and_back() {
    // Adding the first items to an empty list inserts them: two clients doing so concurrently do
    // not conflict.
    let ops = check(json!({"l": []}), json!({"l": [{"id": "a"}, {"id": "b"}]}));
    assert_eq!(ops.len(), 2, "{ops:?}");
    assert!(
        ops.iter()
            .all(|o| matches!(o, Op::Insert { list, .. } if list == &path(&["l"])))
    );
    // Emptying a list removes its items, so a removal is recorded, not a write.
    let before = load(json!({"l": [{"id": "a"}, {"id": "b"}]}));
    let ops = diff(&before, &to_plain(json!({"l": []}))).unwrap();
    assert_eq!(
        ops,
        vec![
            Op::Remove {
                list: path(&["l"]),
                id: Arc::from("a")
            },
            Op::Remove {
                list: path(&["l"]),
                id: Arc::from("b")
            },
        ]
    );
    // The emptied keyed list is unchanged against its own plain form, and takes inserts again.
    let emptied = reach(&before, &ops);
    assert!(matches!(emptied.at(&["l"]), Some(Value::Keyed(k)) if k.is_empty()));
    assert_eq!(diff(&emptied, &emptied.to_plain()).unwrap(), vec![]);
    let ops = diff(&emptied, &to_plain(json!({"l": [{"id": "c"}]}))).unwrap();
    assert!(matches!(ops.as_slice(), [Op::Insert { .. }]), "{ops:?}");
    assert_eq!(
        reach(&emptied, &ops).to_plain(),
        to_plain(json!({"l": [{"id": "c"}]}))
    );
    // An insert into an empty plain list leaves a list that diffs as unchanged.
    let filled = reach(&load(json!({"l": []})), &ops);
    assert_eq!(diff(&filled, &filled.to_plain()).unwrap(), vec![]);
}

#[test]
fn empty_keyed_list_from_the_wire_is_an_empty_list() {
    // The wire may carry an emptied keyed list (`{"$list": []}`); plain data reads it back as `[]`,
    // a plain list. The two are the same to diff.
    let wire = Plain::Map(vec![(
        Plain::from("l"),
        Plain::Map(vec![(Plain::from("$list"), Plain::Array(vec![]))]),
    )]);
    let doc = Value::from_wire(&wire).unwrap();
    assert!(matches!(doc.at(&["l"]), Some(Value::Keyed(k)) if k.is_empty()));
    assert!(matches!(
        Value::from_plain(&doc.to_plain()).unwrap().at(&["l"]),
        Some(Value::List(_))
    ));
    assert_eq!(diff(&doc, &doc.to_plain()).unwrap(), vec![]);
    // A non-empty plain list in place of an empty keyed one is still a whole-value set.
    let ops = diff(&doc, &to_plain(json!({"l": [1]}))).unwrap();
    assert!(matches!(ops.as_slice(), [Op::Set { .. }]), "{ops:?}");
}

#[test]
fn plain_list_and_keyed_list_swap_by_set() {
    let ops = check(json!({"l": [1, 2]}), json!({"l": [{"id": "a"}]}));
    assert_eq!(ops.len(), 1);
    let ops = check(json!({"l": [{"id": "a"}]}), json!({"l": [{"x": 1}]}));
    assert_eq!(ops.len(), 1);
}

#[test]
fn item_moved_and_edited_in_one_diff() {
    let mut after = features(&["c", "a", "b"]);
    after["features"][0]["params"]["depth"] = json!(9);
    let ops = check(features(&["a", "b", "c"]), after);
    assert_eq!(ops.len(), 2);
    assert!(matches!(&ops[0], Op::Move { id, .. } if &**id == "c"));
    assert!(matches!(&ops[1], Op::Set { path: p, .. }
        if p == &path(&["features", "c", "params", "depth"])));
}

fn doc_with_list(items: Vec<(&str, &Pos)>) -> Value {
    let mut list = KeyedList::new();
    for (id, pos) in items {
        let item = map(vec![("id", Value::Str(Arc::from(id)))]);
        list = list.inserted(Arc::from(id), pos.clone(), item);
    }
    map(vec![("l", Value::Keyed(list))])
}

#[test]
fn neighbours_with_equal_positions() {
    let p = Pos::between(None, None);
    let before = doc_with_list(vec![("a", &p), ("c", &p)]);
    // Unchanged: nothing to place.
    assert!(diff(&before, &before.to_plain()).unwrap().is_empty());
    // A new item between two that share a position: the right-hand neighbour moves too.
    let after = to_plain(json!({"l": [{"id": "a"}, {"id": "b"}, {"id": "c"}]}));
    let ops = diff(&before, &after).unwrap();
    assert_eq!(count(&ops, is_move), 1);
    assert_eq!(reach(&before, &ops).to_plain(), after);
    // Same with several items sharing one position, and a reordering among them.
    let before = doc_with_list(vec![("a", &p), ("b", &p), ("c", &p), ("d", &p)]);
    let after =
        to_plain(json!({"l": [{"id": "d"}, {"id": "n"}, {"id": "a"}, {"id": "c"}, {"id": "b"}]}));
    let ops = diff(&before, &after).unwrap();
    assert_eq!(reach(&before, &ops).to_plain(), after);
}

/// Every position in every keyed list of `v` is one `Pos::parse` accepts.
fn positions_parse(v: &Value) -> bool {
    match v {
        Value::Keyed(k) => k
            .iter()
            .all(|(_, p, c)| Pos::parse(p.as_str()).is_some() && positions_parse(c)),
        Value::Map(m) => m.values().all(positions_parse),
        Value::List(l) => l.iter().all(positions_parse),
        _ => true,
    }
}

#[test]
fn neighbours_at_the_length_cap_respread_the_list() {
    let p = |s: String| Pos::parse(&s).unwrap();
    let top = p(format!("V{}", "z".repeat(1023)));
    let (lo, hi) = (p("V".repeat(1023) + "1"), p("V".repeat(1023) + "2"));
    let cases = [
        // Appending after a position of 1024 digits ending in the top digit.
        (
            doc_with_list(vec![("a", &top)]),
            json!({"l": [{"id": "a"}, {"id": "n"}]}),
        ),
        // Inserting between two 1024-digit positions one apart.
        (
            doc_with_list(vec![("a", &lo), ("c", &hi)]),
            json!({"l": [{"id": "a"}, {"id": "n"}, {"id": "c"}]}),
        ),
        // Both, with an item gone and a reordering.
        (
            doc_with_list(vec![("a", &lo), ("c", &hi), ("d", &top)]),
            json!({"l": [{"id": "c"}, {"id": "a"}, {"id": "n"}, {"id": "d"}, {"id": "m"}]}),
        ),
    ];
    for (before, after) in cases {
        let after = to_plain(after);
        let ops = diff(&before, &after).unwrap();
        // Every op survives the wire, positions included.
        for op in &ops {
            assert_eq!(Op::from_wire(&op.to_wire()).as_ref(), Ok(op), "{op:?}");
        }
        let got = reach(&before, &ops);
        assert_eq!(got.to_plain(), after);
        assert!(positions_parse(&got));
        assert_eq!(diff(&got, &got.to_plain()).unwrap(), vec![]);
    }
}

#[test]
fn empty_ids_send_the_list_whole() {
    let ops = check(
        json!({"l": [{"id": "x"}]}),
        json!({"l": [{"id": "x"}, {"id": ""}]}),
    );
    assert_eq!(ops.len(), 1);
    assert!(matches!(&ops[0], Op::Set { .. }));
}

#[test]
fn invalid_after_is_the_value_error() {
    let mut deep = Plain::from(1);
    for _ in 0..65 {
        deep = Plain::Map(vec![(Plain::from("a"), deep)]);
    }
    let before = load(json!({}));
    assert_eq!(diff(&before, &deep), Err(ValueError::TooDeep));
    let bad = Plain::Map(vec![(Plain::from("k"), Plain::F64(f64::NAN))]);
    assert_eq!(diff(&before, &bad), Err(ValueError::NotFinite));
}

#[test]
fn a_document_must_be_a_map() {
    let before = load(json!({}));
    assert!(diff(&before, &Plain::from(1)).is_err());
    assert!(diff(&Value::Int(1), &to_plain(json!({}))).is_err());
}

#[test]
fn large_document_with_one_change_is_one_op() {
    // Cost: O(n log n) over the keyed list (lookups and one longest-increasing-subsequence pass)
    // plus a walk of every item; no pass is quadratic, and unchanged items emit nothing.
    let items = |depth: i64| -> Vec<serde_json::Value> {
        (0..2000)
            .map(|i| {
                let d = if i == 1234 { depth } else { 1 };
                json!({"id": format!("f{i}"), "name": format!("F{i}"), "params": {"depth": d}})
            })
            .collect()
    };
    let ops = check(json!({"features": items(1)}), json!({"features": items(2)}));
    assert_eq!(ops.len(), 1);
}

fn expected(before: &Value, after: &Value) -> Plain {
    let (Value::Map(b), Value::Map(a)) = (before, after) else {
        panic!("documents are maps")
    };
    let mut out = a.clone();
    for k in oswx_core::PROTECTED {
        out.remove(k);
        if let Some(v) = b.get(k) {
            out.insert(Arc::from(k), v.clone());
        }
    }
    Value::Map(out).to_plain()
}

proptest! {
    #[test]
    fn diff_then_apply_reaches_target(a in arb_doc(), b in arb_doc()) {
        let ops = diff(&a, &b.to_plain()).unwrap();
        let got = apply(&a, &ops, &Rules { unique: vec![] }, None).unwrap().doc;
        prop_assert_eq!(got.to_plain(), expected(&a, &b));
    }

    #[test]
    fn diff_of_reordered_lists(order in Just((0..8usize).collect::<Vec<_>>()).prop_shuffle(),
                               keep in prop::collection::vec(any::<bool>(), 8)) {
        let before = keyed((0..8).map(|i| map(vec![("id", Value::Str(Arc::from(format!("i{i}"))))])).collect());
        let before = map(vec![("l", before)]);
        let items: Vec<_> = order.iter().filter(|&&i| keep[i]).map(|i| json!({"id": format!("i{i}")})).collect();
        let after = to_plain(json!({"l": items}));
        let ops = diff(&before, &after).unwrap();
        prop_assert_eq!(reach(&before, &ops).to_plain(), after);
    }
}

#[test]
fn zero_and_negative_zero_differ() {
    let ops = check(json!({"k": 0.0}), json!({"k": -0.0}));
    assert_eq!(ops.len(), 1);
    assert!(matches!(&ops[0], Op::Set { value: Value::Float(f), .. } if f.is_sign_negative()));
    // Inside a plain list too.
    let ops = check(json!({"k": [0.0]}), json!({"k": [-0.0]}));
    assert_eq!(ops.len(), 1);
    // Equal floats emit nothing.
    assert!(check(json!({"k": [0.0, 1.5]}), json!({"k": [0.0, 1.5]})).is_empty());
}

fn ids_of(after: &Plain) -> Vec<String> {
    let Plain::Map(pairs) = after else {
        panic!("map")
    };
    let Plain::Array(items) = &pairs[0].1 else {
        return vec![];
    };
    items
        .iter()
        .map(|i| {
            let Plain::Map(p) = i else { panic!("item") };
            p.iter()
                .find(|(k, _)| k.as_str() == Some("id"))
                .unwrap()
                .1
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect()
}

/// Longest increasing subsequence length, the plain O(n^2) way.
fn lis_len(seq: &[usize]) -> usize {
    let mut best = vec![1usize; seq.len()];
    for i in 0..seq.len() {
        for j in 0..i {
            if seq[j] < seq[i] {
                best[i] = best[i].max(best[j] + 1);
            }
        }
    }
    best.into_iter().max().unwrap_or(0)
}

fn reorder_case_holds(before: &Value, after: &Plain) -> Result<(), TestCaseError> {
    let ops = diff(before, after).unwrap();
    let got = apply(before, &ops, &Rules::default(), None);
    prop_assert!(got.is_ok(), "apply failed: {:?}", got.err());
    prop_assert_eq!(&got.unwrap().doc.to_plain(), after);

    // Bound on moves: items out of order, plus items demoted by tied positions.
    let after_ids = ids_of(after);
    let (survivors, tied) = match before.at(&["l"]) {
        Some(Value::Keyed(k)) => {
            let listed: Vec<_> = k
                .iter()
                .filter(|(id, _, _)| after_ids.iter().any(|a| a == &***id))
                .collect();
            let tied = listed
                .iter()
                .filter(|(_, p, _)| listed.iter().filter(|(_, q, _)| q == p).count() > 1)
                .count();
            (
                listed
                    .iter()
                    .map(|(id, _, _)| id.to_string())
                    .collect::<Vec<_>>(),
                tied,
            )
        }
        _ => (vec![], 0),
    };
    let seq: Vec<usize> = after_ids
        .iter()
        .filter_map(|a| survivors.iter().position(|s| s == a))
        .collect();
    let moves = ops
        .iter()
        .filter(|o| matches!(o, Op::Move { list, .. } if list == &path(&["l"])))
        .count();
    let bound = survivors.len() - lis_len(&seq) + tied;
    prop_assert!(moves <= bound, "{moves} moves, bound {bound}: {ops:?}");
    if tied == 0 {
        // With no tied positions the bound is exact: one move for each item off the longest
        // increasing run, no fewer and no more.
        prop_assert_eq!(moves, bound, "{:?}", ops);
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn reordering_with_ties_converges((before, after) in common::arb_reorder_case(false)) {
        reorder_case_holds(&before, &after)?;
    }

    #[test]
    fn reordering_with_nested_lists_converges((before, after) in common::arb_reorder_case(true)) {
        reorder_case_holds(&before, &after)?;
    }
}

#[test]
fn reorder_generator_reaches_moves_and_mid_list_inserts() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    let strategy = common::arb_reorder_case(false);
    let mut runner = TestRunner::deterministic();
    let cases = 400;
    let mut hits = 0;
    for _ in 0..cases {
        let (before, after) = strategy.new_tree(&mut runner).unwrap().current();
        let ops = diff(&before, &after).unwrap();
        let ids = ids_of(&after);
        let has_move = ops
            .iter()
            .any(|o| matches!(o, Op::Move { list, .. } if list == &path(&["l"])));
        let mid_insert = ops.iter().any(|o| match o {
            Op::Insert { list, item, .. } if list == &path(&["l"]) => {
                let Value::Map(m) = item else { return false };
                let Some(Value::Str(id)) = m.get("id") else {
                    return false;
                };
                ids.iter()
                    .position(|a| a == &**id)
                    .is_some_and(|i| i + 1 < ids.len())
            }
            _ => false,
        });
        if has_move && mid_insert {
            hits += 1;
        }
    }
    assert!(
        hits * 2 >= cases,
        "only {hits} of {cases} cases had a move and a mid-list insert"
    );
}
