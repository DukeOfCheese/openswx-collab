mod common;

use std::sync::Arc;

use common::{arb_doc_and_ops, load, path, to_plain};
use oswx_core::{Applied, Op, OpError, Path, Pos, Rules, Value, apply, apply_with, diff};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use serde_json::json;

/// The Python tests' `PART` (tests/conftest.py).
fn part() -> Value {
    load(json!({
        "format": "openswx-part", "version": 3, "name": "Bracket", "units": "mm",
        "material": {"name": null, "density": 1000.0}, "rollback": null, "active_config": "Default",
        "next_id": 3, "counters": {"Sketch": 1, "Boss-Extrude": 1}, "variables": {},
        "features": [
            {"id": "f1", "name": "Sketch1", "type": "sketch", "suppressed": false,
             "params": {"plane": "Top", "sketch": {"points": {"P1": [0, 0]}, "entities": {}, "counter": 1}}},
            {"id": "f2", "name": "Boss-Extrude1", "type": "extrude", "suppressed": false,
             "params": {"sketch": "Sketch1", "depth": 10}},
        ],
    }))
}

fn rules() -> Rules {
    Rules::openswx()
}

fn set(p: &[&str], v: impl Into<serde_json::Value>) -> Op {
    Op::Set {
        path: path(p),
        value: load(v.into()),
    }
}

fn unset(p: &[&str]) -> Op {
    Op::Unset { path: path(p) }
}

fn pos(s: &str) -> Pos {
    Pos::parse(s).unwrap()
}

fn item(id: &str) -> Value {
    load(json!({"id": id, "name": format!("N-{id}")}))
}

fn insert(list: &[&str], id: &str, at: &str) -> Op {
    Op::Insert {
        list: path(list),
        item: item(id),
        pos: pos(at),
    }
}

fn remove(list: &[&str], id: &str) -> Op {
    Op::Remove {
        list: path(list),
        id: Arc::from(id),
    }
}

fn mv(list: &[&str], id: &str, at: &str) -> Op {
    Op::Move {
        list: path(list),
        id: Arc::from(id),
        pos: pos(at),
    }
}

fn run(doc: &Value, ops: &[Op]) -> Result<Applied, OpError> {
    apply(doc, ops, &rules(), None)
}

fn client(doc: &Value, ops: &[Op]) -> Result<Applied, OpError> {
    apply(doc, ops, &rules(), Some("u1.c1."))
}

/// Applies `ops`, checks that the inverse restores `doc` exactly, and returns the result.
fn round_trip(doc: &Value, ops: &[Op]) -> Value {
    let a = run(doc, ops).unwrap();
    let back = run(&a.doc, &a.inverse).unwrap();
    assert_eq!(&back.doc, doc);
    a.doc
}

fn missing(r: Result<Applied, OpError>, p: &[&str]) {
    assert_eq!(r.err(), Some(OpError::Missing(path(p))));
}

fn invalid(r: Result<Applied, OpError>) {
    assert!(matches!(r, Err(OpError::Invalid(_))), "got {r:?}");
}

fn ids(doc: &Value, list: &[&str]) -> Vec<(String, String)> {
    let Some(Value::Keyed(k)) = doc.at(list) else {
        panic!("not a keyed list")
    };
    k.iter()
        .map(|(id, p, _)| (id.to_string(), p.as_str().to_string()))
        .collect()
}

#[test]
fn set_and_inverse() {
    let a = run(&part(), &[set(&["features", "f2", "params", "depth"], 25)]).unwrap();
    assert_eq!(
        a.doc.at(&["features", "f2", "params", "depth"]),
        Some(&Value::Int(25))
    );
    assert_eq!(run(&a.doc, &a.inverse).unwrap().doc, part());
}

#[test]
fn set_creating_a_key_is_undone_by_unset() {
    let a = run(&part(), &[set(&["variables", "w"], 4)]).unwrap();
    assert_eq!(a.inverse, vec![unset(&["variables", "w"])]);
    assert_eq!(run(&a.doc, &a.inverse).unwrap().doc, part());
}

#[test]
fn unset_and_inverse() {
    let doc = round_trip(&part(), &[unset(&["features", "f1", "params", "plane"])]);
    assert_eq!(doc.at(&["features", "f1", "params", "plane"]), None);
    missing(
        run(&part(), &[unset(&["variables", "w"])]),
        &["variables", "w"],
    );
}

#[test]
fn inverse_undoes_a_sequence_in_reverse() {
    let ops = [
        set(&["variables", "w"], 1),
        set(&["variables", "w"], 2),
        unset(&["variables", "w"]),
        insert(&["features"], "u1.c1.1", "z"),
        set(&["features", "u1.c1.1", "name"], "Late"),
        mv(&["features"], "u1.c1.1", "1"),
        remove(&["features"], "u1.c1.1"),
    ];
    let a = run(&part(), &ops).unwrap();
    assert_eq!(a.doc, part());
    assert_eq!(a.inverse.len(), ops.len());
    assert_eq!(run(&a.doc, &a.inverse).unwrap().doc, part());
}

#[test]
fn legacy_ids_can_be_edited_but_not_introduced() {
    client(
        &part(),
        &[set(&["features", "f1", "params", "plane"], "Front")],
    )
    .unwrap();
    assert_eq!(
        client(&part(), &[insert(&["features"], "f9", "z")]).err(),
        Some(OpError::ForeignId(Arc::from("f9")))
    );
    client(&part(), &[insert(&["features"], "u1.c1.1", "z")]).unwrap();
    assert_eq!(
        client(&part(), &[insert(&["features"], "u2.c1.1", "z")]).err(),
        Some(OpError::ForeignId(Arc::from("u2.c1.1")))
    );
    // The server is not bound by the prefix.
    run(&part(), &[insert(&["features"], "f9", "z")]).unwrap();
}

#[test]
fn nested_new_ids_are_checked() {
    let dims = || json!([{"id": "u2.c9.1"}]);
    assert_eq!(
        client(
            &part(),
            &[set(&["features", "f1", "params", "sketch", "dims"], dims())]
        )
        .err(),
        Some(OpError::ForeignId(Arc::from("u2.c9.1")))
    );
    // Inside an inserted item too.
    let it = load(json!({"id": "u1.c1.5", "sub": [{"id": "x1"}]}));
    let r = client(
        &part(),
        &[Op::Insert {
            list: path(&["features"]),
            item: it,
            pos: pos("z"),
        }],
    );
    assert_eq!(r.err(), Some(OpError::ForeignId(Arc::from("x1"))));
    // A list that only exists inside a plain list is checked too.
    let nested = json!([[{"id": "x2"}]]);
    assert_eq!(
        client(&part(), &[set(&["variables", "grid"], nested)]).err(),
        Some(OpError::ForeignId(Arc::from("x2")))
    );
}

#[test]
fn ids_that_were_there_before_are_not_new() {
    // Re-setting a value that holds legacy ids, at the same path, introduces none.
    let doc = load(json!({"a": {"l": [{"id": "old", "v": 1}]}}));
    client(&doc, &[set(&["a", "l"], json!([{"id": "old", "v": 2}]))]).unwrap();
    // The same id somewhere else is new.
    assert_eq!(
        client(&doc, &[set(&["b"], json!([{"id": "old"}]))]).err(),
        Some(OpError::ForeignId(Arc::from("old")))
    );
    // Removed and re-inserted at the same path: not new.
    let back = Op::Insert {
        list: path(&["a", "l"]),
        item: load(json!({"id": "old", "v": 3})),
        pos: pos("V"),
    };
    client(&doc, &[remove(&["a", "l"], "old"), back]).unwrap();
    // Introduced and gone again within the change: nothing is left to check.
    client(&doc, &[set(&["b"], json!([{"id": "zzz"}])), unset(&["b"])]).unwrap();
}

#[test]
fn swap_two_names_in_one_change() {
    let a = run(
        &part(),
        &[
            set(&["features", "f1", "name"], "Boss-Extrude1"),
            set(&["features", "f2", "name"], "Sketch1"),
        ],
    )
    .unwrap();
    assert_eq!(
        a.doc.at(&["features", "f1", "name"]),
        Some(&Value::Str(Arc::from("Boss-Extrude1")))
    );
}

#[test]
fn duplicate_name_is_refused() {
    let doc = part();
    let r = run(&doc, &[set(&["features", "f2", "name"], "Sketch1")]);
    assert_eq!(r.err(), Some(OpError::NameTaken(Arc::from("Sketch1"))));
    assert_eq!(doc, part());
    // An inserted item's name counts too.
    let clash = Op::Insert {
        list: path(&["features"]),
        item: load(json!({"id": "u1.c1.1", "name": "Sketch1"})),
        pos: pos("z"),
    };
    assert_eq!(
        run(&doc, &[clash]).err(),
        Some(OpError::NameTaken(Arc::from("Sketch1")))
    );
    // Non-string names and items without one are ignored.
    run(
        &doc,
        &[
            set(&["features", "f1", "name"], 1),
            set(&["features", "f2", "name"], 1),
        ],
    )
    .unwrap();
    run(
        &doc,
        &[
            unset(&["features", "f1", "name"]),
            unset(&["features", "f2", "name"]),
        ],
    )
    .unwrap();
    // A clash in a list the rules do not name is fine.
    run(
        &doc,
        &[set(
            &["variables", "l"],
            json!([{"id": "a", "name": "x"}, {"id": "b", "name": "x"}]),
        )],
    )
    .unwrap();
    // Replacing the whole list is checked.
    let dup = json!([{"id": "a", "name": "x"}, {"id": "b", "name": "x"}]);
    assert_eq!(
        run(&doc, &[set(&["features"], dup)]).err(),
        Some(OpError::NameTaken(Arc::from("x")))
    );
}

/// Legacy content in which `f1` and `f2` share a name.
fn duplicates() -> Value {
    load(json!({"features": [
        {"id": "f1", "name": "A"}, {"id": "f2", "name": "A"}, {"id": "f3", "name": "B"},
    ]}))
}

#[test]
fn legacy_duplicates_are_left_alone() {
    let doc = duplicates();
    // Changes that do not give anything a new name pass.
    run(&doc, &[remove(&["features"], "f1")]).unwrap();
    run(&doc, &[unset(&["features", "f1", "name"])]).unwrap();
    run(&doc, &[set(&["features", "f1", "name"], "A")]).unwrap();
    run(&doc, &[set(&["features", "f1", "x"], 1)]).unwrap();
    run(&doc, &[insert(&["features"], "u1.c1.1", "z")]).unwrap();
    // Even replacing the whole list with the same names.
    let same = json!([{"id": "f1", "name": "A"}, {"id": "f2", "name": "A"}]);
    run(&doc, &[set(&["features"], same)]).unwrap();
    // Giving an item a taken name is still refused.
    assert_eq!(
        run(&doc, &[set(&["features", "f3", "name"], "A")]).err(),
        Some(OpError::NameTaken(Arc::from("A")))
    );
    let clash = Op::Insert {
        list: path(&["features"]),
        item: load(json!({"id": "u1.c1.1", "name": "A"})),
        pos: pos("z"),
    };
    assert_eq!(
        run(&doc, &[clash]).err(),
        Some(OpError::NameTaken(Arc::from("A")))
    );
}

#[test]
fn undo_restoring_legacy_duplicates_needs_no_rules() {
    // As `apply`'s contract states: an inverse is exact without rules, judged like any change under them.
    // Undoing these re-creates the duplicate "A" from a document without one. Within one call
    // that cannot be told apart from a change creating a duplicate, so the rules refuse it;
    // without rules the inverse restores the original exactly.
    let doc = duplicates();
    for op in [
        remove(&["features"], "f1"),
        unset(&["features", "f1", "name"]),
    ] {
        let a = run(&doc, std::slice::from_ref(&op)).unwrap();
        assert_eq!(
            run(&a.doc, &a.inverse).err(),
            Some(OpError::NameTaken(Arc::from("A"))),
            "{op:?}"
        );
        let back = apply(&a.doc, &a.inverse, &Rules::default(), None).unwrap();
        assert_eq!(back.doc, doc, "{op:?}");
    }
}

/// A document of `levels` nested maps under key `a` (the root counts as one), the innermost
/// one empty.
fn deep(levels: usize) -> Value {
    let mut v = json!({});
    for _ in 1..levels {
        v = json!({ "a": v });
    }
    load(v)
}

fn a_path(n: usize) -> Vec<&'static str> {
    vec!["a"; n]
}

#[test]
fn set_respects_the_depth_limit() {
    let doc = deep(64);
    let mut p = a_path(63);
    // The innermost map is at 63 segments: a map there fits, a map inside a map does not.
    run(&doc, &[set(&p, json!({}))]).unwrap();
    invalid(run(&doc, &[set(&p, json!({"x": {}}))]));
    invalid(run(&doc, &[set(&p, json!([[]]))]));
    // 64 segments: a scalar fits, any container does not.
    p.push("b");
    run(&doc, &[set(&p, 1)]).unwrap();
    invalid(run(&doc, &[set(&p, json!({}))]));
    invalid(run(&doc, &[set(&p, json!([]))]));
}

#[test]
fn insert_respects_the_depth_limit() {
    let doc = deep(64);
    let at = |n: usize| {
        let mut l = a_path(n);
        l.push("l");
        l
    };
    let ins = |list: &[&str], item: serde_json::Value| Op::Insert {
        list: path(list),
        item: load(item),
        pos: pos("V"),
    };
    // A list at 62 segments puts its items at 63: an item of depth 1 fits, of depth 2 does not.
    run(&doc, &[ins(&at(61), json!({"id": "x"}))]).unwrap();
    invalid(run(&doc, &[ins(&at(61), json!({"id": "x", "v": {}}))]));
    // A list at 63 segments has no room for any item.
    invalid(run(&doc, &[ins(&at(62), json!({"id": "x"}))]));
}

#[test]
fn ids_already_under_a_set_path_are_not_new() {
    // An edit inside a plain list diffs to a Set of the whole list; the ids it carries were there.
    let doc = load(json!({"loops": [{"segs": [{"id": "L1"}, {"id": "L2"}], "closed": false}]}));
    let target = json!({"loops": [{"segs": [{"id": "L1"}, {"id": "L2"}], "closed": true}]});
    let ops = diff(&doc, &to_plain(target.clone())).unwrap();
    assert!(
        matches!(ops.as_slice(), [Op::Set { path: p, .. }] if p == &path(&["loops"])),
        "{ops:?}"
    );
    let a = client(&doc, &ops).unwrap();
    assert_eq!(a.doc.to_plain(), to_plain(target));
    // Re-setting the same content passes too.
    let grid = load(json!({"grid": [[{"id": "x2"}]]}));
    client(&grid, &[set(&["grid"], json!([[{"id": "x2"}]]))]).unwrap();
    // A brand-new foreign id inside a plain list is still refused; the client's own is not.
    let with = |id: &str| json!([{"segs": [{"id": "L1"}, {"id": id}], "closed": true}]);
    assert_eq!(
        client(&doc, &[set(&["loops"], with("x9"))]).err(),
        Some(OpError::ForeignId(Arc::from("x9")))
    );
    client(&doc, &[set(&["loops"], with("u1.c1.7"))]).unwrap();
    // Ids under another path are not exempt.
    assert_eq!(
        client(&doc, &[set(&["other"], json!([[{"id": "L1"}]]))]).err(),
        Some(OpError::ForeignId(Arc::from("L1")))
    );
    // A plain list becoming keyed, and a keyed list becoming plain.
    let dup = load(json!({"m": [{"id": "L1"}, {"id": "L1"}]}));
    client(&dup, &[set(&["m"], json!([{"id": "L1"}]))]).unwrap();
    let nested = load(json!({"m": [{"id": "a", "s": [{"id": "L1"}]}]}));
    client(
        &nested,
        &[set(&["m"], json!([[{"id": "L1"}, {"id": "a"}]]))],
    )
    .unwrap();
    // Exempt only for the Set whose path held them before the change: an id the same change
    // put there first is judged where it was put.
    assert_eq!(
        client(
            &grid,
            &[
                set(&["grid"], json!([[{"id": "x2"}, {"id": "x3"}]])),
                set(&["grid"], json!([[{"id": "x3"}]])),
            ]
        )
        .err(),
        Some(OpError::ForeignId(Arc::from("x3")))
    );
}

#[test]
fn many_sets_over_a_large_subtree_cost_one_walk_of_it() {
    // One change of 10,000 Sets at the path of a 10,000-item keyed list: the ids the path held
    // are learned at most once for the change, and not at all when no Set brings an id in.
    let items: Vec<_> = (0..10_000)
        .map(|i| json!({"id": format!("f{i}"), "v": i}))
        .collect();
    let doc = load(json!({ "big": items }));
    let subtree = 1 + 10_000 * 3; // the list; each item map, its id and its v
    let started = std::time::Instant::now();
    let measure = |ops: &[Op]| {
        let before = oswx_core::apply::held_values_visited();
        client(&doc, ops).unwrap();
        oswx_core::apply::held_values_visited() - before
    };
    let scalars: Vec<Op> = (0..10_000).map(|i| set(&["big"], i)).collect();
    assert_eq!(measure(&scalars), 0);
    let with_ids: Vec<Op> = (0..10_000)
        .map(|i| set(&["big"], json!([{"id": "u1.c1.1", "v": i}])))
        .collect();
    let visited = measure(&with_ids);
    assert!(
        visited <= subtree,
        "visited {visited} values, the subtree has {subtree}"
    );
    // Sets at many paths inside the subtree: each path's old value is walked once.
    let inside: Vec<Op> = (0..10_000)
        .map(|i| {
            set(
                &["big", &format!("f{}", i % 100), "v"],
                json!([{"id": "u1.c1.1"}]),
            )
        })
        .collect();
    let visited = measure(&inside);
    assert!(visited <= 100, "visited {visited}");
    // A generous backstop on the clock (debug build).
    assert!(
        started.elapsed() < std::time::Duration::from_secs(60),
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn remembered_removals_and_held_ids_together() {
    // f1 holds a plain list of loops whose segments carry legacy ids.
    let doc = load(json!({
        "features": [
            {"id": "f1", "name": "A", "loops": [{"segs": [{"id": "L1"}], "closed": false}]},
            {"id": "f2", "name": "B", "loops": [{"segs": [{"id": "L2"}], "closed": false}]},
        ],
    }));
    let removed = run(&doc, &[remove(&["features"], "f1")]).unwrap();
    let back = removed.inverse.clone();
    let f1: Path = path(&["features", "f1"]);
    let remembered = |p: &Path| p == &f1;
    let with = |ops: &[Op], existed: &dyn Fn(&Path) -> bool| {
        apply_with(&removed.doc, ops, &rules(), Some("u2.c1."), existed)
    };
    // Without the removal remembered, putting f1 back is foreign; with it, f1 and everything in
    // it (the unreachable L1 included) may come back.
    assert!(matches!(
        with(&back, &|_| false),
        Err(OpError::ForeignId(_))
    ));
    assert_eq!(with(&back, &remembered).unwrap().doc, doc);
    // In the same change, an edit inside f2's loops passes (L2 was held there) ...
    let edit = set(
        &["features", "f2", "loops"],
        json!([{"segs": [{"id": "L2"}], "closed": true}]),
    );
    let mut ops = back.clone();
    ops.push(edit);
    with(&ops, &remembered).unwrap();
    // ... but a brand-new foreign id there does not, and nor does L1 moved into f2.
    for id in ["x9", "L1"] {
        let mut ops = back.clone();
        ops.push(set(
            &["features", "f2", "loops"],
            json!([{"segs": [{"id": "L2"}, {"id": id}], "closed": true}]),
        ));
        assert_eq!(
            with(&ops, &remembered).err(),
            Some(OpError::ForeignId(Arc::from(id))),
            "{id}"
        );
    }
    // A remembered removal is for exactly that item path: f1 put into another list is foreign.
    let Op::Insert { item, pos, .. } = back[0].clone() else {
        panic!("an insert")
    };
    let elsewhere = [
        set(&["other"], json!({})),
        Op::Insert {
            list: path(&["other", "l"]),
            item,
            pos,
        },
    ];
    assert!(matches!(
        with(&elsewhere, &remembered),
        Err(OpError::ForeignId(_))
    ));
    // The server is not bound by the prefix at all.
    apply_with(&removed.doc, &back, &rules(), None, &|_| false).unwrap();
}

#[test]
fn the_wire_list_key_is_never_written() {
    // Hand-built operations skip `Op::from_wire`; apply refuses `$list` itself.
    let doc = load(json!({"a": {"l": [{"id": "x"}]}, "e": []}));
    for p in [&["$list"][..], &["a", "$list"]] {
        invalid(run(&doc, &[set(p, 1)]));
        invalid(run(&doc, &[insert(p, "u1.c1.1", "V")]));
    }
    invalid(run(&doc, &[insert(&["e"], "$list", "V")]));
    invalid(run(&doc, &[insert(&["a", "l"], "$list", "V")]));
    // A value holding a `$list` key, at any depth, is refused too.
    let bad = map_with_list_key();
    invalid(run(
        &doc,
        &[Op::Set {
            path: path(&["b"]),
            value: bad.clone(),
        }],
    ));
    let item = Value::Map(
        [
            (Arc::from("id"), Value::Str(Arc::from("u1.c1.1"))),
            (Arc::from("sub"), bad),
        ]
        .into_iter()
        .collect(),
    );
    invalid(run(
        &doc,
        &[Op::Insert {
            list: path(&["e"]),
            item,
            pos: pos("V"),
        }],
    ));
    // The document is still readable in its wire form.
    assert_eq!(Value::from_wire(&doc.to_wire()).unwrap(), doc);
    // Similar names are ordinary.
    run(&doc, &[set(&["a", "$lists"], 1), insert(&["e"], "$", "V")]).unwrap();
}

/// `{"k": {"$list": 1, "x": 2}}`, built by hand: no conversion would produce it.
fn map_with_list_key() -> Value {
    let inner: oswx_core::Map = [
        (Arc::from("$list"), Value::Int(1)),
        (Arc::from("x"), Value::Int(2)),
    ]
    .into_iter()
    .collect();
    Value::Map([(Arc::from("k"), Value::Map(inner))].into_iter().collect())
}

#[test]
fn all_or_nothing() {
    let doc = part();
    let r = run(
        &doc,
        &[
            set(&["features", "f2", "params", "depth"], 25),
            remove(&["features"], "nope"),
        ],
    );
    missing(r, &["features", "nope"]);
    assert_eq!(doc, part());
}

#[test]
fn set_through_missing_parent() {
    missing(
        run(&part(), &[set(&["features", "f2", "nope", "depth"], 1)]),
        &["features", "f2", "nope"],
    );
    missing(run(&part(), &[set(&["nope", "a"], 1)]), &["nope"]);
    missing(
        run(&part(), &[set(&["features", "f9", "name"], "x")]),
        &["features", "f9"],
    );
    // Stepping into a scalar or a plain list.
    missing(run(&part(), &[set(&["units", "x"], 1)]), &["units", "x"]);
    missing(
        run(
            &part(),
            &[set(
                &["features", "f1", "params", "sketch", "points", "P1", "x"],
                1,
            )],
        ),
        &["features", "f1", "params", "sketch", "points", "P1", "x"],
    );
    missing(
        run(&part(), &[unset(&["units", "x", "y"])]),
        &["units", "x"],
    );
}

#[test]
fn set_and_unset_cannot_replace_an_item_or_its_id() {
    invalid(run(
        &part(),
        &[set(&["features", "f1"], json!({"id": "f1"}))],
    ));
    invalid(run(&part(), &[unset(&["features", "f1"])]));
    invalid(run(&part(), &[set(&["features", "f1", "id"], "f7")]));
    invalid(run(&part(), &[unset(&["features", "f1", "id"])]));
    // A map key called id outside an item is ordinary.
    round_trip(&part(), &[set(&["variables", "id"], 1)]);
    invalid(run(&part(), &[Op::Unset { path: vec![] }]));
}

#[test]
fn insert_remove_move_inverse() {
    let doc = part();
    let before = ids(&doc, &["features"]);
    let a = round_trip(&doc, &[insert(&["features"], "u1.c1.1", "1")]);
    assert_eq!(ids(&a, &["features"])[0], ("u1.c1.1".into(), "1".into()));

    let a = round_trip(&doc, &[remove(&["features"], "f1")]);
    assert_eq!(ids(&a, &["features"]), before[1..].to_vec());

    let a = round_trip(&doc, &[mv(&["features"], "f1", "z")]);
    assert_eq!(ids(&a, &["features"])[1], ("f1".into(), "z".into()));

    // A shared position orders by id, and the inverse restores the original one.
    let p2 = before[1].1.clone();
    let a = round_trip(&doc, &[mv(&["features"], "f1", &p2)]);
    assert_eq!(ids(&a, &["features"])[0], ("f1".into(), p2));
}

#[test]
fn insert_creates_a_keyed_list() {
    let doc = load(json!({"a": {"empty": [], "full": [1], "n": 1}}));
    let a = round_trip(&doc, &[insert(&["a", "new"], "u1.c1.1", "V")]);
    assert_eq!(ids(&a, &["a", "new"]).len(), 1);
    let a = round_trip(&doc, &[insert(&["a", "empty"], "u1.c1.1", "V")]);
    assert_eq!(ids(&a, &["a", "empty"]).len(), 1);
    invalid(run(&doc, &[insert(&["a", "full"], "u1.c1.1", "V")]));
    invalid(run(&doc, &[insert(&["a", "n"], "u1.c1.1", "V")]));
    invalid(run(&doc, &[insert(&["a"], "u1.c1.1", "V")]));
    missing(run(&doc, &[insert(&["b", "c"], "u1.c1.1", "V")]), &["b"]);
    missing(
        run(&doc, &[insert(&["a", "n", "c"], "u1.c1.1", "V")]),
        &["a", "n", "c"],
    );
}

#[test]
fn insert_of_a_present_id_is_refused() {
    assert_eq!(
        run(&part(), &[insert(&["features"], "f1", "z")]).err(),
        Some(OpError::IdTaken(Arc::from("f1")))
    );
    let bad = Op::Insert {
        list: path(&["features"]),
        item: load(json!({"name": "no id"})),
        pos: pos("z"),
    };
    invalid(run(&part(), &[bad]));
}

#[test]
fn remove_and_move_need_the_item_and_a_keyed_list() {
    missing(
        run(&part(), &[remove(&["features"], "f9")]),
        &["features", "f9"],
    );
    missing(
        run(&part(), &[mv(&["features"], "f9", "V")]),
        &["features", "f9"],
    );
    missing(run(&part(), &[remove(&["nope"], "f1")]), &["nope"]);
    invalid(run(&part(), &[remove(&["variables"], "f1")]));
    invalid(run(&part(), &[mv(&["units"], "f1", "V")]));
}

#[test]
fn nested_keyed_lists() {
    let doc = load(json!({"features": [
        {"id": "f1", "name": "A", "dims": [{"id": "d1", "v": 1}, {"id": "d2", "v": 2}]},
    ]}));
    let a = round_trip(
        &doc,
        &[
            set(&["features", "f1", "dims", "d1", "v"], 5),
            mv(&["features", "f1", "dims"], "d1", "z"),
            insert(&["features", "f1", "dims"], "u1.c1.1", "1"),
            remove(&["features", "f1", "dims"], "d2"),
        ],
    );
    assert_eq!(
        ids(&a, &["features", "f1", "dims"])
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        vec!["u1.c1.1", "d1"]
    );
}

#[test]
fn sharing() {
    let features: Vec<_> = (0..1000)
        .map(|i| {
            json!({"id": format!("f{i}"), "name": format!("Feature{i}"),
                   "params": {"depth": i, "pts": [i, i]}})
        })
        .collect();
    let doc = load(json!({ "features": features }));
    let a = run(&doc, &[set(&["features", "f2", "params", "depth"], 25)]).unwrap();
    let f1 = |d: &Value| match d.at(&["features", "f1"]) {
        Some(Value::Map(m)) => m.clone(),
        other => panic!("{other:?}"),
    };
    assert!(f1(&doc).ptr_eq(&f1(&a.doc)));
    let pts = |d: &Value| match d.at(&["features", "f1", "params", "pts"]) {
        Some(Value::List(l)) => l.clone(),
        other => panic!("{other:?}"),
    };
    assert!(Arc::ptr_eq(&pts(&doc), &pts(&a.doc)));
}

#[test]
fn generated_ops_are_mostly_applicable() {
    let mut runner = TestRunner::deterministic();
    let (mut ok, mut total) = (0, 0);
    // Applied ops of each kind: set, unset, insert, remove, move; and inserts into nested lists.
    let mut kinds = [0; 6];
    for _ in 0..500 {
        let (doc, ops) = arb_doc_and_ops().new_tree(&mut runner).unwrap().current();
        if ops.is_empty() {
            continue;
        }
        total += 1;
        if run(&doc, &ops).is_ok() {
            ok += 1;
            for op in &ops {
                let k = match op {
                    Op::Set { .. } => 0,
                    Op::Unset { .. } => 1,
                    Op::Insert { list, .. } if list.len() > 1 => 5,
                    Op::Insert { .. } => 2,
                    Op::Remove { .. } => 3,
                    Op::Move { .. } => 4,
                };
                kinds[k] += 1;
            }
        }
    }
    assert!(ok * 2 > total, "only {ok} of {total} op lists applied");
    assert!(kinds.iter().all(|&n| n > 0), "applied kinds: {kinds:?}");
}

proptest! {
    #[test]
    fn inverse_restores((doc, ops) in arb_doc_and_ops()) {
        // Forward under the rules; inverses without them, as `apply`'s contract states.
        if let Ok(a) = run(&doc, &ops) {
            let back = apply(&a.doc, &a.inverse, &Rules::default(), None).unwrap();
            prop_assert_eq!(&back.doc, &doc);
            // The inverse of the inverse redoes the change.
            let again = apply(&back.doc, &back.inverse, &Rules::default(), None).unwrap();
            prop_assert_eq!(&again.doc, &a.doc);
        }
    }

    #[test]
    fn accepted_results_reload((doc, ops) in arb_doc_and_ops()) {
        // Whatever apply accepts can be sent as a snapshot and read back, and a client holding
        // it as plain data sees nothing to send.
        if let Ok(a) = run(&doc, &ops) {
            prop_assert_eq!(Value::from_wire(&a.doc.to_wire()), Ok(a.doc.clone()));
            prop_assert_eq!(diff(&a.doc, &a.doc.to_plain()), Ok(vec![]));
        }
    }
}
