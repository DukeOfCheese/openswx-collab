mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::{load, path};
use oswx_core::{
    Accepted, Change, ClientId, ConflictKind, Head, Index, Known, Op, Pos, Refusal, RejectCode,
    Rules, SERVER_CLIENT, Seq, Value, Write, decide, decide_server, diff,
};
use serde_json::json;

fn client(s: &str) -> ClientId {
    Arc::from(s)
}

fn head() -> Head {
    Head {
        seq: 0,
        doc: load(json!({
            "features": [
                {"id": "f1", "name": "Sketch1", "params": {"depth": 10, "draft": 0}},
                {"id": "f2", "name": "Extrude1", "params": {"depth": 5}},
            ],
        })),
        index: Index::default(),
    }
}

fn set(p: &[&str], v: i64) -> Op {
    Op::Set {
        path: path(p),
        value: Value::Int(v),
    }
}

fn change(id: &str, base: Seq, ops: Vec<Op>) -> Change {
    Change {
        change_id: Arc::from(id),
        base,
        ops,
        reads: BTreeSet::new(),
        depends: Vec::new(),
        undo_of: None,
        command: Arc::from("test"),
    }
}

/// Every earlier change was accepted.
fn never(_: &str) -> Known {
    Known::Accepted
}

/// `Refused` for the ids in `refused`, `Accepted` for the others.
fn refusing<'a>(refused: &'a [&'a str]) -> impl Fn(&str) -> Known + 'a {
    move |id| {
        if refused.contains(&id) {
            Known::Refused
        } else {
            Known::Accepted
        }
    }
}

fn decide_for(
    h: &Head,
    who: &str,
    ch: &Change,
    known: &dyn Fn(&str) -> Known,
) -> Result<Accepted, Refusal> {
    decide(h, &client(who), ch, known, None, &Rules::openswx())
}

fn decide_undo(h: &Head, who: &str, ch: &Change, undo: Option<&[Op]>) -> Result<Accepted, Refusal> {
    decide(h, &client(who), ch, &never, undo, &Rules::openswx())
}

/// The head after adopting `a`, as the caller does once it has persisted it.
fn adopt(a: Accepted) -> Head {
    Head {
        seq: a.seq,
        doc: a.doc,
        index: a.index,
    }
}

#[test]
fn disjoint_changes_from_one_base_are_both_accepted() {
    let h0 = head();
    let a = decide_for(
        &h0,
        "u1.c1",
        &change(
            "a",
            0,
            vec![set(&["features", "f1", "params", "depth"], 20)],
        ),
        &never,
    )
    .unwrap();
    assert_eq!(a.seq, 1);
    let h1 = adopt(a);
    let b = decide_for(
        &h1,
        "u2.c1",
        &change("b", 0, vec![set(&["features", "f2", "params", "depth"], 7)]),
        &never,
    )
    .unwrap();
    assert_eq!(b.seq, 2);
    let h2 = adopt(b);
    assert_eq!(
        h2.doc.at(&["features", "f1", "params", "depth"]),
        Some(&Value::Int(20))
    );
    assert_eq!(
        h2.doc.at(&["features", "f2", "params", "depth"]),
        Some(&Value::Int(7))
    );
}

#[test]
fn second_writer_of_a_field_is_stale() {
    let h1 = adopt(
        decide_for(
            &head(),
            "u1.c1",
            &change(
                "a",
                0,
                vec![set(&["features", "f1", "params", "depth"], 20)],
            ),
            &never,
        )
        .unwrap(),
    );
    let err = decide_for(
        &h1,
        "u2.c1",
        &change(
            "b",
            0,
            vec![set(&["features", "f1", "params", "depth"], 30)],
        ),
        &never,
    )
    .unwrap_err();
    let Refusal::Stale(conflicts) = err else {
        panic!("expected Stale, got {err:?}")
    };
    assert_eq!(conflicts.len(), 1);
    assert_eq!(
        conflicts[0].path,
        path(&["features", "f1", "params", "depth"])
    );
    assert_eq!(conflicts[0].seq, 1);
    assert_eq!(&*conflicts[0].by, "u1.c1");
    assert_eq!(conflicts[0].kind, ConflictKind::Written);
}

#[test]
fn change_depending_on_a_refused_change_is_refused() {
    let h1 = adopt(
        decide_for(
            &head(),
            "u1.c1",
            &change(
                "x",
                0,
                vec![set(&["features", "f1", "params", "depth"], 20)],
            ),
            &never,
        )
        .unwrap(),
    );
    // c1 (set depth) is Stale.
    let c1 = change(
        "c1",
        0,
        vec![set(&["features", "f1", "params", "depth"], 30)],
    );
    assert!(matches!(
        decide_for(&h1, "u2.c1", &c1, &never),
        Err(Refusal::Stale(_))
    ));
    let is_refused = refusing(&["c1"]);
    // c2 depends on c1 and sets draft: it would apply, but it builds on a refused change.
    let mut c2 = change(
        "c2",
        0,
        vec![set(&["features", "f1", "params", "draft"], 3)],
    );
    c2.depends = vec![Arc::from("c1")];
    assert_eq!(
        decide_for(&h1, "u2.c1", &c2, &is_refused).unwrap_err(),
        Refusal::Dependency(Arc::from("c1"))
    );
}

#[test]
fn independent_later_change_survives_a_refusal() {
    let h1 = adopt(
        decide_for(
            &head(),
            "u1.c1",
            &change(
                "x",
                0,
                vec![set(&["features", "f1", "params", "depth"], 20)],
            ),
            &never,
        )
        .unwrap(),
    );
    let c1 = change(
        "c1",
        0,
        vec![set(&["features", "f1", "params", "depth"], 30)],
    );
    assert!(matches!(
        decide_for(&h1, "u2.c1", &c1, &never),
        Err(Refusal::Stale(_))
    ));
    let is_refused = refusing(&["c1"]);
    let c2 = change(
        "c2",
        0,
        vec![set(&["features", "f1", "params", "draft"], 3)],
    );
    let a = decide_for(&h1, "u2.c1", &c2, &is_refused).unwrap();
    assert_eq!(a.seq, 2);
    assert_eq!(
        a.doc.at(&["features", "f1", "params", "draft"]),
        Some(&Value::Int(3))
    );
}

#[test]
fn name_clash_is_name_taken() {
    let ch = change(
        "r",
        0,
        vec![Op::Set {
            path: path(&["features", "f2", "name"]),
            value: Value::Str(Arc::from("Sketch1")),
        }],
    );
    assert_eq!(
        decide_for(&head(), "u1.c1", &ch, &never).unwrap_err(),
        Refusal::NameTaken(Arc::from("Sketch1"))
    );
}

#[test]
fn base_below_floor_is_too_old() {
    let mut h = head();
    for (i, field) in ["a", "b", "c"].into_iter().enumerate() {
        let ch = change(
            &format!("c{i}"),
            h.seq,
            vec![set(&["features", "f1", "params", field], 1)],
        );
        h = adopt(decide_for(&h, "u1.c1", &ch, &never).unwrap());
    }
    h.index = h.index.prune(2);
    let old = change(
        "old",
        1,
        vec![set(&["features", "f2", "params", "depth"], 1)],
    );
    assert_eq!(
        decide_for(&h, "u2.c1", &old, &never).unwrap_err(),
        Refusal::TooOld
    );
    // At the floor is still judged.
    let at_floor = change(
        "ok",
        2,
        vec![set(&["features", "f2", "params", "depth"], 1)],
    );
    assert!(decide_for(&h, "u2.c1", &at_floor, &never).is_ok());
}

#[test]
fn accepted_is_not_adopted_until_caller_says() {
    let h = head();
    let before = h.doc.clone();
    let a = decide_for(
        &h,
        "u1.c1",
        &change(
            "a",
            0,
            vec![set(&["features", "f1", "params", "depth"], 20)],
        ),
        &never,
    )
    .unwrap();
    assert_eq!(h.seq, 0);
    assert_eq!(h.doc, before);
    // The head's index knows nothing of the change: the same write from another client is fine.
    let b = decide_for(
        &h,
        "u2.c1",
        &change(
            "b",
            0,
            vec![set(&["features", "f1", "params", "depth"], 30)],
        ),
        &never,
    );
    assert!(b.is_ok());
    // The inverse undoes the accepted change exactly.
    let back = oswx_core::apply(&a.doc, &a.inverse, &Rules::default(), None).unwrap();
    assert_eq!(back.doc, before);
}

#[test]
fn inserted_ids_must_carry_the_clients_prefix() {
    let item = |id: &str| load(json!({"id": id, "name": "Fillet1"}));
    let ins = |id: &str| Op::Insert {
        list: path(&["features"]),
        item: item(id),
        pos: Pos::between(None, None),
    };
    let ok = change("i", 0, vec![ins("u1.c1.1")]);
    assert!(decide_for(&head(), "u1.c1", &ok, &never).is_ok());
    let foreign = change("j", 0, vec![ins("u2.c1.1")]);
    assert!(matches!(
        decide_for(&head(), "u1.c1", &foreign, &never),
        Err(Refusal::Invalid(_))
    ));
    let taken = change("k", 0, vec![ins("f1")]);
    assert_eq!(
        decide_for(&head(), "u1.c1", &taken, &never).unwrap_err(),
        Refusal::IdTaken(Arc::from("f1"))
    );
}

#[test]
fn missing_target_is_invalid() {
    let ch = change(
        "m",
        0,
        vec![set(&["features", "nope", "params", "depth"], 1)],
    );
    assert!(matches!(
        decide_for(&head(), "u1.c1", &ch, &never),
        Err(Refusal::Invalid(_))
    ));
}

#[test]
fn malformed_changes_are_invalid() {
    let op = || vec![set(&["features", "f1", "params", "depth"], 1)];
    for id in ["", &"x".repeat(65)] {
        let ch = change(id, 0, op());
        assert!(
            matches!(
                decide_for(&head(), "u1.c1", &ch, &never),
                Err(Refusal::Invalid(_))
            ),
            "id {id:?}"
        );
    }
    assert!(decide_for(&head(), "u1.c1", &change(&"x".repeat(64), 0, op()), &never).is_ok());
    let empty = change("e", 0, vec![]);
    assert!(matches!(
        decide_for(&head(), "u1.c1", &empty, &never),
        Err(Refusal::Invalid(_))
    ));
    let ahead = change("f", 1, op());
    assert!(matches!(
        decide_for(&head(), "u1.c1", &ahead, &never),
        Err(Refusal::Invalid(_))
    ));
}

#[test]
fn two_clients_adding_the_first_items_to_an_empty_list_both_succeed() {
    let h0 = Head {
        seq: 0,
        doc: load(json!({"sketch": {"entities": []}})),
        index: Index::default(),
    };
    let edit = |who: &str| {
        let target =
            common::to_plain(json!({"sketch": {"entities": [{"id": format!("{who}.1")}]}}));
        change(who, 0, diff(&h0.doc, &target).unwrap())
    };
    let h1 = adopt(decide_for(&h0, "u1.c1", &edit("u1.c1"), &never).unwrap());
    let h2 = adopt(decide_for(&h1, "u2.c1", &edit("u2.c1"), &never).unwrap());
    let Some(Value::Keyed(k)) = h2.doc.at(&["sketch", "entities"]) else {
        panic!("a keyed list")
    };
    assert_eq!(k.len(), 2);
}

/// The head after `who` removed legacy feature `f1` as change `rm`.
fn f1_removed_by(who: &str) -> (Head, Vec<Op>) {
    let rm = change(
        "rm",
        0,
        vec![Op::Remove {
            list: path(&["features"]),
            id: Arc::from("f1"),
        }],
    );
    let a = decide_for(&head(), who, &rm, &never).unwrap();
    let inverse = a.inverse.clone();
    (adopt(a), inverse)
}

fn undo(inverse: Vec<Op>, base: Seq) -> Change {
    let mut ch = change("undo", base, inverse);
    ch.undo_of = Some(Arc::from("rm"));
    ch
}

#[test]
fn undoing_a_removal_restores_the_item() {
    let (h1, inverse) = f1_removed_by("u1.c1");
    // By the client that removed it.
    let a = decide_for(&h1, "u1.c1", &undo(inverse.clone(), 1), &never).unwrap();
    assert_eq!(a.doc, head().doc);
    // By anyone else: restoring someone else's deleted item.
    let a = decide_for(&h1, "u2.c1", &undo(inverse.clone(), 1), &never).unwrap();
    assert_eq!(a.doc, head().doc);
    // The restored item is ordinary again: it can be removed and restored once more.
    let h2 = adopt(a);
    let rm2 = change(
        "rm2",
        2,
        vec![Op::Remove {
            list: path(&["features"]),
            id: Arc::from("f1"),
        }],
    );
    let a = decide_for(&h2, "u3.c1", &rm2, &never).unwrap();
    let h3 = adopt(a.clone());
    assert!(decide_for(&h3, "u1.c1", &undo(a.inverse, 3), &never).is_ok());
}

#[test]
fn only_a_remembered_removal_lets_a_foreign_id_back_in() {
    let (h1, inverse) = f1_removed_by("u1.c1");
    let item = || match &inverse[..] {
        [Op::Insert { item, .. }] => item.clone(),
        other => panic!("one insert, got {other:?}"),
    };
    let insert = |list: &[&str], item: Value| Op::Insert {
        list: path(list),
        item,
        pos: Pos::between(None, None),
    };
    // An id never seen, or seen elsewhere, is still refused.
    let fresh = load(json!({"id": "f9", "name": "Fillet9"}));
    for ops in [
        vec![insert(&["features"], fresh)],
        vec![
            Op::Set {
                path: path(&["other"]),
                value: load(json!({})),
            },
            insert(&["other"], item()),
        ],
    ] {
        assert!(
            matches!(
                decide_for(&h1, "u2.c1", &change("x", 1, ops.clone()), &never),
                Err(Refusal::Invalid(_))
            ),
            "{ops:?}"
        );
    }
    // A removal of an item at the same path that the index no longer remembers: refused.
    let mut pruned = h1.clone();
    pruned.index = pruned.index.prune(1);
    assert!(!pruned.index.removed_at(&path(&["features", "f1"])));
    assert!(matches!(
        decide_for(&pruned, "u1.c1", &undo(inverse.clone(), 1), &never),
        Err(Refusal::Invalid(_))
    ));
    // Before pruning the removal is remembered.
    assert!(h1.index.removed_at(&path(&["features", "f1"])));
    assert!(!h1.index.removed_at(&path(&["features"])));
}

#[test]
fn edit_inside_an_item_removed_since_the_base_is_stale() {
    let rm = change(
        "rm",
        0,
        vec![Op::Remove {
            list: path(&["features"]),
            id: Arc::from("f1"),
        }],
    );
    let h1 = adopt(decide_for(&head(), "u2.c1", &rm, &never).unwrap());
    let edit = change("e", 0, vec![set(&["features", "f1", "params", "depth"], 3)]);
    assert_eq!(
        decide_for(&h1, "u1.c1", &edit, &never).unwrap_err(),
        Refusal::Stale(vec![oswx_core::Conflict {
            path: path(&["features", "f1"]),
            seq: 1,
            by: client("u2.c1"),
            kind: ConflictKind::Removed,
        }])
    );
    // Made after the removal, the same edit finds nothing to edit.
    let late = change("l", 1, vec![set(&["features", "f1", "params", "depth"], 3)]);
    assert!(matches!(
        decide_for(&h1, "u1.c1", &late, &never),
        Err(Refusal::Invalid(_))
    ));
}

#[test]
fn every_refusal_has_a_reject_code() {
    let cases = [
        (Refusal::Stale(vec![]), RejectCode::Stale, "stale"),
        (
            Refusal::Dependency(Arc::from("c")),
            RejectCode::Dependency,
            "dependency",
        ),
        (
            Refusal::NameTaken(Arc::from("n")),
            RejectCode::NameTaken,
            "name_taken",
        ),
        (
            Refusal::IdTaken(Arc::from("i")),
            RejectCode::IdTaken,
            "id_taken",
        ),
        (Refusal::TooOld, RejectCode::TooOld, "too_old"),
        (Refusal::Invalid("x".into()), RejectCode::Invalid, "invalid"),
    ];
    for (refusal, code, name) in cases {
        assert_eq!(refusal.code(), code, "{refusal:?}");
        assert_eq!(refusal.code().as_str(), name);
    }
}

#[test]
fn a_dependency_the_caller_does_not_know_is_refused() {
    let mut ch = change(
        "c2",
        0,
        vec![set(&["features", "f1", "params", "draft"], 3)],
    );
    ch.depends = vec![Arc::from("c1")];
    let unknown = |_: &str| Known::Unknown;
    assert_eq!(
        decide_for(&head(), "u1.c1", &ch, &unknown).unwrap_err(),
        Refusal::Dependency(Arc::from("c1"))
    );
    assert!(decide_for(&head(), "u1.c1", &ch, &never).is_ok());
    assert_eq!(
        decide_for(&head(), "u1.c1", &ch, &refusing(&["c1"])).unwrap_err(),
        Refusal::Dependency(Arc::from("c1"))
    );
}

#[test]
fn accepted_carries_what_it_recorded_in_the_index() {
    let mut ch = change(
        "a",
        0,
        vec![
            set(&["features", "f1", "params", "depth"], 20),
            Op::Remove {
                list: path(&["features"]),
                id: Arc::from("f2"),
            },
            Op::Move {
                list: path(&["features"]),
                id: Arc::from("f1"),
                pos: Pos::between(None, None),
            },
        ],
    );
    ch.reads = BTreeSet::from([path(&["variables"])]);
    let a = decide_for(&head(), "u1.c1", &ch, &never).unwrap();
    assert_eq!(
        a.writes,
        BTreeSet::from([
            Write::At(path(&["features", "f1", "params", "depth"])),
            Write::At(path(&["features", "f2"])),
            Write::Position(path(&["features"]), Arc::from("f1")),
        ])
    );
    assert_eq!(a.removed, BTreeSet::from([path(&["features", "f2"])]));
    assert_eq!(a.reads, ch.reads);
    // Recording them again into the head's index gives the accepted index.
    let again = head()
        .index
        .record(1, &client("u1.c1"), &a.writes, &a.removed, &a.reads);
    assert_eq!(again.to_wire(), a.index.to_wire());
}

/// A head whose legacy feature `f1` holds a sketch with another client's entity and a legacy one,
/// and the change by `u1.c1` that replaced the whole sketch with an empty one (accepted, adopted),
/// with its inverse.
fn sketch_replaced() -> (Head, Vec<Op>) {
    let h0 = Head {
        seq: 0,
        doc: load(json!({
            "features": [{"id": "f1", "name": "Sketch1", "params": {"sketch": {"entities": [
                {"id": "u2.c1.5", "kind": "line", "sub": [{"id": "u2.c1.6"}]},
                {"id": "e9", "kind": "arc"},
            ]}}}],
            "other": {},
        })),
        index: Index::default(),
    };
    let replace = change(
        "rep",
        0,
        vec![Op::Set {
            path: path(&["features", "f1", "params", "sketch"]),
            value: load(json!({"entities": []})),
        }],
    );
    let a = decide_for(&h0, "u1.c1", &replace, &never).unwrap();
    let inverse = a.inverse.clone();
    (adopt(a), inverse)
}

#[test]
fn undoing_a_set_that_replaced_a_subtree_of_foreign_ids_needs_the_kept_inverse() {
    let (h1, inverse) = sketch_replaced();
    let mut ch = undo(inverse.clone(), 1);
    ch.undo_of = Some(Arc::from("rep"));
    // Without the kept inverse, the other client's and the legacy ids are foreign.
    assert!(matches!(
        decide_undo(&h1, "u1.c1", &ch, None),
        Err(Refusal::Invalid(_))
    ));
    // With it, the subtree comes back exactly.
    let a = decide_undo(&h1, "u1.c1", &ch, Some(&inverse)).unwrap();
    assert_eq!(
        a.doc.at(&["features", "f1", "params", "sketch"]),
        sketch_replaced_original().at(&["features", "f1", "params", "sketch"])
    );
}

fn sketch_replaced_original() -> Value {
    load(json!({
        "features": [{"id": "f1", "name": "Sketch1", "params": {"sketch": {"entities": [
            {"id": "u2.c1.5", "kind": "line", "sub": [{"id": "u2.c1.6"}]},
            {"id": "e9", "kind": "arc"},
        ]}}}],
        "other": {},
    }))
}

#[test]
fn the_kept_inverse_exempts_only_the_ids_it_carried_where_it_carried_them() {
    let (h1, inverse) = sketch_replaced();
    let sketch = path(&["features", "f1", "params", "sketch"]);
    let with_undo = |ops: Vec<Op>| {
        let mut ch = change("u", 1, ops);
        ch.undo_of = Some(Arc::from("rep"));
        decide_undo(&h1, "u1.c1", &ch, Some(&inverse))
    };
    // A foreign id the inverse did not carry, beside the ones it did: refused.
    let extra = load(json!({"entities": [
        {"id": "u2.c1.5", "kind": "line", "sub": [{"id": "u2.c1.6"}]},
        {"id": "e9", "kind": "arc"},
        {"id": "u3.c1.1", "kind": "arc"},
    ]}));
    assert!(matches!(
        with_undo(vec![Op::Set {
            path: sketch.clone(),
            value: extra,
        }]),
        Err(Refusal::Invalid(_))
    ));
    // A foreign id slipped inside a carried item: refused (only the carried id is exempt).
    let inside = load(json!({"entities": [
        {"id": "u2.c1.5", "kind": "line", "sub": [{"id": "u2.c1.6"}, {"id": "u3.c1.2"}]},
    ]}));
    assert!(matches!(
        with_undo(vec![Op::Set {
            path: sketch.clone(),
            value: inside,
        }]),
        Err(Refusal::Invalid(_))
    ));
    // A carried id put somewhere else: refused.
    let elsewhere = load(json!({"entities": [{"id": "u2.c1.5", "kind": "line"}]}));
    assert!(matches!(
        with_undo(vec![Op::Set {
            path: path(&["other", "sketch"]),
            value: elsewhere,
        }]),
        Err(Refusal::Invalid(_))
    ));
    // A part of what it carried, at the same path: accepted.
    let part = load(json!({"entities": [{"id": "e9", "kind": "arc"}]}));
    assert!(
        with_undo(vec![Op::Set {
            path: sketch.clone(),
            value: part,
        }])
        .is_ok()
    );
    // The client's own new ids are fine as always.
    let own = load(json!({"entities": [{"id": "u1.c1.1", "kind": "arc"}]}));
    assert!(
        with_undo(vec![Op::Set {
            path: sketch,
            value: own,
        }])
        .is_ok()
    );
}

#[test]
fn the_kept_inverse_of_an_insert_lets_its_item_back() {
    // u1.c1 removed another client's feature; the inverse is an Insert of it. Prune the index so
    // the removal is no longer remembered: only the kept inverse lets it back.
    let (h1, inverse) = f1_removed_by("u1.c1");
    let mut pruned = h1.clone();
    pruned.index = pruned.index.prune(1);
    let ch = undo(inverse.clone(), 1);
    assert!(matches!(
        decide_undo(&pruned, "u1.c1", &ch, None),
        Err(Refusal::Invalid(_))
    ));
    let a = decide_undo(&pruned, "u1.c1", &ch, Some(&inverse)).unwrap();
    assert_eq!(a.doc, head().doc);
}

#[test]
fn the_kept_inverse_is_ignored_unless_the_change_is_an_undo() {
    let (h1, inverse) = sketch_replaced();
    let mut ch = undo(inverse.clone(), 1);
    ch.undo_of = None;
    // Not marked as an undo: the inverse passed in is not consulted, so the foreign ids are refused.
    assert!(matches!(
        decide_undo(&h1, "u1.c1", &ch, Some(&inverse)),
        Err(Refusal::Invalid(_))
    ));
    // Marked as one: it is.
    ch.undo_of = Some(Arc::from("rep"));
    assert!(decide_undo(&h1, "u1.c1", &ch, Some(&inverse)).is_ok());
}

// ------------------------------------------------------------------ server-made changes

#[test]
fn the_server_can_put_back_a_legacy_id_whose_removal_is_forgotten() {
    let (removed, inverse) = f1_removed_by("u1.c1");
    // The removal's mark is pruned: no client could put f1 back now.
    let pruned = Head {
        index: removed.index.prune(removed.seq),
        ..removed
    };
    let back = change("restore", pruned.seq, inverse);
    assert!(matches!(
        decide_for(&pruned, "u2.c1", &back, &never),
        Err(Refusal::Invalid(_))
    ));
    let a = decide_server(&pruned, &back, &Rules::openswx(), false).expect("accepted");
    assert!(a.doc.at(&["features", "f1"]).is_some());
    // The index records the server client.
    let h = adopt(a);
    // (Made on the head before the server's change, at the floor.)
    let later = change(
        "later",
        1,
        vec![set(&["features", "f1", "params", "depth"], 3)],
    );
    let refused = decide_for(&h, "u1.c1", &later, &never);
    let Err(Refusal::Stale(conflicts)) = refused else {
        panic!("expected a conflict with the server's change: {refused:?}")
    };
    assert_eq!(&*conflicts[0].by, SERVER_CLIENT);
}

#[test]
fn the_servers_changes_still_keep_names_unique_and_staleness_is_asked_for() {
    let h = head();
    let clash = change(
        "rename",
        0,
        vec![Op::Set {
            path: path(&["features", "f2", "name"]),
            value: Value::Str(Arc::from("Sketch1")),
        }],
    );
    assert_eq!(
        decide_server(&h, &clash, &Rules::openswx(), false).err(),
        Some(Refusal::NameTaken(Arc::from("Sketch1")))
    );
    // Someone writes f1's depth at seq 1; a server change on base 0 writing it too is stale only
    // when the caller asks.
    let h = adopt(
        decide_for(
            &h,
            "u1.c1",
            &change("c", 0, vec![set(&["features", "f1", "params", "depth"], 1)]),
            &never,
        )
        .unwrap(),
    );
    let restore = change(
        "restore",
        0,
        vec![set(&["features", "f1", "params", "depth"], 10)],
    );
    assert!(matches!(
        decide_server(&h, &restore, &Rules::openswx(), true),
        Err(Refusal::Stale(_))
    ));
    let a = decide_server(&h, &restore, &Rules::openswx(), false).unwrap();
    assert_eq!(a.seq, 2);
}

#[test]
fn a_server_change_checked_for_staleness_sees_earlier_server_changes() {
    let depth = &["features", "f1", "params", "depth"];
    // The server writes f1's depth at seq 1 (a restore).
    let h = adopt(
        decide_server(
            &head(),
            &change("restore", 0, vec![set(depth, 5)]),
            &Rules::openswx(),
            false,
        )
        .unwrap(),
    );
    // A later server change made on base 0 (a revert of something older) writing it too is stale:
    // the earlier server write is not "its own".
    let revert = change("revert", 0, vec![set(depth, 0)]);
    let refused = decide_server(&h, &revert, &Rules::openswx(), true);
    let Err(Refusal::Stale(conflicts)) = refused else {
        panic!("expected a conflict with the earlier server change: {refused:?}")
    };
    assert_eq!((conflicts[0].seq, &*conflicts[0].by), (1, SERVER_CLIENT));
    // Without the check it lands, and it is still recorded under the server's client id.
    let a = decide_server(&h, &revert, &Rules::openswx(), false).unwrap();
    let h = adopt(a);
    let later = change("later", 1, vec![set(depth, 7)]);
    let Err(Refusal::Stale(conflicts)) = decide_for(&h, "u1.c1", &later, &never) else {
        panic!("a client sees the server's write")
    };
    assert_eq!((conflicts[0].seq, &*conflicts[0].by), (2, SERVER_CLIENT));
    // A client's own earlier writes still never conflict with its later ones.
    let mine = adopt(
        decide_for(
            &head(),
            "u1.c1",
            &change("m1", 0, vec![set(depth, 1)]),
            &never,
        )
        .unwrap(),
    );
    assert!(
        decide_for(
            &mine,
            "u1.c1",
            &change("m2", 0, vec![set(depth, 2)]),
            &never
        )
        .is_ok()
    );
}
