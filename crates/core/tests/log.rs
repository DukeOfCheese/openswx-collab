mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::{load, path};
use oswx_core::{
    Accepted, Change, ClientId, ConflictKind, Head, Index, Op, Pos, Refusal, Rules, Seq, Value,
    decide,
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

fn never(_: &str) -> bool {
    false
}

fn decide_for(
    h: &Head,
    who: &str,
    ch: &Change,
    refused: &dyn Fn(&str) -> bool,
) -> Result<Accepted, Refusal> {
    decide(h, &client(who), ch, refused, &Rules::openswx())
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
    let refused: BTreeSet<&str> = BTreeSet::from(["c1"]);
    let is_refused = |id: &str| refused.contains(id);
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
    let is_refused = |id: &str| id == "c1";
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
