use std::sync::Arc;

use oswx_core::{Pos, Value, ValueError};
use serde_json::json;

fn to_plain(j: serde_json::Value) -> rmpv::Value {
    match j {
        serde_json::Value::Null => rmpv::Value::Nil,
        serde_json::Value::Bool(b) => rmpv::Value::Boolean(b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => rmpv::Value::from(i),
            None => rmpv::Value::F64(n.as_f64().unwrap()),
        },
        serde_json::Value::String(s) => rmpv::Value::from(s),
        serde_json::Value::Array(a) => rmpv::Value::Array(a.into_iter().map(to_plain).collect()),
        serde_json::Value::Object(o) => rmpv::Value::Map(
            o.into_iter()
                .map(|(k, v)| (rmpv::Value::from(k), to_plain(v)))
                .collect(),
        ),
    }
}

fn load(j: serde_json::Value) -> Result<Value, ValueError> {
    Value::from_plain(&to_plain(j))
}

fn nested(levels: usize) -> serde_json::Value {
    let mut v = json!(1);
    for _ in 0..levels {
        v = json!({ "a": v });
    }
    v
}

fn keyed(v: &Value) -> &oswx_core::KeyedList {
    match v {
        Value::Keyed(k) => k,
        other => panic!("not keyed: {other:?}"),
    }
}

#[test]
fn plain_round_trip_keeps_order() {
    let p = json!({"features":[{"id":"f1","name":"Sketch1"},{"id":"f2","name":"Boss-Extrude1"}],"units":"mm"});
    let v = Value::from_plain(&to_plain(p.clone())).unwrap();
    assert!(matches!(v.at(&["features"]), Some(Value::Keyed(k)) if k.len() == 2));
    assert_eq!(v.to_plain(), to_plain(p));
}

#[test]
fn wire_round_trip_keeps_positions() {
    let v = load(
        json!({"features":[{"id":"a","x":1},{"id":"b","x":[1,2]},{"id":"c","x":1.5}],"n":null}),
    )
    .unwrap();
    let back = Value::from_wire(&v.to_wire()).unwrap();
    assert_eq!(back, v);
    let (a, b) = (
        keyed(v.at(&["features"]).unwrap()),
        keyed(back.at(&["features"]).unwrap()),
    );
    for id in ["a", "b", "c"] {
        assert_eq!(a.pos(id), b.pos(id));
    }
}

#[test]
fn wire_form_of_a_keyed_list() {
    let v = load(json!([{"id": "a"}])).unwrap();
    let wire = v.to_wire();
    let rmpv::Value::Map(m) = &wire else { panic!() };
    assert_eq!(m.len(), 1);
    assert_eq!(m[0].0.as_str(), Some("$list"));
}

#[test]
fn list_of_scalars_is_not_keyed() {
    assert!(matches!(load(json!([1, 2, 3])).unwrap(), Value::List(l) if l.len() == 3));
    assert!(matches!(load(json!([])).unwrap(), Value::List(l) if l.is_empty()));
    assert!(matches!(load(json!([{"id": 1}])).unwrap(), Value::List(_)));
    assert!(matches!(
        load(json!([{"id": "a"}, {"x": 1}])).unwrap(),
        Value::List(_)
    ));
}

#[test]
fn duplicate_ids_make_a_plain_list() {
    assert!(matches!(
        load(json!([{"id":"a"},{"id":"a"}])).unwrap(),
        Value::List(_)
    ));
}

#[test]
fn one_and_one_point_zero_differ() {
    assert_ne!(Value::Int(1), Value::Float(1.0));
    assert_ne!(Value::Int(1), Value::Bool(true));
}

#[test]
fn depth_65_is_refused() {
    assert!(load(nested(64)).is_ok());
    assert_eq!(load(nested(65)), Err(ValueError::TooDeep));
    // lists count as levels too
    let mut v = json!(1);
    for _ in 0..65 {
        v = json!([v]);
    }
    assert_eq!(load(v), Err(ValueError::TooDeep));
}

#[test]
fn nan_is_refused() {
    assert_eq!(
        Value::from_plain(&rmpv::Value::F64(f64::NAN)),
        Err(ValueError::NotFinite)
    );
    assert_eq!(
        Value::from_plain(&rmpv::Value::F64(f64::INFINITY)),
        Err(ValueError::NotFinite)
    );
    assert_eq!(
        Value::from_plain(&rmpv::Value::F32(f32::NEG_INFINITY)),
        Err(ValueError::NotFinite)
    );
    assert_eq!(
        Value::from_wire(&rmpv::Value::F64(f64::NAN)),
        Err(ValueError::NotFinite)
    );
}

#[test]
fn id_of_201_chars_is_refused() {
    let long = "x".repeat(201);
    assert_eq!(load(json!([{"id": long}])), Err(ValueError::KeyTooLong));
    assert!(load(json!([{"id": "x".repeat(200)}])).is_ok());
    assert_eq!(load(json!({ long: 1 })), Err(ValueError::KeyTooLong));
}

#[test]
fn unsupported_inputs() {
    use rmpv::Value as P;
    let cases = [
        P::Binary(vec![1]),
        P::Ext(1, vec![1]),
        P::Map(vec![(P::from(1), P::from(2))]),
        P::Integer(u64::MAX.into()),
        P::Map(vec![(P::from("a"), P::Nil), (P::from("a"), P::Nil)]),
        P::Map(vec![(P::from("$list"), P::Array(vec![]))]),
    ];
    for c in cases {
        assert_eq!(Value::from_plain(&c), Err(ValueError::Unsupported), "{c:?}");
    }
    assert_eq!(Value::from_plain(&P::F32(1.5)), Ok(Value::Float(1.5)));
    assert_eq!(
        Value::from_plain(&P::Integer(i64::MAX.into())),
        Ok(Value::Int(i64::MAX))
    );
}

#[test]
fn malformed_wire_lists() {
    use rmpv::Value as P;
    let entry = |pos: &str, id: &str| {
        P::Array(vec![
            P::from(pos),
            P::Map(vec![(P::from("id"), P::from(id))]),
        ])
    };
    let list = |entries: Vec<P>| P::Map(vec![(P::from("$list"), P::Array(entries))]);
    assert!(Value::from_wire(&list(vec![entry("1", "a"), entry("2", "b")])).is_ok());
    for bad in [
        P::Map(vec![(P::from("$list"), P::Nil)]),
        list(vec![P::Nil]),
        list(vec![P::Array(vec![P::from("1")])]),
        list(vec![entry("0", "a")]),
        list(vec![entry("", "a")]),
        list(vec![entry("a-", "a")]),
        list(vec![entry("2", "a"), entry("1", "b")]),
        list(vec![P::Array(vec![P::from("1"), P::from(1)])]),
    ] {
        assert_eq!(Value::from_wire(&bad), Err(ValueError::BadId), "{bad:?}");
    }
    assert_eq!(
        Value::from_wire(&list(vec![entry("1", "a"), entry("2", "a")])),
        Err(ValueError::DuplicateId(Arc::from("a")))
    );
}

#[test]
fn clone_shares_structure() {
    let features: Vec<_> = (0..10_000)
        .map(|i| json!({"id": format!("f{i}"), "name": format!("Feature{i}")}))
        .collect();
    let doc = load(json!({ "features": features })).unwrap();
    let copy = doc.clone();
    let name = |v: &Value| match v.at(&["features", "f5000", "name"]) {
        Some(Value::Str(s)) => s.clone(),
        other => panic!("{other:?}"),
    };
    assert!(Arc::ptr_eq(&name(&doc), &name(&copy)));
}

#[test]
fn at_follows_maps_and_ids() {
    let v = load(json!({"a": {"b": [{"id": "x", "c": 7}]}})).unwrap();
    assert_eq!(v.at(&["a", "b", "x", "c"]), Some(&Value::Int(7)));
    assert_eq!(v.at(&[]), Some(&v));
    assert_eq!(v.at(&["a", "b", "y"]), None);
    assert_eq!(v.at(&["a", "b", "x", "c", "d"]), None);
}

#[test]
fn keyed_list_mutators_are_persistent() {
    let ps = Pos::spread(4);
    let base = keyed(&load(json!([{"id": "a"}, {"id": "b"}])).unwrap()).clone();
    let ids =
        |k: &oswx_core::KeyedList| k.iter().map(|(i, _, _)| i.to_string()).collect::<Vec<_>>();

    let with_c = base.inserted(Arc::from("c"), ps[3].clone(), Value::Int(3));
    assert_eq!(ids(&with_c), ["a", "b", "c"]);
    assert_eq!(ids(&base), ["a", "b"]);
    let first = with_c.inserted(Arc::from("z"), Pos::spread(100)[0].clone(), Value::Null);
    assert_eq!(ids(&first)[0], "z");

    let (without_a, pos, value) = with_c.removed("a").unwrap();
    assert_eq!(ids(&without_a), ["b", "c"]);
    assert_eq!(Some(&pos), base.pos("a"));
    assert_eq!(&value, base.get("a").unwrap());
    assert!(with_c.removed("nope").is_none());

    let moved = with_c.moved("a", Pos::spread(100)[99].clone()).unwrap();
    assert_eq!(ids(&moved), ["b", "c", "a"]);
    assert_eq!(moved.get("a"), with_c.get("a"));
    assert_eq!(ids(&with_c), ["a", "b", "c"]);
    assert!(with_c.moved("nope", ps[0].clone()).is_none());

    let updated = with_c.updated("b", Value::Int(9)).unwrap();
    assert_eq!(updated.get("b"), Some(&Value::Int(9)));
    assert_eq!(updated.pos("b"), with_c.pos("b"));
    assert_ne!(updated.get("b"), with_c.get("b"));
    assert!(with_c.updated("nope", Value::Null).is_none());

    // inserting under an existing id replaces it
    let replaced = with_c.inserted(Arc::from("a"), Pos::spread(100)[99].clone(), Value::Null);
    assert_eq!(replaced.len(), 3);
    assert_eq!(ids(&replaced), ["b", "c", "a"]);
}
