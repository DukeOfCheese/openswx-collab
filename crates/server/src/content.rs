//! Document content as it arrives over REST (JSON) and as the core keeps it ([`oswx_core::Value`]).
//!
//! The server never interprets a document: it converts it, finds the blob references in it and
//! drops the keys that belong to one person's copy. Nothing here logs or echoes content.

use std::collections::BTreeSet;

use oswx_core::{Plain, Value};

use crate::error::ApiError;

/// Top-level keys that belong to one person's copy of a document (where they rolled the feature tree
/// back to, the active configuration, what is shown, their undo journal, the file's name). They are
/// dropped from the content a document is created with, as the Python server drops them
/// (`LOCAL_ONLY`). `next_id` and `counters`, the other protected keys, are kept as given.
pub const LOCAL_ONLY: [&str; 5] = [
    "rollback",
    "active_config",
    "display_state",
    "journal",
    "name",
];

/// JSON as [`Plain`] data, for [`Value::from_plain`]. Integers that do not fit an `i64` stay as
/// they are (a `u64`), which `from_plain` refuses.
pub fn plain_from_json(v: &serde_json::Value) -> Plain {
    match v {
        serde_json::Value::Null => Plain::Nil,
        serde_json::Value::Bool(b) => Plain::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Plain::from(i)
            } else if let Some(u) = n.as_u64() {
                Plain::from(u)
            } else {
                // serde_json only makes finite numbers from text.
                Plain::F64(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        serde_json::Value::String(s) => Plain::from(s.as_str()),
        serde_json::Value::Array(items) => {
            Plain::Array(items.iter().map(plain_from_json).collect())
        }
        serde_json::Value::Object(m) => Plain::Map(
            m.iter()
                .map(|(k, v)| (Plain::from(k.as_str()), plain_from_json(v)))
                .collect(),
        ),
    }
}

/// [`Plain`] data as JSON. Binary and extension values (which a [`Value`] never holds) become
/// `null`, and so does a float that is not finite (which neither does).
pub fn json_from_plain(p: &Plain) -> serde_json::Value {
    match p {
        Plain::Nil | Plain::Binary(_) | Plain::Ext(..) => serde_json::Value::Null,
        Plain::Boolean(b) => serde_json::Value::Bool(*b),
        Plain::Integer(i) => match (i.as_i64(), i.as_u64()) {
            (Some(i), _) => serde_json::Value::from(i),
            (None, Some(u)) => serde_json::Value::from(u),
            (None, None) => serde_json::Value::Null,
        },
        Plain::F32(f) => float(f64::from(*f)),
        Plain::F64(f) => float(*f),
        Plain::String(s) => s
            .as_str()
            .map_or(serde_json::Value::Null, serde_json::Value::from),
        Plain::Array(items) => {
            serde_json::Value::Array(items.iter().map(json_from_plain).collect())
        }
        Plain::Map(pairs) => serde_json::Value::Object(
            pairs
                .iter()
                .filter_map(|(k, v)| Some((k.as_str()?.to_owned(), json_from_plain(v))))
                .collect(),
        ),
    }
}

/// Operations as REST shows them (history): each operation's wire map ([`oswx_core::Op::to_wire`])
/// rendered as JSON, values in their wire form (a keyed list is `{"$list": [[pos, item], ...]}`), so
/// that nothing is lost: `[{"op": "set", "path": [...], "value": ...}, {"op": "insert", "list":
/// [...], "item": {...}, "pos": "..."}, ...]`. The same shape the WebSocket carries, in JSON.
pub fn ops_json(ops: &[oswx_core::Op]) -> serde_json::Value {
    serde_json::Value::Array(
        ops.iter()
            .map(|op| json_from_plain(&op.to_wire()))
            .collect(),
    )
}

fn float(f: f64) -> serde_json::Value {
    serde_json::Number::from_f64(f).map_or(serde_json::Value::Null, serde_json::Value::Number)
}

/// The blob references in a document: maps whose keys are `$blob` (a string) and optionally `len`,
/// exactly as the Python `blob_refs` finds them. A reference is not looked inside; any other map,
/// list and keyed list is. Answers every distinct hash, sorted, malformed ones included.
pub fn blob_refs(v: &Value) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    collect_refs(v, &mut out);
    out
}

fn collect_refs(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::Map(m) => {
            let is_ref = matches!(m.get("$blob"), Some(Value::Str(_)))
                && m.keys().all(|k| &**k == "$blob" || &**k == "len");
            if is_ref {
                if let Some(Value::Str(sha)) = m.get("$blob") {
                    out.insert(sha.to_string());
                }
            } else {
                m.values().for_each(|c| collect_refs(c, out));
            }
        }
        Value::List(items) => items.iter().for_each(|c| collect_refs(c, out)),
        Value::Keyed(k) => k.iter().for_each(|(_, _, c)| collect_refs(c, out)),
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::Str(_) => {}
    }
}

/// The content with the [`LOCAL_ONLY`] keys removed from its top level (`v` is a map; anything
/// else is returned as it is).
pub fn without_local_keys(v: Value) -> Value {
    match v {
        Value::Map(mut m) => {
            for key in LOCAL_ONLY {
                m.remove(key);
            }
            Value::Map(m)
        }
        other => other,
    }
}

/// The top-level lists whose items clients edit one by one, for a document of `kind`.
fn item_lists(kind: &str) -> &'static [&'static str] {
    if kind == "assembly" {
        &["features", "components", "mates"]
    } else {
        &["features"]
    }
}

/// Checks that the lists clients edit item by item can be edited that way: `features` (and, in an
/// assembly, `components` and `mates`), when present, must be lists of objects with distinct,
/// non-empty string ids, which [`Value::from_plain`] has made keyed lists (an empty list stays a
/// plain one). Anything else would be stored as a plain list, whose items no operation can reach.
/// A part may have no `components` or `mates`. Only the shape of the lists is looked at, never
/// what the items are; repeated names are fine. The messages are the Python server's.
pub fn check_shape(kind: &str, v: &Value) -> Result<(), ApiError> {
    if kind != "assembly" {
        for coll in ["components", "mates"] {
            if v.at(&[coll]).is_some() {
                return Err(ApiError::Unprocessable(format!("a {kind} has no {coll}")));
            }
        }
    }
    for &coll in item_lists(kind) {
        let ok = match v.at(&[coll]) {
            None => true,
            Some(Value::List(items)) => items.is_empty(),
            Some(Value::Keyed(items)) => items.iter().all(|(id, _, _)| !id.is_empty()),
            Some(_) => false,
        };
        if !ok {
            return Err(ApiError::Unprocessable(format!(
                "content.{coll} must be a list of objects with distinct ids"
            )));
        }
    }
    Ok(())
}

/// The content with the item lists of a `kind` document that are missing filled in as empty
/// lists (as the Python server fills them in), so that a later insert has a list to go into.
pub fn with_item_lists(kind: &str, v: Value) -> Value {
    match v {
        Value::Map(mut m) => {
            for &coll in item_lists(kind) {
                if !m.contains_key(coll) {
                    m.insert(coll.into(), Value::List(Default::default()));
                }
            }
            Value::Map(m)
        }
        other => other,
    }
}

/// The document format version the content declares: its top-level `version` when that is an
/// integer (a boolean is not one), as the Python server reads it.
pub fn format_version(v: &Value) -> Option<i64> {
    match v.at(&["version"]) {
        Some(Value::Int(i)) => Some(*i),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn value(j: serde_json::Value) -> Value {
        Value::from_plain(&plain_from_json(&j)).unwrap()
    }

    #[test]
    fn json_round_trips_through_a_value() {
        let j = json!({"a": [1, 2.5, "x", null, true], "features": [{"id": "f1", "n": -3}],
                       "big": 9_007_199_254_740_993_i64});
        assert_eq!(json_from_plain(&value(j.clone()).to_plain()), j);
    }

    #[test]
    fn an_integer_beyond_i64_is_refused() {
        assert!(Value::from_plain(&plain_from_json(&json!({"n": u64::MAX}))).is_err());
    }

    #[test]
    fn blob_references_are_found_where_python_finds_them() {
        let a = "a".repeat(64);
        let v = value(json!({
            "x": {"$blob": a, "len": 3},
            "features": [{"id": "f1", "body": {"$blob": "short"}}],
            "not_a_ref": {"$blob": "zz", "other": 1, "inner": {"$blob": "deep"}},
            "number": {"$blob": 5},
        }));
        let got: Vec<String> = blob_refs(&v).into_iter().collect();
        assert_eq!(got, vec![a, "deep".to_owned(), "short".to_owned()]);
    }

    #[test]
    fn local_keys_are_dropped_and_the_rest_kept() {
        let v = value(
            json!({"name": "B", "rollback": null, "active_config": "D", "journal": [],
                             "display_state": {}, "next_id": 3, "counters": {}, "version": 3}),
        );
        let got = json_from_plain(&without_local_keys(v).to_plain());
        assert_eq!(got, json!({"next_id": 3, "counters": {}, "version": 3}));
    }

    #[test]
    fn only_an_integer_version_is_a_format_version() {
        assert_eq!(format_version(&value(json!({"version": 3}))), Some(3));
        assert_eq!(format_version(&value(json!({"version": true}))), None);
        assert_eq!(format_version(&value(json!({"version": 3.0}))), None);
        assert_eq!(format_version(&value(json!({"version": "3"}))), None);
        assert_eq!(format_version(&value(json!({}))), None);
    }
}
