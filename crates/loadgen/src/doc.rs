//! The document the v1 dialect creates: the Python tests' `PART` fixture with `features` extrude features,
//! padded with strings to a requested size.

use serde_json::{Value, json};

/// Padding strings stay below the server's blob threshold (16384 characters) so no blob upload is needed.
const CHUNK: usize = 8000;

/// Feature id of the n-th (0-based) extrude feature. `f1` is the sketch.
pub fn feature_id(n: usize) -> String {
    format!("f{}", n + 2)
}

/// A document with `features` extrude features whose JSON is about `doc_bytes` bytes.
pub fn build_document(features: usize, doc_bytes: usize) -> Value {
    let base = serde_json::to_vec(&document(features, &[])).map_or(0, |v| v.len());
    let per_feature = doc_bytes.saturating_sub(base) / features.max(1);
    let pads: Vec<Vec<String>> = (0..features).map(|n| padding(n, per_feature)).collect();
    document(features, &pads)
}

fn document(features: usize, pads: &[Vec<String>]) -> Value {
    let mut list = vec![json!({
        "id": "f1", "name": "Sketch1", "type": "sketch", "suppressed": false,
        "params": {"plane": "Top", "sketch": {"points": {"P1": [0, 0]}, "entities": {}, "counter": 1}},
    })];
    for n in 0..features {
        let mut params = json!({"sketch": "Sketch1", "depth": 10});
        if let Some(p) = pads.get(n).filter(|p| !p.is_empty()) {
            params["pad"] = json!(p);
        }
        list.push(json!({
            "id": feature_id(n), "name": format!("Boss-Extrude{}", n + 1), "type": "extrude",
            "suppressed": false, "params": params,
        }));
    }
    json!({
        "format": "openswx-part", "version": 3, "name": "Bracket", "units": "mm",
        "material": {"name": null, "density": 1000.0}, "rollback": null, "active_config": "Default",
        "next_id": features + 2, "counters": {"Sketch": 1, "Boss-Extrude": features}, "variables": {},
        "features": list,
    })
}

/// Strings totalling about `bytes` bytes of JSON (each costs two quotes and a comma on top of its text).
fn padding(seed: usize, bytes: usize) -> Vec<String> {
    if bytes < 16 {
        return Vec::new();
    }
    let chunks = bytes.div_ceil(CHUNK + 3);
    let len = (bytes - 3 * chunks) / chunks;
    let ch = char::from(b'a' + (seed % 26) as u8);
    (0..chunks).map(|_| ch.to_string().repeat(len)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn five_hundred_features_five_megabytes() {
        let doc = build_document(500, 5_000_000);
        let size = serde_json::to_vec(&doc).unwrap().len();
        assert!(size.abs_diff(5_000_000) * 20 <= 5_000_000, "size {size}");
        let list = doc["features"].as_array().unwrap();
        assert_eq!(list.len(), 501); // the sketch and 500 extrudes
        assert_eq!(list.iter().filter(|f| f["type"] == "extrude").count(), 500);
    }

    #[test]
    fn small_target_is_just_the_part() {
        let doc = build_document(3, 0);
        assert_eq!(doc["features"].as_array().unwrap().len(), 4);
        assert!(doc["features"][1]["params"].get("pad").is_none());
        assert_eq!(doc["features"][1]["id"], feature_id(0));
    }

    #[test]
    fn every_string_is_below_the_blob_threshold() {
        let doc = build_document(2, 1_000_000);
        for f in doc["features"].as_array().unwrap() {
            for s in f["params"]["pad"].as_array().into_iter().flatten() {
                assert!(s.as_str().unwrap().len() < 16384);
            }
        }
    }
}
