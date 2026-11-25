//! Frames shared with the Python wheel: `vectors/*.json`, see `vectors/README.md`.
//!
//! `vectors_decode_and_re_encode` is the test. `write_vectors` (ignored) regenerates the files
//! from the messages below; run it only when the wire format changes on purpose:
//! `cargo test -p oswx-core --test vectors -- --ignored write_vectors`.

mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use common::{load, path, to_plain};
use oswx_core::{
    Change, ClientMsg, Conflict, ConflictKind, Op, Peer, Plain, Pos, RejectCode, ServerMsg,
    decode_client, decode_server, encode_client, encode_server,
};
use serde_json::{Value as Json, json};

const MAX: usize = 8 * 1024 * 1024 + 64 * 1024;

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("vectors")
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd hex");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

/// The frame's MessagePack, as JSON: maps become objects, lists arrays, nil null. Nothing else.
fn render(p: &Plain) -> Json {
    match p {
        Plain::Nil => Json::Null,
        Plain::Boolean(b) => json!(b),
        Plain::Integer(i) => match i.as_i64() {
            Some(n) => json!(n),
            None => json!(i.as_u64().unwrap()),
        },
        Plain::F32(f) => json!(f64::from(*f)),
        Plain::F64(f) => json!(f),
        Plain::String(s) => json!(s.as_str().unwrap()),
        Plain::Array(items) => Json::Array(items.iter().map(render).collect()),
        Plain::Map(pairs) => Json::Object(
            pairs
                .iter()
                .map(|(k, v)| (k.as_str().unwrap().to_string(), render(v)))
                .collect(),
        ),
        Plain::Binary(_) | Plain::Ext(..) => panic!("vectors hold no binary data"),
    }
}

fn rendered(bytes: &[u8]) -> Json {
    render(&rmpv::decode::read_value(&mut &bytes[..]).unwrap())
}

fn peer(client: &str) -> Peer {
    Peer {
        client_id: Arc::from(format!("u1.{client}")),
        user_id: "u1".into(),
        name: "Ada".into(),
        colour: "#3366cc".into(),
        presence: None,
        draft: None,
    }
}

fn five_ops() -> Vec<Op> {
    let first = Pos::between(None, None);
    let second = Pos::between(Some(&first), None);
    vec![
        Op::Set {
            path: path(&["features", "f1", "params", "depth"]),
            value: load(json!({"mm": 12.5, "n": 3, "flag": true, "one": 1.0, "tags": ["a", "b"]})),
        },
        Op::Unset {
            path: path(&["features", "f1", "params", "draft"]),
        },
        Op::Insert {
            list: path(&["features"]),
            item: load(json!({"id": "u1.client-0001.1", "name": "Sketch2", "type": "sketch"})),
            pos: second.clone(),
        },
        Op::Remove {
            list: path(&["features"]),
            id: Arc::from("f2"),
        },
        Op::Move {
            list: path(&["features"]),
            id: Arc::from("f1"),
            pos: second,
        },
    ]
}

/// (name, direction, frame)
fn vectors() -> Vec<(&'static str, &'static str, Vec<u8>)> {
    let c2s = |name, msg: ClientMsg| (name, "c2s", encode_client(&msg).to_vec());
    let s2c = |name, msg: ServerMsg| (name, "s2c", encode_server(&msg).to_vec());
    vec![
        c2s(
            "hello",
            ClientMsg::Hello {
                protocol: 2,
                token: "tok-0123456789".into(),
                client_id: "client-0001".into(),
                format_version: Some(3),
                last_seq: Some(41),
                app: Some("openswx 1.0".into()),
            },
        ),
        c2s(
            "change",
            ClientMsg::Change(Change {
                change_id: Arc::from("c-0007"),
                base: 41,
                ops: five_ops(),
                reads: BTreeSet::from([path(&["features", "f3", "name"])]),
                depends: vec![Arc::from("c-0006")],
                undo_of: None,
                command: Arc::from("edit_feature"),
            }),
        ),
        s2c(
            "change.rejected",
            ServerMsg::Rejected {
                change_id: Arc::from("c-0007"),
                code: RejectCode::Stale,
                message: "features/f1/params/depth changed since 41".into(),
                conflicts: vec![Conflict {
                    path: path(&["features", "f1", "params", "depth"]),
                    seq: 43,
                    by: Arc::from("u2.client-0002"),
                    kind: ConflictKind::Written,
                }],
            },
        ),
        s2c(
            "welcome",
            ServerMsg::Welcome {
                seq: 44,
                project_id: "p-0001".into(),
                role: "editor".into(),
                read_only: false,
                read_only_reason: None,
                you: peer("client-0001"),
                peers: vec![],
                locks: vec![],
                broken: Default::default(),
                content: Some(load(json!({
                    "features": [
                        {"id": "f1", "name": "Sketch1", "type": "sketch", "params": {"w": 10}},
                        {"id": "f2", "name": "Boss-Extrude1", "type": "extrude", "params": {"depth": 5.5}},
                    ],
                }))),
                changes: None,
            },
        ),
        s2c(
            "change.accepted",
            ServerMsg::Accepted {
                change_id: Arc::from("c-0007"),
                seq: 45,
            },
        ),
        c2s(
            "lock.acquire",
            ClientMsg::LockAcquire {
                request_id: "r-1".into(),
                paths: vec![path(&["features", "f1"]), path(&["display"])],
            },
        ),
        c2s(
            "presence",
            ClientMsg::Presence {
                data: to_plain(json!({"pointer": [1.5, -2], "tool": "line", "selection": ["f1"]})),
            },
        ),
        s2c(
            "error",
            ServerMsg::Error {
                code: "bad_token".into(),
                message: "the token is not valid".into(),
                request_id: None,
            },
        ),
    ]
}

#[test]
#[ignore = "rewrites vectors/*.json"]
fn write_vectors() {
    std::fs::create_dir_all(dir()).unwrap();
    for (name, direction, frame) in vectors() {
        let file = json!({
            "name": name,
            "hex": to_hex(&frame),
            "direction": direction,
            "json": rendered(&frame),
        });
        let text = serde_json::to_string_pretty(&file).unwrap() + "\n";
        std::fs::write(dir().join(format!("{name}.json")), text).unwrap();
        println!("{name}: {}", to_hex(&frame));
    }
}

#[test]
fn vectors_decode_and_re_encode() {
    let mut seen = BTreeSet::new();
    for entry in std::fs::read_dir(dir()).unwrap() {
        let entry = entry.unwrap().path();
        if entry.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let file: Json = serde_json::from_str(&std::fs::read_to_string(&entry).unwrap()).unwrap();
        let name = file["name"].as_str().unwrap().to_string();
        let bytes = from_hex(file["hex"].as_str().unwrap());
        assert_eq!(rendered(&bytes), file["json"], "{name}: json");
        match file["direction"].as_str().unwrap() {
            "c2s" => {
                let msg = decode_client(&bytes, MAX).unwrap_or_else(|e| panic!("{name}: {e}"));
                assert_eq!(encode_client(&msg).to_vec(), bytes, "{name}: re-encoded");
            }
            "s2c" => {
                let msg = decode_server(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
                assert_eq!(encode_server(&msg).to_vec(), bytes, "{name}: re-encoded");
            }
            other => panic!("{name}: direction {other}"),
        }
        seen.insert(name);
    }
    let expected: BTreeSet<String> = vectors().iter().map(|(n, _, _)| n.to_string()).collect();
    assert_eq!(seen, expected, "the files and the messages above agree");
}

#[test]
fn vectors_match_what_the_code_encodes() {
    for (name, _, frame) in vectors() {
        let file: Json = serde_json::from_str(
            &std::fs::read_to_string(dir().join(format!("{name}.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(file["hex"].as_str().unwrap(), to_hex(&frame), "{name}");
    }
}
