mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use common::{load, path, to_plain};
use oswx_core::wire::{
    LiveEntry, MAX_BATCH_ENTRIES, MAX_CLIENT_ID, MAX_DRAFT, MAX_LIVE_CLIENT_ID, MAX_PEER_LIST,
    MAX_PRESENCE, MAX_READS, MAX_REBUILD_ENTRIES, MAX_REBUILD_KEY, MAX_REBUILD_MESSAGE,
    MAX_USER_ID, PeerRef, encode_live, encode_peer, encode_peer_list, encode_peer_list_within,
};
use oswx_core::{
    Broken, Change, ChangeEvent, ClientMsg, Conflict, ConflictKind, DenyReason, Lock, LockDenied,
    Op, Peer, Plain, Pos, RejectCode, ServerMsg, WireError, decode_client, decode_server,
    encode_client, encode_server,
};
use serde_json::{Value as Json, json};

const MAX: usize = 8 * 1024 * 1024 + 64 * 1024;

fn raw(p: &Plain) -> Vec<u8> {
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, p).unwrap();
    buf
}

/// Like `to_plain`, but keeps integers above `i64::MAX` as integers.
fn plain_of(j: &Json) -> Plain {
    match j {
        Json::Number(n) if n.as_i64().is_none() && n.as_u64().is_some() => {
            Plain::from(n.as_u64().unwrap())
        }
        Json::Array(a) => Plain::Array(a.iter().map(plain_of).collect()),
        Json::Object(o) => Plain::Map(o.iter().map(|(k, v)| pair(k, plain_of(v))).collect()),
        other => to_plain(other.clone()),
    }
}

fn frame(j: &Json) -> Vec<u8> {
    raw(&plain_of(j))
}

fn client(j: &Json) -> Result<ClientMsg, WireError> {
    decode_client(&frame(j), MAX)
}

fn ok(j: &Json) -> ClientMsg {
    client(j).unwrap_or_else(|e| panic!("expected {j}, got {e}"))
}

fn bad(j: &Json) {
    match client(j) {
        Err(WireError::Invalid(_)) => {}
        other => panic!("expected Invalid for {j}, got {other:?}"),
    }
}

fn bad_raw(bytes: &[u8]) {
    match decode_client(bytes, MAX) {
        Err(WireError::Invalid(_)) => {}
        other => panic!("expected Invalid, got {other:?}"),
    }
}

fn chars(n: usize) -> String {
    "a".repeat(n)
}

fn pair(k: &str, v: Plain) -> (Plain, Plain) {
    (Plain::from(k), v)
}

// ------------------------------------------------------------------------------------ samples

fn all_ops() -> Vec<Op> {
    let pos = Pos::between(None, None);
    vec![
        Op::Set {
            path: path(&["features", "f1", "params", "depth"]),
            value: load(json!({"a": [1, 1.0, true, null, "x"], "b": {"c": 2.5}})),
        },
        Op::Unset {
            path: path(&["features", "f1", "params", "draft"]),
        },
        Op::Insert {
            list: path(&["features"]),
            item: load(json!({"id": "u.c.1", "name": "Sketch1", "params": {"w": 3}})),
            pos: pos.clone(),
        },
        Op::Remove {
            list: path(&["features"]),
            id: Arc::from("f2"),
        },
        Op::Move {
            list: path(&["features"]),
            id: Arc::from("f1"),
            pos,
        },
    ]
}

fn change() -> Change {
    Change {
        change_id: Arc::from("c1"),
        base: 7,
        ops: all_ops(),
        reads: BTreeSet::from([path(&["a", "b"]), path(&["c"])]),
        depends: vec![Arc::from("c0")],
        undo_of: Some(Arc::from("c-1")),
        command: Arc::from("extrude"),
    }
}

fn client_samples() -> Vec<ClientMsg> {
    vec![
        ClientMsg::Hello {
            protocol: 2,
            token: "tok".into(),
            client_id: "client-01".into(),
            format_version: Some(3),
            last_seq: Some(9),
            app: Some("openswx 1.0".into()),
        },
        ClientMsg::Hello {
            protocol: 2,
            token: "tok".into(),
            client_id: "client-01".into(),
            format_version: None,
            last_seq: None,
            app: None,
        },
        ClientMsg::Change(change()),
        ClientMsg::Change(Change {
            reads: BTreeSet::new(),
            depends: vec![],
            undo_of: None,
            command: Arc::from(""),
            ..change()
        }),
        ClientMsg::Revert {
            request_id: "r1".into(),
            seq: 12,
        },
        ClientMsg::LockAcquire {
            request_id: "r2".into(),
            paths: vec![path(&["features", "f1"]), path(&["k"])],
        },
        ClientMsg::LockRelease {
            paths: vec![path(&["features", "f1"])],
        },
        ClientMsg::Presence {
            data: to_plain(json!({"pointer": [1.5, 2], "tool": "line"})),
        },
        ClientMsg::Draft {
            data: Some(to_plain(json!({"sketch": {"points": [[0, 0], [1, 1]]}}))),
        },
        ClientMsg::Draft { data: None },
        ClientMsg::RebuildReport {
            seq: 4,
            errors: BTreeMap::from([("f1".into(), "no solution".into())]),
        },
        ClientMsg::Ping,
    ]
}

fn peer(n: &str) -> Peer {
    Peer {
        client_id: Arc::from(format!("u1.{n}")),
        user_id: "u1".into(),
        name: "Ada".into(),
        colour: "#ff0000".into(),
        read_only: false,
        presence: Some(to_plain(json!({"tool": "line"}))),
        draft: None,
    }
}

fn event(seq: u64) -> ChangeEvent {
    ChangeEvent {
        seq,
        client_id: Arc::from("u1.client-01"),
        author: "u1".into(),
        author_name: "Ada".into(),
        change_id: Arc::from("c1"),
        ops: all_ops(),
        command: Arc::from("extrude"),
        undo_of: Some(Arc::from("c0")),
        reverts: Some(seq - 1),
    }
}

/// A change event as a revert from history has it: no change id, no `undo_of`, `reverts` set.
fn revert_event(seq: u64) -> ChangeEvent {
    ChangeEvent {
        client_id: Arc::from("~server"),
        change_id: Arc::from(""),
        undo_of: None,
        reverts: Some(2),
        ..event(seq)
    }
}

/// A plain change event: neither an undo nor a revert.
fn plain_event(seq: u64) -> ChangeEvent {
    ChangeEvent {
        undo_of: None,
        reverts: None,
        ..event(seq)
    }
}

fn lock() -> Lock {
    Lock {
        path: path(&["features", "f1"]),
        client_id: Arc::from("u1.client-01"),
        user_id: "u1".into(),
        name: "Ada".into(),
        detached: false,
    }
}

fn detached_lock() -> Lock {
    Lock {
        path: path(&["features"]),
        detached: true,
        ..lock()
    }
}

fn broken() -> BTreeMap<String, Broken> {
    BTreeMap::from([
        (
            "f1".to_string(),
            Broken {
                error: "no solution".into(),
                by_seq: Some(3),
                by_user: Some("u2".into()),
                by_name: Some("Bob".into()),
                command: Some("edit".into()),
                since_seq: Some(2),
            },
        ),
        (
            "f2".to_string(),
            Broken {
                error: "x".into(),
                by_seq: None,
                by_user: None,
                by_name: None,
                command: None,
                since_seq: None,
            },
        ),
    ])
}

fn welcome(content: bool, read_only: bool) -> ServerMsg {
    ServerMsg::Welcome {
        seq: 5,
        project_id: "p1".into(),
        role: if read_only { "viewer" } else { "editor" }.into(),
        read_only,
        read_only_reason: read_only.then(|| "viewer role".to_string()),
        you: peer("client-01"),
        peers: vec![
            peer("client-02"),
            Peer {
                read_only: true,
                draft: Some(to_plain(json!({"x": [1, 2]}))),
                ..peer("client-03")
            },
        ],
        locks: vec![lock(), detached_lock()],
        broken: broken(),
        content: content.then(|| load(json!({"features": [{"id": "f1", "name": "A"}]}))),
        changes: (!content).then(|| vec![event(4), event(5)]),
    }
}

fn server_samples() -> Vec<ServerMsg> {
    vec![
        welcome(true, false),
        welcome(false, false),
        welcome(true, true),
        ServerMsg::Error {
            code: "bad_token".into(),
            message: "no".into(),
            request_id: Some("r1".into()),
        },
        ServerMsg::Error {
            code: "x".into(),
            message: String::new(),
            request_id: None,
        },
        ServerMsg::Accepted {
            change_id: Arc::from("c1"),
            seq: 8,
        },
        ServerMsg::Rejected {
            change_id: Arc::from("c1"),
            code: RejectCode::Stale,
            message: "stale".into(),
            conflicts: vec![
                Conflict {
                    path: path(&["a", "b"]),
                    seq: 6,
                    by: Arc::from("u2.client-09"),
                    kind: ConflictKind::Written,
                },
                Conflict {
                    path: path(&["a"]),
                    seq: 7,
                    by: Arc::from("u2.client-09"),
                    kind: ConflictKind::Removed,
                },
                Conflict {
                    path: path(&["c"]),
                    seq: 7,
                    by: Arc::from("u2.client-09"),
                    kind: ConflictKind::Read,
                },
            ],
        },
        ServerMsg::Rejected {
            change_id: Arc::from("c2"),
            code: RejectCode::Locked,
            message: "being edited by Ada".into(),
            conflicts: vec![Conflict {
                path: path(&["features", "f1"]),
                seq: 0,
                by: Arc::from("u1.client-01"),
                kind: ConflictKind::Locked,
            }],
        },
        ServerMsg::Change(event(9)),
        ServerMsg::Change(revert_event(9)),
        ServerMsg::Change(plain_event(9)),
        ServerMsg::RevertResult {
            request_id: "r1".into(),
            ok: true,
            seq: Some(10),
            ops: all_ops(),
            code: None,
            message: None,
        },
        ServerMsg::RevertResult {
            request_id: "r1".into(),
            ok: false,
            seq: None,
            ops: vec![],
            code: Some("stale".into()),
            message: Some("someone changed it since".into()),
        },
        ServerMsg::LockResult {
            request_id: "r1".into(),
            granted: vec![path(&["a"]), path(&["b", "c"])],
            denied: vec![
                LockDenied {
                    path: path(&["features", "f1", "params"]),
                    why: DenyReason::Held,
                    holder: Some(lock()),
                },
                LockDenied {
                    path: path(&["missing"]),
                    why: DenyReason::NotLockable,
                    holder: None,
                },
            ],
        },
        ServerMsg::LockResult {
            request_id: String::new(),
            granted: vec![],
            denied: vec![],
        },
        ServerMsg::LockChanged {
            locks: vec![lock(), detached_lock()],
            released: vec![path(&["x", "y"])],
        },
        ServerMsg::LockChanged {
            locks: vec![],
            released: vec![],
        },
        ServerMsg::PeerJoined(peer("client-03")),
        ServerMsg::PeerLeft {
            client_id: Arc::from("u1.client-03"),
        },
        ServerMsg::Presence {
            client_id: Arc::from("u1.client-03"),
            data: to_plain(json!({"x": 1})),
        },
        ServerMsg::Draft {
            client_id: Arc::from("u1.client-03"),
            data: Some(to_plain(json!({"x": 1}))),
        },
        ServerMsg::Draft {
            client_id: Arc::from("u1.client-03"),
            data: None,
        },
        ServerMsg::PresenceBatch {
            presence: vec![
                (Arc::from("u1.client-03"), to_plain(json!({"x": 1}))),
                (Arc::from("u2.client-04"), to_plain(json!({"y": [1, 2]}))),
            ],
            drafts: vec![
                (Arc::from("u1.client-03"), None),
                (Arc::from("u3.client-05"), Some(to_plain(json!({"z": "a"})))),
            ],
        },
        ServerMsg::PresenceBatch {
            presence: vec![],
            drafts: vec![
                (Arc::from("u1.client-03"), None),
                (Arc::from("u3.client-05"), None),
            ],
        },
        ServerMsg::PeerList {
            peers: vec![peer("client-03"), peer("client-04")],
        },
        ServerMsg::PeerList { peers: vec![] },
        ServerMsg::RebuildStatus {
            seq: 4,
            broken: broken(),
        },
        ServerMsg::RebuildStatus {
            seq: 0,
            broken: BTreeMap::new(),
        },
        ServerMsg::VersionCreated {
            id: "v1".into(),
            name: "Release".into(),
            seq: 11,
        },
        ServerMsg::ReferenceUpdate {
            document: "d1".into(),
            components: vec!["k1".into(), "k2".into()],
        },
        ServerMsg::Pong { seq: 12 },
    ]
}

// ------------------------------------------------------------------------------- round trips

#[test]
fn every_client_message_round_trips() {
    for msg in client_samples() {
        let bytes = encode_client(&msg);
        assert_eq!(decode_client(&bytes, MAX).unwrap(), msg, "{msg:?}");
        assert_eq!(encode_client(&msg), bytes, "deterministic: {msg:?}");
    }
}

#[test]
fn every_server_message_round_trips() {
    for msg in server_samples() {
        let bytes = encode_server(&msg);
        let back = decode_server(&bytes).unwrap_or_else(|e| panic!("{e} for {msg:?}"));
        assert_eq!(back, msg);
        assert_eq!(encode_server(&back), bytes);
    }
}

#[test]
fn every_reject_code_round_trips() {
    let codes = [
        RejectCode::Stale,
        RejectCode::Locked,
        RejectCode::Dependency,
        RejectCode::NameTaken,
        RejectCode::IdTaken,
        RejectCode::Invalid,
        RejectCode::MissingBlob,
        RejectCode::ReadOnly,
        RejectCode::TooOld,
        RejectCode::ServerError,
    ];
    let names = [
        "stale",
        "locked",
        "dependency",
        "name_taken",
        "id_taken",
        "invalid",
        "missing_blob",
        "read_only",
        "too_old",
        "server_error",
    ];
    for (code, name) in codes.into_iter().zip(names) {
        assert_eq!(code.as_str(), name);
        assert_eq!(RejectCode::parse(name), Some(code));
    }
    assert_eq!(RejectCode::parse("nope"), None);
}

#[test]
fn a_change_travels_with_base_seq() {
    let bytes = encode_client(&ClientMsg::Change(change()));
    let Plain::Map(pairs) = rmpv::decode::read_value(&mut &bytes[..]).unwrap() else {
        panic!("a map")
    };
    let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str().unwrap()).collect();
    assert_eq!(
        keys,
        [
            "t",
            "change_id",
            "base_seq",
            "ops",
            "reads",
            "depends",
            "undo_of",
            "command"
        ]
    );
}

// ----------------------------------------------------------------------------------- framing

#[test]
fn a_frame_over_max_is_too_large_before_anything_is_parsed() {
    let ping = encode_client(&ClientMsg::Ping);
    assert!(decode_client(&ping, ping.len()).is_ok());
    let mut padded = ping.to_vec();
    padded.push(0);
    assert_eq!(decode_client(&padded, ping.len()), Err(WireError::TooLarge));
    // Garbage is not looked at when it is too long.
    assert_eq!(decode_client(&[0xc1; 11], 10), Err(WireError::TooLarge));
    assert_eq!(
        decode_client(&vec![0; MAX + 1], MAX),
        Err(WireError::TooLarge)
    );
}

#[test]
fn frames_that_are_not_messages() {
    bad_raw(&[]);
    bad_raw(&[0xc1]); // a byte MessagePack never uses
    bad_raw(&raw(&Plain::Array(vec![Plain::from("ping")])));
    bad_raw(&raw(&Plain::from("ping")));
    bad_raw(&raw(&Plain::Nil));
    bad_raw(&raw(&Plain::from(1)));
    // A JSON text frame.
    bad_raw(br#"{"t":"ping"}"#);
    bad_raw(br#"{"type":"ping"}"#);
    // A map cut short.
    let full = encode_client(&ClientMsg::Hello {
        protocol: 2,
        token: "tok".into(),
        client_id: "client-01".into(),
        format_version: None,
        last_seq: None,
        app: None,
    });
    bad_raw(&full[..full.len() - 1]);
    // Bytes after the map.
    let mut trailing = encode_client(&ClientMsg::Ping).to_vec();
    trailing.push(0xc0);
    bad_raw(&trailing);
    let mut two = encode_client(&ClientMsg::Ping).to_vec();
    two.extend_from_slice(&encode_client(&ClientMsg::Ping));
    bad_raw(&two);
}

#[test]
fn the_message_type_must_be_a_known_string() {
    bad(&json!({}));
    bad(&json!({"type": "ping"}));
    bad(&json!({"t": 1}));
    bad(&json!({"t": null}));
    bad(&json!({"t": ["ping"]}));
    bad(&json!({"t": "Ping"}));
    bad(&json!({"t": "nope"}));
    bad(&json!({"t": ""}));
    bad(&json!({"t": chars(10_000)}));
    // Messages only the server sends.
    for t in ["welcome", "error", "pong", "change.accepted", "peer.joined"] {
        bad(&json!({"t": t}));
    }
    // A key that is not a string.
    bad_raw(&raw(&Plain::Map(vec![
        pair("t", Plain::from("ping")),
        (Plain::from(1), Plain::from(2)),
    ])));
}

#[test]
fn repeated_known_fields_are_refused_unknown_ones_ignored() {
    bad_raw(&raw(&Plain::Map(vec![
        pair("t", Plain::from("ping")),
        pair("t", Plain::from("ping")),
    ])));
    bad_raw(&raw(&Plain::Map(vec![
        pair("t", Plain::from("revert")),
        pair("t", Plain::from("change.revert")),
        pair("request_id", Plain::from("r")),
        pair("seq", Plain::from(1)),
    ])));
    bad_raw(&raw(&Plain::Map(vec![
        pair("t", Plain::from("change.revert")),
        pair("request_id", Plain::from("r")),
        pair("seq", Plain::from(1)),
        pair("seq", Plain::from(2)),
    ])));
    // A repeated field the message does not know is nobody's business.
    let frame = raw(&Plain::Map(vec![
        pair("t", Plain::from("ping")),
        pair("extra", Plain::from(1)),
        pair("extra", Plain::from(2)),
    ]));
    assert_eq!(decode_client(&frame, MAX), Ok(ClientMsg::Ping));
    assert_eq!(
        ok(&json!({"t": "ping", "x": [1, 2], "seq": 3})),
        ClientMsg::Ping
    );
    assert_eq!(
        ok(&json!({"t": "change.revert", "request_id": "r", "seq": 1, "mode": "undo", "zzz": {}})),
        ClientMsg::Revert {
            request_id: "r".into(),
            seq: 1
        }
    );
}

// ------------------------------------------------------------------------------------- hello

fn hello() -> Json {
    json!({"t": "hello", "protocol": 2, "token": "tok", "client_id": "client-01"})
}

fn with(mut j: Json, key: &str, v: Json) -> Json {
    j[key] = v;
    j
}

fn without(mut j: Json, key: &str) -> Json {
    j.as_object_mut().unwrap().remove(key);
    j
}

#[test]
fn hello_limits() {
    assert!(matches!(ok(&hello()), ClientMsg::Hello { protocol: 2, .. }));
    // token 1..=512 characters
    ok(&with(hello(), "token", json!(chars(512))));
    ok(&with(hello(), "token", json!("é".repeat(512))));
    ok(&with(hello(), "token", json!("x")));
    bad(&with(hello(), "token", json!(chars(513))));
    bad(&with(hello(), "token", json!("é".repeat(513))));
    bad(&with(hello(), "token", json!("")));
    bad(&with(hello(), "token", json!(5)));
    bad(&with(hello(), "token", json!(null)));
    bad(&without(hello(), "token"));
    // client_id 8..=64
    ok(&with(hello(), "client_id", json!(chars(8))));
    ok(&with(hello(), "client_id", json!(chars(64))));
    bad(&with(hello(), "client_id", json!(chars(7))));
    bad(&with(hello(), "client_id", json!(chars(65))));
    bad(&with(hello(), "client_id", json!(["client-01"])));
    bad(&without(hello(), "client_id"));
    // Only [A-Za-z0-9_-]: a dot would make `<user>.<client>` ambiguous.
    ok(&with(hello(), "client_id", json!("Az09_-Az09_-")));
    for id in [
        "client.01",
        "u2.c1.xyz",
        "client 01",
        "client/01",
        "clienté01",
        "client\u{0}01",
    ] {
        bad(&with(hello(), "client_id", json!(id)));
    }
    // app at most 100
    ok(&with(hello(), "app", json!(chars(100))));
    ok(&with(hello(), "app", json!(null)));
    bad(&with(hello(), "app", json!(chars(101))));
    bad(&with(hello(), "app", json!(1)));
}

#[test]
fn hello_wants_a_protocol_and_integers() {
    bad(&without(hello(), "protocol"));
    bad(&with(hello(), "protocol", json!(null)));
    bad(&with(hello(), "protocol", json!("2")));
    bad(&with(hello(), "protocol", json!(2.0)));
    bad(&with(hello(), "protocol", json!(-1)));
    bad(&with(hello(), "protocol", json!(u64::from(u32::MAX) + 1)));
    // Another protocol number is for the server to answer, not for the decoder to refuse.
    assert!(matches!(
        ok(&with(hello(), "protocol", json!(u32::MAX))),
        ClientMsg::Hello {
            protocol: u32::MAX,
            ..
        }
    ));
    let ClientMsg::Hello {
        format_version,
        last_seq,
        ..
    } = ok(&with(
        with(hello(), "format_version", json!(-4)),
        "last_seq",
        json!(u64::MAX),
    ))
    else {
        panic!("hello")
    };
    assert_eq!((format_version, last_seq), (Some(-4), Some(u64::MAX)));
    bad(&with(hello(), "last_seq", json!(-1)));
    bad(&with(hello(), "last_seq", json!(1.0)));
    bad(&with(hello(), "last_seq", json!("1")));
    bad(&with(hello(), "format_version", json!("3")));
    bad(&with(hello(), "format_version", json!(1.5)));
    bad(&with(hello(), "format_version", json!(u64::MAX)));
}

#[test]
fn hello_with_a_repeated_known_field() {
    bad_raw(&raw(&Plain::Map(vec![
        pair("t", Plain::from("hello")),
        pair("protocol", Plain::from(2)),
        pair("token", Plain::from("a")),
        pair("token", Plain::from("b")),
        pair("client_id", Plain::from("client-01")),
    ])));
}

// ------------------------------------------------------------------------------------ change

fn unset_op(i: usize) -> Json {
    json!({"op": "unset", "path": [format!("k{i}")]})
}

fn change_json() -> Json {
    json!({"t": "change", "change_id": "c1", "base_seq": 3, "ops": [unset_op(0)]})
}

#[test]
fn change_fields_and_defaults() {
    let ClientMsg::Change(c) = ok(&change_json()) else {
        panic!("change")
    };
    assert_eq!(c.base, 3);
    assert_eq!(&*c.change_id, "c1");
    assert_eq!(c.ops.len(), 1);
    assert!(c.reads.is_empty() && c.depends.is_empty() && c.undo_of.is_none());
    assert_eq!(&*c.command, "");
    // v1's `args` is ignored, not carried.
    ok(&with(change_json(), "args", json!({"a": 1})));
    // So is v1's name for the base.
    bad(&without(with(change_json(), "base", json!(3)), "base_seq"));
}

#[test]
fn change_limits() {
    let ops = |n: usize| Json::Array((0..n).map(unset_op).collect());
    let reads = |n: usize| Json::Array((0..n).map(|i| json!([format!("r{i}")])).collect());
    let ids = |n: usize| Json::Array((0..n).map(|i| json!(format!("d{i}"))).collect());
    let c = change_json;

    ok(&with(c(), "change_id", json!(chars(64))));
    bad(&with(c(), "change_id", json!(chars(65))));
    bad(&with(c(), "change_id", json!("")));
    bad(&with(c(), "change_id", json!(1)));
    bad(&without(c(), "change_id"));

    ok(&with(c(), "ops", ops(10_000)));
    bad(&with(c(), "ops", ops(10_001)));
    bad(&with(c(), "ops", ops(0)));
    bad(&with(c(), "ops", json!({})));
    bad(&with(c(), "ops", json!(null)));
    bad(&without(c(), "ops"));

    ok(&with(c(), "reads", reads(MAX_READS)));
    bad(&with(c(), "reads", reads(MAX_READS + 1)));
    ok(&with(c(), "reads", json!(null)));
    bad(&with(c(), "reads", json!({})));

    ok(&with(c(), "depends", ids(64)));
    bad(&with(c(), "depends", ids(65)));
    ok(&with(c(), "depends", json!([chars(64)])));
    bad(&with(c(), "depends", json!([chars(65)])));
    bad(&with(c(), "depends", json!([""])));
    bad(&with(c(), "depends", json!([1])));
    bad(&with(c(), "depends", json!("d")));

    ok(&with(c(), "undo_of", json!(chars(64))));
    ok(&with(c(), "undo_of", json!(null)));
    bad(&with(c(), "undo_of", json!(chars(65))));
    bad(&with(c(), "undo_of", json!("")));
    bad(&with(c(), "undo_of", json!(3)));

    ok(&with(c(), "command", json!(chars(200))));
    ok(&with(c(), "command", json!("é".repeat(200))));
    bad(&with(c(), "command", json!(chars(201))));
    bad(&with(c(), "command", json!(["x"])));
}

#[test]
fn change_base_seq_must_be_an_unsigned_integer() {
    bad(&without(change_json(), "base_seq"));
    bad(&with(change_json(), "base_seq", json!(-1)));
    bad(&with(change_json(), "base_seq", json!(1.0)));
    bad(&with(change_json(), "base_seq", json!("1")));
    bad(&with(change_json(), "base_seq", json!(null)));
    ok(&with(change_json(), "base_seq", json!(u64::MAX)));
}

#[test]
fn change_ops_and_reads_are_checked() {
    let c = change_json;
    bad(&with(c(), "ops", json!([1])));
    bad(&with(c(), "ops", json!([{"op": "explode"}])));
    bad(&with(c(), "ops", json!([{"op": "unset"}])));
    bad(&with(c(), "ops", json!([{"op": "unset", "path": []}])));
    bad(&with(
        c(),
        "ops",
        json!([{"op": "unset", "path": ["rollback"]}]),
    ));
    bad(&with(c(), "ops", json!([{"op": "unset", "path": [1]}])));
    bad(&with(
        c(),
        "ops",
        json!([{"op": "unset", "path": [chars(201)]}]),
    ));
    bad(&with(
        c(),
        "ops",
        json!([{"op": "set", "path": ["a"], "value": {"$list": 1}}]),
    ));
    // One bad op among good ones spoils the change.
    bad(&with(c(), "ops", json!([unset_op(0), {"op": "nope"}])));
    // NaN inside an op's value.
    let nan = raw(&Plain::Map(vec![
        pair("t", Plain::from("change")),
        pair("change_id", Plain::from("c")),
        pair("base_seq", Plain::from(0)),
        pair(
            "ops",
            Plain::Array(vec![Plain::Map(vec![
                pair("op", Plain::from("set")),
                pair("path", Plain::Array(vec![Plain::from("a")])),
                pair("value", Plain::F64(f64::NAN)),
            ])]),
        ),
    ]));
    bad_raw(&nan);

    bad(&with(c(), "reads", json!([[]])));
    bad(&with(c(), "reads", json!([["a", 1]])));
    bad(&with(c(), "reads", json!(["a"])));
    bad(&with(c(), "reads", json!([[chars(201)]])));
    let deep: Vec<String> = (0..65).map(|i| format!("s{i}")).collect();
    bad(&with(c(), "reads", json!([deep])));
    let fine: Vec<String> = (0..64).map(|i| format!("s{i}")).collect();
    ok(&with(c(), "reads", json!([fine])));
    // `$list` is never a segment: not in an op, a read or a lock path.
    bad(&with(
        c(),
        "ops",
        json!([{"op": "set", "path": ["$list"], "value": 1}]),
    ));
    bad(&with(c(), "reads", json!([["features", "$list"]])));
    ok(&with(c(), "reads", json!([["features", "$lists"]])));
    let acquire = |p: Json| json!({"t": "lock.acquire", "request_id": "r", "paths": p});
    bad(&acquire(json!([["$list"]])));
    ok(&acquire(json!([["list"]])));
}

#[test]
fn change_with_a_repeated_known_field() {
    bad_raw(&raw(&Plain::Map(vec![
        pair("t", Plain::from("change")),
        pair("change_id", Plain::from("c")),
        pair("base_seq", Plain::from(0)),
        pair("base_seq", Plain::from(1)),
        pair("ops", to_plain(json!([unset_op(0)]))),
    ])));
}

// ------------------------------------------------------------------------------ revert, locks

#[test]
fn revert_limits() {
    let r = || json!({"t": "change.revert", "request_id": "r", "seq": 4});
    ok(&r());
    ok(&with(r(), "request_id", json!(chars(64))));
    bad(&with(r(), "request_id", json!(chars(65))));
    bad(&with(r(), "request_id", json!(1)));
    bad(&without(r(), "request_id"));
    bad(&without(r(), "seq"));
    bad(&with(r(), "seq", json!("4")));
    bad(&with(r(), "seq", json!(-4)));
    bad(&with(r(), "seq", json!(4.0)));
    bad(&with(r(), "seq", json!(null)));
}

#[test]
fn lock_limits() {
    let paths = |n: usize| Json::Array((0..n).map(|i| json!([format!("p{i}")])).collect());
    let acquire = |p: Json| json!({"t": "lock.acquire", "request_id": "r", "paths": p});
    let release = |p: Json| json!({"t": "lock.release", "paths": p});

    assert!(matches!(
        ok(&acquire(paths(1))),
        ClientMsg::LockAcquire { .. }
    ));
    ok(&acquire(paths(256)));
    bad(&acquire(paths(257)));
    bad(&acquire(paths(0)));
    bad(&acquire(json!(null)));
    bad(&acquire(json!({})));
    bad(&acquire(json!([[]])));
    bad(&acquire(json!([["a", 2]])));
    bad(&acquire(json!(["a"])));
    bad(&acquire(json!([[chars(201)]])));
    bad(&without(acquire(paths(1)), "paths"));
    bad(&without(acquire(paths(1)), "request_id"));
    bad(&with(acquire(paths(1)), "request_id", json!(chars(65))));
    ok(&with(acquire(paths(1)), "request_id", json!(chars(64))));
    // v1's name for them.
    bad(&without(
        with(acquire(paths(1)), "targets", json!(["f:1"])),
        "paths",
    ));

    assert!(matches!(
        ok(&release(paths(1))),
        ClientMsg::LockRelease { .. }
    ));
    ok(&release(paths(256)));
    bad(&release(paths(257)));
    bad(&release(paths(0)));
    bad(&release(json!("a")));
    bad(&release(json!([[]])));
    bad(&without(release(paths(1)), "paths"));
}

// ------------------------------------------------------------------- presence, draft, rebuild

/// A map whose encoded size is exactly `size` bytes.
fn map_of_size(size: usize) -> Plain {
    let with_n = |n: usize| Plain::Map(vec![pair("k", Plain::from("x".repeat(n)))]);
    let m = (size - 10..size)
        .map(with_n)
        .find(|m| raw(m).len() == size)
        .unwrap();
    assert_eq!(raw(&m).len(), size);
    m
}

/// `n` maps inside one another.
fn nested(n: usize) -> Plain {
    if n <= 1 {
        Plain::Map(vec![])
    } else {
        Plain::Map(vec![pair("a", nested(n - 1))])
    }
}

fn msg_with_data(t: &str, data: Plain) -> Vec<u8> {
    raw(&Plain::Map(vec![
        pair("t", Plain::from(t)),
        pair("data", data),
    ]))
}

#[test]
fn presence_limits() {
    let good = decode_client(&msg_with_data("presence", map_of_size(MAX_PRESENCE)), MAX);
    assert!(matches!(good, Ok(ClientMsg::Presence { .. })));
    bad_raw(&msg_with_data("presence", map_of_size(MAX_PRESENCE + 1)));
    decode_client(&msg_with_data("presence", map_of_size(10)), MAX).unwrap();

    for data in [json!([1]), json!("x"), json!(7), json!(null), json!(true)] {
        bad(&json!({"t": "presence", "data": data}));
    }
    bad(&json!({"t": "presence"}));
}

#[test]
fn presence_and_draft_data_is_checked_but_not_converted() {
    for t in ["presence", "draft"] {
        for hostile in [
            Plain::F64(f64::NAN),
            Plain::F64(f64::INFINITY),
            Plain::F64(f64::NEG_INFINITY),
            Plain::F32(f32::NAN),
            Plain::F32(f32::INFINITY),
            Plain::Binary(vec![1, 2]),
            Plain::Ext(5, vec![1]),
        ] {
            let data = Plain::Map(vec![pair("x", Plain::Array(vec![hostile.clone()]))]);
            bad_raw(&msg_with_data(t, data));
        }
        // A key that is not a string, at any depth.
        bad_raw(&msg_with_data(
            t,
            Plain::Map(vec![(Plain::from(1), Plain::from(1))]),
        ));
        bad_raw(&msg_with_data(
            t,
            Plain::Map(vec![pair(
                "a",
                Plain::Map(vec![(Plain::Nil, Plain::from(1))]),
            )]),
        ));
        // Depth: 64 containers are allowed, 65 are not.
        decode_client(&msg_with_data(t, nested(64)), MAX).unwrap();
        bad_raw(&msg_with_data(t, nested(65)));
        let deep_list = (0..65).fold(Plain::Nil, |inner, _| Plain::Array(vec![inner]));
        bad_raw(&msg_with_data(
            t,
            Plain::Map(vec![pair("a", deep_list.clone())]),
        ));
        // Ordinary things pass untouched: floats, big integers, lists of maps with ids.
        let data = plain_of(
            &json!({"p": [1.5, -2], "items": [{"id": "a"}, {"id": "a"}], "big": u64::MAX}),
        );
        let frame = msg_with_data(t, data.clone());
        match decode_client(&frame, MAX).unwrap() {
            ClientMsg::Presence { data: d } => assert_eq!(d, data),
            ClientMsg::Draft { data: Some(d) } => assert_eq!(d, data),
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn nesting_beyond_what_the_parser_follows_is_refused() {
    bad_raw(&msg_with_data("presence", nested(500)));
    // Containers opened and never closed, as a frame of that size would be.
    let mut open = vec![0x91u8; 100_000];
    open.push(0xc0);
    bad_raw(&open);
}

#[test]
fn draft_limits() {
    let good = decode_client(&msg_with_data("draft", map_of_size(MAX_DRAFT)), MAX);
    assert!(matches!(good, Ok(ClientMsg::Draft { data: Some(_) })));
    bad_raw(&msg_with_data("draft", map_of_size(MAX_DRAFT + 1)));

    assert_eq!(
        ok(&json!({"t": "draft", "data": null})),
        ClientMsg::Draft { data: None }
    );
    assert_eq!(ok(&json!({"t": "draft"})), ClientMsg::Draft { data: None });
    for data in [json!([1]), json!("x"), json!(1), json!(false)] {
        bad(&json!({"t": "draft", "data": data}));
    }
}

#[test]
fn rebuild_report_limits() {
    let report = |errors: Json| json!({"t": "rebuild.report", "seq": 2, "errors": errors});
    let entries = |n: usize| {
        Json::Object(
            (0..n)
                .map(|i| (format!("f{i}"), json!("boom")))
                .collect::<serde_json::Map<_, _>>(),
        )
    };
    ok(&report(json!({})));
    assert!(matches!(
        ok(&report(entries(MAX_REBUILD_ENTRIES))),
        ClientMsg::RebuildReport { seq: 2, .. }
    ));
    bad(&report(entries(MAX_REBUILD_ENTRIES + 1)));

    ok(&report(json!({ chars(MAX_REBUILD_KEY): "x" })));
    bad(&report(json!({ chars(MAX_REBUILD_KEY + 1): "x" })));

    ok(&report(json!({"f": chars(MAX_REBUILD_MESSAGE)})));
    ok(&report(json!({"f": "é".repeat(MAX_REBUILD_MESSAGE)})));
    // Too long is an error; it is not cut short.
    bad(&report(json!({"f": chars(MAX_REBUILD_MESSAGE + 1)})));

    bad(&report(json!([["f", "x"]])));
    bad(&report(json!(null)));
    bad(&report(json!({"f": 1})));
    bad(&report(json!({"f": null})));
    bad(&without(report(json!({})), "errors"));
    bad(&without(report(json!({})), "seq"));
    bad(&with(report(json!({})), "seq", json!(-2)));
    bad(&with(report(json!({})), "seq", json!("2")));
    // A key twice.
    bad_raw(&raw(&Plain::Map(vec![
        pair("t", Plain::from("rebuild.report")),
        pair("seq", Plain::from(2)),
        pair(
            "errors",
            Plain::Map(vec![
                pair("f", Plain::from("a")),
                pair("f", Plain::from("b")),
            ]),
        ),
    ])));
    bad_raw(&raw(&Plain::Map(vec![
        pair("t", Plain::from("rebuild.report")),
        pair("seq", Plain::from(2)),
        pair(
            "errors",
            Plain::Map(vec![(Plain::from(1), Plain::from("a"))]),
        ),
    ])));
}

// ------------------------------------------------------------------------------------ server

fn with_server(mut msg: Json, key: &str, v: Json) -> Vec<u8> {
    msg[key] = v;
    frame(&msg)
}

#[test]
fn decode_server_is_strict_about_types() {
    let accepted = || json!({"t": "change.accepted", "change_id": "c", "seq": 1});
    assert!(decode_server(&frame(&accepted())).is_ok());
    // Unknown fields are ignored.
    assert!(decode_server(&with_server(accepted(), "extra", json!([1]))).is_ok());
    for (key, v) in [
        ("seq", json!("1")),
        ("seq", json!(-1)),
        ("change_id", json!(1)),
    ] {
        assert!(
            matches!(
                decode_server(&with_server(accepted(), key, v)),
                Err(WireError::Invalid(_))
            ),
            "{key}"
        );
    }
    let rejected = |code: Json, conflicts: Json| {
        frame(
            &json!({"t": "change.rejected", "change_id": "c", "code": code, "message": "",
                      "conflicts": conflicts}),
        )
    };
    assert!(decode_server(&rejected(json!("stale"), json!([]))).is_ok());
    for (code, conflicts) in [
        (json!("nope"), json!([])),
        (json!(1), json!([])),
        (json!("stale"), json!({})),
        (
            json!("stale"),
            json!([{"path": ["a"], "seq": 1, "by": "x", "kind": "weird"}]),
        ),
        (
            json!("stale"),
            json!([{"path": [], "seq": 1, "by": "x", "kind": "read"}]),
        ),
        (
            json!("stale"),
            json!([{"path": ["a"], "seq": "1", "by": "x", "kind": "read"}]),
        ),
        (
            json!("stale"),
            json!([{"path": ["a"], "seq": 1, "kind": "read"}]),
        ),
    ] {
        assert!(matches!(
            decode_server(&rejected(code, conflicts)),
            Err(WireError::Invalid(_))
        ),);
    }
    // Client messages are not server messages.
    for msg in client_samples() {
        assert!(
            matches!(
                decode_server(&encode_client(&msg)),
                Err(WireError::Invalid(_))
            ),
            "{msg:?}"
        );
    }
    assert!(decode_server(&[]).is_err());
    assert!(decode_server(b"{\"t\":\"pong\",\"seq\":1}").is_err());
    let mut trailing = encode_server(&ServerMsg::Pong { seq: 1 }).to_vec();
    trailing.push(0);
    assert!(decode_server(&trailing).is_err());
}

#[test]
fn decode_server_is_strict_about_undo_reverts_and_rebuilds() {
    let change = || {
        json!({"t": "change", "seq": 3, "client_id": "u.c", "author": "u", "author_name": "A",
               "change_id": "c", "ops": [], "command": ""})
    };
    let got = decode_server(&with_server(change(), "reverts", json!(2))).unwrap();
    assert!(matches!(
        got,
        ServerMsg::Change(ChangeEvent {
            reverts: Some(2),
            ..
        })
    ));
    assert!(matches!(
        decode_server(&with_server(change(), "reverts", Json::Null)).unwrap(),
        ServerMsg::Change(ChangeEvent { reverts: None, .. })
    ));
    for v in [json!("2"), json!(-1), json!(1.5), json!([2])] {
        assert!(
            decode_server(&with_server(change(), "reverts", v.clone())).is_err(),
            "{v}"
        );
    }

    let result = || json!({"t": "change.revert.result", "request_id": "r", "ok": false, "ops": []});
    assert!(decode_server(&with_server(result(), "message", json!("no"))).is_ok());
    assert!(decode_server(&with_server(result(), "message", json!(1))).is_err());

    let status = |seq: Json, entry: Json| {
        frame(&json!({"t": "rebuild.status", "seq": seq, "broken": {"f1": entry}}))
    };
    let full = json!({"error": "e", "by_seq": 2, "by_user": "u", "by_name": "A", "command": "c",
                      "since_seq": 1});
    assert!(decode_server(&status(json!(2), full.clone())).is_ok());
    assert!(decode_server(&frame(&json!({"t": "rebuild.status", "broken": {}}))).is_err());
    assert!(decode_server(&status(json!("2"), full.clone())).is_err());
    for (key, v) in [
        ("by_user", json!(1)),
        ("since_seq", json!("1")),
        ("since_seq", json!(-1)),
    ] {
        let mut entry = full.clone();
        entry[key] = v;
        assert!(decode_server(&status(json!(2), entry)).is_err(), "{key}");
    }
}

#[test]
fn decode_server_checks_a_welcome_part_by_part() {
    let good = encode_server(&welcome(true, false));
    assert!(decode_server(&good).is_ok());
    let Plain::Map(pairs) = rmpv::decode::read_value(&mut &good[..]).unwrap() else {
        panic!("a map")
    };
    let broken_variants: [(&str, Plain); 8] = [
        ("seq", Plain::from("5")),
        ("read_only", Plain::from(1)),
        ("you", Plain::from("me")),
        ("peers", Plain::Map(vec![])),
        ("locks", Plain::Array(vec![Plain::Nil])),
        ("broken", Plain::Array(vec![])),
        ("content", Plain::Binary(vec![1])),
        ("project_id", Plain::Nil),
    ];
    for (key, v) in broken_variants {
        let patched: Vec<_> = pairs
            .iter()
            .map(|(k, old)| {
                if k.as_str() == Some(key) {
                    (k.clone(), v.clone())
                } else {
                    (k.clone(), old.clone())
                }
            })
            .collect();
        assert!(
            matches!(
                decode_server(&raw(&Plain::Map(patched))),
                Err(WireError::Invalid(_))
            ),
            "{key}"
        );
    }
}

#[test]
fn decode_server_is_strict_about_locks_and_peers() {
    let lock = || json!({"path": ["a"], "client_id": "u.c", "user_id": "u", "name": "Ada", "detached": false});
    let changed = |locks: Json, released: Json| {
        frame(&json!({"t": "lock.changed", "locks": locks, "released": released}))
    };
    assert!(decode_server(&changed(json!([lock()]), json!([["b"]]))).is_ok());
    let mut bad_locks: Vec<Json> = ["path", "client_id", "user_id", "name", "detached"]
        .into_iter()
        .map(|key| without(lock(), key))
        .collect();
    bad_locks.push(with(lock(), "detached", json!(0)));
    bad_locks.push(with(lock(), "path", json!([])));
    bad_locks.push(with(lock(), "user_id", json!(1)));
    for l in bad_locks {
        assert!(
            matches!(
                decode_server(&changed(json!([l]), json!([]))),
                Err(WireError::Invalid(_))
            ),
            "{l}"
        );
    }
    for released in [json!(null), json!(["a"]), json!([[]]), json!({})] {
        assert!(decode_server(&changed(json!([]), released)).is_err());
    }
    assert!(decode_server(&frame(&json!({"t": "lock.changed", "locks": []}))).is_err());

    let result = |granted: Json, denied: Json| {
        frame(&json!({"t": "lock.result", "request_id": "r", "granted": granted, "denied": denied}))
    };
    assert!(
        decode_server(&result(
            json!([["a"]]),
            json!([{"path": ["b"], "why": "not_lockable"}])
        ))
        .is_ok()
    );
    let both = json!([{"path": ["b"], "why": "held", "holder": lock()},
                      {"path": ["c"], "why": "rate", "holder": null}]);
    assert!(decode_server(&result(json!([]), both)).is_ok());
    for why in ["held", "not_lockable", "quota", "rate", "read_only"] {
        let reason = DenyReason::parse(why).unwrap();
        assert_eq!(reason.as_str(), why);
    }
    assert_eq!(DenyReason::parse("nope"), None);
    for (granted, denied) in [
        (json!(["a"]), json!([])),
        (json!([]), json!([{"why": "held", "holder": lock()}])),
        (
            json!([]),
            json!([{"path": ["b"], "why": "held", "holder": "Ada"}]),
        ),
        (
            json!([]),
            json!([{"path": ["b"], "why": "held", "holder": without(lock(), "name")}]),
        ),
        // A reason is required, known, and names a holder exactly when it is "held".
        (json!([]), json!([{"path": ["b"]}])),
        (json!([]), json!([{"path": ["b"], "why": "nope"}])),
        (json!([]), json!([{"path": ["b"], "why": 1}])),
        (json!([]), json!([{"path": ["b"], "why": "held"}])),
        (
            json!([]),
            json!([{"path": ["b"], "why": "quota", "holder": lock()}]),
        ),
        (json!([]), json!({})),
        (json!(null), json!([])),
    ] {
        assert!(
            matches!(
                decode_server(&result(granted.clone(), denied.clone())),
                Err(WireError::Invalid(_))
            ),
            "{granted} {denied}"
        );
    }
    let no_request_id = json!({"t": "lock.result", "granted": [], "denied": []});
    assert!(decode_server(&frame(&no_request_id)).is_err());

    // A peer's read_only is optional, but a boolean when present.
    let joined = |peer: Json| frame(&json!({"t": "peer.joined", "peer": peer}));
    let peer = || json!({"client_id": "u.c", "user_id": "u", "name": "Ada", "colour": "#000"});
    let read_only = |frame: &[u8]| match decode_server(frame) {
        Ok(ServerMsg::PeerJoined(p)) => p.read_only,
        other => panic!("a peer, not {other:?}"),
    };
    assert!(!read_only(&joined(peer())));
    assert!(read_only(&joined(with(peer(), "read_only", json!(true)))));
    assert!(!read_only(&joined(with(peer(), "read_only", json!(null)))));
    assert!(decode_server(&joined(with(peer(), "read_only", json!(1)))).is_err());
}

#[test]
fn a_peer_that_may_edit_travels_without_read_only() {
    let keys = |p: Peer| {
        let bytes = encode_server(&ServerMsg::PeerJoined(p));
        let Plain::Map(pairs) = rmpv::decode::read_value(&mut &bytes[..]).unwrap() else {
            panic!("a map")
        };
        let Plain::Map(peer) = &pairs[1].1 else {
            panic!("a peer map")
        };
        peer.iter()
            .map(|(k, _)| k.as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    assert!(!keys(peer("c")).contains(&"read_only".to_owned()));
    let viewer = Peer {
        read_only: true,
        ..peer("c")
    };
    assert!(keys(viewer).contains(&"read_only".to_owned()));
}

// ---------------------------------------------------------------- strings that are not UTF-8

/// Swaps every "ZZZZ" in a frame for four bytes that are not UTF-8, keeping its length.
fn corrupt(frame: Vec<u8>) -> Vec<u8> {
    let mut out = frame;
    let mut n = 0;
    let mut i = 0;
    while i + 4 <= out.len() {
        if &out[i..i + 4] == b"ZZZZ" {
            out[i..i + 4].copy_from_slice(&[0xff, 0xfe, 0xfd, 0xfc]);
            n += 1;
            i += 4;
        } else {
            i += 1;
        }
    }
    assert!(n > 0, "no placeholder in the frame");
    out
}

#[test]
fn strings_that_are_not_utf8_are_refused_everywhere() {
    let z = json!("ZZZZ");
    let cases = [
        with(hello(), "token", z.clone()),
        with(hello(), "client_id", json!("ZZZZZZZZ")),
        with(hello(), "app", z.clone()),
        with(change_json(), "change_id", z.clone()),
        with(change_json(), "command", z.clone()),
        with(change_json(), "depends", json!([z.clone()])),
        with(change_json(), "undo_of", z.clone()),
        with(change_json(), "reads", json!([[z.clone()]])),
        with(
            change_json(),
            "ops",
            json!([{"op": "unset", "path": [z.clone()]}]),
        ),
        with(
            change_json(),
            "ops",
            json!([{"op": "set", "path": ["a"], "value": z.clone()}]),
        ),
        with(
            change_json(),
            "ops",
            json!([{"op": "set", "path": ["a"], "value": {"k": [z.clone()]}}]),
        ),
        with(
            change_json(),
            "ops",
            json!([{"op": "remove", "list": ["a"], "id": z.clone()}]),
        ),
        json!({"t": "change.revert", "request_id": z.clone(), "seq": 1}),
        json!({"t": "lock.acquire", "request_id": "r", "paths": [[z.clone()]]}),
        json!({"t": "lock.release", "paths": [[z.clone()]]}),
        json!({"t": "rebuild.report", "seq": 1, "errors": {"f": z.clone()}}),
        json!({"t": "rebuild.report", "seq": 1, "errors": {"ZZZZ": "x"}}),
        json!({"t": "presence", "data": {"k": z.clone()}}),
        json!({"t": "presence", "data": {"k": {"j": [1, z.clone()]}}}),
        json!({"t": "presence", "data": {"ZZZZ": 1}}),
        json!({"t": "draft", "data": {"k": z.clone()}}),
        json!({"t": "draft", "data": {"ZZZZ": 1}}),
    ];
    for case in cases {
        bad_raw(&corrupt(frame(&case)));
    }
    // The same text, valid, is fine.
    ok(&with(hello(), "token", z));
}

#[test]
fn invalid_utf8_inside_op_values_is_refused() {
    let value = json!({"a": ["ZZZZ"], "b": {"c": "ZZZZ"}});
    let f = frame(&with(
        change_json(),
        "ops",
        json!([{"op": "set", "path": ["a"], "value": value}]),
    ));
    bad_raw(&corrupt(f));
    let insert = json!([{"op": "insert", "list": ["l"], "item": {"id": "ZZZZ"}, "pos": "a"}]);
    bad_raw(&corrupt(frame(&with(change_json(), "ops", insert))));
}

#[test]
fn the_server_side_refuses_them_too() {
    let mut msg = welcome(true, false);
    if let ServerMsg::Welcome { content, you, .. } = &mut msg {
        *content = Some(load(json!({"name": "ZZZZ"})));
        you.name = "ZZZZ".into();
    }
    let bytes = encode_server(&msg).to_vec();
    assert!(decode_server(&bytes).is_ok());
    assert!(matches!(
        decode_server(&corrupt(bytes)),
        Err(WireError::Invalid(_))
    ));
    let presence = encode_server(&ServerMsg::Presence {
        client_id: Arc::from("u.c"),
        data: to_plain(json!({"k": "ZZZZ"})),
    });
    assert!(matches!(
        decode_server(&corrupt(presence.to_vec())),
        Err(WireError::Invalid(_))
    ));
}

// --------------------------------------------------------------------- presence.batch, peer.list

#[test]
fn live_frames_from_encoded_entries_are_the_messages_encode_server_writes() {
    let a = to_plain(json!({"pointer": [1, 2], "tool": "line"}));
    let b = map_of_size(MAX_PRESENCE);
    let entry = |c: &str, d: Option<&Plain>| LiveEntry::new(c, d);
    let (pa, pb) = (entry("u1.a", Some(&a)), entry("u2.b", Some(&b)));
    let (da, dn) = (entry("u1.a", Some(&a)), entry("u3.c", None));
    // One entry: the plain message.
    assert_eq!(
        encode_live(&[&pa], &[]),
        encode_server(&ServerMsg::Presence {
            client_id: Arc::from("u1.a"),
            data: a.clone()
        })
    );
    assert_eq!(
        encode_live(&[], &[&dn]),
        encode_server(&ServerMsg::Draft {
            client_id: Arc::from("u3.c"),
            data: None
        })
    );
    // More: a batch.
    let batch = ServerMsg::PresenceBatch {
        presence: vec![
            (Arc::from("u1.a"), a.clone()),
            (Arc::from("u2.b"), b.clone()),
        ],
        drafts: vec![
            (Arc::from("u1.a"), Some(a.clone())),
            (Arc::from("u3.c"), None),
        ],
    };
    let bytes = encode_live(&[&pa, &pb], &[&da, &dn]);
    assert_eq!(bytes, encode_server(&batch));
    assert_eq!(decode_server(&bytes).unwrap(), batch);
    // Big enough for 16-bit map headers.
    let many: Vec<LiveEntry> = (0..MAX_BATCH_ENTRIES)
        .map(|n| entry(&format!("u.{n}"), Some(&a)))
        .collect();
    let refs: Vec<&LiveEntry> = many.iter().collect();
    let msg = ServerMsg::PresenceBatch {
        presence: (0..MAX_BATCH_ENTRIES)
            .map(|n| (Arc::from(format!("u.{n}")), a.clone()))
            .collect(),
        drafts: vec![],
    };
    assert_eq!(encode_live(&refs, &[]), encode_server(&msg));
    // A peer list from encoded peers.
    let peers = [peer("client-03"), peer("client-04")];
    let encoded: Vec<Bytes> = peers.iter().map(encode_peer).collect();
    let list = ServerMsg::PeerList {
        peers: peers.to_vec(),
    };
    assert_eq!(
        encode_peer_list(&encoded.iter().collect::<Vec<_>>()),
        encode_server(&list)
    );
    let twenty: Vec<Peer> = (0..20).map(|n| peer(&format!("c{n}"))).collect();
    let encoded: Vec<Bytes> = twenty.iter().map(encode_peer).collect();
    assert_eq!(
        encode_peer_list(&encoded.iter().collect::<Vec<_>>()),
        encode_server(&ServerMsg::PeerList { peers: twenty })
    );
}

/// A peer list encoded from borrowed peers: the bytes `encode_server` writes when every presence
/// fits; past the budget, the peers from the first that does not fit go without.
#[test]
fn a_peer_list_from_borrowed_peers_carries_presence_within_its_budget() {
    let mut peers: Vec<Peer> = (0..20).map(|n| peer(&format!("c{n}"))).collect();
    peers[3].read_only = true;
    peers[5].presence = None;
    fn refs(peers: &[Peer]) -> Vec<PeerRef<'_>> {
        peers
            .iter()
            .map(|p| PeerRef {
                client_id: &p.client_id,
                user_id: &p.user_id,
                name: &p.name,
                colour: &p.colour,
                read_only: p.read_only,
                presence: p.presence.as_ref(),
            })
            .collect()
    }
    let list = ServerMsg::PeerList {
        peers: peers.clone(),
    };
    let (bytes, with) = encode_peer_list_within(&refs(&peers), usize::MAX);
    assert_eq!(with, 20);
    assert_eq!(bytes, encode_server(&list));
    // Room for five presences, a little over: the first five peers carry theirs, the sixth has
    // none, and the seventh's does not fit.
    let one = raw(peers[0].presence.as_ref().unwrap()).len();
    let (bytes, with) = encode_peer_list_within(&refs(&peers), 5 * one + 4);
    assert_eq!(with, 6, "the seventh peer's presence does not fit");
    let ServerMsg::PeerList { peers: got } = decode_server(&bytes).unwrap() else {
        panic!("a peer list");
    };
    let mut want = peers.clone();
    for p in &mut want[6..] {
        p.presence = None;
    }
    assert_eq!(got, want);
    // Nothing fits: every peer goes without, from the first.
    let (bytes, with) = encode_peer_list_within(&refs(&peers), one - 1);
    assert_eq!(with, 0);
    let ServerMsg::PeerList { peers: got } = decode_server(&bytes).unwrap() else {
        panic!("a peer list");
    };
    assert!(got.iter().all(|p| p.presence.is_none()));
    assert_eq!(got.len(), 20);
    // None at all.
    let (bytes, with) = encode_peer_list_within(&[], 0);
    assert_eq!(with, 0);
    assert_eq!(bytes, encode_server(&ServerMsg::PeerList { peers: vec![] }));
}

#[test]
fn decode_server_is_strict_about_a_presence_batch() {
    let batch = |presence: Plain, drafts: Plain| {
        raw(&Plain::Map(vec![
            pair("t", Plain::from("presence.batch")),
            pair("presence", presence),
            pair("drafts", drafts),
        ]))
    };
    let m = |pairs: Vec<(&str, Plain)>| {
        Plain::Map(
            pairs
                .into_iter()
                .map(|(k, v)| (Plain::from(k), v))
                .collect(),
        )
    };
    let data = || to_plain(json!({"x": 1}));
    let good = batch(m(vec![("u.a", data())]), m(vec![("u.b", Plain::Nil)]));
    assert!(decode_server(&good).is_ok());
    let invalid = |frame: Vec<u8>, why: &str| {
        assert!(
            matches!(decode_server(&frame), Err(WireError::Invalid(_))),
            "{why}"
        );
    };
    invalid(
        batch(m(vec![("u.a", data())]), m(vec![])),
        "one entry is a presence message",
    );
    invalid(batch(m(vec![]), m(vec![])), "empty");
    invalid(
        batch(m(vec![("u.a", data()), ("u.a", data())]), m(vec![])),
        "a client twice",
    );
    invalid(
        batch(m(vec![("u.a", Plain::Nil), ("u.b", data())]), m(vec![])),
        "presence is never nil",
    );
    for v in [json!([1]), json!("x"), json!(7), json!(true)] {
        invalid(
            batch(m(vec![("u.a", data())]), m(vec![("u.b", to_plain(v))])),
            "a draft is a map or nil",
        );
    }
    invalid(
        batch(m(vec![("u.a", data()), ("", data())]), m(vec![])),
        "an empty client id",
    );
    let numbered = Plain::Map(vec![(Plain::from(1), data()), (Plain::from("u.b"), data())]);
    invalid(batch(numbered, m(vec![])), "client ids are strings");
    invalid(
        batch(Plain::Array(vec![data(), data()]), m(vec![])),
        "presence is a map",
    );
    invalid(
        raw(&Plain::Map(vec![
            pair("t", Plain::from("presence.batch")),
            pair("presence", m(vec![("u.a", data()), ("u.b", data())])),
        ])),
        "drafts is required",
    );
    // Sizes: an entry's data as a presence or draft may be; the entries in all.
    invalid(
        batch(
            m(vec![
                ("u.a", map_of_size(MAX_PRESENCE + 1)),
                ("u.b", data()),
            ]),
            m(vec![]),
        ),
        "presence data over the presence limit",
    );
    assert!(
        decode_server(&batch(
            m(vec![("u.a", data())]),
            m(vec![("u.b", map_of_size(MAX_DRAFT))])
        ))
        .is_ok()
    );
    invalid(
        batch(
            m(vec![("u.a", data())]),
            m(vec![("u.b", map_of_size(MAX_DRAFT + 1))]),
        ),
        "draft data over the draft limit",
    );
    let half = MAX_BATCH_ENTRIES / 2 + 1;
    let names: Vec<String> = (0..2 * half).map(|n| format!("u.{n}")).collect();
    let too_many = m(names[..=MAX_BATCH_ENTRIES]
        .iter()
        .map(|n| (n.as_str(), data()))
        .collect());
    invalid(batch(too_many, m(vec![])), "too many entries");
    let split = |range: std::ops::Range<usize>| {
        m(names[range].iter().map(|n| (n.as_str(), data())).collect())
    };
    invalid(
        batch(split(0..half), split(half..2 * half)),
        "too many entries together",
    );
    // A key twice inside an entry's data.
    let twice = Plain::Map(vec![pair("a", Plain::from(1)), pair("a", Plain::from(2))]);
    invalid(
        batch(m(vec![("u.a", twice), ("u.b", data())]), m(vec![])),
        "a key twice in the data",
    );
    // The placeholder bytes of `corrupt` are no string.
    let frame = encode_server(&ServerMsg::PresenceBatch {
        presence: vec![
            (Arc::from("u.a"), to_plain(json!({"k": "ZZZZ"}))),
            (Arc::from("u.b"), data()),
        ],
        drafts: vec![],
    });
    invalid(corrupt(frame.to_vec()), "not UTF-8");
}

#[test]
fn decode_server_is_strict_about_a_peer_list() {
    let list = |peers: Json| frame(&json!({"t": "peer.list", "peers": peers}));
    assert!(decode_server(&list(json!([]))).is_ok());
    let good = json!({"client_id": "u.a", "user_id": "u", "name": "A", "colour": "#000"});
    assert!(decode_server(&list(json!([good.clone()]))).is_ok());
    for bad in [
        json!({}),
        json!([[good.clone()]]),
        json!([{"client_id": "u.a"}]),
        json!([with(good.clone(), "presence", json!([1]))]),
        json!([with(good.clone(), "read_only", json!("yes"))]),
    ] {
        assert!(
            matches!(
                decode_server(&list(bad.clone())),
                Err(WireError::Invalid(_))
            ),
            "{bad}"
        );
    }
    assert!(decode_server(&frame(&json!({"t": "peer.list"}))).is_err());
}

/// What a hostile or broken server could send a client: client ids of any length in a batch, a
/// peer named twice in a list, a list of any length. All refused.
#[test]
fn decode_server_bounds_live_client_ids_and_peer_lists() {
    let data = || to_plain(json!({"x": 1}));
    let batch = |id: &str| {
        raw(&Plain::Map(vec![
            pair("t", Plain::from("presence.batch")),
            pair(
                "presence",
                Plain::Map(vec![
                    (Plain::from(id), data()),
                    (Plain::from("u.b"), data()),
                ]),
            ),
            pair("drafts", Plain::Map(vec![])),
        ]))
    };
    let longest = format!("{}.{}", "u".repeat(MAX_USER_ID), "c".repeat(MAX_CLIENT_ID));
    assert_eq!(longest.len(), MAX_LIVE_CLIENT_ID);
    assert!(decode_server(&batch(&longest)).is_ok());
    assert!(matches!(
        decode_server(&batch(&format!("{longest}x"))),
        Err(WireError::Invalid(_))
    ));
    assert!(matches!(
        decode_server(&batch(&"x".repeat(1 << 20))),
        Err(WireError::Invalid(_))
    ));

    let peer_of = |id: &str| {
        Plain::Map(vec![
            pair("client_id", Plain::from(id)),
            pair("user_id", Plain::from("u")),
            pair("name", Plain::from("A")),
            pair("colour", Plain::from("#000")),
        ])
    };
    let list = |peers: Vec<Plain>| {
        raw(&Plain::Map(vec![
            pair("t", Plain::from("peer.list")),
            pair("peers", Plain::Array(peers)),
        ]))
    };
    assert!(decode_server(&list(vec![peer_of("u.a"), peer_of("u.b")])).is_ok());
    assert!(
        matches!(
            decode_server(&list(vec![peer_of("u.a"), peer_of("u.b"), peer_of("u.a")])),
            Err(WireError::Invalid(_))
        ),
        "a peer twice"
    );
    let many = |n: usize| list((0..n).map(|i| peer_of(&format!("u.{i}"))).collect());
    assert!(decode_server(&many(MAX_PEER_LIST)).is_ok());
    assert!(
        matches!(
            decode_server(&many(MAX_PEER_LIST + 1)),
            Err(WireError::Invalid(_))
        ),
        "too many peers"
    );
}

/// What a hostile or broken server could send a client: a peer, a presence, a draft or a
/// departure naming a client id of any length; a welcome naming any number of peers, or one twice.
/// All refused.
#[test]
fn decode_server_bounds_peer_client_ids_and_welcome_peers() {
    let longest = format!("{}.{}", "u".repeat(MAX_USER_ID), "c".repeat(MAX_CLIENT_ID));
    let too_long = format!("{longest}x");
    let refused = |frame: Vec<u8>, why: &str| {
        assert!(
            matches!(decode_server(&frame), Err(WireError::Invalid(_))),
            "{why}"
        );
    };
    let peer_of = |id: &str| {
        Plain::Map(vec![
            pair("client_id", Plain::from(id)),
            pair("user_id", Plain::from("u")),
            pair("name", Plain::from("A")),
            pair("colour", Plain::from("#000")),
        ])
    };
    let message = |t: &str, fields: Vec<(&str, Plain)>| {
        let mut pairs = vec![pair("t", Plain::from(t))];
        pairs.extend(fields.into_iter().map(|(k, v)| pair(k, v)));
        raw(&Plain::Map(pairs))
    };
    let data = || to_plain(json!({"x": 1}));
    // Each message that names a client.
    let joined = |id: &str| message("peer.joined", vec![("peer", peer_of(id))]);
    let left = |id: &str| message("peer.left", vec![("client_id", Plain::from(id))]);
    let presence = |id: &str| {
        message(
            "presence",
            vec![("client_id", Plain::from(id)), ("data", data())],
        )
    };
    let draft = |id: &str| {
        message(
            "draft",
            vec![("client_id", Plain::from(id)), ("data", data())],
        )
    };
    let welcome = |you: Plain, peers: Vec<Plain>| {
        message(
            "welcome",
            vec![
                ("seq", Plain::from(0)),
                ("project_id", Plain::from("p")),
                ("role", Plain::from("editor")),
                ("read_only", Plain::from(false)),
                ("you", you),
                ("peers", Plain::Array(peers)),
                ("locks", Plain::Array(vec![])),
                ("broken", Plain::Map(vec![])),
            ],
        )
    };
    let in_list = |id: &str| {
        raw(&Plain::Map(vec![
            pair("t", Plain::from("peer.list")),
            pair("peers", Plain::Array(vec![peer_of(id)])),
        ]))
    };
    let you_are = |id: &str| welcome(peer_of(id), vec![]);
    let among = |id: &str| welcome(peer_of("u.me"), vec![peer_of(id)]);
    type Make<'a> = &'a dyn Fn(&str) -> Vec<u8>;
    let makers: [(&str, Make); 7] = [
        ("peer.joined", &joined),
        ("peer.left", &left),
        ("presence", &presence),
        ("draft", &draft),
        ("peer.list", &in_list),
        ("welcome you", &you_are),
        ("welcome peers", &among),
    ];
    for (what, make) in makers {
        assert!(
            decode_server(&make(&longest)).is_ok(),
            "{what}: the longest"
        );
        refused(make(&too_long), &format!("{what}: one character more"));
        refused(make(&"x".repeat(1 << 20)), &format!("{what}: a megabyte"));
        refused(make(""), &format!("{what}: empty"));
    }
    // A welcome's peers: each once, and at most as many as a peer list names.
    assert!(
        decode_server(&welcome(
            peer_of("u.me"),
            vec![peer_of("u.a"), peer_of("u.b")]
        ))
        .is_ok()
    );
    refused(
        welcome(
            peer_of("u.me"),
            vec![peer_of("u.a"), peer_of("u.b"), peer_of("u.a")],
        ),
        "a peer twice",
    );
    let many = |n: usize| (0..n).map(|i| peer_of(&format!("u.{i}"))).collect();
    assert!(decode_server(&welcome(peer_of("u.me"), many(MAX_PEER_LIST))).is_ok());
    refused(
        welcome(peer_of("u.me"), many(MAX_PEER_LIST + 1)),
        "too many peers",
    );
}

// -------------------------------------------------------------------------------- duplicates

#[test]
fn repeated_fields_and_keys_are_refused_in_every_message() {
    let paths = || to_plain(json!([["a"]]));
    for t in ["lock.acquire", "lock.release"] {
        let mut pairs = vec![pair("t", Plain::from(t)), pair("paths", paths())];
        if t == "lock.acquire" {
            pairs.push(pair("request_id", Plain::from("r")));
        }
        decode_client(&raw(&Plain::Map(pairs.clone())), MAX).unwrap();
        pairs.push(pair("paths", paths()));
        bad_raw(&raw(&Plain::Map(pairs)));
    }
    for t in ["presence", "draft"] {
        let data = || to_plain(json!({"a": 1}));
        bad_raw(&raw(&Plain::Map(vec![
            pair("t", Plain::from(t)),
            pair("data", data()),
            pair("data", data()),
        ])));
        // A key twice inside the data, at the top and deeper.
        let twice = || Plain::Map(vec![pair("a", Plain::from(1)), pair("a", Plain::from(2))]);
        bad_raw(&msg_with_data(t, twice()));
        bad_raw(&msg_with_data(t, Plain::Map(vec![pair("b", twice())])));
        bad_raw(&msg_with_data(
            t,
            Plain::Map(vec![pair("b", Plain::Array(vec![twice()]))]),
        ));
    }
    bad(&with(change_json(), "depends", json!(["d1", "d2", "d1"])));
    ok(&with(change_json(), "depends", json!(["d1", "d2"])));
}

// ------------------------------------------------------------------------------------- debug

#[test]
fn printing_a_hello_does_not_print_the_token() {
    let msg = ClientMsg::Hello {
        protocol: 2,
        token: "s3cret-token-value".into(),
        client_id: "client-01".into(),
        format_version: None,
        last_seq: Some(3),
        app: None,
    };
    for text in [format!("{msg:?}"), format!("{msg:#?}")] {
        assert!(!text.contains("s3cret"), "{text}");
        assert!(text.contains("<redacted>") && text.contains("client-01"));
    }
    // The other messages still print.
    assert!(format!("{:?}", ClientMsg::Ping).contains("Ping"));
    assert!(format!("{:?}", ClientMsg::Change(change())).contains("change_id"));
}

// ----------------------------------------------------------------------------- deep documents

/// A legal value 64 levels deep that alternates items and keyed lists: 32 items, the last
/// holding an empty map at level 64.
fn deepest() -> oswx_core::Value {
    let mut item = json!({"id": "i31", "leaf": {}});
    for k in (0..31).rev() {
        item = json!({"id": format!("i{k}"), "c": [item]});
    }
    load(item)
}

#[test]
fn the_deepest_legal_value_travels_in_a_change_and_a_welcome() {
    let value = deepest();
    assert_eq!(
        oswx_core::Value::from_wire(&value.to_wire()).unwrap(),
        value
    );

    let op = Op::Set {
        path: path(&["x"]),
        value: value.clone(),
    };
    let msg = ClientMsg::Change(Change {
        ops: vec![op.clone()],
        ..change()
    });
    assert_eq!(decode_client(&encode_client(&msg), MAX).unwrap(), msg);

    let mut welcome = welcome(true, false);
    if let ServerMsg::Welcome {
        content, changes, ..
    } = &mut welcome
    {
        *content = Some(value);
        *changes = Some(vec![ChangeEvent {
            ops: vec![op],
            ..event(1)
        }]);
    }
    let bytes = encode_server(&welcome);
    assert_eq!(decode_server(&bytes).unwrap(), welcome);
}

// -------------------------------------------------------------------------------- pre-scan

fn ping_with(extra: Plain) -> Vec<u8> {
    raw(&Plain::Map(vec![
        pair("t", Plain::from("ping")),
        pair("x", extra),
    ]))
}

#[test]
fn a_frame_of_millions_of_nils_is_refused_without_building_it() {
    let frame = ping_with(Plain::Array(vec![Plain::Nil; 8 * 1024 * 1024 - 32]));
    assert!(frame.len() > 8 * 1024 * 1024 - 64);
    let started = std::time::Instant::now();
    assert!(matches!(
        decode_client(&frame, MAX),
        Err(WireError::Invalid(_))
    ));
    assert!(
        started.elapsed() < std::time::Duration::from_millis(200),
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn only_a_change_may_be_a_large_frame() {
    use oswx_core::wire::SMALL_FRAME;
    let big = || Plain::from("x".repeat(SMALL_FRAME + 100));
    for t in ["presence", "draft"] {
        let f = msg_with_data(t, Plain::Map(vec![pair("k", big())]));
        assert_eq!(decode_client(&f, MAX), Err(WireError::TooLarge), "{t}");
    }
    assert_eq!(
        decode_client(&ping_with(big()), MAX),
        Err(WireError::TooLarge)
    );
    let op = json!([{"op": "set", "path": ["a"], "value": "x".repeat(SMALL_FRAME + 100)}]);
    let f = frame(&with(change_json(), "ops", op));
    assert!(f.len() > SMALL_FRAME);
    assert!(matches!(decode_client(&f, MAX), Ok(ClientMsg::Change(_))));
    // A small frame of any kind is read.
    assert!(matches!(
        decode_client(&ping_with(Plain::Nil), MAX),
        Ok(ClientMsg::Ping)
    ));
    // A second `t` hides the type, so a large frame is not taken for a change.
    let twice = raw(&Plain::Map(vec![
        pair("t", Plain::from("change")),
        pair("t", Plain::from("ping")),
        pair("x", big()),
    ]));
    assert_eq!(decode_client(&twice, MAX), Err(WireError::TooLarge));
}

#[test]
fn the_node_budget_is_exact() {
    use oswx_core::wire::{MAX_NODES, scan};
    // map + "t" + "ping" + "x" + the array + its elements
    let frame = |k: usize| ping_with(Plain::Array(vec![Plain::Nil; k]));
    let exact = frame(MAX_NODES - 5);
    let at = scan(&exact, Some(MAX_NODES)).unwrap();
    assert_eq!((at.nodes, at.t), (MAX_NODES, Some("ping")));
    assert!(matches!(
        scan(&frame(MAX_NODES - 4), Some(MAX_NODES)),
        Err(WireError::Invalid(_))
    ));
    assert_eq!(
        scan(&frame(MAX_NODES - 4), None).unwrap().nodes,
        MAX_NODES + 1
    );
}

#[test]
fn the_scan_refuses_what_runs_past_the_end() {
    use oswx_core::wire::scan;
    let head = [0x81, 0xa1, 0x78];
    let with_tail = |tail: &[u8]| [&head[..], tail].concat();
    let cases: Vec<Vec<u8>> = vec![
        with_tail(&[0xd9, 0xff]),
        with_tail(&[0xd9, 0x03, 0x61, 0x62]),
        with_tail(&[0xda, 0x01]),
        with_tail(&[0xdb, 0xff, 0xff, 0xff, 0xff, 0x61]),
        with_tail(&[0xc4, 0x05, 1, 2]),
        with_tail(&[0xc6, 0xff, 0xff, 0xff, 0xff]),
        with_tail(&[0xc7, 0x05, 0x01, 1, 2]),
        with_tail(&[0xc9, 0xff, 0xff, 0xff, 0xff, 0x01]),
        with_tail(&[0xd8, 0x01, 1, 2]),
        with_tail(&[0xdd, 0xff, 0xff, 0xff, 0xff, 0xc0]),
        with_tail(&[0xdf, 0xff, 0xff, 0xff, 0xff, 0xc0]),
        with_tail(&[0xcb, 0, 0, 0]),
        with_tail(&[0xcf, 0, 0]),
        with_tail(&[0xc1]),
        vec![0x81, 0xa1],
        vec![0x82, 0xa1, 0x78, 0xc0],
        vec![],
    ];
    for case in cases {
        assert!(
            matches!(scan(&case, None), Err(WireError::Invalid(_))),
            "{case:02x?}"
        );
    }
    // Two values.
    let one = frame(&json!({"t": "ping"}));
    assert!(scan(&one, None).is_ok());
    assert!(scan(&[one.clone(), one.clone()].concat(), None).is_err());
    assert!(scan(&[one, vec![0xc0]].concat(), None).is_err());
    // Not a map at the top.
    for top in [vec![0xc0], vec![0x90], vec![0x01], vec![0xa1, 0x74]] {
        assert!(scan(&top, None).is_err());
    }
}

#[test]
fn the_reserved_byte_is_refused_as_a_value() {
    // {"t": "ping", "x": 0xc1}: rmpv would read 0xc1 as nil; the decoder refuses it.
    let mut frame = vec![0x82, 0xa1, b't', 0xa4, b'p', b'i', b'n', b'g', 0xa1, b'x'];
    frame.push(0xc1);
    assert!(matches!(
        decode_client(&frame, 1 << 20),
        Err(WireError::Invalid(_))
    ));
    // The same frame with nil in its place is a ping.
    *frame.last_mut().unwrap() = 0xc0;
    assert_eq!(decode_client(&frame, 1 << 20), Ok(ClientMsg::Ping));
    // As a key, in a frame that is otherwise a ping: {"t": "ping", 0xc1: nil}.
    let key = [0x82, 0xa1, b't', 0xa4, b'p', b'i', b'n', b'g', 0xc1, 0xc0];
    assert!(matches!(
        decode_client(&key, 1 << 20),
        Err(WireError::Invalid(_))
    ));
    // 0xc1 inside a payload (a uint16, a string, binary data) is an ordinary byte.
    for extra in [
        &[0xcd, 0x00, 0xc1][..],
        &[0xa1, 0xc1][..],
        &[0xc4, 0x01, 0xc1][..],
    ] {
        let mut f = vec![0x82, 0xa1, b't', 0xa4, b'p', b'i', b'n', b'g', 0xa1, b'x'];
        f.extend_from_slice(extra);
        let r = decode_client(&f, 1 << 20);
        // A lone 0xc1 is not UTF-8, so the string case is refused for that reason; the scan
        // itself accepts all three.
        assert!(oswx_core::wire::scan(&f, None).is_ok(), "{f:02x?}");
        assert!(!scan_agrees_with_rmpv::reserved_marker(&f), "{f:02x?}");
        if extra[0] != 0xa1 {
            assert_eq!(r, Ok(ClientMsg::Ping), "{f:02x?}");
        }
    }
    // As a value or a key, it is a marker.
    assert!(scan_agrees_with_rmpv::reserved_marker(&key));
    let mut as_value = frame.clone();
    *as_value.last_mut().unwrap() = 0xc1;
    assert!(scan_agrees_with_rmpv::reserved_marker(&as_value));
}

mod scan_agrees_with_rmpv {
    use super::*;
    use proptest::prelude::*;

    fn count(p: &Plain) -> usize {
        match p {
            Plain::Array(items) => 1 + items.iter().map(count).sum::<usize>(),
            Plain::Map(pairs) => {
                1 + pairs
                    .iter()
                    .map(|(k, v)| count(k) + count(v))
                    .sum::<usize>()
            }
            _ => 1,
        }
    }

    fn leaf() -> impl Strategy<Value = Plain> {
        prop_oneof![
            Just(Plain::Nil),
            any::<bool>().prop_map(Plain::Boolean),
            any::<i64>().prop_map(Plain::from),
            any::<u64>().prop_map(Plain::from),
            any::<f64>().prop_map(Plain::F64),
            any::<f32>().prop_map(Plain::F32),
            ".{0,40}".prop_map(Plain::from),
            "[a-z]{0,300}".prop_map(Plain::from),
            prop::collection::vec(any::<u8>(), 0..300).prop_map(Plain::Binary),
            (any::<i8>(), prop::collection::vec(any::<u8>(), 0..20))
                .prop_map(|(t, d)| Plain::Ext(t, d)),
        ]
    }

    fn value() -> impl Strategy<Value = Plain> {
        leaf().prop_recursive(5, 80, 6, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Plain::Array),
                prop::collection::vec((inner.clone(), inner), 0..4).prop_map(Plain::Map),
            ]
        })
    }

    fn root() -> impl Strategy<Value = Plain> {
        prop::collection::vec((value(), value()), 0..5).prop_map(Plain::Map)
    }

    /// Whether 0xc1 stands where a value starts, walking the markers one after another as a
    /// reader does (containers only announce how many values follow, so every marker is met in
    /// turn). Independent of `wire::scan`; stops at the end of the bytes.
    pub fn reserved_marker(bytes: &[u8]) -> bool {
        let be = |at: usize, width: usize| -> Option<usize> {
            let b = bytes.get(at..at + width)?;
            Some(b.iter().fold(0usize, |n, &x| (n << 8) | x as usize))
        };
        let mut at = 0;
        while let Some(&m) = bytes.get(at) {
            at += 1;
            let skip = match m {
                0xc1 => return true,
                0x00..=0x7f | 0x80..=0x9f | 0xc0 | 0xc2 | 0xc3 | 0xe0..=0xff => 0,
                0xa0..=0xbf => (m & 0x1f) as usize,
                0xdc | 0xde => 2,
                0xdd | 0xdf => 4,
                0xcc | 0xd0 => 1,
                0xcd | 0xd1 => 2,
                0xce | 0xd2 | 0xca => 4,
                0xcf | 0xd3 | 0xcb => 8,
                0xd4..=0xd8 => 1 + (1 << (m - 0xd4)),
                0xd9 | 0xc4 => match be(at, 1) {
                    Some(n) => 1 + n,
                    None => return false,
                },
                0xda | 0xc5 => match be(at, 2) {
                    Some(n) => 2 + n,
                    None => return false,
                },
                0xdb | 0xc6 => match be(at, 4) {
                    Some(n) => 4 + n,
                    None => return false,
                },
                0xc7 => match be(at, 1) {
                    Some(n) => 2 + n,
                    None => return false,
                },
                0xc8 => match be(at, 2) {
                    Some(n) => 3 + n,
                    None => return false,
                },
                0xc9 => match be(at, 4) {
                    Some(n) => 5 + n,
                    None => return false,
                },
            };
            at = at.saturating_add(skip);
        }
        false
    }

    /// What rmpv makes of the bytes: one value, all of them used, a map.
    fn reference(bytes: &[u8]) -> Option<usize> {
        let mut rest = bytes;
        let v = rmpv::decode::read_value_with_max_depth(&mut rest, 400).ok()?;
        (rest.is_empty() && v.is_map()).then(|| count(&v))
    }

    proptest! {
        #[test]
        fn counts_nodes_like_a_recursive_walk(root in root()) {
            let bytes = raw(&root);
            let scanned = oswx_core::wire::scan(&bytes, None).unwrap();
            prop_assert_eq!(scanned.nodes, count(&root));
            prop_assert_eq!(reference(&bytes), Some(count(&root)));
        }

        #[test]
        fn accepts_what_rmpv_reads_and_nothing_else(
            root in root(),
            cut in any::<prop::sample::Index>(),
            flip in any::<prop::sample::Index>(),
            bit in 0u8..8,
            tail in prop::option::of(any::<u8>()),
        ) {
            let bytes = raw(&root);
            let mut variants = vec![bytes[..cut.index(bytes.len() + 1)].to_vec()];
            let mut flipped = bytes.clone();
            let at = flip.index(flipped.len());
            flipped[at] ^= 1 << bit;
            variants.push(flipped);
            if let Some(b) = tail {
                variants.push([bytes.clone(), vec![b]].concat());
            }
            for v in variants {
                let scanned = oswx_core::wire::scan(&v, None);
                match reference(&v) {
                    // rmpv reads the reserved marker 0xc1 as nil; the scan refuses it, as it
                    // should (stricter than the parser, never laxer). Only a 0xc1 that stands
                    // where a value starts excuses a refusal; one inside a payload does not.
                    Some(n) => prop_assert!(
                        scanned.as_ref().map(|s| s.nodes) == Ok(n)
                            || (scanned.is_err() && reserved_marker(&v)),
                        "{:02x?}",
                        v
                    ),
                    None => prop_assert!(scanned.is_err(), "{:02x?}", v),
                }
            }
        }
    }
}
