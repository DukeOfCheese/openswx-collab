mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use common::{load, path, to_plain};
use oswx_core::wire::{
    MAX_DRAFT, MAX_PRESENCE, MAX_READS, MAX_REBUILD_ENTRIES, MAX_REBUILD_KEY, MAX_REBUILD_MESSAGE,
};
use oswx_core::{
    Broken, Change, ChangeEvent, ClientMsg, Conflict, ConflictKind, Lock, Op, Peer, Plain, Pos,
    RejectCode, ServerMsg, WireError, decode_client, decode_server, encode_client, encode_server,
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
    }
}

fn lock() -> Lock {
    Lock {
        path: path(&["features", "f1"]),
        client_id: Arc::from("u1.client-01"),
        name: "Ada".into(),
    }
}

fn broken() -> BTreeMap<String, Broken> {
    BTreeMap::from([
        (
            "f1".to_string(),
            Broken {
                error: "no solution".into(),
                by_seq: Some(3),
                by_name: Some("Bob".into()),
                command: Some("edit".into()),
            },
        ),
        (
            "f2".to_string(),
            Broken {
                error: "x".into(),
                by_seq: None,
                by_name: None,
                command: None,
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
        peers: vec![peer("client-02")],
        locks: vec![lock()],
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
        ServerMsg::Change(event(9)),
        ServerMsg::RevertResult {
            request_id: "r1".into(),
            ok: true,
            seq: Some(10),
            ops: all_ops(),
            code: None,
        },
        ServerMsg::RevertResult {
            request_id: "r1".into(),
            ok: false,
            seq: None,
            ops: vec![],
            code: Some("stale".into()),
        },
        ServerMsg::LockChanged {
            locks: vec![lock()],
        },
        ServerMsg::LockChanged { locks: vec![] },
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
        ServerMsg::RebuildStatus { broken: broken() },
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
                    Some(n) => prop_assert_eq!(scanned.map(|s| s.nodes), Ok(n), "{:02x?}", v),
                    None => prop_assert!(scanned.is_err(), "{:02x?}", v),
                }
            }
        }
    }
}
