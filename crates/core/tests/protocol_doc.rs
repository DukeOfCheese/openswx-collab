//! `docs/protocol.md` is the contract with clients: it must name every message of `oswx_core::wire`
//! with every field it carries, every structure inside them, every operation, and every code a
//! client branches on. This test encodes one message of each kind (with every optional field set),
//! reads the field names from the bytes, and looks for them in the document's tables.
//!
//! The matches below have no wildcard arm on purpose: a new message or code does not compile here
//! until it is given a sample, and then fails until the document names it.

use std::collections::BTreeMap;
use std::sync::Arc;

use oswx_core::wire::REVERT_CODES;
use oswx_core::{
    Broken, Change, ChangeEvent, ClientMsg, Conflict, ConflictKind, DenyReason, Lock, LockDenied,
    Op, Path, Peer, Plain, Pos, RejectCode, ServerMsg, Value, encode_client, encode_server,
};

fn doc() -> String {
    let at = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/protocol.md");
    std::fs::read_to_string(at)
        .unwrap_or_else(|e| panic!("docs/protocol.md cannot be read: {e}"))
        .replace("\r\n", "\n")
}

fn path(segments: &[&str]) -> Path {
    segments.iter().map(|s| Arc::from(*s)).collect()
}

fn data() -> Plain {
    Plain::Map(vec![(Plain::from("tool"), Plain::from("sketch"))])
}

fn peer() -> Peer {
    Peer {
        client_id: Arc::from("u.client-1"),
        user_id: "u".into(),
        name: "Ada".into(),
        colour: "#123456".into(),
        read_only: true,
        presence: Some(data()),
        draft: Some(data()),
    }
}

fn lock() -> Lock {
    Lock {
        path: path(&["features", "f1"]),
        client_id: Arc::from("u.client-1"),
        user_id: "u".into(),
        name: "Ada".into(),
        detached: true,
    }
}

fn broken() -> BTreeMap<String, Broken> {
    BTreeMap::from([(
        "f1".to_owned(),
        Broken {
            error: "no profile".into(),
            by_seq: Some(4),
            by_user: Some("u".into()),
            by_name: Some("Ada".into()),
            command: Some("edit".into()),
            since_seq: Some(3),
        },
    )])
}

fn item() -> Value {
    Value::from_plain(&Plain::Map(vec![(Plain::from("id"), Plain::from("i1"))])).unwrap()
}

/// One operation of each kind.
fn ops() -> Vec<Op> {
    let pos = Pos::parse("V").unwrap();
    let all = vec![
        Op::Set {
            path: path(&["k"]),
            value: Value::Int(1),
        },
        Op::Unset { path: path(&["k"]) },
        Op::Insert {
            list: path(&["features"]),
            item: item(),
            pos: pos.clone(),
        },
        Op::Remove {
            list: path(&["features"]),
            id: Arc::from("i1"),
        },
        Op::Move {
            list: path(&["features"]),
            id: Arc::from("i1"),
            pos,
        },
    ];
    for op in &all {
        match op {
            Op::Set { .. }
            | Op::Unset { .. }
            | Op::Insert { .. }
            | Op::Remove { .. }
            | Op::Move { .. } => {}
        }
    }
    all
}

fn event() -> ChangeEvent {
    ChangeEvent {
        seq: 5,
        client_id: Arc::from("u.client-1"),
        author: "u".into(),
        author_name: "Ada".into(),
        change_id: Arc::from("c1"),
        ops: ops(),
        command: Arc::from("edit"),
        undo_of: Some(Arc::from("c0")),
        reverts: Some(4),
    }
}

/// One of every client message, every optional field set.
fn client_messages() -> Vec<ClientMsg> {
    let all = vec![
        ClientMsg::Hello {
            protocol: 2,
            token: "t".into(),
            client_id: "client-1".into(),
            format_version: Some(3),
            last_seq: Some(1),
            app: Some("openswx".into()),
        },
        ClientMsg::Change(Change {
            change_id: Arc::from("c1"),
            base: 1,
            ops: ops(),
            reads: [path(&["features", "f0"])].into_iter().collect(),
            depends: vec![Arc::from("c0")],
            undo_of: Some(Arc::from("c0")),
            command: Arc::from("edit"),
        }),
        ClientMsg::Revert {
            request_id: "r".into(),
            seq: 1,
        },
        ClientMsg::LockAcquire {
            request_id: "r".into(),
            paths: vec![path(&["features", "f1"])],
        },
        ClientMsg::LockRelease {
            paths: vec![path(&["features", "f1"])],
        },
        ClientMsg::Presence { data: data() },
        ClientMsg::Draft { data: Some(data()) },
        ClientMsg::RebuildReport {
            seq: 1,
            errors: BTreeMap::from([("f1".to_owned(), "no profile".to_owned())]),
        },
        ClientMsg::Ping,
    ];
    for m in &all {
        match m {
            ClientMsg::Hello { .. }
            | ClientMsg::Change(_)
            | ClientMsg::Revert { .. }
            | ClientMsg::LockAcquire { .. }
            | ClientMsg::LockRelease { .. }
            | ClientMsg::Presence { .. }
            | ClientMsg::Draft { .. }
            | ClientMsg::RebuildReport { .. }
            | ClientMsg::Ping => {}
        }
    }
    all
}

/// One of every server message, every optional field set.
fn server_messages() -> Vec<ServerMsg> {
    let all = vec![
        ServerMsg::Welcome {
            seq: 5,
            project_id: "p".into(),
            role: "editor".into(),
            read_only: true,
            read_only_reason: Some("viewer".into()),
            you: peer(),
            peers: vec![peer()],
            locks: vec![lock()],
            broken: broken(),
            content: Some(Value::Int(1)),
            changes: Some(vec![event()]),
        },
        ServerMsg::Error {
            code: "bad_message".into(),
            message: "m".into(),
            request_id: Some("r".into()),
        },
        ServerMsg::Accepted {
            change_id: Arc::from("c1"),
            seq: 5,
        },
        ServerMsg::Rejected {
            change_id: Arc::from("c1"),
            code: RejectCode::Stale,
            message: "m".into(),
            conflicts: vec![Conflict {
                path: path(&["features", "f1"]),
                seq: 4,
                by: Arc::from("u.client-2"),
                kind: ConflictKind::Written,
            }],
        },
        ServerMsg::Change(event()),
        ServerMsg::RevertResult {
            request_id: "r".into(),
            ok: false,
            seq: Some(6),
            ops: ops(),
            code: Some("stale".into()),
            message: Some("m".into()),
        },
        ServerMsg::LockResult {
            request_id: "r".into(),
            granted: vec![path(&["features", "f2"])],
            denied: vec![LockDenied {
                path: path(&["features", "f1"]),
                why: DenyReason::Held,
                holder: Some(lock()),
            }],
        },
        ServerMsg::LockChanged {
            locks: vec![lock()],
            released: vec![path(&["features", "f3"])],
        },
        ServerMsg::PeerJoined(peer()),
        ServerMsg::PeerLeft {
            client_id: Arc::from("u.client-1"),
        },
        ServerMsg::Presence {
            client_id: Arc::from("u.client-1"),
            data: data(),
        },
        ServerMsg::Draft {
            client_id: Arc::from("u.client-1"),
            data: Some(data()),
        },
        ServerMsg::PresenceBatch {
            presence: vec![(Arc::from("u.client-1"), data())],
            drafts: vec![(Arc::from("u.client-2"), None)],
        },
        ServerMsg::PeerList {
            peers: vec![peer()],
        },
        ServerMsg::RebuildStatus {
            seq: 5,
            broken: broken(),
        },
        ServerMsg::VersionCreated {
            id: "v".into(),
            name: "Rev A".into(),
            seq: 5,
        },
        ServerMsg::ReferenceUpdate {
            document: "d".into(),
            components: vec!["c".into()],
        },
        ServerMsg::Pong { seq: 5 },
    ];
    for m in &all {
        match m {
            ServerMsg::Welcome { .. }
            | ServerMsg::Error { .. }
            | ServerMsg::Accepted { .. }
            | ServerMsg::Rejected { .. }
            | ServerMsg::Change(_)
            | ServerMsg::RevertResult { .. }
            | ServerMsg::LockResult { .. }
            | ServerMsg::LockChanged { .. }
            | ServerMsg::PeerJoined(_)
            | ServerMsg::PeerLeft { .. }
            | ServerMsg::Presence { .. }
            | ServerMsg::Draft { .. }
            | ServerMsg::PresenceBatch { .. }
            | ServerMsg::PeerList { .. }
            | ServerMsg::RebuildStatus { .. }
            | ServerMsg::VersionCreated { .. }
            | ServerMsg::ReferenceUpdate { .. }
            | ServerMsg::Pong { .. } => {}
        }
    }
    all
}

fn decode(frame: &[u8]) -> Plain {
    let mut rest = frame;
    rmpv::decode::read_value(&mut rest).unwrap()
}

fn fields(map: &Plain) -> Vec<(String, Plain)> {
    let Plain::Map(pairs) = map else {
        panic!("not a map: {map:?}");
    };
    pairs
        .iter()
        .map(|(k, v)| (k.as_str().unwrap().to_owned(), v.clone()))
        .collect()
}

fn field(map: &Plain, name: &str) -> Plain {
    fields(map)
        .into_iter()
        .find(|(k, _)| k == name)
        .unwrap_or_else(|| panic!("no field {name} in {map:?}"))
        .1
}

fn first(list: &Plain) -> Plain {
    match list {
        Plain::Array(items) => items[0].clone(),
        Plain::Map(pairs) => pairs[0].1.clone(),
        _ => panic!("not a list or map: {list:?}"),
    }
}

/// The part of the document from the line `heading` to the next `### ` heading.
fn section<'a>(doc: &'a str, heading: &str) -> &'a str {
    let start = doc
        .find(&format!("\n{heading}\n"))
        .unwrap_or_else(|| panic!("docs/protocol.md has no section {heading:?}"))
        + 1;
    let after = start + heading.len();
    let end = doc[after..].find("\n### ").map_or(doc.len(), |e| after + e);
    &doc[start..end]
}

/// The table row whose first cell is `cell` (as written, backticks included).
fn row<'a>(doc: &'a str, cell: &str) -> Option<&'a str> {
    let start = format!("| {cell} |");
    doc.lines().find(|line| line.starts_with(&start))
}

/// Checks that the row named `cell` names every key of `map` but `skip`; answers what is wrong.
fn check_row(doc: &str, cell: &str, map: &Plain, skip: &str, wrong: &mut Vec<String>) {
    let Some(line) = row(doc, cell) else {
        wrong.push(format!("no table row for {cell}"));
        return;
    };
    for (key, _) in fields(map) {
        if key != skip && !line.contains(&format!("`{key}`")) {
            wrong.push(format!(
                "the row for {cell} does not name the field `{key}`"
            ));
        }
    }
}

#[test]
fn the_document_names_every_message_and_every_field() {
    let doc = doc();
    let mut wrong = Vec::new();
    // (`change`, `presence` and `draft` are both a client's and the server's message: each is
    // looked for in its own table.)
    let client = section(&doc, "### Messages a client sends");
    let server_table = section(&doc, "### Messages the server sends");
    let mut checked = 0;
    for (table, frames) in [
        (
            client,
            client_messages()
                .iter()
                .map(encode_client)
                .collect::<Vec<_>>(),
        ),
        (
            server_table,
            server_messages().iter().map(encode_server).collect(),
        ),
    ] {
        for m in frames.iter().map(|f| decode(f)) {
            let t = field(&m, "t");
            let t = t.as_str().unwrap();
            check_row(table, &format!("`{t}`"), &m, "t", &mut wrong);
            checked += 1;
        }
    }
    assert_eq!(checked, 27, "a sample is missing");

    // The structures inside messages.
    let server: Vec<Plain> = server_messages()
        .iter()
        .map(|m| decode(&encode_server(m)))
        .collect();
    let by_t = |t: &str| {
        server
            .iter()
            .find(|m| field(m, "t").as_str() == Some(t))
            .cloned()
            .unwrap()
    };
    check_row(
        &doc,
        "peer",
        &field(&by_t("peer.joined"), "peer"),
        "",
        &mut wrong,
    );
    check_row(
        &doc,
        "lock",
        &first(&field(&by_t("lock.changed"), "locks")),
        "",
        &mut wrong,
    );
    check_row(
        &doc,
        "denied",
        &first(&field(&by_t("lock.result"), "denied")),
        "",
        &mut wrong,
    );
    check_row(
        &doc,
        "broken",
        &first(&field(&by_t("rebuild.status"), "broken")),
        "",
        &mut wrong,
    );
    check_row(
        &doc,
        "conflict",
        &first(&field(&by_t("change.rejected"), "conflicts")),
        "",
        &mut wrong,
    );

    // Every operation, in its own row.
    for op in field(&by_t("change"), "ops").as_array().unwrap() {
        let name = field(op, "op");
        check_row(
            &doc,
            &format!("`{}`", name.as_str().unwrap()),
            op,
            "op",
            &mut wrong,
        );
    }
    assert!(wrong.is_empty(), "docs/protocol.md:\n{}", wrong.join("\n"));
}

#[test]
fn the_document_names_every_code_a_client_branches_on() {
    let doc = doc();
    let mut codes: Vec<&str> = Vec::new();
    for code in [
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
    ] {
        match code {
            RejectCode::Stale
            | RejectCode::Locked
            | RejectCode::Dependency
            | RejectCode::NameTaken
            | RejectCode::IdTaken
            | RejectCode::Invalid
            | RejectCode::MissingBlob
            | RejectCode::ReadOnly
            | RejectCode::TooOld
            | RejectCode::ServerError => codes.push(code.as_str()),
        }
    }
    for why in [
        DenyReason::Held,
        DenyReason::NotLockable,
        DenyReason::Quota,
        DenyReason::Rate,
        DenyReason::ReadOnly,
    ] {
        match why {
            DenyReason::Held
            | DenyReason::NotLockable
            | DenyReason::Quota
            | DenyReason::Rate
            | DenyReason::ReadOnly => codes.push(why.as_str()),
        }
    }
    for kind in [
        ConflictKind::Written,
        ConflictKind::Removed,
        ConflictKind::Read,
        ConflictKind::Locked,
    ] {
        codes.push(match kind {
            ConflictKind::Written => "written",
            ConflictKind::Removed => "removed",
            ConflictKind::Read => "read",
            ConflictKind::Locked => "locked",
        });
    }
    codes.extend(REVERT_CODES);
    let missing: Vec<&str> = codes
        .into_iter()
        .filter(|code| !doc.contains(&format!("`{code}`")))
        .collect();
    assert!(
        missing.is_empty(),
        "docs/protocol.md does not name {missing:?}"
    );
    // The protocol version the document is about.
    assert!(
        doc.starts_with(&format!(
            "# Protocol (version {})",
            oswx_core::PROTOCOL_VERSION
        )),
        "docs/protocol.md is not about protocol version {}",
        oswx_core::PROTOCOL_VERSION
    );
}

#[test]
fn a_message_missing_from_the_document_is_found() {
    // The check itself: a row that leaves out a field, and a message with no row, are reported.
    let doc = "| `pong` | | answered |\n";
    let pong = decode(&encode_server(&ServerMsg::Pong { seq: 1 }));
    let mut wrong = Vec::new();
    check_row(doc, "`pong`", &pong, "t", &mut wrong);
    assert_eq!(wrong, ["the row for `pong` does not name the field `seq`"]);
    let mut wrong = Vec::new();
    check_row(doc, "`welcome`", &pong, "t", &mut wrong);
    assert_eq!(wrong, ["no table row for `welcome`"]);
}
