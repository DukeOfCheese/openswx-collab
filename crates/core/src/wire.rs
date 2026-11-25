//! Protocol v2 messages: MessagePack maps with a string field `t` saying which message it is.
//!
//! Everything a client sends is hostile. `decode_client` checks the frame's size before parsing,
//! rebuilds each message from the fields it knows (unknown fields are ignored, a repeated known
//! field is refused), and gives every field a type and a limit. Encoding is deterministic: fields
//! are written in a fixed order and optional fields that are `None` are left out.
//!
//! A field that is optional on the wire may also be sent as `nil`. Presence and draft data are
//! relayed opaquely as [`Plain`]; they are only checked, never converted to a [`Value`].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;

use crate::index::{ClientId, Conflict, ConflictKind, Seq};
use crate::log::Change;
use crate::op::{MAX_DEPTH, Op};
use crate::path::{self, Path};
use crate::value::{Plain, Value};

pub const PROTOCOL_VERSION: u32 = 2;

pub const MAX_TOKEN: usize = 512;
pub const MIN_CLIENT_ID: usize = 8;
pub const MAX_CLIENT_ID: usize = 64;
pub const MAX_APP: usize = 100;
pub const MAX_CHANGE_ID: usize = 64;
pub const MAX_REQUEST_ID: usize = 64;
pub const MAX_OPS: usize = 10_000;
pub const MAX_READS: usize = 1_000;
pub const MAX_DEPENDS: usize = 64;
pub const MAX_COMMAND: usize = 200;
pub const MAX_LOCK_PATHS: usize = 256;
pub const MAX_PRESENCE: usize = 16 * 1024;
pub const MAX_DRAFT: usize = 128 * 1024;
pub const MAX_REBUILD_ENTRIES: usize = 500;
pub const MAX_REBUILD_KEY: usize = 200;
pub const MAX_REBUILD_MESSAGE: usize = 500;

/// How deep the parser may go. The parser spends two levels on each container and up to three on
/// a leaf, so this leaves room for the envelope, an op and a value 64 containers deep; the exact
/// limit for values is checked where they are read.
const PARSE_DEPTH: usize = 160;

#[derive(thiserror::Error, Clone, Debug, PartialEq)]
pub enum WireError {
    /// The frame is longer than the caller accepts.
    #[error("frame is too large")]
    TooLarge,
    /// Not a message, or a field of it has the wrong type or breaks a limit.
    #[error("invalid message: {0}")]
    Invalid(String),
}

fn invalid<T>(why: impl Into<String>) -> Result<T, WireError> {
    Err(WireError::Invalid(why.into()))
}

// ---------------------------------------------------------------------------------------- types

#[derive(Clone, Debug, PartialEq)]
pub enum ClientMsg {
    Hello {
        protocol: u32,
        token: String,
        client_id: String,
        format_version: Option<i64>,
        last_seq: Option<Seq>,
        app: Option<String>,
    },
    Change(Change),
    Revert {
        request_id: String,
        seq: Seq,
    },
    LockAcquire {
        request_id: String,
        paths: Vec<Path>,
    },
    LockRelease {
        paths: Vec<Path>,
    },
    Presence {
        data: Plain,
    },
    Draft {
        data: Option<Plain>,
    },
    RebuildReport {
        seq: Seq,
        errors: BTreeMap<String, String>,
    },
    Ping,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectCode {
    Stale,
    Locked,
    Dependency,
    NameTaken,
    IdTaken,
    Invalid,
    MissingBlob,
    ReadOnly,
    TooOld,
    ServerError,
}

const REJECT_CODES: [(RejectCode, &str); 10] = [
    (RejectCode::Stale, "stale"),
    (RejectCode::Locked, "locked"),
    (RejectCode::Dependency, "dependency"),
    (RejectCode::NameTaken, "name_taken"),
    (RejectCode::IdTaken, "id_taken"),
    (RejectCode::Invalid, "invalid"),
    (RejectCode::MissingBlob, "missing_blob"),
    (RejectCode::ReadOnly, "read_only"),
    (RejectCode::TooOld, "too_old"),
    (RejectCode::ServerError, "server_error"),
];

impl RejectCode {
    pub fn as_str(self) -> &'static str {
        REJECT_CODES
            .iter()
            .find(|(c, _)| *c == self)
            .map_or("invalid", |(_, s)| s)
    }

    pub fn parse(s: &str) -> Option<RejectCode> {
        REJECT_CODES.iter().find(|(_, n)| *n == s).map(|(c, _)| *c)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Peer {
    pub client_id: ClientId,
    pub user_id: String,
    pub name: String,
    pub colour: String,
    pub presence: Option<Plain>,
    pub draft: Option<Plain>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Lock {
    pub path: Path,
    pub client_id: ClientId,
    pub name: String,
}

/// A feature that does not rebuild, and the change that broke it if known.
#[derive(Clone, Debug, PartialEq)]
pub struct Broken {
    pub error: String,
    pub by_seq: Option<Seq>,
    pub by_name: Option<String>,
    pub command: Option<String>,
}

/// An accepted change as the server tells everyone about it.
#[derive(Clone, Debug, PartialEq)]
pub struct ChangeEvent {
    pub seq: Seq,
    pub client_id: ClientId,
    /// User id.
    pub author: String,
    pub author_name: String,
    pub change_id: Arc<str>,
    pub ops: Vec<Op>,
    pub command: Arc<str>,
    pub undo_of: Option<Arc<str>>,
}

// A welcome is sent once per connection; boxing its fields would only complicate every caller.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum ServerMsg {
    Welcome {
        seq: Seq,
        project_id: String,
        role: String,
        read_only: bool,
        read_only_reason: Option<String>,
        you: Peer,
        peers: Vec<Peer>,
        locks: Vec<Lock>,
        broken: BTreeMap<String, Broken>,
        content: Option<Value>,
        changes: Option<Vec<ChangeEvent>>,
    },
    Error {
        code: String,
        message: String,
        request_id: Option<String>,
    },
    Accepted {
        change_id: Arc<str>,
        seq: Seq,
    },
    Rejected {
        change_id: Arc<str>,
        code: RejectCode,
        message: String,
        conflicts: Vec<Conflict>,
    },
    Change(ChangeEvent),
    RevertResult {
        request_id: String,
        ok: bool,
        seq: Option<Seq>,
        ops: Vec<Op>,
        code: Option<String>,
    },
    LockChanged {
        locks: Vec<Lock>,
    },
    PeerJoined(Peer),
    PeerLeft {
        client_id: ClientId,
    },
    Presence {
        client_id: ClientId,
        data: Plain,
    },
    Draft {
        client_id: ClientId,
        data: Option<Plain>,
    },
    RebuildStatus {
        broken: BTreeMap<String, Broken>,
    },
    VersionCreated {
        id: String,
        name: String,
        seq: Seq,
    },
    ReferenceUpdate {
        document: String,
        components: Vec<String>,
    },
    Pong {
        seq: Seq,
    },
}

// ------------------------------------------------------------------------------------ reading

/// The fields of a map. Keys must be strings; a field is looked up by name, and a name given
/// twice is refused when it is looked up.
struct Fields<'a>(Vec<(&'a str, &'a Plain)>);

impl<'a> Fields<'a> {
    fn new(p: &'a Plain, what: &str) -> Result<Fields<'a>, WireError> {
        let Plain::Map(pairs) = p else {
            return invalid(format!("{what} must be a map"));
        };
        let mut out = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            let Some(k) = k.as_str() else {
                return invalid(format!("{what} has a key that is not a string"));
            };
            out.push((k, v));
        }
        Ok(Fields(out))
    }

    fn find(&self, key: &str) -> Result<Option<&'a Plain>, WireError> {
        let mut found = self.0.iter().filter(|(k, _)| *k == key);
        let first = found.next().map(|(_, v)| *v);
        if found.next().is_some() {
            return invalid(format!("repeated field {key}"));
        }
        Ok(first)
    }

    fn req(&self, key: &str) -> Result<&'a Plain, WireError> {
        match self.find(key)? {
            Some(p) => Ok(p),
            None => invalid(format!("missing field {key}")),
        }
    }

    /// Absent or `nil` is `None`.
    fn opt(&self, key: &str) -> Result<Option<&'a Plain>, WireError> {
        Ok(self.find(key)?.filter(|p| !p.is_nil()))
    }
}

/// A string of `min..=max` characters.
fn text<'a>(p: &'a Plain, field: &str, min: usize, max: usize) -> Result<&'a str, WireError> {
    let Some(s) = p.as_str() else {
        return invalid(format!("{field} must be a string"));
    };
    let n = s.chars().count();
    if n < min || n > max {
        return invalid(format!("{field} must be {min} to {max} characters"));
    }
    Ok(s)
}

fn string(p: &Plain, field: &str) -> Result<String, WireError> {
    text(p, field, 0, usize::MAX).map(str::to_owned)
}

fn shared(p: &Plain, field: &str, min: usize, max: usize) -> Result<Arc<str>, WireError> {
    text(p, field, min, max).map(Arc::from)
}

fn uint(p: &Plain, field: &str) -> Result<u64, WireError> {
    match p.as_u64() {
        Some(n) => Ok(n),
        None => invalid(format!("{field} must be a non-negative integer")),
    }
}

fn int(p: &Plain, field: &str) -> Result<i64, WireError> {
    match p.as_i64() {
        Some(n) => Ok(n),
        None => invalid(format!("{field} must be an integer")),
    }
}

fn flag(p: &Plain, field: &str) -> Result<bool, WireError> {
    match p.as_bool() {
        Some(b) => Ok(b),
        None => invalid(format!("{field} must be a boolean")),
    }
}

fn list<'a>(p: &'a Plain, field: &str, min: usize, max: usize) -> Result<&'a [Plain], WireError> {
    let Plain::Array(items) = p else {
        return invalid(format!("{field} must be a list"));
    };
    if items.len() < min || items.len() > max {
        return invalid(format!("{field} must have {min} to {max} items"));
    }
    Ok(items)
}

fn strings(p: &Plain, field: &str) -> Result<Vec<String>, WireError> {
    list(p, field, 0, usize::MAX)?
        .iter()
        .map(|s| string(s, field))
        .collect()
}

fn one_path(p: &Plain, field: &str) -> Result<Path, WireError> {
    path::from_wire(p, field).map_err(|e| WireError::Invalid(format!("{field}: {e}")))
}

fn paths(p: &Plain, field: &str, max: usize) -> Result<Vec<Path>, WireError> {
    list(p, field, 0, max)?
        .iter()
        .map(|p| one_path(p, field))
        .collect()
}

fn ops(p: &Plain, field: &str, min: usize, max: usize) -> Result<Vec<Op>, WireError> {
    list(p, field, min, max)?
        .iter()
        .enumerate()
        .map(|(i, o)| {
            Op::from_wire(o).map_err(|e| WireError::Invalid(format!("{field}[{i}]: {e}")))
        })
        .collect()
}

/// Checks data that is relayed without being understood: finite numbers, string keys, no binary,
/// no deeper than `MAX_DEPTH` containers.
fn check_opaque(p: &Plain, field: &str, depth: usize) -> Result<(), WireError> {
    match p {
        Plain::Nil | Plain::Boolean(_) | Plain::Integer(_) | Plain::String(_) => Ok(()),
        Plain::F32(f) if f.is_finite() => Ok(()),
        Plain::F64(f) if f.is_finite() => Ok(()),
        Plain::F32(_) | Plain::F64(_) => invalid(format!("{field} holds NaN or infinity")),
        Plain::Binary(_) | Plain::Ext(..) => invalid(format!("{field} holds binary data")),
        Plain::Array(items) => {
            if depth + 1 > MAX_DEPTH {
                return invalid(format!("{field} is nested too deeply"));
            }
            items
                .iter()
                .try_for_each(|i| check_opaque(i, field, depth + 1))
        }
        Plain::Map(pairs) => {
            if depth + 1 > MAX_DEPTH {
                return invalid(format!("{field} is nested too deeply"));
            }
            for (k, v) in pairs {
                if !k.is_str() {
                    return invalid(format!("{field} has a key that is not a string"));
                }
                check_opaque(v, field, depth + 1)?;
            }
            Ok(())
        }
    }
}

fn encoded_len(p: &Plain) -> usize {
    let mut buf = Vec::new();
    // Writing to a Vec cannot fail.
    let _ = rmpv::encode::write_value(&mut buf, p);
    buf.len()
}

/// A map relayed as it is; its encoded size is at most `max` bytes when a limit is given.
fn opaque_map(p: &Plain, field: &str, max: Option<usize>) -> Result<Plain, WireError> {
    if !p.is_map() {
        return invalid(format!("{field} must be a map"));
    }
    check_opaque(p, field, 0)?;
    if max.is_some_and(|max| encoded_len(p) > max) {
        return invalid(format!("{field} is too large"));
    }
    Ok(p.clone())
}

fn parse(frame: &[u8]) -> Result<Plain, WireError> {
    let mut rest = frame;
    let value = match rmpv::decode::read_value_with_max_depth(&mut rest, PARSE_DEPTH) {
        Ok(v) => v,
        Err(_) => return invalid("not MessagePack"),
    };
    if !rest.is_empty() {
        return invalid("bytes after the message");
    }
    Ok(value)
}

// ------------------------------------------------------------------------------------ writing

/// A map being built, fields in the order they are put.
struct Out(Vec<(Plain, Plain)>);

impl Out {
    fn new(t: &str) -> Out {
        Out(vec![(Plain::from("t"), Plain::from(t))])
    }

    fn put(mut self, key: &str, v: impl Into<Plain>) -> Out {
        self.0.push((Plain::from(key), v.into()));
        self
    }

    fn put_opt(self, key: &str, v: Option<Plain>) -> Out {
        match v {
            Some(v) => self.put(key, v),
            None => self,
        }
    }

    fn bytes(self) -> Bytes {
        let mut buf = Vec::new();
        // Writing to a Vec cannot fail.
        let _ = rmpv::encode::write_value(&mut buf, &Plain::Map(self.0));
        Bytes::from(buf)
    }
}

fn s(v: &str) -> Plain {
    Plain::from(v)
}

fn opt_s(v: &Option<impl AsRef<str>>) -> Option<Plain> {
    v.as_ref().map(|v| s(v.as_ref()))
}

fn arr<T>(items: &[T], f: impl Fn(&T) -> Plain) -> Plain {
    Plain::Array(items.iter().map(f).collect())
}

fn map_of(pairs: Vec<(&str, Plain)>) -> Plain {
    Plain::Map(pairs.into_iter().map(|(k, v)| (s(k), v)).collect())
}

fn op_list(items: &[Op]) -> Plain {
    arr(items, Op::to_wire)
}

// ------------------------------------------------------------------------------------ client

pub fn encode_client(msg: &ClientMsg) -> Bytes {
    match msg {
        ClientMsg::Hello {
            protocol,
            token,
            client_id,
            format_version,
            last_seq,
            app,
        } => Out::new("hello")
            .put("protocol", u64::from(*protocol))
            .put("token", s(token))
            .put("client_id", s(client_id))
            .put_opt("format_version", format_version.map(Plain::from))
            .put_opt("last_seq", last_seq.map(Plain::from))
            .put_opt("app", opt_s(app))
            .bytes(),
        ClientMsg::Change(c) => Out::new("change")
            .put("change_id", s(&c.change_id))
            .put("base_seq", c.base)
            .put("ops", op_list(&c.ops))
            .put(
                "reads",
                Plain::Array(c.reads.iter().map(path::to_wire).collect()),
            )
            .put("depends", arr(&c.depends, |d| s(d)))
            .put_opt("undo_of", opt_s(&c.undo_of))
            .put("command", s(&c.command))
            .bytes(),
        ClientMsg::Revert { request_id, seq } => Out::new("change.revert")
            .put("request_id", s(request_id))
            .put("seq", *seq)
            .bytes(),
        ClientMsg::LockAcquire { request_id, paths } => Out::new("lock.acquire")
            .put("request_id", s(request_id))
            .put("paths", arr(paths, path::to_wire))
            .bytes(),
        ClientMsg::LockRelease { paths } => Out::new("lock.release")
            .put("paths", arr(paths, path::to_wire))
            .bytes(),
        ClientMsg::Presence { data } => Out::new("presence").put("data", data.clone()).bytes(),
        ClientMsg::Draft { data } => Out::new("draft")
            .put("data", data.clone().unwrap_or(Plain::Nil))
            .bytes(),
        ClientMsg::RebuildReport { seq, errors } => Out::new("rebuild.report")
            .put("seq", *seq)
            .put(
                "errors",
                Plain::Map(errors.iter().map(|(k, v)| (s(k), s(v))).collect()),
            )
            .bytes(),
        ClientMsg::Ping => Out::new("ping").bytes(),
    }
}

/// Reads one frame from a client. `max` is the largest frame accepted.
pub fn decode_client(frame: &[u8], max: usize) -> Result<ClientMsg, WireError> {
    if frame.len() > max {
        return Err(WireError::TooLarge);
    }
    let root = parse(frame)?;
    let f = Fields::new(&root, "a frame")?;
    let t = text(f.req("t")?, "t", 0, MAX_NAME_OF_T)?;
    match t {
        "hello" => Ok(ClientMsg::Hello {
            protocol: u32::try_from(uint(f.req("protocol")?, "protocol")?)
                .or_else(|_| invalid("protocol is out of range"))?,
            token: text(f.req("token")?, "token", 1, MAX_TOKEN)?.to_owned(),
            client_id: text(
                f.req("client_id")?,
                "client_id",
                MIN_CLIENT_ID,
                MAX_CLIENT_ID,
            )?
            .to_owned(),
            format_version: f
                .opt("format_version")?
                .map(|p| int(p, "format_version"))
                .transpose()?,
            last_seq: f
                .opt("last_seq")?
                .map(|p| uint(p, "last_seq"))
                .transpose()?,
            app: f
                .opt("app")?
                .map(|p| text(p, "app", 0, MAX_APP).map(str::to_owned))
                .transpose()?,
        }),
        "change" => Ok(ClientMsg::Change(Change {
            change_id: shared(f.req("change_id")?, "change_id", 1, MAX_CHANGE_ID)?,
            base: uint(f.req("base_seq")?, "base_seq")?,
            ops: ops(f.req("ops")?, "ops", 1, MAX_OPS)?,
            reads: match f.opt("reads")? {
                Some(p) => paths(p, "reads", MAX_READS)?.into_iter().collect(),
                None => BTreeSet::new(),
            },
            depends: match f.opt("depends")? {
                Some(p) => list(p, "depends", 0, MAX_DEPENDS)?
                    .iter()
                    .map(|d| shared(d, "depends", 1, MAX_CHANGE_ID))
                    .collect::<Result<_, _>>()?,
                None => Vec::new(),
            },
            undo_of: f
                .opt("undo_of")?
                .map(|p| shared(p, "undo_of", 1, MAX_CHANGE_ID))
                .transpose()?,
            command: match f.opt("command")? {
                Some(p) => shared(p, "command", 0, MAX_COMMAND)?,
                None => Arc::from(""),
            },
        })),
        "change.revert" => Ok(ClientMsg::Revert {
            request_id: text(f.req("request_id")?, "request_id", 0, MAX_REQUEST_ID)?.to_owned(),
            seq: uint(f.req("seq")?, "seq")?,
        }),
        "lock.acquire" => Ok(ClientMsg::LockAcquire {
            request_id: text(f.req("request_id")?, "request_id", 0, MAX_REQUEST_ID)?.to_owned(),
            paths: lock_paths(&f)?,
        }),
        "lock.release" => Ok(ClientMsg::LockRelease {
            paths: lock_paths(&f)?,
        }),
        "presence" => Ok(ClientMsg::Presence {
            data: opaque_map(f.req("data")?, "data", Some(MAX_PRESENCE))?,
        }),
        "draft" => Ok(ClientMsg::Draft {
            data: f
                .opt("data")?
                .map(|p| opaque_map(p, "data", Some(MAX_DRAFT)))
                .transpose()?,
        }),
        "rebuild.report" => Ok(ClientMsg::RebuildReport {
            seq: uint(f.req("seq")?, "seq")?,
            errors: rebuild_errors(f.req("errors")?)?,
        }),
        "ping" => Ok(ClientMsg::Ping),
        _ => invalid("unknown message type"),
    }
}

/// Longest `t` read; nothing legitimate is longer, and this keeps a huge string out of an error.
const MAX_NAME_OF_T: usize = 64;

fn lock_paths(f: &Fields) -> Result<Vec<Path>, WireError> {
    let items = f.req("paths")?;
    let out = paths(items, "paths", MAX_LOCK_PATHS)?;
    if out.is_empty() {
        return invalid("paths must have 1 to 256 items");
    }
    Ok(out)
}

fn rebuild_errors(p: &Plain) -> Result<BTreeMap<String, String>, WireError> {
    let Plain::Map(pairs) = p else {
        return invalid("errors must be a map");
    };
    if pairs.len() > MAX_REBUILD_ENTRIES {
        return invalid(format!(
            "errors must have at most {MAX_REBUILD_ENTRIES} entries"
        ));
    }
    let mut out = BTreeMap::new();
    for (k, v) in pairs {
        let key = text(k, "errors key", 0, MAX_REBUILD_KEY)?.to_owned();
        let message = text(v, "errors message", 0, MAX_REBUILD_MESSAGE)?.to_owned();
        if out.insert(key, message).is_some() {
            return invalid("errors repeats a key");
        }
    }
    Ok(out)
}

// ------------------------------------------------------------------------------------ server

fn peer_to_wire(p: &Peer) -> Plain {
    let mut pairs = vec![
        ("client_id", s(&p.client_id)),
        ("user_id", s(&p.user_id)),
        ("name", s(&p.name)),
        ("colour", s(&p.colour)),
    ];
    if let Some(v) = &p.presence {
        pairs.push(("presence", v.clone()));
    }
    if let Some(v) = &p.draft {
        pairs.push(("draft", v.clone()));
    }
    map_of(pairs)
}

fn peer_from_wire(p: &Plain) -> Result<Peer, WireError> {
    let f = Fields::new(p, "a peer")?;
    Ok(Peer {
        client_id: shared(f.req("client_id")?, "client_id", 0, usize::MAX)?,
        user_id: string(f.req("user_id")?, "user_id")?,
        name: string(f.req("name")?, "name")?,
        colour: string(f.req("colour")?, "colour")?,
        presence: f
            .opt("presence")?
            .map(|p| opaque_map(p, "presence", None))
            .transpose()?,
        draft: f
            .opt("draft")?
            .map(|p| opaque_map(p, "draft", None))
            .transpose()?,
    })
}

fn lock_to_wire(l: &Lock) -> Plain {
    map_of(vec![
        ("path", path::to_wire(&l.path)),
        ("client_id", s(&l.client_id)),
        ("name", s(&l.name)),
    ])
}

fn lock_from_wire(p: &Plain) -> Result<Lock, WireError> {
    let f = Fields::new(p, "a lock")?;
    Ok(Lock {
        path: one_path(f.req("path")?, "path")?,
        client_id: shared(f.req("client_id")?, "client_id", 0, usize::MAX)?,
        name: string(f.req("name")?, "name")?,
    })
}

fn locks_from_wire(p: &Plain) -> Result<Vec<Lock>, WireError> {
    list(p, "locks", 0, usize::MAX)?
        .iter()
        .map(lock_from_wire)
        .collect()
}

fn broken_to_wire(b: &Broken) -> Plain {
    let mut pairs = vec![("error", s(&b.error))];
    if let Some(v) = b.by_seq {
        pairs.push(("by_seq", Plain::from(v)));
    }
    if let Some(v) = &b.by_name {
        pairs.push(("by_name", s(v)));
    }
    if let Some(v) = &b.command {
        pairs.push(("command", s(v)));
    }
    map_of(pairs)
}

fn broken_from_wire(p: &Plain) -> Result<Broken, WireError> {
    let f = Fields::new(p, "a broken entry")?;
    Ok(Broken {
        error: string(f.req("error")?, "error")?,
        by_seq: f.opt("by_seq")?.map(|p| uint(p, "by_seq")).transpose()?,
        by_name: f
            .opt("by_name")?
            .map(|p| string(p, "by_name"))
            .transpose()?,
        command: f
            .opt("command")?
            .map(|p| string(p, "command"))
            .transpose()?,
    })
}

fn broken_map_to_wire(m: &BTreeMap<String, Broken>) -> Plain {
    Plain::Map(m.iter().map(|(k, v)| (s(k), broken_to_wire(v))).collect())
}

fn broken_map_from_wire(p: &Plain) -> Result<BTreeMap<String, Broken>, WireError> {
    let Plain::Map(pairs) = p else {
        return invalid("broken must be a map");
    };
    let mut out = BTreeMap::new();
    for (k, v) in pairs {
        let key = string(k, "broken key")?;
        if out.insert(key, broken_from_wire(v)?).is_some() {
            return invalid("broken repeats a key");
        }
    }
    Ok(out)
}

fn event_pairs(e: &ChangeEvent) -> Vec<(&'static str, Plain)> {
    let mut pairs = vec![
        ("seq", Plain::from(e.seq)),
        ("client_id", s(&e.client_id)),
        ("author", s(&e.author)),
        ("author_name", s(&e.author_name)),
        ("change_id", s(&e.change_id)),
        ("ops", op_list(&e.ops)),
        ("command", s(&e.command)),
    ];
    if let Some(u) = &e.undo_of {
        pairs.push(("undo_of", s(u)));
    }
    pairs
}

fn event_from_fields(f: &Fields) -> Result<ChangeEvent, WireError> {
    Ok(ChangeEvent {
        seq: uint(f.req("seq")?, "seq")?,
        client_id: shared(f.req("client_id")?, "client_id", 0, usize::MAX)?,
        author: string(f.req("author")?, "author")?,
        author_name: string(f.req("author_name")?, "author_name")?,
        change_id: shared(f.req("change_id")?, "change_id", 0, usize::MAX)?,
        ops: ops(f.req("ops")?, "ops", 0, usize::MAX)?,
        command: shared(f.req("command")?, "command", 0, usize::MAX)?,
        undo_of: f
            .opt("undo_of")?
            .map(|p| shared(p, "undo_of", 0, usize::MAX))
            .transpose()?,
    })
}

fn conflict_to_wire(c: &Conflict) -> Plain {
    let kind = match c.kind {
        ConflictKind::Written => "written",
        ConflictKind::Removed => "removed",
        ConflictKind::Read => "read",
    };
    map_of(vec![
        ("path", path::to_wire(&c.path)),
        ("seq", Plain::from(c.seq)),
        ("by", s(&c.by)),
        ("kind", s(kind)),
    ])
}

fn conflict_from_wire(p: &Plain) -> Result<Conflict, WireError> {
    let f = Fields::new(p, "a conflict")?;
    let kind = match text(f.req("kind")?, "kind", 0, usize::MAX)? {
        "written" => ConflictKind::Written,
        "removed" => ConflictKind::Removed,
        "read" => ConflictKind::Read,
        _ => return invalid("unknown conflict kind"),
    };
    Ok(Conflict {
        path: one_path(f.req("path")?, "path")?,
        seq: uint(f.req("seq")?, "seq")?,
        by: shared(f.req("by")?, "by", 0, usize::MAX)?,
        kind,
    })
}

pub fn encode_server(msg: &ServerMsg) -> Bytes {
    match msg {
        ServerMsg::Welcome {
            seq,
            project_id,
            role,
            read_only,
            read_only_reason,
            you,
            peers,
            locks,
            broken,
            content,
            changes,
        } => Out::new("welcome")
            .put("seq", *seq)
            .put("project_id", s(project_id))
            .put("role", s(role))
            .put("read_only", *read_only)
            .put_opt("read_only_reason", opt_s(read_only_reason))
            .put("you", peer_to_wire(you))
            .put("peers", arr(peers, peer_to_wire))
            .put("locks", arr(locks, lock_to_wire))
            .put("broken", broken_map_to_wire(broken))
            .put_opt("content", content.as_ref().map(Value::to_wire))
            .put_opt(
                "changes",
                changes.as_ref().map(|c| arr(c, |e| map_of(event_pairs(e)))),
            )
            .bytes(),
        ServerMsg::Error {
            code,
            message,
            request_id,
        } => Out::new("error")
            .put("code", s(code))
            .put("message", s(message))
            .put_opt("request_id", opt_s(request_id))
            .bytes(),
        ServerMsg::Accepted { change_id, seq } => Out::new("change.accepted")
            .put("change_id", s(change_id))
            .put("seq", *seq)
            .bytes(),
        ServerMsg::Rejected {
            change_id,
            code,
            message,
            conflicts,
        } => Out::new("change.rejected")
            .put("change_id", s(change_id))
            .put("code", s(code.as_str()))
            .put("message", s(message))
            .put("conflicts", arr(conflicts, conflict_to_wire))
            .bytes(),
        ServerMsg::Change(e) => {
            let mut out = Out::new("change");
            for (k, v) in event_pairs(e) {
                out = out.put(k, v);
            }
            out.bytes()
        }
        ServerMsg::RevertResult {
            request_id,
            ok,
            seq,
            ops,
            code,
        } => Out::new("change.revert.result")
            .put("request_id", s(request_id))
            .put("ok", *ok)
            .put_opt("seq", seq.map(Plain::from))
            .put("ops", op_list(ops))
            .put_opt("code", opt_s(code))
            .bytes(),
        ServerMsg::LockChanged { locks } => Out::new("lock.changed")
            .put("locks", arr(locks, lock_to_wire))
            .bytes(),
        ServerMsg::PeerJoined(p) => Out::new("peer.joined").put("peer", peer_to_wire(p)).bytes(),
        ServerMsg::PeerLeft { client_id } => {
            Out::new("peer.left").put("client_id", s(client_id)).bytes()
        }
        ServerMsg::Presence { client_id, data } => Out::new("presence")
            .put("client_id", s(client_id))
            .put("data", data.clone())
            .bytes(),
        ServerMsg::Draft { client_id, data } => Out::new("draft")
            .put("client_id", s(client_id))
            .put("data", data.clone().unwrap_or(Plain::Nil))
            .bytes(),
        ServerMsg::RebuildStatus { broken } => Out::new("rebuild.status")
            .put("broken", broken_map_to_wire(broken))
            .bytes(),
        ServerMsg::VersionCreated { id, name, seq } => Out::new("version.created")
            .put("id", s(id))
            .put("name", s(name))
            .put("seq", *seq)
            .bytes(),
        ServerMsg::ReferenceUpdate {
            document,
            components,
        } => Out::new("reference.update")
            .put("document", s(document))
            .put("components", arr(components, |c| s(c)))
            .bytes(),
        ServerMsg::Pong { seq } => Out::new("pong").put("seq", *seq).bytes(),
    }
}

/// Reads one frame from the server, as strictly about types as `decode_client`. It has no size
/// limit: the server is trusted not to send more than a client can take.
pub fn decode_server(frame: &[u8]) -> Result<ServerMsg, WireError> {
    let root = parse(frame)?;
    let f = Fields::new(&root, "a frame")?;
    let t = text(f.req("t")?, "t", 0, MAX_NAME_OF_T)?;
    let client_id = |key: &str| shared(f.req(key)?, key, 0, usize::MAX);
    match t {
        "welcome" => Ok(ServerMsg::Welcome {
            seq: uint(f.req("seq")?, "seq")?,
            project_id: string(f.req("project_id")?, "project_id")?,
            role: string(f.req("role")?, "role")?,
            read_only: flag(f.req("read_only")?, "read_only")?,
            read_only_reason: f
                .opt("read_only_reason")?
                .map(|p| string(p, "read_only_reason"))
                .transpose()?,
            you: peer_from_wire(f.req("you")?)?,
            peers: list(f.req("peers")?, "peers", 0, usize::MAX)?
                .iter()
                .map(peer_from_wire)
                .collect::<Result<_, _>>()?,
            locks: locks_from_wire(f.req("locks")?)?,
            broken: broken_map_from_wire(f.req("broken")?)?,
            content: f
                .opt("content")?
                .map(|p| {
                    Value::from_wire(p).map_err(|e| WireError::Invalid(format!("content: {e}")))
                })
                .transpose()?,
            changes: f
                .opt("changes")?
                .map(|p| {
                    list(p, "changes", 0, usize::MAX)?
                        .iter()
                        .map(|e| event_from_fields(&Fields::new(e, "a change")?))
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?,
        }),
        "error" => Ok(ServerMsg::Error {
            code: string(f.req("code")?, "code")?,
            message: string(f.req("message")?, "message")?,
            request_id: f
                .opt("request_id")?
                .map(|p| string(p, "request_id"))
                .transpose()?,
        }),
        "change.accepted" => Ok(ServerMsg::Accepted {
            change_id: shared(f.req("change_id")?, "change_id", 0, usize::MAX)?,
            seq: uint(f.req("seq")?, "seq")?,
        }),
        "change.rejected" => {
            let code = text(f.req("code")?, "code", 0, usize::MAX)?;
            Ok(ServerMsg::Rejected {
                change_id: shared(f.req("change_id")?, "change_id", 0, usize::MAX)?,
                code: match RejectCode::parse(code) {
                    Some(c) => c,
                    None => return invalid("unknown rejection code"),
                },
                message: string(f.req("message")?, "message")?,
                conflicts: list(f.req("conflicts")?, "conflicts", 0, usize::MAX)?
                    .iter()
                    .map(conflict_from_wire)
                    .collect::<Result<_, _>>()?,
            })
        }
        "change" => Ok(ServerMsg::Change(event_from_fields(&f)?)),
        "change.revert.result" => Ok(ServerMsg::RevertResult {
            request_id: string(f.req("request_id")?, "request_id")?,
            ok: flag(f.req("ok")?, "ok")?,
            seq: f.opt("seq")?.map(|p| uint(p, "seq")).transpose()?,
            ops: ops(f.req("ops")?, "ops", 0, usize::MAX)?,
            code: f.opt("code")?.map(|p| string(p, "code")).transpose()?,
        }),
        "lock.changed" => Ok(ServerMsg::LockChanged {
            locks: locks_from_wire(f.req("locks")?)?,
        }),
        "peer.joined" => Ok(ServerMsg::PeerJoined(peer_from_wire(f.req("peer")?)?)),
        "peer.left" => Ok(ServerMsg::PeerLeft {
            client_id: client_id("client_id")?,
        }),
        "presence" => Ok(ServerMsg::Presence {
            client_id: client_id("client_id")?,
            data: opaque_map(f.req("data")?, "data", None)?,
        }),
        "draft" => Ok(ServerMsg::Draft {
            client_id: client_id("client_id")?,
            data: f
                .opt("data")?
                .map(|p| opaque_map(p, "data", None))
                .transpose()?,
        }),
        "rebuild.status" => Ok(ServerMsg::RebuildStatus {
            broken: broken_map_from_wire(f.req("broken")?)?,
        }),
        "version.created" => Ok(ServerMsg::VersionCreated {
            id: string(f.req("id")?, "id")?,
            name: string(f.req("name")?, "name")?,
            seq: uint(f.req("seq")?, "seq")?,
        }),
        "reference.update" => Ok(ServerMsg::ReferenceUpdate {
            document: string(f.req("document")?, "document")?,
            components: strings(f.req("components")?, "components")?,
        }),
        "pong" => Ok(ServerMsg::Pong {
            seq: uint(f.req("seq")?, "seq")?,
        }),
        _ => invalid("unknown message type"),
    }
}
