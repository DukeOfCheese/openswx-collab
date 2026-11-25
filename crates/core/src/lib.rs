//! The pure document model shared by the collaboration server and the Python wheel.

pub mod apply;
pub mod diff;
pub mod index;
pub mod limits;
pub mod log;
pub mod op;
pub mod path;
pub mod pos;
pub mod value;
pub mod wire;

pub use apply::{Applied, Rules, apply, apply_with};
pub use diff::diff;
pub use index::{ClientId, Conflict, ConflictKind, Index, Seq};
pub use log::{Accepted, Change, Head, Known, Refusal, decide};
pub use op::{MAX_DEPTH, MAX_NAME, Op, OpError, PROTECTED, Write, removed, writes};
pub use path::Path;
pub use pos::Pos;
pub use value::{Id, KeyedList, Map, Plain, Value, ValueError};
pub use wire::{
    Broken, ChangeEvent, ClientMsg, Lock, PROTOCOL_VERSION, Peer, RejectCode, ServerMsg, WireError,
    decode_client, decode_server, encode_client, encode_server,
};
