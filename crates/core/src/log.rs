//! The change log's logic: judging one client's change against the head of a document.
//!
//! No I/O happens here. The caller persists what `decide` accepts and only then adopts it as the
//! new head, so a failure to persist leaves the room as it was.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::apply::{Rules, apply};
use crate::index::{ClientId, Conflict, Index, Seq};
use crate::op::{Op, OpError, removed, writes};
use crate::path::Path;
use crate::value::{Id, Value};

/// Longest change id, in characters.
const MAX_CHANGE_ID: usize = 64;

/// One change as a client sent it.
#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    /// 1..=64 characters, chosen by the client.
    pub change_id: Arc<str>,
    /// The seq of the document the client made the change on.
    pub base: Seq,
    pub ops: Vec<Op>,
    /// Paths the change relies on without writing them.
    pub reads: BTreeSet<Path>,
    /// The sender's own earlier change ids this one builds on.
    pub depends: Vec<Arc<str>>,
    /// The change this one undoes, if any. Carried, not judged.
    pub undo_of: Option<Arc<str>>,
    /// What the user did, for history.
    pub command: Arc<str>,
}

/// An accepted change: the head it makes, once the caller adopts it.
#[derive(Clone, Debug)]
pub struct Accepted {
    pub seq: Seq,
    pub doc: Value,
    pub index: Index,
    /// The operations that take the change back out of `doc`.
    pub inverse: Vec<Op>,
}

/// Why a change was refused.
#[derive(Clone, Debug, PartialEq)]
pub enum Refusal {
    /// Someone else changed what this change writes, removes or reads since its base.
    Stale(Vec<Conflict>),
    /// It builds on this earlier change of the sender's, which was refused.
    Dependency(Arc<str>),
    /// It would give a feature a name another one has.
    NameTaken(Arc<str>),
    /// It inserts an item under an id the list already has.
    IdTaken(Id),
    /// It is malformed or does not apply.
    Invalid(String),
    /// Its base is older than the history kept to judge it.
    TooOld,
}

/// The current state of a document.
#[derive(Clone, Debug)]
pub struct Head {
    pub seq: Seq,
    pub doc: Value,
    pub index: Index,
}

impl From<OpError> for Refusal {
    fn from(e: OpError) -> Refusal {
        match e {
            OpError::NameTaken(name) => Refusal::NameTaken(name),
            OpError::IdTaken(id) => Refusal::IdTaken(id),
            other => Refusal::Invalid(other.to_string()),
        }
    }
}

/// Decide one change against the head. Pure; the caller persists and then adopts `Accepted`.
///
/// `refused(change_id)` says whether that earlier change of the same client was refused. After
/// checking that the change is well formed, the checks run in this order: a base below the index's
/// floor is `TooOld`; a refused change in `depends` is `Dependency`; conflicts in the index are
/// `Stale`; then the operations are applied under `rules`, new ids having to start with
/// `"<client>."`, and an error there is `NameTaken`, `IdTaken` or `Invalid`.
pub fn decide(
    head: &Head,
    client: &ClientId,
    change: &Change,
    refused: &dyn Fn(&str) -> bool,
    rules: &Rules,
) -> Result<Accepted, Refusal> {
    let id_len = change.change_id.chars().count();
    if !(1..=MAX_CHANGE_ID).contains(&id_len) {
        return Err(Refusal::Invalid(format!(
            "change_id must be 1 to {MAX_CHANGE_ID} characters"
        )));
    }
    if change.ops.is_empty() {
        return Err(Refusal::Invalid(
            "a change needs at least one operation".into(),
        ));
    }
    // A base the server has not reached would make every later write look unseen.
    if change.base > head.seq {
        return Err(Refusal::Invalid("base is ahead of the document".into()));
    }
    if change.base < head.index.floor() {
        return Err(Refusal::TooOld);
    }
    if let Some(dep) = change.depends.iter().find(|d| refused(d)) {
        return Err(Refusal::Dependency(dep.clone()));
    }
    let writes = writes(&change.ops);
    let removed = removed(&change.ops);
    let conflicts = head
        .index
        .check(change.base, client, &writes, &removed, &change.reads);
    if !conflicts.is_empty() {
        return Err(Refusal::Stale(conflicts));
    }
    let prefix = format!("{client}.");
    let applied = apply(&head.doc, &change.ops, rules, Some(&prefix))?;
    let seq = head.seq + 1;
    Ok(Accepted {
        seq,
        doc: applied.doc,
        index: head
            .index
            .record(seq, client, &writes, &removed, &change.reads),
        inverse: applied.inverse,
    })
}
