//! The change log's logic: judging one client's change against the head of a document.
//!
//! No I/O happens here. The caller persists what `decide` accepts and only then adopts it as the
//! new head, so a failure to persist leaves the room as it was.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::apply::{Rules, apply_judged, carried_items};
use crate::index::{ClientId, Conflict, Index, Seq};
use crate::limits::MAX_CHANGE_ID;
use crate::op::{Op, OpError, Write, removed, writes};
use crate::path::Path;
use crate::value::{Id, Value};
use crate::wire::RejectCode;

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
    /// What the change writes (`writes(&change.ops)`), as recorded in `index`.
    pub writes: BTreeSet<Write>,
    /// The items it removes (`removed(&change.ops)`), as recorded in `index`.
    pub removed: BTreeSet<Path>,
    /// What it reads (the change's `reads`), as recorded in `index`.
    pub reads: BTreeSet<Path>,
}

/// What the caller knows of an earlier change of the same client, named in `depends`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Known {
    /// It was accepted.
    Accepted,
    /// It was refused.
    Refused,
    /// The caller has never seen it (or no longer knows it).
    Unknown,
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

impl Refusal {
    /// The code the server sends for this refusal.
    pub fn code(&self) -> RejectCode {
        match self {
            Refusal::Stale(_) => RejectCode::Stale,
            Refusal::Dependency(_) => RejectCode::Dependency,
            Refusal::NameTaken(_) => RejectCode::NameTaken,
            Refusal::IdTaken(_) => RejectCode::IdTaken,
            Refusal::Invalid(_) => RejectCode::Invalid,
            Refusal::TooOld => RejectCode::TooOld,
        }
    }
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
/// `known(change_id)` says what the caller knows of that earlier change of the same client.
/// `undo` is the kept inverse of the change `change.undo_of` names, when the caller has it; `None`
/// otherwise. It is ignored unless `change.undo_of` is set. The caller must take it only from the
/// stored inverse of an accepted change by the same client (the journal's `inverse` of the row
/// `undo_of` names), never from anything the client sent: it decides which foreign ids may come
/// back.
///
/// After checking that the change is well formed, the checks run in this order: a base below the
/// index's floor is `TooOld`; a change in `depends` that is not known to be accepted (refused, or
/// unknown) is `Dependency`; conflicts in the index are `Stale`; then the operations are applied
/// under `rules`, new ids having to start with `"<client>."` unless they put back an item whose
/// removal the index still remembers (see `apply_with`) or `undo` carries an item with that id at
/// the same path (the undone change took it away from there), and an error there is `NameTaken`,
/// `IdTaken` or `Invalid`.
pub fn decide(
    head: &Head,
    client: &ClientId,
    change: &Change,
    known: &dyn Fn(&str) -> Known,
    undo: Option<&[Op]>,
    rules: &Rules,
) -> Result<Accepted, Refusal> {
    judge(head, client, change, known, undo, rules, true, true)
}

/// The client id under which the index records a change the server made (a restore, a revert from
/// history). A real client is `<user id>.<client id>`, which never starts with `~`.
pub const SERVER_CLIENT: &str = "~server";

/// Decide a change the server makes itself (a restore, a revert from history), recorded in the
/// index under [`SERVER_CLIENT`].
///
/// Unlike [`decide`], ids are not checked against a client prefix: a restore puts items back under
/// their original ids, whoever made them and however long ago they were removed. The document's
/// `rules` (unique names) still apply, and so do the shape checks. `depends` and `undo_of` are not
/// consulted. Staleness (conflicts in the index after `change.base`, and a base below the index's
/// floor) is checked only when `check_stale`.
pub fn decide_server(
    head: &Head,
    change: &Change,
    rules: &Rules,
    check_stale: bool,
) -> Result<Accepted, Refusal> {
    let client: ClientId = Arc::from(SERVER_CLIENT);
    judge(
        head,
        &client,
        change,
        &|_| Known::Accepted,
        None,
        rules,
        check_stale,
        false,
    )
}

/// [`decide`] and [`decide_server`]: `check_stale` judges the index; `client_ids` holds new ids to
/// the client's prefix (and consults `depends`).
#[allow(clippy::too_many_arguments)]
fn judge(
    head: &Head,
    client: &ClientId,
    change: &Change,
    known: &dyn Fn(&str) -> Known,
    undo: Option<&[Op]>,
    rules: &Rules,
    check_stale: bool,
    client_ids: bool,
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
    if check_stale && change.base < head.index.floor() {
        return Err(Refusal::TooOld);
    }
    if client_ids && let Some(dep) = change.depends.iter().find(|d| known(d) != Known::Accepted) {
        return Err(Refusal::Dependency(dep.clone()));
    }
    let writes = writes(&change.ops);
    let removed = removed(&change.ops);
    if check_stale {
        let conflicts = head
            .index
            .check(change.base, client, &writes, &removed, &change.reads);
        if !conflicts.is_empty() {
            return Err(Refusal::Stale(conflicts));
        }
    }
    let prefix = format!("{client}.");
    let id_prefix = client_ids.then_some(prefix.as_str());
    // An item whose removal the index still remembers may be put back by anyone (an undo, or a
    // restore of someone else's deleted item); once that removal is pruned, it may not.
    let existed = |p: &Path| head.index.removed_at(p);
    // Undoing a change that dropped a subtree may bring back the ids it held, at the same paths.
    let carried = undo
        .filter(|_| change.undo_of.is_some())
        .map(carried_items)
        .unwrap_or_default();
    let kept = |p: &Path| carried.contains(p);
    let applied = apply_judged(&head.doc, &change.ops, rules, id_prefix, &existed, &kept)?;
    let seq = head.seq + 1;
    Ok(Accepted {
        seq,
        doc: applied.doc,
        index: head
            .index
            .record(seq, client, &writes, &removed, &change.reads),
        inverse: applied.inverse,
        writes,
        removed,
        reads: change.reads.clone(),
    })
}
