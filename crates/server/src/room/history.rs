//! Taking changes back: a client's undo of its own change, and a revert of anyone's change from
//! history.
//!
//! # Undo
//!
//! A client undoes its own change by sending an ordinary `change` whose `undo_of` names the
//! change id being undone and whose operations are the inverse it kept. Before the change is
//! judged (after the resend and read-only checks), the room resolves `undo_of` to an accepted
//! change **of the same client id in this document**: a change still being written is found in
//! memory (its seq and the inverse the room decided for it); any other is read from the journal
//! ([`crate::store::Store::undo_target`], by document, client and change id), off the room's task,
//! the connection's later messages waiting. A change that does not exist, or that another client
//! made (whoever its user), is `invalid`; one already taken back (its `reverted_by` set, or an undo
//! or revert of it being written or made durable since the room loaded) is `invalid` too: a change
//! is undone once, and redo is an undo of the undo (a change of its own).
//!
//! The undo is then judged like any change, with two differences: `decide` gets the **stored**
//! inverse (never anything the client sent), so ids that inverse puts back at the paths the undone
//! change took them from are exempt from the id-prefix check (and only those: other operations
//! of the undo are judged on their own merits); and its base is the older of the client's base
//! and the undone change's seq, so that an undo of something someone else has written since is
//! `stale` (the client's own later writes never conflict). It is journaled with `undo_of` and
//! `reverts` set to the undone seq, and the undone row's `reverted_by` is set in the same
//! transaction; its change event carries `undo_of` (the change id) and `reverts` (the seq).
//!
//! # Revert from history
//!
//! `change.revert {request_id, seq}` (editors) reads the change's stored inverse from the journal
//! (off the task, the connection's later messages waiting) and submits it as a change the server
//! makes for the requester (`author` the requester, no client id in the journal): judged under the
//! server's client id with no id-prefix check (a revert of a removal puts the original ids back;
//! a revert of an insert removes them), `stale` if anything has written what it writes since the
//! reverted change (a client, or the server itself: a restore, another revert), and `locked` by
//! locks other clients hold on it (the requester's own do not count). Failures answer `change.revert.result {ok: false, code, message}` with `read_only`,
//! `not_found`, `already_reverted`, `stale`, `locked`, `server_error` (or another refusal's code);
//! success `{ok: true, seq, ops}`. The requester gets no change event for it; everyone else does.

use std::sync::Arc;

use oswx_core::{Change, Op, Seq};

use super::change::{Incoming, Item, Reverts, Seen};
use super::{ConnId, Internal, Room};
use crate::record::decode_ops;
use crate::store::{ChangeRow, UndoTarget};
use oswx_core::RejectCode;

/// What an undo of a change already taken back is told.
pub(super) const ALREADY_UNDONE: &str = "that change was already undone";
/// What an undo or revert of a change older than the history the room keeps is told.
pub(super) const TOO_OLD: &str = "that change is too old to take back";
/// What an undo naming no change of the sender's is told.
const NOT_YOURS: &str = "there is no change of yours with that id to undo";
/// What a client is told when the journal could not be read.
const NOT_READ: &str = "the server could not read the history; try again";

/// A journal lookup of the change an undo names, coming back.
pub(super) struct UndoLookup {
    conn: ConnId,
    change: Arc<Change>,
    size: usize,
    result: anyhow::Result<Option<UndoTarget>>,
}

/// A journal lookup of the change a revert names, coming back.
pub(super) struct RevertLookup {
    conn: ConnId,
    request_id: String,
    seq: Seq,
    result: anyhow::Result<Option<ChangeRow>>,
}

impl Room {
    /// Whether the change `seq` has been taken back by a change written since the room loaded, or
    /// being written now.
    pub(super) fn taken_back(&self, seq: Seq) -> bool {
        self.undone.contains(&seq) || self.pending_reverts(seq)
    }

    // ------------------------------------------------------------------ undo

    /// Resolves the `undo_of` of a client's change, then judges it (now, or when the journal
    /// answers).
    pub(super) fn resolve_undo(&mut self, conn: ConnId, change: Arc<Change>, size: usize) {
        let Some(c) = self.live_conn(conn) else {
            return;
        };
        let Some(of) = change.undo_of.clone() else {
            return self.judge_conn_change(conn, change, size, None);
        };
        let client = c.client.clone();
        match self.recent.get(&client, &of) {
            // Still being written: memory has everything.
            Some(Seen::Tentative(ticket)) => {
                if let Some((seq, inverse)) = self.pending_change(ticket) {
                    if self.taken_back(seq) {
                        return self.reject(
                            conn,
                            &change.change_id,
                            RejectCode::Invalid,
                            ALREADY_UNDONE,
                        );
                    }
                    let reverts = Reverts {
                        seq,
                        inverse: Some(inverse),
                        ticket: Some(ticket),
                        client_base: change.base,
                    };
                    return self.judge_conn_change(conn, change, size, Some(reverts));
                }
            }
            // The journal refused it as a duplicate and its seq is being looked up: wait for that
            // check, then resolve again (the connection's later messages wait too).
            Some(Seen::Checking(ticket)) => {
                if let Some(c) = self.conns.get_mut(&conn) {
                    c.busy = true;
                    self.undo_waiting
                        .entry(ticket)
                        .or_default()
                        .push((conn, change, size));
                }
                return;
            }
            // Memory holds every id this client journaled here, and not this one.
            None if self.recent.is_complete(&client) => {
                return self.reject(conn, &change.change_id, RejectCode::Invalid, NOT_YOURS);
            }
            _ => {}
        }
        // Durable (or unknown to memory): its inverse is in the journal.
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        c.busy = true;
        let (store, doc) = (self.state.store.clone(), self.doc.clone());
        let internal = self.internal.clone();
        tokio::spawn(async move {
            let result = super::change::caught(store.undo_target(&doc, &client, &of)).await;
            let lookup = UndoLookup {
                conn,
                change,
                size,
                result,
            };
            let _ = internal.send(Internal::Undo(lookup));
        });
    }

    pub(super) fn on_undo_lookup(&mut self, lookup: UndoLookup) {
        let UndoLookup {
            conn,
            change,
            size,
            result,
        } = lookup;
        self.unpark(conn);
        if self.closing.is_some() {
            return;
        }
        if self.live_conn(conn).is_none() {
            return self.void_change(conn, &change.change_id);
        }
        let id = change.change_id.clone();
        let row = match result {
            Ok(Some(row)) => row,
            Ok(None) => return self.reject(conn, &id, RejectCode::Invalid, NOT_YOURS),
            Err(e) => {
                tracing::error!(
                    "could not read an undone change of document {}: {e:#}",
                    self.doc
                );
                return self.reject(conn, &id, RejectCode::ServerError, NOT_READ);
            }
        };
        let Ok(seq) = Seq::try_from(row.seq) else {
            return self.reject(conn, &id, RejectCode::ServerError, NOT_READ);
        };
        // (An undo that became durable while the row was read is in `undone`.)
        if row.reverted_by.is_some() || self.taken_back(seq) {
            return self.reject(conn, &id, RejectCode::Invalid, ALREADY_UNDONE);
        }
        let inverse = match decode_ops(&row.inverse) {
            Ok(ops) => ops,
            Err(e) => {
                tracing::error!("change {seq} of document {} is damaged: {e}", self.doc);
                return self.reject(conn, &id, RejectCode::ServerError, NOT_READ);
            }
        };
        let reverts = Reverts {
            seq,
            inverse: Some(Arc::from(inverse)),
            ticket: None,
            client_base: change.base,
        };
        if self.paused > 0 {
            self.park_first(conn, Incoming::Judge(change, size, Some(reverts)));
        } else {
            self.judge_conn_change(conn, change, size, Some(reverts));
        }
    }

    /// The duplicate check of the change under `ticket` has answered: the undos waiting for it are
    /// resolved again, each before anything its connection sent after it.
    pub(super) fn checked(&mut self, ticket: super::change::Ticket) {
        for (conn, change, size) in self.undo_waiting.remove(&ticket).unwrap_or_default() {
            if self.live_conn(conn).is_none() {
                self.void_change(conn, &change.change_id);
                continue;
            }
            if let Some(c) = self.conns.get_mut(&conn) {
                c.busy = false;
                self.park_first(conn, Incoming::Judge(change, size, None));
            }
        }
    }

    // ------------------------------------------------------------------ revert

    /// `change.revert`: reads the change `seq` from the journal (unless it cannot be there), then
    /// submits its inverse.
    pub(super) fn on_revert(&mut self, conn: ConnId, request_id: String, seq: Seq) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        if let Some(reason) = c.read_only.clone() {
            let frame = self.revert_result(request_id, Err(("read_only", &reason)));
            return self.reply(conn, frame);
        }
        // Seq 0 is the document as it was created; nothing beyond the durable head is in the
        // journal (and a huge seq is no row).
        let row_seq = i64::try_from(seq)
            .ok()
            .filter(|_| (1..=self.durable.seq).contains(&seq));
        let Some(row_seq) = row_seq else {
            let message = format!("there is no change {seq}");
            let frame = self.revert_result(request_id, Err(("not_found", &message)));
            return self.reply(conn, frame);
        };
        c.busy = true;
        let (store, doc) = (self.state.store.clone(), self.doc.clone());
        let internal = self.internal.clone();
        tokio::spawn(async move {
            let result = super::change::caught(store.change_by_seq(&doc, row_seq)).await;
            let lookup = RevertLookup {
                conn,
                request_id,
                seq,
                result,
            };
            let _ = internal.send(Internal::Revert(lookup));
        });
    }

    pub(super) fn on_revert_lookup(&mut self, lookup: RevertLookup) {
        let RevertLookup {
            conn,
            request_id,
            seq,
            result,
        } = lookup;
        self.unpark(conn);
        if self.closing.is_some() {
            return;
        }
        let Some(c) = self.live_conn(conn) else {
            if let Some(to) = self.voided.get(&conn).copied() {
                self.unpark(to);
                let frame =
                    self.revert_result(request_id, Err(("server_error", super::change::VOIDED)));
                self.reply(to, frame);
            }
            return;
        };
        let (client, user, name) = (c.client.clone(), c.user.clone(), c.name.clone());
        let fail = |room: &mut Room, request_id: String, code: &str, message: &str| {
            let frame = room.revert_result(request_id, Err((code, message)));
            room.reply(conn, frame);
        };
        let row = match result {
            Ok(Some(row)) => row,
            Ok(None) => {
                return fail(
                    self,
                    request_id,
                    "not_found",
                    &format!("there is no change {seq}"),
                );
            }
            Err(e) => {
                tracing::error!(
                    "could not read change {seq} of document {}: {e:#}",
                    self.doc
                );
                return fail(self, request_id, "server_error", NOT_READ);
            }
        };
        if row.reverted_by.is_some() || self.taken_back(seq) {
            return fail(self, request_id, "already_reverted", ALREADY_UNDONE);
        }
        let ops: Vec<Op> = match decode_ops(&row.inverse) {
            Ok(ops) if !ops.is_empty() => ops,
            Ok(_) => return fail(self, request_id, "invalid", "that change took nothing out"),
            Err(e) => {
                tracing::error!("change {seq} of document {} is damaged: {e}", self.doc);
                return fail(self, request_id, "server_error", NOT_READ);
            }
        };
        let command = format!("revert {}", row.command);
        let change = Change {
            // `decide` needs an id; it is neither journaled nor broadcast.
            change_id: Arc::from(crate::snapshot::SERVER_CLIENT),
            base: seq,
            ops,
            reads: Default::default(),
            depends: Vec::new(),
            undo_of: None,
            command: Arc::from(command.trim()),
        };
        let item = Item::revert(conn, request_id, client, user, name, change, seq);
        // Judged before anything the requester sent after it (the server's queue goes first).
        self.server_queue.push_front(item);
    }
}
