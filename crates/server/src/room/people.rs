//! A room's connections: joining, leaving, and the frames the room queues for them (answers in the
//! order of the messages they answer, broadcasts, removal of connections that do not keep up).

use std::collections::VecDeque;

use bytes::Bytes;
use futures_util::FutureExt;
use oswx_core::Seq;
use oswx_core::wire::{Peer, ServerMsg};
use std::panic::AssertUnwindSafe;

use super::change::Ticket;
use super::presence::Bucket;
use super::welcome::Welcome;
use super::{Conn, ConnId, Internal, JoinRequest, Reply, Room, close, conn};
use crate::security::{ProjectRole, colour_for};

impl Room {
    // ------------------------------------------------------------------ sending

    /// Queues a frame for one connection; one that breaks its limits is dropped afterwards.
    pub(super) fn send_to(&mut self, conn: ConnId, frame: Bytes) {
        if let Some(c) = self.conns.get(&conn)
            && c.outbox.push(frame) == conn::Pushed::TooSlow
        {
            self.slow.push(conn);
        }
    }

    /// One frame to every connection but `except`.
    pub(super) fn broadcast(&mut self, frame: &Bytes, except: &[ConnId]) {
        let mut recipients = 0;
        for (id, c) in &self.conns {
            if except.contains(id) {
                continue;
            }
            recipients += 1;
            if c.outbox.push(frame.clone()) == conn::Pushed::TooSlow {
                self.slow.push(*id);
            }
        }
        crate::metrics::broadcast(recipients);
    }

    /// An answer to one of the connection's messages: sent at once unless an earlier answer is
    /// still waiting.
    pub(super) fn reply(&mut self, conn: ConnId, frame: Bytes) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        if c.replies.is_empty() {
            self.send_to(conn, frame);
        } else if !self.replies_full(conn)
            && let Some(c) = self.conns.get_mut(&conn)
        {
            c.replies.push_back(Reply::Ready(frame));
        }
    }

    /// Whether the connection's waiting answers are at [`super::MAX_REPLIES`]; if so it is closed as
    /// too slow (a client resending one change over and over while it is being written, say).
    pub(super) fn replies_full(&mut self, conn: ConnId) -> bool {
        let Some(c) = self.conns.get(&conn) else {
            return true;
        };
        if c.replies.len() < super::MAX_REPLIES {
            return false;
        }
        c.outbox.close_now(close::TOO_SLOW, "too slow");
        self.slow.push(conn);
        true
    }

    /// Settles the connection's answer that waits for `ticket`, and sends what is now first in line.
    pub(super) fn resolve(&mut self, conn: ConnId, ticket: Ticket, frame: Bytes) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        if let Some(slot) = c
            .replies
            .iter_mut()
            .find(|r| matches!(r, Reply::Waiting(t) if *t == ticket))
        {
            *slot = Reply::Ready(frame);
        }
        let mut out = Vec::new();
        while let Some(Reply::Ready(_)) = c.replies.front() {
            if let Some(Reply::Ready(f)) = c.replies.pop_front() {
                out.push(f);
            }
        }
        for f in out {
            self.send_to(conn, f);
        }
    }

    /// Removes the connections that broke their limits (each removal is announced, which may break
    /// another's).
    pub(super) fn reap(&mut self) {
        while let Some(conn) = self.slow.pop() {
            if self.conns.contains_key(&conn) {
                crate::metrics::slow_consumer();
                self.remove_conn(conn, None, true);
            }
        }
    }

    // ------------------------------------------------------------------ people

    pub(super) fn join(&mut self, req: JoinRequest) {
        if let Some(c) = &self.closing {
            req.outbox.close(c.code, &c.reason);
            return;
        }
        if req.room != self.id {
            // Its preload was read for another room: it reconnects and reads again.
            req.outbox.close(close::MOVED, "reconnect");
            return;
        }
        if let Some(old) = self.by_client.get(&req.client).copied() {
            // Popped first: its leaving is not announced.
            self.remove_conn(
                old,
                Some((close::REPLACED, "connected again from the same client")),
                false,
            );
        }
        let read_only = if req.role == ProjectRole::Viewer {
            Some("you have view access to this project".to_owned())
        } else if let Some(theirs) = self
            .format_version
            .filter(|fv| req.format_version != Some(*fv))
        {
            let mine = req
                .format_version
                .map_or_else(|| "none".to_owned(), |v| v.to_string());
            Some(format!(
                "this file uses openswx file format {theirs} and this app uses {mine}; update \
                 openswx to edit it"
            ))
        } else {
            None
        };
        let same_user = self.conns.values().find(|c| c.user == req.user);
        let colour = match same_user {
            Some(c) => c.colour.clone(),
            None => {
                let taken: Vec<String> = self.conns.values().map(|c| c.colour.clone()).collect();
                colour_for(&req.user, &taken)
            }
        };
        // Locks this client kept through a disconnect are its again (or released, if it may
        // now only look); a replaced connection's locks simply stay.
        let (back, released) = self.locks_at_join(&req.client, read_only.is_some());
        let conn = Conn {
            client: req.client.clone(),
            user: req.user,
            name: req.name,
            colour,
            read_only,
            outbox: req.outbox,
            replies: VecDeque::new(),
            parked: VecDeque::new(),
            parked_bytes: 0,
            busy: false,
            heard: req.heard,
            presence: None,
            draft: None,
            live_rate: Bucket::default(),
            lock_rate: Bucket::default(),
            joined: self.next_join,
        };
        self.next_join += 1;
        let you = conn.peer();
        let mut others: Vec<&Conn> = self.conns.values().collect();
        others.sort_by_key(|c| c.joined);
        let peers: Vec<Peer> = others.into_iter().map(Conn::peer).collect();
        let joined = self.encode(&ServerMsg::PeerJoined(you.clone()));
        self.broadcast(&joined, &[]);
        self.announce_join_locks(back, released);
        let welcome = Welcome {
            store: self.state.store.clone(),
            doc: self.doc.clone(),
            head: self.durable.clone(),
            last_seq: req.last_seq,
            catchup_limit: self.state.settings.catchup_limit as Seq,
            project: self.project.clone(),
            role: req.role,
            read_only: conn.read_only.clone(),
            you,
            peers,
            locks: self.all_locks(),
            encodes: self.encodes.clone(),
        };
        // What this client already sent here is answered from memory if it comes again.
        for (change_id, seq) in req.recent_ids {
            self.recent.remember(&req.client, &change_id, seq);
        }
        self.recent.set_complete(&req.client, req.ids_complete);
        self.recent.trim();
        self.by_client.insert(req.client, req.conn);
        self.conns.insert(req.conn, conn);
        self.publish_online();
        let internal = self.internal.clone();
        let conn_id = req.conn;
        tokio::spawn(async move {
            // A panic while building it is reported like any failure: the connection is closed.
            let result = match AssertUnwindSafe(welcome.build()).catch_unwind().await {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!("building the welcome panicked")),
            };
            let _ = internal.send(Internal::Welcome {
                conn: conn_id,
                result,
            });
        });
    }

    pub(super) fn on_welcome(&mut self, conn: ConnId, result: anyhow::Result<Bytes>) {
        let Some(c) = self.conns.get(&conn) else {
            return;
        };
        match result {
            Ok(frame) => c.outbox.release(frame),
            Err(e) => {
                tracing::error!("could not make the welcome of document {}: {e:#}", self.doc);
                self.remove_conn(conn, Some((close::SERVER_ERROR, "try again")), true);
            }
        }
    }

    /// Takes a connection out of the room, closing it (with `close`, or a normal close), and tells
    /// the others it left when `announce`; its locks then wait for it (detached). Only a connection
    /// replaced by a new one of the same client is removed without `announce`: its locks pass to the
    /// new one.
    pub(super) fn remove_conn(&mut self, conn: ConnId, close: Option<(u16, &str)>, announce: bool) {
        let Some(c) = self.conns.remove(&conn) else {
            return;
        };
        let registered = self.by_client.get(&c.client) == Some(&conn);
        if registered {
            self.by_client.remove(&c.client);
            // Worked out again at its next join (client ids can be made up at will).
            self.recent.set_complete(&c.client, false);
        }
        self.publish_online();
        // For this task the refused ids are forgotten when the connection goes (TASK 12: after a
        // grace period instead).
        self.refused.remove(&c.client);
        let (code, reason) = close.unwrap_or((1000, ""));
        c.outbox.close(code, reason);
        if announce {
            let left = self.encode(&ServerMsg::PeerLeft {
                client_id: c.client.clone(),
            });
            self.broadcast(&left, &[]);
            if registered {
                self.locks_at_leave(&c.client);
            }
        }
    }
}
