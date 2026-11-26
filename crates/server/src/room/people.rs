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
        if req.outbox.is_closed() {
            // Closed on its way here (its access ended): it never joins.
            return;
        }
        // Dead locks go before anyone's are taken up again (or handed to a replacement).
        self.expire_locks();
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
        // The room's name for the connection: its own id, or, when it takes over a live
        // connection of the same client, that connection's (see `Room::take_over`). It inherits
        // the old connection's work only with the very access that work was sent with: the same
        // role, and not read-only. Otherwise everything of the old connection not yet handed to
        // the journal is discarded, as for a revocation (what was handed over completes), and it
        // starts clean: a client that resends is answered under its access now.
        let mut key = req.conn;
        let mut inherited = None;
        if let Some(old) = self.by_client.get(&req.client).copied() {
            let same_access =
                read_only.is_none() && self.live_conn(old).is_some_and(|c| c.role == req.role);
            if same_access {
                inherited = self.take_over(old, req.conn);
                key = old;
            } else {
                // Closed already (its access ended, or it broke a limit), or its access is not
                // the newcomer's: its work is not inherited. Popped first: its leaving is not
                // announced.
                self.remove_conn(
                    old,
                    Some((close::REPLACED, "connected again from the same client")),
                    false,
                );
            }
        }
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
        let (replies, parked, parked_bytes, busy) = inherited.unwrap_or_default();
        let inherited = if key == req.conn {
            std::collections::HashSet::new()
        } else {
            self.recent.unsettled_of(&req.client)
        };
        let resume = !busy && !parked.is_empty();
        let conn = Conn {
            client: req.client.clone(),
            user: req.user,
            name: req.name,
            colour,
            read_only,
            outbox: req.outbox,
            replies,
            parked,
            parked_bytes,
            busy,
            heard: req.heard,
            presence: None,
            draft: None,
            live_rate: Bucket::default(),
            lock_rate: Bucket::default(),
            joined: self.next_join,
            expires: req.session_expires,
            inherited,
            role: req.role,
        };
        self.next_expiry = self.next_expiry.min(req.session_expires);
        self.idle_since = None;
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
            page_bytes: self.state.room_hooks.page_bytes,
            project: self.project.clone(),
            role: req.role,
            read_only: conn.read_only.clone(),
            you,
            peers,
            locks: self.all_locks(),
            broken: self.rebuilds.broken.clone(),
            encodes: self.encodes.clone(),
        };
        // What this client already sent here is answered from memory if it comes again.
        for (change_id, seq) in req.recent_ids {
            self.recent.remember(&req.client, &change_id, seq);
        }
        self.recent.set_complete(&req.client, req.ids_complete);
        self.recent.trim();
        self.by_client.insert(req.client, key);
        self.conns.insert(key, conn);
        if resume {
            self.ready.push_back(key);
        }
        self.publish_online();
        let internal = self.internal.clone();
        let conn_id = key;
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

    /// A new connection (`socket`) of the same client takes over the live connection the room
    /// knows as `old`: `old` is closed (4409) and its socket's messages are ignored from now on,
    /// and the newcomer goes on under the room's name `old`. So everything of the client still in
    /// the works under that name (answers awaited, parked messages, changes being judged, looked
    /// up, checked or written) is the successor's: it gets their answers, in order, ahead of its
    /// own later ones. Answers what is carried over: the answers awaited, the parked messages and
    /// their bytes, and whether a message is still in the works.
    fn take_over(
        &mut self,
        old: ConnId,
        socket: ConnId,
    ) -> Option<(
        VecDeque<Reply>,
        VecDeque<super::change::Incoming>,
        usize,
        bool,
    )> {
        let c = self.conns.remove(&old)?;
        // Releases it made that still wait to be announced go now.
        self.flush_lock_news(old);
        c.outbox
            .close(close::REPLACED, "connected again from the same client");
        // The socket that served the name `old` until now is retired.
        let previous = self
            .aliases
            .iter()
            .find(|(_, k)| **k == old)
            .map_or(old, |(s, _)| *s);
        self.aliases.remove(&previous);
        self.retired.insert(previous);
        self.served.insert(previous, old);
        if socket != old {
            self.aliases.insert(socket, old);
        }
        // (Its name now belongs to a connection that keeps up.)
        self.slow.retain(|s| *s != old);
        Some((c.replies, c.parked, c.parked_bytes, c.busy))
    }

    /// Takes a connection out of the room, closing it (with `close`, or a normal close), and tells
    /// the others it left when `announce`; its locks then wait for it (detached). Only a connection
    /// replaced by a new one of the same client is removed without `announce`: its locks pass to the
    /// new one. A connection the hub closed because its access ended (4401, 4403, 4404) has its
    /// locks released at once instead, whatever removes it (its `Leave` or the hub's `Revoked`,
    /// in either order): a user whose access ended must not go on blocking others.
    pub(super) fn remove_conn(&mut self, conn: ConnId, close: Option<(u16, &str)>, announce: bool) {
        let Some(c) = self.conns.remove(&conn) else {
            return;
        };
        let revoked = c.outbox.close_code().is_some_and(close::ends_access);
        // What pointed at this name goes with it.
        self.served.retain(|_, name| *name != conn);
        self.voided.retain(|_, to| *to != conn);
        if self.conns.is_empty() {
            self.idle_since = Some(self.state.clock.now());
        }
        let registered = self.by_client.get(&c.client) == Some(&conn);
        if registered {
            self.by_client.remove(&c.client);
            // Worked out again at its next join (client ids can be made up at will).
            self.recent.set_complete(&c.client, false);
        }
        if announce {
            // (A replaced connection's successor joins at once: no flicker in the online list.)
            self.publish_online();
        }
        // Releases it made that still wait to be announced go now.
        self.flush_lock_news(conn);
        // The refused ids of its client are forgotten when the connection goes (a client that
        // reconnects resends only what it has no answer for).
        self.refused.remove(&c.client);
        let (code, reason) = close.unwrap_or((1000, ""));
        c.outbox.close(code, reason);
        if announce {
            let left = self.encode(&ServerMsg::PeerLeft {
                client_id: c.client.clone(),
            });
            self.broadcast(&left, &[]);
        }
        if registered && revoked {
            self.locks_at_revoke(&c.client);
        } else if registered && announce {
            self.locks_at_leave(&c.client);
        }
    }
}
