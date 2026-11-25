//! A simulated client: the reference for the real client engine.
//!
//! The client keeps
//!
//! - `confirmed`: the document as of the last server message it processed, and that message's seq;
//! - `pending`: its own changes the server has not answered yet, in the order it sent them;
//! - `live`: what the user sees, `confirmed` with the pending changes applied in order.
//!
//! Every pending change keeps the inverse it got when it was last applied to `live`, so that a
//! refused change can be taken out of `live` without starting again from `confirmed`. A pending
//! change that no longer applies (something it built on was changed by someone else, or by one of
//! the client's own changes that was refused) stays queued without effect: the server judges it
//! against the same history and will refuse it.

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::sync::Arc;

use oswx_core::{
    Change, ClientId, Op, Path, Refusal, Rules, Seq, Value, Write, apply, removed, writes,
};

/// What the server sends one client, in the order it decided things.
#[derive(Clone, Debug)]
pub enum ToClient {
    /// Someone else's accepted change.
    Change { seq: Seq, ops: Vec<Op> },
    /// This client's change was accepted as `seq`.
    Accepted { change_id: Arc<str>, seq: Seq },
    /// This client's change was refused; it never becomes part of the document.
    Refused {
        change_id: Arc<str>,
        refusal: Refusal,
    },
}

/// One of the client's own changes that the server has not answered yet.
#[derive(Clone, Debug)]
struct Pending {
    change: Change,
    /// What the change wrote or removed, for working out which later changes build on it.
    wrote: BTreeSet<Path>,
    /// The operations that take it back out of `live`; `None` if it did not apply last time.
    inverse: Option<Vec<Op>>,
}

#[derive(Debug)]
pub struct Client {
    pub id: ClientId,
    pub confirmed: Value,
    pub seq: Seq,
    pub live: Value,
    pending: VecDeque<Pending>,
    /// Changes dropped because one they depend on was refused; the server refuses them too.
    dropped: HashSet<Arc<str>>,
    next_change: u64,
    next_item: u64,
    /// How the client's own changes ended, for the test's statistics.
    pub accepted: usize,
    pub refusals: Vec<Refusal>,
}

impl Client {
    pub fn new(id: &str, doc: Value, seq: Seq) -> Client {
        Client {
            id: Arc::from(id),
            confirmed: doc.clone(),
            seq,
            live: doc,
            pending: VecDeque::new(),
            dropped: HashSet::new(),
            next_change: 0,
            next_item: 0,
            accepted: 0,
            refusals: Vec::new(),
        }
    }

    /// A fresh item id, `<user>.<client>.<counter>`.
    pub fn new_item_id(&mut self) -> Arc<str> {
        self.next_item += 1;
        Arc::from(format!("{}.{}", self.id, self.next_item))
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Makes an edit the user did on `live`: applies it locally at once and returns the change to
    /// send. `reads` are the paths the edit relies on without writing them.
    pub fn edit(&mut self, ops: Vec<Op>, reads: BTreeSet<Path>) -> Change {
        let applied = apply(&self.live, &ops, &Rules::default(), None)
            .expect("an edit is made against the live document");
        self.live = applied.doc;
        self.next_change += 1;
        let wrote = written_paths(&ops);
        let touched: BTreeSet<Path> = wrote.iter().chain(&reads).cloned().collect();
        let depends = self
            .pending
            .iter()
            .filter(|p| overlaps(&touched, &p.wrote))
            .map(|p| p.change.change_id.clone())
            .collect();
        let change = Change {
            change_id: Arc::from(format!("{}#{}", self.id, self.next_change)),
            base: self.seq,
            ops,
            reads,
            depends,
            undo_of: None,
            command: Arc::from("edit"),
        };
        self.pending.push_back(Pending {
            change: change.clone(),
            wrote,
            inverse: Some(applied.inverse),
        });
        change
    }

    pub fn receive(&mut self, msg: ToClient) {
        match msg {
            ToClient::Change { seq, ops } => {
                assert_eq!(seq, self.seq + 1, "changes arrive in order, none missing");
                self.confirmed = apply(&self.confirmed, &ops, &Rules::default(), None)
                    .expect("an accepted change applies to the document it was accepted on")
                    .doc;
                self.seq = seq;
                self.rebuild();
            }
            ToClient::Accepted { change_id, seq } => {
                assert_eq!(seq, self.seq + 1, "changes arrive in order, none missing");
                assert!(
                    !self.dropped.contains(&change_id),
                    "a dropped change is never accepted"
                );
                let first = self
                    .pending
                    .pop_front()
                    .expect("an answer to a pending change");
                assert_eq!(
                    first.change.change_id, change_id,
                    "answers come in the order of sending"
                );
                // `confirmed` was the server's document just before this change, and `live` was
                // last rebuilt on it, so the change applied there too: `live` stays as it is.
                assert!(
                    first.inverse.is_some(),
                    "an accepted change applied locally"
                );
                self.confirmed = apply(&self.confirmed, &first.change.ops, &Rules::default(), None)
                    .expect("an accepted change applies to the document it was accepted on")
                    .doc;
                self.seq = seq;
                self.accepted += 1;
            }
            ToClient::Refused { change_id, refusal } => {
                self.refusals.push(refusal);
                if self.dropped.remove(&change_id) {
                    return; // Already taken out with the change it depended on.
                }
                assert_eq!(
                    self.pending.front().map(|p| &p.change.change_id),
                    Some(&change_id),
                    "answers come in the order of sending"
                );
                self.roll_back(&change_id);
            }
        }
    }

    /// `live` again from `confirmed` and the pending changes, refreshing their inverses.
    fn rebuild(&mut self) {
        self.live = self.confirmed.clone();
        self.reapply(0);
    }

    /// Applies `pending[from..]` to `live` in order, keeping each change's new inverse.
    fn reapply(&mut self, from: usize) {
        for p in self.pending.iter_mut().skip(from) {
            p.inverse = match apply(&self.live, &p.change.ops, &Rules::default(), None) {
                Ok(applied) => {
                    self.live = applied.doc;
                    Some(applied.inverse)
                }
                Err(_) => None,
            };
        }
    }

    /// Takes a refused change, and every pending change that depends on it directly or through
    /// others, out of `live`: undoes the pending changes from the newest back to the refused one
    /// with their inverses, drops the doomed ones, and applies the survivors again.
    fn roll_back(&mut self, refused: &Arc<str>) {
        let at = self
            .pending
            .iter()
            .position(|p| &p.change.change_id == refused)
            .expect("the refused change is pending");
        for p in self.pending.iter().skip(at).rev() {
            if let Some(inverse) = &p.inverse {
                self.live = apply(&self.live, inverse, &Rules::default(), None)
                    .expect("an inverse restores the document it was made on")
                    .doc;
            }
        }
        let mut doomed: HashSet<Arc<str>> = HashSet::from([refused.clone()]);
        for p in self.pending.iter().skip(at + 1) {
            if p.change.depends.iter().any(|d| doomed.contains(d)) {
                doomed.insert(p.change.change_id.clone());
            }
        }
        self.pending
            .retain(|p| !doomed.contains(&p.change.change_id));
        doomed.remove(refused);
        self.dropped.extend(doomed);
        self.reapply(at);
    }

    /// `confirmed` with the pending changes applied: what `live` must always equal.
    pub fn expected_live(&self) -> Value {
        let mut doc = self.confirmed.clone();
        for p in &self.pending {
            if let Ok(applied) = apply(&doc, &p.change.ops, &Rules::default(), None) {
                doc = applied.doc;
            }
        }
        doc
    }
}

/// The paths a change writes or removes; a move writes the moved item's path.
fn written_paths(ops: &[Op]) -> BTreeSet<Path> {
    let mut out: BTreeSet<Path> = writes(ops)
        .into_iter()
        .map(|w| match w {
            Write::At(p) => p,
            Write::Position(mut list, id) => {
                list.push(id);
                list
            }
        })
        .collect();
    out.extend(removed(ops));
    out
}

/// Whether any path of `a` equals, contains or lies inside a path of `b`.
fn overlaps(a: &BTreeSet<Path>, b: &BTreeSet<Path>) -> bool {
    a.iter()
        .any(|x| b.iter().any(|y| x.starts_with(y) || y.starts_with(x)))
}
