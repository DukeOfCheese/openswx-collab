//! Small helpers of the room.

use std::collections::{HashSet, VecDeque};

/// A bounded set of hashes, for [`change::KnownBlobs`] and [`change::RefusedIds`].
#[derive(Debug, Default)]
pub(super) struct Bounded<T: std::hash::Hash + Eq + Clone> {
    set: HashSet<T>,
    order: VecDeque<T>,
}

impl<T: std::hash::Hash + Eq + Clone> Bounded<T> {
    pub(super) fn insert(&mut self, item: T, cap: usize) {
        if self.set.insert(item.clone()) {
            self.order.push_back(item);
            while self.order.len() > cap {
                if let Some(old) = self.order.pop_front() {
                    self.set.remove(&old);
                }
            }
        }
    }

    pub(super) fn contains(&self, item: &T) -> bool {
        self.set.contains(item)
    }
}
