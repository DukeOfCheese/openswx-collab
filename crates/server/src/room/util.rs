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

    pub(super) fn len(&self) -> usize {
        self.set.len()
    }
}

/// Counts per key, remembering at most `cap` keys (the least recently added forgotten first).
#[derive(Debug)]
pub(super) struct Tally {
    counts: std::collections::HashMap<String, u32>,
    order: VecDeque<String>,
    cap: usize,
}

impl Tally {
    pub(super) fn new(cap: usize) -> Self {
        Self {
            counts: std::collections::HashMap::new(),
            order: VecDeque::new(),
            cap: cap.max(1),
        }
    }

    /// Adds one to `key`'s count and answers it.
    pub(super) fn add(&mut self, key: &str) -> u32 {
        if !self.counts.contains_key(key) {
            if self.order.len() >= self.cap
                && let Some(old) = self.order.pop_front()
            {
                self.counts.remove(&old);
            }
            self.order.push_back(key.to_owned());
        }
        let n = self.counts.entry(key.to_owned()).or_insert(0);
        *n += 1;
        *n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tally_counts_and_forgets_the_oldest_key_past_its_cap() {
        let mut t = Tally::new(2);
        assert_eq!((t.add("a"), t.add("a"), t.add("b")), (1, 2, 1));
        assert_eq!(t.add("c"), 1); // "a" is forgotten
        assert_eq!(t.add("a"), 1);
        assert_eq!(t.add("c"), 2);
    }
}
