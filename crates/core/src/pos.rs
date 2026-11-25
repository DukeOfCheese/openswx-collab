//! Positions order the items of a keyed list. A position is a non-empty string of base-62 digits
//! (`0-9A-Za-z`, ordered as bytes) that does not end in `0`, so there is always room between two.

use std::sync::Arc;

const DIGITS: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pos(Arc<str>);

impl Pos {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// A position from its string form; `None` unless it is well formed.
    pub(crate) fn from_string(s: &str) -> Option<Pos> {
        let ok = s.bytes().all(|b| DIGITS.contains(&b)) && !s.is_empty() && !s.ends_with('0');
        ok.then(|| Pos(Arc::from(s)))
    }

    /// `n` strictly increasing positions with room between neighbours, and before the first and
    /// after the last.
    pub fn spread(n: usize) -> Vec<Pos> {
        let slots = n as u128 + 1;
        let mut width = 2;
        while 62u128.pow(width) < 62 * slots {
            width += 1;
        }
        let step = 62u128.pow(width) / slots;
        (1..=n as u128)
            .map(|i| {
                let mut v = i * step;
                if v.is_multiple_of(62) {
                    v += 1;
                }
                let mut digits = vec![b'0'; width as usize];
                for d in digits.iter_mut().rev() {
                    *d = DIGITS[(v % 62) as usize];
                    v /= 62;
                }
                Pos(Arc::from(String::from_utf8(digits).expect("ascii digits")))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spread_is_increasing_well_formed_and_roomy() {
        for n in [0, 1, 2, 5, 61, 62, 63, 1000, 10_000] {
            let ps = Pos::spread(n);
            assert_eq!(ps.len(), n);
            for w in ps.windows(2) {
                assert!(w[0] < w[1]);
            }
            for p in &ps {
                assert_eq!(Pos::from_string(p.as_str()).as_ref(), Some(p));
            }
        }
    }
}
