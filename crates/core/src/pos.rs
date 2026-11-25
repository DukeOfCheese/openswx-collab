//! Positions order the items of a keyed list. A position is a non-empty string of base-62 digits
//! (`0-9A-Za-z`, ordered as bytes) that does not end in `0`, so there is always room between two.

use std::sync::Arc;

/// Longest position `parse` accepts, in digits.
pub const MAX_LEN: usize = 1024;
const DIGITS: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pos(Arc<str>);

impl Pos {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// A position from its string form: 1 to 1024 base-62 digits, not ending in `0`; else `None`.
    pub fn parse(s: &str) -> Option<Pos> {
        if s.len() > MAX_LEN {
            return None;
        }
        Pos::from_string(s)
    }

    /// A position from its string form; `None` unless it is well formed (any length).
    pub(crate) fn from_string(s: &str) -> Option<Pos> {
        let ok = s.bytes().all(|b| DIGITS.contains(&b)) && !s.is_empty() && !s.ends_with('0');
        ok.then(|| Pos(Arc::from(s)))
    }

    /// A position strictly between `a` and `b`; `None` stands for the start or the end of the list.
    /// Reads a position as a base-62 fraction in (0, 1). The result is deterministic, never ends in
    /// `0`, and is chosen so that repeated insertion at the front grows it by about one digit per
    /// 60 inserts, at the back by about one digit per 31, and in one gap by about one digit per 6.
    ///
    /// Panics if `a >= b`. Items that share a position are not placed with this; callers keep them
    /// in id order. Also panics if the result would be longer than `MAX_LEN` (only possible when a
    /// neighbour is about that long already); `try_between` answers `None` instead.
    pub fn between(a: Option<&Pos>, b: Option<&Pos>) -> Pos {
        if let (Some(a), Some(b)) = (a, b) {
            assert!(a < b, "Pos::between needs a < b, got {a:?} and {b:?}");
        }
        Pos::try_between(a, b).expect("Pos::between: the result is longer than MAX_LEN")
    }

    /// As `between`, without panicking: `None` when `a >= b` or when the result would be longer
    /// than `MAX_LEN` (so that `parse` would refuse it).
    pub fn try_between(a: Option<&Pos>, b: Option<&Pos>) -> Option<Pos> {
        let a = a.map(Pos::as_str);
        let b = b.map(Pos::as_str);
        if let (Some(a), Some(b)) = (a, b)
            && a >= b
        {
            return None;
        }
        let digits = match (a, b) {
            (None, None) => vec![MID],
            (Some(a), None) => above(a.as_bytes()),
            (None, Some(b)) => below(b.as_bytes()),
            (Some(a), Some(b)) => midpoint(a.as_bytes(), b.as_bytes()),
        };
        (digits.len() <= MAX_LEN)
            .then(|| Pos(Arc::from(String::from_utf8(digits).expect("ascii digits"))))
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

const MID: u8 = DIGITS[31];

fn value(digit: u8) -> usize {
    DIGITS.iter().position(|&d| d == digit).expect("a digit")
}

/// Something above `a` (possibly empty, meaning 0) and below 1: the next digit up, or one more
/// digit when `a` ends in the top digit.
fn above(a: &[u8]) -> Vec<u8> {
    let mut out = a.to_vec();
    match out.last_mut() {
        None => out.push(MID),
        Some(last) if *last == DIGITS[61] => out.push(MID),
        Some(last) => *last = DIGITS[value(*last) + 1],
    }
    out
}

/// Something above 0 and below `b`: the next digit down, or `0z` in place of a final `1`.
fn below(b: &[u8]) -> Vec<u8> {
    let mut out = b.to_vec();
    let last = out.pop().expect("a position is not empty");
    match value(last) {
        1 => out.extend([DIGITS[0], DIGITS[61]]),
        v => out.push(DIGITS[v - 1]),
    }
    out
}

/// Something above `a` and below `b`, with `a < b` and `b` not ending in `0`.
fn midpoint(a: &[u8], b: &[u8]) -> Vec<u8> {
    let at = |s: &[u8], i: usize| s.get(i).map_or(0, |&d| value(d));
    let i = (0..).find(|&i| at(a, i) != at(b, i)).expect("a < b");
    let (da, db) = (at(a, i), at(b, i));
    let mut out = b[..i].to_vec();
    if db - da > 1 {
        out.push(DIGITS[(da + db) / 2]);
    } else {
        // Keep a's digit here, then go above whatever follows it in a.
        out.push(DIGITS[da]);
        out.extend(above(a.get(i + 1..).unwrap_or(&[])));
    }
    out
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
