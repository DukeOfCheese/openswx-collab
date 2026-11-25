use std::sync::Arc;

use oswx_core::{KeyedList, Pos, Value};
use proptest::prelude::*;

/// A small deterministic generator, so a failing seed reproduces.
fn next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 33
}

proptest! {
    #[test]
    fn between_is_strictly_between(seed in any::<u64>()) {
        let mut rng = seed;
        let mut list = Pos::spread(3);
        for _ in 0..500 {
            let gap = next(&mut rng) as usize % (list.len() + 1);
            let a = gap.checked_sub(1).map(|i| list[i].clone());
            let b = list.get(gap).cloned();
            let p = Pos::between(a.as_ref(), b.as_ref());
            prop_assert_eq!(Pos::parse(p.as_str()), Some(p.clone()));
            list.insert(gap, p);
        }
        for w in list.windows(2) {
            prop_assert!(w[0] < w[1], "{} !< {}", w[0].as_str(), w[1].as_str());
        }
    }
}

#[test]
fn repeated_insert_at_front_stays_short() {
    let mut first = Pos::spread(1).remove(0);
    let mut longest = first.as_str().len();
    for _ in 0..1_000 {
        let p = Pos::between(None, Some(&first));
        assert!(p < first);
        longest = longest.max(p.as_str().len());
        first = p;
    }
    assert!(longest < 200, "longest was {longest}");
}

#[test]
fn repeated_insert_at_back_stays_short() {
    let mut last = Pos::spread(1).remove(0);
    let mut longest = last.as_str().len();
    for _ in 0..1_000 {
        let p = Pos::between(Some(&last), None);
        assert!(p > last);
        longest = longest.max(p.as_str().len());
        last = p;
    }
    assert!(longest < 200, "longest was {longest}");
}

// Halving the same gap costs one bit per insert, so about one digit per six.
#[test]
fn repeated_insert_in_the_middle_stays_short() {
    let ends = Pos::spread(2);
    let (a, mut b) = (ends[0].clone(), ends[1].clone());
    let mut longest = 0;
    for _ in 0..1_000 {
        let p = Pos::between(Some(&a), Some(&b));
        assert!(a < p && p < b);
        longest = longest.max(p.as_str().len());
        b = p;
    }
    assert!(longest < 250, "longest was {longest}");
}

#[test]
fn first_position_of_an_empty_list() {
    let p = Pos::between(None, None);
    assert_eq!(Pos::parse(p.as_str()).as_ref(), Some(&p));
}

#[test]
fn adjacent_digits_and_extensions() {
    let pos = |s: &str| Pos::parse(s).unwrap();
    for (a, b) in [
        ("1", "2"),
        ("1", "11"),
        ("1", "1V"),
        ("zz", "zzz1"),
        ("Uz", "V"),
    ] {
        let (a, b) = (pos(a), pos(b));
        let p = Pos::between(Some(&a), Some(&b));
        assert!(
            a < p && p < b,
            "{} {} {}",
            a.as_str(),
            p.as_str(),
            b.as_str()
        );
        assert!(!p.as_str().ends_with('0'));
    }
    assert!(Pos::between(Some(&pos("zz")), None) > pos("zz"));
    assert!(Pos::between(None, Some(&pos("01"))) < pos("01"));
    assert!(Pos::between(None, Some(&pos("1"))) < pos("1"));
}

#[test]
fn same_gap_gives_same_position() {
    let p = Pos::spread(2);
    assert_eq!(
        Pos::between(Some(&p[0]), Some(&p[1])),
        Pos::between(Some(&p[0]), Some(&p[1]))
    );
}

#[test]
#[should_panic]
fn equal_bounds_panic() {
    let p = Pos::spread(1);
    Pos::between(Some(&p[0]), Some(&p[0]));
}

#[test]
#[should_panic]
fn reversed_bounds_panic() {
    let p = Pos::spread(2);
    Pos::between(Some(&p[1]), Some(&p[0]));
}

#[test]
fn parse_accepts_only_well_formed_positions() {
    for ok in ["1", "V", "z", "10001", "aZ9"] {
        assert_eq!(Pos::parse(ok).unwrap().as_str(), ok);
    }
    let long = "1".repeat(1024);
    assert!(Pos::parse(&long).is_some());
    for bad in ["", "0", "10", "a-", "a b", "é", "1.5"] {
        assert!(Pos::parse(bad).is_none(), "{bad:?}");
    }
    assert!(Pos::parse(&"1".repeat(1025)).is_none());
}

#[test]
fn equal_positions_order_by_id() {
    let at = Pos::parse("V").unwrap();
    let list = KeyedList::new()
        .inserted(Arc::from("b.1.1"), at.clone(), Value::Null)
        .inserted(Arc::from("a.1.1"), at, Value::Null);
    let ids: Vec<&str> = list.iter().map(|(id, _, _)| id.as_ref()).collect();
    assert_eq!(ids, ["a.1.1", "b.1.1"]);
}
