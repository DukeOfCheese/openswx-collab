use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::sync::Arc;

use oswx_core::{ClientId, Conflict, ConflictKind, Index, Path, Plain, Seq, Write};
use proptest::prelude::*;

fn p(s: &str) -> Path {
    s.split('/').map(Arc::from).collect()
}

fn c(s: &str) -> ClientId {
    Arc::from(s)
}

/// What one change writes, removes and reads.
#[derive(Clone, Debug, Default)]
struct Ch {
    writes: BTreeSet<Write>,
    removed: BTreeSet<Path>,
    reads: BTreeSet<Path>,
}

impl Ch {
    fn new() -> Ch {
        Ch::default()
    }
    fn set(mut self, path: &str) -> Ch {
        self.writes.insert(Write::At(p(path)));
        self
    }
    fn mv(mut self, list: &str, id: &str) -> Ch {
        self.writes.insert(Write::Position(p(list), Arc::from(id)));
        self
    }
    /// A removal is also a write of the item's path, as `writes(ops)` produces it.
    fn remove(mut self, path: &str) -> Ch {
        self.writes.insert(Write::At(p(path)));
        self.removed.insert(p(path));
        self
    }
    fn read(mut self, path: &str) -> Ch {
        self.reads.insert(p(path));
        self
    }
}

fn record(ix: &Index, seq: Seq, client: &str, ch: &Ch) -> Index {
    ix.record(seq, &c(client), &ch.writes, &ch.removed, &ch.reads)
}

fn check(ix: &Index, base: Seq, client: &str, ch: &Ch) -> Vec<Conflict> {
    ix.check(base, client, &ch.writes, &ch.removed, &ch.reads)
}

fn conflict(path: &str, seq: Seq, by: &str, kind: ConflictKind) -> Conflict {
    Conflict {
        path: p(path),
        seq,
        by: c(by),
        kind,
    }
}

const DEPTH: &str = "features/f2/params/depth";

#[test]
fn different_fields_of_one_feature_do_not_conflict() {
    let ix = record(&Index::default(), 5, "uA.c1", &Ch::new().set(DEPTH));
    let b = Ch::new().set("features/f2/params/draft");
    assert_eq!(check(&ix, 4, "uB.c1", &b), vec![]);
}

#[test]
fn same_field_conflicts_and_names_the_writer() {
    let ix = record(&Index::default(), 5, "uA.c1", &Ch::new().set(DEPTH));
    assert_eq!(
        check(&ix, 4, "uB.c1", &Ch::new().set(DEPTH)),
        vec![conflict(DEPTH, 5, "uA.c1", ConflictKind::Written)]
    );
}

#[test]
fn edit_inside_removed_item_is_a_removal_conflict() {
    let ix = record(
        &Index::default(),
        5,
        "uA.c1",
        &Ch::new().remove("features/f2"),
    );
    let edit = Ch::new().set(DEPTH);
    let removal = conflict("features/f2", 5, "uA.c1", ConflictKind::Removed);
    assert_eq!(check(&ix, 4, "uB.c1", &edit), vec![removal.clone()]);
    // A later insert of the same id clears nothing: a change based before the removal still conflicts.
    let ix = record(&ix, 6, "uC.c1", &Ch::new().set("features/f2"));
    assert_eq!(check(&ix, 4, "uC.c1", &edit), vec![removal]);
    // A change based after the removal does not.
    assert_eq!(check(&ix, 5, "uC.c1", &edit), vec![]);
}

#[test]
fn removal_drops_what_was_recorded_beneath_it() {
    let ix = record(&Index::default(), 5, "uA.c1", &Ch::new().set(DEPTH));
    let ix = record(&ix, 6, "uB.c1", &Ch::new().remove("features/f2"));
    // uB's own removal never conflicts with uB; uA's edit beneath it is gone with the item.
    assert_eq!(check(&ix, 4, "uB.c1", &Ch::new().set(DEPTH)), vec![]);
}

#[test]
fn replacing_a_map_conflicts_with_edits_below_it() {
    let ix = record(&Index::default(), 5, "uB.c1", &Ch::new().set(DEPTH));
    let a = Ch::new().set("features/f2/params");
    assert_eq!(
        check(&ix, 4, "uA.c1", &a),
        vec![conflict(DEPTH, 5, "uB.c1", ConflictKind::Written)]
    );
    // And the other way round: an edit below a replaced map.
    let ix = record(&Index::default(), 5, "uA.c1", &a);
    assert_eq!(
        check(&ix, 4, "uB.c1", &Ch::new().set(DEPTH)),
        vec![conflict(
            "features/f2/params",
            5,
            "uA.c1",
            ConflictKind::Written
        )]
    );
    // Replacing a list conflicts with a removal inside it.
    let ix = record(
        &Index::default(),
        5,
        "uB.c1",
        &Ch::new().remove("features/f2"),
    );
    assert_eq!(
        check(&ix, 4, "uA.c1", &Ch::new().set("features")),
        vec![conflict("features/f2", 5, "uB.c1", ConflictKind::Removed)]
    );
}

#[test]
fn two_inserts_do_not_conflict() {
    let ix = record(
        &Index::default(),
        5,
        "uA.c1",
        &Ch::new().set("features/uA.c1.1"),
    );
    let b = Ch::new().set("features/uB.c1.1");
    assert_eq!(check(&ix, 4, "uB.c1", &b), vec![]);
}

#[test]
fn two_moves_of_one_item_conflict() {
    let ix = record(
        &Index::default(),
        5,
        "uA.c1",
        &Ch::new().mv("features", "f2"),
    );
    assert_eq!(
        check(&ix, 4, "uB.c1", &Ch::new().mv("features", "f2")),
        vec![conflict("features/f2", 5, "uA.c1", ConflictKind::Written)]
    );
    assert_eq!(
        check(&ix, 4, "uB.c1", &Ch::new().mv("features", "f3")),
        vec![]
    );
}

#[test]
fn removing_and_moving_one_item_conflict() {
    let moved = record(
        &Index::default(),
        5,
        "uA.c1",
        &Ch::new().mv("features", "f2"),
    );
    assert_eq!(
        check(&moved, 4, "uB.c1", &Ch::new().remove("features/f2")),
        vec![conflict("features/f2", 5, "uA.c1", ConflictKind::Written)]
    );
    let removed = record(
        &Index::default(),
        5,
        "uA.c1",
        &Ch::new().remove("features/f2"),
    );
    assert_eq!(
        check(&removed, 4, "uB.c1", &Ch::new().mv("features", "f2")),
        vec![conflict("features/f2", 5, "uA.c1", ConflictKind::Removed)]
    );
}

#[test]
fn move_and_edit_of_one_item_do_not_conflict() {
    let moved = record(
        &Index::default(),
        5,
        "uA.c1",
        &Ch::new().mv("features", "f2"),
    );
    assert_eq!(check(&moved, 4, "uB.c1", &Ch::new().set(DEPTH)), vec![]);
    let edited = record(&Index::default(), 5, "uA.c1", &Ch::new().set(DEPTH));
    assert_eq!(
        check(&edited, 4, "uB.c1", &Ch::new().mv("features", "f2")),
        vec![]
    );
}

#[test]
fn removing_something_another_change_read_conflicts() {
    let ix = record(
        &Index::default(),
        5,
        "uB.c1",
        &Ch::new().read("features/f1"),
    );
    assert_eq!(
        check(&ix, 4, "uA.c1", &Ch::new().remove("features/f1")),
        vec![conflict("features/f1", 5, "uB.c1", ConflictKind::Read)]
    );
    // A read beneath the removed item conflicts too; a read of the list holding it does not.
    let ix = record(
        &Index::default(),
        5,
        "uB.c1",
        &Ch::new().read("features/f1/params"),
    );
    assert_eq!(
        check(&ix, 4, "uA.c1", &Ch::new().remove("features/f1")),
        vec![conflict(
            "features/f1/params",
            5,
            "uB.c1",
            ConflictKind::Read
        )]
    );
    let ix = record(&Index::default(), 5, "uB.c1", &Ch::new().read("features"));
    assert_eq!(
        check(&ix, 4, "uA.c1", &Ch::new().remove("features/f1")),
        vec![]
    );
}

#[test]
fn reading_something_removed_conflicts() {
    let ix = record(
        &Index::default(),
        5,
        "uA.c1",
        &Ch::new().remove("features/f1"),
    );
    assert_eq!(
        check(&ix, 4, "uB.c1", &Ch::new().read("features/f1/params")),
        vec![conflict("features/f1", 5, "uA.c1", ConflictKind::Removed)]
    );
    // Writes do not conflict with reads.
    let ix = record(&Index::default(), 5, "uA.c1", &Ch::new().set(DEPTH));
    assert_eq!(check(&ix, 4, "uB.c1", &Ch::new().read(DEPTH)), vec![]);
}

#[test]
fn own_writes_never_conflict() {
    let ix = record(&Index::default(), 5, "uA.c1", &Ch::new().set(DEPTH));
    let ix = record(&ix, 6, "uA.c1", &Ch::new().remove("features/f1"));
    let ix = record(&ix, 7, "uA.c1", &Ch::new().mv("features", "f3"));
    let mine = Ch::new()
        .set(DEPTH)
        .set("features/f1/params")
        .mv("features", "f3");
    assert_eq!(check(&ix, 4, "uA.c1", &mine), vec![]);
    // Another connection of the same user is another client.
    assert_eq!(check(&ix, 4, "uA.c2", &mine).len(), 3);
}

#[test]
fn newest_other_client_is_found_behind_own_newer_write() {
    let ix = record(&Index::default(), 5, "uB.c1", &Ch::new().set(DEPTH));
    let ix = record(&ix, 6, "uA.c1", &Ch::new().set(DEPTH));
    assert_eq!(
        check(&ix, 4, "uA.c1", &Ch::new().set(DEPTH)),
        vec![conflict(DEPTH, 5, "uB.c1", ConflictKind::Written)]
    );
    assert_eq!(
        check(&ix, 4, "uA.c1", &Ch::new().set("features")),
        vec![conflict(DEPTH, 5, "uB.c1", ConflictKind::Written)]
    );
}

#[test]
fn derived_keys_never_conflict() {
    let solved = "features/f2/~solved/x";
    let ix = record(&Index::default(), 5, "uA.c1", &Ch::new().set(solved));
    assert_eq!(check(&ix, 4, "uB.c1", &Ch::new().set(solved)), vec![]);
    // Replacing the map that holds derived data is an ordinary write, and derived data was not recorded.
    assert_eq!(
        check(&ix, 4, "uB.c1", &Ch::new().set("features/f2")),
        vec![]
    );
    // A derived path is not checked against ordinary writes above it either.
    let ix = record(&ix, 6, "uA.c1", &Ch::new().set("features/f2"));
    assert_eq!(check(&ix, 4, "uB.c1", &Ch::new().set(solved)), vec![]);
    assert_eq!(
        check(&ix, 4, "uB.c1", &Ch::new().read("features/~cache")),
        vec![]
    );
}

#[test]
fn before_base_is_ignored() {
    let ix = record(&Index::default(), 5, "uA.c1", &Ch::new().set(DEPTH));
    assert_eq!(check(&ix, 5, "uB.c1", &Ch::new().set(DEPTH)), vec![]);
    assert_eq!(check(&ix, 6, "uB.c1", &Ch::new().set(DEPTH)), vec![]);
}

#[test]
fn conflicts_come_sorted_by_path() {
    let ix = record(
        &Index::default(),
        5,
        "uA.c1",
        &Ch::new().set("b").set("a/x").read("c/y"),
    );
    let ix = record(&ix, 6, "uA.c1", &Ch::new().set("a/y"));
    let got = check(&ix, 4, "uB.c1", &Ch::new().set("b").set("a").remove("c"));
    assert_eq!(
        got,
        vec![
            conflict("a/y", 6, "uA.c1", ConflictKind::Written),
            conflict("b", 5, "uA.c1", ConflictKind::Written),
            conflict("c/y", 5, "uA.c1", ConflictKind::Read),
        ]
    );
}

fn sample_index() -> Index {
    let ix = record(&Index::default(), 3, "uA.c1", &Ch::new().set(DEPTH));
    let ix = record(&ix, 4, "uB.c1", &Ch::new().remove("features/f1"));
    let ix = record(&ix, 5, "uC.c1", &Ch::new().read("features/f3/params"));
    let ix = record(&ix, 6, "uA.c1", &Ch::new().mv("features", "f3"));
    let ix = record(&ix, 7, "uB.c1", &Ch::new().set(DEPTH));
    ix.prune(2)
}

fn sample_changes() -> Vec<Ch> {
    vec![
        Ch::new().set(DEPTH),
        Ch::new().set("features"),
        Ch::new().set("features/f1/params"),
        Ch::new().remove("features/f3"),
        Ch::new().mv("features", "f3"),
        Ch::new().read("features/f1"),
    ]
}

fn all_checks(ix: &Index) -> Vec<Vec<Conflict>> {
    let mut out = vec![];
    for ch in sample_changes() {
        // Bases below the floor are too old to judge.
        for base in ix.floor()..8 {
            for client in ["uA.c1", "uB.c1", "uC.c1", "uD.c1"] {
                out.push(check(ix, base, client, &ch));
            }
        }
    }
    out
}

#[test]
fn wire_round_trip() {
    let ix = sample_index();
    let back = Index::from_wire(&ix.to_wire()).unwrap();
    assert_eq!(back.floor(), 2);
    assert_eq!(all_checks(&back), all_checks(&ix));
    assert!(all_checks(&ix).iter().any(|c| !c.is_empty()));
    assert_eq!(back.to_wire(), ix.to_wire());
    let empty = Index::from_wire(&Index::default().to_wire()).unwrap();
    assert_eq!(empty.floor(), 0);
}

#[test]
fn malformed_wire_is_refused() {
    let s = |t: &str| Plain::from(t);
    let m = |pairs: Vec<(&str, Plain)>| {
        Plain::Map(
            pairs
                .into_iter()
                .map(|(k, v)| (Plain::from(k), v))
                .collect(),
        )
    };
    let good = sample_index().to_wire();
    let mut deep = m(vec![]);
    for _ in 0..200 {
        deep = m(vec![("c", m(vec![("k", deep)]))]);
    }
    let mark = |seq: Plain, client: Plain| Plain::Array(vec![seq, client]);
    for bad in [
        Plain::Nil,
        s("index"),
        m(vec![]),
        m(vec![("floor", Plain::from(1))]),
        m(vec![("floor", Plain::from(-1)), ("root", m(vec![]))]),
        m(vec![("floor", s("1")), ("root", m(vec![]))]),
        m(vec![("floor", Plain::from(1)), ("root", s("x"))]),
        m(vec![
            ("floor", Plain::from(1)),
            ("root", m(vec![])),
            ("extra", Plain::Nil),
        ]),
        m(vec![
            ("floor", Plain::from(1)),
            ("floor", Plain::from(1)),
            ("root", m(vec![])),
        ]),
        m(vec![("floor", Plain::from(0)), ("root", deep)]),
        m(vec![
            ("floor", Plain::from(0)),
            (
                "root",
                m(vec![(
                    "w",
                    Plain::Array(vec![mark(Plain::from(1), s("a"))]),
                )]),
            ),
        ]),
        m(vec![
            ("floor", Plain::from(0)),
            (
                "root",
                m(vec![("c", m(vec![("k", m(vec![("w", s("x"))]))]))]),
            ),
        ]),
        m(vec![
            ("floor", Plain::from(0)),
            (
                "root",
                m(vec![(
                    "c",
                    m(vec![(
                        "k",
                        m(vec![(
                            "w",
                            Plain::Array(vec![mark(Plain::F64(1.0), s("a"))]),
                        )]),
                    )]),
                )]),
            ),
        ]),
        m(vec![
            ("floor", Plain::from(0)),
            (
                "root",
                m(vec![(
                    "c",
                    m(vec![(
                        "k",
                        m(vec![(
                            "w",
                            Plain::Array(vec![
                                mark(Plain::from(1), s("a")),
                                mark(Plain::from(2), s("b")),
                                mark(Plain::from(3), s("c")),
                            ]),
                        )]),
                    )]),
                )]),
            ),
        ]),
        m(vec![
            ("floor", Plain::from(0)),
            (
                "root",
                m(vec![("c", Plain::Map(vec![(Plain::from(1), m(vec![]))]))]),
            ),
        ]),
        m(vec![
            ("floor", Plain::from(0)),
            (
                "root",
                m(vec![("c", m(vec![("k", m(vec![])), ("k", m(vec![]))]))]),
            ),
        ]),
    ] {
        assert!(Index::from_wire(&bad).is_none(), "{bad}");
    }
    assert!(Index::from_wire(&good).is_some());
}

#[test]
fn prune_keeps_writes_drops_old_removals() {
    let ix = record(
        &Index::default(),
        3,
        "uA.c1",
        &Ch::new().remove("features/f1"),
    );
    let ix = record(&ix, 4, "uA.c1", &Ch::new().set(DEPTH));
    let ix = record(&ix, 4, "uB.c1", &Ch::new().read("features/f3"));
    let ix = record(&ix, 5, "uB.c1", &Ch::new().read("features/f4"));
    assert_eq!(ix.floor(), 0);
    let pruned = ix.prune(4);
    assert_eq!(pruned.floor(), 4);
    // The write at 4 is kept; the removal at 3 and the read at 4 are gone; the read at 5 stays.
    // (`check` is only asked about bases at or above the floor, so this is seen in the wire form.)
    let kept = record(&Index::default(), 4, "uA.c1", &Ch::new().set(DEPTH));
    let kept = record(&kept, 5, "uB.c1", &Ch::new().read("features/f4")).prune(4);
    assert_eq!(pruned.to_wire(), kept.to_wire());
    assert_eq!(
        check(&pruned, 4, "uC.c1", &Ch::new().remove("features/f4")),
        vec![conflict("features/f4", 5, "uB.c1", ConflictKind::Read)]
    );
    // The floor never goes down.
    assert_eq!(pruned.prune(2).floor(), 4);
    // Checks from at or above the floor answer as before pruning.
    for base in 4..7 {
        for ch in sample_changes() {
            assert_eq!(
                check(&pruned, base, "uC.c1", &ch),
                check(&ix, base, "uC.c1", &ch)
            );
        }
    }
}

// ---- The trie against a naive model ----

#[derive(Clone, Copy, Debug, PartialEq)]
enum K {
    Wrote,
    Removed,
    Moved,
    Read,
}

#[derive(Clone, Debug)]
struct Mark {
    seq: Seq,
    client: ClientId,
    path: Path,
    kind: K,
}

/// Every mark in a flat list, scanned linearly.
#[derive(Default)]
struct Naive {
    marks: Vec<Mark>,
    floor: Seq,
}

fn derived(path: &[Arc<str>]) -> bool {
    path.iter().any(|s| s.starts_with('~'))
}

/// `a` is `b` or a prefix of it.
fn at_or_above(a: &[Arc<str>], b: &[Arc<str>]) -> bool {
    a.len() <= b.len() && b[..a.len()] == *a
}

fn item(list: &Path, id: &Arc<str>) -> Path {
    let mut q = list.clone();
    q.push(id.clone());
    q
}

impl Naive {
    fn record(&mut self, seq: Seq, client: &ClientId, ch: &Ch) {
        let push = |marks: &mut Vec<Mark>, path: Path, kind| {
            if !derived(&path) {
                marks.push(Mark {
                    seq,
                    client: client.clone(),
                    path,
                    kind,
                });
            }
        };
        for r in &ch.removed {
            if derived(r) {
                continue;
            }
            self.marks
                .retain(|m| !(at_or_above(r, &m.path) && !(m.path == *r && m.kind == K::Removed)));
            push(&mut self.marks, r.clone(), K::Removed);
        }
        for w in &ch.writes {
            match w {
                Write::At(q) if !ch.removed.contains(q) => {
                    push(&mut self.marks, q.clone(), K::Wrote)
                }
                Write::At(_) => {}
                Write::Position(list, id) => push(&mut self.marks, item(list, id), K::Moved),
            }
        }
        for r in &ch.reads {
            push(&mut self.marks, r.clone(), K::Read);
        }
    }

    fn prune(&mut self, floor: Seq) {
        self.floor = self.floor.max(floor);
        let f = self.floor;
        self.marks
            .retain(|m| matches!(m.kind, K::Wrote | K::Moved) || m.seq > f);
    }

    fn check(&self, base: Seq, client: &str, ch: &Ch) -> Vec<Conflict> {
        let live = || {
            self.marks
                .iter()
                .filter(move |m| m.seq > base && &*m.client != client)
        };
        let newest = |found: Vec<(&Mark, ConflictKind)>| {
            found
                .into_iter()
                .min_by_key(|(m, k)| (Reverse(m.seq), m.path.clone(), *k))
                .map(|(m, k)| Conflict {
                    path: m.path.clone(),
                    seq: m.seq,
                    by: m.client.clone(),
                    kind: k,
                })
        };
        let mut out = vec![];
        for w in &ch.writes {
            let found: Vec<_> = match w {
                Write::At(p) if !derived(p) => live()
                    .filter_map(|m| {
                        let above = at_or_above(&m.path, p);
                        let below = at_or_above(p, &m.path);
                        match m.kind {
                            K::Wrote if above || below => Some((m, ConflictKind::Written)),
                            K::Removed if above || below => Some((m, ConflictKind::Removed)),
                            K::Moved if below => Some((m, ConflictKind::Written)),
                            _ => None,
                        }
                    })
                    .collect(),
                Write::Position(list, id) if !derived(&item(list, id)) => {
                    let q = item(list, id);
                    live()
                        .filter_map(|m| match m.kind {
                            K::Wrote if at_or_above(&m.path, &q) => {
                                Some((m, ConflictKind::Written))
                            }
                            K::Removed if at_or_above(&m.path, &q) => {
                                Some((m, ConflictKind::Removed))
                            }
                            K::Moved if m.path == q => Some((m, ConflictKind::Written)),
                            _ => None,
                        })
                        .collect()
                }
                _ => vec![],
            };
            out.extend(newest(found));
        }
        for r in ch.reads.iter().filter(|r| !derived(r)) {
            let found = live()
                .filter(|m| m.kind == K::Removed && at_or_above(&m.path, r))
                .map(|m| (m, ConflictKind::Removed))
                .collect();
            out.extend(newest(found));
        }
        for r in ch.removed.iter().filter(|r| !derived(r)) {
            let found = live()
                .filter(|m| m.kind == K::Read && at_or_above(r, &m.path))
                .map(|m| (m, ConflictKind::Read))
                .collect();
            out.extend(newest(found));
        }
        out.sort_by(|a, b| (&a.path, a.seq, &a.by, a.kind).cmp(&(&b.path, b.seq, &b.by, b.kind)));
        out.dedup();
        out
    }
}

fn arb_path() -> impl Strategy<Value = Path> {
    prop::collection::vec(
        prop_oneof![
            4 => Just("a"),
            4 => Just("b"),
            2 => Just("c"),
            1 => Just("~d"),
        ],
        1..4,
    )
    .prop_map(|segs| segs.into_iter().map(Arc::from).collect())
}

fn arb_change() -> impl Strategy<Value = Ch> {
    (
        prop::collection::vec(arb_path(), 0..3),
        prop::collection::vec(arb_path(), 0..2),
        prop::collection::vec(arb_path(), 0..2),
        prop::collection::vec(arb_path(), 0..2),
    )
        .prop_map(|(sets, moves, removes, reads)| {
            let mut ch = Ch::new();
            for q in sets {
                ch.writes.insert(Write::At(q));
            }
            for mut q in moves {
                let id = q.pop().unwrap();
                if q.is_empty() {
                    q.push(Arc::from("a"));
                }
                ch.writes.insert(Write::Position(q, id));
            }
            for q in removes {
                ch.writes.insert(Write::At(q.clone()));
                ch.removed.insert(q);
            }
            ch.reads.extend(reads);
            ch
        })
}

#[derive(Clone, Debug)]
enum Step {
    Record(usize, Ch),
    Check(Seq, usize, Ch),
    Prune(Seq),
    RoundTrip,
}

fn arb_step() -> impl Strategy<Value = Step> {
    prop_oneof![
        5 => (0..3usize, arb_change()).prop_map(|(c, ch)| Step::Record(c, ch)),
        5 => (0..16u64, 0..3usize, arb_change()).prop_map(|(b, c, ch)| Step::Check(b, c, ch)),
        1 => (0..16u64).prop_map(Step::Prune),
        1 => Just(Step::RoundTrip),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn trie_matches_naive_model(steps in prop::collection::vec(arb_step(), 0..30)) {
        let clients = ["x.1", "y.1", "z.1"];
        let mut ix = Index::default();
        let mut naive = Naive::default();
        let mut seq = 0;
        for step in steps {
            match step {
                Step::Record(client, ch) => {
                    seq += 1;
                    ix = ix.record(seq, &c(clients[client]), &ch.writes, &ch.removed, &ch.reads);
                    naive.record(seq, &c(clients[client]), &ch);
                }
                Step::Check(base, client, ch) => {
                    // Bases below the floor are too old to judge; the server refuses them.
                    let base = base.max(naive.floor);
                    prop_assert_eq!(
                        check(&ix, base, clients[client], &ch),
                        naive.check(base, clients[client], &ch)
                    );
                }
                Step::Prune(floor) => {
                    ix = ix.prune(floor);
                    naive.prune(floor);
                }
                Step::RoundTrip => {
                    ix = Index::from_wire(&ix.to_wire()).unwrap();
                }
            }
            prop_assert_eq!(ix.floor(), naive.floor);
        }
    }
}
