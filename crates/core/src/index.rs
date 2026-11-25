//! The conflict index: per document path, who last wrote, removed, moved or read it, and at which
//! sequence number, so that a change made on an older base can be judged against what others did
//! since.
//!
//! The rule, where *other* means a different client and *after* a sequence number above the
//! change's base:
//!
//! - a `Write::At(p)` conflicts with another's write after base at `p`, at a prefix of `p` or under
//!   `p`, with a move after base of an item at or under `p` (`Written`), and with a removal after
//!   base at, above or under `p` (`Removed`);
//! - a `Write::Position(list, id)` conflicts with another's move of the same item, a write at the
//!   item or a prefix of it (`Written`), or a removal of the item or a prefix (`Removed`); edits
//!   inside the item do not;
//! - a read of `p` conflicts with another's removal after base of `p` or a prefix (`Removed`);
//! - a removal of `p` conflicts with another's read after base of `p` or anything under it (`Read`);
//! - a path with a segment starting `~` is derived data: never checked, never recorded.
//!
//! A removal is a tombstone: it drops everything recorded at or beneath the item except earlier
//! removals of it, and later writes there clear nothing. For each path a change names, `check`
//! reports the newest conflicting mark only.

use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::sync::Arc;

use imbl::OrdMap;

use crate::op::{MAX_DEPTH, Write};
use crate::path::Path;
use crate::value::Plain;

pub type Seq = u64;
/// `<user>.<client>`.
pub type ClientId = Arc<str>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    /// Where the conflicting mark sits.
    pub path: Path,
    pub seq: Seq,
    pub by: ClientId,
    pub kind: ConflictKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConflictKind {
    Written,
    Removed,
    Read,
}

type Mark = (Seq, ClientId);

/// The newest of a set of marks, and the newest by a client other than the newest's: enough to
/// answer "the newest mark not by `c`" for every `c`.
#[derive(Clone, Debug, Default, PartialEq)]
struct Latest {
    first: Option<Mark>,
    second: Option<Mark>,
}

impl Latest {
    fn of(mut marks: Vec<Mark>) -> Latest {
        marks.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let mut it = marks.into_iter();
        let first = it.next();
        let second = first
            .as_ref()
            .and_then(|(_, top)| it.find(|(_, c)| c != top));
        Latest { first, second }
    }

    fn iter(&self) -> impl Iterator<Item = &Mark> {
        self.first.iter().chain(self.second.iter())
    }

    fn add(&mut self, mark: &Mark) {
        let marks = self.iter().cloned().chain([mark.clone()]).collect();
        *self = Latest::of(marks);
    }

    fn merge(&mut self, other: &Latest) {
        if !other.is_empty() {
            let marks = self.iter().chain(other.iter()).cloned().collect();
            *self = Latest::of(marks);
        }
    }

    fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    /// The newest mark after `base` not by `client`.
    fn after(&self, base: Seq, client: &str) -> Option<&Mark> {
        self.iter()
            .find(|(_, c)| &**c != client)
            .filter(|(seq, _)| *seq > base)
    }

    fn above(&self, floor: Seq) -> Latest {
        Latest::of(
            self.iter()
                .filter(|(seq, _)| *seq > floor)
                .cloned()
                .collect(),
        )
    }
}

#[derive(Clone, Debug, Default)]
struct Node {
    wrote: Latest,
    removed: Latest,
    moved: Latest,
    read: Latest,
    /// Writes, removals and moves here or below. It may still name marks a removal or a prune
    /// dropped: it only lets a search skip subtrees, so too much costs time, never correctness.
    changes: Latest,
    /// Reads here or below, in the same way.
    reads: Latest,
    children: OrdMap<Arc<str>, Arc<Node>>,
}

impl Node {
    fn is_empty(&self) -> bool {
        self.wrote.is_empty()
            && self.removed.is_empty()
            && self.moved.is_empty()
            && self.read.is_empty()
            && self.children.is_empty()
    }

    /// Recompute both summaries exactly from the node's own marks and its children's summaries.
    fn sealed(mut self) -> Node {
        let mut changes = self.wrote.clone();
        changes.merge(&self.removed);
        changes.merge(&self.moved);
        let mut reads = self.read.clone();
        for child in self.children.values() {
            changes.merge(&child.changes);
            reads.merge(&child.reads);
        }
        self.changes = changes;
        self.reads = reads;
        self
    }
}

#[derive(Clone, Copy)]
enum Field {
    Wrote,
    Moved,
    Read,
    /// A tombstone.
    Removed,
}

/// `node` with `mark` set at `path`, the nodes along the way copied and the rest shared.
fn marked(node: &Node, path: &[Arc<str>], field: Field, mark: &Mark) -> Node {
    let mut n = node.clone();
    match path.split_first() {
        Some((seg, rest)) => {
            let child = n.children.get(seg).cloned().unwrap_or_default();
            n.children
                .insert(seg.clone(), Arc::new(marked(&child, rest, field, mark)));
        }
        None => match field {
            Field::Wrote => n.wrote.add(mark),
            Field::Moved => n.moved.add(mark),
            Field::Read => n.read.add(mark),
            Field::Removed => {
                let mut removed = std::mem::take(&mut n.removed);
                removed.add(mark);
                n = Node {
                    removed,
                    ..Node::default()
                }
                .sealed();
            }
        },
    }
    match field {
        Field::Read => n.reads.add(mark),
        _ => n.changes.add(mark),
    }
    n
}

fn derived(path: &[Arc<str>]) -> bool {
    path.iter().any(|s| s.starts_with('~'))
}

fn item(list: &Path, id: &Arc<str>) -> Path {
    let mut p = list.clone();
    p.push(id.clone());
    p
}

/// The newest conflicting mark for one path of a change.
struct Search<'a> {
    base: Seq,
    client: &'a str,
    best: Option<Conflict>,
}

impl Search<'_> {
    fn offer(&mut self, path: &[Arc<str>], marks: &Latest, kind: ConflictKind) {
        let Some((seq, by)) = marks.after(self.base, self.client) else {
            return;
        };
        let better = self.best.as_ref().is_none_or(|b| {
            (Reverse(*seq), path, kind) < (Reverse(b.seq), b.path.as_slice(), b.kind)
        });
        if better {
            self.best = Some(Conflict {
                path: path.to_vec(),
                seq: *seq,
                by: by.clone(),
                kind,
            });
        }
    }

    /// Whether a subtree summarised by `summary` may hold a mark worth offering.
    fn worth(&self, summary: &Latest) -> bool {
        match summary.after(self.base, self.client) {
            None => false,
            Some((seq, _)) => self.best.as_ref().is_none_or(|b| *seq >= b.seq),
        }
    }

    /// Writes, removals and moves strictly below `node`, which sits at `path`.
    fn changes_below(&mut self, node: &Node, path: &mut Path) {
        for (seg, child) in node.children.iter() {
            if !self.worth(&child.changes) {
                continue;
            }
            path.push(seg.clone());
            self.offer(path, &child.wrote, ConflictKind::Written);
            self.offer(path, &child.removed, ConflictKind::Removed);
            self.offer(path, &child.moved, ConflictKind::Written);
            self.changes_below(child, path);
            path.pop();
        }
    }

    /// Reads strictly below `node`, which sits at `path`.
    fn reads_below(&mut self, node: &Node, path: &mut Path) {
        for (seg, child) in node.children.iter() {
            if !self.worth(&child.reads) {
                continue;
            }
            path.push(seg.clone());
            self.offer(path, &child.read, ConflictKind::Read);
            self.reads_below(child, path);
            path.pop();
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Index {
    root: Arc<Node>,
    floor: Seq,
}

impl Index {
    /// The nodes along `path` that exist: entry `i` sits at `path[..=i]`.
    fn along(&self, path: &[Arc<str>]) -> Vec<&Node> {
        let mut out = Vec::with_capacity(path.len());
        let mut node = &*self.root;
        for seg in path {
            match node.children.get(seg) {
                Some(child) => {
                    node = child;
                    out.push(node);
                }
                None => break,
            }
        }
        out
    }

    /// Conflicts for a change by `client` made on `base`: empty means acceptable.
    ///
    /// Only sound for `base >= self.floor()`: below the floor, pruned removals and reads are no
    /// longer there to conflict with. Callers refuse such a change as too old instead.
    pub fn check(
        &self,
        base: Seq,
        client: &str,
        writes: &BTreeSet<Write>,
        removed: &BTreeSet<Path>,
        reads: &BTreeSet<Path>,
    ) -> Vec<Conflict> {
        debug_assert!(base >= self.floor, "check needs base >= floor");
        let search = || Search {
            base,
            client,
            best: None,
        };
        let mut out = Vec::new();
        for write in writes {
            let mut s = search();
            match write {
                Write::At(p) if !derived(p) => {
                    let nodes = self.along(p);
                    for (i, node) in nodes.iter().enumerate() {
                        let at = &p[..=i];
                        s.offer(at, &node.wrote, ConflictKind::Written);
                        s.offer(at, &node.removed, ConflictKind::Removed);
                        if at.len() == p.len() {
                            s.offer(at, &node.moved, ConflictKind::Written);
                            s.changes_below(node, &mut p.clone());
                        }
                    }
                }
                Write::Position(list, id) => {
                    let q = item(list, id);
                    if derived(&q) {
                        continue;
                    }
                    for (i, node) in self.along(&q).iter().enumerate() {
                        let at = &q[..=i];
                        s.offer(at, &node.wrote, ConflictKind::Written);
                        s.offer(at, &node.removed, ConflictKind::Removed);
                        if at.len() == q.len() {
                            s.offer(at, &node.moved, ConflictKind::Written);
                        }
                    }
                }
                Write::At(_) => {}
            }
            out.extend(s.best);
        }
        for p in reads.iter().filter(|p| !derived(p)) {
            let mut s = search();
            for (i, node) in self.along(p).iter().enumerate() {
                s.offer(&p[..=i], &node.removed, ConflictKind::Removed);
            }
            out.extend(s.best);
        }
        for p in removed.iter().filter(|p| !derived(p)) {
            let mut s = search();
            if let Some(node) = self.along(p).get(p.len().wrapping_sub(1)) {
                s.offer(p, &node.read, ConflictKind::Read);
                s.reads_below(node, &mut p.clone());
            }
            out.extend(s.best);
        }
        out.sort_by(|a, b| (&a.path, a.seq, &a.by, a.kind).cmp(&(&b.path, b.seq, &b.by, b.kind)));
        out.dedup();
        out
    }

    /// Record an accepted change. Removals are recorded first, as tombstones; a removed path's
    /// `Write::At` is the removal itself.
    pub fn record(
        &self,
        seq: Seq,
        client: &ClientId,
        writes: &BTreeSet<Write>,
        removed: &BTreeSet<Path>,
        reads: &BTreeSet<Path>,
    ) -> Index {
        let mark: Mark = (seq, client.clone());
        let mut root = (*self.root).clone();
        let mut mark_at = |path: &[Arc<str>], field| {
            if !path.is_empty() && !derived(path) {
                root = marked(&root, path, field, &mark);
            }
        };
        for p in removed {
            mark_at(p, Field::Removed);
        }
        for write in writes {
            match write {
                Write::At(p) if removed.contains(p) => {}
                Write::At(p) => mark_at(p, Field::Wrote),
                Write::Position(list, id) => mark_at(&item(list, id), Field::Moved),
            }
        }
        for p in reads {
            mark_at(p, Field::Read);
        }
        Index {
            root: Arc::new(root),
            floor: self.floor,
        }
    }

    /// Bases below this are too old to judge.
    pub fn floor(&self) -> Seq {
        self.floor
    }

    /// Drop removals and reads at or below `floor`, which becomes the floor unless it is lower.
    /// Writes and moves are kept.
    pub fn prune(&self, floor: Seq) -> Index {
        fn pruned(node: &Node, floor: Seq) -> Option<Node> {
            let n = Node {
                wrote: node.wrote.clone(),
                removed: node.removed.above(floor),
                moved: node.moved.clone(),
                read: node.read.above(floor),
                children: node
                    .children
                    .iter()
                    .filter_map(|(seg, child)| Some((seg.clone(), Arc::new(pruned(child, floor)?))))
                    .collect(),
                ..Node::default()
            };
            (!n.is_empty()).then(|| n.sealed())
        }
        let floor = self.floor.max(floor);
        Index {
            root: Arc::new(pruned(&self.root, floor).unwrap_or_default()),
            floor,
        }
    }

    /// For snapshots: `{floor, root}`, a node being `{w, x, m, r, c}` (written, removed, moved,
    /// read: lists of `[seq, client]`; children: a map), with empty entries left out.
    pub fn to_wire(&self) -> Plain {
        fn marks(l: &Latest) -> Plain {
            Plain::Array(
                l.iter()
                    .map(|(seq, c)| Plain::Array(vec![Plain::from(*seq), Plain::from(&**c)]))
                    .collect(),
            )
        }
        fn node(n: &Node) -> Plain {
            let mut pairs = Vec::new();
            for (key, l) in [
                ("w", &n.wrote),
                ("x", &n.removed),
                ("m", &n.moved),
                ("r", &n.read),
            ] {
                if !l.is_empty() {
                    pairs.push((Plain::from(key), marks(l)));
                }
            }
            if !n.children.is_empty() {
                let children = n
                    .children
                    .iter()
                    .map(|(seg, child)| (Plain::from(&**seg), node(child)))
                    .collect();
                pairs.push((Plain::from("c"), Plain::Map(children)));
            }
            Plain::Map(pairs)
        }
        Plain::Map(vec![
            (Plain::from("floor"), Plain::from(self.floor)),
            (Plain::from("root"), node(&self.root)),
        ])
    }

    /// The index `to_wire` wrote; `None` for anything else.
    pub fn from_wire(p: &Plain) -> Option<Index> {
        let [floor, root] = fields(p, ["floor", "root"])?;
        let root = node_from_wire(root?, 0)?;
        // Every path has a segment, so nothing is ever marked at the root.
        let root_marked = [&root.wrote, &root.removed, &root.moved, &root.read]
            .iter()
            .any(|l| !l.is_empty());
        if root_marked {
            return None;
        }
        Some(Index {
            floor: floor?.as_u64()?,
            root: Arc::new(root),
        })
    }
}

/// The values of a map's entries, by key; `None` unless `p` is a map whose keys are distinct
/// strings among `keys`.
fn fields<'a, const N: usize>(p: &'a Plain, keys: [&str; N]) -> Option<[Option<&'a Plain>; N]> {
    let Plain::Map(pairs) = p else {
        return None;
    };
    let mut out = [None; N];
    for (k, v) in pairs {
        let i = keys.iter().position(|key| k.as_str() == Some(key))?;
        if out[i].replace(v).is_some() {
            return None;
        }
    }
    Some(out)
}

fn latest_from_wire(p: Option<&Plain>) -> Option<Latest> {
    let Some(p) = p else {
        return Some(Latest::default());
    };
    let items = p.as_array()?;
    if items.len() > 2 {
        return None;
    }
    let marks = items
        .iter()
        .map(|m| match m.as_array()?.as_slice() {
            [seq, client] => Some((seq.as_u64()?, Arc::from(client.as_str()?))),
            _ => None,
        })
        .collect::<Option<Vec<Mark>>>()?;
    Some(Latest::of(marks))
}

fn node_from_wire(p: &Plain, depth: usize) -> Option<Node> {
    if depth > MAX_DEPTH {
        return None;
    }
    let [w, x, m, r, c] = fields(p, ["w", "x", "m", "r", "c"])?;
    let mut children = OrdMap::new();
    if let Some(c) = c {
        let Plain::Map(pairs) = c else {
            return None;
        };
        for (seg, child) in pairs {
            let seg: Arc<str> = Arc::from(seg.as_str()?);
            let child = Arc::new(node_from_wire(child, depth + 1)?);
            if children.insert(seg, child).is_some() {
                return None;
            }
        }
    }
    let node = Node {
        wrote: latest_from_wire(w)?,
        removed: latest_from_wire(x)?,
        moved: latest_from_wire(m)?,
        read: latest_from_wire(r)?,
        children,
        ..Node::default()
    };
    Some(node.sealed())
}
