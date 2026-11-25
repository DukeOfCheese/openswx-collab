//! Several clients edit one document at random without waiting for each other. Whatever the server
//! accepts or refuses, everyone must end up with the server's document, and replaying the accepted
//! changes from the start must give it again. (A port of `tests/test_convergence.py`.)

mod common;

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use common::client::{Client, ToClient};
use common::{load, path};
use oswx_core::{
    Change, ClientId, Head, Index, Op, Path, Pos, Refusal, Rules, Seq, Value, apply, decide,
};
use serde_json::json;

const CLIENTS: [&str; 4] = ["u1.c1", "u2.c1", "u3.c1", "u1.c2"];
const ROUNDS: usize = 40;

/// A small seeded generator (SplitMix64), so that a run is the same on every machine and the
/// crate needs no extra dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`; `n` > 0.
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }
}

fn initial() -> Value {
    load(json!({
        "name": "Part1",
        "variables": {"x": 1, "y": 2},
        "features": [
            {"id": "f1", "name": "Sketch1", "type": "Sketch", "params": {"a": 1, "b": 2}},
            {"id": "f2", "name": "Extrude1", "type": "Extrude", "params": {"a": 3, "b": 4}},
            {"id": "f3", "name": "Fillet1", "type": "Fillet", "params": {"a": 5}},
        ],
    }))
}

/// The server's side: the head, which changes each client had refused, and what it accepted.
struct Server {
    head: Head,
    refused: HashMap<ClientId, HashSet<Arc<str>>>,
    log: Vec<(Seq, ClientId, Vec<Op>)>,
    /// Messages on their way to each client, in the order they were sent.
    outbox: Vec<VecDeque<ToClient>>,
}

impl Server {
    fn new(doc: Value) -> Server {
        Server {
            head: Head {
                seq: 0,
                doc,
                index: Index::default(),
            },
            refused: HashMap::new(),
            log: Vec::new(),
            outbox: vec![VecDeque::new(); CLIENTS.len()],
        }
    }

    fn receive(&mut self, from: usize, change: Change) {
        let client: ClientId = Arc::from(CLIENTS[from]);
        let refused = self.refused.entry(client.clone()).or_default();
        let is_refused = |id: &str| refused.contains(id);
        match decide(&self.head, &client, &change, &is_refused, &Rules::openswx()) {
            Ok(accepted) => {
                let seq = accepted.seq;
                self.head = Head {
                    seq,
                    doc: accepted.doc,
                    index: accepted.index,
                };
                self.log.push((seq, client, change.ops.clone()));
                for (to, outbox) in self.outbox.iter_mut().enumerate() {
                    outbox.push_back(if to == from {
                        ToClient::Accepted {
                            change_id: change.change_id.clone(),
                            seq,
                        }
                    } else {
                        ToClient::Change {
                            seq,
                            ops: change.ops.clone(),
                        }
                    });
                }
            }
            Err(refusal) => {
                refused.insert(change.change_id.clone());
                self.outbox[from].push_back(ToClient::Refused {
                    change_id: change.change_id,
                    refusal,
                });
            }
        }
    }
}

/// The features of a document: (id, position, name), in order.
fn features(doc: &Value) -> Vec<(Arc<str>, Pos, Arc<str>)> {
    let Some(Value::Keyed(list)) = doc.at(&["features"]) else {
        return Vec::new();
    };
    list.iter()
        .map(|(id, pos, item)| {
            let name = match item.at(&["name"]) {
                Some(Value::Str(s)) => s.clone(),
                _ => Arc::from(""),
            };
            (id.clone(), pos.clone(), name)
        })
        .collect()
}

/// A position in a random gap between the given positions (any order, repeats allowed).
fn random_pos(rng: &mut Rng, positions: impl Iterator<Item = Pos>) -> Pos {
    let distinct: Vec<Pos> = positions.collect::<BTreeSet<_>>().into_iter().collect();
    let gap = rng.below(distinct.len() + 1);
    Pos::between(gap.checked_sub(1).map(|i| &distinct[i]), distinct.get(gap))
}

/// The first of `Sketch1`, `Sketch2`, ... (for `kind` Sketch) that no feature of `doc` has.
fn free_name(doc: &Value, kind: &str) -> Arc<str> {
    let taken: HashSet<Arc<str>> = features(doc).into_iter().map(|(_, _, n)| n).collect();
    (1..)
        .map(|n| Arc::from(format!("{kind}{n}")))
        .find(|n| !taken.contains(n))
        .unwrap()
}

fn feature_path(id: &str, rest: &[&str]) -> Path {
    let mut p = path(&["features", id]);
    p.extend(rest.iter().map(|s| Arc::from(*s)));
    p
}

/// A random edit of the client's live document: its operations and what it reads. Edits cluster
/// on a few features and fields so that clients get in each other's way.
fn random_edit(rng: &mut Rng, client: &mut Client) -> (Vec<Op>, BTreeSet<Path>) {
    let live = client.live.clone();
    let feats = features(&live);
    let pick = |rng: &mut Rng| feats[rng.below(feats.len())].0.clone();
    let roll = rng.unit();
    let kinds = ["Sketch", "Extrude", "Fillet"];
    if feats.len() < 2 || (0.55..0.7).contains(&roll) {
        // Insert a feature, now and then one that refers to another.
        let id = client.new_item_id();
        let kind = kinds[rng.below(kinds.len())];
        let mut reads = BTreeSet::new();
        let mut item =
            json!({"id": &*id, "name": &*free_name(&live, kind), "type": kind, "params": {"a": 0}});
        if !feats.is_empty() && rng.chance(0.5) {
            let other = pick(rng);
            item["ref"] = json!(&*other);
            reads.insert(path(&["features", &other]));
        }
        let pos = random_pos(rng, feats.iter().map(|(_, p, _)| p.clone()));
        let op = Op::Insert {
            list: path(&["features"]),
            item: load(item),
            pos,
        };
        return (vec![op], reads);
    }
    let ops = if roll < 0.45 {
        // Set a parameter, sometimes suppressing the feature in the same change.
        let id = pick(rng);
        let key = ["a", "b"][rng.below(2)];
        let mut ops = vec![Op::Set {
            path: feature_path(&id, &["params", key]),
            value: Value::Int(rng.below(10) as i64),
        }];
        if rng.chance(0.2) {
            ops.push(Op::Set {
                path: feature_path(&id, &["suppressed"]),
                value: Value::Bool(rng.chance(0.5)),
            });
        }
        ops
    } else if roll < 0.55 {
        // Rename to a free name.
        let id = pick(rng);
        let kind = kinds[rng.below(kinds.len())];
        vec![Op::Set {
            path: feature_path(&id, &["name"]),
            value: Value::Str(free_name(&live, kind)),
        }]
    } else if roll < 0.8 && feats.len() >= 3 {
        vec![Op::Remove {
            list: path(&["features"]),
            id: pick(rng),
        }]
    } else if roll < 0.9 {
        let id = pick(rng);
        let others = feats
            .iter()
            .filter(|(f, _, _)| *f != id)
            .map(|(_, p, _)| p.clone());
        let pos = random_pos(rng, others);
        vec![Op::Move {
            list: path(&["features"]),
            id,
            pos,
        }]
    } else {
        let var = ["x", "y", "z"][rng.below(3)];
        vec![Op::Set {
            path: path(&["variables", var]),
            value: Value::Int(rng.below(100) as i64),
        }]
    };
    (ops, BTreeSet::new())
}

/// How one run went.
#[derive(Default)]
struct Stats {
    submitted: usize,
    accepted: usize,
    stale: usize,
    dependency: usize,
    name_taken: usize,
}

fn deliver(server: &mut Server, clients: &mut [Client], to: usize) {
    while let Some(msg) = server.outbox[to].pop_front() {
        clients[to].receive(msg);
        assert_eq!(
            clients[to].live,
            clients[to].expected_live(),
            "live is confirmed plus pending"
        );
    }
}

/// Hands `n` changes in flight to the server, taking clients in a random order but each client's
/// changes in the order it sent them.
fn submit(rng: &mut Rng, server: &mut Server, in_flight: &mut [VecDeque<Change>], n: usize) {
    for _ in 0..n {
        let waiting: Vec<usize> = (0..in_flight.len())
            .filter(|&i| !in_flight[i].is_empty())
            .collect();
        if waiting.is_empty() {
            return;
        }
        let from = waiting[rng.below(waiting.len())];
        let change = in_flight[from].pop_front().unwrap();
        server.receive(from, change);
    }
}

fn run(seed: u64) -> Stats {
    let mut rng = Rng::new(seed);
    let start = initial();
    let mut server = Server::new(start.clone());
    let mut clients: Vec<Client> = CLIENTS
        .iter()
        .map(|id| Client::new(id, start.clone(), 0))
        .collect();
    let mut in_flight: Vec<VecDeque<Change>> = vec![VecDeque::new(); CLIENTS.len()];
    let mut stats = Stats::default();

    for _ in 0..ROUNDS {
        for (i, client) in clients.iter_mut().enumerate() {
            for _ in 0..rng.below(3) {
                let (ops, reads) = random_edit(&mut rng, client);
                in_flight[i].push_back(client.edit(ops, reads));
                stats.submitted += 1;
            }
        }
        let waiting: usize = in_flight.iter().map(VecDeque::len).sum();
        let n = rng.below(waiting + 1);
        submit(&mut rng, &mut server, &mut in_flight, n);
        for i in 0..clients.len() {
            if rng.chance(0.7) {
                deliver(&mut server, &mut clients, i);
            }
        }
    }
    // Quiet: nobody edits any more; everything in flight arrives.
    submit(&mut rng, &mut server, &mut in_flight, usize::MAX);
    for i in 0..clients.len() {
        deliver(&mut server, &mut clients, i);
    }

    let head = &server.head;
    for c in &clients {
        assert!(
            !c.has_pending(),
            "seed {seed}: {} has unanswered changes",
            c.id
        );
        assert_eq!(c.seq, head.seq, "seed {seed}: {} heard every change", c.id);
        assert_eq!(
            c.confirmed, head.doc,
            "seed {seed}: {} has the server's document",
            c.id
        );
        assert_eq!(
            c.live, head.doc,
            "seed {seed}: {} shows the server's document",
            c.id
        );
    }

    // Replaying the accepted changes, judged as they were, gives the head again.
    let mut doc = start;
    for (n, (seq, client, ops)) in server.log.iter().enumerate() {
        assert_eq!(*seq, n as Seq + 1);
        doc = apply(&doc, ops, &Rules::openswx(), Some(&format!("{client}.")))
            .unwrap_or_else(|e| panic!("seed {seed}: replaying seq {seq} failed: {e}"))
            .doc;
    }
    assert_eq!(doc, head.doc, "seed {seed}: replay gives the head");

    let names: Vec<Arc<str>> = features(&head.doc).into_iter().map(|(_, _, n)| n).collect();
    let distinct: HashSet<&Arc<str>> = names.iter().collect();
    assert_eq!(
        distinct.len(),
        names.len(),
        "seed {seed}: feature names are unique: {names:?}"
    );

    stats.accepted = server.log.len();
    assert_eq!(
        stats.accepted,
        clients.iter().map(|c| c.accepted).sum::<usize>()
    );
    for refusal in clients.iter().flat_map(|c| &c.refusals) {
        match refusal {
            Refusal::Stale(_) => stats.stale += 1,
            Refusal::Dependency(_) => stats.dependency += 1,
            Refusal::NameTaken(_) => stats.name_taken += 1,
            other => panic!("seed {seed}: unexpected refusal {other:?}"),
        }
    }
    assert_eq!(
        stats.submitted,
        stats.accepted + stats.stale + stats.dependency + stats.name_taken
    );
    stats
}

#[test]
fn random_concurrent_editing_converges() {
    let runs: Vec<(u64, Stats)> = [1, 2, 3]
        .into_iter()
        .map(|seed| (seed, run(seed)))
        .collect();
    for (seed, st) in &runs {
        assert!(
            st.accepted * 3 >= st.submitted,
            "seed {seed}: only {} of {} changes accepted",
            st.accepted,
            st.submitted
        );
    }
    // The runs really had conflicts of both kinds.
    assert!(
        runs.iter().any(|(_, st)| st.stale > 0),
        "no change was stale"
    );
    assert!(
        runs.iter().any(|(_, st)| st.dependency > 0),
        "no change was refused for a dependency"
    );
}
