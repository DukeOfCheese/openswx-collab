//! Several clients edit one document at random without waiting for each other. Whatever the server
//! accepts or refuses, everyone must end up with the server's document, and replaying the accepted
//! changes from the start must give it again. (A port of `tests/test_convergence.py`.)

mod common;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use common::client::{Client, ToClient};
use common::sim::{Rng, features, initial, random_edit};
use oswx_core::{
    Change, ClientId, Head, Index, Known, Op, Refusal, Rules, Seq, Value, apply, decide,
};

const CLIENTS: [&str; 4] = ["u1.c1", "u2.c1", "u3.c1", "u1.c2"];
const ROUNDS: usize = 40;

/// The server's side: the head, which changes each client had accepted and refused, and what it
/// accepted.
struct Server {
    head: Head,
    accepted: HashMap<ClientId, HashSet<Arc<str>>>,
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
            accepted: HashMap::new(),
            refused: HashMap::new(),
            log: Vec::new(),
            outbox: vec![VecDeque::new(); CLIENTS.len()],
        }
    }

    fn receive(&mut self, from: usize, change: Change) {
        let client: ClientId = Arc::from(CLIENTS[from]);
        let accepted_ids = self.accepted.entry(client.clone()).or_default();
        let refused = self.refused.entry(client.clone()).or_default();
        // An id never seen is Unknown, and refused like a refused one.
        let known = |id: &str| {
            if accepted_ids.contains(id) {
                Known::Accepted
            } else if refused.contains(id) {
                Known::Refused
            } else {
                Known::Unknown
            }
        };
        match decide(
            &self.head,
            &client,
            &change,
            &known,
            None,
            &Rules::openswx(),
        ) {
            Ok(accepted) => {
                accepted_ids.insert(change.change_id.clone());
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
