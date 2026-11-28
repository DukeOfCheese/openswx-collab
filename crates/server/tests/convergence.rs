//! Several clients edit one document at random over real WebSockets, without waiting for each other.
//! Whatever the server accepts or refuses, every client must end with the server's document, a
//! client that connects afterwards gets it too, and a restart serves the same document at the same
//! seq. (A port of `tests/test_convergence.py`, driven by the reference client of the core's tests.)

mod common;

#[allow(dead_code)]
#[path = "../../core/tests/common/client.rs"]
mod client;
#[allow(dead_code)]
#[path = "../../core/tests/common/sim.rs"]
mod sim;

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use client::{Client, ToClient};
use common::*;
use oswx_core::wire::{RejectCode, ServerMsg};
use oswx_core::{Change, Refusal};
use sim::{Rng, random_edit};

const ROUNDS: usize = 40;

/// What the server sends one client, as the reference client reads it (`None`: nothing for it).
fn to_client(msg: ServerMsg) -> Option<ToClient> {
    match msg {
        ServerMsg::Change(ev) => Some(ToClient::Change {
            seq: ev.seq,
            ops: ev.ops,
        }),
        ServerMsg::Accepted { change_id, seq } => Some(ToClient::Accepted { change_id, seq }),
        ServerMsg::Rejected {
            change_id,
            code,
            message,
            conflicts,
        } => {
            let refusal = match code {
                RejectCode::Stale => Refusal::Stale(conflicts),
                RejectCode::Dependency => Refusal::Dependency(Arc::from("")),
                RejectCode::NameTaken => Refusal::NameTaken(Arc::from("")),
                RejectCode::IdTaken => Refusal::IdTaken(Arc::from("")),
                RejectCode::TooOld => Refusal::TooOld,
                RejectCode::Invalid => Refusal::Invalid(message),
                other => panic!("unexpected refusal {other:?}: {message}"),
            };
            Some(ToClient::Refused { change_id, refusal })
        }
        _ => None,
    }
}

/// Hands every message that has arrived for client `i` to it.
async fn deliver(live: &mut Live, client: &mut Client) {
    while let Some(msg) = live.try_recv(Duration::from_millis(2)).await {
        if let Some(m) = to_client(msg) {
            client.receive(m);
            assert_eq!(
                client.live,
                client.expected_live(),
                "live is confirmed plus pending"
            );
        }
    }
    assert!(live.closed.is_none(), "the server closed {}", client.id);
}

/// Reads for client `i` until `done` holds.
async fn deliver_until(live: &mut Live, client: &mut Client, done: impl Fn(&Client) -> bool) {
    while !done(client) {
        if let Some(m) = to_client(live.recv().await) {
            client.receive(m);
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    submitted: usize,
    accepted: usize,
    stale: usize,
    dependency: usize,
    name_taken: usize,
}

async fn run(server: &TestServer, world: &World, tokens: &[String; 4], seed: u64) -> Stats {
    let name = format!("Sim{seed}");
    let doc = world
        .document(server, Some(sim::initial_json()), Some(&name), None)
        .await;
    let mut lives = Vec::new();
    let mut clients = Vec::new();
    for (i, token) in tokens.iter().enumerate() {
        let hello = Hello::client(&format!("sim-client-{i}"));
        let live = Live::connect_with(server, &doc, token, hello).await;
        let content = live.welcome.content.clone().expect("the document");
        clients.push(Client::new(&live.client_id, content, live.welcome.seq));
        lives.push(live);
    }
    let mut rng = Rng::new(seed);
    let mut in_flight: Vec<VecDeque<Change>> = vec![VecDeque::new(); lives.len()];
    let mut stats = Stats::default();
    for _ in 0..ROUNDS {
        for (i, client) in clients.iter_mut().enumerate() {
            for _ in 0..rng.below(3) {
                let (ops, reads) = random_edit(&mut rng, client);
                in_flight[i].push_back(client.edit(ops, reads));
                stats.submitted += 1;
            }
        }
        // Some of what is waiting goes out, clients in a random order, each in its own order.
        let waiting: usize = in_flight.iter().map(VecDeque::len).sum();
        for _ in 0..rng.below(waiting + 1) {
            let ready: Vec<usize> = (0..lives.len())
                .filter(|&i| !in_flight[i].is_empty())
                .collect();
            let from = ready[rng.below(ready.len())];
            let change = in_flight[from].pop_front().unwrap();
            lives[from].send_change(&change).await;
        }
        for i in 0..lives.len() {
            if rng.chance(0.7) {
                deliver(&mut lives[i], &mut clients[i]).await;
            }
        }
    }
    // Quiet: nobody edits any more; everything in flight arrives and is answered.
    for (i, queue) in in_flight.iter_mut().enumerate() {
        while let Some(change) = queue.pop_front() {
            lives[i].send_change(&change).await;
        }
    }
    for (live, client) in lives.iter_mut().zip(&mut clients) {
        deliver_until(live, client, |c| !c.has_pending()).await;
    }
    let head = server.room(&doc).durable_head().await.unwrap();
    for (live, client) in lives.iter_mut().zip(&mut clients) {
        deliver_until(live, client, |c| c.seq >= head.seq).await;
        assert_eq!(
            client.seq, head.seq,
            "seed {seed}: {} heard every change",
            client.id
        );
        assert_eq!(
            client.confirmed, head.doc,
            "seed {seed}: {} has the document",
            client.id
        );
        assert_eq!(
            client.live, head.doc,
            "seed {seed}: {} shows the document",
            client.id
        );
    }
    // A client that connects now gets the same document.
    let fresh = Live::connect(server, &doc, &tokens[0]).await;
    assert_eq!(fresh.welcome.seq, head.seq);
    assert_eq!(
        fresh.welcome.content.as_ref(),
        Some(&head.doc),
        "seed {seed}"
    );

    stats.accepted = clients.iter().map(|c| c.accepted).sum();
    assert_eq!(stats.accepted as u64, head.seq, "seed {seed}");
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
        stats.accepted + stats.stale + stats.dependency + stats.name_taken,
        "seed {seed}: every change was answered once"
    );
    for live in lives {
        live.close().await;
    }
    stats
}

#[tokio::test]
async fn random_concurrent_editing_over_sockets_converges() {
    let (server, world) = world().await;
    let (bob, _) = world.editor(&server, "bob@example.com").await;
    let (cy, _) = world.editor(&server, "cy@example.com").await;
    // Three users; Ada has two clients.
    let tokens = [world.admin.clone(), bob, cy, world.admin.clone()];
    let mut runs = Vec::new();
    for seed in [1, 2, 3] {
        runs.push((seed, run(&server, &world, &tokens, seed).await));
    }
    for (seed, st) in &runs {
        assert!(st.accepted > 0, "seed {seed}: {st:?}");
    }
    assert!(
        runs.iter().any(|(_, st)| st.stale > 0),
        "the runs had conflicts: {runs:?}"
    );
    // A restart serves every document at the same seq.
    let mut heads = Vec::new();
    for doc in server
        .state
        .store
        .documents_in_project(&world.project)
        .await
        .unwrap()
    {
        let head = server.room(&doc.id).durable_head().await.unwrap();
        heads.push((doc.id, head));
    }
    let server = server.restart().await;
    for (doc, head) in heads {
        let (loaded, _) = oswx_server::snapshot::load_head(
            server.state.store.as_ref(),
            server.state.blobs.as_ref(),
            &doc,
        )
        .await
        .unwrap();
        assert_eq!((loaded.seq, &loaded.doc), (head.seq, &head.doc));
        let live = Live::connect(&server, &doc, &world.admin).await;
        assert_eq!(live.welcome.seq, head.seq);
        assert_eq!(live.welcome.content, Some(head.doc));
    }
}
