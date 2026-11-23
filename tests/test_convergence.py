"""Many clients editing at random, without waiting for each other: everyone must end up with the server's
document, and a restarted server must rebuild the same document from its history."""
import copy
import random

import pytest

from openswx_collab_protocol.ops import absorb_ids, apply, diff, fingerprint

from conftest import H, Live, World


class Sim:
    """A client as docs/protocol.md describes one: `confirmed` plus changes not yet answered."""

    def __init__(self, client, doc, token, rng):
        self.live = Live(client, doc, token)
        self.rng = rng
        self.seq = self.live.welcome["seq"]
        self.confirmed = self.live.welcome["content"]
        self.pending: dict[str, list] = {}
        self.accepted = self.rejected = 0
        self.codes: set[str] = set()
        self.n = 0

    def view(self) -> dict:
        v = copy.deepcopy(self.confirmed)
        for ops in self.pending.values():
            try:
                apply(v, ops)
            except Exception:  # noqa: BLE001 - built on something that was since refused: the server refuses it too
                pass
        return v

    def handle(self, m: dict) -> None:
        if m["type"] == "change":
            assert m["seq"] == self.seq + 1, "changes arrive in order, none missing"
            apply(self.confirmed, m["ops"])
            absorb_ids(self.confirmed)
            self.seq = m["seq"]
        elif m["type"] == "change.accepted":
            assert m["seq"] == self.seq + 1
            apply(self.confirmed, self.pending.pop(m["change_id"]))
            self.seq = m["seq"]
            self.accepted += 1
        elif m["type"] == "change.rejected":
            self.pending.pop(m["change_id"])
            self.rejected += 1
            self.codes.add(m["code"])

    def settle(self) -> None:
        for m in self.live.drain():
            self.handle(m)

    def reserve(self, prefix: str) -> tuple[str, str]:
        self.live.send("ids.reserve", features=1, names=[prefix])
        while True:
            m = self.live.ws.receive_json()
            if m["type"] == "ids.reserved":
                return m["ids"][0], m["names"][0]
            self.handle(m)

    def act(self) -> None:
        rng, before = self.rng, self.view()
        after = copy.deepcopy(before)
        feats = after["features"]
        roll = rng.random()
        if roll < 0.2 or len(feats) < 2:
            fid, name = self.reserve(rng.choice(["Sketch", "Fillet", "Cut-Extrude"]))
            feats.insert(rng.randint(0, len(feats)), {"id": fid, "name": name, "type": "x", "suppressed": False,
                                                      "params": {"v": rng.random()}})
        elif roll < 0.27:
            del feats[rng.randrange(len(feats))]
        elif roll < 0.33:
            rng.shuffle(feats)
        elif roll < 0.9:
            f = rng.choice(feats)
            f["params"][rng.choice("abcd")] = {"v": rng.randint(0, 9), "pts": {rng.choice("pq"): [rng.random()]}}
            if rng.random() < 0.2:
                f["suppressed"] = not f["suppressed"]
        else:
            after.setdefault("variables", {})[rng.choice("xyz")] = rng.randint(0, 99)
        ops = diff(before, after)
        if ops:
            self.n += 1
            cid = f"s{self.n}"
            self.pending[cid] = ops
            self.live.send("change", change_id=cid, base_seq=self.seq, ops=ops, command="random")


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_random_concurrent_editing_converges(make_client, seed):
    rng = random.Random(seed)
    client = make_client(snapshot_every=17)
    w = World(client)
    tokens = [w.admin] + [w.invite(f"u{n}@example.com")[0] for n in range(3)]
    doc = w.document()
    sims = [Sim(client, doc, t, random.Random(seed * 100 + n)) for n, t in enumerate(tokens)]
    for _ in range(40):
        for s in rng.sample(sims, k=rng.randint(1, len(sims))):  # several act before anyone hears the others
            for _ in range(rng.randint(1, 3)):
                s.act()
        for s in sims:
            if rng.random() < 0.7:  # some stay behind for a round
                s.settle()
    for _ in range(2):
        for s in sims:
            s.settle()
    server = client.get(f"/api/documents/{doc}/content", headers=H(w.admin)).json()
    head = server["seq"]
    want = fingerprint(server["content"])
    for s in sims:
        assert not s.pending and s.seq == head
        assert fingerprint(s.confirmed) == want
    ids = [f["id"] for f in server["content"]["features"]]
    names = [f["name"] for f in server["content"]["features"]]
    assert len(ids) == len(set(ids)) and len(names) == len(set(names))
    accepted, rejected = sum(s.accepted for s in sims), sum(s.rejected for s in sims)
    assert accepted == head and accepted > 60 and rejected > 0  # the run really had conflicts
    assert set().union(*(s.codes for s in sims)) <= {"stale", "invalid"}
    # every stored inverse really undoes its change, and a restart replays to the same document
    hist = {h["seq"] for h in client.get(f"/api/documents/{doc}/history", params={"limit": 500}, headers=H(w.admin)).json()}
    assert hist == set(range(1, head + 1))
    mid = fingerprint(client.get(f"/api/documents/{doc}/content", params={"seq": head // 2}, headers=H(w.admin)).json()["content"])
    for s in sims:
        s.live.close()
    client.__exit__(None, None, None)
    again = make_client()
    assert fingerprint(again.get(f"/api/documents/{doc}/content", headers=H(w.admin)).json()["content"]) == want
    assert fingerprint(again.get(f"/api/documents/{doc}/content", params={"seq": head // 2},
                                 headers=H(w.admin)).json()["content"]) == mid
    late = Live(again, doc, tokens[1])
    assert late.welcome["seq"] == head and fingerprint(late.welcome["content"]) == want
