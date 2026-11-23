"""What the first real client (openswx) needed from the protocol."""
from conftest import H, Live, World, set_depth


def test_a_refused_reservation_says_which_request_it_answers(world, client):
    doc = world.document()
    a = Live(client, doc, world.admin)
    a.send("ids.reserve", request_id="mine-1", names=["Bad9"])
    err = a.recv("error")
    assert err["code"] == "bad_reserve" and err["request_id"] == "mine-1"
    viewer, _ = world.invite("vi@example.com", role="viewer")
    v = Live(client, doc, viewer)
    v.send("ids.reserve", request_id="v-1", features=1)
    assert v.recv("error") == {"type": "error", "code": "read_only", "message": "you have view access to this project",
                               "request_id": "v-1"}


def test_welcome_says_what_this_client_still_holds_and_where_blobs_are(make_client):
    client = make_client(lock_grace=30)
    w = World(client)
    doc = w.document()
    a = Live(client, doc, w.admin, client_id="ada-desktop-run-1")
    assert a.welcome["project_id"] == w.project and a.welcome["reserved"] == {"ids": [], "names": []}
    a.send("ids.reserve", features=2, names=["Fillet"])
    got = a.recv("ids.reserved")
    a.close()  # a short drop
    again = Live(client, doc, w.admin, client_id="ada-desktop-run-1", last_seq=0)
    assert again.welcome["reserved"] == {"ids": sorted(got["ids"]), "names": got["names"]}
    # another client of the same person does not see them
    other = Live(client, doc, w.admin, client_id="ada-laptop-run-9")
    assert other.welcome["reserved"] == {"ids": [], "names": []}


def test_your_own_locks_do_not_stop_your_restore(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    doc = world.document()
    a = Live(client, doc, world.admin)
    assert a.change(set_depth(30), 0)["seq"] == 1
    assert a.lock("f:f2")["granted"] == ["f:f2"]
    assert client.post(f"/api/documents/{doc}/restore", json={"seq": 0}, headers=H(world.admin)).json() == {"seq": 2}
    b = Live(client, doc, bob)
    assert b.lock("f:f1")["granted"] == ["f:f1"]
    r = client.post(f"/api/documents/{doc}/restore", json={"seq": 1}, headers=H(world.admin))
    assert r.status_code == 409 and list(r.json()["detail"]["conflicts"]) == ["f:f1"]
