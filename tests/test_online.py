"""Who is in each document: the `online` list in GET /api/projects/{id}/documents."""
import time

from conftest import H, Live, World


def listing(client, project, token):
    r = client.get(f"/api/projects/{project}/documents", headers=H(token))
    assert r.status_code == 200, r.text
    return {d["id"]: d for d in r.json()}


def wait_until(check, timeout=5.0):
    deadline = time.time() + timeout
    while not check() and time.time() < deadline:
        time.sleep(0.02)
    assert check()


def test_a_member_sees_who_is_in_an_open_document(world, client):
    open_doc, closed_doc = world.document(name="Bracket"), world.document(name="Cover")
    bob, _ = world.invite("bob@example.com", "Bob")
    a = Live(client, open_doc, world.admin)
    Live(client, open_doc, world.admin)  # a second app of Ada's: she is still one person
    b = Live(client, open_doc, bob)
    docs = listing(client, world.project, bob)
    assert docs[open_doc]["online"] == [{"name": "Ada", "colour": a.welcome["you"]["colour"]},
                                        {"name": "Bob", "colour": b.welcome["you"]["colour"]}]
    assert docs[open_doc]["online_count"] == 2
    assert docs[closed_doc]["online"] == [] and docs[closed_doc]["online_count"] == 0
    assert set(docs[open_doc]["online"][0]) == {"name", "colour"}  # (no ids, presence or drafts)


def test_someone_who_left_is_not_listed(world, client):
    doc = world.document()
    bob, _ = world.invite("bob@example.com", "Bob")
    Live(client, doc, world.admin)
    b = Live(client, doc, bob)
    b.close()
    room = client.app.state.hub.rooms[doc]
    wait_until(lambda: len(room.conns) == 1)
    docs = listing(client, world.project, world.admin)
    assert [p["name"] for p in docs[doc]["online"]] == ["Ada"] and docs[doc]["online_count"] == 1


def test_a_closed_or_evicted_room_lists_no_one(make_client):
    client = make_client(room_idle=0.0, sweep_interval=60)
    w = World(client)
    doc = w.document()
    live = Live(client, doc, w.admin)
    hub = client.app.state.hub
    room = hub.rooms[doc]
    live.close()
    wait_until(lambda: not room.conns)
    assert listing(client, w.project, w.admin)[doc]["online"] == []  # open, but empty
    client.portal.call(hub.sweep)
    assert doc not in hub.rooms
    d = listing(client, w.project, w.admin)[doc]
    assert d["online"] == [] and d["online_count"] == 0


def test_an_evicted_room_still_in_hand_lists_no_one(world, client):
    doc = world.document()
    Live(client, doc, world.admin)
    room = client.app.state.hub.rooms[doc]
    room.evicted = True  # (the moment between eviction and the hub forgetting it)
    try:
        assert listing(client, world.project, world.admin)[doc]["online"] == []
    finally:
        room.evicted = False


def test_the_list_is_capped_and_the_count_is_not(world, client):
    doc = world.document()
    people = [world.invite(f"p{i}@example.com", f"P{i}")[0] for i in range(10)]
    for token in people:
        Live(client, doc, token)
    d = listing(client, world.project, world.admin)[doc]
    assert len(d["online"]) == 8 and d["online_count"] == 10
    assert [p["name"] for p in d["online"]] == [f"P{i}" for i in range(8)]


def test_someone_outside_the_project_gets_404_not_the_list(world, client):
    doc = world.document()
    Live(client, doc, world.admin)
    eve, _ = world.invite("eve@example.com", "Eve", role=None)  # in the organisation, not in the project
    r = client.get(f"/api/projects/{world.project}/documents", headers=H(eve))
    assert r.status_code == 404 and "Ada" not in r.text
