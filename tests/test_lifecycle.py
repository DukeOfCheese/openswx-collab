"""Leaving: deleting an account or an organisation, and exporting."""
import hashlib
import io
import json
import zipfile

from conftest import H, Live, World, part, register, set_depth


def test_deleting_an_account_keeps_the_work_and_frees_the_address(world, client):
    bob, bob_id = world.invite("bob@example.com", "Bob")
    doc = world.document()
    b = Live(client, doc, bob)
    assert b.change(set_depth(20), 0)["seq"] == 1
    assert client.post(f"/api/documents/{doc}/versions", json={"name": "Bob's rev"}, headers=H(bob)).status_code == 201
    assert client.request("DELETE", "/api/me", json={"current_password": "wrong"}, headers=H(bob)).status_code == 403
    assert client.request("DELETE", "/api/me", json={"current_password": "correct horse"}, headers=H(bob)).status_code == 204
    # signed out everywhere, open documents included
    assert client.get("/api/me", headers=H(bob)).status_code == 401
    try:
        while True:
            b.ws.receive_json()
    except Exception as exc:  # noqa: BLE001
        assert getattr(exc, "code", None) == 4401
    assert client.post("/api/auth/login", json={"email": "bob@example.com", "password": "correct horse"}).status_code == 401
    # the work stays, credited to "Deleted user"
    hist = client.get(f"/api/documents/{doc}/history", headers=H(world.admin)).json()
    assert hist[0]["author"]["name"] == "Deleted user" and hist[0]["seq"] == 1
    assert client.get(f"/api/documents/{doc}/versions", headers=H(world.admin)).json()[0]["author"]["name"] == "Deleted user"
    assert client.get(f"/api/documents/{doc}/content", headers=H(world.admin)).json()["content"]["features"][1]["params"]["depth"] == 20
    assert [m["name"] for m in client.get(f"/api/orgs/{world.org}/members", headers=H(world.admin)).json()] == ["Ada"]
    # open documents show the new name too
    a = Live(client, doc, world.admin, last_seq=0)
    assert a.welcome["changes"][0]["author_name"] == "Deleted user"
    # the address is free again
    inv = client.post(f"/api/orgs/{world.org}/invites", json={"email": "bob@example.com"}, headers=H(world.admin)).json()
    new_bob, new_id = register(client, "bob@example.com", "Bob again", invite=inv["token"])
    assert new_id != bob_id


def test_an_account_cannot_leave_an_organisation_or_the_server_without_an_owner(world, client):
    r = client.request("DELETE", "/api/me", json={"current_password": "correct horse"}, headers=H(world.admin))
    assert r.status_code == 409 and r.json()["detail"]["orgs"] == [{"id": world.org, "name": "Robotics"}]
    bob, bob_id = world.invite("bob@example.com", org_role="admin")
    assert client.patch(f"/api/orgs/{world.org}/members/{bob_id}", json={"role": "owner"},
                        headers=H(world.admin)).status_code == 200
    r = client.request("DELETE", "/api/me", json={"current_password": "correct horse"}, headers=H(world.admin))
    assert r.status_code == 409 and "only admin" in r.text  # (the server's only admin, while others exist)


def test_the_last_person_on_a_server_can_leave(client):
    ada, _ = register(client, "ada@example.com")
    assert client.request("DELETE", "/api/me", json={"current_password": "correct horse"}, headers=H(ada)).status_code == 204
    assert client.get("/api/server").json()["registration"] == "invite"  # (the account row is still there)


def test_your_own_data_export(world, client):
    bob, _ = world.invite("bob@example.com", "Bob")
    doc = world.document()
    assert Live(client, doc, bob).change(set_depth(5), 0)["seq"] == 1
    data = client.get("/api/me/export", headers=H(bob)).json()
    assert data["account"]["email"] == "bob@example.com" and data["account"]["has_password"] is True
    assert [o["name"] for o in data["organisations"]] == ["Robotics"]
    assert [p["role"] for p in data["projects"]] == ["editor"]
    assert [c["seq"] for c in data["changes_you_made"]] == [1] and len(data["sessions"]) == 1
    assert "token" not in json.dumps(data["sessions"])  # (tokens are never handed back)
    assert client.get("/api/me/export").status_code == 401


def test_an_organisation_export_has_everything_in_it(world, client):
    body = ("Z" * 30000).encode()
    sha = hashlib.sha256(body).hexdigest()
    assert client.put(f"/api/projects/{world.project}/blobs/{sha}", content=body, headers=H(world.admin)).status_code == 204
    imported = part()
    imported["features"].append({"id": "f3", "name": "Imported1", "type": "import", "suppressed": False,
                                 "params": {"brep": {"$blob": sha, "len": len(body)}}})
    doc = world.document(imported)
    bob, _ = world.invite("bob@example.com", "Bob")
    assert Live(client, doc, bob).change(set_depth(42), 0)["seq"] == 1
    client.post(f"/api/documents/{doc}/versions", json={"name": "Rev A"}, headers=H(bob))
    assert client.get(f"/api/orgs/{world.org}/export", headers=H(bob)).status_code == 403  # owners only
    r = client.get(f"/api/orgs/{world.org}/export", headers=H(world.admin))
    assert r.status_code == 200 and r.headers["content-type"] == "application/zip"
    z = zipfile.ZipFile(io.BytesIO(r.content))
    base = f"projects/{world.project}/{doc}"
    assert {f"{base}/document.json", f"{base}/history.jsonl", f"{base}/versions.json", "organisation.json",
            f"blobs/{sha}"} <= set(z.namelist())
    d = json.loads(z.read(f"{base}/document.json"))
    assert d["seq"] == 1 and d["content"]["features"][1]["params"]["depth"] == 42
    assert json.loads(z.read(f"{base}/history.jsonl").splitlines()[0])["author"] == "Bob"
    assert json.loads(z.read(f"{base}/versions.json"))[0]["name"] == "Rev A"
    assert z.read(f"blobs/{sha}") == body
    org = json.loads(z.read("organisation.json"))
    assert org["name"] == "Robotics" and {m["user"] for m in org["members"]} == {"Ada", "Bob"}


def test_deleting_an_organisation_removes_everything_in_it(world, client, tmp_path):
    body = ("Q" * 30000).encode()
    sha = hashlib.sha256(body).hexdigest()
    client.put(f"/api/projects/{world.project}/blobs/{sha}", content=body, headers=H(world.admin))
    doc = world.document()
    bob, bob_id = world.invite("bob@example.com", "Bob")
    b = Live(client, doc, bob)
    other = client.post("/api/orgs", json={"name": "Other"}, headers=H(world.admin)).json()["id"]
    org_files = tmp_path / "storage" / "orgs" / world.org
    assert org_files.is_dir()
    bad = [({"confirm_name": "Robotics"}, bob, 403), ({"confirm_name": "robotics", "current_password": "correct horse"},
           world.admin, 422), ({"confirm_name": "Robotics", "current_password": "nope"}, world.admin, 403)]
    for payload, token, code in bad:
        assert client.request("DELETE", f"/api/orgs/{world.org}", json=payload, headers=H(token)).status_code == code
    r = client.request("DELETE", f"/api/orgs/{world.org}", json={"confirm_name": "Robotics",
                                                                "current_password": "correct horse"}, headers=H(world.admin))
    assert r.status_code == 204
    try:
        while True:
            b.ws.receive_json()
    except Exception as exc:  # noqa: BLE001
        assert getattr(exc, "code", None) == 4404
    assert client.get(f"/api/documents/{doc}", headers=H(world.admin)).status_code == 404
    assert [o["id"] for o in client.get("/api/orgs", headers=H(world.admin)).json()] == [other]
    assert client.get("/api/orgs", headers=H(bob)).json() == []
    assert not org_files.exists()  # (blobs gone)
    assert not (tmp_path / "storage" / "docs" / doc / "snapshots" / "0000000000.json.gz").exists()
    assert client.get("/api/me", headers=H(bob)).status_code == 200  # (people keep their accounts)
    del bob_id


def test_an_extension_can_refuse_deleting_an_organisation(make_client):
    from fastapi import HTTPException

    from openswx_collab_server.hooks import Hooks

    class StillPaying(Hooks):
        async def before_org_delete(self, db, org):
            raise HTTPException(409, f"{org.name} still has a paid subscription")

    client = make_client(hooks=StillPaying())
    w = World(client)
    r = client.request("DELETE", f"/api/orgs/{w.org}", json={"confirm_name": "Robotics", "current_password": "correct horse"},
                       headers=H(w.admin))
    assert r.status_code == 409 and "paid subscription" in r.text
    assert [o["id"] for o in client.get("/api/orgs", headers=H(w.admin)).json()] == [w.org]
