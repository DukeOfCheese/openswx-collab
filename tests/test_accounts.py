"""Accounts, sign-in, organisations, invites, project roles."""
import hashlib
from urllib.parse import parse_qs, urlsplit

import httpx

from openswx_collab_server.hooks import Hooks

from conftest import H, World, register


def test_first_account_is_admin_then_invite_only(client):
    assert client.get("/api/server").json()["registration"] == "first_user"
    admin, _ = register(client, "ada@example.com")
    assert client.get("/api/me", headers=H(admin)).json()["is_server_admin"] is True
    assert client.get("/api/server").json()["registration"] == "invite"
    r = client.post("/api/auth/register", json={"email": "eve@example.com", "password": "password1", "name": "Eve"})
    assert r.status_code == 403
    assert client.get("/api/me").status_code == 401
    assert client.get("/api/me", headers=H("nonsense")).status_code == 401


def test_login_logout_and_password_rules(client):
    token, _ = register(client, "Ada@Example.com", password="correct horse")
    assert client.post("/api/auth/register", json={"email": "ada@example.com", "password": "correct horse"}).status_code == 409
    assert client.post("/api/auth/register", json={"email": "x@example.com", "password": "short"}).status_code == 422
    assert client.post("/api/auth/login", json={"email": "ada@example.com", "password": "wrong"}).status_code == 401
    t2 = client.post("/api/auth/login", json={"email": "ADA@example.com", "password": "correct horse"}).json()["token"]
    assert client.post("/api/auth/logout", headers=H(t2)).status_code == 204
    assert client.get("/api/me", headers=H(t2)).status_code == 401
    # changing the password ends the other sessions
    t3 = client.post("/api/auth/login", json={"email": "ada@example.com", "password": "correct horse"}).json()["token"]
    assert client.patch("/api/me", json={"password": "new password", "current_password": "nope"}, headers=H(token)).status_code == 403
    assert client.patch("/api/me", json={"password": "new password", "current_password": "correct horse"},
                        headers=H(token)).status_code == 200
    assert client.get("/api/me", headers=H(t3)).status_code == 401
    assert client.get("/api/me", headers=H(token)).status_code == 200


def test_login_is_rate_limited(client):
    register(client, "ada@example.com")
    codes = [client.post("/api/auth/login", json={"email": "ada@example.com", "password": "wrong"}).status_code
             for _ in range(12)]
    assert codes[:10] == [401] * 10 and codes[10:] == [429, 429]


def test_invite_joins_org_and_project_once(world, client):
    r = client.post(f"/api/orgs/{world.org}/invites", headers=H(world.admin),
                    json={"email": "bob@example.com", "projects": [{"project_id": world.project, "role": "viewer"}]})
    token = r.json()["token"]
    # an invite for one address does not let another person in
    assert client.post("/api/auth/register", json={"email": "eve@example.com", "password": "password1",
                                                   "invite": token}).status_code == 403
    bob, bob_id = register(client, "bob@example.com", invite=token)
    assert [o["role"] for o in client.get("/api/orgs", headers=H(bob)).json()] == ["member"]
    assert [p["role"] for p in client.get(f"/api/orgs/{world.org}/projects", headers=H(bob)).json()] == ["viewer"]
    # used up
    assert client.post("/api/auth/register", json={"email": "bob2@example.com", "password": "password1",
                                                   "invite": token}).status_code == 403
    assert client.get(f"/api/orgs/{world.org}/invites", headers=H(world.admin)).json() == []
    # members cannot invite or create organisations
    assert client.post(f"/api/orgs/{world.org}/invites", json={}, headers=H(bob)).status_code == 403
    assert client.post("/api/orgs", json={"name": "Mine"}, headers=H(bob)).status_code == 403


def test_existing_account_accepts_a_link_invite(world, client, make_client):
    other = World.__new__(World)  # a second organisation, same server
    other_org = client.post("/api/orgs", json={"name": "Second"}, headers=H(world.admin)).json()["id"]
    bob, _ = world.invite("bob@example.com")
    link = client.post(f"/api/orgs/{other_org}/invites", json={"role": "admin"}, headers=H(world.admin)).json()
    assert link["url"].endswith(link["token"])
    assert client.post("/api/invites/accept", json={"token": link["token"]}, headers=H(bob)).json()["role"] == "admin"
    assert client.post("/api/invites/accept", json={"token": link["token"]}, headers=H(bob)).status_code == 404
    assert {o["name"]: o["role"] for o in client.get("/api/orgs", headers=H(bob)).json()} == \
        {"Robotics": "member", "Second": "admin"}
    del other


def test_project_roles_gate_everything(world, client):
    viewer, viewer_id = world.invite("vi@example.com", role="viewer")
    editor, editor_id = world.invite("ed@example.com", role="editor")
    outsider, outsider_id = world.invite("out@example.com", role=None)  # in the org, not in the project
    doc = world.document()
    # a project you are not in does not exist for you
    assert client.get(f"/api/projects/{world.project}/documents", headers=H(outsider)).status_code == 404
    assert client.get(f"/api/documents/{doc}", headers=H(outsider)).status_code == 404
    assert client.get(f"/api/orgs/{world.org}/projects", headers=H(outsider)).json() == []
    # viewers read, editors write, owners delete
    assert client.get(f"/api/documents/{doc}/content", headers=H(viewer)).json()["content"]["features"][0]["id"] == "f1"
    body = {"name": "New", "content": {"features": []}}
    assert client.post(f"/api/projects/{world.project}/documents", json=body, headers=H(viewer)).status_code == 403
    assert client.post(f"/api/projects/{world.project}/documents", json=body, headers=H(editor)).status_code == 201
    assert client.patch(f"/api/documents/{doc}", json={"name": "Renamed"}, headers=H(viewer)).status_code == 403
    assert client.patch(f"/api/documents/{doc}", json={"name": "Renamed"}, headers=H(editor)).json()["name"] == "Renamed"
    assert client.delete(f"/api/documents/{doc}", headers=H(editor)).status_code == 403
    assert client.put(f"/api/projects/{world.project}/members/{viewer_id}", json={"role": "owner"},
                      headers=H(editor)).status_code == 403
    # promote the viewer; organisation admins own every project
    assert client.put(f"/api/projects/{world.project}/members/{viewer_id}", json={"role": "editor"},
                      headers=H(world.admin)).status_code == 200
    assert client.patch(f"/api/documents/{doc}", json={"name": "Again"}, headers=H(viewer)).status_code == 200
    assert client.delete(f"/api/documents/{doc}", headers=H(world.admin)).status_code == 204
    assert client.get(f"/api/documents/{doc}", headers=H(world.admin)).status_code == 404
    # someone outside the organisation cannot be added to a project
    assert client.put(f"/api/projects/{world.project}/members/nobody", json={"role": "viewer"},
                      headers=H(world.admin)).status_code == 422


def test_members_roles_and_last_owner(world, client):
    bob, bob_id = world.invite("bob@example.com")
    assert client.patch(f"/api/orgs/{world.org}/members/{bob_id}", json={"role": "admin"},
                        headers=H(world.admin)).json()["role"] == "admin"
    # an admin cannot make owners or touch them
    assert client.patch(f"/api/orgs/{world.org}/members/{bob_id}", json={"role": "owner"}, headers=H(bob)).status_code == 403
    assert client.delete(f"/api/orgs/{world.org}/members/{world.admin_id}", headers=H(bob)).status_code == 403
    # the only owner cannot step down or leave
    assert client.patch(f"/api/orgs/{world.org}/members/{world.admin_id}", json={"role": "member"},
                        headers=H(world.admin)).status_code == 409
    assert client.delete(f"/api/orgs/{world.org}/members/{world.admin_id}", headers=H(world.admin)).status_code == 409
    # removing a member takes their project access with it
    assert client.delete(f"/api/orgs/{world.org}/members/{bob_id}", headers=H(world.admin)).status_code == 204
    assert client.get("/api/orgs", headers=H(bob)).json() == []
    assert client.get(f"/api/projects/{world.project}/documents", headers=H(bob)).status_code == 404


def test_seat_hook_can_refuse(make_client):
    class TwoSeats(Hooks):
        name = "test"

        async def may_add_member(self, db, org, current_members):
            return current_members < 2, "this organisation has 2 seats"

    client = make_client(hooks=TwoSeats())
    w = World(client)
    w.invite("bob@example.com")
    r = client.post(f"/api/orgs/{w.org}/invites", json={"email": "cy@example.com"}, headers=H(w.admin))
    assert r.status_code == 402 and "2 seats" in r.json()["detail"]
    assert client.get("/api/server").json()["edition"] == "test"


def test_open_registration(make_client):
    client = make_client(open_registration=True)
    register(client, "ada@example.com")
    bob, _ = register(client, "bob@example.com")
    assert client.get("/api/me", headers=H(bob)).json()["is_server_admin"] is False
    assert client.post("/api/orgs", json={"name": "Bob's"}, headers=H(bob)).status_code == 201


# ------------------------------------------------------------------ Google / GitHub (the providers are faked)
def fake_provider(emails):
    """Stands in for Google and GitHub: the code is the person's key in `emails`."""
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/token") or url.endswith("/access_token"):
            code = parse_qs(request.content.decode())["code"][0]
            return httpx.Response(200, json={"access_token": f"at-{code}"} if code in emails else {"error": "bad"})
        who = request.headers["authorization"].split("at-")[1]
        if "googleapis.com/v1/userinfo" in url:
            return httpx.Response(200, json={"sub": f"g-{who}", "email": emails[who], "email_verified": True, "name": who})
        if url.endswith("/user/emails"):
            return httpx.Response(200, json=[{"email": "other@example.com", "primary": False, "verified": True},
                                             {"email": emails[who], "primary": True, "verified": True}])
        if url.endswith("/user"):
            return httpx.Response(200, json={"id": f"h-{who}", "login": who, "name": None})
        return httpx.Response(404)
    return lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))


VERIFIER = "v" * 43  # the secret the app keeps; the server only ever sees its hash until the exchange
CHALLENGE = hashlib.sha256(VERIFIER.encode()).hexdigest()


def exchange(client, back, verifier=VERIFIER):
    return client.post("/api/auth/exchange", json={"code": back["code"][0], "code_verifier": verifier})


def oauth(client, provider, code, invite=None, redirect="http://127.0.0.1:53111/done"):
    params = {"redirect_uri": redirect, "code_challenge": CHALLENGE}
    if invite:
        params["invite"] = invite
    r = client.get(f"/api/auth/oauth/{provider}/start", params=params)
    assert r.status_code == 302, r.text
    q = parse_qs(urlsplit(r.headers["location"]).query)
    assert q["redirect_uri"] == [f"http://testserver/api/auth/oauth/{provider}/callback"]
    r = client.get(f"/api/auth/oauth/{provider}/callback", params={"code": code, "state": q["state"][0]})
    assert r.status_code == 302 and r.headers["location"].startswith(redirect)
    return parse_qs(urlsplit(r.headers["location"]).query), q["state"][0]


def test_oauth_sign_in(make_client):
    people = {"ada": "ada@example.com", "bob": "bob@example.com", "eve": "eve@example.com"}
    client = make_client(http_client=fake_provider(people), google_client_id="gid", google_client_secret="gs",
                         github_client_id="hid", github_client_secret="hs")
    assert client.get("/api/server").json()["oauth"] == ["google", "github"]
    # first person through Google becomes the admin
    back, state = oauth(client, "google", "ada")
    ada = exchange(client, back).json()
    assert ada["user"]["email"] == "ada@example.com" and ada["user"]["is_server_admin"]
    # the code and the state work once
    assert exchange(client, back).status_code == 400
    assert client.get("/api/auth/oauth/google/callback", params={"code": "ada", "state": state}).status_code == 400
    # the same person through GitHub lands in the same account (verified address matches)
    back, _ = oauth(client, "github", "ada")
    assert exchange(client, back).json()["user"]["id"] == ada["user"]["id"]
    # a stranger needs an invite
    back, _ = oauth(client, "github", "eve")
    assert back["error"] == ["invite_required"]
    org = client.post("/api/orgs", json={"name": "Robotics"}, headers=H(ada["token"])).json()["id"]
    inv = client.post(f"/api/orgs/{org}/invites", json={"email": "bob@example.com"}, headers=H(ada["token"])).json()
    back, _ = oauth(client, "google", "bob", invite=inv["token"])
    # a code is useless to a program that did not start the sign-in, and one wrong try burns it
    assert exchange(client, back, "w" * 43).status_code == 400
    assert exchange(client, back).status_code == 400
    back, _ = oauth(client, "google", "bob")
    bob = exchange(client, back).json()
    assert [o["id"] for o in client.get("/api/orgs", headers=H(bob["token"])).json()] == [org]
    # a provider that refuses the code
    back, _ = oauth(client, "google", "nobody")
    assert back["error"] == ["provider_refused"]


def test_oauth_only_redirects_to_this_machine(make_client):
    client = make_client(google_client_id="gid", google_client_secret="gs")
    for bad in ("https://evil.example/cb", "http://evil.example:80/cb", "http://127.0.0.1/cb",
                "http://user@127.0.0.1:5000/cb", "myapp://cb"):
        assert client.get("/api/auth/oauth/google/start",
                          params={"redirect_uri": bad, "code_challenge": CHALLENGE}).status_code == 400, bad
    ok = {"redirect_uri": "http://127.0.0.1:5000/", "code_challenge": CHALLENGE}
    assert client.get("/api/auth/oauth/github/start", params=ok).status_code == 404  # not set up
    assert client.get("/api/auth/oauth/google/start", params={**ok, "code_challenge": "short"}).status_code == 400
    assert client.get("/api/auth/oauth/google/start", params={"redirect_uri": ok["redirect_uri"]}).status_code == 422


def test_oauth_does_not_join_an_account_nobody_verified(make_client):
    """Someone registers your address with their password and waits for you to arrive through Google."""
    people = {"vic": "victim@example.com", "inv": "invited@example.com"}
    client = make_client(http_client=fake_provider(people), google_client_id="gid", google_client_secret="gs",
                         open_registration=True)
    admin, _ = register(client, "ada@example.com")
    register(client, "victim@example.com", password="attacker's own")
    back, _ = oauth(client, "google", "vic")
    assert back["error"] == ["account_exists_use_password"]
    # an invite proves nothing either: whoever makes it holds the link and could register the address themselves
    org = client.post("/api/orgs", json={"name": "Robotics"}, headers=H(admin)).json()["id"]
    inv = client.post(f"/api/orgs/{org}/invites", json={"email": "invited@example.com"}, headers=H(admin)).json()
    invited, invited_id = register(client, "invited@example.com", invite=inv["token"])
    back, _ = oauth(client, "google", "inv")
    assert back["error"] == ["account_exists_use_password"]
    # the account's owner, signed in with the password, adds Google themselves
    link = {"redirect_uri": "http://127.0.0.1:53111/done", "code_challenge": CHALLENGE,
            "current_password": "correct horse"}

    def browser(token, who, **over):
        """Start a link as `token`, then the person `who` completes Google in the browser."""
        r = client.post("/api/auth/oauth/google/link", json={**link, **over}, headers=H(token))
        assert r.status_code == 200, r.text
        state = parse_qs(urlsplit(r.json()["url"]).query)["state"][0]
        r = client.get("/api/auth/oauth/google/callback", params={"code": who, "state": state})
        return parse_qs(urlsplit(r.headers["location"]).query)

    finish = lambda token, back, verifier=VERIFIER: client.post(  # noqa: E731
        "/api/auth/oauth/link/finish", json={"code": back["code"][0], "code_verifier": verifier}, headers=H(token))
    assert client.post("/api/auth/oauth/google/link", json=link).status_code == 401
    assert client.post("/api/auth/oauth/google/link", json={**link, "current_password": "wrong"},
                       headers=H(invited)).status_code == 403  # a stolen session alone cannot add a way in
    back = browser(invited, "inv")
    assert exchange(client, back).status_code == 400  # a link code never signs anyone in
    back = browser(invited, "inv")
    assert finish(admin, back).status_code == 400  # ... and only the account that started it can finish it
    back = browser(invited, "inv")
    assert finish(invited, back, "w" * 43).status_code == 400
    back = browser(invited, "inv")
    assert finish(invited, back).json()["linked"] == "google"
    assert client.get("/api/me/providers", headers=H(invited)).json() == {"providers": ["google"], "has_password": True}
    back, _ = oauth(client, "google", "inv")  # from now on Google signs them in
    assert exchange(client, back).json()["user"]["id"] == invited_id

    # The trick this guards against: Mallory starts a link on HER account and gets the victim to open the
    # address. The victim completes Google in their browser - and nothing is attached to Mallory's account.
    mallory, mallory_id = register(client, "mallory@example.com")
    back = browser(mallory, "vic")
    assert client.get("/api/me/providers", headers=H(mallory)).json()["providers"] == []
    back2, _ = oauth(client, "google", "vic")  # the victim's own Google sign-in is still not Mallory's account
    assert back2["error"] == ["account_exists_use_password"]
    # a provider identity that already belongs to one account cannot be moved to another
    assert browser(admin, "inv") == {"error": ["identity_in_use"]}
    # unlinking needs the password; the last way in cannot be removed
    assert client.post("/api/me/providers/google/unlink", json={}, headers=H(invited)).status_code == 403
    assert client.post("/api/me/providers/google/unlink", json={"current_password": "correct horse"},
                       headers=H(invited)).status_code == 204
    assert client.get("/api/me/providers", headers=H(invited)).json()["providers"] == []
    del mallory_id


def test_an_account_with_only_a_provider_cannot_unlink_it(make_client):
    client = make_client(http_client=fake_provider({"ada": "ada@example.com"}), google_client_id="g",
                         google_client_secret="s")
    back, _ = oauth(client, "google", "ada")
    token = exchange(client, back).json()["token"]
    assert client.get("/api/me/providers", headers=H(token)).json() == {"providers": ["google"], "has_password": False}
    assert client.post("/api/me/providers/google/unlink", json={}, headers=H(token)).status_code == 409
    # with no password to ask for, changing sign-in methods needs a recent sign-in instead
    link = {"redirect_uri": "http://127.0.0.1:53111/done", "code_challenge": CHALLENGE}
    assert client.post("/api/auth/oauth/google/link", json=link, headers=H(token)).status_code == 200

    async def age_the_session():
        from sqlalchemy import update
        from openswx_collab_server.db import Session
        async with client.app.state.db() as s:
            await s.execute(update(Session).values(created_at=Session.created_at - 3600))
            await s.commit()
    client.portal.call(age_the_session)
    r = client.post("/api/auth/oauth/google/link", json=link, headers=H(token))
    assert r.status_code == 403 and "sign in again" in r.json()["detail"]
    # ... and so does setting a first password: it would be a way round the rule above
    assert client.patch("/api/me", json={"password": "attacker's own"}, headers=H(token)).status_code == 403
    assert client.post("/api/auth/login", json={"email": "ada@example.com", "password": "attacker's own"}).status_code == 401
    back, _ = oauth(client, "google", "ada")  # a fresh sign-in may set one
    fresh = exchange(client, back).json()["token"]
    assert client.patch("/api/me", json={"password": "my own password"}, headers=H(fresh)).status_code == 200
