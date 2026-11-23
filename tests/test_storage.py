"""Where files are kept: a folder, or an S3-compatible bucket (a fake S3 from the moto package stands in)."""
import asyncio
import hashlib

import pytest

from openswx_collab_server.storage import LocalStorage, S3Storage, blob_key, make_storage, snapshot_key

from conftest import H, Live, World, part, set_depth

moto = pytest.importorskip("moto", reason="pip install 'moto[s3]' boto3 to test S3 storage")


@pytest.fixture
def s3(monkeypatch):
    import boto3
    for k, v in (("AWS_ACCESS_KEY_ID", "test"), ("AWS_SECRET_ACCESS_KEY", "test"), ("AWS_DEFAULT_REGION", "us-east-1")):
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket="collab")
        yield client


def roundtrip(store):
    async def go():
        key = blob_key("org1", "ab" * 32)
        assert await store.get(key) is None and not await store.exists(key)
        await store.put(key, b"hello")
        await store.put(key, b"hello again")  # (overwrite)
        assert await store.exists(key) and await store.get(key) == b"hello again"
        await store.delete(key)
        await store.delete(key)  # (deleting what is not there is fine)
        assert not await store.exists(key) and await store.get(key) is None
    asyncio.run(go())


def test_local_folder(tmp_path):
    store = make_storage(str(tmp_path / "files"))
    assert isinstance(store, LocalStorage)
    roundtrip(store)
    assert not list((tmp_path / "files").rglob(".tmp-*"))  # no half-written files left behind
    for bad in ("../outside", "/abs", "a/../../b", "sp ace", ""):
        with pytest.raises(ValueError):
            asyncio.run(store.put(bad, b"x"))


def test_s3_bucket_with_and_without_a_prefix(s3):
    roundtrip(make_storage("s3://collab"))
    store = make_storage("s3://collab/team/files/")
    assert isinstance(store, S3Storage)
    asyncio.run(store.put(snapshot_key("doc1", 7), b"snap"))
    assert [o["Key"] for o in s3.list_objects_v2(Bucket="collab")["Contents"]] == \
        ["team/files/docs/doc1/snapshots/0000000007.json.gz"]
    roundtrip(store)
    with pytest.raises(ValueError):
        asyncio.run(store.put("../escape", b"x"))


def test_the_whole_server_on_s3(s3, make_client):
    """Documents, snapshots, blobs and a restart, with every file in the bucket."""
    client = make_client(storage_url="s3://collab/prod", snapshot_every=3)
    w = World(client)
    body = ("C" * 30000).encode()
    sha = hashlib.sha256(body).hexdigest()
    assert client.post(f"/api/projects/{w.project}/blobs/missing", json=[sha], headers=H(w.admin)).json() == [sha]
    assert client.put(f"/api/projects/{w.project}/blobs/{sha}", content=body, headers=H(w.admin)).status_code == 204
    assert client.get(f"/api/projects/{w.project}/blobs/{sha}", headers=H(w.admin)).content == body
    imported = part()
    imported["features"].append({"id": "f3", "name": "Imported1", "type": "import", "suppressed": False,
                                 "params": {"brep": {"$blob": sha, "len": len(body)}}})
    doc = w.document(imported)
    a = Live(client, doc, w.admin)
    for n in range(1, 8):
        assert a.change(set_depth(n), n - 1)["seq"] == n
    a.close()
    client.__exit__(None, None, None)
    keys = sorted(o["Key"] for o in s3.list_objects_v2(Bucket="collab")["Contents"])
    assert keys == [f"prod/docs/{doc}/snapshots/{n:010d}.json.gz" for n in (0, 3, 6, 7)] + \
        [f"prod/orgs/{w.org}/blobs/{sha[:2]}/{sha}"]
    again = make_client()
    for seq, depth in ((0, 10), (2, 2), (5, 5), (7, 7)):
        got = again.get(f"/api/documents/{doc}/content", params={"seq": seq}, headers=H(w.admin)).json()["content"]
        assert got["features"][1]["params"]["depth"] == depth and got["features"][2]["params"]["brep"]["$blob"] == sha


def prefix_removal(store):
    async def go():
        for key in ("orgs/a/blobs/1", "orgs/a/blobs/sub/2", "orgs/ab/blobs/3", "orgs/b/x"):
            await store.put(key, b"x")
        await store.delete_prefix("orgs/a/")
        await store.delete_prefix("orgs/nothing-here/")  # (nothing to remove is fine)
        return [await store.exists(k) for k in ("orgs/a/blobs/1", "orgs/a/blobs/sub/2", "orgs/ab/blobs/3", "orgs/b/x")]
    assert asyncio.run(go()) == [False, False, True, True]


def test_removing_everything_under_a_prefix(tmp_path, s3):
    prefix_removal(make_storage(str(tmp_path / "files")))
    prefix_removal(make_storage("s3://collab/team"))
