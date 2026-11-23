"""Where snapshots and blobs (imported bodies) are kept: a folder, or an S3-compatible bucket."""
from __future__ import annotations

import asyncio
import os
import shutil
import re
import tempfile
from pathlib import Path

_SAFE = re.compile(r"^[A-Za-z0-9._\-/]+$")


def _check(key: str) -> str:
    if not _SAFE.match(key) or ".." in key or key.startswith("/"):
        raise ValueError(f"bad storage key {key!r}")
    return key


class LocalStorage:
    def __init__(self, root: str):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / _check(key)

    def _put(self, key: str, data: bytes) -> None:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, p)  # (whole file or nothing: a crash never leaves half a snapshot)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def _get(self, key: str) -> bytes | None:
        p = self._path(key)
        return p.read_bytes() if p.is_file() else None

    async def put(self, key: str, data: bytes) -> None:
        await asyncio.to_thread(self._put, key, data)

    async def get(self, key: str) -> bytes | None:
        return await asyncio.to_thread(self._get, key)

    async def exists(self, key: str) -> bool:
        return await asyncio.to_thread(lambda: self._path(key).is_file())

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(lambda: self._path(key).unlink(missing_ok=True))

    async def delete_prefix(self, prefix: str) -> None:
        """Remove everything under a folder-like prefix ("orgs/<id>/")."""
        path = self._path(prefix.rstrip("/"))
        await asyncio.to_thread(lambda: shutil.rmtree(path) if path.is_dir() else None)


class S3Storage:
    """S3 or anything that speaks its API (MinIO, R2). Needs the `s3` extra (boto3)."""

    def __init__(self, url: str, endpoint: str = ""):
        import boto3  # noqa: PLC0415 - optional dependency
        rest = url[len("s3://"):]
        self.bucket, _, prefix = rest.partition("/")
        self.prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        self.s3 = boto3.client("s3", endpoint_url=endpoint or None)

    def _key(self, key: str) -> str:
        return self.prefix + _check(key)

    async def put(self, key: str, data: bytes) -> None:
        await asyncio.to_thread(self.s3.put_object, Bucket=self.bucket, Key=self._key(key), Body=data)

    def _get(self, key: str) -> bytes | None:
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=self._key(key))["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            return None

    async def get(self, key: str) -> bytes | None:
        return await asyncio.to_thread(self._get, key)

    def _exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError  # noqa: PLC0415
        try:
            self.s3.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except ClientError:
            return False

    async def exists(self, key: str) -> bool:
        return await asyncio.to_thread(self._exists, key)

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(self.s3.delete_object, Bucket=self.bucket, Key=self._key(key))

    def _delete_prefix(self, prefix: str) -> None:
        pages = self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self._key(prefix))
        for page in pages:
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            for n in range(0, len(keys), 1000):
                self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys[n:n + 1000], "Quiet": True})

    async def delete_prefix(self, prefix: str) -> None:
        """Remove every object whose key starts with this prefix ("orgs/<id>/")."""
        await asyncio.to_thread(self._delete_prefix, prefix)


def make_storage(url: str, s3_endpoint: str = ""):
    return S3Storage(url, s3_endpoint) if url.startswith("s3://") else LocalStorage(url)


def blob_key(org_id: str, sha256: str) -> str:
    return f"orgs/{org_id}/blobs/{sha256[:2]}/{sha256}"


def snapshot_key(document_id: str, seq: int) -> str:
    return f"docs/{document_id}/snapshots/{seq:010d}.json.gz"
