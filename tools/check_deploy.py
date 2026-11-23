"""Check the deployment files as far as that is possible without Docker.

    .venv/Scripts/python -m pip install pixeltable-pgserver asyncpg "moto[server]" boto3 pyyaml websockets
    .venv/Scripts/python tools/check_deploy.py

1. Reads deploy/Dockerfile and the Compose files: every OSWX_COLLAB_* variable they set must be a setting the
   server has, and the command the image runs must exist.
2. Installs the server into a fresh virtual environment with the same pip command as the Dockerfile.
3. Starts that installed server with the environment the Compose files give it (PostgreSQL through asyncpg,
   files in an S3 bucket behind a custom endpoint), on 0.0.0.0, and runs the image's health check against it.
4. Signs up, shares a document, edits it over the WebSocket, restarts the server and checks the rows are
   in PostgreSQL and the files in the bucket.

Not covered (needs Docker): building the image, the non-root user's access to /data, and Compose start-up
order.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import venv
from dataclasses import fields
from pathlib import Path

import boto3
import httpx
import pixeltable_pgserver as pgserver
import yaml
from moto.server import ThreadedMotoServer
from websockets.sync.client import connect

from openswx_collab_server.config import PREFIX, Settings

ROOT = Path(__file__).resolve().parent.parent
failures: list[str] = []


def check(ok: bool, what: str) -> None:
    print(("ok   " if ok else "FAIL ") + what, flush=True)
    if not ok:
        failures.append(what)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def static_checks() -> tuple[str, dict]:
    known = {PREFIX + f.name.upper() for f in fields(Settings)}
    docker = (ROOT / "deploy/Dockerfile").read_text(encoding="utf-8")
    base = yaml.safe_load((ROOT / "deploy/docker-compose.yml").read_text(encoding="utf-8"))
    s3 = yaml.safe_load((ROOT / "deploy/docker-compose.s3.yml").read_text(encoding="utf-8"))
    env = {**base["services"]["server"]["environment"], **s3["services"]["server"]["environment"]}
    used = {k for k in env if k.startswith(PREFIX)} | set(re.findall(r"(OSWX_COLLAB_[A-Z0-9_]+)=", docker))
    check(used <= known, f"every OSWX_COLLAB_ variable in the Dockerfile and Compose files is a real setting "
                         f"({len(used)} used; unknown: {sorted(used - known) or 'none'})")
    example = (ROOT / "deploy/.env.example").read_text(encoding="utf-8")
    wanted = set(re.findall(r"\$\{([A-Z0-9_]+)", json.dumps(base) + json.dumps(s3)))
    offered = set(re.findall(r"^([A-Z0-9_]+)=", example, re.M))
    check(wanted <= offered, f".env.example offers every variable the Compose files read "
                             f"(missing: {sorted(wanted - offered) or 'none'})")
    build = yaml.safe_load((ROOT / "deploy/docker-compose.build.yml").read_text(encoding="utf-8"))
    check(build["services"]["server"]["build"] == {"context": "..", "dockerfile": "deploy/Dockerfile"}
          and all((ROOT / p).is_dir() for p in re.findall(r"^COPY (\S+) ", docker, re.M)),
          "the build context and every COPY source exist")
    check(base["services"]["server"]["image"].startswith("ghcr.io/dukeofcheese/openswx-collab-server:"),
          "the Compose file runs the published image")
    check("volumes" in base and set(base["volumes"]) == {"storage", "db"} and {"garage-meta", "garage-data"} <= set(s3["volumes"]),
          "the named volumes are declared")
    pip = re.search(r"^RUN pip install (.+)$", docker, re.M).group(1)
    cmd = json.loads(re.search(r"^CMD (\[.+\])$", docker, re.M).group(1))
    scripts = (ROOT / "server/pyproject.toml").read_text(encoding="utf-8")
    check(f'{cmd[0]} = "' in scripts, f"the image's command {cmd[0]!r} is a console script of the server package")
    health = re.search(r'CMD python -c "(.+)"$', docker, re.M).group(1)
    return pip, {"cmd": cmd, "health": health, "env": env}


def install_like_the_image(pip_args: str, work: Path) -> Path:
    env_dir = work / "image-venv"
    venv.EnvBuilder(with_pip=True).create(env_dir)
    py = env_dir / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    args = [a.strip('"').replace("/app/", str(ROOT).replace("\\", "/") + "/") for a in pip_args.split()]
    r = subprocess.run([str(py), "-m", "pip", "install", "--disable-pip-version-check", "-q", *args],
                       capture_output=True, text=True)
    check(r.returncode == 0, f"`pip install {pip_args}` works in a fresh environment")
    if r.returncode:
        print(r.stderr[-2000:])
    return py


def run_like_compose(py: Path, image: dict, work: Path) -> None:
    pg = pgserver.get_server(str(work / "pg"))
    s3_port, port = free_port(), free_port()
    moto = ThreadedMotoServer(port=s3_port, verbose=False)
    moto.start()
    creds = {"AWS_ACCESS_KEY_ID": "minio-user", "AWS_SECRET_ACCESS_KEY": "minio-secret", "AWS_DEFAULT_REGION": "us-east-1"}
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{s3_port}", aws_access_key_id=creds["AWS_ACCESS_KEY_ID"],
                      aws_secret_access_key=creds["AWS_SECRET_ACCESS_KEY"], region_name="us-east-1")
    s3.create_bucket(Bucket="openswx-collab")
    base = f"http://127.0.0.1:{port}"
    # the values the Compose files produce, with this machine's addresses in place of the service names
    env = dict(os.environ, **creds,
               OSWX_COLLAB_DATABASE_URL=pg.get_uri().replace("postgresql://", "postgresql+asyncpg://", 1),
               OSWX_COLLAB_STORAGE_URL="s3://openswx-collab", OSWX_COLLAB_S3_ENDPOINT=f"http://127.0.0.1:{s3_port}",
               OSWX_COLLAB_PUBLIC_URL=base, OSWX_COLLAB_OPEN_REGISTRATION="false",
               OSWX_COLLAB_GOOGLE_CLIENT_ID="", OSWX_COLLAB_GOOGLE_CLIENT_SECRET="",
               OSWX_COLLAB_GITHUB_CLIENT_ID="", OSWX_COLLAB_GITHUB_CLIENT_SECRET="")
    env.pop("PYTHONPATH", None)
    exe = py.parent / (image["cmd"][0] + (".exe" if os.name == "nt" else ""))
    cmd = [str(exe), *image["cmd"][1:]]
    cmd[cmd.index("--port") + 1] = str(port)
    log = open(work / "server.log", "w")

    def start() -> subprocess.Popen:
        p = subprocess.Popen(cmd, env=env, stdout=log, stderr=log, cwd=str(work))
        for _ in range(200):
            try:
                if httpx.get(f"{base}/api/health", timeout=2).status_code == 200:
                    return p
            except httpx.HTTPError:
                time.sleep(0.1)
        p.kill()
        raise RuntimeError(f"the server did not start: {(work / 'server.log').read_text()[-2000:]}")

    server = start()
    try:
        check(True, f"the installed server starts with the image's command ({' '.join(image['cmd'])}) on PostgreSQL + S3")
        r = subprocess.run([str(py), "-c", image["health"].replace("8787", str(port))], capture_output=True)
        check(r.returncode == 0, "the image's HEALTHCHECK command reports healthy")
        http = httpx.Client(base_url=base, timeout=30)
        info = http.get("/api/server").json()
        check(info["registration"] == "first_user" and info["oauth"] == [], "empty provider settings mean no provider is offered")
        tok = http.post("/api/auth/register", json={"email": "ada@example.com", "password": "correct horse", "name": "Ada"}).json()["token"]
        H = {"Authorization": f"Bearer {tok}"}
        org = http.post("/api/orgs", json={"name": "Robotics"}, headers=H).json()["id"]
        project = http.post(f"/api/orgs/{org}/projects", json={"name": "Rover"}, headers=H).json()["id"]
        content = {"version": 3, "features": [{"id": "f1", "name": "Boss-Extrude1", "type": "extrude",
                                               "params": {"depth": 10.0}, "suppressed": False}]}
        doc = http.post(f"/api/projects/{project}/documents", json={"name": "Part", "content": content}, headers=H).json()["id"]
        with connect(f"ws://127.0.0.1:{port}/api/documents/{doc}/ws") as ws:
            ws.send(json.dumps({"type": "hello", "token": tok, "client_id": "deploy-check", "format_version": 3}))
            welcome = json.loads(ws.recv(timeout=20))
            ws.send(json.dumps({"type": "change", "change_id": "c1", "base_seq": 0, "command": "edit_feature", "ops": [
                {"op": "feature.update", "id": "f1", "set": [[["params", "depth"], 20.5]], "unset": []}]}))
            answer = json.loads(ws.recv(timeout=20))
        check(welcome["type"] == "welcome" and answer == {"type": "change.accepted", "change_id": "c1", "seq": 1},
              "a document is shared and edited over the WebSocket")
        server.terminate()
        server.wait(timeout=30)
        server = start()  # "docker compose restart": everything must come back from PostgreSQL and the bucket
        got = http.get(f"/api/documents/{doc}/content", headers=H).json()
        check(got["seq"] == 1 and got["content"]["features"][0]["params"]["depth"] == 20.5,
              "after a restart the edit is still there (history in PostgreSQL)")
        first = http.get(f"/api/documents/{doc}/content", params={"seq": 0}, headers=H).json()["content"]
        check(first["features"][0]["params"]["depth"] == 10.0 and isinstance(first["features"][0]["params"]["depth"], float),
              "state 0 comes back from the bucket unchanged (10.0 is still a float)")
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket="openswx-collab").get("Contents", [])]
        check(any(k.startswith(f"docs/{doc}/snapshots/") for k in keys), f"snapshots are in the bucket ({len(keys)} objects)")

        async def rows() -> tuple[int, int]:
            import asyncpg
            c = await asyncpg.connect(pg.get_uri())
            try:
                return await c.fetchval("select count(*) from ops"), await c.fetchval("select count(*) from users")
            finally:
                await c.close()
        check(asyncio.run(rows()) == (1, 1), "the change and the account are rows in PostgreSQL")
        http.close()
    finally:
        server.terminate()
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            server.kill()
        log.close()
        moto.stop()
        pg.cleanup()


def main() -> int:
    work = Path(tempfile.mkdtemp(prefix="oswx-collab-deploy-"))
    try:
        pip_args, image = static_checks()
        py = install_like_the_image(pip_args, work)
        if not failures:
            run_like_compose(py, image, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print("\nRESULT:", "PASS" if not failures else f"FAIL ({len(failures)}): {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
