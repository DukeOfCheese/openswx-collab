"""Check the Rust server's deployment files: deploy/Dockerfile.rust and deploy/docker-compose.rust*.yml.

    python tools/check_deploy_rust.py              # the files themselves (needs pyyaml)
    python tools/check_deploy_rust.py --docker     # and build, start and use each Compose variant
                                                   # (needs Docker, and pyyaml + websockets)

The successor of tools/check_deploy.py for the Rust server (that one checks the Python server's image and
Compose file, which stay until the Python server is retired).

Without Docker it reads the files: every OSWX_COLLAB_* variable they set is a setting the Rust server reads
(crates/server/src/config.rs, crates/serverd/src/lib.rs) and none is a retired one; deploy/.env.example
offers every variable the Compose files read; the build context and every COPY source exist; the image's
command is the server's binary; the image runs as an unprivileged user and checks /api/health; the server
is published on 127.0.0.1 only; the Compose network has a fixed private subnet that the server trusts as
its proxy (OSWX_COLLAB_TRUSTED_PROXIES); the volumes are declared.

With --docker it then builds the image and, for each variant (SQLite; PostgreSQL; PostgreSQL + Garage),
starts the stack on a free port of 127.0.0.1 under a Compose project of its own, and checks: the
container becomes healthy; /api/health and /api/server answer; an account, an organisation, a project and
a document are made and the document is edited over the WebSocket (protocol 2); the server sees the TLS
proxy's connections as coming from the Compose network's gateway, believes X-Forwarded-For from it, and
keeps its sign-in limit per forwarded address; after a restart the edit is still there and state 0 comes
back from storage; on PostgreSQL the change is a row there. Each stack is removed afterwards with its
volumes (`down -v`); the built image is kept unless --remove-image is given.
"""
from __future__ import annotations

import argparse
import ipaddress
import json
import re
import secrets
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
DEPLOY = ROOT / "deploy"
PREFIX = "OSWX_COLLAB_"
BASE = "deploy/docker-compose.rust.yml"
POSTGRES = "deploy/docker-compose.rust-postgres.yml"
S3 = "deploy/docker-compose.s3.yml"
IMAGE = "openswx-collab-server-rust:local"
failures: list[str] = []


def check(ok: bool, what: str) -> None:
    print(("ok   " if ok else "FAIL ") + what, flush=True)
    if not ok:
        failures.append(what)


def rust_settings() -> tuple[set[str], set[str]]:
    """The OSWX_COLLAB_* names the Rust server reads, and the retired ones it reads only to refuse to
    start while they are set (their error says "unset: ...")."""
    config = (ROOT / "crates/server/src/config.rs").read_text(encoding="utf-8")
    read = set(re.findall(r'e\.\w+\(\s*"([A-Z0-9_]+)"', config))
    retired = set(re.findall(r'ConfigError::new\(\s*"([A-Z0-9_]+)",\s*"unset\b', config))
    serverd = (ROOT / "crates/serverd/src/lib.rs").read_text(encoding="utf-8")
    program = set(re.findall(r'const \w+: &str = "OSWX_COLLAB_([A-Z0-9_]+)";', serverd))
    return {PREFIX + n for n in (read | program) - retired}, {PREFIX + n for n in retired}


def load(path: str) -> dict:
    return yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))


def static_checks() -> None:
    known, retired = rust_settings()
    docker = (DEPLOY / "Dockerfile.rust").read_text(encoding="utf-8")
    base, postgres, s3 = load(BASE), load(POSTGRES), load(S3)
    server = base["services"]["server"]
    env = {**server["environment"], **postgres["services"]["server"]["environment"],
           **s3["services"]["server"]["environment"]}
    used = {k for k in env if k.startswith(PREFIX)} | set(re.findall(r"(OSWX_COLLAB_[A-Z0-9_]+)=", docker))
    check(used <= known and not used & retired,
          f"every OSWX_COLLAB_ variable in Dockerfile.rust and the Rust Compose files is a setting of the "
          f"Rust server ({len(used)} used; unknown: {sorted(used - known) or 'none'}; retired: "
          f"{sorted(used & retired) or 'none'})")
    aws = {k for k in env if k.startswith("AWS_")}
    check(aws <= {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_DEFAULT_REGION",
                  "AWS_REGION"}, f"the AWS_ variables are ones the server reads ({sorted(aws)})")

    example = (DEPLOY / ".env.example").read_text(encoding="utf-8")
    wanted = set(re.findall(r"\$\{([A-Z0-9_]+)", json.dumps(base) + json.dumps(postgres) + json.dumps(s3)))
    offered = set(re.findall(r"^([A-Z0-9_]+)=", example, re.M))
    check(wanted <= offered, f".env.example offers every variable the Rust Compose files read "
                             f"(missing: {sorted(wanted - offered) or 'none'})")

    build = server.get("build", {})
    copies = re.findall(r"^COPY (?!--from)(.+) \S+$", docker, re.M)
    sources = [s for line in copies for s in line.split()]
    check(build.get("context") == ".." and build.get("dockerfile") == "deploy/Dockerfile.rust"
          and all((ROOT / s).exists() for s in sources) and "rust-toolchain.toml" not in sources,
          f"the build context and every COPY source exist, without rust-toolchain.toml ({sources})")
    check(server.get("image") == IMAGE, f"the Compose file names the image it builds ({server.get('image')})")

    cargo = (ROOT / "crates/serverd/Cargo.toml").read_text(encoding="utf-8")
    binary = re.search(r'\[\[bin\]\]\s*name = "([^"]+)"', cargo).group(1)
    cmd = json.loads(re.search(r"^CMD (\[.+\])$", docker, re.M).group(1))
    check(cmd[:2] == [binary, "serve"] and cmd[cmd.index("--host") + 1] == "0.0.0.0"
          and f"/target/release/{binary}" in docker,
          f"the image runs the server's binary {binary!r} with `serve --host 0.0.0.0` ({cmd})")
    user = re.findall(r"^USER (\S+)$", docker, re.M)
    check(bool(user) and user[-1] not in ("root", "0"), f"the image runs as an unprivileged user ({user})")
    joined = re.sub(r"\\\n\s*", " ", docker)  # continuation lines as one
    check(bool(re.search(r"^HEALTHCHECK .*/api/health", joined, re.M)), "the image checks /api/health")

    ports = server.get("ports", [])
    check(len(ports) == 1 and str(ports[0]).startswith("127.0.0.1:") and str(ports[0]).endswith(":8787"),
          f"the server is published on 127.0.0.1 only ({ports})")
    grace = str(server.get("stop_grace_period", "10s"))
    check(grace.endswith("s") and float(grace[:-1]) >= 10,
          f"docker stop waits at least 10 s, more than the server's 8 s stop ({grace})")

    # The host's TLS proxy reaches the server from the Compose network's gateway: the network has a fixed
    # subnet, and the server trusts exactly that subnet's X-Forwarded-For.
    subnets = [c.get("subnet") for c in base.get("networks", {}).get("default", {}).get("ipam", {}).get("config", [])]
    trusted = server["environment"].get("OSWX_COLLAB_TRUSTED_PROXIES")
    default = re.fullmatch(r"\$\{NETWORK_SUBNET:-([^}]+)\}", trusted or "")
    try:
        net = ipaddress.ip_network(default.group(1)) if default else None
    except ValueError:
        net = None
    check(len(subnets) == 1 and subnets[0] == trusted and net is not None and net.version == 4
          and 8 <= net.prefixlen and net.is_private,
          f"the Compose network has a fixed private subnet and the server trusts it as its proxy "
          f"(subnet {subnets}, OSWX_COLLAB_TRUSTED_PROXIES {trusted!r})")

    db = postgres["services"]["server"]["environment"].get("OSWX_COLLAB_DATABASE_URL", "")
    check(db.startswith("postgres://") and "healthcheck" in postgres["services"]["db"]
          and postgres["services"]["server"]["depends_on"]["db"]["condition"] == "service_healthy",
          "the PostgreSQL file points the server at its database and waits for it to be healthy")
    check(set(base.get("volumes", {})) == {"rust-data"} and set(postgres.get("volumes", {})) == {"rust-db"}
          and any(str(v).startswith("rust-data:/data") for v in server.get("volumes", [])),
          "the named volumes are declared, apart from the Python stack's")
    check(base.get("name") == "openswx-collab-rust", f"the Compose project has its own name ({base.get('name')})")


# ------------------------------------------------------------------------------------------- docker

def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def pack(v) -> bytes:
    """MessagePack, enough for the frames a client sends here."""
    if v is None:
        return b"\xc0"
    if v is True or v is False:
        return b"\xc3" if v else b"\xc2"
    if isinstance(v, int):
        if 0 <= v < 128:
            return bytes([v])
        return b"\xd3" + struct.pack(">q", v)
    if isinstance(v, float):
        return b"\xcb" + struct.pack(">d", v)
    if isinstance(v, str):
        b = v.encode()
        return (bytes([0xA0 | len(b)]) if len(b) < 32 else b"\xdb" + struct.pack(">I", len(b))) + b
    if isinstance(v, list):
        return b"\xdd" + struct.pack(">I", len(v)) + b"".join(pack(x) for x in v)
    if isinstance(v, dict):
        return b"\xdf" + struct.pack(">I", len(v)) + b"".join(pack(k) + pack(x) for k, x in v.items())
    raise TypeError(type(v))


def unpack(b: bytes):
    """MessagePack as the server writes it (no extension types)."""
    def at(i: int):
        m = b[i]
        if m <= 0x7F:
            return m, i + 1
        if m >= 0xE0:
            return m - 256, i + 1
        if 0x80 <= m <= 0x8F:
            return mapping(m & 0x0F, i + 1)
        if 0x90 <= m <= 0x9F:
            return array(m & 0x0F, i + 1)
        if 0xA0 <= m <= 0xBF:
            n = m & 0x1F
            return b[i + 1:i + 1 + n].decode(), i + 1 + n
        simple = {0xC0: None, 0xC2: False, 0xC3: True}
        if m in simple:
            return simple[m], i + 1
        sized = {0xCC: ">B", 0xCD: ">H", 0xCE: ">I", 0xCF: ">Q", 0xD0: ">b", 0xD1: ">h", 0xD2: ">i",
                 0xD3: ">q", 0xCA: ">f", 0xCB: ">d"}
        if m in sized:
            n = struct.calcsize(sized[m])
            return struct.unpack(sized[m], b[i + 1:i + 1 + n])[0], i + 1 + n
        lengths = {0xD9: (">B", "s"), 0xDA: (">H", "s"), 0xDB: (">I", "s"), 0xC4: (">B", "b"),
                   0xC5: (">H", "b"), 0xC6: (">I", "b"), 0xDC: (">H", "a"), 0xDD: (">I", "a"),
                   0xDE: (">H", "m"), 0xDF: (">I", "m")}
        fmt, kind = lengths[m]
        w = struct.calcsize(fmt)
        n = struct.unpack(fmt, b[i + 1:i + 1 + w])[0]
        j = i + 1 + w
        if kind == "a":
            return array(n, j)
        if kind == "m":
            return mapping(n, j)
        raw = b[j:j + n]
        return (raw.decode() if kind == "s" else raw), j + n

    def array(n: int, i: int):
        out = []
        for _ in range(n):
            v, i = at(i)
            out.append(v)
        return out, i

    def mapping(n: int, i: int):
        out = {}
        for _ in range(n):
            k, i = at(i)
            v, i = at(i)
            out[k] = v
        return out, i

    return at(0)[0]


def http(base: str, method: str, path: str, body=None, token: str | None = None,
         headers: dict | None = None) -> tuple[int, object]:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            return r.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, raw


def compose(project: str, files: list[str], env_file: Path, *args: str, capture: bool = False):
    cmd = ["docker", "compose", "-p", project]
    for f in files:
        cmd += ["-f", str(ROOT / f)]
    cmd += ["--env-file", str(env_file), *args]
    return subprocess.run(cmd, cwd=ROOT, text=True, capture_output=capture, check=False)


def wait_healthy(base: str, project: str, files: list[str], env_file: Path) -> bool:
    deadline = time.time() + 300
    while time.time() < deadline:
        r = compose(project, files, env_file, "ps", "--format", "json", "server", capture=True)
        out = (r.stdout or "").strip()
        # (One object a line in newer Compose versions, a list in older ones.)
        state = json.loads(out.splitlines()[0]) if out else {}
        if isinstance(state, list):
            state = state[0] if state else {}
        if state.get("Health") == "healthy":
            return True
        time.sleep(3)
    return False


def gateway_of(project: str) -> str:
    """The Compose network's gateway: the address the host's side of the bridge has, from which a proxy
    on the host reaches the published port. Docker reports it only when it was given; otherwise it is
    the subnet's first address, which on Linux the bridge interface confirms."""
    out = subprocess.run(["docker", "network", "inspect", f"{project}_default", "--format",
                          '{"id": {{json .Id}}, "config": {{json .IPAM.Config}}}'],
                         text=True, capture_output=True).stdout
    try:
        network = json.loads(out)
        config = network["config"][0]
    except (ValueError, KeyError, IndexError, TypeError):
        return ""
    if config.get("Gateway"):
        return config["Gateway"]
    gateway = str(next(ipaddress.ip_network(config["Subnet"]).hosts()))
    bridge = subprocess.run(["ip", "-o", "-4", "addr", "show", f"br-{network['id'][:12]}"], text=True,
                            capture_output=True).stdout if sys.platform.startswith("linux") else ""
    if bridge and f" {gateway}/" not in bridge:
        return ""
    return gateway


def use_the_server(base: str, port: int, project: str, files: list[str], env_file: Path,
                   postgres: bool) -> None:
    from websockets.sync.client import connect

    status, _ = http(base, "GET", "/api/health")
    check(status == 200, "GET /api/health answers 200 through the published port")
    _, info = http(base, "GET", "/api/server")
    check(isinstance(info, dict) and info.get("protocol") == 2 and info.get("registration") == "first_user"
          and info.get("oauth") == [], f"GET /api/server: protocol 2, first user, no provider ({info})")
    status, reg = http(base, "POST", "/api/auth/register",
                       {"email": "ada@example.com", "password": "correct horse battery", "name": "Ada"})
    token = reg["token"]
    _, org = http(base, "POST", "/api/orgs", {"name": "Robotics"}, token)
    _, project_ = http(base, "POST", f"/api/orgs/{org['id']}/projects", {"name": "Rover"}, token)
    content = {"version": 3, "features": [{"id": "f1", "name": "Boss-Extrude1", "type": "extrude",
                                           "params": {"depth": 10.0}, "suppressed": False}]}
    status, doc = http(base, "POST", f"/api/projects/{project_['id']}/documents",
                       {"name": "Part", "content": content}, token)
    check(status == 201, f"a document is created ({status})")
    with connect(f"ws://127.0.0.1:{port}/api/documents/{doc['id']}/ws", open_timeout=20) as ws:
        ws.send(pack({"t": "hello", "protocol": 2, "token": token, "client_id": "deploy-check",
                      "format_version": 3}))
        welcome = unpack(ws.recv(timeout=20))
        ws.send(pack({"t": "change", "change_id": "c1", "base_seq": 0, "command": "edit_feature",
                      "ops": [{"op": "set", "path": ["features", "f1", "params", "depth"], "value": 20.5}]}))
        answer = None
        for _ in range(20):
            msg = unpack(ws.recv(timeout=20))
            if msg.get("t") in ("change.accepted", "change.rejected"):
                answer = msg
                break
    check(welcome.get("t") == "welcome" and answer == {"t": "change.accepted", "change_id": "c1", "seq": 1},
          f"the document is edited over the WebSocket, protocol 2 ({welcome.get('t')}, {answer})")

    # The proxy's address: what a TLS proxy on the host would be. Ten wrong passwords lock one address
    # out for that email address.
    gateway = gateway_of(project)
    check(bool(gateway), f"the Compose network's gateway is known ({gateway!r})")
    if not gateway:
        return
    def attempts(email: str, forwarded: str | None) -> list[int]:
        h = {"X-Forwarded-For": forwarded} if forwarded else {}
        return [http(base, "POST", "/api/auth/login", {"email": email, "password": "wrong password!"},
                     headers=h)[0] for _ in range(11)]
    plain = attempts("bob@example.com", None)
    as_gateway = http(base, "POST", "/api/auth/login", {"email": "bob@example.com", "password": "wrong password!"},
                      headers={"X-Forwarded-For": gateway})[0]
    other = http(base, "POST", "/api/auth/login", {"email": "bob@example.com", "password": "wrong password!"},
                 headers={"X-Forwarded-For": "203.0.113.7"})[0]
    check(plain[:10] == [401] * 10 and plain[10] == 429 and as_gateway == 429 and other == 401,
          f"the server sees connections through the published port as coming from the gateway {gateway} "
          f"(its limit is that address's), and believes X-Forwarded-For from it (another address has its "
          f"own limit): {plain[-1]}, {as_gateway}, {other}")
    forwarded = attempts("carol@example.com", "198.51.100.4")
    after = http(base, "POST", "/api/auth/login", {"email": "carol@example.com", "password": "wrong password!"},
                 headers={"X-Forwarded-For": "198.51.100.5"})[0]
    spoofed = http(base, "POST", "/api/auth/login", {"email": "carol@example.com", "password": "wrong password!"},
                   headers={"X-Forwarded-For": "198.51.100.4, 198.51.100.5"})[0]
    check(forwarded[10] == 429 and after == 401 and spoofed == 401,
          "the sign-in limit is kept per forwarded address, read from the right (a client's own entry to the "
          f"left is ignored): {forwarded[10]}, {after}, {spoofed}")

    r = compose(project, files, env_file, "restart", "server", capture=True)
    check(r.returncode == 0 and wait_healthy(base, project, files, env_file),
          "the server restarts in order (SIGTERM) and is healthy again")
    _, got = http(base, "GET", f"/api/documents/{doc['id']}/content", token=token)
    check(got["seq"] == 1 and got["content"]["features"][0]["params"]["depth"] == 20.5,
          "after a restart the edit is still there")
    _, first = http(base, "GET", f"/api/documents/{doc['id']}/content?seq=0", token=token)
    depth = first["content"]["features"][0]["params"]["depth"]
    check(depth == 10.0 and isinstance(depth, float), "state 0 comes back from storage unchanged (10.0 is a float)")
    if postgres:
        r = compose(project, files, env_file, "exec", "-T", "db", "psql", "-U", "collab", "-tAc",
                    "select count(*) from changes", capture=True)
        check(r.stdout.strip() == "1", f"the change is a row in PostgreSQL ({r.stdout.strip()!r})")
    logs = compose(project, files, env_file, "logs", "--no-color", "server", capture=True).stdout
    check("correct horse" not in logs and token not in logs, "the log holds no password or token")


def docker_checks(remove_image: bool, port: int | None, jobs: int) -> None:
    work = Path(tempfile.mkdtemp(prefix="oswx-collab-rust-deploy-"))
    port = port or free_port()
    variants = [("sqlite", [BASE], False), ("postgres", [BASE, POSTGRES], True),
                ("postgres-s3", [BASE, POSTGRES, S3], True)]
    built = False
    try:
        for name, files, postgres in variants:
            project = f"oswx-deploy-check-{name}"
            env_file = work / f"{name}.env"
            base = f"http://127.0.0.1:{port}"
            env_file.write_text("\n".join([
                f"PUBLIC_URL={base}", f"HOST_PORT={port}", f"POSTGRES_PASSWORD={secrets.token_hex(16)}",
                f"S3_ACCESS_KEY=GK{secrets.token_hex(16)}", f"S3_SECRET_KEY={secrets.token_hex(32)}",
                f"GARAGE_RPC_SECRET={secrets.token_hex(32)}", f"CARGO_BUILD_JOBS={jobs}", ""]), encoding="utf-8")
            print(f"\n--- {name} ({' + '.join(Path(f).name for f in files)}) on 127.0.0.1:{port}", flush=True)
            args = ["up", "-d"] + ([] if built else ["--build"])
            try:
                r = compose(project, files, env_file, *args)
                built = True
                check(r.returncode == 0, f"{name}: `docker compose up` works")
                if r.returncode:
                    continue
                healthy = wait_healthy(base, project, files, env_file)
                check(healthy, f"{name}: the server container becomes healthy (the image's HEALTHCHECK)")
                if not healthy:
                    print(compose(project, files, env_file, "logs", "--tail", "60", capture=True).stdout)
                    continue
                use_the_server(base, port, project, files, env_file, postgres)
            except Exception as e:  # one variant's surprise must not stop the others
                check(False, f"{name}: the checks ran to the end ({type(e).__name__}: {e})")
            finally:
                compose(project, files, env_file, "down", "-v", "--remove-orphans", capture=True)
        size = subprocess.run(["docker", "image", "inspect", IMAGE, "--format", "{{.Size}}"], text=True,
                              capture_output=True).stdout.strip()
        if size:
            print(f"image {IMAGE}: {int(size) / 1e6:.0f} MB")
    finally:
        shutil.rmtree(work, ignore_errors=True)
        if remove_image:
            subprocess.run(["docker", "image", "rm", IMAGE], capture_output=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--docker", action="store_true", help="build and run each Compose variant")
    parser.add_argument("--port", type=int, help="the port of 127.0.0.1 to publish on (default: a free one)")
    parser.add_argument("--remove-image", action="store_true", help="remove the built image afterwards")
    parser.add_argument("--jobs", type=int, default=2, help="parallel compiler jobs for the image build")
    args = parser.parse_args()
    static_checks()
    if args.docker and not failures:
        docker_checks(args.remove_image, args.port, args.jobs)
    print("\nRESULT:", "PASS" if not failures else f"FAIL ({len(failures)}): {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
