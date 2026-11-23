# Self-hosting

What has been run: the whole test suite on SQLite and on PostgreSQL 16; the Docker image and both Compose
stacks below on a Raspberry Pi (arm64, Debian 13, Docker 26), with two real openswx sessions editing through
it and the data surviving a restart; file storage in a folder, a fake S3 and Garage. What has not: AWS S3
itself, and a real Google or GitHub sign-in (the providers are faked in the tests; the addresses match what
they publish).

## Smallest install

```
python -m venv .venv
.venv/bin/python -m pip install ./protocol ./server
OSWX_COLLAB_PUBLIC_URL=https://collab.example.org .venv/bin/openswx-collab-server serve --host 127.0.0.1 --port 8787
```

Data goes to `./data` (SQLite database and files). Back up that folder.

## Docker Compose (server + Postgres)

```
cp deploy/.env.example deploy/.env        # set PUBLIC_URL and POSTGRES_PASSWORD
docker compose -f deploy/docker-compose.yml --env-file deploy/.env up -d
```

The server is published on `127.0.0.1:8787` of the host; set `HOST_PORT` in `deploy/.env` if that port is
taken.

Add `-f deploy/docker-compose.s3.yml` to keep files in an S3 bucket instead of the local volume. That file
runs [Garage](https://garagehq.deuxfleurs.fr), a small open-source S3 server, as a single node; it makes its
bucket and key by itself from three values you put in `deploy/.env` (the example file shows how to generate
them). To use a store you already have (AWS S3, Cloudflare R2), skip that file and set
`OSWX_COLLAB_STORAGE_URL`, `OSWX_COLLAB_S3_ENDPOINT` and the `AWS_*` variables on the server.

## TLS

The server speaks plain HTTP on port 8787. Put a proxy in front that terminates TLS and passes WebSockets.
With Caddy that is the whole configuration:

```
collab.example.org {
    reverse_proxy 127.0.0.1:8787
}
```

## First account and invites

The first account registered becomes the server admin and can create organisations. Everyone else joins
with an invite link made by an organisation owner or admin. Set `OSWX_COLLAB_OPEN_REGISTRATION=true` to let
anyone sign up and create their own organisation.

Locked out? On the server:

```
openswx-collab-server create-admin you@example.org
openswx-collab-server reset-password someone@example.org
```

## Sign-in with Google or GitHub

Create an OAuth app with the provider and set the callback address to

- Google: `<PUBLIC_URL>/api/auth/oauth/google/callback`
- GitHub: `<PUBLIC_URL>/api/auth/oauth/github/callback`

then set `OSWX_COLLAB_GOOGLE_CLIENT_ID` / `_SECRET` or `OSWX_COLLAB_GITHUB_CLIENT_ID` / `_SECRET`.
A provider with no credentials is simply not offered.

Someone who already has a password account adds a provider from inside openswx while signed in. A
provider sign-in never takes over a password account just because the address matches (unless it is the
first, admin, account or one the operator created): otherwise a person could register someone else's
address and wait for its owner to arrive.

## Settings

All are environment variables starting `OSWX_COLLAB_`.

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | `sqlite+aiosqlite:///./data/collab.db` | or `postgresql+asyncpg://user:pw@host/db` |
| `STORAGE_URL` | `./data/storage` | a folder, or `s3://bucket/prefix` |
| `S3_ENDPOINT` | | for MinIO and other S3-compatible stores |
| `PUBLIC_URL` | `http://localhost:8787` | how clients reach the server |
| `OPEN_REGISTRATION` | `false` | |
| `SESSION_DAYS` | `30` | |
| `AUTO_MIGRATE` | `true` | upgrade the database on start |
| `SNAPSHOT_EVERY` | `200` | full copy of a document every N changes |
| `LOCK_TTL` / `LOCK_GRACE` | `45` / `60` | seconds |
| `MAX_CHANGE_BYTES` / `MAX_BLOB_BYTES` | 8 MB / 256 MB | |

## Upgrading

New versions may change the database. The server upgrades it on start; back the database up before you
start a new version. To control when that happens, set `OSWX_COLLAB_AUTO_MIGRATE=false`: the server then
refuses to start on an older database until you run

```
openswx-collab-server migrate        # docker compose ... run --rm server openswx-collab-server migrate
openswx-collab-server db-status      # where the database is, and what this version needs
```

A database written by a newer version is never opened by an older one.

## Seat limits

Not in the open server or its Docker image: they come from a separate package that plugs into the
server's extension hooks (`server/openswx_collab_server/hooks.py`). Without it there is no limit.

## Limits to know about

- **One server process per database.** Open documents are held in that process's memory. Do not run
  several workers or replicas.
- **Locks are in memory.** A restart releases them; edits are never lost, because every accepted change
  is in the database before it is acknowledged.
- **No email is sent.** Invites are links you pass on yourself; forgotten passwords are reset by the
  operator.
- **Deleted documents stay in the database** (marked deleted) and their files stay in storage. A deleted
  assembly no longer counts as using its parts; if an operator brings one back by hand, its parts may be gone.
