# Self-hosting

What has been run: the whole test suite on SQLite and on PostgreSQL 16; the Docker image and both Compose
stacks below on a Raspberry Pi (arm64, Debian 13, Docker 26), with two real openswx sessions editing through
it and the data surviving a restart; file storage in a folder, a fake S3 and Garage; real Google and GitHub sign-in through openswx against a server on
localhost. What has not: AWS S3 itself.

## Smallest install

```
python -m venv .venv
.venv/bin/python -m pip install openswx-collab-server          # or ./protocol ./server from a checkout
OSWX_COLLAB_PUBLIC_URL=https://collab.example.org .venv/bin/openswx-collab-server serve --host 127.0.0.1 --port 8787
```

Data goes to `./data` (SQLite database and files). Back up that folder.

## Docker Compose (server + Postgres)

The Compose file runs the published image, `ghcr.io/dukeofcheese/openswx-collab-server` (amd64 and
arm64); pin a version with `OSWX_COLLAB_VERSION` in `deploy/.env`. Add `-f deploy/docker-compose.build.yml`
to build it from your checkout instead.

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

The proxy must pass the client's address in `X-Forwarded-For` (Caddy does). The server believes that
header only from `OSWX_COLLAB_TRUSTED_PROXIES`; otherwise every client shares the proxy's address and its
per-address limits (sign-in attempts, connections). With Docker Compose the proxy's connections arrive
from the Compose network's gateway, so the Compose file gives that network a fixed subnet,
`NETWORK_SUBNET` in `deploy/.env` (`172.30.87.0/24` unless set), and trusts it. Change it if it overlaps a
network the machine already has.

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

## Email

Optional. Without it, invites are links you pass on yourself and forgotten passwords are reset by you
(`openswx-collab-server reset-password`). With it, an invite made for an address is emailed, and people can
reset a forgotten password themselves through a link that works for an hour.

| Variable | |
|---|---|
| `SMTP_HOST`, `SMTP_PORT` | your mail provider's SMTP server (port 587 by default) |
| `SMTP_SECURITY` | `starttls` (default), `ssl` (usually port 465) or `none` |
| `SMTP_USER`, `SMTP_PASSWORD` | the login, if the server needs one |
| `SMTP_FROM`, `SMTP_FROM_NAME` | the sender address (must be one your provider lets you send from) and name |

Any provider works: your own mail server, Google Workspace, Microsoft 365, Postmark, Amazon SES. If the mail
server cannot be reached, invites still work (the link is in the answer) and the failure is in the log.

## Leaving

People can delete their own account (their work stays with the organisation, credited to "Deleted
user") and export what the server holds about them. Organisation owners can export everything in the
organisation as a zip, and delete it, which also removes its stored files.

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
- **Deleted documents stay in the database** (marked deleted) and their files stay in storage. A deleted
  assembly no longer counts as using its parts; if an operator brings one back by hand, its parts may be gone.
