# Self-hosting

This is about the Rust server (`crates/serverd`, the program `openswx-collab-server`, version 0.2), which
speaks protocol version 2. The Python server 0.1.x (protocol 1, the image
`ghcr.io/dukeofcheese/openswx-collab-server` and `deploy/docker-compose.yml`) is still in the repository
until openswx's client speaks version 2; its settings were the same where both have them, and
[the end of this page](#the-python-server-01x) says what differs.

What has been run: the whole test suite on SQLite and on PostgreSQL 16, files in a folder and in an
in-process S3 server; the image from `deploy/Dockerfile.rust` and the Compose files below (SQLite;
PostgreSQL; PostgreSQL with Garage) on a Raspberry Pi 5 (arm64, Debian 13, Docker 26), checked with
`tools/check_deploy_rust.py --docker`. What has not: AWS S3, Cloudflare R2 or Backblaze itself, and a
real mail relay with the Rust server.

## Smallest install

Build the program (Rust 1.89 or later; `cargo` from [rustup](https://rustup.rs)):

```
cargo build --release -p oswx-serverd             # target/release/openswx-collab-server
OSWX_COLLAB_PUBLIC_URL=https://collab.example.org target/release/openswx-collab-server serve --host 127.0.0.1 --port 8787
```

Data goes to `./data` (the SQLite database `collab.db` and the folder `storage`). Back up that folder (with
the server stopped, or with `sqlite3 data/collab.db ".backup backup.db"` for the database).

```
openswx-collab-server serve [--host 127.0.0.1] [--port 8787]
openswx-collab-server migrate             # upgrade the database (whatever AUTO_MIGRATE says)
openswx-collab-server db-status           # where the database is, and what this version needs
openswx-collab-server create-admin <email> [name]
openswx-collab-server reset-password <email>
openswx-collab-server --help              # every setting, with its default
```

`create-admin` and `reset-password` take the password from `OSWX_COLLAB_PASSWORD`, or ask for it twice on
the terminal.

**Exit codes.** 0: done (`serve`: stopped in order). 1: failed (`serve`: could not start, did not stop in
order within `OSWX_COLLAB_SHUTDOWN_TIMEOUT`, or was told twice to stop; `db-status`: a missing SQLite
file, a database written by a newer version, or one that cannot be reached). 2: the command line or a
setting was wrong; the message names the variable and never repeats its value. `db-status` never creates a
database: on a missing SQLite file it says so and exits 1. An empty database (a PostgreSQL database the
server has not set up yet) is not an error: it prints `database at none (an empty
database); this server needs <version>` and exits 0, as it does for an older database that `migrate` would
upgrade.

## Docker Compose

There is no published image of the Rust server yet: the Compose files build it from your checkout with
`deploy/Dockerfile.rust` (a slim Debian with the one binary, running as an unprivileged user, about
125 MB). The build takes a few minutes on a laptop and most of an hour on a Raspberry Pi; it uses two
compiler jobs unless `CARGO_BUILD_JOBS` in `deploy/.env` says otherwise.

```
cp deploy/.env.example deploy/.env        # set PUBLIC_URL (and POSTGRES_PASSWORD for PostgreSQL)

# SQLite and files in one volume:
docker compose -f deploy/docker-compose.rust.yml --env-file deploy/.env up -d --build

# PostgreSQL:
docker compose -f deploy/docker-compose.rust.yml -f deploy/docker-compose.rust-postgres.yml \
  --env-file deploy/.env up -d --build

# PostgreSQL, and files in an S3 bucket (Garage, a small S3 server, as a single node):
docker compose -f deploy/docker-compose.rust.yml -f deploy/docker-compose.rust-postgres.yml \
  -f deploy/docker-compose.s3.yml --env-file deploy/.env up -d --build
```

The server is published on `127.0.0.1:8787` of the host only; set `HOST_PORT` in `deploy/.env` if that
port is taken. The Compose project is called `openswx-collab-rust`, and its volumes `rust-data` (the
server's `/data`) and `rust-db` (PostgreSQL), so it does not touch a Python stack on the same machine.

`docker-compose.s3.yml` runs [Garage](https://garagehq.deuxfleurs.fr); it makes its bucket and key by
itself from three values you put in `deploy/.env` (the example file shows how to generate them). The
server refuses to start until the bucket can be written, read, listed and deleted from, so it may restart
a few times while Garage comes up. To use a store you already have (AWS S3, Cloudflare R2), skip that
file and set `OSWX_COLLAB_STORAGE_URL`, `OSWX_COLLAB_S3_ENDPOINT` and the `AWS_*` variables on the server
(see [S3-compatible storage](#s3-compatible-storage)).

The Compose file passes on only the settings `deploy/.env.example` lists. For any other setting (below),
edit the `environment:` block in `deploy/docker-compose.rust.yml`, or keep your changes apart in an override
file of your own and add it with one more `-f`:

```
# deploy/docker-compose.override.yml
services:
  server:
    environment:
      OSWX_COLLAB_SESSION_DAYS: "14"
      OSWX_COLLAB_LOG: json
```

```
docker compose -f deploy/docker-compose.rust.yml -f deploy/docker-compose.override.yml \
  --env-file deploy/.env up -d
```

Stopping: `docker stop` sends SIGTERM and waits 10 s; the server closes every open document (writing its
snapshot) and the journal within `OSWX_COLLAB_SHUTDOWN_TIMEOUT` (8 s). Keep `stop_grace_period` above it.

The image's health check asks `GET /api/health`. `GET /api/ready` also checks the database and the storage
(on S3 that costs a request each time): use it for a load balancer, not every few seconds. It gives its
answer again for a second and runs one check at a time, so many callers cost one check; it logs why the
server is not ready when that starts, then at most once a minute.

## TLS and the proxy in front

The server speaks plain HTTP. Put a proxy in front that terminates TLS and passes WebSockets. With Caddy
that is the whole configuration:

```
collab.example.org {
    reverse_proxy 127.0.0.1:8787
}
```

**The client's address.** The server keeps its sign-in limits per client address, read from
`X-Forwarded-For` on connections from `OSWX_COLLAB_TRUSTED_PROXIES` (by default `127.0.0.1` and `::1`, a
proxy on the same machine). The per-address connection limit is applied when a connection is accepted, so
it does not apply to a trusted proxy's connections at all: limit connections per client in the proxy. With
nginx that is `limit_conn_zone $binary_remote_addr zone=perclient:10m;` in `http` and `limit_conn
perclient 512;` in the `location`; Caddy's standard build has no such setting.

The server reads `X-Forwarded-For` from the right: the last entry is what the proxy next to it saw, and
the client is the first address that is not one of your proxies; anything a client wrote to the left of
that is ignored. An entry that is not a bare address (a host name, an address with a port) ends the
reading: the client is then the last address read before it, which is usually your proxy. So a proxy that
appends `ip:port` puts every client on one address, and one set of sign-in limits. **The proxy must append
the bare address it saw to `X-Forwarded-For`** (Caddy, nginx with `$proxy_add_x_forwarded_for`, Traefik
and HAProxy with `option forwardfor` all do), never pass on a client's header unchanged as the only entry.
A range wider than /8 (IPv4) or /16 (IPv6) is refused.

With Docker Compose a proxy on the host reaches the server through the published port, so its connections
arrive from the Compose network's **gateway** (`172.30.87.1`), not from `127.0.0.1`. The Compose file
therefore gives that network a fixed subnet, `NETWORK_SUBNET` in `deploy/.env` (`172.30.87.0/24` unless
set), and sets `OSWX_COLLAB_TRUSTED_PROXIES` to it. Change it if it overlaps a network the machine already
has. Without it every client would share the gateway's address, and so one set of sign-in limits and one
connection limit for all of them. With it, the gateway's connections are not counted against the
per-address connection limit at all: limit connections per client in the proxy, as above.

## First account and invites

The first account registered becomes the server admin and can create organisations. Everyone else joins
with an invite link made by an organisation owner or admin. Set `OSWX_COLLAB_OPEN_REGISTRATION=true` to let
anyone sign up and create their own organisation.

Locked out? On the server (with Compose: `docker compose -f deploy/docker-compose.rust.yml exec server
openswx-collab-server ...`):

```
openswx-collab-server create-admin you@example.org
openswx-collab-server reset-password someone@example.org
```

`reset-password` does not lift the sign-in limits of the running server (429 `too many attempts`): see
[What the error answers mean](#what-the-error-answers-mean).

## Sign-in with Google or GitHub

Create an OAuth app with the provider and set the callback address to

- Google: `<PUBLIC_URL>/api/auth/oauth/google/callback`
- GitHub: `<PUBLIC_URL>/api/auth/oauth/github/callback`

then set `OSWX_COLLAB_GOOGLE_CLIENT_ID` / `_SECRET` or `OSWX_COLLAB_GITHUB_CLIENT_ID` / `_SECRET`.
A provider with no credentials (or only one of the pair) is simply not offered.

Someone who already has a password account adds a provider from inside openswx while signed in. A
provider sign-in never takes over a password account just because the address matches (unless it is the
first, admin, account or one the operator created): otherwise a person could register someone else's
address and wait for its owner to arrive.

## Settings

All are environment variables, read once at start. A setting that cannot be read stops the program before
it opens anything. Booleans are `true`/`false`, `1`/`0`, `yes`/`no` or `on`/`off`; anything else is an
error. Seconds may have a fraction. `openswx-collab-server --help` prints the same list.

### The database

| Variable | Default | |
|---|---|---|
| `OSWX_COLLAB_DATABASE_URL` | `sqlite+aiosqlite:///./data/collab.db` (the same as `sqlite://./data/collab.db`) | `sqlite://<path>` (`sqlite:///abs/path` is absolute) or `postgres://user:password@host/database`. The Python forms `sqlite+aiosqlite:///…` and `postgresql+asyncpg://…` are accepted. No in-memory database, no `?` options on a SQLite path |
| `OSWX_COLLAB_AUTO_MIGRATE` | `true` | upgrade the schema when `serve` starts; `false`: refuse to start on an older database until `migrate` is run |
| `OSWX_COLLAB_DATABASE_POOL` | `10` | PostgreSQL connections for requests |
| `OSWX_COLLAB_DATABASE_JOURNAL_POOL` | `2` | PostgreSQL connections the journal (live edits) has to itself, besides those |
| `OSWX_COLLAB_DATABASE_LOCK_TIMEOUT` | `5` | seconds a request waits for another's row lock on PostgreSQL before it is answered 409 "try again"; at most 20 |
| `OSWX_COLLAB_DATABASE_JOURNAL_LOCK_TIMEOUT` | `2` | seconds the journal waits for a row lock before it writes its batch again one document at a time; at most 7.5 |

PostgreSQL runs every statement under a 30 s time limit, at READ COMMITTED with row locks; give the server
`DATABASE_POOL` + `DATABASE_JOURNAL_POOL` connections (and `max_connections` in PostgreSQL to match).

### Storage for blobs and snapshots

| Variable | Default | |
|---|---|---|
| `OSWX_COLLAB_STORAGE_URL` | `./data/storage` | a folder, or `s3://<bucket>/<prefix>` (the prefix may be empty; letters, digits, `.`, `_`, `-`, `/`) |
| `OSWX_COLLAB_S3_ENDPOINT` | | an S3-compatible store's address, `http://host:port` or `https://host:port`; empty: AWS S3 |
| `OSWX_COLLAB_S3_PATH_STYLE` | on when an endpoint is set | `true`: the bucket in the path (`https://host/bucket/key`); `false`: in the host name |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | | the S3 credentials (both of the first two, or neither); none: the machine's own AWS role |
| `AWS_DEFAULT_REGION` (or `AWS_REGION`) | `us-east-1` | the bucket's region |
| `OSWX_COLLAB_BLOB_IDLE_TIMEOUT` | `60` | seconds without a byte before a blob upload or download is given up |
| `OSWX_COLLAB_MAX_BLOB_BYTES` | `268435456` (256 MiB) | the largest blob |
| `OSWX_COLLAB_UPLOAD_MEMORY` | `754974720` (720 MiB) | bytes the blob uploads running at once may hold in all. Each is charged what its storage holds for one: about 45 MiB for S3 (part buffers), 4 MiB for a folder. One that does not fit waits a few seconds, then 503. A blob already stored takes nothing |
| `OSWX_COLLAB_MAX_BLOB_UPLOADS_PER_ORG` | `8` | blob uploads at once for one organisation; one more: 429 |
| `OSWX_COLLAB_MAX_BLOB_UPLOADS_PER_USER` | `4` | blob uploads at once for one account; one more: 429 |
| `OSWX_COLLAB_MIN_UPLOAD_RATE` | `4096` | bytes a second an upload, or any request body, must keep up on average; 0: no floor. A blob upload that falls behind is answered 400 and nothing is stored |
| `OSWX_COLLAB_UPLOAD_RATE_GRACE` | `30` | seconds of slack against that rate: how long a body may take to start, or pause once it has earned the time back |
| `OSWX_COLLAB_UPLOAD_TIME_LIMIT`, `OSWX_COLLAB_UPLOAD_TIME_RATE` | `600`, `65536` | an upload of n bytes may take at most `max(LIMIT, n / RATE)` seconds (10 minutes, more for a blob over 37.5 MiB), then 400 |

`OSWX_COLLAB_MAX_BLOB_UPLOADS` (the Python server's count of uploads at once) is gone: the server **refuses
to start while it is set**, naming `UPLOAD_MEMORY`. With the defaults, sixteen S3 uploads fit at once (720 /
45), or as many folder uploads as people send.

### Serving

| Variable | Default | |
|---|---|---|
| `OSWX_COLLAB_PUBLIC_URL` | `http://localhost:8787` | how clients and browsers reach the server (OAuth callbacks, invite and reset links) |
| `OSWX_COLLAB_TRUSTED_PROXIES` | `127.0.0.1, ::1` | addresses and CIDR ranges (commas or spaces) of the proxies whose `X-Forwarded-For` is believed; none wider than /8 or /16 |
| `OSWX_COLLAB_REQUEST_TIMEOUT` | `60` | seconds from a request's head, or the last of its body, to the beginning of its answer; then 503 `the request took too long; try again`. Not for blob uploads, downloads, exports or document connections |
| `OSWX_COLLAB_WRITE_TIMEOUT` | `60` | seconds a client may take nothing of what it is sent before its connection is closed |
| `OSWX_COLLAB_MAX_CONNECTIONS` | `4096` | connections held open at once, document connections included; at most **65,536** (a client takes a document's peer list of at most that many) |
| `OSWX_COLLAB_MAX_CONNECTIONS_PER_ADDRESS` | `512` | connections from one address, as the connection arrives (an IPv6 address counts as its /64); a trusted proxy's connections are not counted (limit connections per client in the proxy); 0: no limit |
| `OSWX_COLLAB_MAX_LARGE_BODIES` | `16` | request bodies over 1 MiB (document creation, up to 8 MB) read at once; one more: 503 |
| `OSWX_COLLAB_MAX_EXPORTS` | `2` | organisation exports running at once, and account exports at once apart from them; one more: 429. An account runs one export at a time. A client that reads an export more slowly than `MIN_UPLOAD_RATE` on average loses it, and so does one that pauses for longer than the time it has in hand against that rate: at most `UPLOAD_RATE_GRACE` (30 s by default) and never more than 60 s; 60 s when `MIN_UPLOAD_RATE=0`. An export takes at most 30 minutes |
| `OSWX_COLLAB_SHUTDOWN_TIMEOUT` | `8` | seconds an orderly stop may take before the process ends anyway |
| `OSWX_COLLAB_METRICS_TOKEN` | | the bearer token `GET /metrics` asks for: 16 to 256 printable ASCII characters, no spaces; empty: no `/metrics` (404) |
| `OSWX_COLLAB_LOG` | `text` | `text`, or `json` for one JSON object a line |
| `OSWX_COLLAB_LOG_LEVEL` | `info` | `error`, `warn`, `info`, `debug` or `trace` for the server's own crates (or `oswx=<level>`); other crates log warnings and errors only unless named: `debug,sqlx=info,hyper=trace` |

When the server holds `MAX_CONNECTIONS`, a new connection takes the place of the one that has waited
longest without finishing its first request (someone holding sockets open a byte at a time), or failing
that the one idle longest; never one with a request in its handler, an answer still being sent, or a
document connection. Eight more connections (the **health reserve**) answer `GET /api/health` alone, so a
health check gets through when the server is full; past those a connection is accepted and closed at once.
One address over `MAX_CONNECTIONS_PER_ADDRESS` likewise replaces its own oldest idle connection.
Document connections (WebSockets) are never closed to make room, so they may take at most
`MAX_CONNECTIONS` less an eighth of it, and less at least 256 (3,584 of the default 4,096): the rest stay
for REST. One more is refused at once with close code 4429 (`server_full`), which tells the client to
try again later.

Every connection is an open file. Keep the process's limit on open files (`ulimit -n`) above
`MAX_CONNECTIONS` plus about 260 (the health reserve and the database, storage and log). **Docker gives a
container a soft limit of 1,024 since version 25**, which is too low: `deploy/docker-compose.rust.yml` sets
`ulimits: nofile: 65536`; with `docker run`, pass `--ulimit nofile=65536:65536`. At start the server raises
its soft limit to the hard limit and warns in the log when that is still below what `MAX_CONNECTIONS`
needs. If it runs out anyway, it closes an idle connection for each one it cannot accept (so a health check
can still get in) and logs that at most once a minute. A request head must arrive within 30 s, and a kept-alive connection idle for 30 s is closed.

`/metrics` is Prometheus text (requests, rooms, connections, the journal, uploads, mail). **Make the token
long and random** (`openssl rand -hex 32`): it is compared in constant time, but it is all that stands
between the internet and the server's counters. Better still, do not route `/metrics` through the public
proxy at all.

Logs go to standard error. They never hold a token, a password, an invite, a login code or document
content; a panic is logged with where it happened, never its message.

### Accounts

| Variable | Default | |
|---|---|---|
| `OSWX_COLLAB_OPEN_REGISTRATION` | `false` | anyone may register; `false`: the first account, then invites |
| `OSWX_COLLAB_SESSION_DAYS` | `30` | days a sign-in lasts |
| `OSWX_COLLAB_RESET_MINUTES` | `60` | minutes a password reset link works |
| `OSWX_COLLAB_PASSWORD` | | the password `create-admin` and `reset-password` set (asked for on the terminal when unset) |
| `OSWX_COLLAB_GOOGLE_CLIENT_ID`, `_SECRET`; `OSWX_COLLAB_GITHUB_CLIENT_ID`, `_SECRET` | | sign-in with Google and GitHub (both of a pair) |

### Email

| Variable | Default | |
|---|---|---|
| `OSWX_COLLAB_SMTP_HOST`, `OSWX_COLLAB_SMTP_FROM` | | your mail provider's SMTP server and the sender address (one your provider lets you send from); both, or no email is sent |
| `OSWX_COLLAB_SMTP_PORT` | `587` | |
| `OSWX_COLLAB_SMTP_SECURITY` | `starttls` | `starttls`, `ssl` (usually port 465) or `none` |
| `OSWX_COLLAB_SMTP_USER`, `OSWX_COLLAB_SMTP_PASSWORD` | | the login, if the server needs one |
| `OSWX_COLLAB_SMTP_FROM_NAME` | `openswx collaboration` | the sender's name |
| `OSWX_COLLAB_MAX_MAIL_SENDS` | `16` | emails on their way at once; one more is not sent |

### Documents

| Variable | Default | |
|---|---|---|
| `OSWX_COLLAB_SNAPSHOT_EVERY` | `200` | changes between two stored copies of a document |
| `OSWX_COLLAB_CATCHUP_LIMIT` | `500` | a reconnecting client further behind than this gets the whole document |
| `OSWX_COLLAB_MAX_CHANGE_BYTES` | `8388608` (8 MiB) | the largest change |
| `OSWX_COLLAB_MAX_CONNECTIONS_PER_USER` | `64` | open document connections of one account; one more is refused (close 4429) |
| `OSWX_COLLAB_PRESENCE_RATE` | `60` | presence and draft messages a second from one connection |
| `OSWX_COLLAB_LOCK_TTL` | `45` | seconds: a connected client that stays silent this long loses its locks |
| `OSWX_COLLAB_LOCK_GRACE` | `60` | seconds: a disconnected client keeps its locks this long |
| `OSWX_COLLAB_ROOM_IDLE` | `120` | seconds: an open document nobody is connected to is unloaded (snapshot written) |
| `OSWX_COLLAB_SWEEP_INTERVAL` | `5` | seconds between sweeps of open documents (lock expiry, expired sessions) |
| `OSWX_COLLAB_ACCESS_RECHECK` | `30` | seconds between checks of open connections against sessions and roles |

## Email

Optional. Without it, invites are links you pass on yourself and forgotten passwords are reset by you
(`openswx-collab-server reset-password`). With it, an invite made for an address is emailed, and people can
reset a forgotten password themselves through a link that works for an hour. Any provider works: your own
mail server, Google Workspace, Microsoft 365, Postmark, Amazon SES.

**Certificates.** The mail server's certificate must be for the name in `OSWX_COLLAB_SMTP_HOST` and lead
to one of Mozilla's roots (built into the program, so it works in a container without a certificate
bundle) or to a root in the machine's own trust store. A relay with a certificate from a private authority
works once that authority is installed in the trust store (in the image: copy it to
`/usr/local/share/ca-certificates/` and run `update-ca-certificates` in a derived image, or point
`SSL_CERT_FILE` at a bundle that holds it). There is no setting that turns the check off.

**`emailed: false`.** Sending happens after the invite is stored. An invite's answer says `emailed: true`
only if the mail server took the message within 10 seconds. `false` means it was not confirmed: the mail
server refused it or could not be reached (the reason is in the log, without the address), the server was
already sending `MAX_MAIL_SENDS` emails, or it is **still on its way** (a slow relay). The invite exists
either way and its link is in the answer, which openswx shows.

**Invites are limited**, so that nobody can use the server to mail strangers: an organisation has at most
50 invites emailed an hour and an account 200 a day (past either, the invite is made, not emailed, and the
log says so without the address), and an organisation has at most 1,000 open invites (unused and unexpired;
one more is 409). A password reset request answers 202 for any well-formed request, whether or not there
is such an account and when the request was limited (501 when the server cannot send email).

## S3-compatible storage

Set `OSWX_COLLAB_STORAGE_URL=s3://<bucket>/<prefix>`. **The bucket must exist**; the server never creates
it, and at start it refuses to run until it can write, read, list and delete under the prefix (including
when the store is merely down at that moment: Compose restarts it). A folder and a bucket hold the same
bytes under the same keys, so moving between them is a plain copy (`aws s3 sync`, `rclone`).

- **Permissions** the key needs: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`,
  `s3:AbortMultipartUpload` on `bucket/prefix/*`, and `s3:ListBucket` on the bucket (it may be limited to
  the prefix). Without `ListBucket`, S3 answers 403 instead of 404 for a missing object, which the server
  treats as an outage.
- **Credentials**: the access key and secret (and `AWS_SESSION_TOKEN` for temporary ones), or, with none
  set, an EC2 instance's own role. ECS task roles and EKS service-account roles (IRSA) are not read: hand
  such credentials over as the three variables.
- **Lifecycle rule**: abort incomplete multipart uploads after a day (AWS:
  `AbortIncompleteMultipartUpload`, `DaysAfterInitiation: 1`; Garage and MinIO have their own). Without it,
  the parts of uploads cut off by a crash are kept (and billed) for ever.
- **Versioning**: leave it off, or add a rule that expires non-current versions. Otherwise deleted
  organisations do not free space and their data is not really gone.
- **Path style**: MinIO and Garage want the bucket in the path, which is the default whenever
  `OSWX_COLLAB_S3_ENDPOINT` is set. AWS gets the bucket in the host name; a bucket with dots in its name on
  AWS needs `OSWX_COLLAB_S3_PATH_STYLE=true`.
- **R2**: endpoint `https://<account>.r2.cloudflarestorage.com`, region `auto`. **Backblaze B2**: endpoint
  `https://s3.<region>.backblazeb2.com`, region as in the endpoint (from their documentation, not tested).
  **Garage** (the Compose file): region `garage`, as in `deploy/garage.toml`.
- Uploads go in parts of 8 MiB (the last part may be a little larger); compressed with zstd.
- An `http://` endpoint is used only when written with `http://`.
- The server's HTTPS clients (S3, and the OAuth providers) trust the machine's trust store as well as
  Mozilla's roots.
- **A bucket deleted while the server runs** is noticed by `/api/ready` within a second, but documents and uploads
  notice only once 30 s have passed since the bucket last answered: until then a missing object reads as
  absent (a change referring to a blob is refused `missing_blob`, an upload fails with 500).

## What the error answers mean

For someone reading the server's logs or a user's report:

- **409 `another request changed the same thing at the same moment; try again`**: PostgreSQL put one
  request behind another (a serialisation conflict or deadlock), or the request waited
  `DATABASE_LOCK_TIMEOUT` for another's row lock. Nothing was written; sending it again works. A few now
  and then is normal; many mean one organisation is very busy.
- **503 `the server is busy; try again`**: nothing was done. No database connection came free in time
  (raise `DATABASE_POOL`), a statement ran out of its 30 s, or one organisation's queue is full: requests
  that change an organisation's data run **one at a time per organisation**, queued in the server without
  holding a database connection, and at most 64 wait for one organisation, none longer than 10 s. Other
  organisations are not held up by it. Also 503: `the request took too long; try again`
  (`REQUEST_TIMEOUT`), a full upload memory, too many large bodies at once, or a full server.
- **500 `server error`**: something unexpected; the log has the cause (a database refusing a duplicate a
  route did not expect, a storage error). Report it.
- **402**: an extension (seat limits, a paid plan) refused; the message says why.
- **429**: sign-in attempts (10 in 10 minutes for one email address from one client address, and 100 in
  an hour for one email address from anywhere, both cleared by a success; see the next paragraph) and
  OAuth starts from one address (600 in 10 minutes), an IPv6 client counting as its /64 for all of them; an account's or organisation's
  uploads at once; one export at a time per account, and `MAX_EXPORTS` organisation exports and as many
  account exports per server. Password reset requests over their limit (5 an hour) still
  answer 202, and nothing is sent.

**A password sign-in refused by the limit on the address.** The limit of 100 attempts an hour for one email
address makes guessing from many addresses slow, but it also lets someone keep an account (an admin's too)
from signing in with its password, by guessing wrong from ten addresses. What still works:

- **sessions already open** (openswx stays signed in): the limits apply only to signing in;
- **a sign-in with Google or GitHub**, if the account has one linked: it is not counted against the address;
- **a password reset by email**: once the link is used, both sign-in limits on that address are lifted, for
  every client, and the owner signs in at once (whoever reads the mail controls the account anyway);
- **waiting**: an attempt counts for an hour, so the refusals stop an hour after the guessing stops;
- **`openswx-collab-server reset-password`** sets the password, but it runs as a separate process and does
  not lift the limits of the running server: the counts are kept in memory, so they go only with a server
  restart (which forgets every count, for every account) or by waiting.

## Leaving

People can delete their own account (their work stays with the organisation, credited to "Deleted
user") and export what the server holds about them. Organisation owners can export everything in the
organisation as a zip, and delete it.

**Deletion is two steps.** Deleting a project or an organisation answers at once: one short transaction
marks it, and from then on it is gone for everyone (no query finds it, its open documents are closed, its
name is free, nothing more is written to it). The rows and files are then removed **in the background**, a
chunk at a time, so that other organisations' editing never waits for a large deletion; a server that
stops in the middle goes on after its next start. Until then the space is still used. Something that cannot
be removed (storage refusing, say) is tried again later, with a warning in the log, and holds up nothing
else. A document deleted on its own stays as a marked row with its snapshots (assemblies may still show
what they used).

## Upgrading

New versions may change the database. `serve` upgrades it **before it starts listening**, so a slow
upgrade shows as a server that is not up yet (the health check fails meanwhile); back the database up
before you start a new version. To control when that happens, set `OSWX_COLLAB_AUTO_MIGRATE=false`: the
server then refuses to start on an older database until you run

```
openswx-collab-server migrate      # docker compose -f deploy/docker-compose.rust.yml ... run --rm server openswx-collab-server migrate
openswx-collab-server db-status    # exits 1 on a missing SQLite file or a database written by a newer version
```

A database written by a newer version is never opened by an older one. An edition with extensions adds its
own migrations, which run in the same places; **a migration holds its locks while it runs**, so on a
database that a running server is using, a long one (an extension adding an index to a big table) stops
writes, live edits included, until it is done. Run `migrate` with the server stopped, or at a quiet time.
Two servers creating an empty database at the same moment fail safely: start one first.

A database from the Python server is not opened (the schema is different), and there is no import:
start the Rust server with a new database.

## Stopping

`serve` stops in order on Ctrl-C, SIGTERM and **SIGHUP** (a hang-up stops it too: it does not reload
anything; run it under systemd, Docker or `nohup`, not in a terminal you close). It stops accepting
connections, lets requests in their handlers finish for up to half of `SHUTDOWN_TIMEOUT`, closes every open
document with its snapshot written, writes out the journal and ends; told a second time, it ends at once.
Nothing acknowledged is lost by a hard stop either: every acknowledged change is in the database first.

On **Windows** the same happens on Ctrl-C and Ctrl-Break, but when the console window is closed, the user
logs off or the system shuts down, Windows gives a process only a few seconds before it ends it, which may
be less than an orderly stop takes: stop the server with Ctrl-C first, or run it as a service with a longer
stop time.

## Seat limits and other extensions

Not in the open server or its image: an edition with extensions (seat limits, billing) is a program of its
own that calls `oswx_serverd::run` with its extensions (`crates/server/src/ext.rs`). Without one there is
no limit. When an extension refuses something the answer is 402; when it does not answer within 5 s, 409.

## Limits to know about

- **One node holds a document.** An open document lives in the memory of one server process, which orders
  and stores its changes. Run one process per database: do not start several replicas or workers against
  the same database (several machines are a later version).
- **Locks are in memory.** A restart releases them; edits are never lost, because every accepted change
  is in the database before it is acknowledged.
- **Deleted documents stay in the database** (marked deleted) with their snapshots; deleted projects and
  organisations are removed in the background (above). A deleted assembly no longer counts as using its
  parts.

## The Python server (0.1.x)

`deploy/Dockerfile`, `deploy/docker-compose.yml` and `deploy/docker-compose.build.yml` run the Python
server, which speaks protocol 1 and serves openswx 0.4. They stay until openswx's client speaks version 2
(the Python server is retired in a later release). Its settings are the subset above that it had. Differences that matter to an operator: it reads
`OSWX_COLLAB_MAX_BLOB_UPLOADS` (the Rust server refuses to start with it), it has none of the
connection, upload-memory, request-time, metrics and log settings, and its database is not readable by
the Rust server. It does **not** read `OSWX_COLLAB_TRUSTED_PROXIES`: the line in its Compose file takes
effect only with the Rust server. It believes `X-Forwarded-For` only from `127.0.0.1` (as uvicorn does),
so behind the Compose network's gateway every client shares the gateway's address for its sign-in
limit. `tools/check_deploy.py` checks its files; `tools/check_deploy_rust.py` checks the Rust
ones.
