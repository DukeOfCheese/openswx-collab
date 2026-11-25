# Load generator

`loadgen` (crate `oswx-loadgen`) drives a collaboration server through a `Dialect` and prints a JSON report.
Only the `v1` dialect exists so far: REST plus the JSON WebSocket protocol of the Python server, so it
produces the before-numbers for the Rust rewrite.

## Running it against the Python server

The server's first registered user becomes its admin, so use a fresh database for a clean run (loadgen signs in
again as its own account when the server already has it, which lets several scenarios share one server).

```bash
python -m venv .venv && .venv/bin/pip install -e protocol -e server      # Scripts\ on Windows
export OSWX_COLLAB_DATABASE_URL=sqlite+aiosqlite:///$PWD/bench.db
export OSWX_COLLAB_STORAGE_URL=$PWD/bench-storage
python -m openswx_collab_server serve --port 8791 &

cargo run --release -p oswx-loadgen -- --dialect v1 --url http://127.0.0.1:8791 \
    one-doc --editors 4 --rate 5 --features 20 --duration 10s
```

With 4 editors at 5 edits a second for 10 s that is about 200 changes, all accepted.

## Scenarios

| Scenario | Meaning |
|---|---|
| `one-doc --editors N --rate R --features F` | N connections on one document; each edits its own feature (`feature = n % F`) R times a second |
| `many-docs --docs D --editors-per-doc E --rate R [--features F]` | D documents, E editors each |
| `open --doc-bytes B --times T [--features F]` | time from `hello` to `welcome` for a document of about B bytes |
| `idle --connections N` | N connections sending only pings (every 15 s) and presence (every 5 s); `--pid` adds the server's RSS |

Options on every scenario: `--duration 30s`, `--out file.json` (the report is always printed too),
`--note "text"`, `--database "sqlite"` (what the server runs on; loadgen cannot see it) and `--pid`.

With more editors than features, two editors share a feature and `stale` rejections are expected; they are
counted under `rejected`, not treated as errors. An edit with no answer after 30 s counts as a `timeout`. A
socket the server closes mid-run counts under `dropped_connections` and the run goes on; a frame that is not
valid JSON counts under `protocol_errors`. If the server cannot be reached loadgen exits with code 1.

## The result file

One JSON object: the scenario and its parameters, the server's description, run time, changes sent, accepted,
rejected by code, latency p50/p90/p99/max in milliseconds (`latency_kind` says whether that is edit latency,
send to `change.accepted`, or open time), accepted changes per second, dropped connections.

Each result also records where it was measured: CPU, thread count, RAM and OS of the machine running loadgen,
the database (from `--database`), the loadgen git commit and the start time (UTC). Run the server on the same
machine and say so in `--note` when that is the case, or the numbers mean little.
