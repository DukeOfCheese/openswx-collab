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

One JSON object: the scenario and its parameters, the server's description, run time, `document_bytes` (size of
each generated document), changes sent, accepted, rejected by code, timeouts, errors, latencies in milliseconds,
accepted changes per second and dropped connections. `changes_sent = accepted + rejected + timeouts + errors`,
where `errors` are edits lost because the connection dropped before the answer.

Two latency figures, both p50/p90/p99/max (the same text is in the file as `latency_note`):

- `latency_ms` is service time: from just before an edit is sent until its answer arrives.
- `latency_from_schedule_ms` is measured from the slot the edit was scheduled for. Each editor works to a fixed
  interval (`--rate`, random start offset) and never skips a slot: when the server stalls, the editor sends the
  waiting edits at once afterwards and the lateness counts. This is what a stall costs; `latency_ms` alone hides
  it (coordinated omission).

An edit with no answer after 30 s is a `timeout` and enters both figures as 30000 ms.

For `open`, `first_open_ms` is the first sample on its own (cold: the server loads the document) and
`latency_ms` covers the remaining ones, so use `--times` of at least 2 (5 or more for percentiles worth reading).
`welcome_bytes` (min and max) is the size of the `welcome` frame; a connection whose welcome carries no
document content fails instead of counting. Use it to check the document really was the size you asked for.

Each result also records where it was measured: CPU, thread count, RAM and OS of the machine running loadgen,
the database (from `--database`), the loadgen git commit and the start time (UTC). Run the server on the same
machine and say so in `--note` when that is the case, or the numbers mean little.
