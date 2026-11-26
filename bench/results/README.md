# Load measurements

Two sets, taken on the same machine with the same load generator: the Python server (the baseline, below)
and [the Rust server on the same scenarios](#rust-server-020-dev-the-same-scenarios), with a side-by-side
table and what this laptop can say about the proposed targets.

# Baseline: Python server 0.1.2 on SQLite

These are the "before" numbers for the Rust rewrite: the current Python collaboration server, measured with
`loadgen` (`crates/loadgen`, dialect `v1`) on one machine. One run per scenario, 60 s each. The raw reports are
in [`python-0.1.2-sqlite/`](python-0.1.2-sqlite/), one JSON file per run, plus a `*.server-rss.txt` file
with the server's memory (see "Server memory").

## What was measured

| | |
|---|---|
| Date | 2026-11-25 |
| Machine | 11th Gen Intel Core i7-11800H @ 2.30 GHz, 8 cores / 16 threads, 15.8 GB RAM |
| OS | Windows 11 Education 10.0.22631 |
| Python | 3.13.7 (venv; uvicorn 0.54.0, FastAPI 0.165.0, SQLAlchemy 2.1.9) |
| Server | `openswx-collab` 0.1.2, protocol 1; repo HEAD `aedff16` (the server code was last changed in `1cc6651`) |
| Loadgen | crate `oswx-loadgen`, release build; its code was last changed in `7a82fee`; the reports record `aedff16` (HEAD when run) |
| Database | SQLite, a fresh file and storage folder per scenario (system temp directory) |
| Server settings | defaults, apart from the database and storage locations; `python -m openswx_collab_server serve --port 8791`, one process |

Each scenario ran against a freshly started server and a fresh database; the server was stopped afterwards.

## Results

Columns: *connections* is the number of WebSockets (for `open`: one at a time, 20 in a row). *changes* is
sent / accepted / rejected / timeouts / errors / dropped connections. *service* is `latency_ms` (edit sent to
answer), *schedule* is `latency_from_schedule_ms` (scheduled slot to answer); both are p50 / p99 / max in
milliseconds. *run s* is the measured run time. *doc bytes* is the generated document, *welcome bytes* the
smallest and largest `welcome` frame. *first open* is the cold first open in ms; for the `open` rows the
service column holds the 19 warm opens (hello to welcome). *RSS* is the server's working set in MB:
before the run / peak / at the end. For `open` rows the change counters and accepted per second are n/a, and
the percentiles come from 19 samples, so p99 equals max. In the one-doc and idle rows the welcome size grows
with the number of connected peers.

| Run | connections | changes (sent / accepted / rejected / timeouts / errors / dropped) | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | doc bytes | welcome bytes | first open | RSS MB |
|---|---|---|---|---|---|---|---|---|---|---|
| one-doc 10 editors, rate 2, 200 features | 10 | 1200 / 1200 / 0 / 0 / 0 / 0 | 5.099 / 9.727 / 12.063 | 12.183 / 22.863 / 27.631 | 20.0 | 60 | 23370 | 23779-25488 | - | 98 / 103 / 103 |
| one-doc 40 editors, rate 2, 200 features | 40 | 4800 / 4800 / 0 / 0 / 0 / 0 | 5.839 / 17.039 / 54.943 | 11.367 / 29.055 / 62.111 | 80.0 | 60 | 23370 | 23779-31218 | - | 97 / 108 / 108 |
| one-doc 100 editors, rate 2, 200 features | 100 | 12000 / 12000 / 0 / 0 / 0 / 0 | 531.967 / 590.335 / 612.863 | 2142.21 / 4370.43 / 4407.3 | 186.6 | 64 | 23370 | 23779-42678 | - | 97 / 117 / 118 |
| one-doc 200 editors, rate 2, 200 features | 200 | 24000 / 24000 / 0 / 0 / 0 / 0 | 1851.39 / 3151.87 / 3903.49 | 80543.7 / 169738 / 170525 | 104.1 | 231 | 23370 | 23779-61878 | - | 98 / 133 / 133 |
| one-doc 40 editors, rate 2, 20 features | 40 | 4800 / 4770 / 30 / 0 / 0 / 0 | 5.527 / 16.655 / 52.415 | 11.167 / 28.303 / 62.687 | 79.5 | 60 | 2645 | 3054-10493 | - | 98 / 108 / 108 |
| one-doc 10 editors, rate 20, 200 features | 10 | 12000 / 12000 / 0 / 0 / 0 / 0 | 7.743 / 19.279 / 49.535 | 11.743 / 29.583 / 52.415 | 200.0 | 60 | 23370 | 23779-25488 | - | 97 / 105 / 105 |
| many-docs 200 docs x 3 editors, rate 1 | 600 | 36000 / 36000 / 0 / 0 / 0 / 0 | 1637.38 / 4497.41 / 19071 | 24477.7 / 49381.4 / 53805.1 | 336.7 | 107 | 2645 | 3054-3439 | - | 97 / 198 / 197 |
| open 100 KB x20 | 1 | n/a | 3.053 / 4.403 / 4.403 | - | n/a | 0.2 | 100770 | 101179-101180 | 15.4 | 97 / 97 / 101 |
| open 1 MB x20 | 1 | n/a | 6.459 / 7.651 / 7.651 | - | n/a | 0.2 | 1000770 | 1001179-1001180 | 24.6 | 98 / 98 / 103 |
| open 5 MB x20 | 1 | n/a | 22.911 / 41.087 / 41.087 | - | n/a | 0.7 | 5000770 | 5001179-5001180 | 47.7 | 98 / 108 / 108 |
| idle 500 connections | 500 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 901-96600 | - | 98 / 170 / 170 |
| idle 1000 connections | 1000 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 901-192600 | - | 97 / 252 / 239 |
| idle 2000 connections (FAILED) | 2000 | 0 / 0 / 0 / 0 / 0 / 2000 | - | - | 0.0 | 60 | 492 | 901-385600 | - | 98 / 606 / 364 |

File names in the same order: `onedoc-10`, `onedoc-40`, `onedoc-100`, `onedoc-200`, `onedoc-40-conflict`,
`onedoc-10-rate20`, `manydocs-200x3`, `open-100kb`, `open-1mb`, `open-5mb`, `idle-500`, `idle-1000`,
`idle-2000`. `idle-500` and `idle-1000` were run after `idle-2000` failed, as the fallback sizes. The many-docs
documents have 20 features each (loadgen default). The `open` runs use loadgen's default of 100 features.

## Where it fell over

What was observed:

- **One document, 10 and 40 editors (20 and 80 edits/s offered):** every edit accepted, service p99 under
  20 ms. No problems.
- **One document, 100 editors (200 edits/s offered):** all 12,000 edits were accepted, but service time was
  p50 532 / p99 590 / max 613 ms, the server accepted 187 edits/s instead of 200, and
  the run took 64 s instead of 60. Measured from the schedule, p50 was 2.1 s and max 4.4 s.
- **One document, 200 editors (400 edits/s offered):** all 24,000 edits were accepted, but the server accepted
  104 per second. Service time p50 was 1.85 s (max 3.9 s). The run took 231 s instead of 60; from the
  schedule, p50 was 80.5 s and max 170.5 s. There were no timeouts: each editor has one edit outstanding at a
  time and no answer took 30 s.
- **200 documents x 3 editors at 1 edit/s each (600 connections, 600 edits/s offered):** all 36,000 edits were
  accepted, at 337 per second. Service p50 1.6 s, p99 4.5 s, max 19.1 s; from the schedule p50 24.5 s, max
  53.8 s. The run took 107 s instead of 60. Server memory went from 97 MB to 198 MB.
- **Accepted rate against editors on one document:** 186.6/s at 100 editors (200 offered) and 104.1/s at 200
  editors (400 offered; 24,000 accepted over 230.5 s). The many-docs run (600 connections, 200 documents)
  sustained 336.7/s, more than either of those one-document runs; it still overran, at 107 s. These are
  observations only; no cause is given.
- **Conflicts (40 editors, 20 features):** 30 of 4,800 edits were rejected as `stale`, the rest accepted;
  latency was similar to the 40-editor run with 200 features (p99 16.7 against 17.0 ms), though the
  feature count differs between those two runs (20 against 200).
- **High per-editor rate (10 editors at 20/s = 200 edits/s):** all 12,000 accepted, 200 per second, service
  p99 19 ms. So 200 edits/s on one document was fine from 10 connections and not from 100.
- **Opening documents:** a 100 KB, 1 MB and 5 MB document opened with a warm p50 of 3.1, 6.5 and 22.9 ms and a
  first (cold) open of 15, 25 and 48 ms. The `welcome` frames carried the whole document (5,001,179 bytes for
  5 MB).
- **2000 idle connections: failed.** The result file records `dropped_connections` 2000, `connect_failed` 0,
  duration 60.1 s, and the RSS figures: 98 MB at start, peak 606 MB, 364 MB at the end (loadgen's own sample
  after connecting: 374 MB). At 500 and at 1000 connections nothing was dropped over 60 s. Loadgen cannot say
  when the sockets closed or which side closed them, so the records do not say why.
- **Observed during the run, not preserved in the result files** (noted by the person running it, not
  verifiable from the published files): opening 2000 connections took about 90 s (151 s wall time of the whole
  run minus the 60 s run), against about 25 s for 1000 and about 6 s for 500; the server process was still
  alive and listening after the run; its log had 2000 "connection open" lines and no "connection closed" lines
  when it was stopped.
- Also seen in the idle runs: the `welcome` frame grows with the number of connected peers (largest 96.6 KB
  at 500, 192.6 KB at 1000, 385.6 KB at 2000).

In every run except idle-2000 the result files show no connect failures, timeouts or dropped connections. For
idle-2000 see the two bullets above.

## Server memory

Loadgen reports the server's memory itself only for `idle`, so for every run the `*.server-rss.txt` file holds a
reading taken by the run script: the server (listener) process's Windows working set (the same measure as
`idle`'s `server_rss_bytes`) before loadgen started, the highest value sampled once a second during the run,
and the value at the end. Peaks between samples are not seen. The end value can exceed the peak, because the last reading is taken after
the sampling loop; for runs shorter than a second (the `open` runs) the peak is not meaningful. The `idle` JSON
files also carry loadgen's own `server_rss_bytes`; its before-connect figure differs from the table's before
figure because it is a different reading taken by a different tool at a later moment (after loadgen had
registered and set up). The files are in bytes; the table is in MB
(10^6 bytes). The files are UTF-16 text.

## Caveats

- Loadgen and the server share one machine (16 threads, 15.8 GB); they compete for CPU and the loopback
  interface. Numbers taken on separate machines would differ.
- All peers use one account's token with distinct client ids; per-user costs on the server may be
  under-represented (untested).
- In the unloaded runs the from-schedule figure is 4-7 ms above service time at p50 and 10-13 ms at p99; the
  load generator's timer wake-up lateness (0-15.6 ms on Windows with the default timer resolution) is
  included in it.
- Rejected edits (`stale`) are in neither latency histogram.
- A run can overrun its deadline by any backlog: each editor sends every scheduled slot, so a slow server makes
  the run last longer than 60 s (`run s` shows it), and `accepted per s` is accepted edits over the actual run
  time.
- SQLite only. The PostgreSQL baseline is deliberately not recorded yet: the only PostgreSQL host available to this machine is a
  Raspberry Pi reached over a private network link, which would measure the network rather than the server.
- A single 60 s run per scenario, no repetition, no warm-up phase: small differences between runs are not
  significant. The 2000-connection failure was seen once.
- In `idle` each peer sends presence every 5 s and a ping every 15 s; `open` opens connections one after
  another from one process. Documents are generated by loadgen (flat features with padding strings), not real
  parts.

---

# Rust server 0.2.0-dev: the same scenarios

The baseline's thirteen scenarios, with the baseline's parameters, against the Rust server
(`openswx-collab-server`, crate `oswx-serverd`), measured with the same `loadgen` and its `v2` dialect, on the
same machine. Each scenario was run three times, on SQLite and on PostgreSQL. The reports are in
[`rust-0.2.0-dev-sqlite/`](rust-0.2.0-dev-sqlite/) and [`rust-0.2.0-dev-postgres/`](rust-0.2.0-dev-postgres/):
`<scenario>.run<n>.json` is loadgen's report (the baseline's shape, plus `close_codes`), and `runs.json` holds
what the run script (`bench/run_rust.py`) measured from outside for every run: the server's memory and CPU
time, how busy the machine was, and the warnings and errors in the server's log.

Read "Caveats of the Rust runs" before quoting any number. In particular: **this is a Windows laptop, and on
Windows the server's commits wait for a timer that is about 15 ms late** (see "What the load found", 1). The
server is meant to run on Linux, where this was not measured.

## What was measured

| | |
|---|---|
| Date | 2026-11-26, 17:18 to 19:23 UTC (the night of 26 to 27 November local time) |
| Machine, OS | the baseline's: i7-11800H, 8 cores / 16 threads, 15.8 GB RAM; Windows 11 Education 10.0.22631 |
| Server | `openswx-collab-server` 0.2.0-dev, protocol 2, release build (`cargo build --release`, rustc 1.99.1) of commit `50cdc48`; the server's code was last changed in `9df2410` |
| Loadgen | crate `oswx-loadgen`, release build of commit `50cdc48`, `--dialect v2` |
| SQLite | a fresh database file and storage folder per run, in the system temp directory |
| PostgreSQL | 16.15 on the same machine (the `pixeltable-pgserver` package's binaries, as `tools/test_postgres_rust.py` uses), reached over 127.0.0.1; its defaults: `shared_buffers` 128MB, `fsync` on, `synchronous_commit` on, `wal_sync_method` open_datasync, `max_connections` 100. One instance per batch of runs, a fresh database in it and a fresh storage folder per run |
| Server settings | defaults, apart from the database and storage locations and **`OSWX_COLLAB_MAX_CONNECTIONS_PER_USER=20000`** (default 64); `serve --host 127.0.0.1 --port 8791`, one process. So: PostgreSQL pool 10 connections for requests and 2 for the journal; journal batches of up to 256 changes, waiting 2 ms for more (compiled in); presence limit 60 messages a second per connection; log level `info`, to a file |
| Runs | every run had a server of its own (started, used once, killed), 60 s; 5 s pause between runs |

**The one setting that was raised.** Loadgen opens every connection as one account, as it did for the
baseline. The Python server has no limit on that; the Rust server allows an account 64 document connections
and refuses the next (`too_many_connections`, close 4429). At the default, every scenario with more than 64
connections would have had the rest refused, so the limit was raised from 64 to 20,000 (312 times) for all
runs. No other limit was touched: sign-in limits do not matter here (one registration per server), and no
presence or lock limit was reached (no `error` message arrived in any run: `protocol_errors` is 0 everywhere).

**What else was running.** Nothing was started for the measurements besides the server, loadgen, PostgreSQL
and the run script. The desktop session stayed open: a browser, Discord, Spotify, Windows Defender (real-time
scanning on), and the Claude Code session that drove the runs, which waited while a run was in progress (it
read a few small files during some of them). The machine's CPU use in the two seconds before each run was
1.0 to 1.1 % on average and at most 5.4 % (`machine_cpu_percent_before` in `runs.json`). No build or test ran
during a measurement.

## Python and Rust side by side

The baseline's single run next to the **median run** of the three Rust runs (the middle one by service p99;
for `idle`, by the server's peak memory). Service time is p50 / p99 / max in milliseconds, as in the baseline:
from just before an edit is sent until its `change.accepted` arrives (for `open`: `hello` to `welcome`, the 19
warm opens). *accepted per s (run s)* is accepted edits over the measured run time, and that run time.

| Scenario | Python accepted / rejected | Python service p50 / p99 / max | Python accepted per s (run s) | Python peak RSS MB | Rust sqlite accepted / rejected | Rust sqlite service p50 / p99 / max | Rust sqlite accepted per s (run s) | Rust sqlite peak RSS MB | Rust postgres accepted / rejected | Rust postgres service p50 / p99 / max | Rust postgres accepted per s (run s) | Rust postgres peak RSS MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| onedoc-10 | 1200 / 0 | 5.099 / 9.727 / 12.063 | 20.0 (60) | 103 | 1200 / 0 | 18.623 / 21.615 / 24.575 | 20.0 (60) | 25 | 1200 / 0 | 17.167 / 18.847 / 20.463 | 20.0 (60) | 23 |
| onedoc-40 | 4800 / 0 | 5.839 / 17.039 / 54.943 | 80.0 (60) | 108 | 4800 / 0 | 18.431 / 21.935 / 34.303 | 80.0 (60) | 36 | 4800 / 0 | 17.167 / 19.007 / 80.639 | 80.0 (60) | 32 |
| onedoc-100 | 12000 / 0 | 531.967 / 590.335 / 612.863 | 186.6 (64) | 117 | 12000 / 0 | 19.151 / 34.559 / 43.807 | 199.9 (60) | 49 | 12000 / 0 | 17.727 / 20.031 / 55.167 | 199.9 (60) | 46 |
| onedoc-200 | 24000 / 0 | 1851 / 3152 / 3903 | 104.1 (231) | 133 | 24000 / 0 | 16.703 / 39.615 / 48.511 | 399.8 (60) | 75 | 24000 / 0 | 15.711 / 23.167 / 64.927 | 399.7 (60) | 67 |
| onedoc-40-conflict | 4770 / 30 | 5.527 / 16.655 / 52.415 | 79.5 (60) | 108 | 4560 / 240 | 17.071 / 20.463 / 25.455 | 76.0 (60) | 33 | 4511 / 289 | 17.279 / 18.975 / 25.247 | 75.2 (60) | 28 |
| onedoc-10-rate20 | 12000 / 0 | 7.743 / 19.279 / 49.535 | 200.0 (60) | 105 | 12000 / 0 | 17.535 / 23.663 / 32.367 | 199.9 (60) | 31 | 12000 / 0 | 17.167 / 18.767 / 55.615 | 200.0 (60) | 26 |
| manydocs-200x3 | 36000 / 0 | 1637 / 4497 / 19071 | 336.7 (107) | 198 | 36000 / 0 | 19.135 / 48.767 / 74.175 | 599.3 (60) | 170 | 36000 / 0 | 18.671 / 21.919 / 73.855 | 599.4 (60) | 165 |
| open-100kb | n/a | 3.053 / 4.403 / 4.403 | n/a (0.2) | 97 | n/a | 1.562 / 1.772 / 1.772 | n/a (0.0) | 20 | n/a | 1.952 / 4.021 / 4.021 | n/a (0.1) | 18 |
| open-1mb | n/a | 6.459 / 7.651 / 7.651 | n/a (0.2) | 98 | n/a | 3.825 / 4.455 / 4.455 | n/a (0.1) | 21 | n/a | 4.243 / 20.655 / 20.655 | n/a (0.2) | 20 |
| open-5mb | n/a | 22.911 / 41.087 / 41.087 | n/a (0.7) | 108 | n/a | 16.559 / 36.063 / 36.063 | n/a (0.5) | 25 | n/a | 18.223 / 67.967 / 67.967 | n/a (0.6) | 24 |
| idle-500 | 0 / 0 | - | 0.0 (60) | 170 | 0 / 0 | - | 0.0 (60) | 116 | 0 / 0 | - | 0.0 (60) | 114 |
| idle-1000 | 0 / 0 | - | 0.0 (60) | 252 | 0 / 0 | - | 0.0 (60) | 248 | 0 / 0 | - | 0.0 (60) | 244 |
| idle-2000 | 0 / 0 (FAILED: 2000 dropped) | - | 0.0 (60) | 606 | 0 / 0 (1 of 3 runs FAILED) | - | 0.0 (60) | 706 | 0 / 0 (1 of 3 runs FAILED) | - | 0.0 (60) | 699 |

In short, for this machine and these runs:

- **Light load is slower than Python.** With 10 or 40 editors the Python server answered an edit in 5 to 6 ms
  (p50); the Rust server took 17 to 19 ms, on both databases, in every run. p99 is 19 to 22 ms against
  Python's 10 and 17 ms. The cause is most likely the timer described under "What the load found", 1.
- **Heavy load is where it differs the other way.** 100 and 200 editors on one document, and 600 connections
  on 200 documents, overran in the baseline (187, 104 and 337 edits a second accepted; p50 of 0.5, 1.9 and
  1.6 s). The Rust server accepted everything offered (200, 400 and 600 a second) within the 60 s, with p50
  14 to 21 ms and p99 20 to 59 ms, in all nine such runs on each database.
- **Opening a document** took 1.6 / 3.8 / 16.6 ms (100 KB / 1 MB / 5 MB, warm p50, SQLite median runs)
  against Python's 3.1 / 6.5 / 22.9 ms. The first open on a fresh server took 4 to 41 ms on SQLite and 52 to
  109 ms on PostgreSQL (Python on SQLite: 15 to 48 ms).
- **Conflicts.** With 40 editors on 20 features the Rust server refused 118 to 289 of 4,800 edits as `stale`
  (six runs); the Python server refused 30. See "How the runs differ from the baseline's".
- **Memory.** The server starts at 15 to 16 MB (Python: 97 MB). Its peak was below Python's in every
  scenario except `idle` with 1,000 connections (about the same: 243 to 249 MB against 252 MB) and with 2,000
  (646 to 719 MB against 606 MB).
- **2,000 idle connections**: not dropped wholesale as in the baseline, but not clean either. Two of the six
  runs lost connections (66 and 28 of 2,000, closed by the server as too slow, code 4408), and in all six the
  server used about 11.5 CPU cores for the whole minute. See "What the load found", 2.

## Results on SQLite (median runs)

Columns as in the baseline's table. *RSS MB* is the server's working set before loadgen started / the highest
once-a-second sample / after loadgen had exited and its connections were closed (the Rust server gives memory
back, so the last figure is not what it held during the run; the `idle` reports' own `server_rss_bytes.end`
is taken with the connections still open). *server CPU s* is the server process's CPU time (user and kernel)
from before loadgen started until after it exited: setup, connecting and the run together. The baseline did
not record CPU time. On PostgreSQL the database's own processes are in neither figure.

| Run | connections | changes (sent / accepted / rejected / timeouts / errors / dropped) | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | doc bytes | welcome bytes | first open | RSS MB | server CPU s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| one-doc 10 editors, rate 2, 200 features | 10 | 1200 / 1200 / 0 / 0 / 0 / 0 | 18.623 / 21.615 / 24.575 | 27.007 / 35.711 / 40.703 | 20.0 | 60 | 23370 | 18284-19436 | - | 16 / 25 / 24 | 0.16 |
| one-doc 40 editors, rate 2, 200 features | 40 | 4800 / 4800 / 0 / 0 / 0 / 0 | 18.431 / 21.935 / 34.303 | 24.847 / 34.911 / 43.775 | 80.0 | 60 | 23370 | 18284-23308 | - | 16 / 36 / 28 | 0.64 |
| one-doc 100 editors, rate 2, 200 features | 100 | 12000 / 12000 / 0 / 0 / 0 / 0 | 19.151 / 34.559 / 43.807 | 23.887 / 40.991 / 53.631 | 199.9 | 60 | 23370 | 18285-31048 | - | 16 / 49 / 33 | 2.25 |
| one-doc 200 editors, rate 2, 200 features | 200 | 24000 / 24000 / 0 / 0 / 0 / 0 | 16.703 / 39.615 / 48.511 | 19.919 / 44.575 / 59.679 | 399.8 | 60 | 23370 | 18284-44048 | - | 15 / 75 / 42 | 9.03 |
| one-doc 40 editors, rate 2, 20 features | 40 | 4800 / 4560 / 240 / 0 / 0 / 0 | 17.071 / 20.463 / 25.455 | 23.951 / 33.695 / 39.903 | 76.0 | 60 | 2645 | 2218-7242 | - | 16 / 33 / 27 | 0.52 |
| one-doc 10 editors, rate 20, 200 features | 10 | 12000 / 12000 / 0 / 0 / 0 / 0 | 17.535 / 23.663 / 32.367 | 24.719 / 34.399 / 46.207 | 199.9 | 60 | 23370 | 18284-19436 | - | 16 / 31 / 28 | 0.72 |
| many-docs 200 docs x 3 editors, rate 1 | 600 | 36000 / 36000 / 0 / 0 / 0 / 0 | 19.135 / 48.767 / 74.175 | 24.799 / 56.863 / 83.199 | 599.3 | 60 | 2645 | 2218-2480 | - | 16 / 170 / 89 | 1.52 |
| open 100 KB x20 | 1 | n/a | 1.562 / 1.772 / 1.772 | - | n/a | 0.0 | 100770 | 97982-97983 | 4.0 | 16 / 20 / 20 | 0.05 |
| open 1 MB x20 | 1 | n/a | 3.825 / 4.455 / 4.455 | - | n/a | 0.1 | 1000770 | 997982-997983 | 8.9 | 16 / 21 / 21 | 0.05 |
| open 5 MB x20 | 1 | n/a | 16.559 / 36.063 / 36.063 | - | n/a | 0.5 | 5000770 | 4997982-4997983 | 25.7 | 16 / 25 / 25 | 0.11 |
| idle 500 connections | 500 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 579-65342 | - | 16 / 116 / 27 | 7.06 |
| idle 1000 connections | 1000 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 579-130342 | - | 16 / 248 / 31 | 21.06 |
| idle 2000 connections (1 of 3 runs FAILED) | 2000 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 579-261342 | - | 15 / 706 / 37 | 693.94 |

## Results on PostgreSQL (median runs)

| Run | connections | changes (sent / accepted / rejected / timeouts / errors / dropped) | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | doc bytes | welcome bytes | first open | RSS MB | server CPU s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| one-doc 10 editors, rate 2, 200 features | 10 | 1200 / 1200 / 0 / 0 / 0 / 0 | 17.167 / 18.847 / 20.463 | 24.863 / 33.791 / 34.975 | 20.0 | 60 | 23370 | 18284-19436 | - | 15 / 23 / 21 | 0.09 |
| one-doc 40 editors, rate 2, 200 features | 40 | 4800 / 4800 / 0 / 0 / 0 / 0 | 17.167 / 19.007 / 80.639 | 23.999 / 32.959 / 81.215 | 80.0 | 60 | 23370 | 18285-23308 | - | 15 / 32 / 24 | 0.5 |
| one-doc 100 editors, rate 2, 200 features | 100 | 12000 / 12000 / 0 / 0 / 0 / 0 | 17.727 / 20.031 / 55.167 | 23.215 / 32.287 / 61.055 | 199.9 | 60 | 23370 | 18285-31048 | - | 15 / 46 / 30 | 2.58 |
| one-doc 200 editors, rate 2, 200 features | 200 | 24000 / 24000 / 0 / 0 / 0 / 0 | 15.711 / 23.167 / 64.927 | 18.223 / 33.567 / 68.799 | 399.7 | 60 | 23370 | 18285-44048 | - | 15 / 67 / 33 | 8.42 |
| one-doc 40 editors, rate 2, 20 features | 40 | 4800 / 4511 / 289 / 0 / 0 / 0 | 17.279 / 18.975 / 25.247 | 24.607 / 33.535 / 35.071 | 75.2 | 60 | 2645 | 2218-7242 | - | 15 / 28 / 23 | 0.38 |
| one-doc 10 editors, rate 20, 200 features | 10 | 12000 / 12000 / 0 / 0 / 0 / 0 | 17.167 / 18.767 / 55.615 | 23.791 / 32.287 / 67.519 | 200.0 | 60 | 23370 | 18284-19436 | - | 15 / 26 / 25 | 0.33 |
| many-docs 200 docs x 3 editors, rate 1 | 600 | 36000 / 36000 / 0 / 0 / 0 / 0 | 18.671 / 21.919 / 73.855 | 23.823 / 33.119 / 84.671 | 599.4 | 60 | 2645 | 2218-2480 | - | 15 / 165 / 86 | 1.05 |
| open 100 KB x20 | 1 | n/a | 1.952 / 4.021 / 4.021 | - | n/a | 0.1 | 100770 | 97982-97983 | 52.4 | 15 / 18 / 18 | 0.05 |
| open 1 MB x20 | 1 | n/a | 4.243 / 20.655 / 20.655 | - | n/a | 0.2 | 1000770 | 997982-997983 | 54.2 | 15 / 20 / 20 | 0.11 |
| open 5 MB x20 | 1 | n/a | 18.223 / 67.967 / 67.967 | - | n/a | 0.6 | 5000770 | 4997982-4997983 | 89.5 | 15 / 24 / 24 | 0.19 |
| idle 500 connections | 500 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 578-65342 | - | 15 / 114 / 25 | 6.53 |
| idle 1000 connections | 1000 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 579-130342 | - | 15 / 244 / 29 | 21.91 |
| idle 2000 connections (1 of 3 runs FAILED) | 2000 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 578-261342 | - | 15 / 699 / 36 | 686.55 |

File names: `onedoc-10`, `onedoc-40`, `onedoc-100`, `onedoc-200`, `onedoc-40-conflict`, `onedoc-10-rate20`,
`manydocs-200x3`, `open-100kb`, `open-1mb`, `open-5mb`, `idle-500`, `idle-1000`, `idle-2000`, each with
`.run1.json` to `.run3.json`.

## Every run

*refused connections* is `connect_failed`. *busiest second* is the server's highest CPU use in a one-second
sample, in percent of one core (1,600 would be the whole machine). *machine CPU %* is the whole machine's:
in the two seconds before the run / mean / highest one-second sample during it, loadgen and everything else
included. *server log* counts the `WARN` and `ERROR` lines the server wrote. An `x` marks the median run shown
above. A run is marked FAILED when any connection was dropped or refused, any edit timed out or was lost, a
protocol error was counted, loadgen wrote no report, or the server died.

### SQLite

| Scenario | run | changes (sent / accepted / rejected / timeouts / errors / dropped) | refused connections | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | first open | RSS MB | server CPU s | busiest second, % of a core | machine CPU % before / mean / max | server log warnings / errors | median |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| onedoc-10 | 1 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 18.591 / 21.887 / 23.823 | 26.447 / 35.711 / 38.047 | 20.0 | 60.0 | - | 16 / 25 / 24 | 0.17 | 6 | 0.6 / 0.8 / 3.6 | 0 / 0 |  |
| onedoc-10 | 2 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 18.623 / 21.615 / 24.575 | 27.007 / 35.711 / 40.703 | 20.0 | 60.0 | - | 16 / 25 / 24 | 0.16 | 5 | 0.8 / 0.8 / 3.5 | 0 / 0 | x |
| onedoc-10 | 3 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 18.543 / 21.359 / 24.655 | 26.687 / 35.071 / 35.647 | 20.0 | 59.9 | - | 16 / 25 / 24 | 0.12 | 3 | 1.2 / 0.9 / 3.2 | 0 / 0 |  |
| onedoc-40 | 1 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 18.431 / 21.583 / 39.999 | 24.543 / 34.879 / 51.871 | 80.0 | 60.0 | - | 15 / 36 / 29 | 0.47 | 6 | 0.8 / 0.9 / 3.2 | 0 / 0 |  |
| onedoc-40 | 2 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 18.431 / 21.935 / 34.303 | 24.847 / 34.911 / 43.775 | 80.0 | 60.0 | - | 16 / 36 / 28 | 0.64 | 9 | 0.8 / 0.9 / 3.3 | 0 / 0 | x |
| onedoc-40 | 3 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 18.335 / 22.287 / 37.375 | 24.111 / 34.303 / 45.343 | 80.0 | 60.0 | - | 15 / 35 / 28 | 0.53 | 6 | 1.1 / 1.1 / 6.8 | 0 / 0 |  |
| onedoc-100 | 1 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 19.151 / 34.559 / 43.807 | 23.887 / 40.991 / 53.631 | 199.9 | 60.0 | - | 16 / 49 / 33 | 2.25 | 28 | 2.8 / 1.1 / 5.0 | 0 / 0 | x |
| onedoc-100 | 2 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 19.103 / 34.783 / 44.031 | 23.791 / 39.679 / 54.239 | 199.9 | 60.0 | - | 16 / 51 / 35 | 2.69 | 23 | 1.2 / 1.5 / 5.4 | 0 / 0 |  |
| onedoc-100 | 3 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 19.135 / 33.695 / 44.031 | 23.759 / 40.095 / 55.615 | 199.9 | 60.0 | - | 15 / 49 / 33 | 1.89 | 23 | 1.1 / 1.2 / 4.4 | 0 / 0 |  |
| onedoc-200 | 1 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 16.927 / 40.095 / 54.143 | 20.159 / 44.831 / 57.919 | 399.6 | 60.1 | - | 16 / 73 / 39 | 9.5 | 62 | 0.9 / 2.3 / 5.2 | 0 / 0 |  |
| onedoc-200 | 2 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 16.327 / 35.327 / 45.855 | 19.311 / 40.031 / 57.311 | 399.8 | 60.0 | - | 15 / 75 / 41 | 9.08 | 54 | 0.7 / 2.4 / 7.1 | 0 / 0 |  |
| onedoc-200 | 3 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 16.703 / 39.615 / 48.511 | 19.919 / 44.575 / 59.679 | 399.8 | 60.0 | - | 15 / 75 / 42 | 9.03 | 43 | 0.6 / 2.2 / 5.1 | 0 / 0 | x |
| onedoc-40-conflict | 1 | 4800 / 4593 / 207 / 0 / 0 / 0 | 0 | 17.903 / 23.247 / 25.967 | 24.495 / 34.399 / 39.679 | 76.6 | 60.0 | - | 15 / 32 / 27 | 0.42 | 6 | 0.7 / 1.1 / 5.7 | 0 / 0 |  |
| onedoc-40-conflict | 2 | 4800 / 4651 / 149 / 0 / 0 / 0 | 0 | 17.007 / 19.343 / 24.415 | 24.159 / 33.247 / 38.143 | 77.5 | 60.0 | - | 16 / 33 / 28 | 0.47 | 6 | 1.0 / 0.9 / 3.3 | 0 / 0 |  |
| onedoc-40-conflict | 3 | 4800 / 4560 / 240 / 0 / 0 / 0 | 0 | 17.071 / 20.463 / 25.455 | 23.951 / 33.695 / 39.903 | 76.0 | 60.0 | - | 16 / 33 / 27 | 0.52 | 5 | 0.7 / 1.0 / 3.7 | 0 / 0 | x |
| onedoc-10-rate20 | 1 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 17.599 / 24.623 / 29.087 | 25.119 / 34.591 / 43.615 | 200.0 | 60.0 | - | 16 / 31 / 28 | 0.59 | 8 | 0.6 / 1.0 / 3.5 | 0 / 0 |  |
| onedoc-10-rate20 | 2 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 17.535 / 23.663 / 32.367 | 24.719 / 34.399 / 46.207 | 199.9 | 60.0 | - | 16 / 31 / 28 | 0.72 | 8 | 0.6 / 1.0 / 3.3 | 0 / 0 | x |
| onedoc-10-rate20 | 3 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 17.151 / 23.183 / 33.951 | 23.647 / 32.639 / 44.543 | 199.9 | 60.0 | - | 16 / 31 / 29 | 0.56 | 5 | 1.0 / 1.0 / 3.6 | 0 / 0 |  |
| manydocs-200x3 | 1 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 19.135 / 48.767 / 74.175 | 24.799 / 56.863 / 83.199 | 599.3 | 60.1 | - | 16 / 170 / 89 | 1.52 | 22 | 0.8 / 1.4 / 4.1 | 0 / 0 | x |
| manydocs-200x3 | 2 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 20.495 / 58.495 / 95.679 | 25.407 / 64.703 / 99.135 | 599.5 | 60.0 | - | 16 / 171 / 89 | 2.12 | 12 | 0.7 / 1.2 / 3.8 | 0 / 0 |  |
| manydocs-200x3 | 3 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 19.343 / 42.655 / 94.527 | 24.751 / 51.583 / 107.711 | 599.1 | 60.1 | - | 16 / 170 / 92 | 2.08 | 30 | 0.5 / 1.4 / 4.5 | 0 / 0 |  |
| open-100kb | 1 | n/a | 0 | 1.537 / 1.998 / 1.998 | - | n/a | 0.0 | 6.6 | 16 / 20 / 20 | 0.06 | 6 | 0.5 / 3.5 / 3.5 | 0 / 0 |  |
| open-100kb | 2 | n/a | 0 | 1.562 / 1.772 / 1.772 | - | n/a | 0.0 | 4.0 | 16 / 20 / 20 | 0.05 | 5 | 1.4 / 2.0 / 2.0 | 0 / 0 | x |
| open-100kb | 3 | n/a | 0 | 1.542 / 1.668 / 1.668 | - | n/a | 0.0 | 4.3 | 16 / 20 / 20 | 0.06 | 6 | 2.1 / 2.6 / 2.6 | 0 / 0 |  |
| open-1mb | 1 | n/a | 0 | 3.825 / 4.455 / 4.455 | - | n/a | 0.1 | 8.9 | 16 / 21 / 21 | 0.05 | 5 | 0.9 / 1.9 / 1.9 | 0 / 0 | x |
| open-1mb | 2 | n/a | 0 | 3.999 / 14.487 / 14.487 | - | n/a | 0.1 | 8.3 | 16 / 21 / 21 | 0.02 | 2 | 0.8 / 2.6 / 2.6 | 0 / 0 |  |
| open-1mb | 3 | n/a | 0 | 3.589 / 3.989 / 3.989 | - | n/a | 0.1 | 7.8 | 16 / 21 / 21 | 0.02 | 2 | 0.8 / 2.1 / 2.1 | 0 / 0 |  |
| open-5mb | 1 | n/a | 0 | 16.639 / 33.471 / 33.471 | - | n/a | 0.5 | 41.4 | 15 / 26 / 26 | 0.05 | 5 | 1.2 / 2.3 / 2.3 | 0 / 0 |  |
| open-5mb | 2 | n/a | 0 | 17.519 / 45.887 / 45.887 | - | n/a | 0.6 | 25.6 | 16 / 26 / 26 | 0.08 | 8 | 1.1 / 1.9 / 3.3 | 0 / 0 |  |
| open-5mb | 3 | n/a | 0 | 16.559 / 36.063 / 36.063 | - | n/a | 0.5 | 25.7 | 16 / 25 / 25 | 0.11 | 11 | 0.9 / 2.7 / 2.7 | 0 / 0 | x |
| idle-500 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 16 / 116 / 27 | 7.06 | 54 | 1.3 / 2.3 / 5.2 | 0 / 0 | x |
| idle-500 | 2 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 16 / 116 / 26 | 7.0 | 51 | 0.9 / 2.2 / 5.2 | 0 / 0 |  |
| idle-500 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 16 / 118 / 27 | 6.19 | 41 | 0.6 / 2.2 / 5.4 | 0 / 0 |  |
| idle-1000 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.2 | - | 16 / 246 / 31 | 22.3 | 97 | 1.0 / 5.1 / 15.3 | 0 / 0 |  |
| idle-1000 | 2 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.2 | - | 16 / 248 / 31 | 21.06 | 116 | 1.3 / 5.1 / 16.5 | 0 / 0 | x |
| idle-1000 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.2 | - | 16 / 249 / 33 | 26.0 | 123 | 1.2 / 5.5 / 17.2 | 0 / 0 |  |
| idle-2000 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.4 | - | 16 / 664 / 38 | 687.66 | 1169 | 0.9 / 85.9 / 100.0 | 0 / 0 |  |
| idle-2000 (FAILED: 66 connections dropped) | 2 | 0 / 0 / 0 / 0 / 0 / 66 | 0 | - | - | 0.0 | 60.3 | - | 15 / 719 / 39 | 706.28 | 1178 | 0.6 / 87.7 / 100.0 | 0 / 0 |  |
| idle-2000 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.4 | - | 15 / 706 / 37 | 693.94 | 1197 | 0.7 / 86.2 / 100.0 | 0 / 0 | x |

### PostgreSQL

| Scenario | run | changes (sent / accepted / rejected / timeouts / errors / dropped) | refused connections | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | first open | RSS MB | server CPU s | busiest second, % of a core | machine CPU % before / mean / max | server log warnings / errors | median |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| onedoc-10 | 1 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 17.167 / 18.847 / 20.463 | 24.863 / 33.791 / 34.975 | 20.0 | 60.0 | - | 15 / 23 / 21 | 0.09 | 6 | 3.2 / 1.7 / 5.8 | 0 / 0 | x |
| onedoc-10 | 2 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 17.199 / 18.751 / 47.391 | 25.375 / 34.047 / 59.167 | 20.0 | 60.0 | - | 15 / 23 / 21 | 0.11 | 5 | 0.6 / 1.0 / 3.7 | 0 / 0 |  |
| onedoc-10 | 3 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 17.199 / 18.927 / 23.295 | 25.151 / 33.759 / 35.519 | 20.0 | 60.0 | - | 15 / 23 / 21 | 0.08 | 5 | 0.4 / 1.1 / 5.0 | 0 / 0 |  |
| onedoc-40 | 1 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 17.167 / 19.007 / 80.639 | 23.999 / 32.959 / 81.215 | 80.0 | 60.0 | - | 15 / 32 / 24 | 0.5 | 8 | 1.0 / 1.0 / 3.8 | 0 / 0 | x |
| onedoc-40 | 2 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 17.263 / 19.087 / 21.727 | 24.239 / 33.311 / 37.247 | 80.0 | 60.0 | - | 15 / 32 / 25 | 0.5 | 6 | 0.6 / 1.1 / 3.8 | 0 / 0 |  |
| onedoc-40 | 3 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 17.199 / 18.927 / 25.503 | 23.983 / 32.991 / 35.007 | 80.0 | 60.0 | - | 15 / 32 / 25 | 0.36 | 8 | 0.8 / 1.0 / 2.8 | 0 / 0 |  |
| onedoc-100 | 1 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 17.759 / 20.127 / 50.879 | 23.343 / 32.671 / 51.615 | 199.9 | 60.0 | - | 15 / 45 / 28 | 2.38 | 22 | 0.6 / 1.3 / 4.8 | 0 / 0 |  |
| onedoc-100 | 2 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 17.695 / 19.935 / 50.143 | 23.103 / 32.239 / 51.679 | 199.9 | 60.0 | - | 15 / 46 / 28 | 2.31 | 23 | 0.7 / 1.3 / 4.6 | 0 / 0 |  |
| onedoc-100 | 3 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 17.727 / 20.031 / 55.167 | 23.215 / 32.287 / 61.055 | 199.9 | 60.0 | - | 15 / 46 / 30 | 2.58 | 43 | 2.2 / 1.3 / 6.3 | 0 / 0 | x |
| onedoc-200 | 1 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 15.599 / 23.663 / 68.735 | 18.463 / 34.303 / 69.631 | 399.7 | 60.0 | - | 15 / 67 / 33 | 9.23 | 65 | 1.0 / 2.5 / 8.0 | 0 / 0 |  |
| onedoc-200 | 2 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 14.447 / 22.687 / 55.199 | 17.999 / 33.535 / 59.871 | 399.8 | 60.0 | - | 15 / 67 / 33 | 9.11 | 54 | 1.7 / 2.5 / 6.2 | 0 / 0 |  |
| onedoc-200 | 3 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 15.711 / 23.167 / 64.927 | 18.223 / 33.567 / 68.799 | 399.7 | 60.0 | - | 15 / 67 / 33 | 8.42 | 42 | 0.7 / 2.3 / 5.4 | 0 / 0 | x |
| onedoc-40-conflict | 1 | 4800 / 4680 / 120 / 0 / 0 / 0 | 0 | 17.279 / 19.055 / 23.663 | 24.255 / 33.503 / 36.095 | 78.0 | 60.0 | - | 15 / 29 / 23 | 0.41 | 8 | 0.5 / 0.9 / 3.2 | 0 / 0 |  |
| onedoc-40-conflict | 2 | 4800 / 4682 / 118 / 0 / 0 / 0 | 0 | 17.135 / 18.879 / 24.959 | 23.743 / 33.119 / 35.647 | 78.0 | 60.0 | - | 15 / 29 / 23 | 0.53 | 8 | 1.2 / 0.9 / 3.0 | 0 / 0 |  |
| onedoc-40-conflict | 3 | 4800 / 4511 / 289 / 0 / 0 / 0 | 0 | 17.279 / 18.975 / 25.247 | 24.607 / 33.535 / 35.071 | 75.2 | 60.0 | - | 15 / 28 / 23 | 0.38 | 6 | 0.7 / 1.0 / 3.5 | 0 / 0 | x |
| onedoc-10-rate20 | 1 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 17.071 / 18.687 / 25.391 | 23.551 / 31.967 / 38.751 | 199.9 | 60.0 | - | 15 / 26 / 25 | 0.2 | 3 | 0.9 / 1.1 / 4.1 | 0 / 0 |  |
| onedoc-10-rate20 | 2 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 17.167 / 18.767 / 55.615 | 23.791 / 32.287 / 67.519 | 200.0 | 60.0 | - | 15 / 26 / 25 | 0.33 | 5 | 0.8 / 1.0 / 3.0 | 0 / 0 | x |
| onedoc-10-rate20 | 3 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 17.215 / 18.927 / 24.815 | 23.855 / 32.351 / 35.103 | 200.0 | 60.0 | - | 15 / 26 / 25 | 0.33 | 5 | 0.7 / 1.0 / 5.1 | 0 / 0 |  |
| manydocs-200x3 | 1 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 18.703 / 21.999 / 53.663 | 23.791 / 33.119 / 64.863 | 599.4 | 60.1 | - | 15 / 165 / 85 | 1.17 | 28 | 1.7 / 1.3 / 3.8 | 0 / 0 |  |
| manydocs-200x3 | 2 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 18.719 / 21.567 / 54.015 | 23.839 / 33.055 / 67.071 | 599.5 | 60.0 | - | 15 / 165 / 85 | 0.77 | 11 | 0.4 / 1.1 / 3.3 | 0 / 0 |  |
| manydocs-200x3 | 3 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 18.671 / 21.919 / 73.855 | 23.823 / 33.119 / 84.671 | 599.4 | 60.1 | - | 15 / 165 / 86 | 1.05 | 11 | 0.5 / 1.2 / 6.1 | 0 / 0 | x |
| open-100kb | 1 | n/a | 0 | 2.055 / 3.821 / 3.821 | - | n/a | 0.1 | 53.9 | 15 / 18 / 18 | 0.05 | 5 | 0.7 / 2.6 / 2.6 | 0 / 0 |  |
| open-100kb | 2 | n/a | 0 | 1.92 / 4.179 / 4.179 | - | n/a | 0.1 | 56.2 | 15 / 18 / 18 | 0.05 | 5 | 2.0 / 3.9 / 3.9 | 0 / 0 |  |
| open-100kb | 3 | n/a | 0 | 1.952 / 4.021 / 4.021 | - | n/a | 0.1 | 52.4 | 15 / 18 / 18 | 0.05 | 5 | 1.4 / 2.9 / 2.9 | 0 / 0 | x |
| open-1mb | 1 | n/a | 0 | 4.611 / 43.903 / 43.903 | - | n/a | 0.3 | 56.8 | 15 / 20 / 20 | 0.05 | 5 | 2.0 / 4.1 / 4.1 | 0 / 0 |  |
| open-1mb | 2 | n/a | 0 | 4.191 / 6.175 / 6.175 | - | n/a | 0.2 | 55.8 | 15 / 20 / 20 | 0.06 | 6 | 0.8 / 2.3 / 2.3 | 0 / 0 |  |
| open-1mb | 3 | n/a | 0 | 4.243 / 20.655 / 20.655 | - | n/a | 0.2 | 54.2 | 15 / 20 / 20 | 0.11 | 11 | 1.0 / 2.9 / 2.9 | 0 / 0 | x |
| open-5mb | 1 | n/a | 0 | 22.639 / 56.575 / 56.575 | - | n/a | 0.6 | 108.5 | 15 / 24 / 24 | 0.06 | 6 | 0.9 / 3.8 / 5.9 | 0 / 0 |  |
| open-5mb | 2 | n/a | 0 | 18.431 / 76.735 / 76.735 | - | n/a | 0.7 | 92.6 | 15 / 24 / 24 | 0.08 | 8 | 0.6 / 2.9 / 4.6 | 0 / 0 |  |
| open-5mb | 3 | n/a | 0 | 18.223 / 67.967 / 67.967 | - | n/a | 0.6 | 89.5 | 15 / 24 / 24 | 0.19 | 19 | 0.5 / 2.4 / 3.9 | 0 / 0 | x |
| idle-500 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 15 / 114 / 25 | 6.53 | 42 | 1.0 / 2.3 / 7.4 | 0 / 0 | x |
| idle-500 | 2 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 15 / 114 / 25 | 8.08 | 78 | 0.6 / 2.3 / 6.5 | 0 / 0 |  |
| idle-500 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 15 / 114 / 25 | 6.83 | 92 | 0.8 / 2.2 / 7.9 | 0 / 0 |  |
| idle-1000 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.2 | - | 15 / 245 / 29 | 22.56 | 95 | 0.7 / 5.1 / 14.0 | 0 / 0 |  |
| idle-1000 | 2 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.2 | - | 15 / 244 / 29 | 21.91 | 68 | 0.6 / 4.8 / 9.8 | 0 / 0 | x |
| idle-1000 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.2 | - | 15 / 243 / 29 | 25.06 | 174 | 0.6 / 5.0 / 24.2 | 0 / 0 |  |
| idle-2000 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.4 | - | 15 / 646 / 36 | 691.27 | 1172 | 0.5 / 86.2 / 100.0 | 0 / 0 |  |
| idle-2000 (FAILED: 28 connections dropped) | 2 | 0 / 0 / 0 / 0 / 0 / 28 | 0 | - | - | 0.0 | 60.3 | - | 15 / 704 / 37 | 694.05 | 1166 | 0.4 / 84.9 / 100.0 | 0 / 0 |  |
| idle-2000 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.3 | - | 15 / 699 / 36 | 686.55 | 1158 | 0.4 / 84.9 / 100.0 | 0 / 0 | x |

Run-to-run spread: p50 moved by less than 1.5 ms between the three runs of any edit scenario. p99 moved by up
to 5 ms on one document, and from 43 to 58 ms for `manydocs-200x3` on SQLite. The maximum is one sample and
moved more (for example 20, 47 and 23 ms for `onedoc-10` on PostgreSQL). `open` percentiles come from 19
samples, so their p99 is the maximum and one slow open moves it (4.5, 14.5 and 4.0 ms for 1 MB on SQLite).

## How the runs differ from the baseline's

The scenarios, client counts, rates, durations, document generator, what is timed and the timing code are the
baseline's. What is not the same:

- **Three runs, not one**, and the tables show the median run.
- **The protocol.** v2 frames are MessagePack, so `welcome bytes` is smaller than the JSON `doc bytes` (v1:
  larger). An edit is one `Set` of `features.<id>.params.depth` on the newest seq the connection has heard of;
  in v1 it is a `feature.update` of the same field. None of these scenarios creates an item, so v1's
  `ids.reserve` and v2's client-made ids are exercised by neither.
- **Conflicts are judged per path, not per feature.** In the conflict scenario two editors share a feature and
  both write its `params.depth`, the same path, so both rules call the same pairs of edits conflicting. The
  count still differs (118 to 289 `stale` against 30). An edit is refused when the other editor's edit is
  accepted between the moment it was made and the moment it is judged, and that window grows with the service
  time, 17 ms here against 5.5 ms in the baseline. That reading fits the numbers but was not tested
  separately.
- **The per-account connection limit was raised** (above). The Python server has none.
- **Memory and CPU are read by a different script** (`bench/run_rust.py`, with `psutil`) than the baseline's
  (which is not in the repository). Both read the Windows working set once a second.
- **`close_codes`** is new in the reports: which close code the server sent for each dropped connection.

## What the load found

1. **On Windows an edit waits about 15 ms for a timer.** The journal collects changes for 2 ms before it
   writes them (`journal::MAX_WAIT`). A tokio timer on Windows fires on the system clock tick, 15.6 ms by
   default: on this machine `tokio::time::sleep(Duration::from_millis(2))` returned after 15.5 ms (median of
   200; 14.7 to 16.6 ms). Service time in every lightly loaded run is that plus about 2 ms: p50 17.0 to
   18.6 ms, p99 18.7 to 24.6 ms, whatever the database. Under more load p50 goes *down* (14.4 to 16.9 ms with
   200 editors). The link between the timer and the service time is an inference from those numbers; the
   server was not changed to test it. To see it: any run here, for example
   `loadgen --dialect v2 one-doc --editors 10 --rate 2 --features 200`. On Linux, timers are not rounded to
   15.6 ms; no run was made there.
2. **2,000 connections on one document saturate the machine.** In `idle` every connection sends presence
   every 5 s, and the server relays each to everyone else in the document: 400 messages a second in, 800,000
   frames a second out at 2,000 connections (200,000 at 1,000). The server used 21 to 26 CPU-seconds for the
   run at 1,000 connections and 687 to 706 at 2,000: about 11.5 cores for the whole minute, with loadgen
   taking most of the rest (machine CPU 85 to 88 % mean, 100 % peak). Four times the frames cost about thirty
   times the CPU. Whether that is the server's own doing or what happens when the receiver on the same machine
   cannot read fast enough was not determined. In two of six runs the server closed 66 and 28 connections
   with 4408 (too slow): a connection may have 2,000 frames queued (`room::conn::MAX_QUEUED`), and in a
   document of 2,000 a burst of joins or presence alone can reach that. Peak memory was 646 to 719 MB.
   Reproduce: `loadgen --dialect v2 idle --connections 2000 --duration 60s` against a server with the
   connection limit raised.
3. **Memory per connection across many documents.** With 600 connections on 200 documents the server peaked
   at 165 to 171 MB; with 5,000 on 1,000 documents at about 900 MB and with 10,000 on 2,000 at 1.7 to 1.8 GB
   (see "Proposed targets"): roughly 180 KB per connection, for documents of 2.6 KB. Where it goes was not
   looked into.
4. **The first open on PostgreSQL** took 52 to 57 ms for 100 KB and 1 MB documents and 90 to 109 ms for 5 MB,
   against 4 to 9 ms and 26 to 41 ms on SQLite; later opens are alike on both. One sample per run.
5. Nothing else: in the 78 comparison runs no edit timed out or was lost, no connection was refused, no
   protocol error was counted, the server never died, and its log has no warning or error.

## Proposed targets

The design proposes targets that have **not been confirmed**. This is what one laptop running Windows, with
the load generator on the same machine, can and cannot say about them. The latency here is the same
client-side service time as everywhere above (edit sent to `change.accepted` received, or `hello` to a
decoded `welcome`): it contains the server's time, so a figure under a limit is evidence for it, and a figure
over a limit does not by itself show the server is over it.

| Target | Measured here | Verdict |
|---|---|---|
| 200 editors in one document, p99 under 50 ms | `onedoc-200` above (each editor 2 edits a second, 400 a second in all; the target names no rate): p99 35.3, 39.6 and 40.1 ms on SQLite, 22.7, 23.2 and 23.7 ms on PostgreSQL; maximum 46 to 69 ms | **Met at this rate** on both databases, in all six runs. Not measured at any other rate. On SQLite the margin is 10 ms |
| Opening a 5 MB document under 200 ms server-side | `open-5mb` above: warm opens p50 16.6 to 22.6 ms, slowest 33 to 77 ms; first open on a fresh server 26 to 41 ms (SQLite), 90 to 109 ms (PostgreSQL). These include the network and the client decoding the document | **Met** for this document (100 features padded with strings, not a real part), one opener at a time on an otherwise idle server. Not measured while the server is under load |
| 10,000 connections and 2,000 open documents on one machine, p99 commit under 20 ms on PostgreSQL | the extra scenario below | **Missed here, and this laptop cannot tell why.** See below |

### 10,000 connections on 2,000 documents (extra scenario, not part of the comparison)

`many-docs --docs 2000 --editors-per-doc 5 --rate 0.1`: 10,000 connections, five on each of 2,000 documents,
each editing once every 10 s (1,000 edits a second in all). The target names no edit rate; this one is our
choice. `target-manydocs-1000x5` is half of it (5,000 connections, 1,000 documents, 500 edits a second).
Same server settings as above; 30 s pause between runs (240 s before runs 4 and 5); reports
`target-*.run<n>.json` in the same two folders.

#### SQLite

| Scenario | run | changes (sent / accepted / rejected / timeouts / errors / dropped) | refused connections | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | first open | RSS MB | server CPU s | busiest second, % of a core | machine CPU % before / mean / max | server log warnings / errors | median |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| target-manydocs-2000x5 | 1 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 22.431 / 1165 / 1433 | 29.199 / 1314 / 1634 | 980.6 | 61.2 | - | 16 / 1822 / 493 | 7.8 | 37 | 2.4 / 9.7 / 62.0 | 0 / 0 | x |
| target-manydocs-2000x5 (FAILED: 2059 connections refused) | 2 | 47646 / 47646 / 0 / 0 / 0 / 0 | 2059 | 20.079 / 1391 / 1955 | 25.375 / 1458 / 2122 | 750.4 | 63.5 | - | 16 / 1477 / 410 | 5.89 | 42 | 0.5 / 9.3 / 89.5 | 0 / 0 |  |
| target-manydocs-2000x5 (FAILED: 448 connections refused) | 3 | 57312 / 57312 / 0 / 0 / 0 / 0 | 448 | 24.095 / 2374 / 3633 | 30.031 / 2417 / 3686 | 890.2 | 64.4 | - | 15 / 1729 / 470 | 8.83 | 57 | 0.9 / 11.5 / 98.2 | 0 / 0 |  |
| target-manydocs-2000x5 | 4 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 20.895 / 66.751 / 423.423 | 25.695 / 123.647 / 805.375 | 976.1 | 61.5 | - | 16 / 1777 / 455 | 5.62 | 28 | 0.4 / 2.5 / 8.3 | 0 / 0 |  |
| target-manydocs-2000x5 | 5 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 20.719 / 68.031 / 1086 | 25.391 / 152.191 / 1098 | 980.3 | 61.2 | - | 16 / 1779 / 458 | 6.41 | 36 | 0.6 / 3.2 / 26.4 | 0 / 0 |  |
| target-manydocs-1000x5 | 1 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 19.327 / 59.711 / 391.423 | 24.527 / 89.663 / 402.943 | 495.8 | 60.5 | - | 16 / 909 / 253 | 2.3 | 29 | 3.3 / 3.1 / 34.9 | 0 / 0 |  |
| target-manydocs-1000x5 | 2 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 18.895 / 39.935 / 189.951 | 24.287 / 49.279 / 255.743 | 496.8 | 60.4 | - | 16 / 908 / 242 | 2.64 | 23 | 1.9 / 1.5 / 4.7 | 0 / 0 |  |
| target-manydocs-1000x5 | 3 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 18.895 / 40.095 / 201.599 | 24.367 / 49.215 / 273.151 | 496.7 | 60.4 | - | 15 / 909 / 253 | 3.36 | 30 | 0.6 / 1.6 / 5.8 | 0 / 0 | x |

#### PostgreSQL

| Scenario | run | changes (sent / accepted / rejected / timeouts / errors / dropped) | refused connections | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | first open | RSS MB | server CPU s | busiest second, % of a core | machine CPU % before / mean / max | server log warnings / errors | median |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| target-manydocs-2000x5 | 1 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 19.839 / 186.239 / 671.231 | 22.991 / 509.695 / 1541 | 978.5 | 61.3 | - | 15 / 1724 / 413 | 6.45 | 183 | 0.7 / 3.0 / 26.0 | 0 / 0 | x |
| target-manydocs-2000x5 (FAILED: 2935 connections refused) | 2 | 42390 / 42390 / 0 / 0 / 0 / 0 | 2935 | 19.391 / 4014 / 5165 | 24.063 / 4624 / 7062 | 641.7 | 66.1 | - | 15 / 1329 / 381 | 3.5 | 31 | 0.9 / 9.9 / 100.0 | 1 / 0 |  |
| target-manydocs-2000x5 | 3 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 20.383 / 1822 / 2621 | 24.671 / 1883 / 2810 | 949.3 | 63.2 | - | 15 / 1761 / 468 | 10.58 | 85 | 5.4 / 15.1 / 100.0 | 0 / 0 |  |
| target-manydocs-2000x5 | 4 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 19.791 / 109.247 / 560.127 | 22.959 / 386.047 / 1111 | 980.9 | 61.2 | - | 15 / 1770 / 448 | 4.45 | 22 | 2.4 / 2.4 / 10.2 | 0 / 0 |  |
| target-manydocs-1000x5 | 1 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 18.655 / 21.951 / 191.103 | 23.567 / 33.791 / 267.775 | 496.8 | 60.4 | - | 15 / 900 / 236 | 1.23 | 19 | 2.6 / 1.4 / 6.0 | 0 / 0 | x |
| target-manydocs-1000x5 | 2 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 18.607 / 21.983 / 244.095 | 23.599 / 33.215 / 259.583 | 496.5 | 60.4 | - | 15 / 900 / 237 | 2.08 | 26 | 0.9 / 1.5 / 6.4 | 0 / 0 |  |
| target-manydocs-1000x5 | 3 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 18.623 / 21.887 / 212.095 | 23.535 / 33.279 / 270.847 | 496.7 | 60.4 | - | 15 / 898 / 236 | 2.94 | 40 | 0.4 / 1.7 / 5.5 | 0 / 0 |  |

What this shows:

- **10,000 connections were held** and every one of the 60,000 edits was accepted, in the six runs that got
  all their connections (PostgreSQL runs 1, 3 and 4; SQLite runs 1, 4 and 5). None was dropped.
- **Three runs are FAILED because of the laptop, not the server**: loadgen could not open 448 to 2,935 of its
  sockets (`os error 10048`, "only one usage of each socket address"). Windows has 16,384 ports for outgoing
  connections and keeps a closed one unusable for a while; those runs started 30 s after another
  10,000-connection run. Runs 4 and 5 were added with a 240 s pause and had no such failure. The failed runs
  are kept; they say nothing about the server.
- **p99 is not under 20 ms.** On PostgreSQL the complete runs had p99 109, 186 and 1,822 ms (p50 19.8 to
  20.4 ms); on SQLite 67, 68 and 1,165 ms. At half the size PostgreSQL had p99 21.9, 22.0 and 22.0 ms and
  SQLite 39.9, 40.1 and 59.7 ms.
- **What the laptop cannot tell.** (a) p50 is already 19 to 20 ms, most likely because of the Windows timer
  (finding 1): the smallest scenario in the comparison has p99 18.8 ms, so there is next to no room under
  20 ms on this operating system whatever else the server does. (b) The server used 4 to 11 CPU-seconds in
  these 60 s runs, so it was not short of CPU; the slow tail may be the server, PostgreSQL's disk writes, or
  loadgen keeping 10,000 sockets and 30,000 tasks on the same machine. The three complete PostgreSQL runs
  differ by a factor of 17 in p99, which points at the machine rather than at a steady limit. (c) Memory: the
  server peaked at 1.7 to 1.8 GB.

So: the connection count and the document count were reached on this machine; the 20 ms p99 was missed by a
wide margin at full size and narrowly at half size (on PostgreSQL); and neither says whether the target holds
on a Linux server with the load coming from elsewhere. That needs such a run.

## Caveats of the Rust runs

The baseline's caveats apply unchanged: loadgen and the server share one machine and compete for its CPU and
loopback interface; all connections are one account's; the from-schedule figures include loadgen's timer
lateness (0 to 15.6 ms on Windows); rejected edits are in neither latency histogram; a run can overrun its
deadline; no warm-up; documents are generated, not real parts. In addition:

- **Windows.** The server's 2 ms batching wait takes about 15.5 ms here (finding 1). Every service-time
  figure of the Rust server in this file contains it. The Python server has no such wait, so the side-by-side
  table compares a server slowed by the platform's timer with one that is not. Linux was not measured.
- **PostgreSQL ran on the same machine and the same disk** as the server, loadgen and the SQLite files, with
  its default settings, untuned. There is no Python baseline on PostgreSQL to compare with.
- **Three runs per scenario** show the spread between runs minutes apart; they do not show day-to-day
  variation, and three samples make a weak median. The baseline is still one run.
- **The connection limit was raised** from 64 to 20,000 per account. Per-account costs on the server are not
  represented, as in the baseline.
- **Service time is measured by the client.** It includes loopback networking and loadgen's own scheduling
  and decoding. At 2,000 idle connections and at 10,000 connections loadgen itself is a large part of the
  machine's load.
- **The server was killed, not stopped,** after each run, and its database thrown away: shutdown, restart and
  recovery are not measured. Documents are small (2.6 KB and 23 KB) in the edit scenarios; snapshots are
  taken every 200 changes (the default), so they are part of the edit runs.
- **Windows Defender's real-time scanning was on** and the databases were in the system temp directory.
- **The 2,000-connection idle scenario puts all connections on one document**, which is what the baseline
  did; it measures presence relay among 2,000 peers more than it measures idle sockets.
- `0.2.0-dev` is a development version: these are numbers for commit `9df2410` of the server, not a release.
