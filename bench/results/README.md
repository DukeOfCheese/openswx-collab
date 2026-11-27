# Load measurements

Three sets, taken on the same machine with the same load generator: the Python server (the baseline, below),
[the Rust server on the same scenarios](#rust-server-020-dev-the-same-scenarios), with a side-by-side
table and what this laptop can say about the proposed targets, and
[the Rust server again after the follow-up](#rust-server-after-task-19b-the-same-scenarios-again), which
found and removed the causes of the first set's findings (before and after tables there).

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

---

# Rust server after Task 19b: the same scenarios again

The first Rust runs (above) found four things to look into. This section is what was measured to find
their causes, what changed, and the same runs again with the same script, parameters and settings. The
reports are in [`rust-0.2.0-dev-19b-sqlite/`](rust-0.2.0-dev-19b-sqlite/) and
[`rust-0.2.0-dev-19b-postgres/`](rust-0.2.0-dev-19b-postgres/), in the same shape as before; the earlier
folders are untouched.

## What was measured

| | |
|---|---|
| Date | 2026-11-27, 09:46 to 12:19 local time |
| Machine, OS | the same laptop: i7-11800H, 8 cores / 16 threads, 15.8 GB RAM; Windows 11 Education 10.0.22631 |
| Server | `openswx-collab-server` 0.2.0-dev, release build of commit `b4299cb` |
| Loadgen | release build of the same commit, `--dialect v2`. Loadgen itself is unchanged; it decodes the new `presence.batch` and `peer.list` messages through the shared `oswx_core::wire` |
| PostgreSQL | 16.15 from the same package, the same defaults (`shared_buffers` 128MB, `fsync` on, `synchronous_commit` on) |
| Server settings | as before (`OSWX_COLLAB_MAX_CONNECTIONS_PER_USER=20000`), plus **`OSWX_COLLAB_MAX_CONNECTIONS=20000`**. That limit (4,096 connections in all by default) was added to the server after the first runs, and the 10,000-connection scenario needs it raised; it changes nothing below 4,096 connections. Both are in `bench/run_rust.py` |
| Runs | the 13 comparison scenarios x 3 runs on each database, 5 s between runs, as before; the two target scenarios 3 runs each with **240 s** between runs (before: 30 s, and 240 s for two added runs), so that Windows has its outgoing ports back |
| Also running | the desktop session as before (browser, Discord, Spotify, Defender), and the Claude Code session that started the runs and waited; no build or test ran during them. Machine CPU in the two seconds before a run: 0.3 to 7.2 % |

**Linux:** there is no WSL distribution on this machine (`wsl --list` reports none installed), so no Linux
numbers were taken. Nothing was installed.

## What the load had found, and what it was

The diagnosis runs below were short (15 to 30 s) one-off runs of loadgen against a fresh server, with the
server's own `/metrics` and its user and kernel CPU time read with psutil. They are not in the results
folders; the numbers are quoted here.

1. **Light load slower than Python: proved, the journal's 2 ms wait.** The commit itself took about
   1.2 ms (timed inside the server on SQLite: begin 0.1 to 0.5 ms, the statements 0.4 to 1.2 ms, the commit
   about 0.6 ms), while a room waited for each record about 15 ms (the server's `journal_wait_seconds`, mean
   14.9 ms). The writer waited for more records on a tokio timer, and on Windows a timer of any length fires
   on the next 15.6 ms clock tick: a wait of 0 us still went through the timer and still gave a p50 of 17 ms;
   20 ms gave 32 ms. With the timer taken out of the server's writer (a batch is the first record and
   whatever is queued behind it, no timer at all), `onedoc-10` and `onedoc-40` went to p50 1.6 to 1.9 ms;
   putting the 2 ms wait back brought 17.1 ms back (20 s runs on SQLite, two rounds).
   **Is a short wait worth it under load?** The same comparison with 200 editors and with 200 documents x 3:
   without a wait p50 3.5 and 2.8 ms, p99 10.5 to 17 ms, 1.8 and 2.4 records a batch; with the 2 ms wait
   (15.6 ms here) p50 14 to 19 ms, p99 26 to 37 ms, 4.3 and 9.7 records a batch, and no less server CPU. So
   no wait is kept, adaptive or not: under load, batches form from what arrives while the previous one is
   being written. On Linux a 1 ms timer takes about 1 ms; that was not measured.
2. **2,000 peers in one document: the kernel, sending one frame per recipient per presence.** In 30 s
   `idle` runs: 1,000 peers cost the server 1.6 s user + 9.5 s kernel time for 6.5 million frames out;
   1,500 peers 23 + 242 s for 14.6 million; 2,000 peers 25 + 328 s for 17.3 million, and 197 connections
   closed as too slow (4408). Twice the frames cost thirty times the CPU, nearly all of it in the kernel
   (loopback sends to a receiver, loadgen on the same machine, that could not keep up). Encoding was
   already once per broadcast. After the change (presence and drafts relayed at most every 50 ms, each
   peer's latest, up to 64 peers to a frame; peer and presence frames dropped for a reader that falls behind
   instead of closing it, which then gets the peer list; queued frames written with one flush): 1,000 peers
   0.6 + 1.5 s for 1.0 million frames, 2,000 peers 1.8 + 4.6 s for 3.1 million, nothing closed. Loadgen's own
   CPU at 2,000 peers went from 117 s to 13 s.
3. **About 180 KB per connection: proved, mostly the WebSocket read buffer.** Connections that never edit,
   on 2,000 documents with 1, 3 and 5 connections each, peaked at 344, 1,101 and 1,682 MB: about **167 KB
   per extra connection**. tungstenite gives every connection a 128 KiB read buffer and fills it with zeros at
   its first read. With an 8 KiB buffer the same runs peaked at 269, 372 and 463 MB, about **24 KB per
   connection**. Boxing the large, rare messages of a room's channels (each open document has five, whose
   memory comes in blocks of 32 messages sized by the largest) brought them to 253, 335 and 422 MB, about
   **21 KB per connection**. What remains was not broken down further (no heap profiler without downloading
   one): about 90 to 110 KB per open document of 2.6 KB (1,000 against 2,000 documents with one connection
   each: the document and its conflict index in memory, the room task and its channels); and per
   connection the tungstenite write buffer, which keeps the size of the largest frame it ever sent: the
   welcome, in `idle-2000` up to 261 KB for the last joiners, most of that run's 390 MB. axum's WebSocket
   gives no way to shrink it.
4. **First open slower on PostgreSQL: proved, the journal pool's first connection.** Timed inside the
   server, taking the room lease took 38 to 43 ms, of which `begin` 34 to 37 ms: the journal pool had no
   connection yet (size 0) and opened one (a new PostgreSQL process, and the password exchange). sqlx's
   `min_connections` does not help a lazily opened pool (it is kept only after an idle connection has been
   closed: tried, no change), so the store now opens one connection of each pool when it starts, and the
   pools keep it. First open on a fresh server: 100 KB 52 to 56 ms before, 16 to 19 ms after; 1 MB 54 to 57
   before, 22 to 42 after; 5 MB 90 to 109 before, 41 to 64 after. SQLite stays faster (4 to 30 ms).

## Before and after (median runs)

"Before" is the first Rust run set (commit `50cdc48`), "after" this one. Service time in ms. The median run
is the middle one by service p99 (by peak memory for `idle`), as before; a run without a report sorts last.

#### sqlite

| Scenario | service p50 / p99 before | after | peak RSS MB before | after | server CPU s before | after | failed runs before | after |
|---|---|---|---|---|---|---|---|---|
| onedoc-10 | 18.623 / 21.615 | 1.652 / 3.361 | 25 | 35 | 0.16 | 0.16 | 0 of 3 | 0 of 3 |
| onedoc-40 | 18.431 / 21.935 | 1.695 / 5.071 | 36 | 30 | 0.64 | 0.52 | 0 of 3 | 0 of 3 |
| onedoc-100 | 19.151 / 34.559 | 2.313 / 9.655 | 49 | 38 | 2.25 | 1.53 | 0 of 3 | 0 of 3 |
| onedoc-200 | 16.703 / 39.615 | 3.741 / 18.015 | 75 | 49 | 9.03 | 8.2 | 0 of 3 | 0 of 3 |
| onedoc-40-conflict | 17.071 / 20.463 | 3.095 / 7.619 | 33 | 28 | 0.52 | 0.52 | 0 of 3 | 0 of 3 |
| onedoc-10-rate20 | 17.535 / 23.663 | 2.887 / 9.543 | 31 | 28 | 0.72 | 0.61 | 0 of 3 | 0 of 3 |
| manydocs-200x3 | 19.135 / 48.767 | 3.071 / 23.839 | 170 | 84 | 1.52 | 1.53 | 0 of 3 | 0 of 3 |
| open-100kb | 1.562 / 1.772 | 1.577 / 1.888 | 20 | 20 | 0.05 | 0.05 | 0 of 3 | 0 of 3 |
| open-1mb | 3.825 / 4.455 | 4.199 / 15.879 | 21 | 21 | 0.05 | 0.05 | 0 of 3 | 0 of 3 |
| open-5mb | 16.559 / 36.063 | 19.279 / 38.399 | 25 | 25 | 0.11 | 0.08 | 0 of 3 | 0 of 3 |
| idle-500 | - | - | 116 | 62 | 7.06 | 1.48 | 0 of 3 | 0 of 3 |
| idle-1000 | - | - | 248 | 151 | 21.06 | 2.89 | 0 of 3 | 0 of 3 |
| idle-2000 | - | - | 706 | 391 | 693.94 | 15.41 | 1 of 3 | 0 of 3 |
| target-manydocs-2000x5 | 22.431 / 1165 | 4.021 / 66.303 | 1822 | 532 | 7.8 | 5.34 | 2 of 5 | 0 of 3 |
| target-manydocs-1000x5 | 18.895 / 40.095 | 2.965 / 23.135 | 909 | 275 | 3.36 | 2.8 | 0 of 3 | 0 of 3 |

#### postgres

| Scenario | service p50 / p99 before | after | peak RSS MB before | after | server CPU s before | after | failed runs before | after |
|---|---|---|---|---|---|---|---|---|
| onedoc-10 | 17.167 / 18.847 | 1.844 / 3.379 | 23 | 21 | 0.09 | 0.19 | 0 of 3 | 0 of 3 |
| onedoc-40 | 17.167 / 19.007 | 1.897 / 3.893 | 32 | 26 | 0.5 | 0.62 | 0 of 3 | 0 of 3 |
| onedoc-100 | 17.727 / 20.031 | 2.205 / 4.583 | 46 | 33 | 2.58 | 1.78 | 0 of 3 | 0 of 3 |
| onedoc-200 | 15.711 / 23.167 | 4.199 / 10.463 | 67 | 46 | 8.42 | 7.31 | 0 of 3 | 0 of 3 |
| onedoc-40-conflict | 17.279 / 18.975 | 2.169 / 4.339 | 28 | 24 | 0.38 | 0.75 | 0 of 3 | 0 of 3 |
| onedoc-10-rate20 | 17.167 / 18.767 | 2.153 / 4.335 | 26 | 26 | 0.33 | 0.59 | 0 of 3 | 2 of 5 |
| manydocs-200x3 | 18.671 / 21.919 | 2.775 / 7.147 | 165 | 80 | 1.05 | 1.17 | 0 of 3 | 0 of 3 |
| open-100kb | 1.952 / 4.021 | 2.277 / 4.755 | 18 | 18 | 0.05 | 0.05 | 0 of 3 | 0 of 3 |
| open-1mb | 4.243 / 20.655 | 5.455 / 9.271 | 20 | 20 | 0.11 | 0.05 | 0 of 3 | 0 of 3 |
| open-5mb | 18.223 / 67.967 | 20.063 / 73.727 | 24 | 24 | 0.19 | 0.08 | 0 of 3 | 0 of 3 |
| idle-500 | - | - | 114 | 59 | 6.53 | 2.75 | 0 of 3 | 0 of 3 |
| idle-1000 | - | - | 244 | 143 | 21.91 | 2.84 | 0 of 3 | 0 of 3 |
| idle-2000 | - | - | 699 | 387 | 686.55 | 12.03 | 1 of 3 | 0 of 3 |
| target-manydocs-2000x5 | 19.839 / 186.239 | 3.155 / 105.471 | 1724 | 500 | 6.45 | 3.84 | 1 of 4 | 0 of 3 |
| target-manydocs-1000x5 | 18.655 / 21.951 | 2.759 / 6.359 | 900 | 268 | 1.23 | 1.89 | 0 of 3 | 0 of 3 |

The target scenarios before had 5 runs on SQLite and 4 on PostgreSQL (three of them failed for want of
outgoing ports); after, 3 each, none failed. In `onedoc-10-rate20` on PostgreSQL runs 1 and 2 failed and
runs 4 and 5 were added (see "Failed runs").

## Python and Rust side by side (after)

| Scenario | Python accepted / rejected | Python service p50 / p99 / max | Python accepted per s (run s) | Python peak RSS MB | Rust sqlite accepted / rejected | Rust sqlite service p50 / p99 / max | Rust sqlite accepted per s (run s) | Rust sqlite peak RSS MB | Rust postgres accepted / rejected | Rust postgres service p50 / p99 / max | Rust postgres accepted per s (run s) | Rust postgres peak RSS MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| onedoc-10 | 1200 / 0 | 5.099 / 9.727 / 12.063 | 20.0 (60) | 103 | 1200 / 0 | 1.652 / 3.361 / 7.063 | 20.0 (60) | 35 | 1200 / 0 | 1.844 / 3.379 / 50.207 | 20.0 (60) | 21 |
| onedoc-40 | 4800 / 0 | 5.839 / 17.039 / 54.943 | 80.0 (60) | 108 | 4800 / 0 | 1.695 / 5.071 / 9.663 | 80.0 (60) | 30 | 4800 / 0 | 1.897 / 3.893 / 46.623 | 80.1 (60) | 26 |
| onedoc-100 | 12000 / 0 | 531.967 / 590.335 / 612.863 | 186.6 (64) | 117 | 12000 / 0 | 2.313 / 9.655 / 15.535 | 199.9 (60) | 38 | 12000 / 0 | 2.205 / 4.583 / 49.535 | 200.0 (60) | 33 |
| onedoc-200 | 24000 / 0 | 1851 / 3152 / 3903 | 104.1 (231) | 133 | 24000 / 0 | 3.741 / 18.015 / 35.839 | 399.9 (60) | 49 | 24000 / 0 | 4.199 / 10.463 / 66.303 | 399.9 (60) | 46 |
| onedoc-40-conflict | 4770 / 30 | 5.527 / 16.655 / 52.415 | 79.5 (60) | 108 | 4663 / 137 | 3.095 / 7.619 / 13.543 | 77.7 (60) | 28 | 4724 / 76 | 2.169 / 4.339 / 70.335 | 78.7 (60) | 24 |
| onedoc-10-rate20 | 12000 / 0 | 7.743 / 19.279 / 49.535 | 200.0 (60) | 105 | 12000 / 0 | 2.887 / 9.543 / 15.463 | 200.0 (60) | 28 | 12000 / 0 (2 of 5 runs FAILED) | 2.153 / 4.335 / 54.591 | 200.0 (60) | 26 |
| manydocs-200x3 | 36000 / 0 | 1637 / 4497 / 19071 | 336.7 (107) | 198 | 36000 / 0 | 3.071 / 23.839 / 52.351 | 599.4 (60) | 84 | 36000 / 0 | 2.775 / 7.147 / 42.207 | 599.6 (60) | 80 |
| open-100kb | n/a | 3.053 / 4.403 / 4.403 | n/a (0.2) | 97 | n/a | 1.577 / 1.888 / 1.888 | n/a (0.0) | 20 | n/a | 2.277 / 4.755 / 4.755 | n/a (0.1) | 18 |
| open-1mb | n/a | 6.459 / 7.651 / 7.651 | n/a (0.2) | 98 | n/a | 4.199 / 15.879 / 15.879 | n/a (0.1) | 21 | n/a | 5.455 / 9.271 / 9.271 | n/a (0.2) | 20 |
| open-5mb | n/a | 22.911 / 41.087 / 41.087 | n/a (0.7) | 108 | n/a | 19.279 / 38.399 / 38.399 | n/a (0.5) | 25 | n/a | 20.063 / 73.727 / 73.727 | n/a (0.6) | 24 |
| idle-500 | 0 / 0 | - | 0.0 (60) | 170 | 0 / 0 | - | 0.0 (60) | 62 | 0 / 0 | - | 0.0 (60) | 59 |
| idle-1000 | 0 / 0 | - | 0.0 (60) | 252 | 0 / 0 | - | 0.0 (60) | 151 | 0 / 0 | - | 0.0 (60) | 143 |
| idle-2000 | 0 / 0 (FAILED: 2000 dropped) | - | 0.0 (60) | 606 | 0 / 0 | - | 0.0 (60) | 391 | 0 / 0 | - | 0.0 (60) | 387 |

## What this says

- **Light load is now faster than Python on this machine.** With 10 or 40 editors p50 is 1.7 to 1.9 ms on
  both databases (Python 5.1 and 5.8 ms; Rust before 17 to 19 ms), p99 3.4 to 5.1 ms (Python 9.7 and
  17.0 ms).
- **Heavy load got faster as well**: 200 editors p50 3.7 / 4.2 ms and p99 18.0 / 10.5 ms (SQLite /
  PostgreSQL; before 16.7 / 15.7 and 39.6 / 23.2 ms); 200 documents x 3 p99 23.8 / 7.1 ms (before 48.8 /
  21.9 ms).
- **2,000 peers in one document now runs clean**: no connection closed in six runs (before, 2 of 6 runs lost
  28 and 66), 11 to 15 CPU-seconds of server time for the whole run instead of 687 to 706, peak memory 385
  to 396 MB instead of 646 to 719 MB (Python failed this scenario).
- **10,000 connections on 2,000 documents**: every run got all its connections and every edit was accepted;
  peak memory 500 to 532 MB instead of 1.7 to 1.8 GB; p50 3.2 to 4.4 ms.
- **What did not improve, or got worse.** The proposed 20 ms p99 at 10,000 connections is still missed:
  p99 46 to 222 ms on SQLite and 54 to 152 ms on PostgreSQL (before 67 to 1,165 and 109 to 1,822 ms). At half
  that size PostgreSQL meets it (6.2 to 6.5 ms) and SQLite does not (23 to 24 ms). Opening documents is
  unchanged within the spread (warm p50 1.6 / 4.2 / 19.3 ms on SQLite, 1.6 / 3.8 / 16.6 before).
  Single-sample maxima are higher in some light PostgreSQL runs (`onedoc-10` max 50 ms against 20 ms
  before, with p99 3.4 ms): one slow edit per run, not looked into. `onedoc-10` on SQLite peaked at 35 MB
  against 25 MB before (its other runs: 28 and 35 MB); not looked into.
- **Still slower than Python**: the first open of a document on PostgreSQL (16 to 64 ms; Python, on
  SQLite, 15 to 48 ms), and the conflict scenario still refuses more edits than Python (76 to 137 against
  30; before 118 to 289: the window in which an edit can be overtaken shrank with the service time).
- **What this laptop cannot tell**: Linux (no WSL distribution here), and the server's cost with the load
  coming from another machine. Loadgen still shares the CPU and the loopback interface, and at 10,000
  connections the slow tail may be the server, PostgreSQL's disk writes or loadgen.

## Failed runs

`onedoc-10-rate20` on PostgreSQL, runs 1 and 2: the server could not start (its log has one line), and
loadgen, connecting to port 8791, was answered `401` "openswx token needed": another program on this machine
(an openswx application's local server) held the port for those two minutes; nothing listened there
afterwards. The runs are kept as failed, and runs 4 and 5 were added with the same parameters. They say
nothing about the collaboration server. No other run failed.

## Every run (after)

#### rust-0.2.0-dev-19b-sqlite (comparison): the median run of each scenario

| Run | connections | changes (sent / accepted / rejected / timeouts / errors / dropped) | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | doc bytes | welcome bytes | first open | RSS MB | server CPU s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| one-doc 10 editors, rate 2, 200 features | 10 | 1200 / 1200 / 0 / 0 / 0 / 0 | 1.652 / 3.361 / 7.063 | 9.383 / 18.143 / 19.023 | 20.0 | 60 | 23370 | 18284-19436 | - | 16 / 35 / 34 | 0.16 |
| one-doc 40 editors, rate 2, 200 features | 40 | 4800 / 4800 / 0 / 0 / 0 / 0 | 1.695 / 5.071 / 9.663 | 8.535 / 18.431 / 24.063 | 80.0 | 60 | 23370 | 18284-23308 | - | 15 / 30 / 28 | 0.52 |
| one-doc 100 editors, rate 2, 200 features | 100 | 12000 / 12000 / 0 / 0 / 0 / 0 | 2.313 / 9.655 / 15.535 | 7.787 / 18.703 / 29.263 | 199.9 | 60 | 23370 | 18284-31048 | - | 16 / 38 / 33 | 1.53 |
| one-doc 200 editors, rate 2, 200 features | 200 | 24000 / 24000 / 0 / 0 / 0 / 0 | 3.741 / 18.015 / 35.839 | 6.127 / 22.639 / 38.591 | 399.9 | 60 | 23370 | 18284-44048 | - | 15 / 49 / 39 | 8.2 |
| one-doc 40 editors, rate 2, 20 features | 40 | 4800 / 4663 / 137 / 0 / 0 / 0 | 3.095 / 7.619 / 13.543 | 9.447 / 19.919 / 25.791 | 77.7 | 60 | 2645 | 2218-7242 | - | 16 / 28 / 27 | 0.52 |
| one-doc 10 editors, rate 20, 200 features | 10 | 12000 / 12000 / 0 / 0 / 0 / 0 | 2.887 / 9.543 / 15.463 | 7.371 / 17.663 / 28.079 | 200.0 | 60 | 23370 | 18284-19436 | - | 16 / 28 / 28 | 0.61 |
| many-docs 200 docs x 3 editors, rate 1 | 600 | 36000 / 36000 / 0 / 0 / 0 / 0 | 3.071 / 23.839 / 52.351 | 6.975 / 29.199 / 54.175 | 599.4 | 60 | 2645 | 2218-2480 | - | 15 / 84 / 77 | 1.53 |
| open 100 KB x20 | 1 | n/a | 1.577 / 1.888 / 1.888 | - | n/a | 0.0 | 100770 | 97982-97983 | 6.0 | 16 / 20 / 20 | 0.05 |
| open 1 MB x20 | 1 | n/a | 4.199 / 15.879 / 15.879 | - | n/a | 0.1 | 1000770 | 997982-997983 | 17.4 | 16 / 21 / 21 | 0.05 |
| open 5 MB x20 | 1 | n/a | 19.279 / 38.399 / 38.399 | - | n/a | 0.5 | 5000770 | 4997982-4997983 | 29.6 | 16 / 25 / 25 | 0.08 |
| idle 500 connections | 500 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 578-65342 | - | 15 / 62 / 27 | 1.48 |
| idle 1000 connections | 1000 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 578-130342 | - | 16 / 151 / 32 | 2.89 |
| idle 2000 connections | 2000 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 578-261342 | - | 16 / 391 / 37 | 15.41 |

#### rust-0.2.0-dev-19b-sqlite (comparison): every run

| Scenario | run | changes (sent / accepted / rejected / timeouts / errors / dropped) | refused connections | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | first open | RSS MB | server CPU s | busiest second, % of a core | machine CPU % before / mean / max | server log warnings / errors | median |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| onedoc-10 | 1 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 1.744 / 4.563 / 50.143 | 9.679 / 18.879 / 64.639 | 20.0 | 60.0 | - | 16 / 24 / 23 | 0.11 | 2 | 1.6 / 2.5 / 46.0 | 0 / 0 |  |
| onedoc-10 | 2 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 1.774 / 3.307 / 10.839 | 10.303 / 18.399 / 20.015 | 20.0 | 59.9 | - | 16 / 24 / 24 | 0.16 | 5 | 1.3 / 1.8 / 7.4 | 0 / 0 |  |
| onedoc-10 | 3 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 1.652 / 3.361 / 7.063 | 9.383 / 18.143 / 19.023 | 20.0 | 59.9 | - | 16 / 35 / 34 | 0.16 | 6 | 5.2 / 4.9 / 11.1 | 0 / 0 | x |
| onedoc-40 | 1 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 1.719 / 5.111 / 11.719 | 8.695 / 18.223 / 24.255 | 80.0 | 60.0 | - | 15 / 42 / 39 | 0.5 | 5 | 8.8 / 2.7 / 9.1 | 0 / 0 |  |
| onedoc-40 | 2 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 1.695 / 5.071 / 9.663 | 8.535 / 18.431 / 24.063 | 80.0 | 60.0 | - | 15 / 30 / 28 | 0.52 | 6 | 1.1 / 1.4 / 6.5 | 0 / 0 | x |
| onedoc-40 | 3 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 1.689 / 4.555 / 8.919 | 8.911 / 17.935 / 22.911 | 80.0 | 60.0 | - | 16 / 30 / 28 | 0.53 | 8 | 6.3 / 1.8 / 6.9 | 0 / 0 |  |
| onedoc-100 | 1 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 2.313 / 9.655 / 15.535 | 7.787 / 18.703 / 29.263 | 199.9 | 60.0 | - | 16 / 38 / 33 | 1.53 | 12 | 0.8 / 2.0 / 7.2 | 0 / 0 | x |
| onedoc-100 | 2 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 2.295 / 10.399 / 19.199 | 7.739 / 18.751 / 30.591 | 199.9 | 60.0 | - | 16 / 39 / 33 | 1.34 | 12 | 2.9 / 1.9 / 7.1 | 0 / 0 |  |
| onedoc-100 | 3 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 2.349 / 9.607 / 17.103 | 7.571 / 18.735 / 28.383 | 200.0 | 60.0 | - | 16 / 38 / 33 | 1.88 | 16 | 1.7 / 2.3 / 8.8 | 0 / 0 |  |
| onedoc-200 | 1 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 3.741 / 18.015 / 35.839 | 6.127 / 22.639 / 38.591 | 399.9 | 60.0 | - | 15 / 49 / 39 | 8.2 | 39 | 2.0 / 3.4 / 7.8 | 0 / 0 | x |
| onedoc-200 | 2 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 3.489 / 16.559 / 33.407 | 5.583 / 21.903 / 36.991 | 399.9 | 60.0 | - | 16 / 50 / 40 | 7.7 | 37 | 0.9 / 3.4 / 7.8 | 0 / 0 |  |
| onedoc-200 | 3 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 5.879 / 23.407 / 44.895 | 7.823 / 27.327 / 45.695 | 399.9 | 60.0 | - | 16 / 54 / 42 | 7.41 | 32 | 3.2 / 3.5 / 7.8 | 0 / 0 |  |
| onedoc-40-conflict | 1 | 4800 / 4788 / 12 / 0 / 0 / 0 | 0 | 3.091 / 6.575 / 16.215 | 8.783 / 19.615 / 26.239 | 79.8 | 60.0 | - | 16 / 27 / 26 | 0.69 | 9 | 2.4 / 2.0 / 6.6 | 0 / 0 |  |
| onedoc-40-conflict | 2 | 4800 / 4738 / 62 / 0 / 0 / 0 | 0 | 3.097 / 8.479 / 16.575 | 9.471 / 20.079 / 27.919 | 79.0 | 60.0 | - | 16 / 28 / 27 | 0.39 | 5 | 0.8 / 1.3 / 3.4 | 0 / 0 |  |
| onedoc-40-conflict | 3 | 4800 / 4663 / 137 / 0 / 0 / 0 | 0 | 3.095 / 7.619 / 13.543 | 9.447 / 19.919 / 25.791 | 77.7 | 60.0 | - | 16 / 28 / 27 | 0.52 | 5 | 3.9 / 1.5 / 4.8 | 0 / 0 | x |
| onedoc-10-rate20 | 1 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 2.887 / 9.543 / 15.463 | 7.371 / 17.663 / 28.079 | 200.0 | 60.0 | - | 16 / 28 / 28 | 0.61 | 5 | 0.8 / 1.5 / 6.4 | 0 / 0 | x |
| onedoc-10-rate20 | 2 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 3.315 / 11.295 / 32.319 | 7.891 / 19.903 / 40.479 | 200.0 | 60.0 | - | 16 / 28 / 28 | 0.73 | 8 | 1.2 / 1.7 / 7.8 | 0 / 0 |  |
| onedoc-10-rate20 | 3 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 2.171 / 8.215 / 20.447 | 8.551 / 18.607 / 27.583 | 200.0 | 60.0 | - | 15 / 28 / 28 | 0.52 | 5 | 0.9 / 2.0 / 6.3 | 0 / 0 |  |
| manydocs-200x3 | 1 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 3.071 / 23.839 / 52.351 | 6.975 / 29.199 / 54.175 | 599.4 | 60.1 | - | 15 / 84 / 77 | 1.53 | 9 | 0.9 / 1.8 / 5.0 | 0 / 0 | x |
| manydocs-200x3 | 2 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 3.089 / 23.599 / 37.663 | 7.163 / 29.663 / 47.039 | 599.6 | 60.0 | - | 16 / 85 / 77 | 1.55 | 9 | 0.6 / 2.1 / 5.7 | 0 / 0 |  |
| manydocs-200x3 | 3 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 5.703 / 31.727 / 54.623 | 8.007 / 36.543 / 86.719 | 599.5 | 60.0 | - | 15 / 85 / 78 | 2.98 | 17 | 1.0 / 2.6 / 7.0 | 0 / 0 |  |
| open-100kb | 1 | n/a | 0 | 1.643 / 1.876 / 1.876 | - | n/a | 0.1 | 6.2 | 16 / 20 / 20 | 0.05 | 5 | 5.5 / 2.2 / 2.2 | 0 / 0 |  |
| open-100kb | 2 | n/a | 0 | 1.577 / 1.888 / 1.888 | - | n/a | 0.0 | 6.0 | 16 / 20 / 20 | 0.05 | 5 | 1.2 / 3.6 / 3.6 | 0 / 0 | x |
| open-100kb | 3 | n/a | 0 | 1.642 / 2.04 / 2.04 | - | n/a | 0.1 | 27.9 | 16 / 20 / 20 | 0.05 | 5 | 1.2 / 5.0 / 5.0 | 0 / 0 |  |
| open-1mb | 1 | n/a | 0 | 4.391 / 16.575 / 16.575 | - | n/a | 0.2 | 10.6 | 16 / 22 / 22 | 0.06 | 6 | 1.2 / 2.5 / 2.5 | 0 / 0 |  |
| open-1mb | 2 | n/a | 0 | 4.199 / 15.879 / 15.879 | - | n/a | 0.1 | 17.4 | 16 / 21 / 21 | 0.05 | 5 | 3.1 / 2.1 / 2.1 | 0 / 0 | x |
| open-1mb | 3 | n/a | 0 | 4.671 / 5.239 / 5.239 | - | n/a | 0.1 | 10.3 | 16 / 21 / 21 | 0.05 | 5 | 3.3 / 3.3 / 3.3 | 0 / 0 |  |
| open-5mb | 1 | n/a | 0 | 17.983 / 35.487 / 35.487 | - | n/a | 0.5 | 29.5 | 16 / 25 / 25 | 0.06 | 6 | 1.2 / 2.2 / 3.4 | 0 / 0 |  |
| open-5mb | 2 | n/a | 0 | 19.279 / 38.399 / 38.399 | - | n/a | 0.5 | 29.6 | 16 / 25 / 25 | 0.08 | 8 | 1.0 / 1.9 / 2.6 | 0 / 0 | x |
| open-5mb | 3 | n/a | 0 | 16.847 / 38.591 / 38.591 | - | n/a | 0.4 | 27.4 | 16 / 26 / 26 | 0.08 | 8 | 0.9 / 1.6 / 2.6 | 0 / 0 |  |
| idle-500 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 16 / 64 / 28 | 1.09 | 17 | 1.0 / 2.6 / 6.3 | 0 / 0 |  |
| idle-500 | 2 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 16 / 62 / 27 | 1.23 | 28 | 2.3 / 2.6 / 8.1 | 0 / 0 |  |
| idle-500 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 15 / 62 / 27 | 1.48 | 28 | 2.6 / 2.7 / 10.2 | 0 / 0 | x |
| idle-1000 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.2 | - | 16 / 149 / 32 | 3.34 | 75 | 0.9 / 4.1 / 11.8 | 0 / 0 |  |
| idle-1000 | 2 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 15 / 152 / 32 | 2.66 | 34 | 0.8 / 3.8 / 7.9 | 0 / 0 |  |
| idle-1000 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.2 | - | 16 / 151 / 32 | 2.89 | 64 | 1.8 / 3.9 / 18.7 | 0 / 0 | x |
| idle-2000 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.5 | - | 15 / 384 / 37 | 11.31 | 129 | 4.4 / 7.5 / 30.1 | 0 / 0 |  |
| idle-2000 | 2 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.4 | - | 16 / 392 / 38 | 12.14 | 183 | 2.1 / 7.8 / 25.0 | 0 / 0 |  |
| idle-2000 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.4 | - | 16 / 391 / 37 | 15.41 | 260 | 2.4 / 8.2 / 27.6 | 0 / 0 | x |

#### rust-0.2.0-dev-19b-sqlite (targets): the median run of each scenario

| Run | connections | changes (sent / accepted / rejected / timeouts / errors / dropped) | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | doc bytes | welcome bytes | first open | RSS MB | server CPU s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| many-docs 2000 docs x 5 editors, rate 0.1 | 10000 | 60000 / 60000 / 0 / 0 / 0 / 0 | 4.021 / 66.303 / 494.591 | 7.471 / 244.735 / 928.767 | 984.7 | 61 | 2645 | 2218-2745 | - | 16 / 532 / 374 | 5.34 |
| many-docs 1000 docs x 5 editors, rate 0.1 | 5000 | 30000 / 30000 / 0 / 0 / 0 / 0 | 2.965 / 23.135 / 223.871 | 7.167 / 31.855 / 309.759 | 495.9 | 60 | 2645 | 2218-2744 | - | 16 / 275 / 197 | 2.8 |

#### rust-0.2.0-dev-19b-sqlite (targets): every run

| Scenario | run | changes (sent / accepted / rejected / timeouts / errors / dropped) | refused connections | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | first open | RSS MB | server CPU s | busiest second, % of a core | machine CPU % before / mean / max | server log warnings / errors | median |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| target-manydocs-2000x5 | 1 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 4.021 / 66.303 / 494.591 | 7.471 / 244.735 / 928.767 | 984.7 | 60.9 | - | 16 / 532 / 374 | 5.34 | 26 | 0.6 / 1.9 / 13.0 | 0 / 0 | x |
| target-manydocs-2000x5 | 2 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 3.937 / 46.207 / 295.423 | 7.435 / 72.255 / 677.887 | 984.6 | 60.9 | - | 15 / 531 / 394 | 6.23 | 31 | 2.1 / 1.9 / 7.6 | 0 / 0 |  |
| target-manydocs-2000x5 | 3 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 4.383 / 221.823 / 949.759 | 7.839 / 681.471 / 1566 | 980.1 | 61.2 | - | 16 / 511 / 378 | 5.5 | 23 | 4.4 / 3.3 / 15.7 | 0 / 0 |  |
| target-manydocs-1000x5 | 1 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 3.035 / 22.735 / 197.759 | 7.123 / 30.287 / 270.079 | 497.1 | 60.4 | - | 16 / 272 / 195 | 2.56 | 26 | 0.6 / 1.5 / 6.1 | 0 / 0 |  |
| target-manydocs-1000x5 | 2 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 2.965 / 23.135 / 223.871 | 7.167 / 31.855 / 309.759 | 495.9 | 60.5 | - | 16 / 275 / 197 | 2.8 | 19 | 0.8 / 2.4 / 21.1 | 0 / 0 | x |
| target-manydocs-1000x5 | 3 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 3.143 / 23.711 / 204.415 | 7.187 / 32.223 / 299.775 | 496.3 | 60.5 | - | 16 / 275 / 198 | 2.52 | 22 | 0.3 / 1.8 / 7.6 | 0 / 0 |  |

#### rust-0.2.0-dev-19b-postgres (comparison): the median run of each scenario

| Run | connections | changes (sent / accepted / rejected / timeouts / errors / dropped) | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | doc bytes | welcome bytes | first open | RSS MB | server CPU s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| one-doc 10 editors, rate 2, 200 features | 10 | 1200 / 1200 / 0 / 0 / 0 / 0 | 1.844 / 3.379 / 50.207 | 10.111 / 18.559 / 57.599 | 20.0 | 60 | 23370 | 18284-19436 | - | 14 / 21 / 21 | 0.19 |
| one-doc 40 editors, rate 2, 200 features | 40 | 4800 / 4800 / 0 / 0 / 0 / 0 | 1.897 / 3.893 / 46.623 | 8.751 / 18.191 / 56.639 | 80.1 | 60 | 23370 | 18284-23308 | - | 15 / 26 / 24 | 0.62 |
| one-doc 100 editors, rate 2, 200 features | 100 | 12000 / 12000 / 0 / 0 / 0 / 0 | 2.205 / 4.583 / 49.535 | 7.159 / 17.055 / 50.047 | 200.0 | 60 | 23370 | 18284-31048 | - | 15 / 33 / 28 | 1.78 |
| one-doc 200 editors, rate 2, 200 features | 200 | 24000 / 24000 / 0 / 0 / 0 / 0 | 4.199 / 10.463 / 66.303 | 6.383 / 19.759 / 72.831 | 399.9 | 60 | 23370 | 18284-44048 | - | 14 / 46 / 34 | 7.31 |
| one-doc 40 editors, rate 2, 20 features | 40 | 4800 / 4724 / 76 / 0 / 0 / 0 | 2.169 / 4.339 / 70.335 | 8.559 / 18.191 / 79.295 | 78.7 | 60 | 2645 | 2218-7242 | - | 15 / 24 / 23 | 0.75 |
| one-doc 10 editors, rate 20, 200 features (2 of 5 runs FAILED) | 10 | 12000 / 12000 / 0 / 0 / 0 / 0 | 2.153 / 4.335 / 54.591 | 8.423 / 18.207 / 54.943 | 200.0 | 60 | 23370 | 18284-19436 | - | 15 / 26 / 25 | 0.59 |
| many-docs 200 docs x 3 editors, rate 1 | 600 | 36000 / 36000 / 0 / 0 / 0 / 0 | 2.775 / 7.147 / 42.207 | 6.023 / 18.287 / 46.495 | 599.6 | 60 | 2645 | 2218-2480 | - | 15 / 80 / 70 | 1.17 |
| open 100 KB x20 | 1 | n/a | 2.277 / 4.755 / 4.755 | - | n/a | 0.1 | 100770 | 97982-97983 | 17.5 | 15 / 18 / 18 | 0.05 |
| open 1 MB x20 | 1 | n/a | 5.455 / 9.271 / 9.271 | - | n/a | 0.2 | 1000770 | 997982-997983 | 41.8 | 14 / 20 / 20 | 0.05 |
| open 5 MB x20 | 1 | n/a | 20.063 / 73.727 / 73.727 | - | n/a | 0.6 | 5000770 | 4997982-4997983 | 41.4 | 15 / 24 / 24 | 0.08 |
| idle 500 connections | 500 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 578-65342 | - | 15 / 59 / 26 | 2.75 |
| idle 1000 connections | 1000 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 579-130342 | - | 15 / 143 / 31 | 2.84 |
| idle 2000 connections | 2000 | 0 / 0 / 0 / 0 / 0 / 0 | - | - | 0.0 | 60 | 492 | 578-261342 | - | 15 / 387 / 35 | 12.03 |

#### rust-0.2.0-dev-19b-postgres (comparison): every run

| Scenario | run | changes (sent / accepted / rejected / timeouts / errors / dropped) | refused connections | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | first open | RSS MB | server CPU s | busiest second, % of a core | machine CPU % before / mean / max | server log warnings / errors | median |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| onedoc-10 | 1 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 1.881 / 3.007 / 6.599 | 9.879 / 18.415 / 19.727 | 20.0 | 60.0 | - | 15 / 21 / 21 | 0.12 | 5 | 0.4 / 1.3 / 5.9 | 0 / 0 |  |
| onedoc-10 | 2 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 1.844 / 3.379 / 50.207 | 10.111 / 18.559 / 57.599 | 20.0 | 60.0 | - | 14 / 21 / 21 | 0.19 | 8 | 0.5 / 1.2 / 8.1 | 0 / 0 | x |
| onedoc-10 | 3 | 1200 / 1200 / 0 / 0 / 0 / 0 | 0 | 1.829 / 3.987 / 47.935 | 10.015 / 18.975 / 63.807 | 20.0 | 60.0 | - | 15 / 21 / 21 | 0.25 | 8 | 0.4 / 4.9 / 84.7 | 0 / 0 |  |
| onedoc-40 | 1 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 1.895 / 3.859 / 50.367 | 8.639 / 18.191 / 59.391 | 80.0 | 60.0 | - | 15 / 25 / 24 | 0.7 | 9 | 3.8 / 1.9 / 6.5 | 0 / 0 |  |
| onedoc-40 | 2 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 2.049 / 4.303 / 50.303 | 8.639 / 18.175 / 50.911 | 80.0 | 60.0 | - | 15 / 26 / 25 | 0.31 | 8 | 3.6 / 2.5 / 10.0 | 0 / 0 |  |
| onedoc-40 | 3 | 4800 / 4800 / 0 / 0 / 0 / 0 | 0 | 1.897 / 3.893 / 46.623 | 8.751 / 18.191 / 56.639 | 80.1 | 60.0 | - | 15 / 26 / 24 | 0.62 | 6 | 0.8 / 2.0 / 6.1 | 0 / 0 | x |
| onedoc-100 | 1 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 2.219 / 4.231 / 45.247 | 7.447 / 17.535 / 46.015 | 200.0 | 60.0 | - | 14 / 33 / 27 | 1.72 | 16 | 1.7 / 1.7 / 8.2 | 0 / 0 |  |
| onedoc-100 | 2 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 2.205 / 4.583 / 49.535 | 7.159 / 17.055 / 50.047 | 200.0 | 60.0 | - | 15 / 33 / 28 | 1.78 | 11 | 1.4 / 2.0 / 5.9 | 0 / 0 | x |
| onedoc-100 | 3 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 2.281 / 4.691 / 51.263 | 7.275 / 17.423 / 61.311 | 199.9 | 60.0 | - | 15 / 35 / 29 | 1.27 | 11 | 1.3 / 2.0 / 6.3 | 0 / 0 |  |
| onedoc-200 | 1 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 3.535 / 10.407 / 52.735 | 5.819 / 19.119 / 61.151 | 399.8 | 60.0 | - | 15 / 46 / 34 | 6.38 | 26 | 1.3 / 3.8 / 17.3 | 0 / 0 |  |
| onedoc-200 | 2 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 4.135 / 10.895 / 57.759 | 6.515 / 19.935 / 62.591 | 399.9 | 60.0 | - | 14 / 44 / 32 | 8.03 | 33 | 1.7 / 4.5 / 15.3 | 0 / 0 |  |
| onedoc-200 | 3 | 24000 / 24000 / 0 / 0 / 0 / 0 | 0 | 4.199 / 10.463 / 66.303 | 6.383 / 19.759 / 72.831 | 399.9 | 60.0 | - | 14 / 46 / 34 | 7.31 | 29 | 7.6 / 4.1 / 12.9 | 0 / 0 | x |
| onedoc-40-conflict | 1 | 4800 / 4724 / 76 / 0 / 0 / 0 | 0 | 2.169 / 4.339 / 70.335 | 8.559 / 18.191 / 79.295 | 78.7 | 60.0 | - | 15 / 24 / 23 | 0.75 | 6 | 6.8 / 5.7 / 10.8 | 0 / 0 | x |
| onedoc-40-conflict | 2 | 4800 / 4753 / 47 / 0 / 0 / 0 | 0 | 2.109 / 4.399 / 49.375 | 8.895 / 18.335 / 49.759 | 79.2 | 60.0 | - | 15 / 23 / 23 | 0.69 | 8 | 6.1 / 5.6 / 9.8 | 0 / 0 |  |
| onedoc-40-conflict | 3 | 4800 / 4696 / 104 / 0 / 0 / 0 | 0 | 2.117 / 4.195 / 58.239 | 8.871 / 18.383 / 58.975 | 78.3 | 60.0 | - | 15 / 23 / 23 | 0.66 | 5 | 7.1 / 5.2 / 12.3 | 0 / 0 |  |
| onedoc-10-rate20 | 1 | FAILED: loadgen exit 1, no report | - | - | - | - | - | - | - | - | - | 7.2 / ? / ? | 0 / 0 | |
| onedoc-10-rate20 | 2 | FAILED: loadgen exit 1, no report | - | - | - | - | - | - | - | - | - | 2.0 / ? / ? | 0 / 0 | |
| onedoc-10-rate20 | 3 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 2.153 / 4.335 / 54.591 | 8.423 / 18.207 / 54.943 | 200.0 | 60.0 | - | 15 / 26 / 25 | 0.59 | 8 | 5.7 / 5.8 / 18.0 | 0 / 0 | x |
| onedoc-10-rate20 | 4 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 1.888 / 3.507 / 43.967 | 8.103 / 17.311 / 46.207 | 200.0 | 60.0 | - | 15 / 25 / 24 | 0.27 | 3 | 4.7 / 1.1 / 7.2 | 0 / 0 |  |
| onedoc-10-rate20 | 5 | 12000 / 12000 / 0 / 0 / 0 / 0 | 0 | 1.8 / 3.427 / 45.023 | 7.843 / 16.671 / 55.999 | 200.0 | 60.0 | - | 15 / 25 / 25 | 0.27 | 8 | 0.5 / 1.1 / 5.9 | 0 / 0 |  |
| manydocs-200x3 | 1 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 2.837 / 7.627 / 30.847 | 6.007 / 19.055 / 39.743 | 599.6 | 60.0 | - | 15 / 79 / 72 | 1.72 | 22 | 6.7 / 5.6 / 10.0 | 0 / 0 |  |
| manydocs-200x3 | 2 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 2.775 / 7.147 / 42.207 | 6.023 / 18.287 / 46.495 | 599.6 | 60.0 | - | 15 / 80 / 70 | 1.17 | 9 | 5.9 / 4.5 / 11.0 | 0 / 0 | x |
| manydocs-200x3 | 3 | 36000 / 36000 / 0 / 0 / 0 / 0 | 0 | 2.787 / 7.079 / 36.895 | 6.167 / 18.543 / 75.007 | 599.7 | 60.0 | - | 15 / 81 / 74 | 1.16 | 12 | 5.6 / 4.1 / 10.1 | 0 / 0 |  |
| open-100kb | 1 | n/a | 0 | 2.277 / 4.755 / 4.755 | - | n/a | 0.1 | 17.5 | 15 / 18 / 18 | 0.05 | 5 | 3.0 / 3.3 / 3.3 | 0 / 0 | x |
| open-100kb | 2 | n/a | 0 | 1.963 / 3.783 / 3.783 | - | n/a | 0.1 | 15.8 | 15 / 18 / 18 | 0.05 | 5 | 6.5 / 8.8 / 8.8 | 0 / 0 |  |
| open-100kb | 3 | n/a | 0 | 2.113 / 5.415 / 5.415 | - | n/a | 0.1 | 19.4 | 15 / 19 / 19 | 0.06 | 6 | 5.8 / 7.6 / 7.6 | 0 / 0 |  |
| open-1mb | 1 | n/a | 0 | 5.831 / 47.039 / 47.039 | - | n/a | 0.2 | 22.5 | 15 / 19 / 19 | 0.05 | 5 | 8.8 / 3.5 / 3.5 | 0 / 0 |  |
| open-1mb | 2 | n/a | 0 | 5.455 / 9.271 / 9.271 | - | n/a | 0.2 | 41.8 | 14 / 20 / 20 | 0.05 | 5 | 2.7 / 5.5 / 5.5 | 0 / 0 | x |
| open-1mb | 3 | n/a | 0 | 5.251 / 7.815 / 7.815 | - | n/a | 0.2 | 23.1 | 15 / 19 / 19 | 0.08 | 8 | 3.2 / 5.0 / 5.0 | 0 / 0 |  |
| open-5mb | 1 | n/a | 0 | 28.959 / 82.047 / 82.047 | - | n/a | 0.7 | 63.6 | 15 / 24 / 24 | 0.16 | 16 | 3.9 / 8.6 / 11.0 | 0 / 0 |  |
| open-5mb | 2 | n/a | 0 | 20.063 / 73.727 / 73.727 | - | n/a | 0.6 | 41.4 | 15 / 24 / 24 | 0.08 | 8 | 5.8 / 9.2 / 12.1 | 0 / 0 | x |
| open-5mb | 3 | n/a | 0 | 25.535 / 66.943 / 66.943 | - | n/a | 0.7 | 63.3 | 14 / 24 / 24 | 0.06 | 6 | 3.1 / 5.2 / 5.6 | 0 / 0 |  |
| idle-500 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 15 / 59 / 26 | 2.75 | 30 | 5.5 / 5.3 / 12.8 | 0 / 0 | x |
| idle-500 | 2 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 15 / 59 / 26 | 2.28 | 23 | 6.7 / 4.8 / 16.4 | 0 / 0 |  |
| idle-500 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 15 / 60 / 25 | 1.69 | 23 | 6.5 / 2.4 / 9.4 | 0 / 0 |  |
| idle-1000 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 15 / 143 / 31 | 2.84 | 39 | 0.7 / 2.1 / 8.2 | 0 / 0 | x |
| idle-1000 | 2 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.1 | - | 15 / 142 / 31 | 3.31 | 39 | 1.9 / 2.6 / 7.2 | 0 / 0 |  |
| idle-1000 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.2 | - | 15 / 144 / 29 | 3.66 | 40 | 2.3 / 5.0 / 19.7 | 0 / 0 |  |
| idle-2000 | 1 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.4 | - | 15 / 387 / 35 | 12.03 | 228 | 3.7 / 8.0 / 26.0 | 0 / 0 | x |
| idle-2000 | 2 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.4 | - | 15 / 396 / 37 | 12.06 | 205 | 0.7 / 4.6 / 21.1 | 0 / 0 |  |
| idle-2000 | 3 | 0 / 0 / 0 / 0 / 0 / 0 | 0 | - | - | 0.0 | 60.5 | - | 14 / 385 / 37 | 11.77 | 140 | 0.5 / 4.7 / 24.5 | 0 / 0 |  |

#### rust-0.2.0-dev-19b-postgres (targets): the median run of each scenario

| Run | connections | changes (sent / accepted / rejected / timeouts / errors / dropped) | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | doc bytes | welcome bytes | first open | RSS MB | server CPU s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| many-docs 2000 docs x 5 editors, rate 0.1 | 10000 | 60000 / 60000 / 0 / 0 / 0 / 0 | 3.155 / 105.471 / 865.279 | 5.639 / 359.679 / 1163 | 982.4 | 61 | 2645 | 2218-2745 | - | 15 / 500 / 331 | 3.84 |
| many-docs 1000 docs x 5 editors, rate 0.1 | 5000 | 30000 / 30000 / 0 / 0 / 0 / 0 | 2.759 / 6.359 / 202.111 | 6.327 / 18.351 / 266.239 | 497.2 | 60 | 2645 | 2218-2744 | - | 15 / 268 / 191 | 1.89 |

#### rust-0.2.0-dev-19b-postgres (targets): every run

| Scenario | run | changes (sent / accepted / rejected / timeouts / errors / dropped) | refused connections | service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | first open | RSS MB | server CPU s | busiest second, % of a core | machine CPU % before / mean / max | server log warnings / errors | median |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| target-manydocs-2000x5 | 1 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 3.155 / 105.471 / 865.279 | 5.639 / 359.679 / 1163 | 982.4 | 61.1 | - | 15 / 500 / 331 | 3.84 | 39 | 0.4 / 2.0 / 10.6 | 0 / 0 | x |
| target-manydocs-2000x5 | 2 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 3.163 / 53.567 / 500.479 | 5.563 / 177.663 / 859.135 | 982.2 | 61.1 | - | 14 / 502 / 333 | 3.67 | 29 | 0.3 / 2.1 / 12.0 | 0 / 0 |  |
| target-manydocs-2000x5 | 3 | 60000 / 60000 / 0 / 0 / 0 / 0 | 0 | 3.207 / 152.063 / 918.015 | 5.663 / 513.279 / 1452 | 977.1 | 61.4 | - | 15 / 506 / 333 | 3.52 | 31 | 0.5 / 2.2 / 18.0 | 0 / 0 |  |
| target-manydocs-1000x5 | 1 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 2.759 / 6.359 / 202.111 | 6.327 / 18.351 / 266.239 | 497.2 | 60.3 | - | 15 / 268 / 191 | 1.89 | 25 | 0.3 / 1.3 / 5.3 | 0 / 0 | x |
| target-manydocs-1000x5 | 2 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 2.773 / 6.467 / 223.231 | 6.495 / 18.671 / 265.471 | 497.1 | 60.4 | - | 15 / 267 / 191 | 1.95 | 31 | 0.7 / 1.4 / 6.5 | 0 / 0 |  |
| target-manydocs-1000x5 | 3 | 30000 / 30000 / 0 / 0 / 0 / 0 | 0 | 2.719 / 6.235 / 214.911 | 6.347 / 18.367 / 267.775 | 496.9 | 60.4 | - | 14 / 270 / 192 | 2.56 | 37 | 4.3 / 1.4 / 5.3 | 0 / 0 |  |
