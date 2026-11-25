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
