"""Run the baseline's load scenarios against the Rust server and write the reports.

    cargo build --release -j 2 -p oswx-loadgen -p oswx-serverd
    .venv/Scripts/python bench/run_rust.py run --database sqlite   --out bench/results/rust-<version>-sqlite
    .venv/Scripts/python bench/run_rust.py run --database postgres --out bench/results/rust-<version>-postgres
    .venv/Scripts/python bench/run_rust.py table bench/results/rust-<version>-sqlite
    .venv/Scripts/python bench/run_rust.py compare bench/results/python-0.1.2-sqlite         bench/results/rust-<version>-sqlite bench/results/rust-<version>-postgres

Needs psutil, and for `--database postgres` pixeltable-pgserver (a throwaway PostgreSQL 16, as
tools/test_postgres_rust.py uses; it is stopped and removed at the end).

Every run gets a server of its own: a fresh database (a new SQLite file, or a new database in the
PostgreSQL instance) and a fresh storage folder; the server is killed afterwards. Each scenario is run
`--runs` times (3). For every run there is `<scenario>.run<n>.json`, loadgen's report, and an entry in
`runs.json` with what this script measured from outside: the server process's working set (before loadgen
starts, the highest of the once-a-second samples, and at the end), its CPU time, how busy the machine was,
and the warnings and errors in the server's log. A run in which loadgen failed has no report; its entry
says what happened.

`table` prints the Markdown tables of bench/results/README.md from those files; `compare` prints the
baseline's single run next to the median run of each folder. The median run of a scenario is the middle one
by service p99 (by the server's peak memory when nothing is timed, as in `idle`).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request

import psutil

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXE = ".exe" if os.name == "nt" else ""
SERVER = os.path.join(REPO, "target", "release", "openswx-collab-server" + EXE)
LOADGEN = os.path.join(REPO, "target", "release", "loadgen" + EXE)
PORT = 8791

# One account opens every connection (as in the baseline), so the per-account limit (64 by default) is
# raised out of the way, and so is the server-wide limit (4,096 by default), which only the
# 10,000-connection target scenario reaches. Everything else is the server's default.
SERVER_ENV = {"OSWX_COLLAB_MAX_CONNECTIONS_PER_USER": "20000", "OSWX_COLLAB_MAX_CONNECTIONS": "20000"}

# The baseline's scenarios, with its parameters (bench/results/python-0.1.2-sqlite/*.json).
COMPARISON = [
    ("onedoc-10", ["one-doc", "--editors", "10", "--rate", "2", "--features", "200"]),
    ("onedoc-40", ["one-doc", "--editors", "40", "--rate", "2", "--features", "200"]),
    ("onedoc-100", ["one-doc", "--editors", "100", "--rate", "2", "--features", "200"]),
    ("onedoc-200", ["one-doc", "--editors", "200", "--rate", "2", "--features", "200"]),
    ("onedoc-40-conflict", ["one-doc", "--editors", "40", "--rate", "2", "--features", "20"]),
    ("onedoc-10-rate20", ["one-doc", "--editors", "10", "--rate", "20", "--features", "200"]),
    ("manydocs-200x3", ["many-docs", "--docs", "200", "--editors-per-doc", "3", "--rate", "1"]),
    ("open-100kb", ["open", "--doc-bytes", "100000", "--times", "20"]),
    ("open-1mb", ["open", "--doc-bytes", "1000000", "--times", "20"]),
    ("open-5mb", ["open", "--doc-bytes", "5000000", "--times", "20"]),
    ("idle-500", ["idle", "--connections", "500"]),
    ("idle-1000", ["idle", "--connections", "1000"]),
    ("idle-2000", ["idle", "--connections", "2000"]),
]

# Not in the baseline: the size the design proposes as a target (10,000 connections on 2,000 open
# documents), and half of it. Kept apart from the comparison. The target names no edit rate: here each
# connection edits once every 10 s, 1,000 edits a second in all (500 for the half).
TARGETS = [
    ("target-manydocs-2000x5",
     ["many-docs", "--docs", "2000", "--editors-per-doc", "5", "--rate", "0.1"]),
    ("target-manydocs-1000x5",
     ["many-docs", "--docs", "1000", "--editors-per-doc", "5", "--rate", "0.1"]),
]

SETS = {"comparison": COMPARISON, "targets": TARGETS}


def clean_env() -> dict[str, str]:
    """This process's environment without anything that would be a server setting."""
    return {k: v for k, v in os.environ.items() if not k.startswith(("OSWX_", "AWS_"))}


def wait_healthy(base: str, proc: subprocess.Popen, seconds: float = 60.0) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"the server ended while starting (exit code {proc.returncode})")
        try:
            with urllib.request.urlopen(f"{base}/api/health", timeout=2) as r:
                if r.status == 200:
                    return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("the server did not answer /api/health")


def log_findings(path: str) -> dict:
    """How many warnings and errors the server logged, and the first few different ones."""
    warn = error = lines = 0
    samples: list[str] = []
    seen: set[str] = set()
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                lines += 1
                level = re.search(r"\b(WARN|ERROR)\b", line[:80])
                if not level:
                    continue
                if level.group(1) == "WARN":
                    warn += 1
                else:
                    error += 1
                # The same message with other numbers in it is the same message.
                key = re.sub(r"[0-9a-f]{8,}|\d+", "#", line[30:])[:120]
                if key not in seen and len(samples) < 8:
                    seen.add(key)
                    samples.append(line.strip()[:400])
    except OSError:
        pass
    return {"lines": lines, "warnings": warn, "errors": error, "samples": samples}


def stderr_findings(path: str) -> dict:
    lines = []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = [line.strip() for line in f if line.strip()]
    except OSError:
        pass
    distinct: list[str] = []
    seen: set[str] = set()
    for line in lines:
        key = re.sub(r"\d+", "#", line)[:120]
        if key not in seen and len(distinct) < 8:
            seen.add(key)
            distinct.append(line[:400])
    return {"lines": len(lines), "samples": distinct}


def remove_tree(path: str) -> None:
    for _ in range(20):
        shutil.rmtree(path, ignore_errors=True)
        if not os.path.exists(path):
            return
        time.sleep(0.5)


def one_run(name: str, args: list[str], n: int, database: str, pg, out_dir: str, duration: str,
            version_note: str) -> dict:
    work = tempfile.mkdtemp(prefix="oswx-bench-")
    folder = work.replace("\\", "/")
    env = clean_env()
    env.update(SERVER_ENV)
    env["OSWX_COLLAB_STORAGE_URL"] = f"{folder}/storage"
    pg_db = None
    if database == "sqlite":
        env["OSWX_COLLAB_DATABASE_URL"] = f"sqlite://{folder}/collab.db"
    else:
        pg_db = f"bench_{int(time.time() * 1000)}"
        pg.psql(f"CREATE DATABASE {pg_db};")
        env["OSWX_COLLAB_DATABASE_URL"] = pg.get_uri(database=pg_db)
    base = f"http://127.0.0.1:{PORT}"
    report = os.path.join(out_dir, f"{name}.run{n}.json")
    if os.path.exists(report):
        os.remove(report)
    record: dict = {"scenario": name, "run": n, "database": database, "args": args,
                    "report": os.path.basename(report)}
    server_log = os.path.join(work, "server.log")
    loadgen_err = os.path.join(work, "loadgen.err")
    server = None
    try:
        # How busy the machine is before anything of this run starts.
        record["machine_cpu_percent_before"] = psutil.cpu_percent(interval=2.0)
        with open(server_log, "wb") as log:
            server = subprocess.Popen([SERVER, "serve", "--host", "127.0.0.1", "--port", str(PORT)],
                                      env=env, cwd=work, stdin=subprocess.DEVNULL, stdout=log,
                                      stderr=subprocess.STDOUT)
        wait_healthy(base, server)
        proc = psutil.Process(server.pid)
        rss_before = proc.memory_info().rss
        cpu_before = sum(proc.cpu_times()[:2])
        proc.cpu_percent(None)
        psutil.cpu_percent(None)
        command = [LOADGEN, "--dialect", "v2", "--url", base, "--duration", duration,
                   "--database", database, "--out", report,
                   "--note", f"scenario {name} run {n} - {version_note} on {database} - "
                             "server and loadgen on same machine",
                   *args]
        if args[0] == "idle":
            command += ["--pid", str(server.pid)]
        started = time.monotonic()
        with open(loadgen_err, "wb") as err:
            loadgen = subprocess.Popen(command, cwd=REPO, env=clean_env(), stdin=subprocess.DEVNULL,
                                       stdout=subprocess.DEVNULL, stderr=err)
            peak = rss_before
            cpu_peak = 0.0
            machine: list[float] = []
            threads_peak = 0
            while loadgen.poll() is None:
                time.sleep(1.0)
                try:
                    peak = max(peak, proc.memory_info().rss)
                    cpu_peak = max(cpu_peak, proc.cpu_percent(None))
                    threads_peak = max(threads_peak, proc.num_threads())
                except psutil.Error:
                    record["server_died"] = True
                    break
                machine.append(psutil.cpu_percent(None))
            loadgen.wait()
        record["loadgen_wall_s"] = round(time.monotonic() - started, 1)
        record["loadgen_exit"] = loadgen.returncode
        try:
            record["server_rss_bytes"] = {"before": rss_before, "peak": peak,
                                          "end": proc.memory_info().rss}
            # CPU seconds over setup, connecting and the run; and the busiest one-second sample, in
            # percent of one core.
            record["server_cpu_s"] = round(sum(proc.cpu_times()[:2]) - cpu_before, 2)
            record["server_cpu_peak_percent"] = round(cpu_peak)
            record["server_threads_peak"] = threads_peak
        except psutil.Error:
            record["server_died"] = True
        if machine:
            record["machine_cpu_percent"] = {"mean": round(statistics.fmean(machine), 1),
                                             "max": round(max(machine), 1)}
        record["server_alive_after"] = server.poll() is None
    except Exception as e:  # noqa: BLE001 - whatever went wrong is the result of this run
        record["failed"] = f"{type(e).__name__}: {e}"
    finally:
        if server is not None and server.poll() is None:
            server.kill()
            server.wait()
        record["server_log"] = log_findings(server_log)
        record["loadgen_stderr"] = stderr_findings(loadgen_err)
        if pg_db is not None:
            try:
                pg.psql(f"DROP DATABASE IF EXISTS {pg_db} WITH (FORCE);")
            except Exception as e:  # noqa: BLE001
                record["drop_database_failed"] = str(e)[:200]
        remove_tree(work)
    record["has_report"] = os.path.exists(report)
    return record


def pg_setting(pg, name: str) -> str:
    return pg.psql(f"show {name};").strip().splitlines()[-2].strip()


def cmd_run(a: argparse.Namespace) -> int:
    for path in (SERVER, LOADGEN):
        if not os.path.exists(path):
            print(f"{path} is missing: build it first (see the top of this file)", file=sys.stderr)
            return 2
    scenarios = SETS[a.set]
    if a.only:
        wanted = a.only.split(",")
        scenarios = [s for s in scenarios if s[0] in wanted]
    os.makedirs(a.out, exist_ok=True)
    runs_path = os.path.join(a.out, "runs.json")
    runs: list[dict] = []
    if os.path.exists(runs_path):
        with open(runs_path, encoding="utf-8") as f:
            runs = json.load(f)["runs"]
    version = subprocess.run([SERVER, "--version"], capture_output=True, text=True).stdout.strip()
    meta: dict = {"server": version, "server_env": SERVER_ENV, "port": PORT, "duration": a.duration}
    pg = None
    pg_data = None
    try:
        if a.database == "postgres":
            import pixeltable_pgserver as pgserver
            pg_data = tempfile.mkdtemp(prefix="oswx-bench-pg-")
            pg = pgserver.get_server(pg_data)
            meta["postgres"] = {name: pg_setting(pg, name) for name in
                                ("server_version", "max_connections", "shared_buffers", "fsync",
                                 "synchronous_commit", "wal_sync_method", "listen_addresses")}
            print("PostgreSQL:", meta["postgres"], flush=True)
        for name, args in scenarios:
            for n in range(a.first_run, a.first_run + a.runs):
                print(f"{time.strftime('%H:%M:%S')} {name} run {n} ...", flush=True)
                record = one_run(name, args, n, a.database, pg, a.out, a.duration,
                                 f"rust server {version.split()[-1]}")
                runs = [r for r in runs if (r["scenario"], r["run"]) != (name, n)]
                runs.append(record)
                with open(runs_path, "w", encoding="utf-8", newline="\n") as f:
                    json.dump({**meta, "runs": runs}, f, indent=2)
                    f.write("\n")
                print("   ", {k: record.get(k) for k in
                              ("loadgen_exit", "loadgen_wall_s", "server_cpu_s", "failed")},
                      flush=True)
                time.sleep(a.pause)
    finally:
        if pg is not None:
            pg.cleanup()
        if pg_data is not None:
            remove_tree(pg_data)
    return 0


# ----------------------------------------------------------------------------------------- tables

def load(directory: str) -> tuple[dict, dict[str, list[tuple[dict, dict | None]]]]:
    with open(os.path.join(directory, "runs.json"), encoding="utf-8") as f:
        meta = json.load(f)
    by_scenario: dict[str, list[tuple[dict, dict | None]]] = {}
    for record in sorted(meta["runs"], key=lambda r: r["run"]):
        report = None
        path = os.path.join(directory, record["report"])
        if record.get("has_report") and os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                report = json.load(f)
        by_scenario.setdefault(record["scenario"], []).append((record, report))
    return meta, by_scenario


def sort_key(pair: tuple[dict, dict | None]) -> float:
    """What the median run is the median of: service p99, or peak memory when nothing is timed."""
    record, report = pair
    if report is None:
        return float("inf")
    if report.get("latency_ms"):
        return report["latency_ms"]["p99"]
    return record.get("server_rss_bytes", {}).get("peak", 0)


def median_run(pairs: list[tuple[dict, dict | None]]) -> tuple[dict, dict | None]:
    ordered = sorted(pairs, key=sort_key)
    return ordered[(len(ordered) - 1) // 2]


def num(x: float) -> str:
    return f"{x:.3f}".rstrip("0").rstrip(".") if x < 1000 else f"{x:.0f}"


def lat(l: dict | None) -> str:
    return "-" if not l else f"{num(l['p50'])} / {num(l['p99'])} / {num(l['max'])}"


def mb(record: dict) -> str:
    rss = record.get("server_rss_bytes")
    if not rss:
        return "?"
    return " / ".join(f"{rss[k] / 1e6:.0f}" for k in ("before", "peak", "end"))


def label(name: str, report: dict | None, record: dict) -> str:
    p = report["params"] if report else {}
    kind = record["args"][0]
    if kind == "one-doc":
        return f"one-doc {p['editors']} editors, rate {num(p['rate'])}, {p['features']} features"
    if kind == "many-docs":
        return f"many-docs {p['docs']} docs x {p['editors_per_doc']} editors, rate {num(p['rate'])}"
    if kind == "open":
        size = p["doc_bytes"]
        return f"open {size // 1000000} MB x{p['times']}" if size >= 1000000 \
            else f"open {size // 1000} KB x{p['times']}"
    if kind == "idle":
        return f"idle {p['connections']} connections"
    return name


def connections(record: dict) -> int:
    args = record["args"]
    get = lambda flag: int(args[args.index(flag) + 1])  # noqa: E731
    if args[0] == "one-doc":
        return get("--editors")
    if args[0] == "many-docs":
        return get("--docs") * get("--editors-per-doc")
    if args[0] == "idle":
        return get("--connections")
    return 1


def failed(record: dict, report: dict | None) -> str | None:
    """Why a run counts as failed, or None."""
    if report is None:
        return record.get("failed") or f"loadgen exit {record.get('loadgen_exit')}, no report"
    why = []
    if report["dropped_connections"]:
        why.append(f"{report['dropped_connections']} connections dropped")
    if report["connect_failed"]:
        why.append(f"{report['connect_failed']} connections refused")
    if report["timeouts"]:
        why.append(f"{report['timeouts']} timeouts")
    if report["errors"]:
        why.append(f"{report['errors']} edits lost")
    if report["protocol_errors"]:
        why.append(f"{report['protocol_errors']} protocol errors")
    if record.get("server_died") or record.get("server_alive_after") is False:
        why.append("the server died")
    return ", ".join(why) or None


def row(name: str, record: dict, report: dict | None, others_failed: str = "") -> str:
    if report is None:
        return f"| {name} (FAILED: {failed(record, report)}) | {connections(record)} " + "| - " * 10 + "|"
    kind = record["args"][0]
    title = label(name, report, record)
    if failed(record, report):
        title += " (FAILED)"
    elif others_failed:
        title += f" ({others_failed})"
    rejected = sum(report["rejected"].values())
    changes = "n/a" if kind == "open" else (
        f"{report['changes_sent']} / {report['accepted']} / {rejected} / {report['timeouts']} / "
        f"{report['errors']} / {report['dropped_connections']}")
    per_s = "n/a" if kind == "open" else f"{report['accepted_per_s']:.1f}"
    run_s = f"{report['duration_s']:.1f}" if kind == "open" else f"{report['duration_s']:.0f}"
    w = report.get("welcome_bytes")
    welcome = f"{w['min']}-{w['max']}" if w else "-"
    first = f"{report['first_open_ms']:.1f}" if report.get("first_open_ms") is not None else "-"
    return (f"| {title} | {connections(record)} | {changes} | {lat(report['latency_ms'])} | "
            f"{lat(report['latency_from_schedule_ms'])} | {per_s} | {run_s} | "
            f"{report['document_bytes']} | {welcome} | {first} | {mb(record)} | "
            f"{record.get('server_cpu_s', '?')} |")


HEADER = ("| Run | connections | changes (sent / accepted / rejected / timeouts / errors / dropped) | "
          "service p50 / p99 / max | schedule p50 / p99 / max | accepted per s | run s | doc bytes | "
          "welcome bytes | first open | RSS MB | server CPU s |\n" + "|---" * 12 + "|")


def cmd_table(a: argparse.Namespace) -> int:
    for directory in a.dirs:
        _, everything = load(directory)
        # The target scenarios are tables of their own, never rows among the comparison's.
        for what, wanted in (("comparison", False), ("targets", True)):
            by_scenario = {name: pairs for name, pairs in everything.items()
                           if name.startswith("target-") == wanted}
            if by_scenario:
                print_tables(f"{directory} ({what})", by_scenario)
    return 0


def print_tables(directory: str, by_scenario: dict[str, list[tuple[dict, dict | None]]]) -> None:
    print(f"\n### {directory}: the median run of each scenario\n")
    print(HEADER)
    for name, pairs in by_scenario.items():
        record, report = median_run(pairs)
        bad = sum(1 for r, j in pairs if failed(r, j))
        print(row(name, record, report, f"{bad} of {len(pairs)} runs FAILED" if bad else ""))
    print(f"\n### {directory}: every run\n")
    print("| Scenario | run | changes (sent / accepted / rejected / timeouts / errors / dropped) | "
          "refused connections | service p50 / p99 / max | schedule p50 / p99 / max | "
          "accepted per s | run s | first open | RSS MB | server CPU s | busiest second, % of a core "
          "| machine CPU % before / mean / max | server log warnings / errors | median |")
    print("|---" * 15 + "|")
    for name, pairs in by_scenario.items():
        chosen = median_run(pairs)[0]["run"]
        for record, report in pairs:
            log = record.get("server_log", {})
            m = record.get("machine_cpu_percent", {})
            machine = (f"{record.get('machine_cpu_percent_before', '?')} / {m.get('mean', '?')} / "
                       f"{m.get('max', '?')}")
            if report is None:
                print(f"| {name} | {record['run']} | FAILED: {failed(record, report)} " + "| - " * 8
                      + f"| - | {machine} | {log.get('warnings')} / {log.get('errors')} | |")
                continue
            kind = record["args"][0]
            rejected = sum(report["rejected"].values())
            changes = "n/a" if kind == "open" else (
                f"{report['changes_sent']} / {report['accepted']} / {rejected} / "
                f"{report['timeouts']} / {report['errors']} / {report['dropped_connections']}")
            first = (f"{report['first_open_ms']:.1f}"
                     if report.get("first_open_ms") is not None else "-")
            why = failed(record, report)
            print(f"| {name}{' (FAILED: ' + why + ')' if why else ''} | {record['run']} | {changes} | "
                  f"{report['connect_failed']} | {lat(report['latency_ms'])} | "
                  f"{lat(report['latency_from_schedule_ms'])} | "
                  f"{'n/a' if kind == 'open' else format(report['accepted_per_s'], '.1f')} | "
                  f"{report['duration_s']:.1f} | {first} | {mb(record)} | "
                  f"{record.get('server_cpu_s', '?')} | {record.get('server_cpu_peak_percent', '?')} | "
                  f"{machine} | {log.get('warnings')} / {log.get('errors')} | "
                  f"{'x' if record['run'] == chosen else ''} |")


def baseline_rss(directory: str, name: str) -> str:
    """The peak working set the baseline's run script wrote down (UTF-16 text), in MB."""
    try:
        with open(os.path.join(directory, f"{name}.server-rss.txt"), encoding="utf-16") as f:
            return f"{int(re.search(r'peak=(\d+)', f.read()).group(1)) / 1e6:.0f}"
    except (OSError, AttributeError):
        return "?"


def cmd_compare(a: argparse.Namespace) -> int:
    """The baseline's single run next to the median run of each Rust folder."""
    loaded = [load(d) for d in a.dirs]
    names = [os.path.basename(os.path.normpath(d)).rsplit("-", 1)[-1] for d in a.dirs]
    head = ["Scenario", "Python accepted / rejected", "Python service p50 / p99 / max",
            "Python accepted per s (run s)", "Python peak RSS MB"]
    for n in names:
        head += [f"Rust {n} accepted / rejected", f"Rust {n} service p50 / p99 / max",
                 f"Rust {n} accepted per s (run s)", f"Rust {n} peak RSS MB"]
    print("| " + " | ".join(head) + " |")
    print("|---" * len(head) + "|")

    def cells(report: dict, peak: str, note: str) -> list[str]:
        if report["scenario"] == "open":
            counts, per_s = "n/a", f"n/a ({report['duration_s']:.1f})"
        else:
            counts = f"{report['accepted']} / {sum(report['rejected'].values())}"
            per_s = f"{report['accepted_per_s']:.1f} ({report['duration_s']:.0f})"
        return [counts + note, lat(report["latency_ms"]), per_s, peak]

    for name, _ in COMPARISON:
        with open(os.path.join(a.baseline, f"{name}.json"), encoding="utf-8") as f:
            base = json.load(f)
        note = f" (FAILED: {base['dropped_connections']} dropped)" if base["dropped_connections"] else ""
        line = [name, *cells(base, baseline_rss(a.baseline, name), note)]
        for _, by_scenario in loaded:
            pairs = by_scenario.get(name)
            if not pairs:
                line += ["not run"] * 4
                continue
            record, report = median_run(pairs)
            bad = sum(1 for r, j in pairs if failed(r, j))
            note = f" ({bad} of {len(pairs)} runs FAILED)" if bad else ""
            if report is None:
                line += [f"FAILED: {failed(record, report)}", "-", "-", "-"]
            else:
                peak = f"{record['server_rss_bytes']['peak'] / 1e6:.0f}"
                line += cells(report, peak, note)
        print("| " + " | ".join(line) + " |")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run the scenarios")
    run.add_argument("--database", choices=["sqlite", "postgres"], required=True)
    run.add_argument("--out", required=True, help="the folder for the reports")
    run.add_argument("--set", choices=sorted(SETS), default="comparison")
    run.add_argument("--only", help="scenario names, separated by commas")
    run.add_argument("--runs", type=int, default=3)
    run.add_argument("--first-run", type=int, default=1,
                     help="the number of the first run (to add runs to a folder without replacing any)")
    run.add_argument("--duration", default="60s")
    run.add_argument("--pause", type=float, default=5.0, help="seconds between runs")
    run.set_defaults(func=cmd_run)
    table = sub.add_parser("table", help="print the Markdown tables of a results folder")
    table.add_argument("dirs", nargs="+")
    table.set_defaults(func=cmd_table)
    compare = sub.add_parser("compare", help="print the baseline next to the Rust folders' median runs")
    compare.add_argument("baseline", help="bench/results/python-0.1.2-sqlite")
    compare.add_argument("dirs", nargs="+")
    compare.set_defaults(func=cmd_compare)
    a = parser.parse_args()
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
