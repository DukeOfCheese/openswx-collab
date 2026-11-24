"""The collaboration server under cProfile, for tools/load_test.py --profile.

    load_profile_server.py OUT_PREFIX --port PORT

Runs `serve` (settings from the OSWX_COLLAB_* environment, as usual) with the profiler on in the event loop's
thread, and writes OUT_PREFIX-<n>.prof every 20 s plus OUT_PREFIX-<n>.txt (the top functions by own time and
by cumulative time). Each file covers only the 20 s before it, so the one written during a step shows that step.

It also writes OUT_PREFIX-<n>-timing.json: wall-clock percentiles of Room.change (from the message being read
to the answer: waiting for the room lock plus the commit), of Room._commit alone (with the lock held: checks,
the database write, the broadcast) and of the event loop's lag (how late a 10 ms timer fires). --no-cprofile
keeps only these timings, without the profiler's overhead.
"""
from __future__ import annotations

import argparse
import cProfile
import io
import json
import pstats
import threading
import time

import uvicorn

from openswx_collab_server.app import create_app
from openswx_collab_server.config import Settings


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--every", type=float, default=20.0)
    ap.add_argument("--no-cprofile", action="store_true")
    a = ap.parse_args()
    from openswx_collab_server import hub
    timings: dict[str, list[float]] = {"change": [], "commit": [], "acquire": [], "loop_lag": []}

    def timed(name: str, fn):
        async def wrapper(*args, **kw):
            t = time.perf_counter()
            try:
                return await fn(*args, **kw)
            finally:
                timings[name].append(time.perf_counter() - t)
        return wrapper
    hub.Room.change = timed("change", hub.Room.change)
    hub.Room._commit = timed("commit", hub.Room._commit)
    hub.Room.acquire = timed("acquire", hub.Room.acquire)

    def summary() -> dict:
        out = {}
        for k in list(timings):
            xs, timings[k] = sorted(timings[k]), []
            if xs:
                out[k] = {"n": len(xs), **{f"p{q}": round(1000 * xs[min(len(xs) - 1, int(q / 100 * len(xs)))], 2)
                                           for q in (50, 95, 99)}, "max": round(1000 * xs[-1], 2)}
        return out
    settings = Settings.from_env()
    box = {"prof": cProfile.Profile()}
    lock = threading.Lock()

    def dump() -> None:
        n = 0
        while True:
            time.sleep(a.every)
            n += 1
            with lock:
                old, box["prof"] = box["prof"], cProfile.Profile()
                box["swap"] = True
            time.sleep(0.5)  # (the loop thread switches over at its next tick)
            with open(f"{a.out}-{n:03d}-timing.json", "w") as f:
                json.dump(summary(), f, indent=1)
            if a.no_cprofile:
                continue
            old.dump_stats(f"{a.out}-{n:03d}.prof")
            s = io.StringIO()
            st = pstats.Stats(old, stream=s)
            st.sort_stats("tottime").print_stats(40)
            st.sort_stats("cumulative").print_stats(60)
            with open(f"{a.out}-{n:03d}.txt", "w") as f:
                f.write(s.getvalue())

    app = create_app(settings)
    config = uvicorn.Config(app, host="127.0.0.1", port=a.port, workers=1, log_level="warning",
                            ws_max_size=settings.max_change_bytes + 65536)
    srv = uvicorn.Server(config)

    import asyncio

    async def run() -> None:
        current = {"p": box["prof"]}
        if not a.no_cprofile:
            current["p"].enable()

        async def lag() -> None:
            while True:
                t = time.perf_counter()
                await asyncio.sleep(0.01)
                timings["loop_lag"].append(time.perf_counter() - t - 0.01)
        lag_task = asyncio.create_task(lag())

        async def swapper() -> None:
            while True:
                await asyncio.sleep(0.1)
                if box.pop("swap", False) and not a.no_cprofile:
                    current["p"].disable()
                    current["p"] = box["prof"]
                    current["p"].enable()
        task = asyncio.create_task(swapper())
        try:
            await srv.serve()
        finally:
            task.cancel()
            lag_task.cancel()
    threading.Thread(target=dump, daemon=True).start()
    asyncio.run(run())


if __name__ == "__main__":
    main()
