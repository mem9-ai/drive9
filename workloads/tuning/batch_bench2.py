#!/usr/bin/env python3
"""Parallel small-file writes plus drain: exercises the writeback batch window end to end."""

import json
import multiprocessing as mp
import os
import pathlib
import subprocess
import sys
import time

PAYLOAD = b"x" * 212
BIN = os.environ.get("DRIVE9_BENCH_BIN", "/home/ubuntu/drive9-main-fe9cdcf/drive9")
MOUNT = os.environ.get("DRIVE9_BENCH_MOUNT", "/mnt/d9-six-none-a")


def run_worker(root, idx, per, gate, out):
    d = pathlib.Path(root) / f"w{idx}"
    d.mkdir(parents=True, exist_ok=True)
    gate.wait()
    t0 = time.perf_counter()
    for i in range(per):
        with (d / f"f{i:04d}.dat").open("wb") as f:
            f.write(PAYLOAD)
    out.put((idx, time.perf_counter() - t0))


def main():
    root = pathlib.Path(sys.argv[1])
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    per = int(sys.argv[3]) if len(sys.argv) > 3 else 128
    root.mkdir(parents=True, exist_ok=True)
    ctx = mp.get_context("fork")
    gate = ctx.Event()
    out = ctx.Queue()
    procs = [ctx.Process(target=run_worker, args=(root, i, per, gate, out)) for i in range(workers)]
    for p in procs:
        p.start()
    time.sleep(1.0)
    t0 = time.perf_counter()
    gate.set()
    for p in procs:
        p.join()
    write_wall = time.perf_counter() - t0
    drain_start = time.perf_counter()
    proc = subprocess.run([BIN, "mount", "drain", "--timeout", "600s", "--json", MOUNT],
                          capture_output=True, text=True, timeout=650)
    drain_wall = time.perf_counter() - drain_start
    ok = False
    try:
        ok = json.loads(proc.stdout)["ok"]
    except Exception:
        pass
    total = workers * per
    print(f"files={total} write_phase={write_wall:.2f}s ({total / write_wall:.1f}/s) "
          f"drain={drain_wall:.2f}s drain_ok={ok} total={write_wall + drain_wall:.2f}s", flush=True)


if __name__ == "__main__":
    main()
