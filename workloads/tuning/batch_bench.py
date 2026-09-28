#!/usr/bin/env python3
"""Parallel small-file writer benchmark to exercise the writeback batch window."""

import multiprocessing as mp
import pathlib
import sys
import time

PAYLOAD = b"x" * 212


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
    per = int(sys.argv[3]) if len(sys.argv) > 3 else 64
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
    wall = time.perf_counter() - t0
    total = workers * per
    print(f"workers={workers} files={total} wall={wall:.2f}s throughput={total / wall:.1f} files/s", flush=True)


if __name__ == "__main__":
    main()
