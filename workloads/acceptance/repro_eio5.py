"""Bounded high-frequency reproducer for the rename EIO.

The trigger needs a reader that opens the target at high frequency (a paced
reader does not reproduce), but we must cap total operations so the machine is
not saturated. This script runs one writer plus one burst reader and reports the
error count, so the reproduction rate can be measured over several rounds.

usage: python3 repro_eio5.py <workdir> [writer_iterations] [reader_cap] [rounds]
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import pathlib
import shutil
import sys
import time

A = b"A" * 4096 + b"||END-A||"
B = b"B" * 7000 + b"||END-B||"
VERSIONS = (A, B)


def reader_burst(target, stop_event, queue, max_reads):
    """Open/read/close as fast as possible, capped to bound the load."""
    reads = 0
    while not stop_event.is_set() and reads < max_reads:
        try:
            with open(target, "rb") as fh:
                fh.read()
            reads += 1
        except OSError:
            break
    queue.put({"mode": "burst_open_read", "reads": reads})


def one_round(base, label, writer_iterations, reader_cap):
    root = base / label
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    target = root / "app.js"
    with open(target, "wb") as fh:
        fh.write(A)
        fh.flush()
        os.fsync(fh.fileno())

    ctx = mp.get_context("fork")
    stop_event = ctx.Event()
    queue = ctx.Queue()
    reader = ctx.Process(target=reader_burst, args=(str(target), stop_event, queue, reader_cap))
    reader.start()
    time.sleep(0.2)

    errors = []
    started = time.perf_counter()
    for i in range(writer_iterations):
        tmp = root / ("app.js.tmp%d" % (i % 2))
        try:
            with open(tmp, "wb") as fh:
                fh.write(VERSIONS[i % 2])
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
        except OSError as err:
            errors.append({"iter": i, "tmp": tmp.name, "errno": err.errno})
            if len(errors) >= 5:
                break
    elapsed = time.perf_counter() - started

    stop_event.set()
    reader.join(timeout=30)
    try:
        reader_result = queue.get(timeout=15)
    except Exception:
        reader_result = {"error": "no result"}

    leftovers = sorted(p.name for p in root.iterdir() if p.name != "app.js")
    return {"round": label, "writer_iterations": i + 1, "elapsed_s": round(elapsed, 3),
            "errors": len(errors), "error_detail": errors[:4],
            "reader": reader_result, "leftovers": leftovers}


def main():
    base = pathlib.Path(sys.argv[1])
    writer_iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 40
    reader_cap = int(sys.argv[3]) if len(sys.argv) > 3 else 20000
    rounds = int(sys.argv[4]) if len(sys.argv) > 4 else 3
    rows = [one_round(base, "r%d" % i, writer_iterations, reader_cap) for i in range(rounds)]
    total_errors = sum(row["errors"] for row in rows)
    print(json.dumps({"rounds": rows, "total_errors": total_errors,
                      "writer_iterations": writer_iterations, "reader_cap": reader_cap},
                     ensure_ascii=False, indent=1))
    raise SystemExit(0 if total_errors else 1)


if __name__ == "__main__":
    main()
