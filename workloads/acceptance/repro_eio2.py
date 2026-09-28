"""Reproducer v2: EIO on os.replace while a concurrent reader keeps the path open.

Mirrors S2: writer alternates two temp names (write -> fsync -> replace) and a
reader process continuously stat+opens+reads the target.

usage: python3 repro_eio2.py <workdir> [iterations]
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


def reader_worker(target, stop_event, queue, deadline, max_reads=20000, pace=0.002):
    """Repeated open/read/close with a small pacing sleep and a hard cap so the
    reproducer cannot saturate the machine."""
    reads = 0
    observations = {"A": 0, "B": 0}
    bad = []
    while not stop_event.is_set() and time.time() < deadline and reads < max_reads:
        try:
            st = os.stat(target)
            with open(target, "rb") as fh:
                data = fh.read()
            reads += 1
            if data == A:
                observations["A"] += 1
            elif data == B:
                observations["B"] += 1
            else:
                bad.append({"kind": "mixed", "size": len(data)})
                break
            if st.st_size != len(data):
                bad.append({"kind": "stat-mismatch", "stat": st.st_size, "read": len(data)})
                break
        except FileNotFoundError:
            bad.append({"kind": "missing"})
            break
        except OSError as err:
            bad.append({"kind": "oserror", "errno": err.errno, "msg": str(err)})
            break
    queue.put({"reads": reads, "observations": observations, "bad": bad})


def main():
    root = pathlib.Path(sys.argv[1])
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 200
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
    reader = ctx.Process(target=reader_worker, args=(str(target), stop_event, queue, time.time() + 300))
    reader.start()
    time.sleep(0.3)

    errors = []
    started = time.perf_counter()
    for i in range(iterations):
        tmp = root / ("app.js.tmp%d" % (i % 2))
        try:
            with open(tmp, "wb") as fh:
                fh.write(VERSIONS[i % 2])
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
        except OSError as err:
            errors.append({"iter": i, "errno": err.errno, "msg": str(err),
                           "tmp": str(tmp), "target": str(target)})
            if len(errors) >= 5:
                break
    elapsed = time.perf_counter() - started
    stop_event.set()
    reader.join(timeout=30)
    try:
        observation = queue.get(timeout=20)
    except Exception:
        observation = {"error": "reader produced no result"}

    result = {"iterations": i + 1, "elapsed_s": round(elapsed, 3),
              "writer_errors": errors, "reader": observation}
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(0 if not errors and not observation.get("bad") else 1)


if __name__ == "__main__":
    main()
