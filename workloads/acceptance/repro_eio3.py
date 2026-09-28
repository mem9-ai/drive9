"""Isolate the EIO trigger: mainline rapid replace with/without a concurrent reader.

Runs three phases against fresh directories:
  P1: alternating tmp0/tmp1, no reader
  P2: alternating tmp0/tmp1, with continuous reader
  P3: single tmp name, with continuous reader
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
ITERATIONS = 200


def reader_worker(target, stop_event, queue, deadline):
    reads = 0
    while not stop_event.is_set() and time.time() < deadline:
        try:
            with open(target, "rb") as fh:
                fh.read()
            reads += 1
        except OSError:
            break
    queue.put({"reads": reads})


def phase(root, with_reader, same_tmp, iterations=ITERATIONS):
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
    reader = None
    if with_reader:
        reader = ctx.Process(target=reader_worker, args=(str(target), stop_event, queue, time.time() + 300))
        reader.start()
        time.sleep(0.3)

    errors = []
    started = time.perf_counter()
    for i in range(iterations):
        tmp = root / ("app.js.tmp" if same_tmp else "app.js.tmp%d" % (i % 2))
        try:
            with open(tmp, "wb") as fh:
                fh.write(VERSIONS[i % 2])
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
        except OSError as err:
            errors.append({"iter": i, "tmp": tmp.name, "errno": err.errno, "msg": str(err)})
            if len(errors) >= 8:
                break
    elapsed = time.perf_counter() - started

    reads = None
    if reader is not None:
        stop_event.set()
        reader.join(timeout=30)
        try:
            reads = queue.get(timeout=10).get("reads")
        except Exception:
            reads = "?"

    leftovers = sorted(p.name for p in root.iterdir() if p.name != "app.js")
    return {"root": str(root), "with_reader": with_reader, "same_tmp": same_tmp,
            "iterations_done": i + 1, "elapsed_s": round(elapsed, 3),
            "error_count": len(errors), "errors": errors[:6],
            "leftovers": leftovers, "reader_reads": reads}


def main():
    base = pathlib.Path(sys.argv[1])
    out = []
    out.append(phase(base / "p1-no-reader", with_reader=False, same_tmp=False))
    out.append(phase(base / "p2-with-reader", with_reader=True, same_tmp=False))
    out.append(phase(base / "p3-single-tmp-reader", with_reader=True, same_tmp=True))
    print(json.dumps(out, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
