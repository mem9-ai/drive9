"""Causal experiment for the rename EIO: does an OPEN handle on the target
trigger it?

Variants (writer always: write tmp0/tmp1 alternately -> fsync -> replace):
  V1 no reader                (baseline, expected: no EIO)
  V2 reader only stats the path (no open)
  V3 reader opens+reads+closes  (transient handle)
  V4 reader holds one open fd and reads through it (persistent handle)
  V5 reader holds an open fd on the tmp files instead of the target

usage: python3 repro_eio4.py <workdir> [iterations]
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
ITERATIONS = 40


PACE = 0.002
MAX_READS = 20000


def reader_stat_only(target, stop_event, queue):
    reads = 0
    while not stop_event.is_set() and reads < MAX_READS:
        try:
            os.stat(target)
            reads += 1
        except OSError:
            break
        time.sleep(PACE)
    queue.put({"mode": "stat_only", "reads": reads})


def reader_open_read(target, stop_event, queue):
    reads = 0
    while not stop_event.is_set() and reads < MAX_READS:
        try:
            with open(target, "rb") as fh:
                fh.read()
            reads += 1
        except OSError:
            break
        time.sleep(PACE)
    queue.put({"mode": "open_read", "reads": reads})


def reader_hold_fd(target, stop_event, queue):
    """Open once and keep reading through the same fd."""
    reads = 0
    try:
        fh = open(target, "rb")
    except OSError as err:
        queue.put({"mode": "hold_fd", "error": str(err)})
        return
    try:
        while not stop_event.is_set() and reads < MAX_READS:
            try:
                fh.seek(0)
                fh.read()
                reads += 1
            except OSError:
                break
            time.sleep(PACE)
    finally:
        try:
            fh.close()
        except OSError:
            pass
    queue.put({"mode": "hold_fd", "reads": reads})


def reader_hold_tmp(tmp_path, stop_event, queue):
    reads = 0
    fh = None
    while not stop_event.is_set() and reads < MAX_READS:
        try:
            if fh is None:
                fh = open(tmp_path, "rb")
            fh.seek(0)
            fh.read()
            reads += 1
            time.sleep(PACE)
        except FileNotFoundError:
            if fh is not None:
                try:
                    fh.close()
                except OSError:
                    pass
                fh = None
            time.sleep(0.001)
        except OSError:
            break
    if fh is not None:
        try:
            fh.close()
        except OSError:
            pass
    queue.put({"mode": "hold_tmp", "reads": reads})


def run_variant(base, label, reader_fn, iterations=ITERATIONS):
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
    reader = None
    if reader_fn is not None:
        reader = ctx.Process(target=reader_fn,
                             args=(str(target) if "tmp" not in label else str(root / "app.js.tmp0"),
                                   stop_event, queue))
        reader.start()
        time.sleep(0.3)

    errors = []
    for i in range(iterations):
        tmp = root / ("app.js.tmp%d" % (i % 2))
        try:
            with open(tmp, "wb") as fh:
                fh.write(VERSIONS[i % 2])
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
        except OSError as err:
            errors.append({"iter": i, "tmp": tmp.name, "errno": err.errno, "msg": str(err)[:120]})
            if len(errors) >= 6:
                break

    reader_result = None
    if reader is not None:
        stop_event.set()
        reader.join(timeout=20)
        try:
            reader_result = queue.get(timeout=10)
        except Exception:
            reader_result = {"error": "no result"}

    leftovers = sorted(p.name for p in root.iterdir() if p.name != "app.js")
    return {"variant": label, "iterations": i + 1, "errors": len(errors),
            "error_detail": errors[:4], "reader": reader_result, "leftovers": leftovers}


def main():
    base = pathlib.Path(sys.argv[1])
    global ITERATIONS
    if len(sys.argv) > 2:
        ITERATIONS = int(sys.argv[2])
    rows = []
    rows.append(run_variant(base, "v1-no-reader", None))
    rows.append(run_variant(base, "v2-stat-only", reader_stat_only))
    rows.append(run_variant(base, "v3-open-read", reader_open_read))
    rows.append(run_variant(base, "v4-hold-fd", reader_hold_fd))
    rows.append(run_variant(base, "v5-hold-tmp", reader_hold_tmp))
    print(json.dumps(rows, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
