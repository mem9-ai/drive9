#!/usr/bin/env python3
"""Decompose the concurrent-files case: which per-file ops drive the cost."""

import multiprocessing as mp
import os
import pathlib
import subprocess
import sys
import time

PAYLOAD = b"x" * 4096
BIN = os.environ.get("DRIVE9_BENCH_BIN", "/home/ubuntu/drive9-main-fe9cdcf/drive9")


def worker(root, idx, per, gate, out, do_fsync, do_read, do_unlink):
    d = pathlib.Path(root) / f"w{idx}"
    d.mkdir(parents=True, exist_ok=True)
    gate.wait()
    for i in range(per):
        p = d / f"f{i:04d}.dat"
        with p.open("wb") as f:
            f.write(PAYLOAD)
            f.flush()
            if do_fsync:
                os.fsync(f.fileno())
        if do_read:
            p.read_bytes()
        if do_unlink:
            p.unlink()
    out.put(idx)


def drain(mount):
    subprocess.run([BIN, "mount", "drain", "--timeout", "120s", "--json", mount],
                   capture_output=True, timeout=150)


def run(mount, root, label, do_fsync, do_read, do_unlink, workers=8, per=50):
    root = pathlib.Path(root)
    root.mkdir(parents=True, exist_ok=True)
    ctx = mp.get_context("fork")
    gate = ctx.Event()
    out = ctx.Queue()
    procs = [ctx.Process(target=worker, args=(root, i, per, gate, out, do_fsync, do_read, do_unlink))
             for i in range(workers)]
    for p in procs:
        p.start()
    time.sleep(1.0)
    t0 = time.perf_counter()
    gate.set()
    for _ in procs:
        out.get()
    wall = time.perf_counter() - t0
    for p in procs:
        p.join()
    drain(mount)
    print(f"{label:36s} wall={wall:6.2f}s", flush=True)
    return wall


def main():
    mount = sys.argv[1]
    root = sys.argv[2]
    print(f"=== mount: {mount} (payload 4096 B, 8 workers x 50 iters)", flush=True)
    variants = [
        ("v1 write only", False, False, False),
        ("v2 write+read", False, True, False),
        ("v3 write+fsync", True, False, False),
        ("v4 write+fsync+read", True, True, False),
        ("v5 write+read+unlink", False, True, True),
        ("v6 full case (fsync+read+unlink)", True, True, True),
    ]
    for idx, (label, f, r, u) in enumerate(variants, 1):
        run(mount, f"{root}/v{idx}", label, f, r, u)


if __name__ == "__main__":
    main()
