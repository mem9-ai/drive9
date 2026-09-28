#!/usr/bin/env python3
"""Sample local disk utilization while running the concurrent-files pattern."""

import multiprocessing as mp
import os
import pathlib
import subprocess
import sys
import time

PAYLOAD = b"x" * 4096


def worker(root, idx, per, gate):
    d = pathlib.Path(root) / f"w{idx}"
    d.mkdir(parents=True, exist_ok=True)
    gate.wait()
    for i in range(per):
        p = d / f"f{i:04d}.dat"
        with p.open("wb") as f:
            f.write(PAYLOAD)
            f.flush()
            os.fsync(f.fileno())
        p.read_bytes()
        p.unlink()


def stats(dev):
    with open("/proc/diskstats") as fh:
        for line in fh:
            f = line.split()
            if f[2] == dev:
                return int(f[7]), int(f[9])
    raise SystemExit("device not found: " + dev)


def main():
    mount = sys.argv[1]
    dev = sys.argv[2]
    root = pathlib.Path(mount) / "disk-probe"
    subprocess.run(["rm", "-rf", str(root)])
    root.mkdir(parents=True)
    ctx = mp.get_context("fork")
    gate = ctx.Event()
    procs = [ctx.Process(target=worker, args=(root, i, 50, gate)) for i in range(8)]
    for p in procs:
        p.start()
    time.sleep(1.0)
    prev = stats(dev)
    t_prev = time.perf_counter()
    t0 = time.perf_counter()
    gate.set()
    peak_iops = peak_mbs = 0.0
    total_writes = 0
    total_sectors = 0
    samples = 0
    while any(p.is_alive() for p in procs):
        time.sleep(0.05)
        cur = stats(dev)
        now = time.perf_counter()
        dt = now - t_prev
        if dt <= 0:
            continue
        iops = (cur[0] - prev[0]) / dt
        mbs = (cur[1] - prev[1]) * 512 / 1e6 / dt
        peak_iops = max(peak_iops, iops)
        peak_mbs = max(peak_mbs, mbs)
        total_writes += cur[0] - prev[0]
        total_sectors += cur[1] - prev[1]
        samples += 1
        prev, t_prev = cur, now
    wall = time.perf_counter() - t0
    for p in procs:
        p.join()
    avg_iops = total_writes / wall if wall else 0
    avg_mbs = total_sectors * 512 / 1e6 / wall if wall else 0
    print(f"mount={mount} dev={dev} wall={wall:5.2f}s "
          f"peak_write_iops={peak_iops:7.0f} peak_write_MBps={peak_mbs:6.1f} "
          f"avg_write_iops={avg_iops:7.0f} avg_write_MBps={avg_mbs:6.1f} samples={samples}")


if __name__ == "__main__":
    main()
