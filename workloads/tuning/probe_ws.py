#!/usr/bin/env python3
"""Verify write/close/utime latencies under --durability write-sync."""

import os
import pathlib
import time

from tune_sweep import MOUNT, restart_mount


def probe():
    target = pathlib.Path(MOUNT) / "probe-ws"
    target.mkdir(parents=True, exist_ok=True)
    payload = b"x" * 212
    for i in range(5):
        p = target / f"a{i}.dat"
        t0 = time.perf_counter()
        fd = os.open(p, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
        t1 = time.perf_counter()
        os.write(fd, payload)
        t2 = time.perf_counter()
        os.close(fd)
        t3 = time.perf_counter()
        os.utime(p, None)
        t4 = time.perf_counter()
        time.sleep(0.2)
        os.utime(p, None)
        t5 = time.perf_counter()
        print("open=%.1f write=%.1f close=%.1f utime_immediate=%.1f utime_settled=%.1f ms"
              % (1000 * (t1 - t0), 1000 * (t2 - t1), 1000 * (t3 - t2),
                 1000 * (t4 - t3), 1000 * ((t5 - t4) - 0.2)), flush=True)


def main():
    try:
        restart_mount(["--durability", "write-sync"])
        probe()
    finally:
        restart_mount(["--durability", "fsync"])
        print("restored baseline mount", flush=True)


if __name__ == "__main__":
    main()
