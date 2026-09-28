"""Compare read-after-write cost against steady-state read cost on the same mount.

Motivation: the many-small-files read phase measured 20-24 ms per file in the
production matrix, while a steady-state read of the same 212 B shape costs
15.47 ms on staging. This probe separates the two explanations:

  1. reading a file that was just written and may still have a pending commit,
  2. plain environment difference between the two hosts.

Usage: mount_read_after_write_probe.py <mount-dir> [iterations]
"""

import json
import os
import statistics
import sys
import time


def summarize(name, samples):
    s = sorted(samples)
    print(json.dumps({
        "op": name,
        "n": len(s),
        "avg_ms": round(statistics.mean(s), 2),
        "p50_ms": round(s[len(s) // 2], 2),
        "p95_ms": round(s[max(0, int(len(s) * 0.95) - 1)], 2),
        "max_ms": round(s[-1], 2),
    }, ensure_ascii=False), flush=True)


def read_once(path):
    start = time.perf_counter()
    with open(path, "rb") as stream:
        data = stream.read()
    return (time.perf_counter() - start) * 1000, len(data)


def main():
    if len(sys.argv) < 2:
        print("usage: mount_read_after_write_probe.py <mount-dir> [iterations]", file=sys.stderr)
        return 2
    root = os.path.join(sys.argv[1], "readprobe-" + str(int(time.time())))
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    os.makedirs(root, exist_ok=True)

    payload = b"x" * 212
    paths = [os.path.join(root, f"f-{i:04d}.dat") for i in range(iterations)]

    write_ms = []
    for path in paths:
        start = time.perf_counter()
        with open(path, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        write_ms.append((time.perf_counter() - start) * 1000)
    summarize("create+write+fsync (212 B)", write_ms)

    immediate, sizes = [], set()
    for path in paths:
        ms, size = read_once(path)
        immediate.append(ms)
        sizes.add(size)
    summarize("read immediately after write", immediate)
    assert sizes == {212}, sizes

    later = []
    for path in paths:
        ms, _ = read_once(path)
        later.append(ms)
    summarize("read again (steady state)", later)

    print(f"root={root}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
