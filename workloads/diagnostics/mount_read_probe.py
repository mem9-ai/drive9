"""Measure mount-level read cost on the benchmark host and split it from metadata.

Reads N small files through the FUSE mount, then repeats the loop with open+close
only, so the data-read share can be compared against the direct-HTTP GET number
measured by server_op_probe.py on the same host.

Usage: mount_read_probe.py <mount-dir-with-small-files> [iterations]
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


def main():
    if len(sys.argv) < 2:
        print("usage: mount_read_probe.py <mount-dir-with-small-files> [iterations]", file=sys.stderr)
        return 2
    root = sys.argv[1]
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    names = sorted(n for n in os.listdir(root) if not n.startswith("."))[:iterations]
    if not names:
        print("no files to read", file=sys.stderr)
        return 1
    paths = [os.path.join(root, name) for name in names]
    sizes = {p: os.path.getsize(p) for p in paths}
    print(f"root={root} files={len(paths)} size={sorted(set(sizes.values()))}", flush=True)

    read_ms = []
    for path in paths:
        start = time.perf_counter()
        with open(path, "rb") as stream:
            _ = stream.read()
        read_ms.append((time.perf_counter() - start) * 1000)
    summarize("mount open+read+close", read_ms)

    open_ms = []
    for path in paths:
        start = time.perf_counter()
        with open(path, "rb"):
            pass
        open_ms.append((time.perf_counter() - start) * 1000)
    summarize("mount open+close (attributes only)", open_ms)

    return 0


if __name__ == "__main__":
    sys.exit(main())
