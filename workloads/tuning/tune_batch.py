#!/usr/bin/env python3
"""A/B/C: parallel small-file writes with and without the writeback batch window."""

import json
import pathlib
import subprocess
import sys

from tune_sweep import BASE, MOUNT, restart_mount

VARIANTS = [
    ("b0-fsync-nobatch", ["--durability", "fsync"]),
    ("b1-auto-nobatch", ["--durability", "auto"]),
    ("b2-auto-batch50ms", ["--durability", "auto", "--writeback-batch-window", "50ms"]),
]


def bench(tag):
    root = pathlib.Path(MOUNT) / ("batchbench-" + tag)
    proc = subprocess.run([sys.executable, str(BASE / "batch_bench.py"), str(root), "8", "64"],
                          capture_output=True, text=True, timeout=900)
    return proc.stdout.strip()


def main():
    rows = []
    try:
        for tag, extra in VARIANTS:
            restart_mount(extra)
            line = bench(tag)
            rows.append({"tag": tag, "flags": extra, "result": line})
            print(f"{tag:20s} {line}", flush=True)
    finally:
        restart_mount(["--durability", "fsync"])
        (BASE / "batch-results.json").write_text(json.dumps(rows, indent=2))
        print("restored baseline mount", flush=True)


if __name__ == "__main__":
    main()
