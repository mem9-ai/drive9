#!/usr/bin/env python3
"""D-series: effect of --upload-concurrency on the parallel write + drain benchmark."""

import json
import pathlib
import subprocess
import sys

from tune_sweep import BASE, MOUNT, restart_mount

VARIANTS = [
    ("d0-uc4-nobatch", ["--durability", "auto", "--upload-concurrency", "4"]),
    ("d1-uc64-nobatch", ["--durability", "auto", "--upload-concurrency", "64"]),
    ("d2-uc64-batch50ms", ["--durability", "auto", "--upload-concurrency", "64",
                           "--writeback-batch-window", "50ms"]),
]


def bench(tag):
    root = pathlib.Path(MOUNT) / ("batchbench2-" + tag)
    proc = subprocess.run([sys.executable, str(BASE / "batch_bench2.py"), str(root), "8", "128"],
                          capture_output=True, text=True, timeout=900)
    return proc.stdout.strip()


def main():
    rows = []
    try:
        for tag, extra in VARIANTS:
            restart_mount(extra)
            line = bench(tag)
            rows.append({"tag": tag, "flags": extra, "result": line})
            print(f"{tag:22s} {line}", flush=True)
    finally:
        restart_mount(["--durability", "fsync"])
        (BASE / "uc-results.json").write_text(json.dumps(rows, indent=2))
        print("restored baseline mount", flush=True)


if __name__ == "__main__":
    main()
