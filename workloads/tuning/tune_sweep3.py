#!/usr/bin/env python3
"""Stability re-checks for the two best presets."""

import json
import pathlib

from tune_sweep import BASE, measure, restart_mount

VARIANTS = [
    ("v6b-write-sync", ["--durability", "write-sync"]),
    ("v5c-close-sync", ["--durability", "close-sync"]),
]


def main():
    results = []
    try:
        for tag, extra in VARIANTS:
            restart_mount(extra)
            row = measure(tag)
            row["flags"] = extra
            results.append(row)
            print(f"{tag:20s} unzip300={row['duration_s']} verified={row['verified']}", flush=True)
    finally:
        restart_mount(["--durability", "fsync"])
        (BASE / "tune-results3.json").write_text(json.dumps(results, indent=2))
        print("restored baseline mount; results in tune-results3.json", flush=True)


if __name__ == "__main__":
    main()
