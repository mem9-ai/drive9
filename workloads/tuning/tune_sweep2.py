#!/usr/bin/env python3
"""Confirmation round: repeat close-sync, test write-sync and debounce variants."""

import json
import pathlib

from tune_sweep import BASE, measure, restart_mount

VARIANTS = [
    ("v5b-close-sync", ["--durability", "close-sync"]),
    ("v6-write-sync", ["--durability", "write-sync"]),
    ("v7-close-sync-debounce0", ["--durability", "close-sync", "--flush-debounce", "0"]),
]


def main():
    results = []
    try:
        for tag, extra in VARIANTS:
            restart_mount(extra)
            row = measure(tag)
            row["flags"] = extra
            results.append(row)
            print(f"{tag:26s} unzip300={row['duration_s']} verified={row['verified']} flags={extra}", flush=True)
    finally:
        restart_mount(["--durability", "fsync"])
        (BASE / "tune-results2.json").write_text(json.dumps(results, indent=2))
        print("restored baseline mount; results in tune-results2.json", flush=True)


if __name__ == "__main__":
    main()
