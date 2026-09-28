#!/usr/bin/env python3
"""Does --upload-concurrency help the serial unzip case? (expected: no)"""

import json

from tune_sweep import BASE, measure, restart_mount

VARIANTS = [
    ("e1-auto-uc32-unzip", ["--durability", "auto", "--upload-concurrency", "32"]),
    ("e2-auto-uc64-unzip", ["--durability", "auto", "--upload-concurrency", "64"]),
]


def main():
    rows = []
    try:
        for tag, extra in VARIANTS:
            restart_mount(extra)
            row = measure(tag)
            row["flags"] = extra
            rows.append(row)
            print(f"{tag:24s} unzip300={row['duration_s']} verified={row['verified']}", flush=True)
    finally:
        restart_mount(["--durability", "fsync"])
        (BASE / "uc-unzip-results.json").write_text(json.dumps(rows, indent=2))
        print("restored baseline mount", flush=True)


if __name__ == "__main__":
    main()
