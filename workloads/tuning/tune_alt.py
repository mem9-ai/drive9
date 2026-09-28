#!/usr/bin/env python3
"""F-series: alternative content paths (extent layout, append-log) for the unzip case."""

import json

from tune_sweep import BASE, measure, restart_mount

VARIANTS = [
    ("f0-auto-extent", ["--durability", "auto", "--extent", "**/*.txt"]),
    ("f1-auto-appendlog", ["--durability", "auto", "--append-log", "**/*.txt"]),
]


def main():
    rows = []
    try:
        for tag, extra in VARIANTS:
            restart_mount(extra)
            row = measure(tag)
            row["flags"] = extra
            rows.append(row)
            print(f"{tag:22s} unzip300={row['duration_s']} verified={row['verified']}", flush=True)
    finally:
        restart_mount(["--durability", "fsync"])
        (BASE / "alt-results.json").write_text(json.dumps(rows, indent=2))
        print("restored baseline mount", flush=True)


if __name__ == "__main__":
    main()
