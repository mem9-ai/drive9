#!/usr/bin/env python3
"""Final round: confirm append-log, test write-sync+append-log, retry extent with settle."""

import json
import time

from tune_sweep import BASE, measure, restart_mount

VARIANTS = [
    ("g0-auto-appendlog", ["--durability", "auto", "--append-log", "**/*.txt"], 0),
    ("g1-writesync-appendlog", ["--durability", "write-sync", "--append-log", "**/*.txt"], 0),
    ("g2-auto-extent", ["--durability", "auto", "--extent", "**/*.txt"], 8),
]


def main():
    rows = []
    try:
        for tag, extra, settle in VARIANTS:
            restart_mount(extra)
            if settle:
                time.sleep(settle)
            row = measure(tag)
            row["flags"] = extra
            rows.append(row)
            print(f"{tag:24s} unzip300={row['duration_s']} verified={row['verified']}", flush=True)
    finally:
        restart_mount(["--durability", "fsync"])
        (BASE / "alt2-results.json").write_text(json.dumps(rows, indent=2))
        print("restored baseline mount", flush=True)


if __name__ == "__main__":
    main()
