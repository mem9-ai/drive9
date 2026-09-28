"""Merge segmented official Node test-fs-* runs without hiding failures."""

import argparse
import collections
import csv
import json
import pathlib

from prepare_source import MANIFEST
from run_test_fs import test_id


def merge(paths):
    rows = {}
    sources = []
    for path in paths:
        summary = json.loads(path.read_text())
        if summary["source_sha256"] != MANIFEST["source_sha256"]:
            raise ValueError("Node source checksum differs: " + str(path))
        sources.append(str(path))
        for row in summary["rows"]:
            if row["test"] in rows:
                raise ValueError("Duplicate test: " + row["test"])
            rows[row["test"]] = row
    expected = [test_id(path) for path in MANIFEST["tests"]]
    missing = [name for name in expected if name not in rows]
    unexpected = sorted(set(rows) - set(expected))
    ordered = [rows[name] for name in expected if name in rows]
    return dict(
        node_version=MANIFEST["node_version"],
        source_sha256=MANIFEST["source_sha256"],
        source_summaries=sources,
        expected=len(expected),
        completed=len(ordered),
        missing=missing,
        unexpected=unexpected,
        statuses=dict(collections.Counter(row["status"] for row in ordered)),
        dirty_drains=[row["test"] for row in ordered if not row["drain_after"]["ok"]],
        rows=ordered,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summaries", nargs="+", type=pathlib.Path)
    parser.add_argument("--output-dir", required=True, type=pathlib.Path)
    args = parser.parse_args()
    result = merge(args.summaries)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    (args.output_dir / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    with (args.output_dir / "durations.csv").open("w", newline="") as output:
        fields = (
            "test",
            "status",
            "node_duration_ms",
            "wall_s",
            "drain_after_s",
            "wall_plus_drain_s",
            "configured_durability",
            "actual_sync_mode",
        )
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for row in result["rows"]:
            writer.writerow({key: row.get(key) for key in fields})
    print(
        json.dumps(
            {key: value for key, value in result.items() if key != "rows"}, indent=2
        )
    )
    if result["missing"] or result["unexpected"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
