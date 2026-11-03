#!/usr/bin/env python3
"""fstest perf-baseline comparison (G6): extract benchmark metrics from the
fstest JSON artifacts a simulator suite run leaves behind, compare them
against the previous run's baseline, and exit red when any metric regresses
more than the threshold. Always writes the fresh baseline for next time.

Usage:
  perf_baseline.py --artifacts <suite-home> [--baseline FILE]
                   [--out FILE] [--threshold-pct 10]
"""
import argparse
import glob
import json
import os
import sys

METRICS = {
    "fio": lambda d: {
        f"fio/{direction}.bwBytes": sum(
            (j.get(direction) or {}).get("bwBytes", 0) for j in d.get("jobs", [])
        )
        for direction in ("read", "write")
    },
    "smallfile": lambda d: {
        f"smallfile/{p.get('operation')}.filesPerSec": (p.get("results") or {}).get(
            "filesPerSec", 0
        )
        for p in d.get("phases", [])
    },
    "mdtest": lambda d: {
        f"mdtest/{op}.totalRate": v.get("totalRate", 0)
        for op, v in (d.get("summary") or {}).items()
        if isinstance(v, dict)
    },
}


def collect(artifacts_root: str) -> dict:
    """Latest metric dict per suite from <root>/runs/r*/fstest-*.json."""
    latest: dict[str, tuple[int, dict]] = {}
    for path in glob.glob(os.path.join(artifacts_root, "**", "fstest-*.json"), recursive=True):
        try:
            with open(path) as f:
                report = json.load(f)
        except (OSError, ValueError):
            continue
        suite = report.get("suite", "")
        extractor = METRICS.get(suite)
        if not extractor:
            continue
        stamp = int(os.path.getmtime(path))
        prev = latest.get(suite)
        if prev is None or stamp > prev[0]:
            latest[suite] = (stamp, extractor(report))
    metrics: dict = {}
    for suite, (_, m) in sorted(latest.items()):
        metrics.update(m)
    return metrics


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts", required=True)
    ap.add_argument("--baseline")
    ap.add_argument("--out")
    ap.add_argument("--threshold-pct", type=float, default=10.0)
    args = ap.parse_args()

    current = collect(args.artifacts)
    if not current:
        print("perf-baseline: no benchmark metrics found (nothing to compare)")
        return 0

    old = {}
    if args.baseline and os.path.exists(args.baseline):
        try:
            with open(args.baseline) as f:
                old = json.load(f)
        except (OSError, ValueError) as e:
            print(f"perf-baseline: ignoring unreadable baseline: {e}")

    red = False
    print(f"perf-baseline: {len(current)} metrics (threshold -{args.threshold_pct}%)")
    for name in sorted(current):
        new_v, old_v = current[name], old.get(name)
        if not isinstance(old_v, (int, float)) or old_v <= 0:
            print(f"  NEW   {name} = {new_v:.1f}")
            continue
        delta_pct = (new_v - old_v) / old_v * 100.0
        verdict = "OK  "
        if delta_pct < -args.threshold_pct:
            verdict = "RED "
            red = True
        print(f"  {verdict} {name} = {new_v:.1f} (prev {old_v:.1f}, {delta_pct:+.1f}%)")

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(current, f, indent=2, sort_keys=True)
        print(f"perf-baseline: wrote {args.out}")

    if red:
        print("perf-baseline: REGRESSION beyond threshold — marking red")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
