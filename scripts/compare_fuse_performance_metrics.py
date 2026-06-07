#!/usr/bin/env python3
"""Compare FUSE performance metrics against an archived baseline."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any


SCHEMA = "drive9-fuse-performance-compare/v1"
METRICS_SCHEMA = "drive9-fuse-performance/v1"
HIGHER_IS_BETTER = ("mib_per_second", "files_per_second", "rows_per_second")
REQUIRED_PARAMS = ("small_files", "small_bytes", "large_mb", "large_bytes", "read_passes", "sqlite_rows")
REQUIRED_WORKLOAD_METRICS = {
    "small_file_write": ("mib_per_second", "files_per_second"),
    "small_file_read": ("mib_per_second", "files_per_second"),
    "large_file_write": ("mib_per_second", "files_per_second"),
    "sqlite_insert_transaction": ("rows_per_second",),
    "sqlite_update_transaction": ("rows_per_second",),
    "sqlite_read_aggregate": ("mib_per_second", "rows_per_second"),
    "sqlite_wal_insert_transaction": ("rows_per_second",),
    "sqlite_wal_update_transaction": ("rows_per_second",),
    "sqlite_wal_read_aggregate": ("mib_per_second", "rows_per_second"),
    "sqlite_wal_checkpoint_truncate": ("rows_per_second",),
}


class CompareError(ValueError):
    """Raised when metrics or archive metadata are invalid."""


def load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            doc = json.load(handle)
    except json.JSONDecodeError as exc:
        raise CompareError(f"{path} is not valid JSON: {exc}") from exc
    if not isinstance(doc, dict):
        raise CompareError(f"{path} must contain a JSON object")
    return doc


def validate_metrics(doc: dict[str, Any], label: str) -> None:
    validate_metrics_shape(doc, label)
    workloads = doc["workloads"]
    params = doc["params"]

    expected_rows = params["sqlite_rows"]
    validate_sqlite_read_workload(workloads, "sqlite_read_aggregate", expected_rows, label)
    validate_sqlite_read_workload(workloads, "sqlite_wal_read_aggregate", expected_rows, label)
    sqlite_wal_checkpoint = require_workload(workloads, "sqlite_wal_checkpoint_truncate", label)
    if sqlite_wal_checkpoint.get("integrity_check") != "ok":
        raise CompareError(f"{label} sqlite_wal_checkpoint_truncate.integrity_check must be ok")
    if sqlite_wal_checkpoint.get("checkpoint_busy") != 0:
        raise CompareError(f"{label} sqlite_wal_checkpoint_truncate.checkpoint_busy must be 0")
    for workload, required_metrics in REQUIRED_WORKLOAD_METRICS.items():
        entry = require_workload(workloads, workload, label)
        for metric in required_metrics:
            require_positive_metric(entry, metric, f"{label} workloads.{workload}")

    validate_large_file_reads(workloads, params["read_passes"], label)


def validate_baseline_metrics(doc: dict[str, Any], label: str) -> None:
    validate_metrics_shape(doc, label)
    validate_large_file_reads(doc["workloads"], doc["params"]["read_passes"], label)


def validate_metrics_shape(doc: dict[str, Any], label: str) -> None:
    if doc.get("schema") != METRICS_SCHEMA:
        raise CompareError(f"{label} metrics schema must be {METRICS_SCHEMA}")
    workloads = doc.get("workloads")
    if not isinstance(workloads, dict):
        raise CompareError(f"{label} metrics must include workloads object")
    params = doc.get("params")
    if not isinstance(params, dict):
        raise CompareError(f"{label} metrics must include params object")

    for param in REQUIRED_PARAMS:
        value = params.get(param)
        if not isinstance(value, int) or value <= 0:
            raise CompareError(f"{label} params.{param} must be a positive integer")


def validate_large_file_reads(workloads: dict[str, Any], read_passes: int, label: str) -> None:
    large_reads = workloads.get("large_file_reads")
    if not isinstance(large_reads, list):
        raise CompareError(f"{label} metrics must include large_file_reads list")
    if len(large_reads) != read_passes:
        raise CompareError(f"{label} large_file_reads length={len(large_reads)} does not match read_passes={read_passes}")
    for index, entry in enumerate(large_reads, start=1):
        if not isinstance(entry, dict):
            raise CompareError(f"{label} large_file_reads[{index}] must be an object")
        require_positive_metric(entry, "mib_per_second", f"{label} large_file_reads[{index}]")
        require_positive_metric(entry, "files_per_second", f"{label} large_file_reads[{index}]")


def require_workload(workloads: dict[str, Any], name: str, label: str) -> dict[str, Any]:
    entry = workloads.get(name)
    if not isinstance(entry, dict):
        raise CompareError(f"{label} metrics must include {name} object")
    return entry


def validate_sqlite_read_workload(workloads: dict[str, Any], name: str, expected_rows: int, label: str) -> None:
    entry = require_workload(workloads, name, label)
    verified_rows = entry.get("payload_verified_rows")
    if entry.get("integrity_check") != "ok":
        raise CompareError(f"{label} {name}.integrity_check must be ok")
    if verified_rows != expected_rows:
        raise CompareError(
            f"{label} {name}.payload_verified_rows={verified_rows!r} does not match sqlite_rows={expected_rows!r}"
        )


def require_positive_metric(entry: dict[str, Any], metric: str, label: str) -> None:
    value = finite_number(entry.get(metric))
    if value is None or value <= 0:
        raise CompareError(f"{label}.{metric} must be a positive finite number")


def param_mismatch_warnings(current: dict[str, Any], baseline: dict[str, Any]) -> list[str]:
    current_params = current["params"]
    baseline_params = baseline["params"]
    warnings = []
    for param in REQUIRED_PARAMS:
        current_value = current_params[param]
        baseline_value = baseline_params[param]
        if current_value != baseline_value:
            warnings.append(f"params mismatch {param}: current={current_value!r} baseline={baseline_value!r}")
    return warnings


def sanitize_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    return cleaned or "unknown"


def archive_path_from_manifest(manifest: dict[str, Any], archive_root: str) -> str:
    archive_path = manifest.get("archive_path")
    if isinstance(archive_path, str) and archive_path.startswith("/"):
        return archive_path.rstrip("/")

    archived_at = manifest.get("archived_at")
    if not isinstance(archived_at, str) or len(archived_at) < 10:
        raise CompareError("archive manifest is missing archive_path and archived_at")
    date = archived_at[:10]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        raise CompareError(f"archive manifest has invalid archived_at date: {archived_at!r}")

    branch = manifest.get("branch")
    commit_sha = manifest.get("commit_sha")
    run_id = manifest.get("run_id")
    run_attempt = manifest.get("run_attempt")
    if not all(isinstance(value, str) and value for value in (branch, commit_sha, run_id, run_attempt)):
        raise CompareError("archive manifest cannot derive archive path from branch/commit/run metadata")

    date_path = date.replace("-", "/")
    branch_slug = sanitize_component(branch)
    short_sha = commit_sha[:12]
    return f"{archive_root.rstrip('/')}/{date_path}/{branch_slug}/{short_sha}/{run_id}-{run_attempt}"


def metrics_remote_path_from_manifest(manifest: dict[str, Any], archive_root: str) -> str:
    files = manifest.get("files")
    if not isinstance(files, list):
        raise CompareError("archive manifest must include files list")

    metrics_paths = []
    for file_info in files:
        if not isinstance(file_info, dict):
            continue
        rel_path = file_info.get("path")
        if not isinstance(rel_path, str):
            continue
        name = Path(rel_path).name
        if name.startswith("performance-metrics-") and name.endswith(".json"):
            metrics_paths.append(rel_path)

    if not metrics_paths:
        raise CompareError("archive manifest does not reference performance-metrics-*.json")
    if len(metrics_paths) > 1:
        raise CompareError(f"archive manifest references multiple performance metrics files: {metrics_paths}")

    return f"{archive_path_from_manifest(manifest, archive_root)}/{metrics_paths[0]}"


def metric_entries(metrics: dict[str, Any]) -> dict[str, dict[str, Any]]:
    workloads = metrics.get("workloads")
    if not isinstance(workloads, dict):
        raise CompareError("metrics workloads must be an object")

    entries: dict[str, dict[str, Any]] = {}
    for workload, value in sorted(workloads.items()):
        if isinstance(value, dict):
            entries[workload] = value
            continue
        if isinstance(value, list):
            for index, item in enumerate(value, start=1):
                if not isinstance(item, dict):
                    continue
                label = item.get("pass", index)
                entries[f"{workload}[pass={label}]"] = item
    return entries


def finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    if math.isfinite(number):
        return number
    return None


def compare_metrics(
    current: dict[str, Any],
    baseline: dict[str, Any] | None,
    *,
    warning_ratio: float,
    current_ref: str,
    baseline_ref: str | None,
    missing_baseline_reason: str | None,
) -> dict[str, Any]:
    validate_metrics(current, "current")
    warnings: list[str] = []
    comparisons: list[dict[str, Any]] = []
    status = "ok"

    if baseline is None:
        status = "baseline_missing"
        if missing_baseline_reason:
            warnings.append(missing_baseline_reason)
    else:
        validate_baseline_metrics(baseline, "baseline")
        warnings.extend(param_mismatch_warnings(current, baseline))
        if warnings:
            status = "warning"
        else:
            current_entries = metric_entries(current)
            baseline_entries = metric_entries(baseline)
            for workload, current_entry in sorted(current_entries.items()):
                baseline_entry = baseline_entries.get(workload)
                if not baseline_entry:
                    warnings.append(f"baseline missing workload {workload}")
                    status = "warning"
                    continue
                for metric in HIGHER_IS_BETTER:
                    current_value = finite_number(current_entry.get(metric))
                    baseline_value = finite_number(baseline_entry.get(metric))
                    if current_value is None or baseline_value is None:
                        continue
                    if baseline_value <= 0:
                        if current_value <= 0:
                            continue
                        warnings.append(f"baseline {workload}.{metric} is not positive")
                        status = "warning"
                        continue
                    ratio = current_value / baseline_value
                    comparison_status = "ok"
                    if ratio < (1.0 - warning_ratio):
                        comparison_status = "regressed"
                        status = "warning"
                    comparisons.append(
                        {
                            "workload": workload,
                            "metric": metric,
                            "baseline": baseline_value,
                            "current": current_value,
                            "ratio": ratio,
                            "regression_percent": max(0.0, (1.0 - ratio) * 100.0),
                            "status": comparison_status,
                        }
                    )
            if not comparisons:
                raise CompareError("no comparable performance metrics found")

    return {
        "schema": SCHEMA,
        "status": status,
        "warning_only": True,
        "thresholds": {
            "regression_warning_ratio": warning_ratio,
            "regression_warning_percent": warning_ratio * 100.0,
        },
        "current": {
            "ref": current_ref,
            "generated_at_unix": current.get("generated_at_unix"),
            "params": current.get("params", {}),
        },
        "baseline": {
            "ref": baseline_ref,
            "generated_at_unix": baseline.get("generated_at_unix") if baseline else None,
            "params": baseline.get("params", {}) if baseline else {},
            "missing_reason": missing_baseline_reason if baseline is None else None,
        },
        "comparisons": comparisons,
        "warnings": warnings,
    }


def format_number(value: float) -> str:
    if value == 0:
        return "0"
    if abs(value) >= 100:
        return f"{value:.2f}"
    if abs(value) >= 1:
        return f"{value:.3f}"
    return f"{value:.6f}"


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# FUSE Performance Compare",
        "",
        f"- Status: `{report['status']}`",
        "- Mode: `warning-only`",
        f"- Current: `{report['current']['ref']}`",
        f"- Baseline: `{report['baseline'].get('ref') or 'missing'}`",
        f"- Regression warning threshold: `{report['thresholds']['regression_warning_percent']:.1f}%`",
        "",
    ]
    warnings = report.get("warnings") or []
    if warnings:
        lines.append("## Warnings")
        lines.append("")
        for warning in warnings:
            lines.append(f"- {warning}")
        lines.append("")

    comparisons = report.get("comparisons") or []
    if comparisons:
        lines.extend(
            [
                "## Comparisons",
                "",
                "| Workload | Metric | Baseline | Current | Ratio | Status |",
                "| --- | --- | ---: | ---: | ---: | --- |",
            ]
        )
        for comparison in comparisons:
            lines.append(
                "| {workload} | `{metric}` | {baseline} | {current} | {ratio} | `{status}` |".format(
                    workload=comparison["workload"],
                    metric=comparison["metric"],
                    baseline=format_number(comparison["baseline"]),
                    current=format_number(comparison["current"]),
                    ratio=format_number(comparison["ratio"]),
                    status=comparison["status"],
                )
            )
        lines.append("")

    return "\n".join(lines)


def run_compare(args: argparse.Namespace) -> int:
    current = load_json(Path(args.current))
    baseline = load_json(Path(args.baseline)) if args.baseline else None
    report = compare_metrics(
        current,
        baseline,
        warning_ratio=args.warning_ratio,
        current_ref=args.current_ref,
        baseline_ref=args.baseline_ref,
        missing_baseline_reason=args.missing_baseline_reason,
    )

    output_json = Path(args.output_json)
    output_markdown = Path(args.output_markdown)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_markdown.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_markdown.write_text(render_markdown(report), encoding="utf-8")
    return 0


def run_manifest_metrics_path(args: argparse.Namespace) -> int:
    manifest = load_json(Path(args.manifest))
    print(metrics_remote_path_from_manifest(manifest, args.archive_root))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    compare = subparsers.add_parser("compare", help="compare current metrics with an optional baseline")
    compare.add_argument("--current", required=True)
    compare.add_argument("--baseline")
    compare.add_argument("--output-json", required=True)
    compare.add_argument("--output-markdown", required=True)
    compare.add_argument("--warning-ratio", type=float, default=0.30)
    compare.add_argument("--current-ref", required=True)
    compare.add_argument("--baseline-ref")
    compare.add_argument("--missing-baseline-reason")
    compare.set_defaults(func=run_compare)

    manifest = subparsers.add_parser("manifest-metrics-path", help="print archived metrics path from manifest")
    manifest.add_argument("--manifest", required=True)
    manifest.add_argument("--archive-root", required=True)
    manifest.set_defaults(func=run_manifest_metrics_path)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if hasattr(args, "warning_ratio") and not (0.0 < args.warning_ratio < 1.0):
        parser.error("--warning-ratio must be between 0 and 1")
    try:
        return args.func(args)
    except CompareError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
