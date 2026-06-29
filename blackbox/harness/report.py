"""Blackbox report template engine.

Provides built-in markdown templates for module-level, suite-level, and
overall (cross-suite) reports. Templates are plain string composition — no
third-party dependencies.

Each module can override ``render_report`` to produce a fully custom report.
When it returns ``None``, the framework selects a built-in template based on
the module's ``report_profile`` (functional / performance / compatibility /
customer).
"""
from __future__ import annotations

import json
import platform
from pathlib import Path
from typing import Any

from .core import (
    FAIL,
    PASS,
    SKIP,
    WARN,
    XFAIL,
    Context,
    ModuleRecord,
    latency_summary,
    percentile,
    utc_ts,
)
from .core import env_value

# Statuses that count as "not a failure" for suite/overall pass-rate.
OK_STATUSES = {PASS, SKIP, XFAIL}

# Default threshold for perf regression detection (fractional change).
REGRESSION_THRESHOLD = 0.10


def load_baseline(suite: str, baseline_path: str | None = None) -> dict[str, Any]:
    """Load a baseline JSON for perf comparison.

    Path priority: explicit ``baseline_path`` arg → ``BLACKBOX_BASELINE`` env →
    ``blackbox/baselines/<suite>.json``.
    """
    path_str = baseline_path or env_value("BASELINE", "", suite)
    if not path_str:
        from .core import BLACKBOX_DIR

        default = BLACKBOX_DIR / "baselines" / f"{suite}.json"
        if default.exists():
            path_str = str(default)
    if not path_str:
        return {}
    try:
        return json.loads(Path(path_str).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def classify_metric(current: float, baseline: float, threshold: float = REGRESSION_THRESHOLD) -> str:
    """Classify a metric vs baseline as REGRESSION / IMPROVED / OK."""
    if baseline <= 0:
        return "OK"
    ratio = (current - baseline) / baseline
    if ratio > threshold:
        return "REGRESSION"
    if ratio < -threshold:
        return "IMPROVED"
    return "OK"


def md_cell(value: Any, *, limit: int = 1000) -> str:
    """Escape a value for use in a markdown table cell."""
    text = str(value).replace("|", "\\|").replace("\n", " ")
    return text[:limit]


def format_number(value: Any, *, precision: int = 3) -> str:
    try:
        return f"{float(value):.{precision}f}"
    except (TypeError, ValueError):
        return ""


def status_emoji(status: str) -> str:
    return {
        PASS: "✅",
        FAIL: "❌",
        SKIP: "⏭️",
        XFAIL: "⚠️",
        WARN: "⚡",
    }.get(status, "❓")


# ---------------------------------------------------------------------------
# Module-level report templates
# ---------------------------------------------------------------------------


def render_module_report(ctx: Context, module: Any, record: ModuleRecord) -> str:
    """Dispatch to the appropriate built-in template based on report profile."""
    profile = module.resolve_report_profile() if hasattr(module, "resolve_report_profile") else "functional"
    if profile == "performance":
        return render_performance_module_report(ctx, module, record)
    if profile == "compatibility":
        return render_compatibility_module_report(ctx, module, record)
    if profile == "customer":
        return render_customer_module_report(ctx, module, record)
    return render_functional_module_report(ctx, module, record)


def render_functional_module_report(ctx: Context, module: Any, record: ModuleRecord) -> str:
    lines = [
        f"# Module Report: `{module.id}`",
        "",
        f"- Description: {module.description}",
        f"- Status: {status_emoji(record.status)} **{record.status}**",
        f"- Duration: `{record.seconds:.3f}s`",
        f"- Classification: `{record.classification or 'n/a'}`",
        f"- Session: `{ctx.session}`",
        f"- Timestamp: `{utc_ts()}`",
        "",
        "## Detail",
        "",
        record.detail.strip() or "_No detail._",
        "",
    ]
    artifact_dir = ctx.artifact_dir(module.id)
    _append_artifacts_section(lines, artifact_dir)
    _append_module_metrics_section(lines, record)
    return "\n".join(lines) + "\n"


def render_performance_module_report(ctx: Context, module: Any, record: ModuleRecord) -> str:
    lines = [
        f"# Module Report: `{module.id}`",
        "",
        f"- Description: {module.description}",
        f"- Profile: `performance`",
        f"- Status: {status_emoji(record.status)} **{record.status}**",
        f"- Duration: `{record.seconds:.3f}s`",
        f"- Classification: `{record.classification or 'n/a'}`",
        f"- Session: `{ctx.session}`",
        f"- Timestamp: `{utc_ts()}`",
        "",
    ]
    if record.detail.strip():
        lines.extend(["## Detail", "", record.detail.strip(), ""])
    metrics_path = ctx.result_dir / "metrics.json"
    module_metrics = _load_module_metrics(metrics_path, module.id)
    if module_metrics:
        lines.extend(
            [
                "## Performance Metrics",
                "",
                "| Metric | Unit | Mean | Median | Min | Max | Stddev | Runs |",
                "|---|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for name, item in sorted(module_metrics.items()):
            summary = item.get("summary", {})
            values = summary.get("values", [])
            lines.append(
                "| "
                f"`{md_cell(name)}` | "
                f"`{md_cell(summary.get('unit', ''))}` | "
                f"{format_number(summary.get('mean', 0))} | "
                f"{format_number(summary.get('median', 0))} | "
                f"{format_number(summary.get('min', 0))} | "
                f"{format_number(summary.get('max', 0))} | "
                f"{format_number(summary.get('stdev', 0))} | "
                f"{len(values)} |"
            )
        lines.append("")
        # Per-metric raw measurements (latency percentiles if available)
        lines.extend(["## Latency Distribution", "", "| Metric | Unit | p50 | p95 | p99 | Max | Count |", "|---|---|---:|---:|---:|---:|---:|"])
        for name, item in sorted(module_metrics.items()):
            summary = item.get("summary", {})
            values = summary.get("values", [])
            ls = latency_summary(values)
            lines.append(
                "| "
                f"`{md_cell(name)}` | "
                f"`{md_cell(summary.get('unit', ''))}` | "
                f"{format_number(ls['p50'])} | "
                f"{format_number(ls['p95'])} | "
                f"{format_number(ls['p99'])} | "
                f"{format_number(ls['max'])} | "
                f"{ls['count']} |"
            )
        lines.append("")
        # Baseline comparison (if available)
        baseline = load_baseline(ctx.suite)
        if baseline:
            baseline_metrics = baseline.get("metrics", baseline)
            if isinstance(baseline_metrics, dict) and baseline_metrics:
                lines.extend(["## Baseline Comparison", "", "| Metric | Current | Baseline | Delta | Verdict |", "|---|---:|---:|---:|---|"])
                for name, item in sorted(module_metrics.items()):
                    summary = item.get("summary", {})
                    current_mean = float(summary.get("mean", 0))
                    base_entry = baseline_metrics.get(name, {})
                    base_mean = float(base_entry.get("mean", base_entry) if isinstance(base_entry, dict) else base_entry) if base_entry else 0.0
                    if base_mean > 0:
                        delta = current_mean - base_mean
                        verdict = classify_metric(current_mean, base_mean)
                        lines.append(
                            f"| `{md_cell(name)}` | {format_number(current_mean)} | {format_number(base_mean)} | {format_number(delta)} | {verdict} |"
                        )
                lines.append("")
    else:
        lines.extend(["## Performance Metrics", "", "_No metrics recorded._", ""])
    artifact_dir = ctx.artifact_dir(module.id)
    _append_artifacts_section(lines, artifact_dir)
    return "\n".join(lines) + "\n"


def render_compatibility_module_report(ctx: Context, module: Any, record: ModuleRecord) -> str:
    lines = [
        f"# Module Report: `{module.id}`",
        "",
        f"- Description: {module.description}",
        f"- Profile: `compatibility`",
        f"- Status: {status_emoji(record.status)} **{record.status}**",
        f"- Duration: `{record.seconds:.3f}s`",
        f"- Classification: `{record.classification or 'n/a'}`",
        f"- Session: `{ctx.session}`",
        f"- Timestamp: `{utc_ts()}`",
        "",
    ]
    # Try to extract pass/fail counts from the module's returned metrics
    metrics = record.metrics or {}
    if metrics:
        lines.extend(["## Test Results", ""])
        total = metrics.get("total_cases")
        passed = metrics.get("passed_cases")
        failed = metrics.get("failed_cases")
        xfail = metrics.get("xfail_known_cases")
        regression = metrics.get("fail_regression_cases")
        raw_rate = metrics.get("raw_pass_rate")
        eff_rate = metrics.get("effective_pass_rate")
        if total is not None:
            lines.append(f"- Total test cases: **{total}**")
        if passed is not None:
            lines.append(f"- Passed: **{passed}**")
        if failed is not None:
            lines.append(f"- Failed: **{failed}**")
        if xfail is not None:
            lines.append(f"- Known XFAIL: **{xfail}**")
        if regression is not None:
            lines.append(f"- Regressions: **{regression}**")
        if raw_rate is not None:
            lines.append(f"- Raw pass rate: **{format_number(float(raw_rate) * 100, precision=2)}%**")
        if eff_rate is not None:
            lines.append(f"- Effective pass rate: **{format_number(float(eff_rate) * 100, precision=2)}%**")
        # Failed files detail
        failed_files = metrics.get("failed_files", [])
        if failed_files:
            lines.extend(["", "## Failed Files", "", "| Path | Tests | Failed | Classification |", "|---|---:|---:|---|"])
            for item in failed_files:
                lines.append(
                    f"| `{md_cell(item.get('path', ''))}` | {item.get('tests', 0)} | {item.get('failed', 0)} | {item.get('classification', '')} |"
                )
        lines.append("")
    if record.detail.strip():
        lines.extend(["## Detail", "", record.detail.strip(), ""])
    artifact_dir = ctx.artifact_dir(module.id)
    _append_artifacts_section(lines, artifact_dir)
    return "\n".join(lines) + "\n"


def render_customer_module_report(ctx: Context, module: Any, record: ModuleRecord) -> str:
    """Default customer report — delegates to the module's own render if available."""
    # If the module provided a custom render_report, it was already called and
    # this function won't be invoked. This is the fallback for customer-profiled
    # modules that don't override render_report.
    return render_functional_module_report(ctx, module, record)


# ---------------------------------------------------------------------------
# Suite-level report
# ---------------------------------------------------------------------------


def render_suite_report(
    ctx: Context,
    suite: str,
    records: list[ModuleRecord],
    suite_goals: str = "",
) -> str:
    summary = _status_counts(records)
    lines = [
        f"# Drive9 {suite.upper()} Blackbox Suite Report",
        "",
        f"- Suite: `{suite}`",
        f"- Session: `{ctx.session}`",
        f"- Timestamp: `{utc_ts()}`",
        f"- Platform: `{platform.platform()}`",
        f"- Result dir: `{ctx.result_dir}`",
        f"- Modules: `{len(records)}`",
        "",
        "## Summary",
        "",
        "| Status | Count |",
        "|---|---:|",
    ]
    for status in (PASS, FAIL, SKIP, XFAIL, WARN):
        lines.append(f"| {status} | {summary.get(status, 0)} |")
    overall = "PASS" if summary.get(FAIL, 0) == 0 else "FAIL"
    lines.extend(["", f"**Overall: {overall}**", ""])
    if suite_goals.strip():
        lines.extend(["## Suite Goals", "", suite_goals.strip(), ""])
    # Group records by report profile
    lines.extend(["## Module Results", "", "| Module | Profile | Status | Seconds | Classification | Report |", "|---|---|---|---:|---|---|"])
    for record in records:
        profile = _infer_profile_from_record(record)
        report_path = ctx.result_dir / "artifacts" / record.module / "report.md"
        report_link = f"[report](artifacts/{record.module}/report.md)" if report_path.exists() else "—"
        lines.append(
            f"| `{record.module}` | `{profile}` | {status_emoji(record.status)} {record.status} | {record.seconds:.3f} | {md_cell(record.classification, limit=80)} | {report_link} |"
        )
    lines.append("")
    # Profile-grouped summary
    lines.extend(["## Profile Summary", ""])
    _append_profile_summary(lines, records)
    # Metrics highlights
    _append_suite_metrics_section(lines, ctx, records)
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Overall (cross-suite) report
# ---------------------------------------------------------------------------


def render_overall_report(
    work_dir: Path,
    suite_summaries: list[dict[str, Any]],
) -> str:
    lines = [
        "# Drive9 Blackbox Overall Report",
        "",
        f"- Timestamp: `{utc_ts()}`",
        f"- Work dir: `{work_dir}`",
        f"- Suites: `{len(suite_summaries)}`",
        "",
        "## Suite Results",
        "",
        "| Suite | PASS | FAIL | SKIP | XFAIL | WARN | Overall | Report |",
        "|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for item in sorted(suite_summaries, key=lambda s: s["suite"]):
        s = item["summary"]
        overall = "PASS" if s.get(FAIL, 0) == 0 else "FAIL"
        lines.append(
            f"| `{item['suite']}` | {s.get(PASS, 0)} | {s.get(FAIL, 0)} | {s.get(SKIP, 0)} | {s.get(XFAIL, 0)} | {s.get(WARN, 0)} | {overall} | [report]({item['report']}) |"
        )
    lines.append("")
    total_pass = sum(s["summary"].get(PASS, 0) for s in suite_summaries)
    total_fail = sum(s["summary"].get(FAIL, 0) for s in suite_summaries)
    overall = "PASS" if total_fail == 0 else "FAIL"
    lines.extend([f"**Overall: {overall}** ({total_pass} passed, {total_fail} failed)", ""])
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _status_counts(records: list[ModuleRecord]) -> dict[str, int]:
    out: dict[str, int] = {PASS: 0, FAIL: 0, SKIP: 0, XFAIL: 0, WARN: 0}
    for record in records:
        out[record.status] = out.get(record.status, 0) + 1
    return out


def _infer_profile_from_record(record: ModuleRecord) -> str:
    return record.report_profile or "functional"


def _append_profile_summary(lines: list[str], records: list[ModuleRecord]) -> None:
    profiles: dict[str, list[ModuleRecord]] = {}
    for record in records:
        profile = _infer_profile_from_record(record)
        profiles.setdefault(profile, []).append(record)
    if not profiles:
        lines.append("_No modules._")
        lines.append("")
        return
    for profile in sorted(profiles):
        recs = profiles[profile]
        counts = _status_counts(recs)
        lines.append(
            f"- **{profile}**: {len(recs)} modules — PASS={counts.get(PASS, 0)} FAIL={counts.get(FAIL, 0)} SKIP={counts.get(SKIP, 0)} XFAIL={counts.get(XFAIL, 0)}"
        )
    lines.append("")


def _load_module_metrics(metrics_path: Path, module_id: str) -> dict[str, Any]:
    if not metrics_path.exists():
        return {}
    try:
        data = json.loads(metrics_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    summaries = data.get("summaries", {})
    out: dict[str, Any] = {}
    for name, item in summaries.items():
        if name == module_id or name.startswith(module_id + "."):
            out[name] = item
    return out


def _append_module_metrics_section(lines: list[str], record: ModuleRecord) -> None:
    metrics = record.metrics or {}
    if not metrics:
        return
    lines.extend(["## Returned Metrics", "", "```json", json.dumps(metrics, indent=2, sort_keys=True), "```", ""])


def _append_artifacts_section(lines: list[str], artifact_dir: Path) -> None:
    if not artifact_dir.is_dir():
        return
    files = sorted(p.name for p in artifact_dir.iterdir() if p.is_file())
    if not files:
        return
    lines.extend(["## Artifacts", ""])
    for name in files:
        lines.append(f"- `{name}`")
    lines.append("")


def _append_suite_metrics_section(lines: list[str], ctx: Context, records: list[ModuleRecord]) -> None:
    metrics_path = ctx.result_dir / "metrics.json"
    if not metrics_path.exists():
        return
    try:
        data = json.loads(metrics_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return
    summaries = data.get("summaries", {})
    if not summaries:
        return
    lines.extend(["## Metrics Highlights", "", "| Metric | Unit | Mean | Median | Runs |", "|---|---|---:|---:|---:|"])
    for name in sorted(summaries):
        summary = summaries[name].get("summary", {})
        values = summary.get("values", [])
        lines.append(
            f"| `{md_cell(name)}` | `{md_cell(summary.get('unit', ''))}` | {format_number(summary.get('mean', 0))} | {format_number(summary.get('median', 0))} | {len(values)} |"
        )
    lines.append("")