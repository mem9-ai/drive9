from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import statistics
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


SCHEMA = "drive9-blackbox/v2"
PACKAGE_DIR = Path(__file__).resolve().parent
BLACKBOX_DIR = PACKAGE_DIR.parent
SUITES_DIR = BLACKBOX_DIR / "suites"
REPO_ROOT = BLACKBOX_DIR.parent
RESULT_ROOT = BLACKBOX_DIR / "results"
CACHE_ROOT = BLACKBOX_DIR / "cache"


PASS = "PASS"
FAIL = "FAIL"
SKIP = "SKIP"
XFAIL = "XFAIL"
WARN = "WARN"

# Classification constants for structured SKIP/FAIL reasons.
CLASS_PLATFORM_LINUX = "platform:linux"
CLASS_PLATFORM_DARWIN = "platform:darwin"
CLASS_DEPENDENCY = "dependency"
CLASS_EXPLICIT_OPT_IN = "explicit opt-in"
CLASS_CONFIGURATION_SKIP = "configuration skip"
CLASS_TIMEOUT = "timeout"


class BlackboxError(RuntimeError):
    pass


class ModuleSkip(RuntimeError):
    def __init__(self, detail: str, classification: str = "dependency skip") -> None:
        super().__init__(detail)
        self.classification = classification


class ModuleXFail(RuntimeError):
    def __init__(self, detail: str, classification: str = "known incompatibility") -> None:
        super().__init__(detail)
        self.classification = classification


class DependencyUnavailable(ModuleSkip):
    pass


def utc_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def file_ts() -> str:
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime())


def load_json(name: str, default: Any | None = None, root: Path | None = None) -> Any:
    path = (root or SUITES_DIR) / name
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def env_value(suffix: str, default: str = "", suite: str | None = None) -> str:
    generic = f"BLACKBOX_{suffix}"
    if generic in os.environ:
        return os.environ[generic]
    suite_name = suite or os.environ.get("BLACKBOX_SUITE", "")
    if suite_name:
        suite_specific = f"BLACKBOX_{suite_name.upper()}_{suffix}"
        if suite_specific in os.environ:
            return os.environ[suite_specific]
    return default


def env_flag(suffix: str, default: bool = False, suite: str | None = None) -> bool:
    value = env_value(suffix, "", suite)
    if value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def resolve_module_timeout(module_id: str, default_timeout: int, suite: str, module_config: dict[str, Any] | None = None) -> int:
    """Resolve the wall-clock timeout for a module.

    Priority (highest wins):
    1. BLACKBOX_<SUITE>_<MODULE_UPPER>_TIMEOUT_S environment variable
    2. BLACKBOX_<MODULE_UPPER>_TIMEOUT_S environment variable
    3. The module class's ``timeout`` attribute (``default_timeout``)
    """
    module_env_suffix = module_id.replace(".", "_").replace("-", "_").upper() + "_TIMEOUT_S"
    suite_specific = f"BLACKBOX_{suite.upper()}_{module_env_suffix}"
    if suite_specific in os.environ:
        try:
            return max(1, int(os.environ[suite_specific]))
        except ValueError:
            pass
    generic = f"BLACKBOX_{module_env_suffix}"
    if generic in os.environ:
        try:
            return max(1, int(os.environ[generic]))
        except ValueError:
            pass
    if module_config and isinstance(module_config.get("timeout"), (int, float)):
        return max(1, int(module_config["timeout"]))
    return max(1, int(default_timeout))


def progress(message: str) -> None:
    if env_flag("QUIET", False):
        return
    print(f"blackbox: {message}", flush=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_bytes(size: int, seed: int = 42) -> bytes:
    rng = random.Random(seed)
    data = bytearray(size)
    for idx in range(size):
        data[idx] = rng.randrange(0, 256)
    return bytes(data)


def ensure_empty(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def summarize(values: list[float], unit: str) -> dict[str, Any]:
    return {
        "unit": unit,
        "values": values,
        "mean": statistics.mean(values) if values else 0.0,
        "median": statistics.median(values) if values else 0.0,
        "min": min(values) if values else 0.0,
        "max": max(values) if values else 0.0,
        "stdev": statistics.stdev(values) if len(values) >= 2 else 0.0,
    }


def percentile(ordered: list[float], pct: int) -> float:
    """Linear-interpolation percentile over a pre-sorted list."""
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * pct / 100
    lower = int(idx)
    upper = min(lower + 1, len(ordered) - 1)
    weight = idx - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def latency_summary(values: list[float]) -> dict[str, float]:
    """Return p50/p95/p99/mean/stdev for a list of latency samples."""
    if not values:
        return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "mean": 0.0, "stdev": 0.0}
    ordered = sorted(values)
    return {
        "count": len(values),
        "p50": percentile(ordered, 50),
        "p95": percentile(ordered, 95),
        "p99": percentile(ordered, 99),
        "max": max(values),
        "mean": statistics.mean(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
    }


@dataclass
class CommandResult:
    code: int
    seconds: float
    stdout: Path
    stderr: Path
    ok: bool


@dataclass
class ModuleRecord:
    module: str
    status: str
    seconds: float
    classification: str = ""
    detail: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, str] = field(default_factory=dict)
    report_profile: str = ""


class Module(Protocol):
    id: str
    description: str
    labels: tuple[str, ...]
    timeout: int
    report_profile: str
    needs_setup: bool

    def ensure_dependencies(self, ctx: "Context") -> None:
        ...

    def run(self, ctx: "Context") -> dict[str, Any] | None:
        ...

    def render_report(self, ctx: "Context", record: "ModuleRecord") -> str | None:
        ...


@dataclass
class Context:
    args: Any
    session: str
    result_dir: Path
    tmp_dir: Path
    target: Any
    deps: Any
    recorder: "Recorder"
    capabilities: dict[str, Any]
    config: dict[str, Any]
    runs: int
    suite: str

    def artifact_dir(self, module_id: str) -> Path:
        path = self.result_dir / "artifacts" / module_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def metric(self, name: str, value: float, unit: str, labels: dict[str, str] | None = None) -> None:
        self.recorder.metric(name, value, unit, labels or {})

    def perf_values(self, name: str, values: list[float], unit: str, labels: dict[str, str] | None = None) -> None:
        self.recorder.metric_summary(name, summarize(values, unit), labels or {})


class Recorder:
    def __init__(self, result_dir: Path) -> None:
        self.result_dir = result_dir
        self.result_dir.mkdir(parents=True, exist_ok=True)
        self.records: list[ModuleRecord] = []
        self.metric_rows: list[dict[str, Any]] = []
        self.metric_summaries: dict[str, Any] = {}

    def event(self, event: dict[str, Any]) -> None:
        event.setdefault("timestamp", utc_ts())
        path = self.result_dir / "events.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")

    def record(self, record: ModuleRecord) -> None:
        self.records.append(record)
        path = self.result_dir / "results.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record.__dict__, sort_keys=True) + "\n")
        self.write_results()

    def metric(self, name: str, value: float, unit: str, labels: dict[str, str]) -> None:
        self.metric_rows.append(
            {
                "timestamp": utc_ts(),
                "name": name,
                "value": value,
                "unit": unit,
                "labels": labels,
            }
        )
        self.write_metrics()

    def metric_summary(self, name: str, summary: dict[str, Any], labels: dict[str, str]) -> None:
        self.metric_summaries[name] = {"summary": summary, "labels": labels, "timestamp": utc_ts()}
        self.write_metrics()

    def write_results(self) -> None:
        write_json(
            self.result_dir / "results.json",
            {
                "schema": "drive9-blackbox-results/v2",
                "timestamp": utc_ts(),
                "records": [record.__dict__ for record in self.records],
                "summary": self.summary(),
            },
        )

    def write_metrics(self) -> None:
        write_json(
            self.result_dir / "metrics.json",
            {
                "schema": "drive9-blackbox-metrics/v2",
                "timestamp": utc_ts(),
                "rows": self.metric_rows,
                "summaries": self.metric_summaries,
            },
        )

    def summary(self) -> dict[str, int]:
        out: dict[str, int] = {PASS: 0, FAIL: 0, SKIP: 0, XFAIL: 0, WARN: 0}
        for record in self.records:
            out[record.status] = out.get(record.status, 0) + 1
        return out

    def has_failures(self) -> bool:
        return any(record.status == FAIL for record in self.records)
