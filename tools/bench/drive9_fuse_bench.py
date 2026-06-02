#!/usr/bin/env python3
"""Drive9 FUSE benchmark harness.

This tool is intentionally dependency-free so it can run on live customer-like
hosts without preparing a Python environment. It records raw run values,
median, and p95 in a stable JSON shape that optimization PRs can attach as
before/after evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


SCHEMA_VERSION = "drive9-bench/v1"
DEFAULT_RANDOM_SEED = 42


@dataclass(frozen=True)
class Target:
    name: str
    path: Path


@dataclass(frozen=True)
class BenchConfig:
    runs: int
    cache_state: str
    environment: str
    drop_caches_command: str
    small_count: int
    stat_count: int
    dir_large_count: int
    large_mib: int
    random_reads: int
    macro_files: int
    macro_total_mib: int
    git_clone_mode: str
    keep_workdirs: bool


@dataclass(frozen=True)
class BenchmarkSample:
    metric: str
    unit: str
    value: float
    seconds: float


def parse_target(raw: str) -> Target:
    if "=" not in raw:
        raise argparse.ArgumentTypeError("target must be name=/path")
    name, path = raw.split("=", 1)
    name = name.strip()
    if not name:
        raise argparse.ArgumentTypeError("target name is required")
    target_path = Path(path).expanduser().resolve()
    if not target_path.is_dir():
        raise argparse.ArgumentTypeError(f"target path does not exist: {target_path}")
    return Target(name=name, path=target_path)


def run_command(cmd: list[str], cwd: Path | None = None) -> str:
    return subprocess.check_output(cmd, cwd=str(cwd) if cwd else None, stderr=subprocess.STDOUT, text=True).strip()


def try_command(cmd: list[str], cwd: Path | None = None) -> dict[str, str | int]:
    try:
        out = run_command(cmd, cwd)
        return {"ok": 1, "output": out}
    except (OSError, subprocess.CalledProcessError) as exc:
        return {"ok": 0, "output": str(exc)}


def git_sha() -> str:
    try:
        return run_command(["git", "rev-parse", "HEAD"])
    except Exception:
        return ""


def mount_line(path: Path) -> str:
    path_str = str(path.resolve())
    best = ""
    best_len = -1
    try:
        with open("/proc/mounts", "r", encoding="utf-8") as mounts:
            for line in mounts:
                parts = line.split()
                if len(parts) < 3:
                    continue
                mount_point = parts[1].replace("\\040", " ")
                if path_str == mount_point or path_str.startswith(mount_point.rstrip("/") + "/"):
                    if len(mount_point) > best_len:
                        best = line.strip()
                        best_len = len(mount_point)
    except OSError:
        return ""
    return best


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100.0) * (len(ordered) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def summarize_runs(values: list[float], metric: str, unit: str, seconds: list[float] | None = None) -> dict[str, object]:
    return {
        "metric": metric,
        "unit": unit,
        "sample_count": len(values),
        "runs": values,
        "median": statistics.median(values) if values else 0.0,
        "p95": percentile(values, 95),
        "p95_method": "linear_interpolation",
        "p95_reliable": len(values) >= 20,
        "seconds": seconds or [],
    }


def timed(fn: Callable[[], int | None]) -> tuple[float, int | None]:
    start = time.perf_counter()
    result = fn()
    return time.perf_counter() - start, result


def write_file(path: Path, data: bytes, fsync_file: bool) -> None:
    with open(path, "wb") as handle:
        handle.write(data)
        if fsync_file:
            handle.flush()
            os.fsync(handle.fileno())


def remove_tree(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def ensure_empty(path: Path) -> None:
    remove_tree(path)
    path.mkdir(parents=True, exist_ok=True)


def prepare_cold_cache(config: BenchConfig) -> dict[str, object]:
    if config.cache_state != "cold":
        return {"requested": False, "method": "none"}
    if not config.drop_caches_command:
        return {
            "requested": True,
            "method": "fresh-path-only",
            "ok": False,
            "detail": "no drop-caches command configured",
        }
    proc = subprocess.run(config.drop_caches_command, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if proc.returncode != 0:
        raise RuntimeError(f"drop-caches command failed: {proc.stdout.strip()}")
    return {
        "requested": True,
        "method": "command",
        "command": config.drop_caches_command,
        "ok": proc.returncode == 0,
        "detail": proc.stdout.strip(),
    }


def prepare_measurement_cache(config: BenchConfig) -> None:
    """Prepare cache immediately before the measured operation.

    Cold-cache ownership stays inside each workload because most workloads need
    fixture setup I/O before the measurement. Dropping caches in the outer run
    loop would be invalid: setup writes would repopulate caches before timing.
    """
    prepare_cold_cache(config)


def metric_ops(seconds: float, ops: int) -> float:
    return float(ops) / seconds if seconds > 0 else 0.0


def metric_mib(seconds: float, bytes_count: int) -> float:
    return (float(bytes_count) / (1024 * 1024)) / seconds if seconds > 0 else 0.0


def bench_small_write(root: Path, count: int, config: BenchConfig) -> tuple[float, int]:
    data = b"x" * 4096
    work = root / "small-write"
    ensure_empty(work)
    prepare_measurement_cache(config)

    def body() -> int:
        for index in range(count):
            write_file(work / f"f{index:05d}.bin", data, True)
        return count

    elapsed, ops = timed(body)
    return elapsed, int(ops or 0)


def bench_mkdir(root: Path, count: int, config: BenchConfig) -> tuple[float, int]:
    work = root / "mkdir"
    ensure_empty(work)
    prepare_measurement_cache(config)

    def body() -> int:
        for index in range(count):
            (work / f"d{index:05d}").mkdir()
        return count

    elapsed, ops = timed(body)
    return elapsed, int(ops or 0)


def bench_rename(root: Path, count: int, config: BenchConfig) -> tuple[float, int]:
    work = root / "rename"
    ensure_empty(work)
    for index in range(count):
        (work / f"d{index:05d}").mkdir()
    prepare_measurement_cache(config)

    def body() -> int:
        for index in range(count):
            os.rename(work / f"d{index:05d}", work / f"r{index:05d}")
        return count

    elapsed, ops = timed(body)
    return elapsed, int(ops or 0)


def bench_unlink(root: Path, count: int, config: BenchConfig) -> tuple[float, int]:
    work = root / "unlink"
    ensure_empty(work)
    data = b"x"
    for index in range(count):
        (work / f"f{index:05d}.txt").write_bytes(data)
    prepare_measurement_cache(config)

    def body() -> int:
        for index in range(count):
            (work / f"f{index:05d}.txt").unlink()
        return count

    elapsed, ops = timed(body)
    return elapsed, int(ops or 0)


def prepare_dir(root: Path, name: str, count: int) -> Path:
    work = root / name
    ensure_empty(work)
    for index in range(count):
        (work / f"entry{index:05d}.txt").write_bytes(b"x")
    return work


def bench_readdir(root: Path, name: str, count: int, config: BenchConfig) -> tuple[float, int]:
    work = prepare_dir(root, name, count)
    if config.cache_state == "hot":
        os.listdir(work)
    else:
        prepare_measurement_cache(config)

    def body() -> int:
        return len(os.listdir(work))

    elapsed, ops = timed(body)
    return elapsed, int(ops or 0)


def bench_stat(root: Path, count: int, config: BenchConfig) -> tuple[float, int]:
    work = prepare_dir(root, "stat", count)
    paths = [work / f"entry{index:05d}.txt" for index in range(count)]
    if config.cache_state == "hot":
        for path in paths:
            path.stat()
    else:
        prepare_measurement_cache(config)

    def body() -> int:
        for path in paths:
            path.stat()
        return count

    elapsed, ops = timed(body)
    return elapsed, int(ops or 0)


def prepare_large_file(root: Path, mib: int) -> Path:
    work = root / "large"
    ensure_empty(work)
    path = work / "large.bin"
    block = b"a" * (1024 * 1024)
    with open(path, "wb") as handle:
        for _ in range(mib):
            handle.write(block)
        handle.flush()
        os.fsync(handle.fileno())
    return path


def bench_sequential_write(root: Path, mib: int, config: BenchConfig) -> tuple[float, int]:
    work = root / "seq-write"
    ensure_empty(work)
    path = work / "large.bin"
    block = b"w" * (1024 * 1024)
    prepare_measurement_cache(config)

    def body() -> int:
        with open(path, "wb") as handle:
            for _ in range(mib):
                handle.write(block)
            handle.flush()
            os.fsync(handle.fileno())
        return mib * 1024 * 1024

    elapsed, bytes_count = timed(body)
    return elapsed, int(bytes_count or 0)


def bench_sequential_read(root: Path, mib: int, config: BenchConfig) -> tuple[float, int]:
    path = prepare_large_file(root, mib)
    if config.cache_state == "hot":
        read_file(path)
    else:
        prepare_measurement_cache(config)

    def body() -> int:
        return read_file(path)

    elapsed, bytes_count = timed(body)
    return elapsed, int(bytes_count or 0)


def read_file(path: Path) -> int:
    total = 0
    with open(path, "rb") as handle:
        while True:
            data = handle.read(1024 * 1024)
            if not data:
                break
            total += len(data)
    return total


def bench_random_read(root: Path, mib: int, reads: int, config: BenchConfig) -> tuple[float, int]:
    path = prepare_large_file(root, mib)
    size = path.stat().st_size
    rng = random.Random(DEFAULT_RANDOM_SEED)
    offsets = [rng.randrange(0, max(1, size - 4096)) for _ in range(reads)]
    if config.cache_state == "hot":
        read_file(path)
    else:
        prepare_measurement_cache(config)

    def body() -> int:
        total = 0
        with open(path, "rb") as handle:
            for offset in offsets:
                handle.seek(offset)
                total += len(handle.read(4096))
        return total

    elapsed, bytes_count = timed(body)
    return elapsed, int(bytes_count or 0)


def create_macro_repo(source: Path, file_count: int, total_mib: int) -> None:
    ensure_empty(source)
    run_command(["git", "init", "-b", "main"], source)
    run_command(["git", "config", "user.email", "drive9-bench@example.invalid"], source)
    run_command(["git", "config", "user.name", "Drive9 Bench"], source)
    (source / "go.mod").write_text("module drive9bench\n\ngo 1.22\n", encoding="utf-8")
    (source / "main.go").write_text("package main\n\nfunc main() {}\n", encoding="utf-8")
    payload_size = max(1, (total_mib * 1024 * 1024) // max(1, file_count))
    payload = b"m" * payload_size
    data_dir = source / "data"
    data_dir.mkdir()
    for index in range(file_count):
        (data_dir / f"file{index:05d}.txt").write_bytes(payload)
    run_command(["git", "add", "."], source)
    run_command(["git", "commit", "-m", "bench fixture"], source)


def latency_sample(seconds: float) -> BenchmarkSample:
    return BenchmarkSample(metric="latency", unit="seconds", value=seconds, seconds=seconds)


def git_clone_args(source: Path, clone: Path, mode: str) -> list[str]:
    args = ["git", "clone", "--quiet"]
    if mode == "no-local":
        args.append("--no-local")
    elif mode != "local":
        raise ValueError(f"unsupported git clone mode: {mode}")
    args.extend([str(source), str(clone)])
    return args


def bench_macro(root: Path, config: BenchConfig, hot: bool) -> dict[str, BenchmarkSample]:
    macro_root = root / "macro"
    ensure_empty(macro_root)
    clone = macro_root / "clone"
    results: dict[str, BenchmarkSample] = {}
    with tempfile.TemporaryDirectory(prefix="drive9-bench-source-") as source_tmp:
        source = Path(source_tmp) / "source"
        create_macro_repo(source, config.macro_files, config.macro_total_mib)

        prepare_measurement_cache(config)
        elapsed, _ = timed(lambda: run_command(git_clone_args(source, clone, config.git_clone_mode)))
        results["macro_git_clone"] = latency_sample(elapsed)

        dirty_path = clone / "data" / "file00000.txt"
        with open(dirty_path, "ab") as handle:
            handle.write(b"dirty\n")
        if hot:
            try_command(["git", "status", "--porcelain"], clone)
        else:
            prepare_measurement_cache(config)
        elapsed, _ = timed(lambda: run_command(["git", "status", "--porcelain"], clone))
        results["macro_git_status_dirty"] = latency_sample(elapsed)

        if hot:
            try_command(["git", "diff", "--name-only"], clone)
        else:
            prepare_measurement_cache(config)
        elapsed, _ = timed(lambda: run_command(["git", "diff", "--name-only"], clone))
        results["macro_git_diff_dirty"] = latency_sample(elapsed)

        if hot:
            try_command(["sh", "-c", "find . -type f | wc -l"], clone)
        else:
            prepare_measurement_cache(config)
        elapsed, _ = timed(lambda: run_command(["sh", "-c", "find . -type f | wc -l"], clone))
        results["macro_find_files"] = latency_sample(elapsed)

        if shutil.which("go"):
            if hot:
                try_command(["go", "build", "./..."], clone)
            else:
                prepare_measurement_cache(config)
            elapsed, _ = timed(lambda: run_command(["go", "build", "./..."], clone))
            results["macro_go_build"] = latency_sample(elapsed)
        return results


def run_target(target: Target, config: BenchConfig) -> dict[str, object]:
    run_id = f"drive9-bench-{int(time.time())}-{os.getpid()}-{target.name}"
    root = target.path / run_id
    ensure_empty(root)
    hot = config.cache_state == "hot"
    target_report: dict[str, object] = {
        "path": str(target.path),
        "mount": mount_line(target.path),
        "workdir": str(root),
        "results": {},
    }
    try:
        tests: list[tuple[str, str, str, Callable[[Path], tuple[float, int]]]] = [
            ("sequential_write", "MiB/s", "throughput", lambda base: bench_sequential_write(base, config.large_mib, config)),
            ("sequential_read", "MiB/s", "throughput", lambda base: bench_sequential_read(base, config.large_mib, config)),
            ("random_read_4k", "MiB/s", "throughput", lambda base: bench_random_read(base, config.large_mib, config.random_reads, config)),
            ("small_write_4k_fsync", "ops/s", "throughput", lambda base: bench_small_write(base, config.small_count, config)),
            ("mkdir", "ops/s", "throughput", lambda base: bench_mkdir(base, config.dir_large_count, config)),
            ("rename", "ops/s", "throughput", lambda base: bench_rename(base, config.dir_large_count, config)),
            ("unlink", "ops/s", "throughput", lambda base: bench_unlink(base, config.dir_large_count, config)),
            ("readdir_small", "ops/s", "throughput", lambda base: bench_readdir(base, "readdir-small", 20, config)),
            ("readdir_large", "ops/s", "throughput", lambda base: bench_readdir(base, "readdir-large", config.dir_large_count, config)),
            ("stat", "ops/s", "throughput", lambda base: bench_stat(base, config.stat_count, config)),
        ]
        for test_name, unit, metric, fn in tests:
            values: list[float] = []
            seconds: list[float] = []
            for run_index in range(config.runs):
                run_root = root / f"{test_name}-run{run_index:02d}"
                ensure_empty(run_root)
                elapsed, amount = fn(run_root)
                seconds.append(elapsed)
                if unit == "MiB/s":
                    values.append(metric_mib(elapsed, amount))
                else:
                    values.append(metric_ops(elapsed, amount))
            target_report["results"][test_name] = summarize_runs(values, metric, unit, seconds)

        macro_values: dict[str, list[float]] = {}
        macro_seconds: dict[str, list[float]] = {}
        macro_shapes: dict[str, tuple[str, str]] = {}
        for run_index in range(config.runs):
            run_root = root / f"macro-run{run_index:02d}"
            ensure_empty(run_root)
            for name, sample in bench_macro(run_root, config, hot).items():
                macro_values.setdefault(name, []).append(sample.value)
                macro_seconds.setdefault(name, []).append(sample.seconds)
                macro_shapes[name] = (sample.metric, sample.unit)
        for name, values in macro_values.items():
            metric, unit = macro_shapes[name]
            target_report["results"][name] = summarize_runs(values, metric, unit, macro_seconds.get(name, []))
        return target_report
    finally:
        if not config.keep_workdirs:
            remove_tree(root)


def flatten_results(target_reports: dict[str, dict[str, object]]) -> dict[str, object]:
    flat: dict[str, object] = {}
    for target_name, target_report in target_reports.items():
        results = target_report.get("results", {})
        if not isinstance(results, dict):
            continue
        for test_name, result in results.items():
            flat[f"{target_name}:{test_name}"] = result
    return flat


def render_summary(report: dict[str, object]) -> str:
    lines = [
        "# Drive9 benchmark summary",
        "",
        f"- Schema: `{report['schema_version']}`",
        f"- Version: `{report['version']}`",
        f"- Host: `{report['host']}`",
        f"- Environment: `{report['environment']}`",
        f"- Cache state: `{report['cache_state']}`",
        "",
        "| Target | Workload | Median | P95 | Unit |",
        "|---|---|---:|---:|---|",
    ]
    targets = report.get("targets", {})
    if isinstance(targets, dict):
        for target_name, target_report in targets.items():
            if not isinstance(target_report, dict):
                continue
            results = target_report.get("results", {})
            if not isinstance(results, dict):
                continue
            for workload, result in results.items():
                if not isinstance(result, dict):
                    continue
                lines.append(
                    f"| {target_name} | {workload} | {float(result['median']):.3f} | {float(result['p95']):.3f} | {result['unit']} |"
                )
    return "\n".join(lines) + "\n"


def build_report(args: argparse.Namespace, targets: list[Target], config: BenchConfig) -> dict[str, object]:
    target_reports: dict[str, dict[str, object]] = {}
    for target in targets:
        target_reports[target.name] = run_target(target, config)
    mount_params = {
        "targets": {name: {"path": report["path"], "mount": report["mount"]} for name, report in target_reports.items()},
        "server_url": args.server_url,
        "client_host": args.client_host,
        "server_host": args.server_host,
        "network_note": args.network_note,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "version": args.version or git_sha(),
        "host": socket.gethostname(),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mount_params": mount_params,
        "environment": config.environment,
        "cache_state": config.cache_state,
        "cache_prepare": {
            "requested": config.cache_state == "cold",
            "method": "command" if config.drop_caches_command else ("fresh-path-only" if config.cache_state == "cold" else "none"),
            "drop_caches_command": config.drop_caches_command,
        },
        "params": {
            "runs": config.runs,
            "small_count": config.small_count,
            "stat_count": config.stat_count,
            "dir_large_count": config.dir_large_count,
            "large_mib": config.large_mib,
            "random_reads": config.random_reads,
            "macro_files": config.macro_files,
            "macro_total_mib": config.macro_total_mib,
            "git_clone_mode": config.git_clone_mode,
            "drop_caches_command": config.drop_caches_command,
        },
        "targets": target_reports,
        "results": flatten_results(target_reports),
    }


def positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Run Drive9 FUSE performance benchmarks.")
    parser.add_argument("--target", action="append", type=parse_target, required=True, help="Benchmark target as name=/path. Repeatable.")
    parser.add_argument("--out", required=True, help="JSON report output path.")
    parser.add_argument("--summary-out", help="Markdown summary output path.")
    parser.add_argument("--runs", type=positive_int, default=5, help="Runs per workload. Use 20+ for meaningful p95 comparisons.")
    parser.add_argument("--cache-state", choices=["cold", "hot"], default="hot")
    parser.add_argument("--drop-caches-command", default="", help="Optional shell command before each cold run.")
    parser.add_argument("--environment", choices=["local", "lan", "wan"], default="local")
    parser.add_argument("--server-url", default="", help="Drive9 server URL for report metadata.")
    parser.add_argument("--client-host", default=socket.gethostname(), help="Client host label for network reports.")
    parser.add_argument("--server-host", default="", help="Server host label for network reports.")
    parser.add_argument("--network-note", default="", help="Free-form network topology note.")
    parser.add_argument("--version", default="", help="Drive9 git SHA/version override.")
    parser.add_argument("--small-count", type=positive_int, default=1000)
    parser.add_argument("--stat-count", type=positive_int, default=1000)
    parser.add_argument("--dir-large-count", type=positive_int, default=1000)
    parser.add_argument("--large-mib", type=positive_int, default=512)
    parser.add_argument("--random-reads", type=positive_int, default=4096)
    parser.add_argument("--macro-files", type=positive_int, default=1000)
    parser.add_argument("--macro-total-mib", type=positive_int, default=50)
    parser.add_argument("--git-clone-mode", choices=["no-local", "local"], default="no-local", help="Use no-local by default to avoid local clone hardlink/cache shortcuts.")
    parser.add_argument("--keep-workdirs", action="store_true")
    args = parser.parse_args(argv)

    config = BenchConfig(
        runs=args.runs,
        cache_state=args.cache_state,
        environment=args.environment,
        drop_caches_command=args.drop_caches_command,
        small_count=args.small_count,
        stat_count=args.stat_count,
        dir_large_count=args.dir_large_count,
        large_mib=args.large_mib,
        random_reads=args.random_reads,
        macro_files=args.macro_files,
        macro_total_mib=args.macro_total_mib,
        git_clone_mode=args.git_clone_mode,
        keep_workdirs=args.keep_workdirs,
    )
    report = build_report(args, args.target, config)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.summary_out:
        summary_path = Path(args.summary_out)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(render_summary(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
