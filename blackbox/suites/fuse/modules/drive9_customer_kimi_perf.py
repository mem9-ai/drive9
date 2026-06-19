from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import shlex
import shutil
import socket
import statistics
import subprocess
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

from harness.core import BlackboxError, Context, ModuleSkip, env_flag, env_value, progress, stable_bytes, write_json

from .base import BaseModule, module_config


class Drive9KimiPerf(BaseModule):
    id = "drive9.customer.kimi_perf"
    category = "drive9.customer.performance"
    description = "Kimi sandbox workspace benchmark: namespace scale, small files, fsync, visibility, remount persistence, and same-host mounts."
    labels = ("drive9", "customer", "kimi", "performance", "fuse")
    timeout = 3600

    def ensure_dependencies(self, ctx: Context) -> None:
        if not env_flag("KIMI_PERF_ENABLE", False, ctx.suite):
            raise ModuleSkip("set BLACKBOX_KIMI_PERF_ENABLE=1 to run Kimi performance tests", "explicit opt-in")
        for tool in ("bash", "find"):
            ctx.deps.require_tool(tool)

    def run(self, ctx: Context) -> dict[str, Any]:
        cfg = self.config(ctx)
        artifact = ctx.artifact_dir(self.id)
        raw_dir = artifact / "raw_results"
        summary_dir = artifact / "summary"
        for path in (raw_dir, summary_dir):
            path.mkdir(parents=True, exist_ok=True)

        remote_base = env_value("KIMI_PERF_REMOTE_ROOT", ctx.target.remote_root(self.id), ctx.suite).rstrip("/")
        ctx.target.mkdir_remote(remote_base)
        self.capture_environment(ctx, artifact)
        manifest_config = {key: value for key, value in cfg.items() if not key.startswith("_")}
        write_json(artifact / "manifest.json", {"remote_base": remote_base, "config": manifest_config, "session": ctx.session})

        rows: list[dict[str, Any]] = []
        issues: list[dict[str, Any]] = []

        def checkpoint(extra_rows: list[dict[str, Any]] | None = None) -> None:
            self.write_summary_outputs(ctx, artifact, summary_dir, rows + (extra_rows or []), issues)

        sections: list[tuple[str, bool, Callable[[], list[dict[str, Any]]]]] = [
            ("small_file", bool(cfg["sections"]["small_file"]), lambda: self.run_small_file_suite(ctx, cfg, remote_base, raw_dir, issues)),
            ("flush", bool(cfg["sections"]["flush"]), lambda: self.run_flush_suite(ctx, cfg, remote_base, raw_dir, issues)),
            ("persistence", bool(cfg["sections"]["persistence"]), lambda: self.run_persistence_suite(ctx, cfg, remote_base, raw_dir, issues)),
            ("multi_mount", bool(cfg["sections"]["multi_mount"]), lambda: self.run_same_host_mount_suite(ctx, cfg, remote_base, issues)),
            ("namespace", bool(cfg["sections"]["namespace"]), lambda: self.run_namespace_suite(ctx, cfg, remote_base, raw_dir, issues, checkpoint)),
            ("soak", bool(cfg["sections"]["soak"]), lambda: self.run_soak_suite(ctx, cfg, remote_base, raw_dir, issues)),
        ]
        for name, enabled, fn in sections:
            if not enabled:
                rows.append(self.control_row(name, "skipped", "section disabled by config", 0.0))
                checkpoint()
                continue
            progress(f"kimi perf section start: {name}")
            started = time.perf_counter()
            try:
                produced = fn()
                rows.extend(produced)
                rows.append(self.control_row(name, "completed", f"rows={len(produced)}", time.perf_counter() - started))
                progress(f"kimi perf section done: {name} rows={len(produced)}")
            except Exception as exc:
                elapsed = time.perf_counter() - started
                detail = f"{type(exc).__name__}: {exc}"
                issues.append({"severity": "error", "section": name, "op": "section", "detail": detail})
                rows.append(self.control_row(name, "error", detail, elapsed))
                progress(f"kimi perf section error: {name}: {detail}")
            finally:
                checkpoint()

        checkpoint()
        return {"remote_base": remote_base, "summary_rows": len(rows), "issues": len(issues), "artifact": str(artifact)}

    def config(self, ctx: Context) -> dict[str, Any]:
        cfg = module_config(ctx, self.id)
        scales = cfg.get(
            "scales",
            {
                "S": {"bytes": 100 * 1024 * 1024, "files": 1000, "mount_repeats": 3},
                "M": {"bytes": 1024 * 1024 * 1024, "files": 10000, "mount_repeats": 3},
                "L": {"bytes": 10 * 1024 * 1024 * 1024, "files": 100000, "mount_repeats": 2},
            },
        )
        sections_default = cfg.get("sections", {})
        return {
            "scales": scales,
            "selected_scales": self.csv_env(ctx, "KIMI_PERF_SCALES", cfg.get("selected_scales", ["S"])),
            "layouts": self.csv_env(ctx, "KIMI_PERF_LAYOUTS", cfg.get("layouts", ["single", "tree"])),
            # BLACKBOX_RUNS should control the module by default. The module config is only used when
            # BLACKBOX_KIMI_PERF_RUNS is explicitly set.
            "runs": max(1, int(env_value("KIMI_PERF_RUNS", str(ctx.runs), ctx.suite))),
            "profile": env_value("KIMI_PERF_PROFILE", str(cfg.get("profile", "coding-agent")), ctx.suite),
            "durability": env_value("KIMI_PERF_DURABILITY", str(cfg.get("durability", "auto")), ctx.suite),
            "namespace_stat_samples": int(env_value("KIMI_PERF_STAT_SAMPLES", str(cfg.get("namespace_stat_samples", 300)), ctx.suite)),
            "namespace_cmd_timeout_s": int(env_value("KIMI_PERF_NAMESPACE_CMD_TIMEOUT_S", str(cfg.get("namespace_cmd_timeout_s", 300)), ctx.suite)),
            "namespace_cmd_timeouts_s": self.timeout_map_env(ctx, "KIMI_PERF_NAMESPACE_CMD_TIMEOUTS", cfg.get("namespace_cmd_timeouts_s", {"S": 180, "M": 30, "L": 30})),
            "dataset_timeout_s": float(env_value("KIMI_PERF_DATASET_TIMEOUT_S", str(cfg.get("dataset_timeout_s", 300)), ctx.suite)),
            "dataset_timeouts_s": self.timeout_map_env(ctx, "KIMI_PERF_DATASET_TIMEOUTS", cfg.get("dataset_timeouts_s", {"S": 300, "M": 600, "L": 120})),
            "small_file_sizes": self.int_csv_env(ctx, "KIMI_PERF_SMALL_SIZES", cfg.get("small_file_sizes", [1024, 20 * 1024, 100 * 1024])),
            "small_file_concurrency": self.int_csv_env(ctx, "KIMI_PERF_SMALL_CONCURRENCY", cfg.get("small_file_concurrency", [1, 4, 16])),
            "small_file_ops": int(env_value("KIMI_PERF_SMALL_OPS", str(cfg.get("small_file_ops", 50)), ctx.suite)),
            "flush_file_sizes": self.int_csv_env(ctx, "KIMI_PERF_FLUSH_SIZES", cfg.get("flush_file_sizes", [1024, 20 * 1024, 100 * 1024])),
            "flush_concurrency": self.int_csv_env(ctx, "KIMI_PERF_FLUSH_CONCURRENCY", cfg.get("flush_concurrency", [1, 4, 16])),
            "flush_ops": int(env_value("KIMI_PERF_FLUSH_OPS", str(cfg.get("flush_ops", 30)), ctx.suite)),
            "flush_visibility_samples": int(env_value("KIMI_PERF_FLUSH_VISIBILITY_SAMPLES", str(cfg.get("flush_visibility_samples", cfg.get("visibility_samples", 20))), ctx.suite)),
            "visibility_timeout_s": float(env_value("KIMI_PERF_VISIBILITY_TIMEOUT_S", str(cfg.get("visibility_timeout_s", 30)), ctx.suite)),
            "persistence_samples": int(env_value("KIMI_PERF_PERSISTENCE_SAMPLES", str(cfg.get("persistence_samples", 20)), ctx.suite)),
            "same_host_mount_counts": self.int_csv_env(ctx, "KIMI_PERF_MOUNT_COUNTS", cfg.get("same_host_mount_counts", [1, 2, 5, 10])),
            "soak_minutes": float(env_value("KIMI_PERF_SOAK_MINUTES", str(cfg.get("soak_minutes", 0)), ctx.suite)),
            "raw_results": env_flag("KIMI_PERF_RAW", bool(cfg.get("raw_results", True)), ctx.suite),
            "reuse_datasets": env_flag("KIMI_PERF_REUSE_DATASETS", bool(cfg.get("reuse_datasets", True)), ctx.suite),
            "sections": {
                "namespace": env_flag("KIMI_PERF_NAMESPACE", bool(sections_default.get("namespace", True)), ctx.suite),
                "small_file": env_flag("KIMI_PERF_SMALL_FILE", bool(sections_default.get("small_file", True)), ctx.suite),
                "flush": env_flag("KIMI_PERF_FLUSH", bool(sections_default.get("flush", True)), ctx.suite),
                "persistence": env_flag("KIMI_PERF_PERSISTENCE", bool(sections_default.get("persistence", True)), ctx.suite),
                "multi_mount": env_flag("KIMI_PERF_MULTI_MOUNT", bool(sections_default.get("multi_mount", True)), ctx.suite),
                "soak": env_flag("KIMI_PERF_SOAK", bool(sections_default.get("soak", False)), ctx.suite),
            },
        }

    def run_namespace_suite(
        self,
        ctx: Context,
        cfg: dict[str, Any],
        remote_base: str,
        raw_dir: Path,
        issues: list[dict[str, Any]],
        checkpoint: Callable[[list[dict[str, Any]] | None], None] | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for scale_id in cfg["selected_scales"]:
            if scale_id not in cfg["scales"]:
                raise ModuleSkip(f"unknown BLACKBOX_KIMI_PERF_SCALES value: {scale_id}", "configuration skip")
            scale = cfg["scales"][scale_id]
            for layout in cfg["layouts"]:
                if layout not in {"single", "tree"}:
                    raise ModuleSkip(f"unknown Kimi perf layout: {layout}", "configuration skip")
                remote = f"{remote_base}/datasets/{scale_id}-{layout}"
                ctx.target.mkdir_remote(remote)
                dataset = self.prepare_dataset(ctx, cfg, remote, scale_id, layout, scale, issues)
                rows.append(dataset)
                if checkpoint is not None:
                    checkpoint(rows)
                if dataset.get("status") not in {"ok", "cached"}:
                    issues.append({"severity": "warn", "section": "namespace", "op": "dataset_generate", "scale": scale_id, "layout": layout, "detail": str(dataset.get("detail", ""))})
                    continue
                rows.extend(self.measure_mounts(ctx, cfg, remote, scale_id, layout, scale, issues))
                if checkpoint is not None:
                    checkpoint(rows)
                rows.extend(self.measure_namespace(ctx, cfg, remote, scale_id, layout, scale, raw_dir, issues))
                if checkpoint is not None:
                    checkpoint(rows)
        return rows

    def prepare_dataset(
        self,
        ctx: Context,
        cfg: dict[str, Any],
        remote: str,
        scale_id: str,
        layout: str,
        scale: dict[str, Any],
        issues: list[dict[str, Any]],
    ) -> dict[str, Any]:
        mount_started = time.perf_counter()
        handle = ctx.target.mount(
            "kimi_dataset_prepare",
            remote,
            profile=cfg["profile"],
            durability=cfg["durability"],
            cache_key=f"{scale_id}-{layout}-prepare",
        )
        mount_ms = (time.perf_counter() - mount_started) * 1000
        unmount_ms = 0.0
        row: dict[str, Any] = {
            "section": "namespace",
            "op": "dataset_generate",
            "scale": scale_id,
            "layout": layout,
            "file_size": "",
            "concurrency": "",
            "unit": "seconds",
            "status": "error",
            "errors": 0,
            "error_rate": 0.0,
            "runs": 1,
            "target_bytes": int(scale["bytes"]),
            "target_files": int(scale["files"]),
            "created_files": 0,
            "created_bytes": 0,
            "mount_ms": mount_ms,
        }
        try:
            manifest = handle.mountpoint / ".drive9-kimi-dataset.json"
            expected = {"scale": scale_id, "layout": layout, "bytes": int(scale["bytes"]), "files": int(scale["files"])}
            if cfg["reuse_datasets"] and manifest.exists():
                try:
                    current = json.loads(manifest.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    current = {}
                if all(current.get(key) == value for key, value in expected.items()) and current.get("completed") is True:
                    seconds = float(current.get("seconds", 0.0))
                    row.update(
                        {
                            "status": "cached",
                            "created_files": int(scale["files"]),
                            "created_bytes": int(scale["bytes"]),
                            "count": int(scale["files"]),
                            **self.latency_summary([seconds]),
                        }
                    )
                    return row
            progress(f"kimi dataset generate: {scale_id}-{layout} bytes={scale['bytes']} files={scale['files']}")
            data_dir = handle.mountpoint / "data"
            if data_dir.exists():
                shutil.rmtree(data_dir)
            data_dir.mkdir()
            started = time.perf_counter()
            try:
                timeout_s = float(cfg["dataset_timeouts_s"].get(scale_id, cfg["dataset_timeout_s"]))
                if timeout_s <= 0:
                    detail = f"dataset generation skipped because timeout is {timeout_s:.0f}s"
                    self.write_dataset_manifest(manifest, expected, 0, 0, 0.0, False, detail)
                    result = {"completed": False, "created_files": 0, "created_bytes": 0, "detail": detail}
                else:
                    result = self.generate_dataset(
                        data_dir,
                        layout,
                        int(scale["bytes"]),
                        int(scale["files"]),
                        timeout_s,
                        manifest,
                        expected,
                    )
            except OSError as exc:
                result = {
                    "completed": False,
                    "created_files": 0,
                    "created_bytes": 0,
                    "detail": f"I/O error during dataset generation: {exc}",
                    "error": True,
                }
            seconds = time.perf_counter() - started
            status = "ok" if result["completed"] else ("error" if result.get("error") else "timeout")
            row.update(
                {
                    "status": status,
                    "created_files": result["created_files"],
                    "created_bytes": result["created_bytes"],
                    "count": result["created_files"],
                    "detail": result["detail"],
                    **self.latency_summary([seconds]),
                }
            )
            ctx.metric(f"{self.id}.dataset.generate_seconds", seconds, "seconds", {"scale": scale_id, "layout": layout})
            if not result["completed"]:
                issues.append({"severity": "warn", "section": "namespace", "op": "dataset_generate", "scale": scale_id, "layout": layout, "detail": result["detail"]})
            return row
        finally:
            unmount_ms, _ = self.record_unmount(ctx, handle, issues, section="namespace", op="dataset_generate", row=row, labels={"scale": scale_id, "layout": layout})
            row["unmount_ms"] = unmount_ms

    def generate_dataset(
        self,
        data_dir: Path,
        layout: str,
        total_bytes: int,
        file_count: int,
        timeout_s: float,
        manifest: Path,
        manifest_base: dict[str, Any],
    ) -> dict[str, Any]:
        base = total_bytes // file_count
        extra = total_bytes % file_count
        payload = stable_bytes(min(max(base + 1, 1), 1024 * 1024), seed=file_count)
        checkpoints = {max(1, file_count * pct // 10) for pct in range(1, 11)}
        started = time.perf_counter()
        created_bytes = 0
        created_files = 0
        if timeout_s <= 0:
            detail = f"dataset generation skipped because timeout is {timeout_s:.0f}s"
            self.write_dataset_manifest(manifest, manifest_base, created_files, created_bytes, 0.0, False, detail)
            return {"completed": False, "created_files": created_files, "created_bytes": created_bytes, "detail": detail}
        for idx in range(file_count):
            now = time.perf_counter()
            if timeout_s > 0 and now - started > timeout_s:
                detail = f"dataset generation exceeded {timeout_s:.0f}s at {created_files}/{file_count} files"
                self.write_dataset_manifest(manifest, manifest_base, created_files, created_bytes, now - started, False, detail)
                return {"completed": False, "created_files": created_files, "created_bytes": created_bytes, "detail": detail}
            size = base + (1 if idx < extra else 0)
            parent = data_dir if layout == "single" else data_dir / f"shard-{idx // 1000:05d}"
            parent.mkdir(parents=True, exist_ok=True)
            try:
                self.write_payload(parent / f"file-{idx:08d}.bin", payload, size)
            except OSError as exc:
                detail = f"I/O error after {created_files}/{file_count} files: {exc}"
                self.write_dataset_manifest(manifest, manifest_base, created_files, created_bytes, time.perf_counter() - started, False, detail)
                return {"completed": False, "created_files": created_files, "created_bytes": created_bytes, "detail": detail, "error": True}
            created_files += 1
            created_bytes += size
            if created_files in checkpoints:
                progress(f"kimi dataset progress: {created_files}/{file_count}")
        detail = "completed"
        self.write_dataset_manifest(manifest, manifest_base, created_files, created_bytes, time.perf_counter() - started, True, detail)
        return {"completed": True, "created_files": created_files, "created_bytes": created_bytes, "detail": detail}

    @staticmethod
    def write_dataset_manifest(manifest: Path, base: dict[str, Any], files: int, bytes_written: int, seconds: float, completed: bool, detail: str) -> bool:
        value = {
            **base,
            "created_files": files,
            "created_bytes": bytes_written,
            "seconds": seconds,
            "completed": completed,
            "detail": detail,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        try:
            manifest.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            return True
        except OSError:
            return False

    @staticmethod
    def write_payload(path: Path, payload: bytes, size: int) -> None:
        remaining = size
        with path.open("wb") as handle:
            while remaining > 0:
                chunk = payload[: min(len(payload), remaining)]
                handle.write(chunk)
                remaining -= len(chunk)

    def record_unmount(
        self,
        ctx: Context,
        handle: Any,
        issues: list[dict[str, Any]],
        *,
        section: str,
        op: str,
        row: dict[str, Any] | None = None,
        labels: dict[str, Any] | None = None,
    ) -> tuple[float, bool]:
        started = time.perf_counter()
        outcome = ctx.target.unmount(handle)
        elapsed_ms = (time.perf_counter() - started) * 1000
        exit_code = outcome.exit_code
        failed = (exit_code not in (None, 0)) or bool(outcome.mounted_after)
        if row is not None:
            row["unmount_ms"] = elapsed_ms
            row["unmount_exit_code"] = "" if exit_code is None else exit_code
            row["unmount_mounted_after"] = bool(outcome.mounted_after)
            row["unmount_forced"] = bool(outcome.forced)
        if failed:
            detail = f"exit={exit_code} mounted_after={outcome.mounted_after} forced={outcome.forced} seconds={outcome.seconds:.3f} mountpoint={handle.mountpoint}"
            issue = {"severity": "error" if outcome.mounted_after else "warn", "section": section, "op": f"{op}_unmount", "detail": detail}
            if labels:
                issue.update(labels)
            issues.append(issue)
        return elapsed_ms, failed

    def measure_mounts(self, ctx: Context, cfg: dict[str, Any], remote: str, scale_id: str, layout: str, scale: dict[str, Any], issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        mount_values: list[float] = []
        unmount_values: list[float] = []
        mount_errors = 0
        unmount_errors = 0
        repeats = int(scale.get("mount_repeats", 3))
        for idx in range(repeats):
            handle = None
            start = time.perf_counter()
            try:
                handle = ctx.target.mount(
                    "kimi_mount_latency",
                    remote,
                    profile=cfg["profile"],
                    durability=cfg["durability"],
                    cache_key=f"{scale_id}-{layout}-mount-{idx}",
                )
                mount_values.append((time.perf_counter() - start) * 1000)
            except Exception as exc:
                mount_errors += 1
                issues.append({"severity": "error", "section": "namespace", "op": "mount", "scale": scale_id, "layout": layout, "detail": str(exc)})
            finally:
                if handle is not None:
                    elapsed_ms, failed = self.record_unmount(ctx, handle, issues, section="namespace", op="mount_latency", labels={"scale": scale_id, "layout": layout})
                    unmount_values.append(elapsed_ms)
                    if failed:
                        unmount_errors += 1
        rows.append(self.matrix_row("namespace", "mount", scale_id, layout, "", "", "ms", mount_values, mount_errors, repeats, repeats, status="ok" if mount_errors == 0 else "error"))
        rows.append(self.matrix_row("namespace", "unmount", scale_id, layout, "", "", "ms", unmount_values, unmount_errors, repeats, repeats, status="ok" if unmount_errors == 0 else "error"))
        ctx.perf_values(f"{self.id}.namespace.{scale_id}.{layout}.mount", mount_values, "ms")
        ctx.perf_values(f"{self.id}.namespace.{scale_id}.{layout}.unmount", unmount_values, "ms")
        return rows

    def measure_namespace(self, ctx: Context, cfg: dict[str, Any], remote: str, scale_id: str, layout: str, scale: dict[str, Any], raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        handle = ctx.target.mount(
            "kimi_namespace",
            remote,
            profile=cfg["profile"],
            durability=cfg["durability"],
            cache_key=f"{scale_id}-{layout}-namespace",
        )
        try:
            data_dir = handle.mountpoint / "data"
            commands = {
                "ls": f"ls {shlex.quote(str(data_dir))} >/dev/null",
                "ls_l": f"ls -l {shlex.quote(str(data_dir))} >/dev/null",
                "find": f"find {shlex.quote(str(data_dir))} -type f >/dev/null",
                "find_pattern": f"find {shlex.quote(str(data_dir))} -name '*.bin' >/dev/null",
            }
            for name, command in commands.items():
                timeout_s = float(cfg["namespace_cmd_timeouts_s"].get(scale_id, cfg["namespace_cmd_timeout_s"]))
                values: list[float] = []
                errors = 0
                timeouts = 0
                for run_idx in range(int(cfg["runs"])):
                    result = ctx.target.run_cmd(
                        f"kimi-namespace-{scale_id}-{layout}-{name}-run-{run_idx}",
                        command,
                        timeout=max(1, int(timeout_s)),
                        shell=True,
                    )
                    if result.ok:
                        values.append(result.seconds)
                    else:
                        errors += 1
                        if result.code == 124:
                            timeouts += 1
                        issues.append({"severity": "warn", "section": "namespace", "op": name, "scale": scale_id, "layout": layout, "detail": f"run {run_idx} exit={result.code}; see {result.stderr}"})
                status = "timeout" if timeouts else ("ok" if errors == 0 else "error")
                rows.append(self.matrix_row("namespace", name, scale_id, layout, "", "", "seconds", values, errors, int(cfg["runs"]), int(cfg["runs"]), status=status, extra={"timeouts": timeouts}))
                ctx.perf_values(f"{self.id}.namespace.{scale_id}.{layout}.{name}", values, "seconds")
            stat_values, stat_errors = self.measure_stat_samples(
                data_dir,
                layout,
                int(scale["files"]),
                int(cfg["namespace_stat_samples"]),
                raw_dir / f"stat-{scale_id}-{layout}.jsonl",
            )
            rows.append(self.matrix_row("namespace", "stat", scale_id, layout, "", "", "ms", stat_values, stat_errors, min(int(cfg["namespace_stat_samples"]), int(scale["files"])), 1, status="ok" if stat_errors == 0 else "error"))
            ctx.perf_values(f"{self.id}.namespace.{scale_id}.{layout}.stat", stat_values, "ms")
        finally:
            self.record_unmount(ctx, handle, issues, section="namespace", op="namespace_scan", labels={"scale": scale_id, "layout": layout})
        return rows

    def measure_stat_samples(self, data_dir: Path, layout: str, file_count: int, samples: int, raw_path: Path) -> tuple[list[float], int]:
        if file_count <= 0:
            return [], 0
        rng = random.Random(9)
        values: list[float] = []
        errors = 0
        with raw_path.open("w", encoding="utf-8") as raw:
            for _ in range(min(samples, file_count)):
                idx = rng.randrange(0, file_count)
                path = self.dataset_path(data_dir, layout, idx)
                start = time.perf_counter()
                status = "ok"
                err = ""
                try:
                    os.stat(path)
                    values.append((time.perf_counter() - start) * 1000)
                except OSError as exc:
                    errors += 1
                    status = "error"
                    err = str(exc)
                latency = (time.perf_counter() - start) * 1000
                raw.write(json.dumps({"op": "stat", "idx": idx, "path": str(path), "latency_ms": latency, "status": status, "error": err}, sort_keys=True) + "\n")
        return values, errors

    @staticmethod
    def dataset_path(data_dir: Path, layout: str, idx: int) -> Path:
        if layout == "single":
            return data_dir / f"file-{idx:08d}.bin"
        return data_dir / f"shard-{idx // 1000:05d}" / f"file-{idx:08d}.bin"

    def run_small_file_suite(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/small-file"
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("kimi_small_file", remote, profile=cfg["profile"], durability=cfg["durability"], cache_key="small-file-writer")
        try:
            root = handle.mountpoint / "small-file"
            root.mkdir(exist_ok=True)
            for size in cfg["small_file_sizes"]:
                for concurrency in cfg["small_file_concurrency"]:
                    rows.extend(self.small_file_matrix(ctx, cfg, root, raw_dir, int(size), int(concurrency), issues))
        finally:
            self.record_unmount(ctx, handle, issues, section="small_file", op="suite")
        return rows

    def small_file_matrix(self, ctx: Context, cfg: dict[str, Any], root: Path, raw_dir: Path, size: int, concurrency: int, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        ops = max(1, int(cfg["small_file_ops"]))
        runs = int(cfg["runs"])
        payload = stable_bytes(size, seed=size)
        append_payload = stable_bytes(min(1024, size), seed=size + 1)
        edit_payload = stable_bytes(min(128, size), seed=size + 2)
        for op in ("create", "overwrite", "append", "partial_edit", "read", "stat_after_write"):
            progress(f"kimi small_file: op={op} size={size} concurrency={concurrency}")
            values: list[float] = []
            errors = 0
            wall_seconds = 0.0
            for run_idx in range(runs):
                op_root = root / f"{op}-{size}-{concurrency}-run-{run_idx}"
                if op_root.exists():
                    shutil.rmtree(op_root)
                op_root.mkdir(parents=True)
                if op in {"overwrite", "append", "partial_edit", "read"}:
                    for idx in range(ops):
                        (op_root / f"f-{idx:08d}.bin").write_bytes(payload)
                raw_path = raw_dir / f"small-{op}-{size}-{concurrency}-run-{run_idx}.jsonl"
                run_values, run_errors, run_wall_seconds = self.run_latency_workload(
                    raw_path,
                    ops,
                    concurrency,
                    lambda idx, op=op, op_root=op_root: self.small_file_op(op, op_root, idx, payload, append_payload, edit_payload),
                    cfg["raw_results"],
                    {"section": "small_file", "op": op, "size": size, "concurrency": concurrency, "run": run_idx},
                )
                values.extend(run_values)
                errors += run_errors
                wall_seconds += run_wall_seconds
            total_ops = ops * runs
            row = self.matrix_row("small_file", op, "", "", size, concurrency, "ms", values, errors, total_ops, runs, qps=(len(values) / wall_seconds if wall_seconds > 0 else 0.0), status="ok" if errors == 0 else "error")
            rows.append(row)
            ctx.perf_values(f"{self.id}.small_file.{op}.{size}b.c{concurrency}", values, "ms")
            if errors:
                issues.append({"severity": "error", "section": "small_file", "op": op, "file_size": size, "concurrency": concurrency, "detail": f"errors={errors}"})
        return rows

    def small_file_op(self, op: str, root: Path, idx: int, payload: bytes, append_payload: bytes, edit_payload: bytes) -> None:
        path = root / f"f-{idx:08d}.bin"
        if op == "create":
            path.write_bytes(payload)
        elif op == "overwrite":
            path.write_bytes(payload)
        elif op == "append":
            with path.open("ab") as handle:
                handle.write(append_payload)
        elif op == "partial_edit":
            fd = os.open(path, os.O_RDWR)
            try:
                offset = idx % max(1, len(payload) - len(edit_payload) + 1)
                if hasattr(os, "pwrite"):
                    self.pwrite_fd(fd, edit_payload, offset)
                else:
                    os.lseek(fd, offset, os.SEEK_SET)
                    self.write_fd(fd, edit_payload)
            finally:
                os.close(fd)
        elif op == "read":
            path.read_bytes()
        elif op == "stat_after_write":
            path.write_bytes(payload)
            os.stat(path)
        else:
            raise ValueError(op)

    def run_flush_suite(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/flush"
        ctx.target.mkdir_remote(remote)
        writer = ctx.target.mount("kimi_flush", remote, profile=cfg["profile"], durability=cfg["durability"], cache_key="flush-writer")
        reader = ctx.target.mount("kimi_flush", remote, profile=cfg["profile"], durability=cfg["durability"], cache_key="flush-reader")
        try:
            root = writer.mountpoint / "flush"
            root.mkdir(exist_ok=True)
            reader_root = reader.mountpoint / "flush"
            for size in cfg["flush_file_sizes"]:
                for concurrency in cfg["flush_concurrency"]:
                    for mode in ("close", "fsync", "fdatasync"):
                        rows.extend(self.flush_matrix(ctx, cfg, root, reader_root, raw_dir, int(size), int(concurrency), mode, issues))
        finally:
            self.record_unmount(ctx, reader, issues, section="flush", op="reader")
            self.record_unmount(ctx, writer, issues, section="flush", op="writer")
        return rows

    def flush_matrix(
        self,
        ctx: Context,
        cfg: dict[str, Any],
        root: Path,
        reader_root: Path,
        raw_dir: Path,
        size: int,
        concurrency: int,
        mode: str,
        issues: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        progress(f"kimi flush: mode={mode} size={size} concurrency={concurrency}")
        ops = max(1, int(cfg["flush_ops"]))
        runs = int(cfg["runs"])
        payload = stable_bytes(size, seed=size + concurrency)
        visibility_limit = min(ops, int(cfg["flush_visibility_samples"]))
        durable_values: list[float] = []
        write_values: list[float] = []
        sync_values: list[float] = []
        close_values: list[float] = []
        visible_values: list[float] = []
        errors = 0
        visibility_errors = 0
        wall_seconds = 0.0
        for run_idx in range(runs):
            op_root = root / f"{mode}-{size}-{concurrency}-run-{run_idx}"
            reader_op_root = reader_root / f"{mode}-{size}-{concurrency}-run-{run_idx}"
            if op_root.exists():
                shutil.rmtree(op_root)
            op_root.mkdir(parents=True)
            raw_path = raw_dir / f"flush-{mode}-{size}-{concurrency}-run-{run_idx}.jsonl"

            def body(idx: int, op_root: Path = op_root, reader_op_root: Path = reader_op_root) -> dict[str, float]:
                path = op_root / f"f-{idx:08d}.bin"
                digest = hashlib.sha256(payload).hexdigest()
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                close_ms = 0.0
                sync_ms = 0.0
                try:
                    start_write = time.perf_counter()
                    self.write_fd(fd, payload)
                    write_ms = (time.perf_counter() - start_write) * 1000
                    if mode == "fsync":
                        start_sync = time.perf_counter()
                        os.fsync(fd)
                        sync_ms = (time.perf_counter() - start_sync) * 1000
                    elif mode == "fdatasync":
                        start_sync = time.perf_counter()
                        if hasattr(os, "fdatasync"):
                            os.fdatasync(fd)
                        else:
                            os.fsync(fd)
                        sync_ms = (time.perf_counter() - start_sync) * 1000
                    start_close = time.perf_counter()
                    os.close(fd)
                    fd = -1
                    close_ms = (time.perf_counter() - start_close) * 1000
                finally:
                    if fd >= 0:
                        os.close(fd)
                visible_ms = 0.0
                if idx < visibility_limit:
                    visible_ms = self.wait_visible(reader_op_root / path.name, digest, float(cfg["visibility_timeout_s"]))
                durable_ms = write_ms + sync_ms + close_ms
                return {"durable_ms": durable_ms, "write_ms": write_ms, "sync_ms": sync_ms, "close_ms": close_ms, "visible_ms": visible_ms}

            result = self.run_flush_workload(raw_path, ops, concurrency, body, cfg["raw_results"], {"section": "flush", "mode": mode, "size": size, "concurrency": concurrency, "run": run_idx})
            run_durable, run_errors, run_visibility_errors, run_write, run_sync, run_close, run_visible, run_wall_seconds = result
            durable_values.extend(run_durable)
            write_values.extend(run_write)
            sync_values.extend(run_sync)
            close_values.extend(run_close)
            visible_values.extend(run_visible)
            errors += run_errors
            visibility_errors += run_visibility_errors
            wall_seconds += run_wall_seconds
        total_ops = ops * runs
        rows: list[dict[str, Any]] = []
        status = "ok" if errors == 0 and visibility_errors == 0 else "error"
        qps = len(durable_values) / wall_seconds if wall_seconds > 0 else 0.0
        for metric, metric_values in (("durable", durable_values), ("write", write_values), ("sync", sync_values), ("close", close_values), ("visible", visible_values)):
            if not metric_values:
                continue
            metric_errors = visibility_errors if metric == "visible" else errors
            row = self.matrix_row("flush", f"{mode}_{metric}", "", "", size, concurrency, "ms", metric_values, metric_errors, total_ops, runs, qps=qps if metric == "durable" else "", status=status)
            row["visibility_errors"] = visibility_errors
            rows.append(row)
            ctx.perf_values(f"{self.id}.flush.{mode}.{metric}.{size}b.c{concurrency}", metric_values, "ms")
        if errors or visibility_errors:
            issues.append({"severity": "error", "section": "flush", "op": mode, "file_size": size, "concurrency": concurrency, "detail": f"errors={errors} visibility_errors={visibility_errors}"})
        return rows

    def wait_visible(self, path: Path, digest: str, timeout_s: float) -> float:
        deadline = time.perf_counter() + timeout_s
        start = time.perf_counter()
        while time.perf_counter() < deadline:
            try:
                data = path.read_bytes()
                if hashlib.sha256(data).hexdigest() == digest:
                    return (time.perf_counter() - start) * 1000
            except FileNotFoundError:
                pass
            time.sleep(0.02)
        return -1.0

    def run_persistence_suite(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/persistence"
        ctx.target.mkdir_remote(remote)
        samples = max(1, int(cfg["persistence_samples"]))
        for size in cfg["flush_file_sizes"]:
            for mode in ("close", "fsync"):
                rows.extend(self.persistence_matrix(ctx, cfg, remote, raw_dir, int(size), mode, samples, issues))
        return rows

    def persistence_matrix(
        self,
        ctx: Context,
        cfg: dict[str, Any],
        remote: str,
        raw_dir: Path,
        size: int,
        mode: str,
        samples: int,
        issues: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        progress(f"kimi persistence: mode={mode} size={size} samples={samples}")
        payload = stable_bytes(size, seed=size + samples)
        digest = hashlib.sha256(payload).hexdigest()
        write_values: list[float] = []
        read_values: list[float] = []
        write_unmount_values: list[float] = []
        read_unmount_values: list[float] = []
        errors = 0
        write_unmount_errors = 0
        read_unmount_errors = 0
        runs = int(cfg["runs"])
        for run_idx in range(runs):
            writer = ctx.target.mount("kimi_persistence", remote, profile=cfg["profile"], durability=cfg["durability"], cache_key=f"{mode}-{size}-writer-run-{run_idx}")
            raw_path = raw_dir / f"persistence-{mode}-{size}-run-{run_idx}.jsonl"
            raw_handle = raw_path.open("w", encoding="utf-8") if cfg["raw_results"] else None
            try:
                root = writer.mountpoint / f"{mode}-{size}-run-{run_idx}"
                if root.exists():
                    shutil.rmtree(root)
                root.mkdir(parents=True)
                for idx in range(samples):
                    path = root / f"f-{idx:08d}.bin"
                    start = time.perf_counter()
                    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                    try:
                        self.write_fd(fd, payload)
                        if mode == "fsync":
                            os.fsync(fd)
                    finally:
                        os.close(fd)
                    latency = (time.perf_counter() - start) * 1000
                    write_values.append(latency)
                    self.raw_write(raw_handle, {"section": "persistence", "phase": "write", "mode": mode, "size": size, "idx": idx, "run": run_idx, "latency_ms": latency, "status": "ok"})
            finally:
                if raw_handle is not None:
                    raw_handle.close()
                elapsed_ms, failed = self.record_unmount(ctx, writer, issues, section="persistence", op=f"{mode}_writer", labels={"file_size": size, "run": run_idx})
                write_unmount_values.append(elapsed_ms)
                if failed:
                    write_unmount_errors += 1

            reader = ctx.target.mount("kimi_persistence", remote, profile=cfg["profile"], durability=cfg["durability"], cache_key=f"{mode}-{size}-reader-run-{run_idx}")
            raw_handle = raw_path.open("a", encoding="utf-8") if cfg["raw_results"] else None
            try:
                root = reader.mountpoint / f"{mode}-{size}-run-{run_idx}"
                for idx in range(samples):
                    path = root / f"f-{idx:08d}.bin"
                    start = time.perf_counter()
                    status = "ok"
                    err = ""
                    latency = 0.0
                    try:
                        data = path.read_bytes()
                        latency = (time.perf_counter() - start) * 1000
                        if hashlib.sha256(data).hexdigest() != digest:
                            errors += 1
                            status = "error"
                            err = "checksum mismatch"
                        else:
                            read_values.append(latency)
                    except OSError as exc:
                        latency = (time.perf_counter() - start) * 1000
                        errors += 1
                        status = "error"
                        err = str(exc)
                    self.raw_write(raw_handle, {"section": "persistence", "phase": "remount_read", "mode": mode, "size": size, "idx": idx, "run": run_idx, "latency_ms": latency, "status": status, "error": err})
            finally:
                if raw_handle is not None:
                    raw_handle.close()
                elapsed_ms, failed = self.record_unmount(ctx, reader, issues, section="persistence", op=f"{mode}_reader", labels={"file_size": size, "run": run_idx})
                read_unmount_values.append(elapsed_ms)
                if failed:
                    read_unmount_errors += 1

        total = samples * runs
        rows = [
            self.matrix_row("persistence", f"{mode}_write_before_remount", "", "", size, "", "ms", write_values, 0, total, runs, status="ok"),
            self.matrix_row("persistence", f"{mode}_remount_read", "", "", size, "", "ms", read_values, errors, total, runs, status="ok" if errors == 0 else "error"),
            self.matrix_row("persistence", f"{mode}_writer_unmount", "", "", size, "", "ms", write_unmount_values, write_unmount_errors, runs, runs, status="ok" if write_unmount_errors == 0 else "error"),
            self.matrix_row("persistence", f"{mode}_reader_unmount", "", "", size, "", "ms", read_unmount_values, read_unmount_errors, runs, runs, status="ok" if read_unmount_errors == 0 else "error"),
        ]
        ctx.perf_values(f"{self.id}.persistence.{mode}.write.{size}b", write_values, "ms")
        ctx.perf_values(f"{self.id}.persistence.{mode}.remount_read.{size}b", read_values, "ms")
        if errors:
            issues.append({"severity": "error", "section": "persistence", "op": mode, "file_size": size, "detail": f"errors={errors}"})
        return rows

    def run_same_host_mount_suite(self, ctx: Context, cfg: dict[str, Any], remote_base: str, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/same-host-mount"
        ctx.target.mkdir_remote(remote)
        for count in cfg["same_host_mount_counts"]:
            progress(f"kimi same_host_multi_mount: count={count}")
            values: list[float] = []
            unmount_values: list[float] = []
            mount_errors = 0
            cross_read_errors = 0
            unmount_errors = 0
            cross_read_values: list[float] = []
            for run_idx in range(int(cfg["runs"])):
                handles = []
                try:
                    for idx in range(int(count)):
                        start = time.perf_counter()
                        try:
                            handles.append(ctx.target.mount("kimi_same_host_mount", remote, profile=cfg["profile"], durability=cfg["durability"], cache_key=f"mount-{count}-{idx}-run-{run_idx}"))
                            values.append((time.perf_counter() - start) * 1000)
                        except Exception as exc:
                            mount_errors += 1
                            issues.append({"severity": "error", "section": "same_host_multi_mount", "op": "mount", "concurrency": count, "detail": str(exc)})
                    if handles:
                        content = f"{count}-{run_idx}-{time.time()}\n"
                        probe = handles[0].mountpoint / f"shared-probe-{count}-run-{run_idx}.txt"
                        probe.write_text(content, encoding="utf-8")
                        for idx, handle in enumerate(handles):
                            start = time.perf_counter()
                            try:
                                if (handle.mountpoint / probe.name).read_text(encoding="utf-8") != content:
                                    cross_read_errors += 1
                                else:
                                    cross_read_values.append((time.perf_counter() - start) * 1000)
                            except OSError as exc:
                                cross_read_errors += 1
                                issues.append({"severity": "error", "section": "same_host_multi_mount", "op": "cross_mount_read", "concurrency": count, "detail": str(exc)})
                finally:
                    for handle in reversed(handles):
                        elapsed_ms, failed = self.record_unmount(ctx, handle, issues, section="same_host_multi_mount", op="mount", labels={"concurrency": count, "run": run_idx})
                        unmount_values.append(elapsed_ms)
                        if failed:
                            unmount_errors += 1
            rows.append(self.matrix_row("same_host_multi_mount", "mount", "", "", "", int(count), "ms", values, mount_errors, max(1, int(count) * int(cfg["runs"])), int(cfg["runs"]), status="ok" if mount_errors == 0 else "error"))
            rows.append(self.matrix_row("same_host_multi_mount", "cross_mount_read", "", "", "", int(count), "ms", cross_read_values, cross_read_errors, max(1, int(count) * int(cfg["runs"])), int(cfg["runs"]), status="ok" if cross_read_errors == 0 else "error"))
            rows.append(self.matrix_row("same_host_multi_mount", "unmount", "", "", "", int(count), "ms", unmount_values, unmount_errors, max(1, int(count) * int(cfg["runs"])), int(cfg["runs"]), status="ok" if unmount_errors == 0 else "error"))
            ctx.perf_values(f"{self.id}.same_host_multi_mount.c{count}.mount", values, "ms")
            ctx.perf_values(f"{self.id}.same_host_multi_mount.c{count}.cross_mount_read", cross_read_values, "ms")
            ctx.perf_values(f"{self.id}.same_host_multi_mount.c{count}.unmount", unmount_values, "ms")
        return rows

    def run_soak_suite(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        minutes = float(cfg["soak_minutes"])
        if minutes <= 0:
            return []
        remote = f"{remote_base}/soak"
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("kimi_soak", remote, profile=cfg["profile"], durability=cfg["durability"], cache_key="soak")
        rows: list[dict[str, Any]] = []
        raw_path = raw_dir / "soak.jsonl"
        end = time.perf_counter() + minutes * 60
        idx = 0
        errors = 0
        values: list[float] = []
        try:
            root = handle.mountpoint / "soak"
            root.mkdir(exist_ok=True)
            with raw_path.open("w", encoding="utf-8") as raw:
                while time.perf_counter() < end:
                    path = root / f"soak-{idx:08d}.txt"
                    start = time.perf_counter()
                    status = "ok"
                    err = ""
                    try:
                        path.write_text(f"{idx}-{time.time()}\n", encoding="utf-8")
                        _ = path.read_text(encoding="utf-8")
                    except OSError as exc:
                        errors += 1
                        status = "error"
                        err = str(exc)
                    latency = (time.perf_counter() - start) * 1000
                    values.append(latency)
                    raw.write(json.dumps({"section": "soak", "op": "write_read", "idx": idx, "latency_ms": latency, "status": status, "error": err}, sort_keys=True) + "\n")
                    idx += 1
                    time.sleep(1)
        finally:
            _, failed = self.record_unmount(ctx, handle, issues, section="soak", op="suite")
            if failed:
                errors += 1
        rows.append(self.matrix_row("soak", "write_read", "", "", "", "", "ms", values, errors, max(1, idx), 1, status="ok" if errors == 0 else "error"))
        ctx.perf_values(f"{self.id}.soak.write_read", values, "ms")
        if errors:
            issues.append({"severity": "error", "section": "soak", "op": "write_read", "detail": f"errors={errors}"})
        return rows

    def run_latency_workload(
        self,
        raw_path: Path,
        ops: int,
        concurrency: int,
        fn: Callable[[int], None],
        write_raw: bool,
        labels: dict[str, Any],
    ) -> tuple[list[float], int, float]:
        values: list[float] = []
        errors = 0
        raw_handle = raw_path.open("w", encoding="utf-8") if write_raw else None
        start_wall = time.perf_counter()
        try:
            with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
                futures = {pool.submit(self.timed_call, fn, idx): idx for idx in range(ops)}
                for future in as_completed(futures):
                    idx = futures[future]
                    latency, error = future.result()
                    if error:
                        errors += 1
                    else:
                        values.append(latency)
                    self.raw_write(raw_handle, {**labels, "idx": idx, "latency_ms": latency, "status": "error" if error else "ok", "error": error})
        finally:
            if raw_handle is not None:
                raw_handle.close()
        return values, errors, time.perf_counter() - start_wall

    def run_flush_workload(
        self,
        raw_path: Path,
        ops: int,
        concurrency: int,
        fn: Callable[[int], dict[str, float]],
        write_raw: bool,
        labels: dict[str, Any],
    ) -> tuple[list[float], int, int, list[float], list[float], list[float], list[float], float]:
        durable_values: list[float] = []
        write_values: list[float] = []
        sync_values: list[float] = []
        close_values: list[float] = []
        visible_values: list[float] = []
        errors = 0
        visibility_errors = 0
        raw_handle = raw_path.open("w", encoding="utf-8") if write_raw else None
        start_wall = time.perf_counter()
        try:
            with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
                futures = {pool.submit(self.timed_call_result, fn, idx): idx for idx in range(ops)}
                for future in as_completed(futures):
                    idx = futures[future]
                    elapsed, result, error = future.result()
                    status = "ok"
                    if error:
                        errors += 1
                        status = "error"
                    else:
                        durable_values.append(result.get("durable_ms", elapsed))
                        write_values.append(result.get("write_ms", 0.0))
                        sync_values.append(result.get("sync_ms", 0.0))
                        close_values.append(result.get("close_ms", 0.0))
                        visible_ms = result.get("visible_ms", 0.0)
                        if visible_ms < 0:
                            visibility_errors += 1
                            status = "visibility_timeout"
                        elif visible_ms > 0:
                            visible_values.append(visible_ms)
                    self.raw_write(raw_handle, {**labels, "idx": idx, "latency_ms": elapsed, **result, "status": status, "error": error})
        finally:
            if raw_handle is not None:
                raw_handle.close()
        return durable_values, errors, visibility_errors, write_values, sync_values, close_values, visible_values, time.perf_counter() - start_wall

    @staticmethod
    def raw_write(handle: Any, value: dict[str, Any]) -> None:
        if handle is not None:
            handle.write(json.dumps(value, sort_keys=True) + "\n")

    @staticmethod
    def timed_call(fn: Callable[[int], None], idx: int) -> tuple[float, str]:
        start = time.perf_counter()
        try:
            fn(idx)
            return (time.perf_counter() - start) * 1000, ""
        except Exception as exc:
            return (time.perf_counter() - start) * 1000, str(exc)

    @staticmethod
    def timed_call_result(fn: Callable[[int], dict[str, float]], idx: int) -> tuple[float, dict[str, float], str]:
        start = time.perf_counter()
        try:
            result = fn(idx)
            return (time.perf_counter() - start) * 1000, result, ""
        except Exception as exc:
            return (time.perf_counter() - start) * 1000, {}, str(exc)

    @staticmethod
    def write_fd(fd: int, payload: bytes) -> None:
        view = memoryview(payload)
        offset = 0
        while offset < len(view):
            written = os.write(fd, view[offset:])
            if written <= 0:
                raise OSError("short write")
            offset += written

    @staticmethod
    def pwrite_fd(fd: int, payload: bytes, offset: int) -> None:
        view = memoryview(payload)
        written_total = 0
        while written_total < len(view):
            written = os.pwrite(fd, view[written_total:], offset + written_total)
            if written <= 0:
                raise OSError("short pwrite")
            written_total += written

    def write_summary_outputs(self, ctx: Context, artifact: Path, summary_dir: Path, rows: list[dict[str, Any]], issues: list[dict[str, Any]]) -> None:
        all_keys = sorted({key for row in rows for key in row.keys()})
        csv_path = summary_dir / "summary.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=all_keys)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        write_json(summary_dir / "summary.json", {"rows": rows, "issues": issues})
        (artifact / "report.md").write_text(self.render_customer_report(ctx, rows, issues), encoding="utf-8")

    def render_customer_report(self, ctx: Context, rows: list[dict[str, Any]], issues: list[dict[str, Any]]) -> str:
        status_counts: dict[str, int] = {}
        for row in rows:
            status_counts[str(row.get("status", ""))] = status_counts.get(str(row.get("status", "")), 0) + 1
        completed = lambda section: [row for row in rows if row.get("section") == section and row.get("status") in {"ok", "cached", "completed"}]
        namespace_rows = [row for row in rows if row.get("section") == "namespace"]
        small_rows = [row for row in rows if row.get("section") == "small_file"]
        flush_rows = [row for row in rows if row.get("section") == "flush"]
        persistence_rows = [row for row in rows if row.get("section") == "persistence"]
        mount_rows = [row for row in rows if row.get("section") == "same_host_multi_mount"]
        lines = [
            "# Drive9 Kimi Workspace Performance Report",
            "",
            f"- Session: `{ctx.session}`",
            f"- Result dir: `{ctx.result_dir}`",
            f"- Target: `{getattr(ctx.target, 'server_url', '')}`",
            f"- Rows: `{len(rows)}`; issues: `{len(issues)}`; statuses: `{json.dumps(status_counts, sort_keys=True)}`",
            "",
            "## Requirement Coverage",
            "",
            "| Customer request | Evidence in this run | Status |",
            "|---|---|---|",
            f"| 100MB/1GB/10GB, 1k/10k/100k mount/ls/stat/find | namespace rows={len(namespace_rows)} | {self.coverage_status(namespace_rows)} |",
            f"| single workspace and single-directory scale | dataset_generate rows for selected scales/layouts | {self.dataset_coverage(rows)} |",
            f"| simultaneous sandbox mounts | same_host_multi_mount rows={len(mount_rows)} | {self.coverage_status(mount_rows)} |",
            f"| sandbox lifecycle and unmount stability | unmount rows/issues are captured | {self.lifecycle_status(rows, issues)} |",
            f"| small-file create/overwrite/append/partial edit QPS p95/p99 | small_file rows={len(small_rows)} | {self.coverage_status(small_rows)} |",
            f"| close/fsync/fdatasync p50/p95/p99 and visibility | flush rows={len(flush_rows)} | {self.coverage_status(flush_rows)} |",
            f"| remount persistence and checksum visibility | persistence rows={len(persistence_rows)} | {self.coverage_status(persistence_rows)} |",
            "",
            "## Issues And Limits",
            "",
        ]
        if issues:
            for item in issues:
                detail = item.get("detail", "")
                lines.append(f"- {item.get('severity', 'warn').upper()} `{item.get('section', '')}/{item.get('op', '')}` {detail}")
        else:
            lines.append("- None")
        lines.extend(["", "## Namespace Scale", ""])
        self.append_table(lines, namespace_rows, ["op", "scale", "layout", "status", "target_files", "created_files", "mount_ms", "unmount_ms", "unmount_exit_code", "unmount_mounted_after", "p50", "p95", "p99", "max", "qps", "errors", "timeouts", "unit"])
        lines.extend(["", "## Small File Workload", ""])
        self.append_table(lines, small_rows, ["op", "file_size", "concurrency", "status", "count", "p50", "p95", "p99", "max", "qps", "errors", "unit"])
        lines.extend(["", "## Flush / Fsync / Visibility", ""])
        self.append_table(lines, flush_rows, ["op", "file_size", "concurrency", "status", "count", "p50", "p95", "p99", "max", "qps", "errors", "visibility_errors", "unit"])
        lines.extend(["", "## Remount Persistence", ""])
        self.append_table(lines, persistence_rows, ["op", "file_size", "status", "count", "p50", "p95", "p99", "max", "errors", "unit"])
        lines.extend(["", "## Same-Host Multi-Mount", ""])
        self.append_table(lines, mount_rows, ["op", "concurrency", "status", "count", "p50", "p95", "p99", "max", "errors", "unit"])
        lines.extend(
            [
                "",
                "## Notes",
                "",
                "- `same_host_multi_mount` validates multiple mountpoints on one host; it is a sandbox-density proxy, not a hard fleet-wide upper bound.",
                "- Namespace cold-cache measurements use isolated cache directories per mount.",
                "- Flush visibility uses a separate reader mount/cache directory.",
                "- Persistence unmounts after write, remounts with a fresh cache, then verifies checksums.",
                "- Rows with `timeout` or `error` are product/test-environment findings and are intentionally kept in the report instead of aborting the module.",
                "",
            ]
        )
        return "\n".join(lines)

    @staticmethod
    def coverage_status(rows: list[dict[str, Any]]) -> str:
        if not rows:
            return "NOT RUN"
        statuses = {row.get("status") for row in rows}
        if statuses <= {"ok", "cached", "completed"}:
            return "COMPLETE"
        if statuses & {"ok", "cached", "completed"}:
            return "PARTIAL"
        return "FAILED"

    @staticmethod
    def dataset_coverage(rows: list[dict[str, Any]]) -> str:
        dataset_rows = [row for row in rows if row.get("section") == "namespace" and row.get("op") == "dataset_generate"]
        if not dataset_rows:
            return "NOT RUN"
        if all(row.get("status") in {"ok", "cached"} for row in dataset_rows):
            return "COMPLETE"
        if any(row.get("created_files", 0) for row in dataset_rows):
            return "PARTIAL"
        return "FAILED"

    @staticmethod
    def lifecycle_status(rows: list[dict[str, Any]], issues: list[dict[str, Any]]) -> str:
        unmount_rows = [row for row in rows if str(row.get("op", "")).endswith("unmount") or "unmount_ms" in row]
        unmount_issues = [item for item in issues if str(item.get("op", "")).endswith("_unmount")]
        if not unmount_rows and not unmount_issues:
            return "NOT RUN"
        if any(item.get("severity") == "error" for item in unmount_issues):
            return "FAILED"
        if unmount_issues or any(row.get("status") not in {"ok", "cached", "completed"} for row in unmount_rows):
            return "PARTIAL"
        return "COMPLETE"

    def append_table(self, lines: list[str], rows: list[dict[str, Any]], columns: list[str]) -> None:
        if not rows:
            lines.append("_No rows._")
            return
        lines.append("| " + " | ".join(columns) + " |")
        lines.append("|" + "|".join("---" for _ in columns) + "|")
        for row in rows:
            lines.append("| " + " | ".join(self.format_cell(row.get(column, "")) for column in columns) + " |")

    def matrix_row(
        self,
        section: str,
        op: str,
        scale: str,
        layout: str,
        file_size: Any,
        concurrency: Any,
        unit: str,
        values: list[float],
        errors: int,
        total_ops: int,
        runs: int,
        *,
        qps: Any = "",
        status: str = "ok",
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row = {
            "section": section,
            "op": op,
            "scale": scale,
            "layout": layout,
            "file_size": file_size,
            "concurrency": concurrency,
            "unit": unit,
            "status": status,
            "errors": errors,
            "error_rate": errors / max(1, total_ops),
            "runs": runs,
            "qps": qps,
            **self.latency_summary(values),
        }
        if extra:
            row.update(extra)
        return row

    @staticmethod
    def control_row(section: str, status: str, detail: str, seconds: float) -> dict[str, Any]:
        return {
            "section": "control",
            "op": section,
            "status": status,
            "detail": detail,
            "unit": "seconds",
            "errors": 0 if status in {"completed", "skipped"} else 1,
            "error_rate": 0.0 if status in {"completed", "skipped"} else 1.0,
            "runs": 1,
            **Drive9KimiPerf.latency_summary([seconds] if seconds else []),
        }

    def capture_environment(self, ctx: Context, artifact: Path) -> None:
        commands = {
            "uname": ["uname", "-a"],
            "os_release": ["bash", "-lc", "cat /etc/os-release 2>/dev/null || true"],
            "nproc": ["bash", "-lc", "nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || true"],
            "free": ["bash", "-lc", "free -h 2>/dev/null || vm_stat 2>/dev/null || true"],
            "df": ["df", "-h"],
            "dev_fuse": ["bash", "-lc", "ls -l /dev/fuse 2>/dev/null || true"],
            "drive9_version": [str(ctx.target.cli), "--version"],
        }
        out: dict[str, str] = {}
        for name, command in commands.items():
            try:
                proc = subprocess.run(command, cwd=str(ctx.result_dir), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30, check=False)
                out[name] = proc.stdout
            except Exception as exc:
                out[name] = str(exc)
        out["drive9_server_url"] = getattr(ctx.target, "server_url", "")
        out["drive9_server_probe"] = json.dumps(self.server_probe(getattr(ctx.target, "server_url", "")), sort_keys=True)
        write_json(artifact / "environment.json", out)

    @staticmethod
    def server_probe(server_url: str) -> dict[str, Any]:
        if not server_url:
            return {"skipped": "server url is empty"}
        parsed = urllib.parse.urlparse(server_url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if not host:
            return {"error": f"cannot parse server url: {server_url}"}
        tcp_values: list[float] = []
        errors: list[str] = []
        for _ in range(10):
            start = time.perf_counter()
            try:
                with socket.create_connection((host, port), timeout=5):
                    tcp_values.append((time.perf_counter() - start) * 1000)
            except OSError as exc:
                errors.append(str(exc))
        health: dict[str, Any] = {}
        start = time.perf_counter()
        try:
            with urllib.request.urlopen(server_url.rstrip("/") + "/healthz", timeout=10) as response:
                body = response.read(2048).decode("utf-8", errors="replace")
            health = {"status": "ok", "latency_ms": (time.perf_counter() - start) * 1000, "body": body}
        except Exception as exc:
            health = {"status": "error", "latency_ms": (time.perf_counter() - start) * 1000, "error": str(exc)}
        return {"host": host, "port": port, "tcp_connect_ms": Drive9KimiPerf.latency_summary(tcp_values), "tcp_errors": errors, "healthz": health}

    @staticmethod
    def latency_summary(values: list[float]) -> dict[str, float]:
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

    @staticmethod
    def csv_env(ctx: Context, suffix: str, default: list[str]) -> list[str]:
        value = env_value(suffix, "", ctx.suite)
        if not value:
            return [str(item) for item in default]
        return [item.strip() for item in value.split(",") if item.strip()]

    @staticmethod
    def int_csv_env(ctx: Context, suffix: str, default: list[int]) -> list[int]:
        value = env_value(suffix, "", ctx.suite)
        if not value:
            return [int(item) for item in default]
        return [int(item.strip()) for item in value.split(",") if item.strip()]

    @staticmethod
    def timeout_map_env(ctx: Context, suffix: str, default: dict[str, float]) -> dict[str, float]:
        value = env_value(suffix, "", ctx.suite)
        result = {str(key): float(timeout_s) for key, timeout_s in default.items()}
        if not value:
            return result
        for item in value.split(","):
            item = item.strip()
            if not item:
                continue
            if "=" in item:
                key, timeout_s = item.split("=", 1)
            elif ":" in item:
                key, timeout_s = item.split(":", 1)
            else:
                raise ValueError(f"{suffix} item must be KEY=SECONDS, got {item!r}")
            result[key.strip()] = float(timeout_s.strip())
        return result

    @staticmethod
    def format_cell(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.3f}"
        return str(value)


def percentile(ordered: list[float], pct: int) -> float:
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * pct / 100
    lower = int(idx)
    upper = min(lower + 1, len(ordered) - 1)
    weight = idx - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight
