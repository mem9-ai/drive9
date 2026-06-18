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
    description = "Kimi sandbox workspace benchmark: namespace scale, small files, fsync, visibility, and same-host mounts."
    labels = ("drive9", "customer", "kimi", "performance", "fuse")
    timeout = 86400

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
        write_json(artifact / "manifest.json", {"remote_base": remote_base, "config": cfg, "session": ctx.session})

        rows: list[dict[str, Any]] = []
        failures: list[str] = []

        if cfg["sections"]["namespace"]:
            rows.extend(self.run_namespace_suite(ctx, cfg, remote_base, raw_dir, failures))
        if cfg["sections"]["small_file"]:
            rows.extend(self.run_small_file_suite(ctx, cfg, remote_base, raw_dir, failures))
        if cfg["sections"]["flush"]:
            rows.extend(self.run_flush_suite(ctx, cfg, remote_base, raw_dir, failures))
        if cfg["sections"]["persistence"]:
            rows.extend(self.run_persistence_suite(ctx, cfg, remote_base, raw_dir, failures))
        if cfg["sections"]["multi_mount"]:
            rows.extend(self.run_same_host_mount_suite(ctx, cfg, remote_base, failures))
        if cfg["sections"]["soak"]:
            rows.extend(self.run_soak_suite(ctx, cfg, remote_base, raw_dir, failures))

        self.write_summary_outputs(ctx, artifact, summary_dir, rows, failures)
        if failures:
            raise BlackboxError(f"Kimi perf failures={len(failures)}; see {artifact / 'report.md'}")
        return {"remote_base": remote_base, "summary_rows": len(rows), "artifact": str(artifact)}

    def config(self, ctx: Context) -> dict[str, Any]:
        cfg = module_config(ctx, self.id)
        scales = cfg.get(
            "scales",
            {
                "S": {"bytes": 100 * 1024 * 1024, "files": 1000, "mount_repeats": 5},
                "M": {"bytes": 1024 * 1024 * 1024, "files": 10000, "mount_repeats": 5},
                "L": {"bytes": 10 * 1024 * 1024 * 1024, "files": 100000, "mount_repeats": 3},
            },
        )
        selected_scales = self.csv_env(ctx, "KIMI_PERF_SCALES", cfg.get("selected_scales", ["S"]))
        layouts = self.csv_env(ctx, "KIMI_PERF_LAYOUTS", cfg.get("layouts", ["single", "tree"]))
        small_sizes = self.int_csv_env(ctx, "KIMI_PERF_SMALL_SIZES", cfg.get("small_file_sizes", [1024, 4096, 20 * 1024, 100 * 1024, 1024 * 1024]))
        small_concurrency = self.int_csv_env(ctx, "KIMI_PERF_SMALL_CONCURRENCY", cfg.get("small_file_concurrency", [1, 4, 16, 64]))
        flush_sizes = self.int_csv_env(ctx, "KIMI_PERF_FLUSH_SIZES", cfg.get("flush_file_sizes", [1024, 4096, 20 * 1024, 100 * 1024, 1024 * 1024]))
        flush_concurrency = self.int_csv_env(ctx, "KIMI_PERF_FLUSH_CONCURRENCY", cfg.get("flush_concurrency", [1, 4, 16, 64]))
        sections_default = cfg.get("sections", {})
        return {
            "scales": scales,
            "selected_scales": selected_scales,
            "layouts": layouts,
            "profile": env_value("KIMI_PERF_PROFILE", str(cfg.get("profile", "coding-agent")), ctx.suite),
            "durability": env_value("KIMI_PERF_DURABILITY", str(cfg.get("durability", "auto")), ctx.suite),
            "namespace_stat_samples": int(env_value("KIMI_PERF_STAT_SAMPLES", str(cfg.get("namespace_stat_samples", 1000)), ctx.suite)),
            "small_file_sizes": small_sizes,
            "small_file_concurrency": small_concurrency,
            "small_file_ops": int(env_value("KIMI_PERF_SMALL_OPS", str(cfg.get("small_file_ops", 1000)), ctx.suite)),
            "flush_file_sizes": flush_sizes,
            "flush_concurrency": flush_concurrency,
            "flush_ops": int(env_value("KIMI_PERF_FLUSH_OPS", str(cfg.get("flush_ops", 1000)), ctx.suite)),
            "visibility_samples": int(env_value("KIMI_PERF_VISIBILITY_SAMPLES", str(cfg.get("visibility_samples", 100)), ctx.suite)),
            "visibility_timeout_s": float(env_value("KIMI_PERF_VISIBILITY_TIMEOUT_S", str(cfg.get("visibility_timeout_s", 30)), ctx.suite)),
            "same_host_mount_counts": self.int_csv_env(ctx, "KIMI_PERF_MOUNT_COUNTS", cfg.get("same_host_mount_counts", [1, 2, 5])),
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

    def run_namespace_suite(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, failures: list[str]) -> list[dict[str, Any]]:
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
                self.prepare_dataset(ctx, cfg, remote, scale_id, layout, scale)
                rows.extend(self.measure_mounts(ctx, cfg, remote, scale_id, layout, scale))
                rows.extend(self.measure_namespace(ctx, cfg, remote, scale_id, layout, raw_dir, failures))
        return rows

    def prepare_dataset(self, ctx: Context, cfg: dict[str, Any], remote: str, scale_id: str, layout: str, scale: dict[str, Any]) -> None:
        handle = ctx.target.mount(
            "kimi_dataset_prepare",
            remote,
            profile=cfg["profile"],
            durability=cfg["durability"],
            cache_key=f"{scale_id}-{layout}-prepare",
        )
        try:
            manifest = handle.mountpoint / ".drive9-kimi-dataset.json"
            expected = {"scale": scale_id, "layout": layout, "bytes": int(scale["bytes"]), "files": int(scale["files"])}
            if cfg["reuse_datasets"] and manifest.exists():
                try:
                    current = json.loads(manifest.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    current = {}
                if all(current.get(key) == value for key, value in expected.items()):
                    progress(f"kimi dataset cached: {scale_id}-{layout}")
                    return
            progress(f"kimi dataset generate: {scale_id}-{layout} bytes={scale['bytes']} files={scale['files']}")
            data_dir = handle.mountpoint / "data"
            if data_dir.exists():
                shutil.rmtree(data_dir)
            data_dir.mkdir()
            start = time.perf_counter()
            self.generate_dataset(data_dir, layout, int(scale["bytes"]), int(scale["files"]))
            expected.update({"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "seconds": time.perf_counter() - start})
            manifest.write_text(json.dumps(expected, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            ctx.metric(f"{self.id}.dataset.generate_seconds", float(expected["seconds"]), "seconds", {"scale": scale_id, "layout": layout})
        finally:
            ctx.target.unmount(handle)

    def generate_dataset(self, data_dir: Path, layout: str, total_bytes: int, file_count: int) -> None:
        base = total_bytes // file_count
        extra = total_bytes % file_count
        payload = stable_bytes(min(max(base + 1, 1), 1024 * 1024), seed=file_count)
        checkpoints = {max(1, file_count * pct // 10) for pct in range(1, 11)}
        for idx in range(file_count):
            size = base + (1 if idx < extra else 0)
            if layout == "single":
                parent = data_dir
            else:
                parent = data_dir / f"shard-{idx // 1000:05d}"
                parent.mkdir(parents=True, exist_ok=True)
            path = parent / f"file-{idx:08d}.bin"
            self.write_payload(path, payload, size)
            if idx + 1 in checkpoints:
                progress(f"kimi dataset progress: {idx + 1}/{file_count}")

    @staticmethod
    def write_payload(path: Path, payload: bytes, size: int) -> None:
        remaining = size
        with path.open("wb") as handle:
            while remaining > 0:
                chunk = payload[: min(len(payload), remaining)]
                handle.write(chunk)
                remaining -= len(chunk)

    def measure_mounts(self, ctx: Context, cfg: dict[str, Any], remote: str, scale_id: str, layout: str, scale: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        values: list[float] = []
        repeats = int(scale.get("mount_repeats", 3))
        for idx in range(repeats):
            start = time.perf_counter()
            handle = ctx.target.mount(
                "kimi_mount_latency",
                remote,
                profile=cfg["profile"],
                durability=cfg["durability"],
                cache_key=f"{scale_id}-{layout}-mount-{idx}",
            )
            seconds = time.perf_counter() - start
            values.append(seconds * 1000)
            rows.append(self.summary_row("mount", scale_id, layout, "mount", seconds, 1, 0, "ms", {"phase": "cold"}))
            ctx.target.unmount(handle)
        ctx.perf_values(f"{self.id}.namespace.{scale_id}.{layout}.mount", values, "ms")
        return rows

    def measure_namespace(self, ctx: Context, cfg: dict[str, Any], remote: str, scale_id: str, layout: str, raw_dir: Path, failures: list[str]) -> list[dict[str, Any]]:
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
                result = ctx.target.run_cmd(f"kimi-namespace-{scale_id}-{layout}-{name}", command, timeout=7200, shell=True)
                if result.ok:
                    rows.append(self.summary_row("namespace", scale_id, layout, name, result.seconds, 1, 0, "seconds", {}))
                    ctx.metric(f"{self.id}.namespace.{scale_id}.{layout}.{name}", result.seconds, "seconds")
                else:
                    failures.append(f"namespace {scale_id}/{layout}/{name} failed; see {result.stderr}")
                    rows.append(self.summary_row("namespace", scale_id, layout, name, result.seconds, 0, 1, "seconds", {}))
            stat_values = self.measure_stat_samples(data_dir, int(cfg["namespace_stat_samples"]), raw_dir / f"stat-{scale_id}-{layout}.jsonl")
            if stat_values:
                summary = self.latency_summary(stat_values)
                rows.append({"section": "namespace", "scale": scale_id, "layout": layout, "op": "stat", "unit": "ms", **summary})
                ctx.perf_values(f"{self.id}.namespace.{scale_id}.{layout}.stat", stat_values, "ms")
        finally:
            ctx.target.unmount(handle)
        return rows

    def measure_stat_samples(self, data_dir: Path, samples: int, raw_path: Path) -> list[float]:
        files = list(data_dir.rglob("*.bin"))
        if not files:
            return []
        rng = random.Random(9)
        chosen = [files[rng.randrange(0, len(files))] for _ in range(min(samples, len(files)))]
        values: list[float] = []
        with raw_path.open("w", encoding="utf-8") as raw:
            for path in chosen:
                start = time.perf_counter()
                try:
                    os.stat(path)
                    status = "ok"
                    err = ""
                except OSError as exc:
                    status = "error"
                    err = str(exc)
                latency = (time.perf_counter() - start) * 1000
                if status == "ok":
                    values.append(latency)
                raw.write(json.dumps({"op": "stat", "path": str(path), "latency_ms": latency, "status": status, "error": err}, sort_keys=True) + "\n")
        return values

    def run_small_file_suite(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, failures: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/small-file"
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("kimi_small_file", remote, profile=cfg["profile"], durability=cfg["durability"], cache_key="small-file-writer")
        try:
            root = handle.mountpoint / "small-file"
            root.mkdir(exist_ok=True)
            for size in cfg["small_file_sizes"]:
                for concurrency in cfg["small_file_concurrency"]:
                    rows.extend(self.small_file_matrix(ctx, cfg, root, raw_dir, int(size), int(concurrency), failures))
        finally:
            ctx.target.unmount(handle)
        return rows

    def small_file_matrix(self, ctx: Context, cfg: dict[str, Any], root: Path, raw_dir: Path, size: int, concurrency: int, failures: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        ops = max(1, int(cfg["small_file_ops"]))
        payload = stable_bytes(size, seed=size)
        append_payload = stable_bytes(min(1024, size), seed=size + 1)
        edit_payload = stable_bytes(min(128, size), seed=size + 2)
        for op in ("create", "overwrite", "append", "partial_edit", "read", "stat_after_write"):
            op_root = root / f"{op}-{size}-{concurrency}"
            if op_root.exists():
                shutil.rmtree(op_root)
            op_root.mkdir(parents=True)
            if op in {"overwrite", "append", "partial_edit", "read"}:
                for idx in range(ops):
                    (op_root / f"f-{idx:08d}.bin").write_bytes(payload)
            raw_path = raw_dir / f"small-{op}-{size}-{concurrency}.jsonl"
            values, errors, wall_seconds = self.run_latency_workload(
                raw_path,
                ops,
                concurrency,
                lambda idx, op=op, op_root=op_root: self.small_file_op(op, op_root, idx, payload, append_payload, edit_payload),
                cfg["raw_results"],
                {"section": "small_file", "op": op, "size": size, "concurrency": concurrency},
            )
            summary = self.latency_summary(values)
            qps = len(values) / wall_seconds if wall_seconds > 0 else 0.0
            row = {
                "section": "small_file",
                "scale": "",
                "layout": "",
                "op": op,
                "file_size": size,
                "concurrency": concurrency,
                "unit": "ms",
                "qps": qps,
                "errors": errors,
                "error_rate": errors / ops,
                **summary,
            }
            rows.append(row)
            ctx.perf_values(f"{self.id}.small_file.{op}.{size}b.c{concurrency}", values, "ms")
            if errors:
                failures.append(f"small_file {op} size={size} concurrency={concurrency} errors={errors}")
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

    def run_flush_suite(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, failures: list[str]) -> list[dict[str, Any]]:
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
                        rows.extend(self.flush_matrix(ctx, cfg, root, reader_root, raw_dir, int(size), int(concurrency), mode, failures))
        finally:
            ctx.target.unmount(reader)
            ctx.target.unmount(writer)
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
        failures: list[str],
    ) -> list[dict[str, Any]]:
        ops = max(1, int(cfg["flush_ops"]))
        op_root = root / f"{mode}-{size}-{concurrency}"
        reader_op_root = reader_root / f"{mode}-{size}-{concurrency}"
        if op_root.exists():
            shutil.rmtree(op_root)
        op_root.mkdir(parents=True)
        payload = stable_bytes(size, seed=size + concurrency)
        raw_path = raw_dir / f"flush-{mode}-{size}-{concurrency}.jsonl"
        visibility_limit = min(ops, int(cfg["visibility_samples"]))

        def body(idx: int) -> dict[str, float]:
            path = op_root / f"f-{idx:08d}.bin"
            rel = path.relative_to(root)
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
                visible_ms = self.wait_visible(reader_op_root / rel.name, digest, float(cfg["visibility_timeout_s"]))
                if visible_ms < 0:
                    raise BlackboxError(f"visibility timeout for {rel}")
            return {"write_ms": write_ms, "sync_ms": sync_ms, "close_ms": close_ms, "visible_ms": visible_ms}

        values, errors, sync_values, close_values, visible_values, wall_seconds = self.run_flush_workload(raw_path, ops, concurrency, body, cfg["raw_results"], {"mode": mode, "size": size, "concurrency": concurrency})
        rows: list[dict[str, Any]] = []
        qps = len(values) / wall_seconds if wall_seconds > 0 else 0.0
        for metric, metric_values in (("total", values), ("sync", sync_values), ("close", close_values), ("visible", visible_values)):
            if not metric_values:
                continue
            row = {
                "section": "flush",
                "scale": "",
                "layout": "",
                "op": f"{mode}_{metric}",
                "file_size": size,
                "concurrency": concurrency,
                "unit": "ms",
                "qps": qps if metric == "total" else "",
                "errors": errors,
                "error_rate": errors / ops,
                **self.latency_summary(metric_values),
            }
            rows.append(row)
            ctx.perf_values(f"{self.id}.flush.{mode}.{metric}.{size}b.c{concurrency}", metric_values, "ms")
        if errors:
            failures.append(f"flush {mode} size={size} concurrency={concurrency} errors={errors}")
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

    def run_persistence_suite(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, failures: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/persistence"
        ctx.target.mkdir_remote(remote)
        samples = max(1, int(cfg["visibility_samples"]))
        for size in cfg["flush_file_sizes"]:
            for mode in ("close", "fsync"):
                rows.extend(self.persistence_matrix(ctx, cfg, remote, raw_dir, int(size), mode, samples, failures))
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
        failures: list[str],
    ) -> list[dict[str, Any]]:
        payload = stable_bytes(size, seed=size + samples)
        digest = hashlib.sha256(payload).hexdigest()
        writer = ctx.target.mount("kimi_persistence", remote, profile=cfg["profile"], durability=cfg["durability"], cache_key=f"{mode}-{size}-writer")
        write_values: list[float] = []
        rows: list[dict[str, Any]] = []
        raw_path = raw_dir / f"persistence-{mode}-{size}.jsonl"
        raw_handle = raw_path.open("w", encoding="utf-8") if cfg["raw_results"] else None
        try:
            root = writer.mountpoint / f"{mode}-{size}"
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
                if raw_handle is not None:
                    raw_handle.write(json.dumps({"section": "persistence", "phase": "write", "mode": mode, "size": size, "idx": idx, "latency_ms": latency, "status": "ok"}, sort_keys=True) + "\n")
        finally:
            if raw_handle is not None:
                raw_handle.close()
            ctx.target.unmount(writer)

        reader = ctx.target.mount("kimi_persistence", remote, profile=cfg["profile"], durability=cfg["durability"], cache_key=f"{mode}-{size}-reader")
        read_values: list[float] = []
        errors = 0
        raw_handle = raw_path.open("a", encoding="utf-8") if cfg["raw_results"] else None
        try:
            root = reader.mountpoint / f"{mode}-{size}"
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
                if raw_handle is not None:
                    raw_handle.write(json.dumps({"section": "persistence", "phase": "remount_read", "mode": mode, "size": size, "idx": idx, "latency_ms": latency, "status": status, "error": err}, sort_keys=True) + "\n")
        finally:
            if raw_handle is not None:
                raw_handle.close()
            ctx.target.unmount(reader)

        rows.append({"section": "persistence", "scale": "", "layout": "", "op": f"{mode}_write_before_remount", "file_size": size, "unit": "ms", "errors": 0, "error_rate": 0.0, **self.latency_summary(write_values)})
        rows.append({"section": "persistence", "scale": "", "layout": "", "op": f"{mode}_remount_read", "file_size": size, "unit": "ms", "errors": errors, "error_rate": errors / samples, **self.latency_summary(read_values)})
        ctx.perf_values(f"{self.id}.persistence.{mode}.write.{size}b", write_values, "ms")
        ctx.perf_values(f"{self.id}.persistence.{mode}.remount_read.{size}b", read_values, "ms")
        if errors:
            failures.append(f"persistence {mode} size={size} errors={errors}")
        return rows

    def run_same_host_mount_suite(self, ctx: Context, cfg: dict[str, Any], remote_base: str, failures: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/same-host-mount"
        ctx.target.mkdir_remote(remote)
        for count in cfg["same_host_mount_counts"]:
            handles = []
            values: list[float] = []
            errors = 0
            try:
                for idx in range(int(count)):
                    start = time.perf_counter()
                    try:
                        handles.append(ctx.target.mount("kimi_same_host_mount", remote, profile=cfg["profile"], durability=cfg["durability"], cache_key=f"mount-{count}-{idx}"))
                        values.append((time.perf_counter() - start) * 1000)
                    except Exception:
                        errors += 1
                for idx, handle in enumerate(handles):
                    probe = handle.mountpoint / f"probe-{count}-{idx}.txt"
                    probe.write_text(f"{count}-{idx}\n", encoding="utf-8")
                    if probe.read_text(encoding="utf-8") != f"{count}-{idx}\n":
                        errors += 1
            finally:
                for handle in reversed(handles):
                    ctx.target.unmount(handle)
            row = {"section": "same_host_multi_mount", "scale": "", "layout": "", "op": "mount", "concurrency": int(count), "unit": "ms", "errors": errors, "error_rate": errors / max(1, int(count)), **self.latency_summary(values)}
            rows.append(row)
            ctx.perf_values(f"{self.id}.same_host_multi_mount.c{count}.mount", values, "ms")
            if errors:
                failures.append(f"same_host_multi_mount count={count} errors={errors}")
        return rows

    def run_soak_suite(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, failures: list[str]) -> list[dict[str, Any]]:
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
                    raw.write(json.dumps({"op": "soak_write_read", "latency_ms": latency, "status": status, "error": err}, sort_keys=True) + "\n")
                    idx += 1
                    time.sleep(1)
        finally:
            ctx.target.unmount(handle)
        rows.append({"section": "soak", "scale": "", "layout": "", "op": "write_read", "unit": "ms", "errors": errors, "error_rate": errors / max(1, idx), **self.latency_summary(values)})
        ctx.perf_values(f"{self.id}.soak.write_read", values, "ms")
        if errors:
            failures.append(f"soak errors={errors}")
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
                    if raw_handle is not None:
                        raw_handle.write(json.dumps({**labels, "idx": idx, "latency_ms": latency, "status": "error" if error else "ok", "error": error}, sort_keys=True) + "\n")
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
    ) -> tuple[list[float], int, list[float], list[float], list[float], float]:
        values: list[float] = []
        sync_values: list[float] = []
        close_values: list[float] = []
        visible_values: list[float] = []
        errors = 0
        raw_handle = raw_path.open("w", encoding="utf-8") if write_raw else None
        start_wall = time.perf_counter()
        try:
            with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
                futures = {pool.submit(self.timed_call_result, fn, idx): idx for idx in range(ops)}
                for future in as_completed(futures):
                    idx = futures[future]
                    latency, result, error = future.result()
                    if error:
                        errors += 1
                    else:
                        values.append(latency)
                        sync_values.append(result.get("sync_ms", 0.0))
                        close_values.append(result.get("close_ms", 0.0))
                        if result.get("visible_ms", 0.0) > 0:
                            visible_values.append(result["visible_ms"])
                    if raw_handle is not None:
                        raw_handle.write(json.dumps({**labels, "idx": idx, "latency_ms": latency, **result, "status": "error" if error else "ok", "error": error}, sort_keys=True) + "\n")
        finally:
            if raw_handle is not None:
                raw_handle.close()
        return values, errors, sync_values, close_values, visible_values, time.perf_counter() - start_wall

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

    def write_summary_outputs(self, ctx: Context, artifact: Path, summary_dir: Path, rows: list[dict[str, Any]], failures: list[str]) -> None:
        all_keys = sorted({key for row in rows for key in row.keys()})
        csv_path = summary_dir / "summary.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=all_keys)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        write_json(summary_dir / "summary.json", {"rows": rows, "failures": failures})
        lines = [
            "# Drive9 Kimi Performance Report",
            "",
            f"- Session: `{ctx.session}`",
            f"- Result dir: `{ctx.result_dir}`",
            f"- Failures: `{len(failures)}`",
            "",
            "## Notes",
            "",
            "- `same_host_multi_mount` validates multiple mountpoints on one host; it is not claimed as a multi-sandbox upper bound.",
            "- Cold-cache namespace measurements use isolated cache directories per mount.",
            "- Cross-mount visibility uses a separate reader mount/cache directory.",
            "- Persistence checks unmount after write, then remount with a fresh cache before reading data back.",
            "",
            "## Failures",
            "",
        ]
        if failures:
            lines.extend(f"- {item}" for item in failures)
        else:
            lines.append("- None")
        lines.extend(["", "## Summary Rows", "", "| Section | Operation | Scale | Layout | Size | Concurrency | p50 | p95 | p99 | Max | QPS | Errors | Error Rate | Unit |", "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"])
        for row in rows:
            lines.append(
                "| "
                f"{row.get('section', '')} | {row.get('op', '')} | {row.get('scale', '')} | {row.get('layout', '')} | "
                f"{row.get('file_size', '')} | {row.get('concurrency', '')} | {self.fmt(row.get('p50', ''))} | {self.fmt(row.get('p95', ''))} | "
                f"{self.fmt(row.get('p99', ''))} | {self.fmt(row.get('max', ''))} | {self.fmt(row.get('qps', ''))} | {row.get('errors', 0)} | {self.fmt(row.get('error_rate', ''))} | {row.get('unit', '')} |"
            )
        (artifact / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

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
    def summary_row(section: str, scale: str, layout: str, op: str, seconds: float, success: int, errors: int, unit: str, extra: dict[str, Any]) -> dict[str, Any]:
        value = seconds * 1000 if unit == "ms" else seconds
        return {"section": section, "scale": scale, "layout": layout, "op": op, "count": success, "errors": errors, "p50": value, "p95": value, "p99": value, "max": value, "mean": value, "unit": unit, **extra}

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
    def fmt(value: Any) -> str:
        if value == "":
            return ""
        try:
            return f"{float(value):.3f}"
        except (TypeError, ValueError):
            return str(value)


def percentile(ordered: list[float], pct: int) -> float:
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct / 100
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    weight = rank - lo
    return ordered[lo] * (1 - weight) + ordered[hi] * weight
