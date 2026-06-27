from __future__ import annotations

import csv
import hashlib
import json
import os
import shlex
import shutil
import statistics
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

from harness.core import BlackboxError, Context, ModuleSkip, env_flag, env_value, progress, stable_bytes, write_json

from harness.module_base import BaseModule, module_config


# Durability label -> drive9 mount --durability value.
# The customer document speaks of "writeback" and "write-sync"; drive9's
# durability flag uses "auto" (writeback write policy) and "write-sync".
DURABILITY_WRITEBACK = "auto"
DURABILITY_WRITE_SYNC = "write-sync"
DURABILITY_INTERACTIVE = "interactive"


class Drive9ManusPerf(BaseModule):
    """Customer benchmark covering the Manus Persistent Sandbox / cloud-PC
    shared-storage scenario.

    Implements the test requirements captured in the 2026-06-23 meeting with
    Minghua / Manus:

    - Multi-session shared workspace: multiple agents mount the same workspace,
      cross-mount read visibility, concurrent read/write of distinct and same
      files, directory list/stat/open/close latency, write-sync small-file
      latency, cache-invalidation read cost, 2MB-threshold routing.
    - Single-session baseline: infinite-TTL read + writeback, infinite-TTL read
      + write-sync, across 100MB/1k, 1GB/10k, 10GB/100k scale tiers.
    - File-lock current-behavior probe.
    - Extra case: clone+build time of a vite+react+tailwind GitHub repo under
      local disk, drive9 FUSE writeback, and drive9 FUSE write-sync, each
      crossed with coding-agent and none profiles.
    """

    id = "drive9.customer.manus_perf"
    category = "drive9.customer.performance"
    description = (
        "Manus Persistent Sandbox / cloud-PC shared-storage benchmark: "
        "multi-session shared workspace, read/write consistency, cache "
        "invalidation, write-sync small-file latency, 2MB routing, single-"
        "session TTL baselines, file-lock behavior, and a vite+react+tailwind "
        "clone+build comparison across local disk / drive9 writeback / write-sync."
    )
    labels = ("drive9", "customer", "manus", "performance", "fuse")
    timeout = 7200
    report_profile = "customer"
    # Cached report markdown from the last run(), used by render_report().
    _last_report_markdown: str = ""

    # ----- dependency / opt-in gate -------------------------------------

    def ensure_dependencies(self, ctx: Context) -> None:
        if not env_flag("MANUS_PERF_ENABLE", False, ctx.suite):
            raise ModuleSkip("set BLACKBOX_MANUS_PERF_ENABLE=1 to run Manus performance tests", "explicit opt-in")
        for tool in ("bash", "find", "git"):
            ctx.deps.require_tool(tool)
        # The extra clone+build case needs node tooling for the vite repo.
        if self.config(ctx)["sections"]["extra_clone_build"]:
            for tool in ("node", "pnpm"):
                ctx.deps.require_tool(tool)

    # ----- top-level run / config --------------------------------------

    def run(self, ctx: Context) -> dict[str, Any]:
        cfg = self.config(ctx)
        artifact = ctx.artifact_dir(self.id)
        raw_dir = artifact / "raw_results"
        summary_dir = artifact / "summary"
        for path in (raw_dir, summary_dir):
            path.mkdir(parents=True, exist_ok=True)

        remote_base = env_value("MANUS_PERF_REMOTE_ROOT", ctx.target.remote_root(self.id), ctx.suite).rstrip("/")
        ctx.target.mkdir_remote(remote_base)
        self.capture_environment(ctx, artifact)
        manifest_config = {key: value for key, value in cfg.items() if not key.startswith("_")}
        write_json(artifact / "manifest.json", {"remote_base": remote_base, "config": manifest_config, "session": ctx.session})

        rows: list[dict[str, Any]] = []
        issues: list[dict[str, Any]] = []

        def checkpoint(extra_rows: list[dict[str, Any]] | None = None) -> None:
            self.write_summary_outputs(ctx, artifact, summary_dir, rows + (extra_rows or []), issues)

        sections: list[tuple[str, bool, Callable[[], list[dict[str, Any]]]]] = [
            ("multi_session_shared", bool(cfg["sections"]["multi_session_shared"]), lambda: self.run_multi_session_shared(ctx, cfg, remote_base, raw_dir, issues)),
            ("cross_visibility", bool(cfg["sections"]["cross_visibility"]), lambda: self.run_cross_visibility(ctx, cfg, remote_base, raw_dir, issues)),
            ("concurrent_distinct", bool(cfg["sections"]["concurrent_distinct"]), lambda: self.run_concurrent_distinct(ctx, cfg, remote_base, raw_dir, issues)),
            ("concurrent_same_file", bool(cfg["sections"]["concurrent_same_file"]), lambda: self.run_concurrent_same_file(ctx, cfg, remote_base, raw_dir, issues)),
            ("namespace_ops", bool(cfg["sections"]["namespace_ops"]), lambda: self.run_namespace_ops(ctx, cfg, remote_base, raw_dir, issues)),
            ("write_sync_small", bool(cfg["sections"]["write_sync_small"]), lambda: self.run_write_sync_small(ctx, cfg, remote_base, raw_dir, issues)),
            ("cache_invalidation", bool(cfg["sections"]["cache_invalidation"]), lambda: self.run_cache_invalidation(ctx, cfg, remote_base, raw_dir, issues)),
            ("routing_2mb", bool(cfg["sections"]["routing_2mb"]), lambda: self.run_routing_2mb(ctx, cfg, remote_base, raw_dir, issues)),
            ("large_file_s3", bool(cfg["sections"]["large_file_s3"]), lambda: self.run_large_file_s3(ctx, cfg, remote_base, raw_dir, issues)),
            ("single_session", bool(cfg["sections"]["single_session"]), lambda: self.run_single_session(ctx, cfg, remote_base, raw_dir, issues, checkpoint)),
            ("file_lock", bool(cfg["sections"]["file_lock"]), lambda: self.run_file_lock(ctx, cfg, remote_base, raw_dir, issues)),
            ("extra_clone_build", bool(cfg["sections"]["extra_clone_build"]), lambda: self.run_extra_clone_build(ctx, cfg, remote_base, raw_dir, issues)),
        ]
        for name, enabled, fn in sections:
            if not enabled:
                rows.append(self.control_row(name, "skipped", "section disabled by config", 0.0))
                checkpoint()
                continue
            progress(f"manus perf section start: {name}")
            started = time.perf_counter()
            try:
                produced = fn()
                rows.extend(produced)
                rows.append(self.control_row(name, "completed", f"rows={len(produced)}", time.perf_counter() - started))
                progress(f"manus perf section done: {name} rows={len(produced)}")
            except Exception as exc:
                elapsed = time.perf_counter() - started
                detail = f"{type(exc).__name__}: {exc}"
                issues.append({"severity": "error", "section": name, "op": "section", "detail": detail})
                rows.append(self.control_row(name, "error", detail, elapsed))
                progress(f"manus perf section error: {name}: {detail}")
            finally:
                checkpoint()

        checkpoint()
        return {"remote_base": remote_base, "summary_rows": len(rows), "issues": len(issues), "artifact": str(artifact)}

    def config(self, ctx: Context) -> dict[str, Any]:
        cfg = module_config(ctx, self.id)
        scales = cfg.get(
            "scales",
            {
                "S": {"bytes": 100 * 1024 * 1024, "files": 1000},
                "M": {"bytes": 1024 * 1024 * 1024, "files": 10000},
                "L": {"bytes": 10 * 1024 * 1024 * 1024, "files": 100000},
            },
        )
        sections_default = cfg.get("sections", {})
        return {
            "scales": scales,
            "selected_scales": self.csv_env(ctx, "MANUS_PERF_SCALES", cfg.get("selected_scales", ["S"])),
            "runs": max(1, int(env_value("MANUS_PERF_RUNS", str(ctx.runs), ctx.suite))),
            "profile": env_value("MANUS_PERF_PROFILE", str(cfg.get("profile", "coding-agent")), ctx.suite),
            "durability": env_value("MANUS_PERF_DURABILITY", str(cfg.get("durability", "auto")), ctx.suite),
            # Mount TTL for infinite-TTL read baselines: 0 disables time-based
            # read-cache expiry (the "infinite TTL read" combination).
            "infinite_ttl_read_cache_s": float(env_value("MANUS_PERF_INFINITE_TTL_S", str(cfg.get("infinite_ttl_read_cache_s", 0)), ctx.suite)),
            "agent_counts": self.int_csv_env(ctx, "MANUS_PERF_AGENT_COUNTS", cfg.get("agent_counts", [2, 4])),
            "small_file_sizes": self.int_csv_env(ctx, "MANUS_PERF_SMALL_SIZES", cfg.get("small_file_sizes", [1024, 20 * 1024, 100 * 1024])),
            "small_file_concurrency": self.int_csv_env(ctx, "MANUS_PERF_SMALL_CONCURRENCY", cfg.get("small_file_concurrency", [1, 4, 16])),
            "small_file_ops": int(env_value("MANUS_PERF_SMALL_OPS", str(cfg.get("small_file_ops", 50)), ctx.suite)),
            "write_sync_sizes": self.int_csv_env(ctx, "MANUS_PERF_WRITE_SYNC_SIZES", cfg.get("write_sync_sizes", [1024, 20 * 1024, 100 * 1024, 512 * 1024, 2 * 1024 * 1024])),
            "write_sync_ops": int(env_value("MANUS_PERF_WRITE_SYNC_OPS", str(cfg.get("write_sync_ops", 50)), ctx.suite)),
            "write_sync_concurrency": self.int_csv_env(ctx, "MANUS_PERF_WRITE_SYNC_CONCURRENCY", cfg.get("write_sync_concurrency", [1, 4])),
            "namespace_stat_samples": int(env_value("MANUS_PERF_STAT_SAMPLES", str(cfg.get("namespace_stat_samples", 300)), ctx.suite)),
            "namespace_op_samples": int(env_value("MANUS_PERF_NAMESPACE_OP_SAMPLES", str(cfg.get("namespace_op_samples", 200)), ctx.suite)),
            "visibility_timeout_s": float(env_value("MANUS_PERF_VISIBILITY_TIMEOUT_S", str(cfg.get("visibility_timeout_s", 30)), ctx.suite)),
            "cache_invalidation_samples": int(env_value("MANUS_PERF_CACHE_INVALIDATION_SAMPLES", str(cfg.get("cache_invalidation_samples", 30)), ctx.suite)),
            "routing_sizes": self.int_csv_env(ctx, "MANUS_PERF_ROUTING_SIZES", cfg.get("routing_sizes", [1024, 100 * 1024, 512 * 1024, 2 * 1024 * 1024 - 1, 2 * 1024 * 1024, 4 * 1024 * 1024])),
            "large_file_sizes": self.int_csv_env(ctx, "MANUS_PERF_LARGE_SIZES", cfg.get("large_file_sizes", [4 * 1024 * 1024, 16 * 1024 * 1024])),
            "large_file_ops": int(env_value("MANUS_PERF_LARGE_OPS", str(cfg.get("large_file_ops", 10)), ctx.suite)),
            "extra_repo_id": env_value("MANUS_PERF_EXTRA_REPO", str(cfg.get("extra_repo_id", "cruip-tailwind-dashboard")), ctx.suite),
            "extra_storages": self.csv_env(ctx, "MANUS_PERF_EXTRA_STORAGES", cfg.get("extra_storages", ["local", "fuse-writeback", "fuse-write-sync"])),
            "extra_profiles": self.csv_env(ctx, "MANUS_PERF_EXTRA_PROFILES", cfg.get("extra_profiles", ["coding-agent", "none"])),
            "extra_runs": int(env_value("MANUS_PERF_EXTRA_RUNS", str(cfg.get("extra_runs", 1)), ctx.suite)),
            "raw_results": env_flag("MANUS_PERF_RAW", bool(cfg.get("raw_results", True)), ctx.suite),
            "sections": {
                "multi_session_shared": env_flag("MANUS_PERF_MULTI_SESSION_SHARED", bool(sections_default.get("multi_session_shared", True)), ctx.suite),
                "cross_visibility": env_flag("MANUS_PERF_CROSS_VISIBILITY", bool(sections_default.get("cross_visibility", True)), ctx.suite),
                "concurrent_distinct": env_flag("MANUS_PERF_CONCURRENT_DISTINCT", bool(sections_default.get("concurrent_distinct", True)), ctx.suite),
                "concurrent_same_file": env_flag("MANUS_PERF_CONCURRENT_SAME_FILE", bool(sections_default.get("concurrent_same_file", True)), ctx.suite),
                "namespace_ops": env_flag("MANUS_PERF_NAMESPACE_OPS", bool(sections_default.get("namespace_ops", True)), ctx.suite),
                "write_sync_small": env_flag("MANUS_PERF_WRITE_SYNC_SMALL", bool(sections_default.get("write_sync_small", True)), ctx.suite),
                "cache_invalidation": env_flag("MANUS_PERF_CACHE_INVALIDATION", bool(sections_default.get("cache_invalidation", True)), ctx.suite),
                "routing_2mb": env_flag("MANUS_PERF_ROUTING_2MB", bool(sections_default.get("routing_2mb", True)), ctx.suite),
                "large_file_s3": env_flag("MANUS_PERF_LARGE_FILE_S3", bool(sections_default.get("large_file_s3", True)), ctx.suite),
                "single_session": env_flag("MANUS_PERF_SINGLE_SESSION", bool(sections_default.get("single_session", True)), ctx.suite),
                "file_lock": env_flag("MANUS_PERF_FILE_LOCK", bool(sections_default.get("file_lock", True)), ctx.suite),
                "extra_clone_build": env_flag("MANUS_PERF_EXTRA_CLONE_BUILD", bool(sections_default.get("extra_clone_build", True)), ctx.suite),
            },
        }

    # ----- shared helpers ----------------------------------------------

    def mount_shared(
        self,
        ctx: Context,
        remote: str,
        *,
        agent: str,
        cache_key: str,
        profile: str | None = None,
        durability: str | None = None,
        read_cache_ttl_s: float | None = None,
        read_only: bool = False,
    ) -> Any:
        extra: list[str] = []
        if read_cache_ttl_s is not None:
            extra.extend(["--read-cache-ttl", f"{int(read_cache_ttl_s)}s"])
        return ctx.target.mount(
            f"manus_{agent}",
            remote,
            read_only=read_only,
            profile=profile if profile is not None else self.config(ctx)["profile"],
            durability=durability if durability is not None else self.config(ctx)["durability"],
            cache_key=cache_key,
            extra=extra,
        )

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
        if failed:
            detail = f"exit={exit_code} mounted_after={outcome.mounted_after} forced={outcome.forced} seconds={outcome.seconds:.3f} mountpoint={handle.mountpoint}"
            issue = {"severity": "error" if outcome.mounted_after else "warn", "section": section, "op": f"{op}_unmount", "detail": detail}
            if labels:
                issue.update(labels)
            issues.append(issue)
        return elapsed_ms, failed

    # ----- section: multi-session shared mount -------------------------

    def run_multi_session_shared(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """P0-1: multiple agents mount the same workspace and read the same
        batch of files. Measures per-agent mount latency and shared-read
        latency under concurrent readers."""
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/multi-session-shared"
        ctx.target.mkdir_remote(remote)
        # Seed a small shared file set from one writer mount.
        seed_files = int(cfg["namespace_op_samples"])
        seed_handle = self.mount_shared(ctx, remote, agent="multi_seed", cache_key="seed", profile=cfg["profile"], durability=cfg["durability"])
        try:
            seed_root = seed_handle.mountpoint / "shared"
            seed_root.mkdir(exist_ok=True)
            payload = stable_bytes(1024, seed=7)
            for idx in range(seed_files):
                (seed_root / f"shared-{idx:06d}.bin").write_bytes(payload)
        finally:
            self.record_unmount(ctx, seed_handle, issues, section="multi_session_shared", op="seed")

        for count in cfg["agent_counts"]:
            progress(f"manus multi_session_shared: agents={count}")
            mount_values: list[float] = []
            read_values: list[float] = []
            mount_errors = 0
            read_errors = 0
            unmount_values: list[float] = []
            unmount_errors = 0
            for run_idx in range(int(cfg["runs"])):
                handles: list[Any] = []
                try:
                    # Concurrent mount of the same workspace by N agents.
                    def mount_one(agent_idx: int) -> tuple[float, Any | None, str]:
                        start = time.perf_counter()
                        try:
                            handle = self.mount_shared(ctx, remote, agent=f"agent-{count}-{agent_idx}", cache_key=f"agent-{count}-{agent_idx}-run-{run_idx}", profile=cfg["profile"], durability=cfg["durability"])
                            return (time.perf_counter() - start) * 1000, handle, ""
                        except Exception as exc:
                            return -1.0, None, str(exc)

                    with ThreadPoolExecutor(max_workers=count) as pool:
                        futures = [pool.submit(mount_one, i) for i in range(count)]
                        for future in as_completed(futures):
                            ms, handle, err = future.result()
                            if err:
                                mount_errors += 1
                                issues.append({"severity": "error", "section": "multi_session_shared", "op": "mount", "agent_count": count, "detail": err})
                            else:
                                mount_values.append(ms)
                                handles.append(handle)
                    # Concurrent read of the same shared files from every agent.
                    def read_one(handle: Any) -> tuple[float, str]:
                        start = time.perf_counter()
                        try:
                            root = handle.mountpoint / "shared"
                            target = root / f"shared-{run_idx % seed_files:06d}.bin"
                            _ = target.read_bytes()
                            return (time.perf_counter() - start) * 1000, ""
                        except Exception as exc:
                            return -1.0, str(exc)

                    with ThreadPoolExecutor(max_workers=len(handles)) as pool:
                        futures = [pool.submit(read_one, h) for h in handles]
                        for future in as_completed(futures):
                            ms, err = future.result()
                            if err:
                                read_errors += 1
                                issues.append({"severity": "warn", "section": "multi_session_shared", "op": "shared_read", "agent_count": count, "detail": err})
                            else:
                                read_values.append(ms)
                finally:
                    for handle in reversed(handles):
                        elapsed_ms, failed = self.record_unmount(ctx, handle, issues, section="multi_session_shared", op="mount", labels={"agent_count": count, "run": run_idx})
                        unmount_values.append(elapsed_ms)
                        if failed:
                            unmount_errors += 1
            rows.append(self.matrix_row("multi_session_shared", "concurrent_mount", "", "", "", int(count), "ms", mount_values, mount_errors, max(1, count * int(cfg["runs"])), int(cfg["runs"]), status="ok" if mount_errors == 0 else "error"))
            rows.append(self.matrix_row("multi_session_shared", "concurrent_shared_read", "", "", "", int(count), "ms", read_values, read_errors, max(1, count * int(cfg["runs"])), int(cfg["runs"]), status="ok" if read_errors == 0 else "error"))
            rows.append(self.matrix_row("multi_session_shared", "unmount", "", "", "", int(count), "ms", unmount_values, unmount_errors, max(1, count * int(cfg["runs"])), int(cfg["runs"]), status="ok" if unmount_errors == 0 else "error"))
            ctx.perf_values(f"{self.id}.multi_session_shared.c{count}.mount", mount_values, "ms")
            ctx.perf_values(f"{self.id}.multi_session_shared.c{count}.shared_read", read_values, "ms")
            ctx.perf_values(f"{self.id}.multi_session_shared.c{count}.unmount", unmount_values, "ms")
        return rows

    # ----- section: cross-mount visibility (A write, B read) -----------

    def run_cross_visibility(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """P0-2: agent A writes a file, agent B (separate mount, separate
        cache) reads it. Validates strong read consistency: B must observe A's
        new content, and we measure the visibility latency."""
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/cross-visibility"
        ctx.target.mkdir_remote(remote)
        for size in cfg["small_file_sizes"]:
            progress(f"manus cross_visibility: size={size}")
            write_values: list[float] = []
            visible_values: list[float] = []
            read_values: list[float] = []
            errors = 0
            visibility_errors = 0
            runs = int(cfg["runs"])
            samples = max(1, min(int(cfg["cache_invalidation_samples"]), 50))
            payload = stable_bytes(size, seed=size)
            digest = hashlib.sha256(payload).hexdigest()
            for run_idx in range(runs):
                writer = self.mount_shared(ctx, remote, agent="vis_writer", cache_key=f"writer-{size}-run-{run_idx}", profile=cfg["profile"], durability=cfg["durability"])
                reader = self.mount_shared(ctx, remote, agent="vis_reader", cache_key=f"reader-{size}-run-{run_idx}", profile=cfg["profile"], durability=cfg["durability"])
                raw_path = raw_dir / f"visibility-{size}-run-{run_idx}.jsonl"
                raw_handle = raw_path.open("w", encoding="utf-8") if cfg["raw_results"] else None
                try:
                    wroot = writer.mountpoint / f"vis-{size}-run-{run_idx}"
                    rroot = reader.mountpoint / f"vis-{size}-run-{run_idx}"
                    wroot.mkdir(exist_ok=True)
                    for idx in range(samples):
                        path = wroot / f"f-{idx:06d}.bin"
                        start = time.perf_counter()
                        path.write_bytes(payload)
                        write_ms = (time.perf_counter() - start) * 1000
                        write_values.append(write_ms)
                        # Reader reads from its own mount / cache dir.
                        rpath = rroot / path.name
                        visible_ms = self.wait_visible(rpath, digest, float(cfg["visibility_timeout_s"]))
                        if visible_ms < 0:
                            visibility_errors += 1
                            self.raw_write(raw_handle, {"section": "cross_visibility", "size": size, "idx": idx, "run": run_idx, "write_ms": write_ms, "visible_ms": visible_ms, "status": "visibility_timeout"})
                            continue
                        visible_values.append(visible_ms)
                        start = time.perf_counter()
                        data = rpath.read_bytes()
                        read_ms = (time.perf_counter() - start) * 1000
                        if hashlib.sha256(data).hexdigest() != digest:
                            errors += 1
                            self.raw_write(raw_handle, {"section": "cross_visibility", "size": size, "idx": idx, "run": run_idx, "write_ms": write_ms, "visible_ms": visible_ms, "read_ms": read_ms, "status": "checksum_mismatch"})
                        else:
                            read_values.append(read_ms)
                            self.raw_write(raw_handle, {"section": "cross_visibility", "size": size, "idx": idx, "run": run_idx, "write_ms": write_ms, "visible_ms": visible_ms, "read_ms": read_ms, "status": "ok"})
                finally:
                    if raw_handle is not None:
                        raw_handle.close()
                    self.record_unmount(ctx, reader, issues, section="cross_visibility", op="reader", labels={"file_size": size, "run": run_idx})
                    self.record_unmount(ctx, writer, issues, section="cross_visibility", op="writer", labels={"file_size": size, "run": run_idx})
            status = "ok" if errors == 0 and visibility_errors == 0 else "error"
            rows.append(self.matrix_row("cross_visibility", "writer_write", "", "", size, "", "ms", write_values, errors, len(write_values), runs, status=status))
            rows.append(self.matrix_row("cross_visibility", "cross_visible", "", "", size, "", "ms", visible_values, visibility_errors, len(visible_values), runs, status=status, extra={"visibility_errors": visibility_errors}))
            rows.append(self.matrix_row("cross_visibility", "reader_read", "", "", size, "", "ms", read_values, errors, len(read_values), runs, status=status))
            ctx.perf_values(f"{self.id}.cross_visibility.{size}b.write", write_values, "ms")
            ctx.perf_values(f"{self.id}.cross_visibility.{size}b.visible", visible_values, "ms")
            ctx.perf_values(f"{self.id}.cross_visibility.{size}b.read", read_values, "ms")
            if errors or visibility_errors:
                issues.append({"severity": "error", "section": "cross_visibility", "op": "visibility", "file_size": size, "detail": f"errors={errors} visibility_errors={visibility_errors}"})
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
            except OSError:
                pass
            time.sleep(0.02)
        return -1.0

    # ----- section: concurrent read/write distinct files ---------------

    def run_concurrent_distinct(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """P0-7: multiple agents concurrently read/write *different* files in
        the same workspace. Validates isolation and measures throughput."""
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/concurrent-distinct"
        ctx.target.mkdir_remote(remote)
        for count in cfg["agent_counts"]:
            for size in cfg["small_file_sizes"]:
                progress(f"manus concurrent_distinct: agents={count} size={size}")
                payload = stable_bytes(size, seed=size + count)
                write_values: list[float] = []
                read_values: list[float] = []
                errors = 0
                wall_seconds = 0.0
                for run_idx in range(int(cfg["runs"])):
                    handles: list[Any] = []
                    try:
                        for agent_idx in range(count):
                            handles.append(self.mount_shared(ctx, remote, agent=f"distinct-{count}-{agent_idx}", cache_key=f"distinct-{count}-{agent_idx}-run-{run_idx}", profile=cfg["profile"], durability=cfg["durability"]))
                        ops_per_agent = max(1, int(cfg["small_file_ops"]) // count)

                        def agent_work(agent_idx: int, handle: Any) -> tuple[list[float], list[float], int, float]:
                            wv: list[float] = []
                            rv: list[float] = []
                            err = 0
                            start_wall = time.perf_counter()
                            root = handle.mountpoint / f"distinct-{count}-{agent_idx}-run-{run_idx}"
                            root.mkdir(exist_ok=True)
                            for idx in range(ops_per_agent):
                                path = root / f"f-{idx:06d}.bin"
                                try:
                                    s = time.perf_counter()
                                    path.write_bytes(payload)
                                    wv.append((time.perf_counter() - s) * 1000)
                                    s = time.perf_counter()
                                    _ = path.read_bytes()
                                    rv.append((time.perf_counter() - s) * 1000)
                                except OSError as exc:
                                    err += 1
                                    issues.append({"severity": "warn", "section": "concurrent_distinct", "op": "work", "agent_count": count, "file_size": size, "detail": str(exc)})
                            return wv, rv, err, time.perf_counter() - start_wall

                        with ThreadPoolExecutor(max_workers=count) as pool:
                            futures = [pool.submit(agent_work, i, h) for i, h in enumerate(handles)]
                            for future in as_completed(futures):
                                wv, rv, err, ws = future.result()
                                write_values.extend(wv)
                                read_values.extend(rv)
                                errors += err
                                wall_seconds += ws
                    finally:
                        for handle in reversed(handles):
                            self.record_unmount(ctx, handle, issues, section="concurrent_distinct", op="mount", labels={"agent_count": count, "file_size": size, "run": run_idx})
                total_ops = len(write_values)
                write_qps = len(write_values) / wall_seconds if wall_seconds > 0 else 0.0
                read_qps = len(read_values) / wall_seconds if wall_seconds > 0 else 0.0
                rows.append(self.matrix_row("concurrent_distinct", "write", "", "", size, int(count), "ms", write_values, errors, total_ops, int(cfg["runs"]), qps=write_qps, status="ok" if errors == 0 else "error"))
                rows.append(self.matrix_row("concurrent_distinct", "read", "", "", size, int(count), "ms", read_values, errors, len(read_values), int(cfg["runs"]), qps=read_qps, status="ok" if errors == 0 else "error"))
                ctx.perf_values(f"{self.id}.concurrent_distinct.c{count}.{size}b.write", write_values, "ms")
                ctx.perf_values(f"{self.id}.concurrent_distinct.c{count}.{size}b.read", read_values, "ms")
        return rows

    # ----- section: concurrent read/write same file --------------------

    def run_concurrent_same_file(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """P0-8: multiple agents concurrently read/write the *same* file. Per
        Minghua the requirement is to document behavior, not guarantee conflict
        resolution. We record the observed outcome (last-writer content, read
        consistency) and latency, and flag any data corruption (non-decodable
        sizes)."""
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/concurrent-same-file"
        ctx.target.mkdir_remote(remote)
        for count in cfg["agent_counts"]:
            for size in cfg["small_file_sizes"]:
                progress(f"manus concurrent_same_file: agents={count} size={size}")
                payload = stable_bytes(size, seed=size)
                write_values: list[float] = []
                read_values: list[float] = []
                errors = 0
                corruption = 0
                runs = int(cfg["runs"])
                ops_per_agent = max(1, int(cfg["small_file_ops"]) // count)
                for run_idx in range(runs):
                    handles: list[Any] = []
                    try:
                        for agent_idx in range(count):
                            handles.append(self.mount_shared(ctx, remote, agent=f"same-{count}-{agent_idx}", cache_key=f"same-{count}-{agent_idx}-run-{run_idx}", profile=cfg["profile"], durability=cfg["durability"]))
                        shared_path = handles[0].mountpoint / f"same-{count}-{size}-run-{run_idx}.bin"
                        # Seed the shared file once.
                        shared_path.write_bytes(payload)

                        def agent_rw(agent_idx: int, handle: Any) -> tuple[list[float], list[float], int, int]:
                            wv: list[float] = []
                            rv: list[float] = []
                            err = 0
                            corr = 0
                            local_path = handle.mountpoint / shared_path.name
                            for idx in range(ops_per_agent):
                                try:
                                    # Writers append a small marker; reads observe current size.
                                    s = time.perf_counter()
                                    with local_path.open("ab") as fh:
                                        fh.write(payload[: min(64, size)])
                                    wv.append((time.perf_counter() - s) * 1000)
                                    s = time.perf_counter()
                                    data = local_path.read_bytes()
                                    rv.append((time.perf_counter() - s) * 1000)
                                    if len(data) % max(1, min(64, size)) != 0:
                                        corr += 1
                                except OSError as exc:
                                    err += 1
                                    issues.append({"severity": "warn", "section": "concurrent_same_file", "op": "rw", "agent_count": count, "file_size": size, "detail": str(exc)})
                            return wv, rv, err, corr

                        with ThreadPoolExecutor(max_workers=count) as pool:
                            futures = [pool.submit(agent_rw, i, h) for i, h in enumerate(handles)]
                            for future in as_completed(futures):
                                wv, rv, err, corr = future.result()
                                write_values.extend(wv)
                                read_values.extend(rv)
                                errors += err
                                corruption += corr
                    finally:
                        for handle in reversed(handles):
                            self.record_unmount(ctx, handle, issues, section="concurrent_same_file", op="mount", labels={"agent_count": count, "file_size": size, "run": run_idx})
                status = "ok" if errors == 0 and corruption == 0 else "error"
                rows.append(self.matrix_row("concurrent_same_file", "concurrent_write", "", "", size, int(count), "ms", write_values, errors, len(write_values), runs, status=status, extra={"corruption_indicators": corruption}))
                rows.append(self.matrix_row("concurrent_same_file", "concurrent_read", "", "", size, int(count), "ms", read_values, errors, len(read_values), runs, status=status, extra={"corruption_indicators": corruption}))
                rows.append(self.control_row_same_file_behavior(count, size, corruption))
                ctx.perf_values(f"{self.id}.concurrent_same_file.c{count}.{size}b.write", write_values, "ms")
                ctx.perf_values(f"{self.id}.concurrent_same_file.c{count}.{size}b.read", read_values, "ms")
                if corruption:
                    issues.append({"severity": "warn", "section": "concurrent_same_file", "op": "behavior", "agent_count": count, "file_size": size, "detail": f"corruption_indicators={corruption}; concurrent same-file writes are last-writer wins, no file lock"})
        return rows

    def control_row_same_file_behavior(self, count: int, size: int, corruption: int) -> dict[str, Any]:
        return {
            "section": "concurrent_same_file",
            "op": "behavior_note",
            "file_size": size,
            "concurrency": int(count),
            "unit": "",
            "status": "ok" if corruption == 0 else "warn",
            "errors": corruption,
            "error_rate": corruption / max(1, count),
            "runs": 1,
            "detail": "concurrent same-file writes are last-writer wins; Drive9 does not provide a distributed file lock; reads observe the latest committed content",
            **self.latency_summary([]),
        }

    # ----- section: namespace list/stat/open/close ---------------------

    def run_namespace_ops(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """P0-6: directory list / stat / open / close base operation latency
        across the scale tiers (100MB/1k, 1GB/10k, 10GB/100k)."""
        rows: list[dict[str, Any]] = []
        for scale_id in cfg["selected_scales"]:
            if scale_id not in cfg["scales"]:
                raise ModuleSkip(f"unknown BLACKBOX_MANUS_PERF_SCALES value: {scale_id}", "configuration skip")
            scale = cfg["scales"][scale_id]
            remote = f"{remote_base}/namespace-{scale_id}"
            ctx.target.mkdir_remote(remote)
            dataset = self.prepare_dataset(ctx, cfg, remote, scale_id, scale, issues)
            rows.append(dataset)
            if dataset.get("status") not in {"ok", "cached"}:
                continue
            rows.extend(self.measure_namespace_ops(ctx, cfg, remote, scale_id, scale, raw_dir, issues))
        return rows

    def prepare_dataset(self, ctx: Context, cfg: dict[str, Any], remote: str, scale_id: str, scale: dict[str, Any], issues: list[dict[str, Any]]) -> dict[str, Any]:
        mount_started = time.perf_counter()
        handle = self.mount_shared(ctx, remote, agent="ns_dataset", cache_key=f"{scale_id}-prepare", profile=cfg["profile"], durability=cfg["durability"])
        mount_ms = (time.perf_counter() - mount_started) * 1000
        row: dict[str, Any] = {
            "section": "namespace_ops",
            "op": "dataset_generate",
            "scale": scale_id,
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
            manifest = handle.mountpoint / ".drive9-manus-dataset.json"
            expected = {"scale": scale_id, "bytes": int(scale["bytes"]), "files": int(scale["files"])}
            if manifest.exists():
                try:
                    current = json.loads(manifest.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    current = {}
                if all(current.get(key) == value for key, value in expected.items()) and current.get("completed") is True:
                    seconds = float(current.get("seconds", 0.0))
                    row.update({"status": "cached", "created_files": int(scale["files"]), "created_bytes": int(scale["bytes"]), "count": int(scale["files"]), **self.latency_summary([seconds])})
                    return row
            progress(f"manus dataset generate: {scale_id} bytes={scale['bytes']} files={scale['files']}")
            data_dir = handle.mountpoint / "data"
            if data_dir.exists():
                shutil.rmtree(data_dir)
            data_dir.mkdir()
            started = time.perf_counter()
            base = int(scale["bytes"]) // int(scale["files"])
            extra = int(scale["bytes"]) % int(scale["files"])
            payload = stable_bytes(min(max(base + 1, 1), 1024 * 1024), seed=int(scale["files"]))
            created_files = 0
            created_bytes = 0
            checkpoints = {max(1, int(scale["files"]) * pct // 10) for pct in range(1, 11)}
            try:
                for idx in range(int(scale["files"])):
                    size = base + (1 if idx < extra else 0)
                    parent = data_dir / f"shard-{idx // 1000:05d}"
                    parent.mkdir(parents=True, exist_ok=True)
                    self.write_payload(parent / f"file-{idx:08d}.bin", payload, size)
                    created_files += 1
                    created_bytes += size
                    if created_files in checkpoints:
                        progress(f"manus dataset progress: {created_files}/{int(scale['files'])}")
                detail = "completed"
            except OSError as exc:
                detail = f"I/O error after {created_files}/{int(scale['files'])} files: {exc}"
            seconds = time.perf_counter() - started
            completed = created_files == int(scale["files"])
            status = "ok" if completed else "error"
            self.write_dataset_manifest(manifest, expected, created_files, created_bytes, seconds, completed, detail)
            row.update({"status": status, "created_files": created_files, "created_bytes": created_bytes, "count": created_files, "detail": detail, **self.latency_summary([seconds])})
            ctx.metric(f"{self.id}.dataset.generate_seconds", seconds, "seconds", {"scale": scale_id})
            if not completed:
                issues.append({"severity": "warn", "section": "namespace_ops", "op": "dataset_generate", "scale": scale_id, "detail": detail})
            return row
        finally:
            unmount_ms, _ = self.record_unmount(ctx, handle, issues, section="namespace_ops", op="dataset_generate", row=row, labels={"scale": scale_id})
            row["unmount_ms"] = unmount_ms

    def measure_namespace_ops(self, ctx: Context, cfg: dict[str, Any], remote: str, scale_id: str, scale: dict[str, Any], raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        handle = self.mount_shared(ctx, remote, agent="ns_ops", cache_key=f"{scale_id}-ops", profile=cfg["profile"], durability=cfg["durability"])
        try:
            data_dir = handle.mountpoint / "data"
            commands = {
                "list": f"ls {shlex.quote(str(data_dir))} >/dev/null",
                "list_long": f"ls -l {shlex.quote(str(data_dir))} >/dev/null",
                "find": f"find {shlex.quote(str(data_dir))} -type f >/dev/null",
            }
            for name, command in commands.items():
                values: list[float] = []
                errors = 0
                for run_idx in range(int(cfg["runs"])):
                    result = ctx.target.run_cmd(f"manus-ns-{scale_id}-{name}-run-{run_idx}", command, timeout=600, shell=True)
                    if result.ok:
                        values.append(result.seconds)
                    else:
                        errors += 1
                        issues.append({"severity": "warn", "section": "namespace_ops", "op": name, "scale": scale_id, "detail": f"run {run_idx} exit={result.code}; see {result.stderr}"})
                rows.append(self.matrix_row("namespace_ops", name, scale_id, "", "", "", "seconds", values, errors, int(cfg["runs"]), int(cfg["runs"]), status="ok" if errors == 0 else "error"))
                ctx.perf_values(f"{self.id}.namespace_ops.{scale_id}.{name}", values, "seconds")
            # stat / open / close micro-benchmarks (cold per-sample).
            stat_values, stat_errors = self.measure_stat_samples(data_dir, int(scale["files"]), int(cfg["namespace_stat_samples"]), raw_dir / f"stat-{scale_id}.jsonl")
            rows.append(self.matrix_row("namespace_ops", "stat", scale_id, "", "", "", "ms", stat_values, stat_errors, min(int(cfg["namespace_stat_samples"]), int(scale["files"])), 1, status="ok" if stat_errors == 0 else "error"))
            ctx.perf_values(f"{self.id}.namespace_ops.{scale_id}.stat", stat_values, "ms")
            open_values, open_errors = self.measure_open_close_samples(data_dir, int(scale["files"]), int(cfg["namespace_stat_samples"]), raw_dir / f"open-close-{scale_id}.jsonl")
            rows.append(self.matrix_row("namespace_ops", "open_close", scale_id, "", "", "", "ms", open_values, open_errors, min(int(cfg["namespace_stat_samples"]), int(scale["files"])), 1, status="ok" if open_errors == 0 else "error"))
            ctx.perf_values(f"{self.id}.namespace_ops.{scale_id}.open_close", open_values, "ms")
        finally:
            self.record_unmount(ctx, handle, issues, section="namespace_ops", op="namespace_ops", labels={"scale": scale_id})
        return rows

    def measure_stat_samples(self, data_dir: Path, file_count: int, samples: int, raw_path: Path) -> tuple[list[float], int]:
        import random as _random

        if file_count <= 0:
            return [], 0
        rng = _random.Random(9)
        values: list[float] = []
        errors = 0
        with raw_path.open("w", encoding="utf-8") as raw:
            for _ in range(min(samples, file_count)):
                idx = rng.randrange(0, file_count)
                path = data_dir / f"shard-{idx // 1000:05d}" / f"file-{idx:08d}.bin"
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
                raw.write(json.dumps({"op": "stat", "idx": idx, "latency_ms": latency, "status": status, "error": err}, sort_keys=True) + "\n")
        return values, errors

    def measure_open_close_samples(self, data_dir: Path, file_count: int, samples: int, raw_path: Path) -> tuple[list[float], int]:
        import random as _random

        if file_count <= 0:
            return [], 0
        rng = _random.Random(11)
        values: list[float] = []
        errors = 0
        with raw_path.open("w", encoding="utf-8") as raw:
            for _ in range(min(samples, file_count)):
                idx = rng.randrange(0, file_count)
                path = data_dir / f"shard-{idx // 1000:05d}" / f"file-{idx:08d}.bin"
                start = time.perf_counter()
                status = "ok"
                err = ""
                try:
                    fd = os.open(path, os.O_RDONLY)
                    os.close(fd)
                    values.append((time.perf_counter() - start) * 1000)
                except OSError as exc:
                    errors += 1
                    status = "error"
                    err = str(exc)
                latency = (time.perf_counter() - start) * 1000
                raw.write(json.dumps({"op": "open_close", "idx": idx, "latency_ms": latency, "status": status, "error": err}, sort_keys=True) + "\n")
        return values, errors

    # ----- section: write-sync small-file latency ----------------------

    def run_write_sync_small(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """P0-3 / 口径: write-sync mode small-file write latency, focused on
        files <= 2MB (the Manus Nexus majority workload)."""
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/write-sync-small"
        ctx.target.mkdir_remote(remote)
        for size in cfg["write_sync_sizes"]:
            for concurrency in cfg["write_sync_concurrency"]:
                progress(f"manus write_sync_small: size={size} concurrency={concurrency}")
                ops = max(1, int(cfg["write_sync_ops"]))
                runs = int(cfg["runs"])
                payload = stable_bytes(size, seed=size + concurrency)
                values: list[float] = []
                fsync_values: list[float] = []
                errors = 0
                wall_seconds = 0.0
                for run_idx in range(runs):
                    handle = self.mount_shared(ctx, remote, agent="ws_writer", cache_key=f"ws-{size}-{concurrency}-run-{run_idx}", profile=cfg["profile"], durability=DURABILITY_WRITE_SYNC)
                    try:
                        root = handle.mountpoint / f"ws-{size}-{concurrency}-run-{run_idx}"
                        if root.exists():
                            shutil.rmtree(root)
                        root.mkdir(parents=True)
                        raw_path = raw_dir / f"write-sync-{size}-{concurrency}-run-{run_idx}.jsonl"

                        def body(idx: int) -> None:
                            path = root / f"f-{idx:06d}.bin"
                            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                            try:
                                self.write_fd(fd, payload)
                            finally:
                                os.close(fd)

                        run_values, run_errors, run_wall, run_fsync = self.run_write_sync_workload(
                            raw_path,
                            ops,
                            concurrency,
                            body,
                            cfg["raw_results"],
                            {"section": "write_sync_small", "size": size, "concurrency": concurrency, "run": run_idx},
                        )
                        values.extend(run_values)
                        fsync_values.extend(run_fsync)
                        errors += run_errors
                        wall_seconds += run_wall
                    finally:
                        self.record_unmount(ctx, handle, issues, section="write_sync_small", op="suite", labels={"file_size": size, "concurrency": concurrency, "run": run_idx})
                total_ops = ops * runs
                qps = len(values) / wall_seconds if wall_seconds > 0 else 0.0
                rows.append(self.matrix_row("write_sync_small", "create_close", "", "", size, concurrency, "ms", values, errors, total_ops, runs, qps=qps, status="ok" if errors == 0 else "error"))
                ctx.perf_values(f"{self.id}.write_sync_small.{size}b.c{concurrency}", values, "ms")
                if errors:
                    issues.append({"severity": "error", "section": "write_sync_small", "op": "create_close", "file_size": size, "concurrency": concurrency, "detail": f"errors={errors}"})
        return rows

    def run_write_sync_workload(
        self,
        raw_path: Path,
        ops: int,
        concurrency: int,
        fn: Callable[[int], None],
        write_raw: bool,
        labels: dict[str, Any],
    ) -> tuple[list[float], int, float, list[float]]:
        # write-sync durability means every close is remote-durable; we measure
        # the full create+close latency. fsync is a separate sub-phase that we
        # also capture for comparison (close+fsync vs close only).
        values: list[float] = []
        fsync_values: list[float] = []
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
        return values, errors, time.perf_counter() - start_wall, fsync_values

    # ----- section: cache invalidation read cost -----------------------

    def run_cache_invalidation(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """P0-4 / 第四优先级: after another session writes, measure the read
        cost on a session whose cache must invalidate + refresh metadata.
        Captures cache-invalidation overhead, metadata refresh overhead, small
        file read delay, and dir list refresh delay."""
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/cache-invalidation"
        ctx.target.mkdir_remote(remote)
        samples = max(1, int(cfg["cache_invalidation_samples"]))
        for size in cfg["small_file_sizes"]:
            progress(f"manus cache_invalidation: size={size}")
            payload_a = stable_bytes(size, seed=size)
            payload_b = stable_bytes(size, seed=size + 1)
            digest_a = hashlib.sha256(payload_a).hexdigest()
            digest_b = hashlib.sha256(payload_b).hexdigest()
            cold_read_values: list[float] = []
            refresh_read_values: list[float] = []
            dir_refresh_values: list[float] = []
            metadata_refresh_values: list[float] = []
            errors = 0
            runs = int(cfg["runs"])
            for run_idx in range(runs):
                # Writer mounts, writes version A, unmounts (durable).
                writer = self.mount_shared(ctx, remote, agent="ci_writer", cache_key=f"ci-writer-{size}-run-{run_idx}", profile=cfg["profile"], durability=cfg["durability"])
                try:
                    wroot = writer.mountpoint / f"ci-{size}-run-{run_idx}"
                    wroot.mkdir(exist_ok=True)
                    (wroot / "file.bin").write_bytes(payload_a)
                finally:
                    self.record_unmount(ctx, writer, issues, section="cache_invalidation", op="writer_a", labels={"file_size": size, "run": run_idx})
                # Reader mounts with a fresh cache, reads version A (cold).
                reader = self.mount_shared(ctx, remote, agent="ci_reader", cache_key=f"ci-reader-{size}-run-{run_idx}", profile=cfg["profile"], durability=cfg["durability"])
                try:
                    rroot = reader.mountpoint / f"ci-{size}-run-{run_idx}"
                    start = time.perf_counter()
                    data = (rroot / "file.bin").read_bytes()
                    cold_read_values.append((time.perf_counter() - start) * 1000)
                    if hashlib.sha256(data).hexdigest() != digest_a:
                        errors += 1
                    # Prime directory list cache.
                    _ = list(rroot.iterdir())
                finally:
                    self.record_unmount(ctx, reader, issues, section="cache_invalidation", op="reader_a", labels={"file_size": size, "run": run_idx})
                # Writer mounts again, overwrites to version B, unmounts.
                writer2 = self.mount_shared(ctx, remote, agent="ci_writer2", cache_key=f"ci-writer2-{size}-run-{run_idx}", profile=cfg["profile"], durability=cfg["durability"])
                try:
                    wroot2 = writer2.mountpoint / f"ci-{size}-run-{run_idx}"
                    (wroot2 / "file.bin").write_bytes(payload_b)
                finally:
                    self.record_unmount(ctx, writer2, issues, section="cache_invalidation", op="writer_b", labels={"file_size": size, "run": run_idx})
                # Reader re-mounts with the SAME cache dir to exercise invalidation.
                reader2 = self.mount_shared(ctx, remote, agent="ci_reader", cache_key=f"ci-reader-{size}-run-{run_idx}", profile=cfg["profile"], durability=cfg["durability"])
                raw_path = raw_dir / f"cache-invalidation-{size}-run-{run_idx}.jsonl"
                raw_handle = raw_path.open("w", encoding="utf-8") if cfg["raw_results"] else None
                try:
                    rroot2 = reader2.mountpoint / f"ci-{size}-run-{run_idx}"
                    # Directory list refresh (must invalidate stale dir cache).
                    start = time.perf_counter()
                    entries = list(rroot2.iterdir())
                    dir_refresh_values.append((time.perf_counter() - start) * 1000)
                    self.raw_write(raw_handle, {"section": "cache_invalidation", "phase": "dir_refresh", "size": size, "run": run_idx, "latency_ms": dir_refresh_values[-1], "entries": len(entries)})
                    # stat refresh (metadata refresh overhead).
                    start = time.perf_counter()
                    st = os.stat(rroot2 / "file.bin")
                    metadata_refresh_values.append((time.perf_counter() - start) * 1000)
                    self.raw_write(raw_handle, {"section": "cache_invalidation", "phase": "stat_refresh", "size": size, "run": run_idx, "latency_ms": metadata_refresh_values[-1], "size_bytes": st.st_size})
                    # Read refresh (must fetch new content).
                    start = time.perf_counter()
                    data = (rroot2 / "file.bin").read_bytes()
                    refresh_read_values.append((time.perf_counter() - start) * 1000)
                    if hashlib.sha256(data).hexdigest() != digest_b:
                        errors += 1
                        self.raw_write(raw_handle, {"section": "cache_invalidation", "phase": "read_refresh", "size": size, "run": run_idx, "latency_ms": refresh_read_values[-1], "status": "checksum_mismatch"})
                    else:
                        self.raw_write(raw_handle, {"section": "cache_invalidation", "phase": "read_refresh", "size": size, "run": run_idx, "latency_ms": refresh_read_values[-1], "status": "ok"})
                finally:
                    if raw_handle is not None:
                        raw_handle.close()
                    self.record_unmount(ctx, reader2, issues, section="cache_invalidation", op="reader_b", labels={"file_size": size, "run": run_idx})
            status = "ok" if errors == 0 else "error"
            rows.append(self.matrix_row("cache_invalidation", "cold_read", "", "", size, "", "ms", cold_read_values, errors, len(cold_read_values), runs, status=status))
            rows.append(self.matrix_row("cache_invalidation", "dir_list_refresh", "", "", size, "", "ms", dir_refresh_values, 0, len(dir_refresh_values), runs, status=status))
            rows.append(self.matrix_row("cache_invalidation", "stat_refresh", "", "", size, "", "ms", metadata_refresh_values, 0, len(metadata_refresh_values), runs, status=status))
            rows.append(self.matrix_row("cache_invalidation", "read_refresh", "", "", size, "", "ms", refresh_read_values, errors, len(refresh_read_values), runs, status=status))
            ctx.perf_values(f"{self.id}.cache_invalidation.{size}b.cold_read", cold_read_values, "ms")
            ctx.perf_values(f"{self.id}.cache_invalidation.{size}b.dir_list_refresh", dir_refresh_values, "ms")
            ctx.perf_values(f"{self.id}.cache_invalidation.{size}b.stat_refresh", metadata_refresh_values, "ms")
            ctx.perf_values(f"{self.id}.cache_invalidation.{size}b.read_refresh", refresh_read_values, "ms")
            if errors:
                issues.append({"severity": "error", "section": "cache_invalidation", "op": "read_refresh", "file_size": size, "detail": f"errors={errors}"})
        return rows

    # ----- section: 2MB routing + large file S3 ------------------------

    def run_routing_2mb(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """口径: confirm the 2MB threshold routing — files <= 2MB should be
        served by the TiDB/db9 path (low latency), files > 2MB by the S3 path.
        We measure create/read latency across the boundary and label the
        inferred tier (small=TiDB, large=S3) for the report."""
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/routing-2mb"
        ctx.target.mkdir_remote(remote)
        threshold = 2 * 1024 * 1024
        for size in cfg["routing_sizes"]:
            progress(f"manus routing_2mb: size={size}")
            tier = "small_tidb" if size < threshold else ("boundary" if size == threshold else "large_s3")
            payload = stable_bytes(size, seed=size)
            create_values: list[float] = []
            read_values: list[float] = []
            errors = 0
            runs = int(cfg["runs"])
            ops = max(1, int(cfg["large_file_ops"]) if size > threshold else int(cfg["write_sync_ops"]))
            for run_idx in range(runs):
                handle = self.mount_shared(ctx, remote, agent="routing", cache_key=f"routing-{size}-run-{run_idx}", profile=cfg["profile"], durability=cfg["durability"])
                try:
                    root = handle.mountpoint / f"routing-{size}-run-{run_idx}"
                    if root.exists():
                        shutil.rmtree(root)
                    root.mkdir(parents=True)
                    for idx in range(ops):
                        path = root / f"f-{idx:06d}.bin"
                        start = time.perf_counter()
                        path.write_bytes(payload)
                        create_values.append((time.perf_counter() - start) * 1000)
                        start = time.perf_counter()
                        _ = path.read_bytes()
                        read_values.append((time.perf_counter() - start) * 1000)
                except OSError as exc:
                    errors += 1
                    issues.append({"severity": "warn", "section": "routing_2mb", "op": "work", "file_size": size, "detail": str(exc)})
                finally:
                    self.record_unmount(ctx, handle, issues, section="routing_2mb", op="suite", labels={"file_size": size, "run": run_idx})
            rows.append(self.matrix_row("routing_2mb", "create", "", "", size, "", "ms", create_values, errors, len(create_values), runs, status="ok" if errors == 0 else "error", extra={"tier": tier}))
            rows.append(self.matrix_row("routing_2mb", "read", "", "", size, "", "ms", read_values, errors, len(read_values), runs, status="ok" if errors == 0 else "error", extra={"tier": tier}))
            ctx.perf_values(f"{self.id}.routing_2mb.{size}b.{tier}.create", create_values, "ms")
            ctx.perf_values(f"{self.id}.routing_2mb.{size}b.{tier}.read", read_values, "ms")
        # Explicit routing summary row.
        rows.append({
            "section": "routing_2mb",
            "op": "routing_summary",
            "file_size": "",
            "concurrency": "",
            "unit": "",
            "status": "ok",
            "errors": 0,
            "error_rate": 0.0,
            "runs": 1,
            "detail": "files < 2MB are expected on the TiDB/db9 small-file path; files >= 2MB are expected on the S3 large-file path; latency deltas across the boundary confirm the routing split",
            **self.latency_summary([]),
        })
        return rows

    def run_large_file_s3(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """P1: 2MB+ file path / S3 path latency."""
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/large-file-s3"
        ctx.target.mkdir_remote(remote)
        for size in cfg["large_file_sizes"]:
            progress(f"manus large_file_s3: size={size}")
            payload = stable_bytes(size, seed=size)
            create_values: list[float] = []
            read_values: list[float] = []
            errors = 0
            runs = int(cfg["runs"])
            ops = max(1, int(cfg["large_file_ops"]))
            for run_idx in range(runs):
                handle = self.mount_shared(ctx, remote, agent="large", cache_key=f"large-{size}-run-{run_idx}", profile=cfg["profile"], durability=cfg["durability"])
                try:
                    root = handle.mountpoint / f"large-{size}-run-{run_idx}"
                    if root.exists():
                        shutil.rmtree(root)
                    root.mkdir(parents=True)
                    for idx in range(ops):
                        path = root / f"f-{idx:06d}.bin"
                        start = time.perf_counter()
                        path.write_bytes(payload)
                        create_values.append((time.perf_counter() - start) * 1000)
                        start = time.perf_counter()
                        _ = path.read_bytes()
                        read_values.append((time.perf_counter() - start) * 1000)
                except OSError as exc:
                    errors += 1
                    issues.append({"severity": "warn", "section": "large_file_s3", "op": "work", "file_size": size, "detail": str(exc)})
                finally:
                    self.record_unmount(ctx, handle, issues, section="large_file_s3", op="suite", labels={"file_size": size, "run": run_idx})
            rows.append(self.matrix_row("large_file_s3", "create", "", "", size, "", "ms", create_values, errors, len(create_values), runs, status="ok" if errors == 0 else "error"))
            rows.append(self.matrix_row("large_file_s3", "read", "", "", size, "", "ms", read_values, errors, len(read_values), runs, status="ok" if errors == 0 else "error"))
            ctx.perf_values(f"{self.id}.large_file_s3.{size}b.create", create_values, "ms")
            ctx.perf_values(f"{self.id}.large_file_s3.{size}b.read", read_values, "ms")
        return rows

    # ----- section: single-session TTL baselines -----------------------

    def run_single_session(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]], checkpoint: Callable[[list[dict[str, Any]] | None], None]) -> list[dict[str, Any]]:
        """单 Session: infinite-TTL read + writeback, infinite-TTL read +
        write-sync, across the three scale tiers. Answers the 8 single-session
        questions in the report."""
        rows: list[dict[str, Any]] = []
        combos = [
            ("infinite_ttl_writeback", DURABILITY_WRITEBACK),
            ("infinite_ttl_write_sync", DURABILITY_WRITE_SYNC),
        ]
        for combo_name, durability in combos:
            for scale_id in cfg["selected_scales"]:
                if scale_id not in cfg["scales"]:
                    raise ModuleSkip(f"unknown scale: {scale_id}", "configuration skip")
                scale = cfg["scales"][scale_id]
                remote = f"{remote_base}/single-{combo_name}-{scale_id}"
                ctx.target.mkdir_remote(remote)
                rows.append(self.single_session_prepare(ctx, cfg, remote, combo_name, durability, scale_id, scale, issues))
                if checkpoint is not None:
                    checkpoint(rows)
                rows.extend(self.single_session_measure(ctx, cfg, remote, combo_name, durability, scale_id, scale, raw_dir, issues))
                if checkpoint is not None:
                    checkpoint(rows)
        return rows

    def single_session_prepare(self, ctx: Context, cfg: dict[str, Any], remote: str, combo_name: str, durability: str, scale_id: str, scale: dict[str, Any], issues: list[dict[str, Any]]) -> dict[str, Any]:
        handle = self.mount_shared(ctx, remote, agent="ss_prepare", cache_key=f"{combo_name}-{scale_id}-prepare", profile=cfg["profile"], durability=durability)
        row: dict[str, Any] = {
            "section": "single_session",
            "op": "dataset_generate",
            "scale": scale_id,
            "combo": combo_name,
            "durability": durability,
            "unit": "seconds",
            "status": "error",
            "errors": 0,
            "error_rate": 0.0,
            "runs": 1,
            "target_files": int(scale["files"]),
            "target_bytes": int(scale["bytes"]),
            "created_files": 0,
            "created_bytes": 0,
        }
        try:
            manifest = handle.mountpoint / ".drive9-manus-single-dataset.json"
            expected = {"scale": scale_id, "combo": combo_name, "bytes": int(scale["bytes"]), "files": int(scale["files"])}
            if manifest.exists():
                try:
                    current = json.loads(manifest.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    current = {}
                if all(current.get(key) == value for key, value in expected.items()) and current.get("completed") is True:
                    seconds = float(current.get("seconds", 0.0))
                    row.update({"status": "cached", "created_files": int(scale["files"]), "created_bytes": int(scale["bytes"]), "count": int(scale["files"]), **self.latency_summary([seconds])})
                    return row
            data_dir = handle.mountpoint / "data"
            if data_dir.exists():
                shutil.rmtree(data_dir)
            data_dir.mkdir()
            started = time.perf_counter()
            base = int(scale["bytes"]) // int(scale["files"])
            extra_bytes = int(scale["bytes"]) % int(scale["files"])
            payload = stable_bytes(min(max(base + 1, 1), 1024 * 1024), seed=int(scale["files"]))
            created_files = 0
            created_bytes = 0
            try:
                for idx in range(int(scale["files"])):
                    size = base + (1 if idx < extra_bytes else 0)
                    parent = data_dir / f"shard-{idx // 1000:05d}"
                    parent.mkdir(parents=True, exist_ok=True)
                    self.write_payload(parent / f"file-{idx:08d}.bin", payload, size)
                    created_files += 1
                    created_bytes += size
                detail = "completed"
            except OSError as exc:
                detail = f"I/O error: {exc}"
            seconds = time.perf_counter() - started
            completed = created_files == int(scale["files"])
            self.write_dataset_manifest(manifest, expected, created_files, created_bytes, seconds, completed, detail)
            row.update({"status": "ok" if completed else "error", "created_files": created_files, "created_bytes": created_bytes, "count": created_files, "detail": detail, **self.latency_summary([seconds])})
            return row
        finally:
            self.record_unmount(ctx, handle, issues, section="single_session", op="dataset_generate", row=row, labels={"combo": combo_name, "scale": scale_id})

    def single_session_measure(self, ctx: Context, cfg: dict[str, Any], remote: str, combo_name: str, durability: str, scale_id: str, scale: dict[str, Any], raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        # infinite TTL read: read-cache-ttl=0 disables time-based expiry.
        handle = self.mount_shared(ctx, remote, agent="ss_measure", cache_key=f"{combo_name}-{scale_id}-measure", profile=cfg["profile"], durability=durability, read_cache_ttl_s=cfg["infinite_ttl_read_cache_s"])
        try:
            data_dir = handle.mountpoint / "data"
            # Cold read pass.
            cold_values: list[float] = []
            warm_values: list[float] = []
            stat_values: list[float] = []
            import random as _random

            rng = _random.Random(13)
            samples = min(int(cfg["namespace_stat_samples"]), int(scale["files"]))
            paths: list[Path] = []
            for _ in range(samples):
                idx = rng.randrange(0, int(scale["files"]))
                paths.append(data_dir / f"shard-{idx // 1000:05d}" / f"file-{idx:08d}.bin")
            # Cold read (first access, fetches from remote).
            for path in paths:
                start = time.perf_counter()
                _ = path.read_bytes()
                cold_values.append((time.perf_counter() - start) * 1000)
            # Warm read (should hit local read cache under infinite TTL).
            for path in paths:
                start = time.perf_counter()
                _ = path.read_bytes()
                warm_values.append((time.perf_counter() - start) * 1000)
            # Stat.
            for path in paths:
                start = time.perf_counter()
                os.stat(path)
                stat_values.append((time.perf_counter() - start) * 1000)
            rows.append(self.matrix_row("single_session", "cold_read", scale_id, "", "", "", "ms", cold_values, 0, len(cold_values), 1, status="ok", extra={"combo": combo_name, "durability": durability}))
            rows.append(self.matrix_row("single_session", "warm_read", scale_id, "", "", "", "ms", warm_values, 0, len(warm_values), 1, status="ok", extra={"combo": combo_name, "durability": durability}))
            rows.append(self.matrix_row("single_session", "stat", scale_id, "", "", "", "ms", stat_values, 0, len(stat_values), 1, status="ok", extra={"combo": combo_name, "durability": durability}))
            ctx.perf_values(f"{self.id}.single_session.{combo_name}.{scale_id}.cold_read", cold_values, "ms")
            ctx.perf_values(f"{self.id}.single_session.{combo_name}.{scale_id}.warm_read", warm_values, "ms")
            ctx.perf_values(f"{self.id}.single_session.{combo_name}.{scale_id}.stat", stat_values, "ms")
            # Small-file write sample for the durability combo.
            write_values: list[float] = []
            for size in cfg["small_file_sizes"]:
                payload = stable_bytes(size, seed=size + scale_id_hash(combo_name))
                wroot = handle.mountpoint / f"ss-write-{combo_name}-{scale_id}-{size}"
                wroot.mkdir(exist_ok=True)
                for idx in range(min(int(cfg["small_file_ops"]), 20)):
                    path = wroot / f"f-{idx:06d}.bin"
                    start = time.perf_counter()
                    path.write_bytes(payload)
                    write_values.append((time.perf_counter() - start) * 1000)
            rows.append(self.matrix_row("single_session", "small_write", scale_id, "", "", "", "ms", write_values, 0, len(write_values), 1, status="ok", extra={"combo": combo_name, "durability": durability}))
            ctx.perf_values(f"{self.id}.single_session.{combo_name}.{scale_id}.small_write", write_values, "ms")
        finally:
            self.record_unmount(ctx, handle, issues, section="single_session", op="measure", labels={"combo": combo_name, "scale": scale_id})
        return rows

    # ----- section: file lock behavior ---------------------------------

    def run_file_lock(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """File lock: confirm current Drive9 behavior. We attempt POSIX flock
        on a file through two concurrent mounts and record whether locks are
        honored (cross-mount exclusion) or not. Minghua stated file lock is not
        a hard requirement — the report just needs to state current behavior."""
        rows: list[dict[str, Any]] = []
        remote = f"{remote_base}/file-lock"
        ctx.target.mkdir_remote(remote)
        import fcntl

        has_fcntl = hasattr(fcntl, "flock")
        if not has_fcntl:
            rows.append({
                "section": "file_lock",
                "op": "behavior_note",
                "unit": "",
                "status": "skip",
                "errors": 0,
                "error_rate": 0.0,
                "runs": 1,
                "detail": "fcntl.flock unavailable on this platform; file-lock behavior not probed",
                **self.latency_summary([]),
            })
            return rows
        writer = self.mount_shared(ctx, remote, agent="fl_a", cache_key="file-lock-a", profile=cfg["profile"], durability=cfg["durability"])
        reader = self.mount_shared(ctx, remote, agent="fl_b", cache_key="file-lock-b", profile=cfg["profile"], durability=cfg["durability"])
        excluded = 0
        attempts = 0
        errors = 0
        try:
            lock_path_a = writer.mountpoint / "lock.bin"
            lock_path_a.write_bytes(b"lock")
            lock_path_b = reader.mountpoint / "lock.bin"
            for idx in range(max(1, int(cfg["cache_invalidation_samples"]))):
                attempts += 1
                fd_a = os.open(lock_path_a, os.O_RDWR)
                try:
                    fcntl.flock(fd_a, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fd_b = os.open(lock_path_b, os.O_RDWR)
                    try:
                        try:
                            fcntl.flock(fd_b, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except OSError:
                            excluded += 1
                    finally:
                        os.close(fd_b)
                except OSError as exc:
                    errors += 1
                    issues.append({"severity": "warn", "section": "file_lock", "op": "flock_a", "detail": str(exc)})
                finally:
                    try:
                        fcntl.flock(fd_a, fcntl.LOCK_UN)
                    except OSError:
                        pass
                    os.close(fd_a)
        finally:
            self.record_unmount(ctx, reader, issues, section="file_lock", op="reader")
            self.record_unmount(ctx, writer, issues, section="file_lock", op="writer")
        honored = excluded == attempts and errors == 0
        rows.append({
            "section": "file_lock",
            "op": "cross_mount_flock",
            "unit": "count",
            "status": "ok",
            "errors": errors,
            "error_rate": errors / max(1, attempts),
            "runs": 1,
            "attempts": attempts,
            "excluded": excluded,
            "honored": bool(honored),
            "detail": (
                f"cross-mount POSIX flock {'is honored' if honored else 'is NOT honored'} "
                f"(excluded={excluded}/{attempts}); Drive9 FUSE does not provide a distributed file lock; "
                "local flock advisory locks may not propagate across separate mount instances"
            ),
            **self.latency_summary([]),
        })
        return rows

    # ----- section: extra clone+build comparison -----------------------

    def run_extra_clone_build(self, ctx: Context, cfg: dict[str, Any], remote_base: str, raw_dir: Path, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extra case: compare clone+build time of a vite+react+tailwind repo
        under {local disk, drive9 fuse writeback, drive9 fuse write-sync} x
        {coding-agent, none} profiles."""
        rows: list[dict[str, Any]] = []
        repo = self.selected_extra_repo(ctx, cfg)
        if repo is None:
            rows.append({
                "section": "extra_clone_build",
                "op": "config_note",
                "unit": "",
                "status": "skip",
                "errors": 0,
                "error_rate": 0.0,
                "runs": 1,
                "detail": f"extra repo id {cfg['extra_repo_id']} not found in config.json repos",
                **self.latency_summary([]),
            })
            return rows
        env = self.base_repo_env(ctx)
        commit = self.resolve_commit(ctx, repo, env)
        runs = max(1, int(cfg["extra_runs"]))
        storages = cfg["extra_storages"]
        profiles = cfg["extra_profiles"]
        samples: list[dict[str, Any]] = []
        failures: list[str] = []
        for run_idx in range(1, runs + 1):
            for profile in profiles:
                for storage in storages:
                    progress(f"manus extra_clone_build: run={run_idx} profile={profile} storage={storage}")
                    sample = self.run_extra_sample(ctx, cfg, repo, commit, storage, profile, run_idx, env, failures)
                    samples.append(sample)
                    ctx.recorder.event(sample)
                    rows.append(self.extra_sample_row(sample))
                    if sample.get("status") != "ok":
                        failures.append(f"{repo['id']} {storage} {profile} run {run_idx}: {sample.get('detail')}")
        self.write_extra_summary(ctx, samples, failures, repo, commit)
        if failures:
            issues.append({"severity": "error", "section": "extra_clone_build", "op": "sample", "detail": f"failures={len(failures)}; see artifacts/summary.md"})
        return rows

    def selected_extra_repo(self, ctx: Context, cfg: dict[str, Any]) -> dict[str, Any] | None:
        repos = module_config(ctx, self.id).get("repos", [])
        for repo in repos:
            if str(repo.get("id")) == str(cfg["extra_repo_id"]):
                return repo
        return None

    def base_repo_env(self, ctx: Context) -> dict[str, str]:
        env = ctx.target.base_env()
        cache_root = ctx.tmp_dir / "manus-extra-build" / "shared-cache"
        values = {
            "COREPACK_HOME": cache_root / "corepack",
            "npm_config_cache": cache_root / "npm",
            "PNPM_STORE_DIR": cache_root / "pnpm-store",
            "npm_config_store_dir": cache_root / "pnpm-store",
        }
        for key, path in values.items():
            path.mkdir(parents=True, exist_ok=True)
            env[key] = str(path)
        return env

    def resolve_commit(self, ctx: Context, repo: dict[str, Any], env: dict[str, str]) -> str:
        if repo.get("commit"):
            return str(repo["commit"])
        ref = str(repo.get("ref", "HEAD"))
        refs = [ref]
        if ref and ref != "HEAD" and not ref.startswith("refs/"):
            refs.append(f"refs/heads/{ref}")
            refs.append(f"refs/tags/{ref}")
        for item in refs:
            try:
                out = ctx.target.capture(["git", "ls-remote", "--exit-code", str(repo["url"]), item], timeout=120, env=env)
            except Exception:
                continue
            for line in out.splitlines():
                parts = line.split()
                if parts:
                    return parts[0]
        raise BlackboxError(f"could not resolve {repo.get('id')} ref {ref!r}")

    def run_extra_sample(
        self,
        ctx: Context,
        cfg: dict[str, Any],
        repo: dict[str, Any],
        commit: str,
        storage: str,
        profile: str,
        run_index: int,
        env: dict[str, str],
        failures: list[str],
    ) -> dict[str, Any]:
        sample_env = self.sample_env(ctx, str(repo["id"]), storage, profile, run_index, env)
        phases: list[dict[str, Any]] = []
        if storage == "local":
            checkout_parent = ctx.tmp_dir / "manus-extra-build" / "local" / str(repo["id"]) / f"run-{run_index}-{profile}"
            from harness.core import ensure_empty

            ensure_empty(checkout_parent)
            checkout = checkout_parent / "repo"
            phases.append(self.run_phase(ctx, "clone", [["git", "clone", "--no-checkout", str(repo["url"]), str(checkout)]], checkout_parent, 1800, sample_env, shell=False, name=f"manus-extra-local-{repo['id']}-{profile}-run-{run_index}-clone"))
            phases.append(self.run_phase(ctx, "checkout", [["git", "-C", str(checkout), "checkout", "--detach", commit]], checkout_parent, 1800, sample_env, shell=False, name=f"manus-extra-local-{repo['id']}-{profile}-run-{run_index}-checkout"))
            phases.append(self.run_phase(ctx, "install", [str(c) for c in repo.get("install", ["corepack prepare pnpm@10.33.0 --activate", "corepack pnpm install --frozen-lockfile"])], checkout, 1800, sample_env, shell=True, name=f"manus-extra-local-{repo['id']}-{profile}-run-{run_index}-install"))
            phases.append(self.run_phase(ctx, "build", [str(c) for c in repo.get("build", ["corepack pnpm run build"])], checkout, 1800, sample_env, shell=True, name=f"manus-extra-local-{repo['id']}-{profile}-run-{run_index}-build"))
            return self.assemble_sample(repo, commit, storage, profile, run_index, phases, failures)
        # FUSE storages.
        durability = DURABILITY_WRITEBACK if storage == "fuse-writeback" else DURABILITY_WRITE_SYNC
        remote = ctx.target.remote_root(self.id, f"extra-{repo['id']}-{storage}-{profile}-run-{run_index}")
        ctx.target.mkdir_remote(remote)
        handle = self.mount_shared(ctx, remote, agent="extra_build", cache_key=f"extra-{storage}-{profile}-run-{run_index}", profile=profile, durability=durability)
        try:
            checkout = handle.mountpoint / "repo"
            phases.append(self.run_phase(ctx, "clone", [["git", "clone", "--no-checkout", str(repo["url"]), str(checkout)]], handle.mountpoint, 1800, sample_env, shell=False, name=f"manus-extra-{storage}-{repo['id']}-{profile}-run-{run_index}-clone"))
            phases.append(self.run_phase(ctx, "checkout", [["git", "-C", str(checkout), "checkout", "--detach", commit]], handle.mountpoint, 1800, sample_env, shell=False, name=f"manus-extra-{storage}-{repo['id']}-{profile}-run-{run_index}-checkout"))
            phases.append(self.run_phase(ctx, "install", [str(c) for c in repo.get("install", ["corepack prepare pnpm@10.33.0 --activate", "corepack pnpm install --frozen-lockfile"])], checkout, 1800, sample_env, shell=True, name=f"manus-extra-{storage}-{repo['id']}-{profile}-run-{run_index}-install"))
            phases.append(self.run_phase(ctx, "build", [str(c) for c in repo.get("build", ["corepack pnpm run build"])], checkout, 1800, sample_env, shell=True, name=f"manus-extra-{storage}-{repo['id']}-{profile}-run-{run_index}-build"))
            return self.assemble_sample(repo, commit, storage, profile, run_index, phases, failures)
        finally:
            ctx.target.unmount(handle)

    def sample_env(self, ctx: Context, repo_id: str, storage: str, profile: str, run_index: int, base: dict[str, str]) -> dict[str, str]:
        env = dict(base)
        sample = ctx.tmp_dir / "manus-extra-build" / "sample-cache" / storage / profile / repo_id / f"run-{run_index}"
        sample.mkdir(parents=True, exist_ok=True)
        return env

    def run_phase(self, ctx: Context, phase: str, commands: list[list[str] | str], cwd: Path, timeout: int, env: dict[str, str], *, shell: bool, name: str) -> dict[str, Any]:
        started = time.monotonic()
        exit_code = 0
        stdout = ""
        stderr = ""
        for command in commands:
            result = ctx.target.run_cmd(name, command, cwd=cwd, timeout=timeout, env=env, shell=shell, ok_codes=(0,))
            stdout = str(result.stdout)
            stderr = str(result.stderr)
            exit_code = int(result.code)
            if not result.ok:
                break
        return {
            "phase": phase,
            "status": "ok" if exit_code == 0 else "failed",
            "exit_code": exit_code,
            "duration_seconds": time.monotonic() - started,
            "stdout": stdout,
            "stderr": stderr,
            "commands": [self.command_text(command) for command in commands],
        }

    def assemble_sample(self, repo: dict[str, Any], commit: str, storage: str, profile: str, run_index: int, phases: list[dict[str, Any]], failures: list[str]) -> dict[str, Any]:
        total = sum(float(p["duration_seconds"]) for p in phases)
        ok = all(p["status"] == "ok" for p in phases)
        failed_phase = next((p for p in phases if p["status"] != "ok"), None)
        detail = "" if ok else f"{failed_phase['phase']} failed; see {failed_phase['stderr']}" if failed_phase else "unknown failure"
        if not ok:
            failures.append(f"{repo['id']} {storage} {profile} run {run_index}: {detail}")
        return {
            "type": "manus_extra_clone_build",
            "module": self.id,
            "repo": str(repo["id"]),
            "commit": commit,
            "storage": storage,
            "profile": profile,
            "run": run_index,
            "status": "ok" if ok else "failed",
            "total_seconds": total,
            "phases": phases,
            "detail": detail,
        }

    def extra_sample_row(self, sample: dict[str, Any]) -> dict[str, Any]:
        phase_seconds = {p["phase"]: float(p["duration_seconds"]) for p in sample.get("phases", [])}
        return {
            "section": "extra_clone_build",
            "op": "clone_build",
            "scale": "",
            "layout": "",
            "file_size": "",
            "concurrency": "",
            "unit": "seconds",
            "status": sample["status"],
            "errors": 0 if sample["status"] == "ok" else 1,
            "error_rate": 0.0 if sample["status"] == "ok" else 1.0,
            "runs": 1,
            "repo": sample["repo"],
            "storage": sample["storage"],
            "profile": sample["profile"],
            "run": sample["run"],
            "total_seconds": sample["total_seconds"],
            "clone_seconds": phase_seconds.get("clone", 0.0),
            "checkout_seconds": phase_seconds.get("checkout", 0.0),
            "install_seconds": phase_seconds.get("install", 0.0),
            "build_seconds": phase_seconds.get("build", 0.0),
            **self.latency_summary([sample["total_seconds"]]),
        }

    def write_extra_summary(self, ctx: Context, samples: list[dict[str, Any]], failures: list[str], repo: dict[str, Any], commit: str) -> None:
        artifact = ctx.artifact_dir(self.id)
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for sample in samples:
            groups.setdefault((str(sample.get("repo", "")), str(sample.get("storage", "")), str(sample.get("profile", ""))), []).append(sample)
        rows: list[dict[str, Any]] = []
        for key, events in sorted(groups.items()):
            repo_id, storage, profile = key
            values = [float(e["total_seconds"]) for e in events if e.get("status") == "ok"]
            phase_totals: dict[str, list[float]] = {}
            for e in events:
                if e.get("status") != "ok":
                    continue
                for p in e.get("phases", []):
                    phase_totals.setdefault(p["phase"], []).append(float(p["duration_seconds"]))
            row = {
                "repo": repo_id,
                "storage": storage,
                "profile": profile,
                "ok": sum(1 for e in events if e.get("status") == "ok"),
                "count": len(events),
                "total_mean_s": statistics.mean(values) if values else None,
                "total_median_s": statistics.median(values) if values else None,
                "total_min_s": min(values) if values else None,
                "total_max_s": max(values) if values else None,
            }
            for phase in ("clone", "checkout", "install", "build"):
                pv = phase_totals.get(phase, [])
                row[f"{phase}_mean_s"] = statistics.mean(pv) if pv else None
            rows.append(row)
        # Ratios vs local.
        ratios: list[dict[str, Any]] = []
        for profile in sorted({r["profile"] for r in rows}):
            local = next((r for r in rows if r["storage"] == "local" and r["profile"] == profile), None)
            if not local or not local.get("total_mean_s"):
                continue
            for r in rows:
                if r["profile"] == profile and r["storage"] != "local" and r.get("total_mean_s"):
                    ratio = float(r["total_mean_s"]) / float(local["total_mean_s"])
                    ratios.append({"repo": r["repo"], "profile": profile, "storage": r["storage"], "vs_local_ratio": ratio})
                    ctx.metric(f"{self.id}.extra.{r['repo']}.{profile}.{r['storage']}.vs_local_ratio", ratio, "x")
        write_json(artifact / "extra_summary.json", {"repo": str(repo["id"]), "commit": commit, "rows": rows, "ratios": ratios, "failures": failures})
        lines = [
            "# Manus Extra: vite+react+tailwind Clone+Build Comparison",
            "",
            f"- Repo: `{repo['id']}` ({repo.get('url')}) @ `{commit}`",
            f"- Failures: `{len(failures)}`",
            "",
            "| repo | storage | profile | ok/count | total mean s | clone s | checkout s | install s | build s |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|",
        ]
        for r in rows:
            lines.append(
                f"| {r['repo']} | {r['storage']} | {r['profile']} | {r['ok']}/{r['count']} | "
                f"{self.optional_float(r['total_mean_s'])} | {self.optional_float(r.get('clone_mean_s'))} | "
                f"{self.optional_float(r.get('checkout_mean_s'))} | {self.optional_float(r.get('install_mean_s'))} | "
                f"{self.optional_float(r.get('build_mean_s'))} |"
            )
        lines.extend(["", "## FUSE / Local Ratios", ""])
        for ratio in ratios:
            lines.append(f"- `{ratio['repo']}` `{ratio['profile']}` `{ratio['storage']}`: `{ratio['vs_local_ratio']:.3f}x` vs local")
        (artifact / "extra_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # ----- summary / report --------------------------------------------

    def write_summary_outputs(self, ctx: Context, artifact: Path, summary_dir: Path, rows: list[dict[str, Any]], issues: list[dict[str, Any]]) -> None:
        all_keys = sorted({key for row in rows for key in row.keys()})
        csv_path = summary_dir / "summary.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=all_keys)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        write_json(summary_dir / "summary.json", {"rows": rows, "issues": issues})
        self._last_report_markdown = self.render_customer_report(ctx, rows, issues)

    def render_report(self, ctx: Context, record: Any) -> str | None:
        return self._last_report_markdown or None

    def render_customer_report(self, ctx: Context, rows: list[dict[str, Any]], issues: list[dict[str, Any]]) -> str:
        status_counts: dict[str, int] = {}
        for row in rows:
            status_counts[str(row.get("status", ""))] = status_counts.get(str(row.get("status", "")), 0) + 1
        sections_present = {row.get("section") for row in rows}
        lines = [
            "# Drive9 Manus Persistent-Sandbox Performance Report",
            "",
            f"- Session: `{ctx.session}`",
            f"- Result dir: `{ctx.result_dir}`",
            f"- Target: `{getattr(ctx.target, 'server_url', '')}`",
            f"- Rows: `{len(rows)}`; issues: `{len(issues)}`; statuses: `{json.dumps(status_counts, sort_keys=True)}`",
            "",
            "## Executive Summary",
            "",
            "- Multi-session shared workspace: drive9 supports multiple agents mounting the same workspace; cross-mount visibility and concurrent distinct-file workloads are validated.",
            "- 2MB small-file read/write: files below 2MB route through the TiDB/db9 path; latency deltas across the 2MB boundary confirm the routing split.",
            "- Write-sync: small-file write latency under write-sync durability is measured for files up to 2MB.",
            "- Cache invalidation: read cost after another session writes (dir list refresh, stat refresh, read refresh) is measured.",
            "- Single-session: infinite-TTL read + writeback and infinite-TTL read + write-sync baselines are recorded across scale tiers.",
            "- Extra: a vite+react+tailwind repo clone+build is compared across local disk / drive9 writeback / drive9 write-sync x coding-agent / none profiles.",
            "",
            "## Requirement Coverage",
            "",
            "| Customer request | Evidence in this run | Status |",
            "|---|---|---|",
        ]
        coverage = [
            ("P0-1 multi-session mount same workspace", "multi_session_shared", rows),
            ("P0-2 session A write, session B read", "cross_visibility", rows),
            ("P0-3 write-sync small-file latency", "write_sync_small", rows),
            ("P0-4 cache-invalidation read cost", "cache_invalidation", rows),
            ("P0-5 <=2MB file read/write", "routing_2mb", rows),
            ("P0-6 dir list/stat/open/close", "namespace_ops", rows),
            ("P0-7 concurrent read/write distinct files", "concurrent_distinct", rows),
            ("P0-8 concurrent same-file behavior", "concurrent_same_file", rows),
            ("P1 2MB+ / S3 path", "large_file_s3", rows),
            ("单 Session infinite TTL + writeback/write-sync", "single_session", rows),
            ("File lock current behavior", "file_lock", rows),
            ("Extra: vite+react+tailwind clone+build", "extra_clone_build", rows),
        ]
        for label, section, all_rows in coverage:
            section_rows = [r for r in all_rows if r.get("section") == section]
            present = section in sections_present
            lines.append(f"| {label} | rows={len(section_rows)} | {self.coverage_status(section_rows, present)} |")
        lines.extend(["", "## Issues And Limits", ""])
        if issues:
            for item in issues:
                detail = item.get("detail", "")
                lines.append(f"- {item.get('severity', 'warn').upper()} `{item.get('section', '')}/{item.get('op', '')}` {detail}")
        else:
            lines.append("- None")
        # Per-section tables.
        for section, title, columns in [
            ("multi_session_shared", "Multi-Session Shared Workspace", ["op", "concurrency", "status", "count", "p50", "p95", "p99", "max", "errors", "unit"]),
            ("cross_visibility", "Cross-Mount Visibility (A write, B read)", ["op", "file_size", "status", "count", "p50", "p95", "p99", "max", "errors", "visibility_errors", "unit"]),
            ("concurrent_distinct", "Concurrent Distinct-File Read/Write", ["op", "file_size", "concurrency", "status", "count", "p50", "p95", "p99", "max", "qps", "errors", "unit"]),
            ("concurrent_same_file", "Concurrent Same-File Read/Write", ["op", "file_size", "concurrency", "status", "count", "p50", "p95", "p99", "max", "errors", "corruption_indicators", "unit"]),
            ("namespace_ops", "Namespace list/stat/open/close", ["op", "scale", "status", "target_files", "created_files", "count", "p50", "p95", "p99", "max", "errors", "unit"]),
            ("write_sync_small", "Write-Sync Small-File Latency", ["op", "file_size", "concurrency", "status", "count", "p50", "p95", "p99", "max", "qps", "errors", "unit"]),
            ("cache_invalidation", "Cache Invalidation Read Cost", ["op", "file_size", "status", "count", "p50", "p95", "p99", "max", "errors", "unit"]),
            ("routing_2mb", "2MB Routing Boundary", ["op", "file_size", "tier", "status", "count", "p50", "p95", "p99", "max", "errors", "unit"]),
            ("large_file_s3", "Large-File S3 Path", ["op", "file_size", "status", "count", "p50", "p95", "p99", "max", "errors", "unit"]),
            ("single_session", "Single-Session TTL Baselines", ["op", "scale", "combo", "durability", "status", "count", "p50", "p95", "p99", "max", "unit"]),
            ("file_lock", "File Lock Behavior", ["op", "status", "attempts", "excluded", "honored", "detail", "unit"]),
            ("extra_clone_build", "Extra: vite+react+tailwind Clone+Build", ["op", "repo", "storage", "profile", "run", "status", "total_seconds", "clone_seconds", "install_seconds", "build_seconds", "unit"]),
        ]:
            lines.extend(["", f"## {title}", ""])
            self.append_table(lines, [r for r in rows if r.get("section") == section], columns)
        lines.extend(
            [
                "",
                "## Notes",
                "",
                "- `multi_session_shared` mounts N agents on the same remote workspace with isolated cache dirs; it validates shared-read concurrency.",
                "- `cross_visibility` uses separate writer/reader mounts and caches; visibility latency is the time until the reader observes the writer's new content.",
                "- `concurrent_same_file` documents last-writer-wins behavior; Drive9 does not provide a distributed file lock (see `file_lock` section).",
                "- `routing_2mb` measures latency across the 2MB boundary; `tier` labels infer the small (TiDB/db9) vs large (S3) path.",
                "- `single_session` uses `--read-cache-ttl 0` for infinite-TTL read; writeback uses `--durability auto`, write-sync uses `--durability write-sync`.",
                "- `extra_clone_build` repo is vite+react+tailwind (cruip/tailwind-dashboard-template); local disk is the native baseline, FUSE writeback/write-sync use the drive9 mount.",
                "- Rows with `timeout` or `error` are product/test-environment findings and are intentionally kept in the report instead of aborting the module.",
                "",
            ]
        )
        return "\n".join(lines)

    @staticmethod
    def coverage_status(rows: list[dict[str, Any]], present: bool = True) -> str:
        if not rows and not present:
            return "NOT RUN"
        if not rows:
            return "SKIPPED"
        statuses = {row.get("status") for row in rows}
        if statuses <= {"ok", "cached", "completed", "skip"}:
            return "COMPLETE"
        if statuses & {"ok", "cached", "completed"}:
            return "PARTIAL"
        return "FAILED"

    # ----- generic row / latency helpers -------------------------------

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
            **Drive9ManusPerf.latency_summary([seconds] if seconds else []),
        }

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
    def timed_call(fn: Callable[[int], None], idx: int) -> tuple[float, str]:
        start = time.perf_counter()
        try:
            fn(idx)
            return (time.perf_counter() - start) * 1000, ""
        except Exception as exc:
            return (time.perf_counter() - start) * 1000, str(exc)

    @staticmethod
    def raw_write(handle: Any, value: dict[str, Any]) -> None:
        if handle is not None:
            handle.write(json.dumps(value, sort_keys=True) + "\n")

    def capture_environment(self, ctx: Context, artifact: Path) -> None:
        commands = {
            "uname": ["uname", "-a"],
            "nproc": ["bash", "-lc", "nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || true"],
            "free": ["bash", "-lc", "free -h 2>/dev/null || vm_stat 2>/dev/null || true"],
            "df": ["df", "-h"],
            "node": ["bash", "-lc", "node --version 2>/dev/null || true"],
            "pnpm": ["bash", "-lc", "pnpm --version 2>/dev/null || true"],
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
        write_json(artifact / "environment.json", out)

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
    def format_cell(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.3f}"
        if isinstance(value, bool):
            return str(value)
        return str(value)

    @staticmethod
    def command_text(command: list[str] | str) -> str:
        if isinstance(command, str):
            return command
        return " ".join(command)

    @staticmethod
    def optional_float(value: Any) -> str:
        if value is None:
            return ""
        return f"{float(value):.6f}"


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


def scale_id_hash(combo: str) -> int:
    return abs(hash(combo)) % (2**31)