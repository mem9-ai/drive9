from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from harness.core import BlackboxError, Context, DependencyUnavailable
from harness.module_base import BaseModule
from suites.community.ltp_fs.deps import (
    DEFAULT_LTP_SYSCALL_SCENARIO,
    build_ltp_runner_cmd,
    ensure_ltp,
    ltp_syscall_shards,
    resolve_runner,
)


class CommunityLTPSyscalls(BaseModule):
    description = "Run the filesystem-sensitive Linux Test Project syscall subset on Drive9 FUSE."
    labels = ("compatibility", "linux", "community")
    timeout = 1900

    def ensure_dependencies(self, ctx: Context) -> None:
        ensure_ltp(ctx)

    def run(self, ctx: Context) -> dict[str, Any]:
        ltp = ensure_ltp(ctx)
        runner = resolve_runner(ltp)
        env = ctx.target.base_env()
        env["LTPROOT"] = str(ltp)
        scenario = os.environ.get("LTP_SYSCALLS_SCENARIO", DEFAULT_LTP_SYSCALL_SCENARIO)
        # In deny-list mode, shards are drive9-syscalls-fs-0/1/2 (no unsharded
        # file). Only fall back to the full upstream "syscalls" scenario if
        # neither the unsharded file nor any shard file exists.
        if scenario == DEFAULT_LTP_SYSCALL_SCENARIO:
            runtest_dir = ltp / "runtest"
            has_unsharded = (runtest_dir / scenario).exists()
            has_shards = any((runtest_dir / f"{scenario}-{i}").exists() for i in range(ltp_syscall_shards()))
            if not has_unsharded and not has_shards:
                scenario = "syscalls"
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_ltp_syscalls", remote, profile="none", extra=["--allow-other"])
        try:
            work = handle.mountpoint / "ltp-work"
            work.mkdir()
            # Set TMPDIR to the FUSE mount so LTP tests that create temp files
            # exercise the FUSE filesystem, not host /tmp.
            env["TMPDIR"] = str(work)
            # FUSE is much slower than local /tmp for file-creation-heavy tests.
            # LTP's default 30s test timeout is too short — multiply by 10x.
            env.setdefault("LTP_TIMEOUT_MUL", "10")

            # Determine the scenario files to run. In deny-list mode, shards
            # are named drive9-syscalls-fs-0/1/2; in allow-list or unsharded
            # mode there is a single drive9-syscalls-fs file.
            scenarios = _resolve_scenarios(ltp, scenario)
            # Budget the per-shard timeout from the module-level timeout so
            # that all shards fit within the harness's outer wall-clock. With
            # the default 1900s module timeout and 3 shards, each shard gets
            # ~620s. If LTP_SYSCALLS_TIMEOUT_S is set explicitly, use that
            # per-shard instead (operator takes responsibility for the total).
            explicit_per_shard = os.environ.get("LTP_SYSCALLS_TIMEOUT_S") or os.environ.get("BLACKBOX_LTP_SYSCALLS_TIMEOUT_S")
            if explicit_per_shard:
                per_timeout = int(explicit_per_shard)
            else:
                # Reserve ~20s for mount/unmount overhead, split remainder
                # across shards (minimum 60s per shard).
                per_timeout = max(60, (self.timeout - 20) // max(1, len(scenarios)))

            failures: list[str] = []
            for sc in scenarios:
                cmd = build_ltp_runner_cmd(runner, sc, work)
                result = ctx.target.run_cmd(
                    "community-ltp-syscalls",
                    cmd,
                    timeout=per_timeout,
                    env=env,
                )
                if not result.ok:
                    failures.append(f"{sc} (see {result.stderr})")

            if failures:
                raise BlackboxError(f"LTP syscalls failed: {'; '.join(failures)}")
            return {
                "ltp_root": str(ltp),
                "ltp_scenario": scenario,
                "ltp_runner": Path(runner).name,
                "ltp_shards": len(scenarios),
            }
        finally:
            ctx.target.unmount(handle)


def _resolve_scenarios(ltp: Path, scenario: str) -> list[str]:
    """Resolve the list of scenario files to run.

    If the scenario is the default (drive9-syscalls-fs) and shard files exist,
    return all shard names. Otherwise return the single scenario.
    """
    if scenario != DEFAULT_LTP_SYSCALL_SCENARIO:
        return [scenario]
    runtest_dir = ltp / "runtest"
    shards = ltp_syscall_shards()
    shard_names = []
    for i in range(shards):
        shard_file = runtest_dir / f"{DEFAULT_LTP_SYSCALL_SCENARIO}-{i}"
        if shard_file.exists():
            shard_names.append(f"{DEFAULT_LTP_SYSCALL_SCENARIO}-{i}")
    if shard_names:
        return shard_names
    # Fall back to the unsharded file or the full upstream scenario.
    if (runtest_dir / scenario).exists():
        return [scenario]
    return ["syscalls"]