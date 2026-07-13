from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from harness.core import BlackboxError, Context, DependencyUnavailable
from harness.module_base import BaseModule
from suites.community.ltp_fs.deps import (
    DEFAULT_LTP_FS_SCENARIO,
    build_ltp_runner_cmd,
    ensure_ltp,
    resolve_runner,
)


class CommunityLTPFS(BaseModule):
    description = "Run Linux Test Project filesystem tests with their work directory on Drive9 FUSE."
    labels = ("compatibility", "linux", "community")
    timeout = 1800

    def ensure_dependencies(self, ctx: Context) -> None:
        ensure_ltp(ctx)

    def run(self, ctx: Context) -> dict[str, Any]:
        ltp = ensure_ltp(ctx)
        runner = resolve_runner(ltp)
        env = ctx.target.base_env()
        env["LTPROOT"] = str(ltp)
        scenario = os.environ.get("LTP_FS_SCENARIO", DEFAULT_LTP_FS_SCENARIO)
        if scenario == DEFAULT_LTP_FS_SCENARIO and not (ltp / "runtest" / scenario).exists():
            scenario = "fs"
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_ltp_fs", remote, profile="none", extra=["--allow-other"])
        try:
            work = handle.mountpoint / "ltp-work"
            work.mkdir()
            # Set TMPDIR to the FUSE mount so LTP tests that create temp files
            # (ftest, stream, fs_inod, etc.) exercise the FUSE filesystem, not
            # host /tmp. kirk's --tmp-dir is its session dir; TMPDIR is what
            # the LTP test binaries themselves use.
            env["TMPDIR"] = str(work)
            # FUSE is much slower than local /tmp for file-creation-heavy
            # tests (inode02 creates thousands of nested dirs). LTP's default
            # 30s test timeout is too short — multiply by 10x so tests that
            # normally take <1s have up to 300s to complete on FUSE.
            env.setdefault("LTP_TIMEOUT_MUL", "10")
            cmd = build_ltp_runner_cmd(runner, scenario, work)
            result = ctx.target.run_cmd(
                "community-ltp-fs",
                cmd,
                timeout=int(os.environ.get("LTP_FS_TIMEOUT_S", str(self.timeout))),
                env=env,
            )
            if not result.ok:
                raise BlackboxError(f"LTP fs failed; see {result.stderr}")
            return {"ltp_root": str(ltp), "ltp_scenario": scenario, "ltp_runner": Path(runner).name}
        finally:
            ctx.target.unmount(handle)