from __future__ import annotations

import os
import shutil
from typing import Any

from harness.core import Context, DependencyUnavailable, summarize
from .base import BaseModule, module_config, timeit


class GitOfficialPerf(BaseModule):
    id = "git.official.perf"
    category = "git.official.performance"
    description = "Run selected upstream Git t/perf tests with scratch data on Drive9 FUSE."
    labels = ("performance", "git", "community")
    timeout = 7200

    def run(self, ctx: Context) -> dict[str, Any]:
        if not shutil.which("git"):
            raise DependencyUnavailable("git is required")
        source = ctx.deps.ensure_git_source()
        perf_run = source / "t" / "perf" / "run"
        if not perf_run.exists():
            raise DependencyUnavailable("Git t/perf runner not found")
        tests = module_config(ctx, self.id).get("tests", ["p0001-rev-list.sh"])
        remote = ctx.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("git_official_perf", remote, durability="interactive")
        try:
            root = handle.mountpoint / "git-perf"
            root.mkdir()
            env = ctx.target.base_env()
            env["GIT_TEST_INSTALLED"] = shutil.which("git") or "git"
            values: list[float] = []
            for run in range(ctx.runs):
                seconds = timeit(
                    lambda: ctx.target.capture(
                        [str(perf_run), f"--root={root / ('run-' + str(run))}", *tests],
                        cwd=source / "t" / "perf",
                        timeout=int(os.environ.get("GIT_PERF_TIMEOUT_S", str(self.timeout))),
                        env=env,
                    )
                )
                values.append(seconds)
            ctx.perf_values("git.official.perf.total", values, "seconds")
            return {"tests": tests, "summary": summarize(values, "seconds")}
        finally:
            ctx.target.unmount(handle)
