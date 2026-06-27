from __future__ import annotations

import os
from typing import Any

from harness.core import Context, DependencyUnavailable, summarize
from harness.module_base import BaseModule, module_config, timeit
from suites.git._deps import ensure_git_source


class GitOfficialPerf(BaseModule):
    description = "Run selected upstream Git t/perf tests with scratch data on Drive9 FUSE."
    labels = ("performance", "git", "community")
    timeout = 7200

    def run(self, ctx: Context) -> dict[str, Any]:
        ctx.deps.ensure_git_tool()
        source = ensure_git_source(ctx)
        perf_run = source / "t" / "perf" / "run"
        if not perf_run.exists():
            raise DependencyUnavailable("Git t/perf runner not found")
        tests = module_config(ctx, self.id).get("tests", ["p0001-rev-list.sh"])
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("git_official_perf", remote, durability="interactive")
        try:
            root = handle.mountpoint / "git-perf"
            root.mkdir()
            values: list[float] = []
            for run in range(ctx.runs):
                run_root = root / f"run-{run}"
                run_root.mkdir()
                env = ctx.target.base_env()
                env["GIT_TEST_INSTALLED"] = str(source / "bin-wrappers")
                env["GIT_TEST_OPTS"] = " ".join(part for part in [env.get("GIT_TEST_OPTS", ""), f"--root={run_root}"] if part)
                output_dir = ctx.artifact_dir(self.id) / f"run-{run}"
                output_dir.mkdir(parents=True, exist_ok=True)
                env["TEST_OUTPUT_DIRECTORY"] = str(output_dir)
                seconds = timeit(
                    lambda: ctx.target.capture(
                        [str(perf_run), *tests],
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
