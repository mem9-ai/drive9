from __future__ import annotations

import os
import shutil
from typing import Any

from harness.core import BlackboxError, Context, DependencyUnavailable, ModuleSkip
from .base import BaseModule, module_config


class GitOfficialFunctional(BaseModule):
    id = "git.official.functional"
    category = "git.official.functional"
    description = "Run selected upstream Git functional tests with trash roots on Drive9 FUSE."
    labels = ("compatibility", "git", "community")
    timeout = 7200

    def run(self, ctx: Context) -> dict[str, Any]:
        if not shutil.which("prove"):
            raise DependencyUnavailable("prove is required for Git official tests")
        if not shutil.which("git"):
            raise DependencyUnavailable("git is required")
        source = ctx.deps.ensure_git_source()
        cfg = module_config(ctx, self.id)
        tests = cfg.get("tests", [])
        if not tests:
            raise ModuleSkip("no Git functional tests configured")
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("git_official_functional", remote, durability="write-sync")
        try:
            trash_root = handle.mountpoint / "git-test-trash"
            trash_root.mkdir()
            env = ctx.target.base_env()
            env["GIT_TEST_INSTALLED"] = str(source / "bin-wrappers")
            env["GIT_TEST_DEFAULT_INITIAL_BRANCH_NAME"] = "main"
            output_dir = ctx.artifact_dir(self.id) / "test-output"
            output_dir.mkdir(parents=True, exist_ok=True)
            env["TEST_OUTPUT_DIRECTORY"] = str(output_dir)
            test_paths = [str(source / "t" / test) for test in tests]
            result = ctx.target.run_cmd(
                "git-official-functional",
                ["prove", "--timer", "--verbose", *test_paths, "::", f"--root={trash_root}"],
                cwd=source / "t",
                timeout=int(os.environ.get("GIT_TEST_TIMEOUT_S", str(self.timeout))),
                env=env,
            )
            if not result.ok:
                raise BlackboxError(f"Git official functional tests failed; see {result.stderr}")
            return {"tests": tests, "source": str(source)}
        finally:
            ctx.target.unmount(handle)
