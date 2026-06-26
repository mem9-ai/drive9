from __future__ import annotations

import shutil
from pathlib import Path

from harness.core import BlackboxError, Context, DependencyUnavailable
from harness.module_base import BaseModule


class Drive9WorkflowBase(BaseModule):
    category = "drive9.workflow"
    labels = ("drive9", "workflow")
    timeout = 1800

    def ensure_dependencies(self, ctx: Context) -> None:
        if not shutil.which("git"):
            raise DependencyUnavailable("git is required")

    def configure_git(self, ctx: Context, repo: Path) -> None:
        ctx.target.capture(["git", "config", "user.name", "Drive9 Blackbox"], cwd=repo)
        ctx.target.capture(["git", "config", "user.email", "blackbox@drive9.local"], cwd=repo)

    def assert_clean(self, ctx: Context, repo: Path) -> None:
        status = ctx.target.capture(["git", "status", "--porcelain"], cwd=repo).strip()
        if status:
            raise BlackboxError(f"git status not clean: {status}")
