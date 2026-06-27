from __future__ import annotations

import shutil
from pathlib import Path

from harness.core import BlackboxError, Context, DependencyUnavailable
from harness.module_base import BaseModule


class Drive9WorkflowBase(BaseModule):
    category = "drive9.workflow"
    labels = ("drive9", "workflow", "functional")
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

    def ensure_corepack_shims(self, corepack_home: Path) -> None:
        """Create pnpm/npm/npx shims in corepack_home that delegate to corepack.

        Node 24's corepack (0.34+) stores package managers under
        ``COREPACK_HOME/v1/`` and does not create a top-level ``pnpm`` shim,
        so build scripts that invoke ``pnpm`` directly (not via ``corepack
        pnpm``) fail with ``pnpm: not found``. These shims close that gap.
        """
        corepack_home.mkdir(parents=True, exist_ok=True)
        for tool in ("pnpm", "npm", "npx"):
            shim = corepack_home / tool
            if shim.exists():
                continue
            shim.write_text(
                "#!/bin/sh\n"
                f'exec corepack {tool} "$@"\n',
                encoding="utf-8",
            )
            shim.chmod(0o755)
