from __future__ import annotations

import os
import shutil
from pathlib import Path

from harness.core import Context, DependencyUnavailable, env_value, progress, write_json


def ensure_dependencies(ctx: Context) -> None:
    ensure_fsx(ctx)


def ensure_fsx(ctx: Context) -> str:
    """Resolve or fetch+build fsx (via secfs.test). Returns the fsx binary path."""
    if os.environ.get("FSX_BIN") and Path(os.environ["FSX_BIN"]).exists():
        return os.environ["FSX_BIN"]
    found = shutil.which("fsx")
    if found:
        return found
    if not ctx.deps.auto_fetch:
        raise DependencyUnavailable("fsx is required and auto-fetch is disabled")
    ref = env_value("SECFS_TEST_REF", "master")
    ctx.deps.ensure_system_packages("git", "build-essential", "make")
    ctx.deps.ensure_git_tool()
    root_dir = ctx.deps.ensure_git_clone("secfs.test", "https://github.com/billziss-gh/secfs.test.git", ref)
    candidate = root_dir / "tools" / "bin" / "fsx"
    if not candidate.exists():
        ctx.deps.run("secfs-test-fsx", ["make", "tools/bin/fsx"], cwd=root_dir, timeout=1200)
    if candidate.exists():
        write_json(root_dir / ".drive9-blackbox-dependency.json", {"name": "secfs.test", "source": "https://github.com/billziss-gh/secfs.test", "ref": ref, "license": "Apache-2.0"})
        return str(candidate)
    raise DependencyUnavailable("fsx binary not found after preparing secfs.test")