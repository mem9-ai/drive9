from __future__ import annotations

import os
import shutil
from pathlib import Path

from harness.core import Context, DependencyUnavailable, env_value, progress, write_json

DEFAULT_FIO_REF = "fio-3.42"


def ensure_dependencies(ctx: Context) -> None:
    ensure_fio(ctx)


def ensure_fio(ctx: Context) -> str:
    """Resolve or fetch+build fio. Returns the fio binary path."""
    if os.environ.get("FIO_BIN") and Path(os.environ["FIO_BIN"]).exists():
        return os.environ["FIO_BIN"]
    found = shutil.which("fio")
    if found:
        progress(f"dependency tool: fio -> {found}")
        return found
    if not ctx.deps.auto_fetch:
        raise DependencyUnavailable("fio is required and auto-fetch is disabled")
    ctx.deps.ensure_system_packages("git", "build-essential", "pkg-config")
    ctx.deps.ensure_git_tool()
    ref = env_value("FIO_REF", DEFAULT_FIO_REF)
    root_dir = ctx.deps.ensure_git_clone("fio", "https://github.com/axboe/fio.git", ref)
    candidate = root_dir / "fio"
    if not (candidate.exists() and os.access(candidate, os.X_OK)):
        if (root_dir / "configure").exists():
            ctx.deps.run("fio-configure", ["./configure"], cwd=root_dir, timeout=600)
        jobs = env_value("FIO_MAKE_JOBS", env_value("MAKE_JOBS", "2"))
        ctx.deps.run("fio-make", ["make", f"-j{jobs}"], cwd=root_dir, timeout=int(env_value("FIO_BUILD_TIMEOUT_S", "1800")))
    if candidate.exists() and os.access(candidate, os.X_OK):
        write_json(
            root_dir / ".drive9-blackbox-dependency.json",
            {"name": "fio", "source": "https://github.com/axboe/fio", "ref": ref, "license": "GPL-2.0-only"},
        )
        return str(candidate)
    raise DependencyUnavailable(f"fio binary not found after build: {candidate}")