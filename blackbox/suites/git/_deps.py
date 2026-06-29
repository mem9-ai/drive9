"""Shared git source dependency for git.official.* modules."""

from __future__ import annotations

import os
from pathlib import Path

from harness.core import Context, DependencyUnavailable, env_value, write_json


def ensure_git_source(ctx: Context) -> Path:
    """Resolve or fetch+build the upstream git source tree. Returns the source root."""
    ctx.deps.ensure_git_tool()
    if os.environ.get("GIT_TEST_SOURCE_DIR"):
        path = Path(os.environ["GIT_TEST_SOURCE_DIR"]).expanduser().resolve()
        if (path / "t").is_dir():
            _ensure_git_test_build(ctx, path)
            return path
    ref = env_value("GIT_TEST_REF", "v2.49.0")
    root_dir = ctx.deps.ensure_git_clone("git", "https://github.com/git/git.git", ref)
    _ensure_git_test_build(ctx, root_dir)
    write_json(
        root_dir / ".drive9-blackbox-dependency.json",
        {"name": "git", "source": "https://github.com/git/git", "ref": ref, "license": "GPL-2.0-only"},
    )
    return root_dir


def _ensure_git_test_build(ctx: Context, root_dir: Path) -> None:
    if (root_dir / "GIT-BUILD-OPTIONS").exists() and (root_dir / "bin-wrappers" / "git").exists() and (root_dir / "t" / "helper" / "test-tool").exists():
        return
    if not ctx.deps.auto_fetch:
        raise DependencyUnavailable("Git source is not built and auto-fetch is disabled")
    ctx.deps.ensure_system_packages("build-essential", "gettext", "libcurl4-openssl-dev", "libssl-dev", "make", "perl", "zlib1g-dev")
    ctx.deps.run("git-build", ["make", "-j2"], cwd=root_dir, timeout=int(os.environ.get("GIT_TEST_BUILD_TIMEOUT_S", "1800")))