from __future__ import annotations

import os
import shutil
from pathlib import Path

from harness.core import Context, DependencyUnavailable, ModuleSkip, env_value, progress, write_json


def ensure_dependencies(ctx: Context) -> None:
    if not ctx.capabilities.get("is_root") and os.environ.get("PJDFSTEST_ALLOW_NONROOT", "1") == "0":
        raise ModuleSkip("pjdfstest requires root (PJDFSTEST_ALLOW_NONROOT=0)", "platform skip")
    ctx.deps.ensure_prove()
    ensure_pjdfstest(ctx)


def ensure_pjdfstest(ctx: Context) -> tuple[Path, Path]:
    """Resolve or fetch+build pjdfstest. Returns (tests_dir, bin_path)."""
    tests = os.environ.get("PJDFSTEST_TESTS", "")
    root = os.environ.get("PJDFSTEST_DIR", "")
    candidates: list[Path] = []
    if tests:
        candidates.append(Path(tests).expanduser())
    if root:
        candidates.append(Path(root).expanduser() / "tests")
    cached_root = ctx.deps.tools_root / "pjdfstest" / os.environ.get("PJDFSTEST_REF", "master")
    candidates.append(cached_root / "tests")
    tests_dir = next((path.resolve() for path in candidates if path.is_dir()), None)
    bin_candidates: list[Path] = []
    if os.environ.get("PJDFSTEST_BIN"):
        bin_candidates.append(Path(os.environ["PJDFSTEST_BIN"]).expanduser())
    if tests_dir is not None:
        bin_candidates.append(tests_dir.parent / "pjdfstest")
    if shutil.which("pjdfstest"):
        bin_candidates.append(Path(shutil.which("pjdfstest") or ""))
    bin_path = next((path.resolve() for path in bin_candidates if path.exists() and os.access(path, os.X_OK)), None)
    if tests_dir is not None and bin_path is not None:
        return tests_dir, bin_path

    if not ctx.deps.auto_fetch:
        raise DependencyUnavailable("pjdfstest is not available and auto-fetch is disabled")
    ctx.deps.ensure_system_packages("git", "build-essential", "autoconf", "automake", "libtool", "pkg-config", "perl")
    ctx.deps.ensure_git_tool()
    ref = os.environ.get("PJDFSTEST_REF", "master")
    root_dir = ctx.deps.ensure_git_clone("pjdfstest", "https://github.com/pjd/pjdfstest.git", ref)
    if not (root_dir / "pjdfstest").exists():
        if (root_dir / "autogen.sh").exists():
            ctx.deps.run("pjdfstest-autogen", ["sh", "autogen.sh"], cwd=root_dir, timeout=600)
        elif (root_dir / "configure.ac").exists() and shutil.which("autoreconf"):
            ctx.deps.run("pjdfstest-autoreconf", ["autoreconf", "-fi"], cwd=root_dir, timeout=600)
        if (root_dir / "configure").exists():
            ctx.deps.run("pjdfstest-configure", ["./configure"], cwd=root_dir, timeout=600)
        ctx.deps.run("pjdfstest-make", ["make"], cwd=root_dir, timeout=1200)
    metadata = {
        "name": "pjdfstest",
        "source": "https://github.com/pjd/pjdfstest",
        "ref": ref,
        "license": "BSD-2-Clause",
    }
    write_json(root_dir / ".drive9-blackbox-dependency.json", metadata)
    return root_dir / "tests", root_dir / "pjdfstest"