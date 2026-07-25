from __future__ import annotations

import os
import shutil
from pathlib import Path

from harness.core import Context, DependencyUnavailable, env_value, progress, write_json

_FSX_STRLCPY_PATCH_MARKER = "DRIVE9_STRLCPY_PATCH"


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
        _patch_fsx_for_glibc_strlcpy(root_dir)
        ctx.deps.run("secfs-test-tools", ["make", "tools"], cwd=root_dir, timeout=1200)
        ctx.deps.run("secfs-test-fsx", ["make", "tools/bin/fsx"], cwd=root_dir, timeout=1200)
    if candidate.exists():
        write_json(root_dir / ".drive9-blackbox-dependency.json", {"name": "secfs.test", "source": "https://github.com/billziss-gh/secfs.test", "ref": ref, "license": "Apache-2.0"})
        return str(candidate)
    raise DependencyUnavailable("fsx binary not found after preparing secfs.test")


def _patch_fsx_for_glibc_strlcpy(root_dir: Path) -> None:
    """Skip bundled strlcpy/strlcat on Linux when glibc already provides them.

    secfs.test defines static strlcpy/strlcat under ``#if defined(__linux__)``,
    which fails to compile on glibc that exports those symbols (Arch, recent
    Fedora/Ubuntu). Prefer the libc implementations on Linux.
    """
    candidates = (
        root_dir / "fstools" / "src" / "fsx" / "fsx.c",
        root_dir / "tools" / "fsx" / "fsx.c",
    )
    fsx_c = next((path for path in candidates if path.is_file()), None)
    if fsx_c is None:
        return
    text = fsx_c.read_text(encoding="utf-8", errors="replace")
    if _FSX_STRLCPY_PATCH_MARKER in text:
        return
    old = "#if defined(_WIN64) || defined(__linux__)"
    if old not in text:
        # Fall back: rename local definitions if the guard text moved.
        if "strlcpy(char *dst, const char *src, size_t maxlen)" not in text:
            return
        progress(f"dependency patch: {fsx_c.relative_to(root_dir)} rename bundled strlcpy/strlcat")
        text = text.replace(
            "strlcpy(char *dst, const char *src, size_t maxlen)",
            "drive9_unused_strlcpy(char *dst, const char *src, size_t maxlen)",
            1,
        )
        text = text.replace(
            "strlcat(char *dst, const char *src, size_t maxlen)",
            "drive9_unused_strlcat(char *dst, const char *src, size_t maxlen)",
            1,
        )
        text = f"/* {_FSX_STRLCPY_PATCH_MARKER} */\n" + text
        fsx_c.write_text(text, encoding="utf-8")
        return
    progress(f"dependency patch: {fsx_c.relative_to(root_dir)} use libc strlcpy/strlcat on Linux")
    text = text.replace(
        old,
        f"/* {_FSX_STRLCPY_PATCH_MARKER}: glibc provides strlcpy/strlcat */\n"
        f"#if defined(_WIN64) /* was: _WIN64 || __linux__ */",
        1,
    )
    fsx_c.write_text(text, encoding="utf-8")
