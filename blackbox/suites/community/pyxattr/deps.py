from __future__ import annotations

import os
import subprocess

from harness.core import Context, DependencyUnavailable, progress


def ensure_dependencies(ctx: Context) -> None:
    ensure_pyxattr(ctx)


def ensure_pyxattr(ctx: Context) -> None:
    """Ensure the Python xattr module is importable.

    Prefers a distro package (python3-xattr / python-xattr) on clean Linux hosts,
    then falls back to ``pip install --target`` into the work-dir cache.
    """
    if _xattr_importable():
        return
    if not ctx.deps.auto_fetch:
        raise DependencyUnavailable(
            "python xattr module is missing and auto-fetch is disabled"
        )
    # Distro packages first (works on Ubuntu and Arch via package-name mapping).
    ctx.deps.ensure_system_packages("python3-xattr", "python3-pip", "python3-dev", "build-essential")
    if _xattr_importable():
        progress("dependency tool: python xattr module available via system packages")
        return

    target = ctx.deps.tools_root / "python" / "pyxattr"
    target.mkdir(parents=True, exist_ok=True)
    # Prefer python3 -m pip; fall back to pip3 if the module is missing.
    pip_cmd = _pip_command()
    ctx.deps.run(
        "pyxattr-pip",
        [*pip_cmd, "install", "--target", str(target), "pyxattr"],
        timeout=1200,
    )
    os.environ["PYTHONPATH"] = f"{target}:{os.environ.get('PYTHONPATH', '')}"
    if not _xattr_importable():
        raise DependencyUnavailable("python xattr module still missing after install")


def _xattr_importable() -> bool:
    proc = subprocess.run(
        ["python3", "-c", "import xattr"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return proc.returncode == 0


def _pip_command() -> list[str]:
    proc = subprocess.run(
        ["python3", "-m", "pip", "--version"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if proc.returncode == 0:
        return ["python3", "-m", "pip"]
    return ["pip3"]
