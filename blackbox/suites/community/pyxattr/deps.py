from __future__ import annotations

import os
import subprocess

from harness.core import Context, DependencyUnavailable


def ensure_pyxattr(ctx: Context) -> None:
    """Ensure the Python xattr module is importable (install via pip if needed)."""
    proc = subprocess.run(
        ["python3", "-c", "import xattr"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if proc.returncode == 0:
        return
    if not ctx.deps.auto_fetch:
        raise DependencyUnavailable(
            "python xattr module is missing and auto-fetch is disabled"
        )
    ctx.deps.ensure_system_packages("python3-pip", "python3-dev", "build-essential")
    target = ctx.deps.tools_root / "python" / "pyxattr"
    target.mkdir(parents=True, exist_ok=True)
    ctx.deps.run(
        "pyxattr-pip",
        ["python3", "-m", "pip", "install", "--target", str(target), "pyxattr"],
        timeout=1200,
    )
    os.environ["PYTHONPATH"] = f"{target}:{os.environ.get('PYTHONPATH', '')}"
