from __future__ import annotations

import os
import shutil
from pathlib import Path

from harness.core import Context, DependencyUnavailable


def ensure_dependencies(ctx: Context) -> None:
    ensure_vdbench(ctx)


def ensure_vdbench(ctx: Context) -> str:
    """Resolve vdbench binary. vdbench is never auto-fetched (Oracle download)."""
    if os.environ.get("VDBENCH_BIN") and Path(os.environ["VDBENCH_BIN"]).exists():
        return os.environ["VDBENCH_BIN"]
    found = shutil.which("vdbench")
    if found:
        return found
    raise DependencyUnavailable("vdbench is required; set VDBENCH_BIN or put vdbench on PATH")