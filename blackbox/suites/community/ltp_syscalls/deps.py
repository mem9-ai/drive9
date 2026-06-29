"""LTP syscall tests share the LTP dependency logic with ltp_fs."""

from __future__ import annotations

from harness.core import Context
from suites.community.ltp_fs.deps import ensure_ltp  # noqa: F401


def ensure_dependencies(ctx: Context) -> None:
    ensure_ltp(ctx)