from __future__ import annotations

from harness.core import Context
from suites.git._deps import ensure_git_source


def ensure_dependencies(ctx: Context) -> None:
    ensure_git_source(ctx)