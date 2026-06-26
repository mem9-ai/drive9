from __future__ import annotations
from harness.core import Context

def ensure_dependencies(ctx: Context) -> None:
    ctx.deps.ensure_pyxattr()
