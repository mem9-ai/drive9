from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from harness.core import Context


class BaseModule:
    id = ""
    category = ""
    description = ""
    labels: tuple[str, ...] = ()
    manual = False
    timeout = 600

    def ensure_dependencies(self, ctx: Context) -> None:
        ctx.deps.ensure_all_for_module(self.id)


def module_config(ctx: Context, module_id: str) -> dict[str, Any]:
    return ctx.config.get("modules", {}).get(module_id, {})


def timeit(fn: Any) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
