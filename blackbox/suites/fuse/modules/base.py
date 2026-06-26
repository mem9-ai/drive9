from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from harness.core import Context, ModuleRecord


class BaseModule:
    id = ""
    category = ""
    description = ""
    labels: tuple[str, ...] = ()
    manual = False
    timeout = 600
    # Report profile: "functional", "performance", "compatibility", "customer".
    # Empty string means "infer from labels".
    report_profile = ""
    # Whether this module needs the suite-level provider.setup() to have run
    # (server started, CLI built, etc). Modules that don't need a live server
    # or FUSE mount can set this to False.
    needs_setup = True

    def ensure_dependencies(self, ctx: Context) -> None:
        ctx.deps.ensure_all_for_module(self.id)

    def resolve_report_profile(self) -> str:
        if self.report_profile:
            return self.report_profile
        label_set = set(self.labels)
        if "customer" in label_set:
            return "customer"
        if "performance" in label_set:
            return "performance"
        if "compatibility" in label_set:
            return "compatibility"
        return "functional"

    def render_report(self, ctx: Context, record: ModuleRecord) -> str | None:
        """Return a module-level report markdown string, or None to use the
        framework default template based on ``resolve_report_profile()``."""
        return None


def module_config(ctx: Context, module_id: str) -> dict[str, Any]:
    return ctx.config.get("modules", {}).get(module_id, {})


def timeit(fn: Any) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
