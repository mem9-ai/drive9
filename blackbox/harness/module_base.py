from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .core import Context, ModuleRecord


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
        """Override in module to prepare module-specific dependencies.
        Default: try to import and call deps.py from the module directory."""
        import importlib
        # Derive module directory from module_id
        parts = self.id.split(".", 1)
        if len(parts) == 2:
            group, name = parts
            try:
                deps_mod = importlib.import_module(f"suites.{group}.{name}.deps")
                if hasattr(deps_mod, "ensure_dependencies"):
                    deps_mod.ensure_dependencies(ctx)
                    return
            except ModuleNotFoundError:
                pass  # No deps.py — module has no special dependencies
        # Fallback: call legacy ensure_all_for_module if deps manager supports it
        if hasattr(ctx.deps, "ensure_all_for_module"):
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
    """Load config.json from the module's own directory."""
    import json
    from pathlib import Path
    parts = module_id.split(".", 1)
    if len(parts) == 2:
        config_path = Path(__file__).resolve().parent.parent / "suites" / parts[0] / parts[1] / "config.json"
        if config_path.exists():
            with open(config_path) as f:
                return json.load(f)
    return {}


def timeit(fn: Any) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
