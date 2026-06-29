from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .core import Context, ModuleRecord


class BaseModule:
    _id: str = ""
    _module_dir: Path | None = None
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

    @property
    def id(self) -> str:
        return self._id

    def ensure_dependencies(self, ctx: Context) -> None:
        """Override in module to prepare module-specific dependencies.
        Default: try to import and call deps.py from the module directory."""
        import importlib
        if self._module_dir is not None:
            # Import the deps module that lives alongside this module's module.py.
            rel = self._module_dir.relative_to(Path(__file__).resolve().parent.parent / "suites")
            parts = rel.parts
            if len(parts) >= 2:
                mod_name = ".".join(["suites", *parts, "deps"])
                try:
                    deps_mod = importlib.import_module(mod_name)
                    if hasattr(deps_mod, "ensure_dependencies"):
                        deps_mod.ensure_dependencies(ctx)
                        return
                except ModuleNotFoundError:
                    pass  # No deps.py — module has no special dependencies

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
    """Load config.json from the module's own directory.

    The module directory is resolved in priority order:
    1. The ``_module_dir`` attribute set by ``discover_modules`` on each
       module instance (authoritative — handles ids with more dotted
       segments than directory nesting, e.g. ``drive9.workflow.foo``
       living under ``suites/drive9/foo/``).
    2. The registry attached to ``ctx.config`` (populated by the runner).
    3. A legacy split on the first dot (``group.rest`` → ``suites/group/rest``),
       kept as a last-resort fallback.
    """
    import json
    from pathlib import Path

    registry = ctx.config.get("registry", {}) if ctx else {}
    instance = registry.get(module_id)
    module_dir = getattr(instance, "_module_dir", None)
    if module_dir is not None:
        config_path = module_dir / "config.json"
        if config_path.exists():
            with open(config_path) as f:
                return json.load(f)
        return {}

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
