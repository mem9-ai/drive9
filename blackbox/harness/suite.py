from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any, Protocol

from .core import BlackboxError, Context, ModuleRecord, Recorder, SUITES_DIR


class SuiteProvider(Protocol):
    suite: str
    config_dir: Path

    def module_registry(self) -> dict[str, Any]:
        ...

    def load_config(self) -> dict[str, Any]:
        ...

    def detect_capabilities(self) -> dict[str, Any]:
        ...

    def create_deps(self, *, auto_fetch: bool, recorder: Recorder) -> Any:
        ...

    def create_target(self, args: Any, result_dir: Path, recorder: Recorder, *, session: str) -> Any:
        ...

    def check_prerequisites(self, ctx: Context) -> list[ModuleRecord]:
        ...

    def setup(self, ctx: Context) -> None:
        ...

    def cleanup(self, ctx: Context) -> None:
        ...

    def manifest_fields(self, ctx: Context) -> dict[str, Any]:
        ...

    def render_suite_report(self, ctx: Context, records: list[ModuleRecord]) -> str | None:
        ...

    def suite_goals(self) -> str:
        ...


def load_suite_provider(suite: str, config_dir: Path) -> SuiteProvider:
    """Load the environment provider from env/provider.py."""
    module_name = "env.provider"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name == module_name:
            raise BlackboxError(
                f"blackbox env provider not found at {module_name}"
            ) from exc
        raise
    factory = getattr(module, "create_provider", None)
    if callable(factory):
        return factory(suite=suite, config_dir=config_dir)
    provider_class = getattr(module, "SuiteProvider", None)
    if provider_class is None:
        raise BlackboxError(
            "env.provider must expose create_provider() or SuiteProvider"
        )
    return provider_class(suite=suite, config_dir=config_dir)


def discover_modules() -> dict[str, Any]:
    """Auto-discover all modules under suites/<group>/<module>/module.py.

    Returns a dict mapping module_id -> module instance.
    """
    if not SUITES_DIR.is_dir():
        return {}
    modules: dict[str, Any] = {}
    for group_dir in sorted(SUITES_DIR.iterdir()):
        if not group_dir.is_dir() or group_dir.name.startswith("_") or group_dir.name.startswith("."):
            continue
        # Skip top-level non-module files (e.g., __init__.py, README.md).
        if (group_dir / "module.py").exists():
            continue
        for module_dir in sorted(group_dir.iterdir()):
            if not module_dir.is_dir() or module_dir.name.startswith("_") or module_dir.name.startswith("."):
                continue
            module_py = module_dir / "module.py"
            if not module_py.exists():
                continue
            module_id_prefix = f"{group_dir.name}.{module_dir.name}"
            mod_name = f"suites.{group_dir.name}.{module_dir.name}.module"
            try:
                mod = importlib.import_module(mod_name)
            except Exception as exc:
                # Store the error so the runner can report it
                modules[module_id_prefix] = _ImportError(module_id_prefix, mod_name, exc)
                continue
            # Find all module classes in the file — a single module.py can
            # export multiple test classes (e.g., ltp/module.py has LTPFS + LTPSyscalls).
            # Each class is registered by its own `id` attribute, not by directory path.
            found_any = False
            for attr_name in dir(mod):
                obj = getattr(mod, attr_name)
                if isinstance(obj, type) and hasattr(obj, "id") and hasattr(obj, "run") and hasattr(obj, "category"):
                    instance = obj()
                    # Record the actual directory so module_config() can locate
                    # config.json even when the id has more dotted segments than
                    # the directory nesting (e.g. drive9.workflow.local_overlay_build
                    # lives under suites/drive9/local_overlay_build/).
                    instance._module_dir = module_dir
                    modules[instance.id] = instance
                    found_any = True
            if not found_any:
                modules[module_id_prefix] = _ImportError(module_id_prefix, mod_name, "no module class found")
    return modules


def load_module_config(module_dir: Path) -> dict[str, Any]:
    """Load config.json from a module directory."""
    config_path = module_dir / "config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {}


def discover_suites() -> list[str]:
    """Return sorted list of group names found under suites/."""
    if not SUITES_DIR.is_dir():
        return []
    groups: list[str] = []
    for entry in sorted(SUITES_DIR.iterdir()):
        if entry.is_dir() and not entry.name.startswith("_") and not entry.name.startswith("."):
            # Check if this directory contains module subdirectories
            has_modules = any(
                (entry / sub / "module.py").exists()
                for sub in entry.iterdir()
                if sub.is_dir()
            )
            if has_modules:
                groups.append(entry.name)
    return groups


class _ImportError:
    """Placeholder for a module that failed to import."""
    def __init__(self, module_id: str, mod_name: str, exc: Any) -> None:
        self.id = module_id
        self.category = "import_error"
        self.description = f"Failed to import {mod_name}: {exc}"
        self.labels: tuple[str, ...] = ()
        self.manual = False
        self.timeout = 0
        self.report_profile = ""
        self.needs_setup = False
        self._exc = exc
