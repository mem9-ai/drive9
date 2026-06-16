from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, Protocol

from .core import BlackboxError, Context, ModuleRecord, Recorder


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


def load_suite_provider(suite: str, config_dir: Path) -> SuiteProvider:
    module_name = f"suites.{suite}.provider"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name == module_name:
            raise BlackboxError(f"blackbox suite {suite!r} does not provide provider module {module_name}") from exc
        raise
    factory = getattr(module, "create_provider", None)
    if callable(factory):
        return factory(suite=suite, config_dir=config_dir)
    provider_class = getattr(module, "SuiteProvider", None)
    if provider_class is None:
        raise BlackboxError(f"blackbox suite {suite!r} provider must expose create_provider() or SuiteProvider")
    return provider_class(suite=suite, config_dir=config_dir)
