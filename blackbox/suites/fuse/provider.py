from __future__ import annotations

from pathlib import Path
from typing import Any

from harness.core import CACHE_ROOT, SKIP, Context, ModuleRecord, Recorder, load_json

from .capabilities import detect_capabilities
from .deps import FuseDependencyManager
from .modules import module_registry
from .target import Drive9FuseTargetProvider


class FuseSuiteProvider:
    def __init__(self, *, suite: str, config_dir: Path) -> None:
        self.suite = suite
        self.config_dir = config_dir

    def module_registry(self) -> dict[str, Any]:
        return module_registry()

    def load_config(self) -> dict[str, Any]:
        module_config = load_json("modules.json", {}, self.config_dir)
        return {
            "modules": module_config.get("modules", {}),
            "groups": module_config.get("groups", {}),
            "allowlists": {
                "pjdfstest": load_json("allowlists/pjdfstest.json", load_json("pjdfstests-allowlist.json", {}, self.config_dir), self.config_dir),
            },
            "repos": load_json("repos.json", load_json("cases-perf.json", {}, self.config_dir).get("repos", []), self.config_dir),
        }

    def detect_capabilities(self) -> dict[str, Any]:
        return detect_capabilities()

    def create_deps(self, *, auto_fetch: bool, recorder: Recorder) -> FuseDependencyManager:
        return FuseDependencyManager(CACHE_ROOT, auto_fetch=auto_fetch, recorder=recorder)

    def create_target(self, args: Any, result_dir: Path, recorder: Recorder, *, session: str) -> Drive9FuseTargetProvider:
        return Drive9FuseTargetProvider(args, result_dir, recorder, suite=self.suite, session=session)

    def check_prerequisites(self, ctx: Context) -> list[ModuleRecord]:
        if ctx.capabilities.get("fuse", {}).get("ok"):
            return []
        detail = str(ctx.capabilities.get("fuse", {}).get("detail", "FUSE unavailable"))
        return [ModuleRecord(module="prereq.fuse", category="prereq", status=SKIP, seconds=0, classification="platform skip", detail=detail)]

    def setup(self, ctx: Context) -> None:
        ctx.target.build_cli()
        ctx.target.start_server()

    def cleanup(self, ctx: Context) -> None:
        ctx.target.cleanup()

    def manifest_fields(self, ctx: Context) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "server_mode": ctx.args.server_mode,
            "server_url": ctx.target.server_url,
            "drive9_cli": str(ctx.target.cli),
        }
        try:
            if ctx.target.cli.exists():
                fields["drive9_version"] = ctx.target.capture([str(ctx.target.cli), "--version"], timeout=20).strip()
        except Exception as exc:
            fields["drive9_version_error"] = str(exc)
        return fields

    def render_suite_report(self, ctx: Context, records: list[ModuleRecord]) -> str | None:
        return None

    def suite_goals(self) -> str:
        return (
            "The FUSE suite validates Drive9's FUSE filesystem across POSIX compliance, "
            "performance, workflow correctness, and customer scenario fidelity. Modules "
            "cover community test suites (pjdfstest, LTP, fio, mdtest), ported JuiceFS "
            "stress tests, official Git functional/perf tests, Drive9 workflow scenarios, "
            "and customer workspace benchmarks."
        )


def create_provider(*, suite: str, config_dir: Path) -> FuseSuiteProvider:
    return FuseSuiteProvider(suite=suite, config_dir=config_dir)
