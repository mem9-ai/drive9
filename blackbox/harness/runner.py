from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from pathlib import Path
from typing import Any

from .capabilities import detect_capabilities
from .core import (
    CACHE_ROOT,
    FAIL,
    PASS,
    RESULT_ROOT,
    SCHEMA,
    SKIP,
    XFAIL,
    WARN,
    BlackboxError,
    Context,
    DependencyUnavailable,
    ModuleRecord,
    ModuleSkip,
    ModuleXFail,
    Recorder,
    SUITES_DIR,
    env_flag,
    env_value,
    file_ts,
    load_json,
    utc_ts,
    write_json,
)
from .deps import DependencyManager
from .modules import module_registry
from .target import Drive9TargetProvider


class BlackboxRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.suite = normalize_suite_args(args)
        self.suite_config_dir = SUITES_DIR / self.suite
        if not self.suite_config_dir.is_dir():
            raise BlackboxError(f"blackbox suite {self.suite!r} not found at {self.suite_config_dir}")
        self.session = args.session or f"{self.suite}-{file_ts()}"
        self.result_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else RESULT_ROOT / self.suite / self.session
        self.tmp_dir = self.result_dir / "tmp"
        self.recorder = Recorder(self.result_dir)
        self.registry = module_registry()
        self.presets = load_json("presets.json", {}, self.suite_config_dir)
        self.module_config = load_json("modules.json", {}, self.suite_config_dir)
        self.config = {
            "modules": self.module_config.get("modules", {}),
            "groups": self.module_config.get("groups", {}),
            "allowlists": {
                "pjdfstest": load_json("allowlists/pjdfstest.json", load_json("pjdfstests-allowlist.json", {}, self.suite_config_dir), self.suite_config_dir),
            },
            "repos": load_json("repos.json", load_json("cases-perf.json", {}, self.suite_config_dir).get("repos", []), self.suite_config_dir),
        }
        self.capabilities = detect_capabilities()
        self.target = Drive9TargetProvider(args, self.result_dir, self.recorder)
        self.deps = DependencyManager(CACHE_ROOT, auto_fetch=not args.offline, recorder=self.recorder)
        self.ctx = Context(
            args=args,
            session=self.session,
            result_dir=self.result_dir,
            tmp_dir=self.tmp_dir,
            target=self.target,
            deps=self.deps,
            recorder=self.recorder,
            capabilities=self.capabilities,
            config=self.config,
            runs=args.runs or int(env_value("RUNS", "3", self.suite)),
            suite=self.suite,
            selected_preset=args.preset or "",
        )
        self.selected = self.select_modules()

    def list_modules(self) -> int:
        return emit_module_list(self.args.format)

    def select_modules(self) -> list[str]:
        if self.args.list:
            return []
        selected: list[str] = []
        if self.args.module:
            for raw in self.args.module:
                selected.extend(part.strip() for part in raw.split(",") if part.strip())
        elif self.args.category:
            selected = [module.id for module in self.registry.values() if module.id.startswith(self.args.category) or module.category.startswith(self.args.category)]
        elif self.args.group:
            group = self.config.get("groups", {}).get(self.args.group)
            if group is None:
                raise BlackboxError(f"unknown group {self.args.group!r} for suite {self.suite!r}")
            selected = self.expand_module_list(group)
        elif self.args.preset:
            preset = self.presets.get(self.args.preset)
            if not preset:
                raise BlackboxError(f"unknown preset: {self.args.preset}")
            selected = self.expand_module_list(preset.get("modules", []))
        else:
            raise BlackboxError("one of --preset, --category, --module, --group, or --list is required")
        missing = [module_id for module_id in selected if module_id not in self.registry]
        if missing:
            raise BlackboxError(f"unknown module(s): {', '.join(missing)}")
        seen: set[str] = set()
        out: list[str] = []
        for module_id in selected:
            if module_id not in seen:
                out.append(module_id)
                seen.add(module_id)
        return out

    def expand_module_list(self, values: list[str] | str) -> list[str]:
        if values == "all":
            return [module.id for module in sorted(self.registry.values(), key=lambda item: item.id)]
        out: list[str] = []
        for value in values:
            if value.endswith(".*"):
                prefix = value[:-1]
                out.extend(module.id for module in self.registry.values() if module.id.startswith(prefix))
            elif value in self.registry:
                out.append(value)
            else:
                out.extend(module.id for module in self.registry.values() if module.category == value or module.category.startswith(value + "."))
        return out

    def write_manifest(self) -> None:
        manifest: dict[str, Any] = {
            "schema": SCHEMA,
            "suite": self.suite,
            "session": self.session,
            "timestamp": utc_ts(),
            "result_dir": str(self.result_dir),
            "preset": self.args.preset,
            "category": self.args.category,
            "modules": self.selected,
            "runs": self.ctx.runs,
            "server_mode": self.args.server_mode,
            "server_url": self.target.server_url,
            "drive9_cli": str(self.target.cli),
            "platform": platform.platform(),
            "capabilities": self.capabilities,
            "cache_root": str(CACHE_ROOT),
            "suite_config_dir": str(self.suite_config_dir),
        }
        try:
            manifest["git_sha"] = self.target.capture(["git", "rev-parse", "HEAD"], timeout=20).strip()
        except Exception:
            manifest["git_sha"] = "unknown"
        try:
            if self.target.cli.exists():
                manifest["drive9_version"] = self.target.capture([str(self.target.cli), "--version"], timeout=20).strip()
        except Exception as exc:
            manifest["drive9_version_error"] = str(exc)
        write_json(self.result_dir / "manifest.json", manifest)

    def write_report(self) -> None:
        summary = self.recorder.summary()
        lines = [
            f"# Drive9 {self.suite.upper()} Blackbox Report",
            "",
            f"- Suite: `{self.suite}`",
            f"- Session: `{self.session}`",
            f"- Timestamp: `{utc_ts()}`",
            f"- Preset: `{self.args.preset or ''}`",
            f"- Selected modules: `{len(self.selected)}`",
            f"- Platform: `{platform.platform()}`",
            f"- Result dir: `{self.result_dir}`",
            "",
            "## Summary",
            "",
            "| Status | Count |",
            "|---|---:|",
        ]
        for status in (PASS, FAIL, SKIP, XFAIL, WARN):
            lines.append(f"| {status} | {summary.get(status, 0)} |")
        lines.extend(["", "## Modules", "", "| Module | Category | Status | Seconds | Classification | Detail |", "|---|---|---:|---:|---|---|"])
        for record in self.recorder.records:
            detail = record.detail.replace("|", "\\|")[:240]
            lines.append(f"| `{record.module}` | `{record.category}` | {record.status} | {record.seconds:.3f} | {record.classification} | {detail} |")
        metrics_path = self.result_dir / "metrics.json"
        if metrics_path.exists():
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            summaries = metrics.get("summaries", {})
            if summaries:
                lines.extend(["", "## Metrics", "", "| Metric | Unit | Mean | Median | Runs |", "|---|---|---:|---:|---:|"])
                for name, item in sorted(summaries.items()):
                    summary_item = item.get("summary", {})
                    values = summary_item.get("values", [])
                    lines.append(
                        f"| `{name}` | {summary_item.get('unit', '')} | {float(summary_item.get('mean', 0)):.3f} | {float(summary_item.get('median', 0)):.3f} | {len(values)} |"
                    )
        (self.result_dir / "daily-report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def deps_only(self) -> int:
        for module_id in self.selected:
            module = self.registry[module_id]
            start = time.monotonic()
            try:
                module.ensure_dependencies(self.ctx)
                self.recorder.record(ModuleRecord(module=module.id, category=module.category, status=PASS, seconds=time.monotonic() - start, classification="dependency prepared"))
            except DependencyUnavailable as exc:
                self.recorder.record(ModuleRecord(module=module.id, category=module.category, status=SKIP, seconds=time.monotonic() - start, classification=exc.classification, detail=str(exc)))
            except ModuleSkip as exc:
                self.recorder.record(ModuleRecord(module=module.id, category=module.category, status=SKIP, seconds=time.monotonic() - start, classification=exc.classification, detail=str(exc)))
            except Exception as exc:
                self.recorder.record(ModuleRecord(module=module.id, category=module.category, status=FAIL, seconds=time.monotonic() - start, classification="dependency failure", detail=f"{type(exc).__name__}: {exc}"))
                if self.args.fail_fast:
                    raise
        self.write_manifest()
        self.write_report()
        return 1 if self.recorder.has_failures() else 0

    def run_module(self, module_id: str) -> None:
        module = self.registry[module_id]
        start = time.monotonic()
        try:
            module.ensure_dependencies(self.ctx)
            metrics = module.run(self.ctx) or {}
            self.recorder.record(
                ModuleRecord(
                    module=module.id,
                    category=module.category,
                    status=PASS,
                    seconds=time.monotonic() - start,
                    classification="passed",
                    metrics=metrics,
                )
            )
        except DependencyUnavailable as exc:
            self.recorder.record(ModuleRecord(module=module.id, category=module.category, status=SKIP, seconds=time.monotonic() - start, classification=exc.classification, detail=str(exc)))
        except ModuleSkip as exc:
            self.recorder.record(ModuleRecord(module=module.id, category=module.category, status=SKIP, seconds=time.monotonic() - start, classification=exc.classification, detail=str(exc)))
        except ModuleXFail as exc:
            self.recorder.record(ModuleRecord(module=module.id, category=module.category, status=XFAIL, seconds=time.monotonic() - start, classification=exc.classification, detail=str(exc)))
        except BlackboxError as exc:
            self.recorder.record(ModuleRecord(module=module.id, category=module.category, status=FAIL, seconds=time.monotonic() - start, classification="product regression", detail=str(exc)))
            if self.args.fail_fast:
                raise
        except Exception as exc:
            self.recorder.record(
                ModuleRecord(
                    module=module.id,
                    category=module.category,
                    status=FAIL,
                    seconds=time.monotonic() - start,
                    classification="infra failure",
                    detail=f"{type(exc).__name__}: {exc}",
                )
            )
            if self.args.fail_fast:
                raise

    def run(self) -> int:
        self.result_dir.mkdir(parents=True, exist_ok=True)
        if self.args.list:
            return self.list_modules()
        if self.args.deps_only:
            return self.deps_only()
        strict = bool(self.args.strict_prereqs)
        if self.args.preset:
            strict = strict or bool(self.presets.get(self.args.preset, {}).get("strict_prereqs", False))
        if self.suite == "fuse" and not self.capabilities.get("fuse", {}).get("ok"):
            detail = str(self.capabilities.get("fuse", {}).get("detail", "FUSE unavailable"))
            self.recorder.record(ModuleRecord(module="prereq.fuse", category="prereq", status=SKIP, seconds=0, classification="platform skip", detail=detail))
            self.write_manifest()
            self.write_report()
            if strict:
                return 1
            return 0
        try:
            self.target.build_cli()
            self.target.start_server()
            self.write_manifest()
            for module_id in self.selected:
                self.run_module(module_id)
            self.write_manifest()
            self.write_report()
            return 1 if self.recorder.has_failures() else 0
        finally:
            self.write_report()
            self.target.cleanup()


def parse_args(argv: list[str]) -> argparse.Namespace:
    suite_default = os.environ.get("BLACKBOX_SUITE", "fuse")
    parser = argparse.ArgumentParser(description="Run Drive9 blackbox modules.")
    parser.add_argument("--suite", default=suite_default, help="Blackbox suite domain, e.g. fuse, cli, or api. Defaults to BLACKBOX_SUITE or fuse.")
    selector = parser.add_mutually_exclusive_group(required=False)
    selector.add_argument("--preset", help="Run a suite preset, e.g. smoke, standard, or daily.")
    selector.add_argument("--category", help="Run modules whose id/category has this prefix, e.g. community or ported.juicefs")
    selector.add_argument("--module", action="append", help="Run one module id or a comma-separated list. Can be repeated.")
    selector.add_argument("--group", help="Run a named module group from the selected suite, e.g. functional, posix, or perf.")
    selector.add_argument("--list", action="store_true", help="List available modules.")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format for --list.")
    parser.add_argument("--deps-only", action="store_true", help="Prepare external dependencies for selected modules without starting Drive9 or FUSE.")
    parser.add_argument("--runs", type=int, default=0, help="Performance run count. Defaults to BLACKBOX_RUNS, BLACKBOX_<SUITE>_RUNS, or 3.")
    parser.add_argument("--server-mode", choices=["auto", "existing", "local"], default=env_value("SERVER_MODE", "auto", suite_default))
    parser.add_argument("--drive9-cli", default=env_value("DRIVE9_CLI", "", suite_default))
    parser.add_argument("--out-dir", default=env_value("OUT_DIR", "", suite_default))
    parser.add_argument("--session", default=env_value("SESSION", "", suite_default))
    parser.add_argument("--strict-prereqs", action="store_true", default=env_flag("STRICT", False, suite_default))
    parser.add_argument("--offline", action="store_true", default=env_flag("OFFLINE", False, suite_default))
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--keep-artifacts", action="store_true", default=env_flag("KEEP_ARTIFACTS", False, suite_default))
    return parser.parse_args(argv)


def normalize_suite_args(args: argparse.Namespace) -> str:
    return args.suite


def emit_module_list(output_format: str) -> int:
    rows = []
    for module in sorted(module_registry().values(), key=lambda item: item.id):
        rows.append(
            {
                "id": module.id,
                "category": module.category,
                "labels": list(module.labels),
                "description": module.description,
            }
        )
    if output_format == "json":
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0
    for row in rows:
        labels = ",".join(row["labels"])
        print(f"{row['id']}\t{row['category']}\t{labels}\t{row['description']}")
    return 0


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.list:
        return emit_module_list(args.format)
    try:
        runner = BlackboxRunner(args)
        return runner.run()
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
