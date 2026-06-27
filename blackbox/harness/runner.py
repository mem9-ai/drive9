from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from pathlib import Path
from typing import Any, Callable

from .core import (
    CLASS_PLATFORM_DARWIN,
    CLASS_PLATFORM_LINUX,
    CLASS_TIMEOUT,
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
    progress,
    resolve_module_timeout,
    utc_ts,
    write_json,
)
from .suite import load_suite_provider, discover_modules, discover_suites
from . import report as report_engine


class BlackboxRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.session = args.session or file_ts()
        # Work-dir isolation: all writable state (cache, tmp, results) lives
        # under work_dir so a run never pollutes the repo tree.
        work_dir_raw = args.work_dir or os.environ.get("BLACKBOX_WORK_DIR", "")
        if work_dir_raw:
            self.work_dir = Path(work_dir_raw).expanduser().resolve()
        else:
            self.work_dir = RESULT_ROOT.parent / "work" / self.session
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir = self.work_dir / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if args.out_dir:
            self.result_dir = Path(args.out_dir).expanduser().resolve()
        else:
            self.result_dir = self.work_dir / "results"
        self.result_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir = self.result_dir / "tmp"
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        # Set GOCACHE/GOMODCACHE under work_dir if not already set.
        os.environ.setdefault("GOCACHE", str(self.work_dir / "gocache"))
        os.environ.setdefault("GOMODCACHE", str(self.work_dir / "gomodcache"))
        self.recorder = Recorder(self.result_dir)
        self.provider = load_suite_provider("", SUITES_DIR)
        self.registry = discover_modules()
        self.config = {"modules": {}, "registry": self.registry}
        self.capabilities = self.provider.detect_capabilities()
        self.target = self.provider.create_target(args, self.result_dir, self.recorder, session=self.session)
        # Pass the work_dir-based cache_root to the dependency manager.
        self.deps = self.provider.create_deps(auto_fetch=not args.offline, recorder=self.recorder)
        self.deps.cache_root = self.cache_dir
        self.deps.tools_root = self.cache_dir / "tools"
        self.deps.tools_root.mkdir(parents=True, exist_ok=True)
        self.summary_printed = False
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
            runs=args.runs or int(env_value("RUNS", "1", "")),
            suite="",
        )
        self.selected = self.select_modules()

    def select_modules(self) -> list[str]:
        selected: list[str] = []
        if self.args.module:
            for raw in self.args.module:
                selected.extend(part.strip() for part in raw.split(",") if part.strip())
        elif self.args.all:
            selected = self.expand_module_list("all")
        elif self.args.group:
            # --group <name> selects a directory group: every module whose id
            # starts with "<group>.", mirroring the suites/<group>/ layout.
            prefix = f"{self.args.group}."
            selected = [module.id for module in self.registry.values() if module.id.startswith(prefix)]
            if not selected:
                raise BlackboxError(f"unknown group {self.args.group!r} (no modules under suites/{self.args.group}/)")
        else:
            raise BlackboxError("one of --all, --module, or --group is required")
        # --label is an optional overlay filter (combinable with any selector).
        labels = self._requested_labels()
        if labels:
            selected = [module_id for module_id in selected if labels & set(getattr(self.registry[module_id], "labels", ()))]
        missing = [module_id for module_id in selected if module_id not in self.registry]
        if missing:
            raise BlackboxError(f"unknown module(s): {', '.join(missing)}")
        seen: set[str] = set()
        out: list[str] = []
        for module_id in selected:
            if module_id not in seen:
                out.append(module_id)
                seen.add(module_id)
        if not self.args.module and not env_flag("INCLUDE_MANUAL", False, ""):
            out = [module_id for module_id in out if not getattr(self.registry[module_id], "manual", False)]
        return out

    def _requested_labels(self) -> set[str]:
        if not self.args.label:
            return set()
        labels: set[str] = set()
        for raw in self.args.label:
            labels.update(part.strip() for part in raw.split(",") if part.strip())
        return labels

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
        return out

    def write_manifest(self) -> None:
        manifest: dict[str, Any] = {
            "schema": SCHEMA,
            "suite": "",
            "session": self.session,
            "timestamp": utc_ts(),
            "result_dir": str(self.result_dir),
            "selector": {
                "all": bool(self.args.all),
                "group": self.args.group,
                "module": self.args.module,
            },
            "modules": self.selected,
            "runs": self.ctx.runs,
            "platform": platform.platform(),
            "capabilities": self.capabilities,
            "suite_config_dir": str(SUITES_DIR),
        }
        manifest.update(self.provider.manifest_fields(self.ctx))
        try:
            proc = subprocess.run(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=20, check=False)
            manifest["git_sha"] = proc.stdout.strip() if proc.returncode == 0 else "unknown"
        except Exception:
            manifest["git_sha"] = "unknown"
        write_json(self.result_dir / "manifest.json", manifest)

    def write_module_report(self, module: Any, record: ModuleRecord) -> None:
        """Generate a per-module report (markdown + JSON) under artifacts/<id>/."""
        artifact_dir = self.ctx.artifact_dir(module.id)
        # Module custom report, else framework template by profile.
        custom = None
        if hasattr(module, "render_report"):
            try:
                custom = module.render_report(self.ctx, record)
            except Exception as exc:
                progress(f"module report render error: {module.id}: {exc}")
        if custom is not None:
            markdown = custom
        else:
            markdown = report_engine.render_module_report(self.ctx, module, record)
        report_path = artifact_dir / "report.md"
        report_path.write_text(markdown, encoding="utf-8")
        json_path = artifact_dir / "report.json"
        write_json(
            json_path,
            {
                "schema": "drive9-blackbox-module-report/v1",
                "module": module.id,
                "status": record.status,
                "seconds": record.seconds,
                "classification": record.classification,
                "detail": record.detail,
                "session": self.session,
                "timestamp": utc_ts(),
            },
        )

    def write_suite_report(self) -> Path:
        """Generate the suite-level report (markdown + JSON)."""
        records = list(self.recorder.records)
        goals = ""
        if hasattr(self.provider, "suite_goals"):
            try:
                goals = self.provider.suite_goals()
            except Exception:
                goals = ""
        custom = None
        if hasattr(self.provider, "render_suite_report"):
            try:
                custom = self.provider.render_suite_report(self.ctx, records)
            except Exception as exc:
                progress(f"suite report render error: {exc}")
        if custom is not None:
            markdown = custom
        else:
            markdown = report_engine.render_suite_report(self.ctx, "", records, goals)
        report_path = self.result_dir / "report.md"
        report_path.write_text(markdown, encoding="utf-8")
        summary = self.recorder.summary()
        write_json(
            self.result_dir / "suite-report.json",
            {
                "schema": "drive9-blackbox-suite-report/v1",
                "suite": "",
                "session": self.session,
                "timestamp": utc_ts(),
                "summary": summary,
                "records": [r.__dict__ for r in records],
            },
        )
        return report_path

    def finish_report(self) -> None:
        report_path = self.write_suite_report()
        if self.summary_printed:
            return
        summary = self.recorder.summary()
        counts = " ".join(f"{status}={summary.get(status, 0)}" for status in (PASS, FAIL, SKIP, XFAIL, WARN))
        print(f"blackbox summary: {counts}", flush=True)
        print(f"blackbox report: {report_path}", flush=True)
        self.summary_printed = True

    def bootstrap(self) -> int:
        """Prepare dependencies into the work-dir, then exit.

        Equivalent to ``--deps-only`` but with explicit work-dir printing so
        the caller can reuse it with ``--work-dir <path>`` for subsequent runs.
        """
        progress(f"work dir: {self.work_dir}")
        result = self.deps_only()
        print(f"blackbox work-dir: {self.work_dir}", flush=True)
        print(f"blackbox cache: {self.cache_dir}", flush=True)
        return result

    def deps_only(self) -> int:
        progress(f"result dir: {self.result_dir}")
        progress(f"preparing dependencies for {len(self.selected)} module(s)")
        total = len(self.selected)
        for idx, module_id in enumerate(self.selected, start=1):
            module = self.registry[module_id]
            start = time.monotonic()
            progress(f"deps {idx}/{total} start: {module.id}")
            # Platform compatibility check (modules.json compat field).
            compat_record = self.check_platform_compat(module)
            if compat_record is not None:
                compat_record.report_profile = self._module_profile(module)
                self.recorder.record(compat_record)
                progress(f"deps {idx}/{total} {compat_record.status}: {module.id} ({compat_record.classification})")
                self.write_module_report(module, compat_record)
                continue
            try:
                module.ensure_dependencies(self.ctx)
                record = ModuleRecord(module=module.id, status=PASS, seconds=time.monotonic() - start, classification="dependency prepared")
            except DependencyUnavailable as exc:
                record = ModuleRecord(module=module.id, status=SKIP, seconds=time.monotonic() - start, classification=exc.classification, detail=str(exc))
            except ModuleSkip as exc:
                record = ModuleRecord(module=module.id, status=SKIP, seconds=time.monotonic() - start, classification=exc.classification, detail=str(exc))
            except Exception as exc:
                record = ModuleRecord(module=module.id, status=FAIL, seconds=time.monotonic() - start, classification="dependency failure", detail=f"{type(exc).__name__}: {exc}")
                record.report_profile = self._module_profile(module)
                self.recorder.record(record)
                progress(f"deps {idx}/{total} {record.status}: {module.id} in {record.seconds:.1f}s ({record.classification})")
                self.write_module_report(module, record)
                if self.args.fail_fast:
                    raise
                continue
            record.report_profile = self._module_profile(module)
            self.recorder.record(record)
            progress(f"deps {idx}/{total} {record.status}: {module.id} in {record.seconds:.1f}s ({record.classification})")
            self.write_module_report(module, record)
        self.write_manifest()
        self.finish_report()
        return 1 if self.recorder.has_failures() else 0

    def resolve_timeout(self, module: Any) -> int:
        return resolve_module_timeout(
            module.id,
            getattr(module, "timeout", 600),
            self.ctx.suite,
            self.config.get("modules", {}).get(module.id),
        )

    def check_platform_compat(self, module: Any) -> ModuleRecord | None:
        """Check platform compatibility from modules.json compat field.

        Returns a SKIP ModuleRecord if the module should not run on this
        platform, or None to proceed (including xfail, which runs normally).
        """
        import platform as _platform

        module_cfg = self.config.get("modules", {}).get(module.id, {})
        compat = module_cfg.get("compat", {})
        if not compat:
            return None
        os_name = _platform.system()
        expectation = compat.get(os_name.lower(), "run")
        if expectation == "skip":
            classification = CLASS_PLATFORM_DARWIN if os_name == "Darwin" else CLASS_PLATFORM_LINUX
            return ModuleRecord(
                module=module.id,
                status=SKIP,
                seconds=0.0,
                classification=classification,
                detail=f"module is {os_name}-incompatible per compat matrix",
            )
        return None

    def expects_xfail(self, module: Any) -> bool:
        """Whether this module is expected to fail on the current platform."""
        import platform as _platform

        module_cfg = self.config.get("modules", {}).get(module.id, {})
        compat = module_cfg.get("compat", {})
        if not compat:
            return False
        os_name = _platform.system()
        return compat.get(os_name.lower(), "run") == "xfail"

    @staticmethod
    def _module_profile(module: Any) -> str:
        if hasattr(module, "resolve_report_profile"):
            try:
                return module.resolve_report_profile()
            except Exception:
                pass
        return "functional"

    def execute_module(self, module: Any) -> ModuleRecord:
        """Run ensure_dependencies + run, returning a ModuleRecord.

        Wrapped in a ThreadPoolExecutor with a wall-clock timeout so a hung
        module cannot block the entire run. On timeout the record is FAIL with
        classification ``timeout``; the background thread is left to wind down
        on its own (module sub-commands have their own per-command timeouts).
        """
        start = time.monotonic()
        timeout = self.resolve_timeout(module)

        def body() -> ModuleRecord:
            self.ctx.recorder.event({"type": "module", "phase": "deps-start", "module": module.id})
            module.ensure_dependencies(self.ctx)
            self.ctx.recorder.event({"type": "module", "phase": "deps-end", "module": module.id})
            self.ctx.recorder.event({"type": "module", "phase": "run-start", "module": module.id})
            metrics = module.run(self.ctx) or {}
            self.ctx.recorder.event({"type": "module", "phase": "run-end", "module": module.id, "status": PASS})
            return ModuleRecord(
                module=module.id,
                status=PASS,
                seconds=time.monotonic() - start,
                classification="passed",
                metrics=metrics,
            )

        try:
            pool = ThreadPoolExecutor(max_workers=1)
            future = pool.submit(body)
            record = future.result(timeout=timeout)
        except FutureTimeoutError:
            elapsed = time.monotonic() - start
            record = ModuleRecord(
                module=module.id,
                status=FAIL,
                seconds=elapsed,
                classification=CLASS_TIMEOUT,
                detail=f"module exceeded wall-clock timeout of {timeout}s after {elapsed:.1f}s",
            )
            self.ctx.recorder.event({"type": "module", "phase": "timeout", "module": module.id, "timeout_s": timeout, "elapsed_s": elapsed})
        finally:
            # Do not wait for the background thread to finish. On timeout the
            # thread is left running (it winds down via per-command timeouts);
            # waiting would block the runner and defeat the purpose of the
            # wall-clock guard.
            pool.shutdown(wait=False)
        return record

    def run_module(self, module_id: str, *, index: int, total: int) -> None:
        module = self.registry[module_id]
        start = time.monotonic()
        progress(f"module {index}/{total} start: {module.id}")
        # Platform compatibility check (modules.json compat field).
        compat_record = self.check_platform_compat(module)
        if compat_record is not None:
            compat_record.report_profile = self._module_profile(module)
            self.recorder.record(compat_record)
            progress(f"module {index}/{total} {compat_record.status}: {module.id} ({compat_record.classification})")
            self.write_module_report(module, compat_record)
            return
        record: ModuleRecord | None = None
        re_raise: BaseException | None = None
        try:
            record = self.execute_module(module)
        except DependencyUnavailable as exc:
            record = ModuleRecord(module=module.id, status=SKIP, seconds=time.monotonic() - start, classification=exc.classification, detail=str(exc))
        except ModuleSkip as exc:
            record = ModuleRecord(module=module.id, status=SKIP, seconds=time.monotonic() - start, classification=exc.classification, detail=str(exc))
        except ModuleXFail as exc:
            record = ModuleRecord(module=module.id, status=XFAIL, seconds=time.monotonic() - start, classification=exc.classification, detail=str(exc))
        except BlackboxError as exc:
            record = ModuleRecord(module=module.id, status=FAIL, seconds=time.monotonic() - start, classification="product regression", detail=str(exc))
            re_raise = exc
        except Exception as exc:
            record = ModuleRecord(
                module=module.id,
                status=FAIL,
                seconds=time.monotonic() - start,
                classification="infra failure",
                detail=f"{type(exc).__name__}: {exc}",
            )
            re_raise = exc
        if record is None:
            return
        # Apply compat-matrix xfail expectation: FAIL → XFAIL, PASS → WARN.
        if self.expects_xfail(module):
            if record.status == FAIL:
                record.status = XFAIL
                record.classification = "expected platform incompatibility"
            elif record.status == PASS:
                record.status = WARN
                record.classification = "unexpected pass (xfail expected)"
        record.report_profile = self._module_profile(module)
        self.recorder.record(record)
        progress(f"module {index}/{total} {record.status}: {module.id} in {record.seconds:.1f}s ({record.classification})")
        self.write_module_report(module, record)
        if re_raise is not None and self.args.fail_fast:
            raise re_raise

    def run(self) -> int:
        self.result_dir.mkdir(parents=True, exist_ok=True)
        if self.args.bootstrap:
            return self.bootstrap()
        if self.args.deps_only:
            return self.deps_only()
        progress(f"result dir: {self.result_dir}")
        progress(f"selected modules: {len(self.selected)}")
        progress("checking prerequisites")
        strict = bool(self.args.strict_prereqs)
        prereq_records = self.provider.check_prerequisites(self.ctx)
        if prereq_records:
            for record in prereq_records:
                self.recorder.record(record)
                progress(f"prerequisite {record.status}: {record.module} ({record.detail})")
            self.write_manifest()
            self.finish_report()
            if strict:
                return 1
            return 0
        try:
            setup_start = time.monotonic()
            needs_setup = any(getattr(self.registry[mid], "needs_setup", True) for mid in self.selected)
            if not needs_setup:
                progress("setup skipped: no selected module requires suite setup")
            else:
                try:
                    progress("setup start")
                    self.provider.setup(self.ctx)
                except BlackboxError as exc:
                    detail = str(exc)
                    self.recorder.record(
                        ModuleRecord(
                            module="suite.setup",
                            status=FAIL,
                            seconds=time.monotonic() - setup_start,
                            classification="infra failure",
                            detail=" ".join(detail.split()),
                        )
                    )
                    self.write_manifest()
                    print(f"blackbox setup failed: {detail}", file=sys.stderr, flush=True)
                    self.finish_report()
                    return 1
            progress(f"setup complete in {time.monotonic() - setup_start:.1f}s")
            self.write_manifest()
            total = len(self.selected)
            for idx, module_id in enumerate(self.selected, start=1):
                self.run_module(module_id, index=idx, total=total)
            self.write_manifest()
            self.finish_report()
            return 1 if self.recorder.has_failures() else 0
        except KeyboardInterrupt:
            progress("interrupted; cleaning up")
            raise
        finally:
            self.finish_report()
            self.provider.cleanup(self.ctx)


_parser: argparse.ArgumentParser | None = None


def parse_args(argv: list[str]) -> argparse.Namespace:
    global _parser
    parser = argparse.ArgumentParser(description="Run Drive9 blackbox modules.")
    selector = parser.add_mutually_exclusive_group(required=False)
    selector.add_argument("--all", action="store_true", help="Run every discovered module.")
    selector.add_argument("--module", action="append", help="Run one module id or a comma-separated list. Can be repeated.")
    selector.add_argument("--group", help="Run a directory group, e.g. community, juicefs, drive9, git, or customer.")
    parser.add_argument("--label", action="append", help="Filter selected modules by label. Comma-separated list; can be repeated. Combinable with --all/--group.")
    parser.add_argument("--deps-only", action="store_true", help="Prepare external dependencies for selected modules without running setup.")
    parser.add_argument("--bootstrap", action="store_true", help="Prepare dependencies into a work-dir, then exit. Use --work-dir to reuse later.")
    parser.add_argument("--runs", type=int, default=0, help="Performance run count. Defaults to BLACKBOX_RUNS or 1.")
    parser.add_argument("--server-mode", choices=["config", "local"], default=env_value("SERVER_MODE", "config", ""))
    parser.add_argument("--bin", default="", help="Path to the drive9 CLI binary. Defaults to 'drive9' on PATH.")
    parser.add_argument("--local-server", default="", help="Path to drive9-server-local binary (required for --server-mode local).")
    parser.add_argument("--work-dir", default=env_value("WORK_DIR", "", ""), help="Isolated working directory for cache/tmp/results. Defaults to BLACKBOX_WORK_DIR.")
    parser.add_argument("--out-dir", default=env_value("OUT_DIR", "", ""))
    parser.add_argument("--session", default=env_value("SESSION", "", ""))
    parser.add_argument("--strict-prereqs", action="store_true", default=env_flag("STRICT", False, ""))
    parser.add_argument("--offline", action="store_true", default=env_flag("OFFLINE", False, ""))
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--keep-artifacts", action="store_true", default=env_flag("KEEP_ARTIFACTS", False, ""))
    parser.add_argument("--keep-all-artifacts", action="store_true", default=env_flag("KEEP_ALL_ARTIFACTS", False, ""), help="Never clean tmp_dir, even on success.")
    _parser = parser
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    # No selector provided — print help instead of raising.
    if not any([args.all, args.module, args.group]):
        if _parser is not None:
            _parser.print_help()
        return 1
    try:
        runner = BlackboxRunner(args)
        return runner.run()
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
