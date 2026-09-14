from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any

from harness.core import BlackboxError, Context, write_json
from harness.module_base import BaseModule, read_text
from suites.community.node_fs.deps import ensure_node_fs_deps, module_cfg

# tools/test.py exits 0 when everything passed and 1 when tests failed; both are
# parseable outcomes. Anything else (or 124 from the run_cmd timeout) is a
# harness-level anomaly and must fail closed.
_TEST_RUNNER_OK_CODES = (0, 1)
_PROGRESS_RE = re.compile(r"\[\d+:\d+\|% *\d+\|\+ +(?P<passed>\d+)\|- +(?P<failed>\d+)\]")
_FAILED_NAME_RE = re.compile(r"test/parallel/(?P<name>test-fs-[\w.-]+\.js)")


class CommunityNodeFS(BaseModule):
    """Run the pinned Node.js LTS core ``parallel/test-fs-*`` suite on a Drive9 mount.

    Uses the official node test runner (``tools/test.py``) with the official
    release binary of the pinned version (``--shell``), redirecting every
    test's tmpdir onto the mount via ``NODE_TEST_DIR`` (and ``TMPDIR`` so tests
    that use ``os.tmpdir()`` directly also land on the mount). Failures listed
    in config.json ``exclusions`` are triaged categories (network-FS inherent,
    environment assumptions), not silently ignored: an exclusion that stops
    failing is reported as stale so the list stays honest.

    Mount attribute-cache and coherence races make some permission-transition
    tests fail intermittently, so a failed first pass is retried once by
    re-running only the failed subset. Only tests failing both passes count as
    failures; single-pass failures are reported as ``flaky_recovered`` (metric,
    not fatal).
    """

    description = "Run the pinned Node.js LTS core parallel/test-fs suite with test tmpdirs on a Drive9 FUSE mount."
    labels = ("compatibility", "node", "community")
    timeout = 3600

    def ensure_dependencies(self, ctx: Context) -> None:
        ensure_node_fs_deps(ctx)

    def run(self, ctx: Context) -> dict[str, Any]:
        cfg = module_cfg()
        node_src, node_bin = ensure_node_fs_deps(ctx)
        node_version = str(cfg["node_version"])
        filter_pattern = str(cfg.get("test_filter", "parallel/test-fs-*"))
        exclusions: dict[str, str] = {str(k): str(v) for k, v in cfg.get("exclusions", {}).items()}

        selected = self.select_tests(node_src, filter_pattern, exclusions)
        if not selected:
            raise BlackboxError(f"no tests matched {filter_pattern} under {node_src}/test")
        # Exclusions are xfail-style, not a skip list: every matching test
        # still runs. A consistently failing listed test counts as an expected
        # failure; a listed test that passes is reported stale so the compat
        # matrix keeps tracking reality instead of freezing history.

        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        profile = os.environ.get("FUSE_PROFILE") or "none"
        handle = ctx.target.mount("community_node_fs", remote, profile=profile, extra=["--allow-other"])
        try:
            test_root = handle.mountpoint / "nodejs-test"
            test_root.mkdir()
            env = ctx.target.base_env()
            # Redirect both the runner-managed tmpdir (NODE_TEST_DIR) and
            # os.tmpdir() (TMPDIR) onto the mount; the node checkout and
            # binary stay on local disk so only the fs-under-test is slow.
            env["NODE_TEST_DIR"] = str(test_root)
            env["TMPDIR"] = str(test_root)
            env["PATH"] = f"{Path(node_bin).resolve().parent}:{env.get('PATH', '')}"
            jobs = str(int(os.environ.get("NODE_FS_JOBS", "1")))
            per_test_timeout = str(int(os.environ.get("NODE_FS_TEST_TIMEOUT_S", "300")))
            timeout_s = int(os.environ.get("NODE_FS_TIMEOUT_S", str(self.timeout)))
            cmd = [
                sys.executable,
                str(node_src / "tools" / "test.py"),
                "--shell",
                node_bin,
                "-j",
                jobs,
                "-t",
                per_test_timeout,
            ]
            result = ctx.target.run_cmd(
                "community-node-fs",
                [*cmd, *[f"parallel/{name}" for name in selected]],
                cwd=node_src,
                timeout=timeout_s,
                env=env,
                ok_codes=_TEST_RUNNER_OK_CODES,
            )
            # Read the log snapshot now: run_cmd logs append across
            # invocations, and the retry pass below reuses the same command
            # name (and therefore the same stdout/stderr files).
            stdout_text = read_text(result.stdout)
            stderr_text = read_text(result.stderr)
            first = self.parse(stdout_text, str(result.stdout), result.code)
            failed_names = first["failed_tests"]
            retry_report: dict[str, Any] = {}
            if failed_names:
                retry_report = self._retry_failed(ctx, node_src, cmd, failed_names, env, per_test_timeout)
            log = ctx.artifact_dir(self.id) / "node-fs.log"
            log.write_text(stdout_text + "\n" + stderr_text, encoding="utf-8")
            report = self.combine(first, retry_report, node_version, filter_pattern, selected, exclusions)
            write_json(ctx.result_dir / "node_fs.json", report)
            ctx.metric("community.node_fs.pass_rate", float(report["pass_rate"]), "ratio")
            ctx.metric("community.node_fs.unexpected_failures", float(report["unexpected_failure_count"]), "count")
            ctx.metric("community.node_fs.flaky_recovered", float(report["flaky_recovered_count"]), "count")
            if report["unexpected_failure_count"] > 0:
                raise BlackboxError(
                    f"node core fs failures={report['unexpected_failure_count']}; "
                    f"see {log} (excluded: {report['excluded_failure_count']}, "
                    f"flaky recovered: {report['flaky_recovered_count']})"
                )
            return report
        finally:
            ctx.target.unmount(handle)

    def select_tests(self, node_src: Path, filter_pattern: str, exclusions: dict[str, str]) -> list[str]:
        """Return sorted test filenames under test/parallel matching the filter.

        Only ``test-fs-*.js`` files are in scope. Exclusions do not remove a
        test from selection (see run); they only downgrade consistent failures.
        """
        prefix = filter_pattern.split("/", 1)[1] if "/" in filter_pattern else filter_pattern
        prefix = prefix.rstrip("*")
        parallel = node_src / "test" / "parallel"
        all_names = sorted(p.name for p in parallel.glob("test-fs-*.js") if p.name.startswith(prefix))
        missing = sorted(set(exclusions) - set(all_names))
        if missing:
            raise BlackboxError(
                f"exclusions reference tests that do not exist under {parallel}: {', '.join(missing)}"
            )
        return all_names

    def _retry_failed(
        self,
        ctx: Context,
        node_src: Path,
        cmd: list[str],
        failed_names: list[str],
        env: dict[str, str],
        per_test_timeout: str,
    ) -> dict[str, Any]:
        """Re-run only the failed subset once and return its parse result."""
        result = ctx.target.run_cmd(
            "community-node-fs",
            [*cmd, *[f"parallel/{name}" for name in failed_names]],
            cwd=node_src,
            timeout=max(60, int(per_test_timeout) * max(1, len(failed_names))),
            env=env,
            ok_codes=_TEST_RUNNER_OK_CODES,
        )
        stdout_text = read_text(result.stdout)
        return self.parse(stdout_text, str(result.stdout), result.code)

    @staticmethod
    def _latest_section(text: str) -> str:
        # run_cmd logs append across invocations (across module runs too —
        # the work-dir persists), each starting with a "# <ts> $ <cmd>" header.
        # Score only the latest invocation so stale "Failed tests:" sections
        # from earlier runs are never counted against this one.
        headers = list(re.finditer(r"(?m)^# \d{4}-\d{2}-\d{2}T[^$]*\$ ", text))
        if headers:
            return text[headers[-1].start() :]
        return text

    def parse(self, stdout_text: str, log_path: str, rc: int) -> dict[str, Any]:
        """Parse one tools/test.py invocation (progress counters + failed names)."""
        # Progress frames are separated by carriage returns; normalize first.
        text = self._latest_section(stdout_text).replace("\r", "\n")
        progress = list(_PROGRESS_RE.finditer(text))
        passed = int(progress[-1].group("passed")) if progress else 0
        failed = int(progress[-1].group("failed")) if progress else 0
        failed_names: list[str] = []
        marker = text.rfind("Failed tests:")
        if marker >= 0:
            failed_names = sorted(set(_FAILED_NAME_RE.findall(text[marker:])))
        ran = passed + failed
        anomaly = ""
        if ran == 0:
            anomaly = "no_test_summary"
        elif rc not in _TEST_RUNNER_OK_CODES:
            anomaly = f"runner_exit_{rc}"
        elif rc == 0 and failed > 0:
            anomaly = "exit_zero_with_failures"
        elif len(failed_names) > failed:
            # More failed files than the runner's failed counter: parsing lost
            # or duplicated information — fail closed instead of guessing.
            anomaly = "failed_names_exceed_counter"
        report: dict[str, Any] = {
            "rc": rc,
            "log": log_path,
            "passed": passed,
            "failed": failed,
            "failed_tests": failed_names,
        }
        if anomaly:
            report["anomaly"] = anomaly
        return report

    def combine(
        self,
        first: dict[str, Any],
        retry: dict[str, Any],
        node_version: str,
        filter_pattern: str,
        selected: list[str],
        exclusions: dict[str, str],
    ) -> dict[str, Any]:
        if anomaly := first.get("anomaly"):
            raise BlackboxError(f"node fs first-pass anomaly ({anomaly}); see {first['log']}")
        if retry and (retry_anomaly := retry.get("anomaly")):
            raise BlackboxError(f"node fs retry-pass anomaly ({retry_anomaly}); see {retry['log']}")
        first_failures = set(first["failed_tests"])
        consistent = sorted(first_failures & set(retry.get("failed_tests", []))) if retry else sorted(first_failures)
        flaky_recovered = sorted(first_failures - set(consistent))
        unexpected = [name for name in consistent if name not in exclusions]
        excluded_failures = [name for name in consistent if name in exclusions]
        stale = sorted(set(exclusions) - set(consistent))
        passed = first["passed"]
        ran = passed + len(first_failures)
        report: dict[str, Any] = {
            "schema": "drive9-blackbox-node-fs/v1",
            "node_version": node_version,
            "test_filter": filter_pattern,
            "rc": first["rc"],
            "log": first["log"],
            "selected_tests": len(selected),
            "excluded_tests": len(exclusions),
            "passed": passed,
            "failed": len(consistent),
            "failed_tests": consistent,
            "flaky_recovered": flaky_recovered,
            "flaky_recovered_count": len(flaky_recovered),
            "unexpected_failures": unexpected,
            "unexpected_failure_count": len(unexpected),
            "excluded_failures": excluded_failures,
            "excluded_failure_count": len(excluded_failures),
            "stale_exclusions": stale,
            "pass_rate": (passed / ran) if ran else 0.0,
        }
        return report
