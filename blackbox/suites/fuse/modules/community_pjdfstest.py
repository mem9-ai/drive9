from __future__ import annotations

import fnmatch
import os
import re
import shutil
from typing import Any

from harness.core import BlackboxError, Context, DependencyUnavailable, ModuleSkip, write_json
from .base import BaseModule, module_config, read_text


class CommunityPjdfstest(BaseModule):
    id = "community.pjdfstest"
    category = "community.posix"
    description = "Run pjdfstest on a Drive9 FUSE mount and report raw/effective POSIX pass rate."
    labels = ("compatibility", "posix", "community")
    timeout = 3600

    def ensure_dependencies(self, ctx: Context) -> None:
        if not ctx.capabilities.get("is_root") and os.environ.get("PJDFSTEST_ALLOW_NONROOT", "0") != "1":
            raise ModuleSkip("pjdfstest normally requires root; set PJDFSTEST_ALLOW_NONROOT=1 to run anyway", "platform skip")
        if not shutil.which("prove"):
            raise DependencyUnavailable("prove is required for pjdfstest")
        ctx.deps.ensure_pjdfstest()

    def run(self, ctx: Context) -> dict[str, Any]:
        tests_dir, bin_path = ctx.deps.ensure_pjdfstest()
        cfg = module_config(ctx, self.id)
        groups = cfg.get("groups_by_preset", {}).get(ctx.selected_preset or "daily", "all")
        remote = ctx.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_pjdfstest", remote, durability="write-sync")
        try:
            work_dir = handle.mountpoint / "work"
            work_dir.mkdir(exist_ok=True)
            if groups == "all":
                test_args = [str(tests_dir)]
            else:
                test_args = [str(tests_dir / group) for group in groups if (tests_dir / group).exists()]
                if not test_args:
                    raise ModuleSkip(f"no pjdfstest groups found for preset {ctx.selected_preset}")
            env = ctx.target.base_env()
            env["PATH"] = f"{bin_path.parent}:{tests_dir.parent}:{env.get('PATH', '')}"
            result = ctx.target.run_cmd(
                "community-pjdfstest",
                ["prove", "--recurse", "--verbose", *test_args],
                cwd=work_dir,
                timeout=int(os.environ.get("PJDFSTEST_TIMEOUT_S", str(self.timeout))),
                env=env,
                ok_codes=(0, 1, 124),
            )
            combined = read_text(result.stdout) + "\n" + read_text(result.stderr)
            log = ctx.artifact_dir(self.id) / "pjdfstest.log"
            log.write_text(combined, encoding="utf-8")
            report = self.parse(ctx, combined, str(log), result.code)
            write_json(ctx.result_dir / "pjdfstests.json", report)
            ctx.metric("community.pjdfstest.raw_pass_rate", float(report["raw_pass_rate"]), "ratio")
            ctx.metric("community.pjdfstest.effective_pass_rate", float(report["effective_pass_rate"]), "ratio")
            if report["fail_regression_cases"] > 0:
                raise BlackboxError(f"pjdfstest regressions={report['fail_regression_cases']}; see {log}")
            return report
        finally:
            ctx.target.unmount(handle)

    def parse(self, ctx: Context, text: str, log_path: str, rc: int) -> dict[str, Any]:
        allowlist = ctx.config.get("allowlists", {}).get("pjdfstest", {})
        files_line = re.search(r"Files=(\d+),\s*Tests=(\d+),", text)
        total_files = int(files_line.group(1)) if files_line else 0
        total_cases = int(files_line.group(2)) if files_line else 0
        failed_file_re = re.compile(r"\S+/tests/(?P<rel>[^ ]+?\.t)\s+\(Wstat:\s*\d+\s+Tests:\s*(?P<tests>\d+)\s+Failed:\s*(?P<failed>\d+)\)")
        failed_files = []
        xfail_cases = 0
        fail_regression_cases = 0
        for match in failed_file_re.finditer(text):
            rel = match.group("rel")
            failed = int(match.group("failed"))
            classification = "XFAIL_KNOWN" if self.known_xfail(ctx, allowlist, rel) else "FAIL_REGRESSION"
            if classification == "XFAIL_KNOWN":
                xfail_cases += failed
            else:
                fail_regression_cases += failed
            failed_files.append({"path": rel, "tests": int(match.group("tests")), "failed": failed, "classification": classification})
        failed_cases = sum(item["failed"] for item in failed_files)
        if total_cases == 0:
            failed_cases = len(re.findall(r"^not ok\s+\d+", text, flags=re.MULTILINE))
            total_cases = failed_cases
        if rc != 0 and failed_cases == 0:
            failed_cases = 1
            total_cases = max(total_cases, 1)
            fail_regression_cases = max(fail_regression_cases, 1)
        passed_cases = max(total_cases - failed_cases, 0)
        denominator = max(total_cases - xfail_cases, 1)
        return {
            "schema": "drive9-fuse-pjdfstest/v2",
            "rc": rc,
            "log": log_path,
            "total_files": total_files,
            "total_cases": total_cases,
            "passed_cases": passed_cases,
            "failed_cases": failed_cases,
            "xfail_known_cases": xfail_cases,
            "fail_regression_cases": fail_regression_cases,
            "raw_pass_rate": (passed_cases / total_cases) if total_cases else 0.0,
            "effective_pass_rate": (passed_cases / denominator) if denominator else 0.0,
            "failed_files": failed_files,
        }

    def known_xfail(self, ctx: Context, allowlist: dict[str, Any], rel: str) -> bool:
        group = rel.split("/", 1)[0]
        groups = set(allowlist.get("known_xfail_groups", []))
        if ctx.capabilities.get("os") == "Darwin":
            groups.update(allowlist.get("darwin_known_xfail_groups", []))
        if group in groups:
            return True
        return any(fnmatch.fnmatch(rel, pattern) for pattern in allowlist.get("known_xfail_paths", []))
