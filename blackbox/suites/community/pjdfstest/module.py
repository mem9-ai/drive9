from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from harness.core import BlackboxError, Context, write_json
from harness.module_base import BaseModule, read_text
from suites.community.pjdfstest.deps import ensure_pjdfstest


class CommunityPjdfstest(BaseModule):
    description = "Run pjdfstest on a Drive9 FUSE mount and report POSIX pass rate."
    labels = ("compatibility", "posix", "community")
    timeout = 1800

    def ensure_dependencies(self, ctx: Context) -> None:
        ctx.deps.ensure_prove()
        ensure_pjdfstest(ctx)

    def run(self, ctx: Context) -> dict[str, Any]:
        # Absolute path: Arch puts prove under /usr/bin/core_perl, which is off
        # sudo's secure_path; bare "prove" fails with "command not found".
        prove_bin = ctx.deps.ensure_prove()
        tests_dir, bin_path = ensure_pjdfstest(ctx)
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_pjdfstest", remote, profile="none", extra=["--allow-other"])
        try:
            work_dir = handle.mountpoint / "work"
            work_dir.mkdir(exist_ok=True)
            test_args = [str(tests_dir)]
            env = ctx.target.base_env()
            # Keep pjdfstest + prove dirs on PATH for prove-spawned .t scripts.
            # sudo's secure_path often discards PATH even with -E, so when we
            # elevate we re-inject PATH via `env` and invoke prove by absolute
            # path (Arch: /usr/bin/core_perl/prove is off the default secure_path).
            env["PATH"] = f"{bin_path.parent}:{tests_dir.parent}:{Path(prove_bin).parent}:{env.get('PATH', '')}"
            # pjdfstest exercises privileged operations (chown, chmod, utimens,
            # etc.) that require root. When not root, elevate via passwordless
            # sudo so the harness itself stays unprivileged. If sudo is
            # unavailable or not passwordless, fail hard rather than running a
            # degraded suite.
            prove_cmd = [prove_bin, "--recurse", "--verbose", *test_args]
            if not ctx.capabilities.get("is_root"):
                if not shutil.which("sudo"):
                    raise BlackboxError("pjdfstest requires root or passwordless sudo; sudo not found")
                probe = subprocess.run(["sudo", "-n", "true"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
                if probe.returncode != 0:
                    raise BlackboxError("pjdfstest requires root or passwordless sudo; sudo not available")
                prove_cmd = ["sudo", "-n", "env", f"PATH={env['PATH']}", *prove_cmd]
            result = ctx.target.run_cmd(
                "community-pjdfstest",
                prove_cmd,
                cwd=work_dir,
                timeout=int(os.environ.get("PJDFSTEST_TIMEOUT_S", str(self.timeout))),
                env=env,
                ok_codes=(0, 1, 124),
            )
            combined = read_text(result.stdout) + "\n" + read_text(result.stderr)
            log = ctx.artifact_dir(self.id) / "pjdfstest.log"
            log.write_text(combined, encoding="utf-8")
            report = self.parse(combined, str(log), result.code)
            write_json(ctx.result_dir / "pjdfstests.json", report)
            ctx.metric("community.pjdfstest.raw_pass_rate", float(report["raw_pass_rate"]), "ratio")
            if report["failed_cases"] > 0:
                raise BlackboxError(f"pjdfstest failures={report['failed_cases']}; see {log}")
            return report
        finally:
            ctx.target.unmount(handle)

    def parse(self, text: str, log_path: str, rc: int) -> dict[str, Any]:
        # If command logs were ever appended across runs, only score the latest
        # prove invocation (header line written by target.run_cmd).
        headers = list(re.finditer(r"(?m)^# \d{4}-\d{2}-\d{2}T[^$]*\$ ", text))
        if headers:
            text = text[headers[-1].start() :]
        files_matches = list(re.finditer(r"Files=(\d+),\s*Tests=(\d+),", text))
        files_line = files_matches[-1] if files_matches else None
        total_files = int(files_line.group(1)) if files_line else 0
        total_cases = int(files_line.group(2)) if files_line else 0
        # Only count summary lines with Failed: N (N > 0). The Test Summary
        # Report may also list files with Failed: 0 (e.g. TODO passed).
        failed_file_re = re.compile(
            r"\S+/tests/(?P<rel>[^ ]+?\.t)\s+\(Wstat:\s*\d+\s+Tests:\s*(?P<tests>\d+)\s+Failed:\s*(?P<failed>[1-9]\d*)\)"
        )
        failed_files = []
        for match in failed_file_re.finditer(text):
            failed_files.append({
                "path": match.group("rel"),
                "tests": int(match.group("tests")),
                "failed": int(match.group("failed")),
            })
        failed_cases = sum(item["failed"] for item in failed_files)
        if total_cases == 0:
            # Ignore TAP TODO failures when counting raw not-ok lines.
            failed_cases = len(re.findall(r"(?m)^not ok\s+\d+(?!.*# TODO)", text))
            total_cases = failed_cases
        # Trust prove's final Result line when present.
        if re.search(r"(?m)^Result:\s*PASS\s*$", text) and rc == 0:
            failed_cases = 0
            failed_files = []
        if rc != 0 and failed_cases == 0:
            failed_cases = 1
            total_cases = max(total_cases, 1)
        passed_cases = max(total_cases - failed_cases, 0)
        return {
            "schema": "drive9-fuse-pjdfstest/v2",
            "rc": rc,
            "log": log_path,
            "total_files": total_files,
            "total_cases": total_cases,
            "passed_cases": passed_cases,
            "failed_cases": failed_cases,
            "raw_pass_rate": (passed_cases / total_cases) if total_cases else 1.0 if rc == 0 else 0.0,
            "failed_files": failed_files,
        }