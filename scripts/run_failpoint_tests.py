#!/usr/bin/env python3

"""Run all drive9 failpoint tests.

This script rewrites the repository with ``failpoint-ctl enable .``, discovers
tests declared in ``*_failpoint_test.go`` files, runs only those tests with the
``failpoint`` build tag, and then restores the tree with ``failpoint-ctl
disable .``.
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys
import os
from collections import defaultdict


def main() -> int:
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    env = os.environ.copy()
    failpoint_ctl = (
        pathlib.Path(
            subprocess.check_output(["go", "env", "GOPATH"], text=True).strip()
        )
        / "bin"
        / "failpoint-ctl"
    )
    if not failpoint_ctl.exists():
        print(f"failpoint-ctl not found at {failpoint_ctl}", file=sys.stderr)
        print(
            "Install it with: go install github.com/pingcap/failpoint/failpoint-ctl@latest",
            file=sys.stderr,
        )
        return 1

    tests_by_pkg = collect_failpoint_tests(repo_root)
    if not tests_by_pkg:
        print("no *_failpoint_test.go files found", file=sys.stderr)
        return 1

    exit_code = 0
    try:
        if not env.get("DRIVE9_TEST_MYSQL_DSN") and command_exists("podman"):
            env = load_podman_test_env(repo_root, env)
        run([str(failpoint_ctl), "enable", "."], cwd=repo_root, env=env)
        for pkg, tests in sorted(tests_by_pkg.items()):
            pattern = "^(%s)$" % "|".join(sorted(tests))
            run(
                ["go", "test", "-v", "-tags", "failpoint", pkg, "-run", pattern],
                cwd=repo_root,
                env=env,
            )
    except subprocess.CalledProcessError as exc:
        exit_code = exc.returncode or 1
    finally:
        subprocess.run([str(failpoint_ctl), "disable", "."], cwd=repo_root, check=False, env=env)

    return exit_code


def collect_failpoint_tests(repo_root: pathlib.Path) -> dict[str, set[str]]:
    func_pattern = re.compile(r"^func\s+(Test\w+)\s*\(.*\*testing\.T\)")
    tests_by_pkg: dict[str, set[str]] = defaultdict(set)

    for path in repo_root.rglob("*_failpoint_test.go"):
        rel_dir = path.parent.relative_to(repo_root)
        pkg = "./" + rel_dir.as_posix() if rel_dir.as_posix() != "." else "./"
        for line in path.read_text(encoding="utf-8").splitlines():
            match = func_pattern.match(line.strip())
            if match:
                tests_by_pkg[pkg].add(match.group(1))

    return tests_by_pkg


def load_podman_test_env(repo_root: pathlib.Path, env: dict[str, str]) -> dict[str, str]:
    # Mirror the regular `make test` environment so failpoint runs use the same
    # MySQL testcontainers setup when the suite relies on Podman locally.
    command = "source ./scripts/test-podman.sh && env"
    proc = subprocess.run(
        ["bash", "-lc", command],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    merged = env.copy()
    for line in proc.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        merged[key] = value
    return merged


def command_exists(name: str) -> bool:
    # This shell invocation would be a command-injection risk for untrusted input.
    # Today the only caller passes the constant string "podman", so the exposure is
    # known and intentionally accepted to keep this helper simple for local tooling.
    return subprocess.run(
        ["bash", "-lc", f"command -v {name} >/dev/null 2>&1"],
        check=False,
    ).returncode == 0


def run(cmd: list[str], cwd: pathlib.Path, env: dict[str, str]) -> None:
    subprocess.run(cmd, cwd=cwd, check=True, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
