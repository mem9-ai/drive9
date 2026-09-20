#!/usr/bin/env python3
"""Execute E2E mount builders against fake CLIs and verify argv evidence."""

from __future__ import annotations

import shlex
import subprocess
import tempfile
from pathlib import Path


E2E_DIR = Path(__file__).resolve().parent
MODES = ("interactive", "fsync", "close-sync", "write-sync")


def shell_function(path: Path, name: str) -> str:
    lines = path.read_text().splitlines()
    start = next(i for i, line in enumerate(lines) if line == f"{name}() {{")
    end = next(i for i in range(start + 1, len(lines)) if lines[i] == "}")
    return "\n".join(lines[start : end + 1])


def durability_value(argv: list[str]) -> str | None:
    found: list[str] = []
    for index, token in enumerate(argv):
        if token.startswith("--durability="):
            found.append(token.split("=", 1)[1])
        elif token == "--durability":
            if index + 1 >= len(argv):
                raise AssertionError(f"missing value after --durability: {argv!r}")
            found.append(argv[index + 1])
    if len(found) > 1:
        raise AssertionError(f"multiple durability arguments: {argv!r}")
    return found[0] if found else None


def captured_argv(path: Path) -> list[str]:
    raw = path.read_bytes()
    if not raw:
        raise AssertionError(f"fake CLI did not capture argv: {path}")
    return [part.decode() for part in raw.split(b"\0") if part]


def evidence_argv(stdout: str) -> list[str]:
    lines = [line for line in stdout.splitlines() if line.startswith("mount_argv=")]
    if len(lines) != 1:
        raise AssertionError(f"expected exactly one mount_argv line, got {lines!r}")
    return shlex.split(lines[0].split("=", 1)[1])


def run_bash(harness: str, capture: Path) -> tuple[list[str], list[str]]:
    result = subprocess.run(
        ["bash", "-c", harness],
        cwd=E2E_DIR.parent,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise AssertionError(
            f"harness failed ({result.returncode})\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    actual = captured_argv(capture)
    rendered = evidence_argv(result.stdout)
    if rendered[1:] != actual:
        raise AssertionError(
            f"mount_argv evidence diverged\nevidence={rendered!r}\nactual={actual!r}"
        )
    return rendered, actual


def init_lines(legacy: str, override: str | None) -> str:
    override_line = (
        "unset DRIVE9_E2E_DURABILITY"
        if override is None
        else f"export DRIVE9_E2E_DURABILITY={shlex.quote(override)}"
    )
    return f"""
source {shlex.quote(str(E2E_DIR / 'durability-contract.sh'))}
{override_line}
drive9_e2e_init_durability {shlex.quote(legacy)}
"""


def assert_mode(label: str, actual: list[str], expected: str | None) -> None:
    got = durability_value(actual)
    if got != expected:
        raise AssertionError(f"{label}: expected durability {expected!r}, got {got!r}: {actual!r}")


def assert_full_argv(
    label: str,
    rendered: list[str],
    actual: list[str],
    expected_cli: str,
    expected: list[str],
) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: argv changed\nexpected={expected!r}\nactual={actual!r}")
    expected_evidence = [expected_cli, *expected]
    if rendered != expected_evidence:
        raise AssertionError(
            f"{label}: evidence changed\nexpected={expected_evidence!r}\nrendered={rendered!r}"
        )


def generic_suite(path: Path, legacy: str) -> None:
    function = shell_function(path, "start_mount")
    runs = [(None, legacy or None), *((mode, mode) for mode in MODES)]
    for override, expected in runs:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            capture = root / "captured.argv"
            harness = f"""
set -euo pipefail
{init_lines(legacy, override)}
CAPTURE_FILE={shlex.quote(str(capture))}
MOUNT_LOG={shlex.quote(str(root / 'mount.log'))}
CLI_BIN=/opt/drive9
MOUNT_POINT={shlex.quote(str(root / 'mount point'))}
ROOT_REMOTE='root with space'
CACHE_DIR={shlex.quote(str(root / 'cache dir'))}
RUN_ROOT={shlex.quote(str(root / 'run root'))}
FUSE_PROFILE='coding agent'
FUSE_SQLITE_MOUNT_DEBUG=0
mkdir -p "$MOUNT_POINT" "$CACHE_DIR" "$RUN_ROOT"
capture_args() {{ printf '%s\\0' "$@" >"$CAPTURE_FILE"; }}
drive9() {{ capture_args "$@"; }}
wait_mount_state() {{ return 0; }}
pgrep() {{ printf '4242\\n'; }}
{function}
start_mount
wait || true
for _ in {{1..100}}; do [ -s "$CAPTURE_FILE" ] && break; sleep 0.01; done
"""
            rendered, actual = run_bash(harness, capture)
            mount_point = str(root / "mount point")
            profile = ["--profile", "coding agent"]
            if path.name == "fuse-sqlite-correctness.sh":
                expected_argv = ["mount", "--mode=fuse", *profile]
                if expected is not None:
                    expected_argv.append(f"--durability={expected}")
                expected_argv.append(mount_point)
            elif path.name in (
                "fuse-performance-baseline.sh",
                "fuse-concurrency-stress.sh",
            ):
                expected_argv = ["mount", "--mode=fuse"]
                if expected is not None:
                    expected_argv.append(f"--durability={expected}")
                expected_argv.extend([*profile, mount_point])
            elif path.name == "fuse-write-perf-budget-test.sh":
                expected_argv = [
                    "mount",
                    "--mode=fuse",
                    "--foreground",
                    "--cache-dir",
                    str(root / "cache dir"),
                    "--durability",
                    str(expected),
                    *profile,
                    "--perf-dir",
                    str(root / "run root" / "perf"),
                    "--perf-interval",
                    "1h",
                    "--perf-cpu-duration",
                    "1ms",
                    "--perf-cpu-interval",
                    "1h",
                    "--perf-heap-interval",
                    "1h",
                    "--perf-max-sample-files",
                    "1",
                    "--perf-max-profile-files",
                    "1",
                    ":root with space",
                    mount_point,
                ]
            else:
                expected_argv = [
                    "mount",
                    "--mode=fuse",
                    "--foreground",
                    "--cache-dir",
                    str(root / "cache dir"),
                    "--durability",
                    str(expected),
                    *profile,
                    mount_point,
                ]
            label = f"{path.name} override={override!r}"
            assert_mode(label, actual, expected)
            assert_full_argv(label, rendered, actual, "/opt/drive9", expected_argv)


def supervision_suite() -> None:
    path = E2E_DIR / "fuse-supervision-test.sh"
    functions = "\n\n".join(
        shell_function(path, name)
        for name in (
            "start_supervised_mount",
            "start_foreground_supervised_mount",
            "run_denied_scoped_mount",
            "start_scoped_mount",
        )
    )
    calls = {
        "background": "start_supervised_mount",
        "foreground": "start_foreground_supervised_mount",
        "denied": "run_denied_scoped_mount",
        "scoped": "start_scoped_mount test-context scoped-root",
    }
    for entrypoint, call in calls.items():
        for override, expected in [(None, "close-sync"), *((mode, mode) for mode in MODES)]:
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                capture = root / "captured.argv"
                harness = f"""
set -euo pipefail
{init_lines('close-sync', override)}
CAPTURE_FILE={shlex.quote(str(capture))}
MOUNT_LOG={shlex.quote(str(root / 'mount.log'))}
CLI_BIN=/opt/drive9
MOUNT_POINT={shlex.quote(str(root / 'mount point'))}
ROOT_REMOTE='root with space'
FUSE_PROFILE='coding agent'
mkdir -p "$MOUNT_POINT"
capture_args() {{ printf '%s\\0' "$@" >"$CAPTURE_FILE"; }}
drive9() {{ capture_args "$@"; }}
scoped_drive9() {{ capture_args "$@"; }}
with_timeout_scoped_drive9() {{ shift; capture_args "$@"; }}
select_scoped_context() {{ return 0; }}
wait_mount_state() {{ return 0; }}
wait_healthy_io() {{ return 0; }}
{functions}
{call}
wait || true
for _ in {{1..100}}; do [ -s "$CAPTURE_FILE" ] && break; sleep 0.01; done
"""
                rendered, actual = run_bash(harness, capture)
                expected_argv = ["mount"]
                if entrypoint == "foreground":
                    expected_argv.append("--supervise-foreground")
                expected_argv.extend(
                    [
                        "--mode=fuse",
                        f"--durability={expected}",
                        "--profile",
                        "coding agent",
                        {
                            "background": ":root with space",
                            "foreground": ":root with space",
                            "denied": ":/",
                            "scoped": ":scoped-root",
                        }[entrypoint],
                        str(root / "mount point"),
                    ]
                )
                label = f"supervision/{entrypoint} override={override!r}"
                assert_mode(label, actual, expected)
                assert_full_argv(label, rendered, actual, "/opt/drive9", expected_argv)


def git_suite() -> None:
    path = E2E_DIR / "git-feature-smoke-test.sh"
    functions = "\n\n".join(
        shell_function(path, name) for name in ("start_mount", "start_git_feature_mount")
    )
    paths = ("initial", "fresh-local-root", "recovery-remount")
    for scenario in paths:
        for override, expected in [(None, "interactive"), *((mode, mode) for mode in MODES)]:
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                capture = root / "captured.argv"
                fake = root / "fake drive9"
                fake.write_text("#!/usr/bin/env bash\nprintf '%s\\0' \"$@\" >\"$CAPTURE_FILE\"\n")
                fake.chmod(0o755)
                harness = f"""
set -euo pipefail
{init_lines('interactive', override)}
export CAPTURE_FILE={shlex.quote(str(capture))}
CLI_BIN={shlex.quote(str(fake))}
BASE=http://127.0.0.1:9009
API_KEY=test-only-key
FUSE_PROFILE='coding agent'
MOUNT_POINTS=()
wait_mount_state() {{ return 0; }}
{functions}
start_git_feature_mount \
  {shlex.quote(str(root / ('mount ' + scenario)))} \
  {shlex.quote(str(root / ('mount ' + scenario + '.log')))} \
  {shlex.quote(str(root / ('local ' + scenario)))} \
  {shlex.quote('remote root/' + scenario)}
wait || true
for _ in {{1..100}}; do [ -s "$CAPTURE_FILE" ] && break; sleep 0.01; done
"""
                rendered, actual = run_bash(harness, capture)
                expected_argv = [
                    "mount",
                    "--mode=fuse",
                    "--profile=coding agent",
                    "--local-root",
                    str(root / ("local " + scenario)),
                    f"--durability={expected}",
                    ":/remote root/" + scenario,
                    str(root / ("mount " + scenario)),
                ]
                label = f"git/{scenario} override={override!r}"
                assert_mode(label, actual, expected)
                assert_full_argv(label, rendered, actual, str(fake), expected_argv)


def main() -> None:
    for name, legacy in (
        ("fuse-sqlite-correctness.sh", ""),
        ("fuse-performance-baseline.sh", ""),
        ("fuse-concurrency-stress.sh", ""),
        ("fuse-write-perf-budget-test.sh", "interactive"),
        ("fuse-crash-recovery-test.sh", "interactive"),
    ):
        generic_suite(E2E_DIR / name, legacy)
    supervision_suite()
    git_suite()
    print("PASS catalog mount argv execution contract")


if __name__ == "__main__":
    main()
