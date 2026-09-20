#!/usr/bin/env python3
"""Execute E2E mount builders against fake CLIs and verify argv evidence."""

from __future__ import annotations

import shlex
import subprocess
import tempfile
from pathlib import Path
import re


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


def evidence_argv(stdout: str, expected_role: str) -> list[str]:
    evidence_lines = [
        line for line in stdout.splitlines() if line.startswith("mount_evidence=")
    ]
    if len(evidence_lines) != 1:
        raise AssertionError(
            f"expected exactly one mount_evidence line, got {evidence_lines!r}"
        )
    role_and_argv = shlex.split(evidence_lines[0].split("=", 1)[1])
    if not role_and_argv or role_and_argv[0] != expected_role:
        raise AssertionError(
            f"expected mount role {expected_role!r}, got {role_and_argv!r}"
        )
    lines = [line for line in stdout.splitlines() if line.startswith("mount_argv=")]
    if len(lines) != 1:
        raise AssertionError(f"expected exactly one mount_argv line, got {lines!r}")
    legacy_argv = shlex.split(lines[0].split("=", 1)[1])
    if role_and_argv[1:] != legacy_argv:
        raise AssertionError(
            f"role-bound and legacy argv evidence diverged: {role_and_argv!r} {legacy_argv!r}"
        )
    return legacy_argv


def evidence_role_sequence(stdout: str) -> list[str]:
    evidence_lines = [
        line for line in stdout.splitlines() if line.startswith("mount_evidence=")
    ]
    legacy_lines = [
        line for line in stdout.splitlines() if line.startswith("mount_argv=")
    ]
    if len(evidence_lines) != len(legacy_lines):
        raise AssertionError(
            "role-bound and legacy evidence counts diverged: "
            f"roles={len(evidence_lines)} legacy={len(legacy_lines)}"
        )
    roles: list[str] = []
    for evidence_line, legacy_line in zip(evidence_lines, legacy_lines, strict=True):
        role_and_argv = shlex.split(evidence_line.split("=", 1)[1])
        legacy_argv = shlex.split(legacy_line.split("=", 1)[1])
        if len(role_and_argv) < 3 or role_and_argv[1:] != legacy_argv:
            raise AssertionError(
                f"invalid same-source evidence pair: {evidence_line!r} {legacy_line!r}"
            )
        roles.append(role_and_argv[0])
    return roles


def assert_exact_role_transcript(stdout: str, expected_roles: list[str]) -> None:
    roles = evidence_role_sequence(stdout)
    if roles != expected_roles or len(roles) != len(set(roles)):
        raise AssertionError(
            f"mount transcript role sequence/cardinality mismatch: "
            f"observed={roles!r} expected={expected_roles!r}"
        )


def run_bash(
    harness: str, capture: Path, expected_role: str
) -> tuple[list[str], list[str]]:
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
    rendered = evidence_argv(result.stdout, expected_role)
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


def protocol_mutation_suite() -> None:
    valid = (
        "mount_evidence= primary /opt/drive9 mount --durability=fsync /mnt/run\n"
        "mount_argv= /opt/drive9 mount --durability=fsync /mnt/run\n"
    )
    assert evidence_argv(valid, "primary")[-1] == "/mnt/run"
    mutations = (
        # Missing and duplicate role-bound records must not be accepted.
        "mount_argv= /opt/drive9 mount --durability=fsync /mnt/run\n",
        valid.splitlines()[0] + "\n" + valid,
        # A role-bound argv that differs from the legacy same-source evidence
        # catches a producer change that logs one command but executes another.
        valid.replace(
            "mount_evidence= primary /opt/drive9 mount --durability=fsync /mnt/run",
            "mount_evidence= primary /opt/drive9 mount --durability=fsync /mnt/wrong",
        ),
    )
    for mutated in mutations:
        try:
            evidence_argv(mutated, "primary")
        except AssertionError:
            continue
        raise AssertionError(f"mutated mount evidence unexpectedly accepted: {mutated!r}")


def cli_override_contract_suite() -> None:
    """All catalog suites reuse only a canonical trusted CLI override."""

    suite_paths = [
        E2E_DIR / "fuse-sqlite-correctness.sh",
        E2E_DIR / "fuse-performance-baseline.sh",
        E2E_DIR / "fuse-concurrency-stress.sh",
        E2E_DIR / "fuse-write-perf-budget-test.sh",
        E2E_DIR / "fuse-crash-recovery-test.sh",
        E2E_DIR / "fuse-supervision-test.sh",
        E2E_DIR / "git-feature-smoke-test.sh",
    ]
    for path in suite_paths:
        function = shell_function(path, "prepare_cli_binary")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            trusted = root / "trusted-drive9"
            trusted.write_text("#!/usr/bin/env bash\nexit 0\n")
            trusted.chmod(0o755)
            make_called = root / "make-called"

            # A non-empty valid override is used byte-for-byte, no build runs,
            # and shared cleanup must never delete it.
            harness = f"""
set -euo pipefail
source {shlex.quote(str(E2E_DIR / 'durability-contract.sh'))}
CLI_SOURCE=build
REPO_ROOT={shlex.quote(str(E2E_DIR.parent))}
MAKE_CALLED={shlex.quote(str(make_called))}
make() {{ : >"$MAKE_CALLED"; return 91; }}
export DRIVE9_CLI_BIN={shlex.quote(str(trusted))}
{function}
prepare_cli_binary
test "$CLI_BIN" = "$DRIVE9_CLI_BIN"
test ! -e "$MAKE_CALLED"
drive9_e2e_cleanup_cli_bin
test -f "$DRIVE9_CLI_BIN" -a -x "$DRIVE9_CLI_BIN"
"""
            result = subprocess.run(
                ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
                capture_output=True, check=False,
            )
            if result.returncode:
                raise AssertionError(
                    f"{path.name}: canonical CLI reuse failed ({result.returncode})\n"
                    f"{result.stdout}\n{result.stderr}"
                )

            # Both unset and empty preserve the historical mktemp/build path.
            for empty in (False, True):
                built_marker = root / ("built-empty" if empty else "built-unset")
                env_line = "export DRIVE9_CLI_BIN=''" if empty else "unset DRIVE9_CLI_BIN"
                harness = f"""
set -euo pipefail
source {shlex.quote(str(E2E_DIR / 'durability-contract.sh'))}
CLI_SOURCE=build
REPO_ROOT={shlex.quote(str(E2E_DIR.parent))}
TMPDIR={shlex.quote(str(root))}
export TMPDIR
BUILT_MARKER={shlex.quote(str(built_marker))}
make() {{
  : >"$BUILT_MARKER"
  local token output=''
  for token in "$@"; do
    case "$token" in CLI_BIN=*) output="${{token#CLI_BIN=}}" ;; esac
  done
  test -n "$output"
  printf '#!/usr/bin/env bash\nexit 0\n' >"$output"
  chmod +x "$output"
}}
{env_line}
{function}
prepare_cli_binary
test -e "$BUILT_MARKER"
test "$CLI_BIN" != {shlex.quote(str(trusted))}
test -f "$CLI_BIN" -a -x "$CLI_BIN"
drive9_e2e_cleanup_cli_bin
test ! -e "$CLI_BIN"
"""
                result = subprocess.run(
                    ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
                    capture_output=True, check=False,
                )
                if result.returncode:
                    raise AssertionError(
                        f"{path.name}: historical CLI build changed empty={empty} "
                        f"({result.returncode})\n{result.stdout}\n{result.stderr}"
                    )

            nonexec = root / "nonexec"
            nonexec.write_text("not executable")
            (root / "child").mkdir()
            symlink = root / "symlink"
            symlink.symlink_to(trusted)
            missing = root / "missing"
            invalid = [
                "relative/drive9",
                str(root / "." / "trusted-drive9").replace(
                    str(root), str(root / "child" / ".."), 1
                ),
                str(root),
                str(nonexec),
                str(symlink),
                str(missing),
            ]
            for candidate in invalid:
                harness = f"""
set -euo pipefail
source {shlex.quote(str(E2E_DIR / 'durability-contract.sh'))}
CLI_SOURCE=build
REPO_ROOT={shlex.quote(str(E2E_DIR.parent))}
make() {{ exit 91; }}
export DRIVE9_CLI_BIN={shlex.quote(candidate)}
{function}
if prepare_cli_binary; then exit 90; else rc=$?; fi
test "$rc" -eq 64
test -f {shlex.quote(str(trusted))}
"""
                result = subprocess.run(
                    ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
                    capture_output=True, check=False,
                )
                if result.returncode:
                    raise AssertionError(
                        f"{path.name}: invalid CLI override accepted {candidate!r} "
                        f"({result.returncode})\n{result.stdout}\n{result.stderr}"
                    )

            # A later invalid request must clear the prior external selection
            # and ownership, so cleanup cannot unlink the trusted override.
            harness = f"""
set -euo pipefail
source {shlex.quote(str(E2E_DIR / 'durability-contract.sh'))}
CLI_SOURCE=build
REPO_ROOT={shlex.quote(str(E2E_DIR.parent))}
make() {{ exit 91; }}
export DRIVE9_CLI_BIN={shlex.quote(str(trusted))}
{function}
prepare_cli_binary
test "$CLI_BIN" = "$DRIVE9_CLI_BIN"
export DRIVE9_CLI_BIN=relative/invalid
if prepare_cli_binary; then exit 90; else rc=$?; fi
test "$rc" -eq 64
test -z "$CLI_BIN"
test "$DRIVE9_E2E_CLI_OVERRIDE_ACTIVE" -eq 0
drive9_e2e_cleanup_cli_bin
test -f {shlex.quote(str(trusted))}
"""
            result = subprocess.run(
                ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
                capture_output=True, check=False,
            )
            if result.returncode:
                raise AssertionError(
                    f"{path.name}: valid-to-invalid CLI ownership transition failed "
                    f"({result.returncode})\n{result.stdout}\n{result.stderr}"
                )


def real_suite_cleanup_cli_ownership_suite() -> None:
    """Execute every real suite cleanup against external and owned CLIs."""

    suites = [
        ("fuse-sqlite-correctness.sh", "cleanup"),
        ("fuse-performance-baseline.sh", "cleanup"),
        ("fuse-concurrency-stress.sh", "cleanup"),
        ("fuse-write-perf-budget-test.sh", "cleanup"),
        ("fuse-crash-recovery-test.sh", "cleanup"),
        ("fuse-supervision-test.sh", "cleanup"),
        ("git-feature-smoke-test.sh", "finish"),
    ]
    for name, cleanup_name in suites:
        cleanup_function = shell_function(E2E_DIR / name, cleanup_name)
        for external in (True, False):
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                cli = root / ("external-drive9" if external else "owned-drive9")
                cli.write_text("#!/usr/bin/env bash\nexit 0\n")
                cli.chmod(0o755)
                run_root = root / "run"
                run_root.mkdir()
                harness = f"""
set -euo pipefail
source {shlex.quote(str(E2E_DIR / 'durability-contract.sh'))}
CLI_BIN={shlex.quote(str(cli))}
DRIVE9_E2E_CLI_OVERRIDE_ACTIVE={1 if external else 0}
FAIL=0
PASS=0
SKIP=0
TOTAL=0
RUN_ROOT={shlex.quote(str(run_root))}
MOUNT_LOG={shlex.quote(str(root / 'mount.log'))}
FUSE_SQLITE_KEEP_ARTIFACTS=0
FUSE_SQLITE_MOUNT_DEBUG=0
FUSE_PERF_KEEP_ARTIFACTS=0
FUSE_CONCURRENCY_KEEP_ARTIFACTS=0
WRITE_PERF_KEEP_ARTIFACTS=0
CRASH_KEEP_ARTIFACTS=0
SUPERVISION_KEEP_ARTIFACTS=0
MOUNT_POINTS=()
pseudoroot_contexts=()
pseudoroot_remote_roots=()
stop_mount() {{ :; }}
force_unmount_stale() {{ :; }}
publish_artifacts() {{ :; }}
capture_supervisor_log() {{ :; }}
drive9() {{ :; }}
{cleanup_function}
{cleanup_name}
"""
                result = subprocess.run(
                    ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
                    capture_output=True, check=False,
                )
                if result.returncode:
                    raise AssertionError(
                        f"{name}: real cleanup failed external={external} "
                        f"({result.returncode})\n{result.stdout}\n{result.stderr}"
                    )
                if cli.exists() is not external:
                    raise AssertionError(
                        f"{name}: real cleanup ownership mismatch external={external}"
                    )


def callsite_role_contract_suite() -> None:
    """Lock the roles at the real suite callsites, not only helper fixtures."""

    expected_start_mount = {
        "fuse-sqlite-correctness.sh": ["initial", "remount"],
        "fuse-performance-baseline.sh": ["primary"],
        "fuse-concurrency-stress.sh": ["primary"],
        "fuse-write-perf-budget-test.sh": ["primary"],
        "fuse-crash-recovery-test.sh": ["initial", "recovery", "compaction"],
    }
    for name, expected in expected_start_mount.items():
        source = (E2E_DIR / name).read_text()
        observed = re.findall(r"(?m)^\s*(?:if\s+)?start_mount\s+([a-z][a-z0-9-]*)\b", source)
        if observed != expected or len(observed) != len(set(observed)):
            raise AssertionError(
                f"{name}: real suite mount role sequence changed: "
                f"observed={observed!r} expected={expected!r}"
            )

    supervision = (E2E_DIR / "fuse-supervision-test.sh").read_text()
    observed_supervision = re.findall(
        r"(?m)^\s*(?:if\s+)?(?:start_supervised_mount|"
        r"start_foreground_supervised_mount|start_scoped_mount|"
        r"run_denied_scoped_mount)\s+([a-z][a-z0-9-]*)\b",
        supervision,
    )
    expected_supervision = [
        "background-initial",
        "background-remount",
        "foreground",
        "scoped-root",
        "scoped-team",
        "denied-root",
        "legacy-root",
    ]
    if observed_supervision != expected_supervision:
        raise AssertionError(
            "fuse-supervision-test.sh real callsite role order changed: "
            f"{observed_supervision!r}"
        )
    if len(observed_supervision) != len(set(observed_supervision)):
        raise AssertionError("fuse-supervision-test.sh contains duplicate mount roles")
    required_supervision_fragments = (
        "drive9_e2e_print_mount_evidence ensure-recovery",
        "MOUNT_ROLE_PLAN=(background-initial background-remount)",
        "MOUNT_ROLE_PLAN+=(ensure-recovery)",
        "MOUNT_ROLE_PLAN+=(foreground)",
        "MOUNT_ROLE_PLAN+=(scoped-root scoped-team denied-root legacy-root)",
    )
    for fragment in required_supervision_fragments:
        if fragment not in supervision:
            raise AssertionError(f"supervision role plan lost fragment: {fragment}")
    ensure_gate = "record_ensure_recovery_mount || exit $?"
    ensure_command = 'if drive9 mount ensure "$MOUNT_POINT"'
    if supervision.index(ensure_gate) > supervision.index(ensure_command):
        raise AssertionError("ensure recovery evidence no longer gates mount ensure")

    git_source = (E2E_DIR / "git-feature-smoke-test.sh").read_text()
    main_start = git_source.index("\nmain() {")
    restore_start = git_source.index("\nrun_restore_suite() {")
    restore_end = git_source.index("\n}\n\n\nmain()", restore_start)
    main_source = git_source[main_start:]
    restore_source = git_source[restore_start:restore_end]
    initial = re.findall(
        r"(?m)^\s*(?:if\s+)?start_git_feature_mount\s+([a-z][a-z0-9-]*)\b",
        main_source,
    )
    remounts = re.findall(
        r"(?m)^\s*(?:if\s+)?start_git_feature_mount\s+([a-z][a-z0-9-]*)\b",
        restore_source,
    )
    observed_git = [*initial, *remounts]
    expected_git = ["initial", "fresh-local-root", "ignored-remount"]
    if observed_git != expected_git or len(observed_git) != len(set(observed_git)):
        raise AssertionError(
            "git-feature-smoke-test.sh execution role order changed: "
            f"observed={observed_git!r} expected={expected_git!r}"
        )


def evidence_failure_gate_suite() -> None:
    """Evidence write failure must return before any real mount invocation."""

    generic = (
        "fuse-sqlite-correctness.sh",
        "fuse-performance-baseline.sh",
        "fuse-concurrency-stress.sh",
        "fuse-write-perf-budget-test.sh",
        "fuse-crash-recovery-test.sh",
    )
    for name in generic:
        function = shell_function(E2E_DIR / name, "start_mount")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            capture = root / "mount-ran"
            harness = f"""
set -euo pipefail
{init_lines('interactive' if name in ('fuse-write-perf-budget-test.sh', 'fuse-crash-recovery-test.sh') else '', 'fsync')}
CAPTURE_FILE={shlex.quote(str(capture))}
MOUNT_LOG={shlex.quote(str(root / 'mount.log'))}
CLI_BIN=/opt/drive9
MOUNT_POINT={shlex.quote(str(root / 'mount'))}
ROOT_REMOTE=root
CACHE_DIR={shlex.quote(str(root / 'cache'))}
RUN_ROOT={shlex.quote(str(root))}
FUSE_PROFILE=''
FUSE_SQLITE_MOUNT_DEBUG=0
mkdir -p "$MOUNT_POINT" "$CACHE_DIR"
drive9_e2e_print_mount_evidence() {{ return 70; }}
drive9() {{ : >"$CAPTURE_FILE"; }}
wait_mount_state() {{ return 0; }}
pgrep() {{ printf '4242\n'; }}
{function}
if start_mount primary; then exit 91; else rc=$?; fi
test "$rc" -eq 70
test ! -e "$CAPTURE_FILE"
"""
            result = subprocess.run(
                ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
                capture_output=True, check=False,
            )
            if result.returncode:
                raise AssertionError(
                    f"{name}: evidence failure did not block mount "
                    f"({result.returncode})\n{result.stdout}\n{result.stderr}"
                )

    supervision = E2E_DIR / "fuse-supervision-test.sh"
    for function_name, call in (
        ("start_supervised_mount", "start_supervised_mount blocked-role"),
        ("start_foreground_supervised_mount", "start_foreground_supervised_mount blocked-role"),
        ("run_denied_scoped_mount", "run_denied_scoped_mount blocked-role"),
        ("start_scoped_mount", "start_scoped_mount blocked-role ctx root"),
    ):
        function = shell_function(supervision, function_name)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            capture = root / "mount-ran"
            harness = f"""
set -euo pipefail
{init_lines('close-sync', 'fsync')}
CAPTURE_FILE={shlex.quote(str(capture))}
MOUNT_LOG={shlex.quote(str(root / 'mount.log'))}
CLI_BIN=/opt/drive9
MOUNT_POINT={shlex.quote(str(root / 'mount'))}
ROOT_REMOTE=root
FUSE_PROFILE=''
MOUNT_ROLES_OBSERVED=()
ENSURE_RESTORE_MOUNT_ARGS=()
mkdir -p "$MOUNT_POINT"
drive9_e2e_print_mount_evidence() {{ return 70; }}
drive9() {{ : >"$CAPTURE_FILE"; }}
scoped_drive9() {{ : >"$CAPTURE_FILE"; }}
with_timeout_scoped_drive9() {{ : >"$CAPTURE_FILE"; }}
select_scoped_context() {{ return 0; }}
wait_mount_state() {{ return 0; }}
wait_healthy_io() {{ return 0; }}
{function}
if {call}; then exit 91; else rc=$?; fi
test "$rc" -eq 70
test ! -e "$CAPTURE_FILE"
"""
            result = subprocess.run(
                ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
                capture_output=True, check=False,
            )
            if result.returncode:
                raise AssertionError(
                    f"supervision/{function_name}: evidence failure did not block mount "
                    f"({result.returncode})\n{result.stdout}\n{result.stderr}"
                )

    ensure_function = shell_function(supervision, "record_ensure_recovery_mount")
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        harness = f"""
set -euo pipefail
source {shlex.quote(str(E2E_DIR / 'durability-contract.sh'))}
CLI_BIN=/opt/drive9
MOUNT_LOG={shlex.quote(str(root / 'mount.log'))}
MOUNT_ROLES_OBSERVED=()
ENSURE_RESTORE_MOUNT_ARGS=(mount --mode=fuse --durability=fsync /mnt/run)
drive9_e2e_print_mount_evidence() {{ return 70; }}
{ensure_function}
if record_ensure_recovery_mount; then exit 91; else rc=$?; fi
test "$rc" -eq 70
test "${{#MOUNT_ROLES_OBSERVED[@]}}" -eq 0
"""
        result = subprocess.run(
            ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
            capture_output=True, check=False,
        )
        if result.returncode:
            raise AssertionError(
                "supervision ensure recovery evidence failure did not gate ensure "
                f"({result.returncode})\n{result.stdout}\n{result.stderr}"
            )


def exact_role_sequence_execution_suite() -> None:
    """Execute real suite mount builders in their catalog role order."""

    generic = {
        "fuse-sqlite-correctness.sh": ["initial", "remount"],
        "fuse-performance-baseline.sh": ["primary"],
        "fuse-concurrency-stress.sh": ["primary"],
        "fuse-write-perf-budget-test.sh": ["primary"],
        "fuse-crash-recovery-test.sh": ["initial", "recovery", "compaction"],
    }
    for name, expected_roles in generic.items():
        function = shell_function(E2E_DIR / name, "start_mount")
        legacy = "interactive" if name in (
            "fuse-write-perf-budget-test.sh", "fuse-crash-recovery-test.sh"
        ) else ""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            calls = "\n".join(f"start_mount {role}" for role in expected_roles)
            harness = f"""
set -euo pipefail
{init_lines(legacy, 'fsync')}
MOUNT_LOG={shlex.quote(str(root / 'mount.log'))}
CLI_BIN=/opt/drive9
MOUNT_POINT={shlex.quote(str(root / 'mount'))}
ROOT_REMOTE=root
CACHE_DIR={shlex.quote(str(root / 'cache'))}
RUN_ROOT={shlex.quote(str(root))}
FUSE_PROFILE=''
FUSE_SQLITE_MOUNT_DEBUG=0
mkdir -p "$MOUNT_POINT" "$CACHE_DIR"
drive9() {{ :; }}
wait_mount_state() {{ return 0; }}
pgrep() {{ printf '4242\n'; }}
{function}
{calls}
wait || true
"""
            result = subprocess.run(
                ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
                capture_output=True, check=False,
            )
            if result.returncode:
                raise AssertionError(
                    f"{name}: exact role execution failed ({result.returncode})\n"
                    f"{result.stdout}\n{result.stderr}"
                )
            assert_exact_role_transcript(result.stdout, expected_roles)
            if name == "fuse-crash-recovery-test.sh":
                # A self-consistent extra pair is still an invented mount and
                # must fail exact transcript cardinality.
                mutated = result.stdout + (
                    "mount_evidence= invented /opt/drive9 mount "
                    "--durability=fsync :/ /mnt/invented\n"
                    "mount_argv= /opt/drive9 mount --durability=fsync :/ /mnt/invented\n"
                )
                try:
                    assert_exact_role_transcript(mutated, expected_roles)
                except AssertionError:
                    pass
                else:
                    raise AssertionError("extra crash mount evidence unexpectedly accepted")

    supervision = E2E_DIR / "fuse-supervision-test.sh"
    functions = "\n\n".join(
        shell_function(supervision, name)
        for name in (
            "start_supervised_mount",
            "start_foreground_supervised_mount",
            "record_ensure_recovery_mount",
            "run_denied_scoped_mount",
            "start_scoped_mount",
        )
    )
    expected_supervision = [
        "background-initial", "background-remount", "ensure-recovery",
        "foreground", "scoped-root", "scoped-team", "denied-root", "legacy-root",
    ]
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        harness = f"""
set -euo pipefail
{init_lines('close-sync', 'fsync')}
MOUNT_LOG={shlex.quote(str(root / 'mount.log'))}
CLI_BIN=/opt/drive9
MOUNT_POINT={shlex.quote(str(root / 'mount'))}
ROOT_REMOTE=root
FUSE_PROFILE=''
MOUNT_ROLES_OBSERVED=()
ENSURE_RESTORE_MOUNT_ARGS=()
mkdir -p "$MOUNT_POINT"
drive9() {{ :; }}
scoped_drive9() {{ :; }}
with_timeout_scoped_drive9() {{ shift; :; }}
select_scoped_context() {{ return 0; }}
wait_mount_state() {{ return 0; }}
wait_healthy_io() {{ return 0; }}
{functions}
start_supervised_mount background-initial
start_supervised_mount background-remount
record_ensure_recovery_mount
start_foreground_supervised_mount foreground
start_scoped_mount scoped-root root-token root
start_scoped_mount scoped-team team-token team
run_denied_scoped_mount denied-root
start_scoped_mount legacy-root legacy-token /
wait || true
"""
        result = subprocess.run(
            ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
            capture_output=True, check=False,
        )
        if result.returncode:
            raise AssertionError(
                "supervision exact role execution failed "
                f"({result.returncode})\n{result.stdout}\n{result.stderr}"
            )
        assert_exact_role_transcript(result.stdout, expected_supervision)

    git_path = E2E_DIR / "git-feature-smoke-test.sh"
    git_functions = "\n\n".join(
        shell_function(git_path, name) for name in ("start_mount", "start_git_feature_mount")
    )
    expected_git = ["initial", "fresh-local-root", "ignored-remount"]
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        harness = f"""
set -euo pipefail
{init_lines('interactive', 'fsync')}
CLI_BIN=/bin/true
BASE=http://127.0.0.1:9009
API_KEY=test-only
FUSE_PROFILE=''
MOUNT_POINTS=()
wait_mount_state() {{ return 0; }}
{git_functions}
start_git_feature_mount initial {shlex.quote(str(root / 'mount-a'))} {shlex.quote(str(root / 'a.log'))} {shlex.quote(str(root / 'local-a'))} root
start_git_feature_mount fresh-local-root {shlex.quote(str(root / 'mount-b'))} {shlex.quote(str(root / 'b.log'))} {shlex.quote(str(root / 'local-b'))} root
start_git_feature_mount ignored-remount {shlex.quote(str(root / 'mount-c'))} {shlex.quote(str(root / 'c.log'))} {shlex.quote(str(root / 'local-c'))} root
wait || true
"""
        result = subprocess.run(
            ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
            capture_output=True, check=False,
        )
        if result.returncode:
            raise AssertionError(
                f"git exact role execution failed ({result.returncode})\n"
                f"{result.stdout}\n{result.stderr}"
            )
        assert_exact_role_transcript(result.stdout, expected_git)


def crash_recovery_real_main_transcript_suite() -> None:
    """Execute the real crash-recovery main control flow with bounded stubs.

    This deliberately sources and calls the suite's real main function instead
    of replaying a hand-maintained list of builder calls.  It protects the
    producer protocol from a new, self-consistent evidence pair being inserted
    in the workload without the catalog contract noticing.
    """

    source = (E2E_DIR / "fuse-crash-recovery-test.sh").read_text()

    def execute(script_source: str) -> str:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            script_dir = root / "e2e"
            script_dir.mkdir()
            script = script_dir / "fuse-crash-recovery-test.sh"
            script.write_text(script_source)
            (script_dir / "durability-contract.sh").symlink_to(
                E2E_DIR / "durability-contract.sh"
            )
            harness = f"""
set -euo pipefail
export DRIVE9_API_KEY=test-only
export FUSE_MOUNT_ROOT={shlex.quote(str(root))}
export CRASH_KEEP_ARTIFACTS=0
source {shlex.quote(str(script))}
uname() {{ printf 'Darwin\n'; }}
require_cmd() {{ :; }}
prepare_cli_binary() {{
  CLI_BIN={shlex.quote(str(root / 'fake-drive9'))}
  printf '#!/usr/bin/env bash\nexit 0\n' >"$CLI_BIN"
  chmod +x "$CLI_BIN"
}}
curl_body_code() {{ printf '{{"status":"active"}}\n__HTTP__200\n'; }}
wait_mount_state() {{ return 0; }}
pgrep() {{ printf '4242\n'; }}
write_crash_workload() {{
  mkdir -p "$CACHE_DIR"
  : >"$CACHE_DIR/journal.wal"
  printf '{{}}\n' >"$EXPECTED_MANIFEST"
}}
crash_mount() {{ return 0; }}
wal_path() {{ printf '%s\n' "$CACHE_DIR/journal.wal"; }}
wait_remote_recovered() {{ return 0; }}
check_doomed_absent() {{ return 0; }}
verify_mount_manifest() {{ return 0; }}
unmount_mount() {{ MOUNT_PID=''; return 0; }}
stop_mount() {{ MOUNT_PID=''; return 0; }}
force_unmount_stale() {{ return 0; }}
crash_recovery_main
"""
            result = subprocess.run(
                ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
                capture_output=True, check=False,
            )
            if result.returncode:
                raise AssertionError(
                    "real crash-recovery main stub execution failed "
                    f"({result.returncode})\n{result.stdout}\n{result.stderr}"
                )
            return result.stdout

    expected = ["initial", "recovery", "compaction"]
    assert_exact_role_transcript(execute(source), expected)

    marker = "if start_mount initial; then"
    if source.count(marker) != 1:
        raise AssertionError("crash-recovery initial mount marker is not unique")
    fake_pair = (
        "printf '%s\\n' 'mount_evidence= invented /opt/drive9 mount "
        "--durability=fsync :/ /tmp/invented'\n"
        "printf '%s\\n' 'mount_argv= /opt/drive9 mount "
        "--durability=fsync :/ /tmp/invented'\n"
    )
    mutated = source.replace(marker, fake_pair + marker, 1)
    try:
        assert_exact_role_transcript(execute(mutated), expected)
    except AssertionError:
        pass
    else:
        raise AssertionError(
            "real crash-recovery main accepted an extra self-consistent mount pair"
        )


def supervision_ensure_recovery_contract_suite() -> None:
    path = E2E_DIR / "fuse-supervision-test.sh"
    functions = "\n\n".join(
        shell_function(path, name)
        for name in (
            "start_supervised_mount", "record_ensure_recovery_mount",
            "build_mount_role_plan",
        )
    )
    for override, expected_mode in [
        (None, "close-sync"), *((mode, mode) for mode in MODES),
    ]:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            harness = f"""
set -euo pipefail
{init_lines('close-sync', override)}
CLI_BIN=/opt/drive9
MOUNT_LOG={shlex.quote(str(root / 'mount.log'))}
MOUNT_POINT={shlex.quote(str(root / 'mount'))}
FUSE_PROFILE=''
MOUNT_ROLES_OBSERVED=()
ENSURE_RESTORE_MOUNT_ARGS=()
mkdir -p "$MOUNT_POINT"
drive9() {{ :; }}
wait_mount_state() {{ return 0; }}
wait_healthy_io() {{ return 0; }}
{functions}
ROOT_REMOTE=initial-root
start_supervised_mount background-initial
ROOT_REMOTE=latest-remount-root
start_supervised_mount background-remount
record_ensure_recovery_mount
"""
            result = subprocess.run(
                ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
                capture_output=True, check=False,
            )
            if result.returncode:
                raise AssertionError(
                    "supervision ensure argv contract failed "
                    f"override={override!r} ({result.returncode})\n"
                    f"{result.stdout}\n{result.stderr}"
                )
            evidence = [
                shlex.split(line.split("=", 1)[1])
                for line in result.stdout.splitlines()
                if line.startswith("mount_evidence=")
            ]
            by_role = {tokens[0]: tokens[1:] for tokens in evidence}
            assert_exact_role_transcript(
                result.stdout,
                ["background-initial", "background-remount", "ensure-recovery"],
            )
            if by_role["ensure-recovery"] != by_role["background-remount"]:
                raise AssertionError("ensure recovery argv differs from latest stored remount argv")
            if by_role["ensure-recovery"] == by_role["background-initial"]:
                raise AssertionError("ensure recovery test did not distinguish the latest stored argv")
            if durability_value(by_role["ensure-recovery"][1:]) != expected_mode:
                raise AssertionError("ensure recovery durability mode changed")

    build_plan = shell_function(path, "build_mount_role_plan")
    cases = []
    for run_ensure in ("0", "1"):
        for run_foreground in ("0", "1"):
            for token_code in ("404", "200"):
                expected = ["background-initial", "background-remount"]
                if run_ensure == "1":
                    expected.append("ensure-recovery")
                if run_foreground == "1":
                    expected.append("foreground")
                if token_code == "200":
                    expected.extend(
                        ["scoped-root", "scoped-team", "denied-root", "legacy-root"]
                    )
                cases.append((run_ensure, run_foreground, token_code, expected))
    for run_ensure, run_foreground, token_code, expected in cases:
        harness = f"""
set -euo pipefail
RUN_ENSURE_SMOKE={run_ensure}
RUN_FOREGROUND_SMOKE={run_foreground}
token_management_code={token_code}
MOUNT_ROLE_PLAN=()
{build_plan}
build_mount_role_plan
printf '%s\n' "${{MOUNT_ROLE_PLAN[@]}}"
"""
        result = subprocess.run(
            ["bash", "-c", harness], cwd=E2E_DIR.parent, text=True,
            capture_output=True, check=False,
        )
        if result.returncode or result.stdout.splitlines() != expected:
            raise AssertionError(
                "supervision dynamic role plan mismatch: "
                f"ensure={run_ensure} foreground={run_foreground} token={token_code} "
                f"got={result.stdout.splitlines()!r} expected={expected!r}"
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
start_mount test-role
wait || true
for _ in {{1..100}}; do [ -s "$CAPTURE_FILE" ] && break; sleep 0.01; done
"""
            rendered, actual = run_bash(harness, capture, "test-role")
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
        "background": "start_supervised_mount test-role",
        "foreground": "start_foreground_supervised_mount test-role",
        "denied": "run_denied_scoped_mount test-role",
        "scoped": "start_scoped_mount test-role test-context scoped-root",
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
MOUNT_ROLES_OBSERVED=()
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
                rendered, actual = run_bash(harness, capture, "test-role")
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
  test-role \
  {shlex.quote(str(root / ('mount ' + scenario)))} \
  {shlex.quote(str(root / ('mount ' + scenario + '.log')))} \
  {shlex.quote(str(root / ('local ' + scenario)))} \
  {shlex.quote('remote root/' + scenario)}
wait || true
for _ in {{1..100}}; do [ -s "$CAPTURE_FILE" ] && break; sleep 0.01; done
"""
                rendered, actual = run_bash(harness, capture, "test-role")
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
    protocol_mutation_suite()
    cli_override_contract_suite()
    real_suite_cleanup_cli_ownership_suite()
    callsite_role_contract_suite()
    evidence_failure_gate_suite()
    exact_role_sequence_execution_suite()
    crash_recovery_real_main_transcript_suite()
    supervision_ensure_recovery_contract_suite()
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
