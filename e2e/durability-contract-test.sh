#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/durability-contract.sh"

unset DRIVE9_E2E_DURABILITY
drive9_e2e_init_durability close-sync
test "$DRIVE9_E2E_EFFECTIVE_DURABILITY" = close-sync
test "$DRIVE9_E2E_DURABILITY_OVERRIDDEN" = 0

# The legacy behavior is part of the API contract and must always be explicit.
if drive9_e2e_init_durability 2>/dev/null; then
  echo "missing legacy durability unexpectedly accepted" >&2
  exit 1
fi
if drive9_e2e_init_durability interactive extra 2>/dev/null; then
  echo "multiple legacy durability arguments unexpectedly accepted" >&2
  exit 1
fi
if drive9_e2e_init_durability auto 2>/dev/null; then
  echo "invalid legacy durability unexpectedly accepted" >&2
  exit 1
fi

# Empty is an intentional legacy value: the standalone suite omits the flag.
drive9_e2e_init_durability ""
test -z "$DRIVE9_E2E_EFFECTIVE_DURABILITY"
test "$DRIVE9_E2E_DURABILITY_OVERRIDDEN" = 0

DRIVE9_E2E_DURABILITY=""
drive9_e2e_init_durability interactive
test "$DRIVE9_E2E_EFFECTIVE_DURABILITY" = interactive
test "$DRIVE9_E2E_DURABILITY_OVERRIDDEN" = 0

for mode in interactive fsync close-sync write-sync; do
  DRIVE9_E2E_DURABILITY="$mode"
  drive9_e2e_init_durability close-sync
  test "$DRIVE9_E2E_EFFECTIVE_DURABILITY" = "$mode"
  test "$DRIVE9_E2E_DURABILITY_OVERRIDDEN" = 1
done

DRIVE9_E2E_DURABILITY=auto
if drive9_e2e_init_durability close-sync 2>/dev/null; then
  echo "invalid durability unexpectedly accepted" >&2
  exit 1
fi
for invalid_mode in ' fsync' 'fsync ' FSYNC; do
  DRIVE9_E2E_DURABILITY="$invalid_mode"
  if drive9_e2e_init_durability close-sync 2>/dev/null; then
    echo "non-canonical durability unexpectedly accepted: $invalid_mode" >&2
    exit 1
  fi
done
unset DRIVE9_E2E_DURABILITY

mount_line="$(drive9_e2e_print_mount_argv /opt/drive9 mount --durability=fsync ":/root with space" /mnt/run)"
python3 - "$mount_line" <<'PY'
import shlex
import sys

prefix, rendered = sys.argv[1].split("=", 1)
assert prefix == "mount_argv"
assert shlex.split(rendered) == [
    "/opt/drive9", "mount", "--durability=fsync", ":/root with space", "/mnt/run"
]
PY

mount_evidence="$(drive9_e2e_print_mount_evidence primary /opt/drive9 mount --durability=fsync ":/root with space" /mnt/run)"
python3 - "$mount_evidence" <<'PY'
import shlex
import sys

lines = sys.argv[1].splitlines()
assert len(lines) == 2
prefix, rendered = lines[0].split("=", 1)
assert prefix == "mount_evidence"
assert shlex.split(rendered) == [
    "primary", "/opt/drive9", "mount", "--durability=fsync",
    ":/root with space", "/mnt/run",
]
legacy_prefix, legacy = lines[1].split("=", 1)
assert legacy_prefix == "mount_argv"
assert shlex.split(legacy) == shlex.split(rendered)[1:]
PY

roundtrip_evidence="$(drive9_e2e_print_mount_evidence primary /opt/drive9 mount \
  --durability=fsync "" "quote'and\\slash" "punctuation-_.:@" "/mnt/path with space")"
python3 - "$roundtrip_evidence" <<'PY'
import shlex
import sys

lines = sys.argv[1].splitlines()
role_and_argv = shlex.split(lines[0].split("=", 1)[1])
legacy_argv = shlex.split(lines[1].split("=", 1)[1])
assert role_and_argv == [
    "primary", "/opt/drive9", "mount", "--durability=fsync", "",
    "quote'and\\slash", "punctuation-_.:@", "/mnt/path with space",
]
assert role_and_argv[1:] == legacy_argv
PY

for invalid_role in '' '-initial' 'initial-' 'Initial' 'has space' 'has_underscore'; do
  if drive9_e2e_print_mount_evidence "$invalid_role" /opt/drive9 mount /mnt/run >/dev/null 2>&1; then
    echo "invalid mount role unexpectedly accepted: $invalid_role" >&2
    exit 1
  fi
done
if drive9_e2e_print_mount_evidence primary /opt/drive9 >/dev/null 2>&1; then
  echo "mount evidence without mount argv unexpectedly accepted" >&2
  exit 1
fi
for invalid_subcommand in echo status umount; do
  if drive9_e2e_print_mount_evidence primary /opt/drive9 "$invalid_subcommand" /mnt/run >/dev/null 2>&1; then
    echo "non-mount evidence unexpectedly accepted: $invalid_subcommand" >&2
    exit 1
  fi
done
if drive9_e2e_print_mount_evidence primary "" mount /mnt/run >/dev/null 2>&1; then
  echo "empty CLI unexpectedly accepted" >&2
  exit 1
fi
for control_code in {1..31} 127; do
  printf -v control_octal '%03o' "$control_code"
  printf -v control_char "\\$control_octal"
  control_arg="bad${control_char}path"
  if drive9_e2e_print_mount_evidence primary /opt/drive9 mount "$control_arg" >/dev/null 2>&1; then
    echo "C0/DEL control character unexpectedly accepted: $control_code" >&2
    exit 1
  fi
done

observed_roles=$'initial\nremount'
drive9_e2e_assert_mount_role_plan "$observed_roles" "$observed_roles"
for mutated_roles in initial $'initial\ninitial' $'remount\ninitial'; do
  if drive9_e2e_assert_mount_role_plan "$mutated_roles" "$observed_roles" 2>/dev/null; then
    echo "mutated role sequence unexpectedly accepted: $mutated_roles" >&2
    exit 1
  fi
done
if drive9_e2e_print_mount_role_plan initial initial >/dev/null 2>&1; then
  echo "duplicate mount role plan unexpectedly rendered" >&2
  exit 1
fi
if drive9_e2e_assert_mount_role_plan $'initial\ninitial' $'initial\ninitial' 2>/dev/null; then
  echo "duplicate expected/observed mount role plan unexpectedly accepted" >&2
  exit 1
fi
role_plan="$(drive9_e2e_print_mount_role_plan initial remount)"
python3 - "$role_plan" <<'PY'
import shlex
import sys
prefix, rendered = sys.argv[1].split("=", 1)
assert prefix == "mount_role_plan"
assert shlex.split(rendered) == ["initial", "remount"]
PY

echo "PASS durability override contract"

catalog_scripts=(
  fuse-sqlite-correctness.sh
  fuse-performance-baseline.sh
  fuse-concurrency-stress.sh
  fuse-write-perf-budget-test.sh
  fuse-crash-recovery-test.sh
  fuse-supervision-test.sh
  git-feature-smoke-test.sh
)
for script in "${catalog_scripts[@]}"; do
  bash -n "$SCRIPT_DIR/$script"
  grep -q 'source "$SCRIPT_DIR/durability-contract.sh"' "$SCRIPT_DIR/$script"
  grep -q 'drive9_e2e_print_mount_evidence' "$SCRIPT_DIR/$script"
done

# Suites that historically omitted --durability must still omit it when no
# override is present.  The conditional is the compatibility boundary.
for script in fuse-sqlite-correctness.sh fuse-performance-baseline.sh fuse-concurrency-stress.sh; do
  grep -q 'drive9_e2e_init_durability ""' "$SCRIPT_DIR/$script"
  grep -q 'DRIVE9_E2E_DURABILITY_OVERRIDDEN' "$SCRIPT_DIR/$script"
done

# Historically fixed suites retain their old standalone default while every
# real mount consumes the resolved value instead of a remaining literal.
grep -q 'drive9_e2e_init_durability "close-sync"' "$SCRIPT_DIR/fuse-supervision-test.sh"
test "$(grep -c -- '--durability=\$DRIVE9_E2E_EFFECTIVE_DURABILITY' "$SCRIPT_DIR/fuse-supervision-test.sh")" -eq 4
if grep -q -- '--durability=close-sync' "$SCRIPT_DIR/fuse-supervision-test.sh"; then
  echo "fuse-supervision-test.sh still contains a fixed close-sync mount" >&2
  exit 1
fi
# The denied mount must keep evidence on stdout; only the CLI's own output is
# redirected into the suite log.
grep -q 'with_timeout_scoped_drive9 10 .*>>"\$MOUNT_LOG" 2>&1' "$SCRIPT_DIR/fuse-supervision-test.sh"
if grep -q 'run_denied_scoped_mount >>' "$SCRIPT_DIR/fuse-supervision-test.sh"; then
  echo "denied scoped mount redirects mount_argv evidence away from stdout" >&2
  exit 1
fi

for script in fuse-write-perf-budget-test.sh fuse-crash-recovery-test.sh git-feature-smoke-test.sh; do
  grep -q 'drive9_e2e_init_durability "interactive"' "$SCRIPT_DIR/$script"
  if grep -qE -- '--durability(=|[[:space:]])interactive([[:space:]\\]|$)' "$SCRIPT_DIR/$script"; then
    echo "$script still contains a fixed interactive mount" >&2
    exit 1
  fi
done
test "$(grep -c -- '--durability="\$DRIVE9_E2E_EFFECTIVE_DURABILITY"' "$SCRIPT_DIR/git-feature-smoke-test.sh")" -eq 1
grep -q 'start_mount initial' "$SCRIPT_DIR/fuse-sqlite-correctness.sh"
grep -q 'start_mount remount' "$SCRIPT_DIR/fuse-sqlite-correctness.sh"
grep -q 'start_mount primary' "$SCRIPT_DIR/fuse-performance-baseline.sh"
grep -q 'start_mount primary' "$SCRIPT_DIR/fuse-concurrency-stress.sh"
grep -q 'start_mount primary' "$SCRIPT_DIR/fuse-write-perf-budget-test.sh"
grep -q 'start_mount initial' "$SCRIPT_DIR/fuse-crash-recovery-test.sh"
grep -q 'start_mount recovery' "$SCRIPT_DIR/fuse-crash-recovery-test.sh"
grep -q 'start_mount compaction' "$SCRIPT_DIR/fuse-crash-recovery-test.sh"
test "$(grep -c 'start_git_feature_mount .*git_root_rel' "$SCRIPT_DIR/git-feature-smoke-test.sh")" -eq 3
grep -q 'start_git_feature_mount initial ' "$SCRIPT_DIR/git-feature-smoke-test.sh"
grep -q 'start_git_feature_mount fresh-local-root ' "$SCRIPT_DIR/git-feature-smoke-test.sh"
grep -q 'start_git_feature_mount ignored-remount ' "$SCRIPT_DIR/git-feature-smoke-test.sh"
grep -q 'start_supervised_mount background-initial' "$SCRIPT_DIR/fuse-supervision-test.sh"
grep -q 'start_supervised_mount background-remount' "$SCRIPT_DIR/fuse-supervision-test.sh"
grep -q 'start_foreground_supervised_mount foreground' "$SCRIPT_DIR/fuse-supervision-test.sh"
grep -q 'start_scoped_mount scoped-root ' "$SCRIPT_DIR/fuse-supervision-test.sh"
grep -q 'start_scoped_mount scoped-team ' "$SCRIPT_DIR/fuse-supervision-test.sh"
grep -q 'run_denied_scoped_mount denied-root' "$SCRIPT_DIR/fuse-supervision-test.sh"
grep -q 'start_scoped_mount legacy-root ' "$SCRIPT_DIR/fuse-supervision-test.sh"
grep -q 'drive9_e2e_print_mount_role_plan' "$SCRIPT_DIR/fuse-supervision-test.sh"

echo "PASS catalog suite durability integration"

python3 "$SCRIPT_DIR/durability-argv-test.py"
