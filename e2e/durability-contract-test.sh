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
  grep -q 'drive9_e2e_print_mount_argv' "$SCRIPT_DIR/$script"
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
test "$(grep -c 'start_git_feature_mount .*git_root_rel' "$SCRIPT_DIR/git-feature-smoke-test.sh")" -eq 3

echo "PASS catalog suite durability integration"

python3 "$SCRIPT_DIR/durability-argv-test.py"
