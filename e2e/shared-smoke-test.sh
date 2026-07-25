#!/usr/bin/env bash
# shared-smoke-test: multi-tenant / shared-schema e2e
#   - control plane (meta, soft-cap, lifecycle, gates, metrics)
#   - multi-tenant data plane (cross-tenant isolation)
# Does NOT replace single-tenant api/cli/fuse smokes.
#
# Modes:
#   self (default)  — clean stack via scripts/run-drive9-server-local-shared.sh
#   attach          — existing DRIVE9_BASE + DRIVE9_META_DSN
#   cases-only      — attach; for harness POST_CMD
#
# Usage:
#   bash e2e/shared-smoke-test.sh
#   bash e2e/shared-smoke-test.sh multi-tenant-isolation
#   bash e2e/shared-smoke-test.sh attach
#   bash e2e/shared-smoke-test.sh list
#
set -euo pipefail

E2E_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUITE_DIR="$E2E_DIR/shared-smoke-test"
REPO_ROOT="$(cd "$E2E_DIR/.." && pwd)"
CASES_DIR="$SUITE_DIR/cases"
HARNESS="$REPO_ROOT/scripts/run-drive9-server-local-shared.sh"

# shellcheck source=shared-smoke-test/lib.sh
source "$SUITE_DIR/lib.sh"

ALL_CASES=(
  identity-and-placement
  gates-fork-sql
  multi-tenant-isolation
  lifecycle-delete-and-count
  soft-cap-and-new-pool
  metrics-consistency
)

usage() {
  sed -n '2,20p' "$0"
  echo ""
  echo "Cases:"
  local c
  for c in "${ALL_CASES[@]}"; do
    echo "  - $c"
  done
}

run_one_case() {
  local name="$1"
  local script="$CASES_DIR/${name}.sh"
  if [ ! -f "$script" ]; then
    echo "unknown case: $name (try: list)" >&2
    return 2
  fi
  echo ""
  echo "########## case: $name ##########"
  set +e
  bash "$script"
  local rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    echo "########## PASS: $name ##########"
  else
    echo "########## FAIL: $name (rc=$rc) ##########"
  fi
  return "$rc"
}

run_cases() {
  local names=("$@")
  if [ "${#names[@]}" -eq 0 ]; then
    names=("${ALL_CASES[@]}")
  fi
  require_base
  require_cmds curl jq
  print_env_banner "shared-smoke-test cases"
  if ! meta_available; then
    echo "WARNING: DRIVE9_META_DSN/mysql unavailable — meta assertions will SKIP" >&2
  fi

  local rc=0 name
  for name in "${names[@]}"; do
    if ! run_one_case "$name"; then
      rc=1
    fi
  done
  return "$rc"
}

MODE="self"
CASE_ARGS=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    self|all)
      MODE="self"
      shift
      ;;
    attach|cases-only|cases)
      MODE="attach"
      shift
      ;;
    list|help|-h|--help)
      usage
      exit 0
      ;;
    *)
      CASE_ARGS+=("$1")
      shift
      ;;
  esac
done

case "$MODE" in
  attach)
    if [ -z "${DRIVE9_BASE:-}" ]; then
      echo "attach/cases-only requires DRIVE9_BASE" >&2
      exit 1
    fi
    export DRIVE9_BASE="${DRIVE9_BASE%/}"
    export DRIVE9_META_DSN="${DRIVE9_META_DSN:-}"
    export DRIVE9_TIDBCLOUD_PUBLIC_KEY="${DRIVE9_TIDBCLOUD_PUBLIC_KEY:-local}"
    export DRIVE9_TIDBCLOUD_PRIVATE_KEY="${DRIVE9_TIDBCLOUD_PRIVATE_KEY:-local}"
    BASE="$DRIVE9_BASE"
    META_DSN="${DRIVE9_META_DSN:-}"
    PUBLIC_KEY="$DRIVE9_TIDBCLOUD_PUBLIC_KEY"
    PRIVATE_KEY="$DRIVE9_TIDBCLOUD_PRIVATE_KEY"
    run_cases "${CASE_ARGS[@]+"${CASE_ARGS[@]}"}"
    ;;
  self)
    if [ ! -f "$HARNESS" ]; then
      echo "harness not found: $HARNESS" >&2
      exit 1
    fi
    cases_cmd=(bash "$E2E_DIR/shared-smoke-test.sh" cases-only)
    if [ "${#CASE_ARGS[@]}" -gt 0 ]; then
      cases_cmd+=("${CASE_ARGS[@]}")
    fi
    post_cmd=""
    local_arg=
    for local_arg in "${cases_cmd[@]}"; do
      post_cmd+="$(printf '%q ' "$local_arg")"
    done
    echo "=== shared-smoke-test self-contained ==="
    echo "harness: $HARNESS"
    echo "cases:   ${CASE_ARGS[*]:-all}"
    SOFT_CAP="${SOFT_CAP:-2}" \
    WARM_POOL_SIZE="${WARM_POOL_SIZE:-2}" \
    TENANT_POOL_MAX_SIZE="${TENANT_POOL_MAX_SIZE:-8}" \
    HARD_CAP_RATIO="${HARD_CAP_RATIO:-1.5}" \
    POST_CMD="$post_cmd" \
    bash "$HARNESS"
    ;;
  *)
    usage
    exit 2
    ;;
esac
