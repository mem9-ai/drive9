#!/usr/bin/env bash
# Real dev end-to-end coverage for the shared Drive9 dev deployment.
#
# This runner intentionally is not wired into CI. It is meant for manual runs
# on the POSIX/FUSE e2e EC2 host after deploying a branch with dev-cd.
#
# Required coverage is at least the required local-e2e workflow coverage:
#   1. API smoke
#   2. Existing-key smoke
#   3. CLI smoke
#   4. Layer FS smoke with layer FUSE restore/commit
#   5. FUSE release gate
#   6. Git operations smoke
#
# Optional local-e2e workloads are exposed as environment switches and remain
# disabled by default, matching workflow_dispatch defaults.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BASE="${DRIVE9_BASE:-http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com}"
RUN_ID="${REAL_E2E_RUN_ID:-real-dev-$(date -u '+%Y%m%dT%H%M%SZ')-$$}"
ARTIFACT_DIR="${REAL_E2E_ARTIFACT_DIR:-$REPO_ROOT/e2e-artifacts/$RUN_ID}"

POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"

RUN_SEMANTIC_CHECKS="${RUN_SEMANTIC_CHECKS:-0}"
RUN_CLI_SEMANTIC_CHECKS="${RUN_CLI_SEMANTIC_CHECKS:-0}"

RUN_LAYER_FUSE_SMOKE="${RUN_LAYER_FUSE_SMOKE:-1}"
LAYER_FUSE_STRICT_PREREQS="${LAYER_FUSE_STRICT_PREREQS:-1}"
FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-1}"

RUN_FUSE_CONCURRENCY_STRESS="${RUN_FUSE_CONCURRENCY_STRESS:-0}"
RUN_FUSE_POSIX_FSX="${RUN_FUSE_POSIX_FSX:-0}"
RUN_FUSE_SQLITE_CORRECTNESS="${RUN_FUSE_SQLITE_CORRECTNESS:-1}"
RUN_FUSE_SQLITE_WAL="${RUN_FUSE_SQLITE_WAL:-0}"
RUN_FUSE_SQLITE_CHURN="${RUN_FUSE_SQLITE_CHURN:-0}"
RUN_FUSE_SQLITE_CONCURRENCY="${RUN_FUSE_SQLITE_CONCURRENCY:-0}"
RUN_FUSE_PERFORMANCE_BASELINE="${RUN_FUSE_PERFORMANCE_BASELINE:-0}"

RUN_REAL_E2E_SMOKE_ALL="${RUN_REAL_E2E_SMOKE_ALL:-0}"
RUN_REAL_E2E_GIT_FEATURE_MATRIX="${RUN_REAL_E2E_GIT_FEATURE_MATRIX:-0}"

CLI_SOURCE="${CLI_SOURCE:-build}"
DRIVE9_EXISTING_API_KEY="${DRIVE9_EXISTING_API_KEY:-${DRIVE9_API_KEY:-}}"

PASS=0
FAIL=0
SKIP=0
TOTAL=0
CASE_INDEX=0

mkdir -p "$ARTIFACT_DIR"

slugify() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -c '[:alnum:]_.-' '-'
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

curl_body_code() {
  local method="$1"
  local url="$2"
  local auth="${3:-}"
  local body_file code
  body_file="$(mktemp)"
  if [ -n "$auth" ]; then
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
  else
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
  fi
  cat "$body_file"
  echo
  echo "__HTTP__${code}"
  rm -f "$body_file"
}

wait_tenant_active() {
  local api_key="$1"
  local deadline resp code body state
  deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
  while :; do
    resp=$(curl_body_code GET "$BASE/v1/status" "$api_key")
    code=$(http_code "$resp")
    body=$(json_body "$resp")
    state=$(printf '%s' "$body" | jq -r '.status // empty')
    echo "existing-key status=${code}:${state}" >&2
    if [ "$code" = "200" ] && [ "$state" = "active" ]; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$POLL_INTERVAL_S"
  done
}

provision_existing_key() {
  local resp code body api_key tenant_id
  echo "Provisioning dedicated existing-key tenant for real e2e..." >&2
  resp=$(curl_body_code POST "$BASE/v1/provision")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  if [ "$code" != "202" ]; then
    echo "provision failed: HTTP $code" >&2
    printf '%s\n' "$body" >&2
    return 1
  fi
  api_key=$(printf '%s' "$body" | jq -r '.api_key // empty')
  tenant_id=$(printf '%s' "$body" | jq -r '.tenant_id // empty')
  if [ -z "$api_key" ]; then
    echo "provision response did not contain api_key" >&2
    printf '%s\n' "$body" >&2
    return 1
  fi
  echo "Provisioned tenant_id=${tenant_id:-unknown} for existing-key and git-ops smoke" >&2
  wait_tenant_active "$api_key"
  printf '%s' "$api_key"
}

run_case() {
  local name="$1"
  shift
  CASE_INDEX=$((CASE_INDEX + 1))
  TOTAL=$((TOTAL + 1))

  local slug log rc
  slug="$(printf '%02d-%s' "$CASE_INDEX" "$(slugify "$name")")"
  log="$ARTIFACT_DIR/$slug.log"

  echo
  echo "=== [$name] ==="
  echo "log=$log"

  set +e
  (
    cd "$REPO_ROOT"
    DRIVE9_BASE="$BASE" "$@"
  ) > >(tee "$log") 2>&1
  rc=$?
  set -e

  if [ "$rc" -eq 0 ]; then
    echo "PASS [$name]"
    PASS=$((PASS + 1))
  else
    echo "FAIL [$name] rc=$rc"
    FAIL=$((FAIL + 1))
  fi
}

run_optional_case() {
  local enabled="$1"
  local name="$2"
  shift 2
  if [ "$enabled" = "1" ]; then
    run_case "$name" "$@"
  else
    echo
    echo "SKIP [$name] set ${name}=1 equivalent switch to enable"
    SKIP=$((SKIP + 1))
  fi
}

cat <<EOF
=== Drive9 real dev e2e ===
BASE=$BASE
RUN_ID=$RUN_ID
ARTIFACT_DIR=$ARTIFACT_DIR
RUN_SEMANTIC_CHECKS=$RUN_SEMANTIC_CHECKS
RUN_CLI_SEMANTIC_CHECKS=$RUN_CLI_SEMANTIC_CHECKS
RUN_LAYER_FUSE_SMOKE=$RUN_LAYER_FUSE_SMOKE
RUN_FUSE_CONCURRENCY_STRESS=$RUN_FUSE_CONCURRENCY_STRESS
RUN_FUSE_POSIX_FSX=$RUN_FUSE_POSIX_FSX
RUN_FUSE_SQLITE_CORRECTNESS=$RUN_FUSE_SQLITE_CORRECTNESS
RUN_FUSE_PERFORMANCE_BASELINE=$RUN_FUSE_PERFORMANCE_BASELINE
RUN_REAL_E2E_SMOKE_ALL=$RUN_REAL_E2E_SMOKE_ALL
RUN_REAL_E2E_GIT_FEATURE_MATRIX=$RUN_REAL_E2E_GIT_FEATURE_MATRIX
EOF

if [ -z "$DRIVE9_EXISTING_API_KEY" ]; then
  DRIVE9_EXISTING_API_KEY="$(provision_existing_key)"
else
  echo "Using provided DRIVE9_EXISTING_API_KEY/DRIVE9_API_KEY for existing-key and git-ops smoke"
  wait_tenant_active "$DRIVE9_EXISTING_API_KEY"
fi

run_case "api-smoke" \
  env RUN_SEMANTIC_CHECKS="$RUN_SEMANTIC_CHECKS" \
    bash e2e/api-smoke-test.sh

run_case "existing-key-smoke" \
  env DRIVE9_API_KEY="$DRIVE9_EXISTING_API_KEY" \
    bash e2e/api-smoke-test-existing-key.sh

run_case "cli-smoke" \
  env RUN_CLI_SEMANTIC_CHECKS="$RUN_CLI_SEMANTIC_CHECKS" \
    CLI_SOURCE="$CLI_SOURCE" \
    bash e2e/cli-smoke-test.sh

run_case "layer-fs-smoke" \
  env RUN_LAYER_FUSE_SMOKE="$RUN_LAYER_FUSE_SMOKE" \
    LAYER_FUSE_STRICT_PREREQS="$LAYER_FUSE_STRICT_PREREQS" \
    CLI_SOURCE="$CLI_SOURCE" \
    bash e2e/layer-fs-smoke-test.sh

run_case "fuse-release-gate" \
  env FUSE_STRICT_PREREQS="$FUSE_STRICT_PREREQS" \
    RUN_FUSE_CONCURRENCY_STRESS="$RUN_FUSE_CONCURRENCY_STRESS" \
    RUN_FUSE_POSIX_FSX="$RUN_FUSE_POSIX_FSX" \
    RUN_FUSE_SQLITE_CORRECTNESS="$RUN_FUSE_SQLITE_CORRECTNESS" \
    RUN_FUSE_SQLITE_WAL="$RUN_FUSE_SQLITE_WAL" \
    RUN_FUSE_SQLITE_CHURN="$RUN_FUSE_SQLITE_CHURN" \
    RUN_FUSE_SQLITE_CONCURRENCY="$RUN_FUSE_SQLITE_CONCURRENCY" \
    RUN_FUSE_PERFORMANCE_BASELINE="$RUN_FUSE_PERFORMANCE_BASELINE" \
    CLI_SOURCE="$CLI_SOURCE" \
    bash e2e/fuse-release-gate.sh

run_case "git-ops-smoke" \
  env DRIVE9_API_KEY="$DRIVE9_EXISTING_API_KEY" \
    FUSE_STRICT_PREREQS="$FUSE_STRICT_PREREQS" \
    CLI_SOURCE="$CLI_SOURCE" \
    bash e2e/git-ops-smoke-test.sh

run_optional_case "$RUN_REAL_E2E_SMOKE_ALL" "RUN_REAL_E2E_SMOKE_ALL" \
  env DRIVE9_API_KEY="$DRIVE9_EXISTING_API_KEY" \
    FUSE_STRICT_PREREQS="$FUSE_STRICT_PREREQS" \
    RUN_FUSE_SMOKE=1 \
    RUN_LAYER_FUSE_SMOKE=1 \
    RUN_GIT_OPS_SMOKE=1 \
    RUN_GIT_WORKSPACE_SMOKE=1 \
    RUN_SEMANTIC_CHECKS="$RUN_SEMANTIC_CHECKS" \
    RUN_CLI_SEMANTIC_CHECKS="$RUN_CLI_SEMANTIC_CHECKS" \
    CLI_SOURCE="$CLI_SOURCE" \
    bash e2e/smoke-all.sh

run_optional_case "$RUN_REAL_E2E_GIT_FEATURE_MATRIX" "RUN_REAL_E2E_GIT_FEATURE_MATRIX" \
  env DRIVE9_API_KEY="$DRIVE9_EXISTING_API_KEY" \
    FUSE_STRICT_PREREQS="$FUSE_STRICT_PREREQS" \
    CLI_SOURCE="$CLI_SOURCE" \
    bash e2e/git-feature-matrix.sh

cat <<EOF

=== real dev e2e result ===
PASS=$PASS FAIL=$FAIL SKIP=$SKIP TOTAL=$TOTAL
ARTIFACT_DIR=$ARTIFACT_DIR
EOF

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
