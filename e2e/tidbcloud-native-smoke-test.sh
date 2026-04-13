#!/usr/bin/env bash
# drive9 tidbcloud-native smoke test against a live drive9-server deployment.
#
# This test exercises the tidbcloud-native provision path, which uses the
# X-TIDBCLOUD-ZERO-INSTANCE-ID header to provision a tenant backed by an
# existing TiDB Cloud Serverless cluster.
#
# Coverage:
#  1) Provision tenant via zero-instance header (expect 202)
#  2) Poll tenant status via GET /v1/status with instance ID header until active
#  3) Root list
#  4) Nested mkdir + multi-file write/read
#  5) Content search (grep) and attribute search (find)
#  6) Copy, rename, delete
#  7) Final list verification
#
# All API calls use X-TIDBCLOUD-ZERO-INSTANCE-ID header for authentication —
# the unified tidbcloud-native auth mode (provision included).
#
# Required environment variables:
#   DRIVE9_BASE                  — server URL (e.g. https://fs.dev.tidbapi.com)
#   TIDB_ZERO_INSTANCE_ID        — zero-instance ID from TiDB Cloud
#   TIDB_ZERO_ROOT_USER          — root username (e.g. prefix.root)
#   TIDB_ZERO_ROOT_PASSWORD      — root password
#
# Optional:
#   POLL_TIMEOUT_S               — max wait for tenant to become active (default: 180)
#   POLL_INTERVAL_S              — poll interval (default: 5)
#   REQUEST_MAX_RETRIES          — max retries on 429 (default: 8)
#   REQUEST_RETRY_SLEEP_S        — sleep between retries (default: 2)

set -euo pipefail

BASE="${DRIVE9_BASE:?DRIVE9_BASE is required}"
INSTANCE_ID="${TIDB_ZERO_INSTANCE_ID:?TIDB_ZERO_INSTANCE_ID is required}"
ROOT_USER="${TIDB_ZERO_ROOT_USER:?TIDB_ZERO_ROOT_USER is required}"
ROOT_PASSWORD="${TIDB_ZERO_ROOT_PASSWORD:?TIDB_ZERO_ROOT_PASSWORD is required}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-180}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"

PASS=0
FAIL=0
TOTAL=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RESET='\033[0m'

step() { echo -e "\n${YELLOW}[$1]${RESET} $2"; }
ok() { echo -e "${GREEN}  PASS${RESET} $*"; }
fail() { echo -e "${RED}  FAIL${RESET} $*"; }
info() { echo -e "${CYAN}  ->${RESET} $*"; }

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL+1))
  if [ "$got" = "$want" ]; then
    ok "$desc (got=$got)"
    PASS=$((PASS+1))
  else
    fail "$desc (want=$want got=$got)"
    FAIL=$((FAIL+1))
  fi
}

check_cmd() {
  local desc="$1"
  shift
  TOTAL=$((TOTAL+1))
  if "$@"; then
    ok "$desc"
    PASS=$((PASS+1))
  else
    fail "$desc"
    FAIL=$((FAIL+1))
  fi
}

curl_body_code() {
  local method="$1"
  local url="$2"
  local instance_id="$3"
  local data="${4:-}"

  local attempt=1
  while :; do
    local body_file
    body_file="$(mktemp)"
    local code
    local -a curl_args=(-sS -o "$body_file" -w "%{http_code}" -X "$method")
    curl_args+=(-H "X-TIDBCLOUD-ZERO-INSTANCE-ID: $instance_id")
    if [ -n "$data" ]; then
      curl_args+=(--data-binary "$data")
    fi
    code=$(curl "${curl_args[@]}" "$url")

    if [ "$code" != "429" ] || [ "$attempt" -ge "$REQUEST_MAX_RETRIES" ]; then
      cat "$body_file"
      echo
      echo "__HTTP__${code}"
      rm -f "$body_file"
      return
    fi

    info "throttled (429), retrying ${attempt}/${REQUEST_MAX_RETRIES}: $method $url"
    rm -f "$body_file"
    attempt=$((attempt + 1))
    sleep "$REQUEST_RETRY_SLEEP_S"
  done
}

curl_native_provision() {
  local url="$1"
  local instance_id="$2"
  local root_user="$3"
  local root_password="$4"

  local body_file
  body_file="$(mktemp)"
  local data
  data=$(jq -n --arg u "$root_user" --arg p "$root_password" '{user: $u, password: $p}')
  local code
  code=$(curl -sS -o "$body_file" -w "%{http_code}" -X POST \
    -H "X-TIDBCLOUD-ZERO-INSTANCE-ID: $instance_id" \
    -H "Content-Type: application/json" \
    --data-binary "$data" \
    "$url")
  cat "$body_file"
  echo
  echo "__HTTP__${code}"
  rm -f "$body_file"
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

echo "========================================================"
echo "  drive9 tidbcloud-native smoke test"
echo "  Base URL    : $BASE"
echo "  Instance ID : $INSTANCE_ID"
echo "  Started     : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "========================================================"

TS="$(date +%s)"
ROOT_DIR="native-e2e-${TS}"
NESTED_DIR="${ROOT_DIR}/sub/deep"

# ──────────────────────────────────────────────────────────────
step "1" "Provision tenant via X-TIDBCLOUD-ZERO-INSTANCE-ID"
resp=$(curl_native_provision "$BASE/v1/provision" "$INSTANCE_ID" "$ROOT_USER" "$ROOT_PASSWORD")
code=$(http_code "$resp")
body=$(json_body "$resp")

# Handle idempotent re-provision (409 = already provisioned)
if [ "$code" = "409" ]; then
  info "tenant already provisioned (409)"
  TOTAL=$((TOTAL+1)); PASS=$((PASS+1))
  ok "provision: tenant already exists"
else
  check_eq "POST /v1/provision returns 202" "$code" "202"
  API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
  INIT_STATUS=$(printf '%s' "$body" | jq -r '.status // empty')
  check_cmd "response contains api_key" test -n "$API_KEY"
  check_eq "provision status is provisioning" "$INIT_STATUS" "provisioning"
fi

# ──────────────────────────────────────────────────────────────
step "2" "Poll tenant status via /v1/status until active"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
LAST_STATUS=""
while :; do
  sresp=$(curl_body_code GET "$BASE/v1/status" "$INSTANCE_ID")
  scode=$(http_code "$sresp")
  sbody=$(json_body "$sresp")
  LAST_STATUS=$(printf '%s' "$sbody" | jq -r '.status // empty')
  info "status=$LAST_STATUS"
  if [ "$scode" = "200" ] && [ "$LAST_STATUS" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant status is active" "$LAST_STATUS" "active"

# ──────────────────────────────────────────────────────────────
step "3" "Root list"
resp=$(curl_body_code GET "$BASE/v1/fs/?list" "$INSTANCE_ID")
code=$(http_code "$resp")
check_eq "GET /v1/fs/?list returns 200" "$code" "200"

# ──────────────────────────────────────────────────────────────
step "4" "Nested mkdir"
resp=$(curl_body_code PUT "$BASE/v1/fs/${NESTED_DIR}/" "$INSTANCE_ID")
code=$(http_code "$resp")
check_eq "PUT nested dir returns 200 or 201" "$(echo "$code" | grep -cE '200|201')" "1"

# ──────────────────────────────────────────────────────────────
step "5" "Write files"
FILE_A="${NESTED_DIR}/hello.txt"
FILE_B="${NESTED_DIR}/world.txt"
CONTENT_A="Hello from tidbcloud-native e2e test ${TS}"
CONTENT_B="World file content ${TS}"

resp=$(curl_body_code PUT "$BASE/v1/fs/${FILE_A}" "$INSTANCE_ID" "$CONTENT_A")
code=$(http_code "$resp")
check_eq "PUT ${FILE_A} returns 200" "$code" "200"

resp=$(curl_body_code PUT "$BASE/v1/fs/${FILE_B}" "$INSTANCE_ID" "$CONTENT_B")
code=$(http_code "$resp")
check_eq "PUT ${FILE_B} returns 200" "$code" "200"

# ──────────────────────────────────────────────────────────────
step "6" "Read files back"
resp=$(curl_body_code GET "$BASE/v1/fs/${FILE_A}" "$INSTANCE_ID")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET ${FILE_A} returns 200" "$code" "200"
check_eq "content matches" "$body" "$CONTENT_A"

resp=$(curl_body_code GET "$BASE/v1/fs/${FILE_B}" "$INSTANCE_ID")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET ${FILE_B} returns 200" "$code" "200"
check_eq "content matches" "$body" "$CONTENT_B"

# ──────────────────────────────────────────────────────────────
step "7" "List nested directory"
resp=$(curl_body_code GET "$BASE/v1/fs/${NESTED_DIR}/?list" "$INSTANCE_ID")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET list returns 200" "$code" "200"
count=$(printf '%s' "$body" | jq 'length')
check_eq "nested dir has 2 files" "$count" "2"

# ──────────────────────────────────────────────────────────────
step "8" "Grep search"
query=$(printf '%s' "tidbcloud-native" | jq -sRr @uri)
resp=$(curl_body_code GET "$BASE/v1/fs/${ROOT_DIR}/?grep=${query}&limit=10" "$INSTANCE_ID")
code=$(http_code "$resp")
check_eq "grep returns 200" "$code" "200"
body=$(json_body "$resp")
found=$(printf '%s' "$body" | jq -r '.[].path // empty' | grep -c "hello.txt" || true)
check_cmd "grep finds hello.txt" test "$found" -ge 1

# ──────────────────────────────────────────────────────────────
step "9" "Find by name"
resp=$(curl_body_code GET "$BASE/v1/fs/${ROOT_DIR}/?find=&name=*.txt&limit=10" "$INSTANCE_ID")
code=$(http_code "$resp")
check_eq "find returns 200" "$code" "200"
body=$(json_body "$resp")
count=$(printf '%s' "$body" | jq 'length')
check_cmd "find returns at least 2 .txt files" test "$count" -ge 2

# ──────────────────────────────────────────────────────────────
step "10" "Copy file"
COPY_TARGET="${NESTED_DIR}/hello-copy.txt"
resp=$(curl_body_code POST "$BASE/v1/fs/${FILE_A}?cp=${COPY_TARGET}" "$INSTANCE_ID")
code=$(http_code "$resp")
check_eq "copy returns 200" "$code" "200"

resp=$(curl_body_code GET "$BASE/v1/fs/${COPY_TARGET}" "$INSTANCE_ID")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "copied file returns 200" "$code" "200"
check_eq "copied content matches" "$body" "$CONTENT_A"

# ──────────────────────────────────────────────────────────────
step "11" "Rename file"
RENAME_TARGET="${NESTED_DIR}/hello-renamed.txt"
resp=$(curl_body_code POST "$BASE/v1/fs/${COPY_TARGET}?mv=${RENAME_TARGET}" "$INSTANCE_ID")
code=$(http_code "$resp")
check_eq "rename returns 200" "$code" "200"

resp=$(curl_body_code GET "$BASE/v1/fs/${RENAME_TARGET}" "$INSTANCE_ID")
code=$(http_code "$resp")
check_eq "renamed file accessible" "$code" "200"

resp=$(curl_body_code GET "$BASE/v1/fs/${COPY_TARGET}" "$INSTANCE_ID")
code=$(http_code "$resp")
check_eq "old path returns 404" "$code" "404"

# ──────────────────────────────────────────────────────────────
step "12" "Delete file"
resp=$(curl_body_code DELETE "$BASE/v1/fs/${RENAME_TARGET}" "$INSTANCE_ID")
code=$(http_code "$resp")
check_eq "delete returns 200" "$code" "200"

resp=$(curl_body_code GET "$BASE/v1/fs/${RENAME_TARGET}" "$INSTANCE_ID")
code=$(http_code "$resp")
check_eq "deleted file returns 404" "$code" "404"

# ──────────────────────────────────────────────────────────────
step "13" "Final list verification"
resp=$(curl_body_code GET "$BASE/v1/fs/${NESTED_DIR}/?list" "$INSTANCE_ID")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "final list returns 200" "$code" "200"
count=$(printf '%s' "$body" | jq 'length')
check_eq "nested dir has 2 files after mutations" "$count" "2"

# ──────────────────────────────────────────────────────────────
echo ""
echo "========================================================"
echo "  tidbcloud-native smoke test complete"
echo "  PASS=$PASS  FAIL=$FAIL  TOTAL=$TOTAL"
echo "========================================================"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
