#!/usr/bin/env bash
# dat9 API smoke test against a live dat9-server deployment.
#
# Coverage:
#  1) Create tenant (expect 202, id + name + api_key + status)
#  2) Poll tenant status via GET /v1/status until active
#  3) Root list
#  4) Nested mkdir (multi-level directories)
#  5) Multi-file write/read under nested directories
#  6) Copy, rename, delete
#  7) Final list verification

set -euo pipefail

BASE="${DAT9_BASE:-http://127.0.0.1:9009}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"

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
  local auth="${3:-}"
  local data="${4:-}"

  local body_file
  body_file="$(mktemp)"

  if [ -n "$auth" ] && [ -n "$data" ]; then
    local code
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" --data-binary "$data" "$url")
    cat "$body_file"
    echo
    echo "__HTTP__${code}"
    rm -f "$body_file"
    return
  fi

  if [ -n "$auth" ]; then
    local code
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
    cat "$body_file"
    echo
    echo "__HTTP__${code}"
    rm -f "$body_file"
    return
  fi

  local code
  code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
  cat "$body_file"
  echo
  echo "__HTTP__${code}"
  rm -f "$body_file"
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

echo "========================================================"
echo "  dat9 API smoke test"
echo "  Base URL : $BASE"
echo "  Started  : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "========================================================"

TS="$(date +%s)"
ROOT_DIR="team-${TS}"
BACKEND_DIR="${ROOT_DIR}/backend/go"
FRONTEND_DIR="${ROOT_DIR}/frontend/web"

step "1" "Create tenant"
resp=$(curl_body_code POST "$BASE/v1/create")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "POST /v1/create returns 202" "$code" "202"

TENANT_ID=$(printf '%s' "$body" | jq -r '.id // empty')
TENANT_NAME=$(printf '%s' "$body" | jq -r '.name // empty')
API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
INIT_STATUS=$(printf '%s' "$body" | jq -r '.status // empty')
check_cmd "response contains id" test -n "$TENANT_ID"
check_cmd "response contains name" test -n "$TENANT_NAME"
check_cmd "response contains api_key" test -n "$API_KEY"
check_eq "provision response status is provisioning" "$INIT_STATUS" "provisioning"
keys=$(printf '%s' "$body" | jq -r 'keys_unsorted | sort | join(",")')
check_eq "create response has id,name,api_key,status" "$keys" "api_key,id,name,status"

step "2" "Poll tenant status via /v1/status"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
LAST_STATUS=""
while :; do
  sresp=$(curl_body_code GET "$BASE/v1/status" "$API_KEY")
  scode=$(http_code "$sresp")
  sbody=$(json_body "$sresp")
  LAST_STATUS=$(printf '%s' "$sbody" | jq -r '.status // empty')
  STATUS_ID=$(printf '%s' "$sbody" | jq -r '.id // empty')
  STATUS_NAME=$(printf '%s' "$sbody" | jq -r '.name // empty')
  info "status=$LAST_STATUS"
  if [ "$scode" = "200" ] && [ "$LAST_STATUS" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant eventually becomes active" "$LAST_STATUS" "active"
check_eq "status response keeps same id" "$STATUS_ID" "$TENANT_ID"
check_eq "status response keeps same name" "$STATUS_NAME" "$TENANT_NAME"

step "3" "Root list"
resp=$(curl_body_code GET "$BASE/v1/fs/?list" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET /v1/fs/?list returns 200" "$code" "200"
entries_type=$(printf '%s' "$body" | jq -r '.entries | type')
check_eq "list response contains entries array" "$entries_type" "array"

step "4" "Create nested directories"
for d in "$ROOT_DIR" "${ROOT_DIR}/backend" "$BACKEND_DIR" "${ROOT_DIR}/frontend" "$FRONTEND_DIR"; do
  resp=$(curl_body_code POST "$BASE/v1/fs/$d?mkdir" "$API_KEY")
  code=$(http_code "$resp")
  check_eq "POST /v1/fs/$d?mkdir returns 200" "$code" "200"
done

step "5" "Write and read multiple files"
declare -a FILES
FILES=(
  "$ROOT_DIR/README.md|team-root-$TS"
  "$BACKEND_DIR/main.go|package main\n// smoke-$TS\nfunc main() {}\n"
  "$FRONTEND_DIR/index.html|<html><body>smoke-$TS</body></html>"
  "$BACKEND_DIR/config.yaml|env: smoke-$TS"
)

for item in "${FILES[@]}"; do
  path="${item%%|*}"
  payload="${item#*|}"
  resp=$(curl_body_code PUT "$BASE/v1/fs/$path" "$API_KEY" "$payload")
  code=$(http_code "$resp")
  check_eq "PUT /v1/fs/$path returns 200" "$code" "200"

  rresp=$(curl_body_code GET "$BASE/v1/fs/$path" "$API_KEY")
  rcode=$(http_code "$rresp")
  rbody=$(json_body "$rresp")
  check_eq "GET /v1/fs/$path returns 200" "$rcode" "200"
  check_eq "read back content matches for $path" "$rbody" "$payload"
done

step "6" "Copy, rename, delete"
resp=$(curl -sS -o /tmp/dat9-copy.out -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "X-Dat9-Copy-Source: /$ROOT_DIR/README.md" "$BASE/v1/fs/$ROOT_DIR/README-copy.md?copy")
check_eq "POST ?copy returns 200" "$resp" "200"

resp=$(curl -sS -o /tmp/dat9-rename.out -w "%{http_code}" -X POST -H "Authorization: Bearer $API_KEY" -H "X-Dat9-Rename-Source: /$BACKEND_DIR/config.yaml" "$BASE/v1/fs/$BACKEND_DIR/config-renamed.yaml?rename")
check_eq "POST ?rename returns 200" "$resp" "200"

resp=$(curl -sS -o /tmp/dat9-delete.out -w "%{http_code}" -X DELETE -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/$ROOT_DIR/README-copy.md")
check_eq "DELETE copied file returns 200" "$resp" "200"

step "7" "Final list verification"
resp=$(curl_body_code GET "$BASE/v1/fs/$ROOT_DIR?list" "$API_KEY")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET /v1/fs/$ROOT_DIR?list returns 200" "$code" "200"
backend_exists=$(printf '%s' "$body" | jq -r 'any(.entries[]; .name=="backend" and .isDir==true)')
frontend_exists=$(printf '%s' "$body" | jq -r 'any(.entries[]; .name=="frontend" and .isDir==true)')
copy_exists=$(printf '%s' "$body" | jq -r 'any(.entries[]; .name=="README-copy.md")')
check_eq "backend directory still exists" "$backend_exists" "true"
check_eq "frontend directory still exists" "$frontend_exists" "true"
check_eq "copied file removed" "$copy_exists" "false"

rm -f /tmp/dat9-copy.out /tmp/dat9-rename.out /tmp/dat9-delete.out

echo
echo "========================================================"
echo "  RESULTS: $PASS / $TOTAL passed, $FAIL failed"
echo "  Base URL : $BASE"
echo "  Finished : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
if [ "$FAIL" -eq 0 ]; then
  echo -e "  ${GREEN}All tests passed.${RESET}"
else
  echo -e "  ${RED}$FAIL test(s) failed.${RESET}"
fi
echo "========================================================"

exit "$FAIL"
