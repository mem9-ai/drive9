#!/usr/bin/env bash
# drive9 fs task status endpoint smoke test (server HTTP only).
#
# Covers the data-plane wire contract behind `drive9 fs tasks`:
#   - a successful response carries the X-Dat9-Tasks: 1 marker
#   - the body is {path, tasks[]} with task_type/status and optional last_error
#   - tasks is always an array, never null (including an empty result)
#   - a directory is rejected with 400
#   - unknown query keys are not silently accepted on the tasks arm
#
# The handler lives in the external server repository while this smoke lives
# with the CLI, so it is the cross-repo drift check for the marker name, the
# compared value, and the response field names. An older server that lacks the
# endpoint answers with the plain-read fall-through (file bytes or a 302) and no
# marker, which is why the CLI refuses to decode it.
#
# Tenant mode:
#   - Fresh (default): POST /v1/provision, then run the suite with the returned
#     owner key.
#   - Existing (DRIVE9_API_KEY set): reuse that owner key.
#
# Opt-in (not part of the default smoke-all / local-e2e PR set; enable from an
# integrator that points the server binary at a build with ?tasks):
#   RUN_TASKS_SMOKE=1 bash e2e/smoke-all.sh
#   export DRIVE9_BASE=http://127.0.0.1:9009
#   bash e2e/tasks-smoke-test.sh
#
# DRIVE9_BASE may use http:// only for loopback; every other target must be
# https:// so the owner key is never sent in cleartext.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-300}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"

# The owner key is a credential: never send it in cleartext. http:// is allowed
# only for loopback (single-machine dev); every other target must be https://.
# Default-deny: classify the scheme case-insensitively, allow only https, and
# route http through the loopback host check. Anything else (a missing scheme
# such as "host:9009", an unknown scheme, or an unparseable authority) is
# refused rather than assumed safe. Mirrors embedding-config-smoke-test.sh's
# HTTPS requirement while keeping http for local dev.
base_lower="$(printf '%s' "$BASE" | tr 'A-Z' 'a-z')"
base_scheme="${base_lower%%:*}"
if [ "$base_scheme" = "$base_lower" ]; then
  base_scheme="" # No colon: curl would default this to cleartext http.
fi
if [ "$base_scheme" != "https" ]; then
  base_authority=""
  if [ "$base_scheme" = "http" ]; then
    base_authority="${base_lower#*:}"          # drop the scheme
    base_authority="${base_authority#//}"      # drop two slashes (http://host)
    base_authority="${base_authority#/}"       # drop one slash (http:/host)
    base_authority="${base_authority%%[/?#]*}" # drop path/query/fragment
    base_authority="${base_authority##*@}"     # drop userinfo
  fi
  case "$base_authority" in
    127.0.0.1|127.0.0.1:*|localhost|localhost:*|'[::1]'|'[::1]':*) ;;
    *)
      echo "FATAL: DRIVE9_BASE must use https:// (http:// is allowed only for loopback) so the owner key is not sent in cleartext: $BASE" >&2
      exit 2
      ;;
  esac
fi

for command in curl jq; do
  command -v "$command" >/dev/null || {
    echo "FATAL: $command is required" >&2
    exit 2
  }
done

PASS=0
FAIL=0
SKIP=0
TOTAL=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RESET='\033[0m'

step() { echo -e "\n${YELLOW}[$1]${RESET} $2"; }
ok() { TOTAL=$((TOTAL+1)); PASS=$((PASS+1)); echo -e "${GREEN}  PASS${RESET} $*"; }
fail() { TOTAL=$((TOTAL+1)); FAIL=$((FAIL+1)); echo -e "${RED}  FAIL${RESET} $*"; }
skip_check() { local desc="$1"; TOTAL=$((TOTAL+1)); SKIP=$((SKIP+1)); echo -e "${YELLOW}  SKIP${RESET} $desc"; }
info() { echo "  -> $*"; }

check_eq() {
  local desc="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    ok "$desc (got=$got)"
  else
    fail "$desc (want=$want got=$got)"
  fi
}

check_cmd() {
  local desc="$1"
  shift
  if "$@"; then
    ok "$desc"
  else
    fail "$desc"
  fi
}

HDR_FILE="$(mktemp)"
BODY_FILE="$(mktemp)"
# Delete the tree on any exit, including a hard abort, so a failed run does not
# leak a tasks-smoke-<ts>/ directory. The delete is idempotent and guarded by a
# provisioned key and directory; step 7 repeats it on the happy path.
cleanup() {
  local status=$?
  trap - EXIT
  if [ -n "$API_KEY" ] && [ -n "${DIR:-}" ]; then
    curl -sS -o /dev/null -X DELETE \
      -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/$DIR?recursive" || true
  fi
  rm -f "$HDR_FILE" "$BODY_FILE"
  exit "$status"
}
trap cleanup EXIT

# http returns "<body>__HTTP__<code>" and leaves response headers in $HDR_FILE.
http() {
  local method="$1" url="$2" data="${3:-}"
  local attempt=1
  while :; do
    local code
    if [ -n "$data" ]; then
      code=$(curl -sS -D "$HDR_FILE" -o "$BODY_FILE" -w "%{http_code}" -X "$method" \
        -H "Authorization: Bearer $API_KEY" --data-binary "$data" "$url") || code="000"
    else
      code=$(curl -sS -D "$HDR_FILE" -o "$BODY_FILE" -w "%{http_code}" -X "$method" \
        -H "Authorization: Bearer $API_KEY" "$url") || code="000"
    fi
    if [ "$code" != "429" ] || [ "$attempt" -ge "$REQUEST_MAX_RETRIES" ]; then
      printf '%s\n__HTTP__%s' "$(cat "$BODY_FILE")" "$code"
      return
    fi
    info "throttled (429), retrying ${attempt}/${REQUEST_MAX_RETRIES}"
    attempt=$((attempt+1))
    sleep "$REQUEST_RETRY_SLEEP_S"
  done
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }
header_value() {
  awk -F': *' -v name="$1" 'tolower($1)==tolower(name){v=$2} END{gsub(/\r/,"",v); print v}' "$HDR_FILE"
}

echo "========================================================"
echo "  drive9 fs tasks smoke test"
echo "  Base URL : $BASE"
echo "  Tenant   : $([ -n "$API_KEY" ] && echo "existing (DRIVE9_API_KEY)" || echo "fresh provision")"
echo "  Started  : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "========================================================"

step "1" "Provision / resolve tenant"
if [ -z "$API_KEY" ]; then
  resp=$(http POST "$BASE/v1/provision")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "POST /v1/provision returns 202" "$code" "202"
  API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
  check_cmd "provision response contains api_key" test -n "$API_KEY"
else
  info "using existing DRIVE9_API_KEY (skip provision)"
  skip_check "POST /v1/provision returns 202"
  skip_check "provision response contains api_key"
fi

step "2" "Poll tenant status via /v1/status"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
while :; do
  resp=$(http GET "$BASE/v1/status")
  code=$(http_code "$resp")
  if [ "$code" = "000" ]; then
    # A refused/unreachable transport is not a slow tenant; fail fast instead
    # of waiting out POLL_TIMEOUT_S.
    fail "GET /v1/status transport error (code 000)"
    echo
    echo "RESULT: $PASS passed, $FAIL failed, $SKIP skipped, $TOTAL total"
    exit 1
  fi
  body=$(json_body "$resp")
  status=$(printf '%s' "$body" | jq -r '.status // empty')
  info "status=$status"
  if [ "$code" = "200" ] && [ "$status" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    fail "tenant not active within ${POLL_TIMEOUT_S}s (last=$status)"
    echo
    echo "RESULT: $PASS passed, $FAIL failed, $SKIP skipped, $TOTAL total"
    exit 1
  fi
  sleep "$POLL_INTERVAL_S"
done
ok "tenant is active"

TS="$(date +%s)"
DIR="tasks-smoke-${TS}"
FILE="${DIR}/doc-${TS}.txt"

step "3" "Write a file and a directory"
resp=$(http PUT "$BASE/v1/fs/$FILE" "task status smoke")
check_eq "PUT file returns 200" "$(http_code "$resp")" "200"
resp=$(http POST "$BASE/v1/fs/$DIR?mkdir")
check_eq "POST mkdir returns 200" "$(http_code "$resp")" "200"

step "4" "GET ?tasks on the file"
resp=$(http GET "$BASE/v1/fs/$FILE?tasks=1")
code=$(http_code "$resp")
body=$(json_body "$resp")
check_eq "GET ?tasks returns 200" "$code" "200"
check_eq "X-Dat9-Tasks marker" "$(header_value X-Dat9-Tasks)" "1"
check_cmd "body is valid JSON" bash -c 'printf "%s" "$1" | jq -e . >/dev/null' _ "$body"
check_cmd "body.path is the requested file" bash -c '[ "$(printf "%s" "$1" | jq -r ".path")" = "$2" ]' _ "$body" "/$FILE"
check_cmd "tasks is an array (never null)" bash -c 'printf "%s" "$1" | jq -e "(.tasks|type)==\"array\"" >/dev/null' _ "$body"
check_cmd "task rows carry string task_type/status" bash -c 'printf "%s" "$1" | jq -e "[.tasks[]? | select((.task_type|type)!=\"string\" or (.status|type)!=\"string\")] | length == 0" >/dev/null' _ "$body"
check_cmd "task status is a known value" bash -c 'printf "%s" "$1" | jq -e "[.tasks[]? | select(.status | IN(\"queued\",\"processing\",\"succeeded\",\"failed\") | not)] | length == 0" >/dev/null' _ "$body"

step "5" "?tasks rejects a directory"
resp=$(http GET "$BASE/v1/fs/$DIR?tasks=1")
check_eq "directory ?tasks returns 400" "$(http_code "$resp")" "400"

step "6" "?tasks rejects unknown query keys"
resp=$(http GET "$BASE/v1/fs/$FILE?tasks=1&bogus=1")
code=$(http_code "$resp")
check_cmd "unknown key rejected (got $code)" bash -c 'case "$1" in 400|403) exit 0;; *) exit 1;; esac' _ "$code"

step "7" "Cleanup"
resp=$(http DELETE "$BASE/v1/fs/$FILE")
info "delete file: $(http_code "$resp")"
resp=$(http DELETE "$BASE/v1/fs/$DIR?recursive")
info "delete directory: $(http_code "$resp")"

echo
echo "RESULT: $PASS passed, $FAIL failed, $SKIP skipped, $TOTAL total"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
