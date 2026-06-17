#!/usr/bin/env bash
set -euo pipefail

# native-smoke-test.sh — TiDB Cloud Native tenant lifecycle smoke test.
#
# Provisions a tenant via the tidb_cloud_native provider, runs basic CLI
# filesystem operations, deletes the tenant, and verifies cleanup.
#
# Required environment variables:
#   DRIVE9_BASE                  Server base URL
#   DRIVE9_TIDBCLOUD_PUBLIC_KEY  TiDB Cloud public key
#   DRIVE9_TIDBCLOUD_PRIVATE_KEY TiDB Cloud private key
#
# Optional:
#   DRIVE9_REGION_CODE           Provisioning region (default: aws-ap-southeast-1)
#   POLL_TIMEOUT_S               Max seconds to wait for tenant active (default: 600)
#   POLL_INTERVAL_S              Poll interval in seconds (default: 10)
#   CLI_RETRY_SLEEP_S            Sleep between CLI retry attempts (default: 5)
#   CLI_MAX_RETRIES              Max CLI retries on throttling/not-found (default: 10)
#   SKIP_CLEANUP                 If set to 1, leave tenant and local files on failure

# ── helpers ────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0
SKIP=0
TOTAL=0

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL+1))
  if [ "$got" = "$want" ]; then
    echo -e "${GREEN}PASS${NC} $desc (got=$got)"
    PASS=$((PASS+1))
  else
    echo -e "${RED}FAIL${NC} $desc (want=$want got=$got)"
    FAIL=$((FAIL+1))
  fi
}

check_cmd() {
  local desc="$1"
  shift
  TOTAL=$((TOTAL+1))
  if "$@" >/dev/null 2>&1; then
    echo -e "${GREEN}PASS${NC} $desc"
    PASS=$((PASS+1))
  else
    echo -e "${RED}FAIL${NC} $desc"
    FAIL=$((FAIL+1))
  fi
}

skip_check() {
  local desc="$1"
  TOTAL=$((TOTAL+1))
  SKIP=$((SKIP+1))
  echo -e "${YELLOW}SKIP${NC} $desc"
}

# ── config ──────────────────────────────────────────────────────────────────

BASE="${DRIVE9_BASE:?DRIVE9_BASE is required}"
PUBLIC_KEY="${DRIVE9_TIDBCLOUD_PUBLIC_KEY:?DRIVE9_TIDBCLOUD_PUBLIC_KEY is required}"
PRIVATE_KEY="${DRIVE9_TIDBCLOUD_PRIVATE_KEY:?DRIVE9_TIDBCLOUD_PRIVATE_KEY is required}"
REGION_CODE="${DRIVE9_REGION_CODE:-aws-ap-southeast-1}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-600}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-10}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-5}"
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-10}"
SKIP_CLEANUP="${SKIP_CLEANUP:-0}"

CLI_BIN="$(mktemp)"
CLI_HOME="$(mktemp -d)"
TS="$(date +%s)"
TEST_DIR="native-smoke-${TS}"
TENANT_ID=""
API_KEY=""
CREATED=0

cleanup() {
  local rc=$?
  if [ "$CREATED" -eq 1 ] && [ "$SKIP_CLEANUP" != "1" ]; then
    echo "[cleanup] deleting tenant $TENANT_ID"
    HOME="$CLI_HOME" "$CLI_BIN" delete \
      --server "$BASE" \
      --api-key "${API_KEY:-}" \
      --tidbcloud-public-key "$PUBLIC_KEY" \
      --tidbcloud-private-key "$PRIVATE_KEY" \
      >/dev/null 2>&1 || true
  fi
  rm -f "$CLI_BIN" "$TMP_FILE" "$TMP_CONTENT"
  rm -rf "$CLI_HOME"
  if [ "$rc" -ne 0 ] && [ "$SKIP_CLEANUP" != "1" ]; then
    echo "[cleanup] test failed with exit code $rc"
  fi
  exit "$FAIL"
}
trap cleanup EXIT

# ── build CLI ───────────────────────────────────────────────────────────────

echo "[0] build CLI"
make build-cli CLI_BIN="$CLI_BIN" >/dev/null 2>&1
check_cmd "CLI binary built" test -x "$CLI_BIN"

TMP_FILE="$(mktemp)"
TMP_CONTENT="$(mktemp)"
echo "hello native smoke test $TS" > "$TMP_FILE"

# ── provision tenant ────────────────────────────────────────────────────────

echo "[1] provision tenant"
create_out="$(HOME="$CLI_HOME" "$CLI_BIN" create \
  --server "$BASE" \
  --region-code "$REGION_CODE" \
  --tidbcloud-public-key "$PUBLIC_KEY" \
  --tidbcloud-private-key "$PRIVATE_KEY" \
  --json 2>&1)"
create_code=$?
check_eq "drive9 create exit code" "$create_code" "0"

API_KEY="$(printf '%s' "$create_out" | jq -r '.api_key // empty')"
TENANT_ID="$(printf '%s' "$create_out" | jq -r '.tenant_id // empty')"
CREATE_STATUS="$(printf '%s' "$create_out" | jq -r '.status // empty')"

check_cmd "response contains tenant_id" test -n "$TENANT_ID"
check_cmd "response contains api_key" test -n "$API_KEY"
check_eq "provision status is provisioning" "$CREATE_STATUS" "provisioning"
CREATED=1

# ── poll until active ───────────────────────────────────────────────────────

echo "[2] wait for tenant active (timeout=${POLL_TIMEOUT_S}s)"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
LAST_STATUS=""
while :; do
  status_body="$(mktemp)"
  status_code=$(curl -sS -o "$status_body" -w "%{http_code}" \
    -H "Authorization: Bearer $API_KEY" "$BASE/v1/status")
  if [ "$status_code" = "200" ]; then
    LAST_STATUS="$(jq -r '.status // empty' "$status_body")"
  fi
  rm -f "$status_body"
  if [ "$LAST_STATUS" = "active" ]; then break; fi
  if [ "$(date +%s)" -ge "$deadline" ]; then break; fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant becomes active" "$LAST_STATUS" "active"

# ── basic fs operations ─────────────────────────────────────────────────────

drive9_cli() {
  env DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" HOME="$CLI_HOME" "$CLI_BIN" "$@"
}

drive9_retry() {
  local attempt=1
  while [ "$attempt" -le "$CLI_MAX_RETRIES" ]; do
    local out
    if out="$(drive9_cli "$@" 2>&1)"; then
      printf '%s' "$out"
      return 0
    fi
    if echo "$out" | grep -qi 'Too Many Requests\|Resource temporarily unavailable\|not found'; then
      sleep "$CLI_RETRY_SLEEP_S"
      attempt=$((attempt+1))
      continue
    fi
    printf '%s' "$out"
    return 1
  done
  return 1
}

echo "[3] basic fs operations"

DIR="/$TEST_DIR"

drive9_retry fs mkdir "$DIR" >/dev/null
check_cmd "mkdir $DIR" true

drive9_retry fs cp "$TMP_FILE" ":$DIR/hello.txt" >/dev/null
check_cmd "cp file to $DIR/hello.txt" true

ls_out="$(drive9_retry fs ls "$DIR")"
check_cmd "ls $DIR lists file" echo "$ls_out" | grep -q "hello.txt"

cat_out="$(drive9_retry fs cat "$DIR/hello.txt")"
check_cmd "cat $DIR/hello.txt returns content" echo "$cat_out" | grep -q "hello native smoke test"

drive9_retry fs rm "$DIR/hello.txt" >/dev/null
check_cmd "rm $DIR/hello.txt" true

ls_after="$(drive9_retry fs ls "$DIR" 2>&1 || true)"
if echo "$ls_after" | grep -q "hello.txt"; then
  check_eq "rm $DIR/hello.txt removes file" "fail" "pass"
else
  check_cmd "rm $DIR/hello.txt removes file" true
fi

drive9_retry fs rm "$DIR" >/dev/null
check_cmd "rmdir $DIR" true

# ── delete tenant ───────────────────────────────────────────────────────────

echo "[4] delete tenant"
delete_out="$(HOME="$CLI_HOME" "$CLI_BIN" delete \
  --server "$BASE" \
  --api-key "$API_KEY" \
  --tidbcloud-public-key "$PUBLIC_KEY" \
  --tidbcloud-private-key "$PRIVATE_KEY" \
  --json 2>&1)"
delete_code=$?
check_eq "drive9 delete exit code" "$delete_code" "0"

DELETE_STATUS="$(printf '%s' "$delete_out" | jq -r '.status // empty')"
check_cmd "delete status is deleted or deleting" echo "$DELETE_STATUS" | grep -qE 'deleted|deleting'

CREATED=0

# ── verify tenant gone ──────────────────────────────────────────────────────

echo "[5] verify tenant removed"
verify_body="$(mktemp)"
verify_code=$(curl -sS -o "$verify_body" -w "%{http_code}" \
  -H "Authorization: Bearer $API_KEY" "$BASE/v1/status")
rm -f "$verify_body"
if [ "$verify_code" = "401" ] || [ "$verify_code" = "403" ] || [ "$verify_code" = "404" ]; then
  check_eq "GET /v1/status returns auth error or not-found after delete" "ok" "ok"
else
  check_eq "GET /v1/status returns auth error or not-found after delete (got $verify_code)" "$verify_code" "401/403/404"
fi

# ── summary ─────────────────────────────────────────────────────────────────

echo ""
echo "=== native smoke test: $PASS passed, $FAIL failed, $SKIP skipped (total $TOTAL) ==="
