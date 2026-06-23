#!/usr/bin/env bash
set -euo pipefail

# native-smoke-test.sh — TiDB Cloud Native tenant lifecycle smoke test.
# Manual-only: requires TiDB Cloud API credentials (public/private key).

BASE="${DRIVE9_BASE:?DRIVE9_BASE is required}"
PUBLIC_KEY="${DRIVE9_TIDBCLOUD_PUBLIC_KEY:?DRIVE9_TIDBCLOUD_PUBLIC_KEY is required}"
PRIVATE_KEY="${DRIVE9_TIDBCLOUD_PRIVATE_KEY:?DRIVE9_TIDBCLOUD_PRIVATE_KEY is required}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-600}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-10}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-8}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-2}"
SKIP_CLEANUP="${SKIP_CLEANUP:-0}"

# ── helpers ────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'
PASS=0 FAIL=0 SKIP=0 TOTAL=0

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
  local desc="$1"; shift
  TOTAL=$((TOTAL+1))
  if "$@" >/dev/null 2>&1; then
    echo -e "${GREEN}PASS${NC} $desc"
    PASS=$((PASS+1))
  else
    echo -e "${RED}FAIL${NC} $desc"
    FAIL=$((FAIL+1))
  fi
}

# ── prepare CLI ─────────────────────────────────────────────────────────────

download_official_cli() {
  local os arch
  case "$(uname -s)" in Linux) os="linux" ;; Darwin) os="darwin" ;; *) echo "unsupported OS" >&2; return 1 ;; esac
  case "$(uname -m)" in x86_64|amd64) arch="amd64" ;; aarch64|arm64) arch="arm64" ;; *) echo "unsupported arch" >&2; return 1 ;; esac
  curl -fsSL "${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}/drive9-${os}-${arch}" -o "$CLI_BIN"
  chmod +x "$CLI_BIN"
}

prepare_cli_binary() {
  CLI_BIN="$(mktemp)"
  case "$CLI_SOURCE" in
    build) make build-cli CLI_BIN="$CLI_BIN" ;;
    official) download_official_cli ;;
    *) echo "invalid CLI_SOURCE: $CLI_SOURCE (expected build|official)" >&2; return 1 ;;
  esac
}

echo "=== drive9 native smoke test ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
else
  check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
fi

prepare_cli_binary
check_cmd "drive9 binary ready" test -x "$CLI_BIN"

CLI_HOME="$(mktemp -d)"
TS="$(date +%s)"
TEST_DIR="native-smoke-${TS}"
TENANT_ID=""
API_KEY=""
CREATED=0
TMP_FILE="$(mktemp)"

drive9() {
  env DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" HOME="$CLI_HOME" "$CLI_BIN" "$@"
}

drive9_ctx() {
  env -u DRIVE9_SERVER -u DRIVE9_API_KEY HOME="$CLI_HOME" "$CLI_BIN" "$@"
}

drive9_retry() {
  local attempt=1
  while [ "$attempt" -le "$CLI_MAX_RETRIES" ]; do
    local out
    if out="$(drive9 "$@" 2>&1)"; then
      printf '%s' "$out"; return 0
    fi
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* || "$out" == *"HTTP 502"* || "$out" == *"not found"* || "$out" == *"unexpected EOF"* || "$out" == *"connection reset"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for drive9 $* " >&2
      sleep "$CLI_RETRY_SLEEP_S"; attempt=$((attempt+1)); continue
    fi
    printf '%s' "$out" >&2; return 1
  done
  return 1
}

drive9_ctx_retry() {
  local attempt=1
  while [ "$attempt" -le "$CLI_MAX_RETRIES" ]; do
    local out
    if out="$(drive9_ctx "$@" 2>&1)"; then
      printf '%s' "$out"; return 0
    fi
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* || "$out" == *"HTTP 502"* || "$out" == *"not found"* || "$out" == *"unexpected EOF"* || "$out" == *"connection reset"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for drive9 ctx $* " >&2
      sleep "$CLI_RETRY_SLEEP_S"; attempt=$((attempt+1)); continue
    fi
    printf '%s' "$out" >&2; return 1
  done
  return 1
}

cleanup() {
  if [ "$CREATED" -eq 1 ] && [ "$SKIP_CLEANUP" != "1" ] && [ -n "${TENANT_ID:-}" ]; then
    echo "[cleanup] deleting tenant $TENANT_ID" >&2
    for i in $(seq 1 3); do
      if drive9_ctx delete \
        --server "$BASE" \
        --api-key "${API_KEY:-}" \
        --tidbcloud-public-key "$PUBLIC_KEY" \
        --tidbcloud-private-key "$PRIVATE_KEY" \
        -y \
        >/tmp/native-cleanup.log 2>&1; then
        echo "[cleanup] tenant $TENANT_ID deleted" >&2
        break
      fi
      echo "[cleanup] retry $i/3 deleting tenant $TENANT_ID (see /tmp/native-cleanup.log)" >&2
      sleep 30
    done
  fi
  rm -f "$CLI_BIN" "$TMP_FILE"
  rm -rf "$CLI_HOME"
  exit "$FAIL"
}
trap cleanup EXIT

echo "hello native smoke test $TS" > "$TMP_FILE"

# ── [1] provision tenant ────────────────────────────────────────────────────

echo "[1] provision tenant"
set +e
create_out="$(drive9_ctx create \
  --server "$BASE" \
  --tidbcloud-public-key "$PUBLIC_KEY" \
  --tidbcloud-private-key "$PRIVATE_KEY" \
  --json 2>&1)"
create_code=$?
set -e
check_eq "drive9 create exit code" "$create_code" "0"

API_KEY="$(printf '%s' "$create_out" | jq -r '.api_key // empty')"
TENANT_ID="$(printf '%s' "$create_out" | jq -r '.tenant_id // empty')"
CREATE_STATUS="$(printf '%s' "$create_out" | jq -r '.status // empty')"

check_cmd "response contains tenant_id" test -n "$TENANT_ID"
check_cmd "response contains api_key" test -n "$API_KEY"
check_eq "provision status is provisioning" "$CREATE_STATUS" "provisioning"
CREATED=1

# ── [2] wait tenant active ──────────────────────────────────────────────────

echo "[2] wait tenant active (timeout=${POLL_TIMEOUT_S}s)"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
LAST_STATUS=""
while :; do
  sfile="$(mktemp)"
  scode=$(curl -sS -o "$sfile" -w "%{http_code}" -H "Authorization: Bearer $API_KEY" "$BASE/v1/status")
  LAST_STATUS=$(jq -r '.status // empty' "$sfile")
  rm -f "$sfile"
  if [ "$scode" = "200" ] && [ "$LAST_STATUS" = "active" ]; then break; fi
  if [ "$(date +%s)" -ge "$deadline" ]; then break; fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant becomes active" "$LAST_STATUS" "active"

# ── [3] basic fs operations ─────────────────────────────────────────────────

echo "[3] small file ops"

DIR="/$TEST_DIR"

drive9_retry fs mkdir "$DIR" >/dev/null
drive9_retry fs cp "$TMP_FILE" ":$DIR/hello.txt" >/dev/null

ls_out="$(drive9_retry fs ls "$DIR")"
check_cmd "ls $DIR lists file" bash -c "echo \"\$1\" | grep -q hello.txt" _ "$ls_out"

cat_out="$(drive9_retry fs cat "$DIR/hello.txt")"
check_cmd "cat $DIR/hello.txt returns content" bash -c "echo \"\$1\" | grep -q 'hello native smoke test'" _ "$cat_out"

mv_dst="$DIR/world.txt"
drive9_retry fs mv "$DIR/hello.txt" "$mv_dst" >/dev/null

mv_ls="$(drive9_retry fs ls "$DIR")"
check_cmd "ls after mv shows world.txt" bash -c "echo \"\$1\" | grep -q world.txt" _ "$mv_ls"

drive9_retry fs rm "$mv_dst" >/dev/null
drive9_retry fs rm "$DIR" >/dev/null

echo "[4] batch small file ops"

BATCH_DIR="/native-batch-${TS}"
BATCH_COUNT=5
drive9_retry fs mkdir "$BATCH_DIR" >/dev/null
for i in $(seq 1 "$BATCH_COUNT"); do
  lp="$(mktemp)"
  printf "native-batch-%s-%s" "$TS" "$i" > "$lp"
  drive9_retry fs cp "$lp" ":$BATCH_DIR/file-${i}.txt" >/dev/null
  rm -f "$lp"
done

batch_ls="$(drive9_retry fs ls "$BATCH_DIR")"
batch_count=$(printf '%s' "$batch_ls" | grep -c 'file-' || true)
check_eq "batch dir has $BATCH_COUNT files" "$batch_count" "$BATCH_COUNT"

for i in 1 "$BATCH_COUNT"; do
  got="$(drive9_retry fs cat "$BATCH_DIR/file-${i}.txt")"
  want="native-batch-$TS-$i"
  check_eq "$BATCH_DIR/file-${i}.txt content" "$got" "$want"
done

drive9_retry fs rm -r "$BATCH_DIR" >/dev/null
check_cmd "rm -r batch dir" true

echo "[5] large file upload/download"

LARGE_MB="${CLI_LARGE_FILE_MB:-50}"
LARGE_LOCAL="$(mktemp)"
LARGE_REMOTE="$BATCH_DIR/large-${TS}.bin"
LARGE_DOWNLOADED="$(mktemp)"

dd if=/dev/zero of="$LARGE_LOCAL" bs=1M count="$LARGE_MB" status=none
LARGE_BYTES=$((LARGE_MB * 1048576))

drive9_retry fs mkdir "$BATCH_DIR" >/dev/null
drive9_retry fs cp "$LARGE_LOCAL" ":$LARGE_REMOTE" >/dev/null

stat_out="$(drive9_retry fs stat "$LARGE_REMOTE")"
remote_size=$(printf '%s' "$stat_out" | python3 -c "import sys; [print(l.split(':',1)[1].strip()) for l in sys.stdin.read().splitlines() if l.strip().startswith('size:')]" 2>/dev/null || echo "0")
check_eq "large remote size matches" "$remote_size" "$LARGE_BYTES"

drive9_retry fs cp ":$LARGE_REMOTE" "$LARGE_DOWNLOADED" >/dev/null

sum_src=$(sha256sum "$LARGE_LOCAL" | cut -d' ' -f1)
sum_dst=$(sha256sum "$LARGE_DOWNLOADED" | cut -d' ' -f1)
check_eq "downloaded sha256 matches" "$sum_dst" "$sum_src"

drive9_retry fs rm "$LARGE_REMOTE" >/dev/null
drive9_retry fs rm "$BATCH_DIR" >/dev/null
rm -f "$LARGE_LOCAL" "$LARGE_DOWNLOADED"

# ── [5.5] fork smoke ──────────────────────────────────────────────────────

echo "[5.5] fork smoke"
FORK_CTX_NAME="native-fork"
FORK_REMOTE="/native-fork-test.txt"
FORK_LOCAL="$(mktemp)"

drive9_ctx ctx add --name owner --server "$BASE" --api-key "$API_KEY" --mode TiDBCloud >/dev/null

fork_json="$(drive9_ctx ctx fork "$FORK_CTX_NAME" --from owner --tidbcloud-public-key "$PUBLIC_KEY" --tidbcloud-private-key "$PRIVATE_KEY" --json)"
fork_api_key="$(jq -r '.api_key // empty' <<<"$fork_json")"
fork_tenant_id="$(jq -r '.tenant_id // empty' <<<"$fork_json")"
fork_status="$(jq -r '.status // empty' <<<"$fork_json")"
check_cmd "fork returns api_key" test -n "$fork_api_key"
check_cmd "fork returns tenant_id" test -n "$fork_tenant_id"
check_eq "fork initial status is provisioning" "$fork_status" "provisioning"

fork_deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
fork_state=""
while :; do
  fork_status_file="$(mktemp)"
  fork_status_code=$(curl -sS -o "$fork_status_file" -w "%{http_code}" -H "Authorization: Bearer $fork_api_key" "$BASE/v1/status")
  fork_state=$(jq -r '.status // empty' "$fork_status_file")
  rm -f "$fork_status_file"
  echo "fork-status=${fork_status_code}:${fork_state}"
  if [ "$fork_status_code" = "200" ] && [ "$fork_state" = "active" ]; then break; fi
  if [ "$(date +%s)" -ge "$fork_deadline" ]; then break; fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "fork tenant becomes active" "$fork_state" "active"

printf "native-fork-%s" "$TS" > "$FORK_LOCAL"
drive9_ctx ctx use "$FORK_CTX_NAME" >/dev/null
drive9_ctx_retry fs cp "$FORK_LOCAL" ":$FORK_REMOTE" >/dev/null
fork_cat="$(drive9_ctx_retry fs cat "$FORK_REMOTE")"
check_eq "fork context can read written file" "$fork_cat" "native-fork-${TS}"

fork_delete_body="$(mktemp)"
fork_delete_code=$(curl -sS -o "$fork_delete_body" -w "%{http_code}" -X DELETE \
  -H "Authorization: Bearer $fork_api_key" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg pk "$PUBLIC_KEY" --arg sk "$PRIVATE_KEY" '{public_key: $pk, private_key: $sk}')" \
  "$BASE/v1/fork")
check_eq "DELETE /v1/fork returns 202" "$fork_delete_code" "202"
rm -f "$fork_delete_body"

echo "[6] delete tenant"
set +e
delete_out="$(drive9_ctx delete \
  --server "$BASE" \
  --api-key "$API_KEY" \
  --tidbcloud-public-key "$PUBLIC_KEY" \
  --tidbcloud-private-key "$PRIVATE_KEY" \
  -y \
  --json 2>&1)"
delete_code=$?
set -e
check_eq "drive9 delete exit code" "$delete_code" "0"

DELETE_STATUS="$(printf '%s' "$delete_out" | jq -r '.status // empty')"
check_cmd "delete status is deleted or deleting" bash -c "echo \"\$1\" | grep -qE 'deleted|deleting'" _ "$DELETE_STATUS"
CREATED=0

# ── [5] verify tenant gone ──────────────────────────────────────────────────

echo "[7] verify tenant removed"
vfile="$(mktemp)"
vcode=$(curl -sS -o "$vfile" -w "%{http_code}" -H "Authorization: Bearer $API_KEY" "$BASE/v1/status")
rm -f "$vfile"
if [ "$vcode" = "401" ] || [ "$vcode" = "403" ] || [ "$vcode" = "404" ]; then
  check_eq "GET /v1/status returns auth error or not-found after delete" "ok" "ok"
else
  check_eq "GET /v1/status returns auth error or not-found after delete (got $vcode)" "$vcode" "401/403/404"
fi

# ── summary ─────────────────────────────────────────────────────────────────

echo ""
echo "=== native smoke test: $PASS passed, $FAIL failed, $SKIP skipped (total $TOTAL) ==="
