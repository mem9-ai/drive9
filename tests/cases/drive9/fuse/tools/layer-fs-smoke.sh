#!/usr/bin/env bash
# drive9 LayerFS v36 smoke test against a live deployment (or embedded 501 stub).

set -euo pipefail

BASE="${SERVER:?missing simulator server}"
API_KEY="${API_KEY:?missing simulator tenant key}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"
CLI_HOME="${DRIVE9_E2E_CLI_HOME:-$(mktemp -d)}"
RUN_LAYER_FUSE_SMOKE="${RUN_LAYER_FUSE_SMOKE:-0}"
LAYER_FUSE_STRICT_PREREQS="${LAYER_FUSE_STRICT_PREREQS:-$RUN_LAYER_FUSE_SMOKE}"
FUSE_MOUNT_ROOT="${FUSE_MOUNT_ROOT:-/tmp}"
FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-15s}"
LAYER_CLEAN_UMOUNT_MAX_S="${LAYER_CLEAN_UMOUNT_MAX_S:-15}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-25}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
LAYER_CLI_LARGE_FILE_MB="${LAYER_CLI_LARGE_FILE_MB:-10}"
EXPECT_EMBEDDED_501="${DRIVE9_EXPECT_EMBEDDED_LAYER_501:-0}"
LAYER_FUSE_LOG_AUDIT_PATTERN="${LAYER_FUSE_LOG_AUDIT_PATTERN:-panic|fatal error|Resource temporarily unavailable|force-quit|event=exit code=[1-9]|shutdown incomplete}"

PASS=0
FAIL=0
TOTAL=0
WORK_TMP="$(mktemp -d)"
MOUNT_POINT=""
MOUNT_PID=""
MOUNT_LOG=""
CREATED_LAYERS=()
TEST_ROOT="/e2e-layer-v36-$(date +%s)-$$"

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$got" = "$want" ]; then
    echo "PASS $desc (got=$got)"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc (want=$want got=$got)"
    FAIL=$((FAIL + 1))
  fi
}

check_cmd() {
  local desc="$1"
  shift
  TOTAL=$((TOTAL + 1))
  if "$@"; then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc"
    FAIL=$((FAIL + 1))
  fi
}

check_cmd_fail() {
  local desc="$1"
  shift
  TOTAL=$((TOTAL + 1))
  if "$@"; then
    echo "FAIL $desc (expected failure)"
    FAIL=$((FAIL + 1))
  else
    echo "PASS $desc"
    PASS=$((PASS + 1))
  fi
}

sha256_file() {
  local file_path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file_path" | cut -d' ' -f1
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file_path" | cut -d' ' -f1
    return
  fi
  python3 - "$file_path" <<'PY'
import hashlib, pathlib, sys
h = hashlib.sha256()
with pathlib.Path(sys.argv[1]).open("rb") as f:
    for chunk in iter(lambda: f.read(1024 * 1024), b""):
        h.update(chunk)
print(h.hexdigest())
PY
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

url_escape() {
  python3 - "$1" <<'PY'
import sys
from urllib.parse import quote
print(quote(sys.argv[1], safe=""))
PY
}

curl_body_code() {
  local method="$1" url="$2" auth="${3:-}" data="${4:-}" content_type="${5-application/json}"
  local body_file code curl_rc attempt=1
  local -a args
  body_file="$(mktemp)"
  while :; do
    : >"$body_file"
    args=(-sS -o "$body_file" -w "%{http_code}" -X "$method")
    if [ -n "$auth" ]; then
      args+=(-H "Authorization: Bearer $auth")
    fi
    if [ -n "$data" ]; then
      if [ -n "$content_type" ]; then
        args+=(-H "Content-Type: $content_type")
      fi
      args+=(--data-binary "$data")
    fi
    if code=$(curl "${args[@]}" "$url"); then
      curl_rc=0
    else
      curl_rc=$?
      code="000"
    fi
    if [ "$attempt" -ge "$REQUEST_MAX_RETRIES" ]; then
      break
    fi
    case "$code" in
      000|429|502|503|504)
        sleep "$REQUEST_RETRY_SLEEP_S"
        attempt=$((attempt + 1))
        continue
        ;;
    esac
    if [ "$curl_rc" -eq 0 ]; then
      break
    fi
    sleep "$REQUEST_RETRY_SLEEP_S"
    attempt=$((attempt + 1))
  done
  cat "$body_file"
  echo
  echo "__HTTP__${code}"
  rm -f "$body_file"
}
is_mounted_path() {
  local mount_point="$1"
  local physical_mount_point
  physical_mount_point="$(cd "$(dirname "$mount_point")" 2>/dev/null && pwd -P)/$(basename "$mount_point")"
  if command -v mountpoint >/dev/null 2>&1; then
    mountpoint -q "$mount_point"
    return
  fi
  mount | awk -v mp="$mount_point" -v pmp="$physical_mount_point" '{for(i=1;i<=NF;i++) if($i=="on" && ($(i+1)==mp || $(i+1)==pmp)) found=1} END{exit !found}'
}

force_unmount() {
  local mount_point="$1"
  case "$(uname -s)" in
    Darwin)
      umount "$mount_point" >/dev/null 2>&1 || diskutil unmount force "$mount_point" >/dev/null 2>&1 || true
      ;;
    Linux)
      fusermount3 -uz "$mount_point" >/dev/null 2>&1 || fusermount -uz "$mount_point" >/dev/null 2>&1 || umount -l "$mount_point" >/dev/null 2>&1 || true
      ;;
  esac
}

wait_mount_state() {
  local expect="$1"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  while :; do
    if [ "$expect" = "mounted" ] && is_mounted_path "$MOUNT_POINT"; then
      return 0
    fi
    if [ "$expect" = "unmounted" ] && ! is_mounted_path "$MOUNT_POINT"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$MOUNT_READY_INTERVAL_S"
  done
}

wait_mount_log_ready() {
  local log_file="$1"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  while :; do
    if [ -f "$log_file" ] && grep -q "drive9: mounted on " "$log_file"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$MOUNT_READY_INTERVAL_S"
  done
}

stop_mount() {
  set +e
  if [ -n "${MOUNT_POINT:-}" ] && is_mounted_path "$MOUNT_POINT"; then
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" --no-auto-pack "$MOUNT_POINT" >/dev/null 2>&1 || true
    wait_mount_state unmounted >/dev/null 2>&1 || true
    if is_mounted_path "$MOUNT_POINT"; then
      force_unmount "$MOUNT_POINT"
      wait_mount_state unmounted >/dev/null 2>&1 || true
    fi
  fi
  if [ -n "${MOUNT_PID:-}" ] && kill -0 "$MOUNT_PID" >/dev/null 2>&1; then
    kill "$MOUNT_PID" >/dev/null 2>&1 || true
    wait "$MOUNT_PID" >/dev/null 2>&1 || true
  fi
  MOUNT_PID=""
  MOUNT_POINT=""
  set -e
}

cleanup() {
  set +e
  stop_mount
  if [ -n "$API_KEY" ]; then
    for (( idx=${#CREATED_LAYERS[@]}-1; idx>=0; idx-- )); do
      local lid="${CREATED_LAYERS[$idx]}"
      if [ -n "$lid" ]; then
        curl_body_code DELETE "$BASE/v1/layers/$(url_escape "$lid")?force=1" "$API_KEY" >/dev/null 2>&1 || true
      fi
    done
    curl_body_code DELETE "$BASE/v1/fs${TEST_ROOT}?recursive=1" "$API_KEY" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK_TMP" >/dev/null 2>&1 || true
  if [ -z "${DRIVE9_E2E_CLI_HOME:-}" ] && [ -n "$CLI_HOME" ]; then
    rm -rf "$CLI_HOME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

drive9() {
  env -u DRIVE9_VAULT_TOKEN HOME="$CLI_HOME" DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "${CLI:?missing simulator CLI}" "$@"
}

echo "=== drive9 LayerFS v36 smoke test ==="

if [ "$EXPECT_EMBEDDED_501" = "1" ]; then
  echo "--- embedded server 501 LAYER_REQUIRES_TIDB_SERVER contract ---"
  r_list=$(curl_body_code GET "$BASE/v1/layers" "$API_KEY")
  check_eq "embedded GET /v1/layers returns 501" "$(http_code "$r_list")" "501"
  check_eq "embedded GET /v1/layers error code" "$(json_body "$r_list" | jq -r '.code // empty')" "LAYER_REQUIRES_TIDB_SERVER"

  r_create=$(curl_body_code POST "$BASE/v1/layers" "$API_KEY" '{"base_root_path":"/"}')
  check_eq "embedded POST /v1/layers returns 501" "$(http_code "$r_create")" "501"
  check_eq "embedded POST /v1/layers error code" "$(json_body "$r_create" | jq -r '.code // empty')" "LAYER_REQUIRES_TIDB_SERVER"

  r_view=$(curl_body_code GET "$BASE/v1/views/00000000-0000-0000-0000-000000000001/v1/fs/probe.txt" "$API_KEY")
  check_eq "embedded GET /v1/views/* returns 501" "$(http_code "$r_view")" "501"
  check_eq "embedded GET /v1/views/* error code" "$(json_body "$r_view" | jq -r '.code // empty')" "LAYER_REQUIRES_TIDB_SERVER"

  set +e
  cli_err=$(drive9 fs layer ls 2>&1)
  cli_rc=$?
  set -e
  check_cmd "embedded drive9 fs layer ls exits non-zero" test "$cli_rc" -ne 0
  check_cmd "embedded drive9 fs layer ls mentions TiDB server runtime" grep -q "TiDB server runtime" <<<"$cli_err"

  echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
  [ "$FAIL" -eq 0 ]
  exit 0
fi
echo "--- 1. main seed & CoW snapshot isolation ---"
drive9 fs mkdir ":$TEST_ROOT"
drive9 fs mkdir ":$TEST_ROOT/dir"
printf 'main-v1' >"$WORK_TMP/small.txt"
printf 'keep-in-layer' >"$WORK_TMP/gone.txt"
printf 'sub-v1' >"$WORK_TMP/sub.txt"
drive9 fs cp "$WORK_TMP/small.txt" ":$TEST_ROOT/small.txt"
drive9 fs cp "$WORK_TMP/gone.txt" ":$TEST_ROOT/gone.txt"
drive9 fs cp "$WORK_TMP/sub.txt" ":$TEST_ROOT/dir/sub.txt"
drive9 fs symlink "small.txt" ":$TEST_ROOT/link.txt"

ts_tag="v36_${RANDOM}_$$"
l1_name="e2e-l1-${ts_tag}"
l1_json=$(drive9 fs layer create --name "$l1_name" --tag "run=$ts_tag" --tag "role=primary" --json ":$TEST_ROOT")
l1_id=$(printf '%s' "$l1_json" | jq -r '.layer_id')
CREATED_LAYERS+=("$l1_id")
check_eq "L1 state is active" "$(printf '%s' "$l1_json" | jq -r '.state')" "active"
check_eq "L1 base_root_path" "$(printf '%s' "$l1_json" | jq -r '.base_root_path')" "$TEST_ROOT"
check_eq "L1 initial fence is 0" "$(printf '%s' "$l1_json" | jq -r '.fence')" "0"

l2_name="e2e-l2-${ts_tag}"
r_l2=$(curl_body_code POST "$BASE/v1/layers" "$API_KEY" "$(jq -nc --arg n "$l2_name" --arg b "$TEST_ROOT" '{name:$n,base_root_path:$b}')")
check_eq "POST /v1/layers L2 returns 200" "$(http_code "$r_l2")" "200"
l2_id=$(json_body "$r_l2" | jq -r '.layer_id')
CREATED_LAYERS+=("$l2_id")

printf 'main-v2' >"$WORK_TMP/small-v2.txt"
drive9 fs cp "$WORK_TMP/small-v2.txt" ":$TEST_ROOT/small.txt"
drive9 fs rm ":$TEST_ROOT/gone.txt"

check_eq "main sees updated small.txt" "$(drive9 fs cat ":$TEST_ROOT/small.txt")" "main-v2"
check_eq "L1 sees snapshot small.txt via name" "$(drive9 fs --layer "$l1_name" cat ":$TEST_ROOT/small.txt")" "main-v1"
check_eq "L1 sees snapshot gone.txt via tag:k=v" "$(drive9 fs --layer "tag:run=$ts_tag" cat ":$TEST_ROOT/gone.txt")" "keep-in-layer"
check_eq "L2 sees snapshot gone.txt via HTTP view" "$(json_body "$(curl_body_code GET "$BASE/v1/views/$l2_id/v1/fs${TEST_ROOT}/gone.txt" "$API_KEY")")" "keep-in-layer"

echo "--- 2. layer discovery & base_root_path 403 boundary ---"
check_eq "fs layer ls contains L1" "$(drive9 fs layer ls --json | jq -r --arg id "$l1_id" '[.layers[] | select(.layer_id==$id)] | length')" "1"
check_eq "fs layer inspect by tag:key resolves L1" "$(drive9 fs layer inspect --json "tag:role" | jq -r '.layer_id')" "$l1_id"

r_outside=$(curl_body_code GET "$BASE/v1/views/$l1_id/v1/fs/outside-${ts_tag}.txt" "$API_KEY")
check_eq "HTTP view outside base_root returns 403" "$(http_code "$r_outside")" "403"
check_eq "HTTP view outside base_root code" "$(json_body "$r_outside" | jq -r '.code // empty')" "FORBIDDEN_OUTSIDE_BASE_ROOT"
check_cmd_fail "CLI --layer outside base_root fails" drive9 fs --layer "$l1_name" cp "$WORK_TMP/small.txt" ":/outside-${ts_tag}.txt"

echo "--- 3. overlay writes, whiteouts, symlinks, hardlinks & S3 multipart ---"
printf 'layer-private' >"$WORK_TMP/private.txt"
drive9 fs --layer "$l1_name" mkdir ":$TEST_ROOT/newdir"
drive9 fs --layer "$l1_name" cp "$WORK_TMP/private.txt" ":$TEST_ROOT/newdir/orig.txt"
drive9 fs --layer "$l1_name" mv ":$TEST_ROOT/newdir/orig.txt" ":$TEST_ROOT/newdir/moved.txt"
drive9 fs --layer "$l1_name" chmod 0755 ":$TEST_ROOT/newdir/moved.txt"
drive9 fs --layer "$l1_name" hardlink ":$TEST_ROOT/newdir/moved.txt" ":$TEST_ROOT/newdir/hlink.txt"
drive9 fs --layer "$l1_name" symlink "moved.txt" ":$TEST_ROOT/newdir/slink.txt"
drive9 fs --layer "$l1_name" rm ":$TEST_ROOT/dir/sub.txt"

check_cmd_fail "L1 whiteout hides deleted dir/sub.txt" drive9 fs --layer "$l1_name" stat ":$TEST_ROOT/dir/sub.txt"
check_eq "L2 still sees dir/sub.txt" "$(drive9 fs --layer "$l2_id" cat ":$TEST_ROOT/dir/sub.txt")" "sub-v1"
check_cmd_fail "main does not see L1 private file" drive9 fs stat ":$TEST_ROOT/newdir/moved.txt"
check_eq "L1 hardlink content matches" "$(drive9 fs --layer "$l1_name" cat ":$TEST_ROOT/newdir/hlink.txt")" "layer-private"

dd if=/dev/urandom of="$WORK_TMP/large.bin" bs=1048576 count="$LAYER_CLI_LARGE_FILE_MB" status=none
want_large_sha=$(sha256_file "$WORK_TMP/large.bin")
drive9 fs --layer "$l1_name" cp "$WORK_TMP/large.bin" ":$TEST_ROOT/large.bin"
drive9 fs --layer "$l1_name" cp ":$TEST_ROOT/large.bin" "$WORK_TMP/large-got.bin"
check_eq "L1 large S3 multipart SHA-256 parity" "$(sha256_file "$WORK_TMP/large-got.bin")" "$want_large_sha"

echo "--- 4. recursive CoW fork, 409 merge conflict & clean 3-way merge ---"
child_name="e2e-child-${ts_tag}"
child_json=$(drive9 fs layer fork --name "$child_name" --json "$l1_name")
child_id=$(printf '%s' "$child_json" | jq -r '.layer_id')
CREATED_LAYERS+=("$child_id")
check_eq "child parent_layer_id matches L1" "$(printf '%s' "$child_json" | jq -r '.parent_layer_id')" "$l1_id"

printf 'l1-after-fork' >"$WORK_TMP/after-fork.txt"
drive9 fs --layer "$l1_name" cp "$WORK_TMP/after-fork.txt" ":$TEST_ROOT/newdir/moved.txt"
check_eq "forked child preserves pre-mutation moved.txt" "$(drive9 fs --layer "$child_name" cat ":$TEST_ROOT/newdir/moved.txt")" "layer-private"

drive9 fs layer rm "$l1_name" >/dev/null
check_eq "deleted parent L1 returns 404" "$(http_code "$(curl_body_code GET "$BASE/v1/layers/$l1_id" "$API_KEY")")" "404"
check_eq "child still reads inherited large.bin after parent rm" "$(drive9 fs --layer "$child_name" stat -o json ":$TEST_ROOT/large.bin" | jq -r '.size')" "$((LAYER_CLI_LARGE_FILE_MB * 1048576))"

printf 'main-conflict' >"$WORK_TMP/c-main.txt"
printf 'layer-conflict' >"$WORK_TMP/c-layer.txt"
drive9 fs cp "$WORK_TMP/c-main.txt" ":$TEST_ROOT/conflict.txt"
drive9 fs --layer "$l2_id" cp "$WORK_TMP/c-layer.txt" ":$TEST_ROOT/conflict.txt"
r_mconf=$(curl_body_code POST "$BASE/v1/layers/$l2_id/merge" "$API_KEY" "{}")
check_eq "conflicting merge returns 409" "$(http_code "$r_mconf")" "409"
check_eq "conflicting merge code" "$(json_body "$r_mconf" | jq -r '.code // empty')" "MERGE_CONFLICT"

merge_json=$(drive9 fs layer merge --json "$child_name")
check_eq "merge status is merged" "$(printf '%s' "$merge_json" | jq -r '.status')" "merged"
check_eq "main preserves concurrent small.txt edit" "$(drive9 fs cat ":$TEST_ROOT/small.txt")" "main-v2"
check_cmd_fail "main applies merged whiteout on dir/sub.txt" drive9 fs stat ":$TEST_ROOT/dir/sub.txt"
check_eq "main reflects merged moved.txt" "$(drive9 fs cat ":$TEST_ROOT/newdir/moved.txt")" "layer-private"
check_eq "main reflects merged hardlink" "$(drive9 fs cat ":$TEST_ROOT/newdir/hlink.txt")" "layer-private"
drive9 fs cp ":$TEST_ROOT/large.bin" "$WORK_TMP/large-main.bin"
check_eq "main reflects merged large.bin SHA-256" "$(sha256_file "$WORK_TMP/large-main.bin")" "$want_large_sha"
echo "--- 5. odd/even fence mount lease protocol ---"
lease_name="e2e-lease-${ts_tag}"
lease_json=$(drive9 fs layer create --name "$lease_name" --json ":$TEST_ROOT")
lease_id=$(printf '%s' "$lease_json" | jq -r '.layer_id')
CREATED_LAYERS+=("$lease_id")

r_bind=$(curl_body_code POST "$BASE/v1/layers/$lease_id/bind-mount" "$API_KEY" '{"mount_id":"e2e-mnt-1","operation_id":"e2e-op-1","expected_fence":0}')
check_eq "bind-mount returns 200" "$(http_code "$r_bind")" "200"
check_eq "bind-mount advances fence to 1 (odd)" "$(json_body "$r_bind" | jq -r '.fence')" "1"

r_bind_replay=$(curl_body_code POST "$BASE/v1/layers/$lease_id/bind-mount" "$API_KEY" '{"mount_id":"e2e-mnt-1","operation_id":"e2e-op-1","expected_fence":0}')
check_eq "bind-mount replay returns 200" "$(http_code "$r_bind_replay")" "200"
check_eq "bind-mount replay keeps fence 1" "$(json_body "$r_bind_replay" | jq -r '.fence')" "1"

r_bind_conf=$(curl_body_code POST "$BASE/v1/layers/$lease_id/bind-mount" "$API_KEY" '{"mount_id":"e2e-mnt-2","operation_id":"e2e-op-2","expected_fence":0}')
check_eq "competing bind-mount returns 409" "$(http_code "$r_bind_conf")" "409"
check_eq "competing bind-mount code" "$(json_body "$r_bind_conf" | jq -r '.code // empty')" "LAYER_MOUNT_CONFLICT"

r_del_busy=$(curl_body_code DELETE "$BASE/v1/layers/$lease_id" "$API_KEY")
check_eq "delete mounted layer without force returns 409" "$(http_code "$r_del_busy")" "409"
check_eq "delete mounted layer code" "$(json_body "$r_del_busy" | jq -r '.code // empty')" "LAYER_MOUNTED_BUSY"

r_merge_busy=$(curl_body_code POST "$BASE/v1/layers/$lease_id/merge" "$API_KEY" "{}")
check_eq "merge mounted layer returns 409" "$(http_code "$r_merge_busy")" "409"
check_eq "merge mounted layer code" "$(json_body "$r_merge_busy" | jq -r '.code // empty')" "LAYER_MOUNTED_BUSY"

r_renew=$(curl_body_code POST "$BASE/v1/layers/$lease_id/renew-mount" "$API_KEY" '{"mount_id":"e2e-mnt-1","fence":1}')
check_eq "renew-mount returns 200" "$(http_code "$r_renew")" "200"

r_unbind=$(curl_body_code POST "$BASE/v1/layers/$lease_id/unbind-mount" "$API_KEY" '{"mount_id":"e2e-mnt-1","kind":"fence","fence":1}')
check_eq "unbind-mount returns 200" "$(http_code "$r_unbind")" "200"
check_eq "unbind-mount advances fence to 2 (even)" "$(json_body "$r_unbind" | jq -r '.fence')" "2"

r_unbind_replay=$(curl_body_code POST "$BASE/v1/layers/$lease_id/unbind-mount" "$API_KEY" '{"mount_id":"e2e-mnt-1","kind":"fence","fence":1}')
check_eq "unbind-mount replay returns 200" "$(http_code "$r_unbind_replay")" "200"
check_eq "unbind-mount replay keeps fence 2" "$(json_body "$r_unbind_replay" | jq -r '.fence')" "2"

drive9 fs layer rm "$lease_name" >/dev/null

if [ "$RUN_LAYER_FUSE_SMOKE" = "1" ]; then
  echo "--- 6. live FUSE --layer mount, isolation, drain/umount & merge ---"
  if [ "$(uname -s)" = "Linux" ] && [ ! -e /dev/fuse ]; then
    if [ "$LAYER_FUSE_STRICT_PREREQS" = "1" ]; then
      echo "FAIL /dev/fuse missing under LAYER_FUSE_STRICT_PREREQS=1" >&2
      exit 1
    fi
    echo "SKIP layer FUSE smoke: /dev/fuse missing"
  else
    fuse_name="e2e-fuse-${ts_tag}"
    fuse_json=$(drive9 fs layer create --name "$fuse_name" --json ":$TEST_ROOT")
    fuse_id=$(printf '%s' "$fuse_json" | jq -r '.layer_id')
    CREATED_LAYERS+=("$fuse_id")

    MOUNT_POINT="$(mktemp -d "${FUSE_MOUNT_ROOT}/d9-layer-mp.XXXXXX")"
    local_root="$(mktemp -d "${FUSE_MOUNT_ROOT}/d9-layer-lr.XXXXXX")"
    MOUNT_LOG="$WORK_TMP/fuse-mount.log"
    profile="${FUSE_PROFILE:-coding-agent}"
    mount_args=(mount --foreground --mode=fuse --profile="$profile" --durability=write-sync --flush-debounce=0 --no-persist-credentials --layer "$fuse_name")
    case "$profile" in
      none|extent|interactive) ;;
      *) mount_args+=(--local-root "$local_root") ;;
    esac
    mount_args+=(":$TEST_ROOT" "$MOUNT_POINT")
    drive9 "${mount_args[@]}" >"$MOUNT_LOG" 2>&1 &
    MOUNT_PID="$!"

    if ! wait_mount_state mounted || ! wait_mount_log_ready "$MOUNT_LOG"; then
      echo "FAIL FUSE layer mount did not become ready" >&2
      cat "$MOUNT_LOG" >&2 || true
      exit 1
    fi
    check_eq "FUSE layer mount sees inherited small.txt" "$(cat "$MOUNT_POINT/small.txt")" "main-v2"
    check_eq "FUSE layer mount acquires odd fence 1" "$(drive9 fs layer inspect --json "$fuse_id" | jq -r '.fence')" "1"

    mkdir -p "$MOUNT_POINT/fusedir"
    printf 'fuse-layer-write' >"$MOUNT_POINT/fusedir/a.txt"
    mv "$MOUNT_POINT/fusedir/a.txt" "$MOUNT_POINT/fusedir/b.txt"
    ln -s "b.txt" "$MOUNT_POINT/fusedir/s.txt"
    dd if=/dev/urandom of="$WORK_TMP/fuse-blob.bin" bs=1048576 count=6 status=none
    want_fuse_sha=$(sha256_file "$WORK_TMP/fuse-blob.bin")
    cp "$WORK_TMP/fuse-blob.bin" "$MOUNT_POINT/fusedir/blob.bin"
    check_cmd_fail "main isolated while FUSE layer mounted" drive9 fs stat ":$TEST_ROOT/fusedir/b.txt"

    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" --no-auto-pack "$MOUNT_POINT"
    wait_mount_state unmounted
    wait "$MOUNT_PID"
    MOUNT_PID=""
    check_eq "FUSE umount releases lease to even fence 2" "$(drive9 fs layer inspect --json "$fuse_id" | jq -r '.fence')" "2"
    if grep -E "$LAYER_FUSE_LOG_AUDIT_PATTERN" "$MOUNT_LOG" >/dev/null 2>&1; then
      echo "FAIL FUSE mount log matched audit pattern" >&2
      grep -E "$LAYER_FUSE_LOG_AUDIT_PATTERN" "$MOUNT_LOG" >&2 || true
      FAIL=$((FAIL + 1))
      TOTAL=$((TOTAL + 1))
    else
      echo "PASS FUSE mount log clean"
      PASS=$((PASS + 1))
      TOTAL=$((TOTAL + 1))
    fi

    rm -rf "$MOUNT_POINT" "$local_root"
    MOUNT_POINT=""

    fmerge_json=$(drive9 fs layer merge --json "$fuse_id")
    check_eq "FUSE layer merge status is merged" "$(printf '%s' "$fmerge_json" | jq -r '.status')" "merged"
    check_eq "main sees merged FUSE b.txt" "$(drive9 fs cat ":$TEST_ROOT/fusedir/b.txt")" "fuse-layer-write"
    drive9 fs cp ":$TEST_ROOT/fusedir/blob.bin" "$WORK_TMP/fuse-blob-main.bin"
    check_eq "main sees merged FUSE 6MiB SHA-256" "$(sha256_file "$WORK_TMP/fuse-blob-main.bin")" "$want_fuse_sha"

    if [ "$(uname -s)" = "Linux" ]; then
      echo "--- 7. live Linux FUSE in-place drive9 mount switch (main -> layer -> main) ---"
      sw_name="e2e-switch-${ts_tag}"
      sw_json=$(drive9 fs layer create --name "$sw_name" --json ":$TEST_ROOT")
      sw_id=$(printf '%s' "$sw_json" | jq -r '.layer_id')
      CREATED_LAYERS+=("$sw_id")

      MOUNT_POINT="$(mktemp -d "${FUSE_MOUNT_ROOT}/d9-switch-mp.XXXXXX")"
      sw_local_root="$(mktemp -d "${FUSE_MOUNT_ROOT}/d9-switch-lr.XXXXXX")"
      SW_LOG="$WORK_TMP/fuse-switch.log"
      sw_args=(mount --foreground --mode=fuse --profile="$profile" --durability=write-sync --flush-debounce=0 --no-persist-credentials)
      case "$profile" in
        none|extent|interactive) ;;
        *) sw_args+=(--local-root "$sw_local_root") ;;
      esac
      sw_args+=(":$TEST_ROOT" "$MOUNT_POINT")
      drive9 "${sw_args[@]}" >"$SW_LOG" 2>&1 &
      MOUNT_PID="$!"

      if ! wait_mount_state mounted || ! wait_mount_log_ready "$SW_LOG"; then
        echo "FAIL FUSE main mount for switch did not become ready" >&2
        cat "$SW_LOG" >&2 || true
        exit 1
      fi
      orig_pid="$MOUNT_PID"
      orig_mnt_id="$(awk -v mp="$MOUNT_POINT" '$5 == mp {print $1; exit}' /proc/self/mountinfo)"
      st1=$(drive9 mount status --json "$MOUNT_POINT")
      check_eq "initial mount status view generation is 1" "$(printf '%s' "$st1" | jq -r '.view.generation')" "1"
      check_eq "initial mount status view is main" "$(printf '%s' "$st1" | jq -r '.view.binding.layer_id // ""')" ""

      sw1=""
      for _ in 1 2 3 4 5; do
        if sw1=$(drive9 mount switch --layer "$sw_name" "$MOUNT_POINT" 2>/dev/null); then
          break
        fi
        sleep 0.3
      done
      sw1_mnt=$(printf '%s' "$sw1" | jq -r '.request.mount_id // empty')
      sw1_op=$(printf '%s' "$sw1" | jq -r '.request.operation_id // empty')
      check_eq "mount switch to layer advances generation to 2" "$(printf '%s' "$sw1" | jq -r '.response.receipt.generation // empty')" "2"
      check_eq "mount switch binds canonical layer_id" "$(printf '%s' "$sw1" | jq -r '.response.receipt.target.layer_id // empty')" "$sw_id"
      check_eq "mount switch acquires odd fence 1" "$(printf '%s' "$sw1" | jq -r '.response.receipt.target.fence // empty')" "1"
      check_eq "mount switch preserves kernel mount ID" "$(awk -v mp="$MOUNT_POINT" '$5 == mp {print $1; exit}' /proc/self/mountinfo)" "$orig_mnt_id"
      check_cmd "mount switch preserves worker PID" kill -0 "$orig_pid"

      sw1_replay=$(drive9 mount switch --mount-id "$sw1_mnt" --generation 1 --operation "$sw1_op" --layer "$sw_name" "$MOUNT_POINT")
      check_eq "mount switch replay returns generation 2" "$(printf '%s' "$sw1_replay" | jq -r '.response.receipt.generation')" "2"
      sw1_query=$(drive9 mount switch-operation --mount-id "$sw1_mnt" --generation 1 --operation "$sw1_op" --layer "$sw_name" "$MOUNT_POINT")
      check_eq "mount switch-operation returns generation 2" "$(printf '%s' "$sw1_query" | jq -r '.response.receipt.generation')" "2"

      printf 'switched-layer-payload' >"$MOUNT_POINT/switched.txt"
      printf 'switched-merged-payload' >"$MOUNT_POINT/switched-merged.txt"
      check_cmd_fail "main isolated before switch-back" drive9 fs stat ":$TEST_ROOT/switched.txt"

      exec 9<"$MOUNT_POINT/switched.txt"
      check_cmd_fail "mount switch refuses with EBUSY while fd open" drive9 mount switch --layer main "$MOUNT_POINT"
      exec 9<&-

      sw2=""
      for _ in 1 2 3 4 5; do
        if sw2=$(drive9 mount switch --layer main "$MOUNT_POINT" 2>/dev/null); then
          break
        fi
        sleep 0.3
      done
      check_eq "mount switch back to main advances generation to 3" "$(printf '%s' "$sw2" | jq -r '.response.receipt.generation // empty')" "3"
      check_cmd_fail "switched.txt absent on main before merge" test -e "$MOUNT_POINT/switched.txt"
      for _ in 1 2 3 4 5 6; do
        [ "$(drive9 fs layer inspect --json "$sw_id" | jq -r '.fence')" = "2" ] && break
        sleep 0.5
      done
      check_eq "switch-away drains outgoing lease to even fence 2" "$(drive9 fs layer inspect --json "$sw_id" | jq -r '.fence')" "2"

      sw_merge=$(drive9 fs layer merge --json "$sw_id")
      check_eq "switched layer merge status is merged" "$(printf '%s' "$sw_merge" | jq -r '.status')" "merged"
      check_eq "same FUSE mount on main reads merged file" "$(cat "$MOUNT_POINT/switched-merged.txt")" "switched-merged-payload"

      drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" --no-auto-pack "$MOUNT_POINT"
      wait_mount_state unmounted
      wait "$MOUNT_PID"
      MOUNT_PID=""
      rm -rf "$MOUNT_POINT" "$sw_local_root"
      MOUNT_POINT=""
    fi
  fi
fi

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
