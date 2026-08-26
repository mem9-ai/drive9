#!/usr/bin/env bash
# Hosted S3 --auth=server coverage: prefix isolation, fail-closed mint,
# read-only STS, drive9↔S3 copy, FUSE mount, and in-place STS refresh.
#
# Manual-only (same reason as object-auth-smoke-test.sh): needs a
# tidbcloud-native deployment plus a dedicated test bucket. Refresh uses
# DRIVE9_OBJECT_SESSION_REFRESH_{MIN,MAX}_LEAD so the mount remints in
# seconds instead of waiting ~45m of a 1h session.
#
#   export DRIVE9_BASE="http://k8s-drive9ti-drive9se-b6bbe5ba6e-cee81207452d1185.elb.ap-southeast-1.amazonaws.com"
#   export DRIVE9_TIDBCLOUD_PUBLIC_KEY="..."
#   export DRIVE9_TIDBCLOUD_PRIVATE_KEY="..."
#   export DRIVE9_E2E_OBJECT_BUCKET="your-bucket"
#   export DRIVE9_E2E_OBJECT_ACCESS_KEY_ID="..."
#   export DRIVE9_E2E_OBJECT_SECRET_ACCESS_KEY="..."
#   # optional: DRIVE9_E2E_OBJECT_REGION=ap-southeast-1
#   bash e2e/object-auth-s3-hosted-test.sh

set -euo pipefail

required_env=(
  DRIVE9_BASE
  DRIVE9_TIDBCLOUD_PUBLIC_KEY
  DRIVE9_TIDBCLOUD_PRIVATE_KEY
  DRIVE9_E2E_OBJECT_BUCKET
  DRIVE9_E2E_OBJECT_ACCESS_KEY_ID
  DRIVE9_E2E_OBJECT_SECRET_ACCESS_KEY
)
missing_env=()
for name in "${required_env[@]}"; do
  if [ -z "${!name:-}" ]; then
    missing_env+=("$name")
  fi
done
if [ "${#missing_env[@]}" -gt 0 ]; then
  printf 'SKIP: object-auth s3 hosted coverage requires: %s\n' "${missing_env[*]}"
  exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BASE="${DRIVE9_BASE%/}"
PUBLIC_KEY="$DRIVE9_TIDBCLOUD_PUBLIC_KEY"
PRIVATE_KEY="$DRIVE9_TIDBCLOUD_PRIVATE_KEY"
SCHEME="${DRIVE9_E2E_OBJECT_SCHEME:-s3}"
BUCKET="$DRIVE9_E2E_OBJECT_BUCKET"
REGION="${DRIVE9_E2E_OBJECT_REGION:-ap-southeast-1}"
ENDPOINT="${DRIVE9_E2E_OBJECT_ENDPOINT:-}"
STS_ENDPOINT="${DRIVE9_E2E_OBJECT_STS_ENDPOINT:-}"
PREFIX="${DRIVE9_E2E_OBJECT_PREFIX:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-600}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
CLI_SOURCE="${CLI_SOURCE:-build}"
SKIP_CLEANUP="${SKIP_CLEANUP:-0}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-30}"
WRITEBACK_TIMEOUT_S="${WRITEBACK_TIMEOUT_S:-30}"
REFRESH_WAIT_S="${REFRESH_WAIT_S:-45}"

PASS=0
FAIL=0
TOTAL=0
CLI_BIN=""
CLI_HOME=""
BACKEND_ID=""
MOUNT_POINT=""
MOUNT_PID=""
CACHE_DIR=""
WORKDIR=""

declare -a TENANT_IDS=()
declare -a API_KEYS=()
declare -a CREATED_FLAGS=()
declare -a NAMESPACES=()

die() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

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

contains() {
  local hay="$1" needle="$2"
  printf '%s' "$hay" | grep -Fq "$needle"
}

for command in curl jq; do
  command -v "$command" >/dev/null || die "$command is required"
done
command -v aws >/dev/null || die "aws cli is required for STS isolation checks"

if [ "$CLI_SOURCE" = "build" ]; then
  CLI_BIN="$(mktemp)"
  make -C "$REPO_ROOT" build-cli CLI_BIN="$CLI_BIN" >/dev/null
else
  die "CLI_SOURCE=$CLI_SOURCE not supported (use build)"
fi
chmod +x "$CLI_BIN"

CLI_HOME="$(mktemp -d)"
WORKDIR="$(mktemp -d)"
TS="$(date +%s)"
ADMIN_FLAGS=(--server "$BASE" --tidbcloud-public-key "$PUBLIC_KEY" --tidbcloud-private-key "$PRIVATE_KEY")

drive9_as() {
  local key="$1"
  shift
  env DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$key" HOME="$CLI_HOME" "$CLI_BIN" "$@"
}

drive9_admin() {
  env -u DRIVE9_SERVER -u DRIVE9_API_KEY HOME="$CLI_HOME" "$CLI_BIN" admin "$@"
}

uri_for() {
  local ns="$1"
  local uri="${SCHEME}://${BUCKET}/"
  if [ -n "$PREFIX" ]; then
    uri="${uri}${PREFIX}/"
  fi
  printf '%s%s' "$uri" "$ns"
}

wait_active() {
  local key="$1"
  local deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
  local last="" scode sfile
  while :; do
    sfile="$(mktemp)"
    scode=$(curl -sS -o "$sfile" -w "%{http_code}" -H "Authorization: Bearer $key" "$BASE/v1/status" || true)
    last=$(jq -r '.status // empty' "$sfile" 2>/dev/null || true)
    rm -f "$sfile"
    if [ "$scode" = "200" ] && [ "$last" = "active" ]; then
      printf '%s' "$last"
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      printf '%s' "$last"
      return 1
    fi
    sleep "$POLL_INTERVAL_S"
  done
}

provision_tenant() {
  local out
  out="$(drive9_admin tenant create "${ADMIN_FLAGS[@]}" --json 2>/dev/null || true)"
  if [ -z "$out" ]; then
    out="$(env -u DRIVE9_SERVER -u DRIVE9_API_KEY HOME="$CLI_HOME" "$CLI_BIN" create \
      --server "$BASE" \
      --tidbcloud-public-key "$PUBLIC_KEY" \
      --tidbcloud-private-key "$PRIVATE_KEY" \
      --json)"
  fi
  printf '%s' "$out"
}

mint_creds() {
  local key="$1" uri="$2" write="$3" out="$4"
  curl -sS -o "$out" -w "%{http_code}" --max-time 30 \
    -X POST "$BASE/v1/object-credentials" \
    -H "Authorization: Bearer $key" \
    -H "Content-Type: application/json" \
    -d "{\"uri\":\"$uri\",\"write\":$write}"
}

aws_with_session() {
  local creds="$1"
  shift
  local ak sk tok region
  ak=$(jq -r '.access_key_id' "$creds")
  sk=$(jq -r '.secret_access_key' "$creds")
  tok=$(jq -r '.session_token' "$creds")
  region=$(jq -r '.region // empty' "$creds")
  if [ -z "$region" ] || [ "$region" = "null" ]; then
    region="$REGION"
  fi
  env -u AWS_PROFILE -u AWS_DEFAULT_PROFILE \
    AWS_ACCESS_KEY_ID="$ak" \
    AWS_SECRET_ACCESS_KEY="$sk" \
    AWS_SESSION_TOKEN="$tok" \
    AWS_DEFAULT_REGION="$region" \
    aws --cli-connect-timeout 10 --cli-read-timeout 30 "$@"
}

list_keys() {
  local creds="$1" prefix="$2"
  local out rc
  out="$(mktemp)"
  set +e
  aws_with_session "$creds" s3api list-objects-v2 --bucket "$BUCKET" --prefix "$prefix" >"$out" 2>"$out.err"
  rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    if grep -Eqi 'AccessDenied|ExplicitDeny|not authorized' "$out.err" "$out"; then
      echo ACCESS_DENIED
      rm -f "$out" "$out.err"
      return 0
    fi
    echo "aws list failed prefix=$prefix rc=$rc $(cat "$out.err" "$out")" >&2
    rm -f "$out" "$out.err"
    return 1
  fi
  jq -r '.Contents[]?.Key // empty' "$out"
  rm -f "$out" "$out.err"
}

is_mounted() {
  local mount_point="$1"
  local physical
  physical="$(cd "$(dirname "$mount_point")" 2>/dev/null && pwd -P)/$(basename "$mount_point")"
  if command -v mountpoint >/dev/null 2>&1; then
    mountpoint -q "$mount_point"
    return
  fi
  mount | awk -v mp="$mount_point" -v pmp="$physical" '{for(i=1;i<=NF;i++) if($i=="on" && ($(i+1)==mp || $(i+1)==pmp)) found=1} END{exit !found}'
}

wait_mounted() {
  local mp="$1"
  local deadline=$(($(date +%s) + MOUNT_READY_TIMEOUT_S))
  while :; do
    if is_mounted "$mp"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep 0.4
  done
}

unmount_if_needed() {
  if [ -n "${MOUNT_POINT:-}" ] && is_mounted "$MOUNT_POINT" 2>/dev/null; then
    env DRIVE9_SERVER="$BASE" HOME="$CLI_HOME" "$CLI_BIN" umount "$MOUNT_POINT" >/dev/null 2>&1 || true
    umount "$MOUNT_POINT" >/dev/null 2>&1 || true
  fi
  if [ -n "${MOUNT_PID:-}" ]; then
    kill "$MOUNT_PID" >/dev/null 2>&1 || true
    wait "$MOUNT_PID" >/dev/null 2>&1 || true
    MOUNT_PID=""
  fi
}

cleanup() {
  unmount_if_needed
  if [ "$SKIP_CLEANUP" != "1" ]; then
    local i ns key tid
    if [ "${#NAMESPACES[@]}" -gt 0 ]; then
      for i in "${!NAMESPACES[@]}"; do
        ns="${NAMESPACES[$i]}"
        key="${API_KEYS[$i]:-}"
        if [ -n "$key" ] && [ -n "$ns" ]; then
          drive9_as "$key" fs rm --auth=server -r "$(uri_for "$ns")/" >/dev/null 2>&1 || true
        fi
      done
    fi
    if [ "${#TENANT_IDS[@]}" -gt 0 ]; then
      for tid in "${TENANT_IDS[@]}"; do
        drive9_admin tenant object-namespace clear --tenant-id "$tid" "${ADMIN_FLAGS[@]}" >/dev/null 2>&1 || true
      done
    fi
    if [ -n "${BACKEND_ID:-}" ]; then
      drive9_admin object-backend rm --id "$BACKEND_ID" "${ADMIN_FLAGS[@]}" >/dev/null 2>&1 || true
    fi
    if [ "${#TENANT_IDS[@]}" -gt 0 ]; then
      for i in "${!TENANT_IDS[@]}"; do
        if [ "${CREATED_FLAGS[$i]:-0}" = "1" ] && [ -n "${API_KEYS[$i]:-}" ]; then
          env -u DRIVE9_SERVER HOME="$CLI_HOME" "$CLI_BIN" delete \
            --server "$BASE" --api-key "${API_KEYS[$i]}" \
            --tidbcloud-public-key "$PUBLIC_KEY" --tidbcloud-private-key "$PRIVATE_KEY" -y \
            >/dev/null 2>&1 || true
        fi
      done
    fi
  fi
  rm -f "$CLI_BIN"
  rm -rf "$CLI_HOME" "$WORKDIR"
  if [ "$FAIL" -gt 0 ]; then
    exit 1
  fi
}
trap cleanup EXIT

echo "=== drive9 object-auth S3 hosted coverage ==="
echo "BASE=$BASE SCHEME=$SCHEME BUCKET=$BUCKET REGION=$REGION"

create_a="$(provision_tenant)"
API_A="$(printf '%s' "$create_a" | jq -r '.api_key // empty')"
TID_A="$(printf '%s' "$create_a" | jq -r '.tenant_id // empty')"
check_cmd "provisioned tenant A" test -n "$TID_A" -a -n "$API_A"
TENANT_IDS+=("$TID_A")
API_KEYS+=("$API_A")
CREATED_FLAGS+=(1)
check_eq "tenant A active" "$(wait_active "$API_A" || true)" "active"

create_b="$(provision_tenant)"
API_B="$(printf '%s' "$create_b" | jq -r '.api_key // empty')"
TID_B="$(printf '%s' "$create_b" | jq -r '.tenant_id // empty')"
check_cmd "provisioned tenant B" test -n "$TID_B" -a -n "$API_B"
TENANT_IDS+=("$TID_B")
API_KEYS+=("$API_B")
CREATED_FLAGS+=(1)
check_eq "tenant B active" "$(wait_active "$API_B" || true)" "active"

create_c="$(provision_tenant)"
API_C="$(printf '%s' "$create_c" | jq -r '.api_key // empty')"
TID_C="$(printf '%s' "$create_c" | jq -r '.tenant_id // empty')"
check_cmd "provisioned tenant C (no namespace)" test -n "$TID_C" -a -n "$API_C"
TENANT_IDS+=("$TID_C")
API_KEYS+=("$API_C")
CREATED_FLAGS+=(1)
check_eq "tenant C active" "$(wait_active "$API_C" || true)" "active"

add_args=(object-backend add --scheme "$SCHEME" --bucket "$BUCKET" --json "${ADMIN_FLAGS[@]}"
  --access-key-id "$DRIVE9_E2E_OBJECT_ACCESS_KEY_ID"
  --secret-access-key "$DRIVE9_E2E_OBJECT_SECRET_ACCESS_KEY"
  --region "$REGION" --name "s3-cov-${TS}" --credential-kind static)
if [ -n "$ENDPOINT" ]; then add_args+=(--endpoint "$ENDPOINT"); fi
if [ -n "$STS_ENDPOINT" ]; then add_args+=(--sts-endpoint "$STS_ENDPOINT"); fi
if [ -n "$PREFIX" ]; then add_args+=(--prefix "$PREFIX"); fi
add_out="$(drive9_admin "${add_args[@]}")"
BACKEND_ID="$(printf '%s' "$add_out" | jq -r '.id // empty')"
check_cmd "object-backend add" test -n "$BACKEND_ID"

NS_A="cust"
NS_B="cust-evil"
drive9_admin tenant object-namespace set --tenant-id "$TID_A" --namespace-id "$NS_A" "${ADMIN_FLAGS[@]}" >/dev/null
drive9_admin tenant object-namespace set --tenant-id "$TID_B" --namespace-id "$NS_B" "${ADMIN_FLAGS[@]}" >/dev/null
NAMESPACES+=("$NS_A" "$NS_B" "")
ns_a_got="$(drive9_admin tenant object-namespace get --tenant-id "$TID_A" --json "${ADMIN_FLAGS[@]}" | jq -r '.namespace_id')"
ns_b_got="$(drive9_admin tenant object-namespace get --tenant-id "$TID_B" --json "${ADMIN_FLAGS[@]}" | jq -r '.namespace_id')"
check_eq "namespace A is cust" "$ns_a_got" "$NS_A"
check_eq "namespace B is cust-evil" "$ns_b_got" "$NS_B"

URI_A="$(uri_for "$NS_A")"
URI_B="$(uri_for "$NS_B")"
FILE_A="${WORKDIR}/a.txt"
FILE_B="${WORKDIR}/b.txt"
printf 'alpha-%s\n' "$TS" >"$FILE_A"
printf 'bravo-%s\n' "$TS" >"$FILE_B"

echo "--- fail-closed / admin gate ---"
check_cmd_fail "tenant C without namespace cannot ls" \
  drive9_as "$API_C" fs ls --auth=server "${URI_A}/"
tenant_admin_code=$(curl -sS -o "$WORKDIR/tenant-admin.json" -w "%{http_code}" --max-time 30 \
  -X POST "$BASE/v1/admin/object-backends" \
  -H "Authorization: Bearer $API_A" \
  -H "Content-Type: application/json" \
  -d '{"scheme":"s3","bucket":"denied-'"$TS"'","access_key_id":"AKIAEXAMPLE","secret_access_key":"secret","credential_kind":"static"}')
check_cmd "tenant API key cannot register object-backend" \
  bash -c 'test "$1" != "200" && test "$1" != "201"' _ "$tenant_admin_code"

echo "--- CLI isolation ---"
drive9_as "$API_A" fs cp --auth=server "$FILE_A" "${URI_A}/hello.txt" --force >/dev/null
drive9_as "$API_B" fs cp --auth=server "$FILE_B" "${URI_B}/other.txt" --force >/dev/null
ls_a="$(drive9_as "$API_A" fs ls --auth=server "${URI_A}/")"
ls_b="$(drive9_as "$API_B" fs ls --auth=server "${URI_B}/")"
check_cmd "A ls sees hello.txt" contains "$ls_a" "hello.txt"
check_cmd "A ls does not see other.txt" bash -c '! printf %s "$1" | grep -Fq other.txt' _ "$ls_a"
check_cmd "B ls sees other.txt" contains "$ls_b" "other.txt"
check_cmd "B ls does not see hello.txt" bash -c '! printf %s "$1" | grep -Fq hello.txt' _ "$ls_b"
cat_a="$(drive9_as "$API_A" fs cat --auth=server "${URI_A}/hello.txt")"
check_cmd "A cat own object" contains "$cat_a" "alpha-"
check_cmd_fail "A cannot cat B object" \
  drive9_as "$API_A" fs cat --auth=server "${URI_B}/other.txt"
check_cmd_fail "B cannot cat A object" \
  drive9_as "$API_B" fs cat --auth=server "${URI_A}/hello.txt"
check_cmd_fail "A cannot write B prefix" \
  drive9_as "$API_A" fs cp --auth=server "$FILE_A" "${URI_B}/pwn.txt" --force
check_cmd_fail "A cannot list B prefix" \
  drive9_as "$API_A" fs ls --auth=server "${URI_B}/"

echo "--- STS ListBucket isolation ---"
mint_a="$(mktemp "$WORKDIR/mint-a.XXXXXX")"
mint_b="$(mktemp "$WORKDIR/mint-b.XXXXXX")"
code_a="$(mint_creds "$API_A" "${URI_A}/hello.txt" true "$mint_a")"
code_b="$(mint_creds "$API_B" "${URI_B}/other.txt" true "$mint_b")"
check_eq "mint A write 200" "$code_a" "200"
check_eq "mint B write 200" "$code_b" "200"

keys_cust_slash="$(list_keys "$mint_a" "${NS_A}/")"
keys_cust="$(list_keys "$mint_a" "$NS_A")"
keys_evil_as_a="$(list_keys "$mint_a" "${NS_B}/")"
keys_root_as_a="$(list_keys "$mint_a" "")"
keys_evil_as_b="$(list_keys "$mint_b" "${NS_B}/")"
check_cmd "STS A ListBucket cust/ sees hello.txt" contains "$keys_cust_slash" "${NS_A}/hello.txt"
check_cmd "STS A ListBucket cust/ hides cust-evil" bash -c '! printf %s "$1" | grep -Fq cust-evil' _ "$keys_cust_slash"
check_eq "STS A ListBucket prefix=cust (no slash) denied or empty" \
  "$(if [ "$keys_cust" = "ACCESS_DENIED" ] || [ -z "$keys_cust" ]; then echo isolated; else echo "$keys_cust"; fi)" \
  "isolated"
check_eq "STS A ListBucket cust-evil/ denied or empty" \
  "$(if [ "$keys_evil_as_a" = "ACCESS_DENIED" ] || [ -z "$keys_evil_as_a" ]; then echo isolated; else echo "$keys_evil_as_a"; fi)" \
  "isolated"
check_eq "STS A ListBucket bucket root denied or empty" \
  "$(if [ "$keys_root_as_a" = "ACCESS_DENIED" ] || [ -z "$keys_root_as_a" ]; then echo isolated; else echo "$keys_root_as_a"; fi)" \
  "isolated"
check_cmd "STS B ListBucket cust-evil/ sees other.txt" contains "$keys_evil_as_b" "${NS_B}/other.txt"

echo "--- read-only mint ---"
mint_ro="$(mktemp "$WORKDIR/mint-ro.XXXXXX")"
code_ro="$(mint_creds "$API_A" "${URI_A}/hello.txt" false "$mint_ro")"
check_eq "mint A read 200" "$code_ro" "200"
got_body="$(mktemp)"
check_cmd "read-only session can GetObject" \
  aws_with_session "$mint_ro" s3api get-object --bucket "$BUCKET" --key "${NS_A}/hello.txt" "$got_body"
check_cmd "read-only GetObject body matches" contains "$(cat "$got_body")" "alpha-"
ro_put="$(mktemp)"
set +e
aws_with_session "$mint_ro" s3api put-object --bucket "$BUCKET" --key "${NS_A}/from-ro.txt" --body "$FILE_A" >"$ro_put" 2>"$ro_put.err"
ro_rc=$?
set -e
check_cmd "read-only session cannot PutObject" test "$ro_rc" -ne 0

echo "--- drive9 <-> S3 copy ---"
drive9_as "$API_A" fs mkdir --auth=server :/s3-xfer/ >/dev/null || true
drive9_as "$API_A" fs cp --auth=server "${URI_A}/hello.txt" :/s3-xfer/from-s3.txt --force >/dev/null
d9_got="$(drive9_as "$API_A" fs cat --auth=server :/s3-xfer/from-s3.txt)"
check_cmd "S3 -> drive9 copy" contains "$d9_got" "alpha-"
drive9_as "$API_A" fs cp --auth=server :/s3-xfer/from-s3.txt "${URI_A}/roundtrip.txt" --force >/dev/null
rt_got="$(drive9_as "$API_A" fs cat --auth=server "${URI_A}/roundtrip.txt")"
check_cmd "drive9 -> S3 copy" contains "$rt_got" "alpha-"

echo "--- FUSE mount ---"
MOUNT_POINT="${WORKDIR}/mnt"
CACHE_DIR="${WORKDIR}/obj-cache"
mkdir -p "$MOUNT_POINT" "$CACHE_DIR"
MOUNT_LOG="${WORKDIR}/mount.log"
env DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_A" HOME="$CLI_HOME" \
  DRIVE9_OBJECT_SESSION_REFRESH_MIN_LEAD=59m50s \
  DRIVE9_OBJECT_SESSION_REFRESH_MAX_LEAD=59m50s \
  "$CLI_BIN" mount --mode=fuse --foreground --no-supervise --profile=none --auth=server \
  --cache-dir "$CACHE_DIR" "${URI_A}/" "$MOUNT_POINT" >"$MOUNT_LOG" 2>&1 &
MOUNT_PID=$!
if wait_mounted "$MOUNT_POINT"; then
  check_cmd "object FUSE mounted" true
  echo "mount-via-fuse ${TS}" >"${MOUNT_POINT}/mounted.txt"
  sync
  deadline=$(($(date +%s) + WRITEBACK_TIMEOUT_S))
  mounted_ok=0
  while :; do
    if [ -f "${MOUNT_POINT}/mounted.txt" ] && grep -q "mount-via-fuse" "${MOUNT_POINT}/mounted.txt" 2>/dev/null; then
      mounted_ok=1
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    sleep 0.4
  done
  check_eq "POSIX read through mount" "$mounted_ok" "1"
  ls_mnt="$(ls "$MOUNT_POINT")"
  check_cmd "POSIX ls sees hello.txt" contains "$ls_mnt" "hello.txt"
  check_cmd "POSIX ls sees mounted.txt" contains "$ls_mnt" "mounted.txt"
  remote_mnt="$(drive9_as "$API_A" fs cat --auth=server "${URI_A}/mounted.txt" || true)"
  deadline=$(($(date +%s) + WRITEBACK_TIMEOUT_S))
  while ! contains "$remote_mnt" "mount-via-fuse"; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    sleep 0.5
    remote_mnt="$(drive9_as "$API_A" fs cat --auth=server "${URI_A}/mounted.txt" || true)"
  done
  check_cmd "mount write visible via fs cat" contains "$remote_mnt" "mount-via-fuse"

  echo "--- STS refresh on live mount ---"
  initial_ak="$(jq -r '.access_key_id' "$mint_a")"
  refreshed=0
  deadline=$(($(date +%s) + REFRESH_WAIT_S))
  while :; do
    if grep -F "object mount session refreshed" "$MOUNT_LOG" >/dev/null 2>&1; then
      refreshed=1
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    sleep 1
  done
  check_eq "mount reminted before 1h expiry" "$refreshed" "1"
  refresh_ak="$(grep -F "object mount session refreshed" "$MOUNT_LOG" | tail -1 | python3 -c 'import json,sys,re
raw=sys.stdin.read()
ak=""
try:
    ak=json.loads(raw).get("access_key_id") or ""
except Exception:
    m=re.search(r"access_key_id[=: ]+([A-Z0-9…]+)", raw)
    if m:
        ak=m.group(1)
print(ak)' || true)"
  if [ -n "$refresh_ak" ] && [ -n "$initial_ak" ]; then
    initial_shown="${initial_ak:0:8}…${initial_ak: -4}"
    check_cmd "refreshed access key differs from previous mint" \
      bash -c 'test "$1" != "$2"' _ "$refresh_ak" "$initial_shown"
  else
    check_cmd "refreshed access key logged" test -n "$refresh_ak"
  fi
  echo "after-refresh ${TS}" >"${MOUNT_POINT}/after-refresh.txt"
  sync
  deadline=$(($(date +%s) + WRITEBACK_TIMEOUT_S))
  after_ok=0
  while :; do
    if grep -q "after-refresh" "${MOUNT_POINT}/after-refresh.txt" 2>/dev/null; then
      after_ok=1
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    sleep 0.4
  done
  check_eq "POSIX IO works after refresh" "$after_ok" "1"
  remote_after="$(drive9_as "$API_A" fs cat --auth=server "${URI_A}/after-refresh.txt" || true)"
  deadline=$(($(date +%s) + WRITEBACK_TIMEOUT_S))
  while ! contains "$remote_after" "after-refresh"; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    sleep 0.5
    remote_after="$(drive9_as "$API_A" fs cat --auth=server "${URI_A}/after-refresh.txt" || true)"
  done
  check_cmd "post-refresh write visible via server mint" contains "$remote_after" "after-refresh"
else
  echo "FAIL object FUSE did not become ready; log:" >&2
  cat "$MOUNT_LOG" >&2 || true
  check_cmd "object FUSE mounted" false
fi

unmount_if_needed
if is_mounted "$MOUNT_POINT" 2>/dev/null; then
  check_cmd "object FUSE unmounted" false
else
  check_cmd "object FUSE unmounted" true
fi

echo "=== object-auth S3 hosted coverage done PASS=$PASS FAIL=$FAIL TOTAL=$TOTAL ==="
if [ "$FAIL" -gt 0 ]; then
  echo "mount log:" >&2
  cat "$MOUNT_LOG" >&2 || true
  exit 1
fi
