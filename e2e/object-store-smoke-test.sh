#!/usr/bin/env bash
# Object-store CLI + mount smoke against a local MinIO (or an existing S3 URI).
#
# Does not need drive9-server. Starts MinIO via docker/podman unless
# OBJECT_S3_URI is already set (then uses that URI and the process AWS env).
#
#   bash e2e/object-store-smoke-test.sh
#   OBJECT_S3_URI='s3://bucket/prefix/?region=us-east-1' bash e2e/object-store-smoke-test.sh
#   OBJECT_STRICT_MOUNT=1 bash e2e/object-store-smoke-test.sh   # fail if FUSE unavailable
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
OBJECT_STRICT_MOUNT="${OBJECT_STRICT_MOUNT:-0}"
OBJECT_STRICT_CROSS="${OBJECT_STRICT_CROSS:-$OBJECT_STRICT_MOUNT}"
OBJECT_CMD_TIMEOUT_S="${OBJECT_CMD_TIMEOUT_S:-120}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:RELEASE.2024-12-18T13-15-44Z}"
MINIO_PORT="${MINIO_PORT:-19000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-drive9minio}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-drive9minio}"
MINIO_BUCKET="${MINIO_BUCKET:-drive9-e2e}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-30}"
WRITEBACK_TIMEOUT_S="${WRITEBACK_TIMEOUT_S:-30}"

PASS=0
FAIL=0
SKIP=0
TOTAL=0
CLI_BIN=""
RUNTIME=""
MINIO_CID=""
MINIO_BIN=""
MINIO_PID=""
MOUNT_POINT=""
CACHE_DIR=""
WORKDIR=""

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

skip_check() {
  local desc="$1"
  TOTAL=$((TOTAL + 1))
  SKIP=$((SKIP + 1))
  echo "SKIP $desc"
}

contains() {
  local hay="$1" needle="$2"
  case "$hay" in
    *"$needle"*) return 0 ;;
    *) return 1 ;;
  esac
}

detect_release_target() {
  case "$(uname -s)" in
    Linux) CLI_RELEASE_OS="linux" ;;
    Darwin) CLI_RELEASE_OS="darwin" ;;
    *)
      echo "unsupported OS for official CLI download: $(uname -s)" >&2
      return 1
      ;;
  esac
  case "$(uname -m)" in
    x86_64 | amd64) CLI_RELEASE_ARCH="amd64" ;;
    aarch64 | arm64) CLI_RELEASE_ARCH="arm64" ;;
    *)
      echo "unsupported architecture for official CLI download: $(uname -m)" >&2
      return 1
      ;;
  esac
}

download_official_cli() {
  local target_version="$CLI_RELEASE_VERSION"
  detect_release_target || return 1
  if [ -z "$target_version" ]; then
    target_version=$(curl -fsSL "$CLI_RELEASE_BASE_URL/version" | tr -d '[:space:]')
  fi
  curl -fsSL "$CLI_RELEASE_BASE_URL/drive9-$CLI_RELEASE_OS-$CLI_RELEASE_ARCH" -o "$CLI_BIN"
  chmod +x "$CLI_BIN"
}

prepare_cli_binary() {
  CLI_BIN="$(mktemp "${TMPDIR:-/tmp}/drive9-obj-cli.XXXXXX")"
  case "$CLI_SOURCE" in
    build)
      (cd "$REPO_ROOT" && make build-cli CLI_BIN="$CLI_BIN")
      ;;
    official)
      download_official_cli
      ;;
    *)
      echo "invalid CLI_SOURCE: $CLI_SOURCE (expected build|official)" >&2
      return 1
      ;;
  esac
}

drive9() {
  local bin_args=("$@")
  # Native MinIO uses process AWS env. Server mint is not configured in this
  # smoke, so fs/mount object URIs always take --auth=local.
  case "${1:-}" in
    fs)
      if [ "${#bin_args[@]}" -ge 2 ]; then
        bin_args=("${1}" "${2}" --auth=local "${bin_args[@]:2}")
      fi
      ;;
    mount)
      bin_args=(mount --auth=local "${bin_args[@]:1}")
      ;;
  esac
  if command -v timeout >/dev/null 2>&1; then
    timeout --signal=TERM --kill-after=5s "${OBJECT_CMD_TIMEOUT_S}s" "$CLI_BIN" "${bin_args[@]}"
  else
    "$CLI_BIN" "${bin_args[@]}"
  fi
}

pick_runtime() {
  if command -v docker >/dev/null 2>&1; then
    RUNTIME=docker
    return 0
  fi
  if command -v podman >/dev/null 2>&1; then
    RUNTIME=podman
    return 0
  fi
  return 1
}

minio_arch() {
  case "$(uname -m)" in
    x86_64 | amd64) echo "linux-amd64" ;;
    aarch64 | arm64) echo "linux-arm64" ;;
    *) return 1 ;;
  esac
}

ensure_minio_bin() {
  if command -v minio >/dev/null 2>&1; then
    MINIO_BIN="$(command -v minio)"
    return 0
  fi
  local arch
  arch="$(minio_arch)" || return 1
  MINIO_BIN="$WORKDIR/minio"
  echo "fetching MinIO $arch to $MINIO_BIN"
  curl -fsSL "https://dl.min.io/server/minio/release/${arch}/minio" -o "$MINIO_BIN"
  chmod +x "$MINIO_BIN"
}

start_minio_bin() {
  ensure_minio_bin || return 1
  local data="$WORKDIR/minio-data"
  mkdir -p "$data"
  MINIO_ROOT_USER="$MINIO_ROOT_USER" MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
    "$MINIO_BIN" server "$data" --address "127.0.0.1:${MINIO_PORT}" >"$WORKDIR/minio.log" 2>&1 &
  MINIO_PID=$!
}

wait_http() {
  local url="$1"
  local deadline=$(($(date +%s) + 40))
  while :; do
    if curl -sf "$url" >/dev/null; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep 0.4
  done
}

create_bucket() {
  local host="$1" bucket="$2"
  python3 - "$host" "$bucket" <<'PY'
import datetime, hashlib, hmac, os, sys, urllib.error, urllib.request

host, bucket = sys.argv[1], sys.argv[2]
access = os.environ["AWS_ACCESS_KEY_ID"]
secret = os.environ["AWS_SECRET_ACCESS_KEY"]
region = os.environ.get("AWS_REGION", "us-east-1")
service = "s3"
method = "PUT"
path = "/" + bucket
now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
amzdate = now.strftime("%Y%m%dT%H%M%SZ")
datestamp = now.strftime("%Y%m%d")
payload_hash = hashlib.sha256(b"").hexdigest()
canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amzdate}\n"
signed_headers = "host;x-amz-content-sha256;x-amz-date"
canonical_request = f"{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
scope = f"{datestamp}/{region}/{service}/aws4_request"
string_to_sign = (
    "AWS4-HMAC-SHA256\n"
    + amzdate
    + "\n"
    + scope
    + "\n"
    + hashlib.sha256(canonical_request.encode()).hexdigest()
)

def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()

k_date = _sign(("AWS4" + secret).encode(), datestamp)
k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()
auth = (
    f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
    f"SignedHeaders={signed_headers}, Signature={signature}"
)
req = urllib.request.Request(
    f"http://{host}{path}",
    method="PUT",
    headers={
        "x-amz-date": amzdate,
        "x-amz-content-sha256": payload_hash,
        "Authorization": auth,
    },
)
try:
    with urllib.request.urlopen(req) as resp:
        sys.exit(0 if 200 <= resp.status < 300 else 1)
except urllib.error.HTTPError as err:
    # Already exists is fine.
    if err.code in (409, 200):
        sys.exit(0)
    sys.stderr.write(err.read().decode("utf-8", "replace") + "\n")
    sys.exit(1)
PY
}

urlencode() {
  python3 -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
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

wait_obj_cat_eq() {
  local uri="$1" want="$2"
  local deadline=$(($(date +%s) + WRITEBACK_TIMEOUT_S))
  local got
  while :; do
    if got=$(drive9 fs cat "$uri" 2>/dev/null) && [ "$got" = "$want" ]; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "timeout waiting for $uri == $want (last=$(drive9 fs cat "$uri" 2>&1 || true))" >&2
      return 1
    fi
    sleep 0.5
  done
}

cleanup() {
  if [ -n "${MOUNT_POINT:-}" ] && is_mounted "$MOUNT_POINT" 2>/dev/null; then
    drive9 umount "$MOUNT_POINT" >/dev/null 2>&1 || true
    if command -v fusermount3 >/dev/null 2>&1; then
      fusermount3 -u "$MOUNT_POINT" >/dev/null 2>&1 || true
    elif command -v fusermount >/dev/null 2>&1; then
      fusermount -u "$MOUNT_POINT" >/dev/null 2>&1 || true
    fi
  fi
  if [ -n "${MINIO_CID:-}" ] && [ -n "${RUNTIME:-}" ]; then
    "$RUNTIME" rm -f "$MINIO_CID" >/dev/null 2>&1 || true
  fi
  if [ -n "${MINIO_PID:-}" ]; then
    kill "$MINIO_PID" >/dev/null 2>&1 || true
    wait "$MINIO_PID" >/dev/null 2>&1 || true
  fi
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ -n "${WORKDIR:-}" ]; then
    rm -rf "$WORKDIR"
  fi
}
trap cleanup EXIT

echo "=== drive9 object-store smoke ==="

check_cmd "python3 is available" bash -c 'command -v python3 >/dev/null'
check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
fi
check_cmd "build CLI" prepare_cli_binary
check_cmd "CLI binary executable" test -x "$CLI_BIN"

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/drive9-obj-e2e.XXXXXX")"
CACHE_DIR="$WORKDIR/cache"
mkdir -p "$CACHE_DIR"
TS="$(date +%s)"
PREFIX="run-${TS}"

obj() {
  local key="$1"
  if [ -n "$key" ]; then
    printf '%s/%s%s\n' "$OBJ_BASE" "$key" "$OBJ_QUERY"
  else
    printf '%s/%s\n' "$OBJ_BASE" "$OBJ_QUERY"
  fi
}

if [ -n "${OBJECT_S3_URI:-}" ]; then
  echo "using provided OBJECT_S3_URI"
  # Split user URI so we can append keys before ?query.
  OBJ_BASE="${OBJECT_S3_URI%%\?*}"
  OBJ_BASE="${OBJ_BASE%/}/${PREFIX}"
  if [ "$OBJECT_S3_URI" = "${OBJECT_S3_URI%%\?*}" ]; then
    OBJ_QUERY=""
  else
    OBJ_QUERY="?${OBJECT_S3_URI#*\?}"
  fi
else
  export AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER"
  export AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
  export AWS_REGION="${AWS_REGION:-us-east-1}"
  export AWS_DEFAULT_REGION="$AWS_REGION"

  if pick_runtime; then
    echo "starting MinIO with $RUNTIME on :$MINIO_PORT"
    MINIO_CID="drive9-e2e-minio-$$"
    if ! "$RUNTIME" run -d --name "$MINIO_CID" \
      -p "${MINIO_PORT}:9000" \
      -e "MINIO_ROOT_USER=${MINIO_ROOT_USER}" \
      -e "MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}" \
      "$MINIO_IMAGE" server /data >/dev/null; then
      echo "FAIL start MinIO container"
      exit 1
    fi
  else
    echo "no docker/podman; starting MinIO binary on 127.0.0.1:${MINIO_PORT}"
    if ! start_minio_bin; then
      echo "FAIL need docker, podman, or a downloadable MinIO binary (or set OBJECT_S3_URI)"
      exit 1
    fi
  fi
  check_cmd "MinIO health" wait_http "http://127.0.0.1:${MINIO_PORT}/minio/health/live"
  check_cmd "create MinIO bucket $MINIO_BUCKET" create_bucket "127.0.0.1:${MINIO_PORT}" "$MINIO_BUCKET"

  OBJ_BASE="s3://${MINIO_BUCKET}/${PREFIX}"
  OBJ_QUERY="?region=${AWS_REGION}&endpoint=$(urlencode "http://127.0.0.1:${MINIO_PORT}")&forcePathStyle=true"
fi

echo "OBJ_BASE=$OBJ_BASE"
echo "OBJ_QUERY=$OBJ_QUERY"

# --- path classification ---
set +e
ls_err=$(drive9 fs ls "s3:///" 2>&1)
ls_rc=$?
set -e
check_eq "empty bucket URI is rejected" "$ls_rc" "1"
case "$ls_err" in
  *[Bb]ucket* | *[Ee]mpty*) echo "PASS empty-bucket error mentions bucket" ; PASS=$((PASS + 1)) ;;
  *) echo "FAIL empty-bucket error mentions bucket (err=$ls_err)" ; FAIL=$((FAIL + 1)) ;;
esac
TOTAL=$((TOTAL + 1))

# --- write / list / stat / cat ---
printf 'hello-object' >"$WORKDIR/hello.txt"
check_cmd "fs mkdir prefix" drive9 fs mkdir "$(obj "")"
check_cmd "fs cp local → object" drive9 fs cp "$WORKDIR/hello.txt" "$(obj hello.txt)"
ls_out=$(drive9 fs ls "$(obj "")")
check_cmd "fs ls shows hello.txt" contains "$ls_out" "hello.txt"
stat_out=$(drive9 fs stat "$(obj hello.txt)")
check_cmd "fs stat kind=file" contains "$stat_out" "file"
cat_out=$(drive9 fs cat "$(obj hello.txt)")
check_eq "fs cat hello.txt" "$cat_out" "hello-object"
range_out=$(drive9 fs cat --offset 0 --length 5 "$(obj hello.txt)")
check_eq "fs cat --offset/--length" "$range_out" "hello"

# --- download / same-bucket copy / mv ---
check_cmd "fs cp object → local" drive9 fs cp "$(obj hello.txt)" "$WORKDIR/hello.down"
check_eq "downloaded content" "$(cat "$WORKDIR/hello.down")" "hello-object"
check_cmd "fs cp same-bucket" drive9 fs cp "$(obj hello.txt)" "$(obj hello.copy)"
check_eq "copied object cat" "$(drive9 fs cat "$(obj hello.copy)")" "hello-object"
check_cmd "fs mv same-bucket" drive9 fs mv "$(obj hello.copy)" "$(obj hello.moved)"
check_eq "moved object cat" "$(drive9 fs cat "$(obj hello.moved)")" "hello-object"
set +e
drive9 fs stat "$(obj hello.copy)" >/dev/null 2>&1
mv_src_rc=$?
set -e
check_eq "mv source gone" "$mv_src_rc" "1"

# --- recursive tree ---
mkdir -p "$WORKDIR/tree/sub"
printf 'leaf-a' >"$WORKDIR/tree/a.txt"
printf 'leaf-b' >"$WORKDIR/tree/sub/b.txt"
check_cmd "fs cp -r tree" drive9 fs cp -r "$WORKDIR/tree" "$(obj tree)"
tree_ls=$(drive9 fs ls "$(obj tree)")
check_cmd "fs ls tree has a.txt" contains "$tree_ls" "a.txt"
check_eq "recursive cat sub/b.txt" "$(drive9 fs cat "$(obj tree/sub/b.txt)")" "leaf-b"

# --- stdin / stdout ---
printf 'from-stdin' | drive9 fs cp - "$(obj stdin.txt)"
check_eq "cp stdin → object" "$(drive9 fs cat "$(obj stdin.txt)")" "from-stdin"
stdout_out=$(drive9 fs cp "$(obj stdin.txt)" -)
check_eq "cp object → stdout" "$stdout_out" "from-stdin"

# --- rejected object ops ---
check_cmd_fail "chmod rejected on object URI" drive9 fs chmod 644 "$(obj hello.txt)"
check_cmd_fail "find rejected on object URI" drive9 fs find "$(obj "")"
check_cmd_fail "grep rejected on object URI" drive9 fs grep hello "$(obj "")"
check_cmd_fail "symlink rejected on object URI" drive9 fs symlink "$(obj hello.txt)" "$(obj link)"
check_cmd_fail "hardlink rejected on object URI" drive9 fs hardlink "$(obj hello.txt)" "$(obj hard)"
check_cmd_fail "cross-scheme mv rejected" drive9 fs mv "$(obj hello.txt)" "$WORKDIR/local-mv.txt"
check_cmd_fail "s3:// never becomes a local file named s3" test -e "s3://${MINIO_BUCKET:-bucket}/${PREFIX}/hello.txt"

# --- rm ---
check_cmd "fs rm file" drive9 fs rm "$(obj hello.moved)"
check_cmd "fs rm -r tree" drive9 fs rm -r "$(obj tree)"

# --- mount ---
run_mount=1
mount_reason=""
case "$(uname -s)" in
  Linux)
    if ! command -v fusermount3 >/dev/null 2>&1 && ! command -v fusermount >/dev/null 2>&1; then
      run_mount=0
      mount_reason="no fusermount"
    fi
    ;;
  Darwin) ;;
  *)
    run_mount=0
    mount_reason="unsupported OS $(uname -s)"
    ;;
esac

if [ "$run_mount" != "1" ]; then
  if [ "$OBJECT_STRICT_MOUNT" = "1" ]; then
    echo "FAIL mount required but unavailable: $mount_reason"
    FAIL=$((FAIL + 1))
    TOTAL=$((TOTAL + 1))
  else
    skip_check "object mount ($mount_reason)"
  fi
else
  MOUNT_POINT="$WORKDIR/mnt"
  mkdir -p "$MOUNT_POINT"
  MOUNT_ARGS=(--mode=fuse --cache-dir "$CACHE_DIR")
  echo "mounting $(obj "") -> $MOUNT_POINT"
  check_cmd "drive9 mount object URI" drive9 mount "${MOUNT_ARGS[@]}" "$(obj "")" "$MOUNT_POINT"
  if ! wait_mounted "$MOUNT_POINT"; then
    echo "FAIL wait for object mount"
    FAIL=$((FAIL + 1))
    TOTAL=$((TOTAL + 1))
  else
    echo "PASS object mount became ready"
    PASS=$((PASS + 1))
    TOTAL=$((TOTAL + 1))

    check_cmd "mount ls sees hello.txt" test -f "$MOUNT_POINT/hello.txt"
    check_eq "mount cat hello.txt" "$(cat "$MOUNT_POINT/hello.txt")" "hello-object"
    printf 'via-fuse' >"$MOUNT_POINT/from-mount.txt"
    check_cmd "mount write from-mount.txt" test -f "$MOUNT_POINT/from-mount.txt"
    check_cmd "fs cat sees mount write (after writeback)" wait_obj_cat_eq "$(obj from-mount.txt)" "via-fuse"

    mkdir -p "$MOUNT_POINT/mdir"
    printf 'nested' >"$MOUNT_POINT/mdir/n.txt"
    check_cmd "mount nested write visible via fs" wait_obj_cat_eq "$(obj mdir/n.txt)" "nested"

    mv "$MOUNT_POINT/from-mount.txt" "$MOUNT_POINT/from-mount.renamed.txt"
    check_cmd "mount rename visible via fs" wait_obj_cat_eq "$(obj from-mount.renamed.txt)" "via-fuse"

    check_cmd "mount status" drive9 mount status "$MOUNT_POINT"
    check_cmd "mount health" drive9 mount health "$MOUNT_POINT"
    check_cmd_fail "mount drain rejected on object mount" drive9 mount drain "$MOUNT_POINT"

    check_cmd "umount object mount" drive9 umount --timeout 60s "$MOUNT_POINT"
    if is_mounted "$MOUNT_POINT"; then
      echo "FAIL unmounted"
      FAIL=$((FAIL + 1))
    else
      echo "PASS unmounted"
      PASS=$((PASS + 1))
    fi
    TOTAL=$((TOTAL + 1))

    check_cmd "remount object URI" drive9 mount "${MOUNT_ARGS[@]}" "$(obj "")" "$MOUNT_POINT"
    if wait_mounted "$MOUNT_POINT"; then
      check_eq "remount still has hello.txt" "$(cat "$MOUNT_POINT/hello.txt")" "hello-object"
      check_eq "remount still has renamed write" "$(cat "$MOUNT_POINT/from-mount.renamed.txt")" "via-fuse"
    else
      echo "FAIL remount did not become ready"
      FAIL=$((FAIL + 1))
      TOTAL=$((TOTAL + 1))
    fi
    drive9 umount --timeout 60s "$MOUNT_POINT" >/dev/null 2>&1 || true
  fi
fi

# --- drive9 ↔ object (required in CI when DRIVE9_BASE is up) ---
run_drive9_object_cross() {
  local base="${DRIVE9_BASE:-}"
  local key="${DRIVE9_API_KEY:-}"
  if [ -z "$base" ]; then
    if [ "$OBJECT_STRICT_CROSS" = "1" ]; then
      echo "FAIL drive9 ↔ object (DRIVE9_BASE is required when OBJECT_STRICT_CROSS=1)"
      FAIL=$((FAIL + 1))
      TOTAL=$((TOTAL + 1))
    else
      skip_check "drive9 ↔ object (set DRIVE9_BASE)"
    fi
    return
  fi
  if [ -z "$key" ]; then
    local pfile
    pfile="$(mktemp)"
    local pcode
    pcode=$(curl -sS -o "$pfile" -w "%{http_code}" -X POST "$base/v1/provision" || true)
    if [ "$pcode" != "202" ] && [ "$pcode" != "200" ]; then
      if [ "$OBJECT_STRICT_CROSS" = "1" ]; then
        echo "FAIL provision for drive9 ↔ object (HTTP $pcode)"
        FAIL=$((FAIL + 1))
        TOTAL=$((TOTAL + 1))
      else
        skip_check "drive9 ↔ object (provision HTTP $pcode)"
      fi
      rm -f "$pfile"
      return
    fi
    key=$(jq -r '.api_key // empty' "$pfile")
    rm -f "$pfile"
  fi
  if [ -z "$key" ]; then
    if [ "$OBJECT_STRICT_CROSS" = "1" ]; then
      echo "FAIL drive9 ↔ object (empty api_key)"
      FAIL=$((FAIL + 1))
      TOTAL=$((TOTAL + 1))
    else
      skip_check "drive9 ↔ object (empty api_key)"
    fi
    return
  fi
  echo "[cross] drive9 ↔ object copy via $base"
  export DRIVE9_SERVER="$base"
  export DRIVE9_API_KEY="$key"
  printf 'from-drive9' >"$WORKDIR/d9.txt"
  local remote=":/obj-e2e-${TS}.txt"
  if ! drive9 fs cp "$WORKDIR/d9.txt" "$remote"; then
    echo "FAIL fs cp local → drive9 for cross-backend"
    FAIL=$((FAIL + 1))
    TOTAL=$((TOTAL + 1))
    return
  fi
  check_cmd "fs cp drive9 → object" drive9 fs cp "$remote" "$(obj from-drive9.txt)"
  check_eq "drive9-sourced object" "$(drive9 fs cat "$(obj from-drive9.txt)")" "from-drive9"
  check_cmd "fs cp object → drive9" drive9 fs cp "$(obj hello.txt)" ":/obj-e2e-${TS}-from-s3.txt"
  check_eq "object-sourced drive9 file" "$(drive9 fs cat ":/obj-e2e-${TS}-from-s3.txt")" "hello"
  drive9 fs rm "$remote" >/dev/null 2>&1 || true
  drive9 fs rm ":/obj-e2e-${TS}-from-s3.txt" >/dev/null 2>&1 || true
}

run_drive9_object_cross()

echo
echo "RESULT: $PASS passed, $FAIL failed, $SKIP skipped, $TOTAL total"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
exit 0
