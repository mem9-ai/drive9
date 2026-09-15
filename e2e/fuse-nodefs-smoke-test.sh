#!/usr/bin/env bash
# Deterministic Node.js fs smoke workload on a Drive9 FUSE mount.
#
# Basic-operations gate: a zero-dependency Node script exercises the fs API
# surface real Node tooling depends on (sync/promises roundtrips, streams with
# small and default highWaterMarks, FileHandle positional IO incl. sparse
# writes, readdir withFileTypes, rename/atomic replace, symlink/hardlink,
# stat/statfs, chmod/utimes, copyFile incl. COPYFILE_FICLONE, and the ENOENT/
# EEXIST/ENOTEMPTY/EISDIR error-code matrix), verifies cross-channel
# consistency with the drive9 CLI in both directions, then unmounts/remounts
# and re-verifies the persisted tree from a checksum manifest. Broader
# official Node core fs test coverage lives in blackbox (community.node_fs).

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-20}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
FUSE_MOUNT_ROOT="${FUSE_MOUNT_ROOT:-/tmp}"
FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-0}"
FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-60s}"
FUSE_NODEFS_KEEP_ARTIFACTS="${FUSE_NODEFS_KEEP_ARTIFACTS:-0}"
FUSE_NODEFS_WORKLOAD_TIMEOUT_S="${FUSE_NODEFS_WORKLOAD_TIMEOUT_S:-240}"
FUSE_NODEFS_CROSS_TIMEOUT_S="${FUSE_NODEFS_CROSS_TIMEOUT_S:-60}"
FUSE_NODEFS_LARGE_MB="${FUSE_NODEFS_LARGE_MB:-9}"
FUSE_NODEFS_MIN_NODE_VERSION="${FUSE_NODEFS_MIN_NODE_VERSION:-18.17.0}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODEFS_JS="$SCRIPT_DIR/tools/nodefs_smoke.js"

PASS=0
FAIL=0
TOTAL=0

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$got" = "$want" ]; then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc"
    echo "  want: $want"
    echo "  got:  $got"
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

require_cmd() {
  local name="$1"
  TOTAL=$((TOTAL + 1))
  if command -v "$name" >/dev/null 2>&1; then
    echo "PASS $name is available"
    PASS=$((PASS + 1))
    return 0
  fi
  echo "FAIL $name is available" >&2
  FAIL=$((FAIL + 1))
  exit 1
}

skip() {
  echo "SKIP $*"
  exit 0
}

skip_or_fail() {
  if [ "$FUSE_STRICT_PREREQS" = "1" ]; then
    echo "FAIL $*" >&2
    exit 1
  fi
  skip "$@"
}

node_version_ok() {
  local version
  version=$(node --version 2>/dev/null || true)
  python3 - "$version" "v$FUSE_NODEFS_MIN_NODE_VERSION" <<'PY'
import sys


def vtuple(raw):
    parts = raw.strip().lstrip("v").split(".")[:3]
    out = []
    for part in parts:
        try:
            out.append(int(part))
        except ValueError:
            out.append(0)
    while len(out) < 3:
        out.append(0)
    return tuple(out)


sys.exit(0 if vtuple(sys.argv[1]) >= vtuple(sys.argv[2]) else 1)
PY
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
    x86_64|amd64) CLI_RELEASE_ARCH="amd64" ;;
    aarch64|arm64) CLI_RELEASE_ARCH="arm64" ;;
    *)
      echo "unsupported architecture for official CLI download: $(uname -m)" >&2
      return 1
      ;;
  esac
}

download_official_cli() {
  local target_version="$CLI_RELEASE_VERSION"
  local unversioned_url versioned_url download_url
  detect_release_target || return 1
  unversioned_url="$CLI_RELEASE_BASE_URL/drive9-$CLI_RELEASE_OS-$CLI_RELEASE_ARCH"
  if [ -z "$target_version" ]; then
    target_version=$(curl -fsSL "$CLI_RELEASE_BASE_URL/version" 2>/dev/null | tr -d '[:space:]' || true)
  fi
  download_url="$unversioned_url"
  if [ -n "$target_version" ]; then
    versioned_url="$CLI_RELEASE_BASE_URL/drive9-$target_version-$CLI_RELEASE_OS-$CLI_RELEASE_ARCH"
    if curl -fsI "$versioned_url" >/dev/null 2>&1; then
      download_url="$versioned_url"
    fi
  fi
  curl -fsSL "$download_url" -o "$CLI_BIN"
  chmod +x "$CLI_BIN"
  local actual_version
  actual_version="$($CLI_BIN --version 2>/dev/null | awk '{print $2}')"
  if [ -n "$CLI_RELEASE_VERSION" ] && [ "$actual_version" != "$CLI_RELEASE_VERSION" ]; then
    echo "downloaded version mismatch: expected=$CLI_RELEASE_VERSION actual=$actual_version" >&2
    return 1
  fi
  echo "downloaded official drive9 $actual_version for $CLI_RELEASE_OS/$CLI_RELEASE_ARCH" >&2
}

prepare_cli_binary() {
  CLI_BIN="$(mktemp)"
  case "$CLI_SOURCE" in
    build)
      make build-cli CLI_BIN="$CLI_BIN"
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

is_mounted() {
  local mount_point="$1"
  local physical_mount_point
  physical_mount_point="$(cd "$(dirname "$mount_point")" 2>/dev/null && pwd -P)/$(basename "$mount_point")"
  if command -v mountpoint >/dev/null 2>&1; then
    mountpoint -q "$mount_point"
    return
  fi
  mount | awk -v mp="$mount_point" -v pmp="$physical_mount_point" '{for(i=1;i<=NF;i++) if($i=="on" && ($(i+1)==mp || $(i+1)==pmp)) found=1} END{exit !found}'
}

wait_mount_state() {
  local expect="$1"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  while :; do
    if [ "$expect" = "mounted" ] && is_mounted "$MOUNT_POINT"; then
      return 0
    fi
    if [ "$expect" = "unmounted" ] && ! is_mounted "$MOUNT_POINT"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$MOUNT_READY_INTERVAL_S"
  done
}

start_mount() {
  # --foreground keeps the mount in-process so MOUNT_PID is the real mount
  # process and this script owns the full lifecycle. Without it the CLI
  # detaches a supervised background mount whose heal logic can re-mount
  # after our umount — the cleanup guard then (correctly) refuses to remove
  # the run root and fails the run.
  local mount_args=(mount --mode=fuse --foreground)
  if [ -n "${FUSE_PROFILE:-}" ]; then
    mount_args+=(--profile "$FUSE_PROFILE")
  fi
  mount_args+=("$MOUNT_POINT")
  {
    echo "=== drive9 nodefs mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "root_remote=$ROOT_REMOTE"
    echo "mount_args=${mount_args[*]}"
  } >>"$MOUNT_LOG"
  drive9 "${mount_args[@]}" >>"$MOUNT_LOG" 2>&1 &
  MOUNT_PID="$!"

  if wait_mount_state mounted; then
    return 0
  fi
  cat "$MOUNT_LOG" >&2 || true
  return 1
}

stop_mount() {
  set +e
  if [ -n "${MOUNT_POINT:-}" ] && is_mounted "$MOUNT_POINT"; then
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT" \
      >"${RUN_ROOT:-/tmp}/umount-trap.log" 2>&1 || true
    wait_mount_state unmounted >/dev/null 2>&1 || true
  fi
  if [ -n "${MOUNT_PID:-}" ] && kill -0 "$MOUNT_PID" >/dev/null 2>&1; then
    kill "$MOUNT_PID" >/dev/null 2>&1 || true
    wait "$MOUNT_PID" >/dev/null 2>&1 || true
  fi
  MOUNT_PID=""
  set -e
}

unmount_mount() {
  if is_mounted "$MOUNT_POINT"; then
    if ! drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT"; then
      return 1
    fi
  fi
  if ! wait_mount_state unmounted; then
    return 1
  fi
  if [ -n "${MOUNT_PID:-}" ]; then
    set +e
    wait "$MOUNT_PID" >/dev/null 2>&1
    MOUNT_PID=""
    set -e
  fi
  return 0
}

curl_body_code() {
  local method="$1"
  local url="$2"
  local auth="${3:-}"

  local attempt=1
  while :; do
    local body_file code rc
    body_file="$(mktemp)"
    set +e
    if [ -n "$auth" ]; then
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
      rc=$?
    else
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
      rc=$?
    fi
    set -e
    if [ "$rc" -ne 0 ]; then
      code="000"
    fi

    if { [ "$rc" -eq 0 ] && [ "$code" != "000" ] && [ "$code" != "429" ] && [ "$code" != "403" ] && ! [[ "$code" =~ ^5 ]]; } || [ "$attempt" -ge "$REQUEST_MAX_RETRIES" ]; then
      cat "$body_file"
      echo
      echo "__HTTP__${code}"
      rm -f "$body_file"
      return "$rc"
    fi

    rm -f "$body_file"
    attempt=$((attempt + 1))
    sleep "$REQUEST_RETRY_SLEEP_S"
  done
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

drive9_retry() {
  local attempt=1
  local out rc
  while :; do
    set +e
    out=$(drive9 "$@" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      printf '%s' "$out"
      return 0
    fi
    if [ "$attempt" -lt "$REQUEST_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* || "$out" == *"HTTP 403"* || "$out" == *"403 Forbidden"* ]]; then
      attempt=$((attempt + 1))
      sleep "$REQUEST_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

run_nodefs() {
  # Enforce the workload timeout on hosts without GNU timeout (stock macOS)
  # via a background watchdog; relay the workload's exit status and map
  # watchdog kills to the conventional 124.
  if [ "$FUSE_NODEFS_WORKLOAD_TIMEOUT_S" = "0" ] || command -v timeout >/dev/null 2>&1; then
    local timeout_cmd=()
    if [ "$FUSE_NODEFS_WORKLOAD_TIMEOUT_S" != "0" ]; then
      timeout_cmd=(timeout --kill-after=10s "${FUSE_NODEFS_WORKLOAD_TIMEOUT_S}s")
    fi
    # ${arr[@]+...} keeps empty-array expansion legal under `set -u` on bash 3.2
    # (macOS) where "${arr[@]}" on an empty array is an unbound-variable error.
    ${timeout_cmd[@]+"${timeout_cmd[@]}"} node "$NODEFS_JS" "$@"
    return $?
  fi
  node "$NODEFS_JS" "$@" &
  local workload_pid=$!
  (
    sleep "${FUSE_NODEFS_WORKLOAD_TIMEOUT_S}"
    kill -TERM "$workload_pid" 2>/dev/null || true
    sleep 10
    kill -KILL "$workload_pid" 2>/dev/null || true
  ) &
  local watchdog_pid=$!
  set +e
  wait "$workload_pid"
  local rc=$?
  set -e
  kill -TERM "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  if [ "$rc" -eq 143 ] || [ "$rc" -eq 137 ]; then
    echo "nodefs workload exceeded ${FUSE_NODEFS_WORKLOAD_TIMEOUT_S}s (watchdog)" >&2
    return 124
  fi
  return "$rc"
}

# Auto durability defers the remote commit of a mounted write, and FUSE
# directory caches can delay a CLI upload from appearing on the mount. Both
# cross-channel directions therefore poll bounded instead of asserting
# one-shot.
wait_remote_content_sha() {
  local remote="$1" want_sha="$2"
  local deadline=$(( $(date +%s) + FUSE_NODEFS_CROSS_TIMEOUT_S ))
  local body_file
  body_file="$(mktemp)"
  while :; do
    set +e
    drive9 fs cat ":$remote" > "$body_file" 2>/dev/null
    local rc=$?
    set -e
    if [ "$rc" -eq 0 ] && [ "$(sha256_file "$body_file")" = "$want_sha" ]; then
      rm -f "$body_file"
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    sleep "$MOUNT_READY_INTERVAL_S"
  done
  rm -f "$body_file"
  return 1
}

wait_mount_probe_sha() {
  local target="$1" want_sha="$2"
  local deadline=$(( $(date +%s) + FUSE_NODEFS_CROSS_TIMEOUT_S ))
  while :; do
    if run_nodefs probe "$target" 2>/dev/null | head -n1 | grep -qF "$want_sha"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$MOUNT_READY_INTERVAL_S"
  done
}

echo "=== drive9 FUSE Node.js fs smoke ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "FUSE_STRICT_PREREQS=$FUSE_STRICT_PREREQS"
echo "FUSE_NODEFS_LARGE_MB=$FUSE_NODEFS_LARGE_MB"
echo "FUSE_NODEFS_MIN_NODE_VERSION=$FUSE_NODEFS_MIN_NODE_VERSION"

# The owner key is a credential: never send it in cleartext. http:// is allowed
# only for loopback (single-machine dev); every other target must be https://.
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

if ! [[ "$FUSE_NODEFS_LARGE_MB" =~ ^[0-9]+$ ]] || [ "$FUSE_NODEFS_LARGE_MB" -lt 9 ]; then
  echo "invalid FUSE_NODEFS_LARGE_MB: must be >= 9 (crosses the 8MiB storage tier boundary)" >&2
  exit 1
fi

sha256_file() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    sha256sum "$1" | awk '{print $1}'
  fi
}

sha256_stdin() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
  else
    sha256sum | awk '{print $1}'
  fi
}

require_cmd curl
require_cmd jq
if ! command -v shasum >/dev/null 2>&1 && ! command -v sha256sum >/dev/null 2>&1; then
  echo "FAIL shasum/sha256sum is available" >&2
  exit 1
fi
TOTAL=$((TOTAL + 1))
echo "PASS checksum tool is available"
PASS=$((PASS + 1))
if [ "$CLI_SOURCE" = "build" ]; then
  require_cmd go
fi

if [ "$(uname -s)" != "Linux" ] && [ "$(uname -s)" != "Darwin" ]; then
  skip_or_fail "unsupported OS for this workload"
fi

if ! command -v node >/dev/null 2>&1; then
  skip_or_fail "node >= $FUSE_NODEFS_MIN_NODE_VERSION is required (pack-smoke already requires node on CI runners)"
fi
if ! node_version_ok; then
  skip_or_fail "node $(node --version 2>/dev/null) is too old; need >= $FUSE_NODEFS_MIN_NODE_VERSION (readdir recursive / statfs)"
fi

if [ "$(uname -s)" = "Linux" ]; then
  if ! command -v fusermount >/dev/null 2>&1 && ! command -v fusermount3 >/dev/null 2>&1; then
    skip_or_fail "fusermount/fusermount3 is required for Linux FUSE unmount"
  fi
  if [ ! -e /dev/fuse ]; then
    skip_or_fail "/dev/fuse not available"
  fi
fi

echo "[1] provision tenant"
if [ -n "$DRIVE9_API_KEY" ]; then
  API_KEY="$DRIVE9_API_KEY"
  check_eq "use provided DRIVE9_API_KEY" "true" "true"
else
  resp=$(curl_body_code POST "$BASE/v1/provision")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "POST /v1/provision returns 202" "$code" "202"
  API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
  check_cmd "provision returns api_key" test -n "$API_KEY"
fi

echo "[2] wait tenant active"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
state=""
while :; do
  sresp=$(curl_body_code GET "$BASE/v1/status" "$API_KEY")
  scode=$(http_code "$sresp")
  sbody=$(json_body "$sresp")
  state=$(printf '%s' "$sbody" | jq -r '.status // empty')
  echo "status=${scode}:${state}"
  if [ "$scode" = "200" ] && [ "$state" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant becomes active" "$state" "active"

echo "[3] prepare drive9 cli"
prepare_cli_binary
check_cmd "drive9 binary ready" test -x "$CLI_BIN"

drive9() {
  DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

TS="$(date +%s)"
RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-fuse-nodefs-${TS}.XXXXXX")"
RUN_ID="$(basename "$RUN_ROOT")"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
MANIFEST_JSON="$RUN_ROOT/manifest.json"
CLI_FIXTURE="$RUN_ROOT/cli-fixture.bin"
ROOT_REL="$RUN_ID"
ROOT_REMOTE="/$ROOT_REL"
WORK_MOUNT="$MOUNT_POINT/$ROOT_REL/nodefs"
WORK_REMOTE="$ROOT_REMOTE/nodefs"
MOUNT_PID=""

mkdir -p "$MOUNT_POINT"
: > "$MOUNT_LOG"

cleanup() {
  local rc=$?
  stop_mount
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  # The mount is the tenant root, so a recursive delete while it is still
  # mounted would traverse into the remote workspace beyond this fixture.
  # Never remove the run root unless the mountpoint is verified gone; a
  # failed unmount preserves artifacts and fails the run even if every
  # assertion passed.
  if [ -n "${MOUNT_POINT:-}" ] && is_mounted "$MOUNT_POINT"; then
    echo "ERROR: $MOUNT_POINT is still mounted after cleanup; refusing to remove $RUN_ROOT" >&2
    echo "--- diagnostics ---" >&2
    echo "mountpoint probe:" >&2
    mountpoint "$MOUNT_POINT" >&2 2>&1 || true
    echo "mount table entries:" >&2
    mount | grep -F "$MOUNT_POINT" >&2 || true
    echo "live drive9 mount processes:" >&2
    ps -eo pid,ppid,etime,args | grep '[d]rive9 mount' >&2 || echo "(none)" >&2
    echo "all live drive9 processes:" >&2
    ps -eo pid,ppid,etime,args | grep '[d]rive9' >&2 || echo "(none)" >&2
    echo "trap umount output:" >&2
    cat "${RUN_ROOT:-/dev/null}/umount-trap.log" >&2 2>/dev/null || echo "(none)" >&2
    echo "mount log tail:" >&2
    tail -n 40 "$MOUNT_LOG" >&2 2>/dev/null || true
    echo "--- end diagnostics ---" >&2
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount log: $MOUNT_LOG"
    echo "Node fs manifest: $MANIFEST_JSON"
    exit 1
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$FUSE_NODEFS_KEEP_ARTIFACTS" != "1" ]; then
    rm -rf "$RUN_ROOT"
  else
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount log: $MOUNT_LOG"
    echo "Node fs manifest: $MANIFEST_JSON"
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "[4] create remote root"
drive9_retry fs mkdir "$ROOT_REMOTE" >/dev/null
check_eq "remote nodefs root" "$ROOT_REMOTE" "$ROOT_REMOTE"

echo "[5] mount writable namespace"
if start_mount; then
  check_eq "nodefs mount is mounted" "true" "true"
  if ls "$MOUNT_POINT" >/dev/null 2>&1; then
    check_eq "mount root ls precheck" "true" "true"
  else
    skip_or_fail "mount root ls precheck failed"
  fi
else
  check_eq "nodefs mount is mounted" "false" "true"
fi

if is_mounted "$MOUNT_POINT"; then
  echo "[6] run mounted Node fs workload"
  nodefs_ok=0
  if FUSE_NODEFS_LARGE_BYTES=$((FUSE_NODEFS_LARGE_MB * 1024 * 1024)) run_nodefs create "$WORK_MOUNT" "$MANIFEST_JSON"; then
    check_eq "mounted Node fs workload passes all fs API checks" "true" "true"
    nodefs_ok=1
  else
    check_eq "mounted Node fs workload passes all fs API checks" "false" "true"
  fi

  if [ "$nodefs_ok" = "1" ]; then
    echo "[7] cross-channel consistency (CLI <-> mounted Node)"
    cross_remote="$WORK_REMOTE/cross-channel/node-written.txt"
    expected_cross_sha=$(jq -j '.cross_content' "$MANIFEST_JSON" | sha256_stdin)
    if wait_remote_content_sha "$cross_remote" "$expected_cross_sha"; then
      check_eq "CLI reads Node-written file from mount (bounded poll)" "true" "true"
    else
      check_eq "CLI reads Node-written file from mount (bounded poll)" "false" "true"
    fi

    head -c $((1024 * 1024)) /dev/urandom > "$CLI_FIXTURE"
    fixture_sha=$(sha256_file "$CLI_FIXTURE")
    drive9_retry fs cp "$CLI_FIXTURE" ":$WORK_REMOTE/cli-written.bin" >/dev/null
    if wait_mount_probe_sha "$WORK_MOUNT/cli-written.bin" "$fixture_sha"; then
      check_eq "mounted Node sees CLI-written file with matching checksum (bounded poll)" "true" "true"
    else
      check_eq "mounted Node sees CLI-written file with matching checksum (bounded poll)" "false" "true"
    fi
    rm -f "$WORK_MOUNT/cli-written.bin"

    echo "[8] unmount and remount Node fs workload"
    if unmount_mount; then
      check_eq "unmount nodefs mount" "true" "true"
      MOUNT_PID=""
      if start_mount; then
        check_eq "nodefs mount remounted" "true" "true"
        if run_nodefs verify "$WORK_MOUNT" "$MANIFEST_JSON"; then
          check_eq "remounted Node fs tree matches manifest" "true" "true"
        else
          check_eq "remounted Node fs tree matches manifest" "false" "true"
        fi
      else
        check_eq "nodefs mount remounted" "false" "true"
      fi
    else
      check_eq "unmount nodefs mount" "false" "true"
    fi
  fi
fi

echo "[9] unmount before remote cleanup"
# The remote fixture must only be deleted after the mount is gone: rm -r on
# the backing tree under a live mount makes the subsequent umount fail (and
# the cleanup trap then correctly refuses to remove the run root).
if is_mounted "$MOUNT_POINT"; then
  if unmount_mount; then
    check_eq "final unmount before remote cleanup" "true" "true"
  else
    check_eq "final unmount before remote cleanup" "false" "true"
  fi
fi

echo "[10] cleanup remote fixture"
if ! drive9_retry fs rm -r "$ROOT_REMOTE" >/dev/null 2>&1; then
  echo "WARN: remote cleanup failed for $ROOT_REMOTE; a leftover test tree may remain in the tenant" >&2
fi

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
