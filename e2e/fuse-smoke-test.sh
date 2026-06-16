#!/usr/bin/env bash
# drive9 FUSE smoke test against a live deployment.
# shellcheck disable=SC2317,SC2016

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-20}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
REMOTE_VISIBILITY_TIMEOUT_S="${REMOTE_VISIBILITY_TIMEOUT_S:-5}"
REMOTE_VISIBILITY_INTERVAL_S="${REMOTE_VISIBILITY_INTERVAL_S:-0.2}"
LARGE_FILE_VISIBILITY_TIMEOUT_S="${LARGE_FILE_VISIBILITY_TIMEOUT_S:-30}"
LARGE_FILE_VISIBILITY_INTERVAL_S="${LARGE_FILE_VISIBILITY_INTERVAL_S:-1}"
FUSE_MOUNT_ROOT="${FUSE_MOUNT_ROOT:-/tmp}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-8}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-2}"
FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-0}"
FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-60s}"
RUN_FUSE_GIT_CLONE="${RUN_FUSE_GIT_CLONE:-0}"
RUN_FUSE_UMOUNT_DURABLE="${RUN_FUSE_UMOUNT_DURABLE:-0}"
RUN_FUSE_LOG_AUDIT="${RUN_FUSE_LOG_AUDIT:-0}"
FUSE_GIT_CLONE_URL="${FUSE_GIT_CLONE_URL:-https://github.com/octocat/Hello-World.git}"
FUSE_GIT_CLONE_TIMEOUT_S="${FUSE_GIT_CLONE_TIMEOUT_S:-180}"
FUSE_LOG_AUDIT_PATTERN="${FUSE_LOG_AUDIT_PATTERN:-panic|fatal error|Resource temporarily unavailable}"

PASS=0
FAIL=0
TOTAL=0

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
  detect_release_target || return 1
  if [ -z "$target_version" ]; then
    target_version=$(curl -fsSL "$CLI_RELEASE_BASE_URL/version" | tr -d '[:space:]')
  fi
  if [ -z "$target_version" ]; then
    echo "failed to resolve release version from $CLI_RELEASE_BASE_URL/version" >&2
    return 1
  fi
  curl -fsSL "$CLI_RELEASE_BASE_URL/drive9-$CLI_RELEASE_OS-$CLI_RELEASE_ARCH" -o "$CLI_BIN"
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

is_mounted() {
  local mount_point="$1"
  local physical_mount_point
  physical_mount_point="$(cd "$(dirname "$mount_point")" 2>/dev/null && pwd -P)/$(basename "$mount_point")"
  if command -v mountpoint >/dev/null 2>&1; then
    mountpoint -q "$mount_point"
    return
  fi
  # Fallback for macOS and systems without mountpoint or /proc/mounts.
  mount | awk -v mp="$mount_point" -v pmp="$physical_mount_point" '{for(i=1;i<=NF;i++) if($i=="on" && ($(i+1)==mp || $(i+1)==pmp)) found=1} END{exit !found}'
}

run_with_timeout() {
  local seconds="$1"
  shift
  "$@" &
  local cmd_pid=$!
  (
    sleep "$seconds"
    kill "$cmd_pid" >/dev/null 2>&1 || true
  ) &
  local watchdog_pid=$!
  wait "$cmd_pid"
  local rc=$?
  kill "$watchdog_pid" >/dev/null 2>&1 || true
  wait "$watchdog_pid" 2>/dev/null || true
  return "$rc"
}

curl_body_code() {
  local method="$1"
  local url="$2"
  local auth="${3:-}"

  local attempt=1
  while :; do
    local body_file
    body_file="$(mktemp)"
    local code
    if [ -n "$auth" ]; then
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
    else
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
    fi

    if { [ "$code" != "429" ] && [ "$code" != "403" ]; } || [ "$attempt" -ge "$CLI_MAX_RETRIES" ]; then
      cat "$body_file"
      echo
      echo "__HTTP__${code}"
      rm -f "$body_file"
      return
    fi

    rm -f "$body_file"
    attempt=$((attempt + 1))
    sleep "$CLI_RETRY_SLEEP_S"
  done
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

stat_field() {
  local path="$1"
  local field="$2"
  local out
  out=$(drive9_retry fs stat "$path")
  python3 - "$out" "$field" <<'PY'
import sys
text, field = sys.argv[1], sys.argv[2].lower()
for line in text.splitlines():
    if line.strip().lower().startswith(field + ":"):
        print(line.split(":", 1)[1].strip())
        raise SystemExit(0)
raise SystemExit(1)
PY
}

wait_remote_stat_field_eq() {
  local path="$1"
  local field="$2"
  local want="$3"
  local timeout_s="${4:-$REMOTE_VISIBILITY_TIMEOUT_S}"
  local interval_s="${5:-$REMOTE_VISIBILITY_INTERVAL_S}"
  local deadline
  local out rc got
  deadline=$(python3 - "$timeout_s" <<'PY'
import sys
import time
print(time.time() + float(sys.argv[1]))
PY
)

  while :; do
    set +e
    out=$(drive9 fs stat "$path" 2>&1)
    rc=$?
    set -e

    if [ "$rc" -eq 0 ]; then
      got=$(python3 - "$out" "$field" <<'PY'
import sys
text, field = sys.argv[1], sys.argv[2].lower()
for line in text.splitlines():
    if line.strip().lower().startswith(field + ":"):
        print(line.split(":", 1)[1].strip())
        raise SystemExit(0)
raise SystemExit(1)
PY
      ) || got=""
      if [ "$got" = "$want" ]; then
        printf '%s' "$got"
        return 0
      fi
    elif [[ "$out" != *"not found"* && "$out" != *"Too Many Requests"* && "$out" != *"HTTP 429"* && "$out" != *"HTTP 403"* && "$out" != *"403 Forbidden"* ]]; then
      printf '%s\n' "$out" >&2
      return 1
    fi

    if python3 - "$deadline" <<'PY'
import sys
import time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      printf 'wait_remote_stat_field_eq: timeout path=%s field=%s want=%s last_got=%s\n' \
        "$path" "$field" "$want" "${got:-<none>}" >&2
      return 1
    fi

    sleep "$interval_s"
  done
}

local_size_mtime() {
  local file_path="$1"
  python3 - "$file_path" <<'PY'
import os
import sys
st = os.stat(sys.argv[1])
print(f"{st.st_size}:{int(st.st_mtime)}")
PY
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
import hashlib
import sys
h = hashlib.sha256()
with open(sys.argv[1], 'rb') as f:
    while True:
        chunk = f.read(1024 * 1024)
        if not chunk:
            break
        h.update(chunk)
print(h.hexdigest())
PY
}

wait_remote_file_hash_eq() {
  local remote_path="$1"
  local want_hash="$2"
  local local_path="$3"
  local timeout_s="${4:-$REMOTE_VISIBILITY_TIMEOUT_S}"
  local interval_s="${5:-$REMOTE_VISIBILITY_INTERVAL_S}"
  local deadline
  local out rc got_hash
  deadline=$(python3 - "$timeout_s" <<'PY'
import sys
import time
print(time.time() + float(sys.argv[1]))
PY
)

  while :; do
    rm -f "$local_path"
    set +e
    out=$(drive9 fs cp ":$remote_path" "$local_path" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ] && [ -f "$local_path" ]; then
      got_hash=$(sha256_file "$local_path")
      if [ "$got_hash" = "$want_hash" ]; then
        printf '%s' "$got_hash"
        return 0
      fi
    elif [[ "$out" != *"not found"* && "$out" != *"Too Many Requests"* && "$out" != *"HTTP 429"* && "$out" != *"HTTP 403"* && "$out" != *"403 Forbidden"* ]]; then
      printf '%s\n' "$out" >&2
      return 1
    fi

    if python3 - "$deadline" <<'PY'
import sys
import time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      printf 'wait_remote_file_hash_eq: timeout path=%s want_hash=%s last_hash=%s\n' \
        "$remote_path" "$want_hash" "${got_hash:-<none>}" >&2
      return 1
    fi

    sleep "$interval_s"
  done
}

write_pattern_file() {
  local file_path="$1"
  local size="$2"
  local seed="$3"
  python3 - "$file_path" "$size" "$seed" <<'PY'
import sys

path = sys.argv[1]
size = int(sys.argv[2])
seed = int(sys.argv[3])
chunk = bytearray(1024 * 1024)
remaining = size
offset = 0
with open(path, "wb") as handle:
    while remaining > 0:
        n = min(len(chunk), remaining)
        for idx in range(n):
            chunk[idx] = (offset + idx * 31 + seed) % 251
        handle.write(chunk[:n])
        offset += n
        remaining -= n
PY
}

path_truncate_zero() {
  local file_path="$1"
  python3 - "$file_path" <<'PY'
import os
import sys
os.truncate(sys.argv[1], 0)
PY
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

wait_path_exists() {
  local path="$1"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  while :; do
    if [ -e "$path" ]; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$MOUNT_READY_INTERVAL_S"
  done
}

wait_remote_ls_has_name() {
  local parent="$1"
  local name="$2"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  local out rc
  while :; do
    set +e
    out=$(drive9 fs ls "$parent" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      if python3 - "$out" "$name" <<'PY'
import sys
lines=[ln.strip() for ln in sys.argv[1].splitlines() if ln.strip()]
name=sys.argv[2]
raise SystemExit(0 if any(line == name or line == name + "/" for line in lines) else 1)
PY
      then
        return 0
      fi
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    if [[ "$out" == *"not found"* || "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* || "$out" == *"HTTP 403"* || "$out" == *"403 Forbidden"* ]]; then
      sleep "$MOUNT_READY_INTERVAL_S"
      continue
    fi
    echo "$out" >&2
    return 1
  done
}

wait_remote_ls_missing_name() {
  local parent="$1"
  local name="$2"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  local out rc
  while :; do
    set +e
    out=$(drive9 fs ls "$parent" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      if python3 - "$out" "$name" <<'PY'
import sys
lines=[ln.strip() for ln in sys.argv[1].splitlines() if ln.strip()]
name=sys.argv[2]
raise SystemExit(0 if all(line != name and line != name + "/" for line in lines) else 1)
PY
      then
        return 0
      fi
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    if [[ "$out" == *"not found"* || "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* || "$out" == *"HTTP 403"* || "$out" == *"403 Forbidden"* ]]; then
      sleep "$MOUNT_READY_INTERVAL_S"
      continue
    fi
    echo "$out" >&2
    return 1
  done
}

assert_remote_ls_missing_stable() {
  local parent="$1"
  local name="$2"
  local stable_s="${3:-3}"
  local interval_s="${4:-0.25}"
  local deadline
  local out rc

  wait_remote_ls_missing_name "$parent" "$name" || return 1
  deadline=$(python3 - "$stable_s" <<'PY'
import sys
import time
print(time.time() + float(sys.argv[1]))
PY
)

  while :; do
    set +e
    out=$(drive9 fs ls "$parent" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      if python3 - "$out" "$name" <<'PY'
import sys
lines=[ln.strip() for ln in sys.argv[1].splitlines() if ln.strip()]
name=sys.argv[2]
raise SystemExit(0 if all(line != name and line != name + "/" for line in lines) else 1)
PY
      then
        :
      else
        printf 'assert_remote_ls_missing_stable: %s reappeared under %s\n' "$name" "$parent" >&2
        return 1
      fi
    elif [[ "$out" != *"not found"* && "$out" != *"Too Many Requests"* && "$out" != *"HTTP 429"* && "$out" != *"HTTP 403"* && "$out" != *"403 Forbidden"* ]]; then
      printf '%s\n' "$out" >&2
      return 1
    fi

    if python3 - "$deadline" <<'PY'
import sys
import time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      return 0
    fi
    sleep "$interval_s"
  done
}

wait_remote_cat_eq() {
  local path="$1"
  local want="$2"
  local deadline
  local out rc
  deadline=$(python3 - "$REMOTE_VISIBILITY_TIMEOUT_S" <<'PY'
import sys
import time
print(time.time() + float(sys.argv[1]))
PY
)

  while :; do
    set +e
    out=$(drive9 fs cat "$path" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      if [ "$out" = "$want" ]; then
        printf '%s' "$out"
        return 0
      fi
    elif [[ "$out" != *"not found"* && "$out" != *"Too Many Requests"* && "$out" != *"HTTP 429"* && "$out" != *"HTTP 403"* && "$out" != *"403 Forbidden"* ]]; then
      printf '%s\n' "$out" >&2
      return 1
    fi

    if python3 - "$deadline" <<'PY'
import sys
import time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      if [ "$rc" -eq 0 ]; then
        printf '%s\n' "$out" >&2
      fi
      return 1
    fi

    sleep "$REMOTE_VISIBILITY_INTERVAL_S"
  done
}

start_mount() {
  local mode="$1"
  {
    echo "=== drive9 mount start mode=$mode time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
  } >>"$MOUNT_LOG"
  if [ "$mode" = "ro" ]; then
    drive9 mount --read-only "$MOUNT_POINT" >>"$MOUNT_LOG" 2>&1 &
  else
    drive9 mount "$MOUNT_POINT" >>"$MOUNT_LOG" 2>&1 &
  fi
  MOUNT_PID="$!"

  if wait_mount_state mounted; then
    return 0
  fi
  if [ -f "$MOUNT_LOG" ]; then
    echo "mount log:"
    cat "$MOUNT_LOG"
  fi
  return 1
}

stop_mount() {
  set +e
  if is_mounted "$MOUNT_POINT"; then
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT" >/dev/null 2>&1 || true
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

dump_mount_log() {
  if [ -n "${MOUNT_LOG:-}" ] && [ -f "$MOUNT_LOG" ]; then
    echo "=== drive9 mount log: $MOUNT_LOG ==="
    cat "$MOUNT_LOG"
  fi
}

audit_mount_log() {
  if [ ! -f "$MOUNT_LOG" ]; then
    echo "mount log missing: $MOUNT_LOG" >&2
    return 1
  fi
  if grep -Eina "$FUSE_LOG_AUDIT_PATTERN" "$MOUNT_LOG"; then
    echo "mount log contains release-gate failure pattern" >&2
    return 1
  fi
  return 0
}

echo "=== drive9 FUSE smoke test ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "FUSE_STRICT_PREREQS=$FUSE_STRICT_PREREQS"
echo "RUN_FUSE_GIT_CLONE=$RUN_FUSE_GIT_CLONE"
echo "FUSE_GIT_CLONE_TIMEOUT_S=$FUSE_GIT_CLONE_TIMEOUT_S"
echo "RUN_FUSE_UMOUNT_DURABLE=$RUN_FUSE_UMOUNT_DURABLE"
echo "RUN_FUSE_LOG_AUDIT=$RUN_FUSE_LOG_AUDIT"
echo "FUSE_LOG_AUDIT_PATTERN=$FUSE_LOG_AUDIT_PATTERN"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
else
  check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
fi
check_cmd "python3 is available" bash -c 'command -v python3 >/dev/null'
check_cmd "git is available" bash -c 'command -v git >/dev/null'
if [ "$FUSE_STRICT_PREREQS" = "1" ] && [ "$RUN_FUSE_GIT_CLONE" = "1" ]; then
  if ! command -v timeout >/dev/null 2>&1; then
    skip_or_fail "timeout is required for strict FUSE git clone timeout"
  fi
  check_eq "timeout is available" "true" "true"
fi

if [ "$(uname -s)" != "Linux" ] && [ "$(uname -s)" != "Darwin" ]; then
  skip_or_fail "unsupported OS for this smoke script"
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
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* || "$out" == *"HTTP 403"* || "$out" == *"403 Forbidden"* ]]; then
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

echo "[3.1] mount compatibility precheck"
TS="$(date +%s)"
ROOT_REL="fuse-e2e-${TS}"
ROOT_REMOTE="/${ROOT_REL}"
ROOT_MOUNT="$FUSE_MOUNT_ROOT/drive9-fuse-smoke-${TS}"
MOUNT_POINT="$ROOT_MOUNT"
MOUNT_LOG="$FUSE_MOUNT_ROOT/drive9-fuse-smoke-${TS}.log"
SEED_LOCAL="$FUSE_MOUNT_ROOT/drive9-fuse-seed-${TS}.txt"
LARGE_DOWNLOADED="$FUSE_MOUNT_ROOT/drive9-fuse-large-down-${TS}.bin"
TIER_DOWNLOADED="$FUSE_MOUNT_ROOT/drive9-fuse-tier-down-${TS}.bin"

set +e
ls_out=$(drive9 fs ls / 2>&1)
ls_rc=$?
set -e
if [ "$ls_rc" -eq 0 ]; then
  check_eq "remote root list precheck is supported" "true" "true"
else
  echo "$ls_out" >&2
  check_eq "remote root list precheck is supported" "false" "true"
fi

RW_ALPHA_REL="${ROOT_REL}/alpha"
RW_ALPHA_REMOTE="/${RW_ALPHA_REL}"
RW_ALPHA_MOUNT="$MOUNT_POINT/$RW_ALPHA_REL"
RW_BETA_MOUNT="$RW_ALPHA_MOUNT/beta"
RW_TEXT_REL="${RW_ALPHA_REL}/text.txt"
RW_TEXT_REMOTE="/${RW_TEXT_REL}"
RW_TEXT_MOUNT="$MOUNT_POINT/$RW_TEXT_REL"
RW_SYMLINK_REL="${RW_ALPHA_REL}/text-link"
RW_SYMLINK_REMOTE="/${RW_SYMLINK_REL}"
RW_SYMLINK_MOUNT="$MOUNT_POINT/$RW_SYMLINK_REL"
RW_HARDLINK_REL="${RW_ALPHA_REL}/text-hardlink.txt"
RW_HARDLINK_REMOTE="/${RW_HARDLINK_REL}"
RW_HARDLINK_MOUNT="$MOUNT_POINT/$RW_HARDLINK_REL"
RW_TEXT_RENAMED_REL="${RW_ALPHA_REL}/text-renamed.txt"
RW_TEXT_RENAMED_REMOTE="/${RW_TEXT_RENAMED_REL}"
RW_TEXT_RENAMED_MOUNT="$MOUNT_POINT/$RW_TEXT_RENAMED_REL"
RW_PATH_TRUNC_REL="${RW_ALPHA_REL}/path-truncate.txt"
RW_PATH_TRUNC_REMOTE="/${RW_PATH_TRUNC_REL}"
RW_PATH_TRUNC_MOUNT="$MOUNT_POINT/$RW_PATH_TRUNC_REL"
RW_PATH_TRUNC_RENAME_SRC_REL="${RW_ALPHA_REL}/path-truncate-rename-src.txt"
RW_PATH_TRUNC_RENAME_SRC_REMOTE="/${RW_PATH_TRUNC_RENAME_SRC_REL}"
RW_PATH_TRUNC_RENAME_SRC_MOUNT="$MOUNT_POINT/$RW_PATH_TRUNC_RENAME_SRC_REL"
RW_PATH_TRUNC_RENAME_DST_REL="${RW_ALPHA_REL}/path-truncate-rename-dst.txt"
RW_PATH_TRUNC_RENAME_DST_REMOTE="/${RW_PATH_TRUNC_RENAME_DST_REL}"
RW_PATH_TRUNC_RENAME_DST_MOUNT="$MOUNT_POINT/$RW_PATH_TRUNC_RENAME_DST_REL"
RW_PATH_TRUNC_UNLINK_REL="${RW_ALPHA_REL}/path-truncate-unlink.txt"
RW_PATH_TRUNC_UNLINK_REMOTE="/${RW_PATH_TRUNC_UNLINK_REL}"
RW_PATH_TRUNC_UNLINK_MOUNT="$MOUNT_POINT/$RW_PATH_TRUNC_UNLINK_REL"
RW_ATTR_REL="${RW_ALPHA_REL}/attr.txt"
RW_ATTR_REMOTE="/${RW_ATTR_REL}"
RW_ATTR_MOUNT="$MOUNT_POINT/$RW_ATTR_REL"
RW_ALPHA_RENAMED_REL="${ROOT_REL}/alpha-renamed"
RW_ALPHA_RENAMED_REMOTE="/${RW_ALPHA_RENAMED_REL}"
RW_ALPHA_RENAMED_MOUNT="$MOUNT_POINT/$RW_ALPHA_RENAMED_REL"
RW_PATH_TRUNC_AFTER_RENAME_MOUNT="$MOUNT_POINT/${RW_ALPHA_RENAMED_REL}/path-truncate.txt"
CLI_TO_MOUNT_REL="${RW_ALPHA_RENAMED_REL}/from-cli.txt"
CLI_TO_MOUNT_REMOTE="/${CLI_TO_MOUNT_REL}"
CLI_TO_MOUNT_MOUNT="$MOUNT_POINT/$CLI_TO_MOUNT_REL"
MOUNT_TO_CLI_REL="${RW_ALPHA_RENAMED_REL}/from-mount.txt"
MOUNT_TO_CLI_REMOTE="/${MOUNT_TO_CLI_REL}"
MOUNT_TO_CLI_MOUNT="$MOUNT_POINT/$MOUNT_TO_CLI_REL"
LARGE_REL="${RW_ALPHA_RENAMED_REL}/large-8m.bin"
LARGE_REMOTE="/${LARGE_REL}"
LARGE_MOUNT="$MOUNT_POINT/$LARGE_REL"
TIER_REL="${RW_ALPHA_RENAMED_REL}/tier-transition.bin"
TIER_REMOTE="/${TIER_REL}"
TIER_MOUNT="$MOUNT_POINT/$TIER_REL"
GIT_PROBE_REL="${ROOT_REL}/git-config-probe"
GIT_PROBE_MOUNT="$MOUNT_POINT/$GIT_PROBE_REL"
GIT_PROBE_ORIGIN="https://github.com/mem9-ai/drive9.git"
GIT_CLONE_REL="${ROOT_REL}/hello-world"
GIT_CLONE_MOUNT="$MOUNT_POINT/$GIT_CLONE_REL"
DURABLE_REL="${ROOT_REL}/umount-durable.txt"
DURABLE_REMOTE="/${DURABLE_REL}"
DURABLE_MOUNT="$MOUNT_POINT/$DURABLE_REL"
RO_SEED_REL="${ROOT_REL}/ro-seed.txt"
RO_SEED_REMOTE="/${RO_SEED_REL}"
RO_SEED_MOUNT="$MOUNT_POINT/$RO_SEED_REL"
RO_WRITE_MOUNT="$MOUNT_POINT/${ROOT_REL}/ro-write.txt"

mkdir -p "$MOUNT_POINT"
: >"$MOUNT_LOG"
printf "seed-%s" "$TS" > "$SEED_LOCAL"

MOUNT_PID=""
cleanup() {
  stop_mount
  rm -f "$SEED_LOCAL" "$LARGE_DOWNLOADED" "$TIER_DOWNLOADED" "$CLI_BIN"
  rm -rf "${MOUNT_POINT:?}" || true
}
on_exit() {
  local rc=$?
  if [ "$rc" -ne 0 ] || [ "${FAIL:-0}" -ne 0 ]; then
    dump_mount_log
  fi
  cleanup
  exit "$rc"
}
trap on_exit EXIT

echo "[4] mount rw"
if start_mount rw; then
  check_eq "mount point is mounted" "true" "true"
else
  check_eq "mount point is mounted" "false" "true"
fi

if is_mounted "$MOUNT_POINT"; then
  echo "[5] file and directory semantics"
  if mkdir -p "$RW_BETA_MOUNT"; then
    check_eq "mkdir -p nested directory via mount" "true" "true"
  else
    check_eq "mkdir -p nested directory via mount" "false" "true"
    drive9_retry fs mkdir "$RW_ALPHA_REMOTE" >/dev/null 2>&1 || true
    drive9_retry fs mkdir "$RW_ALPHA_REMOTE/beta" >/dev/null 2>&1 || true
  fi
  check_cmd "nested directory visible in mount" wait_path_exists "$RW_BETA_MOUNT"
  check_cmd "nested directory visible in remote list" wait_remote_ls_has_name "$RW_ALPHA_REMOTE" "beta"

  if ! wait_path_exists "$RW_ALPHA_MOUNT"; then
    check_eq "mounted alpha directory is available for write" "false" "true"
  else

  # Mount writes become locally durable first. Remote visibility may lag
  # briefly because Release can hand the upload off to the async commit path.
  printf "create-%s" "$TS" > "$RW_TEXT_MOUNT"
  mounted_text=$(cat "$RW_TEXT_MOUNT")
  remote_text=$(wait_remote_cat_eq "$RW_TEXT_REMOTE" "create-${TS}")
  check_eq "create/read via mount" "$mounted_text" "create-${TS}"
  check_eq "create visible via remote cat" "$remote_text" "create-${TS}"

  printf "overwrite-%s" "$TS" > "$RW_TEXT_MOUNT"
  remote_overwrite=$(wait_remote_cat_eq "$RW_TEXT_REMOTE" "overwrite-${TS}")
  check_eq "overwrite visible via remote cat" "$remote_overwrite" "overwrite-${TS}"

  printf -- "-append" >> "$RW_TEXT_MOUNT"
  remote_append=$(wait_remote_cat_eq "$RW_TEXT_REMOTE" "overwrite-${TS}-append")
  check_eq "append visible via remote cat" "$remote_append" "overwrite-${TS}-append"

  if ln -s "text.txt" "$RW_SYMLINK_MOUNT"; then
    check_eq "symlink via mount succeeds" "true" "true"
  else
    check_eq "symlink via mount succeeds" "false" "true"
  fi
  check_cmd "symlink is visible as local link" test -L "$RW_SYMLINK_MOUNT"
  if [ -L "$RW_SYMLINK_MOUNT" ]; then
    link_target=$(readlink "$RW_SYMLINK_MOUNT")
    symlink_cat=$(cat "$RW_SYMLINK_MOUNT")
    remote_link_target=$(wait_remote_cat_eq "$RW_SYMLINK_REMOTE" "text.txt")
  else
    link_target=""
    symlink_cat=""
    remote_link_target=""
  fi
  check_eq "readlink returns symlink target" "$link_target" "text.txt"
  check_eq "cat follows mounted symlink" "$symlink_cat" "overwrite-${TS}-append"
  check_eq "remote cat returns symlink payload" "$remote_link_target" "text.txt"

  if ln "$RW_TEXT_MOUNT" "$RW_HARDLINK_MOUNT"; then
    check_eq "hardlink via mount succeeds" "true" "true"
  else
    check_eq "hardlink via mount succeeds" "false" "true"
  fi
  check_cmd "hardlink is visible as local file" test -f "$RW_HARDLINK_MOUNT"
  if [ -f "$RW_HARDLINK_MOUNT" ]; then
    hardlink_cat=$(cat "$RW_HARDLINK_MOUNT")
  else
    hardlink_cat=""
  fi
  check_eq "cat hardlink returns source content" "$hardlink_cat" "overwrite-${TS}-append"
  if hardlink_nlink=$(wait_remote_stat_field_eq "$RW_HARDLINK_REMOTE" "nlink" "2"); then
    check_eq "remote hardlink nlink is 2" "$hardlink_nlink" "2"
  else
    check_eq "remote hardlink nlink is 2" "" "2"
  fi
  if source_resource_id=$(stat_field "$RW_TEXT_REMOTE" "resource_id") && hardlink_resource_id=$(stat_field "$RW_HARDLINK_REMOTE" "resource_id"); then
    check_cmd "remote source resource_id is non-empty" test -n "$source_resource_id"
    check_eq "remote hardlink shares resource_id" "$hardlink_resource_id" "$source_resource_id"
  else
    check_eq "remote hardlink resource_id fields are available" "false" "true"
  fi
  if [ -f "$RW_HARDLINK_MOUNT" ]; then
    printf "hardlink-%s" "$TS" > "$RW_HARDLINK_MOUNT"
    hardlink_source_cat=$(cat "$RW_TEXT_MOUNT")
    if remote_hardlink_source=$(wait_remote_cat_eq "$RW_TEXT_REMOTE" "hardlink-${TS}"); then
      :
    else
      remote_hardlink_source=""
    fi
  else
    hardlink_source_cat=""
    remote_hardlink_source=""
  fi
  check_eq "writing hardlink updates local source" "$hardlink_source_cat" "hardlink-${TS}"
  check_eq "writing hardlink updates remote source" "$remote_hardlink_source" "hardlink-${TS}"

  if : > "$RW_TEXT_MOUNT"; then
    check_eq "truncate via mount succeeds" "true" "true"
    truncated_size=$(wait_remote_stat_field_eq "$RW_TEXT_REMOTE" "size" "0")
    check_eq "truncate sets size to 0" "$truncated_size" "0"
  else
    check_eq "truncate via mount succeeds" "false" "true"
  fi

  printf "path-truncate-%s" "$TS" > "$RW_PATH_TRUNC_MOUNT"
  path_truncate_seed=$(wait_remote_cat_eq "$RW_PATH_TRUNC_REMOTE" "path-truncate-${TS}")
  check_eq "explicit path truncate seed visible via remote cat" "$path_truncate_seed" "path-truncate-${TS}"
  if path_truncate_zero "$RW_PATH_TRUNC_MOUNT"; then
    check_eq "explicit path truncate succeeds" "true" "true"
    path_truncate_local=$(cat "$RW_PATH_TRUNC_MOUNT")
    path_truncate_remote_size=$(wait_remote_stat_field_eq "$RW_PATH_TRUNC_REMOTE" "size" "0")
    path_truncate_remote=$(wait_remote_cat_eq "$RW_PATH_TRUNC_REMOTE" "")
    check_eq "explicit path truncate immediate mounted read is empty" "$path_truncate_local" ""
    check_eq "explicit path truncate remote size is 0" "$path_truncate_remote_size" "0"
    check_eq "explicit path truncate remote content is empty" "$path_truncate_remote" ""
  else
    check_eq "explicit path truncate succeeds" "false" "true"
  fi

  printf "path-truncate-rename-%s" "$TS" > "$RW_PATH_TRUNC_RENAME_SRC_MOUNT"
  path_truncate_rename_seed=$(wait_remote_cat_eq "$RW_PATH_TRUNC_RENAME_SRC_REMOTE" "path-truncate-rename-${TS}")
  check_eq "explicit path truncate rename seed visible via remote cat" "$path_truncate_rename_seed" "path-truncate-rename-${TS}"
  if path_truncate_zero "$RW_PATH_TRUNC_RENAME_SRC_MOUNT" && mv "$RW_PATH_TRUNC_RENAME_SRC_MOUNT" "$RW_PATH_TRUNC_RENAME_DST_MOUNT"; then
    check_eq "explicit path truncate then rename succeeds" "true" "true"
    check_cmd_fail "explicit path truncate rename source missing locally" test -e "$RW_PATH_TRUNC_RENAME_SRC_MOUNT"
    path_truncate_rename_size=$(wait_remote_stat_field_eq "$RW_PATH_TRUNC_RENAME_DST_REMOTE" "size" "0")
    path_truncate_rename_remote=$(wait_remote_cat_eq "$RW_PATH_TRUNC_RENAME_DST_REMOTE" "")
    check_cmd "explicit path truncate rename source stays missing remotely" assert_remote_ls_missing_stable "$RW_ALPHA_REMOTE" "path-truncate-rename-src.txt"
    check_eq "explicit path truncate rename destination remote size is 0" "$path_truncate_rename_size" "0"
    check_eq "explicit path truncate rename destination remote content is empty" "$path_truncate_rename_remote" ""
  else
    check_eq "explicit path truncate then rename succeeds" "false" "true"
  fi

  printf "path-truncate-unlink-%s" "$TS" > "$RW_PATH_TRUNC_UNLINK_MOUNT"
  path_truncate_unlink_seed=$(wait_remote_cat_eq "$RW_PATH_TRUNC_UNLINK_REMOTE" "path-truncate-unlink-${TS}")
  check_eq "explicit path truncate unlink seed visible via remote cat" "$path_truncate_unlink_seed" "path-truncate-unlink-${TS}"
  if path_truncate_zero "$RW_PATH_TRUNC_UNLINK_MOUNT" && rm -f "$RW_PATH_TRUNC_UNLINK_MOUNT"; then
    check_eq "explicit path truncate then unlink succeeds" "true" "true"
    check_cmd_fail "explicit path truncate unlink target missing locally" test -e "$RW_PATH_TRUNC_UNLINK_MOUNT"
    check_cmd "explicit path truncate unlink target stays missing remotely" assert_remote_ls_missing_stable "$RW_ALPHA_REMOTE" "path-truncate-unlink.txt"
  else
    check_eq "explicit path truncate then unlink succeeds" "false" "true"
  fi

  echo "[6] attribute semantics"
  printf "attr-base-%s" "$TS" > "$RW_ATTR_MOUNT"
  stat1=$(local_size_mtime "$RW_ATTR_MOUNT")
  size1="${stat1%%:*}"
  mtime1="${stat1##*:}"
  sleep 1
  printf -- "-x" >> "$RW_ATTR_MOUNT"
  stat2=$(local_size_mtime "$RW_ATTR_MOUNT")
  size2="${stat2%%:*}"
  mtime2="${stat2##*:}"
  remote_attr_size=$(wait_remote_stat_field_eq "$RW_ATTR_REMOTE" "size" "$size2")
  check_cmd "mounted size increases after append" test "$size2" -gt "$size1"
  check_cmd "mounted mtime is monotonic" test "$mtime2" -ge "$mtime1"
  check_eq "remote stat size matches mounted size" "$remote_attr_size" "$size2"

  echo "[7] readdir semantics"
  alpha_ls=$(ls -1 "$RW_ALPHA_MOUNT")
  alpha_has_beta=$(python3 - "$alpha_ls" <<'PY'
import sys
lines=[ln.strip() for ln in sys.argv[1].splitlines() if ln.strip()]
print("true" if "beta" in lines else "false")
PY
)
  alpha_has_text=$(python3 - "$alpha_ls" <<'PY'
import sys
lines=[ln.strip() for ln in sys.argv[1].splitlines() if ln.strip()]
print("true" if "text.txt" in lines else "false")
PY
)
  check_eq "readdir includes beta directory" "$alpha_has_beta" "true"
  check_eq "readdir includes text file" "$alpha_has_text" "true"

  echo "[8] rename semantics"
  mv "$RW_TEXT_MOUNT" "$RW_TEXT_RENAMED_MOUNT"
  check_cmd_fail "old file path missing after rename" test -f "$RW_TEXT_MOUNT"
  renamed_text=$(drive9_retry fs cat "$RW_TEXT_RENAMED_REMOTE")
  check_eq "renamed file readable via remote" "$renamed_text" ""

  rename_dir_ready=false
  if wait_path_exists "$RW_ALPHA_MOUNT"; then
    if mv "$RW_ALPHA_MOUNT" "$RW_ALPHA_RENAMED_MOUNT"; then
      check_eq "rename directory via mount succeeds" "true" "true"
      check_cmd "renamed directory visible via remote list" wait_remote_ls_has_name "$ROOT_REMOTE" "alpha-renamed"
      renamed_nested_text=$(drive9_retry fs cat "$RW_ALPHA_RENAMED_REMOTE/text-renamed.txt")
      check_eq "renamed directory keeps file content" "$renamed_nested_text" ""
      rename_dir_ready=true
    else
      check_eq "rename directory via mount succeeds" "false" "true"
      for _ in $(seq 1 "$CLI_MAX_RETRIES"); do
        drive9_retry fs mv "$RW_ALPHA_REMOTE" "$RW_ALPHA_RENAMED_REMOTE" >/dev/null 2>&1 || true
        if wait_remote_ls_has_name "$ROOT_REMOTE" "alpha-renamed"; then
          break
        fi
        sleep "$CLI_RETRY_SLEEP_S"
      done
      if wait_remote_ls_has_name "$ROOT_REMOTE" "alpha-renamed"; then
        echo "SKIP remote fallback restored alpha-renamed for cleanup only (tracked in issue #248)"
        if wait_path_exists "$RW_ALPHA_RENAMED_MOUNT"; then
          rename_dir_ready=true
        fi
      fi
    fi
  else
    drive9_retry fs mv "$RW_ALPHA_REMOTE" "$RW_ALPHA_RENAMED_REMOTE" >/dev/null 2>&1 || true
    if wait_remote_ls_has_name "$ROOT_REMOTE" "alpha-renamed"; then
      echo "SKIP remote fallback restored alpha-renamed after mount path disappeared (tracked in issue #248)"
      if wait_path_exists "$RW_ALPHA_RENAMED_MOUNT"; then
        rename_dir_ready=true
      fi
      check_eq "rename directory source exists" "false" "true"
    else
      check_eq "rename directory source exists" "false" "true"
    fi
  fi

  echo "[8.1] git config lockfile semantics"
  mkdir -p "$GIT_PROBE_MOUNT"
  git_config_ok=true
  if run_with_timeout 20 git -C "$GIT_PROBE_MOUNT" init >/dev/null; then
    if ! run_with_timeout 20 git -C "$GIT_PROBE_MOUNT" config core.repositoryformatversion 0; then
      git_config_ok=false
    fi
    if ! run_with_timeout 20 git -C "$GIT_PROBE_MOUNT" config core.filemode false; then
      git_config_ok=false
    fi
    if ! run_with_timeout 20 git -C "$GIT_PROBE_MOUNT" config core.bare false; then
      git_config_ok=false
    fi
    if ! run_with_timeout 20 git -C "$GIT_PROBE_MOUNT" config core.logallrefupdates true; then
      git_config_ok=false
    fi
    if ! run_with_timeout 20 git -C "$GIT_PROBE_MOUNT" config core.symlinks false; then
      git_config_ok=false
    fi
    if ! run_with_timeout 20 git -C "$GIT_PROBE_MOUNT" remote add origin "$GIT_PROBE_ORIGIN"; then
      git_config_ok=false
    fi
  else
    git_config_ok=false
  fi
  check_eq "git config lockfile updates succeed" "$git_config_ok" "true"
  git_origin_tmp="$(mktemp)"
  if run_with_timeout 20 git -C "$GIT_PROBE_MOUNT" config --get remote.origin.url >"$git_origin_tmp" 2>/dev/null; then
    git_origin_rc=0
    git_origin="$(cat "$git_origin_tmp")"
  else
    git_origin_rc=1
    git_origin=""
  fi
  rm -f "$git_origin_tmp"
  check_eq "git config remote origin is readable" "$git_origin_rc" "0"
  check_eq "git config remote origin survives lockfile reuse" "$git_origin" "$GIT_PROBE_ORIGIN"

  if [ "$RUN_FUSE_GIT_CLONE" = "1" ]; then
    echo "[8.2] git clone small repo"
    rm -rf "$GIT_CLONE_MOUNT"
    git_clone_ok=true
    date -u '+git clone start: %Y-%m-%dT%H:%M:%SZ'
    if command -v timeout >/dev/null 2>&1; then
      if ! GIT_PROGRESS_DELAY=0 timeout "$FUSE_GIT_CLONE_TIMEOUT_S" git clone --progress --depth 1 "$FUSE_GIT_CLONE_URL" "$GIT_CLONE_MOUNT"; then
        git_clone_ok=false
      fi
    elif ! GIT_PROGRESS_DELAY=0 git clone --progress --depth 1 "$FUSE_GIT_CLONE_URL" "$GIT_CLONE_MOUNT"; then
      git_clone_ok=false
    fi
    date -u '+git clone done:  %Y-%m-%dT%H:%M:%SZ'
    check_eq "git clone small repo succeeds" "$git_clone_ok" "true"
    if [ "$git_clone_ok" = "true" ]; then
      git_status=$(git -C "$GIT_CLONE_MOUNT" status --short)
      check_eq "git status clean after clone" "$git_status" ""
      check_cmd "git log reads latest commit" git -C "$GIT_CLONE_MOUNT" log --oneline -1
      check_cmd "git clone directory visible via remote list" wait_remote_ls_has_name "$ROOT_REMOTE" "hello-world"
      check_cmd "git config lockfile absent after clone" test ! -e "$GIT_CLONE_MOUNT/.git/config.lock"
    fi
  fi

  echo "[9] cross-channel consistency"
  if [ "$rename_dir_ready" = "true" ]; then
    drive9_retry fs cp "$SEED_LOCAL" ":$CLI_TO_MOUNT_REMOTE" >/dev/null
    if wait_path_exists "$CLI_TO_MOUNT_MOUNT"; then
      check_eq "cli write appears in mount" "true" "true"
    else
      check_eq "cli write appears in mount" "false" "true"
    fi
    cli_to_mount_content=$(cat "$CLI_TO_MOUNT_MOUNT")
    check_eq "mount reads cli-written content" "$cli_to_mount_content" "seed-${TS}"

    # The CLI read here is a cross-channel visibility check, not a guarantee
    # that mount Flush immediately made the object remotely readable.
    printf "from-mount-%s" "$TS" > "$MOUNT_TO_CLI_MOUNT"
    mount_to_cli_content=$(wait_remote_cat_eq "$MOUNT_TO_CLI_REMOTE" "from-mount-${TS}")
    check_eq "remote reads mount-written content" "$mount_to_cli_content" "from-mount-${TS}"
  else
    echo "SKIP cross-channel consistency after mount directory rename failure (tracked in issue #248)"
  fi

  echo "[10] large-file boundary"
  if [ "$rename_dir_ready" = "true" ]; then
    dd if=/dev/zero of="$LARGE_MOUNT" bs=1M count=8 status=none
    large_size=$(wait_remote_stat_field_eq "$LARGE_REMOTE" "size" "8388608" "$LARGE_FILE_VISIBILITY_TIMEOUT_S" "$LARGE_FILE_VISIBILITY_INTERVAL_S")
    check_eq "8MB mounted file size matches remote stat" "$large_size" "8388608"
    drive9_retry fs cp ":$LARGE_REMOTE" "$LARGE_DOWNLOADED" >/dev/null
    check_cmd "downloaded large file exists" test -f "$LARGE_DOWNLOADED"
    large_src_hash=$(sha256_file "$LARGE_MOUNT")
    large_dst_hash=$(sha256_file "$LARGE_DOWNLOADED")
    check_eq "large file checksum matches" "$large_dst_hash" "$large_src_hash"

    echo "[10.1] mounted tier-transition parity"
    write_pattern_file "$TIER_MOUNT" 10240 17
    tier_small_initial_hash=$(sha256_file "$TIER_MOUNT")
    tier_small_initial_size=$(wait_remote_stat_field_eq "$TIER_REMOTE" "size" "10240")
    check_eq "tier transition initial 10KiB size matches remote stat" "$tier_small_initial_size" "10240"
    if tier_small_initial_remote_hash=$(wait_remote_file_hash_eq "$TIER_REMOTE" "$tier_small_initial_hash" "$TIER_DOWNLOADED"); then
      :
    else
      tier_small_initial_remote_hash=""
    fi
    check_eq "tier transition initial 10KiB checksum matches" "$tier_small_initial_remote_hash" "$tier_small_initial_hash"

    write_pattern_file "$TIER_MOUNT" 8388608 43
    tier_large_hash=$(sha256_file "$TIER_MOUNT")
    tier_large_size=$(wait_remote_stat_field_eq "$TIER_REMOTE" "size" "8388608" "$LARGE_FILE_VISIBILITY_TIMEOUT_S" "$LARGE_FILE_VISIBILITY_INTERVAL_S")
    check_eq "tier transition 8MiB A size matches remote stat" "$tier_large_size" "8388608"
    if tier_large_remote_hash=$(wait_remote_file_hash_eq "$TIER_REMOTE" "$tier_large_hash" "$TIER_DOWNLOADED" "$LARGE_FILE_VISIBILITY_TIMEOUT_S" "$LARGE_FILE_VISIBILITY_INTERVAL_S"); then
      :
    else
      tier_large_remote_hash=""
    fi
    check_eq "tier transition 8MiB A checksum matches" "$tier_large_remote_hash" "$tier_large_hash"

    write_pattern_file "$TIER_MOUNT" 8388608 67
    tier_large_second_hash=$(sha256_file "$TIER_MOUNT")
    tier_large_second_size=$(wait_remote_stat_field_eq "$TIER_REMOTE" "size" "8388608" "$LARGE_FILE_VISIBILITY_TIMEOUT_S" "$LARGE_FILE_VISIBILITY_INTERVAL_S")
    check_eq "tier transition 8MiB B size matches remote stat" "$tier_large_second_size" "8388608"
    if tier_large_second_remote_hash=$(wait_remote_file_hash_eq "$TIER_REMOTE" "$tier_large_second_hash" "$TIER_DOWNLOADED" "$LARGE_FILE_VISIBILITY_TIMEOUT_S" "$LARGE_FILE_VISIBILITY_INTERVAL_S"); then
      :
    else
      tier_large_second_remote_hash=""
    fi
    check_eq "tier transition 8MiB B checksum matches" "$tier_large_second_remote_hash" "$tier_large_second_hash"

    write_pattern_file "$TIER_MOUNT" 10240 89
    tier_small_final_hash=$(sha256_file "$TIER_MOUNT")
    tier_small_final_size=$(wait_remote_stat_field_eq "$TIER_REMOTE" "size" "10240")
    check_eq "tier transition final 10KiB size matches remote stat" "$tier_small_final_size" "10240"
    if tier_small_final_remote_hash=$(wait_remote_file_hash_eq "$TIER_REMOTE" "$tier_small_final_hash" "$TIER_DOWNLOADED"); then
      :
    else
      tier_small_final_remote_hash=""
    fi
    check_eq "tier transition final 10KiB checksum matches" "$tier_small_final_remote_hash" "$tier_small_final_hash"

    rw_mount_available=true
    if unmount_mount; then
      check_eq "rw mount unmounted after tier transition" "true" "true"
    else
      check_eq "rw mount unmounted after tier transition" "false" "true"
      stop_mount
    fi
    if start_mount rw; then
      check_eq "rw mount remounted after tier transition" "true" "true"
    else
      check_eq "rw mount remounted after tier transition" "false" "true"
      rw_mount_available=false
    fi
    if [ "$rw_mount_available" = "true" ] && is_mounted "$MOUNT_POINT"; then
      check_cmd "tier transition file visible after remount" wait_path_exists "$TIER_MOUNT"
      if [ -f "$TIER_MOUNT" ]; then
        tier_remount_hash=$(sha256_file "$TIER_MOUNT")
      else
        tier_remount_hash=""
      fi
      check_eq "tier transition final checksum survives remount" "$tier_remount_hash" "$tier_small_final_hash"
      check_cmd "explicit path truncate file visible after remount" wait_path_exists "$RW_PATH_TRUNC_AFTER_RENAME_MOUNT"
      if [ -f "$RW_PATH_TRUNC_AFTER_RENAME_MOUNT" ]; then
        path_truncate_remount=$(cat "$RW_PATH_TRUNC_AFTER_RENAME_MOUNT")
      else
        path_truncate_remount="missing"
      fi
      check_eq "explicit path truncate content survives remount" "$path_truncate_remount" ""
    else
      echo "SKIP tier transition remount content checks because rw mount is unavailable"
      rw_mount_available=false
    fi
  else
    echo "SKIP large-file boundary after mount directory rename failure (tracked in issue #248)"
  fi

  if [ "${rw_mount_available:-true}" = "true" ] && is_mounted "$MOUNT_POINT"; then
  echo "[11] error semantics"
  check_cmd_fail "cat missing file via mount fails" cat "$MOUNT_POINT/${ROOT_REL}/no-such-file.txt"

  mkdir -p "$MOUNT_POINT/${ROOT_REL}/dupdir"
  check_cmd_fail "mkdir existing dir fails" mkdir "$MOUNT_POINT/${ROOT_REL}/dupdir"

  mkdir -p "$MOUNT_POINT/${ROOT_REL}/nonempty"
  printf "x" > "$MOUNT_POINT/${ROOT_REL}/nonempty/x.txt"
  check_eq "non-empty dir child visible via remote" "$(wait_remote_cat_eq "/${ROOT_REL}/nonempty/x.txt" "x")" "x"
  check_cmd_fail "rmdir non-empty dir fails" rmdir "$MOUNT_POINT/${ROOT_REL}/nonempty"
  rm -f "$MOUNT_POINT/${ROOT_REL}/nonempty/x.txt"
  if [ -d "$MOUNT_POINT/${ROOT_REL}/nonempty" ]; then
    check_cmd "rmdir empty dir succeeds" bash -c 'rmdir "$1" 2>/dev/null || [ ! -e "$1" ]' _ "$MOUNT_POINT/${ROOT_REL}/nonempty"
  else
    check_eq "rmdir empty dir succeeds" "already-gone" "already-gone"
  fi

  check_cmd_fail "rm missing file fails" rm "$MOUNT_POINT/${ROOT_REL}/missing.txt"

  if [ "$RUN_FUSE_UMOUNT_DURABLE" = "1" ]; then
    echo "[12] umount durable remount"
    printf "durable-%s" "$TS" > "$DURABLE_MOUNT"
    if unmount_mount; then
      check_eq "rw mount unmounted after durable write" "true" "true"
    else
      check_eq "rw mount unmounted after durable write" "false" "true"
      stop_mount
    fi
    if start_mount rw; then
      check_eq "rw mount remounted after durable write" "true" "true"
    else
      check_eq "rw mount remounted after durable write" "false" "true"
    fi
    if is_mounted "$MOUNT_POINT"; then
      if wait_path_exists "$DURABLE_MOUNT"; then
        check_eq "durable file visible after remount" "true" "true"
        durable_mounted=$(cat "$DURABLE_MOUNT")
        durable_remote=$(drive9_retry fs cat "$DURABLE_REMOTE")
        check_eq "durable file content survives remount" "$durable_mounted" "durable-${TS}"
        check_eq "durable file remote content survives umount" "$durable_remote" "durable-${TS}"
      else
        check_eq "durable file visible after remount" "false" "true"
      fi
      if [ "$RUN_FUSE_GIT_CLONE" = "1" ]; then
        check_cmd "git clone directory survives remount" test -d "$GIT_CLONE_MOUNT/.git"
        check_cmd "git log works after remount" git -C "$GIT_CLONE_MOUNT" log --oneline -1
      fi
    fi
  fi

  echo "[12.1] cleanup writable tree"
  rm -rf "${MOUNT_POINT:?}/${ROOT_REL:?}"
  check_cmd "remote root removed from ls after mounted rm -rf" wait_remote_ls_missing_name "/" "$ROOT_REL"

  echo "[13] remount read-only semantics"
  stop_mount
  check_cmd "rw mount unmounted" wait_mount_state unmounted

  drive9_retry fs cp "$SEED_LOCAL" ":$RO_SEED_REMOTE" >/dev/null
  if start_mount ro; then
    check_eq "read-only mount point is mounted" "true" "true"
  else
    check_eq "read-only mount point is mounted" "false" "true"
  fi

  if is_mounted "$MOUNT_POINT"; then
    if wait_path_exists "$RO_SEED_MOUNT"; then
      check_eq "ro seed appears in mount" "true" "true"
      ro_read=$(cat "$RO_SEED_MOUNT")
      check_eq "ro seed readable" "$ro_read" "seed-${TS}"
    else
      check_eq "ro seed appears in mount" "false" "true"
    fi

    check_cmd_fail "write fails on read-only mount" bash -c "printf 'x' > '$RO_WRITE_MOUNT'"
    check_cmd_fail "delete fails on read-only mount" rm "$RO_SEED_MOUNT"
  fi

  echo "[14] unmount"
  if unmount_mount; then
    check_eq "final mount unmounted" "true" "true"
  else
    check_eq "final mount unmounted" "false" "true"
    stop_mount
  fi
  else
    echo "SKIP remaining mounted checks because rw mount is unavailable after tier transition remount"
    drive9_retry fs rm -r "$ROOT_REMOTE" >/dev/null 2>&1 || true
    stop_mount
  fi
  # End of the writable-alpha branch opened after the nested mkdir checks.
  fi
fi # End of the mounted-rw branch.

if [ "$RUN_FUSE_LOG_AUDIT" = "1" ]; then
  echo "[15] mount log audit"
  check_cmd "mount log has no release-gate failure patterns" audit_mount_log
fi

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
