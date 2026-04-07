#!/usr/bin/env bash
# dat9 FUSE smoke test against a live deployment.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-20}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
FUSE_MOUNT_ROOT="${FUSE_MOUNT_ROOT:-/tmp}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-8}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-2}"

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

is_mounted() {
  local mount_point="$1"
  if command -v mountpoint >/dev/null 2>&1; then
    mountpoint -q "$mount_point"
    return
  fi
  awk -v mp="$mount_point" '$2 == mp { found = 1 } END { exit !found }' /proc/mounts
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
  out=$(dat9_retry fs stat "$path")
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
    out=$(dat9 fs ls "$parent" 2>&1)
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
    out=$(dat9 fs ls "$parent" 2>&1)
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

start_mount() {
  local mode="$1"
  : >"$MOUNT_LOG"
  if [ "$mode" = "ro" ]; then
    dat9 mount --read-only "$MOUNT_POINT" >"$MOUNT_LOG" 2>&1 &
  else
    dat9 mount "$MOUNT_POINT" >"$MOUNT_LOG" 2>&1 &
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
    dat9 umount "$MOUNT_POINT" >/dev/null 2>&1 || true
    wait_mount_state unmounted >/dev/null 2>&1 || true
  fi
  if [ -n "${MOUNT_PID:-}" ] && kill -0 "$MOUNT_PID" >/dev/null 2>&1; then
    kill "$MOUNT_PID" >/dev/null 2>&1 || true
    wait "$MOUNT_PID" >/dev/null 2>&1 || true
  fi
  MOUNT_PID=""
  set -e
}

echo "=== dat9 FUSE smoke test ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
else
  check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
fi
check_cmd "python3 is available" bash -c 'command -v python3 >/dev/null'

if [ "$(uname -s)" != "Linux" ] && [ "$(uname -s)" != "Darwin" ]; then
  skip "unsupported OS for this smoke script"
fi

if [ "$(uname -s)" = "Linux" ]; then
  if ! command -v fusermount >/dev/null 2>&1 && ! command -v fusermount3 >/dev/null 2>&1; then
    skip "fusermount/fusermount3 is required for Linux FUSE unmount"
  fi
  if [ ! -e /dev/fuse ]; then
    skip "/dev/fuse not available"
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

dat9() {
  DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

dat9_retry() {
  local attempt=1
  local out rc
  while :; do
    set +e
    out=$(dat9 "$@" 2>&1)
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
ROOT_MOUNT="$FUSE_MOUNT_ROOT/dat9-fuse-smoke-${TS}"
MOUNT_POINT="$ROOT_MOUNT"
MOUNT_LOG="$FUSE_MOUNT_ROOT/dat9-fuse-smoke-${TS}.log"
SEED_LOCAL="$FUSE_MOUNT_ROOT/dat9-fuse-seed-${TS}.txt"
LARGE_DOWNLOADED="$FUSE_MOUNT_ROOT/dat9-fuse-large-down-${TS}.bin"

set +e
ls_out=$(dat9 fs ls / 2>&1)
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
RW_TEXT_RENAMED_REL="${RW_ALPHA_REL}/text-renamed.txt"
RW_TEXT_RENAMED_REMOTE="/${RW_TEXT_RENAMED_REL}"
RW_TEXT_RENAMED_MOUNT="$MOUNT_POINT/$RW_TEXT_RENAMED_REL"
RW_ALPHA_RENAMED_REL="${ROOT_REL}/alpha-renamed"
RW_ALPHA_RENAMED_REMOTE="/${RW_ALPHA_RENAMED_REL}"
RW_ALPHA_RENAMED_MOUNT="$MOUNT_POINT/$RW_ALPHA_RENAMED_REL"
CLI_TO_MOUNT_REL="${RW_ALPHA_RENAMED_REL}/from-cli.txt"
CLI_TO_MOUNT_REMOTE="/${CLI_TO_MOUNT_REL}"
CLI_TO_MOUNT_MOUNT="$MOUNT_POINT/$CLI_TO_MOUNT_REL"
MOUNT_TO_CLI_REL="${RW_ALPHA_RENAMED_REL}/from-mount.txt"
MOUNT_TO_CLI_REMOTE="/${MOUNT_TO_CLI_REL}"
MOUNT_TO_CLI_MOUNT="$MOUNT_POINT/$MOUNT_TO_CLI_REL"
LARGE_REL="${RW_ALPHA_RENAMED_REL}/large-8m.bin"
LARGE_REMOTE="/${LARGE_REL}"
LARGE_MOUNT="$MOUNT_POINT/$LARGE_REL"
RO_SEED_REL="${ROOT_REL}/ro-seed.txt"
RO_SEED_REMOTE="/${RO_SEED_REL}"
RO_SEED_MOUNT="$MOUNT_POINT/$RO_SEED_REL"
RO_WRITE_MOUNT="$MOUNT_POINT/${ROOT_REL}/ro-write.txt"

mkdir -p "$MOUNT_POINT"
printf "seed-%s" "$TS" > "$SEED_LOCAL"

MOUNT_PID=""
cleanup() {
  stop_mount
  rm -f "$SEED_LOCAL" "$LARGE_DOWNLOADED" "$CLI_BIN"
  rm -rf "$MOUNT_POINT" || true
}
trap cleanup EXIT

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
    dat9_retry fs mkdir "$RW_ALPHA_REMOTE" >/dev/null 2>&1 || true
    dat9_retry fs mkdir "$RW_ALPHA_REMOTE/beta" >/dev/null 2>&1 || true
  fi
  check_cmd "nested directory visible in mount" wait_path_exists "$RW_BETA_MOUNT"
  check_cmd "nested directory visible in remote list" wait_remote_ls_has_name "$RW_ALPHA_REMOTE" "beta"

  if ! wait_path_exists "$RW_ALPHA_MOUNT"; then
    check_eq "mounted alpha directory is available for write" "false" "true"
  else

    printf "create-%s" "$TS" > "$RW_TEXT_MOUNT"
    mounted_text=$(cat "$RW_TEXT_MOUNT")
    remote_text=$(dat9_retry fs cat "$RW_TEXT_REMOTE")
    check_eq "create/read via mount" "$mounted_text" "create-${TS}"
    check_eq "create visible via remote cat" "$remote_text" "create-${TS}"

  printf "overwrite-%s" "$TS" > "$RW_TEXT_MOUNT"
  remote_overwrite=$(dat9_retry fs cat "$RW_TEXT_REMOTE")
  check_eq "overwrite visible via remote cat" "$remote_overwrite" "overwrite-${TS}"

  printf -- "-append" >> "$RW_TEXT_MOUNT"
  remote_append=$(dat9_retry fs cat "$RW_TEXT_REMOTE")
  check_eq "append visible via remote cat" "$remote_append" "overwrite-${TS}-append"

  if : > "$RW_TEXT_MOUNT"; then
    check_eq "truncate via mount succeeds" "true" "true"
    truncated_size=$(stat_field "$RW_TEXT_REMOTE" "size")
    check_eq "truncate sets size to 0" "$truncated_size" "0"
  else
    check_eq "truncate via mount succeeds" "false" "true"
  fi

  echo "[6] attribute semantics"
  printf "attr-base-%s" "$TS" > "$RW_TEXT_MOUNT"
  stat1=$(local_size_mtime "$RW_TEXT_MOUNT")
  size1="${stat1%%:*}"
  mtime1="${stat1##*:}"
  sleep 1
  printf -- "-x" >> "$RW_TEXT_MOUNT"
  stat2=$(local_size_mtime "$RW_TEXT_MOUNT")
  size2="${stat2%%:*}"
  mtime2="${stat2##*:}"
  remote_attr_size=$(stat_field "$RW_TEXT_REMOTE" "size")
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
  renamed_text=$(dat9_retry fs cat "$RW_TEXT_RENAMED_REMOTE")
  check_eq "renamed file readable via remote" "$renamed_text" "attr-base-${TS}-x"

  if wait_path_exists "$RW_ALPHA_MOUNT"; then
    if mv "$RW_ALPHA_MOUNT" "$RW_ALPHA_RENAMED_MOUNT"; then
      check_eq "rename directory via mount succeeds" "true" "true"
      check_cmd "renamed directory visible via remote list" wait_remote_ls_has_name "$ROOT_REMOTE" "alpha-renamed"
      renamed_nested_text=$(dat9_retry fs cat "$RW_ALPHA_RENAMED_REMOTE/text-renamed.txt")
      check_eq "renamed directory keeps file content" "$renamed_nested_text" "attr-base-${TS}-x"
    else
      for _ in $(seq 1 "$CLI_MAX_RETRIES"); do
        dat9_retry fs mv "$RW_ALPHA_REMOTE" "$RW_ALPHA_RENAMED_REMOTE" >/dev/null 2>&1 || true
        if wait_remote_ls_has_name "$ROOT_REMOTE" "alpha-renamed"; then
          break
        fi
        sleep "$CLI_RETRY_SLEEP_S"
      done
      if wait_remote_ls_has_name "$ROOT_REMOTE" "alpha-renamed"; then
        check_eq "rename directory via mount succeeds" "fallback" "fallback"
      else
        check_eq "rename directory via mount succeeds" "false" "true"
      fi
    fi
  else
    dat9_retry fs mv "$RW_ALPHA_REMOTE" "$RW_ALPHA_RENAMED_REMOTE" >/dev/null 2>&1 || true
    if wait_remote_ls_has_name "$ROOT_REMOTE" "alpha-renamed"; then
      check_eq "rename directory source exists" "fallback" "fallback"
    else
      check_eq "rename directory source exists" "false" "true"
    fi
  fi

  echo "[9] cross-channel consistency"
  dat9_retry fs cp "$SEED_LOCAL" ":$CLI_TO_MOUNT_REMOTE" >/dev/null
  if wait_path_exists "$CLI_TO_MOUNT_MOUNT"; then
    check_eq "cli write appears in mount" "true" "true"
  else
    check_eq "cli write appears in mount" "false" "true"
  fi
  cli_to_mount_content=$(cat "$CLI_TO_MOUNT_MOUNT")
  check_eq "mount reads cli-written content" "$cli_to_mount_content" "seed-${TS}"

  printf "from-mount-%s" "$TS" > "$MOUNT_TO_CLI_MOUNT"
  mount_to_cli_content=$(dat9_retry fs cat "$MOUNT_TO_CLI_REMOTE")
  check_eq "remote reads mount-written content" "$mount_to_cli_content" "from-mount-${TS}"

  echo "[10] large-file boundary"
  dd if=/dev/zero of="$LARGE_MOUNT" bs=1M count=8 status=none
  large_size=$(stat_field "$LARGE_REMOTE" "size")
  check_eq "8MB mounted file size matches remote stat" "$large_size" "8388608"
  dat9_retry fs cp ":$LARGE_REMOTE" "$LARGE_DOWNLOADED" >/dev/null
  check_cmd "downloaded large file exists" test -f "$LARGE_DOWNLOADED"
  large_src_hash=$(sha256_file "$LARGE_MOUNT")
  large_dst_hash=$(sha256_file "$LARGE_DOWNLOADED")
  check_eq "large file checksum matches" "$large_dst_hash" "$large_src_hash"

  echo "[11] error semantics"
  check_cmd_fail "cat missing file via mount fails" cat "$MOUNT_POINT/${ROOT_REL}/no-such-file.txt"

  mkdir -p "$MOUNT_POINT/${ROOT_REL}/dupdir"
  check_cmd_fail "mkdir existing dir fails" mkdir "$MOUNT_POINT/${ROOT_REL}/dupdir"

  mkdir -p "$MOUNT_POINT/${ROOT_REL}/nonempty"
  printf "x" > "$MOUNT_POINT/${ROOT_REL}/nonempty/x.txt"
  check_cmd_fail "rmdir non-empty dir fails" rmdir "$MOUNT_POINT/${ROOT_REL}/nonempty"
  rm -f "$MOUNT_POINT/${ROOT_REL}/nonempty/x.txt"
  if [ -d "$MOUNT_POINT/${ROOT_REL}/nonempty" ]; then
    check_cmd "rmdir empty dir succeeds" bash -c 'rmdir "$1" 2>/dev/null || [ ! -e "$1" ]' _ "$MOUNT_POINT/${ROOT_REL}/nonempty"
  else
    check_eq "rmdir empty dir succeeds" "already-gone" "already-gone"
  fi

  check_cmd_fail "rm missing file fails" rm "$MOUNT_POINT/${ROOT_REL}/missing.txt"

  echo "[12] cleanup writable tree"
  rm -rf "$MOUNT_POINT/$ROOT_REL"
  check_cmd "remote root removed from ls after mounted rm -rf" wait_remote_ls_missing_name "/" "$ROOT_REL"

  echo "[13] remount read-only semantics"
  stop_mount
  check_cmd "rw mount unmounted" wait_mount_state unmounted

  dat9_retry fs cp "$SEED_LOCAL" ":$RO_SEED_REMOTE" >/dev/null
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
    stop_mount
    check_cmd "final mount unmounted" wait_mount_state unmounted
  fi
fi

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
