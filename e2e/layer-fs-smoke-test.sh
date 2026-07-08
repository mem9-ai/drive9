#!/usr/bin/env bash
# drive9 layer filesystem smoke test against a live deployment.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-8}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-2}"
CLI_HOME="${DRIVE9_E2E_CLI_HOME:-$(mktemp -d)}"
RUN_LAYER_FUSE_SMOKE="${RUN_LAYER_FUSE_SMOKE:-0}"
LAYER_FUSE_STRICT_PREREQS="${LAYER_FUSE_STRICT_PREREQS:-$RUN_LAYER_FUSE_SMOKE}"
FUSE_MOUNT_ROOT="${FUSE_MOUNT_ROOT:-/tmp}"
FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-60s}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-20}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
LAYER_DIFF_TIMEOUT_S="${LAYER_DIFF_TIMEOUT_S:-20}"
LAYER_DIFF_INTERVAL_S="${LAYER_DIFF_INTERVAL_S:-1}"
LAYER_FUSE_LOG_AUDIT_PATTERN="${LAYER_FUSE_LOG_AUDIT_PATTERN:-panic|fatal error|Resource temporarily unavailable|restore fs layer entries failed}"
LAYER_CLI_LARGE_FILE_MB="${LAYER_CLI_LARGE_FILE_MB:-100}"

PASS=0
FAIL=0
TOTAL=0
MOUNT_POINT=""
MOUNT_PID=""
MOUNT_LOG=""

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

readlink_target_for_check() {
  local path="$1"
  local out
  if out=$(readlink "$path" 2>&1); then
    printf '%s' "$out"
    return 0
  fi
  printf 'readlink failed: %s' "$out"
  return 0
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
import pathlib
import sys

h = hashlib.sha256()
with pathlib.Path(sys.argv[1]).open("rb") as f:
    for chunk in iter(lambda: f.read(1024 * 1024), b""):
        h.update(chunk)
print(h.hexdigest())
PY
}

skip_layer_fuse() {
  echo "SKIP layer FUSE restore smoke: $*"
}

fail_layer_fuse_prereq() {
  if [ "$LAYER_FUSE_STRICT_PREREQS" = "1" ]; then
    echo "FAIL layer FUSE restore smoke: $*" >&2
    exit 1
  fi
  skip_layer_fuse "$@"
  return 0
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
  local method="$1"
  local url="$2"
  local auth="${3:-}"
  local data="${4:-}"
  local body_file
  local code
  local curl_rc
  local attempt
  local -a args

  body_file="$(mktemp)"
  attempt=1
  while :; do
    : >"$body_file"
    args=(-sS -o "$body_file" -w "%{http_code}" -X "$method")
    if [ -n "$auth" ]; then
      args+=(-H "Authorization: Bearer $auth")
    fi
    if [ -n "$data" ]; then
      args+=(-H "Content-Type: application/json" --data-binary "$data")
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
      000|429|5??)
        echo "retrying $method $url after HTTP $code (attempt $attempt/$REQUEST_MAX_RETRIES)" >&2
        sleep "$REQUEST_RETRY_SLEEP_S"
        attempt=$((attempt + 1))
        continue
        ;;
    esac
    if [ "$curl_rc" -eq 0 ]; then
      break
    fi
    echo "retrying $method $url after curl exit $curl_rc (attempt $attempt/$REQUEST_MAX_RETRIES)" >&2
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

dump_mount_log() {
  if [ -n "${MOUNT_LOG:-}" ] && [ -f "$MOUNT_LOG" ]; then
    echo "=== drive9 layer mount log: $MOUNT_LOG ==="
    cat "$MOUNT_LOG"
  fi
}

stop_mount() {
  set +e
  if [ -n "${MOUNT_POINT:-}" ] && is_mounted_path "$MOUNT_POINT"; then
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT" >/dev/null 2>&1 || true
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

provision_key() {
  local resp code body status deadline

  resp=$(curl_body_code POST "$BASE/v1/provision")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "POST /v1/provision returns 202" "$code" "202"
  API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
  check_cmd "provision response contains api_key" test -n "$API_KEY"

  deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  while :; do
    resp=$(curl_body_code GET "$BASE/v1/status" "$API_KEY")
    body=$(json_body "$resp")
    status=$(printf '%s' "$body" | jq -r '.status // empty')
    if [ "$status" = "active" ]; then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    sleep "$POLL_INTERVAL_S"
  done
  check_eq "tenant eventually becomes active" "$status" "active"
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
}

prepare_cli_binary() {
  CLI_BIN="$(mktemp)"
  case "$CLI_SOURCE" in
    build)
      script_dir="$(cd "$(dirname "$0")" && pwd)"
      repo_root="$(git -C "$script_dir/.." rev-parse --show-toplevel 2>/dev/null || (cd "$script_dir/.." && pwd))"
      make -C "$repo_root" build-cli CLI_BIN="$CLI_BIN"
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
  env -u DRIVE9_VAULT_TOKEN HOME="$CLI_HOME" DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
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
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* || "$out" == *"not found"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for drive9 $*" >&2
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

start_layer_mount() {
  local layer_ref="$1"
  local checkpoint_ref="$2"
  local remote_root="$3"
  local mount_point="$4"
  local local_root="$5"
  local mount_log="$6"
  MOUNT_POINT="$mount_point"
  MOUNT_LOG="$mount_log"
  mkdir -p "$MOUNT_POINT" "$local_root"
  {
    echo "=== drive9 layer mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "layer_ref=$layer_ref"
    echo "checkpoint_ref=$checkpoint_ref"
    echo "remote_root=$remote_root"
    echo "mount_point=$MOUNT_POINT"
    echo "local_root=$local_root"
  } >>"$MOUNT_LOG"

  local args=(mount --foreground --mode=fuse --profile=coding-agent --local-root "$local_root" --durability=write-sync --flush-debounce=0 --layer "$layer_ref")
  if [ -n "$checkpoint_ref" ]; then
    args+=(--checkpoint "$checkpoint_ref")
  fi
  args+=(":$remote_root" "$MOUNT_POINT")
  drive9 "${args[@]}" >>"$MOUNT_LOG" 2>&1 &
  MOUNT_PID="$!"
  wait_mount_state mounted
  wait_mount_log_ready "$MOUNT_LOG"
}

expect_layer_mount_fail() {
  local layer_ref="$1"
  local checkpoint_ref="$2"
  local remote_root="$3"
  local mount_point="$4"
  local local_root="$5"
  local mount_log="$6"
  mkdir -p "$mount_point" "$local_root"
  {
    echo "=== drive9 layer mount expected failure time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "layer_ref=$layer_ref"
    echo "checkpoint_ref=$checkpoint_ref"
    echo "remote_root=$remote_root"
    echo "mount_point=$mount_point"
    echo "local_root=$local_root"
  } >>"$mount_log"

  local args=(mount --foreground --mode=fuse --profile=coding-agent --local-root "$local_root" --durability=write-sync --flush-debounce=0 --layer "$layer_ref" --checkpoint "$checkpoint_ref" ":$remote_root" "$mount_point")
  set +e
  drive9 "${args[@]}" >>"$mount_log" 2>&1 &
  local pid="$!"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  while kill -0 "$pid" >/dev/null 2>&1; do
    if is_mounted_path "$mount_point"; then
      drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$mount_point" >/dev/null 2>&1
      wait "$pid" >/dev/null 2>&1
      set -e
      echo "layer mount unexpectedly succeeded for mismatched checkpoint" >&2
      return 1
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      kill "$pid" >/dev/null 2>&1
      wait "$pid" >/dev/null 2>&1
      set -e
      echo "layer mount did not fail within ${MOUNT_READY_TIMEOUT_S}s" >&2
      return 1
    fi
    sleep "$MOUNT_READY_INTERVAL_S"
  done
  wait "$pid"
  local rc="$?"
  set -e
  if [ "$rc" -eq 0 ]; then
    echo "layer mount exited successfully, want checkpoint mismatch failure" >&2
    return 1
  fi
  if is_mounted_path "$mount_point"; then
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$mount_point" >/dev/null 2>&1
    echo "layer mount left a mounted path after checkpoint mismatch" >&2
    return 1
  fi
  if ! grep -F "checkpoint $checkpoint_ref belongs to layer" "$mount_log" >/dev/null; then
    echo "layer mount mismatch log did not mention checkpoint ownership" >&2
    return 1
  fi
  return 0
}

unmount_layer_mount() {
  if [ -z "${MOUNT_POINT:-}" ]; then
    return 0
  fi
  if is_mounted_path "$MOUNT_POINT"; then
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT"
  fi
  wait_mount_state unmounted
  if [ -n "${MOUNT_PID:-}" ]; then
    set +e
    wait "$MOUNT_PID" >/dev/null 2>&1
    MOUNT_PID=""
    set -e
  fi
}

audit_mount_log() {
  local log_file="$1"
  if [ ! -f "$log_file" ]; then
    echo "mount log missing: $log_file" >&2
    return 1
  fi
  if grep -Eina "$LAYER_FUSE_LOG_AUDIT_PATTERN" "$log_file"; then
    echo "mount log contains layer FUSE failure pattern" >&2
    return 1
  fi
  return 0
}

wait_layer_diff_count() {
  local layer_ref="$1"
  local want="$2"
  local deadline=$(( $(date +%s) + LAYER_DIFF_TIMEOUT_S ))
  local out rc got
  while :; do
    set +e
    out=$(drive9 fs layer diff --json "$layer_ref" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      got=$(printf '%s' "$out" | jq -r '.entries | length')
      if [ "$got" = "$want" ]; then
        printf '%s' "$got"
        return 0
      fi
    elif [[ "$out" != *"Too Many Requests"* && "$out" != *"HTTP 429"* && "$out" != *"not found"* ]]; then
      printf '%s\n' "$out" >&2
      return 1
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      printf 'wait_layer_diff_count: timeout layer=%s want=%s got=%s\n' "$layer_ref" "$want" "${got:-<none>}" >&2
      return 1
    fi
    sleep "$LAYER_DIFF_INTERVAL_S"
  done
}

wait_layer_diff_entries() {
  local layer_ref="$1"
  local want="$2"
  shift 2
  local -a expected=("$@")
  local deadline=$(( $(date +%s) + LAYER_DIFF_TIMEOUT_S ))
  local out rc got ok i p op
  while :; do
    set +e
    out=$(drive9 fs layer diff --json "$layer_ref" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      got=$(printf '%s' "$out" | jq -r '.entries | length')
      ok=1
      if [ "$got" != "$want" ]; then
        ok=0
      else
        i=0
        while [ "$i" -lt "${#expected[@]}" ]; do
          p="${expected[$i]}"
          op="${expected[$((i + 1))]}"
          if ! printf '%s' "$out" | jq -e --arg p "$p" --arg op "$op" '.entries[] | select(.path == $p and .op == $op)' >/dev/null; then
            ok=0
            break
          fi
          i=$((i + 2))
        done
      fi
      if [ "$ok" -eq 1 ]; then
        printf '%s' "$got"
        return 0
      fi
    elif [[ "$out" != *"Too Many Requests"* && "$out" != *"HTTP 429"* && "$out" != *"not found"* ]]; then
      printf '%s\n' "$out" >&2
      return 1
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      printf 'wait_layer_diff_entries: timeout layer=%s want=%s got=%s expected=%s\n' "$layer_ref" "$want" "${got:-<none>}" "${expected[*]}" >&2
      return 1
    fi
    sleep "$LAYER_DIFF_INTERVAL_S"
  done
}

wait_layer_diff_file_mode() {
  local layer_ref="$1"
  local path="$2"
  local want="$3"
  local deadline=$(( $(date +%s) + LAYER_DIFF_TIMEOUT_S ))
  local out rc got
  while :; do
    set +e
    out=$(drive9 fs layer diff --json "$layer_ref" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      got=$(printf '%s' "$out" | jq -r --arg p "$path" '.entries[] | select(.path == $p) | (.mode // 0)' | tail -1)
      if [ "$got" = "$want" ]; then
        printf '%s' "$got"
        return 0
      fi
    elif [[ "$out" != *"Too Many Requests"* && "$out" != *"HTTP 429"* && "$out" != *"not found"* ]]; then
      printf '%s\n' "$out" >&2
      return 1
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      printf 'wait_layer_diff_file_mode: timeout layer=%s path=%s want=%s got=%s\n' "$layer_ref" "$path" "$want" "${got:-<none>}" >&2
      return 1
    fi
    sleep "$LAYER_DIFF_INTERVAL_S"
  done
}

put_layer_entry() {
  local layer_ref="$1"
  local path="$2"
  local op="$3"
  local kind="$4"
  local text="${5:-}"
  local mode="${6:-420}"
  local content_b64=""
  local body resp code ref_escaped

  if [ "$op" = "upsert" ] || [ "$op" = "symlink" ] || [ "$op" = "rename" ]; then
    content_b64=$(printf '%s' "$text" | base64 | tr -d '\n')
  fi
  body=$(jq -n \
    --arg path "$path" \
    --arg op "$op" \
    --arg kind "$kind" \
    --argjson mode "$mode" \
    --arg content "$content_b64" \
    --arg text "$text" \
    '{path:$path, op:$op, kind:$kind, mode:$mode}
      + (if $content != "" then {content:$content, content_text:$text} else {} end)')
  ref_escaped=$(url_escape "$layer_ref")
  resp=$(curl_body_code POST "$BASE/v1/layers/$ref_escaped/entries" "$API_KEY" "$body")
  code=$(http_code "$resp")
  check_eq "upsert layer entry $path via $layer_ref" "$code" "200"
}

put_layer_entry_expect_code() {
  local desc="$1"
  local want_code="$2"
  local layer_ref="$3"
  local path="$4"
  local op="$5"
  local kind="$6"
  local text="${7:-}"
  local ref_escaped content_b64 body resp
  content_b64=$(printf '%s' "$text" | base64 | tr -d '\n')
  body=$(jq -n \
    --arg path "$path" \
    --arg op "$op" \
    --arg kind "$kind" \
    --arg content "$content_b64" \
    --arg text "$text" \
    '{path:$path, op:$op, kind:$kind, mode:420}
      + (if $content != "" then {content:$content, content_text:$text} else {} end)')
  ref_escaped=$(url_escape "$layer_ref")
  resp=$(curl_body_code POST "$BASE/v1/layers/$ref_escaped/entries" "$API_KEY" "$body")
  check_eq "$desc" "$(http_code "$resp")" "$want_code"
}

require_layer_fuse_prereqs() {
  if [ "$RUN_LAYER_FUSE_SMOKE" != "1" ]; then
    skip_layer_fuse "set RUN_LAYER_FUSE_SMOKE=1 to run real FUSE layer restore coverage"
    return 1
  fi
  case "$(uname -s)" in
    Linux|Darwin) ;;
    *)
      fail_layer_fuse_prereq "unsupported OS: $(uname -s)"
      return 1
      ;;
  esac
  if [ "$(uname -s)" = "Linux" ]; then
    if ! command -v fusermount >/dev/null 2>&1 && ! command -v fusermount3 >/dev/null 2>&1; then
      fail_layer_fuse_prereq "fusermount/fusermount3 is required"
      return 1
    fi
    if [ ! -e /dev/fuse ]; then
      fail_layer_fuse_prereq "/dev/fuse not available"
      return 1
    fi
  fi
  return 0
}

trap stop_mount EXIT

echo "=== drive9 layer filesystem smoke test ==="
echo "BASE=$BASE"
echo "RUN_LAYER_FUSE_SMOKE=$RUN_LAYER_FUSE_SMOKE"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
check_cmd "python3 is available" bash -c 'command -v python3 >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
fi

if [ -z "$API_KEY" ]; then
  provision_key
else
  echo "Using existing DRIVE9_API_KEY"
fi

prepare_cli_binary
check_cmd "drive9 binary ready" test -x "$CLI_BIN"

ts="$(date +%s)"
root="/layer-smoke-${ts}"
base_file="${root}/base.txt"
new_file="${root}/new.txt"
extra_file="${root}/extra.txt"
api_dir="${root}/api-dir"
api_dir_file="${api_dir}/nested.txt"
delete_file="${root}/delete-me.txt"
delete_dir="${root}/delete-dir"
delete_dir_file="${delete_dir}/gone.txt"
empty_delete_dir="${root}/empty-delete-dir"
rename_src="${root}/rename-src.txt"
rename_dst="${root}/rename-dst.txt"
symlink_path="${root}/api-link"
conflict_file="${root}/conflict.txt"
rollback_file="${root}/rollback-only.txt"
base_local="/tmp/drive9-layer-base-${ts}.txt"
delete_local="/tmp/drive9-layer-delete-${ts}.txt"
rename_local="/tmp/drive9-layer-rename-${ts}.txt"
conflict_local="/tmp/drive9-layer-conflict-${ts}.txt"
conflict_mutated_local="/tmp/drive9-layer-conflict-mutated-${ts}.txt"
layer_name="layer-smoke-${ts}"
rollback_name="layer-smoke-rollback-${ts}"
conflict_name="layer-smoke-conflict-${ts}"
dir_conflict_name="layer-smoke-dir-conflict-${ts}"
cli_layer_name="layer-smoke-cli-${ts}"
unique_tag="unique_${ts}"
ckpt_id="ckpt_smoke_${ts}"

printf 'base before layer %s\n' "$ts" >"$base_local"
printf 'delete file before layer %s\n' "$ts" >"$delete_local"
printf 'rename source before layer %s\n' "$ts" >"$rename_local"
printf 'conflict base before layer %s\n' "$ts" >"$conflict_local"
printf 'conflict base changed outside layer %s\n' "$ts" >"$conflict_mutated_local"
drive9_retry fs mkdir "$root" >/dev/null
drive9_retry fs mkdir "$delete_dir" >/dev/null
drive9_retry fs mkdir "$empty_delete_dir" >/dev/null
drive9_retry fs cp "$base_local" ":$base_file" >/dev/null
drive9_retry fs cp "$delete_local" ":$delete_file" >/dev/null
drive9_retry fs cp "$delete_local" ":$delete_dir_file" >/dev/null
drive9_retry fs cp "$rename_local" ":$rename_src" >/dev/null
drive9_retry fs cp "$conflict_local" ":$conflict_file" >/dev/null
check_eq "base file visible before layer" "$(drive9_retry fs cat "$base_file")" "$(cat "$base_local")"

layer_json=$(drive9_retry fs layer create \
  --name "$layer_name" \
  --tag suite=layer-smoke \
  --tag "run=$ts" \
  --tag "${unique_tag}=1" \
  --json \
  ":$root")
layer_id=$(printf '%s' "$layer_json" | jq -r '.layer_id // empty')
check_cmd "layer create returns id" test -n "$layer_id"

status_by_name=$(drive9_retry fs layer status --json "$layer_name")
check_eq "layer status by name resolves id" "$(printf '%s' "$status_by_name" | jq -r '.layer_id')" "$layer_id"

status_by_tag_value=$(drive9_retry fs layer status --json "tag:run=$ts")
check_eq "layer status by tag value resolves id" "$(printf '%s' "$status_by_tag_value" | jq -r '.layer_id')" "$layer_id"

status_by_tag_key=$(drive9_retry fs layer status --json "tag:$unique_tag")
check_eq "layer status by tag key resolves id" "$(printf '%s' "$status_by_tag_key" | jq -r '.layer_id')" "$layer_id"

put_layer_entry "$layer_name" "$base_file" "upsert" "file" "base edited in layer ${ts}"
put_layer_entry "tag:$unique_tag" "$new_file" "upsert" "file" "new file from layer ${ts}"

diff_json=$(drive9_retry fs layer diff --json "tag:run=$ts")
diff_count=$(printf '%s' "$diff_json" | jq '.entries | length')
check_eq "layer diff shows two entries" "$diff_count" "2"
check_eq "base entry content_text present in diff" "$(printf '%s' "$diff_json" | jq -r --arg p "$base_file" '.entries[] | select(.path==$p) | .content_text')" "base edited in layer ${ts}"
check_eq "layer list resolves named layer" "$(drive9_retry fs layer list --json | jq -r --arg id "$layer_id" '[.layers[] | select(.layer_id==$id)] | length')" "1"

checkpoint_json=$(drive9_retry fs layer checkpoint --id "$ckpt_id" --label before-extra --json "$layer_name")
check_eq "checkpoint uses requested id" "$(printf '%s' "$checkpoint_json" | jq -r '.checkpoint_id')" "$ckpt_id"
check_eq "checkpoint durable seq captures first two entries" "$(printf '%s' "$checkpoint_json" | jq -r '.durable_seq')" "2"

checkpoint_ref=$(url_escape "$ckpt_id")
checkpoint_resp=$(curl_body_code GET "$BASE/v1/layer-checkpoints/$checkpoint_ref" "$API_KEY")
check_eq "GET checkpoint returns 200" "$(http_code "$checkpoint_resp")" "200"
check_eq "GET checkpoint resolves layer id" "$(json_body "$checkpoint_resp" | jq -r '.layer_id')" "$layer_id"

put_layer_entry "$layer_name" "$extra_file" "upsert" "file" "extra after checkpoint ${ts}"
put_layer_entry "$layer_name" "$new_file" "chmod" "file" "" 384
put_layer_entry "$layer_name" "${api_dir}/" "mkdir" "dir" "" 493
put_layer_entry "$layer_name" "${api_dir}/" "chmod" "dir" "" 448
put_layer_entry "$layer_name" "$api_dir_file" "upsert" "file" "nested after checkpoint ${ts}"
put_layer_entry "$layer_name" "$delete_file" "whiteout" "file"
put_layer_entry "$layer_name" "${empty_delete_dir}/" "whiteout" "dir"
put_layer_entry "$layer_name" "$symlink_path" "symlink" "symlink" "base.txt" 41471
put_layer_entry "$layer_name" "$rename_src" "upsert" "file" "rename source edited in layer ${ts}"
put_layer_entry "$layer_name" "$rename_src" "rename" "file" "$rename_dst"
put_layer_entry_expect_code "entry outside base root is rejected" "400" "$layer_name" "/outside-layer-${ts}/owned.txt" "upsert" "file" "owned"
diff_after_extra=$(drive9_retry fs layer diff --json "$layer_id")
check_eq "layer diff shows full API entry set after checkpoint" "$(printf '%s' "$diff_after_extra" | jq '.entries | length')" "9"
diff_at_checkpoint=$(curl_body_code GET "$BASE/v1/layers/$(url_escape "$layer_id")/diff?max_seq=2" "$API_KEY")
check_eq "layer diff max_seq returns 200" "$(http_code "$diff_at_checkpoint")" "200"
check_eq "layer diff max_seq excludes post-checkpoint entries" "$(json_body "$diff_at_checkpoint" | jq '.entries | length')" "2"

rollback_json=$(drive9_retry fs layer create \
  --name "$rollback_name" \
  --tag "rollback_run=$ts" \
  --json \
  ":$root")
rollback_id=$(printf '%s' "$rollback_json" | jq -r '.layer_id // empty')
check_cmd "rollback layer create returns id" test -n "$rollback_id"
put_layer_entry "$rollback_name" "$rollback_file" "upsert" "file" "rollback only ${ts}"
check_eq "rollback command returns ok" "$(drive9_retry fs layer rollback "tag:rollback_run=$ts")" "ok"
rollback_status=$(drive9_retry fs layer status --json "$rollback_name")
check_eq "rollback layer state is abandoned" "$(printf '%s' "$rollback_status" | jq -r '.state')" "abandoned"
check_cmd_fail "abandoned layer file is not visible in base" drive9 fs cat "$rollback_file"

conflict_json=$(drive9_retry fs layer create \
  --name "$conflict_name" \
  --tag "conflict_run=$ts" \
  --json \
  ":$root")
conflict_id=$(printf '%s' "$conflict_json" | jq -r '.layer_id // empty')
check_cmd "conflict layer create returns id" test -n "$conflict_id"
put_layer_entry "$conflict_name" "$conflict_file" "upsert" "file" "conflict layer edit ${ts}"
drive9_retry fs cp "$conflict_mutated_local" ":$conflict_file" >/dev/null
conflict_commit_resp=$(curl_body_code POST "$BASE/v1/layers/$(url_escape "tag:conflict_run=$ts")/commit" "$API_KEY" "{}")
check_eq "conflicting commit returns 409" "$(http_code "$conflict_commit_resp")" "409"
check_eq "conflicting commit reports base revision changed" "$(json_body "$conflict_commit_resp" | jq -r '.conflicts[0].reason')" "base revision changed"
conflict_status=$(drive9_retry fs layer status --json "$conflict_id")
check_eq "conflicting layer state is conflicted" "$(printf '%s' "$conflict_status" | jq -r '.state')" "conflicted"
check_eq "conflicting commit preserves mutated base" "$(drive9_retry fs cat "$conflict_file")" "$(cat "$conflict_mutated_local")"

dir_conflict_json=$(drive9_retry fs layer create \
  --name "$dir_conflict_name" \
  --tag "dir_conflict_run=$ts" \
  --json \
  ":$root")
dir_conflict_id=$(printf '%s' "$dir_conflict_json" | jq -r '.layer_id // empty')
check_cmd "directory whiteout conflict layer create returns id" test -n "$dir_conflict_id"
put_layer_entry "$dir_conflict_name" "${delete_dir}/" "whiteout" "dir"
dir_conflict_commit_resp=$(curl_body_code POST "$BASE/v1/layers/$(url_escape "tag:dir_conflict_run=$ts")/commit" "$API_KEY" "{}")
check_eq "non-empty directory whiteout returns 409" "$(http_code "$dir_conflict_commit_resp")" "409"
check_eq "non-empty directory whiteout reports reason" "$(json_body "$dir_conflict_commit_resp" | jq -r '.conflicts[0].reason')" "directory whiteout requires empty directory"
check_eq "non-empty directory whiteout preserves child" "$(drive9_retry fs cat "$delete_dir_file")" "$(cat "$delete_local")"

commit_out=$(drive9_retry fs layer commit "tag:$unique_tag")
case "$commit_out" in
  committed\ layer="$layer_id"\ applied=12) commit_status="ok" ;;
  *) commit_status="$commit_out" ;;
esac
check_eq "commit by tag key succeeds" "$commit_status" "ok"

committed_status=$(drive9_retry fs layer status --json "$layer_id")
check_eq "committed layer state is committed" "$(printf '%s' "$committed_status" | jq -r '.state')" "committed"
check_eq "base file updated after commit" "$(drive9_retry fs cat "$base_file")" "base edited in layer ${ts}"
check_eq "new file visible after commit" "$(drive9_retry fs cat "$new_file")" "new file from layer ${ts}"
check_eq "extra file visible after commit" "$(drive9_retry fs cat "$extra_file")" "extra after checkpoint ${ts}"
check_eq "nested mkdir/upsert visible after commit" "$(drive9_retry fs cat "$api_dir_file")" "nested after checkpoint ${ts}"
check_cmd_fail "whiteout file removed after commit" drive9 fs cat "$delete_file"
check_cmd_fail "whiteout empty directory removed after commit" drive9 fs stat "$empty_delete_dir"
check_cmd_fail "rename source removed after commit" drive9 fs cat "$rename_src"
check_eq "rename target uses layered upsert content after commit" "$(drive9_retry fs cat "$rename_dst")" "rename source edited in layer ${ts}"

echo "[cli-layer] explicit --layer write/search/large-file coverage"
cli_layer_json=$(drive9_retry fs layer create \
  --name "$cli_layer_name" \
  --tag "cli_layer_run=$ts" \
  --json \
  ":$root")
cli_layer_id=$(printf '%s' "$cli_layer_json" | jq -r '.layer_id // empty')
check_cmd "CLI layer create returns id" test -n "$cli_layer_id"
cli_small_local="/tmp/drive9-layer-cli-small-${ts}.txt"
cli_large_local="/tmp/drive9-layer-cli-large-${ts}.bin"
cli_large_download="/tmp/drive9-layer-cli-large-downloaded-${ts}.bin"
cli_small_remote="${root}/cli-layer-small.txt"
cli_large_remote="${root}/cli-layer-large.bin"
cli_dir_remote="${root}/cli-layer-dir"
printf 'cli layer needle %s\n' "$ts" >"$cli_small_local"
python3 - "$cli_large_local" "$LAYER_CLI_LARGE_FILE_MB" "$ts" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
mb = int(sys.argv[2])
seed = f"drive9 layer large {sys.argv[3]}\n".encode()
block = (seed * ((1024 * 1024 // len(seed)) + 1))[:1024 * 1024]
with path.open("wb") as f:
    for _ in range(mb):
        f.write(block)
    f.write(b"drive9-layer-large-tail\n")
PY
drive9_retry fs cp --layer "$cli_layer_name" "$cli_small_local" ":$cli_small_remote" >/dev/null
drive9_retry fs cp --layer "$cli_layer_name" "$cli_large_local" ":$cli_large_remote" >/dev/null
drive9_retry fs mkdir --layer "$cli_layer_name" "$cli_dir_remote" >/dev/null
check_cmd_fail "CLI --layer small file is not visible in base before commit" drive9 fs cat "$cli_small_remote"
grep_layer_out=$(drive9_retry fs grep --layer "$cli_layer_name" "cli layer needle" "$root")
check_eq "CLI grep --layer sees overlay file" "$(printf '%s\n' "$grep_layer_out" | grep -F "$cli_small_remote" | wc -l | tr -d ' ')" "1"
find_layer_out=$(drive9_retry fs find --layer "$cli_layer_name" "$root")
check_eq "CLI find --layer sees overlay file" "$(printf '%s\n' "$find_layer_out" | grep -F "$cli_small_remote" | wc -l | tr -d ' ')" "1"
cli_commit_out=$(drive9_retry fs layer commit "tag:cli_layer_run=$ts")
case "$cli_commit_out" in
  committed\ layer="$cli_layer_id"\ applied=3) cli_commit_status="ok" ;;
  *) cli_commit_status="$cli_commit_out" ;;
esac
check_eq "commit CLI --layer writes succeeds" "$cli_commit_status" "ok"
check_eq "CLI --layer small file visible after commit" "$(drive9_retry fs cat "$cli_small_remote")" "$(cat "$cli_small_local")"
drive9_retry fs cp ":$cli_large_remote" "$cli_large_download" >/dev/null
check_eq "CLI --layer large file hash after commit" "$(sha256_file "$cli_large_download")" "$(sha256_file "$cli_large_local")"

if require_layer_fuse_prereqs; then
  echo "[fuse] layer mount restore coverage"
  fuse_root="/layer-fuse-${ts}"
  fuse_layer_name="layer-fuse-${ts}"
  fuse_other_layer_name="layer-fuse-other-${ts}"
  fuse_ckpt_id="ckpt_fuse_${ts}"
  fuse_other_ckpt_id="ckpt_fuse_other_${ts}"
  fuse_base_local="/tmp/drive9-layer-fuse-base-${ts}.txt"
  fuse_delete_local="/tmp/drive9-layer-fuse-delete-${ts}.txt"
  fuse_move_local="/tmp/drive9-layer-fuse-move-${ts}.txt"
  printf 'fuse base before layer %s\n' "$ts" >"$fuse_base_local"
  printf 'fuse delete before layer %s\n' "$ts" >"$fuse_delete_local"
  printf 'fuse move before layer %s\n' "$ts" >"$fuse_move_local"

  drive9_retry fs mkdir "$fuse_root" >/dev/null
  drive9_retry fs cp "$fuse_base_local" ":${fuse_root}/base.txt" >/dev/null
  drive9_retry fs cp "$fuse_delete_local" ":${fuse_root}/delete.txt" >/dev/null
  drive9_retry fs cp "$fuse_move_local" ":${fuse_root}/move.txt" >/dev/null

  fuse_layer_json=$(drive9_retry fs layer create \
    --name "$fuse_layer_name" \
    --tag "fuse_run=$ts" \
    --durability restore-safe \
    --json \
    ":$fuse_root")
  fuse_layer_id=$(printf '%s' "$fuse_layer_json" | jq -r '.layer_id // empty')
  check_cmd "fuse layer create returns id" test -n "$fuse_layer_id"
  fuse_other_layer_json=$(drive9_retry fs layer create \
    --name "$fuse_other_layer_name" \
    --tag "fuse_other_run=$ts" \
    --durability restore-safe \
    --json \
    ":$fuse_root")
  fuse_other_layer_id=$(printf '%s' "$fuse_other_layer_json" | jq -r '.layer_id // empty')
  check_cmd "fuse other layer create returns id" test -n "$fuse_other_layer_id"
  fuse_other_checkpoint_json=$(drive9_retry fs layer checkpoint --id "$fuse_other_ckpt_id" --label fuse-other --json "$fuse_other_layer_name")
  check_eq "fuse other checkpoint resolves other layer id" "$(printf '%s' "$fuse_other_checkpoint_json" | jq -r '.layer_id')" "$fuse_other_layer_id"

  mount_a="${FUSE_MOUNT_ROOT%/}/drive9-layer-a-${ts}"
  local_a="/tmp/drive9-layer-local-a-${ts}"
  log_a="/tmp/drive9-layer-a-${ts}.log"
  start_layer_mount "tag:fuse_run=$ts" "" "$fuse_root" "$mount_a" "$local_a" "$log_a"
  check_eq "layer mount reads base before edit" "$(cat "$mount_a/base.txt")" "$(cat "$fuse_base_local")"
  printf 'fuse base edited in layer %s\n' "$ts" >"$mount_a/base.txt"
  printf 'fuse new file %s\n' "$ts" >"$mount_a/new.txt"
  chmod 0600 "$mount_a/new.txt"
  mkdir "$mount_a/dir"
  printf 'fuse nested file %s\n' "$ts" >"$mount_a/dir/nested.txt"
  rm "$mount_a/delete.txt"
  mv "$mount_a/move.txt" "$mount_a/moved.txt"
  ln -s base.txt "$mount_a/link"
  check_eq "layer diff receives FUSE writes" "$(wait_layer_diff_entries "tag:fuse_run=$ts" "8" \
    "${fuse_root}/base.txt" "upsert" \
    "${fuse_root}/new.txt" "upsert" \
    "${fuse_root}/dir/" "mkdir" \
    "${fuse_root}/dir/nested.txt" "upsert" \
    "${fuse_root}/delete.txt" "whiteout" \
    "${fuse_root}/move.txt" "whiteout" \
    "${fuse_root}/moved.txt" "upsert" \
    "${fuse_root}/link" "symlink")" "8"
  check_eq "layer diff captures FUSE chmod mode" "$(wait_layer_diff_file_mode "tag:fuse_run=$ts" "${fuse_root}/new.txt" "384")" "384"
  fuse_checkpoint_json=$(drive9_retry fs layer checkpoint --id "$fuse_ckpt_id" --label fuse-before-after --json "$fuse_layer_name")
  check_eq "fuse checkpoint resolves layer id" "$(printf '%s' "$fuse_checkpoint_json" | jq -r '.layer_id')" "$fuse_layer_id"
  printf 'fuse after checkpoint %s\n' "$ts" >"$mount_a/after.txt"
  check_eq "layer diff receives post-checkpoint FUSE write" "$(wait_layer_diff_entries "tag:fuse_run=$ts" "9" \
    "${fuse_root}/base.txt" "upsert" \
    "${fuse_root}/new.txt" "upsert" \
    "${fuse_root}/dir/" "mkdir" \
    "${fuse_root}/dir/nested.txt" "upsert" \
    "${fuse_root}/delete.txt" "whiteout" \
    "${fuse_root}/move.txt" "whiteout" \
    "${fuse_root}/moved.txt" "upsert" \
    "${fuse_root}/link" "symlink" \
    "${fuse_root}/after.txt" "upsert")" "9"
  check_cmd "unmount first layer mount" unmount_layer_mount
  check_cmd "first layer mount log clean" audit_mount_log "$log_a"

  mount_mismatch="${FUSE_MOUNT_ROOT%/}/drive9-layer-mismatch-${ts}"
  local_mismatch="/tmp/drive9-layer-local-mismatch-${ts}"
  log_mismatch="/tmp/drive9-layer-mismatch-${ts}.log"
  check_cmd "layer mount rejects checkpoint from another layer" expect_layer_mount_fail "$fuse_layer_name" "$fuse_other_ckpt_id" "$fuse_root" "$mount_mismatch" "$local_mismatch" "$log_mismatch"

  mount_b="${FUSE_MOUNT_ROOT%/}/drive9-layer-b-${ts}"
  local_b="/tmp/drive9-layer-local-b-${ts}"
  log_b="/tmp/drive9-layer-b-${ts}.log"
  start_layer_mount "$fuse_layer_name" "$fuse_ckpt_id" "$fuse_root" "$mount_b" "$local_b" "$log_b"
  check_eq "checkpoint restore keeps edited base" "$(cat "$mount_b/base.txt")" "fuse base edited in layer ${ts}"
  check_eq "checkpoint restore keeps new file" "$(cat "$mount_b/new.txt")" "fuse new file ${ts}"
  check_eq "checkpoint restore keeps new file mode" "$(stat -c %a "$mount_b/new.txt" 2>/dev/null || stat -f %Lp "$mount_b/new.txt")" "600"
  check_eq "checkpoint restore keeps nested file" "$(cat "$mount_b/dir/nested.txt")" "fuse nested file ${ts}"
  check_cmd_fail "checkpoint restore keeps whiteout" test -e "$mount_b/delete.txt"
  check_cmd_fail "checkpoint restore hides rename source" test -e "$mount_b/move.txt"
  check_eq "checkpoint restore keeps rename target" "$(cat "$mount_b/moved.txt")" "$(cat "$fuse_move_local")"
  check_eq "checkpoint restore keeps symlink" "$(readlink_target_for_check "$mount_b/link")" "base.txt"
  check_cmd_fail "checkpoint restore excludes later write" test -e "$mount_b/after.txt"
  check_cmd "unmount checkpoint restore mount" unmount_layer_mount
  check_cmd "checkpoint restore mount log clean" audit_mount_log "$log_b"

  mount_c="${FUSE_MOUNT_ROOT%/}/drive9-layer-c-${ts}"
  local_c="/tmp/drive9-layer-local-c-${ts}"
  log_c="/tmp/drive9-layer-c-${ts}.log"
  start_layer_mount "tag:fuse_run=$ts" "" "$fuse_root" "$mount_c" "$local_c" "$log_c"
  check_eq "fresh restore includes post-checkpoint write" "$(cat "$mount_c/after.txt")" "fuse after checkpoint ${ts}"
  check_cmd "unmount full restore mount" unmount_layer_mount
  check_cmd "full restore mount log clean" audit_mount_log "$log_c"

  fuse_commit_out=$(drive9_retry fs layer commit "tag:fuse_run=$ts")
  case "$fuse_commit_out" in
    committed\ layer="$fuse_layer_id"\ applied=*)
      fuse_commit_applied="${fuse_commit_out##*applied=}"
      if [ "$fuse_commit_applied" -ge 9 ] 2>/dev/null; then
        fuse_commit_status="ok"
      else
        fuse_commit_status="$fuse_commit_out"
      fi
      ;;
    *) fuse_commit_status="$fuse_commit_out" ;;
  esac
  check_eq "commit FUSE layer after restore succeeds" "$fuse_commit_status" "ok"
  check_eq "committed FUSE base visible" "$(drive9_retry fs cat "${fuse_root}/base.txt")" "fuse base edited in layer ${ts}"
  check_eq "committed FUSE after file visible" "$(drive9_retry fs cat "${fuse_root}/after.txt")" "fuse after checkpoint ${ts}"
  check_cmd_fail "committed FUSE whiteout visible in base" drive9 fs cat "${fuse_root}/delete.txt"
  check_cmd_fail "committed FUSE rename source visible in base" drive9 fs cat "${fuse_root}/move.txt"
  check_eq "committed FUSE rename target visible" "$(drive9_retry fs cat "${fuse_root}/moved.txt")" "$(cat "$fuse_move_local")"

  # ------------------------------------------------------------------
  # [fuse] layer rollback hot-refresh: a mounted layer should revert to
  # the base view after `fs layer rollback` WITHOUT requiring a remount.
  # ------------------------------------------------------------------
  rollback_fuse_root="/layer-rollback-fuse-${ts}"
  rollback_fuse_layer="layer-rollback-fuse-${ts}"
  rollback_fuse_base_local="/tmp/drive9-layer-rollback-base-${ts}.txt"
  printf 'rollback fuse base %s\n' "$ts" >"$rollback_fuse_base_local"
  drive9_retry fs mkdir "$rollback_fuse_root" >/dev/null
  drive9_retry fs cp "$rollback_fuse_base_local" ":${rollback_fuse_root}/base.txt" >/dev/null

  rollback_fuse_layer_json=$(drive9_retry fs layer create \
    --name "$rollback_fuse_layer" \
    --tag "rollback_fuse_run=$ts" \
    --durability restore-safe \
    --json \
    ":$rollback_fuse_root")
  rollback_fuse_layer_id=$(printf '%s' "$rollback_fuse_layer_json" | jq -r '.layer_id // empty')
  check_cmd "rollback fuse layer create returns id" test -n "$rollback_fuse_layer_id"

  mount_rb="${FUSE_MOUNT_ROOT%/}/drive9-layer-rollback-${ts}"
  local_rb="/tmp/drive9-layer-local-rollback-${ts}"
  log_rb="/tmp/drive9-layer-rollback-${ts}.log"
  start_layer_mount "tag:rollback_fuse_run=$ts" "" "$rollback_fuse_root" "$mount_rb" "$local_rb" "$log_rb"
  check_eq "rollback fuse mount reads base before edit" "$(cat "$mount_rb/base.txt")" "$(cat "$rollback_fuse_base_local")"

  # Write a file through the layer mount — it should be visible via the overlay.
  printf 'rollback fuse new file %s\n' "$ts" >"$mount_rb/new.txt"
  check_eq "rollback fuse new file visible in layer mount" "$(cat "$mount_rb/new.txt")" "rollback fuse new file ${ts}"

  # Roll back the layer from a separate CLI call (not through the mount).
  check_eq "rollback fuse command returns ok" "$(drive9_retry fs layer rollback "tag:rollback_fuse_run=$ts")" "ok"
  rollback_fuse_status=$(drive9_retry fs layer status --json "$rollback_fuse_layer")
  check_eq "rollback fuse layer state is abandoned" "$(printf '%s' "$rollback_fuse_status" | jq -r '.state')" "abandoned"

  # Without remounting, the layer overlay should clear within the watcher
  # poll interval (~1s). Poll up to LAYER_DIFF_TIMEOUT_S for the new file
  # to become unreadable (base view restored). We check readability rather
  # than existence because the kernel dentry cache may retain a stale
  # entry for the overlaid file — but the overlay is gone, so reading
  # returns EIO (the file does not exist in the base view).
  rollback_hot_refresh_ok="no"
  rollback_poll_elapsed=0
  while [ "$rollback_poll_elapsed" -lt "$LAYER_DIFF_TIMEOUT_S" ]; do
    if ! cat "$mount_rb/new.txt" >/dev/null 2>&1; then
      rollback_hot_refresh_ok="yes"
      break
    fi
    sleep "$LAYER_DIFF_INTERVAL_S"
    rollback_poll_elapsed=$((rollback_poll_elapsed + LAYER_DIFF_INTERVAL_S))
  done
  check_eq "rollback fuse overlay clears without remount (new.txt disappears)" "$rollback_hot_refresh_ok" "yes"

  # Base file should still be visible after rollback.
  check_eq "rollback fuse base still visible after hot refresh" "$(cat "$mount_rb/base.txt")" "$(cat "$rollback_fuse_base_local")"

  # A new write through the now-abandoned mount should fail with ESTALE.
  rollback_write_err=""
  if printf 'should fail %s\n' "$ts" >"$mount_rb/post-rollback.txt" 2>/dev/null; then
    rollback_write_err="success"
  else
    rollback_write_err="failure"
  fi
  check_eq "rollback fuse new write after rollback fails (ESTALE)" "$rollback_write_err" "failure"

  check_cmd "unmount rollback fuse mount" unmount_layer_mount
  check_cmd "rollback fuse mount log clean" audit_mount_log "$log_rb"
fi

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
