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
FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-15s}"
LAYER_CLEAN_UMOUNT_MAX_S="${LAYER_CLEAN_UMOUNT_MAX_S:-10}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-20}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
LAYER_DIFF_TIMEOUT_S="${LAYER_DIFF_TIMEOUT_S:-20}"
LAYER_DIFF_INTERVAL_S="${LAYER_DIFF_INTERVAL_S:-1}"
LAYER_FUSE_LOG_AUDIT_PATTERN="${LAYER_FUSE_LOG_AUDIT_PATTERN:-panic|fatal error|Resource temporarily unavailable|restore fs layer entries failed|force-quit|event=exit code=[1-9]|shutdown incomplete}"
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
  # Unset 5th arg keeps the historical JSON default; pass "" to omit Content-Type
  # (raw PUT /v1/fs bodies are not JSON).
  local content_type="${5-application/json}"
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
  local durability="${7:-write-sync}"
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

  local profile="${FUSE_PROFILE:-coding-agent}"
  local args=(mount --foreground --mode=fuse --profile="$profile" --durability="$durability" --flush-debounce=0 --layer "$layer_ref")
  case "$profile" in
    none|extent|interactive) ;;
    *) args+=(--local-root "$local_root") ;;
  esac
  if [ -n "$checkpoint_ref" ]; then
    args+=(--checkpoint "$checkpoint_ref")
  fi
  args+=(":$remote_root" "$MOUNT_POINT")
  drive9 "${args[@]}" >>"$MOUNT_LOG" 2>&1 &
  MOUNT_PID="$!"
  if ! wait_mount_state mounted || ! wait_mount_log_ready "$MOUNT_LOG"; then
    echo "FAIL layer mount did not become ready: $MOUNT_POINT" >&2
    tail -n 80 "$MOUNT_LOG" >&2
    return 1
  fi
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

  local args=(mount --foreground --mode=fuse --profile="${FUSE_PROFILE:-coding-agent}" --local-root "$local_root" --durability=write-sync --flush-debounce=0 --layer "$layer_ref" --checkpoint "$checkpoint_ref" ":$remote_root" "$mount_point")
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
	local cli_status=0 worker_status=0 started=$SECONDS deadline
	if [[ -z "${MOUNT_POINT:-}" ]]; then
		return 0
	fi
	if is_mounted_path "$MOUNT_POINT"; then
		drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT" || cli_status=$?
	fi
	wait_mount_state unmounted || return 1
	if [[ -n "${MOUNT_PID:-}" ]]; then
		deadline=$((SECONDS + LAYER_CLEAN_UMOUNT_MAX_S))
		while kill -0 "$MOUNT_PID" 2>/dev/null; do
			if ((SECONDS >= deadline)); then
				echo "layer worker did not exit after unmount" >&2
				return 1
			fi
			sleep 0.1
		done
		wait "$MOUNT_PID" || worker_status=$?
		MOUNT_PID=""
	fi
	if ((cli_status != 0 || worker_status != 0)); then
		echo "layer unmount failed: cli=$cli_status worker=$worker_status" >&2
		return 1
	fi
	if ((SECONDS - started >= LAYER_CLEAN_UMOUNT_MAX_S)); then
		echo "clean layer unmount exceeded ${LAYER_CLEAN_UMOUNT_MAX_S}s" >&2
		return 1
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

wait_mounted_layer_file() {
	python3 - "$1" "$2" "$LAYER_DIFF_TIMEOUT_S" <<'PY_LAYER_REFRESH'
import subprocess
import sys
import time

path, expected, timeout = sys.argv[1:]
deadline = time.monotonic() + float(timeout)
while time.monotonic() < deadline:
    command = ["test", "-e", path] if expected == "absent" else ["cat", path]
    try:
        result = subprocess.run(command, capture_output=True, timeout=2)
        if expected == "absent":
            if result.returncode == 1:
                sys.exit(0)
        elif result.returncode == 0 and result.stdout.decode().rstrip("\n") == expected:
            sys.exit(0)
    except subprocess.TimeoutExpired:
        pass
    time.sleep(0.1)
raise SystemExit(f"mounted Layer did not refresh {path}: expected {expected!r}")
PY_LAYER_REFRESH
}

# ---------------------------------------------------------------------------
# FS-native layer contract: the data plane is the /.drive9/layers/<id>/ tree
# (plain files through the ordinary pipeline); metadata stays on the server's
# layer registry; commit is a client-side journalled 2PC over generic fs CAS
# writes; mounts shadow the classic view copy-on-write and can hot-switch
# with `drive9 fs layer checkout`.
# ---------------------------------------------------------------------------

main() {
  local ts rc
  ts="$(date +%s)"

  prepare_cli_binary
  provision_key

  local root="/lfs-$ts"
  drive9_retry fs mkdir ":$root" >/dev/null

  # --- CLI data plane: layer writes are ordinary tree files ----------------
  local base_file="$root/base.txt"
  printf 'base v1' >"$CLI_HOME/base.txt"
  drive9_retry fs cp "$CLI_HOME/base.txt" ":$base_file" >/dev/null

  local cli_layer_name="cli-layer-$ts"
  local cli_layer_id
  cli_layer_id=$(drive9_retry fs layer create --name "$cli_layer_name" ":$root")
  check_cmd "fs layer create prints an id" test -n "$cli_layer_id"

  # cp --layer lands in the tree; the classic view stays untouched
  printf 'layered small' >"$CLI_HOME/small.txt"
  drive9_retry fs cp --layer "$cli_layer_name" "$CLI_HOME/small.txt" ":$root/small.txt" >/dev/null
  local classic_small
  classic_small=$(drive9_retry fs cat ":$root/small.txt" 2>/dev/null || true)
  check_eq "cp --layer does not touch the classic view" "$classic_small" ""
  local tree_small
  tree_small=$(drive9_retry fs cat ":/.drive9/layers/$cli_layer_id/tree$root/small.txt" 2>/dev/null || true)
  check_eq "cp --layer lands in the layer tree" "$tree_small" "layered small"

  # a big file rides the multipart upload path (well past the server's
  # negotiated large-file threshold)
  local big_mb=12
  dd if=/dev/urandom of="$CLI_HOME/big.bin" bs=1m count="$big_mb" >/dev/null 2>&1 || dd if=/dev/urandom of="$CLI_HOME/big.bin" bs=1M count="$big_mb" >/dev/null 2>&1
  local big_sha
  big_sha=$(sha256_file "$CLI_HOME/big.bin")
  drive9_retry fs cp --layer "$cli_layer_name" "$CLI_HOME/big.bin" ":$root/big.bin" >/dev/null
  # byte-exact readback via cp + cmp (no pipe/sha-on-stdin variables)
  drive9_retry fs cp ":/.drive9/layers/$cli_layer_id/tree$root/big.bin" "$CLI_HOME/big.down" >/dev/null
  local tree_big_verdict
  if cmp -s "$CLI_HOME/big.bin" "$CLI_HOME/big.down"; then tree_big_verdict="ok"; else
    tree_big_verdict="differ(want=$(wc -c <"$CLI_HOME/big.bin" | tr -d ' ') got=$(wc -c <"$CLI_HOME/big.down" | tr -d ' '))"
  fi
  check_eq "large layer upload streams intact" "$tree_big_verdict" "ok"

  # diff folds the tree
  local diff_json
  diff_json=$(drive9_retry fs layer diff --json "$cli_layer_name")
  check_eq "layer diff counts tree entries" "$(printf '%s' "$diff_json" | jq '[.entries[] | select(.kind=="file")] | length')" "2"
  check_eq "layer diff shows the small upsert" "$(printf '%s' "$diff_json" | jq -r --arg p "$root/small.txt" '.entries[] | select(.path == $p and .op == "upsert") | .kind')" "file"

  # rm --layer writes a blackout; the classic file survives
  drive9_retry fs rm --layer "$cli_layer_name" ":$base_file" >/dev/null
  local classic_base
  classic_base=$(drive9_retry fs cat ":$base_file")
  check_eq "layer rm keeps the classic file" "$classic_base" "base v1"
  local blackout_body
  blackout_body=$(drive9_retry fs cat ":/.drive9/layers/$cli_layer_id/blackouts$base_file")
  check_cmd "blackout marker present in the file whiteout root" test -n "$blackout_body"

  # --- commit: client 2PC applies the tree onto the base -------------------
  local commit_layer="commit-$ts"
  local commit_id
  commit_id=$(drive9_retry fs layer create --name "$commit_layer" ":$root")
  printf 'replaced' >"$CLI_HOME/replaced.txt"
  drive9_retry fs cp --layer "$commit_layer" "$CLI_HOME/replaced.txt" ":$base_file" >/dev/null
  drive9_retry fs rm --layer "$commit_layer" ":$root/small.txt" >/dev/null || true
  drive9_retry fs layer commit "$commit_layer" >/dev/null
  check_eq "commit applied the replacement" "$(drive9_retry fs cat ":$base_file")" "replaced"
  local committed_state
  committed_state=$(drive9_retry fs layer status --json "$commit_layer" | jq -r '.state')
  check_eq "committed layer flips state" "$committed_state" "committed"

  # commit conflict: a stale CAS anchor refuses to clobber the base
  local conflict_layer="conflict-$ts"
  drive9_retry fs layer create --name "$conflict_layer" ":$root" >/dev/null
  printf 'stale layered' >"$CLI_HOME/stale.txt"
  drive9_retry fs cp --layer "$conflict_layer" "$CLI_HOME/stale.txt" ":$base_file" >/dev/null
  # the base moves past the layer's anchor before the commit applies
  printf 'live moved on' >"$CLI_HOME/live.txt"
  drive9_retry fs cp "$CLI_HOME/live.txt" ":$base_file" >/dev/null
  check_cmd_fail "stale anchor commit conflicts" drive9 fs layer commit "$conflict_layer"
  check_eq "conflicted commit leaves the base alone" "$(drive9_retry fs cat ":$base_file")" "live moved on"

  # --- fork: client-side deep copy; the child pins the parent tip ----------
  local fork_parent="forkp-$ts"
  local fork_child="forkc-$ts"
  local fork_parent_id
  fork_parent_id=$(drive9_retry fs layer create --name "$fork_parent" ":$root")
  printf 'parent a' >"$CLI_HOME/a.txt"
  drive9_retry fs cp --layer "$fork_parent" "$CLI_HOME/a.txt" ":$root/a.txt" >/dev/null
  local child_id
  child_id=$(drive9_retry fs layer fork --name "$fork_child" "$fork_parent")
  check_cmd "fs layer fork prints the child id" test -n "$child_id"
  # post-fork parent writes stay invisible to the child
  printf 'parent b' >"$CLI_HOME/b.txt"
  drive9_retry fs cp --layer "$fork_parent" "$CLI_HOME/b.txt" ":$root/b.txt" >/dev/null
  local child_tree_b
  child_tree_b=$(drive9_retry fs cat ":/.drive9/layers/$child_id/tree$root/b.txt" 2>/dev/null || true)
  check_eq "fork pins the parent tip (child misses post-fork write)" "$child_tree_b" ""
  local child_tree_a
  child_tree_a=$(drive9_retry fs cat ":/.drive9/layers/$child_id/tree$root/a.txt")
  check_eq "fork copies the pinned content" "$child_tree_a" "parent a"

  # --- checkpoint: snapshot isolation --------------------------------------
  local ckpt_layer="ckpt-$ts"
  local ckpt_id
  ckpt_id=$(drive9_retry fs layer create --name "$ckpt_layer" ":$root")
  printf 'v1' >"$CLI_HOME/v1.txt"
  drive9_retry fs cp --layer "$ckpt_layer" "$CLI_HOME/v1.txt" ":$root/v.txt" >/dev/null
  local cp_id
  cp_id=$(drive9_retry fs layer checkpoint "$ckpt_layer")
  check_cmd "fs layer checkpoint prints an id" test -n "$cp_id"
  printf 'v2' >"$CLI_HOME/v2.txt"
  drive9_retry fs cp --layer "$ckpt_layer" "$CLI_HOME/v2.txt" ":$root/v.txt" >/dev/null
  local snap2
  snap2=$(drive9_retry fs cat ":/.drive9/layers/$ckpt_id/checkpoints/$cp_id/tree$root/v.txt")
  check_eq "checkpoint snapshot stays pinned" "$snap2" "v1"

  # --- rollback clears the shadow (registry semantics) ---------------------
  local rb_layer="rb-$ts"
  drive9_retry fs layer create --name "$rb_layer" ":$root" >/dev/null
  printf 'rb' >"$CLI_HOME/rb.txt"
  drive9_retry fs cp --layer "$rb_layer" "$CLI_HOME/rb.txt" ":$root/rb.txt" >/dev/null
  drive9_retry fs layer rollback "$rb_layer" >/dev/null
  check_eq "rollback marks the layer abandoned" "$(drive9_retry fs layer status --json "$rb_layer" | jq -r '.state')" "abandoned"

  # --- delete sweeps the tree ----------------------------------------------
  local del_layer="del-$ts"
  local del_id
  del_id=$(drive9_retry fs layer create --name "$del_layer" ":$root")
  printf 'x' >"$CLI_HOME/x.txt"
  drive9_retry fs cp --layer "$del_layer" "$CLI_HOME/x.txt" ":$root/x.txt" >/dev/null
  drive9_retry fs layer delete "$del_layer" >/dev/null
  local del_tree
  del_tree=$(drive9_retry fs cat ":/.drive9/layers/$del_id/tree$root/x.txt" 2>/dev/null || true)
  check_eq "delete sweeps the layer tree" "$del_tree" ""

  # --- grep/find --layer point at the mount -------------------------------
  check_cmd_fail "grep --layer is rejected with mount guidance" drive9 fs grep --layer "$cli_layer_name" needle
  check_cmd_fail "find --layer is rejected with mount guidance" drive9 fs find --layer "$cli_layer_name" "$root"

  # --- FUSE: shadow view, CoW, hot checkout --------------------------------
  if [ "$RUN_LAYER_FUSE_SMOKE" = "1" ]; then
    run_fuse_section "$ts" "$root" "$base_file"
  else
    skip_layer_fuse "RUN_LAYER_FUSE_SMOKE=0"
  fi

  echo
  echo "layer fs smoke: pass=$PASS fail=$FAIL total=$TOTAL"
  if [ "$FAIL" -gt 0 ]; then
    exit 1
  fi
}

run_fuse_section() {
  local ts="$1"
  local root="$2"
  local base_file="$3"

  local fuse_layer="fuse-$ts"
  local fuse_id
  fuse_id=$(drive9_retry fs layer create --name "$fuse_layer" ":$root")

  local mp="$FUSE_MOUNT_ROOT/drive9-layer-$ts"
  local lroot="$FUSE_MOUNT_ROOT/drive9-layer-$ts-local"
  local log="$FUSE_MOUNT_ROOT/drive9-layer-$ts.log"
  trap 'stop_mount; dump_mount_log' EXIT

  start_layer_mount "$fuse_layer" "" "$root" "$mp" "$lroot" "$log"

  # the classic view is visible through the shadow
  check_eq "layer mount reads classic content" "$(cat "$mp/base.txt")" "live moved on"

  # CoW: editing a classic file leaves the classic bytes alone
  printf 'replaced in layer' >"$mp/base.txt"
  sync; sleep 1
  check_eq "classic file untouched by the shadow write" "$(drive9_retry fs cat ":$base_file")" "live moved on"

  # new files land in the tree
  printf 'fresh' >"$mp/fresh.txt"
  sync; sleep 1
  local fuse_tree_fresh
  fuse_tree_fresh=$(drive9_retry fs cat ":/.drive9/layers/$fuse_id/tree$root/fresh.txt" 2>/dev/null || true)
  check_eq "new file lands in the layer tree" "$fuse_tree_fresh" "fresh"

  # unlink hides the name but keeps the classic file
  rm -f "$mp/fresh.txt"
  check_eq "unlink hides the tree file" "$(test -e "$mp/fresh.txt" && echo present || echo absent)" "absent"

  # chmod of a classic-backed file is metadata-only (attrs marker)
  chmod 0700 "$mp/base.txt"
  local attr_mode
  case "$(uname -s)" in
    Darwin) attr_mode=$(stat -f '%Lp' "$mp/base.txt") ;;
    *) attr_mode=$(stat -c '%a' "$mp/base.txt") ;;
  esac
  check_eq "chmod shows through the view" "$attr_mode" "700"

  # rollback hot-refresh: overlay clears, writes fail, classic stays visible
  drive9_retry fs layer rollback "$fuse_layer" >/dev/null
  local deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
  local rb_seen=0
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if printf 'post' >"$mp/post.txt" 2>/dev/null; then
      sleep 1
      continue
    fi
    rb_seen=1
    break
  done
  check_eq "rollback fails later writes (ESTALE)" "$rb_seen" "1"
  check_eq "classic view survives rollback" "$(cat "$mp/base.txt" 2>/dev/null || echo missing)" "live moved on"

  unmount_layer_mount || { dump_mount_log; exit 1; }
  audit_mount_log "$log"

  # --- hot checkout: live mount switches views without umount -------------
  local co_layer="co-$ts"
  local co_id
  co_id=$(drive9_retry fs layer create --name "$co_layer" ":$root")
  printf 'from co layer' >"$CLI_HOME/co.txt"
  drive9_retry fs cp --layer "$co_layer" "$CLI_HOME/co.txt" ":$root/co.txt" >/dev/null

  local mp2="$FUSE_MOUNT_ROOT/drive9-live-$ts"
  local lroot2="$FUSE_MOUNT_ROOT/drive9-live-$ts-local"
  local log2="$FUSE_MOUNT_ROOT/drive9-live-$ts.log"
  MOUNT_POINT="$mp2"; MOUNT_LOG="$log2"
  mkdir -p "$mp2" "$lroot2"
  drive9 mount --foreground --mode=fuse --profile="${FUSE_PROFILE:-coding-agent}" --local-root "$lroot2" --durability=write-sync --flush-debounce=0 ":$root" "$mp2" >>"$log2" 2>&1 &
  MOUNT_PID=$!
  wait_mount_state mounted && wait_mount_log_ready "$log2"

  check_eq "live mount cannot see the layer file" "$(test -e "$mp2/co.txt" && echo present || echo absent)" "absent"

  drive9 fs layer checkout --mount "$mp2" "$co_layer" >/dev/null
  check_eq "hot checkout exposes the layer tree" "$(wait_mounted_layer_file "$mp2/co.txt" "from co layer" >/dev/null && echo ok || echo fail)" "ok"
  printf 'via checkout' >"$mp2/co2.txt"
  sync; sleep 1
  local co_tree2
  co_tree2=$(drive9_retry fs cat ":/.drive9/layers/$co_id/tree$root/co2.txt" 2>/dev/null || true)
  check_eq "post-checkout writes land in the layer tree" "$co_tree2" "via checkout"

  drive9 fs layer checkout --live --mount "$mp2" >/dev/null
  check_eq "checkout --live returns to the classic view" "$(test -e "$mp2/co.txt" && echo present || echo absent)" "absent"

  stop_mount
  audit_mount_log "$log2"
}

main "$@"
