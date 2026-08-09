#!/usr/bin/env bash
# e2e: on-demand git-workspace discovery (DORMANT → ARMED).
#
# Covers docs/design/git-workspace-fuse-poll-optimization.md:
#   G0  no workspace under this RemoteRoot → zero ListGitWorkspaces (refresh=0)
#       incl. FS activity on paths with ".git" segments
#   G1  live --fast arms; next FS op uses git layer; refresh in [1,max]
#   G2  fresh LocalRoot remount discovers via remote index; refresh in [1,max]
#   G3  armed: second --fast forces list (forced_refresh>=2); wall-clock armed
#       idle (no .git) stays within refresh max (catches 1s-TTL poll regression)
#   G4  error-path hardening is unit-tested; e2e bounds refresh via caps
#   AC5 delete suite workspaces + filter index for root → remount refresh=0
#
# Not covered here (no client-visible counter yet): AC1 "index Stat ≤1".
# Tenant isolation: all index/list asserts filter by suite RemoteRoot (shared
# local-dev-key may already have other roots' entries; never require global 404).
#
# Usage:
#   DRIVE9_BASE=http://127.0.0.1:9009 DRIVE9_API_KEY=local-dev-key \
#     bash e2e/git-workspace-ondemand-smoke-test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-20}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
FUSE_MOUNT_ROOT="${FUSE_MOUNT_ROOT:-/tmp}"
FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-60s}"
FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-0}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
GIT_ONDEMAND_CLONE_TIMEOUT_S="${GIT_ONDEMAND_CLONE_TIMEOUT_S:-180}"
GIT_ONDEMAND_GIT_TIMEOUT_S="${GIT_ONDEMAND_GIT_TIMEOUT_S:-120}"
GIT_ONDEMAND_KEEP_ARTIFACTS="${GIT_ONDEMAND_KEEP_ARTIFACTS:-0}"
GIT_ONDEMAND_DORMANT_ACTIVITY_LOOPS="${GIT_ONDEMAND_DORMANT_ACTIVITY_LOOPS:-30}"
# Armed idle: wall-clock seconds of ordinary FS activity (no .git segments).
# Must be long enough that a 1s-TTL poll regression would exceed refresh max.
GIT_ONDEMAND_ARMED_IDLE_S="${GIT_ONDEMAND_ARMED_IDLE_S:-8}"
GIT_ONDEMAND_FIXTURE_TREE_FILES="${GIT_ONDEMAND_FIXTURE_TREE_FILES:-8}"
# Upper bound on ListGitWorkspaces-driven refresh counters after normal scenarios.
# Single --fast + armed idle: event-driven ≈ 1–3 lists; 1s poll over ARMED_IDLE_S
# would be ~ARMED_IDLE_S+1. Cap sits between those (catches poll, absorbs SSE/force).
GIT_ONDEMAND_REFRESH_MAX_SINGLE="${GIT_ONDEMAND_REFRESH_MAX_SINGLE:-6}"
# Two --fast on one mount: arm + second force + optional SSE/index-change extras.
# Observed ~11 forced lists on fast hosts (SSE index notifications); leave headroom
# for CI latency without opening a 1s-poll hole (poll needs wall-clock activity).
GIT_ONDEMAND_REFRESH_MAX_DUAL="${GIT_ONDEMAND_REFRESH_MAX_DUAL:-16}"
GIT_ONDEMAND_LOG_AUDIT_PATTERN="${GIT_ONDEMAND_LOG_AUDIT_PATTERN:-panic|fatal error|short read|input/output error}"
export GIT_ALLOW_PROTOCOL="${GIT_ALLOW_PROTOCOL:-file:https:http:ssh}"

PASS=0
FAIL=0
TOTAL=0
SKIP=0
RUN_ROOT=""
CLI_BIN=""
CLI_HOME=""
API_KEY=""
FIXTURE_ROOT=""
FIXTURE_URL=""
MOUNT_PID=""
MOUNT_POINT=""
MOUNT_LOG=""

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$got" = "$want" ]; then
    echo "PASS $desc (got=$got)"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc (want=$want got=$got)" >&2
    FAIL=$((FAIL + 1))
  fi
}

check_ge() {
  local desc="$1" got="$2" min="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$got" -ge "$min" ] 2>/dev/null; then
    echo "PASS $desc (got=$got >= $min)"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc (got=$got want>=$min)" >&2
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
    echo "FAIL $desc" >&2
    FAIL=$((FAIL + 1))
  fi
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

run_with_timeout() {
  local seconds="$1"
  shift
  python3 - "$seconds" "$@" <<'PY'
import os, signal, subprocess, sys, time
seconds = float(sys.argv[1])
cmd = sys.argv[2:]

def exit_code(rc):
    if rc is None:
        return 124
    if rc < 0:
        return 128 + (-rc)
    return rc

proc = subprocess.Popen(cmd, start_new_session=True)
try:
    rc = proc.wait(timeout=seconds)
    raise SystemExit(exit_code(rc))
except subprocess.TimeoutExpired:
    pass
try:
    os.killpg(proc.pid, signal.SIGTERM)
except ProcessLookupError:
    rc = proc.poll()
    if rc is not None:
        raise SystemExit(exit_code(rc))
    raise SystemExit(124)
deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    if proc.poll() is not None:
        raise SystemExit(124)
    time.sleep(0.2)
try:
    os.killpg(proc.pid, signal.SIGKILL)
except ProcessLookupError:
    pass
proc.wait()
raise SystemExit(124)
PY
}

curl_body_code() {
  local method="$1"
  local url="$2"
  local auth="${3:-}"
  local body_file
  body_file="$(mktemp)"
  local code
  if [ -n "$auth" ]; then
    code=$(curl -sS --max-time 30 -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url" || printf '000')
  else
    code=$(curl -sS --max-time 30 -o "$body_file" -w "%{http_code}" -X "$method" "$url" || printf '000')
  fi
  cat "$body_file"
  echo
  echo "__HTTP__${code}"
  rm -f "$body_file"
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

wait_tenant_active() {
  local deadline state code resp
  deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
  while :; do
    resp=$(curl_body_code GET "$BASE/v1/status" "$API_KEY")
    code=$(http_code "$resp")
    state=$(json_body "$resp" | jq -r '.status // empty')
    echo "status=${code}:${state}"
    if [ "$code" = "200" ] && [ "$state" = "active" ]; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$POLL_INTERVAL_S"
  done
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

drive9() {
  env DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" HOME="$CLI_HOME" "$CLI_BIN" "$@"
}

drive9_with_timeout() {
  local seconds="$1"
  shift
  run_with_timeout "$seconds" env DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" HOME="$CLI_HOME" "$CLI_BIN" "$@"
}

git_cmd() {
  run_with_timeout "$GIT_ONDEMAND_GIT_TIMEOUT_S" git "$@"
}

start_mount() {
  local mount_point="$1"
  local local_root="$2"
  local mount_log="$3"
  local remote_root="$4"
  MOUNT_POINT="$mount_point"
  MOUNT_LOG="$mount_log"
  mkdir -p "$MOUNT_POINT" "$local_root"
  {
    echo "=== drive9 git ondemand mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "remote_root=$remote_root"
    echo "mount_point=$MOUNT_POINT"
    echo "local_root=$local_root"
  } >>"$MOUNT_LOG"

  local perf_dir="${mount_log%.log}.perf"
  # --perf-dir enables fusePerfCounters (via PerfSamplesPath), which emits
  # git_workspace refresh= / forced_refresh= on shutdown.
  drive9 mount --foreground --mode=fuse --profile coding-agent \
    --local-root "$local_root" \
    --durability=interactive \
    --no-auto-unpack \
    --perf-dir "$perf_dir" \
    --perf-interval 1h \
    --perf-cpu-duration 1ms \
    --perf-cpu-interval 1h \
    --perf-heap-interval 1h \
    --perf-max-sample-files 1 \
    --perf-max-profile-files 1 \
    ":$remote_root" "$MOUNT_POINT" >>"$MOUNT_LOG" 2>&1 &
  MOUNT_PID="$!"
  # Propagate readiness failures even when called under check_cmd (set -e suppressed).
  if ! wait_mount_state mounted; then
    echo "mount did not become mounted: $MOUNT_POINT" >&2
    return 1
  fi
  if ! wait_mount_log_ready "$MOUNT_LOG"; then
    echo "mount log never showed ready: $MOUNT_LOG" >&2
    return 1
  fi
  # Give async index probe a moment to settle (404 → dormantConfirmed).
  sleep 1
}

stop_mount() {
  set +e
  if [ -n "${MOUNT_POINT:-}" ] && is_mounted "$MOUNT_POINT"; then
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" --no-auto-pack "$MOUNT_POINT" >/dev/null 2>&1 || true
    wait_mount_state unmounted >/dev/null 2>&1 || true
    if is_mounted "$MOUNT_POINT"; then
      force_unmount "$MOUNT_POINT"
      wait_mount_state unmounted >/dev/null 2>&1 || true
    fi
  fi
  if [ -n "${MOUNT_PID:-}" ] && kill -0 "$MOUNT_PID" >/dev/null 2>&1; then
    # Wait for perf summary (printed late in OnUnmount after drain/checkpoint).
    local i
    for i in $(seq 1 40); do
      if [ -n "${MOUNT_LOG:-}" ] && [ -f "$MOUNT_LOG" ] && grep -q "drive9: perf git_workspace refresh=" "$MOUNT_LOG"; then
        break
      fi
      if ! kill -0 "$MOUNT_PID" >/dev/null 2>&1; then
        break
      fi
      sleep 0.5
    done
    kill "$MOUNT_PID" >/dev/null 2>&1 || true
    wait "$MOUNT_PID" >/dev/null 2>&1 || true
  fi
  # Extra log-flush window if the process already exited without a visible line yet.
  if [ -n "${MOUNT_LOG:-}" ] && [ -f "$MOUNT_LOG" ] && ! grep -q "drive9: perf git_workspace refresh=" "$MOUNT_LOG"; then
    local j
    for j in 1 2 3 4 5 6; do
      if grep -q "drive9: perf git_workspace refresh=" "$MOUNT_LOG"; then
        break
      fi
      sleep 0.5
    done
  fi
  MOUNT_PID=""
  MOUNT_POINT=""
  set -e
}

dump_mount_log() {
  local log_file="$1"
  if [ -f "$log_file" ]; then
    echo "=== drive9 mount log: $log_file ==="
    cat "$log_file"
  fi
}

audit_mount_log() {
  local log_file="$1"
  if [ ! -f "$log_file" ]; then
    echo "mount log missing: $log_file" >&2
    return 1
  fi
  if grep -Eina "$GIT_ONDEMAND_LOG_AUDIT_PATTERN" "$log_file"; then
    echo "mount log contains failure pattern" >&2
    return 1
  fi
  return 0
}

# Parse last git_workspace refresh/forced_refresh counters from mount log.
parse_git_workspace_refresh() {
  local log_file="$1"
  python3 - "$log_file" <<'PY'
import re, sys
log_path = sys.argv[1]
refresh = forced = None
with open(log_path, "r", errors="replace") as handle:
    for line in handle:
        m = re.search(r"drive9: perf git_workspace refresh=(\d+) forced_refresh=(\d+)", line)
        if m:
            refresh, forced = int(m.group(1)), int(m.group(2))
if refresh is None:
    print("MISSING")
    raise SystemExit(2)
print(f"{refresh} {forced}")
PY
}

check_refresh_eq() {
  local desc="$1" log_file="$2" want_refresh="$3"
  TOTAL=$((TOTAL + 1))
  local out
  if ! out=$(parse_git_workspace_refresh "$log_file"); then
    echo "FAIL $desc (perf git_workspace line missing in $log_file)" >&2
    FAIL=$((FAIL + 1))
    return
  fi
  local got_refresh got_forced
  got_refresh=$(echo "$out" | awk '{print $1}')
  got_forced=$(echo "$out" | awk '{print $2}')
  if [ "$got_refresh" = "$want_refresh" ]; then
    echo "PASS $desc (refresh=$got_refresh forced=$got_forced)"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc (refresh=$got_refresh want=$want_refresh forced=$got_forced)" >&2
    FAIL=$((FAIL + 1))
  fi
}

check_refresh_ge() {
  local desc="$1" log_file="$2" min_refresh="$3"
  TOTAL=$((TOTAL + 1))
  local out
  if ! out=$(parse_git_workspace_refresh "$log_file"); then
    echo "FAIL $desc (perf git_workspace line missing in $log_file)" >&2
    FAIL=$((FAIL + 1))
    return
  fi
  local got_refresh got_forced
  got_refresh=$(echo "$out" | awk '{print $1}')
  got_forced=$(echo "$out" | awk '{print $2}')
  if [ "$got_refresh" -ge "$min_refresh" ] 2>/dev/null; then
    echo "PASS $desc (refresh=$got_refresh forced=$got_forced >= $min_refresh)"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc (refresh=$got_refresh forced=$got_forced want>=$min_refresh)" >&2
    FAIL=$((FAIL + 1))
  fi
}

check_refresh_between() {
  local desc="$1" log_file="$2" min_refresh="$3" max_refresh="$4"
  TOTAL=$((TOTAL + 1))
  local out
  if ! out=$(parse_git_workspace_refresh "$log_file"); then
    echo "FAIL $desc (perf git_workspace line missing in $log_file)" >&2
    if [ -f "$log_file" ]; then
      echo "--- mount log tail ($log_file) ---" >&2
      tail -n 40 "$log_file" >&2 || true
    fi
    FAIL=$((FAIL + 1))
    return
  fi
  local got_refresh got_forced
  got_refresh=$(echo "$out" | awk '{print $1}')
  got_forced=$(echo "$out" | awk '{print $2}')
  if [ "$got_refresh" -ge "$min_refresh" ] 2>/dev/null && [ "$got_refresh" -le "$max_refresh" ] 2>/dev/null; then
    echo "PASS $desc (refresh=$got_refresh forced=$got_forced in [$min_refresh,$max_refresh])"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc (refresh=$got_refresh forced=$got_forced want in [$min_refresh,$max_refresh])" >&2
    FAIL=$((FAIL + 1))
  fi
}

check_forced_ge() {
  local desc="$1" log_file="$2" min_forced="$3"
  TOTAL=$((TOTAL + 1))
  local out
  if ! out=$(parse_git_workspace_refresh "$log_file"); then
    echo "FAIL $desc (perf git_workspace line missing in $log_file)" >&2
    FAIL=$((FAIL + 1))
    return
  fi
  local got_forced
  got_forced=$(echo "$out" | awk '{print $2}')
  if [ "$got_forced" -ge "$min_forced" ] 2>/dev/null; then
    echo "PASS $desc (forced_refresh=$got_forced >= $min_forced)"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc (forced_refresh=$got_forced want>=$min_forced)" >&2
    FAIL=$((FAIL + 1))
  fi
}

# Count index entries whose root_path is under remote_root (tenant-absolute prefix).
# Transport failures (000 / non-200-non-404) return non-zero so "empty" asserts cannot
# green on API outage.
index_count_for_root() {
  local remote_root="$1"
  local body code
  code=$(stat_remote_index)
  if [ "$code" = "404" ]; then
    echo "0"
    return 0
  fi
  if [ "$code" = "000" ]; then
    echo "index GET transport failure" >&2
    return 1
  fi
  if [ "$code" != "200" ]; then
    echo "index GET unexpected status $code" >&2
    return 1
  fi
  body=$(fetch_remote_index_body)
  printf '%s' "$body" | jq -r --arg root "$remote_root" '
    [.workspaces // []
     | .[]
     | select(
         ((.root_path // "") == $root)
         or ((.root_path // "") | startswith($root + "/"))
       )
    ] | length
  '
}

# True if remote index has no workspaces whose root_path is under remote_root.
# Tenant-shared index may still exist (other suite roots); do not require 404.
index_has_no_entries_for_root() {
  local remote_root="$1"
  local n
  n=$(index_count_for_root "$remote_root") || return 1
  [ "$n" = "0" ]
}

# Count live ListGitWorkspaces entries under remote_root.
# Non-200 is an error (not "0") so zero-asserts cannot false-green on outage.
list_count_for_root() {
  local remote_root="$1"
  local resp code body
  resp=$(curl_body_code GET "$BASE/v1/git-workspaces" "$API_KEY")
  code=$(http_code "$resp")
  if [ "$code" != "200" ]; then
    echo "ListGitWorkspaces unexpected status $code" >&2
    return 1
  fi
  body=$(json_body "$resp")
  printf '%s' "$body" | jq -r --arg root "$remote_root" '
    [.workspaces // []
     | .[]
     | select(
         ((.root_path // "") == $root)
         or ((.root_path // "") | startswith($root + "/"))
       )
    ] | length
  '
}

# Rewrite remote index removing entries under remote_root.
# API DELETE alone does not maintain the index; empty/filtered index is required
# for AC5 remount → DORMANT (refresh=0). Retries read-modify-write; fails hard if
# entries for this root remain after the last attempt.
rewrite_index_without_root() {
  local remote_root="$1"
  local attempt code body new_body url put_code remaining
  url="$BASE/v1/fs/.drive9/git-workspaces/index.json"
  for attempt in 1 2 3 4; do
    code=$(stat_remote_index)
    if [ "$code" = "404" ]; then
      return 0
    fi
    if [ "$code" = "000" ]; then
      echo "rewrite_index: index GET transport failure (attempt $attempt)" >&2
      sleep 0.5
      continue
    fi
    if [ "$code" != "200" ]; then
      echo "rewrite_index: unexpected index status $code (attempt $attempt)" >&2
      sleep 0.5
      continue
    fi
    body=$(fetch_remote_index_body)
    new_body=$(printf '%s' "$body" | jq -c --arg root "$remote_root" '
      .workspaces = ([.workspaces // []
        | .[]
        | select(
            ((.root_path // "") != $root)
            and (((.root_path // "") | startswith($root + "/")) | not)
          )])
      | .updated_at = (now | todateiso8601)
      | .version = (.version // 1)
    ')
    put_code=$(curl -sS --max-time 30 -o /dev/null -w "%{http_code}" -X PUT \
      -H "Authorization: Bearer $API_KEY" \
      -H "Content-Type: application/json" \
      -d "$new_body" \
      "$url" || printf '000')
    if [ "$put_code" != "200" ] && [ "$put_code" != "201" ] && [ "$put_code" != "204" ]; then
      echo "rewrite_index: PUT status $put_code (attempt $attempt)" >&2
      sleep 0.5
      continue
    fi
    remaining=$(index_count_for_root "$remote_root" 2>/dev/null || echo "?")
    if [ "$remaining" = "0" ]; then
      return 0
    fi
    sleep 0.5
  done
  echo "rewrite_index: failed to clear entries under $remote_root" >&2
  return 1
}

# Poll until index+list report zero under remote_root (or timeout).
wait_root_cleared() {
  local remote_root="$1"
  local timeout_s="${2:-20}"
  local deadline i_n l_n
  deadline=$(( $(date +%s) + timeout_s ))
  while :; do
    i_n=$(index_count_for_root "$remote_root" 2>/dev/null || echo "?")
    l_n=$(list_count_for_root "$remote_root" 2>/dev/null || echo "?")
    if [ "$i_n" = "0" ] && [ "$l_n" = "0" ]; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "wait_root_cleared: timeout index=$i_n list=$l_n root=$remote_root" >&2
      return 1
    fi
    sleep 1
  done
}

# Delete all git workspaces whose root_path is under remote_root, and filter index.
cleanup_suite_workspaces() {
  local remote_root="$1"
  local resp code body
  resp=$(curl_body_code GET "$BASE/v1/git-workspaces" "$API_KEY")
  code=$(http_code "$resp")
  if [ "$code" = "200" ]; then
    body=$(json_body "$resp")
    printf '%s' "$body" | jq -r --arg root "$remote_root" '
      (.workspaces // [])[]
      | select(
          ((.root_path // "") == $root)
          or ((.root_path // "") | startswith($root + "/"))
        )
      | .workspace_id
    ' | while read -r ws_id; do
      [ -n "$ws_id" ] || continue
      curl -sS --max-time 30 -o /dev/null -X DELETE \
        -H "Authorization: Bearer $API_KEY" \
        "$BASE/v1/git-workspaces/$ws_id" || true
    done
  elif [ "$code" != "000" ]; then
    # Non-200 list is non-fatal for pre-suite cleanup of a brand-new root.
    :
  fi
  rewrite_index_without_root "$remote_root"
}

wait_path_has_fixture_readme() {
  local path="$1"
  local i
  for i in $(seq 1 40); do
    if [ -f "$path" ] && grep -q "Drive9 fixture" "$path" 2>/dev/null; then
      return 0
    fi
    # Poke the directory so on-demand git layer can arm/list if index is present.
    ls "$(dirname "$path")" >/dev/null 2>&1 || true
    sleep 1
  done
  return 1
}

wait_repo_git_ready() {
  local repo="$1"
  local i
  for i in $(seq 1 40); do
    if [ -e "$repo/.git" ] && git -C "$repo" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
      return 0
    fi
    ls "$repo" >/dev/null 2>&1 || true
    ls -la "$repo/.git" >/dev/null 2>&1 || true
    sleep 1
  done
  return 1
}

prepare_fixture() {
  FIXTURE_ROOT="$RUN_ROOT/fixture"
  local json
  json="$(python3 "$SCRIPT_DIR/tools/git_fixture.py" "$FIXTURE_ROOT" --force --tree-files "$GIT_ONDEMAND_FIXTURE_TREE_FILES")"
  FIXTURE_URL="$(jq -r '.file_url' <<<"$json")"
  test -n "$FIXTURE_URL"
}

configure_git_identity() {
  local repo="$1"
  git_cmd -C "$repo" config user.email "drive9-e2e@example.test" || return
  git_cmd -C "$repo" config user.name "Drive9 E2E" || return
}

assert_repo_ready() {
  local repo="$1"
  test -e "$repo/.git" || return
  git_cmd -C "$repo" rev-parse --is-inside-work-tree >/dev/null || return
  git_cmd -C "$repo" log --oneline -1 >/dev/null || return
  git_cmd -C "$repo" status --porcelain=v1 --untracked-files=all >/dev/null
}

assert_fixture_readme() {
  local repo="$1"
  grep -q "Drive9 fixture" "$repo/README.md"
}

# Generate FS activity that would previously have driven ~1 ListGitWorkspaces/s.
# Includes paths containing a ".git" segment so dormant must not force-list on them.
dormant_fs_activity() {
  local root="$1"
  local loops="${2:-$GIT_ONDEMAND_DORMANT_ACTIVITY_LOOPS}"
  local i
  mkdir -p "$root/activity/a" "$root/activity/b" "$root/activity/fake.git" "$root/activity/proj/.git"
  for i in $(seq 1 "$loops"); do
    echo "loop $i" >"$root/activity/a/file-$i.txt"
    echo "other $i" >"$root/activity/b/other-$i.txt"
    ls -la "$root/activity" >/dev/null
    ls -la "$root/activity/a" >/dev/null
    cat "$root/activity/a/file-$i.txt" >/dev/null
    find "$root/activity" -type f >/dev/null 2>&1 || true
    touch "$root/activity/fake.git/HEAD" 2>/dev/null || true
    touch "$root/activity/proj/.git/config" 2>/dev/null || true
    ls "$root/activity/proj/.git" >/dev/null 2>&1 || true
    cat "$root/activity/proj/.git/config" >/dev/null 2>&1 || true
  done
}

# Ordinary FS noise while ARMED. Intentionally avoids ".git" path segments so
# shouldForceRefreshGitWorkspacesForGitStatePath cannot inflate refresh and
# muddy G3/G4 "no idle list storm" claims. Wall-clock duration is long enough
# that a restored 1s TTL poll would exceed GIT_ONDEMAND_REFRESH_MAX_SINGLE.
armed_idle_fs_activity() {
  local root="$1"
  local seconds="${2:-$GIT_ONDEMAND_ARMED_IDLE_S}"
  local deadline i
  mkdir -p "$root/activity/a" "$root/activity/b"
  deadline=$(( $(date +%s) + seconds ))
  i=0
  while [ "$(date +%s)" -lt "$deadline" ]; do
    i=$((i + 1))
    echo "armed idle $i" >"$root/activity/a/idle-$i.txt"
    echo "other $i" >"$root/activity/b/idle-$i.txt"
    ls -la "$root/activity" >/dev/null
    ls -la "$root/activity/a" >/dev/null
    cat "$root/activity/a/idle-$i.txt" >/dev/null
    find "$root/activity" -type f >/dev/null 2>&1 || true
    sleep 0.25
  done
}

# GET /.drive9/git-workspaces/index.json — 200 if present, 404 if missing.
# Prefer GET over HEAD: some deployments hang or omit status on HEAD for
# missing/.drive9 paths. Always bound curl with --max-time.
stat_remote_index() {
  local url="$BASE/v1/fs/.drive9/git-workspaces/index.json"
  local code
  code=$(curl -sS --max-time 15 -o /dev/null -w "%{http_code}" -X GET \
    -H "Authorization: Bearer $API_KEY" "$url" || printf '000')
  printf '%s' "$code"
}

fetch_remote_index_body() {
  local url="$BASE/v1/fs/.drive9/git-workspaces/index.json"
  curl -sS --max-time 15 -H "Authorization: Bearer $API_KEY" "$url" || true
}

precheck_fuse() {
  case "$(uname -s)" in
    Linux)
      if ! command -v fusermount3 >/dev/null 2>&1 && ! command -v fusermount >/dev/null 2>&1; then
        skip_or_fail "fusermount is not available"
      fi
      if [ ! -e /dev/fuse ]; then
        skip_or_fail "/dev/fuse is not available"
      fi
      ;;
    Darwin) ;;
    *)
      skip_or_fail "FUSE smoke is only supported on Linux and macOS"
      ;;
  esac
}

cleanup() {
  local rc=$?
  stop_mount >/dev/null 2>&1 || true
  # Existing-tenant CI (local-dev-key) accumulates timestamped remote trees; always
  # best-effort delete the suite RemoteRoot when API credentials are known.
  if [ -n "${API_KEY:-}" ] && [ -n "${REMOTE_ROOT:-}" ] && [ -n "${CLI_BIN:-}" ] && [ -x "${CLI_BIN:-}" ]; then
    drive9 fs rm -r --force ":$REMOTE_ROOT" >/dev/null 2>&1 || \
      curl -sS --max-time 60 -o /dev/null -X DELETE \
        -H "Authorization: Bearer $API_KEY" \
        "$BASE/v1/fs${REMOTE_ROOT}?recursive=true" >/dev/null 2>&1 || true
  fi
  if [ "$rc" -ne 0 ] || [ "$FAIL" -ne 0 ]; then
    if [ -n "$RUN_ROOT" ] && [ -d "$RUN_ROOT" ]; then
      find "$RUN_ROOT" -type f -name 'mount-*.log' -print | while read -r log_file; do
        dump_mount_log "$log_file"
      done
    fi
  elif [ "$GIT_ONDEMAND_KEEP_ARTIFACTS" != "1" ] && [ -n "$RUN_ROOT" ]; then
    rm -rf "$RUN_ROOT"
  fi
  if [ -n "${CLI_BIN:-}" ] && [ -f "$CLI_BIN" ] && [ "$CLI_SOURCE" = "build" ]; then
    # temp binary from mktemp
    rm -f "$CLI_BIN" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "=== drive9 git-workspace on-demand discovery e2e ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "DORMANT_ACTIVITY_LOOPS=$GIT_ONDEMAND_DORMANT_ACTIVITY_LOOPS"
echo "ARMED_IDLE_S=$GIT_ONDEMAND_ARMED_IDLE_S"
echo "REFRESH_MAX_SINGLE=$GIT_ONDEMAND_REFRESH_MAX_SINGLE REFRESH_MAX_DUAL=$GIT_ONDEMAND_REFRESH_MAX_DUAL"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
check_cmd "git is available" bash -c 'command -v git >/dev/null'
check_cmd "python3 is available" bash -c 'command -v python3 >/dev/null'
check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
fi
precheck_fuse

RUN_ROOT="$(mktemp -d "${FUSE_MOUNT_ROOT}/drive9-git-ondemand.XXXXXX")"
CLI_HOME="$RUN_ROOT/home"
mkdir -p "$CLI_HOME"

if [ -n "$DRIVE9_API_KEY" ]; then
  API_KEY="$DRIVE9_API_KEY"
  echo "[1] use provided DRIVE9_API_KEY"
else
  echo "[1] provision tenant"
  resp=$(curl_body_code POST "$BASE/v1/provision")
  code=$(http_code "$resp")
  check_eq "POST /v1/provision returns 202" "$code" "202"
  API_KEY=$(json_body "$resp" | jq -r '.api_key // empty')
  check_cmd "provision returns api_key" test -n "$API_KEY"
fi

echo "[2] wait tenant active"
check_cmd "tenant becomes active" wait_tenant_active

echo "[3] prepare drive9 cli"
check_cmd "prepare drive9 cli" prepare_cli_binary
check_cmd "drive9 binary ready" test -x "$CLI_BIN"

echo "[4] prepare local git fixture"
check_cmd "local git fixture ready" prepare_fixture

REMOTE_ROOT="/git-ondemand-$(date -u '+%Y%m%dT%H%M%SZ')-$$"
check_cmd "create remote root" drive9 fs mkdir ":$REMOTE_ROOT"

# ---------------------------------------------------------------------------
# Scenario A — G0: no workspace under this RemoteRoot → zero ListGitWorkspaces
# (tenant may already have a global index from prior suites; filter by root)
# ---------------------------------------------------------------------------
echo "[A] G0 dormant mount (no workspace under this RemoteRoot)"
# Ensure suite isolation: drop any leftover workspaces under our root.
cleanup_suite_workspaces "$REMOTE_ROOT" || true
mount_a="$RUN_ROOT/mnt-a"
local_a="$RUN_ROOT/local-a"
log_a="$RUN_ROOT/mount-a.log"
check_cmd "A mount starts" start_mount "$mount_a" "$local_a" "$log_a" "$REMOTE_ROOT"
# Probe may take a moment; activity that would have listed every second historically.
check_cmd "A dormant FS activity (incl .git segments)" dormant_fs_activity "$mount_a"
check_cmd "A index has no entries for suite RemoteRoot" index_has_no_entries_for_root "$REMOTE_ROOT"
check_cmd "A mount log audit" audit_mount_log "$log_a"
stop_mount
check_refresh_eq "A G0 zero git_workspace refresh after dormant activity" "$log_a" "0"

# ---------------------------------------------------------------------------
# Scenario B — G1: same LocalRoot live --fast arms and lists
# ---------------------------------------------------------------------------
echo "[B] G1 live --fast on same LocalRoot"
mount_b="$RUN_ROOT/mnt-b"
local_b="$RUN_ROOT/local-b"
log_b="$RUN_ROOT/mount-b.log"
check_cmd "B mount starts" start_mount "$mount_b" "$local_b" "$log_b" "$REMOTE_ROOT"
repo_b="$mount_b/repo-main"
check_cmd "B clone --fast" \
  drive9_with_timeout "$GIT_ONDEMAND_CLONE_TIMEOUT_S" git clone --fast --hydrate=off "$FIXTURE_URL" "$repo_b"
check_cmd "B repo ready after --fast" wait_repo_git_ready "$repo_b"
check_cmd "B fixture README readable via git layer" wait_path_has_fixture_readme "$repo_b/README.md"
# Next FS ops should keep using git layer without errors.
check_cmd "B git status works" bash -c "git -C '$repo_b' status --porcelain=v1 >/dev/null"
check_cmd "B git log works" bash -c "git -C '$repo_b' log --oneline -1 >/dev/null"
configure_git_identity "$repo_b" || true
echo "ondemand-edit" >>"$repo_b/README.md"
check_cmd "B git add/commit dirty file" bash -c "
  git -C '$repo_b' add README.md && git -C '$repo_b' commit -m 'ondemand edit' >/dev/null
"
# Remote index must exist after --fast (CLI publishes before success).
# Tenant-global: only require our RemoteRoot has entries (not absolute tenant emptiness).
idx_code=$(stat_remote_index)
check_eq "B remote index GET is 200 after --fast" "$idx_code" "200"
idx_n=$(index_count_for_root "$REMOTE_ROOT") || idx_n="err"
check_ge "B remote index lists >=1 workspace under suite RemoteRoot" "$idx_n" "1"
ws_count=$(list_count_for_root "$REMOTE_ROOT") || ws_count="err"
check_ge "B ListGitWorkspaces returns >=1 under suite RemoteRoot" "$ws_count" "1"
# Local arm markers should exist under local_root.
check_cmd "B local armed marker exists" \
  bash -c "test -f '$local_b/git-workspaces/armed' || ls '$local_b/git-workspaces/refresh' 2>/dev/null | grep -q ."
# Armed idle: wall-clock ordinary FS noise (no .git segments). A restored 1s poll
# over GIT_ONDEMAND_ARMED_IDLE_S would exceed REFRESH_MAX_SINGLE.
check_cmd "B armed idle FS activity (${GIT_ONDEMAND_ARMED_IDLE_S}s, no .git)" \
  armed_idle_fs_activity "$mount_b" "$GIT_ONDEMAND_ARMED_IDLE_S"
check_cmd "B mount log audit" audit_mount_log "$log_b"
stop_mount
check_refresh_between "B G1 git_workspace refresh in [1,max] after live --fast+idle" \
  "$log_b" "1" "$GIT_ONDEMAND_REFRESH_MAX_SINGLE"
check_forced_ge "B G1 forced_refresh >= 1 (arm path)" "$log_b" "1"

# ---------------------------------------------------------------------------
# Scenario C — G2: fresh LocalRoot remount discovers via remote index
# ---------------------------------------------------------------------------
echo "[C] G2 remount with fresh LocalRoot (index discovery)"
mount_c="$RUN_ROOT/mnt-c"
local_c="$RUN_ROOT/local-c"
log_c="$RUN_ROOT/mount-c.log"
check_cmd "C mount starts (fresh local root)" start_mount "$mount_c" "$local_c" "$log_c" "$REMOTE_ROOT"
repo_c="$mount_c/repo-main"
# After index probe, clean tree should be visible without re-cloning.
# Remount discovery can lag on cold list/tree; retry with poke (not a single assert).
check_cmd "C remounted README visible without re-clone" wait_path_has_fixture_readme "$repo_c/README.md"
check_cmd "C remounted repo becomes git-ready" wait_repo_git_ready "$repo_c"
check_cmd "C remounted git log works" bash -c "git -C '$repo_c' log --oneline -1 >/dev/null"
check_cmd "C mount log audit" audit_mount_log "$log_c"
stop_mount
check_refresh_between "C G2 remount armed via index (refresh in [1,max])" \
  "$log_c" "1" "$GIT_ONDEMAND_REFRESH_MAX_SINGLE"

# ---------------------------------------------------------------------------
# Scenario D — G3: second --fast on armed mount forces additional list
# ---------------------------------------------------------------------------
echo "[D] G3 second --fast on armed mount increases refresh"
mount_d="$RUN_ROOT/mnt-d"
local_d="$RUN_ROOT/local-d"
log_d="$RUN_ROOT/mount-d.log"
check_cmd "D mount starts" start_mount "$mount_d" "$local_d" "$log_d" "$REMOTE_ROOT"
repo_d1="$mount_d/repo-d1"
repo_d2="$mount_d/repo-d2"
check_cmd "D first clone --fast" \
  drive9_with_timeout "$GIT_ONDEMAND_CLONE_TIMEOUT_S" git clone --fast --hydrate=off "$FIXTURE_URL" "$repo_d1"
check_cmd "D first repo ready" wait_repo_git_ready "$repo_d1"
# Touch FS so first arm list is definitely done; then short ordinary idle (no .git).
ls -la "$repo_d1" >/dev/null
sleep 0.5
# Baseline after first --fast: at least one workspace under this root.
# Do not default a transport failure to "0" — that softens the growth check.
ws_after_first=""
if ws_after_first=$(list_count_for_root "$REMOTE_ROOT"); then
  :
else
  ws_after_first="err"
fi
check_ge "D ListGitWorkspaces >=1 after first --fast" "$ws_after_first" "1"
check_cmd "D second clone --fast (new root)" \
  drive9_with_timeout "$GIT_ONDEMAND_CLONE_TIMEOUT_S" git clone --fast --hydrate=off "$FIXTURE_URL" "$repo_d2"
check_cmd "D second repo ready" wait_repo_git_ready "$repo_d2"
# Force path resolution after second arm marker so pendingForce actually fires.
ls -la "$repo_d2" >/dev/null
find "$mount_d" -maxdepth 2 >/dev/null 2>&1 || true
check_cmd "D mount log audit" audit_mount_log "$log_d"
stop_mount
# At least two list cycles: initial arm + second workspace registration force.
# forced_refresh >= 2 tightens G3 (second --fast force path) beyond aggregate refresh.
# Upper bound catches silent regression to 1s idle poll during the scenario.
check_refresh_between "D G3 refresh in [2,max] after two --fast on same mount" \
  "$log_d" "2" "$GIT_ONDEMAND_REFRESH_MAX_DUAL"
check_forced_ge "D G3 forced_refresh >= 2 (arm + second --fast force)" "$log_d" "2"
ws_count=$(list_count_for_root "$REMOTE_ROOT") || ws_count="err"
check_ge "D ListGitWorkspaces returns >=2 under suite RemoteRoot" "$ws_count" "2"
# Growth after second clone proves the second registration landed (complements force counter).
if [ "$ws_after_first" != "err" ] && [ -n "$ws_after_first" ]; then
  check_ge "D ListGitWorkspaces grew after second --fast" "$ws_count" "$((ws_after_first + 1))"
else
  check_cmd "D ListGitWorkspaces grew after second --fast (baseline unreadable)" false
fi
idx_n=$(index_count_for_root "$REMOTE_ROOT") || idx_n="err"
check_ge "D remote index has >=2 workspace entries under suite RemoteRoot" "$idx_n" "2"
fetch_remote_index_body >"$RUN_ROOT/index-d.json"

# ---------------------------------------------------------------------------
# Scenario E — index schema is usable for discovery; remount still works.
# ---------------------------------------------------------------------------
echo "[E] index schema is existence-only; remount still discovers"
check_cmd "E index has workspace_id and root_path under suite root" bash -c "
  jq -e --arg root '$REMOTE_ROOT' '
    [.workspaces // []
     | .[]
     | select(
         ((.root_path // \"\") == \$root)
         or ((.root_path // \"\") | startswith(\$root + \"/\"))
       )
    ][0]
    | .workspace_id and .root_path
  ' '$RUN_ROOT/index-d.json' >/dev/null
"
# Optional: ensure we did not require sensitive fields in index consumers
# (index may or may not include extra fields; consumers must not need repo_url).
mount_e="$RUN_ROOT/mnt-e"
local_e="$RUN_ROOT/local-e"
log_e="$RUN_ROOT/mount-e.log"
check_cmd "E final remount starts" start_mount "$mount_e" "$local_e" "$log_e" "$REMOTE_ROOT"
check_cmd "E final remount sees repo-d1 README" wait_path_has_fixture_readme "$mount_e/repo-d1/README.md"
check_cmd "E final remount sees repo-d2 README" wait_path_has_fixture_readme "$mount_e/repo-d2/README.md"
check_cmd "E mount log audit" audit_mount_log "$log_e"
stop_mount
check_refresh_between "E final remount refresh in [1,max]" \
  "$log_e" "1" "$GIT_ONDEMAND_REFRESH_MAX_SINGLE"

# ---------------------------------------------------------------------------
# Scenario F — AC5: delete suite workspaces + empty index for root → remount
# refresh=0 (back to DORMANT for this RemoteRoot).
# ---------------------------------------------------------------------------
echo "[F] AC5 cleanup suite workspaces then remount stays dormant (refresh=0)"
check_cmd "F cleanup suite workspaces + rewrite index" cleanup_suite_workspaces "$REMOTE_ROOT"
check_cmd "F wait root cleared (index+list)" wait_root_cleared "$REMOTE_ROOT" 20
check_cmd "F index has no entries for suite RemoteRoot after cleanup" \
  index_has_no_entries_for_root "$REMOTE_ROOT"
list_after_cleanup=$(list_count_for_root "$REMOTE_ROOT") || list_after_cleanup="err"
check_eq "F ListGitWorkspaces under suite RemoteRoot is 0 after cleanup" \
  "$list_after_cleanup" "0"
mount_f="$RUN_ROOT/mnt-f"
local_f="$RUN_ROOT/local-f"
log_f="$RUN_ROOT/mount-f.log"
check_cmd "F remount starts after cleanup" start_mount "$mount_f" "$local_f" "$log_f" "$REMOTE_ROOT"
check_cmd "F dormant FS activity after cleanup" dormant_fs_activity "$mount_f" 15
check_cmd "F mount log audit" audit_mount_log "$log_f"
stop_mount
check_refresh_eq "F AC5 zero git_workspace refresh after cleanup remount" "$log_f" "0"

echo
echo "=== drive9 git-workspace on-demand e2e result ==="
echo "TOTAL=$TOTAL PASS=$PASS FAIL=$FAIL"
echo "artifacts=$RUN_ROOT"

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
exit 0
