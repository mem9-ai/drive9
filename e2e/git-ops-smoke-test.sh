#!/usr/bin/env bash
# Lightweight Drive9 Git smoke test for local e2e.

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
GIT_OPS_PROFILES="${GIT_OPS_PROFILES:-coding-agent,portable}"
GIT_OPS_CLONE_MODES="${GIT_OPS_CLONE_MODES:-native,fast,blobless}"
GIT_OPS_GIT_TIMEOUT_S="${GIT_OPS_GIT_TIMEOUT_S:-120}"
GIT_OPS_CLONE_TIMEOUT_S="${GIT_OPS_CLONE_TIMEOUT_S:-180}"
GIT_OPS_HYDRATE="${GIT_OPS_HYDRATE:-off}"
GIT_OPS_KEEP_ARTIFACTS="${GIT_OPS_KEEP_ARTIFACTS:-0}"
GIT_OPS_TRACE_GIT="${GIT_OPS_TRACE_GIT:-0}"
GIT_OPS_LOG_AUDIT_PATTERN="${GIT_OPS_LOG_AUDIT_PATTERN:-panic|fatal error|short read|input/output error}"
GIT_OPS_FIXTURE_TREE_FILES="${GIT_OPS_FIXTURE_TREE_FILES:-128}"
# Regression guard for git-workspace refresh storms (one remote refresh per
# FUSE op instead of per TTL window). Healthy refresh traffic is time-bounded
# (~1/s TTL + ~1/s throttled force), so the budget scales with mount uptime;
# a storm scales with op count and blows past it by an order of magnitude.
GIT_OPS_REFRESH_GUARD="${GIT_OPS_REFRESH_GUARD:-1}"
GIT_OPS_REFRESH_BUDGET_BASE="${GIT_OPS_REFRESH_BUDGET_BASE:-20}"
GIT_OPS_REFRESH_BUDGET_PER_SEC="${GIT_OPS_REFRESH_BUDGET_PER_SEC:-6}"
GIT_OPS_FORCED_REFRESH_BUDGET_BASE="${GIT_OPS_FORCED_REFRESH_BUDGET_BASE:-10}"
GIT_OPS_FORCED_REFRESH_BUDGET_PER_SEC="${GIT_OPS_FORCED_REFRESH_BUDGET_PER_SEC:-3}"

export GIT_ALLOW_PROTOCOL="${GIT_ALLOW_PROTOCOL:-file:https:http:ssh}"

PASS=0
FAIL=0
SKIP=0
TOTAL=0
RUN_ID="git-ops-$(date -u '+%Y%m%dT%H%M%SZ')-$$"
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
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc" >&2
    echo "  want: $want" >&2
    echo "  got : $got" >&2
    FAIL=$((FAIL + 1))
  fi
}

check_file_eq() {
  local desc="$1" got_file="$2" want_file="$3"
  TOTAL=$((TOTAL + 1))
  if cmp -s "$got_file" "$want_file"; then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc" >&2
    diff -u "$want_file" "$got_file" >&2 || true
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
  SKIP=$((SKIP + 1))
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
import os
import signal
import subprocess
import sys
import time

seconds = float(sys.argv[1])
cmd = sys.argv[2:]

def exit_code(rc):
    return rc if rc >= 0 else 128 + abs(rc)

proc = subprocess.Popen(cmd, start_new_session=True)
deadline = time.monotonic() + seconds
while True:
    rc = proc.poll()
    if rc is not None:
        raise SystemExit(exit_code(rc))
    if time.monotonic() >= deadline:
        break
    time.sleep(0.2)

rc = proc.poll()
if rc is not None:
    raise SystemExit(exit_code(rc))

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
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
  else
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
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
  if [ "$GIT_OPS_TRACE_GIT" = "1" ]; then
    {
      printf '+ git'
      printf ' %q' "$@"
      printf '\n'
    } >&2
  fi
  run_with_timeout "$GIT_OPS_GIT_TIMEOUT_S" git "$@"
}

start_mount() {
  local profile="$1"
  local mount_point="$2"
  local local_root="$3"
  local mount_log="$4"
  local remote_root="$5"
  local no_auto_unpack="$6"
  local unpack_archive="${7:-}"
  MOUNT_POINT="$mount_point"
  MOUNT_LOG="$mount_log"
  mkdir -p "$MOUNT_POINT" "$local_root"
  {
    echo "=== drive9 git ops mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "profile=$profile"
    echo "remote_root=$remote_root"
    echo "mount_point=$MOUNT_POINT"
    echo "local_root=$local_root"
    echo "no_auto_unpack=$no_auto_unpack"
    echo "unpack_archive=$unpack_archive"
  } >>"$MOUNT_LOG"

  local args=(mount --foreground --mode=fuse --profile "$profile" --local-root "$local_root" --durability=interactive --perf-counters)
  if [ "$no_auto_unpack" = "1" ]; then
    args+=(--no-auto-unpack)
  fi
  if [ -n "$unpack_archive" ]; then
    args+=(--unpack ":$unpack_archive")
  fi
  args+=( ":$remote_root" "$MOUNT_POINT" )
  drive9 "${args[@]}" >>"$MOUNT_LOG" 2>&1 &
  MOUNT_PID="$!"
  wait_mount_state mounted
  wait_mount_log_ready "$MOUNT_LOG"
}

stop_mount() {
  local no_auto_pack="${1:-0}"
  local pack_archive="${2:-}"
  local pack_path="${3:-}"
  set +e
  if [ -n "${MOUNT_POINT:-}" ] && is_mounted "$MOUNT_POINT"; then
    local args=(umount --timeout "$FUSE_UMOUNT_TIMEOUT")
    if [ "$no_auto_pack" = "1" ]; then
      args+=(--no-auto-pack)
    fi
    if [ -n "$pack_archive" ]; then
      args+=(--pack ":$pack_archive")
    fi
    if [ -n "$pack_path" ]; then
      args+=(--pack-path "$pack_path")
    fi
    args+=("$MOUNT_POINT")
    drive9 "${args[@]}" >/dev/null 2>&1 || true
    wait_mount_state unmounted >/dev/null 2>&1 || true
    if is_mounted "$MOUNT_POINT"; then
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
  if grep -Eina "$GIT_OPS_LOG_AUDIT_PATTERN" "$log_file"; then
    echo "mount log contains failure pattern" >&2
    return 1
  fi
  return 0
}

write_portable_profile() {
  mkdir -p "$CLI_HOME/.drive9/profiles"
  cat > "$CLI_HOME/.drive9/profiles/portable" <<'EOF'
[local]
**/.git/**

[remote]
# no remote override paths

[pack]
.git
EOF
}

prepare_fixture() {
  FIXTURE_ROOT="$RUN_ROOT/fixture"
  local json
  json="$(python3 "$SCRIPT_DIR/git_fixture.py" "$FIXTURE_ROOT" --force --tree-files "$GIT_OPS_FIXTURE_TREE_FILES")"
  FIXTURE_URL="$(jq -r '.file_url' <<<"$json")"
  test -n "$FIXTURE_URL"
}

check_workspace_refresh_budget() {
  local desc="$1"
  local log_file="$2"
  if [ "$GIT_OPS_REFRESH_GUARD" != "1" ]; then
    return 0
  fi
  TOTAL=$((TOTAL + 1))
  local out rc
  set +e
  out=$(python3 - "$log_file" \
    "$GIT_OPS_REFRESH_BUDGET_BASE" "$GIT_OPS_REFRESH_BUDGET_PER_SEC" \
    "$GIT_OPS_FORCED_REFRESH_BUDGET_BASE" "$GIT_OPS_FORCED_REFRESH_BUDGET_PER_SEC" <<'PY'
import re
import sys

log_path = sys.argv[1]
refresh_base, refresh_per_sec = int(sys.argv[2]), int(sys.argv[3])
forced_base, forced_per_sec = int(sys.argv[4]), int(sys.argv[5])

UNIT_SECONDS = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 1e-3, "µs": 1e-6, "us": 1e-6, "ns": 1e-9}

def parse_duration(text):
    total = 0.0
    for m in re.finditer(r"([0-9.]+)(h|ms|m|s|µs|us|ns)", text):
        total += float(m.group(1)) * UNIT_SECONDS[m.group(2)]
    return total

uptime = refresh = forced = None
with open(log_path, "r", errors="replace") as handle:
    for line in handle:
        m = re.search(r"drive9: FUSE perf summary uptime=(\S+)", line)
        if m:
            uptime = parse_duration(m.group(1))
        m = re.search(r"drive9: perf git_workspace refresh=(\d+) forced_refresh=(\d+)", line)
        if m:
            refresh, forced = int(m.group(1)), int(m.group(2))

if uptime is None or refresh is None or forced is None:
    print(f"perf summary not found in {log_path} (uptime={uptime} refresh={refresh} forced={forced})")
    raise SystemExit(2)

seconds = int(uptime) + 1
refresh_budget = refresh_base + refresh_per_sec * seconds
forced_budget = forced_base + forced_per_sec * seconds
print(
    f"uptime={uptime:.1f}s refresh={refresh}/{refresh_budget} forced={forced}/{forced_budget}"
)
raise SystemExit(0 if refresh <= refresh_budget and forced <= forced_budget else 1)
PY
  )
  rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    echo "PASS $desc ($out)"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc ($out)" >&2
    FAIL=$((FAIL + 1))
  fi
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

assert_clean_status() {
  local repo="$1"
  local out
  out="$(git_cmd -C "$repo" status --porcelain=v1 --untracked-files=all)" || return
  [ -z "$out" ]
}

assert_fixture_reads() {
  local repo="$1"
  grep -q "Drive9 fixture" "$repo/README.md" || return
  grep -q "Fixture guide" "$repo/docs/guide.md" || return
  git_cmd -C "$repo" ls-files --error-unmatch README.md docs/guide.md src/app.py script.sh >/dev/null || return
  git_cmd -C "$repo" ls-files --stage script.sh | grep -q '^100755 ' || return
  [ "$(readlink "$repo/link-to-readme")" = "README.md" ]
}

clone_repo() {
  local mode="$1"
  local target="$2"
  case "$mode" in
    native)
      git_cmd clone "$FIXTURE_URL" "$target"
      ;;
    fast)
      drive9_with_timeout "$GIT_OPS_CLONE_TIMEOUT_S" git clone --fast "$FIXTURE_URL" "$target"
      ;;
    blobless)
      drive9_with_timeout "$GIT_OPS_CLONE_TIMEOUT_S" \
        git clone --fast --blobless "--hydrate=$GIT_OPS_HYDRATE" "$FIXTURE_URL" "$target"
      ;;
    *)
      echo "unknown clone mode: $mode" >&2
      return 1
      ;;
  esac
}

capture_git_state() {
  local repo="$1"
  local out_dir="$2"
  mkdir -p "$out_dir"
  git_cmd -C "$repo" rev-parse HEAD > "$out_dir/head.txt" || return
  git_cmd -C "$repo" branch --show-current > "$out_dir/branch.txt" || return
  git_cmd -C "$repo" rev-parse --verify refs/stash > "$out_dir/stash.txt" || return
  git_cmd -C "$repo" status --porcelain=v1 --untracked-files=all > "$out_dir/status.txt" || return
  git_cmd -C "$repo" diff --cached --name-only > "$out_dir/cached.txt" || return
  git_cmd -C "$repo" diff --name-only > "$out_dir/unstaged.txt" || return
  git_cmd -C "$repo" ls-files --others --exclude-standard > "$out_dir/untracked.txt" || return
  cat "$repo/staged.txt" > "$out_dir/staged-content.txt" || return
  tail -n 4 "$repo/README.md" > "$out_dir/readme-tail.txt" || return
  cat "$repo/untracked.txt" > "$out_dir/untracked-content.txt"
}

verify_restored_git_state() {
  local repo="$1"
  local state_dir="$2"
  local actual_dir="$state_dir/actual"
  rm -rf "$actual_dir"
  mkdir -p "$actual_dir"

  check_cmd "restored repo ready" assert_repo_ready "$repo"
  if ! capture_git_state "$repo" "$actual_dir"; then
    check_cmd "restored git state capture" false
    return 0
  fi

  check_file_eq "restored HEAD matches" "$actual_dir/head.txt" "$state_dir/head.txt"
  check_file_eq "restored branch matches" "$actual_dir/branch.txt" "$state_dir/branch.txt"
  check_file_eq "restored stash ref matches" "$actual_dir/stash.txt" "$state_dir/stash.txt"
  check_file_eq "restored porcelain status matches" "$actual_dir/status.txt" "$state_dir/status.txt"
  check_file_eq "restored cached paths match" "$actual_dir/cached.txt" "$state_dir/cached.txt"
  check_file_eq "restored unstaged paths match" "$actual_dir/unstaged.txt" "$state_dir/unstaged.txt"
  check_file_eq "restored untracked paths match" "$actual_dir/untracked.txt" "$state_dir/untracked.txt"
  check_file_eq "restored staged content matches" "$actual_dir/staged-content.txt" "$state_dir/staged-content.txt"
  check_file_eq "restored unstaged content matches" "$actual_dir/readme-tail.txt" "$state_dir/readme-tail.txt"
  check_file_eq "restored untracked content matches" "$actual_dir/untracked-content.txt" "$state_dir/untracked-content.txt"
}

exercise_git_operations() {
  local repo="$1"
  local marker="$2"
  local state_dir="$3"
  local branch="drive9-e2e-${marker//[^A-Za-z0-9._-]/-}"

  configure_git_identity "$repo" || return
  assert_repo_ready "$repo" || return
  assert_fixture_reads "$repo" || return
  assert_clean_status "$repo" || return

  git_cmd -C "$repo" switch -c "$branch" || return
  printf '\ncommitted change %s\n' "$marker" >> "$repo/docs/guide.md"
  mkdir -p "$repo/generated"
  printf 'committed file %s\n' "$marker" > "$repo/generated/committed.txt"
  git_cmd -C "$repo" add docs/guide.md generated/committed.txt || return
  git_cmd -C "$repo" commit --no-verify -m "drive9 e2e committed change $marker" >/dev/null || return
  assert_clean_status "$repo" || return

  printf '\nstashed tracked change %s\n' "$marker" >> "$repo/docs/guide.md"
  printf 'stashed untracked %s\n' "$marker" > "$repo/stash-untracked.txt"
  git_cmd -C "$repo" stash push -u -m "drive9 e2e stash $marker" >/dev/null || return
  git_cmd -C "$repo" stash list | grep -q "drive9 e2e stash $marker" || return
  assert_clean_status "$repo" || return

  printf 'staged content %s\n' "$marker" > "$repo/staged.txt"
  git_cmd -C "$repo" add staged.txt || return
  printf '\nunstaged content %s\n' "$marker" >> "$repo/README.md"
  printf 'untracked content %s\n' "$marker" > "$repo/untracked.txt"
  capture_git_state "$repo" "$state_dir"
}

commit_after_restore() {
  local repo="$1"
  local marker="$2"
  git_cmd -C "$repo" add README.md untracked.txt || return
  git_cmd -C "$repo" commit --no-verify -m "drive9 e2e post-restore commit $marker" >/dev/null || return
  assert_clean_status "$repo" || return
  git_cmd -C "$repo" log -1 --format=%s | grep -q "drive9 e2e post-restore commit $marker"
}

run_case() {
  local profile="$1"
  local mode="$2"
  local slug="${profile}-${mode}"
  local marker="${RUN_ID}-${slug}"
  local case_root="$RUN_ROOT/$slug"
  local mount_a="$case_root/mount-a"
  local mount_b="$case_root/mount-b"
  local local_root_a="$case_root/local-root-a"
  local local_root_b="$case_root/local-root-b"
  local log_a="$case_root/mount-a.log"
  local log_b="$case_root/mount-b.log"
  local remote_root="/e2e/$RUN_ID/$slug"
  local repo_a="$mount_a/repo"
  local repo_b="$mount_b/repo"
  local state_dir="$case_root/state"
  local native_pack_archive="$remote_root/native-git-state.tar.gz"
  local use_native_pack=0

  mkdir -p "$case_root"
  echo
  echo "=== [profile=$profile clone=$mode] ==="
  check_cmd "$slug create remote root" drive9 fs mkdir ":$remote_root"

  if [ "$mode" = "native" ]; then
    use_native_pack=1
  fi

  check_cmd "$slug first mount starts" start_mount "$profile" "$mount_a" "$local_root_a" "$log_a" "$remote_root" 1
  check_cmd "$slug clone" clone_repo "$mode" "$repo_a"
  check_cmd "$slug git operations before remount" exercise_git_operations "$repo_a" "$marker" "$state_dir"
  check_cmd "$slug first mount log audit" audit_mount_log "$log_a"

  if [ "$use_native_pack" = "1" ]; then
    stop_mount 1 "$native_pack_archive" "repo/.git"
    check_workspace_refresh_budget "$slug first mount workspace refresh within budget" "$log_a"
    check_cmd "$slug second mount starts" start_mount "$profile" "$mount_b" "$local_root_b" "$log_b" "$remote_root" 1 "$native_pack_archive"
  else
    stop_mount 1
    check_workspace_refresh_budget "$slug first mount workspace refresh within budget" "$log_a"
    check_cmd "$slug second mount starts" start_mount "$profile" "$mount_b" "$local_root_b" "$log_b" "$remote_root" 1
  fi

  verify_restored_git_state "$repo_b" "$state_dir"
  check_cmd "$slug post-restore commit" commit_after_restore "$repo_b" "$marker"
  check_cmd "$slug second mount log audit" audit_mount_log "$log_b"
  stop_mount 1
  check_workspace_refresh_budget "$slug second mount workspace refresh within budget" "$log_b"
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
    Darwin)
      ;;
    *)
      skip_or_fail "FUSE smoke is only supported on Linux and macOS"
      ;;
  esac
}

cleanup() {
  local rc=$?
  stop_mount 1 >/dev/null 2>&1 || true
  if [ "$rc" -ne 0 ] || [ "$FAIL" -ne 0 ]; then
    if [ -n "$RUN_ROOT" ] && [ -d "$RUN_ROOT" ]; then
      find "$RUN_ROOT" -type f -name 'mount-*.log' -print | while read -r log_file; do
        dump_mount_log "$log_file"
      done
    fi
  elif [ "$GIT_OPS_KEEP_ARTIFACTS" != "1" ] && [ -n "$RUN_ROOT" ]; then
    rm -rf "$RUN_ROOT"
  fi
}
trap cleanup EXIT

echo "=== drive9 Git operations smoke test ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "PROFILES=$GIT_OPS_PROFILES"
echo "CLONE_MODES=$GIT_OPS_CLONE_MODES"
echo "HYDRATE=$GIT_OPS_HYDRATE"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
check_cmd "git is available" bash -c 'command -v git >/dev/null'
check_cmd "python3 is available" bash -c 'command -v python3 >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
else
  check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
fi
precheck_fuse

RUN_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/drive9-git-ops.XXXXXX")"
CLI_HOME="$RUN_ROOT/home"
mkdir -p "$CLI_HOME"
write_portable_profile

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
check_cmd "portable profile loads" drive9 profile show portable

echo "[4] prepare local git fixture"
check_cmd "local git fixture ready" prepare_fixture

IFS=',' read -r -a PROFILES <<< "$GIT_OPS_PROFILES"
IFS=',' read -r -a CLONE_MODES <<< "$GIT_OPS_CLONE_MODES"
for profile in "${PROFILES[@]}"; do
  profile="$(printf '%s' "$profile" | xargs)"
  [ -n "$profile" ] || continue
  for mode in "${CLONE_MODES[@]}"; do
    mode="$(printf '%s' "$mode" | xargs)"
    [ -n "$mode" ] || continue
    run_case "$profile" "$mode"
  done
done

echo
echo "=== drive9 Git operations smoke result ==="
echo "TOTAL=$TOTAL PASS=$PASS FAIL=$FAIL SKIP=$SKIP"

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
