#!/usr/bin/env bash
# git-feature-smoke-test: Drive9 Git feature coverage over a FUSE coding-agent mount.
# Covers clone modes, readiness, ops (add/commit/diff/remote), merge/rebase/stash,
# and remount restore. Naming aligns with other e2e/*-smoke-test.sh scripts.
#
# Not a Markdown "feature matrix". PASS/FAIL like other smokes.
# POSIX pjdfstest lives in blackbox (community.pjdfstest), not e2e.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-25}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
REMOTE_VISIBILITY_TIMEOUT_S="${REMOTE_VISIBILITY_TIMEOUT_S:-20}"
REMOTE_VISIBILITY_INTERVAL_S="${REMOTE_VISIBILITY_INTERVAL_S:-0.5}"
FUSE_MOUNT_ROOT="${FUSE_MOUNT_ROOT:-/tmp}"
FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-0}"
FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-60s}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-8}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-2}"
GIT_FEATURE_TIMEOUT_S="${GIT_FEATURE_TIMEOUT_S:-240}"
GIT_FEATURE_RUN_OVERSIZED="${GIT_FEATURE_RUN_OVERSIZED:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PASS=0
FAIL=0
SKIP=0
TOTAL=0
API_KEY=""
CLI_BIN=""
MOUNT_POINTS=()
RUN_ROOT=""
TS="$(date +%Y%m%d-%H%M%S)"

if [ "$(uname -s)" = "Darwin" ] && ! command -v mount_macfuse >/dev/null 2>&1 && ! command -v mount_fusefs >/dev/null 2>&1; then
  for macfuse_dir in "/Library/Filesystems/macfuse.fs/Contents/Resources" "/usr/local/bin" "/opt/homebrew/bin"; do
    if [ -x "$macfuse_dir/mount_macfuse" ] || [ -x "$macfuse_dir/mount_fusefs" ]; then
      PATH="$macfuse_dir:$PATH"
      export PATH
      break
    fi
  done
fi

sanitize_tsv_field() {
  printf '%s' "$1" | tr '\t\r\n' '   ' | sed 's/[[:space:]][[:space:]]*/ /g; s/^ //; s/ $//' | cut -c 1-1200
}

record() {
  local status="$1" category="$2" feature="$3" detail="${4:-}"
  TOTAL=$((TOTAL + 1))
  case "$status" in
    PASS)
      PASS=$((PASS + 1))
      echo "PASS [$category] $feature${detail:+ ($detail)}"
      ;;
    FAIL)
      FAIL=$((FAIL + 1))
      echo "FAIL [$category] $feature${detail:+ ($detail)}" >&2
      ;;
    SKIP|UNSUPPORTED)
      SKIP=$((SKIP + 1))
      echo "SKIP [$category] $feature${detail:+ ($detail)}"
      ;;
    META)
      TOTAL=$((TOTAL - 1))
      ;;
    *)
      FAIL=$((FAIL + 1))
      echo "FAIL [$category] $feature unexpected status=$status${detail:+ ($detail)}" >&2
      ;;
  esac
}


finish() {
  local rc=$?
  stop_mount "${MOUNT_POINTS[@]:-}" >/dev/null 2>&1 || true
  if [ -n "${RUN_ROOT:-}" ] && [ -d "$RUN_ROOT" ]; then
    if [ "$rc" -eq 0 ] && [ "${FAIL:-0}" -eq 0 ]; then
      rm -rf "$RUN_ROOT" 2>/dev/null || true
    else
      echo "Artifacts preserved at $RUN_ROOT" >&2
    fi
  fi
  echo ""
  echo "RESULT: $PASS passed, $FAIL failed, $SKIP skipped, $TOTAL total"
  if [ "$FAIL" -gt 0 ]; then
    exit 1
  fi
  exit "$rc"
}
trap finish EXIT

fail_fast() {
  local category="$1" feature="$2" detail="$3"
  record "FAIL" "$category" "$feature" "$detail"
  exit 1
}

detect_release_target() {
  case "$(uname -s)" in
    Linux) CLI_RELEASE_OS="linux" ;;
    Darwin) CLI_RELEASE_OS="darwin" ;;
    *) return 1 ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64) CLI_RELEASE_ARCH="amd64" ;;
    aarch64|arm64) CLI_RELEASE_ARCH="arm64" ;;
    *) return 1 ;;
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
  if [ -n "$CLI_RELEASE_VERSION" ]; then
    local actual_version
    actual_version="$($CLI_BIN --version 2>/dev/null | awk '{print $2}')"
    [ "$actual_version" = "$CLI_RELEASE_VERSION" ]
  fi
}

prepare_cli_binary() {
  CLI_BIN="$(mktemp)"
  case "$CLI_SOURCE" in
    build)
      make -C "$REPO_ROOT" build-cli CLI_BIN="$CLI_BIN"
      ;;
    official)
      download_official_cli
      ;;
    *)
      return 1
      ;;
  esac
  test -x "$CLI_BIN"
}

curl_body_code() {
  local method="$1" url="$2" auth="${3:-}" data="${4:-}"
  local body_file code rc
  body_file="$(mktemp)"
  set +e
  if [ -n "$auth" ] && [ -n "$data" ]; then
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" --data-binary "$data" "$url")
  elif [ -n "$auth" ]; then
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
  else
    code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
  fi
  rc=$?
  set -e
  cat "$body_file"
  echo
  if [ "$rc" -eq 0 ]; then
    echo "__HTTP__${code}"
  else
    echo "__HTTP__curl-rc-${rc}-${code:-000}"
  fi
  rm -f "$body_file"
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

drive9() {
  DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

drive9_retry() {
  local attempt=1 out rc
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

run_with_timeout_capture() {
  local seconds="$1" out_file="$2"
  shift 2
  python3 - "$seconds" "$out_file" "$@" <<'PY'
import os
import signal
import subprocess
import sys
import time

seconds = float(sys.argv[1])
out_file = sys.argv[2]
cmd = sys.argv[3:]

with open(out_file, "wb") as out:
    proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
    deadline = time.monotonic() + seconds
    while True:
        rc = proc.poll()
        if rc is not None:
            raise SystemExit(rc if rc >= 0 else 128 + abs(rc))
        if time.monotonic() >= deadline:
            break
        time.sleep(0.2)

    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
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

record_cmd() {
  local category="$1" feature="$2" timeout_s="$3"
  shift 3
  local out_file rc out
  out_file="$(mktemp)"
  set +e
  run_with_timeout_capture "$timeout_s" "$out_file" "$@"
  rc=$?
  set -e
  out="$(tail -c 600 "$out_file" 2>/dev/null || true)"
  rm -f "$out_file"
  if [ "$rc" -eq 0 ]; then
    record "PASS" "$category" "$feature" "ok"
  else
    record "FAIL" "$category" "$feature" "rc=$rc ${out:-<no output>}"
  fi
  return 0
}





record_drive9_cmd() {
  local category="$1" feature="$2" timeout_s="$3"
  shift 3
  record_cmd "$category" "$feature" "$timeout_s" env "DRIVE9_SERVER=$BASE" "DRIVE9_API_KEY=$API_KEY" "$CLI_BIN" "$@"
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
  local mount_point="$1" expect="$2"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  while :; do
    if [ "$expect" = "mounted" ] && is_mounted "$mount_point"; then
      return 0
    fi
    if [ "$expect" = "unmounted" ] && ! is_mounted "$mount_point"; then
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

start_mount() {
  local mount_point="$1" log_file="$2"
  shift 2
  mkdir -p "$mount_point"
  mount_point="$(cd "$mount_point" && pwd -P)"
  {
    echo "=== drive9 feature-matrix mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    printf 'args:'
    printf ' %q' "$@"
    printf '\n'
  } >>"$log_file"
  env "DRIVE9_SERVER=$BASE" "DRIVE9_API_KEY=$API_KEY" "$CLI_BIN" mount "$@" >>"$log_file" 2>&1 &
  local mount_pid=$!
  if wait_mount_state "$mount_point" mounted; then
    MOUNT_POINTS+=("$mount_point")
    return 0
  fi
  kill "$mount_pid" >/dev/null 2>&1 || true
  if [ -f "$log_file" ]; then
    tail -n 80 "$log_file" >&2 || true
  fi
  return 1
}

stop_mount() {
  local mount_point="$1"
  local umount_rc=0
  set +e
  if [ -n "$mount_point" ] && is_mounted "$mount_point"; then
    env "DRIVE9_SERVER=$BASE" "DRIVE9_API_KEY=$API_KEY" "$CLI_BIN" umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$mount_point" >/dev/null 2>&1
    umount_rc=$?
    wait_mount_state "$mount_point" unmounted >/dev/null 2>&1 || true
    if is_mounted "$mount_point"; then
      force_unmount "$mount_point"
      wait_mount_state "$mount_point" unmounted >/dev/null 2>&1 || true
    fi
  fi
  set -e
  return "$umount_rc"
}


wait_file_content() {
  local path="$1" want="$2"
  local deadline
  deadline=$(python3 - "$REMOTE_VISIBILITY_TIMEOUT_S" <<'PY'
import sys, time
print(time.time() + float(sys.argv[1]))
PY
)
  while :; do
    if [ -f "$path" ] && [ "$(cat "$path" 2>/dev/null || true)" = "$want" ]; then
      return 0
    fi
    if python3 - "$deadline" <<'PY'
import sys, time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      return 1
    fi
    sleep "$REMOTE_VISIBILITY_INTERVAL_S"
  done
}

wait_drive9_cat() {
  local remote_path="$1" want="$2"
  local deadline out rc
  deadline=$(python3 - "$REMOTE_VISIBILITY_TIMEOUT_S" <<'PY'
import sys, time
print(time.time() + float(sys.argv[1]))
PY
)
  while :; do
    set +e
    out=$(drive9 fs cat "$remote_path" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ] && [ "$out" = "$want" ]; then
      return 0
    fi
    if python3 - "$deadline" <<'PY'
import sys, time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      return 1
    fi
    sleep "$REMOTE_VISIBILITY_INTERVAL_S"
  done
}

wait_api_get() {
  local path="$1" want="$2"
  path="${path#/}"
  local deadline resp code body
  deadline=$(python3 - "$REMOTE_VISIBILITY_TIMEOUT_S" <<'PY'
import sys, time
print(time.time() + float(sys.argv[1]))
PY
)
  while :; do
    resp=$(curl_body_code GET "$BASE/v1/fs/$path" "$API_KEY")
    code=$(http_code "$resp")
    body=$(json_body "$resp")
    if [ "$code" = "200" ] && [ "$body" = "$want" ]; then
      return 0
    fi
    if python3 - "$deadline" <<'PY'
import sys, time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      return 1
    fi
    sleep "$REMOTE_VISIBILITY_INTERVAL_S"
  done
}

sha256_file() {
  python3 - "$1" <<'PY'
import hashlib
import sys
from pathlib import Path
print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
}

wait_drive9_copy_hash() {
  local remote_path="$1" local_path="$2" want_hash="$3"
  local deadline got_hash rc
  deadline=$(python3 - "$REMOTE_VISIBILITY_TIMEOUT_S" <<'PY'
import sys, time
print(time.time() + float(sys.argv[1]))
PY
)
  while :; do
    rm -f "$local_path"
    set +e
    drive9 fs cp "$remote_path" "$local_path" >/dev/null 2>&1
    rc=$?
    set -e
    if [ "$rc" -eq 0 ] && [ -f "$local_path" ]; then
      got_hash="$(sha256_file "$local_path" 2>/dev/null || true)"
      if [ "$got_hash" = "$want_hash" ]; then
        return 0
      fi
    fi
    if python3 - "$deadline" <<'PY'
import sys, time
raise SystemExit(0 if time.time() >= float(sys.argv[1]) else 1)
PY
    then
      return 1
    fi
    sleep "$REMOTE_VISIBILITY_INTERVAL_S"
  done
}

local_mode() {
  local path="$1"
  if [ "$(uname -s)" = "Darwin" ]; then
    stat -f "%Lp" "$path"
  else
    stat -c "%a" "$path"
  fi
}

local_size() {
  local path="$1"
  if [ "$(uname -s)" = "Darwin" ]; then
    stat -f "%z" "$path"
  else
    stat -c "%s" "$path"
  fi
}

head_mode() {
  local path="$1"
  path="${path#/}"
  local out code
  out=$(curl -sS -w "__HTTP__%{http_code}" -I -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/$path")
  code=$(printf '%s' "$out" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n')
  if [ "$code" != "200" ]; then
    return 1
  fi
  printf '%s' "$out" | grep -i '^x-dat9-mode:' | head -1 | cut -d: -f2- | sed 's/^[[:space:]]*//' | tr -d '\r\n'
}

git_cmd_record() {
  local repo="$1" category="$2" feature="$3"
  shift 3
  record_cmd "$category" "$feature" "$GIT_FEATURE_TIMEOUT_S" git -C "$repo" "$@"
}

git_output() {
  local repo="$1"
  shift
  git -C "$repo" "$@" 2>/dev/null | tr -d '\r'
}

record_git_clean() {
  local repo="$1" feature="$2"
  local out
  if out="$(git_output "$repo" status --porcelain=v1)" && [ -z "$out" ]; then
    record "PASS" "Git Clean Repo Readiness" "$feature" "status clean"
  else
    record "FAIL" "Git Clean Repo Readiness" "$feature" "status=${out:-<command failed>}"
  fi
}

record_status_contains() {
  local repo="$1" category="$2" feature="$3" pattern="$4"
  local out
  if out="$(git_output "$repo" status --porcelain=v1)" && grep -Eq "$pattern" <<<"$out"; then
    record "PASS" "$category" "$feature" "matched $pattern"
  else
    record "FAIL" "$category" "$feature" "status=${out:-<empty or command failed>} pattern=$pattern"
  fi
}

configure_git_identity() {
  local repo="$1"
  git -C "$repo" config user.email "drive9-matrix@example.test" >/dev/null 2>&1 || return 1
  git -C "$repo" config user.name "Drive9 Matrix" >/dev/null 2>&1 || return 1
}

repo_ready() {
  local repo="$1" category="$2" feature="$3"
  if [ -d "$repo/.git" ] && git -C "$repo" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    return 0
  fi
  record "FAIL" "$category" "$feature" "repo unavailable: $repo"
  return 1
}

make_remote_ahead_commit() {
  local bare_repo="$1" branch="$2" peer="$3"
  git clone "$bare_repo" "$peer" >/dev/null 2>&1 || return 1
  configure_git_identity "$peer" || return 1
  git -C "$peer" checkout "$branch" >/dev/null 2>&1 || return 1
  printf 'remote ahead\n' > "$peer/remote-ahead.txt"
  git -C "$peer" add remote-ahead.txt >/dev/null 2>&1 || return 1
  git -C "$peer" commit -m "remote ahead" >/dev/null 2>&1 || return 1
  git -C "$peer" push origin "$branch" >/dev/null 2>&1 || return 1
}

clone_drive9_repo() {
  local feature="$1" repo_url="$2" target="$3"
  shift 3
  record_drive9_cmd "Git Clone Modes" "$feature" "$GIT_FEATURE_TIMEOUT_S" git clone --fast "$@" "$repo_url" "$target"
}

run_git_readiness_checks() {
  local repo="$1"
  git_cmd_record "$repo" "Git Clean Repo Readiness" ".git directory is usable" rev-parse --is-inside-work-tree
  git_cmd_record "$repo" "Git Clean Repo Readiness" "git log reads latest commit" log --oneline -1
  git_cmd_record "$repo" "Git Clean Repo Readiness" "git show reads HEAD" show --stat --oneline -1
  git_cmd_record "$repo" "Git Clean Repo Readiness" "git ls-files lists manifest" ls-files
  git_cmd_record "$repo" "Git Clean Repo Readiness" "git cat-file reads clean blob" cat-file -e HEAD:README.md
  record_git_clean "$repo" "git status clean after fast clone"
  if [ -x "$repo/script.sh" ] && git -C "$repo" ls-files -s script.sh | grep -q '^100755 '; then
    record "PASS" "Git Clean Repo Readiness" "executable bit visible" "script.sh executable"
  else
    record "FAIL" "Git Clean Repo Readiness" "executable bit visible" "script.sh is not executable or index mode mismatch"
  fi
  if [ -L "$repo/link-to-readme" ] && [ "$(readlink "$repo/link-to-readme")" = "README.md" ]; then
    record "PASS" "Git Clean Repo Readiness" "symlink visible" "link-to-readme -> README.md"
  else
    record "FAIL" "Git Clean Repo Readiness" "symlink visible" "missing or wrong link"
  fi
  if python3 - "$repo/binary.bin" <<'PY'
import sys
from pathlib import Path
data = Path(sys.argv[1]).read_bytes()
raise SystemExit(0 if data == bytes(range(32)) else 1)
PY
  then
    record "PASS" "Git Clean Repo Readiness" "binary file visible" "binary bytes match"
  else
    record "FAIL" "Git Clean Repo Readiness" "binary file visible" "binary bytes mismatch"
  fi
  if git -C "$repo" tag --list v0.1.0 | grep -q '^v0.1.0$'; then
    record "PASS" "Git Clean Repo Readiness" "tag visibility" "v0.1.0"
  else
    record "FAIL" "Git Clean Repo Readiness" "tag visibility" "v0.1.0 missing"
  fi
}

run_git_ops_suite() {
  local repo="$1" bare_repo="$2" ts="$3"
  if ! repo_ready "$repo" "Git Prerequisites" "ops repo ready"; then
    return
  fi
  if configure_git_identity "$repo"; then
    record "PASS" "Git Prerequisites" "configure git identity for ops repo" "ok"
  else
    record "FAIL" "Git Prerequisites" "configure git identity for ops repo" "git config failed"
    return
  fi

  printf '\ntracked edit %s\n' "$ts" >> "$repo/README.md"
  record_status_contains "$repo" "Git Working Tree Operations" "modify tracked file" '^ M README\.md$'
  git_cmd_record "$repo" "Git Index Operations" "git add individual path" add README.md
  printf 'extra unstaged %s\n' "$ts" >> "$repo/README.md"
  record_status_contains "$repo" "Git Index Operations" "staged vs unstaged status accuracy" '^MM README\.md$'
  git_cmd_record "$repo" "Git Index Operations" "git restore --staged" restore --staged README.md
  record_status_contains "$repo" "Git Index Operations" "unstaged status after restore --staged" '^ M README\.md$'

  mkdir -p "$repo/generated/dir"
  printf 'generated %s\n' "$ts" > "$repo/generated/dir/new.txt"
  record_status_contains "$repo" "Git Working Tree Operations" "create files/directories" '^\?\? generated/'

  git_cmd_record "$repo" "Git Working Tree Operations" "git mv tracked file" mv src/app.py src/app_renamed.py
  git_cmd_record "$repo" "Git Working Tree Operations" "git rm tracked file" rm docs/guide.md
  if chmod 0644 "$repo/script.sh"; then
    record_status_contains "$repo" "Git Working Tree Operations" "chmod executable bit change" '^ M script\.sh$'
  else
    record "FAIL" "Git Working Tree Operations" "chmod executable bit change" "chmod failed"
  fi
  rm -f "$repo/link-to-readme"
  if ! ln -s src/app_renamed.py "$repo/link-to-app"; then
    record "FAIL" "Git Working Tree Operations" "symlink changes" "ln -s failed"
  fi
  record_status_contains "$repo" "Git Working Tree Operations" "symlink changes" '^( D link-to-readme|\?\? link-to-app)'
  if python3 - "$repo/binary.bin" <<'PY'
import sys
from pathlib import Path
p = Path(sys.argv[1])
data = bytearray(p.read_bytes())
data[0:4] = b"D9MX"
p.write_bytes(data)
PY
  then
    record_status_contains "$repo" "Git Working Tree Operations" "binary file modification" '^ M binary\.bin$'
  else
    record "FAIL" "Git Working Tree Operations" "binary file modification" "binary edit failed"
  fi
  mkdir -p "$repo/ignored-build"
  printf 'ignored\n' > "$repo/ignored-build/cache.tmp"
  if git -C "$repo" check-ignore -q ignored-build/cache.tmp; then
    record "PASS" "Git Working Tree Operations" "ignored local-only generated files" "git check-ignore accepted"
  else
    record "FAIL" "Git Working Tree Operations" "ignored local-only generated files" "ignored-build/cache.tmp was not ignored"
  fi

  printf 'reset me\n' > "$repo/generated/reset.txt"
  git_cmd_record "$repo" "Git Index Operations" "git add -A" add -A
  git_cmd_record "$repo" "Git Index Operations" "git reset path" reset HEAD generated/reset.txt
  record_status_contains "$repo" "Git Index Operations" "git reset leaves path unstaged" '^\?\? generated/reset\.txt$'
  git_cmd_record "$repo" "Git Index Operations" "git add -A after reset" add -A

  git_cmd_record "$repo" "Git Diff And Patch" "git diff --cached" diff --cached --stat
  git_cmd_record "$repo" "Git Commit History" "git commit" commit --no-verify -m "drive9 matrix ops"
  record_git_clean "$repo" "clean status after commit"

  printf '\npatch text %s\n' "$ts" >> "$repo/README.md"
  if git -C "$repo" diff > "$repo/../text.patch"; then
    record "PASS" "Git Diff And Patch" "generate text patch" "ok"
  else
    record "FAIL" "Git Diff And Patch" "generate text patch" "git diff failed"
  fi
  git_cmd_record "$repo" "Git Diff And Patch" "restore before text patch apply" checkout -- README.md
  git_cmd_record "$repo" "Git Diff And Patch" "git apply text patch" apply "$repo/../text.patch"
  if git -C "$repo" diff -- README.md | grep -q 'patch text'; then
    record "PASS" "Git Diff And Patch" "git diff nonempty" "patch text visible"
  else
    record "FAIL" "Git Diff And Patch" "git diff nonempty" "expected patch text not visible"
  fi
  git_cmd_record "$repo" "Git Index Operations" "stage text patch result" add README.md

  if python3 - "$repo/binary.bin" <<'PY'
import sys
from pathlib import Path
p = Path(sys.argv[1])
data = bytearray(p.read_bytes())
data[-4:] = b"BIN!"
p.write_bytes(data)
PY
  then
    record "PASS" "Git Diff And Patch" "prepare binary patch edit" "ok"
  else
    record "FAIL" "Git Diff And Patch" "prepare binary patch edit" "binary edit failed"
  fi
  if git -C "$repo" diff --binary > "$repo/../binary.patch"; then
    record "PASS" "Git Diff And Patch" "generate binary patch" "ok"
  else
    record "FAIL" "Git Diff And Patch" "generate binary patch" "git diff --binary failed"
  fi
  git_cmd_record "$repo" "Git Diff And Patch" "restore before binary patch apply" checkout -- binary.bin
  git_cmd_record "$repo" "Git Diff And Patch" "git apply binary patch" apply "$repo/../binary.patch"
  git_cmd_record "$repo" "Git Index Operations" "stage binary patch result" add binary.bin
  git_cmd_record "$repo" "Git Commit History" "git commit --amend" commit --amend --no-edit --no-verify
  record_git_clean "$repo" "clean status after amend"

  local branch="drive9-matrix-$ts"
  git_cmd_record "$repo" "Git Commit History" "branch create/switch" switch -c "$branch"
  printf 'branch work\n' > "$repo/branch-work.txt"
  git_cmd_record "$repo" "Git Index Operations" "stage branch work" add branch-work.txt
  git_cmd_record "$repo" "Git Commit History" "branch commit" commit --no-verify -m "branch work"
  git_cmd_record "$repo" "Git Remote Operations" "push branch to local bare remote" push -u origin HEAD
  git_cmd_record "$repo" "Git Remote Operations" "fetch from local bare remote" fetch origin
  local tag="matrix-$ts"
  git_cmd_record "$repo" "Git Remote Operations" "create local tag" tag "$tag"
  git_cmd_record "$repo" "Git Remote Operations" "push tag to local bare remote" push origin "refs/tags/$tag"
  if make_remote_ahead_commit "$bare_repo" "$branch" "$RUN_ROOT/peer-$ts"; then
    record "PASS" "Git Remote Operations" "remote ahead fixture commit" "ok"
  else
    record "FAIL" "Git Remote Operations" "remote ahead fixture commit" "failed to update bare remote"
  fi
  git_cmd_record "$repo" "Git Remote Operations" "pull from local bare remote" pull --ff-only origin "$branch"
}

# Sets FLOW_REPO to the cloned path. Do not capture stdout: record() prints PASS/FAIL there.
clone_for_flow() {
  local name="$1" file_url="$2" mount_point="$3"
  local target="$mount_point/$name"
  FLOW_REPO=""
  record_drive9_cmd "Git Clone Modes" "$name clone for flow" "$GIT_FEATURE_TIMEOUT_S" git clone --fast --blobless --hydrate=sync "$file_url" "$target"
  if ! configure_git_identity "$target"; then
    record "FAIL" "Git Prerequisites" "$name git identity configured" "git config failed"
    return 1
  fi
  FLOW_REPO="$target"
}

run_git_flow_suite() {
  local mount_point="$1" file_url="$2"
  local repo

  clone_for_flow merge-flow "$file_url" "$mount_point"
  repo="$FLOW_REPO"
  if repo_ready "$repo" "Git Merge/Rebase/Stash" "merge-flow repo ready"; then
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "clean merge" merge origin/feature/clean-merge --no-edit
  fi

  clone_for_flow conflict-flow "$file_url" "$mount_point"
  repo="$FLOW_REPO"
  if repo_ready "$repo" "Git Merge/Rebase/Stash" "conflict-flow repo ready"; then
    printf 'local conflict\n' > "$repo/README.md"
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "conflict fixture stage local edit" add README.md
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "conflict fixture local commit" commit --no-verify -m "local conflict"
    if git -C "$repo" merge origin/feature/conflict >/dev/null 2>&1; then
      record "FAIL" "Git Merge/Rebase/Stash" "conflict detection" "merge unexpectedly succeeded"
    else
      record_status_contains "$repo" "Git Merge/Rebase/Stash" "conflict detection" '^UU README\.md$'
      git -C "$repo" merge --abort >/dev/null 2>&1 || true
    fi
  fi

  clone_for_flow rebase-flow "$file_url" "$mount_point"
  repo="$FLOW_REPO"
  if repo_ready "$repo" "Git Merge/Rebase/Stash" "rebase-flow repo ready"; then
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "rebase fixture branch create" switch -c local-rebase
    mkdir -p "$repo/docs"
    printf 'local rebase\n' > "$repo/docs/rebase-local.md"
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "rebase fixture stage local file" add docs/rebase-local.md
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "rebase fixture local commit" commit --no-verify -m "local rebase"
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "simple rebase" rebase origin/feature/rebase
  fi

  clone_for_flow stash-flow "$file_url" "$mount_point"
  repo="$FLOW_REPO"
  if repo_ready "$repo" "Git Merge/Rebase/Stash" "stash-flow repo ready"; then
    printf 'stash edit\n' >> "$repo/README.md"
    printf 'stash untracked\n' > "$repo/stash-new.txt"
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "stash push -u" stash push -u -m "matrix stash"
    record_git_clean "$repo" "clean status after stash push"
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "stash apply" stash apply
    record_status_contains "$repo" "Git Merge/Rebase/Stash" "dirty status after stash apply" 'README\.md'
    git_cmd_record "$repo" "Git Merge/Rebase/Stash" "stash drop" stash drop
  fi
}

run_restore_suite() {
  local git_root_rel="$1" file_url="$2" mount_point="$3" local_root_a="$4" log_file_a="$5"
  local restore_repo="$mount_point/restore-workspace"
  record_drive9_cmd "Git Clone Modes" "restore workspace clone" "$GIT_FEATURE_TIMEOUT_S" git clone --fast --blobless --hydrate=sync "$file_url" "$restore_repo"
  if ! repo_ready "$restore_repo" "Sandbox Restore" "restore workspace repo ready"; then
    return
  fi
  if configure_git_identity "$restore_repo"; then
    record "PASS" "Git Prerequisites" "configure git identity for restore repo" "ok"
  else
    record "FAIL" "Git Prerequisites" "configure git identity for restore repo" "git config failed"
    return
  fi

  printf 'committed restore\n' > "$restore_repo/committed-local.txt"
  git_cmd_record "$restore_repo" "Sandbox Restore" "stage committed local state before remount" add committed-local.txt
  git_cmd_record "$restore_repo" "Sandbox Restore" "commit local state before remount" commit --no-verify -m "restore local commit"
  local restore_head
  restore_head="$(git_output "$restore_repo" rev-parse HEAD || true)"

  printf 'unstaged restore\n' >> "$restore_repo/README.md"
  mkdir -p "$restore_repo/restore-dir"
  printf 'overlay dir file\n' > "$restore_repo/restore-dir/file.txt"
  git_cmd_record "$restore_repo" "Drive9 Git Workspace Behavior" "prepare overlay whiteout" rm docs/guide.md
  if chmod 0644 "$restore_repo/script.sh"; then
    record "PASS" "Drive9 Git Workspace Behavior" "prepare overlay chmod" "script.sh mode set to 644"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "prepare overlay chmod" "chmod failed"
  fi
  rm -f "$restore_repo/link-to-readme"
  if ln -s README.md "$restore_repo/restore-link"; then
    record "PASS" "Drive9 Git Workspace Behavior" "prepare overlay symlink" "restore-link -> README.md"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "prepare overlay symlink" "ln -s failed"
  fi
  printf 'small staged object\n' > "$restore_repo/small-staged.txt"
  git_cmd_record "$restore_repo" "Sandbox Restore" "stage small local object before remount" add small-staged.txt
  if [ "$GIT_FEATURE_RUN_OVERSIZED" = "1" ]; then
    python3 - "$restore_repo/oversized-staged.bin" <<'PY'
import sys
from pathlib import Path
Path(sys.argv[1]).write_bytes(b"D" * (5 * 1024 * 1024 + 1))
PY
    git_cmd_record "$restore_repo" "Drive9 Git Workspace Behavior" "stage oversized object before remount" add oversized-staged.bin
  else
    record "SKIP" "Drive9 Git Workspace Behavior" "oversized staged object downgrade" "GIT_FEATURE_RUN_OVERSIZED=0"
  fi
  record_status_contains "$restore_repo" "Sandbox Restore" "dirty status before remount" 'README\.md'

  if stop_mount "$mount_point" && ! is_mounted "$mount_point"; then
    record "PASS" "Drive9 Git Workspace Behavior" "unmount drains git workspace state" "rw coding-agent mount unmounted"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "unmount drains git workspace state" "rw coding-agent mount did not gracefully unmount"
  fi

  local mount_point_b="$RUN_ROOT/git-mount-b"
  local local_root_b="$RUN_ROOT/git-local-b"
  local log_file_b="$RUN_ROOT/git-mount-b.log"
  mkdir -p "$mount_point_b" "$local_root_b"
  if start_mount "$mount_point_b" "$log_file_b" --mode=fuse --profile=coding-agent --local-root "$local_root_b" --durability=interactive ":/$git_root_rel" "$mount_point_b"; then
    record "PASS" "Drive9 Git Workspace Behavior" "fresh local-root remount starts" "mounted"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "fresh local-root remount starts" "see $log_file_b"
    return
  fi

  restore_repo="$mount_point_b/restore-workspace"
  if [ -d "$restore_repo/.git" ] && git -C "$restore_repo" status --porcelain=v1 >/dev/null 2>&1; then
    record "PASS" "Drive9 Git Workspace Behavior" ".git checkpoint restored" "git status works"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" ".git checkpoint restored" "missing .git or git status failed"
  fi
  if grep -q "unstaged restore" "$restore_repo/README.md" && [ -f "$restore_repo/restore-dir/file.txt" ]; then
    record "PASS" "Drive9 Git Workspace Behavior" "overlay upsert/dir survives remount" "README and restore-dir restored"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "overlay upsert/dir survives remount" "missing edited README or restore-dir"
  fi
  if [ ! -e "$restore_repo/docs/guide.md" ]; then
    record "PASS" "Drive9 Git Workspace Behavior" "overlay whiteout survives remount" "docs/guide.md absent"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "overlay whiteout survives remount" "docs/guide.md still exists"
  fi
  if [ "$(local_mode "$restore_repo/script.sh")" = "644" ]; then
    record "PASS" "Drive9 Git Workspace Behavior" "overlay chmod survives remount" "script.sh mode 644"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "overlay chmod survives remount" "mode=$(local_mode "$restore_repo/script.sh" 2>/dev/null || echo missing)"
  fi
  if [ -L "$restore_repo/restore-link" ] && [ "$(readlink "$restore_repo/restore-link")" = "README.md" ]; then
    record "PASS" "Drive9 Git Workspace Behavior" "overlay symlink survives remount" "restore-link -> README.md"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "overlay symlink survives remount" "missing or wrong link"
  fi
  if [ -n "$restore_head" ] && [ "$(git_output "$restore_repo" rev-parse HEAD || true)" = "$restore_head" ] && [ -f "$restore_repo/committed-local.txt" ]; then
    record "PASS" "Sandbox Restore" "committed local state survives fresh local-root remount" "HEAD=$restore_head"
  else
    record "FAIL" "Sandbox Restore" "committed local state survives fresh local-root remount" "HEAD=$(git_output "$restore_repo" rev-parse HEAD || true)"
  fi
  record_status_contains "$restore_repo" "Sandbox Restore" "unstaged edits survive fresh local-root remount" 'README\.md'
  record_status_contains "$restore_repo" "Sandbox Restore" "small staged object preserved" '^A  small-staged\.txt$'
  if [ "$GIT_FEATURE_RUN_OVERSIZED" = "1" ]; then
    local status
    status="$(git_output "$restore_repo" status --porcelain=v1 || true)"
    if grep -Eq '^(\?\?| A|AM| M) oversized-staged\.bin$' <<<"$status" && ! grep -q '^A  oversized-staged\.bin$' <<<"$status"; then
      record "PASS" "Drive9 Git Workspace Behavior" "oversized staged object downgrade" "status downgraded: $(grep 'oversized-staged.bin' <<<"$status" | head -1)"
    else
      record "FAIL" "Drive9 Git Workspace Behavior" "oversized staged object downgrade" "status=${status:-<empty>}"
    fi
  fi
  mkdir -p "$restore_repo/ignored-build"
  printf 'local ignored\n' > "$restore_repo/ignored-build/cache.tmp"
  stop_mount "$mount_point_b" >/dev/null 2>&1 || true
  mkdir -p "$mount_point_b" "$RUN_ROOT/git-local-c"
  if start_mount "$mount_point_b" "$RUN_ROOT/git-mount-c.log" --mode=fuse --profile=coding-agent --local-root "$RUN_ROOT/git-local-c" --durability=interactive ":/$git_root_rel" "$mount_point_b"; then
    if [ ! -e "$mount_point_b/restore-workspace/ignored-build/cache.tmp" ]; then
      record "PASS" "Sandbox Restore" "ignored generated files are non-durable by design" "ignored-build/cache.tmp absent after fresh local root"
    else
      record "FAIL" "Sandbox Restore" "ignored generated files are non-durable by design" "ignored file unexpectedly restored"
    fi
  else
    record "FAIL" "Sandbox Restore" "ignored generated files are non-durable by design" "remount failed"
  fi
  stop_mount "$mount_point_b" >/dev/null 2>&1 || true
  [ -n "$log_file_a" ] && [ -f "$log_file_a" ] && : >"$log_file_a"
  [ -n "$local_root_a" ] && [ -d "$local_root_a" ] && : >"$local_root_a/.keep" 2>/dev/null || true
}


main() {
  RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-git-feature.XXXXXX")"
  RUN_ROOT="$(cd "$RUN_ROOT" && pwd -P)"

  echo "=== drive9 git-feature-smoke-test ==="
  echo "BASE=$BASE"
  echo "CLI_SOURCE=$CLI_SOURCE"
  echo "RUN_ROOT=$RUN_ROOT"

  prepare_cli_binary
  record "PASS" "Prerequisites" "CLI binary ready" "$CLI_BIN"

  if [ -n "$DRIVE9_API_KEY" ]; then
    API_KEY="$DRIVE9_API_KEY"
    record "PASS" "Prerequisites" "use provided DRIVE9_API_KEY" "ok"
  else
    local prov body code
    prov="$(curl_body_code POST "$BASE/v1/provision")"
    code="$(http_code "$prov")"
    body="$(json_body "$prov")"
    if [ "$code" != "202" ] && [ "$code" != "200" ]; then
      fail_fast "Prerequisites" "POST /v1/provision" "http=$code body=$body"
    fi
    API_KEY="$(printf '%s' "$body" | jq -r '.api_key // empty')"
    if [ -z "$API_KEY" ]; then
      fail_fast "Prerequisites" "provision returns api_key" "missing api_key"
    fi
    record "PASS" "Prerequisites" "provision tenant" "ok"
  fi

  # Wait active
  local deadline st
  deadline=$(($(date +%s) + POLL_TIMEOUT_S))
  while :; do
    st="$(curl -sS -H "Authorization: Bearer $API_KEY" "$BASE/v1/status" | jq -r '.status // empty' 2>/dev/null || true)"
    if [ "$st" = "active" ]; then
      record "PASS" "Prerequisites" "tenant active" "ok"
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      fail_fast "Prerequisites" "tenant becomes active" "last_status=$st"
    fi
    sleep "$POLL_INTERVAL_S"
  done

  # FUSE prereqs (soft)
  if [ "$(uname -s)" = "Linux" ]; then
    if [ ! -e /dev/fuse ]; then
      if [ "$FUSE_STRICT_PREREQS" = "1" ]; then
        fail_fast "Prerequisites" "FUSE host prerequisites" "/dev/fuse missing"
      fi
      record "SKIP" "Prerequisites" "FUSE host prerequisites" "/dev/fuse missing"
      return 0
    fi
  fi
  record "PASS" "Prerequisites" "FUSE host prerequisites" "ok"

  local fixture_json fixture_root bare_repo file_url
  fixture_root="$RUN_ROOT/git-fixture"
  fixture_json="$(python3 "$SCRIPT_DIR/tools/git_fixture.py" "$fixture_root")"
  bare_repo="$(printf '%s' "$fixture_json" | jq -r '.bare_repo')"
  file_url="$(printf '%s' "$fixture_json" | jq -r '.file_url')"
  record "PASS" "Git Fixture" "local bare fixture repo generated" "$bare_repo"

  local git_root_rel="git-feature-smoke-$TS"
  drive9_retry fs mkdir ":/$git_root_rel" >/dev/null
  local git_mount="$RUN_ROOT/git-mount-a"
  local git_local="$RUN_ROOT/git-local-a"
  mkdir -p "$git_mount" "$git_local"
  if start_mount "$git_mount" "$RUN_ROOT/git-mount-a.log" --mode=fuse --profile=coding-agent --local-root "$git_local" --durability=interactive ":/$git_root_rel" "$git_mount"; then
    record "PASS" "Drive9 Git Workspace Behavior" "coding-agent mount starts" "mounted"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "coding-agent mount starts" "mount failed"
    return 1
  fi

  clone_drive9_repo "drive9 git clone --fast" "$bare_repo" "$git_mount/fast-full"
  clone_drive9_repo "drive9 git clone --fast --blobless --hydrate=off" "$file_url" "$git_mount/blobless-off" --blobless --hydrate=off
  clone_drive9_repo "drive9 git clone --fast --blobless --hydrate=sync" "$file_url" "$git_mount/blobless-sync" --blobless --hydrate=sync
  clone_drive9_repo "drive9 git clone --fast --blobless then explicit hydrate" "$file_url" "$git_mount/explicit-hydrate" --blobless --hydrate=off
  record_drive9_cmd "Git Clone Modes" "drive9 git hydrate explicit" "$GIT_FEATURE_TIMEOUT_S" git hydrate "$git_mount/explicit-hydrate"

  local ops_repo="$git_mount/ops"
  record_drive9_cmd "Git Clone Modes" "ops clone for full Git operation suite" "$GIT_FEATURE_TIMEOUT_S" git clone --fast --blobless --hydrate=sync "$file_url" "$ops_repo"
  run_git_readiness_checks "$ops_repo"
  run_git_ops_suite "$ops_repo" "$bare_repo" "$TS"
  run_git_flow_suite "$git_mount" "$file_url"

  local ws_json ws_id
  ws_json="$(curl -sS -H "Authorization: Bearer $API_KEY" "$BASE/v1/git-workspaces?root_path=/$git_root_rel/ops/" || true)"
  ws_id="$(printf '%s' "$ws_json" | jq -r '.workspace_id // empty' 2>/dev/null || true)"
  if [ -n "$ws_id" ]; then
    record "PASS" "Drive9 Git Workspace Behavior" "tree manifest registered" "workspace_id=$ws_id"
  else
    record "FAIL" "Drive9 Git Workspace Behavior" "tree manifest registered" "$ws_json"
  fi

  run_restore_suite "$git_root_rel" "$file_url" "$git_mount" "$git_local" "$RUN_ROOT/git-mount-a.log"

  if [ "$FAIL" -ne 0 ]; then
    return 1
  fi
  return 0
}

main "$@"

