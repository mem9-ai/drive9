#!/usr/bin/env bash
# drive9 Git workspace smoke test against a live deployment.

set -euo pipefail

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
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-8}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-2}"
GIT_WORKSPACE_REPOS="${GIT_WORKSPACE_REPOS:-drive9=https://github.com/mem9-ai/drive9.git,kimi-cli=https://github.com/MoonshotAI/kimi-cli.git,kimi-code=https://github.com/MoonshotAI/kimi-code.git}"
GIT_WORKSPACE_SCENARIOS="${GIT_WORKSPACE_SCENARIOS:-agent_edit_add_commit,agent_patch_apply,sandbox_restore,fast_worktree}"
GIT_WORKSPACE_EXISTING_FILES="${GIT_WORKSPACE_EXISTING_FILES:-20}"
GIT_WORKSPACE_NEW_FILES="${GIT_WORKSPACE_NEW_FILES:-20}"
GIT_WORKSPACE_PATCH_FILES="${GIT_WORKSPACE_PATCH_FILES:-20}"
GIT_WORKSPACE_CLONE_TIMEOUT_S="${GIT_WORKSPACE_CLONE_TIMEOUT_S:-600}"
GIT_WORKSPACE_GIT_TIMEOUT_S="${GIT_WORKSPACE_GIT_TIMEOUT_S:-120}"
GIT_WORKSPACE_HYDRATE="${GIT_WORKSPACE_HYDRATE:-sync}"
GIT_WORKSPACE_ALLOW_OTHER="${GIT_WORKSPACE_ALLOW_OTHER:-0}"
GIT_WORKSPACE_TRACE_GIT="${GIT_WORKSPACE_TRACE_GIT:-0}"
GIT_WORKSPACE_LOG_AUDIT_PATTERN="${GIT_WORKSPACE_LOG_AUDIT_PATTERN:-panic|fatal error|short read|input/output error}"

PASS=0
FAIL=0
TOTAL=0
MOUNT_PID=""
MOUNT_POINT=""
MOUNT_LOG=""
RUN_ROOT=""

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

start_mount() {
  local mount_point="$1"
  local local_root="$2"
  local mount_log="$3"
  MOUNT_POINT="$mount_point"
  MOUNT_LOG="$mount_log"
  mkdir -p "$MOUNT_POINT" "$local_root"
  {
    echo "=== drive9 git workspace mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "mount_point=$MOUNT_POINT"
    echo "local_root=$local_root"
  } >>"$MOUNT_LOG"

  local args=(mount --mode=fuse --profile=coding-agent --local-root "$local_root" --durability=interactive --perf-counters)
  if [ "$GIT_WORKSPACE_ALLOW_OTHER" = "1" ]; then
    args+=(--allow-other)
  fi
  args+=( :/ "$MOUNT_POINT" )
  drive9 "${args[@]}" >>"$MOUNT_LOG" 2>&1 &
  MOUNT_PID="$!"
  wait_mount_state mounted
  wait_mount_log_ready "$MOUNT_LOG"
}

stop_mount() {
  set +e
  if [ -n "${MOUNT_POINT:-}" ] && is_mounted "$MOUNT_POINT"; then
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT" >/dev/null 2>&1 || true
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
  if [ -n "${MOUNT_LOG:-}" ] && [ -f "$MOUNT_LOG" ]; then
    echo "=== drive9 mount log: $MOUNT_LOG ==="
    cat "$MOUNT_LOG"
  fi
}

audit_mount_log() {
  local log_file="$1"
  if [ ! -f "$log_file" ]; then
    echo "mount log missing: $log_file" >&2
    return 1
  fi
  if grep -Eina "$GIT_WORKSPACE_LOG_AUDIT_PATTERN" "$log_file"; then
    echo "mount log contains git workspace failure pattern" >&2
    return 1
  fi
  return 0
}

drive9() {
  DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

drive9_with_timeout() {
  local seconds="$1"
  shift
  run_with_timeout "$seconds" env "DRIVE9_SERVER=$BASE" "DRIVE9_API_KEY=$API_KEY" "$CLI_BIN" "$@"
}

git_cmd() {
  if [ "$GIT_WORKSPACE_TRACE_GIT" = "1" ]; then
    {
      printf '+ git'
      printf ' %q' "$@"
      printf '\n'
    } >&2
  fi
  run_with_timeout "$GIT_WORKSPACE_GIT_TIMEOUT_S" git "$@"
}

clone_fast_blobless() {
  local repo_url="$1"
  local target="$2"
  drive9_with_timeout "$GIT_WORKSPACE_CLONE_TIMEOUT_S" \
    git clone --fast --blobless "--hydrate=$GIT_WORKSPACE_HYDRATE" "$repo_url" "$target"
}

fast_worktree_add() {
  local base_repo="$1"
  local worktree="$2"
  local branch="$3"
  local commitish="$4"
  drive9_with_timeout "$GIT_WORKSPACE_CLONE_TIMEOUT_S" \
    git worktree add --fast --blobless "--hydrate=$GIT_WORKSPACE_HYDRATE" -b "$branch" "$base_repo" "$worktree" "$commitish"
}

configure_git_identity() {
  local repo="$1"
  git_cmd -C "$repo" config user.email "drive9-e2e@example.test" || return
  git_cmd -C "$repo" config user.name "Drive9 E2E" || return
}

select_and_append_existing() {
  local repo="$1"
  local count="$2"
  local marker="$3"
  local out_file="$4"
  python3 - "$repo" "$count" "$marker" "$out_file" <<'PY'
import os
import subprocess
import sys

repo, count, marker, out_file = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
raw = subprocess.check_output(["git", "-C", repo, "ls-files", "-z"])
selected = []
for rel_b in raw.split(b"\0"):
    if not rel_b:
        continue
    rel = rel_b.decode("utf-8", "surrogateescape")
    path = os.path.join(repo, rel)
    if not os.path.isfile(path) or os.path.islink(path):
        continue
    if os.path.getsize(path) > 256 * 1024:
        continue
    try:
        with open(path, "rb") as f:
            f.read(4096).decode("utf-8")
    except UnicodeDecodeError:
        continue
    selected.append(rel)
    if len(selected) >= count:
        break
if not selected:
    raise SystemExit("no editable tracked files selected")
for i, rel in enumerate(selected):
    with open(os.path.join(repo, rel), "ab") as f:
        f.write(("\n# drive9 git workspace e2e %s existing %04d\n" % (marker, i)).encode())
with open(out_file, "w", encoding="utf-8") as f:
    for rel in selected:
        f.write(rel + "\n")
print(len(selected))
PY
}

write_new_files() {
  local repo="$1"
  local count="$2"
  local marker="$3"
  python3 - "$repo" "$count" "$marker" <<'PY'
import os
import sys

repo, count, marker = sys.argv[1], int(sys.argv[2]), sys.argv[3]
root = os.path.join(repo, "agent-bench", "generated")
os.makedirs(root, exist_ok=True)
for i in range(count):
    with open(os.path.join(root, "file-%04d.md" % i), "w", encoding="utf-8") as f:
        f.write("# drive9 git workspace e2e\n\nmarker=%s\nindex=%04d\n" % (marker, i))
print(count)
PY
}

setup_ignored_local_only_probe() {
  local repo="$1"
  local marker="$2"
  mkdir -p "$repo/.git/info" || return
  printf '\nagent-bench/local-only/\n' >> "$repo/.git/info/exclude" || return
  mkdir -p "$repo/agent-bench/local-only" || return
  printf 'ignored local-only %s\n' "$marker" > "$repo/agent-bench/local-only/cache.txt" || return
  git_cmd -C "$repo" check-ignore -q agent-bench/local-only/cache.txt
}

assert_clean_status() {
  local repo="$1"
  local out
  out="$(git -C "$repo" status --porcelain=v1)" || return
  [ -z "$out" ]
}

assert_repo_ready() {
  local repo="$1"
  test -e "$repo/.git" || return
  git_cmd -C "$repo" rev-parse --is-inside-work-tree >/dev/null || return
  git_cmd -C "$repo" log --oneline -1 >/dev/null || return
  git_cmd -C "$repo" status --porcelain=v1 >/dev/null
}

commit_all() {
  local repo="$1"
  local message="$2"
  git_cmd -C "$repo" status --porcelain=v1 >/dev/null || return
  git_cmd -C "$repo" diff --stat >/dev/null || return
  git_cmd -C "$repo" add -A || return
  git_cmd -C "$repo" commit --no-verify -m "$message" >/dev/null || return
  assert_clean_status "$repo"
}

run_agent_edit_add_commit() {
  local slug="$1" repo_url="$2"
  local scenario="agent_edit_add_commit"
  local marker="${slug}-${scenario}-$(date +%s)"
  local case_root="$RUN_ROOT/$slug-$scenario"
  local mount_point="$case_root/mount"
  local local_root="$case_root/local-root"
  local log_file="$case_root/mount.log"
  local repo="$mount_point/$slug-$scenario"
  local selected="$case_root/selected.txt"
  mkdir -p "$case_root"

  echo "[repo=$slug scenario=$scenario] mount + fast blobless clone"
  check_cmd "$slug $scenario mount starts" start_mount "$mount_point" "$local_root" "$log_file"
  check_cmd "$slug $scenario fast blobless clone" clone_fast_blobless "$repo_url" "$repo"
  check_cmd "$slug $scenario repo ready" assert_repo_ready "$repo"
  configure_git_identity "$repo"
  check_cmd "$slug $scenario ignored local-only probe" setup_ignored_local_only_probe "$repo" "$marker"

  check_cmd "$slug $scenario append tracked files" select_and_append_existing "$repo" "$GIT_WORKSPACE_EXISTING_FILES" "$marker" "$selected"
  check_cmd "$slug $scenario write new files" write_new_files "$repo" "$GIT_WORKSPACE_NEW_FILES" "$marker"
  check_cmd "$slug $scenario git add commit" commit_all "$repo" "drive9 e2e $scenario"
  check_cmd "$slug $scenario ignored file stays untracked" git_cmd -C "$repo" check-ignore -q agent-bench/local-only/cache.txt
  check_cmd "$slug $scenario mount log audit" audit_mount_log "$log_file"
  stop_mount
}

run_agent_patch_apply() {
  local slug="$1" repo_url="$2"
  local scenario="agent_patch_apply"
  local marker="${slug}-${scenario}-$(date +%s)"
  local case_root="$RUN_ROOT/$slug-$scenario"
  local mount_point="$case_root/mount"
  local local_root="$case_root/local-root"
  local log_file="$case_root/mount.log"
  local repo="$mount_point/$slug-$scenario"
  local selected="$case_root/selected.txt"
  local patch_file="$case_root/agent.patch"
  mkdir -p "$case_root"

  echo "[repo=$slug scenario=$scenario] mount + fast blobless clone"
  check_cmd "$slug $scenario mount starts" start_mount "$mount_point" "$local_root" "$log_file"
  check_cmd "$slug $scenario fast blobless clone" clone_fast_blobless "$repo_url" "$repo"
  check_cmd "$slug $scenario repo ready" assert_repo_ready "$repo"
  configure_git_identity "$repo"

  check_cmd "$slug $scenario generate tracked patch" select_and_append_existing "$repo" "$GIT_WORKSPACE_PATCH_FILES" "$marker" "$selected"
  git_cmd -C "$repo" diff --binary > "$patch_file"
  check_cmd "$slug $scenario patch is nonempty" test -s "$patch_file"
  git_cmd -C "$repo" checkout -- .
  check_cmd "$slug $scenario clean before git apply" assert_clean_status "$repo"
  check_cmd "$slug $scenario git apply" git_cmd -C "$repo" apply "$patch_file"
  check_cmd "$slug $scenario git add commit" commit_all "$repo" "drive9 e2e $scenario"
  check_cmd "$slug $scenario mount log audit" audit_mount_log "$log_file"
  stop_mount
}

run_sandbox_restore() {
  local slug="$1" repo_url="$2"
  local scenario="sandbox_restore"
  local marker="${slug}-${scenario}-$(date +%s)"
  local case_root="$RUN_ROOT/$slug-$scenario"
  local mount_point="$case_root/mount"
  local local_root_a="$case_root/local-root-a"
  local local_root_b="$case_root/local-root-b"
  local log_a="$case_root/mount-a.log"
  local log_b="$case_root/mount-b.log"
  local repo="$mount_point/$slug-$scenario"
  local selected="$case_root/selected.txt"
  local status_before="$case_root/status-before.txt"
  local status_after="$case_root/status-after.txt"
  mkdir -p "$case_root"

  echo "[repo=$slug scenario=$scenario] initial mount + dirty index"
  check_cmd "$slug $scenario first mount starts" start_mount "$mount_point" "$local_root_a" "$log_a"
  check_cmd "$slug $scenario fast blobless clone" clone_fast_blobless "$repo_url" "$repo"
  check_cmd "$slug $scenario repo ready" assert_repo_ready "$repo"
  configure_git_identity "$repo"
  check_cmd "$slug $scenario append tracked files" select_and_append_existing "$repo" "$GIT_WORKSPACE_EXISTING_FILES" "$marker" "$selected"
  check_cmd "$slug $scenario write new files" write_new_files "$repo" "$GIT_WORKSPACE_NEW_FILES" "$marker"
  git_cmd -C "$repo" add -A
  git_cmd -C "$repo" status --porcelain=v1 > "$status_before"
  check_cmd "$slug $scenario dirty status before remount" test -s "$status_before"
  check_cmd "$slug $scenario first mount log audit" audit_mount_log "$log_a"
  stop_mount

  echo "[repo=$slug scenario=$scenario] remount with fresh local root"
  check_cmd "$slug $scenario second mount starts" start_mount "$mount_point" "$local_root_b" "$log_b"
  check_cmd "$slug $scenario .git restored" test -d "$repo/.git"
  git_cmd -C "$repo" status --porcelain=v1 > "$status_after"
  check_cmd "$slug $scenario status restored after remount" test -s "$status_after"
  check_cmd "$slug $scenario restored status contains generated files" grep -q "agent-bench/generated/file-0000.md" "$status_after"
  while IFS= read -r rel; do
    [ -z "$rel" ] && continue
    check_cmd "$slug $scenario restored tracked file exists: $rel" test -f "$repo/$rel"
    break
  done < "$selected"
  check_cmd "$slug $scenario second mount log audit" audit_mount_log "$log_b"
  stop_mount
}

run_fast_worktree() {
  local slug="$1" repo_url="$2"
  local scenario="fast_worktree"
  local marker="${slug}-${scenario}-$(date +%s)"
  local case_root="$RUN_ROOT/$slug-$scenario"
  local mount_point="$case_root/mount"
  local local_root_a="$case_root/local-root-a"
  local local_root_b="$case_root/local-root-b"
  local log_a="$case_root/mount-a.log"
  local log_b="$case_root/mount-b.log"
  local base_repo="$mount_point/$slug-$scenario-base"
  local worktree="$mount_point/$slug-$scenario-linked"
  local branch="drive9-e2e-${slug}-${scenario}-$(date +%s)"
  local selected_commit="$case_root/selected-commit.txt"
  local selected_staged="$case_root/selected-staged.txt"
  local status_before="$case_root/status-before.txt"
  local status_after="$case_root/status-after.txt"
  local cached_before="$case_root/cached-before.txt"
  local cached_after="$case_root/cached-after.txt"
  local commit_subject="drive9 e2e $scenario committed $marker"
  local staged_rel
  mkdir -p "$case_root"

  echo "[repo=$slug scenario=$scenario] initial mount + fast linked worktree"
  check_cmd "$slug $scenario first mount starts" start_mount "$mount_point" "$local_root_a" "$log_a"
  check_cmd "$slug $scenario fast blobless clone base" clone_fast_blobless "$repo_url" "$base_repo"
  check_cmd "$slug $scenario base repo ready" assert_repo_ready "$base_repo"
  configure_git_identity "$base_repo"
  check_cmd "$slug $scenario fast worktree add" fast_worktree_add "$base_repo" "$worktree" "$branch" "HEAD"
  check_cmd "$slug $scenario linked worktree ready" assert_repo_ready "$worktree"
  configure_git_identity "$worktree"
  check_cmd "$slug $scenario base lists linked worktree" bash -c 'git -C "$1" worktree list --porcelain | grep -q "^worktree $2$"' _ "$base_repo" "$worktree"

  check_cmd "$slug $scenario append commit files" select_and_append_existing "$worktree" "$GIT_WORKSPACE_EXISTING_FILES" "$marker-commit" "$selected_commit"
  check_cmd "$slug $scenario write commit files" write_new_files "$worktree" "$GIT_WORKSPACE_NEW_FILES" "$marker-commit"
  check_cmd "$slug $scenario linked worktree commit" commit_all "$worktree" "$commit_subject"
  check_eq "$slug $scenario commit subject before remount" "$(git -C "$worktree" log -1 --pretty=%s)" "$commit_subject"

  check_cmd "$slug $scenario append staged file" select_and_append_existing "$worktree" 1 "$marker-staged" "$selected_staged"
  staged_rel="$(head -n 1 "$selected_staged")"
  check_cmd "$slug $scenario git add staged file" git_cmd -C "$worktree" add -- "$staged_rel"
  mkdir -p "$worktree/agent-bench/worktree"
  printf 'unstaged linked worktree marker=%s\n' "$marker" > "$worktree/agent-bench/worktree/unstaged.txt"
  git_cmd -C "$worktree" diff --cached --name-only > "$cached_before"
  git_cmd -C "$worktree" status --porcelain=v1 > "$status_before"
  check_cmd "$slug $scenario cached diff before remount" grep -Fxq "$staged_rel" "$cached_before"
  check_cmd "$slug $scenario dirty status before remount" test -s "$status_before"
  check_cmd "$slug $scenario first mount log audit" audit_mount_log "$log_a"
  stop_mount

  echo "[repo=$slug scenario=$scenario] remount with fresh local root"
  check_cmd "$slug $scenario second mount starts" start_mount "$mount_point" "$local_root_b" "$log_b"
  check_cmd "$slug $scenario linked .git restored as file" test -f "$worktree/.git"
  check_cmd "$slug $scenario base .git restored" test -d "$base_repo/.git"
  check_cmd "$slug $scenario base worktree list restored" bash -c 'git -C "$1" worktree list --porcelain | grep -q "^worktree $2$"' _ "$base_repo" "$worktree"
  check_cmd "$slug $scenario linked worktree ready after remount" assert_repo_ready "$worktree"
  check_eq "$slug $scenario commit subject after remount" "$(git -C "$worktree" log -1 --pretty=%s)" "$commit_subject"
  git_cmd -C "$worktree" diff --cached --name-only > "$cached_after"
  git_cmd -C "$worktree" status --porcelain=v1 > "$status_after"
  check_cmd "$slug $scenario cached diff restored" cmp -s "$cached_before" "$cached_after"
  check_cmd "$slug $scenario status restored" cmp -s "$status_before" "$status_after"
  check_cmd "$slug $scenario unstaged file restored" grep -q "$marker" "$worktree/agent-bench/worktree/unstaged.txt"
  check_cmd "$slug $scenario fast worktree remove" drive9 git worktree remove --fast --force "$worktree"
  check_cmd "$slug $scenario linked worktree removed from git metadata" bash -c 'out=$(git -C "$1" worktree list --porcelain) && ! printf "%s\n" "$out" | grep -q "^worktree $2$"' _ "$base_repo" "$worktree"
  check_cmd "$slug $scenario second mount log audit" audit_mount_log "$log_b"
  stop_mount
}

run_repo_scenario() {
  local slug="$1" repo_url="$2" scenario="$3"
  case "$scenario" in
    agent_edit_add_commit) run_agent_edit_add_commit "$slug" "$repo_url" ;;
    agent_patch_apply) run_agent_patch_apply "$slug" "$repo_url" ;;
    sandbox_restore) run_sandbox_restore "$slug" "$repo_url" ;;
    fast_worktree) run_fast_worktree "$slug" "$repo_url" ;;
    *)
      echo "unknown GIT_WORKSPACE_SCENARIOS entry: $scenario" >&2
      FAIL=$((FAIL + 1))
      ;;
  esac
}

cleanup() {
  stop_mount
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ -n "${RUN_ROOT:-}" ] && [ -d "$RUN_ROOT" ]; then
    rm -rf "$RUN_ROOT"
  fi
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

echo "=== drive9 Git workspace smoke test ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "GIT_WORKSPACE_REPOS=$GIT_WORKSPACE_REPOS"
echo "GIT_WORKSPACE_SCENARIOS=$GIT_WORKSPACE_SCENARIOS"
echo "GIT_WORKSPACE_EXISTING_FILES=$GIT_WORKSPACE_EXISTING_FILES"
echo "GIT_WORKSPACE_NEW_FILES=$GIT_WORKSPACE_NEW_FILES"
echo "GIT_WORKSPACE_PATCH_FILES=$GIT_WORKSPACE_PATCH_FILES"
echo "GIT_WORKSPACE_HYDRATE=$GIT_WORKSPACE_HYDRATE"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
check_cmd "python3 is available" bash -c 'command -v python3 >/dev/null'
check_cmd "git is available" bash -c 'command -v git >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
else
  check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
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

RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-git-workspace-e2e.XXXXXX")"
RUN_ROOT="$(cd "$RUN_ROOT" && pwd -P)"
echo "RUN_ROOT=$RUN_ROOT"

IFS=',' read -r -a repo_specs <<< "$GIT_WORKSPACE_REPOS"
IFS=',' read -r -a scenarios <<< "$GIT_WORKSPACE_SCENARIOS"
for spec in "${repo_specs[@]}"; do
  slug="${spec%%=*}"
  repo_url="${spec#*=}"
  if [ -z "$slug" ] || [ -z "$repo_url" ] || [ "$slug" = "$repo_url" ]; then
    echo "invalid repo spec: $spec (expected slug=url)" >&2
    FAIL=$((FAIL + 1))
    continue
  fi
  for scenario in "${scenarios[@]}"; do
    run_repo_scenario "$slug" "$repo_url" "$scenario"
  done
done

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
