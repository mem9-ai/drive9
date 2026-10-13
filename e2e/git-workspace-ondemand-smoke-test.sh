#!/usr/bin/env bash
# e2e: FS-native git-workspace discovery (dormant → live).
#
# The git workspace store is /.drive9/git-workspaces/ (no dedicated API):
#   G0  no workspace under this RemoteRoot → the mount serves the classic
#       view only (dormant), incl. under FS noise with ".git" segments
#   G1  a live `git clone --fast --hydrate=sync` on a running mount
#       registers the workspace and the clean tree becomes visible without
#       remounting (SSE event → discovery reload)
#   G2  a fresh LocalRoot remount discovers via the remote index and
#       serves the clean tree + restored .git without re-cloning
#   G3  a second --fast on the same mount registers another workspace;
#       both stay visible and the index grows
#   AC5 dropping the index entries for the root → a remount is dormant
#       again for this RemoteRoot (the index is the visibility kill-switch)
#
# Tenant isolation: all index asserts filter by suite RemoteRoot (a reused
# tenant may already have other roots' entries; never require global 404).
#
# Usage:
#   DRIVE9_BASE=http://127.0.0.1:9009 DRIVE9_API_KEY=<provisioned-jwt> \
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
GIT_ONDEMAND_FIXTURE_TREE_FILES="${GIT_ONDEMAND_FIXTURE_TREE_FILES:-8}"
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

  drive9 mount --foreground --mode=fuse --profile "${FUSE_PROFILE:-coding-agent}" \
    --local-root "$local_root" \
    --durability=interactive \
    --no-auto-unpack \
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
  # Give the async store discovery a moment to settle before assertions.
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
    # Give the unmount checkpoint drain a short window before killing.
    local i
    for i in $(seq 1 20); do
      if ! kill -0 "$MOUNT_PID" >/dev/null 2>&1; then
        break
      fi
      sleep 0.5
    done
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
  if grep -Eina "$GIT_ONDEMAND_LOG_AUDIT_PATTERN" "$log_file"; then
    echo "mount log contains failure pattern" >&2
    return 1
  fi
  return 0
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

# Stat index revision via ?stat=1 (prefer over HEAD; some .drive9 paths hang).
# Prints "code revision" (revision empty when missing/404).
fetch_remote_index_stat_rev() {
  local url="$BASE/v1/fs/.drive9/git-workspaces/index.json?stat=1"
  local body_file code rev
  body_file="$(mktemp)"
  code=$(curl -sS --max-time 15 -o "$body_file" -w "%{http_code}" \
    -H "Authorization: Bearer $API_KEY" "$url" || printf '000')
  rev=""
  if [ "$code" = "200" ]; then
    # Distinguish null (missing) from revision 0; never use // empty on numbers.
    rev=$(jq -r 'if .revision == null then empty else (.revision|tostring) end' <"$body_file" 2>/dev/null || true)
  fi
  rm -f "$body_file"
  printf '%s %s' "$code" "$rev"
}

# Rewrite remote index removing entries under remote_root.
# API DELETE alone does not maintain the index; empty/filtered index is required
# for AC5 remount → DORMANT (refresh=0). Uses X-Dat9-Expected-Revision CAS
# (same contract as client.WriteCtxConditionalWithRevision); 409 → retry.
# Fails hard if entries for this root remain after the last attempt.
rewrite_index_without_root() {
  local remote_root="$1"
  local attempt code body new_body url put_code remaining rev stat_out
  url="$BASE/v1/fs/.drive9/git-workspaces/index.json"
  for attempt in 1 2 3 4 5 6; do
    stat_out=$(fetch_remote_index_stat_rev)
    code=$(printf '%s' "$stat_out" | awk '{print $1}')
    rev=$(printf '%s' "$stat_out" | awk '{print $2}')
    if [ "$code" = "404" ]; then
      return 0
    fi
    if [ "$code" = "000" ]; then
      echo "rewrite_index: index stat transport failure (attempt $attempt)" >&2
      sleep 0.5
      continue
    fi
    if [ "$code" != "200" ]; then
      echo "rewrite_index: unexpected index stat status $code (attempt $attempt)" >&2
      sleep 0.5
      continue
    fi
    if [ -z "$rev" ]; then
      # Legacy servers may omit revision; fall back to unconditioned PUT with a warning.
      echo "rewrite_index: warning: missing revision from ?stat=1; PUT without CAS (attempt $attempt)" >&2
    fi
    body=$(fetch_remote_index_body)
    if [ -z "$body" ] || ! printf '%s' "$body" | jq -e . >/dev/null 2>&1; then
      echo "rewrite_index: index body unreadable/invalid (attempt $attempt)" >&2
      sleep 0.5
      continue
    fi
    new_body=$(printf '%s' "$body" | jq -c --arg root "$remote_root" '
      .workspaces = ([.workspaces // []
        | .[]
        | select(
            ((.root_path // "") != $root)
            and (((.root_path // "") | startswith($root + "/")) | not)
          )])
      | .updated_at = (now | todateiso8601)
      | .version = (.version // 1)
    ') || true
    if [ -z "$new_body" ]; then
      echo "rewrite_index: filtered body empty (attempt $attempt)" >&2
      sleep 0.5
      continue
    fi
    # Existing objects start at revision >= 1; rev "0" is create-only — do not CAS with 0.
    if [ -n "$rev" ] && [ "$rev" != "0" ]; then
      put_code=$(curl -sS --max-time 30 -o /dev/null -w "%{http_code}" -X PUT \
        -H "Authorization: Bearer $API_KEY" \
        -H "Content-Type: application/json" \
        -H "X-Dat9-Expected-Revision: $rev" \
        -d "$new_body" \
        "$url" || printf '000')
    else
      put_code=$(curl -sS --max-time 30 -o /dev/null -w "%{http_code}" -X PUT \
        -H "Authorization: Bearer $API_KEY" \
        -H "Content-Type: application/json" \
        -d "$new_body" \
        "$url" || printf '000')
    fi
    if [ "$put_code" = "409" ]; then
      echo "rewrite_index: index CAS conflict (attempt $attempt)" >&2
      sleep 0.5
      continue
    fi
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

# Poll until the index reports zero under remote_root (or timeout).
wait_root_cleared() {
  local remote_root="$1"
  local timeout_s="${2:-20}"
  local deadline i_n
  deadline=$(( $(date +%s) + timeout_s ))
  while :; do
    i_n=$(index_count_for_root "$remote_root" 2>/dev/null || echo "?")
    if [ "$i_n" = "0" ]; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "wait_root_cleared: timeout index=$i_n root=$remote_root" >&2
      return 1
    fi
    sleep 1
  done
}

# Deregister every workspace under remote_root: rewrite the index without
# the root. The store subtrees stay (crash-safe by design; a later sweep
# collects orphans) — with no index entry no mount consumes them.
cleanup_suite_workspaces() {
  local remote_root="$1"
  rewrite_index_without_root "$remote_root"
}

wait_path_has_fixture_readme() {
  local path="$1"
  local i last_obs
  for i in $(seq 1 40); do
    if [ -f "$path" ] && grep -q "Drive9 fixture" "$path" 2>/dev/null; then
      return 0
    fi
    last_obs="$(ls -la "$path" 2>&1 | head -2; head -c 64 "$path" 2>&1 | head -1)"
    # Discovery is event-driven; the ls is only a benign poll tick.
    ls "$(dirname "$path")" >/dev/null 2>&1 || true
    sleep 1
  done
  echo "wait_path_has_fixture_readme: $path never satisfied; last observation:" >&2
  printf '%s\n' "$last_obs" >&2
  # Store forensics: does the fs-native tree content actually exist where
  # the mount resolves it? Dump index, manifests, and tree listings.
  local idx_dump id head_commit rel_repo rel_base
  rel_repo="$(basename "$(dirname "$path")")"
  rel_base="$(basename "$path")"
  idx_dump="$(curl -sS -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/.drive9/git-workspaces/index.json" || true)"
  echo "index: $(printf '%s' "$idx_dump" | head -c 600)" >&2
  for id in $(printf '%s' "$idx_dump" | jq -r --arg root "$REMOTE_ROOT/$rel_repo" '.workspaces[]? | select((.root_path|sub("/$";""))==$root) | .workspace_id' 2>/dev/null); do
    echo "manifest $id: $(curl -sS -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/.drive9/git-workspaces/$id/manifest.json" || true | head -c 400)" >&2
    head_commit="$(curl -sS -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/.drive9/git-workspaces/$id/manifest.json" 2>/dev/null | jq -r '.head_commit // empty')"
    if [ -n "$head_commit" ]; then
      echo "tree $id/$head_commit listing:" >&2
      curl -sS -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/.drive9/git-workspaces/$id/tree/$head_commit/?list=1" 2>&1 | head -c 500 >&2
      echo >&2
      echo "direct read of $rel_base:" >&2
      curl -sS -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs/.drive9/git-workspaces/$id/tree/$head_commit/$rel_base" 2>&1 | head -c 200 >&2
      echo >&2
    fi
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

# Generate ordinary FS noise, including paths with a ".git" segment: a
# dormant mount (no workspaces) must serve it as plain classic paths.
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
  # Existing-tenant CI accumulates timestamped remote trees; always
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
check_cmd "A dormant FS activity (incl .git segments)" dormant_fs_activity "$mount_a"
check_cmd "A index has no entries for suite RemoteRoot" index_has_no_entries_for_root "$REMOTE_ROOT"
check_cmd "A mount log audit" audit_mount_log "$log_a"
stop_mount

# ---------------------------------------------------------------------------
# Scenario B — G1: same LocalRoot live --fast registers and serves
# ---------------------------------------------------------------------------
echo "[B] G1 live --fast on same LocalRoot"
mount_b="$RUN_ROOT/mnt-b"
local_b="$RUN_ROOT/local-b"
log_b="$RUN_ROOT/mount-b.log"
check_cmd "B mount starts" start_mount "$mount_b" "$local_b" "$log_b" "$REMOTE_ROOT"
repo_b="$mount_b/repo-main"
check_cmd "B clone --fast" \
  drive9_with_timeout "$GIT_ONDEMAND_CLONE_TIMEOUT_S" git clone --fast --blobless --hydrate=sync "$FIXTURE_URL" "$repo_b"
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
check_cmd "B mount log audit" audit_mount_log "$log_b"
stop_mount

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

# ---------------------------------------------------------------------------
# Scenario D — G3: second --fast on a live mount registers another workspace
# ---------------------------------------------------------------------------
echo "[D] G3 second --fast on the same mount"
mount_d="$RUN_ROOT/mnt-d"
local_d="$RUN_ROOT/local-d"
log_d="$RUN_ROOT/mount-d.log"
check_cmd "D mount starts" start_mount "$mount_d" "$local_d" "$log_d" "$REMOTE_ROOT"
repo_d1="$mount_d/repo-d1"
repo_d2="$mount_d/repo-d2"
check_cmd "D first clone --fast" \
  drive9_with_timeout "$GIT_ONDEMAND_CLONE_TIMEOUT_S" git clone --fast --blobless --hydrate=sync "$FIXTURE_URL" "$repo_d1"
check_cmd "D first repo ready" wait_repo_git_ready "$repo_d1"
# Baseline after first --fast: at least one workspace under this root.
# Do not default a transport failure to "0" — that softens the growth check.
ws_after_first=""
if ws_after_first=$(index_count_for_root "$REMOTE_ROOT"); then
  :
else
  ws_after_first="err"
fi
check_ge "D remote index >=1 workspace after first --fast" "$ws_after_first" "1"
check_cmd "D second clone --fast (new root)" \
  drive9_with_timeout "$GIT_ONDEMAND_CLONE_TIMEOUT_S" git clone --fast --blobless --hydrate=sync "$FIXTURE_URL" "$repo_d2"
check_cmd "D second repo ready" wait_repo_git_ready "$repo_d2"
check_cmd "D first repo README still visible" wait_path_has_fixture_readme "$repo_d1/README.md"
check_cmd "D second repo README visible" wait_path_has_fixture_readme "$repo_d2/README.md"
check_cmd "D mount log audit" audit_mount_log "$log_d"
stop_mount
ws_count=$(index_count_for_root "$REMOTE_ROOT") || ws_count="err"
check_ge "D remote index returns >=2 workspace entries under suite RemoteRoot" "$ws_count" "2"
# Growth after second clone proves the second registration landed.
if [ "$ws_after_first" != "err" ] && [ -n "$ws_after_first" ]; then
  check_ge "D remote index grew after second --fast" "$ws_count" "$((ws_after_first + 1))"
else
  check_cmd "D remote index grew after second --fast (baseline unreadable)" false
fi
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

# ---------------------------------------------------------------------------
# Scenario F — AC5: drop the index entries for the root → a remount is
# dormant again for this RemoteRoot (the index is the kill-switch).
# ---------------------------------------------------------------------------
echo "[F] AC5 deregister suite workspaces then remount stays dormant"
check_cmd "F cleanup suite workspaces + rewrite index" cleanup_suite_workspaces "$REMOTE_ROOT"
check_cmd "F wait root cleared (index)" wait_root_cleared "$REMOTE_ROOT" 20
check_cmd "F index has no entries for suite RemoteRoot after cleanup" \
  index_has_no_entries_for_root "$REMOTE_ROOT"
mount_f="$RUN_ROOT/mnt-f"
local_f="$RUN_ROOT/local-f"
log_f="$RUN_ROOT/mount-f.log"
check_cmd "F remount starts after cleanup" start_mount "$mount_f" "$local_f" "$log_f" "$REMOTE_ROOT"
check_cmd "F dormant FS activity after cleanup" dormant_fs_activity "$mount_f" 15
# Dormant is observable: with no index entry the workspace view is gone.
check_cmd "F deregistered repo paths stay absent from the dormant view" \
  bash -c '! test -e "$mount_f/repo-main" && ! test -e "$mount_f/repo-d1"'
check_cmd "F mount log audit" audit_mount_log "$log_f"
stop_mount

echo
echo "=== drive9 git-workspace on-demand e2e result ==="
echo "TOTAL=$TOTAL PASS=$PASS FAIL=$FAIL"
echo "artifacts=$RUN_ROOT"

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
exit 0
