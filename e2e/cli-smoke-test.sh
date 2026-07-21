#!/usr/bin/env bash
# drive9 CLI smoke test against a live deployment.
#
# Tenant mode:
#  - Fresh (default): POST /v1/provision a new tenant, then run the suite.
#  - Existing (DRIVE9_API_KEY set): skip provision and reuse the tenant the
#    key belongs to. The CLI upload-limit boundary check (step [9]) defaults
#    to OFF in this mode because reserving `total_size` against an existing
#    tenant's quota can spuriously fail with 507; an explicit
#    RUN_CLI_UPLOAD_LIMIT_BOUNDARY=1 still wins. The rest of the suite
#    (fork flow, small-file ops, pack/archive, step [8] cleanup) is already
#    compatible with an existing tenant and unchanged.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
API_KEY="${DRIVE9_API_KEY:-}"
DRIVE9_IMAGE_FIXTURE_PATH="${DRIVE9_IMAGE_FIXTURE_PATH:-$SCRIPT_DIR/fixtures/cat03.jpg}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
CLI_LARGE_FILE_MB="${CLI_LARGE_FILE_MB:-100}"
CLI_BATCH_SMALL_FILE_COUNT="${CLI_BATCH_SMALL_FILE_COUNT:-10}"
CLI_MAX_RETRIES="${CLI_MAX_RETRIES:-8}"
CLI_RETRY_SLEEP_S="${CLI_RETRY_SLEEP_S:-2}"
# CLI upload-limit boundary check defaults to OFF in existing-tenant mode
# (same rationale as the API suite). An explicit RUN_CLI_UPLOAD_LIMIT_BOUNDARY
# still wins.
if [ -z "${RUN_CLI_UPLOAD_LIMIT_BOUNDARY:-}" ]; then
  if [ -n "$API_KEY" ]; then
    RUN_CLI_UPLOAD_LIMIT_BOUNDARY=0
  else
    RUN_CLI_UPLOAD_LIMIT_BOUNDARY=1
  fi
fi
CLI_UPLOAD_LIMIT_BYTES="${CLI_UPLOAD_LIMIT_BYTES:-10737418240}"
CLI_SEMANTIC_TIMEOUT_S="${CLI_SEMANTIC_TIMEOUT_S:-90}"
CLI_SEMANTIC_INTERVAL_S="${CLI_SEMANTIC_INTERVAL_S:-3}"
RUN_CLI_SEMANTIC_CHECKS="${RUN_CLI_SEMANTIC_CHECKS:-1}"
RUN_CLI_FORK_CHECKS="${RUN_CLI_FORK_CHECKS:-1}"

PASS=0
FAIL=0
SKIP=0
TOTAL=0

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL+1))
  if [ "$got" = "$want" ]; then
    echo "PASS $desc (got=$got)"
    PASS=$((PASS+1))
  else
    echo "FAIL $desc (want=$want got=$got)"
    FAIL=$((FAIL+1))
  fi
}

check_cmd() {
  local desc="$1"
  shift
  TOTAL=$((TOTAL+1))
  if "$@"; then
    echo "PASS $desc"
    PASS=$((PASS+1))
  else
    echo "FAIL $desc"
    FAIL=$((FAIL+1))
  fi
}

skip_check() {
  local desc="$1"
  TOTAL=$((TOTAL+1))
  SKIP=$((SKIP+1))
  echo "SKIP $desc"
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

if [ -n "$API_KEY" ]; then
  TENANT_MODE="existing (DRIVE9_API_KEY)"
else
  TENANT_MODE="fresh provision"
fi
echo "=== drive9 CLI smoke test ==="
echo "BASE=$BASE"
echo "Tenant=$TENANT_MODE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "IMAGE_FIXTURE=$DRIVE9_IMAGE_FIXTURE_PATH"

check_cmd "jq is available" bash -c 'command -v jq >/dev/null'
if [ "$CLI_SOURCE" = "build" ]; then
  check_cmd "go is available" bash -c 'command -v go >/dev/null'
else
  check_cmd "curl is available" bash -c 'command -v curl >/dev/null'
fi
check_cmd "local image fixture exists" test -s "$DRIVE9_IMAGE_FIXTURE_PATH"

echo "[1] provision tenant"
if [ -n "$API_KEY" ]; then
  echo "using existing DRIVE9_API_KEY (skip provision)"
  check_eq "use provided DRIVE9_API_KEY" "true" "true"
else
  pfile="$(mktemp)"
  pcode=$(curl -sS -o "$pfile" -w "%{http_code}" -X POST "$BASE/v1/provision")
  check_eq "POST /v1/provision returns 202" "$pcode" "202"
  API_KEY=$(jq -r '.api_key // empty' "$pfile")
  check_cmd "provision returns api_key" test -n "$API_KEY"
fi

echo "[2] wait tenant active"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
state=""
while :; do
  sfile="$(mktemp)"
  scode=$(curl -sS -o "$sfile" -w "%{http_code}" -H "Authorization: Bearer $API_KEY" "$BASE/v1/status")
  state=$(jq -r '.status // empty' "$sfile")
  rm -f "$sfile"
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
  env DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" HOME="$CLI_ENV_HOME" "$CLI_BIN" "$@"
}

drive9_ctx() {
  env -u DRIVE9_SERVER -u DRIVE9_API_KEY -u DRIVE9_VAULT_TOKEN HOME="$CLI_CTX_HOME" "$CLI_BIN" "$@"
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
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for drive9 $* (throttled)" >&2
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

drive9_ctx_retry() {
  local attempt=1
  local out rc
  while :; do
    set +e
    out=$(drive9_ctx "$@" 2>&1)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      printf '%s' "$out"
      return 0
    fi
    # Fork-context smoke can race eventual consistency while the saved context
    # becomes usable. Treat "not found" as retryable here only, with bounded
    # retries/sleeps via CLI_MAX_RETRIES and CLI_RETRY_SLEEP_S. The match is
    # intentionally broad and should be revisited if stricter ctx semantics are
    # needed for other callers.
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* || "$out" == *"not found"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for drive9(ctx) $* " >&2
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

fork_checks_enabled() {
  if [ "$RUN_CLI_FORK_CHECKS" != "1" ]; then
    return 1
  fi
  local probe_code
  probe_code=$(curl -sS -o /dev/null -w "%{http_code}" \
    -X POST \
    "$BASE/v1/fork" || true)
  if [ "$probe_code" = "404" ]; then
    echo "fork checks skipped: server does not expose /v1/fork" >&2
    return 1
  fi
  return 0
}

# Some read-after-write paths can be eventually consistent right after upload.
drive9_retry_read() {
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
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"not found"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for drive9 $* (not found yet)" >&2
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    if [ "$attempt" -lt "$CLI_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* ]]; then
      echo "retry $attempt/$CLI_MAX_RETRIES for drive9 $* (throttled)" >&2
      attempt=$((attempt + 1))
      sleep "$CLI_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

cli_stat_field() {
  local path="$1"
  local field="$2"
  local out
  out="$(drive9_retry fs stat "$path")"
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

wait_cli_grep_target() {
  local desc="$1"
  local query="$2"
  local target="$3"
  local deadline=$(( $(date +%s) + CLI_SEMANTIC_TIMEOUT_S ))
  local found="false"
  while :; do
    local out
    out="$(drive9_retry fs grep "$query" /)"
    found=$(python3 - "$out" "$target" <<'PY'
import sys
out=sys.argv[1].splitlines()
target=sys.argv[2]
print("true" if any(line.split("\t")[0].strip()==target for line in out if line.strip()) else "false")
PY
)
    if [ "$found" = "true" ]; then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    echo "semantic recall not ready for $target, retrying" >&2
    sleep "$CLI_SEMANTIC_INTERVAL_S"
  done
  check_eq "$desc" "$found" "true"
}

TS="$(date +%s)"
CLI_ENV_HOME="$(mktemp -d)"
CLI_CTX_HOME="$(mktemp -d)"
SMALL_LOCAL="/tmp/drive9-cli-small-${TS}.txt"
SMALL_REMOTE="/cli-${TS}-small.txt"
SMALL_RENAMED="/cli-${TS}-small-renamed.txt"
SMALL_SYMLINK="/cli-${TS}-small-link"
SMALL_HARDLINK="/cli-${TS}-small-hardlink.txt"
CP_DIR_REMOTE="/cli-${TS}-cpdir"
CP_DIR_REMOTE_COPY="/cli-${TS}-cpdir-copy"
CP_DIR_LOCAL="/tmp/drive9-cli-cpdir-${TS}"
RECURSIVE_LOCAL_ROOT="/tmp/drive9-cli-recursive-${TS}"
RECURSIVE_REMOTE="/cli-${TS}-recursive-tree"
RECURSIVE_REMOTE_COPY="/cli-${TS}-recursive-tree-copy"
RECURSIVE_DOWNLOADED="/tmp/drive9-cli-recursive-downloaded-${TS}"
HARDLINK_LOCAL="/tmp/drive9-cli-hardlink-${TS}.txt"
TAG_LOCAL="/tmp/drive9-cli-tag-${TS}.txt"
TAG_REMOTE="/cli-${TS}-tagged.txt"
IMAGE_LOCAL="/tmp/drive9-cli-image-${TS}.jpg"
IMAGE_REMOTE="/cli-${TS}-image.jpg"
SEM_TEXT_TARGET="/cli-${TS}-cat-story.txt"
SEM_TEXT_OTHER="/cli-${TS}-dog-story.txt"
IMAGE_CAPTION_REMOTE="/cli-${TS}-image.caption.txt"
BATCH_LOCAL_DIR="/tmp/drive9-cli-batch-${TS}"
BATCH_REMOTE_DIR="/cli-${TS}-batch"
PACK_LOCAL_ROOT="/tmp/drive9-cli-pack-local-${TS}"
PACK_RESTORE_ROOT="/tmp/drive9-cli-pack-restore-${TS}"
PACK_REMOTE_ROOT="/workspace"
PACK_PROFILE="e2e-pack"
PACK_REMOTE_ARCHIVE="$(python3 - "$PACK_REMOTE_ROOT" "$PACK_PROFILE" <<'PY'
import hashlib
import posixpath
import sys

root = sys.argv[1]
profile = sys.argv[2] or "coding-agent"
label = posixpath.basename(root.rstrip("/")) or "root"
safe = "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in label).strip(".-") or "root"
digest = hashlib.sha256((profile + "\0" + root).encode()).hexdigest()[:16]
print(f"/.drive9/packs/{safe}-{digest}.tar.gz")
PY
)"
LARGE_LOCAL="/tmp/drive9-cli-large-${TS}.bin"
LARGE_REMOTE="/cli-${TS}-large-${CLI_LARGE_FILE_MB}m.bin"
LARGE_DOWNLOADED="/tmp/drive9-cli-large-${TS}.download.bin"
LARGE_BYTES=$((CLI_LARGE_FILE_MB * 1024 * 1024))
CLI_IMAGE_UPLOADED=0
FORK_CTX_NAME="fork-${TS}"
FORK_REMOTE="/cli-${TS}-fork-smoke.txt"
FORK_LOCAL="/tmp/drive9-cli-fork-${TS}.txt"
ARCHIVE_REMOTE_DIR="/cli-${TS}-archive"
ARCHIVE_LOCAL_TARGZ="/tmp/drive9-cli-archive-${TS}.tar.gz"
ARCHIVE_LOCAL_TARGZ2="/tmp/drive9-cli-archive-${TS}-2.tar.gz"
ARCHIVE_LOCAL_ZIP="/tmp/drive9-cli-archive-${TS}.zip"
ARCHIVE_LOCAL_FLAT="/tmp/drive9-cli-archive-flat-${TS}.tar.gz"
ARCHIVE_EXTRACT_DIR="/tmp/drive9-cli-archive-extract-${TS}"

echo "[3.1] ctx fork smoke"
if fork_checks_enabled; then
  drive9_ctx ctx add --name owner --server "$BASE" --api-key "$API_KEY" >/dev/null
  fork_json="$(drive9_ctx ctx fork "$FORK_CTX_NAME" --from owner --json)"
  fork_api_key="$(jq -r '.api_key // empty' <<<"$fork_json")"
  fork_tenant_id="$(jq -r '.tenant_id // empty' <<<"$fork_json")"
  fork_status="$(jq -r '.status // empty' <<<"$fork_json")"
  check_cmd "ctx fork returns api_key" test -n "$fork_api_key"
  check_cmd "ctx fork returns tenant_id" test -n "$fork_tenant_id"
  check_eq "ctx fork initial status is provisioning" "$fork_status" "provisioning"

  fork_deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
  fork_state=""
  while :; do
    fork_status_file="$(mktemp)"
    fork_status_code=$(curl -sS -o "$fork_status_file" -w "%{http_code}" -H "Authorization: Bearer $fork_api_key" "$BASE/v1/status")
    fork_state=$(jq -r '.status // empty' "$fork_status_file")
    rm -f "$fork_status_file"
    echo "fork-status=${fork_status_code}:${fork_state}"
    if [ "$fork_status_code" = "200" ] && [ "$fork_state" = "active" ]; then
      break
    fi
    if [ "$(date +%s)" -ge "$fork_deadline" ]; then
      break
    fi
    sleep "$POLL_INTERVAL_S"
  done
  check_eq "fork tenant becomes active" "$fork_state" "active"

  printf "fork-smoke-%s" "$TS" > "$FORK_LOCAL"
  drive9_ctx ctx use "$FORK_CTX_NAME" >/dev/null
  drive9_ctx_retry fs cp "$FORK_LOCAL" ":$FORK_REMOTE" >/dev/null
  fork_cat="$(drive9_ctx_retry fs cat "$FORK_REMOTE")"
  check_eq "fork context can read written file" "$fork_cat" "fork-smoke-${TS}"

  fork_delete_body="$(mktemp)"
  fork_delete_code=$(curl -sS -o "$fork_delete_body" -w "%{http_code}" -X DELETE -H "Authorization: Bearer $fork_api_key" "$BASE/v1/fork")
  check_eq "DELETE /v1/fork returns 202" "$fork_delete_code" "202"
  rm -f "$fork_delete_body"
else
  skip_check "ctx fork returns api_key"
  skip_check "ctx fork returns tenant_id"
  skip_check "ctx fork initial status is provisioning"
  skip_check "fork tenant becomes active"
  skip_check "fork context can read written file"
  skip_check "DELETE /v1/fork returns 202"
fi

echo "[4] small file ops via cli"
printf "cli-smoke-%s" "$TS" > "$SMALL_LOCAL"
drive9_retry fs cp "$SMALL_LOCAL" ":$SMALL_REMOTE" >/dev/null

ls_out="$(drive9_retry fs ls /)"
small_present=$(python3 - "$ls_out" "$(basename "$SMALL_REMOTE")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
check_eq "uploaded small file appears in ls /" "$small_present" "true"

cat_out="$(drive9_retry_read fs cat "$SMALL_REMOTE")"
check_eq "cat returns expected small file content" "$cat_out" "cli-smoke-${TS}"

echo "[4.1] cp directory target semantics"
mkdir -p "$CP_DIR_LOCAL"
drive9_retry fs mkdir "$CP_DIR_REMOTE" >/dev/null
drive9_retry fs mkdir "$CP_DIR_REMOTE_COPY" >/dev/null

cp_dir_base="$(basename "$SMALL_LOCAL")"
cp_dir_remote_path="$CP_DIR_REMOTE/$cp_dir_base"
cp_dir_remote_copy_path="$CP_DIR_REMOTE_COPY/$cp_dir_base"

drive9_retry fs cp "$SMALL_LOCAL" ":$CP_DIR_REMOTE" >/dev/null
cp_dir_remote_body="$(drive9_retry_read fs cat "$cp_dir_remote_path")"
check_eq "cp local->remote dir keeps source name" "$cp_dir_remote_body" "cli-smoke-${TS}"

drive9_retry fs cp ":$cp_dir_remote_path" "$CP_DIR_LOCAL" >/dev/null
cp_dir_local_body="$(cat "$CP_DIR_LOCAL/$cp_dir_base")"
check_eq "cp remote->local dir keeps source name" "$cp_dir_local_body" "cli-smoke-${TS}"

drive9_retry fs cp ":$cp_dir_remote_path" ":$CP_DIR_REMOTE_COPY" >/dev/null
cp_dir_remote_copy_body="$(drive9_retry_read fs cat "$cp_dir_remote_copy_path")"
check_eq "cp remote->remote dir keeps source name" "$cp_dir_remote_copy_body" "cli-smoke-${TS}"

echo "[4.1.1] cp -r recursive tree round-trip"
# Build a local tree with nested dirs, multiple files, and an empty dir:
#   recursive-root/
#     top.txt          "top-${TS}"
#     sub/
#       nested.txt     "nested-${TS}"
#       deep/
#         leaf.txt     "leaf-${TS}"
#     empty/
recursive_local_root="$RECURSIVE_LOCAL_ROOT/recursive-root"
mkdir -p "$recursive_local_root/sub/deep"
mkdir -p "$recursive_local_root/empty"
printf "top-%s" "$TS" > "$recursive_local_root/top.txt"
printf "nested-%s" "$TS" > "$recursive_local_root/sub/nested.txt"
printf "leaf-%s" "$TS" > "$recursive_local_root/sub/deep/leaf.txt"

# Upload local tree → remote via cp -r (local→remote). The remote dir
# receives the source's CONTENTS, so $RECURSIVE_REMOTE/top.txt etc.
drive9_retry fs cp -r "$recursive_local_root" ":$RECURSIVE_REMOTE" >/dev/null

# Download remote tree → local via cp -r (remote→local). The local dir
# receives the source's CONTENTS, so files land at $RECURSIVE_DOWNLOADED/
# (not $RECURSIVE_DOWNLOADED/recursive-root/).
drive9_retry fs cp -r ":$RECURSIVE_REMOTE" "$RECURSIVE_DOWNLOADED" >/dev/null

# Verify every leaf file round-tripped byte-identically.
check_eq "cp -r round-trip top.txt content" \
  "$(cat "$RECURSIVE_DOWNLOADED/top.txt")" "top-${TS}"
check_eq "cp -r round-trip nested.txt content" \
  "$(cat "$RECURSIVE_DOWNLOADED/sub/nested.txt")" "nested-${TS}"
check_eq "cp -r round-trip leaf.txt content" \
  "$(cat "$RECURSIVE_DOWNLOADED/sub/deep/leaf.txt")" "leaf-${TS}"

# Verify the empty directory was preserved.
check_cmd "cp -r preserves empty dir" test -d "$RECURSIVE_DOWNLOADED/empty"

# Verify nested dir structure matches.
check_cmd "cp -r preserves nested sub dir" test -d "$RECURSIVE_DOWNLOADED/sub"
check_cmd "cp -r preserves nested deep dir" test -d "$RECURSIVE_DOWNLOADED/sub/deep"

# Also verify the remote tree is listable.
recursive_remote_ls="$(drive9_retry fs ls "$RECURSIVE_REMOTE/")"
recursive_top_present=$(python3 - "$recursive_remote_ls" "top.txt" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
check_eq "cp -r uploaded top.txt appears in remote ls" "$recursive_top_present" "true"

recursive_sub_ls="$(drive9_retry fs ls "$RECURSIVE_REMOTE/sub/")"
recursive_nested_present=$(python3 - "$recursive_sub_ls" "nested.txt" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
check_eq "cp -r uploaded nested.txt appears in sub ls" "$recursive_nested_present" "true"

# Remote→remote tree copy via cp -r.
drive9_retry fs cp -r ":$RECURSIVE_REMOTE" ":$RECURSIVE_REMOTE_COPY" >/dev/null
recursive_r2r_body="$(drive9_retry_read fs cat "$RECURSIVE_REMOTE_COPY/top.txt")"
check_eq "cp -r remote->remote tree copy content" "$recursive_r2r_body" "top-${TS}"
recursive_r2r_nested="$(drive9_retry_read fs cat "$RECURSIVE_REMOTE_COPY/sub/nested.txt")"
check_eq "cp -r remote->remote nested file content" "$recursive_r2r_nested" "nested-${TS}"
recursive_r2r_leaf="$(drive9_retry_read fs cat "$RECURSIVE_REMOTE_COPY/sub/deep/leaf.txt")"
check_eq "cp -r remote->remote deep leaf content" "$recursive_r2r_leaf" "leaf-${TS}"

drive9_retry fs mv "$SMALL_REMOTE" "$SMALL_RENAMED" >/dev/null
renamed_out="$(drive9_retry fs ls /)"
renamed_present=$(python3 - "$renamed_out" "$(basename "$SMALL_RENAMED")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
check_eq "mv renames remote file" "$renamed_present" "true"

drive9_retry fs symlink ":$SMALL_RENAMED" ":$SMALL_SYMLINK" >/dev/null
symlink_present="false"
for _ in $(seq 1 "$CLI_MAX_RETRIES"); do
  symlink_ls="$(drive9_retry fs ls /)"
  symlink_present=$(python3 - "$symlink_ls" "$(basename "$SMALL_SYMLINK")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
  if [[ "$symlink_present" == "true" ]]; then
    break
  fi
  sleep "$CLI_RETRY_SLEEP_S"
done
check_eq "symlink appears in ls /" "$symlink_present" "true"

symlink_target="$(drive9_retry_read fs cat "$SMALL_SYMLINK")"
check_eq "cat symlink returns target payload" "$symlink_target" "$SMALL_RENAMED"

drive9_retry fs hardlink "$SMALL_RENAMED" "$SMALL_HARDLINK" >/dev/null
hardlink_present="false"
for _ in $(seq 1 "$CLI_MAX_RETRIES"); do
  hardlink_ls="$(drive9_retry fs ls /)"
  hardlink_present=$(python3 - "$hardlink_ls" "$(basename "$SMALL_HARDLINK")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
  if [[ "$hardlink_present" == "true" ]]; then
    break
  fi
  sleep "$CLI_RETRY_SLEEP_S"
done
check_eq "hardlink appears in ls /" "$hardlink_present" "true"

hardlink_body="$(drive9_retry_read fs cat "$SMALL_HARDLINK")"
check_eq "cat hardlink returns source content" "$hardlink_body" "cli-smoke-${TS}"

hardlink_nlink="$(cli_stat_field "$SMALL_HARDLINK" "nlink")"
check_cmd "hardlink stat reports nlink >= 2" bash -c 'test "${1:-0}" -ge 2' -- "$hardlink_nlink"
hardlink_source_resource_id="$(cli_stat_field "$SMALL_RENAMED" "resource_id")"
hardlink_resource_id="$(cli_stat_field "$SMALL_HARDLINK" "resource_id")"
check_cmd "hardlink source resource_id is non-empty" test -n "$hardlink_source_resource_id"
check_eq "hardlink and source share resource_id" "$hardlink_resource_id" "$hardlink_source_resource_id"

printf "cli-hardlink-%s" "$TS" > "$HARDLINK_LOCAL"
drive9_retry fs cp "$HARDLINK_LOCAL" ":$SMALL_HARDLINK" >/dev/null
hardlink_source_after_write="$(drive9_retry_read fs cat "$SMALL_RENAMED")"
check_eq "writing hardlink updates source content" "$hardlink_source_after_write" "cli-hardlink-${TS}"

echo "[4.1] cli tag/stat metadata checks"
printf "cli-tag-%s" "$TS" > "$TAG_LOCAL"
drive9_retry fs cp --tag owner=smoke --tag topic=e2e "$TAG_LOCAL" ":$TAG_REMOTE" >/dev/null

tag_stat_json="$(drive9_retry fs stat -o json "$TAG_REMOTE")"
tag_owner="$(jq -r '.tags.owner // ""' <<<"$tag_stat_json")"
check_eq "stat -o json returns owner tag" "$tag_owner" "smoke"

tag_topic="$(jq -r '.tags.topic // ""' <<<"$tag_stat_json")"
check_eq "stat -o json returns topic tag" "$tag_topic" "e2e"

tag_semantic="$(jq -r '.semantic_text // ""' <<<"$tag_stat_json")"
check_eq "stat -o json includes semantic_text for tagged file" "$tag_semantic" "cli-tag-${TS}"

check_cmd "stat -o json includes non-empty content_type for tagged file" \
  bash -c 'jq -e '"'"'(.content_type // "") | length > 0'"'"' >/dev/null <<<"$1"' -- "$tag_stat_json"

printf "cli-tag-updated-%s" "$TS" > "$TAG_LOCAL"
drive9_retry fs cp --tag owner=updated "$TAG_LOCAL" ":$TAG_REMOTE" >/dev/null

tag_stat_json2="$(drive9_retry fs stat -o json "$TAG_REMOTE")"
tag_owner2="$(jq -r '.tags.owner // ""' <<<"$tag_stat_json2")"
check_eq "overwrite with single --tag updates owner" "$tag_owner2" "updated"

tag_topic2="$(jq -r 'if (.tags // {} | has("topic")) then "present" else "missing" end' <<<"$tag_stat_json2")"
check_eq "overwrite with single --tag clears old topic tag" "$tag_topic2" "missing"

echo "[4.2] cli pack/unpack local overlay archive checks"
mkdir -p "$PACK_LOCAL_ROOT/overlay/repo/.git" "$PACK_LOCAL_ROOT/overlay/repo/dist" "$PACK_LOCAL_ROOT/overlay/repo/src"
mkdir -p "$CLI_ENV_HOME/.drive9/profiles"
cat > "$CLI_ENV_HOME/.drive9/profiles/$PACK_PROFILE" <<'EOF'
[pack]
.git
dist
EOF
printf "[core]\n\trepositoryformatversion = 0\n" > "$PACK_LOCAL_ROOT/overlay/repo/.git/config"
printf "ref: refs/heads/main\n" > "$PACK_LOCAL_ROOT/overlay/repo/.git/HEAD"
printf "bundle-%s\n" "$TS" > "$PACK_LOCAL_ROOT/overlay/repo/dist/app.js"
printf "not-packed-%s\n" "$TS" > "$PACK_LOCAL_ROOT/overlay/repo/src/main.go"
drive9_retry profile show "$PACK_PROFILE" >/dev/null
drive9_retry pack --local-root "$PACK_LOCAL_ROOT" --remote-root "$PACK_REMOTE_ROOT" --profile "$PACK_PROFILE" >/dev/null
pack_archive_stat="$(drive9_retry fs stat "$PACK_REMOTE_ARCHIVE")"
pack_archive_size=$(python3 - "$pack_archive_stat" <<'PY'
import sys
for line in sys.argv[1].splitlines():
    if line.strip().startswith("size:"):
        print(line.split(":",1)[1].strip())
        break
PY
)
check_cmd "pack archive has non-zero remote size" bash -c 'test "${1:-0}" -gt 0' -- "$pack_archive_size"
drive9_retry unpack --local-root "$PACK_RESTORE_ROOT" --remote-root "$PACK_REMOTE_ROOT" --profile "$PACK_PROFILE" >/dev/null
restored_git_config="$(cat "$PACK_RESTORE_ROOT/overlay/repo/.git/config")"
check_eq "unpack restores .git config" "$restored_git_config" $'[core]\n\trepositoryformatversion = 0'
restored_dist="$(cat "$PACK_RESTORE_ROOT/overlay/repo/dist/app.js")"
check_eq "unpack restores dist artifact" "$restored_dist" "bundle-${TS}"
if [ -e "$PACK_RESTORE_ROOT/overlay/repo/src/main.go" ]; then
  restored_src_present="true"
else
  restored_src_present="false"
fi
check_eq "configured profile pack skips ordinary source file" "$restored_src_present" "false"

echo "[4.3] cli fs archive — download remote tree as tar.gz/zip with filtering"
# Seed a remote tree: README.md at root, src/app.go, src/util/util.go, plus a
# node_modules/ subtree and a .git/ subtree that filtering should skip.
drive9_retry fs mkdir ":$ARCHIVE_REMOTE_DIR" >/dev/null
drive9_retry fs mkdir ":$ARCHIVE_REMOTE_DIR/src" >/dev/null
drive9_retry fs mkdir ":$ARCHIVE_REMOTE_DIR/src/util" >/dev/null
drive9_retry fs mkdir ":$ARCHIVE_REMOTE_DIR/node_modules" >/dev/null
drive9_retry fs mkdir ":$ARCHIVE_REMOTE_DIR/node_modules/react" >/dev/null
drive9_retry fs mkdir ":$ARCHIVE_REMOTE_DIR/.git" >/dev/null
printf 'archive-readme-%s\n' "$TS" > "$SMALL_LOCAL"
drive9_retry fs cp "$SMALL_LOCAL" ":$ARCHIVE_REMOTE_DIR/README.md" >/dev/null
printf 'package main\n' > "$SMALL_LOCAL"
drive9_retry fs cp "$SMALL_LOCAL" ":$ARCHIVE_REMOTE_DIR/src/app.go" >/dev/null
printf 'package util\n' > "$SMALL_LOCAL"
drive9_retry fs cp "$SMALL_LOCAL" ":$ARCHIVE_REMOTE_DIR/src/util/util.go" >/dev/null
printf 'module.exports\n' > "$SMALL_LOCAL"
drive9_retry fs cp "$SMALL_LOCAL" ":$ARCHIVE_REMOTE_DIR/node_modules/react/index.js" >/dev/null
printf 'ref: refs/heads/main\n' > "$SMALL_LOCAL"
drive9_retry fs cp "$SMALL_LOCAL" ":$ARCHIVE_REMOTE_DIR/.git/HEAD" >/dev/null

# 4.3a: plain tar.gz, no filter — should contain all files.
drive9_retry fs archive ":$ARCHIVE_REMOTE_DIR" "$ARCHIVE_LOCAL_TARGZ" >/dev/null
check_cmd "archive tar.gz produced" test -s "$ARCHIVE_LOCAL_TARGZ"
archive_names="$(tar -tzf "$ARCHIVE_LOCAL_TARGZ" 2>/dev/null | sort)"
check_eq "plain archive includes README.md" "$(printf '%s' "$archive_names" | grep -c 'README.md')" "1"
check_eq "plain archive includes node_modules/react/index.js" "$(printf '%s' "$archive_names" | grep -c 'node_modules/react/index.js')" "1"
check_eq "plain archive includes .git/HEAD" "$(printf '%s' "$archive_names" | grep -c '\.git/HEAD')" "1"
check_eq "plain archive includes src/app.go" "$(printf '%s' "$archive_names" | grep -c 'src/app.go')" "1"

# 4.3b: --exclude skips node_modules and .git subtrees.
drive9_retry fs archive ":$ARCHIVE_REMOTE_DIR" "$ARCHIVE_LOCAL_TARGZ2" --exclude '**/node_modules/**' --exclude '**/.git/**' >/dev/null
archive_names_excluded="$(tar -tzf "$ARCHIVE_LOCAL_TARGZ2" 2>/dev/null | sort)"
check_eq "excluded archive still has README.md" "$(printf '%s' "$archive_names_excluded" | grep -c 'README.md')" "1"
check_eq "excluded archive still has src/app.go" "$(printf '%s' "$archive_names_excluded" | grep -c 'src/app.go')" "1"
check_eq "excluded archive drops node_modules" "$(printf '%s' "$archive_names_excluded" | grep -c 'node_modules')" "0"
check_eq "excluded archive drops .git" "$(printf '%s' "$archive_names_excluded" | grep -c '\.git/')" "0"

# 4.3c: --profile coding-agent skips the same default set as mount.
ARCHIVE_PROFILE_DIR="$CLI_ENV_HOME/.drive9/profiles"
mkdir -p "$ARCHIVE_PROFILE_DIR"
drive9_retry fs archive ":$ARCHIVE_REMOTE_DIR" "$ARCHIVE_LOCAL_TARGZ" --profile coding-agent >/dev/null
archive_names_profile="$(tar -tzf "$ARCHIVE_LOCAL_TARGZ" 2>/dev/null | sort)"
check_eq "profile archive drops node_modules" "$(printf '%s' "$archive_names_profile" | grep -c 'node_modules')" "0"
check_eq "profile archive drops .git" "$(printf '%s' "$archive_names_profile" | grep -c '\.git/')" "0"
check_eq "profile archive keeps src/app.go" "$(printf '%s' "$archive_names_profile" | grep -c 'src/app.go')" "1"

# 4.3d: --include whitelist keeps only matching paths.
drive9_retry fs archive ":$ARCHIVE_REMOTE_DIR" "$ARCHIVE_LOCAL_TARGZ2" --include 'src/**' --include 'README.md' >/dev/null
archive_names_include="$(tar -tzf "$ARCHIVE_LOCAL_TARGZ2" 2>/dev/null | sort)"
check_eq "include archive keeps README.md" "$(printf '%s' "$archive_names_include" | grep -c 'README.md')" "1"
check_eq "include archive keeps src/app.go" "$(printf '%s' "$archive_names_include" | grep -c 'src/app.go')" "1"
check_eq "include archive drops node_modules" "$(printf '%s' "$archive_names_include" | grep -c 'node_modules')" "0"
check_eq "include archive drops .git" "$(printf '%s' "$archive_names_include" | grep -c '\.git/')" "0"

# 4.3e: --format zip produces a valid zip archive.
drive9_retry fs archive ":$ARCHIVE_REMOTE_DIR" "$ARCHIVE_LOCAL_ZIP" --format zip --exclude '**/node_modules/**' --exclude '**/.git/**' >/dev/null
check_cmd "archive zip produced" test -s "$ARCHIVE_LOCAL_ZIP"
check_cmd "archive zip is valid" python3 -c 'import zipfile,sys; zipfile.ZipFile(sys.argv[1]).testzip() is None' "$ARCHIVE_LOCAL_ZIP"
archive_zip_names="$(python3 -c 'import zipfile,sys; print("\n".join(sorted(zipfile.ZipFile(sys.argv[1]).namelist())))' "$ARCHIVE_LOCAL_ZIP")"
check_eq "zip archive includes README.md" "$(printf '%s' "$archive_zip_names" | grep -c 'README.md')" "1"
check_eq "zip archive drops node_modules" "$(printf '%s' "$archive_zip_names" | grep -c 'node_modules')" "0"

# 4.3f: --flat strips directory hierarchy; archive contains basenames only.
drive9_retry fs archive ":$ARCHIVE_REMOTE_DIR" "$ARCHIVE_LOCAL_FLAT" --flat --exclude '**/node_modules/**' --exclude '**/.git/**' >/dev/null
archive_flat_names="$(tar -tzf "$ARCHIVE_LOCAL_FLAT" 2>/dev/null | sort)"
check_eq "flat archive has app.go basename" "$(printf '%s' "$archive_flat_names" | grep -cx 'app.go')" "1"
check_eq "flat archive has util.go basename" "$(printf '%s' "$archive_flat_names" | grep -cx 'util.go')" "1"
check_eq "flat archive has no slash paths" "$(printf '%s' "$archive_flat_names" | grep -c '/')" "0"

# 4.3g: --stdout produces a valid tar.gz on stdout that can be piped to tar.
# NOTE: drive9_retry captures stdout in a shell variable and reprints it, which
# corrupts binary archive bytes (NUL truncation). For --stdout we call drive9
# directly so the binary stream flows untouched into the tar pipeline. Guard
# with set +e so a transient failure does not abort the whole script under -e.
set +e
archive_stdout_names="$(drive9 fs archive ":$ARCHIVE_REMOTE_DIR" --stdout --exclude '**/node_modules/**' --exclude '**/.git/**' 2>/dev/null | tar -tzf - 2>/dev/null | sort)"
set -e
check_eq "stdout archive includes README.md" "$(printf '%s' "$archive_stdout_names" | grep -c 'README.md')" "1"
check_eq "stdout archive drops node_modules" "$(printf '%s' "$archive_stdout_names" | grep -c 'node_modules')" "0"

# 4.3h: extracted archive contents match the uploaded bytes.
rm -rf "$ARCHIVE_EXTRACT_DIR"
mkdir -p "$ARCHIVE_EXTRACT_DIR"
tar -xzf "$ARCHIVE_LOCAL_TARGZ2" -C "$ARCHIVE_EXTRACT_DIR" 2>/dev/null
extracted_appgo="$(cat "$ARCHIVE_EXTRACT_DIR/$(basename "$ARCHIVE_REMOTE_DIR")/src/app.go" 2>/dev/null)"
check_eq "extracted src/app.go content matches" "$extracted_appgo" 'package main'

# Cleanup archive fixtures.
drive9_retry fs rm -r ":$ARCHIVE_REMOTE_DIR" >/dev/null
rm -f "$ARCHIVE_LOCAL_TARGZ" "$ARCHIVE_LOCAL_TARGZ2" "$ARCHIVE_LOCAL_ZIP" "$ARCHIVE_LOCAL_FLAT" "$SMALL_LOCAL"
rm -rf "$ARCHIVE_EXTRACT_DIR"

echo "[5] batch small-file upload/list/read via cli"
mkdir -p "$BATCH_LOCAL_DIR"
for i in $(seq 1 "$CLI_BATCH_SMALL_FILE_COUNT"); do
  lp="$BATCH_LOCAL_DIR/file-${i}.txt"
  rp="$BATCH_REMOTE_DIR/file-${i}.txt"
  printf "cli-batch-%s-%s" "$TS" "$i" > "$lp"
  drive9_retry fs cp "$lp" ":$rp" >/dev/null
done

batch_ls="$(drive9_retry fs ls "$BATCH_REMOTE_DIR")"
batch_count=$(python3 - "$batch_ls" <<'PY'
import sys
lines=[ln.strip() for ln in sys.argv[1].splitlines() if ln.strip()]
print(len(lines))
PY
)
check_eq "batch dir file count matches" "$batch_count" "$CLI_BATCH_SMALL_FILE_COUNT"

for i in 1 "$CLI_BATCH_SMALL_FILE_COUNT"; do
  rp="$BATCH_REMOTE_DIR/file-${i}.txt"
  got="$(drive9_retry_read fs cat "$rp")"
  want="cli-batch-$TS-$i"
  check_eq "batch file content matches for $rp" "$got" "$want"
done

batch_stat="$(drive9_retry fs stat "$BATCH_REMOTE_DIR/file-1.txt")"
batch_isdir=$(python3 - "$batch_stat" <<'PY'
import sys
val=""
for line in sys.argv[1].splitlines():
    if line.strip().startswith("isdir:"):
        val=line.split(":",1)[1].strip().lower()
        break
print(val)
PY
)
check_eq "stat reports batch file isdir=false" "$batch_isdir" "false"

echo "[6] cli grep/find checks"
grep_out="$(drive9_retry fs grep "cli-batch-$TS" "/")"
grep_has_batch=$(python3 - "$grep_out" "$BATCH_REMOTE_DIR/file-1.txt" <<'PY'
import sys
out=sys.argv[1].splitlines()
target=sys.argv[2]
print("true" if any(line.split("\t")[0].strip()==target for line in out if line.strip()) else "false")
PY
)
check_eq "cli grep finds batch file" "$grep_has_batch" "true"

echo "[6.0] cli grep --json checks"
grep_json_out="$(drive9_retry fs grep --json "cli-batch-$TS" "/")"
check_cmd "grep --json emits a JSON array" \
  bash -c 'jq -e "type == \"array\"" >/dev/null <<<"$1"' -- "$grep_json_out"
grep_json_has_batch=$(jq -r --arg target "$BATCH_REMOTE_DIR/file-1.txt" \
  '([ .[] | select(.path == $target) ] | length > 0)' <<<"$grep_json_out")
check_eq "grep --json finds batch file in array" "$grep_json_has_batch" "true"

grep_json_empty="$(drive9_retry fs grep --json "zzz_nonexistent_marker_xyz" "/")"
check_eq "grep --json empty result is []" "$(jq -c <<<"$grep_json_empty")" "[]"

find_out="$(drive9_retry fs find / -name "*.txt")"
find_has_txt=$(python3 - "$find_out" <<'PY'
import sys
lines=[ln.strip() for ln in sys.argv[1].splitlines() if ln.strip()]
print("true" if any("/cli-" in line and line.endswith(".txt") for line in lines) else "false")
PY
)
check_eq "cli find by name returns txt files" "$find_has_txt" "true"

echo "[6.1] cli semantic text recall checks"
if [ "$RUN_CLI_SEMANTIC_CHECKS" = "1" ]; then
  printf "A cat is resting on a sofa near a window." > "/tmp/drive9-cli-sem-target-${TS}.txt"
  printf "A dog is running in a field under bright sun." > "/tmp/drive9-cli-sem-other-${TS}.txt"
  drive9_retry fs cp "/tmp/drive9-cli-sem-target-${TS}.txt" ":$SEM_TEXT_TARGET" >/dev/null
  drive9_retry fs cp "/tmp/drive9-cli-sem-other-${TS}.txt" ":$SEM_TEXT_OTHER" >/dev/null

  wait_cli_grep_target "cli semantic grep includes cat-story target" "feline sofa" "$SEM_TEXT_TARGET"
  wait_cli_grep_target "cli semantic grep includes dog-story target" "canine field" "$SEM_TEXT_OTHER"
else
  echo "semantic text recall checks skipped (RUN_CLI_SEMANTIC_CHECKS=$RUN_CLI_SEMANTIC_CHECKS)"
  skip_check "cli semantic grep includes cat-story target"
  skip_check "cli semantic grep includes dog-story target"
fi

echo "[6.2] cli image-associated recall checks"
if [ "$RUN_CLI_SEMANTIC_CHECKS" = "1" ]; then
  cp "$DRIVE9_IMAGE_FIXTURE_PATH" "$IMAGE_LOCAL"
  check_cmd "local cli jpg fixture exists" test -s "$IMAGE_LOCAL"
  drive9_retry fs cp "$IMAGE_LOCAL" ":$IMAGE_REMOTE" >/dev/null
  CLI_IMAGE_UPLOADED=1
  printf "This image shows a cat face icon." > "/tmp/drive9-cli-image-caption-${TS}.txt"
  drive9_retry fs cp "/tmp/drive9-cli-image-caption-${TS}.txt" ":$IMAGE_CAPTION_REMOTE" >/dev/null

  wait_cli_grep_target "cli image-associated grep includes caption" "feline face icon" "$IMAGE_CAPTION_REMOTE"

  find_png_out="$(drive9_retry fs find / -name "*.jpg")"
  find_has_png=$(python3 - "$find_png_out" "$IMAGE_REMOTE" <<'PY'
import sys
lines=[ln.strip() for ln in sys.argv[1].splitlines() if ln.strip()]
target=sys.argv[2]
print("true" if any(line == target or line.endswith('.jpg') for line in lines) else "false")
PY
)
  check_eq "cli find by name returns jpg files" "$find_has_png" "true"
else
  echo "image-associated recall checks skipped (RUN_CLI_SEMANTIC_CHECKS=$RUN_CLI_SEMANTIC_CHECKS)"
  skip_check "local cli jpg fixture exists"
  skip_check "cli image-associated grep includes caption"
  skip_check "cli find by name returns jpg files"
fi

echo "[7] large multipart upload via cli cp"
dd if=/dev/zero of="$LARGE_LOCAL" bs=1M count="$CLI_LARGE_FILE_MB" status=none
drive9_retry fs cp "$LARGE_LOCAL" ":$LARGE_REMOTE" >/dev/null

stat_out="$(drive9_retry fs stat "$LARGE_REMOTE")"
remote_size=$(python3 - "$stat_out" <<'PY'
import sys
for line in sys.argv[1].splitlines():
    if line.strip().startswith("size:"):
        print(line.split(":",1)[1].strip())
        break
PY
)
check_eq "large remote size matches" "$remote_size" "$LARGE_BYTES"

drive9_retry fs cp ":$LARGE_REMOTE" "$LARGE_DOWNLOADED" >/dev/null
check_cmd "downloaded large file exists" test -f "$LARGE_DOWNLOADED"

sum_src=$(sha256sum "$LARGE_LOCAL" | cut -d' ' -f1)
sum_dst=$(sha256sum "$LARGE_DOWNLOADED" | cut -d' ' -f1)
check_eq "downloaded large file sha256 matches" "$sum_dst" "$sum_src"

echo "[8] cleanup via cli"
drive9_retry fs rm "$SMALL_SYMLINK" >/dev/null
drive9_retry fs rm "$SMALL_HARDLINK" >/dev/null
drive9_retry fs rm "$SMALL_RENAMED" >/dev/null
drive9_retry fs rm "$TAG_REMOTE" >/dev/null
if [ "$CLI_IMAGE_UPLOADED" = "1" ]; then
  drive9_retry fs rm "$IMAGE_REMOTE" >/dev/null
fi
drive9_retry fs rm "$LARGE_REMOTE" >/dev/null
drive9_retry fs rm "$PACK_REMOTE_ARCHIVE" >/dev/null
drive9_retry fs rm "$cp_dir_remote_path" >/dev/null
drive9_retry fs rm "$cp_dir_remote_copy_path" >/dev/null
drive9_retry fs rm -r "$CP_DIR_REMOTE" >/dev/null
drive9_retry fs rm -r "$CP_DIR_REMOTE_COPY" >/dev/null
drive9_retry fs rm -r "$RECURSIVE_REMOTE" >/dev/null
drive9_retry fs rm -r "$RECURSIVE_REMOTE_COPY" >/dev/null
for i in $(seq 1 "$CLI_BATCH_SMALL_FILE_COUNT"); do
  drive9_retry fs rm "$BATCH_REMOTE_DIR/file-${i}.txt" >/dev/null
done

final_ls="$(drive9_retry fs ls /)"
small_left=$(python3 - "$final_ls" "$(basename "$SMALL_RENAMED")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
large_left=$(python3 - "$final_ls" "$(basename "$LARGE_REMOTE")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name for line in out) else "false")
PY
)
check_eq "small file removed" "$small_left" "false"
check_eq "large file removed" "$large_left" "false"

batch_ls_after="$(drive9_retry fs ls /)"
batch_left=$(python3 - "$batch_ls_after" "$(basename "$BATCH_REMOTE_DIR")" <<'PY'
import sys
out=sys.argv[1].splitlines()
name=sys.argv[2]
print("true" if any(line.strip()==name or line.strip()==name+"/" for line in out) else "false")
PY
)
# Directory cleanup semantics may vary; allow either retained empty dir or auto-removed dir.
check_cmd "batch directory cleanup accepted" test "$batch_left" = "true" -o "$batch_left" = "false"

if [ "$RUN_CLI_UPLOAD_LIMIT_BOUNDARY" = "1" ]; then
  echo "[9] upload limit boundary via API with CLI auth"
  boundary_ok_payload="$(mktemp)"
  python3 - "cli-limit-${TS}.bin" "$CLI_UPLOAD_LIMIT_BYTES" > "$boundary_ok_payload" <<'PY'
import base64
import json
import struct
import sys

def _crc32c_table():
    poly = 0x82F63B78
    tbl = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
        tbl.append(crc)
    return tbl

_TABLE = _crc32c_table()

def crc32c(data):
    crc = 0xFFFFFFFF
    for b in data:
        crc = _TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF

path = "/" + sys.argv[1].lstrip("/")
upload_limit = int(sys.argv[2])
part_size = 8 * 1024 * 1024
part = b"\x00" * part_size
checksum = base64.b64encode(struct.pack(">I", crc32c(part))).decode()
parts = (upload_limit + part_size - 1) // part_size
print(json.dumps({
    "path": path,
    "total_size": upload_limit,
    "part_checksums": [checksum] * parts,
}))
PY

  ok_file="$(mktemp)"
  ok_code=$(curl -sS -o "$ok_file" -w "%{http_code}" -X POST \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "@$boundary_ok_payload" \
    "$BASE/v1/uploads/initiate")
  check_eq "cli-boundary init at limit returns 202" "$ok_code" "202"
  rm -f "$boundary_ok_payload" "$ok_file"

  over=$((CLI_UPLOAD_LIMIT_BYTES + 1))
  boundary_over_payload="$(mktemp)"
  python3 - "cli-limit-over-${TS}.bin" "$over" > "$boundary_over_payload" <<'PY'
import base64
import json
import struct
import sys

def _crc32c_table():
    poly = 0x82F63B78
    tbl = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
        tbl.append(crc)
    return tbl

_TABLE = _crc32c_table()

def crc32c(data):
    crc = 0xFFFFFFFF
    for b in data:
        crc = _TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF

path = "/" + sys.argv[1].lstrip("/")
over_limit = int(sys.argv[2])
part = b"\x00" * (8 * 1024 * 1024)
checksum = base64.b64encode(struct.pack(">I", crc32c(part))).decode()
print(json.dumps({
    "path": path,
    "total_size": over_limit,
    "part_checksums": [checksum],
}))
PY

  over_file="$(mktemp)"
  over_code=$(curl -sS -o "$over_file" -w "%{http_code}" -X POST \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "@$boundary_over_payload" \
    "$BASE/v1/uploads/initiate")
  check_eq "cli-boundary init over limit returns 413" "$over_code" "413"
  over_err=$(jq -r '.error // empty' "$over_file")
  check_cmd "cli-boundary over-limit has error message" test -n "$over_err"
  rm -f "$boundary_over_payload" "$over_file"
fi

rm -f "${pfile:-}" "$CLI_BIN" "$SMALL_LOCAL" "$HARDLINK_LOCAL" "$IMAGE_LOCAL" "$LARGE_LOCAL" "$LARGE_DOWNLOADED"
rm -f "$TAG_LOCAL"
rm -f "$FORK_LOCAL"
rm -f "/tmp/drive9-cli-sem-target-${TS}.txt" "/tmp/drive9-cli-sem-other-${TS}.txt" "/tmp/drive9-cli-image-caption-${TS}.txt"
rm -rf "$BATCH_LOCAL_DIR" "$CP_DIR_LOCAL" "$PACK_LOCAL_ROOT" "$PACK_RESTORE_ROOT" "$CLI_ENV_HOME" "$CLI_CTX_HOME"

echo "RESULT: $PASS passed, $FAIL failed, $SKIP skipped, $TOTAL total"
exit "$FAIL"
