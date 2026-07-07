#!/usr/bin/env bash
# Deterministic Drive9 FUSE correctness workload.
#
# This script builds a remote fixture tree through the CLI, mounts that tree
# read-only through real FUSE, then verifies ordinary Unix read workloads
# against a manifest. It intentionally avoids concurrent writes, Git, and
# cross-mount checks; those are separate workload classes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/provision-helper.sh"
BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-20}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
FUSE_MOUNT_ROOT="${FUSE_MOUNT_ROOT:-$DRIVE9_E2E_TMPDIR}"
FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-0}"
FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-60s}"
FUSE_CORRECTNESS_KEEP_ARTIFACTS="${FUSE_CORRECTNESS_KEEP_ARTIFACTS:-0}"
FUSE_CORRECTNESS_LARGE_MB="${FUSE_CORRECTNESS_LARGE_MB:-9}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"

PASS=0
FAIL=0
TOTAL=0

check_eq() {
  local desc="$1" got="$2" want="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$got" = "$want" ]; then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc"
    echo "  want: $want"
    echo "  got:  $got"
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

require_cmd() {
  local name="$1"
  TOTAL=$((TOTAL + 1))
  if command -v "$name" >/dev/null 2>&1; then
    echo "PASS $name is available"
    PASS=$((PASS + 1))
    return 0
  fi
  echo "FAIL $name is available" >&2
  FAIL=$((FAIL + 1))
  exit 1
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
  local unversioned_url versioned_url download_url
  detect_release_target || return 1
  unversioned_url="$CLI_RELEASE_BASE_URL/drive9-$CLI_RELEASE_OS-$CLI_RELEASE_ARCH"
  if [ -z "$target_version" ]; then
    target_version=$(curl -fsSL "$CLI_RELEASE_BASE_URL/version" 2>/dev/null | tr -d '[:space:]' || true)
  fi
  download_url="$unversioned_url"
  if [ -n "$target_version" ]; then
    versioned_url="$CLI_RELEASE_BASE_URL/drive9-$target_version-$CLI_RELEASE_OS-$CLI_RELEASE_ARCH"
    if curl -fsI "$versioned_url" >/dev/null 2>&1; then
      download_url="$versioned_url"
    fi
  fi
  curl -fsSL "$download_url" -o "$CLI_BIN"
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

start_mount() {
  {
    echo "=== drive9 correctness mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "remote_root=$ROOT_REMOTE"
  } >>"$MOUNT_LOG"
  drive9 mount --read-only ":$ROOT_REMOTE" "$MOUNT_POINT" >>"$MOUNT_LOG" 2>&1 &
  MOUNT_PID="$!"

  if wait_mount_state mounted; then
    return 0
  fi
  cat "$MOUNT_LOG" >&2 || true
  return 1
}

stop_mount() {
  set +e
  if [ -n "${MOUNT_POINT:-}" ] && is_mounted "$MOUNT_POINT"; then
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

curl_body_code() {
  local method="$1"
  local url="$2"
  local auth="${3:-}"

  local attempt=1
  while :; do
    local body_file code rc
    body_file="$(mktemp)"
    set +e
    if [ -n "$auth" ]; then
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
      rc=$?
    else
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
      rc=$?
    fi
    set -e
    if [ "$rc" -ne 0 ]; then
      code="000"
    fi

    if { [ "$rc" -eq 0 ] && [ "$code" != "429" ] && [ "$code" != "403" ]; } || [ "$attempt" -ge "$REQUEST_MAX_RETRIES" ]; then
      cat "$body_file"
      echo
      echo "__HTTP__${code}"
      rm -f "$body_file"
      return "$rc"
    fi

    rm -f "$body_file"
    attempt=$((attempt + 1))
    sleep "$REQUEST_RETRY_SLEEP_S"
  done
}

http_code() { printf '%s' "$1" | awk -F'__HTTP__' 'NF>1{print $2}' | tr -d '\n'; }
json_body() { printf '%s' "$1" | sed '/__HTTP__/d'; }

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
with open(sys.argv[1], "rb") as f:
    for chunk in iter(lambda: f.read(1024 * 1024), b""):
        h.update(chunk)
print(h.hexdigest())
PY
}

stat_size() {
  local file_path="$1"
  case "$(uname -s)" in
    Darwin) stat -f %z "$file_path" ;;
    *) stat -c %s "$file_path" ;;
  esac
}

stat_nlink() {
  local file_path="$1"
  case "$(uname -s)" in
    Darwin) stat -f %l "$file_path" ;;
    *) stat -c %h "$file_path" ;;
  esac
}

relative_sorted_find() {
  local root="$1"
  local type_flag="$2"
  find "$root" -type "$type_flag" -print | python3 -c '
import os
import sys
root = os.path.abspath(sys.argv[1])
items = []
for line in sys.stdin:
    path = os.path.abspath(line.rstrip("\n"))
    rel = os.path.relpath(path, root)
    if rel == ".":
        rel = "."
    if rel == ".go-fuse-epoll-hack" or rel.endswith("/.go-fuse-epoll-hack"):
        continue
    items.append(rel)
for item in sorted(items):
    print(item)
' "$root"
}

grep_relative_matches() {
  local pattern="$1"
  shift
  { grep -R -l -- "$pattern" "$@" || true; } | python3 -c '
import os
import sys
root = os.path.abspath(sys.argv[1])
items = []
for line in sys.stdin:
    path = os.path.abspath(line.rstrip("\n"))
    items.append(os.path.relpath(path, root))
for item in sorted(items):
    print(item)
' "$MOUNT_POINT"
}

create_fixture_tree() {
  mkdir -p "$FIXTURE_DIR/text" "$FIXTURE_DIR/nested/deep" "$FIXTURE_DIR/binary" "$FIXTURE_DIR/large" "$FIXTURE_DIR/links"

  : > "$FIXTURE_DIR/empty.txt"
  cat > "$FIXTURE_DIR/text/alpha.txt" <<EOF
alpha line
DRIVE9_NEEDLE_ALPHA common-read-correctness
unicode 你好 café
EOF
  cat > "$FIXTURE_DIR/text/space name.txt" <<EOF
space path line
DRIVE9_NEEDLE_SPACE common-read-correctness
EOF
  cat > "$FIXTURE_DIR/text/中文-文件.txt" <<EOF
中文路径
DRIVE9_NEEDLE_UNICODE
EOF
  cat > "$FIXTURE_DIR/nested/deep/story.md" <<EOF
# Story

This nested file carries DRIVE9_NEEDLE_NESTED and common-read-correctness.
EOF
  python3 - "$FIXTURE_DIR/binary/small.bin" "$FIXTURE_DIR/large/large-${FUSE_CORRECTNESS_LARGE_MB}mb.bin" "$FUSE_CORRECTNESS_LARGE_MB" <<'PY'
import pathlib
import sys
small = pathlib.Path(sys.argv[1])
large = pathlib.Path(sys.argv[2])
large_mb = int(sys.argv[3])
small.write_bytes(bytes(range(256)) * 17 + b"\x00drive9-binary-tail\xff")
chunk = bytearray((i * 31 + 7) % 256 for i in range(1024 * 1024))
with large.open("wb") as f:
    for i in range(large_mb):
        chunk[0] = i % 256
        f.write(chunk)
    f.write(b"drive9-large-tail")
PY
}

write_expected_manifests() {
  cat > "$EXPECTED_FILES" <<EOF
binary/small.bin
empty.txt
large/large-${FUSE_CORRECTNESS_LARGE_MB}mb.bin
links/alpha-hardlink.txt
nested/deep/story.md
text/alpha.txt
text/space name.txt
text/中文-文件.txt
EOF
  cat > "$EXPECTED_DIRS" <<'EOF'
.
binary
empty-dir
large
links
nested
nested/deep
text
EOF
  cat > "$EXPECTED_LINKS" <<'EOF'
links/alpha-link
EOF
}

upload_fixture_tree() {
  for dir in "" "text" "nested" "nested/deep" "binary" "large" "links" "empty-dir"; do
    drive9_retry fs mkdir "$ROOT_REMOTE${dir:+/$dir}" >/dev/null
  done

  while IFS= read -r rel; do
    case "$rel" in
      links/alpha-hardlink.txt)
        continue
        ;;
    esac
    drive9_retry fs cp "$FIXTURE_DIR/$rel" ":$ROOT_REMOTE/$rel" >/dev/null
  done < "$EXPECTED_FILES"

  drive9_retry fs symlink "../text/alpha.txt" ":$ROOT_REMOTE/links/alpha-link" >/dev/null
  drive9_retry fs hardlink "$ROOT_REMOTE/text/alpha.txt" "$ROOT_REMOTE/links/alpha-hardlink.txt" >/dev/null
}

assert_manifest_files() {
  local actual_files actual_dirs actual_links
  actual_files="$RUN_ROOT/actual-files.txt"
  actual_dirs="$RUN_ROOT/actual-dirs.txt"
  actual_links="$RUN_ROOT/actual-links.txt"

  relative_sorted_find "$MOUNT_POINT" f > "$actual_files"
  relative_sorted_find "$MOUNT_POINT" d > "$actual_dirs"
  relative_sorted_find "$MOUNT_POINT" l > "$actual_links"

  check_cmd "find -type f matches manifest" diff -u "$EXPECTED_FILES" "$actual_files"
  check_cmd "find -type d matches manifest" diff -u "$EXPECTED_DIRS" "$actual_dirs"
  check_cmd "find -type l matches manifest" diff -u "$EXPECTED_LINKS" "$actual_links"
}

assert_cat_stat_checksum() {
  local rel expected_path mounted_path expected_hash mounted_hash expected_size mounted_size
  while IFS= read -r rel; do
    mounted_path="$MOUNT_POINT/$rel"
    if [ "$rel" = "links/alpha-hardlink.txt" ]; then
      expected_path="$FIXTURE_DIR/text/alpha.txt"
    else
      expected_path="$FIXTURE_DIR/$rel"
    fi
    expected_hash="$(sha256_file "$expected_path")"
    mounted_hash="$(cat "$mounted_path" | sha256_file /dev/stdin)"
    check_eq "cat+sha256 $rel" "$mounted_hash" "$expected_hash"

    expected_size="$(stat_size "$expected_path")"
    mounted_size="$(stat_size "$mounted_path")"
    check_eq "stat size $rel" "$mounted_size" "$expected_size"
  done < "$EXPECTED_FILES"

  local source_nlink hardlink_nlink source_hash hardlink_hash
  source_nlink="$(stat_nlink "$MOUNT_POINT/text/alpha.txt")"
  hardlink_nlink="$(stat_nlink "$MOUNT_POINT/links/alpha-hardlink.txt")"
  check_eq "stat nlink source hardlink" "$source_nlink" "2"
  check_eq "stat nlink hardlink" "$hardlink_nlink" "2"
  source_hash="$(sha256_file "$MOUNT_POINT/text/alpha.txt")"
  hardlink_hash="$(sha256_file "$MOUNT_POINT/links/alpha-hardlink.txt")"
  check_eq "hardlink checksum matches source" "$hardlink_hash" "$source_hash"

  local link_target link_hash
  link_target="$(readlink "$MOUNT_POINT/links/alpha-link")"
  check_eq "readlink symlink target" "$link_target" "../text/alpha.txt"
  link_hash="$(sha256_file "$MOUNT_POINT/links/alpha-link")"
  check_eq "symlink target checksum matches source" "$link_hash" "$source_hash"
}

assert_grep_workload() {
  local actual expected
  actual="$RUN_ROOT/grep-common.txt"
  expected="$RUN_ROOT/grep-common-expected.txt"
  grep_relative_matches "common-read-correctness" \
    "$MOUNT_POINT/text" "$MOUNT_POINT/nested" "$MOUNT_POINT/links/alpha-hardlink.txt" > "$actual"
  cat > "$expected" <<'EOF'
links/alpha-hardlink.txt
nested/deep/story.md
text/alpha.txt
text/space name.txt
EOF
  check_cmd "grep common marker matches expected files" diff -u "$expected" "$actual"

  actual="$RUN_ROOT/grep-space.txt"
  expected="$RUN_ROOT/grep-space-expected.txt"
  grep_relative_matches "DRIVE9_NEEDLE_SPACE" "$MOUNT_POINT/text" > "$actual"
  cat > "$expected" <<'EOF'
text/space name.txt
EOF
  check_cmd "grep handles filename with spaces" diff -u "$expected" "$actual"

  actual="$RUN_ROOT/grep-unicode.txt"
  expected="$RUN_ROOT/grep-unicode-expected.txt"
  grep_relative_matches "DRIVE9_NEEDLE_UNICODE" "$MOUNT_POINT/text" > "$actual"
  cat > "$expected" <<'EOF'
text/中文-文件.txt
EOF
  check_cmd "grep handles unicode filename" diff -u "$expected" "$actual"

  check_cmd "grep through symlink follows target content" grep -q "DRIVE9_NEEDLE_ALPHA" "$MOUNT_POINT/links/alpha-link"
  check_cmd "grep no-match exits non-zero" bash -c 'if grep -R -q -- "DRIVE9_NEEDLE_ABSENT" "$1"; then exit 1; fi' _ "$MOUNT_POINT"
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
    if [ "$attempt" -lt "$REQUEST_MAX_RETRIES" ] && [[ "$out" == *"Too Many Requests"* || "$out" == *"HTTP 429"* || "$out" == *"HTTP 403"* || "$out" == *"403 Forbidden"* ]]; then
      attempt=$((attempt + 1))
      sleep "$REQUEST_RETRY_SLEEP_S"
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

echo "=== drive9 FUSE correctness workload ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "FUSE_STRICT_PREREQS=$FUSE_STRICT_PREREQS"
echo "FUSE_CORRECTNESS_LARGE_MB=$FUSE_CORRECTNESS_LARGE_MB"

require_cmd curl
require_cmd jq
require_cmd python3
require_cmd grep
require_cmd find
require_cmd stat
require_cmd readlink
if [ "$CLI_SOURCE" = "build" ]; then
  require_cmd go
fi

if [ "$(uname -s)" != "Linux" ] && [ "$(uname -s)" != "Darwin" ]; then
  skip_or_fail "unsupported OS for this workload"
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
  resp=$(drive9_provision_curl_body_code "$BASE")
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

TS="$(date +%s)"
RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-fuse-correctness-${TS}.XXXXXX")"
FIXTURE_DIR="$RUN_ROOT/fixture"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
EXPECTED_FILES="$RUN_ROOT/expected-files.txt"
EXPECTED_DIRS="$RUN_ROOT/expected-dirs.txt"
EXPECTED_LINKS="$RUN_ROOT/expected-links.txt"
ROOT_REMOTE="/fuse-correctness-${TS}"
MOUNT_PID=""

mkdir -p "$FIXTURE_DIR" "$MOUNT_POINT"
: > "$MOUNT_LOG"

cleanup() {
  local rc=$?
  stop_mount
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$FUSE_CORRECTNESS_KEEP_ARTIFACTS" != "1" ]; then
    rm -rf "$RUN_ROOT"
  else
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount log: $MOUNT_LOG"
    echo "Fixture root: $FIXTURE_DIR"
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "[4] create fixture manifest"
create_fixture_tree
write_expected_manifests
check_cmd "fixture file manifest generated" test -s "$EXPECTED_FILES"
check_cmd "fixture dir manifest generated" test -s "$EXPECTED_DIRS"
check_cmd "fixture symlink manifest generated" test -s "$EXPECTED_LINKS"

echo "[5] upload fixture through CLI"
upload_fixture_tree
check_eq "remote fixture root" "$ROOT_REMOTE" "$ROOT_REMOTE"

echo "[6] mount fixture read-only"
if start_mount; then
  check_eq "fixture mount is mounted" "true" "true"
else
  check_eq "fixture mount is mounted" "false" "true"
fi

if is_mounted "$MOUNT_POINT"; then
  echo "[7] find manifest parity"
  assert_manifest_files

  echo "[8] cat/stat/checksum parity"
  assert_cat_stat_checksum

  echo "[9] grep workload"
  assert_grep_workload

  echo "[10] read-only guardrail"
  check_cmd "read-only mount rejects writes" bash -c 'if printf x > "$1/should-not-write.txt" 2>/dev/null; then exit 1; fi' _ "$MOUNT_POINT"
fi

echo "[11] cleanup remote fixture"
if is_mounted "$MOUNT_POINT"; then
  check_cmd "unmount correctness mount" drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT"
  wait_mount_state unmounted || true
  MOUNT_PID=""
fi
drive9_retry fs rm -r "$ROOT_REMOTE" >/dev/null || true

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
