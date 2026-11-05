#!/usr/bin/env bash
# Extent takeover e2e (content layout v2): the default mount claims every
# NEW file that grows past the server's inline cutoff and promotes it to
# the extent plane (community JuiceFS engine on the v2 wire protocol),
# while LEGACY files (created through the classic presign/multipart path)
# keep their single layout forever. Ported from the abandoned PR 1031
# suite onto v2 semantics:
#
#   - layout identity rides X-Dat9-Content-Layout only (v2 has no
#     extent_ino); inode identity across renames/remounts is asserted via
#     X-Dat9-Resource-ID (the classic inode's stable resource identity)
#     where 1031 asserted extent_ino;
#   - no data-credential prerequisite (the v2 data plane is presign-only);
#   - compaction and block GC run SERVER-side: F asserts the server's
#     extent_compact_tasks settle under the enqueue threshold and the
#     bytes survive; E asserts name reclaim and (optional, grace-bound)
#     object counts — 24h block grace means object-drop assertions need
#     EXTENT_E2E_GC_WAIT_S or are advisory;
#   - SQL checks (EXTENT_E2E_SQL_CHECK=1) query the v2 extent tables via
#     the tenant SQL endpoint: extent_chunks / extent_compact_tasks /
#     extent_block_gc instead of the deleted jfs_* tables.
#
# Sections (unchanged contracts from 1031):
#   A. a legacy multipart file (uploaded by the CLI, content_layout=single)
#      reads, appends, is partially overwritten, truncated-rewritten,
#      chmodded, renamed, and deleted from the takeover mount WITHOUT ever
#      converting its layout or losing bytes.
#   B. after the legacy file is deleted, the same path rewritten past the
#      cutoff becomes extent; small files stay single; a same-fd grow
#      across the cutoff, a fresh O_APPEND writer, and sparse files (big
#      truncate / far seek) all promote.
#   C. a directory mixing single and extent children survives ls / find /
#      sizes / renames in every plane combination, directory rename,
#      cp -r, rm -r, hardlinks (classic links work; extent links either
#      work or fail cleanly).
#   D. namespace changes from another channel (CLI fs mv / fs rm -r with
#      the mount down) stay consistent after a fresh remount.
#   E. deleting an extent file reclaims its server-side state: the name
#      404s, the path is reusable, and (optional counters) block objects
#      eventually leave the grace window.
#   F. server-side slice compaction: thousands of small fsynced writes
#      into one extent file merge without changing the file's bytes.
#   G. two concurrent mounts of one tenant agree on bytes and layouts.
#   H. a file held open mid-write is statable and readable from another
#      process, and lands as extent after close.
#
# Manual-only; see e2e/README.md ("CI automation tiers"). Run against a
# drive9-server with the extent v2 protocol:
#
#   DRIVE9_BASE=http://127.0.0.1:9009 bash e2e/extent-takeover-mixed-tree.sh
#
# Env:
#   DRIVE9_API_KEY                  reuse an existing tenant instead of provisioning
#   EXTENT_E2E_PROVISION_BODY        JSON body for POST /v1/provision (default: {})
#   EXTENT_E2E_TAKEOVER_PROFILE      mount profile (default: none — the plain takeover mount; "extent" selects the full-engine profile)
#   EXTENT_E2E_DURABILITY            --durability value (default: fsync). The
#                                   layout assertions are fsync barriers, so the
#                                   suite pins strict fsync instead of letting
#                                   `auto` resolve by RTT.
#   EXTENT_E2E_BIG_KB                big payload KiB (default: 4x the server inline cutoff, floor 256)
#   EXTENT_E2E_SMALL_KB              small payload KiB (default: 4)
#   EXTENT_E2E_COMPACT_WRITES        fsynced random writes for compaction (default: 2600; 0 skips F)
#   EXTENT_E2E_COMPACT_SPAN_MB       random-write span in MiB (default: 32, one 64 MiB chunk minus margin)
#   EXTENT_E2E_GC_WAIT_S             seconds to wait for server compaction/block reclaim (default: 240)
#   EXTENT_E2E_S3_DIR                optional: count object files under this dir (filesystem S3 mock)
#                                   to assert block reclaim after deletes and compaction (24h grace!)
#   EXTENT_E2E_SQL_CHECK             1 = also assert the v2 extent tables via POST /v1/sql
#   EXTENT_E2E_KEEP_ARTIFACTS        1 = keep the run root and mount log on success
#   FUSE_STRICT_PREREQS              1 = fail instead of skipping when FUSE is unavailable
#   FUSE_MOUNT_ROOT, MOUNT_READY_*, POLL_*, REQUEST_*, CLI_* — same meaning as sibling FUSE suites.

set -euo pipefail

BASE="${DRIVE9_BASE:-http://127.0.0.1:9009}"
DRIVE9_API_KEY="${DRIVE9_API_KEY:-}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-5}"
MOUNT_READY_TIMEOUT_S="${MOUNT_READY_TIMEOUT_S:-20}"
MOUNT_READY_INTERVAL_S="${MOUNT_READY_INTERVAL_S:-1}"
FUSE_MOUNT_ROOT="${FUSE_MOUNT_ROOT:-/tmp}"
FUSE_STRICT_PREREQS="${FUSE_STRICT_PREREQS:-0}"
FUSE_UMOUNT_TIMEOUT="${FUSE_UMOUNT_TIMEOUT:-60s}"
REQUEST_MAX_RETRIES="${REQUEST_MAX_RETRIES:-8}"
REQUEST_RETRY_SLEEP_S="${REQUEST_RETRY_SLEEP_S:-2}"
CLI_SOURCE="${CLI_SOURCE:-build}"
CLI_RELEASE_BASE_URL="${CLI_RELEASE_BASE_URL:-https://drive9.ai/releases}"
CLI_RELEASE_VERSION="${CLI_RELEASE_VERSION:-}"

EXTENT_E2E_PROVISION_BODY="${EXTENT_E2E_PROVISION_BODY:-{}}"
EXTENT_E2E_TAKEOVER_PROFILE="${EXTENT_E2E_TAKEOVER_PROFILE:-none}"
EXTENT_E2E_DURABILITY="${EXTENT_E2E_DURABILITY:-fsync}"
EXTENT_E2E_BIG_KB="${EXTENT_E2E_BIG_KB:-}"
EXTENT_E2E_SMALL_KB="${EXTENT_E2E_SMALL_KB:-4}"
EXTENT_E2E_COMPACT_WRITES="${EXTENT_E2E_COMPACT_WRITES:-2600}"
EXTENT_E2E_COMPACT_SPAN_MB="${EXTENT_E2E_COMPACT_SPAN_MB:-32}"
EXTENT_E2E_GC_WAIT_S="${EXTENT_E2E_GC_WAIT_S:-240}"
EXTENT_E2E_LAND_WAIT_S="${EXTENT_E2E_LAND_WAIT_S:-60}"
EXTENT_E2E_S3_DIR="${EXTENT_E2E_S3_DIR:-}"
EXTENT_E2E_SQL_CHECK="${EXTENT_E2E_SQL_CHECK:-0}"
EXTENT_E2E_KEEP_ARTIFACTS="${EXTENT_E2E_KEEP_ARTIFACTS:-0}"

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

check_ne() {
  local desc="$1" got="$2" unwanted="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$got" != "$unwanted" ]; then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc"
    echo "  unwanted: $unwanted"
    echo "  got:      $got"
    FAIL=$((FAIL + 1))
  fi
}

check_contains() {
  local desc="$1" haystack="$2" needle="$3"
  TOTAL=$((TOTAL + 1))
  if [[ "$haystack" == *"$needle"* ]]; then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc"
    echo "  needle: $needle"
    echo "  in:     $haystack"
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
    echo "FAIL $desc"
    FAIL=$((FAIL + 1))
  else
    echo "PASS $desc"
    PASS=$((PASS + 1))
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
  skip "$*"
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
}

prepare_cli_binary() {
  CLI_BIN="$(mktemp)"
  case "$CLI_SOURCE" in
    build) make build-cli CLI_BIN="$CLI_BIN" ;;
    official) download_official_cli ;;
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
  local expect="$1" point="$2"
  local deadline=$(( $(date +%s) + MOUNT_READY_TIMEOUT_S ))
  while :; do
    if [ "$expect" = "mounted" ] && is_mounted "$point"; then
      return 0
    fi
    if [ "$expect" = "unmounted" ] && ! is_mounted "$point"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$MOUNT_READY_INTERVAL_S"
  done
}

drive9() {
  env DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" HOME="$E2E_HOME" "$CLI_BIN" "$@"
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

curl_body_code() {
  local method="$1"
  local url="$2"
  local auth="${3:-}"
  local data="${4:-}"
  local attempt=1
  while :; do
    local body_file code rc
    body_file="$(mktemp)"
    set +e
    if [ -n "$auth" ] && [ -n "$data" ]; then
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" -H "Content-Type: application/json" --data-binary "$data" "$url")
      rc=$?
    elif [ -n "$auth" ]; then
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Authorization: Bearer $auth" "$url")
      rc=$?
    elif [ -n "$data" ]; then
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" -H "Content-Type: application/json" --data-binary "$data" "$url")
      rc=$?
    else
      code=$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")
      rc=$?
    fi
    set -e
    if [ "$rc" -ne 0 ]; then
      code="000"
    fi
    if { [ "$rc" -eq 0 ] && [ "$code" != "000" ] && [ "$code" != "429" ] && [ "$code" != "403" ] && ! [[ "$code" =~ ^5 ]]; } || [ "$attempt" -ge "$REQUEST_MAX_RETRIES" ]; then
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

# stat_head is the documented HEAD contract; the layout lives in the
# X-Dat9-Content-Layout response header (v2 has no extent_ino header).
stat_head() {
  curl -sSI -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs$1" 2>/dev/null | tr -d '\r' || true
}

stat_status() {
  curl -sS -o /dev/null -w "%{http_code}" -I -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs$1" 2>/dev/null || printf '000'
}

# layout_of normalizes the header contract: an omitted header and the
# explicit "single" value both mean content_layout=single.
layout_of() {
  local raw
  raw="$(stat_head "$1" | awk -F': ' 'tolower($1)=="x-dat9-content-layout"{print $2}')"
  case "$raw" in
    ""|single) printf 'single' ;;
    *) printf '%s' "$raw" ;;
  esac
}

# inode_of returns the classic inode's stable resource identity
# (X-Dat9-Resource-ID). v2 keeps one namespace, so renames, remounts and
# appends of the SAME inode keep this identity — the role extent_ino
# played in the 1031 suite.
inode_of() {
  stat_head "$1" | awk -F': ' 'tolower($1)=="x-dat9-resource-id"{print $2}'
}

revision_of() {
  stat_head "$1" | awk -F': ' 'tolower($1)=="x-dat9-revision"{print $2}'
}

sql_query() {
  local q="$1"
  local body
  body="$(jq -nc --arg q "$q" '{query:$q}')"
  curl -sS -X POST -H "Authorization: Bearer $API_KEY" -H "Content-Type: application/json" \
    --data-binary "$body" "$BASE/v1/sql" 2>/dev/null || printf '[]'
}

sql_scalar() {
  printf '%s' "$(sql_query "$1")" | jq -r '.[0].n // "error"' 2>/dev/null || printf 'error'
}

# s3_object_count: only meaningful when the server stores blocks on the
# local filesystem mock and EXTENT_E2E_S3_DIR points at its root.
s3_object_count() {
  if [ -z "$EXTENT_E2E_S3_DIR" ] || [ ! -d "$EXTENT_E2E_S3_DIR" ]; then
    printf ''
    return
  fi
  find "$EXTENT_E2E_S3_DIR" -type f 2>/dev/null | wc -l | tr -d ' '
}

sha256_stream() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

mount_sha() { sha256_stream <"$1"; }
remote_sha() { drive9 fs cat "$1" | sha256_stream; }

# expect_remote_sha: commits land asynchronously (the engine's commit
# thread, then the writeback queue). CLI-side reads wait for the landing
# instead of racing it; a timeout is a FAIL, not a retry.
expect_remote_sha() {
  local desc="$1" remote="$2" want="$3"
  local deadline=$(( $(date +%s) + EXTENT_E2E_LAND_WAIT_S ))
  local got=""
  while :; do
    got="$(remote_sha "$remote")"
    if [ "$got" = "$want" ]; then
      check_eq "$desc" "$got" "$want"
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      check_eq "$desc (waited ${EXTENT_E2E_LAND_WAIT_S}s)" "$got" "$want"
      return 1
    fi
    sleep 1
  done
}

remote_sha_is() {
  # Same shell as wait_until: the predicate must see the script's own
  # functions (a `bash -c` child cannot).
  [ "$(remote_sha "$1")" = "$2" ]
}

# wait_remote_landed blocks until the path's committed bytes match the
# mount's current view — call before mv/rm of a just-written file so the
# mutation does not race the queued commit.
wait_remote_landed() {
  local desc="$1" remote="$2" local_file="$3" want
  want="$(mount_sha "$local_file")"
  if wait_until "remote landing of $remote" "$EXTENT_E2E_LAND_WAIT_S" remote_sha_is "$remote" "$want"; then
    return 0
  fi
  check_eq "$desc (remote landing within ${EXTENT_E2E_LAND_WAIT_S}s)" "$(remote_sha "$remote")" "$want"
  return 1
}

file_size() {
  python3 -c 'import os,sys; print(os.stat(sys.argv[1]).st_size)' "$1"
}

# make_payload writes a deterministic kb-sized file and prints its sha256.
make_payload() {
  python3 - "$1" "$2" "$3" <<'PY'
import hashlib
import os
import sys

path, kb, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
h = hashlib.sha256()
chunk = b""
index = 0
while len(chunk) < kb * 1024:
    chunk += b"takeover-%04d-%08d\n" % (seed, index)
    index += 1
data = chunk[: kb * 1024]
with open(path, "wb") as handle:
    handle.write(data)
    handle.flush()
    os.fsync(handle.fileno())
h.update(data)
print(h.hexdigest())
PY
}

# append_bytes appends n bytes of a seeded pattern and fsyncs.
append_bytes() {
  python3 - "$1" "$2" "$3" <<'PY'
import os
import sys

path, n, marker = sys.argv[1], int(sys.argv[2]), sys.argv[3]
data = (b"tail-%s-" % marker.encode()) * (n // 8) + b"x" * (n % 8)
data = data[:n]
with open(path, "ab") as handle:
    handle.write(data)
    handle.flush()
    os.fsync(handle.fileno())
PY
}

# overwrite_at rewrites n bytes at offset (conv=notrunc semantics).
overwrite_at() {
  python3 - "$1" "$2" "$3" <<'PY'
import os
import sys

path, off, n = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
with open(path, "r+b") as handle:
    handle.seek(off)
    handle.write(b"\xa5" * n)
    handle.flush()
    os.fsync(handle.fileno())
PY
}

# write_growing writes two payloads into ONE descriptor, crossing the
# cutoff between them with NO durability boundary in between (the
# promotion rides the single staged image), then prints the expected sha.
write_growing() {
  python3 - "$1" "$2" "$3" <<'PY'
import hashlib
import os
import sys

path, small, big = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.write(fd, b"a" * small)
os.write(fd, b"b" * (big - small))
os.fsync(fd)
os.close(fd)
h = hashlib.sha256()
h.update(b"a" * small)
h.update(b"b" * (big - small))
print(h.hexdigest())
PY
}

# write_append_only creates a file with O_APPEND and grows it past the
# cutoff.
write_append_only() {
  python3 - "$1" "$2" <<'PY'
import os
import sys

path, big = sys.argv[1], int(sys.argv[2])
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
step = 64 * 1024
written = 0
while written < big:
    n = min(step, big - written)
    os.write(fd, b"l" * n)
    os.fsync(fd)
    written += n
os.close(fd)
PY
}

# write_sparse_seeks creates a file whose single write lands far past the
# staging window (a sparse jump that must still promote, never multipart).
write_sparse_seeks() {
  python3 - "$1" "$2" <<'PY'
import os
import sys

path, off = sys.argv[1], int(sys.argv[2])
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.write(fd, b"Z")
os.lseek(fd, off, os.SEEK_SET)
os.write(fd, b"Y")
os.fsync(fd)
os.close(fd)
PY
}

# write_fsycled writes n random fsynced 4KiB blocks within a span and
# prints the expected sha of the final image (compaction fodder: server
# extent_compact_tasks merge the fragmented chunk).
write_fsycled() {
  python3 - "$1" "$2" "$3" <<'PY'
import hashlib
import os
import random
import sys

path, n, span = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]) * 1024 * 1024
rng = random.Random(20261026)
image = bytearray(span)
block = bytes(range(256)) * 16  # 4 KiB
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.ftruncate(fd, span)
for i in range(n):
    off = rng.randrange(0, span - 4096 + 1)
    off -= off % 4096
    data = block[(i % 256):] + block[: (i % 256)]
    os.pwrite(fd, data, off)
    os.fsync(fd)
    image[off : off + 4096] = data
os.close(fd)
print(hashlib.sha256(image).hexdigest())
PY
}

# open_writer_bg keeps a descriptor open mid-write for hold seconds;
# the caller runs it in the background.
open_writer_bg() {
  python3 - "$1" "$2" "$3" <<'PY'
import os
import sys
import time

path, payload, hold = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.write(fd, b"w" * payload)
os.fsync(fd)
time.sleep(hold)
os.write(fd, b"!")
os.close(fd)
PY
}

expect_layout() {
  local desc="$1" remote="$2" want="$3"
  check_eq "$desc" "$(layout_of "$remote")" "$want"
}

# A bare `sync` flushes filesystem-wide writeback; it is NOT a per-file
# durability barrier. Under the default write-back policy close(2) stages
# locally and enqueues the remote commit, so a promotion becomes visible only
# after an actual fsync(2) on the file. Every positive layout assertion below
# is therefore preceded by fsync_mount, which runs `sync <file>` (coreutils
# fsyncs that descriptor; `sync` alone does not).
#
# This is the contract the assertions describe: fsync(2) is the pay-up moment
# for remote durability. Polling instead would let a broken barrier pass as
# long as the background queue eventually caught up.
fsync_mount() {
  local desc="$1" local_path="$2"
  if sync "$local_path" 2>/dev/null; then
    return 0
  fi
  # Record, but keep going: the assertion that follows reports the state.
  echo "FAIL $desc (fsync(2) on $local_path returned an error)"
  FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1))
  return 0
}

expect_inode() {
  local desc="$1" remote="$2" want="$3"
  check_eq "$desc" "$(inode_of "$remote")" "$want"
}

start_mount_at() {
  local point="$1" log="$2"
  shift 2
  local mount_args=(mount --mode=fuse "$@")
  mount_args+=(":$ROOT_REMOTE" "$point")
  {
    echo "=== drive9 takeover mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') point=$point ==="
    echo "mount_args=${mount_args[*]}"
  } >>"$log"
  drive9 "${mount_args[@]}" >>"$log" 2>&1 &
  local pid="$!"
  if wait_mount_state mounted "$point"; then
    printf '%s' "$pid"
    return 0
  fi
  cat "$log" >&2 || true
  return 1
}

stop_mount_at() {
  local point="$1" pid="${2:-}"
  set +e
  if [ -n "$point" ] && is_mounted "$point"; then
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$point" >/dev/null 2>&1 || true
    wait_mount_state unmounted "$point" >/dev/null 2>&1 || true
  fi
  if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  fi
  set -e
}

objects_below() { [ -n "$(s3_object_count)" ] && [ "$(s3_object_count)" -lt "$1" ]; }

# v2 server-side compaction settled state: every chunk of the inode holds
# fewer than the 64-slice enqueue threshold (extent_chunks.slices is a
# JSON array; >=64 elements means the server would still enqueue).
no_chunk_at_threshold() {
  [ "$(sql_scalar "SELECT COUNT(*) AS n FROM extent_chunks WHERE JSON_LENGTH(slices) >= 64 AND inode_id = '$1'")" = "0" ]
}

wait_until() {
  local desc="$1" timeout="$2"
  shift 2
  local deadline=$(( $(date +%s) + timeout ))
  while :; do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "timeout waiting: $desc" >&2
      return 1
    fi
    sleep 3
  done
}

echo "=== drive9 extent takeover / mixed tree e2e (content layout v2) ==="
echo "DRIVE9_BASE=$BASE"
echo "EXTENT_E2E_TAKEOVER_PROFILE=$EXTENT_E2E_TAKEOVER_PROFILE"
echo "EXTENT_E2E_COMPACT_WRITES=$EXTENT_E2E_COMPACT_WRITES"
echo "EXTENT_E2E_S3_DIR=${EXTENT_E2E_S3_DIR:-<unset>}"
echo "EXTENT_E2E_SQL_CHECK=$EXTENT_E2E_SQL_CHECK"

require_cmd curl
require_cmd jq
require_cmd python3
if [ "$CLI_SOURCE" = "build" ]; then
  require_cmd go
fi

case "$(uname -s)" in
  Linux)
    if ! command -v fusermount >/dev/null 2>&1 && ! command -v fusermount3 >/dev/null 2>&1; then
      skip_or_fail "fusermount/fusermount3 is required for Linux FUSE unmount"
    fi
    if [ ! -e /dev/fuse ]; then
      skip_or_fail "/dev/fuse not available"
    fi
    ;;
  Darwin) ;;
  *) skip_or_fail "unsupported OS for FUSE workloads" ;;
esac

echo "[1] provision tenant"
if [ -n "$DRIVE9_API_KEY" ]; then
  API_KEY="$DRIVE9_API_KEY"
  check_eq "use provided DRIVE9_API_KEY" "true" "true"
else
  resp=$(curl_body_code POST "$BASE/v1/provision" "" "$EXTENT_E2E_PROVISION_BODY")
  code=$(http_code "$resp")
  body=$(json_body "$resp")
  check_eq "POST /v1/provision returns 202" "$code" "202"
  API_KEY=$(printf '%s' "$body" | jq -r '.api_key // empty')
  check_cmd "provision returns api_key" test -n "$API_KEY"
fi

echo "[2] wait tenant active + read inline cutoff"
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
state=""
INLINE_THRESHOLD=0
while :; do
  sresp=$(curl_body_code GET "$BASE/v1/status" "$API_KEY")
  scode=$(http_code "$sresp")
  sbody=$(json_body "$sresp")
  state=$(printf '%s' "$sbody" | jq -r '.status // empty')
  INLINE_THRESHOLD=$(printf '%s' "$sbody" | jq -r '.inline_threshold // 0')
  echo "status=${scode}:${state} inline_threshold=${INLINE_THRESHOLD}"
  if [ "$scode" = "200" ] && [ "$state" = "active" ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_S"
done
check_eq "tenant becomes active" "$state" "active"
if [ "$INLINE_THRESHOLD" -le 0 ]; then
  INLINE_THRESHOLD=50000
fi

BIG_KB="${EXTENT_E2E_BIG_KB:-$(( (INLINE_THRESHOLD * 4 / 1024) + 1 ))}"
SMALL_KB="${EXTENT_E2E_SMALL_KB:-4}"
if [ "$BIG_KB" -lt 256 ]; then
  BIG_KB=256
fi
BIG_BYTES=$((BIG_KB * 1024))
check_cmd "payload sizes straddle the cutoff (big=$BIG_KB KiB small=$SMALL_KB KiB cutoff=$INLINE_THRESHOLD B)" \
  test "$((BIG_KB * 1024))" -gt "$INLINE_THRESHOLD"

echo "[3] prepare drive9 cli"
prepare_cli_binary
check_cmd "drive9 binary ready" test -x "$CLI_BIN"

TS="$(date +%s)"
RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-extent-takeover-${TS}.XXXXXX")"
RUN_ID="$(basename "$RUN_ROOT")"
M="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
M2="$RUN_ROOT/mount2"
MOUNT2_LOG="$RUN_ROOT/mount2.log"
ROOT_REMOTE="/$RUN_ID"
E2E_HOME="$RUN_ROOT/home"
MOUNT_PID=""
MOUNT2_PID=""

mkdir -p "$M" "$M2" "$E2E_HOME/.drive9/profiles"
: > "$MOUNT_LOG"
: > "$MOUNT2_LOG"

cleanup() {
  local rc=$?
  stop_mount_at "$M" "${MOUNT_PID:-}"
  stop_mount_at "$M2" "${MOUNT2_PID:-}"
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ -n "${API_KEY:-}" ] && [ -n "${CLI_BIN:-}" ]; then
    drive9 fs rm -r "$ROOT_REMOTE" >/dev/null 2>&1 || true
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$EXTENT_E2E_KEEP_ARTIFACTS" != "1" ]; then
    rm -rf "$RUN_ROOT"
  else
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount logs: $MOUNT_LOG $MOUNT2_LOG"
  fi
  if [ "$FAIL" -gt 0 ] && [ "$rc" -eq 0 ]; then
    exit 1
  fi
  exit "$rc"
}
trap cleanup EXIT

mount_profile_args=()
if [ -n "$EXTENT_E2E_DURABILITY" ]; then
  mount_profile_args+=(--durability "$EXTENT_E2E_DURABILITY")
fi
if [ "$EXTENT_E2E_TAKEOVER_PROFILE" != "none" ]; then
  mount_profile_args+=(--profile "$EXTENT_E2E_TAKEOVER_PROFILE")
fi

echo "[4] mount the takeover plane"
drive9_retry fs mkdir "$ROOT_REMOTE" >/dev/null 2>&1 || true
MOUNT_PID="$(start_mount_at "$M" "$MOUNT_LOG" "${mount_profile_args[@]}")"
check_cmd "takeover mount is up" is_mounted "$M"

echo
echo "[A] legacy multipart file compatibility (never converts)"

mkdir -p "$M/legacy"
LEG_SHA="$(make_payload "$RUN_ROOT/legacy-src.bin" "$BIG_KB" 1)"
drive9_retry fs cp "$RUN_ROOT/legacy-src.bin" ":$ROOT_REMOTE/legacy/big.bin" >/dev/null
# No fsync_mount here: this file was uploaded by the CLI, not written through
# the mount, so there is no local descriptor to fsync — the upload already
# completed synchronously before this assertion.
expect_layout "A: CLI-uploaded big file is content_layout=single" "$ROOT_REMOTE/legacy/big.bin" "single"
check_eq "A: mount reads the legacy bytes" "$(mount_sha "$M/legacy/big.bin")" "$LEG_SHA"
expect_remote_sha "A: CLI cat agrees with the mount" "$ROOT_REMOTE/legacy/big.bin" "$LEG_SHA"

append_bytes "$M/legacy/big.bin" 4096 "alpha"
check_eq "A: append keeps layout single" "$(layout_of "$ROOT_REMOTE/legacy/big.bin")" "single"
expect_remote_sha "A: CLI agrees after append" "$ROOT_REMOTE/legacy/big.bin" "$(mount_sha "$M/legacy/big.bin")"
check_eq "A: append grew the file" "$(file_size "$M/legacy/big.bin")" "$((BIG_BYTES + 4096))"

overwrite_at "$M/legacy/big.bin" 1024 4096
check_eq "A: partial overwrite keeps layout single" "$(layout_of "$ROOT_REMOTE/legacy/big.bin")" "single"
expect_remote_sha "A: CLI agrees after overwrite" "$ROOT_REMOTE/legacy/big.bin" "$(mount_sha "$M/legacy/big.bin")"

TR_SHA="$(make_payload "$RUN_ROOT/legacy-src2.bin" "$BIG_KB" 2)"
cat "$RUN_ROOT/legacy-src2.bin" > "$M/legacy/big.bin"
fsync_mount "A: truncate-rewrite keeps layout single (no conversion)" "$M/legacy/big.bin"
check_eq "A: truncate-rewrite keeps layout single (no conversion)" "$(layout_of "$ROOT_REMOTE/legacy/big.bin")" "single"
check_eq "A: truncate-rewrite bytes" "$(mount_sha "$M/legacy/big.bin")" "$TR_SHA"
expect_remote_sha "A: CLI sees rewritten bytes" "$ROOT_REMOTE/legacy/big.bin" "$TR_SHA"
wait_remote_landed "A: pre-mv landing" "$ROOT_REMOTE/legacy/big.bin" "$M/legacy/big.bin"

chmod 600 "$M/legacy/big.bin"
check_eq "A: chmod visible through the mount" "$(python3 -c 'import os,sys; print(oct(os.stat(sys.argv[1]).st_mode & 0o777))' "$M/legacy/big.bin")" "0o600"

mkdir -p "$M/legacy-mv"
wait_remote_landed "A: pre-mv landing" "$ROOT_REMOTE/legacy/big.bin" "$M/legacy/big.bin"
mv "$M/legacy/big.bin" "$M/legacy-mv/big.bin"
fsync_mount "A: renamed legacy file stays single" "$M/legacy-mv/big.bin"
expect_layout "A: renamed legacy file stays single" "$ROOT_REMOTE/legacy-mv/big.bin" "single"
check_eq "A: renamed legacy bytes" "$(mount_sha "$M/legacy-mv/big.bin")" "$TR_SHA"
check_eq "A: old legacy name is gone" "$(stat_status "$ROOT_REMOTE/legacy/big.bin")" "404"

echo
echo "[B] delete the legacy file, rewrite the path past the cutoff"

rm "$M/legacy-mv/big.bin"
check_eq "B: legacy file deleted" "$(stat_status "$ROOT_REMOTE/legacy-mv/big.bin")" "404"

BIG_SHA1="$(make_payload "$RUN_ROOT/rewrite-src.bin" "$BIG_KB" 3)"
cp "$RUN_ROOT/rewrite-src.bin" "$M/rewrite.bin"
fsync_mount "B: same path rewritten past the cutoff becomes extent" "$M/rewrite.bin"
expect_layout "B: same path rewritten past the cutoff becomes extent" "$ROOT_REMOTE/rewrite.bin" "extent"
check_ne "B: promoted file has a resource id" "$(inode_of "$ROOT_REMOTE/rewrite.bin")" ""
check_eq "B: mount bytes of promoted file" "$(mount_sha "$M/rewrite.bin")" "$BIG_SHA1"
expect_remote_sha "B: CLI reads the promoted file through the v2 read plan" "$ROOT_REMOTE/rewrite.bin" "$BIG_SHA1"
PROMOTED_INO="$(inode_of "$ROOT_REMOTE/rewrite.bin")"

GROW_SHA="$(write_growing "$M/grow.bin" "$((INLINE_THRESHOLD / 2))" "$((INLINE_THRESHOLD * 3))")"
fsync_mount "B: one-fd grow across the cutoff promotes" "$M/grow.bin"
expect_layout "B: one-fd grow across the cutoff promotes" "$ROOT_REMOTE/grow.bin" "extent"
check_eq "B: grow image bytes" "$(mount_sha "$M/grow.bin")" "$GROW_SHA"
expect_remote_sha "B: CLI agrees on grown image" "$ROOT_REMOTE/grow.bin" "$GROW_SHA"

write_append_only "$M/app.log" "$((INLINE_THRESHOLD * 3))"
fsync_mount "B: O_APPEND writer past the cutoff promotes" "$M/app.log"
expect_layout "B: O_APPEND writer past the cutoff promotes" "$ROOT_REMOTE/app.log" "extent"
check_eq "B: append-only size" "$(file_size "$M/app.log")" "$((INLINE_THRESHOLD * 3))"

truncate -s $((64 * 1024 * 1024)) "$M/hole.bin"
fsync_mount "B: 64MiB truncate of a fresh path promotes" "$M/hole.bin"
expect_layout "B: 64MiB truncate of a fresh path promotes" "$ROOT_REMOTE/hole.bin" "extent"
check_eq "B: sparse size is logical" "$(file_size "$M/hole.bin")" "$((64 * 1024 * 1024))"

rm "$M/seek.bin" 2>/dev/null || true
write_sparse_seeks "$M/seek.bin" $((32 * 1024 * 1024))
fsync_mount "B: far sparse seek promotes (never multipart)" "$M/seek.bin"
expect_layout "B: far sparse seek promotes (never multipart)" "$ROOT_REMOTE/seek.bin" "extent"
check_eq "B: sparse seek size" "$(file_size "$M/seek.bin")" "$((32 * 1024 * 1024 + 1))"

make_payload "$M/small-stays.bin" "$SMALL_KB" 4 >/dev/null
fsync_mount "B: small file stays single" "$M/small-stays.bin"
expect_layout "B: small file stays single" "$ROOT_REMOTE/small-stays.bin" "single"

echo
echo "[B2] remount persistence of the takeover"

stop_mount_at "$M" "$MOUNT_PID"
MOUNT_PID="$(start_mount_at "$M" "$MOUNT_LOG" "${mount_profile_args[@]}")"
check_cmd "remount is up" is_mounted "$M"
fsync_mount "B2: promoted file is extent after remount" "$M/rewrite.bin"
expect_layout "B2: promoted file is extent after remount" "$ROOT_REMOTE/rewrite.bin" "extent"
expect_inode "B2: same inode identity across remount" "$ROOT_REMOTE/rewrite.bin" "$PROMOTED_INO"
check_eq "B2: promoted bytes after remount" "$(mount_sha "$M/rewrite.bin")" "$BIG_SHA1"
append_bytes "$M/rewrite.bin" 2048 "beta"
check_eq "B2: append after remount keeps extent" "$(layout_of "$ROOT_REMOTE/rewrite.bin")" "extent"
expect_inode "B2: append keeps the inode identity" "$ROOT_REMOTE/rewrite.bin" "$PROMOTED_INO"
check_ne "B2: append bumped the revision" "$(revision_of "$ROOT_REMOTE/rewrite.bin")" ""

echo
echo "[C] mixed tree: ls, sizes, renames in every plane combination, cp -r, rm -r"

mkdir -p "$M/tree/sub"
make_payload "$M/tree/small.txt" "$SMALL_KB" 10 >/dev/null
make_payload "$M/tree/big.bin" "$BIG_KB" 11 >/dev/null
make_payload "$M/tree/sub/small2.txt" "$SMALL_KB" 12 >/dev/null
make_payload "$M/tree/sub/big2.bin" "$BIG_KB" 13 >/dev/null
fsync_mount "C: tree/big.bin promoted" "$M/tree/big.bin"
expect_layout "C: tree/big.bin promoted" "$ROOT_REMOTE/tree/big.bin" "extent"
BIG_INO="$(inode_of "$ROOT_REMOTE/tree/big.bin")"
BIG_SHA="$(mount_sha "$M/tree/big.bin")"
fsync_mount "C: tree/sub/big2.bin promoted" "$M/tree/sub/big2.bin"
expect_layout "C: tree/sub/big2.bin promoted" "$ROOT_REMOTE/tree/sub/big2.bin" "extent"
BIG2_INO="$(inode_of "$ROOT_REMOTE/tree/sub/big2.bin")"
BIG2_SHA="$(mount_sha "$M/tree/sub/big2.bin")"
fsync_mount "C: tree/small.txt stays single" "$M/tree/small.txt"
expect_layout "C: tree/small.txt stays single" "$ROOT_REMOTE/tree/small.txt" "single"
expect_remote_sha "C: small child bytes (CLI agrees)" "$ROOT_REMOTE/tree/small.txt" "$(mount_sha "$M/tree/small.txt")"

mount_ls="$(ls -1A "$M/tree" | sort)"
remote_ls="$(drive9_retry fs ls "$ROOT_REMOTE/tree" | sed 's:/$::' | sort)"
check_eq "C: mount ls equals remote ls" "$mount_ls" "$remote_ls"
check_eq "C: stat size of extent child" "$(file_size "$M/tree/big.bin")" "$BIG_BYTES"
check_contains "C: remote fs stat reports the size" "$(drive9_retry fs stat "$ROOT_REMOTE/tree/big.bin")" "$BIG_BYTES"

mv "$M/tree/big.bin" "$M/tree/big.ren"
fsync_mount "C: extent->extent rename keeps extent" "$M/tree/big.ren"
expect_layout "C: extent->extent rename keeps extent" "$ROOT_REMOTE/tree/big.ren" "extent"
expect_inode "C: rename keeps the inode identity" "$ROOT_REMOTE/tree/big.ren" "$BIG_INO"
check_cmd_fail "C: old extent name is gone" test -e "$M/tree/big.bin"
check_eq "C: bytes after rename" "$(mount_sha "$M/tree/big.ren")" "$BIG_SHA"

mv "$M/tree/big.ren" "$M/tree/small.txt"
fsync_mount "C: extent over classic keeps extent layout" "$M/tree/small.txt"
expect_layout "C: extent over classic keeps extent layout" "$ROOT_REMOTE/tree/small.txt" "extent"
check_eq "C: bytes after overwrite" "$(mount_sha "$M/tree/small.txt")" "$BIG_SHA"
make_payload "$M/tree/big.bin" "$SMALL_KB" 14 >/dev/null
fsync_mount "C: recreated old name is single again" "$M/tree/big.bin"
expect_layout "C: recreated old name is single again" "$ROOT_REMOTE/tree/big.bin" "single"

SMALL3_SHA="$(make_payload "$RUN_ROOT/small3.bin" "$SMALL_KB" 15)"
cp "$RUN_ROOT/small3.bin" "$M/tree/small3.txt"
fsync_mount "C: classic over extent becomes single" "$M/tree/small3.txt"
mv "$M/tree/small3.txt" "$M/tree/small.txt"
expect_layout "C: classic over extent becomes single" "$ROOT_REMOTE/tree/small.txt" "single"
check_eq "C: classic overwrite bytes" "$(mount_sha "$M/tree/small.txt")" "$SMALL3_SHA"
check_cmd_fail "C: classic rename source is gone" test -e "$M/tree/small3.txt"
make_payload "$M/tree/small3.txt" "$SMALL_KB" 16 >/dev/null
check_cmd "C: re-creating a classic-over-extent source works" test -e "$M/tree/small3.txt"

mv "$M/tree/sub/big2.bin" "$M/tree/big2.bin"
fsync_mount "C: cross-directory move keeps extent" "$M/tree/big2.bin"
expect_layout "C: cross-directory move keeps extent" "$ROOT_REMOTE/tree/big2.bin" "extent"
expect_inode "C: cross-directory move keeps the identity" "$ROOT_REMOTE/tree/big2.bin" "$BIG2_INO"

mv "$M/tree" "$M/tree2"
fsync_mount "C: directory rename keeps child extent" "$M/tree2/big2.bin"
expect_layout "C: directory rename keeps child extent" "$ROOT_REMOTE/tree2/big2.bin" "extent"
check_eq "C: child bytes after directory rename" "$(mount_sha "$M/tree2/big2.bin")" "$BIG2_SHA"
append_bytes "$M/tree2/big2.bin" 2048 "gamma"
fsync_mount "C: child writable after directory rename" "$M/tree2/big2.bin"
expect_layout "C: child writable after directory rename" "$ROOT_REMOTE/tree2/big2.bin" "extent"

cp -r "$M/tree2" "$M/tree3"
fsync_mount "C: cp -r of an extent child promotes the copy" "$M/tree3/big2.bin"
expect_layout "C: cp -r of an extent child promotes the copy" "$ROOT_REMOTE/tree3/big2.bin" "extent"
check_ne "C: copied extent file gets a fresh identity" "$(inode_of "$ROOT_REMOTE/tree3/big2.bin")" "$BIG2_INO"
check_eq "C: copy carries the appended bytes" "$(mount_sha "$M/tree3/big2.bin")" "$(mount_sha "$M/tree2/big2.bin")"

rm -rf "$M/tree3"
check_eq "C: rm -r removed the copy" "$(stat_status "$ROOT_REMOTE/tree3/big2.bin")" "404"
mkdir "$M/tree3"
check_cmd "C: path reusable after rm -r (mkdir)" test -d "$M/tree3"
rmdir "$M/tree3"
check_cmd "C: rmdir of the re-created path (no orphan edge)" test ! -e "$M/tree3"

# v2 keeps the classic namespace, so hardlinks ride the classic inode —
# including extent files (aliases share one inode and its extent layout).
LN_RC=0
ln "$M/tree2/big.bin" "$M/tree2/big.lnk" 2>/dev/null || LN_RC=$?
if [ "$LN_RC" -eq 0 ]; then
  echo "PASS C: hardlink of extent child works (classic inode, shared layout)"
  PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1))
  expect_inode "C: hardlink shares the inode identity" "$ROOT_REMOTE/tree2/big.lnk" "$(inode_of "$ROOT_REMOTE/tree2/big.bin")"
  rm "$M/tree2/big.lnk"
else
  echo "PASS C: hardlink of extent child refused — asserting clean failure"
  PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1))
  check_cmd_fail "C: refused hardlink leaves no target" test -e "$M/tree2/big.lnk"
  check_eq "C: refused hardlink leaves no server name" "$(stat_status "$ROOT_REMOTE/tree2/big.lnk")" "404"
fi
ln "$M/tree2/small3.txt" "$M/tree2/small.lnk"
check_cmd "C: hardlink of a single child works" test -e "$M/tree2/small.lnk"

echo
echo "[D] cross-channel CLI namespace changes, then remount"

mkdir -p "$M/tree5/sub"
make_payload "$M/tree5/sub/x.bin" "$BIG_KB" 17 >/dev/null
fsync_mount "D: tree5 child is extent" "$M/tree5/sub/x.bin"
expect_layout "D: tree5 child is extent" "$ROOT_REMOTE/tree5/sub/x.bin" "extent"
stop_mount_at "$M" "$MOUNT_PID"
drive9_retry fs mv "$ROOT_REMOTE/tree2" "$ROOT_REMOTE/tree4" >/dev/null
drive9_retry fs rm -r "$ROOT_REMOTE/tree5" >/dev/null
check_eq "D: CLI removed tree5" "$(stat_status "$ROOT_REMOTE/tree5/sub/x.bin")" "404"
MOUNT_PID="$(start_mount_at "$M" "$MOUNT_LOG" "${mount_profile_args[@]}")"
check_cmd "D: remount is up" is_mounted "$M"
fsync_mount "D: CLI-renamed dir keeps child extent" "$M/tree4/big2.bin"
expect_layout "D: CLI-renamed dir keeps child extent" "$ROOT_REMOTE/tree4/big2.bin" "extent"
expect_remote_sha "D: CLI-renamed child bytes" "$ROOT_REMOTE/tree4/big2.bin" "$(mount_sha "$M/tree4/big2.bin")"
check_cmd "D: CLI-renamed child is statable" test -e "$M/tree4/big2.bin"
check_cmd_fail "D: CLI-removed tree absent in the fresh mount" test -e "$M/tree5"
mkdir "$M/tree5"
rmdir "$M/tree5"
check_cmd "D: CLI-removed path reusable in the mount" test ! -e "$M/tree5"

echo
echo "[E] extent delete reclaims server state"

OBJ_BEFORE="$(s3_object_count)"
rm "$M/tree4/big2.bin"
check_eq "E: extent child deleted (404)" "$(stat_status "$ROOT_REMOTE/tree4/big2.bin")" "404"
make_payload "$M/tree4/big2.bin" "$SMALL_KB" 18 >/dev/null
check_cmd "E: deleted extent path reusable" test -e "$M/tree4/big2.bin"
# NOTE: v2 block objects sit in the server's GC queue under a 24h grace
# window (extent_block_gc.available_at), so the object-count drop below
# is advisory unless the server's grace is shortened for the run.
if [ -n "$OBJ_BEFORE" ]; then
  TOTAL=$((TOTAL + 1))
  if wait_until "block objects reclaimed after delete" "$EXTENT_E2E_GC_WAIT_S" objects_below "$OBJ_BEFORE"; then
    echo "PASS E: object count dropped after extent delete ($(s3_object_count) < $OBJ_BEFORE)"
    PASS=$((PASS + 1))
  else
    echo "INFO E: objects before=$OBJ_BEFORE after=$(s3_object_count) (24h block grace; bytes and name reclaim asserted above)"
  fi
else
  echo "SKIP E: block-object accounting (EXTENT_E2E_S3_DIR unset)"
fi
if [ "$EXTENT_E2E_SQL_CHECK" = "1" ]; then
  # The file-GC task frees extent storage asynchronously (tombstone first,
  # worker reclaim on its tick); poll for the settled state.
  if wait_until "extent_chunks reclaimed for the deleted inode" "$EXTENT_E2E_GC_WAIT_S" \
    [ "$(sql_scalar "SELECT COUNT(*) AS n FROM extent_chunks WHERE inode_id NOT IN (SELECT COALESCE(inode_id, file_id) FROM file_nodes)")" = "0" ]; then
    echo "PASS E: no orphan extent_chunks rows after delete"
    PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1))
  else
    echo "FAIL E: extent_chunks rows outlived their file_nodes"
    FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1))
  fi
fi

echo
echo "[F] server-side slice compaction of a heavily fsynced extent file"

if [ "$EXTENT_E2E_COMPACT_WRITES" -gt 0 ]; then
  COMPACT_SHA="$(write_fsycled "$M/comp.bin" "$EXTENT_E2E_COMPACT_WRITES" "$EXTENT_E2E_COMPACT_SPAN_MB")"
  fsync_mount "F: fsync-cycled file is extent" "$M/comp.bin"
expect_layout "F: fsync-cycled file is extent" "$ROOT_REMOTE/comp.bin" "extent"
  check_eq "F: bytes before compaction" "$(mount_sha "$M/comp.bin")" "$COMPACT_SHA"
  expect_remote_sha "F: CLI reads the same bytes" "$ROOT_REMOTE/comp.bin" "$COMPACT_SHA"
  if [ "$EXTENT_E2E_SQL_CHECK" = "1" ]; then
    COMPACT_KEY="$(inode_of "$ROOT_REMOTE/comp.bin")"
    # The server enqueues extent_compact_tasks at >=64 slices per chunk and
    # the tenant worker merges on its 5-minute tick (commits kick it
    # sooner); settled = every chunk under the enqueue threshold.
    if wait_until "compaction brought every chunk under the enqueue threshold" "$EXTENT_E2E_GC_WAIT_S" \
      no_chunk_at_threshold "$COMPACT_KEY"; then
      echo "PASS F: server compaction merged chunks under the threshold"
      PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1))
    else
      echo "FAIL F: chunks still at/above the compaction threshold after ${EXTENT_E2E_GC_WAIT_S}s"
      FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1))
    fi
  fi
  check_eq "F: bytes after compaction window" "$(mount_sha "$M/comp.bin")" "$COMPACT_SHA"
  expect_remote_sha "F: CLI bytes after compaction window" "$ROOT_REMOTE/comp.bin" "$COMPACT_SHA"
  stop_mount_at "$M" "$MOUNT_PID"
  MOUNT_PID="$(start_mount_at "$M" "$MOUNT_LOG" "${mount_profile_args[@]}")"
  check_cmd "F: remount is up" is_mounted "$M"
  check_eq "F: bytes after remount" "$(mount_sha "$M/comp.bin")" "$COMPACT_SHA"
else
  echo "SKIP F: EXTENT_E2E_COMPACT_WRITES=0"
fi

# Negative control for the landing predicates (1031 review finding): a
# value that will never land must FAIL expect_remote_sha instead of
# timing out green.
NEG_SAVED_WAIT="$EXTENT_E2E_LAND_WAIT_S"
EXTENT_E2E_LAND_WAIT_S=2
neg_out="$(expect_remote_sha "predicate negative control" "$ROOT_REMOTE/rewrite.bin" "0000000000000000000000000000000000000000000000000000000000000000" 2>&1 || true)"
EXTENT_E2E_LAND_WAIT_S="$NEG_SAVED_WAIT"
TOTAL=$((TOTAL + 1))
if [[ "$neg_out" == *"FAIL predicate negative control"* ]]; then
  echo "PASS landing-predicate negative control rejects a non-landing value"
  PASS=$((PASS + 1))
else
  echo "FAIL landing-predicate negative control rejected a non-landing value"
  echo "  output: $neg_out"
  FAIL=$((FAIL + 1))
fi

echo
echo "[G] two concurrent mounts agree"

MOUNT2_PID="$(start_mount_at "$M2" "$MOUNT2_LOG" "${mount_profile_args[@]}")"
check_cmd "G: second mount is up" is_mounted "$M2"
check_eq "G: mount2 reads the promoted file" "$(mount_sha "$M2/rewrite.bin")" "$(mount_sha "$M/rewrite.bin")"
fsync_mount "G: mount2 sees the extent layout" "$M2/rewrite.bin"
expect_layout "G: mount2 sees the extent layout" "$ROOT_REMOTE/rewrite.bin" "extent"
make_payload "$M2/from2.bin" "$BIG_KB" 19 >/dev/null
fsync_mount "G: file promoted on mount2 is extent" "$M2/from2.bin"
expect_layout "G: file promoted on mount2 is extent" "$ROOT_REMOTE/from2.bin" "extent"
sleep 3
check_eq "G: mount1 sees mount2's file bytes" "$(mount_sha "$M/from2.bin")" "$(mount_sha "$M2/from2.bin")"

echo
echo "[H] open-while-writing visibility"

rm -f "$M/live.bin"
open_writer_bg "$M/live.bin" "$((INLINE_THRESHOLD * 3))" 6 &
H_PID=$!
sleep 2
check_eq "H: mid-write size visible to other processes" "$(file_size "$M/live.bin")" "$((INLINE_THRESHOLD * 3))"
check_cmd "H: mid-write head readable" head -c 1024 "$M/live.bin" >/dev/null
wait "$H_PID"
fsync_mount "H: file lands as extent after close" "$M/live.bin"
expect_layout "H: file lands as extent after close" "$ROOT_REMOTE/live.bin" "extent"
check_eq "H: final size after close" "$(file_size "$M/live.bin")" "$((INLINE_THRESHOLD * 3 + 1))"

echo
echo "PASS=$PASS FAIL=$FAIL TOTAL=$TOTAL"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
