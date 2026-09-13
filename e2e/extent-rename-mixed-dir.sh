#!/usr/bin/env bash
# Extent layout follow-through across file/directory rename and directory ops.
#
# The mount uses a MIXED profile: only the [extent] patterns below are created
# as content_layout=extent, everything else stays content_layout=single.
# Content layout is chosen at create time and is sticky to the inode, so this
# script pins down the contracts a rename must not silently break:
#
#   A. single -> renamed onto an [extent] pattern stays single and keeps bytes
#      (the path glob is a create-time rule, not a path-time rule).
#   B. extent -> renamed off the [extent] pattern stays extent, keeps the SAME
#      extent_ino, keeps bytes, and survives a remount.
#   C. a drive9 directory holding a mix of extent and single children survives
#      ls / rename / rm -r with every child reachable and layout-stable.
#   D. unlink + create of the same extent path allocates a NEW extent_ino
#      (journal unlink-then-create; no 409 against the leftover inode).
#   E. extent -> extent file rename inside one directory keeps the inode.
#   F. a directory renamed by another channel (CLI/HTTP) is still usable from a
#      freshly mounted FUSE view: children keep layout + inode, stay readable
#      and writable, and can be unlinked.
#   G. a tree removed by another channel (CLI `fs rm -r`) leaves no JuiceFS
#      directory edge behind: mkdir + rmdir of the same path must succeed.
#
# G is the regression guard for an orphan JuiceFS dir edge: a server-side
# recursive delete removes extent file edges but not directory edges, so the
# next rmdir of a re-created empty directory fails ENOTEMPTY.
#
# Requires a server whose extent data plane works: POST /v1/data-credential must
# return usable credentials. provider=local with the filesystem S3 mock, or
# MinIO/S3 with static keys, both qualify. Manual-only; see e2e/README.md
# ("CI automation tiers") and docs/design/extent-content-layout.md §9.
#
#   DRIVE9_BASE=http://127.0.0.1:9009 bash e2e/extent-rename-mixed-dir.sh
#
# Env:
#   DRIVE9_API_KEY                  reuse an existing tenant instead of provisioning
#   EXTENT_E2E_PROFILE              profile name written under $HOME/.drive9/profiles (default extent-mixed)
#   EXTENT_E2E_PATTERNS             newline-separated [extent] patterns (default: *.db *.db-wal *.db-journal *.db-shm)
#   EXTENT_E2E_PAYLOAD_KB           deterministic payload size per file (default 256)
#   EXTENT_E2E_DURABILITY           optional --durability value; unset uses the CLI default
#   EXTENT_E2E_SQL_ORPHAN_CHECK     1 = also assert jfs_edge has no orphan dir edge via POST /v1/sql
#   EXTENT_E2E_KEEP_ARTIFACTS       1 = keep the run root and mount log on success
#   FUSE_STRICT_PREREQS             1 = fail instead of skipping when FUSE is unavailable
#   FUSE_MOUNT_ROOT, MOUNT_READY_*, POLL_*, REQUEST_* — same meaning as sibling FUSE suites.

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

EXTENT_E2E_PROFILE="${EXTENT_E2E_PROFILE:-extent-mixed}"
EXTENT_E2E_PAYLOAD_KB="${EXTENT_E2E_PAYLOAD_KB:-256}"
EXTENT_E2E_DURABILITY="${EXTENT_E2E_DURABILITY:-}"
EXTENT_E2E_SQL_ORPHAN_CHECK="${EXTENT_E2E_SQL_ORPHAN_CHECK:-0}"
EXTENT_E2E_KEEP_ARTIFACTS="${EXTENT_E2E_KEEP_ARTIFACTS:-0}"
EXTENT_E2E_PATTERNS="${EXTENT_E2E_PATTERNS:-$(printf '%s\n' '*.db' '*.db-wal' '*.db-journal' '*.db-shm')}"

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

# stat_head is the documented HEAD contract; layout and extent_ino live in
# response headers (X-Dat9-Content-Layout / X-Dat9-Extent-Ino).
stat_head() {
  curl -sSI -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs$1" 2>/dev/null | tr -d '\r' || true
}

stat_status() {
  curl -sS -o /dev/null -w "%{http_code}" -I -H "Authorization: Bearer $API_KEY" "$BASE/v1/fs$1" 2>/dev/null || printf '000'
}

# layout_of normalizes the header contract: an omitted header and the explicit
# "single" value both mean content_layout=single.
layout_of() {
  local raw
  raw="$(stat_head "$1" | awk -F': ' 'tolower($1)=="x-dat9-content-layout"{print $2}')"
  case "$raw" in
    ""|single) printf 'single' ;;
    *) printf '%s' "$raw" ;;
  esac
}

extent_ino_of() {
  stat_head "$1" | awk -F': ' 'tolower($1)=="x-dat9-extent-ino"{print $2}'
}

sha256_stream() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

# mount_sha reads through the FUSE mount; remote_sha reads through the CLI,
# which resolves content_layout=extent through the JuiceFS data reader.
mount_sha() { sha256_stream <"$1"; }
remote_sha() { drive9 fs cat "$1" | sha256_stream; }

write_payload() {
  python3 - "$1" "$2" <<'PY'
import os
import sys

path, kb = sys.argv[1], int(sys.argv[2])
chunk = b""
index = 0
while len(chunk) < kb * 1024:
    chunk += b"extent-e2e-%08d\n" % index
    index += 1
with open(path, "wb") as handle:
    handle.write(chunk[: kb * 1024])
    handle.flush()
    os.fsync(handle.fileno())
PY
}

append_payload() {
  python3 - "$1" "$2" <<'PY'
import os
import sys

path, marker = sys.argv[1], sys.argv[2]
with open(path, "ab") as handle:
    handle.write(b"appended-%s\n" % marker.encode())
    handle.flush()
    os.fsync(handle.fileno())
PY
}

expect_layout() {
  local desc="$1" remote="$2" want="$3"
  check_eq "$desc" "$(layout_of "$remote")" "$want"
}

expect_extent_ino() {
  local desc="$1" remote="$2" want="$3"
  check_eq "$desc" "$(extent_ino_of "$remote")" "$want"
}

expect_sha() {
  local desc="$1" got="$2" want="$3"
  check_eq "$desc" "$got" "$want"
}

start_mount() {
  local mount_args=(mount --mode=fuse)
  if [ -n "$EXTENT_E2E_DURABILITY" ]; then
    mount_args+=(--durability "$EXTENT_E2E_DURABILITY")
  fi
  mount_args+=(--profile "$EXTENT_E2E_PROFILE")
  mount_args+=(":$ROOT_REMOTE" "$MOUNT_POINT")
  {
    echo "=== drive9 extent rename mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "root_remote=$ROOT_REMOTE"
    echo "profile=$EXTENT_E2E_PROFILE"
    echo "mount_args=${mount_args[*]}"
  } >>"$MOUNT_LOG"
  drive9 "${mount_args[@]}" >>"$MOUNT_LOG" 2>&1 &
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

unmount_mount() {
  if is_mounted "$MOUNT_POINT"; then
    if ! drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT"; then
      return 1
    fi
  fi
  if ! wait_mount_state unmounted; then
    return 1
  fi
  if [ -n "${MOUNT_PID:-}" ]; then
    set +e
    wait "$MOUNT_PID" >/dev/null 2>&1
    MOUNT_PID=""
    set -e
  fi
  return 0
}

if ! [[ "$EXTENT_E2E_PAYLOAD_KB" =~ ^[1-9][0-9]*$ ]]; then
  echo "invalid EXTENT_E2E_PAYLOAD_KB: must be >= 1" >&2
  exit 1
fi

echo "=== drive9 extent rename / mixed directory e2e ==="
echo "DRIVE9_BASE=$BASE"
echo "EXTENT_E2E_PROFILE=$EXTENT_E2E_PROFILE"
echo "EXTENT_E2E_PAYLOAD_KB=$EXTENT_E2E_PAYLOAD_KB"
echo "extent patterns:"
printf '%s\n' "$EXTENT_E2E_PATTERNS" | sed 's/^/  /'

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

TS="$(date +%s)"
RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-extent-rename-${TS}.XXXXXX")"
RUN_ID="$(basename "$RUN_ROOT")"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
ROOT_REMOTE="/$RUN_ID"
E2E_HOME="$RUN_ROOT/home"
MOUNT_PID=""

mkdir -p "$MOUNT_POINT" "$E2E_HOME/.drive9/profiles"
: > "$MOUNT_LOG"

cleanup() {
  local rc=$?
  stop_mount
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
    echo "Mount log: $MOUNT_LOG"
  fi
  if [ "$FAIL" -gt 0 ] && [ "$rc" -eq 0 ]; then
    exit 1
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "[4] write mixed profile $EXTENT_E2E_PROFILE"
{
  echo "[extent]"
  printf '%s\n' "$EXTENT_E2E_PATTERNS"
  echo "# [append-log] is intentionally absent: this suite pins content_layout=extent"
  echo "# rename and directory semantics only."
} >"$E2E_HOME/.drive9/profiles/$EXTENT_E2E_PROFILE"
profile_show="$(drive9_retry profile show "$EXTENT_E2E_PROFILE")"
check_eq "profile show lists the extent section" \
  "$(printf '%s' "$profile_show" | awk '/^\[extent\]/{f=1;next} /^\[/{f=0} f && $0 !~ /^#/ && NF {print}' | sort | tr '\n' ',')" \
  "$(printf '%s\n' "$EXTENT_E2E_PATTERNS" | sort | tr '\n' ',')"

echo "[5] mount $ROOT_REMOTE with profile $EXTENT_E2E_PROFILE"
# The mount refuses a remote root that does not exist (startup_permanent 404),
# so create the fixture root first; a re-run of the same RUN_ID tolerates it.
drive9 fs mkdir "$ROOT_REMOTE" >/dev/null 2>&1 || true
if start_mount; then
  check_eq "mount is ready" "true" "true"
else
  check_eq "mount is ready" "false" "true"
fi
if ! is_mounted "$MOUNT_POINT"; then
  echo "mount failed; see $MOUNT_LOG" >&2
  exit 1
fi

M="$MOUNT_POINT"
R="$ROOT_REMOTE"

echo "[6] A: single renamed onto an [extent] pattern stays single"
write_payload "$M/plain.txt" "$EXTENT_E2E_PAYLOAD_KB"
expect_layout "A: plain.txt is single before rename" "$R/plain.txt" "single"
plain_sha="$(mount_sha "$M/plain.txt")"
mv "$M/plain.txt" "$M/renamed.db"
expect_layout "A: renamed.db stays single after rename onto *.db" "$R/renamed.db" "single"
expect_sha "A: renamed.db bytes survive the rename" "$(mount_sha "$M/renamed.db")" "$plain_sha"
append_payload "$M/renamed.db" "A"
expected_a="$RUN_ROOT/expected-A.bin"
write_payload "$expected_a" "$EXTENT_E2E_PAYLOAD_KB"
printf 'appended-A\n' >>"$expected_a"
expect_sha "A: renamed.db append matches" "$(mount_sha "$M/renamed.db")" "$(sha256_stream <"$expected_a")"
check_cmd "A: CLI sees the renamed single path" drive9 fs stat "$R/renamed.db"

echo "[7] B: extent renamed off the [extent] pattern keeps its inode"
write_payload "$M/keep.db" "$EXTENT_E2E_PAYLOAD_KB"
expect_layout "B: keep.db is extent" "$R/keep.db" "extent"
keep_ino="$(extent_ino_of "$R/keep.db")"
check_cmd "B: extent file reports X-Dat9-Extent-Ino" test -n "$keep_ino"
keep_sha="$(mount_sha "$M/keep.db")"
mv "$M/keep.db" "$M/keep.txt"
expect_layout "B: keep.txt stays extent after rename off *.db" "$R/keep.txt" "extent"
expect_extent_ino "B: keep.txt keeps the same extent_ino" "$R/keep.txt" "$keep_ino"
expect_sha "B: keep.txt bytes survive the rename" "$(mount_sha "$M/keep.txt")" "$keep_sha"

echo "[8] C: mixed directory (extent + single + subdir) ls / rename / rm -r"
mkdir -p "$M/mix/sub"
write_payload "$M/mix/a.db" "$EXTENT_E2E_PAYLOAD_KB"
write_payload "$M/mix/b.txt" "$EXTENT_E2E_PAYLOAD_KB"
write_payload "$M/mix/sub/c.db" "$EXTENT_E2E_PAYLOAD_KB"
write_payload "$M/mix/sub/d.log" "$EXTENT_E2E_PAYLOAD_KB"
expect_layout "C: mix/a.db is extent" "$R/mix/a.db" "extent"
expect_layout "C: mix/b.txt is single" "$R/mix/b.txt" "single"
expect_layout "C: mix/sub/c.db is extent" "$R/mix/sub/c.db" "extent"
expect_layout "C: mix/sub/d.log is single" "$R/mix/sub/d.log" "single"
mix_a_ino="$(extent_ino_of "$R/mix/a.db")"
mix_c_ino="$(extent_ino_of "$R/mix/sub/c.db")"
mix_a_sha="$(mount_sha "$M/mix/a.db")"
mix_b_sha="$(mount_sha "$M/mix/b.txt")"
mix_c_sha="$(mount_sha "$M/mix/sub/c.db")"
mix_d_sha="$(mount_sha "$M/mix/sub/d.log")"
check_eq "C: ls mix lists extent and single children" \
  "$(cd "$M/mix" && ls -1 | sort | tr '\n' ',')" \
  "a.db,b.txt,sub,"
mv "$M/mix" "$M/mix2"
check_cmd_fail "C: old directory name is gone after rename" test -e "$M/mix"
check_eq "C: ls mix2 lists every child after the directory rename" \
  "$(cd "$M/mix2" && ls -1 | sort | tr '\n' ',')" \
  "a.db,b.txt,sub,"
expect_layout "C: mix2/a.db is still extent" "$R/mix2/a.db" "extent"
expect_extent_ino "C: mix2/a.db keeps its extent_ino" "$R/mix2/a.db" "$mix_a_ino"
expect_layout "C: mix2/b.txt is still single" "$R/mix2/b.txt" "single"
expect_layout "C: mix2/sub/c.db is still extent" "$R/mix2/sub/c.db" "extent"
expect_extent_ino "C: mix2/sub/c.db keeps its extent_ino" "$R/mix2/sub/c.db" "$mix_c_ino"
expect_sha "C: mix2/a.db bytes survive" "$(mount_sha "$M/mix2/a.db")" "$mix_a_sha"
expect_sha "C: mix2/b.txt bytes survive" "$(mount_sha "$M/mix2/b.txt")" "$mix_b_sha"
expect_sha "C: mix2/sub/c.db bytes survive" "$(mount_sha "$M/mix2/sub/c.db")" "$mix_c_sha"
expect_sha "C: mix2/sub/d.log bytes survive" "$(mount_sha "$M/mix2/sub/d.log")" "$mix_d_sha"
append_payload "$M/mix2/a.db" "C"
mix_a_sha_appended="$(mount_sha "$M/mix2/a.db")"
check_ne "C: extent child write after the directory rename changes bytes" "$mix_a_sha_appended" "$mix_a_sha"
check_cmd "C: CLI sees mix2/a.db" drive9 fs stat "$R/mix2/a.db"

echo "[9] D: unlink + create of the same extent path allocates a new extent_ino"
write_payload "$M/churn.db" "$EXTENT_E2E_PAYLOAD_KB"
churn_ino_first="$(extent_ino_of "$R/churn.db")"
check_cmd "D: churn.db starts as an extent file" test -n "$churn_ino_first"
rm "$M/churn.db"
check_cmd_fail "D: churn.db is gone after unlink" test -e "$M/churn.db"
write_payload "$M/churn.db" "$((EXTENT_E2E_PAYLOAD_KB + 1))"
churn_ino_second="$(extent_ino_of "$R/churn.db")"
expect_layout "D: re-created churn.db is extent again" "$R/churn.db" "extent"
check_cmd "D: re-created churn.db has an extent_ino" test -n "$churn_ino_second"
check_ne "D: re-created churn.db gets a NEW extent_ino" "$churn_ino_second" "$churn_ino_first"

echo "[10] E: extent -> extent file rename keeps the inode"
mv "$M/churn.db" "$M/churn2.db"
expect_layout "E: churn2.db is extent" "$R/churn2.db" "extent"
expect_extent_ino "E: churn2.db keeps the inode across the rename" "$R/churn2.db" "$churn_ino_second"

echo "[11] F: directory renamed by the CLI, then used from a fresh mount"
mkdir -p "$M/cli-mix/sub"
write_payload "$M/cli-mix/a.db" "$EXTENT_E2E_PAYLOAD_KB"
write_payload "$M/cli-mix/sub/c.db" "$EXTENT_E2E_PAYLOAD_KB"
write_payload "$M/cli-mix/b.txt" "$EXTENT_E2E_PAYLOAD_KB"
cli_a_ino="$(extent_ino_of "$R/cli-mix/a.db")"
cli_a_sha="$(mount_sha "$M/cli-mix/a.db")"
check_cmd "F: cli-mix/a.db is an extent file" test -n "$cli_a_ino"

echo "[12] G: tree removed by the CLI, then mkdir + rmdir from a fresh mount"
mkdir -p "$M/cli-rm/sub"
write_payload "$M/cli-rm/a.db" "$EXTENT_E2E_PAYLOAD_KB"
write_payload "$M/cli-rm/sub/c.db" "$EXTENT_E2E_PAYLOAD_KB"

echo "[13] unmount, then mutate the tree through the CLI"
if unmount_mount; then
  check_eq "unmount for CLI-side mutations" "true" "true"
else
  check_eq "unmount for CLI-side mutations" "false" "true"
fi
drive9_retry fs mv "$R/cli-mix" "$R/cli-mix2" >/dev/null
check_cmd "F: CLI sees the renamed directory" drive9 fs stat "$R/cli-mix2"
drive9_retry fs rm -r "$R/cli-rm" >/dev/null
check_cmd_fail "G: CLI removed the tree" drive9 fs stat "$R/cli-rm"

echo "[14] remount and re-verify every artifact"
if start_mount; then
  check_eq "remount is ready" "true" "true"
else
  check_eq "remount is ready" "false" "true"
fi
if ! is_mounted "$MOUNT_POINT"; then
  echo "remount failed; see $MOUNT_LOG" >&2
  exit 1
fi
expect_layout "A: renamed.db is still single after remount" "$R/renamed.db" "single"
expect_layout "B: keep.txt is still extent after remount" "$R/keep.txt" "extent"
expect_extent_ino "B: keep.txt keeps its extent_ino after remount" "$R/keep.txt" "$keep_ino"
expect_sha "B: keep.txt bytes survive the remount" "$(mount_sha "$M/keep.txt")" "$keep_sha"
expect_layout "C: mix2/a.db is still extent after remount" "$R/mix2/a.db" "extent"
expect_extent_ino "C: mix2/a.db keeps its extent_ino after remount" "$R/mix2/a.db" "$mix_a_ino"
expect_layout "C: mix2/b.txt is still single after remount" "$R/mix2/b.txt" "single"
expect_extent_ino "C: mix2/sub/c.db keeps its extent_ino after remount" "$R/mix2/sub/c.db" "$mix_c_ino"
expect_sha "C: mix2/a.db keeps the post-rename write across the remount" \
  "$(mount_sha "$M/mix2/a.db")" "$mix_a_sha_appended"
expect_sha "C: CLI reads mix2/a.db with the same bytes" "$(remote_sha "$R/mix2/a.db")" "$mix_a_sha_appended"
expect_layout "E: churn2.db is still extent after remount" "$R/churn2.db" "extent"
expect_extent_ino "E: churn2.db keeps its extent_ino after remount" "$R/churn2.db" "$churn_ino_second"
check_eq "F: ls cli-mix2 lists every child after a CLI directory rename" \
  "$(cd "$M/cli-mix2" && ls -1 | sort | tr '\n' ',')" \
  "a.db,b.txt,sub,"
expect_layout "F: cli-mix2/a.db is still extent" "$R/cli-mix2/a.db" "extent"
expect_extent_ino "F: cli-mix2/a.db keeps its extent_ino" "$R/cli-mix2/a.db" "$cli_a_ino"
expect_sha "F: cli-mix2/a.db bytes survive the CLI directory rename" "$(mount_sha "$M/cli-mix2/a.db")" "$cli_a_sha"
append_payload "$M/cli-mix2/a.db" "F"
check_cmd "F: cli-mix2/a.db is writable after the CLI directory rename" test -s "$M/cli-mix2/a.db"
rm "$M/cli-mix2/a.db"
check_cmd_fail "F: cli-mix2/a.db is removable after the CLI directory rename" test -e "$M/cli-mix2/a.db"

check_cmd_fail "G: the CLI-removed tree is still absent" test -e "$M/cli-rm"
mkdir "$M/cli-rm"
check_cmd "G: mkdir of the CLI-removed path succeeds" test -d "$M/cli-rm"
check_cmd "G: rmdir of the re-created empty directory succeeds (no orphan jfs dir edge)" rmdir "$M/cli-rm"
if [ "$EXTENT_E2E_SQL_ORPHAN_CHECK" = "1" ]; then
  orphan_sql="$(jq -nc --arg q "SELECT COUNT(*) AS n FROM jfs_edge WHERE name = _binary'cli-rm'" '{query:$q}')"
  orphan_resp=$(curl -sS -X POST -H "Authorization: Bearer $API_KEY" -H "Content-Type: application/json" \
    --data-binary "$orphan_sql" "$BASE/v1/sql" 2>/dev/null || printf '[]')
  check_eq "G: jfs_edge has no leftover cli-rm edge" \
    "$(printf '%s' "$orphan_resp" | jq -r '.[0].n // "error"')" "0"
fi

echo "[15] in-mount rm -r of the mixed tree, then reuse the path"
rm -rf "$M/mix2"
check_cmd_fail "C: in-mount rm -r removed the mixed tree" test -e "$M/mix2"
check_cmd_fail "C: extent child is gone after in-mount rm -r" drive9 fs stat "$R/mix2/a.db"
mkdir "$M/mix2"
check_cmd "C: the path is reusable after in-mount rm -r" test -d "$M/mix2"
check_cmd "C: the re-created directory can be removed" rmdir "$M/mix2"

echo
echo "PASS=$PASS FAIL=$FAIL TOTAL=$TOTAL"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
