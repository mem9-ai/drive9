#!/usr/bin/env bash
# Sequential SQLite WAL commit correctness on a generic Drive9 FUSE mount.
#
# This is the non-S3-Express counterpart to fuse-s3-express-append-log.sh:
# N independent COMMIT transactions with journal_mode=WAL and synchronous=FULL,
# then same-mount reopen, remount, and remote-snapshot fingerprint checks.
# It does not require append-log or Directory Buckets. Honor FUSE_PROFILE so
# the same script covers the default FS and coding-agent-extent.

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
FUSE_SQLITE_COMMIT_KEEP_ARTIFACTS="${FUSE_SQLITE_COMMIT_KEEP_ARTIFACTS:-0}"
FUSE_SQLITE_COMMITS="${FUSE_SQLITE_COMMITS:-1000}"
FUSE_SQLITE_WAL_AUTOCHECKPOINT="${FUSE_SQLITE_WAL_AUTOCHECKPOINT:-100}"
FUSE_SQLITE_COMMIT_TIMEOUT_S="${FUSE_SQLITE_COMMIT_TIMEOUT_S:-600}"
FUSE_SQLITE_COMMIT_DURABILITY="${FUSE_SQLITE_COMMIT_DURABILITY:-fsync}"
FUSE_SQLITE_COMMIT_DEBUG="${FUSE_SQLITE_COMMIT_DEBUG:-0}"
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

require_python_sqlite() {
  TOTAL=$((TOTAL + 1))
  if python3 - <<'PY'
import sqlite3
raise SystemExit(0)
PY
  then
    echo "PASS python3 sqlite3 module is available"
    PASS=$((PASS + 1))
    return 0
  fi
  echo "FAIL python3 sqlite3 module is available" >&2
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

start_mount() {
  local mount_args=(mount --mode=fuse --durability "$FUSE_SQLITE_COMMIT_DURABILITY")
  if [ "$FUSE_SQLITE_COMMIT_DEBUG" = "1" ]; then
    mount_args+=(--debug)
  fi
  if [ -n "${FUSE_PROFILE:-}" ]; then
    mount_args+=(--profile "$FUSE_PROFILE")
  fi
  mount_args+=(":$ROOT_REMOTE" "$MOUNT_POINT")
  {
    echo "=== drive9 sqlite commit-sequence mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "root_remote=$ROOT_REMOTE"
    echo "fuse_profile=${FUSE_PROFILE:-}"
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

run_sqlite_commits() {
  local timeout_cmd=()
  if [ "$FUSE_SQLITE_COMMIT_TIMEOUT_S" != "0" ] && command -v timeout >/dev/null 2>&1; then
    timeout_cmd=(timeout --kill-after=10s "${FUSE_SQLITE_COMMIT_TIMEOUT_S}s")
  fi
  "${timeout_cmd[@]}" python3 - "$DB_MOUNT" "$EXPECTED_JSON" "$FUSE_SQLITE_COMMITS" "$FUSE_SQLITE_WAL_AUTOCHECKPOINT" <<'PY'
import hashlib
import json
import os
import sqlite3
import sys

database, expected_path, transactions, autocheckpoint = sys.argv[1:]
transactions = int(transactions)
autocheckpoint = int(autocheckpoint)
os.makedirs(os.path.dirname(database), exist_ok=True)
conn = sqlite3.connect(database, isolation_level=None, timeout=30.0)
conn.execute("PRAGMA busy_timeout=30000")
journal = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
if journal.lower() != "wal":
    raise SystemExit(f"journal_mode={journal} want=wal")
conn.execute("PRAGMA synchronous=FULL")
conn.execute(f"PRAGMA wal_autocheckpoint={autocheckpoint}")
conn.execute("CREATE TABLE IF NOT EXISTS entries (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
max_wal_size = 0
for index in range(1, transactions + 1):
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("INSERT INTO entries(id, value) VALUES (?, ?)", (index, f"value-{index:04d}"))
    conn.execute("COMMIT")
    wal_path = database + "-wal"
    if os.path.exists(wal_path):
        max_wal_size = max(max_wal_size, os.path.getsize(wal_path))
if max_wal_size <= 0:
    raise SystemExit("WAL never grew")
rows = list(conn.execute("SELECT id, value FROM entries ORDER BY id"))
if len(rows) != transactions:
    raise SystemExit(f"row count={len(rows)} want={transactions}")
for index, (row_id, value) in enumerate(rows, start=1):
    if row_id != index or value != f"value-{index:04d}":
        raise SystemExit(f"row mismatch id={row_id} value={value!r} want=({index}, value-{index:04d})")
fingerprint = hashlib.sha256(repr(rows).encode()).hexdigest()
check = conn.execute("PRAGMA integrity_check").fetchone()[0]
if check != "ok":
    raise SystemExit(f"integrity_check={check}")
conn.close()
with open(expected_path, "w", encoding="utf-8") as handle:
    json.dump(
        {
            "count": len(rows),
            "fingerprint": fingerprint,
            "journal_mode": journal.lower(),
            "max_wal_size": max_wal_size,
            "wal_autocheckpoint": autocheckpoint,
        },
        handle,
        indent=2,
        sort_keys=True,
    )
    handle.write("\n")
PY
}

verify_sqlite_db() {
  local desc="$1"
  local database="$2"
  TOTAL=$((TOTAL + 1))
  if python3 - "$database" "$EXPECTED_JSON" <<'PY'
import hashlib
import json
import sqlite3
import sys

database, expected_path = sys.argv[1:]
with open(expected_path, encoding="utf-8") as handle:
    want = json.load(handle)
conn = sqlite3.connect(database, timeout=30.0)
conn.execute("PRAGMA busy_timeout=30000")
rows = list(conn.execute("SELECT id, value FROM entries ORDER BY id"))
fingerprint = hashlib.sha256(repr(rows).encode()).hexdigest()
check = conn.execute("PRAGMA integrity_check").fetchone()[0]
conn.close()
if len(rows) != want["count"]:
    raise SystemExit(f"row count={len(rows)} want={want['count']}")
if fingerprint != want["fingerprint"]:
    raise SystemExit("logical fingerprint mismatch")
if check != "ok":
    raise SystemExit(f"integrity_check={check}")
for index, (row_id, value) in enumerate(rows, start=1):
    if row_id != index or value != f"value-{index:04d}":
        raise SystemExit(f"row mismatch id={row_id} value={value!r}")
PY
  then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc"
    FAIL=$((FAIL + 1))
  fi
}

if ! [[ "$FUSE_SQLITE_COMMITS" =~ ^[1-9][0-9]*$ ]]; then
  echo "invalid FUSE_SQLITE_COMMITS: must be >= 1" >&2
  exit 1
fi
if ! [[ "$FUSE_SQLITE_WAL_AUTOCHECKPOINT" =~ ^[0-9]+$ ]]; then
  echo "invalid FUSE_SQLITE_WAL_AUTOCHECKPOINT: must be >= 0" >&2
  exit 1
fi
if ! [[ "$FUSE_SQLITE_COMMIT_TIMEOUT_S" =~ ^[0-9]+$ ]]; then
  echo "invalid FUSE_SQLITE_COMMIT_TIMEOUT_S: must be >= 0" >&2
  exit 1
fi

echo "=== drive9 FUSE SQLite commit sequence ==="
echo "DRIVE9_BASE=$BASE"
echo "FUSE_PROFILE=${FUSE_PROFILE:-}"
echo "FUSE_SQLITE_COMMITS=$FUSE_SQLITE_COMMITS"
echo "FUSE_SQLITE_WAL_AUTOCHECKPOINT=$FUSE_SQLITE_WAL_AUTOCHECKPOINT"
echo "FUSE_SQLITE_COMMIT_DURABILITY=$FUSE_SQLITE_COMMIT_DURABILITY"

require_cmd curl
require_cmd jq
require_cmd python3
require_python_sqlite
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

drive9() {
  DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

TS="$(date +%s)"
RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-fuse-sqlite-commits-${TS}.XXXXXX")"
RUN_ID="$(basename "$RUN_ROOT")"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
EXPECTED_JSON="$RUN_ROOT/expected.json"
REMOTE_SNAPSHOT="$RUN_ROOT/remote-snapshot"
ROOT_REMOTE="/$RUN_ID"
DB_MOUNT="$MOUNT_POINT/seq.db"
MOUNT_PID=""

mkdir -p "$MOUNT_POINT"
: > "$MOUNT_LOG"

cleanup() {
  local rc=$?
  stop_mount
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$FUSE_SQLITE_COMMIT_KEEP_ARTIFACTS" != "1" ]; then
    rm -rf "$RUN_ROOT"
  else
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount log: $MOUNT_LOG"
    echo "Expected fingerprint: $EXPECTED_JSON"
  fi
  if [ "$FAIL" -gt 0 ] && [ "$rc" -eq 0 ]; then
    exit 1
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "[4] create remote root"
drive9_retry fs mkdir "$ROOT_REMOTE" >/dev/null
check_eq "remote commit-sequence root" "$ROOT_REMOTE" "$ROOT_REMOTE"

echo "[5] mount writable namespace"
if start_mount; then
  check_eq "commit-sequence mount is mounted" "true" "true"
else
  check_eq "commit-sequence mount is mounted" "false" "true"
fi

if is_mounted "$MOUNT_POINT"; then
  echo "[6] $FUSE_SQLITE_COMMITS WAL FULL commits"
  if run_sqlite_commits; then
    check_eq "sequential WAL commits persist expected rows" "true" "true"
    verify_sqlite_db "same-connection fingerprint already recorded" "$DB_MOUNT"

    echo "[7] same-mount reopen"
    verify_sqlite_db "same-mount reopen fingerprint matches" "$DB_MOUNT"

    echo "[8] unmount and remount"
    if unmount_mount; then
      check_eq "unmount commit-sequence mount" "true" "true"
      MOUNT_PID=""
      if start_mount; then
        check_eq "commit-sequence mount remounted" "true" "true"
        verify_sqlite_db "remounted fingerprint matches" "$DB_MOUNT"
      else
        check_eq "commit-sequence mount remounted" "false" "true"
      fi
    else
      check_eq "unmount commit-sequence mount" "false" "true"
    fi

    echo "[9] remote snapshot"
    if is_mounted "$MOUNT_POINT"; then
      if unmount_mount; then
        check_eq "unmount after remount" "true" "true"
        MOUNT_PID=""
        mkdir -p "$REMOTE_SNAPSHOT"
        drive9_retry fs cp -r ":$ROOT_REMOTE" "$REMOTE_SNAPSHOT" >/dev/null
        snapshot_db=""
        if [ -f "$REMOTE_SNAPSHOT/seq.db" ]; then
          snapshot_db="$REMOTE_SNAPSHOT/seq.db"
        elif [ -f "$REMOTE_SNAPSHOT/$RUN_ID/seq.db" ]; then
          snapshot_db="$REMOTE_SNAPSHOT/$RUN_ID/seq.db"
        else
          snapshot_db="$(find "$REMOTE_SNAPSHOT" -name seq.db -type f | head -n 1 || true)"
        fi
        if [ -n "$snapshot_db" ]; then
          verify_sqlite_db "remote snapshot fingerprint matches" "$snapshot_db"
        else
          check_eq "remote snapshot contains seq.db" "false" "true"
        fi
      else
        check_eq "unmount after remount" "false" "true"
      fi
    else
      check_eq "remote snapshot requires successful remount" "false" "true"
    fi
  else
    check_eq "sequential WAL commits persist expected rows" "false" "true"
  fi
fi

echo
echo "PASS=$PASS FAIL=$FAIL TOTAL=$TOTAL"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
