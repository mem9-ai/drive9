#!/usr/bin/env bash
# Deterministic Drive9 FUSE SQLite correctness workload.
#
# This script mounts a fresh writable namespace through real FUSE, runs SQLite
# workloads in rollback-journal mode by default, verifies PRAGMA integrity_check,
# unmounts/remounts, and verifies the remote snapshot. Set RUN_FUSE_SQLITE_WAL=1
# to add the current WAL detector. It intentionally avoids concurrent
# readers/writers, performance baselines, and crash/recovery checks; those are
# separate workload classes.

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
FUSE_SQLITE_KEEP_ARTIFACTS="${FUSE_SQLITE_KEEP_ARTIFACTS:-0}"
FUSE_SQLITE_ROWS="${FUSE_SQLITE_ROWS:-64}"
RUN_FUSE_SQLITE_WAL="${RUN_FUSE_SQLITE_WAL:-0}"
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
    echo "=== drive9 sqlite mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "root_remote=$ROOT_REMOTE"
  } >>"$MOUNT_LOG"
  drive9 mount "$MOUNT_POINT" >>"$MOUNT_LOG" 2>&1 &
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
    drive9 umount --timeout "$FUSE_UMOUNT_TIMEOUT" "$MOUNT_POINT"
  fi
  wait_mount_state unmounted
  if [ -n "${MOUNT_PID:-}" ]; then
    set +e
    wait "$MOUNT_PID" >/dev/null 2>&1
    MOUNT_PID=""
    set -e
  fi
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

run_sqlite_workload() {
  python3 - "$WORK_MOUNT" "$EXPECTED_JSON" "$FUSE_SQLITE_ROWS" "$RUN_FUSE_SQLITE_WAL" <<'PY'
import hashlib
import json
import os
import shutil
import sqlite3
import sys

root = os.path.abspath(sys.argv[1])
expected_path = sys.argv[2]
rows = int(sys.argv[3])
run_wal = sys.argv[4] == "1"


def payload(kind, index):
    seed = f"drive9-sqlite kind={kind} index={index}\n".encode()
    body = bytearray()
    for counter in range(8):
        body.extend(hashlib.sha256(seed + counter.to_bytes(4, "big")).digest())
    return bytes(body)


def checksum(data):
    return hashlib.sha256(data).hexdigest()


def connect(path):
    conn = sqlite3.connect(path, timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA mmap_size=0")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")
    return conn


def scalar(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()[0]


def fingerprint(conn):
    integrity = scalar(conn, "PRAGMA integrity_check")
    if integrity != "ok":
        raise AssertionError(f"integrity_check={integrity}")
    rows = list(conn.execute("SELECT checksum, LENGTH(payload) FROM items ORDER BY id"))
    digest = hashlib.sha256()
    payload_bytes = 0
    for item_checksum, item_size in rows:
        digest.update(item_checksum.encode())
        digest.update(b"\n")
        payload_bytes += item_size
    rolled_back = scalar(conn, "SELECT COUNT(*) FROM items WHERE bucket='rolled_back'")
    return {
        "count": len(rows),
        "payload_bytes": payload_bytes,
        "checksums_digest": digest.hexdigest(),
        "rolled_back_rows": rolled_back,
    }


def create_schema(conn):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS items "
        "(id INTEGER PRIMARY KEY, bucket TEXT NOT NULL, payload BLOB NOT NULL, checksum TEXT NOT NULL)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_bucket ON items(bucket)")


def build_wal_db(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = connect(path)
    journal = scalar(conn, "PRAGMA journal_mode=WAL").lower()
    if journal != "wal":
        raise AssertionError(f"wal journal_mode={journal}")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    create_schema(conn)
    with conn:
        for index in range(rows):
            data = payload("wal-insert", index)
            conn.execute(
                "INSERT INTO items(id, bucket, payload, checksum) VALUES (?, ?, ?, ?)",
                (index + 1, "wal", data, checksum(data)),
            )
    with conn:
        for index in range(0, rows, 3):
            data = payload("wal-update", index)
            conn.execute(
                "UPDATE items SET bucket=?, payload=?, checksum=? WHERE id=?",
                ("wal-updated", data, checksum(data), index + 1),
            )
    expected = fingerprint(conn)
    expected["journal_mode"] = journal
    conn.close()
    return expected


def build_rollback_db(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = connect(path)
    journal = scalar(conn, "PRAGMA journal_mode=DELETE").lower()
    if journal != "delete":
        raise AssertionError(f"rollback journal_mode={journal}")
    conn.execute("PRAGMA synchronous=FULL")
    create_schema(conn)
    with conn:
        for index in range(rows):
            data = payload("rollback-insert", index)
            conn.execute(
                "INSERT INTO items(id, bucket, payload, checksum) VALUES (?, ?, ?, ?)",
                (index + 1, "rollback", data, checksum(data)),
            )
    conn.execute("BEGIN IMMEDIATE")
    data = payload("rolled-back", 0)
    conn.execute(
        "INSERT INTO items(id, bucket, payload, checksum) VALUES (?, ?, ?, ?)",
        (rows + 1, "rolled_back", data, checksum(data)),
    )
    conn.rollback()
    with conn:
        for index in range(1, rows, 4):
            data = payload("rollback-update", index)
            conn.execute(
                "UPDATE items SET bucket=?, payload=?, checksum=? WHERE id=?",
                ("rollback-updated", data, checksum(data), index + 1),
            )
    expected = fingerprint(conn)
    expected["journal_mode"] = journal
    conn.close()
    return expected


shutil.rmtree(root, ignore_errors=True)
os.makedirs(root, exist_ok=True)
expected = {
    "rollback": build_rollback_db(os.path.join(root, "rollback", "workload.db")),
}
if run_wal:
    expected["wal"] = build_wal_db(os.path.join(root, "wal", "workload.db"))
with open(expected_path, "w", encoding="utf-8") as handle:
    json.dump(expected, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

verify_sqlite_tree() {
  local desc="$1"
  local root="$2"
  local actual_path="$3"
  TOTAL=$((TOTAL + 1))
  if python3 - "$root" "$EXPECTED_JSON" "$actual_path" <<'PY'
import hashlib
import json
import os
import sqlite3
import sys

root = os.path.abspath(sys.argv[1])
expected_path = sys.argv[2]
actual_path = sys.argv[3]


def connect(path):
    conn = sqlite3.connect(path, timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA mmap_size=0")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")
    return conn


def scalar(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()[0]


def fingerprint(path, expected_journal):
    if not os.path.exists(path):
        raise AssertionError(f"missing sqlite db: {path}")
    conn = connect(path)
    integrity = scalar(conn, "PRAGMA integrity_check")
    if integrity != "ok":
        raise AssertionError(f"{path}: integrity_check={integrity}")
    journal = scalar(conn, "PRAGMA journal_mode").lower()
    if journal != expected_journal:
        raise AssertionError(f"{path}: journal_mode={journal} want={expected_journal}")
    rows = list(conn.execute("SELECT checksum, LENGTH(payload) FROM items ORDER BY id"))
    digest = hashlib.sha256()
    payload_bytes = 0
    for item_checksum, item_size in rows:
        digest.update(item_checksum.encode())
        digest.update(b"\n")
        payload_bytes += item_size
    rolled_back = scalar(conn, "SELECT COUNT(*) FROM items WHERE bucket='rolled_back'")
    conn.close()
    return {
        "count": len(rows),
        "payload_bytes": payload_bytes,
        "checksums_digest": digest.hexdigest(),
        "rolled_back_rows": rolled_back,
        "journal_mode": journal,
    }

with open(expected_path, encoding="utf-8") as handle:
    expected = json.load(handle)
actual = {
    name: fingerprint(os.path.join(root, name, "workload.db"), want["journal_mode"])
    for name, want in sorted(expected.items())
}
with open(actual_path, "w", encoding="utf-8") as handle:
    json.dump(actual, handle, indent=2, sort_keys=True)
    handle.write("\n")
if actual != expected:
    print("sqlite fingerprint mismatch", file=sys.stderr)
    print("expected=", json.dumps(expected, indent=2, sort_keys=True), file=sys.stderr)
    print("actual=", json.dumps(actual, indent=2, sort_keys=True), file=sys.stderr)
    raise SystemExit(1)
PY
  then
    echo "PASS $desc"
    PASS=$((PASS + 1))
  else
    echo "FAIL $desc"
    FAIL=$((FAIL + 1))
  fi
}

remote_snapshot_root() {
  if [ -d "$REMOTE_SNAPSHOT/sqlite" ]; then
    printf '%s' "$REMOTE_SNAPSHOT/sqlite"
    return
  fi
  printf '%s' "$REMOTE_SNAPSHOT"
}

echo "=== drive9 FUSE SQLite correctness ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "FUSE_STRICT_PREREQS=$FUSE_STRICT_PREREQS"
echo "FUSE_SQLITE_ROWS=$FUSE_SQLITE_ROWS"
echo "RUN_FUSE_SQLITE_WAL=$RUN_FUSE_SQLITE_WAL"

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

if ! [[ "$FUSE_SQLITE_ROWS" =~ ^[0-9]+$ ]] || [ "$FUSE_SQLITE_ROWS" -lt 4 ]; then
  echo "invalid FUSE_SQLITE_ROWS: must be >= 4" >&2
  exit 1
fi
case "$RUN_FUSE_SQLITE_WAL" in
  0|1) ;;
  *)
    echo "invalid RUN_FUSE_SQLITE_WAL: expected 0 or 1" >&2
    exit 1
    ;;
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

drive9() {
  DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

TS="$(date +%s)"
RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-fuse-sqlite-${TS}.XXXXXX")"
RUN_ID="$(basename "$RUN_ROOT")"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
EXPECTED_JSON="$RUN_ROOT/expected-sqlite.json"
ACTUAL_MOUNT_JSON="$RUN_ROOT/actual-mounted-sqlite.json"
ACTUAL_REMOUNT_JSON="$RUN_ROOT/actual-remounted-sqlite.json"
ACTUAL_REMOTE_JSON="$RUN_ROOT/actual-remote-sqlite.json"
REMOTE_SNAPSHOT="$RUN_ROOT/remote-snapshot"
ROOT_REL="$RUN_ID"
ROOT_REMOTE="/$ROOT_REL"
WORK_MOUNT="$MOUNT_POINT/$ROOT_REL/sqlite"
WORK_REMOTE="$ROOT_REMOTE/sqlite"
MOUNT_PID=""

mkdir -p "$MOUNT_POINT"
: > "$MOUNT_LOG"

cleanup() {
  local rc=$?
  stop_mount
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$FUSE_SQLITE_KEEP_ARTIFACTS" != "1" ]; then
    rm -rf "$RUN_ROOT"
  else
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount log: $MOUNT_LOG"
    echo "Expected SQLite manifest: $EXPECTED_JSON"
    echo "Mounted SQLite manifest: $ACTUAL_MOUNT_JSON"
    echo "Remounted SQLite manifest: $ACTUAL_REMOUNT_JSON"
    echo "Remote SQLite manifest: $ACTUAL_REMOTE_JSON"
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "[4] create remote root"
drive9_retry fs mkdir "$ROOT_REMOTE" >/dev/null
check_eq "remote sqlite root" "$ROOT_REMOTE" "$ROOT_REMOTE"

echo "[5] mount writable namespace"
if start_mount; then
  check_eq "sqlite mount is mounted" "true" "true"
  if ls "$MOUNT_POINT" >/dev/null 2>&1; then
    check_eq "mount root ls precheck" "true" "true"
  else
    skip_or_fail "mount root ls precheck failed"
  fi
else
  check_eq "sqlite mount is mounted" "false" "true"
fi

if is_mounted "$MOUNT_POINT"; then
  echo "[6] run mounted SQLite workload"
  sqlite_workload_ok=0
  if run_sqlite_workload; then
    check_eq "mounted SQLite workload creates deterministic databases" "true" "true"
    sqlite_workload_ok=1
  else
    check_eq "mounted SQLite workload creates deterministic databases" "false" "true"
  fi

  if [ "$sqlite_workload_ok" = "1" ]; then
    verify_sqlite_tree "mounted SQLite integrity_check and logical fingerprint match" "$WORK_MOUNT" "$ACTUAL_MOUNT_JSON"

    echo "[7] unmount and remount SQLite workload"
    if unmount_mount; then
      check_eq "unmount SQLite mount" "true" "true"
      MOUNT_PID=""
      if start_mount; then
        check_eq "sqlite mount remounted" "true" "true"
        verify_sqlite_tree "remounted SQLite integrity_check and logical fingerprint match" "$WORK_MOUNT" "$ACTUAL_REMOUNT_JSON"
      else
        check_eq "sqlite mount remounted" "false" "true"
      fi
    else
      check_eq "unmount SQLite mount" "false" "true"
    fi

    echo "[8] unmount and verify remote snapshot"
    if is_mounted "$MOUNT_POINT"; then
      if unmount_mount; then
        check_eq "unmount SQLite remount" "true" "true"
        MOUNT_PID=""
        mkdir -p "$REMOTE_SNAPSHOT"
        drive9_retry fs cp ":$WORK_REMOTE" "$REMOTE_SNAPSHOT" >/dev/null
        REMOTE_WORK_SNAPSHOT="$(remote_snapshot_root)"
        verify_sqlite_tree "remote SQLite snapshot integrity_check and logical fingerprint match" "$REMOTE_WORK_SNAPSHOT" "$ACTUAL_REMOTE_JSON"
      else
        check_eq "unmount SQLite remount" "false" "true"
      fi
    else
      check_eq "remote snapshot requires successful remount" "false" "true"
    fi
  fi
fi

echo "[9] cleanup remote fixture"
drive9_retry fs rm -r "$ROOT_REMOTE" >/dev/null || true

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
