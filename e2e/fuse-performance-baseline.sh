#!/usr/bin/env bash
# Opt-in Drive9 FUSE performance baseline.
#
# This script mounts a fresh writable namespace through real FUSE and records
# threshold-free baseline metrics for small files, large files, and simple
# SQLite transactions. Correctness is still asserted; performance values are
# emitted as artifacts rather than used as pass/fail thresholds.

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
FUSE_PERF_KEEP_ARTIFACTS="${FUSE_PERF_KEEP_ARTIFACTS:-0}"
FUSE_PERF_SMALL_FILES="${FUSE_PERF_SMALL_FILES:-64}"
FUSE_PERF_SMALL_BYTES="${FUSE_PERF_SMALL_BYTES:-1024}"
FUSE_PERF_LARGE_MB="${FUSE_PERF_LARGE_MB:-16}"
FUSE_PERF_READ_PASSES="${FUSE_PERF_READ_PASSES:-2}"
FUSE_PERF_SQLITE_ROWS="${FUSE_PERF_SQLITE_ROWS:-256}"
FUSE_PERF_ARTIFACT_DIR="${FUSE_PERF_ARTIFACT_DIR:-}"
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

require_positive_int() {
  local name="$1"
  local value="$2"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || [ "$value" -lt 1 ]; then
    echo "invalid $name: must be >= 1" >&2
    exit 1
  fi
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
    echo "=== drive9 performance mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
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

jq_check() {
  jq -e "$@" >/dev/null
}

publish_artifacts() {
  if [ -z "$FUSE_PERF_ARTIFACT_DIR" ]; then
    return 0
  fi
  mkdir -p "$FUSE_PERF_ARTIFACT_DIR"
  if [ -f "$METRICS_JSON" ]; then
    cp "$METRICS_JSON" "$FUSE_PERF_ARTIFACT_DIR/performance-metrics-$RUN_ID.json"
  fi
  if [ -f "$MOUNT_LOG" ]; then
    cp "$MOUNT_LOG" "$FUSE_PERF_ARTIFACT_DIR/mount-$RUN_ID.log"
  fi
  echo "Published performance artifacts to $FUSE_PERF_ARTIFACT_DIR"
}

run_performance_workload() {
  python3 - "$WORK_MOUNT" "$METRICS_JSON" "$FUSE_PERF_SMALL_FILES" "$FUSE_PERF_SMALL_BYTES" "$FUSE_PERF_LARGE_MB" "$FUSE_PERF_READ_PASSES" "$FUSE_PERF_SQLITE_ROWS" <<'PY'
import hashlib
import json
import math
import os
import shutil
import sqlite3
import sys
import time

root = os.path.abspath(sys.argv[1])
metrics_path = sys.argv[2]
small_files = int(sys.argv[3])
small_bytes = int(sys.argv[4])
large_mb = int(sys.argv[5])
read_passes = int(sys.argv[6])
sqlite_rows = int(sys.argv[7])
large_bytes = large_mb * 1024 * 1024
chunk_size = 1024 * 1024


def deterministic_bytes(label, size):
    seed = hashlib.sha256(label.encode()).digest()
    body = bytearray()
    counter = 0
    while len(body) < size:
        body.extend(hashlib.sha256(seed + counter.to_bytes(8, "big")).digest())
        counter += 1
    return bytes(body[:size])


def iter_large_chunks(label, total_bytes):
    chunks = int(math.ceil(total_bytes / chunk_size))
    for index in range(chunks):
        size = min(chunk_size, total_bytes - index * chunk_size)
        yield deterministic_bytes(f"{label}:{index}", size)


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def fsync_parent(path):
    parent = os.path.dirname(path)
    fd = os.open(parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def rate(bytes_count, seconds):
    if seconds <= 0:
        return 0.0
    return bytes_count / seconds


def metric(seconds, bytes_count=0, files=0, rows=0, extra=None):
    out = {
        "seconds": seconds,
        "bytes": bytes_count,
        "files": files,
        "rows": rows,
        "bytes_per_second": rate(bytes_count, seconds),
        "mib_per_second": rate(bytes_count, seconds) / (1024 * 1024),
    }
    if files:
        out["files_per_second"] = files / seconds if seconds > 0 else 0.0
    if rows:
        out["rows_per_second"] = rows / seconds if seconds > 0 else 0.0
    if extra:
        out.update(extra)
    return out


def timed(call):
    start = time.perf_counter()
    result = call()
    return time.perf_counter() - start, result


def small_file_write(small_dir):
    manifest = {}
    bytes_count = 0
    os.makedirs(small_dir, exist_ok=True)
    for index in range(small_files):
        rel = f"small-{index:04d}.bin"
        path = os.path.join(small_dir, rel)
        data = deterministic_bytes(f"small:{index}", small_bytes)
        with open(path, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        manifest[rel] = hashlib.sha256(data).hexdigest()
        bytes_count += len(data)
    fsync_parent(small_dir)
    return manifest, bytes_count


def small_file_read(small_dir, manifest):
    bytes_count = 0
    for rel, want_hash in sorted(manifest.items()):
        path = os.path.join(small_dir, rel)
        got_hash = sha256_file(path)
        if got_hash != want_hash:
            raise AssertionError(f"small file hash mismatch: {rel}")
        bytes_count += os.path.getsize(path)
    return bytes_count


def large_file_write(path):
    digest = hashlib.sha256()
    bytes_count = 0
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as handle:
        for chunk in iter_large_chunks("large", large_bytes):
            handle.write(chunk)
            digest.update(chunk)
            bytes_count += len(chunk)
        handle.flush()
        os.fsync(handle.fileno())
    fsync_parent(path)
    return digest.hexdigest(), bytes_count


def large_file_read(path, want_hash):
    got_hash = sha256_file(path)
    if got_hash != want_hash:
        raise AssertionError(f"large file hash mismatch: {got_hash} != {want_hash}")
    return os.path.getsize(path)


def sqlite_workload(db_path, requested_journal_mode, label):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA mmap_size=0")
    journal_mode = conn.execute(f"PRAGMA journal_mode={requested_journal_mode.upper()}").fetchone()[0].lower()
    if journal_mode != requested_journal_mode.lower():
        raise AssertionError(f"sqlite journal_mode={journal_mode} want={requested_journal_mode.lower()}")
    conn.execute("PRAGMA synchronous=FULL")
    if journal_mode == "wal":
        conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS items "
        "(id INTEGER PRIMARY KEY, version INTEGER NOT NULL, payload BLOB NOT NULL, checksum TEXT NOT NULL)"
    )
    conn.commit()

    insert_start = time.perf_counter()
    with conn:
        for row_id in range(1, sqlite_rows + 1):
            payload = deterministic_bytes(f"{label}-insert:{row_id}", 512)
            conn.execute(
                "INSERT INTO items(id, version, payload, checksum) VALUES (?, ?, ?, ?)",
                (row_id, 1, payload, hashlib.sha256(payload).hexdigest()),
            )
    insert_seconds = time.perf_counter() - insert_start

    update_rows = 0
    update_start = time.perf_counter()
    with conn:
        for row_id in range(1, sqlite_rows + 1, 3):
            payload = deterministic_bytes(f"{label}-update:{row_id}", 512)
            conn.execute(
                "UPDATE items SET version=?, payload=?, checksum=? WHERE id=?",
                (2, payload, hashlib.sha256(payload).hexdigest(), row_id),
            )
            update_rows += 1
    update_seconds = time.perf_counter() - update_start

    read_start = time.perf_counter()
    rows = list(conn.execute("SELECT id, version, checksum, payload FROM items ORDER BY id"))
    read_seconds = time.perf_counter() - read_start
    digest = hashlib.sha256()
    payload_bytes = 0
    max_version = 0
    for row_id, version, checksum, payload in rows:
        payload_hash = hashlib.sha256(payload).hexdigest()
        if payload_hash != checksum:
            raise AssertionError(f"sqlite payload checksum mismatch row={row_id}")
        size = len(payload)
        digest.update(f"{row_id}:{version}:{checksum}:{size}\n".encode())
        payload_bytes += size
        max_version = max(max_version, version)
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        raise AssertionError(f"sqlite integrity_check={integrity}")
    checkpoint_seconds = 0.0
    checkpoint_result = None
    post_checkpoint_integrity = integrity
    if journal_mode == "wal":
        checkpoint_start = time.perf_counter()
        checkpoint_result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        checkpoint_seconds = time.perf_counter() - checkpoint_start
        if checkpoint_result is None or checkpoint_result[0] != 0:
            raise AssertionError(f"sqlite wal_checkpoint(TRUNCATE)={checkpoint_result}")
        post_checkpoint_integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if post_checkpoint_integrity != "ok":
            raise AssertionError(f"sqlite post-checkpoint integrity_check={post_checkpoint_integrity}")
    conn.close()

    return {
        "journal_mode": journal_mode,
        "insert_seconds": insert_seconds,
        "update_seconds": update_seconds,
        "read_seconds": read_seconds,
        "rows": len(rows),
        "update_rows": update_rows,
        "payload_verified_rows": len(rows),
        "max_version": max_version,
        "payload_bytes": payload_bytes,
        "fingerprint": digest.hexdigest(),
        "integrity_check": integrity,
        "checkpoint_seconds": checkpoint_seconds,
        "checkpoint_result": list(checkpoint_result) if checkpoint_result is not None else None,
        "post_checkpoint_integrity_check": post_checkpoint_integrity,
    }


shutil.rmtree(root, ignore_errors=True)
os.makedirs(root, exist_ok=True)

metrics = {
    "schema": "drive9-fuse-performance/v1",
    "generated_at_unix": time.time(),
    "params": {
        "small_files": small_files,
        "small_bytes": small_bytes,
        "large_mb": large_mb,
        "large_bytes": large_bytes,
        "read_passes": read_passes,
        "sqlite_rows": sqlite_rows,
    },
    "workloads": {},
    "correctness": {},
}

small_dir = os.path.join(root, "small-files")
seconds, small_write_result = timed(lambda: small_file_write(small_dir))
small_manifest, small_write_bytes = small_write_result
metrics["workloads"]["small_file_write"] = metric(seconds, small_write_bytes, files=small_files)

seconds, small_read_bytes = timed(lambda: small_file_read(small_dir, small_manifest))
metrics["workloads"]["small_file_read"] = metric(seconds, small_read_bytes, files=small_files)
metrics["correctness"]["small_file_manifest_sha256"] = hashlib.sha256(
    json.dumps(small_manifest, sort_keys=True).encode()
).hexdigest()

large_path = os.path.join(root, "large-file", "large.bin")
seconds, large_write_result = timed(lambda: large_file_write(large_path))
large_hash, large_write_bytes = large_write_result
metrics["workloads"]["large_file_write"] = metric(seconds, large_write_bytes, files=1)
metrics["correctness"]["large_file_sha256"] = large_hash

large_reads = []
for read_index in range(read_passes):
    seconds, large_read_bytes = timed(lambda: large_file_read(large_path, large_hash))
    large_reads.append(metric(seconds, large_read_bytes, files=1, extra={"pass": read_index + 1}))
metrics["workloads"]["large_file_reads"] = large_reads

sqlite_path = os.path.join(root, "sqlite", "perf.db")
sqlite_result = sqlite_workload(sqlite_path, "delete", "sqlite-delete")
metrics["workloads"]["sqlite_insert_transaction"] = metric(
    sqlite_result["insert_seconds"],
    rows=sqlite_result["rows"],
)
metrics["workloads"]["sqlite_update_transaction"] = metric(
    sqlite_result["update_seconds"],
    rows=sqlite_result["update_rows"],
)
metrics["workloads"]["sqlite_read_aggregate"] = metric(
    sqlite_result["read_seconds"],
    bytes_count=sqlite_result["payload_bytes"],
    rows=sqlite_result["rows"],
    extra={
        "journal_mode": sqlite_result["journal_mode"],
        "max_version": sqlite_result["max_version"],
        "payload_verified_rows": sqlite_result["payload_verified_rows"],
        "integrity_check": sqlite_result["integrity_check"],
    },
)
metrics["correctness"]["sqlite_fingerprint"] = sqlite_result["fingerprint"]

sqlite_wal_path = os.path.join(root, "sqlite-wal", "perf.db")
sqlite_wal_result = sqlite_workload(sqlite_wal_path, "wal", "sqlite-wal")
metrics["workloads"]["sqlite_wal_insert_transaction"] = metric(
    sqlite_wal_result["insert_seconds"],
    rows=sqlite_wal_result["rows"],
)
metrics["workloads"]["sqlite_wal_update_transaction"] = metric(
    sqlite_wal_result["update_seconds"],
    rows=sqlite_wal_result["update_rows"],
)
metrics["workloads"]["sqlite_wal_read_aggregate"] = metric(
    sqlite_wal_result["read_seconds"],
    bytes_count=sqlite_wal_result["payload_bytes"],
    rows=sqlite_wal_result["rows"],
    extra={
        "journal_mode": sqlite_wal_result["journal_mode"],
        "max_version": sqlite_wal_result["max_version"],
        "payload_verified_rows": sqlite_wal_result["payload_verified_rows"],
        "integrity_check": sqlite_wal_result["integrity_check"],
    },
)
metrics["workloads"]["sqlite_wal_checkpoint_truncate"] = metric(
    sqlite_wal_result["checkpoint_seconds"],
    rows=sqlite_wal_result["rows"],
    extra={
        "checkpoint_busy": sqlite_wal_result["checkpoint_result"][0],
        "checkpoint_log_frames": sqlite_wal_result["checkpoint_result"][1],
        "checkpointed_frames": sqlite_wal_result["checkpoint_result"][2],
        "integrity_check": sqlite_wal_result["post_checkpoint_integrity_check"],
    },
)
metrics["correctness"]["sqlite_wal_fingerprint"] = sqlite_wal_result["fingerprint"]

with open(metrics_path, "w", encoding="utf-8") as handle:
    json.dump(metrics, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

echo "=== drive9 FUSE performance baseline ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "FUSE_STRICT_PREREQS=$FUSE_STRICT_PREREQS"
echo "FUSE_PERF_SMALL_FILES=$FUSE_PERF_SMALL_FILES"
echo "FUSE_PERF_SMALL_BYTES=$FUSE_PERF_SMALL_BYTES"
echo "FUSE_PERF_LARGE_MB=$FUSE_PERF_LARGE_MB"
echo "FUSE_PERF_READ_PASSES=$FUSE_PERF_READ_PASSES"
echo "FUSE_PERF_SQLITE_ROWS=$FUSE_PERF_SQLITE_ROWS"

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

require_positive_int FUSE_PERF_SMALL_FILES "$FUSE_PERF_SMALL_FILES"
require_positive_int FUSE_PERF_SMALL_BYTES "$FUSE_PERF_SMALL_BYTES"
require_positive_int FUSE_PERF_LARGE_MB "$FUSE_PERF_LARGE_MB"
require_positive_int FUSE_PERF_READ_PASSES "$FUSE_PERF_READ_PASSES"
require_positive_int FUSE_PERF_SQLITE_ROWS "$FUSE_PERF_SQLITE_ROWS"

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
RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-fuse-perf-${TS}.XXXXXX")"
RUN_ID="$(basename "$RUN_ROOT")"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
METRICS_JSON="$RUN_ROOT/performance-metrics.json"
ROOT_REL="$RUN_ID"
ROOT_REMOTE="/$ROOT_REL"
WORK_MOUNT="$MOUNT_POINT/$ROOT_REL/perf"
MOUNT_PID=""

mkdir -p "$MOUNT_POINT"
: > "$MOUNT_LOG"

cleanup() {
  local rc=$?
  stop_mount
  publish_artifacts || true
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$FUSE_PERF_KEEP_ARTIFACTS" != "1" ]; then
    rm -rf "$RUN_ROOT"
  else
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount log: $MOUNT_LOG"
    echo "Performance metrics: $METRICS_JSON"
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "[4] create remote root"
drive9_retry fs mkdir "$ROOT_REMOTE" >/dev/null
check_eq "remote performance root" "$ROOT_REMOTE" "$ROOT_REMOTE"

echo "[5] mount writable namespace"
if start_mount; then
  check_eq "performance mount is mounted" "true" "true"
  if ls "$MOUNT_POINT" >/dev/null 2>&1; then
    check_eq "mount root ls precheck" "true" "true"
  else
    skip_or_fail "mount root ls precheck failed"
  fi
else
  check_eq "performance mount is mounted" "false" "true"
fi

if is_mounted "$MOUNT_POINT"; then
  echo "[6] run mounted performance baseline"
  if run_performance_workload; then
    check_eq "mounted performance workload completes" "true" "true"
  else
    check_eq "mounted performance workload completes" "false" "true"
  fi

  if [ -f "$METRICS_JSON" ]; then
    check_cmd "performance metrics json is valid" jq_check '.schema == "drive9-fuse-performance/v1"' "$METRICS_JSON"
    check_cmd "performance metrics include sqlite rows" jq_check --argjson rows "$FUSE_PERF_SQLITE_ROWS" '.workloads.sqlite_read_aggregate.rows == $rows' "$METRICS_JSON"
    check_cmd "performance metrics verify sqlite payload rows" jq_check --argjson rows "$FUSE_PERF_SQLITE_ROWS" '.workloads.sqlite_read_aggregate.payload_verified_rows == $rows' "$METRICS_JSON"
    check_cmd "performance metrics include sqlite WAL rows" jq_check --argjson rows "$FUSE_PERF_SQLITE_ROWS" '.workloads.sqlite_wal_read_aggregate.rows == $rows' "$METRICS_JSON"
    check_cmd "performance metrics verify sqlite WAL payload rows" jq_check --argjson rows "$FUSE_PERF_SQLITE_ROWS" '.workloads.sqlite_wal_read_aggregate.payload_verified_rows == $rows' "$METRICS_JSON"
    check_cmd "performance metrics include sqlite WAL checkpoint" jq_check '.workloads.sqlite_wal_checkpoint_truncate.integrity_check == "ok" and .workloads.sqlite_wal_checkpoint_truncate.checkpoint_busy == 0' "$METRICS_JSON"
    check_cmd "performance metrics include large read passes" jq_check --argjson passes "$FUSE_PERF_READ_PASSES" '.workloads.large_file_reads | length == $passes' "$METRICS_JSON"
    echo "Performance metrics artifact: $METRICS_JSON"
    cat "$METRICS_JSON"
  else
    check_eq "performance metrics json exists" "false" "true"
  fi

  echo "[7] unmount performance workload"
  if unmount_mount; then
    check_eq "unmount performance mount" "true" "true"
    MOUNT_PID=""
  else
    check_eq "unmount performance mount" "false" "true"
  fi
fi

echo "[8] cleanup remote fixture"
drive9_retry fs rm -r "$ROOT_REMOTE" >/dev/null || true

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
