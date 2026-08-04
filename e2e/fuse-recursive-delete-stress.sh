#!/usr/bin/env bash
# Deterministic Drive9 FUSE recursive-delete stress workload.
#
# Covers the batched recursive directory delete redesign
# (docs/design/recursive-delete-batched-design.md, server flag
# DRIVE9_RECURSIVE_DELETE_BATCHED):
#   a. rm -r of a large tree (default 2000 files / 50 subdirs) built through a
#      real FUSE mount while a background writer keeps writing a sibling tree;
#      asserts rm -r succeeds, reports no lock-wait/deadlock errors, and the
#      sibling data stays intact (exact sha256 manifest, mount + remote).
#   b. rm -r killed (SIGKILL) mid-sweep, then retried; asserts the retry
#      resumes the server-side sweep and the tree converges to fully removed.
#   c. mkdir at the same path racing an in-flight rm -r, then a new tree
#      written at the recreated path; asserts the new tree is complete and no
#      old content survives (path-recreation generation case).
#
# Deletes go through `drive9 fs rm -r` (DELETE /v1/fs/{path}?recursive), which
# is the entry point of the batched server-side sweep; fixture trees and
# sibling writes go through the FUSE mount. To exercise the batched
# implementation the server must run with DRIVE9_RECURSIVE_DELETE_BATCHED=1;
# against a legacy server this suite is the regression detector for the
# single-transaction failure modes (lock-wait timeouts on large trees).

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
FUSE_RDEL_KEEP_ARTIFACTS="${FUSE_RDEL_KEEP_ARTIFACTS:-0}"
FUSE_RDEL_ARTIFACT_DIR="${FUSE_RDEL_ARTIFACT_DIR:-}"
FUSE_RDEL_FILES="${FUSE_RDEL_FILES:-2000}"
FUSE_RDEL_SUBDIRS="${FUSE_RDEL_SUBDIRS:-50}"
FUSE_RDEL_PAYLOAD_KB="${FUSE_RDEL_PAYLOAD_KB:-4}"
FUSE_RDEL_RACE_FILES="${FUSE_RDEL_RACE_FILES:-64}"
FUSE_RDEL_SIBLING_BASE_FILES="${FUSE_RDEL_SIBLING_BASE_FILES:-16}"
FUSE_RDEL_SIBLING_MAX_WRITES="${FUSE_RDEL_SIBLING_MAX_WRITES:-4000}"
FUSE_RDEL_KILL_AFTER_S="${FUSE_RDEL_KILL_AFTER_S:-2}"
FUSE_RDEL_BUILD_TIMEOUT_S="${FUSE_RDEL_BUILD_TIMEOUT_S:-600}"
FUSE_RDEL_SWEEP_TIMEOUT_S="${FUSE_RDEL_SWEEP_TIMEOUT_S:-180}"
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
    echo "=== drive9 recursive-delete stress mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
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

# wait_remote_absent polls the list endpoint until the path returns 404,
# i.e. the recursive sweep has fully converged remotely.
wait_remote_absent() {
  local remote="$1"
  local deadline=$(( $(date +%s) + FUSE_RDEL_SWEEP_TIMEOUT_S ))
  while :; do
    local resp code
    resp=$(curl_body_code GET "$BASE/v1/fs$remote/?list" "$API_KEY")
    code=$(http_code "$resp")
    if [ "$code" = "404" ]; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "remote path still present after ${FUSE_RDEL_SWEEP_TIMEOUT_S}s: $remote (last HTTP $code)" >&2
      return 1
    fi
    sleep "$POLL_INTERVAL_S"
  done
}

wait_for_file() {
  local path="$1"
  local deadline=$(( $(date +%s) + FUSE_RDEL_BUILD_TIMEOUT_S ))
  while :; do
    if [ -f "$path" ]; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep 1
  done
}

# build_tree creates FUSE_RDEL_FILES fsync'd files spread over
# FUSE_RDEL_SUBDIRS subdirectories under $1 through the mount. Content is a
# deterministic function of the relative path.
build_tree() {
  python3 - "$1" "$FUSE_RDEL_FILES" "$FUSE_RDEL_SUBDIRS" "$FUSE_RDEL_PAYLOAD_KB" <<'PY'
import hashlib
import os
import sys
from concurrent.futures import ThreadPoolExecutor

root, files, subdirs, payload_kb = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])


def payload(rel):
    header = f"drive9-rdel path={rel}\n".encode()
    seed = hashlib.sha256(header).digest()
    body = bytearray()
    target = payload_kb * 1024
    counter = 0
    while len(body) < target:
        body.extend(hashlib.sha256(seed + counter.to_bytes(8, "big")).digest())
        counter += 1
    return header + bytes(body[:target]) + b"\nEND\n"


def write_one(index):
    rel = f"sub-{index % subdirs:03d}/file-{index:05d}.txt"
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = payload(rel)
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    try:
        off = 0
        while off < len(data):
            off += os.write(fd, data[off:])
        os.fsync(fd)
    finally:
        os.close(fd)


os.makedirs(root, exist_ok=True)
with ThreadPoolExecutor(max_workers=8) as pool:
    for _ in pool.map(write_one, range(files)):
        pass
PY
}

# start_sibling_writer seeds the sibling tree, touches the ready file, then
# keeps writing deterministic live files (recording an exact sha256 manifest)
# until the stop file appears.
start_sibling_writer() {
  python3 - "$SIBLING_MOUNT" "$SIBLING_MANIFEST" "$READY_FILE" "$STOP_FILE" \
    "$FUSE_RDEL_PAYLOAD_KB" "$FUSE_RDEL_SIBLING_BASE_FILES" "$FUSE_RDEL_SIBLING_MAX_WRITES" <<'PY'
import hashlib
import json
import os
import sys

root, manifest_path, ready_file, stop_file = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
payload_kb, base_files, max_writes = int(sys.argv[5]), int(sys.argv[6]), int(sys.argv[7])
manifest = {}


def payload(rel):
    header = f"drive9-rdel-sibling path={rel}\n".encode()
    seed = hashlib.sha256(header).digest()
    body = bytearray()
    target = payload_kb * 1024
    counter = 0
    while len(body) < target:
        body.extend(hashlib.sha256(seed + counter.to_bytes(8, "big")).digest())
        counter += 1
    return header + bytes(body[:target]) + b"\nEND\n"


def write_fsync(rel):
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = payload(rel)
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    try:
        off = 0
        while off < len(data):
            off += os.write(fd, data[off:])
        os.fsync(fd)
    finally:
        os.close(fd)
    manifest[rel] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def flush_manifest():
    tmp = manifest_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, manifest_path)


for i in range(base_files):
    write_fsync(f"base/base-{i:03d}.txt")
flush_manifest()
with open(ready_file, "w", encoding="utf-8") as f:
    f.write("ready\n")

written = 0
while written < max_writes and not os.path.exists(stop_file):
    write_fsync(f"live/live-{written:05d}.txt")
    flush_manifest()
    written += 1
flush_manifest()
print(f"sibling writer done: base={base_files} live={written}")
PY
}

# write_race_new_tree writes the new deterministic file set at the recreated
# same-path directory and records its expected manifest.
write_race_new_tree() {
  python3 - "$RACE_MOUNT_TARGET" "$RACE_NEW_MANIFEST" "$FUSE_RDEL_RACE_FILES" "$FUSE_RDEL_PAYLOAD_KB" <<'PY'
import hashlib
import json
import os
import sys

root, manifest_path, files, payload_kb = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
manifest = {}


def payload(rel):
    header = f"drive9-rdel-race-new path={rel}\n".encode()
    seed = hashlib.sha256(header).digest()
    body = bytearray()
    target = payload_kb * 1024
    counter = 0
    while len(body) < target:
        body.extend(hashlib.sha256(seed + counter.to_bytes(8, "big")).digest())
        counter += 1
    return header + bytes(body[:target]) + b"\nEND\n"


os.makedirs(root, exist_ok=True)
for i in range(files):
    rel = f"new-{i:04d}.txt"
    data = payload(rel)
    fd = os.open(os.path.join(root, rel), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    try:
        off = 0
        while off < len(data):
            off += os.write(fd, data[off:])
        os.fsync(fd)
    finally:
        os.close(fd)
    manifest[rel] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}

with open(manifest_path, "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

# verify_manifest_dir fails unless the directory tree at $2 exactly matches the
# expected sha256 manifest at $3 (no missing, extra, or mismatched files).
verify_manifest_dir() {
  local desc="$1"
  local root="$2"
  local expected="$3"
  local actual="$4"
  TOTAL=$((TOTAL + 1))
  if python3 - "$root" "$expected" "$actual" <<'PY'
import hashlib
import json
import os
import sys
root = os.path.abspath(sys.argv[1])
expected_path = sys.argv[2]
actual_path = sys.argv[3]
with open(expected_path, encoding="utf-8") as f:
    expected = json.load(f)
actual = {}
for current_root, dirs, files in os.walk(root):
    dirs[:] = [d for d in dirs if d != ".go-fuse-epoll-hack"]
    for name in files:
        path = os.path.join(current_root, name)
        rel = os.path.relpath(path, root)
        if rel == ".go-fuse-epoll-hack" or rel.endswith("/.go-fuse-epoll-hack"):
            continue
        with open(path, "rb") as f:
            data = f.read()
        actual[rel] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
with open(actual_path, "w", encoding="utf-8") as f:
    json.dump(actual, f, indent=2, sort_keys=True)
    f.write("\n")
if actual != expected:
    print("manifest mismatch", file=sys.stderr)
    print("missing:", sorted(set(expected) - set(actual))[:20], file=sys.stderr)
    print("extra:", sorted(set(actual) - set(expected))[:20], file=sys.stderr)
    mismatched = sorted(rel for rel in set(expected) & set(actual) if expected[rel] != actual[rel])
    print("mismatched:", mismatched[:20], file=sys.stderr)
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

echo "=== drive9 FUSE recursive-delete stress ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "FUSE_STRICT_PREREQS=$FUSE_STRICT_PREREQS"
echo "FUSE_RDEL_FILES=$FUSE_RDEL_FILES"
echo "FUSE_RDEL_SUBDIRS=$FUSE_RDEL_SUBDIRS"
echo "FUSE_RDEL_PAYLOAD_KB=$FUSE_RDEL_PAYLOAD_KB"
echo "FUSE_RDEL_RACE_FILES=$FUSE_RDEL_RACE_FILES"
echo "FUSE_RDEL_SIBLING_BASE_FILES=$FUSE_RDEL_SIBLING_BASE_FILES"
echo "FUSE_RDEL_SIBLING_MAX_WRITES=$FUSE_RDEL_SIBLING_MAX_WRITES"
echo "FUSE_RDEL_KILL_AFTER_S=$FUSE_RDEL_KILL_AFTER_S"
echo "FUSE_RDEL_SWEEP_TIMEOUT_S=$FUSE_RDEL_SWEEP_TIMEOUT_S"

require_cmd curl
require_cmd jq
require_cmd python3
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

if [ "$FUSE_RDEL_FILES" -lt 1 ] || [ "$FUSE_RDEL_SUBDIRS" -lt 1 ] || [ "$FUSE_RDEL_RACE_FILES" -lt 1 ]; then
  echo "invalid recursive-delete settings: files/subdirs/race-files must be >= 1" >&2
  exit 1
fi

if [ "$FUSE_RDEL_PAYLOAD_KB" -lt 1 ]; then
  echo "invalid FUSE_RDEL_PAYLOAD_KB: must be >= 1" >&2
  exit 1
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

# HOME is overridden so a pre-existing ~/.drive9 context cannot shadow
# DRIVE9_SERVER (context server takes precedence over the env var).
drive9() {
  HOME="$CTX_HOME" DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

TS="$(date +%s)"
if [ -n "$FUSE_RDEL_ARTIFACT_DIR" ]; then
  mkdir -p "$FUSE_RDEL_ARTIFACT_DIR"
  RUN_ROOT="$(mktemp -d "$FUSE_RDEL_ARTIFACT_DIR/drive9-fuse-rdel-${TS}.XXXXXX")"
else
  RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-fuse-rdel-${TS}.XXXXXX")"
fi
RUN_ID="$(basename "$RUN_ROOT")"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
CTX_HOME="$RUN_ROOT/ctx-home"
REMOTE_SNAPSHOT="$RUN_ROOT/remote-snapshot"
ROOT_REL="$RUN_ID"
ROOT_REMOTE="/$ROOT_REL"
WORK_MOUNT="$MOUNT_POINT/$ROOT_REL/work"
WORK_REMOTE="$ROOT_REMOTE/work"
MOUNT_PID=""

mkdir -p "$MOUNT_POINT" "$CTX_HOME"
: > "$MOUNT_LOG"

cleanup() {
  local rc=$?
  stop_mount
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$FUSE_RDEL_KEEP_ARTIFACTS" != "1" ]; then
    rm -rf "$RUN_ROOT"
  else
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount log: $MOUNT_LOG"
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "[4] create remote root"
drive9_retry fs mkdir "$ROOT_REMOTE" >/dev/null
check_eq "remote recursive-delete root" "$ROOT_REMOTE" "$ROOT_REMOTE"

echo "[5] mount writable namespace"
if start_mount; then
  check_eq "recursive-delete mount is mounted" "true" "true"
else
  check_eq "recursive-delete mount is mounted" "false" "true"
  exit 1
fi

echo "[6] scenario A: rm -r of large tree with concurrent sibling writes"
SIBLING_MOUNT="$WORK_MOUNT/stress/sibling"
SIBLING_MANIFEST="$RUN_ROOT/sibling-manifest.json"
READY_FILE="$RUN_ROOT/sibling.ready"
STOP_FILE="$RUN_ROOT/sibling.stop"
RM_STRESS_LOG="$RUN_ROOT/rm-stress.log"
: >"$RM_STRESS_LOG"

check_cmd "stress target tree built through mount" build_tree "$WORK_MOUNT/stress/target"

start_sibling_writer >>"$RUN_ROOT/sibling-writer.log" 2>&1 &
WRITER_PID=$!
check_cmd "sibling writer seeded base files" wait_for_file "$READY_FILE"

set +e
rm_out=$(drive9_retry fs rm -r "$WORK_REMOTE/stress/target" 2>&1)
rm_rc=$?
set -e
printf '%s\n' "$rm_out" >>"$RM_STRESS_LOG"
check_eq "rm -r of large tree succeeds under sibling write load" "$rm_rc" "0"
lock_hits=$(grep -Eic 'lock[ -]?wait|error 1205|deadlock' "$RM_STRESS_LOG" || true)
check_eq "rm -r reported no lock-wait/deadlock errors" "$lock_hits" "0"
check_cmd "deleted stress tree absent remotely" wait_remote_absent "$WORK_REMOTE/stress/target"

touch "$STOP_FILE"
set +e
wait "$WRITER_PID"
writer_rc=$?
set -e
check_eq "background sibling writer exited cleanly" "$writer_rc" "0"
verify_manifest_dir "sibling tree intact through mount (exact manifest)" "$SIBLING_MOUNT" "$SIBLING_MANIFEST" "$RUN_ROOT/sibling-actual-mounted.json"

echo "[7] scenario B: rm -r killed mid-sweep, retry resumes to completion"
RM_RETRY_LOG="$RUN_ROOT/rm-retry.log"
: >"$RM_RETRY_LOG"

check_cmd "retry target tree built through mount" build_tree "$WORK_MOUNT/retry/target"

set +e
drive9 fs rm -r "$WORK_REMOTE/retry/target" >>"$RM_RETRY_LOG" 2>&1 &
RM_PID=$!
sleep "$FUSE_RDEL_KILL_AFTER_S"
killed=0
if kill -0 "$RM_PID" 2>/dev/null; then
  kill -9 "$RM_PID" 2>/dev/null && killed=1
fi
wait "$RM_PID" 2>/dev/null
set -e
if [ "$killed" = "1" ]; then
  echo "INFO killed first rm -r mid-sweep (kill -9 of CLI cancels the request)"
else
  echo "INFO first rm -r finished before the kill landed; resume path not exercised (raise FUSE_RDEL_FILES or lower FUSE_RDEL_KILL_AFTER_S)"
fi

set +e
retry_out=$(drive9_retry fs rm -r "$WORK_REMOTE/retry/target" 2>&1)
retry_rc=$?
set -e
printf '%s\n' "$retry_out" >>"$RM_RETRY_LOG"
if [ "$retry_rc" -ne 0 ]; then
  # Tolerate "already gone": the first pass may have completed server-side
  # before the kill landed, in which case the retry sees a 404.
  if wait_remote_absent "$WORK_REMOTE/retry/target"; then
    check_eq "retry rm -r (first pass already removed the tree)" "0" "0"
  else
    check_eq "retry rm -r succeeds" "$retry_rc" "0"
  fi
else
  check_eq "retry rm -r succeeds" "0" "0"
fi
check_cmd "retry target fully removed remotely (no residue)" wait_remote_absent "$WORK_REMOTE/retry/target"
lock_hits=$(grep -Eic 'lock[ -]?wait|error 1205|deadlock' "$RM_RETRY_LOG" || true)
check_eq "retry rm -r reported no lock-wait/deadlock errors" "$lock_hits" "0"

echo "[8] scenario C: mkdir at the same path racing an in-flight rm -r"
RACE_MOUNT_TARGET="$WORK_MOUNT/race/target"
RACE_NEW_MANIFEST="$RUN_ROOT/race-new-manifest.json"
RM_RACE_LOG="$RUN_ROOT/rm-race.log"
: >"$RM_RACE_LOG"

check_cmd "race target tree built through mount" build_tree "$RACE_MOUNT_TARGET"

set +e
drive9_retry fs rm -r "$WORK_REMOTE/race/target" >>"$RM_RACE_LOG" 2>&1 &
RM_PID=$!
set -e

# Race: recreate the directory through the mount as soon as the sweep drops
# the old root dentry (root-last ordering means this lands at/just after the
# end of the sweep).
race_mkdir_ok=0
race_deadline=$(( $(date +%s) + FUSE_RDEL_SWEEP_TIMEOUT_S ))
while :; do
  if mkdir "$RACE_MOUNT_TARGET" 2>/dev/null; then
    race_mkdir_ok=1
    break
  fi
  if ! kill -0 "$RM_PID" 2>/dev/null; then
    if mkdir "$RACE_MOUNT_TARGET" 2>/dev/null; then
      race_mkdir_ok=1
    fi
    break
  fi
  if [ "$(date +%s)" -ge "$race_deadline" ]; then
    break
  fi
  sleep 0.05
done

# Write the new tree immediately so early files can land while the sweep is
# still finishing; the sweep must never delete the recreated tree.
check_cmd "new tree written at recreated same-path directory" write_race_new_tree

set +e
wait "$RM_PID"
race_rm_rc=$?
set -e
check_eq "raced rm -r completed successfully" "$race_rm_rc" "0"
check_eq "same-path mkdir succeeded during/after rm -r" "$race_mkdir_ok" "1"
lock_hits=$(grep -Eic 'lock[ -]?wait|error 1205|deadlock' "$RM_RACE_LOG" || true)
check_eq "raced rm -r reported no lock-wait/deadlock errors" "$lock_hits" "0"
verify_manifest_dir "recreated same-path tree intact through mount (new files only)" "$RACE_MOUNT_TARGET" "$RACE_NEW_MANIFEST" "$RUN_ROOT/race-actual-mounted.json"

echo "[9] unmount and verify remote persistence"
check_cmd "unmount recursive-delete stress mount" unmount_mount
MOUNT_PID=""
mkdir -p "$REMOTE_SNAPSHOT"
drive9_retry fs cp -r ":$WORK_REMOTE/stress/sibling" "$REMOTE_SNAPSHOT" >/dev/null
if [ -d "$REMOTE_SNAPSHOT/sibling" ]; then
  SIBLING_SNAP="$REMOTE_SNAPSHOT/sibling"
else
  SIBLING_SNAP="$REMOTE_SNAPSHOT"
fi
verify_manifest_dir "remote sibling snapshot matches mounted manifest" "$SIBLING_SNAP" "$SIBLING_MANIFEST" "$RUN_ROOT/sibling-actual-remote.json"
drive9_retry fs cp -r ":$WORK_REMOTE/race/target" "$REMOTE_SNAPSHOT" >/dev/null
if [ -d "$REMOTE_SNAPSHOT/target" ]; then
  RACE_SNAP="$REMOTE_SNAPSHOT/target"
else
  RACE_SNAP="$REMOTE_SNAPSHOT"
fi
verify_manifest_dir "remote same-path snapshot matches new-tree manifest" "$RACE_SNAP" "$RACE_NEW_MANIFEST" "$RUN_ROOT/race-actual-remote.json"

echo "[10] cleanup remote fixture"
drive9_retry fs rm -r "$ROOT_REMOTE" >/dev/null || true

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
