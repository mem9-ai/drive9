#!/usr/bin/env bash
# Opt-in Drive9 FUSE POSIX/fsx-style gate.
#
# This is intentionally smaller than full pjdfstest/LTP, but it exercises the
# same class of semantics that routinely catch FUSE bugs: deterministic random
# write/read/truncate, atomic rename replacement, unlink-open reads, directory
# fsync, final model hashing, unmount persistence, and remote snapshot parity.

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
FUSE_POSIX_FSX_KEEP_ARTIFACTS="${FUSE_POSIX_FSX_KEEP_ARTIFACTS:-0}"
FUSE_POSIX_FSX_OPS="${FUSE_POSIX_FSX_OPS:-160}"
FUSE_POSIX_FSX_MAX_BYTES="${FUSE_POSIX_FSX_MAX_BYTES:-262144}"
FUSE_POSIX_FSX_SEED="${FUSE_POSIX_FSX_SEED:-drive9-posix-fsx-v1}"
FUSE_POSIX_FSX_TIMEOUT_S="${FUSE_POSIX_FSX_TIMEOUT_S:-120}"
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
    *) echo "unsupported OS for official CLI download: $(uname -s)" >&2; return 1 ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64) CLI_RELEASE_ARCH="amd64" ;;
    aarch64|arm64) CLI_RELEASE_ARCH="arm64" ;;
    *) echo "unsupported architecture for official CLI download: $(uname -m)" >&2; return 1 ;;
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
    *) echo "invalid CLI_SOURCE: $CLI_SOURCE (expected build|official)" >&2; return 1 ;;
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
    echo "=== drive9 posix/fsx mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
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

run_posix_fsx_workload() {
  python3 - "$WORK_MOUNT" "$EXPECTED_MANIFEST" "$ACTUAL_MANIFEST" "$FUSE_POSIX_FSX_OPS" "$FUSE_POSIX_FSX_MAX_BYTES" "$FUSE_POSIX_FSX_SEED" "$FUSE_POSIX_FSX_TIMEOUT_S" <<'PY'
import hashlib
import json
import os
import random
import shutil
import sys
import time

root = os.path.abspath(sys.argv[1])
expected_manifest = sys.argv[2]
actual_manifest = sys.argv[3]
ops = int(sys.argv[4])
max_bytes = int(sys.argv[5])
seed = sys.argv[6]
timeout_s = float(sys.argv[7])
random_state = random.Random(seed)
model = bytearray()
primary = os.path.join(root, "fsx", "work.bin")
alternate = os.path.join(root, "fsx", "work-renamed.bin")
current_op = {"index": "setup", "type": "initialize"}
failure_reported = False


def set_op(index, op_type, **fields):
    current_op.clear()
    current_op.update({"index": index, "type": op_type})
    current_op.update(fields)


def first_mismatch(got, want):
    for index, (got_byte, want_byte) in enumerate(zip(got, want)):
        if got_byte != want_byte:
            return index, got_byte, want_byte
    if len(got) != len(want):
        index = min(len(got), len(want))
        got_byte = got[index] if index < len(got) else None
        want_byte = want[index] if index < len(want) else None
        return index, got_byte, want_byte
    return None, None, None


def report_failure(reason, **fields):
    global failure_reported
    failure_reported = True
    details = {
        "reason": reason,
        "seed": seed,
        "model_size": len(model),
        "op": dict(current_op),
    }
    details.update(fields)
    print("fsx failure:", json.dumps(details, sort_keys=True), file=sys.stderr)


def fail(reason, **fields):
    report_failure(reason, **fields)
    raise AssertionError(reason)


def deterministic_bytes(label, size):
    digest = hashlib.sha256(label.encode()).digest()
    out = bytearray()
    counter = 0
    while len(out) < size:
        out.extend(hashlib.sha256(digest + counter.to_bytes(8, "big")).digest())
        counter += 1
    return bytes(out[:size])


def fsync_parent(path):
    fd = os.open(os.path.dirname(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_model(path, offset, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "r+b" if os.path.exists(path) else "w+b") as handle:
        handle.seek(offset)
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    end = offset + len(data)
    if end > len(model):
        model.extend(b"\0" * (end - len(model)))
    model[offset:end] = data


def truncate_model(path, size):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "r+b" if os.path.exists(path) else "w+b") as handle:
        handle.truncate(size)
        handle.flush()
        os.fsync(handle.fileno())
    del model[size:]
    if size > len(model):
        model.extend(b"\0" * (size - len(model)))


def read_verify(path, offset, size):
    with open(path, "rb") as handle:
        handle.seek(offset)
        got = handle.read(size)
    want = bytes(model[offset:offset + size])
    if got != want:
        mismatch_offset, got_byte, want_byte = first_mismatch(got, want)
        fail(
            "read mismatch",
            path=os.path.relpath(path, root),
            offset=offset,
            requested_size=size,
            got_len=len(got),
            want_len=len(want),
            got_sha256=hashlib.sha256(got).hexdigest(),
            want_sha256=hashlib.sha256(want).hexdigest(),
            first_mismatch_offset=mismatch_offset,
            got_byte=got_byte,
            want_byte=want_byte,
        )


def atomic_replace(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    fsync_parent(path)


def verify_unlink_open():
    set_op("unlink-open", "unlink_open", size=4096)
    path = os.path.join(root, "fsx", "unlink-open.bin")
    data = deterministic_bytes("unlink-open", 4096)
    atomic_replace(path, data)
    with open(path, "rb") as handle:
        os.unlink(path)
        if os.path.exists(path):
            fail("unlink-open path still exists", path=os.path.relpath(path, root))
        got = handle.read()
    if got != data:
        mismatch_offset, got_byte, want_byte = first_mismatch(got, data)
        fail(
            "unlink-open handle did not retain original bytes",
            path=os.path.relpath(path, root),
            got_len=len(got),
            want_len=len(data),
            got_sha256=hashlib.sha256(got).hexdigest(),
            want_sha256=hashlib.sha256(data).hexdigest(),
            first_mismatch_offset=mismatch_offset,
            got_byte=got_byte,
            want_byte=want_byte,
        )
    fsync_parent(path)


def verify_rename_replace(path):
    replacement = deterministic_bytes("rename-replace", min(max_bytes, 8192))
    tmp = os.path.join(root, "fsx", "replace-source.bin")
    atomic_replace(tmp, replacement)
    with open(tmp, "rb") as handle:
        os.replace(tmp, path)
        fsync_parent(path)
        got = handle.read()
    if got != replacement:
        mismatch_offset, got_byte, want_byte = first_mismatch(got, replacement)
        fail(
            "open source handle changed after rename replace",
            path=os.path.relpath(path, root),
            got_len=len(got),
            want_len=len(replacement),
            got_sha256=hashlib.sha256(got).hexdigest(),
            want_sha256=hashlib.sha256(replacement).hexdigest(),
            first_mismatch_offset=mismatch_offset,
            got_byte=got_byte,
            want_byte=want_byte,
        )
    model[:] = replacement


shutil.rmtree(root, ignore_errors=True)
os.makedirs(os.path.dirname(primary), exist_ok=True)
atomic_replace(primary, b"")
start = time.monotonic()
for index in range(ops):
    if time.monotonic() - start > timeout_s:
        raise TimeoutError(f"fsx workload exceeded {timeout_s}s")
    choice = random_state.randrange(100)
    current = primary
    try:
        if choice < 48:
            length = random_state.randint(1, min(max_bytes, 16384))
            offset = random_state.randint(0, max(0, max_bytes - length))
            set_op(index, "write", offset=offset, size=length, before_size=len(model))
            data = deterministic_bytes(f"write:{index}:{offset}:{length}", length)
            write_model(current, offset, data)
        elif choice < 68:
            new_size = random_state.randint(0, max_bytes)
            set_op(index, "truncate", size=new_size, before_size=len(model))
            truncate_model(current, new_size)
        elif choice < 86:
            if len(model) == 0:
                set_op(index, "read", offset=0, size=1, before_size=0)
                read_verify(current, 0, 1)
            else:
                offset = random_state.randint(0, len(model))
                length = random_state.randint(0, min(16384, max(0, len(model) - offset)))
                set_op(index, "read", offset=offset, size=length, before_size=len(model))
                read_verify(current, offset, length)
        elif choice < 94:
            set_op(index, "rename_roundtrip", before_size=len(model))
            os.replace(primary, alternate)
            fsync_parent(alternate)
            os.replace(alternate, primary)
            fsync_parent(primary)
            set_op(index, "rename_roundtrip_read", offset=0, size=len(model), before_size=len(model))
            read_verify(primary, 0, len(model))
        else:
            set_op(index, "rename_replace", replacement_size=min(max_bytes, 8192), before_size=len(model))
            verify_rename_replace(primary)
    except Exception as exc:
        if not failure_reported:
            report_failure("operation failed", exception=repr(exc))
        raise

verify_unlink_open()
set_op("final", "final_read", offset=0, size=len(model), before_size=len(model))
read_verify(primary, 0, len(model))
expected = {
    "fsx/work.bin": {
        "size": len(model),
        "sha256": hashlib.sha256(bytes(model)).hexdigest(),
        "ops": ops,
        "max_bytes": max_bytes,
        "seed": seed,
    }
}
actual = {}
with open(primary, "rb") as handle:
    data = handle.read()
actual["fsx/work.bin"] = {
    "size": len(data),
    "sha256": hashlib.sha256(data).hexdigest(),
    "ops": ops,
    "max_bytes": max_bytes,
    "seed": seed,
}
with open(expected_manifest, "w", encoding="utf-8") as handle:
    json.dump(expected, handle, indent=2, sort_keys=True)
    handle.write("\n")
with open(actual_manifest, "w", encoding="utf-8") as handle:
    json.dump(actual, handle, indent=2, sort_keys=True)
    handle.write("\n")
if actual != expected:
    raise AssertionError("final fsx manifest mismatch")
PY
}

verify_manifest_dir() {
  local desc="$1"
  local root="$2"
  local actual="$3"
  TOTAL=$((TOTAL + 1))
  if python3 - "$root" "$EXPECTED_MANIFEST" "$actual" <<'PY'
import hashlib
import json
import os
import sys
root = os.path.abspath(sys.argv[1])
expected_path = sys.argv[2]
actual_path = sys.argv[3]
with open(expected_path, encoding="utf-8") as handle:
    expected = json.load(handle)
actual = {}
path = os.path.join(root, "fsx", "work.bin")
with open(path, "rb") as handle:
    data = handle.read()
base = expected["fsx/work.bin"]
actual["fsx/work.bin"] = {
    "size": len(data),
    "sha256": hashlib.sha256(data).hexdigest(),
    "ops": base["ops"],
    "max_bytes": base["max_bytes"],
    "seed": base["seed"],
}
with open(actual_path, "w", encoding="utf-8") as handle:
    json.dump(actual, handle, indent=2, sort_keys=True)
    handle.write("\n")
if actual != expected:
    print("fsx manifest mismatch", file=sys.stderr)
    print("expected:", expected, file=sys.stderr)
    print("actual:", actual, file=sys.stderr)
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

echo "=== drive9 FUSE POSIX/fsx gate ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "FUSE_STRICT_PREREQS=$FUSE_STRICT_PREREQS"
echo "FUSE_POSIX_FSX_OPS=$FUSE_POSIX_FSX_OPS"
echo "FUSE_POSIX_FSX_MAX_BYTES=$FUSE_POSIX_FSX_MAX_BYTES"
echo "FUSE_POSIX_FSX_SEED=$FUSE_POSIX_FSX_SEED"
echo "FUSE_POSIX_FSX_TIMEOUT_S=$FUSE_POSIX_FSX_TIMEOUT_S"

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

require_positive_int FUSE_POSIX_FSX_OPS "$FUSE_POSIX_FSX_OPS"
require_positive_int FUSE_POSIX_FSX_MAX_BYTES "$FUSE_POSIX_FSX_MAX_BYTES"
require_positive_int FUSE_POSIX_FSX_TIMEOUT_S "$FUSE_POSIX_FSX_TIMEOUT_S"

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
RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-fuse-posix-fsx-${TS}.XXXXXX")"
RUN_ID="$(basename "$RUN_ROOT")"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
REMOTE_SNAPSHOT="$RUN_ROOT/remote-snapshot"
EXPECTED_MANIFEST="$RUN_ROOT/expected-manifest.json"
ACTUAL_MANIFEST="$RUN_ROOT/actual-mounted-manifest.json"
REMOTE_MANIFEST="$RUN_ROOT/actual-remote-manifest.json"
ROOT_REL="$RUN_ID"
ROOT_REMOTE="/$ROOT_REL"
WORK_MOUNT="$MOUNT_POINT/$ROOT_REL/work"
WORK_REMOTE="$ROOT_REMOTE/work"
MOUNT_PID=""

mkdir -p "$MOUNT_POINT"
: > "$MOUNT_LOG"

cleanup() {
  local rc=$?
  stop_mount
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$FUSE_POSIX_FSX_KEEP_ARTIFACTS" != "1" ]; then
    rm -rf "$RUN_ROOT"
  else
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount log: $MOUNT_LOG"
    echo "Expected manifest: $EXPECTED_MANIFEST"
    echo "Actual mount manifest: $ACTUAL_MANIFEST"
    echo "Remote manifest: $REMOTE_MANIFEST"
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "[4] create remote root"
drive9_retry fs mkdir "$ROOT_REMOTE" >/dev/null
check_eq "remote POSIX/fsx root" "$ROOT_REMOTE" "$ROOT_REMOTE"

echo "[5] mount writable namespace"
if start_mount; then
  check_eq "POSIX/fsx mount is mounted" "true" "true"
else
  check_eq "POSIX/fsx mount is mounted" "false" "true"
fi

if is_mounted "$MOUNT_POINT"; then
  echo "[6] run deterministic fsx-style workload"
  check_cmd "fsx-style workload reaches modeled final state" run_posix_fsx_workload

  echo "[7] unmount and verify remote persistence"
  check_cmd "unmount POSIX/fsx mount" unmount_mount
  MOUNT_PID=""
  mkdir -p "$REMOTE_SNAPSHOT"
  drive9_retry fs cp -r ":$WORK_REMOTE" "$REMOTE_SNAPSHOT" >/dev/null
  if [ -d "$REMOTE_SNAPSHOT/work" ]; then
    REMOTE_WORK_SNAPSHOT="$REMOTE_SNAPSHOT/work"
  else
    REMOTE_WORK_SNAPSHOT="$REMOTE_SNAPSHOT"
  fi
  verify_manifest_dir "remote snapshot manifest matches POSIX/fsx final manifest" "$REMOTE_WORK_SNAPSHOT" "$REMOTE_MANIFEST"
fi

echo "[8] cleanup remote fixture"
drive9_retry fs rm -r "$ROOT_REMOTE" >/dev/null || true

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
