#!/usr/bin/env bash
# Deterministic Drive9 FUSE concurrency stress workload.
#
# This script mounts a fresh writable namespace through real FUSE, then runs
# concurrent readers and writers against ordinary filesystem operations. Final
# correctness is judged by deterministic manifests, not command exit status.
# It intentionally avoids Git and cross-mount checks; those are separate gates.

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
FUSE_CONCURRENCY_KEEP_ARTIFACTS="${FUSE_CONCURRENCY_KEEP_ARTIFACTS:-0}"
FUSE_CONCURRENCY_ARTIFACT_DIR="${FUSE_CONCURRENCY_ARTIFACT_DIR:-}"
FUSE_CONCURRENCY_WORKERS="${FUSE_CONCURRENCY_WORKERS:-4}"
FUSE_CONCURRENCY_FILES_PER_WORKER="${FUSE_CONCURRENCY_FILES_PER_WORKER:-8}"
FUSE_CONCURRENCY_READER_WORKERS="${FUSE_CONCURRENCY_READER_WORKERS:-2}"
FUSE_CONCURRENCY_PAYLOAD_KB="${FUSE_CONCURRENCY_PAYLOAD_KB:-32}"
FUSE_CONCURRENCY_TIMEOUT_S="${FUSE_CONCURRENCY_TIMEOUT_S:-120}"
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
    echo "=== drive9 concurrency mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
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

run_fuse_concurrency_workload() {
  python3 - "$WORK_MOUNT" "$EXPECTED_MANIFEST" "$ACTUAL_MANIFEST" "$READER_ERRORS" "$FUSE_CONCURRENCY_WORKERS" "$FUSE_CONCURRENCY_FILES_PER_WORKER" "$FUSE_CONCURRENCY_READER_WORKERS" "$FUSE_CONCURRENCY_PAYLOAD_KB" "$FUSE_CONCURRENCY_TIMEOUT_S" <<'PY'
import hashlib
import json
import os
import shutil
import sys
import threading
import time
import traceback

root = os.path.abspath(sys.argv[1])
expected_manifest = sys.argv[2]
actual_manifest = sys.argv[3]
reader_errors_path = sys.argv[4]
workers = int(sys.argv[5])
files_per_worker = int(sys.argv[6])
reader_workers = int(sys.argv[7])
payload_kb = int(sys.argv[8])
timeout_s = float(sys.argv[9])

errors = []
reader_errors = []
errors_lock = threading.Lock()
stop_readers = threading.Event()
expected = {}
expected_payloads = {}


def record_error(message):
    with errors_lock:
        errors.append(message)


def record_reader_error(message):
    with errors_lock:
        reader_errors.append(message)


def payload(worker, index, kind):
    header = f"drive9-concurrency kind={kind} worker={worker} index={index}\n".encode()
    seed = hashlib.sha256(header).digest()
    body = bytearray()
    target = payload_kb * 1024
    counter = 0
    while len(body) < target:
        body.extend(hashlib.sha256(seed + counter.to_bytes(8, "big")).digest())
        counter += 1
    return header + bytes(body[:target]) + b"\nEND\n"


def expected_final_file(worker, index):
    return f"final/w{worker}/file-{index:03d}.txt"


def expected_append_file(worker):
    return f"append/w{worker}.log"


def expected_handle_file(worker):
    return f"handles/w{worker}.renamed.txt"


def expected_renamed_dir_file(worker, index):
    return f"renamed-dirs/w{worker}/dst-{index:03d}/payload.txt"


def add_expected(rel, data):
    expected_payloads[rel] = data
    expected[rel] = {
        "size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def first_diff_offset(want, got):
    for offset, (want_byte, got_byte) in enumerate(zip(want, got)):
        if want_byte != got_byte:
            return offset
    if len(want) != len(got):
        return min(len(want), len(got))
    return -1


def sample_hex(data, offset, length=32):
    if offset < 0:
        offset = 0
    start = max(0, offset - (length // 2))
    end = min(len(data), start + length)
    return data[start:end].hex()


def describe_payload_mismatch(rel, want, got):
    diff = first_diff_offset(want, got)
    return (
        f"{rel}: expected_size={len(want)} actual_size={len(got)} "
        f"expected_sha256={hashlib.sha256(want).hexdigest()} "
        f"actual_sha256={hashlib.sha256(got).hexdigest()} "
        f"first_diff={diff} "
        f"expected_hex={sample_hex(want, diff)} "
        f"actual_hex={sample_hex(got, diff)}"
    )


def is_transient_path(rel):
    parts = rel.split(os.sep)
    if rel == ".go-fuse-epoll-hack" or rel.endswith("/.go-fuse-epoll-hack"):
        return True
    if rel.startswith("churn/"):
        return True
    return any(part.startswith(".") or ".tmp." in part or part.endswith(".tmp") for part in parts)


def fsync_parent(path):
    parent = os.path.dirname(path)
    if not parent:
        return
    try:
        fd = os.open(parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fsync_parent(path)


def append_line(path, line):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "ab") as f:
        f.write(line)
        f.flush()
        os.fsync(f.fileno())


def writer(worker):
    try:
        append_data = bytearray()
        for index in range(files_per_worker):
            final_rel = expected_final_file(worker, index)
            final_path = os.path.join(root, final_rel)
            data = payload(worker, index, "final")
            atomic_write(final_path, data)
            append_line_data = f"worker={worker} index={index} append-ok\n".encode()
            append_line(os.path.join(root, expected_append_file(worker)), append_line_data)
            append_data.extend(append_line_data)

            churn_dir = os.path.join(root, "churn", f"w{worker}")
            os.makedirs(churn_dir, exist_ok=True)
            churn_tmp = os.path.join(churn_dir, f"delete-{index:03d}.tmp")
            with open(churn_tmp, "wb") as f:
                f.write(payload(worker, index, "delete"))
                f.flush()
                os.fsync(f.fileno())
            os.unlink(churn_tmp)

            src_dir = os.path.join(root, "renamed-dirs", f"w{worker}", f"src-{index:03d}")
            dst_dir = os.path.join(root, "renamed-dirs", f"w{worker}", f"dst-{index:03d}")
            os.makedirs(src_dir, exist_ok=True)
            dir_data = payload(worker, index, "renamed-dir")
            atomic_write(os.path.join(src_dir, "payload.txt"), dir_data)
            os.replace(src_dir, dst_dir)
            fsync_parent(dst_dir)

            handle_initial = os.path.join(root, "handles", f"w{worker}.txt")
            handle_final = os.path.join(root, expected_handle_file(worker))
            handle_data = payload(worker, index, "handle")
            atomic_write(handle_initial, handle_data)
            with open(handle_initial, "rb") as open_handle:
                os.replace(handle_initial, handle_final)
                fsync_parent(handle_final)
                seen = open_handle.read()
            if seen != handle_data:
                raise AssertionError(f"open handle content mismatch worker={worker} index={index}")
    except Exception:
        record_error(traceback.format_exc())


def reader(reader_id):
    while not stop_readers.is_set():
        try:
            for current_root, _, files in os.walk(root):
                for name in files:
                    path = os.path.join(current_root, name)
                    rel = os.path.relpath(path, root)
                    if is_transient_path(rel):
                        continue
                    try:
                        with open(path, "rb") as f:
                            data = f.read()
                    except FileNotFoundError:
                        continue
                    except IsADirectoryError:
                        continue
                    if rel.startswith("final/"):
                        parts = rel.split("/")
                        try:
                            worker = int(parts[1][1:])
                            index = int(parts[2].split("-")[1].split(".")[0])
                        except Exception:
                            record_reader_error(f"reader={reader_id} unexpected final path {rel}")
                            continue
                        want = payload(worker, index, "final")
                        if data != want:
                            record_reader_error(
                                f"reader={reader_id} mixed/short final read "
                                + describe_payload_mismatch(rel, want, data)
                            )
        except Exception:
            record_reader_error(traceback.format_exc())
        time.sleep(0.01)


def build_expected():
    for worker in range(workers):
        append_data = bytearray()
        for index in range(files_per_worker):
            add_expected(expected_final_file(worker, index), payload(worker, index, "final"))
            add_expected(expected_renamed_dir_file(worker, index), payload(worker, index, "renamed-dir"))
            append_data.extend(f"worker={worker} index={index} append-ok\n".encode())
        add_expected(expected_append_file(worker), bytes(append_data))
        add_expected(expected_handle_file(worker), payload(worker, files_per_worker - 1, "handle"))


def build_actual():
    actual = {}
    actual_payloads = {}
    for current_root, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d != ".go-fuse-epoll-hack"]
        for name in files:
            path = os.path.join(current_root, name)
            rel = os.path.relpath(path, root)
            if is_transient_path(rel):
                continue
            with open(path, "rb") as f:
                data = f.read()
            actual_payloads[rel] = data
            actual[rel] = {
                "size": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
    return actual, actual_payloads


shutil.rmtree(root, ignore_errors=True)
os.makedirs(root, exist_ok=True)
build_expected()

reader_threads = [threading.Thread(target=reader, args=(i,), daemon=True) for i in range(reader_workers)]
writer_threads = [threading.Thread(target=writer, args=(i,), daemon=True) for i in range(workers)]

for thread in reader_threads + writer_threads:
    thread.start()

deadline = time.monotonic() + timeout_s
for thread in writer_threads:
    remaining = deadline - time.monotonic()
    thread.join(max(0.1, remaining))
    if thread.is_alive():
        record_error(f"writer thread timed out: {thread.name}")

stop_readers.set()
for thread in reader_threads:
    thread.join(5)

actual, actual_payloads = build_actual()
with open(expected_manifest, "w", encoding="utf-8") as f:
    json.dump(expected, f, indent=2, sort_keys=True)
    f.write("\n")
with open(actual_manifest, "w", encoding="utf-8") as f:
    json.dump(actual, f, indent=2, sort_keys=True)
    f.write("\n")
with open(reader_errors_path, "w", encoding="utf-8") as f:
    for error in reader_errors:
        f.write(error)
        if not error.endswith("\n"):
            f.write("\n")

missing = sorted(set(expected) - set(actual))
extra = sorted(set(actual) - set(expected))
mismatched = sorted(rel for rel in set(expected) & set(actual) if expected[rel] != actual[rel])

if missing:
    record_error("missing files: " + ", ".join(missing[:20]))
if extra:
    record_error("extra files: " + ", ".join(extra[:20]))
if mismatched:
    record_error("mismatched files: " + ", ".join(mismatched[:20]))
    for rel in mismatched[:20]:
        record_error(
            "mismatch detail: "
            + describe_payload_mismatch(rel, expected_payloads[rel], actual_payloads[rel])
        )
if reader_errors:
    record_error(f"reader observed {len(reader_errors)} inconsistent reads; see {reader_errors_path}")

if errors:
    for error in errors:
        sys.stderr.write(error)
        if not error.endswith("\n"):
            sys.stderr.write("\n")
    raise SystemExit(1)
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
    for rel in mismatched[:20]:
        print(
            "mismatch detail:",
            rel,
            "expected_size=", expected[rel].get("size"),
            "actual_size=", actual[rel].get("size"),
            "expected_sha256=", expected[rel].get("sha256"),
            "actual_sha256=", actual[rel].get("sha256"),
            file=sys.stderr,
        )
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

echo "=== drive9 FUSE concurrency stress ==="
echo "BASE=$BASE"
echo "CLI_SOURCE=$CLI_SOURCE"
echo "FUSE_STRICT_PREREQS=$FUSE_STRICT_PREREQS"
echo "FUSE_CONCURRENCY_WORKERS=$FUSE_CONCURRENCY_WORKERS"
echo "FUSE_CONCURRENCY_FILES_PER_WORKER=$FUSE_CONCURRENCY_FILES_PER_WORKER"
echo "FUSE_CONCURRENCY_READER_WORKERS=$FUSE_CONCURRENCY_READER_WORKERS"
echo "FUSE_CONCURRENCY_PAYLOAD_KB=$FUSE_CONCURRENCY_PAYLOAD_KB"
echo "FUSE_CONCURRENCY_TIMEOUT_S=$FUSE_CONCURRENCY_TIMEOUT_S"

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

if [ "$FUSE_CONCURRENCY_WORKERS" -lt 1 ] || [ "$FUSE_CONCURRENCY_FILES_PER_WORKER" -lt 1 ] || [ "$FUSE_CONCURRENCY_READER_WORKERS" -lt 1 ]; then
  echo "invalid concurrency settings: workers/files/readers must be >= 1" >&2
  exit 1
fi

if [ "$FUSE_CONCURRENCY_PAYLOAD_KB" -lt 1 ]; then
  echo "invalid FUSE_CONCURRENCY_PAYLOAD_KB: must be >= 1" >&2
  exit 1
fi

echo "[1] provision tenant"
if [ -n "$DRIVE9_API_KEY" ]; then
  API_KEY="$DRIVE9_API_KEY"
  check_eq "use provided DRIVE9_API_KEY" "true" "true"
else
  resp=$(drive9_provision_curl_body_code "$BASE" || true)
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
if [ -n "$FUSE_CONCURRENCY_ARTIFACT_DIR" ]; then
  mkdir -p "$FUSE_CONCURRENCY_ARTIFACT_DIR"
  RUN_ROOT="$(mktemp -d "$FUSE_CONCURRENCY_ARTIFACT_DIR/drive9-fuse-concurrency-${TS}.XXXXXX")"
else
  RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-fuse-concurrency-${TS}.XXXXXX")"
fi
RUN_ID="$(basename "$RUN_ROOT")"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
REMOTE_SNAPSHOT="$RUN_ROOT/remote-snapshot"
EXPECTED_MANIFEST="$RUN_ROOT/expected-manifest.json"
ACTUAL_MANIFEST="$RUN_ROOT/actual-mounted-manifest.json"
REMOTE_MANIFEST="$RUN_ROOT/actual-remote-manifest.json"
READER_ERRORS="$RUN_ROOT/reader-errors.log"
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
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$FUSE_CONCURRENCY_KEEP_ARTIFACTS" != "1" ]; then
    rm -rf "$RUN_ROOT"
  else
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount log: $MOUNT_LOG"
    echo "Expected manifest: $EXPECTED_MANIFEST"
    echo "Actual mount manifest: $ACTUAL_MANIFEST"
    echo "Remote manifest: $REMOTE_MANIFEST"
    echo "Reader errors: $READER_ERRORS"
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "[4] create remote root"
drive9_retry fs mkdir "$ROOT_REMOTE" >/dev/null
check_eq "remote concurrency root" "$ROOT_REMOTE" "$ROOT_REMOTE"

echo "[5] mount writable namespace"
if start_mount; then
  check_eq "concurrency mount is mounted" "true" "true"
else
  check_eq "concurrency mount is mounted" "false" "true"
fi

if is_mounted "$MOUNT_POINT"; then
  echo "[6] run concurrent writers/readers/rename/unlink/open-handle workload"
  check_cmd "concurrent mounted workload reaches deterministic final manifest" run_fuse_concurrency_workload

  echo "[7] unmount and verify remote persistence"
  check_cmd "unmount concurrency mount" unmount_mount
  MOUNT_PID=""
  mkdir -p "$REMOTE_SNAPSHOT"
  drive9_retry fs cp -r ":$WORK_REMOTE" "$REMOTE_SNAPSHOT" >/dev/null
  if [ -d "$REMOTE_SNAPSHOT/work" ]; then
    REMOTE_WORK_SNAPSHOT="$REMOTE_SNAPSHOT/work"
  else
    REMOTE_WORK_SNAPSHOT="$REMOTE_SNAPSHOT"
  fi
  verify_manifest_dir "remote snapshot manifest matches mounted final manifest" "$REMOTE_WORK_SNAPSHOT" "$REMOTE_MANIFEST"
fi

echo "[8] cleanup remote fixture"
drive9_retry fs rm -r "$ROOT_REMOTE" >/dev/null || true

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
