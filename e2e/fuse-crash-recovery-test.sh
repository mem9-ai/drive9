#!/usr/bin/env bash
# drive9 FUSE crash-recovery e2e gate.
#
# Simulates a daemon crash (kill -9) in the middle of the interactive write
# path and verifies that fsync'd data survives the crash:
#   - fsync'd small files (shadow + pending + WAL) upload after remount
#   - a large ShadowSpill file killed mid-upload streams again after remount
#   - a file unlinked before the crash does NOT resurrect from the WAL
#   - the journal WAL compacts across clean remounts (no unbounded growth)
#
# Interactive durability contract under test: fsync() returning means the data
# is locally durable and MUST eventually commit remotely, even if the daemon
# dies immediately afterwards.

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
CRASH_SMALL_FILES="${CRASH_SMALL_FILES:-10}"
CRASH_SMALL_KB="${CRASH_SMALL_KB:-16}"
CRASH_LARGE_MB="${CRASH_LARGE_MB:-96}"
CRASH_RECOVERY_TIMEOUT_S="${CRASH_RECOVERY_TIMEOUT_S:-240}"
CRASH_RECOVERY_INTERVAL_S="${CRASH_RECOVERY_INTERVAL_S:-2}"
CRASH_KEEP_ARTIFACTS="${CRASH_KEEP_ARTIFACTS:-0}"
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
    echo "=== drive9 crash-recovery mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "cache_dir=$CACHE_DIR"
  } >>"$MOUNT_LOG"
  # --foreground keeps the daemon as our child: plain `drive9 mount`
  # daemonizes (re-execs a --foreground child and the parent exits), which
  # would make $! useless for kill -9.
  drive9 mount --foreground --cache-dir "$CACHE_DIR" --durability interactive "$MOUNT_POINT" >>"$MOUNT_LOG" 2>&1 &
  MOUNT_PID="$!"
  if wait_mount_state mounted; then
    DAEMON_PID="$(pgrep -f "$CLI_BIN mount" 2>/dev/null | head -1)"
    DAEMON_PID="${DAEMON_PID:-$MOUNT_PID}"
    return 0
  fi
  cat "$MOUNT_LOG" >&2 || true
  return 1
}

# crash_mount kills the daemon with SIGKILL — no flush, no cleanup — then
# clears the stale FUSE mount point so the next mount can attach.
crash_mount() {
  if [ -z "${DAEMON_PID:-}" ]; then
    return 1
  fi
  kill -9 "$DAEMON_PID" 2>/dev/null || return 1
  set +e
  if [ -n "${MOUNT_PID:-}" ]; then
    kill -9 "$MOUNT_PID" 2>/dev/null
    wait "$MOUNT_PID" 2>/dev/null
  fi
  set -e
  MOUNT_PID=""
  DAEMON_PID=""
  force_unmount_stale
  return 0
}

force_unmount_stale() {
  set +e
  if [ "$(uname -s)" = "Linux" ]; then
    fusermount3 -uz "$MOUNT_POINT" 2>/dev/null \
      || fusermount -uz "$MOUNT_POINT" 2>/dev/null \
      || umount -l "$MOUNT_POINT" 2>/dev/null
  else
    umount -f "$MOUNT_POINT" 2>/dev/null \
      || diskutil unmount force "$MOUNT_POINT" >/dev/null 2>&1
  fi
  wait_mount_state unmounted >/dev/null 2>&1
  set -e
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

prepare_cli_binary() {
  CLI_BIN="$(mktemp)"
  make build-cli CLI_BIN="$CLI_BIN"
  chmod +x "$CLI_BIN"
}

# write_crash_workload writes the pre-crash workload through the mount and
# records the expected sha256 manifest. Everything is fsync'd, so the
# interactive durability contract applies to every file. Order matters: the
# large ShadowSpill file goes last so its async upload is most likely still
# in flight when the daemon is killed.
write_crash_workload() {
  python3 - "$WORK_MOUNT" "$EXPECTED_MANIFEST" "$CRASH_SMALL_FILES" "$CRASH_SMALL_KB" "$CRASH_LARGE_MB" <<'PY'
import hashlib
import json
import os
import sys

root, manifest_path = sys.argv[1], sys.argv[2]
small_files, small_kb, large_mb = int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])

os.makedirs(root, exist_ok=True)
manifest = {}

def write_fsync(rel, data):
    path = os.path.join(root, rel)
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    try:
        off = 0
        while off < len(data):
            off += os.write(fd, data[off:off + (1 << 20)])
        os.fsync(fd)
    finally:
        os.close(fd)
    manifest[rel] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}

def payload(name, size):
    seed = hashlib.sha256(name.encode()).digest()
    out = bytearray()
    counter = 0
    while len(out) < size:
        out.extend(hashlib.sha256(seed + counter.to_bytes(8, "big")).digest())
        counter += 1
    return bytes(out[:size])

for i in range(small_files):
    rel = f"small-{i:02d}.bin"
    write_fsync(rel, payload(rel, small_kb * 1024))

# Unlinked-before-crash file: fsync'd then removed. Must NOT resurrect.
write_fsync("doomed.txt", b"doomed-by-unlink\n")
os.unlink(os.path.join(root, "doomed.txt"))
del manifest["doomed.txt"]

# Large ShadowSpill file last: its async commit is the crash victim.
write_fsync("large-spill.bin", payload("large-spill.bin", large_mb << 20))

with open(manifest_path, "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

# wait_remote_recovered polls until every manifest entry is readable remotely
# with the exact expected sha256 (via drive9 fs cat), then confirms the
# unlinked file did not resurrect.
wait_remote_recovered() {
  local deadline=$(( $(date +%s) + CRASH_RECOVERY_TIMEOUT_S ))
  while :; do
    if DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" HOME="$CTX_HOME" \
      python3 - "$CLI_BIN" "$WORK_REMOTE" "$EXPECTED_MANIFEST" <<'PY'
import hashlib
import json
import subprocess
import sys

cli, remote_root, manifest_path = sys.argv[1], sys.argv[2], sys.argv[3]
with open(manifest_path, encoding="utf-8") as f:
    manifest = json.load(f)

for rel, want in sorted(manifest.items()):
    proc = subprocess.run([cli, "fs", "cat", f"{remote_root}/{rel}"], capture_output=True)
    if proc.returncode != 0:
        print(f"waiting: {rel}: {proc.stderr.decode(errors='replace').strip()}", file=sys.stderr)
        raise SystemExit(1)
    got = hashlib.sha256(proc.stdout).hexdigest()
    if got != want["sha256"] or len(proc.stdout) != want["size"]:
        print(f"waiting: {rel}: size={len(proc.stdout)} sha256={got} want size={want['size']} sha256={want['sha256']}", file=sys.stderr)
        raise SystemExit(1)
raise SystemExit(0)
PY
    then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$CRASH_RECOVERY_INTERVAL_S"
  done
}

check_doomed_absent() {
  local out
  set +e
  out=$(drive9 fs cat "$WORK_REMOTE/doomed.txt" 2>&1)
  local rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    echo "  doomed.txt resurrected with content: $out" >&2
    return 1
  fi
  return 0
}

verify_mount_manifest() {
  python3 - "$WORK_MOUNT" "$EXPECTED_MANIFEST" <<'PY'
import hashlib
import json
import os
import sys

root, manifest_path = sys.argv[1], sys.argv[2]
with open(manifest_path, encoding="utf-8") as f:
    manifest = json.load(f)
ok = True
for rel, want in sorted(manifest.items()):
    path = os.path.join(root, rel)
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError as e:
        print(f"mount read {rel}: {e}", file=sys.stderr)
        ok = False
        continue
    got = hashlib.sha256(data).hexdigest()
    if got != want["sha256"]:
        print(f"mount sha mismatch {rel}: got {got} want {want['sha256']}", file=sys.stderr)
        ok = False
if os.path.exists(os.path.join(root, "doomed.txt")):
    print("doomed.txt visible through mount after recovery", file=sys.stderr)
    ok = False
raise SystemExit(0 if ok else 1)
PY
}

wal_path() {
  # cache layout: <cache-dir>/<mountHash>/journal.wal — single mount per dir.
  find "$CACHE_DIR" -name journal.wal 2>/dev/null | head -1
}

echo "=== drive9 FUSE crash-recovery test ==="
echo "BASE=$BASE"
echo "CRASH_SMALL_FILES=$CRASH_SMALL_FILES CRASH_SMALL_KB=$CRASH_SMALL_KB CRASH_LARGE_MB=$CRASH_LARGE_MB"
echo "CRASH_RECOVERY_TIMEOUT_S=$CRASH_RECOVERY_TIMEOUT_S"

require_cmd curl
require_cmd jq
require_cmd python3
require_cmd go
require_cmd make

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

# HOME is overridden so a pre-existing ~/.drive9 context cannot shadow
# DRIVE9_SERVER (context server takes precedence over the env var).
drive9() {
  HOME="$CTX_HOME" DRIVE9_SERVER="$BASE" DRIVE9_API_KEY="$API_KEY" "$CLI_BIN" "$@"
}

TS="$(date +%s)"
RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-fuse-crash-${TS}.XXXXXX")"
RUN_ID="$(basename "$RUN_ROOT")"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
CACHE_DIR="$RUN_ROOT/cache"
CTX_HOME="$RUN_ROOT/ctx-home"
EXPECTED_MANIFEST="$RUN_ROOT/expected-manifest.json"
ROOT_REL="$RUN_ID"
ROOT_REMOTE="/$ROOT_REL"
WORK_MOUNT="$MOUNT_POINT/$ROOT_REL/work"
WORK_REMOTE="$ROOT_REMOTE/work"
MOUNT_PID=""
DAEMON_PID=""

mkdir -p "$MOUNT_POINT" "$CACHE_DIR" "$CTX_HOME"
: > "$MOUNT_LOG"

cleanup() {
  local rc=$?
  stop_mount
  force_unmount_stale 2>/dev/null || true
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$CRASH_KEEP_ARTIFACTS" != "1" ]; then
    rm -rf "$RUN_ROOT"
  else
    echo "Artifacts preserved at $RUN_ROOT"
    echo "Mount log: $MOUNT_LOG"
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "[4] create remote root"
drive9 fs mkdir "$ROOT_REMOTE" >/dev/null
drive9 fs mkdir "$WORK_REMOTE" >/dev/null
check_eq "remote crash-recovery root" "$ROOT_REMOTE" "$ROOT_REMOTE"

echo "[5] mount (interactive durability, dedicated cache dir)"
if start_mount; then
  check_eq "initial mount is mounted" "true" "true"
else
  check_eq "initial mount is mounted" "false" "true"
  exit 1
fi

echo "[6] write fsync'd workload (small files + unlink victim + large ShadowSpill)"
check_cmd "pre-crash workload written and fsync'd" write_crash_workload

echo "[7] kill -9 the mount daemon (no flush, no unmount)"
check_cmd "daemon killed with SIGKILL" crash_mount

WAL_FILE="$(wal_path)"
if [ -n "$WAL_FILE" ] && [ -s "$WAL_FILE" ]; then
  echo "INFO crash state: journal.wal present ($(wc -c <"$WAL_FILE" | tr -d ' ') bytes)"
fi
check_cmd "crash left durable local state (journal.wal exists)" test -n "$WAL_FILE"

echo "[8] remount with the same cache dir (crash recovery)"
if start_mount; then
  check_eq "recovery mount is mounted" "true" "true"
else
  check_eq "recovery mount is mounted" "false" "true"
  exit 1
fi

echo "[9] wait for recovered commits to reach the server"
check_cmd "all fsync'd files recovered remotely with exact content" wait_remote_recovered
check_cmd "unlinked file did not resurrect remotely" check_doomed_absent

echo "[10] verify read-through-mount content after recovery"
check_cmd "mount view matches expected manifest" verify_mount_manifest

echo "[11] clean unmount, then remount to verify WAL compaction"
check_cmd "clean unmount after recovery" unmount_mount
if start_mount; then
  check_eq "compaction mount is mounted" "true" "true"
  WAL_FILE="$(wal_path)"
  WAL_SIZE=0
  if [ -n "$WAL_FILE" ]; then
    WAL_SIZE="$(wc -c <"$WAL_FILE" | tr -d ' ')"
  fi
  echo "INFO post-compaction journal.wal size: $WAL_SIZE bytes"
  # All recovered paths were committed in the previous session, so mount-time
  # compaction must drop their frames. Allow slack for unrelated mount noise.
  check_cmd "journal WAL compacted after clean remount (size <= 4096)" test "$WAL_SIZE" -le 4096
  check_cmd "unmount compaction mount" unmount_mount
else
  check_eq "compaction mount is mounted" "false" "true"
fi

echo "[12] cleanup remote fixture"
drive9 fs rm -r "$ROOT_REMOTE" >/dev/null 2>&1 || true

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
