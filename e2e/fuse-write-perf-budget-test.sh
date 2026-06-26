#!/usr/bin/env bash
# drive9 FUSE write-path performance-regression gate.
#
# Runs a deterministic fsync-heavy write workload (creates + overwrites +
# renames + one medium file) through an interactive-durability mount with
# --perf-dir, waits for remote convergence, then asserts remote-op budgets from
# the perf summary printed at clean unmount:
#   - remote write ops scale with committed files, not with write()/fsync()
#     syscalls (catches upload amplification)
#   - remote stat/list ops stay bounded (catches per-write metadata storms)
#   - remote mutation ops scale with renames (catches rename storms)
#   - commit queue reports no enqueue errors / failures and few retries
#   - fsync stays local-fast (catches fsync blocking on remote round trips)
#
# Budgets are op-count based on purpose: wall-clock thresholds flake on shared
# infrastructure, while op counts are deterministic for a fixed workload and
# blow past any sane budget by an order of magnitude when a storm regresses.

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

WRITE_PERF_FILES="${WRITE_PERF_FILES:-64}"
WRITE_PERF_FILE_KB="${WRITE_PERF_FILE_KB:-64}"
WRITE_PERF_OVERWRITES="${WRITE_PERF_OVERWRITES:-32}"
WRITE_PERF_RENAMES="${WRITE_PERF_RENAMES:-16}"
WRITE_PERF_MEDIUM_MB="${WRITE_PERF_MEDIUM_MB:-8}"
WRITE_PERF_CONVERGE_TIMEOUT_S="${WRITE_PERF_CONVERGE_TIMEOUT_S:-240}"
WRITE_PERF_CONVERGE_INTERVAL_S="${WRITE_PERF_CONVERGE_INTERVAL_S:-2}"
WRITE_PERF_KEEP_ARTIFACTS="${WRITE_PERF_KEEP_ARTIFACTS:-0}"

# Budgets. Write ops scale with committed payloads (multipart uploads count
# several ops per file, hence the generous per-commit factor). Stat/list also
# get a time-scaled term because background invalidation (SSE, TTL refresh)
# legitimately scales with uptime, like the git-workspace refresh budget.
WRITE_PERF_WRITE_BUDGET_BASE="${WRITE_PERF_WRITE_BUDGET_BASE:-20}"
WRITE_PERF_WRITE_BUDGET_PER_COMMIT="${WRITE_PERF_WRITE_BUDGET_PER_COMMIT:-4}"
WRITE_PERF_STAT_BUDGET_BASE="${WRITE_PERF_STAT_BUDGET_BASE:-50}"
WRITE_PERF_STAT_BUDGET_PER_FILE="${WRITE_PERF_STAT_BUDGET_PER_FILE:-8}"
WRITE_PERF_STAT_BUDGET_PER_SEC="${WRITE_PERF_STAT_BUDGET_PER_SEC:-5}"
WRITE_PERF_LIST_BUDGET_BASE="${WRITE_PERF_LIST_BUDGET_BASE:-30}"
WRITE_PERF_LIST_BUDGET_PER_SEC="${WRITE_PERF_LIST_BUDGET_PER_SEC:-3}"
WRITE_PERF_MUTATION_BUDGET_BASE="${WRITE_PERF_MUTATION_BUDGET_BASE:-20}"
WRITE_PERF_MUTATION_BUDGET_PER_RENAME="${WRITE_PERF_MUTATION_BUDGET_PER_RENAME:-4}"
WRITE_PERF_COMMIT_RETRY_BUDGET="${WRITE_PERF_COMMIT_RETRY_BUDGET:-5}"
WRITE_PERF_FSYNC_AVG_BUDGET_MS="${WRITE_PERF_FSYNC_AVG_BUDGET_MS:-150}"

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
    echo "=== drive9 write-perf mount start time=$(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "cache_dir=$CACHE_DIR"
    echo "remote_root=$ROOT_REMOTE"
  } >>"$MOUNT_LOG"
  # --foreground keeps the daemon as our child so its stderr (the perf
  # summary) lands in MOUNT_LOG; plain `drive9 mount` daemonizes.
  local perf_dir="$RUN_ROOT/perf"
  drive9 mount --foreground --cache-dir "$CACHE_DIR" --durability interactive \
    --perf-dir "$perf_dir" \
    --perf-interval 1h \
    --perf-cpu-duration 1ms \
    --perf-cpu-interval 1h \
    --perf-heap-interval 1h \
    --perf-max-sample-files 1 \
    --perf-max-profile-files 1 \
    ":$ROOT_REMOTE" "$MOUNT_POINT" >>"$MOUNT_LOG" 2>&1 &
  MOUNT_PID="$!"
  if wait_mount_state mounted; then
    return 0
  fi
  cat "$MOUNT_LOG" >&2 || true
  return 1
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

# write_perf_workload performs the deterministic write workload through the
# mount and records the expected final-state sha256 manifest:
#   - WRITE_PERF_FILES files of WRITE_PERF_FILE_KB KiB, write+fsync each
#   - first WRITE_PERF_OVERWRITES files overwritten with new content + fsync
#   - next WRITE_PERF_RENAMES files renamed (exercises pending rename)
#   - one WRITE_PERF_MEDIUM_MB MiB file
write_perf_workload() {
  python3 - "$WORK_MOUNT" "$EXPECTED_MANIFEST" \
    "$WRITE_PERF_FILES" "$WRITE_PERF_FILE_KB" \
    "$WRITE_PERF_OVERWRITES" "$WRITE_PERF_RENAMES" "$WRITE_PERF_MEDIUM_MB" <<'PY'
import hashlib
import json
import os
import sys

root, manifest_path = sys.argv[1], sys.argv[2]
n_files, file_kb = int(sys.argv[3]), int(sys.argv[4])
n_overwrites, n_renames, medium_mb = int(sys.argv[5]), int(sys.argv[6]), int(sys.argv[7])
if n_overwrites + n_renames > n_files:
    raise SystemExit("overwrites + renames must fit in file count")

os.makedirs(root, exist_ok=True)
manifest = {}

def payload(name, size):
    seed = hashlib.sha256(name.encode()).digest()
    out = bytearray()
    counter = 0
    while len(out) < size:
        out.extend(hashlib.sha256(seed + counter.to_bytes(8, "big")).digest())
        counter += 1
    return bytes(out[:size])

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

size = file_kb * 1024
for i in range(n_files):
    rel = f"small-{i:03d}.bin"
    write_fsync(rel, payload(rel, size))

for i in range(n_overwrites):
    rel = f"small-{i:03d}.bin"
    write_fsync(rel, payload(rel + "-v2", size))

for i in range(n_overwrites, n_overwrites + n_renames):
    old = f"small-{i:03d}.bin"
    new = f"renamed-{i:03d}.bin"
    os.rename(os.path.join(root, old), os.path.join(root, new))
    manifest[new] = manifest.pop(old)

write_fsync("medium.bin", payload("medium.bin", medium_mb << 20))

with open(manifest_path, "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

# wait_remote_converged polls until every manifest entry is readable remotely
# with the exact expected sha256 (via drive9 fs cat).
wait_remote_converged() {
  local deadline=$(( $(date +%s) + WRITE_PERF_CONVERGE_TIMEOUT_S ))
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
    sleep "$WRITE_PERF_CONVERGE_INTERVAL_S"
  done
}

# check_write_perf_budgets parses the perf summary that --perf-dir enables
# at clean unmount and enforces op-count budgets. Ops absent from the summary
# have count 0 (printSummary skips zero-count ops).
check_write_perf_budgets() {
  local log_file="$1"
  local out rc
  set +e
  out=$(python3 - "$log_file" \
    "$WRITE_PERF_FILES" "$WRITE_PERF_OVERWRITES" "$WRITE_PERF_RENAMES" \
    "$WRITE_PERF_WRITE_BUDGET_BASE" "$WRITE_PERF_WRITE_BUDGET_PER_COMMIT" \
    "$WRITE_PERF_STAT_BUDGET_BASE" "$WRITE_PERF_STAT_BUDGET_PER_FILE" "$WRITE_PERF_STAT_BUDGET_PER_SEC" \
    "$WRITE_PERF_LIST_BUDGET_BASE" "$WRITE_PERF_LIST_BUDGET_PER_SEC" \
    "$WRITE_PERF_MUTATION_BUDGET_BASE" "$WRITE_PERF_MUTATION_BUDGET_PER_RENAME" \
    "$WRITE_PERF_COMMIT_RETRY_BUDGET" "$WRITE_PERF_FSYNC_AVG_BUDGET_MS" <<'PY'
import re
import sys

(log_path,
 n_files, n_overwrites, n_renames,
 write_base, write_per_commit,
 stat_base, stat_per_file, stat_per_sec,
 list_base, list_per_sec,
 mut_base, mut_per_rename,
 retry_budget, fsync_avg_budget_ms) = sys.argv[1], *map(int, sys.argv[2:])

UNIT_SECONDS = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 1e-3, "µs": 1e-6, "us": 1e-6, "ns": 1e-9}

def parse_duration(text):
    total = 0.0
    for m in re.finditer(r"([0-9.]+)(h|ms|m|s|µs|us|ns)", text):
        total += float(m.group(1)) * UNIT_SECONDS[m.group(2)]
    return total

uptime = None
remote = {}
fuse = {}
commit = None
with open(log_path, "r", errors="replace") as handle:
    for line in handle:
        m = re.search(r"drive9: FUSE perf summary uptime=(\S+)", line)
        if m:
            uptime = parse_duration(m.group(1))
            remote.clear()
            fuse.clear()
            commit = None
            continue
        m = re.search(r"drive9: perf (remote|fuse) (\S+) count=(\d+) errors=(\d+) bytes=(\d+) avg=(\S+)", line)
        if m:
            group, name = m.group(1), m.group(2)
            stats = {"count": int(m.group(3)), "errors": int(m.group(4)),
                     "avg_s": parse_duration(m.group(6))}
            (remote if group == "remote" else fuse)[name] = stats
            continue
        m = re.search(r"drive9: perf commit enqueue=(\d+) enqueue_errors=(\d+) retries=(\d+) success=(\d+) failure=(\d+)", line)
        if m:
            commit = {"enqueue": int(m.group(1)), "enqueue_errors": int(m.group(2)),
                      "retries": int(m.group(3)), "success": int(m.group(4)),
                      "failure": int(m.group(5))}

if uptime is None or commit is None:
    print(f"perf summary not found in {log_path} (uptime={uptime} commit={commit})")
    raise SystemExit(2)

def count(table, name):
    return table.get(name, {"count": 0})["count"]

seconds = int(uptime) + 1
expected_commits = n_files + n_overwrites + 1  # +1 medium file
write_budget = write_base + write_per_commit * expected_commits
stat_budget = stat_base + stat_per_file * (n_files + 1) + stat_per_sec * seconds
list_budget = list_base + list_per_sec * seconds
mut_budget = mut_base + mut_per_rename * n_renames

failures = []

def budget(name, got, limit):
    line = f"{name}={got}/{limit}"
    if got > limit:
        failures.append(line)
    return line

report = [
    f"uptime={uptime:.1f}s",
    budget("remote_write", count(remote, "write"), write_budget),
    budget("remote_stat", count(remote, "stat"), stat_budget),
    budget("remote_list", count(remote, "list"), list_budget),
    budget("remote_mutation", count(remote, "mutation"), mut_budget),
    budget("commit_retries", commit["retries"], retry_budget),
    budget("commit_enqueue_errors", commit["enqueue_errors"], 0),
    budget("commit_failures", commit["failure"], 0),
]

fsync_avg_ms = fuse.get("fsync", {"avg_s": 0.0})["avg_s"] * 1000.0
report.append(budget("fsync_avg_ms", int(fsync_avg_ms), fsync_avg_budget_ms))

print(" ".join(report))
raise SystemExit(1 if failures else 0)
PY
  )
  rc=$?
  set -e
  TOTAL=$((TOTAL + 1))
  if [ "$rc" -eq 0 ]; then
    echo "PASS write-path perf budgets ($out)"
    PASS=$((PASS + 1))
  else
    echo "FAIL write-path perf budgets ($out)" >&2
    FAIL=$((FAIL + 1))
  fi
}

echo "=== drive9 FUSE write-path perf budget test ==="
echo "BASE=$BASE"
echo "WRITE_PERF_FILES=$WRITE_PERF_FILES WRITE_PERF_FILE_KB=$WRITE_PERF_FILE_KB WRITE_PERF_OVERWRITES=$WRITE_PERF_OVERWRITES WRITE_PERF_RENAMES=$WRITE_PERF_RENAMES WRITE_PERF_MEDIUM_MB=$WRITE_PERF_MEDIUM_MB"

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
RUN_ROOT="$(mktemp -d "$FUSE_MOUNT_ROOT/drive9-fuse-write-perf-${TS}.XXXXXX")"
RUN_ID="$(basename "$RUN_ROOT")"
MOUNT_POINT="$RUN_ROOT/mount"
MOUNT_LOG="$RUN_ROOT/mount.log"
CACHE_DIR="$RUN_ROOT/cache"
CTX_HOME="$RUN_ROOT/ctx-home"
EXPECTED_MANIFEST="$RUN_ROOT/expected-manifest.json"
ROOT_REL="$RUN_ID"
ROOT_REMOTE="/$ROOT_REL"
# Mount only this test's remote root so fixtures from earlier local-e2e
# suites cannot trigger cache hydration or inflate write-path perf counters.
WORK_MOUNT="$MOUNT_POINT/work"
WORK_REMOTE="$ROOT_REMOTE/work"
MOUNT_PID=""

mkdir -p "$MOUNT_POINT" "$CACHE_DIR" "$CTX_HOME"
: > "$MOUNT_LOG"

cleanup() {
  local rc=$?
  stop_mount
  force_unmount_stale 2>/dev/null || true
  if [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
  if [ "$rc" -eq 0 ] && [ "$FAIL" -eq 0 ] && [ "$WRITE_PERF_KEEP_ARTIFACTS" != "1" ]; then
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
check_eq "remote write-perf root" "$ROOT_REMOTE" "$ROOT_REMOTE"

echo "[5] mount (interactive durability, perf dir)"
if start_mount; then
  check_eq "mount is mounted" "true" "true"
else
  check_eq "mount is mounted" "false" "true"
  exit 1
fi

echo "[6] run fsync-heavy write workload"
WORKLOAD_START="$(date +%s)"
check_cmd "write workload completes" write_perf_workload
WORKLOAD_SECONDS=$(( $(date +%s) - WORKLOAD_START ))
echo "INFO workload wall time: ${WORKLOAD_SECONDS}s"

echo "[7] wait for remote convergence"
check_cmd "all files converged remotely with exact content" wait_remote_converged

echo "[8] clean unmount (flushes perf summary)"
check_cmd "clean unmount" unmount_mount

echo "[9] enforce write-path perf budgets"
check_write_perf_budgets "$MOUNT_LOG"

echo "[10] cleanup remote fixture"
drive9 fs rm -r "$ROOT_REMOTE" >/dev/null 2>&1 || true

echo "RESULT: $PASS/$TOTAL passed, $FAIL failed"
exit "$FAIL"
