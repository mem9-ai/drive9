#!/usr/bin/env bash
# Manual-only S3 Express append-log FUSE validation. This requires a hosted
# Drive9 server backed by a real Directory Bucket and is excluded from every
# local-e2e and smoke-all workflow.

set -u

base="${DRIVE9_BASE:-}"
api_key="${DRIVE9_API_KEY:-}"
enabled="${DRIVE9_E2E_S3_EXPRESS_ENABLED:-}"
cli_bin="${DRIVE9_CLI_BIN:-drive9}"
mount_root="${FUSE_MOUNT_ROOT:-/tmp}"
mount_timeout_s="${MOUNT_READY_TIMEOUT_S:-30}"
mount_interval_s="${MOUNT_READY_INTERVAL_S:-1}"
umount_timeout="${FUSE_UMOUNT_TIMEOUT:-60s}"
keep_artifacts="${FUSE_APPEND_LOG_KEEP_ARTIFACTS:-0}"
max_remote_write_ratio="${FUSE_APPEND_LOG_MAX_REMOTE_WRITE_RATIO:-10}"
max_late_to_early_p95_ratio="${FUSE_APPEND_LOG_MAX_LATE_TO_EARLY_P95_RATIO:-4}"
stage_file="${FUSE_APPEND_LOG_STAGE_FILE:-}"
transactions="${FUSE_APPEND_LOG_TRANSACTIONS:-1000}"
wal_autocheckpoint="${FUSE_APPEND_LOG_WAL_AUTOCHECKPOINT:-1000}"
journal_size_limit="${FUSE_APPEND_LOG_JOURNAL_SIZE_LIMIT:-0}"
force_truncate_after="${FUSE_APPEND_LOG_FORCE_TRUNCATE_AFTER:-0}"
debug_mount="${FUSE_APPEND_LOG_DEBUG:-0}"
trace_only="${FUSE_APPEND_LOG_TRACE_ONLY:-0}"
record_commit_times="${FUSE_APPEND_LOG_RECORD_COMMIT_TIMES:-0}"
sqlite_busy_timeout_ms="${FUSE_APPEND_LOG_SQLITE_BUSY_TIMEOUT_MS:-}"
sqlite_strace_prefix="${FUSE_APPEND_LOG_STRACE_PREFIX:-}"
run_root=""
cli_home=""
mount_point=""
mount_pid=""
remote_root=""
success=0
mount_generation=0

require_command() {
	local name="$1"
	if ! command -v "$name" >/dev/null 2>&1; then
		printf 'FAIL: required command %s is unavailable\n' "$name" >&2
		exit 1
	fi
}

set_stage() {
	if [[ -n "$stage_file" ]]; then
		printf '%s\n' "$1" >"$stage_file" || true
	fi
}

run_cli() {
	HOME="$cli_home" XDG_CONFIG_HOME="$cli_home/xdg" DRIVE9_SERVER="$base" DRIVE9_API_KEY="$api_key" "$cli_bin" "$@"
}

is_mounted() {
	local target="$1"
	local physical
	physical="$(cd "${target%/*}" 2>/dev/null && pwd -P)/${target##*/}"
	if command -v mountpoint >/dev/null 2>&1; then
		mountpoint -q "$target"
		return
	fi
	mount | awk -v target="$target" -v physical="$physical" \
		'{for (i = 1; i <= NF; i++) if ($i == "on" && ($(i + 1) == target || $(i + 1) == physical)) found = 1} END {exit !found}'
}

wait_mount_state() {
	local wanted="$1"
	local deadline=$((SECONDS + mount_timeout_s))
	while ((SECONDS < deadline)); do
		if [[ "$wanted" == mounted ]] && is_mounted "$mount_point"; then
			return 0
		fi
		if [[ "$wanted" == unmounted ]] && ! is_mounted "$mount_point"; then
			return 0
		fi
		sleep "$mount_interval_s"
	done
	return 1
}

stop_mount() {
	if [[ -n "$mount_point" ]] && is_mounted "$mount_point"; then
		run_cli umount --timeout "$umount_timeout" "$mount_point" >/dev/null 2>&1 || true
		wait_mount_state unmounted >/dev/null 2>&1 || true
	fi
	if [[ -n "$mount_pid" ]] && kill -0 "$mount_pid" >/dev/null 2>&1; then
		kill "$mount_pid" >/dev/null 2>&1 || true
		wait "$mount_pid" >/dev/null 2>&1 || true
	fi
	mount_pid=""
}

cleanup() {
	stop_mount
	if [[ -n "$remote_root" ]] && [[ "$success" == 1 ]]; then
		run_cli fs rm -r "$remote_root" >/dev/null 2>&1 || true
	elif [[ -n "$remote_root" ]]; then
		printf 'REMOTE TEST ROOT PRESERVED: %s\n' "$remote_root" >&2
	fi
	if [[ -n "$run_root" ]] && [[ "$success" == 1 ]] && [[ "$keep_artifacts" != 1 ]]; then
		rm -rf "$run_root"
	elif [[ -n "$run_root" ]]; then
		printf 'ARTIFACTS PRESERVED: %s\n' "$run_root" >&2
	fi
}

start_mount() {
	mount_generation=$((mount_generation + 1))
	local perf_dir="$run_root/perf-$mount_generation"
	local mount_log="$run_root/mount-$mount_generation.log"
	local mount_args=(
		mount --mode=fuse --foreground --durability fsync
		--append-log '**/append-log.db-wal'
		--perf-dir "$perf_dir"
	)
	if [[ "$debug_mount" == 1 ]]; then
		mount_args+=(--debug)
	fi
	mount_args+=(":$remote_root" "$mount_point")
	if ! mkdir -p "$perf_dir"; then
		printf 'FAIL: create perf directory %s\n' "$perf_dir" >&2
		return 1
	fi
	run_cli "${mount_args[@]}" >"$mount_log" 2>&1 &
	mount_pid="$!"
	if ! wait_mount_state mounted; then
		printf 'FAIL: mount did not become ready; log follows\n' >&2
		awk '{print}' "$mount_log" >&2
		return 1
	fi
	return 0
}

verify_status_capability() {
	DRIVE9_E2E_STATUS_BASE="$base" DRIVE9_E2E_STATUS_KEY="$api_key" python3 - <<'PY'
import json
import os
import urllib.request

request = urllib.request.Request(
    os.environ["DRIVE9_E2E_STATUS_BASE"].rstrip("/") + "/v1/status",
    headers={"Authorization": "Bearer " + os.environ["DRIVE9_E2E_STATUS_KEY"]},
)
try:
    with urllib.request.urlopen(request, timeout=30) as response:
        status = json.load(response)
except Exception as exc:
    raise SystemExit(f"status request failed: {exc}")
if status.get("storage_capabilities", {}).get("append_log_v1") is not True:
    raise SystemExit("/v1/status does not advertise storage_capabilities.append_log_v1=true")
PY
}

run_sqlite_workload() {
	local database="$mount_point/append-log.db"
	local expected="$run_root/expected.json"
	local sqlite_workload_command=(python3)
	if [[ -n "$sqlite_strace_prefix" ]]; then
		local strace_syscalls="fcntl,fcntl64,flock,fsync,fdatasync,read,pread64,"
		strace_syscalls+="write,pwrite64,poll,ppoll,futex,nanosleep,clock_nanosleep,"
		strace_syscalls+="mmap,mmap2,munmap,msync,madvise"
		sqlite_workload_command=(
			strace -ff -ttt -T -yy -o "$sqlite_strace_prefix"
			-e "trace=$strace_syscalls"
			python3
		)
	fi
	"${sqlite_workload_command[@]}" - "$database" "$expected" "$transactions" "$wal_autocheckpoint" "$journal_size_limit" "$force_truncate_after" "$record_commit_times" "$sqlite_busy_timeout_ms" <<'PY'
import hashlib
import json
import os
import sqlite3
import sys
import time

database, expected, transactions, autocheckpoint, journal_size_limit, force_truncate_after, record_commit_times, busy_timeout_ms = sys.argv[1:]
transactions = int(transactions)
autocheckpoint = int(autocheckpoint)
journal_size_limit = int(journal_size_limit)
force_truncate_after = int(force_truncate_after)
record_commit_times = record_commit_times == "1"
busy_timeout_ms = int(busy_timeout_ms) if busy_timeout_ms else None
conn = sqlite3.connect(database, isolation_level=None)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=FULL")
if busy_timeout_ms is not None:
    conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
conn.execute(f"PRAGMA wal_autocheckpoint={autocheckpoint}")
conn.execute(f"PRAGMA journal_size_limit={journal_size_limit}")
if conn.execute("PRAGMA journal_size_limit").fetchone()[0] != journal_size_limit:
    raise SystemExit("journal_size_limit did not apply")
conn.execute("CREATE TABLE IF NOT EXISTS entries (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
cumulative_wal_size = 0
commit_seconds = []
commit_timeline = []
checkpoints = []
for index in range(1, transactions + 1):
    wal_size_before = os.path.getsize(database + "-wal")
    started_wall_ns = time.time_ns()
    started = time.perf_counter()
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("INSERT INTO entries(id, value) VALUES (?, ?)", (index, f"value-{index:04d}"))
    conn.execute("COMMIT")
    commit_seconds.append(time.perf_counter() - started)
    ended_wall_ns = time.time_ns()
    wal_size_after = os.path.getsize(database + "-wal")
    cumulative_wal_size += wal_size_after
    if record_commit_times:
        commit_timeline.append({
            "commit": index,
            "start_unix_nano": started_wall_ns,
            "end_unix_nano": ended_wall_ns,
            "duration_ms": commit_seconds[-1] * 1000,
            "wal_size_before": wal_size_before,
            "wal_size_after": wal_size_after,
        })
    if force_truncate_after and index == force_truncate_after:
        checkpoints.append({
            "after_commit": index,
            "result": list(conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()),
        })
rows = list(conn.execute("SELECT id, value FROM entries ORDER BY id"))
fingerprint = hashlib.sha256(repr(rows).encode()).hexdigest()
wal_size = os.path.getsize(database + "-wal")
if wal_size <= 0:
    raise SystemExit("WAL did not grow positively")
check = conn.execute("PRAGMA integrity_check").fetchone()[0]
if check != "ok":
    raise SystemExit(f"integrity_check={check}")
conn.close()

def percentile(values, fraction):
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * fraction + 0.999999) - 1))
    return ordered[index]

early = commit_seconds[:100]
late = commit_seconds[-100:]
with open(expected, "w", encoding="utf-8") as handle:
    json.dump({
        "count": len(rows),
        "fingerprint": fingerprint,
        "wal_size": wal_size,
        "cumulative_wal_size": cumulative_wal_size,
        "wal_autocheckpoint": autocheckpoint,
        "journal_size_limit": journal_size_limit,
        "sqlite_busy_timeout_ms": busy_timeout_ms,
        "explicit_checkpoints": checkpoints,
        "early_commit_p95_ms": percentile(early, 0.95) * 1000,
        "late_commit_p95_ms": percentile(late, 0.95) * 1000,
        "commit_ms": [value * 1000 for value in commit_seconds] if record_commit_times else [],
        "commit_timeline": commit_timeline if record_commit_times else [],
    }, handle)
PY
}

verify_sqlite() {
	local database="$mount_point/append-log.db"
	local expected="$run_root/expected.json"
	python3 - "$database" "$expected" <<'PY'
import hashlib
import json
import sqlite3
import sys

database, expected = sys.argv[1:]
with open(expected, encoding="utf-8") as handle:
    want = json.load(handle)
conn = sqlite3.connect(database)
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
PY
}

verify_perf() {
	local perf_file="$run_root/perf-1/perf.jsonl"
	if [[ ! -s "$perf_file" ]]; then
		printf 'FAIL: missing perf sample %s\n' "$perf_file" >&2
		return 1
	fi
	python3 - "$perf_file" "$run_root/expected.json" "$max_remote_write_ratio" "$max_late_to_early_p95_ratio" <<'PY'
import json
import sys

perf_path, expected_path, max_remote_ratio, max_latency_ratio = sys.argv[1:]
with open(expected_path, encoding="utf-8") as handle:
    expected = json.load(handle)
with open(perf_path, encoding="utf-8") as handle:
    lines = [line for line in handle.read().splitlines() if line]
if not lines:
    raise SystemExit("missing FUSE perf sample")
sample = json.loads(lines[-1])
append = sample.get("remote_ops", {}).get("append_log", {})
write = sample.get("remote_ops", {}).get("write", {})
counters = sample.get("counters", {})
append_count = int(append.get("count", 0))
append_bytes = int(append.get("bytes", 0))
full_rewrite_bytes = int(counters.get("append_log_full_rewrite_bytes", 0))
full_rewrites = int(counters.get("append_log_full_rewrite_count", 0))
generation_reset_bytes = int(counters.get("append_log_generation_reset_bytes", 0))
generation_resets = int(counters.get("append_log_generation_reset_count", 0))
generation_reset_shadow_ready = int(counters.get("append_log_generation_reset_shadow_ready", 0))
generation_reset_shadow_degraded = int(counters.get("append_log_generation_reset_shadow_degraded", 0))
remote_write_bytes = append_bytes + full_rewrite_bytes
append_fsync_count = int(counters.get("append_log_fsync_append_count", 0))
append_fsync_total_ns = int(counters.get("append_log_fsync_append_total_ns", 0))
append_fsync_max_ns = int(counters.get("append_log_fsync_append_max_ns", 0))
rewrite_fsync_count = int(counters.get("append_log_fsync_full_rewrite_count", 0))
rewrite_fsync_total_ns = int(counters.get("append_log_fsync_full_rewrite_total_ns", 0))
rewrite_fsync_max_ns = int(counters.get("append_log_fsync_full_rewrite_max_ns", 0))

def average_millis(total_ns, count):
    return total_ns / count / 1_000_000 if count else 0

metrics = {
    "wal_size": expected["wal_size"],
    "cumulative_wal_size": expected["cumulative_wal_size"],
    "append_count": append_count,
    "append_bytes": append_bytes,
    "append_errors": int(append.get("errors", 0)),
    "full_rewrite_count": full_rewrites,
    "full_rewrite_bytes": full_rewrite_bytes,
    "generation_reset_count": generation_resets,
    "generation_reset_bytes": generation_reset_bytes,
    "generation_reset_shadow_ready": generation_reset_shadow_ready,
    "generation_reset_shadow_degraded": generation_reset_shadow_degraded,
    "ordinary_full_rewrite_count": full_rewrites - generation_resets,
    "remote_write_bytes": remote_write_bytes,
    "append_fsync_count": append_fsync_count,
    "append_fsync_avg_ms": average_millis(append_fsync_total_ns, append_fsync_count),
    "append_fsync_max_ms": append_fsync_max_ns / 1_000_000,
    "full_rewrite_fsync_count": rewrite_fsync_count,
    "full_rewrite_fsync_avg_ms": average_millis(rewrite_fsync_total_ns, rewrite_fsync_count),
    "full_rewrite_fsync_max_ms": rewrite_fsync_max_ns / 1_000_000,
    "generic_write_count": int(write.get("count", 0)),
    "generic_write_bytes": int(write.get("bytes", 0)),
    "generic_write_errors": int(write.get("errors", 0)),
    "append_outcome_success": int(counters.get("append_log_outcome_success", 0)),
    "append_outcome_rebased": int(counters.get("append_log_outcome_rebased", 0)),
    "append_outcome_conflict": int(counters.get("append_log_outcome_conflict", 0)),
    "append_outcome_unsupported": int(counters.get("append_log_outcome_unsupported", 0)),
    "append_outcome_too_large": int(counters.get("append_log_outcome_too_large", 0)),
    "append_outcome_error": int(counters.get("append_log_outcome_error", 0)),
    "append_rebase_retry_count": int(counters.get("append_log_rebase_retry_count", 0)),
    "early_commit_p95_ms": expected["early_commit_p95_ms"],
    "late_commit_p95_ms": expected["late_commit_p95_ms"],
}

def fail(reason):
    raise SystemExit(f"{reason}; metrics={json.dumps(metrics, sort_keys=True, separators=(',', ':'))}")

if append_count < 1 or append_bytes < 1:
    fail("append-log perf counters are empty")
if remote_write_bytes > expected["wal_size"] * int(max_remote_ratio):
    fail("logical remote bytes exceed the final WAL bound")
if remote_write_bytes >= expected["cumulative_wal_size"]:
    fail("logical remote bytes scale with cumulative WAL observations")
if generation_resets > full_rewrites:
    fail("generation-reset counters exceed conditional full-PUT count")
if full_rewrites-generation_resets > 1:
    fail("ordinary append-log full rewrites exceed one")
if expected["early_commit_p95_ms"] <= 0 or expected["late_commit_p95_ms"] > expected["early_commit_p95_ms"] * float(max_latency_ratio):
    fail("post-threshold commit latency knee remains")
PY
}

if [[ -z "$base" || -z "$api_key" || "$enabled" != 1 ]]; then
	printf '%s\n' 'SKIP: requires DRIVE9_BASE, DRIVE9_API_KEY, and DRIVE9_E2E_S3_EXPRESS_ENABLED=1'
	exit 0
fi

if ! [[ "$transactions" =~ ^[1-9][0-9]*$ ]]; then
	printf 'FAIL: FUSE_APPEND_LOG_TRANSACTIONS must be a positive integer\n' >&2
	exit 1
fi
if ! [[ "$wal_autocheckpoint" =~ ^[0-9]+$ ]]; then
	printf 'FAIL: FUSE_APPEND_LOG_WAL_AUTOCHECKPOINT must be a non-negative integer\n' >&2
	exit 1
fi
if ! [[ "$journal_size_limit" =~ ^-?[0-9]+$ ]]; then
	printf 'FAIL: FUSE_APPEND_LOG_JOURNAL_SIZE_LIMIT must be an integer\n' >&2
	exit 1
fi
if ! [[ "$force_truncate_after" =~ ^[0-9]+$ ]]; then
	printf 'FAIL: FUSE_APPEND_LOG_FORCE_TRUNCATE_AFTER must be a non-negative integer\n' >&2
	exit 1
fi
if [[ "$record_commit_times" != 0 && "$record_commit_times" != 1 ]]; then
	printf 'FAIL: FUSE_APPEND_LOG_RECORD_COMMIT_TIMES must be 0 or 1\n' >&2
	exit 1
fi
if [[ -n "$sqlite_busy_timeout_ms" ]] && ! [[ "$sqlite_busy_timeout_ms" =~ ^[0-9]+$ ]]; then
	printf 'FAIL: FUSE_APPEND_LOG_SQLITE_BUSY_TIMEOUT_MS must be a non-negative integer\n' >&2
	exit 1
fi

set_stage preflight
require_command "$cli_bin"
require_command python3
if [[ -n "$sqlite_strace_prefix" ]]; then
	require_command strace
fi
if ! python3 -c 'import sqlite3' >/dev/null 2>&1; then
	printf '%s\n' 'FAIL: python3 sqlite3 module is required' >&2
	exit 1
fi

if ! mkdir -p "$mount_root"; then
	printf 'FAIL: create mount root %s\n' "$mount_root" >&2
	exit 1
fi
if ! run_root="$(mktemp -d "$mount_root/drive9-append-log.XXXXXX")"; then
	printf 'FAIL: create test artifact directory below %s\n' "$mount_root" >&2
	exit 1
fi
mount_point="$run_root/mount"
cli_home="$run_root/cli-home"
remote_root="${FUSE_APPEND_LOG_REMOTE_ROOT:-/e2e-s3-express-append-log-${RANDOM}-${RANDOM}}"
if ! mkdir -p "$mount_point"; then
	printf 'FAIL: create mount point %s\n' "$mount_point" >&2
	exit 1
fi
if ! mkdir -p "$cli_home"; then
	printf 'FAIL: create isolated CLI home %s\n' "$cli_home" >&2
	exit 1
fi
if ! mkdir -p "$cli_home/xdg"; then
	printf 'FAIL: create isolated CLI config root %s\n' "$cli_home/xdg" >&2
	exit 1
fi
trap cleanup EXIT INT TERM

set_stage capability
if ! verify_status_capability; then
	exit 1
fi
set_stage remote-root-create
if ! run_cli fs mkdir "$remote_root" >/dev/null; then
	printf 'FAIL: create remote root %s\n' "$remote_root" >&2
	exit 1
fi
set_stage remote-root-created
set_stage first-mount
if ! start_mount; then
	exit 1
fi
set_stage sqlite-workload
if ! run_sqlite_workload; then
	exit 1
fi
set_stage same-mount-reopen
if ! verify_sqlite; then
	exit 1
fi
stop_mount
set_stage fresh-remount
if ! start_mount; then
	exit 1
fi
set_stage fresh-remount-verify
if ! verify_sqlite; then
	exit 1
fi
if [[ "$trace_only" != 1 ]]; then
	set_stage perf-validation
	if ! verify_perf; then
		exit 1
	fi
fi

set_stage complete
printf '%s\n' 'PASS: S3 Express append-log FUSE WAL transactions, reopen, and remount verification'
printf '%s\n' "ARTIFACTS: $run_root/perf-1, $run_root/mount-1.log, and $run_root/mount-2.log"
success=1
