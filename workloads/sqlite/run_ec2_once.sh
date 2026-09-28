#!/usr/bin/env bash
# Run one bounded 10 MiB SQLite WAL transaction on a pinned Drive9 binary.
# The runner captures authority bytes before close, after close, and unmounted.
# DRIVE9_API_KEY_FILE must name a mode-0600 file and is never printed.
# The unique remote root is retained for analysis and explicit later cleanup.

set -u
umask 077

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || exit 2
harness="$script_dir/sqlite_wal_single_txn.py"
authority="${DRIVE9_AUTHORITY_HELPER:-$script_dir/authority_http.py}"
drive9_bin="${DRIVE9_BIN:?DRIVE9_BIN is required}"
credential_file="${DRIVE9_API_KEY_FILE:?DRIVE9_API_KEY_FILE is required}"
server="${DRIVE9_SERVER:-https://drive9.example.invalid}"
run_base="${DRIVE9_RUN_BASE:-/home/ubuntu/drive9-test}"
rows="${SQLITE_ROWS:-2560}"

mount_pid=""
workload_pid=""
mount_dir=""
release_file=""
normal_exit=false

with_credential() (
	local drive9_api_key=""

	IFS= read -r drive9_api_key <"$credential_file" || {
		[[ -n "$drive9_api_key" ]] || return 2
	}
	[[ -n "$drive9_api_key" ]] || return 2
	export DRIVE9_API_KEY="$drive9_api_key"
	export DRIVE9_SERVER="$server"
	"$@"
)

mount_is_active() {
	[[ -n "$mount_dir" ]] || return 1
	findmnt -rn --mountpoint "$mount_dir" >/dev/null 2>&1
}

cleanup() {
	local exit_code=$?

	trap - EXIT INT TERM
	if [[ -n "$release_file" ]]; then
		touch "$release_file" 2>/dev/null || true
	fi
	if [[ -n "$workload_pid" ]] && kill -0 "$workload_pid" 2>/dev/null; then
		kill "$workload_pid" 2>/dev/null || true
		wait "$workload_pid" 2>/dev/null || true
	fi
	if mount_is_active; then
		timeout --signal=TERM --kill-after=3s 35s \
			"$drive9_bin" umount --timeout 30s "$mount_dir" \
			>/dev/null 2>&1 || true
	fi
	if [[ -n "$mount_pid" ]] && kill -0 "$mount_pid" 2>/dev/null; then
		kill "$mount_pid" 2>/dev/null || true
		wait "$mount_pid" 2>/dev/null || true
	fi
	if [[ "$normal_exit" == true ]]; then
		exit "$exit_code"
	fi
	exit 2
}

trap cleanup EXIT
trap 'exit 130' INT TERM

for command_name in date findmnt mkdir mktemp python3 sha256sum timeout; do
	if ! command -v "$command_name" >/dev/null 2>&1; then
		printf 'BROKEN: missing command %s\n' "$command_name" >&2
		exit 2
	fi
done
if [[ ! -x "$drive9_bin" || ! -r "$credential_file" || \
	! -f "$harness" || ! -f "$authority" ]]; then
	printf '%s\n' 'BROKEN: binary, credential, or helper is unavailable' >&2
	exit 2
fi
if [[ ! "$rows" =~ ^[1-9][0-9]*$ ]]; then
	printf '%s\n' 'BROKEN: SQLITE_ROWS must be a positive integer' >&2
	exit 2
fi

mkdir -p "$run_base" || exit 2
timestamp=$(date -u +%Y%m%dT%H%M%SZ) || exit 2
run_dir=$(mktemp -d "$run_base/pr901-fsync-wal10m-$timestamp.XXXXXX") || exit 2
run_name=$(basename -- "$run_dir")
remote_root="/$run_name"
mount_dir="$run_dir/mount"
cache_dir="$run_dir/cache"
perf_dir="$run_dir/perf"
evidence_dir="$run_dir/evidence"
ready_file="$run_dir/workload.ready"
release_file="$run_dir/workload.release"
result_file="$run_dir/result.json"
timeline_file="$run_dir/timeline.jsonl"
remote_db="$remote_root/plain/kv-wal-10m.db"
local_db="$mount_dir/plain/kv-wal-10m.db"

mkdir -p "$mount_dir" "$cache_dir" "$perf_dir" "$evidence_dir" || exit 2
printf '%s\n' "$remote_root" >"$run_dir/remote-root.txt"
printf '%s\n' "$remote_db" >"$run_dir/remote-db.txt"
date -u +%Y-%m-%dT%H:%M:%SZ >"$run_dir/run-start-utc.txt"
sha256sum "$drive9_bin" "$harness" "$authority" >"$run_dir/input.sha256"
"$drive9_bin" version >"$run_dir/drive9-version.txt" 2>&1 || exit 2
with_credential python3 "$authority" --timeout 20 status \
	--output "$run_dir/server-status.json" || exit 2
with_credential timeout --signal=TERM --kill-after=3s 30s \
	"$drive9_bin" fs mkdir ":$remote_root" \
	>"$run_dir/remote-mkdir.log" 2>&1 || exit 2

printf 'run_dir=%s\nremote_root=%s\n' "$run_dir" "$remote_root"
printf '%s\n' 'starting pinned fsync mount'
with_credential "$drive9_bin" mount \
	--server="$server" \
	--mode=fuse \
	--foreground \
	--profile=none \
	--durability=fsync \
	'--append-log=**/*-wal' \
	'--append-log=**/*-journal' \
	--write-cache-size-mb=8192 \
	--writeback-batch-window=20ms \
	--attr-ttl=30s \
	--entry-ttl=30s \
	--dir-ttl=30s \
	--debug \
	--cache-dir="$cache_dir" \
	--perf-dir="$perf_dir" \
	--perf-interval=1s \
	--perf-cpu-duration=1s \
	--perf-cpu-interval=24h \
	--perf-heap-interval=24h \
	":$remote_root" "$mount_dir" \
	>"$run_dir/mount.log" 2>&1 &
mount_pid=$!
printf '%s\n' "$mount_pid" >"$run_dir/mount.pid"

mounted=false
for ((attempt = 0; attempt < 300; attempt++)); do
	if mount_is_active; then
		mounted=true
		break
	fi
	if ! kill -0 "$mount_pid" 2>/dev/null; then
		break
	fi
	sleep 0.1
done
if [[ "$mounted" != true ]]; then
	printf '%s\n' 'FAIL: mount did not become active' >&2
	exit 1
fi

printf 'running rows=%s with SQLite synchronous=FULL\n' "$rows"
timeout --signal=TERM --kill-after=10s 360s \
	python3 "$harness" \
	--db "$local_db" \
	--rows "$rows" \
	--result "$result_file" \
	--timeline "$timeline_file" \
	--ready "$ready_file" \
	--release "$release_file" \
	--hold-seconds 90 \
	>"$run_dir/workload.log" 2>&1 &
workload_pid=$!

ready=false
for ((attempt = 0; attempt < 3300; attempt++)); do
	if [[ -f "$ready_file" ]]; then
		ready=true
		break
	fi
	if ! kill -0 "$workload_pid" 2>/dev/null; then
		break
	fi
	if ((attempt > 0 && attempt % 300 == 0)); then
		printf 'workload still running at %ss\n' "$((attempt / 10))"
	fi
	sleep 0.1
done
if [[ "$ready" != true ]]; then
	printf '%s\n' 'FAIL: workload did not reach the before-close gate' >&2
	touch "$release_file"
	wait "$workload_pid" || true
	workload_pid=""
	exit 1
fi

snapshot_authority() {
	local label="$1"
	local object_label=""
	local object_path=""

	for object_label in main wal; do
		object_path="$remote_db"
		if [[ "$object_label" == wal ]]; then
			object_path="${remote_db}-wal"
		fi
		if with_credential python3 "$authority" --timeout 30 stat \
			--path "$object_path" \
			--output "$evidence_dir/$label-$object_label-stat.json" \
			>>"$run_dir/authority.log" 2>&1; then
			with_credential python3 "$authority" --timeout 90 download \
				--path "$object_path" \
				--destination "$evidence_dir/$label-$object_label.bin" \
				--expect-stat "$evidence_dir/$label-$object_label-stat.json" \
				--output "$evidence_dir/$label-$object_label-download.json" \
				>>"$run_dir/authority.log" 2>&1 || true
		fi
	done
}

printf '%s\n' 'capturing authority before close'
snapshot_authority before-close
touch "$release_file"
wait "$workload_pid"
workload_rc=$?
workload_pid=""
printf 'workload_rc=%s\n' "$workload_rc" >"$run_dir/workload.rc"

printf '%s\n' 'capturing authority after close'
snapshot_authority after-close
timeout --signal=TERM --kill-after=5s 70s \
	"$drive9_bin" umount --timeout 60s "$mount_dir" \
	>"$run_dir/umount.log" 2>&1
umount_rc=$?
printf 'umount_rc=%s\n' "$umount_rc" >"$run_dir/umount.rc"
for ((attempt = 0; attempt < 150; attempt++)); do
	mount_is_active || break
	sleep 0.1
done
if kill -0 "$mount_pid" 2>/dev/null; then
	wait "$mount_pid" || true
fi
mount_pid=""

printf '%s\n' 'capturing authority after unmount'
snapshot_authority unmounted
date -u +%Y-%m-%dT%H:%M:%SZ >"$run_dir/run-end-utc.txt"
sha256sum "$evidence_dir"/*.bin >"$run_dir/authority-bytes.sha256" \
	2>/dev/null || true
printf 'finished: workload_rc=%s umount_rc=%s evidence=%s\n' \
	"$workload_rc" "$umount_rc" "$run_dir"
normal_exit=true
exit "$workload_rc"
