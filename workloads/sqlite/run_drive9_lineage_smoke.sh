#!/usr/bin/env bash
# Run one bounded real-FUSE same-handle growth smoke test on Drive9.
# The test stages 512 KiB, immediately grows it to 768 KiB, and fsyncs both.
# Credentials are read from a mode-0600 file and are never printed.
# The unique remote root is removed after authoritative byte verification.

set -u
umask 077

drive9_bin="${DRIVE9_BIN:?DRIVE9_BIN is required}"
credential_file="${DRIVE9_API_KEY_FILE:?DRIVE9_API_KEY_FILE is required}"
harness="${DRIVE9_GROWTH_HARNESS:?DRIVE9_GROWTH_HARNESS is required}"
server="${DRIVE9_SERVER:-https://drive9.example.invalid}"
run_base="${DRIVE9_RUN_BASE:-/home/ubuntu/drive9-test}"

mount_pid=""
mount_dir=""
remote_root=""
remote_created=false
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
	if mount_is_active; then
		timeout --signal=TERM --kill-after=3s 35s \
			"$drive9_bin" umount --timeout 30s "$mount_dir" \
			>/dev/null 2>&1 || true
	fi
	if [[ -n "$mount_pid" ]] && kill -0 "$mount_pid" 2>/dev/null; then
		kill "$mount_pid" 2>/dev/null || true
		wait "$mount_pid" 2>/dev/null || true
	fi
	if [[ "$remote_created" == true && "$remote_root" == /pr901-lineage-smoke-* ]]; then
		with_credential timeout --signal=TERM --kill-after=3s 30s \
			"$drive9_bin" fs rm -r ":$remote_root" \
			>/dev/null 2>&1 || true
	fi
	if [[ "$normal_exit" == true ]]; then
		exit "$exit_code"
	fi
	exit 2
}

trap cleanup EXIT
trap 'exit 130' INT TERM

for command_name in cmp date findmnt mkdir mktemp python3 sha256sum timeout; do
	command -v "$command_name" >/dev/null 2>&1 || {
		printf 'BROKEN: missing command %s\n' "$command_name" >&2
		exit 2
	}
done
if [[ ! -x "$drive9_bin" || ! -r "$credential_file" || ! -f "$harness" ]]; then
	printf '%s\n' 'BROKEN: binary, credential, or harness is unavailable' >&2
	exit 2
fi

mkdir -p "$run_base" || exit 2
timestamp=$(date -u +%Y%m%dT%H%M%SZ) || exit 2
run_dir=$(mktemp -d "$run_base/pr901-lineage-smoke-$timestamp.XXXXXX") || exit 2
run_name=$(basename -- "$run_dir")
remote_root="/$run_name"
mount_dir="$run_dir/mount"
cache_dir="$run_dir/cache"
perf_dir="$run_dir/perf"
expected_file="$run_dir/expected.bin"
remote_file="$run_dir/remote.bin"
result_file="$run_dir/result.json"
mkdir -p "$mount_dir" "$cache_dir" "$perf_dir" || exit 2

"$drive9_bin" version >"$run_dir/drive9-version.txt" 2>&1 || exit 2
sha256sum "$drive9_bin" "$harness" >"$run_dir/input.sha256"
with_credential timeout --signal=TERM --kill-after=3s 30s \
	"$drive9_bin" fs mkdir ":$remote_root" \
	>"$run_dir/remote-mkdir.log" 2>&1 || exit 2
remote_created=true

printf 'run_dir=%s\nremote_root=%s\n' "$run_dir" "$remote_root"
with_credential "$drive9_bin" mount \
	--server="$server" \
	--mode=fuse \
	--foreground \
	--profile=none \
	--durability=interactive \
	--write-cache-size-mb=64 \
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

timeout --signal=TERM --kill-after=5s 120s python3 "$harness" \
	--path "$mount_dir/grow.bin" \
	--expected "$expected_file" \
	--result "$result_file" \
	--parent-kib 512 \
	--size-kib 768 \
	>"$run_dir/workload.log" 2>&1 || exit 1

timeout --signal=TERM --kill-after=5s 70s \
	"$drive9_bin" umount --timeout 60s "$mount_dir" \
	>"$run_dir/umount.log" 2>&1 || exit 1
for ((attempt = 0; attempt < 150; attempt++)); do
	mount_is_active || break
	sleep 0.1
done
if [[ -n "$mount_pid" ]] && kill -0 "$mount_pid" 2>/dev/null; then
	wait "$mount_pid" || true
fi
mount_pid=""

with_credential timeout --signal=TERM --kill-after=5s 90s \
	"$drive9_bin" fs cp ":$remote_root/grow.bin" "$remote_file" \
	>"$run_dir/download.log" 2>&1 || exit 1
sha256sum "$expected_file" "$remote_file" >"$run_dir/final.sha256"
cmp "$expected_file" "$remote_file" || exit 1
rg_pattern='rebasing direct-child|coalesced exact queued parent|successfully uploaded|terminal failure|conflict committing'
grep -E "$rg_pattern" "$run_dir/mount.log" >"$run_dir/commit-events.log" || true

with_credential timeout --signal=TERM --kill-after=3s 30s \
	"$drive9_bin" fs rm -r ":$remote_root" \
	>"$run_dir/remote-cleanup.log" 2>&1 || exit 1
remote_created=false

if ! grep -Eq 'rebasing direct-child|coalesced exact queued parent' "$run_dir/commit-events.log"; then
	printf '%s\n' 'FAIL: authority bytes match but the lineage branch was not exercised' >&2
	exit 1
fi

printf 'PASS: lineage branch exercised and exact 768 KiB authority bytes match; evidence=%s\n' "$run_dir"
normal_exit=true
