#!/usr/bin/env bash
# Hosted regression runner for TEST_CASE.md in this directory.
# It executes one ext4 control and one Drive9 SQLite WAL checkpoint case.
# Authority reads use authority_http.py, independently of the candidate.
# Credentials are accepted only through the process environment.

set -u
umask 077

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || exit 1
script_path="$script_dir/$(basename -- "${BASH_SOURCE[0]}")"
default_harness="$script_dir/wal4m_scale.py"
if [[ ! -f "$default_harness" ]]; then
	default_harness="$script_dir/../drive9-wal4m-main-scale-20260907/wal4m_scale.py"
fi

without_drive9_secret() (
	unset DRIVE9_API_KEY DRIVE9_VAULT_TOKEN
	"$@"
)

linux_process_state_starttime() {
	local process_pid="$1"
	local stat_line=""
	local stat_tail=""
	local -a stat_fields=()

	[[ "$process_pid" =~ ^[0-9]+$ ]] || return 1
	[[ -r "/proc/$process_pid/stat" ]] || return 1
	IFS= read -r stat_line <"/proc/$process_pid/stat" || return 1
	stat_tail="${stat_line##*) }"
	read -r -a stat_fields <<<"$stat_tail"
	((${#stat_fields[@]} > 19)) || return 1
	printf '%s %s\n' "${stat_fields[0]}" "${stat_fields[19]}"
}

process_matches_starttime() {
	local process_pid="$1"
	local expected_starttime="$2"
	local state=""
	local actual_starttime=""

	read -r state actual_starttime \
		< <(linux_process_state_starttime "$process_pid") || return 1
	[[ "$state" != Z ]] || return 1
	[[ "$actual_starttime" == "$expected_starttime" ]] || return 1
	kill -0 "$process_pid" >/dev/null 2>&1
}

process_is_owned_mount() {
	local process_pid="$1"
	local expected_starttime="$2"
	local expected_executable="$3"
	local expected_mount="$4"

	python3 - "$process_pid" "$expected_starttime" \
		"$expected_executable" "$expected_mount" <<'PY'
import os
import sys
from pathlib import Path

pid, expected_starttime, expected_executable, expected_mount = sys.argv[1:]
process_root = Path("/proc") / pid
try:
    stat_line = (process_root / "stat").read_text(encoding="utf-8")
    stat_tail = stat_line.rsplit(") ", 1)[1].split()
    state = stat_tail[0]
    actual_starttime = stat_tail[19]
    actual_executable = (process_root / "exe").resolve(strict=True)
    expected_executable_path = Path(expected_executable).resolve(strict=True)
    arguments = (process_root / "cmdline").read_bytes().split(b"\0")
except (IndexError, OSError, ValueError):
    raise SystemExit(1)

if (
    state == "Z"
    or actual_starttime != expected_starttime
    or actual_executable != expected_executable_path
    or os.fsencode(expected_mount) not in arguments
):
    raise SystemExit(1)
PY
}

write_outer_verdict() {
	local verdict_path="$1"
	local status="$2"
	local reason="$3"
	local runner_rc="$4"
	local candidate_sha=""
	local verdict_dir=""
	local run_id=""
	local remote_preserved=false

	verdict_dir=$(dirname -- "$verdict_path") || return 1
	run_id=$(basename -- "$verdict_dir") || return 1

	if [[ -f "$verdict_dir/candidate-sha.txt" ]]; then
		read -r candidate_sha \
			<"$verdict_dir/candidate-sha.txt" || true
	fi
	if [[ -f "$verdict_dir/remote-created" ]]; then
		remote_preserved=true
	fi
	python3 - "$verdict_path" "$status" "$reason" "$runner_rc" \
		"$candidate_sha" "${DRIVE9_CANDIDATE_REF:-HEAD}" \
		"${DRIVE9_SERVER:-https://drive9.example.invalid/}" \
		"/$run_id" "$remote_preserved" <<'PY'
import json
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
value = {
    "schema_version": 1,
    "case_id": "sqlite-wal-fsync-50m-truncate-overlap",
    "status": sys.argv[2],
    "observed_status": sys.argv[2],
    "reason": sys.argv[3],
    "runner_rc": int(sys.argv[4]),
    "candidate_sha": sys.argv[5] or None,
    "candidate_ref": sys.argv[6],
    "server": sys.argv[7],
    "remote_root": sys.argv[8],
    "remote_preserved": sys.argv[9] == "true",
}
temporary = path.with_name(path.name + f".part.{os.getpid()}")
with temporary.open("w", encoding="utf-8") as handle:
    json.dump(value, handle, indent=2, sort_keys=True)
    handle.write("\n")
os.replace(temporary, path)
PY
}

outer_fallback_cleanup() {
	local run_dir="$1"
	local mount_dir="$run_dir/mount"
	local drive9_bin="$run_dir/bin/drive9"
	local mount_pid=""
	local mount_starttime=""
	local result=0

	if command -v findmnt >/dev/null 2>&1 && \
		findmnt -rn --mountpoint "$mount_dir" >/dev/null 2>&1 && \
		[[ -x "$drive9_bin" ]]; then
		timeout --signal=TERM --kill-after=3s 10s \
			"$drive9_bin" umount --timeout 7s \
			"$mount_dir" >/dev/null 2>&1 || true
	fi
	if [[ -f "$run_dir/mount.pid" ]]; then
		{
			read -r mount_pid
			read -r mount_starttime
		} <"$run_dir/mount.pid" || true
	fi
	if process_is_owned_mount "$mount_pid" "$mount_starttime" \
		"$drive9_bin" "$mount_dir"; then
		kill "$mount_pid" >/dev/null 2>&1 || true
		for ((attempt = 0; attempt < 50; attempt++)); do
			process_matches_starttime "$mount_pid" "$mount_starttime" || break
			sleep 0.1
		done
		if process_is_owned_mount "$mount_pid" "$mount_starttime" \
			"$drive9_bin" "$mount_dir"; then
			kill -KILL "$mount_pid" >/dev/null 2>&1 || true
			for ((attempt = 0; attempt < 20; attempt++)); do
				process_matches_starttime "$mount_pid" \
					"$mount_starttime" || break
				sleep 0.1
			done
		fi
		if process_matches_starttime "$mount_pid" "$mount_starttime"; then
			result=1
		fi
	fi
	if command -v findmnt >/dev/null 2>&1 && \
		findmnt -rn --mountpoint "$mount_dir" >/dev/null 2>&1; then
		if command -v fusermount3 >/dev/null 2>&1; then
			timeout --signal=TERM --kill-after=2s 5s \
				fusermount3 -uz "$mount_dir" >/dev/null 2>&1 || true
		fi
	fi
	if command -v findmnt >/dev/null 2>&1 && \
		findmnt -rn --mountpoint "$mount_dir" >/dev/null 2>&1; then
		result=1
	fi
	if [[ -n "${DRIVE9_CHECKOUT:-}" ]] && \
		[[ -d "$run_dir/drive9-src" ]]; then
		without_drive9_secret timeout --signal=TERM --kill-after=3s 15s \
			git -C "$DRIVE9_CHECKOUT" worktree remove --force \
			"$run_dir/drive9-src" >/dev/null 2>&1 || result=1
	fi
	return "$result"
}

run_outer() {
	local case_timeout="${DRIVE9_CASE_TIMEOUT_SECONDS:-900}"
	local run_base="${DRIVE9_RUN_BASE:-/tmp/drive9-sqlite-cases}"
	local timestamp=""
	local run_dir=""
	local inner_rc=0
	local cleanup_reason=""

	for command_name in mktemp mkdir date bash python3; do
		if ! command -v "$command_name" >/dev/null 2>&1; then
			printf 'BROKEN: missing command %s\n' "$command_name" >&2
			return 2
		fi
	done
	if [[ ! "$case_timeout" =~ ^[1-9][0-9]*$ ]]; then
		printf '%s\n' 'BROKEN: DRIVE9_CASE_TIMEOUT_SECONDS must be an integer' >&2
		return 2
	fi
	mkdir -p "$run_base" || return 2
	timestamp=$(date -u +%Y%m%dT%H%M%SZ) || return 2
	run_dir=$(mktemp -d \
		"$run_base/sqlite-wal-fsync-50m-$timestamp.XXXXXX") || return 2
	printf '%s\n' "$script_path" >"$run_dir/.runner-owned" || return 2
	printf 'run_dir=%s\n' "$run_dir"
	if ! command -v timeout >/dev/null 2>&1; then
		write_outer_verdict "$run_dir/verdict.json" BROKEN \
			missing_command_timeout 2 || true
		return 2
	fi

	timeout --signal=TERM --kill-after=180s "${case_timeout}s" \
		bash "$script_path" --inner "$run_dir"
	inner_rc=$?
	if ((inner_rc == 124 || inner_rc == 137)); then
		cleanup_reason=overall_timeout
		if ! outer_fallback_cleanup "$run_dir"; then
			cleanup_reason=overall_timeout_cleanup_failed
		fi
		write_outer_verdict "$run_dir/verdict.json" FAIL \
			"$cleanup_reason" "$inner_rc" || true
		printf 'FAIL: overall timeout; evidence retained at %s\n' \
			"$run_dir" >&2
		return "$inner_rc"
	fi
	if [[ ! -f "$run_dir/verdict.json" ]]; then
		outer_fallback_cleanup "$run_dir"
		write_outer_verdict "$run_dir/verdict.json" FAIL \
			missing_verdict "$inner_rc" || true
		return 1
	fi
	return "$inner_rc"
}

if [[ "${1:-}" != "--inner" ]]; then
	run_outer
	exit $?
fi

run_dir="${2:-}"
if [[ -z "$run_dir" || ! -d "$run_dir" || \
	! "$(basename -- "$run_dir")" =~ ^sqlite-wal-fsync-50m-[0-9]{8}T[0-9]{6}Z\.[[:alnum:]]{6}$ || \
	! -f "$run_dir/.runner-owned" ]]; then
	printf '%s\n' 'BROKEN: --inner requires an existing run directory' >&2
	exit 2
fi

case_id="sqlite-wal-fsync-50m-truncate-overlap"
known_bad_sha="e11bfde7be732663af47ade790df7808bf302a03"
server="${DRIVE9_SERVER:-https://drive9.example.invalid/}"
checkout="${DRIVE9_CHECKOUT:-}"
candidate_ref="${DRIVE9_CANDIDATE_REF:-HEAD}"
expected_xfail_sha="${DRIVE9_EXPECT_XFAIL_SHA:-}"
keep_remote_on_failure="${DRIVE9_KEEP_REMOTE_ON_FAILURE:-0}"
harness="${DRIVE9_WAL_HARNESS:-$default_harness}"
assertions="$script_dir/case_assertions.py"
authority="$script_dir/authority_http.py"
source_dir="$run_dir/drive9-src"
mount_dir="$run_dir/mount"
cache_dir="$run_dir/cache"
perf_dir="$run_dir/perf"
result_root="$run_dir/results"
template_dir="$run_dir/template"
control_dir="$run_dir/ext4-control"
authority_dir="$run_dir/authority"
xfail_dir="$run_dir/authority-xfail"
failure_dir="$run_dir/authority-failure"
drive9_bin="$run_dir/bin/drive9"
run_id=$(basename -- "$run_dir")
remote_root="/$run_id"
remote_db="$remote_root/tmp/case50/main.db"
candidate_sha=""
candidate_version=""
mount_pid=""
mount_starttime=""
remote_created=false
worktree_created=false
normal_completion=false
verdict_written=false
signal_reason=""

export DRIVE9_SERVER="$server"

write_verdict() {
	local status="$1"
	local observed_status="$2"
	local reason="$3"
	local runner_rc="$4"
	local remote_preserved="$5"

	python3 - "$run_dir/verdict.json" "$case_id" "$status" \
		"$observed_status" "$reason" "$runner_rc" "$candidate_sha" \
		"$candidate_ref" "$server" "$remote_root" \
		"$remote_preserved" <<'PY'
import json
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
value = {
    "schema_version": 1,
    "case_id": sys.argv[2],
    "status": sys.argv[3],
    "observed_status": sys.argv[4] or None,
    "reason": sys.argv[5],
    "runner_rc": int(sys.argv[6]),
    "candidate_sha": sys.argv[7] or None,
    "candidate_ref": sys.argv[8],
    "server": sys.argv[9],
    "remote_root": sys.argv[10],
    "remote_preserved": sys.argv[11] == "true",
}
temporary = path.with_name(path.name + f".part.{os.getpid()}")
with temporary.open("w", encoding="utf-8") as handle:
    json.dump(value, handle, indent=2, sort_keys=True)
    handle.write("\n")
os.replace(temporary, path)
PY
	if (($? != 0)); then
		return 1
	fi
	verdict_written=true
}

process_is_running() {
	local process_pid="$1"
	local expected_starttime="${2:-${mount_starttime:-}}"
	local state=""
	local actual_starttime=""

	kill -0 "$process_pid" >/dev/null 2>&1 || return 1
	if [[ -r "/proc/$process_pid/stat" ]]; then
		read -r state actual_starttime \
			< <(linux_process_state_starttime "$process_pid") || return 0
		[[ "$state" == Z ]] && return 1
		if [[ -n "$expected_starttime" && \
			"$actual_starttime" != "$expected_starttime" ]]; then
			return 1
		fi
	fi
	return 0
}

wait_mount_process() {
	local limit_seconds="$1"
	local deadline=$((SECONDS + limit_seconds))
	local process_rc=0

	[[ -n "$mount_pid" ]] || return 0
	while process_is_running "$mount_pid"; do
		if ((SECONDS >= deadline)); then
			return 124
		fi
		sleep 0.2
	done
	wait "$mount_pid"
	process_rc=$?
	rm -f -- "$run_dir/mount.pid"
	mount_pid=""
	mount_starttime=""
	return "$process_rc"
}

stop_owned_mount() {
	local strict="$1"
	local result=0
	local wait_rc=0

	if command -v findmnt >/dev/null 2>&1 && \
		findmnt -rn --mountpoint "$mount_dir" >/dev/null 2>&1; then
		if [[ -x "$drive9_bin" ]]; then
			timeout --signal=TERM --kill-after=5s 70s \
				"$drive9_bin" umount --timeout 60s \
				"$mount_dir" >>"$run_dir/cleanup.log" 2>&1 || result=1
		else
			result=1
		fi
	fi
	wait_mount_process 15
	wait_rc=$?
	if ((wait_rc != 0)); then
		result=1
		if [[ -n "$mount_pid" ]] && \
			process_is_running "$mount_pid"; then
			kill "$mount_pid" >/dev/null 2>&1 || true
			wait_mount_process 4 >/dev/null 2>&1 || true
		fi
		if [[ -n "$mount_pid" ]] && \
			process_is_running "$mount_pid"; then
			kill -KILL "$mount_pid" >/dev/null 2>&1 || true
			wait_mount_process 2 >/dev/null 2>&1 || true
		fi
	fi
	if command -v findmnt >/dev/null 2>&1 && \
		findmnt -rn --mountpoint "$mount_dir" >/dev/null 2>&1; then
		result=1
		if [[ "$strict" != true ]] && command -v fusermount3 >/dev/null 2>&1; then
			timeout --signal=TERM --kill-after=2s 5s \
				fusermount3 -uz "$mount_dir" \
				>>"$run_dir/cleanup.log" 2>&1 || true
		fi
	fi
	return "$result"
}

remove_worktree() {
	local result=0

	[[ "$worktree_created" == true ]] || return 0
	without_drive9_secret timeout --signal=TERM --kill-after=5s 30s \
		git -C "$checkout" worktree remove --force "$source_dir" \
		>>"$run_dir/cleanup.log" 2>&1
	result=$?
	if ((result == 0)); then
		worktree_created=false
	fi
	return "$result"
}

remove_remote_root() {
	local result=0

	[[ "$remote_created" == true ]] || return 0
	if [[ "$remote_root" != /sqlite-wal-fsync-50m-* ]]; then
		printf 'refusing remote cleanup for %s\n' \
			"$remote_root" >>"$run_dir/cleanup.log"
		return 1
	fi
	timeout --signal=TERM --kill-after=5s 45s \
		python3 "$authority" --timeout 15 remove-tree \
		--path "$remote_root" >>"$run_dir/cleanup.log" 2>&1
	result=$?
	if ((result == 0)); then
		remote_created=false
		rm -f -- "$run_dir/remote-created"
	fi
	return "$result"
}

normal_cleanup() {
	local observed_status="$1"
	local result=0
	local preserve=false
	local mount_stopped=true

	if ! stop_owned_mount true; then
		result=1
		mount_stopped=false
	fi
	if [[ "$keep_remote_on_failure" == 1 ]] && \
		[[ "$observed_status" != PASS ]] && \
		[[ "$observed_status" != XFAIL ]]; then
		preserve=true
	fi
	if [[ "$preserve" != true && "$mount_stopped" == true ]]; then
		remove_remote_root || result=1
	fi
	remove_worktree || result=1
	return "$result"
}

complete_case() {
	local observed_status="$1"
	local reason="$2"
	local requested_rc="$3"
	local final_status="$observed_status"
	local final_reason="$reason"
	local final_rc="$requested_rc"
	local cleanup_rc=0
	local remote_preserved=false

	normal_cleanup "$observed_status"
	cleanup_rc=$?
	if [[ "$remote_created" == true ]]; then
		remote_preserved=true
	fi
	if ((cleanup_rc != 0)); then
		final_status=FAIL
		final_reason="cleanup_failed_after_${observed_status}"
		final_rc=1
	fi
	write_verdict "$final_status" "$observed_status" "$final_reason" \
		"$final_rc" "$remote_preserved" || final_rc=1
	normal_completion=true
	printf '%s: %s; artifacts=%s\n' \
		"$final_status" "$final_reason" "$run_dir"
	exit "$final_rc"
}

emergency_exit() {
	local runner_rc=$?
	local reason=runner_aborted

	trap - EXIT TERM INT
	if [[ -n "$signal_reason" ]]; then
		reason="$signal_reason"
	fi
	if [[ "$normal_completion" != true ]]; then
		stop_owned_mount false >/dev/null 2>&1 || true
		remove_worktree >/dev/null 2>&1 || true
		if [[ "$verdict_written" != true ]]; then
			write_verdict FAIL "" "$reason" 1 "$remote_created" \
				>/dev/null 2>&1 || true
		fi
		if ((runner_rc == 0)); then
			runner_rc=1
		fi
	fi
	exit "$runner_rc"
}

handle_signal() {
	signal_reason=overall_timeout_or_signal
	exit 124
}

trap emergency_exit EXIT
trap handle_signal TERM INT

collect_unexpected_authority() {
	local label=""
	local path=""

	mkdir -p "$failure_dir" || return 0
	for label in main wal; do
		path="$remote_db"
		if [[ "$label" == wal ]]; then
			path="${remote_db}-wal"
		fi
		python3 "$authority" --timeout 20 stat --path "$path" \
			--output "$failure_dir/$label-stat.json" \
			>>"$run_dir/failure-authority.log" 2>&1 || continue
		python3 "$authority" --timeout 60 download --path "$path" \
			--destination "$failure_dir/$label.bin" \
			--expect-stat "$failure_dir/$label-stat.json" \
			--output "$failure_dir/$label-download.json" \
			>>"$run_dir/failure-authority.log" 2>&1 || true
	done
}

run_pass_authority() {
	mkdir -p "$authority_dir" || return 1
	python3 "$authority" --timeout 30 stat --path "$remote_db" \
		--output "$authority_dir/main-stat-0.json" || return 1
	python3 "$authority" --timeout 90 download --path "$remote_db" \
		--destination "$authority_dir/main-1.db" \
		--expect-stat "$authority_dir/main-stat-0.json" \
		--output "$authority_dir/main-download-1.json" || return 1
	python3 "$authority" --timeout 30 stat --path "$remote_db" \
		--output "$authority_dir/main-stat-1.json" || return 1
	python3 "$authority" --timeout 90 download --path "$remote_db" \
		--destination "$authority_dir/main-2.db" \
		--expect-stat "$authority_dir/main-stat-1.json" \
		--output "$authority_dir/main-download-2.json" || return 1
	python3 "$authority" --timeout 30 stat --path "$remote_db" \
		--output "$authority_dir/main-stat-2.json" || return 1
	python3 "$authority" --timeout 30 wal-gone \
		--path "${remote_db}-wal" \
		--output "$authority_dir/wal-state.json" || return 1
	without_drive9_secret python3 "$assertions" pass-authority \
		--directory "$authority_dir" \
		--prepare "$result_root/prepare.json" || return 1

	local seed_rows=""
	local filler_commits=""
	seed_rows=$(python3 - "$result_root/prepare.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    print(json.load(handle)["seed_rows"])
PY
	) || return 1
	filler_commits=$(python3 - "$result_root/drive9/result.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    print(json.load(handle)["pre_checkpoint"]["filler_commits"])
PY
	) || return 1
	without_drive9_secret python3 "$harness" verify \
		--db "$authority_dir/main-1.db" \
		--acks "$result_root/drive9/acks.jsonl" \
		--seed-rows "$seed_rows" \
		--expected-filler-seq "$filler_commits" \
		--quick-check --immutable --busy-timeout-ms 30000 \
		--output "$authority_dir/verify.json" || return 1
	without_drive9_secret python3 "$assertions" verification \
		--verification "$authority_dir/verify.json" \
		--expected-acks 105 || return 1
	python3 - "$authority_dir/main-1.db" <<'PY'
import sqlite3
import sys
from pathlib import Path

database = Path(sys.argv[1]).absolute()
uri = f"file:{database.as_posix()}?mode=ro&immutable=1"
connection = sqlite3.connect(uri, uri=True, isolation_level=None)
try:
    result = connection.execute("PRAGMA integrity_check").fetchone()[0]
finally:
    connection.close()
if result != "ok":
    raise SystemExit(f"integrity_check={result}")
PY
}

run_pass_diagnostics() {
	without_drive9_secret python3 "$assertions" pass-perf \
		--perf "$perf_dir/perf.jsonl" \
		--candidate-sha "$candidate_sha" || return 1
	if grep -En \
		'revision conflict|SQLITE_IOERR|disk I/O error|flush upload failed' \
		"$run_dir/mount.log" "$run_dir/drive9-workload.log" \
		>"$run_dir/forbidden-log-lines.txt"; then
		return 1
	fi
	return 0
}

download_xfail_object() {
	local label="$1"
	local path="$2"

	python3 "$authority" --timeout 30 stat --path "$path" \
		--output "$xfail_dir/$label-stat-0.json" || return 1
	python3 "$authority" --timeout 90 download --path "$path" \
		--destination "$xfail_dir/$label-1.bin" \
		--expect-stat "$xfail_dir/$label-stat-0.json" \
		--output "$xfail_dir/$label-download-1.json" || return 1
	python3 "$authority" --timeout 30 stat --path "$path" \
		--output "$xfail_dir/$label-stat-1.json" || return 1
	python3 "$authority" --timeout 90 download --path "$path" \
		--destination "$xfail_dir/$label-2.bin" \
		--expect-stat "$xfail_dir/$label-stat-1.json" \
		--output "$xfail_dir/$label-download-2.json" || return 1
	python3 "$authority" --timeout 30 stat --path "$path" \
		--output "$xfail_dir/$label-stat-2.json" || return 1
}

run_xfail_authority() {
	mkdir -p "$xfail_dir" || return 1
	download_xfail_object main "$remote_db" || return 1
	download_xfail_object wal "${remote_db}-wal" || return 1
	without_drive9_secret python3 "$assertions" xfail-authority \
		--directory "$xfail_dir" \
		--template "$template_dir/main.db" || return 1
	mkdir -p "$xfail_dir/verify" || return 1
	cp "$xfail_dir/main-1.bin" \
		"$xfail_dir/verify/main.db" || return 1
	cp "$xfail_dir/wal-1.bin" \
		"$xfail_dir/verify/main.db-wal" || return 1

	local seed_rows=""
	local filler_commits=""
	seed_rows=$(python3 - "$result_root/prepare.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    print(json.load(handle)["seed_rows"])
PY
	) || return 1
	filler_commits=$(python3 - "$result_root/drive9/result.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    print(json.load(handle)["pre_checkpoint"]["filler_commits"])
PY
	) || return 1
	without_drive9_secret python3 "$harness" verify \
		--db "$xfail_dir/verify/main.db" \
		--acks "$result_root/drive9/acks.jsonl" \
		--seed-rows "$seed_rows" \
		--expected-filler-seq "$filler_commits" \
		--quick-check --busy-timeout-ms 30000 \
		--output "$xfail_dir/verify.json" || return 1
	without_drive9_secret python3 "$assertions" verification \
		--verification "$xfail_dir/verify.json" \
		--expected-acks 65 || return 1
	python3 - "$xfail_dir/verify/main.db" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1], isolation_level=None)
try:
    result = connection.execute("PRAGMA integrity_check").fetchone()[0]
finally:
    connection.close()
if result != "ok":
    raise SystemExit(f"integrity_check={result}")
PY
}

run_xfail_gates() {
	without_drive9_secret python3 "$assertions" xfail-result \
		--candidate-sha "$candidate_sha" \
		--return-code "$drive9_rc" \
		--result "$result_root/drive9/result.json" \
		--acks "$result_root/drive9/acks.jsonl" \
		--mount-log "$run_dir/mount.log" || return 1
	run_xfail_authority || return 1
	without_drive9_secret python3 "$assertions" xfail-perf \
		--perf "$perf_dir/perf.jsonl" \
		--candidate-sha "$candidate_sha" || return 1
}

for command_name in basename bash cmp cp date dd dirname findmnt git go grep \
	install make mkdir mktemp python3 rm sha256sum sleep timeout; do
	if ! command -v "$command_name" >/dev/null 2>&1; then
		complete_case BROKEN "missing_command_$command_name" 2
	fi
done
if [[ -z "${DRIVE9_API_KEY:-}" ]]; then
	complete_case BROKEN missing_drive9_api_key 2
fi
if [[ -n "${DRIVE9_VAULT_TOKEN:-}" ]]; then
	complete_case BROKEN drive9_vault_token_must_be_unset 2
fi
if [[ -z "$checkout" ]] || \
	! without_drive9_secret git -C "$checkout" rev-parse --is-inside-work-tree \
	>/dev/null 2>&1; then
	complete_case BROKEN invalid_drive9_checkout 2
fi
if [[ ! -e /dev/fuse ]]; then
	complete_case BROKEN missing_dev_fuse 2
fi
if [[ "$keep_remote_on_failure" != 0 && \
	"$keep_remote_on_failure" != 1 ]]; then
	complete_case BROKEN invalid_keep_remote_setting 2
fi
if [[ -n "$expected_xfail_sha" && \
	"$expected_xfail_sha" != "$known_bad_sha" ]]; then
	complete_case BROKEN unsupported_expected_xfail_sha 2
fi
if [[ ! -f "$harness" || ! -f "$assertions" || ! -f "$authority" ]]; then
	complete_case BROKEN missing_case_helper 2
fi

mkdir -p "$run_dir/bin" "$mount_dir" "$cache_dir" "$perf_dir" \
	"$result_root" || complete_case BROKEN create_run_directories_failed 2
printf '%s\n' "$remote_root" >"$run_dir/remote-root.txt"
sha256sum "$script_path" "$harness" "$assertions" "$authority" \
	>"$run_dir/helpers.sha256" || \
	complete_case BROKEN hash_case_helpers_failed 2

python3 - <<'PY'
import json
import os
from pathlib import Path

config_path = Path.home() / ".drive9" / "config"
if config_path.exists():
    with config_path.open(encoding="utf-8") as handle:
        config = json.load(handle)
    context_name = config.get("current_context", "")
    context = config.get("contexts", {}).get(context_name) or {}
    context_server = context.get("server", "").rstrip("/")
    requested_server = os.environ["DRIVE9_SERVER"].rstrip("/")
    if context_server and context_server != requested_server:
        raise SystemExit("active Drive9 context uses a different server")
PY
config_rc=$?
if ((config_rc != 0)); then
	complete_case BROKEN drive9_context_server_mismatch 2
fi

if [[ "$candidate_ref" == origin/main ]]; then
	without_drive9_secret git -C "$checkout" fetch origin main \
		>"$run_dir/git-fetch.log" 2>&1 || \
		complete_case BROKEN git_fetch_failed 2
else
	printf 'fetch skipped for candidate ref %s\n' "$candidate_ref" \
		>"$run_dir/git-fetch.log"
fi
without_drive9_secret git -C "$checkout" rev-parse --verify \
	"${candidate_ref}^{commit}" >"$run_dir/candidate-resolved.txt" 2>&1 || \
	complete_case BROKEN candidate_ref_not_found 2
without_drive9_secret git -C "$checkout" worktree add --detach \
	"$source_dir" "$candidate_ref" \
	>"$run_dir/git-worktree.log" 2>&1 || \
	complete_case BROKEN git_worktree_failed 2
worktree_created=true
candidate_sha=$(without_drive9_secret git -C "$source_dir" rev-parse HEAD) || \
	complete_case BROKEN resolve_candidate_sha_failed 2
printf '%s\n' "$candidate_sha" >"$run_dir/candidate-sha.txt"
candidate_version="${candidate_sha:0:7}"
if [[ -n "$expected_xfail_sha" && \
	"$candidate_sha" != "$expected_xfail_sha" ]]; then
	complete_case BROKEN expected_xfail_sha_does_not_match_candidate 2
fi
without_drive9_secret make -C "$source_dir" build-cli \
	VERSION="$candidate_version" \
	>"$run_dir/build.log" 2>&1 || \
	complete_case BROKEN candidate_build_failed 2
install -m 0755 "$source_dir/bin/drive9" "$drive9_bin" || \
	complete_case BROKEN install_candidate_binary_failed 2
without_drive9_secret git -C "$source_dir" show -s --format=fuller HEAD \
	>"$run_dir/candidate.txt" || \
	complete_case BROKEN record_candidate_failed 2
sha256sum "$drive9_bin" >"$run_dir/drive9.sha256" || \
	complete_case BROKEN hash_candidate_failed 2
python3 -VV >"$run_dir/python-version.txt" 2>&1 || \
	complete_case BROKEN record_python_failed 2

python3 "$authority" --timeout 30 status \
	--output "$run_dir/server-status.json"
status_rc=$?
if ((status_rc == 3)); then
	complete_case BROKEN append_log_capability_absent 2
elif ((status_rc != 0)); then
	complete_case BROKEN server_status_failed 2
fi

remote_created=true
printf '%s\n' "$remote_root" >"$run_dir/remote-created" || \
	complete_case BROKEN record_remote_create_attempt_failed 2
timeout --signal=TERM --kill-after=5s 45s \
	"$drive9_bin" fs mkdir ":$remote_root" \
	>"$run_dir/remote-mkdir.log" 2>&1 || \
	complete_case BROKEN remote_root_create_failed 2

"$drive9_bin" mount \
	--server="$DRIVE9_SERVER" \
	--mode=fuse \
	--foreground \
	--profile=coding-agent \
	--gvisor-compat \
	--durability=fsync \
	'--append-log=**/*-wal' \
	'--remote-only=**/tmp/**' \
	--attr-ttl=30s \
	--entry-ttl=30s \
	--dir-ttl=30s \
	--readdir-prefetch \
	--readdir-prefetch-max-files=64 \
	--readdir-prefetch-max-file-bytes=50000 \
	--readdir-prefetch-max-bytes=4194304 \
	--writeback-batch-window=20ms \
	--cache-dir="$cache_dir" \
	--perf-dir="$perf_dir" \
	--perf-interval=1s \
	--perf-cpu-duration=5s \
	--perf-cpu-interval=1h \
	--perf-heap-interval=1h \
	":$remote_root" "$mount_dir" \
	>"$run_dir/mount.log" 2>&1 &
mount_pid=$!
read -r mount_state mount_starttime \
	< <(linux_process_state_starttime "$mount_pid") || \
	complete_case FAIL mount_process_identity_failed 1
if [[ "$mount_state" == Z || -z "$mount_starttime" ]]; then
	complete_case FAIL mount_process_identity_failed 1
fi
printf '%s\n%s\n' "$mount_pid" "$mount_starttime" >"$run_dir/mount.pid" || \
	complete_case FAIL record_mount_process_identity_failed 1

mounted=0
for ((attempt = 0; attempt < 300; attempt++)); do
	mount_fstype=$(findmnt -rn --mountpoint "$mount_dir" \
		--output FSTYPE 2>/dev/null || true)
	if [[ "$mount_fstype" == fuse* ]]; then
		mounted=1
		break
	fi
	if ! process_is_running "$mount_pid"; then
		break
	fi
	sleep 0.1
done
if ((mounted != 1)); then
	complete_case FAIL mount_start_failed 1
fi

mkdir -p "$template_dir" "$control_dir" || \
	complete_case BROKEN create_workload_directories_failed 2
without_drive9_secret timeout --signal=TERM --kill-after=10s 180s \
	python3 "$harness" prepare \
	--db "$template_dir/main.db" \
	--target-bytes 52428800 \
	--output "$result_root/prepare.json" \
	>"$run_dir/prepare.log" 2>&1 || \
	complete_case BROKEN template_prepare_failed 2
cp "$template_dir/main.db" "$control_dir/main.db" || \
	complete_case BROKEN control_copy_failed 2
without_drive9_secret timeout --signal=TERM --kill-after=15s 210s \
	python3 "$harness" run \
	--db "$control_dir/main.db" \
	--label ext4-control-50m \
	--output-dir "$result_root/ext4" \
	--baseline-seconds 2 --post-seconds 2 \
	--checkpoint-timeout-seconds 120 \
	--final-checkpoint-timeout-seconds 120 \
	--busy-timeout-ms 150000 \
	>"$run_dir/ext4-workload.log" 2>&1
control_rc=$?
if ((control_rc != 0)); then
	complete_case BROKEN ext4_control_process_failed 2
fi
without_drive9_secret python3 "$assertions" pass-result \
	--result "$result_root/ext4/result.json" || \
	complete_case BROKEN ext4_control_assertions_failed 2

mkdir -p "$mount_dir/tmp/case50" || \
	complete_case FAIL create_mounted_database_directory_failed 1
timeout --signal=TERM --kill-after=10s 120s \
	dd if="$template_dir/main.db" of="$mount_dir/tmp/case50/main.db" \
	bs=1048576 conv=fsync status=none || \
	complete_case FAIL stage_database_failed 1
sha256sum "$template_dir/main.db" >"$run_dir/template.sha256" || \
	complete_case FAIL hash_template_failed 1
timeout --signal=TERM --kill-after=10s 120s \
	sha256sum "$mount_dir/tmp/case50/main.db" \
	>"$run_dir/mounted-template.sha256" || \
	complete_case FAIL hash_staged_database_failed 1
read -r template_sha _ <"$run_dir/template.sha256"
read -r mounted_sha _ <"$run_dir/mounted-template.sha256"
if [[ "$template_sha" != "$mounted_sha" ]]; then
	complete_case FAIL staged_database_sha_mismatch 1
fi

without_drive9_secret timeout --signal=TERM --kill-after=15s 210s \
	python3 "$harness" run \
	--db "$mount_dir/tmp/case50/main.db" \
	--label "drive9-$candidate_version-50m-fsync" \
	--output-dir "$result_root/drive9" \
	--baseline-seconds 2 --post-seconds 2 \
	--checkpoint-timeout-seconds 120 \
	--final-checkpoint-timeout-seconds 120 \
	--busy-timeout-ms 150000 \
	>"$run_dir/drive9-workload.log" 2>&1
drive9_rc=$?
printf '%s\n' "$drive9_rc" >"$run_dir/drive9-workload.rc"

live_assert_rc=1
if ((drive9_rc == 0)); then
	without_drive9_secret python3 "$assertions" pass-result \
		--result "$result_root/drive9/result.json"
	live_assert_rc=$?
fi

route=failure
if ((drive9_rc == 0 && live_assert_rc == 0)); then
	route=pass
elif ((drive9_rc == 2)) && \
	[[ -n "$expected_xfail_sha" ]] && \
	[[ "$candidate_sha" == "$expected_xfail_sha" ]]; then
	route=xfail
fi

stop_owned_mount true
unmount_rc=$?
if ((unmount_rc != 0)); then
	collect_unexpected_authority
	complete_case FAIL clean_unmount_failed 1
fi

case "$route" in
	pass)
		if ! run_pass_authority; then
			complete_case FAIL pass_authority_failed 1
		fi
		if ! run_pass_diagnostics; then
			complete_case FAIL pass_diagnostics_failed 1
		fi
		if [[ -n "$expected_xfail_sha" ]]; then
			complete_case XPASS expected_failure_now_passes 1
		fi
		complete_case PASS all_gates_passed 0
		;;
	xfail)
		if ! run_xfail_gates; then
			complete_case FAIL xfail_fingerprint_changed 1
		fi
		complete_case XFAIL known_failure_matched 0
		;;
	*)
		collect_unexpected_authority
		complete_case FAIL unexpected_workload_result 1
		;;
esac
