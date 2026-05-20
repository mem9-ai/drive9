#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: perf/mount/run.sh <workload>

workloads:
  small-files
  metadata-walk
  large-write
  large-read
  cold-read
USAGE
  exit 2
}

WORKLOAD="${1:-}"
[[ -n "$WORKLOAD" ]] || usage

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKLOAD_SCRIPT="$SCRIPT_DIR/workloads/$WORKLOAD.sh"
[[ -x "$WORKLOAD_SCRIPT" ]] || {
  echo "unknown workload or not executable: $WORKLOAD" >&2
  usage
}

DRIVE9_BIN="${DRIVE9_BIN:-$REPO_ROOT/bin/drive9}"
if [[ ! -x "$DRIVE9_BIN" ]]; then
  DRIVE9_BIN="$(command -v drive9 || true)"
fi
[[ -n "$DRIVE9_BIN" && -x "$DRIVE9_BIN" ]] || {
  echo "drive9 binary not found; set DRIVE9_BIN or run make build-cli" >&2
  exit 1
}

RUN_ID="$(date -u +%Y%m%d-%H%M%S)-$WORKLOAD"
DRIVE9_REMOTE_ROOT="${DRIVE9_REMOTE_ROOT:-/perf/mount/$RUN_ID}"
DRIVE9_MOUNTPOINT="${DRIVE9_MOUNTPOINT:-/tmp/drive9-perf-mnt-$USER}"
DRIVE9_PROFILE_ROOT="${DRIVE9_PROFILE_ROOT:-$SCRIPT_DIR/profiles}"
DRIVE9_PROFILE_DIR="${DRIVE9_PROFILE_DIR:-$DRIVE9_PROFILE_ROOT/$RUN_ID}"
DRIVE9_CACHE_DIR="${DRIVE9_CACHE_DIR:-/tmp/drive9-perf-cache-$RUN_ID}"
DRIVE9_DURABILITY="${DRIVE9_DURABILITY:-${DRIVE9_SYNC_MODE:-interactive}}"
DRIVE9_PROFILE_CPU_MODE="${DRIVE9_PROFILE_CPU_MODE:-workload}"
DRIVE9_PROFILE_HEAP_INTERVAL="${DRIVE9_PROFILE_HEAP_INTERVAL:-0s}"
DRIVE9_PERF_JSONL="${DRIVE9_PERF_JSONL:-$DRIVE9_PROFILE_DIR/perf.jsonl}"
DRIVE9_PERF_INTERVAL="${DRIVE9_PERF_INTERVAL:-1s}"
DRIVE9_PERF_MAX_SAMPLES="${DRIVE9_PERF_MAX_SAMPLES:-7200}"
DRIVE9_PPROF_ADDR="${DRIVE9_PPROF_ADDR:-}"
DRIVE9_MOUNT_EXTRA_FLAGS="${DRIVE9_MOUNT_EXTRA_FLAGS:-}"

if [[ "$DRIVE9_PROFILE_CPU_MODE" != "workload" && "$DRIVE9_PROFILE_CPU_MODE" != "mount" ]]; then
  echo "DRIVE9_PROFILE_CPU_MODE must be workload or mount" >&2
  exit 1
fi
if [[ "$DRIVE9_PROFILE_CPU_MODE" == "workload" && -z "$DRIVE9_PPROF_ADDR" ]]; then
  DRIVE9_PPROF_ADDR="127.0.0.1:0"
fi

mkdir -p "$DRIVE9_MOUNTPOINT" "$DRIVE9_PROFILE_DIR" "$DRIVE9_CACHE_DIR"

MOUNT_LOG="$DRIVE9_PROFILE_DIR/mount.log"
WORKLOAD_LOG="$DRIVE9_PROFILE_DIR/workload.log"

{
  echo "run_id=$RUN_ID"
  echo "workload=$WORKLOAD"
  echo "drive9_bin=$DRIVE9_BIN"
  echo "remote_root=$DRIVE9_REMOTE_ROOT"
  echo "mountpoint=$DRIVE9_MOUNTPOINT"
  echo "profile_dir=$DRIVE9_PROFILE_DIR"
  echo "cache_dir=$DRIVE9_CACHE_DIR"
  echo "durability=$DRIVE9_DURABILITY"
  echo "cpu_mode=$DRIVE9_PROFILE_CPU_MODE"
  echo "heap_interval=$DRIVE9_PROFILE_HEAP_INTERVAL"
  echo "perf_jsonl=$DRIVE9_PERF_JSONL"
  echo "perf_interval=$DRIVE9_PERF_INTERVAL"
  echo "perf_max_samples=$DRIVE9_PERF_MAX_SAMPLES"
  echo "pprof_addr=$DRIVE9_PPROF_ADDR"
  echo "extra_flags=$DRIVE9_MOUNT_EXTRA_FLAGS"
  git_head=""
  if command -v git >/dev/null 2>&1; then
    git_head="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || true)"
  fi
  if [[ -n "$git_head" ]]; then
    echo "git_head=$git_head"
  fi
  "$DRIVE9_BIN" version 2>/dev/null | sed 's/^/drive9_version=/'
} > "$DRIVE9_PROFILE_DIR/env.txt"

server_args=()
if [[ -n "${DRIVE9_BASE:-}" ]]; then
  server_args+=(--server "$DRIVE9_BASE")
  export DRIVE9_SERVER="$DRIVE9_BASE"
fi
if [[ -n "${DRIVE9_API_KEY:-}" ]]; then
  server_args+=(--api-key "$DRIVE9_API_KEY")
  export DRIVE9_API_KEY
fi

"$DRIVE9_BIN" fs mkdir ":$DRIVE9_REMOTE_ROOT" >/dev/null 2>&1 || true

mount_args=(
  --mode fuse
  --cache-dir "$DRIVE9_CACHE_DIR"
  --durability "$DRIVE9_DURABILITY"
  --perf-counters
  --profile-heap "$DRIVE9_PROFILE_DIR/heap-final.pprof"
  --profile-dir "$DRIVE9_PROFILE_DIR"
  --profile-heap-interval "$DRIVE9_PROFILE_HEAP_INTERVAL"
)
if [[ -n "$DRIVE9_PERF_JSONL" ]]; then
  mount_args+=(--perf-jsonl "$DRIVE9_PERF_JSONL")
  mount_args+=(--perf-interval "$DRIVE9_PERF_INTERVAL")
  mount_args+=(--perf-max-samples "$DRIVE9_PERF_MAX_SAMPLES")
fi
if [[ "$DRIVE9_PROFILE_CPU_MODE" == "mount" ]]; then
  mount_args+=(--profile-cpu "$DRIVE9_PROFILE_DIR/cpu.pprof")
fi
if [[ ${#server_args[@]} -gt 0 ]]; then
  mount_args=("${server_args[@]}" "${mount_args[@]}")
fi
if [[ -n "$DRIVE9_PPROF_ADDR" ]]; then
  mount_args+=(--pprof-addr "$DRIVE9_PPROF_ADDR")
fi
if [[ -n "$DRIVE9_MOUNT_EXTRA_FLAGS" ]]; then
  # shellcheck disable=SC2206
  extra_flags=($DRIVE9_MOUNT_EXTRA_FLAGS)
  mount_args+=("${extra_flags[@]}")
fi

cleanup() {
  set +e
  "$DRIVE9_BIN" umount "$DRIVE9_MOUNTPOINT" >>"$MOUNT_LOG" 2>&1
  if [[ -n "${mount_pid:-}" ]]; then
    for _ in $(seq 1 50); do
      kill -0 "$mount_pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$mount_pid" 2>/dev/null; then
      if command -v umount >/dev/null 2>&1; then
        umount "$DRIVE9_MOUNTPOINT" >>"$MOUNT_LOG" 2>&1
      fi
      sleep 1
    fi
    if kill -0 "$mount_pid" 2>/dev/null; then
      echo "mount process did not exit after unmount; sending SIGTERM" >>"$MOUNT_LOG"
      kill "$mount_pid" >>"$MOUNT_LOG" 2>&1
    fi
    wait "$mount_pid" >>"$MOUNT_LOG" 2>&1
  fi
}
trap cleanup EXIT

"$DRIVE9_BIN" mount "${mount_args[@]}" ":$DRIVE9_REMOTE_ROOT" "$DRIVE9_MOUNTPOINT" >"$MOUNT_LOG" 2>&1 &
mount_pid=$!

for _ in $(seq 1 100); do
  if grep -Fq "drive9: mounted on $DRIVE9_MOUNTPOINT" "$MOUNT_LOG" 2>/dev/null; then
    break
  fi
  sleep 0.1
done

if ! kill -0 "$mount_pid" 2>/dev/null; then
  echo "mount process exited early; see $MOUNT_LOG" >&2
  exit 1
fi

pprof_addr=""
if [[ "$DRIVE9_PROFILE_CPU_MODE" == "workload" ]]; then
  for _ in $(seq 1 100); do
    pprof_addr="$(awk '/pprof listening on/ {print $NF; exit}' "$MOUNT_LOG" 2>/dev/null || true)"
    [[ -n "$pprof_addr" ]] && break
    sleep 0.1
  done
  if [[ -z "$pprof_addr" ]]; then
    echo "pprof control address not found; see $MOUNT_LOG" >&2
    exit 1
  fi
  curl -fsS "http://$pprof_addr/debug/drive9/profile/cpu/start?path=$DRIVE9_PROFILE_DIR/cpu.pprof" >>"$MOUNT_LOG" 2>&1
fi

start_ns="$(date +%s)"
set +e
MNT="$DRIVE9_MOUNTPOINT" \
RUN_DIR="$DRIVE9_PROFILE_DIR" \
DRIVE9_BIN="$DRIVE9_BIN" \
DRIVE9_REMOTE_ROOT="$DRIVE9_REMOTE_ROOT" \
"$WORKLOAD_SCRIPT" >"$WORKLOAD_LOG" 2>&1
workload_status=$?
set -e
end_ns="$(date +%s)"
echo "wall_seconds=$((end_ns - start_ns))" >> "$DRIVE9_PROFILE_DIR/env.txt"
echo "workload_status=$workload_status" >> "$DRIVE9_PROFILE_DIR/env.txt"

if [[ "$DRIVE9_PROFILE_CPU_MODE" == "workload" ]]; then
  curl -fsS "http://$pprof_addr/debug/drive9/profile/cpu/stop" >>"$MOUNT_LOG" 2>&1 || true
fi

if [[ "$workload_status" -ne 0 ]]; then
  echo "workload failed with status $workload_status; see $WORKLOAD_LOG" >&2
  exit "$workload_status"
fi

cleanup
trap - EXIT

"$SCRIPT_DIR/scripts/summarize.sh" "$DRIVE9_PROFILE_DIR" "$DRIVE9_BIN"

echo "profile run written to: $DRIVE9_PROFILE_DIR"
