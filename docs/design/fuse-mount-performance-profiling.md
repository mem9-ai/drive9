# FUSE Mount Performance Profiling

## Context

drive9 FUSE performance work needs two different observability modes:

- short, high-resolution profiling for a single workload window;
- long-running, low-overhead telemetry for continuous performance diagnosis.

CPU flame graphs and heap profiles are good at answering "where did this run
spend CPU or allocate memory?" They are less good at answering "what was the
mount doing over the last hour when memory grew?" or "were remote writes,
commit queues, dirty handles, or cache misses increasing before the slowdown?"

The product-level design lives in
`docs/design/continuous-performance-observability.md`. This document focuses on
the FUSE mount producer and harness details.

## Goals

- Capture CPU and heap hotspots for real FUSE workloads.
- Record continuous mount-local metrics that help explain pprof output.
- Keep the default mount behavior unchanged when profiling flags are absent.
- Make the same workload runnable by developers, CI, and customer-support
  environments.
- Prefer real Linux FUSE results for performance conclusions.

## Non-Goals

- This is not a replacement for server-side profiling.
- This does not profile WebDAV mounts.
- This does not yet provide a hosted dashboard or automatic regression gate.
- This does not claim macOS/macFUSE numbers are equivalent to Linux production
  FUSE numbers.

## CLI Surface

The FUSE mount accepts these profiling flags:

```bash
drive9 mount \
  --mode=fuse \
  --profile-cpu /tmp/drive9/cpu.pprof \
  --profile-heap /tmp/drive9/heap-final.pprof \
  --profile-dir /tmp/drive9/profiles \
  --profile-heap-interval 30s \
  --pprof-addr 127.0.0.1:6060 \
  --perf-jsonl /tmp/drive9/perf.jsonl \
  --perf-interval 1s \
  --perf-max-samples 7200 \
  :/ /mnt/drive9
```

Flag behavior:

| Flag | Behavior |
| --- | --- |
| `--profile-cpu` | Start Go CPU profiling at mount startup and stop it on unmount. |
| `--profile-heap` | Write one final heap profile on unmount. |
| `--profile-dir` | Directory used by periodic heap profiles and default pprof control output. |
| `--profile-heap-interval` | Periodically write heap profiles; requires `--profile-dir`. |
| `--pprof-addr` | Serve live Go pprof and drive9 CPU profile control endpoints. |
| `--perf-jsonl` | Write continuous mount performance samples as JSONL. |
| `--perf-interval` | Sampling interval for `--perf-jsonl`; default is `10s` when omitted. |
| `--perf-max-samples` | Maximum samples per active JSONL segment; default is `7200` when omitted. |

Profiling flags are FUSE-only. If mount resolution selects WebDAV, `--perf-jsonl`
is rejected instead of ignored. WebDAV has a different runtime path and would
produce misleading FUSE conclusions.

## pprof Control

When `--pprof-addr` is set, the mount exposes standard Go pprof handlers:

```text
/debug/pprof/
/debug/pprof/profile
/debug/pprof/heap
/debug/pprof/goroutine
/debug/pprof/trace
```

It also exposes workload-scoped CPU profile controls:

```text
/debug/drive9/profile/cpu/start?path=/tmp/drive9/cpu.pprof
/debug/drive9/profile/cpu/stop
```

The perf harness uses these endpoints by default so CPU profiles cover only the
workload window, not mount startup, unmount, or cleanup waits.

## Continuous JSONL Samples

`--perf-jsonl` writes one JSON object per line. Samples are emitted at:

- mount start;
- each interval tick;
- mount stop.

The active segment rotates to `<path>.1` after `--perf-max-samples` samples.
This bounds local disk use while preserving recent history for support bundles.

The current sample shape is:

```json
{
  "timestamp": "2026-05-20T01:02:03.456Z",
  "reason": "interval",
  "uptime_ms": 12345,
  "context": {
    "component": "drive9-fuse",
    "version": "dev",
    "git_hash": "unknown",
    "go_version": "go1.25.1",
    "goos": "linux",
    "goarch": "amd64",
    "pid": 1234,
    "mount_point_hash": "7f91e0a0d55b",
    "remote_root_hash": "e3b0c44298fc",
    "server_hash": "a8f5f167f44f",
    "sync_mode": "interactive",
    "write_policy": "writeback"
  },
  "runtime": {
    "goroutines": 32,
    "heap_alloc_bytes": 10485760,
    "heap_inuse_bytes": 12582912,
    "heap_objects": 123456,
    "stack_inuse_bytes": 1048576,
    "sys_bytes": 33554432,
    "next_gc_bytes": 20971520,
    "num_gc": 7,
    "pause_total_ns": 1234567
  },
  "process": {
    "user_cpu_ns": 1000000000,
    "system_cpu_ns": 200000000,
    "max_rss_bytes": 50331648
  },
  "fuse_ops": {
    "write": {
      "count": 1000,
      "errors": 0,
      "bytes": 131072000,
      "total_ns": 500000000,
      "avg_ns": 500000,
      "p50_ns": 262144,
      "p95_ns": 1048576,
      "p99_ns": 2097152,
      "max_ns": 3000000
    }
  },
  "remote_ops": {
    "write": {
      "count": 20,
      "errors": 0,
      "bytes": 131072000,
      "total_ns": 900000000,
      "avg_ns": 45000000
    }
  },
  "counters": {
    "read_cache_hit": 10,
    "read_cache_miss": 2,
    "commit_enqueue": 5,
    "commit_success": 4
  },
  "queues": {
    "commit_pending": 1,
    "commit_pending_bytes": 65536,
    "uploader_queued": 0,
    "uploader_in_flight": 1,
    "dirty_inodes": 2,
    "open_file_handles": 3,
    "open_directory_handles": 1
  }
}
```

Within each active segment, samples are written as line-delimited JSON so the
file can be tailed, uploaded from customer environments, or post-processed
without a database.

## Perf Harness

The reproducible harness lives under `perf/mount`.

```bash
make build-cli
perf/mount/run.sh small-files
perf/mount/run.sh metadata-walk
perf/mount/run.sh large-write
perf/mount/run.sh large-read
```

Useful environment variables:

```bash
DRIVE9_BIN=./bin/drive9
DRIVE9_BASE=https://...
DRIVE9_API_KEY=...
DRIVE9_REMOTE_ROOT=/perf/mount
DRIVE9_MOUNTPOINT=/tmp/drive9-perf-mnt
DRIVE9_PROFILE_ROOT=/tmp/drive9-perf-profiles
DRIVE9_CACHE_DIR=/tmp/drive9-perf-cache
DRIVE9_DURABILITY=interactive
DRIVE9_PROFILE_CPU_MODE=workload
DRIVE9_PROFILE_HEAP_INTERVAL=0s
DRIVE9_PERF_JSONL=/tmp/drive9-perf-profiles/run/perf.jsonl
DRIVE9_PERF_INTERVAL=1s
DRIVE9_PERF_MAX_SAMPLES=7200
DRIVE9_PPROF_ADDR=127.0.0.1:6060
DRIVE9_MOUNT_EXTRA_FLAGS="--dir-ttl 1s --attr-ttl 1s"
```

Each run writes:

```text
profiles/<timestamp>-<workload>/
  cpu.pprof
  heap-final.pprof
  heap-*.pprof        # only when DRIVE9_PROFILE_HEAP_INTERVAL > 0
  perf.jsonl
  perf.jsonl.1        # only after segment rotation
  perf-last.json
  summary.json
  cpu-top.txt
  cpu-callgraph.svg
  heap-inuse-space-top.txt
  heap-alloc-space-top.txt
  heap-inuse-callgraph.svg
  heap-alloc-callgraph.svg
  env.txt
  mount.log
  workload.log
```

## Workloads

Initial workloads are deliberately simple shell scripts:

| Workload | Purpose |
| --- | --- |
| `small-files` | Stress create/write/close/sync paths and small-file metadata overhead. |
| `metadata-walk` | Stress mkdir, create, readdir, find, and stat patterns. |
| `large-write` | Stress sequential large-file write, buffering, shadow, and upload paths. |
| `large-read` | Stress sequential read and cache/read-through behavior. |

Simple workloads make the first-order bottlenecks easier to identify. More
realistic workloads can be added once the baseline output shape is stable.

## Runtime Requirements

Preferred runtime:

- Linux host or VM with working FUSE/fusermount.
- Built `drive9` CLI.
- Valid drive9 server and credential, either from the active context or
  `DRIVE9_BASE` / `DRIVE9_API_KEY`.
- `curl` for workload-scoped CPU profile control.
- `go tool pprof` for summaries.
- Graphviz for SVG call graphs. If Graphviz is missing, raw `.pprof` files and
  text summaries are still useful.

macOS with macFUSE can run the harness for development smoke tests, but macOS
numbers include Darwin and macFUSE behavior. Use Linux for performance claims
about production FUSE behavior.

## Analysis Workflow

1. Build the CLI from the exact commit under test.
2. Run one workload with a stable remote root and cache directory policy.
3. Check `env.txt` to confirm binary version, git SHA, durability, mount flags,
   and workload parameters.
4. Read `summary.txt` to locate generated profiles.
5. Inspect `cpu-top.txt` and `cpu-callgraph.svg` for hot CPU paths.
6. Inspect heap in-use and allocation summaries separately:
   - in-use shows retained memory;
   - alloc-space shows allocation churn.
7. Inspect `perf.jsonl` for time-series context:
   - rising `heap_alloc_bytes` or `max_rss_bytes`;
   - growing dirty inode or commit queue counts;
   - cache miss spikes;
   - remote operation latency or error increases;
   - FUSE operation count/byte distribution.
8. Compare runs only when commit, workload size, durability, mount flags, host,
   and remote environment are known.

For a running mount, collect a support bundle with:

```bash
drive9 perf collect --mountpoint /mnt/drive9 --duration 60s --out drive9-perf.tar.gz
```

For an existing JSONL file, generate a machine-readable summary with:

```bash
drive9 perf summarize --input perf.jsonl --out summary.json
```

## Overhead Model

Default overhead is zero when profiling flags are absent.

`--perf-jsonl` enables FUSE perf counters even if `--perf-counters` is not set.
Each sample allocates maps for the exported snapshot and reads Go runtime memory
stats plus process rusage. At intervals such as `1s` or `10s`, this is expected
to be low overhead relative to FUSE I/O and remote operations.

CPU profiling adds Go pprof sampling overhead while active. Workload-scoped CPU
profiling is preferred for benchmark runs.

Heap profile writing calls `runtime.GC()` and writes compressed profile data.
Periodic heap profiles are therefore useful for leak/memory-growth analysis but
should stay disabled for CPU-sensitive benchmark runs unless memory timeline is
the target.

## Implementation Map

- `cmd/drive9/cli/mount.go`: CLI flags and FUSE-only validation.
- `cmd/drive9/cli/fuse_bridge*.go`: CLI-to-FUSE option bridge.
- `pkg/fuse/profiling.go`: CPU, heap, live pprof, and CPU control endpoints.
- `pkg/fuse/continuous_perf.go`: JSONL sample recorder.
- `pkg/fuse/perf.go`: low-overhead FUSE and remote operation counters.
- `pkg/fuse/mount.go`: profiling lifecycle integration.
- `perf/mount/run.sh`: reproducible mount/workload runner.
- `perf/mount/scripts/summarize.sh`: pprof summaries and generated artifacts.

## Future Work

- Add CI-oriented continuous perf jobs with fixed workload sizes.
- Add a comparison tool for two run directories.
- Add runtime scheduler, mutex, and block profile capture for concurrency
  investigations.
- Add JSONL post-processing to compute per-interval deltas and p95-ish latency
  estimates from cumulative counters.
- Add customer bundle packaging that redacts credentials but preserves profiles,
  logs, environment metadata, and JSONL samples.
