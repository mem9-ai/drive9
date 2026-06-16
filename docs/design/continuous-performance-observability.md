# Continuous Performance Observability

## Context

drive9 performance problems can originate in several places:

- local FUSE request dispatch;
- Go runtime CPU, allocation, GC, goroutine scheduling, or lock contention;
- local caches, write-back queues, shadow files, and pending indexes;
- client HTTP/TLS and server-side latency;
- backend metadata, db9 small-file storage, or S3 large-file storage;
- workload shape, mount flags, OS, kernel, and FUSE implementation.

Ad-hoc profiling is not enough for customer incidents. A user report such as
"the mount is slow" or "memory keeps growing" needs a small, repeatable support
path that captures the recent time-series context plus targeted profiles.

This design defines a continuous perf system for drive9 as a product-level
facility. FUSE mount is the first producer, but the data model and support
bundle are intentionally not FUSE-only.

## Goals

- Keep low-overhead performance samples available during ordinary operation.
- Allow short, targeted CPU/heap/goroutine captures when a problem is present.
- Package customer-shareable support bundles with redacted context.
- Produce machine-readable summaries that can be compared across workloads,
  commits, hosts, and customer reports.
- Avoid high-cardinality labels and raw customer paths by default.
- Make performance diagnosis data-driven before considering implementation
  changes.

## Non-Goals

- Continuous perf is not a replacement for server-side tracing or database
  observability.
- It does not guarantee complete distributed request tracing in v1.
- It does not collect raw file paths, file contents, API keys, or tokens.
- It does not make macOS/macFUSE results equivalent to Linux FUSE results.

## Architecture

Continuous perf has four layers.

### 1. Low-Overhead Producers

Long-running drive9 processes maintain counters and periodic samples:

- FUSE op counters and latency histograms.
- Remote op counters and latency histograms.
- Cache hit/miss counters.
- Write-back and commit queue depth.
- Runtime memory, goroutine, GC, CPU, and RSS counters.
- Redacted process and mount context.

FUSE mount is the first producer. Server and worker processes can later emit the
same sample envelope with different component names.

### 2. Bounded Local Buffer

Each producer writes local JSONL samples. The writer is bounded using segmented
rotation: once the active segment reaches the configured sample count, the
current file is rotated to `.1` and a fresh segment starts. This keeps disk use
bounded without rewriting a large file on every sample.

For FUSE v1:

```bash
drive9 mount \
  --perf-dir ~/.cache/drive9/perf/mount \
  --perf-interval 1s \
  --perf-max-samples 7200 \
  :/ /mnt/drive9
```

This enables the standard mount profiling suite in one directory. The JSONL
sample file defaults to `~/.cache/drive9/perf/mount/perf.jsonl`. With a
one-second interval and `7200` samples, the active plus previous segment retain
roughly four hours of recent samples.

### 3. Profile Capture

Profiles are captured either by lifecycle hooks or on demand:

- CPU profile: 30s or 60s window.
- Heap profile: final, periodic, or explicit collect-time capture.
- Goroutine profile: collect-time capture for stuck queues or high goroutine
  counts.
- Future: block/mutex profiles for lock contention.
- Future: short Go traces for scheduler/network/GC interactions.

FUSE v1 exposes on-demand captures through the mount pprof server when
`--perf-dir` is set. The listener binds to an ephemeral local address by
default and can be pinned with `--perf-addr`. In the current CLI scope,
`--perf-dir` also writes a mount-lifetime `cpu.pprof` file so the mount can
produce the necessary profile artifacts without a separate collection command.

### 4. Future Support Bundle and Analyzer

The first CLI scope does not add a top-level perf command. A future support
command can package the files produced by `--perf-dir`.

That future bundle would contain:

- recent perf JSONL segments;
- generated `summary.json`;
- CPU/heap/goroutine profiles when available;
- selected mount logs when available;
- redacted manifest with OS/runtime/build/mount context.

A future analyzer can produce `summary.json` from one JSONL file. The analyzer
should stay local and file-based so it can run in CI, developer laptops, and
customer environments without a metrics backend.

## Sample Envelope

The JSONL sample envelope is versioned by shape rather than a strict schema
registry. Unknown fields must be ignored by readers.

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
    "write_policy": "writeback",
    "profile": "interactive"
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
  "remote_ops": {},
  "counters": {},
  "queues": {}
}
```

Path-like context is hashed by default. Raw paths are not used as labels or
operation dimensions.

## Summary JSON

`summary.json` is derived from JSONL and structured for comparison:

- sample count and time range;
- peak runtime memory and goroutine counts;
- CPU seconds and approximate CPU percent over the sample window;
- last cumulative FUSE/remote op counters;
- latency p50/p95/p99/max from low-overhead histograms;
- max queue depths and pending bytes;
- last cache/writeback counters.

The summary format is allowed to grow additively.

## FUSE v1 Implementation Scope

The first implementation lands these pieces:

- `drive9 mount --perf-dir` as the simple standard profiling switch.
- advanced mount overrides:
  `--perf-interval --perf-max-samples --perf-cpu-duration --perf-cpu-interval --perf-heap-interval --perf-addr`.
- segmented JSONL rotation for bounded local retention;
- latency histogram snapshots for FUSE and remote ops;
- redacted mount context in every sample;
- live pprof endpoints and CPU profile start/stop controls;

The following remain future work:

- support bundle and JSONL analyzer commands, with names to be decided later;
- server-side phase timing and distributed correlation;
- block/mutex profile toggles;
- Go trace capture;
- automatic RSS-triggered heap capture;
- CI regression gates;
- zstd bundle output;
- redacted customer upload workflow.

## Operational Guidance

For local profiling:

1. Run the same workload on Linux FUSE when drawing production conclusions.
2. Inspect `perf.jsonl` before opening flame graphs.
3. Use CPU profiles for CPU-bound runs.
4. Use heap in-use for retained memory and alloc-space for churn.
5. Use JSONL queue/cache/remote counters to explain profile hot spots.

For customer support:

1. Ask the customer to mount with `--perf-dir <dir>` and reproduce the issue.
2. Ask them to archive the generated perf directory.
3. Inspect `perf.jsonl` for queue growth, cache misses, CPU, RSS, and remote
   latency.
4. Open pprof files after identifying the likely class of problem.

## Privacy and Cardinality

Continuous perf must not create a path-cardinality metrics system. The default
dimensions are operation type, result, component, and aggregate queue/cache
state. Mount point, remote root, and server URL are represented by short hashes.

Support bundles must not include API keys, tokens, request headers, or file
contents. Logs copied into a bundle should come from explicitly selected mount
logs.
