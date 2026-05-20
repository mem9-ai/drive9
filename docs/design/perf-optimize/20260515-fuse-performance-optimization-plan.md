# FUSE Performance Optimization Plan

This document records the first Drive9 FUSE performance optimization phase and
the follow-up plan. The phase started from EC2 Singapore profiling data and
focused on two low-risk improvements: better performance diagnostics and lower
large-write allocation.

## Context

Profiling compared `drive9 mount --mode=fuse` from a local OrbStack Linux VM
and from an EC2 instance in Singapore, near the current Drive9 service region.
Moving the client closer to the service reduced regional network noise but did
not change the bottleneck shape:

- FUSE local operations are generally microsecond-scale.
- CPU stays below 2% for the tested workloads.
- Small-file writes are dominated by per-file remote commit latency.
- Metadata scans can hit remote `list` / `stat` long tails, especially while
  the commit queue is still uploading.
- Large writes allocated much more than the file size in the Go client path.

This data does not support a Rust rewrite of the FUSE layer as the first
optimization. The higher-value targets are remote round trips, commit queue
throughput, metadata long tails, read prefetch behavior, and avoidable Go
allocations.

## Phase 1 Scope

This phase is split into two reviewable PRs: one for perf diagnostics and this
report, and one companion PR for the large-allocation code changes.

Implemented across the phase:

- Make `perf/mount/run.sh` portable outside a full git checkout.
- Add `cold-read` to the perf harness.
- Add a profiled mount sync control endpoint:
  `/debug/drive9/mount/sync`.
- Add `drive9 perf sync` to call that endpoint.
- Add non-stopping queue wait methods used by sync control.
- Record the actual pprof listener address when `--pprof-addr 127.0.0.1:0`
  is used.
- Reduce large-write allocation by growing `WriteBuffer` parts geometrically.
- Bound multipart upload buffer preallocation by actual part count.

Not implemented in this phase:

- Commit queue concurrency matrix.
- Small-file batch commit protocol.
- Metadata list/stat long-tail optimization.
- Read prefetch allocation optimization.

## Test Method

The optimized worktree was cross-compiled locally and copied to the Singapore
EC2 host:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o drive9-linux-amd64 ./cmd/drive9
```

The EC2 runs used the same FUSE perf harness shape as the previous baseline:

- Host: EC2 in `ap-southeast-1`.
- Mode: `drive9 mount --mode fuse`.
- Durability: `interactive`.
- CPU profile mode: workload-scoped.
- Continuous perf interval: `1s`.
- Heap: final heap profile only.
- Workloads:
  - `small-files`: 50 files, 128-byte payload.
  - `metadata-walk`: 10 directories x 5 files.
  - `large-write`: 32 MiB sequential write.
  - `large-read`: 32 MiB seeded through the same FUSE mount, then read.
  - `cold-read`: 32 MiB seeded through the Drive9 API, then read through FUSE.

Baseline profile roots:

- `perf/mount/profiles-ec2/20260520-060851-singapore-ec2-fuse`
- `perf/mount/profiles-ec2/20260520-061312-singapore-ec2-fuse-rest`

Optimized profile root:

- `perf/mount/profiles-ec2/20260520-065109-current-opt`

Limitations:

- Each row below is a single EC2 run, so network and service-side jitter are
  not fully controlled.
- `large-read` is not a cold remote read because it creates the file through
  the same mount first. `cold-read` was added to close that observability gap.
- The EC2 host did not have `go`, so new pprof text/SVG summaries were
  regenerated locally from the raw `.pprof` files and the exact Linux binary.

## Comparison Results

### Workload Summary

| Workload | Baseline Wall | Optimized Wall | Baseline Heap Alloc Max | Optimized Heap Alloc Max | Baseline RSS | Optimized RSS | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `small-files` | 69.57s | 68.87s | 9.0 MiB | 10.0 MiB | 17.5 MiB | 18.8 MiB | No material change |
| `metadata-walk` | 75.85s | 75.85s | 7.7 MiB | 11.1 MiB | 17.0 MiB | 19.1 MiB | No material change |
| `large-write` | 9.88s | 10.07s | 130.8 MiB | 34.9 MiB | 49.4 MiB | 54.6 MiB | Allocation fixed; wall time still remote-bound |
| `large-read` | 9.90s | 9.98s | 130.8 MiB | 34.9 MiB | 55.0 MiB | 48.1 MiB | Allocation fixed in seed write path |

### Remote and FUSE Timings

| Workload | Baseline Remote Write Avg | Optimized Remote Write Avg | Baseline Commit Drain | Optimized Commit Drain | Baseline FUSE Write Avg | Optimized FUSE Write Avg |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `small-files` | 5.14s | 5.11s | 66.82s | 66.15s | 0.004 ms | 0.003 ms |
| `metadata-walk` | 5.45s | 5.44s | 38.38s | 35.65s | 0.003 ms | 0.004 ms |
| `large-write` | 9.41s | 9.59s | 9.41s | 9.60s | 1.42 ms | 1.04 ms |
| `large-read` | 9.44s | 9.48s | 9.41s | 9.50s | 1.45 ms | 1.02 ms |

Interpretation:

- `small-files` did not improve because this phase did not reduce the number
  of remote writes. The run still performed 50 remote writes for 50 files.
- `small-files` wall time matches the current model: approximately
  `50 files * 5.1s / 4 upload workers = 64s`, plus mount/setup overhead.
- `large-write` and `large-read` wall time did not improve because the remote
  upload still dominates the workload.
- `large-write` and `large-read` allocation improved substantially because the
  local write path now avoids exact-length part reallocations and avoids
  preallocating upload buffers for workers that cannot be used.

## Heap Profile Results

### Large Write

Alloc-space pprof for `large-write`:

| Hotspot | Baseline | Optimized | Change |
| --- | ---: | ---: | ---: |
| Total alloc-space | 288.47 MiB | 107.47 MiB | -62.7% |
| `fuse.(*WriteBuffer).Write` | 144.81 MiB | 58.74 MiB | -59.4% |
| `client.newUploadBufferPool` | 128.00 MiB | 32.00 MiB | -75.0% |

The remaining allocation hotspot is still `WriteBuffer.Write`, but it is now
closer to expected copy cost for 32 MiB of sequential data. The upload buffer
pool now allocates one 8 MiB buffer for each actual part in the 32 MiB upload
instead of allocating the maximum 16-worker pool.

### Large Read

`large-read` still seeds data through FUSE, so the write path dominates the
heap profile before the read begins:

| Hotspot | Baseline | Optimized | Change |
| --- | ---: | ---: | ---: |
| Total alloc-space | 329.17 MiB | 143.04 MiB | -56.6% |
| `fuse.(*WriteBuffer).Write` | 144.81 MiB | 60.78 MiB | -58.0% |
| `client.newUploadBufferPool` | 128.00 MiB | 32.00 MiB | -75.0% |
| `fuse.(*Dat9FS).Read` | 41.93 MiB | 31.13 MiB | -25.8% |

This confirms why `large-read` alone was a misleading read benchmark: its heap
profile mostly measured the FUSE write used to create the test file.

## Cold Read Finding

The new `cold-read` workload seeds a 32 MiB file through the Drive9 API, then
reads it through FUSE. This isolates the read path from the FUSE write path.

`cold-read` result:

| Metric | Value |
| --- | ---: |
| Wall time | 11.41s |
| CPU | 1.19% |
| RSS max | 61.3 MiB |
| Heap alloc max | 37.0 MiB |
| Remote read count | 6 |
| Remote read bytes | 48 MiB |
| Remote read avg | 123 ms |
| FUSE read count | 128 |
| FUSE read bytes | 32 MiB |

Alloc-space pprof exposed a new read-side hotspot:

| Hotspot | Alloc-space |
| --- | ---: |
| `io.ReadAll` | 109.81 MiB |
| `fuse.(*Prefetcher).startPrefetch.func1` | 46.38 MiB cumulative |
| `fuse.NewServer.func2` | 6.98 MiB |

This means read prefetch is now a first-class optimization track. The current
prefetch path can allocate by reading whole ranges into memory and copying
again, and the `cold-read` run read 48 MiB remotely for a 32 MiB user read.
The next read-path optimization should avoid `ReadAll`-style whole-buffer
materialization where possible and bound prefetch over-read.

## Optimization Tracks

### Track 1: Make Perf Runs More Diagnostic

Status: implemented in phase 1.

Implemented:

- `cold-read` workload.
- profiled mount sync endpoint.
- `drive9 perf sync`.
- actual pprof address recording for `127.0.0.1:0`.

The sync control path is intentionally opt-in and does not change normal
directory `fsync` behavior.

### Track 2: Reduce Large-Write Allocation

Status: implemented in phase 1, with code changes split into a companion PR.

Implemented:

- `WriteBuffer.Write` now grows part capacity geometrically instead of
  reallocating at exact lengths.
- Multipart upload parallelism is bounded by actual part count, so upload
  buffer preallocation scales with real work.

Measured result:

- `large-write` heap alloc max: `130.8 MiB -> 34.9 MiB`.
- `large-write` alloc-space pprof total: `288.47 MiB -> 107.47 MiB`.

### Track 3: Small-File Remote Commit Throughput

Status: not implemented yet.

The current commit queue hides close latency but still performs one remote
write per small file. The EC2 small-file run confirms this:

- 50 files generated 50 remote writes.
- remote write average was about 5.1s.
- default upload workers were 4.
- wall time was about 69s.

Next steps:

- Run a commit queue concurrency matrix with `--upload-concurrency=1,2,4,8,16`
  on the same EC2 host.
- Add a server-supported small-file batch commit API if higher client-side
  concurrency stops helping or starts hurting tail latency.
- Preserve per-file status and revision responses so failed files can fall
  back to the existing single-file commit path.

Expected impact:

- Worker tuning may provide incremental wall-time improvement.
- Batch commit is the likely order-of-magnitude improvement because it reduces
  per-file HTTP/server transaction overhead.

### Track 4: Metadata Long Tails

Status: not implemented yet.

Metadata scans currently depend on remote `stat` and `list`. Improvements
should focus on:

- batch stat/list APIs for directory walks,
- better negative and positive namespace cache coverage,
- reducing metadata scans while commit-queue self changes are still arriving,
- recording phase timing on client and server for slow `list` calls.

### Track 5: Read Prefetch Allocation and Over-Read

Status: newly discovered from `cold-read`.

The cold-read heap profile shows `io.ReadAll` and prefetch as dominant
allocation sources. The read path should be optimized after Track 3 measurement
because it is now clearly observable and independent of the write seed path.

Candidate changes:

- Replace `io.ReadAll` in prefetch with bounded reads into reusable buffers.
- Avoid storing duplicate full-byte slices when a range can be streamed or
  directly inserted into the read cache.
- Bound prefetch distance and aggregate bytes so a 32 MiB user read does not
  trigger materially more remote bytes than needed.
- Add perf counters for prefetch remote bytes, cached bytes, dropped bytes, and
  over-read ratio.

## Recommended Next Phase

1. Run the commit queue concurrency matrix on the Singapore EC2 host.
2. Decide whether worker tuning is enough for the current service capacity.
3. Design `batch-write-small` if per-file remote write latency remains dominant.
4. Keep `cold-read` in every EC2 run so read-path regressions are visible.
5. Implement read prefetch allocation/over-read fixes after small-file commit
   measurements are in hand.
