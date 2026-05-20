# drive9 FUSE Mount Perf Harness

This directory contains reproducible workloads for profiling `drive9 mount`.

The harness starts a FUSE mount with CPU/heap profiling and continuous JSONL
sampling enabled, runs one workload, unmounts, and writes summaries into a
per-run directory.

## Quick Start

Build the CLI first:

```bash
make build-cli
```

Run a workload using the active drive9 context:

```bash
perf/mount/run.sh small-files
perf/mount/run.sh metadata-walk
perf/mount/run.sh large-write
perf/mount/run.sh large-read
perf/mount/run.sh cold-read
```

Or provide endpoint credentials explicitly:

```bash
DRIVE9_BASE=https://example \
DRIVE9_API_KEY=... \
perf/mount/run.sh small-files
```

## Useful Environment Variables

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
COLD_READ_SEED_MB=256
```

`DRIVE9_PROFILE_CPU_MODE=workload` starts CPU profiling immediately before the
workload script runs and stops it immediately after the script exits. This is
the default because it keeps unmount and cleanup waits out of `cpu.pprof`.
Set `DRIVE9_PROFILE_CPU_MODE=mount` to profile the full mount lifetime instead.

`DRIVE9_PROFILE_HEAP_INTERVAL=0s` disables periodic heap snapshots by default.
Set it to a duration such as `30s` when you need a heap timeline; keep it off
for CPU-sensitive runs because writing heap profiles allocates and compresses
profile data.

`DRIVE9_PERF_JSONL` defaults to `perf.jsonl` in the run directory. Each sample
includes Go runtime heap stats, process CPU/RSS counters, FUSE operation
counters, remote operation counters, cache counters, queue depths, dirty inode
count, and open handle counts. `DRIVE9_PERF_INTERVAL` defaults to `1s` in the
harness. `DRIVE9_PERF_MAX_SAMPLES` bounds one JSONL segment before rotation to
`.1`. Set `DRIVE9_PERF_JSONL=` to disable continuous samples for a run.

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

- `small-files`: creates many small files.
- `metadata-walk`: creates a modest tree, then runs `find` and `stat`.
- `large-write`: writes one large sequential file.
- `large-read`: reads one large sequential file, creating it first if missing.
- `cold-read`: seeds one large file through the Drive9 API, then reads it
  through FUSE so the mount read path cannot reuse data written by the same
  FUSE session.

These workloads are intentionally simple shell scripts so profile results are
easy to reproduce and reason about.

## Explicit Remote Sync

For a profiled FUSE mount, the pprof control server also exposes a Drive9 sync
fence. It flushes open handles and waits for background remote write queues
without unmounting:

```bash
drive9 perf sync --mountpoint /tmp/drive9-perf-mnt --timeout 5m
```

This is stronger than relying on a shell `sync` command for the harness because
go-fuse does not currently expose Linux `syncfs` to the filesystem
implementation.

## Runtime Requirements

This harness must run on a host that can perform a real FUSE mount. Linux with
FUSE/fusermount is the preferred target for drive9 FUSE performance work. macOS
can run it through macFUSE, but results include macFUSE and Darwin kernel
behavior and should not be treated as Linux production numbers. WebDAV mode is
not used by this harness.

The runner also expects `curl` for workload-scoped CPU profile control and
`go tool pprof` for text summaries. SVG flame/call graphs require Graphviz; if
Graphviz is missing, the run still completes and keeps the raw `.pprof` files.
