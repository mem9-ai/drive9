# Drive9 FUSE Benchmark Harness

This directory contains the P0 benchmark harness for Drive9 FUSE optimization
work. It is intentionally a dependency-free Python script so it can run on live
benchmark hosts without provisioning a Python environment.

## Goals

- Produce repeatable raw JSON reports for before/after PR comparisons.
- Cover micro workloads that isolate filesystem paths.
- Cover macro workloads that resemble agent/toolchain behavior.
- Support local, LAN, and WAN/server-over-network labeling.
- Support hot and cold cache runs with explicit methodology recorded in the
  report.

This harness is not a replacement for correctness tests. Cache/writeback PRs
still need unit and integration tests for revision invalidation, multi-client
stale reads, crash recovery, and GC safety.

## Quick Start

Run a hot-cache benchmark against one mounted Drive9 path:

```bash
python3 tools/bench/drive9_fuse_bench.py \
  --target drive9=/mnt/drive9 \
  --environment local \
  --cache-state hot \
  --runs 5 \
  --out /tmp/drive9-bench-hot.json \
  --summary-out /tmp/drive9-bench-hot.md
```

Run a comparable cold-cache report. If the host can drop OS caches, pass an
explicit command:

```bash
python3 tools/bench/drive9_fuse_bench.py \
  --target drive9=/mnt/drive9 \
  --environment local \
  --cache-state cold \
  --drop-caches-command 'sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null' \
  --runs 5 \
  --out /tmp/drive9-bench-cold.json \
  --summary-out /tmp/drive9-bench-cold.md
```

Without `--drop-caches-command`, cold runs still use fresh per-run paths, but
the report marks the cold-cache method as `fresh-path-only`. Do not claim an OS
cold-cache result unless the drop command actually succeeded.

## Server-over-network Runs

Run the script on the client host where the Drive9 FUSE mount lives. Use
metadata flags to make the topology explicit in the JSON report:

```bash
python3 tools/bench/drive9_fuse_bench.py \
  --target drive9=/mnt/drive9 \
  --environment lan \
  --server-url https://drive9.example.com \
  --client-host client-a \
  --server-host server-b \
  --network-note 'same AZ, separate hosts' \
  --cache-state hot \
  --runs 5 \
  --out /tmp/drive9-bench-lan-hot.json \
  --summary-out /tmp/drive9-bench-lan-hot.md
```

The harness does not create a server. It measures the mounted filesystem path
from the client side and records enough topology metadata to compare local,
LAN, and WAN runs.

## Workloads

Micro workloads:

- `sequential_write`: 1 MiB block writes to a 512 MiB file with final fsync.
- `sequential_read`: 1 MiB block reads from a large file.
- `random_read_4k`: 4 KiB random reads from a large file.
- `small_write_4k_fsync`: create/write/close/fsync 4 KiB files.
- `mkdir`: batch directory creation.
- `rename`: batch directory rename.
- `unlink`: batch file unlink.
- `readdir_small`: read a 20-entry directory.
- `readdir_large`: read a large directory.
- `stat`: stat a batch of files.

Macro workloads:

- `macro_git_clone`: clone a generated Git fixture into the target path.
  The fixture source is created outside the target mount, and the default clone
  mode is `--no-local` to avoid local-clone hardlink/cache shortcuts.
- `macro_git_status_dirty`: `git status --porcelain` on a dirty worktree.
- `macro_git_diff_dirty`: `git diff --name-only` on a dirty worktree.
- `macro_find_files`: recursive `find . -type f | wc -l`.
- `macro_go_build`: `go build ./...` when `go` is installed.

Use `--small-count`, `--stat-count`, `--dir-large-count`, `--large-mib`,
`--random-reads`, `--macro-files`, and `--macro-total-mib` to tune workload
size for a host. Use `--git-clone-mode local` only when explicitly measuring
Git local-clone behavior; optimization PRs should normally use the default
`no-local` mode.

The default `--runs 5` is enough for a quick smoke comparison. For release or
PR performance claims, prefer `--runs 20` or higher; with fewer than 20 samples,
the report marks `p95_reliable=false` because p95 is only a rough guardrail.

## JSON Schema

The JSON report keeps these stable top-level fields:

```json
{
  "schema_version": "drive9-bench/v1",
  "version": "<drive9 git sha>",
  "host": "<client hostname>",
  "timestamp": "<UTC ISO8601>",
  "mount_params": {
    "targets": {
      "<name>": {
        "path": "<mount path>",
        "mount": "<matching /proc/mounts line>"
      }
    },
    "server_url": "<optional>",
    "client_host": "<label>",
    "server_host": "<optional>",
    "network_note": "<optional>"
  },
  "environment": "local|lan|wan",
  "cache_state": "cold|hot",
  "cache_prepare": {
    "requested": true,
    "method": "command|fresh-path-only|none",
    "drop_caches_command": "<optional>"
  },
  "results": {
    "<target>:<workload>": {
      "metric": "throughput|latency",
      "unit": "ops/s|MiB/s|seconds",
      "runs": [1.2, 1.3, 1.1],
      "sample_count": 3,
      "median": 1.2,
      "p95": 1.3,
      "p95_method": "linear_interpolation",
      "p95_reliable": false,
      "seconds": [0.1, 0.2, 0.1]
    }
  }
}
```

Optimization PRs should attach the raw JSON before/after reports and summarize
the specific workload deltas in the PR body.

## Local Validation

```bash
python3 -m unittest discover -s tools/bench -p '*test*.py'
```
