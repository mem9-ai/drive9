# Drive9 FUSE Blackbox Suite

This directory contains the self-contained Drive9 FUSE blackbox runner. It does
not call scripts from `e2e/` or `bench/`; it only builds and executes product
binaries from this checkout.

## Model

There are three real suites:

- `functional`: FUSE correctness and usability.
- `posix`: pjdfstests on a Drive9 FUSE mount.
- `perf`: FUSE micro workloads plus Git/agent-like workloads.

Presets only select suite tiers:

- `smoke`: FUSE prerequisite check plus the smallest functional subset.
- `standard`: full functional plus core POSIX groups.
- `daily`: full functional, full POSIX, full performance with three runs.

## Commands

```bash
make blackbox-fuse-smoke
make blackbox-fuse-standard
make blackbox-fuse-daily
make blackbox-fuse-suite SUITE=functional
make blackbox-fuse-suite SUITE=posix
make blackbox-fuse-suite SUITE=perf BLACKBOX_FUSE_RUNS=3
```

Direct runner usage:

```bash
python3 blackbox/fuse/run.py --preset smoke
python3 blackbox/fuse/run.py --preset standard
python3 blackbox/fuse/run.py --preset daily --runs 3
python3 blackbox/fuse/run.py --suite functional --tier daily
```

## Server Modes

Default `--server-mode auto` uses `DRIVE9_BASE` when set; otherwise it starts a
local `drive9-server-local`.

For local mode, the runner uses `DRIVE9_LOCAL_DSN` when set. If it is unset, it
starts a disposable MySQL container with Docker or Podman.

Useful environment variables:

```bash
DRIVE9_BASE=http://127.0.0.1:9009
DRIVE9_API_KEY=drive9_xxx
DRIVE9_LOCAL_DSN='root:pass@tcp(127.0.0.1:3306)/drive9_local?parseTime=true'
BLACKBOX_FUSE_SERVER_MODE=existing
BLACKBOX_FUSE_STRICT=1
BLACKBOX_FUSE_REPOS=drive9,kimi-code
BLACKBOX_FUSE_RUNS=3
PJDFSTEST_DIR=/path/to/pjdfstest
PJDFSTEST_ALLOW_NONROOT=1
```

## Platform Requirements

Linux:

- `/dev/fuse`
- `fusermount3` or `fusermount`

macOS:

- macFUSE or FUSE-T mount helper, `mount_macfuse` or `mount_fusefs`

When FUSE prerequisites are missing, `smoke` writes a SKIP report and exits 0
unless `BLACKBOX_FUSE_STRICT=1` or `--strict-prereqs` is set. `standard` and
`daily` are strict by default.

## Outputs

Each run writes:

```text
blackbox/results/fuse/<session>/
  manifest.json
  events.jsonl
  functional-results.jsonl
  pjdfstests.json
  perf-results.json
  daily-report.md
  logs/
  mount-logs/
```

`perf-results.json` keeps raw values for every run and reports mean, median,
min, max, and standard deviation. `pjdfstests.json` reports both raw pass rate
and effective pass rate after known XFAIL rows from
`config/pjdfstests-allowlist.json`.
