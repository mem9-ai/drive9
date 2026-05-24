# drive9 Benchmark Harness

This directory contains portable benchmark infrastructure for comparing repo
clone/build workloads on native disk versus a drive9 FUSE mount.

## Quick Start

```bash
bench/bin/bootstrap-host.sh
bench/bin/run-repo-build.py doctor --dry-run

export BENCH_HOME=/mnt/drive9-bench
export DRIVE9_API_KEY=...

bench/bin/run-repo-build.py doctor
bench/bin/run-repo-build.py run
```

Results are written under `$BENCH_HOME/results/<session>/`:

- `events.jsonl`: raw phase, cleanup, manifest, and FUSE perf events
- `manifest.json`: case, session, storage order, and resolved repo commits
- `environment.json`: host/tool versions, disk/mount output, and redacted env
- `summary.csv` and `summary.md`: aggregate timings and FUSE/native ratios
- `logs/`: stdout/stderr for each clone, build, mount, and unmount phase

## Case

The default case is `bench/cases/repo-build.json`.

It runs three repeats for each repo and storage:

- TypeScript: `sst/opencode`, branch `dev`
- Python: `MoonshotAI/kimi-cli`, branch `main`
- Rust: `openai/codex`, branch `main`
- Go: `mem9-ai/drive9`, branch `main`

Each run resolves the target ref with `git ls-remote` before measurement and
then uses `git clone --no-checkout` plus `git checkout --detach <sha>` for the
measured clone phase.

## drive9 CLI

`bench/bin/bootstrap-host.sh` installs the production drive9 CLI with:

```bash
curl -fsSL https://drive9.ai/install.sh | sh
```

The benchmark runner then mounts production drive9 using `DRIVE9_API_KEY`.
Set `BENCH_DRIVE9_CLI` only when you need to point at a specific CLI binary.

## Useful Debug Runs

```bash
# Native-only smoke without network ref resolution or command execution.
bench/bin/run-repo-build.py run --native-only --dry-run --no-resolve --skip-prewarm --runs 1

# Full unattended run with clone/build-specific timeouts.
BENCH_CLONE_TIMEOUT_SECONDS=300 BENCH_BUILD_TIMEOUT_SECONDS=1200 bench/bin/run-repo-build.py run

# Long full run, evaluating the first repo completely before moving on.
BENCH_CLONE_TIMEOUT_SECONDS=1800 BENCH_BUILD_TIMEOUT_SECONDS=1800 bench/bin/run-repo-build.py run --repo-major --stop-after-first-repo-timeout --continue-on-error

# Long run with extra Git fsync boundaries and low-level checkout fallback.
BENCH_GIT_FSYNC=1 BENCH_GIT_CHECKOUT_MODE=read-tree BENCH_GIT_FSYNC_SETTLE_SECONDS=2 BENCH_CLONE_TIMEOUT_SECONDS=1800 BENCH_BUILD_TIMEOUT_SECONDS=1800 bench/bin/run-repo-build.py run --repo-major --stop-after-first-repo-failure --continue-on-error

# Fast FUSE clone probe before spending time on the full native-vs-FUSE matrix.
BENCH_STORAGES=fuse BENCH_REPOS=drive9 BENCH_RUNS=1 BENCH_CLONE_TIMEOUT_SECONDS=180 bench/bin/run-repo-build.py run --clone-only --skip-prewarm --continue-on-error

# Continue a session after interruption, skipping samples already in events.jsonl.
bench/bin/run-repo-build.py run --session <session> --resume --continue-on-error

# Rebuild summaries after editing or collecting events.
bench/bin/run-repo-build.py summarize "$BENCH_HOME/results/<session>"
```

## Notes

- Dependency caches live under `$BENCH_HOME/cache` and are prewarmed outside
  measured timings.
- Each measured sample uses a fresh checkout and deletes it after build.
- Go builds use a per-sample empty `GOCACHE` while retaining the shared
  `GOMODCACHE`.
- FUSE samples mount and unmount per sample. The default FUSE flags use
  `--durability=interactive` so clone/build benchmarks exercise the loosest
  writeback path instead of auto-resolving to strict mode on low-latency hosts.
  Unmount is intentionally outside the measured clone/build phases and drains
  pending writeback before the next sample.
