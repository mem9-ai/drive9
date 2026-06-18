# Drive9 FUSE Blackbox Suite

This directory documents the FUSE suite for the shared Drive9 blackbox harness.
The generic harness lives under `blackbox/harness`; FUSE-specific suite
configuration lives under `blackbox/suites/fuse`.

The current scope is FUSE only, including macFUSE/FUSE-T tolerant behavior on
macOS. WebDAV is intentionally out of scope.

## What It Tests

The framework is organized around modules, not around one-off scripts.

- `community.*`: open source community filesystem test suites or tools, such
  as pjdfstest, LTP, fio, mdtest, vdbench, fsx, xattr, and lock checks.
- `ported.juicefs.*`: Drive9-owned equivalent rewrites inspired by generic
  filesystem tests used by JuiceFS. These are not vendored JuiceFS source.
- `git.official.*`: upstream Git test suites, split into functional and
  performance modules.
- `drive9.workflow.*`: Drive9 CLI workflows that are tightly coupled with FUSE,
  such as `drive9 git clone --fast`, blobless clone, worktrees, profile
  auto-pack, `umount --pack-path`, portable profile, explicit pack/unpack, and
  coding-agent local overlay build behavior.
- `drive9.customer.*`: reproducible customer-scenario benchmarks. These are
  opt-in modules because they can create large datasets and long-running load.

The category names describe where the test idea or suite comes from. They do
not imply that the tested filesystem is JuiceFS or Git. The tested filesystem is
always Drive9 FUSE unless a module explicitly documents otherwise.

## Directory Layout

Module code is intentionally split by file under `blackbox/suites/fuse/modules/`:

```text
blackbox/
  run.py                           generic blackbox entrypoint
  harness/
    runner.py                       CLI, selection, lifecycle, reporting
    suite.py                        suite provider protocol and loader
    deps.py                         generic dependency cache helpers
    capabilities.py                 generic host capability detection
  suites/
    fuse/
      provider.py                   FUSE suite lifecycle and wiring
      target.py                     Drive9 CLI/server/FUSE mount provider
      deps.py                       pjdfstest/LTP/fio/Git/etc preparation
      capabilities.py               macFUSE/FUSE-T/Linux FUSE detection
      modules.json
      repos.json
      allowlists/
      modules/
        registry.py                 FUSE module registry only
        base.py                     FUSE module helpers
        community_pjdfstest.py      community.pjdfstest
        community_ltp.py            community.ltp.fs / community.ltp.syscalls
        community_fio.py            community.fio
        community_mdtest.py         community.mdtest
        community_vdbench.py        community.vdbench
        community_pyxattr.py        community.pyxattr
        community_fsx.py            community.fsx
        community_lock.py           community.lock
        ported_juicefs_*.py         ported.juicefs.* modules
        git_official_*.py           git.official.* modules
        drive9_workflow_*.py        drive9.workflow.* modules
        drive9_customer_*.py        customer-scenario benchmark modules
```

New modules should get their own file unless they are a tiny variant of an
existing module family, such as the two LTP modules in `community_ltp.py`.

## Selection Model

The suite exposes modules and selectors. It does not define run-profile policy.
Callers decide when to run blackbox and which selector to use.

The FUSE suite also defines named module groups:

- `functional`: `ported.juicefs.*` plus Drive9 workflow modules.
- `posix`: `community.pjdfstest`.
- `perf`: performance modules such as fio, mdtest, vdbench, Git perf, and
  Drive9 workflow perf.
- `customer`: opt-in customer-scenario modules. These are intentionally not
  part of `perf` because callers must explicitly choose the dataset scale and
  target environment.

## Commands

Common entry points:

```bash
make blackbox
```

Module-oriented entry points:

```bash
make blackbox-list
make blackbox BLACKBOX_SELECTOR=group:posix
make blackbox BLACKBOX_SELECTOR=category:drive9.workflow
make blackbox BLACKBOX_SELECTOR=module:community.pjdfstest
make blackbox BLACKBOX_SELECTOR=group:customer
make blackbox-deps BLACKBOX_SELECTOR=group:perf
```

Direct runner usage:

```bash
python3 blackbox/run.py --suite fuse --all --runs 3
python3 blackbox/run.py --suite fuse --group posix
python3 blackbox/run.py --suite fuse --category drive9.workflow
python3 blackbox/run.py --suite fuse --module drive9.workflow.git_blobless
python3 blackbox/run.py --suite fuse --list --format json
```

`--deps-only` prepares external test-suite dependencies without starting
Drive9 or mounting FUSE:

```bash
python3 blackbox/run.py --suite fuse --all --deps-only
python3 blackbox/run.py --suite fuse --group perf --deps-only
```

## Runtime Model

The runner does the following:

1. Detects host capabilities: OS, FUSE helper, root/non-root, xattr, locking,
   and common tools.
2. Resolves the selected modules from `--all`, `--category`, `--module`, or
   suite-local `--group`.
3. Prepares module dependencies from environment variables, PATH, or
   `blackbox/cache`.
4. Builds `drive9` and starts a local `drive9-server-local`, unless
   `--drive9-cli` and/or `DRIVE9_BASE` point to existing components.
5. Creates one or more isolated Drive9 remote roots and FUSE mountpoints per
   module.
6. Records structured results, metrics, raw logs, and artifacts.

A module can return these statuses:

- `PASS`: behavior matched expectations.
- `FAIL`: likely Drive9 product regression or test failure.
- `SKIP`: dependency, platform, or capability is unavailable.
- `XFAIL`: known incompatibility that should be tracked but not fail the run.
- `WARN`: non-fatal warning. This is reserved for future richer assertions.

## Server Modes

Default `--server-mode auto` uses `DRIVE9_BASE` when set. Otherwise it starts a
local `drive9-server-local`.

For local mode, the runner uses `DRIVE9_LOCAL_DSN` when set. If it is unset, it
starts a disposable MySQL container with Docker or Podman.

Useful environment variables:

```bash
DRIVE9_BASE=http://127.0.0.1:9009
DRIVE9_API_KEY=drive9_xxx
DRIVE9_LOCAL_DSN='root:pass@tcp(127.0.0.1:3306)/drive9_local?parseTime=true'
BLACKBOX_SUITE=fuse
BLACKBOX_SERVER_MODE=existing
BLACKBOX_STRICT=1
BLACKBOX_RUNS=3
BLACKBOX_REPOS=drive9,kimi-code
BLACKBOX_DRIVE9_CLI=/path/to/drive9
BLACKBOX_OFFLINE=1
BLACKBOX_KEEP_ARTIFACTS=1
BLACKBOX_QUIET=1
BLACKBOX_LOCAL_OVERLAY_PREWARM=0
BLACKBOX_LOCAL_OVERLAY_VERIFY_REMOTE=0
```

## Kimi Performance Module

`drive9.customer.kimi_perf` codifies the Kimi sandbox workload:

- namespace scale at 100MB/1k files, 1GB/10k files, and 10GB/100k files;
- single-directory and sharded-tree layouts;
- mount, `ls`, `ls -l`, `find`, name-pattern `find`, and sampled `stat` latency;
- small-file create, overwrite, append, partial edit, read, and stat-after-write
  QPS plus p50/p95/p99/max;
- close, `fsync`, and `fdatasync` write latency, including separate reader
  mount/cache visibility checks;
- three measurement rounds by default, controlled by `BLACKBOX_RUNS` or
  `BLACKBOX_KIMI_PERF_RUNS`;
- unmount/remount persistence checks for the "next sandbox sees previous data"
  requirement;
- same-host multi-mount validation for a configured set of mount counts.

This module is never executed accidentally. It is registered in the `customer`
group, but `ensure_dependencies` skips it unless explicitly enabled:

```bash
BLACKBOX_KIMI_PERF_ENABLE=1 \
BLACKBOX_SERVER_MODE=existing \
DRIVE9_BASE=http://drive9.pingkai.cn \
make blackbox BLACKBOX_SELECTOR=module:drive9.customer.kimi_perf
```

The default configuration runs only the S scale. Full customer scale requires
explicit selection:

```bash
BLACKBOX_KIMI_PERF_ENABLE=1 \
BLACKBOX_RUNS=3 \
BLACKBOX_KIMI_PERF_SCALES=S,M,L \
BLACKBOX_KIMI_PERF_SMALL_OPS=10000 \
BLACKBOX_KIMI_PERF_FLUSH_OPS=10000 \
BLACKBOX_KIMI_PERF_SOAK=1 \
BLACKBOX_KIMI_PERF_SOAK_MINUTES=30 \
BLACKBOX_SERVER_MODE=existing \
DRIVE9_BASE=http://drive9.pingkai.cn \
make blackbox BLACKBOX_SELECTOR=module:drive9.customer.kimi_perf
```

Useful tunables:

```bash
BLACKBOX_KIMI_PERF_LAYOUTS=single,tree
BLACKBOX_KIMI_PERF_PROFILE=coding-agent
BLACKBOX_KIMI_PERF_DURABILITY=auto
BLACKBOX_KIMI_PERF_RUNS=3
BLACKBOX_KIMI_PERF_STAT_SAMPLES=1000
BLACKBOX_KIMI_PERF_SMALL_SIZES=1024,4096,20480,102400,1048576
BLACKBOX_KIMI_PERF_SMALL_CONCURRENCY=1,4,16,64
BLACKBOX_KIMI_PERF_FLUSH_SIZES=1024,4096,20480,102400,1048576
BLACKBOX_KIMI_PERF_FLUSH_CONCURRENCY=1,4,16,64
BLACKBOX_KIMI_PERF_VISIBILITY_SAMPLES=100
BLACKBOX_KIMI_PERF_MOUNT_COUNTS=1,2,5,10
BLACKBOX_KIMI_PERF_REMOTE_ROOT=/some/reusable/remote/root
BLACKBOX_KIMI_PERF_REUSE_DATASETS=1
BLACKBOX_KIMI_PERF_RAW=1
```

Per-section switches are also available:

```bash
BLACKBOX_KIMI_PERF_NAMESPACE=1
BLACKBOX_KIMI_PERF_SMALL_FILE=1
BLACKBOX_KIMI_PERF_FLUSH=1
BLACKBOX_KIMI_PERF_PERSISTENCE=1
BLACKBOX_KIMI_PERF_MULTI_MOUNT=1
BLACKBOX_KIMI_PERF_SOAK=0
```

Outputs are written under the normal run directory:

```text
blackbox/results/fuse/<session>/artifacts/drive9.customer.kimi_perf/
  environment.json
  manifest.json
  raw_results/*.jsonl
  summary/summary.csv
  summary/summary.json
  report.md
```

`report.md` is the customer-facing summary table. `summary.csv` is meant for
spreadsheets and trend dashboards. `raw_results/*.jsonl` contains per-operation
latency records for deeper p95/p99/debug analysis.

Important scope notes:

- Same-host multi-mount results are reported as "validated on this host" only.
  They are not a true multi-VM or multi-sandbox upper bound.
- Cold namespace mount measurements use unique cache directories per mount.
- Cross-mount visibility uses separate writer and reader cache directories.
- True same-zone cloud measurement should run on a VM or compute instance in
  the same zone as the target Drive9 deployment, using
  `BLACKBOX_SERVER_MODE=existing`.

## External Dependencies

The framework prefers already-installed tools, then environment-provided paths,
then cached auto-fetch under:

```text
blackbox/cache/
```

Important dependency overrides:

```bash
PJDFSTEST_DIR=/path/to/pjdfstest
PJDFSTEST_TESTS=/path/to/pjdfstest/tests
PJDFSTEST_BIN=/path/to/pjdfstest
PJDFSTEST_ALLOW_NONROOT=1
GIT_TEST_SOURCE_DIR=/path/to/git
BLACKBOX_GIT_TEST_REF=v2.46.2
LTP_ROOT=/path/to/ltp
FIO_BIN=/path/to/fio
MDTEST_BIN=/path/to/mdtest
VDBENCH_BIN=/path/to/vdbench
FSX_BIN=/path/to/fsx
```

Dependency metadata lives in `blackbox/suites/fuse/dependencies.json`.
Generated dependency metadata is written next to cached dependencies when a
module prepares them.

## Platform Notes

Linux requirements:

- `/dev/fuse`
- `fusermount3` or `fusermount`

macOS requirements:

- macFUSE or FUSE-T mount helper: `mount_macfuse` or `mount_fusefs`

Some POSIX cases are not expected to behave identically on macOS, especially
permission, ownership, flags, and case-sensitivity-adjacent behavior. Modules
should classify these as `SKIP` or `XFAIL` through capability checks or
allowlists instead of failing the whole run as a Drive9 regression.

## Reports

Every run writes to:

```text
blackbox/results/fuse/<session>/
  manifest.json
  results.json
  results.jsonl
  metrics.json
  events.jsonl
  report.md
  artifacts/
  logs/
  mount-logs/
```

`report.md` is the human-readable run summary. `results.json` is the stable
machine-readable result file. `metrics.json` stores raw metric rows and
aggregate summaries. Performance modules use three runs by default and report
mean, median, min, max, and standard deviation.

## GitHub Actions

`.github/workflows/blackbox.yml` is one possible caller. It runs the selector
provided by workflow inputs, or `all` by default. Any recurring or release-gate
schedule belongs to the workflow configuration, not to the blackbox harness.

The workflow caches `blackbox/cache` and uploads
`blackbox/results/fuse/**` as artifacts. It runs `make blackbox-deps` first so
dependency setup is separated from the actual test run.

## Adding A Module

Add a module when the behavior is reusable, externally meaningful, or likely to
grow. A module can wrap an upstream suite, an equivalent rewrite of a generic FS
stress case, or a Drive9-specific workflow.

1. Add a new file under `blackbox/suites/fuse/modules/`, named after the module
   family or module ID, for example `drive9_workflow_new_case.py`.
2. Give it a stable `id`, `category`, `description`, `labels`, and `timeout`.
3. Implement `ensure_dependencies(ctx)` when it needs external tools.
4. Implement `run(ctx)` and return a small metrics/details dictionary.
5. Mount through `ctx.target.mount(...)` and always unmount in `finally`.
6. Use `ModuleSkip`, `ModuleXFail`, `DependencyUnavailable`, or `BlackboxError`
   for clear classification.
7. Import and register the module in `blackbox/suites/fuse/modules/registry.py`.
8. Add configuration in `blackbox/suites/fuse/modules.json` when the module needs tunables.
9. Add it to a named group in `blackbox/suites/fuse/modules.json` only when a
   stable group selector should include it.
10. Update this README if the module introduces a new dependency or behavior
    class.

Keep module IDs stable because CI reports and dashboards can depend on them.

## Drive9 Workflow Modules

Drive9 CLI features that are meaningful only with FUSE belong under
`drive9.workflow.*`, not under generic POSIX or community categories.

Current workflow modules:

- `drive9.workflow.git_fast_clone`
- `drive9.workflow.git_blobless`
- `drive9.workflow.git_worktree`
- `drive9.workflow.auto_pack_profile`
- `drive9.workflow.auto_pack_umount_path`
- `drive9.workflow.portable_pack`
- `drive9.workflow.pack_unpack_cli`
- `drive9.workflow.pack_git_clone`
- `drive9.workflow.perf`
- `drive9.workflow.local_overlay_build`

These cover Git workspace registration, fast clone modes, auto pack/unpack,
explicit pack/unpack, build, grep/rg, edit, and commit paths that are central
to Drive9 usability.

`drive9.workflow.local_overlay_build` is narrower than the general workflow
performance module. It compares native checkout/build samples with FUSE samples
mounted as `--profile=coding-agent`, using the repo-specific `local_overlay`
configuration from `repos.json`. The FUSE sample writes probe files under
local-only paths, checks that they land under `<local-root>/overlay`, then
remounts the same remote root with `profile=none` to verify that those probes
are not persisted remotely. This is the coverage for coding-agent local overlay
semantics and local-dependency/build-output performance.

Run it directly when you want this heavier matrix:

```bash
make blackbox BLACKBOX_SELECTOR=module:drive9.workflow.local_overlay_build
```
