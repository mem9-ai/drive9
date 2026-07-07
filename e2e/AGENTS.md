---
title: e2e - Live end-to-end scripts
---

## Overview

This directory contains live end-to-end tests for deployed drive9-server instances.
These scripts are integration probes (not unit tests) and call real HTTP endpoints.

## CI wiring rule

Every new e2e script MUST be wired into `.github/workflows/local-e2e.yml` in the
same PR that adds it (PR gate for fast suites, push-to-main/schedule toggles for
heavy ones), or be documented as manual-only with a reason in `e2e/README.md`
("CI automation tiers" section). An e2e script that no automation runs is dead
code. The `e2e-all` workflow (manual dispatch) must keep covering every wired
suite via `run_all_e2e=1`.

## Run

Use a hosted deployment by default. For local development on this machine, use
`drive9-server-local` instead.

### Hosted endpoints

#### Deployment endpoints

Current shared dev deployment:

```bash
# Dev
export DRIVE9_BASE="http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com"

# Dev (tidbcloud-native)
export DRIVE9_BASE="http://k8s-drive9ti-drive9se-b6bbe5ba6e-cee81207452d1185.elb.ap-southeast-1.amazonaws.com"

# Prod
export DRIVE9_BASE="https://api.drive9.ai"
```

Use the dev value unless the environment owner announces a new endpoint.

For `tidb_cloud_native` endpoints that do not provide server-side default TiDB
Cloud credentials, set `DRIVE9_TIDBCLOUD_PUBLIC_KEY` and
`DRIVE9_TIDBCLOUD_PRIVATE_KEY`. The common e2e provisioning helper sends them in
`/v1/provision` for the standard smoke scripts while preserving empty-body
provisioning when they are unset. Set `DRIVE9_TIDBCLOUD_SPENDING_LIMIT` only
when the run must override the create-time TiDB Cloud spending limit.

#### Run smoke scripts

```bash
# Full smoke (provision -> status poll -> nested dirs -> file ops)
bash e2e/api-smoke-test.sh

# Existing key regression
DRIVE9_API_KEY=drive9_xxx bash e2e/api-smoke-test-existing-key.sh

# CLI smoke (provision + drive9 fs workflows + large file cp)
bash e2e/cli-smoke-test.sh

# Portable profile pack/unpack over a deterministic local Git/npm fixture
bash e2e/portable-pack-unpack-e2e.sh

# Journal smoke (provision + journal create/append/find/verify)
bash e2e/journal-smoke-test.sh

# Layer filesystem smoke (API/CLI entries + optional FUSE restore/commit)
bash e2e/layer-fs-smoke-test.sh

# FUSE smoke (mount + bidirectional filesystem checks)
bash e2e/fuse-smoke-test.sh

# Manifest-based FUSE read correctness workload
bash e2e/fuse-correctness-workload.sh

# Bounded FUSE concurrency stress workload
bash e2e/fuse-concurrency-stress.sh

# Opt-in FUSE performance baseline metrics workload
bash e2e/fuse-performance-baseline.sh

# Strict FUSE release gate plus all optional FUSE workloads
RUN_FUSE_ALL_WORKLOADS=1 bash e2e/fuse-release-gate.sh

# Git workspace smoke (fast-blobless clone + common agent Git workloads)
bash e2e/git-workspace-smoke-test.sh

# Lightweight Git operations smoke for PR local e2e
bash e2e/git-ops-smoke-test.sh

# POSIX permission smoke (API/CLI/FUSE chmod and mkdir mode)
bash e2e/posix-permission-smoke-test.sh

# Run the default smoke-all sequence once
bash e2e/smoke-all.sh

# Scheduled hosted smoke runs should opt into registry-based cleanup. Cleanup
# only deletes tenants/forks registered by this local drive9-e2e run; it does not
# scan server tenants.
DRIVE9_E2E_TMPDIR="$HOME/.cache/drive9-smoke/tmp" \
DRIVE9_E2E_CLEANUP=always \
bash e2e/smoke-all.sh

# Or enable optional Git coverage in that same single smoke-all run.
# Set either variable to 1 as needed; setting both includes both Git suites.
RUN_GIT_OPS_SMOKE=1 RUN_GIT_WORKSPACE_SMOKE=1 bash e2e/smoke-all.sh

# Include portable profile pack/unpack coverage in smoke-all when desired.
RUN_PORTABLE_PACK_E2E=1 bash e2e/smoke-all.sh

# TiDB Cloud Native (tidbcloud-native) tenant lifecycle smoke
# Requires credentials, not wired into CI. Set DRIVE9_BASE from Deployment
# endpoints above, or export manually. Credentials are stored in repo secrets
# (DRIVE9_TIDBCLOUD_PUBLIC_KEY, DRIVE9_TIDBCLOUD_PRIVATE_KEY).
DRIVE9_TIDBCLOUD_PUBLIC_KEY="$DRIVE9_TIDBCLOUD_PUBLIC_KEY" \
DRIVE9_TIDBCLOUD_PRIVATE_KEY="$DRIVE9_TIDBCLOUD_PRIVATE_KEY" \
bash e2e/native-smoke-test.sh
```

### Scheduled smoke-all failure handling

When a scheduled `smoke-all.sh` run finishes with any failed suite or assertion,
do the failure analysis before creating or changing repo issues:

1. Identify the failed suite, failed case/assertion, smoke endpoint, command,
   exit status, cleanup result, and full log path.
2. Analyze the failed case enough to state a likely root cause or the concrete
   blocker that prevents a root-cause call. Use the repo, smoke logs, and
   relevant code paths; do not report only the aggregate `PASS=N FAIL=M` line.
3. Report the findings first in the Slock/Raft smoke-test thread that requested
   or owns the run. Include the failed case, evidence, log path, impact, and
   root-cause assessment.
4. Include an investigation-context block with non-secret identifiers needed for
   external/cloud-side debugging when they are available: run id, branch/commit,
   endpoint/region, parent/fork/admin tenant ids, Drive9 tenant status/kind,
   TiDB Cloud cluster id/state, branch id/state/display name/user prefix/public
   endpoint, relevant create/update/failure timestamps, failed checks, cleanup
   registry path, pending-cleanup state, and retained-resource decision. Never
   include API keys, private keys, DB passwords, bearer tokens, full DSNs, or
   credential files in chat or issues.
5. If the failure is a code problem, search existing GitHub issues before filing
   a new one. Create or update the matching issue with: background, reproduction
   steps, evidence logs, impact, root-cause analysis or suspicious code path,
   suggested fix direction, and validation plan.
6. If the failure is due to environment, credentials, runner setup, endpoint
   availability, or another non-code operational condition, report that in the
   Slock/Raft thread instead of filing a code bug, unless durable repo tracking
   is needed because the condition is recurring or needs engineering follow-up.

### Local via `drive9-server-local`

When the task is specifically about local validation on this machine, prefer
`drive9-server-local` over hosted endpoints.

`scripts/drive9-server-local-env.sh` is the source of truth for local default
environment values.

For a disposable local e2e run that does not depend on TiDB auto-embedding,
Ollama, or a hosted dev deployment:

```bash
make e2e-local
```

This starts a temporary MySQL container, initializes
`DRIVE9_LOCAL_EMBEDDING_MODE=none`, starts `drive9-server-local`, and runs
`e2e/smoke-all.sh` with semantic checks disabled and `RUN_FUSE_SMOKE=0` by
default. `smoke-all.sh` derives `RUN_LAYER_FUSE_SMOKE` from `RUN_FUSE_SMOKE`.
Set `DRIVE9_LOCAL_DSN` to reuse an existing local database instead of starting a
container. Set `RUN_FUSE_SMOKE=1` only when the machine has native FUSE support;
macOS WebDAV fallback does not satisfy the symlink/hardlink FUSE smoke
assertions.

### Prerequisites

- Choose one of the following local validation setups before startup:
- Use TiDB Starter with auto-embedding enabled. Set `DRIVE9_LOCAL_DSN` to the
  Starter instance DSN. This is the easier path for semantic smoke coverage
  because it does not require a local Ollama deployment.
- Use a local TiDB/MySQL instance together with a local embedding service.
  Create the database referenced by `DRIVE9_LOCAL_DSN` before startup, then
  make sure the embedding endpoint is available. The default env script expects
  Ollama at `http://127.0.0.1:11434` with model `bge-m3`.
- Use ordinary MySQL with `DRIVE9_LOCAL_EMBEDDING_MODE=none` for non-semantic
  local filesystem/layer/journal/FUSE smoke coverage. Disable semantic checks
  with `RUN_SEMANTIC_CHECKS=0 RUN_CLI_SEMANTIC_CHECKS=0`.

### Terminal 1: start `drive9-server-local`

```bash
export DRIVE9_REPO_ROOT=/path/to/drive9
cd "$DRIVE9_REPO_ROOT"

export DRIVE9_LOCAL_DSN='root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true'   # optional if you use the default local DSN; replace with your TiDB Starter DSN when applicable
export DRIVE9_LOCAL_INIT_SCHEMA=true   # only for a fresh/disposable database
make run-server-local
```

`make run-server-local` already sources `scripts/drive9-server-local-env.sh` and
stays attached to the foreground. Export any overrides before invoking it, then
run the smoke scripts from a second terminal after the server is healthy.

### Terminal 2: verify health and run E2E

```bash
export DRIVE9_REPO_ROOT=/path/to/drive9
cd "$DRIVE9_REPO_ROOT"

export DRIVE9_BASE=http://127.0.0.1:9009

curl "$DRIVE9_BASE/healthz"

bash e2e/api-smoke-test.sh
DRIVE9_API_KEY='local-dev-key' bash e2e/api-smoke-test-existing-key.sh
bash e2e/cli-smoke-test.sh
bash e2e/fuse-smoke-test.sh
bash e2e/git-ops-smoke-test.sh
bash e2e/git-workspace-smoke-test.sh
RUN_FUSE_ALL_WORKLOADS=1 bash e2e/fuse-release-gate.sh
bash e2e/smoke-all.sh
```

Use `http://127.0.0.1:9009` as `DRIVE9_BASE` once `healthz` returns
`{"status":"ok"}`.

If you overrode `DRIVE9_LOCAL_API_KEY` before starting `drive9-server-local`,
use the same value as `DRIVE9_API_KEY` here.

### Local-server-specific expectations

- `drive9-server-local` exposes a built-in single tenant key via
  `DRIVE9_LOCAL_API_KEY`; the default is `local-dev-key` when the env var is
  not overridden.
- `api-smoke-test-existing-key.sh` should use that built-in key instead of
  provisioning a new tenant.
- Upload-limit boundary failures (`507` on the `limit-1g.bin` initiate step)
  can be caused by stale multipart reservations in the tenant `uploads` table,
  not by current file-tree contents.
- If quota looks polluted, inspect and clear `INITIATED` / `UPLOADING` rows for
  the tenant before rerunning the smoke suite.

## Coverage

### `api-smoke-test.sh`

1. `POST /v1/provision` returns `202` with `tenant_id`, `api_key`, and `status`
2. `GET /v1/status` polled until `active`
3. `GET /v1/fs/?list` returns `entries[]`
4. Nested `mkdir` (`/team/...`) across multi-level paths
5. Multi-file `PUT` + `GET` content verification
6. Batch small-file writes (`N` files) + list count + sample reads
7. Search checks (`GET ?grep=...`, `GET ?find=...`)
8. Image upload (`.jpg`) + image query check (`GET ?find=&name=*.jpg`)
9. Semantic text recall checks (`GET ?grep=feline%20sofa`, `GET ?grep=canine%20field`) with async polling
10. Image-associated recall check (`GET ?grep=feline%20face%20icon`) with async polling + image discoverability
11. SQL sanity check (`POST /v1/sql`)
12. `copy`, `hardlink`, `rename`, `delete`
13. Final `list` verifies expected structure after mutations
14. Large multipart upload (`POST /v1/uploads/initiate` + presigned part uploads + complete + download checksum)

15. Upload-limit boundary (`10GiB` initiate accepted, `10GiB+1` rejected)

### `api-smoke-test-existing-key.sh`

1. Existing API key auth on `GET /v1/status`
2. Optional poll from `provisioning` to `active`
3. `GET /v1/fs/?list` baseline read check

### `cli-smoke-test.sh`

1. Provision + readiness polling
2. Prepare `drive9` CLI binary (build local or download official release)
3. CLI fork flow (`ctx add`, `ctx fork`, fork readiness polling, fork-context file read/write, fork delete)
4. CLI small-file flow (`cp`, `ls`, `cat`, `mv`, `symlink`, `hardlink`, `rm`)
5. CLI `cp` directory-target semantics (local->remote dir, remote->local dir, remote->remote dir all preserve source basename)
6. CLI pack/unpack flow (coding-agent local overlay `.git` + `dist` archived to the default hidden pack slot and restored into a fresh local root)
7. CLI batch small-file flow (`cp` many files + dir list count + stat + sample reads)
8. CLI search flow (`fs grep`, `fs find`)
9. CLI semantic and image-associated recall flow (`fs grep` paraphrase + image caption recall) with async polling
10. CLI image flow (`fs cp` jpg + `fs find -name "*.jpg"`)
11. CLI large-file flow (`cp` upload multipart + `cp` download + checksum verification)
12. CLI upload-limit boundary (`10GiB` initiate accepted, `10GiB+1` rejected)

### `portable-pack-unpack-e2e.sh`

This script is intentionally separate from the broad CLI smoke so it can cover
portable profile semantics without making the default suite slower. It does not
depend on GitHub or the npm registry.

1. Provision tenant unless `DRIVE9_API_KEY` is already set
2. Prepare `drive9` CLI binary (build local or download official release)
3. Build a deterministic local fixture under `local-root/overlay/workspace/app`
4. Run offline `npm install` from a local `file:` dependency to create
   `node_modules`
5. Initialize `.git`, commit the fixture, switch to a feature branch, then
   create staged, unstaged, deleted, and untracked Git status changes
6. Capture a normalized overlay manifest and Git branch/HEAD/status
7. `drive9 pack --profile portable` to the default hidden pack archive
8. `drive9 unpack --profile portable` into a fresh local root
9. Verify the restored overlay manifest, `.git`, branch, HEAD, Git status,
   `node_modules`, symlinks, and representative file contents all match
10. Verify non-overlay local-root content, such as `local-root/cache`, is not
    restored

### `journal-smoke-test.sh`

1. Provision tenant unless `DRIVE9_API_KEY` is already set
2. Create journal with repeated-key labels
3. Append an entry and retry with the same `Idempotency-Key`
4. Read entries with `GET /v1/journals/{id}/entries`
5. Search by repeated label filters
6. Validate malformed metadata search returns `400`
7. Validate missing journal entry read returns `404`
8. Verify hash chain and subject projection, and confirm unchecked scopes are omitted

### `fuse-smoke-test.sh`

Host support: Linux and macOS only. Windows is currently limited to non-mount
CLI workflows for FUSE validation; Windows mounts use the built-in WebDAV
redirector with drive letters instead of the FUSE path, so the FUSE smoke
script is not a supported Windows validation path.

1. Provision + readiness polling
2. Prepare `drive9` CLI binary (build local or download official release)
3. Mount compatibility precheck for root `ls /`
4. RW mount lifecycle (`drive9 mount`, `drive9 umount`)
5. File semantics (`create`, `read`, `overwrite`, `append`, `symlink`, `hardlink`, `truncate`, `unlink`)
6. Directory semantics (`mkdir`, nested paths, `readdir`, empty/non-empty `rmdir`)
7. Rename semantics (file + directory rename consistency)
8. Attribute semantics (`size`, `mtime` monotonicity, remote stat parity)
9. Cross-channel consistency (CLI write visible in mount; mount write visible via CLI)
10. Drain semantics (`drive9 mount drain --json` and native `sync -f`, including open-handle flush and post-drain writability)
11. Mounted large file boundary check (8MB write + remote checksum parity) and tier-transition parity (10KiB → 8MiB → 10KiB size/checksum/remount)
12. Read-only mount behavior (`--read-only` blocks writes/deletes, allows reads)
13. Error semantics (missing path reads/deletes and duplicate mkdir failures)
14. Linux prerequisite guardrails (`fusermount`, `/dev/fuse`) with skip behavior when unavailable

Notes:
- The script prechecks root `ls /` reachability before mount behavior checks.
- Optional release-gate knobs add small-repo git clone/status/log checks,
  durable `drive9 umount --timeout` remount visibility checks, and mount-log audit.

### `fuse-correctness-workload.sh`

Host support: Linux and macOS only. This script needs real FUSE support and is
deterministic read-correctness coverage, not a write/concurrency/Git workload.

1. Provision tenant unless `DRIVE9_API_KEY` is already set
2. Prepare `drive9` CLI binary (build local or download official release)
3. Create a remote fixture tree through CLI writes, including empty files,
   text files, binary files, an 8MiB+ file, multi-level directories, filenames
   with spaces, unicode filenames, a symlink, and a hardlink
4. Mount the fixture subtree read-only through real FUSE
5. Verify `find -type f`, `find -type d`, and `find -type l` exactly match
   the fixture manifest
6. Verify `cat` + SHA-256 and `stat` size parity for every manifest file
7. Verify hardlink `nlink` and checksum parity, and symlink `readlink` plus
   target checksum parity
8. Verify `grep` finds expected markers across normal, space-containing,
   unicode, nested, hardlink, and symlink paths, and that no-match grep fails
9. Verify the read-only mount rejects writes
10. Preserve run root, fixture root, and mount log on failure

### `git-ops-smoke-test.sh`

This is the lightweight Git gate for local PR e2e. It creates a small local
bare Git remote with `git_fixture.py`, so it does not require GitHub, dev/prod
deployments, or externally published tenant schema.

For both `coding-agent` and a test-local `portable` overlay profile, it runs
native `git clone`, `drive9 git clone --fast`, and
`drive9 git clone --fast --blobless --hydrate=off`. Each case verifies clean
reads, branch creation, commit, stash, staged/unstaged/untracked state, then
unmounts and remounts the same Drive9 remote root with a fresh local root.

Native clone cases use explicit `.git` pack/unpack for sandbox replacement.
Fast clone cases disable auto-pack and must recover through Git workspace
checkpoint/restore.

### `fuse-sqlite-correctness.sh`

Host support: Linux and macOS only. This script needs real FUSE support and is
deterministic SQLite rollback-journal correctness coverage, not performance or
crash recovery. Set `RUN_FUSE_SQLITE_WAL=1` to add the WAL detector,
`RUN_FUSE_SQLITE_CHURN=1` to add repeated large-DB rewrite churn, and `RUN_FUSE_SQLITE_CONCURRENCY=1`
to add a bounded WAL readers/writer detector.

1. Provision tenant unless `DRIVE9_API_KEY` is already set
2. Prepare `drive9` CLI binary (build local or download official release)
3. Mount a fresh writable namespace through real FUSE
4. Create deterministic SQLite databases in rollback-journal mode, plus optional WAL/churn/concurrency cases
5. Verify `PRAGMA integrity_check` and logical fingerprints while mounted
6. Unmount, remount, and verify the same logical fingerprints
7. Copy the remote tree back through the CLI and verify snapshot integrity
8. Preserve run root, mount log, and expected/actual manifests on failure

### `fuse-concurrency-stress.sh`

Host support: Linux and macOS only. This script needs real FUSE support and is
deterministic writable concurrency coverage, not a Git or cross-mount workload.

1. Provision tenant unless `DRIVE9_API_KEY` is already set
2. Prepare `drive9` CLI binary (build local or download official release)
3. Mount a fresh writable namespace through real FUSE
4. Run parallel writer threads that create files via temp-write/fsync/atomic
   rename, append per-worker logs, churn create/unlink temp files, rename
   directories into final locations, and verify open-handle reads across rename
5. Run concurrent reader threads that continuously walk/read the mounted tree
   and reject mixed, short, or corrupted reads of atomically published files
6. Verify the final mounted tree exactly matches a deterministic manifest
7. Unmount, copy the remote tree back through the CLI, and verify the remote
   snapshot matches the same manifest
8. Preserve run root, mount log, expected/actual manifests, and reader error log
   on failure

### `fuse-performance-baseline.sh`

Host support: Linux and macOS only. This script needs real FUSE support and is
threshold-free performance baseline coverage, not a pass/fail throughput gate.
It asserts workload correctness and emits JSON metrics artifacts for comparison.

1. Provision tenant unless `DRIVE9_API_KEY` is already set
2. Prepare `drive9` CLI binary (build local or download official release)
3. Mount a fresh writable namespace through real FUSE
4. Write and read deterministic small files with checksum verification
5. Write one deterministic large file and read it multiple times with checksum verification
6. Create a SQLite rollback-journal database, run insert/update/read transactions,
   recompute payload checksums from read row bytes, and verify `PRAGMA integrity_check`
7. Emit `performance-metrics.json` with seconds, bytes, MiB/s, file rates,
   row rates, and correctness fingerprints
8. Preserve run root, mount log, and metrics artifact on failure or when
   `FUSE_PERF_KEEP_ARTIFACTS=1`

### `scripts/compare-fuse-performance-metrics.sh`

This is a performance regression reporter for `local-e2e.yml`.
Run it after `RUN_FUSE_PERFORMANCE_BASELINE=1` produces artifacts and before
the current run is archived. It fetches the previous Drive9 archive manifest
from `/benchmarks/fuse-performance/branches/<branch>/latest.json`, falls back to
`/benchmarks/fuse-performance/latest.json`, downloads the archived
`performance-metrics-*.json`, and writes `performance-compare-*.json` plus
`performance-compare-*.md` into the same artifact directory.

With the default `FUSE_PERF_COMPARE_FAIL_ON_REGRESSION=1`, metric regressions
below `1 - FUSE_PERF_COMPARE_WARN_RATIO` fail the compare step after the reports
are written. Missing historical baselines, parameter mismatches, and legacy
baselines missing newly added workloads remain non-failing warnings.
The script must fail closed for invalid current metrics, missing Drive9
credentials when comparison is enabled, malformed archived manifests, malformed
baseline metrics, or multiple current metrics files.

### `git-workspace-smoke-test.sh`

Host support: Linux and macOS only. This script needs real FUSE support and
uses a `--profile=coding-agent` mount with `--local-root`, so it is intended for
developer machines or EC2-style validation rather than the default smoke path.

1. Provision tenant unless `DRIVE9_API_KEY` is already set
2. Prepare `drive9` CLI binary (build local or download official release)
3. For each configured repo, run `drive9 git clone --fast --blobless --hydrate=sync`
   inside a coding-agent FUSE mount
4. Validate repository readiness (`.git`, `rev-parse`, `log`, `status`)
5. Agent edit/add/commit scenario: append to tracked files, write generated
   files, verify ignored local-only path handling, `git status`, `git diff`,
   `git add -A`, `git commit --no-verify`, and clean status
6. Patch scenario: generate a tracked-file patch, restore, `git apply`, then
   `git add`/`commit`
7. Sandbox restore scenario: stage tracked and generated edits, unmount, remount
   with a fresh local root, and verify `.git` plus dirty status survive restore
8. Fast worktree scenario: clone a base workspace, run
   `drive9 git worktree add --fast --blobless`, commit in the linked worktree,
   leave another staged edit and unstaged file, unmount, remount with a fresh
   local root, verify `git worktree list`/`status`/`log`, then force-remove the
   intentionally dirty linked workspace with `drive9 git worktree remove --fast --force`
9. Audit mount logs for fatal FUSE/Git workspace patterns such as short reads

### `posix-permission-smoke-test.sh`

1. Provision + readiness polling
2. API permission semantics (`mkdir` default/explicit mode, `chmod` on file/directory, 404 on missing path, `?list` mode/hasMode fields)
3. Prepare `drive9` CLI binary (build local or download official release)
4. CLI `drive9 fs chmod` on file and directory with remote HEAD verification
5. FUSE mount + shell `chmod` on file and directory with remote/local stat parity
6. FUSE `mkdir -m` with remote/local stat parity
7. Platform-aware `stat` for macOS (`stat -f %Lp`) and Linux (`stat -c %a`)
8. Cleanup of remote permission test trees

### On-demand matrix scripts

These are not part of the normal E2E smoke entry points. Run them only when a
task explicitly asks for a broad compatibility matrix or Git feature matrix.
They use the same live endpoint conventions as the smoke scripts and mount real
FUSE. `pjdfstest-suite.sh` uses pjdfstest as the sole POSIX compatibility
baseline; `posix-feature-matrix.sh` is a compatibility alias for that suite.
Setup/provisioning is only test harness plumbing and is not counted as POSIX
feature coverage. `git-feature-matrix.sh` generates a local bare Git remote
fixture for deterministic Git coverage. Reports are written to
`$FEATURE_MATRIX_REPORT_DIR/<report-name>-<timestamp>.md`; by default that is a
flat path such as `e2e/reports/posix-feature-report-<timestamp>.md` or
`e2e/reports/git-feature-report-<timestamp>.md`. If
`FEATURE_MATRIX_REPORT_DIR` is set to a run-specific directory, reports are
nested under that directory instead, and checked-in samples should be read using
the layout captured by that run.

1. Provision tenant unless `DRIVE9_API_KEY` is already set
2. Prepare `drive9` CLI binary (`CLI_SOURCE=build` or `official`)
3. Validate FUSE prerequisites only enough to mount a writable test target for pjdfstest
4. Run pjdfstest for POSIX syscall compatibility; skip this row when pjdfstest/prove/root prerequisites are missing
5. For POSIX reports, summarize total/pass/failed counts from pjdfstest/prove cases and render pjdfstest `.t` files as the checkbox matrix
6. Generate a local Git fixture repo with executable files, symlinks, binary files, ignore rules, branches, tags, and merge/rebase/conflict graphs
7. Validate `drive9 git clone --fast`, blobless hydrate-off, hydrate-sync, and explicit `drive9 git hydrate`
8. Run Git readiness, working-tree, index, diff/patch, commit, branch, merge, conflict, rebase, stash, fetch, pull, push branch, and push tag probes
9. Validate Drive9 Git workspace behavior: tree manifest registration, `.git` checkpoint restore, overlay upsert/whiteout/chmod/symlink/dir entries across remount, committed local state, staged-object restore, and ignored-file non-durability
10. Emit a Markdown checkbox matrix: `- [x]` for passed feature probes; unchecked entries for `UNSUPPORTED`, `SKIP`, and `FAIL` with observed errno or command-output summaries

### `fuse-release-gate.sh`

1. Runs `fuse-smoke-test.sh` with `FUSE_STRICT_PREREQS=1`
2. Enables small-repo git clone/status/log coverage
3. Enables durable `umount --timeout` followed by remount visibility checks
4. Enables mount-log audit and dumps mount logs on failure
5. Runs manifest read correctness workload
6. Runs SQLite rollback-journal correctness workload by default; set
   `RUN_FUSE_SQLITE_CORRECTNESS=0` to skip it temporarily while diagnosing
   host-specific FUSE failures
7. Runs bounded concurrency stress workload only when
   `RUN_FUSE_CONCURRENCY_STRESS=1`
8. Runs POSIX/fsx workload only when `RUN_FUSE_POSIX_FSX=1`
9. Runs threshold-free FUSE performance baseline metrics only when
   `RUN_FUSE_PERFORMANCE_BASELINE=1`

Set `RUN_FUSE_ALL_WORKLOADS=1` to default the optional concurrency,
POSIX/fsx, and performance workloads to enabled in one release-gate command.
Explicit per-workload env vars still take precedence.

`local-e2e.yml` runs the performance compare before archiving the current
metrics so a run cannot compare against itself. Regressions fail the compare
step by default. It runs concurrency
stress as a separate scheduled/manual step after the release gate and metrics
archive. Scheduled/manual stress failures still fail the workflow when stress is
enabled.

### `smoke-all.sh`

1. Runs `api-smoke-test.sh`
2. Runs `cli-smoke-test.sh`
3. Runs `journal-smoke-test.sh`
4. Runs `fuse-smoke-test.sh`
5. Runs `portable-pack-unpack-e2e.sh` when `RUN_PORTABLE_PACK_E2E=1`
6. Aggregates pass/fail at script level for quick regression checks

### `native-smoke-test.sh`

Manual-only: requires TiDB Cloud API credentials. Not wired into CI.

1. Provision tenant via `drive9 create` with `--tidbcloud-public-key` / `--tidbcloud-private-key`
2. Poll `GET /v1/status` until active
3. Basic CLI fs operations (`mkdir`, `cp`, `cat`, `ls`, `rm`)
4. Batch small file + large file upload/download + checksum verification
5. Fork smoke (`ctx fork`, fork readiness polling, read/write verification, fork delete)
6. `drive9 admin tenant list` — list tenants, verify active tenant appears
7. `drive9 admin tenant get` — get tenant details and quota info
8. `drive9 admin tenant set-quota` — set restrictive file-size quota (storage=102400 Mi, file-size=2 Mi)
9. Verify max-file-size enforcement: 3 MiB file rejected, 1 MiB file accepted
10. `drive9 admin tenant set-quota` — set generous file-count (1000), create 5 files, then lower to 5
11. Verify max-file-count enforcement: 5 files created, excess file rejected at limit=5
12. `drive9 admin tenant set-quota` — set max-storage-size=1 Mi, verify 2 MiB file rejected
13. `drive9 admin tenant set-quota` (reset) — reset all quotas to generous values
14. `drive9 admin tenant create` — create tenant with initial quotas, verify response
15. `drive9 admin tenant get` — verify initial quotas are reflected on the new tenant
16. `drive9 admin tenant delete` — delete the admin-created tenant
17. Delete main tenant via `drive9 delete` and verify removal (401/403/404 on `GET /v1/status`)
18. Trap-based cleanup: attempts to delete both admin and main tenants on script failure unless `SKIP_CLEANUP=1`

## Environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `DRIVE9_BASE` | `http://127.0.0.1:9009` | all scripts |
| `DRIVE9_E2E_TMPDIR` | `${TMPDIR:-/tmp}` | shared e2e temp root; scripts export `TMPDIR` from it and default FUSE roots to it |
| `DRIVE9_IMAGE_FIXTURE_PATH` | `e2e/fixtures/cat03.jpg` | `api-smoke-test.sh`, `cli-smoke-test.sh` |
| `DRIVE9_API_KEY` | - | `api-smoke-test-existing-key.sh` |
| `DRIVE9_API_KEY` | - | `fuse-smoke-test.sh` (optional; skip provision when set) |
| `DRIVE9_API_KEY` | - | `posix-permission-smoke-test.sh` (optional; skip provision when set) |
| `POLL_TIMEOUT_S` | `120` (smoke), `60` (existing-key) | polling scripts |
| `POLL_INTERVAL_S` | `5` | polling scripts |
| `RUN_LARGE_FILE` | `1` | `api-smoke-test.sh` |
| `LARGE_FILE_MB` | `100` | `api-smoke-test.sh` |
| `BATCH_SMALL_FILE_COUNT` | `10` | `api-smoke-test.sh` |
| `REQUEST_MAX_RETRIES` | `8` | `api-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `REQUEST_RETRY_SLEEP_S` | `2` | `api-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `RUN_UPLOAD_LIMIT_BOUNDARY` | `1` | `api-smoke-test.sh` |
| `UPLOAD_LIMIT_BYTES` | `10737418240` | `api-smoke-test.sh` |
| `RUN_SEMANTIC_CHECKS` | `1` | `api-smoke-test.sh` |
| `SEMANTIC_TIMEOUT_S` | `90` | `api-smoke-test.sh` |
| `SEMANTIC_INTERVAL_S` | `3` | `api-smoke-test.sh` |
| `CLI_LARGE_FILE_MB` | `100` | `cli-smoke-test.sh` |
| `CLI_BATCH_SMALL_FILE_COUNT` | `10` | `cli-smoke-test.sh` |
| `CLI_MAX_RETRIES` | `8` | `cli-smoke-test.sh` |
| `CLI_RETRY_SLEEP_S` | `2` | `cli-smoke-test.sh` |
| `RUN_CLI_UPLOAD_LIMIT_BOUNDARY` | `1` | `cli-smoke-test.sh` |
| `CLI_UPLOAD_LIMIT_BYTES` | `10737418240` | `cli-smoke-test.sh` |
| `RUN_CLI_SEMANTIC_CHECKS` | `1` | `cli-smoke-test.sh` |
| `RUN_CLI_FORK_CHECKS` | `1` (auto-skip when `/v1/fork` is unavailable) | `cli-smoke-test.sh` |
| `CLI_SEMANTIC_TIMEOUT_S` | `90` | `cli-smoke-test.sh` |
| `CLI_SEMANTIC_INTERVAL_S` | `3` | `cli-smoke-test.sh` |
| `CLI_SOURCE` | `build` (`build` or `official`) | `cli-smoke-test.sh`, `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `CLI_RELEASE_BASE_URL` | `https://drive9.ai/releases` | `cli-smoke-test.sh`, `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `CLI_RELEASE_VERSION` | *(latest)* | `cli-smoke-test.sh`, `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `MOUNT_READY_TIMEOUT_S` | `20` | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `MOUNT_READY_INTERVAL_S` | `1` | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `FUSE_MOUNT_ROOT` | `$DRIVE9_E2E_TMPDIR` | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `CLI_MAX_RETRIES` | `8` | `fuse-smoke-test.sh` |
| `CLI_RETRY_SLEEP_S` | `2` | `fuse-smoke-test.sh` |
| `FUSE_STRICT_PREREQS` | `0` (`1` in release gate) | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `FUSE_UMOUNT_TIMEOUT` | `60s` | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `FUSE_CORRECTNESS_LARGE_MB` | `9` | `fuse-correctness-workload.sh` |
| `FUSE_CORRECTNESS_KEEP_ARTIFACTS` | `0` | `fuse-correctness-workload.sh` |
| `RUN_FUSE_ALL_WORKLOADS` | `0` | `fuse-release-gate.sh` |
| `RUN_FUSE_SQLITE_CORRECTNESS` | `1` | `fuse-release-gate.sh` |
| `FUSE_SQLITE_ROWS` | `64` | `fuse-sqlite-correctness.sh` |
| `FUSE_SQLITE_CHURN_ROUNDS` | `4` | `fuse-sqlite-correctness.sh` |
| `FUSE_SQLITE_CONCURRENCY_READERS` | `4` | `fuse-sqlite-correctness.sh` |
| `FUSE_SQLITE_CONCURRENCY_WRITES` | `40` | `fuse-sqlite-correctness.sh` |
| `FUSE_SQLITE_WORKLOAD_TIMEOUT_S` | `240` | `fuse-sqlite-correctness.sh` |
| `FUSE_SQLITE_KEEP_ARTIFACTS` | `0` | `fuse-sqlite-correctness.sh` |
| `RUN_FUSE_SQLITE_WAL` | `0` | `fuse-sqlite-correctness.sh` |
| `RUN_FUSE_SQLITE_CHURN` | `0` | `fuse-sqlite-correctness.sh` |
| `RUN_FUSE_SQLITE_CONCURRENCY` | `0` | `fuse-sqlite-correctness.sh` |
| `FUSE_CONCURRENCY_WORKERS` | `4` | `fuse-concurrency-stress.sh` |
| `FUSE_CONCURRENCY_FILES_PER_WORKER` | `8` | `fuse-concurrency-stress.sh` |
| `FUSE_CONCURRENCY_READER_WORKERS` | `2` | `fuse-concurrency-stress.sh` |
| `FUSE_CONCURRENCY_PAYLOAD_KB` | `32` | `fuse-concurrency-stress.sh` |
| `FUSE_CONCURRENCY_TIMEOUT_S` | `120` | `fuse-concurrency-stress.sh` |
| `FUSE_CONCURRENCY_KEEP_ARTIFACTS` | `0` | `fuse-concurrency-stress.sh` |
| `RUN_FUSE_CONCURRENCY_STRESS` | `0` | `fuse-release-gate.sh` |
| `RUN_FUSE_POSIX_FSX` | `0` | `fuse-release-gate.sh` |
| `RUN_FUSE_PERFORMANCE_BASELINE` | `0` | `fuse-release-gate.sh` |
| `ARCHIVE_FUSE_PERFORMANCE_METRICS` | `0` (`1` in the scheduled daily heavy `local-e2e` run) | `local-e2e.yml` |
| `COMPARE_FUSE_PERFORMANCE_METRICS` | `0` (`1` in the scheduled daily heavy `local-e2e` run) | `local-e2e.yml` |
| `FUSE_CONCURRENCY_STRESS_REQUIRED` | `0` (`1` for scheduled `local-e2e` runs or manual runs with `run_fuse_concurrency_stress=1`) | `local-e2e.yml` |
| `FUSE_PERF_SMALL_FILES` | `64` | `fuse-performance-baseline.sh` |
| `FUSE_PERF_SMALL_BYTES` | `1024` | `fuse-performance-baseline.sh` |
| `FUSE_PERF_LARGE_MB` | `16` | `fuse-performance-baseline.sh` |
| `FUSE_PERF_READ_PASSES` | `2` | `fuse-performance-baseline.sh` |
| `FUSE_PERF_SQLITE_ROWS` | `256` | `fuse-performance-baseline.sh` |
| `FUSE_PERF_KEEP_ARTIFACTS` | `0` | `fuse-performance-baseline.sh` |
| `FUSE_PERF_ARTIFACT_DIR` | - | `fuse-performance-baseline.sh`, `local-e2e.yml` |
| `FUSE_PERF_COMPARE_WARN_RATIO` | `0.30` | `scripts/compare-fuse-performance-metrics.sh` |
| `FUSE_PERF_COMPARE_FAIL_ON_REGRESSION` | `1` | `scripts/compare-fuse-performance-metrics.sh`, `local-e2e.yml` |
| `DRIVE9_PERF_ARCHIVE_ROOT` | `/benchmarks/fuse-performance` | `scripts/archive-fuse-performance-metrics.sh` |
| `DRIVE9_PERF_SOURCE_DIR` | `$FUSE_PERF_ARTIFACT_DIR` | `scripts/archive-fuse-performance-metrics.sh`, `scripts/compare-fuse-performance-metrics.sh` |
| `RUN_FUSE_GIT_CLONE` | `0` (`1` in release gate) | `fuse-smoke-test.sh` |
| `FUSE_GIT_CLONE_URL` | `https://github.com/octocat/Hello-World.git` | `fuse-smoke-test.sh` |
| `FUSE_GIT_CLONE_TIMEOUT_S` | `180` | `fuse-smoke-test.sh` |
| `RUN_FUSE_UMOUNT_DURABLE` | `0` (`1` in release gate) | `fuse-smoke-test.sh` |
| `RUN_FUSE_LOG_AUDIT` | `0` (`1` in release gate) | `fuse-smoke-test.sh` |
| `RUN_GIT_WORKSPACE_SMOKE` | `0` | `smoke-all.sh` |
| `RUN_PORTABLE_PACK_E2E` | `0` | `smoke-all.sh`; `portable-pack-unpack-e2e.sh` is required separately by `local-e2e.yml` |
| `GIT_WORKSPACE_REPOS` | `drive9=...,kimi-cli=...,kimi-code=...` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_SCENARIOS` | `agent_edit_add_commit,agent_patch_apply,sandbox_restore,fast_worktree` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_EXISTING_FILES` | `20` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_NEW_FILES` | `20` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_PATCH_FILES` | `20` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_CLONE_TIMEOUT_S` | `600` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_GIT_TIMEOUT_S` | `120` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_HYDRATE` | `sync` | `git-workspace-smoke-test.sh` |
| `FEATURE_MATRIX_REPORT_DIR` | `e2e/reports` | on-demand matrix scripts |
| `FEATURE_MATRIX_STRICT_ALL` | `0` | on-demand matrix scripts |
| `PJDFSTEST_DIR` | - | on-demand `pjdfstest-suite.sh` / `posix-feature-matrix.sh` |
| `PJDFSTEST_TESTS` | - | on-demand `pjdfstest-suite.sh` / `posix-feature-matrix.sh` |
| `PJDFSTEST_BIN` | auto-detected from `PJDFSTEST_DIR` or `PATH` | on-demand `pjdfstest-suite.sh` / `posix-feature-matrix.sh` |
| `PJDFSTEST_TIMEOUT_S` | `900` | on-demand `pjdfstest-suite.sh` / `posix-feature-matrix.sh` |
| `PJDFSTEST_ALLOW_NONROOT` | `0` | on-demand `pjdfstest-suite.sh` / `posix-feature-matrix.sh` |
| `PJDFSTEST_MOUNT_ALLOW_OTHER` | `auto` | on-demand `pjdfstest-suite.sh` / `posix-feature-matrix.sh`; Linux auto-adds `--allow-other`, Darwin does not |
| `GIT_MATRIX_TIMEOUT_S` | `240` | on-demand `git-feature-matrix.sh` |
| `GIT_MATRIX_RUN_OVERSIZED` | `1` | on-demand `git-feature-matrix.sh` |
| `DRIVE9_TIDBCLOUD_PUBLIC_KEY` | *(required)* | `native-smoke-test.sh` |
| `DRIVE9_TIDBCLOUD_PRIVATE_KEY` | *(required)* | `native-smoke-test.sh` |
| `SKIP_CLEANUP` | `0` | `native-smoke-test.sh` |

## Conventions

- Each smoke run provisions a fresh tenant and uses timestamped paths.
- Scripts require `jq`.
- API surface expected by these scripts:
  - `POST /v1/provision`
  - `GET /v1/status`
  - `/v1/fs/*` for file operations

## Anti-patterns

- Do not hardcode long-lived secrets in scripts.
- Do not use these scripts as unit-test substitutes.
- Do not change API paths casually; scripts serve as executable API docs.
