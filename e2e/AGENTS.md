---
title: e2e - Live end-to-end scripts
---

## Overview

This directory contains live end-to-end tests for deployed drive9-server instances.
These scripts are integration probes (not unit tests) and call real HTTP endpoints.

## Run

Use a hosted deployment by default. For local development on this machine, use
`drive9-server-local` instead.

### Hosted endpoints

#### Deployment endpoints

Current shared dev deployment:

```bash
# Dev
export DRIVE9_BASE="http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com"

# Prod
export DRIVE9_BASE="https://api.drive9.ai"
```

Use the dev value unless the environment owner announces a new endpoint.

#### Run smoke scripts

```bash
# Full smoke (provision -> status poll -> nested dirs -> file ops)
bash e2e/api-smoke-test.sh

# Existing key regression
DRIVE9_API_KEY=drive9_xxx bash e2e/api-smoke-test-existing-key.sh

# CLI smoke (provision + drive9 fs workflows + large file cp)
bash e2e/cli-smoke-test.sh

# Journal smoke (provision + journal create/append/find/verify)
bash e2e/journal-smoke-test.sh

# FUSE smoke (mount + bidirectional filesystem checks)
bash e2e/fuse-smoke-test.sh

# Manifest-based FUSE read correctness workload
bash e2e/fuse-correctness-workload.sh

# Bounded FUSE concurrency stress workload
bash e2e/fuse-concurrency-stress.sh

# Git workspace smoke (fast-blobless clone + common agent Git workloads)
bash e2e/git-workspace-smoke-test.sh

# POSIX permission smoke (API/CLI/FUSE chmod and mkdir mode)
bash e2e/posix-permission-smoke-test.sh

# Run all smoke scripts in sequence
bash e2e/smoke-all.sh

# Include Git workspace smoke in smoke-all when desired
RUN_GIT_WORKSPACE_SMOKE=1 bash e2e/smoke-all.sh
```

### Local via `drive9-server-local`

When the task is specifically about local validation on this machine, prefer
`drive9-server-local` over hosted endpoints.

`scripts/drive9-server-local-env.sh` is the source of truth for local default
environment values.

### Prerequisites

- Choose one of the following local validation setups before startup:
- Use TiDB Starter with auto-embedding enabled. Set `DRIVE9_LOCAL_DSN` to the
  Starter instance DSN. This is the easier path for semantic smoke coverage
  because it does not require a local Ollama deployment.
- Use a local TiDB/MySQL instance together with a local embedding service.
  Create the database referenced by `DRIVE9_LOCAL_DSN` before startup, then
  make sure the embedding endpoint is available. The default env script expects
  Ollama at `http://127.0.0.1:11434` with model `bge-m3`.

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
bash e2e/git-workspace-smoke-test.sh
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
6. CLI batch small-file flow (`cp` many files + dir list count + stat + sample reads)
7. CLI search flow (`fs grep`, `fs find`)
8. CLI semantic and image-associated recall flow (`fs grep` paraphrase + image caption recall) with async polling
9. CLI image flow (`fs cp` jpg + `fs find -name "*.jpg"`)
10. CLI large-file flow (`cp` upload multipart + `cp` download + checksum verification)
11. CLI upload-limit boundary (`10GiB` initiate accepted, `10GiB+1` rejected)

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
10. Mounted large file boundary check (8MB write + remote checksum parity) and tier-transition parity (10KiB → 8MiB → 10KiB size/checksum/remount)
11. Read-only mount behavior (`--read-only` blocks writes/deletes, allows reads)
12. Error semantics (missing path reads/deletes and duplicate mkdir failures)
13. Linux prerequisite guardrails (`fusermount`, `/dev/fuse`) with skip behavior when unavailable

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

### `fuse-sqlite-correctness.sh`

Host support: Linux and macOS only. This script needs real FUSE support and is
deterministic SQLite rollback-journal correctness coverage, not concurrency,
performance, or crash recovery. Set `RUN_FUSE_SQLITE_WAL=1` to add the WAL detector.

1. Provision tenant unless `DRIVE9_API_KEY` is already set
2. Prepare `drive9` CLI binary (build local or download official release)
3. Mount a fresh writable namespace through real FUSE
4. Create deterministic SQLite databases in rollback-journal mode, plus WAL when `RUN_FUSE_SQLITE_WAL=1`
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
FUSE. `posix-feature-matrix.sh` uses pjdfstest as the sole POSIX compatibility
baseline; setup/provisioning is only test harness plumbing and is not counted
as POSIX feature coverage. `git-feature-matrix.sh` generates a local bare Git
remote fixture for deterministic Git coverage. Reports are written to
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

### `smoke-all.sh`

1. Runs `api-smoke-test.sh`
2. Runs `cli-smoke-test.sh`
3. Runs `journal-smoke-test.sh`
4. Runs `fuse-smoke-test.sh`
5. Aggregates pass/fail at script level for quick regression checks

## Environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `DRIVE9_BASE` | `http://127.0.0.1:9009` | all scripts |
| `DRIVE9_IMAGE_FIXTURE_PATH` | `e2e/fixtures/cat03.jpg` | `api-smoke-test.sh`, `cli-smoke-test.sh` |
| `DRIVE9_API_KEY` | - | `api-smoke-test-existing-key.sh` |
| `DRIVE9_API_KEY` | - | `fuse-smoke-test.sh` (optional; skip provision when set) |
| `DRIVE9_API_KEY` | - | `posix-permission-smoke-test.sh` (optional; skip provision when set) |
| `POLL_TIMEOUT_S` | `120` (smoke), `60` (existing-key) | polling scripts |
| `POLL_INTERVAL_S` | `5` | polling scripts |
| `RUN_LARGE_FILE` | `1` | `api-smoke-test.sh` |
| `LARGE_FILE_MB` | `100` | `api-smoke-test.sh` |
| `BATCH_SMALL_FILE_COUNT` | `10` | `api-smoke-test.sh` |
| `REQUEST_MAX_RETRIES` | `8` | `api-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh` |
| `REQUEST_RETRY_SLEEP_S` | `2` | `api-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh` |
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
| `CLI_SOURCE` | `build` (`build` or `official`) | `cli-smoke-test.sh`, `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh` |
| `CLI_RELEASE_BASE_URL` | `https://drive9.ai/releases` | `cli-smoke-test.sh`, `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh` |
| `CLI_RELEASE_VERSION` | *(latest)* | `cli-smoke-test.sh`, `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh` |
| `MOUNT_READY_TIMEOUT_S` | `20` | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh` |
| `MOUNT_READY_INTERVAL_S` | `1` | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh` |
| `FUSE_MOUNT_ROOT` | `/tmp` | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh` |
| `CLI_MAX_RETRIES` | `8` | `fuse-smoke-test.sh` |
| `CLI_RETRY_SLEEP_S` | `2` | `fuse-smoke-test.sh` |
| `FUSE_STRICT_PREREQS` | `0` (`1` in release gate) | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh` |
| `FUSE_UMOUNT_TIMEOUT` | `60s` | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh` |
| `FUSE_CORRECTNESS_LARGE_MB` | `9` | `fuse-correctness-workload.sh` |
| `FUSE_CORRECTNESS_KEEP_ARTIFACTS` | `0` | `fuse-correctness-workload.sh` |
| `RUN_FUSE_SQLITE_CORRECTNESS` | `1` | `fuse-release-gate.sh` |
| `FUSE_SQLITE_ROWS` | `64` | `fuse-sqlite-correctness.sh` |
| `FUSE_SQLITE_KEEP_ARTIFACTS` | `0` | `fuse-sqlite-correctness.sh` |
| `RUN_FUSE_SQLITE_WAL` | `0` | `fuse-sqlite-correctness.sh` |
| `FUSE_CONCURRENCY_WORKERS` | `4` | `fuse-concurrency-stress.sh` |
| `FUSE_CONCURRENCY_FILES_PER_WORKER` | `8` | `fuse-concurrency-stress.sh` |
| `FUSE_CONCURRENCY_READER_WORKERS` | `2` | `fuse-concurrency-stress.sh` |
| `FUSE_CONCURRENCY_PAYLOAD_KB` | `32` | `fuse-concurrency-stress.sh` |
| `FUSE_CONCURRENCY_TIMEOUT_S` | `120` | `fuse-concurrency-stress.sh` |
| `FUSE_CONCURRENCY_KEEP_ARTIFACTS` | `0` | `fuse-concurrency-stress.sh` |
| `RUN_FUSE_GIT_CLONE` | `0` (`1` in release gate) | `fuse-smoke-test.sh` |
| `FUSE_GIT_CLONE_URL` | `https://github.com/octocat/Hello-World.git` | `fuse-smoke-test.sh` |
| `FUSE_GIT_CLONE_TIMEOUT_S` | `180` | `fuse-smoke-test.sh` |
| `RUN_FUSE_UMOUNT_DURABLE` | `0` (`1` in release gate) | `fuse-smoke-test.sh` |
| `RUN_FUSE_LOG_AUDIT` | `0` (`1` in release gate) | `fuse-smoke-test.sh` |
| `RUN_GIT_WORKSPACE_SMOKE` | `0` | `smoke-all.sh` |
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
| `PJDFSTEST_DIR` | - | on-demand `posix-feature-matrix.sh` |
| `PJDFSTEST_TESTS` | - | on-demand `posix-feature-matrix.sh` |
| `PJDFSTEST_BIN` | auto-detected from `PJDFSTEST_DIR` or `PATH` | on-demand `posix-feature-matrix.sh` |
| `PJDFSTEST_TIMEOUT_S` | `900` | on-demand `posix-feature-matrix.sh` |
| `PJDFSTEST_ALLOW_NONROOT` | `0` | on-demand `posix-feature-matrix.sh` |
| `GIT_MATRIX_TIMEOUT_S` | `240` | on-demand `git-feature-matrix.sh` |
| `GIT_MATRIX_RUN_OVERSIZED` | `1` | on-demand `git-feature-matrix.sh` |

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
