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
code. Manual "run everything" is Local E2E `workflow_dispatch` with
`run_all_e2e=1`.

## Run

Use a hosted deployment by default. For local development on this machine, use
`drive9-server` with `DRIVE9_TENANT_PROVIDER=local` instead.

### Hosted endpoints

#### Deployment endpoints

Current dev deployments:

```bash
# Dev (HTTP only; never use for credential-bearing hosted config smokes)
export DRIVE9_BASE="http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com"

# Dev, tidbcloud-native (HTTP only; same restriction)
export DRIVE9_BASE="http://k8s-drive9ti-drive9se-b6bbe5ba6e-cee81207452d1185.elb.ap-southeast-1.amazonaws.com"

# Prod
export DRIVE9_BASE="https://api.drive9.ai"
```

Use the dev values only for scripts that do not send control-plane or provider
credentials. Image/video extract-config and embedding-config smokes require an
explicit HTTPS endpoint.

#### Run smoke scripts

```bash
# Full smoke (provision -> status poll -> nested dirs -> file ops)
bash e2e/api-smoke-test.sh

# Reuse an existing tenant (skip provision)
DRIVE9_API_KEY=drive9_xxx bash e2e/api-smoke-test.sh

# CLI smoke (provision + drive9 fs workflows + large file cp)
bash e2e/cli-smoke-test.sh

# Portable profile pack/unpack over a deterministic local Git/npm fixture
bash e2e/pack-smoke-test.sh

# Journal smoke (provision + journal create/append/find/verify)
bash e2e/journal-smoke-test.sh

# Layer filesystem smoke (API/CLI entries + optional FUSE restore/commit)
bash e2e/layer-fs-smoke-test.sh

# FUSE smoke (mount + bidirectional filesystem checks)
bash e2e/fuse-smoke-test.sh

# FUSE supervision PR gate (worker kill-9 heal, umount no-restart, ensure, foreground)
bash e2e/fuse-supervision-test.sh
# Optional toggles: RUN_ENSURE_SMOKE=0 RUN_FOREGROUND_SMOKE=0

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

# On-demand git-workspace discovery (DORMANT refresh=0, live --fast arm, index remount, AC5 cleanup)
# Knobs: GIT_ONDEMAND_ARMED_IDLE_S, GIT_ONDEMAND_REFRESH_MAX_SINGLE/DUAL,
#        GIT_ONDEMAND_DORMANT_ACTIVITY_LOOPS, GIT_ONDEMAND_KEEP_ARTIFACTS
DRIVE9_BASE=http://127.0.0.1:9009 \
DRIVE9_API_KEY=$(curl -sS -X POST http://127.0.0.1:9009/v1/provision | jq -r .api_key) \
  bash e2e/git-workspace-ondemand-smoke-test.sh

# POSIX permission smoke (API/CLI/FUSE chmod and mkdir mode)
bash e2e/posix-permission-smoke-test.sh

# Run the default smoke-all sequence once (local-e2e.yml PR set)
bash e2e/smoke-all.sh

# Skip FUSE-related suites (macOS / no real FUSE)
RUN_FUSE_SMOKE=0 bash e2e/smoke-all.sh

# Post-merge extras on top of the PR set
RUN_JOURNAL_SMOKE=1 RUN_POSIX_SMOKE=1 RUN_GIT_WORKSPACE_SMOKE=1 bash e2e/smoke-all.sh

# Opt-in HTTP extras (off by default, including post-merge)
RUN_TOKENS_SMOKE=1 bash e2e/tokens-smoke-test.sh
RUN_SSE_SMOKE=1 bash e2e/sse-retention-smoke-test.sh

# Object-store CLI + mount against a local MinIO (docker/podman).
# Does not need drive9-server. OBJECT_STRICT_MOUNT=1 fails if FUSE is missing.
# This suite injects --auth=local; it does not exercise server mint.
bash e2e/object-store-smoke-test.sh
OBJECT_STRICT_MOUNT=1 bash e2e/object-store-smoke-test.sh

# Manual --auth=server mint against hosted tidbcloud-native + a real bucket.
# Not wired into CI/local-e2e. Skips if required env is unset.
# DRIVE9_E2E_OBJECT_BUCKET / ACCESS_KEY_ID / SECRET_ACCESS_KEY plus
# DRIVE9_TIDBCLOUD_PUBLIC_KEY / PRIVATE_KEY. Optional: SCHEME, REGION,
# ENDPOINT, STS_ENDPOINT, ACCOUNT_ID, PREFIX, ROLE_ARN.
bash e2e/object-auth-smoke-test.sh

# Hosted first-wave isolation / mount / STS-refresh coverage (manual-only).
# Same admin keys + bucket as object-auth-smoke-test.sh. COS needs REGION +
# ACCOUNT_ID (APPID). TOS needs ROLE_ARN + REGION. Refresh uses
# DRIVE9_OBJECT_SESSION_REFRESH_{MIN,MAX}_LEAD so remint is seconds, not ~45m.
bash e2e/object-auth-s3-hosted-test.sh
bash e2e/object-auth-cos-hosted-test.sh
bash e2e/object-auth-tos-hosted-test.sh

# Optional extra object step inside cli-smoke-test.sh against a real bucket.
DRIVE9_E2E_S3_URI='s3://bucket/prefix/?region=us-east-1' bash e2e/cli-smoke-test.sh

# TiDB Cloud Native (tidbcloud-native) tenant lifecycle smoke
# Requires credentials, not wired into CI. Set DRIVE9_BASE from Deployment
# endpoints above, or export manually. Credentials are stored in repo secrets
# (DRIVE9_TIDBCLOUD_PUBLIC_KEY, DRIVE9_TIDBCLOUD_PRIVATE_KEY).
DRIVE9_TIDBCLOUD_PUBLIC_KEY="$DRIVE9_TIDBCLOUD_PUBLIC_KEY" \
DRIVE9_TIDBCLOUD_PRIVATE_KEY="$DRIVE9_TIDBCLOUD_PRIVATE_KEY" \
bash e2e/native-smoke-test.sh

# Tenant image-extract config smoke. Manual-only: hosted control-plane plus a
# billable OpenAI-compatible vision provider. Skips if any required env is unset.
export DRIVE9_E2E_IMAGE_EXTRACT_API_BASE="https://..."
export DRIVE9_E2E_IMAGE_EXTRACT_API_KEY="..."
export DRIVE9_E2E_IMAGE_EXTRACT_MODEL="..."
bash e2e/image-extract-config-smoke-test.sh

# Tenant video-extract config smoke. Manual-only and skip-if-env-missing.
export DRIVE9_BASE="https://..."
export DRIVE9_TIDBCLOUD_PUBLIC_KEY="..."
export DRIVE9_TIDBCLOUD_PRIVATE_KEY="..."
export DRIVE9_E2E_VIDEO_EXTRACT_API_BASE="https://..."
export DRIVE9_E2E_VIDEO_EXTRACT_API_KEY="..."
export DRIVE9_E2E_VIDEO_EXTRACT_MODEL="..."
export DRIVE9_E2E_VIDEO_FIXTURE_PATH="/path/to/fixture.mp4"
# The MP4 must visibly contain this marker; the script does not put it in the prompt.
export DRIVE9_E2E_VIDEO_EXPECTED_MARKER="..."
bash e2e/video-extract-config-smoke-test.sh

# Tenant embedding config/processing smoke. Model output must be 1024 dimensions.
export DRIVE9_BASE="https://..."
export DRIVE9_TIDBCLOUD_PUBLIC_KEY="..."
export DRIVE9_TIDBCLOUD_PRIVATE_KEY="..."
export DRIVE9_E2E_EMBED_API_BASE="https://..."
export DRIVE9_E2E_EMBED_API_KEY="..."
export DRIVE9_E2E_EMBED_MODEL="..."
bash e2e/embedding-config-smoke-test.sh
```

#### Existing-tenant regression

After a dev release, run `api-smoke-test.sh` and `cli-smoke-test.sh` twice:
once against an existing tenant (set `DRIVE9_API_KEY`) and once against a fresh
provision (unset `DRIVE9_API_KEY`). In existing-tenant mode both suites skip
provision and reuse the tenant; `api-smoke-test.sh` also cleans up its
timestamped test tree at the end, and the upload-limit boundary checks default
to `0` because reserving `total_size` against an existing tenant's quota can
spuriously fail with 507.

```bash
# Export credentials from the current drive9 ctx tenant
# (--reveal prints the full key; keep it out of logs/screenshots)
eval "$(drive9 ctx show --json --reveal | jq -r '"export DRIVE9_BASE=\(.server)\nexport DRIVE9_API_KEY=\(.api_key)"')"

# Pass 1 — existing tenant
bash e2e/api-smoke-test.sh
bash e2e/cli-smoke-test.sh

# Or run the whole smoke-all matrix in existing-tenant mode (re-exports
# DRIVE9_API_KEY to every sub-suite). Set RUN_API_ONLY=1 for just the core
# api + cli pair, RUN_FUSE_SMOKE=0 on hosts without real FUSE.
RUN_API_ONLY=1 bash e2e/smoke-all.sh

# Pass 2 — fresh tenant
unset DRIVE9_API_KEY
bash e2e/api-smoke-test.sh
bash e2e/cli-smoke-test.sh
```

Useful knobs for existing-tenant runs:

- `RUN_CLI_FORK_CHECKS=1` — enable the tenant fork flow (off by default).
- `RUN_LARGE_FILE=0` — skip the 100MB API large-file upload.
- `RUN_SEMANTIC_CHECKS=1` / `RUN_CLI_SEMANTIC_CHECKS=1` — enable semantic recall
  checks (off by default; needs an embedding endpoint).
- `RUN_SQL_CHECKS=1` — enable API `POST /v1/sql` checks (off by default).
- `RUN_UPLOAD_LIMIT_BOUNDARY=1` / `RUN_CLI_UPLOAD_LIMIT_BOUNDARY=1` — force the
  upload-limit boundary checks back on in existing-tenant mode.

### Local via `drive9-server` (`DRIVE9_TENANT_PROVIDER=local`)

When the task is specifically about local validation on this machine, prefer
`drive9-server` with `DRIVE9_TENANT_PROVIDER=local` over hosted endpoints.

CI coverage is `.github/workflows/local-e2e.yml` (TiDB playground +
`provider=local`). On a machine the one-shot entry is:

```bash
make e2e-local
# or: bash scripts/e2e-local.sh
```

That starts TiDB if needed, starts `drive9-server` (`provider=local`), and
runs `e2e/smoke-all.sh` (the `local-e2e.yml` PR set, including FUSE). macOS
needs macFUSE; suites pass `--mode=fuse`. Pass a pre-built server with
`DRIVE9_SERVER_BIN`. For a long-running server you drive yourself, use
`make run-server-local` and the scripts below.

### Prerequisites

- Choose one of the following local validation setups before startup:
- Use TiDB Starter with auto-embedding enabled. Set `DRIVE9_LOCAL_DSN` to the
  Starter instance DSN. This is the easier path for semantic smoke coverage
  because it does not require a local Ollama deployment.
- Use a local TiDB/MySQL instance together with a local embedding service.
  Create the database referenced by `DRIVE9_LOCAL_DSN` before startup, then
  make sure the embedding endpoint is available. For Ollama, set
  `DRIVE9_EMBED_API_BASE=http://127.0.0.1:11434 DRIVE9_EMBED_API_KEY=ollama
  DRIVE9_EMBED_MODEL=bge-m3`.
- Local e2e, fuse-patch, SDK integration, and blackbox local mode all use TiDB
  with `DRIVE9_LOCAL_EMBEDDING_MODE=app` (default). Semantic/SQL/tenant-fork
  cases are off by default; set `RUN_SEMANTIC_CHECKS=1 RUN_CLI_SEMANTIC_CHECKS=1
  RUN_SQL_CHECKS=1 RUN_CLI_FORK_CHECKS=1` when you want them.

### Terminal 1: start local `drive9-server`

```bash
export DRIVE9_REPO_ROOT=/path/to/drive9
cd "$DRIVE9_REPO_ROOT"

export DRIVE9_LOCAL_DSN='root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true'   # optional if you use the default local DSN; replace with your TiDB Starter DSN when applicable
make run-server-local
```

`make run-server-local` sets `DRIVE9_TENANT_PROVIDER=local` and stays attached
to the foreground. Export any `DRIVE9_*` overrides before invoking it, then run
the smoke scripts from a second terminal after the server is healthy.

### Terminal 2: verify health and run E2E

```bash
export DRIVE9_REPO_ROOT=/path/to/drive9
cd "$DRIVE9_REPO_ROOT"

export DRIVE9_BASE=http://127.0.0.1:9009

curl "$DRIVE9_BASE/healthz"

bash e2e/api-smoke-test.sh
bash e2e/cli-smoke-test.sh
bash e2e/fuse-smoke-test.sh
bash e2e/git-ops-smoke-test.sh
bash e2e/git-workspace-smoke-test.sh
RUN_FUSE_ALL_WORKLOADS=1 bash e2e/fuse-release-gate.sh
bash e2e/smoke-all.sh
```

Use `http://127.0.0.1:9009` as `DRIVE9_BASE` once `healthz` returns
`{"status":"ok"}`.

Local `drive9-server` (`provider=local`) returns a JWT from `POST /v1/provision`.
Do not assume `local-dev-key` authenticates. Leave `DRIVE9_API_KEY` unset so
each suite provisions a fresh tenant.

### Local-server-specific expectations

- Blackbox `--server-mode local` and the e2e scripts call `POST /v1/provision`
  after healthz and use the returned `api_key`. Do not assume `local-dev-key`
  authenticates.
- Upload-limit boundary failures (`507` on the `limit-1g.bin` initiate step)
  can be caused by stale multipart reservations in the tenant `uploads` table,
  not by current file-tree contents.
- If quota looks polluted, inspect and clear `INITIATED` / `UPLOADING` rows for
  the tenant before rerunning the smoke suite.

## Coverage

### `api-smoke-test.sh`

1. `POST /v1/provision` returns `202` with `tenant_id`, `api_key`, and `status`
   — honors `DRIVE9_API_KEY`: when set, step 1 skips provision (emits SKIP
   checks) and reuses the existing tenant
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
16. Cleanup test tree (existing-tenant mode only): `DELETE /v1/fs/team-${TS}?recursive`
    removes the timestamped test tree so an existing tenant is not polluted

### `cli-smoke-test.sh`

1. Provision + readiness polling — honors `DRIVE9_API_KEY`: when set, step 1
   skips provision and reuses the existing tenant
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

### `pack-smoke-test.sh`

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
bare Git remote with `tools/git_fixture.py`, so it does not require GitHub, dev/prod
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

### `fuse-s3-express-append-log.sh`

Manual-only hosted validation for the S3 Express append-log FUSE contract. It
requires `DRIVE9_BASE`, `DRIVE9_API_KEY`, and explicit
`DRIVE9_E2E_S3_EXPRESS_ENABLED=1`; `/v1/status` must advertise
`storage_capabilities.append_log_v1=true`. The script defaults to 1,000
SQLite WAL transactions with `synchronous=FULL`, verifies integrity and a
logical fingerprint after close/reopen and a fresh remount, and writes mount
perf artifacts. It rejects logical remote bytes above 10x the final WAL size,
bytes that scale with cumulative WAL observations, and a late-commit P95 more
than 4x the initial-commit P95; both ratios are configurable by environment.
Do not add it to local-e2e or smoke-all: provider=local and this macOS
workstation cannot exercise a real S3 Express Directory Bucket.
Set `FUSE_APPEND_LOG_KEEP_ARTIFACTS=1` to retain successful-run artifacts.

`e2e/microvm/issue-875-auto-reset-header-first/` is the trusted dynamic
MicroVM companion case. It uses `synchronous=FULL`,
`wal_autocheckpoint=5`, `journal_size_limit=-1`, and no explicit checkpoint.
After the SQLite WAL generation-reset optimization lands, it must observe a
valid offset-zero 32-byte header, an exact 32-byte conditional PUT, a fresh
local shadow, and a
subsequent first-frame AppendLog with expected size 32. Do not run or publish
this case from local development without explicit external authorization.

`e2e/microvm/issue-875-append-log-latency-no-journal-limit/` is the trusted
2,000-commit checkpoint-read-source companion. It requires two automatic WAL
generation resets and rejects a second-checkpoint WAL read source other than
`shadow-spill`; it also reports bounded ready/degraded shadow outcomes. Do not
run or publish this case from local development without explicit external
authorization.

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

### `tokens-smoke-test.sh`

Opt-in (`RUN_TOKENS_SMOKE=1`). HTTP `/v1/tokens` management: credential
dispatcher, scoped issue/list/activate/deactivate/delete/revoke/refresh,
pseudoroot projected listing and hidden siblings, scoped gate, and
control-plane generate/list when `provider=local` mock IAM is enabled.
Not part of the PR or post-merge default.

### `sse-retention-smoke-test.sh`

Opt-in (`RUN_SSE_SMOKE=1`). `GET /v1/events` initial sync, live
`file_changed` delivery, cursor replay, and a >1000-event backlog drain.
Optional `SSE_SWEEP_TEST=1` (needs a short `DRIVE9_FS_EVENTS_RETENTION`)
checks retention pruning via dedicated-shape `POST /v1/sql`. Not part of
the PR or post-merge default.

### `git-feature-smoke-test.sh`

Broader Git feature smoke on a coding-agent FUSE mount (clone modes, readiness,
ops, merge/rebase/stash, remount restore). PASS/FAIL like other smokes — not a
Markdown matrix. Complements lighter `git-ops-smoke-test.sh` and
`git-workspace-smoke-test.sh`.

```bash
bash e2e/git-feature-smoke-test.sh
```

POSIX pjdfstest lives in blackbox (`community.pjdfstest`), not under `e2e/`.


### `fuse-patch-storage-class.sh`

Host support: Linux and macOS only. This script needs real FUSE support and is
targeted regression coverage for PATCH-vs-storage-class mismatches
(`patch_unsupported_target`), not a general filesystem workload.

1. Requires a reachable TiDB (`DRIVE9_LOCAL_DSN`, default 127.0.0.1:4000);
   starts `drive9-server` (provider=local) with a high
   `DRIVE9_INLINE_THRESHOLD`, seeds a file that lands inline (db9), then
   restarts the server with a low threshold so the mount's cached threshold is
   below the file size while the object stays db9-stored
2. Mounts with `--mode=fuse --durability write-sync` and partially overwrites
   the file at the same size
3. Default `fixed` scenario: stat header advertises `db9`, zero PATCH attempts
   (storage class seeded from the header), committed bytes correct, storage
   healed to `s3`
4. Manual cross-version scenarios: `SCENARIO=repro` replays the pre-fix EINVAL
   loop with main-built binaries; `SCENARIO=fallback` runs the fixed client
   against an old server binary (no storage-type header) and asserts exactly
   one PATCH attempt before the full-upload fallback commits
5. Override binaries with `DRIVE9_SERVER_BIN` / `DRIVE9_CLI_BIN`; default
   binaries are built from the checkout when missing

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

Default set matches the `local-e2e.yml` PR gate:

1. `api-smoke-test.sh`
2. `cli-smoke-test.sh`
3. `layer-fs-smoke-test.sh`
4. `fuse-release-gate.sh`
5. `fuse-patch-storage-class.sh`
6. `git-ops-smoke-test.sh`
7. `git-workspace-ondemand-smoke-test.sh`
8. `pack-smoke-test.sh`
9. `fuse-crash-recovery-test.sh`
10. `fuse-supervision-test.sh`
11. `fuse-write-perf-budget-test.sh`

Re-exports `DRIVE9_API_KEY` when set so every sub-suite that honors it runs in
existing-tenant mode in one shot. Set `RUN_API_ONLY=1` for api + cli only.
Set `RUN_FUSE_SMOKE=0` to skip FUSE-related suites (and layer-fs FUSE restore);
macOS WebDAV fallback cannot satisfy those asserts. Post-merge extras:
`RUN_JOURNAL_SMOKE=1`, `RUN_POSIX_SMOKE=1`, `RUN_GIT_WORKSPACE_SMOKE=1`.
Opt-in HTTP extras (off even on post-merge): `RUN_TOKENS_SMOKE=1`,
`RUN_SSE_SMOKE=1`.

### `image-extract-config-smoke-test.sh`

Manual-only: hosted control-plane credentials plus a billable OpenAI-compatible
vision provider. Not wired into CI. Skips before any HTTP request when a
required variable is missing. `DRIVE9_TENANT_PROVIDER=local` cannot run this
suite: local image extract is env-only and does not validate tenant config.

1. `POST /v1/provision` with control-plane keys; capture `tenant_id` / `api_key`
2. Poll `GET /v1/status` until `active`
3. `GET /v1/admin/tenants/{id}/extract-config/image` — require new tenant source `none` (a process default would make provider usage ambiguous)
4. Invalid provider API key → 400, config unchanged; unreachable API base → 502/504, config unchanged
5. Valid custom config PUT — provider validated, response/GET mask the API key
6. Upload `e2e/fixtures/cat03.jpg`; poll `?stat` until `tags.e2e_marker` appears; `find` by that tag
7. Disable config (`enabled:false` clears provider fields); upload a second image and require empty semantic text/tags for `DISABLED_EXTRACT_WAIT_S`
8. Exit trap disables config, deletes the test tree, then `DELETE /v1/admin/tenants/{id}`

### `video-extract-config-smoke-test.sh`

Manual-only: hosted control-plane credentials, a billable OpenAI-compatible
vision provider, and a caller-provided MP4. Not wired into CI; skips before any
HTTP request when a required variable is missing.

1. Provision a disposable tenant and wait for `active`
2. Require initial config `source=none`, then PUT a custom video config with `protocol:openai`; verify masked provider output
3. Reject a marker present in the prompt; upload the MP4 via multipart and poll `?stat` until model-derived `semantic_text` containing the fixture's expected marker is written
4. Disable the config, upload the MP4 again, and assert no extracted text appears
5. Exit trap disables config, deletes the test tree, and deletes the tenant

### `embedding-config-smoke-test.sh`

Manual-only: hosted control-plane credentials plus a billable OpenAI-compatible
embedding provider returning exactly 1024 dimensions. Not wired into CI; skips
before any HTTP request when a required variable is missing.

1. Provision a disposable tenant and wait for `active`; require `source=none` and reject database-auto mode
2. Reject an invalid key and unreachable API base, verifying config is unchanged
3. PUT a valid custom config and verify masked provider output and generation
4. Upload target/distractor text and poll a vocabulary-disjoint query for the target
5. Disable the config; exit trap deletes the test tree and tenant

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
| `DRIVE9_IMAGE_FIXTURE_PATH` | `e2e/fixtures/cat03.jpg` | `api-smoke-test.sh`, `cli-smoke-test.sh`, `image-extract-config-smoke-test.sh` |
| `DRIVE9_API_KEY` | - | `api-smoke-test.sh` (optional; when set, skip provision and reuse the tenant; cleanup test tree at end) |
| `DRIVE9_API_KEY` | - | `cli-smoke-test.sh` (optional; when set, skip provision and reuse the tenant) |
| `DRIVE9_API_KEY` | - | `fuse-smoke-test.sh` (optional; skip provision when set) |
| `DRIVE9_API_KEY` | - | `posix-permission-smoke-test.sh` (optional; skip provision when set) |
| `POLL_TIMEOUT_S` | `300` (api smoke), `600` (hosted config smokes), `120` (other smoke) | polling scripts |
| `POLL_INTERVAL_S` | `5` | polling scripts |
| `RUN_LARGE_FILE` | `1` | `api-smoke-test.sh` |
| `LARGE_FILE_MB` | `100` | `api-smoke-test.sh` |
| `BATCH_SMALL_FILE_COUNT` | `10` | `api-smoke-test.sh` |
| `REQUEST_MAX_RETRIES` | `8` | `api-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `REQUEST_RETRY_SLEEP_S` | `2` | `api-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `RUN_UPLOAD_LIMIT_BOUNDARY` | `1` (defaults to `0` when `DRIVE9_API_KEY` is set) | `api-smoke-test.sh` |
| `UPLOAD_LIMIT_BYTES` | `10737418240` | `api-smoke-test.sh` |
| `RUN_SEMANTIC_CHECKS` | `0` | `api-smoke-test.sh` |
| `RUN_SQL_CHECKS` | `0` | `api-smoke-test.sh` |
| `SEMANTIC_TIMEOUT_S` | `90` | `api-smoke-test.sh` |
| `SEMANTIC_INTERVAL_S` | `3` | `api-smoke-test.sh` |
| `CLI_LARGE_FILE_MB` | `100` | `cli-smoke-test.sh` |
| `CLI_BATCH_SMALL_FILE_COUNT` | `10` | `cli-smoke-test.sh` |
| `CLI_MAX_RETRIES` | `8` | `cli-smoke-test.sh` |
| `CLI_RETRY_SLEEP_S` | `2` | `cli-smoke-test.sh` |
| `RUN_CLI_UPLOAD_LIMIT_BOUNDARY` | `1` (defaults to `0` when `DRIVE9_API_KEY` is set) | `cli-smoke-test.sh` |
| `CLI_UPLOAD_LIMIT_BYTES` | `10737418240` | `cli-smoke-test.sh` |
| `RUN_CLI_SEMANTIC_CHECKS` | `0` | `cli-smoke-test.sh` |
| `RUN_CLI_FORK_CHECKS` | `0` (also auto-skip when `/v1/fork` is unavailable) | `cli-smoke-test.sh` |
| `CLI_SEMANTIC_TIMEOUT_S` | `90` | `cli-smoke-test.sh` |
| `CLI_SEMANTIC_INTERVAL_S` | `3` | `cli-smoke-test.sh` |
| `CLI_SOURCE` | `build` (`build` or `official`) | `cli-smoke-test.sh`, `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `CLI_RELEASE_BASE_URL` | `https://drive9.ai/releases` | `cli-smoke-test.sh`, `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `CLI_RELEASE_VERSION` | *(latest)* | `cli-smoke-test.sh`, `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `MOUNT_READY_TIMEOUT_S` | `20` | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `MOUNT_READY_INTERVAL_S` | `1` | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
| `FUSE_MOUNT_ROOT` | `/tmp` | `fuse-smoke-test.sh`, `fuse-correctness-workload.sh`, `fuse-sqlite-correctness.sh`, `fuse-concurrency-stress.sh`, `fuse-performance-baseline.sh` |
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
| `RUN_GIT_WORKSPACE_SMOKE` | `0` | `smoke-all.sh` post-merge extra |
| `RUN_JOURNAL_SMOKE` | `0` | `smoke-all.sh` post-merge extra |
| `RUN_POSIX_SMOKE` | `0` | `smoke-all.sh` post-merge extra |
| `RUN_TOKENS_SMOKE` | `0` | `smoke-all.sh` opt-in extra (`e2e/tokens-smoke-test.sh`) |
| `RUN_SSE_SMOKE` | `0` | `smoke-all.sh` opt-in extra (`e2e/sse-retention-smoke-test.sh`) |
| `RUN_FUSE_SMOKE` | `1` | `smoke-all.sh` |
| `RUN_API_ONLY` | `0` | `smoke-all.sh` (run only api + cli, skip the rest) |
| `GIT_WORKSPACE_REPOS` | `drive9=...,kimi-cli=...,kimi-code=...` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_SCENARIOS` | `agent_edit_add_commit,agent_patch_apply,sandbox_restore,fast_worktree` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_EXISTING_FILES` | `20` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_NEW_FILES` | `20` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_PATCH_FILES` | `20` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_CLONE_TIMEOUT_S` | `600` | `git-workspace-smoke-test.sh` |
| `GIT_WORKSPACE_GIT_TIMEOUT_S` | `120` | `git-workspace-smoke-test.sh` |
| `GIT_FEATURE_TIMEOUT_S` | `240` | `git-feature-smoke-test.sh` |
| `GIT_FEATURE_RUN_OVERSIZED` | `1` | `git-feature-smoke-test.sh` |
| `GIT_WORKSPACE_HYDRATE` | `sync` | `git-workspace-smoke-test.sh` |
| `DRIVE9_TIDBCLOUD_PUBLIC_KEY` | *(required)* | `native-smoke-test.sh`, hosted config smoke tests |
| `DRIVE9_TIDBCLOUD_PRIVATE_KEY` | *(required)* | `native-smoke-test.sh`, hosted config smoke tests |
| `DRIVE9_E2E_IMAGE_EXTRACT_API_BASE` | *(required)* | `image-extract-config-smoke-test.sh` |
| `DRIVE9_E2E_IMAGE_EXTRACT_API_KEY` | *(required)* | `image-extract-config-smoke-test.sh` |
| `DRIVE9_E2E_IMAGE_EXTRACT_MODEL` | *(required)* | `image-extract-config-smoke-test.sh` |
| `DRIVE9_E2E_UNREACHABLE_API_BASE` | `https://example.com:81/v1` | `image-extract-config-smoke-test.sh` |
| `IMAGE_EXTRACT_TIMEOUT_S` | `180` | `image-extract-config-smoke-test.sh` |
| `IMAGE_EXTRACT_INTERVAL_S` | `3` | `image-extract-config-smoke-test.sh` |
| `DISABLED_EXTRACT_WAIT_S` | `30` | image/video extract config smoke tests |
| `DRIVE9_E2E_VIDEO_EXTRACT_API_BASE` | *(required)* | `video-extract-config-smoke-test.sh` |
| `DRIVE9_E2E_VIDEO_EXTRACT_API_KEY` | *(required)* | `video-extract-config-smoke-test.sh` |
| `DRIVE9_E2E_VIDEO_EXTRACT_MODEL` | *(required)* | `video-extract-config-smoke-test.sh` |
| `DRIVE9_E2E_VIDEO_FIXTURE_PATH` | *(required MP4)* | `video-extract-config-smoke-test.sh` |
| `DRIVE9_E2E_VIDEO_EXPECTED_MARKER` | *(required visible fixture fact)* | `video-extract-config-smoke-test.sh` |
| `VIDEO_EXTRACT_TIMEOUT_S` | `600` | `video-extract-config-smoke-test.sh` |
| `VIDEO_EXTRACT_INTERVAL_S` | `5` | `video-extract-config-smoke-test.sh` |
| `DRIVE9_E2E_EMBED_API_BASE` | *(required)* | `embedding-config-smoke-test.sh` |
| `DRIVE9_E2E_EMBED_API_KEY` | *(required)* | `embedding-config-smoke-test.sh` |
| `DRIVE9_E2E_EMBED_MODEL` | *(required, 1024 dimensions)* | `embedding-config-smoke-test.sh` |
| `EMBED_TIMEOUT_S` | `180` | `embedding-config-smoke-test.sh` |
| `EMBED_INTERVAL_S` | `3` | `embedding-config-smoke-test.sh` |
| `EMBED_CONFIG_PROPAGATION_WAIT_S` | `2` | `embedding-config-smoke-test.sh` |
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
