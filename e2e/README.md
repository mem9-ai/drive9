# drive9 E2E tests

Live end-to-end scripts for validating deployed `drive9-server` behavior,
including local single-tenant validation via `drive9-server-local`.

## Prerequisites

- A running server endpoint (`DRIVE9_BASE`)
- `jq` installed
- Bash 4+

For `tidb_cloud_native` endpoints that do not have server-side default TiDB
Cloud credentials, set `DRIVE9_TIDBCLOUD_PUBLIC_KEY` and
`DRIVE9_TIDBCLOUD_PRIVATE_KEY`. The general provisioning smoke scripts then send
those fields in `/v1/provision`; when unset, they keep the existing empty-body
provisioning behavior for anonymous/default-key endpoints. Optionally set
`DRIVE9_TIDBCLOUD_SPENDING_LIMIT` to include `tidbcloud_spending_limit`.

## Scripts

| Script | What it validates |
|--------|--------------------|
| `api-smoke-test.sh` | Fresh provisioning, status polling, nested+batch file ops, hardlink/copy/rename/delete checks, grep/find checks, semantic text recall, image-associated recall, sql checks, large multipart upload+download |
| `api-smoke-test-existing-key.sh` | Existing API key status/list checks |
| `cli-smoke-test.sh` | End-to-end CLI workflow including `fs symlink`, `fs hardlink`, default-slot `pack`/`unpack`, `fs grep`/`fs find`, semantic/image-associated recall checks, image `fs cp`+`fs find`, and large multipart `fs cp` upload/download |
| `layer-fs-smoke-test.sh` | Layer filesystem API+CLI+FUSE workflow: create by name/tag, diff/checkpoint lookup, rollback, commit, scope rejection, conflict detection, mkdir/upsert/whiteout/rename/symlink/chmod entries, and checkpoint/full restore into fresh local roots |
| `layer-fs-smoke-test-realdev.sh` | Manual realdev wrapper for `layer-fs-smoke-test.sh`; defaults to the shared dev endpoint and enables strict FUSE restore coverage without being wired into `local-e2e` CI |
| `portable-pack-unpack-e2e.sh` | Portable profile pack/unpack over a deterministic local repo: offline npm `file:` install creates `node_modules`, Git staged/unstaged/untracked status changes are captured, pack writes the default hidden archive, fresh local-root unpack restores overlay files, symlinks, `.git`, `node_modules`, branch, HEAD, and `git status` |
| `fuse-smoke-test.sh` | FUSE mount lifecycle, file/dir/symlink/hardlink/rename/stat semantics, cross-channel consistency, `mount drain`/native `sync -f` drain checks, mounted 10KiB→8MiB→10KiB tier-transition parity, read-only and error-path checks |
| `fuse-correctness-workload.sh` | Real read-only FUSE workload over a manifest fixture: `find`, `grep`, `stat`, `cat`, `sha256`, symlink, hardlink, unicode/space paths, empty files, binary files, and 8MiB+ files |
| `fuse-sqlite-correctness.sh` | Real writable FUSE SQLite correctness workload with rollback-journal mode, `PRAGMA integrity_check`, unmount/remount parity, and remote snapshot verification; set `RUN_FUSE_SQLITE_WAL=1` for WAL, `RUN_FUSE_SQLITE_CHURN=1` for repeated large-DB rewrite churn, and `RUN_FUSE_SQLITE_CONCURRENCY=1` for the bounded readers/writer detector |
| `fuse-concurrency-stress.sh` | Real writable FUSE concurrency workload with parallel writers/readers, atomic rename, unlink churn, open-handle rename reads, and deterministic final manifest checks |
| `fuse-posix-fsx-gate.sh` | Opt-in JuiceFS-style POSIX/fsx subset over real writable FUSE: deterministic random write/read/truncate, atomic rename replacement, unlink-open reads, directory fsync, final model hash, unmount, and remote snapshot parity |
| `fuse-performance-baseline.sh` | Opt-in real writable FUSE baseline that records small-file, large-file, repeated large-read, and SQLite transaction/read metrics as JSON artifacts without hardcoded throughput thresholds; SQLite reads verify stored row payload bytes against row checksums |
| `fuse-release-gate.sh` | Strict FUSE release/CI gate with hard prereq failures, small-repo git clone/status/log, durable umount/remount, mount-log audit, manifest-based FUSE correctness workload, and SQLite rollback-journal correctness; set `RUN_FUSE_ALL_WORKLOADS=1` to add all optional release-gate workloads, `RUN_FUSE_SQLITE_CORRECTNESS=0` to skip SQLite temporarily, `RUN_FUSE_CONCURRENCY_STRESS=1` to add bounded concurrency stress, `RUN_FUSE_POSIX_FSX=1` to add the POSIX/fsx subset, and `RUN_FUSE_PERFORMANCE_BASELINE=1` to add performance metrics |
| `git-ops-smoke-test.sh` | Lightweight local Git gate using a local bare fixture: native clone, `drive9 git clone --fast`, and `drive9 git clone --fast --blobless` across `coding-agent` and `portable` profiles, followed by edit/add/commit/stash, remount into a fresh local root, and Git state/content verification |
| `fuse-crash-recovery-test.sh` | FUSE crash-recovery gate: fsync'd small files plus a large mid-upload ShadowSpill survive `kill -9` of the mount daemon, recovered commits converge remotely, unlinked files do not resurrect, and the journal WAL compacts after a clean remount |
| `fuse-write-perf-budget-test.sh` | FUSE write-path perf budgets: fsync-heavy workload with deterministic op-count budgets (remote writes/stats/lists/mutations, commit retries/failures) plus an fsync latency ceiling, asserted from mount perf counters |
| `git-workspace-smoke-test.sh` | Git workspace fast-blobless clone with coding-agent local overlay, batched tracked-file edits, ignored local-only paths, `git add`/`commit`, `git apply`, and remount restore |
| `posix-permission-smoke-test.sh` | POSIX permission coverage: API mkdir/chmod mode propagation, CLI `fs chmod`, FUSE `chmod`/`mkdir -m` with remote and local stat parity |
| `native-smoke-test.sh` | TiDB Cloud Native tenant lifecycle: CLI provision with credentials, status poll, basic fs ops (mkdir/cp/cat/ls/rm), delete + verification, trap cleanup on failure |
| `pjdfstest-suite.sh` | On-demand Linux/macOS pjdfstest POSIX compatibility suite over a real Drive9 FUSE mount; `pjdfstests.sh` and `posix-feature-matrix.sh` are aliases |
| `smoke-all.sh` | Runs API + CLI + journal + layer FS + FUSE + POSIX permission smoke scripts in sequence with aggregated pass/fail; set `RUN_FUSE_SMOKE=0` to skip FUSE symlink/hardlink coverage, `RUN_GIT_OPS_SMOKE=1` to include lightweight Git coverage, `RUN_GIT_WORKSPACE_SMOKE=1` to include heavier Git workspace coverage, and `RUN_PORTABLE_PACK_E2E=1` to include portable pack/unpack coverage |
| `local-smoke.sh` | Starts `drive9-server-local` with a disposable local DB by default, then runs `smoke-all.sh` with semantic checks disabled and FUSE smoke skipped unless `RUN_FUSE_SMOKE=1` |

## CI automation tiers

Every script in this directory must be wired into one of these tiers (or be
explicitly listed as manual-only with a reason). Do not merge a new e2e script
without adding it to `.github/workflows/local-e2e.yml`.

| Tier | Trigger | What runs |
|------|---------|-----------|
| PR gate | `pull_request` to `main` (local-e2e) | api, existing-key, cli, layer-fs, fuse-release-gate (smoke + correctness + sqlite rollback), git-ops, portable pack/unpack, fuse-crash-recovery, fuse-write-perf-budget |
| Post-merge | `push` to `main` (local-e2e, coalesced via concurrency group) | PR gate + concurrency stress, POSIX/fsx, sqlite WAL/churn/concurrency, full `smoke-all.sh` (journal, posix-permission, git-workspace), git feature matrix |
| Nightly | cron 20:17 UTC (local-e2e) | Post-merge set + FUSE performance baseline/archive/compare (compare is report-only; hosted-runner noise) |
| Manual all | `e2e-all` workflow (`Run workflow` button) | Everything above + pjdfstest POSIX suite (best-effort) via `run_all_e2e=1` |
| Manual only | not wired, run by hand | `layer-fs-smoke-test-realdev.sh` (shared dev endpoint), `verify-description-e2e.sh` (Docker + Ollama), `verify-description-tidb-zero-e2e.sh` (TiDB Cloud Zero), `native-smoke-test.sh` (TiDB Cloud Native — requires credentials), `local-smoke.sh` (`make e2e-local` wrapper) |

Scheduled and post-merge failures auto-file/append to a `ci-e2e-failure`
GitHub issue, since GitHub only notifies the workflow author otherwise.

## Product-quality report & Feishu notifications

After the suites run, `cmd/e2e-aggregate` turns the per-suite outcomes into one
product-quality report (`internal/e2ereport`). It is driven by
`e2e/suite-manifest.json`, which maps each `local-e2e.yml` step id to its product
area, product promise, owner hint, and default failure class. The aggregator:

- appends a richer summary (grouped by product promise, with failed suites and
  any performance regressions) to the workflow step summary;
- computes a stable **failure signature** so a recurring failure pattern groups
  into one `ci-e2e-failure` issue instead of spawning new ones, and writes a
  structured, signature-led issue body;
- decides whether to notify Feishu/Lark: PR-tier failures stay in GitHub;
  post-merge/nightly/manual failures and explicit performance regressions are
  pushed.

`cmd/feishu-notify` sends the notification card. It auto-detects the transport
from repo secrets and **no-ops (never fails the run) when none is configured**:

- custom-bot webhook — set `FEISHU_WEBHOOK`;
- app (tenant) API — set `FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `FEISHU_CHAT_ID`.

A suite can opt into richer reporting (durations, metrics with budgets/baselines
for soft performance regressions, artifact links) by writing a
`SuiteSummary` JSON to `e2e/reports/summary/<suite>.json`; the aggregator uses it
in place of the synthesized-from-outcome summary. When adding a suite to
`.github/workflows/local-e2e.yml`, also add its entry to `e2e/suite-manifest.json`.

## Run

Use a hosted deployment by default. For local development on this machine, use
`drive9-server-local` instead.

### Hosted endpoints

#### Deployment endpoints

```bash
# Dev
export DRIVE9_BASE="http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com"

# Dev (tidbcloud-native)
export DRIVE9_BASE="http://k8s-drive9ti-drive9se-b6bbe5ba6e-cee81207452d1185.elb.ap-southeast-1.amazonaws.com"

# Prod
export DRIVE9_BASE="https://api.drive9.ai"
```

#### Run smoke scripts

```bash
bash e2e/api-smoke-test.sh

DRIVE9_API_KEY=drive9_xxx bash e2e/api-smoke-test-existing-key.sh

bash e2e/cli-smoke-test.sh

# Layer filesystem API+CLI smoke. Set RUN_LAYER_FUSE_SMOKE=1 to also run
# real FUSE layer checkpoint/full restore coverage.
bash e2e/layer-fs-smoke-test.sh

# Layer filesystem realdev smoke. This targets the shared dev endpoint by
# default and runs the same layer cases with strict FUSE restore coverage.
bash e2e/layer-fs-smoke-test-realdev.sh

# Use official released drive9 CLI instead of local build
CLI_SOURCE=official bash e2e/cli-smoke-test.sh

# Portable profile pack/unpack over a stable local Git/npm fixture.
bash e2e/portable-pack-unpack-e2e.sh

# Use official released drive9 CLI for portable profile pack/unpack.
CLI_SOURCE=official bash e2e/portable-pack-unpack-e2e.sh

bash e2e/fuse-smoke-test.sh

# Manifest-based read correctness workload on a real read-only FUSE mount.
bash e2e/fuse-correctness-workload.sh

# SQLite rollback-journal correctness on a real writable FUSE mount.
bash e2e/fuse-sqlite-correctness.sh

# Bounded concurrency stress on a real writable FUSE mount.
bash e2e/fuse-concurrency-stress.sh

# JuiceFS-style POSIX/fsx subset on a real writable FUSE mount.
bash e2e/fuse-posix-fsx-gate.sh

# Opt-in performance baseline with JSON metrics artifacts.
bash e2e/fuse-performance-baseline.sh

# Fast-blobless Git workspace smoke. This is intentionally opt-in for broad
# smoke runs because it clones real repositories and needs FUSE support.
bash e2e/git-workspace-smoke-test.sh

# Lightweight Git operations smoke. This is the PR-local Git gate and uses a
# local fixture remote instead of GitHub/dev/prod state.
bash e2e/git-ops-smoke-test.sh

# Strict FUSE release gate used by CI
bash e2e/fuse-release-gate.sh

# Strict FUSE release gate plus all optional FUSE workloads.
RUN_FUSE_ALL_WORKLOADS=1 bash e2e/fuse-release-gate.sh

# Add the concurrency stress workload to the strict FUSE release gate.
RUN_FUSE_CONCURRENCY_STRESS=1 bash e2e/fuse-release-gate.sh

# Add the POSIX/fsx subset to the strict FUSE release gate.
RUN_FUSE_POSIX_FSX=1 bash e2e/fuse-release-gate.sh

# Add the threshold-free performance baseline to the strict FUSE release gate.
RUN_FUSE_PERFORMANCE_BASELINE=1 bash e2e/fuse-release-gate.sh

# Preserve performance metrics outside the run root for CI artifact upload.
FUSE_PERF_ARTIFACT_DIR=e2e-artifacts/fuse-performance \
  RUN_FUSE_PERFORMANCE_BASELINE=1 bash e2e/fuse-release-gate.sh

# Compare metrics against the latest Drive9-archived baseline.
DRIVE9_SERVER=https://api.drive9.ai DRIVE9_API_KEY=drive9_xxx \
  FUSE_PERF_ARTIFACT_DIR=e2e-artifacts/fuse-performance \
  bash scripts/compare-fuse-performance-metrics.sh

# Use official released drive9 CLI for FUSE smoke
CLI_SOURCE=official bash e2e/fuse-smoke-test.sh
CLI_SOURCE=official bash e2e/fuse-correctness-workload.sh
CLI_SOURCE=official bash e2e/fuse-sqlite-correctness.sh
CLI_SOURCE=official bash e2e/fuse-concurrency-stress.sh
CLI_SOURCE=official bash e2e/fuse-posix-fsx-gate.sh
CLI_SOURCE=official bash e2e/fuse-performance-baseline.sh
CLI_SOURCE=official bash e2e/fuse-release-gate.sh
CLI_SOURCE=official bash e2e/posix-permission-smoke-test.sh

bash e2e/posix-permission-smoke-test.sh

# TiDB Cloud Native tenant lifecycle smoke (requires credentials, manual-only).
DRIVE9_TIDBCLOUD_PUBLIC_KEY=xxx DRIVE9_TIDBCLOUD_PRIVATE_KEY=xxx bash e2e/native-smoke-test.sh

bash e2e/smoke-all.sh

# Use TiDB Cloud credentials for tidb_cloud_native endpoints without a
# server-side default key.
DRIVE9_TIDBCLOUD_PUBLIC_KEY=xxx DRIVE9_TIDBCLOUD_PRIVATE_KEY=xxx \
  bash e2e/smoke-all.sh

# Include portable profile pack/unpack coverage in smoke-all.
RUN_PORTABLE_PACK_E2E=1 bash e2e/smoke-all.sh
```

#### On-demand pjdfstest POSIX compatibility suite

`pjdfstest-suite.sh` is not part of the normal E2E smoke entry points. Run it
only when you explicitly need a pjdfstest-based POSIX compatibility report.
The older `posix-feature-matrix.sh` entrypoint remains as a compatibility
alias.

```bash
# Linux, full privileged run.
sudo PJDFSTEST_DIR=/path/to/pjdfstest DRIVE9_BASE=http://127.0.0.1:9009 \
  bash e2e/pjdfstest-suite.sh

# macOS/macFUSE local debug run. The script auto-adds the standard macFUSE
# helper path when needed; non-root runs may not cover privileged pjdfstest
# cases with the same semantics as Linux.
PJDFSTEST_ALLOW_NONROOT=1 PJDFSTEST_DIR=/path/to/pjdfstest \
  bash e2e/pjdfstest-suite.sh
```

By default it writes directly under `e2e/reports/`, for example
`e2e/reports/posix-feature-report-<timestamp>.md`, and counts results using
pjdfstest/prove cases. If `FEATURE_MATRIX_REPORT_DIR` points at a run-specific
directory, the same report filename is written inside that directory instead.

- Knobs: `FEATURE_MATRIX_REPORT_DIR`, `FEATURE_MATRIX_STRICT_ALL`,
  `PJDFSTEST_DIR`, `PJDFSTEST_TESTS`, `PJDFSTEST_BIN`,
  `PJDFSTEST_TIMEOUT_S`, `PJDFSTEST_ALLOW_NONROOT`, and
  `PJDFSTEST_MOUNT_ALLOW_OTHER`.
- Build pjdfstest before running so either `$PJDFSTEST_DIR/pjdfstest` exists
  or `pjdfstest` is on `PATH`. The runner adds the pjdfstest binary directory
  to `PATH` while invoking `prove`.
- `PJDFSTEST_MOUNT_ALLOW_OTHER=auto` keeps Linux behavior compatible with the
  existing POSIX matrix by passing `--allow-other`, but avoids that flag on
  macOS where macFUSE often rejects it unless the host is explicitly configured.
- Matrix reports use `- [x]` only for passed pjdfstest `.t` files. Failed or
  skipped entries remain unchecked with observed output summaries.

### Local via `drive9-server-local`

The local smoke flow that is currently exercised on this machine uses
`drive9-server-local`, not the old hosted/local-server path.

`scripts/drive9-server-local-env.sh` is the source of truth for local default
environment values.

For a disposable local run that does not depend on TiDB auto-embedding, Ollama,
or a hosted dev deployment, use:

```bash
make e2e-local
```

`make e2e-local` runs `e2e/local-smoke.sh`, which starts a temporary MySQL
container by default, initializes the local no-embedding tenant schema, starts
`drive9-server-local`, and runs `e2e/smoke-all.sh` with semantic checks and
upload-limit boundary checks disabled. It also skips `fuse-smoke-test.sh` by
default because local macOS runs may fall back to WebDAV, which cannot support
the symlink/hardlink assertions in that script. Override `DRIVE9_LOCAL_DSN` to
reuse an existing database instead of starting a container, and set
`RUN_FUSE_SMOKE=1` when a native FUSE setup is available.

1. Confirm local prerequisites.

- Create the local database referenced by `DRIVE9_LOCAL_DSN` before startup.
- For full smoke coverage, ensure the embedding endpoint is available. The
  default env script expects Ollama at `http://127.0.0.1:11434` with model
  `bge-m3`.
- For MySQL-only local validation without embeddings, set
  `DRIVE9_LOCAL_EMBEDDING_MODE=none` and disable semantic smoke checks:
  `RUN_SEMANTIC_CHECKS=0 RUN_CLI_SEMANTIC_CHECKS=0`.

2. In terminal 1, start `drive9-server-local` from the repository root.

```bash
export DRIVE9_REPO_ROOT=/path/to/drive9
cd "$DRIVE9_REPO_ROOT"

export DRIVE9_LOCAL_DSN='root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true'   # optional if you use the default local DSN; replace with your TiDB Starter DSN when applicable
export DRIVE9_LOCAL_INIT_SCHEMA=true   # only for a fresh/disposable database
make run-server-local
```

`make run-server-local` already sources `scripts/drive9-server-local-env.sh` and
stays attached to the foreground. Export any overrides before invoking it,
leave it running, and use a second terminal for the smoke scripts.

3. In terminal 2, confirm the server is healthy.

```bash
export DRIVE9_REPO_ROOT=/path/to/drive9
cd "$DRIVE9_REPO_ROOT"

export DRIVE9_BASE=http://127.0.0.1:9009

curl "$DRIVE9_BASE/healthz"
```

Expected response:

```json
{"status":"ok"}
```

4. Run the e2e smoke scripts against the local endpoint.

```bash
export DRIVE9_REPO_ROOT=/path/to/drive9
cd "$DRIVE9_REPO_ROOT"

export DRIVE9_BASE=http://127.0.0.1:9009

# Full API smoke on a fresh locally provisioned tenant.
bash e2e/api-smoke-test.sh

# Existing-key regression against the built-in local tenant.
DRIVE9_API_KEY='local-dev-key' bash e2e/api-smoke-test-existing-key.sh

# CLI smoke using the repo build.
bash e2e/cli-smoke-test.sh

# FUSE smoke using the repo build.
bash e2e/fuse-smoke-test.sh

# Deterministic read correctness workload using grep/find/stat/cat/checksum.
bash e2e/fuse-correctness-workload.sh

# Deterministic concurrency workload using parallel reads/writes/rename/unlink.
bash e2e/fuse-concurrency-stress.sh

# Deterministic fsx-style workload using random writes/truncates/renames.
bash e2e/fuse-posix-fsx-gate.sh

# Opt-in small-file, large-file, and SQLite performance baseline.
bash e2e/fuse-performance-baseline.sh

# Strict FUSE release gate using the repo build.
bash e2e/fuse-release-gate.sh

# Strict FUSE release gate plus correctness, SQLite, concurrency, POSIX/fsx,
# and performance workloads.
RUN_FUSE_ALL_WORKLOADS=1 bash e2e/fuse-release-gate.sh

# Strict FUSE release gate plus bounded concurrency stress.
RUN_FUSE_CONCURRENCY_STRESS=1 bash e2e/fuse-release-gate.sh

# Strict FUSE release gate plus POSIX/fsx subset.
RUN_FUSE_POSIX_FSX=1 bash e2e/fuse-release-gate.sh

# Strict FUSE release gate plus threshold-free performance metrics.
RUN_FUSE_PERFORMANCE_BASELINE=1 bash e2e/fuse-release-gate.sh

# POSIX permission smoke (API + CLI + FUSE).
bash e2e/posix-permission-smoke-test.sh

# Run the default smoke-all sequence once.
bash e2e/smoke-all.sh

# Or enable optional Git coverage in that same single smoke-all run.
# Set either variable to 1 as needed; setting both includes both Git suites.
RUN_GIT_OPS_SMOKE=1 RUN_GIT_WORKSPACE_SMOKE=1 bash e2e/smoke-all.sh
```

If you overrode `DRIVE9_LOCAL_API_KEY` before starting `drive9-server-local`,
use the same value as `DRIVE9_API_KEY` here.

5. Optional: use an already-built or official CLI instead of rebuilding.

```bash
CLI_SOURCE=official bash e2e/cli-smoke-test.sh
CLI_SOURCE=official bash e2e/fuse-smoke-test.sh
CLI_SOURCE=official bash e2e/fuse-correctness-workload.sh
CLI_SOURCE=official bash e2e/fuse-sqlite-correctness.sh
CLI_SOURCE=official bash e2e/fuse-concurrency-stress.sh
CLI_SOURCE=official bash e2e/fuse-performance-baseline.sh
CLI_SOURCE=official bash e2e/fuse-release-gate.sh
CLI_SOURCE=official bash e2e/git-ops-smoke-test.sh
CLI_SOURCE=official bash e2e/git-workspace-smoke-test.sh
```

#### `drive9-server-local` notes

- `drive9-server-local` serves a single local tenant with API key
  `${DRIVE9_LOCAL_API_KEY:-local-dev-key}` by default.
- `api-smoke-test.sh`, `cli-smoke-test.sh`, and `fuse-smoke-test.sh` still
  provision fresh timestamped test paths as part of the smoke flow.
- `api-smoke-test-existing-key.sh` is the script that should be pointed at the
  built-in local tenant key.
- If the final upload-limit boundary check unexpectedly returns `507` instead of
  `202`, inspect tenant `uploads` records before blaming the test itself.
  Stale `INITIATED` / `UPLOADING` multipart rows can consume reserved quota even
  when the file tree looks empty.

## Notes

- `api-smoke-test.sh` expects `POST /v1/provision` to return `tenant_id`, `api_key`, and `status`.
- Tenant readiness is checked through `GET /v1/status`.
- File operations use `/v1/fs/*` and include nested directory coverage.
- Semantic recall polling knobs for API smoke are `SEMANTIC_TIMEOUT_S` and `SEMANTIC_INTERVAL_S`.
- Set `RUN_SEMANTIC_CHECKS=0` to skip semantic text recall and image-associated recall in `api-smoke-test.sh`.
- Semantic recall polling knobs for CLI smoke are `CLI_SEMANTIC_TIMEOUT_S` and `CLI_SEMANTIC_INTERVAL_S`.
- Set `RUN_CLI_SEMANTIC_CHECKS=0` to skip semantic text recall and image-associated recall in `cli-smoke-test.sh`.
- Image fixture path is `DRIVE9_IMAGE_FIXTURE_PATH` (default `e2e/fixtures/cat03.jpg`) and uses the repo-local fixture.
- Large-file scenario is enabled by default (`RUN_LARGE_FILE=1`) and runs a multipart upload using checksum-bound presigned parts.
- You can tune size with `LARGE_FILE_MB` (default `100`).
- CLI smoke large-file size can be tuned with `CLI_LARGE_FILE_MB` (default `100`).
- API batch small-file coverage can be tuned with `BATCH_SMALL_FILE_COUNT` (default `10`).
- CLI batch small-file coverage can be tuned with `CLI_BATCH_SMALL_FILE_COUNT` (default `10`).
- API retry knobs for throttling are `REQUEST_MAX_RETRIES` and `REQUEST_RETRY_SLEEP_S`; the FUSE correctness/SQLite/concurrency workloads use these for provisioning/status and CLI retry loops.
- CLI retry knobs for `cli-smoke-test.sh` and `fuse-smoke-test.sh` throttling are `CLI_MAX_RETRIES` and `CLI_RETRY_SLEEP_S`.
- FUSE mount readiness knobs are `MOUNT_READY_TIMEOUT_S`, `MOUNT_READY_INTERVAL_S`, and `FUSE_MOUNT_ROOT`.
- FUSE correctness workload knobs are `FUSE_CORRECTNESS_LARGE_MB` and `FUSE_CORRECTNESS_KEEP_ARTIFACTS`.
- FUSE SQLite correctness workload knobs are `FUSE_SQLITE_ROWS`, `FUSE_SQLITE_CHURN_ROUNDS`, `FUSE_SQLITE_CONCURRENCY_READERS`, `FUSE_SQLITE_CONCURRENCY_WRITES`, `FUSE_SQLITE_WORKLOAD_TIMEOUT_S`, `FUSE_SQLITE_KEEP_ARTIFACTS`, `RUN_FUSE_SQLITE_WAL`, `RUN_FUSE_SQLITE_CHURN`, and `RUN_FUSE_SQLITE_CONCURRENCY`.
- FUSE concurrency workload knobs are `FUSE_CONCURRENCY_WORKERS`, `FUSE_CONCURRENCY_FILES_PER_WORKER`, `FUSE_CONCURRENCY_READER_WORKERS`, `FUSE_CONCURRENCY_PAYLOAD_KB`, `FUSE_CONCURRENCY_TIMEOUT_S`, and `FUSE_CONCURRENCY_KEEP_ARTIFACTS`.
- FUSE POSIX/fsx workload knobs are `FUSE_POSIX_FSX_OPS`, `FUSE_POSIX_FSX_MAX_BYTES`, `FUSE_POSIX_FSX_SEED`, `FUSE_POSIX_FSX_TIMEOUT_S`, and `FUSE_POSIX_FSX_KEEP_ARTIFACTS`. When enabled in CI, failures are hard failures.
- FUSE performance baseline knobs are `FUSE_PERF_SMALL_FILES`, `FUSE_PERF_SMALL_BYTES`, `FUSE_PERF_LARGE_MB`, `FUSE_PERF_READ_PASSES`, `FUSE_PERF_SQLITE_ROWS`, `FUSE_PERF_KEEP_ARTIFACTS`, `FUSE_PERF_ARTIFACT_DIR`, `FUSE_PERF_COMPARE_WARN_RATIO`, and `FUSE_PERF_COMPARE_FAIL_ON_REGRESSION`. The baseline records small-file, large-file, rollback-journal SQLite, WAL SQLite, and WAL checkpoint metrics; SQLite rows are read back as payload bytes and SHA-256 verified before metrics are accepted.
- Layer filesystem knobs are `RUN_LAYER_FUSE_SMOKE`, `LAYER_FUSE_STRICT_PREREQS`, `LAYER_DIFF_TIMEOUT_S`, and `LAYER_DIFF_INTERVAL_S`. Ordinary PR `local-e2e.yml` runs `layer-fs-smoke-test.sh` with `RUN_LAYER_FUSE_SMOKE=1` against `drive9-server-local`, so layer schema/API/FUSE restore coverage does not depend on a dev backend deployment. `layer-fs-smoke-test-realdev.sh` is the manual shared-dev counterpart and is not wired into `local-e2e` CI. `smoke-all.sh` defaults `RUN_LAYER_FUSE_SMOKE` from `RUN_FUSE_SMOKE`.
- Full daily local-e2e knobs are `RUN_E2E_SMOKE_ALL` and `RUN_GIT_FEATURE_MATRIX`; the scheduled run sets both to `1`, and manual `workflow_dispatch` runs can enable them with `run_e2e_smoke_all=1` and `run_git_feature_matrix=1`.
- `local-e2e.yml` runs the lightweight portable pack/unpack e2e on ordinary PR triggers. It does not run the performance baseline or heavy FUSE/Git detectors on ordinary PR triggers. Use manual `workflow_dispatch` inputs `run_fuse_concurrency_stress=1`, `run_fuse_posix_fsx=1`, `run_fuse_sqlite_wal=1`, `run_fuse_sqlite_churn=1`, `run_fuse_sqlite_concurrency=1`, `run_fuse_performance_baseline=1`, `compare_fuse_performance_metrics=1`, `run_e2e_smoke_all=1`, and `run_git_feature_matrix=1` to enable them on demand. The scheduled daily run enables all of these flags; concurrency stress, POSIX/fsx, full smoke-all, and Git feature matrix run as separate hard-fail steps after the release gate and metrics archive, and all are attempted so one failure does not hide another workload's result. `run_e2e_smoke_all=1` also enables Git workspace smoke coverage.
- Set `archive_fuse_performance_metrics=1` on manual `local-e2e` runs, or use the daily scheduled run, to copy `performance-metrics-*.json`, `performance-compare-*.json`, `performance-compare-*.md`, mount logs, and an archive manifest to the Drive9 CI workspace under `/benchmarks/fuse-performance/<YYYY>/<MM>/<DD>/<branch>/<sha>/<run_id>-<attempt>/`. The same files are still uploaded as the GitHub artifact `fuse-performance-baseline`.
- Set `compare_fuse_performance_metrics=1` on manual `local-e2e` runs, or use the daily scheduled run, to compare current metrics against the latest Drive9 archive before archiving the current run. By default, `FUSE_PERF_COMPARE_FAIL_ON_REGRESSION=1` makes any metric below `1 - FUSE_PERF_COMPARE_WARN_RATIO` fail the compare step after writing JSON/Markdown reports. Missing historical baselines, parameter mismatches, and legacy baselines missing newly added workloads still produce non-failing warnings; invalid current metrics, broken Drive9 compare configuration, malformed archived manifests, and structurally invalid baseline metrics fail closed.
- The daily local-e2e gate intentionally covers the local CI-safe SQLite/Git/FUSE scripts. `verify-description-e2e.sh`, `verify-description-tidb-zero-e2e.sh`, and the full `pjdfstest-suite.sh` flow remain explicit environment-specific runs because they require separate Docker/Ollama, TiDB Cloud Zero, or root/pjdfstest prerequisites.
- FUSE release-gate knobs are `FUSE_STRICT_PREREQS`, `RUN_FUSE_GIT_CLONE`, `FUSE_GIT_CLONE_URL`, `FUSE_GIT_CLONE_TIMEOUT_S`, `RUN_FUSE_UMOUNT_DURABLE`, `FUSE_UMOUNT_TIMEOUT`, `RUN_FUSE_LOG_AUDIT`, `RUN_FUSE_ALL_WORKLOADS`, `RUN_FUSE_SQLITE_CORRECTNESS`, `RUN_FUSE_CONCURRENCY_STRESS`, `RUN_FUSE_POSIX_FSX`, `RUN_FUSE_PERFORMANCE_BASELINE`, and the FUSE correctness/SQLite/concurrency/POSIX/fsx/performance workload knobs. Set `RUN_FUSE_ALL_WORKLOADS=1` to default concurrency stress, POSIX/fsx, and performance baseline to enabled in one release-gate command; explicit per-workload env vars still take precedence. `local-e2e.yml` intentionally overrides `RUN_FUSE_CONCURRENCY_STRESS=0` and `RUN_FUSE_POSIX_FSX=0` for its release-gate step, then runs `fuse-concurrency-stress.sh` and `fuse-posix-fsx-gate.sh` separately after metrics artifact/archive steps.
- Git workspace smoke defaults to `drive9`, `kimi-cli`, and `kimi-code`. Override with `GIT_WORKSPACE_REPOS='slug=https://example/repo.git,...'`.
- Git workspace scenarios default to `agent_edit_add_commit,agent_patch_apply,sandbox_restore`; tune with `GIT_WORKSPACE_SCENARIOS`.
- Git workspace file-count knobs are `GIT_WORKSPACE_EXISTING_FILES`, `GIT_WORKSPACE_NEW_FILES`, and `GIT_WORKSPACE_PATCH_FILES`.
- Git workspace timeout knobs are `GIT_WORKSPACE_CLONE_TIMEOUT_S` and `GIT_WORKSPACE_GIT_TIMEOUT_S`.
- Git workspace clone uses `drive9 git clone --fast --blobless --hydrate=${GIT_WORKSPACE_HYDRATE:-sync}` inside a `--profile=coding-agent` FUSE mount.
- Git ops smoke uses `git_fixture.py` to create a local bare remote, so it
  is suitable for `drive9-server-local` and does not depend on dev/prod tenant
  schema rollout. It runs the matrix from `GIT_OPS_PROFILES`
  (`coding-agent,portable`) and `GIT_OPS_CLONE_MODES`
  (`native,fast,blobless`).
- Git ops native clone cases use explicit `.git` pack/unpack for sandbox
  replacement. Fast clone cases disable auto-pack and must recover through the
  Git workspace checkpoint/restore path.
- Git ops knobs are `GIT_OPS_HYDRATE`, `GIT_OPS_GIT_TIMEOUT_S`,
  `GIT_OPS_CLONE_TIMEOUT_S`, `GIT_OPS_KEEP_ARTIFACTS`, and
  `GIT_OPS_TRACE_GIT`.
- CLI source knobs are `CLI_SOURCE` (`build` or `official`), `CLI_RELEASE_BASE_URL`, and optional `CLI_RELEASE_VERSION`.
- API upload-limit boundary check is enabled by default via `RUN_UPLOAD_LIMIT_BOUNDARY=1`.
- `UPLOAD_LIMIT_BYTES` controls the boundary value checked by API e2e (default `10737418240`).
- CLI upload-limit boundary check is enabled by default via `RUN_CLI_UPLOAD_LIMIT_BOUNDARY=1`.
- `CLI_UPLOAD_LIMIT_BYTES` controls the boundary value checked by CLI e2e (default `10737418240`).
- `fuse-smoke-test.sh` will `SKIP` when host prerequisites are missing (for example no `/dev/fuse`) unless `FUSE_STRICT_PREREQS=1`.
- `fuse-release-gate.sh` is the strict CI/release entry point and enables git clone/status/log, durable `umount --timeout` remount checks, mount-log audit, manifest read correctness, and SQLite rollback-journal correctness. Set `RUN_FUSE_ALL_WORKLOADS=1` to add concurrency stress, fsx-style POSIX coverage, and threshold-free performance metrics in one command. Set `RUN_FUSE_SQLITE_CORRECTNESS=0` to skip SQLite temporarily while diagnosing host-specific FUSE failures, `RUN_FUSE_CONCURRENCY_STRESS=1` to add bounded concurrency stress, `RUN_FUSE_POSIX_FSX=1` to add fsx-style POSIX coverage, or `RUN_FUSE_PERFORMANCE_BASELINE=1` to add threshold-free performance metrics.
