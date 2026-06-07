# drive9 E2E tests

Live end-to-end scripts for validating deployed `drive9-server` behavior,
including local single-tenant validation via `drive9-server-local`.

## Prerequisites

- A running server endpoint (`DRIVE9_BASE`)
- `jq` installed
- Bash 4+

## Scripts

| Script | What it validates |
|--------|--------------------|
| `api-smoke-test.sh` | Fresh provisioning, status polling, nested+batch file ops, hardlink/copy/rename/delete checks, grep/find checks, semantic text recall, image-associated recall, sql checks, large multipart upload+download |
| `api-smoke-test-existing-key.sh` | Existing API key status/list checks |
| `cli-smoke-test.sh` | End-to-end CLI workflow including `fs symlink`, `fs hardlink`, `fs grep`/`fs find`, semantic/image-associated recall checks, image `fs cp`+`fs find`, and large multipart `fs cp` upload/download |
| `layer-fs-smoke-test.sh` | Layer filesystem API+CLI workflow: create by name/tag, entry diff, checkpoint lookup, rollback state, and commit into base filesystem |
| `fuse-smoke-test.sh` | FUSE mount lifecycle, file/dir/symlink/hardlink/rename/stat semantics, cross-channel consistency, mounted 10KiB→8MiB→10KiB tier-transition parity, read-only and error-path checks |
| `fuse-correctness-workload.sh` | Real read-only FUSE workload over a manifest fixture: `find`, `grep`, `stat`, `cat`, `sha256`, symlink, hardlink, unicode/space paths, empty files, binary files, and 8MiB+ files |
| `fuse-sqlite-correctness.sh` | Real writable FUSE SQLite correctness workload with rollback-journal mode, `PRAGMA integrity_check`, unmount/remount parity, and remote snapshot verification; set `RUN_FUSE_SQLITE_WAL=1` for WAL, `RUN_FUSE_SQLITE_CHURN=1` for repeated large-DB rewrite churn, and `RUN_FUSE_SQLITE_CONCURRENCY=1` for the bounded readers/writer detector |
| `fuse-concurrency-stress.sh` | Real writable FUSE concurrency workload with parallel writers/readers, atomic rename, unlink churn, open-handle rename reads, and deterministic final manifest checks |
| `fuse-performance-baseline.sh` | Opt-in real writable FUSE baseline that records small-file, large-file, repeated large-read, and SQLite transaction/read metrics as JSON artifacts without hardcoded throughput thresholds; SQLite reads verify stored row payload bytes against row checksums |
| `fuse-release-gate.sh` | Strict FUSE release/CI gate with hard prereq failures, small-repo git clone/status/log, durable umount/remount, mount-log audit, manifest-based FUSE correctness workload, and SQLite rollback-journal correctness; set `RUN_FUSE_SQLITE_CORRECTNESS=0` to skip SQLite temporarily, `RUN_FUSE_CONCURRENCY_STRESS=1` to add bounded concurrency stress, and `RUN_FUSE_PERFORMANCE_BASELINE=1` to add performance metrics |
| `git-workspace-smoke-test.sh` | Git workspace fast-blobless clone with coding-agent local overlay, batched tracked-file edits, ignored local-only paths, `git add`/`commit`, `git apply`, and remount restore |
| `posix-permission-smoke-test.sh` | POSIX permission coverage: API mkdir/chmod mode propagation, CLI `fs chmod`, FUSE `chmod`/`mkdir -m` with remote and local stat parity |
| `smoke-all.sh` | Runs API + CLI + journal + layer FS + FUSE + POSIX permission smoke scripts in sequence with aggregated pass/fail; set `RUN_FUSE_SMOKE=0` to skip FUSE symlink/hardlink coverage and `RUN_GIT_WORKSPACE_SMOKE=1` to include Git workspace coverage |
| `local-smoke.sh` | Starts `drive9-server-local` with a disposable local DB by default, then runs `smoke-all.sh` with semantic checks disabled and FUSE smoke skipped unless `RUN_FUSE_SMOKE=1` |

## Run

Use a hosted deployment by default. For local development on this machine, use
`drive9-server-local` instead.

### Hosted endpoints

#### Deployment endpoints

```bash
# Dev
export DRIVE9_BASE="http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com"

# Prod
export DRIVE9_BASE="https://api.drive9.ai"
```

#### Run smoke scripts

```bash
bash e2e/api-smoke-test.sh

DRIVE9_API_KEY=drive9_xxx bash e2e/api-smoke-test-existing-key.sh

bash e2e/cli-smoke-test.sh

# Layer filesystem API+CLI smoke
bash e2e/layer-fs-smoke-test.sh

# Use official released drive9 CLI instead of local build
CLI_SOURCE=official bash e2e/cli-smoke-test.sh

bash e2e/fuse-smoke-test.sh

# Manifest-based read correctness workload on a real read-only FUSE mount.
bash e2e/fuse-correctness-workload.sh

# SQLite rollback-journal correctness on a real writable FUSE mount.
bash e2e/fuse-sqlite-correctness.sh

# Bounded concurrency stress on a real writable FUSE mount.
bash e2e/fuse-concurrency-stress.sh

# Opt-in performance baseline with JSON metrics artifacts.
bash e2e/fuse-performance-baseline.sh

# Fast-blobless Git workspace smoke. This is intentionally opt-in for broad
# smoke runs because it clones real repositories and needs FUSE support.
bash e2e/git-workspace-smoke-test.sh

# Strict FUSE release gate used by CI
bash e2e/fuse-release-gate.sh

# Add the concurrency stress workload to the strict FUSE release gate.
RUN_FUSE_CONCURRENCY_STRESS=1 bash e2e/fuse-release-gate.sh

# Add the threshold-free performance baseline to the strict FUSE release gate.
RUN_FUSE_PERFORMANCE_BASELINE=1 bash e2e/fuse-release-gate.sh

# Preserve performance metrics outside the run root for CI artifact upload.
FUSE_PERF_ARTIFACT_DIR=e2e-artifacts/fuse-performance \
  RUN_FUSE_PERFORMANCE_BASELINE=1 bash e2e/fuse-release-gate.sh

# Use official released drive9 CLI for FUSE smoke
CLI_SOURCE=official bash e2e/fuse-smoke-test.sh
CLI_SOURCE=official bash e2e/fuse-correctness-workload.sh
CLI_SOURCE=official bash e2e/fuse-sqlite-correctness.sh
CLI_SOURCE=official bash e2e/fuse-concurrency-stress.sh
CLI_SOURCE=official bash e2e/fuse-performance-baseline.sh
CLI_SOURCE=official bash e2e/fuse-release-gate.sh
CLI_SOURCE=official bash e2e/posix-permission-smoke-test.sh

bash e2e/posix-permission-smoke-test.sh

bash e2e/smoke-all.sh
```

#### On-demand POSIX compatibility matrix

`posix-feature-matrix.sh` is not part of the normal E2E smoke entry points.
Run it only when you explicitly need a pjdfstest-based POSIX compatibility
report:

```bash
PJDFSTEST_DIR=/path/to/pjdfstest bash e2e/posix-feature-matrix.sh
```

By default it writes directly under `e2e/reports/`, for example
`e2e/reports/posix-feature-report-<timestamp>.md`, and counts results using
pjdfstest/prove cases. If `FEATURE_MATRIX_REPORT_DIR` points at a run-specific
directory, the same report filename is written inside that directory instead.

- Knobs: `FEATURE_MATRIX_REPORT_DIR`, `FEATURE_MATRIX_STRICT_ALL`,
  `PJDFSTEST_DIR`, `PJDFSTEST_TESTS`, `PJDFSTEST_BIN`,
  `PJDFSTEST_TIMEOUT_S`, and `PJDFSTEST_ALLOW_NONROOT`.
- Build pjdfstest before running so either `$PJDFSTEST_DIR/pjdfstest` exists
  or `pjdfstest` is on `PATH`. The runner adds the pjdfstest binary directory
  to `PATH` while invoking `prove`.
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

# Opt-in small-file, large-file, and SQLite performance baseline.
bash e2e/fuse-performance-baseline.sh

# Strict FUSE release gate using the repo build.
bash e2e/fuse-release-gate.sh

# Strict FUSE release gate plus bounded concurrency stress.
RUN_FUSE_CONCURRENCY_STRESS=1 bash e2e/fuse-release-gate.sh

# Strict FUSE release gate plus threshold-free performance metrics.
RUN_FUSE_PERFORMANCE_BASELINE=1 bash e2e/fuse-release-gate.sh

# POSIX permission smoke (API + CLI + FUSE).
bash e2e/posix-permission-smoke-test.sh

# Run API + CLI + FUSE + POSIX permission in sequence.
bash e2e/smoke-all.sh

# Include Git workspace fast-clone coverage in smoke-all.
RUN_GIT_WORKSPACE_SMOKE=1 bash e2e/smoke-all.sh
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
- FUSE performance baseline knobs are `FUSE_PERF_SMALL_FILES`, `FUSE_PERF_SMALL_BYTES`, `FUSE_PERF_LARGE_MB`, `FUSE_PERF_READ_PASSES`, `FUSE_PERF_SQLITE_ROWS`, `FUSE_PERF_KEEP_ARTIFACTS`, and `FUSE_PERF_ARTIFACT_DIR`.
- `local-e2e.yml` does not run the performance baseline or heavy FUSE detectors on ordinary PR triggers. Use manual `workflow_dispatch` inputs `run_fuse_concurrency_stress=1`, `run_fuse_sqlite_wal=1`, `run_fuse_sqlite_churn=1`, `run_fuse_sqlite_concurrency=1`, and `run_fuse_performance_baseline=1` to enable them on demand. The scheduled daily run enables all of these flags; concurrency stress runs as a separate step after the release gate and metrics archive, and a stress failure fails the scheduled workflow.
- Set `archive_fuse_performance_metrics=1` on manual `local-e2e` runs, or use the daily scheduled run, to copy `performance-metrics-*.json`, mount logs, and an archive manifest to the Drive9 CI workspace under `/benchmarks/fuse-performance/<YYYY>/<MM>/<DD>/<branch>/<sha>/<run_id>-<attempt>/`. The same files are still uploaded as the GitHub artifact `fuse-performance-baseline`.
- FUSE release-gate knobs are `FUSE_STRICT_PREREQS`, `RUN_FUSE_GIT_CLONE`, `FUSE_GIT_CLONE_URL`, `FUSE_GIT_CLONE_TIMEOUT_S`, `RUN_FUSE_UMOUNT_DURABLE`, `FUSE_UMOUNT_TIMEOUT`, `RUN_FUSE_LOG_AUDIT`, `RUN_FUSE_SQLITE_CORRECTNESS`, `RUN_FUSE_CONCURRENCY_STRESS`, `RUN_FUSE_PERFORMANCE_BASELINE`, and the FUSE correctness/SQLite/concurrency/performance workload knobs. `local-e2e.yml` intentionally overrides `RUN_FUSE_CONCURRENCY_STRESS=0` for its release-gate step and runs `fuse-concurrency-stress.sh` separately.
- Git workspace smoke defaults to `drive9`, `kimi-cli`, and `kimi-code`. Override with `GIT_WORKSPACE_REPOS='slug=https://example/repo.git,...'`.
- Git workspace scenarios default to `agent_edit_add_commit,agent_patch_apply,sandbox_restore`; tune with `GIT_WORKSPACE_SCENARIOS`.
- Git workspace file-count knobs are `GIT_WORKSPACE_EXISTING_FILES`, `GIT_WORKSPACE_NEW_FILES`, and `GIT_WORKSPACE_PATCH_FILES`.
- Git workspace timeout knobs are `GIT_WORKSPACE_CLONE_TIMEOUT_S` and `GIT_WORKSPACE_GIT_TIMEOUT_S`.
- Git workspace clone uses `drive9 git clone --fast --blobless --hydrate=${GIT_WORKSPACE_HYDRATE:-sync}` inside a `--profile=coding-agent` FUSE mount.
- CLI source knobs are `CLI_SOURCE` (`build` or `official`), `CLI_RELEASE_BASE_URL`, and optional `CLI_RELEASE_VERSION`.
- API upload-limit boundary check is enabled by default via `RUN_UPLOAD_LIMIT_BOUNDARY=1`.
- `UPLOAD_LIMIT_BYTES` controls the boundary value checked by API e2e (default `10737418240`).
- CLI upload-limit boundary check is enabled by default via `RUN_CLI_UPLOAD_LIMIT_BOUNDARY=1`.
- `CLI_UPLOAD_LIMIT_BYTES` controls the boundary value checked by CLI e2e (default `10737418240`).
- `fuse-smoke-test.sh` will `SKIP` when host prerequisites are missing (for example no `/dev/fuse`) unless `FUSE_STRICT_PREREQS=1`.
- `fuse-release-gate.sh` is the strict CI/release entry point and enables git clone/status/log, durable `umount --timeout` remount checks, mount-log audit, manifest read correctness, and SQLite rollback-journal correctness. Set `RUN_FUSE_SQLITE_CORRECTNESS=0` to skip SQLite temporarily while diagnosing host-specific FUSE failures, `RUN_FUSE_CONCURRENCY_STRESS=1` to add bounded concurrency stress, or `RUN_FUSE_PERFORMANCE_BASELINE=1` to add threshold-free performance metrics.
