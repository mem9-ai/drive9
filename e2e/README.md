# drive9 E2E tests

Live end-to-end scripts for validating deployed `drive9-server` behavior,
including local validation via `drive9-server` with `DRIVE9_TENANT_PROVIDER=local`.

## Prerequisites

- A running server endpoint (`DRIVE9_BASE`)
- `jq` installed
- Bash 4+

## Scripts

| Script | What it validates |
|--------|--------------------|
| `migration_bulk_contract_test.go` | Opt-in live Client→Server #115 gate for Manifest cursor/empty-page behavior, partial BatchMkdir, duplicate delivery, commit-unknown re-observation, systemic failure, BatchChmod identity, and optional dedicated-shape fs_events persistence. Run through `make migration-server-contract` or the manual `Migration Large-Scale Gates` workflow |
| `api-smoke-test.sh` | Fresh provisioning, status polling, nested+batch file ops, hardlink/copy/rename/delete checks, grep/find checks, semantic text recall, image-associated recall, sql checks, large multipart upload+download; set `DRIVE9_API_KEY` to skip provision and reuse an existing tenant (cleans up its test tree in that mode) |
| `cli-smoke-test.sh` | End-to-end CLI workflow including `fs symlink`, `fs hardlink`, default-slot `pack`/`unpack`, `fs grep`/`fs find`, semantic/image-associated recall checks, image `fs cp`+`fs find`, and large multipart `fs cp` upload/download; honors `DRIVE9_API_KEY` to skip provision and reuse an existing tenant |
| `object-store-smoke-test.sh` | Local MinIO (docker/podman) or `OBJECT_S3_URI`: `fs mkdir/cp/ls/stat/cat/mv/rm`, stdin/stdout, rejected object ops, object FUSE mount writeback + remount, `mount status/health`, drain rejected. Does not need drive9-server. Set `OBJECT_STRICT_MOUNT=1` to fail if FUSE is missing |
| `layer-fs-smoke-test.sh` | Layer filesystem API+CLI+FUSE workflow: create by name/tag, diff/checkpoint lookup, rollback, commit, scope rejection, conflict detection, mkdir/upsert/whiteout/rename/symlink/chmod entries, CoW fork (tip/checkpoint pin, chain read, child/parent commit, delete/cascade, depth cap), HTTP CoW (POST /v1/layers + fork/chain, child GET objects falling back to a main-tree PUT, HTTP commit, DELETE ?cascade=1), and checkpoint/full restore into fresh local roots. Point at any backend with `DRIVE9_BASE`; set `RUN_LAYER_FUSE_SMOKE=1` (and optionally `LAYER_FUSE_STRICT_PREREQS=1`) for FUSE restore coverage |
| `pack-smoke-test.sh` | Portable profile pack/unpack over a deterministic local repo: offline npm `file:` install creates `node_modules`, Git staged/unstaged/untracked status changes are captured, pack writes the default hidden archive, fresh local-root unpack restores overlay files, symlinks, `.git`, `node_modules`, branch, HEAD, and `git status` |
| `fuse-smoke-test.sh` | FUSE mount lifecycle, file/dir/symlink/hardlink/rename/stat semantics, cross-channel consistency, `mount drain`/native `sync -f` drain checks, mounted 10KiB→8MiB→10KiB tier-transition parity, read-only and error-path checks |
| `fuse-correctness-workload.sh` | Real read-only FUSE workload over a manifest fixture: `find`, `grep`, `stat`, `cat`, `sha256`, symlink, hardlink, unicode/space paths, empty files, binary files, and 8MiB+ files |
| `fuse-sqlite-correctness.sh` | Real writable FUSE SQLite correctness workload with rollback-journal mode, `PRAGMA integrity_check`, unmount/remount parity, and remote snapshot verification; set `RUN_FUSE_SQLITE_WAL=1` for WAL, `RUN_FUSE_SQLITE_CHURN=1` for repeated large-DB rewrite churn, and `RUN_FUSE_SQLITE_CONCURRENCY=1` for the bounded readers/writer detector |
| `fuse-concurrency-stress.sh` | Real writable FUSE concurrency workload with parallel writers/readers, atomic rename, unlink churn, open-handle rename reads, and deterministic final manifest checks |
| `fuse-posix-fsx-gate.sh` | Opt-in JuiceFS-style POSIX/fsx subset over real writable FUSE: deterministic random write/read/truncate, atomic rename replacement, unlink-open reads, directory fsync, final model hash, unmount, and remote snapshot parity |
| `fuse-performance-baseline.sh` | Opt-in real writable FUSE baseline that records small-file, large-file, repeated large-read, and SQLite transaction/read metrics as JSON artifacts without hardcoded throughput thresholds; SQLite reads verify stored row payload bytes against row checksums |
| `fuse-release-gate.sh` | Strict FUSE release/CI gate with hard prereq failures, small-repo git clone/status/log, durable umount/remount, mount-log audit, manifest-based FUSE correctness workload, and SQLite rollback-journal correctness; set `RUN_FUSE_ALL_WORKLOADS=1` to add all optional release-gate workloads, `RUN_FUSE_SQLITE_CORRECTNESS=0` to skip SQLite temporarily, `RUN_FUSE_CONCURRENCY_STRESS=1` to add bounded concurrency stress, `RUN_FUSE_POSIX_FSX=1` to add the POSIX/fsx subset, and `RUN_FUSE_PERFORMANCE_BASELINE=1` to add performance metrics |
| `git-feature-smoke-test.sh` | Broader Git feature smoke on a coding-agent FUSE mount: clone modes (fast/blobless), readiness, ops, merge/rebase/stash, remount restore |
| `git-ops-smoke-test.sh` | Lightweight local Git gate using a local bare fixture: native clone, `drive9 git clone --fast`, and `drive9 git clone --fast --blobless` across `coding-agent` and `portable` profiles, followed by edit/add/commit/stash, remount into a fresh local root, and Git state/content verification |
| `git-workspace-ondemand-smoke-test.sh` | On-demand git-workspace discovery (G0–G4/AC5): never-`--fast` mount asserts zero `ListGitWorkspaces` (perf `refresh=0`, incl. `.git` path activity), live `--fast` arms with bounded refresh + wall-clock armed idle, fresh LocalRoot remount via `/.drive9/git-workspaces/index.json`, second `--fast` forces list (`forced_refresh≥2`), cleanup + remount stays dormant |
| `fuse-crash-recovery-test.sh` | FUSE crash-recovery gate: fsync'd small files plus a large mid-upload ShadowSpill survive `kill -9` of the mount daemon, recovered commits converge remotely, unlinked files do not resurrect, and the journal WAL compacts after a clean remount |
| `fuse-supervision-test.sh` | FUSE supervision PR gate: supervised mount ready + status/health; worker `kill -9` auto-heal; `umount` no-restart; remount after umount; `mount ensure` after whole-tree death; `--supervise-foreground` smoke; root/non-root `pseudoroot` projection and fast authorization failure when `/v1/tokens` is available (404 capability absence skips only the cross-repo pseudoroot block) |
| `fuse-write-perf-budget-test.sh` | FUSE write-path perf budgets: fsync-heavy workload with deterministic op-count budgets (remote writes/stats/lists/mutations, commit retries/failures) plus an fsync latency ceiling, asserted from mount perf counters |
| `fuse-patch-storage-class.sh` | PATCH-vs-storage-class mismatch (`patch_unsupported_target`) regression: seeds a db9-stored file above the restarted server's inline threshold, then partially overwrites it through a FUSE mount. Default `fixed` scenario asserts zero PATCH attempts, correct committed bytes, and storage heal to S3; `SCENARIO=repro`/`fallback` with `DRIVE9_SERVER_BIN`/`DRIVE9_CLI_BIN` overrides replay the pre-fix EINVAL loop and the old-server fallback locally |
| `git-workspace-smoke-test.sh` | Git workspace fast-blobless clone with coding-agent local overlay, batched tracked-file edits, ignored local-only paths, `git add`/`commit`, `git apply`, and remount restore |
| `posix-permission-smoke-test.sh` | POSIX permission coverage: API mkdir/chmod mode propagation, CLI `fs chmod`, FUSE `chmod`/`mkdir -m` with remote and local stat parity |
| `tokens-smoke-test.sh` | `/v1/tokens` management: dispatcher, scoped lifecycle/refresh, pseudoroot projected listing and hidden siblings, scoped gate, control-plane generate/list/status (needs `provider=local` mock IAM). Off in default `smoke-all.sh`; set `RUN_TOKENS_SMOKE=1` |
| `sse-retention-smoke-test.sh` | SSE `/v1/events` initial sync, live `file_changed` delivery + cursor replay, >1000-event backlog drain; optional long-window replay and short-retention sweep (`SSE_SWEEP_TEST=1`). Off in default `smoke-all.sh`; set `RUN_SSE_SMOKE=1` |
| `image-extract-config-smoke-test.sh` | Opt-in hosted HTTP test: provision a disposable tenant, validate tenant image-extract config (reject invalid key / unreachable base, persist a masked custom config), upload an image, assert a generated attribute tag via stat/find, disable the config, and delete the tenant. Manual-only: needs control-plane keys and a billable vision provider |
| `object-auth-smoke-test.sh` | Manual `--auth=server` STS mint: admin object-backend add/get/update, tenant object-namespace, then `fs cp/ls/cat` against a real bucket without `--auth=local`, plus outside-namespace deny. Not wired into CI/local-e2e; skip-if-env-missing. Needs tidbcloud-native plus a dedicated test bucket |
| `object-auth-s3-hosted-test.sh` | Manual hosted S3 `--auth=server` coverage: two-tenant `cust` vs `cust-evil` isolation (CLI + STS ListBucket), fail-closed mint, tenant key cannot register backends, read-only vs write mint, drive9↔S3 copy, object FUSE mount, and in-place STS refresh with `DRIVE9_OBJECT_SESSION_REFRESH_*_LEAD`. Same credentials as `object-auth-smoke-test.sh` |
| `object-auth-cos-hosted-test.sh` | Same 44-check hosted coverage as S3, against Tencent COS (`tccli`). Needs a dedicated test bucket + CAM user; never point at prod COS. Set `DRIVE9_E2E_OBJECT_REGION` and `DRIVE9_E2E_OBJECT_ACCOUNT_ID` (APPID) |
| `object-auth-tos-hosted-test.sh` | Same hosted coverage against Volcengine TOS (`tosutil`). AssumeRole only: `DRIVE9_E2E_OBJECT_ROLE_ARN` is required, plus region |
| `native-smoke-test.sh` | TiDB Cloud Native tenant lifecycle: CLI provision with credentials, status poll, basic fs ops (mkdir/cp/cat/ls/rm), delete + verification, trap cleanup on failure |
| `smoke-all.sh` | PR-gate aggregator aligned with `local-e2e.yml`: api, cli, layer-fs, fuse-release-gate, fuse-patch-storage-class, git-ops, git-workspace-ondemand, pack, fuse-crash-recovery, fuse-supervision, fuse-write-perf-budget. Re-exports `DRIVE9_API_KEY` for existing-tenant mode. `RUN_FUSE_SMOKE=0` skips FUSE-related suites; `RUN_API_ONLY=1` keeps only api + cli; `RUN_JOURNAL_SMOKE=1` / `RUN_POSIX_SMOKE=1` / `RUN_GIT_WORKSPACE_SMOKE=1` add post-merge extras; `RUN_TOKENS_SMOKE=1` / `RUN_SSE_SMOKE=1` add HTTP tokens and SSE retention (off even on post-merge) |

## CI automation tiers

Every script in this directory must be wired into one of these tiers (or be
explicitly listed as manual-only with a reason). Do not merge a new e2e script
without adding it to `.github/workflows/local-e2e.yml`.

| Tier | Trigger | What runs |
|------|---------|-----------|
| PR gate | `pull_request` to `main` (local-e2e) | api, cli, object-store, layer-fs, fuse-release-gate (smoke + correctness + sqlite rollback), fuse-patch-storage-class, git-ops, git-workspace-ondemand, portable pack/unpack, fuse-crash-recovery, fuse-supervision, fuse-write-perf-budget |
| Post-merge | `push` to `main` (local-e2e, coalesced via concurrency group) | PR gate + concurrency stress, POSIX/fsx, sqlite WAL/churn/concurrency, `smoke-all.sh` extras (journal, posix-permission, git-workspace), git feature smoke |
| Nightly | cron 20:17 UTC (local-e2e) | Post-merge set + FUSE performance baseline/archive/compare (compare is report-only; hosted-runner noise) |
| Manual all | Local E2E `workflow_dispatch` with `run_all_e2e=1` | Everything above |
| Manual only | not wired, run by hand | `description-smoke-test.sh` (Docker + Ollama/stub embedder), `native-smoke-test.sh` (TiDB Cloud Native — requires credentials), `image-extract-config-smoke-test.sh` (hosted control-plane + billable vision provider), `object-auth-smoke-test.sh` (tidbcloud-native + real object-store STS mint; `--auth=server` cannot run in local-e2e), `object-auth-s3-hosted-test.sh` / `object-auth-cos-hosted-test.sh` / `object-auth-tos-hosted-test.sh` (hosted S3/COS/TOS isolation/mount/refresh; same reason) |

The live Migration #865/#115 contract is wired to the separate manual
`Migration Large-Scale Gates` workflow because it requires a deployment that
already contains Server #115 plus repository secrets
`DRIVE9_MIGRATION_CONTRACT_BASE` and `DRIVE9_MIGRATION_CONTRACT_API_KEY`.
For a local Server binary or an existing tenant, run:

```bash
make migration-server-contract \
  MIGRATION_CONTRACT_BASE="$DRIVE9_BASE" \
  MIGRATION_CONTRACT_API_KEY="$DRIVE9_API_KEY" \
  MIGRATION_CONTRACT_SQL=1
```

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
`drive9-server` with `DRIVE9_TENANT_PROVIDER=local` instead.

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

# Reuse an existing tenant (skip provision)
DRIVE9_API_KEY=drive9_xxx bash e2e/api-smoke-test.sh

bash e2e/cli-smoke-test.sh

# Layer filesystem API+CLI smoke. Set RUN_LAYER_FUSE_SMOKE=1 to also run
# real FUSE layer checkpoint/full restore coverage. Against a remote/dev
# backend, set DRIVE9_BASE and optionally LAYER_FUSE_STRICT_PREREQS=1.
bash e2e/layer-fs-smoke-test.sh
DRIVE9_BASE=https://your-dev RUN_LAYER_FUSE_SMOKE=1 LAYER_FUSE_STRICT_PREREQS=1 \
  bash e2e/layer-fs-smoke-test.sh

# Use official released drive9 CLI instead of local build
CLI_SOURCE=official bash e2e/cli-smoke-test.sh

# Portable profile pack/unpack over a stable local Git/npm fixture.
bash e2e/pack-smoke-test.sh

# Use official released drive9 CLI for portable profile pack/unpack.
CLI_SOURCE=official bash e2e/pack-smoke-test.sh

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

# Broader Git feature smoke (clone modes / ops / remount restore)
bash e2e/git-feature-smoke-test.sh

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

# Tenant image-extract config smoke (hosted control-plane + billable vision
# provider). Skips before any HTTP call when a required variable is missing.
# Must target a multi-tenant drive9-server; DRIVE9_TENANT_PROVIDER=local uses
# env-only image extract and cannot validate tenant config resolution.
export DRIVE9_BASE="https://..."
export DRIVE9_TIDBCLOUD_PUBLIC_KEY="..."
export DRIVE9_TIDBCLOUD_PRIVATE_KEY="..."
export DRIVE9_E2E_IMAGE_EXTRACT_API_BASE="https://..."
export DRIVE9_E2E_IMAGE_EXTRACT_API_KEY="..."
export DRIVE9_E2E_IMAGE_EXTRACT_MODEL="..."
bash e2e/image-extract-config-smoke-test.sh

bash e2e/smoke-all.sh

# Skip FUSE-related suites.
RUN_FUSE_SMOKE=0 bash e2e/smoke-all.sh
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

#### Local `drive9-server` (`DRIVE9_TENANT_PROVIDER=local`) notes

- Prefer `POST /v1/provision` and the returned `api_key` (a JWT). Do not
  assume `local-dev-key` authenticates.
- `api-smoke-test.sh`, `cli-smoke-test.sh`, and `fuse-smoke-test.sh` provision
  unless `DRIVE9_API_KEY` is already set, then poll `/v1/status` until `active`.
- If the final upload-limit boundary check unexpectedly returns `507` instead of
  `202`, inspect tenant `uploads` records before blaming the test itself.
  Stale `INITIATED` / `UPLOADING` multipart rows can consume reserved quota even
  when the file tree looks empty.

#### Local server binary

`make e2e-local` (`scripts/e2e-local.sh`) starts TiDB if needed, then in-tree
`drive9-server` with `DRIVE9_TENANT_PROVIDER=local`, then `e2e/smoke-all.sh`.
`local-e2e.yml` and `make run-server-local` also use `provider=local` unless
`DRIVE9_SERVER_BIN` points at a pre-built executable. Blackbox
`--server-mode local` uses `--local-server`, which defaults to
`DRIVE9_SERVER_BIN`.

## Notes

- `api-smoke-test.sh` expects `POST /v1/provision` to return `tenant_id`, `api_key`, and `status`.
- `image-extract-config-smoke-test.sh` always provisions a disposable tenant
  with `DRIVE9_TIDBCLOUD_PUBLIC_KEY` / `DRIVE9_TIDBCLOUD_PRIVATE_KEY`. Provider
  configuration requires `DRIVE9_E2E_IMAGE_EXTRACT_API_BASE`,
  `DRIVE9_E2E_IMAGE_EXTRACT_API_KEY`, and `DRIVE9_E2E_IMAGE_EXTRACT_MODEL`.
  The config PUT performs a real validation request before storing the config,
  and the uploaded fixture causes a second provider request in the image
  worker. The custom prompt fixes the attribute key `e2e_marker`; its value is
  a short lowercase English noun chosen by the model from the image. The test
  never prints either private key. Exit trap disables the custom config first,
  then removes the test file tree and deletes the tenant. `POLL_TIMEOUT_S`
  controls tenant readiness (default `600`); `IMAGE_EXTRACT_TIMEOUT_S` and
  `IMAGE_EXTRACT_INTERVAL_S` control tag polling (defaults `180` and `3`).
  After the positive path, the test disables the custom config, uploads a new
  image, and requires its semantic text and tags to stay empty for
  `DISABLED_EXTRACT_WAIT_S` (default `30`). Individual HTTP calls use
  `CURL_CONNECT_TIMEOUT_S=10` and `CURL_MAX_TIME_S=120` by default. Before
  storing a valid provider config, the test verifies that an invalid API key
  and an unreachable API base are rejected without changing the tenant's
  config. Override `DRIVE9_E2E_UNREACHABLE_API_BASE` only when the default
  public, closed-port endpoint is unsuitable.
- Tenant readiness is checked through `GET /v1/status`.
- `api-smoke-test.sh` defaults `POLL_TIMEOUT_S` to 300s because schema initialization can exceed 120s in some regions.
- File operations use `/v1/fs/*` and include nested directory coverage.
- Semantic recall polling knobs for API smoke are `SEMANTIC_TIMEOUT_S` and `SEMANTIC_INTERVAL_S`.
- Semantic text recall and image-associated recall in `api-smoke-test.sh` are off by default; set `RUN_SEMANTIC_CHECKS=1` to enable them.
- Semantic recall polling knobs for CLI smoke are `CLI_SEMANTIC_TIMEOUT_S` and `CLI_SEMANTIC_INTERVAL_S`.
- Semantic text recall and image-associated recall in `cli-smoke-test.sh` are off by default; set `RUN_CLI_SEMANTIC_CHECKS=1` to enable them.
- API `POST /v1/sql` checks are off by default; set `RUN_SQL_CHECKS=1` to enable them.
- CLI tenant fork checks are off by default; set `RUN_CLI_FORK_CHECKS=1` to enable them.
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
- Layer filesystem knobs are `RUN_LAYER_FUSE_SMOKE`, `LAYER_FUSE_STRICT_PREREQS`, `LAYER_DIFF_TIMEOUT_S`, and `LAYER_DIFF_INTERVAL_S`. Ordinary PR `local-e2e.yml` runs `layer-fs-smoke-test.sh` with `RUN_LAYER_FUSE_SMOKE=1` against local `drive9-server`, so layer schema/API/FUSE restore coverage does not depend on a dedicated dev backend. Point at any deployment with `DRIVE9_BASE`. `smoke-all.sh` defaults `RUN_LAYER_FUSE_SMOKE` from `RUN_FUSE_SMOKE`. FUSE mounts in these suites pass `--mode=fuse` (required on macOS, where `auto` is WebDAV).
- Full daily local-e2e knobs are `RUN_E2E_SMOKE_ALL` and `RUN_GIT_FEATURE_SMOKE`; the scheduled run sets both to `1`, and manual `workflow_dispatch` runs can enable them with `run_e2e_smoke_all=1` and `run_git_feature_smoke=1`.
- `local-e2e.yml` runs the lightweight pack smoke on ordinary PR triggers. It does not run the performance baseline or heavy FUSE/Git detectors on ordinary PR triggers. Use manual `workflow_dispatch` inputs `run_fuse_concurrency_stress=1`, `run_fuse_posix_fsx=1`, `run_fuse_sqlite_wal=1`, `run_fuse_sqlite_churn=1`, `run_fuse_sqlite_concurrency=1`, `run_fuse_performance_baseline=1`, `compare_fuse_performance_metrics=1`, `run_e2e_smoke_all=1`, and `run_git_feature_smoke=1` to enable them on demand. The scheduled daily run enables all of these flags; concurrency stress, POSIX/fsx, full smoke-all, and Git feature smoke run as separate hard-fail steps after the release gate and metrics archive, and all are attempted so one failure does not hide another workload's result. `run_e2e_smoke_all=1` also enables journal, posix-permission, and Git workspace smoke coverage.
- Set `archive_fuse_performance_metrics=1` on manual `local-e2e` runs, or use the daily scheduled run, to copy `performance-metrics-*.json`, `performance-compare-*.json`, `performance-compare-*.md`, mount logs, and an archive manifest to the Drive9 CI workspace under `/benchmarks/fuse-performance/<YYYY>/<MM>/<DD>/<branch>/<sha>/<run_id>-<attempt>/`. The same files are still uploaded as the GitHub artifact `fuse-performance-baseline`.
- Set `compare_fuse_performance_metrics=1` on manual `local-e2e` runs, or use the daily scheduled run, to compare current metrics against the latest Drive9 archive before archiving the current run. By default, `FUSE_PERF_COMPARE_FAIL_ON_REGRESSION=1` makes any metric below `1 - FUSE_PERF_COMPARE_WARN_RATIO` fail the compare step after writing JSON/Markdown reports. Missing historical baselines, parameter mismatches, and legacy baselines missing newly added workloads still produce non-failing warnings; invalid current metrics, broken Drive9 compare configuration, malformed archived manifests, and structurally invalid baseline metrics fail closed.
- The daily local-e2e gate intentionally covers the local CI-safe SQLite/Git/FUSE scripts. `description-smoke-test.sh` remains a manual environment-specific run (Docker + Ollama or stub embedder). Full pjdfstest lives under `blackbox` (`community.pjdfstest`).
- FUSE release-gate knobs are `FUSE_STRICT_PREREQS`, `RUN_FUSE_GIT_CLONE`, `FUSE_GIT_CLONE_URL`, `FUSE_GIT_CLONE_TIMEOUT_S`, `RUN_FUSE_UMOUNT_DURABLE`, `FUSE_UMOUNT_TIMEOUT`, `RUN_FUSE_LOG_AUDIT`, `RUN_FUSE_ALL_WORKLOADS`, `RUN_FUSE_SQLITE_CORRECTNESS`, `RUN_FUSE_CONCURRENCY_STRESS`, `RUN_FUSE_POSIX_FSX`, `RUN_FUSE_PERFORMANCE_BASELINE`, and the FUSE correctness/SQLite/concurrency/POSIX/fsx/performance workload knobs. Set `RUN_FUSE_ALL_WORKLOADS=1` to default concurrency stress, POSIX/fsx, and performance baseline to enabled in one release-gate command; explicit per-workload env vars still take precedence. `local-e2e.yml` intentionally overrides `RUN_FUSE_CONCURRENCY_STRESS=0` and `RUN_FUSE_POSIX_FSX=0` for its release-gate step, then runs `fuse-concurrency-stress.sh` and `fuse-posix-fsx-gate.sh` separately after metrics artifact/archive steps.
- Git workspace smoke defaults to `drive9`, `kimi-cli`, and `kimi-code`. Override with `GIT_WORKSPACE_REPOS='slug=https://example/repo.git,...'`.
- Git workspace scenarios default to `agent_edit_add_commit,agent_patch_apply,sandbox_restore`; tune with `GIT_WORKSPACE_SCENARIOS`.
- Git workspace file-count knobs are `GIT_WORKSPACE_EXISTING_FILES`, `GIT_WORKSPACE_NEW_FILES`, and `GIT_WORKSPACE_PATCH_FILES`.
- Git workspace timeout knobs are `GIT_WORKSPACE_CLONE_TIMEOUT_S` and `GIT_WORKSPACE_GIT_TIMEOUT_S`.
- Git workspace clone uses `drive9 git clone --fast --blobless --hydrate=${GIT_WORKSPACE_HYDRATE:-sync}` inside a `--profile=coding-agent` FUSE mount.
- Git ops smoke uses `tools/git_fixture.py` to create a local bare remote, so it
  is suitable for local `drive9-server` and does not depend on dev/prod tenant
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
- API upload-limit boundary check is enabled by default via `RUN_UPLOAD_LIMIT_BOUNDARY=1`. It defaults to `0` when `DRIVE9_API_KEY` is set (existing-tenant mode), because reserving `total_size` against an already-consumed quota can spuriously fail with 507.
- `UPLOAD_LIMIT_BYTES` controls the boundary value checked by API e2e (default `10737418240`).
- CLI upload-limit boundary check is enabled by default via `RUN_CLI_UPLOAD_LIMIT_BOUNDARY=1`. It defaults to `0` when `DRIVE9_API_KEY` is set (existing-tenant mode), for the same reason as the API check.
- `CLI_UPLOAD_LIMIT_BYTES` controls the boundary value checked by CLI e2e (default `10737418240`).
- `fuse-smoke-test.sh` will `SKIP` when host prerequisites are missing (for example no `/dev/fuse`) unless `FUSE_STRICT_PREREQS=1`.
- `fuse-release-gate.sh` is the strict CI/release entry point and enables git clone/status/log, durable `umount --timeout` remount checks, mount-log audit, manifest read correctness, and SQLite rollback-journal correctness. Set `RUN_FUSE_ALL_WORKLOADS=1` to add concurrency stress, fsx-style POSIX coverage, and threshold-free performance metrics in one command. Set `RUN_FUSE_SQLITE_CORRECTNESS=0` to skip SQLite temporarily while diagnosing host-specific FUSE failures, `RUN_FUSE_CONCURRENCY_STRESS=1` to add bounded concurrency stress, `RUN_FUSE_POSIX_FSX=1` to add fsx-style POSIX coverage, or `RUN_FUSE_PERFORMANCE_BASELINE=1` to add threshold-free performance metrics.
