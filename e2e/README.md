# drive9 E2E tests

Live end-to-end scripts for validating deployed `drive9-server` behavior,
including local single-tenant validation via `drive9-server-local`.

## Prerequisites

- A running server endpoint (`DRIVE9_BASE`)
- `jq` installed
- Bash 4+

## Run Against `drive9-server-local`

The local smoke flow that is currently exercised on this machine uses
`drive9-server-local`, not the old hosted/local-server path.

1. Start `drive9-server-local`.

```bash
export DRIVE9_LOCAL_DIR=/path/to/dir-containing-run-drive9-server-local.sh
cd "$DRIVE9_LOCAL_DIR"
./run-drive9-server-local.sh 2>&1 | tee local-server.log
```

2. Confirm the server is healthy.

```bash
curl http://127.0.0.1:9009/healthz
```

Expected response:

```json
{"status":"ok"}
```

3. Run the e2e smoke scripts against the local endpoint.

```bash
export DRIVE9_REPO_ROOT=/path/to/drive9
cd "$DRIVE9_REPO_ROOT"

export DRIVE9_BASE=http://127.0.0.1:9009

# Full API smoke on a fresh locally provisioned tenant.
bash e2e/api-smoke-test.sh

# Existing-key regression against the built-in local tenant.
DRIVE9_API_KEY="${DRIVE9_LOCAL_API_KEY:-local-dev-key}" bash e2e/api-smoke-test-existing-key.sh

# CLI smoke using the repo build.
bash e2e/cli-smoke-test.sh

# FUSE smoke using the repo build.
bash e2e/fuse-smoke-test.sh

# Run API + CLI + FUSE in sequence.
bash e2e/smoke-all.sh
```

4. Optional: use an already-built or official CLI instead of rebuilding.

```bash
CLI_SOURCE=official bash e2e/cli-smoke-test.sh
CLI_SOURCE=official bash e2e/fuse-smoke-test.sh
```

### `drive9-server-local` notes

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
- On this branch, `api` and `cli` smoke are passing against
  `drive9-server-local`. `fuse` still has a known directory-rename failure path
  tracked separately, so `smoke-all.sh` can still end in `PASS=2 FAIL=1`.

## Scripts

| Script | What it validates |
|--------|--------------------|
| `api-smoke-test.sh` | Fresh provisioning, status polling, nested+batch file ops, grep/find checks, semantic text recall, image-associated recall, sql checks, large multipart upload+download |
| `api-smoke-test-existing-key.sh` | Existing API key status/list checks |
| `cli-smoke-test.sh` | End-to-end CLI workflow including `fs grep`/`fs find`, semantic/image-associated recall checks, image `fs cp`+`fs find`, and large multipart `fs cp` upload/download |
| `fuse-smoke-test.sh` | FUSE mount lifecycle, file/dir/rename/stat semantics, cross-channel consistency, read-only and error-path checks |
| `tidbcloud-native-smoke-test.sh` | TiDB Cloud native provisioning via zero-instance header, status poll, file CRUD, search, copy/rename/delete |
| `smoke-all.sh` | Runs API + CLI + FUSE + tidbcloud-native (if configured) smoke scripts in sequence with aggregated pass/fail |

## Run

```bash
DEPLOY=https://<api-endpoint>

DRIVE9_BASE=$DEPLOY bash e2e/api-smoke-test.sh

DRIVE9_BASE=$DEPLOY DRIVE9_API_KEY=drive9_xxx bash e2e/api-smoke-test-existing-key.sh

DRIVE9_BASE=$DEPLOY bash e2e/cli-smoke-test.sh

# Use official released drive9 CLI instead of local build
DRIVE9_BASE=$DEPLOY CLI_SOURCE=official bash e2e/cli-smoke-test.sh

DRIVE9_BASE=$DEPLOY bash e2e/fuse-smoke-test.sh

# Use official released drive9 CLI for FUSE smoke
DRIVE9_BASE=$DEPLOY CLI_SOURCE=official bash e2e/fuse-smoke-test.sh

# TiDB Cloud native smoke (provision via zero-instance header + file ops)
DRIVE9_BASE=$DEPLOY TIDB_ZERO_INSTANCE_ID=<instance-id> bash e2e/tidbcloud-native-smoke-test.sh

DRIVE9_BASE=$DEPLOY bash e2e/smoke-all.sh
```

## Deployment Endpoints

### Dev

```bash
export DRIVE9_BASE="http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com"
```

### Prod

```bash
export DRIVE9_BASE="https://api.drive9.ai"
```

## Notes

- `api-smoke-test.sh` expects `POST /v1/provision` to return only `api_key` and `status`.
- Tenant readiness is checked through `GET /v1/status`.
- File operations use `/v1/fs/*` and include nested directory coverage.
- Semantic recall polling knobs for API smoke are `SEMANTIC_TIMEOUT_S` and `SEMANTIC_INTERVAL_S`.
- Semantic recall polling knobs for CLI smoke are `CLI_SEMANTIC_TIMEOUT_S` and `CLI_SEMANTIC_INTERVAL_S`.
- Image fixture path is `DRIVE9_IMAGE_FIXTURE_PATH` (default `e2e/fixtures/cat03.jpg`) and uses the repo-local fixture.
- Large-file scenario is enabled by default (`RUN_LARGE_FILE=1`) and runs a multipart upload using checksum-bound presigned parts.
- You can tune size with `LARGE_FILE_MB` (default `100`).
- CLI smoke large-file size can be tuned with `CLI_LARGE_FILE_MB` (default `100`).
- API batch small-file coverage can be tuned with `BATCH_SMALL_FILE_COUNT` (default `10`).
- CLI batch small-file coverage can be tuned with `CLI_BATCH_SMALL_FILE_COUNT` (default `10`).
- API retry knobs for throttling are `REQUEST_MAX_RETRIES` and `REQUEST_RETRY_SLEEP_S`.
- CLI retry knobs for throttling are `CLI_MAX_RETRIES` and `CLI_RETRY_SLEEP_S`.
- FUSE mount readiness knobs are `MOUNT_READY_TIMEOUT_S`, `MOUNT_READY_INTERVAL_S`, and `FUSE_MOUNT_ROOT`.
- CLI source knobs are `CLI_SOURCE` (`build` or `official`), `CLI_RELEASE_BASE_URL`, and optional `CLI_RELEASE_VERSION`.
- API upload-limit boundary check is enabled by default via `RUN_UPLOAD_LIMIT_BOUNDARY=1`.
- `UPLOAD_LIMIT_BYTES` controls the boundary value checked by API e2e (default `10737418240`).
- CLI upload-limit boundary check is enabled by default via `RUN_CLI_UPLOAD_LIMIT_BOUNDARY=1`.
- `CLI_UPLOAD_LIMIT_BYTES` controls the boundary value checked by CLI e2e (default `10737418240`).
- `fuse-smoke-test.sh` will `SKIP` when host prerequisites are missing (for example no `/dev/fuse`) or when the server does not support mount precheck `ls /`.
