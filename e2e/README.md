# dat9 E2E tests

Live end-to-end scripts for validating deployed `dat9-server` behavior.

## Prerequisites

- A running server endpoint (`DAT9_BASE`)
- `jq` installed
- Bash 4+

## Scripts

| Script | What it validates |
|--------|--------------------|
| `api-smoke-test.sh` | Fresh provisioning, status polling, nested+batch file ops, grep/find checks, semantic text recall, image-associated recall, sql checks, large multipart upload+download |
| `api-smoke-test-existing-key.sh` | Existing API key status/list checks |
| `cli-smoke-test.sh` | End-to-end CLI workflow including `fs grep`/`fs find`, semantic/image-associated recall checks, image `fs cp`+`fs find`, and large multipart `fs cp` upload/download |
| `fuse-smoke-test.sh` | FUSE mount lifecycle, file/dir/rename/stat semantics, cross-channel consistency, read-only and error-path checks |
| `smoke-all.sh` | Runs API + CLI + FUSE smoke scripts in sequence with aggregated pass/fail |

## Run

```bash
DEPLOY=https://<api-endpoint>

DAT9_BASE=$DEPLOY bash e2e/api-smoke-test.sh

DAT9_BASE=$DEPLOY DAT9_API_KEY=dat9_xxx bash e2e/api-smoke-test-existing-key.sh

DAT9_BASE=$DEPLOY bash e2e/cli-smoke-test.sh

DAT9_BASE=$DEPLOY bash e2e/fuse-smoke-test.sh

DAT9_BASE=$DEPLOY bash e2e/smoke-all.sh
```

## Deployment Endpoints

### Dev

```bash
export DAT9_BASE="https://xkopoerih4.execute-api.ap-southeast-1.amazonaws.com"
```

### Prod

```bash
export DAT9_BASE="https://4w9z8cd9b7.execute-api.ap-southeast-1.amazonaws.com"
```

## Notes

- `api-smoke-test.sh` expects `POST /v1/provision` to return only `api_key` and `status`.
- Tenant readiness is checked through `GET /v1/status`.
- File operations use `/v1/fs/*` and include nested directory coverage.
- Semantic recall polling knobs for API smoke are `SEMANTIC_TIMEOUT_S` and `SEMANTIC_INTERVAL_S`.
- Semantic recall polling knobs for CLI smoke are `CLI_SEMANTIC_TIMEOUT_S` and `CLI_SEMANTIC_INTERVAL_S`.
- Large-file scenario is enabled by default (`RUN_LARGE_FILE=1`) and runs a multipart upload using checksum-bound presigned parts.
- You can tune size with `LARGE_FILE_MB` (default `100`).
- CLI smoke large-file size can be tuned with `CLI_LARGE_FILE_MB` (default `100`).
- API batch small-file coverage can be tuned with `BATCH_SMALL_FILE_COUNT` (default `10`).
- CLI batch small-file coverage can be tuned with `CLI_BATCH_SMALL_FILE_COUNT` (default `10`).
- API retry knobs for throttling are `REQUEST_MAX_RETRIES` and `REQUEST_RETRY_SLEEP_S`.
- CLI retry knobs for throttling are `CLI_MAX_RETRIES` and `CLI_RETRY_SLEEP_S`.
- FUSE mount readiness knobs are `MOUNT_READY_TIMEOUT_S`, `MOUNT_READY_INTERVAL_S`, and `FUSE_MOUNT_ROOT`.
- API upload-limit boundary check is enabled by default via `RUN_UPLOAD_LIMIT_BOUNDARY=1`.
- `UPLOAD_LIMIT_BYTES` controls the boundary value checked by API e2e (default `1073741824`).
- CLI upload-limit boundary check is enabled by default via `RUN_CLI_UPLOAD_LIMIT_BOUNDARY=1`.
- `CLI_UPLOAD_LIMIT_BYTES` controls the boundary value checked by CLI e2e (default `1073741824`).
- `fuse-smoke-test.sh` will `SKIP` when host prerequisites are missing (for example no `/dev/fuse`) or when the server does not support mount precheck `stat /`.
