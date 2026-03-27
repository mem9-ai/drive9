# dat9 E2E tests

Live end-to-end scripts for validating deployed `dat9-server` behavior.

## Prerequisites

- A running server endpoint (`DAT9_BASE`)
- `jq` installed
- Bash 4+

## Scripts

| Script | What it validates |
|--------|--------------------|
| `api-smoke-test.sh` | Fresh provisioning, status polling, nested directories, multi-file CRUD-style operations |
| `api-smoke-test-existing-key.sh` | Existing API key status/list checks |
| `cli-smoke-test.sh` | End-to-end CLI workflow including large multipart `fs cp` upload/download |

## Run

```bash
DEPLOY=https://<api-endpoint>

DAT9_BASE=$DEPLOY bash e2e/api-smoke-test.sh

DAT9_BASE=$DEPLOY DAT9_API_KEY=dat9_xxx bash e2e/api-smoke-test-existing-key.sh

DAT9_BASE=$DEPLOY bash e2e/cli-smoke-test.sh
```

## Dev endpoint

Current dev deployment endpoint:

```bash
export DAT9_BASE="https://xkopoerih4.execute-api.ap-southeast-1.amazonaws.com"
```

## Notes

- `api-smoke-test.sh` expects `POST /v1/provision` to return only `api_key` and `status`.
- Tenant readiness is checked through `GET /v1/status`.
- File operations use `/v1/fs/*` and include nested directory coverage.
- Large-file scenario is enabled by default (`RUN_LARGE_FILE=1`) and runs a multipart upload using checksum-bound presigned parts.
- You can tune size with `LARGE_FILE_MB` (default `100`).
- CLI smoke large-file size can be tuned with `CLI_LARGE_FILE_MB` (default `100`).
- API batch small-file coverage can be tuned with `BATCH_SMALL_FILE_COUNT` (default `10`).
- CLI batch small-file coverage can be tuned with `CLI_BATCH_SMALL_FILE_COUNT` (default `10`).
