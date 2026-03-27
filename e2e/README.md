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

## Run

```bash
DEPLOY=https://<api-endpoint>

DAT9_BASE=$DEPLOY bash e2e/api-smoke-test.sh

DAT9_BASE=$DEPLOY DAT9_API_KEY=dat9_xxx bash e2e/api-smoke-test-existing-key.sh
```

## Dev endpoint

Current dev deployment endpoint:

```bash
export DAT9_BASE="https://xkopoerih4.execute-api.ap-southeast-1.amazonaws.com"
```

## Notes

- `api-smoke-test.sh` expects `POST /v1/create` to return `id`, `name`, `api_key`, and `status`.
- Tenant readiness is checked through `GET /v1/status`, which returns `id`, `name`, and `status`.
- File operations use `/v1/fs/*` and include nested directory coverage.
