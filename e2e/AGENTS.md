---
title: e2e - Live end-to-end scripts
---

## Overview

This directory contains live end-to-end tests for deployed dat9-server instances.
These scripts are integration probes (not unit tests) and call real HTTP endpoints.

## Quick start

```bash
DEPLOY=https://<your-api-gateway-or-server>

# Full smoke (provision -> status poll -> nested dirs -> file ops)
DAT9_BASE=$DEPLOY bash e2e/api-smoke-test.sh

# Existing key regression
DAT9_BASE=$DEPLOY DAT9_API_KEY=dat9_xxx bash e2e/api-smoke-test-existing-key.sh

# CLI smoke (provision + dat9 fs workflows + large file cp)
DAT9_BASE=$DEPLOY bash e2e/cli-smoke-test.sh
```

## Dev endpoint

Current shared dev deployment:

```bash
export DAT9_BASE="https://xkopoerih4.execute-api.ap-southeast-1.amazonaws.com"
```

Use this value unless the environment owner announces a new endpoint.

## Coverage

### `api-smoke-test.sh`

1. `POST /v1/provision` returns `202` with only `api_key` + `status`
2. `GET /v1/status` polled until `active`
3. `GET /v1/fs/?list` returns `entries[]`
4. Nested `mkdir` (`/team/...`) across multi-level paths
5. Multi-file `PUT` + `GET` content verification
6. Batch small-file writes (`N` files) + list count + sample reads
7. Search checks (`GET ?grep=...`, `GET ?find=...`)
8. Image upload (`.png`) + image query check (`GET ?find=&name=*.png`)
9. SQL sanity check (`POST /v1/sql`)
10. `copy`, `rename`, `delete`
11. Final `list` verifies expected structure after mutations
12. Large multipart upload (`PUT` plan + presigned part uploads + complete + download checksum)
13. Upload-limit boundary (`1GiB` initiate accepted, `1GiB+1` rejected)

### `api-smoke-test-existing-key.sh`

1. Existing API key auth on `GET /v1/status`
2. Optional poll from `provisioning` to `active`
3. `GET /v1/fs/?list` baseline read check

### `cli-smoke-test.sh`

1. Provision + readiness polling
2. Build local `dat9` CLI binary
3. CLI small-file flow (`cp`, `ls`, `cat`, `mv`, `rm`)
4. CLI batch small-file flow (`cp` many files + dir list count + stat + sample reads)
5. CLI search flow (`fs grep`, `fs find`)
6. CLI image flow (`fs cp` png + `fs find -name "*.png"`)
7. CLI large-file flow (`cp` upload multipart + `cp` download + checksum verification)
8. CLI upload-limit boundary (`1GiB` initiate accepted, `1GiB+1` rejected)

## Environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `DAT9_BASE` | `http://127.0.0.1:9009` | all scripts |
| `DAT9_API_KEY` | - | `api-smoke-test-existing-key.sh` |
| `POLL_TIMEOUT_S` | `120` (smoke), `60` (existing-key) | polling scripts |
| `POLL_INTERVAL_S` | `5` | polling scripts |
| `RUN_LARGE_FILE` | `1` | `api-smoke-test.sh` |
| `LARGE_FILE_MB` | `100` | `api-smoke-test.sh` |
| `BATCH_SMALL_FILE_COUNT` | `10` | `api-smoke-test.sh` |
| `REQUEST_MAX_RETRIES` | `8` | `api-smoke-test.sh` |
| `REQUEST_RETRY_SLEEP_S` | `2` | `api-smoke-test.sh` |
| `RUN_UPLOAD_LIMIT_BOUNDARY` | `1` | `api-smoke-test.sh` |
| `UPLOAD_LIMIT_BYTES` | `1073741824` | `api-smoke-test.sh` |
| `CLI_LARGE_FILE_MB` | `100` | `cli-smoke-test.sh` |
| `CLI_BATCH_SMALL_FILE_COUNT` | `10` | `cli-smoke-test.sh` |
| `CLI_MAX_RETRIES` | `8` | `cli-smoke-test.sh` |
| `CLI_RETRY_SLEEP_S` | `2` | `cli-smoke-test.sh` |
| `RUN_CLI_UPLOAD_LIMIT_BOUNDARY` | `1` | `cli-smoke-test.sh` |
| `CLI_UPLOAD_LIMIT_BYTES` | `1073741824` | `cli-smoke-test.sh` |

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
