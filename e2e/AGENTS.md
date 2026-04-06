---
title: e2e - Live end-to-end scripts
---

## Overview

This directory contains live end-to-end tests for deployed drive9-server instances.
These scripts are integration probes (not unit tests) and call real HTTP endpoints.

## Quick start

```bash
DEPLOY=https://<your-api-gateway-or-server>

# Full smoke (provision -> status poll -> nested dirs -> file ops)
DRIVE9_BASE=$DEPLOY bash e2e/api-smoke-test.sh

# Existing key regression
DRIVE9_BASE=$DEPLOY DRIVE9_API_KEY=drive9_xxx bash e2e/api-smoke-test-existing-key.sh

# CLI smoke (provision + drive9 fs workflows + large file cp)
DRIVE9_BASE=$DEPLOY bash e2e/cli-smoke-test.sh

# FUSE smoke (mount + bidirectional filesystem checks)
DRIVE9_BASE=$DEPLOY bash e2e/fuse-smoke-test.sh

# Run all smoke scripts in sequence
DRIVE9_BASE=$DEPLOY bash e2e/smoke-all.sh
```

## Dev endpoint

Current shared dev deployment:

```bash
export DRIVE9_BASE="https://xkopoerih4.execute-api.ap-southeast-1.amazonaws.com"
```

Use this value unless the environment owner announces a new endpoint.

## Prod endpoint

```bash
export DRIVE9_BASE="https://api.drive9.ai"
```

## Coverage

### `api-smoke-test.sh`

1. `POST /v1/provision` returns `202` with only `api_key` + `status`
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
12. `copy`, `rename`, `delete`
13. Final `list` verifies expected structure after mutations
14. Large multipart upload (`POST /v1/uploads/initiate` + presigned part uploads + complete + download checksum)

15. Upload-limit boundary (`50GiB` initiate accepted, `50GiB+1` rejected)

### `api-smoke-test-existing-key.sh`

1. Existing API key auth on `GET /v1/status`
2. Optional poll from `provisioning` to `active`
3. `GET /v1/fs/?list` baseline read check

### `cli-smoke-test.sh`

1. Provision + readiness polling
2. Prepare `drive9` CLI binary (build local or download official release)
3. CLI small-file flow (`cp`, `ls`, `cat`, `mv`, `rm`)
4. CLI batch small-file flow (`cp` many files + dir list count + stat + sample reads)
5. CLI search flow (`fs grep`, `fs find`)
6. CLI semantic and image-associated recall flow (`fs grep` paraphrase + image caption recall) with async polling
7. CLI image flow (`fs cp` jpg + `fs find -name "*.jpg"`)
8. CLI large-file flow (`cp` upload multipart + `cp` download + checksum verification)
9. CLI upload-limit boundary (`50GiB` initiate accepted, `50GiB+1` rejected)

### `fuse-smoke-test.sh`

1. Provision + readiness polling
2. Prepare `drive9` CLI binary (build local or download official release)
3. Mount compatibility precheck for root `ls /`
4. RW mount lifecycle (`drive9 mount`, `drive9 umount`)
5. File semantics (`create`, `read`, `overwrite`, `append`, `truncate`, `unlink`)
6. Directory semantics (`mkdir`, nested paths, `readdir`, empty/non-empty `rmdir`)
7. Rename semantics (file + directory rename consistency)
8. Attribute semantics (`size`, `mtime` monotonicity, remote stat parity)
9. Cross-channel consistency (CLI write visible in mount; mount write visible via CLI)
10. Mounted large file boundary check (8MB write + remote checksum parity)
11. Read-only mount behavior (`--read-only` blocks writes/deletes, allows reads)
12. Error semantics (missing path reads/deletes and duplicate mkdir failures)
13. Linux prerequisite guardrails (`fusermount`, `/dev/fuse`) with skip behavior when unavailable

Notes:
- The script prechecks root `ls /` reachability before mount behavior checks.

### `smoke-all.sh`

1. Runs `api-smoke-test.sh`
2. Runs `cli-smoke-test.sh`
3. Runs `fuse-smoke-test.sh`
4. Aggregates pass/fail at script level for quick regression checks

## Environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `DRIVE9_BASE` | `http://127.0.0.1:9009` | all scripts |
| `DRIVE9_IMAGE_FIXTURE_PATH` | `e2e/fixtures/cat03.jpg` | `api-smoke-test.sh`, `cli-smoke-test.sh` |
| `DRIVE9_API_KEY` | - | `api-smoke-test-existing-key.sh` |
| `DRIVE9_API_KEY` | - | `fuse-smoke-test.sh` (optional; skip provision when set) |
| `POLL_TIMEOUT_S` | `120` (smoke), `60` (existing-key) | polling scripts |
| `POLL_INTERVAL_S` | `5` | polling scripts |
| `RUN_LARGE_FILE` | `1` | `api-smoke-test.sh` |
| `LARGE_FILE_MB` | `100` | `api-smoke-test.sh` |
| `BATCH_SMALL_FILE_COUNT` | `10` | `api-smoke-test.sh` |
| `REQUEST_MAX_RETRIES` | `8` | `api-smoke-test.sh` |
| `REQUEST_RETRY_SLEEP_S` | `2` | `api-smoke-test.sh` |
| `RUN_UPLOAD_LIMIT_BOUNDARY` | `1` | `api-smoke-test.sh` |
| `UPLOAD_LIMIT_BYTES` | `53687091200` | `api-smoke-test.sh` |
| `SEMANTIC_TIMEOUT_S` | `90` | `api-smoke-test.sh` |
| `SEMANTIC_INTERVAL_S` | `3` | `api-smoke-test.sh` |
| `CLI_LARGE_FILE_MB` | `100` | `cli-smoke-test.sh` |
| `CLI_BATCH_SMALL_FILE_COUNT` | `10` | `cli-smoke-test.sh` |
| `CLI_MAX_RETRIES` | `8` | `cli-smoke-test.sh` |
| `CLI_RETRY_SLEEP_S` | `2` | `cli-smoke-test.sh` |
| `RUN_CLI_UPLOAD_LIMIT_BOUNDARY` | `1` | `cli-smoke-test.sh` |
| `CLI_UPLOAD_LIMIT_BYTES` | `53687091200` | `cli-smoke-test.sh` |
| `CLI_SEMANTIC_TIMEOUT_S` | `90` | `cli-smoke-test.sh` |
| `CLI_SEMANTIC_INTERVAL_S` | `3` | `cli-smoke-test.sh` |
| `CLI_SOURCE` | `build` (`build` or `official`) | `cli-smoke-test.sh`, `fuse-smoke-test.sh` |
| `CLI_RELEASE_BASE_URL` | `https://drive9.ai/releases` | `cli-smoke-test.sh`, `fuse-smoke-test.sh` |
| `CLI_RELEASE_VERSION` | *(latest)* | `cli-smoke-test.sh`, `fuse-smoke-test.sh` |
| `MOUNT_READY_TIMEOUT_S` | `20` | `fuse-smoke-test.sh` |
| `MOUNT_READY_INTERVAL_S` | `1` | `fuse-smoke-test.sh` |
| `FUSE_MOUNT_ROOT` | `/tmp` | `fuse-smoke-test.sh` |
| `CLI_MAX_RETRIES` | `8` | `fuse-smoke-test.sh` |
| `CLI_RETRY_SLEEP_S` | `2` | `fuse-smoke-test.sh` |

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
