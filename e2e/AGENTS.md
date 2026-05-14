---
title: e2e - Live end-to-end scripts
---

## Overview

This directory contains live end-to-end tests for deployed drive9-server instances.
These scripts are integration probes (not unit tests) and call real HTTP endpoints.

## Run

Use a hosted deployment by default. For local development on this machine, use
`drive9-server-local` instead.

### Hosted endpoints

#### Deployment endpoints

Current shared dev deployment:

```bash
# Dev
export DRIVE9_BASE="http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com"

# Prod
export DRIVE9_BASE="https://api.drive9.ai"
```

Use the dev value unless the environment owner announces a new endpoint.

#### Run smoke scripts

```bash
# Full smoke (provision -> status poll -> nested dirs -> file ops)
bash e2e/api-smoke-test.sh

# Existing key regression
DRIVE9_API_KEY=drive9_xxx bash e2e/api-smoke-test-existing-key.sh

# CLI smoke (provision + drive9 fs workflows + large file cp)
bash e2e/cli-smoke-test.sh

# Journal smoke (provision + journal create/append/find/verify)
bash e2e/journal-smoke-test.sh

# FUSE smoke (mount + bidirectional filesystem checks)
bash e2e/fuse-smoke-test.sh

# POSIX permission smoke (API/CLI/FUSE chmod and mkdir mode)
bash e2e/posix-permission-smoke-test.sh

# Run all smoke scripts in sequence
bash e2e/smoke-all.sh
```

### Local via `drive9-server-local`

When the task is specifically about local validation on this machine, prefer
`drive9-server-local` over hosted endpoints.

`scripts/drive9-server-local-env.sh` is the source of truth for local default
environment values.

### Prerequisites

- Choose one of the following local validation setups before startup:
- Use TiDB Starter with auto-embedding enabled. Set `DRIVE9_LOCAL_DSN` to the
  Starter instance DSN. This is the easier path for semantic smoke coverage
  because it does not require a local Ollama deployment.
- Use a local TiDB/MySQL instance together with a local embedding service.
  Create the database referenced by `DRIVE9_LOCAL_DSN` before startup, then
  make sure the embedding endpoint is available. The default env script expects
  Ollama at `http://127.0.0.1:11434` with model `bge-m3`.

### Terminal 1: start `drive9-server-local`

```bash
export DRIVE9_REPO_ROOT=/path/to/drive9
cd "$DRIVE9_REPO_ROOT"

export DRIVE9_LOCAL_DSN='root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true'   # optional if you use the default local DSN; replace with your TiDB Starter DSN when applicable
export DRIVE9_LOCAL_INIT_SCHEMA=true   # only for a fresh/disposable database
make run-server-local
```

`make run-server-local` already sources `scripts/drive9-server-local-env.sh` and
stays attached to the foreground. Export any overrides before invoking it, then
run the smoke scripts from a second terminal after the server is healthy.

### Terminal 2: verify health and run E2E

```bash
export DRIVE9_REPO_ROOT=/path/to/drive9
cd "$DRIVE9_REPO_ROOT"

export DRIVE9_BASE=http://127.0.0.1:9009

curl "$DRIVE9_BASE/healthz"

bash e2e/api-smoke-test.sh
DRIVE9_API_KEY='local-dev-key' bash e2e/api-smoke-test-existing-key.sh
bash e2e/cli-smoke-test.sh
bash e2e/fuse-smoke-test.sh
bash e2e/smoke-all.sh
```

Use `http://127.0.0.1:9009` as `DRIVE9_BASE` once `healthz` returns
`{"status":"ok"}`.

If you overrode `DRIVE9_LOCAL_API_KEY` before starting `drive9-server-local`,
use the same value as `DRIVE9_API_KEY` here.

### Local-server-specific expectations

- `drive9-server-local` exposes a built-in single tenant key via
  `DRIVE9_LOCAL_API_KEY`; the default is `local-dev-key` when the env var is
  not overridden.
- `api-smoke-test-existing-key.sh` should use that built-in key instead of
  provisioning a new tenant.
- Upload-limit boundary failures (`507` on the `limit-1g.bin` initiate step)
  can be caused by stale multipart reservations in the tenant `uploads` table,
  not by current file-tree contents.
- If quota looks polluted, inspect and clear `INITIATED` / `UPLOADING` rows for
  the tenant before rerunning the smoke suite.

## Coverage

### `api-smoke-test.sh`

1. `POST /v1/provision` returns `202` with `tenant_id`, `api_key`, and `status`
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

15. Upload-limit boundary (`10GiB` initiate accepted, `10GiB+1` rejected)

### `api-smoke-test-existing-key.sh`

1. Existing API key auth on `GET /v1/status`
2. Optional poll from `provisioning` to `active`
3. `GET /v1/fs/?list` baseline read check

### `cli-smoke-test.sh`

1. Provision + readiness polling
2. Prepare `drive9` CLI binary (build local or download official release)
3. CLI fork flow (`ctx add`, `ctx fork`, fork readiness polling, fork-context file read/write, fork delete)
4. CLI small-file flow (`cp`, `ls`, `cat`, `mv`, `rm`)
5. CLI `cp` directory-target semantics (local->remote dir, remote->local dir, remote->remote dir all preserve source basename)
6. CLI batch small-file flow (`cp` many files + dir list count + stat + sample reads)
7. CLI search flow (`fs grep`, `fs find`)
8. CLI semantic and image-associated recall flow (`fs grep` paraphrase + image caption recall) with async polling
9. CLI image flow (`fs cp` jpg + `fs find -name "*.jpg"`)
10. CLI large-file flow (`cp` upload multipart + `cp` download + checksum verification)
11. CLI upload-limit boundary (`10GiB` initiate accepted, `10GiB+1` rejected)

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
- Optional release-gate knobs add small-repo git clone/status/log checks,
  durable `drive9 umount --timeout` remount visibility checks, and mount-log audit.

### `posix-permission-smoke-test.sh`

1. Provision + readiness polling
2. API permission semantics (`mkdir` default/explicit mode, `chmod` on file/directory, 404 on missing path, `?list` mode/hasMode fields)
3. Prepare `drive9` CLI binary (build local or download official release)
4. CLI `drive9 fs chmod` on file and directory with remote HEAD verification
5. FUSE mount + shell `chmod` on file and directory with remote/local stat parity
6. FUSE `mkdir -m` with remote/local stat parity
7. Platform-aware `stat` for macOS (`stat -f %Lp`) and Linux (`stat -c %a`)
8. Cleanup of remote permission test trees

### `fuse-release-gate.sh`

1. Runs `fuse-smoke-test.sh` with `FUSE_STRICT_PREREQS=1`
2. Enables small-repo git clone/status/log coverage
3. Enables durable `umount --timeout` followed by remount visibility checks
4. Enables mount-log audit and dumps mount logs on failure

### `smoke-all.sh`

1. Runs `api-smoke-test.sh`
2. Runs `cli-smoke-test.sh`
3. Runs `journal-smoke-test.sh`
4. Runs `fuse-smoke-test.sh`
5. Aggregates pass/fail at script level for quick regression checks

## Environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `DRIVE9_BASE` | `http://127.0.0.1:9009` | all scripts |
| `DRIVE9_IMAGE_FIXTURE_PATH` | `e2e/fixtures/cat03.jpg` | `api-smoke-test.sh`, `cli-smoke-test.sh` |
| `DRIVE9_API_KEY` | - | `api-smoke-test-existing-key.sh` |
| `DRIVE9_API_KEY` | - | `fuse-smoke-test.sh` (optional; skip provision when set) |
| `DRIVE9_API_KEY` | - | `posix-permission-smoke-test.sh` (optional; skip provision when set) |
| `POLL_TIMEOUT_S` | `120` (smoke), `60` (existing-key) | polling scripts |
| `POLL_INTERVAL_S` | `5` | polling scripts |
| `RUN_LARGE_FILE` | `1` | `api-smoke-test.sh` |
| `LARGE_FILE_MB` | `100` | `api-smoke-test.sh` |
| `BATCH_SMALL_FILE_COUNT` | `10` | `api-smoke-test.sh` |
| `REQUEST_MAX_RETRIES` | `8` | `api-smoke-test.sh` |
| `REQUEST_RETRY_SLEEP_S` | `2` | `api-smoke-test.sh` |
| `RUN_UPLOAD_LIMIT_BOUNDARY` | `1` | `api-smoke-test.sh` |
| `UPLOAD_LIMIT_BYTES` | `10737418240` | `api-smoke-test.sh` |
| `RUN_SEMANTIC_CHECKS` | `1` | `api-smoke-test.sh` |
| `SEMANTIC_TIMEOUT_S` | `90` | `api-smoke-test.sh` |
| `SEMANTIC_INTERVAL_S` | `3` | `api-smoke-test.sh` |
| `CLI_LARGE_FILE_MB` | `100` | `cli-smoke-test.sh` |
| `CLI_BATCH_SMALL_FILE_COUNT` | `10` | `cli-smoke-test.sh` |
| `CLI_MAX_RETRIES` | `8` | `cli-smoke-test.sh` |
| `CLI_RETRY_SLEEP_S` | `2` | `cli-smoke-test.sh` |
| `RUN_CLI_UPLOAD_LIMIT_BOUNDARY` | `1` | `cli-smoke-test.sh` |
| `CLI_UPLOAD_LIMIT_BYTES` | `10737418240` | `cli-smoke-test.sh` |
| `RUN_CLI_SEMANTIC_CHECKS` | `1` | `cli-smoke-test.sh` |
| `RUN_CLI_FORK_CHECKS` | `1` (auto-skip when `/v1/fork` is unavailable) | `cli-smoke-test.sh` |
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
| `FUSE_STRICT_PREREQS` | `0` (`1` in release gate) | `fuse-smoke-test.sh` |
| `FUSE_UMOUNT_TIMEOUT` | `60s` | `fuse-smoke-test.sh` |
| `RUN_FUSE_GIT_CLONE` | `0` (`1` in release gate) | `fuse-smoke-test.sh` |
| `FUSE_GIT_CLONE_URL` | `https://github.com/octocat/Hello-World.git` | `fuse-smoke-test.sh` |
| `FUSE_GIT_CLONE_TIMEOUT_S` | `180` | `fuse-smoke-test.sh` |
| `RUN_FUSE_UMOUNT_DURABLE` | `0` (`1` in release gate) | `fuse-smoke-test.sh` |
| `RUN_FUSE_LOG_AUDIT` | `0` (`1` in release gate) | `fuse-smoke-test.sh` |

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
