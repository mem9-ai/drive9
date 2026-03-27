---
title: e2e - Live end-to-end scripts
---

## Overview

This directory contains live end-to-end tests for deployed dat9-server instances.
These scripts are integration probes (not unit tests) and call real HTTP endpoints.

## Quick start

```bash
DEPLOY=https://<your-api-gateway-or-server>

# Full smoke (create -> status poll -> nested dirs -> file ops)
DAT9_BASE=$DEPLOY bash e2e/api-smoke-test.sh

# Existing key regression
DAT9_BASE=$DEPLOY DAT9_API_KEY=dat9_xxx bash e2e/api-smoke-test-existing-key.sh
```

## Dev endpoint

Current shared dev deployment:

```bash
export DAT9_BASE="https://xkopoerih4.execute-api.ap-southeast-1.amazonaws.com"
```

Use this value unless the environment owner announces a new endpoint.

## Coverage

### `api-smoke-test.sh`

1. `POST /v1/create` returns `202` with `id` + `name` + `api_key` + `status`
2. `GET /v1/status` polled until `active` and includes `id` + `name` + `status`
3. `GET /v1/fs/?list` returns `entries[]`
4. Nested `mkdir` (`/team/...`) across multi-level paths
5. Multi-file `PUT` + `GET` content verification
6. `copy`, `rename`, `delete`
7. Final `list` verifies expected structure after mutations

### `api-smoke-test-existing-key.sh`

1. Existing API key auth on `GET /v1/status`
2. Optional poll from `provisioning` to `active`
3. `GET /v1/fs/?list` baseline read check

## Environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `DAT9_BASE` | `http://127.0.0.1:9009` | all scripts |
| `DAT9_API_KEY` | - | `api-smoke-test-existing-key.sh` |
| `POLL_TIMEOUT_S` | `120` (smoke), `60` (existing-key) | polling scripts |
| `POLL_INTERVAL_S` | `5` | polling scripts |

## Conventions

- Each smoke run provisions a fresh tenant and uses timestamped paths.
- Scripts require `jq`.
- API surface expected by these scripts:
  - `POST /v1/create`
  - `GET /v1/status`
  - `/v1/fs/*` for file operations

## Anti-patterns

- Do not hardcode long-lived secrets in scripts.
- Do not use these scripts as unit-test substitutes.
- Do not change API paths casually; scripts serve as executable API docs.
