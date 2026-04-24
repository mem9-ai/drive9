# Server-Mode Quota E2E Test

This directory contains an end-to-end test suite for drive9's **server-mode quota**
(central quota enforcement via `DRIVE9_QUOTA_SOURCE=server`).

## Architecture

```
┌─────────────────────────────────────────┐
│  Docker: meta-db (MySQL 8.0)            │
│  Port 3306 / DB: drive9_meta            │
│  Tables: tenant_quota_usage,            │
│          quota_mutation_log, ...        │
└─────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────┐
│  Docker: tenant-db (TiDB)               │
│  Port 4000 / DB: drive9_local           │
│  Tables: files, file_nodes, uploads, ...│
└─────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────┐
│  bin/drive9-server-local                │
│  - DRIVE9_QUOTA_SOURCE=server           │
│  - DRIVE9_LOCAL_META_DSN set            │
│  - MutationReplayWorker running         │
│  - ExpirySweepWorker running            │
└─────────────────────────────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `docker-compose.quota.yml` | Spins up meta-db (MySQL) and tenant-db (TiDB) |
| `quota-server-e2e.sh` | Full E2E test script |
| `QUOTA_E2E.md` | This document |

## Prerequisites

- Docker & docker-compose
- Go 1.25+
- curl, jq
- macOS or Linux (bash)

## How to run

```bash
# From repo root
bash e2e/server-quota/quota-server-e2e.sh
```

The script will:
1. Start two Docker containers
2. Build `drive9-server-local` with the `DRIVE9_LOCAL_META_DSN` patch
3. Start the server in **server quota mode**
4. Run 10 test cases
5. Tear down containers (unless `KEEP_CONTAINERS=1`)

## Test Coverage

| # | Scenario | What it validates |
|---|----------|-------------------|
| 1 | Inline write within quota (512 KiB / 1 MiB limit) | `storage_bytes` incremented |
| 2 | Inline write exceeds quota (2 MiB) | HTTP 507, write rejected |
| 3 | Central quota counters accuracy | `tenant_quota_usage` matches actual file sizes |
| 4 | Overwrite with smaller file | Negative delta reduces `storage_bytes` |
| 5 | Image upload increments media count | `media_file_count` incremented for `image/*` |
| 6 | Multipart upload initiate within quota | `reserved_bytes` incremented, HTTP 202 |
| 7 | Abort upload releases reservation | `reserved_bytes` decremented back to 0 |
| 8 | Multipart upload initiate exceeds quota | HTTP 507 at initiate time |
| 9 | Mutation log records operations | `quota_mutation_log` has entries for all mutations |
| 10 | Backfill quota counters | Simulated backfill produces correct aggregates |

## Key implementation changes

### `cmd/drive9-server-local/main.go`

Added support for `DRIVE9_LOCAL_META_DSN`:

- Opens a `meta.Store` connection to the control-plane DB
- Wires `tenant.NewMetaQuotaAdapter(metaStore)` into the backend
- Starts `MutationReplayWorker` and `ExpirySweepWorker`
- Calls `EnsureQuotaUsageRow` to bootstrap the usage counter row

This allows `drive9-server-local` — previously single-DB only — to exercise
the full server-mode quota stack without requiring the multi-tenant
`drive9-server` entrypoint.

## Environment variables (test script sets these)

| Variable | Value in test | Meaning |
|----------|---------------|---------|
| `DRIVE9_QUOTA_SOURCE` | `server` | Read quota state from central DB |
| `DRIVE9_LOCAL_META_DSN` | `root:root@tcp(127.0.0.1:3306)/drive9_meta?parseTime=true` | Control-plane DB |
| `DRIVE9_LOCAL_DSN` | `root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true` | Tenant DB (TiDB) |
| `DRIVE9_MAX_TENANT_STORAGE_BYTES` | `1048576` (1 MiB) | Small limit for fast boundary testing |
| `DRIVE9_MAX_UPLOAD_BYTES` | `10485760` (10 MiB) | Per-upload size limit |
| `DRIVE9_MAX_MEDIA_LLM_FILES` | `2` | Media file soft-quota limit |

## Debugging a failed run

```bash
# Keep containers alive after test
KEEP_CONTAINERS=1 bash e2e/server-quota/quota-server-e2e.sh

# Inspect meta DB
docker compose -f e2e/server-quota/docker-compose.quota.yml exec meta-db \
  mysql -uroot -proot drive9_meta \
  -e "SELECT * FROM tenant_quota_usage; SELECT * FROM quota_mutation_log;"

# Inspect tenant DB
docker compose -f e2e/server-quota/docker-compose.quota.yml exec tenant-db \
  mysql -uroot -P4000 drive9_local \
  -e "SELECT file_id, path, size_bytes, content_type, status FROM files;"

# View server logs
# (the script prints to stdout; capture with `bash e2e/server-quota/quota-server-e2e.sh 2>&1 | tee quota.log`)
```
