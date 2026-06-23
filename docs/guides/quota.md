# drive9 quota guide

Last verified: 2026-06-23.

This guide shows how to query and update Drive9 tenant quota from the CLI and
HTTP API.

## What quota tracks

Drive9 exposes one user-settable tenant quota setting:

| Field | Meaning |
| --- | --- |
| `max_storage_size` | Maximum confirmed plus reserved file storage size, in Mi. |

Quota responses also include usage counters:

| Field | Meaning |
| --- | --- |
| `storage_bytes` | Confirmed file storage bytes. |
| `reserved_bytes` | Bytes reserved by active uploads. |
| `media_file_count` | Confirmed image/audio file count. |
| `monthly_cost_mc` | Current monthly LLM cost in millicents. |

## Permissions and supported modes

Querying quota is supported in two ways:

- Use a Drive9 owner API key to query the active tenant.
- Use TiDB Cloud API keys plus a Drive9 tenant id to query a
  `tidb_cloud_native` tenant.

Updating quota is stricter:

- Only `tidb_cloud_native` tenants support quota updates through this API.
- Updates require TiDB Cloud API keys and a Drive9 tenant id.
- A Drive9 tenant API key cannot authorize quota updates for its own tenant.
- Other tenant providers do not support quota updates through this API and use
  their configured defaults.

When a quota update succeeds, Drive9 verifies the TiDB Cloud credentials by
updating TiDB Cloud cluster labels, then writes the quota config in the Drive9
meta store.

## CLI

Use `drive9 quota get` without `--tenant-id` to query the tenant from the active
Drive9 context or `DRIVE9_API_KEY`.

```bash
export DRIVE9_SERVER=https://drive9.example.com
export DRIVE9_API_KEY=<owner-api-key>

drive9 quota get
```

Example output:

```text
tenant_id: tnt_abc123
provider: tidb_cloud_native
status: active
supports_update: true
config: max_storage_size=102400Mi
usage: storage_bytes=1048576 reserved_bytes=0 media_file_count=12 monthly_cost_mc=350
```

Use `--json` for script-friendly output.

```bash
drive9 quota get --json
```

Use TiDB Cloud credentials and `--tenant-id` when you need to query a
TiDBCloud mode tenant without a Drive9 owner API key.

```bash
drive9 quota get \
  --region-code aws-ap-southeast-1 \
  --tenant-id tnt_abc123 \
  --tidbcloud-public-key <tidbcloud-public-key> \
  --tidbcloud-private-key <tidbcloud-private-key> \
  --json
```

You may provide TiDB Cloud keys through environment variables:

```bash
export DRIVE9_PUBLIC_KEY=<tidbcloud-public-key>
export DRIVE9_PRIVATE_KEY=<tidbcloud-private-key>

drive9 quota get --region-code aws-ap-southeast-1 --tenant-id tnt_abc123
```

Environment TiDB Cloud keys do not change plain `drive9 quota get` behavior.
Credential-based quota query is selected only when `--tenant-id` is present.

Set quota with `drive9 quota set`. Only TiDBCloud mode supports quota set.
Pass `--max-storage-size` in Mi.

```bash
drive9 quota set \
  --region-code aws-ap-southeast-1 \
  --tenant-id tnt_abc123 \
  --tidbcloud-public-key <tidbcloud-public-key> \
  --tidbcloud-private-key <tidbcloud-private-key> \
  --max-storage-size 102400
```

Use `--server` instead of `--region-code` when targeting a known Drive9 server
URL directly. If both are present, `--server` wins.

Validation rules:

- `--max-storage-size` must be positive.

## HTTP API

All quota endpoints return the same response shape:

```json
{
  "tenant_id": "tnt_abc123",
  "provider": "tidb_cloud_native",
  "status": "active",
  "supports_update": true,
  "config": {
    "max_storage_size": 102400
  },
  "usage": {
    "storage_bytes": 1048576,
    "reserved_bytes": 0,
    "media_file_count": 12,
    "monthly_cost_mc": 350
  }
}
```

### GET /v1/quota

Query quota for the tenant identified by the Drive9 owner API key.

```bash
curl -sS \
  -H "Authorization: Bearer <owner-api-key>" \
  https://drive9.example.com/v1/quota
```

Use this endpoint for owner-key self-service reads. Do not use it for
credential-based tenant lookup.

### POST /v1/quota/query

Query quota for a `tidb_cloud_native` tenant using TiDB Cloud credentials.

```bash
curl -sS \
  -H "Content-Type: application/json" \
  -X POST https://drive9.example.com/v1/quota/query \
  -d '{
    "tenant_id": "tnt_abc123",
    "public_key": "<tidbcloud-public-key>",
    "private_key": "<tidbcloud-private-key>"
  }'
```

The server verifies that the TiDB Cloud API key can access the tenant's cluster
before returning quota.

### POST /v1/quota

Set quota for a `tidb_cloud_native` tenant using TiDB Cloud credentials.

```bash
curl -sS \
  -H "Content-Type: application/json" \
  -X POST https://drive9.example.com/v1/quota \
  -d '{
    "tenant_id": "tnt_abc123",
    "public_key": "<tidbcloud-public-key>",
    "private_key": "<tidbcloud-private-key>",
    "max_storage_size": 102400
  }'
```

`max_storage_size` is required and must be a positive Mi value.

## Error responses

The quota API returns JSON errors through the standard server error shape.

| Status | When it happens |
| --- | --- |
| `400 Bad Request` | Invalid JSON, missing `tenant_id`, missing or partial TiDB Cloud credentials, missing `max_storage_size` in a set request, or an invalid quota value. |
| `401 Unauthorized` | `GET /v1/quota` is missing a valid Drive9 owner API key. |
| `403 Forbidden` | TiDB Cloud returns unauthorized or forbidden for the supplied API key. The message is `no permission to query quota with TiDB Cloud API key` or `no permission to update quota with TiDB Cloud API key`. |
| `404 Not Found` | The Drive9 tenant does not exist, quota is not enabled on this server, or TiDB Cloud cannot find the backend cluster. For the backend-cluster case, check the TiDB Cloud starter/native cluster status. |
| `409 Conflict` | The tenant provider is not `tidb_cloud_native`. |
| `502 Bad Gateway` | TiDB Cloud returned another upstream error while querying or updating quota labels. |

## Notes for operators

- Server-side quota reads high-churn usage counters from the central meta store.
  Low-churn quota config is cached per tenant and refreshed by version polling.
- In cloud-native mode, small write quota mutations are recorded in a
  tenant-local quota outbox and applied asynchronously to central quota state.
- The write path includes pending outbox deltas in quota admission so
  multi-server deployments do not over-admit against stale central usage.
