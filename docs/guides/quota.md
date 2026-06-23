# drive9 quota guide

Last verified: 2026-06-23.

This guide shows how to query and update Drive9 tenant quota from the CLI and
HTTP API.

## What quota tracks

Drive9 exposes these user-settable quota settings:

| Field | Meaning |
| --- | --- |
| `max_storage_size` | Maximum confirmed plus reserved file storage size, in Mi. Stored in Drive9. |
| `tidbcloud_spending_limit` | TiDB Cloud Cluster Spending Limit. The value is passed through to TiDB Cloud, read from and written to the TiDB Cloud cluster, and not stored in Drive9. See the [TiDB Cloud Spending Limit guide](https://docs.pingcap.com/tidbcloud/manage-serverless-spend-limit). |

Quota responses also include usage counters:

| Field | Meaning |
| --- | --- |
| `storage_bytes` | Confirmed file storage bytes. |
| `reserved_bytes` | Bytes reserved by active uploads. |
| `media_file_count` | Confirmed image/audio file count. |
| `monthly_cost_mc` | Current monthly LLM cost in millicents. |

## Permissions and supported modes

Quota query and update both require TiDB Cloud API keys plus a Drive9 tenant id.
A Drive9 tenant API key is not accepted for quota query or update.
Server-side default TiDB Cloud credentials configured for tenant provision or
deprovision are not used as a fallback by quota query or update. Callers must
supply TiDB Cloud credentials on each quota request; those credentials may be
the same keys as the server defaults when passed explicitly.

Only `tidb_cloud_native` tenants support quota update through this API. Other
tenant providers use their configured defaults.

When a quota update succeeds, Drive9 verifies the TiDB Cloud credentials by
reading the TiDB Cloud cluster. If `tidbcloud_spending_limit` is present, it
patches the TiDB Cloud cluster Spending Limit next. If `max_storage_size` is
present, Drive9 then writes it to the Drive9 meta store. After those updates
succeed, Drive9 updates TiDB Cloud cluster labels to record the successful quota
update.

## CLI

Use TiDB Cloud credentials and `--tenant-id` to query a TiDBCloud mode tenant.

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

Example output:

```text
tenant: tnt_abc123
provider: tidb_cloud_native
status: active
supports_update: true
config: max_storage_size=102400Mi tidbcloud_spending_limit=10000
usage: storage_bytes=1048576 reserved_bytes=0 media_file_count=12 monthly_cost_mc=350
```

Set quota with `drive9 quota set`. Only TiDBCloud mode supports quota set. Pass
at least one of `--max-storage-size` or `--tidbcloud-spending-limit`.

```bash
drive9 quota set \
  --region-code aws-ap-southeast-1 \
  --tenant-id tnt_abc123 \
  --tidbcloud-public-key <tidbcloud-public-key> \
  --tidbcloud-private-key <tidbcloud-private-key> \
  --max-storage-size 102400 \
  --tidbcloud-spending-limit 10000
```

Use `--server` instead of `--region-code` when targeting a known Drive9 server
URL directly. If both are present, `--server` wins.

Validation rules:

- `--max-storage-size` must be positive.
- `--tidbcloud-spending-limit` must be a non-negative 32-bit integer. Drive9
  passes `0` through to TiDB Cloud; TiDB Cloud may reject it.

## HTTP API

All quota endpoints return the same response shape:

```json
{
  "tenant_id": "tnt_abc123",
  "provider": "tidb_cloud_native",
  "status": "active",
  "supports_update": true,
  "config": {
    "max_storage_size": 102400,
    "tidbcloud_spending_limit": 10000
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

Query quota for a `tidb_cloud_native` tenant using TiDB Cloud credentials.

```bash
curl -sS \
  -H "X-TiDBCloud-Public-Key: <tidbcloud-public-key>" \
  -H "X-TiDBCloud-Private-Key: <tidbcloud-private-key>" \
  "https://drive9.example.com/v1/quota?tenant_id=tnt_abc123"
```

The server gets the TiDB Cloud cluster before returning quota, so the response
includes the live cluster Spending Limit.

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
    "max_storage_size": 102400,
    "tidbcloud_spending_limit": 10000
  }'
```

At least one of `max_storage_size` or `tidbcloud_spending_limit` is required;
`max_storage_size` must be positive, and `tidbcloud_spending_limit` must be
non-negative. Drive9 passes `tidbcloud_spending_limit: 0` through to TiDB
Cloud; TiDB Cloud may reject it.

## Error responses

The quota API returns JSON errors through the standard server error shape.

| Status | When it happens |
| --- | --- |
| `400 Bad Request` | Invalid JSON, missing `tenant_id`, missing or partial TiDB Cloud credentials, missing all settable quota fields in a set request, or an invalid quota value. |
| `403 Forbidden` | TiDB Cloud returns unauthorized or forbidden for the supplied API key. The message is `no permission to query quota with TiDB Cloud API key` or `no permission to update quota with TiDB Cloud API key`. |
| `404 Not Found` | The Drive9 tenant does not exist, quota is not enabled on this server, or TiDB Cloud cannot find the backend cluster. For the backend-cluster case, check the TiDB Cloud starter/native cluster status. |
| `409 Conflict` | The tenant provider is not `tidb_cloud_native`. |
| `502 Bad Gateway` | TiDB Cloud returned another upstream error while querying quota, updating quota labels, or updating Spending Limit. |

## Notes for operators

- Server-side quota reads high-churn usage counters from the central meta store.
  Low-churn quota config is cached per tenant and refreshed by version polling.
- In cloud-native mode, small write quota mutations are recorded in a
  tenant-local quota outbox and applied asynchronously to central quota state.
- Small-write quota admission includes pending outbox deltas to reduce
  stale-central-usage undercounting, but concurrent writes on multiple servers
  may briefly over-admit. Durable outbox processing and backfill restore
  convergence.
- Multipart uploads use a stricter reserve-first path before writing tenant
  upload state.
