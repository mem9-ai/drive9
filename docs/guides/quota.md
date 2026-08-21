# drive9 quota guide

Last verified: 2026-08-22.

This guide shows how to read and update Drive9 tenant quota from the CLI and
HTTP API in TiDBCloud Mode.

## What quota tracks

Drive9 exposes these user-settable quota settings:

| Field | Meaning |
| --- | --- |
| `max_storage_size` | Maximum confirmed plus reserved file storage size, in Mi. Stored in Drive9. |
| `max_file_size` | Maximum single file size, in Mi. Stored in Drive9. Must not exceed the server `DRIVE9_MAX_UPLOAD_BYTES` limit. |
| `max_file_count` | Maximum file count. Stored in Drive9. `0` means unlimited. |
| `max_media_llm_files` | Maximum number of media (image/audio) LLM extraction files admitted for the tenant. Stored in Drive9. Updating it requires the tenant-specific media extract config to be enabled. |
| `max_video_llm_files` | Maximum number of video LLM extraction tasks admitted for the tenant. Stored in Drive9. Updating it requires the tenant-specific video extract config to be enabled. |
| `tidbcloud_spending_limit` | TiDB Cloud Cluster Spending Limit for dedicated native tenants. Shared tenants omit this response field and ignore it when supplied. `0` remains a valid explicit input for dedicated compatibility. See the [TiDB Cloud Spending Limit guide](https://docs.pingcap.com/tidbcloud/manage-serverless-spend-limit). |

Quota responses include these storage usage counters:

| Field | Meaning |
| --- | --- |
| `storage_bytes` | Confirmed file storage bytes. |
| `reserved_bytes` | Bytes reserved by active uploads. |
| `file_count` | Current file count used by quota admission. Active upload reservations for new files may be included until they complete or abort. |
| `media_file_count` | Current media files counted by LLM extraction admission. |
| `video_file_count` | Current video extraction tasks counted by LLM extraction admission. |

## Permissions and supported modes

Tenant list/get and quota update require TiDB Cloud API keys. A Drive9 tenant
API key is not accepted for these admin operations.
Server-side default TiDB Cloud credentials configured for tenant provision or
deprovision are not used as a fallback by quota read or update. Callers must
supply TiDB Cloud credentials on each quota request; those credentials may be
the same keys as the server defaults when passed explicitly.

`tidb_cloud_native` and `tidb_cloud_native_shared` tenants support quota update
through this API. Other tenant providers use their configured defaults.

Tenant list/get validates that the supplied TiDB Cloud API key can access the
tenant's cluster. Drive9 caches successful API-key-to-cluster authorization in
process for up to one hour to avoid calling the TiDB Cloud OpenAPI on every
read. Empty managed-cluster list results are not retained, and successful tenant
create/delete paths invalidate the full-list cache for the submitted
credentials. The quota response itself is assembled from Drive9's local tenant,
quota config, and usage tables after authorization succeeds.

For a dedicated `tidb_cloud_native` tenant, quota update validates TiDB Cloud
cluster write permission, patches `tidbcloud_spending_limit` when present, and
then writes the Drive9-stored quota fields. The effective dedicated spending
limit is returned by the provisioner and persisted locally. For a
`tidb_cloud_native_shared` tenant, quota update authorizes the shared physical
pool and writes the Drive9-stored quota fields; any supplied spending-limit
value, including `0`, is accepted and ignored. Quota GET responses are
assembled from Drive9's local tenant, quota config, and usage tables after
authorization.

## CLI

Use TiDB Cloud credentials to list TiDBCloud Mode tenants with quota:

```bash
drive9 admin tenant list \
  --region-code aws-ap-southeast-1 \
  --tidbcloud-public-key <tidbcloud-public-key> \
  --tidbcloud-private-key <tidbcloud-private-key>
```

Use tenant get when you already have the Drive9 tenant id:

```bash
drive9 admin tenant get \
  --region-code aws-ap-southeast-1 \
  --tenant-id tnt_abc123 \
  --tidbcloud-public-key <tidbcloud-public-key> \
  --tidbcloud-private-key <tidbcloud-private-key>
```

Example output:

```text
TENANT_ID   STATUS  KIND  MAX_STORAGE  MAX_FILE_SIZE  MAX_FILE_COUNT  MAX_MEDIA_LLM_FILES  MAX_VIDEO_LLM_FILES  SPENDING_LIMIT  STORAGE_USED  RESERVED  FILE_COUNT  MEDIA_FILE_COUNT  VIDEO_FILE_COUNT
tnt_abc123  active  live  102400 Mi    1024 Mi        100000          400                  50                   10000           1.0 MiB       0 B       12          20                3
```

Set quota with `drive9 admin tenant set-quota`. Pass at least one of
`--max-storage-size`, `--max-file-size`, `--max-file-count`,
`--max-media-llm-files`, `--max-video-llm-files`, or
`--tidbcloud-spending-limit`. The two LLM quota fields can be modified only
after the corresponding tenant-specific extract config is enabled; the CLI
passes the values through and the server enforces that prerequisite.

```bash
drive9 admin tenant set-quota \
  --region-code aws-ap-southeast-1 \
  --tenant-id tnt_abc123 \
  --tidbcloud-public-key <tidbcloud-public-key> \
  --tidbcloud-private-key <tidbcloud-private-key> \
  --max-storage-size 102400 \
  --max-file-size 1024 \
  --max-file-count 100000 \
  --max-media-llm-files 400 \
  --max-video-llm-files 50 \
  --tidbcloud-spending-limit 10000
```

Use `--server` instead of `--region-code` when targeting a known Drive9 server
URL directly. If both are present, `--server` wins.

Validation rules:

- `--max-storage-size` must be positive.
- `--max-file-size` must be positive and no larger than the server
  `DRIVE9_MAX_UPLOAD_BYTES` limit.
- `--max-file-count` must be non-negative. `0` means unlimited.
- `--max-media-llm-files` and `--max-video-llm-files` must be non-negative and
  require the corresponding tenant-specific extract config on the server.
- `--tidbcloud-spending-limit` must be a non-negative 32-bit integer. Drive9
  passes it through for dedicated tenants. Shared tenants accept and ignore
  this field.

## HTTP API

Tenant get returns tenant information with quota. This dedicated-tenant example
includes `tidbcloud_spending_limit`; shared-tenant responses omit that field:

```json
{
  "tenant_id": "tnt_abc123",
  "status": "active",
  "kind": "live",
  "quota": {
    "config": {
      "max_storage_size": 102400,
      "max_file_size": 1024,
      "max_file_count": 100000,
      "max_media_llm_files": 400,
      "max_video_llm_files": 50,
      "tidbcloud_spending_limit": 10000
    },
    "usage": {
      "storage_bytes": 1048576,
      "reserved_bytes": 0,
      "file_count": 12,
      "media_file_count": 20,
      "video_file_count": 3
    }
  }
}
```

### GET /v1/admin/tenants

List authorized TiDBCloud Mode tenants. Add `include_quota=true` when the
response should include quota for each tenant.

```bash
curl -sS \
  -H "X-TiDBCloud-Public-Key: <tidbcloud-public-key>" \
  -H "X-TiDBCloud-Private-Key: <tidbcloud-private-key>" \
  "https://drive9.example.com/v1/admin/tenants?page=1&page_size=10&include_quota=true"
```

### GET /v1/admin/tenants/{tenant-id}

Get a single authorized TiDBCloud Mode tenant. The response includes quota.

```bash
curl -sS \
  -H "X-TiDBCloud-Public-Key: <tidbcloud-public-key>" \
  -H "X-TiDBCloud-Private-Key: <tidbcloud-private-key>" \
  "https://drive9.example.com/v1/admin/tenants/tnt_abc123"
```

### POST /v1/admin/tenants/{tenant-id}/quota

Set quota for a `tidb_cloud_native` or `tidb_cloud_native_shared` tenant using
TiDB Cloud credentials.

```bash
curl -sS \
  -H "Content-Type: application/json" \
  -X POST https://drive9.example.com/v1/admin/tenants/tnt_abc123/quota \
  -d '{
    "public_key": "<tidbcloud-public-key>",
    "private_key": "<tidbcloud-private-key>",
    "max_storage_size": 102400,
    "max_file_size": 1024,
    "max_file_count": 100000,
    "max_media_llm_files": 400,
    "max_video_llm_files": 50,
    "tidbcloud_spending_limit": 10000
  }'
```

At least one of `max_storage_size`, `max_file_size`, `max_file_count`,
`max_media_llm_files`, `max_video_llm_files`, or `tidbcloud_spending_limit` is
required. `max_storage_size` and `max_file_size` are in Mi;
`max_file_size` must not exceed the server `DRIVE9_MAX_UPLOAD_BYTES` limit.
`max_file_count`, `max_media_llm_files`, and `max_video_llm_files` must be
non-negative, and `0` means unlimited for the corresponding limit. LLM quota
updates require the matching tenant-specific extract config. Shared tenants
accept and ignore `tidbcloud_spending_limit`, including `0`; dedicated tenants
retain the existing TiDB Cloud spending-limit behavior.

## Error responses

The quota API returns JSON errors through the standard server error shape.

| Status | When it happens |
| --- | --- |
| `400 Bad Request` | Invalid JSON, missing tenant id where required, missing or partial TiDB Cloud credentials, missing all settable quota fields in a set request, or an invalid quota value. |
| `403 Forbidden` | TiDB Cloud returns unauthorized or forbidden for the supplied API key. |
| `404 Not Found` | The Drive9 tenant does not exist, quota is not enabled on this server, or TiDB Cloud cannot find the backend cluster. For the backend-cluster case, check the TiDB Cloud starter/native cluster status. |
| `409 Conflict` | The tenant provider does not support quota updates. |
| `502 Bad Gateway` | TiDB Cloud returned another upstream error while listing managed clusters, updating quota labels, or updating Spending Limit. |

## Notes for operators

- Server-side quota admission reads high-churn usage counters from the central
  meta store. Low-churn quota config is cached per tenant and refreshed by
  version polling.
- In cloud-native mode, small write quota mutations are recorded in the central
  meta DB `quota_mutation_log` and applied asynchronously to central quota
  state.
- Small-write storage and file-count quota admission include this process's
  pending central mutation deltas to reduce stale-central-usage undercounting,
  but concurrent writes on multiple servers may briefly over-admit. Durable
  mutation replay restores convergence for logged mutations; rare post-commit
  gaps require operational reconciliation from tenant file metadata.
- Multipart uploads use a stricter reserve-first path before writing tenant
  upload state.
- TiDB Cloud quota reads expose low-cardinality observability counters:
  `drive9_tidbcloud_rbac_cache_requests_total`,
  `drive9_tidbcloud_openapi_requests_total`,
  `drive9_tidbcloud_spending_limit_sync_total`, and
  `drive9_tidbcloud_spending_limit_missing_total`. These metrics intentionally
  do not include `tenant_id`.
- Do not alert on cache misses, spending-limit backfills, or missing local
  spending-limit observations being greater than zero. Those are normal during
  first access, key changes, or migration. Alert only on sustained TiDB Cloud
  OpenAPI error rate or an unexpected OpenAPI call-rate baseline increase.
