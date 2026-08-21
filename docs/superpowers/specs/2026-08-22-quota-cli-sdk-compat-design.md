# Quota CLI and SDK Compatibility Design

## Goal

Bring the Drive9 Go SDK and CLI in line with the merged FS quota contract while
removing the current duplication between quota request payloads and CLI quota
rendering. Existing exported client methods and existing storage/file quota
behavior remain backward compatible.

## Scope

The client contract will support these quota configuration fields:

- `max_storage_size`
- `max_file_size`
- `max_file_count`
- `max_media_llm_files`
- `max_video_llm_files`
- `tidbcloud_spending_limit`

The response usage contract will support:

- `storage_bytes`
- `reserved_bytes`
- `file_count`
- `media_file_count`
- `video_file_count`

The CLI `admin tenant set-quota` command will accept the two LLM quota limits,
and tenant list/get/set text output will show both limits and both usage
counters. JSON output will use the same SDK structs, so it will include the
fields without a separate CLI-specific schema.

`tidbcloud_spending_limit` remains available for existing dedicated tenants.
The CLI and SDK continue to send it when explicitly supplied; shared-tenant
ignore behavior is owned by the server and must not be duplicated or rejected
by the client.

Tenant extract-config APIs are out of scope because they are already present on
the current `main` branch through PR #851. Free-organization limits, quota
validation, enforcement, provisioning behavior, and server API behavior are
also out of scope.

## Design

### SDK quota model

`pkg/client/quota.go` remains the source of truth for the public quota model.
`QuotaConfig` gains the two LLM limit fields and `QuotaUsage` gains the two
extraction usage counters. `QuotaSetRequest` gains pointer fields for the two
LLM limits so omitted fields remain distinguishable from an explicit zero.

The existing admin quota endpoint will serialize through one private request
payload type defined beside the quota model. Both the deprecated `/v1/quota`
compatibility method and `AdminSetTenantQuota` will use the same field mapping,
while preserving their existing credential placement and URL behavior. This
eliminates the anonymous duplicate body currently maintained in
`admin_tenants.go` without changing public method signatures.

### CLI quota model and rendering

`cmd/drive9/cli/quota.go` will parse the two new flags using the existing
non-negative integer validation and pass pointer values into
`client.QuotaSetRequest`. The command must still accept a request containing
only `--tidbcloud-spending-limit`, because dedicated compatibility depends on
that path and shared servers intentionally ignore it.

The shared admin table formatter will own the complete quota column order and
cell formatting for tenant list/get and quota set output. It will add:

`MAX_MEDIA_LLM_FILES`, `MAX_VIDEO_LLM_FILES`, `MEDIA_FILE_COUNT`, and
`VIDEO_FILE_COUNT`.

The formatter will keep `-` for absent quota data and preserve the existing
`unlimited` representation for `max_file_count == 0`. `tidbcloud_spending_limit`
will remain `-` when the server omits it, which is the expected shared-tenant
response.

Regular help, visual help, and the quota guide will describe the new flags and
fields. The guide will state that LLM quota updates require the corresponding
tenant-specific extract configuration; this is a server-side precondition, not
a client-side validation rule.

## Error and compatibility behavior

- Explicit zero for either LLM limit is serialized because pointer fields are
  non-nil; omitted values are not serialized.
- The client performs only the existing numeric syntax/range checks. It does
  not inspect provider, extract configuration, or spending-limit policy.
- Unknown server response fields remain ignored by `encoding/json`, preserving
  compatibility with newer server fields.
- Existing callers using storage/file limits or dedicated spending limits keep
  the same JSON field names, endpoints, credentials, and response types.

## Testing

Focused tests will cover:

1. SDK response decoding for both LLM limits and both usage counters.
2. SDK request serialization for non-zero and explicit-zero LLM limits, with
   omitted fields absent from JSON.
3. Deprecated and admin quota methods sharing the complete request field set.
4. CLI parsing and request forwarding for both LLM flags, including zero.
5. CLI text output for tenant list/get and quota set with the new columns and
   omitted shared spending limit.
6. Help text and quota guide references for the new controls.

Validation will run the focused client, CLI, and cookbook tests, followed by
format/lint checks and the relevant full package tests before the PR is opened.
