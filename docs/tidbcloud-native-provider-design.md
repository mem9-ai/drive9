# drive9 `tidbcloud-native` Provider Design

## Background

drive9 needs a new provider mode, `tidbcloud-native`, for deep TiDB Cloud integration across zero and starter/essential.

The key requirement is request-header-driven target resolution:

- `X-TIDBCLOUD-ZERO-INSTANCE-ID`
- `X-TIDBCLOUD-CLUSTER-ID`

When these headers are present, drive9 must resolve and validate against TiDB Cloud control plane, then use the real cluster connection plus `cloud_admin` password for SQL/provision flows.

## Goals

- Support `X-TIDBCLOUD-ZERO-INSTANCE-ID` and `X-TIDBCLOUD-CLUSTER-ID`.
- Enforce header usage only for provider `tidbcloud-native`.
- Keep existing behavior unchanged for non-`tidbcloud-native` providers.
- For `cluster-id`, require TiDB Cloud authz check (API key or OAuth).
- Keep current operational model: CLI still runs Provision first.
- Provision uses database `mysql` (not default `test`).

## Non-Goals

- No auto-watch/auto-provision from TiDB cluster creation events.
- No tenant allowlist/whitelist gray rollout.

## Header Semantics

### Supported headers

- `X-TIDBCLOUD-ZERO-INSTANCE-ID`
- `X-TIDBCLOUD-CLUSTER-ID`

### Priority

If both are present, `X-TIDBCLOUD-CLUSTER-ID` wins.

### Provider gating

If either `X-TIDBCLOUD-*` header is present and tenant provider is not `tidbcloud-native`, return:

- HTTP `400`
- message: `unsupported X-TIDBCLOUD-ZERO-INSTANCE-ID header` or `unsupported X-TIDBCLOUD-CLUSTER-ID header` (based on the provided header)

If `DRIVE9_TENANT_PROVIDER=tidbcloud-native` but neither `X-TIDBCLOUD-ZERO-INSTANCE-ID` nor `X-TIDBCLOUD-CLUSTER-ID` is provided, return:

- HTTP `400`
- message: `missing required header X-TIDBCLOUD-ZERO-INSTANCE-ID or X-TIDBCLOUD-CLUSTER-ID`

## Auth and Resolution Flow

### A) Zero instance flow (`X-TIDBCLOUD-ZERO-INSTANCE-ID`)

1. Validate provider is `tidbcloud-native`.
2. Query TiDB Cloud Global Server by instance ID.
3. Resolve tenant mapping and real cluster connection metadata.
4. Get connection info + `cloud_admin` password.
5. Continue Provision/SQL.

### B) Cluster flow (`X-TIDBCLOUD-CLUSTER-ID`)

1. Validate provider is `tidbcloud-native`.
2. Extract auth from request:
   - Prefer TiDB Cloud API key pair (`public key` + `private key`) from request metadata.
   - For upstream TiDB Cloud Account API calls, drive9 uses HTTP Digest auth with `public:private` (same as official usage, e.g. `curl --digest --user 'PUBLIC:PRIVATE' ...`).
   - Fallback to OAuth token path only if configured by product/API contract.
   - `Authorization` on the drive9 request remains reserved for drive9 tenant auth and must not be repurposed directly for TiDB Cloud API key auth.
3. Call TiDB Cloud Account + Global services to verify this auth can operate the target cluster.
4. On success, call Global Server by cluster ID to get connection info + `cloud_admin` password.
5. Continue Provision/SQL.

If auth is missing/invalid -> `401`.
If auth exists but lacks permission -> `403`.

## Provision Behavior

Provision remains mandatory before data operations.

For `tidbcloud-native`:

- Apply the same header decision/auth logic described above.
- If validation passes, initialize tenant state.
- Use database name `mysql` for initialization and runtime SQL context.

## Runtime SQL Behavior

For `tidbcloud-native` + valid `X-TIDBCLOUD-*`:

1. Resolve target using header flow.
2. Build runtime DSN from resolved real cluster + `cloud_admin` password.
3. Execute SQL against that resolved cluster.

No plaintext secrets in logs.

## Server Changes (Proposed)

- `pkg/server/auth.go`
  - Parse `X-TIDBCLOUD-*` headers and request auth.
  - Build request-scoped TiDB Cloud context.
- `pkg/server/server.go`
  - In Provision/SQL paths, branch to `tidbcloud-native` resolver when headers exist.
- `pkg/tenant/*`
  - Ensure provider enum/value supports `tidbcloud-native`.
  - Note: existing provider names are mostly snake_case; this hyphenated value is intentional and must be normalized explicitly in provider parsing (`tenant.NormalizeProvider`) and related config docs.
- New package `pkg/tidbcloud/`:
  - `account_client.go`: permission verification for cluster operations.
  - `global_client.go`: resolve instance/cluster -> real connection data.
  - `resolver.go`: unified decision logic for header/auth/provider.

## Error Contract

- `400`: unsupported header for provider, malformed header input.
- `401`: missing/invalid TiDB Cloud auth for cluster flow.
- `403`: auth valid but forbidden for cluster.
- `404`: instance/cluster not found in TiDB Cloud.
- `502`: upstream TiDB Cloud service failure.
- `500`: internal drive9 failure.

## Configuration and Release

No dedicated feature flag is required.

Enablement is decided only by tenant provider value:

- `DRIVE9_TENANT_PROVIDER=tidbcloud-native` -> native flow enabled.
- any other provider -> native headers are rejected as unsupported.

So rollout/rollback is done by provider configuration, not by a separate `*_ENABLED` flag.

## Security and Observability

- Never log `cloud_admin` password or full DSN.
- Add structured audit fields:
  - provider
  - header type (`instance`/`cluster`)
  - instance ID / cluster ID
  - auth mode (`api_key`/`oauth`)
  - decision (`allow`/`deny`) and reason
  - trace ID
- Add timeout + retry with bounded backoff for TiDB Cloud control-plane calls.

## TiDB Cloud API Auth Note

- TiDB Cloud API key authentication follows official digest mode:
  - `curl --digest --user 'YOUR_PUBLIC_KEY:YOUR_PRIVATE_KEY' --request GET --url https://api.tidbcloud.com/api/v1beta/projects`
- drive9 should mirror this when calling TiDB Cloud Account/Global services with API key credentials.

## Testing Plan

### Unit tests

- Header precedence (`cluster` over `instance`).
- Provider gating and unsupported-header errors.
- Cluster flow auth checks (`401`/`403` cases).
- Provision DB name forced to `mysql`.

### Integration tests

- Mock Account/Global APIs for both header flows.
- Validate resolved DSN path and SQL execution behavior.

### E2E tests

- CLI Provision first, then file/SQL operations for `tidbcloud-native`.
- Validate both zero-instance and cluster paths.
