# RFC: dat9 Control Plane and Provisioning

## 1. Goal

This RFC defines how dat9 handles tenant onboarding, control-plane responsibilities, provisioning, routing, and fleet-level policy.

## 2. Non-goals

This RFC does not define:

- tenant-local storage schemas
- task-runtime internals
- semantic derivation internals

This RFC also does not require every future provisioning and anti-abuse feature to be implemented immediately. It defines the control-plane contract and the minimum safe model.

## 3. Definitions

- **tenant onboarding**: the process of creating a new tenant and making it routable
- **tenant-to-cell mapping**: the routing relation from tenant identity to its Tenant Cell
- **control-plane metadata**: the global metadata needed to locate and manage Tenant Cells

## 4. Current Implementation Target

### 4.1 P0 / P1 control-plane contract

For the current phase, the control plane should provide at least:

- tenant creation and activation
- tenant-to-cell routing
- API-key-based authentication
- tenant status tracking
- basic rate limiting and provisioning controls

### 4.2 Minimum operational model

The minimum viable onboarding flow should support:

- creating a Tenant Cell
- provisioning or assigning the tenant-local `db9` unit
- assigning the tenant-scoped `S3` namespace
- making the tenant routable through control-plane metadata

## 5. Design

### 5.1 Control-plane responsibilities

The Global Control Plane is responsible for:

- authentication entry points
- tenant routing
- provisioning and deprovisioning
- quota and policy enforcement
- cross-tenant observability aggregation

### 5.2 Provisioning model

Each tenant should be provisioned with an isolated Tenant Cell that includes:

- an isolated metadata and task-execution unit
- an isolated `db9` cluster or equivalent isolated `db9` service unit
- an isolated durable `queuefs`
- a tenant-scoped `S3` namespace for large-file storage

Representative onboarding flow:

```text
client -> POST /v1/provision
control plane -> create tenant identity
control plane -> provision db9 unit and S3 namespace
control plane -> store tenant-to-cell mapping
control plane -> return tenant credential once
```

Recommended additional provisioning details:

- credentials should be revealed in plaintext only once at creation time if the product uses generated API keys
- tenant metadata should record lifecycle state such as `PROVISIONING`, `ACTIVE`, `SUSPENDED`, and `DELETED`
- provisioning should create the tenant-scoped object prefix before the tenant becomes routable

Representative tenant lifecycle:

```text
create request
     |
     v
PROVISIONING -----> ACTIVE <-----> SUSPENDED
                      |
                      +-----------> DELETED
```

The important routing rule is:

- `PROVISIONING` is not routable for normal tenant traffic
- `ACTIVE` is routable
- `SUSPENDED` fails auth or policy checks explicitly
- `DELETED` is terminal for normal access

Recommended provisioning phases:

1. Create the control-plane tenant record with status `PROVISIONING`.
2. Allocate or assign the tenant-local `db9` unit and object namespace.
3. Initialize tenant-local schema and queue/runtime prerequisites.
4. Persist tenant-to-cell routing metadata and mark the tenant `ACTIVE`.
5. Reveal the generated credential once, if the product uses generated API keys.

### 5.3 Control-plane metadata model

The new RFC set intentionally avoids freezing one exact DDL, but the old document was right that reviewers need a concrete metadata shape.

Representative tenant metadata fields:

- `tenant_id`
- `api_key_prefix`
- `api_key_hash`
- `cell_id` or equivalent routing target
- tenant-local `db9` service reference
- tenant `S3` bucket/prefix or equivalent namespace reference
- `status`
- `created_at`
- `last_active_at`
- optional policy or quota references

Representative logical table:

```text
tenants
  tenant_id         primary identity
  api_key_prefix    lookup accelerator only
  api_key_hash      full credential verifier
  cell_id           routing target
  db9_ref           tenant-local state endpoint
  s3_bucket/prefix  tenant object namespace
  status            PROVISIONING | ACTIVE | SUSPENDED | DELETED
  created_at        audit timestamp
  last_active_at    routing / abuse / ops signal
```

Practical rules:

- prefix lookup is only an optimization; full-hash verification remains mandatory
- control-plane metadata stores routing and operational facts, not tenant business state
- partially provisioned tenants remain in `PROVISIONING` until all required tenant-local dependencies exist

### 5.4 Routing model

Requests should be resolved through:

- tenant identity
- tenant-to-cell mapping
- access to tenant-local backends for that cell

Practical auth flow:

```text
Authorization header
  -> derive lookup key
  -> control-plane metadata lookup by prefix/hash
  -> verify credential and tenant status
  -> resolve Tenant Cell and backend references
  -> route request to tenant-local db9 + S3 + runtime
```

Representative request path:

```text
client request
  -> control plane auth
  -> tenant metadata lookup
  -> status check
  -> tenant-to-cell resolution
  -> tenant-local connection / service selection
  -> forward to the target Tenant Cell
```

Recommended auth behavior:

- extract the credential from `Authorization`
- derive `api_key_prefix`
- select candidate tenant rows by prefix
- verify the full credential hash
- reject non-`ACTIVE` tenants before routing tenant traffic

### 5.5 Policy model

The control plane should own:

- quotas
- rate limiting
- provisioning policy
- deletion/suspension policy

### 5.6 Credential and metadata security

The control plane should follow these minimum rules:

- tenant credentials must not be stored in plaintext
- credential lookup may use a short prefix plus full-hash verification
- tenant status must be enforced during auth
- transport must require HTTPS

More concrete recommendations:

- use a credential format that is easy to identify in logs and secret-scanning systems, for example a stable prefix such as `dat9_`
- use prefix lookup only as an optimization, never as the sole proof of identity
- require full-hash verification after any prefix lookup hit

Representative metadata fields include:

- `tenant_id`
- credential lookup fields
- tenant-local `db9` connection or service reference
- tenant `S3` bucket/prefix or equivalent namespace reference
- tenant status such as `PROVISIONING`, `ACTIVE`, `SUSPENDED`, `DELETED`

### 5.7 Provisioning safety and initialization

Provisioning should be observable as a multi-step control-plane workflow rather than one opaque side effect.

Representative initialization actions:

- create the tenant metadata row
- allocate the tenant-local `db9` unit or equivalent service binding
- create the tenant object-storage prefix such as `tenants/<tenant_id>/...`
- initialize required tenant-local schemas, indexes, or runtime tables
- persist routing metadata only after the target cell is ready

If one of these steps fails:

- leave the tenant in `PROVISIONING`
- record enough metadata for retry or operator cleanup
- do not route ordinary tenant traffic to the half-initialized cell

### 5.8 Anti-abuse and operational controls

The control plane should define minimum abuse protections for provisioning and tenant access.

Examples:

- rate limits on `/v1/provision`
- maximum simultaneous provisioning attempts from one source
- limits on active tenants or cells per source identity, if needed
- explicit rejection of unauthenticated or suspended tenant requests

## 6. Invariants / Correctness Rules

- tenant-local state must remain isolated at the Tenant Cell level
- global control-plane metadata must not become the holder of tenant business truth
- routing must always resolve to one valid Tenant Cell or fail explicitly

Additional practical rules:

- tenant credentials should be shown in plaintext only at creation time if the product uses one-time secret reveal
- suspended or deleted tenant state must fail auth explicitly
- tenants in `PROVISIONING` must not receive normal routed workload

## 7. Failure / Recovery

- control-plane failures should not silently corrupt tenant-cell state
- provisioning must be retryable and observable
- tenant-to-cell mappings must be recoverable from durable control-plane metadata

For the current phase, operational safety should include:

- retryable provisioning flow
- explicit tenant status transitions
- protection against unauthenticated or partially provisioned tenant access

## 8. Open Questions

- how much automatic provisioning should be exposed directly to users versus administrators
- whether suspended tenants retain warm cells or require cold reactivation

Current product decisions still worth documenting explicitly:

- API key only vs scoped tokens
- rate limit only vs stronger anti-abuse gating for `/v1/provision`
- one tenant per isolated db9 unit vs other equivalent service-unit strategies

Recommended current-phase default leaning:

- API key based auth for P0/P1
- rate limiting as the baseline anti-abuse control, with stronger gating added later if needed

## 9. References / Dependencies

- `docs/design/system-architecture.md`
- `docs/overview.md`
- `docs/design/api-and-ux-contract.md`
