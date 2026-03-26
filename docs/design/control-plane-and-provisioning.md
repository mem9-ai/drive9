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
- **tenant-to-cell mapping**: the routing relation from tenant identity to its tenant cell
- **control-plane metadata**: the global metadata needed to locate and manage tenant cells

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

- creating a tenant cell
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

Each tenant should be provisioned with an isolated tenant cell that includes:

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

### 5.3 Routing model

Requests should be resolved through:

- tenant identity
- tenant-to-cell mapping
- access to tenant-local backends for that cell

Practical auth flow:

```text
Authorization header
  -> derive lookup key
  -> verify credential against control-plane metadata
  -> resolve tenant status and tenant cell
  -> route request to tenant-local db9 + S3 + runtime
```

### 5.4 Policy model

The control plane should own:

- quotas
- rate limiting
- provisioning policy
- deletion/suspension policy

### 5.5 Credential and metadata security

The control plane should follow these minimum rules:

- tenant credentials must not be stored in plaintext
- credential lookup may use a short prefix plus full-hash verification
- tenant status must be enforced during auth
- transport must require HTTPS

Representative metadata fields include:

- `tenant_id`
- credential lookup fields
- tenant-local `db9` connection or service reference
- tenant `S3` bucket/prefix or equivalent namespace reference
- tenant status such as `PROVISIONING`, `ACTIVE`, `SUSPENDED`, `DELETED`

## 6. Invariants / Correctness Rules

- tenant-local state must remain isolated at the tenant-cell level
- global control-plane metadata must not become the holder of tenant business truth
- routing must always resolve to one valid tenant cell or fail explicitly

Additional practical rules:

- tenant credentials should be shown in plaintext only at creation time if the product uses one-time secret reveal
- suspended or deleted tenant state must fail auth explicitly

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

## 9. References / Dependencies

- `docs/design/system-architecture.md`
- `docs/overview.md`
- `docs/design/api-and-ux-contract.md`
