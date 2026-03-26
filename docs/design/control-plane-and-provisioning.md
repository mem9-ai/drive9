# RFC: dat9 Control Plane and Provisioning

## 1. Goal

This RFC defines how dat9 handles tenant onboarding, control-plane responsibilities, provisioning, routing, and fleet-level policy.

## 2. Non-goals

This RFC does not define:

- tenant-local storage schemas
- task-runtime internals
- semantic derivation internals

## 3. Definitions

- **tenant onboarding**: the process of creating a new tenant and making it routable
- **tenant-to-cell mapping**: the routing relation from tenant identity to its tenant cell
- **control-plane metadata**: the global metadata needed to locate and manage tenant cells

## 4. Design

### 4.1 Control-plane responsibilities

The Global Control Plane is responsible for:

- authentication entry points
- tenant routing
- provisioning and deprovisioning
- quota and policy enforcement
- cross-tenant observability aggregation

### 4.2 Provisioning model

Each tenant should be provisioned with an isolated tenant cell that includes:

- an isolated metadata and task-execution unit
- an isolated `db9` cluster or equivalent isolated `db9` service unit
- an isolated durable `queuefs`
- a tenant-scoped `S3` namespace for large-file storage

### 4.3 Routing model

Requests should be resolved through:

- tenant identity
- tenant-to-cell mapping
- access to tenant-local backends for that cell

### 4.4 Policy model

The control plane should own:

- quotas
- rate limiting
- provisioning policy
- deletion/suspension policy

## 5. Invariants / Correctness Rules

- tenant-local state must remain isolated at the tenant-cell level
- global control-plane metadata must not become the holder of tenant business truth
- routing must always resolve to one valid tenant cell or fail explicitly

## 6. Failure / Recovery

- control-plane failures should not silently corrupt tenant-cell state
- provisioning must be retryable and observable
- tenant-to-cell mappings must be recoverable from durable control-plane metadata

## 7. Open Questions

- how much automatic provisioning should be exposed directly to users versus administrators
- whether suspended tenants retain warm cells or require cold reactivation

## 8. References / Dependencies

- `docs/design/system-architecture.md`
- `dat9/docs/overview.md`
