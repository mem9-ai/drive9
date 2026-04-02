# Proposal: Use `tenants.schema_version` to manage the `semantic_tasks` typed-claim index upgrade

**Date**: 2026-04-02
**Purpose**: Define an incremental schema upgrade plan for TiDB Zero / TiDB Cloud Starter tenants based on `tenants.schema_version`, so `semantic_tasks.idx_task_claim_type` can be reconciled without re-running index repair logic every time a tenant backend is opened, while keeping the upgrade path verifiable, rollback-friendly, and incrementally deployable.

## Summary

The current code already adds `idx_task_claim_type` to newly created tenant schemas, but it still does not provide an explicit upgrade path for existing Zero / Starter tenants. This proposal recommends:

1. Converging `tenants.schema_version` into an explicit control-plane version number for the tenant data-plane schema, instead of leaving it as an unused field.
2. Defining `schema_version = 1` as the old baseline where `semantic_tasks.idx_task_claim_type` is not guaranteed to exist, and `schema_version = 2` as the new baseline where the typed-claim index is present.
3. Introducing an idempotent, version-gated schema reconcile flow for Zero / Starter tenants: attempt index repair only when `schema_version < 2`, and advance `schema_version` with CAS semantics after success so later opens do not repeat the work.
4. Marking new tenants as `active` only after schema initialization succeeds and the latest `schema_version` is written; existing tenants perform a single version-gated reconcile the first time their backend is opened.
5. Treating `schema_version` only as a gate for "should migration be attempted"; the actual tenant DB schema remains the source of truth, and runtime validation still stays in place after migration to avoid long-term drift between control-plane records and real schema state.

This design deliberately reuses the existing boundaries around `meta.Store`, `tenant.Pool`, `Provisioner.InitSchema`, and `ValidateTiDBSchemaForMode(...)`. It does not introduce a separate migration service, and it does not require a one-time offline fleet-wide migration prerequisite.

## Context

### Current State

The following facts are directly verified in the current repository:

- The typed claim query for `semantic_tasks` already exists. In `pkg/datastore/semantic_tasks.go`, `ClaimSemanticTask(..., taskTypes...)` generates a `WHERE status = ? AND task_type ... AND available_at <= ? ORDER BY ... FOR UPDATE SKIP LOCKED` query when task-type filtering is present.
- Commit `4a7bb89` added the new index `idx_task_claim_type ON semantic_tasks(status, task_type, available_at, created_at, task_id)` to three schema builders: `pkg/tenant/schema_tidb_auto.go`, `pkg/tenant/schema_tidb_app.go`, and `pkg/tenant/schema_db9.go`.
- `ValidateTiDBSchemaForMode(...)` currently validates the `files` table structure strictly, and only confirms that the `semantic_tasks` table exists; it does not validate indexes on `semantic_tasks`, see `pkg/tenant/schema_tidb_auto.go`.
- Zero tenants run `initZeroSchema(dsn)` directly from `InitSchema`, so the application currently owns table/index creation for Zero, see `pkg/tenant/zero.go` and `pkg/tenant/schema_zero.go`.
- Starter tenants currently call only `validateTiDBSchemaForModeDSN(..., TiDBEmbeddingModeAuto)` from `InitSchema`, and do not proactively execute DDL, see `pkg/tenant/starter.go`.
- The backend-open path lives in `tenant.Pool.createBackend(...)`. For Zero / Starter with `DatabaseAutoEmbedding = true`, it currently only calls `ValidateTiDBSchemaForMode(store.DB(), TiDBEmbeddingModeAuto)`, see `pkg/tenant/pool.go`.
- The control-plane `tenants` table already contains a `schema_version` column with default `1`, and `meta.Tenant` already carries `SchemaVersion`, but no code currently advances that version, see `pkg/meta/meta.go`.
- During new-tenant provisioning, `server.handleProvision` still writes `SchemaVersion: 1` into `tenants`, and `initTenantSchemaAsync(...)` only updates tenant status to `active` on success; it does not update `schema_version`, see `pkg/server/server.go`.

As a result, the system currently has two mismatches:

1. The new schema already defines the typed-claim index, but existing Zero / Starter tenants have no explicit upgrade path.
2. The control plane already has `schema_version`, but it neither reflects the actual current schema version nor participates in any upgrade gating.

### Problem Statement

Without adding a `schema_version`-based migration path, the current implementation leaves three problems behind:

1. Existing Zero / Starter tenants will not automatically gain `idx_task_claim_type`; typed claim must rely on the old index plan, which risks scan amplification and throughput regression under `FOR UPDATE SKIP LOCKED`.
2. If runtime simply executes an index `ensure` step every time a tenant backend is opened, the DDL remains idempotent, but stable tenants still pay unnecessary schema-check / DDL noise forever.
3. Even new Starter tenants cannot be considered upgraded merely because the new schema definition exists in code, since `StarterProvisioner.InitSchema(...)` currently validates only; if the externally provisioned Starter schema lags behind, new Starter tenants may still remain on the old baseline.

### Constraints and Decision Drivers

- This is an additive schema change. It does not involve data rewrite, column-type changes, or table rebuild; the design should reuse existing runtime seams as much as possible.
- The correctness of `ClaimSemanticTask(..., taskTypes...)` does not depend on this index, but its performance and predictability do; the migration should therefore prioritize convergence to one contract instead of tolerating permanent drift.
- `schema_version` cannot be treated as the only truth. Control-plane metadata may temporarily diverge from actual tenant DB state, so runtime schema validation must remain.
- The solution should cover both new tenants and existing tenants, without requiring a one-shot offline migration.
- The solution should avoid unconditional DDL on every backend open.

## Terminology Baseline

| Term | Meaning |
|---|---|
| tenant schema version | The control-plane version stored in `meta.tenants.schema_version`, representing which generation of data-plane schema contract the tenant has converged to |
| reconcile | An idempotent additive schema repair step for a known version gap, such as creating a missing index |
| current tenant schema version | The latest tenant schema version that the current binary supports and expects all tenants to converge to |
| typed-claim index | `semantic_tasks.idx_task_claim_type`, used by claim queries that filter by `task_type` |

This proposal fixes version semantics as follows:

- `schema_version = 1`: `semantic_tasks` exists, but `idx_task_claim_type` is not guaranteed to exist
- `schema_version = 2`: `semantic_tasks.idx_task_claim_type` has been reconciled, and the tenant has converged to the current typed-claim schema contract

## Goals

1. Provide an explicit index-upgrade path for Zero / Starter tenants that is repeatable if needed, but does not keep firing after success.
2. Make both new tenants and existing tenants converge to one typed-claim schema contract, instead of relying on two long-lived behaviors: "new schema definition" and "old tenant runtime state".
3. Turn `tenants.schema_version` from an unused field into a real control-plane migration gate.
4. Preserve tenant DB structural validation so the version gate does not replace real schema verification.

## Non-Goals

- This proposal does not introduce a generic arbitrary-DDL migration framework.
- This proposal does not handle schema migration between app embedding and auto embedding.
- This proposal does not pre-design a multi-version orchestration system for every future schema change; this iteration only covers `semantic_tasks.idx_task_claim_type`.
- This proposal does not add an offline fleet-wide backfill service or a separate operational console entrypoint.

## Architecture Overview

```text
New tenant provisioning
-----------------------
handleProvision
  -> InsertTenant(status=provisioning, schema_version=1)
  -> initTenantSchemaAsync
      -> provisioner.InitSchema(...)
          -> reconcile semantic_tasks index to v2
          -> validate target contract
      -> meta: set status=active, schema_version=2

Existing active tenant open
---------------------------
auth / semantic worker
  -> load tenant from meta (schema_version may be 1)
  -> tenant.Pool.createBackend
      -> if provider in {zero, starter} && schema_version < 2:
           reconcile semantic_tasks index
           CAS bump schema_version to 2
      -> validate target contract
      -> construct backend
```

## Design

### 1) Version definitions and ownership

Introduce explicit constants inside the `tenant` package:

- `TenantSchemaVersionBase = 1`
- `TenantSchemaVersionTypedClaimIndex = 2`
- `CurrentTenantSchemaVersion = TenantSchemaVersionTypedClaimIndex`

Ownership is split as follows:

- `meta.Store` persists `schema_version` and provides CAS-style update primitives.
- The `tenant` package defines version meaning, version-specific migration steps, and tenant DB structure validation.
- The `server` provisioning path writes the latest schema version once new-tenant schema initialization succeeds.
- `tenant.Pool` performs one version-gated lazy reconcile for old tenants on first use.

### 2) New-tenant initialization path

For new tenants, the proposal requires that a tenant becomes `active` only after the actual schema has converged to the current version.

Concrete changes:

1. `handleProvision` may still insert a `provisioning` tenant with `schema_version = 1`; at that point the tenant is not externally usable yet.
2. When `initTenantSchemaAsync(...)` succeeds, it should no longer call only `UpdateTenantStatus(..., active)`; it should atomically update:
   - `status = active`
   - `schema_version = CurrentTenantSchemaVersion`
3. To avoid a temporary mismatch where a tenant is activated but still shows the old version, add a combined update API such as `ActivateTenantWithSchemaVersion(...)`, implemented as a single SQL update in `meta.Store`.

Provider-side behavior:

- `ZeroProvisioner.InitSchema(...)`: keep the current schema creation logic, then ensure `semantic_tasks` contains the typed-claim index, then validate the latest contract.
- `StarterProvisioner.InitSchema(...)`: move from "validate only" to "safe additive reconcile + validate". The reason is that Starter's externally provisioned schema may lag behind, and validate alone cannot guarantee that new Starter tenants converge to version 2.

### 3) Lazy reconcile for existing tenants

For existing Zero / Starter tenants with `status = active`, place the migration entrypoint in `tenant.Pool.createBackend(...)`.

Execution rules:

1. If the tenant provider is not `tidb_zero` or `tidb_cloud_starter`, skip this version migration.
2. If `t.SchemaVersion >= CurrentTenantSchemaVersion`, do not migrate; just validate the target contract.
3. If `t.SchemaVersion < CurrentTenantSchemaVersion`, execute one version-specific migration for `semantic_tasks`:
   - check whether `idx_task_claim_type` exists
   - create it if missing
   - validate again after migration
4. After validate succeeds, call a CAS version-advance API on `meta.Store` to move `schema_version` from the old value to `2`

The key point here is that `schema_version` is responsible for avoiding repeated post-success `ensure` work; it is not a replacement for validating the actual DB structure.

### 4) Concrete migration boundary

This proposal defines a deliberately narrow migration surface:

- allowed migration action: add the missing `idx_task_claim_type` index to `semantic_tasks`
- disallowed migration actions: table rewrite, column type change, data backfill, or rebuilding the `files` table contract

The migration should inspect `information_schema.statistics` to determine whether the index already exists, and only then issue DDL. It should not rely only on "run `CREATE INDEX` and swallow duplicate-index errors", because one of the core goals is to reduce unnecessary schema / DDL noise on stable tenants.

### 5) Control-plane update semantics

`meta.Store` needs two new APIs:

1. `ActivateTenantWithSchemaVersion(ctx, tenantID, schemaVersion)`
   - called when provisioning succeeds
   - ensures `status=active` and `schema_version=2` are committed together
2. `CompareAndSwapTenantSchemaVersion(ctx, tenantID, from, to)`
   - called after lazy reconcile succeeds
   - SQL semantics similar to: `UPDATE tenants SET schema_version=?, updated_at=? WHERE id=? AND schema_version=?`

This handles multi-instance concurrency:

- multiple instances may run the same idempotent reconcile for one tenant concurrently
- only one instance will actually advance the control-plane version
- when other instances see 0 rows affected, they should simply re-read or continue validation; this must not be treated as an error

### 6) Failure handling and degraded-path semantics

This proposal treats "version migration failed" as "tenant schema has not converged yet", not as "control-plane version update failed".

- If additive reconcile fails:
  - do not update `schema_version`
  - backend creation fails
  - callers follow the existing error / retry path
- If reconcile succeeds but the CAS update of `schema_version` fails:
  - backend creation may still proceed as long as subsequent validate succeeds
  - because the tenant DB has already converged; failure to update the control-plane version only means a future idempotent reconcile may run once more

This tradeoff is intentional:

- the actual tenant DB structure is the ultimate source of truth
- `schema_version` is a gate to avoid repeated work, not a higher-priority authority than runtime validation

## Compatibility and Invariants

1. There is no behavior change for tenant-facing APIs, tenant auth protocol, or `semantic_tasks` task semantics.
2. The migration only adds an index; it does not modify table data or change runtime semantics around `task_id`, `receipt`, or `lease`.
3. Any Zero / Starter tenant with `schema_version >= 2` must satisfy:
   - `ValidateTiDBSchemaForMode(..., TiDBEmbeddingModeAuto)` succeeds
   - `semantic_tasks.idx_task_claim_type` exists
4. Any tenant with `schema_version < 2` must not be recorded as already converged to the current typed-claim contract.
5. `schema_version` may advance only after the actual tenant DB migration / validation succeeds; it must not be written ahead of time and then rely on async compensation.

## Alternatives Considered

### Option A: Keep the current state and only add the index to new schema definitions

Rejected. This does not cover existing Zero / Starter tenants, and it is not even reliable for new Starter tenants because `StarterProvisioner.InitSchema(...)` does not currently execute DDL.

### Option B: Run `ensure index` every time a tenant backend is opened

Rejected. The DDL can be idempotent, but this turns version migration into a permanent runtime cost, ignores the existing control-plane capability in `schema_version`, and keeps stable tenants paying unnecessary schema/DDL noise forever.

### Option C: Add a dedicated background migrator to scan and upgrade all tenants

Not selected for this iteration. It could further reduce migration latency on the first request path, but it also requires new scheduling, concurrency control, and failure-management logic. This proposal intentionally chooses the smaller production-safe option first: complete the upgrade at the two existing boundaries, provisioning and first backend open.

## Rollout Plan

- Phase A: Introduce `CurrentTenantSchemaVersion = 2`, add schema-version update APIs to `meta.Store`, and keep existing validate behavior unchanged.
- Phase B: Add `idx_task_claim_type` reconcile into Zero / Starter `InitSchema(...)`, and activate the tenant with `schema_version = 2` once schema init succeeds.
- Phase C: Add lazy reconcile + CAS version advance for `schema_version < 2` inside `tenant.Pool.createBackend(...)`, covering existing active tenants.
- Phase D: Tighten `ValidateTiDBSchemaForMode(...)` for `semantic_tasks`, so tenants with `schema_version >= 2` must satisfy the typed-claim index contract.

## Validation Strategy

- `meta` unit tests: cover success, concurrent update, and not-found paths for `ActivateTenantWithSchemaVersion(...)` and `CompareAndSwapTenantSchemaVersion(...)`.
- `tenant` schema tests: start from a version-1 TiDB auto schema, verify that reconcile can add `idx_task_claim_type`, and verify that repeated execution remains idempotent.
- `server` provisioning tests: verify that once schema init succeeds and the tenant becomes `active`, its `schema_version` is already `2`.
- `pool` tests: create an active Zero / Starter tenant with `schema_version = 1`, verify that the first `Acquire/Get` triggers reconcile and advances the version, and then verify that later opens do not repeat the work.
- Integration validation: inspect `information_schema.statistics` to verify that `semantic_tasks.idx_task_claim_type` exists, and confirm that typed-claim behavior remains unchanged in semantic-task tests.

## Risks and Mitigations

1. **Risk: control-plane version advance fails and future opens repeat reconcile.**
   - Mitigation: backend creation is gated by tenant DB validation, not by successful CAS. CAS failure only causes repeated work, not correctness failure.

2. **Risk: multiple instances migrate the same tenant concurrently.**
   - Mitigation: migration DDL is idempotent, and control-plane version advance uses CAS to avoid overwriting each other.

3. **Risk: ownership boundaries for Starter external schema are not fully clear.**
   - Mitigation: this proposal allows only a very narrow additive migration surface (single-index repair), not broader column/table rewrites.

4. **Risk: the first open of an old tenant backend pays the migration latency.**
   - Mitigation: the migration surface is only one index; if rollout later shows that first-open latency is a real issue, introduce a background tenant schema reconciler separately.

## Open Questions

1. Should a background tenant schema reconciler be introduced in a later version to pre-advance `schema_version` in bulk and further reduce first-access migration latency for existing tenants? The current answer is no, unless rollout shows that first-open latency becomes a real production issue.
