# Proposal: dat9-2 Async Embedding Phase A

**Date**: 2026-03-30
**Purpose**: Turn Phase A of `async-embedding-generation-proposal.md` into a directly executable design document, with clear schema, datastore contracts, test coverage, and implementation order.

## Summary

Phase A only covers "contract hardening and schema preparation." It does not change the current write path, search path, worker lifecycle, or introduce a real embedding provider. The goal of this phase is to lay down all static foundations required by later async embedding work in one pass: new tenant schema no longer depends on generated `EMBED_TEXT(...)`, tenant DBs gain a durable `semantic_tasks` table, `pkg/datastore` can read `embedding_revision` and operate semantic tasks, and the test baseline covers durable-contract semantics such as receipt / lease / recover.

To control blast radius, Phase A intentionally keeps the principle of "capability landed, behavior still disabled by default." In this phase, `pkg/backend/dat9.go`, `pkg/backend/upload.go`, and `pkg/datastore/search.go` do not switch over to the new path. They only gain a stable data model and SQL contract that Phase B/C can build on.

## Context

### Current State

At the moment, embedding-related infrastructure in `dat9-2` is spread across three layers:

1. `pkg/tenant/schema_zero.go`, `pkg/tenant/schema_starter.go`, and `pkg/tenant/schema_db9.go` all define `files.embedding` as `GENERATED ALWAYS AS (EMBED_TEXT(...))`.
2. The `File` struct and scan helpers in `pkg/datastore/store.go` only cover fields such as `content_text`, `revision`, and `status`; there is no representation of `embedding_revision` or task state yet.
3. The repository still has no durable task substrate. The only existing async capability is the in-memory worker in `pkg/backend/image_extract.go`, which performs revision-gated writeback through `UpdateFileSearchText(ctx, fileID, revision, text)`, but has no claim / ack / recover semantics.

The directly relevant code boundaries for Phase A are:

- tenant initial schema: `pkg/tenant/schema_zero.go`, `pkg/tenant/schema_starter.go`, `pkg/tenant/schema_db9.go`
- datastore core model and query projections: `pkg/datastore/store.go`
- datastore/schema helper tests: `pkg/datastore/schema_test_helper_test.go`, `pkg/server/schema_test_helper.go`, `pkg/client/schema_test_helper.go`, `pkg/backend/schema_test_helper.go`
- provider initialization entry points: `pkg/tenant/zero.go`, `pkg/tenant/starter.go`, `pkg/tenant/db9.go`

Another fact that needs to be made explicit is that the current repository has no tenant data-plane migration runner. `InitSchema()` is only executed when creating a new tenant database. There is no unified framework today for migrating schema on already-existing tenants. Based on that, Phase A makes a more direct tradeoff: schema changes in this phase are treated as a breaking change. It guarantees only that the new schema is correct, does not cover old-tenant data migration, and does not introduce a generic tenant migration runner.

### Problem Statement

If implementation jumps straight into Phase B/C before completing Phase A, later work will be blocked by three issues at the same time:

1. the schema still assumes the database can generate embeddings automatically, so the application layer cannot safely take over vector state
2. datastore has neither a task model nor revision-aware state such as `embedding_revision`, so later worker/search code has no unified read/write surface
3. the durable queue contract has not yet been landed in dat9's own store layer, so once Phase B starts refactoring the write path, file persistence and async task registration are likely to become a half-finished mix

The value of Phase A is not "implement part of embedding functionality ahead of time." Its value is fixing the contracts that later phases actually depend on, so Phase B/C do not have to keep reworking schema, SQL, and test semantics.

### Constraints and Decision Drivers

- This phase must not change the external behavior of `PUT /v1/fs`, multipart upload, or `grep`.
- New contracts must work for both TiDB/MySQL providers and db9/Postgres providers.
- The current repo has no generic migration framework, so Phase A explicitly excludes existing-tenant migration and treats the related schema changes as a breaking change.
- The durable queue contract should stay as close as possible to QueueFS's already-validated claim / ack / recover semantics, but without introducing QueueFS's filesystem control surface.
- `pkg/datastore/store.go` is already the center of SQL access, so Phase A should prefer reusing this seam instead of introducing a new database abstraction layer too early.
- Nothing currently needs to read embedding vectors themselves on the Go side. Phase A should avoid prematurely committing to a cross-driver vector codec for "possible future use."

## Goals

1. New tenant schema clearly supports application-managed embedding state rather than generated `EMBED_TEXT(...)`.
2. Tenant DBs contain a testable durable `semantic_tasks` contract covering `Enqueue`, `Claim`, `Ack`, `Retry`, and `RecoverExpired`.
3. `pkg/datastore` can read `embedding_revision` and expose the data model and error semantics needed for semantic tasks.
4. After landing, all Phase A changes remain dormant and do not alter current request-path behavior.
5. Phase B/C can directly implement write-path integration, workers, and search changes on top of these contracts without changing schema shape again.

## Non-Goals

- Do not connect any embedding provider in Phase A.
- Do not refactor create / overwrite transactions in `pkg/backend/dat9.go` in Phase A.
- Do not start `SemanticWorkerManager` in Phase A.
- Do not change query embedding in `pkg/datastore/search.go` in Phase A.
- Do not convert the existing image extract worker into a durable pipeline in Phase A.

## Design

### 1) Scope Cut: land dormant contracts first

The core principle of Phase A is "lock the database and store contracts first, then wire in behavioral changes later."

```text
Phase A
-------
tenant schema init/test helpers
    -> mutable embedding columns
    -> semantic_tasks table
datastore
    -> file model adds embedding_revision
    -> semantic task SQL contract
tests
    -> schema shape
    -> durable queue semantics

No behavior change yet
----------------------
backend write path unchanged
image extract path unchanged
search path unchanged
server worker manager unchanged
```

That means the done signal for Phase A is not "embedding now works." It is:

- the schema now contains the capabilities required later
- datastore now contains the future call points
- tests have pinned down the durable contract

### 2) Tenant schema changes

#### 2.1 Files table: stop generated embedding

For all three providers, adopt a unified "writable embedding column + revision marker" model for the `files` table:

- TiDB Zero / TiDB Cloud Starter:

```sql
embedding           VECTOR(1024) NULL,
embedding_revision  BIGINT NULL,
```

- db9/Postgres:

```sql
embedding           vector(1024),
embedding_revision  BIGINT,
```

Keep the existing vector-index name `idx_files_cosine`, but from now on it indexes a normal writable column instead of the result of a generated expression.

This step only requires that the schema allow future embedding write/clear operations. Phase A itself does not need to write vectors through Go code paths.

#### 2.2 Add `semantic_tasks` table in every tenant schema

`semantic_tasks` is the tenant-local durable substrate. Keep the task shape from the main proposal, and make provider-specific DDL explicit in Phase A.

TiDB/MySQL version:

```sql
CREATE TABLE IF NOT EXISTS semantic_tasks (
    task_id           VARCHAR(64) PRIMARY KEY,
    task_type         VARCHAR(32) NOT NULL,
    resource_id       VARCHAR(64) NOT NULL,
    resource_version  BIGINT NOT NULL,
    status            VARCHAR(20) NOT NULL,
    attempt_count     INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 5,
    receipt           VARCHAR(128) NULL,
    leased_at         DATETIME(3) NULL,
    lease_until       DATETIME(3) NULL,
    available_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    payload_json      JSON NULL,
    last_error        TEXT NULL,
    created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    completed_at      DATETIME(3) NULL,
    UNIQUE KEY uk_task_resource_version (task_type, resource_id, resource_version),
    KEY idx_task_claim (status, available_at, lease_until, created_at)
)
```

db9/Postgres version:

```sql
CREATE TABLE IF NOT EXISTS semantic_tasks (
    task_id           VARCHAR(64) PRIMARY KEY,
    task_type         VARCHAR(32) NOT NULL,
    resource_id       VARCHAR(64) NOT NULL,
    resource_version  BIGINT NOT NULL,
    status            VARCHAR(20) NOT NULL,
    attempt_count     INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 5,
    receipt           VARCHAR(128),
    leased_at         TIMESTAMPTZ,
    lease_until       TIMESTAMPTZ,
    available_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload_json      JSONB,
    last_error        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at      TIMESTAMPTZ,
    UNIQUE (task_type, resource_id, resource_version)
);

CREATE INDEX IF NOT EXISTS idx_task_claim
    ON semantic_tasks (status, available_at, lease_until, created_at);
```

State strings are fixed to lowercase in Phase A: `queued`, `processing`, `succeeded`, `failed`, `dead_lettered`. Lowercase is chosen to stay aligned with QueueFS durable concepts such as `pending/processing`, while avoiding confusion with the existing domain enums used by `files.status` and `uploads.status`.

In Phase A, `semantic_tasks` keeps only the fields required for delivery correctness. Do not add runtime attribution columns such as `last_claimed_by`. For now, observability should be carried primarily through worker logs and metrics, not by writing high-cardinality runtime identity into the task table.

#### 2.3 Breaking-change boundary for existing tenants

Because the repo currently has no tenant data-plane migration runner, Phase A defines the boundary for existing tenants directly as a breaking change:

- **Must do**: switch all `InitSchema()` paths and all test schema helpers to the new schema
- **Explicitly not doing**: old-tenant data migration, compatibility schema, or a background migrator that scans and upgrades already-existing tenant DBs
- **Explicit impact**: tenants created under the old schema are outside the compatibility scope of this proposal unless additional manual handling is performed

This allows Phase A to land a verifiable contract inside this repository first, without prematurely designing migration machinery before the schema contract itself is stable.

### 3) Datastore model and query surface

#### 3.1 `File` model: add `EmbeddingRevision`, defer raw vector codec

`pkg/datastore.File` should add only the portable state directly needed by Phase A:

```go
type File struct {
    ...
    ContentText        string
    EmbeddingRevision  *int64
    CreatedAt          time.Time
    ...
}
```

Phase A intentionally does **not** introduce `Embedding []float32`, `[]byte`, or a provider-specific vector wrapper, for two reasons:

1. no current code path needs to read vector bodies in the application layer
2. the repository does not yet contain any established vector scan/encode contract for `mysql` / `pgx`, so Phase A should not lock in a codec shape prematurely

Phase A only requires that `embedding_revision` be readable as a first-class field. The actual embedding writeback helper should land in Phase C together with the provider.

#### 3.2 Scan helpers and projections to update

The following SQL projections all need to include `embedding_revision` in their read surface, otherwise Phase B/C will hit the mismatch of "schema has the field, but store cannot read it":

- `GetFile()`
- `ListDir()`
- orphaned-file projection returned by `DeleteFileWithRefCheck()`
- orphaned-file projection returned by `DeleteDirRecursive()`
- `scanFileWithBlob()`
- `scanFileNoBlob()`
- `scanNodeWithFileWithBlob()`
- `scanNodeWithFileNoBlob()`

These changes should be completed together with the `File` struct extension, plus a set of store tests that verify the new field survives through `InsertFile` / `GetFile` / `Stat` / `ListDir`.

#### 3.3 Insert/update API scope

Phase A does not require `InsertFile()` / `UpdateFileContent()` to immediately support embedding writes, but it should leave a clear place for future expansion:

- `InsertFile()` continues to accept no embedding parameter and defaults to `embedding=NULL`, `embedding_revision=NULL`
- `UpdateFileContent()` continues to maintain only existing fields
- reserve a dedicated embedding-state helper location inside `pkg/datastore`, for example Phase C's future `UpdateFileEmbedding(...)`

This avoids refactoring the current write path in Phase A merely for the convenience of future writeback.

### 4) Semantic task contract ownership

#### 4.1 Package split

Recommended ownership in Phase A:

- `pkg/semantic`: task enums, request/response structs, and error semantics only
- `pkg/datastore/semantic_tasks.go`: the actual SQL implementation, exposed as a new set of `Store` methods

Do not create a generalized `pkg/queue` in Phase A. The only confirmed consumer today is semantic indexing, and an early abstraction would just add an unnecessary adaptation layer for later behavior changes.

#### 4.2 Proposed API surface

In `pkg/semantic`, the following types should be fixed:

```go
type TaskType string

const (
    TaskTypeEmbed TaskType = "embed"
)

type TaskStatus string

const (
    TaskQueued       TaskStatus = "queued"
    TaskProcessing   TaskStatus = "processing"
    TaskSucceeded    TaskStatus = "succeeded"
    TaskFailed       TaskStatus = "failed"
    TaskDeadLettered TaskStatus = "dead_lettered"
)

type Task struct {
    TaskID          string
    TaskType        TaskType
    ResourceID      string
    ResourceVersion int64
    Status          TaskStatus
    AttemptCount    int
    MaxAttempts     int
    Receipt         string
    LeasedAt        *time.Time
    LeaseUntil      *time.Time
    AvailableAt     time.Time
    PayloadJSON     []byte
    LastError       string
    CreatedAt       time.Time
    UpdatedAt       time.Time
    CompletedAt     *time.Time
}

type ClaimResult struct {
    Task  Task
    Found bool
}
```

Corresponding methods in `pkg/datastore` are recommended as:

- `EnqueueSemanticTask(ctx, task) (created bool, err error)`
- `ClaimSemanticTask(ctx, now, leaseDuration) (*semantic.Task, bool, error)`
- `AckSemanticTask(ctx, taskID, receipt) error`
- `RetrySemanticTask(ctx, taskID, receipt, retryAt, lastErr) error`
- `RecoverExpiredSemanticTasks(ctx, now, limit) (int, error)`

Among them:

- `Enqueue` uses the unique key `(task_type, resource_id, resource_version)` for dedupe; duplicate enqueue returns `created=false`, not an error
- `Claim` uses `FOR UPDATE SKIP LOCKED`, sets `status=processing`, a new `receipt`, `leased_at`, `lease_until`, and increments `attempt_count` at claim time
- `Ack` accepts only the current receipt; on success it changes status to `succeeded` and writes `completed_at`
- `Retry` accepts only the current receipt; when not dead-lettered, it resets status to `queued` and updates `available_at` and `last_error`
- `RecoverExpired` only handles tasks where `status=processing` and `lease_until < now`, resetting them to `queued` and clearing receipt/lease fields

This API is smaller than QueueFS because dat9 Phase A needs only two result paths: "confirmed success" and "failed retry." `Release` semantics can be covered by `Retry(..., retryAt=now)`.

#### 4.3 Error model

To avoid continued string matching in later call sites, Phase A should define at least two sentinel errors in `pkg/datastore` or `pkg/semantic`:

- `ErrTaskLeaseMismatch`: receipt does not match or the claim has expired
- `ErrTaskNotFound`: task does not exist

`Ack` / `Retry` should return `ErrTaskLeaseMismatch` for stale receipts. That directly affects how Phase B/C workers treat late ack as no-op versus hard failure.

### 5) Test strategy as a first-class deliverable

Tests in Phase A are not "add a few unit tests." They are one of the main deliverables of the phase. Recommended structure:

#### 5.1 Schema shape tests

Goal: prove that the new schema has already removed the generated-embedding dependency, and that all test helpers match tenant init schema.

Suggested additions/extensions:

- `pkg/datastore/schema_test.go`
  - verify that provider split still preserves the `content_blob` difference
  - verify that `files.embedding_revision` exists
  - verify that `semantic_tasks` exists
- provider-specific schema smoke
  - executing `UPDATE files SET embedding = NULL, embedding_revision = 7 ...` on the new schema should succeed
  - this proves `embedding` is no longer a generated column, even though real vector writeback logic does not exist yet

#### 5.2 Datastore projection tests

Goal: prove the new `File` field is consistently visible across common read paths.

Suggested coverage:

- `InsertFile` / `GetFile`
- `Stat`
- `ListDir`
- `DeleteFileWithRefCheck`
- `DeleteDirRecursive`

The minimal assertion is that `embedding_revision` is not lost during scanning in any of these paths.

#### 5.3 Durable task contract tests

Goal: translate the most critical durable lifecycle tests from QueueFS into dat9's store contract.

Must cover:

- enqueue -> claim -> ack happy path
- duplicate enqueue for the same `(task_type, resource_id, resource_version)` returns `created=false`
- wrong receipt ack fails with `ErrTaskLeaseMismatch`
- claim order respects `available_at`, then `created_at`
- lease expiration + `RecoverExpired` makes the task claimable again
- reopen store after claim, then verify recover still works

If the test environment allows it, multi-provider contract tests should share the same semantic assertions and swap only the schema/bootstrap.

## Incremental Plan

### Workstream A: Schema contract

1. modify `pkg/tenant/schema_zero.go`, `pkg/tenant/schema_starter.go`, and `pkg/tenant/schema_db9.go`
2. change `files.embedding` to a normal nullable column and add `embedding_revision`
3. add `semantic_tasks` DDL and the claim index
4. keep the existing `idx_files_cosine` name unchanged, so later search SQL and operational terminology do not fork

### Workstream B: Test schema helpers

5. update `pkg/datastore/schema_test_helper_test.go`
6. update `pkg/server/schema_test_helper.go`
7. update `pkg/client/schema_test_helper.go`
8. update `pkg/backend/schema_test_helper.go`

### Workstream C: Datastore model seam

9. extend `pkg/datastore.File`
10. update `scanFile*` / `scanNodeWithFile*` / related `SELECT`s
11. add store tests proving that `embedding_revision` consistently survives common read paths

### Workstream D: Semantic task substrate

12. add task types / errors in `pkg/semantic`
13. add `pkg/datastore/semantic_tasks.go`
14. implement `Enqueue`, `Claim`, `Ack`, `Retry`, and `RecoverExpired`
15. keep one shared semantic test suite for TiDB/MySQL and db9/Postgres

### Workstream E: Breaking-change note

16. mark the schema change explicitly as a breaking change in the proposal
17. make it explicit that existing-tenant migration is outside the scope of this phase

## Validation Strategy

- schema tests verify `files.embedding` is writable, `embedding_revision` exists, and `semantic_tasks` exists
- datastore tests verify the new `File` field is preserved in `GetFile`, `Stat`, `ListDir`, and delete return values
- task contract tests verify receipt-based ack, lease recovery, claim order, and restart recovery
- targeted package tests should cover at least `pkg/datastore/...`, `pkg/tenant/...`, and new `pkg/semantic/...`
- final acceptance is: after Phase A lands, no backend/search code changes are required, and existing e2e/unit-test behavior remains unchanged

## Risks and Mitigations

1. **Schema helpers drift from the real tenant schema** - make helper updates a required Phase A task and validate both real schema init and helper schema in schema tests.
2. **Introducing a vector codec too early creates provider coupling** - Phase A adds only `embedding_revision` reads and does not read vector payloads on the Go side.
3. **The durable queue becomes an over-generalized abstraction** - use the minimal split of `pkg/semantic` + `pkg/datastore` first, without introducing a general `pkg/queue`.
4. **The schema breaking change blocks compatibility for old tenants** - explicitly state in the document that old-tenant migration is out of scope, and treat this impact as an accepted scope constraint rather than a hidden assumption.
5. **Observability needs get prematurely baked into the task table** - let `semantic_tasks` carry only delivery contract first; fill worker attribution through logs/metrics, then decide later whether more columns are actually needed.
6. **Phase A accidentally triggers current behavior changes** - keep backend write path, server workers, and search path entirely disconnected from the new contract; add only dormant schema/store/test infrastructure.
