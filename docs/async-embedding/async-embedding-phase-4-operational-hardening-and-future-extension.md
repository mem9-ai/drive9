# Proposal: dat9-2 Async Embedding Phase 4 Operational Hardening and Future Extension

**Date**: 2026-03-31
**Purpose**: Turn Phase D of `async-embedding-generation-proposal.md` into a review-ready proposal for production observability, dead-letter/queue-lag diagnosis, and safe extension of the durable semantic task substrate beyond `embed`.

## Summary

Phase 4 starts after Phase 3 has already made async embedding behavior work end to end. At that point, the main gap is no longer correctness of task execution or search ranking; it is whether operators can understand what the system is doing in production, diagnose stuck or failing work quickly, and extend the same durable task substrate to adjacent semantic jobs without reworking the contract again.

This phase therefore has two tightly scoped responsibilities. First, it defines the minimum production metrics and structured logs needed for dead-letter triage, queue-lag diagnosis, and provider/runtime incident analysis. Second, it defines how the existing `semantic_tasks` contract can be extended from `embed` to future task types such as `extract_text`, `generate_l0`, and `generate_l1` without changing the lease/ack/recover fundamentals introduced earlier.

The phase should stay operationally conservative. It should not redesign the queue contract, introduce a new workflow engine, or turn semantic processing into a general-purpose orchestration platform. The goal is to harden the existing path, not to replace it.

## Context

### Current State

The current repository already has the core Phase 3 execution path in place:

- `pkg/server/semantic_worker.go` starts background workers, claims `embed` tasks, retries failures, and runs `RecoverExpiredSemanticTasks`
- `pkg/datastore/semantic_tasks.go` provides durable task primitives such as `Enqueue`, `Claim`, `Ack`, `Retry`, and `RecoverExpired`
- `pkg/backend/dat9.go` and `pkg/datastore/search.go` already make current-revision vectors queryable end to end

The repository also already contains a generic process-wide metrics surface through `pkg/metrics/operations.go`:

- `metrics.RecordOperation(component, operation, result, d)` records counters and duration histograms
- `metrics.RecordGauge(component, name, value)` records gauges

That metrics API is already used by the in-memory image extraction pipeline:

- `pkg/backend/options.go` records `image_extract` worker count, queue capacity, and queue depth gauges
- `pkg/backend/image_extract.go` records `enqueue`, `process`, and `fallback` outcomes with result labels such as `ok`, `queue_full`, `extract_error`, `stale`, and `update_error`

By contrast, the semantic worker path is only partially instrumented today:

- `pkg/server/semantic_worker.go` records `metrics.RecordOperation("semantic_worker", string(task.TaskType), result, duration)` for task outcomes
- the same file emits structured logs for ack, retry, recovery, and certain failure paths
- there are no queue backlog gauges, no queue-lag metric, no semantic-worker inflight gauge, and no stable metric naming guidance for production dashboards

The log surface is also incomplete for operational debugging:

- existing logs already include fields such as `tenant_id`, `task_id`, `resource_id`, `resource_version`, and sometimes `attempt_count`
- but the coverage is not systematic across claim, writeback success, retry scheduling, dead-letter transitions, and recovery sweeps
- the current code does not yet define a stable "minimum required fields" contract for production diagnosis

Finally, the task substrate itself is still intentionally narrow:

- `pkg/semantic/task.go` currently defines only `TaskTypeEmbed`
- image extraction remains a best-effort in-memory producer that bridges into durable `embed`
- `.abstract.md` / `.overview.md` generation is still a future capability described in design docs, not in the runtime contract

### Problem Statement

After Phase 3, async embedding can succeed, fail, retry, and recover correctly, but production operation is still under-specified. Without Phase 4:

1. operators cannot quickly answer whether the system is keeping up with work or accumulating lag
2. dead-lettered tasks are visible in SQL, but there is no agreed minimal signal set for diagnosis
3. provider failures and runtime failures are mixed together without a stable metrics/logging contract
4. future task types risk growing ad hoc because there is no explicit extension boundary for the durable task substrate

In other words, the system can execute semantic work, but it is not yet hardened as an observable, supportable, and safely extensible production subsystem.

### Constraints and Decision Drivers

- Phase 4 must preserve the current durable task contract; `Claim`/`Ack`/`Retry`/`RecoverExpired` stay the foundation
- observability should align with the existing `pkg/metrics` API and, where practical, with the naming style already used by `image_extract`
- the minimum metric/log set should be enough for real diagnosis, not a large speculative telemetry matrix
- hot-path logging must remain selective; not every normal loop iteration should produce noisy logs
- task-type extension should reuse the same `semantic_tasks` table and task model rather than adding one table per task family
- Phase 4 should not block provider-neutral runtime work, but it should not pretend that db9/Postgres multi-tenant execution is already solved

## Goals

1. Define the minimum production metrics needed to diagnose queue lag, retries, recovery, success/failure mix, and dead-letter accumulation.
2. Define the minimum structured log fields and log points needed for dead-letter triage and task-lifecycle reconstruction.
3. Keep observability naming aligned with the existing generic `pkg/metrics` surface and `image_extract` conventions where practical.
4. Define how the current durable semantic task substrate extends from `embed` to `extract_text`, `generate_l0`, and `generate_l1` without redesigning leases and receipts.
5. Keep the design small enough to be implemented as a sequence of reviewable, low-risk follow-up changes.

## Non-Goals

- Do not replace `pkg/metrics` with a different telemetry framework in this phase.
- Do not add a full task-management API, dashboard server, or replay UI.
- Do not migrate image extraction to a durable task in the same change as metrics/logging hardening.
- Do not implement L0/L1 generation itself in this phase; only define how it should fit the task substrate.
- Do not solve provider-neutral multi-tenant store opening in this phase.

## Architecture Overview

```text
Operational Signals
-------------------
semantic_worker / semantic_tasks
    -> generic service metrics (operation counters + duration histograms)
    -> semantic queue gauges (backlog, inflight, lag, dead-letter count)
    -> structured task lifecycle logs

Future Task Types
-----------------
semantic_tasks table
    -> embed
    -> extract_text
    -> generate_l0
    -> generate_l1
    -> same Claim/Ack/Retry/RecoverExpired semantics
    -> task-specific handler chosen by task_type
```

## Design

### 1) Minimum production metrics

Phase 4 should standardize the minimum required metrics into two layers.

#### Layer A: task lifecycle counters and durations

Continue using `metrics.RecordOperation(...)` and treat `component="semantic_worker"` as the main namespace. The minimal operation/result matrix should be:

- `operation="embed"`, `result="ok"`
- `operation="embed"`, `result="obsolete"`
- `operation="embed"`, `result="embed_error"`
- `operation="embed"`, `result="writeback_error"`
- `operation="embed"`, `result="embed_empty"`
- `operation="embed"`, `result="unsupported"`

Add dedicated operation names for non-task-loop actions that matter operationally:

- `operation="claim"`, result labels such as `ok`, `empty`, `error`
- `operation="retry"`, result labels such as `scheduled`, `error`, `dead_lettered`
- `operation="recover"`, result labels such as `ok`, `error`

This keeps the naming consistent with the generic metrics API while making dashboard queries much easier than inferring every behavior from the current mixed result labels.

#### Layer B: queue state gauges

Phase 4 should add semantic-worker gauges analogous to `image_extract.queue_depth` and `image_extract.workers`. The minimum gauge set is:

- `component="semantic_worker", name="workers"`
- `component="semantic_worker", name="inflight"`
- `component="semantic_worker", name="queued"`
- `component="semantic_worker", name="processing"`
- `component="semantic_worker", name="dead_lettered"`
- `component="semantic_worker", name="queue_lag_seconds"`

Definitions:

- `workers`: configured worker count currently running in the process
- `inflight`: currently claimed tasks being processed by live workers
- `queued`: tasks whose `status='queued'`
- `processing`: tasks whose `status='processing'`
- `dead_lettered`: tasks whose `status='dead_lettered'`
- `queue_lag_seconds`: current time minus the oldest claimable task's `available_at`; zero when no claimable work exists

`queue_lag_seconds` is the most important new gauge because it answers the operator question that raw counts cannot: whether work is simply present or actually late.

### 2) How to collect gauges without a new telemetry subsystem

Phase 4 should stay within the current architecture and add a small amount of datastore support instead of inventing a separate observer service.

Recommended approach:

- add a small datastore helper that returns semantic task state counts for one tenant database
- add a small datastore helper that returns the oldest claimable `available_at` for one tenant database
- in `semanticWorkerManager`, run a lightweight periodic observation sweep next to `recoverLoop`
- for local fallback mode, observe the fallback tenant store directly
- for multi-tenant mode, aggregate counts across active tenants scanned in that round

This keeps the data model authoritative in SQL and avoids trying to reconstruct queue state from logs or in-process maps only.

The observation sweep should be best-effort. If one tenant store cannot be opened, log the error and continue rather than failing task execution.

### 3) Minimum structured log contract

Phase 4 should define a stable set of fields and require them on the key lifecycle logs.

Minimum field set:

- `tenant_id`
- `task_id`
- `task_type`
- `resource_id`
- `resource_version`
- `receipt`
- `attempt_count`
- `result`
- `message` or `error`

Recommended optional fields when available:

- `retry_at`
- `lease_until`
- `embedding_dim`
- `provider_model`
- `recovered`

Minimum required log points:

1. claim success
2. writeback success / ack success
3. retry scheduled
4. dead-letter transition
5. recover sweep success when at least one task was recovered
6. claim/open-store/recover errors

The purpose of this contract is not to log every normal branch. It is to make one task's lifecycle reconstructible across claim, retry, and dead-letter transitions without opening the database first.

### 4) Decision: minimum observability set for production triage

The Phase 3 open question about observability should be resolved here.

The minimum production set is:

- metrics: lifecycle counters/durations for claim/retry/recover/embed plus gauges for workers, inflight, queued, processing, dead_lettered, and queue_lag_seconds
- logs: structured claim, ack/writeback success, retry scheduling, dead-letter, and recovery logs with the minimum field set above

Anything beyond that, such as per-model dashboards, tenant-specific alert rules, or replay tooling, is a later optimization rather than Phase 4 scope.

### 5) Extending the durable task substrate beyond `embed`

Phase 4 should make the extension boundary explicit before any new task type is implemented.

The durable substrate should remain shared across task types:

- keep one `semantic_tasks` table
- keep one `semantic.Task` model
- add new `TaskType` constants in `pkg/semantic/task.go`
- dispatch by `task.TaskType` in the worker manager or a small handler registry
- keep claim/ack/retry/recover semantics identical across task types

Planned future task types:

- `extract_text`: durable content-text extraction for files that currently depend on best-effort image extract
- `generate_l0`: generate or refresh `.abstract.md`
- `generate_l1`: generate or refresh `.overview.md`

The key design rule is that task-type expansion should change only handler logic and enqueue policy, not delivery semantics.

### 6) Handler contract for future task types

To avoid ad hoc branching as task types grow, Phase 4 should define a small internal handler contract, even if only `embed` uses it at first:

```go
type TaskHandler interface {
    TaskType() semantic.TaskType
    Handle(ctx context.Context, tenantID string, store *datastore.Store, task *semantic.Task) TaskOutcome
}
```

`TaskOutcome` should remain intentionally small and should map back to the existing datastore delivery actions:

- `ack`
- `retry`
- `obsolete`

This is not a workflow engine. It is only a way to keep `semanticWorkerManager` from turning into a long `switch` statement once `extract_text`, `generate_l0`, and `generate_l1` arrive.

### 7) Explicit boundary for Phase 4

Phase 4 should harden the current runtime path, not broaden it silently.

That means:

- keep the current MySQL/TiDB execution path as the primary supported runtime
- continue to record provider-neutral store opening as separate follow-up work
- do not mix durable `extract_text` migration and L0/L1 generation implementation into the same initial metrics/logging hardening change

## Compatibility and Invariants

- Task delivery remains at-least-once.
- Receipts remain mandatory for ack/retry correctness.
- `embed` remains the only task type that must be fully production-ready at the start of Phase 4.
- Queue observation must not mutate queue state.
- Logs and metrics must describe current task state; they must not become an alternative source of truth.
- Future task types must reuse the same durable substrate rather than forking a separate queue system.

## Incremental Plan

### Step 1: Observability helpers

1. add datastore helpers for semantic task state counts and oldest claimable `available_at`
2. add a semantic-worker observation sweep that periodically emits gauges
3. align metric names with the existing `pkg/metrics` conventions

### Step 2: Structured logging hardening

4. standardize the minimum log field set for claim, ack, retry, dead-letter, and recovery events
5. add explicit logs for dead-letter transitions and successful claims
6. make provider/runtime errors distinguishable in retry/dead-letter logs

### Step 3: Substrate extension boundary

7. define new `TaskType` constants for `extract_text`, `generate_l0`, and `generate_l1` without implementing handlers yet
8. introduce a small handler dispatch seam so future task types do not expand the worker inline
9. document enqueue preconditions and idempotency expectations per future task type

### Step 4: Validation and rollout

10. add tests for gauge helper queries and observation sweeps
11. add tests for dead-letter logging/metrics behavior where practical
12. roll out Phase 4 first for `embed`, then reuse the substrate for future task handlers in separate follow-up phases

## Validation Strategy

- **Metrics tests**
  - queue state helper returns correct counts for `queued`, `processing`, and `dead_lettered`
  - oldest claimable `available_at` is converted into `queue_lag_seconds` correctly
  - local fallback mode and multi-tenant aggregation both emit stable gauges

- **Log contract tests**
  - retry scheduling logs include `tenant_id`, `task_id`, `resource_id`, `resource_version`, `attempt_count`, and `retry_at`
  - dead-letter transitions are visible and distinguishable from ordinary retries
  - claim success and ack success logs include the minimum task identity fields

- **Operational smoke**
  - forced provider failure increments retry/dead-letter metrics and emits the expected logs
  - pausing workers causes queue lag to rise while queued count increases
  - restarting workers after a stuck claim leads to recovery metrics and recovery logs

- **Extension-boundary tests**
  - handler dispatch rejects unknown task types predictably
  - new task types can be added without changing datastore delivery semantics

## Risks and Mitigations

1. **Metrics naming may drift from the existing codebase style** - Keep the implementation on top of `pkg/metrics.RecordOperation` and `RecordGauge`, and mirror `image_extract` naming patterns where practical.
2. **Observation queries may become expensive on large queues** - Limit Phase 4 to simple counts and oldest-available queries backed by existing task indexes, and keep the sweep interval modest.
3. **Logs may become too noisy** - Restrict required logs to claim, ack/writeback success, retry, dead-letter, and recovery; do not log every empty poll.
4. **Future task-type support may overcomplicate the current worker** - Introduce only a small internal handler seam rather than a general workflow abstraction.
5. **Phase 4 may expand into durable `extract_text` migration immediately** - Keep this proposal explicit that observability hardening and extension-boundary definition come first; durable `extract_text` should remain a separate follow-up implementation proposal.

## Decisions

1. The minimum production observability set for async embedding consists of lifecycle counters/durations for `claim`, `retry`, `recover`, and `embed`, plus gauges for `workers`, `inflight`, `queued`, `processing`, `dead_lettered`, and `queue_lag_seconds`.
2. Structured logs must include `tenant_id`, `task_id`, `task_type`, `resource_id`, `resource_version`, `receipt`, `attempt_count`, and `result`/`error` at the key lifecycle points.
3. Future semantic work types should reuse the same `semantic_tasks` table and delivery contract rather than introducing separate task tables or a new queue engine.
4. Phase 4 should use an internal static dispatch table such as `handlers := map[semantic.TaskType]TaskHandler{...}` for task-type routing. Explicit registration is deferred until the second or third concrete task type lands and test/deployment wiring actually needs replaceable handler composition.
5. Future task types should begin with fixed internal concurrency budgets. The exact scheduling algorithm and any external configuration interface are deferred to a later implementation proposal.

## Open Questions

No open questions are currently required for Phase 4 scope. Handler routing and initial concurrency-budget policy are now pinned as decisions, while detailed scheduling and configuration shape remain deferred to future implementation proposals.

## References

- `dat9-2/docs/async-embedding/async-embedding-generation-proposal.md`
- `dat9-2/docs/async-embedding/async-embedding-phase-3-worker-and-search-correctness.md`
- `dat9-2/pkg/server/semantic_worker.go`
- `dat9-2/pkg/semantic/task.go`
- `dat9-2/pkg/datastore/semantic_tasks.go`
- `dat9-2/pkg/backend/image_extract.go`
- `dat9-2/pkg/backend/options.go`
- `dat9-2/pkg/metrics/operations.go`
