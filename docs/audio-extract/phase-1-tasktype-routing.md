# Proposal: Converge semantic worker capability routing onto `TaskType` sets (Audio Extract Phase 1 prerequisite)

**Date**: 2026-04-10  
**Purpose**: Based on the current `drive9` codebase and Design item 8 in `docs/audio-extract-text-proposal.md`, this proposal defines an independently reviewable prerequisite phase: before introducing `audio_extract_text`, first converge `semantic worker` capability routing from image/embed-specific booleans to `semantic.TaskType` sets.

## Summary

The current `semantic worker` already provides durable `claim / ack / retry / recover` semantics, but its routing entry points are still organized around image/embed-specific capability booleans. That structure is still manageable when only `embed` and `img_extract_text` exist, but if `audio_extract_text` is added directly on top of it, the implementation will naturally drift toward more parallel branches such as:

- `hasImageHandler()`
- `hasAudioHandler()`
- `allowedImgTaskTypes`
- `allowedAudioTaskTypes`

That would make the worker capability model diverge further from the `semantic.TaskType` model. Every new durable semantic task would require another round of duplicated routing branches.

This proposal therefore pulls Design item 8 from `docs/audio-extract-text-proposal.md` forward and treats it as an explicit Phase 1 prerequisite for the broader audio design. The decisions are:

1. Express `semantic worker` routing capability uniformly as `[]semantic.TaskType`.
2. Whether a target can be scanned, whether it can be claimed, and which task types it can claim should all be derived from task type sets rather than from image/embed-specific boolean names.
3. `dispatchTask()` remains an explicit `switch task.TaskType`; this phase does not introduce a generic handler registry or a workflow engine.
4. This phase does not implement `audio_extract_text` itself. It only removes the audio design's dependency on image-specific routing.

The goal is not to add abstraction for its own sake. The goal is to isolate and stabilize the worker contract that must exist before audio can land, with the smallest change surface possible.

## Context

### Verified current state

The repository already has durable semantic task infrastructure:

- `pkg/semantic/task.go` defines `semantic.TaskType`
- `pkg/datastore/semantic_tasks.go` already supports filtered claims via `ClaimSemanticTask(...)` with one or more `task_type` values
- `pkg/server/semantic_worker.go` already implements the worker loop, lease renew, ack, retry, and recover

However, the worker's capability routing is still task-specific:

1. `pkg/server/semantic_worker.go`
   - `hasEmbedHandler()` only expresses whether an app-managed embed handler exists
   - `hasImageHandler()` only expresses whether image extraction runtime exists
   - `supportsTenantProvider(...)` checks only image capability for auto providers and only embed capability for non-auto providers
   - `shouldIncludeFallback()` follows the same image/embed split
   - `allowedTaskTypesForTarget(...)` currently returns only:
     - `[]semantic.TaskType{semantic.TaskTypeImgExtractText}`
     - `[]semantic.TaskType{semantic.TaskTypeEmbed}`
     - or `nil`

2. `dispatchTask(...)` in `pkg/server/semantic_worker.go` is still explicit dispatch, but it only supports:
   - `semantic.TaskTypeEmbed`
   - `semantic.TaskTypeImgExtractText`

3. `SupportsAsyncImageExtract()` in `pkg/backend/image_extract.go` exposes image runtime capability as a dedicated boolean, and the worker's auto-provider routing is currently built on top of that boolean.

4. In addition to `embed` and `img_extract_text`, `pkg/semantic/task.go` already also defines:
   - `generate_l0`
   - `generate_l1`

This means the repository's durable task vocabulary is already larger than two task types, while the worker capability vocabulary is still limited to the two historical image/embed branches.

### Problem statement

If audio support is added using the current pattern, the most likely implementation path is to duplicate another audio-specific routing branch inside the worker:

- add audio-specific conditions when scanning tenants
- add audio-specific conditions to fallback inclusion rules
- add another audio branch in `allowedTaskTypesForTarget(...)`
- add another parallel family of tests for the new task type

That creates three concrete problems:

1. The subject of routing logic becomes historical handler names instead of the task types the worker is actually allowed to process.
2. Scan scope and claim filtering become spread across multiple boolean branches, making it harder to reason about and prove the absence of incorrect claims.
3. `audio_extract_text`, as another durable text-production task, would continue to depend on the historical image-specific model instead of on the task-type contract already provided by `semantic_tasks`.

### Constraints and decision drivers

This proposal must satisfy the following constraints:

1. The change must remain small. This is not a task framework rewrite.
2. The explicit `switch` structure in `dispatchTask()` stays in place because this phase is about stabilizing the routing contract, not redesigning execution abstraction.
3. The task-type filtering semantics of `ClaimSemanticTask(...)` must remain unchanged; the worker should just use them more consistently.
4. Lease / retry / recover semantics and datastore schema must remain unchanged.
5. Phase 1 must be independently shippable and testable even without an `audio_extract_text` handler.

## Goals

1. Make `semantic worker` capability routing use `semantic.TaskType` sets as its single capability expression.
2. Make target scanning, fallback inclusion, and `ClaimSemanticTask(...taskTypes)` all derive from the same task-type computation logic.
3. Keep the existing explicit `dispatchTask()` model and avoid introducing a new execution framework in this prerequisite phase.
4. Create a stable, non-image-specific prerequisite for future `audio_extract_text` support.

## Non-Goals

- This phase does not add `semantic.TaskTypeAudioExtractText`.
- This phase does not implement audio runtime, upload-path handling, or writeback behavior.
- This phase does not turn `dispatchTask()` into a generic registry or plugin mechanism.
- This phase does not add execution support for `generate_l0` or `generate_l1`.
- This phase does not change the foundational lease, ack, retry, or recover semantics of semantic tasks.

## Design

### 1. Current architecture snapshot

The current `semantic worker` routing path can be summarized as:

```text
worker manager
  -> hasEmbedHandler() / hasImageHandler()
  -> supportsTenantProvider() / shouldIncludeFallback()
  -> allowedTaskTypesForTarget()
  -> store.ClaimSemanticTask(...allowedTaskTypes)
  -> dispatchTask(task.TaskType)
```

The real problem is not that `dispatchTask()` uses an explicit `switch`. The real problem is that the earlier routing layers do not operate directly on `TaskType`; they operate on the historical image/embed capability names.

### 2. Canonical capability expression

In this phase, the canonical expression for worker capability becomes:

- a worker or target expresses its currently supported durable semantic work as `[]semantic.TaskType`
- an empty set means "no semantic task types are currently supported"
- a non-empty set means:
  - the target may participate in scheduling
  - the set may be passed to `ClaimSemanticTask(...taskTypes)` as the filter
  - the claimed task is expected to be compatible with explicit dispatch

This set does not replace `dispatchTask()`. It replaces the current image/embed-specific routing entry points.

### 3. Capability sources remain split, but expression becomes uniform

The current codebase effectively has two sources of semantic work. Phase 1 unifies how they are expressed, not how they are owned:

1. app-managed semantic tasks
   - today this is only `embed`
   - execution capability comes from `semanticWorkerManager.embedder`, not from the backend itself

2. auto-backend semantic tasks
   - today this is only `img_extract_text`
   - execution capability comes from backend runtime

The minimal design is therefore:

- the worker manager maintains a set of app-managed task types
- the auto backend exposes the set of auto semantic task types it can currently execute
- `allowedTaskTypesForTarget(...)` becomes responsible only for computing the effective task type set for a target, instead of centering its logic around image/embed booleans

The intended code shape is:

1. `backend.Dat9Backend` should expose task type sets for the auto-embedding path rather than only exposing an image-specific boolean.
2. `tenant.Pool` should also expose the auto task type set supported by the tenant backends it creates, so tenant list filtering can use the same model.
3. `semanticWorkerManager` should express capabilities such as `embedder != nil` through an app-managed task type set instead of through a dedicated `hasEmbedHandler()` concept.

This phase does not require a single global registry for every capability source. It only requires `TaskType` sets to become the canonical routing subject.

### 4. Worker routing rules

The converged routing rules should be:

1. `listTenantRefs(...)`
   - for auto providers: include them only when the pool's auto task type set is non-empty
   - for app-managed providers: include them only when the worker's app-managed task type set is non-empty

2. `shouldIncludeFallback()`
   - include the fallback backend in worker scanning only when the effective task type set for its mode is non-empty

3. `targetForRef(...)`
   - compute `allowedTaskTypes` for the target
   - if the set is empty, skip the target and do not enter claim

4. `processNext(...)`
   - the worker must always call `ClaimSemanticTask(...)` with a non-empty task type set
   - the worker must not fall back to "claim any task with an empty filter" due to a routing computation mistake

Rule 4 is the key invariant of this proposal. `pkg/datastore/semantic_tasks.go` explicitly supports unfiltered claim behavior when no task types are passed. If the worker ever sends an otherwise unsupported target into claim with an empty set, the behavior would immediately degrade from "claim only supported tasks" to "claim any pending task". Phase 1 must eliminate that risk at the contract level.

### 5. `dispatchTask()` stays explicit

This proposal does not change the explicit `switch task.TaskType` execution model in `pkg/server/semantic_worker.go`.

After Phase 1, the worker should still behave as:

- routing based on task type sets
- dispatch based on explicit `switch`

The reasons are:

1. The real problem today is unstable routing capability expression, not explicit task dispatch.
2. The explicit `switch` continues to map unsupported task types into the existing retry / dead-letter path via the current `unsupported` result.
3. Before a third actually supported handler exists, a generic registry does not provide enough benefit to justify the additional abstraction cost.

So Phase 1 converges capability routing only. It does not generalize the execution framework.

### 6. Boundary with the follow-up audio proposal

Once this proposal is complete, the audio proposal should then add only the next increments:

1. add `semantic.TaskTypeAudioExtractText` in `pkg/semantic/task.go`
2. let backend audio runtime include `audio_extract_text` in the auto semantic task type set
3. add an `audio_extract_text` case to `dispatchTask()`

In other words, the audio proposal should no longer be responsible for "first fixing the routing model and then adding audio." It should build directly on top of a stabilized worker contract and extend it with one more task type.

## Compatibility and Invariants

Phase 1 must keep the following unchanged:

1. The datastore semantics of `ClaimSemanticTask(...)`, `AckSemanticTask(...)`, `RetrySemanticTask(...)`, and `RecoverExpiredSemanticTasks(...)`.
2. The execution entry points for `embed` and `img_extract_text` in `dispatchTask()`.
3. For currently supported targets:
   - app-managed targets still claim only `embed`
   - auto targets still claim only `img_extract_text`
4. The worker must never perform an unfiltered claim for a target that supports no task types.
5. This phase does not commit to bringing `generate_l0` or `generate_l1` into worker support; they only demonstrate that the task vocabulary is already broader than the current capability vocabulary.

## Rollout Plan

- Phase A: introduce the task-type-set capability model
  - add the minimal task type set expressions to backend, pool, and worker manager
  - keep existing image/embed runtime and dispatch logic unchanged

- Phase B: switch worker routing to task type sets
  - rewrite `supportsTenantProvider(...)` around task type sets
  - rewrite `shouldIncludeFallback()` around task type sets
  - rewrite `allowedTaskTypesForTarget(...)` around task type sets
  - guarantee that empty-set targets never enter claim

- Phase C: use this as the prerequisite for audio design
  - add `audio_extract_text` in a separate proposal
  - integrate audio only by extending task type sets and adding an explicit dispatch case

## Validation Strategy

1. Add unit tests for the capability helpers in `pkg/server/semantic_worker.go` to cover:
   - app-managed targets returning only `embed`
   - auto targets returning only `img_extract_text`
   - targets with no capability returning an empty set and being skipped

2. Preserve and strengthen the tenant scan / fallback include behavior tests in `pkg/server/semantic_worker_test.go` so they verify:
   - auto-provider scan eligibility depends on non-empty task type sets rather than on image-specific boolean naming
   - app tenants do not incorrectly claim auto-only tasks
   - auto tenants do not incorrectly claim app-only tasks

3. Continue to rely on the existing multiple-task-type filtered claim coverage in `pkg/datastore/semantic_tasks_test.go` to show that the datastore contract already supports the new routing input shape.

4. Regress the current image path:
   - `img_extract_text` is still claimed, executed, and acked
   - existing lease renew / lease lost / retry behavior does not regress

## Risks and Mitigations

1. Risk: over-generalizing in order to unblock audio and accidentally turning this into a new task framework.
   Mitigation: explicitly limit this proposal to capability routing convergence and keep `dispatchTask()` as explicit dispatch.

2. Risk: an incorrect task type set computation causes the worker to perform an unfiltered claim.
   Mitigation: define "targets with an empty task type set must never enter claim" as an explicit invariant and cover that behavior with tests.

3. Risk: pool, fallback backend, and worker manager continue to maintain separate capability logic and drift again later.
   Mitigation: make task type sets the canonical routing subject and reuse the same helper logic for include / scan / claim decisions wherever possible.

4. Risk: Phase 1 is misread as also enabling `generate_l0` / `generate_l1`.
   Mitigation: state clearly in `Non-Goals` and `Compatibility and Invariants` that this phase only converges the model and does not expand execution support.
