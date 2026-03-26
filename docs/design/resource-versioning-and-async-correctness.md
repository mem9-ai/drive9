# RFC: dat9 Resource Versioning and Async Correctness

## 1. Goal

This RFC defines how dat9 keeps asynchronous semantic processing correct.

It focuses on:

- `resource_id`
- `resource_version`
- processing and index state
- version-aware writeback
- stale-task suppression
- authoritative state versus rebuildable state

## 2. Non-goals

This RFC does not define:

- exact queue backend schema
- exact object upload API
- complete retrieval ranking algorithms

## 3. Definitions

- **resource_id**: the stable logical identity of a resource
- **resource_version**: the version token bound to async work and derived-state progression
- **authoritative state**: the state the system trusts for correctness and recovery
- **rebuildable state**: derived state that may be recomputed from authoritative inputs
- **stale task**: a task whose input version is no longer the active version for current-state progression

## 4. Design

### 4.1 Resource-version boundary

Asynchronous work must run against explicit versioned inputs.

Every task that produces derived state must bind at least:

- `resource_id`
- `resource_version`
- task-specific input parameters

### 4.2 Version-aware writeback

When async work writes results back, the system must enforce one of two outcomes:

- the result is still valid for the current version and may advance current-state pointers
- the result is only retained as historical output and must not advance current-state pointers

### 4.3 Authoritative versus rebuildable state

Authoritative state should include:

- resource identity
- resource version state
- processing state
- task execution state

Rebuildable state should include at least some derived artifacts such as:

- summaries
- overviews
- vectors
- other derived semantic products that can be recomputed

### 4.4 Mental model

```text
resource_id -> resource_version -> derived states
                   |
                   +--> parsed
                   +--> summarized
                   +--> overview_generated
                   +--> indexed
```

### 4.5 Lifecycle and async separation

Path/file semantics are not enough to guarantee async correctness.

The system must separate:

- naming and content location semantics
- lifecycle and async writeback semantics

## 5. Invariants / Correctness Rules

- async tasks must bind explicit `resource_version`
- stale tasks must not overwrite current derived state
- current-state progression must be version-aware
- rebuildable state must not be treated as the sole source of truth

## 6. Failure / Recovery

- worker interruption must not corrupt current-state pointers
- reconcile must be able to compare authoritative state with rebuildable state and trigger regeneration where needed
- partial async completion must be recoverable without assuming exactly-once execution

## 7. Open Questions

- the exact compare-and-set boundaries for every writeback path
- the final lifecycle rules for delete, soft delete, restore, rename, and move against in-flight tasks
- the exact protocol for aggregate snapshot version generation

These questions may be resolved in follow-up RFCs without changing the core direction of this RFC.

## 8. References / Dependencies

- `docs/design/system-architecture.md`
- `docs/design/durable-queue-runtime.md`
- `docs/design/semantic-derivation-and-retrieval.md`
