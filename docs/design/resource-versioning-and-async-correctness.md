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

This RFC also does not require the project to ship the strongest version model on day one.
It defines the target direction and the minimum correctness contract that current implementation work should satisfy.

## 3. Definitions

- **resource_id**: the stable logical identity of a resource
- **resource_version**: the version token bound to async work and derived-state progression
- **authoritative state**: the state the system trusts for correctness and recovery
- **rebuildable state**: derived state that may be recomputed from authoritative inputs
- **stale task**: a task whose input version is no longer the active version for current-state progression
- **file**: the user-visible path-addressable item
- **logical object**: the internal content identity behind one or more file paths
- **derived artifact**: any output generated from resource processing, whether user-visible or internal
- **semantic artifact**: a user-visible derived artifact such as `.abstract.md` or `.overview.md`

## 4. Current Implementation Target

This RFC intentionally separates the minimum viable correctness contract from the stronger long-term model.

### 4.1 P0 / P1 minimum contract

For the current implementation phase, dat9 should guarantee at least:

- every async task binds a version input for the file or resource it processes
- writeback is guarded by version equality or an equivalent stale-write check
- stale work may be retained as a no-op or historical artifact, but must not advance current derived state
- reconcile can detect missing or stale derived artifacts and schedule regeneration

In practice, this means a simple file revision guard is acceptable for the first implementation stage if it enforces the same stale-write safety boundary.

### 4.2 Longer-term target

The stronger target model remains:

- explicit `resource_id`
- explicit `resource_version`
- richer historical derived-state retention
- aggregate snapshot versioning

The system should be designed so the P0/P1 contract can evolve into this stronger model without changing the basic async correctness direction.

## 5. Design

### 5.1 Resource-version boundary

Asynchronous work must run against explicit versioned inputs.

Every task that produces derived state must bind at least:

- `resource_id`
- `resource_version`
- task-specific input parameters

For the current implementation phase, this may be approximated by a file-level revision token if a separate `resource_version` record is not yet materialized.

### 5.2 Version-aware writeback

When async work writes results back, the system must enforce one of two outcomes:

- the result is still valid for the current version and may advance current-state pointers
- the result is only retained as historical output and must not advance current-state pointers

Example:

```text
file v1 -> task T1(summary for v1)
file v2 -> task T2(summary for v2)

If T1 finishes after v2 is current:
- T1 must not replace the current summary pointer
- T2 or reconcile must remain able to establish the current summary
```

Representative late-task timeline:

```text
time ---->

t0   file write produces revision r1
     -> enqueue T1(resource_version = r1)

t1   file write produces revision r2
     -> enqueue T2(resource_version = r2)

t2   T1 finishes late
     -> compare expected version r1 with current version r2
     -> mismatch
     -> do not advance current derived-state pointer
     -> optionally retain as historical / diagnostic output only

t3   T2 finishes
     -> compare expected version r2 with current version r2
     -> match
     -> advance current derived-state pointer

t4   if T2 is missing or fails repeatedly
     -> reconcile detects stale or missing current derived state
     -> enqueue or repair until current version is represented
```

Writeback decision shape:

```text
task(version = vN) finishes
          |
          v
compare task.version with current.version
          |
     +----+----+
     |         |
   match    mismatch
     |         |
     v         v
advance     no current-state advance
current     retain only as historical
pointer     output or drop as no-op
```

### 5.3 Authoritative versus rebuildable state

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

### 5.4 Mental model

```text
resource_id -> resource_version -> derived states
                   |
                   +--> parsed
                   +--> summarized
                   +--> overview_generated
                   +--> indexed
```

### 5.5 Lifecycle and async separation

Path/file semantics are not enough to guarantee async correctness.

The system must separate:

- naming and content location semantics
- lifecycle and async writeback semantics

This does not mean the product stops feeling file-oriented. It means internal resource state is allowed to be stronger than the external file metaphor.

## 6. Invariants / Correctness Rules

- async tasks must bind explicit `resource_version`
- stale tasks must not overwrite current derived state
- current-state progression must be version-aware
- rebuildable state must not be treated as the sole source of truth

For P0/P1, the practical invariant is:

- every async writeback must be protected by a stale-write guard tied to the version observed when the task was created

Additional current-phase constraints:

- a stale task may finish successfully as background work, but it must not become the current derived-state winner
- current derived-state pointers must move forward only through version-valid writeback
- reconcile must reason from authoritative current version, not from "whichever task finished last"

## 7. Failure / Recovery

- worker interruption must not corrupt current-state pointers
- reconcile must be able to compare authoritative state with rebuildable state and trigger regeneration where needed
- partial async completion must be recoverable without assuming exactly-once execution

## 8. Open Questions

- the exact compare-and-set boundaries for every writeback path
- the final lifecycle rules for delete, soft delete, restore, rename, and move against in-flight tasks
- the exact protocol for aggregate snapshot version generation

These questions may be resolved in follow-up RFCs without changing the core direction of this RFC.

## 9. References / Dependencies

- `docs/design/system-architecture.md`
- `docs/design/durable-queue-runtime.md`
- `docs/design/semantic-derivation-and-retrieval.md`
- `docs/design/write-path-and-reconcile.md`
