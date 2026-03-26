# RFC: dat9 Durable Queue Runtime

## 1. Goal

This RFC defines how dat9 uses durable `queuefs` as its tenant-local asynchronous execution substrate.

It builds on `docs/design/queuefs-durable-task-queue-rfc.md` and specifies the dat9-specific runtime contract.

## 2. Non-goals

This RFC does not redefine queuefs itself from scratch.

It does not define:

- all backend table details
- complex workflow DAG semantics
- cross-tenant fairness scheduling as a queue concern

This RFC also does not require dat9 to ship the full durable queue feature set in its first implementation phase.

## 3. Definitions

- **durable queuefs**: the queuefs interface backed by durable task state
- **lease**: a temporary claim on a task by a worker
- **recover**: the act of re-queueing a stale processing task after lease expiry
- **dead-lettered**: a terminal task state after retry exhaustion or explicit non-retry failure
- **resource**: the internal versioned processing unit carried by async tasks for correctness

## 4. Current Implementation Target

The durable queue direction should be preserved, but rollout should be phased.

### 4.1 P0 / P1 queue runtime

Minimum expected capabilities for the current implementation phase:

- `enqueue`
- `dequeue`
- `ack`
- simple retry
- stale-task recovery

This is enough to support the first dat9 async flows without pretending that dequeue means permanent success.

### 4.2 Recommended P0 implementation path

For the first implementation stage, dat9 should prefer a simple tenant-local `db9` tasks table or equivalent direct runtime table model.

This P0 path should provide at least:

- durable task state
- lease-aware dequeue behavior
- `ack` / retry / recover behavior
- version-aware task payloads and writeback guards

This is the recommended P0 direction because it solves the current async requirements directly, while keeping the rollout smaller than a full queuefs-shaped runtime surface for internal workers on day one.

The longer-term target remains:

- a queuefs-compatible durable runtime contract
- compatibility with the AGFS/queuefs interface shape
- the ability to evolve toward richer queue-runtime behavior without changing the core task semantics

### 4.3 Later extensions

The following features remain part of the target runtime model, but may be added incrementally:

- `renew`
- dead-letter behavior
- richer queue statistics
- queue classes, priorities, or broader worker routing

## 5. Design

### 5.1 Runtime role

dat9 uses durable `queuefs` as:

- a tenant-local task execution substrate
- not a global shared scheduler
- not a complete workflow engine by itself

### 5.2 Backend assumption

durable `queuefs` should keep the AGFS/queuefs interface shape but use a tenant-local `db9` backend for durable task state.

In practice, the current implementation may realize this through a direct `db9` tasks table or equivalent runtime interface, as long as the resulting semantics remain compatible with the durable queue contract defined in this RFC.

AGFS/queuefs should be treated as:

- the canonical queue-shaped access contract
- an integration surface for tooling and system boundaries

It should not be read as a requirement that all internal worker logic must literally use filesystem-like operations when a direct runtime interface is clearer and more efficient.

### 5.3 Required operations

dat9 depends on the following operations:

- `enqueue`
- `dequeue`
- `ack`
- `nack`
- `renew`
- `recover`
- `stats`

For P0/P1, `renew` and richer `stats` may be deferred if task duration and operational risk are still low, but the long-term contract should remain compatible with them.

### 5.4 Runtime contract

Each task must carry enough information for correct async execution, including at least:

- `task_id`
- `task_type`
- `resource_id`
- `resource_version`
- attempt/lease metadata

Suggested state machine:

```text
queued -> processing -> succeeded
   |         |   |
   |         |   +-> dead_lettered
   |         +----> queued (retry/recover)
   +----------------> failed
```

### 5.5 Worker expectations

Workers must:

- renew leases for long-running tasks
- write results with version awareness
- `ack` only after durable success conditions are met
- `nack` or fail explicitly when processing cannot complete correctly

If the current phase does not yet expose a full queuefs filesystem path to internal workers, the same runtime rules should still hold through a direct worker/runtime interface.

## 6. Invariants / Correctness Rules

- `dequeue` must not mean permanent success
- lease expiry must not silently lose work
- retry and dead-letter behavior must be explicit
- task runtime state must remain tenant-local
- queuefs file interfaces must not be mistaken for the full semantic workflow definition

## 7. Failure / Recovery

- stale processing tasks must be recoverable
- worker crashes must leave tasks recoverable through lease expiry and recover behavior
- at-least-once delivery is acceptable; exactly-once is not required

## 8. Open Questions

- how much task typing and payload normalization should be enforced centrally
- whether some aggregate work should use separate queue classes or priorities

## 9. References / Dependencies

- `docs/design/queuefs-durable-task-queue-rfc.md`
- `docs/design/system-architecture.md`
- `docs/design/resource-versioning-and-async-correctness.md`
- `docs/design/write-path-and-reconcile.md`
