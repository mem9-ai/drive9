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

## 3. Definitions

- **durable queuefs**: the queuefs interface backed by durable task state
- **lease**: a temporary claim on a task by a worker
- **recover**: the act of re-queueing a stale processing task after lease expiry
- **dead-lettered**: a terminal task state after retry exhaustion or explicit non-retry failure

## 4. Design

### 4.1 Runtime role

dat9 uses durable `queuefs` as:

- a tenant-local task execution substrate
- not a global shared scheduler
- not a complete workflow engine by itself

### 4.2 Backend assumption

durable `queuefs` should keep the AGFS/queuefs interface shape but use a tenant-local `db9` backend for durable task state.

### 4.3 Required operations

dat9 depends on the following operations:

- `enqueue`
- `dequeue`
- `ack`
- `nack`
- `renew`
- `recover`
- `stats`

### 4.4 Runtime contract

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

### 4.5 Worker expectations

Workers must:

- renew leases for long-running tasks
- write results with version awareness
- `ack` only after durable success conditions are met
- `nack` or fail explicitly when processing cannot complete correctly

## 5. Invariants / Correctness Rules

- `dequeue` must not mean permanent success
- lease expiry must not silently lose work
- retry and dead-letter behavior must be explicit
- task runtime state must remain tenant-local
- queuefs file interfaces must not be mistaken for the full semantic workflow definition

## 6. Failure / Recovery

- stale processing tasks must be recoverable
- worker crashes must leave tasks recoverable through lease expiry and recover behavior
- at-least-once delivery is acceptable; exactly-once is not required

## 7. Open Questions

- how much task typing and payload normalization should be enforced centrally
- whether some aggregate work should use separate queue classes or priorities

## 8. References / Dependencies

- `docs/design/queuefs-durable-task-queue-rfc.md`
- `docs/design/system-architecture.md`
- `docs/design/resource-versioning-and-async-correctness.md`
