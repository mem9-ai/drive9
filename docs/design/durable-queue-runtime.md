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

### 5.4 Task input and state model

Each task must carry enough information for correct async execution, including at least:

- `task_id`
- `task_type`
- `resource_id`
- `resource_version`
- attempt/lease metadata

The canonical `tasks` table, state fields, and baseline indexes are defined in `docs/design/canonical-schema.md`.

Tasks may also carry business input such as:

- explicit parser or summarizer parameters
- aggregate snapshot identifiers
- dedupe keys or priority hints

The critical rule is that every task that can advance derived state must carry an explicit input boundary, not just "the current file when processed later".

### 5.5 Runtime contract and state machine

Suggested state machine:

```text
queued --dequeue--> processing --ack------------------> succeeded
   ^                    |  \
   |                    |   \
   |                    |    +--nack(retry=false)----> failed
   |                    |
   |                    +--renew---------------------> processing
   |                    |
   |                    +--nack(retry=true)---------> queued
   |                    |
   +--recover(expired lease)<------------------------+
                        |
                        +--attempts exhausted-------> dead_lettered
```

Important runtime meanings:

- `dequeue` claims work temporarily; it does not imply success
- `processing` is lease-bound, not permanent ownership
- `recover` only re-queues tasks whose lease has truly expired
- `dead_lettered` is terminal for automated retry

### 5.6 Lease, renew, and recovery timeline

Representative timing model:

```text
t0  enqueue task T
    status = queued

t1  worker W1 dequeues T
    status = processing
    lease_until = t1 + lease_duration

t2  W1 is still healthy
    -> renew
    -> lease_until = t2 + lease_duration

t3  W1 crashes or stops renewing

t4  lease expires

t5  recover sweep observes:
    status = processing AND lease_until < now
    -> move T back to queued

t6  worker W2 dequeues T and continues
```

Recovery rules:

- long-running tasks must renew or heartbeat before expiry
- recover logic must not steal tasks that still have a valid lease
- at-least-once redelivery is acceptable and expected after expiry

### 5.7 Worker expectations

Workers must:

- renew leases for long-running tasks
- write results with version awareness
- `ack` only after durable success conditions are met
- `nack` or fail explicitly when processing cannot complete correctly

If the current phase does not yet expose a full queuefs filesystem path to internal workers, the same runtime rules should still hold through a direct worker/runtime interface.

Representative success/failure discipline:

```text
worker dequeues task(version = v7)
  -> reads authoritative current state
  -> performs work
  -> attempts version-aware writeback
  -> if writeback durably succeeds: ack
  -> if retryable failure: nack(retry=true)
  -> if permanent failure: nack(retry=false) or mark failed
```

### 5.8 Runtime observability

At minimum, the runtime should expose:

- queued / processing / failed / dead-letter counts
- dequeue / renew / ack / nack / recover rates
- task latency by task type
- lease-expiry recovery count

Useful correlation keys include:

- `tenant_id`
- `task_id`
- `task_type`
- `resource_id`
- `resource_version`

## 6. Invariants / Correctness Rules

- `dequeue` must not mean permanent success
- lease expiry must not silently lose work
- retry and dead-letter behavior must be explicit
- task runtime state must remain tenant-local
- queuefs file interfaces must not be mistaken for the full semantic workflow definition
- task payloads that can advance derived state must bind explicit versioned input

## 7. Failure / Recovery

- stale processing tasks must be recoverable
- worker crashes must leave tasks recoverable through lease expiry and recover behavior
- at-least-once delivery is acceptable; exactly-once is not required
- long-running tasks without renew support must be kept short enough that false recovery is not likely in practice

## 8. Open Questions

- how much task typing and payload normalization should be enforced centrally
- whether some aggregate work should use separate queue classes or priorities

## 9. References / Dependencies

- `docs/design/queuefs-durable-task-queue-rfc.md`
- `docs/design/canonical-schema.md`
- `docs/design/system-architecture.md`
- `docs/design/resource-versioning-and-async-correctness.md`
- `docs/design/write-path-and-reconcile.md`
