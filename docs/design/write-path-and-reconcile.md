# RFC: dat9 Write Path and Reconcile

## 1. Goal

This RFC defines how dat9 handles writes, uploads, commit points, asynchronous cleanup, and reconcile.

It covers both:

- small-file writes into `db9`
- large-file direct upload into `S3`

## 2. Non-goals

This RFC does not define:

- full user-facing API contracts
- full queue backend schema
- complete retrieval semantics

This RFC also does not require dat9 to expose all final upload and repair behaviors in the first release. It defines the target write discipline and the minimum implementation guidance needed now.

## 3. Definitions

- **commit point**: the point at which a resource write becomes accepted by the system
- **reconcile**: background logic that compares actual state with expected authoritative state and repairs drift
- **orphan object**: an object present in storage without valid committed metadata state
- **outbox / compensation marker**: durable metadata used to ensure downstream work can be retried or repaired

## 4. Current Implementation Target

### 4.1 P0 / P1 write contract

For the current implementation phase, dat9 should guarantee at least:

- every write has one explicit commit point
- small-file writes commit metadata and content together in `db9`
- large-file writes use direct `S3` upload plus an explicit completion step
- missing downstream async work can be detected and repaired by reconcile
- orphaned uploads or objects can be discovered and cleaned later

### 4.2 Practical implementation guidance

The current implementation does not need a heavy distributed transaction protocol. It does need clear sequencing.

For example:

- small files may commit content, metadata, and version state in one tenant-local transaction
- large files may commit in two phases: upload bytes first, then confirm metadata/version state
- downstream async derivation may be triggered after commit, but must be repairable if submission fails

## 5. Design

### 5.1 Small-file write path

For small files:

- content is written into `db9`
- metadata and version state are committed in the tenant-local state plane
- minimum visibility may be established synchronously
- downstream semantic derivation may continue asynchronously

A typical small-file path should have one clear commit transaction, followed by async follow-up work.

### 5.2 Large-file write path

For large files:

- the server returns `202` and presigned upload instructions
- the client uploads bytes directly to `S3`
- a completion step finalizes metadata, version state, and post-write work

A typical large-file path should not treat upload success alone as final system commit. Final commit happens when metadata/version state is confirmed.

### 5.3 Commit discipline

For both write paths:

- a consistent commit point must exist
- version progression must be explicit
- downstream async work must be durable or compensatable

In practice, this means one of the following must exist after commit:

- the async task is durably enqueued
- or a durable marker exists so reconcile can enqueue or repair it later

### 5.4 Reconcile and cleanup

Reconcile should handle at least:

- missing async follow-up work
- orphan uploads
- stale or missing derived artifacts
- orphaned storage objects

Examples of practical checks:

- a committed file version with no summary/index task record
- a completed upload record with no confirmed resource commit
- an `S3` object under a tenant prefix with no valid metadata reference

### 5.5 Cleanup separation

Logical state transitions and physical cleanup should remain separable.

Examples:

- metadata may mark deletion first
- old blobs may be cleaned later
- timed-out uploads may be aborted asynchronously

## 6. Invariants / Correctness Rules

- writes must have an explicit commit point
- direct large-file upload must not require the server to proxy file bytes
- cleanup may be asynchronous, but commit state must remain explicit
- reconcile must be a built-in system capability, not an emergency-only mechanism

## 7. Failure / Recovery

- interrupted uploads must be resumable or recoverable
- failed downstream task submission must be compensatable
- orphaned storage must be discoverable and cleanable

For the current phase, dat9 should at least support:

- resumable or restartable direct uploads
- periodic detection of committed files missing downstream semantic work
- periodic cleanup of orphaned or abandoned upload state

## 8. Open Questions

- whether outbox state should be modeled explicitly or folded into broader reconcile markers
- what recovery SLOs should apply for orphan cleanup and async repair

## 9. References / Dependencies

- `docs/design/storage-and-namespace.md`
- `docs/design/resource-versioning-and-async-correctness.md`
- `docs/design/durable-queue-runtime.md`
- `docs/design/api-and-ux-contract.md`
