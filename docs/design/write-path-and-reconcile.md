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

## 3. Definitions

- **commit point**: the point at which a resource write becomes accepted by the system
- **reconcile**: background logic that compares actual state with expected authoritative state and repairs drift
- **orphan object**: an object present in storage without valid committed metadata state
- **outbox / compensation marker**: durable metadata used to ensure downstream work can be retried or repaired

## 4. Design

### 4.1 Small-file write path

For small files:

- content is written into `db9`
- metadata and version state are committed in the tenant-local state plane
- minimum visibility may be established synchronously
- downstream semantic derivation may continue asynchronously

### 4.2 Large-file write path

For large files:

- the server returns `202` and presigned upload instructions
- the client uploads bytes directly to `S3`
- a completion step finalizes metadata, version state, and post-write work

### 4.3 Commit discipline

For both write paths:

- a consistent commit point must exist
- version progression must be explicit
- downstream async work must be durable or compensatable

### 4.4 Reconcile and cleanup

Reconcile should handle at least:

- missing async follow-up work
- orphan uploads
- stale or missing derived artifacts
- orphaned storage objects

### 4.5 Cleanup separation

Logical state transitions and physical cleanup should remain separable.

Examples:

- metadata may mark deletion first
- old blobs may be cleaned later
- timed-out uploads may be aborted asynchronously

## 5. Invariants / Correctness Rules

- writes must have an explicit commit point
- direct large-file upload must not require the server to proxy file bytes
- cleanup may be asynchronous, but commit state must remain explicit
- reconcile must be a built-in system capability, not an emergency-only mechanism

## 6. Failure / Recovery

- interrupted uploads must be resumable or recoverable
- failed downstream task submission must be compensatable
- orphaned storage must be discoverable and cleanable

## 7. Open Questions

- whether outbox state should be modeled explicitly or folded into broader reconcile markers
- what recovery SLOs should apply for orphan cleanup and async repair

## 8. References / Dependencies

- `docs/design/storage-and-namespace.md`
- `docs/design/resource-versioning-and-async-correctness.md`
- `docs/design/durable-queue-runtime.md`
