# Create Micro-Batching Design

## Problem

Production load testing showed small-file create throughput flattening while
`backend_write_create_timing` reported tenant DB transaction time around
`in_tx ~= 500ms`. The earlier quota hot-path work reduced repeated quota reads
and made pending quota outbox aggregation cheaper, but it did not change the
dominant cost: every small create still commits one independent TiDB
transaction.

For create-if-absent writes, the current write path performs one tenant DB
transaction per file:

1. Check storage quota and file-count quota.
2. Insert file metadata and content rows.
3. Ensure parent directories.
4. Insert the dentry.
5. Replace tags, when present.
6. Enqueue semantic tasks, when applicable.
7. Enqueue the quota outbox mutation.
8. Commit.

On TiDB, each transaction commit pays distributed commit overhead: prewrite,
commit, timestamp allocation, and Raft replication. For tiny files this fixed
cost dominates file size and payload processing. With one transaction per file,
additional application workers eventually hit TiDB commit throughput rather
than CPU or network limits.

## Goal

Reduce the per-file cost of TiDB transaction commit for high-throughput
small-file create workloads by committing multiple independent create-if-absent
writes in one tenant DB transaction.

The external API must remain unchanged:

- A caller still waits for the individual file write result.
- Duplicate create-if-absent paths still return conflict for that request.
- A quota failure for one request must not roll back unrelated requests in the
  same batch.
- Successful writes must still enqueue semantic and quota outbox work exactly
  once.

## Non-Goals

This first implementation intentionally does not batch:

- Overwrite, append, patch, or revision-CAS writes.
- S3-backed simple PUT writes.
- Tenant-quota fallback mode.
- Image/audio media files that may need media LLM quota checks.
- Cross-tenant work.

Those paths have more complicated object cleanup, revision, quota, or tenant DB
boundaries. Keeping the first scope narrow makes the rollout reversible and the
correctness surface reviewable.

## Current Scope

The batcher only runs when all of these are true:

- `DRIVE9_QUOTA_SOURCE=server`
- `DRIVE9_CREATE_BATCH_MAX > 1`
- the write is create-if-absent
- the file is DB-inline
- the detected content type is not counted as quota media

The default remains disabled:

```text
DRIVE9_CREATE_BATCH_MAX=1
```

For load testing, start with:

```text
DRIVE9_QUOTA_SOURCE=server
DRIVE9_CREATE_BATCH_MAX=16
DRIVE9_CREATE_BATCH_MAX_BYTES=1048576
DRIVE9_CREATE_BATCH_CONCURRENCY=4
DRIVE9_CREATE_BATCH_LINGER_MS=1
```

`DRIVE9_CREATE_BATCH_LINGER_MS` is the only linger setting. If unset, it
defaults to 1 ms. `DRIVE9_CREATE_BATCH_MAX_BYTES` caps the cumulative DB-inline
payload bytes in one transaction. `DRIVE9_CREATE_BATCH_CONCURRENCY` bounds the
number of in-flight flush transactions per tenant backend.

## Architecture

Each `Dat9Backend` owns one `createBatcher` when batching is enabled. Backends
are already tenant-scoped, so this naturally avoids cross-tenant batching.

Request goroutines still perform per-request preparation:

- upload size check
- file size quota check
- content type detection
- checksum
- text extraction
- DB-inline content copy
- ID and timestamp allocation

After preparation, the request enqueues a `createBatchJob` and waits on that
job's result channel.

The collector goroutine seals a batch when any of these happens:

- it collects `DRIVE9_CREATE_BATCH_MAX` jobs, or
- cumulative DB-inline payload bytes reach `DRIVE9_CREATE_BATCH_MAX_BYTES`, or
- the first job in the current batch waits longer than the configured linger.

Sealed batches are handed to background flush goroutines behind a bounded
semaphore. This lets the collector keep assembling later batches while earlier
batches are in TiDB commit, but prevents unbounded same-tenant transaction
fan-out. Each flush opens one `Store.InTx` and processes active jobs in order.

## Per-Job Isolation

A batch must not become "all or nothing" for expected per-file errors such as a
duplicate path. Each job runs inside a SQL savepoint:

```sql
SAVEPOINT drive9_create_batch_job;
-- one file create sequence
ROLLBACK TO SAVEPOINT drive9_create_batch_job; -- only on that job's error
RELEASE SAVEPOINT drive9_create_batch_job;
```

If a job fails, only that job is marked failed. The batch continues with later
jobs and commits successful jobs.

Savepoint setup, rollback, or release failures are treated as fatal transaction
errors. In that case the outer transaction is aborted and all active jobs receive
the transaction error.

If the outer transaction itself fails, all successful jobs in that transaction
receive the transaction error, matching the fact that none of the writes
committed.

## Quota Correctness

Batching introduces one new correctness requirement: quota checks must account
for earlier successful jobs in the same batch.

For storage quota, each job checks:

```text
central_usage.storage_bytes
+ central_usage.reserved_bytes
+ pending_quota_outbox_storage_delta_snapshot
+ current_batch_storage_delta
+ this_job_size
<= max_storage_bytes
```

For file-count quota, each job checks:

```text
central_usage.file_count
+ pending_quota_outbox_file_delta_snapshot
+ current_batch_file_delta
+ 1
<= max_file_count
```

The pending quota outbox deltas are loaded once at the start of the batch
transaction. Successful jobs then update in-memory batch deltas. This avoids
double-counting rows that were inserted earlier in the same transaction.

With `DRIVE9_CREATE_BATCH_CONCURRENCY > 1`, concurrently flushing batches do not
see each other's uncommitted in-transaction deltas. This is the same soft
small-write admission model already used across multiple server pods: within
the quota usage and pending-delta cache TTL, the system may briefly over-admit
by roughly the in-flight batch concurrency window. Successful writes still
durably enqueue quota outbox mutations and reconcile through accounting. Strict
quota enforcement remains on the upload reservation path, which uses live
counters under the tenant-wide admission lock.

When a job commits and enqueues a quota outbox mutation, the existing local
pending-delta cache is adjusted after the transaction commits, just like the
non-batched path.

## Post-Commit Side Effects

After a successful batch transaction:

- each successful job returns its own byte count
- semantic task notifications are fired for jobs that created semantic work
- quota outbox notifications are fired for jobs that enqueued quota mutations
- local pending quota deltas are updated per successful job

Failed jobs receive their own error and do not run post-commit side effects.

## Observability

The implementation records:

- `component="create_batch", operation="wait"` duration for per-job queue wait
- `component="create_batch", operation="flush"` duration for batch transaction
  flush time
- `component="create_batch", name="batch_size"` gauge with active jobs per flush
- `component="create_batch", name="in_flight"` gauge with active flush
  transactions

During rollout, compare these with:

- HTTP PUT p50/p95/p99
- `backend_write_create_timing.tenant_tx_ms`
- TiDB transaction commit latency
- write QPS
- quota reject counts
- quota outbox backlog

Good rollout signs:

- batch size is regularly above 1 under load
- create batch wait remains near the configured linger
- QPS increases without a matching p99 increase
- quota outbox backlog remains bounded
- in-flight flush concurrency reaches the configured bound under high load

Bad rollout signs:

- `create_batch/wait` grows far beyond linger
- batch size remains near 1 under high load
- p99 increases while QPS does not improve
- quota outbox backlog grows faster than the worker drains it

## Rollout

Roll out with the default off, then enable for a pressure-test environment:

```text
DRIVE9_QUOTA_SOURCE=server
DRIVE9_CREATE_BATCH_MAX=16
DRIVE9_CREATE_BATCH_MAX_BYTES=1048576
DRIVE9_CREATE_BATCH_CONCURRENCY=4
DRIVE9_CREATE_BATCH_LINGER_MS=1
```

If the batcher improves throughput without p99 regression, test larger batches
such as 32 or higher concurrency such as 6 to 8. Do not increase linger just to
force larger batches; extra linger directly adds request latency. If queue wait
grows while TiDB remains healthy, increase concurrency before increasing linger.

Rollback is configuration-only:

```text
DRIVE9_CREATE_BATCH_MAX=1
```

## Tests

The PR includes regression tests for the main new correctness properties:

- concurrent batched creates all commit and enqueue quota outbox rows
- duplicate paths in one batch fail only the conflicting job
- cumulative storage quota is enforced across jobs in the same batch

Existing create-if-absent tests continue to cover conflict precedence and the
default non-batched path.
