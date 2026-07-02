# Quota Accounting: Remove the User DB from the Runtime Hot Path

## Context

High-QPS small-file writes used to write a `quota_outbox` row into the tenant
database inside every write transaction. A per-tenant `quotaOutboxWorker` then
claimed, acked, and retried those rows against the same tenant database before
applying quota deltas to the meta database. When the tenant database was slow or
had bad connections, central quota convergence stalled and worker alerts became
noisy or real.

Runtime quota accounting now has a stricter goal: do not touch the tenant/user
database for quota work. That means no outbox writes in the write transaction,
no outbox claim/ack/retry worker, and no pending-outbox `SUM` read during
admission. Eventual consistency is acceptable; strict per-write reservation
through a tenant-DB `FOR UPDATE` lock is not required for small writes.

## Correctness Tradeoff

Tenant file metadata and central quota live in different databases. Without a
distributed transaction, removing the tenant outbox cannot be fully equivalent
to the old durable handoff. The old outbox avoided a crash gap by committing the
file mutation and the quota marker in the same tenant transaction.

The new runtime path uses the meta database `quota_mutation_log` as the only
quota handoff. File create/overwrite mutations are logged after the tenant write
transaction commits, then applied asynchronously and replayed by
`MutationReplayWorker` if the in-process apply is missed.

This leaves a known residual window:

```text
tenant DB commit succeeds -> process crashes before meta quota_mutation_log insert
```

In that case, quota may remain undercounted until reconciliation/backfill. The
new path intentionally surfaces meta-log insert failure as an error rather than
silently dropping it, but it cannot roll back the already-committed tenant
write. Runtime accounting is user-DB-free; repair/backfill may still read the
tenant database.

## Runtime Flow

Small create/overwrite writes:

1. Perform normal tenant DB file mutation.
2. Insert a meta `quota_mutation_log` row.
3. Add the mutation delta to the in-process pending-delta cache.
4. Enqueue async apply.
5. On apply success, update `tenant_file_meta` and `tenant_quota_usage`, mark the
   log row applied, and subtract the pending delta.

Uploads:

1. Reserve bytes in the meta DB with `AtomicReserveAndInsertUpload`.
2. Mark the reservation `completing` before finalization so expiry sweep cannot
   abort an upload that is being finalized.
3. After tenant finalization commits, log `upload_complete` in the meta mutation
   log.
4. Async apply settles the reservation and transfers reserved bytes to confirmed
   usage.

Admission:

Small-write quota checks read central quota config/usage and add only local
in-process pending mutation deltas. They do not read tenant `quota_outbox`.
Different pods do not see each other's local pending deltas, so burst admission
is optimistic and may temporarily over-admit until replay converges.

## Removed Runtime Dependencies

- `DRIVE9_QUOTA_SOURCE` is retired. Central quota is active when a meta quota
  store is wired into the backend.
- The tenant `quota_outbox` worker and datastore helpers have been removed from
  the runtime.
- Runtime create/overwrite/upload paths do not enqueue tenant `quota_outbox`
  rows, and new tenant schemas no longer create `quota_outbox` or
  `quota_admission_locks`.
- Runtime admission no longer calls `PendingQuotaOutboxDeltas` or any tenant
  `quota_outbox` pending read.

Existing tenant databases may still contain historical `quota_outbox` rows from
pre-cutover deployments. They are not drained by runtime code. The one-time
central quota reconciliation has already been completed, so the
`drive9-server backfill-quota` command has been removed. Use this order for any
remaining historical table cleanup:

1. Deploy the meta-quota runtime everywhere and confirm old pods are gone.
2. Confirm central quota usage matches expected tenant usage and
   `quota_mutation_log` backlog is drained.
3. Delete historical tenant `quota_outbox` rows or drop the old tables. Do not
   delete them before verification; after deletion they are no longer available
   for diagnosing pre-cutover handoff gaps.

## Operational Guardrails

Watch the meta pipeline instead of tenant outbox health:

- `drive9_service_gauge{component="mutation_replay",name="pending_mutations"}`
  for `quota_mutation_log` pending backlog by tenant.
- `drive9_service_gauge{component="mutation_replay",name="oldest_pending_age_seconds"}`
  for oldest pending mutation age by tenant.
- The replay backlog gauges reflect the last observation from a live
  `mutation_replay` worker. A fatal worker exit intentionally leaves the last
  non-zero values in place instead of clearing them, so pair these gauges with
  `mutation_replay` worker error/bad-connection alerts.
- `central_quota_mutation_log_insert_failed` log/metric.
- `central_quota/upload_reset_active` errors or
  `central_quota_upload_reset_active_failed`, which indicate a retryable upload
  complete failure could not reset the reservation back to active.
- any new tenant `quota_outbox` row after cutover, which indicates a missed
  runtime code path. This is an operational DB check, not a runtime metric,
  because the runtime intentionally no longer queries tenant `quota_outbox`.

For permanent gaps caused by the residual crash window, reconcile central
counters from tenant file metadata with an operational repair job. Runtime code
does not read tenant quota state.

## Verification

Useful checks for this change:

- Small create and overwrite increase central usage and do not create new tenant
  `quota_outbox` rows.
- Meta log insert failure is returned to the caller and recorded, not fail-open.
- Replaying the same mutation is idempotent through central `tenant_file_meta`
  old-state reads.
- Upload completion settles `active` or `completing` reservations and expiry
  sweep does not abort `completing` rows.
- A tenant DB connection issue no longer stops central quota mutation replay,
  because replay uses only the meta DB.
