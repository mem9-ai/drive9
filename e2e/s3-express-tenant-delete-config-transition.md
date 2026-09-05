---
title: S3 Express Tenant Delete Config Transition Runbook
status: manual-only
---

# S3 Express Tenant Delete Config Transition Runbook

This manual-only server validation proves the durable namespace-binding delete
contract. It is not a FUSE test and must not be added to `smoke-all.sh` or
local E2E: it requires real Directory Buckets and deliberate server
configuration transitions.

## Preconditions

1. Use a disposable tenant and dedicated Directory Buckets/prefixes A and B.
2. All server replicas use the same MetaDB and normal S3 configuration.
3. Prepare three sequential deployments:
   - A: `DRIVE9_S3_EXPRESS_BUCKET`, prefix, and region point at target A.
   - B: those values point at target B.
   - restored A: the original target-A values are restored exactly.
4. Keep the tenant API key, the MetaDB connection, and enough AWS access to
   list the two namespace prefixes. Never use production tenants or buckets.

## Phase A: bind and write

1. Deploy target A and wait until every server replica is healthy.
2. Provision a disposable tenant, then append-create a file through
   `POST /v1/fs/{path}?append-log` with expected revision and size `0`.
3. Query MetaDB for the tenant's `storage_namespace_id` and its
   `storage_namespaces` row. Assert:

   ```text
   append_log_binding_state = bound
   append_log_bucket        = <target A bucket>
   append_log_prefix        = <normalized target A base prefix>
   append_log_region        = <target A region>
   ```

4. Record the namespace ID and confirm the target-A prefix contains the
   append-log object.

## Phase B: fail closed on a mismatched configuration

1. Deploy target B to every server replica. Do not leave an A-configured
   worker running: it could complete cleanup before this phase is observed.
2. Delete the disposable tenant through the normal tenant-delete API.
3. Wait for one delete-worker retry interval, then query MetaDB. Assert:

   ```text
   tenants.status                  = deleting
   tenant_delete_jobs.state        = pending
   tenant_delete_jobs.completed_at = NULL
   storage_namespaces.state        = deleting
   ```

4. Assert the error records an append-log configuration/binding mismatch.
5. List target B at:

   ```text
   <normalized B base prefix>append-log/<namespace-id>/
   ```

   It must be empty. Target B must not receive a cleanup request for target A.

## Phase A restored: complete cleanup

1. Restore target A exactly on every server replica.
2. Wait for the pending delete job to complete. Assert:

   ```text
   tenants.status                   = deleted
   tenant_delete_jobs.state         = deleted
   tenant_delete_jobs.completed_at != NULL
   storage_namespaces.state         = deleted
   ```

3. List target A at:

   ```text
   <normalized A base prefix>append-log/<namespace-id>/
   ```

   It must be empty. Recheck target B remains empty.

## Pass criteria

1. An unbound namespace never requires S3 Express cleanup.
2. A bound namespace with mismatched configuration retries and never finalizes.
3. The mismatched target receives no cleanup.
4. Restoring the exact bound configuration deletes only the original target-A
   namespace prefix and finalizes the tenant exactly once.
