---
title: Drive9 Migration V1 Operator Runbook
---

# Drive9 Migration V1 operator runbook

## Safety boundary

Drive9 Migration V1 moves one mounted, read-only EBS Source per Job to one
Drive9 Space/Prefix. It has no rollback or unfence. After T0 it never propagates
Delete or Rename, so old target paths can remain as non-blocking residue.

The prominent V1 limits are post-T0 residue, metadata loss, per-Job throttling,
residual ABA risk, one Source per Job, and no rollback. V1 preserves supported
path, link, content, and `mode & 0777` semantics. It does not preserve UID/GID,
timestamps, xattrs, ACLs, special bits, or sparse layout. Revision reuse after
delete/recreate retains a residual ABA overwrite risk. Throttling is per Job,
not batch-wide, and Jobs have no cross-Job transaction.

## Inputs and handoff mapping

Use [the sample configuration](../examples/drive9-migration/config.yaml) as the
strict `version: v3` shape. Every node name must select exactly one Job. The
credential reference is only a Secret-volume filename; the API key itself must
exist only in the read-only Secret volume at
`/var/run/secrets/drive9-migration/<credential_ref>`.

The sample demonstrates both accepted CSI handoff layouts:

| EBS Source | Drive9 target | Layout |
| --- | --- | --- |
| `/mnt/ebs/vol-0a` | Space `volume-a`, Prefix `/` | One EBS per Space |
| `/mnt/ebs/vol-0b` | Space `shared`, Prefix `/vol-0b` | Shared Space, disjoint Prefix |
| `/mnt/ebs/vol-0c` | Space `shared`, Prefix `/vol-0c` | Shared Space, disjoint Prefix |

Set `DRIVE9_MIGRATION_NODE_NAME` to the local node name. Supply the phase from
exactly one source: a sibling `phase` file beside `config.yaml`, or the accepted
`DRIVE9_MIGRATION_PHASE` fallback. Never place a key in config, argv, or an
environment variable.

Before starting a Job, mount its EBS Source read-only, provision the referenced
Secret file with mode `0600`, write `SYNCING` to the phase source, and run:

```text
drive9-migration plan -f /etc/drive9-migration/config.yaml
drive9-migration run -f /etc/drive9-migration/config.yaml
```

The `plan` JSON is the non-sensitive CSI handoff record: verify `volume_id`,
`node_name`, `source_root`, `space_ref`, `prefix`, `credential_ref`, source
identity, limits, target emptiness, and required capabilities. It never returns
the API key.

## SYNCING and rollout readiness

Inspect each Job independently:

```text
drive9-migration status --output json
drive9-migration diff --output jsonl
```

Proceed only when the latest complete round reports `ready_for_rollout=true`,
no Attention condition, and no blockers. This signal does not change phase.

## T0: start dual-write repair

T0 is the customer decision to begin rolling out the business dual-write
version. Change the selected phase source to `DUAL_WRITE_REPAIRING`, update the
customer-owned ConfigMap if applicable, and rollout-restart the Migration
DaemonSet. A mounted ConfigMap update alone does not change a running Worker.

After restart, require `recovery_complete=true` before using convergence
claims. The Worker performs a new deep recovery round and derives a fresh
in-memory repair mtime floor. Migration does not decide whether the business
rollout has completed.

## T1: external rollout confirmation and full verification

T1 is an external customer signal that every business Pod runs the dual-write
version; it is not a Migration phase. For every Job, invoke:

```text
drive9-migration verify-full
drive9-migration status --output json
```

The verification is process-local, scans every current Source path without an
mtime filter, and does not trigger cutover. Restart clears it, so rerun it after
any restart. Target-only residue is a warning and does not by itself fail
one-way verification.

## T2: irreversible cutover fence

T2 remains a customer decision after business observation. Confirm every Job
has a current passed full verification, no Attention condition, no grace or CAS
retry work, and a converged latest round. Then invoke once per Job:

```text
drive9-migration prepare-drive9-cutover
drive9-migration status --output json
```

The Worker drains in-flight Migration work, persists Fence Intent, completes
the fence, and reports `CUTOVER_READY`. Once Intent is durable, Migration can
never write business data again; restart only finishes the fence. There is no
rollback, phase regression, or unfence.

## Retained control data

V1 has no automatic cleanup or cleanup command. Keep
`/.drive9-migration/jobs/<volume_id>/` while fence recovery might be needed.
Manual deletion is allowed only after every Job is `CUTOVER_READY`, the
Migration Worker has been permanently removed or disabled, and recovery data
is no longer needed. Use approved Drive9 administration tooling and target only
the exact per-Job control directory; never delete the shared control root.
