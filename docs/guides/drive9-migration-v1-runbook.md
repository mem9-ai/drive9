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

## Public container image

The manually triggered `publish-migration-image.yml` workflow builds Linux
AMD64 and ARM64 images from `main` and publishes one multi-architecture manifest
to:

```text
ghcr.io/drive9-ai/drive9-migration:<source-sha7>
```

The workflow does not publish `latest`. Use the published commit tag directly.

The Kubernetes example remains pinned to `3a6d226` for its tested T0/T1 trial.
That image predates ConfigMap-driven `CUTOVER_READY`. Before T2, replace both
image fields with a published source tag that contains ConfigMap-driven cutover;
otherwise the init container and Worker reject the requested phase. A failed
rollout is never evidence that T2 completed.

The `mem9-ai/drive9` repository must provide the Actions secrets
`DRIVE9_AI_GHCR_USERNAME` and `DRIVE9_AI_GHCR_TOKEN`. The token must belong to an
account authorized to publish packages in `drive9-ai` and needs only the GitHub
Packages `write:packages` scope. Never put this token in configuration, source,
workflow output, or the container image.

GitHub creates the package as private on its first publication. A `drive9-ai`
owner must then open the `drive9-migration` package settings and change its
visibility to Public. This is a one-time and irreversible visibility change.
Afterward, verify from an environment that is not logged in to GHCR:

```bash
docker pull ghcr.io/drive9-ai/drive9-migration:<source-sha7>
docker run --rm \
  ghcr.io/drive9-ai/drive9-migration:<source-sha7> --help
```

The image entrypoint is `drive9-migration`. A Worker container therefore uses
arguments `run -f /etc/drive9-migration/config.yaml`; the mounts and environment
described below remain required.

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

## Kubernetes single-PVC trial

Use the [single-file Kubernetes manifest](../examples/drive9-migration/kubernetes.yaml)
for one existing EBS PVC on one Node. Before applying it:

1. Set the Drive9 endpoint and Owner API key for an empty test Space.
2. Replace `volume_id` with the EBS PV's `.spec.csi.volumeHandle`.
3. Set both `node_name` and `kubernetes.io/hostname` to the Node that owns the
   PVC.
4. Replace `claimName` with the existing PVC name. Apply every resource to that
   PVC's namespace because Kubernetes does not allow cross-namespace PVC
   mounts.
5. Keep both PVC declarations read-only. The initContainer runs `plan`; the
   Worker starts only when that preflight succeeds.

The example intentionally uses UID/GID 0 for both containers. This is a
restricted migration exception: the projected credential is mode `0600`, and
the existing Source may expose only customer-approved root-readable paths. The
profile was exercised by the dev single-PVC trial with a read-only PVC,
read-only root filesystem, no privilege escalation, all Linux capabilities
dropped, and the runtime-default seccomp profile. It does not prove that every
customer permission layout is readable.

The example deliberately omits `fsGroup` because kubelet ownership adjustment
could mutate an existing Source PVC. Before deployment, verify that UID 0 with
no added capabilities can read every supported file and traverse every Source
directory. If `plan` reports a Source read-access failure, stop and have the
customer grant narrowly scoped read/traverse mode or ACL access. Do not
recursively change Source ownership and do not add broad capabilities merely to
make preflight pass.

Apply and inspect the trial with:

```bash
migration_namespace=<pvc-namespace>
kubectl -n "$migration_namespace" apply \
  -f docs/examples/drive9-migration/kubernetes.yaml
kubectl -n "$migration_namespace" logs daemonset/drive9-migration -c plan
kubectl -n "$migration_namespace" rollout status daemonset/drive9-migration
kubectl -n "$migration_namespace" exec daemonset/drive9-migration \
  -c drive9-migration -- \
  /drive9-migration status --output json
```

The inline Secret is for a short-lived trial file only. For repeatable or
production use, remove that Secret document and create the same
`drive9-migration-credentials` Secret from a protected local file or secret
manager. Never commit the populated manifest.

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

For the Kubernetes manifest above, run:

```bash
kubectl -n "$migration_namespace" patch configmap drive9-migration \
  --type merge \
  --patch '{"data":{"phase":"DUAL_WRITE_REPAIRING"}}'
kubectl -n "$migration_namespace" rollout restart \
  daemonset/drive9-migration
kubectl -n "$migration_namespace" rollout status \
  daemonset/drive9-migration
kubectl -n "$migration_namespace" exec daemonset/drive9-migration \
  -c drive9-migration -- \
  /drive9-migration status --output json
```

After restart, require `recovery_complete=true` before using convergence
claims. The Worker performs a new deep recovery round and derives a fresh
in-memory repair mtime floor. Migration does not decide whether the business
rollout has completed.

In `DUAL_WRITE_REPAIRING`, automatic repair fails closed if an existing target
mode differs from the Source mode, or if a newly discovered regular file does
not use mode `0644`. Resolve such mode differences through the business
dual-write path before cutover; do not force-clear Attention.

## T1: external rollout confirmation and full verification

T1 is an external customer signal that every business Pod runs the dual-write
version; it is not a Migration phase. For every Job, invoke:

```text
drive9-migration verify-full
drive9-migration status --output json
```

From the Kubernetes Worker, run:

```bash
kubectl -n "$migration_namespace" exec daemonset/drive9-migration \
  -c drive9-migration -- \
  /drive9-migration verify-full
kubectl -n "$migration_namespace" exec daemonset/drive9-migration \
  -c drive9-migration -- \
  /drive9-migration status --output json
```

The verification is process-local, scans every current Source path without an
mtime filter, and does not trigger cutover. A normal restart clears it and
requires another explicit verification. A rollout configured with
`phase: CUTOVER_READY` automatically runs a fresh verification after deep
recovery. Target-only residue is a warning and does not by itself fail one-way
verification.

## T2: irreversible cutover fence

T2 remains a customer decision after business observation. For Kubernetes,
request it through the same ConfigMap and rollout mechanism used at T0:

```bash
kubectl -n "$migration_namespace" patch configmap drive9-migration \
  --type merge \
  --patch '{"data":{"phase":"CUTOVER_READY"}}'
kubectl -n "$migration_namespace" rollout restart \
  daemonset/drive9-migration
kubectl -n "$migration_namespace" rollout status \
  daemonset/drive9-migration
```

Each restarted Worker requires an existing `DUAL_WRITE_REPAIRING` Checkpoint,
runs a new deep recovery and full verification, then executes the irreversible
fence protocol. `kubectl rollout status` confirms only that the replacement
Pods started; it is not the T2 completion gate.

The aggregate gate requires the released `kubectl-drive9-migration` client-side
plugin, `jq` 1.6 or later, and `yq` v4. Install the platform plugin artifact as
`kubectl-drive9-migration` on `PATH`, then confirm that
`kubectl drive9 migration --help` succeeds. The manifest labels every Worker
with batch `single-pvc-trial` and names its Worker container
`drive9-migration`, which are required for plugin discovery.

Run this exact gate. `yq` reads the complete declared Job ID set from the
ConfigMap, the plugin queries every labeled observed Worker, and `jq` requires
an exact unique ID-set match plus completed fencing:

```bash
(
  set -euo pipefail
  migration_batch=single-pvc-trial
  expected_job_ids_json="$(
    kubectl -n "$migration_namespace" get configmap drive9-migration \
      -o jsonpath='{.data.config\.yaml}' | \
      yq -o=json '.jobs | map(.volume_id)'
  )"
  kubectl drive9 migration status \
    -n "$migration_namespace" \
    --batch "$migration_batch" \
    -o json | \
    jq -e --argjson expected "$expected_job_ids_json" '
      ([.jobs[].job_id] | sort) as $observed
      | ($expected | sort) as $declared
      | ($observed == $declared)
        and (($observed | length) == ($observed | unique | length))
        and all(.jobs[];
          .status == "CUTOVER_READY"
          and .worker_status.phase == "CUTOVER_READY"
          and .worker_status.fence_complete == true)
    '
)
```

The subshell exits nonzero for a missing ConfigMap, missing or duplicate Job,
unreachable Worker, plugin collection failure, ID-set mismatch, incomplete
phase, or incomplete fence. If the plugin is not installed, T2 is blocked. Only
after this command exits zero may the business switch to Drive9-only.

The existing explicit per-Job path remains available and idempotent. It requires
a current passed full verification, a converged latest round, no Attention, and
no grace, retry, pending, or in-flight repair work:

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
