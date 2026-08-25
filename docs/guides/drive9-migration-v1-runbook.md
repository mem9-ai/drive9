---
title: Drive9 Migration V1 Operator Runbook
---

# Drive9 Migration V1 operator runbook

## Safety boundary

Drive9 Migration V1 moves configured subpaths from one mounted, read-only EBS
Source into independent Drive9 Space/Prefix Jobs. It has no rollback or unfence.
After T0 it never propagates Delete or Rename, so old target paths can remain as
non-blocking residue.

The prominent V1 limits are post-T0 residue, metadata loss, per-Job throttling,
residual ABA risk, one subpath Source Root per Job, and no rollback. V1
preserves supported path, link, content, and `mode & 0777` semantics. It does
not preserve UID/GID,
timestamps, xattrs, ACLs, special bits, or sparse layout. Revision reuse after
delete/recreate retains a residual ABA overwrite risk. Throttling is per Job,
not EBS-wide, and Jobs have no cross-Job transaction. With N active Jobs, the
process may use up to N times the configured per-Job bandwidth and worker count.

## Public container image

The manually triggered `publish-migration-image.yml` workflow builds Linux
AMD64 and ARM64 images from the selected repository branch and publishes one
multi-architecture manifest to:

```text
ghcr.io/drive9-ai/drive9-migration:<source-sha7>
```

The workflow does not publish `latest`. It reports the source ref and commit;
use the published commit tag directly. Non-main publication is intended for
explicitly selected development and E2E refs.

Dispatch only a trusted repository ref: the workflow executes that ref's
workflow and Dockerfile while using a GHCR write credential.

The Kubernetes example uses `REPLACE_WITH_V4_SOURCE_TAG`. Replace both image
fields with the same published source tag containing the strict v4 subpath
contract. A v3 binary rejects the v4 example, and a failed rollout is never
evidence that T2 completed.

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
strict `version: v4` shape. Every node name selects exactly one EBS Source, and
one process runs all Jobs nested beneath that Source. v3 configuration and
checkpoint v1 are not accepted or converted. The
credential reference is only a Secret-volume filename; the API key itself must
exist only in the read-only Secret volume at
`/var/run/secrets/drive9-migration/<credential_ref>`.

The ConfigMap does not contain `tenant_id`. At each `plan` and `run` startup,
Migration authenticates every configured credential against `/v1/status`,
resolves its actual `tenant_id`, and rejects overlapping Prefixes across
different credential filenames that reach the same Tenant/Space. Every
configured credential must resolve before any Worker starts. After that
batch safety gate succeeds, Job initialization and runtime failures remain per
Job.

The sample demonstrates one EBS with three customer subpath mappings:

| EBS subpath | Drive9 target | Job ID |
| --- | --- | --- |
| `/mnt/ebs/vol-0a/A` | Space `user-a`, Prefix `/` | `vol-0a-user-a` |
| `/mnt/ebs/vol-0a/B` | Space `user-b`, Prefix `/` | `vol-0a-user-b` |
| `/mnt/ebs/vol-0a/C` | Space `user-c`, Prefix `/` | `vol-0a-user-c` |

Subpath names are not copied into target paths. Configured subpaths must be
disjoint; unconfigured EBS paths are ignored. A subpath root must be a real
directory on the EBS device. Hardlinks spanning different subpath Jobs become
independent target files.

Set `DRIVE9_MIGRATION_NODE_NAME` to the local node name. Supply the phase from
exactly one source: a sibling `phase` file beside `config.yaml`, or the accepted
`DRIVE9_MIGRATION_PHASE` fallback. Never place a key in config, argv, or an
environment variable.

Before starting the process, mount its EBS Source read-only, provision every
referenced Secret file with mode `0600`, write `SYNCING` to the phase source,
and run:

```text
drive9-migration plan -f /etc/drive9-migration/config.yaml
drive9-migration run -f /etc/drive9-migration/config.yaml
```

The `plan` JSON is the non-sensitive CSI handoff record: verify `job_id`,
`volume_id`, `node_name`, `ebs_root`, `subpath`, `source_root`, `space_ref`,
`prefix`, `credential_ref`, source identity, limits, target emptiness, and
required capabilities for every Job. It never returns an API key. Plan retains
all Job results and exits nonzero if any Job fails.

An older Server that omits `/v1/status.tenant_id` is unsupported for this v4
multi-Job flow. Credential rotation is accepted only when the replacement key
returns the same process-lifetime `tenant_id`; a different tenant fails the Job
closed before the client is swapped.

## Kubernetes single-PVC trial

Use the [single-file Kubernetes manifest](../examples/drive9-migration/kubernetes.yaml)
for one existing EBS PVC on one Node. Before applying it:

1. Set the Drive9 endpoint and one Owner API key for each empty test Space.
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

## Aggregate status with the kubectl plugin

Label the Migration DaemonSet and its Pod template so the client-side plugin can
discover Workers. Use one instance value for one customer migration batch:

```yaml
metadata:
  labels:
    app.kubernetes.io/name: drive9-migration
    app.kubernetes.io/component: worker
    app.kubernetes.io/instance: customer-a-20260810
spec:
  template:
    metadata:
      labels:
        app.kubernetes.io/name: drive9-migration
        app.kubernetes.io/component: worker
        app.kubernetes.io/instance: customer-a-20260810
```

The Worker container name must be `drive9-migration`. Download the plugin
artifact for the operator's platform together with
`migration-kube-plugin-checksums.txt`. Verify the downloaded artifact before
installing it. For Linux:

```bash
plugin_artifact=kubectl-drive9-migration-linux-amd64
grep " $plugin_artifact$" migration-kube-plugin-checksums.txt |
  sha256sum --check
```

For macOS:

```bash
plugin_artifact=kubectl-drive9-migration-darwin-arm64
grep " $plugin_artifact$" migration-kube-plugin-checksums.txt |
  shasum --algorithm 256 --check
```

For Windows PowerShell:

```powershell
$artifact = "kubectl-drive9-migration-windows-amd64.exe"
$line = Get-Content migration-kube-plugin-checksums.txt |
  Where-Object { $_ -match "  $([regex]::Escape($artifact))$" }
if (-not $line) { throw "checksum entry not found" }
$expected = ($line -split '\s+')[0].ToLowerInvariant()
$actual = (Get-FileHash -Algorithm SHA256 $artifact).Hash.ToLowerInvariant()
if ($actual -ne $expected) { throw "checksum mismatch" }
```

Choose the artifact name matching the operator's architecture. After successful
verification, rename it to `kubectl-drive9-migration`
(`kubectl-drive9-migration.exe` on Windows), make it executable where
applicable, and place it on `PATH`. Confirm discovery with:

```bash
kubectl plugin list
```

The zero-argument command queries all labeled Workers in the current namespace:

```bash
kubectl drive9 migration status
kubectl drive9 migration status --batch customer-a-20260810
kubectl drive9 migration status -n migration-system
kubectl drive9 migration status -A
kubectl drive9 migration status -o wide
kubectl drive9 migration status -o json
```

The plugin uses the operator's existing kubectl context and Pod list/exec
permissions. It does not read the Migration ConfigMap or Secret and does not add
RBAC. Each summary deliberately says `observed N jobs`: a Job with no Pod is not
discoverable. Pending, Failed, Terminating, unreachable, and duplicate observed
Pods remain visible and make the command exit nonzero. Attention and incomplete
migration remain status facts and do not make an otherwise complete query fail.

An observed `CUTOVER_READY` summary does not prove that every configured Job was
discovered, does not prove external T1, and does not authorize the business
switch. The T1 and T2 gates below remain authoritative.

## SYNCING and rollout readiness

Inspect each Job independently:

```text
drive9-migration status --output json
drive9-migration diff --job-id <job-id> --output jsonl
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
drive9-migration verify-full --job-id <job-id>
drive9-migration status --job-id <job-id> --output json
```

From the Kubernetes Worker, run:

```bash
kubectl -n "$migration_namespace" exec daemonset/drive9-migration \
  -c drive9-migration -- \
  /drive9-migration verify-full --job-id <job-id>
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
      yq -o=json '[.ebs_sources[].jobs[].job_id]'
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
drive9-migration prepare-drive9-cutover --job-id <job-id>
drive9-migration status --job-id <job-id> --output json
```

The Worker drains in-flight Migration work, persists Fence Intent, completes
the fence, and reports `CUTOVER_READY`. Once Intent is durable, Migration can
never write business data again; restart only finishes the fence. There is no
rollback, phase regression, or unfence.

## Retained control data

V1 has no automatic cleanup or cleanup command. Keep
`/.drive9-migration/jobs/<job_id>/` while fence recovery might be needed.
Manual deletion is allowed only after every Job is `CUTOVER_READY`, the
Migration Worker has been permanently removed or disabled, and recovery data
is no longer needed. Use approved Drive9 administration tooling and target only
the exact per-Job control directory; never delete the shared control root.
