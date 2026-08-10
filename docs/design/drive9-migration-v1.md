---
title: Drive9 Migration V1 Design
status: accepted
source_status: accepted
audience: implementation-agents
source_document: https://pingcap.feishu.cn/wiki/XDKnwKGk7iMwBtkfOvcc3lgBnWe
source_revision: 1517
scope_class: large
production_net_loc: "2480-3820"
repository_amendment: memory-only-worker-state-no-multipart-resume
implementation_plans:
  - .sisyphus/plans/drive9-migration-server-v1.md
  - .sisyphus/plans/drive9-migration-cli-v1.md
---

This document is the repository implementation design for Drive9 Migration V1.
It distills the accepted Feishu design at revision 1517 into normative contracts
for implementation agents.

The repository amendment recorded in this file supersedes the local SQLite and
Multipart Resume details from that source revision. V1 keeps operational
working state in memory, restarts from a complete deep recovery round, and
stores only non-regressible Job control state in Drive9.

Local SQLite remains a possible opt-in durability enhancement for a later
iteration. It is not required, implemented, or accepted as part of the initial
V1 path.

The design defines behavior and scope. The implementation plans define task
order, ownership, and acceptance gates; they MUST NOT override this document.
If implementation requires a different state, API behavior, consistency
boundary, persistence model, or V1 scope, stop and record a design deviation
before changing production code.

The terms MUST, MUST NOT, SHOULD, and MAY are normative.

Implementation reading map:

1. Every implementation agent reads sections 1-2 and 18 first.
2. Server/Go Client work is defined by sections 10, 13, 19.1, and 20.
3. Migration CLI/Worker work is defined by sections 3-12, 14-17, 19.2, and 20.
4. Section 21 contains deployment inputs; agents MUST NOT convert them into
   implicit product scope.

## 1. Required outcome

Deliver an independent foreground binary, `drive9-migration`, that copies the
current visible namespace of an EBS filesystem into Drive9, keeps it converged
while EBS is authoritative, repairs safe Create/Update differences during the
business dual-write rollout, performs a full per-Job verification, and
establishes an irreversible no-more-Migration-writes fence before the business
switches to Drive9-only writes.

### 1.1 In scope

| Area | V1 contract |
| --- | --- |
| Source | One EBS filesystem already mounted as a local directory on an eligible EKS node |
| Job | One EBS Source Root to one Drive9 `(Space, Prefix)` |
| Parallelism | Multiple independent Jobs may run concurrently |
| Target layouts | One EBS per Space, or multiple EBS Jobs in one Space under disjoint Prefixes |
| Synchronization | Repeated full namespace rounds, incremental convergence, complete deep recovery after restart, and fresh conditional uploads |
| Handoff | `SYNCING -> DUAL_WRITE_REPAIRING -> CUTOVER_READY` |
| Verification | EBS-to-Drive9 one-way coverage using whole-file SHA-256 and supported metadata |
| Runtime control | Startup configuration plus local single-Job Unix socket commands |
| Deployment assumption | Existing Kubernetes operations manage a DaemonSet; one eligible node, one EBS, one Worker, one Job |
| CSI | Reuse the existing CSI driver and its Secret/`remote-root`/RWX behavior without code changes |

### 1.2 Explicitly out of scope

1. Non-EBS sources, including generic Local, S3-compatible, GCS, and Azure.
2. EBS snapshots, deleted-file history, or data outside the configured Source
   Root.
3. A `drive9` migration subcommand, central Migration Service, network control
   API, Web UI, Operator, CRD, Kubernetes Condition, or additional RBAC.
4. DaemonSet, Helm, IAM, EBS attach/mount, CSI Secret/PVC creation, or other
   deployment automation.
5. Strict-stop and direct Best-effort cutover implementations. They are future
   options and MUST NOT appear in V1 CLI, phases, tests, or acceptance claims.
6. Post-T0 Delete/Rename propagation, reverse synchronization, automatic merge,
   rollback, unfence, or phase rollback.
7. Cross-Job transactions, distributed leases, HA ownership, multi-Worker Jobs,
   batch-wide throttling, or Space-wide throttling.
8. Prefix-scoped credentials, new token types, or per-node hard isolation of a
   batch Secret.
9. Stable Resource/Dentry identity CAS or elimination of the accepted
   Revision-only ABA risk.
10. Event persistence, history queries, automatic alerting, or event retry
    workers in Drive9 Server.
11. Automatic cleanup of Migration control data.
12. Local SQLite, PVC-backed Worker state, persisted per-file state, and
    Multipart upload-session recovery in the initial V1 path. SQLite may be
    reconsidered later as an opt-in durability enhancement.
13. A new checksum-aware Multipart Resume contract. Existing generic
    `drive9 cp` Resume behavior remains unchanged but is not used by Migration.

### 1.3 Effort boundary

Production Net LoC counts additions and modifications to production code only.

| Delivery surface | Production Net LoC |
| --- | ---: |
| Go Client Migration adapter | 180-240 |
| Independent Migration CLI and Worker | 2300-3580 |
| Total in this repository | 2480-3820 |

The Drive9 Server Wire Contract is implemented in the external Server
repository and is not part of this repository's Production Net LoC.

A new source type, state, control plane, persistent Server schema, watcher,
rollback path, distributed ownership mechanism, CSI change, or post-T0
Delete/Rename primitive is scope expansion.

## 2. Non-negotiable invariants

| ID | Invariant |
| --- | --- |
| `INV-01` | One Job has exactly one EBS Source Root and one target `(Space, Prefix)`. |
| `INV-02` | Every business-data read from EBS is read-only; Migration never mutates EBS. |
| `INV-03` | A Job never reads, verifies, mutates, or deletes business data outside its Target Prefix. |
| `INV-04` | EBS remains Migration's authority through T2. The business remains EBS-primary for reads until the cutover fence is complete for every Job. |
| `INV-05` | The only V1 phases are `SYNCING`, `DUAL_WRITE_REPAIRING`, and `CUTOVER_READY`. T1 is not a phase. |
| `INV-06` | Phase requests are explicit. A startup `CUTOVER_READY` request runs verification and the fence protocol; conditions never advance a phase by themselves. |
| `INV-07` | From T0 onward, Migration never calls Drive9 Delete or Rename. |
| `INV-08` | Create uses Must-Not-Exist/expected Revision 0. Update uses the exact observed positive Revision. A conflict never falls back to an unconditional write. |
| `INV-09` | An incomplete source scan never implies deletion and never produces Delete work. |
| `INV-10` | Immutable Job identity, highest applied phase, fence intent, and the final fence survive restart through one conditionally updated remote Checkpoint. Round state, `repair_mtime_floor`, findings, verification state, and upload state do not survive restart. |
| `INV-11` | API keys and file contents never enter ConfigMap, argv, environment, memory snapshots, Checkpoints, status, or ordinary logs. |
| `INV-12` | `status`, `diff`, `verify-full`, and `prepare-drive9-cutover` control one local Job through a mode-0600 Unix socket. |
| `INV-13` | Diff event reporting is asynchronous and non-blocking. Reporter failure cannot change scan, repair, phase, verification, or fence behavior. |
| `INV-14` | Post-T0 verification is one-way coverage: every current EBS path must match Drive9; target-only residue is a warning, not a failure by itself. |
| `INV-15` | Jobs are independent. Partial batch progress never rolls back a successful Job. |
| `INV-16` | Migration reuses `pkg/client` transfer, fresh Multipart, filesystem, and CAS code. It never resumes or adopts a prior Multipart session, shells out to `drive9 cp`, or reimplements the upload protocol. |

## 3. Domain model and target mapping

### 3.1 Terms

| Term | Definition |
| --- | --- |
| Job | The smallest state, recovery, rate-limit, verification, and fence unit |
| `job_id` | Stable V1 Job identifier; exactly equal to `volume_id` |
| `volume_id` | AWS EBS volume ID in `vol-xxxx` form; no separate Job ID is configured |
| Source Root | The mounted EBS directory included in the migration |
| Space | Configuration reference to one Drive9 Tenant filesystem |
| Prefix | The Job-owned business path inside a Space; not an authorization boundary |
| Source Version Token | EBS identity/change tuple: device, inode, type, size, nanosecond mtime, nanosecond ctime, and mode |
| Sync Round | One complete source scan, diff, allowed apply, target reread, and in-memory result commit |
| Checkpoint | Minimal durable record for immutable identity and non-regressible Job control state in Drive9 |
| T0 | Start of the dual-write rollout procedure |
| T1 | External confirmation that every business Pod runs the dual-write version |
| T2 | External decision to fence Migration and switch the business to Drive9-only writes |

### 3.2 Path mapping

For a source entry `sourceRoot/relative/path`:

~~~text
target path = normalize(target Prefix + relative/path)
~~~

The Source Root directory name itself is not copied. For example:

~~~text
/ebs/xxxx/a/b.txt -> Space-001:/a/b.txt
/ebs/xxxx/a/b.txt -> Shared:/vol-001/a/b.txt
~~~

### 3.3 Supported layouts

| Layout | Mapping | Isolation |
| --- | --- | --- |
| One Space per EBS | `vol-001 -> Space-001:/` | Tenant/Space, credential, and fault-domain isolation |
| Shared Space | `vol-001 -> Shared:/vol-001` | Path-view isolation only |

Prefix rules:

1. Paths MUST use Drive9 absolute, UTF-8, NFC-normalized path semantics.
2. Backslashes, `.`, and `..` segments are invalid.
3. Two Jobs in one Space MUST NOT use equal or ancestor/descendant Prefixes.
4. If two or more Jobs share a Space, no Job may target `/`.
5. A first run requires an absent or empty business Target Prefix. A restart
   may continue only when the remote Checkpoint identifies the same Job and
   immutable Source/Target configuration.
6. `/.drive9-migration/` is reserved control space. A non-root Target Prefix
   may not equal or descend from it.
7. A source relative path that maps into the reserved control Prefix is a
   blocker; Migration MUST NOT rename it automatically.
8. A root Target Prefix uses `/.drive9-migration/` as an explicit carve-out
   from business scanning, mutation, and verification.
9. Pre-T0, the Target Prefix is exclusive to Migration. From T0 through T2,
   only this Job and the designated business writer may mutate it.
10. Plan, Checkpoint, and status MUST retain
    `volume_id -> Space -> Prefix -> credential_ref`.

A shared Prefix is not a security boundary. The Owner API key can access the
entire Tenant/Space.

## 4. Runtime architecture

~~~text
Operator-managed Kubernetes
  ConfigMap snapshot + Secret Volume + DaemonSet rollout + optional kubectl exec
                                   |
                                   v
                         drive9-migration
  +----------------+----------------+----------------+----------------+
  | config / Job   | EBS scanner    | in-memory Job  | local UDS      |
  | resolver       | and token      | state/rounds   | control        |
  +----------------+----------------+----------------+----------------+
  | diff/reconcile | Job rate limit | checkpoint     | event reporter |
  +----------------+----------------+----------------+----------------+
                                   |
                                   v
                        existing pkg/client
       conditional upload / fresh Multipart / fs operations / events
                                   |
                                   v
                            Drive9 Server
~~~

Runtime boundaries:

1. `drive9-migration run` is a foreground Worker process.
2. A Worker selects exactly one declared Job using
   `DRIVE9_MIGRATION_NODE_NAME`, populated from `spec.nodeName` through the
   Downward API. `volume_id`, not `node_name`, is the stable Job identity.
3. The Worker does not call the Kubernetes API and needs no Migration-specific
   RBAC.
4. The Worker exposes no network listener. Local commands use one mode-0600
   Unix Domain Socket with bounded JSON, deadlines, and serialized mutations.
5. One Worker owns one Job. Internal small-file, large-file, and Multipart work
   may be concurrent.
6. V1 has no SQLite, local database, or PVC requirement. Per-round, per-path,
   finding, verification, and upload state lives only in process memory.
7. The remote Checkpoint contains only immutable identity and non-regressible
   Job control state. After every start, the Worker completes a deep full
   recovery round before normal rounds or any convergence claim.
8. Duplicate Workers are not resolved by election. Checkpoint or target
   ownership conflicts fail closed.

## 5. CLI and configuration contract

### 5.1 Commands

~~~bash
drive9-migration plan -f /etc/drive9-migration/config.yaml
drive9-migration run -f /etc/drive9-migration/config.yaml
drive9-migration status --output json
drive9-migration diff [--type <type>] [--limit <n>] --output jsonl
drive9-migration verify-full
drive9-migration prepare-drive9-cutover
~~~

| Command | Contract |
| --- | --- |
| `plan` | Read-only batch-static validation plus dynamic preflight for the selected local Job; no Drive9 business-data mutation |
| `run` | Start the foreground Worker for the Job selected on this node |
| `status` | Query current local Job state and summaries; differences still return success |
| `diff` | Stream detailed findings as JSONL |
| `verify-full` | Run one in-memory full verification inside `DUAL_WRITE_REPAIRING`; phase does not change |
| `prepare-drive9-cutover` | Execute the irreversible per-Job fence protocol |

The four local commands do not accept `job_id` because one Pod contains one Job.

### 5.2 Exit codes

| Code | Meaning |
| ---: | --- |
| `0` | Query or operation succeeded, including idempotent replay |
| `1` | Argument, configuration, or internal execution error |
| `2` | Illegal phase or operation, including unknown phase, dual phase sources, rollback, early request, or skipped phase |
| `3` | The local Worker socket is unavailable |

### 5.3 ConfigMap schema

`phase` and `config.yaml` are both startup snapshots.

~~~yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: drive9-migration
data:
  phase: SYNCING
  config.yaml: |
    version: v3
    drive9:
      endpoint: https://drive9.example.com
    job_defaults:
      sync:
        grace_period: 60s
      performance:
        max_bytes_per_second: 209715200
        small_file_workers: 16
        large_file_workers: 2
    spaces:
      space-001:
        credential_ref: space-001-key
    jobs:
      - volume_id: vol-001
        node_name: ip-10-0-1-10
        source:
          type: ebs
          root: /ebs/xxxx
        target:
          space_ref: space-001
          prefix: /
~~~

Shared-Space Jobs use the same schema and distinct Prefixes such as
`/vol-001` and `/vol-002`.

Configuration rules:

1. ConfigMap changes are not hot-loaded. Every change requires a DaemonSet
   rollout restart.
2. `phase` accepts `SYNCING`, `DUAL_WRITE_REPAIRING`, or `CUTOVER_READY`.
   `CUTOVER_READY` is a startup request, not proof that the actual phase has
   changed. It requires an existing `DUAL_WRITE_REPAIRING` Checkpoint and runs a
   new deep recovery, full verification, and the same fence protocol used by
   `prepare-drive9-cutover`.
3. By default, phase is read from the file named `phase` beside the `-f`
   configuration file. `DRIVE9_MIGRATION_PHASE` MAY replace that file at
   startup. Supplying both sources, or neither source, is an error. API keys
   MUST NOT use environment variables.
4. All Jobs use the same `job_defaults`. V1 rejects per-Job overrides.
5. `grace_period` defaults to 60 seconds, accepts 30 seconds through 10 minutes,
   and is immutable while a Job runs.
6. `max_bytes_per_second` is the total upload limit for each Job, not each
   internal worker and not the batch.
7. `config_hash` covers the parsed static Source, Target, endpoint,
   `space_ref -> credential_ref` mapping, and `job_defaults`. It excludes phase
   and API-key values.
8. A recovered Job whose static configuration or Source/Target identity differs
   from its Checkpoint fails closed.
9. Repeating the current phase is idempotent. Before fence intent, requesting a
   phase lower than the highest applied phase is rejected. Fence state never
   regresses, regardless of a later ConfigMap rollback.
10. The shared ConfigMap phase is a batch-wide desired startup phase, but each
    Worker applies it independently during rollout.
11. A configured `CUTOVER_READY` request does not directly update
    `highest_phase`. Actual phase remains `DUAL_WRITE_REPAIRING` until durable
    Fence Intent and Fence Complete succeed.

### 5.4 Credentials

The ConfigMap stores only `credential_ref`. The API key is read from:

~~~text
/var/run/secrets/drive9-migration/<credential_ref>
~~~

Rules:

1. The Secret Volume is read-only and MUST NOT use `subPath`, so kubelet can
   update projected files.
2. The Worker reloads the current Job's key after file change or authentication
   failure.
3. A batch Secret may contain multiple keys. Every DaemonSet Pod can
   technically read all mounted keys; V1 accepts this because the Worker is
   trusted.
4. Deleting a Kubernetes Secret does not revoke the underlying Owner API key.
5. Migration Secret layout is not the CSI Secret layout and cannot be reused
   directly as one CSI Secret.

### 5.5 Plan and startup preflight

`plan` and `run` share the same preflight implementation. `run` MUST repeat it
before its first business-data mutation; a previous `plan` result is never
trusted as a startup authorization.

Batch-static checks use only configuration and therefore cover every declared
Job:

1. Strictly decode schema version v3 and reject unknown fields, duplicate IDs,
   malformed `volume_id` values, unsupported Source types, and per-Job default
   overrides.
2. Resolve exactly one Job for `DRIVE9_MIGRATION_NODE_NAME` and reject duplicate
   active Job identity.
3. Normalize and validate every Space/Prefix mapping, including cross-Job
   overlap and the control Prefix carve-out.

Dynamic probes cover only the selected local Job because a Worker cannot access
another node's EBS:

1. Validate Source Root existence, traversal, and read access.
2. Verify EBS serial or `/dev/disk/by-id` against `volume_id` when available.
   Otherwise report `volume_identity_verified=false`.
3. Validate the selected credential file and Drive9 authentication without
   exposing the key.
4. Verify all required Server capabilities before business-data mutation.
   Event ingestion capability is optional and non-blocking.
5. Confirm first-run Target Prefix emptiness or valid same-Job recovery state.
6. Count source entries and logical bytes and report the observed namespace
   size. `plan` does not retain that inventory after returning.
7. Emit the selected Job's non-sensitive CSI handoff mapping.

Batch readiness is external: the Kubernetes operations layer runs this dynamic
preflight on every eligible node and aggregates the per-Job results.

## 6. Phase, condition, and milestone model

### 6.1 Phase graph

~~~text
startup
  |
  +-- configured SYNCING ----------------------> SYNCING
  |
  +-- configured DUAL_WRITE_REPAIRING --------> DUAL_WRITE_REPAIRING
                                                   |
  +-- configured CUTOVER_READY --------------------+ startup recovery +
                                                   | verify-full + fence
                                                   |
                                                   | or prepare-drive9-cutover
                                                   v
                                             CUTOVER_READY
~~~

At startup, the Worker loads the minimal Checkpoint, validates monotonicity,
then conditionally applies the configured non-regressing phase. A configured
`CUTOVER_READY` value is handled specially: the Checkpoint must already be at
least `DUAL_WRITE_REPAIRING`; the Worker keeps that actual phase, completes deep
recovery and a fresh full verification, and only then executes the fence
protocol. Every convergence condition remains false until recovery succeeds.
`ReadyForRollout` and `Attention` are not startup gates. There is no phase
watcher; ConfigMap changes require a rollout restart.

If a durable fence intent exists, recovery may only finish fencing. It may not
resume a writable phase.

### 6.2 Phase semantics

| Phase | Mutation policy | Namespace policy |
| --- | --- | --- |
| `SYNCING` | Conditional Create/Update plus exclusive Delete; Rename converges as create-new then delete-old | Exact convergence of the exclusive business Prefix |
| `DUAL_WRITE_REPAIRING` | Conditional Create/Update only | EBS one-way coverage; Delete skipped, Rename not inferred, target-only paths warned |
| `CUTOVER_READY` | No Migration mutation of Drive9 | Read-only inspection and reporting only |

EBS is authoritative for Migration in the first two phases.

### 6.3 Job conditions

| Condition | True only when |
| --- | --- |
| `ReadyForRollout` | Phase is `SYNCING`; startup recovery completed; initial copy completed; latest in-memory complete Round converged; `Attention=false` |
| `CurrentConverged` | Phase is `DUAL_WRITE_REPAIRING`; startup recovery completed; latest in-memory fast or full Round converged; no Source-token candidate, grace candidate, blocker, backlog, pending repair, or in-flight repair; `Attention=false` |
| `Attention` | An unsafe/unrecoverable error exists, or the same retryable operational blocker has remained unresolved for five minutes |

Condition rules:

1. Conditions update automatically but never change phase.
2. New differences, backlog, in-flight work, restart, or an incomplete Round
   clear the applicable convergence condition.
3. Ordinary differences and grace candidates do not set `Attention` by
   themselves.
4. Unsafe errors set `Attention` immediately.
5. Migration clears `Attention` only after it rechecks that the blocker is
   gone. There is no force-clear command.
6. `Attention=true` forces `CurrentConverged=false` and rejects cutover, but
   does not roll back phase or clear an existing fence.
7. `CurrentConverged` is evidence about the latest Round, not an observation
   duration, T1 proof, or cutover decision.

### 6.4 T0/T1/T2 operator sequence

| Interval | Business behavior | Required operations | Migration behavior |
| --- | --- | --- | --- |
| Before T0 | EBS primary read/write | Run repeated `SYNCING` rounds; optionally use `ReadyForRollout` as a Runbook signal | Full rounds, exact exclusive convergence |
| T0 | Prepare business dual-write rollout | Set ConfigMap phase to `DUAL_WRITE_REPAIRING`; rollout restart the Migration DaemonSet; wait until every new Worker reports the actual phase; only then roll out business Pods | Persist the higher phase, run a complete deep recovery round, derive the in-memory floor, and start repair |
| T0-T1 | Old Pods write EBS; new Pods write EBS and Drive9; EBS remains read authority | Complete the rolling business deployment | Fast rounds, grace, stable-source checks, conditional Create/Update; no Delete/Rename |
| T1 | Every business Pod is externally confirmed dual-write | Record T1 outside Migration; invoke `verify-full` once per Job | Phase remains `DUAL_WRITE_REPAIRING`; serialize a full verification after the current fast Round |
| T1-T2 | Business remains EBS-primary and dual-writes Drive9 | Observe per-Job status and diff; the external operator chooses duration and T2 | Continue fast repair after full verification |
| T2 | Prepare Drive9-only writes | Set ConfigMap phase to `CUTOVER_READY` and rollout restart the Migration DaemonSet, or invoke `prepare-drive9-cutover` per Job; use the external kube plugin to wait until all Jobs are actually `CUTOVER_READY` with `fence_complete=true`; then switch the business | Run fresh startup recovery and verification for the ConfigMap path, then permanently fence Migration writes |

Migration cannot detect whether all business Pods are dual-write, cannot select
T1 or T2, and cannot aggregate a batch decision. DaemonSet rollout and per-Job
fencing are not atomic. A partial result pauses the external Runbook; successful
Jobs retain their progress.

The business writer owns retry and error handling when an application operation
succeeds on only one of the EBS or Drive9 directories. Migration does not
participate in application request success; it relies on EBS remaining
authoritative and performs later repair.

## 7. Source model and filesystem semantics

### 7.1 Source Version Token

For each EBS entry:

~~~text
SourceVersion = {
  dev,
  inode,
  type,
  size,
  mtime_ns,
  ctime_ns,
  mode
}
~~~

The scanner uses `lstat` and never follows symlinks. A deep regular-file read
uses stat/open/read/stat and revalidates the token after hashing/upload. Any
change invalidates the result and retries from the new token.

For a conditional upload, the Worker also validates both the opened file and
its current Source path against that same token after every byte used by the
attempt has been read and immediately before a direct write or Multipart
Complete. A failed check prevents Complete; V2 cleanup uses an independent,
bounded context, while V1 is abandoned without adopting or resuming the upload.

Two token fields are distinct:

1. `last_observed_source_token` records the latest namespace observation.
2. `last_reconciled_source_token` advances only after successful deep
   verification or repair in the current process.

An ordinary scan MUST NOT advance the reconciled token. A changed token remains
a candidate until deep work succeeds. Restart discards both tokens and forces a
new complete deep recovery round.

### 7.2 Filesystem behavior

| Source object or metadata | V1 behavior |
| --- | --- |
| Regular file | Migrate logical bytes and whole SHA-256 |
| Directory and empty directory | Migrate |
| Mode | Preserve `mode & 0777` |
| setuid, setgid, sticky | Do not preserve; emit a warning |
| Symlink | Preserve link text exactly; do not follow or rewrite; warn for absolute, external, or dangling targets |
| Hardlink | Group by `dev + inode`; upload one primary then create aliases |
| UID/GID, mtime/ctime, xattr, ACL | Do not preserve |
| FIFO, socket, block/character device | Block by default; allow explicit exclusion |
| Nested mount/device change | Do not cross by default; block or explicitly exclude |
| Sparse file | Copy logical bytes; sparse layout is not preserved |
| Invalid UTF-8 path or symlink target | Block |
| NFC collision | Block; never auto-rename |

Symlink target classification is metadata-only. Relative targets are resolved
lexically from the link's parent, and each in-root link hop is inspected with
`lstat`/`readlink`; no target directory is enumerated and no target file content
is opened. An absolute target or any hop outside the Source Root is external. A
missing component, non-directory intermediate component, or link cycle is
dangling. Each condition emits the existing symlink-target warning while the
original link text remains unchanged.

Directory creation precedes children. Regular files and hardlink primaries
precede link aliases. Directory permissions are applied last. In `SYNCING`,
deletion proceeds child-before-parent.

## 8. Sync Round contract

A complete Round executes:

1. Validate effective configuration, Source/Target identity, credential,
   required Server capabilities, and the minimal remote Checkpoint.
2. Scan the complete current EBS business namespace into an in-memory
   manifest containing metadata and checksums, never file content.
3. Normalize paths and record type, Source Version Token, supported metadata,
   and checksum reuse facts.
4. Mark `scan_complete=true` only after the full scan succeeds.
5. Compute typed source-only, target-only, content, metadata, link, type,
   identity, and Revision differences only from complete observations.
6. Apply only mutations allowed by the current phase and dependency order.
7. Reread target Revision/checksum and treat a remote write that already
   matches EBS as converged.
8. Atomically replace the current in-memory Round result, then update phase
   conditions.

`round_converged=true` requires:

1. `scan_complete=true`.
2. No grace candidate.
3. No blocking difference.
4. No retry backlog.
5. No pending or in-flight repair.
6. The result was committed as the current in-memory Round.

On scan interruption, `scan_complete` and `round_converged` remain false.
Missing entries from that scan MUST NOT generate Delete work. A per-path blocker
may allow independent siblings to finish, but the Round fails.

## 9. Phase-specific reconciliation

### 9.1 `SYNCING`

The Target Prefix is exclusive. Each complete Round may:

1. Create missing directories, files, and links.
2. Conditionally update changed objects and supported metadata.
3. Delete target-only business paths child-before-parent.
4. Converge a Rename as Create at the new path followed by Delete at the old
   path.
5. Set `ReadyForRollout=true` only after startup recovery, initial copy, and
   the latest complete in-memory Round converge.

`ReadyForRollout` is informational. Startup configuration may request
`DUAL_WRITE_REPAIRING` even when it is false.

### 9.2 Entering `DUAL_WRITE_REPAIRING`

On the first accepted startup in this phase, the Worker MUST first
conditionally persist the higher phase and immutable Job/config identity in
the remote Checkpoint. It then runs a complete deep recovery round. Only after
that round succeeds does it derive an in-memory `repair_mtime_floor`:

~~~text
repair_mtime_floor = recovery_round.started_at - grace_period
~~~

The floor is immutable only for the current Worker process. Restart discards
it and repeats the complete deep recovery round before deriving a new floor or
starting fast rounds.

### 9.3 Fast-Round candidate selection

Every fast Round still performs complete namespace traversal and `lstat` on
every entry. Deep processing—content read, SHA-256, Drive9 lookup, and possible
repair—runs only when at least one condition holds:

1. `mtime >= repair_mtime_floor`.
2. Current Source Version Token differs from
   `last_reconciled_source_token`.
3. The path is new to the current in-memory baseline.
4. The path is an active grace candidate.
5. The path is queued for failed-operation retry.
6. The path is queued after a CAS conflict.

A token-changed path stays pending across ordinary scans until successful deep
work in the current process. Fast rounds reduce source reads, hashing, and Drive9 calls;
they do not reduce namespace `readdir/lstat` cost.

### 9.4 Grace period

A candidate key is:

~~~text
(normalized path, Source Version Token)
~~~

The first stable mismatch records `first_seen_at` and enters one Job-wide
delayed queue. Do not create one goroutine per file.

Only a Source Version Token change restarts the full grace interval. A Drive9
Revision change does not restart it:

1. If Drive9 now matches EBS, clear the candidate.
2. If Drive9 still differs, update target evidence and retain
   `first_seen_at`.

At expiry:

1. Reread current EBS token, Drive9 Revision, type, and checksum.
2. If the EBS token changed, create a new candidate with a new grace interval.
3. If Drive9 matches, clear the candidate and advance the in-memory reconciled
   baseline.
4. If the EBS token is stable and Drive9 still differs, perform one safe
   conditional repair.
5. If EBS disappeared, clear the Create/Update candidate. In
   `DUAL_WRITE_REPAIRING`, report any remaining target path as target-only.

Grace candidates are in-memory only. Restart starts their full interval again.
This may delay convergence but never weakens revalidation. Grace is not a
business observation period, retry budget, or proof that EBS stopped changing.

### 9.5 Conditional Create/Update algorithm

For each stable mismatch:

1. Reread the current EBS token and expected content/metadata.
2. Read Drive9 type, diagnostic Resource ID, Revision, and checksum.
3. If target content and supported semantics already match, adopt the target
   result without writing.
4. If target is absent, create with Must-Not-Exist/expected Revision 0.
5. If target exists and its identity/type is safe, update with the exact
   observed positive Revision.
6. If identity changed, type conflicts, Revision is unknown, or the operation
   lacks a safe conditional primitive, record a blocker and do not write.
7. On CAS conflict, reread both sides and requeue. Never retry
   unconditionally.
8. After a successful write, reread target Revision/checksum and revalidate the
   EBS token before committing the current in-memory result.

The current Server CAS checks Revision, not stable Resource/Dentry identity.
Diagnostic `resource_id` can reveal some changes but is not part of the atomic
condition. If a path is deleted and recreated with the same Revision, an update
may overwrite the replacement object. V1 explicitly accepts this residual ABA
risk and MUST NOT claim otherwise.

### 9.6 Delete and Rename after T0

In `DUAL_WRITE_REPAIRING`:

1. EBS Delete is skipped.
2. Rename is not inferred.
3. A renamed path still present in EBS is treated as a new Create/Update path.
4. Its former Drive9 path may remain readable and consume space.
5. Target-only residue is reported as a warning and does not block one-way
   coverage or convergence by itself.
6. A conflicting object type or otherwise unsafe target remains a blocker.

No Drive9 Server or Client conditional Delete/Rename API is added in V1.

## 10. Checksum and Multipart contract

1. Migration computes whole-file SHA-256 for every deeply processed regular
   file.
2. Small-file writes continue to let Drive9 Server compute SHA-256.
3. Multipart Parts continue to use CRC32C for transport integrity.
4. V1 and V2 Multipart Complete accept an optional worker-supplied
   `checksum_sha256` and persist it atomically with the committed file Revision.
5. The trusted Worker supplies the whole checksum; Drive9 does not reread the
   completed object to prove it independently.
   Only an Owner-authenticated caller may submit a non-empty whole checksum;
   scoped tokens receive 403 and retain existing behavior when the field is omitted.
6. HEAD Stat returns persisted whole-file checksum.
7. BatchStat returns checksum only when `include_checksum=true`; omission
   preserves current cost and response behavior.
8. Post-upload target Revision/checksum reread is mandatory.
9. A migrated file without target checksum cannot pass full verification or
   cutover.
10. Every attempt is a fresh conditional upload. Migration never lists,
    resumes, adopts, or persists a prior Multipart session.
11. A failed or interrupted Multipart attempt is abandoned. The next attempt
    rereads the target Revision, revalidates the Source Token, and starts from
    Part 1 with a new upload ID and the same CAS rule.
12. An ordinary Multipart overwrite that omits `checksum_sha256` clears any
    checksum stored for the previous content. Preserving the old checksum would
    bind it incorrectly to the replacement bytes.
13. A first large-file attempt may read EBS twice: once to compute whole
    SHA-256 and Part checksums, then to upload. V1 accepts the extra I/O and
    possible orphaned incomplete uploads rather than adding local persistence.
14. The Migration Client adapter invokes a Worker-supplied Source-stability
    check after all direct or Multipart reads and before the operation that can
    commit the supplied whole checksum. The check is required for Migration but
    does not change generic upload or Resume behavior.

## 11. Full verification and cutover

### 11.1 `verify-full`

Preconditions:

1. Current phase is `DUAL_WRITE_REPAIRING`.
2. No full verification is already running.
3. Source, credential, and Drive9 query capabilities are available.

Execution:

1. Record an idempotent verification request in memory.
2. Wait for the current fast Round to finish; never overlap both scans.
3. Traverse the full Source Root without the mtime filter.
4. Hash every current EBS regular file and compare every current business path
   with Drive9.
5. Repair only safe Create/Update mismatches through the same stable-source,
   grace, and CAS rules.
6. Require matching type, whole-file SHA-256, `mode & 0777`, and link semantics
   for every EBS path.
7. Record target-only paths as warnings; do not delete them and do not fail
   solely because they exist.
8. Store the result in memory, then resume fast rounds.

`full_verification.status` is one of `pending`, `running`, `passed`, or
`failed` for the current process. An overlapping request is rejected and a
duplicate call after completion returns the current result. Restart discards
the request and result. A normal `DUAL_WRITE_REPAIRING` restart requires the
operator to invoke `verify-full` again; a configured `CUTOVER_READY` startup
request runs a fresh verification automatically after deep recovery.

A passed result covers data only through its completion time. It does not prove
T1, encode an observation interval, or choose T2.

### 11.2 `prepare-drive9-cutover`

The fence protocol has two triggers:

1. `prepare-drive9-cutover` explicitly invokes it against the running Worker.
2. A rollout with startup phase `CUTOVER_READY` first completes deep recovery
   and a fresh full verification, then invokes the same protocol internally.

The explicit command remains supported and idempotent after automatic
completion.

Per-Job preconditions:

1. Phase is `DUAL_WRITE_REPAIRING`.
2. `CurrentConverged=true`.
3. `Attention=false`.
4. Latest in-memory `full_verification.status=passed`.
5. No newer full-verification request is pending, running, or failed.

Neither trigger verifies external T1, business Pod state, or any other Job.
Those are external batch-level gates.

Protocol:

1. Stop scheduling new fast Rounds.
2. Drain current scan and repair work.
3. Recheck all preconditions against the latest Job facts.
4. If the recheck fails before fence intent, resume repair and return an
   explicit error without changing phase.
5. Conditionally persist irreversible fence intent in the remote Checkpoint.
6. After intent, no restart or retry may schedule another Migration write.
7. Persist the complete write fence in the remote Checkpoint, then set
   `CUTOVER_READY`.
8. Duplicate calls after completion succeed idempotently.

A failure before intent is recoverable into `DUAL_WRITE_REPAIRING`. A failure
after intent can only recover by completing the fence. There is no unfence.

`CUTOVER_READY` Workers may perform read-only inspection and report new
differences, but they never repair them. If some Jobs fence and another fails,
the successful Jobs remain fenced. The external Runbook MUST NOT switch the
business until every declared Job is `CUTOVER_READY`.

## 12. State and recovery

### 12.1 In-memory working state

Each Worker keeps its current scan rounds, source and target observations,
observed/reconciled Source tokens, checksums, target Revisions, findings,
conditions, grace candidates, retry queues, `repair_mtime_floor`, verification
request/result, and active upload attempt state in memory.

Rules:

1. V1 uses no SQLite, local database, PVC, or other durable Worker store.
2. Memory contains metadata and checksums but never caches complete file
   content or API keys in diagnostic snapshots.
3. Memory use grows with the observed namespace. The accepted deployment
   expectation is no more than 100,000 directories per Source; total file count
   remains a deployment input that `plan` must report before production use.
4. An incomplete scan never replaces the last complete in-memory Round and
   never implies deletion.
5. Process exit discards all working state. No per-file state, finding,
   verification result, or Multipart upload ID is restored.

### 12.2 Minimal remote Checkpoint

Each Job owns one conditionally updated control record beneath:

~~~text
/.drive9-migration/jobs/<job-id>/
~~~

The Checkpoint contains only:

1. Stable Job identity and immutable `config_hash`, Source, Target, Space, and
   Prefix identity.
2. Highest applied phase.
3. Irreversible fence intent.
4. Complete write fence.

It MUST NOT contain round manifests, per-file tokens or Revisions,
`repair_mtime_floor`, findings, upload sessions, verification requests/results,
reports, credentials, or file content. Conditional update prevents a stale
Worker from regressing phase or fence state.

### 12.3 Recovery rules

1. Load and validate the minimal Checkpoint before any business-data mutation.
2. Reject immutable identity/config mismatch, requested phase rollback, and a
   `CUTOVER_READY` request without an existing `DUAL_WRITE_REPAIRING`
   Checkpoint.
3. If fence intent exists, keep writes disabled and only finish persisting the
   complete fence.
4. Otherwise enter configured `SYNCING` or `DUAL_WRITE_REPAIRING` with every
   condition false. For a configured `CUTOVER_READY` request, restore actual
   `DUAL_WRITE_REPAIRING` without advancing the Checkpoint. Complete one deep
   full recovery round before normal or fast rounds, automatic verification, or
   any convergence claim.
   Recovery completion requires a complete source/target observation, not
   convergence; blockers or grace candidates remain visible and keep the
   applicable condition false.
5. A remote business write committed before process failure is rediscovered by
   the full target reread; a matching object is treated as converged and a
   mismatch follows the current phase's normal CAS rules.
6. In `DUAL_WRITE_REPAIRING`, derive a new in-memory
   `repair_mtime_floor` from the recovery Round. Restart every grace interval.
7. Discard an interrupted `verify-full`. A normal dual-write restart requires
   another operator request; a configured `CUTOVER_READY` startup reruns it
   automatically. Before fence intent, an interrupted explicit cutover command
   is retried by the operator. After fence intent, recovery may only complete
   fencing.
8. A Checkpoint or target ownership conflict indicates a duplicate/stale
   Worker and fails closed with `Attention=true`.

Control data is retained after `CUTOVER_READY`. V1 has no TTL or cleanup
command. Manual cleanup is allowed only after recovery data is no longer needed
and must target the exact per-Job control directory.

## 13. Drive9 Server and Go Client prerequisites

The Drive9 Server Wire Contract is available from the external Server
repository. This repository implements and accepts the matching Go Client
Migration adapter before any Worker task consumes Drive9 APIs.

| Contract | Required behavior |
| --- | --- |
| Stat checksum | Authenticated metadata-only read of persisted whole SHA-256 |
| BatchStat checksum | Opt-in `include_checksum=true`; omitted behavior remains compatible |
| Multipart checksum | Optional validated `checksum_sha256` on V1/V2 Complete, persisted atomically with file Revision |
| Conditional upload | Preserve Revision 0 create and exact positive-Revision update; no unconditional fallback |
| Fresh upload | Carry the whole checksum through a new V1/V2 conditional Multipart completion; Migration does not call Resume |
| Capabilities | Expose bounded Migration-required checksum read/complete capabilities |
| Event endpoint | Owner-authenticated `POST /v1/migration/events` with bounded JSON; Server derives Tenant identity from the authenticated Owner API key |
| Event storage | One structured log and low-cardinality metrics only; no database table |

Required data capabilities are fail-fast preflight gates. Event availability is
reported separately and is optional for data correctness.

The external Server repository owns and freezes exact capability names,
request-model names, payload-size constant, and wire behavior. The current
CLI/Worker plan owns only the matching `pkg/client` adapter described above.
It MUST NOT guess names, duplicate Server wire structs in the Worker, or add a
new Server Wire Contract.

The event endpoint rejects scoped tokens in production deployments. Production
requests use tenant authentication, and caller-supplied checksums and Migration
events require an Owner API key. Local/fallback Server mode is test-only, is not
enabled in production, and is excluded from this V1 production acceptance
surface. The request MUST NOT accept or require a caller-supplied Tenant ID;
the Server resolves Tenant identity from the authenticated Owner API key. No
new token type or Prefix scope is added.

## 14. Observability and local control

### 14.1 `status`

`status --output json` returns current facts for one Job:

1. Actual phase and startup-configured phase.
2. `ReadyForRollout`, `CurrentConverged`, and `Attention` with reasons.
3. Fence intent and complete-fence state.
4. Current in-memory `repair_mtime_floor`, startup recovery state, and latest
   Round mode/timestamps.
5. mtime, Source-token-changed, new-path, and filtered-path counts.
6. Diff counts by class.
7. Grace, backlog, pending, and in-flight repair counts.
8. Event queue depth and sent/failed/dropped counters.
9. `full_verification` status, request/completion times, and diff summary.

Differences do not make `status` exit nonzero. Status and ordinary logs do not
contain the full path list.

### 14.2 `diff`

`diff --output jsonl` streams detailed findings through the local UDS. It is the
explicit interface for per-path diagnosis. Reads may run concurrently; state
mutations are serialized.

### 14.3 Post-grace CAS events

Emit exactly one event after each actual CAS attempt in
`DUAL_WRITE_REPAIRING`, including success, conflict, and failure. Do not emit an
event for a candidate that converges before CAS.

Required diagnostic fields:

| Category | Fields |
| --- | --- |
| Event | `event_id`, `emitted_at`, `phase`, `round_id`, `cas_attempt`, `first_seen_at`, `grace_seconds` |
| Job/runtime | `job_id`, `volume_id`, `node_name`, `pod_name`, `space_id` |
| Source | `source_path`, `target_path`, `source_version_token`, `size`, `mtime`, `source_checksum_sha256` |
| Target | diagnostic `resource_id`, `revision`, `drive9_checksum_sha256`, `expected_revision` |
| Result | `operation`, `result`, `error_class`, `latency_ms` |

The V1 wire schema is closed: all 26 fields above are required and unknown
fields are rejected. `mtime` is signed Unix nanoseconds and may be negative;
all other numeric fields are non-negative, with `cas_attempt >= 1`.
`operation` is `create` or `update`; `result` is `success`, `conflict`, or
`failure`.

The reporter uses a bounded in-memory queue, request timeout, short bounded
retry, and sent/failed/dropped counters. Endpoint absence, timeout, Server
error, or a full queue affects observability only.

The Server structured log may contain the full path for diagnosis. Metrics may
label only low-cardinality phase, CAS result, and error class. Job, path,
volume, node, Pod, tenant, and Space MUST NOT be metric labels.

## 15. Concurrency, rate, and capacity

1. One Worker process owns one Job.
2. Small-file, large-file, and Multipart workers share one Job-level byte token
   bucket.
3. `max_bytes_per_second` is the aggregate upload ceiling for that Job.
4. N Jobs may consume up to N times the configured per-Job limit.
5. V1 provides no batch or Space aggregate limiter.
6. Namespace and target inventory are retained in memory for the current
   process. Memory use therefore scales with total observed entry count; no
   file content is retained.
7. Current expectation is TB-scale aggregate data and at most 100,000
   directories per Source, but file count, bytes per EBS, change rate, largest
   file, and small-file ratio are deployment inputs.
8. Fast rounds still pay complete `readdir/lstat` cost.
9. `verify-full` reads and hashes every current regular file and may create
   substantial EBS I/O.
10. `plan` reports observed entry/byte distribution and Round timing inputs.
    V1 makes no completion-time, memory ceiling, or stop-window promise from
    unknown file-count data.

## 16. Security contract

1. EBS access is read-only and confined to the configured Source Root. Nested
   device changes follow the block-or-explicit-exclude rule in section 7.2.
2. Drive9 API keys enter the process only through read-only Secret files.
3. Redact keys from errors, logs, UDS responses, in-memory diagnostic output,
   Checkpoints, events, and metrics.
4. File content is never written to control state, logs, status, or events.
5. Full path details are limited to explicit `diff` output, in-memory findings,
   and intentional Server diagnostic event logs.
6. Owner API keys are broad, normally long-lived, and can access the entire
   Tenant/Space. Prefix mapping does not reduce authorization.
7. Shared-Space and batch-Secret blast radius is an accepted V1 limitation.
8. Local UDS permissions are mode 0600; requests are bounded and deadline
   controlled.
9. Control and business Prefixes are disjoint and enforced on every read,
   mutation, verification, and Checkpoint write.

## 17. Failure behavior

| Failure | Required response |
| --- | --- |
| Network, throttle, or 5xx | Bounded exponential backoff; after the same blocker persists five minutes, set `Attention=true` and continue safe retry |
| API-key authentication failure | Reload Secret file, revalidate, then set `Attention=true` and stop the current Round if still invalid |
| Source changes during read/hash/upload | Discard result and restart from the new Source Version Token |
| Incomplete source scan | Fail the Round; no missing-entry Delete |
| Per-path source blocker | Record finding, allow independent siblings, fail the Round |
| Unknown target Revision or unsafe identity/type | Fail closed; never overwrite or delete |
| Create/Update CAS conflict | Reread both sides and requeue; no unconditional fallback |
| Interrupted Multipart upload | Abandon the upload ID; reread target/source and retry as a fresh conditional upload from Part 1 |
| Remote write committed before process failure | Rediscover by full target reread and converge through normal phase rules |
| Remote Checkpoint commit failure | Preserve the previous non-regressible control state; reject the phase/fence transition |
| Worker restart | Discard working state, clear conditions, and require a complete deep recovery round |
| Duplicate Worker | Checkpoint/target conflict, fail closed, `Attention=true` |
| Event endpoint failure | Increment reporter counters only; data path continues |
| Worker restart in dual-write | Restore only highest phase, rerun deep recovery, derive a new floor, and restart grace intervals |
| Worker restart during full verification | Discard the request/result; require the operator to invoke it again after recovery |
| Failure before fence intent | Restart recovery in `DUAL_WRITE_REPAIRING`; operator retries the command |
| Failure after fence intent | Keep writes disabled and finish fencing |

## 18. Correctness boundary and accepted residual risks

| Topic | V1 guarantee | Explicit non-guarantee |
| --- | --- | --- |
| Pre-T0 target | Repeatable exact convergence while Target Prefix is exclusive | No safe merge into an already active Prefix |
| T0-T2 content | Safe, conditional Create/Update repair toward current EBS state | No cross-system atomicity, zero-loss proof, or maximum RPO |
| Delete/Rename | No destructive Migration operation after T0 | Target may retain deleted or pre-Rename paths |
| Verification | One-way coverage at full-verification completion for supported semantics | No No-Extras or bidirectional equality claim |
| Fast rounds | Detect normal mtime or Source Version Token changes | A change preserving/reusing every token field may be missed |
| Full verification | Covers every current EBS path through completion | Does not cover later changes and does not prove T1 |
| CAS | Detects ordinary Revision conflicts and some observed identity/type changes | Revision reuse after delete/recreate can produce undetected ABA overwrite |
| Batch | Independent per-Job phase/fence progress and external all-Job gate | No cross-Job transaction; partial success is possible, and restart loses in-memory working progress |
| Cutover | Durable irreversible no-more-Migration-writes boundary | No unfence, rollback, or Drive9-to-EBS synchronization |
| Checksum | End-to-end worker-computed SHA-256 persisted by Drive9 | No independent Server reread proof for Multipart content |
| Credentials | Secret-file handling and redaction | Owner key is not Prefix-scoped and batch Secret is not per-node isolated |
| Metadata | Supported paths, links, and `mode & 0777` | No UID/GID, timestamps, xattr, ACL, special bits, or sparse layout |

If any non-guarantee is unacceptable, the implementation MUST NOT silently
harden around it. Reopen the design and scope instead.

## 19. Acceptance contract

The detailed task gates live in the two implementation plans. At minimum, V1
acceptance must prove the following behavior.

### 19.1 Server and Go Client

1. New checksum fields are authenticated, metadata-only, and backward
   compatible when omitted.
2. BatchStat default query/response cost does not increase when checksum is not
   requested.
3. Multipart checksum validation and persistence cover fresh V1 and V2 create
   and overwrite-CAS completion.
4. File Revision and checksum commit atomically.
5. Revision 0 and positive-Revision conflicts retain current semantics.
6. Event ingestion enforces Owner auth, bounded payload, redaction, and
   low-cardinality metrics without persistence.
7. Existing generic Server, Client, upload, Resume, and `drive9 cp` behavior
   remains compatible; Migration adds no checksum-aware Resume contract.
8. Required capability preflight populates the Client transfer limits used by
   the immediately following upload, including the small-file threshold.
9. Production Server mode validates the configured Owner API key before
   accepting events or caller-supplied checksums. Local/fallback mode is
   test-only and excluded from this production acceptance requirement.

### 19.2 Migration CLI and Worker

1. Strict config parsing, one-Job node selection, phase-source exclusivity,
   phase rollback rejection, and Secret rotation are tested.
2. Preflight is read-only and rejects every invalid mapping before Drive9
   business mutation.
3. EBS fixtures cover all supported and blocked object/metadata cases.
4. Scan interruption never generates Delete.
5. Path normalization and Prefix confinement hold for business and control
   data.
6. Every Create/Update is conditional; fake-Client tests prove no
   unconditional retry path.
7. Source mutation invalidates hash/upload results.
8. Hash, Part upload, and Complete failures abandon the upload session; retry
   starts a fresh conditional upload and never adopts a path-selected session.
9. `SYNCING` repeatability, Delete, and rename-as-create-plus-delete converge
   only under exclusive ownership.
10. Fast rounds traverse the full namespace while skipping deep work for
    non-candidates.
11. Token candidates survive ordinary scans until deep work in the current
    process; restart forces a complete deep baseline.
12. Grace reset, expiry, target Revision changes, CAS conflicts, and restart
    semantics match this design.
13. No post-T0 Delete/Rename reaches Drive9; target-only residue alone remains
    non-blocking.
14. The residual ABA case is a documented negative test, not a safety claim.
15. Event endpoint hang/failure/full-queue behavior does not affect repair or
    phase progress.
16. UDS permissions, bounds, deadlines, streaming, serialization, schema, and
    exit codes are tested.
17. `verify-full` is serialized, unfiltered by mtime, in-memory only, and does
    not change phase; a configured `CUTOVER_READY` startup runs a fresh
    verification after restart.
18. ConfigMap cutover cannot create a fresh Job, skip `SYNCING`, or directly
    persist actual `CUTOVER_READY`. Automatic and explicit triggers produce the
    same fence.
19. Fence failures before and after intent prove the irreversible recovery
    split; no post-intent write is possible.
20. Keys and file contents are absent from every forbidden sink.
21. Existing binaries build unchanged; `drive9-migration` builds with
    `CGO_ENABLED=0` for Linux AMD64 and ARM64.

No acceptance test may claim strict consistency, No Extras, maximum RPO,
undetectable-ABA safety, automatic rollback, or cross-Job atomicity.

## 20. Implementation ownership and code anchors

The delivery is split across repositories and plans:

1. Server Wire Contract: external Drive9 Server repository.
2. Go Client Migration adapter:
   `.sisyphus/plans/drive9-migration-cli-v1.md`.
3. Migration binary and Worker:
   `.sisyphus/plans/drive9-migration-cli-v1.md`.

| Surface | Current anchor | Ownership |
| --- | --- | --- |
| Server routing/auth | External Server repository | External Server Wire Contract |
| Server BatchStat | External Server repository | External Server Wire Contract |
| Server HEAD Stat | External Server repository | External Server Wire Contract |
| Client Stat/BatchStat | `pkg/client/client.go:254`, `pkg/client/client.go:294`, `pkg/client/client.go:842` | CLI/Worker plan, Go Client adapter |
| Conditional writes | `pkg/client/transfer.go:354`, `pkg/client/transfer.go:361` | CLI/Worker plan, matching external contract |
| Multipart Complete | `pkg/client/transfer.go:821`, `pkg/client/transfer.go:1171` | CLI/Worker plan, matching external contract |
| Existing generic Part Resume | `pkg/client/transfer.go:1650` | Compatibility only; Migration MUST NOT call or extend it |
| New binary | `cmd/drive9-migration` | CLI plan |
| New implementation packages | `internal/migration` | CLI plan |
| Build targets | `Makefile:109`, `Makefile:136` | CLI plan |
| Existing UDS pattern | `pkg/mountcontrol/control.go:85`, `pkg/fuse/mount_control_unix.go:31` | Pattern reuse only |
| Existing CSI mapping | `k8s-csi` repo: `internal/driver/driver.go:391`, `internal/driver/k8s_secret.go:19` | Read-only validation; no CSI change |

Line numbers are discovery anchors and must be re-resolved if surrounding code
moves.

The CLI/Worker delivery may modify `pkg/client` only for the Migration Contract
listed in section 13 and accepted by its Client prerequisite task. It must not
modify Server or datastore/backend code. Any required new Server Wire Contract
is a `SERVER_BLOCKER`; do not hide it in the current repository. Do not
introduce a public Migration SDK or a generalized reconciliation framework.

## 21. Deployment inputs, not design gaps

The following values are required before production migration but do not change
the V1 implementation contract:

1. Final `volume_id -> node -> Source Root -> Space -> Prefix ->
   credential_ref` mapping.
2. Choice of one-Space-per-EBS or shared-Space layout.
3. Business CSI PVC/Secret/`remote-root` mapping.
4. Actual file count, logical bytes, change rate, largest file, and small-file
   distribution for each EBS.
5. Whether EBS serial/by-id is readable in the Worker environment.
6. Operator Runbook integration for ConfigMap updates at T0 and T2, DaemonSet
   restart, per-Job status, external T1 recording, `verify-full`, optional
   `prepare-drive9-cutover`, and the kube plugin's all-Job T2 gate.
7. Validation that the business writer normally changes at least one mtime or
   Source Version Token field for Create, Update, metadata change, atomic
   replace, and Rename.

If an environment input violates an accepted assumption, block deployment and
reopen the affected design decision. Do not broaden implementation scope
implicitly.
