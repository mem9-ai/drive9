---
title: Drive9 Migration kubectl Plugin Design
status: accepted
accepted_at: 2026-08-10
scope_class: medium
production_net_loc: 850-1050
---

# Drive9 Migration kubectl plugin design

## 1. Objective

Deliver a client-side `kubectl` plugin that discovers existing Drive9 Migration
Worker Pods and presents one customer-readable, read-only view of their Job
status. The zero-argument happy path is:

```bash
kubectl drive9 migration status
```

The plugin aggregates only observed Pods. It does not make the Migration Worker
batch-aware and does not introduce a Kubernetes or Drive9 control plane.

## 2. Scope baseline

### 2.1 In scope

1. Discover Worker Pods through a fixed label contract in the current namespace,
   a selected namespace, or all namespaces.
2. Optionally select one migration batch through
   `app.kubernetes.io/instance`.
3. Query each existing Worker Pod through bounded concurrent `kubectl exec`
   calls to its local `status --output json` command and expand the returned EBS
   envelope into one row per Job.
4. Render deterministic `table`, `wide`, and aggregate `json` output.
5. Derive a concise per-Job display status and one observed-status summary per
   `(namespace, batch)`.
6. Preserve partial results when one or more Pods are unavailable.
7. Build customer-side binaries for Linux, macOS, and Windows on AMD64 and
   ARM64, using the existing CLI release path.
8. Document labels, installation, commands, output semantics, and the observed
   Pod limitation.

### 2.2 Explicitly deferred or out of scope

1. Reading Migration ConfigMaps or discovering Jobs for which no Pod exists.
2. `diff`, `verify-full`, `prepare-drive9-cutover`, or any other mutating or
   per-path operation.
3. Watch mode, TUI, Web UI, history, persistence, alerting, or notifications.
4. CRDs, Controllers, Operators, Services, network APIs, Kubernetes Conditions,
   or new RBAC resources.
5. Additional Worker status mutations or Worker-image lifecycle management.
6. Krew publication or a separate plugin distribution service.

### 2.3 Accepted completeness boundary

The plugin cannot know the configured number of Jobs without reading the
ConfigMap. Every summary therefore says `observed N jobs`; it must never claim
that all configured Jobs were discovered. A Pod that does not exist is invisible.
A matching Pending, Failed, Terminating, or unreachable Pod remains visible.

## 3. Kubernetes resource contract

The DaemonSet and its Pod template carry these labels:

```yaml
metadata:
  labels:
    app.kubernetes.io/name: drive9-migration
    app.kubernetes.io/component: worker
    app.kubernetes.io/instance: customer-a-20260810
```

`app.kubernetes.io/instance` is the customer-selected migration batch name.
The Worker container name is `drive9-migration`. The plugin does not require a
per-Job label because every nested status item returns explicit `job_id` and
`volume_id` fields from `internal/migration/control.go`.

The fixed discovery selector is:

```text
app.kubernetes.io/name=drive9-migration,app.kubernetes.io/component=worker
```

`--batch NAME` adds an exact
`app.kubernetes.io/instance=NAME` requirement. Without `--batch`, all matching
instances in scope are displayed and summarized separately. Missing instance
labels are reported as a contract error rather than silently excluded.

## 4. CLI contract

The binary name `kubectl-drive9-migration` exposes the nested kubectl command
`kubectl drive9 migration`.

```bash
kubectl drive9 migration status
kubectl drive9 migration status --batch customer-a-20260810
kubectl drive9 migration status -n migration-system
kubectl drive9 migration status -A
kubectl drive9 migration status -o table
kubectl drive9 migration status -o wide
kubectl drive9 migration status -o json
```

The plugin accepts `--context` and `--kubeconfig` and forwards them to every
child `kubectl` invocation. `-n/--namespace` and `-A/--all-namespaces` are
mutually exclusive. `table` is the default output.

## 5. Runtime architecture and data flow

The implementation is a CGO-free Go binary that invokes the user's existing
`kubectl`. It does not import `client-go` or implement the Kubernetes exec
transport.

```text
kubectl-drive9-migration
  -> kubectl get pods -l <fixed selector> -o json
  -> validate labels and Kubernetes Pod state
  -> at most 8 concurrent kubectl exec calls
       /drive9-migration status --output json
  -> validate and parse the multi-Job EBS Worker payload
  -> expand one Pod into Job rows
  -> derive per-Job display states and batch summaries
  -> stable sort
  -> render table, wide, or JSON
```

Every Pod exec has a 10-second timeout. Commands are executed with argument arrays,
never through a shell. The plugin uses a small injected command function for
unit tests, following the existing CLI dependency-injection pattern at
`cmd/drive9-migration/main.go:26`; it does not introduce a general command
runner interface.

## 6. Display-state derivation

Kubernetes availability is evaluated before Worker state. After all responses
are collected, duplicate `(namespace, batch, job_id)` results are marked as
ambiguous.

| Display status | Derivation |
| --- | --- |
| `POD_<PHASE>` | Any non-`Running` Kubernetes Pod, including `POD_PENDING`, `POD_FAILED`, and `POD_SUCCEEDED` |
| `TERMINATING` | Kubernetes Pod has a deletion timestamp |
| `UNAVAILABLE` | Exec failed, timed out, or returned invalid JSON |
| `DUPLICATE` | More than one observed Pod reports the same Job identity in one batch |
| `ATTENTION` | `conditions.attention=true`, or actual phase is `CUTOVER_READY` while the fence is incomplete |
| `CUTOVER_READY` | Actual phase is `CUTOVER_READY` and the fence is complete |
| `READY_FOR_ROLLOUT` | `conditions.ready_for_rollout=true` |
| `CONVERGED` | `conditions.current_converged=true` |
| `REPAIRING` | Actual phase is `DUAL_WRITE_REPAIRING` and is not converged |
| `SYNCING` | Actual phase is `SYNCING` and is not ready for rollout |

The batch summary precedence is:

```text
NEEDS_ATTENTION > MIXED_PHASE > CUTOVER_READY > CONVERGED
                > READY_FOR_ROLLOUT > REPAIRING > SYNCING
```

`CUTOVER_READY` describes only all observed Jobs. It does not prove external T1,
authorize a business switch, or change the Runbook safety boundary documented at
`docs/design/drive9-migration-v1.md:454`.

## 7. Output contract

Default table columns are `BATCH`, `JOB`, `PHASE`, `STATUS`, `ROUND`, `FILES`,
`DIFF`, `RETRY`, `VERIFY`, and `POD`. When only one batch is present, the
renderer may omit `BATCH`. Wide output also includes namespace, node, target
Space/Prefix, candidate counts, pending/in-flight work, and a bounded error
message.

JSON output contains:

1. Generation time and effective query scope.
2. One batch summary per `(namespace, instance)`.
3. One item per observed Pod with Kubernetes identity, derived status, an
   optional bounded error, and the validated raw Worker status object.

Items and summaries are sorted by namespace, batch, Job ID, and Pod name. New
Worker fields remain available in the raw JSON object without changing table
rendering.

## 8. Errors and exit codes

| Code | Meaning |
| ---: | --- |
| `0` | Pod discovery and every applicable Worker query succeeded; migration progress, differences, mixed phase, and Attention remain status facts |
| `1` | No matching Pod, Kubernetes discovery failure, partial/unavailable result, invalid labels, invalid Worker payload, or duplicate Job identity |
| `2` | Invalid CLI arguments |

Partial result failures still render every successfully collected Job. Stderr
from `kubectl` is normalized to one bounded message and cannot replace structured
stdout. Cancellation stops outstanding child processes.

## 9. Security and compatibility

1. The plugin reads no Drive9 key or Migration Secret.
2. It inherits the user's existing kubectl authentication and requires only the
   permissions already needed for Pod list and Pod exec.
3. Kubeconfig contents, environment values, and raw child commands are not
   logged.
4. The Worker status contract already excludes API keys and file contents at
   `docs/design/drive9-migration-v1.md:918`.
5. The table intentionally excludes `credential_ref`; JSON preserves the
   existing non-secret Worker payload.
6. Unknown additive Worker JSON fields are accepted for forward compatibility;
   required identity, phase, conditions, and fence fields are validated.

## 10. Build, release, and documentation

Add dedicated local and six-platform release targets beside the existing CLI
targets at `Makefile:142`. Extend `.github/workflows/release-cli.yml` to build
and copy plugin artifacts through the existing release directory. Actual release
execution remains a separately authorized external action.

Update `docs/guides/drive9-migration-v1-runbook.md` with the label contract,
installation name, commands, output scope, and the rule that this display does
not replace external T1 confirmation.

## 11. Acceptance criteria

1. Zero-argument status discovers all correctly labeled Worker Pods in the
   current kubectl namespace and renders one row per reported Job.
2. Namespace, all-namespace, batch, context, kubeconfig, and output flags produce
   the exact child kubectl arguments required by this design.
3. No more than eight exec calls run concurrently, and one hung Pod is cancelled
   after 10 seconds without losing other results.
4. Every display and batch state in Sections 6 and 8 is covered by deterministic
   unit tests.
5. Pending, Failed, Terminating, missing-label, timeout, malformed-output, and
   duplicate-Job cases remain visible and return code 1.
6. Attention or incomplete migration with otherwise successful queries returns
   code 0, matching the single-Job status behavior at
   `cmd/drive9-migration/main.go:69`.
7. Linux, macOS, and Windows AMD64/ARM64 artifacts build with `CGO_ENABLED=0`.
8. Existing Worker and drive9 test suites remain unchanged and passing.

## 12. Effort boundary

Expected production implementation is `~850-1050 LoC`, Medium, excluding tests,
generated artifacts, release boilerplate, and documentation. Reading ConfigMaps,
adding mutations, adding a watcher, or introducing `client-go` is scope expansion
and requires a new design decision. The post-implementation estimate is higher
than the initial estimate because the accepted behavior requires cross-platform
Pod and Worker wire DTOs, strict partial-result validation, and three renderers;
the owned surfaces and scope class are unchanged.
