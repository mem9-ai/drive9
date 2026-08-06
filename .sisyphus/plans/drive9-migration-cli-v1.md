---
title: Drive9 Migration V1 CLI and Worker Implementation Plan
status: ready_for_dev_regression
plan_id: drive9-migration-cli-v1
delivery_pr: 2
source_document: https://pingcap.feishu.cn/wiki/XDKnwKGk7iMwBtkfOvcc3lgBnWe
source_revision: 1517
design_document: docs/design/drive9-migration-v1.md
scope_class: large
production_net_loc: 2480-3820
actual_production_net_loc: 3853
scope_audit: non_expanding_overage_33
repository_amendment: memory-only-worker-state-no-multipart-resume
external_server_contract: available
---

## 1. Objective

Implement the Go Client Migration adapter and accepted EBS-to-Drive9 V1
migration workflow as an independent `drive9-migration` binary in the current
CLI/Worker repository.

The external Server repository provides the available Wire Contract. This
repository first implements and accepts its matching `pkg/client` adapter,
then consumes that adapter without changing Server or datastore/backend
behavior. The repository design at
`docs/design/drive9-migration-v1.md`, derived from accepted Feishu revision
1517, remains the product and behavior baseline.

The repository amendment in that design is authoritative: Worker operational
state is memory-only, every restart begins with a complete deep recovery round,
and Migration never resumes a prior Multipart upload.

## 2. External dependency and PR boundary

### 2.1 Start gate

The external Server Wire Contract is available. This statement does not assert
or modify `SG99` in the Server plan.

1. `C01` implements only the matching Go Client Migration Contract.
2. `A01` accepts that Client Contract before any Worker task that consumes a
   Drive9 API enters `IN_PROGRESS`.
3. Pure configuration, in-memory state, and EBS scanner work may proceed
   independently when files do not overlap, but may not integrate an
   unaccepted Client Contract.
4. Whole-system Dev environment regression runs after the Worker is complete;
   it is not a development start gate.

Commit, push, PR creation, deployment, and external-document updates are not
part of this plan.

### 2.2 Owned production surfaces

1. `cmd/drive9-migration`.
2. Focused packages under `internal/migration`.
3. `pkg/client`, limited to the Migration Contract in `C01`.
4. The Migration build target and release outputs.
5. Migration-only samples, runbook, and operator handoff documentation.
6. Migration and Client tests and test-only fixtures/fakes.

### 2.3 Forbidden production surfaces

The current delivery must not modify:

1. `pkg/server`, Server routing, authentication, wire handlers, or metrics.
2. Datastore/backend persistence used by Server protocol prerequisites.
3. `k8s-csi`, CSI behavior, Helm, deployment manifests, Operator/CRD, or
   Kubernetes RBAC.

`pkg/client` may implement only checksum Stat/BatchStat, Migration capability
preflight and typed errors, fresh V1/V2 conditional whole-checksum upload, and
Migration events as fixed by `C01`. Any required new or changed Server Wire
Contract is a `SERVER_BLOCKER`; stop the affected task and return it to the
external Server repository.

## 3. Scope baseline

### 3.1 In scope

1. Support only an EBS Source Root already mounted as a local directory on an
   EKS node.
2. Model one Job as one EBS Source Root mapped to one Drive9 `(Space, Prefix)`.
3. Support one EBS per Space and multiple EBS sharing one Space through
   non-overlapping Prefixes.
4. Deliver `plan`, `run`, `status --output json`,
   `diff --output jsonl`, `verify-full`, and
   `prepare-drive9-cutover`.
5. Implement only
   `SYNCING -> DUAL_WRITE_REPAIRING -> CUTOVER_READY`.
6. Read phase once at Worker startup from ConfigMap-mounted configuration or
   the accepted environment fallback. A ConfigMap change takes effect only
   after the customer rolls the Migration DaemonSet.
7. Resolve API keys only through read-only Secret Volume files and never place
   them in ConfigMap, argv, environment, memory snapshots, Checkpoints, logs,
   or status.
8. Implement and reuse the accepted `pkg/client` fresh upload, Multipart,
   checksum, Revision
   CAS, capability, and event APIs. Never list, resume, or adopt a prior
   Multipart session.
9. Preserve accepted regular-file, directory, empty-directory, symlink,
   hardlink, and `mode & 0777` semantics.
10. Keep scan rounds, per-file baselines, findings, verification, grace, and
    upload attempts in memory. Persist only immutable identity, highest phase,
    fence intent, and complete fence in the per-Job Drive9 Checkpoint.
11. In target-exclusive `SYNCING`, converge Create, Update, Delete, and
    rename-as-create-plus-delete through complete rounds.
12. From T0 through T2, keep EBS authoritative and repair safe Create/Update
    operations with grace, Source Version rechecks, checksum, and Revision CAS.
13. After T0, skip Delete, do not recognize Rename, treat a renamed new path as
    Create, and retain the old Drive9 path as a warning.
14. Provide T1 `verify-full` without the fast-round mtime filter, while
    leaving phase and the T2 decision unchanged.
15. Provide per-Job T2 `prepare-drive9-cutover`, drain in-flight work, and
    persist an irreversible fence before reporting `CUTOVER_READY`.
16. Keep one Worker per Job, internal concurrency, and one shared token bucket
    per Job.
17. Report actual post-grace CAS attempts asynchronously to the merged Server
    event endpoint without allowing reporting failure to block the data path.

### 3.2 Explicitly out of scope

1. Non-EBS Sources, EBS Snapshot/history migration, or data outside Source
   Root.
2. A `drive9` migration subcommand, central service, remote control API, Web
   UI, Operator/CRD, Kubernetes Condition, watcher, or extra RBAC.
3. CSI code or resource automation, customer DaemonSet/Helm delivery, IAM, EBS
   attach/mount, or dynamic Ordinal mapping.
4. Strict and direct Best-effort paths in CLI, states, tests, or acceptance.
5. Post-T0 conditional Delete/Rename, reverse sync, automatic rollback,
   unfence, phase rollback, conflict merge, cross-Job transaction, distributed
   lease, HA ownership, or batch/Space-wide throttling.
6. Elimination of the accepted Revision-only residual ABA risk.
7. Prefix-scoped credentials or per-Node hard isolation of a batch Secret.
8. Automatic cleanup of retained control data.
9. Any new Server Wire Contract or `pkg/client` surface beyond `C01`.
10. SQLite, a local database, PVC-backed Worker state, persisted per-file
    state, or restart recovery of Multipart uploads and verification results.
11. A Migration checksum-aware Resume API. Existing generic `drive9 cp`
    Resume behavior is a compatibility boundary, not a Worker dependency.

### 3.3 Acceptance outcome

V1 CLI/Worker is accepted only when every declared Job can:

1. Complete repeated `SYNCING` rounds and report `ReadyForRollout=true` from
   the latest complete in-memory result.
2. After every Worker start, restore only non-regressible Job state, complete
   one deep full recovery round, and make no convergence claim before it
   succeeds. In dual-write phase, derive a new in-memory
   `repair_mtime_floor` from that round.
3. Run fast rounds, grace handling, and CAS repair without unconditional
   overwrite or post-T0 Delete/Rename.
4. Preserve the Source Version candidate until successful deep verification or
   repair in the current process; restart rebuilds the complete baseline.
5. Complete a per-Job `verify-full` and expose its result in `status`
   without changing phase.
6. Persist an irreversible T2 fence, survive restart, report
   `CUTOVER_READY`, and perform no later Migration write.
7. Recover safely from failures at scan, checksum, Part upload, Complete,
   phase/fence Checkpoint update, full verification, and fence intent. Upload
   retry always starts a fresh conditional Multipart upload from Part 1.
8. Continue data repair and state progress when event reporting fails, while
   exposing reporter counters.
9. Produce no API-key or file-content leakage and no mutation outside the Job
   Target Prefix or its exact control directory.

### 3.4 Effort baseline

| Work area | Production Net LoC |
| --- | ---: |
| Go Client Migration Contract | 180-240 |
| Binary, config, credentials | 220-320 |
| In-memory state and EBS source model | 370-580 |
| Plan, minimal Checkpoint, inventory, diff | 430-680 |
| Apply and `SYNCING` orchestration | 420-660 |
| Dual-write repair and event reporter | 310-470 |
| Local control, verification, fence | 490-750 |
| Build and operator handoff | 60-120 |
| **Worker subtotal** | **2300-3580** |
| **Total** | **2480-3820** |

If the estimate exceeds 3820 Net LoC or crosses beyond the accepted Large
scope, record the accepted requirement causing the delta and perform a scope
audit. A new state, Source, persistent local schema, control plane, watcher,
rollback path, Server Wire Contract, Client surface beyond `C01`, CSI change,
or later-phase option
requires explicit user approval.

## 4. Execution contract

### 4.1 Task states

| State | Meaning |
| --- | --- |
| `BLOCKED` | A dependency has not passed; implementation does not start. |
| `READY` | All dependencies passed; development may start. |
| `IN_PROGRESS` | Production code or tests are being changed. |
| `IMPLEMENTED` | Development is complete and handed to acceptance. |
| `VERIFYING` | Acceptance tests and review are running. |
| `PASSED` | Evidence is complete and dependents may proceed. |
| `FAILED` | Acceptance found a defect; the paired development task reopens. |

### 4.2 Convergence rules

1. Downstream development depends on upstream acceptance, not merely code
   presence.
2. Acceptance tasks do not modify production code. Failures return exact
   evidence to the paired development task.
3. No task passes with skipped required tests, unresolved flakes, an
   undocumented deviation, or a production diff outside this PR boundary.
4. Parallel tasks must use isolated worktrees or serialize overlapping files.
5. Failpoint tests run alone because instrumentation rewrites source.
6. This plan reaches `READY_FOR_DEV_REGRESSION` only when the local `CG99`
   gate passes and the deviation register is empty or every entry has a
   user-confirmed disposition.

### 4.3 Required evidence

Each task records:

1. Owner and status.
2. Changed production and test paths.
3. Production Net LoC.
4. Exact validation commands and exit codes.
5. Coverage or branch-coverage evidence.
6. Correctness, code-quality, design-alignment, and PR-boundary conclusions.
7. Any deviation ID and disposition.

### 4.4 Quality floor

1. Every new non-trivial branch has success and failure tests.
2. New Migration packages reach at least 80% statement coverage.
3. State, reconciliation, grace/CAS, verification, and fence packages reach at
   least 85%; fence transitions reach at least 90%.
4. Concurrency-sensitive code passes targeted race tests.
5. Scan, checksum, Part upload, Complete, Checkpoint, full verification, and
   fence-intent failures have deterministic injection tests.
6. Tests prove API keys and file contents do not enter ConfigMap, in-memory
   diagnostic snapshots, Checkpoints, ordinary logs, or status.
7. Tests do not claim strict consistency, No Extras, maximum RPO, safe
   undetectable ABA, rollback, or cross-Job atomicity.

## 5. Code and contract anchors

| Surface | Anchor | Planned use |
| --- | --- | --- |
| External Server Wire Contract | External Server repository | Available immutable protocol boundary; does not assert local Server-plan `SG99`. |
| Conditional write semantics | `pkg/client/transfer.go:354`, `pkg/client/transfer.go:361` | Consume create-if-absent and exact Revision CAS. |
| Fresh Multipart Complete | `pkg/client/transfer.go:821`, `pkg/client/transfer.go:1171` | Consume whole-checksum completion without listing or resuming prior sessions. |
| Existing generic Resume | `pkg/client/transfer.go:1650` | Compatibility-only surface; Migration MUST NOT call or extend it. |
| Independent binary pattern | `Makefile:109`, `Makefile:136` | Add a separate Migration target without changing existing targets. |
| Unix-socket JSON pattern | `pkg/mountcontrol/control.go:85`, `pkg/fuse/mount_control_unix.go:31` | Reuse protocol and socket-permission patterns, not mount types. |
| CSI mapping contract | `k8s-csi` repo: `internal/driver/driver.go:391`, `internal/driver/k8s_secret.go:19` | Verify handoff only; no CSI production change. |

## 6. Target component boundary

```text
Operator-managed Kubernetes
  ConfigMap config.yaml + startup phase
  Secret Volume API-key files
  DaemonSet rollout / per-Job kubectl exec
                  |
                  v
cmd/drive9-migration
  config and Job selection
  local Unix control socket
                  |
                  v
internal/migration
  EBS scanner -> in-memory rounds -> diff/reconcile -> checkpoint/fence
                  |
                  v
accepted pkg/client adapter -> external Drive9 Server Wire Contract
```

Use focused packages under `internal/migration`. Do not expose a public
Migration SDK or introduce a generalized reconciliation framework.

## 7. Dependency graph and tracker

```text
EXT00 -> C01 -> A01 ----------------------------------------------+
                                                                  |
EXT00 -> D04 -> A04 --+--> D05 -> A05 --+--> D08 -> A08 ---------+
                      |                 |                         |
                      +--> D06 -> A06 --+--> D09 -> A09 ----------+--> D10 -> A10
                                 |                                      |
                                 +--> D07 -> A07 ------------------------+--> D11 -> A11
                                                                               |
                                                                               v
                                                                         D12 -> A12
                                                                           |      |
                                                                           |      +--> D13 -> A13
                                                                           |                |
                                                                           +----------------+--> D14 -> A14
                                                                                                 |
                                                                                                 v
                                                                                           D15 -> A15
                                                                                                 |
                                                                                                 v
                                                                                           D16 -> A16
                                                                                                 |
                       A01 + A07 ---------------------------------------------------------------+
                                                                                                 v
                                                                                           D17 -> A17
                                                                                                 |
                                                                                                 v
                                                                                                CG99
```

| ID | Type | Component | Depends on | Current state | Completion signal |
| --- | --- | --- | --- | --- | --- |
| `EXT00` | Gate | External Server Wire Contract | User-supplied repository split | `PASSED` | Contract is available; no Server-plan `SG99` assertion. |
| `C01` | Dev | Go Client Migration Contract | `EXT00` | `COMPLETED` | Code and tests handed to `A01`. |
| `A01` | Accept | Go Client Contract | `C01` | `PASSED` | Client checks pass without production edits. |
| `D04` | Dev | Binary and config | `EXT00` | `COMPLETED` | Code and tests handed to `A04`. |
| `A04` | Accept | CLI/config/secret | `D04` | `PASSED` | Required checks pass. |
| `D05` | Dev | In-memory working state | `A04` | `COMPLETED` | Code and tests handed to `A05`. |
| `A05` | Accept | State atomicity | `D05` | `PASSED` | Required checks pass. |
| `D06` | Dev | EBS scanner | `A04` | `COMPLETED` | Code and tests handed to `A06`. |
| `A06` | Accept | Filesystem semantics | `D06` | `PASSED` | Required checks pass. |
| `D07` | Dev | Plan/preflight | `A01`, `A04`, `A06` | `COMPLETED` | Code and tests handed to `A07`. |
| `A07` | Accept | Mapping/capability | `D07` | `PASSED` | Required checks pass. |
| `D08` | Dev | Minimal Checkpoint/recovery | `A01`, `A04`, `A05` | `COMPLETED` | Code and tests handed to `A08`. |
| `A08` | Accept | Recovery gate | `D08` | `PASSED` | Required checks pass. |
| `D09` | Dev | Inventory/diff | `A01`, `A05`, `A06` | `COMPLETED` | A09 test-lint finding fixed; handed back to acceptance. |
| `A09` | Accept | Round completeness | `D09` | `PASSED` | Required checks pass. |
| `D10` | Dev | Conditional apply | `A01`, `A05`, `A06`, `A08`, `A09` | `COMPLETED` | Code and tests handed to `A10`. |
| `A10` | Accept | Transfer/order/rate | `D10` | `PASSED` | Required checks pass. |
| `D11` | Dev | `SYNCING` | `A07`, `A08`, `A10` | `COMPLETED` | Code and tests handed to `A11`. |
| `A11` | Accept | Exclusive convergence | `D11` | `PASSED` | Required checks pass. |
| `D12` | Dev | Dual-write repair | `A11` | `COMPLETED` | Code and tests handed to `A12`. |
| `A12` | Accept | Fast round/grace/CAS | `D12` | `PASSED` | Required checks pass. |
| `D13` | Dev | Event reporter | `A01`, `A12` | `COMPLETED` | Code and tests handed to `A13`. |
| `A13` | Accept | Non-blocking reporting | `D13` | `PASSED` | Required checks pass. |
| `D14` | Dev | Local control/conditions | `A05`, `A12`, `A13` | `COMPLETED` | Code and tests handed to `A14`. |
| `A14` | Accept | UDS/status/diff | `D14` | `PASSED` | Required checks pass. |
| `D15` | Dev | Full verification | `A12`, `A14` | `COMPLETED` | Code and tests handed to `A15`. |
| `A15` | Accept | Verification lifecycle | `D15` | `PASSED` | Required checks pass. |
| `D16` | Dev | Cutover fence | `A08`, `A14`, `A15` | `COMPLETED` | Code and tests handed to `A16`. |
| `A16` | Accept | Irreversible fence | `D16` | `PASSED` | Required checks pass. |
| `D17` | Dev | Build/handoff | `A01`, `A07`, `A14`, `A16` | `COMPLETED` | Code and tests handed to `A17`. |
| `A17` | Accept | Packaging/scope | `D17` | `PASSED` | Required checks pass. |
| `CG99` | Gate | Local Client/Worker acceptance | `A01`, `A04`-`A17` | `PASSED` | Reached `READY_FOR_DEV_REGRESSION`; deferred Dev checks are recorded, not passed. |

## 8. Work packages

### WP01: Go Client Migration Contract

#### C01 development

Status: `COMPLETED`
Dependencies: `EXT00`
Estimate: 180-240 production Net LoC

1. Add optional whole-file SHA-256 to Stat and opt-in
   `include_checksum=true` to BatchStat while preserving omitted-field
   compatibility and treating an absent checksum as unknown.
2. Fetch exact Migration capabilities through one successful status path that
   caches `max_upload_bytes`, `inline_threshold`, and capability together;
   failed responses never populate cache and remain retryable. A status
   response without the capability returns a recognizable unsupported error.
3. Preserve typed status, network, context, and JSON errors through wrapping.
4. Add fresh conditional V2/V1 whole-checksum upload. Prefer V2 and fall back
   only when the V2 endpoint explicitly reports unavailable; create uses
   Revision 0 and update uses an exact positive Revision.
5. After a successful write, reread Stat and return the actual Revision and
   persisted checksum to the caller.
6. Do not add checksum-aware Resume, select sessions by path, persist upload
   IDs, or alter generic `drive9 cp` Resume semantics. A retry uses a new
   upload ID and restarts at Part 1.
7. Add the closed `POST /v1/migration/events` DTO and Client method using
   `source_version_token`; return reporting errors without exposing keys or
   file content.

Execution record:

1. Owner: primary implementation agent.
2. Production files: `pkg/client/client.go`, `pkg/client/transfer.go`.
3. Test files: `pkg/client/client_test.go`, `pkg/client/transfer_test.go`,
   `pkg/client/migration_contract_test.go`.
4. Production Net LoC: 212 additions, 45 deletions; 167 net file growth.
5. Review disposition: first A01 review returned four findings; all were fixed
   and the complete acceptance was rerun. No Server, Resume, or Worker surface
   was added.
6. Deviation: none.

#### A01 acceptance

Status: `PASSED`

1. Run targeted Client unit, compatibility, fake-Server integration, race,
   build, vet, and incremental-lint checks without production edits.
2. Cover checksum omission/presence, default BatchStat request compatibility,
   preflight cache atomicity and retry, Reader-only small upload, and
   recognizable old-Server unsupported behavior.
3. Distinguish wrapped 401/403, 429, 5xx, network, context, and malformed JSON
   failures and prove failed responses do not populate cache.
4. Prove V2 preference, explicit-only V1 fallback, Revision 0/positive CAS,
   post-complete Stat reread, new upload ID/Part-1 retry, no Migration Resume,
   and unchanged generic Resume tests.
5. Verify the event DTO uses `source_version_token` and Client APIs, errors,
   and tests do not leak API keys or file contents.

Pass signal: the Client exactly matches the external Migration Wire Contract
and is accepted before Worker code consumes Drive9 APIs.

Acceptance record:

1. Owner: primary acceptance agent plus independent code and architecture
   read-only reviewers.
2. `go test ./pkg/client -run '^TestMigration' -count=1`: exit 0.
3. Client checksum/status/threshold compatibility group: exit 0.
4. Non-database `WriteStream` and generic `ResumeUpload` group: exit 0.
5. Targeted race test for Migration and status cache readers: exit 0.
6. `go vet ./pkg/client`: exit 0; CGO-disabled `cmd/drive9` build: exit 0;
   incremental `golangci-lint ./pkg/client/...`: exit 0 with zero issues;
   `git diff --check -- pkg/client`: exit 0.
7. Targeted package coverage is 14.9% because `pkg/client` includes unrelated
   legacy APIs. Changed-function coverage is 62.1%-100%; status/preflight,
   Migration upload, validation, and event functions are 80%-100%, except
   legacy Stat/BatchStat branches at 62.1%/66.7%.
8. Final review: code reviewer `APPROVE`; architecture reviewer `GO`; Contract,
   code quality, scope, and Design alignment accepted.
9. Deferred, not passed: two existing database-backed transfer/Resume tests
   and the unfiltered package suite require a working MySQL testcontainer.
   Local Podman/Docker setup was unavailable; the corresponding `make test`
   attempt exited 1 before tests started.
10. Deviation: none.

### WP04: Independent binary, startup config, and credentials

#### D04 development

Status: `COMPLETED`
Dependencies: `EXT00`
Estimate: 220-320 production Net LoC

1. Add `cmd/drive9-migration` with the six accepted commands and exact exit
   codes: 0 success, 1 argument/internal failure, 2 invalid phase/config/action,
   and 3 unavailable local control socket.
2. Strictly parse `version: v3`, endpoint, `job_defaults`, `spaces`, and
   Jobs; reject unknown fields and per-Job grace/rate/concurrency overrides.
3. Resolve exactly one local Job from stable `volume_id`, declared
   `node_name`, and startup inputs; do not add batch scheduling.
4. Read phase once from the `phase` file beside `-f` or
   `DRIVE9_MIGRATION_PHASE`; reject both sources, a missing source, unknown
   values, and historical rollback.
5. Resolve
   `space_ref -> credential_ref -> read-only Secret Volume file`, support
   file rotation, and never persist the key.
6. Compute `config_hash` from effective immutable settings, excluding phase
   and secret values.
7. Add parser, validation, dispatch, exit-code, redaction, phase-source, and
   secret-rotation tests.

Execution record:

1. Owner: primary implementation agent.
2. Production files: `internal/migration/config.go`,
   `cmd/drive9-migration/main.go`.
3. Test files: `internal/migration/config_test.go`,
   `cmd/drive9-migration/main_test.go`.
4. Production Net LoC: 521 additions.
5. Scope audit: 201 LoC above the package estimate ceiling. The delta remains
   inside the accepted D04 surfaces and implements only strict V3 validation,
   phase/credential safety, hashing, selection, and six-command dispatch. No
   new schema, state machine, runtime watcher, Server wire, or deployment
   surface was introduced; the repository hard cap is not reached.
6. Deviation: `DEV-001`, resolved by the user-specified authority order.

#### A04 acceptance

Status: `PASSED`

1. Run command/config tests with coverage and race detection.
2. Verify `drive9` dispatch is untouched.
3. Cover malformed YAML, unknown fields, duplicate IDs, invalid volume IDs,
   missing Secret files, both phase sources, rollback, and rotation.
4. Search ConfigMap models, argv/env, errors, logs, and hash inputs for key
   exposure.
5. Audit the diff against the CLI PR boundary.

Pass signal: the independent binary starts from a strict, secret-safe immutable
snapshot and selects exactly one declared Job.

Acceptance record:

1. Owner: primary acceptance agent; no production edits during the final run.
2. `go test ./internal/migration`: exit 0, 81.2% statement coverage for the
   D04 package surface; `go test ./cmd/drive9-migration`: exit 0, 80.3%.
3. Targeted race, `go vet`, CGO-disabled binary build, incremental lint, and
   diff-format checks: exit 0; lint reported zero issues.
4. Malformed/unknown YAML, duplicate and invalid identities, exact Job
   selection, missing/dual/regressive phase sources, missing/rotated Secret,
   secret-free hash/output, and exact exit codes are covered.
5. Review found and fixed lint findings plus an over-broad batch-level hash;
   the accepted hash now covers only the selected effective Job, its Space
   credential reference, endpoint, and global defaults.
6. `cmd/drive9`, Server/backend/datastore, CSI, and deployment surfaces remain
   untouched. Production secret scan found no API-key value or key-bearing
   config/argv/environment field.
7. Correctness, code quality, Design alignment, and scoped D04 boundary:
   accepted. Deviation `DEV-001` remains resolved by the user authority rule.

### WP05: In-memory Job working state

#### D05 development

Status: `COMPLETED`
Dependencies: `A04`
Estimate: 90-150 production Net LoC

1. Define one synchronized per-Job in-memory state model for current and last
   complete Rounds, source/target entries, findings, conditions, grace/retry
   queues, verification, and active operations.
2. Store metadata, checksums, tokens, and Revisions but never API keys or file
   content in diagnostic snapshots.
3. Atomically publish only a complete Round; failed or interrupted scans leave
   convergence false and cannot imply deletion.
4. Serialize state mutations while allowing bounded read-only status and diff
   snapshots without data races.
5. Initialize empty working state on every process start. Do not add SQLite, a
   local database, PVC state, serialization, or upload-session recovery.
6. Add lifecycle, incomplete-round, concurrent-reader, restart-reset,
   no-secret, and memory-release tests.

Execution record:

1. Owner: primary implementation agent.
2. Production file: `internal/migration/state.go`; test file:
   `internal/migration/state_test.go`.
3. Production Net LoC: 274 additions.
4. Scope audit: 124 LoC above the package estimate ceiling, chiefly explicit
   Source/Target/token/finding DTOs required by this package's state contract
   and reused by D06/D09. No durable store, serialization, upload recovery,
   framework, or second state machine was added; repository hard cap remains
   open.
5. Deviation: none.

#### A05 acceptance

Status: `PASSED`

1. Run state tests with race detection and transition coverage.
2. Inject interruption before and after in-memory Round publication.
3. Inspect exposed snapshots for API keys and file contents.
4. Prove a fresh process begins with no conditions, findings, verification
   result, baseline, floor, or upload ID.
5. Review allocations and release behavior at the accepted namespace scale.

Pass signal: working state is race-free, process-local, secret-free, and unable
to infer deletion or convergence from incomplete work.

Acceptance record:

1. Owner: primary acceptance agent; final acceptance made no production edit.
2. `go test ./internal/migration`: exit 0, 85.4% package statement coverage;
   `state.go`: 58/61 statements, 95.1%.
3. Targeted state race, `go vet`, incremental lint, and diff-format checks:
   exit 0; lint reported zero issues.
4. Before-publication interruption, rejected incomplete publication,
   after-publication replacement, failed-round retention, concurrent readers,
   caller-map isolation, prior-manifest release, Attention derivation, and
   restart reset are covered.
5. Snapshot audit found no API-key, credential, regular-file content, upload
   ID, or durable state field. Correctness, code quality, Design alignment,
   allocation release, and scoped D05 boundary: accepted.
6. Deviation: none.

### WP06: EBS scanner and source model

#### D06 development

Status: `COMPLETED`
Dependencies: `A04`
Estimate: 280-430 production Net LoC

1. Scan the complete EBS namespace into the Job's in-memory manifest using
   bounded per-file read buffers; never retain file content.
2. Use `lstat`, never follow symlinks, and build the token from
   `dev/inode/type/size/mtime_ns/ctime_ns/mode`.
3. Use stat/open/read/stat for deep reads and discard unstable results.
4. Preserve symlink text, hardlink groups, empty directories, and
   `mode & 0777`.
5. Detect invalid UTF-8, NFC collisions, special files, nested mounts, source
   identity uncertainty, and sparse-file semantics.
6. Add fixtures for every accepted type and failure, including changed token
   with unchanged mtime.

Execution record:

1. Owner: primary implementation agent.
2. Production files: `internal/migration/scanner.go`, platform stat helpers,
   and bounded Source/Finding fields in `state.go`.
3. Test file: `internal/migration/scanner_test.go`.
4. Production Net LoC: approximately 356 additions, inside the 280-430
   estimate.
5. Deviation: none.

#### A06 acceptance

Status: `PASSED`

1. Run source tests with race detection and at least 85% coverage for token and
   stable-read logic.
2. Cover every accepted and unsupported filesystem case.
3. Confirm buffer size is bounded and manifest memory scales predictably with
   observed entry count; cover the accepted 100,000-directory expectation.
4. Prove no source operation mutates EBS or follows a symlink.
5. Review portability and error classification.

Pass signal: the scanner produces stable, capacity-tested, non-mutating source
facts without retaining file content.

Acceptance record:

1. Owner: primary acceptance agent; final acceptance made no production edit.
2. `go test ./internal/migration`: exit 0, 85.7% package coverage;
   Scanner/platform token files: 142/165 statements, 86.1%.
3. Targeted Scanner race, `go vet`, incremental lint, diff-format check, and
   Linux AMD64/ARM64 CGO-disabled test compilation: exit 0.
4. Fixtures cover regular files, directories/empty directories, exact symlink
   text without traversal, hardlink grouping/primary, mode 0777, stable hash,
   same-mtime token mutation, invalid UTF-8, NFC collision, FIFO/special,
   nested-device exclusion, identity uncertainty, sparse warning, canceled
   scans, path escape, and 100,000-directory manifest capacity.
5. Manifest/DeepRead audit found metadata and checksums only; no regular-file
   bytes survive calls and no Source operation writes EBS. Correctness, code
   quality, portability, error classification, Design alignment, and scope:
   accepted.
6. Deviation: none.

### WP07: `plan` and preflight

#### D07 development

Status: `COMPLETED`
Dependencies: `A01`, `A04`, `A06`
Estimate: 120-190 production Net LoC

1. Share one preflight implementation between `plan` and `run`; `run` repeats it
   before its first business-data mutation.
2. Validate all declared Job identities and target mappings statically, then
   probe only the Job selected by `DRIVE9_MIGRATION_NODE_NAME` because other
   nodes' EBS filesystems are inaccessible.
3. Reject duplicate or ancestor/descendant Prefixes and shared-Space root Jobs.
4. Permit the one-Space root layout only with an explicit
   `/.drive9-migration/` carve-out.
5. Fail before writing when required merged Server capabilities are missing;
   report event capability as optional.
6. Verify the selected EBS serial/by-id when available; otherwise report
   `volume_identity_verified=false`.
7. Stream local counts and logical bytes and report the selected Job's
   non-sensitive CSI mapping.
8. Test both layouts, local selection, static-versus-dynamic validation, invalid
   maps, old Server behavior, reserved paths, and redaction.

Execution record:

1. Owner: primary implementation agent.
2. Production files: `internal/migration/preflight.go`, Linux/Darwin volume
   identity helpers, and the shared `cmd/drive9-migration` startup dispatch.
3. Test files: `internal/migration/preflight_test.go` and command dispatch tests.
4. Production Net LoC: approximately 259 additions/modifications.
5. Scope audit: 69 LoC above the package estimate ceiling, limited to real
   Linux sysfs plus `/dev/disk/by-id` EBS verification and shared plan/run
   dispatch. No CSI, Server, deployment, adapter framework, or mutation path
   was added; repository hard cap remains open.
6. Deviation: none.

#### A07 acceptance

Status: `PASSED`

1. Run plan/config/source/Client tests.
2. Prove every rejected mapping fails before Drive9 mutation.
3. Verify segment-aware normalized Prefix checks.
4. Compare generated mappings with existing CSI contracts without changing
   CSI.
5. Verify required versus optional capability handling.

Pass signal: each local `plan` is a read-only fail-fast proof for its selected
Job; the operations layer can aggregate all per-Job results.

Acceptance record:

1. Owner: primary acceptance agent; final run changed tests only to meet the
   package coverage gate.
2. Migration/config/source/preflight suite: exit 0, 85.1% package coverage;
   independent command suite: exit 0, 87.0%; targeted Client Contract: exit 0.
3. Full Migration/command race, `go vet`, incremental lint, Linux CGO-disabled
   build, and diff-format checks: exit 0; lint reported zero issues.
4. Tests prove one-Space root and shared-Space non-overlapping layouts,
   segment-aware `/vol-1` versus `/vol-10`, ancestor/root/control collisions,
   duplicate node/source identity, NFC rejection, selected-only Source probe,
   required versus optional capability behavior, old Server/typed errors,
   target emptiness/recovery control, and no remote mutation before failure.
5. Production scan found no Worker-built Drive9 request, upload/Resume path,
   shell `drive9 cp`, key-bearing config/argv/env, or forbidden surface change.
   CSI handoff fields match the accepted Design without modifying CSI.
6. Correctness, code quality, mapping safety, Design alignment, redaction, and
   scope: accepted. Deviation: none.

### WP08: Minimal Checkpoint and restart gate

#### D08 development

Status: `COMPLETED`
Dependencies: `A01`, `A04`, `A05`
Estimate: 100-160 production Net LoC

1. Write one conditionally updated per-Job Checkpoint only beneath the exact
   control directory.
2. Persist only immutable Job/config/Source/Target identity, highest applied
   phase, fence intent, and complete fence.
3. Never persist Round state, per-file tokens/Revisions, findings,
   `repair_mtime_floor`, verification requests/results, reports, or upload IDs.
4. Reject immutable identity mismatch and phase regression. A stale
   Checkpoint CAS fails closed as duplicate/stale ownership.
5. On restart, restore only non-regressible state, clear every in-memory
   condition, and require a complete deep recovery round before normal work or
   convergence claims.
6. If fence intent exists, disable writes and recover only toward complete
   fence.
7. Add deterministic hooks around phase/fence Checkpoint CAS and restart.

Execution record:

1. Owner: primary implementation agent.
2. Production file: `internal/migration/checkpoint.go`; test file:
   `internal/migration/checkpoint_test.go`.
3. Production Net LoC: 260 additions.
4. Scope audit: 100 LoC above the package estimate ceiling, limited to strict
   closed-schema decoding, stable HEAD/read/HEAD, exact CAS post-read
   verification, idempotent control-directory creation, and deterministic
   before/after-CAS hooks. The persisted schema remains exactly the Design's
   minimal identity/phase/fence record; no per-file persistence or recovery
   framework was added.
5. Deviation: none.

#### A08 acceptance

Status: `PASSED`

1. Run recovery tests with race detection and at least 85% branch coverage.
2. Inject failures before and after every phase/fence Checkpoint CAS.
3. Prove stale writers cannot regress phase or clear fence state.
4. Verify config mismatch blocks recovery and no restart inherits convergence.
5. Search remote artifacts and logs for per-file state, findings, verification,
   upload IDs, secrets, and file contents.

Pass signal: only non-regressible Job control state survives restart, and every
writable restart is gated by a complete deep recovery round.

Acceptance record:

1. Owner: primary acceptance agent; final run changed no production code.
2. Migration suite: exit 0, 85.4% package coverage; Checkpoint file:
   97/113 statements, 85.8%.
3. Targeted Checkpoint race, `go vet`, incremental lint, Linux CGO-disabled
   build, and diff-format checks: exit 0; lint reported zero issues.
4. Tests inject before/after CAS failure, stale CAS, stable-read revision
   change, oversized/corrupt/unknown-field records, existing-directory
   conflicts, identity mismatch, phase rollback, illegal fence states,
   irreversible intent/complete, and restart after each durable boundary.
5. Remote-body audit proves the closed schema contains only immutable Job,
   config, Source/Target/Space identity, highest Phase, intent, and complete
   fence. No Round, per-file token/Revision, finding, floor, verification,
   report, upload ID, key, or content is serialized.
6. Fresh State and Deep Recovery are mandatory after every writable restart;
   intent recovery disables writes and only permits fence completion.
   Correctness, code quality, Design alignment, and scope: accepted.
7. Deviation: none.

### WP09: Target inventory, diff, and round model

#### D09 development

Status: `COMPLETED`
Dependencies: `A01`, `A05`, `A06`
Estimate: 210-330 production Net LoC

1. Stream target inventory within the Job Prefix and retrieve Revision,
   resource ID, type, mode, and checksum only when deep comparison requires it.
2. Store source/target observations in the Job's in-memory state and publish
   typed diffs only after a complete namespace scan.
3. Keep `scan_complete` separate from `round_converged`.
4. Track `last_observed_source_token` separately from
   `last_reconciled_source_token`; ordinary scans cannot advance the latter.
5. Classify target-only, source-only, content, metadata, type, identity, and
   Revision differences without crossing Target or control Prefix boundaries.
6. Add interrupted-scan, pagination, missing-checksum, concurrent-target, and
   capacity tests.

Execution record:

1. Owner: primary implementation agent.
2. Production files: `internal/migration/inventory.go`, bounded Target/observed
   fields in `state.go`, and symlink checksum population in `scanner.go`.
3. Test files: `internal/migration/inventory_test.go` and state test updates.
4. Production Net LoC: approximately 340. Scope audit: about 10 LoC above the
   package estimate, with no new state, persistence, Wire Contract, framework,
   or deferred feature; the repository hard cap remains open.
5. TDD red command failed to compile before the D09 API existed; targeted and
   full Migration unit tests then passed with exit 0.
6. Deviation: none.

#### A09 acceptance

Status: `PASSED`

1. Run target/diff/state tests with race detection and at least 85% coverage for
   round completion.
2. Prove an interrupted or failed scan never generates Delete work.
3. Verify control files are excluded and business paths remain confined.
4. Verify unresolved token candidates survive ordinary scans in the current
   process and keep `CurrentConverged=false`; restart requires a new baseline.
5. Review memory bounds, batch usage, and error classification.

Pass signal: diffs describe one complete bounded Job snapshot and never turn
partial observation into destructive work.

Acceptance record:

1. Owner: primary acceptance agent; final acceptance made no production edit.
2. `go test ./internal/migration`: exit 0, 86.3% package coverage;
   inventory/state statement coverage 90.3% (213/236).
3. Targeted Race, `go vet`, modified-file gofmt, and diff-format checks: exit 0.
4. Incremental lint with a fresh `/tmp` cache: exit 0, zero issues. The default
   global cache was sandbox-inaccessible (exit 5); the first fresh-cache run
   found one test-only QF1003 issue (exit 1), which was fixed before the full
   acceptance rerun.
5. Tests prove incomplete/interrupted scans expose no partial manifest or
   Delete finding, control traversal and path escape are rejected, List/Stat
   concurrency invalidates the scan, and BatchStat pages stay at or below 256
   while checksums are requested only for deep paths.
6. Observed and reconciled tokens remain distinct; ordinary rounds cannot
   advance reconciliation or converge an unresolved token, and restart clears
   both maps. Typed Client status errors, deterministic hardlink identity, memory
   bounds, correctness, code quality, Design alignment, and scope: accepted.
7. Deviation: none. The existing List Contract has no cursor; the Plan's
   pagination/capacity boundary is satisfied by per-directory streaming and
   bounded BatchStat pages without introducing a forbidden Server Wire change.

### WP10: Conditional apply engine

#### D10 development

Status: `COMPLETED`
Dependencies: `A01`, `A05`, `A06`, `A08`, `A09`
Estimate: 230-360 production Net LoC

1. Apply directories, regular files/hardlink primaries, link aliases, and
   directory permissions in dependency order; delete child before parent only
   in exclusive `SYNCING`.
2. Compute whole SHA-256 and consume merged conditional stream, fresh
   Multipart, and post-completion Stat APIs.
3. Create with expected Revision 0 and update with the exact observed positive
   Revision; return a rescan result on conflict and never retry
   unconditionally.
4. Revalidate Source Version Token around hashing and upload.
5. Use existing mkdir/symlink/hardlink/chmod/delete APIs only where the accepted
   state and ownership permit them.
6. Make a post-T0 operation without a safe conditional primitive an explicit
   blocker.
7. Use small- and large-file worker pools sharing one Job-level byte token
   bucket.
8. Keep only the current upload attempt in memory. On failure or interruption,
   abandon its upload ID; the next attempt rereads target Revision and Source
   Token and starts a new conditional upload from Part 1.
9. Never list, select, resume, or adopt a Multipart session by path.
10. Test ordering, CAS conflict, source mutation, fresh retry after every
    Multipart failure boundary, checksum reread, special types, and aggregate
    rate limiting.

Execution record:

1. Owner: primary implementation agent.
2. Production file: `internal/migration/apply.go`; test file:
   `internal/migration/apply_test.go`.
3. Production Net LoC: approximately 510. Scope audit: above the 230-360
   estimate, but every added branch maps to D10's concrete Source stability,
   create-only/CAS classification, type/link ordering, post-T0 safety, fresh
   Multipart, or shared-limiter acceptance. No Wire Contract, persistence,
   framework, or deferred feature was added; Worker production remains about
   2,518 LoC against the 3,580 hard cap.
4. TDD red command failed before the Apply API existed; targeted/full unit,
   coverage, Vet, lint, and diff-format development checks then passed.
5. Deviation: none.

#### A10 acceptance

Status: `PASSED`

1. Run apply/recovery tests with race detection and at least 85% coverage of
   CAS and source-stability branches.
2. Instrument a fake Client and prove every create/update is conditional.
3. Prove all workers share one limiter.
4. Inject failures at hash, Part upload, Complete, and post-upload reread; prove
   retry uses a new upload ID and starts from Part 1.
5. Reject path-based session lookup, upload-ID persistence, copied upload
   protocol, shell-based `drive9 cp`, or `pkg/client` changes beyond `C01`.

Pass signal: writes are source-stable, conditional, fresh-attempt-only, and
bounded by one per-Job limiter.

Acceptance record:

1. Owner: primary acceptance agent; final acceptance made no production edit.
2. `go test ./internal/migration`: exit 0, 85.6% package coverage. CAS/source
   stability functions `applyRegular`, `currentTarget`, `openSource`, and
   `validateSource`: 88.9%, 93.3%, 86.7%, and 100%.
3. Targeted Apply/token-bucket Race, `go vet`, lint with isolated `/tmp` cache,
   modified-file gofmt, and diff-format checks: exit 0; lint zero issues.
4. Client fresh V2/V1 retry/fallback/CAS fake-server tests: exit 0 with explicit
   non-container DSN. The first attempt without that DSN reached unavailable
   Docker in package `TestMain` and failed exit 1; it is not recorded as passed.
5. Tests prove Revision 0 create, exact positive-Revision update, 409 rescan,
   Source mutation at hash/open/upload boundaries, checksum reread, deterministic
   type/link/permission/delete ordering, child-before-parent exclusive delete,
   and one aggregate limiter shared by both pools.
6. Part/Complete failure creates a new Upload ID and starts again at Part 1.
   Post-upload Stat failure forces a rescan with no blind second write; any later
   necessary upload remains a fresh Client attempt.
7. Production scan found no Resume/session lookup, upload-ID persistence,
   copied HTTP protocol, shell-out, or D10 Client change. Correctness, code
   quality, Design alignment, and scope: accepted. Deviation: none.

### WP11: `SYNCING` orchestration

#### D11 development

Status: `COMPLETED`
Dependencies: `A07`, `A08`, `A10`
Estimate: 190-300 production Net LoC

1. Implement complete rounds: validate, scan, diff, apply, reread, and
   atomically publish the in-memory result.
2. In target-exclusive `SYNCING`, converge Create, Update, Delete, and
   rename-as-new-path-plus-old-path-delete.
3. On every start, run a complete deep recovery round before normal scheduling
   or readiness claims. The gate requires complete source/target observation,
   not convergence; blockers and grace candidates remain visible.
4. Set `ReadyForRollout` only after startup recovery, initial copy, and the
   latest complete in-memory Round converge.
5. Keep readiness informational and independent of startup phase.
6. Implement bounded retry, cancellation, clean shutdown, and immediate unsafe
   blockers.
7. Test repeatability, incremental changes, empty rounds, retry exhaustion,
   cancellation, mutation, restart recovery, and incomplete Round publication.

Execution record:

1. Owner: primary implementation agent.
2. Production files: `internal/migration/worker.go` and the `run` wiring in
   `cmd/drive9-migration/main.go`; test file:
   `internal/migration/worker_test.go`.
3. Production Net LoC: approximately 279, within the 190-300 estimate. Worker
   cumulative production is approximately 2,797 LoC against the 3,580 cap.
4. Complete scan/apply/reread publication, startup deep recovery, exclusive
   create/update/delete/rename convergence, bounded retry, typed authentication
   reload, cancellation, and unsafe complete-observation behavior are covered.
5. Deviation: none.

#### A11 acceptance

Status: `PASSED`

1. Run orchestration tests with race detection and at least 85% coverage of
   convergence decisions.
2. Prove repeated rounds are idempotent and exclusive Delete/Rename converges.
3. Prove `scan_complete` alone cannot set readiness or convergence.
4. Inject every Round boundary failure and retain the previous complete
   in-memory result while clearing convergence.
5. Review retry bounds and shutdown behavior.

Pass signal: `SYNCING` converges the exclusive Prefix through complete,
repeatable in-memory rounds without false readiness after start or failure.

Acceptance record:

1. Owner: primary acceptance agent; acceptance made no production edit.
2. Full Migration package tests: exit 0, 84.5% statement coverage;
   convergence-decision slice (`state.go` plus Worker deep/full Round logic):
   89.4% (127/142 statements).
3. Targeted orchestration Race, command tests, Vet, modified-file gofmt, and
   incremental lint: exit 0; lint reported zero issues.
4. Tests prove idempotent empty and non-empty rounds, incremental create/update,
   exclusive delete and rename convergence, restart deep recovery, readiness
   gating, and complete unsafe observation without false convergence.
5. Injected source-scan, target-scan, apply, and post-apply reread failures
   retain the prior complete Round and clear current convergence/readiness.
   Retry delay is capped, context cancellation is clean, and 401/403 reload the
   Secret-backed Client without making authentication errors permanent.
6. Correctness, code quality, Design alignment, and scope: accepted.
   Deviation: none.

### WP12: `DUAL_WRITE_REPAIRING`

#### D12 development

Status: `COMPLETED`
Dependencies: `A11`
Estimate: 260-390 production Net LoC

1. Persist the higher phase in the minimal Checkpoint, then run a complete deep
   recovery round. Derive an in-memory `repair_mtime_floor` as that Round's
   start minus grace before starting fast rounds.
2. Traverse the full EBS namespace and `lstat` every entry each fast round.
3. Deep-process only new paths, paths at/after the floor, changed Source Version
   Tokens, grace candidates, retries, and CAS conflicts.
4. Key in-memory grace candidates by normalized path and Source Version Token;
   only a token change restarts grace.
5. Use one delayed queue and, at expiry, reread both sides, revalidate source,
   and conditionally repair safe Create/Update.
6. Default grace to 60 seconds, enforce 30 seconds through 10 minutes, and
   reject runtime or per-Job changes.
7. Skip Delete, do not infer Rename, create renamed new paths, and retain
   target-only residue as a warning.
8. Block unsafe identity/type/non-regular updates, unreadable sources,
   permanent errors, and missing required checksum.
9. Preserve residual Revision-only ABA as a documented negative test.
10. On restart, discard floor, baselines, candidates, and conditions; repeat
    deep recovery and derive a new floor.
11. Test T0 restart, rollback rejection, candidate selection, unchanged-mtime
    token changes, grace lifecycle, CAS conflict, Delete/Rename boundaries, and
    convergence.

Execution record:

1. Owner: primary implementation agent.
2. Production files: `internal/migration/dual.go`, plus focused dual-phase
   additions in `internal/migration/state.go` and `internal/migration/worker.go`;
   tests are in `internal/migration/worker_test.go`.
3. Production Net LoC: approximately 306, within the 260-390 estimate. Worker
   cumulative production is approximately 3,102 LoC against the 3,580 cap.
4. The implementation persists phase through existing Checkpoint recovery,
   derives a process-local floor after deep recovery, traverses every Source
   entry in fast rounds, filters deep reads, maintains one in-memory grace/retry
   queue, and uses only the accepted conditional Apply engine.
5. Deviation: none.

#### A12 acceptance

Status: `PASSED`

1. Run dual-write tests with race detection and at least 85% coverage of grace,
   candidate, and CAS decisions.
2. Prove every fast round traverses the namespace while ordinary old paths skip
   deep reads.
3. Prove token candidates remain until successful deep work in the current
   process and restart cannot inherit convergence.
4. Prove post-T0 Delete never reaches Drive9 and Rename is not inferred.
5. Prove target-only residue alone does not block convergence.
6. Run the ABA negative test and reject any safety claim for undetectable
   Revision reuse.

Pass signal: online repair follows the exact fast-round, grace, CAS, and
non-destructive T0-T2 contract.

Acceptance record:

1. Owner: primary acceptance agent; acceptance made no production edit.
2. Full Migration tests: exit 0, 85.1% statement coverage; Dual/Grace/CAS
   decision slice: 88.4% (153/173 statements).
3. Targeted dual-write Race, Vet, modified-file gofmt, diff check, and
   incremental lint: exit 0; lint reported zero issues.
4. Tests prove complete namespace traversal with non-candidate deep-read skip,
   token-change persistence, unchanged-mtime detection, Grace reset only on
   Source Token change, target-Revision stability, CAS requeue, and restart
   loss of process-local facts.
5. Post-T0 delete/rename never reaches Drive9; a renamed new path is created
   after Grace while the old target remains a Warning. Unknown Revision blocks
   without write, and target-only residue alone converges.
6. The named Revision-reuse ABA negative test demonstrates the accepted
   non-guarantee and makes no safety claim. Correctness, code quality, Design
   alignment, and scope: accepted. Deviation: none.

### WP13: Asynchronous Diff event reporter

#### D13 development

Status: `COMPLETED`
Dependencies: `A01`, `A12`
Estimate: 50-80 production Net LoC

1. After each actual post-grace CAS attempt, enqueue the accepted event with
   Job, source, target, version, checksum, attempt, result, timing, and runtime
   location.
2. Use a bounded queue, short bounded retry, request timeout, and
   sent/failed/dropped counters.
3. Treat missing endpoint, timeout, Server error, and full queue as
   observability outcomes only.
4. Never include API keys or file contents.
5. Add success, retry, timeout, old-Server, full-queue, shutdown, and redaction
   tests.

Execution record:

1. Owner: primary implementation agent.
2. Production files: `internal/migration/reporter.go`, the CAS observation hook
   in `internal/migration/apply.go`, and Worker event construction/lifecycle in
   `internal/migration/worker.go`; tests are in
   `internal/migration/reporter_test.go` and `worker_test.go`.
3. Production Net LoC: approximately 151, above the 50-80 estimate. Scope
   audit: the delta is the closed 27-field diagnostic construction plus
   explicit lifecycle, timeout/retry, atomic counters, credential-rotation
   Client swap, and the actual-conditional-write hook required by D13. No new
   Wire Contract, persistence, generic framework, or data-path dependency was
   added. Worker cumulative production is approximately 3,253 LoC against the
   3,580 hard cap.
4. Deviation: none.

#### A13 acceptance

Status: `PASSED`

1. Run reporter and dual-write integration tests with race detection.
2. Make the endpoint hang, fail, disappear, and recover; prove data-path
   behavior is unchanged.
3. Verify one event per actual CAS attempt and none for a candidate that
   converges before CAS.
4. Verify bounded memory and accurate drop counters.
5. Verify the reporter uses the `A01`-accepted Client contract without
   changing it.

Pass signal: event reporting is useful and has a proven non-blocking failure
boundary.

Acceptance record:

1. Owner: primary acceptance agent; acceptance made no production edit.
2. Full Migration tests: exit 0, 85.5% statement coverage. Reporter functions
   are 94.4-100%; event construction is 80%.
3. Targeted Reporter/Dual integration Race, Vet, gofmt, diff check, and
   incremental lint: exit 0; lint reported zero issues.
4. Success, one bounded retry, timeout, 404/5xx, queue overflow, cancellation,
   credential-body redaction, and recovery are deterministic tests. Endpoint
   failure leaves the converged repair state unchanged.
5. The Apply hook emits one logical event only after an actual conditional
   write attempt, including success/conflict/failure; a candidate that matches
   before CAS emits none. Client endpoint retries retain the same event ID.
6. Correctness, code quality, Design alignment, and scope: accepted.
   Deviation: none.

### WP14: Local control socket, status, diff, and Attention

#### D14 development

Status: `COMPLETED`
Dependencies: `A05`, `A12`, `A13`
Estimate: 230-350 production Net LoC

1. Add a mode-0600 UDS with bounded JSON, deadlines, serialized mutations, and
   clean shutdown.
2. Implement local single-Job `status --output json` and streaming
   `diff --output jsonl`; do not aggregate Jobs or access Kubernetes APIs.
3. Report state, startup phase, startup-recovery gate, conditions, fence,
   current in-memory floor and Round facts, candidates, diffs,
   backlog/in-flight counts, event counters, and full verification.
4. Keep complete paths out of status and ordinary logs; expose them only via
   explicit diff and intentional Server diagnostic events.
5. Set `Attention` immediately for unsafe/unrecoverable failures or after the
   same retryable operational blocker lasts five minutes.
6. Clear Attention only after Migration rechecks recovery; never provide a
   force-clear.
7. Make Attention force `CurrentConverged=false` and reject cutover without
   phase rollback or unfence.
8. Test protocol, permission, deadline, exit code, schema, streaming,
   threshold, clearing, serialization, and unavailable socket.

Execution record:

1. Owner: primary implementation agent.
2. Production files: `internal/migration/control.go`, Worker control lifecycle
   in `internal/migration/worker.go`, and CLI socket wiring in
   `cmd/drive9-migration/main.go`; tests are in
   `internal/migration/control_test.go`, `worker_test.go`, and `main_test.go`.
3. Production Net LoC: approximately 231, within the 230-350 estimate. Worker
   cumulative production is approximately 3,484 LoC against the 3,580 cap.
4. The local protocol uses a fixed single-Job socket, mode 0600, a 64 KiB
   request bound, per-connection/client deadlines, JSON status, streamed JSONL
   findings, concurrent read-only requests, and a serialized mutation lock.
5. Deviation: none.

#### A14 acceptance

Status: `PASSED`

1. Run control/condition tests with race detection and at least 85% coverage of
   mutation serialization and Attention.
2. Verify concurrent status is read-only and mutating requests serialize.
3. Verify exact exit codes 0/1/2/3.
4. Scan output and logs for paths, contents, and keys; prove diff streams.
5. Compare the status contract with revision 1517.

Pass signal: operators can inspect and control one Job with deterministic,
safe, secret-free semantics.

Acceptance record:

1. Owner: primary acceptance agent; acceptance made no production edit.
2. Full Migration tests: exit 0, 86.9% statement coverage. Control mutation
   dispatch is 89.5%, status schema 100%, state condition recomputation 100%,
   and Client streaming 91.3%.
3. Targeted control/Attention Race, command tests, Vet, modified-file gofmt,
   diff check, and incremental lint: exit 0; lint reported zero issues.
4. Tests prove mode 0600, bounded strict JSON, stable path-free status,
   explicit path-bearing JSONL diff, concurrent status, serialized mutation
   dispatch, unavailable-socket classification, and exact CLI exits 0/1/2/3.
5. Retry Attention is deterministic at the five-minute threshold and clears
   only after a successful recheck; unsafe blockers remain immediate. Status,
   errors, and ordinary output contain neither API keys nor file bodies.
6. Correctness, code quality, Design alignment, and scope: accepted.
   Deviation: none.

### WP15: T1 `verify-full`

#### D15 development

Status: `COMPLETED`
Dependencies: `A12`, `A14`
Estimate: 100-160 production Net LoC

1. Record an idempotent verification request in memory and serialize it after
   the current fast Round without adding a phase.
2. Scan and checksum every current EBS file without the mtime filter.
3. Compare required target semantics and repair only safe mismatches through
   the same grace, stable-read, and CAS rules.
4. Resume fast rounds and expose in-memory request/result times, counts, and
   failure state through status.
5. Reject requests outside `DUAL_WRITE_REPAIRING`, reject overlap, and return
   the current process's completed result idempotently.
6. On restart, discard any request/result and require the operator to invoke
   `verify-full` again after the mandatory deep recovery round.
7. Keep observation duration and T2 decisions outside Migration.
8. Test serialization, idempotency, restart loss/reinvoke, mismatch, mutation,
   and interrupted verification.

Execution record:

1. Owner: primary implementation agent.
2. Production files: `internal/migration/verification.go`, with focused reuse
   hooks in `dual.go`, `control.go`, `state.go`, and `worker.go`; tests are in
   `internal/migration/verification_test.go`.
3. Production Net LoC: approximately 65, below the 100-160 estimate because
   verification reuses the accepted deep observation, Grace/CAS, state, and
   serialized-control paths. Worker cumulative production is approximately
   3,549 LoC against the 3,580 cap.
4. Deviation: none.

#### A15 acceptance

Status: `PASSED`

1. Run verification/control/recovery tests with race detection and at least 85%
   coverage of request transitions.
2. Prove full verification has no mtime filter and cannot overlap a fast round.
3. Prove completion is published only after one complete in-memory
   verification Round.
4. Prove restart discards the request/result, blocks cutover, and requires an
   explicit reinvocation after recovery.
5. Verify passed results cover only data through completion and make no cutover
   decision.

Pass signal: T1 produces a complete process-local per-Job verification result
without changing phase or automating T2.

Acceptance record:

1. Owner: primary acceptance agent; acceptance made no production edit.
2. Full Migration tests: exit 0, 87.1% statement coverage; full-verification
   lifecycle is 100% covered.
3. Targeted verification Race, modified-file gofmt, and full package tests:
   exit 0.
4. Tests prove unfiltered deep reads, serialized pending/running/completed
   states, passed/failed idempotency, mismatch counting, phase preservation,
   interruption failure, and restart loss/reinvoke semantics.
5. Target-only warnings remain non-failing, while blockers, Grace, retry, or
   Attention prevent a passed result. No observation duration or T2 decision
   was added. Correctness, code quality, Design alignment, and scope: accepted.
   Deviation: none.

### WP16: T2 irreversible cutover fence

#### D16 development

Status: `COMPLETED`
Dependencies: `A08`, `A14`, `A15`
Estimate: 160-240 production Net LoC

1. Accept cutover only in `DUAL_WRITE_REPAIRING` when converged, not in
   Attention, latest full verification passed, and no newer request invalidates
   it.
2. Stop new rounds, drain current work, and recheck the gate; resume repair if
   the gate fails before intent.
3. Persist irreversible fence intent in the minimal Checkpoint before exposing
   a final result.
4. After intent, restart may only finish fencing.
5. Once fenced, prohibit every Migration write and never implement unfence.
6. Keep successful Jobs fenced when another Job fails.
7. Inject failures before/after intent, during drain, and during final fence
   Checkpoint update.

Execution record:

1. Owner: primary implementation agent.
2. Production files: `internal/migration/fence.go`, with focused fence gates in
   `worker.go` and `control.go`; tests are in
   `internal/migration/fence_test.go`.
3. Production Net LoC: approximately 77, below the 160-240 estimate because
   the implementation reuses the accepted Checkpoint CAS and control-operation
   lock. Worker cumulative production is approximately 3,626 LoC; Client plus
   Worker is approximately 3,838 LoC. Scope audit: the overage maps only to the
   specified Client, Worker, control, verification, reporter, and fence
   contracts. The user explicitly directed that LoC remain advisory and that
   development continue when no implementation drift exists.
4. Deviation: none.

#### A16 acceptance

Status: `PASSED`

1. Run cutover/recovery tests with race detection and at least 90% coverage of
   fence transitions.
2. Prove no post-intent restart can resume Migration writes.
3. Prove pre-intent failure resumes repair and post-intent failure only
   completes fencing.
4. Cover early, stale-verification, Attention, skipped, and duplicate requests.
5. Review for hidden rollback or cross-Job behavior.

Pass signal: cutover creates a durable, irreversible, restart-safe
no-more-Migration-writes boundary.

Acceptance record:

1. Owner: primary acceptance agent; acceptance made no production edit.
2. Full Migration tests: exit 0, 87.3% statement coverage. Fence intent is
   100% covered and fence completion 93.8%.
3. Targeted Fence Race, Vet, modified-file gofmt, diff check, and incremental
   lint: exit 0; lint reported zero issues.
4. Tests prove early/Attention/stale-verification rejection, drain through the
   shared operation lock, exact Intent then Complete CAS, duplicate idempotency,
   and no post-intent Round or business write.
5. A known pre-intent failure resumes repair; an ambiguous/post-intent failure
   remains fenced. Restart from durable Intent can only complete the fence;
   complete-step failure retries forward. No rollback, unfence, or cross-Job
   behavior exists.
6. Correctness, code quality, Design alignment, and scope: accepted.
   Deviation: none.

### WP17: Build and operator handoff

#### D17 development

Status: `COMPLETED`
Dependencies: `A01`, `A07`, `A14`, `A16`
Estimate: 60-120 production Net LoC

1. Add `build-migration` and accepted release outputs while preserving all
   existing build targets and `CGO_ENABLED=0`.
2. Produce concise process-local plan, status, diff, verification, warning,
   limit, event, and fence output; do not add persisted reports.
3. Emit non-sensitive CSI handoff mappings for both target layouts.
4. Add a sample `config.yaml` and Runbook for T0 ConfigMap update/rollout,
   per-Job status, T1 verify, T2 prepare, and manual control-data cleanup.
5. State post-T0 residue, metadata loss, per-Job throttling, residual ABA, one
   Source per Job, and no rollback prominently.
6. Add build, help, sample, output-schema, redaction, and documentation
   consistency tests.

#### A17 acceptance

Status: `PASSED`

1. Build `drive9-migration` for Linux AMD64/ARM64 with `CGO_ENABLED=0` and
   confirm existing Server/CLI builds remain unchanged.
2. Run help and sample-config smoke tests.
3. Verify outputs and examples contain no key or unsupported behavior.
4. Verify no CSI, Server, deployment, CRD, `drive9` subcommand, or Client
   production change beyond `C01`.
5. Review documentation traceability and final Net LoC.

Pass signal: the binary is reproducibly buildable and operator handoff matches
the accepted customer workflow without widening V1.

Acceptance record:

1. Owner: primary implementation and acceptance agent; acceptance made no
   production edit.
2. Production files: `Makefile`, `cmd/drive9-migration/main.go`. Tests and
   handoff: `cmd/drive9-migration/main_test.go`,
   `internal/migration/handoff_test.go`,
   `internal/migration/migration_failpoint_test.go`, sample `config.yaml`, and
   the local operator Runbook. D17 added 15 production Go LoC plus 20 Makefile
   build-target lines; documentation and tests are excluded.
3. Migration and command unit suites: exit 0, with 87.3% and 87.2% statement
   coverage. Full targeted Race, Vet, gofmt/diff checks, and incremental lint:
   exit 0; lint reported zero issues.
4. The eight serial failpoint cases for Scan, checksum, Part upload, Complete,
   Checkpoint CAS, full verification, Fence Intent, and Fence Complete all
   passed. Multipart retry used a new Upload ID and retransmitted every Part.
5. `build-migration`, Linux AMD64/ARM64 release output, existing Server/CLI
   build, binary help, strict sample config, output schema, redaction, and
   Runbook consistency checks: exit 0.
6. Both one-EBS-per-Space and shared-Space/disjoint-Prefix handoff layouts are
   represented without keys. The Runbook states post-T0 residue, metadata
   loss, per-Job throttling, residual ABA, one Source per Job, manual exact
   control-directory cleanup, and no rollback.
7. Correctness, code quality, Design alignment, and scope: accepted. No Server,
   CSI, deployment, CRD, controller, or post-C01 Client production change.
   Deviation: none.

## 9. Final local Client/Worker acceptance gate

### CG99 validation

Status: `PASSED`
Dependencies: `A01` and every `A04` through `A17` are `PASSED`

Run failpoint instrumentation serially after ordinary tests and lint are idle:

1. `gofmt` checks, `go vet` for changed packages, and `make lint`.
2. Unit and race suites for `cmd/drive9-migration` and
   `internal/migration`, with all coverage floors.
3. `python3 scripts/run_failpoint_tests.py` or
   `make test-failpoint`, run alone.
4. `make test`, using the repository-approved MySQL/Podman path when locally
   available; otherwise record the unexecuted dependency-backed coverage for
   Dev environment regression.
5. Existing Server/`drive9` builds plus `drive9-migration` Linux
   AMD64/ARM64 builds with `CGO_ENABLED=0`.
6. Fake-Server integration checks against the external Server contract for checksum reads,
   fresh V1/V2 whole-checksum completion, capability preflight, CAS, and
   optional event reporting; assert Migration never invokes Resume.
7. Locally executable end-to-end scenarios: initial copy; incremental
   Create/Update/Delete/Rename
   in `SYNCING`; crash/restart during scan/upload/verification and at each
   phase/fence Checkpoint boundary; mandatory deep recovery; T0 rollout;
   concurrent Create/Update with grace/CAS; post-T0 residue; T1 full
   verification; T2 fence recovery; both layouts; multiple independent Jobs.
8. Security checks for secrets, contents, Prefix escape, control-path escape,
   and over-broad UDS permissions.
9. A PR-boundary audit proving no Server, datastore/backend, CSI, deployment,
   controller, or protocol-contract production change, and no `pkg/client`
   production change beyond `C01`.
10. Traceability from every in-scope requirement to passing evidence and from
    every exclusion to an absence check or negative test.

`CG99` passes only when:

1. Every locally executable required command succeeds. Dependency-backed Dev
   environment checks that cannot run locally are listed as unexecuted and are
   never recorded as passed.
2. Coverage, race, failpoint, regression, integration, and build checks pass.
3. Production Net LoC remains 2480-3820 or has a recorded non-expanding
   explanation.
4. The Client adapter matches and the Worker consumes the external Server
   Wire Contract without changing it.
5. T0, T1, T2, observation duration, and batch progression remain customer
   decisions.
6. The deviation register is empty or every entry has a user-confirmed
   disposition.
7. The final state is `READY_FOR_DEV_REGRESSION`, not whole-system Migration
   V1 acceptance.

Acceptance record:

1. Owner: primary acceptance agent; CG99 made no production edit after A17.
2. Modified-Go gofmt, changed-package Vet, `git diff --check`, and full
   `make lint`: exit 0; lint reported zero issues. Migration and command unit
   suites: exit 0 at 87.3% and 87.2% statement coverage. Client Migration plus
   non-DB generic Resume targets: exit 0 at 18.3% whole-package targeted
   coverage. All three targeted Race suites: exit 0.
3. Coverage floors pass: State 94.9%; Reconciliation aggregate 86.0%;
   Grace/CAS 87.7%; Verification 100%; Fence 97.2%. The eight serial Migration
   failpoint cases all pass in the final tree, and instrumentation disable
   succeeds. The repository runner also passed all Migration cases before the
   pre-existing backend suite stopped because no Podman/Docker socket or MySQL
   testcontainer was available; that dependency-backed remainder is not
   recorded as passed.
4. Client fake-Server integration passes checksum Stat/BatchStat compatibility,
   capability/limit preflight, typed errors and cache recovery, V2 preference,
   explicit-only V1 fallback, Revision 0/positive CAS, fresh V1/V2 retries,
   closed event DTO/redaction, and the assertion that Migration never invokes
   Resume. Existing non-DB generic Resume targets remain passing.
5. Local Worker scenarios pass initial copy, repeatable and incremental
   `SYNCING` Create/Update/Delete/Rename, incomplete-round safety, restart deep
   recovery, T0 grace/CAS repair, post-T0 residue, T1 full verification, T2
   fence and forward-only recovery, both target layouts, and two independent
   Worker/target states.
6. Existing Server/`drive9` host builds and `drive9-migration` Linux AMD64 and
   ARM64 static `CGO_ENABLED=0` release builds: exit 0. Binary help, strict
   sample config, stable output schema, UDS bounds/mode, Prefix/control escape,
   secret/content redaction, and Runbook consistency checks pass.
7. `make test TEST_TIMEOUT=30s` is not passed: it exits 2 because Podman and
   Docker are unavailable and the host has only about 0.8% free disk, below
   existing FUSE write-back tests' 10% safety threshold. No user data was
   deleted and no unrelated threshold was changed. Live external Server/MySQL,
   deployment, CSI, and whole Dev-environment scenarios remain unexecuted.
8. Production Go LoC is 212 Client plus 3,641 Worker, total 3,853. This is 33
   above the advisory reference ceiling. Scope audit finds no new state,
   Source, persistence, protocol, control plane, watcher, framework,
   deployment, or deferred feature; the user explicitly directed that a
   non-expanding LoC overage must not interrupt development. The 20-line
   Makefile build target is outside the production-Go count.
9. Final boundary audit passes: no Server, datastore/backend, CSI, deployment,
   CRD, controller, or external repository file changed; `pkg/client`
   production changes are limited to C01. Open deviations: zero; `DEV-001` is
   resolved by the user-specified Design-over-Plan authority rule.
10. Final state: `READY_FOR_DEV_REGRESSION`. This is not whole-system Migration
    V1 acceptance. No commit, push, PR, deployment, or external-document update
    was performed.

## 10. Deviation register

Append one row before making any behavior choice not fixed by revision 1517.

| ID | Task | Observed mismatch or ambiguity | Classification | Decision | Status |
| --- | --- | --- | --- | --- | --- |
| `DEV-001` | `D04` | D04 prose assigns generic config failure to exit 2, while Design 5.2 assigns configuration failure to exit 1. | `BLOCKER` | Follow the user-defined authority order: Design wins; configuration/argument/internal errors use 1, illegal phase/action uses 2. | `RESOLVED_BY_USER_AUTHORITY_RULE` |

Classifications:

1. `BLOCKER`: accepted CLI/Worker cannot be correct without resolution; stop
   the affected task and request direction.
2. `SERVER_BLOCKER`: the external Server Wire Contract is missing or wrong;
   stop and return the change to the external Server repository.
3. `FOLLOW_UP`: real but independently fixable; keep it out of V1.
4. `LATER_PHASE`: belongs to an explicitly deferred option.

## 11. Final handoff record

| Field | Result |
| --- | --- |
| Final state | `READY_FOR_DEV_REGRESSION` |
| Server contract base | External Wire Contract declared available; no local `SG99` assertion |
| Passed acceptance tasks | 15/15 (`A01`, `A04`-`A17`) plus local `CG99` |
| Production Net LoC | 212 Client + 3,641 Worker = 3,853 production Go LoC; non-expanding overage audit recorded |
| Coverage | Client targeted 18.3%; Migration 87.3%; command 87.2%; State 94.9%; Reconciliation 86.0%; Grace/CAS 87.7%; Verification 100%; Fence 97.2% |
| Race/failpoint result | Client, Worker, and command targeted Race passed; all eight Migration failpoints passed; dependency-backed repository remainder not passed |
| Integration result | Client fake-Server Contract and all local Worker scenarios passed; live external Server/MySQL not run |
| PR-boundary audit | Passed; only allowed Client/Worker/build/test/local-document surfaces changed |
| Dev environment regression | Not run and not passed: live Server/MySQL, Podman/Docker, deployment/CSI, and whole-environment scenarios remain |
| Open deviations | 0 |
| Commit/push/PR | Not part of this plan |
