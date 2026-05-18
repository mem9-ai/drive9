---
title: Drive9 reusable agent harness v2 plan
updated: 2026-05-14
watches:
  - AGENTS.local.md
  - claude-notes/drive9-ec2-adversarial-usage-test-plan.md
  - claude-notes/drive9-ec2-adversarial-usage-test-report.md
  - e2e/
  - pkg/client/client.go
  - pkg/fuse/
  - pkg/metrics/
  - pkg/server/instrumentation.go
---

## Summary

The EC2 adversarial run produced valuable Drive9 findings, but the harness itself was too ad hoc to reuse safely. The next step is a phase-1, in-repository, typed harness for local smoke and targeted regression workloads. It should make Drive9 agent workloads legible to future agents and maintainers through structured cases, machine-readable results, minimized repro scripts, and strict cleanup boundaries. Stress, fault injection, garbage collection, production log collection, and per-request CLI/FUSE trace propagation are explicit follow-up phases.

## Input Sources

1. Existing one-off plan: `claude-notes/drive9-ec2-adversarial-usage-test-plan.md`
2. Triaged test report: `claude-notes/drive9-ec2-adversarial-usage-test-report.md`
3. Harness practice reference: OpenAI, "Harness engineering: leveraging Codex in an agent-first world", https://openai.com/index/harness-engineering/

## Report Evaluation

1. The report contains valuable product findings.
2. Highest-value P1 finding: path edge-case failure for `space name unicode-测试 punct_!@%.txt` in both strict and interactive modes. This likely points at path escaping, normalization, FUSE create/write handling, or server path handling.
3. Highest-value workload finding: `git clone` and local git operations are unreliable on the mount. Completed clones left `.git/index.lock` in strict runs and one interactive run, and follow-up `git status/add/commit/mv/rm` failed.
4. Useful P2 finding: strict `fio` write throughput was extremely slow at about `704984` bytes/s for 1 GiB. This needs a control baseline before being treated as a regression, but it is a strong performance target.
5. Useful UX finding: `drive9 doctor fuse` exits nonzero when `/etc/fuse.conf` lacks `user_allow_other`, even when the test does not request `--allow-other`.
6. Invalid raw finding: the `strict-tail` P0 was caused by harness misuse of `--sync-mode`. Future reports must distinguish product, harness, infrastructure, environment, and inconclusive outcomes mechanically.

## Current Plan Assessment

1. Good one-time campaign:
   1. Fresh remote roots and local mountpoints.
   2. Explicit avoidance of existing mounts.
   3. Strict and interactive mode coverage.
   4. Realistic agent workload using git.
   5. Stress coverage for large files, small files, concurrency, unmount, and kill-during-write.
   6. Artifact discipline under `/tmp/drive9-agent-test-$TS/`.

2. Not yet a reusable harness:
   1. Shell orchestration allowed harness bugs such as bare `wait` and label/sync-mode confusion.
   2. Test case identity, sync mode, remote root suffix, and mountpoint suffix were not typed separately.
   3. Results were mostly markdown/text rather than trendable structured output.
   4. There was no local filesystem or prior-run baseline for performance claims.
   5. Failure classes were not mechanically separated.
   6. Resuming after partial failure required manual continuation.
   7. Observability was collected, but not in an agent-legible schema.

## Harness Design Principles

1. Repository-local system of record:
   The harness, case definitions, schemas, reports, and known repros should live under `e2e/agent-harness/`, not in temporary scripts or chat context.

2. Agent-legible feedback loop:
   A future agent should be able to run one command, inspect JSONL events and summary JSON, identify the failing case, rerun a minimized repro, and then validate a fix.

3. Mechanical boundaries:
   The harness must enforce root and mountpoint safety in code. Case names, sync modes, mount labels, and remote roots must be separate typed values.

4. Structured evidence first:
   Markdown is for humans. `events.jsonl`, `failures.jsonl`, and `summary.json` are the primary data model.

5. Progressive disclosure:
   Default runs should be short, local, and useful. Stress, fault-injection, production observability, and destructive cleanup phases must be opt-in modes with explicit approval.

6. Garbage collection:
   Successful generated roots should be cleanable by a dedicated GC command. Failed roots should be retained with explicit retention metadata.

7. Phase-1 implementation boundary:
   The first implementation should ship only `preflight`, `run --suite smoke`, `run --suite regression --case ...`, and `report`. It should not collect production Kubernetes logs or metrics, run stress/fault cases, or delete remote roots.

8. Contract-first implementation:
   Phase 1 must define case-file structure, workload config, expected outcome semantics, and harness error classification before adding workload execution code.

## Module And Dependency Contracts

1. Go module path:
   All harness Go imports must use the repository module root from `go.mod`: `github.com/mem9-ai/dat9`.

2. Expected internal imports:
   1. `github.com/mem9-ai/dat9/pkg/client` for file read/write helpers and any direct filesystem API probes.
   2. `github.com/mem9-ai/dat9/pkg/traceid` for deterministic direct-HTTP trace id generation when direct HTTP probes are added.
   3. `github.com/mem9-ai/dat9/pkg/mountpath` or `github.com/mem9-ai/dat9/pkg/pathutil` for path canonicalization where local validation must match Drive9 path rules.

3. Provision and status helper boundary:
   Use a local harness `net/http` helper, not `pkg/client`, for `POST /v1/provision` and `GET /v1/status`. The SDK does not expose a public provision method or typed status response; the harness should keep these two control-plane calls explicit with small typed request/response structs.

4. YAML parser:
   Use `gopkg.in/yaml.v3` for case parsing. Promote it to a direct dependency if phase-1 code imports it.

5. CLI execution boundary:
   FUSE workloads should drive the `drive9` CLI binary specified by `--drive9-bin`. Direct API probes may use `pkg/client`; CLI/FUSE behavior must still be validated through the binary under test.

## Proposed Layout

```text
e2e/agent-harness/
  README.md
  cases/
    smoke.yaml
    regression.yaml
    stress.yaml        # deferred
    fault.yaml         # deferred
  cmd/drive9-agent-harness/
    main.go
  internal/
    casefile/
    runner/
    mountproc/
    oracle/
    report/
    safety/
  schemas/
    case.schema.json
    manifest.schema.json
    event.schema.json
    failure.schema.json
    metric.schema.json
    summary.schema.json
    gating.schema.json
  repro/
    known-path-edge.sh
    known-git-lock.sh
```

## Runner Commands

Phase-1 commands:

1. `drive9-agent-harness preflight`
   Validate host, binary, config, server, FUSE, tool availability, mount safety, and writable artifact root.

2. `drive9-agent-harness run --suite smoke`
   Fast correctness and remount tests. Default reusable confidence check.

3. `drive9-agent-harness run --suite regression --case path-edge,git-lock,doctor-no-allow-other`
   Target known P1 failures and validate fixes quickly.

4. `drive9-agent-harness report --run-dir /tmp/drive9-agent-test-$RUN_ID`
   Rebuild `summary.md` and `summary.json` from structured artifacts.

Common phase-1 flags:

1. `--artifact-root /tmp`
   Parent for run directories. The runner creates `drive9-agent-test-$RUN_ID` below it.
2. `--mount-root /tmp`
   Parent for generated mountpoints. The runner creates `drive9-agent-$RUN_ID-$CASE_ID` below it.
3. `--remote-root-base /agent-adversarial-$RUN_ID`
   Remote root prefix used for generated case roots.
4. `--drive9-bin drive9`
   CLI binary or absolute path to the binary under test.
5. `--server $DRIVE9_BASE`
   Server URL, defaulting to the same environment convention as existing e2e scripts. Phase 1 should default to `http://127.0.0.1:9009`; hosted or production endpoints must be selected explicitly.
6. `--api-key $DRIVE9_API_KEY`
   Existing API key. Phase 1 requires this unless `--provision` is explicitly set.
7. `--provision`
   Provision a fresh tenant before running cases. The harness must call `POST /v1/provision`, capture the returned `api_key`, then poll `GET /v1/status` until `status == "active"` or the provision timeout expires.
8. `--provision-timeout 120s`
   Maximum wait for a provisioned tenant to become active.

Deferred commands:

1. `drive9-agent-harness run --suite stress`
   Run bounded `fio`, small-file storm, and concurrent workloads.

2. `drive9-agent-harness run --suite fault`
   Run kill-during-write and unmount edge cases. Requires explicit human approval.

3. `drive9-agent-harness gc --older-than 7d --successful-only`
   Clean only successful generated roots and mountpoints.

4. `drive9-agent-harness collect-server-evidence --run-dir ...`
   Optional production log and metrics collection. Requires explicit external-service approval and configured Kubernetes or metrics-backend credentials.

## Phase 1 Scope

Phase 1 is ready to implement with this scope:

1. Go command under `e2e/agent-harness/cmd/drive9-agent-harness`.
2. YAML case loading for smoke and regression suites only.
3. Local mount lifecycle, process tracking, command timeouts, and structured artifact writing.
4. Local-only observability: command stdout/stderr, mount logs, mount table snapshots, tool versions, process snapshots on timeout, and mount perf counters when available.
5. Report regeneration from `manifest.json`, `events.jsonl`, `failures.jsonl`, and `metrics.jsonl`.
6. Regression coverage for path-edge, git-lock, and doctor-no-allow-other cases.

Phase 1 non-goals:

1. Stress suite.
2. Fault-injection suite.
3. Garbage collection command.
4. Kubernetes log collection.
5. Production metrics backend queries.
6. New Drive9 client or CLI trace propagation flags.

## Remote Root Lifecycle

Phase 1 must create the remote source before mounting because `drive9 mount :/remote <mountpoint>` rejects nonexistent remote roots.

1. For each case, derive:
   1. `remote_root_base`: canonical `--remote-root-base`, for example `/agent-adversarial-$RUN_ID`.
   2. `case_remote_root`: `path.Join(remote_root_base, case.remote_root_suffix)`, for example `/agent-adversarial-$RUN_ID/path-edge-strict`.
   3. `mountpoint`: `filepath.Join(mount_root, "drive9-agent-"+run_id+"-"+case.mountpoint_suffix)`.
2. Before starting the mount, run `drive9 fs mkdir :$case_remote_root` with the selected API key. `drive9 fs mkdir` creates parent directories, so this also creates `remote_root_base` when absent.
3. Mount exactly `:$case_remote_root` at the generated mountpoint. Workload paths are relative to the mounted case root.
4. Do not mount `/` and do not emulate scoping by prefixing every workload path under the base. The mount source is the isolation boundary.
5. Phase 1 must not delete `case_remote_root`. Successful local mountpoints may be removed after verified unmount; remote-root cleanup is deferred to `gc`.

## Case Schema

Each suite file contains optional defaults and a list of cases. Phase 1 uses `cases/smoke.yaml` and `cases/regression.yaml`.

```yaml
defaults:
  timeout: 2m
  cleanup: retain_on_failure
  mount_ready_timeout: 20s
  remote_visibility_timeout: 5s
  command_retry_count: 8
  command_retry_sleep: 2s
cases:
  - id: smoke-strict
    suite: smoke
    sync_mode: strict
    expected_outcome: baseline_pass
    remote_root_suffix: smoke-strict
    mountpoint_suffix: smoke-strict
    workload:
      type: mount_smoke
      files:
        - relative_path: cli-to-fuse.txt
          content: smoke cli to fuse
        - relative_path: fuse-to-cli.txt
          content: smoke fuse to cli
    oracles:
      - type: cli_read_equals
      - type: remount_hash_equal
    severity:
      failure: P1
```

Each case should be declarative:

```yaml
id: path-edge-strict
suite: regression
sync_mode: strict
expected_outcome: bug_reproduced
remote_root_suffix: path-edge-strict
mountpoint_suffix: path-edge-strict
timeout: 2m
cleanup: retain_on_failure
workload:
  type: path_matrix
  paths:
    - "space name unicode-测试 punct_!@%.txt"
oracles:
  - type: fuse_write_success
  - type: cli_read_equals
  - type: remount_hash_equal
severity:
  failure: P1
```

Required typed fields:

1. `id`: stable case id.
2. `suite`: `smoke`, `regression`, `stress`, or `fault`.
3. `expected_outcome`: `baseline_pass`, `bug_reproduced`, or `fix_verified`.
4. `sync_mode`: `strict`, `interactive`, `auto`, or omitted for CLI-only cases. The harness maps these to the current `drive9 mount --durability` values (`strict` -> `fsync`, `interactive` -> `interactive`, `auto` -> `auto`) while retaining compatibility with older `--sync-mode` binaries.
5. `remote_root_suffix`: not reused as sync mode or label.
6. `mountpoint_suffix`: not reused as sync mode or label.
7. `timeout`: total case timeout, inherited from defaults when omitted.
8. `cleanup`: `always`, `retain_on_failure`, or `never`, inherited from defaults when omitted.
9. `workload`: typed workload config.
10. `oracles`: expected observable outcomes.
11. `severity`: default classification if an oracle fails.

Expected outcome semantics:

1. `baseline_pass`
   All oracles must pass. Any oracle failure is a product, environment, infrastructure, harness, or inconclusive failure according to classification.
2. `bug_reproduced`
   The case is expected to reproduce a known bug, but its oracles still encode correct behavior. If a correctness oracle fails, the report should count it under "known bugs reproduced", not as a new product failure or CI-gating failure. If all oracles pass, report `known_bug_fixed_candidate` so the case can be flipped to `fix_verified`.
3. `fix_verified`
   The case validates a previously known bug fix. All oracles must pass. Any oracle failure is a product regression.

Phase-1 workload types:

1. `mount_smoke`
   Mount a generated remote root, run CLI-to-FUSE and FUSE-to-CLI byte equality checks, unmount, remount, and verify durability.
   Required config:
   1. `files`: list of `{relative_path, content}` objects.
   2. Optional `read_after_write_timeout`, defaulting to suite `remote_visibility_timeout`.
   3. Optional `remount: true|false`, defaulting to `true`.
2. `path_matrix`
   Create each configured path through FUSE, verify exact bytes through CLI, remount, and verify exact bytes again.
   Required config:
   1. `paths`: list of relative file paths.
   2. Optional `content_template`, defaulting to `case_id:path`.
3. `git_workflow`
   Clone a small configured repository into the mount, check for `.git/*.lock`, run `git status`, modify a tracked file, run local `git add` and `git commit`, then verify expected file state after remount.
   Required config:
   1. `clone_url`.
   2. Optional `git_binary`, defaulting to `git`.
   3. Optional `git_timeout`, defaulting to `case timeout`.
   4. Optional `git_user_name`, defaulting to `Drive9 Harness`.
   5. Optional `git_user_email`, defaulting to `drive9-harness@example.invalid`.
   6. `clone_dir`: directory name under the mountpoint.
   7. `mutation`: object with `path`, `mode` (`append` or `overwrite`), and `content`.
   8. `commit_message`: local commit message.
   9. `expected_locks`: glob list expected after the workflow. This must encode correct behavior. For phase-1 git cases it should be empty, including `expected_outcome: bug_reproduced` cases.
   10. `expected_status`: expected `git status --porcelain` state after commit, usually empty for fix verification.
   11. `expected_commit_delta`: expected commit count delta after local commit.
   12. `remount_verify_paths`: list of repository-relative paths and expected SHA-256 values to verify after remount.
4. `doctor_fuse`
   Run `drive9 doctor fuse` for a generated mountpoint and classify the result against a configured expectation.
   Required config:
   1. `expect_exit`: expected exit code.
   2. `allow_nonzero_when_no_allow_other`: boolean, defaulting to `false`.

Phase-1 oracle payloads:

1. `fuse_write_success`
   Derived from workload unless explicitly overridden. For `mount_smoke`, derive paths and byte counts from `workload.files`. For `path_matrix`, derive paths from `workload.paths` and byte counts from generated content. Optional YAML override fields: `path`, `expected_bytes`, `command_id`.
2. `cli_read_equals`
   Derived from workload unless explicitly overridden. For file workloads, derive remote path and expected SHA-256 from generated or configured content. Optional YAML override fields: `remote_path`, `sha256`.
3. `remount_hash_equal`
   Derived from workload unless explicitly overridden. The runner records pre-unmount hashes for workload output paths, then compares them after remount. Optional YAML override fields: `paths`, `expected_hashes`.
4. `no_git_locks`
   Required for `git_workflow` and derived from `workload.expected_locks`. For correctness, this list should normally be empty. Optional YAML override fields: `globs`.
5. `git_status_equals`
   Required for `git_workflow` and derived from `workload.expected_status`. Optional YAML override fields: `expected_status`.
6. `git_commit_count`
   Required for `git_workflow` and derived from `workload.expected_commit_delta`. Optional YAML override fields: `repo_path`, `expected_delta`.
7. `command_exit`
   Required when the workload defines a command whose exit status is the primary oracle, such as `doctor_fuse`. Derived from `workload.expect_exit` when present. Optional YAML override fields: `command_id`, `expected_exit`.

Bare oracle entries such as `{type: cli_read_equals}` are allowed only when the selected workload defines enough data for deterministic derivation. Case validation must reject a bare oracle when required derived fields are unavailable.

All timeout, retry, and visibility waits must be explicit in the case file or inherited from suite defaults. Phase-1 defaults should match existing e2e conventions unless a case overrides them: mount readiness `20s`, remote visibility `5s`, large-file visibility `30s`, command retry count `8`, and retry sleep `2s`.

Provisioning rules:

1. A suite may set `requires_fresh_tenant: true` in defaults or on a case.
2. If any selected case requires a fresh tenant and `--api-key` is absent, the runner must require `--provision`.
3. If `--api-key` is present, the runner must not provision unless `--provision` is also set.
4. `preflight` must verify either an API key is available or `--provision` was requested.
5. Provisioned API keys must be written only to `manifest.json` redacted form and process environment for child `drive9` commands. Full secrets must not be emitted to logs or markdown reports.
6. `POST /v1/provision` response shape: `{ "api_key": string, "status": string }`.
7. `GET /v1/status` response shape for readiness: `{ "status": string }`. Unknown fields must be ignored.

## Structured Artifacts

All runs write to `$ARTIFACT_ROOT/drive9-agent-test-$RUN_ID/`; the default artifact root is `/tmp`.

1. `manifest.json`
   Run id, host, binary version, server, suites, git SHA of harness, approval mode, artifact root, mount root, remote root base, generated mountpoints, generated remote roots, and tracked process groups.

2. `events.jsonl`
   Every command, process start/stop, mount lifecycle event, timeout, artifact path, duration, exit code, and signal.

3. `failures.jsonl`
   One object per failure with fields:
   `case_id`, `severity`, `class`, `oracle`, `message`, `repro_path`, `artifact_refs`.

4. `summary.json`
   Counts by severity/class/suite, durations, performance metrics, pass/fail status, cleanup status.

5. `summary.md`
   Human-readable report generated from `summary.json` and `failures.jsonl`.

6. `repro/<case_id>.sh`
   Minimized reproduction for each failed product case.

7. `mount/*.log`
   Raw mount logs and perf counter output.

8. `metrics/*.json`
   Parsed metric sidecars. Phase 1 should include git duration and mount perf counters. Fio and small-file throughput sidecars are deferred until the stress suite lands.

9. `metrics.jsonl`
   One derived metric event per case and metric name. Phase 1 should include mount startup duration, command durations, git clone duration, byte counts, file counts, and parsed mount perf counters when present.

10. `gating.json`
   Machine-readable CI decision with `pass`, `fail`, `known_bug_reproduced`, `known_bug_fixed_candidate`, and `non_gating` counts. `bug_reproduced` cases must not fail the gate unless explicitly selected with a future strict mode.

## V1 Structured Artifact Schemas

All v1 artifacts must include `schema_version: "agent-harness.v1"` where the format is JSON object based. JSONL records must include the same field per line.

1. `manifest.json`
   Required fields: `schema_version`, `run_id`, `started_at`, `host`, `harness_git_sha`, `drive9_version`, `server`, `suites`, `selected_cases`, `artifact_root`, `mount_root`, `remote_root_base`, `generated_mountpoints`, `generated_remote_roots`, `process_groups`, `api_key_redacted`, `approval_mode`.
2. `events.jsonl`
   Required fields: `schema_version`, `run_id`, `case_id`, `ts`, `type`, `message`, `duration_ms`, `command_id`, `exit_code`, `signal`, `artifact_refs`.
   Allowed `type` values: `run_start`, `run_end`, `case_start`, `case_end`, `command_start`, `command_end`, `mount_start`, `mount_ready`, `mount_end`, `unmount_start`, `unmount_end`, `oracle_start`, `oracle_end`, `timeout`, `artifact_written`, `cleanup_start`, `cleanup_end`.
3. `failures.jsonl`
   Required fields: `schema_version`, `run_id`, `case_id`, `ts`, `severity`, `class`, `oracle`, `expected_outcome`, `message`, `observed`, `expected`, `repro_path`, `artifact_refs`.
   Allowed `severity` values: `P0`, `P1`, `P2`, `P3`.
   Allowed `class` values: `product`, `harness`, `environment`, `infrastructure`, `inconclusive`.
4. `metrics.jsonl`
   Required fields: `schema_version`, `run_id`, `case_id`, `ts`, `name`, `value`, `unit`, `source`, `artifact_refs`.
   Allowed `unit` values: `ms`, `bytes`, `files`, `count`, `bool`.
   Phase-1 metric names: `mount_startup_ms`, `command_duration_ms`, `git_clone_duration_ms`, `bytes_written`, `bytes_read`, `file_count`, `mount_perf_counter`.
5. `summary.json`
   Required fields: `schema_version`, `run_id`, `status`, `started_at`, `ended_at`, `duration_ms`, `cases`, `counts`, `artifacts`, `cleanup`.
   Allowed `status` values: `passed`, `failed`, `known_bugs_reproduced`, `inconclusive`, `harness_failed`.
   `counts` must include totals by `suite`, `expected_outcome`, `severity`, and `class`.
6. `gating.json`
   Required fields: `schema_version`, `run_id`, `gate_status`, `pass`, `fail`, `known_bug_reproduced`, `known_bug_fixed_candidate`, `non_gating`, `blocking_failures`.
   Allowed `gate_status` values: `pass`, `fail`, `non_gating`, `harness_failed`.
   `blocking_failures` must include only failures from `baseline_pass` and `fix_verified` cases unless a future strict-known-bug mode is explicitly enabled.

## Failure Classification

1. `product`
   Drive9 behavior violates the oracle.

2. `harness`
   Runner bug, invalid case definition, bad shell snippet, or invalid argument generation.

3. `environment`
   Host setup issue such as missing tool or unavailable FUSE capability.

4. `infrastructure`
   Network, EC2, GitHub, or external service availability issue.

5. `inconclusive`
   Evidence is insufficient; data is preserved but not counted as product failure.

The report should show both raw and triaged counts, but product release decisions should use triaged counts.

Internal error contracts:

1. `internal/casefile`
   Define sentinel errors for parse and validation failures, for example `ErrParse` and `ErrValidation`. Both classify as `harness`.
2. `internal/safety`
   Define sentinel errors for unsafe generated paths, for example `ErrInvalidRoot`, `ErrInvalidGeneratedPath`, and `ErrExistingMountpoint`. These classify as `harness` unless they reveal host state outside the harness contract, in which case they classify as `environment`.
3. `internal/mountproc`
   Define sentinel errors for mount startup timeout, unmount timeout, process-state mismatch, and unexpected process exit. Startup timeout is usually `environment`; process-state mismatch and unsafe cleanup are `harness`; remote Drive9 errors observed after successful mount are `product` or `infrastructure` according to oracle evidence.
4. `internal/oracle`
   Oracle failures must carry `case_id`, `oracle`, observed value, expected value, and enough artifact references for report regeneration.

## Core Oracles

1. CLI write to FUSE read: bytes match within timeout.
2. FUSE write to CLI read: bytes match within timeout.
3. Rename: target visible, old path absent.
4. Overwrite/truncate: final remote bytes match exact expected bytes.
5. Delete: deleted path absent via CLI and FUSE after TTL budget.
6. Path matrix: FUSE write succeeds and CLI read returns exact bytes.
7. Remount durability: normalized file hash set matches before/after.
8. Dual mount: mount B observes mount A write and mount A observes mount B write within TTL budget.
9. Git clone: no `.git/*.lock` after successful clone.
10. Git local workflow: expected commit count and expected renamed/deleted state.
11. Small-file storm: expected file count, read count, delete completion within threshold.
12. Open-fd unmount: first unmount may return busy, follow-up after fd close must succeed.
13. Kill-during-write: classify partial data as inconclusive unless writer acknowledged completion.
14. Performance: compare against configured local baseline and previous Drive9 median before calling a regression.

## Workload Suites

1. Smoke:
   1. preflight
   2. mount strict
   3. CLI write to FUSE read
   4. FUSE write to CLI read
   5. remount hash check
   6. clean unmount

2. Regression:
   1. Phase 1: path-edge strict and interactive
   2. Phase 1: git-lock strict and interactive
   3. Phase 1: doctor-no-allow-other UX check
   4. Deferred: dual-mount stale-cache check

3. Stress (deferred):
   1. `fio` 1 GiB sequential write with fsync
   2. cold sequential read
   3. random read
   4. 1000 small files
   5. 8 parallel 64 MiB writers

4. Fault (deferred):
   1. unmount with open fd
   2. kill fresh mount process during active write
   3. remount and classify recovered, partial, missing, or corrupt state

## Safety Model

1. `--artifact-root`, `--mount-root`, and `--remote-root-base` must be absolute, canonicalized paths with no `..`, no empty generated suffixes, and no shell expansion.
2. Generated mountpoints must be direct children of `--mount-root` and must match `drive9-agent-$RUN_ID-$CASE_ID`.
3. Generated remote roots must be direct children of `--remote-root-base`: `path.Join(remote_root_base, remote_root_suffix)`. The suffix must be a single clean path segment equal to the case's `remote_root_suffix`; it must not start with `/` and must not contain `/`, `.`, `..`, or backslashes.
4. The runner must reject a mountpoint that already exists as a mount, symlink, non-empty directory, regular file, or path outside `--mount-root`.
5. The runner must reject remote roots outside the generated remote-root base and must reject a computed `case_remote_root` equal to `remote_root_base` itself.
6. Existing EC2 mounts `/home/ubuntu/w3`, `/home/ubuntu/w4`, `/home/ubuntu/w5` are only part of the EC2 preset and remain read-only context. They are not phase-1 defaults.
7. Each mount process runs in its own process group.
8. Cleanup may kill only process groups recorded in `manifest.json`.
9. No bare `wait`; wait only explicitly tracked workload pids.
10. Every external command has a timeout.
11. Failed cases retain data by default.
12. Successful case cleanup may remove only the generated local mountpoint after a verified unmount. Remote-root GC is deferred to the future `gc` command.
13. Fault suite requires explicit human approval and is out of phase-1 scope.

## Observability

1. Client-side harness observability:
   1. Capture `drive9 --version`, mount table, config server, tool versions, `/dev/fuse`, and `doctor fuse`.
   2. Capture mount perf counters and parse them into `metrics/mount-*.json`.
   3. On timeout, capture `ps`, `/proc/$pid/status`, `/proc/$pid/stack` if readable, `/proc/$pid/fd`, and mount table.
   4. Capture git timings, file counts, lock paths, and exact git stderr.
   5. Deferred stress suite: capture fio JSON and derive summary metrics.
   6. Capture cleanup status for each mountpoint and remote root.

2. Agent-readable debug interface:
   1. `events.jsonl` is the unified run timeline.
   2. `metrics.jsonl` records derived metrics per case.
   3. `failures.jsonl` records oracle failures with artifact references.
   4. `debug/<case_id>/trace-ids.jsonl` records request step, trace id, route/path, start time, end time, and response status when known.
   5. `debug/<case_id>/` contains sliced logs, process snapshots, mount table, proc data, stderr/stdout, metric snapshots, and a repro script.

3. Phase 1 trace correlation:
   1. Drive9 server supports caller-provided `X-Trace-ID`; if absent, it generates one and echoes it in the response header.
   2. Direct HTTP harness probes may generate deterministic trace IDs, for example `drive9-harness-$RUN_ID-$CASE_ID-$STEP-$N`.
   3. The harness must record both the requested trace ID and the response `X-Trace-ID` for direct HTTP probes. If they differ, the case should be marked `trace_status: mismatch`.
   4. Current CLI and FUSE flows do not expose a trace ID option. Phase-1 CLI/FUSE cases must record `trace_status: server_generated_unlinked` and rely on local artifacts, generated remote paths, bounded time windows, actor IDs, and tenant/auth fields.

## Deferred: Server-Side Evidence

These sections are phase-2+ design notes. They are not phase-1 implementation tasks.

1. Future CLI/FUSE trace propagation:
   1. Add one of:
      1. a CLI flag such as `--trace-id-prefix` or `--trace-id`;
      2. an environment variable such as `DRIVE9_TRACE_ID_PREFIX`;
      3. a client option that injects `X-Trace-ID` before each request.
   2. Do not put per-case `trace_id`, `run_id`, or `case_id` into Prometheus labels. Those belong in logs and structured artifacts, not metrics labels.

2. Server-side log access and local LogQL analysis workflow:
   This is a deferred, opt-in mode and not part of phase 1. It requires the user to approve the external service action and provide or select the target environment. The production preset currently uses kube context `prod-dat9-eks-ap-southeast-1`, namespace `dat9`, and deployment selector `app=dat9-server`.

   1. Current verified Kubernetes commands for the production preset:

      ```bash
      kubectl config get-contexts
      kubectl --context prod-dat9-eks-ap-southeast-1 -n dat9 get pods -l app=dat9-server -o wide
      kubectl --context prod-dat9-eks-ap-southeast-1 -n dat9 get deploy dat9-server -o wide
      kubectl --context prod-dat9-eks-ap-southeast-1 -n dat9 logs -l app=dat9-server --since=10m --tail=500 > "$RUN_DIR/debug/<case_id>/server-logs.jsonl"
      ```

   2. Query the downloaded slice with `logcli --stdin`. The verified stdin selector is `{source="logcli"}`:

      ```bash
      SERVER_LOG="$RUN_DIR/debug/<case_id>/server-logs.jsonl"
      cat "$SERVER_LOG" | logcli --stdin labels
      cat "$SERVER_LOG" | logcli --stdin --quiet --output raw query '{source="logcli"} | json | msg="http_request"'
      cat "$SERVER_LOG" | logcli --stdin --quiet --output raw query '{source="logcli"} | json | msg="http_request" | duration_ms > 300'
      cat "$SERVER_LOG" | logcli --stdin --quiet --output raw query '{source="logcli"} | json | msg="http_request" | status >= 400'
      cat "$SERVER_LOG" | logcli --stdin --quiet --output raw query '{source="logcli"} | json | trace_id="<trace_id>"'
      cat "$SERVER_LOG" | logcli --stdin --quiet --output raw query '{source="logcli"} | json | msg="datastore_op_timing" | duration_ms > 100 | line_format "{{.operation}} result={{.result}} dur={{.duration_ms}} trace={{.trace_id}}"'
      ```

   3. In the future opt-in server-evidence mode, every failing or slow case should attach logcli extracts:
      1. `debug/<case_id>/server-http-errors.txt` for `status >= 400`.
      2. `debug/<case_id>/server-http-slow.txt` for slow `http_request` entries.
      3. `debug/<case_id>/server-datastore-slow.txt` for slow datastore operations.
      4. `debug/<case_id>/server-trace-<trace_id>.jsonl` for full trace expansion when a relevant `trace_id` is known.

   4. Current log format is JSON and includes fields such as `trace_id`, `msg`, `path`, `method`, `route`, `status`, `duration_ms`, `tenant_id`, `api_key_id`, backend timing fields, and datastore operation timings.
   5. `logcli --stdin` is useful for log filtering and JSON field extraction, but not metric queries. For aggregate summaries from downloaded slices, use `jq` or a small harness parser unless the logs are queried from Loki directly.
   6. `--limit` may not cap stdin output reliably, so generated extracts should use deterministic post-processing such as `head`, `sed -n`, or harness-side line limits.
   7. Stdin mode assigns local ingestion timestamps, so case analysis should rely on embedded JSON fields such as `ts`, `trace_id`, `duration_ms`, `path`, and `msg`.

3. Server-side metrics workflow:
   This is a deferred, opt-in mode and not part of phase 1.

   1. The server exposes Prometheus text at `/metrics`.
   2. In Kubernetes, the verified service proxy path uses the named service port `http`:

      ```bash
      kubectl --context prod-dat9-eks-ap-southeast-1 get --raw '/api/v1/namespaces/dat9/services/dat9-server:http/proxy/metrics'
      ```

   3. Current exposed metric families:
      1. `dat9_http_requests_total`
      2. `dat9_http_request_duration_seconds`
      3. `dat9_http_inflight_requests`
      4. `dat9_db_operations_total`
      5. `dat9_db_operation_duration_seconds`
      6. `dat9_db_pool_registered`
      7. `dat9_db_pool_connections`
      8. `dat9_db_pool_wait_count_total`
      9. `dat9_db_pool_wait_duration_seconds_total`
      10. `dat9_db_pool_closes_total`
      11. `dat9_service_operations_total`
      12. `dat9_service_operation_duration_seconds`
      13. `dat9_service_gauge`
      14. `dat9_tenant_events_total`
      15. `dat9_module_up`
      16. `dat9_module_uptime_seconds`
      17. `dat9_fuse_operations_total`
      18. `dat9_fuse_operation_duration_seconds`
      19. `dat9_fuse_operation_bytes_total`
      20. `dat9_fuse_remote_operations_total`
      21. `dat9_fuse_remote_operation_duration_seconds`
      22. `dat9_fuse_remote_operation_bytes_total`

   4. These metrics are useful for service-level triage:
      1. route-level HTTP error and latency changes;
      2. DB operation latency and errors;
      3. backend, datastore, S3, tenant pool, upload, quota, media extraction, and worker operation failures;
      4. DB pool pressure;
      5. tenant lifecycle/auth event counts.
   5. These metrics are not sufficient for per-case root cause by themselves because they do not include `trace_id`, `case_id`, `run_id`, path, tenant, or API key labels.
   6. In the future opt-in server-evidence mode, the harness should collect `metrics/server-before.prom` and `metrics/server-after.prom` around each case, then write `metrics/server-delta.json` with changed counters, histogram counts/sums, and gauges.
   7. When the production metrics backend query interface is known, the harness should prefer backend range queries around `[case_start - 30s, case_end + 30s]` and store results under `metrics/backend/<case_id>/`.
   8. Metrics findings should be attached as supporting evidence, not primary case identity. The primary per-case join key remains `trace_id` for direct HTTP probes and log time/path filters for CLI/FUSE until trace propagation exists.

4. Server-side case correlation contract:
   1. Direct HTTP case correlation is solved: inject `X-Trace-ID`, record the echoed header, and query logs by `trace_id`.
   2. CLI/FUSE phase-1 cases must record `server_trace_status: server_generated_unlinked` and rely on local artifacts, generated remote paths, bounded time windows, tenant/auth fields, and the FUSE `X-Dat9-Actor` value.
   3. CLI/FUSE trace propagation is a future Drive9 client/CLI change, not a blocker for phase-1 harness implementation.
   4. Need to document the production metrics backend query interface, credentials, and recommended query syntax before implementing server-evidence mode.
   5. Need to decide whether per-case server metric evidence should come from live `/metrics` before/after snapshots, metrics backend range queries, or both.
   6. Until CLI/FUSE trace propagation is implemented, harness reports should include `server_trace_status: direct_http_trace_verified_cli_fuse_unlinked` when a run mixes direct HTTP and CLI/FUSE cases.

## Report Format

`summary.md` should include:

1. Run identity.
2. Product findings first.
3. Harness/environment/infrastructure failures second.
4. Performance table with baselines.
5. Passed oracles.
6. Inconclusive evidence.
7. Cleanup status.
8. Repro script paths.

`summary.json` should be treated as authoritative for automation.

## Harness Testing

1. `internal/casefile`
   Table-driven tests for suite defaults, multi-case YAML loading, expected-outcome validation, missing required workload fields, and invalid enum values.
2. `internal/safety`
   Unit tests for rejecting empty roots, relative roots, `..`, symlinks, existing non-empty directories, existing mounts, and generated paths outside configured roots.
3. `internal/oracle`
   Unit tests for pass/fail evaluation, `bug_reproduced` handling, `known_bug_fixed_candidate`, and artifact references.
4. `internal/report`
   Golden tests that regenerate `summary.json` and `summary.md` from fixture `manifest.json`, `events.jsonl`, `failures.jsonl`, and `metrics.jsonl`.
5. `internal/mountproc`
   Unit tests for command construction, timeout classification, process state recording, and cleanup decisions using fake process runners where possible.

## Phase 1 Delivery Split

1. Phase 1a:
   Preflight, suite YAML parsing, safety validation, process/artifact writers, `mount_smoke`, and report regeneration.
2. Phase 1b:
   Regression workloads, oracle semantics for `bug_reproduced` and `fix_verified`, `gating.json`, and harness unit/golden tests.

## Implementation Plan

1. Build a minimal Go runner for `preflight`, `run --suite smoke`, `run --suite regression --case ...`, and `report`.
2. Add phase-1 case schema parsing and validation for `mount_smoke`, `path_matrix`, `git_workflow`, and `doctor_fuse`.
3. Add safety validation for artifact root, mount root, remote root base, generated mountpoints, and generated remote roots.
4. Add mount process manager with process groups, explicit PID tracking, timeouts, and verified unmount.
5. Add event, failure, metric, and manifest JSONL/JSON writers.
6. Add local artifact collectors for versions, mount table, mount logs, command stdout/stderr, timeout process snapshots, and mount perf counters.
7. Port the path-edge, git-lock, and doctor-no-allow-other cases as regression tests.
8. Add report regeneration from structured artifacts.
9. Defer stress suite, fault suite, remote-root GC, production logs/metrics, and CLI/FUSE trace propagation.

## Estimated Scope

1. Phase-1 smoke and regression runner: `800-1200 LoC`.
2. JSON schemas, docs, and harness tests: `250-450 LoC` equivalent documentation/test code.
3. Deferred stress and fault suites: additional `250-500 LoC`.
4. Deferred production server-evidence mode: additional `150-300 LoC`.

## Success Criteria

1. A future agent can run a single smoke command and get a structured pass/fail report.
2. Known path-edge, git-lock, and doctor-no-allow-other findings are reproducible by named regression cases.
3. Harness failures cannot be confused with product P0/P1 findings.
4. All fresh mounts are unmounted after a completed run.
5. All failed data is retained with explicit metadata.
6. Reports can be regenerated from raw structured artifacts without rerunning workloads.
