---
title: Reusable Performance Report Format Proposal
---

## Problem

Current performance reports are manually assembled from one-off run artifacts. The upload/download report has a useful customer-facing shape, but it is not yet reusable across other workloads such as FUSE, metadata, API, search, journal, or mixed workloads.

Manual report editing creates three risks:

1. Metric definitions can drift across reports.
2. Important environment details can be omitted.
3. Reproducing a report requires chat history or operator memory instead of structured artifacts.

## Goal

Create a reusable, reproducible customer performance report workflow where Markdown reports are generated from structured harness artifacts.

The same report format should support different test cases while preserving consistent sections, metric definitions, gates, and artifact references.

## Non-Goals

1. Do not make the report template specific to upload/download.
2. Do not require manual editing for normal report generation.
3. Do not replace raw harness artifacts with Markdown.
4. Do not hide failed or inconclusive scenarios.

## Proposed Format

Every customer performance report should use these sections:

1. Executive Summary
2. Scope
3. Environment
4. Infrastructure Specification
5. Results
6. Observations
7. Artifacts
8. Conclusion

The Results table should use a common minimum shape:

| Case | Workload | Load | Success | QPS | Throughput | Avg | p50 | p95 | p99 | Gate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|

Workload-specific reports may add columns only when needed, but the core columns should remain stable.

## Metric Definitions

The report generator should define metrics explicitly in each report:

1. QPS: successful operations or estimated request units divided by measured wall time.
2. Throughput: successful bytes divided by measured wall time.
3. Avg: mean successful operation latency.
4. p50, p95, p99: latency percentiles for successful operations.
5. Success: successful operations over attempted operations.
6. Gate: pass, fail, or non-gating based on the workload gates.

Workload-specific QPS definitions must be named clearly. For example, upload/download can report both file completion QPS and multipart request QPS.

## Artifact Contract

The implementation should preserve the current `agent-harness.v1` artifact files and add perf-specific artifacts under a `perf/` subdirectory. This avoids changing the meaning of existing files such as `manifest.json`, `metrics.jsonl`, `summary.json`, and `gating.json`.

Existing harness artifacts remain:

1. `manifest.json`: existing `agent-harness.v1` run manifest.
2. `events.jsonl`: existing command and lifecycle events.
3. `failures.jsonl`: existing oracle failures.
4. `metrics.jsonl`: existing `agent-harness.v1` metrics.
5. `summary.json`: existing harness summary.
6. `gating.json`: existing gate result.

New perf artifacts:

1. `perf/environment.json`: host, product, server, cloud, and storage metadata.
2. `perf/results.jsonl`: one line per measured operation.
3. `perf/summary.json`: per-scenario perf rollup used by the customer renderer.
4. `perf/customer-report.md`: customer-facing Markdown report.
5. `perf/publish-manifest.json`: written only after publish succeeds or partially succeeds.
6. Optional `perf/evidence/`: workload logs, CLI log slices, and server evidence.

Markdown is a presentation artifact. JSON and JSONL remain the source of truth.

### `perf/results.jsonl` Schema

Each line should use `schema_version: "perf-result.v1"` and contain:

1. `run_id`: string.
2. `case_id`: string.
3. `scenario_id`: string, optional when equal to `case_id`.
4. `operation_id`: string.
5. `operation`: string such as `upload`, `download`, `fuse_write`, `fuse_read`, `http_request`, or `metadata_op`.
6. `status`: `ok`, `failed`, `timeout`, or `skipped`.
7. `started_at`: RFC3339 string.
8. `ended_at`: RFC3339 string.
9. `duration_ms`: number.
10. `bytes`: number, 0 when not applicable.
11. `request_units`: number, used for request-QPS calculations when one operation maps to multiple data-plane requests.
12. `local_path`: string, optional.
13. `remote_path`: string, optional.
14. `error_class`: `product`, `harness`, `environment`, `infrastructure`, `inconclusive`, or empty.
15. `error`: string, optional tail-safe error text.
16. `artifact_refs`: list of relative artifact paths.

### `perf/environment.json` Schema

The file should use `schema_version: "perf-environment.v1"`.

Required fields:

1. `run_id`
2. `host`
3. `os`
4. `kernel`
5. `architecture`
6. `product_version`
7. `server_endpoint`

Optional fields should be present with `"unknown"` when unavailable:

1. `cloud_provider`
2. `instance_type`
3. `vcpu`
4. `cpu_model`
5. `memory`
6. `storage_type`
7. `storage_size`
8. `storage_iops`
9. `storage_throughput`
10. `storage_encrypted`

### `perf/summary.json` Schema

The file should use `schema_version: "perf-summary.v1"` and include the `PerfReport` model from the Data Model section. The renderer should read `perf/summary.json` first. If it is missing, the report command may compute it from `perf/results.jsonl` and `perf/environment.json`.

## Published Workspace

After local archival, the harness should upload the full report result to a dedicated Drive9 workspace so team members and customers can download the report bundle from one stable location.

The published bundle should include:

1. Final customer report Markdown.
2. `manifest.json`.
3. `summary.json`.
4. `gating.json`.
5. `perf/environment.json`.
6. `perf/results.jsonl`.
7. `perf/summary.json`.
8. Relevant log slices and evidence files.
9. A publish manifest describing report title, run ID, upload time, source server, workload, gate status, and artifact paths.

Recommended remote layout:

```text
/performance-reports/
  <suite>/
    <YYYY-MM-DD>/
      <run_id>/
        manifest.json
        summary.json
        gating.json
        perf/
          customer-report.md
          environment.json
          results.jsonl
          summary.json
          evidence/
        publish-manifest.json
```

The upload should be an explicit harness step, not an implicit side effect of report generation.

Example command:

```bash
drive9-agent-harness publish-perf \
  --run-dir /tmp/drive9-perf-... \
  --workspace-root :/performance-reports \
  --api-key "$DRIVE9_API_KEY"
```

Publishing should create or update an index file, for example:

```text
/performance-reports/index.json
```

The index should contain enough metadata for users to find reports without browsing raw directories:

1. Report title.
2. Run ID.
3. Test window.
4. Workload name.
5. Service endpoint.
6. Gate status.
7. Report path.
8. Summary path.

### Publish Failure Model

Publishing is explicit and idempotent by `run_id`.

1. Upload artifacts under the immutable run path first.
2. Write `publish-manifest.json` after artifact upload attempts finish.
3. Update `/performance-reports/index.json` last.
4. If artifact upload fails, return a non-zero exit and leave any uploaded files under the run path for inspection.
5. If index update fails after artifacts upload, return a non-zero exit with a retryable error. Re-running `publish-perf` for the same `run_id` should retry index update without duplicating artifacts.
6. If concurrent publishers update the index, use best-effort read-modify-write. If the backend supports revision checks, use them. If not, detect duplicate `run_id` entries and keep the newest entry for the same `run_id`.
7. Authentication uses the same environment and flags as other harness Drive9 operations: `--api-key`, `DRIVE9_API_KEY`, and `--server` or `DRIVE9_BASE`.

## Data Model

Add a perf-specific summary model alongside the current harness report model:

1. `PerfReport`
   1. `schema_version`
   2. `run_id`
   3. `title`
   4. `started_at`
   5. `ended_at`
   6. `overall_status`: derived from existing `summary.json.status` and `gating.json.gate_status`, rendered as `PASS`, `FAIL`, or `INCONCLUSIVE` in customer Markdown
   7. `scope`
   8. `environment`
   9. `infrastructure`
   10. `scenarios`
   11. `artifacts`
   12. `conclusion`

2. `PerfScenario`
   1. `case_id`
   2. `workload`
   3. `load`
   4. `attempted`
   5. `successful`
   6. `failed`
   7. `qps`
   8. `throughput`
   9. `latency_avg`
   10. `latency_p50`
   11. `latency_p95`
   12. `latency_p99`
   13. `gate_status`: same values as existing `gating.json.gate_status`
   14. `metric_notes`

3. `InfrastructureSpec`
   1. `provider`
   2. `instance_type`
   3. `architecture`
   4. `vcpu`
   5. `cpu_model`
   6. `memory`
   7. `storage_type`
   8. `storage_size`
   9. `storage_iops`
   10. `storage_throughput`

## CLI Design

Add a customer report mode to the harness:

```bash
drive9-agent-harness report --run-dir /tmp/drive9-agent-test-... --format customer-perf
```

Optional flags:

```bash
drive9-agent-harness report \
  --run-dir /tmp/drive9-agent-test-... \
  --format customer-perf \
  --title "Drive9 Upload and Download Performance Test Report" \
  --output customer-report.md
```

The default output should be:

```text
<run-dir>/perf/customer-report.md
```

Add a publish command for uploading the final report bundle to Drive9:

```bash
drive9-agent-harness publish-perf \
  --run-dir /tmp/drive9-agent-test-... \
  --workspace-root :/performance-reports
```

This intentionally follows the existing flat harness command style (`run`, `report`, `gc`, `collect-server-evidence`). Nested commands such as `perf publish` are deferred unless the CLI is refactored more broadly.

## Implementation Plan

1. Add `internal/report/perf.go`.
   1. Load existing `manifest.json`, `summary.json`, and `gating.json`.
   2. Load `perf/environment.json`, `perf/results.jsonl`, and optional `perf/summary.json`.
   3. Normalize workload-specific metrics into `PerfScenario`.
   4. Compute missing rollups from `perf/results.jsonl` when available.

2. Add perf artifact recording.
   1. Extend the runner with a small perf recorder that writes `perf/results.jsonl` for workloads that opt into customer perf reporting.
   2. Capture one result row per measured operation.
   3. Keep existing `events.jsonl`, `failures.jsonl`, and `metrics.jsonl` behavior unchanged.
   4. Write `perf/environment.json` once per run during preflight or run startup.

3. Add `internal/report/perf_markdown.go`.
   1. Render the stable Markdown sections.
   2. Keep the Results table stable across workloads.
   3. Include workload-specific metric notes below the table.

4. Add `perf/environment.json` generation.
   1. Capture OS, kernel, product binary version, server endpoint, and host.
   2. On EC2, capture instance type and EBS specs when AWS credentials are available.
   3. If cloud metadata is unavailable, write `"unknown"` instead of omitting fields.

5. Extend the `report` command.
   1. Add `--format summary` as the current default.
   2. Add `--format customer-perf`.
   3. Add `--output`.

6. Add schemas.
   1. `schemas/perf-summary.schema.json`
   2. `schemas/perf-result.schema.json`
   3. `schemas/perf-environment.schema.json`
   4. Keep existing `agent-harness.v1` schemas compatible. Do not add perf-only fields to existing closed enums unless a workload explicitly records them in `metrics.jsonl`.

7. Add tests.
   1. Golden Markdown rendering test.
   2. Missing optional environment fields test.
   3. Failed scenario rendering test.
   4. Upload/download fixture test using the current report as a golden reference.
   5. Perf result schema validation test.
   6. Publish idempotency and partial-failure tests with a fake Drive9 client.

8. Add publish support.
   1. Upload the final report bundle to a configured Drive9 workspace root.
   2. Preserve the run directory structure under `<suite>/<date>/<run_id>/`.
   3. Write `publish-manifest.json`.
   4. Update `index.json` with the published report metadata.
   5. Make publish idempotent for the same run ID.

9. Define failed-scenario rendering.
   1. `Gate` shows `fail`, `harness_failed`, or `non_gating`.
   2. Latency and throughput columns show measured values for successful completed operations.
   3. If no operations completed, metric cells render as `N/A`.
   4. The Observations section includes the top failure class and the first bounded error message.

## Reproducibility Requirements

Every generated report must include:

1. Run ID.
2. Test window.
3. Product version and git hash.
4. Server endpoint.
5. Workload parameters.
6. Gate definitions.
7. Infrastructure specification.
8. Raw artifact references.
9. Report generator schema version.

The report must be regenerable from the run directory without rerunning the workload.

Published reports must also be downloadable from the Drive9 workspace using only the published report path or index entry.

## Resolved Design Decisions

1. `--format customer-perf` is not the default report format. The current `summary` behavior remains the default.
2. `perf/environment.json` reuses existing manifest values conceptually but stores a perf-specific environment snapshot so the report is regenerable from `perf/` artifacts.
3. `PerfReport.overall_status` is derived from existing `summary.json.status` and `gating.json.gate_status`.
4. `report --format customer-perf` requires `manifest.json`, `summary.json`, and `gating.json`. It tolerates missing `perf/environment.json` by rendering `"unknown"` fields, and tolerates missing `perf/summary.json` when `perf/results.jsonl` is present.
5. `perf/results.jsonl` is produced by the runner for perf-aware workloads. It is not derived from generic `events.jsonl` because generic command events do not always contain bytes, request units, or workload-specific operation IDs.
6. Publishing uses the Drive9 client path already available to the harness, with the same server and API key configuration as run-time Drive9 operations.

## Acceptance Criteria

1. `drive9-agent-harness report --format customer-perf` produces a customer-ready Markdown report at `perf/customer-report.md`.
2. The same renderer supports upload/download, FUSE, API, and metadata workloads.
3. The report includes Avg, p50, p95, and p99 latency.
4. The report includes infrastructure details when available, including EBS type, size, provisioned IOPS, and provisioned throughput.
5. The Markdown output is deterministic for the same input artifacts.
6. Missing optional fields are rendered as `unknown`, not silently dropped.
7. Raw JSON and JSONL artifacts remain the source of truth.
8. `drive9-agent-harness publish-perf` uploads the complete report bundle to the configured Drive9 workspace.
9. The publish step writes a discoverable `index.json` entry for the run.
10. Failed scenarios render measured partial metrics or `N/A` without hiding the failure.

## Estimated Scope

Expected production change: `350-550 LoC`.

Expected tests and schemas: `300-450 LoC`.

Total expected scope: `650-1000 LoC`.
