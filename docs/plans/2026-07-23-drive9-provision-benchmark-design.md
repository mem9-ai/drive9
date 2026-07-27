---
title: Drive9 Provision Benchmark CLI Design
---

## Objective

Add an internal `drive9-create-bench` command that measures Drive9 tenant provisioning
through the public `POST /v1/provision` and authenticated `GET /v1/status`
workflow.

## Scope baseline

In scope:

1. A standalone Go command at `cmd/drive9-create-bench`.
2. Explicit server selection through `--server` or `DRIVE9_SERVER`.
3. Optional TiDB Cloud credentials from `DRIVE9_PUBLIC_KEY` and
   `DRIVE9_PRIVATE_KEY`.
4. Worker concurrency, provision-request rate limiting, total tenant count,
   readiness polling, interruption handling, and JSON reporting.
5. Provision-request and end-to-end ready latency summaries.
6. Unit tests using an in-process HTTP server.
7. Console histograms for provision and ready latency samples.

Out of scope:

1. A `drive9 bench` product subcommand or release artifact.
2. A new config file or changes to `~/.drive9/config`.
3. Changes to Drive9 provisioning or client SDK interfaces.
4. FUSE and filesystem data-path workloads.
5. Automatic tenant deletion, pool creation, or shared-pool configuration.

Expected production size is `740-780 LoC` (Large). The original `300-450 LoC`
estimate did not fully account for cancellation-safe worker coordination,
rate limiting, polling error handling, report serialization, and secret
redaction. The histogram follow-up adds approximately `60-90 LoC` without
changing the request or report schema surfaces.

## Approach

The command uses standard HTTP directly. The public provision API has no typed,
context-aware SDK method, and adding one would expand this task into a shared
SDK change. Extending the Python FUSE benchmark would mix provisioning and
filesystem workloads, while adding a product CLI subcommand would expose an
internal load generator to normal users.

Each worker waits on one process-wide provision rate limiter, sends one
non-retried provision request, and optionally polls the returned tenant API key
until the tenant is active. A provision POST is never retried because the API
does not expose an idempotency key and an ambiguous retry could create an extra
tenant. Transient status failures are retried until the tenant timeout; terminal
tenant states and non-retryable status failures are reported immediately.

## CLI contract

The command requires `--server` or `DRIVE9_SERVER` and `--out`. TiDB Cloud keys
must be both present or both absent. Supplying a spending limit requires both
keys. Benchmark controls are flags:

1. `--total`, `--concurrency`, and `--rps`.
2. `--wait-ready`, `--poll-interval`, and `--timeout`.
3. `--tidbcloud-spending-limit`.
4. `--out`.

Safe defaults create one tenant with one worker at one request per second.
Credentials and returned tenant API keys are never printed or written to the
report.

## Results and failures

The JSON report records configuration without secrets, timestamps, total
elapsed time, provision and status request counts, success/failure totals,
failure messages, and raw plus median/p95 latency values. A run exits non-zero
when any tenant fails. Interrupting the process cancels queued work and
in-flight requests, then writes the partial report when possible.

Console output includes separate provision and ready latency histograms when
the corresponding samples exist. The renderer follows the original `bench`
style: up to 20 range buckets, a 10-second minimum bucket width, and bars scaled
to 50 characters. Histograms are derived from existing raw samples and do not
change the JSON report schema.

Tests cover flag validation, credential validation, provision payloads, the
provision-to-active flow, no-wait behavior, failed requests, and secret
exclusion from reports, plus histogram rendering and empty-sample behavior.

## Live observability follow-up

The benchmark must make a long-running create workload observable across a
Drive9 server upgrade without restarting the client.

In scope:

1. Make each accepted inventory record visible to readers immediately while
   retaining batched durability syncs.
2. Write timestamped, report-interval window statistics to stdout, including
   success and failure counts plus provision and ready P50/P90/P95/P99 latency.
3. Keep request failures on stderr, add request latency to each error, and log
   each final tenant failure.
4. Preserve cumulative final reports, request pacing, retry behavior, and
   secret redaction.

Out of scope:

1. Per-success request logs.
2. Log rotation, retention, or external metric publication.
3. Report schema changes.
4. Drive9 server changes or benchmark deployment.

The final production change is approximately `190-230 LoC` (Medium). The
original estimate did not fully account for exact per-window quantile state and
separate terminal tenant failure records.

The inventory writer flushes its userspace buffer after each JSONL record so
live readers can observe it. It retains the existing `fsync` boundary of 100
records and syncs any remainder during graceful shutdown.

Each report interval owns a bounded latency window. It prints the window and
cumulative counts followed by exact latency summaries for completed provision
requests and successfully measured readiness flows. The window resets after
printing so pre-upgrade samples do not dilute post-upgrade results. The final
report continues using its existing bounded cumulative histograms.
