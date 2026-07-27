---
title: Drive9 Space Benchmark Error Logging Design
---

## Objective

Make workload failures diagnosable from stderr without producing one log line
per failed request during a long-running benchmark.

## Scope baseline

In scope:

1. Keep periodic throughput and counters on stdout.
2. Write periodic write/read error summaries to stderr.
3. Include the new and cumulative error counts plus the most recent tenant,
   worker, remote path, and complete error message.
4. Redact TiDB Cloud AK/SK and Space API keys before retaining or printing an
   error sample.
5. Flush errors accumulated since the last interval during shutdown.

Out of scope:

1. Logging every failed request.
2. Log file creation, rotation, retention, or compression.
3. Error classification or aggregation by cause.
4. Changing the JSON report schema.
5. Changing workload retries, pacing, or failure semantics.

The final production change is approximately `130-150 LoC` (Medium). The
initial `80-120 LoC` estimate did not fully account for threading stderr through
the CLI/runner boundary and flushing a consistent final snapshot.

## Data flow

Each worker continues to record its existing counters and bounded latency
histograms. On failure, it also atomically replaces the latest sample for that
operation with a sanitized immutable value containing the request context and
error message.

The existing report ticker owns error emission. Once per report interval it
compares cumulative write/read error counts with the counts previously emitted.
For each operation with new failures, it writes one line to stderr containing
the delta, cumulative count, and latest sample. This bounds output to at most
two lines per interval, independent of worker count or failure rate.

The same comparison runs once when the workload exits so errors after the last
tick are not lost. Error samples remain internal and are not serialized into
the benchmark JSON report.

## Security and testing

Sanitization happens before an error sample enters shared statistics. Log
fields use quoted formatting so response bodies cannot inject additional log
lines. Tests cover delta calculation, no duplicate output, shutdown flushing,
write/read context, and AK/SK/API-key exclusion.
