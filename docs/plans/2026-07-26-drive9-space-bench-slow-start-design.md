---
title: Drive9 Space Benchmark Slow Start Design
---

## Objective

Gradually admit selected Spaces into the filesystem workload so a large run
does not activate every cold backend at once.

## Scope baseline

In scope:

1. Add `--space-start-rps`, the maximum number of new Spaces activated per
   second.
2. Keep `0` as the default and preserve immediate activation.
3. Start all configured workers for a Space together when that Space receives
   an activation slot.
4. Keep `--io-rps` as the independent global filesystem request limit.
5. Report `active_spaces=<active>/<total>` in periodic stdout progress.
6. Stop pending activation promptly when the workload context is cancelled.
7. Add focused tests and update the benchmark README.

Out of scope:

1. An in-flight request limit.
2. Ramping or dynamically adjusting `--io-rps`.
3. Retry or backoff changes.
4. Server-side changes.
5. Readiness polling changes.
6. Changes to the JSON report schema.

Estimated production size: `70-110 LoC` (Medium). Tests and documentation are
excluded.

## Behavior

The workload keeps the selected inventory order. The first Space activates
immediately, and later Spaces receive evenly paced activation slots. Once a
Space activates, all of its `--workers-per-space` workers start and continue
until the workload ends.

`--space-start-rps 20` activates 20 new Spaces per second, so 10,000 Spaces
take about 8 minutes 20 seconds to enter the workload. This does not guarantee
that a backend is warm; `active_spaces` means the client has enabled the Space
and launched its workers.

The existing global `--io-rps` limiter remains unchanged and applies to PUT,
GET, and DELETE requests from all active Spaces.

## Acceptance criteria

1. Without the new flag, all Spaces start as before.
2. With a positive rate, later Spaces do not issue requests before their
   activation slots.
3. The global operation limiter remains shared by every active worker.
4. Progress output exposes active and total Space counts.
5. Cancellation ends both pending activation and active workers without
   waiting for future activation slots.
