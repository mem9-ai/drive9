# SSE Event Log: 7-Day Retention, Tenant-DB Residency, Zero Cold-DB Wakeup

Status: implemented on branch `feat/sse-event-log-retention` (P0–P2; structural payload remains a separate track)

## Context

drive9 delivers filesystem change notifications over SSE (`GET /v1/events`). Each mutation writes one durable row into the tenant's `fs_events` table; a lightweight pointer in the central `tenant_notify_outbox` table wakes peer pods so cross-pod subscribers can fetch the row without polling every tenant database.

Today `fs_events` rows are retained for **1 hour** (hard-coded `fsEventsRetention` at `pkg/server/server.go:1178`). Two new requirements motivate a redesign of the retention and durability behavior:

1. **Events are pushed to external customers.** The stream is becoming a customer-facing integration channel, so the replay window must cover realistic consumer outages: **7 days** instead of 1 hour.
2. **Tenant databases are serverless.** Periodically waking cold tenant DBs for polling or retention sweeps creates a per-request billing storm across thousands of mostly-idle tenants. The design must touch a tenant DB **only** when that tenant is already active.

Durability requirement: **99.99% event availability**, defined below as SLIs plus an explicit loss-budget formula — not as a bare percentage. A failed event insert must **not** fail the enclosing filesystem mutation — mutations are primary, events are derived. If a customer contract genuinely requires every single event, this design does not satisfy it; that requirement leads back to co-transactional writes or per-consumer acks, both rejected here (see "Rejected alternatives"). Align on 99.99% before implementation.

## Goals

- `fs_events` retention raised to 7 days, operator-configurable.
- **Persisted completeness:** every event that was successfully written to `fs_events` remains replayable for the full retention window, for arbitrarily large backlogs, regardless of how quiet the tenant is.
- **Attempted durability:** a failed insert is captured in a bounded retry buffer and flushed with backoff; the mutation is never blocked by event durability.
- Zero periodic access to cold dedicated tenant DBs: no TTL, no per-tenant partition maintenance, no cluster-wide cron sweeper over dedicated tenant DBs.
- Signal-plane health bounded: the 1-hour outbox cannot be pinned into unbounded growth by dead **or** stalled pods.

## Non-goals and residual loss modes

Exactly-once / 100% delivery is explicitly out of scope. The accepted residual loss modes are:

1. Process crash with a non-empty, unflushed retry buffer (quantified below).
2. Retry buffer full under sustained tenant-DB outage — oldest entries for that tenant dropped (counted per tenant, alerted).
3. Tenant DB unavailable longer than the buffer can hold that tenant's event stream (same as 2, viewed from the DB side).
4. Consumer offline beyond the retention window — not silent: detected and surfaced as `reset(seq_too_old)`, recovered by full listing.
5. Buffered entry dropped before its durable insert without overflow: either the entry aged past the retry max age (set from the fs_events retention, clamped to [1h, 24h] — a 168h retention is thus actually reachable by retried events, while memory stays bounded), or it was enqueued after the buffer stopped during shutdown, or its tenant ceased to exist before the flush landed. All are counted per tenant (`reason="expired"` / `reason="stopped"` / `reason="tenant_gone"`, plus `reason="shutdown"` for leftovers after the shutdown flush budget) and alerted like mode 2.

Other exclusions: replacing the cross-pod signal plane (outbox/poller/bus) stays as-is; no per-consumer acks, no webhook/MQ push channel — consumers that must survive week-long process outages need a separate channel, not this design; `chmod` remains a known silent op (no event today, `docs/guides/sse-notifications.md` documents the gap) and fixing it is not part of this design; a central event log (meta-DB table or Kafka) is rejected — see "Rejected alternatives".

## Loss budget math

Per-event loss fraction from the crash window (mode 1):

```
annual_lost_events ≈ crashes_per_year × mean_unflushed_events_per_crash
mean_unflushed_events_per_crash ≈ events_per_second_per_pod × mean_buffer_residency_seconds
loss_fraction ≈ annual_lost_events / annual_total_events
```

Worked example with deliberately pessimistic inputs: 20 pods × 12 crashes/pod/year = 240 crashes; 50 events/s/pod with 5 s mean buffer residency → 250 events lost per crash → 60k events lost/year. Fleet total at 1,000 events/s ≈ 3.16×10¹⁰ events/year. Loss fraction ≈ **2×10⁻⁶** — about 50× inside the 10⁻⁴ budget. The real numbers must be computed from measured crash rate, event rate, and buffer residency (all exported as metrics); the formula, not the example, is the contract. Modes 2–3 (buffer overflow) require a sustained tenant-DB outage and are governed by the `buffer_drop_oldest` SLI with paging alerts, not by this formula.

## Key insight

Event completeness constrains only the **lower bound** of retention (≥ 7 days). Over-retention has no correctness cost — a consumer whose cursor is older than 7 days simply gets a luckier replay. Therefore the retention sweep does not need to be a periodic task at all: it can run **lazily, on the write path**, because writes are the only thing that both (a) creates rows and (b) guarantees the DB is already awake.

This decouples the two planes:

- **Log (tenant DB, 7 days):** the source of truth for replay.
- **Signal (meta DB, 1 hour):** a wake-up hint, consumed within ~200 ms by the outbox poller and protected by the `MIN(tenant_outbox_cursor)` pruning gate. Its lifetime is deliberately short and fully independent of the log's retention. (The comment at `pkg/server/server.go:1180-1182` that couples the two retentions is wrong under this model and must be removed.)

## Architecture

```
mutation ──► fs_events INSERT (tenant DB, bounded timeout) ──┐ ok
             │                                               ▼
             │ failure                              retry ring buffer
             │ (never fails the mutation)           (per-pod, per-tenant
             ▼                                        caps, backoff flush)
        metrics + log                                    │
                                                         ▼ flushed
                                                   fs_events (tenant DB)
                                                   + insertTenantNotify (second wake)
                                                   + bus.Publish()

mutation ──► tenant_notify_outbox INSERT (meta DB, 1h retention, coalesced)
             └─► outbox poller (200ms, meta DB only) ──► EventBus.Publish()
                                                             │
connected SSE client ◄── replay: SELECT ... WHERE seq > cursor (paginated)
```

Who touches a tenant DB, and when:

| Tenant state | Tenant-DB traffic |
| --- | --- |
| Cold, no subscribers, no writes | **None.** The outbox poller reads only the meta DB. |
| Cold, SSE client connected, no writes | **None** by default. Heartbeats are in-memory; Phase 2 is notify-driven; the catch-up loop disarms once drained. (Optional liveness poll — item 4 — is off by default.) |
| Active writes | 1 `fs_events` insert per mutation + throttled lazy sweep (≤ 1/hour/tenant, async, batched). |
| Subscriber with backlog | Paginated replay reads until drained, then stops. |

## Changes

### 1. Configurable `fs_events` retention (P0)

- Replace `const fsEventsRetention = 1 * time.Hour` (`pkg/server/server.go:1178`) with a `Config` field wired to a new env var `DRIVE9_FS_EVENTS_RETENTION` (default `1h`; production sets `168h`).
- Both sweep paths must read the same Config value: the new lazy write-path sweep (primary) **and** the existing `piggybackMaintenance` sweep in the tenant worker (secondary backstop) — the worker currently references the package-level constant directly, so the value must be plumbed through `tenantWorkerOpts`. "Unchanged" is not an option.
- **Do not** change `DRIVE9_SSE_NOTIFY_RETENTION`; the outbox stays at 1h. Update the coupling comment at `pkg/server/server.go:1180-1182`.
- Log the effective retention at startup and add it to the production deploy checklist, so a missing env var never silently reverts to 1h.

### 2. Lazy retention sweep on the write path, batched (P0)

- In `publishEvent` (`pkg/server/sse.go:212`), after a successful insert, run the sweep throttled to at most once per hour per tenant, on a detached background context so it never adds mutation latency. Failures are metrics + warn logs only.
- `DeleteFSEventsBefore` (`pkg/datastore/fs_events.go:134`) is currently a single unbounded `DELETE`. Change it to **batched deletes** (`DELETE ... LIMIT n` loop, e.g. 5k rows per batch) with a per-sweep batch cap; leftover rows are deleted next hour. The cost of one unbounded statement is driven by table size, not rows matched — a 7-day hot tenant can hold millions of rows, and the first sweep after a long idle over-retention period (or after a retention decrease) would otherwise hit TiDB transaction size limits on the write path's own DB.
- **Rollback note:** raising retention (1h → 7d) needs no catch-up delete. Lowering it (7d → 1h) triggers a one-time large purge; the rollback runbook must say so and let the batched sweeper drain it over several cycles (or run the operator sweep manually).
- Active tenants: zero extra wakeups. Tenants that go cold stop sweeping and simply over-retain their last ≤ 7 days of rows (storage ∝ past activity; correctness unaffected).
- No TiDB `TTL`, no per-tenant partitioned tables, no cluster-level periodic sweeper over dedicated tenant DBs — all three are periodic per-tenant-DB access by another name.

### 3. Event insert retry buffer (P0)

- On `InsertFSEvent` failure (`pkg/server/sse.go:228-241` currently logs and drops), enqueue the event into a per-pod bounded in-memory ring (global cap, e.g. 10k entries) **with a per-tenant sub-cap** (e.g. 1k). One tenant's sustained DB outage must not fill the shared buffer and cause drop-oldest to discard healthy tenants' events: retry backoff is per-tenant, and the drop counter is labeled per tenant.
- **Per-tenant flush workers.** Flushing is sharded per tenant: one FIFO flusher per tenant with due entries, bounded by a global concurrency cap (8), so a tenant whose DB hangs for 30 s cannot delay other tenants' flushes. In-flight entries stay counted against the caps and the depth gauge (no cap evasion), are skipped by eviction (the oldest non-flushing entry is dropped instead — no age-order inversion), and keep their queue position on failure (the queue stays enqueuedAt-ordered by construction). Per-entry 30 s insert timeout and 1 s→5 min per-entry backoff are kept.
- **Store re-resolution.** The flush first tries the bus's cached store pointer; when it is nil (the pool idle-closed the backend) or the insert fails with a closed-DB error (`sql.ErrConnDone` / "database is closed"), the server's normal backend acquisition path (`GetTenant` + `pool.Acquire`, the same one the request path uses) re-resolves the store once and refreshes the bus pointer as a side effect — a tenant DB that recovers after an idle close still receives its buffered events without waiting for new tenant traffic. A tenant that no longer exists (or is no longer active) drops its entries with the distinct reason `tenant_gone` instead of spinning.
- **Second wake is mandatory.** A successful flush must run the same steps as a fresh publish: `insertTenantNotify(tenantID, WorkSSE)` (otherwise cross-pod subscribers sleep until the next write) **and** `bus.Publish()` (otherwise same-pod connections that already saw an empty poll believe they are caught up — today `publishEvent` publishes even when the insert fails, so local clients spin once and conclude "no new rows").
- **Ordering decision — bounded reorder, idempotent consumers.** `seq` is assigned at INSERT success, so a retried event is sequenced after events that persisted while it was buffered: consumers can observe B-then-A where A's mutation happened first. Consumers that apply events as an ordered change log (create → rename, mkdir → write) must instead treat events as idempotent hints and re-fetch state on ambiguity — the same contract FUSE cache invalidation already uses. Head-of-line blocking (pausing a tenant's subsequent event writes until a failed one flushes) is **rejected**: it couples mutation-path latency and availability to the event path, violating the prime directive. Reorder is rare (only around insert failures) and bounded by buffer residency; it is stated in the consumer contract.
- Metrics: buffer depth, per-tenant dropped-oldest total, flush success/failure. Alert on non-zero drops.

### 4. Replay completeness, liveness, and signal-loss stall (P0 + optional P1)

`EventsSince` returns at most 1000 rows per call (`pkg/server/eventbus.go:185`). Today Phase 1 sends one batch, and Phase 2 only re-queries on a notify signal — but the outbox poller advances its cursor even when no local bus exists (`pkg/server/tenant_outbox_poller.go:195`), so a quiet tenant with a > 1000-event backlog stalls after the first batch until the next write. With a 7-day window this is a guaranteed customer-facing stall.

- Phase 1 (`pkg/server/sse.go:368-412`): loop `EventsSince` until a call returns fewer than the limit (backlog drained), then send the initial heartbeat. During a long drain, **interleave a heartbeat (and flush) every N batches or T seconds** — LB and client idle timeouts will otherwise kill multi-minute replays, and the client has no liveness signal. Optionally cap a single connection's replay budget (max M events or T seconds); the cursor protocol makes early handoff safe — the client simply reconnects with its updated `since`.
- Phase 2: replace fixed-delay catch-up with an **event-driven drain**: when `pollAndSend` returns a full page, pull again immediately; when it returns a short page, disarm. To keep a huge burst from starving heartbeats and disconnect handling, one wake drains at most 10 consecutive full pages, then flushes and re-arms a short (~10 ms) catch-up timer that polls again from the select loop — only a short page disarms, so a single 200 ms coalescer window's >10k-event burst still drains completely with no further write. Consecutive query errors re-arm with bounded exponential backoff (10 ms doubling, capped at 1 s) so streams stuck on a failing tenant DB cannot hammer it at a fixed 10 ms cadence; the first successful poll resets the backoff, so a recovered DB resumes full-speed drain immediately. No standing poll, no fixed timer spinning against cross-region RTTs — quiet tenants still generate zero tenant-DB traffic.
- **Event loss vs signal loss (P1, optional).** The catch-up drain fixes backlog stalls but not signal loss: if the meta DB is down long enough that outbox rows are never written (or the poller stalls), a connected client is never woken and new `fs_events` rows sit undelivered until reconnect — events are durable but delivery stalls. Optional mitigation: a low-frequency liveness poll (e.g. 60 s, configurable, **default off**) that only polls tenants which have an active SSE connection **and** have not successfully polled within the interval. This touches only DBs of tenants with live customer connections — a much weaker claim than "wake cold DBs", but it is still a standing poll against otherwise-quiet DBs, so the trade-off is made explicit and configurable rather than silently accepted.

### 5. Payload for structural operations — split out

Rename/delete/mkdir/copy currently surface only as `reset(structural_change)` (`pkg/server/sse.go:515-525`). Giving them machine-readable detail (old/new path etc.) is **orthogonal to retention** and touches 4 schema sources plus `schema dump-init-sql` re-exports for every provider, the `publishEvent` signature, ~12 call sites, and a dual-emit client migration window. It is split into a separate design document and PR so it cannot hold up the P0 retention work; this document only requires that the wire format remains additive so old clients ignore unknown fields. Tracked separately (design TBD: `docs/design/sse-structural-event-payload.md`).

### 6. Outbox pruning gates: dead pods (done) and stalled pods (P1)

- **Dead pods — already implemented.** Since #660, `SweepStalePods` deletes the stale pod's `tenant_outbox_cursor` row (`pkg/server/pod_registry.go`), and `DeleteTenantNotifyBefore` prunes against `MIN(last_id)` across cursors. Remaining gap: `TestSSEOutboxStalePodSweep` asserts subscription cleanup but not cursor-row deletion — add that assertion. No other code change budgeted.
- **Alive-but-stalled pods — new.** The `MIN(cursor)` gate is defeated by a pod whose heartbeat is healthy but whose outbox poller is stuck: its cursor pins `MIN(last_id)` forever and the 1-hour outbox table grows unboundedly. Fix: give the gate an **age bound** — ignore cursors whose `updated_at` is older than a threshold (e.g. 10 min) when computing the prune floor; SSE notifications are lossy hints by design, so pruning past a stalled pod's cursor is safe (its connected clients recover via reconnect replay). Additionally alert on outbox oldest-row age (the poller already exports `oldestAge`) so a pinned floor is visible before it matters.

### 7. Estimate-based `fs_events` row counting (P1, ship with P0)

`CountFSEvents` is a full-table `COUNT(*)` per warm tenant per maintenance interval (`pkg/datastore/fs_events.go:113`). Do not wait for it to become expensive: switch to a bounded predicate count or an `information_schema.TABLES` estimate in the same release that enables 7-day retention, since hot-tenant tables jump 168× immediately.

### 8. Retire the legacy push endpoint (P2)

`/v1/internal/sse-notify` was superseded by the outbox poller and retained only for sub-10 ms cross-pod latency. **Resolved (implemented): removed entirely** — handler, request type, route registration, `PodNotifySecret` config, and `DRIVE9_POD_NOTIFY_SECRET` env — after a repo-wide search confirmed no active sender ever existed.

## Durability SLIs (what "99.99%" means operationally)

| SLI | Meaning | Target / alert |
| --- | --- | --- |
| `insert_success / insert_attempt` | Hot-path insert success ratio | High; dips feed retry buffer |
| `retry_flush_success` | Buffer salvage rate | ~100%; sustained failure = tenant DB outage |
| `buffer_drop_oldest` (per tenant; `reason` ∈ `tenant_cap`, `global_cap`, `expired`, `stopped`, `shutdown`, `tenant_gone`) | Hard loss (buffer overflow, entry aged past the retry max age = fs_events retention clamped to [1h, 24h], enqueue after shutdown stop, leftover after the shutdown flush budget, or tenant deleted before flush) | ≈ 0; page on non-zero |
| `drive9_sse_reset_sent_total{reason="seq_too_old"}` | Consumer pushed outside the window | ≈ 0; the only consumer-visible loss signal |
| outbox oldest-row age (from poller `oldestAge`) | Signal-plane pruning health | < 2× retention; catch stalled-pod pinning |
| crashes/year, events/s, buffer residency | Inputs to the loss-budget formula | Recompute budget on material change |

The target is "drops ≈ 0, transient insert failures recovered by the buffer, crash window quantified by the loss-budget formula" — auditable from metrics, not a bare percentage.

## Rejected alternatives

- **Central partitioned log in the meta DB.** One append-only table with daily `DROP PARTITION` retention is simpler in the abstract, but it puts the meta DB on the write path of every mutation (availability coupling: meta-DB outage kills the event stream fleet-wide), violates tenant-DB residency, and forfeits the property that event durability rides the same DB that just committed the mutation.
- **TiDB table `TTL`.** TTL scans are server-side periodic jobs that wake every tenant DB on schedule — exactly the billing pattern we must avoid.
- **Per-tenant partitioned tables.** `DROP PARTITION` still requires a per-tenant DDL alarm clock, plus per-tenant-day schema churn.
- **Cluster-level periodic sweeper over dedicated tenant DBs.** Wakes cold tenants by design; superseded by lazy sweeping.
- **Co-transactional event insert, or fail-the-mutation on insert error.** Buys the last fraction of a percent of durability at the cost of coupling mutation availability to the event path. The retry buffer reaches the SLI targets without either.
- **Head-of-line-ordered flush.** Pausing a tenant's event writes until a failed insert flushes restores global order but couples mutation-path latency to event-DB health. Bounded reorder plus idempotent consumers is the chosen trade.
- **Kafka / external log infra.** Massive operational step-up for one event stream; the relational log is sufficient at filesystem event rates.

## Deployment and observability

- Config: set `DRIVE9_FS_EVENTS_RETENTION=168h`. Leave `DRIVE9_SSE_NOTIFY_RETENTION` at its 1h default. Startup logs the effective retention; production checklist verifies it.
- Pre-production checklist: run the cursor-upsert freshness test (`TestUpsertTenantOutboxCursorRefreshesUpdatedAt`, pkg/meta) against real TiDB before enabling the freshness-bound prune floor — the `ON DUPLICATE KEY UPDATE` assignment-order semantics it depends on are covered on MySQL 8.0 only.
- Rollback: 1h → 7d needs no catch-up delete; 7d → 1h triggers a one-time large purge drained by the batched sweeper over multiple cycles (or run the manual operator sweep).
- Alerts: the SLIs above, plus per-tenant `drive9_fs_events_rows` for capacity tracking.
- Capacity: size tenant DB storage at peak write rate × 168 h per tenant.

## Consumer contract

- Persist the `seq` cursor durably; reconnect with `?since=<seq>`.
- `seq` is a **persistence order**, not a causal or wall-clock order: retried inserts are sequenced at flush time, so consumers can observe bounded reorder around insert failures. Treat events as **idempotent hints** and re-fetch state on ambiguity; use `ts` only for loose ordering. `seq` also contains holes (AUTO_INCREMENT ids burned by rolled-back inserts); cursors compare only against the oldest retained seq.
- There are **no per-consumer acks**. Offline beyond the retention window produces `reset(seq_too_old)`; the official recovery path is a full listing followed by a fresh stream. `reset` reasons `server_restart` / `initial_sync` carry the same fallback.
- Structural events will gain an additive `payload` field (separate design); ignore unknown fields. During its dual-emit window, structural ops appear both as detailed `file_changed` events and as `reset(structural_change)`.
- This design does not provide a webhook/MQ push channel; customers that cannot hold a reconnecting SSE client need a separate mechanism.

## Rollout

1. **P0:** items 1–4 (config plumbed to both sweep paths + startup log; batched lazy sweep; retry buffer with per-tenant caps and second wake; replay pagination with interleaved heartbeats + event-driven catch-up) and item 7 (estimate-based counting). No schema change; ship and enable 7-day retention.
2. **P1:** item 6 (cursor-deletion test assertion, gate age bound, oldest-age alert), item 4's optional liveness poll if the signal-loss stall is deemed worth it.
3. **P2:** item 8 + doc updates (`docs/guides/sse-notifications.md`: retention semantics, seq ordering caveat, cursor contract).
4. **Separate track:** structural-event payload (item 5) in its own design and PR.
