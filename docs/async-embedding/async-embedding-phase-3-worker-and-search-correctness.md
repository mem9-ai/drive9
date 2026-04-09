# Proposal: drive9-2 Async Embedding Phase 3 Worker and Search Correctness

**Date**: 2026-03-31
**Purpose**: Turn Phase C of `async-embedding-generation-proposal.md` into a review-ready proposal for embedding-provider wiring, server-owned worker execution, and search-path correctness.

## Summary

Phase 3 should be the first phase that makes async embedding behavior live end to end. Phase 1 established mutable embedding state and durable semantic task contracts. Phase 2 made task registration part of committed file writes. Phase 3 completes the loop: the server starts background embed workers, workers generate vectors and write them back conditionally, and grep/search stops depending on raw-text vector SQL by embedding queries in the application layer.

This phase keeps the existing request semantics intact. File writes still succeed at the same boundaries as before, and semantic indexing remains eventually consistent. The new guarantee is narrower and more important: once a revision has valid `content_text` and a durable `embed` task, the system has a concrete execution path to produce a current-revision vector, and search reads only vectors whose `embedding_revision` still matches `files.revision`.

## Context

### Current State

The repository already contains the Phase 1 and Phase 2 seams that Phase 3 depends on:

- tenant schemas now define mutable `files.embedding`, `files.embedding_revision`, and tenant-local `semantic_tasks`
- `pkg/datastore/semantic_tasks.go` already provides `Enqueue`, `Claim`, `Ack`, `Retry`, `RecoverExpired`, and same-revision requeue helpers
- `pkg/datastore/embedding_writeback.go` already provides revision-gated `UpdateFileEmbedding(ctx, fileID, revision, vector)`
- `pkg/backend` write paths already clear stale embedding state and register `embed` work during committed revision changes
- async image text writeback already bridges into same-revision embed work instead of relying on generated embedding

The verified Phase 3 code boundaries are already visible in the repository:

- `pkg/embedding/client.go` defines the embedding client contract
- `pkg/embedding/openai.go` provides an OpenAI-compatible `/v1/embeddings` client
- `pkg/server/server.go` can start a server-owned `SemanticWorkerManager` through `Config.SemanticEmbedder` and `Config.SemanticWorkers`
- `pkg/server/semantic_worker.go` claims tasks, filters obsolete work, retries failures, runs recover sweeps, and conditionally writes vectors back through `UpdateFileEmbedding`
- `pkg/backend/drive9.go` already embeds grep queries in the application layer and merges FTS and vector results with the existing RRF logic
- `pkg/datastore/search.go` already exposes `VectorSearch(ctx, queryEmbedding, pathPrefix, limit)` and filters on `f.embedding_revision = f.revision`

The test baseline also shows that the intended behavior is concrete rather than aspirational:

- `pkg/server/semantic_worker_test.go` covers happy path processing, stale revision obsolescence, retry/dead-letter behavior, and recover-after-claim
- `pkg/backend/grep_test.go` covers fallback behavior when query embedding is disabled or unavailable

At the same time, two verified boundaries remain explicit in the codebase:

- `pkg/server/semantic_worker.go` still opens multi-tenant stores through the current MySQL-oriented `tenantDSN(...)` and `datastore.Open(...)` path
- `pkg/datastore/schema_test.go` explicitly documents that db9/Postgres runtime behavior is not yet exercised end to end by the current test harness

### Problem Statement

After Phase 2, the system can durably register semantic work, but that alone is not enough to make async embedding correct or useful. Without Phase 3:

1. queued `embed` tasks remain durable records with no server-owned execution loop
2. vector writeback has no active provider-backed path from `content_text` to `files.embedding`
3. grep still needs an application-owned query embedding path to avoid falling back to nonexistent database-side text embedding
4. stale vectors must be filtered consistently at query time, not only cleared at write time

In other words, Phase 2 guarantees that semantic work is recorded, but Phase 3 is the phase that guarantees semantic work can be executed safely and consumed correctly.

### Constraints and Decision Drivers

- File write semantics must remain unchanged: small-file writes stay synchronous, multipart upload keeps the current `202` completion flow.
- The worker must remain revision-gated and idempotent because semantic task delivery is at-least-once.
- Background execution must be owned by the server process, not by backend cache lifetime inside `tenant.Pool`.
- Search correctness depends on two checks together: query embedding must happen in the app layer, and vector SQL must filter out stale document vectors.
- The first production-safe version should extend existing seams (`pkg/embedding`, `pkg/server`, `pkg/backend`, `pkg/datastore`) rather than introducing a new orchestration abstraction.
- The current multi-tenant runtime is still MySQL-oriented; the proposal must acknowledge that boundary instead of silently claiming provider-neutral runtime support that the code does not yet implement.

## Goals

1. Server startup can enable a durable semantic worker loop that consumes `embed` tasks independently of backend cache residency.
2. Workers generate embeddings from current `content_text`, write them back only for the matching confirmed revision, and safely retry or dead-letter failures.
3. Grep/search computes query embeddings in the application layer and never depends on database-side raw-text vector functions.
4. Vector search returns only current-revision vectors by filtering on `embedding_revision = revision`.
5. If query embedding is unavailable at request time, grep degrades to the existing FTS/keyword behavior instead of failing the whole API.

## Non-Goals

- Do not redesign the durable task contract introduced in Phase 1.
- Do not migrate image extraction itself into a durable `extract_text` task in this phase.
- Do not add external task-inspection APIs, admin UIs, or manual replay tooling.
- Do not solve provider-neutral multi-tenant store opening for db9/Postgres in this phase.
- Do not make Phase 3 depend on the Phase D metrics/logging expansion before enabling worker correctness.

## Architecture Overview

```text
Server Startup
--------------
cmd/drive9-server
    -> build query embedder / semantic embedder from env
    -> server.NewWithConfig(...)
    -> start SemanticWorkerManager when semantic embedder is configured

Background Embed Path
---------------------
SemanticWorkerManager
    -> choose tenant store (local fallback or active tenant round-robin)
    -> ClaimSemanticTask(..., taskTypes...)
    -> GetFile(resource_id)
    -> if missing / stale / empty: ack obsolete task
    -> embed file.ContentText
    -> UpdateFileEmbedding(file_id, revision, vector)
    -> ack success or retry failure

Query Path
----------
backend.Grep
    -> run FTS immediately
    -> embed query text in app layer
    -> datastore.VectorSearch(queryVector)
    -> filter to embedding_revision = revision
    -> merge with RRF
    -> fallback to keyword search when semantic ranking is unavailable
```

## Design

### 1) Scope cut and ownership

Phase 3 should keep the ownership split explicit:

- `pkg/embedding` owns the provider-facing client contract and vector formatting helpers
- `cmd/drive9-server/main.go` owns runtime wiring from environment variables into query embedding and background embedding clients
- `pkg/server` owns semantic task execution lifetime, tenant scheduling, and recover sweeps
- `pkg/backend` owns request-path grep orchestration and fallback behavior
- `pkg/datastore` owns vector search SQL and revision-gated embedding writeback

That split keeps the design minimal. The backend does not become a worker host, and the worker does not gain responsibility for search orchestration.

### 2) Embedding client contract and configuration

Phase 3 should standardize on one minimal embedding contract:

```go
type Client interface {
    EmbedText(ctx context.Context, text string) ([]float32, error)
}
```

Verified design points from the current codebase:

- `embedding.NopClient` returns `(nil, nil)` so callers can keep simple fallback logic
- `embedding.OpenAIClient` targets an OpenAI-compatible embeddings endpoint and supports `BaseURL`, `APIKey`, `Model`, optional `Dimensions`, and timeout control
- `cmd/drive9-server/main.go` already allows query embedding and background embedding to be configured separately through env vars
- when a semantic worker embedder is configured but a dedicated query embedder is not, server startup already reuses the semantic embedder for query embedding

Phase 3 should keep that split as an explicit configuration policy rather than prematurely collapsing the two roles into one path. Query embedding and background document embedding live on different execution paths and may later need different models, timeouts, or rate limits. Keeping two config surfaces while allowing default reuse is the simplest way to preserve that flexibility without complicating the current rollout.

This is the right Phase 3 scope boundary. The client contract stays intentionally small, and provider selection remains a server wiring concern rather than leaking into task rows or search SQL.

### 3) Server-owned worker lifecycle

Embedding work must be server-owned because task durability would be meaningless if execution stopped whenever a backend instance was evicted or recycled.

`pkg/server/server.go` should therefore host `SemanticWorkerManager` with these rules:

- do not start the manager when no semantic embedder is configured
- in local/single-tenant mode, run workers directly against the fallback backend store
- in multi-tenant mode, list active tenants from `meta.Store` and poll them in round-robin order
- cap work globally by worker count and locally by per-tenant concurrency
- run a separate recovery loop that periodically calls `RecoverExpiredSemanticTasks`

This gives Phase 3 two correctness properties that backend-owned goroutines cannot provide:

1. worker lifetime is tied to server lifetime, not to backend cache residency
2. lease recovery continues to function after crash/restart or interrupted claims

### 4) Task execution and conditional writeback

The worker execution model should stay deterministic and revision-gated:

1. claim one `embed` task with receipt + lease
2. load the target file by `resource_id`
3. if the file is missing, not `CONFIRMED`, revision-mismatched, or has empty `content_text`, treat the task as obsolete and ack it
4. generate an embedding from the current `content_text`
5. call `UpdateFileEmbedding(fileID, revision, vector)`
6. if the conditional update affects zero rows, treat the task as obsolete and ack it
7. ack only after successful writeback or confirmed obsolescence
8. on provider/runtime failures, retry with backoff until `max_attempts`, then dead-letter

The critical invariant is that writeback is never unconditional. `files.embedding` is valid only when `files.embedding_revision = files.revision`, so the worker must always write both fields together and only for the current confirmed revision.

`pkg/server/semantic_worker.go` already reflects this invariant through:

- `GetFile(...)` before embed execution
- stale/empty checks before provider calls
- `UpdateFileEmbedding(...)` for revision-gated SQL writeback
- `RetrySemanticTask(...)` with exponential backoff
- `RecoverExpiredSemanticTasks(...)` in the background sweep

### 5) Search-path correctness and graceful degradation

Phase 3 must fix both sides of semantic search correctness: who produces the query vector, and which stored vectors are eligible to match.

`pkg/backend/drive9.go` should keep the current parallel search structure:

- FTS starts immediately
- the vector path first calls `queryEmbedder.EmbedText(ctx, query)`
- if query embedding succeeds and returns a non-empty vector, call `store.VectorSearch(ctx, queryVec, pathPrefix, fetch)`
- merge ranked FTS and vector results through the existing RRF logic

`pkg/datastore/search.go` should keep the vector SQL boundary narrow:

- accept a precomputed query vector rather than raw query text
- require `f.status = 'CONFIRMED'`
- require `f.embedding IS NOT NULL`
- require `f.embedding_revision = f.revision`

This is what prevents stale vectors from leaking back into results after overwrite. Write-path clearing is necessary, but it is not sufficient on its own; the read path must also refuse historical vectors explicitly.

Fallback behavior is also part of correctness here. If the embedding provider is disabled, returns an error, or vector search itself fails, grep should still serve the request through FTS and keyword fallback. Semantic ranking is an enhancement over the base text path, not a new failure mode for search.

### 6) Explicit boundary: current provider-neutrality gap

The current codebase already documents one real boundary that this proposal should preserve explicitly: multi-tenant worker runtime is not yet provider-neutral.

Today, `pkg/server/semantic_worker.go` opens tenant stores through `tenantDSN(...)` and `datastore.Open(...)`, which follow the current MySQL-oriented runtime path. That means:

- schema support for db9/Postgres exists at the DDL level
- local fallback and current MySQL/TiDB runtime behavior are covered by tests
- full multi-tenant runtime parity for db9/Postgres is still deferred

Phase 3 should therefore define its support boundary honestly: worker and search correctness land on the existing runtime path first, while provider-neutral multi-tenant execution remains follow-up work rather than implicit scope creep inside this phase.

## Compatibility and Invariants

- File write success semantics do not change.
- Semantic tasks remain at-least-once; workers must stay idempotent.
- `files.revision` remains the only trusted content version marker.
- `files.embedding` is considered queryable only when `embedding_revision = revision`.
- Query embedding happens in the application layer, not inside vector SQL.
- Search degrades to FTS/keyword behavior when semantic ranking is unavailable.
- Tenant-local task execution remains isolated per tenant database; Phase 3 does not introduce a shared cross-tenant queue.

## Incremental Plan

### Step 1: Provider and config wiring

1. keep `pkg/embedding.Client` as the shared contract for background and query embedding
2. wire `DRIVE9_QUERY_EMBED_*` and `DRIVE9_EMBED_*` configuration into `cmd/drive9-server/main.go`
3. reuse the semantic embedder for query embedding when dedicated query config is absent

### Step 2: Worker activation

4. start `SemanticWorkerManager` from `server.NewWithConfig`
5. keep local fallback mode and active-tenant round-robin scheduling
6. enable recover sweeps and retry backoff using the existing datastore task contract

### Step 3: Search correctness

7. keep grep's concurrent FTS + vector structure in `pkg/backend/drive9.go`
8. require the vector branch to consume an app-generated query vector
9. filter vector SQL on `embedding_revision = revision`
10. preserve keyword fallback when semantic ranking is unavailable

### Step 4: Validation and scope pinning

11. extend worker tests around obsolete tasks, retries, recover sweeps, and dead-letter behavior
12. extend grep tests around disabled/unavailable embedding and vector failure fallback
13. record the db9/Postgres multi-tenant runtime gap as explicit deferred scope

## Validation Strategy

- **Worker execution tests**
  - claimed embed task writes `embedding_revision` for the matching file revision
  - stale tasks are acked as obsolete without writing vectors
  - provider failure retries and eventually dead-letters while preserving `last_error`
  - expired claims become claimable again after recovery

- **Search tests**
  - grep uses an app-generated query vector instead of raw query text in SQL
  - vector search reads only rows where `embedding_revision = revision`
  - disabled or failing query embedding falls back without breaking grep

- **End-to-end smoke**
  - write file -> task queued -> worker writes vector -> grep can rank the file semantically
  - overwrite file before worker completion -> stale task becomes obsolete and only the latest revision remains queryable

- **Deferred validation note**
  - add a future db9/Postgres-backed runtime smoke once worker store opening becomes provider-neutral

## Risks and Mitigations

1. **Worker lifetime may accidentally regress back to backend lifetime** - Keep worker startup and shutdown owned only by `pkg/server`, not by `backend.Options` or tenant-pool cache entries.
2. **Late tasks may still overwrite newer content** - Keep `GetFile(...)` prechecks and `UpdateFileEmbedding(...)` conditional writeback mandatory for every task execution.
3. **Search may silently mix current and stale vectors** - Filter vector SQL with `embedding_revision = revision` and keep write-path clearing from Phase 2 in place.
4. **Embedding provider outages may make grep flaky** - Treat semantic ranking as degradable and preserve FTS/keyword fallback as the request-level safety net.
5. **Provider-neutral support may be overstated** - Document the current MySQL-oriented multi-tenant runtime boundary explicitly and defer db9/Postgres runtime parity to follow-up work.

## Decisions

1. Keep query embedding and background document embedding separately configurable in Phase 3, but default query embedding to reuse the semantic embedder when no dedicated query config is provided. This keeps rollout simple while preserving the ability to diverge model and runtime policy later.
2. Defer the minimum production metrics/logging set for dead-letter triage and queue-lag diagnosis to Phase D of `async-embedding-generation-proposal.md`, so worker correctness can land without prematurely fixing the observability surface here.

## Open Question

1. What is the smallest provider-neutral store-opening seam for multi-tenant workers that can support both TiDB/MySQL and db9/Postgres without duplicating worker logic?

## References

- `drive9-2/docs/async-embedding/async-embedding-generation-proposal.md`
- `drive9-2/docs/async-embedding/async-embedding-phase-1-foundation.md`
- `drive9-2/docs/async-embedding/async-embedding-phase-2-write-path.md`
- `drive9-2/cmd/drive9-server/main.go`
- `drive9-2/pkg/embedding/client.go`
- `drive9-2/pkg/embedding/openai.go`
- `drive9-2/pkg/server/server.go`
- `drive9-2/pkg/server/semantic_worker.go`
- `drive9-2/pkg/server/semantic_worker_test.go`
- `drive9-2/pkg/backend/drive9.go`
- `drive9-2/pkg/backend/grep_test.go`
- `drive9-2/pkg/datastore/search.go`
- `drive9-2/pkg/datastore/embedding_writeback.go`
- `drive9-2/pkg/datastore/schema_test.go`
