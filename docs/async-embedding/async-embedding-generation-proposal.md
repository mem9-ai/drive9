# Proposal: dat9-2 Async Embedding Generation

**Date**: 2026-03-30
**Purpose**: Replace the current incorrect dependency on `EMBED_TEXT()` with a minimal, recoverable, testable async task pipeline, and make vector search in `dat9-2` stand on real executable code paths.

## Summary

`dat9-2` should move file embedding generation from database-generated columns into the application layer, and introduce a tenant-local durable task pipeline. This pipeline should not directly carry over the QueueFS filesystem interface from AGFS. Instead, it should reuse QueueFS's well-tested claim / ack / recover semantics and implement them on top of dat9's own `semantic_tasks` table and worker manager.

Phase 1 focuses on durable `embed` tasks. After a file write succeeds, the request should still return immediately. In the same database transaction, the server should clear the old embedding, record the target revision, and enqueue an async task. Workers claim tasks through leases, call the embedding API, and write back the result only if `files.revision` still equals the task's bound `resource_version`. The search path should also switch to generating query embeddings in the application layer, then performing vector-to-vector retrieval.

Compared with 2026-03-29, the latest code already includes an in-memory queue worker used only for image caption / OCR. Because of that, this proposal also needs one explicit bridge rule: any async `content_text` writeback (today mainly image extraction) must explicitly trigger the corresponding `embed` work after it successfully updates the target revision, instead of continuing to rely implicitly on a generated column to produce vectors automatically.

This design keeps the current synchronous storage semantics of `PUT /v1/fs` and multipart upload, and it also preserves the existing inode / hard-link / revision constraints. The only change is how semantic indexing is produced.

## Context

### Current State

The current small-file write path in `pkg/backend/dat9.go` synchronously writes content, extracts `content_text`, and persists metadata. `createAndWriteCtx` writes `content_text` directly when inserting into `files`, and `overwriteFileCtx` recomputes `content_text` synchronously on overwrite and calls `UpdateFileContent`. If the file is an image, both paths also call `enqueueImageExtract` after the write completes, so a background worker can asynchronously patch in caption / OCR text. Today, neither create nor overwrite places "file write + follow-up semantic work registration" in the same transaction.

The large-file path returns `202` through `PUT /v1/fs/{path}`, then `pkg/backend/upload.go` completes the multipart upload in `ConfirmUpload` and overwrites the existing inode in place. In that path, the overwrite transaction sets `content_text` to `NULL`, and after the transaction commits, if the target file looks like an image, it also calls `enqueueImageExtractForUpload` to patch `content_text` asynchronously. In other words, upload completion already combines "in-place overwrite inside the transaction + best-effort semantic enrichment after the transaction."

The current search path concurrently executes FTS and vector search in `pkg/datastore/search.go`. `vectorSearch` requires `f.embedding IS NOT NULL` and directly calls `VEC_EMBED_COSINE_DISTANCE(f.embedding, ?)`, passing the raw query string as the second parameter.

The tenant schema still assumes the database can generate text embeddings automatically. `pkg/tenant/schema_zero.go`, `pkg/tenant/schema_starter.go`, and `pkg/tenant/schema_db9.go` all define `files.embedding` as `GENERATED ALWAYS AS (EMBED_TEXT(...))`. But `dat9` origin issue #30 has already confirmed that online db9 instances do not provide `EMBED_TEXT(text) -> vector`, `CHUNK_TEXT(text)`, or `vec_embed_cosine_distance(vector, text)`.

`dat9-2` already has a content-semantic worker, but it only covers image -> text enrichment. `pkg/backend/options.go` starts `imageExtractQueue` / a goroutine when constructing the backend, `pkg/backend/image_extract.go` performs revision-gated async writeback through `UpdateFileSearchText(ctx, fileID, revision, text)`, and `pkg/backend/image_extract_test.go` covers the case where a stale revision must not write back new content.

However, this existing path is still only a P0 best-effort mechanism, not a durable pipeline. The queue is an in-memory `chan`; when full, tasks are dropped directly. There is no `claim/ack/recover`, no retry/backoff/dead-letter, and the worker lifetime is tied to the `Dat9Backend` instance. In multi-tenant mode, LRU eviction in `pkg/tenant/pool.go` calls `backend.Close()`, which stops the image extract goroutine for that tenant. Compared with the proposal target, the current implementation is better understood as "a prototype that has already validated revision-gated writeback constraints," not as a reusable task system.

Today, the only server-owned background logic in `pkg/server/server.go` is still provisioning resume. Semantic workers are not yet hosted independently of backend / pool lifecycle.

On the other hand, `agfs-worktree/agfs-server/pkg/plugins/queuefs` already implements the core semantics of a durable queue and has fairly complete contract tests across memory / SQLite / PostgreSQL / TiDB. `Claim` uses leases, `Ack` requires receipts, `RecoverExpired` handles recovery after crash/restart, invalid receipts are rejected, and recovered ordering remains stable. Those semantics are already mature. But QueueFS itself is a queue-only substrate: it does not include task dedupe, task history, result/error storage, and it exposes filesystem control surface that dat9 does not need.

### Problem Statement

The current design bases search correctness on two capabilities that do not exist:

1. The database automatically generates `files.embedding` when `content_text` is written
2. The database can directly include raw query text in vector distance computation at query time

Both assumptions have already been ruled out by issue #30, so the current schema and `vectorSearch` code path cannot serve as the foundation for the future implementation.

At the same time, the `dat9-2` write model has several facts that cannot be broken:

- overwrite is an in-place inode update, not copy-on-write
- zero-copy copy shares the same `file_id`
- every content mutation increments `files.revision`
- large-file upload completion is also an in-place overwrite on the same inode

Once semantic writeback becomes async, the system must explicitly handle the case where an old task arrives late and overwrites newer content. Otherwise search will see stale embeddings.

In addition, the latest code already has one async `content_text` writeback source: the image extract worker. As soon as `embedding` is no longer `GENERATED ALWAYS AS (...)`, any such async `content_text` update must explicitly trigger embedding generation for the same revision. Otherwise image files will silently regress into "full-text search updates, but vector search no longer updates."

### Constraints and Decision Drivers

- Write API behavior must stay unchanged: small files still return `200` synchronously, and large files still use the `202` multipart flow.
- File persistence and task enqueue must commit in the same transaction, to avoid a gap where "the file exists but no task was created" or "the task exists but the file revision does not."
- Hard-link semantics must remain intact. Paths that share the same `file_id` refer to the same content entity, so embeddings must also be bound to `file_id + revision`, not path.
- Delivery semantics may be at-least-once; exactly-once is not required. The embedding handler must therefore be idempotent.
- Multi-tenant workers cannot depend on whether a `tenant.Pool` cache entry happens to stay resident, or background processing may be accidentally paused by LRU eviction.
- Existing `AsyncImageExtract` must not silently degrade after the migration. It is not durable today, but it does already update `content_text` asynchronously. Once embedding moves to the app layer, vector searchability for those files must be preserved.
- QueueFS provides high test value, but dat9 does not need to expose queues as filesystem APIs like `/queue/<name>/ack`.

## Goals

1. Newly written or overwritten small text files can generate embeddings in the background without blocking `PUT /v1/fs` or upload completion.
2. overwrite, small-to-large transitions, delete, and similar operations never let the search path see stale embeddings.
3. Unfinished tasks can continue through lease recovery after worker crash or process restart.
4. Vector search no longer depends on nonexistent database functions, and instead stands on real app-side query embedding + stored vectors.
5. Phase 1 only implements `embed`, but the underlying queue contract can naturally extend to `extract_text`, `generate_l0`, and `generate_l1`.

## Non-Goals

- Do not vendor the full QueueFS plugin directly into `dat9-2` in Phase 1
- Do not implement L0/L1 generation for `.abstract.md` / `.overview.md` in Phase 1
- Do not add external task query / task watch HTTP APIs
- Do not require Phase 1 to immediately migrate the existing image -> text extraction into durable `extract_text` tasks; however, after it successfully writes back `content_text`, it must be able to trigger the corresponding revision's `embed`
- Do not pursue exactly-once delivery or a distributed worker orchestration platform

## Architecture Overview

```text
Write Path
---------
HTTP PUT / upload complete
    -> Dat9Backend transaction
    -> update files row
    -> clear stale embedding state
    -> enqueue semantic_tasks(embed, file_id, revision)
    -> return 200/202 semantics unchanged

Background Path
---------------
SemanticWorkerManager
    -> list active tenants
    -> claim task with lease
    -> load current file row
    -> if revision mismatched or content_text empty: ack as obsolete
    -> call embedding API
    -> conditional UPDATE files ... WHERE file_id=? AND revision=?
    -> ack task

Query Path
----------
grep/search request
    -> app-side query embedding
    -> vector search only on rows where embedding_revision = revision
    -> merge with FTS using existing RRF logic
```

## Architecture Blueprint

### 1) Durable task substrate: reuse QueueFS semantics, not QueueFS surface

Inside `dat9-2`, add a `semantic_tasks` table and corresponding `pkg/queue` / `pkg/semantic` code, rather than moving in the whole QueueFS filesystem plugin.

Semantics to preserve and borrow directly:

- `Claim` returns a lease-bound task and receipt
- `Ack` must carry the current receipt; an old worker cannot confirm an old claim
- `RecoverExpired` returns lease-expired tasks to the claimable state
- `FOR UPDATE SKIP LOCKED` is the preferred concurrency pattern for claim
- Invalid receipts must fail rather than silently succeed

Parts not to inherit:

- queue registry and per-queue table mechanisms
- filesystem control surfaces such as `/enqueue`, `/dequeue`, `/ack`, `/recover`
- payload structures meant only for generic message queues

What dat9 needs is a task-aware queue, not a queue-only substrate, so task metadata must live in the same table.

The suggested minimal schema is:

```sql
CREATE TABLE semantic_tasks (
    task_id           VARCHAR(64) PRIMARY KEY,
    task_type         VARCHAR(32) NOT NULL,         -- first phase: 'embed'
    resource_id       VARCHAR(64) NOT NULL,         -- files.file_id
    resource_version  BIGINT NOT NULL,              -- files.revision when enqueued
    status            VARCHAR(20) NOT NULL,         -- queued|processing|succeeded|failed|dead_lettered
    attempt_count     INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 5,
    receipt           VARCHAR(128) NULL,
    leased_at         DATETIME(3) NULL,
    lease_until       DATETIME(3) NULL,
    available_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    payload_json      JSON NULL,
    last_error        TEXT NULL,
    created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    completed_at      DATETIME(3) NULL,
    UNIQUE KEY uk_task_resource_version (task_type, resource_id, resource_version),
    KEY idx_task_claim (status, available_at, lease_until, created_at)
);
```

`payload_json` is only for debugging and lightweight hints such as path, `content_type`, and `size_bytes`. Worker correctness must not depend on payload content. Instead, it should read the real current state back from storage using `resource_id + resource_version`.

### 2) File schema: embedding must become mutable and revision-aware

The current design that defines `embedding` as a generated column must end. Phase 1 needs to make embedding application-managed state rather than DB-computed state.

This proposal also makes the rollout boundary explicit: Phase 1 treats this schema change as a breaking change and only guarantees correctness for the new schema. Migration of existing tenants is out of scope for the current plan.

To prevent stale vectors from being returned by search, add revision-aware embedding state to `files`:

```sql
ALTER TABLE files
    ADD COLUMN embedding VECTOR(1024) NULL,
    ADD COLUMN embedding_revision BIGINT NULL;

CREATE INDEX idx_files_cosine ON files ((VEC_COSINE_DISTANCE(embedding)));
```

Because the current repository has no tenant data-plane migration runner and no rollout mechanism designed for old-tenant schema compatibility, Phase 1 does not handle migration from the old generated-column schema for existing tenants. In other words, this phase only guarantees the new schema shape is correct. Tenants that were already created under the old schema are outside the compatibility scope of this proposal.

`embedding_revision` is the key to this design. Only when `embedding_revision = files.revision` does the vector represent the current content. Otherwise it must be treated as stale or absent.

Put differently, `embedding_revision` is the version stamp of `files.embedding`. Because overwrite and upload completion both increment `revision` in place on the same `file_id`, an async worker may easily write back a vector produced from old content after arriving late. Without this column, the system cannot distinguish "the valid vector for the current revision" from "a stale vector left over from a historical revision," and the query path cannot safely filter out stale results.

### 3) Write path changes: clear first, enqueue in the same transaction

Small-file create / overwrite in `pkg/backend/dat9.go` do not currently use `Store.InTx`. To bind file persistence and task enqueue together, the following paths must be refactored into transactional versions:

- `createAndWriteCtx`
- `overwriteFileCtx`
- the large-file overwrite / completion branch inside `ConfirmUpload`

Phase 1 rules:

1. When content is written as small text content:
   - synchronously update `content_text`
   - synchronously set `embedding = NULL` and `embedding_revision = NULL`
   - in the same transaction, `INSERT ... ON CONFLICT/IGNORE` an `embed` task bound to the new `revision`

2. When the content is an image:
   - keep the current image extract entry points (`enqueueImageExtract` / `enqueueImageExtractForUpload`) so caption / OCR does not regress first
   - but after image extract successfully calls `UpdateFileSearchText(..., revision, text)`, it must additionally `enqueue/ensure` an `(task_type='embed', resource_id=file_id, resource_version=revision)` task
   - this ensures that even if `extract_text` itself is not yet durable in Phase 1, image files still do not lose vector updates when generated embedding is removed

3. When a file is overwritten with binary content or a large S3 object, and there is no available `content_text` producer in the current phase:
   - synchronously set `content_text = NULL` (or keep the current rule)
   - synchronously set `embedding = NULL` and `embedding_revision = NULL`
   - do not enqueue `embed` unless `extract_text` is implemented later

4. When performing zero-copy copy or rename:
   - do not create new tasks
   - because they do not produce a new `file_id + revision`

5. When a file is deleted:
   - existing tasks do not need to be synchronously canceled
   - after claim, if the worker finds the file deleted or its revision changed, it simply treats the task as obsolete and acks it

This strategy is consistent with the `resource_id + resource_version` constraint in issue #30 and with current inode semantics.

### 4) Worker lifecycle: server-owned manager, not pool-owned goroutines

The existing image extract worker is already attached to backend lifecycle. That is acceptable for P0 best-effort OCR, but embedding must not repeat this pattern. `Pool` is for backend reuse, not for long-lived background scheduling. A tenant evicted by LRU must not therefore stop processing queued tasks forever.

Add a `SemanticWorkerManager` in `pkg/server`, managed by `server.NewWithConfig` when the process starts:

- in single-tenant / local mode where `cfg.Backend != nil`: run a fixed number of workers directly against the fallback backend
- in multi-tenant mode where `cfg.Meta != nil && cfg.Pool != nil`: periodically call `meta.ListTenantsByStatus(active, limit)` and poll tenants in round-robin order
- allow only very small concurrency per tenant in Phase 1 (for example 1-2 embed workers), with a global concurrency cap
- run a separate recovery sweep goroutine that periodically calls `RecoverExpired`

Worker execution flow for a single task:

1. claim one `embed` task and get `task_id + receipt + resource_id + resource_version`
2. read the current `files` row
3. if the file does not exist, is not `CONFIRMED`, `revision != resource_version`, or `content_text` is empty, treat the task as obsolete and ack it directly
4. call the new `EmbeddingClient` to generate a vector
5. conditionally write back:

```sql
UPDATE files
SET    embedding = ?,
       embedding_revision = ?,
       confirmed_at = confirmed_at
WHERE  file_id = ?
  AND  revision = ?
  AND  status = 'CONFIRMED';
```

6. if `RowsAffected = 0`, the content has already changed or the file is no longer valid, so the task is treated as obsolete and acked
7. ack the task only after writeback succeeds or obsolescence is confirmed

This flow naturally supports at-least-once delivery because writeback for the same `file_id + revision` is idempotent.

### 5) Search path changes: query embedding must move to the app layer too

Document embedding alone is not enough. `pkg/datastore/search.go` currently passes the query string directly to `VEC_EMBED_COSINE_DISTANCE`, and that path also depends on a capability already ruled out by issue #30.

So the search path must change at the same time:

- add a `pkg/embedding` interface that provides query embedding
- keep the overall concurrent FTS + vector structure in `Grep`
- FTS can execute immediately
- the vector path should first generate a query vector in the app layer, then call `vectorSearchByVector`
- the SQL filter must become:

```sql
f.status = 'CONFIRMED'
AND f.embedding IS NOT NULL
AND f.embedding_revision = f.revision
```

- if the embedding provider is temporarily unavailable, the request should degrade to FTS + keyword search instead of making the whole search API fail
- any async `content_text` writeback (today mainly image extract) can no longer rely implicitly on generated embedding; it must explicitly trigger `embed`

The RRF merge logic can stay. What needs to change is who produces the query vector.

### 6) Retry, recovery, and dead-letter behavior

In Phase 1, task state should stay in the smallest operationally useful set:

- `queued`
- `processing`
- `succeeded`
- `failed`
- `dead_lettered`

Recommended semantics:

- worker failure that does not exceed `max_attempts`: `status -> queued`, and `available_at` is delayed with exponential backoff
- worker failure that exceeds `max_attempts`: `status -> dead_lettered`, preserving `last_error`
- process crash: `processing` tasks are returned to `queued` by `RecoverExpired` after lease expiration
- ack must validate `task_id + receipt` together

This part should follow QueueFS durable tests as closely as possible, because those tests have already shown that receipt-based ack and restart recovery are necessary, not optional nice-to-haves.

## Compatibility and Invariants

- The success semantics of `PUT /v1/fs/{path}` and upload completion do not change. Eventual consistency for semantic indexing must not redefine what it means for a file write to succeed.
- `files.revision` remains the only trusted content version identifier. Async tasks must never write back when `revision` does not match.
- zero-copy copy / rename do not create new semantic work because they do not change content version.
- Large files and binary files may have no embedding in Phase 1. But if some file class has obtained current-revision `content_text` through an async flow already, such as image extract, then it should still obtain an embedding through the bridge rule.
- vector search may return only vectors where `embedding_revision = revision`, never historical vectors.
- all tasks are stored inside each tenant's own database; no cross-tenant shared queue is introduced.

## Alternatives Considered

### A. Move the full QueueFS durable mode directly into dat9-2

Not recommended for Phase 1. QueueFS has a good durable queue contract, but its filesystem control surface, queue registry, and per-queue table design are too heavy for dat9's internal semantic pipeline, and they cannot directly express task metadata such as `resource_id + resource_version + last_error + max_attempts`.

### B. Build only the simplest possible task table and do not absorb QueueFS contract

Also not recommended. Issue #30 requires lease, ack, crash recovery, and at-least-once delivery. QueueFS has already explored those boundaries once and has cross-backend regression tests. Dat9 does not need to reinvent claim / receipt / recover semantics.

### C. Start with in-process channel / goroutine and add durable queue later

Not acceptable. Embedding is already a prerequisite for search availability, not a P8 optimization. An in-memory queue drops tasks directly on crash/restart and does not satisfy the premise of issue #30.

## Incremental Plan

### Phase A: Contract hardening and schema prep

1. Remove the dependency on generated `EMBED_TEXT(...)` in tenant schema; introduce writable embedding columns and `embedding_revision` for new tenants
2. Extend `pkg/datastore.File`, scan helpers, and query projections so the application can read and write new state such as `embedding_revision`
3. Add the `semantic_tasks` table and required indexes
4. Extend test schema helpers:
   - `pkg/datastore/schema_test_helper_test.go`
   - `pkg/server/schema_test_helper.go`
   - `pkg/client/schema_test_helper.go`
   - `pkg/backend/schema_test_helper.go`
5. Add queue contract and datastore helpers under `pkg/queue` or `pkg/semantic`: `Enqueue`, `Claim`, `Ack`, `Retry`, `RecoverExpired`
6. Translate the key QueueFS durable lifecycle tests into local dat9 tests: wrong receipt, recovery after restart, claim order, ack after stale claim

### Phase B: Write-path integration

7. Convert create / overwrite in `pkg/backend/dat9.go` into transactional implementations
8. On successful write, synchronously clear old embedding state and enqueue an `embed` task
9. Modify `pkg/backend/upload.go` so upload completion explicitly clears embedding state during overwrite; for large/binary files without a `content_text` producer, Phase 1 still does not create `embed` tasks
10. Add a bridge to the existing `pkg/backend/image_extract.go`: after `UpdateFileSearchText` succeeds, enqueue/ensure an `embed` task for the same revision, so image-file vector search does not regress
11. Add a unique constraint on `(task_type, resource_id, resource_version)` to avoid duplicate tasks for the same version

### Phase C: Worker and search correctness

12. Add `pkg/embedding` interface and concrete provider implementation (OpenAI / Titan / other provider injected through config)
13. Start `SemanticWorkerManager` and the recovery sweep in `pkg/server`
14. Change the vector path in `pkg/datastore/search.go` to accept a query vector rather than query text
15. Keep the current FTS + RRF structure, but require vector search to read only current-revision vectors

### Phase D: Operational hardening and future extension

16. Add metrics: task queued / processing / recovered / succeeded / failed / dead_lettered, embedding latency, queue lag; naming should align with existing `image_extract` metrics where possible
17. Add log fields: `tenant_id`, `task_id`, `resource_id`, `resource_version`, `receipt`, `attempt_count`
18. After `embed` is stable, reuse the same task substrate to add `extract_text`, `generate_l0`, and `generate_l1`

Phase D is also the place to decide the minimum production observability set needed for dead-letter triage and queue-lag diagnosis. Phase C should not block worker/search correctness on that decision; instead, once correctness is stable, this phase should finalize the smallest useful metrics/logging surface and align naming with existing `image_extract` instrumentation where practical.

## Validation Strategy

- **Queue contract tests**:
  - claim -> ack happy path
  - wrong receipt ack fails
  - lease expiration + recover makes task claimable again
  - restart after claim still allows recover
  - recovered tasks keep stable FIFO-compatible order

- **Backend write-path tests**:
  - small text create enqueues `embed` task and clears stale embedding state
  - overwrite increments revision and only the latest revision can accept writeback
  - small-to-large overwrite clears embedding state and does not leave stale vectors searchable
  - image extract writes `content_text` for revision N and triggers exactly one `embed` task for revision N
  - copy / rename do not enqueue duplicate tasks

- **Worker correctness tests**:
  - obsolete task (revision mismatch) is acked without writeback
  - duplicate delivery writes the same revision idempotently
  - stale image extract completion cannot enqueue or write back an embedding for an old revision
  - provider failure triggers retry/backoff, then dead-letter after max attempts

- **Search tests**:
  - vector search uses an app-generated query vector, not raw text
  - vector search returns only rows where `embedding_revision = revision`
  - when the embedding provider is unavailable for queries, grep falls back to FTS / keyword path

- **End-to-end smoke**:
  - write file -> worker runs -> search returns the file
  - crash worker after claim -> recover sweep -> task completes after restart

## Risks and Mitigations

1. **Write-path refactor may introduce metadata inconsistencies** - reuse the `Store.InTx` pattern first, add `InsertFileTx` / `UpdateFileContentTx` helpers, and cover create/overwrite/copy/rename with existing backend regression tests.
2. **Stale embeddings may remain queryable after overwrite** - the write path must clear `embedding` / `embedding_revision` first, and the query path must read only `embedding_revision = revision`.
3. **Worker starvation in multi-tenant mode** - use round-robin tenant polling + per-tenant concurrency caps, instead of tying workers to pool cache lifetime.
4. **The existing image extract path may lose vector update capability after migration** - in Phase 1, encode "successful `UpdateFileSearchText` -> enqueue same-revision `embed`" as a bridge rule in both implementation and tests, so semantic retrieval for image files does not regress.
5. **Embedding provider latency or rate limits may slow indexing** - move embedding to the background, add retry/backoff/dead-letter, and allow the query path to degrade to FTS when the provider is unavailable.
6. **This schema breaking change blocks compatibility for existing tenants** - Phase 1 explicitly does not handle old-tenant migration; the impact is declared clearly in the document instead of continuing to assume an additive migration fallback.

## Open Questions

1. Which embedding provider should Phase 1 use by default, and which environment variables provide its secrets/config? The current repository has no existing `EmbeddingClient`, but `pkg/backend/image_extract_openai.go` already shows an OpenAI-compatible HTTP client pattern. We need to decide whether to reuse that transport/config convention directly or define separate embedding-provider config.
2. The `autoEmbedModel` constant in `pkg/tenant/schema_common.go` is already `tidbcloud_free/amazon/titan-embed-text-v2`. Should async text embedding continue to treat it as the canonical text model, and do we need to record the model ID in file/task metadata to support future re-embedding?
3. After `embed` is stable, should the current image-only `AsyncImageExtract` also be migrated into the same durable task substrate, or should it remain a best-effort `content_text` producer for now and trigger durable `embed` on top?

## References

- `dat9` origin issue #30: durable async processing is a prerequisite because db9 does not provide `EMBED_TEXT(text) -> vector`
- `agfs-worktree/agfs-server/docs/concepts/durable-queuefs-concept.md`
- `agfs-worktree/agfs-server/pkg/plugins/queuefs/internal/sqlqueue/backend.go`
- `agfs-worktree/agfs-server/pkg/plugins/queuefs/queuefs_durable_contract_test.go`
- `dat9-2/pkg/backend/dat9.go`
- `dat9-2/pkg/backend/upload.go`
- `dat9-2/pkg/backend/image_extract.go`
- `dat9-2/pkg/backend/image_extract_test.go`
- `dat9-2/pkg/backend/options.go`
- `dat9-2/pkg/datastore/search.go`
- `dat9-2/pkg/datastore/store.go`
- `dat9-2/pkg/tenant/pool.go`
- `dat9-2/pkg/tenant/schema_zero.go`
- `dat9-2/pkg/tenant/schema_starter.go`
- `dat9-2/pkg/tenant/schema_db9.go`
