# Proposal: Phase 1 rollout for durable `img_extract_text` in dat9

**Date**: 2026-04-01
**Purpose**: Based on the current `dat9` codebase, propose a production-oriented, clearly scoped, incrementally rollable Phase 1 plan to migrate the current backend-owned asynchronous image text extraction path from a best-effort in-memory queue to the durable `semantic_tasks` substrate, with priority on `auto embedding + create + overwrite + upload completion`.

## Summary

The current system already has most of the image extraction processing logic and the durable task substrate, but migrating the auto-embedding image path onto `semantic_tasks` requires the design to satisfy four production-grade requirements:

1. Durable registration of `img_extract_text` must commit in the same transaction as file revision visibility; otherwise the system still has a correctness gap where a file revision is visible but its task is missing.
2. Semantic worker startup and tenant scan logic cannot continue to be designed only around the app-side `embedder`; otherwise the auto embedding path may enqueue durable tasks with no worker able to process them.
3. The runtime dependency source for the `img_extract_text` handler must be explicit. In the current code, image extraction depends on backend-owned capabilities such as the extractor, S3 client, and size/timeout limits; Phase 1 must define where those dependencies come from in multi-tenant execution.
4. Phase 1 must define a clear coexistence strategy between auto mode and the legacy in-memory queue; otherwise durable enqueue and the old `enqueueImageExtract*()` path can both run and cause duplicate processing or duplicate writeback.

This proposal follows these design principles:

- Keep a single `semantic_tasks` table; do not introduce a second queueing system
- Narrow the future reserved `extract_text` task type into the more honest `img_extract_text`
- In Phase 1, switch only the three auto embedding write paths: `create`, `overwrite`, and `upload completion`
- Keep semantic worker as the only delivery owner
- Reuse the existing fallback backend and tenant backends created by `tenant.Pool` as the preferred source of `img_extract_text` runtime dependencies, instead of inventing a new generic runtime framework in Phase 1
- Keep app embedding's `img_extract_text -> embed` flow explicitly out of Phase 1

## Context

### Capabilities that already exist in the current system

The current repository already has most of the image text extraction machinery. What is missing is durable delivery.

**Synchronous text fast path**

- `extractText(data, contentType)` lives in `pkg/backend/dat9.go:719`
- It only handles a subset of text content types; once a file exceeds `smallFileThreshold`, it returns an empty string, see `pkg/backend/dat9.go:32` and `pkg/backend/dat9.go:726`
- This path is responsible for synchronous `content_text` production for small text files, not images

**Current asynchronous image extraction prototype**

- The backend starts an in-memory queue and worker in `configureOptions()` based on `AsyncImageExtractOptions`, see `pkg/backend/options.go:47`
- When the queue is full, tasks are dropped immediately, see `pkg/backend/image_extract.go:77`
- The image worker reads the current `files` row first, then performs revision gating, image-type checks, byte loading, extraction, sanitization, and writeback, see `pkg/backend/image_extract.go:146`
- Successful writeback uses `UpdateFileSearchTextTx(tx, fileID, revision, text)`, see `pkg/backend/image_extract.go:215` and `pkg/datastore/store.go:487`
- In app embedding mode, successful writeback bridges an `embed` task; in auto embedding mode it does not create an app-side `embed` task, see `pkg/backend/image_extract.go:221`

**Durable substrate is already present**

- Tenant schemas already include `semantic_tasks` and its unique index in both auto/app TiDB schemas, see `pkg/tenant/schema_tidb_auto.go:117` and `pkg/tenant/schema_tidb_app.go:80`
- `semantic_tasks` already supports `Enqueue`, `EnsureQueued`, `Claim`, `Ack`, `Retry`, and `RecoverExpired`, see `pkg/datastore/semantic_tasks.go:26`, `pkg/datastore/semantic_tasks.go:227`, `pkg/datastore/semantic_tasks.go:295`, `pkg/datastore/semantic_tasks.go:318`, and `pkg/datastore/semantic_tasks.go:366`
- Semantic worker already has polling, claiming, retry, recovery, queue gauges, and structured logging, see `pkg/server/semantic_worker.go:137`, `pkg/server/semantic_worker.go:198`, and `pkg/server/semantic_worker.go:588`

### The key gap in the current system

Even though the durable substrate already exists, image text production in auto embedding mode still depends on a backend-owned in-memory queue. The relevant write paths are:

- `create` still directly calls `enqueueImageExtract(...)`, see `pkg/backend/dat9.go:368`
- `overwrite` still directly calls `enqueueImageExtract(...)`, see `pkg/backend/dat9.go:450`
- `upload completion` still directly calls `enqueueImageExtractForUpload(...)`, see `pkg/backend/upload.go:311`

This means auto embedding still has the following production risks:

- tasks in the queue are lost after process restart
- new image revisions have no compensation path when the queue is full
- in multi-tenant mode, when a backend is evicted from `tenant.Pool`, its worker stops with it, see `pkg/backend/options.go:99` and `pkg/tenant/pool.go:231`
- large images mainly go through `upload completion`; changing only ordinary `Write()` would still leave the auto embedding image path incomplete

### The current split between auto embedding and app embedding

The current code already distinguishes the two embedding modes clearly:

- In auto embedding mode, `files.embedding` is a stored generated column derived by the database from `content_text`, see `pkg/tenant/schema_tidb_auto.go:72`
- Auto embedding write paths do not enqueue app-managed `embed` tasks, see `pkg/backend/dat9.go:358` and `pkg/backend/upload.go:252`
- Auto embedding overwrite/confirm helpers do not clear embedding columns, see `pkg/datastore/file_tx.go:77` and `pkg/datastore/file_tx.go:120`
- In auto embedding mode, grep uses `VectorSearchByText` directly instead of an app-side query embedder, see `pkg/backend/dat9.go:768`

So prioritizing auto embedding in Phase 1 creates a smaller and real closed loop: once `content_text` is written back durably, the database's existing auto embedding behavior continues to work.

## Goals

1. In Phase 1, narrow the asynchronous image text extraction task type to `img_extract_text`.
2. Prioritize auto embedding in Phase 1 instead of switching app embedding at the same time.
3. Ensure all three auto-mode image write paths - `create`, `overwrite`, and `upload completion` - durably register `img_extract_text`.
4. Ensure the successful `img_extract_text` path only performs revision-gated `content_text` writeback and does not create app-side `embed` tasks.
5. Make semantic worker the only delivery owner so image text extraction no longer depends on backend in-memory queue delivery semantics.

## Non-Goals

- Do not implement generic `extract_text` in this phase.
- Do not switch app embedding to `img_extract_text -> embed` in this phase.
- Do not rename `AsyncImageExtractOptions` in this phase.
- Do not solve DB9/Postgres provider-neutral runtime in this phase.
- Do not change the synchronous `extractText()` fast path within `smallFileThreshold`.
- Do not fully retire the legacy image queue in this phase; it remains temporarily for app embedding paths.

## Support Levels

| Scope | Phase 1 support level | Notes |
| --- | --- | --- |
| local fallback backend + auto embedding | Supported | Requires `AsyncImageExtractOptions.Enabled=true`, which is the existing image extraction runtime switch |
| multi-tenant TiDB auto providers | Supported | Continues using the current TiDB/MySQL-oriented `datastore.Open(...)` worker runtime path |
| app embedding | Deferred | Keeps the current `embed` and legacy image queue behavior |
| DB9/Postgres provider-neutral runtime | Deferred | Out of scope for Phase 1 |

## Architecture Comparison

```text
Current Architecture
--------------------

Request Path                                  Background Path
+----------------------------------+          +----------------------------------+
| create / overwrite /            |          | backend-owned image worker       |
| upload completion                |          | (started by backend options)     |
+----------------+-----------------+          +----------------+-----------------+
                 |                                           ^
                 v                                           |
      +---------------------------+                +---------+---------+
      | files revision committed  |                | in-memory image   |
      | in tenant store           |                | queue (channel)   |
      +-------------+-------------+                +---------+---------+
                    |                                        ^
                    | auto mode: no durable task             |
                    +----------------------------------------+
                    | enqueueImageExtract(...)
                    v
      +---------------------------+
      | load image bytes +        |
      | extract + sanitize text   |
      +-------------+-------------+
                    |
                    v
      +---------------------------+
      | UpdateFileSearchTextTx    |
      | (revision-gated writeback)|
      +-------------+-------------+
                    |
          +---------+---------+
          |                   |
          v                   v
   auto embedding       app embedding
   stops here           may requeue / bridge embed

Separate server-owned semantic worker today:
    semantic worker in dat9-server -> only handles durable embed tasks


Target Architecture (Phase 1)
-----------------------------

Request Path                                  Background Path
+----------------------------------+          +----------------------------------+
| create / overwrite /            |          | semantic worker in dat9-server   |
| upload completion                |          | (server-owned delivery owner)    |
+----------------+-----------------+          +----------------+-----------------+
                 |                                           |
                 v                                           v
      +---------------------------+                +---------------------------+
      | same DB transaction       |                | claim img_extract_text    |
      | - write/confirm revision  |                | from semantic_tasks       |
      | - enqueue img_extract_text|                +-------------+-------------+
      +-------------+-------------+                              |
                    |                                            v
                    v                                 +---------------------------+
      +---------------------------+                   | resolve backend runtime   |
      | semantic_tasks            |                   | (fallback or tenant       |
      | img_extract_text row      |                   | backend)                  |
      +-------------+-------------+                   +-------------+-------------+
                    |                                            |
                    +--------------------------------------------+
                                                                 v
                                                      +---------------------------+
                                                      | load image bytes +        |
                                                      | extract + sanitize text   |
                                                      +-------------+-------------+
                                                                    |
                                                                    v
                                                      +---------------------------+
                                                      | UpdateFileSearchTextTx    |
                                                      | + ack / retry / recover   |
                                                      +-------------+-------------+
                                                                    |
                                                                    v
                                                      +---------------------------+
                                                      | database auto embedding   |
                                                      | derives vector from       |
                                                      | latest content_text       |
                                                      +---------------------------+

Transition note:
    legacy backend image queue remains only for app embedding paths
```

## Design

### 1) Use the existing `semantic_tasks` substrate for `img_extract_text`

Phase 1 continues to reuse the existing `semantic_tasks` table. It does not introduce a second task table, and it no longer treats the backend in-memory queue as the source of truth for delivery.

The minimum required changes are:

- Rename `semantic.TaskTypeExtractText` to `semantic.TaskTypeImgExtractText`, currently in `pkg/semantic/task.go:12`
- Add a small helper analogous to `newEmbedTask(...)`, for example `newImgExtractTask(...)`, while keeping task identity as `(task_type, resource_id, resource_version)`
- Continue to use the unique index `uk_task_resource_version` on `semantic_tasks`, see `pkg/tenant/schema_tidb_auto.go:135`

Phase 1 does not require a generic handler registry or workflow engine. Adding one concrete case to `dispatchTask()` is sufficient, see `pkg/server/semantic_worker.go:498`.

### 2) Task payload should keep only minimal non-authoritative hints

`img_extract_text` needs a minimal `payload_json`. The reason is not to make payload the source of truth, but because the in-memory task currently carries `Path` and `ContentType`, while the durable `semantic.Task` model does not.

The payload should contain only:

- `path`
- `content_type`

Usage rules:

- `resource_id + resource_version` remains the only authoritative identity
- The handler must read the current `files` row and apply revision gating
- Payload is only used for logging, extract requests, and image-like fallback checks when `files.content_type` is missing

Phase 1 should not expand payload into a large debugging or business-state envelope.

### 3) Semantic worker remains the only delivery owner

In Phase 1, backend-owned goroutines no longer execute image text extraction for auto mode. The true delivery owner becomes `semanticWorkerManager`.

However, this proposal does not recommend inventing a new generic runtime abstraction in Phase 1. The current code already has two stable runtime sources:

- Local mode directly holds the fallback backend, see `pkg/server/server.go:138`
- Multi-tenant mode creates tenant backends through `tenant.Pool` with shared `BackendOptions`, see `pkg/tenant/pool.go:164`

So the simplest design is:

- **Delivery stays in semantic worker**: claim / ack / retry / recover all continue through `datastore.Store`
- **Image extraction runtime comes from backend**: when handling a tenant task, the handler uses the fallback backend or `tenant.Pool.Get(ctx, tenant)` to obtain the corresponding backend and reuse its image extraction capabilities

This avoids redesigning extractor config, S3 client wiring, or a generic runtime framework in Phase 1, while continuing to reuse the existing runtime surface represented by `AsyncImageExtractOptions`, `ImageTextExtractor`, `loadImageBytesForExtract()`, and `sanitizeExtractedText()`. Delivery itself no longer depends on backend queue goroutines; backend becomes a runtime dependency container rather than the async task scheduler.

### 4) Worker gate must depend on handler capability, not only on embedder presence

The current worker gate is clearly designed for embed-only execution:

- When `embedder == nil`, the manager does not start, see `pkg/server/semantic_worker.go:114`
- Local auto fallback backend is disabled directly, see `pkg/server/semantic_worker.go:119`
- Multi-tenant scan skips auto providers directly, see `pkg/server/semantic_worker.go:435`

Phase 1 does not need full generalization; it only needs a small enablement change:

- `hasEmbedHandler := embedder != nil`
- `hasImgExtractHandler := async image extraction is enabled in the backend options represented by fallback/pool`
- Semantic worker should start unless both are false

Tenant scan should also be grouped by handler capability:

- When only `embed` exists: keep current app-mode scan behavior
- When only `img_extract_text` exists: scan only auto embedding tenants and the local auto fallback backend
- When both exist: scan both sets

`ClaimSemanticTask(...)` does not filter by `task_type`, see `pkg/datastore/semantic_tasks.go:227`. If an img-only worker scans an app tenant, it may claim an `embed` task first, and unsupported task types are retried as unsupported, see `pkg/server/semantic_worker.go:498`. Therefore, “img-only worker scans only auto stores” is not an optimization; it is a correctness precondition for Phase 1.

As a rollout precondition, any store cut over to an `img_extract_text`-only handler must not have pending app-side `embed` backlog. Otherwise, even with correct scan scope, the worker may still claim historical `embed` tasks within the same store. Such stores must either be drained first or excluded from the Phase 1 cutover set.

### 5) Register `img_extract_text` only when image extraction runtime is enabled

Today the system only treats a revision as having an async image text source when `AsyncImageExtractOptions.Enabled` is true and an extractor is configured, see `pkg/backend/semantic_tasks.go:39` and `pkg/backend/options.go:56`.

Phase 1 should keep that precondition:

- Enqueue `img_extract_text` for image-like revisions only when image extraction runtime is enabled
- If async image extraction is not enabled in the current deployment, the system should not register a durable task with no handler/runtime

This both aligns with current behavior and avoids registering durable tasks that have no handler.

### 6) Write paths must register `img_extract_text` inside the same transaction

This is one of the most important constraints in the Phase 1 design.

**Principle**: once an auto-mode image revision becomes visible to the tenant, its corresponding `img_extract_text` task must already be durably registered.

Therefore, task registration must occur in the same transaction that commits the file revision.

#### `create`

In `createAndWriteCtx()`, an image-like small-file create should enqueue `img_extract_text` after `InsertFileTx + EnsureParentDirsTx + InsertNodeTx` succeed and before the transaction commits. Reference point: `pkg/backend/dat9.go:339`.

#### `overwrite`

In `overwriteFileCtx()`, an image-like overwrite should enqueue `img_extract_text` after `UpdateFileContentAutoEmbeddingTx(...)` returns the new revision and before the transaction commits. Reference point: `pkg/backend/dat9.go:422`.

#### `upload completion`

In `ConfirmUpload()`, image-like upload completion must enqueue `img_extract_text` inside the transaction. Reference point: `pkg/backend/upload.go:205`.

Overwrite completion needs special care:

- The task `resource_id` must use the surviving inode's `confirmedFileID`
- The task `resource_version` must use the new `confirmedRevision` after overwrite
- The system must not mistakenly use the pending upload file's `upload.FileID` as task identity

This is also why `upload completion` must be included in the Phase 1 cutline: large image objects do not go through the synchronous small-file write path.

### 7) Auto mode must stop double-publishing to the legacy in-memory queue

Phase 1 does not require fully deleting the legacy image queue, but once auto mode switches to durable `img_extract_text`, the three paths must stop calling:

- `enqueueImageExtract(...)`, see `pkg/backend/image_extract.go:61`
- `enqueueImageExtractForUpload(...)`, see `pkg/backend/image_extract.go:91`

The recommended coexistence strategy is:

- **auto embedding + create/overwrite/upload completion**: register only durable `img_extract_text`; do not publish to the legacy queue
- **app embedding**: keep the current `embed` + legacy image queue behavior

This transition strategy is simple, rollback-friendly, and avoids touching app-mode task graph changes in Phase 1.

### 8) Handler logic should follow the correctness contract of the current image worker

The recommended `img_extract_text` handler flow is:

1. claim an `img_extract_text` task
2. read the current `files` row
3. if file is missing, not `CONFIRMED`, or `revision != resource_version`, ack obsolete
4. use `files.content_type` plus payload hints to determine whether the current revision is still image-like; if not, ack obsolete
5. load image bytes through the fallback backend or tenant backend and apply the existing `MaxImageBytes` limit
6. call `ImageTextExtractor`
7. sanitize the result with the existing `sanitizeExtractedText()`
8. if the sanitized result is empty, ack obsolete or `empty_result`
9. execute `UpdateFileSearchTextTx(tx, fileID, revision, text)` in a transaction
10. ack on success; use obsolete for permanently inapplicable cases; use retry / dead-letter for transient runtime errors

Recommended error classification:

- **obsolete / non-retry**: file not found, not confirmed, revision mismatch, not image, too large, empty result
- **retry**: S3/object-store read failure, extractor API failure, transactional writeback failure, transient database error

This matches the current in-memory worker behavior; it just turns implicit skip/return behavior into explicit durable task outcomes.

### 9) Phase 1 convergence decisions

To avoid further branching during implementation, Phase 1 adopts the following explicit decisions:

1. **`too_large` and `empty_result` share the same non-retry ack delivery semantics**
   - Neither should be retried, and both end task lifecycle through ack.
   - Metrics and logging should still preserve finer result labels such as `too_large`, `empty_result`, `stale`, and `not_image` for operability.

2. **The legacy image queue remains in Phase 1, but only for app embedding paths**
   - Auto embedding `create`, `overwrite`, and `upload completion` no longer publish to the legacy queue.
   - App embedding continues to depend on the current legacy queue until its image task handling also migrates to `semantic_tasks`.
   - To prevent this transitional dependency from being mistaken for long-term design, relevant app-mode branches must include code comments stating that the logic is temporary compatibility and can be removed once app embedding switches to `semantic_tasks`.

3. **In Phase 1, img-only workers scan only auto stores, and cutover stores must not retain app-side `embed` backlog**
   - Phase 1 does not introduce `task_type`-level claim filtering; it uses tenant/store scan scope to ensure img-only workers never see app-side `embed` tasks.
   - Any store being cut over to an img-only worker must be confirmed free of pending `embed` backlog before rollout; otherwise it is outside the supported Phase 1 cutover scope.

## Incremental Plan

### Step 1: narrow task type and fix worker gate

1. Rename `TaskTypeExtractText` to `TaskTypeImgExtractText`
2. Add an `img_extract_text` case to `dispatchTask()`
3. Narrow the semantic worker startup rule from “must have embedder” to “must have at least one configured handler”
4. Change tenant scan scope to depend on handler capability instead of hard-coded skipping of auto providers
5. Make “img-only worker scans only auto stores” and “no `embed` backlog before cutover” explicit implementation and operational preconditions

### Step 2: add minimal payload and runtime accessors

6. Define a minimal `payload_json` for `img_extract_text` (`path`, `content_type`)
7. Provide a minimal backend accessor for durable image extraction handling rather than exposing the old queue
8. In multi-tenant mode, use `tenant.Pool.Get(...)` to obtain tenant backend as the image extraction runtime source

### Step 3: connect the three auto-mode write paths

9. Enqueue `img_extract_text` inside the transaction for `create`
10. Enqueue `img_extract_text` inside the transaction for `overwrite`
11. Enqueue `img_extract_text` inside the transaction for `upload completion`
12. Stop calling `enqueueImageExtract*()` on those auto-mode paths
13. Add comments in the app-mode branches that still keep the legacy queue, explicitly marking the dependency as temporary compatibility to be removed once app embedding image tasks move to `semantic_tasks`

### Step 4: implement the handler and validate it

14. Implement `img_extract_text` claim / ack / retry / recover
15. Keep the success path limited to `UpdateFileSearchTextTx(...)`
16. Preserve fine-grained metrics / logging labels such as `too_large`, `empty_result`, `stale`, and `not_image` for non-retry ack cases
17. Add tests for stale revision, too large, empty result, extract failure, restart recovery, upload overwrite, and img-only worker scan boundaries

## Validation Strategy

- **Write path tests**
  - In auto embedding mode, image `create` leaves an `img_extract_text` task after transaction commit
  - In auto embedding mode, image `overwrite` registers `img_extract_text` for the new revision
  - In auto embedding mode, image `upload completion` and overwrite completion both register `img_extract_text` for the surviving inode / current revision
  - In auto embedding mode, those three paths no longer publish into the legacy image queue

- **Worker startup and tenant-scope tests**
  - The local fallback backend still starts semantic worker under `auto embedding + image extract enabled + no embedder`
  - In multi-tenant mode, auto providers are no longer skipped unconditionally
  - When only the image handler exists, app tenants are not scanned
  - Stores that still have `embed` backlog are excluded from Phase 1 img-only cutover

- **Handler correctness tests**
  - `img_extract_text` success updates only the current revision's `content_text`
  - A stale revision never overwrites the current revision
  - A non-image task that was enqueued by mistake is acked obsolete
  - Too-large images and empty results do not cause infinite retries
  - Metrics / logging distinguish `too_large` from `empty_result` even though both are non-retry ack cases
  - Auto embedding mode does not create extra `embed` tasks

- **Recovery and lifecycle tests**
  - After claim and process restart, the task can be recovered after lease expiry
  - Tenant pool eviction no longer causes permanent task stall; the worker can resolve tenant backend runtime again on the next attempt
  - Code branches that still keep the legacy queue for app embedding contain explicit comments stating that they are temporary compatibility logic

## Risks and Mitigations

1. **Auto mode and legacy queue coexist and cause duplicate processing**
   - Keep exactly one delivery target for auto-mode `create`, `overwrite`, and `upload completion`: durable `img_extract_text`; keep legacy queue only for app mode.

2. **Worker runtime and scan scope remain unclear, causing wrong claims or excessive refactoring**
   - In Phase 1, reuse fallback backend and tenant backend as runtime sources; restrict worker scan to auto stores; require `embed` backlog to be drained before cutover.

3. **Upload completion overwrite binds task identity to the pending upload file ID**
   - Use `confirmedFileID + confirmedRevision` as durable task identity inside the transaction, and add dedicated tests for overwrite completion.

4. **Phase 1 scope expands into app embedding or provider-neutral runtime**
   - Keep app mode's `img_extract_text -> embed` and DB9/Postgres runtime explicitly deferred and separate from this phase.

## Conclusion

To meet production quality, Phase 1 must define transaction boundaries, worker gate behavior, runtime dependency sources, and coexistence strategy precisely. This proposal does that without expanding the abstraction surface, adding a new queue engine, or mixing in app embedding early. It focuses only on moving the three auto embedding image write paths - `create`, `overwrite`, and `upload completion` - onto durable `img_extract_text`.

The benefit is concrete and verifiable: the most fragile image text production path in the current system moves from backend in-memory best-effort behavior to a durable flow that can claim, retry, and recover, while the database's existing auto embedding behavior continues to reuse `files.content_text` without redesigning vector generation in Phase 1.
