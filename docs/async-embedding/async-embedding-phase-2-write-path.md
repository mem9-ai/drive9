# Proposal: dat9-2 Async Embedding Phase 2 Write-Path Integration

**Date**: 2026-03-31
**Purpose**: Turn Phase B of `async-embedding-generation-proposal.md` into a scoped, review-ready proposal for transactional write-path integration and image-to-embed bridging.

## Summary

Phase 2 should make semantic work registration part of the committed write path. Small-file create, small-file overwrite, and multipart upload completion should clear stale embedding state and enqueue an `embed` task in the same tenant transaction that makes the new file revision visible. The existing async image extract path should remain a best-effort `content_text` producer, but once it successfully writes text back for a specific revision, it must explicitly ensure the same revision's `embed` task is queued.

This phase does not introduce the background worker itself and does not change query embedding. Its job is narrower: guarantee that every revision which should eventually receive an embedding has a durable task record, and guarantee that revisions which no longer have valid semantic state stop exposing stale vectors immediately.

## Context

### Current State

The repository already contains the Phase 1 foundation required by write-path integration:

- tenant schemas now define mutable `files.embedding`, `files.embedding_revision`, and tenant-local `semantic_tasks` in `pkg/tenant/schema_zero.go` and `pkg/tenant/schema_db9.go`
- datastore exposes tx-aware file seams in `pkg/datastore/file_tx.go`
- datastore exposes durable task seams in `pkg/datastore/semantic_tasks.go`
- the `File` model already projects `embedding_revision` through common read paths in `pkg/datastore/store.go`

The write paths that Phase 2 needs to integrate are still owned by `pkg/backend`:

- small-file create / overwrite in `pkg/backend/dat9.go`
- multipart upload completion and in-place overwrite in `pkg/backend/upload.go`
- async image caption / OCR writeback in `pkg/backend/image_extract.go`

Those code paths also preserve production constraints that cannot be broken:

- `PUT /v1/fs` remains the synchronous success boundary for small writes
- large files still follow `202` + multipart completion semantics
- overwrite remains an in-place inode update on the same `file_id`
- zero-copy copy and rename do not create a new content revision
- image extract is still an in-memory, backend-owned queue, not a durable worker

### Problem Statement

With application-managed embeddings, the write path must now explicitly connect two pieces of state that used to be implicitly coupled by generated columns:

1. the committed file revision in `files`
2. the durable semantic work item that will later produce the embedding for that revision

If Phase 2 does not bind those updates together, the system can enter correctness gaps such as:

- a file revision becomes visible, but no `embed` task exists for it
- overwrite clears `content_text` or changes content, but stale `embedding` / `embedding_revision` remain queryable
- multipart upload completion preserves inode identity but forgets to register semantic work for the new revision
- image extract updates `content_text` asynchronously, but no follow-up embed work exists for the same revision

The issue is not only eventual consistency delay. The issue is silent semantic drift: search can observe a confirmed file revision whose semantic task state no longer matches the visible content state.

### Constraints and Decision Drivers

- Public file-write semantics must stay stable: small writes return synchronously; multipart upload still completes through the existing API flow.
- File persistence and task enqueue must commit atomically inside one tenant transaction.
- `file_id + revision` remains the only valid binding for semantic work because hard links share the same inode and overwrite increments `revision` in place.
- Overwrite and upload overwrite must clear stale embedding state immediately, before any background worker runs.
- Copy and rename must not create semantic work because they do not create a new content revision.
- Large/binary objects that do not have a current `content_text` producer should not enqueue `embed` tasks in this phase.
- The existing image extract path must keep image files semantically searchable after generated embedding removal, even though image extract itself is still best-effort.
- The design should reuse existing datastore seams and avoid introducing a second orchestration layer below `pkg/backend`.

## Goals

1. Every small-file create or overwrite that produces current `content_text` commits a matching `embed` task for the same `file_id + revision`.
2. Multipart upload completion clears stale semantic state and enqueues durable `embed` work when the completed object should later receive an embedding.
3. Overwrite, small-to-large transitions, and upload overwrite stop exposing stale vectors immediately by clearing `embedding` and `embedding_revision` during the write transaction.
4. Async image text writeback preserves vector freshness by explicitly ensuring the same revision's `embed` task after a successful `content_text` update.
5. Copy and rename continue to avoid duplicate semantic work because they do not create a new content revision.

## Non-Goals

- Do not start the durable semantic worker in this phase.
- Do not introduce a real embedding provider or document-vector writeback path in this phase.
- Do not migrate image extract itself into a durable `extract_text` task substrate.
- Do not change grep/query embedding behavior in this phase.
- Do not add synchronous task cancellation on delete; obsolete-task handling remains a worker responsibility.

## Architecture Overview

```text
Small write / overwrite
-----------------------
backend.WriteCtx
    -> backend transaction
    -> update files row for new revision
    -> clear embedding + embedding_revision
    -> enqueue semantic_tasks(embed, file_id, revision)
    -> commit
    -> optionally enqueue image extract (best effort)

Multipart completion
--------------------
backend.ConfirmUpload
    -> complete S3 multipart outside tx
    -> backend transaction
    -> create or overwrite inode metadata
    -> clear embedding + embedding_revision
    -> enqueue semantic_tasks(embed, file_id, revision) when applicable
    -> commit
    -> optionally enqueue image extract (best effort)

Async image text update
-----------------------
image extract worker
    -> UpdateFileSearchText(file_id, revision, text)
    -> if revision update succeeds, ensure embed task for same revision
```

## Design

### 1) Ownership and seam usage

Phase 2 should keep ownership simple:

- `pkg/backend` owns request-path orchestration and decides whether semantic work should exist for the new revision
- `pkg/datastore/file_tx.go` owns tx-local file mutation helpers
- `pkg/datastore/semantic_tasks.go` owns tx-local task creation / ensure semantics
- `pkg/backend/image_extract.go` remains the only owner of the current image `content_text` producer

No additional queue abstraction or background coordination layer is needed in this phase. The only new backend responsibility is to call the existing tx helpers in the right order and under the right content-type rules.

### 2) Transaction rules for each write surface

Use the following operational rule set:

| Write surface | `content_text` source at commit time | Transaction behavior | Post-commit behavior |
| --- | --- | --- | --- |
| Small text create | synchronous `extractText` | insert file row, set `content_text`, clear embedding state, enqueue `embed` | none |
| Small text overwrite | synchronous `extractText` | update file row in place, increment revision, clear embedding state, enqueue `embed` | none |
| Small image create/overwrite | synchronous mime detection; async image text | commit file row and clear embedding state; enqueue `embed` for the initial revision if current path semantics still expect semantic indexing for the revision | enqueue image extract best-effort |
| Multipart new file | no immediate text producer for generic object; content type still known | confirm metadata, clear embedding state, enqueue only when Phase 2 rules classify the object as needing eventual embedding | optionally enqueue image extract for image-like objects |
| Multipart overwrite | no immediate text producer for generic object; inode preserved | overwrite file row in place, increment revision, clear embedding state, enqueue only when Phase 2 rules classify the object as needing eventual embedding | optionally enqueue image extract for image-like objects |
| Copy / rename | none | no semantic task changes | none |

The table intentionally separates "clear stale state" from "enqueue embed". Clearing stale state is mandatory on any content mutation. Enqueueing `embed` depends on whether this phase has a valid current or future `content_text` source for the new revision.

### 3) Small-file create and overwrite

For `pkg/backend/dat9.go`, Phase 2 should replace the previous split operations with one transaction per committed revision.

For create:

1. compute `content_type`, checksum, and synchronous `content_text` from the request body
2. decide storage mode (inline vs S3) using the existing size policy
3. inside one transaction:
   - insert the `files` row for revision `1`
   - ensure parent directories
   - insert the node binding
   - insert the `embed` task for `(file_id, revision=1)`
4. after commit, enqueue image extract only if the file should go through the existing image path

For overwrite:

1. compute final bytes using the existing truncate/append/offset semantics
2. compute `content_type`, checksum, and synchronous `content_text`
3. store any newly required blob payload before the metadata transaction, following the current large-file behavior
4. inside one transaction:
   - update the existing `files` row in place
   - increment `revision`
   - clear `embedding` and `embedding_revision`
   - enqueue the `embed` task for the new revision
5. after commit:
   - delete the replaced blob reference, if any
   - enqueue image extract if the new revision is image-like

This preserves the current synchronous file-write API while moving semantic correctness to an atomic metadata boundary.

### 4) Multipart upload completion and overwrite

`pkg/backend/upload.go` has two distinct data-plane steps that must stay separate:

- complete the S3 multipart upload outside the SQL transaction
- commit tenant metadata changes and semantic task registration inside the SQL transaction

The Phase 2 design should keep that split, because object-store completion is already an external side effect that cannot be rolled back by tenant SQL.

For multipart new-file completion:

- mark the pending file row as confirmed
- set `storage_type`, `storage_ref`, `content_type`, size, and confirmed timestamp
- clear `embedding` and `embedding_revision`
- insert the path node
- enqueue `embed` only when the file is expected to receive current or future `content_text` in this phase

For multipart overwrite:

- preserve inode identity by updating the existing `files` row for the path's current `file_id`
- increment `revision`
- clear `embedding` and `embedding_revision`
- update `content_type` at the same time; the overwrite path must not leave stale metadata behind
- tombstone the upload's placeholder file row and rebind the `uploads` row to the surviving inode
- enqueue `embed` only when the new revision should eventually receive semantic text

The key invariant is that upload overwrite never creates a second semantic identity. The surviving inode remains the semantic resource, and the new task binds to the same `file_id` with a new `revision`.

### 5) Image extract bridge and same-revision ensure semantics

The current image extract worker in `pkg/backend/image_extract.go` already uses `UpdateFileSearchText(ctx, fileID, revision, text)` to avoid stale writeback. Phase 2 should preserve that revision gate and add one more requirement:

- if `UpdateFileSearchText(...)` returns `updated=true`, the backend must ensure that `(task_type='embed', resource_id=file_id, resource_version=revision)` is queued

This bridge must satisfy two cases:

1. if the same-revision `embed` task does not exist yet, create it
2. if the same-revision `embed` task already exists but has already reached a terminal state, re-queue it in place so the fresher `content_text` can still drive embedding generation

It must not requeue work for stale image results. If the revision-gated text update fails because the file has already moved to a newer revision, the image path must stop there.

### 6) Failure handling, rollback, and degraded behavior

Phase 2 needs explicit handling for the mismatch between SQL atomicity and object-store side effects.

- If blob upload to S3 fails before the metadata transaction starts, abort the write and do not create a file row or task.
- If the metadata transaction fails after a new blob object was written, best-effort delete the new blob reference before returning the error.
- If overwrite succeeds and an old blob is no longer referenced, delete the old blob only after transaction commit.
- If image extract enqueue fails, the file write still succeeds; the degraded behavior is limited to temporarily missing asynchronous image text enrichment.
- If semantic task enqueue fails inside the transaction, the entire file revision commit must fail. A visible revision without a durable task is not acceptable for this phase.

This gives Phase 2 a clear failure policy: SQL-visible file revisions and durable semantic registration are atomic together; object-store cleanup remains best-effort around that boundary.

## Compatibility and Invariants

- `PUT /v1/fs/{path}` and multipart completion keep their existing success semantics.
- Overwrite continues to mutate the same inode and increment `files.revision`.
- `embedding` and `embedding_revision` are cleared during any content mutation before new semantic work is processed.
- `embed` tasks are keyed by `task_type + resource_id + resource_version`, so the same revision can never accumulate duplicate durable tasks.
- Copy and rename remain semantic no-ops because they do not create a new content revision.
- Deleting a file does not synchronously cancel queued work; future workers must treat missing files or mismatched revisions as obsolete.

## Incremental Plan

### Step 1: Small-file create / overwrite

- switch `createAndWriteCtx` to `Store.InTx`
- use `InsertFileTx`, `EnsureParentDirsTx`, `InsertNodeTx`, and `EnqueueSemanticTaskTx`
- switch overwrite to `UpdateFileContentTx` + transactional embed enqueue
- add tests for create/overwrite task creation and stale embedding-state clearing

### Step 2: Multipart completion

- wire `ConfirmUpload` to the same tx-local semantic contract
- preserve inode identity on overwrite and refresh `content_type`
- add tests for new-file completion, overwrite, and upload-row rebinding

### Step 3: Image extract bridge

- add same-revision ensure logic after successful `UpdateFileSearchText`
- add tests for re-queue after terminal embed task and stale image result suppression

### Step 4: Regression pass

- rerun backend CRUD, upload, and image-extract suites
- verify no semantic task is created for copy/rename paths
- verify stale embedding state is cleared across overwrite transitions

## Validation Strategy

- **Backend write-path tests**
  - small text create enqueues exactly one revision-1 `embed` task
  - overwrite increments revision, clears `embedding_revision`, and enqueues exactly one task for the new revision
  - upload completion enqueues semantic work for the confirmed inode/revision when applicable
  - upload overwrite preserves inode identity and rebinds the upload row to the surviving inode
- **Image bridge tests**
  - successful image text writeback ensures the same revision's `embed` task exists
  - a terminal same-revision task can be re-queued after fresher async text arrives
  - stale image extract completion does not requeue an old revision
- **Datastore seam tests**
  - transactional enqueue rolls back with the outer transaction
  - tx-local content update clears `embedding` and `embedding_revision`
- **Behavioral regression**
  - copy / rename do not create semantic tasks
  - write failures do not leave visible revisions without a durable task record

## Risks and Mitigations

1. **Write-path refactor can break existing CRUD semantics** - Reuse the existing datastore tx helpers instead of inventing new write-path abstractions, and keep regression coverage on standard write/append/truncate flows.
2. **Overwrite can leave stale metadata or vectors visible** - Clear `embedding` / `embedding_revision` inside the same transaction and refresh `content_type` during multipart overwrite.
3. **Blob side effects can drift from SQL state on failure** - Keep object-store completion outside the SQL transaction, but add best-effort cleanup for newly written replacement blobs when the transaction fails.
4. **Image files can lose vector freshness after generated embedding removal** - Treat `UpdateFileSearchText` success -> ensure same-revision `embed` as a mandatory bridge rule, not a best-effort enhancement.
5. **Scope can leak into worker/search changes** - Keep Phase 2 focused on committed-write semantics only. Worker execution and query embedding remain separate later-phase concerns.
