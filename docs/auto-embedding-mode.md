# Proposal: TiDB Auto Embedding Mode for dat9

**Date**: 2026-03-31  
**Purpose**: Define a production-grade auto embedding mode for TiDB tenants. In this mode, the application continues to produce and maintain `files.content_text`, while the database derives document vectors from `content_text` and embeds query text on the database side during search.

## Summary

`dat9` should introduce a **database-managed auto embedding** mode for TiDB tenants as the launch schema baseline for **TiDB Cloud Starter** and **TiDB Cloud Zero**. In this mode:

- `files.content_text` is the canonical semantic text for retrieval
- the database derives `files.embedding` from `content_text`
- the search path uses database-side query embedding instead of app-side query embedding
- the current app-managed durable `embed` task and `embed` worker no longer own the `text -> vector` stage for TiDB tenants

The project has not launched yet, so the first rollout does not define online migration from app-managed schema to auto-embedding schema. Before launch, existing pre-release TiDB tenants may be rebuilt so that Starter and Zero share the same semantic baseline at launch. Backends outside the scope of this proposal continue to use the current app-managed path.

## Context

### Current State

Today, the semantic indexing pipeline in `dat9` is app-managed:

#### Current responsibilities of the `files` table

A row in the current `files` table does not represent only one notion of "content." It simultaneously carries raw-content location, searchable text representation, semantic vector state, and version/validity metadata. This proposal changes mainly the `content_text -> embedding` segment of that model, not the raw-content storage layer itself.

| Column | Current responsibility | Produced by | Directly used in search | Notes |
|---|---|---|---|---|
| `storage_type` | Identifies where raw file bytes are stored | Write/upload paths | No | Small TiDB files and large object-stored files share the same `files` table |
| `storage_ref` | Locates raw file content | Write/upload paths | No | Usually `inline` for DB-inline storage and a blob key for object storage |
| `content_blob` | Inline copy of raw file bytes | Small-file write path | No | Usually empty for large files |
| `content_type` | MIME/content type used to determine later handling | Detected at write time | Indirectly | Affects text extraction and async image handling |
| `content_text` | Text representation used for retrieval and semantic processing | Synchronous extraction or async text generation | Yes | Current FTS, keyword fallback, and embedding generation all depend on it |
| `embedding` | Vector representation of `content_text` | Currently generated asynchronously by the application | Yes | Currently a writable column, not a generated column |
| `revision` | Current file-content version | Incremented on content changes | Indirectly | Used to detect whether content has changed |
| `embedding_revision` | File revision that produced the stored vector | Embedding writeback path | Yes | Prevents stale vectors from leaking into a new revision |
| `size_bytes` | Raw content size | Write/upload paths | No | Raw-content metadata |
| `checksum_sha256` | Raw content checksum | Write/upload paths | No | Raw-content consistency metadata |
| `status` | File lifecycle state | Write/upload/delete paths | Yes | Search usually reads only `CONFIRMED` rows |
| `confirmed_at` | Confirmation timestamp | Confirm/write paths | Indirectly | Used by some fallback ordering |

#### How the current semantic pipeline uses those columns

- small text writes and overwrites extract text and write it to `files.content_text`
- large-file upload confirmation updates the `files` row and enqueues `embed` when semantic text is available
- async image extraction writes back `content_text` in a revision-gated way and then enqueues `embed`
- the background `semantic worker` consumes `embed` tasks, calls an embedding provider, and writes vectors back to `files.embedding`
- search runs FTS and vector search concurrently, and the vector path depends on app-side query embedding
- the current vector path relies on `embedding_revision = revision` to filter stale vectors

This design is already correct, but it means the application continues to own:

1. alignment between document embedding and query embedding
2. the lifecycle, retry, writeback, and observability of the `text -> vector` stage
3. the revision-aware vector-state model centered on `embedding_revision = revision`

The current TiDB tenant schema reflects this app-managed design as well: `files.embedding` is still a writable `VECTOR(1024)` column paired with `embedding_revision`, rather than a generated column derived from `content_text`.

### Verified TiDB Capabilities

TiDB Cloud has already been verified to provide the capability surface required for database-side auto embedding:

- `EMBED_TEXT(...)` can be used for a generated vector column
- `VEC_EMBED_COSINE_DISTANCE(...)` can be used for text-query vector ranking
- both Starter and Zero have been verified to support generated vector columns, vector indexes, and text-query search

This allows TiDB tenants to treat `content_text` as the source of truth and push the final `text -> vector` step into the database.

### Why Now

The key reason this design is viable now is that the project has not launched yet. Instead of designing a complex migration path for pre-release tenants that can still be rebuilt, it is smaller and safer to define a launch-time TiDB schema baseline and converge Starter and Zero before launch. That yields a clearer production boundary with less design complexity.

## Goals

1. Define `files.content_text` as the canonical semantic input for retrieval on TiDB tenants.
2. Push the TiDB tenant `text -> vector` stage into the database without redesigning the current `content_text` production paths.
3. Ensure Starter and Zero share the same semantic behavior boundary at launch.
4. Preserve the current high-level search behavior: FTS, vector ranking, RRF merge, and keyword fallback remain in place.
5. Reduce direct application ownership of embedding providers, worker writeback, and revision-aware vector state.

## Non-Goals

- Do not replace the current app-managed embedding pipeline for all backends.
- Do not design online migration, backfill, or rollback for already launched tenants.
- Do not include `db9` in the first auto-embedding rollout.
- Do not redesign the current async `content_text` production system, including durable image-extraction queues.
- Do not introduce a complex provider-neutral runtime abstraction in this proposal.
- Do not define multi-column semantic representations such as `overview_text`, `ocr_text`, or `full_text` here.

## Architecture Overview

```text
Current app-managed path
------------------------
write / upload / image_extract
    -> update content_text
    -> enqueue embed task
    -> embed worker calls embedding provider
    -> worker writes embedding + embedding_revision
    -> grep uses app-side query embedding

Target TiDB auto-embedding path
-------------------------------
write / upload / image_extract
    -> update content_text
    -> TiDB derives embedding from content_text
    -> grep uses DB-side query embedding
    -> no TiDB-owned embed task / embed worker stage
```

## Design

### 1. Scope and Rollout Boundary

This proposal covers only **TiDB Cloud Starter** and **TiDB Cloud Zero**.

Both deployment shapes should converge on the auto-embedding schema baseline before launch. The point here is not to preserve old schema behavior; it is to define the launch schema. Therefore:

- pre-release TiDB tenants may be rebuilt
- launch does not provide online migration from app-managed schema to auto-embedding schema
- non-TiDB backends continue to use the current app-managed design

### 2. `content_text` as the Canonical Semantic Source

In TiDB auto-embedding mode, the application still owns the production of `content_text`, while the database owns only the derived vector generation.

By file path:

- small text files: the write path synchronously writes `content_text`
- images: the current async `image -> text` path remains in place and writes back `content_text` in a revision-gated way
- other file types: if new text-production paths are added later, their contract remains writing into `content_text`

The design intent is explicit: `content_text` is semantic retrieval text. It is not required to be a full-fidelity textual replica of the raw content for every file type.

### 3. TiDB Schema Shape

The current TiDB schema starts from an app-managed writable-vector model: the application clears old vector state, the background worker writes back new vectors, and search relies on `embedding_revision = revision` to exclude stale state. This proposal replaces that column behavior.

For TiDB tenants, `files.embedding` should become a generated vector column derived from `content_text`. Conceptually:

```sql
embedding VECTOR(1024) GENERATED ALWAYS AS (
    EMBED_TEXT('<launch-model>', content_text)
) STORED
```

For launch, Starter and Zero only need to share one semantic model contract under the same launch baseline. To keep the design simple, this proposal does not require a complex tenant-level metadata system for that contract in the first step. It only requires:

- all TiDB tenants in the same launch baseline use the same model / dimensions / distance metric
- the query path and the generated column use the same semantic contract

`embedding_revision` no longer acts as a correctness gate in TiDB auto-embedding mode. It may remain as a compatibility field in the launch schema, but runtime behavior must no longer depend on it to determine vector validity.

### 4. Write Path Split

In TiDB auto-embedding mode, the write path is responsible for file metadata and `content_text`, not for vector-state management.

Required behavior:

- For small-file writes and overwrites:
  - update `content_text`
  - increment `revision`
  - do not clear `embedding`
  - do not write `embedding_revision`
  - do not enqueue `embed` tasks

- For large-file upload confirmation:
  - preserve current inode and storage semantics
  - allow `content_text` to remain empty if no synchronous text representation exists yet
  - stop enqueuing `embed` for TiDB tenants

- For async image extraction:
  - continue revision-gated updates to `content_text`
  - do not append a follow-up `embed` task after a successful update

This means TiDB auto-embedding mode must explicitly split the write semantics of current store helpers. It cannot keep reusing the assumption that a new revision clears embedding state first and a worker writes the vector back later.

### 5. Search Path Split

In TiDB auto-embedding mode, the high-level search behavior stays the same. Only the component that produces vectors changes.

Preserved behavior:

- FTS still reads `content_text`
- vector ranking still participates as a ranked signal in RRF merge
- keyword fallback remains the safety net when the ranked paths return no results

Changed behavior:

- app-side query embedding is no longer used
- query text participates in vector ranking directly inside the database

Conceptually:

```sql
ORDER BY VEC_EMBED_COSINE_DISTANCE(embedding, ?)
```

Degradation rules in TiDB auto-embedding mode:

1. if the DB-side vector path fails but a lexical path succeeds, return lexical results
2. do not silently fall back to app-side query embedding when the vector path fails
3. fail the whole search request only when every available path fails

This preserves API availability while keeping the mode boundary clear and avoiding reintroduction of document/query model drift.

### 6. Worker Behavior

TiDB auto-embedding mode does not eliminate all asynchronous work, but it does remove the durable `embed` stage for TiDB tenants.

That means:

- TiDB tenants no longer create `embed` tasks
- the `semantic worker` no longer owns `text -> vector` work for TiDB tenants
- existing `content_text` production paths such as image extraction remain unchanged

If the system still carries other app-managed backends during transition, the worker runtime may continue to exist. But it must no longer treat TiDB tenants as requiring `embed` writeback.

## Rollout Plan

### Phase 1: TiDB Schema Baseline

1. define the auto-embedding `files` schema for TiDB
2. confirm Starter and Zero use the same launch schema baseline
3. rebuild existing pre-release TiDB tenants before launch so they all land on the new baseline

### Phase 2: Runtime Split

1. adjust write paths so TiDB tenants no longer clear or write back embedding state
2. stop creating `embed` tasks for TiDB tenants
3. switch the TiDB tenant search path to DB-side query embedding
4. stop `semantic worker` handling of TiDB `embed` work

### Phase 3: Launch Validation

1. run separate end-to-end validation for Starter and Zero
2. validate that write / upload / image_extract paths all produce searchable results
3. validate ranked-path and degraded-path behavior in search
4. make the launch schema baseline a release gate

## Validation Strategy

- **Schema validation**
  - Starter and Zero both accept generated vector columns
  - vector indexes can be created and used by the search path

- **Write-path validation**
  - small text writes become semantically searchable without app-managed `embed`
  - upload confirmation no longer creates `embed` tasks for TiDB tenants
  - image extraction no longer requires an `embed` worker after writing back `content_text`

- **Search validation**
  - FTS, vector ranking, RRF merge, and keyword fallback all remain valid
  - query text and stored vectors use the same TiDB semantic model contract
  - when the DB-side vector path fails, requests can degrade to lexical paths but not to app-side query embedding

- **Operational validation**
  - TiDB tenants no longer depend on `embedding_revision = revision`
  - TiDB tenant `embed` queue depth should stay at zero
  - the `semantic worker` no longer processes TiDB `embed` writeback

## Risks and Mitigations

1. **Starter and Zero fail to converge on one schema baseline**
   - Mitigation: make the unified launch schema baseline a release gate instead of treating them as separate feature levels.

2. **DB-side vector-path failures reduce search quality**
   - Mitigation: preserve lexical paths and keyword fallback so availability takes priority over any single ranking path.

3. **Old app-managed tenants remain in pre-release environments and blur behavior boundaries**
   - Mitigation: explicitly rebuild and validate TiDB tenants before launch, and do not treat old schema as a supported launch state.

4. **`content_text` becomes overloaded semantically**
   - Mitigation: this proposal defines it only as canonical semantic retrieval text; if multiple representations are needed later, they can be added separately.

## Follow-up

| Priority | Topic | Problem to solve |
|---|---|---|
| P1 | `db9 Auto Embedding Runtime` | Complete schema, store open, query path, and runtime dispatch for db9 so it becomes an independently usable auto-embedding backend. |
| P1 | `Semantic Contract Metadata` | Introduce explicit metadata and runtime config when the system needs tenant-level mode, per-tenant model contracts, or stricter drift detection. |
| P2 | `Post-GA Migration and Compatibility` | If old app-managed tenants must be supported after GA, design migration, backfill, dual-schema coexistence, and rollback separately. |
| P2 | `Async Text-Production Evolution` | Make image extraction durable, add new text-production tasks, or introduce asynchronous semantic artifacts such as L0/L1/overview. |
| P3 | `Multi-Representation Retrieval Text` | When one `content_text` column is no longer sufficient, design multiple semantic representations such as `overview_text`, `ocr_text`, and `full_text`. |

## Open Questions

The reduced proposal leaves no open question that blocks the launch boundary. Items intentionally excluded from the main body are listed explicitly in `Follow-up` with priorities.

## References

- `database-auto-embedding/database-managed-auto-embedding-mode.zh.md`
- `dat9-2/pkg/backend/dat9.go`
- `dat9-2/pkg/backend/upload.go`
- `dat9-2/pkg/backend/image_extract.go`
- `dat9-2/pkg/datastore/search.go`
- `dat9-2/pkg/datastore/file_tx.go`
- `dat9-2/pkg/server/semantic_worker.go`
- `dat9-2/pkg/tenant/schema_zero.go`
- `dat9-2/pkg/tenant/starter.go`
