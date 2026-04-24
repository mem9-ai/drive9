**Status**: Draft
**Date**: 2026-04-23

# File Description Design Doc

> Goal: Introduce a `description` field for drive9 files, support carrying it via CLI at write time, enforce a length limit, and embed the description independently to enable semantic recall for large files and non-text files.

## Background & Motivation

Currently, drive9's semantic recall relies entirely on the `files.content_text` → `files.embedding` pipeline. For large files or non-text files (images, audio, video, binaries, etc.), `content_text` is often empty or extraction fails, so no embedding is generated and the file cannot be recalled via semantic search.

By introducing `description`, users can attach a human- or machine-generated descriptive text to a file at upload time. This description follows its own embedding pipeline and participates in semantic search, filling the recall gap described above.

## Design Principles

1. **Minimal Intrusion**: Reuse the existing embedding task queue, semantic worker, and search RRF framework as much as possible.
2. **Backward Compatibility**: `description` is nullable across the entire pipeline; old files and old clients behave unchanged when no description is present.
3. **Decoupled from `content_text` embedding**: `description` uses a separate vector column to avoid cross-contamination and to allow tracking revision independently.
4. **Unified Entry Point**: The CLI `--description` flag is provided on the `cp` command, covering both the small-file direct-write path and the large-file multipart upload path.

---

## 1. Data Model Changes

### 1.1 `files` Table (All Schema Providers)

Add three new columns:

```sql
ALTER TABLE files
    ADD COLUMN description            TEXT,
    ADD COLUMN description_embedding  VECTOR(1024),
    ADD COLUMN description_embedding_revision BIGINT;
```

| Column | Type | Description |
|---|---|---|
| `description` | `TEXT` | User-provided file description; length limit enforced at the application layer (recommended 2000 characters). |
| `description_embedding` | `VECTOR(1024)` | Embedding vector for the description. Written by the worker in app-managed mode; a generated column in auto-embedding mode. |
| `description_embedding_revision` | `BIGINT` | Records which file revision the `description_embedding` corresponds to, used for concurrency safety and idempotency. |

**Schema Provider-Specific Differences**

- **`pkg/tenant/schema/tidb_app.go`** (App-managed embedding):
  - `description_embedding VECTOR(1024)` is a regular writable column.
  - `description_embedding_revision BIGINT` is a regular writable column.

- **`pkg/tenant/schema/tidb_auto.go`** (TiDB Auto-embedding):
  - `description_embedding` is a generated column:
    ```sql
    description_embedding VECTOR(1024) GENERATED ALWAYS AS (
        EMBED_TEXT('tidbcloud_free/amazon/titan-embed-text-v2', description, '{"dimensions":1024}')
    ) STORED
    ```
  - `description_embedding_revision` remains a regular column (a worker or trigger may update it, but in TiDB auto mode the vector is automatically recalculated when description changes; this column is kept mainly for schema consistency with app-managed mode and may be left unwritten for now).

- **`pkg/tenant/schema/db9/schema.go`** (PostgreSQL / pgvector):
  - `description TEXT`
  - `description_embedding vector(1024)`
  - `description_embedding_revision BIGINT`
  - Also add an `hnsw` index in `db9` (consistent with the existing `embedding` column):
    ```sql
    CREATE INDEX idx_files_desc_embedding ON files USING hnsw (description_embedding vector_cosine_ops);
    ```

### 1.2 `File` struct (`pkg/datastore/store.go`)

```go
type File struct {
    FileID         string
    StorageType    StorageType
    // ... existing fields ...
    ContentText    string
    Description    string          // new
    EmbeddingRevision       *int64 // existing: revision of the content_text embedding
    DescriptionEmbeddingRevision *int64 // new
    // ...
}
```

> **Note**: The current `File` struct does not store the `embedding` vector value (`GetFile` reads via `scanFileWithBlob` but does not include embedding). To remain consistent with existing style, `File` does not add a `DescriptionEmbedding []float32` field directly; instead, only `Description string` and `DescriptionEmbeddingRevision *int64` are added.

### 1.3 `uploads` Table

Large-file multipart uploads need to temporarily hold the description between initiate and finalize. Add to the `uploads` table:

```sql
ALTER TABLE uploads ADD COLUMN description TEXT;
```

The corresponding `Upload` struct, CLI initiate request, and server handler must all be updated accordingly.

---

## 2. CLI Changes

### 2.1 `drive9 fs cp --description "..."`

Add `--description` to the `cp` command flag-parsing loop:

```go
// cmd/drive9/cli/cp.go
var description string
filtered := args[:0]
for i := 0; i < len(args); i++ {
    switch args[i] {
    case "--description":
        if i+1 >= len(args) {
            return fmt.Errorf("--description requires argument")
        }
        i++
        description = args[i]
    // ... existing --resume / --append ...
    }
}
```

**Length validation** (fail early on the CLI side):

```go
const MaxDescriptionLen = 2000
if len(description) > MaxDescriptionLen {
    return fmt.Errorf("description exceeds %d characters", MaxDescriptionLen)
}
```

### 2.2 Client SDK (`pkg/client/client.go` & `transfer.go`)

Existing upload/write function signatures need to be extended to carry `description`. To avoid signature explosion, the cleanest approach is a **functional option** pattern:

```go
// pkg/client/transfer.go

type WriteOption func(*writeOptions)

type writeOptions struct {
    description      string
    expectedRevision int64
}

func WithDescription(desc string) WriteOption {
    return func(o *writeOptions) {
        o.description = desc
    }
}

func WithExpectedRevision(rev int64) WriteOption {
    return func(o *writeOptions) {
        o.expectedRevision = rev
    }
}
```

Then refactor the following methods to variadic options:

```go
func (c *Client) WriteStreamWithSummary(ctx context.Context, path string, r io.Reader, size int64, progress ProgressFunc, opts ...WriteOption) (*UploadSummary, error)
func (c *Client) WriteCtxConditional(ctx context.Context, path string, data []byte, opts ...WriteOption) error
func (c *Client) ResumeUploadWithSummary(ctx context.Context, path string, r io.Reader, size int64, progress ProgressFunc, opts ...WriteOption) (*UploadSummary, error)
func (c *Client) AppendStream(ctx context.Context, path string, r io.Reader, size int64, progress ProgressFunc, opts ...WriteOption) error
```

**Backward compatibility**: All existing call sites can migrate transparently by simply adding `...` at the end.

**Small-file direct PUT** (`WriteCtxConditional`): Pass description via the `X-Dat9-Description` header. Because description has a 2000-character limit, the header is safe (well below typical 8KB–16KB header limits).

**Large-file initiate** (v1 & v2): Add a `description` field to the initiate request JSON body:

```go
type uploadInitiateRequest struct {
    Path             string   `json:"path"`
    TotalSize        int64    `json:"total_size"`
    PartChecksums    []string `json:"part_checksums,omitempty"`
    ExpectedRevision *int64   `json:"expected_revision,omitempty"`
    Description      string   `json:"description,omitempty"`  // new
}
```

The v2 initiate body also adds `description`.

---

## 3. Server Handler Changes

### 3.1 `handleWrite` (`pkg/server/server.go`)

- Read the `X-Dat9-Description` header (small-file path).
- Read `Description` from the initiate body (large-file path).
- Validate length: `if len(description) > MaxDescriptionLen { return 400 }`.
- Small file: pass description through to `b.WriteCtxIfRevision(...)` (backend interface needs extension).
- Large file: store description together with the upload plan in the `uploads` table (passed through `b.InitiateUploadWithChecksumsIfRevision(...)`).

### 3.2 `handleAppend` / `handleCopy`

- `append` does not change the description (content and description are decoupled; appending bytes should not affect semantic description).
- `copy` target **inherits the source file's description** by default (because copy semantically clones the file). If future support for overriding is needed, it can be extended via a copy header, but this design does not cover that.

---

## 4. Backend Changes (`pkg/backend/dat9.go` & `upload.go`)

### 4.1 Small-File Write

Extend the `WriteCtxIfRevision` signature (or internally use an options struct):

```go
func (b *Dat9Backend) WriteCtxIfRevision(
    ctx context.Context,
    path string, data []byte, offset int64,
    flags filesystem.WriteFlag,
    expectedRevision int64,
    description string, // new
) (n int64, err error)
```

- `createAndWriteCtx`: write `Description: description` during `InsertFileTx`.
- `overwriteFileCtx`:
  - If `description != ""`, update to the new description and set `description_embedding = NULL, description_embedding_revision = NULL` (trigger re-embedding).
  - If `description == ""`, keep the old description and its embedding (only content updated, semantic description unchanged).

### 4.2 Large-File Multipart Upload

- `InitiateUploadWithChecksumsIfRevision` / `InitiateUploadV2IfRevision`: add a `description string` parameter and write it to the `description` column of the `uploads` table.
- `finalizeUpload`: in the transaction, when confirming the file from `uploads` to `files`, write `upload.Description` into `files.description`; if this is an overwrite, apply the same old-description handling rule described above.

### 4.3 New / Modified Datastore Methods

In `pkg/datastore/file_tx.go`:

```go
// UpdateFileDescriptionTx updates description independently (for a future PATCH API)
func (s *Store) UpdateFileDescriptionTx(db execer, fileID string, description string) error
```

In `pkg/datastore/store.go`, update `InsertFile` / `GetFile` / scan methods to include `description` and `description_embedding_revision`.

---

## 5. Embedding & Semantic Worker

### 5.1 Task Enqueue Strategy

Modify `shouldEnqueueEmbedForRevision` in `pkg/backend/semantic_tasks.go`:

```go
func (b *Dat9Backend) shouldEnqueueEmbedForRevision(path, contentType, contentText, description string) bool {
    if strings.TrimSpace(contentText) != "" {
        return true
    }
    if strings.TrimSpace(description) != "" {
        return true
    }
    return b.hasAsyncImageTextSource(path, contentType)
}
```

Pass `description` when calling from `createAndWriteCtx`, `overwriteFileCtx`, and `finalizeUpload`.

### 5.2 Worker Processing (`pkg/server/semantic_worker.go`)

Modify `processEmbedTask` so that a single embed task handles both the `content_text` and `description` vectors:

```go
func (m *semanticWorkerManager) processEmbedTask(ctx context.Context, store *datastore.Store, task *semantic.Task) semanticTaskOutcome {
    file, err := store.GetFile(ctx, task.ResourceID)
    // ... stale check ...

    // 1. ContentText embedding (existing logic)
    if strings.TrimSpace(file.ContentText) != "" && file.Revision == task.ResourceVersion {
        vec, err := m.embedder.EmbedText(ctx, file.ContentText)
        // ... writeback to files.embedding ...
        updated1, err := store.UpdateFileEmbedding(ctx, task.ResourceID, task.ResourceVersion, vec)
    }

    // 2. Description embedding (new)
    if strings.TrimSpace(file.Description) != "" && file.Revision == task.ResourceVersion {
        vecDesc, err := m.embedder.EmbedText(ctx, file.Description)
        if err != nil {
            return semanticTaskOutcome{action: semanticTaskActionRetry, ...}
        }
        updated2, err := store.UpdateFileDescriptionEmbedding(ctx, task.ResourceID, task.ResourceVersion, vecDesc)
    }

    // ack if at least one succeeded or both are obsolete
}
```

Add `UpdateFileDescriptionEmbedding`:

```go
// pkg/datastore/embedding_writeback.go
func (s *Store) UpdateFileDescriptionEmbedding(ctx context.Context, fileID string, revision int64, vector []float32) (updated bool, err error) {
    res, err := s.db.ExecContext(ctx,
        `UPDATE files SET description_embedding = ?, description_embedding_revision = ?
         WHERE file_id = ? AND revision = ? AND status = 'CONFIRMED'`,
        embedding.FormatVector(vector), revision, fileID, revision)
    // ... same pattern as UpdateFileEmbedding ...
}
```

**Why reuse the same `TaskTypeEmbed` instead of creating a new `TaskTypeEmbedDescription`?**
- Reduces task count: one file create/overwrite produces only one task, and the worker generates both content and description vectors in one go.
- Simplifies enqueue logic: no need to decide whether content_text or description is non-empty to enqueue separately.
- If embedding service calls have cost, they can still ack/retry independently (see 5.3).

### 5.3 Failure Handling

If a file has both content_text and description, but the content embedding succeeds while the description embedding fails (or vice versa), the current design causes the entire task to retry, resulting in the content embedding being written again (idempotent, no harm). If finer granularity is desired:

- Split into two independent tasks (`TaskTypeEmbed` and `TaskTypeEmbedDescription`), each with its own retry/ack lifecycle.
- This design recommends keeping a single task for simplicity; if later observation shows that description embedding fails significantly more often than content, then split.

---

## 6. Search / Semantic Recall

### 6.1 Existing Recall Pipeline

`backend.Grep` currently runs in parallel:
1. `FTSSearch(query, pathPrefix, fetch)` — full-text search over `content_text`.
2. `VectorSearch(...)` (App-managed) or `VectorSearchByText(...)` (Auto-embedding) — over `files.embedding`.
3. `RRFMerge(fts, vec, limit)` — fused ranking.

### 6.2 Extended Dual-Vector Recall

Add `VectorSearchDescription` and `VectorSearchDescriptionByText`, both based on `files.description_embedding`.

`backend.Grep` becomes three-way parallel (or two vector streams merged before RRF with FTS):

```go
func (b *Dat9Backend) Grep(ctx context.Context, query, pathPrefix string, limit int) ([]datastore.SearchResult, error) {
    // ...
    ftsCh := asyncFTSSearch(...)
    vecCh := asyncVectorSearch(...)      // content embedding
    vecDescCh := asyncVectorSearchDescription(...) // description embedding

    fts := <-ftsCh
    vec := <-vecCh
    vecDesc := <-vecDescCh

    // Merge the two vector results first: for the same file, keep the smallest distance (highest similarity)
    mergedVec := mergeVectorResults(vec, vecDesc)

    return RRFMerge(fts, mergedVec, limit), nil
}
```

`mergeVectorResults` logic (in Go, avoiding complex SQL):

```go
func mergeVectorResults(a, b []datastore.SearchResult) []datastore.SearchResult {
    best := make(map[string]datastore.SearchResult)
    for _, r := range a {
        best[r.Path] = r
    }
    for _, r := range b {
        if existing, ok := best[r.Path]; ok {
            if *r.Score > *existing.Score { // score = 1 - distance, larger is more similar
                best[r.Path] = r
            }
        } else {
            best[r.Path] = r
        }
    }
    out := make([]datastore.SearchResult, 0, len(best))
    for _, r := range best {
        out = append(out, r)
    }
    sort.Slice(out, func(i, j int) bool {
        return *out[i].Score > *out[j].Score
    })
    return out
}
```

**SQL Layer Implementation (`pkg/datastore/search.go`)**

```go
func (s *Store) VectorSearchDescription(ctx context.Context, queryEmbedding []float32, pathPrefix string, limit int) ([]SearchResult, error) {
    q, args, ok := buildVectorSearchDescriptionQuery(queryEmbedding, pathPrefix, limit)
    // ... same scan pattern as VectorSearch ...
}

func buildVectorSearchDescriptionQuery(queryEmbedding []float32, pathPrefix string, limit int) (string, []any, bool) {
    if len(queryEmbedding) == 0 {
        return "", nil, false
    }
    conds := []string{"f.status = 'CONFIRMED'", "f.description_embedding IS NOT NULL", "f.description_embedding_revision = f.revision"}
    vecParam := embedding.FormatVector(queryEmbedding)
    args := []any{vecParam, vecParam, limit} // two vecParam for SELECT and ORDER BY
    // ... pathPrefix cond ...
    q := `SELECT fn.path, fn.name, f.size_bytes,
              VEC_EMBED_COSINE_DISTANCE(f.description_embedding, ?) AS distance
          FROM file_nodes fn JOIN files f ON fn.file_id = f.file_id
          WHERE ` + strings.Join(conds, " AND ") + `
          ORDER BY VEC_EMBED_COSINE_DISTANCE(f.description_embedding, ?)
          LIMIT ?`
    return q, args, true
}
```

For auto-embedding mode, `VectorSearchDescriptionByText` is analogous, using `EMBED_TEXT` as the query vector.

### 6.3 Should FTS Cover Description?

Currently `FTSSearch` only searches `content_text`. To include description in full-text search, there are two options:

- **Option A (Recommended)**: Combine `content_text` and `description` in `FTSSearch`. TiDB's `fts_match_word` supports FULLTEXT INDEX on multiple columns, but the current schema only has `idx_fts_content(content_text)`. It can be changed to:
  ```sql
  ALTER TABLE files
      DROP FULLTEXT INDEX idx_fts_content,
      ADD FULLTEXT INDEX idx_fts_content_desc(content_text, description);
  ```
  Then change the `fts_match_word` target in `FTSSearch` to `(content_text, description)`.
- **Option B**: Keep the status quo; description only participates in vector recall, not FTS. Because descriptions are usually human-written summaries, vector semantic matching is often more suitable than keyword matching.

**This design recommends Option A**, allowing description to participate in both FTS and vector recall to maximize recall. However, the schema change must be synchronized across all providers.

---

## 7. Checklist

### Schema
- [ ] `pkg/tenant/schema/tidb_app.go` — add `description`, `description_embedding`, `description_embedding_revision` to `files`; add `description` to `uploads`.
- [ ] `pkg/tenant/schema/tidb_auto.go` — same as above, but `description_embedding` is a generated column.
- [ ] `pkg/tenant/schema/db9/schema.go` — same as above, plus `hnsw` index; extend FULLTEXT index to `(content_text, description)`.
- [ ] Run `drive9-server schema dump-init-sql --provider tidb_cloud_starter` and update externally managed schema.

### Datastore
- [ ] `pkg/datastore/store.go` — add fields to `File` struct; update `InsertFile` / `GetFile` / scan methods.
- [ ] `pkg/datastore/store.go` — add `Description string` to `Upload` struct.
- [ ] `pkg/datastore/file_tx.go` — update `InsertFileTx` / `UpdateFileContentTx` family; add `UpdateFileDescriptionTx`.
- [ ] `pkg/datastore/embedding_writeback.go` — add `UpdateFileDescriptionEmbedding`.
- [ ] `pkg/datastore/search.go` — add `VectorSearchDescription` / `VectorSearchDescriptionByText`; optionally extend `FTSSearch` to cover description.

### Backend
- [ ] `pkg/backend/dat9.go` — extend `WriteCtxIfRevision` / `createAndWriteCtx` / `overwriteFileCtx` with description parameter.
- [ ] `pkg/backend/upload.go` — extend `InitiateUpload...` family with description; `finalizeUpload` writes `files.description`.
- [ ] `pkg/backend/semantic_tasks.go` — extend `shouldEnqueueEmbedForRevision` with description check; update enqueue logic.
- [ ] `pkg/backend/dat9.go` — add description vector recall path to `Grep`.

### Server
- [ ] `pkg/server/server.go` — `handleWrite` reads `X-Dat9-Description` / description from initiate body, validates length, and passes through to backend.
- [ ] `pkg/server/server.go` — `handleCopy` copies source file description (implemented in `CopyFileCtx`).
- [ ] `pkg/server/semantic_worker.go` — `processEmbedTask` also generates description embedding.

### Client SDK
- [ ] `pkg/client/transfer.go` / `client.go` — introduce `WriteOption` mechanism; refactor `WriteStreamWithSummary`, `WriteCtxConditional`, `ResumeUploadWithSummary`, `AppendStream` to variadic options; pass description through each protocol path.
- [ ] `pkg/client/transfer.go` — add `Description` to initiate request structs.

### CLI
- [ ] `cmd/drive9/cli/cp.go` — parse `--description` flag, validate length, pass to client.
- [ ] `cmd/drive9/cli/find.go` — optional: add `-description` filter (similar to `-tag`), filtering by description LIKE or FTS (if Option A is implemented).

### Tests
- [ ] `pkg/datastore/*_test.go` — CRUD tests with the new description column.
- [ ] `pkg/server/*_test.go` — test PUT with `X-Dat9-Description`, initiate upload with description, description embedding writeback.
- [ ] `pkg/backend/*_test.go` — test large-file finalize with description, semantic search recalling description.
- [ ] E2E: add `cp --description` flow verification to `e2e/cli-smoke-test.sh`.

---

## 8. Edge Cases

| Scenario | Behavior |
|---|---|
| Description exceeds length limit | CLI returns an error immediately; server returns 400 Bad Request. |
| Old client does not write description | Description is empty; all existing logic remains unchanged. |
| Overwrite with a new description | Overwrites old description, clears `description_embedding` and `description_embedding_revision`, and re-enqueues embed task. |
| Overwrite without a description | Keeps old description and its embedding (content-only update). |
| `content_text` empty but description non-empty | `shouldEnqueueEmbedForRevision` returns true; semantic worker generates only `description_embedding`. |
| Both `content_text` and description non-empty | Worker generates both vectors in a single task. |
| Description modified in auto-embedding mode | TiDB automatically recalculates `description_embedding`; `description_embedding_revision` can be ignored or updated by trigger. |
| Copy file | Target file inherits source file's description and description_embedding. |
| Rename / Move file | Description moves with file_id; no change needed. |

---

## 9. Future Extensions

1. **Standalone PATCH description API**: Later support `PATCH /v1/fs/{path}?description=...` to update description without modifying file content.
2. **AI-generated description**: The semantic worker could, after `img_extract_text` / `audio_extract_text`, call an LLM to generate a description, write it back to `files.description`, and trigger embedding.
3. **Markdown / Rich Text support for descriptions**: The current design uses plain TEXT; if structured content is needed in the future, it can be migrated to `description_json JSON`.


---

## 10. Integration Testing on TiDB Cloud

Local TiDB (tiup playground / Docker) does **not** support `VEC_COSINE_DISTANCE`, `fts_match_word`, or `EMBED_TEXT`, so the vector and full-text recall paths cannot be validated locally. To verify that description recall works end-to-end, run the following against a **TiDB Cloud Serverless** instance that has VECTOR and FTS enabled.

### Prerequisites

- A TiDB Cloud Serverless cluster with VECTOR and FULLTEXT support.
- An embedding provider (e.g., OpenAI, Ollama) reachable from the test machine.

### Steps

1. **Provision the tenant schema** (auto-embedding or app-embedding mode):
   ```bash
   export DRIVE9_BASE="https://<your-drive9-server>/v1"
   export DRIVE9_API_KEY="<tenant-api-key>"
   ```

2. **Upload a file with a description**:
   ```bash
   drive9 cp --description "quarterly financial report Q3 2024" ./report.pdf :/reports/q3.pdf
   ```

3. **Wait for embedding writeback** (app-managed mode) or verify auto-embedding (TiDB auto mode):
   ```bash
   drive9 stat :/reports/q3.pdf
   # Confirm description_embedding_revision matches revision.
   ```

4. **Test keyword recall via description**:
   ```bash
   drive9 grep "financial report"
   # Expected: /reports/q3.pdf appears in results even though content_text is empty (binary file).
   ```

5. **Test semantic recall via description**:
   ```bash
   drive9 grep "company earnings overview"
   # Expected: /reports/q3.pdf appears due to semantic similarity of description_embedding.
   ```

6. **Test overwrite semantics**:
   ```bash
   drive9 cp --description "updated Q3 report" ./report-v2.pdf :/reports/q3.pdf
   drive9 grep "quarterly financial report"
   # Expected: old description no longer matches; new description matches.
   ```

### CI Integration

For automated CI, point `e2e/verify-description-e2e.sh` at a TiDB Cloud test instance:

```bash
export DRIVE9_BASE="https://drive9-e2e.example.com/v1"
export DRIVE9_LOCAL_DSN="<tidb-cloud-dsn>"
export DRIVE9_LOCAL_INIT_SCHEMA=true
export DRIVE9_EMBED_API_BASE="https://api.openai.com/v1"
export DRIVE9_EMBED_API_KEY="<openai-key>"
export DRIVE9_EMBED_MODEL="text-embedding-3-small"
export DRIVE9_EMBED_DIMENSIONS=1024
bash e2e/verify-description-e2e.sh
```

> Note: Remove or adjust the "known limitation" skip in the Grep test section when running against TiDB Cloud.
