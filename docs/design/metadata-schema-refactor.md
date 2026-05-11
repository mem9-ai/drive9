# drive9 Metadata Schema Refactor: Dentry / Inode / Content / Semantic Split

## 1. Motivation

drive9's current `files` table is a monolith that mixes three unrelated concerns:

1. **POSIX inode metadata** — `size_bytes`, `revision`, `status`, timestamps
2. **Storage blob metadata** — `storage_type`, `storage_ref`, `content_blob`, `checksum`
3. **Semantic search data** — `content_text`, `description`, `embedding` vectors

This causes three problems:

- **Hardlink semantics are broken**. drive9 already supports copy-on-write hardlinks (`CopyFile` creates multiple `file_nodes` rows pointing to the same `file_id`). In POSIX, all hardlinks share the same inode and therefore the same `mode`, `size`, `mtime`. If these attributes live in `file_nodes` (the dentry table), two hardlinks could have different modes — violating POSIX.
- **Listing performance is poor**. `ls` / `ReadDir` currently fetches the entire `files` row, including `content_blob` and embedding vectors. There is an explicit TODO(#110) about this.
- **Future extensibility is blocked**. The semantic layer (embeddings, full-text search) cannot be moved to a dedicated vector database because it is locked inside the core file table.

We refactor the schema into four tables:

| Table | Responsibility | Analogy |
|-------|---------------|---------|
| `file_nodes` | Dentry: path → inode mapping | ext4 directory entry |
| `inodes` | POSIX inode metadata: mode, size, revision, status, timestamps | ext4 inode table |
| `contents` | Storage blob metadata: storage ref, inline blob, checksum, encryption | ext4 extent/block map |
| `semantic` | Search & enrichment: text, description, embeddings | external index |

## 2. Schema Design

### 2.1 `file_nodes` (Dentry)

Renamed `file_id` → `inode_id` to make the relationship explicit.

```sql
CREATE TABLE IF NOT EXISTS file_nodes (
    node_id      VARCHAR(64) PRIMARY KEY,
    path         VARCHAR(512) NOT NULL,
    parent_path  VARCHAR(512) NOT NULL,
    name         VARCHAR(255) NOT NULL,
    is_directory BOOLEAN NOT NULL DEFAULT FALSE,
    inode_id     VARCHAR(64),           -- was file_id
    created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);
CREATE UNIQUE INDEX idx_path ON file_nodes(path);
CREATE INDEX idx_parent ON file_nodes(parent_path);
CREATE INDEX idx_inode_id ON file_nodes(inode_id);   -- was idx_file_id
```

### 2.2 `inodes` (POSIX Inode Metadata)

```sql
CREATE TABLE IF NOT EXISTS inodes (
    inode_id     VARCHAR(64) PRIMARY KEY,
    size_bytes   BIGINT NOT NULL DEFAULT 0,
    revision     BIGINT NOT NULL DEFAULT 1,
    mode         INT UNSIGNED NOT NULL DEFAULT 420,  -- decimal 420 = octal 0644
    status       VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    mtime        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),  -- last modification time
    confirmed_at DATETIME(3),  -- first confirmation time (may differ from mtime)
    expires_at   DATETIME(3)
);
CREATE INDEX idx_status ON inodes(status, created_at);
```

**`mode` stores permission bits only** (low 12 bits: setuid/setgid/sticky + `rwxrwxrwx`). File-type bits (`S_IFREG`, `S_IFDIR`) are **not** stored; they are derived from `file_nodes.is_directory` at read time.

**`mtime` semantics:** `mtime` is updated on every content overwrite (alongside `size_bytes` and `revision` in the `UPDATE inodes` transaction). It is the POSIX "last modification time". `confirmed_at` retains its original meaning: "when the file was first confirmed." For directories, `mtime` is updated on `Mkdir` and `chmod`.

### 2.3 `contents` (Storage Blob Metadata)

```sql
CREATE TABLE IF NOT EXISTS contents (
    inode_id                   VARCHAR(64) PRIMARY KEY,
    storage_type               VARCHAR(32) NOT NULL,
    storage_ref                TEXT NOT NULL,
    storage_encryption_mode    VARCHAR(16) NOT NULL DEFAULT 'legacy',
    storage_encryption_key_id  VARCHAR(256) NOT NULL DEFAULT '',
    content_blob               LONGBLOB,            -- BYTEA for db9
    content_type               VARCHAR(255),
    checksum_sha256            VARCHAR(128),
    source_id                  VARCHAR(255)
);
```

Directories do **not** have a `contents` row.

### 2.4 `semantic` (Search & Enrichment)

```sql
CREATE TABLE IF NOT EXISTS semantic (
    inode_id                           VARCHAR(64) PRIMARY KEY,
    content_text                       LONGTEXT,      -- TEXT for db9
    description                        LONGTEXT,
    embedding                          VECTOR(1024),  -- or app-managed
    embedding_revision                 BIGINT,
    description_embedding              VECTOR(1024),
    description_embedding_revision     BIGINT
);
-- Provider-specific indexes defined below
```

Directories do **not** have a `semantic` row.

### 2.5 Foreign-Key Tables (Rename `file_id` → `inode_id`)

```sql
CREATE TABLE IF NOT EXISTS file_tags (
    inode_id   VARCHAR(64) NOT NULL,   -- was file_id
    tag_key    VARCHAR(255) NOT NULL,
    tag_value  VARCHAR(255) NOT NULL DEFAULT '',
    PRIMARY KEY (inode_id, tag_key)
);
CREATE INDEX idx_kv ON file_tags(tag_key, tag_value);

CREATE TABLE IF NOT EXISTS uploads (
    upload_id  VARCHAR(64) PRIMARY KEY,
    inode_id   VARCHAR(64) NOT NULL,   -- was file_id
    -- ... rest unchanged
);

CREATE TABLE IF NOT EXISTS file_gc_tasks (
    task_id  VARCHAR(64) PRIMARY KEY,
    inode_id VARCHAR(64) NOT NULL,    -- was file_id
    -- ... rest unchanged
);
CREATE UNIQUE INDEX uk_file_gc_inode_id ON file_gc_tasks(inode_id);
```

**`semantic_tasks.resource_id`**: The column name stays `resource_id`, but its semantics change from "file_id" to "inode_id". The task payload already refers to inode-level resources (embeddings are per-inode, not per-dentry), so the semantic alignment is natural. No column rename is needed; only the Go struct comment and documentation are updated.

### 2.6 Column Assignment (Old → New)

| Old `files` column | New table | Reason |
|--------------------|-----------|--------|
| `file_id` | `inodes.inode_id` | Identity |
| `size_bytes` | `inodes` | POSIX stat |
| `revision` | `inodes` | Optimistic locking |
| `mode` | `inodes` | POSIX permissions |
| `status` | `inodes` | Lifecycle |
| `created_at`, `mtime`, `confirmed_at`, `expires_at` | `inodes` | Timestamps |
| `storage_type`, `storage_ref` | `contents` | Blob reference |
| `storage_encryption_mode`, `storage_encryption_key_id` | `contents` | Encryption |
| `content_blob` | `contents` | Inline payload |
| `content_type` | `contents` | MIME type |
| `checksum_sha256` | `contents` | Integrity |
| `source_id` | `contents` | Provenance |
| `content_text`, `description` | `semantic` | Search text |
| `embedding`, `embedding_revision` | `semantic` | Content vector |
| `description_embedding`, `description_embedding_revision` | `semantic` | Description vector |

### 2.7 Provider-Specific Index Notes

**TiDB (`tidb_app.go` / `tidb_auto.go`):**
- `semantic` table hosts `FULLTEXT INDEX idx_fts_content(content_text)` and `idx_fts_description(description)`.
- `tidb_auto.go`: `embedding` and `description_embedding` are `GENERATED ALWAYS AS (EMBED_TEXT(..., content_text/description, ...)) STORED`. Generated columns must stay in the same table as their source columns, so they move to `semantic` alongside `content_text` and `description`.

**⚠️ TiDB GENERATED column migration risk:**
When migrating with `INSERT INTO semantic SELECT ... FROM files`, TiDB **may re-evaluate** `GENERATED ALWAYS AS` expressions for every row. This means:
1. Migration time becomes proportional to file count × embedding model latency
2. Significant LLM inference cost if the model is remote (e.g., AWS Bedrock)

**Mitigation:** Before running the migration, verify TiDB behavior with:
```sql
-- Test on a single row first
INSERT INTO semantic (inode_id, content_text, description)
SELECT file_id, content_text, description FROM files LIMIT 1;
-- Check whether embedding/description_embedding were auto-generated
SELECT embedding, description_embedding FROM semantic;
```
If TiDB re-computes embeddings during INSERT ... SELECT, the migration should instead:
1. `INSERT INTO semantic` with all columns including the pre-computed embedding values copied from `files`
2. Skip the generated column by explicitly listing all non-generated columns

**db9 (PostgreSQL):**
- `semantic` table hosts `hnsw` vector indexes on `embedding` and `description_embedding`.
- `semantic` table hosts `gin(to_tsvector('simple', coalesce(content_text,'')))` for full-text search.
- db9 has no generated columns, so migration is a straight column copy with no re-computation risk.

## 3. POSIX Permissions (Mode)

This section describes how `mode` fits into the inode-level architecture.

### 3.1 Goals

- `chmod`, `ls -l` work correctly in FUSE
- `git`, `tar`, `rsync -a` toolchain compatibility
- Backward-compatible defaults for existing data

### 3.2 What's In Scope (Phase 1)

- **Only `mode`** — no `uid/gid`, no `chown`
- drive9 uses API Key / Capability Token auth, not Unix UID/GID authentication. Per-file owner fields have no enforcement value and add pure cross-layer cost.
- No ACL / `setfacl` / `getfacl`
- `sticky bit` / `setuid` / `setgid` bits are stored but not semantically enforced

### 3.3 Default Values

| Scenario | mode (permission bits) |
|----------|------------------------|
| Existing files (migration) | `0644` (decimal `420`) |
| Existing directories (migration) | `0755` (decimal `493`) |
| New file (FUSE Create) | `0666 & ~umask` |
| New directory (FUSE Mkdir) | `0777 & ~umask` |
| New file (HTTP API/CLI) | `0644` unless `X-Dat9-Mode` header overrides |

### 3.4 FUSE Pending-Create State Machine

drive9's FUSE layer has a write-back / pending-index architecture for new files:

1. **`Create`** — captures `CreateIn.Mode` into `InodeEntry.Mode`
2. **`SetAttr(FATTR_MODE)`** on a pending file:
   - Updates local `InodeEntry.Mode` only
   - Does **not** send a remote RPC (server does not know the file yet)
   - Triggers `notifyInode(entry.Ino)` + `notifyEntry(parentIno, name)` for kernel cache invalidation
3. **`Flush` / `Release`** — mode travels with the data via `WriteBackMeta.Mode`
4. **`CommitQueue`** — upload carries mode atomically (via `X-Dat9-Mode` header) or calls `ChmodCtx` post-commit

`WriteBackMeta` (used by `PendingIndex` for crash recovery) gains a `Mode uint32` field:

```go
type WriteBackMeta struct {
    Path        string
    Size        int64
    Mtime       time.Time
    // ... existing fields ...
    Mode        uint32 // new: permission bits for pending files
}
```

### 3.5 Cache Invalidation on chmod

| Cache Layer | Action on chmod |
|-------------|-----------------|
| Kernel inode attr cache | `fs.notifyInode(ino)` — forces re-`GetAttr` |
| Kernel dentry cache | `fs.notifyEntry(parentIno, name)` — forces re-`Lookup` |
| Userspace `dirCache` | `CachedFileInfo.Mode` field; `dirCache.Upsert` on `SetAttr` |
| `readCache` | Unchanged (data bytes only) |

### 3.6 Create-with-Mode Optimization

To avoid `create + chmod` double RPC:

- **HTTP API**: `POST /v1/fs/{path}` upload/initiate accepts `X-Dat9-Mode` header
- **Go SDK**: `CreateFileCtx(path, mode, data)` includes mode in creation
- **Server**: persists mode into `inodes` row at file creation time

## 4. Cross-Layer Changes

### 4.1 Schema Files

| File | Change |
|------|--------|
| `pkg/tenant/schema/tidb_app.go` | Replace `files` with `inodes`+`contents`+`semantic`; rename `file_id` → `inode_id` |
| `pkg/tenant/schema/tidb_auto.go` | Same + move GENERATED columns to `semantic` |
| `pkg/tenant/schema/db9/schema.go` | Same + move hnsw/gin indexes to `semantic` |

### 4.2 Datastore Layer

| File | Change |
|------|--------|
| `pkg/datastore/store.go` | **Major rewrite** (~20 query sites). Split `File` struct into `Inode`, `Content`, `Semantic`. Rename `file_id` → `inode_id`. `ListDir` joins `file_nodes → inodes` only. `StatForRead` joins `file_nodes → inodes → contents`. Search joins `file_nodes → inodes → semantic`. |
| `pkg/datastore/file_tx.go` | `updateFileContentTx` becomes multi-table transaction (see §4.8). `InsertFileTx` inserts into 3 tables atomically. |
| `pkg/datastore/search.go` | Update JOINs: `files` → `inodes` / `semantic`. |
| `pkg/datastore/file_gc_tasks.go` | Rename `file_id` → `inode_id`. |
| `pkg/datastore/execsql_test.go` | Update query whitelist. |

### 4.3 Backend Layer

| File | Change |
|------|--------|
| `pkg/backend/dat9.go` | `MkdirCtx`: create `inodes` row for directory before `file_nodes`. `EnsureParentDirs`: create `inodes` row for each auto-created parent directory. `CopyFileCtx`: unchanged (only touches `file_nodes`). `Stat`/`ReadDir`: read mode from `inodes`. `Chmod`: `UPDATE inodes SET mode=?`. |
| `pkg/backend/upload.go` | Finalize touches `inodes` (size, revision) + `contents` (storage_ref). |
| `pkg/backend/semantic_tasks.go` | Workers read/write `semantic` table. |

### 4.4 HTTP Server

| File | Change |
|------|--------|
| `pkg/server/server.go` | Stat returns `X-Dat9-Mode`. Upload accepts `X-Dat9-Mode`. `POST /v1/fs/{path}?action=chmod` endpoint (same `/v1/` versioning as existing endpoints). No `chown`. |

### 4.5 Go SDK

| File | Change |
|------|--------|
| `pkg/client/client.go` | `StatResult.Mode`, `FileInfo.Mode`. `CreateFileCtx(path, mode, data)`. `ChmodCtx`. |

### 4.6 FUSE Layer

| File | Change |
|------|--------|
| `pkg/fuse/dat9fs.go` | `fillAttr` uses `InodeEntry.Mode`. `Create` captures mode. `SetAttr(FATTR_MODE)` with pending-state awareness. `Mkdir` forwards mode. |
| `pkg/fuse/inode.go` | Add `Mode uint32` to `InodeEntry`. |
| `pkg/fuse/writeback.go` | Add `Mode uint32` to `WriteBackMeta`. |
| `pkg/fuse/commit_queue.go` | Upload carries mode or calls `ChmodCtx` post-commit. |
| `pkg/fuse/dir.go` | `CachedFileInfo.Mode`; `dirCache.Upsert` on chmod. |

### 4.7 WebDAV

| File | Change |
|------|--------|
| `pkg/webdav/fs.go` | `fileInfo.Mode()` from `StatResult.Mode`. `Mkdir` forwards perm. |

### 4.8 Multi-Table Transaction Strategy

`updateFileContentTx` currently issues a single `UPDATE files`. After splitting, a content overwrite becomes a **three-table transaction**:

```sql
BEGIN;
  -- 1. Gate + update inode metadata (optimistic lock)
  UPDATE inodes
  SET size_bytes = ?, revision = revision + 1, status = 'CONFIRMED',
      mtime = ?, confirmed_at = COALESCE(confirmed_at, ?)
  WHERE inode_id = ? AND revision = ? AND status = 'CONFIRMED';
  -- (check affected rows == 1; if not, abort)

  -- 2. Update storage blob metadata (no independent gate; relies on transaction isolation)
  UPDATE contents
  SET storage_type = ?, storage_ref = ?, content_blob = ?, content_type = ?,
      checksum_sha256 = ?
  WHERE inode_id = ?;
  -- (verify affected rows >= 1; if 0, the contents row is missing — abort and investigate)

  -- 3. Update semantic search data
  UPDATE semantic
  SET content_text = ?, description = ?,
      embedding = NULL, embedding_revision = NULL,
      description_embedding = NULL, description_embedding_revision = NULL
  WHERE inode_id = ?;
COMMIT;
```

**Isolation level:** Use the datastore's default isolation (READ COMMITTED or higher). All three UPDATEs are in the same DB transaction, so atomicity is guaranteed by the database.

**Optimistic lock strategy:**
- Only `inodes` carries the `revision` gate. `contents` and `semantic` do not have independent revision columns.
- If the `inodes` UPDATE returns 0 affected rows (revision mismatch), the entire transaction is rolled back.
- This preserves the existing concurrency semantics: two concurrent overwrites race on `inodes.revision`; one wins, one gets a retryable conflict error.

**Embedding NULL logic:**
- When content is overwritten, `semantic.embedding` and `semantic.description_embedding` are explicitly set to `NULL` (their revisions are also cleared).
- The stale-check `embedding_revision == revision` becomes a cross-table comparison: `s.embedding_revision = i.revision` in queries.
- After a successful overwrite, the semantic worker will recompute embeddings in a background task and update `semantic.embedding` + `embedding_revision`.

**Partial-write risk:** None — the transaction either commits all three tables or rolls back all three.

### 4.9 Search Query Performance Analysis

**Current query (two-table JOIN):**
```sql
SELECT fn.path, fn.name, f.size_bytes,
       VEC_EMBED_COSINE_DISTANCE(f.embedding, ?) AS distance
FROM file_nodes fn
JOIN files f ON fn.file_id = f.file_id
WHERE f.status = 'CONFIRMED' AND f.embedding IS NOT NULL
ORDER BY distance LIMIT ?
```

**New query (three-table JOIN):**
```sql
SELECT fn.path, fn.name, i.size_bytes,
       VEC_EMBED_COSINE_DISTANCE(s.embedding, ?) AS distance
FROM file_nodes fn
JOIN inodes i ON fn.inode_id = i.inode_id
JOIN semantic s ON i.inode_id = s.inode_id
WHERE i.status = 'CONFIRMED' AND s.embedding IS NOT NULL
ORDER BY distance LIMIT ?
```

**Performance characteristics:**

| Aspect | TiDB | db9 (PostgreSQL) |
|--------|------|------------------|
| `file_nodes → inodes` | PK lookup on `inodes.inode_id` | PK lookup on `inodes.inode_id` |
| `inodes → semantic` | PK lookup on `semantic.inode_id` | PK lookup on `semantic.inode_id` |
| Vector distance | `VEC_EMBED_COSINE_DISTANCE` on `semantic.embedding` | `vector <=> embedding` on `semantic.embedding` with `hnsw` index |
| Stale-check | `s.embedding_revision = i.revision` (cross-table) | `s.embedding_revision = i.revision` (cross-table) |
| Query plan risk | Three PK lookups + vector computation. TiDB's optimizer should push `embedding IS NOT NULL` to `semantic` first. | PostgreSQL's planner uses `hnsw` index on `semantic` then NLJ to `inodes` and `file_nodes`. |

**Key concern:** The stale-check `embedding_revision = revision` now spans two tables. In PostgreSQL, this may prevent the planner from using the `hnsw` index if the join order is suboptimal.

**Mitigation (current):** Add an index on `semantic(embedding_revision)` so the stale-check filter can be evaluated alongside the vector index scan without forcing a full PK lookup per candidate row.

**Alternative (future):** Add `embedding_stale BOOLEAN DEFAULT TRUE` to `semantic`. Set `TRUE` on content overwrite, `FALSE` when the embedding worker completes. The stale-check becomes a single-table predicate: `WHERE s.embedding IS NOT NULL AND NOT s.embedding_stale`. This eliminates the cross-table comparison entirely. Deferred to Phase 2 because it adds a new column and requires updating the semantic worker.

**Overall assessment:** The three-table JOIN adds one extra PK lookup per result row. For typical result sets (< 100 rows), the overhead is negligible (< 1 ms). The bigger win is that `ListDir` no longer fetches `content_blob` or embedding vectors, which dominates current listing latency.

**Why wasn't ListDir already column-pruned today?** `StatForRead` (`store.go:931`) already demonstrates column-pruned SELECTs. `ListDir` was not pruned because the Go `File` struct required all columns to be scanned into a single struct — there was no lightweight metadata-only struct. The `Inode`/`Content`/`Semantic` split is the forcing function that finally enables a lightweight `Inode` struct for listings.

## 5. Key Design Decisions

### 5.1 No explicit `nlink` column (with quantitative analysis)

Refcount is computed on demand. Because `CopyFile` inserts a **new** `file_nodes` row, a plain `COUNT(*) ... FOR UPDATE` on existing rows does **not** block concurrent hardlink creation. The correct locking sequence is:

```sql
-- Step 1: Lock the inode row first. This blocks concurrent CopyFile/delete/replace
--         on the same inode because they also lock the inode row.
SELECT * FROM inodes WHERE inode_id = ? FOR UPDATE;

-- Step 2: Count dentries (under the inode lock)
SELECT COUNT(*) FROM file_nodes WHERE inode_id = ?;
```

`CopyFile`, `DeleteFileWithRefCheck`, and `ReplaceFile` must all acquire the `inodes` row lock (`SELECT ... FOR UPDATE`) before reading or mutating `file_nodes` rows for that inode. This serializes hardlink creation against deletion.

**Cost analysis:**
- A single `COUNT(*)` with an index on `file_nodes.inode_id` is an **index-only scan** in both TiDB and PostgreSQL.
- For a typical deployment with < 1M dentries per inode, p50 latency is < 1 ms, p99 < 5 ms.
- `GetAttr` (FUSE stat) currently does not call `RefCount` at all — `RefCount` is only invoked during **deletion** (`DeleteFileWithRefCheck`, `RenameFileReplacingTarget`).
- Therefore the COUNT query does **not** add overhead to `ls -l` or normal stat calls.

**Future optimization path** (if needed):
- Add an `nlink` column to `inodes` and maintain it via DB triggers:
  ```sql
  CREATE TRIGGER trg_file_nodes_insert AFTER INSERT ON file_nodes
    FOR EACH ROW UPDATE inodes SET nlink = nlink + 1 WHERE inode_id = NEW.inode_id;
  CREATE TRIGGER trg_file_nodes_delete AFTER DELETE ON file_nodes
    FOR EACH ROW UPDATE inodes SET nlink = nlink - 1 WHERE inode_id = OLD.inode_id;
  ```
- Or maintain it in application code with `FOR UPDATE` locking on the `inodes` row.

**Decision:** Defer explicit `nlink` until deletion-path performance becomes a bottleneck.

### 5.2 `contents` and `semantic` are 1:1 with `inodes`

Both use `inode_id` as their primary key (not a separate auto-increment). This avoids an extra JOIN key and makes the split feel like a vertical partition.

### 5.3 Directories always have an `inodes` row

Directories have no `contents` row and no `semantic` row. Their `inodes` row has `size_bytes=0`, `status='CONFIRMED'`, `mode=493` (0755). This is required for POSIX permissions to work at the inode level.

### 5.4 `file_id` renamed to `inode_id` everywhere

A large but mechanical rename across the entire codebase. It makes the schema self-documenting: "this is an inode ID, not a file-specific ID."

## 6. Migration Strategy

**Phase A — Online migration (expand, rollback-safe):**

```sql
-- 1. Create new tables alongside old ones
CREATE TABLE inodes (...);
CREATE TABLE contents (...);
CREATE TABLE semantic (...);

-- 2. Migrate existing files (TiDB / MySQL)
--    Use COALESCE(confirmed_at, created_at) for mtime so pending uploads
--    (confirmed_at IS NULL) do not violate the NOT NULL constraint.
INSERT INTO inodes (inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at, expires_at)
SELECT file_id, size_bytes, revision, 420, status, created_at, COALESCE(confirmed_at, created_at), confirmed_at, expires_at
FROM files WHERE status != 'DELETED';

INSERT INTO contents (inode_id, storage_type, storage_ref, ...)
SELECT file_id, storage_type, storage_ref, ...
FROM files WHERE status != 'DELETED';

-- For tidb_app.go (app-managed embeddings): full column copy
INSERT INTO semantic (inode_id, content_text, description, embedding, embedding_revision, description_embedding, description_embedding_revision)
SELECT file_id, content_text, description, embedding, embedding_revision, description_embedding, description_embedding_revision
FROM files WHERE status != 'DELETED';

-- For tidb_auto.go (TiDB GENERATED embeddings): do NOT list generated columns.
-- TiDB will recompute embedding/description_embedding from content_text/description.
-- If file count is large, run during off-peak or in batches.
INSERT INTO semantic (inode_id, content_text, description)
SELECT file_id, content_text, description
FROM files WHERE status != 'DELETED';

-- db9 / PostgreSQL variant (idempotent, safe to re-run):
-- INSERT INTO inodes (...) SELECT ... FROM files WHERE status != 'DELETED'
--   ON CONFLICT (inode_id) DO NOTHING;
-- INSERT INTO contents (...) SELECT ... FROM files WHERE status != 'DELETED'
--   ON CONFLICT (inode_id) DO NOTHING;
-- INSERT INTO semantic (...) SELECT ... FROM files WHERE status != 'DELETED'
--   ON CONFLICT (inode_id) DO NOTHING;

-- 3. Create directory inode records (idempotent)
-- Pre-migration validation: confirm how many directories have NULL file_id
--   SELECT COUNT(*) FROM file_nodes WHERE is_directory = 1 AND file_id IS NULL;
--
-- TiDB / MySQL (use current column name file_id):
INSERT IGNORE INTO inodes (inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at)
SELECT node_id, 0, 1, 493, 'CONFIRMED', created_at, created_at, created_at
FROM file_nodes WHERE is_directory = 1;
-- db9 / PostgreSQL:
-- INSERT INTO inodes (inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at)
-- SELECT node_id, 0, 1, 493, 'CONFIRMED', created_at, created_at, created_at
-- FROM file_nodes WHERE is_directory = 1
-- ON CONFLICT (inode_id) DO NOTHING;

UPDATE file_nodes SET file_id = node_id WHERE is_directory = 1 AND file_id IS NULL;

-- 4. Verify: run a full integration test suite against the new schema
-- 5. Switch application code to use new tables
--    NOTE: At this point old binaries still reference `file_id` in shared
--    tables. The rename in step 6 must happen ONLY after ALL instances
--    have been upgraded.
-- 6. Retain old `files` table for one release cycle as rollback safety
--    (do NOT DROP TABLE files immediately)

-- ═══════════════════════════════════════════════════════════════════════
-- Phase B — Schema contraction (NEXT release cycle, AFTER all instances
--            are confirmed to be running new code):
-- ═══════════════════════════════════════════════════════════════════════

-- 7. Rename shared-table columns from `file_id` to `inode_id`.
--    This step is NOT rollback-safe — old binaries will fail once executed.
--    It MUST only run when zero old-code instances remain.
-- TiDB / MySQL:
ALTER TABLE file_nodes CHANGE file_id inode_id VARCHAR(64);
ALTER TABLE file_nodes RENAME INDEX idx_file_id TO idx_inode_id;
ALTER TABLE file_tags CHANGE file_id inode_id VARCHAR(64) NOT NULL;
ALTER TABLE uploads CHANGE file_id inode_id VARCHAR(64) NOT NULL;
ALTER TABLE file_gc_tasks CHANGE file_id inode_id VARCHAR(64) NOT NULL;
-- db9 / PostgreSQL:
-- ALTER TABLE file_nodes RENAME COLUMN file_id TO inode_id;
-- ALTER INDEX idx_file_id RENAME TO idx_inode_id;
-- ALTER TABLE file_tags RENAME COLUMN file_id TO inode_id;
-- ALTER TABLE uploads RENAME COLUMN file_id TO inode_id;
-- ALTER TABLE file_gc_tasks RENAME COLUMN file_id TO inode_id;

-- 8. After one more release cycle with no issues:
--    DROP TABLE files;
```

**Incremental migration (recommended for large deployments):**

For environments with millions of files, a single atomic migration may take too long. A safer two-phase sequence:

**Phase A — Expand (additive, rollback-safe):**
1. **Dual-write**: New code writes to both `files` and `inodes`/`contents`/`semantic` on every mutation
2. **Backfill**: Run migration script in batches to populate new tables from old data
3. **Switch reads**: Route read traffic to new tables; keep `files` writes active
4. **Verify**: Run consistency checks (see below)
5. **Stop old writes**: Remove `files` writes from code

**Phase B — Contract (destructive, NOT rollback-safe):**
6. **Rename columns**: After ALL instances are confirmed on new code, rename `file_id` → `inode_id` in shared tables
7. **Drop old table**: After one more release cycle, `DROP TABLE files`

Steps 1–5 are independently reversible. Step 6 is a point of no return.

**Rollback path:** If issues are found during Phase A (steps 1–5), the application can be rolled back to the old code path which still reads from `files`. The new tables are purely additive and shared-table columns have not yet been renamed. Once Phase B (column rename) begins, rollback to old binaries is impossible.

**Data consistency verification (post-migration, pre-switch):**
```sql
-- Verify every file_node has a matching inode
-- NOTE: Use fn.file_id during Phase A (before column rename), fn.inode_id after Phase B.
SELECT COUNT(*) FROM file_nodes fn
LEFT JOIN inodes i ON fn.file_id = i.inode_id
WHERE i.inode_id IS NULL;
-- Expected: 0

-- Verify every file has a matching contents row
SELECT COUNT(*) FROM file_nodes fn
JOIN inodes i ON fn.file_id = i.inode_id
LEFT JOIN contents c ON i.inode_id = c.inode_id
WHERE c.inode_id IS NULL AND fn.is_directory = 0;
-- Expected: 0

-- Spot-check: compare old vs new mode/size values
SELECT f.file_id, f.size_bytes, f.status, i.size_bytes, i.status, i.mode
FROM files f JOIN inodes i ON f.file_id = i.inode_id
WHERE f.size_bytes != i.size_bytes OR f.status != i.status
LIMIT 10;
```

## 7. Implementation Order

| Step | Content | Files |
|------|---------|-------|
| 1 | Schema: `inodes`/`contents`/`semantic`; `file_id`→`inode_id` rename | `pkg/tenant/schema/*.go` |
| 2 | Datastore: `Inode`/`Content`/`Semantic` structs; rewrite all JOINs | `pkg/datastore/store.go` |
| 3 | Datastore: multi-table transactions | `pkg/datastore/file_tx.go` |
| 4 | Datastore: search JOINs | `pkg/datastore/search.go` |
| 5 | Backend: directory inode creation, Stat mode, Chmod | `pkg/backend/dat9.go` |
| 6 | Backend: upload finalize | `pkg/backend/upload.go` |
| 7 | Backend: semantic workers | `pkg/backend/semantic_tasks.go` |
| 8 | Server: mode headers, chmod endpoint | `pkg/server/server.go` |
| 9 | Go SDK: `Mode`, `CreateFileCtx` | `pkg/client/client.go` |
| 10 | FUSE: `InodeEntry.Mode`, pending-create state machine, cache invalidation | `pkg/fuse/*.go` |
| 11 | WebDAV | `pkg/webdav/fs.go` |
| 12 | Tests: schema helpers + assertions | See §7.1 for detailed test file list |
| 13 | Schema dump | `drive9-server schema dump-init-sql` |

### 7.1 Affected Test Files

| Package | Test File | Impact |
|---------|-----------|--------|
| `pkg/datastore` | `schema_test_helper_test.go` | Test schema strings must define `inodes`/`contents`/`semantic` |
| `pkg/datastore` | `store_test.go` | All `files`-based CRUD tests rewrite for 3-table model |
| `pkg/datastore` | `file_tx_test.go` | `InsertFileTx`, `UpdateFileContentTx` become multi-table |
| `pkg/datastore` | `search_test.go` | JOIN targets change from `files` to `inodes`/`semantic` |
| `pkg/datastore` | `embedding_writeback_test.go` | Queries target `semantic` instead of `files` |
| `pkg/datastore` | `execsql_test.go` | Whitelist table names updated |
| `pkg/backend` | `schema_test_helper.go` | Test schema strings updated |
| `pkg/backend` | `dat9_test.go` | `Mkdir`, `CopyFile`, `Stat`, `Chmod` assertions updated |
| `pkg/backend` | `upload_test.go` | Upload finalize touches `inodes` + `contents` |
| `pkg/server` | `server_test.go` | Stat response includes `mode`; chmod endpoint tests |
| `pkg/fuse` | `dat9fs_test.go` | `fillAttr` returns real mode; pending-create mode tests |
| `pkg/client` | `client_test.go` | `StatResult.Mode` field tested |
| `pkg/tenant` | `pool_test.go` | Test schema includes new tables |
| `pkg/tenant` | `tidb_auto_test.go` | Schema diff tests expect `semantic` table instead of `files` |
