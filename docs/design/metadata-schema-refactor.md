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
    confirmed_at DATETIME(3),
    expires_at   DATETIME(3)
);
CREATE INDEX idx_status ON inodes(status, created_at);
```

**`mode` stores permission bits only** (low 12 bits: setuid/setgid/sticky + `rwxrwxrwx`). File-type bits (`S_IFREG`, `S_IFDIR`) are **not** stored; they are derived from `file_nodes.is_directory` at read time.

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

### 2.6 Column Assignment (Old → New)

| Old `files` column | New table | Reason |
|--------------------|-----------|--------|
| `file_id` | `inodes.inode_id` | Identity |
| `size_bytes` | `inodes` | POSIX stat |
| `revision` | `inodes` | Optimistic locking |
| `mode` | `inodes` | POSIX permissions |
| `status` | `inodes` | Lifecycle |
| `created_at`, `confirmed_at`, `expires_at` | `inodes` | Timestamps |
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

**db9 (PostgreSQL):**
- `semantic` table hosts `hnsw` vector indexes on `embedding` and `description_embedding`.
- `semantic` table hosts `gin(to_tsvector('simple', coalesce(content_text,'')))` for full-text search.

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
| `pkg/datastore/file_tx.go` | `updateFileContentTx` becomes multi-table: `UPDATE contents` + `UPDATE inodes SET size_bytes=..., revision=revision+1`. `InsertFileTx` inserts into 3 tables. |
| `pkg/datastore/search.go` | Update JOINs: `files` → `inodes` / `semantic`. |
| `pkg/datastore/file_gc_tasks.go` | Rename `file_id` → `inode_id`. |
| `pkg/datastore/execsql_test.go` | Update query whitelist. |

### 4.3 Backend Layer

| File | Change |
|------|--------|
| `pkg/backend/dat9.go` | `MkdirCtx`: create `inodes` row for directory before `file_nodes`. `CopyFileCtx`: unchanged (only touches `file_nodes`). `Stat`/`ReadDir`: read mode from `inodes`. `Chmod`: `UPDATE inodes SET mode=?`. |
| `pkg/backend/upload.go` | Finalize touches `inodes` (size, revision) + `contents` (storage_ref). |
| `pkg/backend/semantic_tasks.go` | Workers read/write `semantic` table. |

### 4.4 HTTP Server

| File | Change |
|------|--------|
| `pkg/server/server.go` | Stat returns `X-Dat9-Mode`. Upload accepts `X-Dat9-Mode`. `?action=chmod` endpoint. No `chown`. |

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

## 5. Key Design Decisions

### 5.1 No explicit `nlink` column

Refcount is computed on demand: `SELECT COUNT(*) FROM file_nodes WHERE inode_id = ?`. Adding an explicit `nlink` would require updating it on every `InsertNode`/`DeleteNode`/`CopyFile` with proper locking. The cost is not justified until stat performance becomes a bottleneck.

### 5.2 `contents` and `semantic` are 1:1 with `inodes`

Both use `inode_id` as their primary key (not a separate auto-increment). This avoids an extra JOIN key and makes the split feel like a vertical partition.

### 5.3 Directories always have an `inodes` row

Directories have no `contents` row and no `semantic` row. Their `inodes` row has `size_bytes=0`, `status='CONFIRMED'`, `mode=493` (0755). This is required for POSIX permissions to work at the inode level.

### 5.4 `file_id` renamed to `inode_id` everywhere

A large but mechanical rename across the entire codebase. It makes the schema self-documenting: "this is an inode ID, not a file-specific ID."

## 6. Migration Strategy

```sql
-- 1. Create new tables
CREATE TABLE inodes (...);
CREATE TABLE contents (...);
CREATE TABLE semantic (...);

-- 2. Migrate existing files
INSERT INTO inodes (inode_id, size_bytes, revision, mode, status, created_at, confirmed_at, expires_at)
SELECT file_id, size_bytes, revision, 420, status, created_at, confirmed_at, expires_at
FROM files WHERE status != 'DELETED';

INSERT INTO contents (inode_id, storage_type, storage_ref, ...)
SELECT file_id, storage_type, storage_ref, ...
FROM files WHERE status != 'DELETED';

INSERT INTO semantic (inode_id, content_text, description, ...)
SELECT file_id, content_text, description, ...
FROM files WHERE status != 'DELETED';

-- 3. Create directory inode records
INSERT INTO inodes (inode_id, size_bytes, revision, mode, status, created_at, confirmed_at)
SELECT node_id, 0, 1, 493, 'CONFIRMED', created_at, created_at
FROM file_nodes WHERE is_directory = 1;

UPDATE file_nodes SET inode_id = node_id WHERE is_directory = 1 AND inode_id IS NULL;

-- 4. Rename FK columns
ALTER TABLE file_tags CHANGE file_id inode_id VARCHAR(64) NOT NULL;
ALTER TABLE uploads CHANGE file_id inode_id VARCHAR(64) NOT NULL;
ALTER TABLE file_gc_tasks CHANGE file_id inode_id VARCHAR(64) NOT NULL;

-- 5. Drop old table
DROP TABLE files;
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
| 12 | Tests: schema helpers + assertions | Various `*_test.go` |
| 13 | Schema dump | `drive9-server schema dump-init-sql` |
