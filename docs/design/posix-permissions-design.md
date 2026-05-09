# drive9 POSIX Permissions Design

## 1. Current State and Goals

drive9 currently has **no POSIX permission storage at all**:
- Files are hard-coded to `0644`, directories to `0755`
- `uid/gid` are fixed to the user who performed the mount
- `Chmod` is a no-op; `Chown` does not exist
- Neither the `file_nodes` nor the `files` metadata tables has permission columns

Goals:
1. **Storage layer**: persist `mode` in metadata at the **inode level** (not dentry)
2. **FUSE compatibility**: `chmod`, `ls -l` work correctly; tools such as `git`, `tar`, and `rsync -a` are usable
3. **API compatibility**: permissions flow through the HTTP protocol, Go SDK, and WebDAV layers
4. **Backward compatibility**: existing data can be migrated smoothly, new columns have sensible defaults
5. **Hardlink correctness**: multiple `file_nodes` pointing to the same `file_id` must share the same mode (POSIX semantics)

**Out of scope (Phase 1)**:
- `uid/gid` — drive9 uses API Key / Capability Token auth, not Unix UID/GID authentication. Owner fields have no enforcement value and add cross-layer cost.
- `chown` — no uid/gid, so no chown.
- ACL / `setfacl` / `getfacl`
- Semantic enforcement of `sticky bit` / `setuid` / `setgid` (bits are stored but not enforced)

## 2. Industry Research

| System | Permission Storage | chmod/chown | Characteristics |
|--------|-------------------|-------------|-----------------|
| **s3fs-fuse** | S3 object metadata headers (`x-amz-meta-mode`, etc.) | ✅ Supported | Best compatibility, but relies on S3 object headers; not returned in listings, requiring an extra HEAD per object |
| **goofys / mountpoint-s3** | Not persisted | ❌ Not supported | Only global flags `--file-mode` / `--uid`; applied uniformly at mount time |
| **geesefs** | S3 object metadata | ⚠️ Yandex S3 only | Standard S3 listings do not return user-metadata; inefficient |
| **JuiceFS** | Metadata engine (Redis/MySQL) inode table | ✅ Full POSIX + ACL | The canonical POSIX implementation; supports ACL and Ranger integration |
| **cunoFS/Storj** | Hidden metadata files in object storage | ✅ Supported | Stores metadata in shadow files; lost when accessed by non-client tools |

**Implications for drive9**:
- drive9 already has its own relational metadata database (TiDB/MySQL), so **there is no need to stuff permissions into object metadata like S3**. It should follow the **JuiceFS approach**: add columns directly to the metadata tables.
- ACL/Ranger are not needed initially; basic POSIX 9-bit mode covers ~90% of use cases.

## 3. Permission Model Design

We adopt **POSIX Base Permissions** (rwxrwxrwx + setuid/setgid/sticky). ACL is **not** supported in this phase.

### 3.1 Storage Location: `files` Table (Inode Level)

**Why `files`, not `file_nodes`?**

In POSIX, permissions are **inode attributes**, not dentry attributes. drive9 already supports hardlinks (`CopyFile` creates multiple `file_nodes` rows pointing to the same `file_id`). If mode lived in `file_nodes`, two hardlinks could have different modes — violating POSIX.

**Directory inode records:**

Today, directories have **no** `files` record (`file_nodes.file_id` is NULL for directories). To store mode at the inode level, directories **must** gain a `files` record.

```sql
-- files table: add mode column
ALTER TABLE files ADD COLUMN mode INT UNSIGNED NOT NULL DEFAULT 420;
```

Directory `files` records use sentinel values for file-specific columns:

| Column | Directory Value |
|--------|-----------------|
| `file_id` | generated UUID |
| `storage_type` | `'dir'` |
| `storage_ref` | `''` |
| `size_bytes` | `0` |
| `status` | `'CONFIRMED'` |
| `mode` | `493` (`0755` decimal) |

Files default to `420` (`0644` decimal). `420` is used because SQL does **not** parse octal literals; `0644` would be interpreted as decimal `644`.

### 3.2 `mode` Column Semantics

- `mode` stores **permission bits only** (low 12 bits: setuid/setgid/sticky + `rwxrwxrwx`).
- File-type bits (`S_IFREG`, `S_IFDIR`) are **not** stored here.
- At read time, the code combines the stored permission bits with `S_IFREG` or `S_IFDIR` based on `file_nodes.is_directory`.

### 3.3 Default-Value Strategy (Backward Compatible)

| Scenario | mode (permission bits only) |
|----------|-----------------------------|
| Existing files (post-migration) | `0644` (decimal `420`) |
| Existing directories (post-migration) | `0755` (decimal `493`) |
| New files (FUSE Create) | `0666 & ~umask` (standard FUSE behavior) |
| New directories (FUSE Mkdir) | `0777 & ~umask` |
| New files (HTTP API/CLI) | `0644` (decimal `420`) unless header overrides |

## 4. Cross-Layer Changes

### 4.1 Schema & Migration (`pkg/tenant/schema/`)

**Step 1: Add `mode` to `files` table in all three schema files:**

```sql
CREATE TABLE IF NOT EXISTS files (
    file_id            VARCHAR(64) PRIMARY KEY,
    storage_type       VARCHAR(32) NOT NULL,
    storage_ref        TEXT NOT NULL,
    storage_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'legacy',
    storage_encryption_key_id VARCHAR(256) NOT NULL DEFAULT '',
    content_blob       LONGBLOB,
    content_type       VARCHAR(255),
    size_bytes         BIGINT NOT NULL DEFAULT 0,
    checksum_sha256    VARCHAR(128),
    revision           BIGINT NOT NULL DEFAULT 1,
    status             VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    source_id          VARCHAR(255),
    content_text       LONGTEXT,
    description        LONGTEXT,
    embedding          VECTOR(1024),
    embedding_revision BIGINT,
    description_embedding VECTOR(1024),
    description_embedding_revision BIGINT,
    mode               INT UNSIGNED NOT NULL DEFAULT 420,  -- new
    created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    confirmed_at       DATETIME(3),
    expires_at         DATETIME(3)
);
```

**Step 2: Existing data migration:**

```sql
-- 1. Update all existing files to mode 0644
UPDATE files SET mode = 420 WHERE storage_type != 'dir';

-- 2. Create inode records for all existing directories
INSERT INTO files (file_id, storage_type, storage_ref, size_bytes, revision, status, mode, created_at, confirmed_at)
SELECT genID(), 'dir', '', 0, 1, 'CONFIRMED', 493, created_at, created_at
FROM file_nodes WHERE is_directory = 1;

-- 3. Link directories to their new inode records
UPDATE file_nodes fn
JOIN files f ON f.storage_type = 'dir' AND fn.is_directory = 1
SET fn.file_id = f.file_id
WHERE fn.file_id IS NULL;
```

> Note: The actual migration script needs a deterministic `file_id` generation or a temp table to map `node_id → file_id`, since `genID()` is non-deterministic. A practical approach: use `fn.file_id = fn.node_id` (or a hash) for directories.

**Step 3: `file_nodes.file_id` becomes NOT NULL:**

After migration, every `file_nodes` row has a `file_id`. Update schema to `NOT NULL` (optional, can be deferred).

### 4.2 Datastore Layer (`pkg/datastore/`)

**Model change:**

```go
type File struct {
    FileID                 string
    StorageType            StorageType
    StorageRef             string
    StorageEncryptionMode  StorageEncryptionMode
    StorageEncryptionKeyID string
    ContentBlob            []byte
    ContentType            string
    SizeBytes              int64
    ChecksumSHA256         string
    Revision               int64
    EmbeddingRevision      *int64
    Status                 FileStatus
    SourceID               string
    ContentText            string
    Description            string
    DescriptionEmbeddingRevision *int64
    CreatedAt              time.Time
    ConfirmedAt            *time.Time
    ExpiresAt              *time.Time
    Mode        uint32  // new (permission bits only)
}
```

**Directory creation (`MkdirCtx`, `EnsureParentDirs`):**

Today, directory creation only inserts into `file_nodes`. New flow:

```go
func (s *Store) InsertNode(ctx context.Context, node *FileNode) error {
    if node.IsDirectory {
        // 1. Create inode record for the directory
        fileID := s.genID()
        _, err := s.db.ExecContext(ctx,
            `INSERT INTO files (file_id, storage_type, storage_ref, size_bytes, revision, status, mode, created_at, confirmed_at)
             VALUES (?, 'dir', '', 0, 1, 'CONFIRMED', ?, ?, ?)`,
            fileID, node.Mode, node.CreatedAt, node.CreatedAt)
        if err != nil { return err }
        node.FileID = fileID
    }
    // 2. Insert dentry
    _, err := s.db.ExecContext(ctx,
        `INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, file_id, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        node.NodeID, node.Path, node.ParentPath, node.Name, node.IsDirectory,
        nullStr(node.FileID), node.CreatedAt)
    return err
}
```

**Directory deletion (`DeleteDirRecursive`, `Rmdir`):**

Today, directory deletion only removes `file_nodes` rows. New flow:

```go
func (s *Store) DeleteDirWithInode(ctx context.Context, path string) error {
    // 1. Get the directory's file_id
    var fileID string
    err := s.db.QueryRowContext(ctx,
        `SELECT file_id FROM file_nodes WHERE path = ? AND is_directory = 1`, path).Scan(&fileID)
    if err != nil { return err }

    // 2. Delete dentry
    _, err = s.db.ExecContext(ctx,
        `DELETE FROM file_nodes WHERE path = ?`, path)
    if err != nil { return err }

    // 3. Delete inode record (directories are not hardlinked, refcount is always 1)
    _, err = s.db.ExecContext(ctx,
        `DELETE FROM files WHERE file_id = ? AND storage_type = 'dir'`, fileID)
    return err
}
```

**JOIN query changes:**

Remove `AND fn.is_directory = 0` from `LEFT JOIN files` clauses. Directories now have `files` records.

```sql
-- Before (StatPathFallback):
FROM file_nodes fn
LEFT JOIN files f ON fn.file_id = f.file_id AND fn.is_directory = 0 AND f.status = 'CONFIRMED'
WHERE fn.path = ?

-- After:
FROM file_nodes fn
LEFT JOIN files f ON fn.file_id = f.file_id AND f.status = 'CONFIRMED'
WHERE fn.path = ?
```

All `scanNodeWithFile*` helpers now scan `f.mode` into `File.Mode`.

**Chmod SQL:**

```go
func (s *Store) UpdateFileMode(ctx context.Context, fileID string, mode uint32) error {
    _, err := s.db.ExecContext(ctx,
        `UPDATE files SET mode = ? WHERE file_id = ?`, mode, fileID)
    return err
}
```

### 4.3 Backend Layer (`pkg/backend/dat9.go`)

```go
func (b *Dat9Backend) Chmod(path string, mode uint32) error {
    // Get the file_id (works for both files and directories)
    node, err := b.store.GetNode(ctx, path)
    if err != nil { return err }
    if node.FileID == "" {
        return fmt.Errorf("no inode for path %s", path)
    }
    return b.store.UpdateFileMode(ctx, node.FileID, mode)
}

func (b *Dat9Backend) Stat(...) {
    // ...
    if nf.File != nil {
        info.Mode = syscall.S_IFREG | uint32(nf.File.Mode)
        info.Size = nf.File.SizeBytes
        info.ModTime = fileMtime(nf.File)
    } else {
        // This branch should no longer happen for directories
        info.Mode = syscall.S_IFREG | 0644
    }
    if nf.Node.IsDirectory {
        info.Mode = syscall.S_IFDIR | uint32(nf.File.Mode)
    }
    // ...
}
```

### 4.4 HTTP Server Layer (`pkg/server/`)

**Stat / HEAD / batchStat:**
- Return header: `X-Dat9-Mode`
- `batchStatResult` gains `mode` field

**Mkdir:**
- Accept header `X-Dat9-Mode` (optional, default `0755`)
- Pass mode to backend `MkdirCtx`

**Create / Upload Initiate:**
- Accept header `X-Dat9-Mode` (optional, default `0644`)
- Store mode in the `files` row at creation time

**New endpoint:**

```http
POST /v1/fs/{path}?action=chmod
  Header: X-Dat9-Mode: <octal>
```

No `chown` endpoint (uid/gid removed).

### 4.5 Go SDK Client (`pkg/client/`)

```go
type StatResult struct {
    Size     int64
    IsDir    bool
    Revision int64
    Mtime    time.Time
    Mode     uint32  // new (permission bits only)
}

type FileInfo struct {
    Name  string
    Size  int64
    IsDir bool
    Mtime int64
    Mode  uint32  // new
}

// Create with mode
func (c *Client) CreateFileCtx(ctx context.Context, path string, mode uint32, data []byte) error

// Chmod
func (c *Client) ChmodCtx(ctx context.Context, path string, mode uint32) error
```

### 4.6 FUSE Layer (`pkg/fuse/dat9fs.go`)

#### 4.6.1 `InodeEntry` Extension

```go
type InodeEntry struct {
    Ino      uint64
    Path     string
    IsDir    bool
    Nlookup  int64
    Size     int64
    Mtime    time.Time
    Revision int64
    Mode     uint32 // new (permission bits only, no S_IFMT)
}
```

#### 4.6.2 `fillAttr`

```go
func (fs *Dat9FS) fillAttr(entry *InodeEntry, out *gofuse.Attr) {
    out.Ino = entry.Ino
    out.Size = uint64(entry.Size)
    out.Blocks = (uint64(entry.Size) + 511) / 512
    out.Uid = fs.uid   // mount-time uid (no per-file owner)
    out.Gid = fs.gid   // mount-time gid

    mtime := entry.Mtime
    if mtime.IsZero() { mtime = time.Now() }
    out.SetTimes(&mtime, &mtime, &mtime)

    if entry.IsDir {
        out.Mode = syscall.S_IFDIR | entry.Mode
        out.Nlink = 2
    } else {
        out.Mode = syscall.S_IFREG | entry.Mode
        out.Nlink = 1
    }
}
```

#### 4.6.3 `Create` — Capture Mode at Birth

```go
func (fs *Dat9FS) Create(...) {
    mode := uint32(input.Mode) & 0777
    entry := &InodeEntry{
        Ino:     fs.inodes.Lookup(path),
        Path:    path,
        IsDir:   false,
        Mode:    mode,
        // Size, Mtime, Revision default to zero
    }
    // ... allocate FileHandle, WriteBuffer, etc.
}
```

#### 4.6.4 `SetAttr` — Handle `FATTR_MODE` with Pending-State Awareness

```go
func (fs *Dat9FS) SetAttr(ctx context.Context, input *gofuse.SetAttrIn, out *gofuse.AttrOut) fuse.Status {
    entry, ok := fs.inodes.Get(input.NodeId)
    if !ok { return fuse.ENOENT }

    if input.Valid&gofuse.FATTR_MODE != 0 {
        newMode := input.Mode & 0777
        entry.Mode = newMode
        fs.inodes.UpdateMode(input.NodeId, newMode)

        if !fs.hasPendingLocalState(entry.Path) {
            // File is already on server — send remote chmod
            if err := fs.client.ChmodCtx(ctx, fs.remotePath(entry.Path), newMode); err != nil {
                return fuse.EIO
            }
        }
        // If pending: mode stays in InodeEntry and WriteBackMeta;
        // it travels with the data on commit.

        // Invalidate kernel cache for this inode
        fs.notifyInode(entry.Ino)
        // Also invalidate parent dentry cache so ls -l sees the new mode
        if parentIno, ok := fs.inodes.GetInode(path.Dir(entry.Path)); ok {
            fs.notifyEntry(parentIno, path.Base(entry.Path))
        }
    }

    // Existing FATTR_SIZE / FATTR_MTIME handling ...

    fs.fillAttr(entry, &out.Attr)
    return fuse.OK
}
```

#### 4.6.5 `Mkdir` — Forward Mode

```go
func (fs *Dat9FS) Mkdir(ctx context.Context, input *gofuse.MkdirIn, name string, out *gofuse.EntryOut) fuse.Status {
    dirPath := path.Join(parentPath, name)
    mode := uint32(input.Mode) & 0777
    if err := fs.backend.MkdirCtx(ctx, dirPath, mode); err != nil {
        return fuse.EIO
    }
    // ... upsert dirCache, fill entry, etc.
}
```

#### 4.6.6 Pending-Create State Machine — Mode Propagation

`WriteBackMeta` (used by `PendingIndex` and `WriteBackCache`) gains a `Mode` field:

```go
type WriteBackMeta struct {
    Path        string
    Size        int64
    Mtime       time.Time
    CreatedAt   time.Time
    Generation  uint64
    Kind        PendingKind
    BaseRev     int64
    ShadowSpill bool
    Mode        uint32 // new
}
```

`PendingIndex` stores `*WriteBackMeta`, so `.meta` JSON files on disk automatically persist `Mode` for crash recovery.

**Commit-time mode delivery:**

When `CommitQueue` uploads a pending file, the server API must accept mode. Two options:

**Option A (preferred):** Extend upload finalize to accept `X-Dat9-Mode` header.

```go
// In commit_queue.go worker, after successful upload:
if entry.Mode != 0 {
    // If server API supports mode on upload, pass it during upload.
    // Otherwise, send a follow-up ChmodCtx.
    fs.client.ChmodCtx(ctx, entry.Path, entry.Mode)
}
```

**Option B:** After successful upload, call `client.ChmodCtx` in `onCommitQueueSuccess` if the committed mode differs from the server default.

For small files using `client.WriteCtxConditionalWithRevision`, add a `mode` parameter to avoid the second RPC.

### 4.7 Cache Invalidation

drive9 FUSE has three cache layers that `chmod` affects:

| Cache | Invalidation Action |
|-------|---------------------|
| **Kernel inode cache** | `fs.notifyInode(ino)` — forces kernel to re-`GetAttr` |
| **Kernel dentry cache** | `fs.notifyEntry(parentIno, name)` — forces re-`Lookup` |
| **Userspace `dirCache`** | `dirCache.Upsert(parent, item)` or `dirCache.Invalidate(dir)` |

`dirCache` stores `CachedFileInfo` entries. `CachedFileInfo` must gain a `Mode` field so directory listings reflect permission changes immediately.

### 4.8 WebDAV Layer (`pkg/webdav/fs.go`)

- `fileInfo.Mode()` uses `StatResult.Mode` (combined with `S_IFREG`/`S_IFDIR`)
- `Mkdir` forwards `perm` to backend

## 5. Security Boundary

drive9's authentication is **API Key / Capability Token**. Therefore:

| Layer | Behavior |
|-------|----------|
| **POSIX semantics layer** | `chmod`/`ls -l` correctly read and write mode bits so that toolchains (git, tar, cp -a) work |
| **Actual access control** | Still enforced by server-side API Key auth. FUSE `uid/gid` are fixed at mount time; mode bits are for kernel local checks and app readability |

This is analogous to **NFSv3**: the server trusts the client; real enforcement depends on the auth token, not Unix permissions.

## 6. Backward Compatibility and Migration

### 6.1 Schema Migration Script

```sql
-- Step 1: Add mode column to files
ALTER TABLE files ADD COLUMN mode INT UNSIGNED NOT NULL DEFAULT 420;

-- Step 2: Backfill existing files
UPDATE files SET mode = 420 WHERE storage_type != 'dir';

-- Step 3: Create inode records for existing directories
-- (Use node_id as file_id for deterministic mapping)
INSERT INTO files (file_id, storage_type, storage_ref, size_bytes, revision, status, mode, created_at, confirmed_at)
SELECT node_id, 'dir', '', 0, 1, 'CONFIRMED', 493, created_at, created_at
FROM file_nodes WHERE is_directory = 1;

-- Step 4: Link directories to their inode records
UPDATE file_nodes fn
SET fn.file_id = fn.node_id
WHERE fn.is_directory = 1 AND fn.file_id IS NULL;
```

### 6.2 Test Schema Helpers

All test schema helpers must be updated:
- `pkg/datastore/schema_test_helper_test.go`
- `pkg/backend/schema_test_helper.go`
- `pkg/tenant/pool_test.go`

Directory `files` records must be created in test fixtures that set up directory trees.

### 6.3 FUSE Tests

`TestLookupOpenCreatedFileAfterForgetSupportsGitChmodLock` currently relies on "chmod is ignored". After the update:
- `chmod` changes the mode for real
- The test verifies that a git lock file can still be opened after `chmod` — this should still pass because mode change does not affect openability

## 7. Implementation Steps

| Step | Content | Files |
|------|---------|-------|
| 1 | Schema: add `mode` to `files`; update `CREATE TABLE` in all schema files | `pkg/tenant/schema/*.go` |
| 2 | Datastore: `File.Mode` field; `InsertNode` creates directory `files` record; `DeleteDir` cleans up directory inode; update all JOINs | `pkg/datastore/store.go`, `file_tx.go` |
| 3 | Datastore: `UpdateFileMode`, `scanNodeWithFile*` helpers | `pkg/datastore/store.go` |
| 4 | Backend: real `Chmod`, `Stat`/`ReadDir` use `nf.File.Mode`, `MkdirCtx` accepts mode | `pkg/backend/dat9.go` |
| 5 | Server: `X-Dat9-Mode` header on stat/mkdir/create; `?action=chmod` endpoint | `pkg/server/server.go` |
| 6 | Go SDK: `StatResult.Mode`, `FileInfo.Mode`, `ChmodCtx`, `CreateFileCtx` with mode | `pkg/client/client.go` |
| 7 | FUSE: `InodeEntry.Mode`, `fillAttr`, `Create` capture mode, `SetAttr(FATTR_MODE)`, `Mkdir` forward mode | `pkg/fuse/dat9fs.go`, `pkg/fuse/inode.go` |
| 8 | FUSE pending state: `WriteBackMeta.Mode`, `PendingIndex` persist, commit-time mode delivery | `pkg/fuse/writeback.go`, `pkg/fuse/commit_queue.go`, `pkg/fuse/dat9fs.go` |
| 9 | FUSE cache invalidation: `notifyInode` + `notifyEntry` on chmod, `dirCache` Mode field | `pkg/fuse/dir.go`, `pkg/fuse/dat9fs.go` |
| 10 | WebDAV | `pkg/webdav/fs.go` |
| 11 | Tests: schema helpers + FUSE tests | Various `*_test.go` |
| 12 | Schema dump | `drive9-server schema dump-init-sql` per `AGENTS.md` |
