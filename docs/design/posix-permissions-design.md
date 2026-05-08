# drive9 POSIX Permissions Design

## 1. Current State and Goals

drive9 currently has **no POSIX permission storage at all**:
- Files are hard-coded to `0644`, directories to `0755`
- `uid/gid` are fixed to the user who performed the mount
- `Chmod` is a no-op; `Chown` does not exist
- Neither the `file_nodes` nor the `files` metadata tables has `mode/uid/gid` columns

Goals:
1. **Storage layer**: persist `mode`, `uid`, and `gid` in metadata
2. **FUSE compatibility**: `chmod`, `chown`, `ls -l` work correctly; tools such as `git`, `tar`, and `rsync -a` are usable
3. **API compatibility**: permissions flow through the HTTP protocol, Go SDK, and WebDAV layers
4. **Backward compatibility**: existing data can be migrated smoothly, and new columns have sensible defaults

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
- ACL/Ranger are not needed initially; basic POSIX 9-bit mode + uid/gid covers ~90% of use cases.

## 3. Permission Model Design

We adopt **POSIX Base Permissions** (rwxrwxrwx + setuid/setgid/sticky). ACL is **not** supported in this phase.

### 3.1 Field Definitions

Stored in the **`file_nodes`** table (directories only have `file_nodes` records, not `files` records; POSIX permissions are inode/dentry attributes, so this is the natural home):

```sql
ALTER TABLE file_nodes ADD COLUMN uid  INT  NOT NULL DEFAULT 0;
ALTER TABLE file_nodes ADD COLUMN gid  INT  NOT NULL DEFAULT 0;
ALTER TABLE file_nodes ADD COLUMN mode INT UNSIGNED NOT NULL DEFAULT 420;
```

- `uid`: file owner (numeric ID)
- `gid`: file group (numeric ID)
- `mode`: **permission bits only** (low 12 bits: setuid/setgid/sticky + `rwxrwxrwx`). File-type bits (`S_IFREG`, `S_IFDIR`, etc.) are **not** stored here; instead, the code combines the stored permission bits with `S_IFREG` or `S_IFDIR` at read time based on the existing `is_directory` column.

> `420` is the decimal value of octal `0644`. Directories default to `0755` (decimal `493`), files to `0644` (decimal `420`). During migration of existing data, `mode` preserves the current hard-coded behavior.

### 3.2 Default-Value Strategy (Backward Compatible)

| Scenario | uid | gid | mode (permission bits only) |
|----------|-----|-----|-----------------------------|
| Existing data (post-migration) | `0` (root) or mount user | `0` | `0644` (decimal `420`) for files, `0755` (decimal `493`) for directories |
| New files (FUSE Create) | `fs.uid` (mount user) | `fs.gid` | `0666 & ~umask` (standard FUSE behavior) |
| New directories (FUSE Mkdir) | `fs.uid` | `fs.gid` | `0777 & ~umask` |
| New files (HTTP API/CLI) | `0` unless request carries `X-Dat9-Uid` header | `0` | `0644` (decimal `420`) |

## 4. Cross-Layer Changes

### 4.1 Datastore Layer (`pkg/datastore/`)

**Model change**:

```go
type FileNode struct {
    NodeID      string
    Path        string
    ParentPath  string
    Name        string
    IsDirectory bool
    FileID      string
    CreatedAt   time.Time
    UID         int64   // new
    GID         int64   // new
    Mode        uint32  // new (permission bits only; S_IFMT is applied at read time via is_directory)
}
```

**SQL changes**:
- All `INSERT` / `SELECT` / `UPDATE` statements touching `file_nodes` must include the new columns
- `EnsureParentDirs` / `EnsureParentDirsTx` should create intermediate directories with `mode = 0755`, inheriting `uid/gid` from context or the mount user
- `scanNode` helpers need additional scan targets

### 4.2 Backend Layer (`pkg/backend/dat9.go`)

`filesystem.FileInfo` already contains `Mode uint32`; the backend just needs to read real values from the datastore:

```go
func (b *Dat9Backend) Chmod(path string, mode uint32) error {
    // Actually update file_nodes.mode
    return b.store.UpdateNodeMode(ctx, path, mode)
}

// New
func (b *Dat9Backend) Chown(path string, uid, gid uint32) error {
    return b.store.UpdateNodeOwner(ctx, path, int64(uid), int64(gid))
}

func (b *Dat9Backend) Stat(...) {
    // Combine stored permission bits with file-type bit from is_directory
    if node.IsDirectory {
        info.Mode = syscall.S_IFDIR | uint32(node.Mode)
    } else {
        info.Mode = syscall.S_IFREG | uint32(node.Mode)
    }
    info.Uid = uint32(node.UID)    // FileInfo may need extension
    info.Gid = uint32(node.GID)
}
```

### 4.3 HTTP Server Layer (`pkg/server/`)

**Stat / HEAD**:
- Return headers: `X-Dat9-Mode`, `X-Dat9-Uid`, `X-Dat9-Gid`
- `batchStatResult` gains `uid`, `gid`, `mode` fields

**Mkdir**:
- Accept `X-Dat9-Mode` header (or continue forwarding `MkdirIn.Mode`)
- Default to `0755` if absent

**New endpoints** (or reuse existing `POST /v1/fs/{path}` with an action query parameter):

```http
POST /v1/fs/{path}?action=chmod
  Header: X-Dat9-Mode: <octal>

POST /v1/fs/{path}?action=chown
  Header: X-Dat9-Uid: <int>
  Header: X-Dat9-Gid: <int>
```

### 4.4 Go SDK Client (`pkg/client/`)

```go
type StatResult struct {
    Size     int64
    IsDir    bool
    Revision int64
    Mtime    time.Time
    UID      int64   // new
    GID      int64   // new
    Mode     uint32  // new
}

type FileInfo struct {
    Name  string
    Size  int64
    IsDir bool
    Mtime int64
    UID   int64   // new
    GID   int64   // new
    Mode  uint32  // new
}

func (c *Client) ChmodCtx(ctx context.Context, path string, mode uint32) error
func (c *Client) ChownCtx(ctx context.Context, path string, uid, gid int64) error
```

### 4.5 FUSE Layer (`pkg/fuse/dat9fs.go`)

**`fillAttr`**: use real values from `InodeEntry` instead of hard-coding:

```go
func (fs *Dat9FS) fillAttr(entry *InodeEntry, out *gofuse.Attr) {
    out.Ino = entry.Ino
    out.Size = uint64(entry.Size)
    out.Uid = uint32(entry.UID)
    out.Gid = uint32(entry.GID)
    // entry.Mode stores permission bits only; combine with file-type bit
    if entry.IsDir {
        out.Mode = syscall.S_IFDIR | entry.Mode
    } else {
        out.Mode = syscall.S_IFREG | entry.Mode
    }
    // ...
}
```

**`SetAttr`**: handle `FATTR_MODE`, `FATTR_UID`, and `FATTR_GID`:

```go
if valid&fuse.FATTR_MODE != 0 {
    // invoke backend.Chmod
}
if valid&fuse.FATTR_UID != 0 || valid&fuse.FATTR_GID != 0 {
    // invoke backend.Chown
}
```

**`Mkdir`**: forward `MkdirIn.Mode` to the backend:

```go
func (fs *Dat9FS) Mkdir(...) {
    mode := uint32(in.Mode) & 0777  // filter the mode supplied by FUSE
    return fs.backend.MkdirCtx(ctx, path, mode)
}
```

**`Access`** (optional):
- When mounted **without** `allow_other`, the kernel checks permissions itself using the mode returned by `GetAttr`; **no `Access` implementation is required**.
- If `allow_other` is supported in the future (allowing other users to access the mount point), an `Access` check must be implemented.
- **Phase 1 recommendation: do not implement `Access`**; rely on the kernel's standard check.

### 4.6 WebDAV Layer (`pkg/webdav/fs.go`)

- `fileInfo.Mode()` uses `StatResult.Mode`
- `Mkdir` forwards `perm`

### 4.7 Schema Definitions (`pkg/tenant/schema/`)

Add columns in all three schema files:
- `tidb_auto.go` / `tidb_app.go` / `db9/schema.go`

```sql
CREATE TABLE IF NOT EXISTS file_nodes (
    node_id      VARCHAR(64) PRIMARY KEY,
    path         VARCHAR(512) NOT NULL,
    parent_path  VARCHAR(512) NOT NULL,
    name         VARCHAR(255) NOT NULL,
    is_directory BOOLEAN NOT NULL DEFAULT FALSE,
    file_id      VARCHAR(64),
    uid          INT NOT NULL DEFAULT 0,
    gid          INT NOT NULL DEFAULT 0,
    mode         INT UNSIGNED NOT NULL DEFAULT 420,
    created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);
```

## 5. Security and Authorization Boundaries

**Key insight**: drive9's current authentication is based on an **API Key / Capability Token** model, not Unix UID/GID authentication. Therefore:

| Layer | Behavior |
|-------|----------|
| **POSIX semantics layer** | `chmod`/`chown`/`ls -l` correctly read and write attributes so that toolchains (git, tar, cp -a) work normally |
| **Actual access control** | Still enforced by the server-side API Key authentication + possible future RBAC. The mode bits returned by the FUSE layer are mainly for the kernel's local checks and for applications to read |

This is analogous to **NFSv3** behavior: the server trusts the UID/GID sent by the client; real enforcement depends on the server's auth policy.

**Phase 1 exclusions**:
- The server does **not** reject HTTP requests based on file uid/gid (avoids breaking the existing auth model)
- ACL / `setfacl` / `getfacl` are **not** implemented
- Semantic enforcement of `sticky bit` / `setuid` / `setgid` is **not** implemented (the bits are only stored)

## 6. Backward Compatibility and Migration

### 6.1 Existing Data
- TiDB/MySQL: `ALTER TABLE ... ADD COLUMN ... DEFAULT`
- Default values guarantee unchanged behavior (`0644` for files, `0755` for directories)
- db9 (PostgreSQL): same approach

### 6.2 Test Schema Helpers
All test helper files must be updated together:
- `pkg/datastore/schema_test_helper_test.go`
- `pkg/backend/schema_test_helper.go`
- `pkg/tenant/pool_test.go`

### 6.3 FUSE Tests
Existing tests such as `TestLookupOpenCreatedFileAfterForgetSupportsGitChmodLock` currently rely on "chmod is ignored" behavior. After the update:
- `chmod` will actually change the mode
- The test expectation needs adjustment: the test verifies that a git lock file can still be opened, and a mode change should not affect the open operation, so it should still pass

## 7. Recommended Implementation Steps

| Step | Content | Files |
|------|---------|-------|
| 1 | Add schema columns | `pkg/tenant/schema/*.go` |
| 2 | Datastore model + SQL | `pkg/datastore/store.go`, `file_tx.go`, `dir_tx.go` |
| 3 | Backend interface implementation | `pkg/backend/dat9.go` |
| 4 | Server HTTP API | `pkg/server/server.go` (stat/mkdir/chmod/chown) |
| 5 | Go SDK client | `pkg/client/client.go` |
| 6 | FUSE layer | `pkg/fuse/dat9fs.go` (fillAttr, SetAttr, Mkdir, InodeEntry) |
| 7 | WebDAV layer | `pkg/webdav/fs.go` |
| 8 | Test updates | Per-package test schema helpers + specific tests |
| 9 | Schema dump command update | Run `drive9-server schema dump-init-sql` per `AGENTS.md` |
