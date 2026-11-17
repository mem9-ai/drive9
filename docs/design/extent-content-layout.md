---
title: Extent Content Layout v2 - Upstream JuiceFS VFS on Drive9 Presigned Blocks and Classic Schema
status: active
date: 2026-11-17
---

# 1. Overview

`extent` is drive9's block-addressed content layout for large files, random overwrites, `mmap`, repeated `fsync`, and SQLite workloads.

Unlike the earlier prototype, **Extent Content Layout v2** achieves five strict architectural invariants:

1. **Upstream JuiceFS `vfs.VFS` (`github.com/juicedata/juicefs v1.4.1`, zero fork)**:
   `pkg/extensions/extent` registers an in-process `meta.Meta` (`drive9://`) and `object.ObjectStorage` (`drive9-presign`) via upstream `meta.Register` and `vfs.NewVFS`, keeping upstream `DataReader`, `DataWriter`, `chunk.ChunkStore`, and kernel coherence untouched without maintaining a fork.
2. **Per-object presigned URLs (no long-lived STS credentials)**:
   Clients never receive bucket-wide STS tokens (`/v1/data-credential` is unused). Reads obtain short-lived per-block presigned GET URLs from `GET /v1/fs/{path}?extent-plan=1&offset=...&length=...`; writes obtain per-block presigned PUT URLs inside upload sessions (`/v2/uploads` -> `/parts` -> `/complete`). Content-addressed SHA-256 block keys skip redundant uploads when already referenced on the server.
3. **100% server-side compaction and GC**:
   Clients disable all JuiceFS background jobs (`NoBGJob = true`, `MaxDeletes = 0`, no client-side `CompactChunk` or `DeleteSlice` workers). Slice compaction, unreferenced block GC, stale presign sweep, orphan S3 block scan, and terminal maintenance row retention all execute exclusively on `drive9-server` (`tidbcloud/fs`).
4. **Classic Drive9 schema as the single metadata source of truth**:
   There are **no** parallel `jfs_node`/`jfs_edge` namespace tables, no projection columns, and no dual-writes. Extent files live in the exact same `file_nodes`, `inodes`, and `contents` tables as classic files (`contents.storage_type = 'extent'`, `inodes.size_bytes` = logical file size for quota), augmented only by extent block/maintenance tables (`extent_chunks`, `extent_block_refs`, `extent_block_gc`, `extent_compact_tasks`, `extent_presigned_blocks`, `fs_locks`).
5. **Zero regression to classic small files or existing `multipart` files**:
   - Small files (`< inline_threshold`, ~50 KiB) keep the classic inline DB/write-back path untouched (with automatic `/v1/fs:batch-write` coalescing under backlog).
   - New large files (and SQLite family files in `coding-agent`, or all new files in `extent`) write as `content_layout = extent` instead of classic `multipart`.
   - Existing classic `single` / `multipart` (`storage_type = 's3'`) files remain 100% readable and updatable on their classic path without implicit conversion.

---

# 2. Routing and Profiles

| Profile | Small non-SQLite files (`< inline_threshold`) | Large new files (`>= inline_threshold`) | SQLite family (`*.db`, `*.sqlite*`, `-wal`, `-shm`, `-journal`, `-mj*`) | Existing classic `single`/`multipart` files |
|---|---|---|---|---|
| `coding-agent` (default) | Classic inline + write-back (`/v1/fs:batch-write` under backlog) | Promoted to `extent` via `CommitQueue` -> `WriteExtentStreamConditional`, then live handle migrated via `OpenExisting` | Claimed on `extent` from byte 0 | Stay on classic plane |
| `extent` / `coding-agent-extent` | Claimed on `extent` from byte 0 (`*` pattern, `Threshold = 0`) | Claimed on `extent` from byte 0 | Claimed on `extent` from byte 0 | Stay on classic plane (`tryClaimExistingLocked` declines `ContentLayout != extent`) |

### Zero-byte extent creation
When an `extent`-routed file is synced or closed at 0 bytes (`open(O_CREAT) -> fsync()` or `close()`), the client calls `POST /v1/fs/{path}?create=1&layout=extent` (`CreateExtentFileCreatedCtx`), creating a genuine 0-byte `content_layout = extent` inode in 1 RPC (falling back cleanly to a replaceable empty placeholder against older servers). Subsequent writes commit incremental `PATCH` sessions directly onto that inode without ever degrading to whole-file classic uploads.

---

# 3. Client Architecture (`pkg/extensions/extent`)

```
kernel FUSE
   │
   ▼
Dat9FS (namespace cache, permissions, local-only overlays, SSE invalidation)
   │
   ├─ extent.Handler (claims existing ContentLayout=extent files + pattern/profile creates)
   │     ├─ vfs.VFS (upstream JuiceFS v1.4.1 DataReader / DataWriter / ChunkStore)
   │     ├─ extent.Bridge (meta.Meta adapter: path <-> synthetic ino, 64 MiB commit batching, CAS revisions)
   │     └─ extent.objectStorage (stages dirty slices within bounded 384 MiB RAM budget; fetches presigned GET ranges)
   │
   └─ writeback.Writeback (classic inline + shadow staging)
         ├─ WriteBackUploader (small files -> auto /v1/fs:batch-write with atomic mode under backlog)
         └─ CommitQueue (large files -> parallel 16-way WriteExtentStreamConditional + live handle migration to extent.Handler)
```

### Key Client Properties
- **Bounded memory & write backpressure**: When JuiceFS native disk writeback is off, `objectStorage.Put` stages 4 MiB blocks in RAM under a strict `384 MiB` total budget (`maxPinnedCommitBytes`) with ascending slice-ID admission so concurrent `MaxUploads` workers never starve earlier slices or exceed container memory limits.
- **Batched multi-block PATCH commits**: `Bridge.Write` accumulates up to `64 MiB` (`commitBatchBytes`) per commit round-trip (`InitiateExtentUploadV2` -> `PresignExtentPartsV2` -> parallel PUTs -> `CompleteUploadV2`) and binds the returned `resource_id` and `revision` directly without post-commit `HEAD` polling.
- **Parallel sequential stream upload**: `uploadExtentSequential` (`pkg/client/extent_v2.go`) reads each 4 MiB block once, hashes in parallel, presigns in 32-part batches, and uploads missing blocks with 16-way concurrency (with single-part create-once conflict re-presign).
- **Live descriptor migration on `fsync`**: When a buffered writer promotes a file to `extent` in `coding-agent`, `scheduleExtentMigration` opens an `extent.Handler` descriptor verified against `LandedResourceID`, and strict `Sync()` awaits migration completion before returning so subsequent `pwrite + fsync` loops issue 4 KiB extent patches rather than re-uploading the full image.

---

# 4. Server Storage Model, Quota, Compaction, and GC (`tidbcloud/fs`)

### 4.1 Schema (Shared Classic Namespace + Block Tables)
- **Authoritative namespace & quota**: `file_nodes`, `inodes` (`size_bytes` = logical file size), `contents` (`storage_type = 'extent'`, `object_key = ''`).
- **Block & maintenance tables**:
  - `extent_chunks`: `(tenant_id, file_id, chunk_index)` -> compact binary slice list `(file_offset, length, block_offset, block_key)` covering 64 MiB windows.
  - `extent_block_refs`: `(tenant_id, block_key)` -> `ref_count`, `size_bytes` for content-addressed S3 objects under `t/<tenant>/eb/<h0:2>/<h2:4>/<sha256>`.
  - `extent_presigned_blocks`: tracks blocks presigned in active upload sessions so `RunExtentOrphanScan` never deletes in-flight uploads, and sweeps stale abandoned sessions into `extent_block_gc`.
  - `extent_compact_tasks`: leased per-chunk compaction queue (`PENDING` / `COMPACTING` / `COMPLETED`).
  - `extent_block_gc`: leased zero-ref S3 block deletion queue (`PENDING` / `DELETING` / `COMPLETED` / `FAILED`) with grace window (`delete_after`) to protect concurrent readers.
  - `fs_locks`: lease-backed `flock` / POSIX byte-range locks (`setlk` / `getlk`).

### 4.2 Quota Accuracy
Customer quota queries `SUM(i.size_bytes)` over live `inodes` (excluding transient `PENDING` upload staging inodes).
- Overwriting 4 KiB inside a 1 GiB extent file leaves `inodes.size_bytes = 1 GiB` unchanged (temporary uncompacted S3 slices do **not** inflate logical customer quota).
- `truncate` updates `inodes.size_bytes` immediately in the same transaction.
- Server-side `ExtentCompactCommit` replaces shadowed slice runs under a `chunk_version` CAS without changing `inodes.size_bytes`.
- Hardlink copy-on-write (`CloneExtentInodeForWriteTx`) clones `extent_chunks` and increments `extent_block_refs.ref_count` atomically when breaking shared links on scoped writes.

### 4.3 Server-Side Compaction, Block GC, and Retention
1. **Compaction (`RunExtentCompaction`)**: Triggered when a chunk exceeds the slice threshold; merges visible spans into `<= 4 MiB` blocks while preserving sparse holes (`> 64 KiB` gaps), uploads merged blocks server-side, and commits via `ExtentCompactCommit` (or enqueues newly uploaded blocks into `extent_block_gc` if `ErrExtentChunkChanged` loses a concurrent write race).
2. **Block GC (`RunExtentBlockGC`)**: Deletes S3 objects whose `extent_block_refs.ref_count == 0` after `delete_after` expires. If a new write re-references a block whose `extent_block_gc` row is still an untouched `PENDING` orphan candidate (`attempt_count == 0`), `extentApplyRefDeltaTx` cancels the GC row instead of failing the commit; blocks actively `DELETING` or inside a post-ref grace fence return `ErrExtentBlockDeleting` so the client re-presigns cleanly.
3. **Orphan Scan &Bounded Queue Retention**: `RunExtentOrphanScan` pages S3 with a rolling cursor, batch-filters keys via `ExtentFilterOrphanCandidates`, and purges `COMPLETED` rows from `extent_block_gc` and `extent_compact_tasks` older than 7 days (`ExtentPurgeTerminalMaintenance`).

---

# 5. Known Follow-Up TODOs

1. **CLI / Go SDK default large-file write path (`pkg/client/transfer.go`)**:
   `drive9 cp` and SDK `Client.WriteStream` / `Client.Upload` currently keep classic `multipart` (`storage_type = 's3'`) as the default for new large files while FUSE mounts exercise `extent` in production; switch CLI/SDK large-file uploads to `WriteExtentStreamConditional` once rollout stabilizes.
2. **Multimodal semantic extraction on `extent` (`pkg/backend/extent.go`)**:
   Server-side multimodal extraction (`image_extract` / `audio_extract` / `video_extract`) currently skips `StorageExtent` inodes rather than enqueuing tasks that expect a single S3 object key; add an extent reader stream adapter in the semantic worker when customers enable multimodal indexing on large media files.
