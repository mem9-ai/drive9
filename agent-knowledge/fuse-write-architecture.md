# FUSE Write Architecture

## FUSE Write Semantics

FUSE writers (cp, dd, ffmpeg, editors, etc.) can exhibit the following behaviors:
- **Sequential append**: write monotonically increasing offsets (cp, dd, video recording)
- **Random write**: jump to arbitrary offsets before close (editors, databases)
- **Header/footer patching**: write sequentially, then go back to update magic bytes, checksums, or headers (MP4, ZIP, etc.)

The final file size is **not known until close**. A writer may truncate or extend at any point.

## Why We Cannot Eagerly Upload All Parts During Write()

Previous attempt: upload every part as soon as it's written during Write().
**Problem**: FUSE writers can revisit any offset before close. If we upload part 1 during Write(), and the writer later patches byte 0 of part 1 (header update), S3 already has the stale version. We'd need to detect the rewrite and re-upload, but the upload may have already been used in a Complete call.

**Lesson learned**: You can only eagerly upload parts that are **provably final** — i.e., parts that will never be modified again.

## Write Mode Classification

| Mode | Trigger | maxSize | Memory | Flush Path |
|------|---------|---------|--------|------------|
| Small file | size < 50KB | 64MB | O(size) | Direct PUT |
| New file, sequential | Create() or Open(O_TRUNC), append-only | 10GB | **~24MB constant** | Streaming FinishStreaming |
| New file, non-sequential | Create() or Open(O_TRUNC), back-write detected | 10GB | O(remaining_size) | FinishStreaming + re-upload dirty |
| Existing file, random write | Open(O_RDWR) without O_TRUNC | max(size*2, 1GB) | **O(touched_parts)** | PatchFile (dirty parts only) |

## Sequential Write Detection + Streaming Upload

For **pure sequential append** workloads (the common case for large files), parts behind the write cursor are provably final. The design:

1. Track `appendCursor` — the byte offset where the next sequential write should start
2. If `Write(offset, data)` has `offset == appendCursor`, it's sequential: update cursor to `offset + len(data)`
3. If `offset > appendCursor` (gap), still sequential (gap is zero-filled)
4. If `offset < appendCursor` (back-write), mark `sequential = false` permanently for this file handle

When `sequential == true` and the cursor crosses a part boundary, the completed part is submitted for background upload via `StreamUploader.SubmitPart()`. After upload completes, `WriteBuffer.EvictPart()` releases the memory.

### Memory Model — Sequential Streaming (10GB video example)

```
Write(0..8MB)       → part 0 fills → OnPartFull → SubmitPart(1)
                      WriteBuffer: part 0 (8MB)
                      SubmitPart copy: 8MB
                      StreamWriter copy: 8MB           ← peak ~24MB

                      ... upload completes ...
                      EvictPart(0) → delete part 0     ← back to ~8MB

Write(8MB..16MB)    → part 1 fills → same cycle
                      ...steady state ~24MB peak...

close()             → wait inflight → upload last partial part → Complete
```

Each inflight part exists in 3 copies briefly:
1. `WriteBuffer.parts[p]` — evicted after upload
2. `SubmitPart()` copy (`stream_upload.go`) — passed to goroutine
3. `StreamWriter.WritePart()` copy (`stream.go`) — released after S3 upload

Steady-state: **~24-40MB** for any file size up to 10GB.

### Back-Write After Eviction

If a writer back-writes to a part that was already uploaded and evicted:
1. `sequential` is set to `false` (no more streaming for this handle)
2. `ensurePart()` recreates the part as a zero-filled slice
3. Only the newly written bytes are meaningful; the original data is gone (already on S3)
4. The part is marked dirty and will be re-uploaded at flush time
5. A WARNING log is emitted

This is acceptable because:
- Pure sequential workloads (cp, dd, ffmpeg -o) never trigger this
- It's strictly better than EFBIG (refusing the write entirely)
- The alternative (downloading the part back from S3) adds latency and complexity

## Existing File Random Write — Lazy Load + Sparse Buffer

When opening an existing file for writing without O_TRUNC (`Open(O_RDWR)`), the system uses a **lazy load** strategy via `preloadWritableHandle()`:

### Open-time behavior

```
Open(O_RDWR, "/large-file.bin")   // 800MB file
  └─ Stat() → size=800MB
  └─ partSize = CalcAdaptivePartSize(800MB)
  └─ bufMax = max(800MB * 2, 1GB) = 1.6GB    ← logical limit, NOT memory
  └─ WriteBuffer: totalSize=800MB, remoteSize=800MB
  └─ LoadPart callback installed (HTTP range read per part)
  └─ NO data loaded into memory yet           ← 0 bytes used
```

### Write-time behavior (lazy load on demand)

```
Write(offset=500MB, data=4KB)
  └─ partIdx = 500MB / partSize
  └─ ensurePart(partIdx)
       └─ part not in memory → LoadPart(partNum)
            └─ ReadStreamRange(500MB, partSize)   ← loads ONE part from server
       └─ parts[partIdx] = loaded data            ← only this part in memory
  └─ copy 4KB into part at correct offset
  └─ dirtyParts[partIdx] = true
```

### Key insight: `bufMax ≠ memory usage`

`bufMax` is the **logical write limit** (EFBIG if exceeded). Actual memory is determined by the sparse `parts map[int][]byte` — only parts touched by Write() or lazily loaded are in memory.

| File size | bufMax | Random writes to 3 parts | Actual memory |
|-----------|--------|--------------------------|---------------|
| 500MB | 1GB | 3 × 8MB parts loaded | ~24MB |
| 800MB | 1.6GB | 3 × 8MB parts loaded | ~24MB |
| 2GB | 4GB | 3 × 8MB parts loaded | ~24MB |

### bufMax calculation

```go
bufMax := stat.Size * 2                    // allow file to double in size
if bufMax < maxPreloadSize {               // at least 1GB
    bufMax = maxPreloadSize                // (maxPreloadSize = 1GB)
}
```

This means:
- 100MB file → bufMax = 1GB (floor), can grow to 1GB
- 800MB file → bufMax = 1.6GB, can grow to 1.6GB
- 3GB file → bufMax = 6GB, can grow to 6GB

### Flush behavior (PatchFile — dirty parts only)

```
close()
  └─ flushHandle() → Path 3 (existing large file)
  └─ dirtyParts = [part 62, part 125]       ← only 2 parts modified
  └─ PatchFile(size, dirtyParts, ...)
       ├─ Upload part 62 (modified data)
       ├─ Upload part 125 (modified data)
       └─ UploadPartCopy for all other parts  ← server-side copy, no data transfer
```

Only dirty parts are uploaded. Untouched parts use S3 `UploadPartCopy` (server-side, zero bandwidth).

### Why no streaming for existing file random writes

Existing file opens do NOT set `sequential = true` or wire `OnPartFull`. This is intentional:
- The file already has data on S3 that must be preserved
- Random writes may touch any part in any order
- Lazy load ensures original data is not lost
- PatchFile efficiently uploads only modified parts

## Flush Paths

| Path | Condition | Behavior |
|------|-----------|----------|
| 1a | Streamer has streamed parts | FinishStreaming: wait inflight, re-upload dirty, upload last part, complete |
| 1b | Streamer exists, no streamed parts, size >= 50KB | UploadAll: upload all parts at flush time |
| 2 | Small file (< 50KB) | Direct PUT |
| 3 | Existing large file, dirty parts only | PatchFile (UploadPartCopy for clean parts) |
| 4 | New large file, no streamer | WriteStream (full multipart) |

## Random Read Support

Random reads are fully supported and independent of the write path:

| Scenario | Read path | Memory |
|----------|-----------|--------|
| Read-only small file | ReadCache → client.Read() | Cached in memory |
| Read-only large file | Prefetcher → ReadStreamRange() | HTTP range read, on-demand |
| Writable handle with dirty data | WriteBuffer.ReadAt() | Reads from sparse parts map |
| Writable handle, no dirty data | Fall through → ReadStreamRange() | Server-side range read |

`ReadAt()` reads directly from individual parts without materializing the entire buffer.

## Comparison with mountpoint-s3

mountpoint-s3 (AWS's official FUSE for S3):
- **Strictly prohibits** random writes — only sequential append is allowed
- All parts are uploaded during write, no buffering at close
- Simpler but less compatible (can't handle editors, header-patching workflows)

Our approach:
- **Detects** sequential mode automatically
- Streams when possible, falls back to flush-time upload when not
- Supports both sequential and random writes (with different performance characteristics)
- Existing file random writes use lazy load + PatchFile (dirty-only upload)
- Back-write to evicted parts degrades gracefully (zero-fill, re-upload)

## Known Limitations

1. Back-writing to an evicted part loses the original data (replaced with zeros + new write)
2. Once `sequential` is set to false, it never returns to true for that file handle
3. Streaming upload uses `math.MaxInt64` as initial totalSize (server plan metadata only)
4. Maximum streaming file size is 10GB (streamingWriteMaxSize constant)
5. Existing file random write: memory grows with number of touched parts (O(touched_parts × partSize))
6. Double copy in streaming path: SubmitPart copies data, then StreamWriter.WritePart copies again (optimization opportunity)

## Constants Reference

| Constant | Value | Used for |
|----------|-------|----------|
| `defaultWriteBufferMaxSize` | 64MB | Default maxSize for generic WriteBuffer |
| `streamingWriteMaxSize` | 10GB | maxSize for new files (Create / Open+O_TRUNC) |
| `maxPreloadSize` | 1GB | Floor for bufMax in existing file opens |
| `DefaultPartSize` | 8MB | Default part size (v2 may use adaptive) |
| `smallFileThreshold` | 50KB | Below this: direct PUT; above: multipart |
| `uploadMaxConcurrency` | 16 | Max parallel part uploads in StreamWriter |
