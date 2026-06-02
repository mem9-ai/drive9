# Cache Invalidation Spec

> Status: DRAFT — requires review by @adversary-1, @adversary-2 before any P1 cache implementation begins.
>
> Issue: #485

## 1. Purpose

Define the invalidation semantics for all FUSE-side caches (read cache, directory/stat cache, negative cache). Every cache implementation in the FUSE layer MUST follow this spec. No cache may serve data that contradicts the server's current revision.

## 2. Definitions

- **revision**: A monotonically increasing integer assigned by the server to each file/directory mutation. Every write, rename, chmod, or delete produces a new revision.
- **SSE**: Server-Sent Events stream from `/v1/events`. Delivers `ChangeEvent` (per-path) and `ResetEvent` (full invalidation).
- **stale**: Cache entry whose revision is older than the server's current revision for that path.
- **orphan**: Cache entry for a path/file that no longer exists on the server.

## 3. Cache Key Structure

### 3.1 File Read Cache (disk-backed)

```
Key = (file_id, revision, offset, length)
```

- `file_id`: Stable file identifier (path or server-assigned ID).
- `revision`: The file revision at the time of fetch.
- `offset` + `length`: Byte range within the file.
- A cache hit is valid ONLY if the entry's revision matches the current known revision for that file.

### 3.2 Directory Cache

```
Key = (dir_path)
Value = { entries: [...], revision: dir_revision, expires: timestamp }
```

- `dir_path`: Absolute path of the directory within the mount.
- `revision`: The directory's revision at the time the listing was fetched.
- `expires`: TTL-based expiry as a secondary safeguard (not primary invalidation).
- A cache hit is valid ONLY if no SSE invalidation has been received for this directory since the entry was stored.

### 3.3 Stat/Attr Cache

```
Key = (file_path)
Value = { size, mtime, mode, revision, expires: timestamp }
```

- Stat cache entries are stored as part of the directory cache (readdir returns stat info).
- Individual stat lookups may also populate this cache.
- Validity: same as directory cache — SSE invalidation or revision mismatch.

### 3.4 Negative Cache

```
Key = (parent_dir, child_name)
Value = { expires: timestamp }
```

- Records "this name does not exist in this directory" for a bounded time.
- Speeds up `git status` scanning non-existent paths.
- Default TTL: 1 second (matches JuiceFS `NegativeTimeout`).

## 4. Invalidation Triggers

### 4.1 SSE ChangeEvent

When SSE delivers a `ChangeEvent` for path P:

1. **File read cache**: Invalidate ALL entries where `file_id` matches P.
2. **Directory cache**: Invalidate the entry for `parent(P)`.
3. **Stat cache**: Invalidate the entry for P.
4. **Negative cache**: Remove negative entry for `(parent(P), basename(P))`.
5. **Kernel cache**: Notify kernel via `NotifyInvalInode` and `NotifyInvalEntry`.

This is already implemented in `pkg/fuse/sse.go:handleChange()`.

### 4.2 SSE ResetEvent

When SSE delivers a `ResetEvent`:

1. Invalidate ALL entries in ALL caches (read, dir, stat, negative).
2. Notify kernel for all known inodes.
3. This is the nuclear option — used when the server cannot enumerate individual changes (e.g., bulk import, schema migration).

This is already implemented in `pkg/fuse/sse.go:handleReset()`.

### 4.3 Local Mutation

When this mount performs a write, rename, unlink, mkdir, or chmod:

1. Invalidate affected cache entries immediately (before the server responds).
2. Do NOT wait for SSE echo — the local mutation is authoritative for this mount.
3. SSE self-filtering (`actor` check) prevents double-invalidation.

### 4.4 Revision Mismatch on Server Response

When a server response includes a revision newer than the cached revision:

1. Invalidate the stale cache entry.
2. Cache the new data with the new revision.
3. This is a fallback — SSE should have already triggered invalidation.

## 5. SSE Disconnect Handling

**Decision: Option C — Lazy Revalidation**

When the SSE connection drops:

1. Mark all caches as "unverified" (set a flag, do NOT delete entries).
2. Continue serving cached data.
3. On next cache hit for an unverified entry:
   - Issue a lightweight HTTP HEAD / conditional GET to the server.
   - If revision matches: re-verify the entry, clear "unverified" flag.
   - If revision differs: invalidate and re-fetch.
4. When SSE reconnects:
   - Server sends a `ResetEvent` (current behavior).
   - All caches are fully invalidated.
   - "Unverified" flag is cleared (replaced by full invalidation).

**Rationale**: Option A (block reads) would make the mount unusable during network hiccups. Option B (bounded stale) is complex to tune. Option C is pragmatic: reads continue with lazy revalidation, and full invalidation happens on reconnect.

**Staleness window**: Between SSE disconnect and the next read of a given path, data may be stale. This is bounded by:
- The time until the next read triggers revalidation.
- The time until SSE reconnects and triggers a full reset.

**Acceptable because**: The current system already has TTL-based caching with similar staleness characteristics. Option C makes it strictly better (revision-check on read vs pure TTL expiry).

## 6. Multi-Mount Isolation

When multiple mounts access the same workspace:

1. Mount A writes file F → server updates revision → SSE broadcasts ChangeEvent.
2. Mount B receives ChangeEvent → invalidates its cache for F.
3. Mount B's next read of F fetches the new revision from server.

**Edge case: SSE delivery delay**

- SSE delivery is not instantaneous. Mount B may read stale data between A's write and B receiving the SSE event.
- This is acceptable: the staleness window is bounded by SSE delivery latency (typically <100ms on LAN).
- If this is unacceptable for a specific use case, the caller should use explicit coordination (e.g., `fsync` + notification outside the filesystem).

**Edge case: Both mounts write simultaneously**

- Server's revision-based conflict detection (409 Conflict) handles this.
- FUSE-side cache is not involved in write conflict resolution.

## 7. Cache Eviction vs Invalidation

These are fundamentally different operations:

| | Eviction | Invalidation |
|---|---|---|
| Trigger | Capacity limit / LRU | Revision change / SSE |
| Meaning | Data removed for space | Data is stale |
| Re-fetch | Allowed, same revision | Required, new revision |
| Safety | Can serve stale if re-cached | MUST NOT serve stale |

Implementation requirement: Invalidation MUST take priority over eviction. An invalidated entry must be marked as invalid, not just evicted. If an evicted entry is re-fetched, it must be fetched with the current revision, not the old one.

## 8. Negative Cache Rules

| Event | Action |
|---|---|
| `stat(path)` returns ENOENT | Cache `(parent, name)` as negative, TTL = `NegativeTimeout` |
| SSE ChangeEvent for `path` | Remove negative entry for `(parent(path), basename(path))` |
| Local `create(path)` | Remove negative entry for `(parent(path), basename(path))` |
| Directory cache invalidated | Remove ALL negative entries for that directory |
| TTL expires | Remove negative entry |
| SSE ResetEvent | Remove ALL negative entries |

**NegativeTimeout**: Configurable, default 1 second. Must be short to avoid masking newly created files.

## 9. Test Scenarios

Each scenario MUST have a corresponding test before the cache implementation is merged.

### 9.1 Basic correctness
- T1: Read file → cache hit → read again → served from cache (same revision)
- T2: Read file → SSE invalidate → read again → fetched from server (new revision)
- T3: Readdir → cache hit → readdir again → served from cache
- T4: Readdir → SSE invalidate parent → readdir again → fetched from server

### 9.2 Revision binding
- T5: Cache entry with revision R1, server has R2 → cache miss, re-fetch
- T6: Cache entry with revision R1, server still R1 → cache hit
- T7: File deleted (revision gone) → cache entry orphaned → invalidated by SSE

### 9.3 SSE disconnect
- T8: SSE disconnects → reads continue (lazy revalidation)
- T9: SSE disconnects → read triggers HEAD check → revision matches → serve cached
- T10: SSE disconnects → read triggers HEAD check → revision differs → re-fetch
- T11: SSE reconnects → ResetEvent → all caches invalidated

### 9.4 Multi-mount
- T12: Mount A writes → Mount B's cache invalidated via SSE → B reads new data
- T13: Mount A writes → SSE delayed → Mount B reads stale → SSE arrives → B's cache invalidated

### 9.5 Negative cache
- T14: stat(nonexistent) → negative cached → stat again → ENOENT from cache
- T15: stat(nonexistent) → negative cached → create file → negative removed → stat returns file
- T16: Negative TTL expires → next stat goes to server

### 9.6 Eviction vs invalidation
- T17: Cache full → LRU evicts entry → re-read fetches with current revision
- T18: Cache entry invalidated → even if capacity available, entry not served

### 9.7 Concurrent access
- T19: Two goroutines read same file simultaneously → singleflight, one HTTP call
- T20: Read and SSE invalidation race → read returns either old or new, never corrupt

## 10. Configuration

| Parameter | Default | Description |
|---|---|---|
| `cache-dir` | `~/.cache/drive9/<mount-hash>/` | Disk cache root |
| `cache-size` | `1GB` | Maximum disk cache size |
| `cache-free-ratio` | `0.1` | Minimum free disk ratio before cache stops writing |
| `dir-cache-ttl` | `10s` | Directory cache TTL (secondary to SSE invalidation) |
| `attr-cache-ttl` | `10s` | Attribute cache TTL |
| `negative-timeout` | `1s` | Negative lookup cache TTL |
| `read-cache-max-file` | `1MB` | Maximum file size for in-memory read cache |

## 11. Non-goals

- This spec does NOT cover write-back cache invalidation. Write-path caching requires GC/session safety (#490) first.
- This spec does NOT define server-side API changes. If the server needs to return additional data (e.g., stat info in readdir), that is a separate issue.
- This spec does NOT define cache storage format (on-disk layout, compression, checksums). That is an implementation detail for #486.
