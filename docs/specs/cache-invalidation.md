# Cache Invalidation Spec

> Status: DRAFT — requires review by @adversary-1, @adversary-2 before any P1 cache implementation begins.
>
> Issue: #485

## 1. Purpose

Define the invalidation semantics for all FUSE-side caches (read cache, directory/stat cache, negative cache). Every cache implementation in the FUSE layer MUST follow this spec.

**Consistency invariant**: When the SSE connection is healthy, no cache may serve data that contradicts the server's current revision. During SSE disconnect, bounded staleness is permitted under the lazy revalidation rules defined in §5:
- **File read cache**: stale until the next read triggers a HEAD revalidation check.
- **Directory/stat cache**: stale until TTL expires (directory revision not yet exposed by server API).
- **Negative cache**: stale for up to `NegativeTimeout` (default 1 second).

These staleness windows are strictly bounded and documented. Upon SSE reconnect, either event replay or full reset eliminates all staleness.

### 1.1 Current State vs Proposed

This spec describes the **target architecture** for P1 cache implementation. The table below clarifies what exists today vs what needs to be built.

| Component | Current State | Proposed (this spec) |
|---|---|---|
| File read cache | In-memory LRU, whole-file, path-keyed, revision-aware (`pkg/fuse/read.go`) | Add disk-backed layer (#486) with byte-range blocks; in-memory layer unchanged |
| Directory cache | TTL-based with positive/negative entries, `CachedFileInfo` has `Revision` field (`pkg/fuse/dir.go`) | Add SSE-driven invalidation on top of existing TTL |
| Stat/attr cache | Embedded in directory cache | Same, with revision-bound validity |
| Negative cache | Embedded in directory cache (`NegativeTimeout` = 1s) | Same semantics, formalized rules |
| SSE invalidation | `pkg/fuse/sse.go` — handles `ChangeEvent` and `ResetEvent`, self-filters by actor | Extend to cover new disk cache layer |
| Disk read cache | Does not exist | New (#486) |
| Singleflight | Does not exist | New (#488) |

**API prerequisites** (NOT in scope of this spec, tracked as separate issues):

- **Directory revision**: `handleStat` only returns `X-Dat9-Revision` when `nf.File != nil` (server.go:1865-1866). Directories have no revision exposed. Until this is fixed, directory cache validity relies on SSE invalidation + TTL only (no revision-based lazy revalidation for directories).

## 2. Definitions

- **revision**: A monotonically increasing integer assigned by the server to each file mutation. Every write and chmod produces a new revision.
- **SSE**: Server-Sent Events stream from `/v1/events`. Delivers `ChangeEvent` (per-path, for write/upload_complete/create/symlink ops), `ResetEvent` (full invalidation, including for structural ops like rename/delete/mkdir/copy), and heartbeat/current markers after replay/reset catch-up.
- **trusted event stream**: An SSE stream that is safe to use as a freshness guarantee for revision-bound stat-cache hits. The current server event bus is process-local, so the stream is trusted only for single-server/sticky-routing deployments, or for future deployments with a cluster-wide durable event stream. Mounts must fail closed unless the operator explicitly enables this trust boundary.
- **stale**: Cache entry whose revision is older than the server's current revision for that path.
- **orphan**: Cache entry for a path/file that no longer exists on the server.
- **structural op**: An operation that affects paths beyond the single target (rename, delete, mkdir, copy). The server converts these to `ResetEvent` because targeted single-path invalidation cannot reliably cover old paths, subtrees, and parent directory caches.

## 3. Cache Key Structure

### 3.1 File Read Cache

**Current (in-memory):**

```text
Key = (path)
Value = { data: []byte, revision, expires }
```

- `path`: Absolute path within the mount. This is the key used by the existing `ReadCache` in `pkg/fuse/read.go`.
- `revision`: The file revision at the time of fetch.
- Whole-file cache (no byte-range splitting). Maximum file size: `read-cache-max-file` (default 1MB).
- A cache hit is valid ONLY if the entry's revision matches the current known revision for that file.

**Proposed (disk-backed, #486):**

```text
Key = (path, revision, offset, length)
```

- `path`: Same path used by the in-memory layer.
- `revision`: The file revision at the time of fetch.
- `offset` + `length`: Byte range within the file (block-aligned).
- Disk cache stores larger files in fixed-size blocks.
- A cache hit is valid ONLY if the entry's revision matches the current known revision for that file.

### 3.2 Directory Cache

```text
Key = (dir_path)
Value = { entries: [...], expires: timestamp }
```

- `dir_path`: Absolute path of the directory within the mount.
- `expires`: TTL-based expiry (primary invalidation mechanism for directories, since directory revision is not yet exposed by the server API — see §1.1).
- SSE invalidation (ChangeEvent for child paths, or ResetEvent for structural ops) also triggers invalidation.
- A cache hit is valid ONLY if: (a) no SSE invalidation has been received for this directory since the entry was stored, AND (b) the entry is not marked "unverified" due to SSE disconnect.

### 3.3 Stat/Attr Cache

```text
Key = (file_path)
Value = { size, mtime, mode, revision, expires: timestamp }
```

- Stat cache entries are stored as part of the directory cache (readdir returns stat info).
- Individual stat lookups may also populate this cache.
- For files: `revision` field enables revision-based validity checks.
- For directories: `revision` is not available from the server API (see §1.1). Validity relies on SSE + TTL only.

### 3.4 Negative Cache

```text
Key = (parent_dir, child_name)
Value = { expires: timestamp }
```

- Records "this name does not exist in this directory" for a bounded time.
- Speeds up `git status` scanning non-existent paths.
- Default TTL: 1 second (matches JuiceFS `NegativeTimeout`).

## 4. Invalidation Triggers

### 4.0 Operation → SSE Event Routing

Every server mutation maps to exactly one SSE event type. This table is the authoritative routing for cache invalidation:

| Server operation | SSE event | Invalidation scope | Notes |
|---|---|---|---|
| `write` | ChangeEvent | Targeted (path P) | File content changed |
| `upload_complete` | ChangeEvent | Targeted (path P) | Multipart upload finalized |
| `create` | ChangeEvent | Targeted (path P) | New file/symlink |
| `symlink` | ChangeEvent | Targeted (path P) | New symbolic link |
| `rename` | ResetEvent | Full (all caches) | Source path, dest path, both parents, subtrees affected |
| `delete` | ResetEvent | Full (all caches) | Path + parent dir + potential subtree |
| `mkdir` | ResetEvent | Full (all caches) | Parent dir structure changed |
| `copy` | ResetEvent | Full (all caches) | Source metadata + dest path + dest parent |
| `chmod` | ChangeEvent | Targeted (path P) | Mode metadata changed |

**Why structural ops use ResetEvent**: Targeted single-path invalidation cannot reliably cover all affected caches. For example, `rename(A, B)` affects: read cache for A, stat cache for A, stat cache for B, dir cache for `parent(A)`, dir cache for `parent(B)`, negative cache for `(parent(B), basename(B))`, and any subtree under A if A is a directory. Full reset is the safe default; future optimization MAY use the `path`/`op` payload fields for targeted invalidation.

### 4.1 SSE ChangeEvent

The server sends `ChangeEvent` for non-structural operations: `write`, `upload_complete`, `create`, `symlink` (see §4.0 routing table). All ChangeEvent ops use the same invalidation logic because they affect a single path.

When SSE delivers a `ChangeEvent` for path P:

1. **File read cache**: Invalidate ALL entries where path matches P (both in-memory and disk layers).
2. **Directory cache**: Invalidate the entry for `parent(P)` (the child list changed).
3. **Stat cache**: Invalidate the entry for P (size/mtime/revision changed).
4. **Negative cache**: Remove negative entry for `(parent(P), basename(P))` (path may now exist).
5. **Kernel cache**: Notify kernel via `NotifyInvalInode` and `NotifyInvalEntry`.

This is already implemented in `pkg/fuse/sse.go:handleChange()` for the in-memory read cache and kernel notifications. Disk cache invalidation (#486) will extend this.

### 4.2 SSE ResetEvent

The server sends `ResetEvent` in these cases:

1. **Structural operations**: rename, delete, mkdir, copy — converted by `isStructuralOp()` in `pkg/server/sse.go:313-319`. These use full invalidation because targeted invalidation cannot reliably cover source paths, destination paths, parent directories, subtrees, and negative cache entries across all affected locations (see §4.0 for per-op rationale).
2. **Sequence gap**: Client reconnects with a `since` value that the server's ring buffer can no longer replay (`seq_too_old`).
3. **Server restart**: Client's `since` is ahead of the server's current head (`server_restart`).
4. **Initial sync**: Client connects with `since=0` (`initial_sync`).

When SSE delivers a `ResetEvent`:

1. Invalidate ALL entries in ALL caches (read, dir, stat, negative).
2. Notify kernel for all known inodes.

This is already implemented in `pkg/fuse/sse.go:handleReset()`.

**Note**: Structural-op resets carry `path`, `op`, and `actor` fields in the payload. Future optimization MAY use these for targeted invalidation instead of full reset, but the current spec requires full invalidation for safety.

### 4.3 Local Mutation

When this mount performs a write, rename, unlink, mkdir, or chmod:

1. Invalidate affected cache entries immediately (before the server responds).
2. Do NOT wait for SSE echo — the local mutation is authoritative for this mount.
3. SSE self-filtering (`actor` field matching) prevents double-invalidation. Each mount instance MUST use a unique actor ID.

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
   - For **files**: Issue a lightweight HTTP HEAD to the server (`handleStat`, which returns `X-Dat9-Revision`). Compare revision. If match: re-verify the entry, clear "unverified" flag. If mismatch: invalidate and re-fetch.
   - For **directories**: Directory revision is not exposed by the current API (see §1.1). During SSE disconnect, directory cache entries fall back to TTL-only validity. After TTL expires, the next readdir fetches from server.
4. When SSE reconnects:
   - The client reconnects with its last seen sequence number (`WatchEvents` in `pkg/client/events.go:50`).
   - If the server's ring buffer can replay events since that sequence: server replays missed events. Each replayed event triggers normal invalidation (§4.1/§4.2). The server then emits a heartbeat/current marker. Only that marker can clear the "unverified" flag for entries that survive replay without invalidation.
   - If the server cannot replay (sequence too old, server restart): server sends a `ResetEvent`, then a heartbeat/current marker. All caches are fully invalidated before the stream is considered verified.

Revision-bound stat-cache hits may use the verified state only when the mount is configured to trust the SSE deployment boundary. With the current process-local event bus, default mounts do not trust SSE for regular-file `GetAttr` freshness and must fall back to remote HEAD revalidation.

**Rationale**: Option A (block reads) would make the mount unusable during network hiccups. Option B (bounded stale) is complex to tune. Option C is pragmatic: reads continue with lazy revalidation, and missed events are replayed or full invalidation happens on reconnect.

**Staleness window**: Between SSE disconnect and the next read of a given path, data may be stale. This is bounded by:
- For files: the time until the next read triggers a HEAD revalidation check.
- For directories: the remaining TTL on the directory cache entry.
- For negative cache: the remaining TTL (default 1s). A file created remotely during disconnect may be masked by a negative entry for up to 1 second.

**Acceptable because**: The current system already has TTL-based caching with similar staleness characteristics. Option C makes it strictly better for files (revision-check on read vs pure TTL expiry). For directories, behavior is unchanged until the server exposes directory revisions.

## 6. Multi-Mount Isolation

When multiple mounts access the same workspace:

1. Mount A writes file F -> server updates revision -> SSE broadcasts ChangeEvent (or ResetEvent for structural ops).
2. Mount B receives the event -> invalidates its cache for F.
3. Mount B's next read of F fetches the new revision from server.

**Requirement**: Each mount instance MUST use a unique actor ID for SSE self-filtering. Two mounts sharing an actor ID would cause SSE events from one mount to be silently filtered by the other, leading to stale caches.

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
| SSE ResetEvent (any reason) | Remove ALL negative entries |
| Local `create(path)` | Remove negative entry for `(parent(path), basename(path))` |
| Directory cache invalidated | Remove ALL negative entries for that directory |
| TTL expires | Remove negative entry |

**NegativeTimeout**: Configurable, default 1 second. Must be short to avoid masking newly created files.

**SSE disconnect behavior**: During SSE disconnect, negative entries continue to use TTL-only expiry. A file created remotely may be masked for up to `NegativeTimeout` (1s default). This is acceptable given the short default TTL.

## 9. Test Scenarios

Each scenario MUST have a corresponding test before the cache implementation is merged.

### 9.1 Basic correctness
- T1: Read file -> cache hit -> read again -> served from cache (same revision)
- T2: Read file -> SSE ChangeEvent -> read again -> fetched from server (new revision)
- T3: Readdir -> cache hit -> readdir again -> served from cache
- T4: Readdir -> SSE ChangeEvent for child -> readdir again -> fetched from server

### 9.2 Revision binding
- T5: Cache entry with revision R1, server has R2 -> cache miss, re-fetch
- T6: Cache entry with revision R1, server still R1 -> cache hit
- T7: File deleted -> SSE ResetEvent (structural op) -> cache entry invalidated

### 9.3 SSE disconnect
- T8: SSE disconnects -> reads continue (lazy revalidation)
- T9: SSE disconnects -> file read triggers HEAD check -> revision matches -> serve cached
- T10: SSE disconnects -> file read triggers HEAD check -> revision differs -> re-fetch
- T11: SSE reconnects with replay -> missed events applied -> affected entries invalidated
- T12: SSE reconnects with reset (seq_too_old) -> all caches invalidated

### 9.4 SSE event types
- T13: Structural op (rename) -> ResetEvent received -> all caches invalidated
- T14: Non-structural op (write) -> ChangeEvent received -> targeted invalidation
- T15: Self-event filtering -> local write does not trigger SSE-driven re-invalidation

### 9.5 Multi-mount
- T16: Mount A writes -> Mount B's cache invalidated via SSE -> B reads new data
- T17: Mount A writes -> SSE delayed -> Mount B reads stale -> SSE arrives -> B's cache invalidated

### 9.6 Negative cache
- T18: stat(nonexistent) -> negative cached -> stat again -> ENOENT from cache
- T19: stat(nonexistent) -> negative cached -> SSE ChangeEvent for path -> negative removed -> stat goes to server
- T20: Negative TTL expires -> next stat goes to server
- T21: SSE ResetEvent -> all negative entries removed

### 9.7 Eviction vs invalidation
- T22: Cache full -> LRU evicts entry -> re-read fetches with current revision
- T23: Cache entry invalidated -> even if capacity available, entry not served

### 9.8 Concurrent access
- T24: Two goroutines read same file simultaneously -> singleflight, one HTTP call
- T25: Read and SSE invalidation race -> read returns either old or new, never corrupt

### 9.9 Local mutation
- T26: Local write -> cache entry invalidated immediately (before server response)
- T27: Local mkdir -> directory cache for parent invalidated
- T28: Local write fails (server 500) -> cache entry stays invalidated (not restored to pre-write state)

### 9.10 Structural operation boundaries
- T29: Remote rename(A, B) -> ResetEvent -> read cache for A invalidated, dir cache for parent(A) and parent(B) invalidated, negative cache for B cleared
- T30: Own rename(A, B) -> local invalidation covers A + B + parents -> SSE self-filters the echo ResetEvent -> no double-invalidation
- T31: Remote delete of directory with children -> ResetEvent -> all caches invalidated including subtree entries

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
- This spec does NOT define server-side API changes (chmod SSE event, directory revision API). Those are tracked as separate prerequisite issues (see §1.1).
- This spec does NOT define cache storage format (on-disk layout, compression, checksums). That is an implementation detail for #486.
