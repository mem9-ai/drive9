---
title: Extent known issues (fix guide)
status: open
date: 2026-09-09
scope: content_layout=extent data plane (JuiceFS grafted as the extent storage/meta engine)
audience: engineer or agent picking up the fixes
---

# 0. How to read this document

- Each issue is organized as **symptom → root cause → evidence → how to verify → suggested fix → acceptance criteria**, so someone with no prior context can act on it directly.
- Every `file:line` is a **hint, not a contract**: line numbers drift as the code moves. **Locate by function name / constant name / SQL fragment first.**
- **[verified]** means I read the code line by line and confirmed it; **[to confirm]** means it is an inference — run a check before changing anything.
- Read **§1 Architecture in 30 seconds** and **§4 Do not "fix" these** first, so you do not turn intentional design into a bug.
- **Scope**: this document only covers the **extent implementation in the drive9 repo** (`pkg/datastore/jfs.go`, `pkg/server/extent_meta.go`, `pkg/fuse/extent*.go`, `pkg/extent/*`, `pkg/s3client/aws.go`). The `[append-log]` server half lives in a different repo, `tidbcloud/fs`; the drive9 repo only has the FUSE client side. Therefore `[append-log]` being inert under a local `provider=local` deployment **is not a defect** — do not file an issue for it or "add the server half". Likewise, whenever a conclusion touches **production** server behavior, confirm first which repo owns that code; do not change the wrong repo.

---

# 1. Architecture in 30 seconds

```
kernel FUSE ──> Dat9FS (drive9's FUSE frontend)
                  ├─ non-extent files → existing drive9 paths (single / append_log)
                  └─ extent files (basename matches a profile [extent] glob)
                        ├─ Create/Unlink/Lookup/Rename → JuiceFS meta driver (one RPC)
                        └─ Read/Write/Flush/Fsync/Truncate → vfs.DataReader/DataWriter
                                                      │
                                        ┌─────────────┴───────────────┐
                                  meta.Meta HTTP client        chunk.ChunkStore
                                  (mem9-ai/juicefs fork)              │
                                        │                      object.ObjectStorage
                                        ▼                      "drive9s3" (STS/static/file mock)
                              drive9-server /v1/extent/meta           │
                              one TiDB transaction:                   ▼
                                jfs_node/jfs_edge/jfs_chunk    S3 [serverPrefix]t/<tenant>/chunks/...
                                + file_nodes projection
```

Two invariants (**no fix may break these**):

1. **JuiceFS meta is the source of truth for an extent file's name/inode/length; `file_nodes` is a projection written in the same transaction.** Create/Unlink/Rename must stay "one RPC, one transaction"; the FUSE side must never dual-write.
2. **`content_layout` is an inode property, not a path property**: no UPDATE of the `content_layout` column exists anywhere in the repo (only INSERTs, plus UPDATEs of path/extent_ino/inode_id), and rename never converts layout. This is intentional; see §4.

---

# 2. Issue overview

| ID | Severity | One-liner | Impact |
|---|---|---|---|
| P0-1 | Critical | Deleting an extent file **reclaims no S3 blocks**: `jfs_delfile` has no consumer, and the slice list is deleted in the same transaction | Permanent space leak (main path) |
| P0-2 | Critical | The CAS loser of a compaction **never cleans up the blob it uploaded** | One leaked object per concurrent compact |
| P0-3 | Critical | Server-side directory operations (`fs rm -r` / `fs mv`) **never touch the jfs tree** | Orphan dir edges → empty dir cannot be removed, same-name create fails EEXIST, deletes leak |
| P0-4 | Critical | The local JuiceFS cache directory is **shared by every tenant** (hardcoded format UUID + slice ids restart per tenant) | Cross-tenant data leak/corruption through the read cache and the writeback staging dir |
| P1-1 | High | FUSE directory rename is two non-atomic steps and the second step's error is swallowed; the directory `extent_ino` is not exposed to clients | jfs directory tree diverges; behavior degrades across mount/remount |
| P1-2 | High | compact claim is an **overwrite-style lease**, and the self-feeding scan is **unindexed** | Duplicate merges, duplicate uploads, full table scans |
| P1-3 | High | block GC has **no grace period** (ignores `available_at`) | Readers on another mount can hit a block that was just deleted |
| P1-4 | Medium | The path glob participates in `Open`/`Lookup`'s by-name jfs probe | With an orphan edge present, a single-layout file is mis-bound to the extent data plane |
| P1-5 | Medium (perf, open) | Every kernel writeback batch costs one extra meta commit RPC (the fsx fix); `community.fio` 132 s → 227 s | ≈ +30 % on the write-heavy suites until the commit is folded into the write RPC |
| P1-7 | High (open) | A file written through an open handle is **invisible to remote readers until close**: `mount drain` reports idle queues and the remote read returns an empty body | `e2e/fuse-smoke-test.sh [9.1]` FAIL; the upload barrier alone does not fix it |
| P1-8 | High (open) | After `umount` + remount an extent file reads back as **size 0** on the new mount while the server/S3 have the full content | `e2e/fuse-supervision-test.sh [8]` FAIL; extent-only (profile `none` passes) |
| P2-1 | Medium | Projection size is not kept in sync (Write only updates `jfs_node.length`) | `ls -l` shows a stale size when the jfs overlay misses |
| P2-2 | Medium | STS grants that can be narrowed (DeleteObject / multipart / static branch has no IAM / TTL has no jitter / per-process mint) | Over-broad permissions + STS noise |
| P2-3 | Low | Server-side fallback compactor builds a new Runtime per task (new session never closed) + `WrapS3` reads the whole blob into memory | Session churn, memory spikes |
| Q-1 | To confirm | `TestExtentUnlinkEnqueuesBlockGC` disagrees with the implementation and may be red | Run it before touching P0-1 |

---

# P0-1 Deleting an extent file reclaims no S3 blocks

**Symptom** [verified]
After `rm` / `rm -r` / the last open-unlinked handle closing, **not a single object under `t/<tenant>/chunks/...` is deleted**. Only **compaction** reclaims space.

**Root cause**
`jfsEnqueueFileGCTx` does two things, and **nothing consumes either of them**:

```go
// pkg/datastore/jfs.go:1698
DELETE FROM jfs_chunk WHERE inode = ?                                  // (1) throws away the slice list
INSERT IGNORE INTO jfs_delfile (inode, length, expire) VALUES (?,?,?)  // (2) keeps only inode/length
```

1. **`jfs_delfile` has no consumer**: repo-wide the table appears only in the schema, this one INSERT, and `internal/testtidb`'s truncation list.
2. The fork side is stubbed anyway: `drive9_engine.go:653-657` has `doFindDeletedFiles` returning nothing and `doDeleteFileData` as a no-op; on top of that `conf.NoBGJob = true` (`pkg/extent/runtime.go:57`) means the `cleanupDeletedFiles` goroutine never starts (fork `pkg/meta/base.go:808-815`).
3. **Worse, point (1)**: upstream's `doDeleteFileData` (fork `pkg/meta/sql.go:3781`) **keeps the chunk rows** and walks them in the background → `deleteChunk` → slice refs-- → GC. Here `jfs_chunk` is deleted, so the delfile row only holds `(inode, length, expire)` — **not even the slice ids survive**, which means that even if a drain were added, it could not locate the object keys.

Call sites:
- `jfsUnlinkTx` (`jfs.go:1201`, nlink→0 and not open)
- `jfsDeleteSustainedTx` (`jfs.go:1726`, last open-unlinked handle released)

**Why it was written this way**: the comment at `jfs.go:1699-1702` says "Walking every 24-byte slice here made delete-journal COMMIT unlink take seconds (sqlite `--wait all` is 10s)" — i.e. walking slices inside the unlink transaction was too slow. The intent was to mirror JuiceFS's `MaxDeletes=0` model, but the "keep the chunk rows + drain them in the background" half was left out.

**How to verify**
- Unit test: `pkg/datastore/jfs_data_test.go:719 TestExtentUnlinkEnqueuesBlockGC` asserts that after unlink `ListPendingBlockGC` contains a `99_` key — which contradicts the current implementation (see Q-1; run it first).
- e2e: run `e2e/extent-rename-mixed-dir.sh` with `EXTENT_E2E_SQL_ORPHAN_CHECK=1`, or query `SELECT COUNT(*) FROM block_gc_tasks` / `SELECT COUNT(*) FROM jfs_delfile` through `POST /v1/sql`.

**Suggested fix (pick one)**
- **Option A (align with upstream)**: stop doing `DELETE FROM jfs_chunk` in `jfsEnqueueFileGCTx`; only write `jfs_delfile`. Then implement the drain in the server tenant worker: pick delfile rows with `expire <= now - grace` → walk that inode's `jfs_chunk` (100 chunks per round, like upstream) → `jfs_chunk_ref.refs-1` → write slices with refs≤0 into `block_gc_tasks` → delete the delfile row.
- **Option B (no schema change)**: keep the chunk rows inside the unlink/sustained transaction; **after it commits**, use a separate transaction (mirroring `runCompactOp`'s two-phase shape, `jfs.go:264-269`) to turn that file's slices directly into `block_gc_tasks` rows. Do not stretch the `FOR UPDATE` across an S3 call.

**Acceptance criteria**
- After unlinking an extent file that wrote a large slice, `block_gc_tasks` contains the matching `chunks/<id/1e6>/<id/1e3>/<id>_i_len` rows;
- After one server worker pass the S3 objects are gone and the rows are `COMPLETED`;
- If a crash lands between "projection deleted" and "GC rows inserted", the next scan pass still recovers it (eventual consistency is fine; permanent leakage is not).

---

# P0-2 The CAS loser of a compaction never cleans up the blob it uploaded

**Symptom** [verified]
When two executors merge the same chunk concurrently, only one wins the CAS; **the loser's freshly uploaded merged blob becomes a permanent orphan** (no `jfs_chunk_ref` row, no `block_gc_tasks` row, nothing can reclaim it).

**Root cause**
- Upstream calls `m.deleteSlice(id, size)` on CAS failure (`EINVAL`) (fork `pkg/meta/base.go:2896-2898`) to hand the just-uploaded slice back to GC.
- Two places cut that wire for us:
  1. `conf.MaxDeletes = 0` (`pkg/extent/runtime.go:58`) → `baseMeta.deleteSlice` returns immediately (fork `base.go:2996-2998`) → nothing enters `m.dslices`, the `delete_slice` op is never called.
  2. Our own `ExecuteCompact` does not call deleteSlice either: `pkg/extent/compact.go:44-47` returns the error as soon as `Drive9CommitCompact` is non-zero.
- Concurrency sources (**multiple mounts only amplify this; a single mount already hits it**):
  - read path: any chunk with ≥5 slices triggers `go compactChunk` (fork `base.go:2125`; `once=false` skips the 2500 threshold check);
  - write path: `numSlices%100==99 || numSlices>350` triggers it; at `≥maxSlices(2500)` it runs **synchronously inside Write** (fork `base.go:2177-2180`);
  - claim loop: `extentCompactLoop` (`pkg/fuse/extent_ops.go:610`, every 2s) calls `ExecuteCompact` directly, **bypassing `baseMeta.compacting`'s in-process dedup**, so it can race the same process's write/read triggers;
  - N mounts + the server fallback → N× the probability.

**How to verify**
- Single process: for an extent file with many small slices, trigger the claim loop and a large write at the same time, then look for `chunks/<newid>/...` in S3 with no matching `jfs_chunk_ref` row.
- Multiple mounts: have two mounts read the same file at the same time (≥5 slices triggers compaction) and assert the loser's blob key lands in `block_gc_tasks` (today it does not).
- Metrics: `vfs.Compact` Put count vs. new `jfs_chunk_ref` rows.

**Suggested fix**
- Preferred: **do not set `MaxDeletes = 0`** (or at least route `deleteSlice` through the `delete_slice` op) so upstream's loser cleanup works; note `delete_slice` → `jfsEnqueueSliceGCTx` (`jfs.go:1597`) → `block_gc_tasks` already exists.
- Alternative: explicitly call `delete_slice` in the failure branches of both `ExecuteCompact` and the fork's `doCompactChunk` (this needs a call site exposed on the transport).
- Also: make the claim loop participate in `m.compacting` dedup (or at least avoid compacting a chunk the write path is already compacting).

**Acceptance criteria**
- The blob uploaded by the CAS loser always appears in `block_gc_tasks` (or is deleted outright);
- After N rounds of concurrent compaction, no `chunks/*` object exists that is "referenced by no `jfs_chunk_ref` row and absent from the GC queue".

---

# P0-3 Server-side directory operations never touch the JuiceFS tree

**Symptom** [verified]
After a CLI/HTTP `fs rm -r <dir>` / `fs mv <dir> <dir2>`:

- Re-creating an empty directory with the same name and then **`rmdir` returns ENOTEMPTY** (the jfs side still holds an orphan child edge);
- Deleting an extent child of that directory from a new mount **silently leaks** the jfs node + S3 blocks (parent resolution lands on the new empty inode → jfs Unlink returns ENOENT, which is tolerated).

**Root cause**
1. The server-side directory implementations only touch the projection:
   - `RenameDir` (`pkg/datastore/store.go:600`): only `UPDATE file_nodes SET path/...`;
   - `DeleteDirRecursive` (`store.go:2032`) → `unlinkExtentTree` (`pkg/datastore/jfs.go:2033`): the query is `is_directory = 0 AND content_layout = 'extent'`, so it **only removes extent files**; the directory's own `jfs_node`/`jfs_edge` rows stay;
   - `DeleteEmptyDir` (`store.go:358`) and `DeleteNodesByPrefix` (`store.go:388`) likewise never touch jfs.
2. Repo-wide, `DELETE FROM jfs_edge` / `DELETE FROM jfs_node` appear only in `jfs.go`'s unlink / rmdir / rename, and are only reachable through JuiceFS meta RPCs.
3. Why `rmdir` hits ENOTEMPTY: `jfsRmdirTx` (`jfs.go:1239`) first runs `SELECT COUNT(*) FROM jfs_edge WHERE parent = ?` and returns ENOTEMPTY when >0 (`jfs.go:1252-1258`). The projection is empty, but jfs still holds the orphan `sub` edge.

**Reproduction**
- Already pinned down as case **G** in `e2e/extent-rename-mixed-dir.sh`:
  1. inside the mount create `cli-rm/a.db` (extent) and `cli-rm/sub/c.db`;
  2. unmount, run `drive9 fs rm -r :/cli-rm`;
  3. remount, `mkdir cli-rm` (hits EEXIST→Lookup and reuses the orphan inode) → `rmdir cli-rm` → **fails today (ENOTEMPTY)**.
- Case **F** covers the other half: after a CLI `fs mv` of a directory, the extent children are still readable/writable/deletable (projection side is fine), but the jfs directory name is stale → a new mount creates a second, empty dir inode for the same path.

**Suggested fix**
- Make server-side directory operations inline the jfs changes **in the same transaction** (this is the real fix; the FUSE two-step only ever covers FUSE clients):
  - `RenameDir`: use `file_nodes.extent_ino` (both source and target directory rows have it — see P1-1) to move one `jfs_edge`; children are keyed by parent inode, so no per-file work is needed;
  - `DeleteDirRecursive`: recursively delete the subtree's `jfs_edge`/`jfs_node` rows (and reuse P0-1's block GC);
  - `DeleteEmptyDir`: delete the directory's `jfs_edge` + `jfs_node`.
- Belt and braces: add an orphan-repair task (scan `jfs_edge` for edges whose parent no longer exists in `file_nodes` or whose path disagrees, plus `jfs_node` rows with nlink>0 and no reachable edge) that reclaims blocks through the P0-1 replacement.

**Acceptance criteria**
- `rmdir` in case G of `e2e/extent-rename-mixed-dir.sh` passes;
- After `fs rm -r`, `SELECT COUNT(*) FROM jfs_edge WHERE name = _binary'<dir>'` is 0;
- After `fs mv`, the jfs tree contains only the edge under the new name.

---

# P0-4 All tenants share one local JuiceFS cache directory: cross-tenant block collision

**Symptom** [verified]
When a mount passes `--cache-dir DIR` (the only way to turn on the chunk-layer write-back cache, see the note below), the local cache root is `<DIR>/jfs/drive9-extent/` — **identical for every tenant**. Because the cache key carries no filesystem identity and every tenant's slice ids start at 1, two tenants mounted on the same host with the same `--cache-dir` overwrite and serve each other's blocks:

- a **read** of tenant B's slice can be served from tenant A's cached file (same id/index/length) → B receives A's bytes;
- a **write** stages into the same `rawstaging/` path, so a leftover staged block can be uploaded as B's object (and vice versa);
- eviction (`cache.used` accounting) also evicts the other tenant's blocks.

This is a **data-correctness / isolation bug**, not a perf bug: the bytes are self-consistent (the cache file carries its own checksum), so nothing downstream detects the mixup.

**Root cause** (three independent facts, all required for the collision)
1. **The format UUID is a hardcoded constant**: `pkg/extent/runtime.go:148` sets `Format{UUID: "drive9-extent"}` for every tenant. The server stores that JSON per tenant (`jfsStoreFormatTx`, `pkg/datastore/jfs.go:830`), and `NewRuntime` re-runs `m.Init(format, …)` on every mount, so every tenant keeps the same UUID. `chunk.Config.SelfCheck` then appends the UUID to the cache dir (fork `pkg/chunk/cached_store.go:594-597`, `filepath.Join(ds[i], uuid)`), and `applyChunkCacheDir` only adds a `jfs/` component (`runtime.go:74`) — so the final path has no per-tenant component.
2. **The cache key has no fs/tenant component**: `rSlice.key` produces `chunks/<id/1e6>/<id/1e3>/<id>_<indx>_<blen>` (fork `pkg/chunk/cached_store.go:77-82`; `HashPrefix` is off), `getCacheKey` parses only id/index/size (`disk_cache.go:603-640`), and `cachePath`/`stagePath` join that onto `raw/` and `rawstaging/` (`disk_cache.go:728-734`). Nothing in the local path derives from the tenant prefix `t/<tenant>/` that the server adds to the S3 key.
3. **The slice id space restarts per tenant**: `jfsInitTx` seeds `nextChunk = 1` (`pkg/datastore/jfs.go:747`), the fork allocates in batches of `sliceIdBatch = 4 << 10` (`pkg/meta/base.go:51`) and `NewSlice` sets `freeSlices.next = v - sliceIdBatch` (`base.go:2131-2141`), so the **first slice id is 1 in every tenant** and the low ids are dense and identical across tenants. Two files of comparable shape therefore collide almost immediately.

**Scope / trigger**
- Only when `--cache-dir` is set: `pkg/fuse/extent_jfs.go:50` sets `Writeback: cacheDir != ""`, and `cmd/drive9/cli/mount.go:168` defaults `--cache-dir` to `""` (the `~/.cache/drive9` default is only used for drive9's own read cache / append-log journal, `pkg/fuse/mount.go:428`). Without `--cache-dir` the chunk store runs `CacheDir="memory"` and write-back is disabled, so there is no shared on-disk cache.
- The blackbox harness **always** passes `--cache-dir` (`blackbox/harness/target.py:566-569`), as do `e2e/fuse-crash-recovery-test.sh:128` and `e2e/fuse-write-perf-budget-test.sh:152`, so any two suites/tenants sharing one host cache root hit this.
- drive9's own read cache is unaffected: it is namespaced by `MountReadCacheHash(server, mountpoint, remoteRoot, credential)`.

**How to verify**
- Two tenants on one host, both mounted with the same `--cache-dir`:
  1. tenant A writes a 4 MiB-aligned file as its first extent file (slice id 1, block 0, len 4194304);
  2. `ls -l <cache-dir>/jfs/drive9-extent/raw/chunks/0/0/` shows `1_0_4194304` — then tenant B reads its own first file and must not see A's bytes;
  3. simplest static check: assert that the resolved cache root (`chunk.Config.CacheDir` after `SelfCheck`) differs between two tenant runtimes — today it does not.
- `pkg/extent/runtime_test.go:98` already pins the hardcoded `"drive9-extent"` UUID; extend it to assert a per-tenant/per-fs component.

**Suggested fix** (pick one, but keep the crash-replay property in mind)
- **Preferred: make the cache root per-filesystem.** Give each tenant a stable UUID (derive it from the tenant id / server prefix instead of the literal `"drive9-extent"`) so `SelfCheck` produces `<cache-dir>/jfs/<tenant-uuid>/`. `RuntimeConfig` (`pkg/extent/runtime.go:33`) needs a new field plumbed from `pkg/fuse/extent_jfs.go`; the server-side fallback runtime (`pkg/server/extent_meta.go:212`) can pass the same value or leave the cache off.
- **Alternative: namespace the cache dir before `SelfCheck`** — `applyChunkCacheDir` could append a per-tenant/mount component (e.g. `path.Join(cacheDir, "jfs", tenantKey)`), which is enough because `SelfCheck` only appends the UUID onto whatever directory it is given.
- Do **not** fix this by clearing the cache dir on exit: the whole point of `scanStaging` (`disk_cache.go:1016-1062`, `cached_store.go:1100-1116`) is that staged blocks survive a crash and are re-uploaded on remount of the same directory.

**Acceptance criteria**
- Two tenants mounted with the same `--cache-dir` on one host resolve to different cache roots, and tenant B reading its own file after tenant A wrote returns B's bytes (test at the `extent.NewRuntime` level plus one e2e case);
- The crash-recovery replay still works: a staged block left by a crashed mount is re-uploaded on remount of that tenant's cache dir;
- No regression to the existing "same tenant, two mount points, one cache dir" reuse.

---

# P1-1 Directory rename is two non-atomic steps; the directory extent_ino is not exposed

**Symptom** [verified]
A FUSE `mv <dir> <dir2>` is **two independent transactions**, and the second one's error is swallowed; any non-FUSE directory rename (CLI/HTTP) does not even perform the second step.

**Root cause**
- FUSE order: `renameRemoteWithTransientRetry` (projection subtree UPDATE, `pkg/fuse/dat9fs.go:10351`) → `extentRenameDirEdge` (`pkg/fuse/extent_ops.go:529`, called from `dat9fs.go:10421`); the latter does `_ = fs.extentRT.rt.Transport.Call(...)` (`extent_ops.go:550`) and **ignores the error**, and the directory branch does not pass flags (so `RENAME_EXCHANGE`, which `jfsRenameTx` rejects with ENOTSUP at `jfs.go:1282`, is silently dropped too).
- The directory → jfs inode mapping **only lives in client memory**: the DB does record it (`UPDATE file_nodes SET extent_ino` at `jfs.go:1026` and `jfs.go:1071`), but the API never returns it:
  - `server.go:3432` only emits `X-Dat9-Content-Layout` / `X-Dat9-Extent-Ino` when `proj.ContentLayout != ""`, and a directory row's `content_layout` is the default `''`;
  - `client.MkdirCtx` (`pkg/client/client.go:1401-1419`) does not send a layout header;
  - FUSE only adopts `stat.ExtentIno` when `stat.ContentLayout == extent` (`dat9fs.go:7231`).
  So after a remount the directory's jfs ino can only be guessed by "mirror it again by name" (`extentEnsureParent`, `extent_jfs.go:259`), and any name change forks the tree.

**How to verify**
- Case F (`e2e/extent-rename-mixed-dir.sh`) already covers "CLI directory rename + remount + read/write/delete"; turn on `EXTENT_E2E_SQL_ORPHAN_CHECK=1` to see whether an extra edge under the old name remains.

**Suggested fix**
- Short term: when `extentRenameDirEdge` fails, at least **log and return an error** (so the FUSE rename fails as a whole) instead of silently succeeding; or fold the directory's jfs rename into the server's `RenameDir` (same transaction — see P0-3) and delete the FUSE step entirely.
- Medium term: **expose the directory's `extent_ino` to clients via stat/lookup** (for example also set `content_layout='extent'` on directory rows, or add a dedicated header) so parent resolution no longer depends on rebuilding by name.

**Acceptance criteria**
- After a directory rename through any channel, deleting an extent child from a new mount leaves no orphan jfs node;
- A failed directory rename makes FUSE return an error instead of OK.

---

# P1-2 compact claim is an overwrite-style lease; the self-feeding scan is unindexed

**Symptom** [verified]
The same chunk gets merged by multiple executors; every mount full-scans `jfs_chunk` every 2 seconds.

**Root cause**
```go
// pkg/datastore/jfs.go:2122 jfsClaimCompactTx
SELECT task_id, extent_ino, chunk FROM slice_compact_tasks
 WHERE status='PENDING' OR (status='LEASED' AND lease_until < now) ... FOR UPDATE
-- when nothing is claimable, it self-feeds:
SELECT inode, indx FROM jfs_chunk
 WHERE LENGTH(slices) >= ? AND LENGTH(slices) <= ? LIMIT 1        -- 2145: no FOR UPDATE, no index
INSERT INTO slice_compact_tasks (...) VALUES (...)
 ON DUPLICATE KEY UPDATE status='LEASED', task_id=VALUES(task_id)  -- 2151: overwrites someone else's lease
```
- Two concurrent claims can select the same chunk; the later one overwrites `task_id` → both run → they collide in P0-2.
- The first executor's `RequeueCompact` fails silently because `WHERE task_id = ? AND status='LEASED'` (`jfs.go:2160-2163`) no longer matches; the task waits for the 5-minute lease to expire before it can be claimed again.
- `jfs_chunk` only has `UNIQUE(inode, indx)` (`pkg/tenant/schema/extent.go:56-62`) and no index on `LENGTH(slices)` → one full table scan per mount every 2s; N mounts means N× that.

**Suggested fix**
- Turn the claim into a real lease: `UPDATE ... SET status='LEASED', task_id=?, lease_until=? WHERE (extent_ino, chunk)=(?,?) AND (status='PENDING' OR lease_until < now)`, check `RowsAffected`, and only treat 1 as "claimed"; the self-feeding branch must also verify the `task_id` is still its own.
- Self-feeding scan: add a generated `slices_len` column + index on `jfs_chunk`, or have the Write path insert a row when it crosses the threshold (but avoid putting that INSERT inside the write transaction that holds `FOR UPDATE` — see the comment at `jfs.go:1570-1572`).
- Limit the number of concurrent compactors per tenant (mirroring upstream's `setIfSmall` pattern).

**Acceptance criteria**
- N concurrent claims produce exactly one LEASED task (a unit test with two goroutines calling `ClaimCompactTask` concurrently, asserting only one gets the `task_id`, is enough);
- The claim SQL uses an index (`EXPLAIN` shows no full scan of `jfs_chunk`).

---

# P1-3 block GC has no grace period

**Symptom** [verified]
A `block_gc_tasks` row is deletable the moment it is `PENDING`; the `available_at` column exists but is never used.

**Root cause**
```go
// pkg/datastore/jfs.go:2167 ListPendingBlockGC
SELECT block_key FROM block_gc_tasks WHERE status='PENDING' ORDER BY created_at LIMIT ?
```
The DDL has `available_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)` (`pkg/tenant/schema/extent.go:138`), and the inserts never write a future timestamp either. The consumer `runExtentBlockGC` (`pkg/server/extent_meta.go:165`) calls `DeleteObject` directly.

With multiple mounts / writeback caching, a reader may still be holding the pre-compaction slice list and read a block that was just deleted. The official JuiceFS docs warn about exactly this combination (compaction + write cache: the merged slice is committed to metadata before it is uploaded, so other clients block and retry). Our `applyChunkCacheDir` also enables writeback (`pkg/extent/runtime.go:75`).

**Suggested fix**
- On enqueue write `available_at = now + grace` (60s to start with; tune by chunk size / read amplification) and add `AND available_at <= CURRENT_TIMESTAMP(3)` to `ListPendingBlockGC`;
- While you are there, use `attempt_count`/`max_attempts` (inserts already set `max_attempts=8` but nothing increments it): log/alert when a row exceeds the limit instead of retrying forever.

**Acceptance criteria**
- Unit test: a freshly enqueued row is invisible; it becomes visible after `grace`;
- e2e: fill a chunk → trigger compaction → a concurrent reader of the old data gets no 404/EIO.

---

# P1-4 The path glob participates in Open/Lookup's by-name jfs probe

**Symptom** [verified, narrow trigger]
After a standard (single-layout) file is renamed onto a name that matches `[extent]`, FUSE's `Open`/`Lookup` probe jfs by `(parent, name)`; **if an orphan jfs edge happens to exist at that path** (orphan/fork), the single-layout file gets bound to that inode and subsequent reads/writes go through the extent data plane.

**Root cause**
```go
// pkg/fuse/dat9fs.go:11539 (Open)
if fs.extentVFS() != nil || fs.shouldUseExtentPath(p) { ... extentLookupChild ... }
// pkg/fuse/dat9fs.go:7234 (Lookup)
if !stat.IsDir && (stat.ContentLayout == ContentLayoutExtent || fs.shouldUseExtentPath(childP)) { fs.extentRefreshFromVFS(...) }
```
`shouldUseExtentPath` (`pkg/fuse/extent.go:51`) only looks at the basename glob, never at the layout.

**Suggested fix**
- Change both sites to require `stat.ContentLayout == extent` (or `extent_ino != 0`) before entering the extent data plane; keep `shouldUseExtentPath` only for "create" paths (create/symlink/mknod).
- Alternatively, verify that the jfs edge's inode agrees with the projection before binding by name.

**Acceptance criteria**
- Unit test: construct "projection says single + an orphan jfs edge with the same name" and assert `Open` takes the single path;
- Shipping it together with the P0-3 orphan cleanup is safer.

---

# P1-5 Writeback commit costs one extra meta RPC per kernel writeback batch

**Open cost** [verified]
`extentWrite` commits the slice before replying to a `FUSE_WRITE` carrying `WRITE_CACHE` (`pkg/fuse/extent_jfs.go`, `v.Flush(...)`), so the kernel cannot mark the page clean while the slice commit is still in flight. That forced `flush` + `WaitWrites` costs one meta RPC per kernel writeback batch: in the 2026-09-09 `community.*` run `fio` went 132 s → 227 s (≈ +32 % on the group total).

**Do not remove the flush without replacing it**
Slice commits are asynchronous (`QueueWriteParts`); without the flush a read landing between the writeback reply and the commit gets zeros, and the kernel caches those zeros (`community.fsx` `data miscompare`, reproduced 100 % with seed `1791112639`). Any optimization must keep "meta is current when the writeback reply lands" true.

**Options (preference order)**
1. **Fold the writeback commit into the write RPC**: have the `write`/`write_parts` op carry a "commit and reply" flag so the flush does not need a second round trip (the fork's `WriteParts` already batches; the extra cost is the forced `flush` + `WaitWrites`).
2. **Keep the async queue and gate the read path**: the read side would have to wait for in-flight writes of the inode, but that requires the `FUSE_WRITE` to *register* itself before replying — `dataWriter.Flush`/`WaitWrites` drains alone are not enough because a concurrent read can be processed before the write reaches the writer.
3. Anything that relies on the kernel not reclaiming clean pages is not available from userspace.

**Acceptance criteria**
- `community.fsx` and seed `1791112639` stay green in the full `community.*` group;
- `community.fio` back toward ~132 s;
- no extra meta RPC per writeback batch (count `/v1/extent/meta` ops during an fsx run).

---

# P1-7 Open-handle data is invisible to remote readers until close

**Symptom** [verified]
With `FUSE_PROFILE=extent` and JuiceFS writeback on, an app writes through an open handle (no `fsync`) and `drive9 mount drain` reports `ok: true` with idle queues, yet a remote reader (`drive9 fs cat`, which goes server → S3) returns **an empty body with rc=0**. The local read of the same file returns the correct bytes. It stayed empty after a second drain and after a 10 s wait; it became visible once the writer closed the fd. `e2e/fuse-smoke-test.sh [9.1]` fails on exactly this assertion. (The supervision `[8]` probe has a *different* root cause — see P1-8.)

**What is already ruled out** [verified]
- **Not the upload leg**: a `cachedStore.Flush(ctx)` upload barrier (fork commit `50521e05`) was added and wired into the `flush_extent` drain phase; the gate still failed, so "the block was never PUT to S3" is not the whole story. The barrier was **reverted** again (fork HEAD is back to `a53df2a9`) because it did not fix the assertion.
- **Not a plain drain bug**: a fresh focused repro (mount `--profile extent`, open-handle write, `drive9 mount drain`) **passes**. The failure only appears on a mount that has already been through the smoke test's earlier sections, so it is state-dependent.
- **Not P2-1 alone**: if only the projection were stale, the server-side `fs cat` would still apply `overlayExtentStat` and return the jfs length; the empty body means `jfs_node.length` was 0 at that moment, i.e. the write had not reached the daemon at all.

**Evidence pointing at meta/projection rather than the data** [verified]
- The failing mount's JuiceFS cache holds the block for the open handle (`.../jfs/drive9-extent/raw/chunks/0/0/6_0_24`, 28 B = 24 B payload + footer), i.e. the bytes reached the data plane.
- The remote read returns **rc=0 with an empty body** — not zeros, not an error — so the reader sees a file of length 0, not a hole.
- The extent write path does not keep `file_nodes`/`inodes.size_bytes` in sync (see P2-1), and the projection is the only thing a server-side reader can use.

**Next diagnostics (ranked)**
1. Snapshot the DB **at the failure point** for the open-handle file and for a closed file in the same run: `file_nodes(path, size_bytes, extent_ino)`, `jfs_node(inode, length)`, `jfs_chunk(inode, indx, slices)`. This separates "length never committed" from "path → inode mismatch".
2. Log the Write dispatch in `pkg/fuse/dat9fs.go` (`source=start|missing-handle|extent-vfs-fh|extent-orphan|extent-orphan-discard`) for the failing file — `extent-orphan-discard` drops data silently.
3. Check whether the drain CLI's `syncfs(2)` really flushed the page (it ignores ENOENT by design, `cmd/drive9/cli/mount_drain.go:86`) and compare against a manual `sync -f` in the same run.

**Acceptance criteria**
- `e2e/fuse-smoke-test.sh` 103/103 with `FUSE_PROFILE=extent` and writeback on;
- `e2e/fuse-supervision-test.sh` 51/51 (its `[8]` probe);
- no write may be discarded silently: every `extent-orphan-discard` must log.

---

# P1-8 After a remount an extent file reads back as size 0 (mount-side, extent-only)

**Symptom** [verified]
Write a file through an extent mount, `umount`, mount the same remote again: the file **exists but is 0 bytes** on the new mount (`stat` 0, `cat` empty, even after `readdir`). The same file is intact everywhere else: server-side `drive9 fs cat` returns the full content, `drive9 fs stat` reports the right size, the HEAD response carries the right `Content-Length`, and the object is in S3. This is the `e2e/fuse-supervision-test.sh [8]` failure (`fresh mount reads seeded cache probe bytes`, pre-existing: it also failed on 2026-09-08, `~/logs/extent-e2e-20260908T150928Z/fuse-supervision.log:88`).

**Minimal repro** [verified]
On the instance (`~/repro/repro-remount.sh`, driven by `PROFILE`, `MODE`):
1. `drive9 mount --mode=fuse --profile extent :/remount-<ts> $MP`
2. `printf "fresh-cache-boundary-<ts>\n" > $MP/fresh.txt` → local read 38 B, `fs cat` 38 B, `fs stat` size 38
3. `drive9 umount $MP` → mount a fresh mount (same path **and** a brand-new mount point both reproduce)
4. `stat -c %s $MP/fresh.txt` → **0**; `cat` → empty; `readdir` also shows size 0

With `PROFILE=none` the identical script passes (38 B before and after remount), so this **is extent-specific** — the first hypothesis ("supervision has nothing to do with extent") is wrong.

**Evidence** [verified]
- DB for the file: `file_nodes.content_layout='extent'`, `extent_ino` set, `contents.storage_type='extent'`, `inodes.size_bytes=0`, `jfs_node.length=38`, one `jfs_chunk` slice covering 0..38; the S3 object (`chunks/0/0/<id>_0_38`) exists.
- Server HEAD (`curl -I /v1/fs/<path>`) returns `Content-Length: 38` and `X-Dat9-Content-Layout: extent`, so `overlayExtentStat` works on the server.
- Kernel view: file `dev/ino` is identical before and after the remount (e.g. `55 2`), only the size changes 38 → 0.
- Daemon instrumentation (`DRIVE9_EXTENT_DEBUG`, temporary): on the fresh mount `extentOpen` is called with `openLen=38` (the jfs attr is correct), but **no FUSE `LOOKUP` is logged for the path** and `extentRead` is never called — the kernel's `i_size` is 0, so `cat` never issues a READ. In one run the single `extentRefresh` (jlen=38) appears *after* the opens.
- `pending/` and `shadow/` cache dirs of the mount are empty, so the persisted write-back index is not (on disk) the source of the stale 0.

**Open question / next steps (ranked)**
1. The kernel serves the attr without a LOOKUP on the remounted path: confirm whether `drive9 umount` really drops the FUSE superblock before the next `drive9 mount` (mountinfo still listed the mount right after `umount` in the repro) and whether the kernel reuses the previous dentry/inode (same `dev`/`ino`).
2. Add logging to the early-return branches of `Dat9FS.Lookup` (`specialNodeEntry`, `lookupLayerNamespaceEntry`, `localOverlayForPath`, `pendingIndex`/`writeBack`, `openHandleEntry`) — the current instrumentation only logs after the dir-cache check, so an early hit is invisible.
3. Verify with `strace -e trace=statx,openat` whether the second mount issues a FUSE LOOKUP at all, and whether `drive9 mount` reuses a live FUSE connection instead of creating a new one.
4. If the stale attr comes from the projection, the extent size must be applied on **every** attr-producing path (Lookup/GetAttr/readdir), not only in `extentRefreshFromVFS`; `TestJfsWriteDoesNotDualWriteInodeSize` (`pkg/datastore/jfs_data_test.go:346`) shows updating `inodes.size_bytes` on the write hot path is deliberately avoided, so the fix belongs on the read/attr side.

**Acceptance criteria**
- `PROFILE=extent` repro reads the 38 bytes after umount + remount (both same path and fresh mount point);
- `e2e/fuse-supervision-test.sh` 51/51;
- no extent attr path may answer from the projection when `jfs_node.length` is authoritative.

---

# P2-1 Projection size is not kept in sync

**Symptom** [verified]
`ls -l` / `fs stat` shows the stale size from the projection when the jfs overlay misses.

**Root cause**
`jfsWritePartsTx` only updates `jfs_node.length` (`jfs.go:1491-1494`); it does not update `file_nodes` / `inodes.size_bytes`, although by contract the projection's size/mtime should be updated in the same Write transaction. Today `overlayExtentStat` (`jfs.go:101-122`) patches `fs stat`, and `extentOverlayDirEntries` (`pkg/fuse/extent_jfs.go:920`) only overrides size when the jfs by-name lookup hits.

**Suggested fix**
- In `jfsWritePartsTx` / `jfsTruncateTx`, also run `UPDATE inodes SET size_bytes/mtime WHERE inode_id IN (SELECT inode_id FROM file_nodes WHERE extent_ino = ?)` (`jfsTruncateTx` already does this at `jfs.go:1533-1534`; Write is missing the other half).
- Remember Write is a hot path: do not add an extra SELECT — reuse the attr you already return.

**Acceptance criteria**
- After writing an extent file, `drive9 fs stat` reports the correct size without mounting; `ls -l` and the in-mount `stat` agree.

---

# P2-2 STS grants that can be narrowed

Current flow (`pkg/server/extent_meta.go:61-120`, `pkg/s3client/aws.go:264-304`):
`POST /v1/data-credential` (scoped tokens rejected) → `AWSS3Client.CanMintSTS()` → `sts.AssumeRole(RoleArn=DRIVE9_S3_ROLE_ARN, RoleSessionName="drive9-extent-<tenant>", DurationSeconds=3600, Policy=<inline session policy>)`, prefix = `serverPrefix + "t/" + tenantID + "/"`.

| Point | Today | Suggestion |
|---|---|---|
| Granularity | tenant level (= one JuiceFS volume); a fork is a new tenant → new prefix | Finer scoping needs a new prefix layout; at minimum document it |
| Static MinIO branch | Hands out the **server's global static keys**; the prefix is only a client-side convention, IAM does not enforce it (`aws.go:246-262`) | Document it, or require `DRIVE9_S3_ROLE_ARN` |
| file mock branch | Hands out a local directory path; no isolation | Test-only; document it |
| session policy | Grants `s3:DeleteObject` (the client does not need it — GC runs server-side) and is missing `AbortMultipartUpload`/`ListBucketMultipartUploads`/`ListMultipartUploadParts` | Drop DeleteObject; add the multipart actions (today JuiceFS `Put` is a single `PutObject`, `pkg/object/s3.go:155`, so it is not hit yet — but enabling multipart would return AccessDenied) |
| TTL / refresh | 1h hardcoded (`extent_meta.go:106`), 2min refresh window hardcoded (`pkg/extent/storage.go:135`), no jitter | Add jitter; many mounts of one tenant all AssumeRole in the same window |
| Call frequency | Once per process + once per ≤1h; every CLI `fs cat` is an AssumeRole (`pkg/client/extent_io.go:13-36`) | Consider an in-process cache / shared session |
| `force_path_style` | Present in the response but unused by `openInner` (`storage.go:85-94`); JuiceFS defaults to path style (only `JFS_S3_VHOST_STYLE=1` switches it) | Either wire it up or delete the field |
| Refresh failure | `refreshingStore.innerStore` silently keeps the old inner store (`storage.go:138-141`) | Log / emit a metric so a 403 is not misattributed to a data-plane bug |

---

# P2-3 Resource cost of the server-side fallback compactor

**Symptom** [verified]
- `runExtentCompactFallback` (`pkg/server/extent_meta.go:200-219`) calls `extent.NewRuntime(...)` **for every task**, and `NewRuntime` runs `m.NewSession(true)` (`pkg/extent/runtime.go:163`) which is never closed → `jfs_session2` churn, reclaimed only by the session sweep.
- It uses `extent.WrapS3(s3, "t/"+tenantID+"/")` (`extent_meta.go:208`) with the server's own credentials; `s3Wrap.Put` does `io.ReadAll` into memory and then `PutObject` (`pkg/extent/s3wrap.go:66-72`) → merging a 64MiB chunk holds a full copy in memory.
- Invocation timing: only in `drainFileGC`'s "file_gc queue is empty" branch (`pkg/server/tenant_worker.go:550-552`), and `drainFileGC` itself is only called when the kick mask contains `WorkFileGC` (`tenant_worker.go:499-503`). When file_gc is busy, the three extent jobs starve.

**Suggested fix**
- Give the three extent jobs (block GC / session sweep / compact fallback) their own schedule or their own kick instead of piggybacking on the empty file_gc branch;
- Have the fallback compactor reuse/cache the Runtime (or at least close the session); make `s3Wrap.Put` streaming (`PutObject` needs a length, so Head first or chunk it).

---

# Q-1 Questions to confirm first

1. **Is `TestExtentUnlinkEnqueuesBlockGC` (`pkg/datastore/jfs_data_test.go:719`) red right now?**
   It asserts that after unlink `block_gc_tasks` contains a `99_` key, but `jfsEnqueueFileGCTx` never writes that table (P0-1). `newTestStore` calls `testtidb.ResetDB` (`pkg/datastore/store_test.go:20-29`), so in theory no leftover row can make it pass by accident.
   Run: `DRIVE9_TEST_TIDB_DSN=... make test TEST_RUN=TestExtentUnlinkEnqueuesBlockGC TEST_PKGS=./pkg/datastore/...`
   (`ResetDB` wipes the test database, so do not run this concurrently with other test runs that share the same DSN.)
2. **Are there other sources of the P0-3 orphan edges?** `DeleteNodesByPrefix`, layer discard, and the fork cleanup paths all delete projection rows; use `grep -rn "DELETE FROM file_nodes"` to check each one for the jfs cleanup it may need.
3. **Semantics of a directory's extent_ino**: `jfsMknodTx`/`jfsEnsureParentsTx` already write `extent_ino` on directory rows, but no `content_layout`. Before exposing it to clients, confirm every read path (stat/list/lookup) can carry it safely.

---

# 3. Test and verification tools

| Tool | Purpose |
|---|---|
| `e2e/extent-rename-mixed-dir.sh` | Added with this list. Mixed `[extent]` profile (`*.db/*.db-wal/*.db-journal/*.db-shm`) covering: layout stickiness (single→pattern stays single, extent→non-pattern stays extent with the same ino), mixed-directory ls/rename/rm -r, unlink+create yielding a new ino, CLI directory rename / `rm -r` followed by read/write/delete from a fresh mount, and the **orphan dir edge regression (case G)**. `EXTENT_E2E_SQL_ORPHAN_CHECK=1` also queries `jfs_edge`. Manual-only; needs a live extent data plane. |
| `e2e/fuse-sqlite-commit-sequence.sh` | The one FUSE suite that has an extent-profile track, but it only has one flat directory plus one `seq.db` and **does not cover directory-level operations**. |
| `pkg/datastore/jfs_test.go` / `jfs_data_test.go` | meta-layer unit tests: rename/unlink/compact/GC transaction semantics. |
| `pkg/extent/compact_test.go`, `lockmem_test.go` | data-plane / lock unit tests. |
| `POST /v1/sql` | Fastest way to inspect `jfs_edge`/`jfs_node`/`jfs_chunk`/`block_gc_tasks`/`jfs_delfile`/`slice_compact_tasks` when verifying GC. |

Full extent sweep (extent profile, every optional workload on):

```bash
FUSE_PROFILE=coding-agent-extent RUN_FUSE_ALL_WORKLOADS=1 \
  RUN_FUSE_SQLITE_COMMIT_SEQUENCE=1 RUN_POSIX_SMOKE=1 RUN_EXTENT_E2E=1 \
  make e2e-local
```

---

# 4. Do not "fix" these

1. **Same-transaction dual write**: extent Create/Unlink/Rename must stay "one RPC, one TiDB transaction" that writes both the jfs tables and the `file_nodes` projection. Do not turn the FUSE side into two HTTP calls.
2. **Layout stickiness**: rename not converting `content_layout` is **intentional** (layout is an inode property — see §1 invariant 2). Do not "helpfully" re-evaluate layout from the new name.
3. **CAS prefix comparison**: `jfsCompactTx`'s `cur[:len(origin)]` prefix comparison matches upstream `doCompactChunk` (fork `sql.go:3864-3874`) and is deliberate (it allows concurrent appends). Do not change it to a full comparison.
4. **Blocks never go through drive9-server**: the extent data plane talks to S3 directly (`pkg/extent/storage.go:95-96` explicitly rejects `scheme=drive9`). Do not proxy blocks back through the server for convenience.
5. **No extra queries on the hot path**: the comment in `jfsWritePartsTx` (`jfs.go:1439-1442`) explains why there is no extra SELECT / compact INSERT — sqlite `--wait all` is 10s.

---

# 5. Appendix: key code map

| Concern | Location |
|---|---|
| meta RPC entry point / transactions | `pkg/datastore/jfs.go:148 RunExtentMetaOp`, `dispatchExtentOp` (same file, `jfs.go:274`) |
| Namespace | `jfsMknodTx` 945, `jfsUnlinkTx` 1153, `jfsRmdirTx` 1239, `jfsRenameTx` 1277, `jfsReaddirTx` 1381 |
| Data | `jfsReadTx` 1410, `jfsWritePartsTx` 1438, `jfsTruncateTx` 1505, `jfsCompactTx` 1544 |
| GC | `jfsEnqueueSliceGCTx` 1597, `jfsEnqueueDeadSliceGCTx` 1613, `jfsEnqueueFileGCTx` 1698, `ListPendingBlockGC` 2167, `MarkBlockGCDone` 2185 |
| Compact queue | `jfsClaimCompactTx` 2122, `jfsRequeueCompactTx` 2160, `runCompactOp` 218 |
| Server worker | `pkg/server/tenant_worker.go:523 drainFileGC`, `pkg/server/extent_meta.go:165/193/200` |
| FUSE dispatch | `dat9fs.go` Unlink 9296, Rmdir 9780, Rename 10262/10421 |
| FUSE extent implementation | `pkg/fuse/extent_jfs.go` (runtime/ensureParent/rmdir/unlink/create), `pkg/fuse/extent_ops.go` (rename/truncate/locks/claim loop) |
| Data-plane runtime | `pkg/extent/runtime.go`, `storage.go`, `compact.go`, `s3wrap.go` |
| Local block cache | `pkg/extent/runtime.go:70 applyChunkCacheDir` (UUID at 148), `pkg/fuse/extent_jfs.go:50`, fork `pkg/chunk/disk_cache.go:603-640` (cache key), `:728-734` (cache/staging paths), `:1016-1062` (`scanStaging`) |
| Credentials | `pkg/server/extent_meta.go:61/91`, `pkg/s3client/aws.go:231-304`, `pkg/client/extent_meta.go:52` |
| JuiceFS fork | the `github.com/juicedata/juicefs` module replaced in `go.mod` (fork `mornyx/juicefs`), `pkg/meta/{base.go,drive9.go,drive9_engine.go,sql.go}` |
