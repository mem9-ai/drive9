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
| P1-1 | High | FUSE directory rename is two non-atomic steps and the second step's error is swallowed; the directory `extent_ino` is not exposed to clients | jfs directory tree diverges; behavior degrades across mount/remount |
| P1-2 | High | compact claim is an **overwrite-style lease**, and the self-feeding scan is **unindexed** | Duplicate merges, duplicate uploads, full table scans |
| P1-3 | High | block GC has **no grace period** (ignores `available_at`) | Readers on another mount can hit a block that was just deleted |
| P1-4 | Medium | The path glob participates in `Open`/`Lookup`'s by-name jfs probe | With an orphan edge present, a single-layout file is mis-bound to the extent data plane |
| P1-5 | Medium (perf, open) | Every kernel writeback batch costs one extra meta commit RPC (the fsx fix); `community.fio` 132 s → 227 s | ≈ +30 % on the write-heavy suites until the commit is folded into the write RPC |
| P1-7 | High (open) | A file written through an open handle is **invisible to remote readers until close**: `mount drain` reports idle queues and the remote read returns an empty body | `e2e/fuse-smoke-test.sh [9.1]` FAIL; the upload barrier alone does not fix it |
| P1-8 | High (open) | After `umount` + remount an extent file reads back as **size 0** on the new mount while the server/S3 have the full content | `e2e/fuse-supervision-test.sh [8]` FAIL; extent-only (profile `none` passes) |
| P1-9 | High (open) | With the kernel writeback cache **off**, concurrent small writes stall the extent write path: `community.sqlite` mptest/threadtest3 fail with `database is locked`, slices sit `freezed:true done:true committed:false` until `flush timeout after waited 5m0s` | Reached by `--durability write-sync` (cache off by design), macFUSE/older kernels that ignore the cap, and any workload whose writes stay small |
| P1-10 | High (open) | With the kernel writeback cache **on**, the classic-path SQLite correctness suite fails at the mounted `PRAGMA integrity_check` after the rollback workload with `sqlite3.OperationalError: disk I/O error` | `e2e/fuse-sqlite-correctness.sh [6]` FAIL; exposed (not caused) by the classic durability fixes, which let the suite run this far for the first time; passes 20/20 on macFUSE (no real writeback cache), so it is cache-dependent and needs a real-kernel `--debug` diagnosis |
| P2-1 | Medium | Projection size is not kept in sync (Write only updates `jfs_node.length`) | `ls -l` shows a stale size when the jfs overlay misses |
| P2-2 | Medium | STS grants that can be narrowed (DeleteObject / multipart / static branch has no IAM / TTL has no jitter / per-process mint) | Over-broad permissions + STS noise |
| P2-3 | Low | Server-side fallback compactor builds a new Runtime per task (new session never closed) + `WrapS3` reads the whole blob into memory | Session churn, memory spikes |

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
- Short term: when `extentRenameDirEdge` fails, at least **log and return an error** (so the FUSE rename fails as a whole) instead of silently succeeding; or fold the directory's jfs rename into the server's `RenameDir` (same transaction) and delete the FUSE step entirely. **Done for the server side**: `RenameDir` now moves the directory's `jfs_edge` in its own transaction (see the P0-3 fix), so a CLI/HTTP rename no longer forks the tree and the FUSE second step is redundant (it currently returns ENOENT, which the caller ignores). Still open here: the swallowed error, `RENAME_EXCHANGE`, and exposing the directory `extent_ino`.
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

**Update (2026-09-11, writeback-zombie work)**: the missing-handle Write
branch no longer discards on `extentEnabled()` alone. The discard is gated on
`InodeToPath.WasEverExtent(nodeId)` (the inode provably belonged to an extent
file) and always logs; a released **classic** handle is kept as a zombie until
FORGET so its late kernel writebacks land through the normal Write path
(`pkg/fuse/writeback_zombie.go`,
`docs/design/kernel-writeback-cache-classic-durability.md`). The classic-file
silent-loss part of this issue is closed by that change; the open-handle
visibility symptom above (state-dependent `9.1` failure) remains open.

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

# P1-9 Concurrent small writes stall the extent write path when the kernel writeback cache is off

**Symptom** [verified, 2026-09-09]
With `FUSE_WRITEBACK_CACHE` disabled every `write(2)` becomes its own FUSE `WRITE`, and a workload that writes many small blocks from several processes (SQLite WAL) stalls: `community.sqlite` `mptest crash01` fails with `database is locked` (SQLite's 10 s busy timeout) and `threadtest3 walthread5` never finishes. The mount log shows `flush <n> timeout after waited 5m0s` and pending slices stuck at `freezed:true done:true committed:false`.

**Evidence** [verified]
A/B on the same host, same build, extent profile, two runs each:

| kernel writeback cache | `mptest crash01 --journalmode wal` | `--journalmode delete` | `threadtest3 walthread5` |
|---|---|---|---|
| off | FAIL rc=1, 14 / 17 `database is locked` (28.6 s / 28.0 s) | FAIL rc=1, 7 / 8 errors (39.7 s / 52.9 s) | hang, rc=124 at 120 s (both runs) |
| on | PASS rc=0, 0 errors (14.4 s / 12.3 s) | PASS rc=0, 0 errors (57.9 s / 62.5 s) | PASS rc=0 (21.2 s / 26.9 s) |

A goroutine dump taken while the mount was stalled showed no pending FUSE handler (only idle commit-queue workers), so the stall was not captured at the right instant yet.

**Reachability**
`--durability write-sync` keeps the cache off by design, and macFUSE/older kernels ignore the cap, so both still reach this path. It is not the default anymore: `--durability auto|interactive|fsync|close-sync` enables the cache (design doc §4.2).

**How to reproduce**
`~/repro/ab-sqlite.sh` on the EC2 test host (starts the local stack, mounts with `--profile extent`, runs both configurations), or the blackbox module with a cache-off binary: `python3 blackbox/run.py --module community.sqlite --server-mode local`.

**Suggested next step**
Capture `/debug/pprof/goroutine?debug=2` from the mount every 2 s while `mptest` retries, then check whether the slice commit is stuck in the JuiceFS writer, in the forced `flush`+`WaitWrites` barrier of `extentWrite`, or in the server transaction. Fix the starvation rather than relying on the kernel cache.

**Status update (2026-09-10): the stall is the read path, and there is no lock leak** [verified]
Reproduced with the cap really off (the harness knob had been a no-op — `--writeback-cache on` on the command line is what negotiates it; verify with the INIT reply, not with the harness): `mptest crash01` fails with `database is locked` while `multiwrite01` passes 0 errors/81, and an *isolated single-client* spill task (`PRAGMA cache_size=10`, 5 × 20 KB blobs, then 5 NULL updates) takes **81 s on the extent mount vs 0.195 s on ext4**. During it the holder is alive (instrumented `SETLK` refusals name it; its `FLUSH` carries the identical `lock_owner` and `RELEASE` empties the table), so the 6252 `W@120` EAGAINs are peers whose 10 s busy timeout expires, not a leaked lock.

Split of the 81 s (instrumented fork): **41.9 s in the per-read `vfs.VFS.Read` writer drain** (`pkg/vfs/vfs.go:790` flushes the inode's writer before *every* read) + 26.9 s in the store read; the daemon wrote 39 423 times at 82 µs but read 9 509 times at 7.4 ms (234 MB read for 64 MB written). With the kernel writeback cache negotiated the same task takes 16 s and the suite passes, because the kernel serves the read-backs and merges the writes.

A bounded read-your-writes window in `Dat9FS` (serve a read from bytes this daemon just wrote) was implemented and measured: it did **not** help (spill 132 s vs 81 s), because SQLite's read-back only partly overlaps the written ranges, so most reads still fall through to the store and pay the barrier. The remaining fix has to be in the read path itself: drop or narrow the unconditional `writer.Flush` in the fork's `VFS.Read` (it is only needed for read-your-writes on *other* clients) and/or let the daemon answer a clean read from the git-style local staging area without a store round trip. Until then the cap-off extent path cannot meet SQLite's 10 s busy timeout for this workload.

**The remaining fix is a fork change, and it is now specified** [verified]
The three delays behind the 41.9 s read barrier are hard-coded in the JuiceFS fork, not exposed through `vfs.Config`, so drive9 cannot tune them:

| Where | Constant | Effect per read-after-write |
|---|---|---|
| `pkg/vfs/vfs.go:790` (`VFS.Read`) | `v.writer.Flush(ctx, ino)` unconditional | every read drains the inode's writer, even when the read range has nothing staged |
| `pkg/meta/drive9_engine.go:407,447` (`inodeWriteWorker`) | `8 * time.Millisecond` batch window | a staged slice waits up to 8 ms before its meta write is sent |
| `pkg/vfs/writer.go:386` (`pickCommitCount`) | `5 ms` deadline before committing leading done slices | each read-back waits for that window; `writePartsBatch = 64` (`writer.go:326`) is the ceiling |

For the isolated spill that is 4.4 ms per read on average (p50 0.02 ms, 3997 of 9483 reads > 1 ms), i.e. almost exactly the two windows, repeated 9.5 k times.

Proposed fork change, in order of value:
1. Serve a read whose range is fully covered by the writer's **staged** slices from the local staging area (`chunkWriter`/`sliceWriter` hold the bytes) instead of draining: this removes both the barrier and the store round trip for read-your-writes, and unlike a daemon-side cache it knows the staged ranges exactly (a daemon-side window was tried and failed precisely because SQLite's read-back only partly overlaps what was written).
2. When a drain *is* required (read outside the staged ranges, Fsync, Flush, Release), flush the pending meta batch immediately instead of waiting out the 8 ms window; `WaitWrites` already queues behind the parts, so this only removes timer latency.
3. Make the two windows configurable (`vfs.Config`) so a mount can trade meta batching for read-after-write latency without a fork release.

Local-replace mechanics (verified the hard way): a fork working copy still declares `module github.com/juicedata/juicefs`, so a scratch tree must point the *original* path at it — `go mod edit -replace github.com/juicedata/juicefs=/path/to/forkcopy` — not at `github.com/mornyx/juicefs`, which fails the module-path check.

Fork working copy: `~/work/juicefs` on the dev machine is a clone of `github.com/mornyx/juicefs` with push access (ADMIN) whose `a53df2a9` is the commit the current `replace` pseudo-version pins. Work there, push a branch, then `go mod edit -replace github.com/hanwen/go-fuse/v2=...`-style bump: `go get github.com/mornyx/juicefs@<sha>` for the meta package replace, rebuild, and verify with the spill repro plus `crash01` before landing.

Acceptance for this item: with `--writeback-cache off`, the isolated spill task returns to single-digit seconds (ext4: 0.2 s; cap-on: 16 s; cap-off today: 81 s) and `mptester crash01.test --journalmode wal` reports `0 errors out of 94 tests`, then the whole `community.sqlite` module passes on the extent profile.

Why no tuning shortcut exists: a synchronous commit per write (what `WRITE_CACHE` writes do under the cap) removes the pending-slice part of the barrier but leaves the store read itself — 9 509 reads at 2.8 ms is already 27 s for this 64 MB spill, so the read-backs must be served locally (kernel page cache, or a staged-range read overlay in the fork). A daemon-side cache cannot substitute, because it cannot know which ranges the writer has staged.

**A/B harness (EC2 host, reusable)** [verified]
Everything lives in `/home/ec2-user/night` on the ap-southeast-1 test host (`ssh -i ~/.ssh/drive9_extent_e2e ec2-user@13.214.129.143`); the tree it builds is `/home/ec2-user/drive9-src` (rsync a worktree over `pkg/fuse/` plus `docs/`; use `rsync --delete` or a stale file breaks the build).

* `env.sh` — PATH (prepends `/home/ec2-user/night/bin`), private `HOME`, `DRIVE9_*`, `provision()`.
* `bin/drive9` — PATH shim that injects `--writeback-cache "$FUSE_WB_CAP"` into `drive9 mount <opts> ...`. It only covers mounts launched as bare `drive9` (git-ops, git-workspace-*, supervision, sqlite-correctness and `mount.sh` all do); a script that mounts through its own `$CLI_BIN` bypasses it, so **always confirm the negotiation from the mount's INIT reply**, never from the harness env.
* `mount.sh <profile> <on|off> <tag> [extra args]`, `umount.sh <tag>` — mount/unmount with the cap forced.
* `sqlite-probe.sh <mount> [cases]` — mptest wal/delete (crash01), threadtest3 walthread2/5, kvtest.
* `e2e.sh <profile> <cap> <script> [tag]`, `single.sh` — run one `e2e/*.sh` gate with the profile and cap set; `blackbox.sh <profile> <cap> <tag>` runs `community.sqlite` via `blackbox/run.py --server-mode local`.
* **Fork experiment (2026-09-11): what cap-on really buys, and where a daemon-side cache breaks** [verified, unlanded]
With the cap off, `write()` is FUSE direct IO, so every read-back misses the kernel page cache and reaches the daemon; with the cap on the kernel page cache serves them (a whole spill run issued **2** FUSE reads). Measured per read on the mount: **1.54 ms** in the meta slice lookup (`drive9Meta.Read`, whose cached mapping is invalidated by `commitWriteParts`, `pkg/meta/drive9_engine.go:483`) plus **1.39 ms** in the chunk read (`sliceReader.run` → `dataReader.Read`) — 22.5 k reads, 34.7 s + 31.2 s of a 71 s spill.

A daemon-side **read-your-writes window** in the fork (`fileWriter`: 4 KiB pages, per-inode budget, filled on write and on store read, served when a read is fully covered, marked committed on flush) fixed the speed: hit rate **98.5 %**, meta calls 22 500 → 500, chunk reads 22 488 → 499, and `mptester crash01.test` reached **`Summary: 0 errors out of 94 tests`** with the cap off. It is **not landable as written**: SQLite then reported `database disk image is malformed` in the isolated spill (the suite still passed, so the corruption is pattern-dependent). A variant that also skipped the writer drain when a read did not touch the window was tried first and reverted (it can serve a reader's stale chunk mapping). The unlanded patch lives only in the scratch clone `~/work/juicefs` (branch `drive9-staged-read-window`) — no fork commit was pushed and drive9's `go.mod` is untouched.

**Why JuiceFS/Cloud pass SQLite with a remote meta, and what our graft is missing** [analysis + measurements]
JuiceFS Cloud's metadata is remote too, so "meta is local" is not the explanation. Three mechanisms account for it, and our graft satisfies none of them:

| JuiceFS mechanism | Where | Our graft |
|---|---|---|
| Cheap per-op meta RPC (pooled Redis/SQL/TiKV or a managed service, sub-ms) | `baseMeta.Read`/`Write` | one HTTP request **plus a server-side TiDB transaction** per op: measured **1.54 ms** per read (`drive9Meta.Read` → `doRead`), 22 500 calls = 34.7 s of a 71 s spill |
| Data reads served from the client's local cache dir (`--cache-dir`, write-through) | `dataReader`/`cacheStore` | measured **1.39 ms** per read (22 488 calls = 31.2 s): not a local-cache hit profile |
| Adaptive readahead amortizes lookups over growing windows | `fileReader.checkReadahead` (`reader.go:423-432`), `MaxReadAhead` | exists, but SQLite's page access is not sequential so the window never grows; measured ~10 KiB covered per meta+chunk pair |

Note upstream invalidates the chunk mapping on every write exactly like we do (`baseMeta.Write` → `m.of.InvalidateChunk(inode, indx)`, `pkg/meta/base.go:2168`), so the invalidation is not the defect — the *price of the refetch* is.

Two fork variants were implemented and both are **unlanded because they corrupt data** (SQLite `database disk image is malformed` in the isolated spill, while `crash01` still reported 0/94 in the first one, i.e. pattern-dependent):
1. daemon-side read/write cache in `fileWriter` + `VFS.Read` (98.5 % hit rate, meta calls 500, chunk reads 499, spill 71 s → 21 s) — corrupted with the meta path untouched, root cause not found;
2. keep the chunk mapping across our own commit by appending `parts[i].Slice` to `m.of`'s cached list instead of `InvalidateChunk` (`drive9_engine.go:483`) — corrupted immediately (spill fails in 2 s); blind append almost certainly duplicates slices already present in the cached list, so a reconciliation by slice id (or plain invalidate) is required.

Plan, in leverage order, none of which needs a new cache semantics:
1. Make a read hit the client's local chunk cache (write-through retention of the chunk just written, sane cache quota/eviction); target ≈0.1 ms per read (data leg 31 s → ~2 s).
2. Batch meta lookups (one HTTP op for several chunks, or prefetch the next K chunks' mappings on a miss); target meta leg 34.7 s → 2-4 s.
3. Make the server-side read op cheap: a chunk lookup should not need the write-path transaction wrapper (`RunExtentMetaOp`) and the HTTP client should reuse connections; target sub-ms.
4. Over-fetch one `blockSize` (64 KiB) per block miss and let `sliceReader` serve the following small reads (the mechanism is already there; only the request size is small).

Reference experiment to set the budget before optimising further: mount **stock JuiceFS with a remote meta** on the same host and run the same `mptester crash01` workload with the same instrumentation, to obtain its per-op meta and data-read latencies and readahead hit rate. Our numbers (1.54 ms + 1.39 ms per ~10 KiB read) should be compared against that baseline rather than against zero.

Next attempt should prefer the **read-side cost** over caching, which has no staleness surface: keep the chunk mapping valid across our own commits (append the committed slices to `m.of` instead of `InvalidateChunk` at `drive9_engine.go:483`), and fetch bigger blocks per read so one meta + one chunk read covers more than the measured ~10 KiB. Only then revisit a cache, with the correctness question above answered.

Status of the delegated attempt (2026-09-10): a background implementation session was given this spec (fork clone at `~/work/juicefs`, base `a53df2a9`, push access, the four acceptance cases) and ran for many hours without producing a branch, commit or measurement; it was stopped. The next attempt should be made by a session with a full context budget, following the same spec and finishing with `accept-fork-patch.sh`.

One-shot acceptance for a candidate fork patch: `bash /home/ec2-user/night/accept-fork-patch.sh [tree]` — prints the effective `replace`, builds, then reports the isolated spill seconds, `crash01`'s `Summary: N errors out of M tests`, and the `RESULT:` line of the extent cap-off, extent cap-on and classic cap-off `sqlite-correctness` gates (targets: spill < 10 s, 0/94, 20/20 each).
* Isolated spill repro (the fastest signal, ~2 min): `/tmp/bigspill2.test` with `$SQLITE_TOOLS/mptester probe.db --sync --journalmode wal --timeout 30000 /tmp/bigspill2.test` inside the mount; SQLite tools are in `/home/ec2-user/bb-work/cache/tools/sqlite/master`.

**Landed fix: read from the write buffer instead of draining the inode** [verified, fork `e7a7fe2a`]
The fork's `VFS.Read` flushed the inode before every read. The landed change answers a read from whatever is *already committed* plus whatever the writer still holds in memory, and keeps the flush only for ranges the buffer has already handed to the object store without their commit having landed:

* `chunk.BufferedWriter` (`wSlice.BufferedStart`/`ReadBuffered`) exposes the bytes a block writer has not uploaded yet — the same page layout `WriteAt` fills, so it never invents bytes.
* `vfs.dataWriter.ReadBuffered` reports the buffered ranges covering a read plus whether a byte is uploaded-but-uncommitted (only a flush can answer those).
* `drive9Meta.WriteState` returns the inode's pending-commit count and a commit sequence number. A read takes the fast path only when **no commit is in flight** (a slice leaves the write buffer the moment its commit is queued, while metadata still holds the previous mapping) and **no commit was enqueued while the read ran** (otherwise the reader window cached from the pre-commit mapping is invalidated); either condition failing falls back to the original flush.

Measured on the isolated cap-off spill (64 MB, same host, same harness; counters were temporary instrumentation, removed before landing):

| | before | after |
|---|---|---|
| per-read flush (`vfs_read_flush`) | 9 479 calls / 43.2 s | 891 calls / 15.7 s |
| meta slice query over HTTP | 4 064 calls / 14.4 s | 1 821 calls / 5.9 s |
| chunk reads (reader windows) | 8 500 | 2 982 |
| object writes | 202 MB | 132 MB |
| meta commits | 3 546 | 992 |
| **spill** | **83–87 s** | **37 s** |
| `crash01` | rc=1, `database is locked` | `Summary: 0 errors out of 94 tests` (25–49 s) |

Acceptance on the landed commit (`accept-fork-patch.sh`, tree pinned to `e7a7fe2a`, loaded host): spill **40 s** (was 81 s, target < 10 s), `crash01` rc=1 at 67 s, and **20/20 on all three `sqlite-correctness` gates** (extent cap-off, extent cap-on, classic cap-off).

Correctness evidence: a verify build recomputed the flush-then-read answer for every fast-path read on a 3-round crash-shaped workload — **30 612 reads compared, 0 byte mismatches** — and a 6-round single-client spill/integrity loop is clean. Two bugs were found and fixed on the way and are worth remembering: serving a *partial* range needs the reader's answer overlaid (not replaced), and the fast path must require `pending == 0` up front; without that precondition a slice whose commit is already queued is neither in the buffer nor in metadata, and reads of its range silently return the previous contents (that one produced `database disk image is malformed` in about one round out of three).

**Still open on this item**: `crash01` is now corrupt-free but not yet reliably `rc=0` on a loaded host. Its remaining failure is `mptest`'s `--wait all` timeout in `crash02.subtest` line 53, because the client that crashes (`--exit 1`) holds the SQLite write lock while its process exit waits for the daemon to drain that transaction; on the currently loaded EC2 host a 200 MB transaction takes ~35 s while the peers' busy timeout is 10 s. The same test passes on an idle host (0/94, 25–33 s) and its corruption is gone. Two independent facts from this session bound the problem: native JuiceFS 1.4.1 against the same local TiDB + MinIO **also fails** `crash01` with the cap off (300 s timeout, meta ops averaging 5.7 ms, `setlk` in the meta path), and the instance's disk filling to 100 % wedged TiDB and produced spurious corruption — check `df -h /` before trusting any cap-off failure on this host.

The remaining lever is the exit-time drain itself (freeze/commit cost of a large pending buffer), not the read path: meta commits are 992 × 10.4 ms and object PUTs 3 943 × 10 ms of a 37 s spill.

---

# P1-10 Classic-path SQLite mounted integrity_check fails with disk I/O error under the kernel writeback cache

**Symptom** [verified]
With the kernel writeback cache **on** (the default auto policy), `e2e/fuse-sqlite-correctness.sh` fails at section `[6]`: the rollback-journal workload completes, then the mounted `PRAGMA integrity_check` raises `sqlite3.OperationalError: disk I/O error`. On a separate EC2 run the same build instead failed one check later — the remounted tree was missing `sqlite/rollback/workload.db` while the remote snapshot held a valid copy — so the failure surface is a read-after-write visibility problem on the SQLite file under the real kernel writeback cache, not a deterministic data-corruption signature.

**Attribution** [verified]
The same suite passes **20/20 on macFUSE** (which ignores the writeback-cache cap, so there is no real kernel writeback cache), so the failure is writeback-cache-dependent. It is **not a regression from the classic durability fixes** (`4ce69d3a`, `8ccf7a0a`, `2c4ec400`, `bc31ad7a`, `981041d9`): before those fixes the suite never reached section `[6]` on CI — earlier commits failed in the release gate (git clone EIO, hardlink, truncate/unlink) and aborted before the SQLite suite ran. The durability fixes are what let the suite run this far for the first time.

**Next step**
Reproduce on a real Linux kernel with `FUSE_SQLITE_MOUNT_DEBUG=1` and inspect the mount log for the read that returns EIO — check whether the integrity_check's read-back of `workload.db` (or the replayed `-journal`) hits a missing-handle/orphan writeback, a stale kernel page invalidation, or a torn committed write. The reliable repro is the GitHub runner; EC2+SSM tunnels proved too flaky (mount-readiness races, tunnel drops) to iterate there.

**Status update (classic-path fallout, fixed separately)** [verified]
Chasing the `local-e2e` failures this entry predicted found two concrete classic-path rules the kernel writeback cache imposes, both now implemented:

- A partial-page write is merged by reading the page back through the caller's *own* handle, so a `READ` arrives on an `O_WRONLY` handle. Local overlay handles answered it with `EBADF` because their backing fd was write-only; the kernel then reported the caller's write as `unable to append to '.git/logs/HEAD': Bad file descriptor` (`git commit` after a remount). Fixed by `openLocalBackingFile` (read-write backing fd, requested mode as fallback).
- A blobless git workspace serves its clean tree as empty placeholders whose blobs arrive on read. With the cap on, a file that was never written locally keeps size 0 in the kernel — `fuse_change_attributes()` overwrites the size in every attr reply with the inode's cached `i_size` (`fuse_get_cache_mask()` returns `STATX_MTIME|STATX_CTIME|STATX_SIZE` for any regular file) and a `GETATTR`/`INVAL_INODE` pair left it at 0 — so no `READ` reaches the daemon, the read-through hydration never runs and `git status` reports every path as modified. Fixed by `maybeCorrectKernelCleanNodeSize`: when the daemon learns a blobless clean node's size it pushes the last page's real bytes with `FUSE_NOTIFY_STORE`, the only notification that still calls `fuse_write_update_attr()` and grows `i_size`. Verified: the Git operations gate passes all 74 checks per profile with `--writeback-cache on`.

**Default (2026-09-10, multi-writer decision)** [verified]
Because drive9 is used with several writers on one tree (multiple mounts, the CLI, server-side tasks) and extreme cases must keep multi-writer semantics, the cap is now **off by default for every durability policy**; `--writeback-cache on` is the explicit "this mount is the only writer of these files" opt-in. See `kernelWritebackCacheEnabled` in `pkg/fuse/mount.go` and design doc §4.2 for the kernel-side reasons (server size/mtime/ctime are discarded for cached regular files, dirty pages reach the daemon at unpredictable times).

That makes the cap-off extent write path the default path, so the remaining work is to remove the extent data plane's performance dependence on the cap (P1-9): with the cap off, `community.sqlite`'s `mptest crash01` still fails with `database is locked` while `multiwrite01` passes 0/81, the WAL write lock (offset 120) is granted 94 times and refused 6252 times in one run, and a minimal POSIX-lock `kill -9` test through the same mount does not leak — so the stall is a lock-lifecycle problem specific to a crashing client, not raw write throughput.

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

# Q-1 Remaining questions

1. **Are there other sources of P0-3 orphan edges?** The server-side directory paths are fixed (`DeleteEmptyDir`, `DeleteDirRecursive`, `RenameDir` all touch the jfs tree now). `DeleteNodesByPrefix` has no callers today; if a new path deletes `file_nodes` rows directly, check whether it needs the same jfs cleanup (`grep -rn "DELETE FROM file_nodes"`).
2. **Semantics of a directory's extent_ino**: `jfsMknodTx`/`jfsEnsureParentsTx` already write `extent_ino` on directory rows, but no `content_layout`. Before exposing it to clients (P1-1), confirm every read path (stat/list/lookup) can carry it safely.

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
