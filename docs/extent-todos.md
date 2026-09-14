---
title: Extent known issues and invariants
status: open
date: 2026-09-13
scope: content_layout=extent data plane (JuiceFS grafted as the extent storage/meta engine)
audience: engineer or agent picking up the remaining work
---

# 0. How to read this document

- What is left here is what is still **open**. Fixed issues are deleted, not archived: `git log -- docs/extent-todos.md` has the history.
- Every `file:line` is a **hint, not a contract**: locate by function name / constant name / SQL fragment first.
- Read **§1 Architecture** and **§3 Do not "fix" these** first, so you do not turn intentional design into a bug.
- **Scope**: the extent implementation in this repo (`pkg/datastore/jfs*.go`, `pkg/server/extent_meta.go`, `pkg/fuse/extent*.go`, `pkg/extent/*`, `pkg/s3client/aws.go`). The `[append-log]` server half lives in `tidbcloud/fs`; `[append-log]` being inert under a local `provider=local` deployment is **not** a defect.

---

# 1. Architecture in 30 seconds

```
kernel FUSE ──> Dat9FS (drive9's FUSE frontend)
                  ├─ non-extent files → existing drive9 paths (single / append_log)
                  └─ extent files (basename matches a profile [extent] glob)
                        ├─ Create/Unlink/Lookup/Rename/Locks → JuiceFS meta driver (one RPC)
                        └─ Read/Write/Flush/Fsync/Truncate → vfs.DataReader/DataWriter
                                                      │
                                        ┌─────────────┴───────────────┐
                                  meta.Meta HTTP client        chunk.ChunkStore
                                  (mornyx/juicefs fork)              │
                                        │                      object.ObjectStorage
                                        ▼                      "drive9s3" (STS/static/file mock)
                              drive9-server /v1/extent/meta           │
                              one TiDB transaction:                   ▼
                                jfs_node/jfs_edge/jfs_chunk    S3 [serverPrefix]t/<tenant>/chunks/...
                                + file_nodes projection
```

Three invariants (**no fix may break these**):

1. **JuiceFS meta is the source of truth for an extent file's name/inode/length/locks; `file_nodes` is a projection written in the same transaction.** Create/Unlink/Rename must stay "one RPC, one transaction"; the FUSE side must never dual-write.
2. **`content_layout` is an inode property, not a path property**: no UPDATE of the `content_layout` column exists anywhere in the repo (only INSERTs, plus UPDATEs of path/extent_ino/inode_id), and rename never converts layout. This is intentional; see §3.
3. **The projection decides which data plane a file uses; the profile glob only decides where new files are created.** Open/Lookup must not bind an existing file to a JuiceFS inode by name.
---

# 2. Open issues

## P1-9 The extent profile is slower than the classic path on small-write workloads

**Status: open, deliberately on hold** [verified 2026-09-12]

Every 4 KiB `write(2)` is its own FUSE `WRITE` and its own meta RPC. `community.sqlite`'s `mptest crash01` (a ~20 s multi-client transaction against a 10 s sync window) therefore fails on the extent profile while the classic path passes:

| carrier (same `mptester`, same `mptest/crash01.test`, `--journalmode wal --timeout 30000`) | rc | time | result |
|---|---|---|---|
| ext4 (no FUSE) | 0 | 2 s | `Summary: 0 errors out of 94 tests` |
| classic path (FUSE) | 0 | 86 s | `Summary: 0 errors out of 92 tests` |
| extent | 1 | 24–119 s | `timeout waiting for all clients` + peer `database is locked` |

Native JuiceFS 1.4.1 against the same local TiDB + MinIO fails the same test with a 300 s timeout, so this is a **topology** gap (remote HTTP meta + object store), not an extent logic bug: the remaining cost is the client-side data leg and the ~10 ms TiDB transaction behind every `extent_write`.

**Rejected optimizations (all measured; do not repeat without new evidence)**: late upload 58 s; `WaitWrites` instead of flushing 52 s; wider commit batching (64→256, deadline 5→20 ms) 40 s; serving fully covered ranges from the buffer before the reader 38 s; a bounded in-daemon RAM block cache (84 s → 42 s, but it needs three invalidation points plus a memory bound and still does not make `crash01` green).

**Acceptance criteria if it is picked up**: `blackbox/run.py --module community.sqlite` green on the extent profile, with the `--wait all` window met by real work reduction (not by a bigger busy timeout).

## Q-1 Remaining questions

1. **Should a directory expose its `extent_ino`?** `jfsMknodTx`/`jfsEnsureParentsTx` already write `extent_ino` on directory rows, but no `content_layout`. Nothing needs it today (`RenameDir` moves the edge, and `jfsEnsureParentsTx` mirrors parents on demand), but a client that had to resolve a directory's jfs inode without a name would need a stat/header field plus a FUSE adoption rule.

---

# 3. Do not "fix" these

1. **Same-transaction dual write**: extent Create/Unlink/Rename must stay "one RPC, one TiDB transaction" that writes both the jfs tables and the `file_nodes` projection. Do not turn the FUSE side into two HTTP calls, and do not add a second FUSE-side jfs mutation after a server-side rename.
2. **Layout stickiness**: rename not converting `content_layout` is intentional (invariant 2). Do not re-evaluate layout from the new name.
3. **CAS prefix comparison**: `jfsCompactTx`'s `cur[:len(origin)]` prefix comparison matches upstream `doCompactChunk` (fork `sql.go:3864-3874`) and is deliberate (it allows concurrent appends). Do not change it to a full comparison.
4. **Blocks never go through drive9-server**: the extent data plane talks to S3 directly (`pkg/extent/storage.go` rejects `scheme=drive9`). Do not proxy blocks through the server.
5. **No extra queries on the write transaction**: the comment in `jfsWritePartsTx` explains why the projection size is not maintained there (measured +40–50 % on `extent_write`; sqlite's `--wait all` budget is 10 s). Readers take the length from `jfs_node` (attr overlays for stat/readdirplus, `extentSizeSelect`/`extentSizeJoin` for search, `ExtentUnreportedUsageBytes` for quota), and the quota total must stay a count of distinct extent *files* (see §5's quota row).
6. **Do not bind by name**: Open/Lookup must decide the data plane from `content_layout`/`extent_ino`. The profile glob is only for create paths.
7. **A new path that deletes `file_nodes` rows directly must clean the jfs side too.** The live sites do: `DeleteEmptyDir` and `DeleteDirRecursive` drop the directory's subtree, unlink and rename go through the meta engine, and the orphan-projection cleanups release the entity and hand the inode to the drain. `DeleteNodesByPrefix` and `Store.DeleteNode` have **no callers** and do no jfs cleanup at all, so they are the ones to watch when something starts using them (or when a new `DELETE FROM file_nodes` appears): audit with `grep -rn "DELETE FROM file_nodes"` and check that each reachable site owns its extent cleanup.
8. **Do not delete a block before its grace period**: `available_at` exists because a reader on another mount may still hold the pre-compaction slice list.
9. **Do not re-enable JuiceFS chunk-store writeback** (`chunk.Config.Writeback`): with it, `Finish` stages the block locally and the object PUT happens afterwards, so Flush/Fsync would return before the data is remote-durable. The cache dir is still the local read cache.
10. **Locks belong to the metadata service**: `setlk`/`getlk`/`flock` run in one TiDB transaction against `jfs_plock`/`jfs_flock` (node row `FOR UPDATE`), like JuiceFS's own SQL engine. Do not reintroduce a process-local lock table — two mounts of one filesystem must exclude each other.

---

# 4. Verification tools

| Tool | Purpose |
|---|---|
| `e2e/extent-rename-mixed-dir.sh` | Mixed `[extent]` profile covering layout stickiness, mixed-directory `ls`/rename/`rm -r`, unlink+create yielding a new ino, CLI directory rename / `rm -r` from a fresh mount, orphan dir edges. `EXTENT_E2E_SQL_ORPHAN_CHECK=1` also queries `jfs_edge`. |
| `e2e/fuse-sqlite-commit-sequence.sh` | FUSE suite with an extent-profile track (one flat directory plus `seq.db`). |
| `pkg/datastore/jfs_test.go`, `jfs_data_test.go` | meta-layer unit tests: rename/unlink/compact/GC transaction semantics, compact enqueue/lease, block GC grace, quota admission, cross-runtime locks, usage accounting. |
| `pkg/extent/compact_test.go`, `s3wrap_test.go`, `storage_test.go` | data-plane, upload and credential tests. |
| `pkg/server/extent_runtime_pool_test.go`, `extent_meta_test.go` | server-side extent jobs, credential minting/gating, runtime reuse. |
| `POST /v1/sql` | Fastest way to inspect `jfs_edge`/`jfs_node`/`jfs_chunk`/`jfs_plock`/`jfs_flock`/`block_gc_tasks`/`jfs_delfile`/`slice_compact_tasks`. |

Full extent sweep (extent profile, every optional workload on):

```bash
FUSE_PROFILE=coding-agent-extent RUN_FUSE_ALL_WORKLOADS=1 \
  RUN_FUSE_SQLITE_COMMIT_SEQUENCE=1 RUN_POSIX_SMOKE=1 RUN_EXTENT_E2E=1 \
  make e2e-local
```

Local unit tests need TiDB (`DRIVE9_TEST_TIDB_DSN`). Running several packages in parallel against **one** database produces `Error 8028 information schema is changed ... DDL ran in parallel`; use `-p 1` and a fresh database per run.

Deployment notes: STS deployments can run extent anywhere; a static-key MinIO deployment additionally needs `DRIVE9_EXTENT_ALLOW_STATIC_CREDENTIALS=1`; a deployment whose resolved object-encryption policy needs a per-object SSE header cannot run the extent data plane at all (JuiceFS's S3 backend cannot set those headers — use bucket-default encryption).

---

# 5. Appendix: key code map

| Concern | Location |
|---|---|
| meta RPC entry point / transactions | `pkg/datastore/jfs.go` `RunExtentMetaOp`, `dispatchExtentOp`, `dispatchExtentRead` |
| Read-only ops outside the write txn | `pkg/datastore/jfs.go` `extentReadOnlyOp`, `dispatchExtentRead` |
| Namespace | `jfsMknodTx`, `jfsUnlinkTx`, `jfsRmdirTx`, `jfsRenameTx`, `jfsReaddirTx`, `jfsRenameDirEdgeTx`, `jfsEnsureParentsTx` (also used by `Store.RenameDir`) |
| Data | `jfsReadTx`, `jfsWritePartsTx` (quota-admitted, `FOR UPDATE` length), `jfsTruncateTx`, `jfsCompactTx` |
| Locks | `jfsFlockTx`, `jfsGetlkTx`, `jfsSetlkTx` (`jfsLockInodeTx`, `jfsPLock` records) over `jfs_flock`/`jfs_plock` |
| Quota | `ExtentQuotaLimit` (`admit`), `Store.ExtentUnreportedUsageBytes` (2 s cache of total − reported), `Store.PeekExtentUsageDelta` / `Store.CommitExtentUsageDelta` (the `extentQuotaReportedBytes` marker, a compare-and-set), `Store.ClaimExtentUsageReport` / `Store.ReleaseExtentUsageReport` (the cross-pod report lease), `pkg/backend.ReportExtentUsageDelta` + the `extent_usage` mutation, `pkg/backend.PendingCentralStorageDelta`, `pkg/server.extentQuotaLimit`. The total is `SUM(jfs_node.length)` over **distinct** `file_nodes.extent_ino` with `jfs_node.type = file`, so a hardlink alias and a mirrored directory are each charged once (or not at all) — do not go back to a per-row sum |
| Replaced-target reclaim | `Store.reclaimReplacedTargetTx` (classic rename and `jfsMoveProjectionTx`) |
| Compact queue | `enqueueCompactAfterWrite` (post-commit), `jfsClaimCompactTx` (lease), `jfsRequeueCompactTx`, `runCompactOp` |
| Block GC | `jfsEnqueueSliceGCTx`, `jfsEnqueueDeadSliceGCTx` (dead `jfs_chunk_ref` rows are deleted once their GC tasks are durable), `ListPendingBlockGC` (grace filter), `MarkBlockGCDone`, `RequeueBlockGC` |
| Extent constants | `pkg/datastore/layout.go` (`extentBlockGCGraceSQL`, `extentCompactSlices`, `ExtentChunkSize`, `ExtentBlockSize`) |
| Attr overlays | `pkg/datastore/jfs.go` `overlayExtentStat`, `overlayExtentStatDir`; layout/`extent_ino` ride on `StatLite`'s row |
| File size for search | `pkg/datastore/search.go` `extentSizeSelect` / `extentSizeJoin` |
| Search/stat readers | `pkg/server/server.go` `handleStat` (layout from the stat row) |
| Server worker | `pkg/server/tenant_worker.go` `drainExtentMaintenance`, `reportExtentQuotaUsage`; `pkg/server/safety_net_scan.go` (periodic `WorkExtent` kick via `Store.HasPendingExtentWork`); `pkg/server/extent_meta.go` `runExtentFileGC`/`runExtentBlockGC`/`runExtentSessionSweep`/`runExtentCompactFallback`; `pkg/server/extent_runtime_pool.go` |
| Credentials | `pkg/server/extent_meta.go` `mintDataCredential`, `staticDataCredentialsAllowed`, `requiresObjectSSE`; `pkg/s3client/aws.go` `tenantPrefixSessionPolicy`; `pkg/extent/storage.go` `refreshingStore`, `endpointWithBucket` |
| FUSE dispatch | `pkg/fuse/dat9fs.go` `Rename` (flags), `Lookup`, `Open`; `pkg/fuse/extent_ops.go` `extentOpenIno`, `extentStatIno`, `extentCompactLoop` (joined by `FlushAll`), `resolveJuiceIno`, `extentGetLk`/`extentSetLk` |
| FUSE extent implementation | `pkg/fuse/extent_jfs.go` (runtime/ensureParent/rmdir/unlink/create/write/fsync) |
| Data-plane runtime | `pkg/extent/runtime.go`, `storage.go`, `compact.go`, `s3wrap.go`, `read.go` (context-aware reads) |
| JuiceFS fork | `github.com/juicedata/juicefs` replaced in `go.mod` (fork `mornyx/juicefs`), `pkg/meta/{drive9.go,drive9_engine.go}`, `pkg/vfs/{vfs.go,writer.go}` |
