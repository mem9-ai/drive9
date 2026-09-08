---
title: Drive9 FUSE Multi-Connection SQLite WAL Design
status: draft
date: 2026-09-08
---

## 1. Objective and scope baseline

Support same-mount multi-connection SQLite WAL workloads on Drive9 FUSE with
`durability=fsync` and append-log enabled: one connection writing, one running
`PRAGMA wal_checkpoint(TRUNCATE)`, and concurrent readers on the same mount.
SQLite serializes mutators via POSIX locks, so "multi connection" here means
turn-taking mutations across handles, not byte-level concurrent writers.

This design is the implementation umbrella for #907. It extends the accepted
append-log spec `docs/design/s3-express-append-log-fuse.md`, whose "Out of
scope" item 5 and section 3.2 mark multi-writer coordination out of scope. This
design narrows that line to "same-mount multi-connection" and keeps multi-mount
/ multi-process concurrent writers out of scope.

In scope:

1. W1: cross-handle append-log generation rebind (fixes B / EEXIST).
2. W2: checkpoint read reuse (fixes A / serial remote reads).
3. W3: growth-aware PATCH for `main.db` (fixes C / 2 GiB growth).
4. W4: deadline / memory coherence (optional hardening).
5. W5: cross-handle read consistency audit.
6. W6: verification (unit + three-connection e2e + server contract test).

Out of scope:

1. Multi-mount / multi-process concurrent writers (server-side distributed fencing).
2. Generic POSIX byte-interleaved concurrent writes; conditional-write CAS fail-closed semantics remain.
3. A byte-granular server range-write primitive; PATCH stays part-granular.
4. Automatic append-log discovery (existing spec backlog).

Estimates are production net LoC; tests are excluded:

| Workstream | Estimate |
| --- | --- |
| W1 | 60-120 (includes the reset-gate relaxation) |
| W2 | 150-300 |
| W3 | 80-180 (includes the fsync reroute) |
| W4 | 20-50 |
| W5 | 100-400 (audit-driven) |
| W6 | not counted |

The design adds no persistent schema, no new lock, no background worker, and no
second commit state machine. All coordination reuses the same-path remote
commit lock, per-handle mutex, and revision/generation fences from the accepted
spec.

## 2. Background: three failure modes in one matrix (#902)

The 4 MiB WAL / TRUNCATE checkpoint matrix at 1 MiB, 2 MiB, and 2 GiB `main.db`
exposes three independent failures:

1. A: a fresh checkpoint fd does ~400 serial 4 KiB remote WAL reads; a 12 ms ext4 checkpoint takes ~38 s.
2. B: after the checkpoint truncates the WAL and publishes a new revision, the pre-opened writer's new 32-byte header fsync returns EEXIST -> `SQLITE_IOERR_FSYNC`.
3. C: growing a 2 GiB `main.db` by ~1 MiB disables PATCH, hydrates all 257 old parts, builds full-size in-memory copies, hits an already-expired 30 s fsync context, and returns `SQLITE_IOERR_FSYNC`.

## 3. Decision: growth goes through PATCH

The server patch plan already supports growing an S3-backed `single` file:
`appendDirtyPartNumbers` (pkg/backend/patch.go:125) computes the dirty tail
parts beyond the original object, and the plan presigns a `read_url` only for
parts inside the original object (pkg/backend/patch.go:44,
pkg/backend/patch.go:290). The blockers are client-side: two `size <= OrigSize`
gates. W3 is therefore client-only; no server feature work is required for
growth.

Append-log remains the transport for the WAL itself (frequent small tail
appends with constant latency). PATCH rebuilds the object and copies all
untouched parts per commit, which would reintroduce the #875 upload
amplification. PATCH applies only to `main.db` growth.

Under writeback policy a strict fsync does not reach the write-sync or flush
PATCH path: it goes through `WriteBackUploader.UploadSyncWithRevision`
(pkg/fuse/writeback_uploader.go:486) ->
`uploadBufferedRemoteFileWithRevision` (pkg/fuse/remote_upload.go:20) ->
`WriteStreamConditional`, which is always a full-file multipart upload with no
PATCH. W3 therefore also reroutes the strict-fsync growth case through the
patch-capable flush path; this remains client-only and the writeback uploader
itself is unchanged for background commits.

## 4. Workstreams

### 4.1 W1: rebind append-log generation on sibling zero-truncate commit

Current behavior:

1. `SetAttr` for `ftruncate(wal, 0)` publishes a pending truncate event to sibling handles via `publishSQLiteZeroTruncate` (pkg/fuse/dat9fs.go:2266).
2. An idle sibling consumes it in `applySQLiteZeroTruncateLocked` (pkg/fuse/dat9fs.go:2282); `appendLogRecordTruncate` (pkg/fuse/append_log.go:84) captures `rewriteBaseRevision = BaseRev`, `rewriteBaseSize = OrigSize`, and sets `sqliteWALTruncated = true`, `appendSafe = false`.
3. The truncate commit goes through the append-log full-rewrite route on the checkpointer's own handle (`sqliteWALTruncated` gates reset, `appendSafe == false` gates tail) and, on success, refreshes siblings via `refreshCommittedRevisionForOpenHandlesWithSize(..., 0)` (pkg/fuse/append_log.go:967). That path adopts `BaseRev`/`OrigSize` and rebuilds the clean WriteBuffer via `rebindCleanWriteBufferToRemoteLocked` (pkg/fuse/dat9fs.go:2748), but it does not touch `fh.appendLog` at all: `rewriteBaseRevision`/`rewriteBaseSize`, `sqliteWALTruncated`, and `appendSafe` all keep their pre-truncate values.
4. The writer's 32-byte header fsync is gated away from generation reset (`sqliteWALTruncated`, pkg/fuse/append_log.go:697) and tail append (`appendSafe == false`), so it enters `tryAppendLogFullRewriteLocked` (pkg/fuse/append_log.go:835). `appendLogCommittedBaseline` (pkg/fuse/append_log.go:157) still returns the old revision, so the stat/CAS fails with EEXIST.

```text
writer A (open, clean, committed header H0)
checkpointer B: ftruncate(wal, 0) -> fsync commits rev R+1, size 0
  publishSQLiteZeroTruncate(A)
    -> A: hasRewriteBase=true, sqliteWALTruncated=true, appendSafe=false
  refreshCommittedRevisionForOpenHandlesWithSize(A, R+1, 0)
    -> A.BaseRev=R+1, WriteBuffer rebuilt; fh.appendLog untouched
A: Write(0, H1[0:32]) -> fsync
  sqliteWALTruncated -> skip generation reset; appendSafe=false -> skip tail append
  tryAppendLogFullRewriteLocked: baseline=(oldRev, oldSize) -> stat/CAS EEXIST
```

Change:

1. In `refreshCommittedRevisionForOpenHandlesWithSize` (pkg/fuse/dat9fs.go:2547), rebind the append-log baseline for any clean sibling that carries the zero-truncate marker, with no further size condition: `if fh.appendLog.sqliteWALTruncated { fh.appendLogAdoptCommittedBaseline(revision, committedSize) }` (pkg/fuse/append_log.go:212). The marker is a one-shot latch — set by `applySQLiteZeroTruncateLocked`, cleared by the adopt — so it fires exactly once per truncate and covers siblings that skip the 0-byte commit and instead adopt a follow-on commit such as the writer's `(R+2, 32)`. The adopt is idempotent and runs only for handles that pass `handleCanAdoptCommittedRevisionLocked`, so over-application is harmless. Place the rebind before the shadow-removed early-continue (`clearRemovedCommittedShadowLocked`, pkg/fuse/dat9fs.go:2562) or add it to that branch, so a shadow-spilled sibling is not skipped. The helper clears `hasRewriteBase`/`rewriteBase*`, clears `sqliteWALTruncated`, restores `appendSafe`, and rebinds the observed layout to `(revision, committedSize)`. It deliberately preserves `sqliteWALCommittedHeader` (the committed header `H0`), which the reset route needs for the salt comparison; nothing reads a new header from the committed view, which is empty after the truncate.
2. Relax the generation-reset eligibility so the reset route actually fires. `tryAppendLogGenerationResetLocked` currently refuses `expectedSize < sqliteWALHeaderSize` (pkg/fuse/append_log.go:706), but the rebased baseline is `(R+1, 0)`. Admit a 0-byte baseline only for a confirmed SQLite WAL: change the guard to `expectedRevision <= 0 || (expectedSize != 0 && expectedSize < sqliteWALHeaderSize)`. The existing upstream fences remain the correctness gates: `IsNew`, `appendLogConfirmSQLiteWALLocked`, the `saltsDiffer` check, and the layout/revision verification.
3. The reset route is chosen over tail append because it rotates the shadow at the generation boundary (`rotateAppendLogGenerationShadowLocked`, pkg/fuse/append_log.go:642), which W2 depends on for checkpoint reads. Server-side this is safe: `RewriteAppendLog` keeps `append_log` layout for a 0-byte rewrite (tidbcloud/fs pkg/backend/append_log.go:295), so the reset's revision CAS against `(R+1, 0)` succeeds. If `saltsDiffer(H0, H1)` is false, the reset returns NotApplicable and the header write falls through to tail append or the layout-aware full rewrite against `(R+1, 0)`; both publish a correct 32-byte object, and neither rotates the shadow in that case.

Keep the existing rule that a dirty sibling never silently adopts a newer base:
`handleCanAdoptCommittedRevisionLocked` (pkg/fuse/dat9fs.go:2579) already
refuses `DirtySeq != 0` / `HasDirtyParts`, and the rebind branch above runs only
inside that adopt branch.

Failure windows for the new transition:

1. Sibling adopts `(R+1, 0)` and the process dies before rebind: no persistent state exists; a later open lazily re-proves the header.
2. Checkpointer commits the truncate and exits before the writer's header write: adoption happens on the commit refresh regardless of checkpointer liveness.
3. Writer writes `H1` but dies before the reset PUT: the dirty buffer is lost on unmount, the remote WAL stays 0 bytes at `R+1`, and SQLite re-writes the header on the next write; no acknowledged transaction is lost, and CAS fail-closed semantics are preserved.
4. Reset PUT succeeds but shadow rotation fails: finalization must retain the committed 32-byte generation and degrade reads, per the accepted spec's shadow rules.

Tests:

1. Unit: clean pre-opened WAL handle + sibling `ftruncate(0)` + fsync commit + own 32-byte header write + fsync must not return EEXIST.
2. Field-level debug logging (`BaseRev`, `hasRewriteBase`, `rewriteBaseRevision`, `sqliteWALTruncated`, `appendSafe`) confirms the rebind before landing.
3. The same scenario with a shadow-spilled sibling (`ShadowSpill` set at adoption) must also rebind; the shadow-removed early-continue must not skip it.
4. Salts-unchanged fallback: a 32-byte header write whose salts equal `H0` publishes a correct 32-byte object without rotating the shadow.

### 4.2 W2: serve checkpoint WAL reads locally

Current behavior:

1. Sidecar reads bypass stable caches: `bypassStableRemoteReadCachesForSQLiteSidecar` (pkg/fuse/dat9fs.go:1727).
2. The dirty-sibling fast path only accepts `DirtySeq > 0` candidates (pkg/fuse/dat9fs.go:4600, pkg/fuse/dat9fs.go:4772); the writer is clean after fsync.
3. A clean handle's own buffer is deliberately refused (the `-shm` short-read guard).
4. The revision-keyed read cache is only populated on full materialization: `cacheCommittedSQLitePersistentJournalLocked` requires `writeBufferHasLoadedFullRange` (pkg/fuse/dat9fs.go:3826).
5. A sub-64 MiB WAL never gets a shadow: `smallFileShadowThreshold = 64 << 20` (pkg/fuse/shadow.go:17).

Result: every page is one remote range GET, counted as
`sqlite-sidecar-clean-remote` (pkg/fuse/dat9fs.go:11881).

Change:

1. Allow revision-gated clean-sibling reuse for sidecar reads: serve from a clean sibling buffer/shadow only when its revision equals the latest committed revision (`latestCommittedRevisionWithSize`), preserving the `-shm` short-read protection.
2. Enable a write-through path shadow for configured append-log WAL files below 64 MiB so the checkpoint fd reads the local shadow instead of remote ranges.

Tests:

1. Second-generation checkpoint reads no longer use `sqlite-sidecar-clean-remote`.
2. Regression: a clean sibling whose revision is behind the latest committed revision is not served (short-read protection).

### 4.3 W3: allow PATCH when main.db grows (client-only)

Current behavior:

1. `canPatchExisting` requires `size <= fh.OrigSize` (pkg/fuse/dat9fs.go:12826).
2. `usePatch` requires `size <= handleOrigSize` (pkg/fuse/dat9fs.go:15252).
3. Both are disabled when `StorageClass == DB9`.
4. Growth falls into whole-file hydration + upload: all 257 parts load, `materializeFullForUploadLocked` builds full-size in-memory copies, and the strict-sync writeback path uploads with the 30 s `fuseTimeout` context (pkg/fuse/dat9fs.go:452) created at `Fsync` entry (pkg/fuse/dat9fs.go:13663) and passed to `UploadSyncWithRevision` (pkg/fuse/dat9fs.go:14038). That path has no PATCH at all, so relaxing the two gates alone does not touch it. The 30 s context expires before multipart initiation finishes -> `SQLITE_IOERR_FSYNC`; close retries via `Flush` under the size-aware `releaseTimeout(size)` (pkg/fuse/dat9fs.go:560).

Change:

1. Relax both gates to `OrigSize >= threshold && StorageClass != DB9`, dropping `size <= OrigSize`. Keep `OrigSize >= threshold` so inline (< 50 KB) files still take the cheap whole-file PUT; keep the DB9 override.
2. Add a residency guard: if a dirty part beyond the original file is not loaded (evicted by streaming), fall back to the full rewrite instead of uploading a zero-filled part. The guard must consult `IsPartLoaded` (as pkg/fuse/append_log.go:249 does): `PartData` (pkg/fuse/write.go:786) returns a zero-filled slice for an evicted in-range part, and the snapshot loop's `src != nil` check (pkg/fuse/dat9fs.go:12868) would otherwise capture and upload zeros.
3. Optional: skip the `read_url` download in `uploadPatchPart` (pkg/client/patch.go:163) when the callback already holds the complete merged part (FUSE returns `partSnapshots[partNumber]` and ignores `origData`).
4. Reroute the strict-fsync growth case: in the `Fsync` writeback branch (pkg/fuse/dat9fs.go:13991), when `fh.Dirty` qualifies under the relaxed gate (S3-backed, `OrigSize >= threshold`, dirty parts present), commit via `flushHandle` instead of `UploadSyncWithRevision`; first discard the path's writeback snapshot (pkg/fuse/dat9fs.go:14079 pattern) so the background uploader cannot re-upload a stale full image. The writeback uploader itself is unchanged for background commits.
5. Add a growth-region coverage guard. The server reconstructs every part outside the original object from the client upload and presigns such parts even when they are absent from the client's dirty set (pkg/backend/patch.go:247); a sparse `pwrite` beyond EOF leaves the gap parts neither loaded nor dirty in the WriteBuffer (pkg/fuse/write.go:259), and the fallback callback would then upload an empty body and corrupt the assembly. Therefore, before taking the PATCH route, require that every part overlapping the region `[OrigSize, newSize)` is present in `DirtyPartNumbers()` and loaded (`IsPartLoaded`); otherwise fall back to the full rewrite, whose `bytesView` zero-fills the gaps with the correct logical zeros. SQLite's contiguous truncate-extend and page writes satisfy the guard; only sparse growth is rerouted.
6. Widen the patch-failure fallback. The write-sync path today retries the full upload only on `IsUnsupportedStorageTargetErr` (pkg/fuse/dat9fs.go:12899); a plan rejection such as part-count-over-`MaxMultipartParts` would surface as a hard error instead. Treat patch-plan initiation failures (unsupported storage target, part count over the S3 limit, invalid plan) as triggers for the existing full-rewrite fallback in both the write-sync and flushHandle paths; part-upload failures after a valid plan keep failing closed.

The server contract is already in place: parts beyond the original object carry
no `read_url` (pkg/backend/patch.go:44) and are never presigned
(pkg/backend/patch.go:290).

Tests:

1. Growth with dirty parts beyond `OrigSize` uses PATCH and performs no whole-file download.
2. Scattered dirty parts.
3. Inline growth stays a full PUT.
4. Inline -> S3 cross-threshold adoption.
5. Strict fsync of a grown S3-backed file commits via PATCH with no whole-file upload and no stale writeback re-upload.
6. Sparse growth (`pwrite` beyond EOF leaving gap parts) falls back to the full rewrite and produces zero-filled gaps, never a corrupt PATCH.

### 4.4 W4: deadline / memory coherence (optional)

With W3 in place the full-file fallback no longer runs on the growth path, so
the 30 s deadline failure and the `bytesView` + `dataCopy` amplification are
mostly avoided. Remaining hardening:

1. Derive the lazy part loader context from the parent context instead of `context.Background()` (pkg/fuse/dat9fs.go:3215; loader installed at pkg/fuse/dat9fs.go:3197).
2. Align the strict-sync writeback upload with `releaseTimeout(size)` (pkg/fuse/dat9fs.go:560): re-derive the upload context at pkg/fuse/dat9fs.go:14038 instead of reusing the 30 s `fuseCtx`, mirroring the append-log (pkg/fuse/dat9fs.go:13753) and shadowspill (pkg/fuse/dat9fs.go:13862) paths.

### 4.5 W5: cross-handle read consistency audit

Audit `-shm` reader-index semantics, kernel-cache bypass, shadow generations,
and close-to-open visibility under the three-connection topology; fix gaps
found. This is the guard rail for W2's clean-sibling reuse; its size is
audit-driven and its findings feed back into W2's acceptance tests.

### 4.6 W6: verification

1. Re-run the #902 matrix on `main`: 1/2 MiB and 2 GiB.
2. Add a three-connection e2e (writer + checkpoint + reader) covering TRUNCATE timing, the post-checkpoint writer header fsync, and the 2 GiB growth case.
3. Server-side: add a contract test for the growth patch plan (parts beyond the original object without `read_url`).

## 5. Spec amendments (pending acceptance)

On acceptance, amend `docs/design/s3-express-append-log-fuse.md`:

1. Out-of-scope item 5: narrow "Multi-writer coordination" to "Multi-mount / multi-process concurrent writers"; same-mount multi-connection coordination is covered by this design.
2. Section 3.2 final paragraph: replace "multi-writer coordination remains out of scope" with the same narrowed statement.

The amendment is recorded here but not yet applied to the accepted spec.

## 6. Acceptance criteria

1. #902 matrix passes: checkpoint in seconds (not 38-91 s), no EEXIST / `SQLITE_IOERR_FSYNC`, and the 2 GiB case converges so every acknowledged transaction is recoverable from `main.db` alone (`quick_check=ok`).
2. Checkpoint WAL reads are served from shadow/clean-sibling; the `sqlite-sidecar-clean-remote` flood disappears.
3. 2 GiB growth uploads only dirty parts with server-side copies of untouched parts (no 2 GiB download, bounded RSS).
4. Writers block only on SQLite locking, never on a Drive9-side stale generation; concurrent readers see consistent WAL snapshots.

## 7. Risks and observability

1. W2 stale-sibling reuse: mitigated by the revision-equality gate plus the behind-revision regression test; the `-shm` short-read guard stays in force.
2. W3 zero-filled part corruption: mitigated by the residency guard (`IsPartLoaded`), which fails closed to a full rewrite.
3. W1 dirty-sibling adoption: the existing `handleCanAdoptCommittedRevisionLocked` gate keeps the silent-rollback shape impossible.
4. Fail-closed CAS semantics are preserved; no EEXIST retry loop is added.

## 8. Traceability

| #902 mode | Root cause | Workstream | Acceptance |
| --- | --- | --- | --- |
| A | serial remote sidecar reads | W2 | 2 |
| B | stale append-log baseline EEXIST | W1 | 1, 4 |
| C | PATCH disabled on growth | W3 | 1, 3 |
