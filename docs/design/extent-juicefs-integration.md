---
title: Extent Content Layout via Embedded JuiceFS Data Plane
status: proposal
date: 2026-09-07
---

## 1. Background and problem statement

drive9 needs a `content_layout=extent` read/write path for files matched by the
mount profile `[extent]` section: chunked, slice-overlaid, block-addressed
storage (the JuiceFS model) for overwrite-heavy workloads such as SQLite WAL,
while `single`/`append_log` files keep their existing paths.

The first attempt re-implemented the JuiceFS model on drive9 primitives
(per-object presigned URLs, drive9-server HTTP, TiDB): 64MiB chunks / 4MiB
blocks, per-chunk append-only slice blobs in `file_chunks`, CAS commits via
`inodes.revision` + `contents.slice_generation`, a `prepare-blocks` /
`commit-slices` / `read-plan` / `compact-slices` HTTP protocol, and a FUSE-side
writer/reader/staging/compactor (`pkg/fuse/extent_*.go`,
`pkg/client/extent*.go`, `pkg/backend/extent.go`, `pkg/datastore/extent*.go`).

That implementation has not converged. The recurring defect themes, visible in
code comments and the test matrix, are exactly the ones JuiceFS already solves
upstream:

- read consistency of the slice list vs. Stat (all-zero btree pages under
  sqlite multiwrite; VACUUM truncate-then-write exposing holes),
- presigned ranged-GET semantics when S3 ignores Range (compacted block bytes
  misinterpreted as page data),
- concurrent-write slice/reference accounting (negative refs, EIO),
- compact-vs-write CAS interactions,
- crash recovery of staged writes and expired presign retries.

All extent code is currently **uncommitted** (untracked files only). There is
no deployed extent data, so pivoting costs code deletion, not data migration.
`TODO.md` already records the question this document answers: "Missing a clear
cutover point where JuiceFS-like integration is the better path."

## 2. Proposal

Embed JuiceFS community edition (Apache-2.0) as a library, and give
`content_layout=extent` files the complete JuiceFS **data plane**:

- reuse `pkg/chunk` (block layout, chunk store, disk cache/writeback),
- reuse `pkg/vfs` `DataReader` / `DataWriter` (the write-buffering, slice-freeze,
  flush, and read path semantics that are the defect-prone part of the current
  reimplementation),
- implement `pkg/meta`'s `Meta` interface as an HTTP client of a new
  drive9-server meta API, backed by tenant TiDB transactions,
- replace per-object presigned URLs with **tenant-prefix-scoped, auto-refreshing
  data credentials**; the client talks to S3 directly for extent blocks.

The namespace (dentries, rename, permissions, locks, xattrs, search/tags) stays
entirely in the existing drive9 path-based model. `extent` describes how a
file's bytes are organized, not where its name lives.

### Non-goals

1. Mapping drive9's whole namespace onto JuiceFS meta ("Pattern B"). It would
   make extent files invisible to drive9 search/tags/layers, cannot represent
   cross-layout renames, and fights the dentry model (drive9 directory rename
   is a subtree `UPDATE`; JuiceFS is a single edge insert).
2. Importing JuiceFS's own FUSE frontend (`pkg/fuse` in juicefs). drive9 keeps
   `Dat9FS`.
3. Client-side-encrypted extent blocks. Server-side compaction/GC requires the
   server to read block bytes (see §6.4); this is a recorded constraint, not a
   regression versus the current presign+SSE model.
4. Multi-machine concurrent writers on one extent file beyond JuiceFS's
   close-to-open semantics.

## 3. Verified constraints that shape the design

These were checked against juicefs main (`pkg/meta/interface.go`,
`pkg/meta/utils.go`, `pkg/vfs/vfs.go`, `pkg/chunk/cached_store.go`,
`pkg/object/object_storage.go`, `go.mod`, `Makefile`):

1. **`meta.Meta` cannot be implemented outside package `meta`.** The interface
   contains unexported methods (`getBase() *baseMeta`, `chroot(Ino)`). A custom
   meta driver must live inside juicefs's `pkg/meta` package → we maintain a
   fork `mem9-ai/juicefs` and select it with a `replace` directive, adding one
   additive file/package (the `drive9` meta driver). Precedent: go.mod already
   replaces `hanwen/go-fuse/v2` with `mornyx/go-fuse/v2`.
2. **No go-fuse conflict.** `pkg/vfs`, `pkg/chunk`, `pkg/meta` do not import
   go-fuse (verified import lists). Only juicefs's `pkg/fuse` does, and we do
   not import it.
3. **Data-plane-only reuse is supported upstream.** `vfs.NewDataReader(conf, m,
   store)` and `vfs.NewDataWriter(conf, m, store, reader)` are exported
   constructors; juicefs's own `pkg/fs` SDK frontend consumes them directly.
4. **Compaction and block deletion are client-callback driven upstream.**
   `Meta.Write`/`Read` trigger `compactChunk` when a chunk's slice count
   crosses thresholds; cleanup goroutines push deleted slices to a local
   `dslices` channel; both dispatch through `baseMeta.msgCallbacks`
   (`OnMsg(CompactChunk/DeleteSlice)`) to the *same process's* chunk store.
   Cross-client coordination uses meta-store locks/counters, not messages. With
   the engine behind HTTP, we replace this push model with server-side GC and
   a pull-based compact task queue (§5.4), so the meta driver's `OnMsg` is a
   no-op.
5. **Dependency cost is trimmable but real.** juicefs is a single Go module
   that by default compiles every object-store SDK and meta engine
   (redis/tikv/etcd/...). Upstream's `no*` build tags (see juicefs Makefile
   `juicefs.lite`) cut the compile surface to just what we need (s3 object
   backend; no built-in meta engines). The go.mod require-graph still grows,
   and juicefs's `go 1.25.10` directive forces a toolchain bump from our
   `go 1.25.1`.
6. **License.** Apache-2.0; embedding and modification are permitted with
   NOTICE preservation.
7. **Tenant credential scoping is strictly stronger than upstream.** Stock
   juicefs stores full-bucket credentials in the volume `Format` and hands them
   to every mount. We issue prefix-scoped, short-lived credentials instead
   (§5.3).

## 4. Architecture

```
kernel FUSE ──> Dat9FS (unchanged frontend)
                  │  profile [extent] glob at create; ContentLayout from stat
                  ├─ single / append_log files → existing writeback/shadow path
                  └─ extent files ──> vfs.DataWriter / vfs.DataReader
                                          │
                            ┌─────────────┴───────────────┐
                      meta.Meta "drive9" driver      chunk.ChunkStore
                      (in mem9-ai/juicefs fork;            │
                       thin HTTP client + retry)    object.ObjectStorage
                            │                        "drive9s3": tenant-scoped
                            ▼                         creds, auto-refresh
                    drive9-server meta API                   │
                    (TiDB txns: slices, slice-id             ▼
                     counter, sessions, length/       S3  t/<tenant>/chunks/...
                     mtime, compact tasks, GC queues)
```

### 4.1 Routing (existing, kept)

- Profile `[extent]` globs are evaluated client-side
  (`pkg/fuse/extent.go:shouldUseExtentPath`); create carries
  `X-Dat9-Content-Layout: extent`; the server never infers layout from paths
  (`pkg/datastore/layout.go:ResolveContentLayout`).
- Existing files dispatch on the stat-returned `ContentLayout`.
- `pkg/datastore/layout.go` constants already match JuiceFS (64MiB chunk,
  4MiB max block) and stay.

### 4.2 Client data plane (replaces `pkg/fuse/extent_{writer,read,staging,compact}.go`)

- On Open of an extent file, Dat9FS binds a `vfs.DataReader`/`DataWriter` pair
  keyed by the drive9 inode mapped to a `meta.Ino` (server allocates a uint64
  extent-inode number at create; see §5.2).
- Write/Flush/Fsync/Truncate delegate to the DataWriter; Read delegates to the
  DataReader. Kernel-cache policy stays drive9's (existing bypass/SSE
  invalidation wiring), with `reader.Invalidate` hooked to SSE events.
- The per-handle bespoke writer, zero-from tracking, hole punching, staging
  replay, and client-side compact loop are deleted. Crash durability comes from
  the chunk store's writeback cache dir (integrated with drive9's existing
  cache-dir and quota management).
- Locks, xattrs, chmod, rename remain on the existing drive9 paths — the
  DataWriter never sees them.

### 4.3 Object storage and credentials

- Block keys move from the global `blocks/<fileID>/<ulid>` namespace to a
  tenant-isolated JuiceFS layout: `t/<tenantID>/chunks/<id/1e6>/<id/1e3>/<id>_<idx>_<len>`
  (prefix hashing optional). Prefix isolation is what makes tenant-scoped
  credentials safe.
- New endpoint `POST /v1/data-credential` → `{endpoint, bucket, prefix,
  accessKey, secretKey, sessionToken, expiresAt}`:
  - AWS / MinIO: STS `AssumeRole` with a session policy pinned to the tenant
    prefix;
  - mock/local backend: pass-through;
  - backends without STS: documented fallback is a narrow server-presign
    ObjectStorage (slower path, kept behind a capability flag).
- The client's `object.ObjectStorage` implementation refreshes credentials
  before expiry (juicefs's s3 backend only supports static credentials, hence
  a small custom backend modeled on `pkg/object/webdav.go`/`restful.go`).
- This replaces the 10-minute per-object presign TTL machinery
  (`PresignPutObject` per block, `PresignGetObjectRange` per read window) and
  the `prepare-blocks`/`pending_blocks` protocol. JuiceFS semantics are "data
  lands in S3 first, then `Meta.Write` records the slice", so no per-block
  server authorization is needed; orphaned blocks from crashed writes are
  reclaimed by the pending-slice scanner (§5.4).

### 4.4 Meta API on drive9-server

The fork's `drive9` meta driver is a thin, stateless HTTP client. All
consistency logic lives server-side in TiDB transactions against the tenant
schema. Minimal operation set (everything else in the `Meta` interface returns
ENOSYS initially):

| Group | Operations | Notes |
| --- | --- | --- |
| Session | `NewSession`, `CloseSession`, `FlushSession`/heartbeat | session table; stale-session sweep releases sustained files |
| IDs | `NewSlice` (batch, e.g. 4096) | global uint64 counter per tenant |
| Data | `Read` (slices for chunk), `Write` (append slice **and** update length/mtime/space in one txn), `Truncate` | mirrors `WriteSliceTx` semantics; Write response carries the chunk's slice count as a compact hint |
| Handles | `Open`, `Close` | sustained-delete: unlinked-but-open files are GC'd only after last close (fixes today's open-unlinked gap) |
| Attr | `GetAttr`, `SetAttr`, `Access`, `StatFS` | backed by existing inode/contents rows |
| Compaction | `Compact` (CAS with origin slice list) | called by whoever executes a compact task |
| Driver glue | `Init`, `Load`, `GetFormat`, `Name`, `Shutdown`, `OnMsg` (no-op), `OnReload` | `Format` persisted per tenant by drive9-server |

Schema work aligns the existing extent tables to JuiceFS semantics rather than
inventing new ones: `file_chunks` stays a per-chunk append-only slice blob but
addressed by `(extent_ino, chunk)` with global uint64 slice ids;
`file_slice_refs` becomes the block reference table feeding GC; new small
tables: `extent_sessions`, `extent_meta_counters`, and the compact task queue
(`slice_compact_tasks` reworked).

### 4.5 Garbage collection and compaction without OnMsg

- **Block GC is server-side** (`block_gc_tasks` + `extent_worker` pattern kept):
  deleting blocks is `DeleteObject` calls with zero data movement. Slices are
  marked deleted in the same txn that unlinks/supersedes them; a grace period
  protects in-flight readers. This removes the need to deliver `DeleteSlice`
  to any client.
- **Compaction is server-scheduled, client-executed (pull model):**
  1. the `Write` handler checks the slice-count threshold in the same txn and
     enqueues a `slice_compact_tasks` row;
  2. any mounted client lease-claims the task (existing worker pattern), reads
     the chunk via its DataReader, merges, uploads a new slice with its scoped
     credentials, and CAS-commits via `Compact`;
  3. the server GCs the superseded slices.
- **Optional server-side fallback worker** (bounded concurrency, per-tenant
  rate limit) executes compact tasks when no client is online — strictly more
  available than upstream, where zero mounts means no compaction and no GC.
- Server-side compaction cost is explicit: per compacted chunk up to 64MiB S3
  read + 64MiB write + deletes. It loses the writer's disk-cache locality and
  concentrates bandwidth on the control plane, which is why the pull model is
  the default and the fallback is throttled. Metrics: compact count, CAS-fail
  rate, bytes moved (extending `drive9_extent_put_bytes_total`).

### 4.6 Multi-client coherence

JuiceFS gives close-to-open consistency: slice lists are read from meta at
open/read time; attr/slice caches are bounded by TTL. drive9 keeps its existing
kernel-cache bypass + SSE invalidation for cross-mount visibility; compaction
commits bump the chunk's generation, and SSE invalidation triggers
`DataReader.Invalidate`. This is the same coherence envelope the current extent
code targets, but the slice machinery inside it is upstream-tested code.

## 5. What is deleted, what survives

Deleted (all currently uncommitted):

- `pkg/fuse/extent_writer.go`, `extent_staging.go`, `extent_read.go`,
  `extent_compact.go`, most of `extent_open.go` state machine
- `pkg/client/extent.go` prepare/commit protocol, `client/extent_read.go`
- `pkg/server/extent.go` endpoints: `prepare-blocks`, `commit-slices`,
  `read-plan`, `presign-put`, `compact-slices`
- `pending_blocks`, `slice_commit_ops` machinery

Survives / is reworked:

- profile `[extent]` routing and `X-Dat9-Content-Layout` plumbing
- `pkg/datastore/layout.go` constants and `content_layout` columns
- `file_chunks` / `file_slice_refs` tables (reworked to slice-id addressing)
- `block_gc_tasks` / `slice_compact_tasks` queues and `extent_worker.go`
  (reworked into GC + compact execution)
- the extent e2e gates (§8), re-pointed at the new path
- the 96 extent FUSE unit tests are triaged: behaviors now owned by JuiceFS
  drop to upstream coverage; drive9-specific glue keeps targeted tests

## 6. Risks and mitigations

1. **Fork maintenance** (`mem9-ai/juicefs`, additive `pkg/meta` driver).
   Mitigation: pin upstream tags, rebase quarterly, keep the delta to one
   package; same workflow as the existing go-fuse fork.
2. **The HTTP meta driver is new consistency-sensitive code.** Mitigation: it
   is a thin stateless stub (~12 meaningful methods); server handlers are TiDB
   transactions; behavior is tested against juicefs's own SQL-engine semantics
   as the reference.
3. **Dependency/toolchain cost.** juicefs go.mod requires `go 1.25.10`; the
   require-graph grows even though `no*` build tags cut the compile surface.
   Mitigation: CI toolchain bump, supply-chain review of the trimmed set,
   binary-size budget check in Phase 0.
4. **Server-side data movement** (fallback compactor, GC is free).
   Mitigation: pull model by default; fallback worker capped (2–4 concurrent,
   per-tenant rate limit); metrics and alerts on bytes moved and CAS failures.
5. **DataWriter/DataReader used outside a full `vfs.VFS`** — the precise
   attr-length handoff and invalidation contract is the top spike risk.
   Mitigation: Phase 0 acceptance harness (§7); fallback is embedding the whole
   `vfs.VFS` for extent files (still with the data-plane-only meta driver).
6. **Record-locked constraint:** extent blocks must remain server-readable
   (SSE or none); client-side encryption of extent blocks is out of scope.

## 7. Execution plan

### Phase 0 — spike (~1 week; gates the whole effort)

1. Fork `juicedata/juicefs` → `mem9-ai/juicefs`; add `pkg/meta` `drive9`
   driver skeleton implementing the §4.4 subset against a mock server.
2. drive9 `go.mod` replace + `no*` build tags; verify build, toolchain bump,
   binary-size delta.
3. Standalone harness: DataWriter → mock meta → local-disk ObjectStorage →
   DataReader, under random writes, overwrites, ftruncate, and reopen.
   **Acceptance: DataWriter/DataReader are usable without a full vfs.VFS, with
   a documented attr/length/invalidation contract.**

### Phase 1 — server (~2 weeks)

4. `POST /v1/data-credential` with tenant-prefix scoping + refresh contract.
5. Meta HTTP API (§4.4) on tenant TiDB; schema alignment; session sweep;
   open-unlinked sustained GC.
6. Compact task queue + CAS commit endpoint; server-side block GC rework.

### Phase 2 — client (~2 weeks)

7. `drive9s3` ObjectStorage with credential auto-refresh.
8. Dat9FS rewiring: extent open → DataReader/DataWriter; delete the bespoke
   writer/staging/read-plan/compact code and the prepare/commit endpoints;
   SSE → `Invalidate` wiring.
9. Compact executor in the mount process (lease-claim loop).

### Phase 3 — hardening (~2 weeks, overlapping rollout)

10. Re-run the existing e2e gates on the new path: `fuse-sqlite-correctness.sh`
    (the historical defect hotspot), `fuse-sqlite-commit-sequence.sh`, fio,
    pjdfstest subset, `fuse-crash-recovery-test.sh`, plus a new dual-mount
    coherence script.
11. Triage/delete the 96 extent unit tests; add reference-behavior tests for
    the meta driver and server txn handlers.
12. Perf baseline versus the old extent path and versus `single` upload path;
    publish numbers in the PR.

Total estimate: **5–7 weeks, one engineer**, with a hard go/no-go at the end of
Phase 0.

## 8. Acceptance criteria

- `coding-agent-extent` profile passes the full FUSE e2e suite on the JuiceFS
  data plane, including the sqlite WAL correctness and crash-recovery gates.
- No per-object presign on the extent path: extent block I/O uses only
  tenant-scoped credentials; `prepare-blocks`/`commit-slices`/`read-plan` are
  gone from the server router.
- Orphan blocks and stale sessions are reclaimed without any client online.
- pjdfstest results on extent files are no worse than on `single` files.

## 9. References

- JuiceFS: `pkg/meta/interface.go` (Meta interface, unexported methods),
  `pkg/meta/utils.go` (`msgCallbacks`), `pkg/meta/base.go` (Write commit,
  compactChunk, session loops), `pkg/vfs/vfs.go` (`NewVFS`, `NewDataReader`,
  `NewDataWriter`), `pkg/chunk/cached_store.go` (block key layout),
  `pkg/object/object_storage.go` (ObjectStorage minimal surface), juicefs
  Makefile (`no*` build-tag list).
- drive9: `pkg/datastore/layout.go`, `pkg/datastore/extent*.go`,
  `pkg/backend/extent*.go`, `pkg/client/extent*.go`, `pkg/server/extent.go`,
  `pkg/fuse/extent*.go`, `cmd/drive9/cli/profile.go`,
  `pkg/tenant/schema/extent.go`, `TODO.md`.
