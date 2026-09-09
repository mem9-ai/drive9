---
title: Extent Content Layout via Embedded JuiceFS Data Plane and Meta
status: proposal
date: 2026-09-07
revision: 2026-09-07 r2 (JuiceFS meta is SoT for extent names/inodes; file_nodes is a same-txn projection)
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
`content_layout=extent` files the complete JuiceFS **data plane and meta
model** (not only slices):

- reuse `pkg/chunk` (block layout, chunk store, disk cache/writeback),
- reuse `pkg/vfs` `DataReader` / `DataWriter` (write-buffering, slice-freeze,
  flush, read; the defect-prone part of the current reimplementation),
- implement `pkg/meta`'s `Meta` interface as an HTTP client of a new
  drive9-server meta API, backed by tenant TiDB transactions — including
  **Create / Unlink / Lookup / Rename / Open / Close / Flock / Setlk**, not
  only `Read`/`Write` of slices,
- replace per-object presigned URLs with **tenant-prefix-scoped, auto-refreshing
  data credentials**; the client talks to S3 directly for extent blocks.

**Source of truth for an extent file's name, nlink, inode, length, and slices
is JuiceFS meta** (server-side tables + one RPC per Meta op). drive9
`file_nodes` is a **projection written in the same TiDB transaction**, so
`ls`, `rm -r`, search, tags, and the HTTP API still see the file. The FUSE
client never dual-writes; `Meta.Create` / `Meta.Unlink` are each a single
RPC whose server handler updates both trees atomically.

Directories are not extent objects. Parent dirs of extent files are
**lazily mirrored** into JuiceFS as dir inodes so `Meta.Create(parent, name)`
has a parent (see §4.2). `ls` of a directory still reads drive9 `file_nodes`.

This is **not** Pattern B (the whole drive9 namespace lives only in JuiceFS
meta). Pattern B would hide extent files from search/tags/layers and fights
directory rename (drive9 subtree `UPDATE` vs JuiceFS single edge). The
projection exists so the drive9 tree can *point at* JuiceFS inodes.

### Non-goals

1. Pattern B: replacing drive9 `file_nodes` with JuiceFS `edge` as the only
   namespace. Extent files must remain visible to `ls` / `rm -r` / search /
   tags / the HTTP API; that is what the same-txn projection is for.
2. FUSE doing two HTTP calls (Meta.Create then InsertNode, or DELETE then
   CreateFileWithLayout). That is the journal `create_conflict` / `--wait all`
   failure mode of the first attempt.
3. Importing JuiceFS's own FUSE frontend (`pkg/fuse` in juicefs). drive9 keeps
   `Dat9FS` as the kernel frontend and routes extent files to DataWriter/Reader
   after Meta.Create returns an `extent_ino`.
4. Client-side-encrypted extent blocks. Server-side compaction/GC requires the
   server to read block bytes (see §6); this is a recorded constraint, not a
   regression versus the current presign+SSE model.
5. Multi-machine concurrent writers on one extent file beyond JuiceFS's
   close-to-open semantics.
6. Layer/fork copy-on-write of extent inodes in v1. Layers keep path whiteouts
   on the projection; writing extent files inside a layer is out of scope until
   a follow-up (COW a new `extent_ino` or bind the layer to a meta session).
   The `coding-agent-extent` sqlite gate does not use layers.

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
3. **DataReader/DataWriter are usable without JuiceFS FUSE.** `vfs.NewDataReader(conf, m,
   store)` and `vfs.NewDataWriter(conf, m, store, reader)` are exported
   constructors; juicefs's own `pkg/fs` SDK frontend consumes them directly.
   Name ops still go through `meta.Meta` (Create/Unlink), not through Dat9FS
   HTTP.
4. **Compaction and block deletion are client-callback driven upstream.**
   `Meta.Write`/`Read` trigger `compactChunk` when a chunk's slice count
   crosses thresholds; cleanup goroutines push deleted slices to a local
   `dslices` channel; both dispatch through `baseMeta.msgCallbacks`
   (`OnMsg(CompactChunk/DeleteSlice)`) to the *same process's* chunk store.
   Cross-client coordination uses meta-store locks/counters, not messages. With
   the engine behind HTTP, we replace this push model with server-side GC and
   a pull-based compact task queue (§4.6), so the meta driver's `OnMsg` is a
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
   (§4.4).

## 4. Architecture

```
kernel FUSE ──> Dat9FS (frontend kept)
                  │  profile [extent] glob at create; ContentLayout from stat
                  ├─ single / append_log / dir → existing drive9 paths
                  └─ extent files
                        │
                        ├─ Create/Unlink/Lookup/Rename/Flock
                        │     → meta.Meta "drive9" driver (one RPC)
                        └─ Read/Write/Flush/Fsync/Truncate
                              → vfs.DataWriter / vfs.DataReader
                                        │
                          ┌─────────────┴───────────────┐
                    meta.Meta HTTP client         chunk.ChunkStore
                    (mem9-ai/juicefs fork)               │
                          │                       object.ObjectStorage
                          ▼                       "drive9s3" STS, auto-refresh
                  drive9-server meta API                  │
                  ONE TiDB txn:                           ▼
                    JuiceFS node+edge+chunk        S3  t/<tenant>/chunks/...
                    + file_nodes projection
```

### 4.1 Routing (existing, kept)

- Profile `[extent]` globs are evaluated client-side
  (`pkg/fuse/extent.go:shouldUseExtentPath`); create carries
  `X-Dat9-Content-Layout: extent`; the server never infers layout from paths
  (`pkg/datastore/layout.go:ResolveContentLayout`).
- Existing files dispatch on the stat-returned `ContentLayout`.
- `pkg/datastore/layout.go` constants already match JuiceFS (64MiB chunk,
  4MiB max block) and stay.

### 4.2 Extent names: JuiceFS meta is SoT, `file_nodes` is a same-txn projection

Dat9FS does **not** call `CreateFileWithLayout` or HTTP DELETE for extent
files. Create/Unlink/Lookup of an extent name go through the meta driver,
matching JuiceFS `VFS.Create = Meta.Create` then `newFileHandle` and
`VFS.Unlink = Meta.Unlink` (name/nlink only).

The server handler for each of those RPCs updates **both** the JuiceFS tables
and the drive9 projection in **one tenant TiDB transaction**. Duplicate is
allowed; dual HTTP from FUSE is not.

| Question | Source of truth | Projection (`file_nodes`) |
| --- | --- | --- |
| Is this path an extent file? | JuiceFS `edge` + `node` | row: `path`, `content_layout=extent`, `extent_ino` |
| nlink, open-unlinked, length, slices | JuiceFS `node` + chunk blob | optional denormalized size/mtime for `ls -l` / search, updated in the same `Write`/`Truncate` txn |
| Directory listing | — | `ls` / `readdir` read `file_nodes` only (no JuiceFS Readdir on the hot path) |
| search / tags / HTTP API | — | continue to key on `file_nodes.path` |

**Parent directories.** JuiceFS `Meta.Create(parent, name)` needs a parent dir
inode. Directories are not extent files.

- **Lazy mirror (mixed profile, e.g. only `*.db`):** on first extent create
  under `/foo/bar.db`, the server ensures JuiceFS dir inodes for `/` and
  `/foo/` exist, then `Create(foo_ino, "bar.db")` and inserts the projection.
- **Full mirror (`coding-agent-extent` with `ExtentPaths: *`):** every regular
  file on that mount is extent; the JuiceFS dir tree is 1:1 with drive9
  directories. This is the sqlite product gate. Projection still exists so
  `ls` / `rm -r` / search do not change entry points.

`ls /foo/` always reads drive9 `file_nodes`. JuiceFS Readdir is not used for
user-visible listings.

**Create** (one RPC, one txn), JuiceFS order:

1. allocate tenant-unique `extent_ino` (uint64);
2. insert JuiceFS `node` + `edge(parent, name)`;
3. insert `file_nodes(path, layout=extent, extent_ino, …)`;
4. commit.

The client then `DataWriter.Open(extent_ino, 0)`. Failure rolls the whole txn
back. A sqlite `.db-journal` create is this RPC, not a local-first handle
that later `CreateFileWithLayout`s.

**Unlink** (one RPC, one txn), JuiceFS order:

1. delete `edge`, `nlink--`;
2. delete (or tombstone) the `file_nodes` row so `ls` no longer sees the name.

If `nlink > 0` or a session still holds the inode, the JuiceFS node remains
(open-unlinked / hard link). The projection is already gone so `ls` is clean.
A subsequent Create of the same name allocates a **new** `extent_ino` — it
must not Stat the leftover path. That is the journal `unlink` then `create`
sequence community.sqlite needs.

**`rm -r`:** drive9 walks the projection; each `layout=extent` child is the
same Unlink RPC; empty directories are still drive9 `DeleteEmptyDir`.

**Rename file:** one txn: JuiceFS `Rename` (edge) + `file_nodes.path` update.

**Rename directory:** drive9 keeps subtree `UPDATE path` on the projection;
if the directory was lazily/fully mirrored, JuiceFS renames that one dir
edge. Children in JuiceFS are relative names and do not need per-file updates.

**POSIX locks** on extent files (`Flock` / `Setlk`) go through the meta
driver — sqlite needs them on the inode. chmod/chown/xattr stay on the
drive9 inode/projection so the existing HTTP API does not fork.

**Layers.** v1: layer whiteouts apply to the projection only. JuiceFS inodes
on the main view are unchanged. Writing extent files inside a layer is a
follow-up (COW a new `extent_ino`). Not required for `coding-agent-extent`
sqlite.

### 4.3 Client data plane (replaces `pkg/fuse/extent_{writer,read,staging,compact}.go`)

- After Meta.Create (or Lookup of an existing extent projection), Dat9FS
  binds a `vfs.DataReader`/`DataWriter` pair keyed by `extent_ino`.
- Write/Flush/Fsync/Truncate delegate to the DataWriter; Read to the
  DataReader. Kernel-cache policy stays drive9's (bypass/SSE), with
  `reader.Invalidate` hooked to SSE events.
- The per-handle bespoke writer, zero-from tracking, hole punching, staging
  replay, and client-side compact loop are deleted. Crash durability comes from
  the chunk store's writeback cache dir (integrated with drive9's existing
  cache-dir and quota management).
- DataWriter never sees dentries. Name ops are §4.2.

### 4.4 Object storage and credentials

- Block keys move from the global `blocks/<fileID>/<ulid>` namespace to a
  tenant-isolated JuiceFS layout: `t/<tenantID>/chunks/<id/1e6>/<id/1e3>/<id>_<idx>_<len>`
  (prefix hashing optional). Prefix isolation is what makes tenant-scoped
  credentials safe.
- New endpoint `POST /v1/data-credential` → `{endpoint, bucket, prefix,
  accessKey, secretKey, sessionToken, expiresAt}`:
  - AWS (and MinIO with a role): STS `AssumeRole` with a session policy
    pinned to the tenant prefix;
  - local MinIO without STS: mint the server's static keys as a
    non-expiring `scheme=s3` session; the FUSE client still talks S3/MinIO
    directly (same JuiceFS object path as production);
  - filesystem mock (`LocalS3Client`, unit tests): `scheme=file` pass-through.
  Extent block I/O never goes through drive9-server HTTP.
- The client's `object.ObjectStorage` implementation refreshes credentials
  before expiry (juicefs's s3 backend only supports static credentials, hence
  a small custom backend modeled on `pkg/object/webdav.go`/`restful.go`).
- This replaces the 10-minute per-object presign TTL machinery
  (`PresignPutObject` per block, `PresignGetObjectRange` per read window) and
  the `prepare-blocks`/`pending_blocks` protocol. JuiceFS semantics are "data
  lands in S3 first, then `Meta.Write` records the slice", so no per-block
  server authorization is needed; orphaned blocks from crashed writes are
  reclaimed by the pending-slice scanner (§4.6).

### 4.5 Meta API on drive9-server

The fork's `drive9` meta driver is a thin, stateless HTTP client. All
consistency logic lives server-side in TiDB transactions against the tenant
schema. Each name op is **one RPC**; the handler writes JuiceFS tables and the
`file_nodes` projection together (see §4.2). Everything else in the `Meta`
interface returns ENOSYS initially.

| Group | Operations | Notes |
| --- | --- | --- |
| Session | `NewSession`, `CloseSession`, `FlushSession`/heartbeat | session table; stale-session sweep releases sustained files |
| Namespace | `Lookup`, `Create`, `Unlink`, `Rename`, `Mkdir` (lazy parent only) | same txn as `file_nodes` projection; Create allocates `extent_ino` |
| IDs | `NewSlice` (batch, e.g. 4096) | global uint64 counter per tenant |
| Data | `Read` (slices for chunk), `Write` (append slice **and** update length/mtime/space **and** projection size/mtime in one txn), `Truncate` | mirrors JuiceFS `Meta.Write`; response carries chunk slice count as a compact hint; keep this RPC thin (no extra blob-count SELECT on the CONCAT path) |
| Handles | `Open`, `Close` | sustained-delete: unlinked-but-open files are GC'd only after last close |
| Locks | `Flock`, `Getlk`, `Setlk` | posix/flock on the extent inode (sqlite); not the drive9 path lock table |
| Attr | `GetAttr`, `SetAttr`, `Access`, `StatFS` | JuiceFS `node` is SoT for length/mtime/nlink; projection denormalized in the same txn |
| Compaction | `Compact` (CAS with origin slice list) | called by whoever executes a compact task |
| Driver glue | `Init`, `Load`, `GetFormat`, `Name`, `Shutdown`, `OnMsg` (no-op), `OnReload` | `Format` persisted per tenant by drive9-server |

`Meta.Write` / `NewSlice` stay on the DataWriter freeze path (one HTTP per
committed slice after a batched `NewSlice`). That is acceptable for 4MiB
slices; it is **not** a substitute for putting Create/Unlink in this API.
Journal create/unlink must not go through Dat9FS `CreateFileWithLayout` /
HTTP DELETE.

Schema work aligns the existing extent tables to JuiceFS semantics rather than
inventing new ones: `file_chunks` stays a per-chunk append-only slice blob but
addressed by `(extent_ino, chunk)` with global uint64 slice ids;
`file_slice_refs` becomes the block reference table feeding GC; new small
tables: `extent_sessions`, `extent_meta_counters`, JuiceFS-style `node`/`edge`
(or equivalent), and the compact task queue (`slice_compact_tasks` reworked).
`file_nodes` gains `extent_ino` (nullable) for the projection.

### 4.6 Garbage collection and compaction without OnMsg

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

### 4.7 Multi-client coherence

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
- `file_nodes` as the **projection** (new `extent_ino` column); `ls` / `rm -r`
  / search still enter here
- `file_chunks` / `file_slice_refs` tables (reworked to slice-id addressing)
- `block_gc_tasks` / `slice_compact_tasks` queues and `extent_worker.go`
  (reworked into GC + compact execution)
- the extent e2e gates (§8), re-pointed at the new path
- the 96 extent FUSE unit tests are triaged: behaviors now owned by JuiceFS
  drop to upstream coverage; drive9-specific glue (projection txn, routing,
  STS refresh) keeps targeted tests

Dat9FS Create/Unlink of extent names stop calling `CreateFileWithLayout` /
HTTP DELETE; they call the meta driver.

## 6. Risks and mitigations

1. **Fork maintenance** (`mem9-ai/juicefs`, additive `pkg/meta` driver).
   Mitigation: pin upstream tags, rebase quarterly, keep the delta to one
   package; same workflow as the existing go-fuse fork.
2. **The HTTP meta driver is new consistency-sensitive code.** Mitigation: it
   is a thin stateless stub (~20 meaningful methods including namespace);
   server handlers are TiDB
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
   `vfs.VFS` for extent files (still with the drive9 meta driver).
6. **Meta HTTP is still on the Create/Unlink/Write path.** JuiceFS SQL is
   microseconds; one RPC is milliseconds. Mitigation: one RPC per name op
   (never two); batched `NewSlice`; thin `Write` (no extra SELECTs); Phase 0
   must include a delayed-mock-HTTP harness so we learn whether journal
   `--wait all` needs further batching before Phase 2.
7. **Same-txn dual tables.** A bug that commits JuiceFS `edge` without
   `file_nodes` (or the reverse) makes `ls` and sqlite disagree.
   Mitigation: one stored transaction; integration tests that kill the
   handler between the two inserts must still roll back; a repair job for
   orphans is a belt, not the protocol.
8. **Layers vs JuiceFS nlink.** v1 whiteouts touch only the projection
   (§4.2). Do not pretend layer-extent writes work until COW inodes exist.
9. **Record-locked constraint:** extent blocks must remain server-readable
   (SSE or none); client-side encryption of extent blocks is out of scope.

## 7. Execution plan

### Phase 0 — spike (~1 week; gates the whole effort)

1. Fork `juicedata/juicefs` → `mem9-ai/juicefs`; add `pkg/meta` `drive9`
   driver skeleton implementing the §4.5 subset against a mock server.
2. drive9 `go.mod` replace + `no*` build tags; verify build, toolchain bump,
   binary-size delta.
3. Standalone harness: DataWriter → mock meta → local-disk ObjectStorage →
   DataReader, under random writes, overwrites, ftruncate, and reopen.
   **Acceptance:**
   - DataWriter/DataReader are usable without a full vfs.VFS, with a
     documented attr/length/invalidation contract (Open length, Flush vs
     next Read, GetAttr trusts writer length).
   - Small-I/O: 1KiB overwrite × N + fsync + reopen same inode and read
     back (sqlite-shaped, not only large random writes).
   - The same harness with **injected 20–50ms Meta RPC delay**. If
     sqlite-shaped `--wait all` (two writers, ~1s remaining after a 5s
     sleep) fails here, stop and change the RPC shape before Phase 1.

### Phase 1 — server (~2 weeks)

4. `POST /v1/data-credential` with tenant-prefix scoping + refresh contract.
5. Meta HTTP API (§4.5) on tenant TiDB: **Create/Unlink/Lookup/Rename in the
   same txn as the `file_nodes` projection**; schema alignment (`extent_ino`,
   node/edge); session sweep; open-unlinked sustained GC.
6. Compact task queue + CAS commit endpoint; server-side block GC rework.
   Tests: crash between JuiceFS insert and projection insert still rolls
   back; Unlink then Create of the same name yields a new `extent_ino`.

### Phase 2 — client (~2 weeks)

7. `drive9s3` ObjectStorage with credential auto-refresh.
8. Dat9FS rewiring: extent **Create/Unlink/Lookup → meta driver**; Open →
   DataReader/DataWriter; delete the bespoke writer/staging/read-plan/compact
   code and the prepare/commit endpoints; SSE → `Invalidate` wiring.
9. Compact executor in the mount process (lease-claim loop).

### Phase 3 — hardening (~2 weeks, overlapping rollout)

10. Re-run the existing e2e gates on the new path: `fuse-sqlite-correctness.sh`
    (the historical defect hotspot), `fuse-sqlite-commit-sequence.sh`, Orb
    `community.sqlite` under `FUSE_PROFILE=coding-agent-extent` (wal **and**
    delete-journal mptest), fio, pjdfstest subset, `fuse-crash-recovery-test.sh`,
    plus a new dual-mount coherence script. Confirm `ls`/`rm -r` see extent
    files via the projection.
11. Triage/delete the 96 extent unit tests; add reference-behavior tests for
    the meta driver and server txn handlers.
12. Perf baseline versus the old extent path and versus `single` upload path;
    publish numbers in the PR.

Total estimate: **5–7 weeks, one engineer**, with a hard go/no-go at the end of
Phase 0.

## 8. Acceptance criteria

- `coding-agent-extent` profile passes the full FUSE e2e suite on the JuiceFS
  data plane **and** JuiceFS meta name ops, including sqlite WAL **and**
  delete-journal gates (`mptest` wal-multiwrite **and** delete-multiwrite,
  crash recovery). `ls` and `rm -r` on a tree that contains extent files
  succeed via the projection.
- Extent Create/Unlink are a single meta RPC each; the server commits JuiceFS
  `node`/`edge` and `file_nodes` together. Unlink then Create of the same
  path allocates a new `extent_ino` (no 409-Stat of the leftover file).
  `CreateFileWithLayout` / per-path HTTP DELETE are gone from the extent
  path.
- No per-object presign and no drive9-server block proxy on the extent path:
  extent block I/O uses only S3 (STS or local static keys) or the filesystem
  mock; `prepare-blocks`/`commit-slices`/`read-plan` are gone from the server
  router.
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
- Discussion: same-txn `file_nodes` projection vs Pattern B; journal
  Create/Unlink must follow JuiceFS `Meta.Create`/`Meta.Unlink`, not
  Dat9FS HTTP.
