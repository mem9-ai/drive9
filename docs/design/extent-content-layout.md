---
title: Extent Content Layout - JuiceFS Data Plane and Meta Engine inside drive9
status: design
date: 2026-09-09
---

# 1. Overview

## 1.1 Three content layouts

drive9 stores file content under one of three `content_layout` values. The layout is a property of
the file's inode, chosen when the file is created, and it determines both where the bytes live and
which write path the FUSE client uses.

| Layout | Bytes live in | Write model | Optimized for |
|---|---|---|---|
| `single` | one immutable object per revision (S3 / db9 inline) | whole-object upload, or same-size in-place PATCH; every write publishes a new revision | large sequential files, immutable artifacts, cheap reads |
| `append_log` | one object, appended server-side | tail-append via the server-owned append-log endpoint; falls back to a full rewrite when the append cannot be proven safe | WAL-style append-only files (e.g. SQLite `-wal`) |
| `extent` | immutable blocks under a tenant prefix in S3 | JuiceFS chunk/slice model: appends a slice, overwrites shadow a range, compaction merges slices, GC reclaims blocks | complex write workloads: random overwrite, small random I/O, mmap, repeated fsync, SQLite databases and journals |

`extent` is the layout this document describes.

## 1.2 Why a JuiceFS-shaped layout

The classic `single` layout is revision-based: a write produces a new immutable object and a new
`revision`, and readers key on that revision. That is ideal for large sequential writes and for
immutable artifacts, but it is a poor fit for workloads whose write pattern is *small and repeated
in the middle of a large file*:

- every commit re-uploads (or patches) a whole object and bumps a revision, so a 1 KiB SQLite WAL
  commit on a 1 GiB database pays for the whole file, and every revision invalidates client caches;
- in-place PATCH requires a same-size overwrite and a storage class that supports it, and it still
  serializes on the file;
- random 4 KiB overwrite inside an mmap'ed region cannot be expressed as "one new object".

The `extent` layout adopts the JuiceFS storage model instead:

- a file is an ordered list of **slices**; each slice is a range inside an immutable **block**
  object (4 MiB blocks, 64 MiB chunks, 24-byte slice records);
- a write appends a slice that covers the written range; a later slice shadows an earlier one, so an
  overwrite is a small append rather than a whole-object rewrite;
- blocks are addressed by slice id and reference-counted, so superseded data is reclaimed rather
  than duplicated;
- asynchronous **compaction** merges long slice lists into fewer, larger slices; server-side **GC**
  deletes blocks whose reference count reached zero.

Metadata (inode attributes, directory entries, and the chunk -> slice mapping) lives in TiDB, blocks
live in S3. That split is what makes `extent` usable for SQLite, journaled databases, and any
workload that overwrites in place: the write path touches metadata (a few rows in one transaction)
plus the block uploads for the written range, not a whole file.

## 1.3 Compatibility contract

An extent file is an ordinary drive9 file. `ls`, `rm`, `rm -r`, `mv` (file and directory), search,
tags, descriptions, the HTTP API, and the FUSE tree all keep working through the `file_nodes`
projection (§6). The only differences a user observes are:

- where the bytes are stored, and therefore which write path is used;
- that layout is decided at create time by a mount profile or the `--extent` flag (§2);
- that layout is sticky: renaming a file never converts it, and a name that matches an extent
  pattern does not retroactively convert an existing file (§2.6).

## 1.4 Non-goals

- Replacing the drive9 namespace: `file_nodes` remains the tree that `ls`, search, and the HTTP API
  read. JuiceFS meta is the source of truth only for extent names/inodes/lengths and slice lists.
- Importing the JuiceFS FUSE frontend: `Dat9FS` stays the kernel frontend (§4).
- Client-side encryption of extent blocks. Compaction is normally executed by the client that holds
  the data, but the design also executes it on the server as a fallback when no mount is doing so
  (§7.2), and that path reads block bytes through the server's own object-storage client. Client-side
  encryption would therefore require either dropping the fallback compactor or handing the keys to
  the server, which defeats its purpose. GC is not the constraint: it only deletes object keys and
  never reads block content.
- Layers/fork copy-on-write of extent inodes: layer whiteouts apply to the projection only.

---

# 2. User experience

## 2.1 How a file becomes extent

Layout is chosen by the FUSE client at **create** time, by matching the new file's **basename**
against the mount's extent patterns:

| Pattern form | Matches |
|---|---|
| `*` | every regular file |
| `*.db` | any basename ending in `.db` (case-insensitive) |
| `foo*` | `path.Match` on the basename |
| `snapshot.db` | the exact basename (case-insensitive) |

Directories are never extent objects. Patterns are matched against the **basename only**: a
directory component is not used, so `sub/*.db` behaves like `*.db` and `sub/x.db` matches nothing.
The server never infers a layout from the path; the client decides and the server records the
decision in the same transaction that creates the inode (§6.2).

## 2.2 Built-in profiles

`drive9 mount --profile <name>` selects a built-in profile or a profile file (§2.3).

| Profile | Contents | Use |
|---|---|---|
| `coding-agent` (default) | local-only overlay patterns (`.git`, `node_modules`, `dist`, caches, …) + `[append-log] **/*-wal` | agent workspaces on a normal drive9 tree |
| `coding-agent-extent` | `coding-agent` **plus** `[extent] *` | the SQLite/product gate: every file on the mount is `extent` |
| `extent` | `[extent] *` only | a pure extent volume, no overlay rules |
| `portable` | local-only patterns + `[pack] /` | pack/unpack archives |
| `none` | no rules | raw tree, no overlay, no policy |
| `interactive` | reserved | interactive shells |

`coding-agent-extent` exists so an agent workspace can keep the coding-agent overlay semantics while
storing *all* file content through the extent data plane. `extent` is the same data plane without the
overlay rules, for mounts that should behave like a plain shared filesystem.

## 2.3 Custom profiles

A profile is a small INI-like file under `~/.drive9/profiles/<name>` (the name is what `--profile`
takes). Sections are `[local]`, `[remote]`, `[pack]`, `[append-log]`, and `[extent]`; each line in a
section is one pattern.

```ini
# ~/.drive9/profiles/sqlite-extent
[local]
**/.git/**
**/node_modules/**

[extent]
*.db
*.db-wal
*.db-journal
*.db-shm
```

```bash
drive9 profile show sqlite-extent   # print the resolved profile without contacting the server
drive9 mount --profile sqlite-extent :/workspace ./mnt
```

Only the matching basenames become extent files; `notes.md` or `Makefile` in the same directory stay
`single`. This is the recommended shape for mixed trees: databases and their journals are extent,
everything else keeps the classic write path.

## 2.4 The `--extent` flag

Patterns can also be supplied per mount, without a profile file:

```bash
drive9 mount --extent '*.db' --extent '*.db-wal' :/workspace ./mnt
DRIVE9_MOUNT_EXTENT_PATTERNS=$'*.db\n*.db-wal' drive9 mount :/workspace ./mnt
```

- `--extent PATTERN` is repeatable; `DRIVE9_MOUNT_EXTENT_PATTERNS` is newline-delimited.
- Rules are combined in **profile, then environment, then flag** order and deduplicated.
- Invalid patterns are configuration errors; `--extent` is rejected for object-store mounts and for
  non-FUSE modes.
- The resolved rules are snapshotted into the supervised worker's argv, so restarts and `mount
  ensure` keep the original contract.

`--extent` is the direct analogue of `--append-log`; the difference is that extent needs no server
capability probe, because the extent data plane is available on every deployment, while append-log
depends on an object store that supports append.

## 2.5 Observability

- `drive9 fs stat`, `ls -l`, and any in-mount `stat` behave normally. Inside the mount, extent files
  are ordinary files: `cat`, `dd`, `sqlite3`, `mmap` all work.
- The stat contract exposes the layout to clients and operators:
  `X-Dat9-Content-Layout: extent` and `X-Dat9-Extent-Ino: <juicefs inode>` are returned for extent
  files; the FUSE client uses them to route reads/writes and to detect layout changes.
- `curl -sI -H "Authorization: Bearer $DRIVE9_API_KEY" "$DRIVE9_BASE/v1/fs/path/to/app.db"` is the
  quickest operator check of a file's layout.
- Server-side, `file_nodes.content_layout` / `file_nodes.extent_ino` are the projection columns
  (§6.2); the JuiceFS tables carry the authoritative inode, edge, and chunk rows (§6.1).

## 2.6 Lifecycle rules

| Operation | Result for an extent file |
|---|---|
| create matching a pattern | created as `content_layout=extent`, `extent_ino` allocated in the same transaction |
| rename within the glob | stays `extent`, same `extent_ino`; one transaction moves the edge and the projection row |
| rename out of the glob | stays `extent`; layout is an inode property, not a path property |
| rename a `single` file into the glob | stays `single`; globs are a create-time rule |
| unlink | edge removed, `nlink--`; open-unlinked files are released when the last handle closes; blocks are reclaimed through GC (§7.3) |
| unlink then create the same path | the new file gets a **new** `extent_ino`; it never reuses the leftover inode |
| hardlink | both names share one `extent_ino`; the second edge is created in the same transaction as the projection row |
| truncate | appends a zero slice (JuiceFS semantics); space is reclaimed later by compaction |
| directory containing extent children | `ls` lists it through the projection; rename/rmdir/`rm -r` handle the JuiceFS subtree in the same transaction (§6.5-§6.7) |
| remount | nothing to replay: layout, inode, and slice list are all in TiDB |

## 2.7 CLI cheat sheet

```bash
# mount an all-extent volume
drive9 mount --profile extent :/data ./mnt

# mixed tree: only databases are extent
drive9 mount --extent '*.db' --extent '*.db-*' :/workspace ./mnt

# inspect the resolved profile
drive9 profile show coding-agent-extent

# operate on files exactly as before
drive9 fs ls /workspace
drive9 fs cp app.db :/workspace/app.db
drive9 fs mv :/workspace/app.db :/workspace/app-v2.db
drive9 fs rm -r :/workspace/scratch
```

---

# 3. A custom `pkg/meta` engine in a JuiceFS fork

## 3.1 Why a fork is required

`meta.Meta` cannot be implemented from outside the JuiceFS module: the interface contains
unexported methods (`getBase() *baseMeta`, `chroot(Ino)`). A custom meta driver therefore has to live
inside `pkg/meta`. The fork (`mem9-ai/juicefs`, module path kept as `github.com/juicedata/juicefs`
via a `replace` directive in `go.mod`) adds exactly two additive files inside `pkg/meta`:

- `pkg/meta/drive9.go` - the driver glue: `NewDrive9Meta(conf, transport)`, the `Drive9Transport`
  interface, the op-name constants, the path/opened context keys, and the inode writer.
- `pkg/meta/drive9_engine.go` - the `engine` method set, each one a single HTTP RPC.

Everything else (baseMeta, VFS, chunk store, object backends) stays upstream, which is the point:
slice bookkeeping, CAS, and read consistency are upstream code rather than a reimplementation.

## 3.2 Driver shape

```go
type Drive9Transport interface {
    Call(ctx Context, op string, req, resp any) syscall.Errno
}
```

The driver is stateless. `baseMeta` keeps doing what it always does - in-memory attr/entry caches,
chunk caching, slice-list parsing, compaction decisions, session heartbeat - and calls into the
engine for anything persistent. Each engine method marshals a small request and issues one RPC; the
drive9-server handler executes it inside one tenant TiDB transaction.

Op set:

| Group | Ops |
|---|---|
| Format / counters | `load`, `init`, `get_counter`, `incr_counter`, `set_if_small` |
| Session | `new_session`, `refresh_session`, `get_session`, `find_stale_sessions`, `clean_stale_session` |
| Namespace | `lookup`, `getattr`, `setattr`, `mknod`, `unlink`, `rmdir`, `rename`, `readdir` |
| Data | `read`, `write`, `truncate`, `compact`, `delete_slice`, `delete_sustained` |
| Locks | `flock`, `getlk`, `setlk` |
| drive9 extensions | `claim_compact`, `requeue_compact` (pull-model compaction, §7.2) |

Two context values carry drive9 semantics across the interface without changing JuiceFS:

- `Drive9PathKey` - the drive9 projection path for `Create`/`Unlink`/`Mkdir`/`Rename`, so the server
  can dual-write the projection in the same transaction (§6.2);
- `Drive9OpenedKey` - set by the FUSE frontend when the unlinking client still holds the file open,
  so the server records a sustained row instead of dropping the inode.

## 3.3 What is deliberately disabled

`pkg/extent/runtime.go` configures the driver with:

- `NoBGJob = true` - no per-client `cleanupDeletedFiles`, `cleanupSlices`, `cleanupTrash`, or
  symlink scans. Those jobs exist to let *some* client do the work; in drive9 they run on the server
  (§7.4), so N mounts do not become N background scanners.
- `MaxDeletes = 0` - the client does not run delete-slice workers; slice deletion is a server-side
  transaction followed by server-side object deletion.
- `OnMsg(DeleteSlice)` is a no-op, and `OnMsg(CompactChunk)` uploads the merged blob through the
  local chunk store (`vfs.Compact`) before the meta CAS commits it (§7.2).
- `TrashDays = 0` - no recycle bin; unlink reclaims through GC directly.

## 3.4 Dependency and maintenance cost

- Build tags (`nogspt,noredis,nosqlite,nomysql,nopg,notikv,nobadger,noetcd,…`) trim the compile
  surface to the S3 object backend and no built-in meta engines; the `replace` keeps the import path
  stable so upstream imports keep compiling.
- Apache-2.0 with NOTICE preserved; the fork's delta is two additive files in `pkg/meta`.
- Upstream is pinned by pseudo-version and rebased periodically, the same workflow as the existing
  `go-fuse` fork.

---

# 4. Grafting the drive9 FUSE frontend onto JuiceFS VFS

## 4.1 Component layout

```
kernel FUSE
   │
   ▼
Dat9FS (drive9 frontend: namespace, dir cache, write-back, layers, git workspaces)
   │
   ├─ layout routing: profile/--extent glob at create, ContentLayout from stat afterwards
   │
   ├─ single / append_log / directories → existing drive9 paths (unchanged)
   │
   └─ extent files
        ├─ name ops  → meta.Meta "drive9" driver  → drive9-server /v1/extent/meta (one RPC)
        └─ data ops  → vfs.DataReader / vfs.DataWriter → chunk.ChunkStore → S3
                                                          ▲
                                                   object storage (STS-scoped)
```

`Dat9FS` remains the only kernel frontend. It owns path resolution, inode/entry caching, the
directory cache, write-back staging, layers, git workspaces, and SSE-driven invalidation. JuiceFS is
used as a library, never as a second FUSE mount.

## 4.2 Runtime construction

`extent.NewRuntime` builds one runtime per process (lazily, on the first extent operation):

1. `meta.NewDrive9Meta(conf, transport)` with the config from §3.3, then `Init`/`Load`/`NewSession`;
2. a `chunk.ChunkStore` over the credential-scoped object store (§8), with the write-back cache
   directory (when configured) under `<cache-dir>/jfs`;
3. `vfs.NewDataReader` / `vfs.NewDataWriter` over that meta and store;
4. a `vfs.VFS` built with an isolated `_FUSE_STATE_PATH`, so a stale `/tmp/state<pid>.json` from an
   unrelated mount can never be adopted;
5. `registerJuiceMetaMsg`, which wires JuiceFS's `CompactChunk` message to a local `vfs.Compact`
   upload and neutralizes `DeleteSlice`.

`vfs.Config` keeps `AttrTimeout`/`EntryTimeout`/`DirEntryTimeout` at one second and enables
writeback, matching the JuiceFS FUSE defaults that the perf tests were tuned against.

## 4.3 Operation dispatch

| Kernel operation | drive9 handling for an extent file |
|---|---|
| `Create` / `Mknod` / `Symlink` | profile glob decides extent; `v.Create`/`Mknod`/`Symlink` issues one RPC that also writes the projection; the handle records `extentIno` |
| `Lookup` / `GetAttr` | projection stat first; layout comes from `ContentLayout`; extent attr/length refreshed from `v.GetAttr` and `v.UpdateLength` |
| `Open` | `v.Open` for a data handle; `extentFh` stored on the drive9 file handle |
| `Read` | `v.Read` through `DataReader`; kernel page cache policy stays drive9's |
| `Write` | `v.Write` through `DataWriter`; O_APPEND offset fixed up from the committed length; kernel writeback (`WRITE_CACHE`) falls back to `v.WriteBack` |
| `Flush` / `Fsync` | `v.Flush` / `v.Fsync`; the write-back cache directory makes fsync a local stage, with uploads continuing afterwards |
| `Truncate` / `SetAttr` | `v.SetAttr(SetAttrSize/…)`; length, mode, uid/gid, times go to the JuiceFS inode |
| `Unlink` | `v.Unlink` (edge + `nlink--` + projection in one txn); open handles are marked unlinked and, when the last one closes, the inode is released |
| `Rename` (file) | `v.Rename` in one transaction (edge move + projection move) |
| `Rename` (dir) | drive9 subtree update plus one JuiceFS edge move in the same transaction (§6.5) |
| `Rmdir` / `rm -r` | drive9 emptiness/recursion rules, with the JuiceFS directory node/edges maintained in the same transaction (§6.6-§6.7) |
| `Readdir` | drive9 lists the projection, then overlays the JuiceFS length for each entry by name |
| `Flock` / `Setlk` / `Getlk` | JuiceFS meta locks on the extent inode (SQLite needs them on the inode, not on a path) |
| `Release` | releases the VFS handle; unlinked-but-open files are released only after the last close |

## 4.4 Length and attribute contract

JuiceFS distinguishes the *meta* length (`GetAttr`) from the *writer* length (dirty data not yet
flushed). The graft keeps both correct:

- on every `GetAttr`, `Readdir`, and `Lookup`, the returned attr is fed back with `UpdateLength`, so
  the data reader and the kernel see the committed length;
- `extentApplyWriterLength` lets an in-memory writer length win over a stale attr, but never
  synthesizes a zero length (a synthetic zero would truncate the reader and turn a flushed file into
  EOF);
- the projection's `size_bytes`/`mtime` are updated in the same write/truncate transaction as the
  JuiceFS inode (§6.2), so a stat that never touches JuiceFS still reports the right size;
- hardlink aliases keep `nlink` from the drive9 inode; JuiceFS attr refreshes never clobber it.

## 4.5 Caching policy

- Kernel attr/entry TTL for extent files is one second (`juiceFSAttrTimeout`), matching JuiceFS's
  `--attr-cache`/`--entry-cache` default; hardlinked or multi-path inodes get TTL 0 so a rename
  cannot leave a stale alias.
- drive9's own dir cache, negative cache, read cache, and disk cache are unchanged; remote SSE
  events still invalidate kernel state.
- The chunk store's slice cache is invalidated by JuiceFS on write. Cross-mount coherence relies on
  JuiceFS's close-to-open slice refresh plus the drive9 SSE path notifying the kernel inode; there is
  deliberately no parallel drive9-side extent reader cache.
- With a write-back cache directory configured, dirty blocks stage locally and are uploaded by the
  chunk store; leftover staging is replayed on the next mount of the same cache directory.

---

# 5. Remote meta store: keeping the RPC count flat

JuiceFS assumes the metadata engine is next to the client (local SQL, Redis, or TiKV on the same
network): its design tolerates a meta round trip per operation because that round trip is cheap.
drive9's meta engine is TiDB behind an HTTP API, so every unnecessary call is a real latency and
load cost. The graft is therefore built around one rule: **the client must not make a meta call it
could have derived, cached, or batched.**

| Mechanism | What it avoids |
|---|---|
| One RPC per name operation, with the projection written in the same transaction | the old "create via HTTP, then create in JuiceFS" two-call pattern and its conflict window |
| Parent inode taken from the kernel's parent (`input.NodeId`) instead of walking the path | a Lookup per path component on every create/unlink/rename |
| Lazily mirrored parent directories with an in-process `path -> juicefs ino` binding | repeated parent lookups, and the name-based re-mirroring that a remount would otherwise need |
| Projection-driven `ls`/`readdir`: the drive9 projection is the entry source, and the JuiceFS tree is consulted only to overlay lengths | listing correctness depending on the JuiceFS directory tree, and a second namespace to reconcile |
| `open-cache` (1s, 10k entries) and one-second attr/entry TTLs | repeated `GetAttr` on the same inode during a burst of syscalls |
| Chunk slice-list caching in the chunk store, invalidated on write | a meta `Read` per 4 KiB read |
| Batched `NewSlice` (4096 ids per call) | one counter RPC per slice |
| Thin `Write`: one transaction that appends the slice, updates length/mtime, and updates the projection; slice count read back with `LENGTH()` on the CONCAT path | an extra `SELECT` on the write hot path (and holding `FOR UPDATE` across it) |
| Compaction scheduled from a leased queue with a fat-chunk discovery query that is bounded to `<= 4 x maxSlices` | every client scanning `jfs_chunk` on a timer, and runaway blobs saturating TiDB |
| Compaction thresholds on the write path (see §7.2) | letting live chunks grow to thousands of slices, which would make every later write slow |
| Write-back staging in the chunk store | a meta call (or an upload) on every small write |
| Background GC/compaction/session sweep on the server | N mounts each running the same background jobs |
| Denormalized projection columns (`content_layout`, `extent_ino`, size, mtime) | `ls`, `stat`, and search having to join the JuiceFS tables |

The projection is not only a compatibility device; it is also a cache. Reads that only need
namespace metadata (`ls`, `stat`, `find`, search) never touch the JuiceFS tables, and the JuiceFS
tables are only read when an extent file's content is actually accessed or its directory is modified.

---

# 6. Schema: JuiceFS tables inside the drive9 tenant schema

## 6.1 Tables

The tenant schema (created by the tenant provisioner and exported by
`drive9-server schema dump-init-sql --provider …`) contains the JuiceFS tables alongside the drive9
tables:

| Table | Role |
|---|---|
| `jfs_node` | inode: type, mode, uid/gid, times, nlink, length, parent |
| `jfs_edge` | directory entry: `(parent, name) -> inode` |
| `jfs_chunk` | per `(inode, chunk)` append-only slice blob |
| `jfs_chunk_ref` | per-slice block reference count (feeds GC) |
| `jfs_symlink`, `jfs_flock`, `jfs_plock`, `jfs_sustained`, `jfs_session2` | symlink targets, locks, open-unlinked files, sessions |
| `jfs_delfile`, `jfs_delslices` | file deletion queue; upstream's delayed-slice queue (unused here, since `TrashDays = 0`) |
| `jfs_counter`, `jfs_setting` | inode/slice counters and the persisted format |
| `jfs_dir_stats` | directory stats |
| `slice_compact_tasks`, `block_gc_tasks` | drive9-side work queues (§7) |

Plus two projection columns on the drive9 side: `file_nodes.content_layout` and
`file_nodes.extent_ino`.

## 6.2 The projection invariant

Every namespace mutation is **one RPC, one transaction** that writes both trees:

| Operation | JuiceFS side | drive9 side |
|---|---|---|
| create file | `node` + `edge(parent, name)` | `file_nodes(path, content_layout=extent, extent_ino)` + `inodes`/`contents` rows |
| unlink file | delete `edge`, `nlink--` (or sustained if open) | delete the `file_nodes` row |
| rename file | move `edge`, update `node.parent` | move the `file_nodes` row |
| mkdir | `node` + `edge` (lazy mirror or full mirror) | the directory's `file_nodes` row gets `extent_ino` |
| rmdir / rm -r | delete the directory `edge`/`node`, and every extent child edge/node | delete the `file_nodes` rows |
| hardlink | new `edge`, `nlink++` | new `file_nodes` row with the same `extent_ino` |

Because the two writes share a transaction, a crash cannot leave a file visible in `ls` but unknown
to JuiceFS (or the reverse): either both rows exist or neither does. The FUSE client never
dual-writes; it issues one RPC and the server does the rest.

## 6.3 Parent directories: lazy mirror and full mirror

JuiceFS `Create(parent, name)` needs a parent directory inode. Directories are not extent objects;
they are mirrored into JuiceFS only when needed:

- **lazy mirror** (mixed profile such as `*.db`): the first extent create under `/a/b/x.db` ensures
  JuiceFS dir inodes for `/` and `/a/`/`/a/b/`, then creates the file. The mirror is cached in
  process (`path -> ino`) and the directory's `extent_ino` is written back to its `file_nodes` row;
- **full mirror** (`coding-agent-extent` / `extent`, where every file matches): the JuiceFS directory
  tree is 1:1 with the drive9 directory tree.

`ls` always reads the drive9 projection; the JuiceFS directory tree exists so that name operations
have a parent to attach to.

## 6.4 Create, unlink, rename (files)

- **Create**: allocate `extent_ino`, insert `node` + `edge`, insert the projection, commit. The
  client then opens a `DataWriter` on that inode. A failed create rolls back the whole transaction,
  including the projection.
- **Unlink**: delete the `edge`, decrement `nlink`, delete the projection. If `nlink > 0` or a
  session still holds the inode open, the `node` stays (open-unlinked) and is released when the last
  handle closes; the projection is already gone, so `ls` is clean immediately.
- **Unlink then create the same path**: the new file allocates a *new* `extent_ino`. This is the
  sequence journaled databases perform constantly, and it must not resurrect or reuse the leftover
  inode.
- **Rename**: move the `edge` and update the projection in one transaction. Renaming an extent file
  out of its glob does not change its layout (§2.6).

## 6.5 Rename a directory

A directory rename touches two trees at once, in one transaction:

1. the drive9 subtree update (`file_nodes.path/parent_path/name` for the directory and every
   descendant) - this is what `ls` reads;
2. a single JuiceFS edge move for the directory itself. Children are keyed by *parent inode*, not by
   path, so they do not need to be rewritten.

Because the directory's `extent_ino` is stored on its projection row (§6.2), the JuiceFS edge can be
moved by inode rather than by re-walking the path, and a rename performed through any channel (FUSE,
CLI, HTTP, SDK) keeps the two trees in step.

## 6.6 Remove an empty directory

`rmdir` deletes the directory's `file_nodes` row and its JuiceFS `edge`/`node` in the same
transaction. The JuiceFS side is what prevents a later `mkdir` of the same name from reusing a stale
directory inode.

## 6.7 Recursive remove

`rm -r` (FUSE or `drive9 fs rm -r`) walks the projection and, for every `content_layout=extent`
descendant, performs the same single-transaction unlink as §6.4: delete the edge, decrement `nlink`,
delete the projection, and enqueue the file's blocks for GC. Directory edges/nodes are removed as the
walk unwinds. The result is that after `rm -r`:

- `ls` of the removed path fails;
- the JuiceFS tree has no leftover edges for the subtree (so re-creating the same names works, and a
  later `rmdir` of a re-created empty directory succeeds);
- every block that belonged to a removed extent file is on the GC queue.

## 6.8 Other namespace operations

- **Symlink / mknod / special files** follow the same pattern: a JuiceFS node/edge plus the drive9
  projection row, with `content_layout=extent` when the profile matches.
- **Hardlink**: the second edge is created and `nlink` incremented in the same transaction that
  inserts the second `file_nodes` row, so both names share one `extent_ino`.
- **Layers**: layer whiteouts and layer-local files apply to the projection only; writing extent
  files *inside* a layer is out of scope (it needs copy-on-write of the extent inode).
- **Forks**: a fork is a new tenant - its own database and therefore its own JuiceFS tables and its
  own object prefix (§8).

## 6.9 Schema lifecycle

The JuiceFS tables are part of the tenant init schema, so a tenant is fully initialized by the same
`provision` path as before, and externally managed deployments stay in sync through the exported
`dump-init-sql` output. There is no separate JuiceFS "format" to create: the driver persists the
JuiceFS `Format` record in `jfs_setting` on first use.

---

# 7. Compaction and GC

## 7.1 JuiceFS semantics, briefly

- A write appends a slice to the chunk's slice list. Slice lists are append-only; a new slice shadows
  the range it covers.
- Slice lists must not grow without bound, so JuiceFS **compacts** a chunk: it reads the slices,
  merges the non-overlapping tail into one new slice, uploads the merged blob, and CAS-commits a new
  slice list that replaces the merged range.
- Every slice has a reference count (`jfs_chunk_ref`). When a slice is superseded, its count drops;
  at zero the block objects are unreferenced and can be deleted.
- Upstream runs compaction from the client that is writing/reading, and runs the deletion queues in
  per-client background jobs.

## 7.2 Compaction in drive9

**Triggers.** The write/read triggers run in the client, because only the client has the chunk cache;
the pull queue can be executed by any mount or by the server worker.

| Trigger | Condition | Behaviour |
|---|---|---|
| write path | `numSlices % 100 == 99` or `numSlices > 350` | async `compactChunk` when the chunk is below `maxSlices` (2500); **synchronous** compact when it is at or above 2500, so a hot chunk cannot grow past the limit |
| read path | a chunk with ≥ 5 slices is read and not already being compacted | async `compactChunk` |
| pull queue (client) | every mount runs a claim loop (2s idle) | `claim_compact` → execute → CAS; requeue on failure |
| pull queue (server) | the tenant worker, as a fallback when no mount is executing compaction | claims one task and executes it with server-side credentials |

The pull queue is **self-feeding**: a claim first takes a pending or expired-lease row; if there is
none, it looks for a chunk whose slice blob is at least `jfsCompactThreshold` slices' worth of bytes
and at most four times that, and leases it. The bound matters: an unbounded `LENGTH(slices)` scan can
find a runaway blob and saturate TiDB, so the query is indexed and capped.

**Execution**: read the chunk's slices (one RPC), merge (`skipSome` + overlay), allocate a slice id,
upload the merged blob with the local chunk store (`vfs.Compact`), then CAS-commit through the
`compact` op. The server compares the caller's `origin` slice list with the current one inside one
`FOR UPDATE` transaction, writes the new slice list, decrements the superseded slices' reference
counts, and marks the task complete. A loser of the CAS (another client compacted the same chunk
first) releases the blob it uploaded back to the GC queue, so a lost race costs one upload, not a
leak. Merged chunks are capped at `maxCompactSlices` (1000) per compaction, like upstream.

## 7.3 GC in drive9

| Step | Where | Detail |
|---|---|---|
| record deletion | meta transaction | unlink / sustained release moves the file's chunk rows into the deletion queue (`jfs_delfile`) |
| drain the queue | server worker | walk the file's chunk rows, decrement slice reference counts, and enqueue the blocks of slices whose count reached zero |
| enqueue blocks | server worker | one `block_gc_tasks` row per block object key, with a grace `available_at` |
| delete objects | server worker | `DeleteObject` under the tenant prefix, then mark the row complete; failures retry up to `max_attempts` and are logged |
| superseded slices | compaction | a successful compact enqueues the slices it replaced (count already zero) |
| stale sessions | server worker | expired sessions release their open-unlinked files and locks, which feeds the same queue |

The grace period (`available_at`) protects readers on other mounts and readers with cached slice
lists: a block is only deletable after the grace window, not the moment its reference count reaches
zero. Every queue row carries attempt counters, so a permanently failing object is visible instead of
being retried forever.

## 7.4 Differences from upstream JuiceFS

| Aspect | Upstream JuiceFS | drive9 |
|---|---|---|
| Who runs compaction | the writing/reading client (per-operation triggers) | the same client triggers, plus a leased pull queue executed by any mount **or** by the server worker as a fallback |
| Background jobs | each client runs `cleanupDeletedFiles` / `cleanupSlices` / trash | disabled in the client (`NoBGJob`); the server runs GC, the deletion drain, the session sweep, and the compaction fallback |
| Delete-slice workers | per-client goroutines (`MaxDeletes`) | disabled; deletion is a server transaction plus server-side object deletes |
| Recycle bin | `TrashDays` (default non-zero) | `TrashDays = 0`: unlink reclaims directly, no delayed slices |
| Compaction lease | in-process map only; other clients race on CAS | a real lease row (`slice_compact_tasks`) with a 5-minute lease, requeue, and attempt limits |
| GC with zero clients | does not progress | progresses: the server worker needs no mount |
| Block key namespace | volume prefix | `[serverPrefix]t/<tenant>/chunks/<id/1e6>/<id/1e3>/<id>_<block>_<len>` - tenant-scoped so credentials can be prefix-scoped (§8) |
| Meta latency assumption | local/Redis/TiKV | TiDB behind HTTP; §5 is the mitigation list |

---

# 8. Object storage credentials

## 8.1 The problem with per-object presign

The `single` layout hands the client a presigned URL per object (or per part), minted by the server
for one specific key with a short TTL. That works because a single-layout write is one object with a
known key, but it does not fit the extent data plane:

- a write produces many small block objects at keys derived from slice ids the server has not seen
  yet, so the client would need a presign per block (or a prepare/commit protocol);
- compaction reads a chunk and writes a new blob at a fresh key, again per chunk;
- presigns expire, so long-lived mounts need re-minting and the old `pending_blocks` machinery to
  avoid orphaned objects.

## 8.2 Per-filesystem prefix STS

The extent data plane instead issues a **tenant-prefix-scoped, medium-lived S3 session**:

```
POST /v1/data-credential          (tenant API key; scoped tokens rejected)
  → AssumeRole(RoleArn = DRIVE9_S3_ROLE_ARN,
               RoleSessionName = "drive9-extent-<tenant>",
               DurationSeconds = 3600,
               Policy = inline session policy)
```

The inline session policy is what makes the grant safe:

```json
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow",
  "Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],
  "Resource":"arn:aws:s3:::<bucket>/<prefix>*"},
 {"Effect":"Allow",
  "Action":["s3:ListBucket"],
  "Resource":"arn:aws:s3:::<bucket>",
  "Condition":{"StringLike":{"s3:prefix":["<prefix>*"]}}}]}
```

where `<prefix>` is the server's configured bucket prefix plus `t/<tenantID>/`. A tenant is one
drive9 filesystem (a fork is a new tenant), so the grant is per-filesystem: a mount can read and
write its own blocks and nothing else, even though all tenants share one bucket. The policy also
grants the multipart actions the S3 backend may need (`AbortMultipartUpload`,
`ListMultipartUploadParts`, `ListBucketMultipartUploads`); today's block uploads are single `PUT`s.

Fallbacks, for deployments without STS:

- **static-key S3-compatible stores** (local MinIO): the server hands out its own static keys with
  the same tenant prefix. The prefix is then a client-side convention, not an IAM guarantee - an
  explicit tradeoff documented for local development;
- **filesystem mock** (unit tests, no object store): a `file` scheme pointing at the mock's directory.

## 8.3 Client side

`extent.OpenStorage` turns the credential into a JuiceFS object backend:

1. build an `s3` backend from `endpoint`/`bucket`/session credentials (JuiceFS's S3 backend takes
   static credentials, which is why the session is minted by the server rather than assumed by the
   client);
2. wrap it with the tenant prefix, so every key the chunk store generates lands under
   `[serverPrefix]t/<tenant>/chunks/…`;
3. wrap that in a refreshing store: on every read/write/delete/head, if the credential is within two
   minutes of expiry it re-mints (`POST /v1/data-credential`) and rebuilds the backend. Refresh is
   lazy and per-operation, so an idle mount makes no credential calls, and a mount that is actively
   writing simply continues when the session rotates.

Blocks are never proxied through drive9-server: the client talks to S3 directly. The server keeps its
own credentials for the work it performs itself (GC object deletes, the compaction fallback).

## 8.4 What this replaces

The per-object presign path (`PresignPutObject` per block, `PresignGetObjectRange` per read window,
`prepare-blocks` / `pending_blocks` / `commit-slices`) is gone from the extent path. JuiceFS's
semantics are "data lands in object storage first, then meta records the slice", so there is no
per-block authorization step to perform; blocks that meta records as deleted or superseded are
reclaimed by the GC queues of §7.

---

# 9. Validation

The extent path is covered by the existing correctness suites plus extent-specific gates. All of the
following pass on the extent profile:

| Suite | What it covers |
|---|---|
| `community.pjdfstest` | POSIX filesystem call conformance (create/unlink/rename/link/truncate/mkdir/rmdir/chmod/…) against a FUSE mount; extent files pass at the same rate as `single` files |
| `community.ltp.fs` / `community.ltp.syscalls` | LTP filesystem scenario and the filesystem-sensitive syscall subset |
| `community.sqlite` | upstream SQLite `speedtest1`, `mptester` (WAL and delete-journal, multi-writer), `threadtest3`, `kvtest`, plus a `MAP_SHARED` mmap probe |
| `community.fio` / `community.fsx` | I/O patterns and randomized file-operation stress |
| `e2e/fuse-sqlite-correctness.sh`, `e2e/fuse-sqlite-commit-sequence.sh` | SQLite rollback-journal and 1000-commit WAL `synchronous=FULL` sequences with remount and remote-snapshot fingerprints |
| `e2e/fuse-crash-recovery-test.sh` | `kill -9` of the mount daemon: fsync'd files and staged uploads survive, unlinked files do not resurrect |
| `e2e/fuse-smoke-test.sh`, `e2e/fuse-concurrency-stress.sh`, `e2e/fuse-posix-fsx-gate.sh`, `e2e/posix-permission-smoke-test.sh` | mount lifecycle, concurrency, POSIX/fsx subset, permissions |
| `e2e/extent-rename-mixed-dir.sh` | layout stickiness across rename, mixed-directory `ls`/rename/`rm -r`, unlink+create inode identity, directory operations performed by another channel |

```bash
# local: TiDB + drive9-server (provider=local) + FUSE
FUSE_PROFILE=coding-agent-extent make e2e-local

# full extent sweep: the same suites with every optional workload enabled
FUSE_PROFILE=coding-agent-extent RUN_FUSE_ALL_WORKLOADS=1 \
  RUN_FUSE_SQLITE_COMMIT_SEQUENCE=1 RUN_POSIX_SMOKE=1 RUN_EXTENT_E2E=1 \
  make e2e-local

# blackbox SQLite gate under the extent profile
FUSE_PROFILE=coding-agent-extent python3 blackbox/run.py --module community.sqlite \
  --server-mode local --bin ./bin/drive9 --local-server ./bin/drive9-server

# extent-specific e2e
DRIVE9_BASE=http://127.0.0.1:9009 bash e2e/extent-rename-mixed-dir.sh
```

---

# 10. Appendix

## 10.1 Where the code lives

| Concern | Location |
|---|---|
| Profile/flag plumbing | `cmd/drive9/cli/profile.go`, `cmd/drive9/cli/mount.go` |
| FUSE routing and extent operations | `pkg/fuse/extent.go`, `pkg/fuse/extent_ops.go`, `pkg/fuse/extent_jfs.go` |
| Data-plane runtime | `pkg/extent/runtime.go`, `storage.go`, `compact.go`, `s3wrap.go` |
| Meta RPC client | `pkg/client/extent_meta.go` |
| Meta server handlers and tenant transactions | `pkg/server/extent_meta.go`, `pkg/datastore/jfs.go` |
| Tenant schema | `pkg/tenant/schema/extent.go` |
| Credentials | `pkg/server/extent_meta.go`, `pkg/s3client/aws.go` |
| JuiceFS fork | `pkg/meta/drive9.go`, `pkg/meta/drive9_engine.go` in the pinned fork |

## 10.2 Glossary

- **chunk** - 64 MiB range of a file; the unit of slice lists.
- **slice** - a range inside a block; the unit of writes and reference counting.
- **block** - 4 MiB immutable object; the unit of object-storage reads, writes, and GC.
- **extent_ino** - the JuiceFS inode that backs an extent file; recorded on `file_nodes`.
- **projection** - the drive9 `file_nodes` row for an extent file, written in the same transaction as
  the JuiceFS node/edge.
- **sustained** - a JuiceFS row that keeps an unlinked-but-open inode alive until the last close.
- **data credential** - the tenant-prefix-scoped S3 session from `POST /v1/data-credential`.
