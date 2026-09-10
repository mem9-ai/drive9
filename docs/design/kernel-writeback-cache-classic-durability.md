---
title: Kernel Writeback Cache and Classic-Path Durability - Zombie Handle Design
status: design
date: 2026-09-11
scope: classic drive9 write paths (single / append_log) under FUSE_WRITEBACK_CACHE; mixed classic+extent mounts
audience: engineer or agent implementing the fix
related: docs/design/extent-content-layout.md, docs/design/fuse-durability-policy.md, docs/extent-todos.md (P1-7, P1-9)
---

# 0. How to read this document

- Each change references functions and constants, not line numbers; line
  numbers drift. Locate by name first.
- **[verified]** means the claim was read line by line in this tree;
  **[assumed]** means it follows from kernel FUSE semantics that the extent
  suites already exercise; re-verify before relying on it in code.
- Read §1 (the kernel facts) and §2 (the contract matrix) before touching
  code. The entire design is a consequence of those two sections.

---

# 1. Problem and kernel facts

## 1.1 What changed

`feat(fuse): enable the kernel writeback cache by durability` negotiates
`FUSE_WRITEBACK_CACHE` mount-wide for every durability policy except
`write-sync` (auto), with `--writeback-cache on|off` as the override
(`kernelWritebackCacheEnabled` in `pkg/fuse/mount.go`). The extent data plane
depends on it: without the kernel cache every 4 KiB `write(2)` is its own FUSE
WRITE and the extent write path stalls under concurrent small writes
(`docs/extent-todos.md` P1-9: `community.sqlite` mptest/threadtest3).

The cap is mount-wide, so it also changes the write pattern seen by the
**classic** (`single` / `append_log`) paths.

## 1.2 The two kernel facts the design rests on

**(a) Write-ordering around sync points** [assumed, empirically confirmed by
the extent suites]: with `FUSE_WRITEBACK_CACHE`, every dirty page produced by
`write(2)` is written back to the daemon (as a `FUSE_WRITE` with
`WRITE_CACHE`) before the kernel completes the FSYNC, the FLUSH, and the final
RELEASE of the file. Consequences that already hold today:

- close-sync and fsync policies need **no** commit-path change: the daemon's
  Flush/Fsync handlers see all `write()` data because it arrived before the
  sync point;
- P1-7's observation ("the file became remotely visible once the writer
  closed the fd") is the same fact from the extent side.

**(b) The post-Release window** [verified]: dirty pages produced through
`mmap(MAP_SHARED)` (SQLite `-shm`, `*.db` with `mmap_size>0`, and any plain
buffered mmap now that the cache is on) are **not** covered by (a). The
mapping can outlive the fd, and the kernel writes those pages back at its own
pace - at the latest before the inode is evicted, i.e. before FORGET. Those
late writes arrive with a stale or zero file handle.

`pkg/fuse/dat9fs.go` `Write` already contains the proof: the missing-handle
branch, `extentWriteKernelFh` ("writeback after Dat9FS Release still hits
VFS"), and `extentWriteByNode` ("after the last handle is Released: the kernel
still writebacks mmap/dirty pages") exist precisely because this window is
real.

## 1.3 The hole

In the missing-handle branch of `Dat9FS.Write` today:

1. `extentWriteKernelFh` / `extentWriteByNode` resolve the write only when the
   node provably maps to a JuiceFS inode (extent file);
2. otherwise, on a mount with `--extent` patterns configured
   (`extentEnabled()` checks the mount config, **not** the target file's
   layout), the `extent-orphan-discard` branch returns OK and **silently
   drops the data** - including data belonging to a *classic* file on a mixed
   mount;
3. on a non-extent mount the write fails with ENOENT, surfacing as EIO at the
   application's next close/sync.

Before the kernel cache this branch was nearly unreachable; with the cache on
it is on the mmap write path for every classic file.

## 1.4 The WRITE_CACHE + O_APPEND + local fd hazard (second hole)

The same offset-trust issue exists **while the handle is still live**, and it
is what broke `git clone` in Local E2E:

- applications like git open reflog files with `O_APPEND`;
- with the kernel writeback cache, those writes arrive as `WRITE_CACHE`
  requests carrying **absolute offsets** of the dirty range (the kernel
  writes back `[0, old+new)` when the previous bytes were still dirty), so
  the daemon must honor the request offset rather than appending;
- the local-overlay fd branches used `os.File.WriteAt` there, and **Go
  rejects `WriteAt` on a file opened with `O_APPEND`** ("invalid use of
  WriteAt on file opened with O_APPEND"), which mapped to EIO at the first
  writeback — `git clone` died at "unable to append to
  `.git/logs/refs/remotes/origin/HEAD`".

Fix: `writeLocalFileForHandle` — plain `O_APPEND` syscall writes keep fd
append semantics; `WRITE_CACHE` writes on an `O_APPEND` fd do
`Truncate(offset)` (no-op when the file already ends there, zero-fill when
short) followed by append, which is exactly `WriteAt`'s effect for a
full-range writeback without the forbidden call or the prefix duplication a
plain append would produce. The remote dirty-buffer path is unaffected (its
`WriteBuffer` has no such restriction, and it already trusts the kernel
offset for `WRITE_CACHE`).

## 1.5 The hardlink-alias truncate race (third hole)

`printf "x" > hardlink.txt` (a truncate-then-write through a hardlink alias)
surfaced in Local E2E as `writing hardlink updates remote source` failing
with the remote stuck at 0 bytes while the local read was correct. The
mechanism is a commit-ordering race on the shared object:

- the O_TRUNC arrives as a path-based SetAttr (no fh) and, on the
  write-back staged path, is committed as its own async zero-byte write to
  the canonical path — racing the writer's content commit on the alias
  path, both carrying the open-time base revision;
- whichever lands first wins and the commit queue terminally refuses the
  other ("refusing LWW rebase"), so the remote ends at 0 either way;
- a second, independent zero could come from a **clean zombie**: the
  path-truncate buffer sync dirtied its buffer, and it then committed the
  truncated image with its own stale base revision.

The fix has two tiers:

1. **Caller-owned fold (the common case).** When the truncating process is
   the inode's ONLY live writer, `adoptCallerOwnedInodeTruncate` folds the
   truncate into the caller's own handle buffer (including hardlink
   aliases) and **no remote zero-truncate is committed at all**: the
   truncate and the caller's subsequent writes commit as a single write, so
   the race does not exist. The caller's base revision stays at its open
   value and its next commit CAS-succeeds at it. This also removes the
   production-only O_TRUNC-flag bug — the kernel issues the truncate as a
   separate SetAttr and does not propagate O_TRUNC into the FUSE open
   flags, so the refresh that was supposed to save the writer never fired
   outside unit tests.
2. **Concurrent-writer fencing (unchanged semantics).** With a live writer
   from another process on the inode, the remote truncate still commits
   and its revision bump remains the CAS fence that makes a stale writer's
   next commit conflict detectably instead of silently clobbering. The
   base-revision refresh now iterates every alias path of the inode (and
   dirty zombies of the inode), and `canStagePathTruncateToZero` declines
   the async staged path while any live (non-zombie) writer holds the
   inode, forcing the synchronous truncate that lands first and refreshes
   the writer before its commit. The truncate buffer syncs no longer
   create phantom dirty state on clean zombies (they truncate the buffer
   but keep the zombie clean, so it never publishes the zero image with a
   stale base).

---

# 2. The durability contract that must not change

`docs/design/fuse-durability-policy.md` defines the user-facing profiles.
Under kernel-writeback-cache-on the matrix must remain exactly:

| Policy | write() returns | close / FLUSH | fsync | drain / syncfs |
|---|---|---|---|---|
| `write-sync` | remote-durable | - | - | - |
| `close-sync` | local (kernel page cache) | remote-durable | remote-durable | everything remote |
| `fsync` | local | local-durable (shadow) | remote-durable | everything remote |
| `auto` / `interactive` | local | local-durable + async remote | local-durable | everything remote |

Two clauses follow immediately:

- **`write-sync` is mathematically incompatible with the kernel cache**:
  "remote-durable when write() returns" cannot hold while the kernel buffers.
  `auto` already leaves the cache off for write-sync; the explicit
  `--writeback-cache on` + `--durability write-sync` combination must be
  **rejected**, not silently honored.
- Post-close mmap stragglers have no fsync boundary to honor: POSIX promises
  nothing for data the application never synced. drive9's floor for them is
  the `auto` floor - **local-durable staging plus an async remote commit** -
  with a working "reopen + fsync" escape hatch (§3.4).

macFUSE and older kernels ignore the cap, and `auto` keeps the cache off for
write-sync: in both cases nothing in this document activates (§6).

---

# 3. Design: extend the FileHandle lifetime to FORGET ("zombie handle")

## 3.0 Principle

When the kernel writeback cache is negotiated, a classic file handle's data
responsibility does not end at RELEASE; it ends at FORGET (the kernel cannot
evict an inode with dirty pages - eviction forces writeback first). The fix is
therefore not a new write path but a lifetime change:

```
normal handle:   Open ────────────── Release ── deleteFileHandle
zombie handle:   Open ────────────── Release ── zombie ── FORGET ── deleteFileHandle
                                   (commit      (accept late    (final sync,
                                    as today)    writebacks)     free)
```

The zombie keeps the handle in `fileHandles` (keyed by fh, so a writeback
carrying the released fh hits it directly) plus a dedicated per-inode index
`zombiesByInode` (so a node-addressed writeback finds it), with a `Zombie`
flag. It is deliberately **not** in `openHandles`: a zombie is closed for
every namespace purpose, and keeping it out leaves the ~20 open-handle call
sites (unlink-while-open, rename retarget, SQLite sidecar probes,
`shouldPreserveForgottenInode`) byte-identical. The zombie reuses the entire
existing Write machinery: dirty buffer, shadow write-through, DirtySeq
generation fences, `remoteCommitLock` serialization, unlinked discards.
**No second staging or commit path is introduced.** The two namespace flows
that must see it are wired explicitly:

- unlink: `markOpenHandlesUnlinked` additionally marks the path's zombies
  (`markZombiesUnlinkedForPath`), so absorbed writebacks keep anonymous-fd
  semantics and can never resurrect the removed path;
- rename: `retargetOpenHandlesForRename` additionally retargets zombie paths
  (`retargetZombiePathsForRename`), so a pending zombie commit lands on the
  post-rename path.

## 3.1 Release: commit as today, then become a zombie instead of dying

In `Dat9FS.Release`:

1. Run the existing release logic unchanged - writeback-cache fast path,
   ShadowSpill commit, synchronous `flushHandle`, pending-mode finalizer.
   close-time durability semantics and latency are untouched.
2. After a successful release, if **all** of the following hold, mark
   `fh.Zombie = true` and skip `deleteFileHandle`:
   - `kernelWritebackCacheEnabled(opts)` (Linux, cache negotiated);
   - the handle was opened writable;
   - the handle is **not** extent (extent has its own post-Release handling:
     `extentWriteKernelFh` / `extentWriteByNode`);
   - the handle is **not** on an append-log path (§6.2);
   - the handle is not unlinked-discarded (unlinked handles keep today's
     discard semantics - late writes to them are anonymous-fd data that must
     never resurrect the path).
3. Local-file handles (`isLocalFileHandle`, git-workspace local files): keep
   the underlying `*os.File` open in the zombie instead of closing it, so a
   late kernel writeback has somewhere to land; close it at FORGET.
4. `cleanupReleasedInode` already skips inodes with open handles
   (`hasOpenHandle`), so the inode mapping survives until FORGET with no extra
   work. This matters for §4 (attr consistency).

The zombie's dirty buffer is empty after the release commit (`ClearDirty`
already ran), so the steady-state memory cost is one `FileHandle` skeleton per
closed-but-not-forgotten inode.

## 3.2 Write routing: the missing-handle branch, reordered

`Dat9FS.Write` missing-handle branch becomes, in order:

1. `fileHandles.Get(input.Fh)` hit, handle is a zombie -> run the **normal**
   Write path (it already handles everything: dirty buffer, shadow
   write-through, DirtySeq, `inodes.UpdateSize`, setid clearing). After the
   write lands, the zombie's debounced remote commit is (re)armed.
2. `extentWriteKernelFh` - unchanged (extent stale VFS fh).
3. `extentWriteByNode` - now returns ENOENT for nodes without a live extent
   mapping instead of silently accepting them; the caller classifies.
4. No fh hit -> look for a zombie by inode (`zombiesByInode`; the kernel may
   send a zero fh) and handle as in (1).
5. Discard with OK **only** when the extent path answered ENOENT **and**
   `InodeToPath.WasEverExtent(nodeId)` proves the inode carried an extent
   mapping (inode numbers are never reused, so this is exact). Every such
   discard is logged (this is P1-7's acceptance criterion: "no write may be
   discarded silently"). A hard extent data-plane error is propagated to the
   kernel, not swallowed.
6. Otherwise -> ENOENT, logged loudly.

The invariant after this change: **classic-file bytes can never enter the
discard branch.** On a mixed mount (`--extent '*.db'`) a late writeback to a
classic `notes.txt` hits its zombie; a forgotten extent inode hits (5).

**At most one zombie per inode.** The kernel page cache is per-inode, so a
late writeback must land in the newest handle's buffer (its BaseRev covers
everything committed so far). Registering a zombie purges the inode's older
zombies (after syncing them); map iteration never picks a stale one.

## 3.3 What the zombie Write reuses (and why that is the point)

| Concern | Mechanism the zombie inherits for free |
|---|---|
| Local durability | shadow write-through in Write (`shadowStore.WriteAt`) |
| Remote commit | the zombie's debounced timer drives `syncHandleToRemoteLocked` — the same "make remote-durable now" path Flush/Fsync use |
| Concurrent same-path ordering | `fh.mu` + `remoteCommitLock` + DirtySeq fences |
| Size/mtime visibility | `inodes.UpdateSize/UpdateMtime` on every write |
| GetAttr consistency | `dirtyHandleSize` reads the dirty-size map, which zombie writes update (§4.2) |
| Unlink racing a late write | `markZombiesUnlinkedForPath` (wired into `markOpenHandlesUnlinked`) — anonymous-fd semantics, never resurrected |
| Rename racing a late write | `retargetZombiePathsForRename` (wired into `retargetOpenHandlesForRename`) |
| Crash recovery | shadow + pendingIndex are on disk; `RecoverFromDisk` replays them on the next mount - identical to the `auto` contract |

## 3.4 Closing the loop: fsync/flush/truncate on a *different* fd

The POSIX escape hatch `mmap write -> close -> reopen -> fsync` must flush the
zombie's staged data:

- `syncZombieHandlesForInode` runs at the top of `Fsync` (before the calling
  handle is locked): a reopened fd's fsync commits every dirty zombie of the
  same inode through `syncHandleToRemoteLocked` first. The zombie commit is
  deliberately the *stronger* remote commit even under interactive mode -
  zombie data is rare and this keeps `mmap write -> close -> reopen -> fsync`
  exact. `syncOpenSourceForHardlink` is unchanged (it serves live handles).
- Path-based `truncate(path, size)` syncs the path's zombies before taking
  the per-path commit lock (`syncZombieHandlesForPath` ahead of
  `waitQueuedRemoteCommitBeforeWrite` in SetAttr); the ftruncate(FATTR_FH)
  branch truncates zombie buffers in step with the kernel's own page
  invalidation (`truncateZombieBuffersForInode`).
- `Flush`/`Fsync` on the surviving live handles are otherwise unchanged.

## 3.5 FORGET is the exact end of life

`Dat9FS.Forget` purges the inode's zombies: sync any dirty zombie state into
the commit queue, then `deleteFileHandle` (and close a kept local fd).
Correctness of FORGET as the boundary: the kernel cannot forget an inode that
still has dirty pages - writeback is forced before eviction - so no writeback
can arrive after FORGET. [assumed; this is also the invariant
`extentWriteByNode` already relies on]

Backstop against leaks: a janitor TTL (~300 s, ≫ the kernel's 30 s dirty
writeback default) that force-syncs and frees zombies whose FORGET never
arrived, logging each eviction; plus a soft cap (~10k zombies) evicting the
oldest clean ones.

## 3.6 drain / unmount / reexec

- `drive9 mount drain` issues syncfs (the go-fuse fork's FUSE_SYNCFS
  dispatch): the kernel writes back dirty pages, they land in zombies, and the
  commit queue drains them - the existing drain phases cover the rest. Wire
  the drain's "no open handles" check to treat clean zombies as drainable
  (sync-then-free), not as a blocker.
- Unmount `FlushAll` syncs and frees zombies the same way.
- The reexec audit (`docs/design/fuse-clean-state-reexec-audit.md`) gains one
  row: "zombie handles - refused unless clean; clean zombies are synced and
  freed at handoff."

---

# 4. Correctness argument

## 4.1 Coverage completeness

Under kernel-writeback-cache-on, a byte the daemon has not seen lives in
exactly one of two windows: before Release (covered by kernel fact (a) and the
unchanged Flush/Fsync/Release paths) or inside [Release, FORGET] (covered by
the zombie, by kernel fact (b)). After FORGET no dirty page can exist. The
zombie lifetime is therefore exact - neither shorter (data loss) nor longer
(leak, bounded by §3.5's janitor anyway).

## 4.2 Attr consistency (the GETATTR-shrink hazard)

With the cache on, a GETATTR reply whose size is smaller than the kernel's
i_size risks the kernel shrinking i_size and discarding dirty pages beyond it.
The daemon's answers stay safe because every write - including every zombie
write - goes through the normal path's `inodes.UpdateSize`, and `GetAttr`
already prefers dirty sizes via `dirtyHandleSize`, which reads the dirty-size
map that zombie writes update.

## 4.3 Concurrency

No new lock ordering is introduced: zombie writes hold `fh.mu` and the
per-path `remoteCommitLock` exactly like live-handle writes, and the
commit-queue entries carry the same generation guards that protect
release-time commits from superseded-mutation cleanup.

## 4.4 Crash recovery

A zombie write is shadow-write-through (local disk) plus a pendingIndex entry
before the daemon replies OK to the kernel. A daemon `kill -9` therefore loses
only bytes still in the kernel - the same window `auto` always had - and the
next mount replays staged state through `RecoverFromDisk`. fsync'd data is
unaffected end-to-end (fact (a)).

---

# 5. Mixed-profile compatibility

- Extent files: unchanged. `extentWriteKernelFh` / `extentWriteByNode` keep
  their positions in the routing order (§3.2); the zombie branch is
  classic-only.
- Classic files on an extent-enabled mount: now always resolve to their
  zombie; the blanket `extentEnabled()` discard is gone (§3.2 step 5-6). This
  is the mixed-profile acceptance criterion.
- Extent inode orphan detection becomes explicit (`WasEverExtent`, fed by
  every `SetExtentIno`) instead of a mount-config heuristic, which also
  removes a mis-binding class for P1-4-adjacent probes.

---

# 6. Scope boundaries (do not "fix" these here)

1. **cache-off behavior is byte-identical.** macFUSE, old kernels,
   `--durability write-sync`, and `--writeback-cache off` never create
   zombies. The entire change is gated on `kernelWritebackCacheEnabled(opts)`.
2. **append-log paths do not zombify.** Their Flush is already a remote sync;
   mmap-writing a WAL is not a real workload. A late writeback to an
   append-log path gets ENOENT + log (observable, reproducible, fixable
   later) instead of silent acceptance.
3. **P1-5 is out of scope.** The extra meta commit RPC per kernel writeback
   batch on the extent path is an extent-side optimization and proceeds
   independently.
4. **P1-9 is not fixed by this document.** cache-off stalls remain a separate
   (degraded-mode) effort; this design does not make them worse.
5. Do not weaken the write-sync rejection (§2): no silent downgrade of
   `--writeback-cache on` + `--durability write-sync`.

---

# 7. Change list (implementation order)

| # | Where | Change |
|---|---|---|
| 1 | `pkg/fuse/mount.go` `Validate`, `cmd/drive9/cli/mount.go` | reject `--writeback-cache on` with `--durability write-sync`; keep auto = off for write-sync |
| 2 | `pkg/fuse/handle.go`, `pkg/fuse/open_handles.go`, `pkg/fuse/dat9fs.go` `Release` | `FileHandle.Zombie`; zombie transition after a successful release under the §3.1 conditions; keep local fds; skip `deleteFileHandle`; at most one zombie per inode (registering purges predecessors after syncing them) |
| 3 | `pkg/fuse/dat9fs.go` `Write` missing-handle branch, `pkg/fuse/extent_jfs.go` `extentWriteByNode`, `pkg/fuse/inode.go` | reorder per §3.2; zombie lookup by fh then by inode; `extentWriteByNode` reports ENOENT for unmapped nodes instead of accepting them; `WasEverExtent` gates every discard; mandatory logging on every discard |
| 4 | `pkg/fuse/writeback_zombie.go` | debounced zombie commit timer driving `syncHandleToRemoteLocked`; retry backoff; unlinked zombies discard |
| 5 | `pkg/fuse/dat9fs.go` `Fsync` / `SetAttr` | `syncZombieHandlesForInode` at fsync entry; `syncZombieHandlesForPath` before path truncate; `truncateZombieBuffersForInode` on ftruncate |
| 6 | `pkg/fuse/dat9fs.go` `Forget` | zombie purge (sync + free + close local fd); janitor TTL + soft cap |
| 7 | drain / `FlushAll` | drain already walks `fileHandles` (`drainOpenHandles`), so dirty zombies are flushed by the existing phase; `FlushAll` stops the janitor and purges the rest |
| 8 | `pkg/fuse/dat9fs.go` `markOpenHandlesUnlinked` / `retargetOpenHandlesForRename` | zombie unlink marking + rename retargeting |
| 9 | `pkg/fuse/dat9fs.go` `writeLocalFileForHandle` (§1.4) | `O_APPEND` + `WRITE_CACHE` on local overlay fds: `Truncate(offset)` + append instead of the forbidden `WriteAt` (the `git clone` reflog EIO) |
| 10 | docs | update the durability matrix in `fuse-durability-policy.md`; mark the covered items in `docs/extent-todos.md` |

Estimated size: ~600-900 lines of implementation plus a comparable test
surface.

---

# 8. Test plan

## 8.1 Unit tests (`pkg/fuse`)

- zombie lifecycle: Release -> zombie -> Forget; TTL janitor eviction;
- missing-handle routing matrix: extent live / extent forgotten / extent
  unmapped x classic zombie hit / miss / unlinked - asserting both the status
  and that no classic byte reaches the discard branch;
- GetAttr after a zombie write reports the grown size;
- unlinked zombie accepts writes but never stages/uploads;
- mount validation rejects write-sync + forced cache.

## 8.2 e2e (run under `none` and a mixed `--extent '*.db'` profile)

1. `mmap(MAP_SHARED) write -> munmap -> close -> reopen -> read`: data intact;
2. `mmap write -> close -> drive9 mount drain -> remote fs cat`: visible;
3. `mmap write -> close -> reopen -> fsync -> remote read`: visible (fsync and
   close-sync policies);
4. `kill -9` with mmap dirt not yet written back: remount shows the old
   content intact (losing unsynced data is allowed by `auto`);
5. mixed-profile acceptance: mmap late write to a classic `notes.txt` under
   `--extent '*.db'` survives, and the mount log contains no
   `extent-orphan-discard` for the path.

## 8.3 Regression gates

`community.sqlite`, `community.fsx` (including its mmap mode),
`community.pjdfstest`, `community.fio` under both `none` and
`coding-agent-extent`; `e2e/fuse-sqlite-correctness.sh`,
`e2e/fuse-sqlite-commit-sequence.sh`, `e2e/fuse-crash-recovery-test.sh`;
`e2e/fuse-supervision-test.sh` drain assertions.

---

# 9. Risks

| Risk | Mitigation |
|---|---|
| zombie state lingers when the kernel delays FORGET (dcache pressure) | janitor TTL + soft cap (§3.5); each zombie is a small skeleton |
| a late write arrives after FORGET due to a kernel/go-fuse edge we did not model | ENOENT + loud log (§3.2 step 6) makes it observable instead of silent; the routing matrix test pins the behavior |
| append-log exclusion leaves a narrow EIO window | logged and bounded to a workload (mmap over WAL) we have not observed; §6.2 |
| zombie commits racing live commits of the same path | same `remoteCommitLock` + generation fences as today (§4.3) |
