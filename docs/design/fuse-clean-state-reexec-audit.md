# Drive9 FUSE Clean-State Binary Reexec Audit

**Status:** audit boundary for V0 implementation
**Date:** 2026-07-09

## Scope

This document defines the first safe implementation boundary for Drive9 FUSE
binary reexec. V0 is a **clean-state binary reexec prototype** only.

V0 explicitly does not cover:

- CSI Node Pod rolling upgrade.
- Mount Pod recreate upgrade.
- Dirty or active workloads.
- JuiceFS-style smooth upgrade product semantics.
- Vault and WebDAV mounts.

Those require a later CSI/mount-pod lifecycle split plus fd relay through the
node agent.

## Current FUSE Lifecycle

The CLI default path starts a background child process and turns the request
into `drive9 mount --foreground`; readiness is the child writing mount process
state through `pkg/mountstate` (`cmd/drive9/cli/mount.go`).

`pkg/fuse.Mount` currently owns the full foreground mount lifecycle:

1. Validate credentials and remote root.
2. Construct `Dat9FS`.
3. Initialize process-local caches and persistent writeback state:
   `PendingIndex`, `ShadowStore`, `Journal`, `WriteBackCache`,
   `CommitQueue`, and `WriteBackUploader`.
4. Create the go-fuse server with `gofuse.NewServer`.
5. Start `server.Serve`, wait for `server.WaitMount`, then start SSE watchers.
6. Start the mount-control socket and write `mountstate.ProcessState`.
7. Wait for `SIGINT`/`SIGTERM` or `server.Wait`.
8. On shutdown, stop watchers, call `Dat9FS.FlushAll`, then unmount.

The mounted FUSE connection fd is currently private to go-fuse. Drive9 calls
`gofuse.NewServer(dat9fs, mountPoint, opts)` and never receives the fd. The
vendored replacement's `fuse.Server` stores it in private field `mountFd`, and
`Serve` closes it when the serve loop exits. V0 cannot implement fd handoff
without either a go-fuse extension or a Drive9-owned mount/server wrapper.

## Existing Drain Semantics

`Dat9FS.Drain` is a useful data-safety primitive, but it is not a reexec gate by
itself. It currently:

- Flushes debounced writes.
- Flushes open file handles.
- Drains git overlay work.
- Waits for the legacy uploader and `CommitQueue`.
- Waits for async kernel notification goroutines.
- Reports pending open handles, dirty handles, commit queue work, and uploader
  work.

It does not:

- Stop new FUSE requests from entering handlers.
- Track all in-flight go-fuse requests.
- Require clean open file/directory handles to be absent.
- Prove the kernel no longer holds non-root inode lookup references.
- Snapshot xattrs, locks, dir handles, or inode maps for another process.

Therefore V0 must add a stricter reexec-specific gate instead of treating a
successful drain response as sufficient.

## State Matrix

| State | Current owner | Classification | V0 gate |
| --- | --- | --- | --- |
| Kernel FUSE connection fd | go-fuse `Server.mountFd` | 必须传递 | Blocked until Drive9 can extract/import the fd without closing the mount. |
| go-fuse serve loop and request readers | go-fuse `Server` | 必须传递 | New process must not serve until old process explicitly cuts over. |
| Mount process state and control socket | `pkg/mountstate`, `mount_control_unix.go` | 不需要 | Recreate after accept; old state remains authoritative until cutover. |
| SSE and layer event watchers | `pkg/fuse/sse.go`, `layer_events.go` | 不需要 | Stop/recreate; no correctness state may live only in the watcher. |
| Read cache and disk read cache | `ReadCache`, `DiskReadCache` | 不需要 | Drop and rebuild from server. |
| Directory/stat caches | `DirCache`, stat freshness flags | 不需要 | Drop and rebuild from server after inode gate passes. |
| Remote read singleflight/read slots | `SingleFlight`, `readSlots` | 不需要 | Must have no in-flight requests at cutover. |
| `PendingIndex` metadata | `<cache>/<mount-hash>/pending/*.meta` | 已持久化可重建 | V0 still requires no pending entries; non-empty state is a refusal until reexec recovery tests exist. |
| `ShadowStore` file data | `<cache>/<mount-hash>/shadow/*.shadow` | 已持久化可重建 | V0 still requires no active dirty shadow/pending bytes; non-empty dirty state is a refusal. |
| `Journal` WAL | `<cache>/<mount-hash>/journal.wal` | 已持久化可重建 | V0 requires drain/compact to leave no replay-required state. |
| Legacy `WriteBackCache` | `<cache>/<mount-hash>/pending/*.dat` + `.meta` | 已持久化可重建 | V0 refuses if uploader cached/queued/in-flight work remains. |
| `CommitQueue` | process workers + persisted pending index/shadow | 已持久化可重建 | V0 refuses if pending, delayed, in-flight, or conflict count is non-zero. |
| `WriteBackUploader` | process workers + legacy writeback cache | 已持久化可重建 | V0 refuses if queued, in-flight, or cached count is non-zero. |
| Open file handles | `fileHandles`, `openHandles`, kernel fh ids | 必须传递 | V0 refuses if any file handle exists, even read-only. |
| Open directory handles | `dirHandles`, kernel fh ids | 必须传递 | V0 refuses if any directory handle exists. |
| Dirty write buffers | `FileHandle.Dirty`, `DirtySeq`, shadow flags | 必须阻止升级 | Refuse until dirty state is flushed and the handle is closed. |
| Inode/path map with kernel refs | `InodeToPath` and kernel NodeId cache | 必须传递 | V0 refuses if any non-root inode has `Nlookup > 0`; entries with no kernel refs may be dropped. |
| FUSE lock table | `fuseLockTable` | 必须传递 | V0 refuses if any lock is held; add a mechanical count before implementation. |
| In-memory xattrs | `XAttrStore` | 必须传递 | V0 refuses if any xattr exists; add a mechanical count before implementation. |
| `committedRev` and path commit locks | `Dat9FS.committedRev`, `remoteCommitLocks` | 不需要 | Drop only after no open handles and no commit/upload work. |
| Deleted-path tombstones | `Dat9FS.deletedPaths` | 不需要 | Drop; stale backend listings are a temporary cache concern, not durable state. |
| Local overlay / git workspace state | `LocalOverlay`, `gitWorkspaceLayer`, git checkpoint queues | 必须阻止升级 | Out of V0 unless a separate audit proves all local state is persisted and idle. |
| FS layer overlay state | layer maps, whiteouts, layer event watcher | 必须阻止升级 | Out of V0 unless a separate audit proves restore equivalence. |
| Perf/profiling state | `Profiler`, `ContinuousPerfRecorder` | 不需要 | Recreate after accept. |

## V0 Mechanical Gate

The reexec request must be rejected unless every check below passes.

### Static Scope Checks

- Mount kind is normal FUSE, not vault or WebDAV.
- No local overlay, git workspace, fs layer, checkpoint, or pack/unpack profile
  state is enabled.
- Old and new process advertise the same reexec protocol version.
- New binary path and argv/env are resolved before the old process changes any
  serve-loop state.
- A single upgrade attempt is active; repeated `SIGHUP` returns
  `already_in_progress` while the first attempt is unresolved.

### Runtime Clean-State Checks

- A reexec coordinator has stopped accepting new reexec attempts.
- A Drive9-side drain succeeds before timeout.
- No file handles exist.
- No directory handles exist.
- No dirty handle state exists.
- No non-root inode has a positive kernel lookup reference.
- No FUSE locks exist.
- No in-memory xattrs exist.
- Commit queue snapshot has `pending=0`, `in_flight=0`, `delayed=0`,
  `conflicts=0`.
- Uploader snapshot has `queued=0`, `in_flight=0`, `cached=0`.
- Pending index has no pending paths.
- Shadow store has zero pending bytes and no active shadow files.
- Journal has no replay-required frames.
- No git overlay/checkpoint work is pending.
- In-flight FUSE request count reaches zero before timeout.

### Failure Semantics

- If any preflight or runtime gate fails, abort reexec and keep the old process
  serving with the existing fd.
- If the new process exits or fails validation before accept, abort reexec and
  keep the old process serving.
- If fd transfer succeeds but the new process does not acknowledge accept before
  timeout, abort and keep the old process serving; the old process must still own
  its fd until accept.
- If protocol versions differ, abort before fd transfer.
- If a second `SIGHUP` arrives during an attempt, return a deterministic
  `already_in_progress` result and do not start a second child.
- Only after new-process accept may the old process stop serving and close its
  duplicate fd.

## Required V0 Tests

The first implementation PR must include these tests before fd handoff can be
reviewed as correct:

1. Idle mount reexec keeps the mount point mounted and readable.
2. Dirty file handle refuses reexec and old process continues serving.
3. Clean open read handle refuses reexec because the fh table is not transferred.
4. Open directory handle refuses reexec.
5. In-flight commit queue entry refuses reexec.
6. Queued/cached legacy writeback uploader entry refuses reexec.
7. Non-root inode with `Nlookup > 0` refuses reexec.
8. Held FUSE lock refuses reexec.
9. In-memory xattr refuses reexec.
10. New process crash before accept keeps old process serving.
11. New process protocol mismatch keeps old process serving.
12. Repeated `SIGHUP` during a running attempt returns `already_in_progress`.
13. FUSE drain timeout refuses reexec with a defined error.

Existing tests already cover parts of the drain behavior, but they are not
enough for V0. In particular, `TestDrainAllowsCleanOpenHandles` is correct for
`syncfs`/control-drain semantics, but V0 reexec must be stricter because file
handle ids are process-local.

## Implementation Implications

The smallest safe implementation sequence is:

1. Add read-only snapshot/count helpers for reexec gates: file handles, dir
   handles, inode lookup refs, locks, xattrs, pending index, shadow store,
   journal replay state, and in-flight FUSE request count.
2. Add a reexec preflight command path that returns structured refusal reasons
   without spawning a child.
3. Extend or wrap go-fuse so Drive9 can create a server from an already-mounted
   FUSE fd and transfer the old fd by `SCM_RIGHTS`.
4. Add a two-phase old/new handshake: `prepare -> send_fd -> validate -> accept`
   with old-process rollback until `accept`.
5. Only after those gates pass should `SIGHUP` call the reexec path.

Any implementation that sends the fd before proving the clean-state matrix, or
lets the old process exit before new-process accept, is outside this V0 scope.
