# Extent content layout v2 — client adaptation

Status: implemented on `feat/extent-v2-client` (hard cutover, no compatibility
window). The server-side design lives in the deployment repository
(design doc `extent-content-layout-v2.md` in the private server repository); this document
records the client half and the transition state of this repository's local
server.

## What changed

Extent stops being an embedded filesystem and becomes a content layout:

- **The v1 embedded engine is gone.** `pkg/extent` (the JuiceFS meta
  client, VFS, chunk store, STS data plane, client-side compaction
  loop), the `mornyx/juicefs` fork dependency, and the
  `/v1/extent/meta` + `/v1/data-credential` client surface are deleted.
- **The extent plane is the community JuiceFS engine.**
  `pkg/extensions/extent` bridges the unmodified community engine
  (vfs + chunk store) onto the v2 wire protocol — see
  `extent-jfs-engine-plane.md` for the architecture. It claims
  extent-layout files (and, at threshold 0, every fresh create) above
  the write plane; classic files keep the drive9 planes untouched.
- **Writes ride the unified write plane in hybrid mode.** The commit
  queue carries an extent branch
  (`pkg/extensions/writeback/extent_commit.go`):
  - existing extent target + dirty ranges → incremental `PATCH`
    (`dirty_blocks` with revision CAS; only the delta is uploaded — the
    append/`.log` case),
  - fresh large file (≥ the server's inline cutoff) or spilled whole-image
    entry → sequential session (`initiate layout=extent`; a legacy server
    ignores the field and the same session proceeds classically, so the
    takeover degrades gracefully),
  - existing classic targets are never converted — they keep the multipart
    path (legacy read/modify capability preserved).
  Dirty ranges are captured from the write buffer's dirty parts at enqueue
  (`CommitEntry.DirtyRanges`) before `ClearDirty`. In full engine mode
  (threshold 0) the engine claims creates itself and this branch stays
  disarmed.
- **`pkg/client` speaks v2 natively.** New wire types and methods
  (`GetExtentReadPlan`, `InitiateExtentPatch`, `CommitExtentPatch`,
  `WriteExtentStreamConditional`, `FetchExtentBlockRange`); `ReadStream` /
  `ReadStreamRange` serve extent files from read plans transparently (the
  old 422 + `X-Dat9-Extent-Ino` hint fallback is gone). CLI/SDK reads of
  extent files work with no extent runtime.
- **`bulkfile` is deleted** — the write plane itself owns what it did.

## Local server transition state

The v1 extent server surface in this repository is removed
(`pkg/server/extent_meta.go`, `pkg/datastore/jfs*.go`, the extent schema,
the quota-report plumbing, `file_nodes.extent_ino`). Until the v2 server
implementation upstreams from the private server repository, a local `drive9-server` is
classic-only:

- extent files cannot exist on a local server, so the extension is dormant
  and every write takes the classic path;
- `WriteExtentStreamConditional` against such a server silently lands a
  classic multipart file (see the graceful-degradation note above);
- extent e2e coverage moves to the deployment repository's harness
  (`make e2e-local` there grafts this client); the v1 extent suite
  (`e2e/extent-rename-mixed-dir.sh`) is deleted.

## Known protocol workarounds

- A pure truncate-grow has no dirty bytes, but PATCH requires at least one
  dirty block: the commit queue synthesizes one zero block at the old end
  (content addressing dedupes it). This should be relaxed server-side.
- Sequential extent sessions are confirmed by `UploadPlanV2.key == ""`; a
  non-empty key means the server predates `layout` and the session is
  classic.

## Open items

- Server upstream of the v2 handlers into this repository (coordinated with
  the server repository session) to restore extent support on local servers.
- FUSE-level end-to-end coverage once that lands (mixed-tree takeover suite
  semantics were sketched in the abandoned PR 1031 and should be re-landed
  against this implementation).
- `/v1/status` has no extent capability flag yet; discovery is per-file via
  the stat content-layout header.
