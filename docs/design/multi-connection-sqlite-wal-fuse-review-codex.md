---
title: Review of Drive9 FUSE Multi-Connection SQLite WAL Design
reviewed: 2026-09-08
proposal: docs/design/multi-connection-sqlite-wal-fuse.md
verdict: NOT_READY
blockers: 2
concerns: 4
---

## Verdict

NOT_READY - scope is correct and W2/W3 direction is grounded, but W1's
current-behavior description cites a call that does not exist on the adoption
path, and its prescribed fix cannot reach the generation-reset route it names
because the post-truncate committed size is 0.

## Strengths

What the proposal does well:

1. Faithful 1:1 transcription of #907: workstreams, estimates, acceptance criteria, and out-of-scope lines all match the issue.
2. Scope baseline is explicit and honest: in-scope/out-of-scope and the LoC table are stated up front, and no new schema, lock, worker, or second state machine is introduced.
3. The W2 and W3 change directions check out against the code: the `size <= OrigSize` gates (pkg/fuse/dat9fs.go:12826, pkg/fuse/dat9fs.go:15252), the DB9 override, the `-shm` short-read guard (pkg/fuse/dat9fs.go:11881), the 64 MiB shadow threshold (pkg/fuse/shadow.go:17), and the server-side read_url rule (pkg/backend/patch.go:44, pkg/backend/patch.go:290) all exist as described.
4. The PATCH-for-growth decision is verified against the server plan code; the client-only nature of W3 is correct.

## Review Summary

| Dimension | Status | Action Required |
|-----------|--------|-----------------|
| Problem Understanding | OK | - |
| Completeness | CONCERN | Crash-window enumeration for the new cross-handle transition |
| Technical Approach | BLOCKER | W1 route eligibility vs size-0 baseline unresolved |
| Assumptions & Risks | BLOCKER | W1 step 3 cites a nonexistent call; W1 change targets the wrong site |
| Scope Appropriateness | OK | - |
| Workload Size | CONCERN | W1 estimate assumes a local rebind only |
| Contract Consistency | OK | Field names and helpers exist; one H0 phrasing issue, see blocker 2 |
| Implementation Readiness | BLOCKER | W1 cannot be coded from the current text |
| Open Questions | OK | See Questions for Author |

## Blockers (Must Fix)

### Assumptions & Risks: W1 step 3 and the prescribed fix name a call that does not exist on the adoption path

- **Issue**: Section 4.1 step 3 says `refreshCommittedRevisionForOpenHandlesWithSize` (pkg/fuse/dat9fs.go:2547) rebuilds the clean WriteBuffer via `rebindCleanWriteBufferToRemoteLocked`, "which calls `appendLogRebindLayout` (pkg/fuse/append_log.go:235)". That is false: the body of `rebindCleanWriteBufferToRemoteLocked` (pkg/fuse/dat9fs.go:2748) never touches `fh.appendLog`, and neither does `refreshCommittedRevisionForOpenHandlesWithSize`. The adoption path leaves the append-log baseline entirely untouched, which is precisely the stale-baseline state that step 4 correctly describes. Consequently the Change step "replace `appendLogRebindLayout` with `appendLogAdoptCommittedBaseline`" targets a call that is not there.
- **Location**: section 4.1, steps 3 and "Change".
- **Suggestion**: Restate step 3 as "the adoption path updates `BaseRev`/`OrigSize` and rebuilds the buffer but does not touch `fh.appendLog`", and restate the fix as "add an append-log baseline adopt for clean siblings that had applied a zero-truncate, in the path that actually runs". Name that path precisely: the zero-size handle sync at pkg/fuse/dat9fs.go:13192 refreshes siblings with `committedSize=0`; precedent adopt calls already exist at pkg/fuse/dat9fs.go:2317 and pkg/fuse/dat9fs.go:2370.

### Technical Approach: the named generation-reset route cannot fire with a 0-byte committed baseline

- **Issue**: The Change step claims the rebind makes "the next header write take the generation-reset route against the new expected revision". But `tryAppendLogGenerationResetLocked` returns NotApplicable when `expectedSize < sqliteWALHeaderSize` (pkg/fuse/append_log.go:705), and after `wal_checkpoint(TRUNCATE)` the committed size is 0, so `appendLogAdoptCommittedBaseline(R+1, 0)` yields `expectedSize=0`. With the gates cleared, the 32-byte header write would instead fall through to tail append / generic routing, and its outcome depends on the server layout of the 0-byte commit. Also, "rebind the committed header H0 from the new committed 32-byte view" is impossible: the new committed view is 0 bytes; H0 must be preserved from the old generation (adopt leaves `sqliteWALCommittedHeader` untouched, which is the right behavior, but the text says the opposite).
- **Location**: section 4.1 "Change".
- **Suggestion**: Specify the actual post-rebind route and any gate change explicitly. At minimum: (a) whether the `expectedSize >= sqliteWALHeaderSize` gate is relaxed for the post-truncate rebind, or whether a different route (tail append onto the 0-byte base / layout-aware rewrite) is intended and how the shadow rotation boundary is preserved; (b) what layout the server assigns to the 0-byte truncate commit on an append-log path, and what happens if it is `single`; (c) restate H0 as "preserve the pre-checkpoint committed header for the salt comparison", not "read from the new committed view".

## Concerns (Should Fix)

### Assumptions & Risks: the 30 s fsync-context citation points at the wrong function

- **Issue**: Section 4.3 step 4 cites "the fixed 30 s fsync context (pkg/fuse/dat9fs.go:3785)". Line 3785 is `applyPendingModeWithTimeoutLocked` (pending chmod), not the fsync upload. The 30 s constant is `fuseTimeout = 30 * time.Second` (pkg/fuse/dat9fs.go:452), and a fixed 30 s context also exists in the commit queue (pkg/fuse/commit_queue.go:2313). The write-sync path itself already uses `releaseTimeout(size)` (pkg/fuse/dat9fs.go:12711), so the path that actually applies a fixed 30 s timeout to the growth upload must be pinned.
- **Location**: section 4.3 step 4.
- **Suggestion**: Correct the reference and name the exact context path that expires for the 2 GiB growth case.

### Assumptions & Risks: residency guard wording understates the failure shape

- **Issue**: Section 4.3 change 2 says the guard prevents "uploading a zero-length part". `WriteBuffer.PartData` (pkg/fuse/write.go:786) returns a zero-filled slice for an in-range evicted part, not nil, so the current snapshot loop at pkg/fuse/dat9fs.go:12839 (`if src != nil`) would capture zeros and upload a zero-filled part, silently corrupting content. A loaded check (`IsPartLoaded`) is required; `PartData != nil` is not one.
- **Location**: section 4.3 change 2.
- **Suggestion**: Reword to "zero-filled snapshot from an evicted part" and state that the guard must consult `IsPartLoaded` (as pkg/fuse/append_log.go:249 does), not nil-ness.

### Completeness: crash-window analysis is missing for the new cross-handle transition

- **Issue**: W1 introduces a new adopted state on sibling handles. The proposal does not trace the writer-unavailable / checkpointer-crash windows: sibling adopts R+1 but dies before rebind; checkpointer commits truncate and exits before the writer observes it; H0 provenance across those windows.
- **Location**: section 4.1.
- **Suggestion**: Add a short enumeration of the transition's failure windows and confirm each lands in a fail-closed CAS/EEXIST or re-prove state, consistent with the accepted spec's crash rules.

### Workload Size: W1 estimate presumes the simpler fix

- **Issue**: The 60-120 LoC estimate assumes a local rebind. If blocker 2 forces a generation-reset gate change plus layout-verification handling, W1 may grow past the estimate.
- **Location**: section 1 estimate table.
- **Suggestion**: Re-estimate after blocker 2 is resolved; per the scope-gate rule, report if it crosses 1.5x or a size class.

## Questions for Author

1. Which commit path actually refreshes the writer sibling after the checkpointer's TRUNCATE fsync: the size-0 refresh at pkg/fuse/dat9fs.go:13192, or `updateOpenHandleBaseRevision` via `rewriteAppendLogPathTruncate` (pkg/fuse/append_log.go:1182)? Both exist, and the fix site depends on the answer.
2. Is relaxing `expectedSize >= sqliteWALHeaderSize` at pkg/fuse/append_log.go:705 in scope for W1, or is a non-reset route intended for the first header after a TRUNCATE checkpoint?
3. Is the server's layout for a 0-byte conditional write on an append-log path guaranteed to remain `append_log`? This determines whether the reset route can even observe the expected layout.
