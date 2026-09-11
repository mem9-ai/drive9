---
title: Review of Drive9 FUSE Multi-Connection SQLite WAL Design (v2)
reviewed: 2026-09-08
proposal: docs/design/multi-connection-sqlite-wal-fuse.md
verdict: READY
blockers: 0
concerns: 3
---

## Verdict

READY - all v1 blockers are resolved with code- and server-verified evidence;
scope is unchanged and every cited mechanism now matches the current
implementation. Remaining items are placement/fallback details that belong in
the impl plan, not design blockers.

## Strengths

What the proposal does well:

1. All three v1 open questions are answered with evidence rather than assertion: the sibling-adoption path is `refreshCommittedRevisionForOpenHandlesWithSize` (pkg/fuse/append_log.go:967 with size 0); the reset gate is explicitly relaxed with a named predicate; and the server keeps `append_log` layout on a 0-byte rewrite (`RewriteAppendLog`, tidbcloud/fs pkg/backend/append_log.go:295).
2. W1's current behavior is now mechanistically accurate, including the `touched` flag that keeps `HasDirtyParts()` true after `Truncate(0)` and routes the truncate commit through `tryAppendLogFullRewriteLocked`.
3. The H0 semantics are corrected: preserve the pre-checkpoint committed header for the salt comparison, never read from the empty post-truncate view.
4. W3/W4 citations are corrected to the real 30 s path (`fuseTimeout` -> `Fsync` ctx -> `UploadSyncWithRevision`), and the residency guard is tied to `IsPartLoaded` with the zero-filled `PartData` behavior named.
5. Crash windows for the new cross-handle transition are enumerated, satisfying the primary-writer-unavailable trace.

## Prior Review Resolution

1. Blocked: W1 cited a nonexistent `appendLogRebindLayout` call -> fixed; the fix site is now the clean-adopt branch of `refreshCommittedRevisionForOpenHandlesWithSize` (pkg/fuse/dat9fs.go:2547), gated on `fh.appendLog.sqliteWALTruncated`.
2. Blocked: generation-reset eligibility vs 0-byte baseline -> fixed; the guard at pkg/fuse/append_log.go:706 becomes `expectedRevision <= 0 || (expectedSize != 0 && expectedSize < sqliteWALHeaderSize)`, with the upstream confirm/salt/layout fences retained.
3. Concern: 30 s citation -> fixed; now `fuseTimeout` (pkg/fuse/dat9fs.go:452), `Fsync` ctx (pkg/fuse/dat9fs.go:13663), `UploadSyncWithRevision` (pkg/fuse/dat9fs.go:14038).
4. Concern: zero-length vs zero-filled part -> fixed; guard uses `IsPartLoaded`, snapshot loop at pkg/fuse/dat9fs.go:12868.
5. Concern: missing crash-window analysis -> added as four enumerated failure windows.
6. Concern: W1 estimate -> retained at 60-120 LoC with the gate relaxation noted in the estimate row.

## Review Summary

| Dimension | Status | Action Required |
|-----------|--------|-----------------|
| Problem Understanding | OK | - |
| Completeness | CONCERN | State the salts-equal fallback route |
| Technical Approach | OK | - |
| Assumptions & Risks | OK | - |
| Scope Appropriateness | OK | - |
| Workload Size | OK | - |
| Contract Consistency | OK | - |
| Implementation Readiness | CONCERN | Pin rebind placement vs shadow-removed early-continue |
| Open Questions | OK | - |

## Concerns (Should Fix)

### Implementation Readiness: the W1 rebind must not be skipped by the shadow-removed early-continue

- **Issue**: `refreshCommittedRevisionForOpenHandlesWithSize` returns early for a clean sibling when `clearRemovedCommittedShadowLocked(fh, revision, committedSize, true)` fires (pkg/fuse/dat9fs.go:2562). A sibling that adopted the zero-truncate with `ShadowSpill` set can take that branch and skip the new `appendLogAdoptCommittedBaseline` call, leaving the stale baseline that W1 exists to fix.
- **Location**: section 4.1 Change item 1.
- **Suggestion**: Specify that the rebind runs for every clean sibling that carries `sqliteWALTruncated`, including the shadow-removed branch (place it before the early-continue, or add it to that branch). Add the shadow-spilled sibling to the W1 unit test matrix.

### Completeness: the salts-equal fallback is unnamed

- **Issue**: With the gate relaxed, the reset route still returns NotApplicable when `saltsDiffer(H0, H1)` is false. The design does not say what happens then. With `appendSafe` restored, the header write falls through to tail append onto the 0-byte base or to the layout-aware full rewrite against `(R+1, 0)`; both publish a correct 32-byte object, but the shadow rotation boundary differs. SQLite recycles salts, so this is rare, but the behavior should be pinned.
- **Location**: section 4.1 Change item 3.
- **Suggestion**: Add one sentence naming the fallback (tail append / layout-aware rewrite against `(R+1, 0)`) and confirming it must not rotate the shadow or leave the handle dirty.

### Open Questions: W2's "latest committed revision" source

- **Issue**: Section 4.2 Change item 1 gates clean-sibling reuse on "its revision equals the latest committed revision" but does not name the lookup (`latestCommittedRevisionWithSize` is the existing helper family) or state how the `-shm` short-read guard composes with a shadow that exists below the 64 MiB threshold.
- **Location**: section 4.2 Change item 1.
- **Suggestion**: Name the lookup helper and the shadow-serve path in the impl plan; no design change required.

## Questions for Author

1. Should the rebind be unconditional for `sqliteWALTruncated` clean siblings, or only when the adopted `committedSize == 0`? The marker already implies the zero-truncate, but stating the exact condition prevents over-application to unrelated commits.
