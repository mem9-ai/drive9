# Task: Review PR #73 (drive9)

## Objective
Perform a full logic review of GitHub PR #73, understand intent, and identify correctness risks/regressions.

## Plan
- [x] 1. Collect PR metadata and full diff from local branch (`main...feat/fuse-mount`), with GitHub API fetch attempts timing out.
- [x] 2. Map changed files to runtime flows (FUSE read/write/flush/patch lifecycle).
- [x] 3. Review each changed file for correctness, races, edge cases, and API contract mismatches.
- [x] 4. Cross-check with tests and identify coverage gaps.
- [x] 5. Produce prioritized findings (critical/high/medium), open questions, and verdict.

## Risks checked
- Inode refcount/lifecycle
- Read path efficiency and correctness
- Write -> flush lifecycle
- PatchFile correctness (dirty parts/copy semantics)
- Concurrency races and lock ordering
- Memory behavior for large-file preload

---
Status: COMPLETED
Updated: 2026-03-30

---

# Task: Review Current FUSE Mount Write-Path Changes

## Objective
Evaluate whether the current `pkg/fuse` changes are correct, performant, and robust for slow `echo > file` and `vim file` workflows against a remote drive9 server.

## Plan
- [x] 1. Inspect the current diff and the affected write/read/close lifecycle in `pkg/fuse`.
- [x] 2. Verify semantics around `Flush`, `Fsync`, `Release`, cache visibility, and crash recovery.
- [x] 3. Review tests for coverage gaps and run the relevant package tests.
- [x] 4. Produce prioritized findings with concrete design alternatives and performance recommendations.

## Risks checked
- Close-to-open consistency
- Error propagation on `close(2)` / `fsync(2)`
- Pending local-cache visibility after async upload
- Shutdown and crash-recovery durability
- Upload deduplication / backpressure / memory copies
- macOS/Linux unmount behavior and failure modes

---
Status: COMPLETED
Updated: 2026-04-10

---

# Task: Reconcile External Review for FUSE Performance Priorities

## Objective
Collect the review sent to `s3-mount:1`, compare it against the latest `pkg/fuse` implementation, and produce a refined priority list for the two product goals:
1. Single-client, interactive editing, subjectively near-local disk
2. Long-term stable, multi-client, broad workloads near-local

## Plan
- [ ] 1. Capture the response from tmux window `s3-mount:1`.
- [ ] 2. Reconcile the review with the current `shadow/journal/pending-index/commit-queue` design.
- [ ] 3. Refine the top refactor priorities for Goal A and Goal B.
- [ ] 4. Summarize the implications for whether drive9 should keep pushing its own one-process mount path.

## Risks checked
- Over-prioritizing chunking instead of writable-state semantics
- Underestimating metadata invalidation and multi-client coherence
- Confusing single-client latency goals with long-term distributed correctness
- Missing a clear cutover point where JuiceFS-like integration is the better path

---
Status: IN_PROGRESS
Updated: 2026-04-12

---

# Task: Implement P0 Local-Truth Write Path for Interactive FUSE

## Objective
Start the FUSE refactor from the highest-priority correctness and latency path:
- make new / truncating writable handles shadow-backed locally
- prevent partial-shadow/full-file commit corruption for overwrite workloads
- keep the implementation conservative where the local snapshot is not yet complete

## Plan
- [x] 1. Add the minimum shadow-store primitives needed for write-through local shadow files.
- [x] 2. Track whether an open writable handle has a safe full local shadow snapshot.
- [x] 3. Route create / O_TRUNC write paths to shadow immediately and update truncate semantics.
- [x] 4. Gate Flush/Fsync async-local staging on snapshot completeness to avoid corrupting untouched remote ranges.
- [x] 5. Add regression tests for shadow-backed writes and the new safety checks.

## Risks checked
- Partial dirty extents being mistaken for a full-file shadow snapshot
- Existing-file overwrite fast path zeroing untouched remote ranges
- Regressing `echo > file` / temp-file save / O_TRUNC write latency
- Mixing new shadow logic with the legacy write-back uploader path unsafely

---
Status: COMPLETED
Updated: 2026-04-12

---

# Task: Implement Pending-Shadow Reopen And BaseRev CAS Commit

## Objective
Land the next two refactor steps needed after the initial local-truth write path:
- reopen pending writable files from local shadow as the authoritative source
- carry `baseRev` through async commit and enforce compare-and-swap semantics remotely

## Plan
- [x] 1. Prefer pending shadow snapshots on writable reopen instead of reloading stale remote data.
- [x] 2. Persist and propagate `baseRev` in pending metadata and commit entries.
- [x] 3. Add conditional write support in client/server/backend/datastore for full-file commit CAS.
- [x] 4. Recover pending commits on mount while skipping legacy overwrite entries that have no safe `baseRev`.
- [x] 5. Add regression tests for shadow reopen, CAS success/conflict, and legacy pending recovery behavior.

## Risks checked
- Reopening a locally pending file and accidentally reading stale server content
- Async overwrite commit silently clobbering concurrent remote edits
- Retrying terminal 409 conflicts forever in the background queue
- Auto-recovering pre-CAS pending overwrites that no longer have enough conflict context

---
Status: COMPLETED
Updated: 2026-04-12

---

# Task: Add CAS To Patch And Multipart Finalize Paths

## Objective
Close the remaining overwrite hole after the initial `baseRev` work by carrying
expected revision semantics through patch uploads and multipart uploads:
- persist expected revision in upload metadata
- reject stale finalize operations instead of clobbering newer remote content
- thread the CAS contract through server/client/FUSE large-file paths

## Plan
- [x] 1. Extend `uploads` metadata/schema with persisted `expected_revision`.
- [x] 2. Add expected-revision support to v1/v2 upload initiate APIs and patch initiate API.
- [x] 3. Enforce revision checks in backend finalize for overwrite, create-if-absent, and create-race cases.
- [x] 4. Propagate `BaseRev` / create-if-absent semantics from FUSE large-file flush and streaming paths.
- [x] 5. Add regression coverage for FUSE patch/multipart request propagation and cleanup behavior.

## Risks checked
- Multipart upload overwriting a newer remote revision after a long transfer
- Patch upload copying old ranges into a file that has already advanced remotely
- Create-if-absent large upload racing with a concurrent create
- Finalize transaction failing after `CompleteMultipartUpload`, leaving stale upload metadata or orphaned blobs

---
Status: COMPLETED
Updated: 2026-04-12
