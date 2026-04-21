# FUSE create -> write -> read investigation

## Context

We investigated a failure in `e2e/fuse-smoke-test.sh` where the writable mount path
returned:

```text
FAIL create/read via mount (want=create-<ts> got=)
```

and later, after introducing partial fixes, sometimes:

```text
cat .../alpha/text.txt: Input/output error
```

The target environment was `drive9-server-local` running against the current
`server-metadata-authority` branch.

## Confirmed initial symptoms

### CLI smoke

- `e2e/cli-smoke-test.sh` passed after clearing stale multipart upload reservations.
- The upload boundary failure (`507`) was environmental quota pollution, not a code
  regression.

### FUSE smoke

- `e2e/fuse-smoke-test.sh` consistently failed on create -> read through the mount.
- Remote reads (`drive9 fs cat`) succeeded, which initially suggested the problem
  was local mount consistency rather than server write failure.

## What we checked first

### server logs

Once the correct realtime log path was fixed, `server-local.log` confirmed the
server was running the current branch:

- `git_branch = server-metadata-authority`
- `git_hash = 5ffcfa16cb179acb307c84b9fdc76c0492c11845`

Earlier confusion came from reading an old log file from a previous branch.

### smoke script assertions

The FUSE smoke script contained two misleading rename assertions that expected
empty string content after an earlier truncate. Those assertions were clarified,
not semantically changed.

Committed separately as:

- `d9f1baa` `tests: clarify fuse rename assertions`

## Directions tried and their results

### 1. Treat create as a local-cache/direct-io issue

#### Attempt

- Changed `Create()` to return `FOPEN_DIRECT_IO` instead of `FOPEN_KEEP_CACHE`.

#### Result

- No improvement.
- `fuse-smoke-test.sh` still failed at create -> read.

#### Conclusion

- The failure was not fixed by simply bypassing kernel cache for newly created files.

### 2. Add pending/shadow/writeback preload on read/open

#### Attempt

- Added logic to preload newly opened read handles from:
  - `pendingIndex + shadowStore`
  - `writeBack`

#### Result

- No improvement in the real smoke test.

#### Conclusion

- The failure was not just a missing `Read()` fallback to pending local data.

### 3. Add JuiceFS-style read-before-flush barrier

#### Motivation

We inspected `/home/ubuntu/juicefs` to compare behavior.

Key finding from JuiceFS:

- It keeps local writer state per inode.
- `Read` forces pending writes for that inode to flush before the reader consults
  metadata-backed content.

This suggested trying a similar read barrier in drive9.

#### Attempt

- Added a `flushPendingPath(...)` helper.
- Called it from `Read()` before remote reads.

#### Result

- No improvement in the real smoke test.

#### Conclusion

- In the failing real-mount path, the operation did not appear to reach the
  userland `Read()` path in the way our focused tests did.

### 4. Investigate upstream issue 265 and align to it

We read GitHub issue 265:

- `FUSE can return EIO for newly created files during JuiceFS bench`

Important lesson from issue 265:

- `Lookup()` only consulted:
  - `pendingIndex`
  - `writeBack`
  - remote `Stat/List`
- It did not consider live open-handle / dirty state for just-created files.

The issue argued that the real missing source of truth was live pending state,
not just durable pending cache state.

#### Working hypothesis

- Fix create/read visibility by introducing a live pending layer visible to:
  - `Lookup()`
  - `GetAttr()`
  - `Open()/Read()`
  - `Unlink()`

### 5. Introduce `livePending`

#### Attempt

Implemented an in-memory `livePending` map in `pkg/fuse/dat9fs.go`.

It records, for live local write sessions:

- `Path`
- `Ino`
- `Size`
- `Mtime`
- `IsNew`
- `BaseRev`
- `HasShadow`
- eventually, a materialized `Data` snapshot when available

Hooked this into:

- `Create()`
- `Write()`
- `Lookup()`
- `GetAttr()`
- `Open()`
- `Unlink()`
- `Rename()`

#### Result

- Focused tests started to pass.
- Real smoke still failed.

#### Important intermediate observation

Once `livePending` was active, behavior changed from "empty read" to explicit
`EIO`, which meant the fix direction was affecting the real path but was still
incomplete.

### 6. Reuse dirty handle directly across opens

#### Attempt

- Tried reusing the existing writable `FileHandle` for later opens of the same
  path while live pending state existed.

#### Result

- Still produced `EIO`.

#### Conclusion

- Direct handle reuse is not a stable solution across real mount opens.
- The problem is not simply "find the old handle".

### 7. Build stable local snapshot for new opens

#### Attempt

- Instead of reusing the original writable handle, built a read-only snapshot
  view from either:
  - shadow/writeback state, or
  - a materialized dirty buffer snapshot
- Eventually stored `Data` directly inside `livePending` so the next `Open()`
  could construct a local stable read view.

#### Result

- All focused tests below passed.
- Real smoke still failed.

## Focused tests added during investigation

These tests now exist and pass:

### `pkg/fuse/writeback_test.go`

1. `TestLookupAndOpenReadUseLivePendingForNewFile`
   - Verifies create -> write -> lookup -> open -> read for a live-pending file.

2. `TestGetAttrUsesLivePendingForNewFile`
   - Verifies `GetAttr()` sees live pending size.

3. `TestUnlinkPendingNewFileSkipsRemoteDelete`
   - Verifies a still-local pending-new file does not trigger remote delete.

4. `TestCreateEntryTracksLivePendingSizeAfterWrite`
   - Verifies the inode entry and live pending state track written size after create.

5. `TestLookupReusesLivePendingInode`
   - Verifies the same path reuses the same inode while live pending is active.

These tests demonstrated that the pure `pkg/fuse` logic had improved, but they
did not eliminate the real mounted smoke failure.

## Real mount integration test added

To narrow the gap between direct callback tests and the real mounted behavior,
we added:

### `pkg/fuse/mount_integration_test.go`

- `TestMountCreateWriteReadPendingNew`

This test:

- starts a real `httptest.Server`
- calls `pkg/fuse.Mount(...)`
- writes through the mounted filesystem with `os.WriteFile`
- reads back using `os.ReadFile`
- removes the file

### Current state of the integration test

- We later expanded it into a shared helper plus two scenarios:
  - `TestMountCreateWriteReadPendingNew`
  - `TestMountNestedCreateWriteReadPendingNew`
- Both scenarios passed functionally at one point, including the nested
  `root/alpha/text.txt` shape that more closely matches the smoke failure.
- However, while trying to tighten teardown / unmount timing, the integration
  test base became unstable again and regressed into lifecycle failures such as:
  - `UnmountForTest: exit status 1`
  - `mount did not exit within cleanup timeout`
  - `list dir plus failed for /: ... connect: connection refused`
  - tempdir cleanup `input/output error`
- So the integration test is still valuable as a direction, but it is not yet a
  clean, reliable long-term base. The current state should be treated as
  experimental rather than final.

This is still useful because it proves a smaller real-mount `create -> write -> read`
 flow can succeed even while the larger smoke test still fails.

## Current workspace triage

The current worktree contains three categories of changes related to this investigation.

### Should keep

1. `e2e/fuse-smoke-test.sh`
   - The separate committed wording-only fix is already preserved in commit
     `d9f1baa`.
   - The additional mount-log capture support via `FUSE_MOUNT_LOG_DIR` is worth
     keeping because it materially improved real-mount diagnosis.

2. `docs/investigate.md`
   - This document is the running record of what was tried, what failed, and
     what remains unclear.

3. Focused regression tests in `pkg/fuse/writeback_test.go`
   - The added tests for live pending visibility / unlink / inode reuse are all
     useful regression protection and passed during the investigation.

### Should revert

1. `pkg/fuse/mount_integration_test.go`
   - Not because the direction is wrong, but because the current file is in a
     regressed state after repeated teardown experiments. It should either be
     repaired deliberately in a dedicated pass or reverted to a previously
     passing revision before further use.

2. Any temporary trace logging in `pkg/fuse/dat9fs.go`
   - Logging under `traceFusePath(...)` was useful for diagnosis, but it should
     not remain indefinitely once the next concrete repair direction is chosen.

### Experimental / reference only

1. The broader `livePending` changes in `pkg/fuse/dat9fs.go` and supporting
   structures in `pkg/fuse/handle.go`
   - These changes are not known-good end-to-end yet.
   - They are useful as evidence and as a partial implementation direction,
     especially because the focused tests now pass.
   - But they should still be treated as experimental until either:
     - the real mount integration test is restabilized, and
     - `e2e/fuse-smoke-test.sh` is brought back to green.

## Most useful logs gathered

### Mount trace logs

We modified `e2e/fuse-smoke-test.sh` so the mount process writes logs to a file
in a stable directory via `FUSE_MOUNT_LOG_DIR`.

This is worth keeping because it makes real callback ordering inspectable.

### Key mount-trace finding

For the failing path `/fuse-e2e-.../alpha/text.txt`, we observed:

1. first lookup: remote not found
2. create: inode allocated, entry returned with size 0
3. second lookup: live pending visible with size 17
4. later, another entry for the same path could still appear as size 0 in real mount logs

That suggested a mismatch between:

- what `Create()` initially exposed to the kernel
- what later `Lookup()`/live-pending state knew about the file

### Server logs

Server logs consistently showed the same pattern:

1. `create_ok`
2. early `read_ok bytes=0`
3. later `write_ok bytes=17`
4. later `read_ok bytes=17`

This showed that the server was behaving consistently: the problem window was in
the local mount's view of a just-created file before durable content was visible.

## Current best understanding

At this point, the investigation indicates:

1. The original issue is not a simple server-side write failure.
2. A pure `pkg/fuse` logic fix is partially in place and verified by focused tests.
3. Real mounted behavior still diverges from focused tests.
4. The remaining gap likely involves real go-fuse/kernel lifecycle behavior:
   - create-returned entry state vs later lookup/open behavior
   - callback ordering in the mounted environment
   - mount teardown / pending async work timing

## Things that clearly helped

- Reading issue 265 before continuing repairs.
- Separating focused tests from full smoke tests.
- Adding a real mounted integration test.
- Keeping mount logs in a deterministic file path.
- Introducing `livePending` as a first-class visibility source.

## Things that did not solve the problem on their own

- Switching `Create()` to `FOPEN_DIRECT_IO`
- Read-time flush before remote read
- Simple shadow/writeback preload alone
- Reusing a writable dirty handle directly for later opens

## Recommended next steps

1. Keep the focused tests as regression protection.
2. Keep the real mount integration test and improve its teardown stability.
3. Use the integration test, not the full smoke test, as the next debugging loop.
4. Investigate the exact kernel/go-fuse sequence around:
   - the initial `Create()` returned `EntryOut`
   - subsequent opens/lookups of the same path in real mount mode
5. Only after the integration test is fully stable should we return to the full
   `e2e/fuse-smoke-test.sh` loop.
