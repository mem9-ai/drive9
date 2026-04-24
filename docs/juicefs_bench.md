# JuiceFS Bench Investigation

## Context

The current issue is focused on `juicefs bench` failures against the local drive9
FUSE mount, specifically the small-file `lookup/getattr/stat` path after async commit.

The reproduction commands used were:

```bash
# terminal 1
cd /home/ubuntu/bench/
./run-drive9-server-local.sh 2>&1 | tee /home/ubuntu/bench/fuse_test/server-local.log

# terminal 2
cd /home/ubuntu/drive9/
DRIVE9_SERVER=http://127.0.0.1:9009 ./bin/drive9 mount /home/ubuntu/drive9_mount_test/ 2>&1 | tee /home/ubuntu/bench/fuse_test/fuse_mount.log

# terminal 3
cd /home/ubuntu/juicefs/
./juicefs bench /home/ubuntu/drive9_mount_test -p 1 2>&1 | tee /home/ubuntu/bench/fuse_test/juicefs_bench.log
```

## Immediate Symptom

`juicefs_bench.log` reports:

```text
2026/04/24 03:31:47.922198 juicefs[23502] <FATAL>: Failed to stat file /home/ubuntu/drive9_mount_test/__juicefs_benchmark_1777001412790918099__/smallfile.0.3: stat /home/ubuntu/drive9_mount_test/__juicefs_benchmark_1777001412790918099__/smallfile.0.3: input/output error [statFiles@bench.go:157]
```

There is also earlier noise around stats probing:

```text
2026/04/24 03:30:13.036496 juicefs[23502] <WARNING>: open stats file under mount point /home/ubuntu/drive9_mount_test: open /home/ubuntu/drive9_mount_test/.stats: input/output error [readStats@stats.go:372]
```

The fatal failure we care about is the `smallfile.0.3` stat error.

## Key Observation From FUSE Log

The FUSE mount log shows that the same small file was successfully uploaded and the
async commit callback ran:

```text
2026/04/24 03:31:23 commit queue: successfully uploaded /__juicefs_benchmark_1777001412790918099__/smallfile.0.3 (131072 bytes)
2026/04/24 03:31:23 commit queue: async commit success for /__juicefs_benchmark_1777001412790918099__/smallfile.0.3: base_rev=1 committed_rev=2 kind=1; refreshed FUSE revision state
```

This strongly suggests the small-file write itself is not the primary failure.
The observed failure happens after write/commit, during later metadata lookup/stat.

## Related Server Log Signals

The provided `server-local.log` excerpt does not show the exact failing request for
`smallfile.0.3`, but it does show that metadata requests can be unstable in this run:

1. Requests can briefly see `404` before later succeeding for new benchmark paths.
2. `HEAD /v1/fs/.stats` returned a server-side `context canceled` and surfaced as `500`:

```text
{"level":"error","msg":"server_event","event":"stat_failed","path":"/.stats","error":"context canceled"}
{"level":"info","msg":"http_request","method":"HEAD","path":"/v1/fs/.stats","status":500}
```

This matters because FUSE `Lookup` can directly depend on `StatCtx`, and current code
does not tolerate transient remote metadata failures well once local pending state has
been cleared.

## Current Code Path Analysis

### Async commit success cleanup

`pkg/fuse/commit_queue.go` removes local pending state before calling the filesystem
success callback:

```go
if cq.shadows != nil {
    cq.shadows.Remove(entry.Path)
}
if cq.index != nil {
    cq.index.Remove(entry.Path)
}
...
if cq.onSuccess != nil {
    cq.onSuccess(entry.Path, committedRev)
}
```

The callback is wired in `pkg/fuse/dat9fs.go`:

```go
cq.SetSuccessCallback(fs.applyAsyncCommittedRevision)
```

`applyAsyncCommittedRevision` updates inode revision state, but for clean handles it
also clears `livePending`:

```go
fs.livePendingMu.Lock()
if snap, ok := fs.livePending[remotePath]; ok {
    if hasDirtyHandle {
        ...
        fs.livePending[remotePath] = snap
    } else {
        delete(fs.livePending, remotePath)
    }
}
fs.livePendingMu.Unlock()
```

So after async commit success, the local protection layers for a recently written small
file can disappear quickly:

1. `shadowStore` entry removed
2. `pendingIndex` entry removed
3. `livePending` removed

### Lookup behavior after `livePending` is cleared

`pkg/fuse/dat9fs.go` implements `Lookup` like this:

```go
if live, ok := fs.getLivePending(childP); ok {
    ...
    return gofuse.OK
}

stat, err := fs.client.StatCtx(ctx, childP)
if err != nil {
    if !isNotFoundErr(err) {
        return httpToFuseStatus(err)
    }
    ...
}
```

Once `livePending` is gone, `Lookup` does not first reuse a locally known inode entry
for the same path. It immediately depends on remote `StatCtx`.

### GetAttr behavior is less likely to be the primary issue

`GetAttr` first reads the inode entry and only refreshes from the server when the entry
does not yet have a known revision:

```go
if entry == nil || nodeID == 1 || entry.IsDir || entry.Revision > 0 {
    return entry, gofuse.OK
}
```

Since `applyAsyncCommittedRevision` updates the inode revision:

```go
if ino, ok := fs.inodes.GetInode(remotePath); ok {
    fs.inodes.UpdateRevision(ino, revision)
}
```

plain `GetAttr` on an already-known inode should often succeed. A fresh path-based
lookup is more fragile because it goes back through `Lookup -> StatCtx`.

## Current Most Likely Root Cause

After adding targeted FUSE and server-side stat tracing and rerunning the benchmark, the
main root-cause hypothesis changed.

The primary confirmed issue is now:

1. `juicefs bench` issues path lookups that can be interrupted by the kernel.
2. FUSE `Lookup` calls remote `StatCtx` / HTTP `HEAD`.
3. Some of those requests are canceled before completion.
4. The server records the same request as `context canceled`.
5. FUSE maps that canceled stat request to `EIO`.
6. JuiceFS surfaces that as:

```text
input/output error
```

In short:

**The current main issue is `Lookup -> StatCtx -> context canceled -> EIO`, not the
previously suspected async-commit cleanup window.**

## Confirmed Evidence From The New Tracing

### The first `EIO` happens before any small-file async commit

From `fuse_mount.log`:

```text
2026/04/24 03:50:43 fuse trace: lookup path=/__juicefs_benchmark_1777002643146172552__
2026/04/24 03:50:43 fuse trace: lookup remote-stat-error path=/__juicefs_benchmark_1777002643146172552__ err=Head "http://127.0.0.1:9009/v1/fs/__juicefs_benchmark_1777002643146172552__": context canceled fuse_status=5 has_live_pending=false inode_missing=true
2026/04/24 03:50:43 tx 16:     5=input/output error
```

This occurs while JuiceFS is still probing the benchmark root directory itself, before
the later small-file phase. That rules out `smallfile.0.3` async-commit cleanup as the
main explanation for the first fatal class of errors.

### Server-side logs match the same canceled stat request

From `server-local.log`:

```text
{"msg":"server_stat_trace","event":"stat_failed_trace","path":"/__juicefs_benchmark_1777002643146172552__","error":"context canceled"}
{"msg":"http_request","method":"HEAD","path":"/v1/fs/__juicefs_benchmark_1777002643146172552__","status":500}
```

This confirms the request really is being canceled mid-flight, not merely misclassified
inside FUSE.

### The same pattern repeats later on small-file fresh lookups

Another traced example from `fuse_mount.log`:

```text
2026/04/24 03:51:57 fuse trace: lookup remote-stat-error path=/__juicefs_benchmark_1777002643146172552__/smallfile.0.12 err=Head "http://127.0.0.1:9009/v1/fs/__juicefs_benchmark_1777002643146172552__/smallfile.0.12": context canceled fuse_status=5 has_live_pending=false inode_missing=true
2026/04/24 03:51:57 tx 20706:     5=input/output error
```

Note the important details:

1. `has_live_pending=false`
2. `inode_missing=true`

This is a fresh path lookup for a file that is not yet represented locally, so the
failure is not evidence of a stale local inode after async commit. It is again a remote
`HEAD` canceled during lookup and translated into `EIO`.

## What The New Tracing Says About `smallfile.0.3`

The new logs did confirm that `smallfile.0.3` follows the expected async-success cleanup
path:

```text
commit queue: successfully uploaded /__juicefs_benchmark_1777002643146172552__/smallfile.0.3 (131072 bytes)
fuse trace: async-commit-success begin path=/__juicefs_benchmark_1777002643146172552__/smallfile.0.3 revision=2 had_live_pending=true inode=8 inode_rev=1 inode_size=131072
fuse trace: async-commit-success end path=/__juicefs_benchmark_1777002643146172552__/smallfile.0.3 revision=2 had_live_pending=true cleared_live_pending=true has_dirty_handle=false inode=8 inode_rev=2 inode_size=131072 notify_inode=false notify_entry=false
commit queue: async commit success for /__juicefs_benchmark_1777002643146172552__/smallfile.0.3: base_rev=1 committed_rev=2 kind=1; refreshed FUSE revision state
```

So the earlier suspicion was partially valid in that:

1. `livePending` really is cleared on async success.
2. No inode/entry notify is sent in that path.

However, in this rerun there is no direct evidence that `smallfile.0.3` itself later
failed due to that cleanup window. The stronger and better-supported failure mechanism is
the interrupted `Lookup -> StatCtx` path described above.

## Updated Code-Level Interpretation

The relevant code path in `pkg/fuse/dat9fs.go` is still:

```go
stat, err := fs.client.StatCtx(ctx, childP)
if err != nil {
    if !isNotFoundErr(err) {
        return httpToFuseStatus(err)
    }
    ...
}
```

And `fuseCtx(cancel)` converts FUSE interrupts into context cancelation:

```go
ctx, cf := context.WithTimeout(...)
go func() {
    select {
    case <-cancel:
        cf()
    case <-ctx.Done():
    }
}()
```

For errors like:

```text
Head "...": context canceled
```

`httpToFuseStatus` currently falls through to `gofuse.EIO`, because the error is not a
typed HTTP 404/403/etc. and does not match the string-based HTTP status checks.

That makes path lookups fragile under normal interrupted-probe behavior from JuiceFS.

## Why This Fits the Evidence Better Than a Write Failure

The updated explanation matches the observed facts better:

1. The first `EIO` appears before any small-file async commit is relevant.
2. The exact same request is recorded server-side as `context canceled`.
3. The FUSE trace shows that canceled remote stat is mapped to `fuse_status=5`.
4. Fresh path lookups with `inode_missing=true` also fail this way later in the run.
5. Small-file async commit cleanup is real, but not the strongest explanation for the
   observed benchmark failure in this rerun.

## Things Not Yet Proven

It is still possible that the async-success cleanup window causes additional problems for
already-created files under some timings. The current rerun simply does not make that the
best-supported primary cause.

The strongest confirmed issue now is the interrupted remote stat path during `Lookup`.

## Suggested Next Validation

The next most useful confirmation would be one of:

1. Add a focused FUSE test for `Lookup` where the cancel channel fires during `StatCtx`
   and verify current behavior maps it to `EIO`.
2. Decide whether interrupted lookup/stat requests should map to a retryable status or a
   non-fatal lookup result instead of hard `EIO`.
3. Separately, keep the async-success cleanup suspicion as a secondary investigation, but
   not as the leading explanation for this benchmark run.

## Scope Note

This note intentionally focuses on the small-file `lookup/getattr/stat` issue. The
separate `bigfile.0.0` upload-size problem observed in the same benchmark run is out of
scope for this investigation.
