---
title: Drive9 FUSE S3 Express Append Log Specification
status: draft
date: 2026-09-02
---

## 1. Scope

This specification owns the `mem9-ai/drive9` configuration, client, and FUSE
half of the append-log optimization. SQLite WAL is the initial workload and
GitHub issue #875 acceptance case, but the client mechanism applies to any path
explicitly declared by the mount operator. The corresponding server contract,
metadata, and S3 Express lifecycle live in `tidbcloud/fs`.

The goal is to replace the current full-file upload on a strict append-log
`fsync` with a request containing only the newly appended tail, without
weakening SQLite `synchronous=FULL` durability.

### In scope

1. Add repeatable mount path-pattern configuration for operator-declared
   append-log files.
2. Read and cache the server's `append_log_v1` capability and parse its
   content-layout stat header.
3. Add typed streaming client APIs for tail append and one server-proxied
   conditional full-body PUT, and preserve structured error codes.
4. Prove a strict pure extension from immutable per-handle mutation state.
5. Use the append API for pure extensions and the layout-aware conditional
   full-body PUT for resets and non-tail rewrites in every strict synchronous
   upload path, including zero-byte creation and the one explicit rebase retry.
6. Add configuration, client, FUSE, and SQLite regression coverage.

### Out of scope

1. Drive9 server, TiDB metadata, S3 Express credentials, or object lifecycle.
2. Automatic file-type discovery, including SQLite WAL header inspection.
3. Files not matched by operator configuration and automatic conversion of an
   existing `single` file to `append_log`.
4. #876's stale sibling / delayed Release payload-generation fix.
5. Multi-writer coordination, direct S3 access, durable append-operation
   idempotency, or multipart rebase.

The Drive9 production estimate is **550-800 net LoC** (tests excluded). The
increase from the original estimate accounts for mount configuration plumbing,
exact per-handle mutation tracking, structured error handling, all synchronous
upload entry points, layout-aware full-rewrite routing, fallback-loop
prevention, and observability. It does not add a persistent schema, background
worker, or second state machine.

## 2. Configuration and compatibility contract

### 2.1 Operator declaration

The FUSE mount accepts a repeatable path-pattern flag:

```text
--append-log <pattern>
```

The equivalent environment variable is:

```text
DRIVE9_MOUNT_APPEND_LOG_PATTERNS
```

The environment value contains one pattern per line. Environment and CLI
patterns are additive. The CLI consumes and validates the environment value,
then snapshots it into `--append-log=...` arguments so background workers,
supervised restarts, and later process adoption retain the original mount
contract. `MountOptions.AppendLogPatterns []string` carries the resolved list
into FUSE.

Pattern syntax, validation, and canonical matching reuse the existing
`--local-only` / `--remote-only` `pathfilter` contract. Patterns match the
canonical path inside the mounted filesystem, not the host mountpoint or the
server path after `RemoteRoot` translation. The option is valid only for a
Drive9-backed FUSE mount, not WebDAV or direct object-store mounts. A match
does not override local-only, remote-only, layer, or read-only routing; only a
remote-persistent writable file can use the append-log API.

An empty pattern list disables the client append-log path. A matching pattern
is the authoritative operator declaration: FUSE does not require a `-wal`
suffix, an open sibling database, a WAL header, or a content-layout HEAD before
attempting append-log.

### 2.2 Server capability

At mount initialization, the existing `/v1/status` warm-up caches:

```json
{
  "storage_capabilities": {
    "append_log_v1": true
  }
}
```

An absent field, a failed status request, or `false` means disabled for that
mount. FUSE retains its current conditional full-write behavior and must not
probe an unknown `?append-log` endpoint on every `fsync`. Tail append and new
append-log creation therefore require both an operator pattern match and the
cached server capability.

Capability absence does not authorize PATCH/V2 for an existing physical
`append_log` target. A configured existing file that requires a full rewrite
still uses the layout-aware routing below; if stat reports `append_log`, FUSE
uses conditional full-body PUT. This remains true after a server capability
change.

`StatResult` parses `X-Dat9-Content-Layout: single|append_log`. The field is not
an append eligibility gate: a missing, unknown, or `single` value does not
override an explicit `--append-log` match for a pure extension. FUSE does not
add a HEAD during `Open` solely to obtain content layout. Existing stat calls
continue to populate revision, size, storage type, and layout normally.

Layout is consulted only to select the correct full-rewrite transport when an
existing configured file receives a truncate or non-tail write. The server
requires an `append_log` full rewrite to use its ordinary server-proxied
conditional PUT, while an existing `single` file retains the current generic
write path. If the handle has not already learned its layout from an append
result or earlier stat, FUSE performs one flush-time stat. It does not move
that lookup into `Open`.

### 2.3 Client API and domain errors

The typed client operation is:

```go
AppendLog(ctx, path, tail, tailSize, expectedRevision, expectedSize)
    -> (AppendLogResult{Revision, Size}, error)
```

`tail` is an `io.Reader`; the client sets the explicit `Content-Length` from
`tailSize` and does not buffer the whole request. It sends
`POST /v1/fs/{path}?append-log`, `X-Dat9-Expected-Revision`, and
`X-Dat9-Expected-Size`. All three sizes/revisions must be non-negative.

`StatusError` gains the server's optional machine-readable `Code`. The client
preserves at least these codes as typed constants:

1. `append_log_rebased`;
2. `append_log_conflict`;
3. `append_log_unsupported`; and
4. `append_log_too_large`.

On success, the client requires a positive committed revision and verifies
that the returned size equals `expectedSize + tailSize`. A malformed success
response is a protocol error. The API never exposes an S3 endpoint, session
credential, or presigned URL.

The narrow full-rewrite operation is:

```go
WriteServerStreamConditional(ctx, path, body, size, expectedRevision)
    -> (revision, error)
```

It sends exactly one ordinary HTTP PUT to the Drive9 server with an explicit
content length and `X-Dat9-Expected-Revision`. Unlike
`WriteStreamConditional`, it never selects PATCH, V2 multipart, direct-upload,
or legacy `POST ?append`, and it never follows an upload-plan fallback. This
lets FUSE stream a complete immutable snapshot without selecting an upload
plan.

An existing `append_log` full rewrite uses this operation even when the body
exceeds the negotiated inline threshold. The server owns the target-layout
lookup and streams the ordinary PUT body to a new S3 Express object. The client
requires the response to contain a positive committed revision; the committed
size is the immutable request size.

## 3. Eligibility and immutable snapshot

FUSE considers the append path only when all conditions hold:

1. The canonical path matches an operator `--append-log` pattern.
2. The cached server capability is enabled.
3. The handle has not received `append_log_unsupported` during its current
   lifetime.
4. The handle is new, or the current dirty generation is a strict pure
   extension of an existing file.
5. The snapshot has a non-negative expected revision, committed original size,
   dirty sequence, and immutable tail byte range.

Content layout is deliberately absent from this list. For a matching existing
file, FUSE attempts `AppendLog` without an extra open-time HEAD. If the actual
server layout is `single`, the server returns `append_log_unsupported`; FUSE
falls back to a full rewrite and suppresses further append attempts for that
handle. Reopening the file creates a new handle and may probe once again.
Neither the failed attempt nor the fallback converts the existing `single`
file. Only append-log creation of a missing matching path establishes the
layout.

Each configured handle keeps an observed physical layout of `unknown`,
`single`, or `append_log` solely for full-rewrite routing. A normal stat can
populate it; successful AppendLog or append-log creation sets `append_log`;
`append_log_unsupported` from the AppendLog endpoint disables further append
attempts for this handle but does not by itself prove `single`, because the
backend may have become unavailable. The fallback performs an on-demand stat
when layout is still unknown. Observed layout never overrides the operator
pattern, except that the same handle suppresses attempts after an explicit
unsupported response. Layout is always associated with the committed revision
and size that established it.

### 3.1 Pure-extension proof

Pure-extension tracking belongs to `FileHandle`, not `WriteBuffer`, so cache
preload and lazy part materialization are not mistaken for user mutations.
For an existing file, the handle carries a monotonic append-safe flag for the
current dirty generation:

1. At a clean committed baseline, the flag starts true and the baseline size
   is `OrigSize`.
2. The first non-empty user write remains append-safe only when its offset and
   the pre-write logical size both equal `OrigSize`.
3. Every later non-empty write remains append-safe only when its offset equals
   the pre-write logical size. Gaps, overlapping writes, and back-writes make
   the flag false for the rest of that dirty generation.
4. Any truncate of an existing file, including `O_TRUNC`, truncate-to-zero,
   shrink, or growth, makes the flag false. A later write cannot restore it
   before a successful generic rewrite establishes a new clean baseline.
5. Internal buffer loads do not affect the flag.

For a new matching file, the first non-empty or zero-byte strict flush uses
`expectedRevision=0`, `expectedSize=0`, and sends the complete immutable file
image as the create body. Because no committed prefix exists, random writes,
holes, or truncates before that first commit do not disqualify append-log
creation as long as the complete image can be materialized. For an existing
file, FUSE copies only
`[OrigSize, Dirty.Size())` into an immutable tail before releasing the handle
lock. It must not materialize or upload the preceding bytes.

Any failed append predicate enters full-rewrite routing. An unmatched or known
`single` path uses the existing generic conditional path; a configured existing
path resolves physical layout first so `append_log` cannot leak into PATCH/V2.
After a successful rewrite, the handle adopts the committed revision and size
as a new clean baseline, so a later independent pure extension may attempt
append-log again unless this handle was disabled by `append_log_unsupported`.

If a concurrent user write advances `DirtySeq` while AppendLog is in flight,
the flushed generation is not cleared. After success, `OrigSize` advances only
to the server-returned committed size. If all concurrent mutations preserved
the append-safe flag, the next flush may append the remaining suffix from that
new base; otherwise it must perform a full rewrite.

## 4. Flush and durability behavior

Configured append-log paths participate in the strict remote-sync behavior
needed by the SQLite WAL workload, even when the mount otherwise uses
interactive durability. This does not remove the existing strict treatment of
SQLite persistent journals that are not configured for append-log.

Append-log routing occurs before every existing upload shortcut: empty create,
streaming-uploader finish/upload-all, direct PUT, PATCH, V2 multipart,
direct-upload, and legacy append-plan selection. This ordering applies in every
synchronous remote-commit entry point, including `flushHandle`,
`syncHandleToRemoteLocked`, strict ShadowSpill upload, and their Release
fallbacks. A configured existing handle, or a capability-enabled configured new
handle, must not start or reuse a generic multipart `StreamUploader` before
this decision.

The implementation may use one narrow shared routing/finalization helper, but
it does not introduce a second commit state machine. It retains the existing
same-path remote commit lock, path/dirty-generation fences, and immutable
snapshot discipline. A ShadowStore pin protects lifetime, not content: active
shadow writes can still mutate the same file descriptor. Therefore neither an
active path nor a pinned active generation may be used directly as an HTTP
request body after releasing the handle lock.

Before unlocking, FUSE creates an independent immutable request snapshot from
the authoritative resident `WriteBuffer` or ShadowStore contents. Small tails
and full images may use a copied byte slice. Larger bodies use a dedicated
temporary snapshot file under the mount cache directory and stream it through
the client operation. The snapshot file is not reused by writers and is removed
after success or failure. Missing remote-backed ranges must be loaded before a
full-image snapshot is accepted; failure to prove a complete image preserves
dirty state and returns an error. A generic streaming uploader must not have
evicted bytes needed to construct the snapshot.

```text
strict fsync/flush
  -> freeze path, expected revision, committed size, dirty sequence,
     append-safe state, and tail bytes
  -> AppendLog
  -> server S3 append + metadata CAS
  -> validate committed revision and size
  -> update FUSE committed revision and size
  -> clear exactly the flushed dirty generation
  -> return success
```

FUSE returns success only when `AppendLog` returns a valid committed revision
and size. It updates inode size, handle original size, and all same-path
committed-revision bookkeeping from the server response while preserving
newer dirty data. An append success establishes that the committed server
layout is `append_log`, but FUSE does not require a persistent layout cache for
future routing.

Zero-byte append-log creation must take this branch before any existing
`CreateFileCtx` shortcut. Non-tail writes, truncates, and per-handle unsupported
state enter layout-aware full-rewrite selection. Capability absence sends a
configured new file through the existing generic create path, but a configured
existing file must first resolve layout: `append_log` uses conditional
full-body PUT and `single` uses the generic path. Unmatched paths retain current
behavior. The server preserves an existing `append_log` layout across a full
rewrite; FUSE does not choose the object bucket itself. An existing `single`
target remains `single` after fallback.

For a configured existing file that needs a full rewrite, transport selection
uses per-handle observed layout without affecting later append eligibility:

1. A prior successful AppendLog proves `append_log`; send the complete
   immutable image through the ordinary server-proxied conditional PUT, never
   PATCH, V2 multipart, legacy append, or a presigned object URL.
2. A prior `append_log_unsupported` disables further AppendLog calls for this
   handle but does not choose the rewrite transport. Resolve layout if still
   unknown: `single` uses the generic path and `append_log` uses full-body PUT.
3. If layout is still unknown, stat once at flush time. `append_log` selects
   the server-proxied PUT and `single` selects the generic path. A missing or
   unknown layout is a protocol error: preserve dirty state rather than guess
   a transport.

The flush-time stat is required only for a configured existing file whose first
mutation is not appendable. A pure extension still attempts AppendLog directly
without a layout stat. The routing stat must report the handle's existing
`BaseRev` and `OrigSize`; a changed/unknown revision or changed size is a
conflict, not permission to adopt a newer base or perform LWW. FUSE preserves
dirty state and does not issue a full PUT or upload plan in that case. A cached
layout is usable only while its associated revision and size still equal that
same committed baseline.

### 4.1 Layout-aware full-body PUT

For `ftruncate(0)`, any other truncate, non-tail writes, and SQLite checkpoint
reuse of an existing `append_log` file, FUSE freezes the complete new file
image, path, snapshot size, expected revision, dirty sequence, and observed
`append_log` layout before releasing the handle lock. It acquires the same-path
remote commit lock before the network call and sends one conditional full-body
PUT. The selection happens before threshold-based PATCH/V2 routing, so a body
larger than the inline threshold still takes this PUT.

```text
truncate / non-tail write / checkpoint reuse
  -> prove existing physical layout is append_log
  -> freeze complete immutable image + revision + size + dirty sequence
  -> conditional full-body PUT
  -> server writes a new Express object and metadata-CASes its reference
  -> validate positive committed revision
  -> publish revision + snapshot size + append_log layout
  -> clear only the matching dirty generation
```

On success, while preserving the existing remote-commit ordering:

1. Record the server-returned revision before releasing the same-path remote
   commit lock.
2. Associate that revision with the immutable snapshot size and
   `content_layout=append_log`; refresh the inode and eligible same-path handle
   bookkeeping from those committed values.
3. If the handle path is unchanged and `DirtySeq` still equals the snapshot
   sequence, clear exactly that dirty generation and set `OrigSize` to the
   committed snapshot size.
4. If the path was retargeted or a concurrent write advanced `DirtySeq`, do not
   clear the live dirty buffer. Retain the newer data, but advance the committed
   base revision and `OrigSize` only to the full-PUT snapshot.
5. Because the PUT snapshot is a complete immutable image, seed the read cache
   under its committed revision when normal cache-size rules permit. Never use
   the post-unlock live buffer as that image.

### 4.2 One-way fallback and loop prevention

`append_log_unsupported` has phase-specific meaning:

1. From `POST ?append-log`, it means the existing target is `single` or the
   append backend is unavailable. Mark append unsupported for this handle and
   resolve the physical layout if necessary. `single` falls back once to the
   existing generic conditional full-write path; `append_log` falls back once
   to conditional full-body PUT. An existing `single` file remains `single`.
2. From PATCH, V2, direct-upload, or legacy append-plan selection, it means the
   target is actually `append_log`. Mark its observed layout `append_log` and
   reroute the same immutable full snapshot once to conditional full-body PUT.

The second case must never recurse into `flushHandle` with unchanged routing
inputs or retry the same PATCH/V2/direct-upload/legacy plan. After the one-way
transition to full-body PUT, success finalizes that snapshot; any PUT error is
returned with dirty state preserved. This bounds fallback depth at one and
prevents upload-plan loops.

The first case originally froze only an append tail. Before constructing a
full-body fallback, FUSE reacquires the handle in normal lock order and requires
the original path and `DirtySeq` to remain unchanged. It then materializes and
freezes a complete image before the fallback request. If the path was retargeted,
the handle was unlinked, the dirty sequence advanced, or the full image cannot
be materialized, FUSE does not issue a same-call fallback; it preserves dirty
state and lets the next flush use the now-disabled append state.

## 5. Error and retry policy

<!-- markdownlint-disable MD013 -->

| Server/client result | FUSE action |
| --- | --- |
| AppendLog success | Validate revision/size and finalize only the appended snapshot. |
| conditional full-body PUT success | Require a positive revision; publish the snapshot size and `append_log` layout, then clear only the matching dirty generation. |
| `409 append_log_rebased` | Re-stat, require a positive changed revision and the original expected size, then retry the same immutable tail once with the new revision. Content-layout headers do not override the operator match. |
| `409 append_log_conflict` or full-PUT revision conflict | Preserve dirty state and return the error; never perform an LWW retry. |
| timeout / response loss / 5xx | Preserve dirty state and return an error; do not assume append succeeded. |
| `400 append_log_unsupported` from AppendLog | Disable append for this handle and select exactly one layout-correct full rewrite. |
| `400 append_log_unsupported` from PATCH/V2/direct-upload/legacy plan | Reroute the same immutable full snapshot once to conditional full-body PUT; never retry the rejected plan. |
| `413 append_log_too_large` from AppendLog or full-body PUT | Preserve dirty state, retain `append_log`, and return `EFBIG`; never retry through V2/PATCH or migrate to `single`. |
| malformed success or any other error | Preserve dirty state and return an error. |

<!-- markdownlint-enable MD013 -->

The one rebase retry is safe only when re-stat reports the same committed size
as the original snapshot and a different positive revision. The server
explicitly discarded or copied only that metadata-visible prefix, and FUSE
still owns the unchanged tail snapshot. A changed size, unchanged/unknown
revision, stat failure, or second rebase fails closed without retrying again.
`append_log_too_large` and all ordinary conflicts are terminal for that flush;
neither enters rebase, LWW, or upload-plan fallback.

This feature does not alter dirty-sibling revision handling, commit queue
generation tokens, auto-resolution, or Release payload behavior. Those #876
fixes remain required independently; append-log must not cause a stale queued
payload to become valid.

## 6. Reads, caches, and observability

Drive9 continues to use the normal file read API. The server is responsible
for bounded S3 Express range reads. Open does not issue a content-layout HEAD
for configured paths. An on-demand stat is allowed only for full-rewrite
transport selection or the explicit rebase retry.

After an append success, FUSE may seed its read cache only when it already has
the complete immutable file image. It must not fabricate a whole-file cache
entry from the tail alone. A cached reopen may reuse revision-matched data and
still attempt append-log because operator configuration, not cached layout,
controls eligibility.

Add FUSE performance counters and timing fields for:

1. append-log request count, tail bytes, latency, and outcome code;
2. full-rewrite fallback count and bytes for configured append-log paths;
3. retry-after-rebase count; and
4. strict fsync latency split by append versus rewrite.

The #875 workload uses these counters to prove remote-write bytes scale with
new WAL frames rather than cumulative WAL length.

## 7. Required tests and rollout

1. Configuration tests cover repeatable `--append-log`, additive newline env
   patterns, validation, canonical matching, background/supervisor argv
   snapshotting, default-disabled behavior, and rejection outside FUSE mode.
2. Client tests cover capability parsing, layout stat parsing, request headers
   and explicit content length, success validation, structured error codes,
   conditional full-body PUT revision decoding, `append_log_too_large`
   response classification, and older-server compatibility.
3. FUSE tests cover matched and unmatched paths without filename assumptions,
   no append-specific HEAD on cached reopen, first non-empty and zero-byte
   creation including a randomly assembled initial image, pure tail append,
   and small-to-large growth.
4. Keep all existing #876 bounded checkpoint and stale-generation tests;
   append-log is not a substitute for their fence.
5. Full-rewrite tests separately cover truncate-to-zero, non-tail writes,
   checkpoint reuse, and an existing `append_log` body above the inline
   threshold. Each asserts one complete immutable request body, the expected
   revision header, no PATCH/V2/direct-upload/legacy plan, and successful
   revision/size/layout/dirty-generation finalization both with and without a
   concurrent newer write.
6. Fallback tests cover AppendLog unsupported on an existing `single`,
   per-handle suppression, reopen reprobe, and PATCH/V2 unsupported on an
   `append_log` target. They assert a one-way transition to the correct
   full-write transport and prove that the rejected upload-plan endpoint is
   never called twice. A concurrent-write case proves AppendLog unsupported
   does not build a full-body fallback from an advanced dirty generation.
7. Failure tests cover rebase retry validation, timeout/response-loss dirty
   preservation, normal conflict, routing-stat revision/size drift, and
   `append_log_too_large` from both append and full-body PUT. Drift tests assert
   no PUT/plan request; too-large tests assert no V2/PATCH retry and no layout
   downgrade.
8. Capability-change tests prove a configured existing `append_log` file still
   uses conditional full-body PUT, never PATCH/V2, when tail append is disabled.
9. Snapshot tests mutate the live `WriteBuffer` and active ShadowStore after
   the HTTP request starts and prove the uploaded tail/full body remains the
   frozen generation. They cover temporary-snapshot cleanup on success,
   timeout, and server error, plus failure when a complete image cannot be
   materialized.
10. End-to-end #875 runs with an explicit `--append-log` pattern: WAL mode,
   `synchronous=FULL`, default
   `wal_autocheckpoint=1000`, 1,000 individual commits, close/reopen, and
   fresh remount verification.
11. New append-log creation and tail append remain dormant unless both the
    mount configuration matches and the server advertises the capability, so
    client release may precede server configuration safely.

## 8. Backlog

Add automatic SQLite WAL discovery by validating the WAL header. Dynamic
discovery is additive with explicit `--append-log` patterns and must preserve
the same capability, pure-extension, snapshot, and fallback gates. It is not
implemented in this release.
