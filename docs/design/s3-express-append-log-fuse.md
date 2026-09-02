---
title: Drive9 FUSE S3 Express Append Log Specification
status: final
date: 2026-09-02
---

# Drive9 FUSE S3 Express Append Log Specification

## 1. Scope

This specification owns the `mem9-ai/drive9` client and FUSE half of the
SQLite WAL append optimization. The corresponding server contract, metadata,
and S3 Express lifecycle live in `tidbcloud/fs`.

The goal is to replace the current full-WAL upload on a strict SQLite WAL
`fsync` with a request containing only the newly appended WAL tail, without
weakening `synchronous=FULL` durability.

### In scope

1. Read and cache the server's `append_log_v1` capability and content-layout
   stat header.
2. Add a typed streaming append-log client API.
3. Detect an eligible SQLite `-wal` pure extension from an immutable FUSE
   handle snapshot.
4. Use the API in the strict WAL flush path, maintain the local durability
   contract, and handle the one explicit rebase retry.
5. Add unit, FUSE, and SQLite regression coverage.

### Out of scope

1. Drive9 server, TiDB metadata, S3 Express credentials, or object lifecycle.
2. `-journal`, `-shm`, main database files, arbitrary `.wal` suffixes, and
   non-SQLite files.
3. #876's stale sibling / delayed Release payload-generation fix.
4. Multi-writer WAL coordination, direct S3 access, or durable append-op
   idempotency.

The Drive9 production estimate is **~200-300 net LoC** (tests excluded).

## 2. Compatibility contract

At mount initialization, the existing `/v1/status` warm-up caches:

```json
{
  "storage_capabilities": {
    "append_log_v1": true
  }
}
```

An absent field, a failed status request, or `false` means disabled. FUSE must
then retain its current conditional full-write behavior without probing an
unknown `?append-log` endpoint on every `fsync`.

`StatResult` parses `X-Dat9-Content-Layout: single|append_log`. A file handle
retains the last authoritative layout with its committed revision and original
size. Existing servers omit the header; that is treated as `single`.

The new typed client operation is conceptually:

```go
AppendLog(ctx, path, tail, expectedRevision, expectedSize)
    -> (revision, size, error)
```

It sends `POST /v1/fs/{path}?append-log`, `Content-Length`,
`X-Dat9-Expected-Revision`, and `X-Dat9-Expected-Size`. It parses success JSON
and preserves the server's structured `append_log_rebased`,
`append_log_conflict`, and `append_log_unsupported` errors. It never exposes
an S3 endpoint, session credential, or presigned URL.

## 3. Eligibility and snapshot

FUSE considers the append path only when all conditions hold:

1. The server capability is enabled.
2. The path is a confirmed SQLite `-wal` sidecar, not merely an arbitrary
   filename ending in `-wal`.
3. The handle is new (creation) or its authoritative layout is `append_log`.
4. The write is a strict pure extension: snapshot offset equals the committed
   original size, and no dirty range changes bytes below that offset.
5. The snapshot has an expected revision and an immutable byte range for the
   tail.

For a new WAL, the first non-empty or zero-byte strict flush uses
`expectedRevision=0` and sends the complete initial buffer as the create body.
For an existing `append_log` WAL, FUSE copies only
`[OrigSize, Dirty.Size())` into an immutable tail before it releases the handle
lock. It must not materialize or upload the preceding WAL bytes.

Any failed predicate takes the existing generic conditional full-rewrite path.
An existing `single` WAL remains on that path until server-side reset/new-file
creation establishes `append_log`.

## 4. Flush and durability behavior

The append branch is inserted before the generic PATCH/full-upload selection in
`flushHandle`. It keeps the existing same-path remote commit lock and uses the
same immutable-snapshot discipline as other synchronous upload paths.

```text
SQLite WAL fsync
  → freeze revision, committed size, dirty sequence, and tail bytes
  → AppendLog
  → server S3 append + metadata CAS
  → update FUSE committed revision/size/layout
  → clear exactly the flushed dirty generation
  → return success
```

FUSE returns success only when `AppendLog` returns a committed revision. It
then updates the inode size, read cache, handle original size, and all
same-path committed-revision bookkeeping from the server response. If a
concurrent local write advanced the dirty sequence while the RPC was in flight,
it retains that newer dirty data exactly as the existing flush paths do.

`ftruncate(0)`, non-tail writes, checkpoint reuse, a server capability change,
and unsupported layout fall back to the current conditional full rewrite. The
server preserves an existing `append_log` layout across such a rewrite; FUSE
does not choose the object bucket itself.

## 5. Error and retry policy

| Server/client result | FUSE action |
|---|---|
| success | finalize only the snapshot that was appended |
| `append_log_rebased` | re-stat, verify `append_log` and changed revision, retry the same immutable tail once |
| normal `409 append_log_conflict` | preserve dirty state and return the error; no LWW retry |
| timeout / response loss / 5xx | preserve dirty state and return an error; do not assume append succeeded |
| `append_log_unsupported` | fall back once to the existing conditional full rewrite |
| any other error | preserve dirty state and return an error |

The one rebase retry is safe because the server explicitly discarded or copied
only the metadata-visible prefix and FUSE still owns the unchanged tail
snapshot. Retrying an arbitrary conflict is not safe and is forbidden.

This feature does not alter dirty-sibling revision handling, commit queue
generation tokens, auto-resolution, or Release behavior. Those #876 fixes
remain required independently; append-log must not cause a stale queued payload
to become valid.

## 6. Reads, caches, and observability

Drive9 continues to use the normal file read API. The server is responsible
for bounded S3 Express range reads. After an append success, FUSE may seed its
read cache only when it already has the complete immutable file image; it must
not fabricate a whole-WAL cache entry from the tail alone.

Add FUSE performance counters and timing fields:

1. append-log request count, tail bytes, latency, and outcome;
2. full-rewrite fallback count and bytes for eligible WALs;
3. retry-after-rebase count; and
4. WAL fsync latency split by append versus rewrite.

The #875 workload uses these counters to prove remote-write bytes scale with
new WAL frames rather than the cumulative WAL length.

## 7. Required tests and rollout

1. Client tests for capability parsing, layout stat parsing, request headers,
   success decoding, structured conflicts, and older-server compatibility.
2. FUSE tests for first WAL creation, pure tail append, a small-to-large WAL,
   a cached `append_log` reopen, zero truncate, non-tail rewrite, unsupported
   fallback, rebase retry, and timeout preservation of dirty bytes.
3. Keep all existing #876 bounded checkpoint and stale-generation tests;
   append-log is not a substitute for their fence.
4. End-to-end #875: WAL mode, `synchronous=FULL`, default
   `wal_autocheckpoint=1000`, 1,000 individual commits, close/reopen, and
   fresh remount verification.
5. The feature remains dormant until a server advertises the capability, so
   client release may precede server configuration safely.
