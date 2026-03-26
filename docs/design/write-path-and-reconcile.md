# RFC: dat9 Write Path and Reconcile

## 1. Goal

This RFC defines how dat9 handles writes, uploads, commit points, asynchronous cleanup, and reconcile.

It covers both:

- small-file writes into `db9`
- large-file direct upload into `S3`

## 2. Non-goals

This RFC does not define:

- full user-facing API contracts
- full queue backend schema
- complete retrieval semantics

This RFC also does not require dat9 to expose all final upload and repair behaviors in the first release. It defines the target write discipline and the minimum implementation guidance needed now.

## 3. Definitions

- **commit point**: the point at which a resource write becomes accepted by the system
- **reconcile**: background logic that compares actual state with expected authoritative state and repairs drift
- **orphan object**: an object present in storage without valid committed metadata state
- **outbox / compensation marker**: durable metadata used to ensure downstream work can be retried or repaired
- **file**: the user-visible path-addressable item
- **logical object**: the internal content identity behind one or more file paths
- **derived artifact**: any output generated from resource processing, whether user-visible or internal

## 4. Current Implementation Target

### 4.1 P0 / P1 write contract

For the current implementation phase, dat9 should guarantee at least:

- every write has one explicit commit point
- small-file writes commit metadata and content together in `db9`
- large-file writes use direct `S3` upload plus an explicit completion step
- missing downstream async work can be detected and repaired by reconcile
- orphaned uploads or objects can be discovered and cleaned later

### 4.2 Practical implementation guidance

The current implementation does not need a heavy distributed transaction protocol. It does need clear sequencing.

For example:

- small files may commit content, metadata, and version state in one tenant-local transaction
- large files may commit in two phases: upload bytes first, then confirm metadata/version state
- downstream async derivation may be triggered after commit, but must be repairable if submission fails

## 5. Design

### 5.1 Small-file write path

For small files:

- content is written into `db9`
- metadata and version state are committed in the tenant-local state plane
- minimum visibility may be established synchronously
- downstream semantic derivation may continue asynchronously

A typical small-file path should have one clear commit transaction, followed by async follow-up work.

Representative create sequence:

```text
client              dat9 server                tenant-local state           async runtime
  | PUT small file      |                               |                         |
  |-------------------->|                               |                         |
  |                     | BEGIN                         |                         |
  |                     |------------------------------>|                         |
  |                     | write content + metadata      |                         |
  |                     | advance version state         |                         |
  |                     | enqueue task or write         |                         |
  |                     | durable reconcile marker      |                         |
  |                     | COMMIT                        |                         |
  |                     |------------------------------>|                         |
  |<--------------------| 200/201 + committed revision  |                         |
  |                     |                                                       process later
```

Representative overwrite sequence:

```text
client              dat9 server                tenant-local state           cleanup/reconcile
  | PUT overwrite       |                               |                         |
  | If-Match: r7        |                               |                         |
  |-------------------->|                               |                         |
  |                     | BEGIN                         |                         |
  |                     |------------------------------>|                         |
  |                     | resolve and lock path         |                         |
  |                     | compare revision / policy     |                         |
  |                     | write new content             |                         |
  |                     | advance version to r8         |                         |
  |                     | enqueue task or marker        |                         |
  |                     | COMMIT                        |                         |
  |                     |------------------------------>|                         |
  |<--------------------| 200/201 + ETag: r8           |                         |
  |                     |                                                   old derived state
  |                     |                                                   is repaired async
```

If the overwrite precondition fails, the server must return conflict or precondition failure before any new committed version is exposed.

### 5.2 Large-file write path

For large files:

- the server returns `202` and presigned upload instructions
- the client uploads bytes directly to `S3`
- a completion step finalizes metadata, version state, and post-write work

A typical large-file path should not treat upload success alone as final system commit. Final commit happens when metadata/version state is confirmed.

Representative completion sequence:

```text
1. client uploads parts directly to object storage
2. client calls /complete
3. server finalizes multipart upload or verifies uploaded object state
4. server commits metadata/version state
5. server creates or confirms the logical path binding
6. server marks upload state as completed
7. commit succeeds -> file becomes confirmed
8. path conflict or validation failure -> commit fails explicitly
```

Representative staged upload flow:

```text
client              dat9 server                object storage               tenant-local state
  | request upload      |                               |                         |
  |-------------------->|                               |                         |
  |<--------------------| 202 + presigned parts         |                         |
  | upload bytes -------------------------------------->|                         |
  | complete upload     |                               |                         |
  |-------------------->| verify object / finalize MPU  |                         |
  |                     |------------------------------>|                         |
  |                     | BEGIN                                                   |
  |                     |-------------------------------------------------------->|
  |                     | commit metadata + version                               |
  |                     | create/confirm path binding                             |
  |                     | mark upload completed                                   |
  |                     | COMMIT                                                  |
  |                     |-------------------------------------------------------->|
  |<--------------------| 200/201 committed                                       |
```

Representative `/complete` conflict path:

```text
client              dat9 server                object storage               tenant-local state
  | complete upload     |                               |                         |
  |-------------------->| verify object / finalize MPU  |                         |
  |                     | BEGIN                                                   |
  |                     |-------------------------------------------------------->|
  |                     | path conflict / revision mismatch / validation failure   |
  |                     | ROLLBACK                                                |
  |                     |-------------------------------------------------------->|
  |<--------------------| 409 / 412                                               |

Result:
- upload must not transition to `COMPLETED`
- confirmed file state must not advance
- uploaded bytes may remain temporarily until retry, cancel, or reaper cleanup
```

Representative cross-tier overwrite:

```text
existing path /x currently points to:

  old confirmed version
  /x -> file/logical object -> old storage ref

overwrite with a new representation:

  1. stage new bytes in the target tier
  2. begin commit transaction
  3. validate overwrite precondition and path ownership
  4. advance current version / storage reference to the new body
  5. durably enqueue downstream work or write repair marker
  6. commit
  7. clean old storage asynchronously

Invariant:
- the user-visible path stays stable
- the new committed version becomes current before old storage is deleted
- old storage cleanup is never part of the critical commit path
```

### 5.3 Commit discipline

For both write paths:

- a consistent commit point must exist
- version progression must be explicit
- downstream async work must be durable or compensatable

In practice, this means one of the following must exist after commit:

- the async task is durably enqueued
- or a durable marker exists so reconcile can enqueue or repair it later

### 5.4 Reconcile and cleanup

Reconcile should handle at least:

- missing async follow-up work
- orphan uploads
- stale or missing derived artifacts
- orphaned storage objects

Examples of practical checks:

- a committed file version with no summary/index task record
- a completed upload record with no confirmed resource commit
- an `S3` object under a tenant prefix with no valid metadata reference

### 5.5 State machines and cross-state invariants

Suggested file state machine:

```text
PENDING -------------------------> CONFIRMED -------------------------> DELETED
   |                                   |                                   |
   | failed staging /                  | overwrite produces                | cleanup worker
   | abandoned draft                   | a newer version, but              | or reaper removes
   |                                   | user-visible state stays          | physical storage later
   +--> reconcile removes              | `CONFIRMED`                       |
       invalid draft                   +--> normal read / list / search    +--> terminal logical state
```

Suggested upload state machine:

```text
UPLOADING -----------------------> COMPLETED
    |                                  |
    | /complete conflict or            | confirmed file state exists
    | validation failure               | and points at a valid object
    |                                  |
    | stays non-terminal until         +--> normal retained upload record
    | retry or explicit cancel
    |
    +-------------------------------> ABORTED
    |
    +-------------------------------> EXPIRED
                                       |
                                       +--> cleanup / reaper
```

Important cross-state invariants:

- completed upload state must imply confirmed file state
- confirmed file state must imply complete storage object exists
- logical path bindings must point to valid file -> logical object bindings

Recommended current-phase state constraints:

- `upload.status = COMPLETED` must imply:
  - metadata commit succeeded
  - the file is `CONFIRMED`
  - the target path binding exists
- `/complete` returning `409` or `412` must imply:
  - the file does not advance to a new committed version
  - the upload does not become `COMPLETED`
- physical storage cleanup must imply:
  - the old version is no longer current
  - no live logical binding still depends on that storage reference
- reconcile may repair missing downstream work, but must not invent a committed version that never passed the write commit point

### 5.6 Delete path and cleanup separation

Logical state transitions and physical cleanup should remain separable.

Examples:

- metadata may mark deletion first
- old blobs may be cleaned later
- timed-out uploads may be aborted asynchronously

Representative delete flow:

```text
1. resolve path -> file -> logical object
2. remove namespace entry
3. check whether other logical paths still reference the same content
4. if references remain, stop at namespace delete
5. if no references remain, mark the logical object as deleted
6. cleanup worker or reaper removes physical storage later
```

The implementation should serialize the "last reference removed" decision strongly enough to avoid leaving orphaned logical-object state behind.

Representative concurrent delete / refcount flow:

```text
Initial state:
  /a -> file F
  /b -> file F

tx1: rm /a                         tx2: rm /b
--------------------------------   --------------------------------
lock path /a and file F            waits for file F lock
delete path /a
remaining refs for F = 1
commit
                                   lock path /b and file F
                                   delete path /b
                                   remaining refs for F = 0
                                   mark F logically deleted
                                   commit

after commit:
  cleanup worker / reaper removes physical storage once
```

This serialization is important because "last reference removed" is a correctness boundary, not just a convenience optimization.

### 5.7 Reaper responsibilities

The background reaper should at least be able to:

- abort timed-out uploads
- clean orphaned upload metadata
- clean orphaned storage objects
- remove storage for deleted files after logical deletion is complete
- trigger repair or cleanup for partially committed write flows

Representative reconcile / reaper sweep:

```text
periodic sweep
   |
   +--> scan upload records
   |      |
   |      +--> `UPLOADING` past expiry -> abort or expire
   |      +--> inconsistent `COMPLETED` -> investigate / repair
   |
   +--> scan committed files
   |      |
   |      +--> missing async task -> enqueue
   |      +--> stale or missing derived artifact -> enqueue repair
   |
   +--> scan logically deleted state
   |      |
   |      +--> storage still present -> delete asynchronously
   |
   +--> scan storage namespace
          |
          +--> object has no valid metadata reference -> quarantine or delete
```

## 6. Invariants / Correctness Rules

- writes must have an explicit commit point
- direct large-file upload must not require the server to proxy file bytes
- cleanup may be asynchronous, but commit state must remain explicit
- reconcile must be a built-in system capability, not an emergency-only mechanism

## 7. Failure / Recovery

- interrupted uploads must be resumable or recoverable
- failed downstream task submission must be compensatable
- orphaned storage must be discoverable and cleanable

For the current phase, dat9 should at least support:

- resumable or restartable direct uploads
- periodic detection of committed files missing downstream semantic work
- periodic cleanup of orphaned or abandoned upload state
- periodic cleanup of logically deleted data whose physical storage still exists

## 8. Open Questions

- whether outbox state should be modeled explicitly or folded into broader reconcile markers
- what recovery SLOs should apply for orphan cleanup and async repair

## 9. References / Dependencies

- `docs/design/storage-and-namespace.md`
- `docs/design/resource-versioning-and-async-correctness.md`
- `docs/design/durable-queue-runtime.md`
- `docs/design/api-and-ux-contract.md`
