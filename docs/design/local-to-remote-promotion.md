# Local-to-Remote Persistence Promotion

Status: Accepted for staged delivery

Date: 2026-09-18

## Summary

Drive9 currently classifies every path as `local-only`, `git_workspace`,
`fs_layer`, or `remote_persistent`. A rename between `local-only` and a remote
layer returns `EXDEV`, even when both paths are in the same FUSE mount.

That behavior is intentionally fail-closed. A local-only tree does not have a
remote namespace or durability lineage, and the current rename implementation
cannot create one without losing atomicity, open-handle continuity, or crash
recovery.

This document defines the product and architecture direction for promoting a
local-only tree into remote-managed storage. The long-term user experience is a
normal rename within one mount. The implementation is an idempotent persistence
promotion followed by an atomic namespace publish; it is not a recursive
visible copy followed by delete.

The accepted delivery is deliberately staged. P0 supplies the server primitive,
P1 exposes a bounded opt-in preview, P2 closes stable-inode and open-handle
semantics, and P3 treats Git workspaces separately. Until P2 passes its handle
matrix, the default cross-layer rename behavior remains `EXDEV`.

## User Problem

Coding-agent profiles keep build and cache paths such as `dist`, `build`, and
`tmp` in the local overlay for performance. Applications commonly build a
complete directory locally and atomically publish it by renaming it to a final
path. Today the internal storage policy leaks into that application workflow:

```text
dist/   local-only
site/   remote-persistent

rename("dist", "site") -> EXDEV
```

The immediate workaround is to place both trees in the same layer with
`--remote-only` or `--local-only`. That unblocks known workflows but does not
remove the abstraction leak.

## Goals

- Promote a homogeneous `local-only` tree to `remote_persistent` storage.
- Publish the remote tree atomically after all content is durable.
- Preserve the source on any pre-publish failure.
- Recover deterministically across process crashes, restarts that retain the
  same local root, network failures, and response loss.
- Keep persistence monotonic: a rename must not silently demote durable data to
  one host's ephemeral overlay.
- Eventually preserve stable inode identity and POSIX open-handle behavior.
- Keep Git workspace mutations on the Git workspace implementation.

## Non-goals

The first implementation does not support:

- remote-to-local rename or durability demotion;
- Git workspace directory rename;
- mixed-policy subtrees;
- hard links or special files;
- target replacement or renameat2 extensions;
- transparent cross-layer rename while a source file or directory handle is
  open;
- unbounded trees without entry, byte, depth, and time limits.

These restrictions are fail-closed. They must not cause partial copy, partial
publish, or a fall back to best-effort delete.

## Product Model

### Persistence state is not pathname state

Path policy chooses the initial placement of a newly created inode. It does not
permanently define the durability of an existing inode.

The following transition is allowed:

```text
LocalEphemeral -> RemoteManaged
```

P0 through P2 reject `RemoteManaged` to local-only rename with `EXDEV` and zero
side effects. The current pathname resolver cannot preserve remote placement
under a local-only path. A future P3 design may preserve remote placement only
after it defines durable placement binding, ancestor inheritance for lookup,
readdir and create, cache invalidation, and remount recovery. Explicit demotion
would require a separate data-loss contract.

### Capability levels

Drive9 exposes three capability levels while the implementation matures:

1. `promote/persist`: an explicit API or CLI operation with documented
   restrictions.
2. experimental cross-layer rename: opt-in FUSE integration using the same
   promotion primitive and restrictions.
3. transparent cross-layer rename: default support only after stable inode and
   open-handle migration is complete.

Level 1 or 2 must not be marketed as transparent rename support. Final product
acceptance includes:

```text
fd = open("dist/a")
rename("dist", "site")
write(fd, ...)
fsync(fd)
```

The write and fsync must affect `site/a` through the same inode identity.

### Recovery boundary

Before remote commit, the source remains local-only and retains its existing
failure model. Permanent loss of the host or local disk may lose the source;
Drive9 does not claim cross-host recovery for local-only data. A replacement
host may garbage collect an abandoned remote stage but cannot reconstruct the
local source.

After remote commit, the committed target and migration result are remotely
durable and discoverable from another host. Process crashes and host restarts
that retain the same local root also recover the client-side source hiding and
cleanup work.

## Required Semantics

### Namespace atomicity

Before publish, other mounts see no target. After publish, they see the complete
target. No user-visible intermediate path may contain a partial tree.

For the initiating mount:

- before success, the source remains the authoritative visible tree;
- after success, the source is hidden and the complete target is visible;
- failure before publish preserves the source and hides the target.

### Durability

Successful synchronous promotion means:

- every supported object and required metadata is durable remotely;
- the namespace publish is durably committed;
- the local source-hide record is durably committed;
- deleting the local overlay and mounting from another host still exposes the
  complete target.

An asynchronous operation requires a different API contract and must not return
successful POSIX rename before this boundary.

### Metadata

The first version preserves:

- regular file bytes;
- directories, including empty directories;
- symbolic links as link text without following them;
- supported mode and modification time metadata.

Unsupported ownership, ACL, xattr, hard-link, sparse-file, or special-file
semantics cause the operation to fail before remote side effects.

### Error contract

The explicit promotion API returns stable typed errors. The restricted FUSE
preview maps them as follows:

| Condition | Promotion error | FUSE result |
| --- | --- | --- |
| Cross-layer preview disabled | `promotion_not_enabled` | `EXDEV` |
| Open handle, mixed tree, Git ownership, unsupported metadata/type, unsupported target replacement | `promotion_not_supported` | `EXDEV` |
| Target created or changed before final CAS | `target_conflict` | `EEXIST` or `ENOTEMPTY`, matching the observed target |
| Source changed during snapshot | `source_changed` | `EAGAIN` |
| Permission failure | `permission_denied` | `EACCES` or `EPERM` |
| Quota reservation failure | `quota_exceeded` | `EDQUOT` or `ENOSPC` |
| Configured tree limit exceeded | `promotion_limit_exceeded` | `EFBIG` |
| Corrupt local record/manifest or unresolved committed outcome | `promotion_recovery_required` | `EIO` |

Capability and static validation failures occur before remote staging. A source
change detected after staging aborts the hidden import and preserves the source.
In an outcome-unknown state, only the affected source and target paths are blocked
with `EIO` until reconciliation; the implementation may instead fail the whole
mount closed, but it must not guess or expose both paths.

### Authorization and quota

Promotion requires both source mutation and destination creation authority.
The mount coordinator proves source authority by owning the single-writer local
root and accepting the originating FUSE operation; the server cannot infer that
authority from a source path that has no remote namespace entry. The server
independently reauthorizes destination creation on both `CreateImport` and
`CommitImport`.

`CreateImport` reserves the canonical manifest byte and entry totals and
returns a versioned quota reservation. Uploads cannot exceed those totals.

`CommitImport` atomically converts staged reservation into committed usage and
must not charge the same content twice. Abort or expired-stage GC idempotently
releases the reservation. Reservation, settlement, release, and GC failures are
auditable.

## Architecture

### Components

```text
FUSE promotion coordinator
  | durable local MigrationRecord and subtree fence
  v
private remote staging namespace
  | import-scoped capability + idempotent upload + manifest verification
  v
CommitImport transaction
  | target CAS + quota settlement + outbox + complete tree publication
  v
same-filesystem source quarantine and cleanup
```

The staging namespace is not a normal user path and is not returned by list,
stat, search, or semantic indexing.

### Migration record

The coordinator persists a record before remote side effects:

```text
migration_id
source_path
target_path
source_identity (local root UUID, st_dev, st_ino, generation)
manifest_hash
target_precondition (parent inode incarnation, child name, expected absence)
staging_ref
phase
created_at
updated_at
```

The manifest is written to a temporary file, fsynced, renamed to its final
name, and followed by a parent-directory fsync. The checksummed migration record
is then written with the same temp-file, fsync, rename, and directory-fsync
sequence. Recovery accepts only a complete record whose manifest hash matches;
it never assumes the pair was atomically written.

Phases are monotonic:

```text
PREPARED
  -> STAGING
  -> STAGED_VERIFIED
  -> COMMIT_REQUESTED
  -> REMOTE_COMMITTED
  -> SOURCE_QUARANTINED
  -> LOCAL_CLEANED
  -> DONE
```

No phase transition relies only on an in-memory flag.

`COMMIT_REQUESTED` is an outcome-unknown state. It is fsynced before sending
`CommitImport`. Until `GetImport` proves committed or not committed, both source
and target paths remain fenced and fail closed. A retry of the same
`(source identity, source path, target path)` resumes the existing migration ID.

After remote commit, the coordinator atomically renames the physical local
source into a migration-owned quarantine on the same filesystem:

```text
overlay/.drive9-promotions/<migration_id>/source
```

It then fsyncs the old parent, quarantine parent, and migration record. Cleanup
uses the quarantine identity and never deletes by the old user path. The old
path may therefore be recreated as a new generation after the fence is
released without being hidden or deleted by the old migration.

### Server API sketch

The exact wire shape may change, but the runtime requirements are fixed:

```text
CreateImport(migration_id, target, target_parent_incarnation,
             expected_target_absent,
             manifest_hash, entry_total, byte_total)
CreateImportContent(migration_id, relative_path, entry_hash, size)
PutImportEntry(migration_id, relative_path, entry_hash,
               import_content_token, metadata)
VerifyImport(migration_id, manifest_hash)
CommitImport(migration_id, target_precondition)
GetImport(migration_id)
AbortImport(migration_id)
RenewImportLease(migration_id, owner_epoch)
```

Every method is idempotent for the same migration identity and payload. Reuse
with different target, manifest, or content returns conflict.

`migration_id` is scoped by tenant. Content references are server-issued for
that import and tenant; the client cannot attach an arbitrary storage key or a
content reference owned by another tenant. `CreateImportContent` returns the
bounded upload plan/capability used by the existing direct or multipart upload
machinery. The completed object is checked against the declared size and hash
before an entry can reference it.

The target precondition is an immutable destination-parent incarnation plus
the child name and expected absence. A pathname, mtime, or reusable numeric
revision is not a sufficient fence: deleting and recreating the parent or
target must make the commit conflict even if a revision value is reused.

The server import state machine is:

```text
CREATED -> STAGING -> VERIFIED -> COMMITTING -> COMMITTED
                    \-> ABORTING -> ABORTED
```

- `VERIFIED` freezes the entry set and manifest version. A late or different
  `PutImportEntry` cannot mutate or revive it.
- `COMMITTING` and `ABORTING` are acquired by CAS; only one can win.
- `COMMITTED` and `ABORTED` are terminal. Abort and lease GC cannot remove a
  committed result, and a late upload cannot revive an aborted import.
- Only the current nonterminal owner epoch may renew the lease. GC first CASes
  an expired import to `ABORTING` before deleting stage data and releasing
  quota.
- The staging uploader uses an import-scoped internal capability. It never
  writes through the ordinary user namespace or FUSE writeback path.

`CommitImport` is the namespace linearization point. In one transaction it:

1. revalidates destination authorization and quota;
2. revalidates the target precondition and rename type rules;
3. verifies the complete staged manifest;
4. transfers every staged content reference to its committed inode owner;
5. converts the quota reservation to committed usage without double charge;
6. materializes the complete remote tree;
7. writes the committed result and one structural-reset outbox event containing
   target parent, migration ID, and tree generation.

P0 caps manifest entries and total metadata bytes below the database
transaction and statement limits. A larger tree fails before staging; it is not
split across multiple visible commits. Raising the cap later requires a
different atomic namespace indirection, not merely a larger transaction.

### Server data model sketch

The exact SQL names may follow the existing schema conventions, but these
durable facts are required:

```text
imports(
  tenant_id, migration_id, target_path, target_parent_inode,
  target_parent_incarnation, manifest_hash, entry_total, byte_total,
  quota_reservation_id, state, owner_epoch, lease_expires_at,
  committed_root_inode, committed_generation, created_at, updated_at,
  primary key (tenant_id, migration_id)
)

import_entries(
  tenant_id, migration_id, relative_path_hash, relative_path,
  entry_type, metadata_blob, entry_hash, import_content_id,
  primary key (tenant_id, migration_id, relative_path_hash)
)

import_contents(
  tenant_id, migration_id, import_content_id, storage_ref,
  size_bytes, checksum_sha256, upload_state, ownership_state,
  primary key (tenant_id, migration_id, import_content_id)
)
```

All three tables are tenant-scoped. A unique relative-path constraint rejects
duplicate manifest entries after canonical path normalization. Terminal import
rows retain enough target/result identity to answer retries after staging rows
and unowned objects are garbage collected. Commit changes content ownership and
namespace visibility in the same transaction; storage deletion is always an
outbox/GC consequence and never precedes the durable ownership decision.

The outbox is part of the transaction. It invalidates positive and negative
directory caches on other mounts. Staging emits no user-visible namespace or
search events. Namespace, content, and metadata are complete at commit;
semantic indexing remains asynchronous, but its task is registered through the
same committed outbox.

The initial implementation requires the destination to be absent. Support for
replacement of files or empty directories is a later compatibility step.

### Subtree fence

The coordinator fences both source and destination subtrees on the initiating
mount before taking a manifest. Lookup, open, create, write, truncate, unlink,
rename, and writeback enqueue paths must participate in this fence. Other
mounts cannot share this in-memory fence; their target races are serialized by
the server-side parent-incarnation and absent-child precondition at commit.

`WaitPrefix` or scanning current handles is not a fence: a new operation can
start immediately after either check. The fence must prevent new work while
the coordinator drains existing work and validates the tree.

For the restricted preview, any open source file or directory handle, pending
writeback, mmap, or concurrent mutation fails before remote staging begins.

The fence is acquired before preflight and remains held until remote commit is
known, the local source has been quarantined, and inode/cache publication is
complete. Recovery rebuilds all fences from migration records before accepting
FUSE requests. Normal commit queues and uploaders cannot enqueue work under the
fenced prefixes; the import uploader is exempt only through its migration ID
and internal capability.

The local root has a single-writer mount lock. Direct mutation of the private
local root is unsupported, but P1 still detects it. The coordinator traverses
with `openat`/`fstat` and no-follow semantics, records each entry's device,
inode, type, size, ctime, mtime and content hash, and revalidates identity and
metadata after each read and again before `VerifyImport`. Any mismatch or
symlink swap aborts the import. Coordinator-owned traversal handles are marked
internal and do not fail the open-handle validation.

### Policy homogeneity

The preview validates every source and translated destination descendant.

- every source entry must be local-only;
- every destination entry must be remote-persistent;
- `.git`, Git workspace ownership, fs-layer ownership, remote-only children in
  the source, and local-only children in the destination make the tree mixed.

A mixed tree is rejected as one operation. It is never split into independently
published and local holes.

### Open handles and stable identity

Transparent support requires a stable inode identity that survives promotion.
All handles for an inode must atomically move from:

```text
Layer=LocalOnly + LocalFile
```

to a migration-aware remote-managed state. After publication, write, append,
truncate, flush, fsync, and close through an old file descriptor must join the
new remote writeback lineage. Multiple descriptors must still observe the same
file content.

Changing `fh.Path` with the existing local rename helper is insufficient and
must not be used as evidence of support.

### Git workspace boundary

Git workspace directory rename remains separate. Its durable authority is the
Git overlay and object store, not the normal file namespace.

Future support needs an atomic batch that writes all required objects, applies
the destination entries and source whiteouts, and uses a workspace/head
generation CAS. The generic import path must not bypass Git routing.

## Recovery Rules

On a restart with the same local root, recovery restores all migration fences
and queries the server by `migration_id` before serving affected paths.

| Last durable state | Recovery result |
| --- | --- |
| Local record missing | No operation existed. |
| `PREPARED` or `STAGING`; server not committed | Source remains visible. Resume or abort hidden staging. |
| `STAGED_VERIFIED`; no commit request sent | Source remains visible. Commit or abort with the same ID. |
| `COMMIT_REQUESTED` | Query `GetImport`; never retry with a new ID. If the server is unreachable, keep both paths fenced and return `EIO`. |
| Server `REMOTE_COMMITTED`, local quarantine missing | Keep both paths fenced, atomically quarantine the source, publish inode/cache state, then release the fence. Never roll back the target. |
| `SOURCE_QUARANTINED`, physical local files remain | Target is authoritative. A janitor removes the migration-owned quarantine identity. |
| Manifest missing, corrupt, or mismatched | Fail closed and require repair; do not guess. |

Once the server reports committed, old uploaders and recovery attempts cannot
write a different manifest under the same migration ID.

The server durably accepts commit ownership before work is detached from the
request cancellation path, or completes the database transaction before a
request can report success. Client disconnect does not create a second commit
owner.

## Concurrency Rules

- Two promotions of the same source or destination have one winner through the
  subtree fence and server target CAS.
- A different client creating the target during staging causes final commit to
  conflict. The source remains visible.
- A client timeout does not cancel an accepted server commit. The caller
  resolves the outcome by migration ID. Commit, abort, and lease GC race through
  the import-state CAS and have one winner.
- Source mutations during the restricted preview are blocked or rejected. A
  future concurrent implementation requires a snapshot generation plus a final
  delta barrier; a directory walk alone is never sufficient.

## Failure Behavior

- quota, authorization, validation, upload, hash, network, or object-storage
  failures before commit preserve the source and keep the final target hidden;
- a committed target is never rolled back by deleting it from the client;
- local cleanup failure does not re-expose the old path after durable source
  quarantine;
- abandoned hidden staging is garbage collected by lease and retention policy;
- committed imports transfer object ownership and cannot be removed by staging
  GC; aborted imports idempotently release quota and cannot be revived;
- entry, byte, depth, and time limits are checked before or during hidden
  staging and never produce a partial final target.

## Delivery Plan

### P0: primitive and specification

- freeze this contract and public capability naming;
- implement hidden staging, idempotent upload, manifest verification,
  versioned quota reservation, the import state CAS, `CommitImport`, committed
  outbox/status lookup, and garbage collection;
- add server-side failure injection and crash recovery tests.

### P1: restricted preview

- explicit `promote/persist` and/or opt-in FUSE cross-layer rename;
- homogeneous ordinary trees only;
- absent target, no open handles, no Git workspace, bounded tree;
- durable local migration record, subtree fence, synchronous completion.

### P2: transparent rename

- stable inode and migration-aware handles;
- extend the P0 API with `BeginImportVersion`/`VerifyImportVersion`; P2 cannot
  mutate a manifest after P0 `VerifyImport` or pretend the one-shot API is
  sufficient;
- use versioned manifests while a base snapshot uploads;
- record per-inode deltas during that upload, then acquire a final barrier over
  every related handle;
- merge the deltas, create and verify the final immutable manifest version,
  commit while the barrier is held, atomically switch all handles to one remote
  shadow/writeback lineage, then release the barrier;
- multi-descriptor, append, truncate, fsync, unlink/recreate, and crash behavior;
- only after these pass may default rename be advertised as transparent.

### P3: advanced semantics

- Git workspace batch rename/import;
- hard links, richer metadata, target replacement, and large-tree performance;
- explicit durability demotion, if a safe product need is established.

## Test Matrix

### Functional

- empty and deep directories;
- zero-byte and large files;
- symlink without following the target;
- mode and mtime round trip;
- Unicode, spaces, long paths, and tree-size limits;
- target absent, present, wrong type, nonempty, or inside source;
- mixed local/remote/Git/fs-layer descendants rejected with zero side effects.

### Crash recovery

Kill and restart before and after:

- local record and manifest fsync;
- each object upload;
- stage verification;
- durable `COMMIT_REQUESTED` before request send;
- server commit;
- successful commit with lost response;
- remote commit followed by local quarantine fsync failure;
- local source quarantine;
- physical local deletion;
- inode/cache publication.

Every case converges to only the complete old tree or only the complete new
tree. Repeated recovery is idempotent.

### Concurrency

- source create, write, truncate, unlink, rename, and open during promotion;
- target creation by another mount during staging;
- two promotions competing for source or target;
- commit versus abort and commit versus lease GC;
- verify versus a late upload and upload versus an aborted terminal import;
- writeback enqueue immediately after a drain attempt;
- direct local-root file replacement and symlink swap while a snapshot is read;
- a second mount reading while publish occurs;
- a committed outbox invalidating a second mount's positive and negative cache.

### Import and accounting

- the same migration ID with a different target, manifest, entry payload, or
  content hash conflicts;
- missing, extra, duplicate, absolute, escaping, or symlink-following manifest
  entries are rejected;
- an upload cannot exceed the reserved entry or byte total;
- commit converts the reservation exactly once after response loss and retry;
- abort and GC release it exactly once and cannot race a committed import;
- committed content references survive staging GC;
- a structural-reset outbox entry exists exactly once for each commit, while
  no staging entry is indexed or emitted.

### Local recovery and reuse

- recovery reinstalls fences before the first lookup;
- `COMMIT_REQUESTED` with an unreachable server blocks only the affected paths
  or fails the mount closed;
- successful publish followed immediately by recreating the old path creates a
  new generation that quarantine cleanup cannot hide or delete;
- the import-scoped uploader cannot deadlock on the user-namespace subtree
  fence;
- direct source mutation or identity change rejects the migration rather than
  uploading a torn snapshot.

### Handle behavior

- preview: any source file or directory handle is rejected before staging;
- transparent support: open, rename, write, append, truncate, fsync, close,
  multi-fd, unlink/recreate, and recovery all preserve identity and publish one
  final lineage.

### Regression strength

Tests must fail when independently deleting:

- the subtree fence;
- migration idempotency;
- target CAS;
- the server-state CAS between commit, abort, and GC;
- quota reservation conversion and content-ref ownership transfer;
- the post-commit source quarantine identity;
- the committed outbox/cache reset;
- handle state migration;
- Git workspace routing.

## Rollout

- Keep current `EXDEV` by default until the restricted preview is explicitly
  enabled.
- Provide path-policy overrides as the immediate workaround.
- Ship the preview behind a capability flag and record promotion outcomes,
  duration, bytes, entries, conflicts, and recovery actions.
- Enable transparent rename by default only after the P2 handle and concurrency
  matrix passes on Linux FUSE and the live local E2E environment.
