# Local-to-Remote Persistence Promotion

Status: Proposed for staged delivery; P0/P1 contract under review

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

The sole xattr exception is Drive9's exact reserved source-incarnation key on
the source root. It is private coordinator metadata: traversal filters it from
the manifest, checksum, destination metadata, and unsupported-xattr result.
The same key on a descendant is corrupt state, and every other xattr remains
unsupported. A symlink source root, a filesystem without durable no-follow
xattr operations, or failure to fsync the reserved marker is rejected before
`CreateImport` or quota reservation.

### Error contract

The explicit promotion API returns stable typed errors. The restricted FUSE
preview maps them as follows:

| Condition | Promotion error | FUSE result |
| --- | --- | --- |
| Cross-layer preview disabled | `promotion_not_enabled` | `EXDEV` |
| Open handle, mixed tree, Git ownership, unsupported metadata/type, unsupported target replacement | `promotion_not_supported` | `EXDEV` |
| Target child exists or has the wrong type at final CAS | `target_conflict` | `EEXIST` or `ENOTEMPTY`, matching the observed target |
| Target remains absent but its parent edge, child-set generation, or namespace-CAS epoch changed | `target_precondition_changed` | `EAGAIN` |
| Source changed during snapshot | `source_changed` | `EAGAIN` |
| Import activity deadline won before commit acceptance | `promotion_deadline_exceeded` | `ETIMEDOUT` |
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
source_identity (local root UUID, reserved source-incarnation UUID)
manifest_hash
target_expectation (canonical target, expected absence,
                    server precondition digest after CreateImport)
staging_ref
owner_epoch (0 until returned or recovered from GetImport)
owner_token
recovery_token
activity_deadline (unset until returned or recovered from GetImport)
phase
created_at
updated_at
```

The manifest is written to a temporary file, fsynced, renamed to its final
name, and followed by a parent-directory fsync. The checksummed migration record
is then written with the same temp-file, fsync, rename, and directory-fsync
sequence. The record, owner token, and recovery token are private mode `0600`
state. Recovery accepts only a complete record whose manifest hash matches; it
never assumes the pair was atomically written.

Per-content upload progress is a checksummed, fsynced journal in the same
private state directory. Before each `CreateImportContent`, it durably records
the relative content identity and random attempt idempotency key; after a
response it records the returned logical attempt ID. Before each write-grant
request it likewise records the attempt, part/direct-write identity, maximum
bytes, and random grant idempotency key, then records the returned grant ID.
Recovery truncates only an incomplete tail and retries the last complete key,
so response loss cannot be misread as permission to allocate either a
replacement attempt or an unaccounted write grant.

The initial record contains the canonical target and expected absence. After a
successful or recovered `CreateImport`, the coordinator rewrites and fsyncs the
record with the server precondition digest and owner epoch. The digest is for
reconciliation only; the server always uses its own stored namespace facts at
commit.

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

#### Durable local source identity

`st_dev` and `st_ino` are diagnostics, not identity: both may be reused after
unlink. Before writing `PREPARED`, while holding the subtree fence, the
coordinator opens the source root with no-follow semantics and reads a reserved
Drive9 source-incarnation xattr. If absent, it creates a cryptographically
random 128-bit UUID with create-only xattr semantics, fsyncs the source inode
and its containing directory, and then persists that UUID in the migration
record. The reserved xattr is not readable, writable, copied, or removable
through the mounted user namespace. A local filesystem that cannot durably
store this xattr is unsupported by P1 and returns
`promotion_not_supported` before remote staging.

The local-root UUID is stored in the private promotion state directory and is
created and fsynced when that root is initialized. The security identity is
therefore `(local_root_uuid, source_incarnation_uuid)`. Device, inode, type,
ctime, and mtime remain snapshot validation inputs, but never substitute for
the UUID. A UUID is never reassigned to a different local object. A duplicate
reserved UUID observed at two live objects, including one introduced by direct
mutation of the private overlay, fails closed as corrupt local state.

Recovery and cleanup open the recorded source or quarantine object first and
compare its reserved UUID before rename or removal. A same-path object with a
different UUID is a new source generation and is never hidden, quarantined, or
deleted by the old migration. The quarantine rename preserves the xattr. A
crash after xattr creation but before `PREPARED` leaves only an inert marker;
a later promotion may safely reuse that object's UUID with a new migration ID.

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
AllocateImportID()
CreateImport(migration_id, target, expected_target_absent,
             manifest_hash, entry_total, byte_total,
             owner_token, recovery_token)
CreateImportContent(migration_id, owner_epoch, owner_token,
                    relative_path, entry_hash, size,
                    attempt_idempotency_key)
CreateImportWriteGrant(migration_id, owner_epoch, owner_token,
                       content_id, upload_attempt_id,
                       write_grant_idempotency_key,
                       direct_or_part_number, maximum_bytes)
CompleteImportContentUpload(migration_id, owner_epoch, owner_token,
                            content_id, upload_attempt_id,
                            ordered_write_grants, multipart_completion)
SealImportContent(migration_id, owner_epoch, owner_token,
                  content_id, upload_attempt_id)
PutImportEntry(migration_id, owner_epoch, owner_token,
               relative_path, entry_hash,
               import_content_token, metadata)
VerifyImport(migration_id, owner_epoch, owner_token, manifest_hash)
CommitImport(migration_id, owner_epoch, owner_token)
GetImport(migration_id)
AbortImport(migration_id, owner_epoch, owner_token)
RenewImportLease(migration_id, owner_epoch, owner_token)
TakeOverImport(migration_id, expected_owner_epoch,
               recovery_token, new_owner_token)
AcknowledgeImportResult(migration_id, recovery_token,
                        terminal_state, result_digest)
```

Every method is idempotent for the same migration identity and payload. Reuse
with different target, manifest, or content returns conflict.
Repeating `CreateImport` with the same owner and recovery tokens returns the
current state and does not duplicate the reservation. Repeating it with either
token changed never acts as takeover or resets an epoch; the caller must follow
the explicit expired-lease takeover contract.

Migration IDs are tenant-bound, authenticated IDs returned by
`AllocateImportID`, with a signed `create_before` time and random, non-reusable
identity. The coordinator obtains and fsyncs the ID and both tokens before
`CreateImport`, so allocation creates no stage or quota side effect.
`CreateImport` accepts an ID only before `create_before`; an expired or
previously consumed ID can never start a new import. An active import has no
allocation-time retirement deadline: its durable row remains queryable until
the server reaches a terminal state, and terminal retention begins only then.

`migration_id` is scoped by tenant. Content references are server-issued for
that import and tenant; the client cannot attach an arbitrary storage key or a
content reference owned by another tenant.

#### Immutable staged content and capability closure

An object-store upload capability can outlive a server epoch check. Therefore
no direct or multipart capability ever writes a key or version that a verified
manifest references. `CreateImportContent` creates only the durable logical
upload attempt. `CreateImportWriteGrant` then creates one durable grant for one
direct object write or one numbered multipart-part generation. Each grant is
scoped to the exact tenant, migration, content, attempt UUID, unique temporary
object/upload identity, maximum bytes, and checksum header where supported. It
cannot write any committed or sealed key.

P0 uses a Drive9 upload proxy unless a provider can prove equivalent native
semantics. A proxy grant has separate `accept_before`, `body_complete_before`,
and `landing_not_after` bounds. The proxy admits the request only before the
first bound, stops reading and fails it closed unless the complete bounded body
arrives before the second, and creates a durable provider-mutation claim before
forwarding any bytes or part to object storage. The provider mutation's
`mutation_not_after` is no later than `landing_not_after`. Thus a request that
started before expiry cannot remain an untracked writer afterward.

A native signed URL is eligible only when the provider enforces the same finite
landing bound **and** the grant is single-use or every possible replay creates
an exactly enumerable write/version identity with separately reserved debt.
Checking expiry only when a PUT or `UploadPart` begins is insufficient. In
particular, replayable presigned S3 PUT/UploadPart URLs do not satisfy P0: they
fail closed during staging and the proxy path must be used. A backend that can
only detect an oversized upload afterward, cannot enumerate every version or
part generation, or cannot bound an accepted request's eventual side effect is
unsupported for promotion.

The client supplies a random `attempt_idempotency_key` that it durably records
before the request. A retry with the same key and identical payload returns the
same logical attempt UUID, upload mode, and multipart upload ID (when
applicable) only while the stored owner epoch is still current; a different
payload conflicts. After takeover, that key resolves as a stale-owner attempt
and the new owner must generate and fsync a new key. Replaying
`CreateImportContent` never issues a write grant. A grant retry only returns
the already-recorded grant for the same grant key; neither retry starts another
provider write. Replacing or restarting an upload requires a new attempt
idempotency key and therefore a new attempt UUID/upload identity; it is never
inferred from a timeout.

The client also supplies and journals a random write-grant idempotency key. A
same-payload retry returns the same grant; a different payload conflicts. A
proxy grant is single-flight: concurrent presentation CASes the same durable
mutation claim, so at most one provider write starts. Response-loss recovery
resumes or discovers that operation rather than issuing another. A replacement
direct write or multipart part generation needs a new grant/key, unique temp
identity, mutation claim, and debt charge.

Each grant records its three time bounds, exact provider write identity,
maximum bytes, state, debt charge, and GC status before the capability is
returned. Multipart grants permit only their numbered part generation;
completion is an epoch-fenced Drive9 operation over an ordered, unique set of
accepted grant IDs and exact ETags/checksums. Reissuing a part number replaces
the selected generation but does not erase the earlier generation's operation,
debt, or cleanup obligation. A takeover always allocates a new attempt UUID,
grant identities, temporary key/upload ID, and subsequent per-grant charges;
nothing from an earlier owner epoch is reused.

Before direct completion/seal, the attempt durably selects one terminal grant
and exact temp identity; every other direct grant remains unreferenced cleanup
debt. Before multipart completion, every selected grant must be terminal, all
superseded generations for its part number must have passed their landing
bound, and an exact `ListParts`-equivalent read must match the ordered selected
part number, ETag/checksum, and size set. Otherwise completion waits or fails
closed. A late older generation therefore cannot replace a selected part
between proof and multipart completion.

Every provider mutation, whether started by the proxy for a client write or by
the server for multipart completion and `SealImportContent` copy/create, has a
durable operation claim created before the request is sent. The claim records a
random operation UUID, kind, exact source and destination/upload identities,
owner epoch, state, and a provider-guaranteed `mutation_not_after`: after that
instant the provider cannot newly materialize the side effect. A worker checks
database time and the import/claim state immediately before sending and never
starts the request after that bound. A synchronous timeout is not such a
guarantee; an asynchronous provider operation remains claimed and is polled by
its provider operation ID until terminal. A backend without idempotent or
exactly rediscoverable operations and a finite, enforceable side-effect bound
is not eligible for promotion. Retrying the same grant or control-plane request
resumes its existing mutation claim and provider operation; it cannot allocate
a second side effect or move the closure bound.

Before touching object storage, `SealImportContent` first creates a durable
seal-intent row containing a server-generated seal-attempt UUID, expected
content identity/hash/size, owner epoch, and a deterministic, server-only
create-once destination key. Only then does it snapshot the uploaded attempt
into that destination. The backend must support an atomic create-if-absent
object or an equivalently discoverable immutable version; an API that can
create a version but cannot unambiguously rediscover it after response loss is
unsupported.

There is at most one seal intent for a given content/upload-attempt pair.
Repeating `SealImportContent` after request or response loss resumes that exact
intent; it cannot allocate another destination. A checksum-invalid candidate
terminally fails both the seal and its mutable upload attempt, so replacement
starts with a new content-attempt idempotency key rather than re-sealing bytes
that a late writer might still change.

After the create/copy, the server reads the exact deterministic destination,
validates byte count and checksum, and records its provider version ID or
equivalent create-once identity. One database transaction then locks the seal
intent and import, records that immutable candidate in the intent ledger, and
attaches it to `import_contents` only if the current import state and owner
epoch still match. If takeover or abort won the race, the same transaction
marks the candidate for GC and never attaches it. A crash after object creation
but before this DB transaction resumes the same intent and exact destination;
response loss after the transaction returns the already-recorded attach or GC
outcome. Neither path re-reads a possibly late-overwritten temporary key or
creates an untracked second version. A failed candidate is terminal for that
seal intent and upload attempt. A replacement upload attempt receives a new
seal-attempt UUID/destination while the old row remains retained for cleanup.

Abort, takeover, and GC do not assume cancellation of a claimed provider
mutation. They preserve its claim and destination/upload identity through
`mutation_not_after` plus the provider clock, propagation, and in-flight safety
margin. Only after that closure bound may cleanup issue another exact-key
delete or multipart abort and prove with an exact-key/version lookup (not an
eventually consistent prefix listing) that no object, version, upload, or part
remains. An absence observed before the bound is provisional and cannot set
`gc_done`: a delayed operation may still land and must be removed by the
post-bound sweep. If the worker crashes after the provider side effect but
before DB attach, the durable claim keeps the ledger and tombstone alive until
the same deterministic destination is either attached or collected.

The manifest references only the successfully attached sealed identity.
`VerifyImport` freezes the ordered set of sealed identities, sizes, and hashes;
`CommitImport` rechecks those exact identities and never resolves a mutable
temporary key. A late PUT or part from an old owner can therefore affect only
its old temporary attempt, not verified or committed bytes.

On owner-epoch takeover, verify, commit acceptance, or abort, the server stops
issuing grants for the old epoch and aborts outstanding multipart upload IDs
where supported. Revocation is not assumed. Every write grant and upload
attempt stays in its durable ledger until the grant's `landing_not_after`, all
related provider-mutation closure bounds, and the provider clock/propagation
margin have passed and a sweeper has confirmed every exact temp
key/version/part/upload identity absent. Cleanup repeats delete/abort after the
maximum bound, so a write that lands after an earlier delete is still
collected. `ABORTED` may release customer quota once sealed ownership is
detached, but its grant/attempt ledgers and internal cleanup obligation remain
until `gc_done`; deleting them early is forbidden. Object-store lifecycle
policy is a backstop, not the correctness mechanism.

Each write grant is bounded by the import reservation and a separate internal
cleanup-debt budget. Before returning a grant, the server charges that grant's
maximum bytes. The charged live-grant sum, not merely the logical attempt size,
bounds temporary provider storage. A retry that could create another direct
object version or multipart part generation must use a separately charged
grant; a backend whose replay-created versions cannot be identified and
counted cannot issue the capability at all. Before claiming a seal copy/create,
the server separately charges the candidate's expected bytes; temp writes and
the candidate may exist simultaneously and therefore all charges count toward
the peak bound. The attach transaction atomically converts an accepted sealed
candidate from internal candidate debt to reservation-backed staged ownership.
If abort/takeover wins, it instead leaves the candidate as internal cleanup
debt. A later abort atomically transfers every detached staged candidate back
to internal cleanup debt before releasing the reservation. Customer quota may
be released at `ABORTED`, but live-grant and unowned-candidate charges remain
until their respective landing/operation closure and `gc_done`.
Retrying a durable attempt or seal intent reuses its charge; it never
double-charges or silently drops debt after response loss. This prevents
repeated uploads, seals, aborts, or takeovers from turning the closure window
into unbounded provider storage debt.

During `CreateImport`, the server resolves and records an immutable target
precondition; clients cannot supply or replace its namespace facts. It is a
snapshot of all of:

- the canonical destination-parent path;
- the incarnation of that exact parent **path edge**, not merely the directory
  inode identity;
- the parent's monotonic child-set generation;
- the canonical child name and expected absence.

Every direct child create, unlink, or rename into or out of a directory
increments its child-set generation in the same namespace transaction. Every
directory-entry create or rename assigns a fresh, non-reusable path-edge
incarnation, even when the underlying inode identity is retained. The
filesystem root has a durable root-edge incarnation and child-set generation.
Because `file_nodes` stores canonical paths, a subtree rename assigns a fresh
edge incarnation to every row whose canonical path changes; otherwise an
ancestor rename-away-and-back could preserve the recorded direct-parent edge.
These facts are server-maintained and cannot be supplied by clients.

The concrete P0 namespace representation adds an opaque edge-incarnation UUID
to each `file_nodes` row and an unsigned child-set generation to each directory
inode (with equivalent root metadata for `/`). Structural writers lock the
canonical parent edge and directory inode in a single order, mutate the child,
and increment the generation in one transaction. Generation overflow fails the
mutation closed; it never wraps or resets within an incarnation.

`CommitImport` resolves the canonical parent path under lock and requires the
same edge incarnation, child-set generation, and absent child immediately
before publication. Publication increments the child-set generation in that
same transaction. Consequently target `create -> delete`, parent
`rename -> rename back`, parent delete/recreate, and a parent moved away from
the requested path all conflict. This conservative P0 fence may also reject an
unrelated sibling mutation; the caller restarts with a fresh precondition. A
pathname, mtime, inode ID alone, or reusable numeric revision is not sufficient.

The server import state machine is:

```text
CREATED -----> STAGING -----> VERIFIED -----> COMMITTING -----> COMMITTED
   |              |              |
   +--------------+--------------+----------> ABORTING -------> ABORTED
```

The transitions and their durable effects are normative:

| From | Trigger | To | Durable result |
| --- | --- | --- | --- |
| none | `CreateImport` | `CREATED` | Identity, target precondition, manifest totals, reservation, namespace-CAS epoch, immutable activity deadline, owner epoch `1`, owner/recovery-token hashes, and lease are stored atomically. |
| `CREATED` | first accepted entry/content mutation | `STAGING` | Partial hidden stage and the same reservation remain owned by the import. |
| `STAGING` | successful `VerifyImport` | `VERIFIED` | Entry set, content set, and manifest version become immutable. |
| `VERIFIED` | accepted `CommitImport` CAS | `COMMITTING` | Commit attempt ID and accepted outcome are durable; request cancellation can no longer abort it. |
| `COMMITTING` | server commit worker | `COMMITTED` | Target CAS, namespace publication, ownership transfer, reservation settlement, result row, and structural outbox event commit atomically. |
| `CREATED`, `STAGING`, or `VERIFIED` | explicit abort, expired-lease GC, or activity-deadline CAS | `ABORTING` | No new grant, completion, seal, entry, verify, renew, takeover, or commit is accepted; cleanup ownership, including every grant landing bound, provider-mutation closure bound, object identity, and debt charge, is durable. |
| `COMMITTING` | permanent target/auth/manifest failure before publication | `ABORTING` | A durable terminal reason is recorded; no namespace or committed quota changed. Transient failure stays `COMMITTING` and is retried by the server. |
| `ABORTING` | server cleanup worker | `ABORTED` | Hidden entries and sealed unowned objects are detached, the reservation is released exactly once, and the terminal reason/result is retained. Upload/seal attempts, provider-mutation claims, and cleanup debt remain tracked through the post-closure sweep until `gc_done`. |

There are no other transitions. `COMMITTED` and `ABORTED` are terminal.
`COMMITTING` and `ABORTING` are server-owned recovery states: client lease
expiry does not abandon them, and their workers are durably discoverable after
a server crash. `COMMITTING` never transitions directly back to `VERIFIED`, and
`ABORTING` cannot be revived.

`CreateImport` assigns an immutable `activity_deadline` no later than the
server's maximum import lifetime. Owner leases, every write grant's
`accept_before`, `body_complete_before`, and `landing_not_after`, and every
provider mutation's `mutation_not_after` must be at or before that deadline;
renew, takeover, or grant retry may shorten their normal duration but never
extend the deadline. `CommitImport` can be accepted only before it. Every owner
mutation performs that comparison with database time inside the same
state/version transaction; application clocks do not decide the winner. At the
deadline, a server scanner (and every racing API call) CASes `CREATED`,
`STAGING`, or `VERIFIED` to `ABORTING` with reason
`activity_deadline_exceeded`; no new owner action is accepted. A commit whose
`VERIFIED -> COMMITTING` CAS won before the deadline remains server-owned and
runs to `COMMITTED` or `ABORTING`. Thus active work has a finite
client-controlled horizon, but `GetImport` remains available and its ID/status
never retire before server-owned terminalization. A FUSE rename whose import
reaches this terminal reason returns the stable
`promotion_deadline_exceeded`/`ETIMEDOUT` mapping above; retries query the same
terminal result rather than translating it from a transport timeout.

Grant issuance also fails closed when the remaining activity window cannot fit
the configured minimum body-completion interval plus the provider's mutation
and safety bounds. It never returns a nominally unexpired grant whose accepted
request could outlive the import deadline.

Every transition compares and increments `state_version`. Commit and cleanup
workers additionally claim a durable attempt ID; duplicate workers either
resume that attempt or observe its terminal state. Reservation settlement and
release use the reservation ID plus terminal ownership state as their
idempotency key, so a crash after the accounting write but before the worker
ack cannot double-charge or double-release.

`CreateImport` receives opaque, random owner and recovery tokens that the
coordinator generated and fsynced in `PREPARED` before the request. The server
stores only their hashes, creates `owner_epoch = 1`, and returns the epoch and
lease expiry. Every staging, verify, commit, abort, and renew mutation must
present both the current epoch and owner token. Thus a lost create response is
recoverable with the already-durable token; `GetImport` may disclose the epoch
and state but never either token.

For `CREATED`, `STAGING`, or `VERIFIED`, the current owner may renew before
expiry. After expiry, `TakeOverImport` requires the stable recovery token and
atomically compares the old epoch and state, increments the epoch, installs the
hash of a newly generated and fsynced owner token, and grants a new lease.
Tenant authentication or knowledge of a migration ID alone cannot take over an
import. Takeover is rejected while the old lease is live and in every
server-owned or terminal state. Any request carrying the old epoch or owner
token is then fenced, including an old renew, content-create,
multipart-complete, seal, entry, verify, commit, or abort control-plane
mutation. Already-issued write grants are not treated as revoked; their
isolation and cleanup follow the write-grant/landing-bound contract above. The
single-writer local-root lock prevents routine concurrent takeover; the server
epoch is the authoritative fence for a paused or zombie coordinator.

`VERIFIED` freezes the entry set and manifest version. A late or different
`PutImportEntry` cannot mutate it. Commit acceptance and abort/GC acquisition
race through the state CAS, so only one wins. The staging uploader uses an
import-scoped internal capability and never writes through the ordinary user
namespace or FUSE writeback path.

`CommitImport` first durably accepts work by CASing `VERIFIED -> COMMITTING`
and recording a commit attempt. The handler may drive the worker synchronously,
but request lifetime is not ownership of the accepted work. The worker's final
database transaction is the namespace linearization point. In that transaction
it:

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
  target_parent_path, target_parent_edge_incarnation,
  target_parent_children_generation, manifest_hash, entry_total, byte_total,
  namespace_cas_epoch, quota_reservation_id, state, state_version,
  owner_epoch, owner_token_hash, recovery_token_hash,
  activity_deadline, lease_expires_at, commit_attempt_id, terminal_reason,
  committed_root_inode, committed_generation, terminal_result_blob,
  terminal_result_digest, terminal_at, result_acknowledged_at,
  full_row_compact_not_before, retire_after, created_at, updated_at,
  primary key (tenant_id, migration_id)
)

import_entries(
  tenant_id, migration_id, relative_path_hash, relative_path,
  entry_type, metadata_blob, entry_hash, import_content_id,
  primary key (tenant_id, migration_id, relative_path_hash)
)

import_contents(
  tenant_id, migration_id, import_content_id, sealed_storage_ref,
  sealed_storage_version, size_bytes, checksum_sha256,
  accepted_seal_attempt_id, seal_state, ownership_state,
  primary key (tenant_id, migration_id, import_content_id)
)

import_upload_attempts(
  tenant_id, migration_id, import_content_id, upload_attempt_id,
  attempt_idempotency_key, request_digest, owner_epoch,
  upload_mode, provider_upload_id, maximum_size_bytes,
  selected_direct_write_grant_id,
  provider_mutation_id, provider_mutation_kind,
  provider_mutation_state, provider_mutation_not_after,
  state, gc_not_before, gc_state,
  primary key (tenant_id, migration_id, import_content_id, upload_attempt_id),
  unique (tenant_id, migration_id, import_content_id, attempt_idempotency_key)
)

import_upload_write_grants(
  tenant_id, migration_id, import_content_id, upload_attempt_id,
  write_grant_id, write_grant_idempotency_key, request_digest, owner_epoch,
  write_kind, part_number, part_generation, temporary_storage_ref,
  maximum_bytes, accept_before, body_complete_before, landing_not_after,
  provider_mutation_id, provider_mutation_state,
  provider_mutation_not_after, provider_write_identity,
  temporary_debt_charge_id, state, gc_not_before, gc_state,
  primary key (tenant_id, migration_id, import_content_id,
               upload_attempt_id, write_grant_id),
  unique (tenant_id, migration_id, import_content_id,
          upload_attempt_id, write_grant_idempotency_key)
)

import_seal_attempts(
  tenant_id, migration_id, import_content_id, seal_attempt_id,
  source_upload_attempt_id, owner_epoch, deterministic_storage_ref,
  sealed_storage_version, expected_size_bytes, expected_checksum_sha256,
  provider_mutation_id, provider_mutation_state,
  provider_mutation_not_after, candidate_debt_charge_id,
  state, gc_state, created_at, updated_at,
  primary key (tenant_id, migration_id, import_content_id, seal_attempt_id),
  unique (tenant_id, migration_id, import_content_id, source_upload_attempt_id)
)

import_tombstones(
  tenant_id, migration_id, request_digest, recovery_token_hash, terminal_state,
  terminal_result_blob, terminal_result_digest, retire_after,
  primary key (tenant_id, migration_id)
)
```

All tables are tenant-scoped. A unique relative-path constraint rejects
duplicate manifest entries after canonical path normalization. Terminal import
rows retain enough target/result identity to answer retries after staging rows
and unowned objects are garbage collected. Commit changes content ownership and
namespace visibility in the same transaction; storage deletion is always an
outbox/GC consequence and never precedes the durable ownership decision.

The ordinary namespace schema also supplies the non-reusable parent
path-edge incarnation and monotonic child-set generation described above.
Every namespace mutation, including legacy create/unlink/rename code paths,
must use the same helper that updates those facts. P0 cannot claim the target
CAS until delete-the-fix tests prove that bypassing either update is detected.

Full terminal rows are never compacted before
`full_row_compact_not_before`. A valid `AcknowledgeImportResult`, sent only
after the client has fsynced its terminal local phase, permits compaction after
that minimum. A missing acknowledgement may retain the full row longer, but no
later than a configured maximum full-row retention time; at that bound it is
also compacted. The compact tombstone therefore stores the complete immutable
terminal result needed by `GetImport`, not merely a digest.

Only terminalization assigns `retire_after = terminal_at +` the configured
maximum offline-recovery window. The tenant-bound migration ID, request digest,
recovery-token hash, terminal state, result, and result digest remain in
`import_tombstones` until that time. During the interval, `CreateImport`
conflicts and `GetImport` returns the same terminal result. At and after
`retire_after`, every external API returns `import_id_retired`; the ID is never
interpreted as a new or not-found operation. If all durable rows have since
been removed, a valid allocation token whose `create_before` has passed also
resolves as retired rather than reusable. Recovery beyond this documented
maximum offline window fails closed with `promotion_recovery_required`. The
configured minimum and maximum full-row windows are both shorter than the
terminal retention window, making storage retention bounded and migration-ID
reuse behavior testable with a fake clock.

Physical tombstone deletion additionally requires every write grant, upload
attempt, and unowned seal candidate to be `gc_done`, every landing/provider-
mutation closure bound to have passed, and every corresponding debt charge to
be released. If a provider outage delays cleanup past `retire_after`, external
APIs still return `import_id_retired`; the terminal result blob and recovery
token hash may be pruned, while a minimal internal migration/tenant cleanup
anchor and all write/attempt ledgers remain.
Result-payload retention stays bounded; cleanup debt is never orphaned merely
to satisfy that bound.

The outbox is part of the transaction. It invalidates positive and negative
directory caches on other mounts. Staging emits no user-visible namespace or
search events. Namespace, content, and metadata are complete at commit;
semantic indexing remains asynchronous, but its task is registered through the
same committed outbox.

The initial implementation requires the destination to be absent. Support for
replacement of files or empty directories is a later compatibility step.

### Mixed-version namespace rollout gate

Target CAS is unsafe while any structural writer can bypass edge incarnation
or child-set generation updates. Schema availability alone is not capability
availability. Rollout is therefore ordered and fail-closed:

1. Add the namespace-version schema and import schema while promotion remains
   disabled. Deploy binaries that dual-write the new namespace facts on every
   create, link, unlink, rename, replacement, recursive delete, maintenance,
   and migration path.
2. Each namespace-writing process publishes a renewable writer-capability
   lease containing its build protocol version. The deployment controller
   drains all older processes and DB sessions, revokes their tenant-write
   credentials, and enforces a minimum writer version at admission. Writer
   credentials are deployment-generation scoped, so an old binary that never
   publishes a lease cannot continue writing through an unversioned shared
   credential. An absent, stale, or old-version inventory entry blocks
   enablement.
3. Under a per-tenant namespace maintenance barrier, backfill a fresh edge
   incarnation for every existing path, initialize every directory/root
   child-set generation, and audit that the new facts cover the complete
   canonical namespace. Backfill is retried idempotently but never overlaps an
   unfenced legacy writer.
4. Atomically mark that tenant `namespace_cas_ready` with a new CAS epoch only
   after the audit and all-writer gate pass. `CreateImport` records this epoch;
   `CommitImport` requires the tenant to remain ready at the same epoch.

Promotion API routes and the FUSE preview return `promotion_not_enabled` until
that per-tenant gate is ready. A rollback below the minimum writer version must
first disable new imports, advance the CAS epoch so every in-flight import
fails closed, wait for all imports to reach a terminal state, revoke promotion,
and only then admit old writers. Re-enabling after such a rollback repeats the
barrier, backfill, audit, and epoch change. There is no mixed-version window in
which an old writer and an enabled import may coexist.

### Subtree fence

The coordinator fences both source and destination subtrees on the initiating
mount before taking a manifest. Lookup, open, create, write, truncate, unlink,
rename, and writeback enqueue paths must participate in this fence. Other
mounts cannot share this in-memory fence; their target races are serialized by
the server-side parent path-edge incarnation, parent child-set generation, and
absent-child precondition at commit.

Prefix membership is component-aware: a fence for `/a` covers `/a` and
`/a/...`, but not `/ab`. Source and target fences use canonical paths and reject
`.`/`..`, separator, or case-normalization aliases according to the mounted
filesystem's namespace rules.

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

FUSE request cancellation is phase-sensitive. Before the server durably
accepts `VERIFIED -> COMMITTING`, cancellation stops new work and drives the
same owner-epoch abort path. After acceptance it cannot cancel the server
commit; the request returns outcome-unknown if necessary, retains both fences,
and resolves `GetImport` until `COMMITTED` or `ABORTED`.

The local root has a single-writer mount lock. The coordinator traverses with
`openat`/`fstat` and no-follow semantics, records each entry's device, inode,
reserved root incarnation, type, size, ctime, mtime and content hash, and
revalidates identity and metadata after each read, before `VerifyImport`, and
again immediately before `CommitImport`. Any mismatch or symlink swap observed
at those barriers aborts the import. Coordinator-owned traversal handles are
marked internal and do not fail the open-handle validation.

Supported mutations enter through Drive9/FUSE and are blocked by the subtree
fence through commit. Raw mutation of the private local root is unsupported and
cannot be made atomic with a remote transaction; a raw write after the final
revalidation has begun may be quarantined after a successful commit. P1 is
enabled only where the overlay root is private to Drive9. This is an explicit
support boundary, not a claimed fail-closed guarantee. Closing it would require
a filesystem snapshot or kernel-enforced external-writer exclusion and is a
separate capability.

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

The server state is authoritative for publish and accounting outcome:

| `GetImport` result | Client authority and fence | Server resources and next action | Local-record disposition |
| --- | --- | --- | --- |
| not found from an authorized, strongly consistent lookup | Source is authoritative; keep both paths fenced while reconciling the lost-create outcome. | No reservation or stage exists. | Fsync a local aborted result, then release fences and remove the active record. |
| `CREATED` | Source is authoritative; keep both fenced during recovery. | Reservation exists; no or partial stage. Resume with the current durable owner token, take over after expiry, or abort. | Retain until `ABORTED` or a later commit result. |
| `STAGING` | Source is authoritative; keep both fenced during recovery. | Reservation and partial hidden stage remain. Resume/take over or abort. Old control-plane mutations are epoch-fenced; already-issued write grants remain isolated to their old attempts and tracked through landing closure plus GC. | Retain. |
| `VERIFIED` | Source is authoritative; keep both fenced. | Immutable hidden stage and reservation remain. Commit with the same migration ID or abort; no entry mutation is allowed. | Retain. |
| `COMMITTING` | Outcome is unknown; neither source nor target is released. | Server owns completion; lease expiry and client abort do nothing. Poll `GetImport`; a recovered server worker reaches `COMMITTED` or `ABORTING`. | Retain; never create a second migration ID. |
| `ABORTING` | Source remains authoritative, but both paths stay fenced until cleanup completes. | Server resumes hidden-stage deletion and exactly-once reservation release; no client mutation or takeover is accepted. | Retain and poll. |
| `ABORTED` | Source is authoritative; target was not published. | Reservation is released, sealed ownership is detached, and the terminal reason is durable. Upload/seal-attempt cleanup, provider-mutation claims, and debt may continue from retained ledgers until `gc_done`. | Fsync the local aborted result, release fences, then acknowledge/retire the active record; the server tombstone remains for retry identity. |
| `COMMITTED` | Target is authoritative; keep both fenced until source reconciliation finishes. | Quota is settled, sealed content ownership and namespace/outbox result are durable; cleanup of temporary and unowned candidates cannot remove committed versions and continues independently until `gc_done`. | Verify the source-incarnation UUID, quarantine only the matching source, publish inode/cache state, fsync `DONE`, release fences, then acknowledge/retire the active record. |
| unreachable or invalid response | Authority is unresolved; keep both paths fenced or fail the mount closed. | No cleanup, quota release, abort, or new migration is guessed. | Retain and return `promotion_recovery_required`. |

The durable local phase refines, but never overrides, that server result:

| Last durable local phase | Recovery action |
| --- | --- |
| Local record missing | No client operation existed. A server import with no matching record is reclaimed only by its lease/GC state machine. |
| `PREPARED` | Install fences and call `GetImport`; not-found is the only proof that create never became durable. |
| `STAGING` | Reconcile owner epoch and server state before uploading or aborting. |
| `STAGED_VERIFIED` | Reconcile before commit; do not mutate the frozen manifest. |
| `COMMIT_REQUESTED` | Query `GetImport`; never retry with a new migration ID or infer failure from disconnect. |
| `REMOTE_COMMITTED` | Verify the durable source-incarnation UUID before quarantine. Never roll back the target. |
| `SOURCE_QUARANTINED` | Target is authoritative. A janitor removes only the matching migration-owned quarantine identity. |
| Manifest/record/token missing, corrupt, or mismatched | Fail closed and require repair; do not guess, take over, or delete either tree. |

Once the server reports committed, old uploaders and recovery attempts cannot
write a different manifest under the same migration ID.

The server durably accepts commit ownership before work is detached from the
request cancellation path, or completes the database transaction before a
request can report success. Client disconnect does not create a second commit
owner.

## Concurrency Rules

- Two promotions of the same source or destination have one winner through the
  subtree fence and server target CAS.
- A different client creating, deleting, or renaming the target during staging
  advances the parent child-set generation and causes final commit to conflict,
  even when the child is absent again at commit. Moving, moving-away-and-back,
  deleting, or recreating the target parent changes the parent edge
  incarnation and also conflicts. The source remains visible.
- A client timeout does not cancel an accepted server commit. The caller
  resolves the outcome by migration ID. Commit, abort, and lease GC race through
  the import-state CAS and have one winner.
- A coordinator restart reuses its durable current token while the lease is
  live, or takes over an expired client-owned state with a new token and
  incremented epoch. The previous epoch is rejected by every mutation.
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
- lease GC can acquire only `CREATED`, `STAGING`, or `VERIFIED`; it never
  cancels `COMMITTING`, and a crashed `ABORTING` cleanup is resumed until the
  exactly-once release is durable;
- accepted data-plane writes can target only migration-owned temporary
  grant identities; their durable ledgers outlive abort/commit until every
  landing bound and provider-mutation bound closes and a later exact sweep
  proves cleanup, while sealed and committed versions are immutable;
- terminal full rows and compact tombstones follow the acknowledgement and
  post-terminal retirement contract; an expired allocation or forgotten ID is
  never accepted as new;
- entry, byte, depth, and time limits are checked before or during hidden
  staging and never produce a partial final target.

## Delivery Plan

### P0: primitive and specification

- freeze this contract and public capability naming;
- add namespace path-edge incarnations and child-set generations to every
  structural mutation path before enabling target CAS;
- implement hidden staging, idempotent upload, manifest verification,
  versioned quota reservation, owner epoch/token takeover, the complete import
  state CAS, `CommitImport`, committed outbox/status lookup, and garbage
  collection;
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
- source-root symlink and no-durable-xattr filesystem rejected before staging;
- the exact reserved root-incarnation xattr excluded from the manifest while
  any user xattr and a reserved-key descendant are rejected;
- mode and mtime round trip;
- Unicode, spaces, long paths, and tree-size limits;
- target absent, present, wrong type, nonempty, or inside source;
- mixed local/remote/Git/fs-layer descendants rejected with zero side effects.

### Crash recovery

Kill and restart before and after:

- local record and manifest fsync;
- content-attempt idempotency-key journal fsync, request send, and returned
  attempt-ID journal fsync;
- write-grant idempotency-key journal fsync, grant response, proxy acceptance,
  bounded body completion, provider-write claim, and provider-write outcome;
- durable provider-mutation claim, multipart-complete/copy request send,
  provider-side completion, and returned outcome persistence;
- durable seal intent, create-once sealed-object creation, sealed-version
  verification, and DB attach;
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
- target `create -> delete` ABA between precondition issue and commit;
- target parent rename away, rename away-and-back, and delete/recreate;
- unrelated target-parent sibling mutation conservatively returns
  `target_precondition_changed`/`EAGAIN` in P0 while the requested child remains
  absent; it never masquerades as `EEXIST`;
- two promotions competing for source or target;
- commit versus abort and commit versus lease GC;
- activity deadline versus renew, takeover, write-grant issuance/acceptance,
  body completion, provider mutation, verify, and commit acceptance;
  client-owned states go `ABORTING`, while a commit accepted before the
  deadline remains queryable and reaches a terminal state;
- coordinator crash followed by expired-lease takeover; every old-epoch renew,
  content-create, multipart-complete, seal, entry, verify, commit, and abort
  control-plane mutation is rejected;
- a proxy grant is accepted before `accept_before`, cleanup observes absence,
  and its tracked PUT lands just before `landing_not_after`; sealed/committed
  bytes never change, pre-bound absence cannot set `gc_done`, and the
  post-bound sweep deletes the late object;
- an `UploadPart` accepted before grant expiry lands after takeover/abort and
  cannot be completed or referenced; its write-grant claim/debt survive until
  provider abort plus the post-landing-bound sweep converges to no parts;
- native replayable S3 PUT and `UploadPart` capabilities are rejected before
  staging because request-start expiry is not a landing bound; deleting this
  admission check makes an expiry-before-cleanup/landing-after-cleanup fixture
  leave an orphan;
- abort cleanup first observes an absent destination or incomplete multipart,
  then a previously claimed server-side complete/copy lands and its worker
  crashes before recording the outcome; the pre-bound absence cannot set
  `gc_done`, and the post-bound sweep removes the late side effect;
- verify versus a late upload, and content completion/seal versus an aborted
  terminal import;
- `CreateImportContent` response loss followed by same-key retry returns one
  logical attempt; `CreateImportWriteGrant` response loss likewise returns one
  grant/mutation/debt charge, while a new key creates a separately tracked and
  charged replacement;
- after owner takeover, retrying an earlier epoch's content-attempt or
  write-grant key is rejected as stale and cannot reissue access to an old
  temp identity;
- crash after create-once seal succeeds but before its DB attach resumes the
  same seal intent/version without consulting the mutable temp key; deleting
  the intent makes the failure-injection test leak or attach wrong bytes;
- deleting the provider-mutation claim or allowing cleanup before its closure
  bound makes deterministic delayed multipart-complete and delayed seal-copy
  tests leave an object/version/part after the ledger would have been removed;
- writeback enqueue immediately after a drain attempt;
- direct local-root file replacement and symlink swap while a snapshot is read;
- a second mount reading while publish occurs;
- a committed outbox invalidating a second mount's positive and negative cache.

### Import and accounting

- the same migration ID with a different target, manifest, entry payload, or
  content hash conflicts;
- the same content-attempt idempotency key with a different request conflicts;
- missing, extra, duplicate, absolute, escaping, or symlink-following manifest
  entries are rejected;
- an upload cannot exceed the reserved entry or byte total;
- repeated attempt issuance, seal creation, and owner takeover cannot exceed
  the internal cleanup-debt budget; customer quota release does not release
  temporary or unowned-candidate debt before `gc_done`;
- each direct-write or multipart-part generation has its own maximum-byte debt
  charge before grant issuance; the sum of concurrently live grants, rather
  than logical content size alone, is enforced against the cleanup budget;
- replaying a direct upload against a versioned backend either yields an
  enumerated, separately charged version identity or is rejected; an
  unbounded/unknown-version native replay mode can never pass admission;
- multipart tests bound the approved part-number/generation/size set, reject
  completion with an unlisted or duplicate generation, and retain superseded
  part debt until exact abort/delete plus landing closure; completion waits for
  every superseded same-number generation to close and for exact part-list
  equality, so a delayed older generation cannot replace the selected part;
- a mutable upload and its unowned sealed candidate are charged separately at
  their simultaneous peak; attach atomically converts candidate debt to
  reservation-backed staging, while abort retains both cleanup charges until
  their individual `gc_done` transitions;
- request/response loss around upload-attempt creation, provider-mutation
  claim, seal attach, abort, and debt transfer neither double-charges nor
  releases debt before the corresponding bytes can no longer exist;
- commit converts the reservation exactly once after response loss and retry;
- abort and GC release it exactly once and cannot race a committed import;
- committed content references survive staging GC;
- verify records an immutable storage version, and mutating the upload-attempt
  key afterward cannot change the bytes observed by commit/read;
- removing the seal/version binding or deleting the retained upload-attempt
  ledger makes the late-PUT/late-part tests fail;
- a structural-reset outbox entry exists exactly once for each commit, while
  no staging entry is indexed or emitted;
- response loss and server restart are injected in every state:
  `CREATED`, `STAGING`, `VERIFIED`, `COMMITTING`, `ABORTING`, `ABORTED`, and
  `COMMITTED`; each case asserts source/target authority, fence retention,
  staging ownership, reservation/committed quota, and local-record retention;
- deleting the owner-epoch comparison or allowing takeover of a live,
  server-owned, or terminal state makes a focused test fail;
- lease renewal, write-grant bounds, and provider-mutation bounds at the
  activity boundary never extend beyond it; every active import is forced into
  `COMMITTING` or `ABORTING` before terminal retention/retirement begins, and
  its public result remains `promotion_deadline_exceeded`/`ETIMEDOUT` across
  restart and retry.

### Upgrade and rollback

- fresh schema with promotion disabled accepts old and new namespace traffic;
- a live or stale old-writer fixture blocks `namespace_cas_ready` enablement;
- backfill racing an unfenced old writer is rejected, never marked ready;
- after all-writer cutover and barriered backfill, create/unlink and
  rename/rename-back through every structural writer advance the recorded
  namespace facts;
- a subtree rename refreshes the edge incarnation of every descendant row
  whose canonical path changes; deleting that refresh makes an ancestor
  rename-away-and-back test fail;
- changing or disabling the tenant namespace CAS epoch after `CreateImport`
  makes `CommitImport` fail closed;
- rollback below the minimum writer version is rejected while imports are
  active or promotion remains enabled, and re-enable requires a fresh audit;
- deleting the writer-version/admission check makes the old-writer fixture
  reproduce target ABA and fail the test.

### Local recovery and reuse

- recovery reinstalls fences before the first lookup;
- `COMMIT_REQUESTED` with an unreachable server blocks only the affected paths
  or fails the mount closed;
- successful publish followed immediately by recreating the old path creates a
  new source-incarnation UUID that quarantine cleanup cannot hide or delete;
- a fake local filesystem reuses the same `(st_dev, st_ino)` after
  delete/recreate but returns a new reserved UUID; recovery and quarantine must
  refuse to touch it, and deleting the UUID comparison makes the test fail;
- crash points after create-only xattr, after inode fsync, and after local-record
  fsync either leave an inert marker or a fully recoverable identity;
- the import-scoped uploader cannot deadlock on the user-namespace subtree
  fence;
- direct source mutation or identity change rejects the migration rather than
  uploading a torn snapshot;
- a fence for `/a` blocks `/a` and `/a/...` but not `/ab`;
- FUSE cancellation before commit acceptance reaches `ABORTED`, while
  cancellation after acceptance retains fences and resolves the server-owned
  `COMMITTING` outcome;
- a supported FUSE write after verify remains blocked; a raw private-root write
  in the documented final-revalidation gap is explicitly tested as
  out-of-contract and is never advertised as fail-closed;
- terminal result acknowledgement, minimum and unacknowledged-maximum full-row
  compaction, complete-result tombstone lookup, and post-terminal ID retirement
  are tested with a fake clock; an active import never becomes retired, and a
  replay after terminal retirement is `import_id_retired`, never a fresh
  import;
- a provider cleanup outage past `retire_after` prunes the externally
  recoverable result but retains the internal attempt/cleanup anchor until
  `gc_done`, then removes it.

### Handle behavior

- preview: any source file or directory handle is rejected before staging;
- transparent support: open, rename, write, append, truncate, fsync, close,
  multi-fd, unlink/recreate, and recovery all preserve identity and publish one
  final lineage.

### Regression strength

Tests must fail when independently deleting:

- the subtree fence;
- migration idempotency;
- the parent path-edge incarnation, child-set generation, or absent-child term
  of the target CAS;
- the server-state CAS between commit, abort, and GC;
- owner epoch/owner-token fencing, recovery-token authorization, and
  expired-lease takeover;
- immutable sealed-version binding, durable seal intent, write-grant and
  upload/seal-attempt retention through landing closure/grace, and
  content-attempt/grant idempotency;
- native-capability admission requiring a finite landing bound plus single-use
  or completely enumerable replay identities;
- provider-mutation claims, their side-effect closure bounds, and the required
  post-bound exact-key/multipart rescan before `gc_done`;
- per-write-grant/part-generation and unowned-sealed-candidate debt charges
  plus their atomic attach/abort transfer;
- the activity deadline and forced terminalization before terminal retention;
- the namespace all-writer rollout/admission gate;
- quota reservation conversion and content-ref ownership transfer;
- the reserved source-incarnation UUID comparison used by recovery and
  post-commit quarantine;
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
