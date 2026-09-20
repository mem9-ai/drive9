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

The first P0 storage implementation is intentionally narrower than the full
protocol: it accepts only trees whose regular-file payloads fit the existing
DB9 inline-content policy. The current production S3-compatible adapter cannot
prove a finite latest side-effect time, even when Drive9 proxies the request,
so object-backed promotion fails closed until a provider-specific contract is
implemented and enabled.

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
- object-backed regular-file payloads until a production provider supplies the
  operation-identity and finite latest-landing contract below;
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
| Selected storage backend or object size lacks the required promotion guarantees | `promotion_storage_backend_unsupported` | `EXDEV` |
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
Owning the single-writer local root is not source authorization. The real
`Dat9FS.Rename` handler may classify the two paths only to recognize a possible
cross-layer operation; it must not return from an early cross-layer branch or
create promotion state until it performs the same daemon-side POSIX preflight
as an ordinary rename using `RenameIn.Owner`:
source existence, both parents' write plus search permission, sticky-directory
ownership rules for source and destination, parent/type/symlink validity,
self-subtree rejection, target type compatibility, and target-directory
emptiness. Drive9 cannot rely on kernel `default_permissions`: it is not enabled
for every mount/profile, and macOS explicitly defers these checks to the
daemon. The validation portion of `renamePreflight` must therefore be extracted
or reused as one side-effect-free check; overlay-parent pruning or other cleanup
is not part of that check and cannot run until validation succeeds. This shared
preflight runs while the in-memory subtree fence is held but before any durable
promotion record, `PlanImport`, `AllocateImportID`, `CreateImport`, quota
reservation, or hidden staging. Only after it succeeds may P1 enter the
promotion branch and apply its stricter absent-target/no-open-handle eligibility
checks. Failure returns the ordinary rename errno and produces zero promotion
side effects.

The server cannot infer authority over a local-only source path. It does,
however, independently authorize the canonical destination against the
**current request's** tenant identity and filesystem scope on every
client-facing import call. Migration IDs and allocation/owner/recovery tokens
are idempotency, operation-identity, and epoch-fencing secrets; none is a bearer
credential and none preserves a revoked or narrowed filesystem scope.

The normative request authorization matrix is:

| API | Required current-request authorization before any visible effect or mutation |
| --- | --- |
| `PlanImport`, `AllocateImportID` | Tenant authentication plus `write` on the canonical target. Allocation stores that target in its claim. |
| First/retried `CreateImport` | Tenant authentication plus `write` on the request target; it must equal the plan and allocation-claim target. Recovery-before-admission does not bypass this check. |
| `PutInlineImportContent`, future content/grant/complete/seal/attach calls, `VerifyImport`, `RenewImportLease` | Resolve the import under the authenticated tenant, then require current `write` scope on its stored target before reading a body, returning a capability, renewing, or mutating state. |
| `CommitImport` | Require current `write` scope on the stored target before the `VERIFIED -> COMMITTING` acceptance CAS. |
| `AbortImport`, `TakeOverImport`, `RetireImportAllocation`, `AcknowledgeImportResult` | Require current `write` scope on the stored or authenticated-proof target in addition to the applicable owner, recovery, or allocation proof. |
| `GetImport` | Require current `read` scope on the stored or authenticated-proof target plus the applicable allocation proof or recovery token before returning existence, state, target, or result. |

The HTTP dispatcher admits scoped tokens only to import routes whose handler
implements this table. Entry handlers authorize their canonical request
target; continuation and recovery handlers additionally authorize the
server-stored target rather than trusting a caller-supplied path. Revocation,
token expiry, or scope narrowing therefore rejects every later client mutation
and read, even when the caller still has a migration secret. Existing
client-owned state remains fenced and is eventually aborted by lease/deadline
GC.

Authorization cannot itself become an existence oracle. Target-bearing
requests (`PlanImport`, `AllocateImportID`, and first or retried `CreateImport`)
authorize the request target before plan, claim, or import lookup, so an
out-of-scope request always returns the same canonical `403 fs access denied`
regardless of whether the supplied ID exists. After a retried Create finds an
accepted claim/import, it also authorizes the stored target before returning
recovery; denial is non-disclosing rather than a target-mismatch oracle.
ID-only continuation handlers look up tenant-owned state internally only to
obtain its stored target; when that target is outside the current scope, they
return the exact canonical `404 import not found` response used for an unknown
or unauthorized retired-sequence proof, including status, body shape, and cache
policy. They do not reveal whether an active row, terminal row, tombstone,
allocation claim, retired sequence, target, or result was found. A valid
current scope plus the applicable operation credential is required before
normal active, terminal, or `import_id_retired` semantics are exposed.

Server-owned work is deliberately different. Once `CommitImport` wins
`VERIFIED -> COMMITTING`, or abort/deadline/GC wins a transition to
`ABORTING`, the durable worker runs under Drive9's service authority and reaches
a real terminal state even if the initiating user token is later revoked. It
does not re-evaluate mutable caller scope after acceptance; otherwise revocation
could strand an accepted commit or cleanup. The final transaction still checks
tenant ownership, immutable target identity, quota, namespace-CAS readiness,
and every structural precondition described below.

`CreateImport` reserves the canonical manifest byte and entry totals and
returns a versioned quota reservation. Inline content writes, and any future
external upload mode, cannot exceed those totals.

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
  | P0: idempotent DB-inline content; future: proven provider adapter
  | manifest verification
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
allocation_sequence
allocation_idempotency_key
allocation_proof
source_path
target_path
source_identity (local root UUID, reserved source-incarnation UUID)
manifest_hash
create_request_digest
target_expectation (canonical target, expected absence fixed to true in P0,
                    server precondition digest after CreateImport)
storage_plan (mode, digest, backend capability generation, limits)
staging_ref
owner_epoch (0 until returned or recovered from GetImport)
owner_token
recovery_token
activity_deadline (unset until returned or recovered from GetImport)
fence_state (`INSTALLING` or `INSTALLED`)
phase
created_at
updated_at
```

The manifest is written to a temporary file, fsynced, renamed to its final
name, and followed by a parent-directory fsync. The checksummed migration record
is then written with the same temp-file, fsync, rename, and directory-fsync
sequence. The record and its allocation, owner, and recovery tokens are private
mode `0600` state. Recovery accepts only a complete record whose manifest hash
matches; it never assumes the pair was atomically written.

Per-content staging progress is a checksummed, fsynced journal in the same
private state directory. P0 records the relative content identity, hash, size,
and random inline-content idempotency key before `PutInlineImportContent`, then
records the returned content ID. Recovery truncates only an incomplete tail and
retries the last complete key. If a future external-object adapter is enabled,
the same rule applies separately to its content attempt and every write-grant
key, so response loss is never interpreted as permission to allocate a
replacement side effect.

The initial record contains the allocation sequence/proof, canonical target,
expected absence, and exact create-request digest. After a successful or
recovered `CreateImport`, the coordinator rewrites and fsyncs the record with
the server precondition digest and owner epoch and advances the local phase to
`STAGING` before sending content. The target-precondition digest is for
reconciliation only; the server always uses its own stored namespace facts at
commit.

Phases are monotonic:

```text
PREPARED
  -> CREATE_REQUESTED
  -> STAGING
  -> STAGED_VERIFIED
  -> COMMIT_REQUESTED
  -> REMOTE_COMMITTED
  -> SOURCE_QUARANTINED
  -> LOCAL_CLEANED
  -> DONE
```

No phase transition relies only on an in-memory flag.

`fence_state` is an orthogonal local durability field, not a server phase. A
complete record first becomes durable with `fence_state=INSTALLING`; recovery
must treat that value as requiring both prefixes to remain fenced. The local
fence manager then confirms that the already-installed registry entry is bound
to this record, rewrites and fsyncs `fence_state=INSTALLED`, and only then lets
the normal `PREPARED` workflow continue. Both values are checksummed recovery
inputs and neither authorizes a remote request.

`PREPARED` means the complete request identity is durable and the coordinator
has **not** dispatched `CreateImport`. Immediately before the first network
write that can dispatch that request, it rewrites and fsyncs
`CREATE_REQUESTED`; therefore this phase means the create outcome may be
unknown. A transport send is forbidden directly from `PREPARED`. This ordering
is the local proof that distinguishes “never sent” from “possibly accepted.”

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
PlanImport(target, expected_target_absent=true, canonical_manifest)
  -> manifest_hash, entry_total, byte_total, max_content_size,
     storage_mode, storage_plan_digest, backend_capability_generation,
     limits, plan_expires_at
AllocateImportID(canonical_target, expected_target_absent=true,
                 allocation_idempotency_key)
  -> migration_id, allocation_sequence, allocation_proof, create_before
CreateImport(migration_id, allocation_proof,
             target, expected_target_absent=true,
             canonical_manifest,
             manifest_hash, entry_total, byte_total, max_content_size,
             storage_plan_digest, backend_capability_generation,
             plan_expires_at,
             owner_token, recovery_token)
PutInlineImportContent(migration_id, owner_epoch, owner_token,
                       relative_path, entry_hash, size, checksum_sha256,
                       content_idempotency_key, bounded_body)

# Future external-object extension; disabled for current production adapters.
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
AttachImportContent(migration_id, owner_epoch, owner_token,
                    relative_path, entry_hash, import_content_token)
VerifyImport(migration_id, owner_epoch, owner_token, manifest_hash)
CommitImport(migration_id, owner_epoch, owner_token)
GetImport(migration_id, allocation_proof_or_recovery_token)
RetireImportAllocation(migration_id, allocation_proof)
AbortImport(migration_id, owner_epoch, owner_token)
RenewImportLease(migration_id, owner_epoch, owner_token)
TakeOverImport(migration_id, expected_owner_epoch,
               recovery_token, new_owner_token)
AcknowledgeImportResult(migration_id, recovery_token,
                        terminal_state, result_digest)
```

Every method is idempotent for the same migration identity and payload. Reuse
with different target, manifest, or content returns conflict.
`PlanImport` is side-effect free: it validates the complete bounded canonical
manifest but creates no import row, reservation, object, or cleanup debt. P0 is
an absent-target-only protocol: both `PlanImport` and `CreateImport` require
`expected_target_absent=true`. `PlanImport` rejects `false` instead of issuing a
plan, and a first `CreateImport` rejects `false` before admission or side
effects. Target replacement remains outside this API contract.
`CreateImport` repeats the authorization and provider check,
requires the same server-authenticated plan digest and backend capability
generation, and returns
`promotion_storage_backend_unsupported` before creating the import row or
reserving quota if the plan is no longer eligible. The manifest summary is not
trusted: each staged content request enforces its own size and checksum, and
`VerifyImport` recomputes the complete entry/byte/hash summary.

The plan digest is an expiring server MAC over tenant, canonical target,
`expected_target_absent=true`, manifest summary, selected storage mode, exact
limits, capability generation, and `plan_expires_at`; `PlanImport` keeps no
server-side reservation. Replaying or altering any claim fails verification.
`CreateImport` recomputes and compares the expected-absence claim rather than
trusting the duplicate request field. It accepts the plan only before expiry
and only while the recorded generation still equals current runtime
configuration **when creating a row for the first time**. It must also receive
the identical canonical manifest bytes, recompute every summary and the MAC
input, and atomically persist the import row, quota reservation, and complete
hidden `import_entries` set. A hash or summary alone is not an entry transport
and cannot create an import.

`CreateImport` resolves lost responses before it performs new-admission checks:

1. Validate the server-authenticated allocation proof, its tenant/target/ID/
   sequence binding, and current target scope. Then look up
   `(tenant_id, migration_id)` in active/full-terminal rows, retained
   tombstones, the allocation claim, and sparse retired-sequence set before
   treating it as new.
2. If an active/full row exists, compare the stored immutable create-request
   digest, owner-token hash, and recovery-token hash. An exact match returns the
   current state, reservation, and manifest-entry result even if
   `create_before` or `plan_expires_at` has since passed or the current backend
   capability generation has changed. A retained terminal tombstone performs
   the same comparison and returns its terminal result. A valid proof whose
   sequence is at or below the tenant's retired watermark, or is present in the
   sparse retired set, returns `import_id_retired`; without a valid proof and
   current scope it is non-disclosing not-found. These are recovery of an
   accepted transaction, not new admission. Any visible mismatch returns
   conflict and cannot reset the epoch.
3. Only when no accepted row exists does the server lock the durable allocation
   claim. It rechecks for an accepted winner, then validates claim state/proof,
   `create_before`, plan MAC/expiry, current capability generation,
   authorization, limits, and quota before attempting the atomic insert.
4. The transaction changes the claim from `ALLOCATED` to `ACCEPTED` together
   with the import, reservation, and entry insert. Concurrent identical creates
   serialize on the claim and the loser applies step 2; a mismatched loser
   conflicts. A concurrent retirement winner makes create return
   `import_id_retired`. No path allocates a second reservation or entry set.

The create-request digest covers every immutable request field including the
canonical manifest hash, canonical target, `expected_target_absent=true`, and
all plan claims. Repeating `CreateImport` with either the owner or recovery
token changed never acts as takeover; the caller must follow the explicit
expired-lease takeover contract. Changing the expected-absence field on a
recovery request therefore conflicts rather than recovering or creating a
replacement import.

Migration IDs are server-generated opaque identifiers assigned alongside a
tenant-bound, monotonically increasing `allocation_sequence`; the transaction
rejects an ID collision, and a sequence is never decremented or reassigned.
The database type and check constraint forbid sequence wrap; exhausting the
representable range fails with `promotion_identity_budget_exceeded` before
issuing an ID rather than resetting or reusing a sequence.
The server also returns an opaque, authenticated
`allocation_proof` binding proof-format/key version, tenant, sequence,
migration ID, canonical target, fixed expected-absence claim, `create_before`,
and allocation-request digest (including the idempotency-key digest).
Verification-only key material for issued proof versions remains available;
proof validation does not require a per-ID row. The proof is an operation-
identity credential, not tenant authorization.

The coordinator generates and fsyncs an allocation idempotency key before the
request. The allocation transaction locks the tenant identity-budget row and a
global materialized-identity counter, applies the limits below, increments the
tenant sequence, and inserts one quota-neutral claim keyed by tenant/sequence/
migration ID plus the request-key digest. Retrying the same key and request
while that bounded mapping is retained returns the same ID, sequence, proof,
and deadline; reusing it with different claims conflicts. The claim retains the
exact bounded proof blob until sequence retirement, so signing-key rotation
cannot change a lost-response retry. Only after this response is fsynced does
the coordinator
generate/fsync owner and recovery tokens and call `CreateImport`.

`AllocateImportID` resolves an existing idempotency-key row before applying
new-admission limits. An exact retry therefore recovers its original result
even while the tenant or deployment is at capacity; a mismatch conflicts. This
idempotency guarantee lasts through the documented allocation/terminal
recovery window, while the claim/import/tombstone still exists. After its
sequence has been retired and folded into the watermark, the per-request key
mapping is intentionally gone; an allocation request at that point is a new
operation and receives a new non-reused sequence. No safety or deduplication
decision may use the caller's idempotency key after that bounded window.

Identity admission is bounded independently from byte quota. Each tenant has a
durable allocation token-bucket/rate limit, a maximum number of live allocation
claims, and a maximum sequence window
`last_issued_sequence - retired_through`. The deployment also caps the total
number of materialized allocation identities across all tenants. One sequence
consumes exactly one identity slot whether it is represented by an allocation
claim, an accepted claim plus import, a terminal tombstone, or a sparse retired
marker. State transitions transfer that slot without charging it again; an
out-of-order retirement preserves the charge until watermark compaction
consumes the marker. The schema permits only a fixed, documented maximum
number of control-plane rows per charged identity, so the identity caps also
place a hard bound on these tables and indexes.

Allocation checks and charges all applicable counters in the same transaction
that increments the sequence and inserts the claim. Tenant rate, live-claim,
or sequence-window exhaustion returns the stable
`promotion_identity_budget_exceeded` (`429`, FUSE `EDQUOT`); global backpressure
returns `promotion_identity_capacity_unavailable` (`503`, FUSE `EAGAIN`). Both
produce no claim, sequence, proof, quota reservation, or staging side effect.
Limits are configuration-versioned and observable; callers cannot bypass them
with new idempotency keys. Counter/row disagreement fails allocation closed and
raises a repair alert; a reconciliation job may repair counters from the
bounded materialized state but never guesses that an identity was retired.

The first-create transaction locks that allocation claim. It accepts only an
`ALLOCATED` claim with the matching authenticated proof and database time before
`create_before`, then creates the import/reservation/entries and changes the
claim to `ACCEPTED` atomically. An `ACCEPTED` claim resolves through the
existing-row recovery contract; a `RETIRED` or missing claim can never create
an import. `RetireImportAllocation` locks the same claim and CASes
`ALLOCATED -> RETIRED`. If it races a delayed `CreateImport`, exactly one wins:
retirement makes that and every later create fail irreversibly, while an
accepted create makes retirement return the existing import for recovery.
Thus `GetImport=not found` is only an observation, never proof that a request
cannot still commit. Only the durable local `PREPARED` phase (request provably
never dispatched) or a successful claim retirement supplies that proof.

Recovery of an already-accepted row follows the digest/token comparison above
and is not a new allocation. An active import has no
allocation-time retirement deadline: its durable row remains queryable until
the server reaches a terminal state, and terminal retention begins only then.

`migration_id` is scoped by tenant. Content references are server-issued for
that import and tenant; the client cannot attach an arbitrary storage key or a
content reference owned by another tenant.

#### P0 storage eligibility and inline staging

P0 freezes a concrete, code-backed provider matrix rather than assuming that a
proxy turns every synchronous object API into a bounded operation:

| Storage path | Production code/API evidence | Durable operation identity and finite latest landing | P0 status |
| --- | --- | --- | --- |
| DB9 inline (`content_blob`) | `Dat9Backend.shouldStoreInDB`: enabled only when `smallInDB && size < inlineThreshold`; the default cutoff is `DefaultInlineThreshold` | The content row and its idempotency result are committed by TiDB; there is no external provider side effect | **Supported** for every regular file strictly below the recorded threshold |
| `LocalS3Client` | `pkg/s3client` identifies it as the testing implementation | The in-process fake can model a bound, but this is not production evidence | **Test/E2E only**; never enables production promotion |
| Native AWS/Aliyun/Tencent S3-compatible upload | `AWSS3Client.PresignUploadPart`; Aliyun and Tencent construct the same `AWSS3Client` | S3 checks presigned expiry when the HTTP request starts, not when its effect finishes; replay and in-flight parts lack a finite latest-landing proof | **Unsupported** |
| Proxied AWS/Aliyun/Tencent S3-compatible upload or copy | Synchronous `AWSS3Client.PutObject`, `CompleteMultipartUpload`, and `UploadPartCopy`; the production interface has no proxied `UploadPart` operation | The interface returns no durable provider operation ID or provider-enforced latest-landing time. Context cancellation/timeout is not evidence that S3 cannot finish later | **Unsupported** |
| Unknown/future provider | No reviewed adapter contract | Missing proof is denial, not an inferred capability | **Unsupported by default** |

The S3 conclusions follow the provider contract, not a fake timing model. AWS
documents that a transfer begun before a presigned URL expires can continue
after expiry, because expiration is checked when the HTTP request starts
([presigned URL expiration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html)). It also documents that in-progress part uploads may still succeed after abort and must be listed/aborted until gone
([AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)), while `CompleteMultipartUpload` may continue processing after its initial response and can report a later embedded error
([CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)). A Drive9 proxy can bound request acceptance and body receipt, but it cannot add a provider guarantee absent from those APIs.

`PlanImport` selects `db9_inline` only when inline storage is enabled for the
tenant, every regular file is strictly smaller than the captured inline
threshold, and the per-request, total-inline-byte, entry, and transaction limits
admit the complete tree. It binds those limits plus the server
configuration/capability generation into the plan digest. An empty tree is
eligible. If any file requires object storage, or the inline configuration
changes before `CreateImport`, the operation fails before an import row, quota
reservation, hidden entry, or object-store request exists. There is no partial
inline staging followed by a late object-store rejection. Once `CreateImport`
accepts a plan, its captured mode and limits remain immutable for that import;
a later configuration rollout affects only new plans.

Eligibility is for the resolved content path, not for the tenant's unused S3
client. A production tenant configured with `AWSS3Client` may still promote an
all-inline tree; it cannot promote a tree containing an object-backed file.
The planner uses an explicit, closed capability enum and configuration
generation produced by the real backend constructor. It never promotes an
unknown wrapper because it happens to implement `S3Client`, nor infers support
from provider or endpoint strings.

`backend_capability_generation` is a cluster-visible, tenant-scoped database
fact, not an unsynchronized process-local counter. A storage/config rollout
advances it before minting new plans. The first-create transaction locks and
rechecks that row, and the serving process must also report the exact closed
adapter capability; disagreement fails closed. Thus a plan minted on one server
cannot be admitted by a differently configured server. Existing accepted rows
continue under their captured inline mode/limits and are recovered by request
digest rather than reclassified.

The canonical manifest contains one normalized, ordered tuple per entry. Every
tuple includes path, type, mode, mtime, and entry hash. A regular-file tuple
also includes expected size and SHA-256; a symlink tuple includes the link text;
a directory has no content field. `CreateImport` persists all tuples before
returning. Its one transaction also creates the import row and reservation, so
there is no state in which content is accepted but the expected entry metadata
is absent. This is the only P0 entry-creation path: empty directories, symlinks,
mode, and mtime do not depend on a later content call.

`PutInlineImportContent` accepts a bounded body only for a `db9_inline` import
and an already-recorded regular-file entry. Before reading/storing the body it
checks the path, entry hash, declared size/SHA-256, owner epoch, state, and
remaining reservation against that tuple. It reads at most expected-size plus
one byte, computes SHA-256 itself, and inserts nothing unless actual size/hash
match. It then stores an immutable hidden content row and atomically fills the
entry's previously-null content ID. The
random idempotency key and request digest are unique within the import:
response-loss retry with identical input returns the same content ID; reuse
with different input conflicts. A second different content ID cannot bind the
same entry.

`VerifyImport` requires every regular-file entry to have exactly one bound
content ID whose stored size/hash matches the tuple, and every directory or
symlink to have none; it rejects any missing, extra, or mismatched entry before
freezing the manifest. Commit changes content ownership and publishes those
already-complete namespace entries in one database transaction without copying
the blobs. Abort deletes hidden rows and releases the reservation idempotently.
The content write's state/owner check, content insert, and entry bind are one
transaction: if its commit result is unknown, retry/query by the same key
discovers the single result; if abort wins first, the state CAS rejects the
write, and if the content write wins first, abort owns and deletes it. No
provider operation can materialize after that database ordering. P0 never calls
`CreateImportContent`, `CreateImportWriteGrant`, `AttachImportContent`, or
`SealImportContent`.

The initial P1 preview consequently supports only all-inline trees. A tree with
one object-backed file returns `promotion_storage_backend_unsupported`/`EXDEV`
before remote side effects. This limitation is advertised as capability scope,
not silently routed through the ordinary S3 upload path.

#### Future external-object capability contract (not enabled in P0)

The remainder of this section is a necessary contract for a future production
object adapter. It does **not** make the current `AWSS3Client` eligible. Such an
adapter must add concrete operation identity, finite landing bounds, exact
enumeration, accounting, cleanup, and adapter-classification tests before its
mode can appear in `PlanImport`.

Even in that future mode, `CreateImport` remains the sole entry/metadata
transport. `AttachImportContent` may only bind an immutable sealed content
token to the matching already-persisted regular-file tuple; it cannot create an
entry or modify type, path, mode, mtime, size, hash, or symlink text.

An object-store upload capability can outlive a server epoch check. Therefore
no direct or multipart capability ever writes a key or version that a verified
manifest references. `CreateImportContent` creates only the durable logical
upload attempt. `CreateImportWriteGrant` then creates one durable grant for one
direct object write or one numbered multipart-part generation. Each grant is
scoped to the exact tenant, migration, content, attempt UUID, unique temporary
object/upload identity, maximum bytes, and checksum header where supported. It
cannot write any committed or sealed key.

An eligible future adapter may use a Drive9 upload proxy, but the proxy alone
is insufficient. A proxy grant has separate `accept_before`, `body_complete_before`,
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
fail closed during planning, and the current synchronous proxy path does not
repair the missing provider-side landing bound. A backend that can
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
CREATED --content--> STAGING --verify--> VERIFIED --> COMMITTING --> COMMITTED
   \------ metadata-only verify -------->/

CREATED | STAGING | VERIFIED --abort/deadline--> ABORTING --> ABORTED
```

The direct `CREATED -> VERIFIED` edge is used only when verification proves the
persisted manifest requires no regular-file content, such as an empty,
empty-directory-only, or symlink-only tree.

The transitions and their durable effects are normative:

| From | Trigger | To | Durable result |
| --- | --- | --- | --- |
| none | `CreateImport` | `CREATED` | Identity, target precondition, complete hidden manifest-entry set, reservation, namespace-CAS epoch, immutable activity deadline, owner epoch `1`, owner/recovery-token hashes, and lease are stored atomically. |
| `CREATED` | first accepted content mutation | `STAGING` | Partial hidden content stage and the same manifest/reservation remain owned by the import. |
| `CREATED` or `STAGING` | successful `VerifyImport` | `VERIFIED` | The complete persisted entry set is validated; every required file content is bound; entry set, content set, and manifest version become immutable. A manifest with no regular-file content may take this edge directly from `CREATED`. |
| `VERIFIED` | accepted `CommitImport` CAS | `COMMITTING` | Commit attempt ID and accepted outcome are durable; request cancellation can no longer abort it. |
| `COMMITTING` | server commit worker | `COMMITTED` | Target CAS, namespace publication, ownership transfer, reservation settlement, result row, and structural outbox event commit atomically. |
| `CREATED`, `STAGING`, or `VERIFIED` | explicit abort, expired-lease GC, or activity-deadline CAS | `ABORTING` | No new inline content, entry, verify, renew, takeover, or commit is accepted. For a future external mode, this also rejects new grants/completion/seal and durably owns every closure bound, object identity, and debt charge. |
| `COMMITTING` | permanent target/auth/manifest failure before publication | `ABORTING` | A durable terminal reason is recorded; no namespace or committed quota changed. Transient failure stays `COMMITTING` and is retried by the server. |
| `ABORTING` | server cleanup worker | `ABORTED` | Hidden entries and inline contents are deleted, the reservation is released exactly once, and the terminal reason/result is retained. A future external mode additionally detaches sealed unowned objects and retains upload/seal attempts, provider-mutation claims, and cleanup debt through `gc_done`. |

There are no other transitions. `COMMITTED` and `ABORTED` are terminal.
`COMMITTING` and `ABORTING` are server-owned recovery states: client lease
expiry does not abandon them, and their workers are durably discoverable after
a server crash. `COMMITTING` never transitions directly back to `VERIFIED`, and
`ABORTING` cannot be revived.

`CreateImport` assigns an immutable `activity_deadline` no later than the
server's maximum import lifetime. Owner leases and every inline-content write
must finish before it. For a future external mode, every write grant's
`accept_before`, `body_complete_before`, and `landing_not_after`, and every
provider mutation's `mutation_not_after` must be at or before that deadline;
renew, takeover, or grant retry may shorten their normal duration but never
extend the deadline. `CommitImport` can be accepted only before it. Every owner
mutation performs that comparison with database time inside the same
state/version transaction; application clocks do not decide the winner. At the
deadline, a server scanner (and every racing API call) CASes `CREATED`,
`STAGING`, or `VERIFIED` to `ABORTING` with reason
`activity_deadline_exceeded`; no new owner action is accepted. The winner is
the first state CAS serialized by the database, not whichever caller returns
first: if the deadline CAS wins, a racing commit cannot be accepted; if the
`VERIFIED -> COMMITTING` CAS wins while database time is still before the
deadline, the deadline scanner cannot rewrite its outcome. That commit remains
server-owned and runs to `COMMITTED` or to `ABORTING` with the actual permanent
commit failure reason. It is never relabeled `activity_deadline_exceeded`
merely because wall-clock time passes while it is `COMMITTING`. Thus active
work has a finite client-controlled horizon, but `GetImport` remains available
and its ID/status
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
recoverable with the already-durable token; `GetImport` requires the applicable
allocation or recovery token and may disclose the epoch and state, but never
either token.

For `CREATED`, `STAGING`, or `VERIFIED`, the current owner may renew before
expiry. After expiry, `TakeOverImport` requires the stable recovery token and
atomically compares the old epoch and state, increments the epoch, installs the
hash of a newly generated and fsynced owner token, and grants a new lease.
Tenant authentication or knowledge of a migration ID alone cannot take over an
import. Takeover is rejected while the old lease is live and in every
server-owned or terminal state. Any request carrying the old epoch or owner
token is then fenced, including an old renew, content-create,
multipart-complete, seal, entry, verify, commit, or abort control-plane
mutation. In a future external mode, already-issued write grants are not
treated as revoked; their isolation and cleanup follow the
write-grant/landing-bound contract above. The
single-writer local-root lock prevents routine concurrent takeover; the server
epoch is the authoritative fence for a paused or zombie coordinator.

`VERIFIED` freezes the entry set, every entry-to-content binding, and the
manifest version. No late inline write or future `AttachImportContent` can
mutate it. Commit acceptance and abort/GC acquisition race through the state
CAS, so only one wins. The staging uploader uses an
import-scoped internal capability and never writes through the ordinary user
namespace or FUSE writeback path.

`CommitImport` first durably accepts work by CASing `VERIFIED -> COMMITTING`
and recording a commit attempt. The handler may drive the worker synchronously,
but request lifetime is not ownership of the accepted work. The worker's final
database transaction is the namespace linearization point. In that transaction
it:

1. locks the tenant namespace-capability row and requires
   `namespace_cas_ready=true` and `namespace_cas_epoch` equal to the epoch
   captured by `CreateImport`;
2. revalidates tenant ownership, immutable target binding, server policy, and
   quota, but does not reinterpret the initiating caller's mutable scope after
   commit acceptance;
3. revalidates the target precondition and rename type rules;
4. verifies the complete staged manifest;
5. transfers every staged content reference to its committed inode owner;
6. converts the quota reservation to committed usage without double charge;
7. materializes the complete remote tree;
8. writes the committed result and one structural-reset outbox event containing
   target parent, migration ID, and tree generation.

The readiness/epoch check is not satisfied merely because `CommitImport`
entered `COMMITTING`. If it fails at the final linearization point, that same
transaction publishes nothing, transfers no content/quota, emits no outbox
event, and moves the import to `ABORTING` with the durable
`target_precondition_changed` reason. The source remains authoritative. A
rollback/epoch-change transaction and final publish therefore serialize on the
same tenant row: publish may linearize first under the old valid epoch, or the
epoch change may win and force the import to abort, but an old worker cannot
publish afterward.

P0 caps manifest entries and total metadata bytes below the database
transaction and statement limits. A larger tree fails before staging; it is not
split across multiple visible commits. Raising the cap later requires a
different atomic namespace indirection, not merely a larger transaction.

### Server data model sketch

The exact SQL names may follow the existing schema conventions, but these
durable facts are required:

```text
promotion_storage_capabilities(
  tenant_id, generation, inline_enabled, inline_threshold,
  allowed_mode, updated_at,
  primary key (tenant_id)
)

promotion_namespace_capabilities(
  tenant_id, namespace_cas_ready, namespace_cas_epoch,
  minimum_writer_protocol, updated_at,
  primary key (tenant_id)
)

import_id_claims(
  tenant_id, allocation_sequence, migration_id, target_path,
  expected_target_absent, allocation_idempotency_key_hash,
  allocation_request_digest, allocation_proof_blob, allocation_proof_digest,
  create_before, claim_state,
  accepted_create_request_digest, retired_at, created_at, updated_at,
  primary key (tenant_id, allocation_sequence),
  unique (tenant_id, migration_id),
  unique (tenant_id, allocation_idempotency_key_hash)
)

promotion_import_identity_counters(
  tenant_id, last_issued_sequence, retired_through,
  live_claim_count, materialized_identity_count,
  rate_bucket_state, config_generation, updated_at,
  primary key (tenant_id)
)

promotion_import_identity_global(
  capacity_key, materialized_identity_count, config_generation, updated_at,
  primary key (capacity_key)
)

imports(
  tenant_id, migration_id, allocation_sequence, allocation_proof_digest,
  target_path, target_parent_inode,
  target_parent_path, target_parent_edge_incarnation,
  target_parent_children_generation, manifest_hash, entry_total, byte_total,
  max_content_size, storage_mode, storage_plan_digest,
  backend_capability_generation, inline_threshold, plan_expires_at,
  create_request_digest,
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
  entry_type, mode, mtime_ns, symlink_target,
  expected_size_bytes, expected_checksum_sha256,
  metadata_blob, entry_hash, import_content_id,
  primary key (tenant_id, migration_id, relative_path_hash)
)

import_contents(
  tenant_id, migration_id, import_content_id,
  inline_content_blob, inline_content_idempotency_key, inline_request_digest,
  sealed_storage_ref,
  sealed_storage_version, size_bytes, checksum_sha256,
  accepted_seal_attempt_id, seal_state, ownership_state,
  primary key (tenant_id, migration_id, import_content_id),
  unique (tenant_id, migration_id, inline_content_idempotency_key)
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
  tenant_id, migration_id, allocation_sequence, allocation_proof_digest,
  target_path, request_digest, owner_token_hash,
  recovery_token_hash, terminal_state,
  terminal_result_blob, terminal_result_digest, retire_after,
  primary key (tenant_id, migration_id)
)

retired_import_sequences(
  tenant_id, allocation_sequence, retired_at,
  primary key (tenant_id, allocation_sequence)
)
```

`inline_content_blob` and its idempotency fields are the only P0 content-write
path. The upload/write-grant/seal tables describe the disabled future
external-object extension and need not ship until a concrete production
adapter satisfies the matrix above. Keeping that future schema in the design
does not authorize an implementation to classify `AWSS3Client` as eligible.

All `import_entries` rows are inserted from the canonical manifest in the same
transaction as `imports` and the reservation. `import_content_id` starts null
for regular files and is filled only by the matching idempotent inline-content
transaction; it remains null for directories and symlinks. The expected size,
checksum, type, and metadata columns are immutable after `CreateImport`.

All tables are tenant-scoped. A unique relative-path constraint rejects
duplicate manifest entries after canonical path normalization. Terminal import
rows retain enough target/result identity to answer retries after staging rows
and unowned objects are garbage collected. Commit changes content ownership and
namespace visibility in the same transaction; storage deletion is always an
outbox/GC consequence and never precedes the durable ownership decision.

`import_id_claims` is also the serialization row for first create versus
allocation retirement. Its state is `ALLOCATED`, `ACCEPTED`, or `RETIRED` and
never moves backward. An accepted claim remains linked to the import/tombstone;
the expiry scanner retires an unused `ALLOCATED` claim under the same row lock
used by first Create, so an in-flight create or retirement is the sole winner.
After `create_before` plus the maximum request/recovery window, an unused
retired claim enters the same sequence-retirement transaction as a terminal
import. A missing claim with a valid proof but no retired-sequence evidence is
corrupt/outcome-unknown, not permission to reconstruct or admit the operation.

`live_claim_count` counts only unused `ALLOCATED` claims. The per-tenant and
global `materialized_identity_count` values count allocation sequences, not
physical rows: a sequence is charged once at allocation and remains charged as
its representation moves through claim, import, tombstone, and sparse-retired
states. An accepted claim may coexist with its import/tombstone as a fixed
serialization row without a second identity charge. The schema and retention
rules bound the physical rows per charged sequence by a fixed constant; manifest
entries and content use their separate entry/byte quota. Claim acceptance,
terminal compaction, sparse retirement, watermark advancement, and counter
transfer/decrement each occur in the same transaction as the row transition.

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
also compacted. The compact tombstone therefore stores the canonical target
and complete immutable terminal result needed to reauthorize and answer
`GetImport` or an idempotent acknowledgement retry, not merely a digest.

Only terminalization assigns `retire_after = terminal_at +` the configured
maximum offline-recovery window. The tenant-bound migration ID, canonical
target, request digest, recovery-token hash, terminal state, result, and result
digest remain in `import_tombstones` until that time. During the interval,
`CreateImport` conflicts and an authorized `GetImport` returns the same terminal
result. At `retire_after`, the server atomically marks the allocation sequence
retired before deleting the result tombstone and its linked accepted claim. An
out-of-order retirement inserts one sparse `retired_import_sequences` row in
that same transaction. Under the tenant identity-counter lock, a compactor
advances `retired_through` only across a contiguous prefix of retired sequences
and deletes the consumed sparse markers. The materialized identity counters are
decremented in the same transactions. The configured sequence-window and row
budgets bound the sparse set even if an older operation temporarily prevents
watermark advancement. A live allocation or accepted import is never inferred
retired and therefore blocks the watermark at its sequence; exhausting the
window blocks only new allocations and does not alter the live operation.

The authenticated allocation proof supplies the retired operation's tenant,
target, migration ID, and sequence after per-ID rows are gone. With current
scope on that target and a valid proof, the proof-bearing `CreateImport`,
`GetImport`, and `RetireImportAllocation` endpoints return the stable
`import_id_retired` result when the sequence is at or below `retired_through` or
has a sparse retired marker. Without both proofs they return the same canonical
not-found response as an unknown ID. Other mutation endpoints do not carry an
allocation proof and return canonical not-found once their active/tombstone row
has retired; an old owner or recovery token alone cannot reveal or resurrect
the identity. A valid proof above the watermark with no active/tombstone/claim/
sparse row is a storage-invariant violation and returns fail-closed
`promotion_recovery_required`; it is never treated as new. Recovery beyond the
documented maximum offline window therefore remains deterministic through
`GetImport` without retaining one permanent database row per allocation.

For a future external mode, result-tombstone compaction at `retire_after` also
creates or retains a separate minimal internal migration/tenant cleanup anchor.
That cleanup anchor cannot be deleted until every write grant, upload attempt,
and unowned seal candidate is `gc_done`, every landing/provider-mutation closure
bound has passed, and every corresponding debt charge is released. A provider
outage may therefore retain the cleanup anchor and write/attempt ledgers beyond
`retire_after`, while the sequence proof/watermark independently returns
`import_id_retired` to authorized callers and the terminal result blob is
already pruned. Result-payload retention stays bounded; cleanup debt is never
orphaned merely to satisfy that bound.

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
   after the audit and all-writer gate pass. `CreateImport` records this epoch.
   Both commit acceptance and, normatively, the final namespace-publish
   transaction lock the tenant capability row and require it to remain ready at
   exactly that epoch; an earlier check cannot authorize later publication.

Promotion API routes and the FUSE preview return `promotion_not_enabled` until
that per-tenant gate is ready. A rollback below the minimum writer version must
first disable new imports and advance the CAS epoch under the same tenant-row
lock used by final publish. Every import that has **not** already linearized its
publish then fails closed at its final ready/epoch check. A publish transaction
that acquired the row lock first may legally finish under the old epoch; the
rollback waits for that transaction and observes the already-terminal commit.
After all imports reach a terminal state, the controller revokes promotion and
only then admits old writers. Re-enabling after such a rollback repeats the
barrier, backfill, audit, and epoch change. There is no mixed-version window in
which an old writer and an enabled, nonterminal import may coexist.

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

Fence ownership is structured but does **not** assume disk and memory can be
updated atomically. One mount-local fence manager owns the same registry entry
from initial acquisition through terminal release; a request and coordinator
hold references to that entry rather than transferring ownership by toggling a
boolean. Immediately after atomically acquiring both prefixes, the manager
installs one idempotent resolution guard before invoking preflight. The
initiating rename is owner-exempt while it performs validation.

After preflight and snapshot validation, `PersistAndBind` first marks the
manager entry `PERSISTING`, writes the complete checksummed migration record
with `fence_state=INSTALLING`, and fsyncs the file and directory while the same
in-memory fence remains installed. It then binds the coordinator to that same
registry entry, rewrites/fsyncs `fence_state=INSTALLED`, and signals recovery.
There is no request-guard-to-coordinator fence handoff and therefore no point
at which successful record fsync requires releasing and reacquiring a prefix.

Every exit invokes the manager's idempotent resolver, never a raw fence
release. Before persistence starts, preflight/eligibility error, cancellation,
or recovered panic removes both prefixes and wakes waiters. Once persistence
starts, an error or panic makes the resolver inspect the durable record: a
valid `INSTALLING`/`INSTALLED` record keeps the existing fence and schedules the
supervisor to finish binding; a provably absent record permits release; an
ambiguous fsync/read outcome keeps the fence and marks the mount fail-closed
until the supervisor resolves it. If binding or supervisor startup fails, the
mount stays fail-closed rather than exposing an unfenced durable migration.
Process restart scans and installs fences for both fence states before serving
FUSE. Exactly one registry entry exists per migration, and only terminal
reconciliation releases it.

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
| not found, or an `ALLOCATED` claim with no import | Source is currently authoritative, but `CREATE_REQUESTED` remains outcome-unknown; keep both paths fenced. | A delayed create may still win. Retry the exact `CreateImport` or serialize with it through `RetireImportAllocation`; lookup absence alone causes no cleanup. | Retain; never release a fence or delete the record from this observation. |
| `RETIRED` allocation claim or proof-backed `import_id_retired` | Source is authoritative. | Claim/sequence retirement serialized after every in-flight create loser, so no reservation or stage can appear later. | Fsync a local aborted result, release fences, then remove the active record. |
| `CREATED` | Source is authoritative; keep both fenced during recovery. | Reservation exists; no or partial stage. Resume with the current durable owner token, take over after expiry, or abort. | Retain until `ABORTED` or a later commit result. |
| `STAGING` | Source is authoritative; keep both fenced during recovery. | Reservation and partial hidden inline stage remain. Resume/take over or abort. In a future external mode, old control-plane mutations are epoch-fenced and already-issued grants remain isolated/tracked through landing closure plus GC. | Retain. |
| `VERIFIED` | Source is authoritative; keep both fenced. | Immutable hidden stage and reservation remain. Commit with the same migration ID or abort; no entry mutation is allowed. | Retain. |
| `COMMITTING` | Outcome is unknown; neither source nor target is released. | Server owns completion; lease expiry and client abort do nothing. Poll `GetImport`; a recovered server worker reaches `COMMITTED` or `ABORTING`. | Retain; never create a second migration ID. |
| `ABORTING` | Source remains authoritative, but both paths stay fenced until cleanup completes. | Server resumes hidden-stage deletion and exactly-once reservation release; no client mutation or takeover is accepted. | Retain and poll. |
| `ABORTED` | Source is authoritative; target was not published. | Reservation is released, hidden inline content is gone, and the terminal reason is durable. A future external mode may continue upload/seal cleanup, provider claims, and debt from retained ledgers until `gc_done`. | Fsync the local aborted result, release fences, then acknowledge/retire the active record; the server tombstone remains for retry identity. |
| `COMMITTED` | Target is authoritative; keep both fenced until source reconciliation finishes. | Quota is settled, sealed content ownership and namespace/outbox result are durable; cleanup of temporary and unowned candidates cannot remove committed versions and continues independently until `gc_done`. | Verify the source-incarnation UUID, quarantine only the matching source, publish inode/cache state, fsync `DONE`, release fences, then acknowledge/retire the active record. |
| unreachable or invalid response | Authority is unresolved; keep both paths fenced or fail the mount closed. | No cleanup, quota release, abort, or new migration is guessed. | Retain and return `promotion_recovery_required`. |

The durable local phase refines, but never overrides, that server result:

Before interpreting `phase`, recovery processes `fence_state`. `INSTALLING`
and `INSTALLED` both require both prefixes to be installed before FUSE accepts
requests. `INSTALLING` is completed to `INSTALLED` with the existing fence
entry; it is never treated as permission to release. A corrupt or ambiguous
record fails the mount closed.

| Last durable local phase | Recovery action |
| --- | --- |
| Local record missing | No client operation existed. A server import with no matching record is reclaimed only by its lease/GC state machine. |
| `PREPARED` | Install fences. The fsynced phase proves no create dispatch occurred; retire the allocation claim before recording local abort/releasing fences. If retirement instead reports an accepted import, fail closed and reconcile it. |
| `CREATE_REQUESTED` | Install fences and treat the result as unknown. Retry the exact idempotent `CreateImport`, or call `RetireImportAllocation`; a not-found lookup never releases the fence. Retirement and first create lock the same claim, so the returned accepted-or-retired result is definitive. |
| `STAGING` | Reconcile owner epoch and server state before uploading or aborting. |
| `STAGED_VERIFIED` | Reconcile before commit; do not mutate the frozen manifest. |
| `COMMIT_REQUESTED` | Query `GetImport`; never retry with a new migration ID or infer failure from disconnect. |
| `REMOTE_COMMITTED` | Verify the durable source-incarnation UUID before quarantine. Never roll back the target. |
| `SOURCE_QUARANTINED` | Target is authoritative. A janitor removes only the matching migration-owned quarantine identity. |
| Manifest/record/proof/token missing, corrupt, or mismatched | Fail closed and require repair; do not guess, take over, or delete either tree. |

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
- implement side-effect-free `PlanImport` and a fail-closed production adapter
  classifier; initially only `db9_inline` plans are eligible;
- add namespace path-edge incarnations and child-set generations to every
  structural mutation path before enabling target CAS;
- implement hidden DB-inline staging, idempotent content writes, manifest verification,
  versioned quota reservation, owner epoch/token takeover, the complete import
  state CAS, `CommitImport`, committed outbox/status lookup, and garbage
  collection;
- add server-side failure injection and crash recovery tests.

### P1: restricted preview

- explicit `promote/persist` and/or opt-in FUSE cross-layer rename;
- homogeneous ordinary all-inline trees only; any object-backed file fails
  before `CreateImport` and quota reservation;
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

- the production `Dat9FS.Rename` entry point, with `AllowOther=false` so kernel
  `default_permissions` is absent, rejects a cross-layer candidate when either
  parent lacks write or search permission, when either applicable sticky-dir
  ownership check fails, when the source is missing, when the destination is
  inside the source, when source/target types conflict, or when a target
  directory is nonempty. Each case asserts the ordinary `EACCES`, `EPERM`,
  `ENOENT`, `EINVAL`, `ENOTDIR`/`EISDIR`, or `ENOTEMPTY` result as applicable,
  with the source unchanged and no local migration record, plan, allocation,
  import, reservation, or staging side effect;
- those entry-point tests must traverse the same shared daemon preflight as an
  ordinary rename rather than call a validation helper directly. Moving the
  cross-layer branch ahead of preflight, relying on `default_permissions`, or
  deleting any parent/sticky/type/emptiness check makes a focused test fail;
- after each preflight/eligibility negative case and cancellation, a second
  goroutine performs lookup, write, and rename under both source and target
  prefixes with a bounded deadline. It must observe ordinary namespace
  semantics rather than block, and the fence registry/waiter count must be
  empty;
- deterministic fault injection covers panic/error immediately after fence
  acquisition, after entering `PERSISTING`, before and after the temporary-file
  fsync, after record rename, before and after the parent-directory fsync, after
  a successful directory fsync but before manager binding, after binding but
  before `INSTALLED` fsync, and after that fsync.
  Cases without a valid durable record release both prefixes; cases with a
  valid or outcome-unknown record retain exactly one registry fence and are
  adopted by the supervisor, or the mount rejects all operations fail-closed.
  Deleting manager resolution, releasing one prefix, or treating the fsync-to-
  binding interval as an ordinary request error makes a production-entry test
  expose an unfenced durable record or hang;
- empty and deep directories;
- zero-byte files, files at `inlineThreshold-1`, and complete all-inline trees;
- files at `inlineThreshold` or larger and disabled inline storage require an
  external-object plan; `LocalS3Client` in production, `AWSS3Client`, and an
  unknown adapter all return `promotion_storage_backend_unsupported` before
  import/quota/object side effects for that plan, while an all-inline tree on
  the same tenant remains eligible;
- `PlanImport` is side-effect free, and a backend capability generation or
  inline-threshold change before `CreateImport` invalidates the plan;
- a plan minted on server A is rejected on differently configured server B;
  first-create generation validation and import insertion share one database
  transaction/lock, while retry of an existing matching row is recoverable;
- changing, omitting, reordering, or adding any canonical manifest tuple
  between `PlanImport` and `CreateImport` fails before the import transaction;
- the real API sequence `PlanImport` -> `CreateImport(full manifest)` ->
  `PutInlineImportContent*` -> `VerifyImport` -> `CommitImport` round-trips an
  empty directory, symlink text, mode, mtime, zero-byte file, and nonempty file;
- empty, empty-directory-only, and symlink-only manifests verify directly from
  `CREATED`; a manifest with any unbound regular file fails verification and
  remains in its original state;
- removing `CreateImport`'s manifest persistence loses those metadata-only
  entries and makes the production-path round-trip fail;
- an inline content call for an absent/non-file path or a mismatched entry
  hash, size, or checksum is rejected before reading/storing its body;
- Aliyun and Tencent endpoints are classified through their actual
  `AWSS3Client` construction path and remain unsupported, not inferred from an
  endpoint-name allowlist;
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
- `PREPARED -> CREATE_REQUESTED` record fsync, first Create request dispatch,
  allocation-claim lock, atomic Create commit, and response delivery;
- atomic `CreateImport` manifest-entry/reservation commit and lost response;
- inline-content idempotency-key journal fsync, bounded body/transaction send,
  DB commit, response loss, and returned content-ID journal fsync;
- stage verification;
- durable `COMMIT_REQUESTED` before request send;
- server commit;
- successful commit with lost response;
- remote commit followed by local quarantine fsync failure;
- local source quarantine;
- physical local deletion;
- inode/cache publication.

Before any future external-object adapter is enabled, additionally kill and
restart before and after:

- content-attempt idempotency-key journal fsync, request send, and returned
  attempt-ID journal fsync;
- write-grant idempotency-key journal fsync, grant response, proxy acceptance,
  bounded body completion, provider-write claim, and provider-write outcome;
- durable provider-mutation claim, multipart-complete/copy request send,
  provider-side completion, and returned outcome persistence;
- durable seal intent, create-once sealed-object creation, sealed-version
  verification, and DB attach;

Every case converges to only the complete old tree or only the complete new
tree. Repeated recovery is idempotent.

### Concurrency

- source create, write, truncate, unlink, rename, and open during promotion;
- target creation by another mount during staging;
- target `create -> delete` ABA between precondition issue and commit;
- target parent rename away, rename away-and-back, and delete/recreate;
- final publish versus namespace-CAS disable/epoch advance: a barrier test lets
  publish lock the tenant capability row first and permits the old-epoch commit
  to finish, while the complementary test lets rollback advance the epoch first
  and requires the `COMMITTING` worker to publish nothing and reach `ABORTED`
  with `target_precondition_changed`;
- unrelated target-parent sibling mutation conservatively returns
  `target_precondition_changed`/`EAGAIN` in P0 while the requested child remains
  absent; it never masquerades as `EEXIST`;
- two promotions competing for source or target;
- commit versus abort and commit versus lease GC;
- activity deadline versus renew, takeover, inline-content mutation, verify,
  and commit acceptance;
  client-owned states go `ABORTING`, while a commit accepted before the
  deadline remains queryable and reaches a terminal state;
- coordinator crash followed by expired-lease takeover; every old-epoch renew,
  inline-content, entry, verify, commit, and abort mutation is rejected;
- a delayed first `CreateImport` versus recovery: one strongly consistent
  not-found observation must leave fences intact. A barrier then lets either
  Create accept the allocation claim or `RetireImportAllocation` retire it;
  recovery must respectively resume the one import or prove no import can ever
  appear before releasing fences;

The following cases are not claims about the initial inline-only P0. They are
mandatory gates before enabling any future external-object adapter:

- activity deadline and takeover versus content-attempt creation, write-grant
  issuance/acceptance, body completion, multipart completion, seal, and every
  provider mutation;
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

The remaining P0/FUSE concurrency cases are:

- writeback enqueue immediately after a drain attempt;
- direct local-root file replacement and symlink swap while a snapshot is read;
- a second mount reading while publish occurs;
- a committed outbox invalidating a second mount's positive and negative cache.

### Import and accounting

- allocation flood tests use distinct idempotency keys until the tenant rate,
  live-claim, and sequence-window limits and the global materialized-identity
  limit are reached. The boundary request gets the specified stable error and
  creates no sequence/proof/claim; retrying a prior key still returns its one
  original allocation even after capacity is full. Two tenants contend for a
  deliberately small global limit to prove that the shared counter, not only
  each tenant's limit, rejects the boundary request. A fake clock retires
  claims out of order, proves the sparse set cannot exceed the configured
  window, and leaves the oldest sequence live until the window rejects new
  allocations. Retiring that sequence closes the gap, advances the watermark,
  releases the exact tenant/global counters, and permits allocation again.
  Independently deleting the rate, live-claim, window, global-cap, or atomic
  counter-transfer check makes the production `AllocateImportID` flood test
  exceed its configured rows, double charge/release, or issue one extra proof;
- allocation response loss before the client records the result retries the
  same fsynced idempotency key and obtains the identical sequence, migration
  ID, proof bytes, and deadline without a second counter charge. After the
  documented retention window and watermark compaction, a reused key is
  explicitly a new allocation with a new sequence; neither path can resurrect
  or reuse the retired migration ID;
- authorization is exercised before import creation, not only on
  continuations: out-of-scope `PlanImport` returns no plan, out-of-scope
  `AllocateImportID` creates no claim, and scope revocation between allocation
  and first `CreateImport` creates no import, manifest entries, reservation, or
  staging. Retrying an already-accepted `CreateImport` after revocation is also
  denied before recovery can return the existing row; recovery-before-admission
  never becomes recovery-before-authorization;
- after a successful create, revoke the caller or replace it with a scoped
  credential that cannot access the import's stored target. Even with the
  correct migration/allocation/owner/recovery token, every client continuation
  is reauthorized: inline/external content and grant calls, verify, renew,
  commit acceptance, abort, takeover, retirement, acknowledgement, and status
  read all return the canonical authorization error and make no state, lease,
  capability, body-read, quota, or storage mutation. `GetImport` does not leak
  the import's existence, target, or result to the denied caller;
- the complementary allowed-scope cases succeed, and every continuation
  authorizes the server-stored target rather than a caller-supplied path. An
  exact method/path dispatcher matrix admits scoped credentials only to import
  handlers with that check; unknown actions and method mismatches remain
  denied;
- an ID-only continuation table crosses authorized and out-of-scope callers
  with active rows, full-terminal rows, result tombstones, unused retired
  allocations, sparse retired sequences, watermark-compacted sequences, and
  random unknown IDs. Every stored/proof-target denial or missing-proof case is
  a byte-for-byte equivalent canonical not-found response with no state-
  dependent headers. Current scope plus the applicable authenticated
  allocation proof or recovery token exposes the defined active/terminal
  result; only a proof-bearing endpoint exposes stable `import_id_retired`.
  An unknown ID remains not-found, while a valid proof for an impossible above-
  watermark gap fails closed as recovery required;
- the target-bearing Create-recovery table separately proves its two gates: an
  out-of-scope request target returns the same 403 before lookup for existing
  and unknown IDs, while an in-scope request target that resolves to a stored
  out-of-scope target receives the same 404 as an unknown ID rather than a
  mismatch conflict. Only a caller authorized for the matching stored target
  receives active, terminal, or retired recovery;
- a deterministic revocation barrier after `CommitImport` has already won
  `VERIFIED -> COMMITTING`, and after abort/deadline GC has already won
  `... -> ABORTING`, does not strand server work: the service-owned worker
  reaches its real terminal result while later client requests remain denied;
- the same migration ID with a different target, manifest, entry payload, or
  content hash conflicts;
- `PlanImport(expected_target_absent=false)` returns no plan, and changing a
  valid plan/Create request from `true` to `false` fails MAC/request-digest
  verification before row, reservation, or entry creation; the same mutation
  against an existing import conflicts instead of entering recovery;
- a lost `CreateImport` response followed by the same token, plan, and full
  manifest returns the one import, one reservation, and identical persisted
  entry set; a different manifest cannot reuse either token or reservation;
- fake-clock and configuration-rollout tests move past `create_before` and
  `plan_expires_at` and advance the backend capability generation after the
  first create transaction commits: the identical retry still recovers that
  row, while an absent-row request is rejected by new-admission checks;
- concurrent identical creates serialize on the durable allocation claim and
  return the winner; a mismatched request conflicts, retirement versus create
  has exactly one durable winner, and no path creates a second
  reservation/entry set;
- the import row, quota reservation, and all manifest entries are all present
  or all absent after transaction failure injection; no content endpoint can
  observe an import without its complete expected entry set;
- the same inline-content idempotency key with an identical request returns the
  same content ID after response loss; a different body, hash, size, or path
  conflicts;
- missing, extra, duplicate, absolute, escaping, or symlink-following manifest
  entries are rejected;
- verify rejects a regular file with no content or multiple/mismatched content,
  and rejects any directory/symlink with a content binding;
- an inline content write cannot exceed its planned size, the captured inline
  threshold, or the reserved entry/byte total;
- deleting or weakening the real-adapter classifier makes tests admit
  `AWSS3Client` before `CreateImport`, and therefore fails; a fake provider is
  insufficient evidence for production eligibility;
- commit converts the reservation exactly once after response loss and retry;
- abort and GC release it exactly once and cannot race a committed import;
- committed inline content survives staging GC;
- a structural-reset outbox entry exists exactly once for each commit, while
  no staging entry is indexed or emitted;
- response loss and server restart are injected in every state:
  `CREATED`, `STAGING`, `VERIFIED`, `COMMITTING`, `ABORTING`, `ABORTED`, and
  `COMMITTED`; each case asserts source/target authority, fence retention,
  staging ownership, reservation/committed quota, and local-record retention;
- deleting the owner-epoch comparison or allowing takeover of a live,
  server-owned, or terminal state makes a focused test fail;
- lease renewal and inline-content mutation at the activity boundary never
  extend beyond it; a deterministic barrier test lets the deadline CAS win
  from each of `CREATED`, `STAGING`, and `VERIFIED`, then requires
  `ABORTING -> ABORTED` with the stable
  `promotion_deadline_exceeded`/`ETIMEDOUT` result across restart and retry;
- a complementary barrier test lets `CommitImport` win the
  `VERIFIED -> COMMITTING` CAS before the deadline, then advances database time
  past the deadline. The deadline scanner must leave `COMMITTING` untouched,
  and recovery must expose the actual `COMMITTED` result or the actual durable
  commit-failure reason, never synthesize `promotion_deadline_exceeded`;
- moving either deadline comparison outside the state-CAS transaction, or
  allowing both race participants to claim the import, makes one of those two
  winner tests fail.

The following accounting cases are mandatory before enabling a future
external-object adapter:

- the same content-attempt idempotency key with a different request conflicts;
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
- committed content references survive staging GC;
- verify records an immutable storage version, and mutating the upload-attempt
  key afterward cannot change the bytes observed by commit/read;
- removing the seal/version binding or deleting the retained upload-attempt
  ledger makes the late-PUT/late-part tests fail;
- lease renewal, write-grant bounds, and provider-mutation bounds at the
  activity boundary never extend beyond it. The same two winner tests apply:
  a deadline CAS that wins from a client-owned state yields the stable deadline
  result, while a commit CAS accepted before the deadline remains
  server-owned and reports its actual commit outcome after all tracked landing
  bounds close.

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
  but before the final publish transaction makes the accepted `COMMITTING`
  worker fail closed; deleting only the final-transaction recheck (while
  retaining the earlier check) makes the rollback-winner barrier test fail;
- rollback below the minimum writer version is rejected while imports are
  active or promotion remains enabled, and re-enable requires a fresh audit;
- deleting the writer-version/admission check makes the old-writer fixture
  reproduce target ABA and fail the test.

### Local recovery and reuse

- recovery reinstalls fences before the first lookup;
- a crash in `PREPARED` proves the request was never dispatched and retires the
  allocation claim before releasing fences; a crash in `CREATE_REQUESTED`
  treats Create as outcome-unknown, and deleting that phase fsync or treating a
  single not-found lookup as terminal makes the delayed-create barrier fail;
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
  compaction, complete-result tombstone lookup, and post-terminal sequence
  retirement are tested with a fake clock. Out-of-order retirement creates a
  sparse marker; closing the oldest gap advances the contiguous watermark and
  deletes consumed markers/counter debt atomically. An active sequence never
  becomes retired, and a valid authorized proof after terminal retirement is
  `import_id_retired`, never a fresh import. Missing/invalid proof,
  out-of-scope proof, and an unknown ID are indistinguishable not-found; a
  valid proof for an impossible above-watermark gap is recovery-required;
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

- the production `Dat9FS.Rename` cross-layer call to the shared daemon-side
  POSIX preflight, or any required parent write/search, sticky-directory,
  source/target type, self-subtree, or target-emptiness check;
- the fence manager's idempotent two-prefix resolution on any pre-record error,
  cancellation, or panic path, continuous registry ownership across every
  persist/bind boundary, or restart adoption of `INSTALLING`/`INSTALLED`;
- the subtree fence;
- migration idempotency;
- the parent path-edge incarnation, child-set generation, or absent-child term
  of the target CAS;
- the server-state CAS between commit, abort, and GC;
- owner epoch/owner-token fencing, recovery-token authorization, and
  expired-lease takeover;
- current-request authorization against the server-stored target on every
  client-facing import entry and continuation (including accepted-row Create
  recovery), scoped-route dispatcher allowlisting, the non-disclosing lookup
  response rule, or the separation that lets already-accepted
  `COMMITTING`/`ABORTING` workers finish under service authority;
- authenticated allocation proof binding, monotonic sequence allocation,
  transactional sparse retirement/watermark advancement before payload or
  claim removal, and every per-tenant/global identity admission counter;
- side-effect-free plan revalidation and the production adapter classifier that
  permits DB-inline content but rejects `AWSS3Client`, `LocalS3Client` in
  production, and unknown backends before import/quota creation;
- the P0 absent-target claim being fixed to `true` in both APIs, authenticated
  by the plan MAC, included in the create-request digest, and revalidated before
  first admission; changing it to `false` must reject before side effects, and
  deleting it from either digest must fail a mutation test;
- `CreateImport` re-sending and atomically persisting the full authenticated
  manifest entry set before any content API can run;
- existing-row recovery taking precedence over plan/allocation expiry and
  capability-generation admission, while mismatched retries still conflict;
- inline-content idempotency, per-file threshold enforcement, manifest/hash
  tuple validation, entry binding, verification, and hidden-content ownership
  transfer;
- direct `CREATED -> VERIFIED` validation for metadata-only manifests;
- the activity-deadline state CAS, including both deterministic winners:
  deadline-before-commit yields the stable deadline result, while
  commit-before-deadline preserves the actual server-owned commit outcome;
- the namespace all-writer rollout/admission gate and the final publish
  transaction's locked ready/epoch revalidation;
- the fsynced `CREATE_REQUESTED` outcome-unknown phase and durable allocation
  claim serialization between delayed first create and retirement;
- quota reservation conversion and content-ref ownership transfer;
- the reserved source-incarnation UUID comparison used by recovery and
  post-commit quarantine;
- the committed outbox/cache reset;
- handle state migration;
- Git workspace routing.

Before an external-object mode can be enabled, its tests must also fail when
independently deleting:

- immutable sealed-version binding, durable seal intent, write-grant and
  upload/seal-attempt retention through landing closure/grace, and
  content-attempt/grant idempotency;
- native-capability admission requiring a finite landing bound plus single-use
  or completely enumerable replay identities;
- provider-mutation claims, their side-effect closure bounds, and the required
  post-bound exact-key/multipart rescan before `gc_done`;
- per-write-grant/part-generation and unowned-sealed-candidate debt charges
  plus their atomic attach/abort transfer;

## Rollout

- Keep current `EXDEV` by default until the restricted preview is explicitly
  enabled.
- Provide path-policy overrides as the immediate workaround.
- Ship the preview behind a capability flag only for tenants whose namespace
  CAS gate is ready and whose complete manifest receives a `db9_inline` plan;
  record promotion outcomes, duration, bytes, entries, storage mode,
  unsupported-provider rejections, conflicts, and recovery actions.
- Do not enable object-backed promotion merely because traffic is proxied.
  Each production adapter needs a separately reviewed capability-matrix row
  backed by its real API contract and classification regression.
- Enable transparent rename by default only after the P2 handle and concurrency
  matrix passes on Linux FUSE and the live local E2E environment.
