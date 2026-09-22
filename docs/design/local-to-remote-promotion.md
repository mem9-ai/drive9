# Local-to-Remote Synchronous Promotion

The FUSE/client coordinator and canonical wire manifest live in this
repository. The production server endpoint, transactional publisher, namespace
locking, accounting, and tenant schema are owned by `tidbcloud/fs`; Drive9 must
not carry a second server implementation.

Status: Phase-one contract

Date: 2026-09-21

## Problem

Coding-agent mounts keep build directories such as `dist` in the local
overlay. Publishing a completed build to a normal remote path currently leaks
the storage boundary:

```text
rename("/project/dist/.site-next", "/project/site") -> EXDEV
```

Phase one supports this specific workflow without changing the semantics of
ordinary rename, writeback, or commit queues.

## Phase-one contract

The only new operation is:

```text
closed, homogeneous local-only directory tree
    -> absent remote-persistent target
```

It is synchronous. `rename` returns success only after:

1. the local tree has been frozen and read completely;
2. the server has validated every entry and payload;
3. one tenant-database transaction has published the complete target tree;
4. the initiating mount has durably hidden the local source and updated its
   local view.

Other mounts see either no target or the complete target. They never see a
partially published tree.

## Minimal protocol

Phase one uses one bounded atomic-import request, not a remote staging state
machine.

The client sends:

- a random operation ID;
- the absent target path;
- a canonical manifest digest;
- the complete bounded manifest and DB-inline file payloads.

The server authorizes the target first and namespaces the receipt key by the
pair `(target, operation ID)`. Lookup and acknowledgement carry that same
target, so a scoped caller cannot collide with or probe a receipt under a path
it cannot access. The server then reads and validates the bounded request
before opening the publish transaction. The transaction:

1. returns the stored result for an exact operation-ID retry;
2. locks and verifies the current target parent;
3. verifies the target is absent;
4. creates all directory and file rows;
5. records the committed operation result;
6. commits the namespace and result together.

The three wire operations form one cross-repository contract. `POST` publishes
the namespace and its receipt in the same transaction, completes any pending
accounting for an exact retry, and emits the structural reset event before it
returns success. Side-effect-free `GET` is keyed by `(target, operation ID)`
and returns either the completed committed receipt or `404`; it performs no
accounting and emits no event. After the client has durably persisted
`released`, `DELETE` carries
the exact `(target, operation ID, manifest digest)` tuple; it is idempotent and
an already-absent receipt returns `404`. `409`, `413`, and `507` POST responses
are authoritative pre-commit rejections only when no earlier attempt with the
same operation ID may have reached the server.

If the response is lost, the live syscall queries the same operation ID as
evidence and must obtain a successful exact `POST` retry before completing the
local transition. This preserves `GET` as a pure query while keeping accounting
and the cross-mount structural reset on the successful `POST` boundary. Whether
any request may have reached the server is sticky across those retries: a later
local/proxy rejection plus `NotFound` cannot disprove an earlier in-flight
attempt. A committed matching result plus that successful exact retry means it
may finish the local source transition. Startup recovery never turns
a prepared local record with no durable server result into a new publish; it
fails closed instead. A single receipt `NotFound` is not proof that a publish
request from the crashed process cannot still commit, so recovery also does
not discard that record automatically. Reusing an operation ID for the same
target with a different manifest is rejected; the same caller ID under another
authorized target is a separate operation.

There is no Plan/Allocate/Create/Put/Verify/Commit pipeline, allocation proof,
keyring, owner lease, takeover protocol, external authority, or remote cleanup
worker in phase one. The request is either committed atomically or has no
remote effect.

## Supported entries and bounds

Phase one supports directories and regular files whose payloads use the
existing DB-inline storage path. It preserves ordinary rwx permission bits and
modification time.

The request is rejected before the remote call if any of these is true:

- the target exists;
- the target is directly under the implicit remote root rather than under an
  existing project directory;
- the source or target subtree has an open file or directory handle;
- the tree contains a symlink, hard link, special file, sparse file, xattr,
  ACL, setuid/setgid/sticky permission bit, or an entry owned by another
  uid/gid;
- any final entry name is not already in Drive9's canonical path grammar;
- the source is not wholly local-only, or any entry's mapped final path is not
  remote-persistent (for example `site/node_modules/x.js`);
- an entry, path, depth, file size, total byte size, or request size exceeds
  the configured phase-one bound;
- a local-root single-writer lock cannot be held.

Unsupported cases return `EXDEV` with no remote side effect. A current target
conflict returns the ordinary conflict error.

## Local crash recovery

Before sending the atomic request, the mount fsyncs one small local record
containing the operation ID, source, target, manifest digest, and a one-way
fingerprint of the server and credential identity. Recovery fails closed
before any request or local mutation if that identity no longer matches.

- Known pre-commit failure: durably mark the record aborted, then delete it and
  leave the source unchanged. A failed delete can only be retried as cleanup;
  it cannot publish on restart.
- Unknown response: query the same operation ID. Restart only moves forward
  from a matching committed result and never re-POSTs a prepared record.
- Committed result: move the source into an operation-ID-private quarantine
  directory, fsync both parent directories, persist `released`, and update the
  mount view. `rename` may then return success. Receipt acknowledgement,
  quarantine deletion, and journal deletion are retryable background cleanup;
  they never inspect or remove a user path that later reuses the old source
  name.
- Restart: process any remaining record before serving requests for that local
  root.

If the live mount cannot determine a publish outcome, it refuses all namespace
work, including opens and directory reads, until remount recovery resolves the
journal. Phase one intentionally uses this mount-wide fail-closed behavior
instead of a second path-scoped recovery state machine.

If startup reaches a durable contradiction (for example, an absent receipt for
a prepared or committed journal, a receipt tuple mismatch, or changed local
content after a commit), it exits with the permanent-startup code. The error
names the journal path, phase, and operation ID. Supervisors must not restart
that condition in a loop; it requires the operator procedure below.

The journal contains no distributed lease or restore generation. Any mount
using the local root takes the same cross-process lock, even when creation of
new promotions is gated off. A pending journal is recovered or rejected before
that mount serves; the gate never bypasses the lock or recovery. An enabled
mount also uses one in-process mutation barrier while scanning and committing.

### Operator recovery for a refused journal

Do not delete `record.json` merely because its first result lookup returned
`NotFound`: the previous server request can still be in flight. Instead:

1. Stop every mount using the LocalRoot. Copy the whole
   `LocalRoot/.drive9/promotion` directory and the referenced source/quarantine
   tree before changing either.
2. Record the error's operation ID and phase. Prove from server request and
   transaction logs that no publish for that exact operation remains in
   flight, then query the receipt again and inspect the remote target.
3. If the matching receipt exists, restore normal connectivity/configuration
   and restart so Drive9 can forward-complete it. Do not move or delete the
   source/quarantine manually.
4. Only for a `prepared` record, after proving the request is quiescent, the
   receipt and target are absent, and the source is still present while its
   quarantine is absent, move `record.json` aside and fsync its parent
   directory. Keep the backup until the mount starts and the unchanged local
   source is verified.
5. For `committed`/`released`, a missing receipt can mean the tenant database
   was restored. Do not delete the journal or local tree. Restore/reconcile the
   server result first, or escalate with the journal and local backups.

The conservative procedure is intentional: without a server tombstone or
in-progress marker, the client cannot turn one `NotFound` into proof of
non-commit.

## Explicit non-goals

Phase one does not include:

- target replacement;
- open-FD, directory-handle, or mmap continuity;
- remote-to-local moves;
- mixed-policy or Git workspace trees;
- object-store payloads or unbounded trees;
- returning rename success before remote commit or the durable local source
  switch (post-success receipt/quarantine cleanup is asynchronous);
- online restore/clone concurrency;
- stable-inode migration.

Restore and clone must disable and drain synchronous promotion before changing
the tenant database. Online restore fencing is a separate feature and is not a
dependency of this rename workflow.

## Rollout

The feature is gated and initially disabled. With the gate disabled, existing
namespace behavior is unchanged and cross-layer rename continues to return
`EXDEV`. Enabling the gate requires the atomic-import server endpoint and the
FUSE coordinator from the same release.

Two correctness locks are deliberately not gated per process. Any mount with a
LocalRoot takes `LocalRoot/.drive9/promotion/local-root.lock`, so a gate-off
mount cannot race an enabled mount or bypass a pending journal. Server
namespace writers always take the shared parent-row serialization needed to
race safely with a promotion; per-replica gating would be unsafe during a
rolling deployment. Consequently, default-off deployments still pay that
database lock/transaction cost on affected namespace writes. Throughput must
be re-measured before promotion is enabled by default.

## Acceptance tests

The phase is complete only when tests prove:

- `dist/.site-next -> absent site` publishes the complete tree;
- a second real mount that cached target absence sees the complete tree after
  structural invalidation;
- target conflict and every unsupported source shape have zero remote effect;
- a lost commit response resolves through the same operation ID;
- a crash after remote commit but before local quarantine completes forward;
- disabling the gate preserves the previous namespace byte-for-byte;
- a second process, including one with the gate off, cannot mount the same
  local root;
- a gate-off restart still resolves or rejects an existing durable journal
  before serving.
