# Drive9 Layer FS V1 Design

Date: 2026-06-03

This document defines the Drive9 Layer FS V1 design. V1 follows the foundational direction that the industry is converging on: read-only base, one writable overlay, copy-up, whiteout, and explicit checkpoint/commit/rollback. This document does not adopt DeltaFS/DeltaBox-style multi-segment stack optimizations in V1; DeltaFS ideas remain a future direction for high-frequency checkpoint/rollback.

The goal of V1 is to provide an agent/sandbox-friendly layered filesystem on Drive9 without breaking existing Drive9 capabilities or performance. When no layer is used, existing `/v1/fs`, FUSE, Git workspace, S3 multipart, semantic search, and tenant fork behavior must remain unchanged.

## User Journey

### 1. Start an Agent Work Session

The user creates a layer from an existing Drive9 path:

```bash
drive9 fs layer create :/repo --name fix-auth-bug --tag task=auth --tag env=dev --durability=restore-safe
```

Response:

```text
layer_id: lyr_abc
base: :/repo
status: active
durability: restore-safe
```

Then the user mounts the layer:

```bash
drive9 mount :/repo ./repo --layer lyr_abc --profile=coding-agent
```

Any place that needs a layer can use any of these references:

```bash
drive9 mount :/repo ./repo --layer lyr_abc
drive9 mount :/repo ./repo --layer fix-auth-bug
drive9 mount :/repo ./repo --layer task=auth
drive9 mount :/repo ./repo --layer tag:task=auth
```

Resolution order is `layer_id -> name -> tag`. If `name` or `tag` matches multiple layers, the command must return a conflict and prompt the user to use `layer_id` or a more specific tag. It must not choose randomly.

User mental model:

```text
base :/repo does not change
agent changes go into lyr_abc first
```

### 2. Agent Reads and Writes Normally

The agent or user works normally inside the mount:

```bash
cd ./repo
vim pkg/server/auth.go
go test ./pkg/server
npm install
```

The interaction does not require the user to understand Drive9 internals:

- Durable files such as source code, docs, and config enter the layer.
- `.git`, `node_modules`, and build/cache output prefer the existing local-only overlay.
- File `close`, `fsync`, checkpoint, and unmount advance the durable layer to the backend.
- Regular Drive9 mount behavior remains unchanged when no layer is used.

### 3. Inspect Status and Diff

The user can inspect layer status at any time:

```bash
drive9 fs layer status lyr_abc
drive9 fs layer status fix-auth-bug
drive9 fs layer status task=auth
```

Example output:

```text
Layer lyr_abc active
Base :/repo
Durable seq 42
Pending local writes 0

M pkg/server/auth.go
A docs/auth-flow.md
D pkg/server/legacy_token.go
L node_modules/        local-only, rebuildable
```

Inspect the diff:

```bash
drive9 fs layer diff lyr_abc
```

The diff should look like Git diff, while explicitly showing base revision, layer revision, and local-only items.

### 4. Create a Restore Checkpoint

Before sandbox replacement, at long-task intermediate points, or before risky operations, the user or orchestrator runs:

```bash
drive9 fs layer checkpoint lyr_abc --wait --label before-refactor
drive9 fs layer checkpoint fix-auth-bug --wait --label before-refactor
```

Response:

```text
checkpoint: cp_before_refactor
durable_seq: 57
restore_safe: true
```

Meaning:

- All durable file changes before the checkpoint have been written to the Drive9 backend.
- A new sandbox can restore from this checkpoint.
- Open dirty handles that were not closed, fsynced, or checkpointed are not guaranteed to survive cross-sandbox restore.

### 5. Restore Across Sandboxes

After a new sandbox starts:

```bash
drive9 mount :/repo ./repo --layer lyr_abc --checkpoint cp_before_refactor --profile=coding-agent
drive9 mount :/repo ./repo --layer fix-auth-bug --checkpoint cp_before_refactor --profile=coding-agent
```

The user sees:

- Durable files added, modified, or deleted in the layer are restored.
- Base remains unchanged until commit.
- Local-only directories are regenerated or lazily loaded according to policy.
- Git workspace continues using the existing Git fast workspace restore mechanism.

### 6. Commit or Roll Back When Done

When satisfied, commit:

```bash
drive9 fs layer commit lyr_abc
drive9 fs layer commit fix-auth-bug
```

Commit is all-or-nothing. On success:

```text
base :/repo updated
layer lyr_abc committed
```

When not satisfied, roll back:

```bash
drive9 fs layer rollback lyr_abc
drive9 fs layer rollback task=auth
```

Base remains completely unchanged. The layer moves to `abandoned` and is later GCed according to retention.

If someone else modified base:

```text
conflict: pkg/server/auth.go
base revision changed: 12 -> 15
layer preserved for review
```

The user can create a new layer, merge manually, or roll back. The system must not partially commit.

## Design Principles

V1 uses the basic layered FS model:

```text
visible tree
  writable overlay
  immutable base
```

Key principles:

- Zero behavior change when no layer is used.
- When a layer is used, base is immutable until commit.
- Rollback never mutates base.
- The write hot path prefers local storage and must not turn every `write(2)` into a remote round trip.
- Checkpoint/close/fsync/unmount are restore-safe durability barriers.
- The backend durable layer is the authority for cross-sandbox restore.
- Local-only overlay is a performance layer and rebuildable state; it does not automatically enter the durable layer.
- Git workspace keeps its specialized implementation and is not forced into the generic layer.

## Filesystem Model

The V1 visible tree has three layers:

```text
visible tree
  local runtime overlay      # FUSE hot path, local shadow/writeback/WAL
  durable fs layer           # Drive9 backend authoritative overlay
  base Drive9 namespace      # existing file_nodes / inodes / contents / semantic
```

Read order:

1. Local dirty/pending data.
2. Backend durable layer entry.
3. Base Drive9 file.

Write order:

1. `write(2)` writes to local shadow/WAL.
2. `close`, `fsync`, checkpoint, and unmount push to the backend layer.
3. Only `commit` applies changes to base.

Path classification order:

1. `local-only` policy: `.git`, `node_modules`, and build/cache output continue using the existing local overlay.
2. `git_workspace`: the existing Git fast workspace continues handling clean tree + Git overlay.
3. `fs_layer`: regular Drive9 files under layer mount/API use the generic layer resolver.
4. `remote_persistent`: when there is no layer or the layer misses, use the existing base path.

## Data Model

Add tenant-local tables:

```text
fs_layers
fs_layer_entries
fs_layer_events
fs_layer_checkpoints
fs_layer_tags
```

### fs_layers

`fs_layers` represents one agent session:

```text
layer_id
base_root_path
state = active | sealed | committed | abandoned | conflicted
durability_mode = restore-safe | write-through | local-fast
actor_id
durable_seq
created_at
updated_at
sealed_at
```

`name` is a human-readable reference and is not globally unique. If multiple layers share the same name during resolution, return a conflict.

### fs_layer_tags

`fs_layer_tags` supports business-semantic layer references:

```text
layer_id
tag_key
tag_value
created_at
```

Index requirements:

```text
PRIMARY KEY (layer_id, tag_key)
INDEX (tag_key, tag_value)
INDEX (tag_key)
```

Layer refs in commands and APIs uniformly support:

```text
layer_id
name
tag:key=value
key=value
tag:key
```

`key=value` is resolved as a tag only when there is no same-name layer. `tag:key` means a match where the tag key exists. Every tag/name reference must resolve to exactly one layer; otherwise return a conflict.

### fs_layer_entries

`fs_layer_entries` represents overlay changes:

```text
layer_id
path
path_hash
parent_path
parent_path_hash
name
op = upsert | whiteout | mkdir | symlink | chmod | rename
kind = file | dir | symlink
base_inode_id
base_revision
storage_type
storage_ref
storage_ref_hash
content_blob
content_type
content_text
checksum_sha256
size_bytes
mode
entry_seq
created_at
updated_at
```

V1 storage boundary: small files are stored inline in `content_blob` in the metadata store. Layer file content above the inline threshold uses `storage_ref` pointing to backend object storage and is streamed during restore / commit, avoiding any large-file limit in the entry body itself.

Index requirements:

```text
PRIMARY KEY (layer_id, path_hash, entry_seq)
INDEX (layer_id, parent_path_hash)
INDEX (layer_id, entry_seq)
INDEX (layer_id, op)
```

`path_hash` preserves TiDB/MySQL long-path index performance. `path` remains the authoritative value, and lookup must verify hash + path. `entry_seq` keeps repeated modifications to the same path as an append-only log: user diff/search use the latest-per-path current-state view, while commit/restore use ordered replay logs. This avoids losing data when sequences such as `upsert -> chmod`, `upsert -> rename`, or `mkdir -> chmod` are collapsed.

### fs_layer_events

`fs_layer_events` is an append-only audit log for diff, audit, and agent-action explanation:

```text
event_id
layer_id
seq
actor_id
op
path
before_json
after_json
idempotency_key
created_at
```

### fs_layer_checkpoints

`fs_layer_checkpoints` records durable restore points:

```text
checkpoint_id
layer_id
durable_seq
label
created_at
```

## Resolver Semantics

### stat(path)

- If the layer has `whiteout`, return not found.
- If the layer has `upsert`, `mkdir`, `symlink`, or `chmod`, return layer metadata.
- Otherwise look up base.

### readdir(dir)

- Merge base children + layer children.
- Same-name layer entries override base.
- Whiteouts hide base children.
- Local-only children are visible in FUSE listings but marked rebuildable.

### read(path)

- Local dirty data has priority.
- Layer content comes next.
- Base content is the fallback.
- S3-backed layer content continues using the presign/read-plan approach.

### write(path)

- New files are written directly to the layer.
- When modifying a base file, record `base_inode_id` and `base_revision`, then copy up content to the layer.
- Small files are inline; large files use the existing multipart/S3 path.
- Base remains unchanged.

### delete(path)

- Base files write `whiteout`.
- Layer-only files can delete the layer entry or write a tombstone.
- V1 only guarantees file and empty-directory delete; recursive delete is a later extension.

### rename(old, new)

- V1 layer mount handles regular file rename by copy-up: write an `upsert` entry at the target and a `whiteout` entry at the source path.
- If the source file comes from base, rename first materializes it from base into the target layer entry and local shadow/pending state; restore does not depend on the original base source path still existing.
- API-level `rename` entries in V1 only allow file rename. Commit uses no-replace semantics: if the target already exists, return conflict and do not overwrite the base target.
- Directory rename is a future enhancement. V1 rejects it explicitly in API/FUSE to avoid requiring recursive directory tree snapshots for rollback.

## API / CLI

New APIs:

```text
POST /v1/layers
GET  /v1/layers
GET  /v1/layers/{layer-ref}
GET  /v1/layers/{layer-ref}/diff
GET  /v1/layers/{layer-ref}/diff?replay=1
GET  /v1/layers/{layer-ref}/entries?path=/...
POST /v1/layers/{layer-ref}/entries
POST /v1/layers/{layer-ref}/checkpoints
POST /v1/layers/{layer-ref}/commit
POST /v1/layers/{layer-ref}/rollback
GET  /v1/layer-checkpoints/{checkpoint-id}
```

V1 keeps the existing `/v1/fs` API code path unchanged. Layer mounts write layer entries through explicit `/v1/layers/{layer-ref}/entries`; when there is no `--layer` or layer API call, old clients still use the original `/v1/fs` read/write path and performance path.

```text
drive9 mount :/repo ./repo --layer <layer-ref>
```

CLI:

```bash
drive9 fs layer create :/repo --name task --durability=restore-safe
drive9 fs layer status <layer-ref>
drive9 fs layer diff <layer-ref>
drive9 fs layer checkpoint <layer-ref> --wait
drive9 fs layer commit <layer-ref>
drive9 fs layer rollback <layer-ref>
drive9 mount :/repo ./repo --layer <layer-ref>
```

FUSE mount options:

```text
MountOptions.LayerRef
MountOptions.CheckpointRef
```

`FlushAll` must drain:

- open dirty handles
- layer checkpoint queue
- existing Git checkpoints
- existing writeback uploader
- existing commit queue

## Durability Strategy

Default `restore-safe`:

- `write(2)`: return after local durable WAL/shadow.
- `close`, `fsync`, checkpoint, and unmount: must wait until the backend layer is durable.
- Cross-sandbox restore only guarantees data before a durable checkpoint.
- The sandbox orchestrator must run `checkpoint --wait` before replacement.

V1 accepts and stores `write-through` / `local-fast` for API/CLI compatibility and future scheduling policy extension. The current FUSE write implementation always uses `restore-safe` behavior and does not change write/flush/checkpoint paths based on those two modes. Any scheduler that depends on more aggressive or more conservative durability must still use explicit checkpoints as cross-sandbox restore boundaries in V1.

V1 restore-safe boundary:

- Durable files that have been `close`d, `fsync`ed, checkpointed, or drained at unmount must survive cross-sandbox restore.
- Dirty handles that are still open and uncheckpointed when a process is killed are not guaranteed to survive cross-sandbox restore. V1 `write-through` is only a recorded intent field and does not provide extra guarantees yet.

## Commit / Rollback

Commit flow:

1. Run `checkpoint --wait`.
2. Seal the layer.
3. Read the ordered replay log.
4. Validate each base path's revision.
5. Apply entries to base in `entry_seq` order. If a failure occurs, use a pre-commit snapshot to perform best-effort rollback of already-applied entries.
6. Enqueue semantic tasks.
7. Send old S3 refs to the existing GC path.
8. Use CAS to move the layer from `committing` to `committed`.

Rollback flow:

1. Mark the layer `abandoned`.
2. Stop reading the layer in the visible tree.
3. Clean overlay content according to retention.
4. Leave base completely unchanged.

Conflict handling:

- Validate `base_revision` / `base_inode_id` before commit, and revalidate destructive / metadata-only ops before apply.
- If base revision changed, commit returns a conflict list.
- The layer remains `conflicted` so the user can review, merge manually, or roll back.
- `active|sealed -> committing -> committed|conflicted` uses conditional state transitions. A `committing` layer without commit owner/epoch cannot re-enter commit or rollback, preventing duplicate apply.
- The current implementation is preflight + ordered apply + best-effort rollback. It does not claim single-transaction atomicity across DB/filesystem mutations. Production-grade behavior still needs an applied-entry ledger / commit owner lease to support crash-safe resumption.

## Search / Semantic

Layer-aware search:

- Base hits must be filtered by layer whiteouts/replacements.
- New/modified layer files participate in `find` / `grep`.
- V1 can first support metadata/keyword search.
- Vector/embedding can be introduced in V2 through layer-scoped semantic tasks or `fs_layer_semantic`.

Key constraint:

- If a layer deletes or replaces `/foo.txt`, the old `/foo.txt` semantic hit from base must not appear in layer search results.

## Compatibility

Must preserve:

- Regular `/v1/fs` behavior when no layer is used.
- Existing FUSE writeback/shadow performance.
- Git fast workspace continues using `git_workspace_*`.
- Tenant fork remains the heavy branching capability and is not replaced by layer.
- S3 multipart, read redirect, batch-stat, and batch-read-small add no no-layer overhead.
- Existing scoped FS authorization is not bypassed; layer headers must enter the authorization model.

## Test Plan

Core tests:

- No-layer requests preserve old behavior exactly.
- Layer upsert overrides base read/stat/list.
- Whiteout hides base.
- Rollback restores base visibility.
- Successful commit updates base.
- Commit conflict preserves the layer and does not partially commit.
- New sandbox restore after checkpoint does not lose closed/fsynced files.
- Local-only directories do not enter the durable layer.
- Large layer writes are verified through object-backed layer entries and are not constrained by JSON entry body limits.
- Layer-aware search does not return base hits hidden by whiteout/replacement.

Performance tests:

- No-layer read/write/list benchmarks add no extra DB queries.
- Layer read miss adds only an overlay lookup.
- Layer readdir uses base list + layer children query + in-memory merge.
- After large layer entries use externalized storage, verify streaming commit through the shadow/S3 path.

Failure-recovery tests:

- After local WAL write and process crash, the same sandbox restart can recover pending data.
- After successful checkpoint, a new sandbox can restore.
- Failed checkpoint does not advance durable seq.
- S3 upload failure does not mark an entry durable.

## Explicit Non-goals

V1 does not do:

- DeltaFS multi-segment stack.
- Per-write millisecond-level rollback.
- Process/memory checkpoint.
- Recursive directory rename.
- Full whiteout expansion for recursive delete.
- Migrating Git workspace tables into generic layer tables.
- Automatically persisting local-only build/cache output.

DeltaFS/DeltaBox multi-segment checkpoint/rollback can be a future V2/V3 optimization direction, not a source of V1 complexity.
