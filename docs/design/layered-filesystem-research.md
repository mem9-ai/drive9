# Drive9 Layered Filesystem Research Report

Date: 2026-06-01

This report surveys emerging agent and sandbox filesystem systems, including AgentFS, Cloudflare Sandbox SDK / ArtifactFS, Tilde, E2B, Modal, Daytona, YoloFS, and DeltaFS. It focuses on how they implement layered filesystems and translates the findings into implementation guidance for Drive9.

The main conclusion: Drive9 should not copy AgentFS' "single SQLite file as the authoritative upper filesystem" model directly. Instead, Drive9 should implement layered fs as a native server-side metadata overlay: use the existing `file_nodes` / `inodes` / `contents` / `semantic` model as the base, add generic `fs_layers` / `fs_layer_entries` tables as the writable upper layer, and reuse Drive9's existing db9/S3 content plane, revision model, FUSE shadow/writeback path, and Git workspace overlay experience.

## Executive Summary

- The mainstream community direction has converged on the same model: read-only base layer + writable upper/delta/session layer + whiteout delete markers + explicit diff/commit/rollback.
- AgentFS' value is not only file isolation. It also makes the upper layer queryable, auditable, and portable.
- Cloudflare Sandbox SDK is valuable as an execution-isolation model. Its filesystem persistence relies on backup/R2/squashfs/overlayfs, so it is not a good authoritative Drive9 file model, but it is useful as an execution sandbox reference.
- Cloudflare ArtifactFS is closest to Drive9's existing `git_workspace_*` model: a clean Git tree is the base, dirty/new/delete records are the overlay, reads are merged, and writes either copy up or write to the upper layer.
- Tilde treats a sandbox run as a transaction: success commits, while failure, cancellation, and timeout roll back. This maps very well to Drive9 agent sessions.
- E2B / Modal snapshots are closer to VM/container/image-level checkpoints. They are useful for runtime fork/resume, but not as Drive9's primary model for file-level diff, search, and commit.
- Daytona persistent volumes are closer to shared durable directories than transactional overlays. They are useful for large dataset reuse, but are not safe enough for agent code editing by themselves.
- YoloFS and DeltaFS point to two useful directions: agent-visible staged effects, and freezing the writable layer plus inserting a new layer for high-frequency checkpoints.

## Primary Sources

- [AgentFS website](https://www.agentfs.ai/), [GitHub README](https://github.com/tursodatabase/agentfs), [Copy-on-Write Overlays docs](https://docs.turso.tech/agentfs/guides/overlay), [SQLite schema spec](https://github.com/tursodatabase/agentfs/blob/main/SPEC.md), and [FUSE article](https://turso.tech/blog/agentfs-fuse).
- [Cloudflare Sandbox SDK architecture](https://developers.cloudflare.com/sandbox/concepts/architecture/), [sandbox lifecycle](https://developers.cloudflare.com/sandbox/concepts/sandboxes/), [Files API](https://developers.cloudflare.com/sandbox/api/files/), [Backup/Restore](https://developers.cloudflare.com/sandbox/guides/backup-restore/), and [Bucket mounts](https://developers.cloudflare.com/sandbox/guides/mount-buckets/).
- [Cloudflare ArtifactFS](https://github.com/cloudflare/artifact-fs).
- [Tilde Filesystem Isolation](https://docs.tilde.run/sandboxes/filesystem/).
- [E2B Sandbox Snapshots](https://e2b.dev/docs/sandbox/snapshots).
- [Modal Sandbox Snapshots](https://frontend.modal.com/docs/guide/sandbox-snapshots).
- [Daytona Volumes](https://www.daytona.io/docs/en/volumes/) and [Volume overview](https://www.daytona.io/dotfiles/volumes).
- [YoloFS arXiv](https://arxiv.org/abs/2604.13536) and [Microsoft Research summary](https://www.microsoft.com/en-us/research/publication/dont-let-ai-agents-yolo-your-files-shifting-information-and-control-to-filesystems-for-agent-safety-and-autonomy/).
- [DeltaBox / DeltaFS arXiv](https://arxiv.org/abs/2605.22781).

## Solution Comparison

| System | Base layer | Writable layer | Snapshot / fork | External interface | Drive9 takeaway |
| --- | --- | --- | --- | --- | --- |
| AgentFS | Original directory or AgentFS SQLite filesystem | SQLite delta layer | Snapshot/fork by copying the SQLite file | Linux FUSE, macOS NFS, SDK, browser OPFS | The upper layer must be queryable, auditable, and portable. Copy-up + whiteout is enough for an MVP. |
| Cloudflare Sandbox SDK | In-container ephemeral filesystem, optionally restored from backup | Container writable layer; the upper layer after restore does not mutate the backup | Backup stored in R2 as squashfs + metadata | TypeScript SDK, Files API, inotify watch | Execution sandbox state and durable file state should be separate. Default container state must not be authoritative data. |
| Cloudflare ArtifactFS | Git tree snapshot / generation | SQLite overlay metadata + upper content directory | Re-index base after HEAD changes and reconcile overlay | FUSE | Git clean tree + dirty overlay is the same family as Drive9 Git workspace and can inform a generic resolver. |
| Tilde | Versioned repository state | Transactional session | Commit on exit 0; rollback on failure/cancel/timeout; optional human approval | FUSE mount inside sandbox | Treat agent runs as transactions. Commit must be all-or-nothing. Permission failures should happen at syscall/open/create time. |
| E2B | Template / running sandbox state | Runtime modifications inside sandbox | Snapshot captures filesystem + memory and can spawn many sandboxes | SDK | Good for runtime forks, not for Drive9's primary file-level diff/search/commit model. |
| Modal | Base image | Sandbox filesystem changes | Filesystem snapshot is a diff from the base image; directory snapshots can be mounted | SDK | Good for cold-start and environment reuse. File-level queryability is weaker than a metadata overlay. |
| Daytona | Persistent volume mounted into sandbox | Same shared volume | Volume lifecycle is independent from sandbox; many sandboxes can mount it | FUSE volume + SDK | Useful for large datasets/model reuse. Not a transaction layer, so concurrent writes to the same path need application coordination. |
| YoloFS | User/project filesystem | Staged mutation layer | Snapshots help agents self-correct; progressive permissions control risky paths | Agent-native FS research | The filesystem should expose staged effects to agents/users, not only protect after the fact. |
| DeltaFS | Layered sandbox file state | Freezable writable layer | Checkpoint freezes current upper and inserts a new upper; rollback is a layer switch | OS-level sandbox research | High-frequency checkpoint/rollback should use layer switching, not full copies. |

## Common Layered FS Mechanics

### 1. Merged View

The agent sees a merged view:

```text
visible tree
  upper/delta/session layer
  base layer
```

Typical rules:

- `stat(path)`: check upper first; if it is a whiteout, return not found; otherwise fall back to base.
- `readdir(dir)`: merge base children and upper children; upper entries with the same name override base entries; whiteouts hide base children.
- `read(path)`: read upper content if present; otherwise read base.
- `write(path)`: write new files directly to upper; on the first write to a base file, copy up and then modify upper.
- `delete(path)`: if the file comes from base, write a whiteout; if it is upper-only, delete the upper entry directly.
- `rename(old, new)`: MVPs usually represent this as old whiteout + new upsert; directory rename needs careful handling.

### 2. Whiteout

Whiteout is the key primitive in layered fs. It turns delete into an explicit upper-layer record instead of a mutation to base:

```text
base:  /src/a.go exists
upper: /src/a.go op=whiteout
view:  /src/a.go not found
```

This makes rollback cheap: discard the upper layer.

### 3. Copy-up

When an agent modifies a base file, systems generally copy base content into upper first, then write to upper. AgentFS documents this model directly; ArtifactFS has a similar `ensureOverlay` behavior: hydrate the base blob if it is not localized yet, then write upper.

For Drive9, the MVP can start with full-file copy-up because the existing FUSE shadow/writeback path already tends to turn writes into complete object commits eventually. Large files can later use chunk/extent overlays.

### 4. Snapshot / Fork

There are three mainstream approaches:

- AgentFS: copy the SQLite session DB, or use WAL/database-layer capabilities for time travel and fork.
- DeltaFS: freeze the current writable layer, insert a new writable layer, and roll back by switching layers.
- Modal/E2B: save the whole sandbox/image/memory snapshot.

Drive9 fits the second metadata-layer model best: freeze the current layer, create a child layer that points to its parent, and control layer depth later through compaction.

### 5. Audit / Diff

Mature agent filesystems treat "what changed" as a first-class capability. AgentFS emphasizes a queryable SQLite upper layer; Tilde records structured metadata for every sandbox commit; YoloFS focuses on staged effects being visible to agents and users.

Drive9 should design diff/audit into V1 instead of trying to infer changes from object mutations after the fact.

## Detailed System Notes

### AgentFS

AgentFS is positioned around the idea that agents can use real CLI tools without directly damaging user files. It stores agent filesystem state in SQLite and exposes it as a filesystem through FUSE/NFS/SDK/OPFS.

Layered implementation points:

- The base layer is the original directory and is read-only.
- The delta layer is an AgentFS SQLite DB.
- Reads check delta first, then base.
- Writes to base files copy up into delta.
- Deletes of base files write whiteouts into delta.
- Delta stores structured tables for files, directories, blocks, tool calls, KV, and more.
- The session DB can be copied, moved, and queried, naturally becoming the snapshot/audit unit.

Strengths:

- Strong portability: one SQLite file is one agent session.
- Audit-friendly: file changes and tool calls live in a structured DB.
- Tool-compatible: after FUSE/NFS mount, `git`, `grep`, `cat`, and similar tools work directly.

Limitations:

- A SQLite upper layer is better for a single-machine, single-session portable model.
- Server-side multitenancy, S3 large objects, semantic search, quota, and GC all need extra integration.
- Full-file copy-up needs later optimization for very large files.

Drive9 recommendations:

- Adopt AgentFS' copy-up / whiteout / diff / audit semantics.
- Do not use SQLite as Drive9's authoritative layer. Drive9's authoritative upper layer should live in TiDB/db9 metadata tables.
- Later, provide portability features such as exporting a layer as a SQLite/zip bundle.

### Cloudflare Sandbox SDK

Cloudflare Sandbox SDK is a secure execution environment built as Worker -> Durable Object -> Container. It provides file read/write, command execution, watch, bucket mounts, backup/restore, and related capabilities.

Layered implementation points:

- Each sandbox has an independent filesystem, but by default it only persists while the container is active. State can be lost after idle/restart.
- Backup/restore stores `data.sqsh` and metadata in R2.
- Production restore mounts the backup as a read-only lower layer through FUSE overlayfs; new writes go into a writable upper layer and do not mutate the original backup.
- Local development restore is not a COW overlay. It uses `unsquashfs` to unpack and replace the directory.
- R2/S3 buckets can be mounted at paths with prefix and read-only support. The docs explicitly note that bucket mounts are slower than the local filesystem.

Drive9 recommendations:

- The execution sandbox lifecycle should not determine the Drive9 file lifecycle.
- Drive9 layers should be durable storage/session objects decoupled from Cloudflare/E2B/Modal-style sandboxes.
- If Drive9 integrates with Cloudflare Sandbox later, mount/restore Drive9 layers into the sandbox instead of treating the sandbox filesystem as Drive9's authoritative storage.

### Cloudflare ArtifactFS

ArtifactFS is a Git-backed FUSE filesystem whose goal is to make large repositories visible quickly and hydrate blobs on demand. It is very close to Drive9's current Git fast workspace.

Layered implementation points:

- The base is a Git commit tree snapshot, not a full checkout.
- FUSE exposes the full directory tree immediately.
- Blob content is read from Git objects on demand and cached.
- The writable overlay stores dirty/new/delete entries.
- The resolver merges snapshot + overlay.
- Deletes use whiteouts.
- E2E tests cover FUSE, Git operations, commit, and overlay reconciliation.

Drive9 already has a similar structure:

- `git_workspaces` stores workspace metadata.
- `git_workspace_tree_nodes` stores the clean tree manifest.
- `git_workspace_overlay` stores dirty/new/delete/chmod/symlink entries.
- `pkg/fuse/git_workspace.go` implements merged view, hydrate, and overlay read/write in FUSE.

Drive9 recommendations:

- The generic layered FS should reuse the same ideas: base identity + overlay op + path hash + merged resolver.
- Git workspace should keep its specialized base tree. Do not write clean Git blobs into generic `file_nodes`.
- Extract a common overlay resolver, but do not rush to force Git workspace tables into the generic tables.

### Tilde

Tilde's model is: each sandbox gets a versioned FUSE mount that corresponds to a transactional session. Writes inside the sandbox mount are staged; exit success commits; failure, cancellation, or timeout rolls back. Same-path conflicts make commit fail.

Drive9 recommendations:

- Give layers a clear state machine: `active`, `sealed`, `awaiting_approval`, `committed`, `abandoned`.
- `commit` must be all-or-nothing to avoid partial commits.
- Permission errors should happen at `open/create/unlink` time whenever possible, not only at commit time.
- If the base revision changed for the same path, commit should fail and later offer rebase/resolve.

### E2B and Modal

E2B snapshots capture a running sandbox's filesystem + memory and can create many new sandboxes. Modal has filesystem snapshots, directory snapshots, and memory snapshots; filesystem snapshots are diffs relative to the base image and can be stored long-term.

They are useful for:

- Caching heavy environments to reduce cold start.
- Forking runtime state.
- Copying agent execution state.

They are not good as Drive9's primary file model:

- File-level diff/search/commit/rollback is not transparent enough.
- Semantic retrieval, audit, quota, and per-path conflict handling need to be owned by Drive9 metadata.

Drive9 recommendations:

- Treat E2B/Modal snapshots as execution acceleration.
- Use the Drive9 layer ID as the durable file-state handle for external sandboxes.

### Daytona Volumes

Daytona volumes are FUSE-backed persistent volumes stored in an S3-compatible object store. Multiple sandboxes can mount the same volume, with subpaths used for isolation.

Implications for Drive9:

- Persistent volumes are useful for large models, large datasets, and shared artifacts.
- They are not transactional overlays. The docs state that shared FUSE volumes are not transactional and concurrent writes to the same path are last-write-wins.
- Drive9 agent editing needs review/rollback/conflict handling, so a shared volume alone is insufficient.

### YoloFS and DeltaFS

YoloFS argues that agent-native filesystems should move information and control into the filesystem: staging isolates all mutations, snapshots let agents self-correct, and progressive permissions reduce unnecessary prompts.

DeltaFS focuses on high-frequency checkpoint/rollback: file state is layered; checkpoint freezes the current writable layer and inserts a new writable layer; rollback becomes a layer switch.

Drive9 recommendations:

- `layer diff`, `layer status`, and `layer rollback` should be callable by agents themselves, not only by human UI.
- Snapshot should be a metadata operation, avoiding full copies.
- Layer chains must have a depth limit and compaction.

## Drive9 Current State Mapping

Relevant current code:

- Generic file metadata and content: `pkg/datastore/store.go`, `pkg/datastore/file_tx.go`, `pkg/backend/dat9.go`.
- FUSE local write staging: `pkg/fuse/shadow.go`, `pkg/fuse/writeback.go`, `pkg/fuse/commit_queue.go`.
- Coding-agent local-only overlay: `pkg/fuse/local_overlay.go`, `pkg/fuse/local_policy.go`.
- Git-specific layered workspace: `pkg/tenant/schema/git_workspace.go`, `pkg/datastore/git_workspace.go`, `pkg/fuse/git_workspace.go`, `docs/design/git-fast-clone-workspace.md`.

Drive9 already has:

- Canonical path rules: absolute paths, directories ending with `/`, files not ending with `/`.
- Separation between `file_nodes` / `inodes` / `contents` / `semantic`.
- Revision-based write conflict detection.
- DB-inline / S3 large-object tiering.
- FUSE shadow files, writeback, and pending index.
- Git workspace overlay semantics for `upsert` / `whiteout` / `chmod` / `symlink`.

Gaps:

- Generic Drive9 files do not yet have a server-side generic overlay layer.
- API/FUSE/backend lack a layer-aware merged resolver.
- There is no layer lifecycle, diff, commit, or rollback.
- There is no layer-aware search / semantic indexing.
- There is no generic per-layer audit event stream.

## Recommended Architecture

### Core Model

Add a native Drive9 layer:

```text
Merged view
  upper: fs_layer_entries(layer_id)
  lower: base Drive9 namespace(base_tenant_id, base_root_path)
  content: existing DB-inline / S3 content plane
  search: existing semantic plane + layer scope
```

A layer is a lightweight per-agent/per-task writable overlay, not a tenant fork. Tenant fork can continue to exist as the heavier isolation/branching capability.

### Data Model Draft

The MVP should imitate `git_workspace_overlay` to avoid excessive abstraction up front:

```sql
CREATE TABLE fs_layers (
  layer_id          VARCHAR(64) PRIMARY KEY,
  base_tenant_id    VARCHAR(64) NOT NULL,
  base_root_path    VARCHAR(512) NOT NULL,
  parent_layer_id   VARCHAR(64) NOT NULL DEFAULT '',
  actor_id          VARCHAR(255) NOT NULL DEFAULT '',
  state             VARCHAR(32) NOT NULL DEFAULT 'active',
  commit_policy     VARCHAR(32) NOT NULL DEFAULT 'manual',
  created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  sealed_at         DATETIME(3),
  updated_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);

CREATE TABLE fs_layer_entries (
  layer_id          VARCHAR(64) NOT NULL,
  path              VARCHAR(1024) NOT NULL,
  path_hash         VARCHAR(64) NOT NULL,
  parent_path       VARCHAR(1024) NOT NULL,
  parent_path_hash  VARCHAR(64) NOT NULL,
  name              VARCHAR(255) NOT NULL,
  op                VARCHAR(16) NOT NULL,
  kind              VARCHAR(16) NOT NULL DEFAULT 'file',
  mode              INT NOT NULL DEFAULT 420,
  size_bytes        BIGINT NOT NULL DEFAULT 0,
  base_inode_id     VARCHAR(64) NOT NULL DEFAULT '',
  base_revision     BIGINT NOT NULL DEFAULT 0,
  storage_type      VARCHAR(32) NOT NULL DEFAULT '',
  storage_ref       TEXT NOT NULL,
  storage_ref_hash  VARCHAR(64) NOT NULL DEFAULT '',
  checksum_sha256   VARCHAR(128) NOT NULL DEFAULT '',
  content_blob      LONGBLOB,
  content_type      VARCHAR(255),
  content_text      LONGTEXT,
  metadata_json     JSON,
  created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (layer_id, path_hash)
);

CREATE INDEX idx_fs_layer_parent
  ON fs_layer_entries(layer_id, parent_path_hash);

CREATE INDEX idx_fs_layer_op
  ON fs_layer_entries(layer_id, op);
```

Design notes:

- `path_hash` reuses Drive9/Git workspace's long-path indexing strategy.
- `path` remains the authoritative value; lookup must verify both hash and path.
- `base_inode_id` / `base_revision` are used for commit conflict detection.
- `content_blob` is suitable for small files; large files still use the existing S3 `storage_ref`.
- Later, content fields can move into layer-scoped `inodes` / `contents`, but the MVP should not start with that extra abstraction.

### Merged Resolver

`Stat(path, layer_id)`:

1. Look up the exact overlay entry.
2. If `op=whiteout`, return not found.
3. If `op=upsert|mkdir|symlink|chmod`, return overlay metadata.
4. Otherwise look up base.

`ReadDir(dir, layer_id)`:

1. List base children.
2. List overlay children.
3. Let same-name overlay entries override base.
4. Let whiteouts hide base children.
5. Add overlay-only children to the result.

`Read(path, layer_id)`:

1. If overlay has content, read overlay.
2. If overlay is a whiteout, return not found.
3. Otherwise read base.

`Write(path, layer_id)`:

1. Update the overlay entry directly for overlay-only files.
2. On the first write to a base file, record `base_inode_id` and `base_revision`, copy up the full content into overlay, then write overlay.
3. Large files use the existing upload/S3 path; the overlay entry stores only the storage ref.

`Delete(path, layer_id)`:

1. Overlay-only entries can be deleted or converted to tombstones.
2. Base files write `op=whiteout`.
3. Directory delete in the MVP only supports empty directories; recursive delete can later use batch whiteouts.

`Rename(old, new, layer_id)`:

- MVP supports files and symlinks: old whiteout + new upsert.
- Directory rename first returns a typed error or FUSE `EXDEV`, letting callers fall back to copy/delete.
- Later versions can add atomic recursive rename or `op=rename`.

`Commit(layer_id)`:

1. Seal the layer and reject new writes.
2. Open a DB transaction.
3. Check base revision for every entry.
4. Apply upserts to base `file_nodes` / `inodes` / `contents`.
5. Apply whiteouts as base deletes.
6. Enqueue semantic tasks.
7. Mark the layer committed.
8. Send uncommitted/replaced S3 refs to GC.

`Rollback(layer_id)`:

1. Mark the layer abandoned.
2. Stop reading the layer in merged views.
3. Send overlay content to GC according to retention.

`Snapshot(layer_id)`:

- Seal the current layer or record a snapshot point.
- Create a child layer with `parent_layer_id = current_layer_id`.
- Limit MVP depth to 1-2; add flatten/compaction later.

## API / CLI Recommendations

Server:

- `POST /v1/layers`: create a layer.
- `GET /v1/layers/{id}`: inspect status.
- `GET /v1/layers/{id}/diff`: inspect staged changes.
- `POST /v1/layers/{id}/checkpoints`: create a restore checkpoint.
- `POST /v1/layers/{id}/commit`: all-or-nothing commit.
- `POST /v1/layers/{id}/rollback`: abandon.
- Existing `/v1/fs/{path}` selects a layer through a header such as `Drive9-Layer-ID` to avoid polluting paths.

CLI:

```bash
drive9 fs layer create :/project --name agent-task-123
drive9 mount :/project ./mnt --layer <layer-id>
drive9 fs layer diff <layer-id>
drive9 fs layer commit <layer-id>
drive9 fs layer rollback <layer-id>
```

FUSE:

- Add `LayerID` to `MountOptions`.
- `Stat`, `ReadDir`, `Read`, `Write`, `Mkdir`, `Rename`, `Unlink`, `Symlink`, `Chmod`, and `Flush` all use the layer-aware client.
- Keep the coding-agent local-only overlay for `.git`, `node_modules`, build output, and cache.
- Local-only state must be clearly marked as rebuildable and must not automatically enter the durable layer.

## Search / Semantic Semantics

Layer-aware search rules:

- Base search results must be filtered by overlay whiteouts/replacements.
- New/modified overlay files should be searchable before commit.
- MVP can start with keyword/FTS over overlay `content_text`.
- P1 can add `layer_id` to semantic task/resource identity, or introduce `fs_layer_semantic`.
- After commit, semantic entries should move to the base inode revision or trigger recomputation.

Key constraint: if a layer deletes or replaces `/foo.txt`, the old `/foo.txt` semantic hit from base must not appear in layer search results.

## Permissions and Security

Recommended permission split:

- Read base: requires base read.
- Write layer: requires layer write.
- Commit to base: requires base write/delete for every affected path.
- Rollback: requires layer owner or layer admin.

Following Tilde/YoloFS, permission errors that can fail at `open/create/unlink` should fail there instead of being delayed until commit. This lets agents observe failures immediately and adapt.

## Audit

Add an append-only layer event stream:

```sql
CREATE TABLE fs_layer_events (
  event_id       VARCHAR(64) PRIMARY KEY,
  layer_id       VARCHAR(64) NOT NULL,
  seq            BIGINT NOT NULL,
  actor_id       VARCHAR(255) NOT NULL DEFAULT '',
  tool_call_id   VARCHAR(255) NOT NULL DEFAULT '',
  op             VARCHAR(32) NOT NULL,
  path           VARCHAR(1024) NOT NULL,
  before_json    JSON,
  after_json     JSON,
  created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_layer_seq(layer_id, seq)
);
```

This gives Drive9 the most valuable part of AgentFS: a layer is not just a set of blobs, but an explainable, traceable, auditable agent session. Later this can integrate with the existing journal product.

## Phased Implementation Plan

### Phase 0: Design Convergence

- Write an ADR/spec defining `fs_layers`, `fs_layer_entries`, and `fs_layer_events`.
- Decide whether overlay content is copied fields or references to layer-scoped inode/content.
- Define conflict rules for file replace, file delete, directory delete, and directory rename.
- Define the GC strategy for abandoned layers and uncommitted S3 refs.

### Phase 1: Backend MVP

- Update `pkg/tenant/schema/tidb_auto.go`, `tidb_app.go`, and `db9/schema.go`.
- Add datastore methods:
  - `CreateLayer`
  - `GetLayer`
  - `UpsertLayerEntry`
  - `GetLayerEntry`
  - `ListLayerChildren`
  - `DiffLayer`
  - `CommitLayer`
  - `RollbackLayer`
- Implement a layer-aware resolver around `Dat9Backend`.
- Unit tests cover `Stat`, `ReadDir`, `Read`, `Write`, `Delete`, and commit conflicts.

### Phase 2: API / CLI

- Add layer lifecycle endpoints.
- Add layer headers/options to the client.
- Add the `drive9 fs layer` command group.
- Preserve existing `/v1/fs` behavior when no layer ID is provided.

### Phase 3: FUSE Agent Workflow

- Add `drive9 mount --layer`.
- Route all FUSE path operations through the layer-aware client.
- Drain overlay writes during `FlushAll` / unmount.
- Add FUSE-level diff/rollback tests.

### Phase 4: Search / Audit / Snapshot

- Integrate `fs_layer_events`.
- Filter base hits in layer-aware search.
- Add overlay semantic indexing.
- Add snapshot/child layers with max-depth guard.
- Add compaction/flattening.

### Phase 5: Moderate Unification with Git Workspace

Git workspace has special needs for clean Git trees, blob hydration, and `.git` checkpoints, so it should not be migrated into generic tables too early. After the generic layer stabilizes, Drive9 can:

- Share whiteout/diff/commit code.
- Extract a shared overlay resolver interface.
- Keep the Git base tree independent from generic `file_nodes`.

## Risks and Notes

- Directory operations are complex. The MVP should handle directory rename / recursive delete conservatively.
- If layer-aware search fails to filter base hits correctly, it will give agents incorrect context.
- S3 GC must account for uncommitted layers, committed base refs, tenant forks, and abandoned sessions.
- FUSE kernel cache must be invalidated correctly when an overlay is modified externally.
- Deep layer chains will slow down `stat/readdir/read`, so depth limits are needed early.
- Commit must be all-or-nothing; otherwise review/rollback semantics collapse.
- Local-only overlay and durable layer must be clearly separated in UI/CLI so agents do not assume build output is persisted.

## Final Recommendation

Drive9's layered filesystem should land in this shape:

```text
existing base namespace
  + fs_layer_entries upper layer
  + existing DB-inline/S3 content plane
  + explicit diff/commit/rollback
```

This path absorbs AgentFS' safety and audit model, ArtifactFS' lazy base + overlay resolver, and Tilde's transactional session, while preserving Drive9's existing multitenancy, semantic search, S3/db9 storage, FUSE writeback, and Git workspace foundations.

Minimum viable product:

```bash
drive9 fs layer create :/repo
drive9 mount :/repo ./mnt --layer <id>
# agent normal workflow
drive9 fs layer diff <id>
drive9 fs layer commit <id>   # or rollback
```

This gives Drive9 agent workspaces four core capabilities: safe isolation, reviewability, recoverability, and forkability.
