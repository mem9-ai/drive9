# Drive9 Git Fast Workspace Design

## Background

Drive9 has two high-priority requirements for agent workspaces:

- `git clone` into a mounted directory must be fast, without checking out the full working tree into Drive9's generic file tables.
- After a sandbox or container is replaced, the agent's partially edited working tree and `.git` state must be recoverable from Drive9 so `git status`, `git add`, `git commit`, and `git push` can continue.

This is therefore not a pure generic filesystem optimization. It is a Git-aware fast workspace layered on top of the generic Drive9 FUSE semantics. The design intentionally reuses Git's own object model: the clean tree is represented by a Git manifest, dirty working tree changes are represented by a Drive9 overlay, and `.git` is kept in the local overlay with backend checkpoints.

## Goals

- `drive9 git clone --fast <repo-url> <mounted-path>` registers only the HEAD tree manifest and does not check out clean file content into `file_nodes`.
- The local `.git` object database is a full clone, while the working tree checkout is virtual. Build-time reads are served from local Git objects instead of Drive9 file rows or GitHub lazy blob fetches.
- Working tree changes such as edit/create/delete/chmod/symlink are persisted to the Drive9 backend overlay.
- `.git` keeps local disk read/write performance while being checkpointed to Drive9 for cross-sandbox recovery.
- `git add`, `git commit`, and `git push` should remain as close as possible to the native Git workflow, without introducing a separate push API.

## Non-Goals

- Do not add a full artifact-style command suite or turn this into a general content-addressed checkout tool.
- Do not write clean tree file content into `file_nodes` or `contents`.
- Do not persist the full Git object database in Drive9. Cross-sandbox recovery may depend on the original Git remote, such as GitHub, to repopulate local objects.
- Do not introduce local SQLite. Local state continues to use the existing local overlay, shadow files, and `journal.wal`; authoritative state lives in Drive9 backend Git workspace tables.
- Do not implement complete semantics for complex Git features in the first version, such as optimized submodule/LFS handling, merge conflict assistance, or automatic remote branch synchronization.

## Data Model

`git_workspaces`

- One row represents one fast workspace.
- Key fields: `workspace_id`, `root_path`, `repo_url`, `remote_name`, `branch_name`, `base_commit`, `head_commit`, `mode=fast`, and `status=active`.
- `root_path` is unique and maps to the repo root directory inside the mounted tree.

`git_workspace_tree_nodes`

- Stores the base/head commit tree manifest.
- Paths are relative to the workspace root and do not have a leading `/`.
- Includes `kind`, `mode`, `object_sha`, and `size_bytes` for directories, files, symlinks, and submodules.
- Clean tree file content is not stored in Drive9's generic file content tables.
- `size_bytes` is still populated without relying on checkout file content. Manifest generation uses `git ls-tree -r -t -z HEAD` and must not use `git ls-tree -l`; `-l` asks Git to print blob sizes and is unsafe if a future mode uses a partial clone again.
- For GitHub repositories, the CLI fills blob sizes through the GitHub Trees API. If the recursive response is truncated, it falls back to walking trees level by level. This preserves accurate `stat` and `git status` sizes while still avoiding blob downloads during clone.
- For non-GitHub repositories, unavailable GitHub API responses, or temporarily missing sizes, `size_bytes=-1` means unknown size. FUSE reports size 0 to the kernel attr path as a fallback, but real read/write/truncate/rename-copy paths load the base blob on demand and update the local inode size from the actual content length. This avoids treating unknown-size files as empty files.

`git_workspace_overlay`

- Stores working tree changes relative to the clean tree.
- `op=upsert` means a file or directory was created or modified; `op=whiteout` means a clean tree entry was deleted.
- File payloads are currently stored inline in `content_blob`; large-file support can later extend this to S3/storage refs.
- Directory creation is stored as an overlay entry with `kind=dir`.

`git_workspace_git_state`

- Stores `.git` directory checkpoints.
- The checkpoint intentionally excludes Git object databases such as `.git/objects` and `.git/modules/*/objects`.
- The current format is `storage_type=tar.gz-no-objects`, with the lightweight payload in `content_blob`.
- When sandbox B mounts the same drive, FUSE first recreates a full local `.git` object database from `repo_url`, then overlays the Drive9 checkpoint to restore refs, index, config, logs, and other lightweight state.
- Local commits that were not pushed to the remote are outside this recovery model because their objects are not stored in Drive9.

`file_nodes`

- Still represents ordinary Drive9 filesystem directories and files.
- Clean or dirty Git file content under a fast workspace should not leak into `file_nodes` file rows.
- The repo parent directory and repo root directory may exist as ordinary directory rows.

## Clone Flow

1. The user first mounts Drive9:

   ```bash
   drive9 mount --mode=fuse --profile=coding-agent --local-root <local-root> --cache-dir <cache> --durability=write-sync :/ <mountpoint>
   ```

2. The user runs:

   ```bash
   drive9 git clone --fast https://github.com/org/repo.git <mountpoint>/<path>/repo
   ```

3. The CLI runs a full-object no-checkout clone under the mounted path:

   ```bash
   git clone --no-checkout <repo-url> <target>
   ```

   `.git` is routed to local disk by the coding-agent local overlay and does not enter Drive9's generic file tables. The local object database is complete, so clean file reads during build are local Git object reads.

4. The CLI reads `HEAD`, branch, and `git ls-tree -r -t -z HEAD`, generates the tree manifest, and fills file sizes according to the `git_workspace_tree_nodes.size_bytes` rules.

5. The CLI initializes the Git index:

   ```bash
   git read-tree --reset HEAD
   ```

   This tells Git the clean tree object IDs without checking out file content.

6. The CLI calls Drive9 APIs:

   - upsert `git_workspaces`
   - replace `git_workspace_tree_nodes`
   - archive the local overlay `.git` directory without object databases and upsert `git_workspace_git_state`

7. After FUSE rediscovers the workspace, directory listings come from the synthetic view of `git_workspace_tree_nodes` plus `git_workspace_overlay`.

## Read/Edit/Add/Commit/Push Flow

Read a clean file:

- FUSE looks up the workspace manifest and constructs a virtual inode.
- On read, FUSE returns blob content through the local full-object `.git`.
- File content is not written into `file_nodes`.

Edit a file:

- When a tracked clean file is written, FUSE stores the new content in a `git_workspace_overlay` `upsert` entry.
- New files and directories also enter the overlay.
- Deleting a clean file writes a `whiteout`.
- With `write-sync`, the overlay is uploaded before write returns, so partially edited files survive an agent or sandbox stop.

`git add`:

- Git reads the clean+overlay synthetic working tree through FUSE.
- `.git/index`, objects, logs, and related files are written to the local overlay.
- Writable `.git` handles checkpoint to `git_workspace_git_state` on flush/release/rename write paths.
- Read-only `.git` handles do not checkpoint, which prevents commands such as `git status` from repeatedly uploading the full `.git` archive.

`git commit`:

- Git updates local `.git` through its native workflow.
- When the working tree overlay matches the Git index after commit, `git status` reports clean.
- The first version does not automatically write the new commit tree back as the new clean manifest or clear the overlay. The overlay remains relative to the original base tree.

`git push`:

- Git pushes natively from the local full-object `.git`.
- Drive9 does not participate in the push protocol.

## Sandbox Replacement Flow

1. Sandbox A mounts Drive9 and fast-clones a repo.
2. The agent edits files, runs `git add`, `git commit`, and `git push`, then leaves another uncommitted change.
3. A unmounts or stops.
4. Sandbox B mounts the same Drive9 path with a new local root and cache.
5. When B accesses the repo path, FUSE loads from the backend:

   - `git_workspaces`
   - `git_workspace_tree_nodes`
   - `git_workspace_overlay`
   - `git_workspace_git_state`

6. FUSE runs a full no-checkout clone from `repo_url` into B's local overlay to repopulate Git objects, then extracts the lightweight `.git` checkpoint over it.
7. B sees working tree = clean tree manifest + durable overlay; `git status`, `git log`, and file content are restored to A's last persisted state.

## Local State and SQLite

The current implementation does not introduce SQLite.

Local fast workspace state includes:

- `<local-root>/overlay/.../.git`: local `.git`.
- `<cache-dir>/<mount-id>/journal.wal`: the existing FUSE write/cache journal.
- shadow/cache files used by the existing write-back and strict durability mechanisms.

Authoritative cross-sandbox working tree state lives in backend DB Git workspace tables. Local files are the current sandbox's performance layer and restore target. Git object content is recovered from the Git remote rather than from Drive9.

## Dev E2E Validation

Validated in the dev environment on 2026-05-27:

- endpoint: `http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com`
- repo: `https://github.com/githubtraining/hellogitworld.git`
- run_id: `fastclone-e2e-20260527222938`
- tenant_id: `b1dc5fa2-f166-431b-9672-754887571975`
- workspace_id: `55ab295b-e674-4b24-80da-e515d7bf2f38`

Covered scenario:

- A mounts Drive9 and runs `drive9 git clone --fast`.
- Modify `README.txt`, create `notes/drive9-note.txt`, and delete `fix.txt`.
- Run `git add`, `git commit`, and push to a pre-mirrored bare remote.
- After the commit, modify `build.gradle` and leave it uncommitted.
- A unmounts; B mounts the same Drive9 path with a new local root/cache.
- B restores `.git`; `git log -1` shows the new commit and `git status --porcelain=v1` shows ` M build.gradle`.

Backend table checks:

- `git_workspaces`: active fast workspace row exists.
- `git_workspace_tree_nodes`: 26 rows, equal to the base commit tree count.
- `git_workspace_git_state`: `storage_type=tar.gz-no-objects`; payload contains lightweight `.git` state and excludes object databases.
- `git_workspace_overlay` contains:
  - `README.txt` upsert file
  - `notes` upsert dir
  - `notes/drive9-note.txt` upsert file
  - `fix.txt` whiteout file
  - `build.gradle` upsert file
- `file_nodes`: file row count under the workspace is 0; only the run directory and repo root directory rows exist.
- Local SQLite file count is 0.

## Known Limits and Follow-Ups

- `.git` checkpointing is objectless. Restoring local unpushed commits would require persisting Git objects through S3/storage refs or an object/pack dedup layer.
- After `git commit`, the overlay remains relative to the base tree. Later versions can add an explicit `drive9 git checkpoint` or an automatic manifest-advance mechanism.
- Large overlay files need to move from inline `content_blob` to S3/storage refs.
- Branch switching, merge, rebase, conflicts, submodules, and LFS need deeper semantic validation.
- The fast clone E2E should be turned into a repeatable script instead of relying on manual commands.
