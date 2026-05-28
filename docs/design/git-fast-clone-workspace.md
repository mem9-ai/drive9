# Drive9 Git Fast Workspace Design

## Background

Drive9 has two high-priority requirements for agent workspaces:

- `git clone` into a mounted directory must be fast, without checking out the full working tree into Drive9's generic file tables.
- After a sandbox or container is replaced, the agent's partially edited working tree and `.git` state must be recoverable from Drive9 so `git status`, `git add`, `git commit`, and `git push` can continue.

This is therefore not a pure generic filesystem optimization. It is a Git-aware fast workspace layered on top of the generic Drive9 FUSE semantics. The design intentionally reuses Git's own object model: the clean tree is represented by a Git manifest, dirty working tree changes are represented by a Drive9 overlay, and `.git` is kept in the local overlay with backend checkpoints.

## Goals

- `drive9 git clone --fast <repo-url> <mounted-path>` registers only the HEAD tree manifest and does not check out clean file content into `file_nodes`.
- The default local `.git` object database is a full clone, while the working tree checkout is virtual. Build-time reads are served from local Git objects instead of Drive9 file rows.
- `drive9 git clone --fast --blobless` is an opt-in mode that keeps the local clone blobless and lets Git lazy-fetch clean blobs from the remote on first read.
- Blobless mode keeps clone latency low while avoiding build-time lazy-fetch amplification by hydrating clean blobs into a hidden local cache and serving clean reads from that cache before falling back to Git.
- Working tree changes such as edit/create/delete/chmod/symlink are persisted to the Drive9 backend overlay.
- `.git` keeps local disk read/write performance while lightweight state and small local-only Git objects are checkpointed to Drive9 for cross-sandbox recovery.
- `git add`, `git commit`, and `git push` should remain as close as possible to the native Git workflow, without introducing a separate push API.

## Non-Goals

- Do not add a full artifact-style command suite or turn this into a general content-addressed checkout tool.
- Do not write clean tree file content into `file_nodes` or `contents`.
- Do not persist the full Git object database in Drive9. Cross-sandbox recovery may depend on the original Git remote, such as GitHub, to repopulate clean objects.
- Do not store remote clean blobs in Drive9. The only Git objects Drive9 may store are local-only objects needed to restore staged or local Git state.
- Do not treat the hidden hydrated clean-tree cache as authoritative state. It is local, rebuildable, and may be discarded with the sandbox or local root.
- Do not introduce local SQLite. Local state continues to use the existing local overlay, shadow files, and `journal.wal`; authoritative state lives in Drive9 backend Git workspace tables.
- Do not implement complete semantics for complex Git features in the first version, such as optimized submodule/LFS handling, merge conflict assistance, or automatic remote branch synchronization.

## Data Model

`git_workspaces`

- One row represents one fast workspace.
- Key fields: `workspace_id`, `root_path`, `repo_url`, `remote_name`, `branch_name`, `base_commit`, `head_commit`, `mode`, and `status=active`.
- `mode=fast` is the default full-object local clone; `mode=fast-blobless` is the opt-in blobless local clone.
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
- When sandbox B mounts the same drive, FUSE recreates local `.git` from `repo_url`, applies any Drive9 local-only object packs, then overlays the Drive9 checkpoint to restore refs, index, config, logs, and other lightweight state.

`git_workspace_object_packs`

- Stores inline Git packfiles for local-only objects that are not available from the remote.
- `pack_id` is the SHA-256 checksum of the pack bytes. Uploads are idempotent by `(workspace_id, pack_id)`.
- V1 stores packs inline in `content_blob`; no S3/storage-ref path is used.
- Per local-only blob cap: 5 MiB. Total inline pack cap: 256 MiB.
- Oversized staged objects are not packed. Their working-tree content still survives through `git_workspace_overlay`, but staged state is downgraded to unstaged on restore.
- Local refs, stash, or reflog state that would require omitted oversized objects is dropped from the checkpoint rather than restoring broken refs.

`file_nodes`

- Still represents ordinary Drive9 filesystem directories and files.
- Clean or dirty Git file content under a fast workspace should not leak into `file_nodes` file rows.
- The repo parent directory and repo root directory may exist as ordinary directory rows.

Hidden local clean cache

- Blobless workspaces use a hidden, rebuildable cache under `<local-root>/git-workspaces/<workspace-id>/<head-commit>/`.
- `tree/` stores a materialized clean source tree for fast build-time reads.
- `blobs/<sha-prefix>/<sha>` stores read-through clean blob cache entries when a file is read before hydrate finishes.
- `hydrate.json` records the last hydrate provider, status, file count, byte count, duration, and error string.
- The cache key includes workspace ID and head commit so old clean content is not reused after a manifest change.

Coding-agent local overlay policy

- The coding-agent mount profile routes heavyweight local state and generated output to `<local-root>/overlay` instead of Drive9 backend storage.
- Default local-only paths include VCS state (`.git`, `.hg`, `.svn`), dependency directories (`node_modules`, `.venv`, `.pnpm-store`), build outputs (`dist`, `build`, `target`, `coverage`), temporary/cache directories (`tmp`, `.tmp`, `.cache`, `.turbo`, `.next/cache`, `.vitepress/cache`), and tool-specific generated output such as `.tmp-api-extractor`.
- These local-only paths are still merged into FUSE directory listings with tracked Git workspace entries, so generated directories under a tracked source directory remain visible to local build tools without being uploaded to Drive9.
- Local-only dependency and generated-output files are a rebuildable performance layer. Their ordinary FUSE `Flush` path does not force `fsync`; it refreshes local inode metadata only. Explicit `Fsync` still syncs the local file, and `.git` local-only state still syncs/checkpoints so Git state can be restored in replacement sandboxes.

## Clone Flow

1. The user first mounts Drive9:

   ```bash
   drive9 mount --mode=fuse --profile=coding-agent --local-root <local-root> --cache-dir <cache> --durability=write-sync :/ <mountpoint>
   ```

2. The user runs one of:

   ```bash
   drive9 git clone --fast https://github.com/org/repo.git <mountpoint>/<path>/repo
   drive9 git clone --fast --blobless https://github.com/org/repo.git <mountpoint>/<path>/repo
   drive9 git clone --fast --blobless --hydrate=sync https://github.com/org/repo.git <mountpoint>/<path>/repo
   ```

   Blobless hydrate modes are:

   - `--hydrate=background` (default for `--blobless`): return after workspace registration and hydrate the clean tree in the background.
   - `--hydrate=sync`: materialize the clean tree before returning, useful for deterministic diagnostics and benchmarking.
   - `--hydrate=off`: preserve the pure lazy-fetch behavior.

3. The CLI runs a no-checkout clone under the mounted path:

   ```bash
   git clone --no-checkout <repo-url> <target>
   git clone --filter=blob:none --no-checkout <repo-url> <target> # with --blobless
   ```

   `.git` is routed to local disk by the coding-agent local overlay and does not enter Drive9's generic file tables. In default mode the local object database is complete; in blobless mode clean blobs are fetched by Git into the local object database only when read.

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

7. In blobless mode, the CLI starts `drive9 git hydrate <mounted-path>` in the background unless `--hydrate=sync` or `--hydrate=off` was selected. GitHub repositories use codeload tarballs for batch hydrate; non-GitHub repositories fall back to a separate Git index plus `git checkout-index` into the hidden local cache.

8. After FUSE rediscovers the workspace, directory listings come from the synthetic view of `git_workspace_tree_nodes` plus `git_workspace_overlay`. FUSE also best-effort starts hydrate for `mode=fast-blobless` workspaces so replacement sandboxes still warm their local cache.

9. In the coding-agent mount profile, FUSE treats repository-ignored generated paths as local-only. The policy first applies explicit local/remote patterns; for otherwise remote-default paths inside a Git workspace, it runs cached `git check-ignore` against the hidden hydrated clean tree and the local `.git` state. Paths that Git would ignore are routed to the local overlay, while tracked clean files and durable Git overlay entries keep their normal Git workspace semantics.

## Read/Edit/Add/Commit/Push Flow

Read a clean file:

- FUSE looks up the workspace manifest and constructs a virtual inode.
- On read, FUSE first checks the hidden materialized clean tree, then the local blob cache, and only then falls back to `git cat-file blob <sha>`.
- If fallback reads a clean blob, FUSE writes it to the local blob cache with singleflight protection so concurrent reads of the same object do not fan out into repeated Git lazy fetches.
- In blobless mode, hydrate is intended to move remote blob download off the build hot path. If hydrate has not finished, fallback Git lazy fetch still works for correctness.
- File content is not written into `file_nodes`.

Edit a file:

- When a tracked clean file is written, FUSE stores the new content in a `git_workspace_overlay` `upsert` entry.
- New files and directories also enter the overlay.
- In the coding-agent profile, untracked paths matched by the repository's `.gitignore` are local-only. This keeps repo-specific build outputs such as generated web assets, package build directories, and temporary tool output off Drive9 without hand-maintaining per-repo mount patterns.
- Deleting a clean file writes a `whiteout`.
- With `write-sync`, the overlay is uploaded before write returns, so partially edited files survive an agent or sandbox stop.

`git add`:

- Git reads the clean+overlay synthetic working tree through FUSE.
- `.git/index`, objects, logs, and related files are written to the local overlay.
- Writable `.git` handles sync local file state and checkpoint local-only object packs first, then `git_workspace_git_state`, on flush/fsync/release/rename write paths.
- Read-only `.git` handles do not checkpoint, which prevents commands such as `git status` from repeatedly uploading the full `.git` archive.
- If a staged blob is larger than 5 MiB, restore downgrades that staged state to unstaged. The file content remains durable through the Drive9 dirty overlay.

`git commit`:

- Git updates local `.git` through its native workflow.
- When the working tree overlay matches the Git index after commit, `git status` reports clean.
- The first version does not automatically write the new commit tree back as the new clean manifest or clear the overlay. The overlay remains relative to the original base tree.

`git push`:

- Git pushes natively from the local `.git`.
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

6. FUSE runs a no-checkout clone from `repo_url` into B's local overlay:
   - `mode=fast`: full clone.
   - `mode=fast-blobless`: blobless partial clone.
7. FUSE downloads and unpacks local-only object packs, then extracts the lightweight `.git` checkpoint over the local clone.
8. B sees working tree = clean tree manifest + durable overlay; `git status`, `git log`, and file content are restored to A's last persisted state, subject to the explicit oversized staged/local-ref downgrade rules.

## Local State and SQLite

The current implementation does not introduce SQLite.

Local fast workspace state includes:

- `<local-root>/overlay/.../.git`: local `.git`.
- `<local-root>/overlay/.../<ignored-path>`: coding-agent local-only files for paths matched by static local-only patterns or repository `.gitignore` rules.
- `<local-root>/git-workspaces/<workspace-id>/<head-commit>/tree`: hidden hydrated clean source tree cache.
- `<local-root>/git-workspaces/<workspace-id>/<head-commit>/blobs`: hidden read-through clean blob cache.
- `<cache-dir>/<mount-id>/journal.wal`: the existing FUSE write/cache journal.
- shadow/cache files used by the existing write-back and strict durability mechanisms.

Authoritative cross-sandbox working tree state lives in backend DB Git workspace tables. Local files are the current sandbox's performance layer and restore target. Clean Git object content is recovered from the Git remote or a local hidden hydrate cache; local-only Git object content may be recovered from inline Drive9 object packs under the v1 size limits.

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
- `git_workspace_object_packs`: empty for the historical full-clone pushed-commit validation unless staged/local-only objects need recovery.
- `git_workspace_overlay` contains:
  - `README.txt` upsert file
  - `notes` upsert dir
  - `notes/drive9-note.txt` upsert file
  - `fix.txt` whiteout file
  - `build.gradle` upsert file
- `file_nodes`: file row count under the workspace is 0; only the run directory and repo root directory rows exist.
- Local SQLite file count is 0.

## Known Limits and Follow-Ups

- Object packs are inline only in v1. Local-only Git objects above the 5 MiB per-blob cap or 256 MiB pack cap are downgraded instead of being preserved as staged/local refs.
- Restoring large local-only Git objects without downgrades will require S3/storage refs or an object/pack dedup layer.
- After `git commit`, the overlay remains relative to the base tree. Later versions can add an explicit `drive9 git checkpoint` or an automatic manifest-advance mechanism.
- Large overlay files need to move from inline `content_blob` to S3/storage refs.
- Branch switching, merge, rebase, conflicts, submodules, and LFS need deeper semantic validation.
- V1 hydrate does not optimize LFS or submodule object fetching. Non-GitHub repositories use a Git checkout-index fallback, which can be slower than GitHub codeload but still keeps the work out of FUSE read calls.
- The fast clone E2E should be turned into a repeatable script instead of relying on manual commands.
