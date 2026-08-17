---
name: drive9
description: Persistent network filesystem for TiDB Cloud users — store, mount, search, and share workspace files across sessions and sandboxes.
metadata:
  version: 0.1.0
---

# drive9

drive9 is a persistent workspace for sandboxes and agents. Files survive context resets, session restarts, and agent handoffs. The default workflow is to mount a remote path as a local folder and use ordinary tools (`vim`, `code`, `git`, `python`). You can also operate on remote files directly with `drive9 fs`.

This skill is for **TiDB Cloud** users. Every `drive9 create` and `drive9 ctx add` must pass the Drive9 server URL. `drive9 create` must also pass the user's TiDB Cloud API public key (AK) and private key (SK).

## Setup

Do not run `drive9 create` with no flags. Collect the user's TiDB Cloud **public key** and **private key** first. If either key is missing, ask for both and wait. Do not invent keys, and do not echo them back after the command runs.

The server URL is required on create (and on `ctx add`). Always pass:

```text
https://aws-ap-southeast-1.drive9.ai/
```

If `drive9 ctx show` already returns a context name, skip `drive9 create`.

Otherwise install the CLI (macOS / Linux), then create a workspace:

```bash
curl -fsSL https://drive9.ai/install.sh | sh

drive9 create --server https://aws-ap-southeast-1.drive9.ai/ \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>
```

Do not stop to ask for confirmation between install and create once both keys are available.

`drive9 create` provisions a workspace and saves credentials to `~/.drive9/config`. Then activate the new context and mount it:

```bash
drive9 ctx use <CTX_ID>
drive9 fs mkdir :/my-project
drive9 mount :/my-project ./workspace
```

`./workspace` is now backed by drive9. Use it like any local folder:

```bash
echo "hello drive9" > ./workspace/hello.txt
cat ./workspace/hello.txt
cp ./report.pdf ./workspace/
git clone https://github.com/your/repo ./workspace/repo
```

Unmount when done:

```bash
drive9 umount ./workspace
```

Setup is complete when `drive9 create` succeeds and `drive9 ctx show` returns a context name.

---

## When to use drive9

drive9 is useful in two directions — **storing** and **retrieving**. Prefer the mount as the working directory.

**Storing**: When the user wants to save, persist, or back up a file (e.g. "save this report", "keep this config for later"), write it into the mounted workspace or `drive9 fs cp` it to a remote path. Confirm with the user before uploading.

**Retrieving**: When the user is looking for a file they previously saved (e.g. "find that pricing doc", "what did I save about the migration plan"), look in the mount or use `drive9 fs grep` / `drive9 fs find`.

**Isolated edits**: When an agent or sandbox should try changes without touching the shared workspace, create a layer, mount it, then `commit` or `rollback`.

In all cases, briefly tell the user what you're about to do.

---

## Commands

All commands exit 0 on success, non-zero on failure. Remote paths start with `:/` — that is the drive9 root.

### Context management

Each context is an isolated credential scope. Owner contexts hold an API key.

```bash
drive9 create --server https://aws-ap-southeast-1.drive9.ai/ \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>
drive9 ctx show
drive9 ctx ls
drive9 ctx use <name>
drive9 ctx rm <name>
```

`drive9 create` may take `--name <name>`. Never omit `--server`, `--tidbcloud-public-key`, or `--tidbcloud-private-key`.

### Sharing a workspace

The same workspace can be mounted from another sandbox, another machine, or by a teammate. Share the Drive9 API key (the key stored in the owner context, not the TiDB Cloud keys). On the other side:

```bash
drive9 ctx add --name shared --server https://aws-ap-southeast-1.drive9.ai/ --api-key "THE_API_KEY"
drive9 ctx use shared
drive9 mount :/my-project ./workspace
```

`--server` is required on `ctx add`. Files written by one sandbox are immediately available to the next.

### Local mounts

```bash
drive9 mount :/my-project ./workspace
drive9 umount ./workspace
```

The remote path `:/my-project` must already exist. Create it with `drive9 fs mkdir :/my-project`.

After mounting, the directory behaves like a local folder. Verify with ordinary tools:

```bash
ls ./workspace
df -h ./workspace
```

A mount binds to the context active at mount time. After `drive9 ctx use`, unmount and mount again.

### Layer operations

A layer is a writable overlay on a drive9 path. The base stays unchanged while you work. `commit` applies the overlay back to the base; `rollback` discards it.

**Create a layer and mount it**

```bash
drive9 fs layer create :/my-project --name fix-auth
drive9 mount --mode=fuse --layer fix-auth :/my-project ./attempt
```

`./attempt` behaves like a local folder. Reads fall through to the base; writes stay in the layer until you commit. `--layer` requires FUSE (`--mode=fuse`). You can create more than one layer on the same base and mount each in its own directory.

**Inspect**

```bash
drive9 fs layer list
drive9 fs layer status fix-auth
drive9 fs layer diff fix-auth
```

Refer to a layer by id, name, or tag (for example `tag:agent=a`).

**Checkpoint**

```bash
drive9 fs layer checkpoint fix-auth --label tests-pass
```

A checkpoint is a restore point. Remount a checkpoint as a read-only view:

```bash
drive9 mount --mode=fuse --layer fix-auth --checkpoint <checkpoint-id> :/my-project ./restore
```

**Commit or roll back**

```bash
drive9 fs layer commit fix-auth
drive9 fs layer rollback fix-auth
```

Commit is all-or-nothing. If the base changed while the layer was open, the layer becomes `conflicted` and stays available for review — it is not overwritten or discarded.

| Command | Description |
|---|---|
| `drive9 fs layer create :/path --name NAME` | Create a layer on a base path |
| `drive9 fs layer list` | List layers |
| `drive9 fs layer status NAME` | Show state, base, and durable seq |
| `drive9 fs layer diff NAME` | List overlay entries (add / modify / delete) |
| `drive9 fs layer checkpoint NAME --label LABEL` | Record a restore point |
| `drive9 fs layer commit NAME` | Apply the layer to the base |
| `drive9 fs layer rollback NAME` | Discard the layer |

#### Next release — copy-on-write fork

The next CLI version adds copy-on-write fork. Fork creates a child layer from an existing layer without copying files, so two attempts can share work so far and then diverge. Do not run these commands until that CLI is installed.

```bash
drive9 fs layer fork fix-auth --name fix-auth-b
drive9 fs layer fork fix-auth --name restore-view --checkpoint <checkpoint-id>
drive9 mount --mode=fuse --layer fix-auth-b :/my-project ./attempt-b
drive9 fs layer chain fix-auth-b
drive9 fs layer delete fix-auth-b
drive9 fs layer delete fix-auth --cascade
```

`fork` pins the child to the parent's current tip, or to a checkpoint if you pass `--checkpoint`. Reads walk the child, then the parent at that pin, then the base. `commit` on any layer still writes the effective view to the base, not into the parent. `delete --cascade` abandons a parent and its descendants together.

### File operations

Operate on remote files directly, without mounting.

**Browse and read**

| Command | Description |
|---|---|
| `drive9 fs ls :/path` | List directory contents |
| `drive9 fs ls -l :/path` | List with size and timestamp |
| `drive9 fs cat :/path/file.txt` | Print file contents |
| `drive9 fs stat :/path/file.txt` | Show file metadata |

**Upload and download**

| Command | Description |
|---|---|
| `drive9 fs cp local.txt :/remote.txt` | Upload a file |
| `drive9 fs cp :/remote.txt ./local.txt` | Download a file |
| `drive9 fs cp :/remote.txt -` | Download to stdout |

**Organize**

| Command | Description |
|---|---|
| `drive9 fs mkdir :/new-dir` | Create a directory |
| `drive9 fs mv :/old :/new` | Move or rename |
| `drive9 fs rm :/file` | Delete a file |
| `drive9 fs rm -r :/dir` | Delete a directory |

**Search**

| Command | Description |
|---|---|
| `drive9 fs grep "keyword" :/` | Search file contents |
| `drive9 fs grep "keyword" :/docs` | Search within a directory |
| `drive9 fs find :/ -name "*.md"` | Find files by name |
| `drive9 fs find :/ -newer 2026-01-01` | Find files modified after a date |
| `drive9 fs find :/ -size +1000000` | Find files larger than 1MB |

Use `grep` to find files by what they contain. Use `find` to find files by name, date, or size.

### Git operations

Drive9 provides a Git-aware fast clone for repositories inside a mounted drive:

```bash
drive9 git clone --fast <repo-url> <mounted-path>
```

Fast clone does not materialize every clean working-tree file as normal drive9 file rows. Instead, drive9 records the repository `HEAD` tree as a Git workspace manifest, keeps `.git` in the local overlay, and exposes the working tree through the FUSE Git workspace layer. This makes large repositories much faster to clone while preserving normal Git and file operations after mount.

By default, `--fast` uses a full local Git object database:

```bash
drive9 git clone --fast https://github.com/org/repo.git <mountpoint>/repo
```

Use `--blobless` when initial clone latency and network transfer matter more than having all blobs locally immediately:

```bash
drive9 git clone --fast --blobless https://github.com/org/repo.git <mountpoint>/repo
```

`--blobless` uses Git partial clone (`--filter=blob:none`). Drive9 registers the HEAD tree immediately, then hydrates clean blob content later. Hydration runs in the background by default; use `--hydrate=sync` to wait before returning, or `drive9 git hydrate` to hydrate an existing blobless workspace.

```bash
drive9 git clone --fast --blobless --hydrate=sync https://github.com/org/repo.git <mountpoint>/repo
drive9 git hydrate <mountpoint>/repo
```

Rule of thumb: use plain `--fast` for better local Git performance after clone; use `--fast --blobless` for faster startup and lower initial data transfer.

---

## Platform notes

### macOS

drive9 auto-detects the mount backend:

- **FUSE** (recommended) — full POSIX filesystem semantics. Supports `git`, `symlink`, file locking, and all standard development tools. Requires [macFUSE](https://osxfuse.github.io/):

```bash
brew install --cask macfuse
```

Allow the system extension in **System Settings → Privacy & Security**, then restart.

- **WebDAV** (fallback) — used automatically when macFUSE is not installed. Built into macOS, zero dependencies. Sufficient for basic file read/write, but does not support symlinks, file locking, or some `git` operations.

For development workflows, install macFUSE. Layer mounts (`--layer`) require FUSE (`--mode=fuse`).

### Linux

FUSE is pre-installed on most Linux distributions, including E2B sandboxes and cloud VMs.

Verify:

```bash
ls /dev/fuse
```

If `/dev/fuse` exists, you're ready. No additional setup needed.

---

## Environment variables

| Variable | Description |
|---|---|
| `DRIVE9_SERVER` | Does not replace `--server` on `create` / `ctx add`. Always pass `--server https://aws-ap-southeast-1.drive9.ai/` on those commands. |
| `DRIVE9_API_KEY` | Owner API key override; otherwise the active owner context in `~/.drive9/config` |

---

## Error handling

| Symptom | Cause | Fix |
|---|---|---|
| `no current context` | No context created yet | Run `drive9 create` with `--server` and both TiDB Cloud keys |
| `context "X" not found` | Typo or deleted context | Run `drive9 ctx ls` to see available names |
| `provision failed` | Server unreachable, wrong URL, or bad keys | Confirm `--server https://aws-ap-southeast-1.drive9.ai/` and both keys |
| `401` / `403` | Invalid owner API key | Run `drive9 ctx ls` to verify, then `drive9 create` or `drive9 ctx add` with `--server` |
| `404` on file ops / remote directory not found | Path does not exist | Create it first: `drive9 fs mkdir :/name` |
| `Permission denied` on mount | Wrong or missing context | Run `drive9 ctx` / `drive9 ctx show` |
| `mount` hangs or fails on macOS | Mount point still in use | Close editors or terminals using the mount point, then retry |
| `umount` fails | A process still has the directory open | Close programs accessing the mounted directory first |
| Non-zero exit, no output | Generic failure | Re-run with `DRIVE9_CLI_LOG_ENABLED=true` for debug logs |

## Tips

- Always pass `--server https://aws-ap-southeast-1.drive9.ai/` to `drive9 create` and `drive9 ctx add`.
- Always pass `--tidbcloud-public-key` and `--tidbcloud-private-key` to `drive9 create`.
- Prefer the mount for day-to-day work; use `drive9 fs` when you do not want to mount.
- Create the remote directory with `drive9 fs mkdir` before `drive9 mount`.
- Use a layer when an agent should try changes without writing the shared workspace.
- Use `drive9 git clone --fast` inside a FUSE mount for large repositories.
- Use `drive9 --help` and `drive9 fs --help` to inspect the current CLI surface.
