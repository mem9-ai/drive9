# Drive9 Pack and Unpack Profile Spec

## Overview

Drive9 mounts can split a workspace into two kinds of state:

- **Remote-managed state** lives in Drive9 and is available from every mount.
- **Local overlay state** lives under the mount's `local-root` on the current
  machine. This is used for files that are expensive, noisy, or tool-specific,
  such as `.git`, `node_modules`, build outputs, and language caches.

`drive9 pack` and `drive9 unpack` make selected local overlay state portable.
A pack is a compressed archive stored as an ordinary Drive9 file. It lets a
later mount on another machine restore the local overlay before the filesystem
is exposed to tools.

This feature is intended for agent and developer workspaces where local
performance matters, but the workspace also needs to survive sandbox or host
replacement.

## Core Concepts

### Local Root

Overlay profiles use a `local-root` directory. Drive9 stores local-only files
under:

```text
<local-root>/overlay/
```

For example, if a mounted workspace contains `/repo/.git/config`, and `.git` is
routed to the local overlay, the corresponding local file is:

```text
<local-root>/overlay/repo/.git/config
```

The `local-root` itself is machine-local. A replacement host can use a different
`local-root`; pack/unpack restores the overlay content into that new root.

### Pack Archive

A pack archive contains:

- a manifest describing the profile, remote root, archived paths, replacement
  paths, and entries;
- regular files;
- directories;
- symlinks, with their original link target preserved.

The archive is uploaded to Drive9. It is not stored inside the local root.

### Profiles

A profile controls three things:

- `[local]`: paths that should live in the local overlay;
- `[remote]`: overrides that force paths back to Drive9-managed remote storage;
- `[pack]`: local overlay paths that should be included when packing.

The built-in profiles are:

| Profile | Local overlay policy | Automatic pack policy |
| --- | --- | --- |
| `coding-agent` | Routes common agent/developer local state to local disk, including `.git`, `node_modules`, `.venv`, caches, and build output. This is the default FUSE profile. | No automatic pack paths. Nothing is packed unless the user explicitly asks. |
| `portable` | Uses the same local overlay policy as `coding-agent`. | Packs all files and directories that currently exist under `local-root/overlay`. |
| `none` | No local overlay policy. | No pack policy. |

`coding-agent` is optimized for interactive local performance. `portable` is
optimized for moving that local overlay state across machines.

## Default Archive Location

When no archive path is specified, Drive9 uses a hidden archive path under:

```text
/.drive9/packs/
```

The concrete filename is derived from:

- the normalized mounted remote root, such as `/` or `/workspace`;
- the profile name, such as `portable`;
- a stable hash.

This means the same profile mounted at the same Drive9 remote root can find the
same default pack archive on the next mount, even if the mountpoint or
`local-root` changes.

The default archive path is intentionally independent of the local machine.
Changing the profile or remote root changes the default archive path.

## Automatic Mount and Unmount Behavior

Automatic pack/unpack is enabled only for overlay profiles that have `[pack]`
paths. Today this means `portable` and any custom profile with a non-empty
`[pack]` section.

### Mount

On mount, Drive9 does this before starting the FUSE filesystem:

1. Resolve the profile.
2. Resolve or create the `local-root`.
3. If the profile has `[pack]` paths, look for the default archive.
4. If the archive exists, unpack it into `local-root/overlay`.
5. If the archive does not exist, continue without restoring anything.
6. Start the mount.

Use `--no-auto-unpack` to disable this behavior:

```bash
drive9 mount --profile=portable --no-auto-unpack :/workspace /mnt/workspace
```

### Unmount

On unmount, Drive9 does this:

1. Ask the operating system/FUSE layer to unmount the filesystem.
2. Wait for the mount process to exit, subject to the `--timeout` value.
3. If unmount succeeded and the profile has `[pack]` paths, create and upload
   the default pack archive.

The pack happens after unmount because the unmount operation is what gives FUSE
the chance to flush and close active filesystem state. The pack upload is a
normal Drive9 client write to the hidden archive path, so it does not require
the old FUSE mount to be flushed again.

Use `--no-auto-pack` to disable automatic packing:

```bash
drive9 umount --no-auto-pack /mnt/workspace
```

If unmount fails, automatic pack does not run. Fix the busy mount and unmount
again, or run `drive9 pack` explicitly after the mount is clean.

## Common Usage

### Portable Workspace

Use `portable` when local overlay state should follow the workspace:

```bash
drive9 mount --mode=fuse --profile=portable :/workspace /mnt/workspace
cd /mnt/workspace
git clone <repo-url> repo
cd repo
npm install
make build
drive9 umount /mnt/workspace
```

On another machine:

```bash
drive9 mount --mode=fuse --profile=portable :/workspace /mnt/workspace
cd /mnt/workspace/repo
git status
```

The second mount automatically restores the saved local overlay before exposing
the filesystem. Files that were remote-managed are read from Drive9 as usual.
Files that were local-only, such as `.git` or `node_modules`, are restored from
the pack archive.

### Default Coding Agent Workspace

Without an explicit profile, FUSE mounts use `coding-agent`:

```bash
drive9 mount --mode=fuse :/workspace /mnt/workspace
```

This keeps common heavy paths local, but it does not automatically pack them.
If the local root is lost, those local-only paths are not restored unless the
user explicitly packed them.

### Manual Pack and Unpack

You can pack a mounted workspace using mount metadata:

```bash
drive9 pack --mount /mnt/workspace
```

To restore outside the automatic mount flow, provide the local root and remote
root directly:

```bash
drive9 pack \
  --local-root /var/lib/drive9/workspace-local \
  --remote-root /workspace \
  --profile portable

drive9 unpack \
  --local-root /var/lib/drive9/new-workspace-local \
  --remote-root /workspace \
  --profile portable
```

To use a custom archive path:

```bash
drive9 pack --mount /mnt/workspace :/packs/workspace-snapshot.tar.gz
drive9 unpack --local-root /tmp/restore :/packs/workspace-snapshot.tar.gz
```

Automatic mount restore only looks at the default archive path for the selected
profile and remote root. To restore a custom archive during mount, pass it
explicitly:

```bash
drive9 mount \
  --profile=portable \
  --unpack :/packs/workspace-snapshot.tar.gz \
  :/workspace /mnt/workspace
```

## What Gets Packed

Pack always reads from:

```text
<local-root>/overlay/
```

It does not fetch, copy, or duplicate files that are already Drive9-managed.
Those files already live in Drive9 and do not need to be packed.

For `portable`, `[pack] /` means:

> Pack every top-level entry that currently exists under `local-root/overlay`
> for this mounted remote root.

If the overlay contains:

```text
<local-root>/overlay/repo/.git/
<local-root>/overlay/repo/node_modules/
<local-root>/overlay/repo/dist/
```

then `portable` includes those local overlay entries. If a source file is
remote-managed and therefore is not present under `local-root/overlay`, it is
not included in the pack.

## Custom Profiles

Custom profiles live under:

```text
~/.drive9/profiles/<profile-name>
```

The file name is the profile name. Use `drive9 profile show` to inspect the
effective config:

```bash
drive9 profile show portable
drive9 profile show my-profile
```

Example:

```ini
[local]
**/.git/**
**/node_modules/**
**/.venv/**
**/dist/**

[remote]
**/dist/release/**

[pack]
.git
node_modules
.venv
dist
```

`[pack]` accepts these forms:

- `/` or `.`: pack all current local overlay entries under the mounted path.
- A simple name such as `.git` or `node_modules`: pack local overlay trees with
  that basename wherever they appear under the mounted path.
- An explicit relative or absolute path: pack that path if it exists in the
  local overlay.

`[pack]` is not a remote file selector. It selects local overlay content only.

## Replacement Semantics

Unpack is replacement-oriented by default.

Before installing archive content, Drive9 removes the paths recorded in the
pack manifest as replacement roots. This keeps deletes accurate. For example,
if `node_modules` was present in an older pack but was removed before the next
pack, a later unpack should not leave the old `node_modules` behind.

Use `--no-replace` to merge archive content into the local overlay without first
removing recorded replacement roots:

```bash
drive9 unpack --no-replace --local-root /tmp/restore --profile portable
```

The default replacement behavior is usually what users want for restoring a
workspace into a fresh or disposable local root.

## Safety and Validation

Drive9 unpacks into a temporary staging directory first. It validates the
archive manifest and entry paths before installing content into the real
`local-root/overlay`.

Safety rules include:

- archive entries cannot escape `local-root/overlay`;
- archive paths cannot contain `..`, backslashes, empty path segments, or NUL
  bytes;
- unpack refuses to write through symlink ancestors in the destination overlay;
- symlink targets are preserved, including absolute and relative targets, but
  empty targets, NUL bytes, and backslash targets are rejected.

This allows real local package manager layouts to restore correctly while still
preventing archive path traversal.

## Preserved Metadata

Pack/unpack preserves:

- regular file contents;
- directory structure;
- symlinks and symlink targets;
- POSIX permission bits;
- file and directory modification times.

It does not aim to preserve:

- file ownership;
- ACLs;
- extended attributes;
- device files, sockets, FIFOs, or other special file types.

Unsupported file types cause pack to fail rather than silently producing a
partial archive.

## Concurrency and Last Writer Wins

The default archive path is a single snapshot slot for a `(profile, remote
root)` pair. If two hosts mount the same remote root with the same profile and
both unmount with automatic packing enabled, the later successful pack replaces
the earlier default archive.

Use explicit archive paths if you need multiple snapshots:

```bash
drive9 pack --mount /mnt/workspace :/packs/workspace-before-upgrade.tar.gz
drive9 pack --mount /mnt/workspace :/packs/workspace-after-upgrade.tar.gz
```

## Design Limits

Pack/unpack is for local overlay portability. It is not a full backup of the
entire mounted tree.

In particular:

- remote-managed files are not packed;
- automatic restore only finds the default archive for the selected profile and
  remote root;
- custom archive paths must be restored explicitly with `drive9 unpack` or
  `drive9 mount --unpack`;
- local overlay content can be large, so `portable` may add noticeable time to
  unmount and first mount on a new machine;
- a broken or incompatible archive causes mount-time auto-unpack to fail rather
  than starting with partially restored local state.

Use `coding-agent` when local performance matters and local-only state can be
rebuilt. Use `portable` when the local overlay itself is part of the workspace
state you expect to recover.
