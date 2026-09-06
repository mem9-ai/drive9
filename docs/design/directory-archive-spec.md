---
title: drive9 directory archive (drive9 fs archive)
---

Design spec for downloading a remote directory tree as a single compressed
archive (tar.gz by default, zip optional), with profile-based file filtering.

## Motivation

Agents and operators frequently need to materialize a remote drive9 tree as a
single portable artifact — for handoff to a downstream tool, for offline
inspection, or for snapshotting a workspace without mounting FUSE. Until now
the only bulk-download path was `drive9 fs cp -r <remote> <local>`, which
materializes a directory tree on disk. `drive9 fs archive` produces a single
compressed file instead, streamable to stdout for piping.

## Command surface

```
drive9 fs archive <remote:/dir> [<out>] [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--format tar.gz\|zip` | `tar.gz` | Archive format |
| `--exclude <pattern>` | — | Skip paths matching pattern (repeatable) |
| `--include <pattern>` | — | Keep only paths matching pattern (repeatable) |
| `--profile <name>` | `none` | Apply profile `[local]`/`[remote]` rules |
| `--jobs <n>` | `16` | Concurrent file downloads |
| `--stdout` | off | Write archive to stdout (pipe-friendly) |
| `--flat` | off | Strip directory hierarchy; archive basenames only |
| `--output <path>` | `<basename>.<ext>` | Output file path |

Positional args: one remote directory + optional local output path. Flags may
appear before or after positionals (the CLI splits them so users are not forced
into a rigid ordering). Output defaults to `<dir basename>.tar.gz` when no path
or `--stdout` is given.

## Direction

- **Remote → local only.** A remote→remote archive is meaningless (no local
  artifact); a local→remote archive is the existing `drive9 pack` command's
  job. `archive` deliberately does not overlap with `pack`.
- The server stays purely file-level: there is no server-side tar/zip endpoint.
  The walk (`walkRemoteTreeBFS` via `c.ListCtx`) and per-leaf download
  (`c.ReadStream`) happen client-side, reusing the same primitives as
  `drive9 fs cp -r` (PR #664). The archive writer is the only net-new glue.

## Filtering semantics

The archive command exposes `--exclude` / `--include` (archive-domain
vocabulary) rather than the mount profile's `--local-only` / `--remote-only`
(the latter's "local layer vs remote layer" routing semantics read awkwardly in
a bulk-download context). `--profile` loads a profile's rules and translates
them internally:

```
exclude           = profile.[local]  + --exclude          (deduped)
include-override  = profile.[remote]                    (deduped; profile-only)
include-whitelist = --include                            (--profile does not participate)
```

Per-entry decision:

1. If `override` matches → include.
2. Else if `exclude` matches → drop.
3. Else if `--include` is non-empty and no include pattern matches → drop.
4. Otherwise → include.

Rationale for the override-only-from-profile choice: a user who writes an
`--exclude` and then wants to override it back is contradicting themselves and
should simply remove the exclude. The override mechanism only earns its keep
for preset rule sets (profiles), where e.g. "coding-agent skips node_modules,
but this project's node_modules/.package-lock.json must be archived."

### Pattern forms

The FUSE `LocalPolicy` matcher and SDK archives share these `pkg/pathfilter`
pattern forms:

| Form | Example | Meaning |
|------|---------|---------|
| `**/x/**` | `**/node_modules/**`, `**/cache-*/**` | Match a subpath at any depth and all descendants; each segment supports glob matching and literal equality |
| `**/glob` | `**/*-wal`, `**/cache-*/*.log` | Match at any depth, ending at the final path segment |
| `**/literal` | `**/node_modules` | Preserve the existing literal-subpath behavior, including descendants |
| `prefix/**` | `dist/**` | Everything under the prefix |
| exact / glob | `*.log`, `go.mod` | `path.Match` glob + exact equality |

In the leading `**/` form, `*`, `?`, and character classes match within a
single segment. Patterns containing these metacharacters must end at the final
segment unless they have a trailing `/**`. Thus `**/*-wal` matches a WAL filename
at any depth but does not match `/snapshots-wal/data.bin`. Use `**/cache-*/**` to
include a matching directory's descendants. Rules without glob metacharacters
retain the existing literal-subpath behavior.

Go and TypeScript match `?` and character classes by Unicode code point.
Character classes follow Go `path.Match` syntax, including non-empty classes
and well-formed range endpoints. Invalid globs retain literal-equality fallback.

### Directory pruning

When a directory's relative path matches an exclude (and no override restores
it), subtree rules skip the entire subtree at BFS time — no extra `ListCtx` round-trips
for children that would be dropped. This is what makes `--profile coding-agent`
skip `node_modules/` without exploding the request count.

Recursive file globs exclude matching entries without pruning their descendants;
the walker continues into matching directories and filters each child separately.

## Archive formats

### tar.gz (default)

`tar.NewWriter(gzip.NewWriter(out))`. Each leaf is streamed directly from
`c.ReadStream` into the tar writer via `io.Copy` — no temp file, no full-tree
buffering. Directory entries are emitted first so empty dirs survive
extraction. Downloads run in parallel (bounded by `recursiveCopyConcurrency` =
16) but header+body writes are serialized via `tarWriteMu` so the tar byte
stream stays coherent. Symlinks are currently archived as regular files (their
target content via `ReadStream`), matching `cp -r`'s behavior; a future PR can
emit `TypeSymlink` headers once the client exposes a ReadLink primitive.

### zip

`archive/zip.NewWriter(out)`. zip requires sequential central-directory
offsets, so the writer is single-threaded; download concurrency still prefetches
the next leaf's `ReadStream`. zip uses `Deflate` for file bodies and `Store`
for directory entries.

## Relationship to `cp -r` and `pack`

| Command | Direction | Output | Filter |
|---------|-----------|--------|--------|
| `fs cp -r` | remote→local / local→remote / remote→remote | Directory tree on disk | none |
| `fs archive` | remote→local | Single compressed file | profile + exclude/include |
| `pack` | local overlay→remote file | Remote tar.gz snapshot | profile `pack` paths |

`archive` shares `walkRemoteTreeBFS` + `parallelTransfer` with `cp -r` but
diverges at the write step (archive writer vs per-file download). `pack` is
the inverse direction (local→remote) and uses a different archive layout
(`drive9.pack.v1` manifest).

## TS SDK

`clients/drive9-js` mirrors the API:

```ts
client.archive("/proj", { exclude: ["**/node_modules/**"] })
  : Promise<ReadableStream<Uint8Array>>  // tar.gz stream

client.archiveToFile("/proj", "./proj.tar.gz", { profile: "coding-agent" })
  : Promise<void>  // tar.gz or zip to a file
```

The TS SDK ships a zero-runtime-dependency ustar tar writer and a minimal zip
(STORE) writer so the package stays dependency-light. The TS pathfilter
(`src/pathfilter.ts`) re-implements the three pattern forms to keep Go/TS
matching semantics aligned.

## Reused primitives

- `walkRemoteTreeBFS` (`cmd/drive9/cli/cp_recursive.go`) — client-driven BFS
  tree walk via `c.ListCtx`.
- `parallelTransfer` — bounded (16) concurrency with sibling-continues-on-
  failure semantics.
- `loadProfileConfig` / `mergeProfileValues` (`cmd/drive9/cli/profile.go`) —
  profile loading and dedup-merge.
- `pkg/pathfilter` (new) — extracted from `pkg/fuse/local_policy.go` so the
  CLI does not depend on `pkg/fuse`. The FUSE `LocalPolicy` is now a thin
  wrapper over `pathfilter.Pattern` / `Matcher`, with zero behavior change.
