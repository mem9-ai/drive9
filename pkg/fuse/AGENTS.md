---
title: FUSE mount layer — drive9/pkg/fuse
---

## Purpose

FUSE mount implementation using go-fuse/v2 RawFileSystem. Two filesystems: **Dat9FS** (read-write network drive) + **VaultFS** (read-only secrets). ~48 files, ~33K lines.

## Sub-systems

| Sub-system | Key files | What it does |
|---|---|---|
| Mount entrypoint | `mount.go`, `mount_profile.go`, `mode.go` | Mount options, profiles, sync policies |
| Read pipeline | `read.go`, `prefetch.go`, `readdir_prefetch.go` | Read cache, adaptive prefetch (256KB→16MB sliding window) |
| Write pipeline | `write.go`, `stream_upload.go` | WriteBuffer (sparse part-map), streaming multipart upload |
| Crash recovery | `shadow.go`, `journal.go`, `commit_queue.go`, `pending_index.go`, `remote_upload.go` | ShadowStore→PendingIndex→Journal→CommitQueue |
| Cache invalidation | `sse.go`, `dir.go`, `debounce.go` | SSE watcher (self-filters own actorID), DirCache (TTL), flush debouncer |
| Local overlay | `local_policy.go`, `local_overlay.go` | Local-only path routing for coding-agent profile |

## Write classification (4 modes)

| Mode | Trigger | Upload path |
|---|---|---|
| Small file | <50KB | Direct PUT after close/debounce |
| Sequential append | New appending file | Streaming v2 multipart during Write() |
| Non-sequential new | Back-write detected | Shadow spill + async upload |
| Existing edit | Opened w/o truncate | Dirty part PATCH at flush |

## Crash recovery stack

Write() → ShadowStore (disk) → PendingIndex (in-memory) → Journal (WAL, CRC32) → CommitQueue (async upload + backoff).

Recovery order on startup: Journal.Replay → PendingIndex.RecoverFromDisk → CommitQueue.RecoverPending.

## Read priority chain (10-step cascade)

local-only → shadow → dirty-buf → shadow-store → writeback-cache → prefetcher → read-cache → small-file-http → range-read.

## Conventions

- **Thread-unsafe WriteBuffer** — callers must hold `FileHandle.mu`.
- **Public-field FileHandle** — 30+ public fields, no getters. Lock via explicit `Lock()`/`Unlock()`.
- **Massive `dat9fs.go`** (~6K lines) — intentional. All RawFileSystem methods in one file matching go-fuse interface shape.
- **Generic HandleTable[T]** — Go generics for FileHandle and DirHandle allocation.
- **Generation-based pin/retire** — ShadowStore uses monotonic gen tokens for safe concurrent read during commit cleanup.
- **SSE self-filtering** — SSEWatcher skips events from own actorID.

## Anti-patterns

- Do not access FileHandle fields without holding `fh.mu`.
- Do not store Dat9FS state without explicit mutex protection.
- Do not add WriteBuffer methods without documenting lock requirements.

## Failpoint tests

`//go:build failpoint` + `scripts/run_failpoint_tests.py`. Never parallel with normal tests.

## Key types

Dat9FS, VaultFS, MountOptions, FileHandle, ReadCache, WriteBuffer, ShadowStore, Journal, CommitQueue, PendingIndex, WriteBackCache, WriteBackUploader, StreamUploader, Prefetcher, InodeToPath, SSEWatcher, LocalPolicy, LocalOverlay, DirCache, HandleTable, SyncMode, WritePolicy.
