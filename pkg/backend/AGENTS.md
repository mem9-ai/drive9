---
title: AGENTS.md - drive9 backend package (pkg/backend)
---

## Overview

Core filesystem backend implementing `filesystem.FileSystem` (AGFS).
Central type: `Dat9Backend`. ~24 non-test Go files, ~16K lines, flat `package backend`.

## File map

| File | Responsibility |
|---|---|
| `dat9.go` (1598 lines) | Dat9Backend, all FS operations (Create/Read/Write/Mkdir/Remove/Rename/Stat/ReadDir/CopyFile/Chmod/Symlink/Grep/Find/ExecSQL), ReadPlan |
| `upload.go` | V1/V2 upload protocol: Initiate, Presign, Confirm, Resume, Abort |
| `patch.go` | Patch/Append for large S3 files via UploadPartCopy |
| `upload_reservation.go` | Server-reserve-first saga for central quota |
| `quota.go`, `quota_store.go`, `quota_mutation.go` | Dual-source quota (tenant-DB vs server-DB), durable outbox |
| `mutation_replay.go` | Background MutationReplayWorker for failed quota mutations |
| `file_gc_worker.go` | Durable file cleanup via file_gc_tasks, lease-based claiming |
| `object_gc.go`, `expiry_sweep.go` | S3 object GC, upload reservation expiry sweep |
| `semantic_tasks.go` | Enqueue durable tasks (embed/img_extract/audio_extract) |
| `image_extract*.go` | Pluggable ImageTextExtractor (basic + OpenAI vision + structured writeback) |
| `audio_extract*.go` | Pluggable AudioTextExtractor (OpenAI ASR + Qwen ASR) |
| `llm_usage.go`, `meta_llm.go` | LLM cost accounting |
| `options.go` | Options, sub-options, configureOptions, Close lifecycle |
| `s3_encryption.go` | S3 encryption policy resolution |
| `runtime_metrics.go` | Global backend runtime metrics singleton |
| `errors.go` | Sentinels: ErrNotS3Stored, ErrS3NotConfigured, etc. |

## Conventions

- **Ctx/non-Ctx pairs**: Every FS op has `FooCtx(ctx, ...)` and `Foo(...)` (background ctx). Non-Ctx variants satisfy AGFS interface.
- **observeBackend timer/defer**: Every exported method starts with `start := time.Now(); defer func() { observeBackend(ctx, op, err, start) }()`.
- **Durable quota outbox**: Quota mutations go through `applyLoggedQuotaMutation()` -- insert the durable mutation log first, then use one transaction to apply the mutation and mark the log applied. Background `MutationReplayWorker` retries unapplied logs.
- **Fail-open quotas**: When server DB unreachable, operations proceed silently. Convergence via replay worker + reconciliation.
- **Interface injection via setter**: `MetaQuotaStore` set via `SetMetaQuotaStore()` after construction to avoid circular dep with `meta` package.
- **Pluggable extractors**: `ImageTextExtractor` and `AudioTextExtractor` are interface fields. Use `NewFallbackImageTextExtractor` for primary+fallback composition.
- **genID via ULID**: `b.genID` produces ULIDs. Pass as function arg to let datastore create IDs in its own transactions.
- **Only 1 `var _` assertion**: `CapabilityProvider`. Add one when implementing new interfaces.
- **Sentinels split**: Check both `errors.go` and `quota.go` for error sentinels.
- **No sub-packages**: Add new features as new files in this flat package.

## Quota architecture

Two sources: tenant-DB (legacy) vs server-DB (Rev 4). Switchable via `QuotaSource`.
Per-operation checks dispatch by `QuotaSource`: `tenant` uses tenant-DB quota checks; `server` uses central quota/reservations and skips tenant-DB upload checks after the server-reserve-first saga. Small server-quota writes use `ensureStorageQuotaServer`; mutation logs converge central counters.
Upload reservations use a server-first saga: reserve -> upload -> complete/abort on server.

## Key types

`Dat9Backend`, `Options`, `MetaQuotaStore` (60-method interface), `ImageTextExtractor`, `AudioTextExtractor`, `ReadPlan`, `UploadPlan`, `UploadPlanV2`, `PatchPlan`, `AppendPlan`, `FileGCWorker`, `MutationReplayWorker`, `ExpirySweepWorker`, `QuotaSource`, `ImageExtractUsage`, `AudioExtractUsage`
