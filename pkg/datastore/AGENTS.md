---
title: datastore — TiDB/MySQL metadata store for drive9
---

Core metadata persistence layer backed by TiDB/MySQL. Inode-style path tree, file metadata, tags, uploads, semantic tasks, full-text and vector indexes. ~23 files (~9K lines). Central type: `Store`.

## File map

| File | Responsibility |
|---|---|
| `store.go` (2826 lines) | Store struct, core file/upload CRUD, transaction helper, sentinels |
| `file_tx.go` | Transactional file-node/file-inode helpers |
| `inode.go` | Inode table operations |
| `content.go` | Small-file content persistence |
| `journal.go` | Append-only log with hash-chain verification |
| `quota.go` | Tenant-DB quota operations |
| `semantic.go` | Semantic metadata rows |
| `semantic_tasks.go` | Semantic task persistence (lease, claim, complete) |
| `search.go` | Full-text search and vector search queries |
| `file_gc_tasks.go` | Durable file-GC task persistence |
| `embedding_writeback.go` | Embedding revision writeback helpers |
| `storage_ref_hash.go` | Storage-ref hash helper |
| `llm_usage.go` | LLM usage persistence |

## Sentinel errors (12)

All in `store.go`: `ErrNotFound`, `ErrUploadNotActive`, `ErrUploadExpired`, `ErrPathConflict`, `ErrUploadConflict`, `ErrIdempotencyConflict`, `ErrJournalConflict`, `ErrJournalClosed`, `ErrJournalValidation`, `ErrJournalPayloadTooLarge`, `ErrRevisionConflict`, `ErrFileGCTaskLeaseMismatch`.

Check with `errors.Is(err, datastore.ErrNotFound)` — never compare error strings.

## Key types

`Store`, `InTx(ctx context.Context, func(tx *sql.Tx) error) error`, `FileNode`, `File`, `NodeWithFile`, `Upload`, `Semantic`, `SemanticTaskObservation`, `JournalWriter`, `StorageType`, `StorageEncryptionMode`, `FileStatus`, `UploadStatus`.

## Typed string constants

- `StorageType` = `"db9"` | `"s3"`
- `StorageEncryptionMode` = `"legacy"` | `"none"` | `"sse-s3"` | `"sse-kms"` | `"dsse-kms"`
- `FileStatus` = `"PENDING"` | `"CONFIRMED"` | `"DELETED"`
- `UploadStatus` = `"INITIATED"` | `"UPLOADING"` | `"COMPLETED"` | `"ABORTED"` | `"EXPIRED"`

## Schema

Tables include `file_nodes`, `files`, `uploads`, `semantic_tasks`, `file_gc_tasks`, `inodes`, `contents`, `semantic`, journal tables, and usage/quota tables. Tenant init schema sources are `pkg/tenant/schema/tidb_auto.go`, `pkg/tenant/schema/tidb_app.go`, and `pkg/tenant/db9/schema.go`; defer to the root schema synchronization section and exported `drive9-server schema dump-init-sql` commands when changing schema shape.

## Conventions

- **Transaction primitive**: `InTx(ctx context.Context, func(tx *sql.Tx) error) error` — use for all multi-statement operations.
- **Context**: All DB calls accept `context.Context` as first parameter. Never store in structs.
- **Absent fields**: Use pointer types (`*int64`, `*time.Time`).
- **Constructors**: Prefer `*T` return. Embed only for strong behavioral reasons.
- **IDs**: ULID-based (file, upload, task IDs).
- **MySQL driver**: `go-sql-driver/mysql` with `parseTime=true`. Instrumented via `pkg/mysqlutil`.

## ResetDB

`testmysql.ResetDB(t, db)` deletes in dependency order: `file_gc_tasks`, `semantic_tasks`, `file_nodes`, `file_tags`, `uploads`, `files`, `inodes`, `contents`, `semantic`.

## Anti-patterns

- Do not compare error strings — use `errors.Is()`.
- Do not `panic` in store operations — return errors.
- Do not perform multi-step operations outside `InTx`.
- Do not hardcode DSNs — use `DRIVE9_TEST_MYSQL_DSN` or constructor injection.
