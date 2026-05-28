---
title: datastore — TiDB/MySQL metadata store for drive9
---

Core metadata persistence layer backed by TiDB/MySQL. Inode-style path tree, file metadata, tags, uploads, semantic tasks, full-text and vector indexes. ~23 files (~9K lines). Central type: `Store`.

## File map

| File | Responsibility |
|---|---|
| `store.go` (2826 lines) | Store struct, all CRUD, 10 sentinel errors |
| `journal.go` | Append-only log with hash-chain verification |
| `quota.go` | Tenant-DB quota operations |
| `semantic.go` | Semantic task persistence (lease, claim, complete) |
| `search.go` | Full-text search and vector search queries |
| `upload.go` | Upload session persistence |
| `node.go` | File node (inode) operations |
| `file.go` | File entity operations |
| `tag.go` | Tag CRUD |
| `transaction.go` | Transaction helpers |
| `schema.go` | Schema management |

## Sentinel errors (10)

All in `store.go`: `ErrNotFound`, `ErrPathConflict`, `ErrUploadNotActive`, `ErrUploadAlreadyComplete`, `ErrUploadCommitVersionStale`, `ErrUploadPartIndexConflict`, `ErrUploadRevisionMismatch`, `ErrFileVersionConflict`, `ErrSemanticTaskNotFound`, `ErrSemanticTaskLeaseMismatch`.

Check with `errors.Is(err, datastore.ErrNotFound)` — never compare error strings.

## Key types

`Store`, `InTx(func(tx *sql.Tx) error) error`, `FileNode`, `FileEntity`, `FileTag`, `UploadSession`, `SemanticTask`, `StorageType`, `NodeType`, `StatusType`, `JournalEntry`, `JournalMetadata`.

## Typed string constants

- `StorageType` = `"db9"` | `"s3"`
- `NodeType` = `"file"` | `"dir"` | `"symlink"`
- `StatusType` = `"active"` | `"deleted"`

## Schema

Tables: `file_nodes`, `files`, `file_tags`, `uploads`, `semantic_tasks`, `file_gc_tasks`, `inodes`, `contents`, `semantic`. Schema source of truth is `pkg/tenant/schema/tidb_auto.go`. Auto-embedding tenants use generated vector columns from `content_text`.

## Conventions

- **Transaction primitive**: `InTx(func(tx *sql.Tx) error) error` — use for all multi-statement operations.
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
