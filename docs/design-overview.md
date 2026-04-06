# drive9: Agent-Native Data Infrastructure

**Status**: Proposal (Draft v2)
**Date**: 2026-03-26
**License**: Apache 2.0

---

## 1. Overview

drive9 is a unified data infrastructure for AI agents — a **network drive with built-in semantic search**. It presents a single filesystem-like interface for storing, retrieving, and querying data of any kind, while the underlying complexity (tiered storage, embedding, full-text indexing) is invisible to the user.

An agent (or a human) interacts with drive9 the same way they interact with a local filesystem:

```bash
drive9 cp ./dataset.tar /data/dataset.tar        # upload (auto: presigned URL for large files)
drive9 cat /config/settings.json                  # read
drive9 ls /data/                                  # list
drive9 cp /data/a.bin /shared/a.bin               # zero-copy link (no re-upload)
drive9 mv /data/old.bin /data/new.bin             # rename (zero storage cost)
drive9 search "training data for image classification"  # semantic search
drive9 sh                                         # interactive shell
```

```python
client = Drive9("https://drive9.example.com", api_key="...")
client.write("/data/file.bin", open("local.bin", "rb"))
client.read("/data/file.bin")
client.search("training data")
```

### Core Insight: Build on db9, Not Around It

Each drive9 tenant is backed by a [db9](https://db9.ai/) database. db9 already provides:

- **fs9**: File storage in TiKV (16KB pages, up to 100MB per file)
- **EMBED_TEXT()**: Auto-embedding as `GENERATED ALWAYS AS` columns
- **VECTOR + HNSW**: Vector similarity search
- **tsvector + GIN**: Full-text search with Chinese tokenizer (jieba)
- **CHUNK_TEXT()**: Markdown-aware document chunking
- **Hybrid search**: FTS filter + vector ranking in one SQL query

drive9 adds what db9 doesn't have: **large-file S3 direct upload**, **path-tree namespace (inode model)**, **tiered context (L0/L1/L2)**, **cross-tenant sharing**, and an **AGFS-compatible filesystem interface**.

### Problem Statement

- Agent tool fragmentation: each agent tool uses its own storage semantics and credentials.
- Server bandwidth bottlenecks for large files: proxying large uploads is slow and expensive.
- Missing semantic discoverability: files exist, but cannot be found by meaning.
- No unified path-based abstraction across storage tiers (db9 small files, S3 large files, memory scratch).

### Non-goals

- POSIX-complete semantics in P0.
- Transactional multi-file updates.
- Data warehouse replacement.
- Reimplementing embedding/FTS/vector search (db9 already has these).
- Retrieval algorithms (intent analysis, reranking) — that's the upper-layer agent framework's job.

### Design Principles

1. **Users see only file operations** --- `cp`, `cat`, `ls`, `search`. All protocol complexity is hidden.
2. **Leverage db9 native capabilities** --- embedding, FTS, vector search, chunking are db9 built-in. drive9 orchestrates, not reimplements.
3. **Tiered storage** --- Small files (< 50,000 bytes) in db9 (zero network overhead, instant search). Large files (>= 50,000 bytes) in S3 (presigned URL direct upload). One path namespace spanning both.
4. **inode model** --- Paths and file entities are separate. One file can appear at multiple paths (zero-copy `cp`). `mv` is a metadata-only operation.
5. **Import, don't fork** --- Built on [AGFS](https://github.com/c4pt0r/agfs)'s `FileSystem` interface and `MountableFS` routing layer.
6. **Tiered context loading** --- Inspired by [OpenViking](https://github.com/volcengine/OpenViking)'s L0/L1/L2 model. Every directory can carry a ~100-token abstract (L0) and a ~1k-token overview (L1), enabling agents to scan cheaply before loading full content (L2).

---

## 2. Architecture

### Storage Tiering

```
┌──────────────────────────────────────────────────────────────────────┐
│                          drive9 Server (Go)                             │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │                     MountableFS (AGFS)                           │  │
│  │                radix-tree path → backend routing                 │  │
│  │                                                                  │  │
│  │   /          → Drive9Backend                                       │  │
│  │   /mem/      → memfs (in-memory scratch)                         │  │
│  └──────────┬──────────────────────────────────────────────────────┘  │
│             │                                                         │
│  ┌──────────▼──────────────────────────────────────────────────────┐  │
│  │                       Drive9Backend                                │  │
│  │              (implements AGFS FileSystem)                         │  │
│  │                                                                  │  │
│  │  Write path:                                                     │  │
│  │    < 50,000B → db9 fs9_write()      (instant, auto-embedding)     │  │
│  │    >= 50,000B → S3 presigned URL   (direct upload, never proxied)│  │
│  │                                                                  │  │
│  │  Read path:                                                      │  │
│  │    db9 file → fs9_read()           (~1ms)                        │  │
│  │    S3 file  → presigned URL 302    (~50ms)                       │  │
│  │                                                                  │  │
│  │  Search:                                                         │  │
│  │    semantic  → db9 HNSW vector     (auto-embedded content)       │  │
│  │    keyword   → db9 GIN FTS         (auto-indexed content)        │  │
│  │    hybrid    → FTS WHERE + vector ORDER BY                       │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  ┌──────────┐  ┌──────────┐                                          │
│  │ 租户 db9 │  │    S3    │                                          │
│  │          │  │          │                                          │
│  │ 4 tables │  │ blobs/   │                                          │
│  │ + fs9    │  │ <ulid>   │                                          │
│  │ + embed  │  │          │                                          │
│  │ + FTS    │  │ 大文件    │                                          │
│  │ + vector │  │ >= 50,000B│                                          │
│  └──────────┘  └──────────┘                                          │
│                                                                       │
│  ┌───────────────────────────────────────────────────────────────┐    │
│  │  Background Workers                                           │    │
│  │  - SemanticProcessor: L2 file → LLM → L0/L1 generation       │    │
│  │  - Reaper: cleanup expired / orphaned / aborted uploads       │    │
│  └───────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
```

### Why Two Storage Tiers?

| Concern | db9 (< 50,000B) | S3 (>= 50,000B) |
|---------|-------------|-------------|
| **Latency** | ~1ms (TiKV local read) | ~50ms (HTTP round-trip) |
| **Max size** | 100MB (db9 limit) | Unlimited |
| **Auto-embedding** | `GENERATED ALWAYS AS (EMBED_TEXT(...))` — free | Not possible |
| **FTS** | `GENERATED ALWAYS AS (to_tsvector(...))` — free | Not possible |
| **Semantic search** | Native HNSW + GIN | Only via L0 abstract (small file in db9) |
| **Cost** | db9 compute + TiKV storage | S3 storage (cheap) + transfer |

Small files benefit from db9's native embedding/FTS. Large files are too big to embed entirely — they participate in search only through their L0 abstracts (which are small files stored in db9).

### Relationship with AGFS

drive9's server imports AGFS as a Go module dependency (Apache 2.0).

| AGFS Package | What We Use |
|---|---|
| `pkg/filesystem` | `FileSystem` interface (Create, Read, Write, ReadDir, Stat, Rename, ...), `Capabilities` system, `WriteFlag`/`OpenFlag` types, `StreamReader`/`Toucher`/`Symlinker` extension interfaces |
| `pkg/mountablefs` | `MountableFS` radix-tree path router --- dispatches `/path` to the correct backend plugin via longest-prefix match |
| `pkg/plugin` | `ServicePlugin` interface (Name, Validate, Initialize, GetFileSystem, GetReadme, GetConfigParams, Shutdown) |
| `pkg/plugins/memfs` | In-memory filesystem plugin used for `/mem` scratch mount |

```go
import (
    "github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
    "github.com/c4pt0r/agfs/agfs-server/pkg/mountablefs"
    "github.com/c4pt0r/agfs/agfs-server/pkg/plugin"
)

// Drive9Backend implements AGFS's FileSystem interface
type Drive9Backend struct {
    db9  *db9.Client    // tenant db9 (metadata + small file storage)
    s3   S3Client       // large file storage
}

func (b *Drive9Backend) Read(path string, offset, size int64) ([]byte, error) { ... }
func (b *Drive9Backend) Write(path string, data []byte, offset int64, flags filesystem.WriteFlag) (int64, error) { ... }
func (b *Drive9Backend) ReadDir(path string) ([]filesystem.FileInfo, error) { ... }
func (b *Drive9Backend) Stat(path string) (*filesystem.FileInfo, error) { ... }

// Capability detection via type assertion (AGFS pattern)
if cp, ok := backend.(filesystem.CapabilityProvider); ok {
    caps := cp.GetCapabilities()
    if caps.IsObjectStore { /* use presigned URL path */ }
}

// Mount it
mfs := mountablefs.NewMountableFS(api.DefaultPoolConfig())
mfs.Mount("/", &Drive9Plugin{backend: drive9backend})
mfs.Mount("/mem", memfsPlugin)
```

### Client Layer

```
    ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐
    │   CLI    │  │   SDK    │  │   MCP    │  │ FUSE (later) │
    │ drive9 cp  │  │ Go/Py   │  │ Tools    │  │              │
    │ drive9 sh  │  │          │  │          │  │              │
    └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────┬───────┘
         └─────────────┼─────────────┘               │
                       │  (all go through SDK)        │
    ┌──────────────────▼──────────────────────────────▼──────┐
    │  Transfer Engine (inside SDK)                          │
    │                                                        │
    │  Small file: PUT body → server → db9 fs9_write         │
    │  Large file: PUT → 202 + presigned URLs → direct to S3 │
    │  Resume: GET /v1/uploads?path=... → re-issue URLs      │
    └───────────────────────────────────────────────────────┘
```

---

## 3. inode Model: Paths and Files

### The Key Separation

drive9 uses an **inode model** inspired by Unix: paths (directory entries) and file entities (inodes) are separate concerns.

```
                    file_nodes (dentry)              files (inode)
                    ─────────────────               ──────────────
path                  file_id (FK)                  file_id (PK)
parent_path           ────────────▶                 storage_type: db9 | s3
name                                                storage_ref
is_directory                                        size_bytes
                                                    content_type
                                                    revision
                                                    ...
```

**One file entity can appear at multiple paths** (N:1 mapping):

```
file_nodes                                    files
─────────────────────────────                 ──────────────────────
/data/training-v3/images.tar  ──┐             file_id: 01JQ...
/shared/team-a/images.tar     ──┼──▶          storage_type: s3
/backup/2026/images.tar       ──┘             storage_ref: blobs/01JQ...
                                              size_bytes: 10737418240
(3 paths, 1 file, 1 S3 object)
```

### Why inode Model?

| Operation | Without inode (path = file) | With inode (file_nodes + files) |
|-----------|---------------------------|-------------------------------|
| `cp /a /b` | Copy S3 object ($$$, slow for 10GB) | `INSERT file_nodes` (instant, zero storage cost) |
| `mv /a /b` | Copy S3 + delete old ($$$) | `UPDATE file_nodes SET path=...` (zero storage cost; O(1) for files, O(N) prefix rewrite for dirs) |
| `rm /a` (has other links) | Complex reference tracking | `DELETE file_nodes WHERE path='/a'` (file survives) |
| `rm /a` (last link) | Delete S3 object | Delete file_nodes → refcount=0 → Reaper deletes file+blob |
| `stat --nlink` | Not possible | `SELECT COUNT(*) FROM file_nodes WHERE file_id=?` |

This is the same model as Unix (dentry + inode), Plan 9 (name space + file server), and OpenViking (URI tree + resource storage).

### S3 Key Strategy

Large files are stored at content-addressed S3 keys: `blobs/<ulid>`. The path-to-blob mapping lives only in the database.

```
S3 Bucket
  blobs/
    01JQ7R8K3M0000000000000001     ← /data/training-v3/images.tar.gz
    01JQ7R8K3M0000000000000002     ← /config/model.bin
```

Small files are stored in db9 via `fs9_write('/blobs/<ulid>', content)`. Same ULID scheme, different storage backend.

### Operation Mapping

| drive9 operation | What happens |
|---|---|
| `drive9 ls /data/` | `SELECT name, is_directory, f.size_bytes FROM file_nodes fn LEFT JOIN files f ON fn.file_id = f.file_id WHERE fn.parent_path = '/data/'` |
| `drive9 cat /data/a.txt` | `file_nodes → file_id → files.storage_type` → if db9: `fs9_read(storage_ref)` / if s3: `S3.GetObject(storage_ref)` |
| `drive9 cp /a /b` (file) | `INSERT file_nodes(path='/b', file_id=same)` — zero-copy link, no storage copy |
| `drive9 cp /a/ /b/` (dir) | Recursive: for each descendant of `/a/`, `INSERT file_nodes` with same `file_id` and rewritten path prefix. Zero storage cost, O(N) metadata INSERTs. |
| `drive9 mv /a /b` (file) | `UPDATE file_nodes SET path='/b', parent_path=..., name=... WHERE path='/a'` — O(1) |
| `drive9 mv /a/ /b/` (dir) | Batch prefix rewrite: UPDATE the directory node + all descendants' `path` and `parent_path`. O(N) metadata UPDATEs, zero storage cost. |
| `drive9 rm /a` | `DELETE file_nodes WHERE path='/a'` → if refcount=0: mark file DELETED |
| `drive9 rm -r /a/` | Recursive: `DELETE FROM file_nodes WHERE path = '/a/' OR path LIKE '/a/%'` → per-file refcount check → mark orphans DELETED |
| `drive9 stat /a` | `SELECT fn.*, f.* FROM file_nodes fn JOIN files f ON ... WHERE fn.path='/a'` |
| `drive9 search "query"` | `SELECT ... FROM files f JOIN file_nodes fn ON ... ORDER BY vec_embed_cosine_distance(f.vec, 'query') LIMIT k` |

**Note on directory operations**: Directory `mv` and `cp` are O(N) in the number of descendants — but zero storage cost. This matches AGFS's philosophy: keep the filesystem interface simple, let the server handle batch metadata. Plan 9's `rename(2)` has the same property. For P0, batch operations run in a single transaction; sharded optimization is a future concern.

---

## 4. Two Data Paths

### Small Files (< 50,000 bytes): Server Proxy → db9

```
Client ──PUT body──▶ drive9 server
                        │
                 fs9_write('/blobs/<ulid>', body)
                 INSERT files (storage_type='db9', ...)
                 INSERT file_nodes (path=..., file_id=...)
                 ← db9 auto-computes: vec (EMBED_TEXT), tsv (to_tsvector)
                        │
Client ◀── 200 OK ──────┘
```

The server reads the request body, writes to db9, creates metadata, and returns. Simple, synchronous. **Embedding and FTS indexing happen automatically** via db9's `GENERATED ALWAYS AS` columns — no async pipeline needed for small files.

### Large Files (>= 50,000 bytes): Presigned URL Direct Upload → S3

```
Client ──PUT (Content-Length only, no body)──▶ drive9 server
                                                  │
                                           INSERT files (PENDING)
                                           INSERT uploads
                                           CreateMultipartUpload
                                           PresignUploadPart x N
                                                  │
Client ◀── 202 { parts: [{url, size}, ...] } ─────┘

Client ──PUT part 1──▶ S3 (direct, server not involved)
Client ──PUT part 2──▶ S3
  ...
Client ──PUT part N──▶ S3

Client ──POST /v1/uploads/{id}/complete──▶ drive9 server
                                              │
                                       CompleteMultipartUpload (S3)
                                       BEGIN;
                                         UPDATE files → CONFIRMED
                                         INSERT file_nodes (path=target_path)
                                         Auto-create parent dirs
                                         UPDATE uploads → COMPLETED
                                       COMMIT;
                                              │
Client ◀── 200 { confirmed } ────────────────┘
```

The server never touches large file data. Large files have `vec=NULL` and `tsv=NULL` — they don't participate in search directly. They participate through their L0 abstracts (see §5).

**Capability-aware write handler**: the server checks `backend.(filesystem.CapabilityProvider).GetCapabilities().IsObjectStore` via type assertion. If true and size >= 50,000 bytes threshold, return 202 with presigned URLs.

### Resumable Uploads

The SDK is **stateless**. On interruption, the SDK queries the server:

```
GET /v1/uploads?path=/data/big.bin&status=UPLOADING
```

The server calls `S3.ListParts()` to determine which parts were already uploaded, then re-issues presigned URLs for the remaining parts.

---

## 5. Tiered Context: L0 / L1 / L2

drive9 adopts a three-layer content model inspired by [OpenViking](https://github.com/volcengine/OpenViking)'s L0/L1/L2 tiered context architecture. The core insight: agents rarely need the full content of a file. They need just enough context to decide whether to load more.

### 5.1 The Model

Every directory in drive9 can optionally carry three layers of progressively detailed content:

| Layer | File | Token Budget | Purpose | Storage |
|-------|------|-------------|---------|---------|
| **L0** | `.abstract.md` | ~100 tokens (~400B) | Ultra-short summary. Vector search, quick filtering. | db9 (small file, auto-embedded) |
| **L1** | `.overview.md` | ~1-2k tokens (~4KB) | Structured overview with navigation pointers. | db9 (small file, auto-embedded) |
| **L2** | Original files | Unlimited | Full content. Loaded only when the agent confirms it needs the detail. | db9 (< 50,000B) or S3 (>= 50,000B) |

Example directory:

```
/data/training-v3/
  .abstract.md          # L0: "ImageNet-subset training data, 50k images, labeled, v3."
  .overview.md          # L1: structured summary + navigation pointers
  .relations.json       # Cross-resource links (advisory)
  metadata.json         # L2: full metadata (small, in db9)
  images.tar.gz         # L2: full data (10 GB, in S3)
```

**Key**: L0 and L1 are **ordinary small files** stored in db9 via the same Drive9Backend. They are file_nodes entries pointing to files entries. "Everything is a file" — no special tables, no caching layer. Because they're in db9, they are automatically embedded and FTS-indexed.

Token savings: scanning 20 directories via L0 costs ~2k tokens. Loading 3 L1 overviews costs ~3k. Loading 1 full L2 costs ~5k. Total: **10k tokens instead of 100k** (10x reduction).

### 5.2 Why No context_layers Table?

In the previous design, a `context_layers` table cached L0/L1 content to avoid S3 round-trips. With the new tiered storage:

- L0/L1 files are already in db9 (~1ms read latency)
- Their content is in `files.content_text` with auto-embedding
- Batch scan: `SELECT fn.path, f.content_text FROM file_nodes fn JOIN files f ON ... WHERE fn.parent_path = '/data/' AND fn.name = '.abstract.md'`

No caching table needed. db9 **is** the cache.

### 5.3 Search: Everything Goes Through db9

```
Semantic search (vector):
  SELECT fn.path, f.content_text, fn.parent_path
  FROM files f
  JOIN file_nodes fn ON fn.file_id = f.file_id
  WHERE f.vec IS NOT NULL AND f.status = 'CONFIRMED'
  ORDER BY vec_embed_cosine_distance(f.vec, 'training data for image classification')
  LIMIT 10;

  → Returns: .abstract.md files (L0 of directories) + small L2 files
  → Agent reads L1 of top results, then loads specific L2 files

Full-text search (keyword):
  SELECT fn.path, ts_headline('jieba', f.content_text, q) AS snippet
  FROM files f
  JOIN file_nodes fn ON fn.file_id = f.file_id,
       plainto_tsquery('jieba', '训练数据') q
  WHERE f.tsv @@ q AND f.status = 'CONFIRMED'
  ORDER BY ts_rank(f.tsv, q) DESC
  LIMIT 10;

Hybrid search (FTS filter + vector ranking):
  SELECT fn.path, f.content_text
  FROM files f
  JOIN file_nodes fn ON fn.file_id = f.file_id
  WHERE f.tsv @@ plainto_tsquery('jieba', 'training')
    AND f.status = 'CONFIRMED'
  ORDER BY vec_embed_cosine_distance(f.vec, 'image classification training data')
  LIMIT 10;
```

Large files (S3) don't have `vec` or `tsv`. They participate in search **indirectly** through their directory's `.abstract.md`:

```
Agent: "find training data for image classification"
  → Vector search hits /data/training-v3/.abstract.md (L0, in db9)
  → Agent reads /data/training-v3/.overview.md (L1, in db9)
  → Agent decides to download /data/training-v3/images.tar.gz (L2, in S3)
```

### 5.4 Hierarchical Retrieval

The filesystem directory structure itself becomes the navigation hierarchy:

```
Agent: "find training data for image classification"

Step 1: Vector search over all files with embeddings
  → Query embedding vs all file vectors (L0 abstracts + small L2 files)
  → Returns candidate paths: [/data/training-v3/.abstract.md, /data/imagenet/.abstract.md]

Step 2: Agent reads L1 of top candidates
  → drive9 cat /data/training-v3/.overview.md
  → ~1k tokens, structured: "50k images, labeled, classes: dog/cat/bird..."
  → Agent decides: this is the one.

Step 3: Agent loads specific L2 files
  → drive9 cat /data/training-v3/metadata.json  (small, from db9)
  → drive9 cat /data/training-v3/images.tar.gz  (large, 302 → S3 presigned URL)
```

### 5.5 Cross-Resource Relations (.relations.json)

Each directory can optionally carry a `.relations.json` sidecar file:

```json
{
  "relations": [
    { "target": "/data/imagenet/", "type": "derived_from", "description": "Training subset extracted from ImageNet" },
    { "target": "/experiments/resnet-v2/", "type": "used_by", "description": "Used as training input" }
  ]
}
```

- `.relations.json` is a regular small file stored in db9. "Everything is a file."
- Relations are **advisory**, not enforced. Deleting a target does not cascade.
- P0: users write `.relations.json` manually. Future: auto-generated by SemanticProcessor.

### 5.6 Async Processing Pipeline

For auto-generating L0/L1 from L2 files. This is a **P8+ feature** — not needed for P0.

```
File Write (synchronous)              Background Workers (asynchronous)
─────────────────────                 ──────────────────────────────────

drive9 cp file.md /docs/               SemanticProcessor (picks from queue):
  │                                     │
  ├─▶ store content (db9 or S3)        ├─▶ read file content
  ├─▶ INSERT files + file_nodes        ├─▶ LLM: generate L0 (.abstract.md)
  ├─▶ ENQUEUE(semantic_queue,          ├─▶ LLM: generate L1 (.overview.md)
  │     {path, action: "created"})     ├─▶ drive9 write .abstract.md (→ db9, auto-embedded)
  │                                     ├─▶ drive9 write .overview.md (→ db9, auto-embedded)
  └─▶ 200 OK  (immediate)             └─▶ Propagate: re-generate parent L0 (bottom-up)
```

### 5.7 Scope and Boundaries

```
┌─────────────────────────────────────────────────────────┐
│  Upper Layer (Agent Framework / Application)             │
│  - Intent analysis, query planning, reranking            │
│  - Context assembly, conversation memory                 │
└──────────────────────────┬──────────────────────────────┘
                           │  calls drive9 API
┌──────────────────────────▼──────────────────────────────┐
│  drive9 (This System)                                      │
│  - File CRUD: cp, cat, ls, mv, stat, rm                  │
│  - Semantic search: vector, FTS, hybrid (via db9)         │
│  - Tags, queries, revisions                               │
│  - Large-file upload: presigned URLs, multipart, resume   │
│  - L0/L1/L2 tiered context                                │
│  - Sharing: snapshot export/import                        │
└──────────────────────────┬──────────────────────────────┘
                           │  uses
┌──────────────────────────▼──────────────────────────────┐
│  db9 (Tenant Database)                                   │
│  - fs9: small file storage                               │
│  - EMBED_TEXT + HNSW: auto-embedding + vector search     │
│  - to_tsvector + GIN: full-text search                   │
│  - SQL: metadata queries                                 │
└─────────────────────────────────────────────────────────┘
```

---

## 6. API Design

### Unified FS Endpoint

All file operations go through `/v1/fs/{path}`. The server auto-routes based on file size and operation.

```
PUT    /v1/fs/{path}          Write (200 for small, 202 for large)
GET    /v1/fs/{path}          Read  (200 for small, 302 redirect for large)
DELETE /v1/fs/{path}          Delete
HEAD   /v1/fs/{path}          Stat  (standard HTTP semantics)
GET    /v1/fs/{path}?list     List directory

POST   /v1/fs/{path}?copy     Server-side link (zero-copy, same file_id)
  Header: X-Drive9-Copy-Source: /source/path

POST   /v1/search             Semantic search (vector + FTS + hybrid)
POST   /v1/query              Metadata query (tags, status, source_id)
```

### Search API

```
POST /v1/search
{
  "query": "training data for image classification",
  "mode": "vector",                    // "vector" | "fts" | "hybrid"
  "scope": "/data/",                   // optional: scope to subtree
  "tags": {"env": "prod"},             // optional: tag filter
  "top_k": 10
}
→ [{ "path": "/data/training-v3/.abstract.md", "score": 0.92, "content": "..." }, ...]
```

### API Error Model

- 200: success
- 202: large file upload initiated
- 302: redirect to presigned download URL
- 400: bad request
- 404: not found
- 409: conflict (upload already exists for different file)
- 412: precondition failed (If-Match revision mismatch)

### Upload Management (SDK-internal)

```
GET    /v1/uploads?path=...&status=UPLOADING   Query incomplete uploads
POST   /v1/uploads/{id}/resume                  Resume: get missing parts
POST   /v1/uploads/{id}/complete                Finalize upload
DELETE /v1/uploads/{id}                         Cancel upload
```

### Query API

```
POST /v1/query
{
  "filter": {
    "source_id": "agent-007",
    "tags": {"env": "prod"},
    "created_after": "2026-03-24T00:00:00Z",
    "status": "CONFIRMED"
  },
  "order_by": "created_at",
  "cursor": "...",
  "limit": 100
}
```

### Concurrency Control

- Default: **Last Writer Wins** (LWW)
- Optional: `If-Match: <revision>` on PUT for optimistic locking. Mismatch returns `412 Precondition Failed`.
- `revision` is a server-managed, auto-incrementing BIGINT stored in `files.revision`.
- Write to a path auto-creates parent directories (mkdir -p semantics).

### Overwrite Semantics (Write to Existing Path)

When a client writes to a path that already exists, drive9 uses **in-place update** on the existing `files` row:

```
PUT /v1/fs/data/config.json  (path already exists, file_id = 01JQ...)

  1. Resolve file_nodes.path → file_id
  2. UPDATE files SET storage_ref=?, size_bytes=?, content_text=?,
     storage_type=?, revision=revision+1
     WHERE file_id = ? [AND revision = ? if If-Match supplied]
  3. Write new blob; async-delete old blob (see §8 Write Path)
```

**Cross-tier overwrite**: If a small file grows past the threshold, `storage_type` flips from `db9` to `s3`; `content_text` is set to NULL (db9 auto-clears `vec`/`tsv`). If a large file shrinks, it flips back to `db9` and gets auto-embedded. The old storage is cleaned up asynchronously.

**Why in-place, not COW (copy-on-write)?** If `/a` and `/b` both point to the same file_id (zero-copy links), updating `/a`'s content should be visible at `/b` — they are the same file, just as with Unix hard links. This is consistent, unsurprising, and matches the inode model. If the caller wants independent copies, they should `cat /a > /tmp/a && cp /tmp/a /b` (read + write-new) instead of `cp /a /b` (link).

**Atomic conditional update**:

```sql
UPDATE files
SET    revision = revision + 1,
       storage_ref = ?,
       size_bytes = ?,
       checksum_sha256 = ?,
       confirmed_at = NOW(3)
WHERE  file_id = ?
  AND  revision = ?;          -- client-supplied from If-Match header

-- affected_rows = 0  →  return 412 Precondition Failed
-- affected_rows = 1  →  success, return new revision in ETag
```

---

## 7. Metadata Schema

All metadata lives in the tenant's db9 database. **Four tables**:

```
┌────────────────┐      ┌──────────────┐      ┌─────────────┐      ┌──────────────┐
│  file_nodes    │ N:1  │    files     │      │  file_tags  │      │   uploads    │
│  (dentry)      │─────▶│   (inode)    │◀─────│             │      │              │
│                │      │              │      │ file_id+key │      │ upload_id    │
│ path    (UK)   │      │ file_id (PK) │      │ tag_value   │      │ file_id      │
│ parent_path    │      │ storage_type │      └─────────────┘      │ s3_upload_id │
│ name           │      │ storage_ref  │                           │ status       │
│ file_id  (FK)  │      │ size_bytes   │      Precise SQL          └──────────────┘
│ is_directory   │      │ vec (auto)   │      queries on tags
│                │      │ tsv (auto)   │                           Large-file S3
│ Path tree      │      │ content_text │                           multipart state
│ (ls, mv, cp)   │      │ revision     │
└────────────────┘      │ status       │
                        └──────────────┘
                        File entity +
                        auto search index
```

### file_nodes — path tree (dentry)

```sql
CREATE TABLE file_nodes (
    node_id       VARCHAR(26) PRIMARY KEY,    -- ULID
    path          VARCHAR(4096) NOT NULL,      -- canonical full path
    parent_path   VARCHAR(4096) NOT NULL,      -- parent directory path
    name          VARCHAR(255) NOT NULL,        -- basename
    is_directory  BOOLEAN NOT NULL DEFAULT FALSE,
    file_id       VARCHAR(26),                 -- → files.file_id, NULL for directories
    created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    UNIQUE KEY idx_path (path),
    INDEX idx_parent (parent_path),
    INDEX idx_file (file_id)
);
-- No namespace_id: each tenant has its own db9 cluster. Isolation is at the cluster level.
```

**Design notes**:

- `file_id` is NULL for directories (directories have no content).
- Multiple file_nodes can share the same `file_id` (N:1 = hard link / zero-copy cp).
- `parent_path` enables `ls` via `SELECT ... WHERE parent_path = ?`.
- `name` is denormalized from `path` for display (avoids string parsing in queries).
- `mv` on a file is a single UPDATE. `mv` on a directory is a batch prefix rewrite (O(N) descendants, zero storage cost).

### files — file entity (inode)

```sql
CREATE TABLE files (
    file_id         VARCHAR(26) PRIMARY KEY,    -- ULID
    storage_type    ENUM('db9', 's3') NOT NULL,
    storage_ref     VARCHAR(1024) NOT NULL,      -- db9: '/blobs/<ulid>'; s3: 'blobs/<ulid>'
    content_type    VARCHAR(127),
    size_bytes      BIGINT NOT NULL DEFAULT 0,
    checksum_sha256 CHAR(64),
    revision        BIGINT NOT NULL DEFAULT 1,
    status          ENUM('PENDING','CONFIRMED','DELETED') NOT NULL DEFAULT 'PENDING',
    source_id       VARCHAR(255),                -- provenance: "agent-007"

    -- semantic columns (auto-computed by db9, only for small text files)
    content_text    TEXT,                         -- file text content (NULL for binary/large)
    vec             VECTOR(1024) GENERATED ALWAYS AS (
                      EMBED_TEXT('amazon.titan-embed-text-v2:0', content_text, '{"dimensions": 1024}')
                    ) STORED,
    tsv             TSVECTOR GENERATED ALWAYS AS (
                      to_tsvector('jieba', COALESCE(content_text, ''))
                    ) STORED,

    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    confirmed_at    DATETIME(3),
    expires_at      DATETIME(3),

    INDEX idx_status (status, created_at),
    INDEX idx_expires (expires_at),
    INDEX idx_vec USING hnsw (vec vector_cosine_ops),
    INDEX idx_fts USING gin (tsv)
);
```

**Design notes**:

- `storage_type='db9'`: small file, `storage_ref='/blobs/01JQ...'` → `fs9_read(storage_ref)`.
- `storage_type='s3'`: large file, `storage_ref='blobs/01JQ...'` → `S3.GetObject(storage_ref)`.
- `content_text`: populated for small text files (including L0/L1). NULL for binary files and S3 files.
- `vec`: **auto-computed by db9** via `GENERATED ALWAYS AS (EMBED_TEXT(...))`. Writing `content_text` automatically generates the embedding. NULL when `content_text` is NULL.
- `tsv`: **auto-computed by db9** via `GENERATED ALWAYS AS (to_tsvector(...))`. Automatic FTS indexing with jieba Chinese tokenizer.
- HNSW index on `vec` enables fast ANN search. GIN index on `tsv` enables fast keyword search.
- `revision` supports optimistic concurrency (If-Match header).

### file_tags — tags

```sql
CREATE TABLE file_tags (
    file_id   VARCHAR(26) NOT NULL,
    tag_key   VARCHAR(255) NOT NULL,
    tag_value VARCHAR(1024) NOT NULL DEFAULT '',
    PRIMARY KEY (file_id, tag_key),
    INDEX idx_kv (tag_key, tag_value)
);
```

Separate table for proper SQL indexing. Supports precise filtering: `drive9 ls --tag env=prod`.

### uploads — large-file multipart upload state

```sql
CREATE TABLE uploads (
    upload_id          VARCHAR(26) PRIMARY KEY,
    file_id            VARCHAR(26) NOT NULL,
    target_path        VARCHAR(4096) NOT NULL,        -- destination path for resume lookup
    s3_upload_id       VARCHAR(255) NOT NULL,
    s3_key             VARCHAR(1024) NOT NULL,        -- blobs/<ulid>
    total_size         BIGINT NOT NULL,
    part_size          BIGINT NOT NULL,
    parts_total        INT NOT NULL,
    status             ENUM('UPLOADING','COMPLETED','ABORTED','EXPIRED') NOT NULL DEFAULT 'UPLOADING',
    fingerprint_sha256 CHAR(64),                      -- dedup/conflict detection
    idempotency_key    VARCHAR(255),                   -- client-provided
    created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    expires_at         DATETIME(3) NOT NULL,
    INDEX idx_path_status (target_path, status),      -- resume lookup: WHERE target_path=? AND status='UPLOADING'
    INDEX idx_status_expires (status, expires_at),
    UNIQUE KEY idx_idempotency (idempotency_key)      -- at most one upload per idempotency_key
);
```

### Design notes

**Why ULID for primary keys?** Time-ordered (efficient range scans) + random suffix (avoids write hotspots in distributed DBs).

**Why separate file_nodes and files?** inode model: one file entity, multiple path references. Enables zero-copy `cp`, zero-cost `mv`, and proper reference-counted `rm`.

**Why content_text on files instead of a separate table?** L0/L1 text is tiny (100-4000 tokens). With db9's `GENERATED ALWAYS AS`, embedding and FTS indexing are automatic. No async pipeline, no cache table, no eventual consistency — it's just a column.

---

## 8. Consistency Model

### Write Path

```
Small file (new path):
  BEGIN;
    1. INSERT files (storage_type='db9', content_text=content, status='CONFIRMED')
       ← db9 auto-computes vec + tsv on INSERT
    2. fs9_write('/blobs/<ulid>', content)             -- same TiKV txn context
    3. INSERT file_nodes (path=..., file_id=...)
    4. Auto-create parent directories (INSERT IGNORE for each ancestor)
  COMMIT;

Small file (overwrite existing path):
  BEGIN;
    1. SELECT file_id FROM file_nodes WHERE path = ? FOR UPDATE
    2. UPDATE files SET storage_ref=?, size_bytes=?, content_text=?,
       content_type=?, revision=revision+1 WHERE file_id = ?
       ← db9 auto-recomputes vec + tsv on UPDATE
    3. fs9_write('/blobs/<new-ulid>', content)
    4. old_ref = previous storage_ref
  COMMIT;
  5. Async: fs9_remove(old_ref) or S3.DeleteObject(old_ref) if tier changed

Large file:
  1. INSERT files (storage_type='s3', PENDING) + INSERT uploads (target_path=...)
  2. Client uploads parts directly to S3 via presigned URLs
  3. Client calls /complete → CompleteMultipartUpload (S3 side)
  4. BEGIN;
       UPDATE files SET status='CONFIRMED', confirmed_at=NOW(3)
       INSERT file_nodes (path=target_path, file_id=...)
       Auto-create parent directories (INSERT IGNORE)
       UPDATE uploads SET status='COMPLETED', updated_at=NOW(3)
     COMMIT;
     -- If file_nodes INSERT conflicts (path exists): ROLLBACK, return 409
```

**Atomicity**: db9's fs9 and SQL share the same TiKV transaction context — `fs9_write()` inside a `BEGIN/COMMIT` block is atomic with the metadata INSERTs. If the transaction rolls back, both metadata and blob are discarded.

**Cross-tier overwrite**: When overwriting a small file (db9) with a large file (S3) or vice versa, the server updates `storage_type` + `storage_ref` in the files row and cleans up the old storage asynchronously. The `content_text` column is set to NULL for S3 files (db9 auto-clears `vec` and `tsv`).

### State Machines

**files state machine**:

```
PENDING ──────────────────▶ CONFIRMED ──▶ (normal use)
    │  (upload complete         │
    │   + /complete called)     │ expires_at / explicit delete / refcount=0
    │                           ▼
    │                        DELETED ──▶ Reaper (db9 fs9 delete or S3 delete)
    │
    │ Reaper: storage check fails
    ▼
 (Reaper deletes metadata)
```

**uploads state machine**:

```
UPLOADING ──▶ COMPLETED
    │             │
    │ timeout     │ (triggers files: PENDING → CONFIRMED)
    ▼
 ABORTED ──▶ Reaper (S3.AbortMultipartUpload)
    │
    │ expires_at
    ▼
 EXPIRED ──▶ Reaper
```

**Cross-table invariants**:

| Invariant | Meaning |
|-----------|---------|
| `uploads.status = COMPLETED` ⟹ `files.status = CONFIRMED` ∧ `file_nodes` exists | Completed upload always has confirmed file **and** a path. Enforced by atomic `/complete` transaction. |
| `files.status = CONFIRMED` ⟹ storage has the complete object | Fundamental data integrity guarantee |
| `file_nodes.file_id` references existing `files.file_id` | Referential integrity |

### Delete Path

```
DELETE /v1/fs/{path}
  BEGIN;
    1. SELECT file_id FROM file_nodes WHERE path = ? FOR UPDATE
    2. DELETE FROM file_nodes WHERE path = ?
    3. SELECT f.file_id FROM files f WHERE f.file_id = ?
       FOR UPDATE                              -- lock the files row
    4. SELECT COUNT(*) FROM file_nodes WHERE file_id = ?
       → if refcount > 0: COMMIT (other paths still reference this file)
       → if refcount = 0:
          5. DELETE FROM file_tags WHERE file_id = ?
          6. UPDATE files SET status = 'DELETED'
  COMMIT;
  7. Async (outside txn): if db9: fs9 delete; if s3: S3.DeleteObject(storage_ref)
  → 200 OK
```

**Why `FOR UPDATE`?** Without it, two concurrent `rm` calls on different paths pointing to the same file can both see refcount=1, both decide "not last link", and leave an orphan file with refcount=0. The `FOR UPDATE` on the `files` row serializes concurrent deletions and prevents this TOCTOU race. Storage deletion is deferred to outside the transaction (and also handled by the Reaper) so the critical section stays short.

Reference-counted delete: the file entity is only removed when no paths reference it.

### Reaper (Background Cleanup)

Runs periodically to:
1. **Abort timed-out uploads**: `S3.AbortMultipartUpload` + update status.
2. **Delete TTL-expired files**: Remove storage objects + mark DELETED.
3. **Clean orphan files**: files with refcount=0 and status != DELETED.

---

## 9. Sharing

### V1: Snapshot Share (Recommended)

```bash
drive9 share create /knowledge/ml-papers/ --to agent-007 --mode snapshot
drive9 share accept sh_01J... --to /shared/ml-papers/
```

Snapshot mode performs point-in-time export/import across tenants:

1. Source tenant freezes a manifest (paths, checksums, sizes).
2. Small files (db9): content copied to target tenant's db9 → auto-embedded.
3. Large files (S3): S3 object-to-object copy to target tenant's S3 prefix.
4. Target tenant creates file_nodes + files in its own db9.

Because L0/L1 are ordinary files, they're included automatically.

### V2 (Future): Live Read-Only Mount

Read-only only. No recipient-side caching. Source tenant owns bytes.

---

## 10. Autoprovision & Control Plane

### One Tenant = One Agent = One db9 Cluster

Each agent gets its own db9 cluster. No registration — the first API call triggers autoprovision (same pattern as [mem9](https://mem9.ai)).

```
Agent (no key yet)
  │
  POST /v1/provision
  │
  drive9 control plane:
    1. Generate api_key: "drive9_" + 32 random bytes (base62)
    2. Call db9 API: create cluster → get connection string
    3. Connect to new cluster, run schema init (4 tables + indexes + extensions)
    4. Create S3 prefix: s3://<bucket>/tenants/<tenant_id>/
    5. INSERT INTO tenants (api_key_hash, db9_dsn, s3_prefix, ...)
    6. Return api_key to agent (only time it's shown in plaintext)
  │
  ◀── 200 { "api_key": "drive9_7kQ3x..." }

Subsequent requests:
  Authorization: Bearer drive9_7kQ3x...
    → SHA-256(key) prefix lookup in tenants table
    → Resolve db9 connection + S3 config
    → All operations scoped to this tenant's db9 cluster
```

### Control Plane Database

The control plane has its own database (separate from tenant db9 clusters). Can be a single db9 instance or PostgreSQL.

```sql
CREATE TABLE tenants (
    tenant_id       VARCHAR(26) PRIMARY KEY,    -- ULID
    api_key_prefix  CHAR(12) NOT NULL,           -- first 12 chars of api_key, for fast lookup
    api_key_hash    CHAR(64) NOT NULL,           -- SHA-256(api_key), for verification
    db9_dsn         TEXT NOT NULL,                -- db9 cluster connection string (encrypted at rest)
    s3_bucket       VARCHAR(63) NOT NULL,
    s3_prefix       VARCHAR(1024) NOT NULL,       -- tenants/<tenant_id>/
    status          ENUM('PROVISIONING','ACTIVE','SUSPENDED','DELETED') NOT NULL DEFAULT 'PROVISIONING',
    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    last_active_at  DATETIME(3),

    INDEX idx_prefix (api_key_prefix),
    INDEX idx_status (status)
);
```

**api_key security**:

- **Never stored in plaintext.** Only `SHA-256(api_key)` is stored. The prefix (first 12 chars) is stored separately for fast lookup.
- **Prefix is non-unique.** 12 chars base62 ≈ 3.2 × 10^21 combinations — collision is astronomically unlikely, but the index is non-unique and auth verifies the full SHA-256 hash. If multiple rows match a prefix, each is checked.
- **Format**: `drive9_` + 32 random bytes (base62). The `drive9_` prefix enables GitHub secret scanning and similar leak-detection tools.
- **Transport**: HTTPS only. drive9 server rejects plain HTTP requests.

### Schema Init

When a new db9 cluster is provisioned, drive9 runs:

```sql
-- Extensions
CREATE EXTENSION IF NOT EXISTS embedding;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS fs9;

-- Tables (see §7 for full DDL)
CREATE TABLE file_nodes (...);
CREATE TABLE files (...);
CREATE TABLE file_tags (...);
CREATE TABLE uploads (...);
```

### Auth Flow (per request)

```
1. Extract api_key from Authorization header
2. Compute prefix = api_key[:12], hash = SHA-256(api_key)
3. SELECT * FROM tenants WHERE api_key_prefix = ? AND api_key_hash = ? AND status = 'ACTIVE'
4. No row → 401 Unauthorized
5. Open db9 connection (from pool, keyed by tenant_id)
6. Route all operations to this tenant's db9 + S3
```

Single query, no second round-trip. Connection pooling with LRU eviction for idle tenant connections.

---

## 11. Security Model

### 11.1 Path Canonicalization

```
Raw input (URL-decoded once)
  → Reject if contains: NUL (\x00), control characters (\x01-\x1f), backslash (\)
  → Reject if any segment is "." or ".."
  → Collapse consecutive slashes: "///" → "/"
  → Directory paths: MUST end with "/" (e.g., "/data/")
  → File paths: MUST NOT end with "/" (e.g., "/data/a.txt")
  → Unicode NFC normalization
  → Result: canonical path
```

**Trailing slash convention**: Directories always end with `/`, files never do. This is consistent throughout: `parent_path` stores `/data/`, `ls` queries `WHERE parent_path = '/data/'`, and `rm -r` uses `LIKE '/data/%'`. The server enforces this on all API inputs.

### 11.2 Presigned URL Security

| Control | Spec |
|---------|------|
| **TTL** | Upload: max 10 minutes. Download: max 10 minutes. |
| **Binding** | Upload URLs bind: part number, `Content-Length`, `x-amz-checksum-sha256`. |
| **Log hygiene** | Redact `X-Amz-Signature` and `X-Amz-Credential` in logs. |
| **Download indirection** | `GET /v1/fs/{path}` for large files → one-time ticket → presigned URL. |

### 11.3 Rate Limiting

| Control | Scope | Default |
|---------|-------|---------|
| Request rate | Per tenant | 100 req/s |
| Upload bandwidth | Per tenant | 1 GB/hour |
| Concurrent uploads | Per tenant per path | 1 active session |
| Max file size | Per tenant | 100 GB |

---

## 12. Cost Controls

| Strategy | Implementation |
|---|---|
| TTL expiration | `expires_at` column + Reaper |
| Storage tiering | Small files in db9 (compute cost), large files in S3 (storage cost) |
| Cold data archive | S3 Lifecycle: 7d → Glacier Instant Retrieval |
| Incomplete upload cleanup | Reaper + S3 Lifecycle `AbortIncompleteMultipartUpload` |

---

## 13. Roadmap

| Phase | Scope | Effort |
|---|---|---|
| **P0** | Server: AGFS MountableFS + Drive9Backend + db9 integration (files + file_nodes tables) + small-file CRUD + auth + tenant | M |
| **P1** | Large-file upload: 202 flow + presigned URLs + uploads table + resume + Go SDK Transfer Engine | L |
| **P2** | CLI: `drive9 cp/cat/ls/stat/mv/rm/search` + progress bar + auto-resume | M |
| **P3** | Reaper + S3 Lifecycle + TTL cleanup + reference-counted delete | S |
| **P4** | file_tags table + tag CRUD + Query API + zero-copy cp | M |
| **P5** | MCP Server | S |
| **P6** | Python SDK | M |
| **P7** | Server-side grep/digest (small files) + mount management API | M |
| **P8** | Semantic processing pipeline: async L0/L1 generation from L2 (LLM-powered, bottom-up aggregation, .relations.json) | L |
| **P9** | Smart Parser & TreeBuilder: content-aware parsing (PDF→Markdown splitting), automatic categorization (OpenViking-inspired) | L |
| **P10** | FUSE mount (HTTP-backed + cache layers, reuse agfs-fuse patterns) | L |

---

## 14. Open Questions

| Question | Options | Leaning |
|---|---|---|
| Small/large file threshold | 50,000 bytes | Aligned with embedding model's max input (50,000 characters). Files below threshold get auto-embedding and FTS in db9. Files above go to S3 and participate in search via L0 abstracts. |
| db9 embedding model | text-embedding-v4 / amazon.titan-embed-text-v2:0 | titan (matches user config) |
| db9 FTS tokenizer | simple / jieba / chinese_ngram | jieba (Chinese support) |
| Object store | AWS S3 / MinIO / R2 | S3 for cloud, MinIO for on-prem |
| Upload conflict policy | Reuse session if same fingerprint / 409 if different | Reuse if same fingerprint, 409 if different |
| File versioning | None / simple version chain | None for P0 |
| Change notifications | None / polling / WebSocket | Polling for P0 |
| content_text for binary files | NULL / auto-extract text | NULL (only text files get content_text) |
| Provision anti-abuse | Rate limit only / CAPTCHA / invite-only | Rate limit for MVP. At scale: per-IP throttle on `/v1/provision`, max clusters per source. |
| Tenant-internal ACL | Single key full access / scoped tokens / path-based ACL | Single key for P0. Path-based ACL is a post-MVP concern (enterprise). |
| Cross-cluster schema migration | Manual / versioned migration tool / blue-green | Manual for P0. Need a rollout strategy before 100+ tenants. |
| S3 orphan reconciliation | Reaper only / S3 Inventory cross-check | Reaper for P0. S3 Inventory periodic audit at scale. |
| Observability | Structured logs only / metrics + traces / full o11y stack | Structured logs + Prometheus metrics for P0. Traces later. |

---

## Appendix A. Future Extensions

### Server-side grep/digest (small files only)

```
POST   /v1/fs/{path}?grep     Server-side search (small files in db9)
POST   /v1/fs/{path}?digest   Server-side hash (small files in db9)
```

### Mount Management (Admin-only)

```
GET    /v1/mounts              List mounted backends
POST   /v1/mounts              Mount a new backend (runtime, admin-only)
DELETE /v1/mounts/{path}       Unmount (admin-only)
```

Mount management is restricted to control-plane administrators. Tenant API keys cannot mount/unmount backends. Only built-in plugins are allowed; dynamic arbitrary plugin loading is prohibited.

P0 ships with two mounts: `/ -> Drive9Backend` and `/mem -> memfs`.

### Smart Parser & TreeBuilder (P9, OpenViking-inspired)

```
User uploads file.pdf to /inbox/
  │
  ├─▶ Parser: PDF → 12 Markdown sections (heading-based, 1024 tokens each)
  ├─▶ TreeBuilder: determine target path → /papers/2026/transformer-survey/
  ├─▶ mv /inbox/paper.pdf → /papers/2026/transformer-survey/paper.pdf
  │     (zero-cost: only UPDATE file_nodes, no storage copy)
  ├─▶ Write parsed sections as small files (→ db9, auto-embedded)
  └─▶ Generate .relations.json linking original → sections
```

**Key difference from OpenViking**: drive9 **always preserves the original file**. Both original and parsed sections coexist, linked via `.relations.json`.

---

## References

- **db9**: https://db9.ai/ --- Serverless database with built-in embedding, FTS, vector search, and fs9 file storage. Tenant database backend.
- **OpenViking**: https://github.com/volcengine/OpenViking --- Context database for AI agents. Tiered storage (L0/L1/L2) design reference.
- **AGFS**: https://github.com/c4pt0r/agfs --- Plan 9-inspired agent filesystem. We import its core interfaces.
- **Git LFS Batch API**: https://github.com/git-lfs/git-lfs/blob/main/docs/api/batch.md --- Control-plane upload pattern reference.
- **S3 Multipart Upload**: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
