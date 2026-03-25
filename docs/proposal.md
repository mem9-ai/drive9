# dat9: Agent-Native Data Infrastructure

**Status**: Proposal (Draft)  
**Date**: 2026-03-25  
**License**: Apache 2.0

---

## 1. Overview

dat9 is a unified data infrastructure for AI agents. It presents a single filesystem-like interface for storing, retrieving, and querying data of any kind --- small config files, large datasets, structured metadata, key-value pairs --- while keeping the underlying complexity invisible to the user.

An agent (or a human) interacts with dat9 the same way they interact with a local filesystem:

```bash
dat9 cp ./dataset.tar /data/dataset.tar        # upload (auto: presigned URL for large files)
dat9 cat /config/settings.json                  # read
dat9 ls /data/                                  # list
dat9 cp /data/a.bin /data/b.bin                 # server-side copy (no download/re-upload)
dat9 sh                                         # interactive shell
```

```python
client = Dat9("https://dat9.example.com", api_key="...")
client.write("/data/file.bin", open("local.bin", "rb"))
client.read("/data/file.bin")
```

Underneath, dat9 separates **control plane** (metadata + auth + upload orchestration) from **data plane** (object storage). Large files never flow through the server --- clients upload directly to S3 via presigned URLs, with automatic multipart chunking and resumable uploads. The CLI also offers an interactive shell (`dat9 sh`) for quick navigation and file operations.

### Problem Statement

- Agent tool fragmentation: each agent tool uses its own storage semantics and credentials.
- Server bandwidth bottlenecks for large files: proxying large uploads is slow and expensive.
- Missing metadata queryability: files exist, but cannot be filtered or discovered by rich tags.
- No unified path-based abstraction across backends: object store, memory, and future backends are not addressable by a single path model.

### Non-goals

- POSIX-complete semantics in P0.
- Transactional multi-file updates.
- Data warehouse replacement.
- A database for relational data (dat9 stores files + metadata, not relational records).

### Design Principles

1. **Users see only file operations** --- `cp`, `cat`, `ls`, `write`, `read`. All protocol complexity (presigned URLs, multipart, resume) is hidden in the SDK/CLI.
2. **Server is pure control plane** --- metadata, presigned URL issuance, upload progress tracking. Never touches large file data.
3. **Import, don't fork** --- Built on [AGFS](https://github.com/c4pt0r/agfs)'s `FileSystem` interface and `MountableFS` routing layer. Extend with our own backends and upload flow.
4. **Pluggable backends via MountableFS** --- A radix-tree router dispatches paths to different backends. P0 ships with S3+metadata and `/mem` scratch space. Future: KV store, vector search --- all addressable by path.
5. **Metadata and content are separated** --- Structured metadata lives in a relational DB; file content lives in object storage. This enables rich queries without scanning objects.
6. **Cost-aware from day one** --- TTL, storage tiering, and automatic cleanup of incomplete multipart uploads.
7. **Tiered context loading** --- Inspired by [OpenViking](https://github.com/volcengine/OpenViking)'s L0/L1/L2 model. Every directory can carry a ~100-token abstract (L0) and a ~1k-token overview (L1), enabling agents to scan cheaply before loading full content (L2).

---

## 2. Architecture

### Three-Layer Storage Design

dat9 separates storage into three independent layers, inspired by [OpenViking](https://github.com/volcengine/OpenViking)'s dual-layer architecture and extended with a metadata layer:

```
┌──────────────────────────────────────────────────────────────────────┐
│                        dat9 Server (Go)                              │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                   MountableFS (AGFS)                           │  │
│  │              radix-tree path → backend routing                 │  │
│  │                                                                │  │
│  │   /          → S3MetaBackend (files + metadata)                │  │
│  │   /mem/      → memfs (in-memory scratch)                       │  │
│  │   /kv/       → kvfs (future)                                   │  │
│  └──────────┬─────────────────────────┬───────────────────────────┘  │
│             │                         │                              │
│  ┌──────────▼──────────┐   ┌──────────▼──────────┐                  │
│  │  Content Layer      │   │  Metadata Layer      │                  │
│  │                     │   │                      │                  │
│  │  S3 / Object Store  │   │  Relational DB       │                  │
│  │  blobs/<ulid>       │   │  - files table       │                  │
│  │  (content-addressed │   │  - file_tags table   │                  │
│  │   L2, L0, L1 all   │   │  - uploads table     │                  │
│  │   stored by ULID)   │   │  - context_layers    │                  │
│  │  memfs (volatile)   │   │                      │                  │
│  └─────────────────────┘   └──────────────────────┘                  │
│             │                                                        │
│             │  async (write → queue → process)                       │
│             ▼                                                        │
│  ┌─────────────────────┐                                             │
│  │  Index Layer        │                                             │
│  │                     │                                             │
│  │  Vector DB          │  ← L0 abstracts → embeddings               │
│  │  - URI + vector     │  ← semantic search over directory tree      │
│  │  - parent_uri       │  ← hierarchical retrieval                   │
│  │  - context_type     │                                             │
│  │  - sparse vector    │                                             │
│  │                     │                                             │
│  │  (not in read path  │                                             │
│  │   for file I/O)     │                                             │
│  └─────────────────────┘                                             │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────────┐   │
│  │  Background Workers                                           │   │
│  │  - SemanticProcessor: file → LLM → L0/L1 generation          │   │
│  │  - VectorIndexer: L0 → embedding → vector DB upsert          │   │
│  │  - Reaper: cleanup expired / orphaned / aborted uploads       │   │
│  └───────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

**Key separation**:

| Layer | Stores | Read Latency | Write Path | Scales Independently |
|-------|--------|-------------|------------|---------------------|
| **Content** (S3 + memfs) | Actual file bytes (L0, L1, L2) | ~10-100ms (S3) / <1ms (memfs) | Synchronous (small) or presigned URL (large) | Yes (S3 is infinite) |
| **Metadata** (Relational DB) | Paths, tags, upload state, revisions | ~1-5ms | Synchronous, always through server | Yes (DB scales separately) |
| **Index** (Vector DB) | Embeddings of L0 abstracts, URIs | ~10-50ms | **Asynchronous** --- never blocks file writes | Yes (vector DB scales separately) |

**The index layer is async and decoupled from the write path.** Writing a file to dat9 returns immediately. The vector index is updated later by a background worker. This means:

- File writes are never slowed by embedding computation or vector DB latency.
- The vector index can be rebuilt from scratch by re-reading L0s from S3.
- If the vector DB is down, file I/O continues unaffected. Search is degraded but files are safe.

### Client Layer

```
    ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐
    │   CLI    │  │   SDK    │  │   MCP    │  │ FUSE (later) │
    │ dat9 cp  │  │ Go/Py   │  │ Tools    │  │              │
    │ dat9 sh  │  │          │  │          │  │              │
    └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────┬───────┘
         └─────────────┼─────────────┘               │
                       │  (all go through SDK)        │
    ┌──────────────────▼──────────────────────────────▼──────┐
    │  Transfer Engine (inside SDK)                          │
    │                                                        │
    │  Small file: PUT body → server proxy → S3              │
    │  Large file: PUT → 202 + presigned URLs → direct to S3 │
    │  Resume: GET /v1/uploads?path=... → re-issue URLs      │
    └───────────────────────────────────────────────────────┘
```

Large file data goes directly from client to S3 via presigned URLs. The server never touches it.

### Relationship with AGFS

dat9's server imports AGFS as a Go module dependency (Apache 2.0). We pin the AGFS dependency to a specific commit or tag to avoid interface drift.

| AGFS Package | What We Use |
|---|---|
| `pkg/filesystem` | `FileSystem` interface (Create, Read, Write, ReadDir, Stat, Rename, ...), `Capabilities` system, `WriteFlag`/`OpenFlag` types, `StreamReader`/`Toucher`/`Symlinker` extension interfaces |
| `pkg/mountablefs` | `MountableFS` radix-tree path router --- dispatches `/path` to the correct backend plugin via longest-prefix match, lock-free reads (`atomic.Value`) |
| `pkg/plugin` | `ServicePlugin` interface (Name, Validate, Initialize, GetFileSystem, GetReadme, GetConfigParams, Shutdown) |
| `pkg/plugins/memfs` | In-memory filesystem plugin used for `/mem` scratch mount |

We write our own HTTP handlers (AGFS's handlers bind to a different URL schema), our own `S3MetaBackend` (implements `filesystem.FileSystem`), and all upload/query/reaper logic.

```go
import (
    "github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
    "github.com/c4pt0r/agfs/agfs-server/pkg/mountablefs"
    "github.com/c4pt0r/agfs/agfs-server/pkg/plugin"
)

// Our backend implements AGFS's FileSystem interface
type S3MetaBackend struct {
    meta MetadataStore  // files + file_tags + context_layers tables
    s3   S3Client       // object storage (blobs/<ulid> keys)
}

func (b *S3MetaBackend) Read(path string, offset, size int64) ([]byte, error) { ... }
func (b *S3MetaBackend) Write(path string, data []byte, offset int64, flags filesystem.WriteFlag) (int64, error) { ... }
func (b *S3MetaBackend) ReadDir(path string) ([]filesystem.FileInfo, error) { ... }
func (b *S3MetaBackend) Stat(path string) (*filesystem.FileInfo, error) { ... }
// ... all FileSystem methods

// Capability detection via type assertion (AGFS pattern)
if cp, ok := backend.(filesystem.CapabilityProvider); ok {
    caps := cp.GetCapabilities()
    if caps.IsObjectStore { /* use presigned URL path */ }
}

// Mount it
mfs := mountablefs.NewMountableFS(api.DefaultPoolConfig())
mfs.Mount("/", &S3MetaPlugin{backend: s3meta})
mfs.Mount("/mem", memfsPlugin)  // in-memory scratch space, AGFS built-in
// Future:
// mfs.Mount("/kv", kvfsPlugin)
```

---

## 3. Two Data Paths

dat9 serves two fundamentally different workloads through a single interface.

### Small Files (< 10 MB): Server Proxy

```
Client ──PUT body──▶ dat9 server ──PUT──▶ S3
                            │
                     INSERT metadata
                            │
                     ◀── 200 OK ──
```

The server reads the request body, uploads to S3, writes metadata, and returns. Simple, synchronous, one round-trip for the client.

### Large Files (>= 10 MB): Presigned URL Direct Upload

```
Client ──PUT (Content-Length only, no body)──▶ dat9 server
                                                  │
                                           CreateMultipartUpload
                                           PresignUploadPart x N
                                                  │
Client ◀── 202 { parts: [{url, size}, ...] } ─────┘

Client ──PUT part 1──▶ S3 (direct, server not involved)
Client ──PUT part 2──▶ S3
  ...
Client ──PUT part N──▶ S3

Client ──POST /v1/uploads/{id}/complete──▶ dat9 server
                                              │
                                       CompleteMultipartUpload
                                       UPDATE files → CONFIRMED
                                              │
Client ◀── 200 { confirmed } ────────────────┘
```

The server never touches large file data. It issues presigned URLs and tracks progress.

**Capability-aware write handler**: the server checks `backend.(filesystem.CapabilityProvider).GetCapabilities().IsObjectStore` via type assertion. If true and size >= threshold, return 202 with presigned URLs. If false (e.g., `memfs`) or if the backend does not implement `CapabilityProvider`, always use direct writes and never return 202.

### Resumable Uploads

The SDK is **stateless** --- no local state files. On interruption, the SDK queries the server:

```
GET /v1/uploads?path=/data/big.bin&status=UPLOADING
```

The server calls `S3.ListParts()` to determine which parts were already uploaded, then re-issues presigned URLs for the remaining parts. The client resumes from where it left off.

```bash
$ dat9 cp ./10gb.tar /data/10gb.tar
Uploading /data/10gb.tar (10.0 GB)
[████████████░░░░░░░░░░] 45%  ^C  # interrupted

$ dat9 cp ./10gb.tar /data/10gb.tar     # just re-run the same command
Resuming upload (72/160 parts done)
[█████████████████████░] 95%
Upload complete: /data/10gb.tar (10.0 GB)
```

### S3 Key Strategy: Content-Addressed Storage

dat9 uses **content-addressed S3 keys**, not semantic paths. Every file is stored at `blobs/<ulid>`:

```
S3 Bucket
  blobs/
    01JQ7R8K3M0000000000000001     ← /data/training-v3/images.tar.gz
    01JQ7R8K3M0000000000000002     ← /data/training-v3/.abstract.md
    01JQ7R8K3M0000000000000003     ← /config/settings.json
```

The **path-to-blob mapping** lives in the `files` table:

```
files.path                          → files.s3_key
"/data/training-v3/images.tar.gz"   → "blobs/01JQ7R8K3M0000000000000001"
"/data/training-v3/.abstract.md"    → "blobs/01JQ7R8K3M0000000000000002"
"/config/settings.json"             → "blobs/01JQ7R8K3M0000000000000003"
```

**Why content-addressed keys?**

| Concern | Semantic keys (`/data/foo.bin`) | Content-addressed (`blobs/<ulid>`) |
|---------|---|----|
| **Rename / Move** | Must copy S3 object + delete old ($$, slow for large files) | `UPDATE files SET path=? WHERE file_id=?` — zero S3 cost |
| **S3 key conflicts** | Path collisions require escaping | ULIDs are globally unique by construction |
| **Key length** | Deep paths can exceed S3 limits | Fixed ~30 chars |
| **Garbage collection** | S3 objects carry semantic meaning, risky to delete | Any blob not referenced by `files.s3_key` is garbage |

**Operation mapping**:

| dat9 operation | What happens |
|---|---|
| `dat9 ls /data/` | `SELECT path, size_bytes, ... FROM files WHERE parent_path = '/data/' AND status = 'CONFIRMED'` |
| `dat9 cat /data/a.bin` | `SELECT s3_key FROM files WHERE path = '/data/a.bin'` → `S3.GetObject(s3_key)` |
| `dat9 mv /data/a.bin /data/b.bin` | `UPDATE files SET path = '/data/b.bin', parent_path = '/data/' WHERE path = '/data/a.bin'` (zero S3 cost) |
| `dat9 cp /data/a.bin /data/b.bin` | `S3.CopyObject(src_key, new_ulid_key)` + `INSERT files (path='/data/b.bin', s3_key='blobs/<new_ulid>')` |
| `dat9 stat /data/a.bin` | `SELECT * FROM files WHERE path = '/data/a.bin'` |

This design follows db9's philosophy: **the path namespace is a metadata concern, not a storage concern.** S3 is a dumb blob store; the database gives it structure.

---

## 4. Tiered Context Storage and Semantic Index

dat9 adopts a three-layer content model inspired by [OpenViking](https://github.com/volcengine/OpenViking)'s L0/L1/L2 tiered context architecture. The core insight: agents rarely need the full content of a file. They need just enough context to decide whether to load more.

### 4.1 The L0 / L1 / L2 Model

Every directory in dat9 can optionally carry three layers of progressively detailed content:

| Layer | File | Token Budget | Purpose |
|-------|------|-------------|---------|
| **L0** | `.abstract.md` | ~100 tokens | Ultra-short summary. Used for vector search, quick filtering, directory-level scans. |
| **L1** | `.overview.md` | ~1-2k tokens | Structured overview with navigation pointers. Tells the agent *what's here* and *how to access details*. |
| **L2** | Original files | Unlimited | Full content. Loaded only when the agent confirms it needs the detail. |

Example directory:

```
/data/training-v3/
  .abstract.md          # L0: "ImageNet-subset training data, 50k images, labeled, v3."
  .overview.md          # L1: structured summary + navigation to L2 files
  metadata.json         # L2: full metadata
  images.tar.gz         # L2: full data (10 GB)
```

Token savings: scanning 20 directories via L0 costs ~2k tokens. Loading 3 L1 overviews costs ~3k. Loading 1 full L2 costs ~5k. Total: **10k tokens instead of 100k** (10x reduction).

### 4.2 Dual-Layer Storage: Content + Index

Following OpenViking's architecture, dat9 separates content storage from semantic index:

```
┌──────────────────────────────────────────────────────────────────┐
│                    dat9 Storage Architecture                      │
│                                                                  │
│  ┌────────────────────────────────┐  ┌────────────────────────┐  │
│  │     Content Layer (S3/memfs)   │  │   Index Layer (Vector) │  │
│  │                                │  │                        │  │
│  │  Stores:                       │  │  Stores:               │  │
│  │  - L2 original files           │  │  - URI (path)          │  │
│  │  - L1 .overview.md             │  │  - parent_uri          │  │
│  │  - L0 .abstract.md             │  │  - dense vector        │  │
│  │  - .relations.json             │  │  - sparse vector       │  │
│  │                                │  │  - L0 text (denorm)    │  │
│  │  Source of truth for content.  │  │  - context_type        │  │
│  │  File I/O reads from here.     │  │  - is_leaf             │  │
│  │                                │  │                        │  │
│  │  Scales: S3 = infinite         │  │  NOT in file I/O path. │  │
│  │                                │  │  Only for search().    │  │
│  └────────────────────────────────┘  └────────────────────────┘  │
│                                                                  │
│  Single Data Source Principle:                                    │
│  - All reads come from the Content Layer (S3).                   │
│  - Vector Index stores only references (URIs) + embeddings.      │
│  - If the vector index is lost, rebuild from L0s in S3.          │
└──────────────────────────────────────────────────────────────────┘
```

**Why separate?**

| Concern | Content Layer (S3) | Index Layer (Vector DB) |
|---------|-------------------|------------------------|
| **Availability** | Must be 100% available for file I/O | Can be temporarily down --- search degrades, but files are safe |
| **Consistency** | Strong (S3 read-after-write) | Eventual (async indexing) |
| **Rebuild** | Cannot rebuild (source of truth) | Can rebuild from L0s in S3 |
| **Scaling** | S3 scales infinitely | Vector DB scales independently |
| **Cost** | S3 storage cost | Compute (embeddings) + vector DB memory |

### 4.3 Async Processing Pipeline

File writes and vector indexing are **decoupled by a queue**. This is critical: file I/O must never block on LLM calls or vector DB writes.

```
File Write (synchronous)              Background Workers (asynchronous)
─────────────────────                 ──────────────────────────────────

dat9 cp file.md /docs/               SemanticProcessor (picks from queue):
  │                                     │
  ├─▶ S3.PutObject(blobs/<ulid>)       ├─▶ S3.GetObject(blobs/<ulid>)
  ├─▶ INSERT INTO files                ├─▶ LLM: generate L0 (.abstract.md)
  │     (path, s3_key, CONFIRMED)      ├─▶ LLM: generate L1 (.overview.md)
  ├─▶ ENQUEUE(semantic_queue,          ├─▶ S3.PutObject(.abstract.md)
  │     {path, action: "created"})     ├─▶ S3.PutObject(.overview.md)
  │                                     │
  └─▶ 200 OK  (immediate)             VectorIndexer (after L0 ready):
                                        │
       ← file is usable NOW,           ├─▶ S3.GetObject(.abstract.md)
         search catches up later        ├─▶ Embed(L0 text) → dense vector
                                        ├─▶ VectorDB.Upsert({
                                        │     uri: "/docs/file.md",
                                        │     parent_uri: "/docs/",
                                        │     vector: [...],
                                        │     abstract: "...",
                                        │     context_type: "resource",
                                        │     is_leaf: true
                                        │   })
                                        └─▶ Propagate: re-generate parent
                                              /docs/.abstract.md (bottom-up)
```

**Key properties**:

- **File write returns immediately.** The 200 OK is returned after S3 + metadata. The agent can read the file right away.
- **L0/L1 generation is async.** Uses an LLM (configurable: local model or API). Runs bottom-up: leaf files first, then parent directories aggregate child L0s into their own L1.
- **Vector indexing is async.** Embedding computation and vector DB upsert happen after L0 is generated. Search results for newly written files may lag by seconds to minutes.
- **Queue provides backpressure.** If the LLM or vector DB is slow, the queue buffers. File I/O is unaffected.
- **Rebuilable.** The vector index can be fully rebuilt by scanning all `.abstract.md` files in S3 and re-embedding. No data loss.

### 4.4 Hierarchical Retrieval

The tiered model enables directory-recursive semantic search, following OpenViking's `HierarchicalRetriever` pattern:

```
Agent: "find training data for image classification"

Step 1: Vector search over L0s
  → Query embedding vs all L0 vectors
  → Returns candidate URIs: [/data/training-v3/, /data/imagenet/, /experiments/resnet/]

Step 2: Agent reads L1 of top candidates
  → dat9 overview /data/training-v3/
  → ~1k tokens, structured: "50k images, labeled, classes: dog/cat/bird..."
  → Agent decides: this is the one.

Step 3: Agent loads specific L2 files
  → dat9 cat /data/training-v3/metadata.json
  → Full detail, loaded on-demand.
```

The filesystem directory structure itself becomes the navigation hierarchy. No separate taxonomy or ontology needed.

### 4.5 Vector Index Schema

```
Collection: dat9_context
─────────────────────────
id            string       Primary key (ULID)
uri           string       File/directory path (e.g., "/data/training-v3/")
parent_uri    string       Parent directory URI
context_type  string       "resource" | "memory" | "skill" | ... (extensible)
is_leaf       bool         true for files, false for directories
vector        float[]      Dense embedding of L0 abstract
sparse_vector map          Sparse vector (BM25-style, for keyword matching)
abstract      string       L0 text (denormalized for reranking without S3 read)
name          string       Display name
created_at    string       ISO timestamp
active_count  int64        Usage counter (for relevance boosting)
```

### 4.6 Cross-Resource Relations (.relations.json)

Inspired by OpenViking's relation graph, each directory can optionally carry a `.relations.json` sidecar file describing links to related resources:

```json
{
  "relations": [
    {
      "target": "/data/imagenet/",
      "type": "derived_from",
      "description": "Training subset extracted from ImageNet"
    },
    {
      "target": "/experiments/resnet-v2/",
      "type": "used_by",
      "description": "Used as training input for ResNet v2 experiment"
    }
  ]
}
```

**Design constraints**:

- `.relations.json` is a regular file stored in S3 via the same `blobs/<ulid>` mechanism. "Everything is a file" applies.
- Relations are **advisory**, not enforced. Deleting a target does not cascade-delete the relation entry.
- P0: users write `.relations.json` manually. P8+: the semantic processor auto-generates relation suggestions based on content similarity.
- The vector index can optionally index relation targets for graph-aware retrieval (e.g., "find all datasets used by experiment X").

### 4.7 Scope and Boundaries

dat9's responsibility is clearly scoped:

```
┌─────────────────────────────────────────────────────────┐
│  Upper Layer (Agent Framework / Application)             │
│                                                          │
│  - Intent analysis ("what does the user want?")          │
│  - Query planning ("which files to load?")               │
│  - Reranking (cross-encoder, business logic)             │
│  - Context assembly (compose prompt from L0/L1/L2)       │
│  - Conversation memory management                        │
│  - Access control policies beyond path-level auth         │
└──────────────────────────┬──────────────────────────────┘
                           │  calls dat9 API
┌──────────────────────────▼──────────────────────────────┐
│  dat9 (This System)                                      │
│                                                          │
│  - File CRUD: cp, cat, ls, mv, stat, rm                  │
│  - Metadata: tags, queries, revisions                    │
│  - Tiered context: L0/L1/L2 storage and caching          │
│  - Vector search: /v1/search, /v1/find (P9+)             │
│  - Upload orchestration: presigned URLs, multipart        │
│  - Sharing: snapshot export/import, live read-only mounts │
│  - Background: semantic generation, vector indexing        │
└─────────────────────────────────────────────────────────┘
```

dat9 provides **storage, retrieval, and basic semantic search**. It does **not** interpret user intent, orchestrate multi-step reasoning, or decide which files to load. Those decisions belong to the calling agent framework.

### 4.8 Relationship to dat9 Core

Tiered storage and vector indexing are **built on top of** dat9's existing file operations, not a separate system:

- `.abstract.md` and `.overview.md` are real files in S3 (stored at `blobs/<ulid>` like any other file), stored via the same S3MetaBackend. "Everything is a file" is preserved.
- L0/L1 text is **cached** in a dedicated `context_layers` table (separate from `files` to avoid core table bloat). See [design-content-locality.md](./design-content-locality.md) for the full rationale.
- The `context_layers` table includes a `content_hash` and `source_s3_etag` for staleness detection. A periodic reconciler catches drift.
- The `semantic_queue` is a table in the metadata DB (or an external queue like SQS/Redis).
- The vector DB is a pluggable backend (local, HTTP, or managed service).
- **P0 ships without auto-generation or vector search.** Users manage L0/L1 manually. Semantic processing and vector search are P8+ features.
- Even without the async pipeline, the L0/L1/L2 convention is useful --- agents can manually write `.abstract.md` and benefit from tiered loading.

### 4.9 Sharing

Sharing a directory tree means sharing its L0/L1/L2 files together. Because L0/L1 are real files in S3, they travel with the directory naturally.

Assumption: each tenant (agent) has its own TiDB Serverless cluster. Therefore cross-tenant share metadata must live in a global control-plane registry, not inside one tenant DB.

**V1 (default, recommended): Snapshot share create/accept**

```bash
dat9 share create /knowledge/ml-papers/ --to agent-007 --mode snapshot
dat9 share accept sh_01J... --to /shared/ml-papers/
```

Snapshot mode performs point-in-time export/import across clusters. L0/L1 are included automatically because they are files. The recipient writes metadata in its own tenant DB and rebuilds `context_layers` cache and vector index locally. This keeps search consistency and ownership fully local to the recipient tenant.

**V2 (future, optional): Cross-namespace read-only mounts**

```bash
dat9 share create /knowledge/ml-papers/ --to agent-007 --mode ro
dat9 share mount sh_01J... /shared/ml-papers/
```

Read-only only in V2. No recipient-side caching for shared paths (always read-through to source S3). Non-transitive (no share-of-share). Source tenant owns bytes/revisions; target tenant owns only mount metadata and optional local derived indexes. Because per-tenant vector stores are isolated, live-share search freshness requires a separate sync/index policy and is intentionally deferred. See [design-content-locality.md](./design-content-locality.md) for security and consistency analysis.

### 4.10 API for Search

```
# Semantic search (uses vector index)
POST /v1/search
{
  "query": "training data for image classification",
  "target_uri": "/data/",              # scope search to a subtree
  "context_type": "resource",          # optional filter
  "top_k": 10
}
→ [{ "uri": "/data/training-v3/", "score": 0.92, "abstract": "..." }, ...]

# Hierarchical find (vector search + L1 loading in one call)
POST /v1/find
{
  "query": "training data for image classification",
  "target_uri": "/data/",
  "depth": 2,                          # how many levels to recurse
  "include_overview": true              # return L1 alongside results
}
→ [{ "uri": "/data/training-v3/", "score": 0.92, "abstract": "...", "overview": "..." }, ...]
```

---

## 5. API Design

### Unified FS Endpoint

All file operations go through `/v1/fs/{path}`. The server auto-routes based on file size and operation.

```
PUT    /v1/fs/{path}          Write (200 for small, 202 for large)
GET    /v1/fs/{path}          Read  (200 for small, 302 redirect for large)
DELETE /v1/fs/{path}          Delete
HEAD   /v1/fs/{path}          Stat  (standard HTTP semantics)
GET    /v1/fs/{path}?list     List directory

GET    /v1/fs/{path}?abstract Returns L0 (.abstract.md) content directly
GET    /v1/fs/{path}?overview Returns L1 (.overview.md) content directly

POST   /v1/fs/{path}?copy     Server-side copy (S3 CopyObject, no download)
  Header: X-AgentFS-Copy-Source: /source/path

POST   /v1/search             Semantic search over L0 vectors (see Section 4.8)
POST   /v1/find               Hierarchical search with L1 loading (see Section 4.8)
```

### API Error Model

- 200: success
- 202: large file upload required
- 302: redirect to presigned download URL
- 400: bad request
- 404: not found
- 409: conflict (upload already exists for different file)
- 412: precondition failed (If-Match revision mismatch)
- 413: file too large (exceeds namespace quota)

### Upload Management (SDK-internal, not user-facing)

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

No raw SQL exposed. Structured filters are translated server-side.

### Concurrency Control

- Default: **Last Writer Wins** (LWW)
- Optional: `If-Match: <revision>` on PUT for optimistic locking. Mismatch returns `412 Precondition Failed`.
- `revision` is a server-managed, auto-incrementing BIGINT stored in `files.revision`.
- Write to a path auto-creates parent directories (mkdir -p semantics).

**Atomic conditional update** (prevents lost updates):

```sql
UPDATE files
SET    revision = revision + 1,
       s3_key = ?,
       size_bytes = ?,
       checksum_sha256 = ?,
       confirmed_at = NOW(3)
WHERE  file_id = ?
  AND  revision = ?;          -- client-supplied from If-Match header

-- affected_rows = 0  →  return 412 Precondition Failed
-- affected_rows = 1  →  success, return new revision in ETag
```

The same conditional-update pattern applies to `Rename`, `Copy`, and `Delete` operations.

---

## 6. Metadata Schema

All metadata lives in a per-namespace relational database. **Four tables** per tenant:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌────────────────┐
│   files     │────▶│  file_tags  │     │   uploads   │     │ context_layers │
│             │     │             │     │             │     │                │
│ path (UK)   │     │ file_id+key │     │ upload_id   │     │ path+layer     │
│ s3_key      │     │ tag_value   │     │ file_id     │     │ content        │
│ parent_path │     │             │     │ s3_upload_id│     │ content_hash   │
│ is_directory│     └─────────────┘     └─────────────┘     └────────────────┘
│ revision    │
│ status      │     Core FS table       Upload state         Semantic cache
└─────────────┘     (precise queries)   (multipart mgmt)     (L0/L1, derived)
```

### files — unified path + content metadata

The `files` and `file_paths` tables are **merged into a single `files` table**. There is no separate path table. Path is a column on `files`, not a separate entity. This simplifies all operations (`ls` = one query, `stat` = one query, `mv` = one UPDATE) and follows AGFS's pattern where the filesystem itself is the namespace authority.

```sql
CREATE TABLE files (
    file_id         VARCHAR(26) PRIMARY KEY,    -- ULID (time-ordered, distributed-friendly)
    namespace_id    VARCHAR(255) NOT NULL,       -- tenant isolation
    path            VARCHAR(4096) NOT NULL,      -- canonical path (e.g., "/data/training-v3/images.tar.gz")
    parent_path     VARCHAR(4096) NOT NULL,      -- parent directory (e.g., "/data/training-v3/")
    is_directory    BOOLEAN NOT NULL DEFAULT FALSE,
    s3_key          VARCHAR(1024),               -- blobs/<ulid>, NULL for directories
    content_type    VARCHAR(127),
    size_bytes      BIGINT NOT NULL DEFAULT 0,
    checksum_sha256 CHAR(64),
    revision        BIGINT NOT NULL DEFAULT 1,
    status          ENUM('PENDING','CONFIRMED','ORPHANED','DELETED') NOT NULL DEFAULT 'PENDING',
    source_id       VARCHAR(255),                -- e.g., "agent-007", for provenance tracking
    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    confirmed_at    DATETIME(3),
    updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    expires_at      DATETIME(3),                 -- TTL
    UNIQUE KEY idx_ns_path (namespace_id, path),
    INDEX idx_parent (namespace_id, parent_path),
    INDEX idx_status_created (status, created_at),
    INDEX idx_expires (expires_at)
);
```

**Key design notes**:

- `s3_key = 'blobs/<ulid>'` for files, `NULL` for directories. Content-addressed: see §3 S3 Key Strategy.
- `path` + `namespace_id` is the unique key. This is the canonical identity of a file.
- `parent_path` enables `ls` via `SELECT ... WHERE parent_path = ?`, mirroring AGFS sqlfs's single-table pattern.
- Directories are rows with `is_directory = TRUE` and `s3_key = NULL`. Created automatically via mkdir-p semantics on file write.
- `revision` supports optimistic concurrency (see §5 Concurrency Control). Atomic update: `UPDATE ... WHERE revision = ?`.

### file_tags — separate tag table for precise queries

Tags are a **separate table**, not a JSON column on `files`. This enables proper SQL indexing for precise filtering (`dat9 ls --tag env=prod`), efficient tag modification (add/remove individual tags without rewriting the row), and multi-tag queries with standard SQL.

```sql
CREATE TABLE file_tags (
    namespace_id VARCHAR(255) NOT NULL,          -- denormalized for efficient tag scans
    file_id      VARCHAR(26) NOT NULL,
    tag_key      VARCHAR(255) NOT NULL,
    tag_value    VARCHAR(1024) NOT NULL DEFAULT '',
    updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (file_id, tag_key),
    INDEX idx_kv (namespace_id, tag_key, tag_value),  -- for "find all files where env=prod"
    INDEX idx_file (file_id)                           -- for "get all tags of file X"
);
```

**Tags dual-write (P9+)**: When tags are written to SQL, they are also propagated to the vector index as scalar metadata. This enables combined semantic + tag queries:

```
POST /v1/search
{
  "query": "training data",
  "filter": { "tags": {"env": "prod"} }    ← scalar filter in vector DB
}
```

SQL handles precise queries (`tag_key = 'env' AND tag_value = 'prod'`). Vector DB handles semantic queries with tag filters. Both are authoritative for their respective query types.

### uploads — multipart upload state tracking

```sql
CREATE TABLE uploads (
    upload_id          VARCHAR(26) PRIMARY KEY,
    file_id            VARCHAR(26) NOT NULL,
    namespace_id       VARCHAR(255) NOT NULL,         -- tenant isolation
    path               VARCHAR(4096) NOT NULL,         -- for GET /v1/uploads?path=...
    fingerprint_sha256 CHAR(64),                       -- content fingerprint for dedup/conflict detection
    idempotency_key    VARCHAR(255),                   -- client-provided, prevents duplicate sessions
    s3_upload_id       VARCHAR(255) NOT NULL,
    bucket             VARCHAR(63) NOT NULL,
    s3_key             VARCHAR(1024) NOT NULL,          -- blobs/<ulid>, matches files.s3_key
    total_size         BIGINT NOT NULL,
    part_size          BIGINT NOT NULL,
    parts_total        INT NOT NULL,
    status             ENUM('UPLOADING','COMPLETED','ABORTED','EXPIRED') NOT NULL DEFAULT 'UPLOADING',
    created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    expires_at         DATETIME(3) NOT NULL,
    INDEX idx_path_status (path, status),
    INDEX idx_status_expires (status, expires_at),
    UNIQUE idx_active_upload (namespace_id, path, (IF(status='UPLOADING', 0, upload_id)))
    -- Enforces: at most one UPLOADING session per (namespace, path).
    -- Implementation note: for DBs without expression indexes, enforce in application layer
    -- via SELECT ... FOR UPDATE before INSERT.
);
```

### context_layers — semantic cache (derived, rebuildable)

Renamed from `content_cache`. This table caches L0/L1 text extracted from S3 sidecar files, enabling batch scans without per-file S3 GETs. It is part of the **semantic pipeline**, not the core filesystem — if the pipeline is disabled, this table stays empty. Zero invasiveness.

```sql
CREATE TABLE context_layers (
    namespace_id    VARCHAR(255) NOT NULL,            -- tenant isolation
    path            VARCHAR(4096) NOT NULL,
    layer           ENUM('L0', 'L1') NOT NULL,
    content         TEXT NOT NULL,
    content_hash    CHAR(64) NOT NULL,                -- SHA-256, for staleness detection
    source_s3_etag  VARCHAR(255),                     -- S3 ETag at cache fill time
    status          ENUM('PENDING', 'READY') NOT NULL DEFAULT 'PENDING',
    updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (namespace_id, path, layer),
    INDEX idx_status (status),
    CHECK (LENGTH(content) <= 8192)                   -- 8 KB max, prevent unbounded growth
);
```

**Why `context_layers` and not `content_cache`?** The name reflects that this table stores semantic context at different abstraction layers (L0 abstract, L1 overview), not just cached content. It is extensible: future layers (multi-language L0, L0 version history, L1 variants) are just more rows.

### Design notes

**Why ULID for primary keys?** Time-ordered (efficient range scans) + random suffix (avoids write hotspots in distributed DBs like TiDB).

**Why merge `files` + `file_paths` into one table?** The original design had a separate `file_paths` table with `(path PK, file_id FK)`. This added JOIN cost to every `ls` and `stat` operation, complicated `mv` (update both tables), and separated concerns that belong together. AGFS's `sqlfs` uses a single table with `path` + `parent_path` columns — dat9 follows this pattern. The merged table is simpler, faster (single-table scans), and easier to reason about.

---

## 7. Consistency Model

### Write Path: S3-first, then Metadata

```
Small file:
  1. S3.PutObject(blobs/<ulid>)                  -- fail → return error, no dirty data
  2. INSERT INTO files (path, s3_key, CONFIRMED)  -- fail → best-effort S3 delete; Reaper catches the rest
  3. Auto-create parent directories (mkdir -p)    -- INSERT IGNORE for each ancestor

Large file:
  1. INSERT INTO files (path, s3_key, PENDING) + INSERT INTO uploads
  2. Client uploads parts directly to S3 via presigned URLs
  3. Client calls /complete → CompleteMultipartUpload → UPDATE files SET status = 'CONFIRMED'
```

### State Machines

`files` and `uploads` have **separate** state machines with cross-table invariants.

**files state machine**:

```
PENDING ──────────────────▶ CONFIRMED ──▶ (normal use)
    │  (S3 upload complete     │
    │   + /complete called)    │ expires_at / explicit delete
    │                          ▼
    │                       DELETED ──▶ Reaper (S3 + metadata + context_layers + vector cleanup)
    │
    │ Reaper: S3.HeadObject fails
    ▼
 ORPHANED ──▶ Reaper (metadata cleanup)
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
| `uploads.status = COMPLETED` ⟹ `files.status = CONFIRMED` | A completed upload always has a confirmed file |
| `uploads.status = UPLOADING` ⟹ `files.status = PENDING` | An in-progress upload has a pending file |
| `files.status = CONFIRMED` ⟹ S3 has the complete object | The fundamental data integrity guarantee |
| At most one `uploads` row with `status = UPLOADING` per `(namespace, path)` | Prevents concurrent upload races (enforced by DB constraint) |

### Delete Path: Synchronous Cleanup

File deletion (`DELETE /v1/fs/{path}`) performs **synchronous** cleanup of all derived data before returning:

```
DELETE /v1/fs/{path}
  1. DELETE FROM context_layers WHERE path = ? OR path LIKE ?/%   -- L0/L1 cache
  2. DELETE FROM file_tags WHERE file_id = ?                      -- tags
  3. VectorDB.Delete(uri = path)                                  -- vector index entry
  4. S3.DeleteObject(s3_key)                                      -- content blob
  5. UPDATE files SET status = 'DELETED'                          -- metadata (soft delete)
  → 200 OK
```

This matches OpenViking's `VikingFS.rm()` pattern: derived indexes are cleaned synchronously on delete. The Reaper is a **safety net**, not the primary cleanup path.

### Reaper (Background Cleanup)

Runs periodically to:
1. **Abort timed-out uploads**: `S3.AbortMultipartUpload` + update status. Critical --- incomplete multipart uploads accumulate storage costs silently.
2. **Reconcile orphaned PENDING files**: `S3.HeadObject` to check if data exists. Promote to CONFIRMED or mark ORPHANED.
3. **Delete TTL-expired files**: Remove S3 objects + mark DELETED.
4. **Sweep stale derived data**: Remove `context_layers` rows and vector index entries for paths whose `files.status` is `DELETED` or `ORPHANED` (idempotent --- re-running is safe).

**Invariant**: `status=CONFIRMED` implies S3 has the complete file.

---

## 8. Failure Modes

- Init timeout → client retries create.
- Part presigned URL expired → SDK calls resume to get fresh URLs.
- `CompleteMultipartUpload` returns 200 with error body → server parses XML body, retries or marks ABORTED.
- DB write fails after S3 success → Reaper reconciles (PENDING → CONFIRMED or ORPHANED).
- Multiple clients same path → server returns existing upload session if fingerprint matches, 409 Conflict if different file.

---

## 9. Multi-Tenancy

```
Client (API Key)
  → Auth Middleware
    → Resolve: Credential → Namespace record (DB conn + S3 config)
    → All operations scoped to this Namespace
```

Each namespace has its own database (or schema) and S3 prefix. Connection pooling with LRU eviction for idle connections.

For cross-tenant sharing, dat9 adds a **global share registry** in control-plane infrastructure:

- Tenant DBs remain fully isolated for file metadata.
- Global registry stores only share contracts (`share_id`, source/target tenant+cluster, mode, status, expiry).
- Authorization checks for shared paths call the global registry first, then resolve source tenant data-plane access.
- Revocation is centralized and immediate at policy level.

---

## 10. Security Model

### 10.1 Path Canonicalization

All incoming paths are normalized **before** authorization and routing. This prevents path traversal, mount boundary bypass, and encoding-based attacks.

**Canonical Path Spec**:

```
Raw input (URL-decoded once)
  → Reject if contains: NUL (\x00), control characters (\x01-\x1f), backslash (\)
  → Reject if any segment is "." or ".."
  → Collapse consecutive slashes: "///" → "/"
  → Strip trailing slash (except root "/")
  → Unicode NFC normalization
  → Result: canonical path (used for all subsequent authorization + routing)
```

**Invariant**: Authorization checks, MountableFS routing, and metadata writes all use the canonical path. Raw paths are never stored or matched against.

**Implementation note**: AGFS's `pathutil.NormalizePath()` provides `path.Clean` + leading-slash enforcement. dat9 wraps this with additional reject rules (NUL, control chars, `..` segments) as a pre-filter before the path reaches MountableFS.

### 10.2 Presigned URL Security

Presigned URLs are bearer tokens. Leakage (via logs, Referer headers, browser history) grants unauthorized S3 access. dat9 enforces:

| Control | Spec |
|---------|------|
| **TTL** | Upload: max 120 seconds. Download: max 60 seconds. |
| **Binding** | Upload URLs bind: part number, `Content-Length`, `x-amz-checksum-sha256`. Download URLs bind: S3 key only. |
| **Single-use (uploads)** | Each presigned URL is tracked in the upload session. Re-issuing URLs for the same part invalidates the previous URL's ETag expectation at `CompleteMultipartUpload` time. |
| **Log hygiene** | API gateway and application logs redact `X-Amz-Signature` and `X-Amz-Credential` query parameters. Response headers include `Cache-Control: private, no-store`. |
| **Download indirection** | `GET /v1/fs/{path}` for large files returns a **one-time ticket** (`302` to `/v1/download/{ticket}`), which exchanges for a short-lived presigned URL. The ticket expires in 30 seconds and is single-use. This keeps presigned URLs out of redirect chains and Referer headers. |

### 10.3 Cross-Tenant Share Authorization

Every read through a shared mount performs a **three-layer authorization check**:

```
Shared path read request
  │
  ├─▶ 1. Global registry check
  │     share.status = ACTIVE AND NOT expired
  │     → fail: 403 "share revoked or expired"
  │
  ├─▶ 2. Tenant binding check
  │     requester.tenant_id == share.target_tenant
  │     → fail: 403 "not authorized for this share"
  │
  └─▶ 3. Path prefix check
        requested_path starts with share.target_mount_path
        → fail: 403 "path outside share scope"
```

**Capability token** (V2 live mounts): Each shared-path read uses a short-lived token:

```
{
  "share_id": "sh_01J...",
  "target_tenant": "tenant-007",
  "target_path_prefix": "/shared/ml-papers/",
  "exp": 1711411200,          // 5-minute expiry
  "nonce": "a1b2c3d4"
}
// HMAC-SHA256 signed by control-plane key
```

**Revocation semantics**: Policy revocation in the global registry is immediate. The next read fails. Already-issued presigned URLs are bounded by their short TTL (max 60 seconds) and cannot be renewed after revocation.

### 10.4 Mount Management Permissions

The mount management API (`/v1/mounts`) is **control-plane admin only**. Tenant APIs do not expose mount operations.

| Rule | Rationale |
|------|-----------|
| Tenant API cannot call `POST /v1/mounts` or `DELETE /v1/mounts/{path}` | Prevents path-overlap attacks and arbitrary backend injection |
| Backend type allowlist: only built-in plugins (`s3meta`, `memfs`, `kvfs`) | No dynamic plugin loading from untrusted sources |
| All mount changes are audit-logged: operator, before/after mount tree, timestamp | Forensic traceability |

### 10.5 Rate Limiting and Abuse Prevention

| Control | Scope | Default |
|---------|-------|---------|
| Request rate | Per namespace | 100 req/s (configurable) |
| Upload bandwidth | Per namespace | 1 GB/hour (configurable) |
| Concurrent uploads | Per namespace per path | 1 active session (enforced by DB unique constraint) |
| Max file size | Per namespace | 100 GB (configurable, enforced at `PUT` via `Content-Length` pre-check) |
| Upload session TTL | Per upload | 24 hours (Reaper aborts expired sessions) |

---

## 11. Cost Controls

| Strategy | Implementation |
|---|---|
| TTL expiration | `expires_at` column + Reaper |
| Storage tiering | S3 Intelligent-Tiering (automatic hot/cold) |
| Cold data archive | Lifecycle rule: 7d → Glacier Instant Retrieval |
| Incomplete upload cleanup | Reaper `AbortMultipartUpload` + S3 Lifecycle `AbortIncompleteMultipartUpload` |

---

## 12. Client Access Methods

| Method | Large File Data Path | User Experience |
|---|---|---|
| **CLI** | Client → S3 direct | `dat9 cp ./big.bin /data/` (one command, progress bar, auto-resume) |
| **CLI Shell** | Client → S3 direct | `dat9 sh` (interactive prompt for `cp/cat/ls`) |
| **Go SDK** | SDK process → S3 direct | `client.Write("/data/big.bin", reader)` (one method) |
| **Python SDK** | SDK process → S3 direct | `client.write("/data/big.bin", data)` (one method) |
| **MCP Tools** | MCP server → S3 direct | `dat9_write("/data/big.bin", content)` (one tool call) |
| **FUSE** | FUSE daemon → S3 direct | `cp big.bin /mnt/dat9/data/` (standard POSIX) |
| **curl** | Manual (small files only) | `curl -X PUT .../v1/fs/path -d @file` |

All methods keep large file data off the server.

---

## 13. Roadmap

| Phase | Scope | Effort |
|---|---|---|
| **P0** | Server: AGFS MountableFS + S3MetaBackend (content-addressed `blobs/<ulid>`) + `files` table (merged path+metadata) + small-file CRUD + auth + namespace | M |
| **P1** | Large-file upload: 202 flow + presigned URLs + `uploads` table + resume + Go SDK Transfer Engine | L |
| **P2** | CLI: `dat9 cp/cat/ls/stat/mv/rm` + progress bar + auto-resume | M |
| **P3** | Reaper + S3 Lifecycle + TTL cleanup | S |
| **P4** | `file_tags` table + tag CRUD API + Query API (`POST /v1/query`) + server-side copy | M |
| **P5** | MCP Server | S |
| **P6** | Python SDK | M |
| **P7** | Server-side grep/digest (small files) + mount management API | M |
| **P8** | Semantic processing pipeline: `context_layers` table + async L0/L1 generation (LLM-powered, bottom-up aggregation, `.relations.json` auto-generation, semantic queue) | L |
| **P9** | Vector index integration: embedding, upsert, `/v1/search` + `/v1/find` endpoints, hierarchical retrieval, tags dual-write to vector DB | L |
| **P10** | Smart Parser & TreeBuilder: content-aware file parsing (PDF→Markdown splitting, heading-based chunking), automatic categorization and path assignment (inspired by OpenViking's ingestion pipeline) | L |
| **P11** | FUSE mount (HTTP-backed + cache layers, reuse agfs-fuse patterns) | L |

---

## 14. Open Questions

| Question | Options | Leaning |
|---|---|---|
| Small/large file threshold | 8 MB / 10 MB / 64 MB | 10 MB |
| Part size | Fixed 64 MB / dynamic | Dynamic (8-256 MB based on file size) |
| DB backend | TiDB / PostgreSQL / SQLite | Start with SQLite, graduate to TiDB |
| Object store | AWS S3 / MinIO / R2 | S3 for cloud, MinIO for on-prem |
| Multiple clients uploading same path | Reuse existing session / 409 Conflict / LWW | Reuse existing session |
| Upload conflict policy | Reuse session if same file fingerprint / 409 if different | Reuse session if same file fingerprint, 409 if different |
| File versioning | None / simple version chain | None for P0, reserve `previous_file_id` in schema |
| Change notifications | None / polling / WebSocket / webhook | Polling (`GET /v1/fs/?changes_since=<cursor>`) for P0 |
| Vector DB backend | Local (SQLite+HNSW) / Qdrant / VikingDB / pgvector | Start local, graduate to managed |
| Embedding model | OpenAI / local (e5-small) / configurable | Configurable, default to local for cost |
| Semantic queue implementation | DB table polling / Redis / SQS | DB table for P0 simplicity |

---

## Appendix A. Future Extensions

### Server-side grep/digest (small files only)

```
POST   /v1/fs/{path}?grep     Server-side search (small files only)
POST   /v1/fs/{path}?digest   Server-side hash (small files only)
```

### Mount Management

```
GET    /v1/mounts              List mounted backends
POST   /v1/mounts              Mount a new backend (runtime)
DELETE /v1/mounts/{path}       Unmount
```

P0 ships with two mounts: `/ -> S3MetaBackend` and `/mem -> memfs`. Future phases add `kvfs`, etc.

### Smart Parser & TreeBuilder (P10, OpenViking-inspired)

In P0-P9, users specify file paths directly (`dat9 cp ./file.pdf /data/papers/file.pdf`). Starting P10, dat9 adds an optional **ingestion pipeline** inspired by OpenViking's Parser/TreeBuilder architecture:

```
User uploads file (path unspecified or /inbox/)
  │
  ├─▶ Parser
  │     - Content-type dispatch: PDF, Markdown, HTML, ...
  │     - Split strategies: heading-based, token-budget, semantic
  │     - Example: 50-page PDF → 12 Markdown sections (threshold: 1024 tokens each)
  │     - Original file preserved as-is (dat9 != OpenViking: we keep originals)
  │
  ├─▶ TreeBuilder
  │     - Determine target path from content analysis + user-defined rules
  │     - Collision detection: append suffix if path exists
  │     - Example: /inbox/paper.pdf → /papers/2026/transformer-survey/paper.pdf
  │     - Parsed sections → /papers/2026/transformer-survey/sections/*.md
  │
  ├─▶ Atomic move
  │     - UPDATE files SET path = <final_path>  (zero S3 cost, content-addressed keys)
  │     - Generate L0/L1 for new directory
  │     - Update .relations.json with source → parsed links
  │
  └─▶ 200 OK { original: "/papers/.../paper.pdf", sections: [...] }
```

**Key difference from OpenViking**: dat9 **always preserves the original file**. OpenViking discards originals after parsing (PDF → Markdown, original gone). dat9 keeps both the original and parsed sections, linked via `.relations.json`. This is critical for agent workflows where the original format matters (e.g., sending a PDF attachment).

The content-addressed S3 key strategy (`blobs/<ulid>`) makes the atomic move step zero-cost — only the `files.path` column changes, no S3 copies needed.

---

## References

- **OpenViking**: https://github.com/volcengine/OpenViking --- Context database for AI agents. Tiered storage (L0/L1/L2) design reference.
- **AGFS**: https://github.com/c4pt0r/agfs --- Plan 9-inspired agent filesystem. We import its core interfaces.
- **Git LFS Batch API**: https://github.com/git-lfs/git-lfs/blob/main/docs/api/batch.md --- Control-plane upload pattern reference.
- **GCS Resumable Uploads**: https://docs.google.com/storage/docs/resumable-uploads --- Resume semantics reference.
- **S3 Multipart Upload**: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- **rclone**: https://rclone.org/ --- CLI UX benchmark (progress bar, auto-retry, multipart).
- **db9 fs**: https://db9.ai/ --- Agent file-sharing tool. CLI UX reference (`<db>:/path` syntax).
