# dat9

Agent-native data infrastructure — a network drive with built-in semantic search.

dat9 presents a single filesystem-like interface for storing, retrieving, and querying data of any kind. Agents (or humans) interact with dat9 the same way they interact with a local filesystem: `cp`, `cat`, `ls`, `mv`, `rm`, `search`. All protocol complexity — tiered storage, embedding, full-text indexing — is invisible to the user.

## Why dat9?

- **Agent tool fragmentation**: each agent tool uses its own storage semantics and credentials. dat9 unifies them under one path namespace.
- **Server bandwidth bottlenecks**: proxying large uploads is slow and expensive. dat9 uses S3 presigned URLs for direct upload — the server never touches large file data.
- **Missing semantic discoverability**: files exist, but cannot be found by meaning. dat9 leverages [db9](https://db9.ai/)'s built-in embedding and vector search.
- **No unified abstraction across storage tiers**: dat9 provides one path namespace spanning db9 (small files, instant, auto-embedded) and S3 (large files, cheap, unlimited).

## Quick Start

### CLI

```bash
# Upload a file
dat9 cp ./dataset.tar :/data/dataset.tar

# Read a file
dat9 cat :/config/settings.json

# List directory
dat9 ls :/data/

# Zero-copy link (no re-upload)
dat9 cp :/data/a.bin :/shared/a.bin

# Rename (metadata-only, zero storage cost)
dat9 mv :/data/old.bin :/data/new.bin

# Start the server
dat9-server
```

### Go SDK

```go
import "github.com/mem9-ai/dat9/pkg/client"

c := client.New("http://localhost:9009", "")

// Write
c.Write("/data/file.txt", []byte("hello world"))

// Read
data, _ := c.Read("/data/file.txt")

// List
entries, _ := c.List("/data/")

// Zero-copy
c.Copy("/data/file.txt", "/shared/file.txt")

// Stat
info, _ := c.Stat("/data/file.txt")

// Rename
c.Rename("/data/old.txt", "/data/new.txt")

// Delete
c.Delete("/data/file.txt")
```

### Environment Variables

| Variable | Description | Default |
|---|---|---|
| `DAT9_SERVER` | Server URL | `http://localhost:9009` |
| `DAT9_API_KEY` | API key | |
| `DAT9_LISTEN_ADDR` | Server listen address | `:9009` |
| `DAT9_MYSQL_DSN` | MySQL DSN (example: `user:pass@tcp(127.0.0.1:3306)/dat9?parseTime=true`) | |
| `DAT9_BLOB_DIR` | Blob storage directory | `./blobs` |

## Architecture

```
┌──────────┐  ┌──────────┐  ┌──────────┐
│   CLI    │  │ Go SDK   │  │ MCP/FUSE │
│ dat9 cp  │  │ client   │  │ (future) │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     └─────────────┼──────────────┘
                   ▼
         dat9 HTTP Server
         /v1/fs/{path}
                   │
     ┌─────────────┼─────────────┐
     ▼             ▼             ▼
  Dat9Backend   memfs        (plugins)
  (AGFS FileSystem)
     │
     ├── < 1MB → TiDB(MySQL protocol) + local blobs (P0)
     │            db9 + fs9 (production)
     │
     └── ≥ 1MB → S3 presigned URL
                  (direct upload, server not involved)
```

### Key Design Decisions

**inode model** — Paths (file_nodes) and file entities (files) are separate, just like Unix. One file can appear at multiple paths. `cp` is a zero-cost link. `mv` is metadata-only. `rm` is reference-counted.

**Tiered storage** — Small files (< 1MB) go to db9 with automatic embedding and FTS indexing. Large files (≥ 1MB) go directly to S3 via presigned URLs. One path namespace spans both.

**Tiered context (L0/L1/L2)** — Every directory can carry `.abstract.md` (~100 tokens) and `.overview.md` (~1k tokens). Agents scan cheaply via L0/L1 before loading full content (L2). 10x token savings.

**Built on AGFS** — Imports [AGFS](https://github.com/c4pt0r/agfs)'s `FileSystem` interface and `MountableFS` radix-tree router as a Go module dependency.

## HTTP API

All file operations go through `/v1/fs/{path}`:

```
PUT    /v1/fs/{path}              Write file
GET    /v1/fs/{path}              Read file
GET    /v1/fs/{path}?list         List directory
HEAD   /v1/fs/{path}              Stat (metadata)
DELETE /v1/fs/{path}              Delete
DELETE /v1/fs/{path}?recursive    Delete recursively

POST   /v1/fs/{path}?copy         Zero-copy link
  Header: X-Dat9-Copy-Source: /source/path

POST   /v1/fs/{path}?rename       Rename/move
  Header: X-Dat9-Rename-Source: /old/path

POST   /v1/fs/{path}?mkdir        Create directory
```

## Project Structure

```
cmd/dat9/           CLI entrypoint and commands
cmd/dat9-server/    Server entrypoint
pkg/
  backend/          Dat9Backend — AGFS FileSystem implementation (inode model)
  client/           Go SDK HTTP client
  meta/             Metadata store (TiDB/MySQL P0 / db9 production)
  server/           HTTP server (/v1/fs/{path} router)
  pathutil/         Path canonicalization and validation
  parser/           Content-aware parsing interface (future)
  treebuilder/      Parsed content → path namespace mapping (future)
docs/
  design-overview.md  Full design document
```

## Metadata Schema

Four tables, all in the tenant's database:

| Table | Purpose |
|---|---|
| `file_nodes` | Path tree (dentry) — path, parent, name, file_id reference |
| `files` | File entity (inode) — storage type/ref, size, checksum, revision, status, content_text |
| `file_tags` | Key-value tags for precise SQL filtering |
| `uploads` | Large-file S3 multipart upload state tracking |

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| **P0** | Server + Dat9Backend + metadata + small-file CRUD + auth | ✅ Done |
| **P1** | Large-file upload: 202 flow + presigned URLs + resume | Planned |
| **P2** | CLI: full command set + progress bar + auto-resume | In Progress |
| **P3** | Reaper + S3 Lifecycle + TTL cleanup | Planned |
| **P4** | Tags + Query API + zero-copy cp | Planned |
| **P5** | MCP Server | Planned |
| **P6** | Python SDK | Planned |
| **P7** | Server-side grep/digest | Planned |
| **P8** | Async L0/L1 generation (LLM-powered) | Planned |
| **P9** | Smart Parser & TreeBuilder | Planned |
| **P10** | FUSE mount | Planned |

## Building

```bash
go build -o dat9 ./cmd/dat9
go build -o dat9-server ./cmd/dat9-server
```

## Running Tests

```bash
go test ./...
```

## References

- [db9](https://db9.ai/) — Serverless database with built-in embedding, FTS, vector search, and fs9 file storage
- [AGFS](https://github.com/c4pt0r/agfs) — Plan 9-inspired agent filesystem (we import its core interfaces)
- [OpenViking](https://github.com/volcengine/OpenViking) — Context database for AI agents (L0/L1/L2 design reference)

## License

Apache 2.0
