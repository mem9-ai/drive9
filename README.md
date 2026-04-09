# drive9

> **Note**: This project was formerly known as `dat9`. The Go module path is being migrated in a separate PR.

Agent-native data infrastructure — a network drive with built-in semantic search.

drive9 presents a single filesystem-like interface for storing, retrieving, and querying data of any kind. Agents (or humans) interact with drive9 the same way they interact with a local filesystem: `cp`, `cat`, `ls`, `mv`, `rm`, `search`. All protocol complexity — tiered storage, embedding, full-text indexing — is invisible to the user.

## Why drive9?

- **Agent tool fragmentation**: each agent tool uses its own storage semantics and credentials. drive9 unifies them under one path namespace.
- **Server bandwidth bottlenecks**: proxying large uploads is slow and expensive. drive9 uses S3 presigned URLs for direct upload — the server never touches large file data.
- **Missing semantic discoverability**: files exist, but cannot be found by meaning. drive9 leverages [db9](https://db9.ai/)'s built-in embedding and vector search.
- **No unified abstraction across storage tiers**: drive9 provides one path namespace spanning db9 (small files, instant, auto-embedded) and S3 (large files, cheap, unlimited).

## Quick Start

### CLI

```bash
# Upload a file
drive9 cp ./dataset.tar :/data/dataset.tar

# Read a file
drive9 cat :/config/settings.json

# List directory
drive9 ls :/data/

# Zero-copy link (no re-upload)
drive9 cp :/data/a.bin :/shared/a.bin

# Rename (metadata-only, zero storage cost)
drive9 mv :/data/old.bin :/data/new.bin

# Start the server
drive9-server

# Mount as local filesystem
drive9 mount /mnt/drive9

# Unmount
drive9 umount /mnt/drive9
```

### FUSE Mount

Mount drive9 as a local filesystem — use `ls`, `cat`, `vim`, `cp` directly on your drive9 data.

```bash
drive9 mount /mnt/drive9
```

#### Prerequisites

FUSE mount requires a platform-specific FUSE provider:

**macOS**

Install [macFUSE](https://osxfuse.github.io/):

```bash
brew install --cask macfuse
```

On Apple Silicon, you may need to approve the macFUSE system extension in **System Settings > Privacy & Security** and reboot before mounts will work.

**Linux**

Install FUSE 3 (or FUSE 2) via your package manager:

```bash
# Debian / Ubuntu
sudo apt-get install fuse3

# RHEL / CentOS / Fedora
sudo dnf install fuse3

# Arch
sudo pacman -S fuse3
```

Ensure your user is in the `fuse` group (or use `allow_other` mount option with `/etc/fuse.conf`).

**Windows**

Not currently supported. Contributions welcome via [WinFsp](https://winfsp.dev/).

#### Mount Options

```
drive9 mount [flags] <mountpoint>

  --server       drive9 server URL (default: $DRIVE9_SERVER)
  --api-key      API key (default: $DRIVE9_API_KEY)
  --cache-size   read cache size in MB (default: 128)
  --dir-ttl      directory cache TTL (default: 5s)
  --attr-ttl     kernel attr cache TTL (default: 1s)
  --entry-ttl    kernel entry cache TTL (default: 1s)
  --allow-other  allow other users to access mount
  --read-only    mount as read-only
  --debug        enable FUSE debug logging
```

### Go SDK

```go
import "github.com/mem9-ai/drive9/pkg/client"

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
| `DRIVE9_SERVER` | Server URL | `http://localhost:9009` |
| `DRIVE9_API_KEY` | API key | |
| `DRIVE9_LISTEN_ADDR` | Server listen address | `:9009` |
| `DRIVE9_PUBLIC_URL` | Externally reachable base URL (required for remote clients) | |
| `DRIVE9_META_DSN` | Control-plane MySQL DSN for the multi-tenant server | |
| `DRIVE9_S3_BUCKET` | S3 bucket name (enables AWS S3 mode; omit for local mock) | |
| `DRIVE9_S3_REGION` | AWS region | `us-east-1` |
| `DRIVE9_S3_PREFIX` | S3 key prefix (e.g. `tenants/abc/`) | |
| `DRIVE9_S3_ROLE_ARN` | IAM role ARN to assume via STS | |
| `DRIVE9_S3_DIR` | Local S3 mock directory (only used without `DRIVE9_S3_BUCKET`) | `./s3` |
| `DRIVE9_TENANT_PROVIDER` | Tenant provisioner: `db9`, `tidb_zero`, or `tidb_cloud_starter` | `tidb_zero` |
| `DRIVE9_MAX_UPLOAD_BYTES` | Maximum allowed upload size in bytes | `53687091200` |
| `DRIVE9_MAX_TENANT_STORAGE_BYTES` | Maximum logical storage per tenant in bytes | `53687091200` |
| `DRIVE9_ZERO_API_URL` | TiDB Zero provision API base URL | |
| `DRIVE9_TIDBCLOUD_API_URL` | TiDB Cloud Starter API base URL | |
| `DRIVE9_TIDBCLOUD_API_KEY` | TiDB Cloud Starter API key | |
| `DRIVE9_TIDBCLOUD_API_SECRET` | TiDB Cloud Starter API secret | |
| `DRIVE9_TIDBCLOUD_POOL_ID` | TiDB Cloud Starter pool id | |
| `DRIVE9_DB9_API_URL` | db9 provision API base URL | |
| `DRIVE9_DB9_API_KEY` | db9 provision API key | |
| `DRIVE9_ENCRYPT_TYPE` | Encryption backend: `local_aes` or `kms` | `local_aes` |
| `DRIVE9_MASTER_KEY` | 32-byte hex key for `local_aes` encryption | |
| `DRIVE9_ENCRYPT_KEY` | KMS key id or alias when `DRIVE9_ENCRYPT_TYPE=kms` | |
| `DRIVE9_TOKEN_SIGNING_KEY` | 32-byte hex key for JWT API key signing | |
| `DRIVE9_QUERY_EMBED_API_BASE` | OpenAI-compatible base URL for query embedding | |
| `DRIVE9_QUERY_EMBED_API_KEY` | API key for query embedding | |
| `DRIVE9_QUERY_EMBED_MODEL` | Model name for query embedding | |
| `DRIVE9_QUERY_EMBED_DIMENSIONS` | Optional query embedding dimensions override | |
| `DRIVE9_QUERY_EMBED_TIMEOUT_SECONDS` | Query embedding timeout seconds | `20` |
| `DRIVE9_EMBED_API_BASE` | OpenAI-compatible base URL for background embedding | |
| `DRIVE9_EMBED_API_KEY` | API key for background embedding | |
| `DRIVE9_EMBED_MODEL` | Model name for background embedding | |
| `DRIVE9_EMBED_DIMENSIONS` | Optional background embedding dimensions override | |
| `DRIVE9_EMBED_TIMEOUT_SECONDS` | Background embedding timeout seconds | `20` |
| `DRIVE9_SEMANTIC_WORKERS` | Number of semantic workers | `1` |
| `DRIVE9_SEMANTIC_POLL_INTERVAL_MS` | Semantic worker poll interval in milliseconds | `200` |
| `DRIVE9_SEMANTIC_LEASE_SECONDS` | Semantic task lease duration in seconds | `30` |
| `DRIVE9_SEMANTIC_RECOVER_INTERVAL_MS` | Expired semantic task recover interval in milliseconds | `5000` |
| `DRIVE9_SEMANTIC_RETRY_BASE_MS` | Base semantic retry backoff in milliseconds | `200` |
| `DRIVE9_SEMANTIC_RETRY_MAX_MS` | Max semantic retry backoff in milliseconds | `30000` |
| `DRIVE9_SEMANTIC_TENANT_LIMIT` | Active tenants scanned per round | `128` |
| `DRIVE9_SEMANTIC_PER_TENANT_CONCURRENCY` | Max concurrent semantic tasks per tenant | `1` |
| `DRIVE9_IMAGE_EXTRACT_ENABLED` | Enable async image->text extraction for search | `false` |
| `DRIVE9_IMAGE_EXTRACT_QUEUE_SIZE` | In-memory queue size for extraction tasks | `128` |
| `DRIVE9_IMAGE_EXTRACT_WORKERS` | Number of extraction workers | `1` |
| `DRIVE9_IMAGE_EXTRACT_MAX_BYTES` | Max image bytes processed per task | `8388608` |
| `DRIVE9_IMAGE_EXTRACT_TIMEOUT_SECONDS` | Timeout per extraction task | `20` |
| `DRIVE9_IMAGE_EXTRACT_MAX_TEXT_BYTES` | Max extracted text stored into `files.content_text` | `8192` |
| `DRIVE9_IMAGE_EXTRACT_API_BASE` | OpenAI-compatible base URL (optional; set with key/model) | |
| `DRIVE9_IMAGE_EXTRACT_API_KEY` | API key for image extraction provider | |
| `DRIVE9_IMAGE_EXTRACT_MODEL` | Vision model name (for example Qwen VL model id) | |
| `DRIVE9_IMAGE_EXTRACT_PROMPT` | Custom extraction prompt | `用中文描述这张图片，用于文件搜索。包括：主要物体、场景描述、图中可见文字（OCR）、简洁标签。最后一行用英文写5-10个关键词标签（English tags），用逗号分隔。` |
| `DRIVE9_IMAGE_EXTRACT_MAX_TOKENS` | Max output tokens for model extraction | `256` |
| `DRIVE9_CLI_LOG_ENABLED` | Enable CLI structured log file output, including benchmark-consumable transfer summary events | `false` |
| `DRIVE9_CLI_LOG_MAX_SIZE_MB` | CLI log rotation max size in MB | `10` |
| `DRIVE9_CLI_LOG_MAX_BACKUPS` | CLI log rotation max backups | `5` |
| `DRIVE9_CLI_LOG_MAX_AGE_DAYS` | CLI log rotation max age in days | `14` |
| `DRIVE9_TEST_MYSQL_DSN` | MySQL/TiDB DSN reused by test suites (example: `user:pass@tcp(127.0.0.1:3306)/drive9_test?parseTime=true`) | |
| `DRIVE9_LOCAL_DSN` | Local single-tenant datastore DSN for `drive9-server-local` | |
| `DRIVE9_LOCAL_INIT_SCHEMA` | Initialize local tenant schema on startup | `false` |
| `DRIVE9_LOCAL_EMBEDDING_MODE` | Local embedding mode: `auto`, `app`, or `detect` | `detect` |

## Architecture

```
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│   CLI    │  │ Go SDK   │  │   FUSE   │  │   MCP    │
│ drive9 cp  │  │ client   │  │drive9 mount│  │ (future) │
└────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
     └─────────────┼──────────────┼──────────────┘
                   ▼              │
         drive9 HTTP Server        │
         /v1/fs/{path}           │
                   │         ┌───┘
     ┌─────────────┼─────────┤
     ▼             ▼         ▼
  Drive9Backend   memfs     S3 direct
  (AGFS FileSystem)       (presigned URL,
     │                     FUSE ↔ S3)
     ├── < 50,000B → TiDB(MySQL protocol) + local blobs (P0)
     │               db9 + fs9 (production)
     │
     └── ≥ 50,000B → S3 presigned URL
                  (direct upload, server not involved)
```

### Key Design Decisions

**inode model** — Paths (file_nodes) and file entities (files) are separate, just like Unix. One file can appear at multiple paths. `cp` is a zero-cost link. `mv` is metadata-only. `rm` is reference-counted.

**Tiered storage** — Small files (< 50,000 bytes) go to db9 with automatic embedding and FTS indexing. Large files (≥ 50,000 bytes) go directly to S3 via presigned URLs. One path namespace spans both.

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
  Header: X-Drive9-Copy-Source: /source/path

POST   /v1/fs/{path}?rename       Rename/move
  Header: X-Drive9-Rename-Source: /old/path

POST   /v1/fs/{path}?mkdir        Create directory
```

## Project Structure

```
cmd/drive9/         CLI entrypoint and commands (cp, cat, ls, mount, umount, ...)
cmd/drive9-server/    Server entrypoint
pkg/
  backend/          Drive9Backend — AGFS FileSystem implementation
  client/           Go SDK HTTP client
  datastore/        Core metadata store and semantic task persistence
  embedding/        Embedding provider integration and vector helpers
  encrypt/          Encryption helpers
  fuse/             FUSE mount (go-fuse/v2 RawFileSystem, inode mapping, caching)
  logger/           Structured logging helpers
  meta/             Metadata/search-facing models and helpers
  metrics/          Metrics recording
  semantic/         Durable semantic task types and contracts
  s3client/         S3-compatible object store interface (AWS + local mock)
  server/           HTTP server (/v1/fs/{path} router)
  tenant/           Tenant schema management and embedding mode detection
  pathutil/         Path canonicalization and validation
  parser/           Content-aware parsing interface (future)
  traceid/          Trace ID helpers
  treebuilder/      Parsed content → path namespace mapping (future)
docs/
  design-overview.md  Full design document
```

## Metadata Schema

Five core tables, all in the tenant's database:

| Table | Purpose |
|---|---|
| `file_nodes` | Path tree (dentry) — path, parent, name, directory flag, file_id reference |
| `files` | File entity (inode) — storage location, size, checksum, revision, lifecycle status, extracted text, embedding state |
| `file_tags` | Key-value tags for precise filtering |
| `uploads` | Large-file multipart upload state and idempotency tracking |
| `semantic_tasks` | Durable background work queue for async embedding and image text extraction |

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| **P0** | Server + Drive9Backend + metadata + small-file CRUD + auth | ✅ Done |
| **P1** | Large-file upload: 202 flow + presigned URLs + resume | Planned |
| **P2** | CLI: full command set + progress bar + auto-resume | In Progress |
| **P3** | Reaper + S3 Lifecycle + TTL cleanup | Planned |
| **P4** | Tags + Query API + zero-copy cp | Planned |
| **P5** | MCP Server | Planned |
| **P6** | Python SDK | Planned |
| **P7** | Server-side grep/digest | Planned |
| **P8** | Async L0/L1 generation (LLM-powered) | Planned |
| **P9** | Smart Parser & TreeBuilder | Planned |
| **P10** | FUSE mount | ✅ Done |

## Building

```bash
mkdir -p bin
go build -o bin/drive9 ./cmd/drive9
go build -o bin/drive9-server ./cmd/drive9-server
```

## Local Validation Server

For single-tenant local validation of async embedding and search correctness, use
the dedicated local entrypoint plus the repository-owned env script:

```bash
source ./scripts/drive9-server-local-env.sh
export DRIVE9_LOCAL_INIT_SCHEMA=true   # only for disposable local databases
make run-server-local
```

The helper script keeps `DRIVE9_LOCAL_INIT_SCHEMA=false` by default so the local
entrypoint does not mutate an existing database unless you opt in explicitly.

Override any variables you need before `make run-server-local`, for example a
custom local/remote TiDB DSN:

```bash
export DRIVE9_LOCAL_DSN='root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true'
export DRIVE9_LOCAL_INIT_SCHEMA=true   # only if this is a disposable database
make run-server-local
```

The helper script sets sensible defaults for local validation, including:

- `DRIVE9_LOCAL_DSN`
- `DRIVE9_LOCAL_INIT_SCHEMA=false`
- local mock S3 via `DRIVE9_S3_DIR`
- local Ollama-compatible `DRIVE9_EMBED_*` defaults
- query embedding reusing `DRIVE9_EMBED_*` unless `DRIVE9_QUERY_EMBED_*` is set explicitly

## Running Tests

```bash
make test
```

`make test` is the standard test entrypoint and runs `go test ./...`.

For MySQL-backed test suites:

- if `DRIVE9_TEST_MYSQL_DSN` is set, the tests reuse that MySQL instance
- otherwise, if `podman` is available locally, `make test` automatically configures the Podman-backed testcontainers environment
- otherwise, testcontainers uses the default Docker-compatible runtime environment

For example, to reuse an existing local MySQL instance:

```bash
DRIVE9_TEST_MYSQL_DSN='drive9:drive9pass@tcp(127.0.0.1:3306)/drive9_test?parseTime=true' make test
```

If you do not provide `DRIVE9_TEST_MYSQL_DSN`, make sure a Docker-compatible container runtime is available locally.

## References

- [db9](https://db9.ai/) — Serverless database with built-in embedding, FTS, vector search, and fs9 file storage
- [AGFS](https://github.com/c4pt0r/agfs) — Plan 9-inspired agent filesystem (we import its core interfaces)
- [OpenViking](https://github.com/volcengine/OpenViking) — Context database for AI agents (L0/L1/L2 design reference)

## License

Apache 2.0
