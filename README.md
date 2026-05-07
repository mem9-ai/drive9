# drive9

drive9 is a network drive for agent sandboxes and human workflows. Mount it,
use normal filesystem tools, and keep the backing storage, upload protocol,
cache invalidation, and semantic indexing behind one path namespace.

```bash
drive9 mount :/ ~/drive9
git clone https://github.com/mem9-ai/drive9.git ~/drive9/drive9
mkdir -p ~/drive9/notes
cp notes.md ~/drive9/notes/
drive9 fs grep "pricing" :/notes
```

The main implementation is a Go HTTP server plus CLI, SDK, FUSE mount, WebDAV
adapter, tenant provisioning, object storage integration, and semantic workers.
The current storage path is **TiDB + S3-compatible object storage**. db9 remains
a provider/inspiration path in the codebase, but it is not the default storage
backend described by the current production architecture.

## What It Does

**One path namespace.** Files live at paths such as `:/src/main.go` or
`:/datasets/run.bin`. The same API is exposed through CLI commands, the Go SDK,
FUSE mounts, and HTTP endpoints.

**TiDB metadata and small-file storage.** Tenant databases store the inode-style
path tree, file metadata, tags, uploads, semantic task state, inline small-file
bytes, full-text indexes, and vector indexes. TiDB tenants use the MySQL
protocol.

**Object storage for large bytes.** Files at or above 50,000 bytes use
S3-compatible object storage through presigned multipart uploads. The drive9
server coordinates metadata and upload state; it does not proxy large object
bytes.

**FUSE for agent workspaces.** `drive9 mount` provides a local filesystem with
read caching, adaptive range prefetch, writeback through ShadowStore + Journal +
CommitQueue, SSE-driven invalidation, and Git lockfile/rename semantics needed
by real `git clone` workflows.

**Semantic retrieval.** drive9 stores searchable `content_text` and
`description` fields. TiDB auto-embedding tenants derive vector columns in the
database from that text. Optional image/audio extraction workers can produce
text for media files; app-managed embedding remains available for non-auto
embedding paths.

**Zero-copy namespace operations.** File copies inside drive9 can be metadata
links. Rename updates path metadata. Object bytes are garbage-collected after
the last path reference is removed.

## Quick Start

### Build

```bash
go build -o bin/drive9 ./cmd/drive9
go build -o bin/drive9-server ./cmd/drive9-server
go build -o bin/drive9-server-local ./cmd/drive9-server-local
```

### Configure A Client

Use an existing server:

```bash
drive9 ctx add --name prod --server https://api.drive9.ai --api-key "$DRIVE9_API_KEY"
drive9 ctx use prod
```

Or provision a tenant when the server is configured for tenant creation:

```bash
drive9 create --name dev --server https://api.drive9.ai
drive9 ctx use dev
```

### Tenant API Key Commands

The provisioned `default` API key can manage additional tenant API keys:

```bash
drive9 api-key ls

drive9 api-key create worker --json
drive9 api-key get worker
drive9 api-key rm worker
```

### Filesystem Commands

```bash
drive9 fs cp ./data.tar :/data/data.tar
drive9 fs cp --append ./tail.log :/logs/app.log
drive9 fs cp --tag topic=pricing --tag owner=agent ./plan.md :/notes/plan.md
drive9 fs cp --description "deployment notes" ./runbook.md :/docs/runbook.md

drive9 fs cat :/docs/runbook.md
drive9 fs ls -l :/docs/
drive9 fs stat -o json :/docs/runbook.md

drive9 fs grep "pricing" :/
drive9 fs find :/notes -tag topic=pricing
drive9 fs find :/notes -tag owner

drive9 fs cp :/a.bin :/b.bin
drive9 fs mv :/old.bin :/new.bin
drive9 fs mkdir :/new-dir
drive9 fs rm :/old.bin
drive9 fs rm -r :/old-dir
```

### Mount

```bash
mkdir -p ~/drive9
drive9 mount :/ ~/drive9 --debug

# In another shell:
git clone https://github.com/mem9-ai/drive9.git ~/drive9/drive9

drive9 umount ~/drive9
```

The mount command accepts an optional remote root:

```bash
drive9 mount :/projects/my-agent ./work
```

### Tag Filter Semantics

`drive9 fs find -tag` uses exact matching:

- `-tag key=value` matches files where both tag key and tag value are equal.
- `-tag key` matches files that contain that tag key.
- Prefix, contains, and regex matching are not supported for `-tag`.

## FUSE Prerequisites

**macOS**: install macFUSE.

```bash
brew install --cask macfuse
```

On Apple Silicon, approve the system extension in System Settings and reboot if
macOS asks for it.

**Linux**: install FUSE 3.

```bash
sudo apt-get install fuse3
# or: sudo dnf install fuse3
# or: sudo pacman -S fuse3
```

## Go SDK

The Go module path is currently `github.com/mem9-ai/dat9`.

```go
package main

import (
	"log"

	"github.com/mem9-ai/dat9/pkg/client"
)

func main() {
	c := client.New("https://api.drive9.ai", "your-api-key")

	if err := c.Write("/data/file.txt", []byte("hello")); err != nil {
		log.Fatal(err)
	}

	data, err := c.Read("/data/file.txt")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("read %d bytes", len(data))

	entries, err := c.List("/data/")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("entries: %d", len(entries))

	if err := c.Copy("/data/file.txt", "/data/file-copy.txt"); err != nil {
		log.Fatal(err)
	}
	if err := c.Rename("/data/file-copy.txt", "/data/file-renamed.txt"); err != nil {
		log.Fatal(err)
	}
	if err := c.Delete("/data/file-renamed.txt"); err != nil {
		log.Fatal(err)
	}
}
```

## Architecture

```text
             +--------------------------------------+
             |             path namespace           |
             |   :/src/main.go   :/data/run.bin     |
             +------------------+-------------------+
                                |
          +---------------------+----------------------+
          |                    |                       |
   +------v------+     +-------v------+        +-------v------+
   |     CLI     |     |     FUSE     |        |    Go SDK    |
   | drive9 fs   |     | drive9 mount |        | pkg/client   |
   +------+------+     +-------+------+        +-------+------+
          |                    |                       |
          +--------------------+-----------------------+
                               |
                    drive9 HTTP server
                    /v1/fs, /v2/uploads, /v1/events
                               |
          +--------------------+----------------------+
          |                                           |
   < 50KB |                                    >= 50KB |
          v                                           v
   +-------------------+                      +-------------------+
   | TiDB tenant DB    |                      | S3-compatible     |
   | metadata, inline  |                      | object storage    |
   | bytes, FTS, vec   |                      | presigned upload  |
   +-------------------+                      +-------------------+
```

### Tenant Databases

Current tenant providers are selected explicitly:

```text
DRIVE9_TENANT_PROVIDER=db9 | tidb_zero | tidb_cloud_starter
```

- `tidb_zero`: default development/provisioning path backed by TiDB Zero.
- `tidb_cloud_starter`: production-oriented TiDB Cloud Starter pool takeover.
- `db9`: supported provider path with its own schema, but not the default
  architecture used in this README.

The server also requires a control-plane metadata database:

```text
DRIVE9_META_DSN=<mysql-compatible-dsn>
```

### Storage Model

drive9 separates paths from file entities, like a Unix dentry/inode model:

- `file_nodes`: path tree entries, parent/name, directory flag, file reference
- `files`: file entity, storage location, size, checksum, revision, search text
- `file_tags`: key-value tags
- `uploads`: multipart upload state
- `semantic_tasks`: durable async work for extraction / semantic processing
- `llm_usage`: usage accounting for semantic workers

TiDB auto-embedding tenants use generated vector columns from `content_text` and
`description`. App-managed embedding paths keep revision-aware vector state in
application-managed columns.

### Upload Protocol

Small files use direct `PUT /v1/fs/{path}` and may be stored inline in the tenant
database. Large files use object storage:

- **v1 uploads**: initiate upload, presign all parts, upload parts, complete.
- **v2 uploads**: initiate upload, presign parts on demand or in batches, upload
  parts, complete. The FUSE streaming write path uses this mode.

Object storage can be AWS S3 or the local S3-compatible mock used by
`drive9-server-local`. S3 server-side encryption is configurable with
`none`, `sse-s3`, `sse-kms`, or `dsse-kms`.

### FUSE Write Path

The FUSE layer classifies writes at runtime:

| Mode | Condition | Memory | Upload |
|---|---|---|---|
| Small file | `< 50,000` bytes | O(size) | direct commit after close/debounce |
| Sequential append | new append-only file | bounded by part buffers | streaming multipart during `write()` |
| Non-sequential new file | back-write detected | shadow/spill backed | upload at flush/release |
| Existing file edit | opened without truncate | dirty parts only | patch/overwrite at flush/release |

Local durability uses `ShadowStore`, `PendingIndex`, and a journal-backed
`CommitQueue`. This lets the mount recover queued data after process restarts
instead of relying only on in-memory write state.

### FUSE Read Path

- Small files: in-memory LRU read cache.
- Large sequential reads: adaptive prefetch, growing from 256KB up to 16MB.
- Large random reads: HTTP range reads.
- Dirty handles: reads are served from the local write buffer / shadow first.

SSE events from `/v1/events` invalidate caches for foreign changes. Local
same-mount mutations update userspace caches directly and avoid redundant kernel
notify storms.

## HTTP API

Core filesystem endpoints:

```text
PUT    /v1/fs/{path}              write small file
GET    /v1/fs/{path}              read file
HEAD   /v1/fs/{path}              lightweight stat
GET    /v1/fs/{path}?stat         JSON metadata stat
GET    /v1/fs/{path}?list         list directory
GET    /v1/fs/{path}?grep         search file contents
GET    /v1/fs/{path}?find         find files by attributes
PATCH  /v1/fs/{path}              patch large file parts
DELETE /v1/fs/{path}              delete
DELETE /v1/fs/{path}?recursive    recursive delete
POST   /v1/fs/{path}?append       append
POST   /v1/fs/{path}?copy         copy (X-Drive9-Copy-Source)
POST   /v1/fs/{path}?rename       rename (X-Drive9-Rename-Source)
POST   /v1/fs/{path}?mkdir        mkdir
```

Upload and event endpoints:

```text
POST   /v1/uploads/initiate
GET    /v1/uploads?path={path}
POST   /v1/uploads/{id}/resume
POST   /v1/uploads/{id}/complete
DELETE /v1/uploads/{id}

POST   /v2/uploads/initiate
POST   /v2/uploads/{id}/presign
POST   /v2/uploads/{id}/presign-batch
POST   /v2/uploads/{id}/complete
POST   /v2/uploads/{id}/abort

GET    /v1/events                 SSE change stream
GET    /v1/status                 server status and upload limits
```

Vault endpoints also exist for secret storage, scoped grants, delegated tokens,
and read-only vault mounts.

## Mount Options

```text
drive9 mount [flags] [:/remote] <mountpoint>

  -server string                 server URL
  -api-key string                owner API key
  -mode auto|fuse|webdav          mount mode (default: auto)
  -sync-mode auto|interactive|strict
  -profile string                mount profile
  -cache-dir string              write-back cache dir (default: ~/.cache/drive9)
  -cache-size int                read cache size in MB (default: 128)
  -dir-ttl duration              directory cache TTL (default: 10s)
  -attr-ttl duration             kernel attr cache TTL (default: 10s)
  -entry-ttl duration            kernel entry cache TTL (default: 10s)
  -flush-debounce duration       small-file flush debounce (default: 2s)
  -lookup-retry-count int        detached stat retries after transient lookup errors
  -lookup-retry-timeout duration timeout per detached lookup retry
  -allow-other                   allow other users to access mount
  -read-only                     mount read-only
  -debug                         enable FUSE debug logging
```

## Server Configuration

Important environment variables:

```text
DRIVE9_META_DSN                 control-plane MySQL/TiDB DSN
DRIVE9_TENANT_PROVIDER          db9 | tidb_zero | tidb_cloud_starter
DRIVE9_TOKEN_SIGNING_KEY        32-byte hex JWT signing key
DRIVE9_ENCRYPT_TYPE             local_aes | kms
DRIVE9_MASTER_KEY               local AES key
DRIVE9_ENCRYPT_KEY              KMS key id or alias
DRIVE9_MAX_UPLOAD_BYTES         max upload size

DRIVE9_S3_BUCKET                enable AWS/S3-compatible object storage
DRIVE9_S3_REGION
DRIVE9_S3_PREFIX
DRIVE9_S3_ENDPOINT              custom endpoint for MinIO/S3-compatible stores
DRIVE9_S3_FORCE_PATH_STYLE
DRIVE9_S3_ACCESS_KEY_ID
DRIVE9_S3_SECRET_ACCESS_KEY
DRIVE9_S3_ROLE_ARN
DRIVE9_S3_ENCRYPTION_MODE       none | sse-s3 | sse-kms | dsse-kms
DRIVE9_S3_KMS_KEY_ID

DRIVE9_QUERY_EMBED_*            app-side query embedding provider
DRIVE9_EMBED_*                  app-managed background embedding provider
DRIVE9_IMAGE_EXTRACT_*          async image-to-text extraction
DRIVE9_AUDIO_EXTRACT_*          async audio-to-text extraction
```

`drive9-server-local` is the preferred local single-tenant validation server.
It uses `DRIVE9_LOCAL_DSN` for the tenant database and can use a local object
store directory when no S3 bucket is configured.

## Testing

```bash
make test
make test TEST_RUN='TestInsertAndGetNode' TEST_PKGS='./pkg/datastore/...'
```

Tests use `DRIVE9_TEST_MYSQL_DSN` when set; otherwise the test harness starts a
MySQL/TiDB-compatible container through testcontainers and the Podman-aware
scripts.

### Failpoint Tests

drive9 has failpoint-based tests for high-value concurrency and failure paths,
including semantic task lease expiry, renew boundaries, lease loss, and panic
cleanup.

```bash
make test-failpoint
# or
python3 scripts/run_failpoint_tests.py
```

Failpoint tests temporarily rewrite instrumented source files and restore them
after the run. Do not run them in parallel with ordinary `go test` commands.

### FUSE / E2E Tests

FUSE behavior depends on OS support and `/dev/fuse`, so real mount validation
lives in e2e scripts and local Ubuntu/macOS runs. The important release gate is
a real mount plus Git workflow:

```bash
drive9 mount :/ ./work --debug
git clone https://github.com/mem9-ai/drive9.git ./work/drive9
git -C ./work/drive9 status
```

## Project Layout

```text
cmd/drive9/              CLI
cmd/drive9-server/       multi-tenant HTTP server
cmd/drive9-server-local/ single-tenant local validation server

pkg/backend/             filesystem backend, upload, semantic task integration
pkg/client/              Go HTTP client and transfer engine
pkg/datastore/           TiDB/MySQL metadata and file state store
pkg/fuse/                go-fuse filesystem, caching, writeback, SSE invalidation
pkg/server/              HTTP API, SSE, auth, metrics, semantic worker
pkg/s3client/            AWS S3 and local S3-compatible storage
pkg/tenant/              tenant provisioning, pooling, provider selection
pkg/tenant/schema/       TiDB schema and auto-embedding validation
pkg/tenant/db9/          db9 provider path
pkg/vault/               secret vault, grants, delegated token primitives
pkg/embedding/           OpenAI-compatible embedding client
pkg/encrypt/             local AES and KMS encryption wrappers
pkg/meta/                control-plane metadata store
pkg/webdav/              WebDAV adapter
```

## References

- [TiDB](https://www.pingcap.com/tidb/) — current primary tenant database path.
- [db9](https://db9.ai/) — related provider path and original design
  inspiration; not the default backend in the current README architecture.
- [AGFS](https://github.com/c4pt0r/agfs) — Plan 9-inspired filesystem
  interfaces used by the backend.
- [go-fuse](https://github.com/hanwen/go-fuse) — FUSE library used by the mount.
- [Plan 9](https://plan9.io/plan9/) — names and files as the interface.

## License

Apache 2.0
