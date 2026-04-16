# drive9

A network drive. Mount it, use it.

```bash
drive9 mount ~/drive
cp video.mp4 ~/drive/
ls ~/drive/
```

Everything is a path. `cp`, `cat`, `ls`, `mv`, `rm` — they just work. The storage backend (db9, S3, local), the upload protocol (presigned multipart, streaming, direct PUT), the semantic indexing (embedding, FTS, image OCR) — all invisible. You see a filesystem.

Inspired by Plan 9's "everything is a file" and the idea that naming things is the only interface you need.

## What it does

**One path namespace, multiple storage tiers.** Small files (< 50KB) go to [db9](https://db9.ai/) — instant writes, auto-embedded, full-text indexed, vector searchable. Large files (≥ 50KB) go to S3 via presigned URLs — the server never touches the bytes.

**FUSE mount with streaming I/O.** Mount as a local filesystem. Sequential writes (cp, dd, ffmpeg) stream parts directly to S3 during `write()`, releasing memory after each part uploads. A 10GB video uses ~24MB of RAM, constant. Random reads use adaptive prefetch — the read window grows from 256KB to 16MB as sequential patterns are detected.

**Semantic search built in.** Every file gets background-embedded. Images get OCR'd. Directories carry `.abstract.md` (100 tokens) and `.overview.md` (1K tokens) for cheap agent scanning — 10x token savings over reading full content.

**Zero-cost operations.** `cp` between drive9 paths is a metadata link (inode refcount). `mv` is a rename. No bytes move.

## Quick start

```bash
# CLI
drive9 cp ./data.tar :/data/data.tar
drive9 fs cp --append ./tail.log :/logs/app.log
drive9 cat :/config/app.json
drive9 ls :/data/
drive9 cp :/a.bin :/b.bin        # zero-copy
drive9 mv :/old.bin :/new.bin    # metadata-only

# Mount
drive9 mount /mnt/drive9

# Unmount
drive9 umount /mnt/drive9
```

### FUSE prerequisites

**macOS** — `brew install --cask macfuse` (Apple Silicon: approve system extension in System Settings, reboot)

**Linux** — `apt install fuse3` / `dnf install fuse3` / `pacman -S fuse3`

### Go SDK

```go
import "github.com/mem9-ai/drive9/pkg/client"

// Create client (get API key from 'drive9 create' or drive9.ai console)
c := client.New("https://api.drive9.ai", "your-api-key")

// Write file (auto-handles small/large files)
if err := c.Write("/data/file.txt", []byte("hello")); err != nil {
    log.Fatal(err)
}

// Read file
data, err := c.Read("/data/file.txt")
if err != nil {
    log.Fatal(err)
}

// List directory
entries, err := c.List("/data/")

// Copy (zero-copy, metadata only)
err = c.Copy("/a.txt", "/b.txt")

// Rename (metadata only)
err = c.Rename("/old.txt", "/new.txt")

// Delete
err = c.Delete("/data/file.txt")
```

## Architecture

```
             ┌───────────────────────────────────┐
             │           path namespace           │
             │  /data/video.mp4  /config/app.json │
             └──────────┬────────────────────────-┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
   ┌────▼────┐    ┌─────▼─────┐   ┌─────▼─────┐
   │  CLI    │    │   FUSE    │   │  Go SDK   │
   │ drive9  │    │ drive9    │   │  client   │
   │ cp/cat  │    │ mount     │   │  pkg      │
   └────┬────┘    └─────┬─────┘   └─────┬─────┘
        │               │               │
        └───────────────┼───────────────┘
                        │
               drive9 HTTP server
               /v1/fs/{path}
                        │
          ┌─────────────┼─────────────┐
          │                           │
   < 50KB │                    ≥ 50KB │
          ▼                           ▼
   ┌─────────────┐            ┌──────────────┐
   │    db9      │            │   S3         │
   │  embedded   │            │  presigned   │
   │  FTS + vec  │            │  multipart   │
   └─────────────┘            └──────────────┘
```

### Inode model

Paths and files are separate, just like Unix. `file_nodes` are dentries, `files` are inodes. One file can have multiple paths. `cp` increments a refcount. `mv` updates a name. `rm` decrements. Storage is GC'd when refcount hits zero.

### Upload protocol

Two generations:

- **v1**: server-side initiate → presign all parts → parallel upload → complete. Simple, but requires knowing total parts upfront.
- **v2**: server-side initiate → presign parts on demand → streaming upload → complete. Parts can be presigned individually as data arrives. This is what the FUSE streaming write path uses.

Both use S3 presigned URLs — the server never proxies file data.

### FUSE write modes

The FUSE layer classifies writes at runtime:

| Mode | Condition | Memory | Upload |
|------|-----------|--------|--------|
| Small file | < 50KB | O(size) | Direct PUT at close |
| Sequential append | New file, append-only | ~24MB steady | Streaming: parts upload during `write()`, memory freed after each |
| Non-sequential | New file, back-write detected | O(size) | All parts at close |
| Existing file edit | Open without truncate | O(touched parts) | Only dirty parts at close (server-side copy for rest) |

Sequential detection: track an append cursor. If writes always advance it, parts behind the cursor are provably final and can be uploaded immediately. Back-write sets `sequential = false` — no data loss, just falls back to buffered mode.

### FUSE read path

- **Small files**: in-memory LRU cache (128MB default)
- **Large files, sequential**: adaptive prefetch — window doubles per sequential read (256KB → 16MB cap)
- **Large files, random**: HTTP range reads, no prefetch
- **Dirty handles**: read from write buffer (lazy-load unmodified parts from server on demand)

## HTTP API

```
PUT    /v1/fs/{path}              Write
GET    /v1/fs/{path}              Read
GET    /v1/fs/{path}?list         List directory
HEAD   /v1/fs/{path}              Stat
DELETE /v1/fs/{path}              Delete
DELETE /v1/fs/{path}?recursive    Recursive delete
POST   /v1/fs/{path}?append       Initiate incremental append upload
POST   /v1/fs/{path}?copy         Zero-copy (X-Drive9-Copy-Source header)
POST   /v1/fs/{path}?rename       Rename (X-Drive9-Rename-Source header)
POST   /v1/fs/{path}?mkdir        Mkdir
```

## Schema

Five tables per tenant:

| Table | Role |
|---|---|
| `file_nodes` | Path tree (dentry) — parent, name, file_id ref |
| `files` | File entity (inode) — storage, size, checksum, revision, embedding state |
| `file_tags` | Key-value tags |
| `uploads` | Multipart upload tracking |
| `semantic_tasks` | Background work queue (embedding, image extraction) |

## Mount options

```text
drive9 mount [flags] <mountpoint>

  --server           Server URL (default: $DRIVE9_SERVER)
  --api-key          API key (default: $DRIVE9_API_KEY)
  --cache-size       Read cache size in MB (default: 128)
  --dir-ttl          Directory cache TTL (default: 5s)
  --attr-ttl         Kernel attr cache TTL (default: 60s)
  --entry-ttl        Kernel entry cache TTL (default: 60s)
  --flush-debounce   Small-file flush debounce window (default: 2s, 0 disables)
  --allow-other      Allow other users to access mount
  --read-only        Mount as read-only
  --debug            Enable FUSE debug logging
```

## Building

```bash
go build -o bin/drive9 ./cmd/drive9
go build -o bin/drive9-server ./cmd/drive9-server
```

## Testing

```bash
make test
```

Uses `DRIVE9_TEST_MYSQL_DSN` if set, otherwise spins up a container via testcontainers.

### Failpoint tests

drive9 also has targeted failpoint-based tests for high-value semantic task timing and
failure-path behavior such as lease expiry, renew boundaries, lease loss, and panic cleanup.

Run them with:

```bash
make test-failpoint
# or
python3 scripts/run_failpoint_tests.py
```

Notes:

- Failpoint tests rewrite instrumented source files during execution, then restore them when
  the run finishes. Do not run failpoint tests in parallel with ordinary `go test` commands.
- The failpoint runner uses the same MySQL test environment as normal tests:
  `DRIVE9_TEST_MYSQL_DSN` if set, otherwise Podman via `scripts/test-podman.sh` when
  available, or another Docker-compatible runtime through testcontainers.
- Failpoint-specific test files use the `failpoint` build tag. Some editors may show build-tag
  warnings for `*_failpoint_test.go` unless configured to include `-tags=failpoint`; this does
  not mean the tests are broken.
- Prefer failpoint for correctness-sensitive concurrency boundaries, not for simple happy-path
  unit tests that ordinary mocks or polling already cover well.

## Project layout

```
cmd/drive9/          CLI (cp, cat, ls, mount, umount, ...)
cmd/drive9-server/   Server
pkg/
  backend/           Drive9Backend — AGFS FileSystem impl
  client/            Go SDK (HTTP client, streaming upload, patch)
  datastore/         Metadata store, semantic task persistence
  fuse/              FUSE filesystem (go-fuse/v2, streaming write, prefetch, caching)
  server/            HTTP server (/v1/fs/{path})
  s3client/          S3-compatible object store (AWS + local mock)
  tenant/            Tenant schema, embedding mode detection
  embedding/         Embedding providers, vector helpers
  encrypt/           Encryption (AES, KMS)
  semantic/          Durable semantic task types
  meta/              Search-facing models
  pathutil/          Path canonicalization
```

## References

- [db9](https://db9.ai/) — Serverless database with built-in embedding, FTS, vector search
- [AGFS](https://github.com/c4pt0r/agfs) — Plan 9-inspired agent filesystem
- [Plan 9](https://plan9.io/plan9/) — Everything is a file. Names are the interface.

## License

Apache 2.0
