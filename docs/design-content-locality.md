# dat9 Design Note: Content Locality and Sharing

**Status**: Design (reviewed, v2)  
**Context**: Where should L0/L1 content physically live? How does sharing work?

---

## The Question

L0 abstracts are tiny (~100 tokens, ~400 bytes). L1 overviews are small (~1-2k tokens). Storing them in S3 means an HTTP round-trip per read. Scanning 1000 directories would mean 1000 S3 GETs.

Meanwhile, sharing a directory tree requires L0/L1 to travel with the files. How do we resolve **read performance** vs **architectural consistency**?

---

## Design: Single Source, Derived Caches

### Principle

> **S3 is canonical for content. Metadata DB is canonical for namespace/path/auth. DB and vector index hold derived caches that are always rebuildable from S3.**

```
                         S3 (Content Source of Truth)
                         ───────────────────────────
                         /docs/
                           .abstract.md     (L0, ~400 bytes)
                           .overview.md     (L1, ~4 KB)
                           guide.pdf        (L2, 10 MB)

                              │
                     async propagation (on write / on generate)
                              │
               ┌──────────────┼──────────────┐
               ▼                             ▼
     ┌───────────────────┐         ┌───────────────────┐
     │  context_layers    │         │  Vector Index      │
     │  (Metadata DB)    │         │                    │
     │                   │         │  uri               │
     │  path             │         │  vector            │
     │  layer (L0/L1)    │         │  abstract (denorm) │
     │  content          │         │  content_hash      │
     │  content_hash     │         │                    │
     └───────────────────┘         └───────────────────┘

     Read: batch scan              Read: semantic search
     (1 SQL query, ~5ms)           (ANN query, ~20ms)
```

### Why "everything is a file" matters

L0 and L1 are real files at real paths in S3. This means:

- `dat9 cat /docs/.abstract.md` works --- it's a standard file read.
- `dat9 cp /docs/ /backup/docs/` copies L0/L1 alongside L2 files. No special logic.
- Sharing = sharing the directory. Everything travels together.
- The vector index can be rebuilt from scratch by scanning `.abstract.md` files.

### context_layers: Separate Table, Not Column

L0/L1 text is cached in a **dedicated table**, not inlined on `files`:

```sql
CREATE TABLE context_layers (
    namespace_id    VARCHAR(255) NOT NULL,            -- tenant isolation
    path            VARCHAR(4096) NOT NULL,
    layer           ENUM('L0', 'L1') NOT NULL,
    content         TEXT NOT NULL,
    content_hash    CHAR(64) NOT NULL,       -- SHA-256 of content, for staleness detection
    source_s3_etag  VARCHAR(255),            -- S3 ETag at time of cache fill
    status          ENUM('PENDING', 'READY') NOT NULL DEFAULT 'PENDING',
    updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (namespace_id, path, layer),
    INDEX idx_status (status),
    CHECK (LENGTH(content) <= 8192)          -- 8 KB max for L0, prevents unbounded growth
);
```

**Why separate table, not a column on `files`?**

1. **No row bloat on hot table.** `files` is queried on every `ls`, `stat`, path resolution. Adding TEXT columns degrades InnoDB page density and buffer pool utilization.
2. **Smart layer stays decoupled.** `context_layers` is part of the semantic pipeline, not the core filesystem. If you disable the pipeline, this table simply stays empty. Zero invasiveness.
3. **Extensible.** Future: multi-language L0 (Chinese + English), L0 version history, L1 cache --- just more rows.
4. **`status` column.** Distinguishes "L0 not yet generated" (`PENDING`) from "L0 doesn't exist" (no row). Batch scans can filter `status = 'READY'`.

### Cache Invalidation

Write-through with content hash verification:

```
File Write (.abstract.md to S3):
  1. S3.PutObject(.abstract.md)              → success, get S3 ETag
  2. UPSERT context_layers SET               → update DB cache
       content = <text>,
       content_hash = SHA256(<text>),
       source_s3_etag = <etag>,
       status = 'READY'
  3. ENQUEUE vector_index_update             → async vector upsert
```

**If step 2 fails**: S3 has new content, DB cache is stale. Fix:
- **Periodic reconciler**: scan `context_layers` rows where `source_s3_etag != S3.HeadObject().ETag`. Re-read from S3 and refresh. HeadObject is cheap (~$0.0004/1000 requests).
- **On-read validation**: when serving `?abstract`, compare `source_s3_etag` with current S3 ETag. Stale? Re-read, update cache, return fresh content.

**If step 3 fails (queue drops message)**:
- **Catch-up indexer**: periodically scan `context_layers.updated_at > vector_index.last_indexed_at`, re-embed and upsert.

Both the reconciler and catch-up indexer are background jobs, same pattern as the Reaper.

### Batch Scan Read Path

```sql
-- Agent scans /data/ for 1000 directories with their L0 abstracts
SELECT f.path, cl.content AS abstract
FROM files f
LEFT JOIN context_layers cl ON cl.path = f.path AND cl.layer = 'L0' AND cl.status = 'READY'
WHERE f.parent_path = '/data/' AND f.is_directory = true AND f.status = 'CONFIRMED'
ORDER BY f.path
LIMIT 1000;

-- One query, ~5ms, returns paths + abstracts. No S3 calls.
```

---

## Sharing

Assumption: **one tenant (agent) owns one TiDB Serverless cluster**. This means share metadata cannot be stored only in a per-tenant DB.

### V1: Snapshot Share (Recommended default)

V1 is explicit export/import. This is the safest primitive under strict tenant isolation.

```bash
# Source tenant exports a signed snapshot package
dat9 share create /knowledge/ml-papers/ --to agent-007 --mode snapshot

# Target tenant accepts and imports into its own namespace
dat9 share accept sh_01J... --to /shared/ml-papers/
```

Implementation model:

1. Source tenant freezes a point-in-time manifest (`path`, `checksum`, `size`, `etag`, `version`).
2. Data is copied object-to-object (or streamed by client) into target tenant S3 prefix.
3. Target tenant writes metadata in its own TiDB cluster and rebuilds `context_layers`/vector index asynchronously.

Because L0/L1 are files in S3, they are included automatically. No cross-cluster transaction is needed.

### V2 (Future, Optional): Live Read-Only Share Mount

V2 adds live read-through sharing across tenant clusters:

```bash
dat9 share create /knowledge/ml-papers/ --to agent-007 --mode ro
dat9 share mount sh_01J... /shared/ml-papers/
```

`ro` is mandatory in V2. `rw` remains out of scope.

Important: per-tenant vector stores are isolated. A live mount can read source bytes, but semantic retrieval in target tenant still depends on target-local derived index refresh. This is why V1 snapshot remains the recommended default.

Global control-plane table (separate infra DB, not in tenant TiDB):

```sql
CREATE TABLE global_shares (
    share_id           VARCHAR(26) PRIMARY KEY,
    source_tenant      VARCHAR(255) NOT NULL,
    source_cluster     VARCHAR(255) NOT NULL,
    source_path        VARCHAR(4096) NOT NULL,
    target_tenant      VARCHAR(255) NOT NULL,
    target_cluster     VARCHAR(255) NOT NULL,
    target_mount_path  VARCHAR(4096) NOT NULL,
    mode               ENUM('snapshot','ro') NOT NULL,
    status             ENUM('PENDING','ACTIVE','REVOKED','EXPIRED') NOT NULL,
    created_at         DATETIME(3) NOT NULL,
    expires_at         DATETIME(3)
);
```

**Scoping decisions for V2**:

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Read-write mode | `ro` only. `rw` deferred. | Cross-tenant writes create ownership and billing ambiguity. |
| Control plane | Global share registry. | Per-tenant TiDB cannot resolve cross-cluster paths safely. |
| Data plane | Source S3 read-through. | No duplicate storage and instant visibility of source updates. |
| Recipient cache | Disabled by default for shared paths. | Avoid invalidation fanout across clusters. |
| Recipient vector indexing | Optional local index on shared L0, eventually consistent. | Keeps search local while preserving read-only ownership; freshness policy is explicit, not implicit. |
| Revocation | Immediate policy revocation in global registry. | Next read authorization fails deterministically. |
| Share-of-share | Prohibited. | Prevents privilege escalation chains. |

### Ownership Rule (Critical)

- Source tenant owns bytes and revisions for live shares.
- Target tenant owns only mount metadata and optional derived index/cache.
- Snapshot share converts ownership: imported bytes become target-owned data.

This ownership split keeps one-tenant-one-cluster boundaries intact while still enabling collaboration.

---

## Read Path Decision Matrix

| Operation | Source | Latency | Notes |
|-----------|--------|---------|-------|
| `dat9 cat /docs/.abstract.md` | S3 | ~50ms | Standard file read |
| `dat9 ls /data/ --with-abstracts` | Metadata DB (`context_layers` JOIN `files`) | ~5ms for 100 dirs | One SQL query, no S3 |
| `dat9 overview /data/training/` | S3 (or `context_layers` for L1) | ~50ms (S3) / ~5ms (cache) | |
| `dat9 search "training data"` | Vector Index | ~20ms | ANN query over L0 embeddings |
| `dat9 find "training data" --include-overview` | Vector Index + S3 | ~70ms | Search + load L1 for top-K |
| Shared path read | Source namespace S3 (read-through) | ~50-100ms | No local cache for shared paths in V2 |

---

## Summary

```
Layer           │ Canonical?  │ Rebuildable?  │ Content
────────────────┼─────────────┼───────────────┼─────────────────────────
S3              │ Yes         │ No            │ .abstract.md, .overview.md, *.pdf, ...
context_layers  │ No (cache)  │ Yes, from S3  │ L0/L1 text + hash (separate table)
Vector Index    │ No (index)  │ Yes, from S3  │ L0 embedding + abstract text
files           │ Yes (paths) │ No            │ Path tree + file metadata. No content mixed in.
global_shares   │ Yes (perms) │ No            │ Cross-tenant share contracts
```

- **Everything is a file.** L0/L1 live in S3 as real files. `cp` copies them, `export` includes them.
- **Core table stays clean.** `files` has zero content columns. Smart layer is in `context_layers`.
- **Sharing V1 = snapshot import (recommended).** Simple, safe, and search-consistent in each tenant.
- **Sharing V2 = read-only bind mount (optional later).** Live bytes, but `ro` only, no recipient caching, no `rw`, and vector freshness is policy-driven.
- **Cache invalidation = content hash + periodic reconciler.** S3 ETag comparison catches drift.
