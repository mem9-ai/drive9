# dat9 Design Note: Content Locality and Sharing

**Status**: Design (reviewed, v3 — aligned with tiered storage + db9)
**Context**: Where should L0/L1 content physically live? How does sharing work?

---

## The Question

L0 abstracts are tiny (~100 tokens, ~400 bytes). L1 overviews are small (~1-2k tokens). In the previous design, they lived in S3 with a `context_layers` cache table in the metadata DB to avoid HTTP round-trips.

With the new tiered storage architecture (small files in db9, large files in S3), this question has a simpler answer.

---

## Design: L0/L1 Are Ordinary Small Files in db9

### Principle

> **L0 and L1 are ordinary small files stored in db9 via fs9. They are file_nodes entries pointing to files entries. No special tables, no caching layer. db9 IS the cache.**

```
                         db9 (Small File Storage + Search)
                         ─────────────────────────────────
                         files table:
                           .abstract.md → file_id, content_text, vec (auto), tsv (auto)
                           .overview.md → file_id, content_text, vec (auto), tsv (auto)
                           metadata.json → file_id, content_text, vec (auto), tsv (auto)

                         S3 (Large File Storage)
                         ─────────────────────────────────
                         blobs/<ulid>  → images.tar.gz (10 GB)


                         file_nodes (Path Tree):
                           /data/.abstract.md   → file_id_1
                           /data/.overview.md   → file_id_2
                           /data/metadata.json  → file_id_3
                           /data/images.tar.gz  → file_id_4
```

### Why no context_layers table?

In the previous design, `context_layers` cached L0/L1 text from S3 to avoid per-file S3 GETs. With tiered storage:

| Previous design | New design |
|----------------|-----------|
| L0/L1 in S3 (~50ms per read) | L0/L1 in db9 (~1ms per read) |
| Needed cache table to batch scan | db9 read is already fast enough |
| Cache invalidation complexity | No cache = no invalidation |
| Eventual consistency (cache lag) | Strong consistency (direct read) |

**The cache table was solving a problem that no longer exists.** db9 *is* the cache.

### Why "everything is a file" still matters

L0 and L1 are real files at real paths, stored as file_nodes + files entries:

- `dat9 cat /docs/.abstract.md` works --- it's a standard file read from db9.
- `dat9 cp /docs/ /backup/docs/` copies L0/L1 alongside L2 files. No special logic.
- Sharing = sharing the directory. Everything travels together.
- L0 files have `content_text` populated → auto-embedded by db9 → searchable.

### Batch Scan Read Path

```sql
-- Agent scans /data/ for 1000 directories with their L0 abstracts
SELECT fn_dir.path AS dir_path, f_l0.content_text AS abstract
FROM file_nodes fn_dir
LEFT JOIN file_nodes fn_l0
    ON fn_l0.parent_path = fn_dir.path AND fn_l0.name = '.abstract.md'
LEFT JOIN files f_l0
    ON f_l0.file_id = fn_l0.file_id AND f_l0.status = 'CONFIRMED'
WHERE fn_dir.parent_path = '/data/' AND fn_dir.is_directory = true
ORDER BY fn_dir.path
LIMIT 1000;

-- One query, ~5ms, returns paths + abstracts. No S3 calls. No cache table.
```

---

## Sharing

Assumption: **one tenant (agent) owns one db9 database**. Cross-tenant share metadata cannot be stored only in a per-tenant DB.

### V1: Snapshot Share (Recommended default)

V1 is explicit export/import. This is the safest primitive under strict tenant isolation.

```bash
dat9 share create /knowledge/ml-papers/ --to agent-007 --mode snapshot
dat9 share accept sh_01J... --to /shared/ml-papers/
```

Implementation model:

1. Source tenant freezes a point-in-time manifest (`path`, `checksum`, `size`, `storage_type`).
2. Small files (db9): content read via `fs9_read`, written to target tenant's db9 via `fs9_write`. Target db9 auto-computes `vec` and `tsv`.
3. Large files (S3): S3 object-to-object copy to target tenant's S3 prefix.
4. Target tenant creates `file_nodes` + `files` in its own db9.

Because L0/L1 are ordinary small files in db9, they are included automatically and **auto-indexed** in the target tenant.

### V2 (Future, Optional): Live Read-Only Share Mount

```bash
dat9 share create /knowledge/ml-papers/ --to agent-007 --mode ro
dat9 share mount sh_01J... /shared/ml-papers/
```

`ro` is mandatory in V2. `rw` remains out of scope.

Global control-plane table (separate infra DB, not in tenant db9):

```sql
CREATE TABLE global_shares (
    share_id           VARCHAR(26) PRIMARY KEY,
    source_tenant      VARCHAR(255) NOT NULL,
    source_path        VARCHAR(4096) NOT NULL,
    target_tenant      VARCHAR(255) NOT NULL,
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
| Control plane | Global share registry. | Per-tenant db9 cannot resolve cross-tenant paths safely. |
| Data plane | Small files: source db9 read-through. Large files: source S3 read-through. | No duplicate storage. |
| Revocation | Immediate policy revocation in global registry. | Next read fails deterministically. |
| Share-of-share | Prohibited. | Prevents privilege escalation chains. |

### Ownership Rule (Critical)

- Source tenant owns bytes and revisions for live shares.
- Target tenant owns only mount metadata.
- Snapshot share converts ownership: imported bytes become target-owned data (with target-local embeddings/FTS).

---

## Read Path Decision Matrix

| Operation | Source | Latency | Notes |
|-----------|--------|---------|-------|
| `dat9 cat /docs/.abstract.md` | db9 `fs9_read` | ~1ms | Small file, direct read |
| `dat9 cat /data/large.tar.gz` | S3 presigned URL | ~50ms | Large file, redirect |
| `dat9 ls /data/ --with-abstracts` | db9 SQL (file_nodes JOIN files) | ~5ms for 100 dirs | One query, no S3 |
| `dat9 search "training data"` | db9 HNSW vector search | ~10-20ms | Auto-embedded content_text |
| `dat9 search "训练数据" --mode=fts` | db9 GIN FTS | ~5-10ms | jieba tokenizer |
| Shared path read (V2) | Source tenant db9/S3 | ~50-100ms | Read-through, no local cache |

---

## Summary

```
Layer           │ Canonical?  │ Rebuildable?  │ Content
────────────────┼─────────────┼───────────────┼─────────────────────────
db9 (fs9)       │ Yes         │ No            │ Small files (< 1MB): L0, L1, configs, JSON, ...
S3              │ Yes         │ No            │ Large files (>= 1MB): datasets, binaries, ...
files table     │ Yes         │ No            │ File metadata + content_text + vec (auto) + tsv (auto)
file_nodes      │ Yes         │ No            │ Path tree (inode model, N:1 path→file)
global_shares   │ Yes         │ No            │ Cross-tenant share contracts
```

- **Everything is a file.** L0/L1 are ordinary small files in db9. `cp` copies them, `export` includes them.
- **No cache table.** db9 read latency (~1ms) eliminates the need for context_layers.
- **Auto-indexed.** db9 `GENERATED ALWAYS AS` columns provide embedding + FTS with zero application code.
- **Sharing V1 = snapshot import.** Simple, safe. Target gets auto-indexed copies.
- **Sharing V2 = read-only bind mount (optional later).** Live bytes, `ro` only.
