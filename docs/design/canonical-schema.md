# RFC: dat9 Canonical Schema

## 1. Goal

This RFC defines the canonical schema layer for dat9.

Its role is to turn the current RFC set's representative logical models into a more implementation-grade reference without collapsing the design set back into one monolithic document.

This RFC is normative for:

- canonical table names
- core columns and key fields
- required status fields
- primary indexes and lookup paths
- which schemas are already code-backed versus only planned

## 2. Non-goals

This RFC does not define:

- every API request and response shape
- every transaction protocol in detail
- ranking formulas or retrieval algorithms
- final physical schema for all semantic/index/materialization tables

This RFC also does not require every deployment engine to use byte-for-byte identical SQL syntax.
It defines canonical schema semantics first, then names the current reference DDL where it already exists.

## 3. Definitions

- **canonical schema**: the stable table-and-column contract that implementation and future migrations should converge on
- **reference DDL**: the concrete SQL used by the current codebase to realize part of the canonical schema
- **tenant-local schema**: metadata and runtime tables scoped to one tenant cell
- **control-plane schema**: global tables used for tenant routing, provisioning, and auth
- **deferred schema**: a logical model that is intentionally not yet frozen into exact physical tables

## 4. Current Implementation Target

### 4.1 P0 / P1 schema scope

The current schema work should be read in three buckets:

| Schema area | Status | Canonical expectation |
| --- | --- | --- |
| `file_nodes`, `files`, `file_tags`, `uploads` | implemented | current reference DDL exists in code |
| `tasks`, `tenants` | planned but important | schema should now be treated as canonical target, even if code is not complete yet |
| semantic/vector/processing-state tables | intentionally deferred | logical model is defined, exact DDL is not frozen yet |

### 4.2 Current code-backed reference

The current code-backed reference DDL lives in:

- `pkg/meta/meta.go`

That code uses SQLite-compatible SQL as a local stand-in for `db9`.
The exact engine-specific type names may change in production, but the table names, key columns, state fields, and core indexes defined in this RFC should remain stable.

### 4.3 Why this RFC exists

Several design RFCs currently say "representative logical records" or "representative runtime fields."
That is useful for architecture review, but it is not enough when reviewers ask:

- what exact tables should exist
- which fields are authoritative
- which indexes the implementation should rely on
- which schema parts are still intentionally unfrozen

This RFC answers those questions directly.

## 5. Design

### 5.1 Schema ownership map

The schema should be split by ownership boundary:

- control plane
  - `tenants`
- tenant-local namespace and file state
  - `file_nodes`
  - `files`
  - `file_tags`
  - `uploads`
- tenant-local async runtime
  - `tasks`
- deferred derived-state area
  - semantic material
  - vector material
  - processing-state material

This split follows the rest of the design set:

- naming and storage bindings are tenant-local
- auth and routing metadata are global
- async execution is tenant-local
- derived-state physical layout is still allowed to evolve

### 5.2 Tenant-local namespace schema

These tables are already code-backed in `pkg/meta/meta.go`.

#### `file_nodes`

Purpose:

- canonical path tree
- directory entries
- source of truth for namespace membership

Reference DDL:

```sql
CREATE TABLE IF NOT EXISTS file_nodes (
    node_id      TEXT PRIMARY KEY,
    path         TEXT NOT NULL,
    parent_path  TEXT NOT NULL,
    name         TEXT NOT NULL,
    is_directory INTEGER NOT NULL DEFAULT 0,
    file_id      TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_path
    ON file_nodes(path);

CREATE INDEX IF NOT EXISTS idx_parent
    ON file_nodes(parent_path);

CREATE INDEX IF NOT EXISTS idx_file_id
    ON file_nodes(file_id);
```

Canonical rules:

- `path` is unique within one tenant-local namespace
- directories have `is_directory = 1` and may keep `file_id = NULL`
- non-directory rows bind one visible path to one underlying file identity
- multiple `file_nodes` rows may reference the same `file_id`

Operational meaning:

- `ls` is primarily `parent_path` lookup
- file `mv` is normally one-row metadata rewrite
- file `cp` is normally one-row insert pointing at an existing `file_id`
- directory `mv` and `cp` rewrite or insert multiple `file_nodes` rows, but still avoid byte-copy by default

#### `files`

Purpose:

- inode-like file entity
- stable storage binding
- file revision and lifecycle state

Reference DDL:

```sql
CREATE TABLE IF NOT EXISTS files (
    file_id         TEXT PRIMARY KEY,
    storage_type    TEXT NOT NULL,
    storage_ref     TEXT NOT NULL,
    content_type    TEXT,
    size_bytes      INTEGER NOT NULL DEFAULT 0,
    checksum_sha256 TEXT,
    revision        INTEGER NOT NULL DEFAULT 1,
    status          TEXT NOT NULL DEFAULT 'PENDING',
    source_id       TEXT,
    content_text    TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    confirmed_at    TEXT,
    expires_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_status
    ON files(status, created_at);
```

Canonical rules:

- `file_id` is the stable logical file identity behind one or more paths
- `storage_type` is currently expected to be `db9` or `s3`
- `storage_ref` is a stable blob/object reference and must not encode the user-visible path
- `revision` is the current optimistic-concurrency and stale-write guard token
- `status` is currently expected to move through `PENDING`, `CONFIRMED`, and `DELETED`

Important current-phase decision:

- the canonical P0 schema does **not** assume db9-generated embedding columns such as `GENERATED ALWAYS AS (EMBED_TEXT(...))`
- `content_text` is a normal stored column
- embedding, chunking, and other semantic derivation outputs must be produced by application or worker logic, not assumed to appear automatically inside the database

This is deliberate because issue `#30` invalidated the earlier assumption that `EMBED_TEXT()` and similar generated-column shortcuts already exist as usable database primitives.

#### `file_tags`

Purpose:

- exact tag filtering without overloading the `files` row with sparse key-value columns

Reference DDL:

```sql
CREATE TABLE IF NOT EXISTS file_tags (
    file_id   TEXT NOT NULL,
    tag_key   TEXT NOT NULL,
    tag_value TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (file_id, tag_key)
);

CREATE INDEX IF NOT EXISTS idx_kv
    ON file_tags(tag_key, tag_value);
```

Canonical rules:

- tags are tenant-local metadata keyed by `file_id`
- the primary key allows only one current value per `(file_id, tag_key)`
- this table is optional for operations that do not use tags, but its shape is canonical once tag filtering is enabled

#### `uploads`

Purpose:

- multipart upload state for large-file direct-to-object-store flow
- resume and completion lookup

Reference DDL:

```sql
CREATE TABLE IF NOT EXISTS uploads (
    upload_id          TEXT PRIMARY KEY,
    file_id            TEXT NOT NULL,
    target_path        TEXT NOT NULL,
    s3_upload_id       TEXT NOT NULL,
    s3_key             TEXT NOT NULL,
    total_size         INTEGER NOT NULL,
    part_size          INTEGER NOT NULL,
    parts_total        INTEGER NOT NULL,
    status             TEXT NOT NULL DEFAULT 'UPLOADING',
    fingerprint_sha256 TEXT,
    idempotency_key    TEXT,
    created_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    updated_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    expires_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_upload_path
    ON uploads(target_path, status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_idempotency
    ON uploads(idempotency_key);
```

Canonical rules:

- `status` is currently expected to use `UPLOADING`, `COMPLETED`, `ABORTED`, and `EXPIRED`
- `target_path + status='UPLOADING'` is the primary resume lookup shape
- `idempotency_key` is the primary dedupe handle when the client provides one
- `s3_key` is the staged object identity; it must not be treated as a committed path binding by itself

Cross-table invariant:

- `uploads.status = COMPLETED` should imply that the corresponding file commit succeeded and the target path binding exists

### 5.3 Current tenant-local integrity notes

The current code-backed schema intentionally keeps some integrity rules in application transactions rather than enforcing every edge as a database foreign key.

Current practical notes:

- `file_nodes.file_id` is not currently declared as a formal SQL foreign key
- `file_tags.file_id` is not currently declared as a formal SQL foreign key
- delete refcount and orphan transitions are serialized by application logic in `pkg/meta/meta.go`

This means the following are canonical semantic requirements even where the current DDL does not yet express them directly:

- non-directory `file_nodes.file_id` should reference an existing `files.file_id`
- deleting the last visible path must transition the underlying `files` row to `DELETED`
- tag rows for a deleted file should be removed in the same logical delete flow

### 5.4 Canonical control-plane schema

This table is not yet code-backed in the repo, but the schema shape is clear enough to freeze.

#### `tenants`

Purpose:

- tenant identity
- auth lookup
- tenant-to-cell routing
- provisioning lifecycle state

Canonical target DDL:

```sql
CREATE TABLE tenants (
    tenant_id      TEXT PRIMARY KEY,
    api_key_prefix TEXT NOT NULL,
    api_key_hash   TEXT NOT NULL,
    cell_id        TEXT NOT NULL,
    db9_ref        TEXT NOT NULL,
    s3_bucket      TEXT NOT NULL,
    s3_prefix      TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'PROVISIONING',
    created_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_active_at TEXT
);

CREATE INDEX idx_tenants_prefix
    ON tenants(api_key_prefix);

CREATE INDEX idx_tenants_status
    ON tenants(status);
```

Canonical rules:

- `tenant_id` is the stable control-plane identity
- `api_key_prefix` is only a lookup accelerator; it is never sufficient as proof of identity
- `api_key_hash` is the credential verifier
- `cell_id`, `db9_ref`, `s3_bucket`, and `s3_prefix` together define the tenant's routing target
- `status` gates routability and is currently expected to use `PROVISIONING`, `ACTIVE`, `SUSPENDED`, and `DELETED`

Future-compatible note:

- if dat9 later supports multiple credentials per tenant, credential material should move into a separate `tenant_credentials` table rather than overloading the `tenants` row

### 5.5 Canonical tenant-local task schema

This table is also not yet code-backed in the repo, but it is now specific enough to freeze as the direct-runtime P0 target.

#### `tasks`

Purpose:

- durable async execution state
- version-aware task input
- retry and recovery control

Canonical target DDL:

```sql
CREATE TABLE tasks (
    task_id            TEXT PRIMARY KEY,
    task_type          TEXT NOT NULL,
    resource_id        TEXT NOT NULL,
    resource_version   INTEGER NOT NULL,
    payload            TEXT,
    status             TEXT NOT NULL DEFAULT 'queued',
    attempt_count      INTEGER NOT NULL DEFAULT 0,
    max_attempts       INTEGER NOT NULL DEFAULT 5,
    leased_by          TEXT,
    leased_at          TEXT,
    lease_until        TEXT,
    last_heartbeat_at  TEXT,
    last_error         TEXT,
    dedupe_key         TEXT,
    priority           INTEGER NOT NULL DEFAULT 0,
    created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at       TEXT
);

CREATE INDEX idx_tasks_status_created
    ON tasks(status, created_at);

CREATE INDEX idx_tasks_status_lease
    ON tasks(status, lease_until);

CREATE INDEX idx_tasks_resource
    ON tasks(resource_id, resource_version, task_type);

CREATE INDEX idx_tasks_dedupe
    ON tasks(dedupe_key);
```

Canonical rules:

- every task that can advance derived state must bind explicit versioned input through `resource_id` and `resource_version`
- `status` is currently expected to use `queued`, `processing`, `succeeded`, `failed`, and `dead_lettered`
- `lease_until` is the recovery boundary for stale `processing` tasks
- `attempt_count` and `max_attempts` define retry exhaustion
- `payload` carries task-specific parameters but does not replace the canonical top-level fields

Current-phase interpretation:

- until a separate `resources` table exists, `resource_id` may be implemented as `file_id`
- this table is the canonical direct-runtime P0 target even if a later queuefs-compatible runtime presents the same semantics through a different internal layout

### 5.6 Deferred semantic, vector, and processing-state schema

These areas are intentionally **not** yet frozen into exact physical DDL:

- semantic material tables
- vector material tables
- processing-state tables

Reason:

- issue `#30` invalidated the earlier assumption that embedding generation can be hidden behind database-generated columns
- chunk granularity, parsed-text retention, and vector source layout still depend on the async pipeline design
- current docs define the logical model clearly enough, but freezing a physical schema too early would create the same kind of premature certainty that the new RFC set is trying to avoid

Current canonical stance:

- the logical split between semantic material, vector material, and processing state is real
- the physical tables for that split are deferred until the async semantic pipeline lands
- no document should assume "embedding just appears in the `files` row automatically" unless the implementation is later verified and updated accordingly

## 6. Invariants / Correctness Rules

- `file_nodes.path` must remain unique per tenant-local namespace
- `files.revision` is the current write-concurrency and async stale-write guard token
- `uploads.status = COMPLETED` must imply successful metadata commit, not merely uploaded bytes
- `tenants.status` must gate routability explicitly
- `tasks` must carry versioned input for any writeback-producing work
- deferred schema areas must stay explicitly marked as deferred rather than silently implied by old assumptions

## 7. Failure / Recovery

- schema migrations must preserve the canonical semantic meaning of keys and states even when engine-specific SQL changes
- current code-backed tenant-local schema relies partly on application-level transactional discipline rather than only on SQL foreign keys
- future control-plane and task schema rollouts should be introduced with additive migrations first, then wired into request and worker flows
- deferred semantic/index tables should not be backfilled through undocumented ad hoc schema decisions

## 8. Open Questions

- whether db9 production schema should add formal foreign keys for `file_nodes`, `file_tags`, and future `tasks`
- whether `uploads` needs an additional production index on `(status, expires_at)` before large-scale reaper workloads
- whether task dedupe should become a unique constraint or stay as an advisory lookup index
- when the semantic/vector/processing-state physical schema becomes stable enough to freeze in this RFC or a follow-up companion RFC

## 9. References / Dependencies

- `docs/overview.md`
- `docs/design/storage-and-namespace.md`
- `docs/design/write-path-and-reconcile.md`
- `docs/design/control-plane-and-provisioning.md`
- `docs/design/durable-queue-runtime.md`
- `docs/design/resource-versioning-and-async-correctness.md`
- `docs/design/semantic-derivation-and-retrieval.md`
- `pkg/meta/meta.go`
