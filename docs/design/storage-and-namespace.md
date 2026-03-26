# RFC: dat9 Storage and Namespace

## 1. Goal

This RFC defines how dat9 organizes paths, logical file identity, storage tiers, and tenant-scoped object namespaces.

It focuses on:

- small files in `db9`
- large files in `S3`
- inode-like path semantics
- logical path versus object identity
- rename, move, copy, and delete behavior

## 2. Non-goals

This RFC does not define:

- async processing state
- writeback correctness rules for derived artifacts
- queue runtime semantics
- full retrieval derivation behavior

## 3. Definitions

- **path**: the user-visible logical name in the filesystem-like namespace
- **logical file/object identity**: the internal identity of stored content, distinct from any one path
- **small file**: content stored directly in `db9`
- **large file**: content stored in `S3`
- **tenant-scoped object namespace**: the `S3` namespace reserved for one tenant, typically a tenant-specific prefix in a shared bucket

## 4. Design

### 4.1 Tiered storage

- small files are stored in `db9`
- large files are stored in `S3`

The exact size threshold is a product/configuration decision and is not fixed by this RFC.

### 4.2 Path versus storage identity

dat9 should preserve inode-like semantics at the product level:

- path identity and underlying content identity are not the same thing
- zero-copy `cp` should be possible where semantics allow
- metadata-only `mv` should be preferred over physical object copying

Suggested mental model:

```text
path -> logical file/object -> storage backend
                         |- db9 blob
                         \- S3 object
```

### 4.3 Stable object storage keys

`object_key` should remain as stable as possible.

Rename and move should normally be treated as logical metadata operations, not as reasons to physically rename large objects in `S3`.

### 4.4 Tenant-scoped object namespace

Each tenant must have a tenant-scoped object storage namespace for direct large-file storage.

The default shape is:

- a shared bucket
- a tenant-specific prefix such as `tenants/<tenant_id>/...`

Stronger isolation may use:

- a dedicated bucket
- another equivalent isolated object-storage unit

### 4.5 Copy, move, delete semantics

At the product level, dat9 should support:

- copy without necessarily duplicating large-object storage
- rename and move without implying physical object rewrite
- logical delete separate from later cleanup

The exact internal schema can vary, but these semantics should remain visible in the product model.

## 5. Invariants / Correctness Rules

- a path is not the same thing as a physical storage key
- rename/move should not require large-object copy unless explicitly necessary
- large-object storage must remain tenant-scoped
- cleanup of abandoned objects may happen asynchronously after logical state changes

## 6. Failure / Recovery

- interrupted large-file uploads must be resumable or recoverable
- logical metadata must not assume object-store rename atomicity
- orphaned objects must be reconcilable and cleanable

Detailed write and cleanup behavior is defined in `write-path-and-reconcile.md`.

## 7. Open Questions

- default small/large file threshold
- how aggressively zero-copy semantics should extend across internal file/resource boundaries

## 8. References / Dependencies

- `dat9/docs/overview.md`
- `docs/design/system-architecture.md`
- `docs/design/write-path-and-reconcile.md`
