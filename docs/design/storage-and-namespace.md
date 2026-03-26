# RFC: dat9 Storage and Namespace

## 1. Goal

This RFC defines how dat9 organizes paths, logical object identity, storage tiers, and tenant-scoped object namespaces.

It focuses on:

- small files in `db9`
- large files in `S3`
- inode-like path semantics
- logical path versus logical object identity
- rename, move, copy, and delete behavior

## 2. Non-goals

This RFC does not define:

- async processing state
- writeback correctness rules for derived artifacts
- queue runtime semantics
- full retrieval derivation behavior

This RFC also does not freeze one exact physical schema. It defines product-visible namespace and storage semantics that different concrete schemas may implement.

## 3. Definitions

- **path**: the user-visible logical name in the filesystem-like namespace
- **file**: the user-visible path-addressable item resolved from a path
- **logical object**: the internal identity of stored content, distinct from any one path
- **small file**: content stored directly in `db9`
- **large file**: content stored in `S3`
- **tenant-scoped object namespace**: the `S3` namespace reserved for one tenant, typically a tenant-specific prefix in a shared bucket

## 4. Current Implementation Target

### 4.1 P0 / P1 storage contract

For the current phase, dat9 should guarantee at least:

- small files are written directly into `db9`
- large files are written into a tenant-scoped `S3` namespace
- path semantics remain stable across read, list, copy, move, and delete
- object-store physical layout stays decoupled from user-visible paths

### 4.2 Configuration choices that still need product defaults

The following decisions should be documented as implementation choices even if they remain configurable:

- small/large file threshold
- default object-store backend shape
- whether zero-copy semantics apply only to files or also to some directory operations

## 5. Design

### 5.1 Tiered storage

- small files are stored in `db9`
- large files are stored in `S3`

The exact size threshold is a product/configuration decision and is not fixed by this RFC.

Recommended default decision to document explicitly:

- choose one threshold for the current product phase and treat it as a product default, even if it remains configurable later

### 5.2 Path versus storage identity

dat9 should preserve inode-like semantics at the product level:

- path identity and underlying content identity are not the same thing
- zero-copy `cp` should be possible where semantics allow
- metadata-only `mv` should be preferred over physical object copying

Suggested mental model:

```text
path -> file -> logical object -> storage backend
                              |- db9 blob
                              \- S3 object
```

Representative operation mapping:

```text
cp /a /b      -> create another file path bound to the same logical object where semantics allow
mv /a /b      -> rewrite file path metadata, not object-store bytes
rm /a         -> remove file path first, clean storage later if no references remain
cat /a        -> resolve path -> file -> logical object -> storage backend
```

More concrete operational guidance:

- `cp /a /b` for a file should prefer creating a second logical reference rather than copying bytes when product semantics allow
- `cp /a/ /b/` for a directory may require recursive metadata work across descendants
- `mv /a /b` for a file should be an O(1) metadata rewrite when possible
- `mv /a/ /b/` for a directory may be O(N) in descendants because path metadata must be rewritten
- `rm /a` should delete the namespace entry first and only remove storage when the last logical reference is gone

Representative multi-path mapping:

```text
file_nodes (dentry)                         files (inode-like file entity)
--------------------                        --------------------------------
/data/a.tar  ---------\                     file_id: 01J...
/shared/a.tar ---------+------------------> storage_type: s3
/backup/a.tar --------/                     storage_ref: blobs/01J...
                                            size_bytes: 10737418240
                                            revision: r7
```

This mapping is the reason zero-copy file `cp`, metadata-only file `mv`, and refcount-aware delete remain possible without encoding user paths into object keys.

### 5.3 Representative namespace data model

The new design docs should stay schema-flexible, but the old document was right that the namespace model needs a concrete implementation picture.

Representative logical records:

- `file_nodes`
  - path tree / dentry layer
  - key fields: `path`, `parent_path`, `name`, `is_directory`, `file_id`
- `files`
  - inode-like file entity
  - key fields: `file_id`, `storage_type`, `storage_ref`, `size_bytes`, `content_type`, `revision`, `status`

Important structural rules:

- `file_nodes.path` is unique per tenant namespace
- directories may have `file_id = NULL`
- multiple `file_nodes` may reference the same `files.file_id`
- `files.storage_ref` points to stable storage identity, not to the user-visible path

Representative query and mutation consequences:

| Operation | Expected metadata effect |
| --- | --- |
| `ls /dir/` | lookup by `parent_path = '/dir/'` |
| `cat /a` | resolve `path -> file_id -> storage_ref` |
| `cp /a /b` for files | insert a second `file_nodes` row pointing at the same `file_id` |
| `mv /a /b` for files | update one `file_nodes` row |
| `mv /a/ /b/` for directories | rewrite descendant path metadata, but do not rewrite storage bytes |
| `rm /a` | remove one namespace entry; physical cleanup only happens after the last reference is gone |

The delete path therefore depends on refcount-aware semantics even if the final implementation does not literally expose Unix `nlink`.

### 5.4 Stable object storage keys

`object_key` should remain as stable as possible.

Rename and move should normally be treated as logical metadata operations, not as reasons to physically rename large objects in `S3`.

Recommended strategy:

- store large objects under stable keys such as `blobs/<id>`
- keep path-to-object mapping in metadata rather than encoding user paths into object keys

Representative object-key planning:

```text
tenant-visible path:   /data/training-v3/images.tar.gz
metadata binding:      file_nodes.path -> files.file_id -> files.storage_ref
stable object key:     tenants/<tenant_id>/blobs/01JQ7R8K3M...
```

Rename and move should change the left side of this mapping, not the right side, unless a separate migration intentionally rewrites storage.

### 5.5 Tenant-scoped object namespace

Each tenant must have a tenant-scoped object storage namespace for direct large-file storage.

The default shape is:

- a shared bucket
- a tenant-specific prefix such as `tenants/<tenant_id>/...`

Stronger isolation may use:

- a dedicated bucket
- another equivalent isolated object-storage unit

### 5.6 Copy, move, delete semantics

At the product level, dat9 should support:

- copy without necessarily duplicating large-object storage
- rename and move without implying physical object rewrite
- logical delete separate from later cleanup

The exact internal schema can vary, but these semantics should remain visible in the product model.

More concrete guidance:

- file copy should prefer zero-copy additional file paths where possible
- file move should be metadata-only when possible
- directory move may require O(N) metadata rewrite for descendants, but should still avoid storage-byte rewriting
- delete should remove namespace entries first and defer physical cleanup to later stages when needed

This means the storage design should support reference-aware delete semantics even if the final schema does not literally use Unix inode terminology.

Directory operation cost should also be explicit:

- file `mv` is normally O(1) metadata work
- directory `mv` and directory `cp` are O(N) in descendants
- both cases should still avoid physical large-object copying unless a product feature explicitly asks for it

## 6. Invariants / Correctness Rules

- a path is not the same thing as a physical storage key
- rename/move should not require large-object copy unless explicitly necessary
- large-object storage must remain tenant-scoped
- cleanup of abandoned objects may happen asynchronously after logical state changes

## 7. Failure / Recovery

- interrupted large-file uploads must be resumable or recoverable
- logical metadata must not assume object-store rename atomicity
- orphaned objects must be reconcilable and cleanable

Detailed write and cleanup behavior is defined in `write-path-and-reconcile.md`.

## 8. Open Questions

- default small/large file threshold
- how aggressively zero-copy semantics should extend across internal file/resource boundaries

Current candidate defaults worth deciding explicitly:

- threshold options: `1MB`, `5MB`, `10MB`
- object-store options: `S3`, `MinIO`, `R2`

## 9. References / Dependencies

- `docs/overview.md`
- `docs/design/system-architecture.md`
- `docs/design/write-path-and-reconcile.md`
- `docs/design/api-and-ux-contract.md`
