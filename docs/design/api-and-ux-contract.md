# RFC: dat9 API and UX Contract

## 1. Goal

This RFC defines the user-facing contract for dat9.

It exists to keep:

- `docs/overview.md` short and product-oriented
- system design RFCs focused on backend structure and correctness
- implementation-facing API and UX behavior in one place

This RFC is the primary reference for user-visible operation semantics, HTTP status expectations, and filesystem-like UX conventions.

## 2. Non-goals

This RFC does not define:

- internal storage schemas
- async task backend internals
- full retrieval ranking algorithms
- low-level queue runtime internals

Those belong in other design RFCs.

## 3. Definitions

- **filesystem-like UX**: a product experience shaped around path-based operations such as copy, move, read, list, remove, and search
- **semantic artifact**: a user-visible derived file such as `.abstract.md`, `.overview.md`, or `.relations.json`
- **small file**: content that is accepted through the direct small-file write path
- **large file**: content that is accepted through the direct object-upload path
- **completion step**: the API call that confirms a large-file upload and establishes final system commit

## 4. Current Implementation Target

### 4.1 P0 / P1 user contract

For the current implementation phase, dat9 should provide a stable contract for:

- path-based file operations
- direct small-file writes
- direct large-file upload using `202 + presigned URL + complete`
- visible semantic artifacts
- search as a first-class user operation

### 4.2 What does not need to be complete yet

The first implementation does not need to expose:

- every future semantic artifact type
- every future task/repair state to end users
- a fully finalized advanced search language

The important requirement is that the basic user-facing behavior remains coherent and stable.

## 5. Design

### 5.1 Filesystem-like operation model

dat9 should preserve a simple path-oriented mental model.

Representative user-facing operations include:

- `cp`
- `cat`
- `ls`
- `mv`
- `rm`
- `search`

These operations define the product shape even if the backend implementation is distributed and asynchronous.

### 5.2 Visible semantic artifacts

Semantic artifacts should remain inspectable where practical.

Examples:

- `.abstract.md`
- `.overview.md`
- `.relations.json`

This contract is important because it keeps derived knowledge legible to both users and agents.

### 5.3 Upload contract

The upload contract should distinguish small files from large files.

#### Small file path

- client sends content directly to dat9
- dat9 commits content and metadata through the small-file write path
- the response indicates the write has been accepted and committed

#### Large file path

- client requests upload
- dat9 returns `202` with presigned upload instructions
- client uploads bytes directly to object storage
- client sends a completion request
- dat9 confirms the resource commit and returns the final accepted state

Suggested flow:

```text
client -> request upload
server -> 202 + presigned upload info
client -> upload bytes to object storage
client -> complete upload
server -> confirm metadata/version commit
```

The upload contract should also support resumable uploads for large files.

Recommended SDK-internal management endpoints:

```text
GET    /v1/uploads?path=...&status=UPLOADING
POST   /v1/uploads/{id}/resume
POST   /v1/uploads/{id}/complete
DELETE /v1/uploads/{id}
```

Recommended behavior:

- upload lookup is path-aware
- resume returns only the remaining work needed to finish upload
- complete is the only operation that establishes final committed large-file state
- cancel marks the upload abandoned and eligible for cleanup

### 5.4 Suggested HTTP status semantics

The following status model should guide implementation.

- `200 OK`
  - synchronous read or operation success
- `201 Created`
  - explicit resource creation where immediate creation semantics are clearer than generic success
- `202 Accepted`
  - async or staged acceptance, especially direct large-file upload initialization
- `204 No Content`
  - successful delete or mutation with no response body
- `302 Found` or equivalent redirect semantics
  - optional direct-download or temporary object access flow where redirect behavior is appropriate
- `400 Bad Request`
  - malformed input
- `401 Unauthorized`
  - missing or invalid credentials
- `403 Forbidden`
  - authenticated but not allowed
- `404 Not Found`
  - missing path or resource
- `409 Conflict`
  - semantic conflict such as incompatible overwrite state or conflicting upload session
- `412 Precondition Failed`
  - failed revision/version precondition

Exact endpoint-level usage may evolve, but the model should stay consistent.

Additional guidance:

- `202 Accepted` should mean the server accepted staged work, not that the file is already fully committed
- `409 Conflict` should be used for meaningful semantic conflicts, such as a conflicting upload session for the same path
- `412 Precondition Failed` should be used when optimistic write preconditions fail, such as revision mismatch on overwrite

### 5.5 Search contract

Search should be a first-class user operation.

The API/UX contract should support at least:

- path-scoped search
- metadata-aware search
- semantic search over available indexed material

The contract does not need to expose every backend retrieval primitive. It should expose a coherent user-facing query model.

### 5.6 Copy, move, and delete semantics

User-facing semantics should remain simple:

- copy should behave like copying a logical file, even if physical storage is reused internally
- move should behave like renaming or relocating a logical file, even if no physical object move occurs internally
- delete should behave like removing the logical item from user view, even if physical cleanup happens later

This preserves the product model while allowing backend-efficient implementations.

### 5.7 Concurrency and overwrite contract

The default overwrite model may be last-writer-wins, but the user contract should support an explicit optimistic guard.

Recommended contract:

- clients may send `If-Match: <revision>` on overwrite-sensitive writes
- mismatch returns `412 Precondition Failed`
- successful writes return the new revision identifier through a standard response field or header such as `ETag`

Writes should also preserve simple directory semantics:

- writing to a file path may auto-create missing parent directories when the product chooses `mkdir -p` style behavior
- file and directory path canonicalization rules must remain consistent across read, write, list, move, and delete

### 5.8 Path canonicalization and path safety

The API contract should treat path canonicalization as a user-visible safety guarantee.

Recommended canonicalization rules:

- reject NUL bytes, control characters, and backslash path separators
- reject `.` and `..` path segments
- collapse repeated slashes
- require directory paths to end with `/`
- require file paths not to end with `/`
- normalize Unicode consistently

This keeps path behavior stable across SDKs, CLIs, and HTTP clients.

### 5.9 Presigned URL security

Presigned URL usage should follow a narrow contract:

- upload URLs should be short-lived
- download URLs should be even shorter-lived
- upload URLs should bind request shape where possible, including part number, size, and checksum-related inputs
- signature-bearing query parameters must be redacted in logs

The preferred model for large-file download is:

- user requests `GET /v1/fs/{path}`
- dat9 performs authorization and path resolution
- dat9 returns redirect or equivalent temporary access only after those checks

### 5.10 Rate limiting and abuse controls

The user-facing contract should assume basic protection controls exist.

At minimum, dat9 should support limits on:

- request rate per tenant
- upload bandwidth or upload concurrency per tenant
- active upload sessions per tenant/path
- maximum file size per tenant or deployment tier

These limits may vary by product tier, but they should not be left undefined.

### 5.11 Inspectability over hidden magic

Where practical, dat9 should prefer:

- visible derived artifacts
- visible path semantics
- understandable user-facing states

over hidden, non-inspectable semantic behavior.

## 6. Invariants / Correctness Rules

- the user-facing contract must remain filesystem-like even if internal implementation is layered
- large-file upload must not require the server to proxy bytes
- semantic artifacts should remain inspectable where practical
- move/copy/delete user semantics must not leak unnecessary backend complexity
- version/precondition failures should surface through clear conflict semantics rather than silent corruption

## 7. Failure / Recovery

- interrupted large-file uploads should be resumable or restartable
- failed completion should not leave users unable to determine whether the file was committed
- stale semantic artifacts may exist transiently, but the system must converge through async regeneration and reconcile
- user-visible contract should distinguish accepted, committed, and failed operations clearly enough to avoid ambiguity

Recommended practical behavior:

- resume should reuse an existing active upload session when the target path and upload identity still match
- resume should return conflict rather than silently rebind an incompatible upload session to new content

## 8. Open Questions

- whether a more explicit CLI contract should be documented separately from the HTTP contract
- whether direct download should always be proxied by dat9 or may use temporary object-store access URLs
- how much internal repair state, if any, should be exposed in user-visible diagnostics
- whether incomplete upload reuse should be keyed by path only, by path plus fingerprint, or by an explicit idempotency key

## 9. References / Dependencies

- `docs/overview.md`
- `docs/design/system-architecture.md`
- `docs/design/storage-and-namespace.md`
- `docs/design/write-path-and-reconcile.md`
- `docs/design/semantic-derivation-and-retrieval.md`
- `docs/design/control-plane-and-provisioning.md`
- `docs/design/resource-versioning-and-async-correctness.md`
