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
- **file**: the user-visible path-addressable item in the filesystem-like namespace
- **logical object**: the internal content identity behind one or more file paths
- **derived artifact**: any generated output produced from resource processing
- **semantic artifact**: a user-visible derived artifact such as `.abstract.md`, `.overview.md`, or `.relations.json`
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

Representative endpoint surface:

```text
PUT    /v1/fs/{path}          write or initiate large-file upload
GET    /v1/fs/{path}          read file or begin authorized download flow
DELETE /v1/fs/{path}          remove file from user-visible namespace
HEAD   /v1/fs/{path}          fetch metadata / revision / status
GET    /v1/fs/{path}?list     list directory contents

POST   /v1/search             search by semantic / keyword / metadata criteria

GET    /v1/uploads?path=...&status=UPLOADING
POST   /v1/uploads/{id}/resume
POST   /v1/uploads/{id}/complete
DELETE /v1/uploads/{id}
```

This should be treated as the representative current-phase endpoint shape, even if exact query parameters or response fields evolve.

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
- if an active upload already exists for the same logical target, resume/reuse policy must be explicit rather than implicit

Representative large-file initiation example:

```http
PUT /v1/fs/data/archive.tar
Content-Length: 5368709120
Content-Type: application/octet-stream

HTTP/1.1 202 Accepted
Content-Type: application/json

{
  "upload_id": "upl_01JXYZ...",
  "path": "/data/archive.tar",
  "status": "UPLOADING",
  "parts": [
    {"part_number": 1, "size": 67108864, "url": "https://object-store.example/..."},
    {"part_number": 2, "size": 67108864, "url": "https://object-store.example/..."}
  ],
  "complete_url": "/v1/uploads/upl_01JXYZ.../complete"
}
```

Representative upload management examples:

```http
GET /v1/uploads?path=/data/archive.tar&status=UPLOADING

HTTP/1.1 200 OK
Content-Type: application/json

[
  {
    "upload_id": "upl_01JXYZ...",
    "path": "/data/archive.tar",
    "status": "UPLOADING"
  }
]
```

```http
POST /v1/uploads/upl_01JXYZ.../resume

HTTP/1.1 200 OK
Content-Type: application/json

{
  "upload_id": "upl_01JXYZ...",
  "remaining_parts": [
    {"part_number": 7, "size": 67108864, "url": "https://object-store.example/..."},
    {"part_number": 8, "size": 67108864, "url": "https://object-store.example/..."}
  ]
}
```

```http
POST /v1/uploads/upl_01JXYZ.../complete

HTTP/1.1 200 OK
ETag: "r18"
Content-Type: application/json

{
  "path": "/data/archive.tar",
  "status": "CONFIRMED",
  "revision": "r18"
}
```

```http
DELETE /v1/uploads/upl_01JXYZ...

HTTP/1.1 204 No Content
```

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

Recommended conflict policy:

- reuse an existing upload session only when the server can prove it refers to the same logical upload intent
- otherwise return `409 Conflict` rather than silently rebinding state

Representative conflict split:

```text
409 Conflict:
- active upload session exists for the same path but a different logical upload intent
- `/complete` fails because path ownership or upload/session identity is incompatible

412 Precondition Failed:
- client supplied `If-Match` does not match the current revision
- overwrite precondition fails even though the path itself is valid
```

### 5.5 Search contract

Search should be a first-class user operation.

The API/UX contract should support at least:

- path-scoped search
- metadata-aware search
- semantic search over available indexed material

The contract does not need to expose every backend retrieval primitive. It should expose a coherent user-facing query model.

Representative search request:

```http
POST /v1/search
Content-Type: application/json

{
  "query": "training data for image classification",
  "mode": "hybrid",
  "scope": "/data/",
  "filters": {
    "tags": {"env": "prod"}
  },
  "top_k": 10
}

HTTP/1.1 200 OK
Content-Type: application/json

[
  {
    "path": "/data/training-v3/.abstract.md",
    "score": 0.92,
    "kind": "semantic_artifact"
  }
]
```

The exact ranking formula may evolve, but the user-facing request shape should remain coherent across vector, keyword, and hybrid search modes.

### 5.6 Copy, move, and delete semantics

User-facing semantics should remain simple:

- copy should behave like copying a file path binding, even if physical storage is reused through the same logical object internally
- move should behave like renaming or relocating a file path, even if no physical object move occurs internally
- delete should behave like removing the logical item from user view, even if physical cleanup happens later

This preserves the product model while allowing backend-efficient implementations.

Representative delete example:

```http
DELETE /v1/fs/data/archive.tar

HTTP/1.1 204 No Content
```

This response means the path has been removed from the user-visible namespace. It does not require physical storage cleanup to have already finished.

### 5.7 Concurrency and overwrite contract

The default overwrite model may be last-writer-wins, but the user contract should support an explicit optimistic guard.

Recommended contract:

- clients may send `If-Match: <revision>` on overwrite-sensitive writes
- mismatch returns `412 Precondition Failed`
- successful writes return the new revision identifier through a standard response field or header such as `ETag`

Writes should also preserve simple directory semantics:

- writing to a file path may auto-create missing parent directories when the product chooses `mkdir -p` style behavior
- file and directory path canonicalization rules must remain consistent across read, write, list, move, and delete

Representative optimistic overwrite success:

```http
PUT /v1/fs/data/config.json
If-Match: "r7"
Content-Type: application/json

{"feature": true}

HTTP/1.1 200 OK
ETag: "r8"
Content-Type: application/json

{
  "path": "/data/config.json",
  "revision": "r8",
  "status": "CONFIRMED"
}
```

Representative revision mismatch:

```http
PUT /v1/fs/data/config.json
If-Match: "r7"
Content-Type: application/json

{"feature": true}

HTTP/1.1 412 Precondition Failed
ETag: "r9"
Content-Type: application/json

{
  "error": {
    "code": "revision_mismatch",
    "message": "The current revision no longer matches If-Match."
  }
}
```

If the server exposes the current revision on mismatch, it should do so in a standard response field or header so the client can retry deliberately rather than guessing.

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

Representative path outcomes:

```text
"/data/report.txt"      -> valid file path
"/data/reports/"        -> valid directory path
"/data//report.txt"     -> normalized to "/data/report.txt"
"/data/../secret.txt"   -> rejected
"/data/report.txt/"     -> rejected as file path
```

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
- stale derived artifacts may exist transiently, but the system must converge through async regeneration and reconcile
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
