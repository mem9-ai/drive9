# Drive9 Server API Contract Specification

> Note: The legacy Go server previously located inside the `drive9` repository (`cmd/drive9-server`) is **deprecated**. This document defines the single authoritative HTTP wire contract for `drive9-server` and its built-in simulator twin.

## Contract Tiers & Test Twin Model

Every endpoint in this specification carries one of two contract tags:

- **`[core]`** — Core Data-Plane & Tenant Lifecycle contract. Implemented **both** by the production `drive9-server` and by `drive9-simulator`'s built-in Rust server (`tests/simulator/src/server/`), and verified identically by `tests/cases/drive9/api/api.t` across three tiers:
  1. **Client PR CI**: `drive9-simulator run --no-mount tests/cases/drive9/api/api.t` (against the built-in Rust server);
  2. **Server PR CI**: `drive9-simulator run --no-mount --server http://127.0.0.1:9009 tests/cases/drive9/api/api.t` (against a locally built `drive9-server`);
  3. **Daily / Release Action**: `drive9-simulator run --no-mount --server https://<staging|prod> [--api-key <key> | --tidbcloud-public-key ... --tidbcloud-private-key ...] tests/cases/drive9/api/api.t`.
- **`[prod-ext]`** — Extended Production Surfaces served by `drive9-server` (LayerFS, Scoped Tokens & Pseudoroot, Quota, Vault, Multi-cloud Object STS, Runtime Sandbox, Migration & Organization Admin Control Plane). The built-in simulator server does not re-implement these cloud/control-plane surfaces; cases targeting them run against `drive9-server`.

---

## 1. Wire Conventions

- **Base URL**: `http(s)://<host>` (local dev default `http://127.0.0.1:9009`). Business routes live under `/v1/*` except V2 multipart uploads under `/v2/uploads/*` and local S3 mock object routes under `/s3/*`.
- **Content Types**:
  - Default request/response payloads: `application/json`.
  - Raw file reads/writes & `append-log`: `application/octet-stream` (symlinks return target text; object-backed single-layout reads may return `302 Found` to a presigned GET URL; extent reads return `206 Partial Content` JSON block read plans).
  - Streams: `text/event-stream` for `/v1/events` SSE; `application/x-ndjson` for `/v1/journals/*/entries`, `/v1/journal-entries`, and `/v1/runtime/exec`.
- **Paths & Header Source Encoding**:
  - URL paths under `/v1/fs/{path}` are absolute, UTF-8 (NFC-normalized on `drive9-server`), disallow `\` and `..` traversal above `/`. Canonical file paths never end with `/`; canonical directory paths end with `/` (lookup accepts directory paths with or without trailing `/`).
  - Special characters in URL paths (` `, `#`, `?`, `%`, non-ASCII UTF-8) MUST be percent-encoded on the request line.
  - Cross-path headers (`X-Dat9-Copy-Source`, `X-Dat9-Rename-Source`, `X-Dat9-Hardlink-Source`) accept either raw UTF-8 absolute paths (when `X-Dat9-Path-Encoding` is absent) or unpadded URL-safe base64 when `X-Dat9-Path-Encoding: base64url` is set. Any other `X-Dat9-Path-Encoding` value MUST be rejected with `400`.
- **Selector Presence Rule**:
  - Query action selectors on `/v1/fs/{path}` (`?stat`, `?list`, `?mkdir`, `?create`, `?copy`, `?rename`, `?hardlink`, `?symlink`, `?chmod`, `?setmeta`, `?append`, `?append-log`, `?find`, `?tasks`) match by **query key presence**: both bare `?mkdir` and `?mkdir=1` are valid and equivalent.
- **Actor & Idempotency Headers**:
  - `X-Dat9-Actor`: optional client/mount actor identifier echoed into `/v1/events` SSE notifications so a FUSE mount can distinguish its own landed mutations from remote peers.
  - `Idempotency-Key`: required UUID header on `POST /v1/journals/{journal_id}/entries` and `POST /v1/runtime/executions`.

---

## 2. Authentication & Authorization

### 2.1 Tenant Owner API Key `[core]`
- Passed via `Authorization: Bearer <api_key>` or `X-API-Key: <api_key>`.
- Keys carry the `drive9_` prefix (legacy `dat9_` accepted). Missing or invalid credentials return `401 {"error": "..."}`.
- Tenant state gate on business endpoints:
  - `active`: admitted.
  - `pending` / `provisioning`: `503 Service Unavailable` + `Retry-After: 1`.
  - `failed` / `suspended` / `deleting` / `deleted`: `403 Forbidden`.

### 2.2 Path/Permission-Scoped Tokens `[prod-ext]`
- Issued via `POST /v1/tokens` or `POST /v1/tokens/generate` with `scopes: [{prefix, ops[]}]` (empty `scopes` rejected with `400`).
- Admitted only on `/v1/fs*`, `/v1/uploads*`, `/v2/uploads*`, `/v1/layers*`, and `POST /v1/tokens/refresh`.
- Permanently denied (`403`) on `/v1/sql`, `/v1/fork`, `/v1/events`, `/v1/journals*`, `/v1/vault*`, `/v1/object-credentials`, `/v1/runtime*`, `POST /v1/fs/{path}?chmod`, `batch-write` items with `hasMode: true`, and `checksum_sha256` on upload completion.
- Pseudoroot projection: ancestor directories above a granted `prefix` allow projected `stat`/`list` showing only authorized descendants, hiding sibling entries (`404`/`403`).

### 2.3 Control-Plane Credentials `[prod-ext]`
- Three mutually exclusive credential forms (mixing forms returns `400 {"code": "ambiguous_credentials"}`):
  1. TiDB Cloud API keys via `X-TiDBCloud-Public-Key` + `X-TiDBCloud-Private-Key` headers (or JSON body `public_key` / `private_key`);
  2. TiDB Cloud JWT via `X-TiDB-Cloud-Authorization` + `?orgId=`;
  3. Drive9 Bearer key (only on `GET /v1/quota` and `/v1/tokens*`).

### 2.4 Vault Capability & Grant Tokens `[prod-ext]`
- `vault_` (legacy capability) and `vt_` (delegated grant) tokens are accepted exclusively on `GET /v1/vault/read*`.

---

## 3. Health & Tenant Lifecycle

### 3.1 `GET /healthz` `[core]`
- Unauthenticated liveness check.
- Response: `200 {"status": "ok"}`.

### 3.2 `POST /v1/provision` `[core]`
- Provisions a new tenant.
  - Under `provider=local` or the simulator built-in server: unauthenticated or optional JSON body `{}` is accepted.
  - Under hosted TiDB Cloud deployments: requires control-plane credentials (§2.3) unless anonymous provisioning is explicitly enabled.
- Optional JSON body (`<= 1 MiB`): `{public_key?, private_key?, max_storage_size?, max_file_size?, max_file_count?, max_media_llm_files?, max_video_llm_files?, tidbcloud_spending_limit?}`.
- Response: `202 Accepted`
  ```json
  {
    "tenant_id": "t...",
    "api_key": "drive9_...",
    "status": "provisioning | active"
  }
  ```

### 3.3 `GET /v1/status` `[core]`
- Authenticated with the tenant Bearer key. Used for readiness polling and capability/limit discovery.
- Response: `200 OK`
  ```json
  {
    "tenant_id": "t...",
    "status": "active",
    "max_upload_bytes": 10737418240,
    "inline_threshold": 50000,
    "migration_capabilities": {
      "checksum_read": true,
      "checksum_complete": true,
      "conditional_create": true,
      "conditional_update": true,
      "event_ingest": true
    },
    "storage_capabilities": {
      "append_log_v1": true,
      "batch_write_mode_v1": true,
      "completion_identity_v1": true,
      "batch_upload_presigned_v1": true
    }
  }
  ```

### 3.4 `DELETE /v1/tenant` `[core]`
- Authenticated with tenant owner Bearer key (optional `{public_key, private_key}` body for `tidb_cloud_native`).
- Response: `200` or `202` `{"status": "deleted" | "deleting" | "ok"}`. Subsequent requests with the deleted tenant's key fail with `401` / `403`.

### 3.5 `POST /v1/fork` & `DELETE /v1/fork` `[core]`
- `POST /v1/fork`: clones the current tenant's filesystem metadata/state into an independent tenant. Returns `202 {"tenant_id", "api_key", "status"}`.
- `DELETE /v1/fork`: deletes the fork tenant (`200`/`202`).

### 3.6 `POST /v1/sql` `[core]`
- Owner Bearer key only. Body (`<= 1 MiB`): `{"query": "SELECT ..."}`.
- Response: `200` JSON array of row objects (`[{"col": val, ...}]`). Client SQL syntax/semantic errors return `400`.

---

## 4. Filesystem Data & Metadata Plane (`/v1/fs/{path}`) `[core]`

### 4.1 Strong `ETag` Validator Formula
Every regular file and extent file exposes a strong `ETag` derived from `(resource_id, revision, size)`:
```text
ETag: "<sha256_hex(resource_id)>-<revision_hex>-<size_hex>"
```
Because `resource_id` is the inode UUID/hex identity, the `ETag` changes on every content overwrite (even same-size) **and** across `unlink` + recreate at the same path.

### 4.2 `HEAD /v1/fs/{path}`
- Returns metadata headers only with **empty body** (`0` bytes; `Content-Length` reflects the logical file size for files):
  - `ETag`: strong validator (§4.1, for non-directory nodes)
  - `Accept-Ranges: bytes`
  - `Content-Length`: `<size>` (files)
  - `X-Dat9-IsDir`: `"true"` or `"false"`
  - `X-Dat9-Size`: `<int64>`
  - `X-Dat9-Revision`: `<int64>`
  - `X-Dat9-Mtime`: `<unix_epoch_seconds>`
  - `X-Dat9-Btime`: `<unix_epoch_seconds>` (birth time; immutable across overwrites)
  - `X-Dat9-Resource-ID`: `<inode_id>` (shared across hardlink aliases)
  - `X-Dat9-Nlink`: `<int>` (`>= 1`, increments with hardlinks)
  - `X-Dat9-Mode`: `<decimal uint32>` (`st_mode & 07777`, e.g. `420` for `0644`, `493` for `0755`)
  - `X-Dat9-Content-Layout`: `"single"` or `"extent"` (files)
  - `X-Dat9-Storage-Type`: `"db9"` (inline), `"s3"` (object-backed single), or `"extent"`
  - `X-Dat9-Extent-Ino`: `<inode_id>` (extent files only)
  - `X-Dat9-Checksum-SHA256`: `<64-char hex>` (when inline/available)
- Missing path returns `404` with empty body.

### 4.3 `GET /v1/fs/{path}` (Plain Read, Range & Preconditions)
- **Directory**: returns `404` (use `?list` or `?stat`).
- **Symlink**: returns `200` with raw target string (`text/plain`).
- **`If-Match`**: evaluated before serving bytes, redirects, or extent read plans. Accepts `*` or comma-separated strong ETags. Mismatch returns `412 Precondition Failed`.
- **`If-Range`**: when paired with `Range`, serves `206` only if `If-Range` exactly matches the current strong `ETag`; otherwise ignores `Range` and returns `200` with the full file.
- **`Range`**: supports a single byte range (`bytes=start-end`, `bytes=start-`, `bytes=-suffix`).
  - Satisfiable range on inline/object bytes: `206 Partial Content` with `Content-Range: bytes <start>-<end>/<total>`, `Content-Length`, `Accept-Ranges: bytes`, `ETag`, `X-Dat9-Resource-ID`, `X-Dat9-Revision`.
  - Invalid syntax, multi-range, or unsatisfiable range (including `start >= size` or `size == 0`): `416 Range Not Satisfiable` with header `Content-Range: bytes */<size>` and error code `"invalid_range"`.
- **Single-Layout Full Read**: `200 OK` (`application/octet-stream` inline, or `302 Found` to a presigned S3 GET URL on `drive9-server`) with `ETag`, `Accept-Ranges: bytes`, `X-Dat9-Resource-ID`, `X-Dat9-Revision`.
- **Extent-Layout Read**:
  - With request header `X-Dat9-Extent-Read-Plan: 1`: returns `206 Partial Content` (`application/json`) with headers `X-Dat9-Content-Layout: extent`, `X-Dat9-Storage-Type: extent`, `X-Dat9-Extent-Ino`, `ETag`, `X-Dat9-Resource-ID`, `X-Dat9-Revision`, and JSON body:
    ```json
    {
      "size": 8388608,
      "blocks": [
        {"file_offset": 0, "length": 4194304, "block_key": "...", "block_offset": 0, "url": "http..."},
        {"file_offset": 4194304, "length": 4194304, "block_offset": 0, "zero": true}
      ],
      "next_offset": 67108864
    }
    ```
  - Without `X-Dat9-Extent-Read-Plan`: the content is streamed inline as bytes (`200`, or `206` for a `Range`, `application/octet-stream`) with headers `X-Dat9-Content-Layout: extent`, `X-Dat9-Storage-Type: extent`, `X-Dat9-Extent-Length: <size>`, and the same range/conditional semantics as a classic read. Extent layout is an internal storage choice: a client that cannot parse a plan still reads the file, and it never receives a JSON block plan as file bytes.

### 4.4 `GET /v1/fs/{path}?stat`
- Returns `200` JSON metadata (+ headers `X-Dat9-Mtime`, `X-Dat9-Btime`), or `404` if missing:
  ```json
  {
    "size": 17,
    "isdir": false,
    "resource_id": "3f8a...",
    "nlink": 1,
    "revision": 1,
    "mtime": 1763452800,
    "btime": 1763452800,
    "content_type": "text/plain",
    "semantic_text": "...",
    "description": "...",
    "tags": {"env": "prod"},
    "content_layout": "single"
  }
  ```

### 4.5 `GET /v1/fs/{path}?list`
- Lists direct children of a directory path:
  ```json
  {
    "entries": [
      {
        "name": "file.txt",
        "size": 12,
        "isDir": false,
        "mtime": 1763452800,
        "revision": 2,
        "mode": 420,
        "hasMode": true,
        "resource_id": "3f8a...",
        "nlink": 1
      }
    ]
  }
  ```
- Targeting an existing non-directory file returns `400`.
- Listing a non-existent / deleted directory prefix returns `200 {"entries": []}` (while `?stat` on that deleted directory returns `404`).

### 4.6 `GET /v1/fs/{path}?grep=<query>&limit=<n>` & `?find`
- **`?grep=<q>`**: non-empty `grep` query required (empty -> `400`); `limit` defaults to `20`. Returns `200` JSON array `[{"path": "/...", "snippet": "..."}]`.
- **`?find`**: filters descendants under `{path}` using optional `name` (glob `*` / `?`), `tag` (`key=value` exact match or `key` existence), `minsize`, `maxsize`, `newer`, `older`, `limit` (default `100`). Returns `200` JSON array of matching nodes `[{"path", "size", "mtime", "revision", "isdir"}]` with exact leading-slash paths.

### 4.7 `GET /v1/fs/{path}?tasks`
- File-scoped semantic/extract task status endpoint:
  - Root `/` or any directory -> `400` (`"tasks are only available for files"`).
  - Missing file -> `404`.
  - Unknown query parameters alongside `?tasks` -> `400`.
  - Success -> `200 OK` with mandatory response header `X-Dat9-Tasks: 1` and body:
    ```json
    {
      "path": "/dir/file.jpg",
      "tasks": [
        {
          "task_type": "img_extract_text",
          "status": "completed | pending | running | failed | skipped",
          "last_error": "...",
          "pending_reason": "...",
          "usage": {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "llm_calls": 0,
            "audio_duration_seconds": 0
          }
        }
      ]
    }
    ```
    `tasks` is **always** a JSON array (`[]` when empty, never `null`).

### 4.8 `PUT /v1/fs/{path}`
- Writes raw request bytes (`application/octet-stream`), auto-creating missing ancestor directories (`EnsureParentDirs`). Default new-file mode is `0644` (`420`).
- Request headers:
  - `X-Dat9-Expected-Revision`: optional integer CAS precondition.
    - Absent or `-1`: unconditional upsert (creates or overwrites).
    - `0`: create-only (`409` if path already exists).
    - `N > 0`: exact revision match (`409` if missing or current revision `!= N`).
    - Non-integer string: `400 Bad Request`.
  - `X-Dat9-Content-Length`: optional expected byte length; if `!= actual body length`, returns `400`.
  - `X-Dat9-Tag`: repeatable `key=value` header (`400` if `=` is missing or `key` is empty).
  - `X-Dat9-Description`: UTF-8 string `<= 2000` runes (`> 2000` returns `400`).
  - `X-Dat9-Part-Checksums`: comma-separated 4-byte base64 CRC32C values when initiating large multipart PUTs (`202` UploadPlan).
- Writing over an existing directory or an `extent`-layout file via classic `PUT` returns `409`.
- Response: `200 {"status": "ok", "revision": <new_rev>}` (or `202` UploadPlan when initiating multipart via `X-Dat9-Part-Checksums`).

### 4.9 `PATCH /v1/fs/{path}`
- **Extent layout**: body `{new_size, dirty_blocks: [{chunk, off, size, checksum}], expected_revision}` -> `202` `ExtentWritePlan` (`{upload_id, new_size, expires_at, blocks: [...], checksum_contract}`). `dirty_blocks` may be `[]` for metadata-only truncation (`new_size`, including `0`), which commits via `/v2/uploads/{upload_id}/complete` with `parts: []`.
- **Single layout**: body `{new_size, dirty_parts: [1-based], part_size?, expected_revision?}` -> `202` `PatchPlan`.

### 4.10 `POST /v1/fs/{path}` Action Selectors
**Exactly one** action selector MUST be present on `POST /v1/fs/{path}`; zero or multiple action selectors return `400`.

| Selector | Request Headers / Query / Body | Success Response & Semantics |
|---|---|---|
| `?mkdir` | Optional `?mode=<decimal uint32>` (default `493` = `0755`) | `200 {"status": "ok"}`. Auto-creates missing parents; `409` if path already exists. |
| `?create` | Empty body | `200 {"status": "ok", "revision": 1, "resource_id": "<inode>"}`. Creates 0-byte file (`mode=420`); `409` if path exists. |
| `?chmod` | JSON `{"mode": <uint32>}` | `200 {"status": "ok"}`. Updates low 12 permission bits (`mode & 07777`) on file or dir **without** bumping `revision`. `404` if missing; `403` for scoped tokens. |
| `?setmeta` | JSON `{"tags"?: {...}, "description"?: "..."}` | `200 {"status": "ok"}`. File-only (`400` on dir, `404` if missing). At least one field required (`{}` -> `400`). `description > 2000` runes -> `400`. Empty `tags: {}` clears tags; empty `description: ""` clears description. |
| `?copy` | Header `X-Dat9-Copy-Source` (+ optional `X-Dat9-Path-Encoding: base64url`) | `200 {"status": "ok", "revision": <rev>}`. Missing header -> `400`; missing source -> `404`. |
| `?rename` | Header `X-Dat9-Rename-Source` (+ optional `X-Dat9-Path-Encoding: base64url`) | `200 {"status": "ok", "revision": <rev>}`. Replaces target file atomically, moves directory subtrees, **preserves `revision` and `resource_id`**. Destination is existing dir -> `409`. |
| `?hardlink` | Header `X-Dat9-Hardlink-Source` | `200 {"status": "ok", "revision": <rev>}`. Shares `resource_id` and `nlink` with source; content writes via any link update all aliases. Existing dst -> `409`. |
| `?symlink` | JSON `{"target": "<non-empty>"}` | `200 {"status": "ok", "revision": 1}`. Empty target -> `400`; existing dst -> `409`. |
| `?append-log` | Raw bytes + `Content-Length` + `X-Dat9-Expected-Revision` + `X-Dat9-Expected-Size` | `200 {"revision": <rev>, "size_bytes": <new_size>}`. Mismatched revision/size -> `409` (`append_log_conflict`). |
| `?append` | JSON `{append_size, part_size?, expected_revision?}` | `202` `AppendPlan`. |

### 4.11 `DELETE /v1/fs/{path}`
- Query parameters:
  - `?recursive` (`?recursive=1`): recursively deletes a directory subtree (`200 {"status": "ok"}`).
  - `?kind=file` or `?kind=dir`: optional type assertion; any other `kind` value returns `400`. `?kind=file` on a directory or `?kind=dir` on a non-directory returns `400`/`409`.
- Deleting a non-empty directory without `?recursive` returns `409` (`directory_not_empty`).
- Deleting a non-existent path returns `404`.

---

## 5. Batch Endpoints `[core]`

All batch endpoints accept `POST` only; any other HTTP method returns `405 Method Not Allowed`.

### 5.1 `POST /v1/fs:batch-stat`
- Request (`<= 1 MiB`): `{"paths": ["/a", ...], "include_checksum": false}`. Maximum **256** paths (`> 256` -> `400`).
- Response: `200 {"results": [{"path", "status": 200|404, "isDir", "size", "revision", "mtime", "btime", "mode", "hasMode", "resource_id", "nlink", "checksum_sha256"?}]}`.

### 5.2 `POST /v1/fs:batch-read-small`
- Request (`<= 1 MiB`): `{"paths": ["/a", ...], "max_bytes"?: 50000}`. Maximum **128** paths (`> 128` -> `400`). Default/ceiling `max_bytes` is `inline_threshold` (`50,000`).
- Response: `200 {"results": [...]}`:
  - Inline file `<= effective_max_bytes`: `{"path", "status": 200, "data": "<base64>", "size", "revision", "mtime"}`.
  - File `> effective_max_bytes` or non-inline storage: per-item `{"path", "status": 413, "error": "..."}` (never silently truncated).
  - Directory: per-item `status: 400`; missing: per-item `status: 404`.

### 5.3 `POST /v1/fs:batch-write`
- Advertised via `/v1/status` `storage_capabilities.batch_write_mode_v1`.
- Request (`<= 8 MiB` body, `<= 4 MiB` decoded data, `<= 128` items; exceeding request caps rejects the whole request with `400`/`413`):
  ```json
  {
    "items": [
      {"path": "/a.txt", "expected_revision": 0, "data": "<base64>", "hasMode": true, "mode": 420}
    ]
  }
  ```
- Per-item rules:
  - Item `len(data) >= inline_threshold`: per-item `status: 413` (storage untouched).
  - `expected_revision == 0`: create-only (`409` if exists); `> 0`: exact CAS (`409` on mismatch or missing); `< 0`: upsert.
  - Directory or extent target: per-item `status: 409`.
  - Winning items commit and return `{"path", "status": 200, "revision": <new_rev>}`.

### 5.4 `POST /v1/fs:batch-upload` & `/v1/fs:batch-upload:complete` `[prod-ext]`
- Grouped image batch upload (`type: "image"`, `transport: "proxy" | "presigned"`, default limit 20 items).

---

## 6. Multipart Uploads (V1 & V2) `[core]`

### 6.1 V1 Uploads (`/v1/uploads*`)
- `POST /v1/uploads` or `POST /v1/uploads/initiate`:
  - Body: `{"path", "total_size" (>0, <= max_upload_bytes), "part_checksums": ["<4B-b64>", ...], "expected_revision"?, "description"?}`.
  - `total_size > max_upload_bytes` -> `413`; missing/empty `part_checksums` -> `400`; CAS mismatch at open -> `409`.
  - Re-initiating on a path with an existing `UPLOADING` V1 session **supersedes** (aborts) the previous session so at most one V1 session is live per path.
  - Response: `202 {"upload_id", "key", "part_size", "parts": [{"number", "url", "size", "headers"?, "checksum_crc32c"?}]}`.
- `GET /v1/uploads?path=<path>[&status=uploading]`:
  - Lists sessions matching `path` and status (default `UPLOADING`): `200 {"uploads": [{"upload_id", "path", "total_size", "parts_total", "status": "UPLOADING", "created_at", "expires_at"}]}`.
- `POST /v1/uploads/{upload_id}/resume` & `GET /v1/uploads/{upload_id}/resume`:
  - Requires `part_checksums` (JSON body `{"part_checksums": [...]}`, `text/plain` lines, or `X-Dat9-Part-Checksums` header; missing -> `400`).
  - Active session -> `200` UploadPlan; aborted/completed session -> `409`; expired -> `410`; unknown -> `404`.
- `POST /v1/uploads/{upload_id}/complete`:
  - Optional body `{"tags"?, "checksum_sha256"?}` -> `200 {"status": "ok", "revision": <rev>}`.
- `DELETE /v1/uploads/{upload_id}` (and alias `POST /v1/uploads/{upload_id}/abort`):
  - Aborts an active session -> `200 {"status": "ok"}`.

### 6.2 V2 On-Demand Presigned Uploads (`/v2/uploads/*`)
- `POST /v2/uploads/initiate`:
  - Body: `{"path", "total_size", "expected_revision"?, "description"?, "layout"?: "single" | "extent"}`.
  - Response: `202 {"upload_id", "key", "part_size", "total_parts", "expires_at", "resumable", "checksum_contract": {"supported": ["SHA-256"], "required": true}}` (`part_size` = 8 MiB for `single`, 4 MiB for `extent`).
- `POST /v2/uploads/{upload_id}/presign`:
  - Body: `{"part_number": >=1, "checksum"?: {"algorithm": "sha256", "value": "<hex>"}}`.
  - Response: `200 {"number", "url", "size", "headers"?, "checksum_sha256"?, "expires_at", "skip_upload"?}`.
- `POST /v2/uploads/{upload_id}/presign-batch`:
  - Body: `{"parts": [...]}` (`<= 500` items) -> `200 {"parts": [...]}`.
- `POST /v2/uploads/{upload_id}/complete`:
  - Body: `{"parts": [{"number", "etag", "chunk"?, "off"?, "size"?}], "tags"?, "checksum_sha256"?}` (`parts: []` allowed on metadata-only extent `PATCH` truncate).
  - Response: `200 {"status": "completed", "revision": <rev>, "resource_id": "<inode>"}`.
  - Completing an aborted or already-completed session returns `409 Conflict` (`upload is not active`); expired returns `410`.
- `POST /v2/uploads/{upload_id}/abort`:
  - Aborts an active session -> `200 {"status": "ok"}`.

---

## 7. Mount Leases, SSE Events, Git Workspaces & Journals `[core]`

### 7.1 Mount Leases (`/v1/mounts`)
- `POST /v1/mounts`: body `{mount_id, mount_point, host?, pid?, mode?, ttl_seconds?}` (TTL clamped to `10..=300` seconds, default `60`). Returns `200`:
  ```json
  {
    "tenant_id": "t...",
    "mount_id": "m1",
    "mount_point": "/mnt/ws",
    "host": "node1",
    "pid": 1234,
    "mode": "read_write",
    "started_at": "2026-11-18T00:00:00Z",
    "last_seen_at": "2026-11-18T00:00:00Z",
    "expires_at": "2026-11-18T00:01:00Z",
    "heartbeat_interval_seconds": 20
  }
  ```
- `GET /v1/mounts`: lists unexpired leases `200 {"tenant_id", "mounts": [...]}`.
- `GET /v1/mounts/{mount_id}`: returns one unexpired lease (`200`) or `404` if expired/released.
- `POST /v1/mounts/{mount_id}/heartbeat`: optional `{ttl_seconds?}` -> extends `last_seen_at` and `expires_at` (`200`), or `404` if missing/expired.
- `DELETE /v1/mounts/{mount_id}`: idempotent release -> **`204 No Content`** (returns `204` even if already absent).
- Wrong HTTP methods return `405`.

### 7.2 SSE Change Stream (`GET /v1/events?since=<seq>`)
- Response: `200 text/event-stream` (`Cache-Control: no-cache`, `X-Accel-Buffering: no`).
- Frames:
  - `event: reset` (`{"seq": <head>, "reason": "initial_sync" | "seq_too_old" | "server_restart", "path"?, "op"?, "actor"?}`) — sent on `since=0` (`initial_sync`), when `since` predates retention (`seq_too_old`), and for structural namespace ops (`mkdir`, `rename`, `delete`, `copy`).
  - `event: file_changed` (`{"seq": <n>, "path": "/...", "op": "write", "actor"?: "..."}`).
  - `event: heartbeat` (`{"seq": <head>}`, emitted initially and every 30s).

### 7.3 Git Workspaces (`/v1/git-workspaces*`)
- `POST /v1/git-workspaces`: upserts `{root_path, repo_url, remote_name?, branch_name?, base_commit?, head_commit?, mode?, workspace_kind?, common_workspace_id?, worktree_name?, gitdir_rel?}` -> `200` workspace object.
  - `workspace_kind=linked` requires `common_workspace_id`, `worktree_name` (one path segment) and `gitdir_rel`; `gitdir_rel` must be `worktrees/<worktree_name>` (a leading `.git/` is accepted) -> `400` otherwise. The mount republishes a tenant git-state checkpoint at `<common>/.git/<gitdir_rel>`, so the shape is an allow-list.
- `GET /v1/git-workspaces`: without `root_path` returns `200 {"workspaces": [...]}`; with `?root_path=<path>` returns a single workspace object (`200`) or `404`.
- `GET /v1/git-workspaces/{workspace_id}` (`200` / `404`) & `DELETE /v1/git-workspaces/{workspace_id}` (`200 {"status": "ok"}`).
- Sub-resources under `/v1/git-workspaces/{workspace_id}`: `/tree` (`POST` replace, `GET ?commit_sha=`), `/git-state` (`POST|PUT|GET`), `/object-packs` (`POST|PUT|GET`) & `/object-packs/{pack_id}` (`GET`), `/overlay` (`POST|PUT|GET ?path=&limit=&after=`).

### 7.4 Append-Only Journals (`/v1/journals*`)
- `POST /v1/journals`: `{journal_id?, kind?, title?, actor?, source?, meta?, labels?: [{key, value}]}` -> `200` journal metadata.
- `POST /v1/journals/{journal_id}/entries`: requires `Idempotency-Key` header; body is single JSON entry, JSON array, or NDJSON -> `200 {"journal_id", "append_id", "first_seq", "last_seq", "count", "head_hash", "idempotent"}`. Retrying with the same `Idempotency-Key` returns the original result without duplicating entries (`idempotent: true`).
- `GET /v1/journals/{journal_id}/entries?after_seq=&limit=`: `200 application/x-ndjson` (one JSON entry per line; `404` if journal missing).
- `GET /v1/journals/{journal_id}/verify`: `200 {"ok": true, "projection_ok": true, "journal_id", "entries", "head_hash", "hash_chain_ok": true}` (unchecked fields like `seal_ok` / `artifact_bytes_available` omitted when not requested).
- `GET /v1/journal-entries`: cross-journal NDJSON search (`meta=key=val` repeatable — `meta` without `=` returns `400`; `type`, `status`, `kind`, `subject`, `actor`, `limit`, `cursor`).

---

## 8. Extended Production Surfaces `[prod-ext]`

1. **LayerFS (`/v1/layers*`, `/v1/layer-checkpoints/{id}`)**: CoW overlay layers (`POST|GET /v1/layers`, `GET|DELETE /v1/layers/{ref}?cascade=`, `.../fork`, `.../chain`, `.../diff?max_seq=&replay=&mode=log`, `.../checkpoints`, `.../rollback`, `.../commit` -> `200 committed` or `409 conflicted`, `.../entries`, `.../objects`, `.../uploads/*`, `.../events`).
2. **Tokens (`/v1/tokens*`)**: scoped token issuance (`POST /v1/tokens`), listing (`GET /v1/tokens`), rotation (`POST /v1/tokens/refresh`), revocation (`POST /v1/tokens/revoke`, `DELETE /v1/tokens/{id}`), activation/deactivation, and control-plane generation (`POST /v1/tokens/generate`).
3. **Quota (`/v1/quota`)**: `GET /v1/quota` (Drive9 key or control-plane + `tenant_id` / `X-Drive9-Tenant-ID`) & `POST /v1/quota` (control-plane).
4. **Vault (`/v1/vault/*`)**: secret CRUD (`/v1/vault/secrets*`), capability/grant token issuance (`/v1/vault/tokens*`, `/v1/vault/grants*`), audit (`/v1/vault/audit`), and capability-authenticated secret reads (`/v1/vault/read*`).
5. **Object Credentials (`POST /v1/object-credentials`)**: STS credential minting for `s3://`, `cos://`, `tos://`, `oss://`, `gs://`, `az://`.
6. **Runtime Execution (`/v1/runtime/*`)**: `GET /v1/runtime/capabilities`, streaming `POST /v1/runtime/exec` (`application/x-ndjson`), async `POST|GET /v1/runtime/executions*` + `/cancel`.
7. **Admin Control Plane (`/v1/admin/*`)**: organization tenants, tenant pools, object backends, embedding/extract provider configs, Loki log queries, and cross-region migration orchestration.

---

## 9. Error Envelopes, Status Codes & Limits

### 9.1 Error Envelopes
- Data plane: `{"error": "<message>"}` or `{"error": "<message>", "code": "<stable_code>"}`.
- Control plane: `{"error": "<message>", "code": "<stable_code>", "details"?: {...}, "action"?: "add_payment_method"}`.

### 9.2 HTTP Status Summary

| Status | Contract Meaning |
|---|---|
| `200` / `201` / `202` / `204` / `206` | Sync success / created / async plan or provision accepted / lease deleted / partial content or extent read plan |
| `302` / `307` | Presigned object GET redirect (S3-backed files / layer objects on `drive9-server`) |
| `400` | Invalid path, bad JSON, missing/multiple POST selectors, malformed header (`X-Dat9-Expected-Revision`, `X-Dat9-Content-Length`, `X-Dat9-Tag`, `X-Dat9-Path-Encoding`), `?setmeta` / `?tasks` on directory, batch count exceeded |
| `401` | Missing or invalid API key |
| `402` | Quota / free-plan payment limit |
| `403` | Scoped token denied, tenant suspended/deleted, org not allowlisted |
| `404` | Path, upload session, lease, journal, or route not found |
| `405` | Disallowed HTTP method on route |
| `409` | Path conflict (`?mkdir` / `?create` / `?symlink` / `?hardlink` on existing), CAS `expected_revision` mismatch, non-empty directory delete, layout mismatch, completing/resuming an aborted or completed upload |
| `410` | Upload session expired |
| `412` | `If-Match` precondition failed |
| `413` | Payload / `total_size` exceeds `max_upload_bytes`, or batch item exceeds `inline_threshold` / batch byte cap |
| `416` | Unsatisfiable or malformed `Range` (carries `Content-Range: bytes */<size>`) |
| `429` | Rate-limited (carries `Retry-After`) |
| `503` | Tenant `provisioning`/`pending` (`Retry-After: 1`) or transient backend unavailability |

### 9.3 Protocol Limits & Constants

| Constant | Value |
|---|---|
| Default `inline_threshold` | `50,000` bytes |
| Default `max_upload_bytes` | `10 GiB` (`10,737,418,240` bytes) |
| Classic multipart `part_size` | `8 MiB` (`MaxMultipartParts = 10,000`) |
| Extent `chunk_size` / `block_part_size` | `64 MiB` / `4 MiB` |
| `batch-stat` max paths | `256` |
| `batch-read-small` max paths / default `max_bytes` | `128` / `50,000` bytes |
| `batch-write` max items / max decoded bytes / max body | `128` / `4 MiB` / `8 MiB` |
| `presign-batch` max parts | `500` |
| `description` max length | `2,000` Unicode scalar values (runes) |
| Mount lease TTL clamp / advised heartbeat | `10s .. 300s` / `20s` |
| SSE heartbeat interval | `30s` |
