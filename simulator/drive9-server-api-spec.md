# drive9 Server API Specification

Status: **draft v1** — the backend contract the simulator's cases run
against. The simulator only talks to REAL backends (`--server`; see
ROADMAP.md); this spec documents what they must serve. Provenance tags on
every section:

- `[prod]` — the production server implementation in `tidbcloud/fs`
  (module `github.com/mem9-ai/drive9`). Machine-readable skeleton:
  `docs/api/data-plane.openapi.json` (74 paths / 92 schemas) and the two
  control-plane variants, regenerated via `make swagger`; source of truth
  `pkg/server/openapi_registry.go`.
- `[v1]` — the extent v1 data plane implemented **in this repository**
  (`pkg/server/extent_meta.go`, `pkg/datastore/jfs_meta.go`). The production
  v2 design drops these endpoints; they are specified here because the client
  in this repo speaks them.
- `[both]` — verified identical in both implementations.

## 1. Conventions

- Base URL: `http(s)://<host>/`; all business routes under `/v1` (uploads v2
  under `/v2`). Local dev default `http://127.0.0.1:9009`.
- Auth header: `Authorization: Bearer <api-key>` or `X-API-Key: <api-key>`
  `[both]`. Keys are `drive9_`-prefixed (legacy `dat9_` accepted) HS256 JWTs
  wrapped in base64url (§2).
- Paths in URLs are absolute, UTF-8, NFC-normalized, no `\`, no `..`; files
  never end in `/`, directories always do. Source paths carried in headers
  (`X-Dat9-Copy-Source` etc.) may be base64url-encoded with
  `X-Dat9-Path-Encoding: base64url` `[prod]`.
- Request/response bodies are `application/json` unless the endpoint says
  otherwise (raw octet-streams for fs reads/writes; NDJSON for journals
  entries, runtime exec, SQL-adjacent streams; `text/event-stream` for SSE).
- `Idempotency-Key` (UUID) is REQUIRED on journal entry appends and runtime
  async-job creation `[prod]`.
- Retryable responses carry `Retry-After` (seconds): 503 → 1–2s, 429 →
  endpoint-specific (§19).
- Client-canceled requests: 499, no body `[prod]`.

## 2. Authentication

### 2.1 Tenant API key `[both]`

- Format: JWT `alg=HS256`, claims `{tenant_id, token_version, iat, exp?,
  journal_permissions?}`, base64url-wrapped, prefixed `drive9_` (legacy
  `dat9_`). The meta DB stores only `sha256(token)` plus an encrypted copy.
- Validation chain: resolve by key-hash (constant-time) → key active →
  decrypt → constant-time compare plaintext → verify signature+exp → claims
  match the record. Any failure: `401 {"error":"invalid API key"}`.
- Tenant-status gating on every business request: `pending`/`provisioning` →
  `503` + `Retry-After: 1`; `failed`/`suspended`/`deleting`/`deleted` → `403`
  (SSE bus closed too).

### 2.2 Scoped tokens `[prod]`

- Issued via `/v1/tokens` with `scopes: [{prefix, ops[]}]` (explicit empty
  array rejected). Route admission: fs read/write/batch/uploads/layers are
  scoped-able; SQL, fork, events, journals, vault, object-credentials,
  data-credential are permanently denied; chmod is never allowed. Each path is
  additionally authorized per-scope (`authorizeFS`).
- Scoped tokens submitting `checksum_sha256` on upload complete → `403`.

### 2.3 Control-plane credentials `[prod]`

Three mutually exclusive forms; mixing → `400 ambiguous_credentials`:
`X-TiDBCloud-Public-Key` + `X-TiDBCloud-Private-Key` headers (or body
`public_key`/`private_key`); TiDB Cloud JWT (`X-TiDB-Cloud-Authorization` +
`?orgId=`); drive9 Bearer (only `GET /v1/quota` and `/v1/tokens*`).

### 2.4 Vault capability tokens `[prod]`

`vault_` (legacy capability) and `vt_` (delegated grant) prefixes, accepted
only by `/v1/vault/read*`; full HMAC/TTL/revocation check happens in the
handler, middleware only loads the tenant.

## 3. Lifecycle

### 3.1 `GET /healthz` `[both]`

No auth. `200 {"status":"ok"}`.

### 3.2 `POST /v1/provision` `[prod]` (local provider in `[v1]` serves the
same response shape)

Auth: control-plane (§2.3); anonymous allowed only when
`DRIVE9_ALLOW_ANONYMOUS_PROVISION` is set with a complete default keypair, or
under `provider=local` on loopback.

Body (≤1MiB, `DisallowUnknownFields`):
`{public_key?, private_key?, max_storage_size?(Mi), max_file_size?(Mi),
max_file_count?, max_media_llm_files?, max_video_llm_files?,
tidbcloud_spending_limit?(RMB, 0 or ≥10)}` — quota fields validated
(`max_file_size` ≤ server max upload, etc.; violations 400/402 with
`details`).

`202` → `{tenant_id, api_key, status, cloud_provider?, region?, created_by?}`.
Free-plan over-limit → `402` with
`details {max_storage_bytes, max_file_size_bytes, max_file_count}` and
`action: add_payment_method`. Pool claim failure → `409
tenant_pool_claim_failed`.

### 3.3 `GET /v1/status` — self-authenticated (bearer)

`[v1]` response: `{status, kind?, message?, max_upload_bytes,
inline_threshold?}` — `inline_threshold` is the DB-inline vs S3 cutoff
clients use to pick simple PUT vs multipart; omitted by old servers.

`[prod]` additionally reports `migration_capabilities {checksum_read,
checksum_complete, conditional_create, conditional_update, event_ingest}`
(all true) and `storage_capabilities {append_log_v1?, batch_write_mode_v1?}`
derived from the serving backend.

### 3.4 `DELETE /v1/tenant` `[both]` — tenant Bearer; deletes the tenant.

### 3.5 `POST /v1/fork` `[prod]` — tenant Bearer; forks a tenant.

## 4. fs domain — `/v1/fs/{path}` `[both]` unless noted

### 4.1 `GET` (read)

- Headers: `Range` (single range), `If-Match`, `If-Range`.
- 200 inline `application/octet-stream` + `ETag` (strong), `Accept-Ranges`,
  `X-Dat9-Resource-ID`, `X-Dat9-Revision`.
- S3-stored large file `[prod]`: `302` redirect to a presigned URL.
- extent layout `[prod v2]`: `206` + JSON block read plan, headers
  `X-Dat9-Content-Layout: extent`, `X-Dat9-Storage-Type: extent`; plan carries
  presigned block GETs, zero-holes, `next_offset`.
- append-log layout: handed to the workspace append-read handler.
- Errors: `416` + `Content-Range: bytes */size`; `412` If-Match failure.

### 4.2 `GET ?stat=1`

`{size, isdir, resource_id, nlink, revision, mtime, btime, content_type,
semantic_text, description, tags}` + headers `X-Dat9-Mtime`, `X-Dat9-Btime`.

### 4.3 `GET ?list=1`

`{entries: [{name, size, isDir, mtime, revision, mode, hasMode, resource_id,
nlink}]}`.

### 4.4 `GET ?grep=<q>&limit=&layer=`

`grep` required (empty → 400); `limit` default 20. `?find=1` with
`name,tag,newer,older,minsize,maxsize,limit,layer` → matched nodes.

### 4.5 `GET ?tasks=1`

File task statuses (incl. `last_error`, LLM usage); success carries response
header `X-Dat9-Tasks: 1`.

### 4.6 `HEAD`

Headers only: `ETag`, `Accept-Ranges`, `X-Dat9-*` size/isdir/revision/mtime/
btime. 404 semantics: plain `404`.

### 4.7 `PUT` (write)

- Body: raw bytes. Headers: `X-Dat9-Expected-Revision` (default −1),
  `X-Dat9-Tag` (repeatable `key=value`), `X-Dat9-Content-Length`,
  `X-Dat9-Part-Checksums` (4-byte base64 values, comma-separated),
  `X-Dat9-Description` (≤2000 runes).
- 200 `{"status":"ok","revision":N}`; size ≥ inline threshold → `202` +
  UploadPlan (§6); append-log target → 200 + `X-Dat9-Revision` + body
  `{revision, size_bytes}`.
- Errors: `409` path conflict / revision CAS; `413` over limit; `402` quota
  (uploads path uses `413`/`507`).

### 4.8 `PATCH`

- single layout: `{new_size, dirty_parts:[1-based], part_size?,
  expected_revision?}` → `202` PatchPlan `{upload_id, part_size, upload_parts:
  [{number,url,size,headers,expires_at,read_url,read_headers}],
  copied_parts}`.
- extent layout `[v1/v2 client]`: `{new_size, dirty_blocks:[{chunk, off,
  size, checksum}], expected_revision (required)}` → `202` ExtentWritePlan.

### 4.9 `POST` selectors

| selector | input | notes |
|---|---|---|
| `?append-log=1` | raw body + `Content-Length` + `X-Dat9-Expected-Revision` + `X-Dat9-Expected-Size` (both required) | 200 `{revision,size_bytes}`; errors carry `append_log_*` codes (§19) |
| `?append=1` | `{append_size, part_size?, expected_revision?}` | `202` AppendPlan `{base_size, …PatchPlan}` |
| `?copy=1` | header `X-Dat9-Copy-Source` | `copy_on_write_unsupported` → 400 on unsupported backends |
| `?rename=1` | header `X-Dat9-Rename-Source` | |
| `?mkdir=1[&mode=]` | — | |
| `?chmod=1` | `{mode}` | owner-only (scoped always 403) |
| `?setmeta=1` | `{tags?, description?}` | |
| `?create=1` | — | 409 on create race |
| `?symlink=1` | `{target}` | |
| `?hardlink=1` | header `X-Dat9-Hardlink-Source` | |

Unknown selector → `400`. Selector matching is by KEY PRESENCE: the real
Go client sends bare selectors (`?rename`, `?mkdir`, `?copy`), while the
documented form is `?selector=1`; a backend must accept both. `?mkdir`
auto-creates missing ancestors (the backend's EnsureParentDirs semantics)
and so do first writes into fresh trees (PUT/rename/copy destinations).

### 4.10 `DELETE`

`?recursive=1`, `?kind=file|dir`. 200 `{"status":"ok"}`. Recursive delete
incomplete → `503` + `Retry-After: 2`.

## 5. batch endpoints `[both]`

| endpoint | body | limits |
|---|---|---|
| `POST /v1/fs:batch-stat` | `{paths[≤256], include_checksum?}` | 256 paths |
| `POST /v1/fs:batch-read-small` | `{paths[≤128], max_bytes?}` | 128 paths, ≤50,000 bytes total |
| `POST /v1/fs:batch-write` | `{items:[{path, expected_revision, data(base64), mode?, hasMode}]}` | 128 items, ≤4MiB data, default body cap 8MiB |
| `POST /v1/fs:batch-upload` | `{type:"image", items:[{id,path,expected_revision,data(base64)}]}` | per-item results |

`batch_write_mode_v1` capability is advertised via `/v1/status`
`storage_capabilities` `[prod]`.

## 6. uploads

### 6.1 V1 (presigned parts) `[both]`

- `POST /v1/uploads` (or `/v1/uploads/initiate`): `{path, total_size (>0,
  ≤max_upload_bytes), part_checksums[] (each 4-byte base64, REQUIRED),
  expected_revision?, description?}` → `202` UploadPlan `{upload_id, key,
  part_size, parts:[UploadPartURL]}`.
- `POST /v1/uploads/{id}/complete`: body `{tags?, checksum_sha256? (64-hex
  lowercase)}` or empty → 200 `{"status":"ok"}`.
- `POST|GET /v1/uploads/{id}/resume`: `{part_checksums}` (body) or
  `X-Dat9-Part-Checksums` header → 200 UploadPlan; `410` expired.
- `DELETE /v1/uploads/{id}` (abort) → 200.
- `GET /v1/uploads?path=&status=` → list `{uploads:[{upload_id, path,
  total_size, parts_total, status, created_at, expires_at}]}`.
- Every complete/resume/abort re-authorizes the target path under the
  current request scope.

### 6.2 V2 (on-demand presign) `[prod]`

- `POST /v2/uploads/initiate`: `{path, total_size, expected_revision?,
  description?, layout?("single"|"extent")}` → `202` UploadPlanV2
  `{upload_id, key, part_size, total_parts, expires_at, resumable,
  checksum_contract:{supported:["SHA-256"],required:true}}`. extent branch is
  content-addressed (block sha256).
- `POST /v2/uploads/{id}/presign`: `{part_number ≥1, checksum?{algorithm,
  value}}` → `200` UploadPartURL `{number,url,size,checksum_sha256?,
  checksum_crc32c?, headers?, expires_at, skip_upload?}`.
- `POST …/presign-batch`: `{parts:[…]}` ≤500 → `{parts:[…]}`.
- `POST …/complete`: `{parts:[CompletePart…], tags?, checksum_sha256?}`;
  extent parts carry `etag=sha256, chunk, off, size`.
- `POST …/abort`.
- Presign format: AWS SigV4, **header hoisting disabled, URI path escaping
  disabled**; checksums signed in (`x-amz-checksum-sha256` /
  `-crc32c`) and echoed in `headers`. GCS/Azure/local variants exist.
- `410 Gone` on expired sessions (resume/presign).

## 7. extent v1 data plane `[v1]`

> Divergence: `[prod]` v2 removes both endpoints below. Reads become the
> `206` block plan (§4.1); writes become V2 `layout=extent` + `PATCH
> dirty_blocks`; compaction/GC move server-side.

### 7.1 `POST /v1/data-credential`

- Owner Bearer required; scoped → `403 "scoped token cannot mint data
  credentials"`. Empty body.
- `200` → `{scheme("file"|"s3"), endpoint, bucket?, prefix:"t/<tenant_id>/",
  tenant_id?, access_key_id?, secret_access_key?, session_token?, region?,
  force_path_style?, encryption_mode?, encryption_key_id?, expires_at?
  (RFC3339)}`.
- Mint branches (in order): LocalS3Client → `scheme=file` (endpoint = objects
  dir, no isolation); AWSS3Client STS (`CanMintSTS`) → AssumeRole 1h with
  inline policy scoped to `t/<tenant>/` (the only enforced isolation);
  AWSS3Client static (`CanMintStatic`) → **requires**
  `DRIVE9_EXTENT_ALLOW_STATIC_CREDENTIALS ∈ {1,true,yes,on}`, else `501`
  (`errNoExtentCredentials`). Deployment requiring per-object SSE → `501`
  (JuiceFS S3 backend cannot set SSE headers; bucket-default encryption is
  the supported mode, reported via `encryption_mode`).

### 7.2 `POST /v1/extent/meta`

- Tenant Bearer (scoped: admitted via the fs whitelist). Op selector: header
  `X-Drive9-Extent-Op` or query `op=` (missing → 400). Body ≤4MiB JSON
  (empty → `{}`).
- Dispatch: read-only ops run on a transaction-free fast path; all others in
  one write transaction with soft-quota admission.
- Response: HTTP `200` normally, `500` iff the op's errno is `EIO`. Body is
  the op's JSON, conventionally `{"errno": <int>}` plus payload — Linux
  syscall errno semantics (`EINVAL` bad input, `EIO` internal, `ENOSYS`
  unknown op, `EAGAIN` for blocking flock retry, …).
- Op registry (grouped):
  - read-only fast path: `get_counter, lookup, getattr, readlink, readdir,
    read`
  - write transaction: `get_counter, incr_counter, set_if_small, load, init,
    new_session, refresh_session, lookup, getattr, setattr, mknod, readlink,
    unlink, rmdir, rename, readdir, read, write, delete_slice, truncate,
    open_ref, delete_sustained, flock, getlk, setlk, find_stale_sessions,
    clean_stale_session, get_session`
  - server tasks: `claim_compact, requeue_compact, complete_compact
    ({task_id, receipt}), compact`
- Backing tables (per-tenant schema): `jfs_node, jfs_edge, jfs_chunk,
  jfs_delfile, jfs_session, jfs_counter, jfs_compact` — JuiceFS-compatible
  metadata (see `pkg/datastore/jfs_*.go`); observable via `/v1/sql` (§8).

## 8. `POST /v1/sql` `[both]`

Body `{"query": string}` ≤1MiB; scoped tokens denied. `200` = JSON array of
row objects (column → value) from the tenant backend's `ExecSQL`. Client-
caused SQL errors → `400` (message = driver error); server-side → 5xx per
§19.

## 9. events — `GET /v1/events?since=<seq>` `[both]`

`text/event-stream` + `X-Accel-Buffering: no`. Event types:
- `file_changed` (ChangeEvent JSON), `reset` `({seq, reason, path?, op?,
  actor?})`, `heartbeat` `({seq}`, every 30s).
- Structured ops (rename/delete/mkdir/copy) are always emitted as `reset`.
- Reset reasons: `initial_sync`, `seq_too_old`, `server_restart`.
- Disconnects: tenant deactivated mid-stream → `403`; replay query failure →
  stream drops silently (no heartbeat), client reconnects from its persisted
  cursor; tenant deletion drain terminates.

## 10. journals `[prod]`

- `POST /v1/journals`: `{journal_id?, kind?, title?, actor?, source?, meta?,
  labels?, retention?}` → journal JSON.
- `POST /v1/journals/{id}/entries`: array / single / NDJSON; header
  `Idempotency-Key` REQUIRED → `{journal_id, append_id, first_seq, last_seq,
  count, head_hash, idempotent}`.
- `GET /v1/journals/{id}/entries?after_seq|after=&limit=`: NDJSON stream.
- `GET /v1/journals/{id}/verify`: `{ok, journal_id, entries, head_hash,
  hash_chain_ok, seal_ok?, projection_ok?, artifact_bytes_available?,
  head_sealed?, latest_seal_seq?}`.
- `GET /v1/journal-entries`: cross-journal search (many filters), NDJSON,
  last line carries the cursor.

## 11. git-workspaces `[prod]`

CRUD `POST|GET /v1/git-workspaces` (`{root_path, repo_url, remote_name?,
branch_name?, base_commit?, head_commit?, mode?, workspace_kind?,
common_workspace_id?, worktree_name?, gitdir_rel?}`); `GET|DELETE
/v1/git-workspaces/{id}`; `POST|GET …/tree` (GET needs `?commit_sha=`);
`POST|PUT|GET …/git-state`; `POST|PUT|GET …/object-packs` and
`…/object-packs/{pack_id}`; `POST|PUT|GET …/overlay[?path=]`.

## 12. layers (LayerFS) `[prod]`

`POST /v1/layers` (`{layer_id?, base_root_path (required), name?, tags?,
durability_mode?, actor_id?}`); list/get/delete (`?cascade=`); `…/fork`;
`…/chain`; `…/diff?max_seq=&replay=&mode=log`; `…/checkpoints`; `…/rollback`;
`…/commit` (200 committed / 409 + conflicts); layer uploads reuse the V2
protocol (`…/uploads/initiate|presign|presign-batch|complete|abort`);
entries `POST|PUT|GET …/entries?path=&max_seq=`; objects
`GET …/objects?path=` (307 → S3) / `POST|PUT …/objects`; events
`GET …/events?since=`; `GET /v1/layer-checkpoints/{id}`.

## 13. `POST /v1/object-credentials` `[prod]`

`{uri, write}` owner-only (scoped 403) → `{access_key_id?, secret_access_key?,
session_token?, sas_url?, access_token?, account?, expiration?, endpoint?,
region?, force_path_style, scheme, bucket, prefix}`. Schemes: s3/cos/tos/oss/
gs/az. Errors: 401 unauthenticated; 403 scoped / no namespace / backend
mismatch; 409 org unbound / ambiguous backend.

## 14. quota `[prod]`

`GET /v1/quota?tenant_id=` (or header `X-Drive9-Tenant-ID`); drive9 key
returns own tenant. `POST /v1/quota` control-plane; `402` free-plan family;
`409 free_quota_busy`.

## 15. tokens `[both]`

- `POST /v1/tokens` (owner): `{subject?, ttl_seconds, scopes[{prefix,
  ops[]}]}` → `{token, token_id?, subject?, scope_kind, expires_at?, scopes}`.
- `GET /v1/tokens?include_expired=&offset=&limit=[&tenant_id=]`.
- `POST /v1/tokens/refresh` `{ttl_seconds?}` (works with scoped tokens).
- `POST /v1/tokens/revoke` (control plane: by API key; data plane:
  `{api_key}`).
- `DELETE /v1/tokens/{id}`, `POST …/activate`, `POST …/deactivate`.
- `POST /v1/tokens/generate` (control plane): `{tenant_id, key_name,
  ttl_seconds?, scopes?, public_key?, private_key?}`.

## 16. vault `[prod]`

Management (tenant Bearer): `GET|POST /v1/vault/secrets`; `GET|PUT|DELETE
/v1/vault/secrets/{name}`; `GET …/value[?format=env]`;
`GET …/value/{field}`; `POST /v1/vault/tokens`; `DELETE
/v1/vault/tokens/{id}`; `POST /v1/vault/grants`; `DELETE
/v1/vault/grants/{id}`; `GET /v1/vault/audit?secret=&limit=`.
Read (capability tokens §2.4): `GET /v1/vault/read`, `/v1/vault/read/{name}
[?format=env]`, `/v1/vault/read/{name}/{field}`.

## 17. runtime/exec `[prod]` (only when runtime is configured)

- `GET /v1/runtime/capabilities` → `{version, provider, persistence,
  workspace_path, user, streaming, cancel, heartbeat_ms,
  default_timeout_ms, max_timeout_ms, max_output_bytes,
  max_active_per_tenant, warm_sandbox_idle_ms, async?}`.
- `POST /v1/runtime/exec` (owner key): `{argv[], cwd?, env?, workspace{root?,
  layer_id?, read_only?, persistence?}, timeout_ms?, fresh?}` → `200`
  `application/x-ndjson` frames `{type: started|stdout|stderr|heartbeat|
  exit|error, execution_id, cwd, sandbox, data_base64, exit_code,
  duration_ms, error_code, message}`. Exactly one terminal (exit|error)
  frame. `429 busy` + `Retry-After: 5`. Error codes: `invalid_request,
  unavailable, start_failed, timed_out, canceled, output_limit,
  outcome_unknown`.
- `POST /v1/runtime/executions` (async; `Idempotency-Key` UUID) → `202`/`200`
  Job; `409` conflict; `503` uncertain. `GET …/executions/{id}`;
  `POST …/executions/{id}/cancel` (200/202/404/409/503).

## 18. control plane & misc

- `GET|POST /v1/admin/tenants`, `GET|PATCH|DELETE
  /v1/admin/tenants/{tenant_id}` (PATCH `{display_name?, label?}` ≥1 field),
  `POST …/quota`; `GET|PUT …/embedding-config`; `GET|PUT
  …/extract-config/{media_type:image|audio|video}` (`{enabled, api_base,
  api_key, model, prompt, protocol, max_image_pixels?}`); `GET …/logs?
  keyword=&time_range=&limit=` (default 5m/100, max 30m/1000; `403
  log_query_org_not_allowed`; `429 rate_limited`); `GET|PUT|DELETE
  …/object-namespace`; `GET|POST /v1/admin/object-backends`,
  `GET|PATCH|PUT|DELETE …/{backend_id}`; `POST|GET|PATCH|DELETE
  /v1/admin/tenant-pool` `[prod]`.
- `[v1]` only: `POST /v1/auth/slock/login|callback`; `/metrics` on the main
  mux (prod serves `/metrics` on a dedicated listener).

## 19. Error model

### 19.1 Envelopes

- Data plane: `{"error": "<msg>"}` or `{"error": "<msg>", "code":
  "<stable>"}`; retryable variant adds `Retry-After: 1` with 503. Internal
  storage failures use opaque messages (`"storage backend unavailable;
  contact support"`); only sentinel-listed errors pass their text through.
- Control plane: `{"error", "code", "details"?, "action"?}`; `action:
  add_payment_method` is derived from free-plan codes.

### 19.2 HTTP status semantics (backend error → status)

| status | meaning |
|---|---|
| 400 | invalid path / append-log unsupported / copy-on-write unsupported / bad JSON / ambiguous credentials |
| 401 | unauthenticated / bad key |
| 402 | quota (data plane); uploads path uses 413/507 |
| 403 | forbidden (scoped, suspended, chmod non-owner, org not allowed) |
| 404 | not found |
| 405 | wrong method |
| 409 | dir-not-empty, content-layout conflict, path conflict (create race), revision CAS conflict, upload not active, extent layout mismatch |
| 410 | upload session expired |
| 412 | If-Match failure |
| 413 | upload too large / body too large |
| 416 | invalid/unsatisfiable Range (+`Content-Range: bytes */size`) |
| 429 | rate limited (logs; runtime busy, `Retry-After: 5`) |
| 499 | client canceled (no body) |
| 502 | schema migration / upstream failure (TiDB Cloud) |
| 503 | delete incomplete (+`Retry-After: 2`), tenant provisioning/pending (+1), shutdown |
| 504 | upstream timeout |
| 507 | quota on upload completion |
| 501 | extent credentials unavailable (v1) |

### 19.3 Stable codes

- Control plane: `free_tenant_limit_reached, free_quota_busy,
  free_quota_exceeded, free_spending_limit_forbidden,
  free_quota_mutation_forbidden, free_tenant_pool_forbidden,
  spending_limit_invalid, quota_permission_denied,
  quota_backend_not_found, extract_config_not_enabled,
  credentials_required, billing_unavailable, tenant_pool_claim_failed`,
  status fallbacks `client_canceled, unauthorized, forbidden,
  payment_required, not_found, not_enabled, method_not_allowed,
  invalid_request, conflict, upstream_failed, upstream_timeout,
  unavailable, failed`.
- Data plane (with `code` field): `append_log_invalid_request,
  append_log_unsupported, append_log_conflict, append_log_too_large,
  copy_on_write_unsupported, invalid_range`, runtime `invalid_request,
  unavailable, start_failed, timed_out, canceled, output_limit,
  outcome_unknown, busy`, logs `log_query_org_not_allowed, rate_limited,
  payload_too_large`, inner-auth `ambiguous_credentials/invalid_request,
  unauthorized, credentials_required, forbidden, upstream_timeout,
  upstream_failed, unavailable`, v1 extent `errNoExtentCredentials` (501).

### 19.4 errno

Linux errno appear **literally** only in `/v1/extent/meta` bodies (`{"errno":
N}`; EIO also flips HTTP to 500; unknown op → ENOSYS). Everywhere else the
client maps HTTP status + `code` → its own errno surface at the FUSE layer;
there is no server-side errno table beyond §19.2/§19.3 (this spec is that
table).

## 20. Limits & constants

| constant | value |
|---|---|
| maxBatchStatPaths | 256 |
| maxBatchReadSmallPaths / Bytes | 128 / 50,000 |
| maxBatchWriteItems / Bytes / default body | 128 / 4MiB / 8MiB |
| MaxMultipartParts | 10,000 |
| MaxPresignBatch | 500 |
| MaxDescriptionLen | 2,000 runes |
| provision/SQL body cap | 1MiB |
| extent/meta body cap | 4MiB |
| SSE heartbeat | 30s |
| STS extent session TTL | 1h |
| admin logs rate limit | 60/min, burst 10 (env-tunable) |
