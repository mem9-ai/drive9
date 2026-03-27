# Dat9 Tenant Provisioning Design (DB9 / TiDB Zero / TiDB Starter)

## 1. Goal and Scope

This design defines how dat9 provisions a database for a new tenant, initializes the required schema, and stores connection credentials securely.

- Support three provider options: `db9`, `tidb_zero`, `tidb_cloud_starter`
- `db9` and `zero/starter` are peer options, selected by config
- Provider selection is explicit only: `db9` / `tidb_zero` / `tidb_cloud_starter`
- Tenant DB password must be encrypted with KMS before persistence

---

## 2. Key Decisions

- The dat9 data schema includes only: `file_nodes`, `files`, `file_tags`, `uploads`
- FTS and Vector are required capabilities (on `files`)
- Encryption uses a unified abstraction: `Encryptor` (`Encrypt/Decrypt`) with a `plain/md5/kms` factory
- `tenant_id` is the canonical tenant identity across all layers
- API keys are JWT-based and stored in a dedicated `tenant_api_keys` table

---

## 3. Architecture

### 3.1 Provisioning Flow

1. API receives `CreateTenant`
2. `TenantService` selects a provisioner (`db9` / `zero` / `starter`)
3. Provisioner returns connection info (`host/port/user/password/dbname/cluster_id`)
4. `SchemaInitializer` initializes dat9 schema (including FTS + Vector)
5. `Encryptor(kms)` encrypts DB password
6. Persist tenant metadata in control-plane meta DB
7. Issue a default JWT API key (non-expiring machine key)
8. Return create result (`tenant_id` + `api_key`)

### 3.2 Provider Selection (Config-Driven)

No automatic fallback. Provider is selected strictly by config:

```text
DAT9_TENANT_PROVIDER=db9 | tidb_zero | tidb_cloud_starter
```

Behavior:
- `db9`: use `DB9Provisioner` (if DB9 create API is unavailable, return error)
- `tidb_zero`: use `TiDBZeroProvisioner`
- `tidb_cloud_starter`: use `TiDBStarterProvisioner`

---

## 4. Encryption Abstraction

### 4.1 Interface

```go
type Encryptor interface {
    Encrypt(ctx context.Context, plaintext string) (string, error)
    Decrypt(ctx context.Context, ciphertext string) (string, error)
}
```

### 4.2 Factory Types

- `plain`: plaintext pass-through (local dev only)
- `md5`: symmetric encryption (non-production optional)
- `kms`: AWS KMS (production default)

Recommended defaults:

```text
DAT9_ENCRYPT_TYPE=kms
DAT9_ENCRYPT_KEY=alias/dat9-<env>-db-password
```

### 4.3 KMS Storage Convention

- Persist `base64(ciphertextBlob)`
- Decrypt does not require key id (ciphertext carries key metadata)

---

## 5. Provisioner Design

### 5.1 Unified Interface

```go
type Provisioner interface {
    Provision(ctx context.Context, req ProvisionRequest) (*ClusterInfo, error)
    ProviderType() string // db9 | tidb_zero | tidb_cloud_starter
}

type ClusterInfo struct {
    TenantID   string
    ClusterID  string
    Host       string
    Port       int
    Username   string
    Password   string // plaintext only in memory during provisioning
    DBName     string
    Provider   string
    ClaimURL   string     // zero optional
    ClaimUntil *time.Time // zero optional
}
```

### 5.2 DB9Provisioner

- DB9 create API: pending
- Reserve interface path for `CreateInstance(tag, root_password, ...)`

### 5.3 TiDBZeroProvisioner (available now)

- Suitable for dev or DB9-not-available scenarios
- Call Zero API to create temporary instance
- Return `claim_url/claim_expires_at`

### 5.4 TiDBStarterProvisioner (available now)

- Suitable for prod or non-zero usage
- Use TiDB Cloud Pool takeover API
- Digest auth

---

## 6. Dat9 Schema

### 6.1 Common Logical Requirement

Every tenant DB must initialize:

1. `file_nodes`
2. `files`
3. `file_tags`
4. `uploads`

And `files` must support:

- FTS on `content_text`
- Vector similarity on `embedding`

### 6.2 TiDB Variant (zero/starter)

```sql
CREATE TABLE IF NOT EXISTS file_nodes (
  node_id VARCHAR(255) PRIMARY KEY,
  path VARCHAR(4096) NOT NULL,
  parent_path VARCHAR(4096) NOT NULL,
  name VARCHAR(255) NOT NULL,
  is_directory TINYINT NOT NULL DEFAULT 0,
  file_id VARCHAR(255),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE INDEX idx_path(path),
  INDEX idx_parent(parent_path),
  INDEX idx_file_id(file_id)
);

CREATE TABLE IF NOT EXISTS files (
  file_id VARCHAR(255) PRIMARY KEY,
  storage_type VARCHAR(50) NOT NULL,
  storage_ref VARCHAR(4096) NOT NULL,
  content_type VARCHAR(255),
  size_bytes BIGINT NOT NULL DEFAULT 0,
  checksum_sha256 VARCHAR(64),
  revision BIGINT NOT NULL DEFAULT 1,
  status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  source_id VARCHAR(255),
  content_text LONGTEXT,
  embedding VECTOR(1536) NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  confirmed_at DATETIME,
  expires_at DATETIME,
  INDEX idx_status(status, created_at)
);

CREATE TABLE IF NOT EXISTS file_tags (
  file_id VARCHAR(255) NOT NULL,
  tag_key VARCHAR(255) NOT NULL,
  tag_value VARCHAR(255) NOT NULL DEFAULT '',
  PRIMARY KEY(file_id, tag_key),
  INDEX idx_kv(tag_key, tag_value)
);

CREATE TABLE IF NOT EXISTS uploads (
  upload_id VARCHAR(255) PRIMARY KEY,
  file_id VARCHAR(255) NOT NULL,
  target_path VARCHAR(4096) NOT NULL,
  s3_upload_id VARCHAR(255) NOT NULL,
  s3_key VARCHAR(4096) NOT NULL,
  total_size BIGINT NOT NULL,
  part_size BIGINT NOT NULL,
  parts_total INT NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'UPLOADING',
  fingerprint_sha256 VARCHAR(64),
  idempotency_key VARCHAR(255),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  expires_at DATETIME NOT NULL,
  INDEX idx_upload_path(target_path, status),
  UNIQUE INDEX idx_idempotency(idempotency_key)
);

-- Required: FTS
ALTER TABLE files
  ADD FULLTEXT INDEX idx_fts_content(content_text)
  WITH PARSER MULTILINGUAL
  ADD_COLUMNAR_REPLICA_ON_DEMAND;

-- Required: Vector
ALTER TABLE files
  ADD VECTOR INDEX idx_files_cosine((VEC_COSINE_DISTANCE(embedding)))
  ADD_COLUMNAR_REPLICA_ON_DEMAND;
```

### 6.3 DB9/PostgreSQL Variant (preferred target)

```sql
CREATE TABLE IF NOT EXISTS file_nodes (
  node_id VARCHAR(255) PRIMARY KEY,
  path VARCHAR(4096) NOT NULL,
  parent_path VARCHAR(4096) NOT NULL,
  name VARCHAR(255) NOT NULL,
  is_directory BOOLEAN NOT NULL DEFAULT FALSE,
  file_id VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_path ON file_nodes(path);
CREATE INDEX IF NOT EXISTS idx_parent ON file_nodes(parent_path);
CREATE INDEX IF NOT EXISTS idx_file_id ON file_nodes(file_id);

CREATE TABLE IF NOT EXISTS files (
  file_id VARCHAR(255) PRIMARY KEY,
  storage_type VARCHAR(50) NOT NULL,
  storage_ref VARCHAR(4096) NOT NULL,
  content_type VARCHAR(255),
  size_bytes BIGINT NOT NULL DEFAULT 0,
  checksum_sha256 VARCHAR(64),
  revision BIGINT NOT NULL DEFAULT 1,
  status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  source_id VARCHAR(255),
  content_text TEXT,
  embedding vector(1536),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  confirmed_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_status ON files(status, created_at);

CREATE TABLE IF NOT EXISTS file_tags (
  file_id VARCHAR(255) NOT NULL,
  tag_key VARCHAR(255) NOT NULL,
  tag_value VARCHAR(255) NOT NULL DEFAULT '',
  PRIMARY KEY(file_id, tag_key)
);
CREATE INDEX IF NOT EXISTS idx_kv ON file_tags(tag_key, tag_value);

CREATE TABLE IF NOT EXISTS uploads (
  upload_id VARCHAR(255) PRIMARY KEY,
  file_id VARCHAR(255) NOT NULL,
  target_path VARCHAR(4096) NOT NULL,
  s3_upload_id VARCHAR(255) NOT NULL,
  s3_key VARCHAR(4096) NOT NULL,
  total_size BIGINT NOT NULL,
  part_size BIGINT NOT NULL,
  parts_total INT NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'UPLOADING',
  fingerprint_sha256 VARCHAR(64),
  idempotency_key VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_upload_path ON uploads(target_path, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_idempotency ON uploads(idempotency_key);

-- Required: FTS
CREATE INDEX IF NOT EXISTS idx_fts_content
  ON files USING GIN (to_tsvector('simple', COALESCE(content_text, '')));

-- Required: Vector
CREATE INDEX IF NOT EXISTS idx_files_embedding_hnsw
  ON files USING hnsw (embedding vector_cosine_ops);
```

---

## 7. Control Plane Persistence and Auth

### 7.1 Meta DB Model

Tenant metadata and DB connection info are stored in `tenants`.

```sql
CREATE TABLE IF NOT EXISTS tenants (
  id               VARCHAR(36)   PRIMARY KEY,

  -- DB connection (password stored as ciphertext)
  db_host          VARCHAR(255)  NOT NULL,
  db_port          INT           NOT NULL,
  db_user          VARCHAR(255)  NOT NULL,
  db_password      TEXT          NOT NULL,  -- KMS-encrypted base64 ciphertext
  db_name          VARCHAR(255)  NOT NULL,
  db_tls           TINYINT(1)    NOT NULL DEFAULT 1,

  -- Provision metadata
  provider         VARCHAR(50)   NOT NULL,  -- db9|tidb_zero|tidb_cloud_starter
  cluster_id       VARCHAR(255)  NULL,
  claim_url        TEXT          NULL,
  claim_expires_at TIMESTAMP     NULL,

  -- Lifecycle
  status           VARCHAR(20)   NOT NULL DEFAULT 'provisioning',
  schema_version   INT           NOT NULL DEFAULT 1,
  created_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  deleted_at       TIMESTAMP     NULL,

  INDEX idx_tenant_status (status),
  INDEX idx_tenant_provider (provider)
);

CREATE TABLE IF NOT EXISTS tenant_api_keys (
  id               VARCHAR(36)   PRIMARY KEY,
  tenant_id        VARCHAR(36)   NOT NULL,

  -- JWT metadata
  key_name         VARCHAR(64)   NOT NULL DEFAULT 'default',
  jwt_ciphertext   TEXT          NOT NULL,  -- KMS-encrypted base64 ciphertext
  jwt_hash         VARCHAR(128)  NOT NULL,  -- sha256(raw_jwt)
  token_version    INT           NOT NULL DEFAULT 1,

  status           VARCHAR(20)   NOT NULL DEFAULT 'active', -- active|revoked
  issued_at        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  revoked_at       TIMESTAMP     NULL,
  created_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  UNIQUE INDEX idx_api_keys_hash (jwt_hash),
  INDEX idx_api_keys_tenant (tenant_id, status),
  UNIQUE INDEX idx_api_keys_tenant_name (tenant_id, key_name)
);
```

Field conventions:
- `tenants.id`: canonical tenant identity (tenant scope root)
- `tenants.db_password`: ciphertext only, never plaintext
- `tenants.status`: `provisioning|active|suspended|deleted`
- `tenant_api_keys.jwt_ciphertext`: encrypted JWT, decrypted only at runtime
- `tenant_api_keys.jwt_hash`: `sha256(raw_jwt)` for lookup and dedup
- `tenant_api_keys.token_version`: supports key versioning and global invalidation patterns
- `tenant_api_keys.status`: `active|revoked`

Read/write rules:
- Write encrypted values only (`db_password`, `jwt_ciphertext`)
- Decrypt only in memory at runtime
- Never log plaintext secrets or ciphertext payloads

### 7.2 Environment Variables

```bash
# provider selection
DAT9_TENANT_PROVIDER=db9|tidb_zero|tidb_cloud_starter

# encryption
DAT9_ENCRYPT_TYPE=kms
DAT9_ENCRYPT_KEY=alias/dat9-<env>-db-password

# db9 (preferred)
DAT9_DB9_API_URL=https://<db9-api>
DAT9_DB9_API_KEY=<token>

# tidb zero
DAT9_ZERO_API_URL=https://<zero-api>

# tidb starter
DAT9_TIDBCLOUD_API_URL=https://<tidb-cloud-api>
DAT9_TIDBCLOUD_API_KEY=<key>
DAT9_TIDBCLOUD_API_SECRET=<secret>
DAT9_TIDBCLOUD_POOL_ID=<pool-id>

# meta db
DAT9_META_DSN=<meta-db-dsn>
```

### 7.3 API Key (JWT) Auth and Tenant Scope

#### 7.3.1 Key issuance and storage

- On tenant creation, issue a default JWT API key with no `exp`
- JWT claims must include at least: `tenant_id`, `token_version`
- Return plaintext token once; never persist plaintext
- Persist only:
  - `api_key_id` (`tenant_api_keys.id`)
  - `jwt_hash` (`sha256(raw_jwt)`)
  - `jwt_ciphertext` (KMS ciphertext)

#### 7.3.2 Header contract

- Standard: `Authorization: Bearer <jwt_api_key>`
- Optional compatibility: `X-API-Key: <jwt_api_key>`

#### 7.3.3 Middleware flow

1. Extract token from headers
2. Compute `sha256(raw_jwt)` and find row in `tenant_api_keys`
3. Validate key status (`active`)
4. Decrypt `jwt_ciphertext` and constant-time compare with incoming token
5. Verify JWT signature and parse claims (`tenant_id`, `token_version`)
6. Load tenant by `tenant_id` and validate tenant status (`active` only)
7. Decrypt tenant DB password and get/create tenant backend from pool
8. Inject `TenantScope` into context
9. Downstream handlers/services/repos read tenant context only from scope

Suggested scope API:

```go
type TenantScope struct {
    TenantID     string
    APIKeyID     string
    TokenVersion int
    Backend      *backend.Dat9Backend
}

func ScopeFromContext(ctx context.Context) *TenantScope
```

#### 7.3.4 Error semantics

- Missing or malformed key: `401`
- Unknown key: `401`
- Tenant not active (`suspended/deleted`): `403`
- Tenant still provisioning: `503`
- Meta DB / decrypt / pool internal errors: `5xx`

Notes:
- Use generic external error text; do not leak tenant existence details
- Never log plaintext token; internal troubleshooting uses only `api_key_id` / `jwt_hash`

#### 7.3.5 Revocation and immediate effect

- Revoke by setting `tenant_api_keys.status='revoked'` (or bump `token_version`)
- Tenant `suspended/deleted` rejects all keys automatically
- On non-active tenant detection, call `pool.Invalidate(tenant_id)`
- Effect must be immediate without process restart

#### 7.3.6 Tenant scope constraints (avoid cross-layer drift)

- Handler layer must not re-parse tenant id from headers/query/path
- Service/Repo layer must not accept ad-hoc tenant id string parameters
- DB/storage access must always be tenant-scoped (scope-bound backend or explicit tenant filter)
- Logs/audit must always include `tenant_id` and `api_key_id`

---

## 8. KMS Bootstrap (AWS CLI)

```bash
# 1) Create KMS key
aws kms create-key \
  --description "dat9 tenant db password encryption" \
  --tags TagKey=project,TagValue=dat9 TagKey=env,TagValue=shared

# 2) Create alias
aws kms create-alias \
  --alias-name alias/dat9-<env>-db-password \
  --target-key-id <KeyId>

# 3) Verify
aws kms describe-key --key-id alias/dat9-<env>-db-password
```

Server config stores only alias, not plaintext key material.

---

## 9. Failure and Rollback Strategy

- Provision succeeds but schema init fails:
  - mark tenant as `provisioned_not_initialized`
  - keep `cluster_id`
  - block write traffic
  - retry in background
- KMS encryption fails:
  - fail request, do not persist tenant
- Meta DB write fails:
  - fail request, do not expose tenant as usable

---

## 10. Milestones

### Phase 1 (now)

- Provisioner abstraction
- Zero + Starter implementation
- KMS Encryptor implementation
- dat9 schema init (FTS + Vector)
- JWT API key table + tenant scope middleware

### Phase 2 (as soon as DB9 API is ready)

- Implement `DB9Provisioner` and make it the default path
- Keep zero as a development option

### Phase 3

- Audit logs, compensating actions, quota and cost controls

---

## 11. Acceptance Criteria

- New tenant provisioning enables file metadata read/write
- `files` supports both FTS and Vector queries
- Meta DB has no plaintext DB password (only encrypted `db_password`)
- Meta DB has no plaintext API key (only `tenant_api_keys.jwt_ciphertext/jwt_hash`)
- KMS decrypt restores DB connectivity and health checks pass
- Provider selection follows config strictly (no automatic fallback)
