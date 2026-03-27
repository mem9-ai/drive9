# Dat9 租户数据库自动创建设计（DB9 / TiDB Zero / TiDB Starter）

## 1. 目标与范围

本设计用于 dat9 在「新租户创建」时自动准备数据库与基础表结构，并安全保存连接信息。

- 支持三类后端：`db9`、`tidb_zero`、`tidb_cloud_starter`
- `db9` 与 `zero/starter` 是同级方案
- provider 由配置显式指定：`db9` / `tidb_zero` / `tidb_cloud_starter`
- 租户数据库密码必须走 KMS 加密后再持久化

不在本文范围：应用部署、EKS 网络、o11y agent 安装。

---

## 2. 关键结论

- dat9 的核心 schema 只有四张表：`file_nodes`、`files`、`file_tags`、`uploads`
- FTS 和 Vector 是 dat9 的必需能力（在 `files` 上）
- 加密采用统一接口：`Encryptor`（`Encrypt/Decrypt`）+ `plain/md5/kms` 工厂

---

## 3. 总体架构

### 3.1 创建流程

1. API 接收 `CreateTenant` 请求
2. `TenantService` 选择 Provisioner（db9/zero/starter）
3. Provisioner 获取实例连接信息（host/port/user/password/dbname/cluster_id）
4. `SchemaInitializer` 初始化 dat9 schema（含 FTS + Vector）
5. `Encryptor(kms)` 加密密码
6. 写入租户元数据（meta db，含 KMS 密文）
7. 返回租户创建结果

### 3.2 Provisioner 选择策略（按配置）

系统不做自动降级；严格按配置选择 provider：

```text
DAT9_TENANT_PROVIDER=db9 | tidb_zero | tidb_cloud_starter
```

行为约定：
- 配置为 `db9`：调用 DB9Provisioner（若 DB9 create API 未开放，则直接返回错误）
- 配置为 `tidb_zero`：调用 TiDBZeroProvisioner
- 配置为 `tidb_cloud_starter`：调用 TiDBStarterProvisioner

---

## 4. 加密抽象

### 4.1 接口定义

```go
type Encryptor interface {
    Encrypt(ctx context.Context, plaintext string) (string, error)
    Decrypt(ctx context.Context, ciphertext string) (string, error)
}
```

### 4.2 工厂类型

- `plain`: 明文透传（仅本地开发）
- `md5`: 对称加密（兼容模式）
- `kms`: AWS KMS（生产默认）

建议默认：

```text
DAT9_ENCRYPT_TYPE=kms
DAT9_ENCRYPT_KEY=alias/dat9-<env>-db-password
```

### 4.3 KMS 存储约定

- 存储的是 `base64(ciphertextBlob)`
- 解密时不需要 key id（KMS ciphertext 内含 key metadata）

---

## 5. Provisioner 设计

### 5.1 统一接口

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
    Password   string // 明文仅在内存短暂存在
    DBName     string
    Provider   string
    ClaimURL   string     // zero 可选
    ClaimUntil *time.Time // zero 可选
}
```

### 5.2 DB9Provisioner

- 当前状态：create API 尚未开放
- 接口预留：`CreateInstance(tag, root_password, ... )`
- 一旦 API 可用，直接切主路径

### 5.3 TiDBZeroProvisioner（当前可用）

- 用于 dev 或 DB9 不可用时
- 调 Zero API 创建临时实例
- 返回 `claim_url/claim_expires_at`

### 5.4 TiDBStarterProvisioner（当前可用）

- 用于 prod 或 zero 不适用场景
- 通过 TiDB Cloud Pool takeover API 获取实例
- 使用 digest auth

---

## 6. Dat9 Schema

## 6.1 通用逻辑

每个租户库必须初始化：

1. `file_nodes`
2. `files`
3. `file_tags`
4. `uploads`

并在 `files` 上启用：

- FTS（全文检索，字段 `content_text`）
- Vector（相似度检索，字段 `embedding`）

## 6.2 TiDB 版本（zero/starter）

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

## 6.3 DB9/PostgreSQL 版本（首选目标）

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

## 7. Meta DB 持久化与配置

## 7.1 Meta DB 持久化模型

租户控制面与连接信息统一存储在 `tenants` 表。

建议表结构（示例）：

```sql
CREATE TABLE IF NOT EXISTS tenants (
  id               VARCHAR(36)   PRIMARY KEY,

  -- DB connection (password 保存密文)
  db_host          VARCHAR(255)  NOT NULL,
  db_port          INT           NOT NULL,
  db_user          VARCHAR(255)  NOT NULL,
  db_password      TEXT          NOT NULL,  -- KMS Encrypt 后的 base64 密文
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
```

字段约定：
- `db_password`：仅存 KMS 密文，不存明文
- `id`：租户唯一标识（不再维护独立 `name` 字段）
- `status`：`provisioning|active|suspended|deleted`
- `claim_url/claim_expires_at`：仅 zero provider 使用
- `schema_version`：用于租户库 schema 演进控制

读写规则：
- 写入：`db_password` 写入 KMS 密文
- 读取：运行时读取密文并调用 KMS Decrypt 后构建 DSN
- 日志：禁止输出 `db_password` 密文和解密后的明文

## 7.2 环境变量

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

---

## 8. KMS 初始准备（AWS CLI）

```bash
# 1) 创建 KMS key
aws kms create-key \
  --description "dat9 tenant db password encryption" \
  --tags TagKey=project,TagValue=dat9 TagKey=env,TagValue=shared

# 2) 创建 alias（推荐）
aws kms create-alias \
  --alias-name alias/dat9-<env>-db-password \
  --target-key-id <KeyId>

# 3) 验证
aws kms describe-key --key-id alias/dat9-<env>-db-password
```

服务端只持有 alias，不持有明文密钥。

---

## 9. 错误与回滚策略

- Provision 成功但 schema 失败：
  - 标记 tenant 状态 `provisioned_not_initialized`
  - 记录 cluster_id，并阻断后续写入请求
  - 后台重试初始化
- KMS 加密失败：
  - 直接失败，不落库
- Meta DB 写入失败：
  - 直接失败，不返回 tenant 可用

---

## 10. 里程碑

## Phase 1（现在）
- Provisioner 抽象
- Zero + Starter 落地
- KMS Encryptor（统一加密接口实现）
- dat9 schema 初始化（含 FTS + Vector）

## Phase 2（DB9 API 可用后立即做）
- DB9Provisioner 实现并切为默认路径
- 逐步下线 zero 在非开发环境的路径

## Phase 3
- 审计日志、失败补偿、配额与成本控制

---

## 11. 验收标准

- 新租户创建后可完成文件元数据写入与读取
- `files` 同时具备 FTS 与 Vector 查询能力
- Meta DB 中无明文密码（仅存 `db_password` KMS 密文）
- KMS decrypt 可恢复连接并通过健康检查
- provider 选择严格遵循配置值（无自动降级）
