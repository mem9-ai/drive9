// Package meta provides control-plane metadata storage for multi-tenant auth.
package meta

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/internal/schemaspec"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/mysqlutil"
	"github.com/mem9-ai/drive9/pkg/pathutil"
	"go.uber.org/zap"
)

var (
	ErrNotFound                 = errors.New("not found")
	ErrDuplicate                = errors.New("duplicate entry")
	ErrStorageQuotaExceeded     = errors.New("tenant storage quota exceeded")
	ErrFileCountQuotaExceeded   = errors.New("tenant file count quota exceeded")
	ErrReservationAlreadyExists = errors.New("upload reservation already exists")
	ErrQuotaReservationBusy     = errors.New("quota reservation busy")
)

type TenantStatus string

const (
	TenantPending      TenantStatus = "pending"
	TenantProvisioning TenantStatus = "provisioning"
	TenantActive       TenantStatus = "active"
	TenantFailed       TenantStatus = "failed"
	TenantSuspended    TenantStatus = "suspended"
	TenantDeleting     TenantStatus = "deleting"
	TenantDeleted      TenantStatus = "deleted"
)

var allTenantStatuses = []TenantStatus{
	TenantPending,
	TenantProvisioning,
	TenantActive,
	TenantFailed,
	TenantSuspended,
	TenantDeleting,
	TenantDeleted,
}

const tidbCloudNativeProvider = "tidb_cloud_native"
const maxTiDBCloudOrgBindingDuplicateTuples = 20

type TenantKind string

const (
	TenantKindLive TenantKind = "live"
	TenantKindFork TenantKind = "fork"
)

type APIKeyStatus string

const (
	APIKeyActive  APIKeyStatus = "active"
	APIKeyRevoked APIKeyStatus = "revoked"
)

type APIKeyScopeKind string

const (
	APIKeyScopeKindOwner APIKeyScopeKind = "owner"
	APIKeyScopeKindFS    APIKeyScopeKind = "fs_scoped"
)

type Tenant struct {
	ID                 string
	Status             TenantStatus
	Kind               TenantKind
	ParentTenantID     string
	StorageNamespaceID string
	DBHost             string
	DBPort             int
	DBUser             string
	DBPasswordCipher   []byte
	DBName             string
	DBTLS              bool
	Provider           string
	ClusterID          string
	BranchID           string
	ClaimURL           string
	ClaimExpiresAt     *time.Time
	SchemaVersion      int
	S3EncryptionMode   S3EncryptionMode
	S3KMSKeyID         string
	S3BucketKeyEnabled *bool
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type TenantCounts struct {
	Statuses []TenantStatusCount
}

type TenantStatusCount struct {
	Status TenantStatus
	Count  int64
}

func TenantStatuses() []TenantStatus {
	return append([]TenantStatus(nil), allTenantStatuses...)
}

func (c TenantCounts) Count(status TenantStatus) int64 {
	for _, rec := range c.Statuses {
		if rec.Status == status {
			return rec.Count
		}
	}
	return 0
}

type TenantAutoEmbeddingProfile struct {
	TenantID      string
	EmbeddingMode string
	Model         string
	Dimensions    int
	OptionsJSON   string
	APIBase       string
	APIKeyCipher  []byte
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

const (
	TenantEmbeddingModeAuto    = "auto"
	TenantEmbeddingModeFTSOnly = "fts_only"
)

func ResolveTenantEmbeddingMode(persisted string, defaultFTSOnly bool) (mode string, wasNull bool, err error) {
	switch persisted {
	case "":
		if defaultFTSOnly {
			return TenantEmbeddingModeFTSOnly, true, nil
		}
		return TenantEmbeddingModeAuto, true, nil
	case TenantEmbeddingModeAuto, TenantEmbeddingModeFTSOnly:
		return persisted, false, nil
	default:
		return "", false, fmt.Errorf("unsupported tenant embedding mode %q", persisted)
	}
}

type StorageNamespaceState string

const (
	StorageNamespaceActive   StorageNamespaceState = "active"
	StorageNamespaceDeleting StorageNamespaceState = "deleting"
	StorageNamespaceDeleted  StorageNamespaceState = "deleted"
)

type StorageNamespace struct {
	ID            string
	OwnerTenantID string
	Backend       string
	Bucket        string
	Prefix        string
	State         StorageNamespaceState
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type ObjectGCCandidateState string

const (
	ObjectGCCandidatePending  ObjectGCCandidateState = "pending"
	ObjectGCCandidateDeleting ObjectGCCandidateState = "deleting"
	ObjectGCCandidateDeleted  ObjectGCCandidateState = "deleted"
	ObjectGCCandidateFailed   ObjectGCCandidateState = "failed"
)

type ObjectGCCandidateReason string

const (
	ObjectGCReasonOverwrite   ObjectGCCandidateReason = "overwrite"
	ObjectGCReasonFileDelete  ObjectGCCandidateReason = "file_delete"
	ObjectGCReasonFailedWrite ObjectGCCandidateReason = "failed_write"
	ObjectGCReasonForkDelete  ObjectGCCandidateReason = "fork_delete"
)

type TenantDeleteJobState string

const (
	TenantDeleteJobPending TenantDeleteJobState = "pending"
	TenantDeleteJobRunning TenantDeleteJobState = "running"
	TenantDeleteJobDeleted TenantDeleteJobState = "deleted"
)

type TenantDeleteJob struct {
	TenantID                string
	NamespaceID             string
	Backend                 string
	Bucket                  string
	Prefix                  string
	State                   TenantDeleteJobState
	Attempts                int
	LastError               string
	NotBefore               time.Time
	DeletedObjects          int64
	AbortedMultipartUploads int64
	CreatedAt               time.Time
	UpdatedAt               time.Time
	CompletedAt             *time.Time
}

func (t Tenant) S3EncryptionPolicy() S3EncryptionPolicy {
	return S3EncryptionPolicy{
		Mode:             t.S3EncryptionMode,
		KMSKeyID:         t.S3KMSKeyID,
		BucketKeyEnabled: t.S3BucketKeyEnabledValue(),
	}
}

func (t Tenant) S3BucketKeyEnabledValue() bool {
	if t.S3BucketKeyEnabled == nil {
		return true
	}
	return *t.S3BucketKeyEnabled
}

type APIKey struct {
	ID                   string
	TenantID             string
	KeyName              string
	JWTCiphertext        []byte
	JWTHash              string
	TokenVersion         int
	Status               APIKeyStatus
	ScopeKind            APIKeyScopeKind
	IssuedByProvider     string
	IssuedBySubjectKey   string
	IssuedByMetadataJSON []byte
	IssuedAt             time.Time
	RevokedAt            *time.Time
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type APIKeyFSScope struct {
	TenantID   string
	APIKeyID   string
	Prefix     string
	PrefixHash string
	Ops        string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type TenantWithAPIKey struct {
	Tenant         Tenant
	APIKey         APIKey
	TiDBCloudOrgID string
}

type ExternalBinding struct {
	Provider     string
	SubjectKey   string
	TenantID     string
	MetadataJSON []byte
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type TenantTiDBCloudOrgBinding struct {
	TenantID       string
	OrganizationID string
	ClusterID      string
	BranchID       string
	PoolID         string
	PoolStatus     TenantPoolBindingStatus
	UsedAt         *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type TenantWithTiDBCloudOrgBinding struct {
	Tenant  Tenant
	Binding TenantTiDBCloudOrgBinding
}

type metaQueryExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type TenantPoolBindingStatus string

const (
	TenantPoolBindingUsed TenantPoolBindingStatus = "used"
	TenantPoolBindingFree TenantPoolBindingStatus = "free"
)

type TenantPoolStatus string

const (
	TenantPoolActive   TenantPoolStatus = "active"
	TenantPoolDeleting TenantPoolStatus = "deleting"
)

type TenantPool struct {
	PoolID         string
	OrganizationID string
	Size           int
	Status         TenantPoolStatus
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type TenantPoolBindingStatusCount struct {
	PoolID         string
	OrganizationID string
	Status         TenantPoolBindingStatus
	Count          int64
}

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	return OpenContext(context.Background(), dsn)
}

func OpenContext(ctx context.Context, dsn string) (*Store, error) {
	if strings.Contains(dsn, "multiStatements=true") {
		return nil, fmt.Errorf("multiStatements=true is not allowed in production DSN")
	}
	db, err := mysqlutil.OpenInstrumented(ctx, dsn, mysqlutil.RoleMeta)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = mysqlutil.CloseInstrumented(db)
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error { return mysqlutil.CloseInstrumented(s.db) }
func (s *Store) DB() *sql.DB  { return s.db }

const metaSchemaMigrateLockNamePrefix = "dat9_meta_schema_migrate:"
const metaSchemaMigrateLockTimeoutSeconds = 30
const externalBindingLockTimeoutSeconds = 90
const externalBindingReleaseLockTimeout = 5 * time.Second
const tidbCloudOrgBindingLockTimeoutSeconds = 90
const tidbCloudOrgBindingReleaseLockTimeout = 5 * time.Second
const tenantPoolLockTimeoutSeconds = 300
const tenantPoolReleaseLockTimeout = 5 * time.Second

func (s *Store) migrate() (err error) {
	ctx := context.Background()
	releaseLock, err := acquireMetaSchemaMigrationLock(ctx, s.db)
	if err != nil {
		return fmt.Errorf("acquire meta schema migration lock: %w", err)
	}
	defer func() {
		if releaseErr := releaseLock(); releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("release meta schema migration lock: %w", releaseErr))
		}
	}()

	stmts := metaInitSchemaStatements()
	spec, err := metaSchemaSpecFromStatements(stmts)
	if err != nil {
		return fmt.Errorf("parse meta schema statements: %w", err)
	}
	if err := dropObsoleteMetaIndexes(ctx, s.db); err != nil {
		return err
	}
	diffs, err := diffMetaSchema(ctx, s.db, spec)
	if err != nil {
		return fmt.Errorf("diff meta schema: %w", err)
	}
	if err := applyMetaSchemaRepairs(ctx, s.db, plannedMetaSchemaRepairs(diffs)); err != nil {
		return err
	}
	if err := backfillTiDBCloudOrgBindingBranchIDs(ctx, s.db); err != nil {
		return err
	}
	if err := deleteStaleTiDBCloudOrgBindings(ctx, s.db); err != nil {
		return err
	}
	if err := ensureTiDBCloudOrgBindingUniqueIndex(ctx, s.db); err != nil {
		return err
	}
	diffs, err = diffMetaSchema(ctx, s.db, spec)
	if err != nil {
		return fmt.Errorf("re-diff meta schema: %w", err)
	}
	if len(diffs) > 0 {
		return &metaSchemaDiffError{diffs: diffs}
	}
	return nil
}

func acquireMetaSchemaMigrationLock(ctx context.Context, db *sql.DB) (func() error, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx,
		"SELECT GET_LOCK(CONCAT(?, DATABASE()), ?)",
		metaSchemaMigrateLockNamePrefix,
		metaSchemaMigrateLockTimeoutSeconds,
	).Scan(&got); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if !got.Valid {
		_ = conn.Close()
		return nil, fmt.Errorf("named lock returned NULL")
	}
	if got.Int64 != 1 {
		_ = conn.Close()
		return nil, fmt.Errorf("timed out waiting for named lock")
	}

	return func() error {
		defer func() { _ = conn.Close() }()

		var released sql.NullInt64
		if err := conn.QueryRowContext(ctx,
			"SELECT RELEASE_LOCK(CONCAT(?, DATABASE()))",
			metaSchemaMigrateLockNamePrefix,
		).Scan(&released); err != nil {
			return err
		}
		if !released.Valid {
			return fmt.Errorf("named lock release returned NULL")
		}
		if released.Int64 != 1 {
			return fmt.Errorf("named lock was not held by current connection")
		}
		return nil
	}, nil
}

type metaColumnMeta struct {
	columnType string
}

type metaTableMeta struct {
	tableName string
	columns   map[string]metaColumnMeta
}

type metaSchemaDiffKind string

const (
	metaSchemaDiffMissingTable  metaSchemaDiffKind = "missing_table"
	metaSchemaDiffMissingColumn metaSchemaDiffKind = "missing_column"
	metaSchemaDiffMissingIndex  metaSchemaDiffKind = "missing_index"
	metaSchemaDiffColumnType    metaSchemaDiffKind = "column_type_mismatch"
)

type metaSchemaDiff struct {
	kind       metaSchemaDiffKind
	tableName  string
	columnName string
	indexName  string
	detail     string
	repairSQL  string
}

type metaSchemaDiffError struct {
	diffs []metaSchemaDiff
}

func (e *metaSchemaDiffError) Error() string {
	if e == nil || len(e.diffs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(e.diffs))
	for _, d := range e.diffs {
		parts = append(parts, d.detail)
	}
	return "meta schema contract mismatch: " + strings.Join(parts, "; ")
}

type metaSchemaSpec struct {
	tables []metaTableSpec
}

type metaTableSpec struct {
	name            string
	createStatement string
	columns         map[string]metaColumnSpec
	indexes         map[string]metaIndexSpec
}

type metaColumnSpec struct {
	columnType string
	addSQL     string
	modifySQL  string
}

type metaIndexSpec struct {
	createSQL string
	isPrimary bool
}

func metaInitSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS tenants (
			id               VARCHAR(64) PRIMARY KEY,
			status           VARCHAR(20) NOT NULL DEFAULT 'provisioning',
			kind             VARCHAR(16) NOT NULL DEFAULT 'live',
			parent_tenant_id VARCHAR(64) NOT NULL DEFAULT '',
			storage_namespace_id VARCHAR(64) NOT NULL DEFAULT '',
			db_host          VARCHAR(255) NOT NULL,
			db_port          INT NOT NULL,
			db_user          VARCHAR(255) NOT NULL,
			db_password      VARBINARY(2048) NOT NULL,
			db_name          VARCHAR(255) NOT NULL,
			db_tls           TINYINT(1) NOT NULL DEFAULT 1,
			provider         VARCHAR(50) NOT NULL,
			cluster_id       VARCHAR(255) NULL,
			branch_id        VARCHAR(255) NOT NULL DEFAULT '',
			claim_url        TEXT NULL,
			claim_expires_at DATETIME(3) NULL,
			schema_version   INT UNSIGNED NOT NULL DEFAULT 1,
			s3_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'inherit',
			s3_kms_key_id VARCHAR(256) NOT NULL DEFAULT '',
			s3_bucket_key_enabled TINYINT(1) NOT NULL DEFAULT 1,
			created_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			deleted_at       DATETIME(3) NULL,
			INDEX idx_tenant_status (status),
			INDEX idx_tenant_status_created_id (status, created_at, id),
			INDEX idx_tenant_provider (provider),
			INDEX idx_tenant_namespace (storage_namespace_id, kind, status),
			INDEX idx_tenant_parent (parent_tenant_id)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_auto_embedding_profiles (
			tenant_id      VARCHAR(64) PRIMARY KEY,
			embedding_mode VARCHAR(32) NULL,
			model          VARCHAR(255) NOT NULL DEFAULT 'tidbcloud_free/amazon/titan-embed-text-v2',
			dimensions     INT UNSIGNED NOT NULL DEFAULT 1024,
			options_json   VARCHAR(2048) NOT NULL DEFAULT '{"dimensions":1024}',
			api_base       TEXT NULL,
			api_key_cipher VARBINARY(2048) NULL,
			created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS storage_namespaces (
			namespace_id    VARCHAR(64) PRIMARY KEY,
			owner_tenant_id VARCHAR(64) NOT NULL,
			backend         VARCHAR(16) NOT NULL,
			bucket          VARCHAR(255) NOT NULL DEFAULT '',
			prefix          VARCHAR(2048) NOT NULL,
			state           VARCHAR(20) NOT NULL DEFAULT 'active',
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			INDEX idx_storage_namespace_owner (owner_tenant_id),
			INDEX idx_storage_namespace_state (state)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_delete_jobs (
			tenant_id                  VARCHAR(64) PRIMARY KEY,
			namespace_id               VARCHAR(64) NOT NULL,
			backend                    VARCHAR(16) NOT NULL,
			bucket                     VARCHAR(255) NOT NULL DEFAULT '',
			prefix                     VARCHAR(2048) NOT NULL,
			state                      VARCHAR(20) NOT NULL DEFAULT 'pending',
			attempts                   INT NOT NULL DEFAULT 0,
			last_error                 TEXT NULL,
			not_before                 DATETIME(3) NOT NULL,
			deleted_objects            BIGINT NOT NULL DEFAULT 0,
			aborted_multipart_uploads BIGINT NOT NULL DEFAULT 0,
			created_at                 DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at                 DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			completed_at               DATETIME(3) NULL,
			INDEX idx_tenant_delete_jobs_due (state, not_before),
			INDEX idx_tenant_delete_jobs_namespace (namespace_id, state)
		)`,
		`CREATE TABLE IF NOT EXISTS object_gc_candidates (
			namespace_id     VARCHAR(64) NOT NULL,
			storage_ref      TEXT NOT NULL,
			storage_ref_hash VARCHAR(64) NOT NULL,
			reason           VARCHAR(32) NOT NULL,
			source_tenant_id VARCHAR(64) NOT NULL DEFAULT '',
			source_file_id   VARCHAR(64) NOT NULL DEFAULT '',
			not_before       DATETIME(3) NOT NULL,
			state            VARCHAR(20) NOT NULL DEFAULT 'pending',
			attempts         INT NOT NULL DEFAULT 0,
			last_error       TEXT NULL,
			created_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			deleted_at       DATETIME(3) NULL,
			PRIMARY KEY (namespace_id, storage_ref_hash),
			INDEX idx_object_gc_due (state, not_before),
			INDEX idx_object_gc_namespace_due (namespace_id, state, not_before)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_api_keys (
			id             VARCHAR(64) PRIMARY KEY,
			tenant_id      VARCHAR(64) NOT NULL,
			key_name       VARCHAR(64) NOT NULL DEFAULT 'default',
			jwt_ciphertext VARBINARY(4096) NOT NULL,
			jwt_hash       VARCHAR(128) NOT NULL,
			token_version  INT NOT NULL DEFAULT 1,
			status         VARCHAR(20) NOT NULL DEFAULT 'active',
			scope_kind     VARCHAR(32) NOT NULL DEFAULT 'owner',
			issued_by_provider VARCHAR(64) NOT NULL DEFAULT '',
			issued_by_subject_key VARCHAR(512) NOT NULL DEFAULT '',
			issued_by_metadata_json JSON NULL,
			issued_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			revoked_at     DATETIME(3) NULL,
			created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			UNIQUE INDEX idx_api_keys_hash (jwt_hash),
			INDEX idx_api_keys_tenant (tenant_id, status),
			INDEX idx_api_keys_issuer (tenant_id, issued_by_provider, issued_by_subject_key, status, created_at)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_external_bindings (
			provider      VARCHAR(64) NOT NULL,
			subject_key   VARCHAR(512) NOT NULL,
			tenant_id     VARCHAR(64) NOT NULL,
			metadata_json JSON NULL,
			created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			UNIQUE INDEX uk_external_binding_subject (provider, subject_key),
			INDEX idx_external_binding_tenant (tenant_id)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_tidbcloud_org_bindings (
				tenant_id       VARCHAR(64) PRIMARY KEY,
				organization_id VARCHAR(64) NOT NULL,
				cluster_id      VARCHAR(255) NOT NULL,
				branch_id       VARCHAR(255) NOT NULL DEFAULT '',
				pool_id         VARCHAR(64) NOT NULL DEFAULT '',
				pool_status     VARCHAR(20) NOT NULL DEFAULT 'used',
				used_at         DATETIME(3) NULL,
				created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
				updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
				UNIQUE INDEX uk_tidbcloud_org_cluster_branch (organization_id, cluster_id, branch_id),
				INDEX idx_tidbcloud_org_cluster (organization_id, cluster_id, branch_id, created_at, tenant_id),
				INDEX idx_tidbcloud_org_created (organization_id, created_at, tenant_id),
				INDEX idx_tidbcloud_pool_free (organization_id, pool_status, created_at, tenant_id)
			)`,
		`CREATE TABLE IF NOT EXISTS tenant_tidbcloud_pools (
			pool_id         VARCHAR(64) PRIMARY KEY,
			organization_id VARCHAR(64) NULL,
			size            INT NOT NULL,
			status          VARCHAR(20) NOT NULL DEFAULT 'active',
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			UNIQUE INDEX uk_tidbcloud_pool_org (organization_id),
			INDEX idx_tidbcloud_pool_status (status, created_at)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_api_key_fs_scopes (
			tenant_id   VARCHAR(64) NOT NULL,
			api_key_id  VARCHAR(64) NOT NULL,
			prefix      TEXT NOT NULL,
			prefix_hash VARCHAR(64) NOT NULL,
			ops         VARCHAR(255) NOT NULL,
			created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, api_key_id, prefix_hash),
			INDEX idx_fs_scopes_api_key (api_key_id),
			INDEX idx_fs_scopes_tenant_key (tenant_id, api_key_id)
		)`,
		`CREATE TABLE IF NOT EXISTS llm_usage (
			id              BIGINT AUTO_INCREMENT PRIMARY KEY,
			tenant_id       VARCHAR(64) NOT NULL DEFAULT '',
			task_type       VARCHAR(64) NOT NULL,
			task_id         VARCHAR(255) NOT NULL,
			cost_millicents BIGINT NOT NULL,
			raw_units       BIGINT NOT NULL DEFAULT 0,
			raw_unit_type   VARCHAR(32) NOT NULL DEFAULT '',
			created_at      DATETIME(3) NOT NULL,
			INDEX idx_llm_usage_tenant_created (tenant_id, created_at),
			INDEX idx_llm_usage_created (created_at)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_quota_config (
			tenant_id             VARCHAR(64) PRIMARY KEY,
			max_storage_bytes     BIGINT NOT NULL DEFAULT 53687091200,
			max_file_size_bytes   BIGINT NOT NULL DEFAULT 0,
			max_file_count        BIGINT NOT NULL DEFAULT 0,
			max_media_llm_files   BIGINT NOT NULL DEFAULT 500,
			max_monthly_cost_mc   BIGINT NOT NULL DEFAULT 0,
			quota_limits_overridden TINYINT(1) NOT NULL DEFAULT 1,
			tidbcloud_spending_limit BIGINT NULL,
			tidbcloud_spending_limit_checked_at DATETIME(3) NULL,
			created_at            DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at            DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_quota_usage (
			tenant_id          VARCHAR(64) PRIMARY KEY,
			storage_bytes      BIGINT NOT NULL DEFAULT 0,
			reserved_bytes     BIGINT NOT NULL DEFAULT 0,
			file_count         BIGINT NOT NULL DEFAULT 0,
			media_file_count   BIGINT NOT NULL DEFAULT 0,
			updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_file_meta (
			tenant_id    VARCHAR(64) NOT NULL,
			file_id      VARCHAR(64) NOT NULL,
			size_bytes   BIGINT NOT NULL DEFAULT 0,
			is_media     TINYINT(1) NOT NULL DEFAULT 0,
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, file_id)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_upload_reservations (
			tenant_id      VARCHAR(64) NOT NULL,
			upload_id      VARCHAR(64) NOT NULL,
			reserved_bytes BIGINT NOT NULL,
			file_count_delta BIGINT NOT NULL DEFAULT 0,
			target_path    VARCHAR(4096) NOT NULL,
			status         VARCHAR(20) NOT NULL DEFAULT 'active',
			expires_at     DATETIME(3) NOT NULL,
			created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (tenant_id, upload_id),
			INDEX idx_active_reservations (tenant_id, status, expires_at)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_llm_usage (
			id              BIGINT AUTO_INCREMENT PRIMARY KEY,
			tenant_id       VARCHAR(64) NOT NULL,
			task_type       VARCHAR(32) NOT NULL,
			task_id         VARCHAR(64) NOT NULL,
			cost_millicents BIGINT NOT NULL DEFAULT 0,
			raw_units       BIGINT NOT NULL DEFAULT 0,
			raw_unit_type   VARCHAR(16) NOT NULL,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			INDEX idx_tenant_month (tenant_id, created_at)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_monthly_llm_cost (
			tenant_id    VARCHAR(64) NOT NULL,
			month_start  DATE NOT NULL,
			total_mc     BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (tenant_id, month_start)
		)`,
		`CREATE TABLE IF NOT EXISTS quota_mutation_log (
			id             BIGINT AUTO_INCREMENT PRIMARY KEY,
			tenant_id      VARCHAR(64) NOT NULL,
			mutation_type  VARCHAR(32) NOT NULL,
			mutation_data  JSON NOT NULL,
			status         VARCHAR(20) NOT NULL DEFAULT 'pending',
			retry_count    INT NOT NULL DEFAULT 0,
			created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			applied_at     DATETIME(3) NULL,
			INDEX idx_pending (status, created_at),
			INDEX idx_pending_tenant_age (status, tenant_id, created_at),
			INDEX idx_tenant_order (tenant_id, id)
		)`,
		// sse_notify_outbox is the legacy central notification pointer table for
		// cross-pod SSE event fan-out. Deprecated: replaced by
		// tenant_notify_outbox (which carries the SSE work bit alongside
		// semantic/file_gc/quota). The table is retained in the schema for
		// migration safety (no DROP) but production code no longer writes to
		// it. To be dropped in a future PR.
		`CREATE TABLE IF NOT EXISTS sse_notify_outbox (
			id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			tenant_id  VARCHAR(64) NOT NULL,
			seq        BIGINT UNSIGNED NOT NULL,
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			INDEX idx_sse_notify_created (created_at)
		)`,
		// pod_registry tracks all drive9-server pods for cross-pod SSE push
		// notification routing. Each pod upserts its row on startup and heartbeats
		// every ~10s. The leader marks pods with stale heartbeats as inactive so
		// writers don't push to dead pods. This table lives in the central meta DB
		// (always provisioned).
		`CREATE TABLE IF NOT EXISTS pod_registry (
			pod_id         VARCHAR(64) PRIMARY KEY,
			addr           VARCHAR(255) NOT NULL,
			last_heartbeat DATETIME(3) NOT NULL,
			status         VARCHAR(20) NOT NULL DEFAULT 'active',
			created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			INDEX idx_pod_heartbeat (last_heartbeat)
		)`,
		// pod_subscriptions maps each pod to the tenant IDs for which it has active
		// SSE subscribers. Writers consult this (via a locally cached reverse index)
		// to push notifications only to pods that care about a given tenant, avoiding
		// wasteful fan-out to pods with no subscribers for that tenant. Each pod
		// periodically upserts its current subscriber set and prunes stale entries.
		`CREATE TABLE IF NOT EXISTS pod_subscriptions (
			pod_id     VARCHAR(64) NOT NULL,
			tenant_id VARCHAR(64) NOT NULL,
			updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (pod_id, tenant_id)
		)`,
		// tenant_notify_outbox is the unified outbox that replaces per-tenant TiDB
		// polling for SSE, semantic, file_gc, and quota work. Each write path inserts
		// a lightweight row (tenant_id + work_mask) into this always-provisioned meta
		// DB table. Every pod polls it at 200ms and dispatches by work_mask: SSE bits
		// wake the local bus (broadcast to all pods with subscribers); semantic/file_gc/
		// quota bits kick the unified worker only on the shard owner. This eliminates
		// all periodic per-tenant TiDB scans, enabling serverless scale-to-zero.
		`CREATE TABLE IF NOT EXISTS tenant_notify_outbox (
			id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			tenant_id   VARCHAR(64) NOT NULL,
			work_mask   TINYINT UNSIGNED NOT NULL DEFAULT 0,
			created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			INDEX idx_tenant_notify_created (created_at)
		)`,
		// tenant_outbox_cursor stores each pod's last-processed outbox id so a pod
		// can resume after restart without skipping work and without re-reading all
		// history. Each pod owns its own row (keyed by pod_id).
		`CREATE TABLE IF NOT EXISTS tenant_outbox_cursor (
			pod_id      VARCHAR(64) NOT NULL,
			last_id     BIGINT UNSIGNED NOT NULL DEFAULT 0,
			updated_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			PRIMARY KEY (pod_id)
		)`,
	}
}

func dropObsoleteMetaIndexes(ctx context.Context, db *sql.DB) error {
	if err := dropMetaIndexIfExists(ctx, db, "tenant_api_keys", "idx_api_keys_tenant_name"); err != nil {
		return fmt.Errorf("drop obsolete meta index idx_api_keys_tenant_name: %w", err)
	}
	if err := dropMetaIndexIfColumns(ctx, db, "tenant_tidbcloud_org_bindings", "idx_tidbcloud_org_cluster",
		[]string{"organization_id", "cluster_id", "created_at", "tenant_id"}); err != nil {
		return fmt.Errorf("drop obsolete meta index idx_tidbcloud_org_cluster: %w", err)
	}
	return nil
}

func dropMetaIndexIfExists(ctx context.Context, db *sql.DB, tableName, indexName string) error {
	if err := validateMetaIdentifier(tableName); err != nil {
		return fmt.Errorf("invalid table name %q: %w", tableName, err)
	}
	if err := validateMetaIdentifier(indexName); err != nil {
		return fmt.Errorf("invalid index name %q: %w", indexName, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
		tableName, indexName).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	_, err := db.ExecContext(ctx, fmt.Sprintf("DROP INDEX %s ON %s", indexName, tableName))
	return err
}

func dropMetaIndexIfColumns(ctx context.Context, db *sql.DB, tableName, indexName string, columns []string) error {
	if err := validateMetaIdentifier(tableName); err != nil {
		return fmt.Errorf("invalid table name %q: %w", tableName, err)
	}
	if err := validateMetaIdentifier(indexName); err != nil {
		return fmt.Errorf("invalid index name %q: %w", indexName, err)
	}
	got, err := loadMetaIndexColumns(ctx, db, tableName, indexName)
	if err != nil {
		return err
	}
	if !sameStringSlice(got, columns) {
		return nil
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP INDEX %s ON %s", indexName, tableName))
	return err
}

func validateMetaIdentifier(s string) error {
	if s == "" {
		return fmt.Errorf("identifier is empty")
	}
	for _, r := range s {
		if r == '_' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || ('0' <= r && r <= '9') {
			continue
		}
		return fmt.Errorf("identifier contains unsupported character %q", r)
	}
	return nil
}

func loadMetaIndexColumns(ctx context.Context, db *sql.DB, tableName, indexName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT column_name
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?
		ORDER BY seq_in_index`, tableName, indexName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		out = append(out, strings.ToLower(col))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func sameStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func metaSchemaSpecFromStatements(stmts []string) (metaSchemaSpec, error) {
	tables := make([]metaTableSpec, 0)
	for _, stmt := range stmts {
		table, ok, err := parseCreateMetaTableSpec(stmt)
		if err != nil {
			return metaSchemaSpec{}, err
		}
		if ok {
			tables = append(tables, table)
		}
	}
	return metaSchemaSpec{tables: tables}, nil
}

func parseCreateMetaTableSpec(stmt string) (metaTableSpec, bool, error) {
	tableName, defs, ok, err := parseMetaCreateTableStatement(stmt)
	if err != nil {
		return metaTableSpec{}, false, err
	}
	if !ok {
		return metaTableSpec{}, false, nil
	}
	columns := make(map[string]metaColumnSpec)
	indexes := make(map[string]metaIndexSpec)
	for _, def := range splitMetaTopLevelComma(defs) {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}
		if idxName, idxSpec, ok := parseMetaConstraintIndexDefinition(tableName, def); ok {
			indexes[idxName] = idxSpec
			continue
		}
		if idxName, idxSQL, ok := parseMetaInlineIndexDefinition(tableName, def); ok {
			indexes[idxName] = metaIndexSpec{createSQL: idxSQL}
			continue
		}
		if isMetaConstraintDefinition(def) {
			continue
		}
		colName, colSpec, ok := parseMetaColumnDefinition(tableName, def)
		if ok {
			columns[colName] = colSpec
			normalizedDef := normalizeMetaSQLFragment(def)
			if strings.Contains(normalizedDef, " primary key") {
				indexes["primary"] = metaIndexSpec{isPrimary: true}
			}
		}
	}
	return metaTableSpec{
		name:            tableName,
		createStatement: strings.TrimSpace(stmt),
		columns:         columns,
		indexes:         indexes,
	}, true, nil
}

func diffMetaSchema(ctx context.Context, db *sql.DB, spec metaSchemaSpec) ([]metaSchemaDiff, error) {
	var diffs []metaSchemaDiff
	for _, table := range spec.tables {
		tableDiffs, err := diffMetaTable(ctx, db, table)
		if err != nil {
			return nil, err
		}
		diffs = append(diffs, tableDiffs...)
	}
	return diffs, nil
}

func diffMetaTable(ctx context.Context, db *sql.DB, table metaTableSpec) ([]metaSchemaDiff, error) {
	meta, err := loadMetaTableMeta(ctx, db, table.name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []metaSchemaDiff{{
				kind:      metaSchemaDiffMissingTable,
				tableName: table.name,
				detail:    fmt.Sprintf("%s schema contract: missing table", table.name),
				repairSQL: table.createStatement,
			}}, nil
		}
		return nil, fmt.Errorf("load %s table metadata: %w", table.name, err)
	}
	indexNames, err := loadMetaIndexNames(ctx, db, table.name)
	if err != nil {
		return nil, fmt.Errorf("load %s index metadata: %w", table.name, err)
	}
	return diffMetaTableMetaWithObservedIndexes(table, meta, "", indexNames), nil
}

func diffMetaTableMeta(table metaTableSpec, meta metaTableMeta, createStmt string) []metaSchemaDiff {
	return diffMetaTableMetaWithObservedIndexes(table, meta, createStmt, parseObservedMetaIndexes(createStmt))
}

func diffMetaTableMetaWithObservedIndexes(table metaTableSpec, meta metaTableMeta, createStmt string, observedIndexes map[string]struct{}) []metaSchemaDiff {
	var diffs []metaSchemaDiff
	for _, name := range sortedMetaColumnNames(table.columns) {
		spec := table.columns[name]
		col, ok := meta.columns[name]
		if !ok {
			diffs = append(diffs, metaSchemaDiff{
				kind:       metaSchemaDiffMissingColumn,
				tableName:  table.name,
				columnName: name,
				detail:     fmt.Sprintf("%s schema contract: missing %s column", table.name, name),
				repairSQL:  spec.addSQL,
			})
			continue
		}
		observedType := normalizeMetaSQLFragment(col.columnType)
		desiredType := normalizeMetaSQLFragment(spec.columnType)
		if observedType != desiredType {
			// Only generate repair SQL for the one known-safe widening:
			// tenants.schema_version INT → INT UNSIGNED.
			var repairSQL string
			if table.name == "tenants" && name == "schema_version" &&
				observedType == "int" && desiredType == "int unsigned" {
				repairSQL = spec.modifySQL
			}
			diffs = append(diffs, metaSchemaDiff{
				kind:       metaSchemaDiffColumnType,
				tableName:  table.name,
				columnName: name,
				detail:     fmt.Sprintf("%s schema contract: %s column type = %q, want %s", table.name, name, col.columnType, spec.columnType),
				repairSQL:  repairSQL,
			})
		}
	}
	for _, name := range sortedMetaIndexNames(table.indexes) {
		spec := table.indexes[name]
		if !hasObservedMetaIndex(observedIndexes, name, spec) {
			detail := fmt.Sprintf("%s schema contract: missing %s index", table.name, name)
			if spec.isPrimary {
				detail = fmt.Sprintf("%s schema contract: missing primary key constraint", table.name)
			}
			diffs = append(diffs, metaSchemaDiff{
				kind:      metaSchemaDiffMissingIndex,
				tableName: table.name,
				indexName: name,
				detail:    detail,
				repairSQL: spec.createSQL,
			})
		}
	}
	return diffs
}

func hasObservedMetaIndex(observedIndexes map[string]struct{}, indexName string, spec metaIndexSpec) bool {
	if len(observedIndexes) == 0 {
		return false
	}
	name := strings.ToLower(indexName)
	if spec.isPrimary {
		name = "primary"
	}
	_, ok := observedIndexes[name]
	return ok
}

func parseObservedMetaIndexes(createStmt string) map[string]struct{} {
	_, defs, ok, err := parseMetaCreateTableStatement(createStmt)
	if err != nil || !ok {
		return nil
	}
	observed := make(map[string]struct{})
	for _, def := range splitMetaTopLevelComma(defs) {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}
		if idxName, _, ok := parseMetaConstraintIndexDefinition("", def); ok {
			observed[strings.ToLower(idxName)] = struct{}{}
			continue
		}
		if idxName, _, ok := parseMetaInlineIndexDefinition("", def); ok {
			observed[strings.ToLower(idxName)] = struct{}{}
			continue
		}
		if strings.Contains(normalizeMetaSQLFragment(def), " primary key") {
			observed["primary"] = struct{}{}
		}
	}
	return observed
}

func plannedMetaSchemaRepairs(diffs []metaSchemaDiff) []string {
	tableMissing := make(map[string]bool)
	for _, diff := range diffs {
		if diff.kind == metaSchemaDiffMissingTable {
			tableMissing[diff.tableName] = true
		}
	}

	seen := make(map[string]struct{})
	plans := make([]string, 0, len(diffs))
	for _, diff := range diffs {
		if diff.repairSQL == "" {
			continue
		}
		if !isSafeMetaRepairDiff(diff, tableMissing) {
			continue
		}
		if _, ok := seen[diff.repairSQL]; ok {
			continue
		}
		seen[diff.repairSQL] = struct{}{}
		plans = append(plans, diff.repairSQL)
	}
	return plans
}

func applyMetaSchemaRepairs(ctx context.Context, db *sql.DB, stmts []string) error {
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			if isIgnorableMetaSchemaError(err) {
				continue
			}
			return fmt.Errorf("apply meta schema repair %q: %w", schemaSnippet(stmt), err)
		}
	}
	return nil
}

func backfillTiDBCloudOrgBindingBranchIDs(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `UPDATE tenant_tidbcloud_org_bindings b
		JOIN tenants t ON t.id = b.tenant_id
		SET b.branch_id = TRIM(COALESCE(t.branch_id, ''))
		WHERE b.branch_id <> TRIM(COALESCE(t.branch_id, ''))`)
	if err != nil {
		return fmt.Errorf("backfill tidbcloud org binding branch ids: %w", err)
	}
	return nil
}

func deleteStaleTiDBCloudOrgBindings(ctx context.Context, db metaQueryExecer) error {
	_, err := db.ExecContext(ctx, `DELETE b
		FROM tenant_tidbcloud_org_bindings b
		LEFT JOIN tenants t ON t.id = b.tenant_id
		WHERE t.id IS NULL OR t.status = ?`, TenantDeleted)
	if err != nil {
		return fmt.Errorf("delete stale tidbcloud org bindings: %w", err)
	}
	return nil
}

func ensureTiDBCloudOrgBindingUniqueIndex(ctx context.Context, db *sql.DB) error {
	exists, err := metaIndexExists(ctx, db, "tenant_tidbcloud_org_bindings", "uk_tidbcloud_org_cluster_branch")
	if err != nil {
		return fmt.Errorf("check tidbcloud org binding unique index: %w", err)
	}
	if exists {
		return nil
	}
	if err := ensureNoDuplicateTiDBCloudOrgBindingTuples(ctx, db); err != nil {
		return fmt.Errorf("preflight tidbcloud org binding unique index: %w", err)
	}
	_, err = db.ExecContext(ctx, `CREATE UNIQUE INDEX uk_tidbcloud_org_cluster_branch
		ON tenant_tidbcloud_org_bindings(organization_id, cluster_id, branch_id)`)
	if err != nil {
		if isIgnorableMetaSchemaError(err) {
			return nil
		}
		return fmt.Errorf("create tidbcloud org binding unique index: %w", err)
	}
	return nil
}

func metaIndexExists(ctx context.Context, db *sql.DB, tableName, indexName string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
		tableName, indexName).Scan(&count); err != nil {
		return false, fmt.Errorf("check meta index %s.%s exists: %w", tableName, indexName, err)
	}
	return count > 0, nil
}

func ensureNoDuplicateTiDBCloudOrgBindingTuples(ctx context.Context, db *sql.DB) error {
	var total int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM (
		SELECT organization_id, cluster_id, branch_id
		FROM tenant_tidbcloud_org_bindings
		GROUP BY organization_id, cluster_id, branch_id
		HAVING COUNT(*) > 1
	) duplicates`).Scan(&total); err != nil {
		return fmt.Errorf("count duplicate tidbcloud org binding tuples: %w", err)
	}
	if total == 0 {
		return nil
	}

	rows, err := db.QueryContext(ctx, `SELECT organization_id, cluster_id, branch_id, COUNT(*) AS n, GROUP_CONCAT(tenant_id ORDER BY tenant_id)
		FROM tenant_tidbcloud_org_bindings
		GROUP BY organization_id, cluster_id, branch_id
		HAVING COUNT(*) > 1
		ORDER BY n DESC, organization_id, cluster_id, branch_id
		LIMIT ?`, maxTiDBCloudOrgBindingDuplicateTuples)
	if err != nil {
		return fmt.Errorf("list duplicate tidbcloud org binding tuples: %w", err)
	}
	defer func() { _ = rows.Close() }()

	conflicts := make([]string, 0, min(total, maxTiDBCloudOrgBindingDuplicateTuples))
	for rows.Next() {
		var organizationID, clusterID, branchID, tenantIDs string
		var count int
		if err := rows.Scan(&organizationID, &clusterID, &branchID, &count, &tenantIDs); err != nil {
			return fmt.Errorf("scan duplicate tidbcloud org binding tuple: %w", err)
		}
		conflicts = append(conflicts, fmt.Sprintf("organization_id=%q cluster_id=%q branch_id=%q count=%d tenant_ids=%s", organizationID, clusterID, branchID, count, tenantIDs))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate duplicate tidbcloud org binding tuples: %w", err)
	}
	if total > len(conflicts) {
		conflicts = append(conflicts, fmt.Sprintf("... %d more duplicate tuple(s) omitted", total-len(conflicts)))
	}
	return fmt.Errorf("cannot create tidbcloud org binding unique index: found %d duplicate tuple(s): %s", total, strings.Join(conflicts, "; "))
}

func parseMetaCreateTableStatement(stmt string) (tableName string, definitions string, ok bool, err error) {
	return schemaspec.ParseCreateTableStatement(stmt)
}

func splitMetaTopLevelComma(definitions string) []string {
	return schemaspec.SplitTopLevelComma(definitions)
}

func parseMetaInlineIndexDefinition(tableName, def string) (indexName, createSQL string, ok bool) {
	n := normalizeMetaSQLFragment(def)
	if strings.HasPrefix(n, "unique index ") {
		name, cols := parseMetaIndexNameAndColumns(def, "UNIQUE INDEX")
		if name == "" || cols == "" {
			return "", "", false
		}
		return name, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s%s", name, tableName, cols), true
	}
	if strings.HasPrefix(n, "index ") {
		name, cols := parseMetaIndexNameAndColumns(def, "INDEX")
		if name == "" || cols == "" {
			return "", "", false
		}
		return name, fmt.Sprintf("CREATE INDEX %s ON %s%s", name, tableName, cols), true
	}
	return "", "", false
}

func parseMetaConstraintIndexDefinition(tableName, def string) (indexName string, spec metaIndexSpec, ok bool) {
	n := normalizeMetaSQLFragment(def)
	if strings.HasPrefix(n, "primary key") {
		return "primary", metaIndexSpec{isPrimary: true}, true
	}
	if strings.HasPrefix(n, "unique key ") {
		name, cols := parseMetaIndexNameAndColumns(def, "UNIQUE KEY")
		if name == "" || cols == "" {
			return "", metaIndexSpec{}, false
		}
		return name, metaIndexSpec{
			createSQL: fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s%s", name, tableName, cols),
		}, true
	}
	if strings.HasPrefix(n, "constraint ") && strings.Contains(n, " unique key ") {
		// CONSTRAINT name UNIQUE KEY idx_name (...) -> parse using UNIQUE KEY token.
		name, cols := parseMetaIndexNameAndColumns(def, "UNIQUE KEY")
		if name == "" || cols == "" {
			return "", metaIndexSpec{}, false
		}
		return name, metaIndexSpec{
			createSQL: fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s%s", name, tableName, cols),
		}, true
	}
	return "", metaIndexSpec{}, false
}

func parseMetaIndexNameAndColumns(def, prefix string) (indexName, columns string) {
	trimmed := strings.TrimSpace(def)
	upper := strings.ToUpper(trimmed)
	p := strings.Index(upper, prefix)
	if p < 0 {
		return "", ""
	}
	rest := strings.TrimSpace(trimmed[p+len(prefix):])
	name, remainder := splitMetaIdentifierAndRest(rest)
	if name == "" {
		return "", ""
	}
	open := strings.Index(remainder, "(")
	if open < 0 {
		return "", ""
	}
	return strings.ToLower(name), strings.TrimSpace(remainder[open:])
}

func parseMetaColumnDefinition(tableName, def string) (string, metaColumnSpec, bool) {
	name, rest := splitMetaIdentifierAndRest(def)
	if name == "" || rest == "" {
		return "", metaColumnSpec{}, false
	}
	colType := parseMetaColumnType(rest)
	if colType == "" {
		return "", metaColumnSpec{}, false
	}
	return strings.ToLower(name), metaColumnSpec{
		columnType: strings.ToLower(strings.TrimSpace(colType)),
		addSQL:     fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, name, strings.TrimSpace(rest)),
		modifySQL:  fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", tableName, name, strings.TrimSpace(rest)),
	}, true
}

func parseMetaColumnType(rest string) string {
	return schemaspec.ParseColumnType(rest)
}

func splitMetaIdentifierAndRest(s string) (identifier string, rest string) {
	return schemaspec.SplitIdentifierAndRest(s)
}

func isMetaConstraintDefinition(def string) bool {
	n := normalizeMetaSQLFragment(def)
	return strings.HasPrefix(n, "primary key") || strings.HasPrefix(n, "constraint ") || strings.HasPrefix(n, "unique key ")
}

func isSafeMetaRepairDiff(diff metaSchemaDiff, tableMissing map[string]bool) bool {
	switch diff.kind {
	case metaSchemaDiffMissingTable:
		return true
	case metaSchemaDiffMissingColumn:
		return isSafeAddColumnRepairSQL(diff.repairSQL)
	case metaSchemaDiffMissingIndex:
		normalized := normalizeMetaSQLFragment(diff.repairSQL)
		if strings.HasPrefix(normalized, "create index ") {
			return true
		}
		if strings.HasPrefix(normalized, "create unique index ") {
			return tableMissing[diff.tableName]
		}
		return false
	case metaSchemaDiffColumnType:
		return isSafeModifyColumnRepairSQL(diff)
	default:
		return false
	}
}

func isSafeAddColumnRepairSQL(sqlText string) bool {
	return schemaspec.IsSafeAddColumnRepairSQL(sqlText)
}

// isSafeModifyColumnRepairSQL returns true only for the single known-safe
// column widening: tenants.schema_version INT → INT UNSIGNED.
// schema_version values come from CRC32Version which produces uint32 (non-negative);
// widening from INT to INT UNSIGNED is data-preserving for this column.
// General MODIFY COLUMN is not considered safe without an explicit entry here.
func isSafeModifyColumnRepairSQL(diff metaSchemaDiff) bool {
	if diff.tableName != "tenants" || diff.columnName != "schema_version" {
		return false
	}
	n := strings.TrimSuffix(normalizeMetaSQLFragment(diff.repairSQL), ";")
	return n == "alter table tenants modify column schema_version int unsigned not null default 1"
}

func isIgnorableMetaSchemaError(err error) bool {
	return schemaspec.IsIgnorableMySQLError(err)
}

func loadMetaTableMeta(ctx context.Context, db *sql.DB, tableName string) (metaTableMeta, error) {
	rows, err := db.QueryContext(ctx, `SELECT column_name, column_type
		FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ?`, tableName)
	if err != nil {
		return metaTableMeta{}, err
	}
	defer func() { _ = rows.Close() }()

	columns := make(map[string]metaColumnMeta)
	for rows.Next() {
		var name, columnType string
		if err := rows.Scan(&name, &columnType); err != nil {
			return metaTableMeta{}, err
		}
		columns[strings.ToLower(name)] = metaColumnMeta{columnType: columnType}
	}
	if err := rows.Err(); err != nil {
		return metaTableMeta{}, err
	}
	if len(columns) == 0 {
		return metaTableMeta{}, sql.ErrNoRows
	}
	return metaTableMeta{tableName: tableName, columns: columns}, nil
}

func loadMetaIndexNames(ctx context.Context, db *sql.DB, tableName string) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT index_name
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ?`, tableName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	indexNames := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		indexNames[strings.ToLower(name)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return indexNames, nil
}

func normalizeMetaSQLFragment(s string) string {
	return schemaspec.NormalizeSQLFragment(s)
}

func sortedMetaColumnNames(columns map[string]metaColumnSpec) []string {
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedMetaIndexNames(indexes map[string]metaIndexSpec) []string {
	names := make([]string, 0, len(indexes))
	for name := range indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func schemaSnippet(stmt string) string {
	return schemaspec.SQLSnippet(stmt)
}

func (s *Store) InsertTenant(ctx context.Context, t *Tenant) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_tenant", start, &err)
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenants
		(id, status, kind, parent_tenant_id, storage_namespace_id, db_host, db_port, db_user, db_password, db_name, db_tls,
		 provider, cluster_id, branch_id, claim_url, claim_expires_at, schema_version,
		 s3_encryption_mode, s3_kms_key_id, s3_bucket_key_enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ID, t.Status, tenantKindForInsert(t), t.ParentTenantID, t.StorageNamespaceID,
		t.DBHost, t.DBPort, t.DBUser, t.DBPasswordCipher, t.DBName, boolToInt(t.DBTLS),
		t.Provider, nullStr(t.ClusterID), t.BranchID, nullStr(t.ClaimURL), t.ClaimExpiresAt, t.SchemaVersion,
		tenantS3EncryptionModeForInsert(t), t.S3KMSKeyID, boolToInt(tenantS3BucketKeyEnabledForInsert(t)),
		t.CreatedAt.UTC(), t.UpdatedAt.UTC())
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) UpsertTenantAutoEmbeddingProfile(ctx context.Context, p *TenantAutoEmbeddingProfile) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "upsert_tenant_auto_embedding_profile", start, &err)
	if p == nil {
		return fmt.Errorf("nil tenant auto-embedding profile")
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenant_auto_embedding_profiles
		(tenant_id, embedding_mode, model, dimensions, options_json, api_base, api_key_cipher, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			embedding_mode = VALUES(embedding_mode),
			model = VALUES(model),
			dimensions = VALUES(dimensions),
			options_json = VALUES(options_json),
			api_base = VALUES(api_base),
			api_key_cipher = VALUES(api_key_cipher),
			updated_at = VALUES(updated_at)`,
		p.TenantID, nullStr(p.EmbeddingMode), p.Model, p.Dimensions, p.OptionsJSON, nullStr(p.APIBase), nullableBytes(p.APIKeyCipher),
		p.CreatedAt.UTC(), p.UpdatedAt.UTC())
	return err
}

func (s *Store) GetTenantAutoEmbeddingProfile(ctx context.Context, tenantID string) (out *TenantAutoEmbeddingProfile, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_tenant_auto_embedding_profile", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT tenant_id, embedding_mode, model, dimensions, options_json,
			api_base, api_key_cipher, created_at, updated_at
		FROM tenant_auto_embedding_profiles WHERE tenant_id = ?`, tenantID)
	var rec TenantAutoEmbeddingProfile
	var embeddingMode sql.NullString
	var apiBase sql.NullString
	var apiKeyCipher []byte
	if err = row.Scan(&rec.TenantID, &embeddingMode, &rec.Model, &rec.Dimensions, &rec.OptionsJSON,
		&apiBase, &apiKeyCipher, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
			return nil, err
		}
		return nil, err
	}
	if embeddingMode.Valid {
		rec.EmbeddingMode = embeddingMode.String
	}
	if apiBase.Valid {
		rec.APIBase = apiBase.String
	}
	rec.APIKeyCipher = apiKeyCipher
	return &rec, nil
}

func (s *Store) EnsureTenantAutoEmbeddingProfile(ctx context.Context, tenantID string) (out *TenantAutoEmbeddingProfile, err error) {
	start := time.Now()
	defer observeMeta(ctx, "ensure_tenant_auto_embedding_profile", start, &err)
	profile, err := s.GetTenantAutoEmbeddingProfile(ctx, tenantID)
	if err == nil {
		return profile, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenant_auto_embedding_profiles (tenant_id) VALUES (?)`, tenantID)
	if err != nil && !isDuplicateEntry(err) {
		return nil, err
	}
	return s.GetTenantAutoEmbeddingProfile(ctx, tenantID)
}

func (s *Store) SetTenantAutoEmbeddingProfileModeIfNull(ctx context.Context, tenantID, mode string) (updated bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "set_tenant_auto_embedding_profile_mode_if_null", start, &err)
	res, err := s.db.ExecContext(ctx, `UPDATE tenant_auto_embedding_profiles
		SET embedding_mode = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND embedding_mode IS NULL`, mode, tenantID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Store) InsertAPIKey(ctx context.Context, k *APIKey) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_api_key", start, &err)
	scopeKind, err := apiKeyScopeKindForInsert(k)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenant_api_keys
		(id, tenant_id, key_name, jwt_ciphertext, jwt_hash, token_version, status, scope_kind,
		 issued_by_provider, issued_by_subject_key, issued_by_metadata_json,
		 issued_at, revoked_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		k.ID, k.TenantID, k.KeyName, k.JWTCiphertext, k.JWTHash, k.TokenVersion, k.Status, scopeKind,
		k.IssuedByProvider, k.IssuedBySubjectKey, nullableBytes(k.IssuedByMetadataJSON),
		k.IssuedAt.UTC(), k.RevokedAt, k.CreatedAt.UTC(), k.UpdatedAt.UTC())
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) GetExternalBinding(ctx context.Context, provider, subjectKey string) (out *ExternalBinding, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_external_binding", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT provider, subject_key, tenant_id, metadata_json, created_at, updated_at
		FROM tenant_external_bindings
		WHERE provider = ? AND subject_key = ?`, provider, subjectKey)
	var rec ExternalBinding
	var metadata []byte
	if err = row.Scan(&rec.Provider, &rec.SubjectKey, &rec.TenantID, &metadata, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
			return nil, err
		}
		return nil, err
	}
	rec.MetadataJSON = metadata
	return &rec, nil
}

func (s *Store) InsertExternalBinding(ctx context.Context, b *ExternalBinding) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_external_binding", start, &err)
	if b == nil {
		return fmt.Errorf("external binding is required")
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenant_external_bindings
		(provider, subject_key, tenant_id, metadata_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		b.Provider, b.SubjectKey, b.TenantID, nullableBytes(b.MetadataJSON), b.CreatedAt.UTC(), b.UpdatedAt.UTC())
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) DeleteExternalBinding(ctx context.Context, provider, subjectKey string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_external_binding", start, &err)
	res, err := s.db.ExecContext(ctx, `DELETE FROM tenant_external_bindings WHERE provider = ? AND subject_key = ?`, provider, subjectKey)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) UpsertTenantTiDBCloudOrgBinding(ctx context.Context, b *TenantTiDBCloudOrgBinding) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "upsert_tidbcloud_org_binding", start, &err)
	if b == nil {
		return fmt.Errorf("tidbcloud org binding is required")
	}
	tenantID := strings.TrimSpace(b.TenantID)
	organizationID := strings.TrimSpace(b.OrganizationID)
	clusterID := strings.TrimSpace(b.ClusterID)
	if tenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if organizationID == "" {
		return fmt.Errorf("organization_id is required")
	}
	if clusterID == "" {
		return fmt.Errorf("cluster_id is required")
	}
	branchID := strings.TrimSpace(b.BranchID)
	// tenants.branch_id is the source of truth; callers cannot override the
	// branch dimension used for duplicate-ownership checks.
	if tenantBranchID, ok, lookupErr := s.lookupTenantBranchID(ctx, tenantID); lookupErr != nil {
		return lookupErr
	} else if ok {
		branchID = tenantBranchID
	}
	now := time.Now().UTC()
	createdAt := b.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := b.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}
	return s.withTiDBCloudOrgBindingLock(ctx, organizationID, clusterID, branchID, func(ctx context.Context, conn metaQueryExecer) error {
		if err := deleteStaleTiDBCloudOrgBindingConflicts(ctx, conn, tenantID, organizationID, clusterID, branchID); err != nil {
			return err
		}
		if err := ensureTiDBCloudOrgBindingAvailable(ctx, conn, tenantID, organizationID, clusterID, branchID); err != nil {
			return err
		}
		_, execErr := conn.ExecContext(ctx, `INSERT INTO tenant_tidbcloud_org_bindings
			(tenant_id, organization_id, cluster_id, branch_id, pool_id, pool_status, used_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				organization_id = VALUES(organization_id),
				cluster_id = VALUES(cluster_id),
				branch_id = VALUES(branch_id),
				pool_id = VALUES(pool_id),
				pool_status = VALUES(pool_status),
				used_at = VALUES(used_at),
			updated_at = VALUES(updated_at)`,
			tenantID, organizationID, clusterID, branchID, strings.TrimSpace(b.PoolID), tenantPoolBindingStatusForInsert(b.PoolStatus), b.UsedAt,
			createdAt.UTC(), updatedAt.UTC())
		return execErr
	})
}

func deleteStaleTiDBCloudOrgBindingConflicts(ctx context.Context, q metaQueryExecer, tenantID, organizationID, clusterID, branchID string) error {
	_, err := q.ExecContext(ctx, `DELETE b
		FROM tenant_tidbcloud_org_bindings b
		LEFT JOIN tenants t ON t.id = b.tenant_id
		WHERE b.organization_id = ? AND b.cluster_id = ? AND b.branch_id = ?
			AND b.tenant_id <> ? AND (t.id IS NULL OR t.status = ?)`,
		organizationID, clusterID, branchID, tenantID, TenantDeleted)
	if err != nil {
		return fmt.Errorf("delete stale tidbcloud org binding conflicts: %w", err)
	}
	return nil
}

func (s *Store) lookupTenantBranchID(ctx context.Context, tenantID string) (string, bool, error) {
	var branchID sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT branch_id FROM tenants WHERE id = ?`, tenantID).Scan(&branchID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if branchID.Valid {
		return strings.TrimSpace(branchID.String), true, nil
	}
	return "", true, nil
}

func (s *Store) withTiDBCloudOrgBindingLock(ctx context.Context, organizationID, clusterID, branchID string, fn func(context.Context, metaQueryExecer) error) (err error) {
	if fn == nil {
		return fmt.Errorf("tidbcloud org binding lock callback is required")
	}
	lockName := tidbCloudOrgBindingLockName(organizationID, clusterID, branchID)
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	var databaseName sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&databaseName); err != nil {
		return err
	}
	lockName = tenantPoolDatabaseLockName(lockName, databaseName.String)

	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, tidbCloudOrgBindingLockTimeoutSeconds).Scan(&got); err != nil {
		return err
	}
	if !got.Valid {
		return fmt.Errorf("tidbcloud org binding named lock returned NULL")
	}
	if got.Int64 != 1 {
		return fmt.Errorf("timed out waiting for tidbcloud org binding named lock")
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), tidbCloudOrgBindingReleaseLockTimeout)
		defer cancel()
		var released sql.NullInt64
		releaseErr := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released)
		if releaseErr != nil {
			err = errors.Join(err, releaseErr)
			return
		}
		if !released.Valid {
			err = errors.Join(err, fmt.Errorf("tidbcloud org binding named lock release returned NULL"))
			return
		}
		if released.Int64 != 1 {
			err = errors.Join(err, fmt.Errorf("tidbcloud org binding named lock was not held by current connection"))
		}
	}()

	return fn(ctx, conn)
}

func ensureTiDBCloudOrgBindingAvailable(ctx context.Context, q metaQueryExecer, tenantID, organizationID, clusterID, branchID string) error {
	var existingTenantID string
	err := q.QueryRowContext(ctx, `SELECT b.tenant_id
		FROM tenant_tidbcloud_org_bindings b
		JOIN tenants t ON t.id = b.tenant_id
		WHERE b.organization_id = ? AND b.cluster_id = ? AND b.branch_id = ?
			AND b.tenant_id <> ? AND t.status <> ?
		LIMIT 1`,
		organizationID, clusterID, branchID, tenantID, TenantDeleted).Scan(&existingTenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: tidbcloud organization cluster branch already bound to tenant %s", ErrDuplicate, existingTenantID)
}

func (s *Store) GetTenantTiDBCloudOrgBinding(ctx context.Context, tenantID string) (out *TenantTiDBCloudOrgBinding, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_tidbcloud_org_binding", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT tenant_id, organization_id, cluster_id, branch_id, pool_id, pool_status, used_at, created_at, updated_at
		FROM tenant_tidbcloud_org_bindings WHERE tenant_id = ?`, tenantID)
	var rec TenantTiDBCloudOrgBinding
	var usedAt sql.NullTime
	if err = row.Scan(&rec.TenantID, &rec.OrganizationID, &rec.ClusterID, &rec.BranchID, &rec.PoolID, &rec.PoolStatus, &usedAt, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
			return nil, err
		}
		return nil, err
	}
	if usedAt.Valid {
		t := usedAt.Time.UTC()
		rec.UsedAt = &t
	}
	return &rec, nil
}

func (s *Store) CreateTenantPool(ctx context.Context, p *TenantPool) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "create_tidbcloud_pool", start, &err)
	if p == nil {
		return fmt.Errorf("tenant pool is required")
	}
	poolID := strings.TrimSpace(p.PoolID)
	if poolID == "" {
		return fmt.Errorf("pool_id is required")
	}
	if p.Size < 0 {
		return fmt.Errorf("pool size must be non-negative")
	}
	now := time.Now().UTC()
	createdAt := p.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := p.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenant_tidbcloud_pools
		(pool_id, organization_id, size, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		poolID, nullStr(strings.TrimSpace(p.OrganizationID)), p.Size, tenantPoolStatusForInsert(p.Status),
		createdAt.UTC(), updatedAt.UTC())
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) GetTenantPoolByOrganization(ctx context.Context, organizationID string) (out *TenantPool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_tidbcloud_pool_by_org", start, &err)
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return nil, fmt.Errorf("organization_id is required")
	}
	row := s.db.QueryRowContext(ctx, `SELECT pool_id, organization_id, size, status, created_at, updated_at
		FROM tenant_tidbcloud_pools WHERE organization_id = ?`, organizationID)
	return scanTenantPoolRow(row)
}

func (s *Store) GetTenantPoolByID(ctx context.Context, poolID string) (out *TenantPool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_tidbcloud_pool_by_id", start, &err)
	poolID = strings.TrimSpace(poolID)
	if poolID == "" {
		return nil, fmt.Errorf("pool_id is required")
	}
	row := s.db.QueryRowContext(ctx, `SELECT pool_id, organization_id, size, status, created_at, updated_at
		FROM tenant_tidbcloud_pools WHERE pool_id = ?`, poolID)
	return scanTenantPoolRow(row)
}

func (s *Store) UpdateTenantPoolOrganization(ctx context.Context, poolID, organizationID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tidbcloud_pool_org", start, &err)
	poolID = strings.TrimSpace(poolID)
	organizationID = strings.TrimSpace(organizationID)
	if poolID == "" || organizationID == "" {
		return fmt.Errorf("pool_id and organization_id are required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tenant_tidbcloud_pools
		SET organization_id = ?, updated_at = ? WHERE pool_id = ?`, organizationID, time.Now().UTC(), poolID)
	if err != nil {
		if isDuplicateEntry(err) {
			return ErrDuplicate
		}
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) CountFreeTenantPoolBindings(ctx context.Context, organizationID string) (out int, err error) {
	return s.countFreeTenantPoolBindingsByStatus(ctx, organizationID, []TenantStatus{TenantActive})
}

func (s *Store) CountTenantPoolFreeSlots(ctx context.Context, organizationID string) (out int, err error) {
	return s.countFreeTenantPoolBindingsByStatus(ctx, organizationID, []TenantStatus{TenantPending, TenantProvisioning, TenantActive})
}

func (s *Store) countFreeTenantPoolBindingsByStatus(ctx context.Context, organizationID string, statuses []TenantStatus) (out int, err error) {
	start := time.Now()
	defer observeMeta(ctx, "count_free_tidbcloud_pool_bindings", start, &err)
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return 0, fmt.Errorf("organization_id is required")
	}
	if len(statuses) == 0 {
		return 0, fmt.Errorf("tenant statuses are required")
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(statuses)), ",")
	args := make([]any, 0, 3+len(statuses))
	args = append(args, organizationID, TenantPoolBindingFree, tidbCloudNativeProvider)
	for _, status := range statuses {
		args = append(args, status)
	}
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM tenant_tidbcloud_org_bindings b
		JOIN tenants t ON t.id = b.tenant_id
		WHERE b.organization_id = ? AND b.pool_status = ? AND t.provider = ?
			AND t.status IN (`+placeholders+`)`, args...)
	if err = row.Scan(&out); err != nil {
		return 0, err
	}
	return out, nil
}

func (s *Store) CountTenantPoolBindingsByStatus(ctx context.Context) (out []TenantPoolBindingStatusCount, err error) {
	start := time.Now()
	defer observeMeta(ctx, "count_tidbcloud_pool_bindings_by_status", start, &err)
	rows, err := s.db.QueryContext(ctx, `SELECT
			p.pool_id,
			p.organization_id,
			statuses.pool_status,
			COUNT(t.id)
		FROM tenant_tidbcloud_pools p
		JOIN (
			SELECT ? AS pool_status
			UNION ALL
			SELECT ? AS pool_status
		) statuses
		LEFT JOIN tenant_tidbcloud_org_bindings b
			ON b.pool_id = p.pool_id
			AND b.organization_id = p.organization_id
			AND b.pool_status = statuses.pool_status
		LEFT JOIN tenants t
			ON t.id = b.tenant_id
			AND t.provider = ?
			AND t.status <> ?
		WHERE p.pool_id <> ''
			AND p.organization_id IS NOT NULL
			AND p.organization_id <> ''
		GROUP BY p.pool_id, p.organization_id, statuses.pool_status
		ORDER BY p.pool_id, p.organization_id, statuses.pool_status`,
		TenantPoolBindingFree, TenantPoolBindingUsed, tidbCloudNativeProvider, TenantDeleted)
	if err != nil {
		return nil, fmt.Errorf("count tenant pool bindings by status query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var rec TenantPoolBindingStatusCount
		if err := rows.Scan(&rec.PoolID, &rec.OrganizationID, &rec.Status, &rec.Count); err != nil {
			return nil, fmt.Errorf("count tenant pool bindings by status scan: %w", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("count tenant pool bindings by status rows: %w", err)
	}
	return out, nil
}

func (s *Store) ListFreeTenantPoolBindings(ctx context.Context, organizationID string, newestFirst bool, limit int) (out []TenantWithTiDBCloudOrgBinding, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_free_tidbcloud_pool_bindings", start, &err)
	return s.listFreeTenantPoolBindings(ctx, organizationID, newestFirst, limit, []TenantStatus{TenantActive})
}

func (s *Store) ListFreeTenantPoolBindingsForDelete(ctx context.Context, organizationID string, newestFirst bool, limit int) (out []TenantWithTiDBCloudOrgBinding, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_free_tidbcloud_pool_bindings_for_delete", start, &err)
	return s.listFreeTenantPoolBindings(ctx, organizationID, newestFirst, limit, []TenantStatus{TenantPending, TenantProvisioning, TenantActive, TenantFailed})
}

func (s *Store) ListTenantPoolFreeSlotsForDelete(ctx context.Context, organizationID string, newestFirst bool, limit int) (out []TenantWithTiDBCloudOrgBinding, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_tidbcloud_pool_free_slots_for_delete", start, &err)
	return s.listFreeTenantPoolBindings(ctx, organizationID, newestFirst, limit, []TenantStatus{TenantPending, TenantProvisioning, TenantActive})
}

func (s *Store) ListPendingTenantPoolBindingsForResume(ctx context.Context, organizationID string, limit int) (out []TenantWithTiDBCloudOrgBinding, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_pending_tidbcloud_pool_bindings_for_resume", start, &err)
	return s.listFreeTenantPoolBindings(ctx, organizationID, false, limit, []TenantStatus{TenantPending})
}

func (s *Store) listFreeTenantPoolBindings(ctx context.Context, organizationID string, newestFirst bool, limit int, statuses []TenantStatus) (out []TenantWithTiDBCloudOrgBinding, err error) {
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return nil, fmt.Errorf("organization_id is required")
	}
	if len(statuses) == 0 {
		return nil, fmt.Errorf("tenant statuses are required")
	}
	if limit <= 0 {
		limit = 100
	}
	order := "ASC"
	if newestFirst {
		order = "DESC"
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(statuses)), ",")
	query := `SELECT
			t.id, t.status, t.kind, t.parent_tenant_id, t.storage_namespace_id,
			t.db_host, t.db_port, t.db_user, t.db_password, t.db_name,
			t.db_tls, t.provider, t.cluster_id, t.branch_id, t.claim_url, t.claim_expires_at, t.schema_version,
			t.s3_encryption_mode, t.s3_kms_key_id, t.s3_bucket_key_enabled, t.created_at, t.updated_at,
			b.tenant_id, b.organization_id, b.cluster_id, b.branch_id, b.pool_id, b.pool_status, b.used_at, b.created_at, b.updated_at
		FROM tenant_tidbcloud_org_bindings b
		JOIN tenants t ON t.id = b.tenant_id
			WHERE b.organization_id = ? AND b.pool_status = ? AND t.provider = ?
				AND t.status IN (` + placeholders + `)
			ORDER BY b.created_at ` + order + `, b.tenant_id ` + order + `
			LIMIT ?`
	args := make([]any, 0, 4+len(statuses))
	args = append(args, organizationID, TenantPoolBindingFree, tidbCloudNativeProvider)
	for _, status := range statuses {
		args = append(args, status)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanTenantBindingRows(rows)
}

func (s *Store) ClaimOldestFreeTenantPoolBinding(ctx context.Context, organizationID string) (out *TenantWithTiDBCloudOrgBinding, err error) {
	start := time.Now()
	defer observeMeta(ctx, "claim_free_tidbcloud_pool_binding", start, &err)
	organizationID = strings.TrimSpace(organizationID)
	if organizationID == "" {
		return nil, fmt.Errorf("organization_id is required")
	}
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		query := `SELECT
				t.id, t.status, t.kind, t.parent_tenant_id, t.storage_namespace_id,
				t.db_host, t.db_port, t.db_user, t.db_password, t.db_name,
				t.db_tls, t.provider, t.cluster_id, t.branch_id, t.claim_url, t.claim_expires_at, t.schema_version,
				t.s3_encryption_mode, t.s3_kms_key_id, t.s3_bucket_key_enabled, t.created_at, t.updated_at,
				b.tenant_id, b.organization_id, b.cluster_id, b.branch_id, b.pool_id, b.pool_status, b.used_at, b.created_at, b.updated_at
			FROM tenant_tidbcloud_org_bindings b
			JOIN tenants t ON t.id = b.tenant_id
				WHERE b.organization_id = ? AND b.pool_status = ? AND t.provider = ?
					AND t.status = ?
				ORDER BY b.created_at ASC, b.tenant_id ASC
				LIMIT 1 FOR UPDATE`
		row := tx.QueryRowContext(ctx, query, organizationID, TenantPoolBindingFree, tidbCloudNativeProvider, TenantActive)
		rec, scanErr := scanTenantBindingRow(row)
		if scanErr != nil {
			return scanErr
		}
		now := time.Now().UTC()
		res, execErr := tx.ExecContext(ctx, `UPDATE tenant_tidbcloud_org_bindings
			SET pool_status = ?, used_at = ?, updated_at = ?
			WHERE tenant_id = ? AND pool_status = ?`,
			TenantPoolBindingUsed, now, now, rec.Binding.TenantID, TenantPoolBindingFree)
		if execErr != nil {
			return execErr
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ErrNotFound
		}
		rec.Binding.PoolStatus = TenantPoolBindingUsed
		rec.Binding.UsedAt = &now
		rec.Binding.UpdatedAt = now
		out = rec
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) UpdateTenantPoolSize(ctx context.Context, poolID string, size int) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tidbcloud_pool_size", start, &err)
	poolID = strings.TrimSpace(poolID)
	if poolID == "" {
		return fmt.Errorf("pool_id is required")
	}
	if size < 0 {
		return fmt.Errorf("pool size must be non-negative")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tenant_tidbcloud_pools SET size = ?, updated_at = ? WHERE pool_id = ?`, size, time.Now().UTC(), poolID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) UpdateTenantPoolStatus(ctx context.Context, poolID string, status TenantPoolStatus) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tidbcloud_pool_status", start, &err)
	poolID = strings.TrimSpace(poolID)
	if poolID == "" {
		return fmt.Errorf("pool_id is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tenant_tidbcloud_pools SET status = ?, updated_at = ? WHERE pool_id = ?`,
		tenantPoolStatusForInsert(status), time.Now().UTC(), poolID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) DeleteTenantPool(ctx context.Context, poolID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_tidbcloud_pool", start, &err)
	poolID = strings.TrimSpace(poolID)
	if poolID == "" {
		return fmt.Errorf("pool_id is required")
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM tenant_tidbcloud_pools WHERE pool_id = ?`, poolID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) UpdateTenantPoolBindingStatus(ctx context.Context, tenantID string, status TenantPoolBindingStatus, usedAt *time.Time) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tidbcloud_pool_binding_status", start, &err)
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tenant_tidbcloud_org_bindings
		SET pool_status = ?, used_at = ?, updated_at = ? WHERE tenant_id = ?`,
		tenantPoolBindingStatusForInsert(status), usedAt, time.Now().UTC(), tenantID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) MarkFreeTenantPoolTenantDeleting(ctx context.Context, tenantID string, from TenantStatus) (updated bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "mark_free_tidbcloud_pool_tenant_deleting", start, &err)
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return false, fmt.Errorf("tenant_id is required")
	}
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		var id string
		row := tx.QueryRowContext(ctx, `SELECT t.id
			FROM tenants t
			JOIN tenant_tidbcloud_org_bindings b ON b.tenant_id = t.id
			WHERE t.id = ? AND t.status = ? AND b.pool_status = ?
			LIMIT 1 FOR UPDATE`, tenantID, from, TenantPoolBindingFree)
		if scanErr := row.Scan(&id); scanErr != nil {
			return scanErr
		}
		res, execErr := tx.ExecContext(ctx, `UPDATE tenants SET status = ?, updated_at = ? WHERE id = ? AND status = ?`,
			TenantDeleting, time.Now().UTC(), tenantID, from)
		if execErr != nil {
			return execErr
		}
		n, _ := res.RowsAffected()
		updated = n > 0
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return updated, nil
}

func (s *Store) ListTenantsByTiDBCloudOrganizations(ctx context.Context, organizationIDs []string, offset, limit int) (out []TenantWithTiDBCloudOrgBinding, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_tenants_by_tidbcloud_orgs", start, &err)
	organizationIDs = compactStrings(organizationIDs)
	if len(organizationIDs) == 0 {
		return []TenantWithTiDBCloudOrgBinding{}, nil
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 10
	}
	placeholders := make([]string, len(organizationIDs))
	args := make([]any, 0, len(organizationIDs)+5)
	for i, id := range organizationIDs {
		placeholders[i] = "?"
		args = append(args, id)
	}
	args = append(args, tidbCloudNativeProvider, TenantPoolBindingFree, TenantDeleted, limit, offset)
	query := `SELECT
			t.id, t.status, t.kind, t.parent_tenant_id, t.storage_namespace_id,
			t.db_host, t.db_port, t.db_user, t.db_password, t.db_name,
			t.db_tls, t.provider, t.cluster_id, t.branch_id, t.claim_url, t.claim_expires_at, t.schema_version,
			t.s3_encryption_mode, t.s3_kms_key_id, t.s3_bucket_key_enabled, t.created_at, t.updated_at,
			b.tenant_id, b.organization_id, b.cluster_id, b.branch_id, b.pool_id, b.pool_status, b.used_at, b.created_at, b.updated_at
			FROM tenant_tidbcloud_org_bindings b
			JOIN tenants t ON t.id = b.tenant_id
			WHERE b.organization_id IN (` + strings.Join(placeholders, ",") + `)
				AND t.provider = ?
				AND b.pool_status <> ?
				AND t.status <> ?
			ORDER BY t.created_at DESC, t.id DESC
			LIMIT ? OFFSET ?`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanTenantBindingRows(rows)
}

func (s *Store) ListTenantsByTiDBCloudOrgClusterBindings(ctx context.Context, bindings []TenantTiDBCloudOrgBinding, offset, limit int) (out []TenantWithTiDBCloudOrgBinding, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_tenants_by_tidbcloud_org_clusters", start, &err)
	bindings = compactTiDBCloudOrgClusterBindings(bindings)
	if len(bindings) == 0 {
		return []TenantWithTiDBCloudOrgBinding{}, nil
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 10
	}
	placeholders := make([]string, 0, len(bindings))
	args := make([]any, 0, len(bindings)*2+5)
	for _, binding := range bindings {
		placeholders = append(placeholders, "(?, ?)")
		args = append(args, binding.OrganizationID, binding.ClusterID)
	}
	args = append(args, tidbCloudNativeProvider, TenantPoolBindingFree, TenantDeleted, limit, offset)
	query := `SELECT
			t.id, t.status, t.kind, t.parent_tenant_id, t.storage_namespace_id,
			t.db_host, t.db_port, t.db_user, t.db_password, t.db_name,
			t.db_tls, t.provider, t.cluster_id, t.branch_id, t.claim_url, t.claim_expires_at, t.schema_version,
			t.s3_encryption_mode, t.s3_kms_key_id, t.s3_bucket_key_enabled, t.created_at, t.updated_at,
			b.tenant_id, b.organization_id, b.cluster_id, b.branch_id, b.pool_id, b.pool_status, b.used_at, b.created_at, b.updated_at
			FROM tenant_tidbcloud_org_bindings b
			JOIN tenants t ON t.id = b.tenant_id
			WHERE (b.organization_id, b.cluster_id) IN (` + strings.Join(placeholders, ",") + `)
				AND t.provider = ?
				AND b.pool_status <> ?
				AND t.status <> ?
			ORDER BY t.created_at DESC, t.id DESC
			LIMIT ? OFFSET ?`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanTenantBindingRows(rows)
}

func compactStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func compactTiDBCloudOrgClusterBindings(bindings []TenantTiDBCloudOrgBinding) []TenantTiDBCloudOrgBinding {
	out := make([]TenantTiDBCloudOrgBinding, 0, len(bindings))
	seen := make(map[string]bool, len(bindings))
	for _, binding := range bindings {
		orgID := strings.TrimSpace(binding.OrganizationID)
		clusterID := strings.TrimSpace(binding.ClusterID)
		if orgID == "" || clusterID == "" {
			continue
		}
		key := orgID + "\x00" + clusterID
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, TenantTiDBCloudOrgBinding{OrganizationID: orgID, ClusterID: clusterID})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OrganizationID != out[j].OrganizationID {
			return out[i].OrganizationID < out[j].OrganizationID
		}
		return out[i].ClusterID < out[j].ClusterID
	})
	return out
}

func (s *Store) WithExternalBindingLock(ctx context.Context, provider, subjectKey string, fn func(context.Context) error) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "external_binding_lock", start, &err)
	if fn == nil {
		return fmt.Errorf("external binding lock callback is required")
	}
	lockName := externalBindingLockName(provider, subjectKey)
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(CONCAT(?, DATABASE()), ?)", lockName, externalBindingLockTimeoutSeconds).Scan(&got); err != nil {
		return err
	}
	if !got.Valid {
		return fmt.Errorf("external binding named lock returned NULL")
	}
	if got.Int64 != 1 {
		return fmt.Errorf("timed out waiting for external binding named lock")
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), externalBindingReleaseLockTimeout)
		defer cancel()
		var released sql.NullInt64
		releaseErr := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(CONCAT(?, DATABASE()))", lockName).Scan(&released)
		if releaseErr != nil {
			err = errors.Join(err, releaseErr)
			return
		}
		if !released.Valid {
			err = errors.Join(err, fmt.Errorf("external binding named lock release returned NULL"))
			return
		}
		if released.Int64 != 1 {
			err = errors.Join(err, fmt.Errorf("external binding named lock was not held by current connection"))
		}
	}()

	return fn(ctx)
}

func (s *Store) WithTenantPoolLock(ctx context.Context, poolID string, fn func(context.Context) error) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "tenant_pool_lock", start, &err)
	if fn == nil {
		return fmt.Errorf("tenant pool lock callback is required")
	}
	lockName := tenantPoolLockName(poolID)
	if lockName == "" {
		return fmt.Errorf("pool_id is required")
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	var databaseName sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&databaseName); err != nil {
		return err
	}
	lockName = tenantPoolDatabaseLockName(lockName, databaseName.String)

	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, tenantPoolLockTimeoutSeconds).Scan(&got); err != nil {
		return err
	}
	if !got.Valid {
		return fmt.Errorf("tenant pool named lock returned NULL")
	}
	if got.Int64 != 1 {
		return fmt.Errorf("timed out waiting for tenant pool named lock")
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), tenantPoolReleaseLockTimeout)
		defer cancel()
		var released sql.NullInt64
		releaseErr := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released)
		if releaseErr != nil {
			err = errors.Join(err, releaseErr)
			return
		}
		if !released.Valid {
			err = errors.Join(err, fmt.Errorf("tenant pool named lock release returned NULL"))
			return
		}
		if released.Int64 != 1 {
			err = errors.Join(err, fmt.Errorf("tenant pool named lock was not held by current connection"))
		}
	}()

	return fn(ctx)
}

func externalBindingLockName(provider, subjectKey string) string {
	sum := sha256.Sum256([]byte(provider + "\x00" + subjectKey))
	return "d9_extbind:" + hex.EncodeToString(sum[:20])
}

func tidbCloudOrgBindingLockName(organizationID, clusterID, branchID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(organizationID) + "\x00" + strings.TrimSpace(clusterID) + "\x00" + strings.TrimSpace(branchID)))
	return "d9_tidb_orgbind:" + hex.EncodeToString(sum[:16])
}

func tenantPoolLockName(poolID string) string {
	poolID = strings.TrimSpace(poolID)
	if poolID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(poolID))
	return "d9_tenant_pool:" + hex.EncodeToString(sum[:16])
}

func tenantPoolDatabaseLockName(baseLockName, databaseName string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(databaseName)))
	return baseLockName + ":" + hex.EncodeToString(sum[:4])
}

func (s *Store) ResolveByAPIKeyHash(ctx context.Context, hash string) (out *TenantWithAPIKey, err error) {
	start := time.Now()
	defer observeMeta(ctx, "resolve_api_key_hash", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT
			t.id, t.status, t.kind, t.parent_tenant_id, t.storage_namespace_id,
			t.db_host, t.db_port, t.db_user, t.db_password, t.db_name, t.db_tls,
			t.provider, t.cluster_id, t.branch_id, t.claim_url, t.claim_expires_at, t.schema_version,
			t.s3_encryption_mode, t.s3_kms_key_id, t.s3_bucket_key_enabled, t.created_at, t.updated_at,
			k.id, k.tenant_id, k.key_name, k.jwt_ciphertext, k.jwt_hash, k.token_version, k.status, k.scope_kind,
			k.issued_by_provider, k.issued_by_subject_key, k.issued_by_metadata_json, k.issued_at,
			k.revoked_at, k.created_at, k.updated_at, COALESCE(b.organization_id, '')
		FROM tenant_api_keys k
		JOIN tenants t ON t.id = k.tenant_id
		LEFT JOIN tenant_tidbcloud_org_bindings b ON b.tenant_id = t.id
		WHERE k.jwt_hash = ?`, hash)

	var rec TenantWithAPIKey
	var dbTLS int
	var claimURL sql.NullString
	var claimExp sql.NullTime
	var clusterID sql.NullString
	var parentTenantID sql.NullString
	var storageNamespaceID sql.NullString
	var revokedAt sql.NullTime
	var s3BucketKeyEnabled int
	var issuedByProvider sql.NullString
	var issuedBySubjectKey sql.NullString
	var issuedByMetadataJSON []byte
	if err = row.Scan(
		&rec.Tenant.ID, &rec.Tenant.Status, &rec.Tenant.Kind, &parentTenantID, &storageNamespaceID,
		&rec.Tenant.DBHost, &rec.Tenant.DBPort, &rec.Tenant.DBUser,
		&rec.Tenant.DBPasswordCipher, &rec.Tenant.DBName, &dbTLS, &rec.Tenant.Provider, &clusterID,
		&rec.Tenant.BranchID, &claimURL, &claimExp, &rec.Tenant.SchemaVersion, &rec.Tenant.S3EncryptionMode,
		&rec.Tenant.S3KMSKeyID, &s3BucketKeyEnabled, &rec.Tenant.CreatedAt, &rec.Tenant.UpdatedAt,
		&rec.APIKey.ID, &rec.APIKey.TenantID, &rec.APIKey.KeyName, &rec.APIKey.JWTCiphertext,
		&rec.APIKey.JWTHash, &rec.APIKey.TokenVersion, &rec.APIKey.Status, &rec.APIKey.ScopeKind,
		&issuedByProvider, &issuedBySubjectKey, &issuedByMetadataJSON, &rec.APIKey.IssuedAt,
		&revokedAt, &rec.APIKey.CreatedAt, &rec.APIKey.UpdatedAt, &rec.TiDBCloudOrgID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
			return nil, err
		}
		return nil, err
	}
	rec.Tenant.DBTLS = dbTLS == 1
	rec.Tenant.S3BucketKeyEnabled = boolPtr(s3BucketKeyEnabled == 1)
	if clusterID.Valid {
		rec.Tenant.ClusterID = clusterID.String
	}
	if parentTenantID.Valid {
		rec.Tenant.ParentTenantID = parentTenantID.String
	}
	if storageNamespaceID.Valid {
		rec.Tenant.StorageNamespaceID = storageNamespaceID.String
	}
	if claimURL.Valid {
		rec.Tenant.ClaimURL = claimURL.String
	}
	if claimExp.Valid {
		t := claimExp.Time.UTC()
		rec.Tenant.ClaimExpiresAt = &t
	}
	if revokedAt.Valid {
		t := revokedAt.Time.UTC()
		rec.APIKey.RevokedAt = &t
	}
	if issuedByProvider.Valid {
		rec.APIKey.IssuedByProvider = issuedByProvider.String
	}
	if issuedBySubjectKey.Valid {
		rec.APIKey.IssuedBySubjectKey = issuedBySubjectKey.String
	}
	rec.APIKey.IssuedByMetadataJSON = issuedByMetadataJSON
	out = &rec
	return out, nil
}

func (s *Store) InsertAPIKeyFSScope(ctx context.Context, scope *APIKeyFSScope) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_api_key_fs_scope", start, &err)
	if scope == nil {
		return fmt.Errorf("fs scope is required")
	}
	prefix, err := canonicalFSScopePrefix(scope.Prefix)
	if err != nil {
		return err
	}
	if err := validateFSScopeOps(scope.Ops); err != nil {
		return err
	}
	if strings.TrimSpace(prefix) == "" {
		return fmt.Errorf("fs scope prefix is required")
	}
	prefixHash := fsScopePrefixHash(prefix)
	createdAt := scope.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	updatedAt := scope.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenant_api_key_fs_scopes
		(tenant_id, api_key_id, prefix, prefix_hash, ops, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		scope.TenantID, scope.APIKeyID, prefix, prefixHash, scope.Ops,
		createdAt.UTC(), updatedAt.UTC())
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) ListAPIKeyFSScopes(ctx context.Context, tenantID, apiKeyID string) (out []APIKeyFSScope, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_api_key_fs_scopes", start, &err)
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id, api_key_id, prefix, prefix_hash, ops, created_at, updated_at
		FROM tenant_api_key_fs_scopes
		WHERE tenant_id = ? AND api_key_id = ?
		ORDER BY prefix`, tenantID, apiKeyID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var scope APIKeyFSScope
		if err := rows.Scan(&scope.TenantID, &scope.APIKeyID, &scope.Prefix, &scope.PrefixHash, &scope.Ops, &scope.CreatedAt, &scope.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, scope)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func apiKeyScopeKindForInsert(k *APIKey) (APIKeyScopeKind, error) {
	if k == nil || k.ScopeKind == "" {
		return APIKeyScopeKindOwner, nil
	}
	if !isValidAPIKeyScopeKind(k.ScopeKind) {
		return "", fmt.Errorf("unsupported api key scope kind %q", k.ScopeKind)
	}
	return k.ScopeKind, nil
}

func isValidAPIKeyScopeKind(kind APIKeyScopeKind) bool {
	switch kind {
	case APIKeyScopeKindOwner, APIKeyScopeKindFS:
		return true
	default:
		return false
	}
}

func canonicalFSScopePrefix(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("fs scope prefix is required")
	}
	if strings.TrimSpace(raw) == ":" {
		return "", fmt.Errorf("fs scope prefix is required")
	}
	raw = strings.TrimPrefix(raw, ":")
	prefix, err := pathutil.Canonicalize(raw)
	if err != nil {
		return "", fmt.Errorf("invalid fs scope prefix: %w", err)
	}
	return prefix, nil
}

func validateFSScopeOps(raw string) error {
	ops := make(map[string]bool)
	for _, part := range strings.Split(raw, ",") {
		op := strings.TrimSpace(part)
		if op == "" {
			continue
		}
		switch op {
		case "read", "list", "search", "write", "delete":
			ops[op] = true
		default:
			return fmt.Errorf("unknown fs scope op %q", op)
		}
	}
	if len(ops) == 0 {
		return fmt.Errorf("empty fs scope ops")
	}
	if ops["search"] && !ops["read"] {
		return fmt.Errorf("search fs scope requires read")
	}
	return nil
}

func fsScopePrefixHash(prefix string) string {
	sum := sha256.Sum256([]byte(prefix))
	return hex.EncodeToString(sum[:])
}

func (s *Store) GetTenant(ctx context.Context, id string) (out *Tenant, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_tenant", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT id, status, kind, parent_tenant_id, storage_namespace_id,
		db_host, db_port, db_user, db_password, db_name,
		db_tls, provider, cluster_id, branch_id, claim_url, claim_expires_at, schema_version,
		s3_encryption_mode, s3_kms_key_id, s3_bucket_key_enabled, created_at, updated_at
		FROM tenants WHERE id = ?`, id)
	var dbTLS int
	var clusterID sql.NullString
	var parentTenantID sql.NullString
	var storageNamespaceID sql.NullString
	var claimURL sql.NullString
	var claimExp sql.NullTime
	var s3BucketKeyEnabled int
	var rec Tenant
	if err = row.Scan(&rec.ID, &rec.Status, &rec.Kind, &parentTenantID, &storageNamespaceID,
		&rec.DBHost, &rec.DBPort, &rec.DBUser, &rec.DBPasswordCipher,
		&rec.DBName, &dbTLS, &rec.Provider, &clusterID, &rec.BranchID, &claimURL, &claimExp, &rec.SchemaVersion,
		&rec.S3EncryptionMode, &rec.S3KMSKeyID, &s3BucketKeyEnabled, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
			return nil, err
		}
		return nil, err
	}
	rec.DBTLS = dbTLS == 1
	rec.S3BucketKeyEnabled = boolPtr(s3BucketKeyEnabled == 1)
	if clusterID.Valid {
		rec.ClusterID = clusterID.String
	}
	if parentTenantID.Valid {
		rec.ParentTenantID = parentTenantID.String
	}
	if storageNamespaceID.Valid {
		rec.StorageNamespaceID = storageNamespaceID.String
	}
	if claimURL.Valid {
		rec.ClaimURL = claimURL.String
	}
	if claimExp.Valid {
		t := claimExp.Time.UTC()
		rec.ClaimExpiresAt = &t
	}
	out = &rec
	return out, nil
}

func (s *Store) ListTenantsByStatus(ctx context.Context, status TenantStatus, limit int) (out []Tenant, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_tenants_by_status", start, &err)
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, status, kind, parent_tenant_id, storage_namespace_id,
		db_host, db_port, db_user, db_password, db_name,
		db_tls, provider, cluster_id, branch_id, claim_url, claim_expires_at, schema_version,
		s3_encryption_mode, s3_kms_key_id, s3_bucket_key_enabled, created_at, updated_at
		FROM tenants WHERE status = ? ORDER BY created_at ASC, id ASC LIMIT ?`, status, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	return scanTenantRows(rows)
}

func (s *Store) CountTenants(ctx context.Context) (out TenantCounts, err error) {
	start := time.Now()
	defer observeMeta(ctx, "count_tenants", start, &err)
	counts := make(map[TenantStatus]int64, len(allTenantStatuses))
	for _, status := range allTenantStatuses {
		counts[status] = 0
	}
	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM tenants GROUP BY status`)
	if err != nil {
		return out, fmt.Errorf("count tenants: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var status TenantStatus
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return out, fmt.Errorf("scan tenant count: %w", err)
		}
		if _, ok := counts[status]; ok {
			counts[status] = count
		}
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("count tenants rows: %w", err)
	}
	for _, status := range allTenantStatuses {
		out.Statuses = append(out.Statuses, TenantStatusCount{Status: status, Count: counts[status]})
	}
	return out, nil
}

// ListTenantsByStatusAfter returns one keyset page of tenants after
// (afterCreatedAt, afterID), ordered by (created_at, id). Pass a zero
// afterCreatedAt and empty afterID to scan from the beginning.
func (s *Store) ListTenantsByStatusAfter(ctx context.Context, status TenantStatus, afterCreatedAt time.Time, afterID string, limit int) (out []Tenant, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_tenants_by_status_after", start, &err)
	if limit <= 0 {
		limit = 100
	}

	var rows *sql.Rows
	if afterCreatedAt.IsZero() && afterID == "" {
		rows, err = s.db.QueryContext(ctx, `SELECT id, status, kind, parent_tenant_id, storage_namespace_id,
			db_host, db_port, db_user, db_password, db_name,
			db_tls, provider, cluster_id, branch_id, claim_url, claim_expires_at, schema_version,
			s3_encryption_mode, s3_kms_key_id, s3_bucket_key_enabled, created_at, updated_at
			FROM tenants WHERE status = ? ORDER BY created_at ASC, id ASC LIMIT ?`, status, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, `SELECT id, status, kind, parent_tenant_id, storage_namespace_id,
			db_host, db_port, db_user, db_password, db_name,
			db_tls, provider, cluster_id, branch_id, claim_url, claim_expires_at, schema_version,
			s3_encryption_mode, s3_kms_key_id, s3_bucket_key_enabled, created_at, updated_at
			FROM tenants
			WHERE status = ? AND (created_at > ? OR (created_at = ? AND id > ?))
			ORDER BY created_at ASC, id ASC LIMIT ?`, status, afterCreatedAt.UTC(), afterCreatedAt.UTC(), afterID, limit)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	return scanTenantRows(rows)
}

func scanTenantRows(rows *sql.Rows) ([]Tenant, error) {
	out := make([]Tenant, 0)
	for rows.Next() {
		var t Tenant
		var dbTLS int
		var clusterID sql.NullString
		var parentTenantID sql.NullString
		var storageNamespaceID sql.NullString
		var claimURL sql.NullString
		var claimExp sql.NullTime
		var s3BucketKeyEnabled int
		if err := rows.Scan(&t.ID, &t.Status, &t.Kind, &parentTenantID, &storageNamespaceID,
			&t.DBHost, &t.DBPort, &t.DBUser, &t.DBPasswordCipher,
			&t.DBName, &dbTLS, &t.Provider, &clusterID, &t.BranchID, &claimURL, &claimExp, &t.SchemaVersion,
			&t.S3EncryptionMode, &t.S3KMSKeyID, &s3BucketKeyEnabled, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		t.DBTLS = dbTLS == 1
		t.S3BucketKeyEnabled = boolPtr(s3BucketKeyEnabled == 1)
		if clusterID.Valid {
			t.ClusterID = clusterID.String
		}
		if parentTenantID.Valid {
			t.ParentTenantID = parentTenantID.String
		}
		if storageNamespaceID.Valid {
			t.StorageNamespaceID = storageNamespaceID.String
		}
		if claimURL.Valid {
			t.ClaimURL = claimURL.String
		}
		if claimExp.Valid {
			ts := claimExp.Time.UTC()
			t.ClaimExpiresAt = &ts
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func scanTenantBindingRows(rows *sql.Rows) ([]TenantWithTiDBCloudOrgBinding, error) {
	out := make([]TenantWithTiDBCloudOrgBinding, 0)
	for rows.Next() {
		rec, err := scanTenantBindingScanner(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type tenantBindingScanner interface {
	Scan(dest ...any) error
}

func scanTenantBindingRow(row tenantBindingScanner) (*TenantWithTiDBCloudOrgBinding, error) {
	return scanTenantBindingScanner(row)
}

func scanTenantBindingScanner(row tenantBindingScanner) (*TenantWithTiDBCloudOrgBinding, error) {
	var rec TenantWithTiDBCloudOrgBinding
	var dbTLS int
	var clusterID sql.NullString
	var parentTenantID sql.NullString
	var storageNamespaceID sql.NullString
	var claimURL sql.NullString
	var claimExp sql.NullTime
	var s3BucketKeyEnabled int
	var usedAt sql.NullTime
	if err := row.Scan(&rec.Tenant.ID, &rec.Tenant.Status, &rec.Tenant.Kind, &parentTenantID, &storageNamespaceID,
		&rec.Tenant.DBHost, &rec.Tenant.DBPort, &rec.Tenant.DBUser, &rec.Tenant.DBPasswordCipher,
		&rec.Tenant.DBName, &dbTLS, &rec.Tenant.Provider, &clusterID, &rec.Tenant.BranchID, &claimURL, &claimExp, &rec.Tenant.SchemaVersion,
		&rec.Tenant.S3EncryptionMode, &rec.Tenant.S3KMSKeyID, &s3BucketKeyEnabled, &rec.Tenant.CreatedAt, &rec.Tenant.UpdatedAt,
		&rec.Binding.TenantID, &rec.Binding.OrganizationID, &rec.Binding.ClusterID, &rec.Binding.BranchID, &rec.Binding.PoolID, &rec.Binding.PoolStatus, &usedAt,
		&rec.Binding.CreatedAt, &rec.Binding.UpdatedAt); err != nil {
		return nil, err
	}
	rec.Tenant.DBTLS = dbTLS == 1
	rec.Tenant.S3BucketKeyEnabled = boolPtr(s3BucketKeyEnabled == 1)
	if clusterID.Valid {
		rec.Tenant.ClusterID = clusterID.String
	}
	if parentTenantID.Valid {
		rec.Tenant.ParentTenantID = parentTenantID.String
	}
	if storageNamespaceID.Valid {
		rec.Tenant.StorageNamespaceID = storageNamespaceID.String
	}
	if claimURL.Valid {
		rec.Tenant.ClaimURL = claimURL.String
	}
	if claimExp.Valid {
		ts := claimExp.Time.UTC()
		rec.Tenant.ClaimExpiresAt = &ts
	}
	if usedAt.Valid {
		ts := usedAt.Time.UTC()
		rec.Binding.UsedAt = &ts
	}
	return &rec, nil
}

func (s *Store) UpdateTenantStatus(ctx context.Context, id string, status TenantStatus) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tenant_status", start, &err)
	res, err := s.db.ExecContext(ctx, `UPDATE tenants SET status = ?, updated_at = ? WHERE id = ?`, status, time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) UpdateTenantStatusIf(ctx context.Context, id string, from, to TenantStatus) (updated bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tenant_status_if", start, &err)
	res, err := s.db.ExecContext(ctx, `UPDATE tenants SET status = ?, updated_at = ? WHERE id = ? AND status = ?`,
		to, time.Now().UTC(), id, from)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) UpdateTenantConnection(ctx context.Context, id string, cluster *Tenant) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tenant_connection", start, &err)
	if cluster == nil {
		return fmt.Errorf("tenant connection is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tenants
		SET db_host = ?, db_port = ?, db_user = ?, db_password = ?, db_name = ?, db_tls = ?,
			provider = ?, cluster_id = ?, branch_id = ?, claim_url = ?, claim_expires_at = ?, updated_at = ?
		WHERE id = ?`,
		cluster.DBHost, cluster.DBPort, cluster.DBUser, cluster.DBPasswordCipher, cluster.DBName, boolToInt(cluster.DBTLS),
		cluster.Provider, nullStr(cluster.ClusterID), cluster.BranchID, nullStr(cluster.ClaimURL), cluster.ClaimExpiresAt,
		time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) UpdateTenantClusterReference(ctx context.Context, id string, cluster *Tenant) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tenant_cluster_reference", start, &err)
	if cluster == nil {
		return fmt.Errorf("tenant cluster reference is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tenants
		SET db_host = ?, db_port = ?, db_user = ?, db_name = ?, db_tls = ?,
			provider = ?, cluster_id = ?, branch_id = ?, claim_url = ?, claim_expires_at = ?, updated_at = ?
		WHERE id = ?`,
		cluster.DBHost, cluster.DBPort, cluster.DBUser, cluster.DBName, boolToInt(cluster.DBTLS),
		cluster.Provider, nullStr(cluster.ClusterID), cluster.BranchID, nullStr(cluster.ClaimURL), cluster.ClaimExpiresAt,
		time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ClearTenantProvisionMetadata(ctx context.Context, tenantID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "clear_tenant_provision_metadata", start, &err)
	now := time.Now().UTC()
	return s.InTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE tenants
			SET db_host = '', db_port = 0, db_user = '', db_password = ?, db_name = '', db_tls = 1,
				cluster_id = NULL, branch_id = '', claim_url = NULL, claim_expires_at = NULL, updated_at = ?
			WHERE id = ?`,
			[]byte{}, now, tenantID)
		if err != nil {
			return err
		}
		if err := requireAffected(res); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM tenant_tidbcloud_org_bindings WHERE tenant_id = ?`, tenantID); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) UpdateTenantDBCredentialIf(ctx context.Context, id, fromDBUser, dbUser string, dbPasswordCipher []byte) (updated bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tenant_db_credential_if", start, &err)
	res, err := s.db.ExecContext(ctx, `UPDATE tenants SET db_user = ?, db_password = ?, updated_at = ? WHERE id = ? AND db_user = ?`,
		dbUser, dbPasswordCipher, time.Now().UTC(), id, fromDBUser)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) UpdateTenantBranch(ctx context.Context, id string, cluster *Tenant) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tenant_branch", start, &err)
	if cluster == nil {
		return fmt.Errorf("tenant branch is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tenants
		SET provider = ?, cluster_id = ?, branch_id = ?, claim_url = ?, claim_expires_at = ?, updated_at = ?
		WHERE id = ?`,
		cluster.Provider, nullStr(cluster.ClusterID), cluster.BranchID, nullStr(cluster.ClaimURL), cluster.ClaimExpiresAt,
		time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) RevokeTenantAPIKeys(ctx context.Context, tenantID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "revoke_tenant_api_keys", start, &err)
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `UPDATE tenant_api_keys
		SET status = ?, revoked_at = COALESCE(revoked_at, ?), updated_at = ?
		WHERE tenant_id = ? AND status = ?`,
		APIKeyRevoked, now, now, tenantID, APIKeyActive)
	return err
}

func (s *Store) RevokeAPIKey(ctx context.Context, tenantID, apiKeyID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "revoke_api_key", start, &err)
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE tenant_api_keys
		SET status = ?, revoked_at = COALESCE(revoked_at, ?), updated_at = ?
		WHERE tenant_id = ? AND id = ? AND status = ?`,
		APIKeyRevoked, now, now, tenantID, apiKeyID, APIKeyActive)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) RevokeAPIKeysByIssuer(ctx context.Context, tenantID, provider, subjectKey, exceptAPIKeyID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "revoke_api_keys_by_issuer", start, &err)
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `UPDATE tenant_api_keys
		SET status = ?, revoked_at = COALESCE(revoked_at, ?), updated_at = ?
		WHERE tenant_id = ? AND issued_by_provider = ? AND issued_by_subject_key = ? AND status = ?
			AND (? = '' OR id <> ?)`,
		APIKeyRevoked, now, now, tenantID, provider, subjectKey, APIKeyActive, exceptAPIKeyID, exceptAPIKeyID)
	return err
}

func (s *Store) GetActiveAPIKeyByIssuer(ctx context.Context, tenantID, provider, subjectKey string) (out *APIKey, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_active_api_key_by_issuer", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT
			id, tenant_id, key_name, jwt_ciphertext, jwt_hash, token_version, status, scope_kind,
			issued_by_provider, issued_by_subject_key, issued_by_metadata_json, issued_at,
			revoked_at, created_at, updated_at
		FROM tenant_api_keys
		WHERE tenant_id = ? AND issued_by_provider = ? AND issued_by_subject_key = ? AND status = ?
		ORDER BY created_at DESC
		LIMIT 1`, tenantID, provider, subjectKey, APIKeyActive)

	var rec APIKey
	var revokedAt sql.NullTime
	var issuedByProvider sql.NullString
	var issuedBySubjectKey sql.NullString
	var issuedByMetadataJSON []byte
	if err = row.Scan(
		&rec.ID, &rec.TenantID, &rec.KeyName, &rec.JWTCiphertext,
		&rec.JWTHash, &rec.TokenVersion, &rec.Status, &rec.ScopeKind,
		&issuedByProvider, &issuedBySubjectKey, &issuedByMetadataJSON, &rec.IssuedAt,
		&revokedAt, &rec.CreatedAt, &rec.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
			return nil, err
		}
		return nil, err
	}
	if revokedAt.Valid {
		t := revokedAt.Time.UTC()
		rec.RevokedAt = &t
	}
	if issuedByProvider.Valid {
		rec.IssuedByProvider = issuedByProvider.String
	}
	if issuedBySubjectKey.Valid {
		rec.IssuedBySubjectKey = issuedBySubjectKey.String
	}
	rec.IssuedByMetadataJSON = issuedByMetadataJSON
	out = &rec
	return out, nil
}

func (s *Store) UpsertStorageNamespace(ctx context.Context, ns *StorageNamespace) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "upsert_storage_namespace", start, &err)
	if ns == nil {
		return fmt.Errorf("storage namespace is required")
	}
	state := ns.State
	if state == "" {
		state = StorageNamespaceActive
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `INSERT INTO storage_namespaces
		(namespace_id, owner_tenant_id, backend, bucket, prefix, state, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			owner_tenant_id = VALUES(owner_tenant_id),
			backend = VALUES(backend),
			bucket = VALUES(bucket),
			prefix = VALUES(prefix),
			state = VALUES(state),
			updated_at = VALUES(updated_at)`,
		ns.ID, ns.OwnerTenantID, ns.Backend, ns.Bucket, ns.Prefix, state, now, now)
	return err
}

func (s *Store) GetStorageNamespace(ctx context.Context, id string) (out *StorageNamespace, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_storage_namespace", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT namespace_id, owner_tenant_id, backend, bucket, prefix, state, created_at, updated_at
		FROM storage_namespaces WHERE namespace_id = ?`, id)
	var ns StorageNamespace
	if err = row.Scan(&ns.ID, &ns.OwnerTenantID, &ns.Backend, &ns.Bucket, &ns.Prefix, &ns.State, &ns.CreatedAt, &ns.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
			return nil, err
		}
		return nil, err
	}
	return &ns, nil
}

func (s *Store) UpdateStorageNamespaceState(ctx context.Context, id string, state StorageNamespaceState) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_storage_namespace_state", start, &err)
	if id == "" {
		return fmt.Errorf("namespace id is required")
	}
	if state == "" {
		return fmt.Errorf("namespace state is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE storage_namespaces SET state = ?, updated_at = ? WHERE namespace_id = ?`,
		state, time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) EnsureTenantStorageNamespace(ctx context.Context, tenantID, backendName, bucket, prefix string) (out *StorageNamespace, err error) {
	start := time.Now()
	defer observeMeta(ctx, "ensure_tenant_storage_namespace", start, &err)
	if tenantID == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	if backendName == "" {
		return nil, fmt.Errorf("storage backend is required")
	}
	if prefix == "" {
		return nil, fmt.Errorf("storage prefix is required")
	}
	ns := &StorageNamespace{
		ID:            tenantID,
		OwnerTenantID: tenantID,
		Backend:       backendName,
		Bucket:        bucket,
		Prefix:        prefix,
		State:         StorageNamespaceActive,
	}
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, `SELECT storage_namespace_id FROM tenants WHERE id = ? FOR UPDATE`, tenantID)
		var existing sql.NullString
		if err := row.Scan(&existing); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if existing.Valid && existing.String != "" {
			existingNS, err := getStorageNamespaceTx(ctx, tx, existing.String)
			if err != nil {
				return err
			}
			ns = existingNS
			return nil
		}
		now := time.Now().UTC()
		if _, err := tx.ExecContext(ctx, `INSERT INTO storage_namespaces
			(namespace_id, owner_tenant_id, backend, bucket, prefix, state, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE updated_at = updated_at`,
			ns.ID, ns.OwnerTenantID, ns.Backend, ns.Bucket, ns.Prefix, ns.State, now, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tenants SET storage_namespace_id = ?, updated_at = ? WHERE id = ?`,
			ns.ID, now, tenantID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ns, nil
}

func (s *Store) EnqueueTenantDeleteJob(ctx context.Context, job *TenantDeleteJob) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "enqueue_tenant_delete_job", start, &err)
	if job == nil {
		return fmt.Errorf("tenant delete job is required")
	}
	if job.TenantID == "" {
		return fmt.Errorf("tenant id is required")
	}
	if job.NamespaceID == "" {
		return fmt.Errorf("namespace id is required")
	}
	if job.Backend == "" {
		return fmt.Errorf("storage backend is required")
	}
	if job.Prefix == "" {
		return fmt.Errorf("storage prefix is required")
	}
	notBefore := job.NotBefore.UTC()
	if notBefore.IsZero() {
		notBefore = time.Now().UTC()
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenant_delete_jobs
		(tenant_id, namespace_id, backend, bucket, prefix, state, not_before, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			namespace_id = VALUES(namespace_id),
			backend = VALUES(backend),
			bucket = VALUES(bucket),
			prefix = VALUES(prefix),
			state = CASE WHEN state = 'deleted' THEN state ELSE 'pending' END,
			not_before = LEAST(not_before, VALUES(not_before)),
			updated_at = VALUES(updated_at)`,
		job.TenantID, job.NamespaceID, job.Backend, job.Bucket, job.Prefix, TenantDeleteJobPending, notBefore, now, now)
	return err
}

func (s *Store) TenantDeleteJobExists(ctx context.Context, tenantID string) (out bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "tenant_delete_job_exists", start, &err)
	if tenantID == "" {
		return false, fmt.Errorf("tenant id is required")
	}
	row := s.db.QueryRowContext(ctx, `SELECT 1 FROM tenant_delete_jobs WHERE tenant_id = ? LIMIT 1`, tenantID)
	var one int
	if err := row.Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) ListDueTenantDeleteJobs(ctx context.Context, now time.Time, limit int) (out []TenantDeleteJob, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_due_tenant_delete_jobs", start, &err)
	if limit <= 0 {
		limit = 25
	}
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id, namespace_id, backend, bucket, prefix,
			state, attempts, COALESCE(last_error, ''), not_before, deleted_objects,
			aborted_multipart_uploads, created_at, updated_at, completed_at
		FROM tenant_delete_jobs
		WHERE state = ? AND not_before <= ?
		ORDER BY not_before ASC, created_at ASC
		LIMIT ?`, TenantDeleteJobPending, now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var job TenantDeleteJob
		var completedAt sql.NullTime
		if err := rows.Scan(&job.TenantID, &job.NamespaceID, &job.Backend, &job.Bucket, &job.Prefix,
			&job.State, &job.Attempts, &job.LastError, &job.NotBefore, &job.DeletedObjects,
			&job.AbortedMultipartUploads, &job.CreatedAt, &job.UpdatedAt, &completedAt); err != nil {
			return nil, err
		}
		if completedAt.Valid {
			t := completedAt.Time.UTC()
			job.CompletedAt = &t
		}
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) RecoverStaleTenantDeleteJobs(ctx context.Context, before time.Time) (n int64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "recover_stale_tenant_delete_jobs", start, &err)
	res, err := s.db.ExecContext(ctx, `UPDATE tenant_delete_jobs
		SET state = ?, not_before = ?, last_error = ?, updated_at = ?
		WHERE state = ? AND updated_at < ?`,
		TenantDeleteJobPending, time.Now().UTC(), "recovered stale running delete job", time.Now().UTC(),
		TenantDeleteJobRunning, before.UTC())
	if err != nil {
		return 0, err
	}
	n, _ = res.RowsAffected()
	return n, nil
}

func (s *Store) MarkTenantDeleteJobRunning(ctx context.Context, tenantID string) (updated bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "mark_tenant_delete_job_running", start, &err)
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE tenant_delete_jobs
		SET state = ?, updated_at = ?
		WHERE tenant_id = ? AND state = ? AND not_before <= ?`,
		TenantDeleteJobRunning, now, tenantID, TenantDeleteJobPending, now)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) RetryTenantDeleteJob(ctx context.Context, tenantID string, notBefore time.Time, lastError string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "retry_tenant_delete_job", start, &err)
	_, err = s.db.ExecContext(ctx, `UPDATE tenant_delete_jobs
		SET state = ?, not_before = ?, attempts = attempts + 1, last_error = ?, updated_at = ?
		WHERE tenant_id = ? AND state = ?`,
		TenantDeleteJobPending, notBefore.UTC(), nullStr(lastError), time.Now().UTC(), tenantID, TenantDeleteJobRunning)
	return err
}

func (s *Store) MarkTenantDeleted(ctx context.Context, tenantID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "mark_tenant_deleted", start, &err)
	now := time.Now().UTC()
	return s.InTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE tenants SET status = ?, updated_at = ? WHERE id = ?`,
			TenantDeleted, now, tenantID)
		if err != nil {
			return err
		}
		if err := requireAffected(res); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM tenant_tidbcloud_org_bindings WHERE tenant_id = ?`, tenantID); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) MarkTenantDeleteJobDeleted(ctx context.Context, tenantID string, deletedObjects, abortedMultipartUploads int64) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "mark_tenant_delete_job_deleted", start, &err)
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `UPDATE tenant_delete_jobs
		SET state = ?, deleted_objects = ?, aborted_multipart_uploads = ?, completed_at = ?,
			last_error = NULL, updated_at = ?
		WHERE tenant_id = ?`,
		TenantDeleteJobDeleted, deletedObjects, abortedMultipartUploads, now, now, tenantID)
	return err
}

func (s *Store) FinalizeTenantDelete(ctx context.Context, tenantID, namespaceID string, deletedObjects, abortedMultipartUploads int64) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "finalize_tenant_delete", start, &err)
	now := time.Now().UTC()
	return s.InTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE tenant_delete_jobs
			SET state = ?, deleted_objects = ?, aborted_multipart_uploads = ?, completed_at = ?,
				last_error = NULL, updated_at = ?
			WHERE tenant_id = ? AND state = ?`,
			TenantDeleteJobDeleted, deletedObjects, abortedMultipartUploads, now, now, tenantID, TenantDeleteJobRunning)
		if err != nil {
			return err
		}
		if err := requireAffected(res); err != nil {
			return err
		}
		if namespaceID != "" {
			res, err = tx.ExecContext(ctx, `UPDATE storage_namespaces SET state = ?, updated_at = ? WHERE namespace_id = ?`,
				StorageNamespaceDeleted, now, namespaceID)
			if err != nil {
				return err
			}
			if err := requireAffected(res); err != nil {
				return err
			}
		}
		res, err = tx.ExecContext(ctx, `UPDATE tenants SET status = ?, updated_at = ? WHERE id = ?`,
			TenantDeleted, now, tenantID)
		if err != nil {
			return err
		}
		if err := requireAffected(res); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM tenant_tidbcloud_org_bindings WHERE tenant_id = ?`, tenantID); err != nil {
			return err
		}
		return nil
	})
}

func requireAffected(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func getStorageNamespaceTx(ctx context.Context, tx *sql.Tx, id string) (*StorageNamespace, error) {
	row := tx.QueryRowContext(ctx, `SELECT namespace_id, owner_tenant_id, backend, bucket, prefix, state, created_at, updated_at
		FROM storage_namespaces WHERE namespace_id = ?`, id)
	var ns StorageNamespace
	if err := row.Scan(&ns.ID, &ns.OwnerTenantID, &ns.Backend, &ns.Bucket, &ns.Prefix, &ns.State, &ns.CreatedAt, &ns.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &ns, nil
}

type ObjectGCCandidateInput struct {
	NamespaceID    string
	StorageRef     string
	StorageRefHash string
	Reason         ObjectGCCandidateReason
	SourceTenantID string
	SourceFileID   string
	NotBefore      time.Time
}

type ObjectGCCandidate struct {
	NamespaceID    string
	StorageRef     string
	StorageRefHash string
	Reason         ObjectGCCandidateReason
	SourceTenantID string
	SourceFileID   string
	NotBefore      time.Time
	State          ObjectGCCandidateState
	Attempts       int
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (s *Store) EnqueueObjectGCCandidate(ctx context.Context, c *ObjectGCCandidateInput) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "enqueue_object_gc_candidate", start, &err)
	if c == nil {
		return fmt.Errorf("object gc candidate is required")
	}
	if c.NamespaceID == "" {
		return fmt.Errorf("namespace id is required")
	}
	if c.StorageRef == "" {
		return fmt.Errorf("storage ref is required")
	}
	if c.StorageRefHash == "" {
		return fmt.Errorf("storage ref hash is required")
	}
	if c.Reason == "" {
		return fmt.Errorf("object gc reason is required")
	}
	notBefore := c.NotBefore.UTC()
	if notBefore.IsZero() {
		notBefore = time.Now().UTC()
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `INSERT INTO object_gc_candidates
		(namespace_id, storage_ref, storage_ref_hash, reason, source_tenant_id, source_file_id, not_before, state, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			storage_ref = VALUES(storage_ref),
			reason = VALUES(reason),
			source_tenant_id = VALUES(source_tenant_id),
			source_file_id = VALUES(source_file_id),
			not_before = LEAST(not_before, VALUES(not_before)),
			state = CASE WHEN state = 'deleted' THEN state ELSE 'pending' END,
			updated_at = VALUES(updated_at)`,
		c.NamespaceID, c.StorageRef, c.StorageRefHash, c.Reason, c.SourceTenantID, c.SourceFileID,
		notBefore, ObjectGCCandidatePending, now, now)
	return err
}

func (s *Store) ListDueObjectGCCandidates(ctx context.Context, now time.Time, limit int) (out []ObjectGCCandidate, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_due_object_gc_candidates", start, &err)
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT namespace_id, storage_ref, storage_ref_hash, reason,
			source_tenant_id, source_file_id, not_before, state, attempts, COALESCE(last_error, ''),
			created_at, updated_at
		FROM object_gc_candidates
		WHERE state = ? AND not_before <= ?
		ORDER BY not_before ASC
		LIMIT ?`, ObjectGCCandidatePending, now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var c ObjectGCCandidate
		if err := rows.Scan(&c.NamespaceID, &c.StorageRef, &c.StorageRefHash, &c.Reason,
			&c.SourceTenantID, &c.SourceFileID, &c.NotBefore, &c.State, &c.Attempts, &c.LastError,
			&c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) NamespaceHasNonDeletedFork(ctx context.Context, namespaceID string) (out bool, err error) {
	start := time.Now()
	defer observeMeta(ctx, "namespace_has_non_deleted_fork", start, &err)
	if namespaceID == "" {
		return false, fmt.Errorf("namespace id is required")
	}
	row := s.db.QueryRowContext(ctx, `SELECT 1 FROM tenants
		WHERE storage_namespace_id = ? AND kind = ? AND status <> ?
		LIMIT 1`, namespaceID, TenantKindFork, TenantDeleted)
	var one int
	if err := row.Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) PostponeObjectGCCandidate(ctx context.Context, c ObjectGCCandidate, notBefore time.Time, lastError string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "postpone_object_gc_candidate", start, &err)
	_, err = s.db.ExecContext(ctx, `UPDATE object_gc_candidates
		SET not_before = ?, last_error = ?, updated_at = ?
		WHERE namespace_id = ? AND storage_ref_hash = ? AND storage_ref = ? AND state = ?`,
		notBefore.UTC(), nullStr(lastError), time.Now().UTC(),
		c.NamespaceID, c.StorageRefHash, c.StorageRef, ObjectGCCandidatePending)
	return err
}

func (s *Store) RetryObjectGCCandidate(ctx context.Context, c ObjectGCCandidate, notBefore time.Time, lastError string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "retry_object_gc_candidate", start, &err)
	_, err = s.db.ExecContext(ctx, `UPDATE object_gc_candidates
		SET not_before = ?, last_error = ?, attempts = attempts + 1, updated_at = ?
		WHERE namespace_id = ? AND storage_ref_hash = ? AND storage_ref = ? AND state = ?`,
		notBefore.UTC(), nullStr(lastError), time.Now().UTC(),
		c.NamespaceID, c.StorageRefHash, c.StorageRef, ObjectGCCandidatePending)
	return err
}

func (s *Store) MarkObjectGCCandidateDeleted(ctx context.Context, c ObjectGCCandidate) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "mark_object_gc_candidate_deleted", start, &err)
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `UPDATE object_gc_candidates
		SET state = ?, deleted_at = ?, updated_at = ?, last_error = NULL
		WHERE namespace_id = ? AND storage_ref_hash = ? AND storage_ref = ?`,
		ObjectGCCandidateDeleted, now, now, c.NamespaceID, c.StorageRefHash, c.StorageRef)
	return err
}

// UpdateTenantSchemaVersion records the tenant DB schema version after a
// successful ensure/repair cycle.  Callers should treat failures as best-
// effort: log the error but do not fail the overall operation.
func (s *Store) UpdateTenantSchemaVersion(ctx context.Context, id string, version int) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_tenant_schema_version", start, &err)
	res, err := s.db.ExecContext(ctx,
		`UPDATE tenants SET schema_version = ?, updated_at = ? WHERE id = ?`,
		version, time.Now().UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func observeMeta(ctx context.Context, op string, start time.Time, errp *error) {
	result := "ok"
	if errp != nil && *errp != nil {
		switch {
		case errors.Is(*errp, ErrNotFound), errors.Is(*errp, sql.ErrNoRows):
			result = "not_found"
		case errors.Is(*errp, ErrDuplicate):
			result = "duplicate"
		case errors.Is(*errp, sql.ErrConnDone):
			// Connection closed during shutdown — not an unexpected failure.
			result = "conn_closed"
		default:
			if strings.Contains((*errp).Error(), "database is closed") {
				result = "conn_closed"
			} else {
				result = "error"
			}
		}
		switch result {
		case "conn_closed":
			// Connection closed during shutdown — suppress the noisy log and
			// only record the metric below.
		case "not_found", "duplicate":
			logger.Warn(ctx, "meta_op_failed", zap.String("operation", op), zap.String("result", result), zap.String("detail", (*errp).Error()))
		case "error":
			logger.Error(ctx, "meta_op_failed", zap.String("operation", op), zap.String("result", result), zap.Error(*errp))
		}
	}
	metrics.RecordOperation("meta", op, result, time.Since(start))
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func tenantKindForInsert(t *Tenant) TenantKind {
	if t.Kind == "" {
		return TenantKindLive
	}
	return t.Kind
}

func tenantS3EncryptionModeForInsert(t *Tenant) S3EncryptionMode {
	if t.S3EncryptionMode == "" {
		return S3EncryptionModeInherit
	}
	return t.S3EncryptionMode
}

func tenantS3BucketKeyEnabledForInsert(t *Tenant) bool {
	if t.S3BucketKeyEnabled == nil {
		return true
	}
	return *t.S3BucketKeyEnabled
}

func tenantPoolBindingStatusForInsert(status TenantPoolBindingStatus) TenantPoolBindingStatus {
	if status == "" {
		return TenantPoolBindingUsed
	}
	return status
}

func tenantPoolStatusForInsert(status TenantPoolStatus) TenantPoolStatus {
	if status == "" {
		return TenantPoolActive
	}
	return status
}

func scanTenantPoolRow(row tenantBindingScanner) (*TenantPool, error) {
	var rec TenantPool
	var orgID sql.NullString
	if err := row.Scan(&rec.PoolID, &orgID, &rec.Size, &rec.Status, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if orgID.Valid {
		rec.OrganizationID = orgID.String
	}
	return &rec, nil
}

func boolPtr(v bool) *bool {
	return &v
}

func nullStr(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nullableBytes(v []byte) any {
	if len(v) == 0 {
		return nil
	}
	return v
}

func isDuplicateEntry(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Duplicate entry") || strings.Contains(msg, "UNIQUE constraint failed")
}

// ---------------------------------------------------------------------------
// SSE notify outbox (deprecated — replaced by tenant_notify_outbox)
// ---------------------------------------------------------------------------

// SSENotifyRow mirrors a row in sse_notify_outbox.
//
// Deprecated: replaced by tenant_notify_outbox + TenantNotifyRow. The
// sse_notify_outbox table is no longer written to by production code; it is
// retained in the schema for migration safety and will be dropped in a future
// PR.
type SSENotifyRow struct {
	ID        uint64
	TenantID  string
	Seq       uint64
	CreatedAt time.Time
}

// PodRegistryStatus is the lifecycle state of a pod in pod_registry.
type PodRegistryStatus string

const (
	PodRegistryActive PodRegistryStatus = "active"
	PodRegistryStale  PodRegistryStatus = "stale"
)

// PodRow mirrors a row in pod_registry.
type PodRow struct {
	PodID  string
	Addr   string
	Status PodRegistryStatus
}

// InsertSSENotify writes a notification pointer to sse_notify_outbox.
//
// Deprecated: replaced by InsertTenantNotify. Retained for migration safety;
// production code no longer calls this. The sse_notify_outbox table will be
// dropped in a future PR.
func (s *Store) InsertSSENotify(ctx context.Context, tenantID string, seq uint64) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_sse_notify", start, &err)
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO sse_notify_outbox (tenant_id, seq) VALUES (?, ?)`,
		tenantID, seq)
	return err
}

// ListSSENotifySince returns outbox rows with id > afterID, ordered by id, up to
// limit.
//
// Deprecated: replaced by ListTenantNotifySince.
func (s *Store) ListSSENotifySince(ctx context.Context, afterID uint64, limit int) (out []SSENotifyRow, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_sse_notify_since", start, &err)
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, seq, created_at FROM sse_notify_outbox WHERE id > ? ORDER BY id LIMIT ?`,
		afterID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var rec SSENotifyRow
		if err = rows.Scan(&rec.ID, &rec.TenantID, &rec.Seq, &rec.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// MaxSSENotifyID returns the current maximum id in sse_notify_outbox, or 0 if
// the table is empty.
//
// Deprecated: replaced by MaxTenantNotifyID.
func (s *Store) MaxSSENotifyID(ctx context.Context) (out uint64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "max_sse_notify_id", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id), 0) FROM sse_notify_outbox`)
	if err = row.Scan(&out); err != nil {
		return 0, err
	}
	return out, nil
}

// DeleteSSENotifyBefore prunes outbox rows older than the given threshold.
//
// Deprecated: replaced by DeleteTenantNotifyBefore.
func (s *Store) DeleteSSENotifyBefore(ctx context.Context, before time.Time) (n int64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_sse_notify_before", start, &err)
	res, err := s.db.ExecContext(ctx, `DELETE FROM sse_notify_outbox WHERE created_at < ?`, before)
	if err != nil {
		return 0, err
	}
	n, err = res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// Pod registry
// ---------------------------------------------------------------------------

// UpsertPod registers or refreshes this pod's heartbeat in pod_registry. On
// first insert the row is created with status='active'; on subsequent calls
// the addr, last_heartbeat, and status are updated (reviving a stale pod).
func (s *Store) UpsertPod(ctx context.Context, podID, addr string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "upsert_pod", start, &err)
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO pod_registry (pod_id, addr, last_heartbeat, status)
		 VALUES (?, ?, NOW(3), 'active')
		 ON DUPLICATE KEY UPDATE addr = VALUES(addr), last_heartbeat = NOW(3), status = 'active'`,
		podID, addr)
	return err
}

// ListActivePods returns all active pods excluding selfID. Used by the pod
// notifier to build its peer list for cross-pod HTTP push.
func (s *Store) ListActivePods(ctx context.Context, selfID string) (out []PodRow, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_active_pods", start, &err)
	rows, err := s.db.QueryContext(ctx,
		`SELECT pod_id, addr, status FROM pod_registry WHERE status = 'active' AND pod_id != ?`,
		selfID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var rec PodRow
		if err = rows.Scan(&rec.PodID, &rec.Addr, &rec.Status); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// MarkStalePods marks pods with heartbeats older than the threshold as stale.
// Leader-gated: prevents writers from pushing notifications to dead pods.
func (s *Store) MarkStalePods(ctx context.Context, before time.Time) (n int64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "mark_stale_pods", start, &err)
	res, err := s.db.ExecContext(ctx,
		`UPDATE pod_registry SET status = 'stale' WHERE last_heartbeat < ? AND status = 'active'`,
		before)
	if err != nil {
		return 0, err
	}
	n, err = res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// DeletePod removes a pod and its subscriptions from the registry. Used when a
// pod is decommissioned. Best-effort: stale pod cleanup handles the common case.
// DeletePod removes a pod and its subscriptions from the registry. Both
// deletions run in a single transaction so a partial failure doesn't leave
// orphaned subscription rows. Used when a pod is decommissioned.
func (s *Store) DeletePod(ctx context.Context, podID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_pod", start, &err)
	return s.InTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM pod_subscriptions WHERE pod_id = ?`, podID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM pod_registry WHERE pod_id = ?`, podID)
		return err
	})
}

// ---------------------------------------------------------------------------
// Pod subscriptions
// ---------------------------------------------------------------------------

// UpsertPodSubscriptions records the set of tenant IDs for which podID has
// active SSE subscribers. Each (pod_id, tenant_id) pair is upserted with the
// current timestamp. Callers should call PrunePodSubscriptions to remove
// tenants that are no longer active.
func (s *Store) UpsertPodSubscriptions(ctx context.Context, podID string, tenantIDs []string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "upsert_pod_subscriptions", start, &err)
	if len(tenantIDs) == 0 {
		return nil
	}
	return s.InTx(ctx, func(tx *sql.Tx) error {
		for _, tenantID := range tenantIDs {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO pod_subscriptions (pod_id, tenant_id)
				 VALUES (?, ?)
				 ON DUPLICATE KEY UPDATE updated_at = NOW(3)`,
				podID, tenantID); err != nil {
				return err
			}
		}
		return nil
	})
}

// PrunePodSubscriptions removes pod_subscriptions rows for podID whose
// tenant_id is not in keepSet. This lets a pod prune tenants whose SSE
// subscribers have all disconnected. keepSet may be empty (prunes all).
func (s *Store) PrunePodSubscriptions(ctx context.Context, podID string, keepSet []string) (n int64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "prune_pod_subscriptions", start, &err)
	if len(keepSet) == 0 {
		res, err := s.db.ExecContext(ctx,
			`DELETE FROM pod_subscriptions WHERE pod_id = ?`, podID)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	}
	// Build a NOT IN clause with placeholders. keepSet is bounded by the number
	// of tenants with active SSE connections on this pod (typically small).
	placeholders := make([]string, len(keepSet))
	args := make([]any, 0, len(keepSet)+1)
	args = append(args, podID)
	for i, t := range keepSet {
		placeholders[i] = "?"
		args = append(args, t)
	}
	query := fmt.Sprintf(
		`DELETE FROM pod_subscriptions WHERE pod_id = ? AND tenant_id NOT IN (%s)`,
		strings.Join(placeholders, ","))
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ListPodSubscriptions returns the tenant IDs for which podID has active SSE
// subscribers. Used by the route cache builder to compute which peers care
// about each tenant.
func (s *Store) ListPodSubscriptions(ctx context.Context, podID string) (out []string, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_pod_subscriptions", start, &err)
	rows, err := s.db.QueryContext(ctx,
		`SELECT tenant_id FROM pod_subscriptions WHERE pod_id = ?`, podID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var tenantID string
		if err = rows.Scan(&tenantID); err != nil {
			return nil, err
		}
		out = append(out, tenantID)
	}
	return out, rows.Err()
}

// ListAllPodSubscriptions returns all (pod_id, tenant_id) pairs from
// pod_subscriptions. Used by the route cache builder to construct a
// tenant→pods reverse index for push notification routing.
func (s *Store) ListAllPodSubscriptions(ctx context.Context) (out []PodSubscriptionRow, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_all_pod_subscriptions", start, &err)
	rows, err := s.db.QueryContext(ctx,
		`SELECT pod_id, tenant_id FROM pod_subscriptions`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var rec PodSubscriptionRow
		if err = rows.Scan(&rec.PodID, &rec.TenantID); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// PodSubscriptionRow mirrors a row in pod_subscriptions.
type PodSubscriptionRow struct {
	PodID    string
	TenantID string
}

// DeletePodSubscriptions removes all subscription rows for podID. Called when
// a pod is decommissioned or marked stale by the leader sweeper.
func (s *Store) DeletePodSubscriptions(ctx context.Context, podID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_pod_subscriptions", start, &err)
	_, err = s.db.ExecContext(ctx, `DELETE FROM pod_subscriptions WHERE pod_id = ?`, podID)
	return err
}

// ListStalePods returns pod IDs with status='stale'. Used by the leader sweeper
// to clean up subscriptions for dead pods.
func (s *Store) ListStalePods(ctx context.Context) (out []string, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_stale_pods", start, &err)
	rows, err := s.db.QueryContext(ctx,
		`SELECT pod_id FROM pod_registry WHERE status = 'stale'`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var podID string
		if err = rows.Scan(&podID); err != nil {
			return nil, err
		}
		out = append(out, podID)
	}
	return out, rows.Err()
}

// DeleteSubscriptionsForStalePods deletes pod_subscriptions rows whose pod is
// currently stale, using a subquery join so the stale check and the delete are
// atomic. This avoids a TOCTOU race where a pod recovers to active between
// ListStalePods and DeletePodSubscriptions, which would delete subscriptions for
// a live pod. Returns the number of deleted rows.
func (s *Store) DeleteSubscriptionsForStalePods(ctx context.Context) (n int64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_subscriptions_for_stale_pods", start, &err)
	res, err := s.db.ExecContext(ctx,
		`DELETE ps FROM pod_subscriptions ps
		 INNER JOIN pod_registry pr ON ps.pod_id = pr.pod_id
		 WHERE pr.status = 'stale'`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ---------------------------------------------------------------------------
// Unified tenant notify outbox
// ---------------------------------------------------------------------------

// TenantNotifyRow mirrors a row in tenant_notify_outbox. Each row is a
// lightweight work signal: tenant_id identifies the tenant and work_mask is a
// bitmask of work types to dispatch (SSE, semantic, file_gc, quota).
type TenantNotifyRow struct {
	ID             uint64
	TenantID       string
	TiDBCloudOrgID string
	WorkMask       int
	CreatedAt      time.Time
}

// InsertTenantNotify writes a unified work signal to tenant_notify_outbox.
// This is best-effort: if the INSERT fails, the work is still durable in the
// per-tenant TiDB and will be recovered by the safety-net scan. The central
// meta DB is always provisioned so this write never wakes a serverless tenant.
func (s *Store) InsertTenantNotify(ctx context.Context, tenantID string, workMask int) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_tenant_notify", start, &err)
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_notify_outbox (tenant_id, work_mask) VALUES (?, ?)`,
		tenantID, workMask)
	return err
}

// ListTenantNotifySince returns outbox rows with id > afterID, ordered by id,
// up to limit. The consumer advances its cursor to the last row's id so
// subsequent calls do not re-read the same rows.
func (s *Store) ListTenantNotifySince(ctx context.Context, afterID uint64, limit int) (out []TenantNotifyRow, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_tenant_notify_since", start, &err)
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT o.id, o.tenant_id, COALESCE(b.organization_id, ''), o.work_mask, o.created_at
		 FROM tenant_notify_outbox o
		 LEFT JOIN tenant_tidbcloud_org_bindings b ON b.tenant_id = o.tenant_id
		 WHERE o.id > ?
		 ORDER BY o.id
		 LIMIT ?`,
		afterID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var rec TenantNotifyRow
		if err = rows.Scan(&rec.ID, &rec.TenantID, &rec.TiDBCloudOrgID, &rec.WorkMask, &rec.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// MaxTenantNotifyID returns the current maximum id in tenant_notify_outbox, or
// 0 if the table is empty. Used by a pod on first launch to skip historical rows
// (the pod never owned work before its first start).
func (s *Store) MaxTenantNotifyID(ctx context.Context) (out uint64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "max_tenant_notify_id", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id), 0) FROM tenant_notify_outbox`)
	if err = row.Scan(&out); err != nil {
		return 0, err
	}
	return out, nil
}

// DeleteTenantNotifyBefore prunes outbox rows older than the given threshold.
// Leader-gated; retention is relative to DB insert time (created_at). Rows are
// only pruned up to the minimum last_id across all pods' cursors so a lagging
// pod that has not yet processed a row does not lose its work signal. When no
// cursors exist (e.g. before any pod has registered), falls back to age-only
// pruning.
func (s *Store) DeleteTenantNotifyBefore(ctx context.Context, before time.Time) (n int64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_tenant_notify_before", start, &err)
	res, err := s.db.ExecContext(ctx, `DELETE FROM tenant_notify_outbox
WHERE created_at < ?
AND (
  NOT EXISTS (SELECT 1 FROM tenant_outbox_cursor)
  OR id <= (SELECT MIN(last_id) FROM tenant_outbox_cursor)
)`, before)
	if err != nil {
		return 0, err
	}
	n, err = res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// Tenant outbox cursor
// ---------------------------------------------------------------------------

// TenantOutboxCursorRow mirrors a row in tenant_outbox_cursor.
type TenantOutboxCursorRow struct {
	PodID     string
	LastID    uint64
	UpdatedAt time.Time
}

// GetTenantOutboxCursor returns the persisted cursor (last_id) for podID, or
// ErrNotFound if no row exists (first launch). The pod uses this on restart to
// resume processing without skipping work.
func (s *Store) GetTenantOutboxCursor(ctx context.Context, podID string) (out *TenantOutboxCursorRow, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_tenant_outbox_cursor", start, &err)
	row := s.db.QueryRowContext(ctx,
		`SELECT pod_id, last_id, updated_at FROM tenant_outbox_cursor WHERE pod_id = ?`, podID)
	var rec TenantOutboxCursorRow
	if err = row.Scan(&rec.PodID, &rec.LastID, &rec.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &rec, nil
}

// UpsertTenantOutboxCursor persists (or refreshes) the cursor for podID. The
// poller flushes its in-memory last_id every few seconds so a restart resumes
// from the last processed row.
func (s *Store) UpsertTenantOutboxCursor(ctx context.Context, podID string, lastID uint64) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "upsert_tenant_outbox_cursor", start, &err)
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenant_outbox_cursor (pod_id, last_id) VALUES (?, ?)
		 ON DUPLICATE KEY UPDATE last_id = VALUES(last_id), updated_at = CURRENT_TIMESTAMP(3)`,
		podID, lastID)
	return err
}

// DeleteTenantOutboxCursor removes the cursor row for podID. Used when a pod
// is decommissioned so its cursor doesn't linger.
// DeleteTenantOutboxCursor deletes the cursor row for the given pod, but only
// if the pod is currently stale in pod_registry. This prevents a TOCTOU race
// where a pod recovers between ListStalePods and the cursor delete — a
// recovered pod retains its cursor so its last_id is not lost. The conditional
// join mirrors DeleteSubscriptionsForStalePods.
func (s *Store) DeleteTenantOutboxCursor(ctx context.Context, podID string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_tenant_outbox_cursor", start, &err)
	_, err = s.db.ExecContext(ctx,
		`DELETE FROM tenant_outbox_cursor WHERE pod_id = ?
		 AND EXISTS (SELECT 1 FROM pod_registry WHERE pod_id = ? AND status = 'stale')`,
		podID, podID)
	return err
}

// ListAllActivePodIDs returns the pod_id of every active pod in pod_registry,
// ordered by pod_id. Used by the shard resolver to build the active pod ring.
func (s *Store) ListAllActivePodIDs(ctx context.Context) (out []string, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_all_active_pod_ids", start, &err)
	rows, err := s.db.QueryContext(ctx,
		`SELECT pod_id FROM pod_registry WHERE status = 'active' ORDER BY pod_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var podID string
		if err = rows.Scan(&podID); err != nil {
			return nil, err
		}
		out = append(out, podID)
	}
	return out, rows.Err()
}
