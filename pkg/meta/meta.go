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

	"github.com/mem9-ai/dat9/internal/schemaspec"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/mysqlutil"
	"github.com/mem9-ai/dat9/pkg/pathutil"
	"go.uber.org/zap"
)

var (
	ErrNotFound                 = errors.New("not found")
	ErrDuplicate                = errors.New("duplicate entry")
	ErrStorageQuotaExceeded     = errors.New("tenant storage quota exceeded")
	ErrReservationAlreadyExists = errors.New("upload reservation already exists")
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

type StorageNamespaceState string

const (
	StorageNamespaceActive  StorageNamespaceState = "active"
	StorageNamespaceDeleted StorageNamespaceState = "deleted"
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
	Tenant Tenant
	APIKey APIKey
}

type ExternalBinding struct {
	Provider     string
	SubjectKey   string
	TenantID     string
	MetadataJSON []byte
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	if strings.Contains(dsn, "multiStatements=true") {
		return nil, fmt.Errorf("multiStatements=true is not allowed in production DSN")
	}
	db, err := mysqlutil.OpenInstrumented(context.Background(), dsn, mysqlutil.RoleMeta)
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
const externalBindingLockTimeoutSeconds = 30

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
			INDEX idx_api_keys_tenant (tenant_id, status)
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
			tenant_id       VARCHAR(64) NOT NULL,
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
			max_media_llm_files   BIGINT NOT NULL DEFAULT 500,
			max_monthly_cost_mc   BIGINT NOT NULL DEFAULT 0,
			created_at            DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at            DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_quota_usage (
			tenant_id          VARCHAR(64) PRIMARY KEY,
			storage_bytes      BIGINT NOT NULL DEFAULT 0,
			reserved_bytes     BIGINT NOT NULL DEFAULT 0,
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
			INDEX idx_tenant_order (tenant_id, id)
		)`,
	}
}

func dropObsoleteMetaIndexes(ctx context.Context, db *sql.DB) error {
	if err := dropMetaIndexIfExists(ctx, db, "tenant_api_keys", "idx_api_keys_tenant_name"); err != nil {
		return fmt.Errorf("drop obsolete meta index idx_api_keys_tenant_name: %w", err)
	}
	return nil
}

func dropMetaIndexIfExists(ctx context.Context, db *sql.DB, tableName, indexName string) error {
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
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, externalBindingLockTimeoutSeconds).Scan(&got); err != nil {
		return err
	}
	if !got.Valid {
		return fmt.Errorf("external binding named lock returned NULL")
	}
	if got.Int64 != 1 {
		return fmt.Errorf("timed out waiting for external binding named lock")
	}
	defer func() {
		var released sql.NullInt64
		releaseErr := conn.QueryRowContext(context.Background(), "SELECT RELEASE_LOCK(?)", lockName).Scan(&released)
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

func externalBindingLockName(provider, subjectKey string) string {
	sum := sha256.Sum256([]byte(provider + "\x00" + subjectKey))
	return "d9_extbind:" + hex.EncodeToString(sum[:26])
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
			k.revoked_at, k.created_at, k.updated_at
		FROM tenant_api_keys k
		JOIN tenants t ON t.id = k.tenant_id
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
		&revokedAt, &rec.APIKey.CreatedAt, &rec.APIKey.UpdatedAt,
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
		case errors.Is(*errp, ErrNotFound):
			result = "not_found"
		case errors.Is(*errp, ErrDuplicate):
			result = "duplicate"
		default:
			result = "error"
		}
		logger.Error(ctx, "meta_op_failed", zap.String("operation", op), zap.String("result", result), zap.Error(*errp))
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
