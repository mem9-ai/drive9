// Package meta provides control-plane metadata storage for multi-tenant auth.
package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/mem9-ai/dat9/internal/schemaspec"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/mysqlutil"
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
	TenantProvisioning TenantStatus = "provisioning"
	TenantActive       TenantStatus = "active"
	TenantFailed       TenantStatus = "failed"
	TenantSuspended    TenantStatus = "suspended"
	TenantDeleted      TenantStatus = "deleted"
)

type APIKeyStatus string

const (
	APIKeyActive  APIKeyStatus = "active"
	APIKeyRevoked APIKeyStatus = "revoked"
)

type Tenant struct {
	ID               string
	Status           TenantStatus
	DBHost           string
	DBPort           int
	DBUser           string
	DBPasswordCipher []byte
	DBName           string
	DBTLS            bool
	Provider         string
	ClusterID        string
	ClaimURL         string
	ClaimExpiresAt   *time.Time
	SchemaVersion    int
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type APIKey struct {
	ID            string
	TenantID      string
	KeyName       string
	JWTCiphertext []byte
	JWTHash       string
	TokenVersion  int
	Status        APIKeyStatus
	IssuedAt      time.Time
	RevokedAt     *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type TenantWithAPIKey struct {
	Tenant Tenant
	APIKey APIKey
}

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	if strings.Contains(dsn, "multiStatements=true") {
		return nil, fmt.Errorf("multiStatements=true is not allowed in production DSN")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	applyMySQLPoolDefaults(db)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) DB() *sql.DB  { return s.db }

func applyMySQLPoolDefaults(db *sql.DB) {
	mysqlutil.ApplyPoolDefaults(db)
}

const metaSchemaMigrateLockNamePrefix = "dat9_meta_schema_migrate:"
const metaSchemaMigrateLockTimeoutSeconds = 30

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
			db_host          VARCHAR(255) NOT NULL,
			db_port          INT NOT NULL,
			db_user          VARCHAR(255) NOT NULL,
			db_password      VARBINARY(2048) NOT NULL,
			db_name          VARCHAR(255) NOT NULL,
			db_tls           TINYINT(1) NOT NULL DEFAULT 1,
			provider         VARCHAR(50) NOT NULL,
			cluster_id       VARCHAR(255) NULL,
			claim_url        TEXT NULL,
			claim_expires_at DATETIME(3) NULL,
			schema_version   INT UNSIGNED NOT NULL DEFAULT 1,
			created_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			deleted_at       DATETIME(3) NULL,
			INDEX idx_tenant_status (status),
			INDEX idx_tenant_provider (provider)
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_api_keys (
			id             VARCHAR(64) PRIMARY KEY,
			tenant_id      VARCHAR(64) NOT NULL,
			key_name       VARCHAR(64) NOT NULL DEFAULT 'default',
			jwt_ciphertext VARBINARY(4096) NOT NULL,
			jwt_hash       VARCHAR(128) NOT NULL,
			token_version  INT NOT NULL DEFAULT 1,
			status         VARCHAR(20) NOT NULL DEFAULT 'active',
			issued_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			revoked_at     DATETIME(3) NULL,
			created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			UNIQUE INDEX idx_api_keys_hash (jwt_hash),
			INDEX idx_api_keys_tenant (tenant_id, status),
			UNIQUE INDEX idx_api_keys_tenant_name (tenant_id, key_name)
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
		if normalizeMetaSQLFragment(col.columnType) != normalizeMetaSQLFragment(spec.columnType) {
			diffs = append(diffs, metaSchemaDiff{
				kind:       metaSchemaDiffColumnType,
				tableName:  table.name,
				columnName: name,
				detail:     fmt.Sprintf("%s schema contract: %s column type = %q, want %s", table.name, name, col.columnType, spec.columnType),
				repairSQL:  spec.modifySQL,
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
		return isSafeModifyColumnRepairSQL(diff.repairSQL)
	default:
		return false
	}
}

func isSafeAddColumnRepairSQL(sqlText string) bool {
	return schemaspec.IsSafeAddColumnRepairSQL(sqlText)
}

// isSafeModifyColumnRepairSQL returns true for MODIFY COLUMN statements that
// only widen a column type without data loss (e.g. INT → INT UNSIGNED).
func isSafeModifyColumnRepairSQL(sqlText string) bool {
	n := normalizeMetaSQLFragment(sqlText)
	// Only allow: ALTER TABLE <t> MODIFY COLUMN <col> INT UNSIGNED ...
	// This covers the schema_version INT → INT UNSIGNED upgrade path.
	return strings.Contains(n, " modify column ") && strings.Contains(n, " int unsigned ")
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
		(id, status, db_host, db_port, db_user, db_password, db_name, db_tls,
		 provider, cluster_id, claim_url, claim_expires_at, schema_version, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ID, t.Status, t.DBHost, t.DBPort, t.DBUser, t.DBPasswordCipher, t.DBName, boolToInt(t.DBTLS),
		t.Provider, nullStr(t.ClusterID), nullStr(t.ClaimURL), t.ClaimExpiresAt, t.SchemaVersion, t.CreatedAt.UTC(), t.UpdatedAt.UTC())
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) InsertAPIKey(ctx context.Context, k *APIKey) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_api_key", start, &err)
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenant_api_keys
		(id, tenant_id, key_name, jwt_ciphertext, jwt_hash, token_version, status, issued_at, revoked_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		k.ID, k.TenantID, k.KeyName, k.JWTCiphertext, k.JWTHash, k.TokenVersion, k.Status,
		k.IssuedAt.UTC(), k.RevokedAt, k.CreatedAt.UTC(), k.UpdatedAt.UTC())
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) ResolveByAPIKeyHash(ctx context.Context, hash string) (out *TenantWithAPIKey, err error) {
	start := time.Now()
	defer observeMeta(ctx, "resolve_api_key_hash", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT
			t.id, t.status, t.db_host, t.db_port, t.db_user, t.db_password, t.db_name, t.db_tls,
			t.provider, t.cluster_id, t.claim_url, t.claim_expires_at, t.schema_version, t.created_at, t.updated_at,
			k.id, k.tenant_id, k.key_name, k.jwt_ciphertext, k.jwt_hash, k.token_version, k.status, k.issued_at,
			k.revoked_at, k.created_at, k.updated_at
		FROM tenant_api_keys k
		JOIN tenants t ON t.id = k.tenant_id
		WHERE k.jwt_hash = ?`, hash)

	var rec TenantWithAPIKey
	var dbTLS int
	var claimURL sql.NullString
	var claimExp sql.NullTime
	var clusterID sql.NullString
	var revokedAt sql.NullTime
	if err = row.Scan(
		&rec.Tenant.ID, &rec.Tenant.Status, &rec.Tenant.DBHost, &rec.Tenant.DBPort, &rec.Tenant.DBUser,
		&rec.Tenant.DBPasswordCipher, &rec.Tenant.DBName, &dbTLS, &rec.Tenant.Provider, &clusterID,
		&claimURL, &claimExp, &rec.Tenant.SchemaVersion, &rec.Tenant.CreatedAt, &rec.Tenant.UpdatedAt,
		&rec.APIKey.ID, &rec.APIKey.TenantID, &rec.APIKey.KeyName, &rec.APIKey.JWTCiphertext,
		&rec.APIKey.JWTHash, &rec.APIKey.TokenVersion, &rec.APIKey.Status, &rec.APIKey.IssuedAt,
		&revokedAt, &rec.APIKey.CreatedAt, &rec.APIKey.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
			return nil, err
		}
		return nil, err
	}
	rec.Tenant.DBTLS = dbTLS == 1
	if clusterID.Valid {
		rec.Tenant.ClusterID = clusterID.String
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
	out = &rec
	return out, nil
}

func (s *Store) GetTenant(ctx context.Context, id string) (out *Tenant, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_tenant", start, &err)
	row := s.db.QueryRowContext(ctx, `SELECT id, status, db_host, db_port, db_user, db_password, db_name,
		db_tls, provider, cluster_id, claim_url, claim_expires_at, schema_version, created_at, updated_at
		FROM tenants WHERE id = ?`, id)
	var dbTLS int
	var clusterID sql.NullString
	var claimURL sql.NullString
	var claimExp sql.NullTime
	var rec Tenant
	if err = row.Scan(&rec.ID, &rec.Status, &rec.DBHost, &rec.DBPort, &rec.DBUser, &rec.DBPasswordCipher,
		&rec.DBName, &dbTLS, &rec.Provider, &clusterID, &claimURL, &claimExp, &rec.SchemaVersion,
		&rec.CreatedAt, &rec.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrNotFound
			return nil, err
		}
		return nil, err
	}
	rec.DBTLS = dbTLS == 1
	if clusterID.Valid {
		rec.ClusterID = clusterID.String
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
	rows, err := s.db.QueryContext(ctx, `SELECT id, status, db_host, db_port, db_user, db_password, db_name,
		db_tls, provider, cluster_id, claim_url, claim_expires_at, schema_version, created_at, updated_at
		FROM tenants WHERE status = ? ORDER BY created_at ASC LIMIT ?`, status, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out = make([]Tenant, 0)
	for rows.Next() {
		var t Tenant
		var dbTLS int
		var clusterID sql.NullString
		var claimURL sql.NullString
		var claimExp sql.NullTime
		if err := rows.Scan(&t.ID, &t.Status, &t.DBHost, &t.DBPort, &t.DBUser, &t.DBPasswordCipher,
			&t.DBName, &dbTLS, &t.Provider, &clusterID, &claimURL, &claimExp, &t.SchemaVersion,
			&t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		t.DBTLS = dbTLS == 1
		if clusterID.Valid {
			t.ClusterID = clusterID.String
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

func nullStr(v string) any {
	if v == "" {
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
