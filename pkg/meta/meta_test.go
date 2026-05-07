package meta

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/metrics"
)

func newControlStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	_, _ = s.DB().Exec("DELETE FROM tenant_api_keys")
	_, _ = s.DB().Exec("DELETE FROM tenants")
	_, _ = s.DB().Exec("DELETE FROM llm_usage")
	return s
}

func TestMetaDBMetrics(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:               "metrics-meta-tenant",
		Status:           TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_db_metrics",
		DBTLS:            true,
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `dat9_db_operations_total{role="meta"`) {
		t.Fatalf("expected meta db operation metric in response: %s", text)
	}
	if !strings.Contains(text, `dat9_db_pool_registered{role="meta"}`) {
		t.Fatalf("expected meta db pool metric in response: %s", text)
	}
}

func TestInsertAndResolveByAPIKeyHash(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	tenant := &Tenant{
		ID:               "t1",
		Status:           TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_db",
		DBTLS:            true,
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := s.InsertTenant(context.Background(), tenant); err != nil {
		t.Fatal(err)
	}
	key := &APIKey{
		ID:            "k1",
		TenantID:      tenant.ID,
		KeyName:       "default",
		JWTCiphertext: []byte("jwt-cipher"),
		JWTHash:       "hash1",
		TokenVersion:  1,
		Status:        APIKeyActive,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := s.InsertAPIKey(context.Background(), key); err != nil {
		t.Fatal(err)
	}

	got, err := s.ResolveByAPIKeyHash(context.Background(), "hash1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Tenant.ID != "t1" || got.APIKey.ID != "k1" {
		t.Fatalf("unexpected resolve result: tenant=%s key=%s", got.Tenant.ID, got.APIKey.ID)
	}
	if got.Tenant.Status != TenantActive {
		t.Fatalf("unexpected tenant status: %s", got.Tenant.Status)
	}
	if got.Tenant.S3EncryptionMode != S3EncryptionModeInherit {
		t.Fatalf("unexpected tenant encryption mode: %s", got.Tenant.S3EncryptionMode)
	}
	if got.Tenant.S3KMSKeyID != "" {
		t.Fatalf("unexpected tenant kms key: %q", got.Tenant.S3KMSKeyID)
	}
	if !got.Tenant.S3BucketKeyEnabledValue() {
		t.Fatal("tenant bucket key enabled = false, want true")
	}
	if got.APIKey.Status != APIKeyActive {
		t.Fatalf("unexpected key status: %s", got.APIKey.Status)
	}
}

func TestRevokeAPIKeyByName(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:               "tenant-revoke-key",
		Status:           TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_revoke_key_db",
		DBTLS:            true,
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertAPIKey(context.Background(), &APIKey{
		ID:            "k-revoke",
		TenantID:      "tenant-revoke-key",
		KeyName:       "worker",
		JWTCiphertext: []byte("jwt-cipher"),
		JWTHash:       "hash-revoke",
		TokenVersion:  1,
		Status:        APIKeyActive,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}

	revokedAt := now.Add(time.Minute)
	if err := s.RevokeAPIKeyByName(context.Background(), "tenant-revoke-key", "worker", revokedAt); err != nil {
		t.Fatalf("RevokeAPIKeyByName: %v", err)
	}

	got, err := s.ResolveByAPIKeyHash(context.Background(), "hash-revoke")
	if err != nil {
		t.Fatal(err)
	}
	if got.APIKey.Status != APIKeyRevoked {
		t.Fatalf("key status = %s, want %s", got.APIKey.Status, APIKeyRevoked)
	}
	if got.APIKey.RevokedAt == nil || !got.APIKey.RevokedAt.Equal(revokedAt.Round(time.Millisecond)) {
		t.Fatalf("revoked_at = %v, want %v", got.APIKey.RevokedAt, revokedAt)
	}

	if err := s.RevokeAPIKeyByName(context.Background(), "tenant-revoke-key", "worker", revokedAt.Add(time.Minute)); err != ErrNotFound {
		t.Fatalf("second revoke err = %v, want %v", err, ErrNotFound)
	}
	if err := s.RevokeAPIKeyByName(context.Background(), "tenant-revoke-key", "missing", revokedAt); err != ErrNotFound {
		t.Fatalf("missing revoke err = %v, want %v", err, ErrNotFound)
	}
}

func TestListAndGetAPIKeysByTenant(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:               "tenant-list-keys",
		Status:           TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_list_keys_db",
		DBTLS:            true,
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	keys := []*APIKey{
		{
			ID:            "k-default",
			TenantID:      "tenant-list-keys",
			KeyName:       "default",
			JWTCiphertext: []byte("jwt-default"),
			JWTHash:       "hash-default",
			TokenVersion:  1,
			Status:        APIKeyActive,
			IssuedAt:      now,
			CreatedAt:     now,
			UpdatedAt:     now,
		},
		{
			ID:            "k-worker",
			TenantID:      "tenant-list-keys",
			KeyName:       "worker",
			JWTCiphertext: []byte("jwt-worker"),
			JWTHash:       "hash-worker",
			TokenVersion:  2,
			Status:        APIKeyActive,
			IssuedAt:      now.Add(time.Second),
			CreatedAt:     now.Add(time.Second),
			UpdatedAt:     now.Add(time.Second),
		},
	}
	for _, key := range keys {
		if err := s.InsertAPIKey(context.Background(), key); err != nil {
			t.Fatal(err)
		}
	}

	gotKeys, err := s.ListAPIKeysByTenant(context.Background(), "tenant-list-keys")
	if err != nil {
		t.Fatal(err)
	}
	if len(gotKeys) != 2 {
		t.Fatalf("len(keys)=%d, want 2", len(gotKeys))
	}
	if gotKeys[0].KeyName != "default" || gotKeys[1].KeyName != "worker" {
		t.Fatalf("unexpected key order: %q, %q", gotKeys[0].KeyName, gotKeys[1].KeyName)
	}

	gotKey, err := s.GetAPIKeyByName(context.Background(), "tenant-list-keys", "worker")
	if err != nil {
		t.Fatal(err)
	}
	if gotKey.ID != "k-worker" || gotKey.KeyName != "worker" {
		t.Fatalf("unexpected key: %#v", gotKey)
	}
	if _, err := s.GetAPIKeyByName(context.Background(), "tenant-list-keys", "missing"); err != ErrNotFound {
		t.Fatalf("missing key err = %v, want %v", err, ErrNotFound)
	}
}

func TestGetTenantReadsS3EncryptionPolicy(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:                 "tenant-s3-policy",
		Status:             TenantActive,
		DBHost:             "127.0.0.1",
		DBPort:             4000,
		DBUser:             "root",
		DBPasswordCipher:   []byte("cipher"),
		DBName:             "tenant_db_s3_policy",
		DBTLS:              true,
		Provider:           "tidb_zero",
		SchemaVersion:      1,
		S3EncryptionMode:   S3EncryptionModeSSEKMS,
		S3BucketKeyEnabled: boolPtr(false),
		CreatedAt:          now,
		UpdatedAt:          now,
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetTenant(context.Background(), "tenant-s3-policy")
	if err != nil {
		t.Fatal(err)
	}
	if got.S3EncryptionMode != S3EncryptionModeSSEKMS {
		t.Fatalf("S3EncryptionMode = %q, want sse-kms", got.S3EncryptionMode)
	}
	if got.S3KMSKeyID != "" {
		t.Fatalf("S3KMSKeyID = %q, want empty", got.S3KMSKeyID)
	}
	if got.S3BucketKeyEnabledValue() {
		t.Fatal("S3BucketKeyEnabled = true, want false")
	}
}

func TestInsertTenantPreservesExplicitBucketKeyFalseWithEmptyMode(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:                 "tenant-explicit-bucket-key-false",
		Status:             TenantActive,
		DBHost:             "127.0.0.1",
		DBPort:             4000,
		DBUser:             "root",
		DBPasswordCipher:   []byte("cipher"),
		DBName:             "tenant_db_explicit_bucket_false",
		DBTLS:              true,
		Provider:           "tidb_zero",
		SchemaVersion:      1,
		S3BucketKeyEnabled: boolPtr(false),
		CreatedAt:          now,
		UpdatedAt:          now,
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetTenant(context.Background(), "tenant-explicit-bucket-key-false")
	if err != nil {
		t.Fatal(err)
	}
	if got.S3EncryptionMode != S3EncryptionModeInherit {
		t.Fatalf("S3EncryptionMode = %q, want inherit", got.S3EncryptionMode)
	}
	if got.S3BucketKeyEnabledValue() {
		t.Fatal("S3BucketKeyEnabled = true, want false")
	}
}

func TestUpdateTenantStatus(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:               "t2",
		Status:           TenantProvisioning,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_db2",
		DBTLS:            true,
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTenantStatus(context.Background(), "t2", TenantSuspended); err != nil {
		t.Fatal(err)
	}

	row := s.DB().QueryRow(`SELECT status FROM tenants WHERE id = ?`, "t2")
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(TenantSuspended) {
		t.Fatalf("status=%s", status)
	}
}

func TestListTenantsByStatus(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	for _, tc := range []struct {
		id     string
		status TenantStatus
	}{
		{id: "tp1", status: TenantProvisioning},
		{id: "tp2", status: TenantProvisioning},
		{id: "ta1", status: TenantActive},
	} {
		if err := s.InsertTenant(context.Background(), &Tenant{
			ID:               tc.id,
			Status:           tc.status,
			DBHost:           "127.0.0.1",
			DBPort:           4000,
			DBUser:           "root",
			DBPasswordCipher: []byte("cipher"),
			DBName:           "tenant_db",
			DBTLS:            true,
			Provider:         "tidb_zero",
			SchemaVersion:    1,
			CreatedAt:        now,
			UpdatedAt:        now,
		}); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.ListTenantsByStatus(context.Background(), TenantProvisioning, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 provisioning tenants, got %d", len(got))
	}
	if got[0].Status != TenantProvisioning || got[1].Status != TenantProvisioning {
		t.Fatalf("unexpected statuses: %s, %s", got[0].Status, got[1].Status)
	}
}

func TestMetaSchemaSpecFromStatementsParsesNewTable(t *testing.T) {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS tenant_custom_events (
			event_id VARCHAR(64) PRIMARY KEY,
			tenant_id VARCHAR(64) NOT NULL,
			payload JSON,
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			INDEX idx_tenant_custom_events_tenant (tenant_id)
		)`,
	}
	spec, err := metaSchemaSpecFromStatements(stmts)
	if err != nil {
		t.Fatalf("metaSchemaSpecFromStatements: %v", err)
	}
	table := mustMetaTableSpec(t, spec, "tenant_custom_events")
	if _, ok := table.columns["tenant_id"]; !ok {
		t.Fatal("expected tenant_id in parsed columns")
	}
	if _, ok := table.indexes["idx_tenant_custom_events_tenant"]; !ok {
		t.Fatal("expected idx_tenant_custom_events_tenant in parsed indexes")
	}
}

func TestDiffMetaTableMetaReportsMissingColumnAndIndex(t *testing.T) {
	spec := mustMetaTableSpec(t, mustMetaSpec(t), "tenant_api_keys")
	meta := metaTableMeta{
		tableName: "tenant_api_keys",
		columns: map[string]metaColumnMeta{
			"id":             {columnType: "varchar(64)"},
			"tenant_id":      {columnType: "varchar(64)"},
			"jwt_ciphertext": {columnType: "varbinary(4096)"},
			"jwt_hash":       {columnType: "varchar(128)"},
		},
	}
	createStmt := `CREATE TABLE tenant_api_keys (
		id VARCHAR(64) PRIMARY KEY,
		tenant_id VARCHAR(64) NOT NULL,
		jwt_ciphertext VARBINARY(4096) NOT NULL,
		jwt_hash VARCHAR(128) NOT NULL
	)`
	diffs := diffMetaTableMeta(spec, meta, createStmt)
	if !hasMetaDiff(diffs, metaSchemaDiffMissingColumn, "key_name") {
		t.Fatalf("expected missing key_name diff, got %#v", diffs)
	}
	if !hasMetaDiff(diffs, metaSchemaDiffMissingIndex, "idx_api_keys_tenant") {
		t.Fatalf("expected missing idx_api_keys_tenant diff, got %#v", diffs)
	}
}

func TestMetaSchemaSpecTracksPrimaryKeyConstraint(t *testing.T) {
	spec := mustMetaSpec(t)
	table := mustMetaTableSpec(t, spec, "tenant_quota_config")
	pk, ok := table.indexes["primary"]
	if !ok {
		t.Fatal("expected primary key constraint to be tracked in schema spec")
	}
	if !pk.isPrimary {
		t.Fatal("expected primary constraint marker")
	}
}

func TestMetaSchemaSpecIncludesTenantS3EncryptionColumns(t *testing.T) {
	table := mustMetaTableSpec(t, mustMetaSpec(t), "tenants")
	tests := map[string]string{
		"s3_encryption_mode":    "ALTER TABLE tenants ADD COLUMN s3_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'inherit'",
		"s3_kms_key_id":         "ALTER TABLE tenants ADD COLUMN s3_kms_key_id VARCHAR(256) NOT NULL DEFAULT ''",
		"s3_bucket_key_enabled": "ALTER TABLE tenants ADD COLUMN s3_bucket_key_enabled TINYINT(1) NOT NULL DEFAULT 1",
	}
	for column, wantAddSQL := range tests {
		spec, ok := table.columns[column]
		if !ok {
			t.Fatalf("missing %s in tenants schema spec", column)
		}
		if spec.addSQL != wantAddSQL {
			t.Fatalf("%s addSQL = %q, want %q", column, spec.addSQL, wantAddSQL)
		}
	}
}

func TestDiffMetaTableMetaReportsMissingPrimaryKeyConstraint(t *testing.T) {
	spec := mustMetaTableSpec(t, mustMetaSpec(t), "tenant_quota_config")
	meta := metaTableMeta{
		tableName: "tenant_quota_config",
		columns: map[string]metaColumnMeta{
			"tenant_id":           {columnType: "varchar(64)"},
			"max_storage_bytes":   {columnType: "bigint"},
			"max_media_llm_files": {columnType: "bigint"},
			"max_monthly_cost_mc": {columnType: "bigint"},
			"created_at":          {columnType: "datetime(3)"},
			"updated_at":          {columnType: "datetime(3)"},
		},
	}
	createStmt := `CREATE TABLE tenant_quota_config (
		tenant_id VARCHAR(64) NOT NULL,
		max_storage_bytes BIGINT NOT NULL,
		max_media_llm_files BIGINT NOT NULL,
		max_monthly_cost_mc BIGINT NOT NULL,
		created_at DATETIME(3) NOT NULL,
		updated_at DATETIME(3) NOT NULL
	)`
	diffs := diffMetaTableMeta(spec, meta, createStmt)
	if !hasMetaDiff(diffs, metaSchemaDiffMissingIndex, "missing primary key constraint") {
		t.Fatalf("expected missing primary key diff, got %#v", diffs)
	}
}

func TestPlannedMetaSchemaRepairsSkipsUnsafeRepairs(t *testing.T) {
	diffs := []metaSchemaDiff{
		{kind: metaSchemaDiffMissingColumn, tableName: "tenant_api_keys", columnName: "must_fill", repairSQL: "ALTER TABLE tenant_api_keys ADD COLUMN must_fill BIGINT NOT NULL"},
		{kind: metaSchemaDiffMissingIndex, tableName: "tenant_api_keys", indexName: "uk_key_name", repairSQL: "CREATE UNIQUE INDEX uk_key_name ON tenant_api_keys(key_name)"},
		{kind: metaSchemaDiffMissingIndex, tableName: "tenant_api_keys", indexName: "idx_api_keys_tenant", repairSQL: "CREATE INDEX idx_api_keys_tenant ON tenant_api_keys(tenant_id, status)"},
	}

	plans := plannedMetaSchemaRepairs(diffs)
	if len(plans) != 1 {
		t.Fatalf("expected exactly one safe repair, got %#v", plans)
	}
	if plans[0] != "CREATE INDEX idx_api_keys_tenant ON tenant_api_keys(tenant_id, status)" {
		t.Fatalf("unexpected plan: %#v", plans)
	}
}

func mustMetaSpec(t *testing.T) metaSchemaSpec {
	t.Helper()
	spec, err := metaSchemaSpecFromStatements(metaInitSchemaStatements())
	if err != nil {
		t.Fatalf("meta schema spec: %v", err)
	}
	return spec
}

func mustMetaTableSpec(t *testing.T, spec metaSchemaSpec, tableName string) metaTableSpec {
	t.Helper()
	for _, table := range spec.tables {
		if table.name == tableName {
			return table
		}
	}
	t.Fatalf("missing table %q in meta schema spec", tableName)
	return metaTableSpec{}
}

func hasMetaDiff(diffs []metaSchemaDiff, kind metaSchemaDiffKind, contains string) bool {
	for _, diff := range diffs {
		if diff.kind != kind {
			continue
		}
		if strings.Contains(strings.ToLower(diff.detail), strings.ToLower(contains)) {
			return true
		}
	}
	return false
}

func TestColumnTypeMismatchSchemaVersionPlansRepair(t *testing.T) {
	spec := mustMetaSpec(t)
	tenantsSpec := mustMetaTableSpec(t, spec, "tenants")

	// Simulate tenants table with schema_version as INT (old type).
	observed := metaTableMeta{
		tableName: "tenants",
		columns:   map[string]metaColumnMeta{"schema_version": {columnType: "int"}},
	}
	diffs := diffMetaTableMetaWithObservedIndexes(tenantsSpec, observed, "", map[string]struct{}{})

	var typeDiff *metaSchemaDiff
	for i := range diffs {
		if diffs[i].kind == metaSchemaDiffColumnType && diffs[i].columnName == "schema_version" {
			typeDiff = &diffs[i]
			break
		}
	}
	if typeDiff == nil {
		t.Fatal("expected a column_type_mismatch diff for schema_version, got none")
	}
	if typeDiff.repairSQL == "" {
		t.Fatal("expected repairSQL to be set for schema_version type mismatch")
	}

	plans := plannedMetaSchemaRepairs([]metaSchemaDiff{*typeDiff})
	if len(plans) != 1 {
		t.Fatalf("expected exactly one planned repair, got %#v", plans)
	}
	want := "ALTER TABLE tenants MODIFY COLUMN schema_version INT UNSIGNED NOT NULL DEFAULT 1"
	if plans[0] != want {
		t.Fatalf("unexpected repair plan:\n  got  %q\n  want %q", plans[0], want)
	}
}

func TestColumnTypeMismatchOtherColumnsNoRepair(t *testing.T) {
	spec := mustMetaSpec(t)
	tenantsSpec := mustMetaTableSpec(t, spec, "tenants")

	// Simulate a type mismatch on a column other than schema_version — no auto-repair expected.
	observed := metaTableMeta{
		tableName: "tenants",
		columns:   map[string]metaColumnMeta{"db_port": {columnType: "bigint"}},
	}
	diffs := diffMetaTableMetaWithObservedIndexes(tenantsSpec, observed, "", map[string]struct{}{})

	for _, d := range diffs {
		if d.kind == metaSchemaDiffColumnType && d.columnName != "schema_version" && d.repairSQL != "" {
			t.Errorf("unexpected repairSQL for non-schema_version column %q: %q", d.columnName, d.repairSQL)
		}
	}
}

func TestIsSafeModifyColumnRepairSQLAcceptsSchemaVersion(t *testing.T) {
	diff := metaSchemaDiff{
		tableName:  "tenants",
		columnName: "schema_version",
		repairSQL:  "ALTER TABLE tenants MODIFY COLUMN schema_version INT UNSIGNED NOT NULL DEFAULT 1",
	}
	if !isSafeModifyColumnRepairSQL(diff) {
		t.Fatal("expected isSafeModifyColumnRepairSQL to return true for schema_version repair")
	}
}

func TestIsSafeModifyColumnRepairSQLRejectsOtherCases(t *testing.T) {
	cases := []metaSchemaDiff{
		{tableName: "tenants", columnName: "db_port", repairSQL: "ALTER TABLE tenants MODIFY COLUMN db_port INT UNSIGNED NOT NULL"},
		{tableName: "tenant_api_keys", columnName: "schema_version", repairSQL: "ALTER TABLE tenant_api_keys MODIFY COLUMN schema_version INT UNSIGNED NOT NULL DEFAULT 1"},
		{tableName: "tenants", columnName: "schema_version", repairSQL: "ALTER TABLE tenants MODIFY COLUMN schema_version BIGINT NOT NULL DEFAULT 1"},
		{tableName: "tenants", columnName: "schema_version", repairSQL: ""},
		{tableName: "tenants", columnName: "schema_version", repairSQL: "ALTER TABLE tenants MODIFY COLUMN schema_version INT UNSIGNED"},
	}
	for _, diff := range cases {
		if isSafeModifyColumnRepairSQL(diff) {
			t.Errorf("isSafeModifyColumnRepairSQL(%q.%q sql=%q) = true, want false",
				diff.tableName, diff.columnName, diff.repairSQL)
		}
	}
}
