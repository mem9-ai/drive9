package meta

import (
	"context"
	"strings"
	"testing"
	"time"
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
	if got.APIKey.Status != APIKeyActive {
		t.Fatalf("unexpected key status: %s", got.APIKey.Status)
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
