package meta

import (
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
	if err := s.InsertTenant(tenant); err != nil {
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
	if err := s.InsertAPIKey(key); err != nil {
		t.Fatal(err)
	}

	got, err := s.ResolveByAPIKeyHash("hash1")
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
	if err := s.InsertTenant(&Tenant{
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
	if err := s.UpdateTenantStatus("t2", TenantSuspended); err != nil {
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
		if err := s.InsertTenant(&Tenant{
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

	got, err := s.ListTenantsByStatus(TenantProvisioning, 10)
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
