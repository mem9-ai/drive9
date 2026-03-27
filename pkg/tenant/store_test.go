package tenant

import (
	"crypto/rand"
	"strings"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	if testDSN == "" {
		t.Skip("no test database available")
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	enc, err := NewEncryptor(key)
	if err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(testDSN, enc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Clean up test data
		s.db.Exec("DELETE FROM tenants")
		s.Close()
	})
	// Clean before test too
	s.db.Exec("DELETE FROM tenants")
	return s
}

func TestInsertAndGetByHash(t *testing.T) {
	s := newTestStore(t)

	raw, prefix, hash, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	_ = raw // only returned to caller once

	pwEnc, err := s.EncryptPassword("secret123")
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	tenant := &Tenant{
		ID:              "t-001",
		APIKeyPrefix:    prefix,
		APIKeyHash:      hash,
		Status:          StatusProvisioning,
		DBHost:          "127.0.0.1",
		DBPort:          4000,
		DBUser:          "root",
		DBPasswordEnc:   pwEnc,
		DBName:          "tenant_001",
		ClusterID:       "cluster-abc",
		ProvisionerType: "local",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := s.Insert(tenant); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Lookup by hash
	got, err := s.GetByAPIKeyHash(hash)
	if err != nil {
		t.Fatalf("GetByAPIKeyHash: %v", err)
	}
	if got.ID != "t-001" {
		t.Errorf("ID = %q, want %q", got.ID, "t-001")
	}
	if got.Status != StatusProvisioning {
		t.Errorf("Status = %q, want %q", got.Status, StatusProvisioning)
	}
	if got.DBHost != "127.0.0.1" {
		t.Errorf("DBHost = %q", got.DBHost)
	}

	// Decrypt password
	pw, err := s.DecryptPassword(got.DBPasswordEnc)
	if err != nil {
		t.Fatalf("DecryptPassword: %v", err)
	}
	if pw != "secret123" {
		t.Errorf("password = %q, want %q", pw, "secret123")
	}
}

func TestGetByID(t *testing.T) {
	s := newTestStore(t)

	_, prefix, hash, _ := GenerateAPIKey()
	now := time.Now().UTC().Truncate(time.Millisecond)
	s.Insert(&Tenant{
		ID: "t-002", APIKeyPrefix: prefix, APIKeyHash: hash,
		Status: StatusActive, CreatedAt: now, UpdatedAt: now,
	})

	got, err := s.Get("t-002")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != StatusActive {
		t.Errorf("Status = %q", got.Status)
	}
}

func TestUpdateStatus(t *testing.T) {
	s := newTestStore(t)

	_, prefix, hash, _ := GenerateAPIKey()
	now := time.Now().UTC().Truncate(time.Millisecond)
	s.Insert(&Tenant{
		ID: "t-003", APIKeyPrefix: prefix, APIKeyHash: hash,
		Status: StatusActive, CreatedAt: now, UpdatedAt: now,
	})

	if err := s.UpdateStatus("t-003", StatusSuspended); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get("t-003")
	if got.Status != StatusSuspended {
		t.Errorf("Status = %q, want suspended", got.Status)
	}
}

func TestNotFound(t *testing.T) {
	s := newTestStore(t)

	_, err := s.GetByAPIKeyHash("nonexistent")
	if err != ErrNotFound {
		t.Errorf("err = %v, want ErrNotFound", err)
	}

	_, err = s.Get("nonexistent")
	if err != ErrNotFound {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestDuplicateAPIKey(t *testing.T) {
	s := newTestStore(t)

	_, prefix, hash, _ := GenerateAPIKey()
	now := time.Now().UTC().Truncate(time.Millisecond)
	s.Insert(&Tenant{
		ID: "t-dup1", APIKeyPrefix: prefix, APIKeyHash: hash,
		Status: StatusActive, CreatedAt: now, UpdatedAt: now,
	})

	err := s.Insert(&Tenant{
		ID: "t-dup2", APIKeyPrefix: prefix, APIKeyHash: hash,
		Status: StatusActive, CreatedAt: now, UpdatedAt: now,
	})
	if err != ErrDuplicate {
		t.Errorf("err = %v, want ErrDuplicate", err)
	}
}

func TestList(t *testing.T) {
	s := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Millisecond)
	for i, st := range []Status{StatusActive, StatusActive, StatusSuspended} {
		_, prefix, hash, _ := GenerateAPIKey()
		s.Insert(&Tenant{
			ID: "t-list-" + itoa(i), APIKeyPrefix: prefix, APIKeyHash: hash,
			Status: st, CreatedAt: now, UpdatedAt: now,
		})
	}

	all, err := s.List("")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("List all: got %d, want 3", len(all))
	}

	active, err := s.List(StatusActive)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 2 {
		t.Fatalf("List active: got %d, want 2", len(active))
	}
}

func TestHashAPIKey(t *testing.T) {
	raw, _, hash, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	if HashAPIKey(raw) != hash {
		t.Error("HashAPIKey mismatch")
	}
}

func TestDSN(t *testing.T) {
	got := DSN("db.example.com", 4000, "admin", "pass", "mydb", "")
	want := "admin:pass@tcp(db.example.com:4000)/mydb?parseTime=true"
	if got != want {
		t.Errorf("DSN = %q, want %q", got, want)
	}

	// With TLS
	gotTLS := DSN("cloud.tidbapi.com", 4000, "user", "pw", "test", TLSConfigName())
	if !strings.Contains(gotTLS, "tls=tidb-cloud") {
		t.Errorf("DSN with TLS = %q, want tls=tidb-cloud", gotTLS)
	}
}

func itoa(i int) string {
	return string(rune('0' + i))
}
