package meta

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

func newControlStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	testmysql.ResetMetaDB(t, s.DB())
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
	if !strings.Contains(text, `drive9_db_operations_total{operation="`) || !strings.Contains(text, `role="meta"`) {
		t.Fatalf("expected meta db operation metric in response: %s", text)
	}
	if !strings.Contains(text, `drive9_db_pool_registered{role="meta"}`) {
		t.Fatalf("expected meta db pool metric in response: %s", text)
	}
}

func TestUpdateTenantDBCredentialIfComparesPreviousUser(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:               "credential-cas-tenant",
		Status:           TenantProvisioning,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "u1.root",
		DBPasswordCipher: []byte("root-cipher"),
		DBName:           "tenant_db",
		DBTLS:            true,
		Provider:         tidbCloudNativeProvider,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	updated, err := s.UpdateTenantDBCredentialIf(context.Background(), "credential-cas-tenant", "other.root", "u1.tdc_fs_sys", []byte("system-cipher"))
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Fatal("credential update succeeded with mismatched previous user")
	}
	var dbUser string
	var passCipher []byte
	if err := s.DB().QueryRow("SELECT db_user, db_password FROM tenants WHERE id = ?", "credential-cas-tenant").Scan(&dbUser, &passCipher); err != nil {
		t.Fatal(err)
	}
	if dbUser != "u1.root" || string(passCipher) != "root-cipher" {
		t.Fatalf("tenant credential changed after failed CAS: user=%q pass=%q", dbUser, passCipher)
	}

	updated, err = s.UpdateTenantDBCredentialIf(context.Background(), "credential-cas-tenant", "u1.root", "u1.tdc_fs_sys", []byte("system-cipher"))
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatal("credential update did not match previous user")
	}
	if err := s.DB().QueryRow("SELECT db_user, db_password FROM tenants WHERE id = ?", "credential-cas-tenant").Scan(&dbUser, &passCipher); err != nil {
		t.Fatal(err)
	}
	if dbUser != "u1.tdc_fs_sys" || string(passCipher) != "system-cipher" {
		t.Fatalf("tenant credential = %q/%q, want system credential", dbUser, passCipher)
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
	if err := s.UpsertTenantTiDBCloudOrgBinding(context.Background(), &TenantTiDBCloudOrgBinding{
		TenantID:       tenant.ID,
		OrganizationID: "org-resolved",
		ClusterID:      "cluster-resolved",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
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
	if got.TiDBCloudOrgID != "org-resolved" {
		t.Fatalf("resolved org = %q, want org-resolved", got.TiDBCloudOrgID)
	}
	if got.Tenant.TiDBCloudOrgID != "org-resolved" {
		t.Fatalf("tenant org = %q, want org-resolved", got.Tenant.TiDBCloudOrgID)
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
	if got.APIKey.ScopeKind != APIKeyScopeKindOwner {
		t.Fatalf("unexpected key scope kind: %s", got.APIKey.ScopeKind)
	}

	badKey := *key
	badKey.ID = "bad-scope-kind"
	badKey.KeyName = "bad-scope-kind"
	badKey.JWTHash = "bad-scope-kind-hash"
	badKey.ScopeKind = APIKeyScopeKind("unknown")
	if err := s.InsertAPIKey(context.Background(), &badKey); err == nil {
		t.Fatal("InsertAPIKey with unknown scope kind error = nil, want error")
	}

	if err := s.RevokeAPIKey(context.Background(), tenant.ID, key.ID); err != nil {
		t.Fatalf("RevokeAPIKey active key error = %v, want nil", err)
	}
	revoked, err := s.ResolveByAPIKeyHash(context.Background(), "hash1")
	if err != nil {
		t.Fatal(err)
	}
	if revoked.APIKey.Status != APIKeyRevoked || revoked.APIKey.RevokedAt == nil {
		t.Fatalf("revoked API key = status %s revoked_at %v, want revoked timestamp", revoked.APIKey.Status, revoked.APIKey.RevokedAt)
	}
	if err := s.RevokeAPIKey(context.Background(), tenant.ID, key.ID); err != ErrNotFound {
		t.Fatalf("RevokeAPIKey already revoked error = %v, want ErrNotFound", err)
	}
	if err := s.RevokeAPIKey(context.Background(), "wrong-tenant", key.ID); err != ErrNotFound {
		t.Fatalf("RevokeAPIKey wrong tenant error = %v, want ErrNotFound", err)
	}

	key2 := &APIKey{
		ID:                 "k2",
		TenantID:           tenant.ID,
		KeyName:            "k2",
		JWTCiphertext:      []byte("jwt2"),
		JWTHash:            "hash2",
		TokenVersion:       2,
		Status:             APIKeyActive,
		ScopeKind:          APIKeyScopeKindOwner,
		IssuedByProvider:   "slock",
		IssuedBySubjectKey: "subject-1",
		IssuedAt:           now,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if err := s.InsertAPIKey(context.Background(), key2); err != nil {
		t.Fatal(err)
	}
	key3 := *key2
	key3.ID = "k3"
	key3.KeyName = "k3"
	key3.JWTHash = "hash3"
	if err := s.InsertAPIKey(context.Background(), &key3); err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeAPIKeysByIssuer(context.Background(), tenant.ID, "slock", "subject-1", key3.ID); err != nil {
		t.Fatalf("RevokeAPIKeysByIssuer error = %v, want nil", err)
	}
	issuerRevoked, err := s.ResolveByAPIKeyHash(context.Background(), "hash2")
	if err != nil {
		t.Fatal(err)
	}
	if issuerRevoked.APIKey.Status != APIKeyRevoked {
		t.Fatalf("issuer revoked key status = %s, want %s", issuerRevoked.APIKey.Status, APIKeyRevoked)
	}
	issuerKept, err := s.ResolveByAPIKeyHash(context.Background(), "hash3")
	if err != nil {
		t.Fatal(err)
	}
	if issuerKept.APIKey.Status != APIKeyActive {
		t.Fatalf("issuer kept key status = %s, want %s", issuerKept.APIKey.Status, APIKeyActive)
	}
}

func TestInsertAndGetExternalBinding(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:               "binding-tenant",
		Status:           TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_db_binding",
		DBTLS:            true,
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	metadata := []byte(`{"server_id":"srv","sub":"sub"}`)
	if err := s.InsertExternalBinding(context.Background(), &ExternalBinding{
		Provider:     "slock",
		SubjectKey:   "srv:sub",
		TenantID:     "binding-tenant",
		MetadataJSON: metadata,
		CreatedAt:    now,
		UpdatedAt:    now,
	}); err != nil {
		t.Fatalf("InsertExternalBinding: %v", err)
	}
	got, err := s.GetExternalBinding(context.Background(), "slock", "srv:sub")
	if err != nil {
		t.Fatalf("GetExternalBinding: %v", err)
	}
	if got.Provider != "slock" || got.SubjectKey != "srv:sub" || got.TenantID != "binding-tenant" ||
		!strings.Contains(string(got.MetadataJSON), `"server_id": "srv"`) ||
		!strings.Contains(string(got.MetadataJSON), `"sub": "sub"`) {
		t.Fatalf("binding = %+v metadata=%s", got, string(got.MetadataJSON))
	}
	if err := s.InsertExternalBinding(context.Background(), got); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate InsertExternalBinding error = %v, want ErrDuplicate", err)
	}
	if _, err := s.GetExternalBinding(context.Background(), "slock", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing GetExternalBinding error = %v, want ErrNotFound", err)
	}
}

func TestInsertAPIKeyStoresIssuedByMetadata(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	tenant := &Tenant{
		ID:               "issued-by-tenant",
		Status:           TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_db_issued_by",
		DBTLS:            true,
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := s.InsertTenant(context.Background(), tenant); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertAPIKey(context.Background(), &APIKey{
		ID:                   "issued-key",
		TenantID:             tenant.ID,
		KeyName:              "slock",
		JWTCiphertext:        []byte("cipher"),
		JWTHash:              "hash-issued",
		TokenVersion:         1,
		Status:               APIKeyActive,
		ScopeKind:            APIKeyScopeKindOwner,
		IssuedByProvider:     "slock",
		IssuedBySubjectKey:   "srv:sub",
		IssuedByMetadataJSON: []byte(`{"type":"agent"}`),
		IssuedAt:             now,
		CreatedAt:            now,
		UpdatedAt:            now,
	}); err != nil {
		t.Fatalf("InsertAPIKey: %v", err)
	}
	got, err := s.ResolveByAPIKeyHash(context.Background(), "hash-issued")
	if err != nil {
		t.Fatalf("ResolveByAPIKeyHash: %v", err)
	}
	if got.APIKey.IssuedByProvider != "slock" || got.APIKey.IssuedBySubjectKey != "srv:sub" ||
		!strings.Contains(string(got.APIKey.IssuedByMetadataJSON), `"type": "agent"`) {
		t.Fatalf("issued-by metadata not round-tripped: %+v metadata=%s", got.APIKey, string(got.APIKey.IssuedByMetadataJSON))
	}
}

func TestWithExternalBindingLockSerializesCallbacks(t *testing.T) {
	s := newControlStore(t)
	var running int32
	var maxRunning int32
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.WithExternalBindingLock(context.Background(), "slock", "srv:sub", func(context.Context) error {
				n := atomic.AddInt32(&running, 1)
				for {
					max := atomic.LoadInt32(&maxRunning)
					if n <= max || atomic.CompareAndSwapInt32(&maxRunning, max, n) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
				atomic.AddInt32(&running, -1)
				return nil
			})
			if err != nil {
				t.Errorf("WithExternalBindingLock: %v", err)
			}
		}()
	}
	wg.Wait()
	if maxRunning != 1 {
		t.Fatalf("max concurrent lock holders = %d, want 1", maxRunning)
	}
}

func TestWithTenantPoolLockSerializesCallbacks(t *testing.T) {
	s := newControlStore(t)
	var running int32
	var maxRunning int32
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.WithTenantPoolLock(context.Background(), "pool-lock-test", func(context.Context) error {
				n := atomic.AddInt32(&running, 1)
				for {
					max := atomic.LoadInt32(&maxRunning)
					if n <= max || atomic.CompareAndSwapInt32(&maxRunning, max, n) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
				atomic.AddInt32(&running, -1)
				return nil
			})
			if err != nil {
				t.Errorf("WithTenantPoolLock: %v", err)
			}
		}()
	}
	wg.Wait()
	if maxRunning != 1 {
		t.Fatalf("max concurrent lock holders = %d, want 1", maxRunning)
	}
}

func TestTenantPoolDatabaseLockNameStaysWithinMySQLLimit(t *testing.T) {
	base := tenantPoolLockName("pool-lock-test")
	got := tenantPoolDatabaseLockName(base, "drive9_test_with_a_long_database_name")
	if len(got) >= 64 {
		t.Fatalf("lock name length = %d, want below 64", len(got))
	}
	if !strings.HasPrefix(got, base+":") {
		t.Fatalf("lock name = %q, want base prefix %q", got, base+":")
	}
	other := tenantPoolDatabaseLockName(base, "drive9_test_other")
	if got == other {
		t.Fatalf("database-scoped lock names should differ: %q", got)
	}
}

func TestInsertAPIKeyAllowsRepeatedKeyName(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	tenant := &Tenant{
		ID:               "repeat-key-name-tenant",
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
	for _, id := range []string{"k-repeat-a", "k-repeat-b"} {
		if err := s.InsertAPIKey(context.Background(), &APIKey{
			ID:            id,
			TenantID:      tenant.ID,
			KeyName:       "same-audit-label",
			JWTCiphertext: []byte("jwt-cipher-" + id),
			JWTHash:       "hash-" + id,
			TokenVersion:  1,
			Status:        APIKeyActive,
			IssuedAt:      now,
			CreatedAt:     now,
			UpdatedAt:     now,
		}); err != nil {
			t.Fatalf("InsertAPIKey %s: %v", id, err)
		}
	}
}

func TestInsertAndListAPIKeyFSScopes(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:               "scope-tenant",
		Status:           TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_db_scopes",
		DBTLS:            true,
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:               "other-tenant",
		Status:           TenantActive,
		DBHost:           "127.0.0.1",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tenant_db_other_scopes",
		DBTLS:            true,
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertAPIKey(context.Background(), &APIKey{
		ID:            "scope-key",
		TenantID:      "scope-tenant",
		KeyName:       "scoped",
		JWTCiphertext: []byte("jwt-cipher"),
		JWTHash:       "scope-hash",
		TokenVersion:  1,
		Status:        APIKeyActive,
		ScopeKind:     APIKeyScopeKindFS,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}
	resolved, err := s.ResolveByAPIKeyHash(context.Background(), "scope-hash")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.APIKey.ScopeKind != APIKeyScopeKindFS {
		t.Fatalf("resolved scope kind = %s, want %s", resolved.APIKey.ScopeKind, APIKeyScopeKindFS)
	}
	if err := s.InsertAPIKey(context.Background(), &APIKey{
		ID:            "other-key",
		TenantID:      "scope-tenant",
		KeyName:       "other-scoped",
		JWTCiphertext: []byte("jwt-cipher"),
		JWTHash:       "other-scope-hash",
		TokenVersion:  1,
		Status:        APIKeyActive,
		ScopeKind:     APIKeyScopeKindFS,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertAPIKey(context.Background(), &APIKey{
		ID:            "other-tenant-key",
		TenantID:      "other-tenant",
		KeyName:       "scoped",
		JWTCiphertext: []byte("jwt-cipher"),
		JWTHash:       "other-tenant-scope-hash",
		TokenVersion:  1,
		Status:        APIKeyActive,
		ScopeKind:     APIKeyScopeKindFS,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.InsertAPIKeyFSScope(context.Background(), &APIKeyFSScope{
		TenantID: "scope-tenant",
		APIKeyID: "scope-key",
		Prefix:   "/scratch/run-1",
		Ops:      "read,list",
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertAPIKeyFSScope(context.Background(), &APIKeyFSScope{
		TenantID: "scope-tenant",
		APIKeyID: "other-key",
		Prefix:   "/wrong-key",
		Ops:      "read",
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertAPIKeyFSScope(context.Background(), &APIKeyFSScope{
		TenantID: "other-tenant",
		APIKeyID: "scope-key",
		Prefix:   "/wrong-tenant",
		Ops:      "read",
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListAPIKeyFSScopes(context.Background(), "scope-tenant", "scope-key")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("scope count = %d, want 1: %#v", len(got), got)
	}
	if got[0].TenantID != "scope-tenant" || got[0].APIKeyID != "scope-key" || got[0].Prefix != "/scratch/run-1" || got[0].Ops != "read,list" {
		t.Fatalf("unexpected scope row: %#v", got[0])
	}

	if err := s.InsertAPIKeyFSScope(context.Background(), &APIKeyFSScope{
		TenantID: "scope-tenant",
		APIKeyID: "scope-key",
		Prefix:   "",
		Ops:      "read",
	}); err == nil {
		t.Fatal("InsertAPIKeyFSScope with empty prefix error = nil, want error")
	}
	if err := s.InsertAPIKeyFSScope(context.Background(), &APIKeyFSScope{
		TenantID: "scope-tenant",
		APIKeyID: "scope-key",
		Prefix:   ":",
		Ops:      "read",
	}); err == nil {
		t.Fatal("InsertAPIKeyFSScope with bare colon prefix error = nil, want error")
	}
	if err := s.InsertAPIKeyFSScope(context.Background(), &APIKeyFSScope{
		TenantID: "scope-tenant",
		APIKeyID: "scope-key",
		Prefix:   ":/",
		Ops:      "read",
	}); err != nil {
		t.Fatalf("InsertAPIKeyFSScope with explicit root prefix error = %v, want nil", err)
	}
	if err := s.InsertAPIKeyFSScope(context.Background(), &APIKeyFSScope{
		TenantID: "scope-tenant",
		APIKeyID: "scope-key",
		Prefix:   "/bad-search",
		Ops:      "search",
	}); err == nil {
		t.Fatal("InsertAPIKeyFSScope with search-only ops error = nil, want error")
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

func TestFinalizeTenantDeleteUpdatesJobNamespaceAndTenant(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := s.InsertTenant(ctx, &Tenant{
		ID:                 "delete-finalize-tenant",
		Status:             TenantDeleting,
		StorageNamespaceID: "delete-finalize-ns",
		DBHost:             "127.0.0.1",
		DBPort:             4000,
		DBUser:             "root",
		DBPasswordCipher:   []byte("cipher"),
		DBName:             "tenant_delete_finalize",
		DBTLS:              true,
		Provider:           tidbCloudNativeProvider,
		SchemaVersion:      1,
		CreatedAt:          now,
		UpdatedAt:          now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID:       "delete-finalize-tenant",
		OrganizationID: "org-delete-finalize",
		ClusterID:      "cluster-delete-finalize",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertStorageNamespace(ctx, &StorageNamespace{
		ID:            "delete-finalize-ns",
		OwnerTenantID: "delete-finalize-tenant",
		Backend:       "s3",
		Bucket:        "bucket",
		Prefix:        "tenant/delete-finalize-tenant",
		State:         StorageNamespaceDeleting,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueTenantDeleteJob(ctx, &TenantDeleteJob{
		TenantID:    "delete-finalize-tenant",
		NamespaceID: "delete-finalize-ns",
		Backend:     "s3",
		Bucket:      "bucket",
		Prefix:      "tenant/delete-finalize-tenant",
	}); err != nil {
		t.Fatal(err)
	}
	updated, err := s.MarkTenantDeleteJobRunning(ctx, "delete-finalize-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatal("MarkTenantDeleteJobRunning updated = false, want true")
	}
	if err := s.FinalizeTenantDelete(ctx, "delete-finalize-tenant", "delete-finalize-ns", 11, 2); err != nil {
		t.Fatal(err)
	}

	var jobState, nsState, tenantStatus string
	var deletedObjects, abortedMultipartUploads int64
	if err := s.DB().QueryRow(`SELECT state, deleted_objects, aborted_multipart_uploads FROM tenant_delete_jobs WHERE tenant_id = ?`, "delete-finalize-tenant").Scan(&jobState, &deletedObjects, &abortedMultipartUploads); err != nil {
		t.Fatal(err)
	}
	if jobState != string(TenantDeleteJobDeleted) || deletedObjects != 11 || abortedMultipartUploads != 2 {
		t.Fatalf("job = state %s deleted %d aborted %d, want deleted/11/2", jobState, deletedObjects, abortedMultipartUploads)
	}
	if err := s.DB().QueryRow(`SELECT state FROM storage_namespaces WHERE namespace_id = ?`, "delete-finalize-ns").Scan(&nsState); err != nil {
		t.Fatal(err)
	}
	if nsState != string(StorageNamespaceDeleted) {
		t.Fatalf("namespace state = %s, want %s", nsState, StorageNamespaceDeleted)
	}
	if err := s.DB().QueryRow(`SELECT status FROM tenants WHERE id = ?`, "delete-finalize-tenant").Scan(&tenantStatus); err != nil {
		t.Fatal(err)
	}
	if tenantStatus != string(TenantDeleted) {
		t.Fatalf("tenant status = %s, want %s", tenantStatus, TenantDeleted)
	}
	if _, err := s.GetTenantTiDBCloudOrgBinding(ctx, "delete-finalize-tenant"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("org binding err = %v, want %v", err, ErrNotFound)
	}
}

func TestListTenantNotifySinceIncludesTiDBCloudOrgBinding(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	tenantID := "notify-binding-tenant"
	if err := s.InsertTenant(ctx, &Tenant{
		ID:                 tenantID,
		Status:             TenantActive,
		StorageNamespaceID: "notify-binding-ns",
		DBHost:             "127.0.0.1",
		DBPort:             4000,
		DBUser:             "root",
		DBPasswordCipher:   []byte("cipher"),
		DBName:             "tenant_notify_binding",
		DBTLS:              true,
		Provider:           tidbCloudNativeProvider,
		SchemaVersion:      1,
		CreatedAt:          now,
		UpdatedAt:          now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID:       tenantID,
		OrganizationID: "org-notify",
		ClusterID:      "cluster-notify",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertTenantNotify(ctx, tenantID, 3); err != nil {
		t.Fatal(err)
	}

	rows, err := s.ListTenantNotifySince(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("notify row count = %d, want 1", len(rows))
	}
	if rows[0].TiDBCloudOrgID != "org-notify" {
		t.Fatalf("notify row org = %q, want org-notify", rows[0].TiDBCloudOrgID)
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

func TestCountTenants(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC()
	for _, tc := range []struct {
		id     string
		status TenantStatus
	}{
		{id: "count-active", status: TenantActive},
		{id: "count-provisioning", status: TenantProvisioning},
		{id: "count-failed", status: TenantFailed},
		{id: "count-deleted", status: TenantDeleted},
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

	got, err := s.CountTenants(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := map[TenantStatus]int64{
		TenantPending:      0,
		TenantProvisioning: 1,
		TenantActive:       1,
		TenantFailed:       1,
		TenantSuspended:    0,
		TenantDeleting:     0,
		TenantDeleted:      1,
	}
	if len(got.Statuses) != len(want) {
		t.Errorf("status count length = %d, want %d: %+v", len(got.Statuses), len(want), got.Statuses)
	}
	for status, wantCount := range want {
		if got.Count(status) != wantCount {
			t.Errorf("count[%s] = %d, want %d", status, got.Count(status), wantCount)
		}
	}
}

func TestListTenantsByStatusAfterKeyset(t *testing.T) {
	s := newControlStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, tc := range []struct {
		id        string
		status    TenantStatus
		createdAt time.Time
	}{
		{id: "active-a", status: TenantActive, createdAt: now},
		{id: "active-b", status: TenantActive, createdAt: now},
		{id: "active-c", status: TenantActive, createdAt: now.Add(time.Second)},
		{id: "provisioning-a", status: TenantProvisioning, createdAt: now},
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
			CreatedAt:        tc.createdAt,
			UpdatedAt:        tc.createdAt,
		}); err != nil {
			t.Fatal(err)
		}
	}

	first, err := s.ListTenantsByStatusAfter(context.Background(), TenantActive, time.Time{}, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].ID != "active-a" || first[1].ID != "active-b" {
		t.Fatalf("first page ids = %v, want active-a, active-b", tenantIDs(first))
	}

	second, err := s.ListTenantsByStatusAfter(context.Background(), TenantActive, first[1].CreatedAt, first[1].ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].ID != "active-c" {
		t.Fatalf("second page ids = %v, want active-c", tenantIDs(second))
	}

	empty, err := s.ListTenantsByStatusAfter(context.Background(), TenantActive, second[0].CreatedAt, second[0].ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("tail page ids = %v, want empty", tenantIDs(empty))
	}
}

func tenantIDs(tenants []Tenant) []string {
	ids := make([]string, 0, len(tenants))
	for _, t := range tenants {
		ids = append(ids, t.ID)
	}
	return ids
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

func TestValidateMetaIdentifier(t *testing.T) {
	for _, id := range []string{"tenant_tidbcloud_org_bindings", "idx_tidbcloud_org_cluster", "A1_b2"} {
		if err := validateMetaIdentifier(id); err != nil {
			t.Fatalf("validateMetaIdentifier(%q): %v", id, err)
		}
	}
	for _, id := range []string{"", "tenant-table", "idx;drop", "idx name"} {
		if err := validateMetaIdentifier(id); err == nil {
			t.Fatalf("validateMetaIdentifier(%q) error = nil, want error", id)
		}
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
	spendingLimit, ok := table.columns["tidbcloud_spending_limit"]
	if !ok {
		t.Fatal("tenant_quota_config schema missing tidbcloud_spending_limit")
	}
	if spendingLimit.addSQL != "ALTER TABLE tenant_quota_config ADD COLUMN tidbcloud_spending_limit BIGINT NULL" {
		t.Fatalf("tidbcloud_spending_limit addSQL = %q", spendingLimit.addSQL)
	}
	checkedAt, ok := table.columns["tidbcloud_spending_limit_checked_at"]
	if !ok {
		t.Fatal("tenant_quota_config schema missing tidbcloud_spending_limit_checked_at")
	}
	if checkedAt.addSQL != "ALTER TABLE tenant_quota_config ADD COLUMN tidbcloud_spending_limit_checked_at DATETIME(3) NULL" {
		t.Fatalf("tidbcloud_spending_limit_checked_at addSQL = %q", checkedAt.addSQL)
	}
	videoLLM, ok := table.columns["max_video_llm_files"]
	if !ok {
		t.Fatal("tenant_quota_config schema missing max_video_llm_files")
	}
	if videoLLM.addSQL != "ALTER TABLE tenant_quota_config ADD COLUMN max_video_llm_files BIGINT NOT NULL DEFAULT 50" {
		t.Fatalf("max_video_llm_files addSQL = %q", videoLLM.addSQL)
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

func TestMetaSchemaSpecIncludesForkStorageNamespaceColumns(t *testing.T) {
	table := mustMetaTableSpec(t, mustMetaSpec(t), "tenants")
	tests := map[string]string{
		"kind":                 "ALTER TABLE tenants ADD COLUMN kind VARCHAR(16) NOT NULL DEFAULT 'live'",
		"parent_tenant_id":     "ALTER TABLE tenants ADD COLUMN parent_tenant_id VARCHAR(64) NOT NULL DEFAULT ''",
		"storage_namespace_id": "ALTER TABLE tenants ADD COLUMN storage_namespace_id VARCHAR(64) NOT NULL DEFAULT ''",
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
	if _, ok := table.indexes["idx_tenant_namespace"]; !ok {
		t.Fatal("tenants schema missing idx_tenant_namespace")
	}
	if _, ok := table.indexes["idx_tenant_parent"]; !ok {
		t.Fatal("tenants schema missing idx_tenant_parent")
	}
	_ = mustMetaTableSpec(t, mustMetaSpec(t), "storage_namespaces")
	_ = mustMetaTableSpec(t, mustMetaSpec(t), "tenant_delete_jobs")
	_ = mustMetaTableSpec(t, mustMetaSpec(t), "object_gc_candidates")
}

func TestMetaSchemaSpecIncludesTenantStatusCreatedIDIndex(t *testing.T) {
	table := mustMetaTableSpec(t, mustMetaSpec(t), "tenants")
	idx, ok := table.indexes["idx_tenant_status_created_id"]
	if !ok {
		t.Fatal("tenants schema missing idx_tenant_status_created_id")
	}
	wantCreateSQL := "CREATE INDEX idx_tenant_status_created_id ON tenants(status, created_at, id)"
	if idx.createSQL != wantCreateSQL {
		t.Errorf("idx_tenant_status_created_id createSQL = %q, want %q", idx.createSQL, wantCreateSQL)
	}

	observed := metaTableMeta{
		tableName: "tenants",
		columns: map[string]metaColumnMeta{
			"id":         {columnType: "varchar(64)"},
			"status":     {columnType: "varchar(20)"},
			"created_at": {columnType: "datetime(3)"},
		},
	}
	observedIndexes := map[string]struct{}{
		"primary":           {},
		"idx_tenant_status": {},
	}
	diffs := diffMetaTableMetaWithObservedIndexes(table, observed, "", observedIndexes)
	if !hasMetaDiff(diffs, metaSchemaDiffMissingIndex, "idx_tenant_status_created_id") {
		t.Errorf("expected missing idx_tenant_status_created_id diff, got %#v", diffs)
	}

	var indexDiff metaSchemaDiff
	for _, diff := range diffs {
		if diff.indexName == "idx_tenant_status_created_id" {
			indexDiff = diff
			break
		}
	}
	if indexDiff.repairSQL != wantCreateSQL {
		t.Errorf("idx_tenant_status_created_id repairSQL = %q, want %q", indexDiff.repairSQL, wantCreateSQL)
	}

	plans := plannedMetaSchemaRepairs([]metaSchemaDiff{indexDiff})
	if len(plans) != 1 || plans[0] != wantCreateSQL {
		t.Errorf("repair plans = %#v, want [%q]", plans, wantCreateSQL)
	}
}

func TestFinalizeTenantDeleteRollsBackWhenTenantStatusUpdateFails(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := s.UpsertStorageNamespace(ctx, &StorageNamespace{
		ID:            "delete-ns-rollback",
		OwnerTenantID: "missing-delete-tenant",
		Backend:       "s3",
		Bucket:        "bucket",
		Prefix:        "tenant/missing-delete-tenant",
		State:         StorageNamespaceDeleting,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueTenantDeleteJob(ctx, &TenantDeleteJob{
		TenantID:    "missing-delete-tenant",
		NamespaceID: "delete-ns-rollback",
		Backend:     "s3",
		Bucket:      "bucket",
		Prefix:      "tenant/missing-delete-tenant",
	}); err != nil {
		t.Fatal(err)
	}
	updated, err := s.MarkTenantDeleteJobRunning(ctx, "missing-delete-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatal("MarkTenantDeleteJobRunning updated = false, want true")
	}

	err = s.FinalizeTenantDelete(ctx, "missing-delete-tenant", "delete-ns-rollback", 7, 3)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("FinalizeTenantDelete error = %v, want ErrNotFound", err)
	}

	var jobState string
	var deletedObjects int64
	if err := s.DB().QueryRow(`SELECT state, deleted_objects FROM tenant_delete_jobs WHERE tenant_id = ?`, "missing-delete-tenant").Scan(&jobState, &deletedObjects); err != nil {
		t.Fatal(err)
	}
	if jobState != string(TenantDeleteJobRunning) || deletedObjects != 0 {
		t.Fatalf("tenant delete job = state %s deleted_objects %d, want running/0", jobState, deletedObjects)
	}
	var nsState string
	if err := s.DB().QueryRow(`SELECT state FROM storage_namespaces WHERE namespace_id = ?`, "delete-ns-rollback").Scan(&nsState); err != nil {
		t.Fatal(err)
	}
	if nsState != string(StorageNamespaceDeleting) {
		t.Fatalf("namespace state = %s, want %s", nsState, StorageNamespaceDeleting)
	}
}

func TestMarkTenantDeleteJobRunningHonorsNotBefore(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	future := time.Now().UTC().Add(time.Hour)
	if err := s.EnqueueTenantDeleteJob(ctx, &TenantDeleteJob{
		TenantID:    "delete-job-future",
		NamespaceID: "delete-ns-future",
		Backend:     "s3",
		Bucket:      "bucket",
		Prefix:      "tenant/delete-job-future",
		NotBefore:   future,
	}); err != nil {
		t.Fatal(err)
	}
	updated, err := s.MarkTenantDeleteJobRunning(ctx, "delete-job-future")
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Fatal("MarkTenantDeleteJobRunning updated future job, want false")
	}
	var state string
	if err := s.DB().QueryRow(`SELECT state FROM tenant_delete_jobs WHERE tenant_id = ?`, "delete-job-future").Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(TenantDeleteJobPending) {
		t.Fatalf("job state = %s, want %s", state, TenantDeleteJobPending)
	}
}

func TestMetaSchemaSpecIncludesAPIKeyScopeTables(t *testing.T) {
	spec := mustMetaSpec(t)
	apiKeys := mustMetaTableSpec(t, spec, "tenant_api_keys")
	if _, ok := apiKeys.indexes["idx_api_keys_tenant_name"]; ok {
		t.Fatal("tenant_api_keys schema must not require key_name uniqueness")
	}
	scopeKind, ok := apiKeys.columns["scope_kind"]
	if !ok {
		t.Fatal("tenant_api_keys schema missing scope_kind")
	}
	if scopeKind.addSQL != "ALTER TABLE tenant_api_keys ADD COLUMN scope_kind VARCHAR(32) NOT NULL DEFAULT 'owner'" {
		t.Fatalf("scope_kind addSQL = %q", scopeKind.addSQL)
	}
	for column, wantAddSQL := range map[string]string{
		"issued_by_provider":      "ALTER TABLE tenant_api_keys ADD COLUMN issued_by_provider VARCHAR(64) NOT NULL DEFAULT ''",
		"issued_by_subject_key":   "ALTER TABLE tenant_api_keys ADD COLUMN issued_by_subject_key VARCHAR(512) NOT NULL DEFAULT ''",
		"issued_by_metadata_json": "ALTER TABLE tenant_api_keys ADD COLUMN issued_by_metadata_json JSON NULL",
	} {
		spec, ok := apiKeys.columns[column]
		if !ok {
			t.Fatalf("tenant_api_keys schema missing %s", column)
		}
		if spec.addSQL != wantAddSQL {
			t.Fatalf("%s addSQL = %q, want %q", column, spec.addSQL, wantAddSQL)
		}
	}

	scopes := mustMetaTableSpec(t, spec, "tenant_api_key_fs_scopes")
	for _, column := range []string{"tenant_id", "api_key_id", "prefix", "prefix_hash", "ops"} {
		if _, ok := scopes.columns[column]; !ok {
			t.Fatalf("tenant_api_key_fs_scopes schema missing %s", column)
		}
	}
	for _, index := range []string{"primary", "idx_fs_scopes_api_key", "idx_fs_scopes_tenant_key"} {
		if _, ok := scopes.indexes[index]; !ok {
			t.Fatalf("tenant_api_key_fs_scopes schema missing index %s", index)
		}
	}
}

func TestMetaSchemaSpecIncludesExternalBindings(t *testing.T) {
	table := mustMetaTableSpec(t, mustMetaSpec(t), "tenant_external_bindings")
	for _, column := range []string{"provider", "subject_key", "tenant_id", "metadata_json", "created_at", "updated_at"} {
		if _, ok := table.columns[column]; !ok {
			t.Fatalf("tenant_external_bindings schema missing %s", column)
		}
	}
	for _, index := range []string{"uk_external_binding_subject", "idx_external_binding_tenant"} {
		if _, ok := table.indexes[index]; !ok {
			t.Fatalf("tenant_external_bindings schema missing index %s", index)
		}
	}
}

func TestMetaSchemaSpecIncludesTiDBCloudOrgBindings(t *testing.T) {
	table := mustMetaTableSpec(t, mustMetaSpec(t), "tenant_tidbcloud_org_bindings")
	for _, column := range []string{"tenant_id", "organization_id", "cluster_id", "branch_id", "created_at", "updated_at"} {
		if _, ok := table.columns[column]; !ok {
			t.Fatalf("tenant_tidbcloud_org_bindings schema missing %s", column)
		}
	}
	for _, index := range []string{"primary", "uk_tidbcloud_org_cluster_branch", "idx_tidbcloud_org_cluster", "idx_tidbcloud_org_created"} {
		if _, ok := table.indexes[index]; !ok {
			t.Fatalf("tenant_tidbcloud_org_bindings schema missing index %s", index)
		}
	}
}

func TestMetaSchemaSpecIncludesManagedSharedDBControlPlane(t *testing.T) {
	dbPool := mustMetaTableSpec(t, mustMetaSpec(t), "db_pool")
	for _, column := range []string{
		"db_id", "uuid", "org_id", "cluster_id", "provisioning_key", "cloud_provider", "region",
		"role", "db_host", "db_port", "db_user", "db_password", "db_name", "db_tls",
		"max_tenants", "tenant_count", "soft_cap_reached", "spending_limit", "schema_version", "status",
		"created_at", "updated_at",
	} {
		if _, ok := dbPool.columns[column]; !ok {
			t.Fatalf("db_pool schema missing %s", column)
		}
	}
	for _, index := range []string{
		"primary", "uk_db_pool_uuid", "uk_db_pool_cloud_resource", "uk_db_pool_endpoint",
		"idx_db_pool_allocate", "idx_db_pool_provisioning_key",
	} {
		if _, ok := dbPool.indexes[index]; !ok {
			t.Fatalf("db_pool schema missing index %s", index)
		}
	}

	memberships := mustMetaTableSpec(t, mustMetaSpec(t), "tenant_pool_memberships")
	for _, column := range []string{
		"tenant_id", "tidbcloud_organization_id", "pool_id", "pool_status", "used_at", "created_at", "updated_at",
	} {
		if _, ok := memberships.columns[column]; !ok {
			t.Fatalf("tenant_pool_memberships schema missing %s", column)
		}
	}
	if _, ok := memberships.columns["organization_id"]; ok {
		t.Fatal("tenant_pool_memberships must not use ambiguous organization_id column")
	}
	for _, index := range []string{"primary", "idx_tenant_pool_claim", "idx_tenant_pool_org_status"} {
		if _, ok := memberships.indexes[index]; !ok {
			t.Fatalf("tenant_pool_memberships schema missing index %s", index)
		}
	}
}

func TestMigrateExpandsLegacyDBPoolForManagedProvisioning(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	if _, err := s.DB().ExecContext(ctx, `DROP TABLE db_pool`); err != nil {
		t.Fatalf("drop current db_pool: %v", err)
	}
	t.Cleanup(func() {
		_, _ = s.DB().ExecContext(context.Background(), `DROP TABLE IF EXISTS db_pool`)
		if err := s.migrate(); err != nil {
			t.Errorf("restore current db_pool schema: %v", err)
		}
	})
	if _, err := s.DB().ExecContext(ctx, `CREATE TABLE db_pool (
		db_id BIGINT AUTO_INCREMENT PRIMARY KEY,
		org_id VARCHAR(64) NOT NULL DEFAULT '',
		`+"`role`"+` VARCHAR(20) NOT NULL,
		db_host VARCHAR(255) NOT NULL,
		db_port INT NOT NULL,
		db_user VARCHAR(255) NOT NULL,
		db_password VARBINARY(2048) NOT NULL,
		db_name VARCHAR(255) NOT NULL,
		db_tls VARCHAR(32) NOT NULL DEFAULT '',
		max_tenants INT NOT NULL DEFAULT 0,
		tenant_count INT NOT NULL DEFAULT 0,
		status VARCHAR(20) NOT NULL DEFAULT 'active',
		created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
		updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
		UNIQUE INDEX uk_db_pool_endpoint (org_id, db_host, db_name),
		INDEX idx_db_pool_org (org_id, status)
	)`); err != nil {
		t.Fatalf("create legacy db_pool: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO db_pool
		(org_id, `+"`role`"+`, db_host, db_port, db_user, db_password, db_name, max_tenants)
		VALUES ('org-legacy', 'shared', 'legacy.example.com', 4000, 'root', X'01', 'legacy_db', 2)`); err != nil {
		t.Fatalf("insert legacy db_pool row: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `UPDATE db_pool SET tenant_count = 2 WHERE org_id = 'org-legacy'`); err != nil {
		t.Fatalf("fill legacy db_pool row: %v", err)
	}

	if err := s.migrate(); err != nil {
		t.Fatalf("migrate legacy db_pool: %v", err)
	}

	for _, column := range []string{"org_id", "db_host", "db_port", "db_user", "db_password", "db_name"} {
		var nullable string
		if err := s.DB().QueryRowContext(ctx, `SELECT is_nullable
			FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = 'db_pool' AND column_name = ?`, column).Scan(&nullable); err != nil {
			t.Fatalf("load db_pool.%s nullability: %v", column, err)
		}
		if nullable != "YES" {
			t.Fatalf("db_pool.%s is_nullable = %q, want YES", column, nullable)
		}
	}
	for _, index := range []string{"uk_db_pool_uuid", "uk_db_pool_cloud_resource", "idx_db_pool_allocate", "idx_db_pool_provisioning_key"} {
		exists, err := metaIndexExists(ctx, s.DB(), "db_pool", index)
		if err != nil {
			t.Fatalf("check %s: %v", index, err)
		}
		if !exists {
			t.Fatalf("db_pool index %s was not created", index)
		}
	}
	var dbPoolUUID, status string
	var softCapReached bool
	var spendingLimit *int64
	if err := s.DB().QueryRowContext(ctx, `SELECT uuid, status, spending_limit, soft_cap_reached FROM db_pool WHERE org_id = 'org-legacy'`).Scan(&dbPoolUUID, &status, &spendingLimit, &softCapReached); err != nil {
		t.Fatalf("load migrated legacy row: %v", err)
	}
	if _, err := uuid.Parse(dbPoolUUID); err != nil {
		t.Fatalf("migrated db_pool uuid = %q: %v", dbPoolUUID, err)
	}
	var uuidNullable string
	if err := s.DB().QueryRowContext(ctx, `SELECT is_nullable
		FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'db_pool' AND column_name = 'uuid'`).Scan(&uuidNullable); err != nil {
		t.Fatalf("load db_pool.uuid nullability: %v", err)
	}
	if uuidNullable != "NO" {
		t.Fatalf("db_pool.uuid is_nullable = %q, want NO", uuidNullable)
	}
	if status != "active" || spendingLimit != nil {
		t.Fatalf("legacy row status/spending_limit = %q/%v, want active/NULL", status, spendingLimit)
	}
	if !softCapReached {
		t.Fatal("legacy row at max_tenants must be backfilled with soft_cap_reached=true")
	}

	if _, err := s.DB().ExecContext(ctx, `DROP INDEX uk_db_pool_uuid ON db_pool`); err != nil {
		t.Fatalf("drop db_pool uuid index to simulate partial migration: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `UPDATE db_pool SET uuid = UPPER(uuid) WHERE org_id = 'org-legacy'`); err != nil {
		t.Fatalf("uppercase db_pool uuid to simulate partial migration: %v", err)
	}
	if err := s.migrate(); err != nil {
		t.Fatalf("resume partial db_pool uuid migration: %v", err)
	}
	if err := s.migrate(); err != nil {
		t.Fatalf("repeat completed db_pool uuid migration: %v", err)
	}
	var uuidAfterRetries string
	if err := s.DB().QueryRowContext(ctx, `SELECT uuid FROM db_pool WHERE org_id = 'org-legacy'`).Scan(&uuidAfterRetries); err != nil {
		t.Fatalf("load db_pool uuid after repeated migration: %v", err)
	}
	if uuidAfterRetries != dbPoolUUID {
		t.Fatalf("db_pool uuid after repeated migration = %q, want stable %q", uuidAfterRetries, dbPoolUUID)
	}
	if exists, err := metaIndexExists(ctx, s.DB(), "db_pool", "uk_db_pool_uuid"); err != nil || !exists {
		t.Fatalf("db_pool uuid index after repeated migration = %t/%v, want true/nil", exists, err)
	}
}

func TestMigrateNormalizesManagedSharedDBSpendingLimitToCloudMaximum(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	limit := MaxTiDBCloudSpendingLimit
	id, err := s.CreateManagedSharedDBPool(ctx, &SharedDB{
		TiDBCloudOrganizationID: "org-spending-migration", ProvisioningKey: make([]byte, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &limit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `UPDATE db_pool SET spending_limit = 10000000 WHERE db_id = ?`, id); err != nil {
		t.Fatalf("seed legacy spending limit: %v", err)
	}

	if err := s.migrate(); err != nil {
		t.Fatalf("migrate legacy spending limit: %v", err)
	}
	got, err := s.GetSharedDB(ctx, id)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if got.SpendingLimit == nil || *got.SpendingLimit != MaxTiDBCloudSpendingLimit {
		t.Fatalf("spending limit = %v, want %d", got.SpendingLimit, MaxTiDBCloudSpendingLimit)
	}

	if err := s.migrate(); err != nil {
		t.Fatalf("repeat spending-limit migration: %v", err)
	}
}

func TestUpsertTenantTiDBCloudOrgBindingRejectsDuplicateLiveClusterBranch(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	insertTiDBCloudBindingTenant(t, s, "binding-live-tenant", TenantKindLive, TenantActive, "cluster-shared", "", now)
	insertTiDBCloudBindingTenant(t, s, "binding-other-tenant", TenantKindLive, TenantActive, "cluster-shared", "", now)
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID:       "binding-live-tenant",
		OrganizationID: "org-1",
		ClusterID:      "cluster-shared",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert first binding: %v", err)
	}
	err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID:       "binding-other-tenant",
		OrganizationID: "org-1",
		ClusterID:      "cluster-shared",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate cluster branch upsert err = %v, want ErrDuplicate", err)
	}
}

func TestUpsertTenantTiDBCloudOrgBindingRejectsSharedTenant(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	insertTiDBCloudBindingTenant(t, s, "binding-shared-tenant", TenantKindLive, TenantActive, "", "", now)
	if err := s.UpdateTenantProvider(ctx, "binding-shared-tenant", "tidb_cloud_native_shared"); err != nil {
		t.Fatalf("mark tenant shared: %v", err)
	}
	err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID: "binding-shared-tenant", OrganizationID: "org-shared",
		ClusterID: "cluster-shared", CreatedAt: now, UpdatedAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "shared tenant") {
		t.Fatalf("shared binding error = %v, want shared tenant rejection", err)
	}
}

func TestUpsertTenantTiDBCloudOrgBindingAllowsSharedClusterAcrossBranches(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	insertTiDBCloudBindingTenant(t, s, "binding-live-tenant", TenantKindLive, TenantActive, "cluster-shared", "", now)
	insertTiDBCloudBindingTenant(t, s, "binding-fork-tenant", TenantKindFork, TenantActive, "cluster-shared", "branch-1", now)
	for _, tenantID := range []string{"binding-live-tenant", "binding-fork-tenant"} {
		if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
			TenantID:       tenantID,
			OrganizationID: "org-1",
			ClusterID:      "cluster-shared",
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			t.Fatalf("upsert binding for %s: %v", tenantID, err)
		}
	}
	forkBinding, err := s.GetTenantTiDBCloudOrgBinding(ctx, "binding-fork-tenant")
	if err != nil {
		t.Fatalf("get fork binding: %v", err)
	}
	if forkBinding.BranchID != "branch-1" {
		t.Fatalf("fork binding branch id = %q, want branch-1", forkBinding.BranchID)
	}
}

func TestUpsertTenantTiDBCloudOrgBindingIgnoresDeletedClusterBranch(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	insertTiDBCloudBindingTenant(t, s, "binding-deleted-tenant", TenantKindLive, TenantDeleted, "cluster-reused", "", now)
	insertTiDBCloudBindingTenant(t, s, "binding-new-tenant", TenantKindLive, TenantActive, "cluster-reused", "", now)
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID:       "binding-deleted-tenant",
		OrganizationID: "org-1",
		ClusterID:      "cluster-reused",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert deleted binding: %v", err)
	}
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID:       "binding-new-tenant",
		OrganizationID: "org-1",
		ClusterID:      "cluster-reused",
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert new binding after deleted tenant: %v", err)
	}
}

func TestEnsureNoDuplicateTiDBCloudOrgBindingTuplesReportsConflict(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	tenantIDs := []string{"binding-dup-1", "binding-dup-2", "binding-dup-3", "binding-dup-4"}
	if _, err := s.DB().ExecContext(ctx, `DROP INDEX uk_tidbcloud_org_cluster_branch ON tenant_tidbcloud_org_bindings`); err != nil {
		t.Fatalf("drop unique index: %v", err)
	}
	t.Cleanup(func() {
		_, _ = s.DB().ExecContext(context.Background(), `DELETE FROM tenant_tidbcloud_org_bindings WHERE tenant_id IN (?, ?, ?, ?)`, tenantIDs[0], tenantIDs[1], tenantIDs[2], tenantIDs[3])
		_, _ = s.DB().ExecContext(context.Background(), `DELETE FROM tenants WHERE id IN (?, ?, ?, ?)`, tenantIDs[0], tenantIDs[1], tenantIDs[2], tenantIDs[3])
		if err := ensureTiDBCloudOrgBindingUniqueIndex(context.Background(), s.DB()); err != nil {
			t.Fatalf("restore unique index: %v", err)
		}
	})
	insertTiDBCloudBindingTenant(t, s, "binding-dup-1", TenantKindLive, TenantActive, "cluster-dup-a", "", now)
	insertTiDBCloudBindingTenant(t, s, "binding-dup-2", TenantKindLive, TenantActive, "cluster-dup-a", "", now)
	insertTiDBCloudBindingTenant(t, s, "binding-dup-3", TenantKindLive, TenantActive, "cluster-dup-b", "branch-dup", now)
	insertTiDBCloudBindingTenant(t, s, "binding-dup-4", TenantKindLive, TenantActive, "cluster-dup-b", "branch-dup", now)
	for _, binding := range []struct {
		tenantID  string
		clusterID string
		branchID  string
	}{
		{tenantID: "binding-dup-1", clusterID: "cluster-dup-a"},
		{tenantID: "binding-dup-2", clusterID: "cluster-dup-a"},
		{tenantID: "binding-dup-3", clusterID: "cluster-dup-b", branchID: "branch-dup"},
		{tenantID: "binding-dup-4", clusterID: "cluster-dup-b", branchID: "branch-dup"},
	} {
		if _, err := s.DB().ExecContext(ctx, `INSERT INTO tenant_tidbcloud_org_bindings
			(tenant_id, organization_id, cluster_id, branch_id, pool_id, pool_status, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			binding.tenantID, "org-1", binding.clusterID, binding.branchID, "", TenantPoolBindingUsed, now, now); err != nil {
			t.Fatalf("insert duplicate binding %s: %v", binding.tenantID, err)
		}
	}
	err := ensureNoDuplicateTiDBCloudOrgBindingTuples(ctx, s.DB())
	if err == nil {
		t.Fatal("ensureNoDuplicateTiDBCloudOrgBindingTuples error = nil, want duplicate error")
	}
	for _, want := range []string{"found 2 duplicate tuple(s)", "org-1", "cluster-dup-a", "cluster-dup-b", "branch-dup", "binding-dup-1", "binding-dup-2", "binding-dup-3", "binding-dup-4"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("duplicate error %q does not contain %q", err, want)
		}
	}
}

func TestBackfillTiDBCloudOrgBindingBranchIDsTrimsTenantBranchID(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	insertTiDBCloudBindingTenant(t, s, "binding-spaced-branch-tenant", TenantKindFork, TenantActive, "cluster-spaced", " branch-1 ", now)
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO tenant_tidbcloud_org_bindings
		(tenant_id, organization_id, cluster_id, branch_id, pool_id, pool_status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"binding-spaced-branch-tenant", "org-1", "cluster-spaced", "", "", TenantPoolBindingUsed, now, now); err != nil {
		t.Fatalf("insert raw binding: %v", err)
	}
	if err := backfillTiDBCloudOrgBindingBranchIDs(ctx, s.DB()); err != nil {
		t.Fatalf("backfill branch ids: %v", err)
	}
	binding, err := s.GetTenantTiDBCloudOrgBinding(ctx, "binding-spaced-branch-tenant")
	if err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if binding.BranchID != "branch-1" {
		t.Fatalf("binding branch id = %q, want branch-1", binding.BranchID)
	}
}

func insertTiDBCloudBindingTenant(t *testing.T, s *Store, tenantID string, kind TenantKind, status TenantStatus, clusterID, branchID string, now time.Time) {
	t.Helper()
	if err := s.InsertTenant(context.Background(), &Tenant{
		ID:               tenantID,
		Status:           status,
		Kind:             kind,
		DBHost:           "db.example.com",
		DBPort:           4000,
		DBUser:           "u.root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tidbCloudNativeProvider,
		ClusterID:        clusterID,
		BranchID:         branchID,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant %s: %v", tenantID, err)
	}
}

func TestClaimOldestFreeTenantPoolBindingRequiresActiveTenant(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := s.CreateTenantPool(ctx, &TenantPool{
		PoolID:         "pool-claim-active",
		OrganizationID: "org-claim-active",
		Size:           2,
		Status:         TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("create tenant pool: %v", err)
	}
	provisioningCreated := now.Add(-2 * time.Minute)
	if err := s.InsertTenant(ctx, &Tenant{
		ID:               "pool-tenant-provisioning",
		Status:           TenantProvisioning,
		Kind:             TenantKindLive,
		DBHost:           "db.example.com",
		DBPort:           4000,
		DBUser:           "u.root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tidbCloudNativeProvider,
		ClusterID:        "cluster-provisioning",
		SchemaVersion:    1,
		CreatedAt:        provisioningCreated,
		UpdatedAt:        provisioningCreated,
	}); err != nil {
		t.Fatalf("insert provisioning tenant: %v", err)
	}
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID:       "pool-tenant-provisioning",
		OrganizationID: "org-claim-active",
		ClusterID:      "cluster-provisioning",
		PoolID:         "pool-claim-active",
		PoolStatus:     TenantPoolBindingFree,
		CreatedAt:      provisioningCreated,
		UpdatedAt:      provisioningCreated,
	}); err != nil {
		t.Fatalf("upsert provisioning binding: %v", err)
	}
	if _, err := s.ClaimOldestFreeTenantPoolBinding(ctx, "org-claim-active"); !errors.Is(err, ErrNotFound) {
		t.Errorf("claim provisioning tenant err = %v, want ErrNotFound", err)
	}

	activeCreated := now.Add(-time.Minute)
	if err := s.InsertTenant(ctx, &Tenant{
		ID:               "pool-tenant-active",
		Status:           TenantActive,
		Kind:             TenantKindLive,
		DBHost:           "db.example.com",
		DBPort:           4000,
		DBUser:           "u.root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tidbCloudNativeProvider,
		ClusterID:        "cluster-active",
		SchemaVersion:    1,
		CreatedAt:        activeCreated,
		UpdatedAt:        activeCreated,
	}); err != nil {
		t.Fatalf("insert active tenant: %v", err)
	}
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID:       "pool-tenant-active",
		OrganizationID: "org-claim-active",
		ClusterID:      "cluster-active",
		PoolID:         "pool-claim-active",
		PoolStatus:     TenantPoolBindingFree,
		CreatedAt:      activeCreated,
		UpdatedAt:      activeCreated,
	}); err != nil {
		t.Fatalf("upsert active binding: %v", err)
	}
	row, err := s.ClaimOldestFreeTenantPoolBinding(ctx, "org-claim-active")
	if err != nil {
		t.Errorf("claim active tenant: %v", err)
	} else if row.Tenant.ID != "pool-tenant-active" {
		t.Errorf("claimed tenant = %q, want pool-tenant-active", row.Tenant.ID)
	}
	provisioningBinding, err := s.GetTenantTiDBCloudOrgBinding(ctx, "pool-tenant-provisioning")
	if err != nil {
		t.Fatalf("get provisioning binding: %v", err)
	}
	if provisioningBinding.PoolStatus != TenantPoolBindingFree {
		t.Errorf("provisioning pool status = %s, want %s", provisioningBinding.PoolStatus, TenantPoolBindingFree)
	}
}

func TestCountTenantPoolBindingsByStatusGroupsByPoolOrganizationAndStatus(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	for _, pool := range []TenantPool{
		{
			PoolID:         "pool-binding-counts-a",
			OrganizationID: "org-binding-counts-a",
			Size:           4,
			Status:         TenantPoolActive,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			PoolID:         "pool-binding-counts-empty",
			OrganizationID: "org-binding-counts-empty",
			Size:           2,
			Status:         TenantPoolActive,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	} {
		if err := s.CreateTenantPool(ctx, &pool); err != nil {
			t.Fatalf("create tenant pool %s: %v", pool.PoolID, err)
		}
	}
	for _, tc := range []struct {
		tenantID string
		cluster  string
		status   TenantStatus
		pool     TenantPoolBindingStatus
	}{
		{tenantID: "binding-counts-free-1", cluster: "cluster-binding-counts-free-1", status: TenantActive, pool: TenantPoolBindingFree},
		{tenantID: "binding-counts-free-2", cluster: "cluster-binding-counts-free-2", status: TenantProvisioning, pool: TenantPoolBindingFree},
		{tenantID: "binding-counts-used-1", cluster: "cluster-binding-counts-used-1", status: TenantActive, pool: TenantPoolBindingUsed},
		{tenantID: "binding-counts-deleted-1", cluster: "cluster-binding-counts-deleted-1", status: TenantDeleted, pool: TenantPoolBindingFree},
	} {
		insertTiDBCloudBindingTenant(t, s, tc.tenantID, TenantKindLive, tc.status, tc.cluster, "", now)
		if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
			TenantID:       tc.tenantID,
			OrganizationID: "org-binding-counts-a",
			ClusterID:      tc.cluster,
			PoolID:         "pool-binding-counts-a",
			PoolStatus:     tc.pool,
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			t.Fatalf("upsert binding %s: %v", tc.tenantID, err)
		}
	}

	counts, err := s.CountTenantPoolBindingsByStatus(ctx)
	if err != nil {
		t.Fatalf("count tenant pool bindings by status: %v", err)
	}
	got := map[string]int64{}
	for _, count := range counts {
		got[count.PoolID+"|"+count.OrganizationID+"|"+string(count.Status)] = count.Count
	}
	want := map[string]int64{
		"pool-binding-counts-a|org-binding-counts-a|free":         2,
		"pool-binding-counts-a|org-binding-counts-a|used":         1,
		"pool-binding-counts-empty|org-binding-counts-empty|free": 0,
		"pool-binding-counts-empty|org-binding-counts-empty|used": 0,
	}
	for key, wantCount := range want {
		if got[key] != wantCount {
			t.Errorf("count %s = %d, want %d; all counts=%v", key, got[key], wantCount, got)
		}
	}
}

func TestDiffMetaTableMetaReportsMissingPrimaryKeyConstraint(t *testing.T) {
	spec := mustMetaTableSpec(t, mustMetaSpec(t), "tenant_quota_config")
	meta := metaTableMeta{
		tableName: "tenant_quota_config",
		columns: map[string]metaColumnMeta{
			"tenant_id":                           {columnType: "varchar(64)"},
			"max_storage_bytes":                   {columnType: "bigint"},
			"max_file_size_bytes":                 {columnType: "bigint"},
			"max_file_count":                      {columnType: "bigint"},
			"max_media_llm_files":                 {columnType: "bigint"},
			"max_video_llm_files":                 {columnType: "bigint"},
			"max_monthly_cost_mc":                 {columnType: "bigint"},
			"tidbcloud_spending_limit":            {columnType: "bigint"},
			"tidbcloud_spending_limit_checked_at": {columnType: "datetime(3)"},
			"created_at":                          {columnType: "datetime(3)"},
			"updated_at":                          {columnType: "datetime(3)"},
		},
	}
	createStmt := `CREATE TABLE tenant_quota_config (
		tenant_id VARCHAR(64) NOT NULL,
		max_storage_bytes BIGINT NOT NULL,
		max_file_size_bytes BIGINT NOT NULL,
		max_file_count BIGINT NOT NULL,
		max_media_llm_files BIGINT NOT NULL,
		max_video_llm_files BIGINT NOT NULL,
		max_monthly_cost_mc BIGINT NOT NULL,
		tidbcloud_spending_limit BIGINT NULL,
		tidbcloud_spending_limit_checked_at DATETIME(3) NULL,
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
		return
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
