package vault

import (
	"context"
	"crypto/rand"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	mk, err := NewMasterKey(key)
	if err != nil {
		t.Fatal(err)
	}
	// Clean vault tables before each test.
	for _, tbl := range []string{"vault_audit_log", "vault_tokens", "vault_secret_fields", "vault_secrets", "vault_deks", "vault_policies"} {
		if _, err := testDB.Exec("DELETE FROM " + tbl); err != nil {
			t.Fatalf("clean %s: %v", tbl, err)
		}
	}
	return NewStore(testDB, mk)
}

func TestStoreDEKRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// First call creates a new DEK.
	dek1, err := s.GetOrCreateDEK(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("GetOrCreateDEK: %v", err)
	}
	if len(dek1) != 32 {
		t.Fatalf("expected 32-byte DEK, got %d", len(dek1))
	}

	// Second call returns the same DEK (idempotent).
	dek2, err := s.GetOrCreateDEK(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("GetOrCreateDEK second call: %v", err)
	}
	if string(dek1) != string(dek2) {
		t.Fatal("DEK should be stable across calls")
	}

	// Different tenant gets a different DEK.
	dek3, err := s.GetOrCreateDEK(ctx, "tenant-2")
	if err != nil {
		t.Fatalf("GetOrCreateDEK tenant-2: %v", err)
	}
	if string(dek1) == string(dek3) {
		t.Fatal("different tenants should have different DEKs")
	}
}

func TestStoreSecretCRUD(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tenantID := "tenant-crud"

	// Create
	fields := map[string][]byte{
		"username": []byte("admin"),
		"password": []byte("s3cret"),
	}
	sec, err := s.CreateSecret(ctx, tenantID, "db-prod", "agent-1", SecretTypeDatabase, fields)
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}
	if sec.Name != "db-prod" || sec.Revision != 1 {
		t.Fatalf("unexpected secret: name=%s rev=%d", sec.Name, sec.Revision)
	}

	// Get
	got, err := s.GetSecret(ctx, tenantID, "db-prod")
	if err != nil {
		t.Fatalf("GetSecret: %v", err)
	}
	if got.SecretID != sec.SecretID {
		t.Fatalf("GetSecret ID mismatch: %s != %s", got.SecretID, sec.SecretID)
	}

	// ReadSecretFields — decrypt and verify round-trip
	decrypted, err := s.ReadSecretFields(ctx, tenantID, "db-prod")
	if err != nil {
		t.Fatalf("ReadSecretFields: %v", err)
	}
	if string(decrypted["username"]) != "admin" || string(decrypted["password"]) != "s3cret" {
		t.Fatalf("decrypted fields mismatch: %v", decrypted)
	}

	// ReadSecretField — single field
	pw, err := s.ReadSecretField(ctx, tenantID, "db-prod", "password")
	if err != nil {
		t.Fatalf("ReadSecretField: %v", err)
	}
	if string(pw) != "s3cret" {
		t.Fatalf("ReadSecretField mismatch: %q", pw)
	}

	// List
	list, err := s.ListSecrets(ctx, tenantID)
	if err != nil {
		t.Fatalf("ListSecrets: %v", err)
	}
	if len(list) != 1 || list[0].Name != "db-prod" {
		t.Fatalf("ListSecrets unexpected: %d", len(list))
	}

	// Update — bumps revision, re-encrypts fields
	newFields := map[string][]byte{
		"username": []byte("admin"),
		"password": []byte("n3wpass"),
	}
	updated, err := s.UpdateSecret(ctx, tenantID, "db-prod", "agent-1", newFields)
	if err != nil {
		t.Fatalf("UpdateSecret: %v", err)
	}
	if updated.Revision != 2 {
		t.Fatalf("expected revision 2, got %d", updated.Revision)
	}

	// Verify updated field
	pw2, err := s.ReadSecretField(ctx, tenantID, "db-prod", "password")
	if err != nil {
		t.Fatalf("ReadSecretField after update: %v", err)
	}
	if string(pw2) != "n3wpass" {
		t.Fatalf("updated password mismatch: %q", pw2)
	}

	// Delete (soft)
	if err := s.DeleteSecret(ctx, tenantID, "db-prod"); err != nil {
		t.Fatalf("DeleteSecret: %v", err)
	}

	// Get after delete should fail
	_, err = s.GetSecret(ctx, tenantID, "db-prod")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got: %v", err)
	}

	// Delete non-existent should fail
	if err := s.DeleteSecret(ctx, tenantID, "db-prod"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for double delete, got: %v", err)
	}
}

func TestStoreCapTokenIssueRevokeVerify(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tenantID := "tenant-token"

	tokenStr, capToken, err := s.IssueCapToken(ctx, IssueCapTokenParams{
		TenantID:      tenantID,
		Issuer:        "https://drive9.example.com",
		PrincipalType: PrincipalDelegated,
		Agent:         "agent-1",
		Scope:         []string{"secret-a"},
		Perm:          PermRead,
		LabelHint:     "agent-1-secret-a",
		TTL:           time.Hour,
	})
	if err != nil {
		t.Fatalf("IssueCapToken: %v", err)
	}
	if tokenStr == "" || capToken.GrantID == "" {
		t.Fatal("empty token")
	}
	if capToken.Perm != PermRead {
		t.Fatalf("perm not set: %s", capToken.Perm)
	}

	// Verify should succeed.
	resolved, err := s.VerifyAndResolveCapToken(ctx, tenantID, tokenStr)
	if err != nil {
		t.Fatalf("VerifyAndResolveCapToken: %v", err)
	}
	if resolved.GrantID != capToken.GrantID {
		t.Fatalf("grant ID mismatch: %s != %s", resolved.GrantID, capToken.GrantID)
	}
	if resolved.Issuer != "https://drive9.example.com" {
		t.Fatalf("issuer mismatch: %s", resolved.Issuer)
	}

	// Revoke.
	if err := s.RevokeCapToken(ctx, tenantID, capToken.GrantID, "admin", "test revoke"); err != nil {
		t.Fatalf("RevokeCapToken: %v", err)
	}

	// Verify after revoke should fail.
	_, err = s.VerifyAndResolveCapToken(ctx, tenantID, tokenStr)
	if err == nil {
		t.Fatal("expected error for revoked token")
	}

	// Double revoke should fail.
	if err := s.RevokeCapToken(ctx, tenantID, capToken.GrantID, "admin", "again"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for double revoke, got: %v", err)
	}
}

func TestStoreAuditWriteQuery(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tenantID := "tenant-audit"

	event := &AuditEvent{
		EventID:    "evt-1",
		TenantID:   tenantID,
		EventType:  "secret.read",
		Agent:      "agent-1",
		SecretName: "db-prod",
		FieldName:  "password",
		Adapter:    "env",
		Detail:     map[string]string{"reason": "task execution"},
		Timestamp:  time.Now(),
	}
	if err := s.WriteAuditEvent(ctx, event); err != nil {
		t.Fatalf("WriteAuditEvent: %v", err)
	}

	// Query by tenant.
	events, err := s.QueryAuditLog(ctx, tenantID, "", 10)
	if err != nil {
		t.Fatalf("QueryAuditLog: %v", err)
	}
	if len(events) != 1 || events[0].EventID != "evt-1" {
		t.Fatalf("unexpected audit events: %d", len(events))
	}

	// Query by secret name.
	events2, err := s.QueryAuditLog(ctx, tenantID, "db-prod", 10)
	if err != nil {
		t.Fatalf("QueryAuditLog by secret: %v", err)
	}
	if len(events2) != 1 {
		t.Fatalf("expected 1 audit event for db-prod, got %d", len(events2))
	}

	// Query for non-existent secret returns empty.
	events3, err := s.QueryAuditLog(ctx, tenantID, "nonexistent", 10)
	if err != nil {
		t.Fatalf("QueryAuditLog nonexistent: %v", err)
	}
	if len(events3) != 0 {
		t.Fatalf("expected 0 events for nonexistent, got %d", len(events3))
	}
}
