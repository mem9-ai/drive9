package vault

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

var vaultSharedTables = []string{
	"vault_audit_log",
	"vault_grants",
	"vault_tokens",
	"vault_secret_fields",
	"vault_secrets",
	"vault_deks",
	"vault_policies",
}

// installSharedVaultSchema swaps the 7 vault tables to the shared (fs_id)
// shape and restores the standalone schema on cleanup. Tests in this package
// run sequentially, so the swap is safe. A second database cannot be used
// instead: the testcontainer MySQL user only has privileges on the test
// database.
func installSharedVaultSchema(t *testing.T) {
	t.Helper()
	drop := func() {
		t.Helper()
		for _, tbl := range vaultSharedTables {
			if _, err := testDB.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
				t.Fatalf("drop %s: %v", tbl, err)
			}
		}
	}
	drop()
	if err := schema.ExecSchemaStatements(testDB, schema.VaultMySQLSharedSchemaStatements()); err != nil {
		t.Fatalf("apply shared vault schema: %v", err)
	}
	t.Cleanup(func() {
		for _, tbl := range vaultSharedTables {
			if _, err := testDB.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
				t.Errorf("restore: drop %s: %v", tbl, err)
			}
		}
		if err := schema.ExecSchemaStatements(testDB, schema.VaultTiDBSchemaStatements()); err != nil {
			t.Errorf("restore standalone vault schema: %v", err)
		}
	})
}

// newSharedTestMasterKey returns a fresh random master key.
func newSharedTestMasterKey(t *testing.T) *MasterKey {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	mk, err := NewMasterKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return mk
}

// newSharedVaultStore returns a Store bound to the shared vault schema with
// the given fs_id as its tenant row key.
func newSharedVaultStore(t *testing.T, fsID int64, mk *MasterKey) *Store {
	t.Helper()
	return NewStoreScoped(testDB, mk, datastore.SharedScope(fsID))
}

// runVaultCoreScenario exercises the vault core flow (DEK round trip, secret
// CRUD, capability tokens, grants, audit log) against a store. It is run
// against both schema shapes to prove behavioral parity.
func runVaultCoreScenario(t *testing.T, s *Store, tenantID string) {
	t.Helper()
	ctx := context.Background()

	// DEK round trip: stable across calls.
	dek1, err := s.GetOrCreateDEK(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetOrCreateDEK: %v", err)
	}
	dek2, err := s.GetOrCreateDEK(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetOrCreateDEK second call: %v", err)
	}
	if string(dek1) != string(dek2) {
		t.Fatal("DEK should be stable across calls")
	}

	// Secret create / get / read.
	sec, err := s.CreateSecret(ctx, tenantID, "db-prod", "agent-1", SecretTypeDatabase, map[string][]byte{
		"username": []byte("admin"),
		"password": []byte("s3cret"),
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}
	if sec.TenantID != tenantID {
		t.Fatalf("created TenantID = %q, want %q", sec.TenantID, tenantID)
	}
	got, err := s.GetSecret(ctx, tenantID, "db-prod")
	if err != nil {
		t.Fatalf("GetSecret: %v", err)
	}
	if got.SecretID != sec.SecretID || got.TenantID != tenantID {
		t.Fatalf("GetSecret = %#v, want id %s tenant %q", got, sec.SecretID, tenantID)
	}
	decrypted, err := s.ReadSecretFields(ctx, tenantID, "db-prod")
	if err != nil {
		t.Fatalf("ReadSecretFields: %v", err)
	}
	if string(decrypted["username"]) != "admin" || string(decrypted["password"]) != "s3cret" {
		t.Fatalf("decrypted fields mismatch: %v", decrypted)
	}
	pw, err := s.ReadSecretField(ctx, tenantID, "db-prod", "password")
	if err != nil {
		t.Fatalf("ReadSecretField: %v", err)
	}
	if string(pw) != "s3cret" {
		t.Fatalf("ReadSecretField mismatch: %q", pw)
	}
	list, err := s.ListSecrets(ctx, tenantID)
	if err != nil {
		t.Fatalf("ListSecrets: %v", err)
	}
	if len(list) != 1 || list[0].Name != "db-prod" || list[0].TenantID != tenantID {
		t.Fatalf("ListSecrets unexpected: %#v", list)
	}

	// Update bumps revision and re-encrypts fields.
	updated, err := s.UpdateSecret(ctx, tenantID, "db-prod", "agent-1", map[string][]byte{
		"username": []byte("admin"),
		"password": []byte("n3wpass"),
	})
	if err != nil {
		t.Fatalf("UpdateSecret: %v", err)
	}
	if updated.Revision != 2 {
		t.Fatalf("expected revision 2, got %d", updated.Revision)
	}
	pw2, err := s.ReadSecretField(ctx, tenantID, "db-prod", "password")
	if err != nil {
		t.Fatalf("ReadSecretField after update: %v", err)
	}
	if string(pw2) != "n3wpass" {
		t.Fatalf("updated password mismatch: %q", pw2)
	}

	// Capability token: issue / verify / revoke.
	tokenStr, capToken, err := s.IssueCapToken(ctx, tenantID, "agent-1", "task-1", []string{"db-prod"}, time.Hour)
	if err != nil {
		t.Fatalf("IssueCapToken: %v", err)
	}
	resolved, err := s.VerifyAndResolveCapToken(ctx, tenantID, tokenStr)
	if err != nil {
		t.Fatalf("VerifyAndResolveCapToken: %v", err)
	}
	if resolved.TokenID != capToken.TokenID {
		t.Fatalf("token ID mismatch: %s != %s", resolved.TokenID, capToken.TokenID)
	}
	if err := s.RevokeCapToken(ctx, tenantID, capToken.TokenID, "admin", "test revoke"); err != nil {
		t.Fatalf("RevokeCapToken: %v", err)
	}
	if _, err := s.VerifyAndResolveCapToken(ctx, tenantID, tokenStr); err == nil {
		t.Fatal("expected error for revoked token")
	}

	// Grant: issue / verify / revoke.
	const issuer = "https://vault-test.invalid"
	grantStr, grant, err := s.IssueGrant(ctx, tenantID, issuer, PrincipalDelegated, "agent-1",
		[]string{"db-prod"}, GrantPermRead, time.Hour, "")
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}
	claims, err := s.VerifyAndResolveGrant(ctx, tenantID, issuer, grantStr)
	if err != nil {
		t.Fatalf("VerifyAndResolveGrant: %v", err)
	}
	if claims.GrantID != grant.GrantID {
		t.Fatalf("grant ID mismatch: %s != %s", claims.GrantID, grant.GrantID)
	}
	if err := s.RevokeGrant(ctx, tenantID, grant.GrantID, "admin", "test revoke"); err != nil {
		t.Fatalf("RevokeGrant: %v", err)
	}
	if _, err := s.VerifyAndResolveGrant(ctx, tenantID, issuer, grantStr); err == nil {
		t.Fatal("expected error for revoked grant")
	}

	// Audit: write and query back, with and without the secret filter.
	if err := s.WriteAuditEvent(ctx, &AuditEvent{
		TenantID:   tenantID,
		EventType:  "secret.read",
		AgentID:    "agent-1",
		SecretName: "db-prod",
		FieldName:  "password",
		Adapter:    "env",
		Detail:     map[string]string{"reason": "task execution"},
	}); err != nil {
		t.Fatalf("WriteAuditEvent: %v", err)
	}
	events, err := s.QueryAuditLog(ctx, tenantID, "", 10)
	if err != nil {
		t.Fatalf("QueryAuditLog: %v", err)
	}
	if len(events) != 1 || events[0].TenantID != tenantID || events[0].SecretName != "db-prod" {
		t.Fatalf("unexpected audit events: %#v", events)
	}
	events, err = s.QueryAuditLog(ctx, tenantID, "db-prod", 10)
	if err != nil {
		t.Fatalf("QueryAuditLog by secret: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 audit event for db-prod, got %d", len(events))
	}

	// Delete frees the (tenant, name) unique slot for recreate.
	if err := s.DeleteSecret(ctx, tenantID, "db-prod"); err != nil {
		t.Fatalf("DeleteSecret: %v", err)
	}
	if _, err := s.GetSecret(ctx, tenantID, "db-prod"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got: %v", err)
	}
	if _, err := s.CreateSecret(ctx, tenantID, "db-prod", "agent-2", SecretTypeGeneric,
		map[string][]byte{"host": []byte("db2.example.com")}); err != nil {
		t.Fatalf("CreateSecret after delete should succeed, got: %v", err)
	}
}

// TestVaultSharedShapeParity runs the same core flow covered by the
// standalone store tests against the shared (fs_id) schema shape.
func TestVaultSharedShapeParity(t *testing.T) {
	installSharedVaultSchema(t)
	store := newSharedVaultStore(t, 4300001, newSharedTestMasterKey(t))
	runVaultCoreScenario(t, store, "tenant-shared-parity")
}

// TestVaultSharedShapeCrossTenantIsolation proves rows of one fs_id are
// invisible to another fs_id on the same shared tables, and that the same
// secret name can coexist under two fs_ids with independent values.
func TestVaultSharedShapeCrossTenantIsolation(t *testing.T) {
	installSharedVaultSchema(t)
	ctx := context.Background()
	mk := newSharedTestMasterKey(t)
	storeA := newSharedVaultStore(t, 4300002, mk)
	storeB := newSharedVaultStore(t, 4300003, mk)
	tenantA, tenantB := "tenant-iso-a", "tenant-iso-b"

	// Same secret name under two different fs_ids must coexist.
	if _, err := storeA.CreateSecret(ctx, tenantA, "shared-name", "agent-a", SecretTypeGeneric,
		map[string][]byte{"password": []byte("a-secret")}); err != nil {
		t.Fatalf("CreateSecret A: %v", err)
	}
	if _, err := storeB.CreateSecret(ctx, tenantB, "shared-name", "agent-b", SecretTypeGeneric,
		map[string][]byte{"password": []byte("b-secret")}); err != nil {
		t.Fatalf("CreateSecret B: %v", err)
	}

	// Each side reads only its own value.
	pwA, err := storeA.ReadSecretField(ctx, tenantA, "shared-name", "password")
	if err != nil {
		t.Fatalf("ReadSecretField A: %v", err)
	}
	if string(pwA) != "a-secret" {
		t.Fatalf("A password = %q, want a-secret", pwA)
	}
	pwB, err := storeB.ReadSecretField(ctx, tenantB, "shared-name", "password")
	if err != nil {
		t.Fatalf("ReadSecretField B: %v", err)
	}
	if string(pwB) != "b-secret" {
		t.Fatalf("B password = %q, want b-secret", pwB)
	}

	// A name that only A created is invisible to B.
	if _, err := storeA.CreateSecret(ctx, tenantA, "a-only", "agent-a", SecretTypeGeneric,
		map[string][]byte{"k": []byte("v")}); err != nil {
		t.Fatalf("CreateSecret a-only: %v", err)
	}
	if _, err := storeB.GetSecret(ctx, tenantB, "a-only"); err != ErrNotFound {
		t.Fatalf("B GetSecret a-only err = %v, want ErrNotFound", err)
	}
	if _, err := storeB.ReadSecretFields(ctx, tenantB, "a-only"); err != ErrNotFound {
		t.Fatalf("B ReadSecretFields a-only err = %v, want ErrNotFound", err)
	}
	listB, err := storeB.ListSecrets(ctx, tenantB)
	if err != nil {
		t.Fatalf("ListSecrets B: %v", err)
	}
	if len(listB) != 1 || listB[0].Name != "shared-name" {
		t.Fatalf("B sees %d secrets, want only its own: %#v", len(listB), listB)
	}

	// Updates stay on their own side.
	if _, err := storeB.UpdateSecret(ctx, tenantB, "shared-name", "agent-b",
		map[string][]byte{"password": []byte("b-secret-2")}); err != nil {
		t.Fatalf("UpdateSecret B: %v", err)
	}
	pwA2, err := storeA.ReadSecretField(ctx, tenantA, "shared-name", "password")
	if err != nil {
		t.Fatalf("ReadSecretField A after B update: %v", err)
	}
	if string(pwA2) != "a-secret" {
		t.Fatalf("A password after B update = %q, want a-secret (cross-tenant write)", pwA2)
	}

	// B cannot revoke A's tokens or grants, and A's rows survive the attempts.
	tokenStrA, capA, err := storeA.IssueCapToken(ctx, tenantA, "agent-a", "", []string{"shared-name"}, time.Hour)
	if err != nil {
		t.Fatalf("IssueCapToken A: %v", err)
	}
	if err := storeB.RevokeCapToken(ctx, tenantB, capA.TokenID, "agent-b", "x"); err != ErrNotFound {
		t.Fatalf("B RevokeCapToken A's token err = %v, want ErrNotFound", err)
	}
	if _, err := storeA.VerifyAndResolveCapToken(ctx, tenantA, tokenStrA); err != nil {
		t.Fatalf("A's token must survive B's revoke attempt: %v", err)
	}
	const issuer = "https://vault-test.invalid"
	grantStrA, grantA, err := storeA.IssueGrant(ctx, tenantA, issuer,
		PrincipalDelegated, "agent-a", []string{"shared-name"}, GrantPermRead, time.Hour, "")
	if err != nil {
		t.Fatalf("IssueGrant A: %v", err)
	}
	if err := storeB.RevokeGrant(ctx, tenantB, grantA.GrantID, "agent-b", "x"); err != ErrNotFound {
		t.Fatalf("B RevokeGrant A's grant err = %v, want ErrNotFound", err)
	}
	if _, err := storeA.VerifyAndResolveGrant(ctx, tenantA, issuer, grantStrA); err != nil {
		t.Fatalf("A's grant must survive B's revoke attempt: %v", err)
	}

	// Audit rows written under A are invisible to B.
	if err := storeA.WriteAuditEvent(ctx, &AuditEvent{
		TenantID:   tenantA,
		EventType:  "secret.read",
		SecretName: "a-only",
	}); err != nil {
		t.Fatalf("WriteAuditEvent A: %v", err)
	}
	eventsB, err := storeB.QueryAuditLog(ctx, tenantB, "", 10)
	if err != nil {
		t.Fatalf("QueryAuditLog B: %v", err)
	}
	if len(eventsB) != 0 {
		t.Fatalf("B sees %d audit events, want 0 (cross-tenant leak)", len(eventsB))
	}

	// Deletes stay on their own side.
	if err := storeB.DeleteSecret(ctx, tenantB, "shared-name"); err != nil {
		t.Fatalf("DeleteSecret B: %v", err)
	}
	if _, err := storeA.GetSecret(ctx, tenantA, "shared-name"); err != nil {
		t.Fatalf("A's secret must survive B's delete: %v", err)
	}
}

// TestVaultSharedShapeStoresFsID asserts every vault table row carries the
// scope's fs_id as its tenant discriminator.
func TestVaultSharedShapeStoresFsID(t *testing.T) {
	installSharedVaultSchema(t)
	const fsID int64 = 4300004
	store := newSharedVaultStore(t, fsID, newSharedTestMasterKey(t))
	runVaultCoreScenario(t, store, "tenant-fsid-check")

	for _, tbl := range vaultSharedTables {
		var got int64
		err := store.DB().QueryRow("SELECT COUNT(*) FROM "+tbl+" WHERE fs_id != ?", fsID).Scan(&got)
		if err != nil {
			t.Fatalf("count %s rows with foreign fs_id: %v", tbl, err)
		}
		if got != 0 {
			t.Fatalf("%s has %d rows with fs_id != %d", tbl, got, fsID)
		}
		if tbl == "vault_policies" {
			continue // no writer path for policies in pkg/vault
		}
		var total int64
		if err := store.DB().QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&total); err != nil {
			t.Fatalf("count %s: %v", tbl, err)
		}
		if total == 0 {
			t.Fatalf("%s is empty; scenario should have written rows", tbl)
		}
	}
	// No residual tenant_id column in the shared shape, and vault_secret_fields
	// gained fs_id as its leading column.
	for _, tc := range []struct {
		table, column string
		want          int
	}{
		{"vault_secrets", "tenant_id", 0},
		{"vault_secret_fields", "fs_id", 1},
	} {
		var n int
		if err := store.DB().QueryRow(`SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
			tc.table, tc.column).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != tc.want {
			t.Fatalf("%s.%s column count = %d, want %d", tc.table, tc.column, n, tc.want)
		}
	}
}
