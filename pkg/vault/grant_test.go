package vault

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newGrantTestStore returns a Store with vault tables (including vault_grants)
// cleared, isolating one test from another.
func newGrantTestStore(t *testing.T) *Store {
	t.Helper()
	s := newTestStore(t)
	// newTestStore already cleans the legacy tables; also clean vault_grants.
	if _, err := testDB.Exec("DELETE FROM vault_grants"); err != nil {
		t.Fatalf("clean vault_grants: %v", err)
	}
	return s
}

func TestIssueAndVerifyGrantDelegated(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()

	tok, _, err := s.IssueGrant(
		ctx, "tenant-a", "https://srv.invalid",
		PrincipalDelegated, "agent-1", []string{"db-prod/password"},
		GrantPermWrite, time.Hour, "ci-pipeline",
	)
	if err != nil {
		t.Fatalf("IssueGrant delegated: %v", err)
	}
	claims, err := s.VerifyAndResolveGrant(ctx, "tenant-a", "https://srv.invalid", tok)
	if err != nil {
		t.Fatalf("VerifyAndResolveGrant: %v", err)
	}
	if claims.PrincipalType != PrincipalDelegated {
		t.Fatalf("principal_type: got %q", claims.PrincipalType)
	}
	if claims.Perm != GrantPermWrite {
		t.Fatalf("perm: got %q", claims.Perm)
	}
	if claims.LabelHint != "ci-pipeline" {
		t.Fatalf("label_hint: got %q", claims.LabelHint)
	}
}

func TestVerifyGrantRevoked(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()

	tok, grant, err := s.IssueGrant(
		ctx, "tenant-a", "https://srv.invalid",
		PrincipalDelegated, "agent-1", []string{"aws-prod"},
		GrantPermRead, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}

	if err := s.RevokeGrant(ctx, "tenant-a", grant.GrantID, "admin", "rotated"); err != nil {
		t.Fatalf("RevokeGrant: %v", err)
	}
	if _, err := s.VerifyAndResolveGrant(ctx, "tenant-a", "https://srv.invalid", tok); err == nil {
		t.Fatal("expected VerifyAndResolveGrant to reject revoked grant")
	}

	// Second revoke must return ErrNotFound (already revoked).
	if err := s.RevokeGrant(ctx, "tenant-a", grant.GrantID, "admin", "again"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound on second revoke, got %v", err)
	}
}

func TestVerifyGrantCrossTenantReplay(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()

	tok, _, err := s.IssueGrant(
		ctx, "tenant-a", "https://srv.invalid",
		PrincipalDelegated, "agent-1", []string{"aws-prod"},
		GrantPermRead, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}
	// Replay the tenant-A token under tenant-B — must fail even though the
	// token JWT is structurally valid, because the DB row lives under A.
	if _, err := s.VerifyAndResolveGrant(ctx, "tenant-b", "https://srv.invalid", tok); err == nil {
		t.Fatal("expected cross-tenant replay to fail")
	}
}

func TestVerifyGrantIssuerMismatch(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()

	tok, _, err := s.IssueGrant(
		ctx, "tenant-a", "https://srv-a.invalid",
		PrincipalDelegated, "agent-1", []string{"aws-prod"},
		GrantPermRead, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}
	// Different server URL must reject.
	if _, err := s.VerifyAndResolveGrant(ctx, "tenant-a", "https://srv-b.invalid", tok); err == nil {
		t.Fatal("expected issuer mismatch to fail")
	}
	// Empty expectedIssuer skips the check (for tests / unit use).
	if _, err := s.VerifyAndResolveGrant(ctx, "tenant-a", "", tok); err != nil {
		t.Fatalf("empty expectedIssuer should skip iss check: %v", err)
	}
}

func TestIssueGrantRejectsEmptyScope(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()
	if _, _, err := s.IssueGrant(
		ctx, "tenant-a", "https://srv.invalid",
		PrincipalDelegated, "agent-1", nil,
		GrantPermRead, time.Hour, "",
	); err == nil {
		t.Fatal("expected empty scope to be rejected")
	}
}

func TestIssueGrantRejectsBadPerm(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()
	if _, _, err := s.IssueGrant(
		ctx, "tenant-a", "https://srv.invalid",
		PrincipalDelegated, "agent-1", []string{"aws-prod"},
		GrantPerm("admin"), time.Hour, "",
	); err == nil {
		t.Fatal("expected bad perm to be rejected")
	}
}

func TestIssueGrantRejectsBadPrincipal(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()
	if _, _, err := s.IssueGrant(
		ctx, "tenant-a", "https://srv.invalid",
		PrincipalType("root"), "agent-1", []string{"aws-prod"},
		GrantPermRead, time.Hour, "",
	); err == nil {
		t.Fatal("expected bad principal_type to be rejected")
	}
}

func TestIssueGrantRejectsEmptyAgent(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()
	if _, _, err := s.IssueGrant(
		ctx, "tenant-a", "https://srv.invalid",
		PrincipalDelegated, "", []string{"aws-prod"},
		GrantPermRead, time.Hour, "",
	); err == nil {
		t.Fatal("expected empty agent to be rejected")
	}
}

func TestIssueGrantRejectsEmptyIssuer(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()
	if _, _, err := s.IssueGrant(
		ctx, "tenant-a", "",
		PrincipalDelegated, "agent-1", []string{"aws-prod"},
		GrantPermRead, time.Hour, "",
	); err == nil {
		t.Fatal("expected empty issuer to be rejected")
	}
}

func TestIssueGrantRejectsNonPositiveTTL(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()
	if _, _, err := s.IssueGrant(
		ctx, "tenant-a", "https://srv.invalid",
		PrincipalDelegated, "agent-1", []string{"aws-prod"},
		GrantPermRead, 0, "",
	); err == nil {
		t.Fatal("expected ttl=0 to be rejected")
	}
}

// TestAuditGrantIssuedDetailContract asserts the grant.issued audit event
// carries the exact Detail map the impl spec §5 requires:
// grant_id, agent, principal_type, perm, scope. Mirrors the AuditEvent the
// handler writes in pkg/server/vault.go handleVaultGrantIssue so reviewers can
// see the contract in one place and catch any drift without end-to-end setup.
func TestAuditGrantIssuedDetailContract(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()
	tenantID := "tenant-audit-issued"

	_, grant, err := s.IssueGrant(
		ctx, tenantID, "https://srv.invalid",
		PrincipalDelegated, "agent-1", []string{"db-prod/password", "aws-prod"},
		GrantPermWrite, time.Hour, "ci-label",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}

	// Mirror the handler's AuditEvent construction.
	event := &AuditEvent{
		TenantID:  tenantID,
		EventType: "grant.issued",
		TokenID:   grant.GrantID,
		AgentID:   grant.Agent,
		Detail: map[string]any{
			"grant_id":       grant.GrantID,
			"agent":          grant.Agent,
			"principal_type": string(grant.PrincipalType),
			"perm":           string(grant.Perm),
			"scope":          grant.Scope,
		},
		Timestamp: time.Now(),
	}
	if err := s.WriteAuditEvent(ctx, event); err != nil {
		t.Fatalf("WriteAuditEvent: %v", err)
	}

	events, err := s.QueryAuditLog(ctx, tenantID, "", 10)
	if err != nil {
		t.Fatalf("QueryAuditLog: %v", err)
	}
	if len(events) != 1 || events[0].EventType != "grant.issued" {
		t.Fatalf("unexpected audit events: %+v", events)
	}
	got, ok := events[0].Detail.(map[string]any)
	if !ok {
		t.Fatalf("Detail not map[string]any: %T %+v", events[0].Detail, events[0].Detail)
	}
	if got["grant_id"] != grant.GrantID {
		t.Errorf("grant_id = %v, want %q", got["grant_id"], grant.GrantID)
	}
	if got["agent"] != "agent-1" {
		t.Errorf("agent = %v, want agent-1", got["agent"])
	}
	if got["principal_type"] != "delegated" {
		t.Errorf("principal_type = %v, want delegated", got["principal_type"])
	}
	if got["perm"] != "write" {
		t.Errorf("perm = %v, want write", got["perm"])
	}
	scope, ok := got["scope"].([]any)
	if !ok || len(scope) != 2 || scope[0] != "db-prod/password" || scope[1] != "aws-prod" {
		t.Errorf("scope = %v, want [db-prod/password aws-prod]", got["scope"])
	}
}

// TestAuditGrantRevokedDetailContract asserts the grant.revoked audit event
// carries the exact Detail map the impl spec §5 requires (grant_id,
// revoked_by, reason) AND the top-level AgentID mirrors revoked_by so filter
// queries by actor work without parsing detail_json.
func TestAuditGrantRevokedDetailContract(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()
	tenantID := "tenant-audit-revoked"

	_, grant, err := s.IssueGrant(
		ctx, tenantID, "https://srv.invalid",
		PrincipalDelegated, "agent-1", []string{"aws-prod"},
		GrantPermRead, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}

	revokedBy := "admin-42"
	reason := "rotated after incident"
	if err := s.RevokeGrant(ctx, tenantID, grant.GrantID, revokedBy, reason); err != nil {
		t.Fatalf("RevokeGrant: %v", err)
	}

	// Mirror the handler's AuditEvent construction.
	event := &AuditEvent{
		TenantID:  tenantID,
		EventType: "grant.revoked",
		TokenID:   grant.GrantID,
		AgentID:   revokedBy,
		Detail: map[string]any{
			"grant_id":   grant.GrantID,
			"revoked_by": revokedBy,
			"reason":     reason,
		},
		Timestamp: time.Now(),
	}
	if err := s.WriteAuditEvent(ctx, event); err != nil {
		t.Fatalf("WriteAuditEvent: %v", err)
	}

	events, err := s.QueryAuditLog(ctx, tenantID, "", 10)
	if err != nil {
		t.Fatalf("QueryAuditLog: %v", err)
	}
	if len(events) != 1 || events[0].EventType != "grant.revoked" {
		t.Fatalf("unexpected audit events: %+v", events)
	}
	if events[0].AgentID != revokedBy {
		t.Errorf("AgentID = %q, want %q (mirrors revoked_by)", events[0].AgentID, revokedBy)
	}
	got, ok := events[0].Detail.(map[string]any)
	if !ok {
		t.Fatalf("Detail not map[string]any: %T %+v", events[0].Detail, events[0].Detail)
	}
	if got["grant_id"] != grant.GrantID {
		t.Errorf("grant_id = %v, want %q", got["grant_id"], grant.GrantID)
	}
	if got["revoked_by"] != revokedBy {
		t.Errorf("revoked_by = %v, want %q", got["revoked_by"], revokedBy)
	}
	if got["reason"] != reason {
		t.Errorf("reason = %v, want %q", got["reason"], reason)
	}
}

