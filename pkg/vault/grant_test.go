package vault

import (
	"context"
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

func TestIssueAndVerifyGrantOwner(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()

	tok, grant, err := s.IssueGrant(
		ctx, "tenant-a", "https://srv.invalid",
		PrincipalOwner, "agent-1", []string{"aws-prod"},
		GrantPermRead, time.Hour, "",
	)
	if err != nil {
		t.Fatalf("IssueGrant: %v", err)
	}
	if grant.GrantID == "" {
		t.Fatal("grant_id should be set")
	}

	claims, err := s.VerifyAndResolveGrant(ctx, "tenant-a", "https://srv.invalid", tok)
	if err != nil {
		t.Fatalf("VerifyAndResolveGrant: %v", err)
	}
	if claims.PrincipalType != PrincipalOwner {
		t.Fatalf("principal_type: got %q", claims.PrincipalType)
	}
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
		PrincipalOwner, "agent-1", []string{"aws-prod"},
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
	if err := s.RevokeGrant(ctx, "tenant-a", grant.GrantID, "admin", "again"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound on second revoke, got %v", err)
	}
}

func TestVerifyGrantCrossTenantReplay(t *testing.T) {
	s := newGrantTestStore(t)
	ctx := context.Background()

	tok, _, err := s.IssueGrant(
		ctx, "tenant-a", "https://srv.invalid",
		PrincipalOwner, "agent-1", []string{"aws-prod"},
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
		PrincipalOwner, "agent-1", []string{"aws-prod"},
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
		PrincipalOwner, "agent-1", nil,
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
		PrincipalOwner, "agent-1", []string{"aws-prod"},
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
		PrincipalOwner, "", []string{"aws-prod"},
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
		PrincipalOwner, "agent-1", []string{"aws-prod"},
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
		PrincipalOwner, "agent-1", []string{"aws-prod"},
		GrantPermRead, 0, "",
	); err == nil {
		t.Fatal("expected ttl=0 to be rejected")
	}
}
