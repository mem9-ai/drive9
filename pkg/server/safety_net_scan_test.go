package server

import (
	"context"
	"testing"

	"github.com/mem9-ai/drive9/pkg/meta"
)

func TestSafetyNetScanNilSafe(t *testing.T) {
	// A nil-meta or nil-pool server should return immediately without panic.
	s := &Server{}
	s.safetyNetScan(context.Background())
}

func TestSafetyNetScanContextCancelled(t *testing.T) {
	// A cancelled context should return immediately without panic.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &Server{}
	s.safetyNetScan(ctx)
}

func TestSafetyNetScanSkipsColdTenants(t *testing.T) {
	// Verify that AcquireCached returns false for a tenant not in the pool,
	// so the safety-net scan never opens a cold tenant TiDB.
	pool := newTestTenantPoolWithLeaderChecker(t, backendOptionsWithFileGC(), nil)
	// No tenants inserted — AcquireCached should return false.
	_, _, ok := pool.AcquireCached(&meta.Tenant{
		ID:     "nonexistent-tenant",
		Status: meta.TenantActive,
	})
	if ok {
		t.Fatal("AcquireCached should return false for a tenant not in the pool cache")
	}
}

func TestSafetyNetScanSkipsNilTenant(t *testing.T) {
	// AcquireCached with a nil tenant should return false, not panic.
	pool := newTestTenantPoolWithLeaderChecker(t, backendOptionsWithFileGC(), nil)
	_, _, ok := pool.AcquireCached(nil)
	if ok {
		t.Fatal("AcquireCached should return false for nil tenant")
	}
}
