package server

import (
	"context"
	"testing"
	"time"

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

func TestSafetyNetScanIntervalZeroDisables(t *testing.T) {
	// A zero SafetyNetScanInterval must be preserved as disabled instead of
	// falling back to the old 5min default — the default now lives in the
	// server binaries' env parsing, not in NewWithConfig. A configured
	// interval passes through untouched.
	s := NewWithConfig(Config{})
	if s.safetyNetScanInterval != 0 {
		t.Fatalf("expected zero SafetyNetScanInterval to be preserved (disabled), got %v", s.safetyNetScanInterval)
	}
	s = NewWithConfig(Config{SafetyNetScanInterval: 30 * time.Minute})
	if s.safetyNetScanInterval != 30*time.Minute {
		t.Fatalf("expected configured interval to be preserved, got %v", s.safetyNetScanInterval)
	}
}
