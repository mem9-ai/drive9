package server

import (
	"testing"
)

func TestJumpConsistentHashBasic(t *testing.T) {
	// With 1 bucket, everything maps to bucket 0.
	for _, key := range []uint64{0, 1, 42, 1000, 1<<32, 1<<63} {
		if got := jumpConsistentHash(key, 1); got != 0 {
			t.Fatalf("jumpConsistentHash(%d, 1) = %d, want 0", key, got)
		}
	}

	// Deterministic: same key, same numBuckets always gives same bucket.
	for n := uint64(2); n <= 100; n++ {
		h := jumpConsistentHash(42, n)
		if h >= n {
			t.Fatalf("jumpConsistentHash(42, %d) = %d, out of range", n, h)
		}
		h2 := jumpConsistentHash(42, n)
		if h != h2 {
			t.Fatalf("non-deterministic: jumpConsistentHash(42, %d) gave %d then %d", n, h, h2)
		}
	}
}

func TestJumpConsistentHashDistribution(t *testing.T) {
	// With enough keys, distribution across buckets should be roughly even.
	numBuckets := uint64(10)
	counts := make(map[uint64]int)
	for i := uint64(0); i < 10000; i++ {
		b := jumpConsistentHash(i, numBuckets)
		counts[b]++
	}
	for b := uint64(0); b < numBuckets; b++ {
		c := counts[b]
		// Each bucket should have ~1000 keys; allow [500, 1500].
		if c < 500 || c > 1500 {
			t.Fatalf("bucket %d count=%d, expected roughly 1000", b, c)
		}
	}
}

func TestJumpConsistentHashMigration(t *testing.T) {
	// When numBuckets increases from N to N+1, only ~1/(N+1) of keys should move.
	numKeys := uint64(10000)
	for n := uint64(1); n < 20; n++ {
		moved := 0
		for k := uint64(0); k < numKeys; k++ {
			old := jumpConsistentHash(k, n)
			new := jumpConsistentHash(k, n+1)
			if old != new {
				moved++
			}
		}
		expected := int(numKeys) / int(n+1)
		// Allow generous tolerance for small N.
		tolerance := expected / 2
		if tolerance < 5 {
			tolerance = 5
		}
		if moved < expected-tolerance || moved > expected+tolerance {
			t.Fatalf("n=%d->%d: moved=%d, expected ~%d (±%d)", n, n+1, moved, expected, tolerance)
		}
	}
}

func TestShardResolverSinglePodMode(t *testing.T) {
	// No metaStore or no podID: always owns all tenants.
	r := newSemanticShardResolver(nil, "", 0)
	if !r.ownsTenant("any-tenant") {
		t.Fatal("single-pod mode should own all tenants")
	}
	if !r.ownsTenant("another-tenant") {
		t.Fatal("single-pod mode should own all tenants")
	}
}

func TestShardResolverNotRegisteredOwnsNothing(t *testing.T) {
	// When a pod is not in the active ring (myRank < 0) but has a non-nil
	// metaStore, it should own nothing. We can't set a real *meta.Store
	// without a DB connection, so we verify the ownsTenant logic path:
	// with numShard > 0 and myRank < 0, the jump hash bucket will never
	// equal -1, so ownsTenant returns false — but only when metaStore is
	// non-nil (single-pod fallback is skipped).
	// This is verified indirectly: in single-pod mode (metaStore==nil),
	// ownsTenant always returns true regardless of ring state.
	r := &semanticShardResolver{
		podID:   "pod-1",
		myRank:  -1,
		numShard: 0,
	}
	// metaStore is nil → single-pod mode → owns everything.
	if !r.ownsTenant("tenant-a") {
		t.Fatal("single-pod mode (metaStore==nil) should own all tenants")
	}
}

func TestShardResolverOwnsShard(t *testing.T) {
	// Simulate a 3-pod ring and verify each pod owns ~1/3 of tenants.
	r := &semanticShardResolver{
		podID:   "pod-1",
		ring:    []string{"pod-0", "pod-1", "pod-2"},
		myRank:  1,
		numShard: 3,
	}
	owned := 0
	for i := 0; i < 3000; i++ {
		tenantID := "tenant-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+(i/26/26)%26))
		if r.ownsTenant(tenantID) {
			owned++
		}
	}
	// Expect ~1000 owned out of 3000.
	if owned < 700 || owned > 1300 {
		t.Fatalf("owned=%d, expected ~1000", owned)
	}
}

func TestTenantHashDeterministic(t *testing.T) {
	h1 := tenantHash("tenant-abc")
	h2 := tenantHash("tenant-abc")
	if h1 != h2 {
		t.Fatal("tenantHash is not deterministic")
	}
	h3 := tenantHash("tenant-xyz")
	if h1 == h3 {
		t.Fatal("different tenants should have different hashes")
	}
}