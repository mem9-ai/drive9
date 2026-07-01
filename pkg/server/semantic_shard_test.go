package server

import (
	"testing"
)

func TestJumpConsistentHash(t *testing.T) {
	t.Parallel()
	// Same key + same bucket count → same bucket.
	for _, key := range []uint64{0, 1, 42, 1000, 1<<33, ^uint64(0)} {
		a := jumpConsistentHash(key, 10)
		b := jumpConsistentHash(key, 10)
		if a != b {
			t.Fatalf("jumpConsistentHash not deterministic: key=%d got %d then %d", key, a, b)
		}
		if a < 0 || a >= 10 {
			t.Fatalf("jumpConsistentHash out of range: key=%d bucket=%d", key, a)
		}
	}
}

func TestJumpConsistentHashDistribution(t *testing.T) {
	t.Parallel()
	const numBuckets = 10
	const numKeys = 100000
	counts := make([]int, numBuckets)
	for i := 0; i < numKeys; i++ {
		bucket := jumpConsistentHash(tenantHashKey(tenantIDForIndex(i)), numBuckets)
		if bucket < 0 || bucket >= numBuckets {
			t.Fatalf("bucket out of range: %d", bucket)
		}
		counts[bucket]++
	}
	// Each bucket should get ~10% of keys. Allow ±50% tolerance for this small test.
	expected := numKeys / numBuckets
	for i, c := range counts {
		if c < expected/2 || c > expected*2 {
			t.Fatalf("bucket %d count %d outside expected range %d±50%%", i, c, expected)
		}
	}
}

func TestTenantHashKeyDeterministic(t *testing.T) {
	t.Parallel()
	a := tenantHashKey("tenant-abc")
	b := tenantHashKey("tenant-abc")
	if a != b {
		t.Fatal("tenantHashKey not deterministic")
	}
	c := tenantHashKey("tenant-def")
	if a == c {
		t.Fatal("tenantHashKey collision for different inputs")
	}
}

func TestShardResolverSinglePod(t *testing.T) {
	t.Parallel()
	// No meta store → single-pod mode → owns everything.
	r := newSemanticShardResolver(nil, "pod1", 0)
	if !r.ownsTenant("tenant1") {
		t.Fatal("single-pod resolver should own all tenants")
	}
	if !r.ownsTenant("") {
		t.Fatal("single-pod resolver should own even empty tenant")
	}
}

func TestShardResolverOwnsTenantFn(t *testing.T) {
	t.Parallel()
	r := newSemanticShardResolver(nil, "", 0)
	fn := r.ownsTenantFn()
	if fn == nil {
		t.Fatal("ownsTenantFn should not be nil")
	}
	if !fn("any-tenant") {
		t.Fatal("single-pod ownsTenantFn should own everything")
	}
}

func tenantIDForIndex(i int) string {
	return "tenant-" + string(rune('A'+i%26)) + string(rune('A'+(i/26)%26))
}