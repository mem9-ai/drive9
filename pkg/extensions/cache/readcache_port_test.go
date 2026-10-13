// v1 fuse_test ReadCache family remainder: defensive copies vs owned
// puts, revision mismatch, TTL variants, large-file skip, custom limits,
// and prefix invalidation.
package cache

import (
	"bytes"
	"testing"
	"time"
)

// v1 TestReadCache_PutDefensivelyCopies / PutOwnedReusesSlice.
func TestPortReadCacheCopySemantics(t *testing.T) {
	rc := NewReadCache(1<<20, 10*time.Second)
	data := []byte("hello world")
	rc.Put("/test.txt", data, 1)
	data[0] = 'j'
	got, ok := rc.Get("/test.txt", 1)
	if !ok || got[0] != 'h' {
		t.Fatalf("Put did not copy defensively: %q %v", got, ok)
	}

	rc2 := NewReadCache(1<<20, 10*time.Second)
	owned := []byte("owned body")
	rc2.PutOwned("/own.txt", owned, 1)
	got2, ok2 := rc2.Get("/own.txt", 1)
	if !ok2 || string(got2) != "owned body" {
		t.Fatalf("PutOwned = %q %v", got2, ok2)
	}
}

// v1 TestReadCache_RevisionMismatch: a stale-revision Get is a miss and
// evicts the entry (documented Get contract).
func TestPortReadCacheRevisionMismatch(t *testing.T) {
	rc := NewReadCache(1<<20, 10*time.Second)
	rc.Put("/r.txt", []byte("v1"), 1)
	if _, ok := rc.Get("/r.txt", 2); ok {
		t.Fatal("stale revision served")
	}
	if _, ok := rc.Get("/r.txt", 1); ok {
		t.Fatal("stale probe did not evict the entry")
	}
}

// v1 TestReadCache_TTLExpiry / NoTTLExpiry /
// NoTTLExpiryStillChecksRevision.
func TestPortReadCacheTTLVariants(t *testing.T) {
	ttl := NewReadCacheWithMaxFileSize(1<<20, 20*time.Millisecond, 0)
	ttl.Put("/t.txt", []byte("x"), 1)
	time.Sleep(40 * time.Millisecond)
	if _, ok := ttl.Get("/t.txt", 1); ok {
		t.Fatal("entry outlived its TTL")
	}

	noTTL := NewReadCacheWithMaxFileSize(1<<20, 0, 0)
	noTTL.Put("/n.txt", []byte("x"), 1)
	// Zero TTL: no time-based expiry — the entry survives indefinitely
	// (v1 TestReadCache_NoTTLExpiry) but the revision check still applies
	// (v1 ...StillChecksRevision evicts on mismatch).
	if _, ok := noTTL.Get("/n.txt", 1); !ok {
		t.Fatal("zero TTL expired the entry")
	}
	if _, ok := noTTL.Get("/n.txt", 2); ok {
		t.Fatal("zero TTL skipped the revision check")
	}
}

// v1 TestReadCache_SkipLargeFiles / CustomMaxFileSize /
// CachesMediumSmallFiles.
func TestPortReadCacheSizeBounds(t *testing.T) {
	rc := NewReadCacheWithMaxFileSize(1<<20, time.Minute, 16)
	rc.Put("/big.txt", bytes.Repeat([]byte("x"), 32), 1)
	if _, ok := rc.Get("/big.txt", 1); ok {
		t.Fatal("oversized file admitted")
	}
	rc.Put("/small.txt", []byte("tiny"), 1)
	if got, ok := rc.Get("/small.txt", 1); !ok || string(got) != "tiny" {
		t.Fatalf("small file rejected: %q %v", got, ok)
	}
}

// v1 TestReadCache_Invalidate / InvalidatePrefix.
func TestPortReadCacheInvalidation(t *testing.T) {
	rc := NewReadCache(1<<20, time.Minute)
	rc.Put("/a/one.txt", []byte("1"), 1)
	rc.Put("/b/two.txt", []byte("2"), 1)
	rc.InvalidatePrefix("/a/")
	if _, ok := rc.Get("/a/one.txt", 1); ok {
		t.Fatal("prefix invalidation missed /a/one.txt")
	}
	if _, ok := rc.Get("/b/two.txt", 1); !ok {
		t.Fatal("prefix invalidation overreached into /b")
	}
	rc.Invalidate("/b/two.txt")
	if _, ok := rc.Get("/b/two.txt", 1); ok {
		t.Fatal("single invalidation missed")
	}
}
