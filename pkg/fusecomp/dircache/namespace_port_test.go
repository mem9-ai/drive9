// v1 fuse_test NamespaceCache family: positive/negative answering
// classes, escalation threshold/window, and the large-directory
// completeness guard.
package dircache

import (
	"fmt"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

func info(name string) fs.DirEntryInfo {
	return fs.DirEntryInfo{Name: name, Size: 1}
}

func infoN(i int) fs.DirEntryInfo {
	return fs.DirEntryInfo{Name: fmt.Sprintf("f%04d", i), Size: 1}
}

// v1 TestNamespaceCache_PositiveHit / CompleteMissIsSafeNegative /
// PartialMissFallsThrough / SessionCreatedMissIsSafeNegative.
func TestPortNamespaceLookupClasses(t *testing.T) {
	dc := NewNamespaceCache(time.Second, time.Second, 0)
	dc.Upsert("/", info("alpha.txt"))
	if _, kind := dc.LookupNamespace("/", "alpha.txt"); kind != fs.LookupPositive {
		t.Fatalf("alpha kind = %v, want positive", kind)
	}
	// A seeded negative row answers safe-negative.
	dc.MarkNegative("/", "gone.txt")
	if _, kind := dc.LookupNamespace("/", "gone.txt"); kind != fs.LookupNegative {
		t.Fatalf("gone kind = %v, want negative", kind)
	}
	// A session-created dir answers session-miss for unknown names.
	dc.MarkSessionCreatedDir("/sess")
	if _, kind := dc.LookupNamespace("/sess", "unknown"); kind != fs.LookupSessionMiss {
		t.Fatalf("session kind = %v, want session miss", kind)
	}
	// A partial-miss state (an upserted row without completeness) falls
	// through for other names.
	dc.Upsert("/part", info("one.txt"))
	if _, kind := dc.LookupNamespace("/part", "other.txt"); kind != fs.LookupPartialMiss {
		t.Fatalf("partial kind = %v, want partial miss (falls to remote)", kind)
	}
}

// v1 TestNamespaceCache_RecordRemoteNegativeEscalatesAtThreshold /
// WindowExpiryResets.
func TestPortNamespaceEscalationWindow(t *testing.T) {
	dc := NewNamespaceCache(time.Second, time.Second, 0)
	// First two misses do not escalate (threshold 3).
	if dc.RecordRemoteNegative("/dir") {
		t.Fatal("first miss escalated")
	}
	if dc.RecordRemoteNegative("/dir") {
		t.Fatal("second miss escalated")
	}
	if !dc.RecordRemoteNegative("/dir") {
		t.Fatal("third miss did not escalate")
	}
	// The escalation cooldown suppresses an immediate re-escalation.
	if dc.RecordRemoteNegative("/dir") {
		t.Fatal("cooldown window re-escalated")
	}
}

// v1 TestNamespaceCache_NegativeExpires / CompleteMissExpiresWithTTL /
// NegativeEntriesExpireAtNegativeTTL.
func TestPortNamespaceNegativeExpiry(t *testing.T) {
	dc := NewNamespaceCache(time.Second, 50*time.Millisecond, 0)
	dc.MarkNegative("/", "tmp.txt")
	if _, kind := dc.LookupNamespace("/", "tmp.txt"); kind != fs.LookupNegative {
		t.Fatalf("kind before expiry = %v", kind)
	}
	// The cache reads a wall clock; sleep past the 50ms negative TTL.
	time.Sleep(80 * time.Millisecond)
	if _, kind := dc.LookupNamespace("/", "tmp.txt"); kind == fs.LookupNegative {
		t.Fatal("negative row outlived its TTL")
	}
}

// v1 TestNamespaceCache_LargeDirGuardDoesNotMarkComplete /
// LargeDirCachesCompleteWithHighMaxEntries: an oversized listing is not
// marked complete, a raised limit is.
func TestPortNamespaceLargeDirGuard(t *testing.T) {
	big := make([]fs.DirEntryInfo, 0, 3000)
	for i := 0; i < 3000; i++ {
		big = append(big, infoN(i))
	}
	dc := NewNamespaceCache(time.Second, time.Second, 2000)
	dc.Put("/", big)
	if _, kind := dc.LookupNamespace("/", "missing-row"); kind != fs.LookupPartialMiss {
		t.Fatalf("oversized kind = %v, want partial miss (not complete)", kind)
	}

	dcCap := NewNamespaceCache(time.Second, time.Second, 4096)
	dcCap.Put("/", big)
	if _, kind := dcCap.LookupNamespace("/", "missing-row"); kind != fs.LookupCompleteMiss {
		t.Fatalf("capped kind = %v, want complete miss", kind)
	}
}
