package staging

import (
	"bytes"
	"testing"
)

// TestRotationPreservesPartialUpdateSemantics pins the rotation primitive's
// content contract: a pinned image rotated for a partial update keeps the
// untouched bytes. WriteAt and Truncate are partial-update APIs; a rotation
// that left an empty replacement published zeros as a successful image.
func TestRotationPreservesPartialUpdateSemantics(t *testing.T) {
	s, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.WriteFull("/f", []byte("abcdef"), 1); err != nil {
		t.Fatal(err)
	}
	gen := s.Pin("/f")

	if _, err := s.WriteAt("/f", 1, []byte("X"), 1); err != nil {
		t.Fatal(err)
	}
	data, err := s.ReadAll("/f")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "aXcdef" {
		t.Fatalf("active image after pinned WriteAt = %q, want aXcdef", data)
	}
	// The pinned generation still sees the ORIGINAL image.
	pin := make([]byte, 6)
	if n, err := s.ReadAtGen(gen, 0, pin); err != nil || n != 6 || string(pin) != "abcdef" {
		t.Fatalf("pinned generation = %q n=%d err=%v, want abcdef", pin[:n], n, err)
	}

	if err := s.Truncate("/f", 3, 1); err != nil {
		t.Fatal(err)
	}
	data, err = s.ReadAll("/f")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "aXc" {
		t.Fatalf("active image after pinned Truncate = %q, want aXc", data)
	}
	pin2 := make([]byte, 6)
	if n, err := s.ReadAtGen(gen, 0, pin2); err != nil || n != 6 || string(pin2) != "abcdef" {
		t.Fatalf("pinned generation after truncate = %q n=%d err=%v", pin2[:n], n, err)
	}
	s.Unpin(gen)
}

// TestRotationRejectedReplacementKeepsOldImage pins the failure ordering:
// with a pinned acknowledged image, a replacement that fails the byte quota
// must leave the image, its generation, and the pending counter intact — the
// rotation only runs after the quota gates.
func TestRotationRejectedReplacementKeepsOldImage(t *testing.T) {
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	image := bytes.Repeat([]byte("z"), 1<<20)
	if err := s.WriteFull("/f", image, 1); err != nil {
		t.Fatal(err)
	}
	before := s.PendingBytes()
	gen := s.Pin("/f")

	// A replacement twice the size under the 1 MiB byte quota.
	err = s.WriteFull("/f", bytes.Repeat([]byte("y"), 2<<20), 1)
	if err == nil {
		t.Fatal("oversized replacement unexpectedly succeeded under the quota")
	}
	data, rerr := s.ReadAll("/f")
	if rerr != nil || !bytes.Equal(data, image) {
		t.Fatalf("rejected replacement changed the image: len=%d err=%v", len(data), rerr)
	}
	pin := make([]byte, len(image))
	if n, err := s.ReadAtGen(gen, 0, pin); err != nil || n != len(image) {
		t.Fatalf("pinned generation broken after rejected replacement: n=%d err=%v", n, err)
	}
	if got := s.PendingBytes(); got != before {
		t.Fatalf("pending bytes after rejected replacement = %d, want %d", got, before)
	}
	s.Unpin(gen)
}

// TestRotationAccounting pins the pending-byte ledger across a full
// rotation cycle: pin, rotate via replacement, unpin the old generation,
// remove the replacement — the counter returns to zero (the retired copy's
// bytes are debited at the final Unpin, not leaked).
func TestRotationAccounting(t *testing.T) {
	s, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	image := bytes.Repeat([]byte("q"), 4096)
	if err := s.WriteFull("/f", image, 1); err != nil {
		t.Fatal(err)
	}
	gen := s.Pin("/f")
	if err := s.WriteFull("/f", image, 1); err != nil {
		t.Fatal(err)
	}
	s.Unpin(gen)
	s.Remove("/f")
	if got := s.PendingBytes(); got != 0 {
		t.Fatalf("pending bytes after rotation cycle = %d, want 0", got)
	}
}

// TestRotationRacedLastUnpinCleansUp pins the publication re-check: when the
// pinned generation's LAST Unpin runs during the rotation's copy window (the
// refs count hits zero before the retired entry is published), the rotation
// itself must clean the retired original up — no orphaned fd, no leaked
// pending bytes — because nobody will ever call Unpin again.
func TestRotationRacedLastUnpinCleansUp(t *testing.T) {
	s, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	image := []byte("race-image")
	if err := s.WriteFull("/f", image, 1); err != nil {
		t.Fatal(err)
	}
	gen := s.Pin("/f")
	before := s.PendingBytes()
	// Simulate the race deterministically: the last reader drops its ref
	// during the copy window, before publication.
	s.mu.Lock()
	s.refs[gen] = 0
	s.mu.Unlock()

	if _, err := s.WriteAt("/f", 0, []byte("RACE-image"), 1); err != nil {
		t.Fatal(err)
	}
	if got := s.PendingBytes(); got != before {
		t.Fatalf("pending bytes after raced rotation = %d, want unchanged %d", got, before)
	}
	s.mu.RLock()
	_, retired := s.retired[gen]
	s.mu.RUnlock()
	if retired {
		t.Fatal("raced rotation published a retired entry nobody will unpin")
	}
	s.Remove("/f")
	if got := s.PendingBytes(); got != 0 {
		t.Fatalf("pending bytes after remove = %d, want 0", got)
	}
}

// TestRotationQuotaBytesTransfer pins the byte-quota coverage transfer: a
// full pin→replace→unpin→remove cycle must release the byte quota entirely —
// the replacement inherits the old written coverage (no double charge) and
// the retired original transfers its charge away (no leak).
func TestRotationQuotaBytesTransfer(t *testing.T) {
	const n = 1 << 20
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, n)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	image := make([]byte, n)
	if err := s.WriteFull("/f", image, 1); err != nil {
		t.Fatal(err)
	}
	gen := s.Pin("/f")
	if err := s.WriteFull("/f", image, 1); err != nil {
		t.Fatalf("same-size replacement under a %d-byte quota: %v", n, err)
	}
	s.Unpin(gen)
	s.Remove("/f")

	// The quota must be fully released: a fresh same-size write succeeds.
	if err := s.WriteFull("/g", image, 1); err != nil {
		t.Fatalf("quota leaked after rotation cycle: %v", err)
	}
}
