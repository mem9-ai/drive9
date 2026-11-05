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
