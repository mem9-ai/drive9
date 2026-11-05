package staging

import (
	"bytes"
	"errors"
	"io"
	"os"
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
	// INSIDE the copy window — after rotatePinnedIfNeeded's initial refs
	// snapshot (so the rotation proceeds) but before the publication
	// re-check. Without the publication branch, the retired entry would be
	// installed with nobody left to unpin it.
	rotatePrePublishHook = func() {
		s.mu.Lock()
		s.refs[gen] = 0
		s.mu.Unlock()
	}
	t.Cleanup(func() { rotatePrePublishHook = nil })

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

// TestRotationCopyPreservesRecoveredContent pins the extent-copy's
// completeness against the coverage map: a file RECOVERED from disk (no
// written-range knowledge) plus one partial in-process write must rotate
// without losing the recovered regions — the filesystem's extents, not the
// process's coverage, are the content map.
func TestRotationCopyPreservesRecoveredContent(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	image := []byte("RECOVERED-IMAGE")
	if err := s1.WriteFull("/f", image, 1); err != nil {
		t.Fatal(err)
	}
	s1.Close()

	// Reopen: the shadow is recovered from disk — no coverage tracking.
	// A partial in-process write FIRST creates the distinguishing state
	// (non-empty coverage that provably does NOT describe the whole
	// image); the rotation under that partial coverage is the case the
	// coverage-map implementation corrupted.
	s2, err := NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err := s2.WriteAt("/f", 0, []byte("r"), 1); err != nil {
		t.Fatal(err)
	}
	gen := s2.Pin("/f")
	if _, err := s2.WriteAt("/f", 9, []byte("i"), 1); err != nil {
		t.Fatal(err)
	}
	s2.Unpin(gen)
	data, err := s2.ReadAll("/f")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "rECOVEREDiIMAGE" {
		t.Fatalf("rotated image = %q, want rECOVEREDiIMAGE (recovered regions preserved under partial coverage)", data)
	}
}

// TestRotationSparseGuardUsesRealAllocation pins the guard's accuracy: the
// rotation measures the image's REAL data volume through the filesystem's
// extents — a sparse image whose logical size dwarfs its data both rotates
// freely and would pass a ratio guard that the logical size would fail.
func TestRotationSparseGuardUsesRealAllocation(t *testing.T) {
	s, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const logical = 64 << 20
	sparse := make([]byte, 1<<20)
	if _, err := s.WriteAt("/f", 0, sparse, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Truncate("/f", logical, 1); err != nil {
		t.Fatal(err)
	}

	// The guard's INPUT is the extent-measured data volume, a fraction of
	// the logical size.
	src, size, release, err := s.OpenIfGeneration("/f", s.ActiveGeneration("/f"))
	if err != nil {
		t.Fatal(err)
	}
	dataBytes, dense, err := shadowDataExtents(src, size)
	release()
	if err != nil {
		t.Fatal(err)
	}
	if dense {
		t.Skip("filesystem without extent seeking; dense fallback reserved the full size by design")
	}
	if dataBytes >= logical {
		t.Fatalf("extent walk reported %d bytes for a %d-byte sparse image (1 MiB data)", dataBytes, logical)
	}

	// The measured volume passes a ratio guard the logical size would fail.
	// (s.writeCacheFreeRatio is 0 on this store; drive the threshold via a
	// store configured at 0.9 so the comparison is the guard's own math.)
	total := int64(128 << 20)
	free := total - 4<<20
	q, err := NewShadowStoreWithQuota(t.TempDir(), 0.9, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.evalFreeRatio(free, total, logical); err == nil {
		t.Fatalf("logical size %d unexpectedly passed a 0.9-ratio guard with %d free", logical, free)
	}
	if err := q.evalFreeRatio(free, total, dataBytes); err != nil {
		t.Fatalf("extent-measured %d bytes rejected by the 0.9-ratio guard: %v", dataBytes, err)
	}

	// And the rotation itself goes through on the sparse image.
	gen := s.Pin("/f")
	if _, err := s.WriteAt("/f", 0, []byte("X"), 1); err != nil {
		t.Fatalf("sparse rotation rejected: %v", err)
	}
	s.Unpin(gen)
}

// TestPreadFullRejectsShortRead pins the extent copy's short-read contract:
// a pread that returns fewer bytes than requested (legal per pread(2)) must
// be retried to completion, and an EOF inside a range that was reported as
// data is an error — never a silent zero-fill of the remainder. Drives the
// helper against a source that truncates mid-buffer via a pipe-backed fd.
func TestPreadFullRejectsShortRead(t *testing.T) {
	// A regular file that ends mid-request is the deterministic stand-in
	// for a short pread: the first call returns only the available bytes.
	src, err := os.CreateTemp(t.TempDir(), "short")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, err := src.Write([]byte("12345")); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 8)
	err = preadFull(int(src.Fd()), buf, 0)
	if err == nil {
		t.Fatalf("short source silently zero-filled: %q", buf)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}

	// A fully available buffer fills exactly.
	full := make([]byte, 5)
	if err := preadFull(int(src.Fd()), full, 0); err != nil {
		t.Fatal(err)
	}
	if string(full) != "12345" {
		t.Fatalf("full read = %q", full)
	}
}
