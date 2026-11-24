package staging

import (
	"path/filepath"
	"testing"
)

// TestAppendSyncMakesMarkerDurable pins the difference between Append and
// AppendSync: a marker frame that fences earlier frames has to be on disk
// before the caller drops the state those frames would replay.
func TestAppendSyncMakesMarkerDurable(t *testing.T) {
	j, err := NewJournal(filepath.Join(t.TempDir(), "marker.wal"))
	if err != nil {
		t.Fatal(err)
	}

	if err := j.Append(JournalEntry{Op: JournalWrite, Path: "/a", Length: 10}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := j.AppendSync(JournalEntry{Op: JournalCommit, Path: "/a"}); err != nil {
		t.Fatalf("append sync: %v", err)
	}
	if got, want := j.DurableSeq(), uint64(2); got != want {
		t.Fatalf("DurableSeq after AppendSync = %d, want %d (the marker must be durable)", got, want)
	}

	// A barrier that cannot reach the disk must report failure instead of
	// claiming durability: after Close the frame cannot be written at all.
	if err := j.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := j.AppendSync(JournalEntry{Op: JournalCommit, Path: "/b"}); err == nil {
		t.Fatal("AppendSync on a closed journal = nil, want an error")
	}
}
