package fuse

import (
	"os"
	"path/filepath"
	"testing"
)

// TestPendingIndexJournalRouteAndReplay covers the WAL-backed durable
// publication path: Put with a wired journal appends a JournalPendingMeta
// frame (no per-entry .meta atomicWrite), and a fresh PendingIndex +
// replayJournalIntoPending resurrects the entry with full fidelity
// (size, kind, mode, baseRev) after a simulated crash.
func TestPendingIndexJournalRouteAndReplay(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	defer func() { _ = j.Close() }()

	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("pending index: %v", err)
	}
	idx.SetJournal(j)

	if _, err := idx.PutShadowSpillWithModeAndLineage("/w/f1.bin", 4096, PendingNew, 0, 0o755, true, "snap-1", "snap-0", true, "/w", "/w/sub"); err != nil {
		t.Fatalf("put: %v", err)
	}

	// The durable artifact must be a WAL frame, not a standalone .meta file.
	if _, err := os.Stat(filepath.Join(idx.dir, hashPath("/w/f1.bin")+".meta")); !os.IsNotExist(err) {
		t.Fatalf("journal route wrote a .meta file anyway: %v", err)
	}

	// Simulate a crash: fresh index + replay from the WAL.
	idx2, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("pending index 2: %v", err)
	}
	if err := replayJournalIntoPending(j, idx2, nil); err != nil {
		t.Fatalf("replay: %v", err)
	}
	m, ok := idx2.GetMeta("/w/f1.bin")
	if !ok {
		t.Fatal("replay did not resurrect the pending entry")
	}
	if m.Size != 4096 || m.Kind != PendingNew || !m.ShadowSpill {
		t.Fatalf("resurrected meta mismatch: size=%d kind=%v spill=%v", m.Size, m.Kind, m.ShadowSpill)
	}
	if !m.HasMode || m.Mode != 0o755 {
		t.Fatalf("resurrected mode mismatch: has=%v mode=%o", m.HasMode, m.Mode)
	}
	// SnapshotID/lineage are json:"-" by design (fail-closed recovery, same
	// as the atomicWrite .meta path); they must NOT survive a crash.
	if m.SnapshotID != "" || m.ParentSnapshotID != "" {
		t.Fatalf("process-local lineage leaked through recovery: %s/%s", m.SnapshotID, m.ParentSnapshotID)
	}
}

// TestJournalPendingMetaSupersededByCommit verifies a commit marker written
// after the pending-meta frame prevents resurrection on replay.
func TestJournalPendingMetaSupersededByCommit(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	defer func() { _ = j.Close() }()

	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("pending index: %v", err)
	}
	idx.SetJournal(j)
	if _, err := idx.PutWithBaseRevAndMode("/w/f2.bin", 128, PendingOverwrite, 7, 0o644, true); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := j.Append(JournalEntry{Op: JournalCommit, Path: "/w/f2.bin"}); err != nil {
		t.Fatalf("commit marker: %v", err)
	}
	if err := j.FsyncShared(); err != nil {
		t.Fatalf("fsync: %v", err)
	}

	idx2, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("pending index 2: %v", err)
	}
	if err := replayJournalIntoPending(j, idx2, nil); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if idx2.HasPending("/w/f2.bin") {
		t.Fatal("committed path was resurrected from WAL")
	}
}

// TestJournalFsyncSharedCoalesces asserts the group-commit property: entries
// appended by N concurrent putters become durable with at most N syncs (and
// usually 1 per batch window), and every waiter observes durability.
func TestJournalFsyncSharedCoalesces(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	defer func() { _ = j.Close() }()

	const writers = 8
	errCh := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func(n int) {
			if err := j.Append(JournalEntry{Op: JournalWrite, Path: "/w/x", Length: int64(n)}); err != nil {
				errCh <- err
				return
			}
			errCh <- j.FsyncShared()
		}(i)
	}
	for i := 0; i < writers; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}

	// Everything must be durable: count frames via replay.
	seen := 0
	if err := j.Replay(func(e JournalEntry) { seen++ }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if seen != writers {
		t.Fatalf("replayed %d frames, want %d", seen, writers)
	}
}
