package fuse

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLazyStagingSurvivesDaemonRestart covers the daemon-crash path of lazy
// staging (issue #964): with SetJournalSyncOnPut(false), Put appends a WAL
// frame without fsyncing; killing the Journal (daemon crash) and reopening it
// must still replay the entry, because the frame data sits in the kernel
// page cache of the local volume.
func TestLazyStagingSurvivesDaemonRestart(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "j.wal"))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	idx.SetJournal(j)
	idx.SetJournalSyncOnPut(false)

	if _, err := idx.PutShadowSpillWithModeAndLineage("/w/l1", 1024, PendingNew, 0, 0o644, true, "", "", true); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Daemon crash: close without fsync (data is in the page cache).
	_ = j.Close()

	j2, err := NewJournal(filepath.Join(dir, "j.wal"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = j2.Close() })
	idx2, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("pending2: %v", err)
	}
	if err := replayJournalIntoPending(j2, idx2, nil); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !idx2.HasPending("/w/l1") {
		t.Fatal("lazy-staged entry lost across daemon restart (page-cache survival broken)")
	}
}

// TestLazyStagingTornShadowDropped verifies the ext4-equivalent power-loss
// semantics: a WAL meta frame whose shadow content is short (kernel dropped
// the data write before power loss but persisted the meta) is dropped on
// replay instead of resurrecting a torn file.
func TestLazyStagingTornShadowDropped(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "j.wal"))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	shadowDir := filepath.Join(dir, "shadow")
	if err := os.MkdirAll(shadowDir, 0o755); err != nil {
		t.Fatal(err)
	}
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatalf("shadow: %v", err)
	}
	// Stage real content, then corrupt the size picture: WAL claims 4096,
	// shadow holds only 64 bytes.
	if err := shadows.WriteFull("/w/t1", make([]byte, 64), 0); err != nil {
		t.Fatalf("shadow write: %v", err)
	}
	meta := WriteBackMeta{Path: "/w/t1", Size: 4096, Kind: PendingNew, ShadowSpill: true}
	metaBytes, merr := json.Marshal(meta)
	if merr != nil {
		t.Fatalf("marshal: %v", merr)
	}
	if err := j.Append(JournalEntry{Op: JournalPendingMeta, Path: "/w/t1", Meta: metaBytes}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := j.FsyncShared(); err != nil {
		t.Fatalf("fsync: %v", err)
	}

	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(j, idx, shadows); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if idx.HasPending("/w/t1") {
		t.Fatal("torn shadow entry resurrected; should be dropped (ext4 page-cache-loss semantics)")
	}
	if shadows.Has("/w/t1") {
		t.Fatal("torn shadow file not cleaned up")
	}
}

// TestLazyStagingTornShadowDroppedNonSpill is the non-spill twin of
// TestLazyStagingTornShadowDropped: a lazy overwrite of a pre-existing file
// stages through the non-spill pending-index path, and its WAL meta frame
// must be dropped just the same when the shadow content is short. Non-spill
// payloads also upload from the shadow file, so resurrecting a torn frame
// would CAS-commit truncated bytes over a good remote revision.
func TestLazyStagingTornShadowDroppedNonSpill(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "j.wal"))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	shadowDir := filepath.Join(dir, "shadow")
	if err := os.MkdirAll(shadowDir, 0o755); err != nil {
		t.Fatal(err)
	}
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatalf("shadow: %v", err)
	}
	// Lazy overwrite of an existing file at base revision 42: only 1024 of
	// the 4096 staged bytes survived the power loss.
	if err := shadows.WriteFull("/w/o1", make([]byte, 1024), 42); err != nil {
		t.Fatalf("shadow write: %v", err)
	}
	meta := WriteBackMeta{Path: "/w/o1", Size: 4096, Kind: PendingOverwrite, BaseRev: 42}
	metaBytes, merr := json.Marshal(meta)
	if merr != nil {
		t.Fatalf("marshal: %v", merr)
	}
	if err := j.Append(JournalEntry{Op: JournalPendingMeta, Path: "/w/o1", Meta: metaBytes}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := j.FsyncShared(); err != nil {
		t.Fatalf("fsync: %v", err)
	}

	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(j, idx, shadows); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if idx.HasPending("/w/o1") {
		t.Fatal("torn non-spill overwrite resurrected; would CAS-commit truncated bytes over a good remote revision")
	}
	if shadows.Has("/w/o1") {
		t.Fatal("torn shadow file not cleaned up")
	}
}

// TestLazyStagingNonSpillCompleteShadowKept is the control for the guard
// above: a non-spill frame whose shadow content is complete must still be
// resurrected.
func TestLazyStagingNonSpillCompleteShadowKept(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "j.wal"))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	shadowDir := filepath.Join(dir, "shadow")
	if err := os.MkdirAll(shadowDir, 0o755); err != nil {
		t.Fatal(err)
	}
	shadows, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatalf("shadow: %v", err)
	}
	if err := shadows.WriteFull("/w/o2", make([]byte, 4096), 42); err != nil {
		t.Fatalf("shadow write: %v", err)
	}
	meta := WriteBackMeta{Path: "/w/o2", Size: 4096, Kind: PendingOverwrite, BaseRev: 42}
	metaBytes, merr := json.Marshal(meta)
	if merr != nil {
		t.Fatalf("marshal: %v", merr)
	}
	if err := j.Append(JournalEntry{Op: JournalPendingMeta, Path: "/w/o2", Meta: metaBytes}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := j.FsyncShared(); err != nil {
		t.Fatalf("fsync: %v", err)
	}

	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(j, idx, shadows); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !idx.HasPending("/w/o2") {
		t.Fatal("complete non-spill overwrite dropped on replay; guard is over-firing")
	}
}

// TestJournalSyncLoopBounded verifies the background syncer makes appended
// frames durable within the window (power-loss bound of issue #964).
func TestJournalSyncLoopBounded(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "j.wal"))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go j.SyncLoop(ctx, 20*time.Millisecond)
	t.Cleanup(func() { _ = j.Close() })

	if err := j.Append(JournalEntry{Op: JournalWrite, Path: "/w/s1"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if j.DurableSeq() >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("SyncLoop did not make the frame durable within the window")
}
