package fuse

import (
	"os"
	"path/filepath"
	"testing"
)

func TestJournalMaintenanceFailureDoesNotFailPublication(t *testing.T) {
	idx, _ := newLayerShutdownState(t)
	j, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	idx.SetJournal(j)
	j.compactAfterBytes = 1
	// A nonempty directory prevents the maintenance rename without breaking
	// the open journal fd. Publications and fsync must remain usable.
	if err := os.Mkdir(j.path+".compact", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(j.path+".compact", "block"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, err := idx.PutLayerCache("/a", int64(i), 0, 0600, true, LayerCacheIdentity{LayerID: "layer-1", EntrySeq: int64(i + 1)}); err != nil {
			t.Fatalf("maintenance failure rejected publication: %v", err)
		}
	}
	if err := j.compact(false); err == nil {
		t.Fatal("wanted a maintenance failure")
	}
	if _, err := idx.PutLayerCache("/a", 4, 0, 0600, true, LayerCacheIdentity{LayerID: "layer-1", EntrySeq: 4}); err != nil {
		t.Fatal(err)
	}
	if err := j.FsyncShared(); err != nil {
		t.Fatal(err)
	}
	meta, _ := idx.GetMeta("/a")
	if meta == nil || meta.LayerEntrySeq != 4 {
		t.Fatalf("publication not visible: %+v", meta)
	}
	if err := j.compact(false); err != nil {
		t.Fatalf("maintenance did not back off: %v", err)
	}
	if err := os.RemoveAll(j.path + ".compact"); err != nil {
		t.Fatal(err)
	}
	if err := j.Compact(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := j.Replay(func(JournalEntry) { count++ }); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("frames=%d, want 1", count)
	}
}

func TestJournalCompactsRepeatedFsyncFrames(t *testing.T) {
	j, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	for i := range 50 {
		mustAppend(t, j, JournalEntry{Op: JournalFsync, Path: "/a", Length: int64(i + 1)})
	}
	if err := j.Compact(); err != nil {
		t.Fatal(err)
	}
	var entries []JournalEntry
	if err := j.Replay(func(e JournalEntry) { entries = append(entries, e) }); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Length != 50 {
		t.Fatalf("kept fsync history: %+v", entries)
	}
}

func TestJournalShutdownTreatsCompactionAsMaintenance(t *testing.T) {
	idx, shadows := newLayerShutdownState(t)
	j, err := NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	idx.SetJournal(j)
	for range 2 {
		if _, err := idx.PutLayerCache("/a", 0, 0, 0600, true, LayerCacheIdentity{LayerID: "layer-1", EntrySeq: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(j.path+".compact", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(j.path+".compact", "block"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
	fs.journal, fs.pendingIndex, fs.shadowStore = j, idx, shadows
	if err := fs.flushAll(); err != nil {
		t.Fatalf("clean shutdown failed on housekeeping: %v", err)
	}
	if err := j.Compact(); err == nil {
		t.Fatal("maintenance reopened a closed journal")
	}
	reopened, err := NewJournal(j.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(reopened, recovered, nil); err != nil {
		t.Fatal(err)
	}
	if meta, ok := recovered.GetMeta("/a"); !ok || !meta.LayerClean {
		t.Fatalf("maintenance failure lost durable metadata: %+v", meta)
	}
}
