package fuse

import (
	"path/filepath"
	"testing"
)

func TestJournalAppendReplay(t *testing.T) {
	dir := t.TempDir()
	jPath := filepath.Join(dir, "test.wal")

	j, err := NewJournal(jPath)
	if err != nil {
		t.Fatal(err)
	}

	// Append some entries.
	entries := []JournalEntry{
		{Op: JournalWrite, Path: "/a.txt", Offset: 0, Length: 100},
		{Op: JournalFsync, Path: "/a.txt"},
		{Op: JournalRename, Path: "/tmp.txt", NewPath: "/a.txt"},
		{Op: JournalUnlink, Path: "/old.txt"},
	}
	for _, e := range entries {
		if err := j.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Fsync(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// Replay from a new journal instance.
	j2, err := NewJournal(jPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j2.Close() }()

	var replayed []JournalEntry
	err = j2.Replay(func(e JournalEntry) {
		replayed = append(replayed, e)
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(replayed) != len(entries) {
		t.Fatalf("replayed %d entries, want %d", len(replayed), len(entries))
	}

	for i, e := range replayed {
		if e.Op != entries[i].Op {
			t.Errorf("entry[%d].Op = %d, want %d", i, e.Op, entries[i].Op)
		}
		if e.Path != entries[i].Path {
			t.Errorf("entry[%d].Path = %q, want %q", i, e.Path, entries[i].Path)
		}
		if e.Seq == 0 {
			t.Errorf("entry[%d].Seq = 0, want > 0", i)
		}
	}
}

func TestJournalCompact(t *testing.T) {
	dir := t.TempDir()
	jPath := filepath.Join(dir, "compact.wal")

	j, err := NewJournal(jPath)
	if err != nil {
		t.Fatal(err)
	}

	// Write, commit for /a.txt; write (no commit) for /b.txt.
	_ = j.Append(JournalEntry{Op: JournalWrite, Path: "/a.txt"})
	_ = j.Append(JournalEntry{Op: JournalWrite, Path: "/b.txt"})
	_ = j.Append(JournalEntry{Op: JournalCommit, Path: "/a.txt"})
	_ = j.Fsync()

	// Compact.
	if err := j.Compact(); err != nil {
		t.Fatal(err)
	}

	// Replay — should only see /b.txt entries (committed /a.txt removed).
	var replayed []JournalEntry
	err = j.Replay(func(e JournalEntry) {
		replayed = append(replayed, e)
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(replayed) != 1 {
		t.Fatalf("after compact: replayed %d entries, want 1", len(replayed))
	}
	if replayed[0].Path != "/b.txt" {
		t.Errorf("after compact: path = %q, want /b.txt", replayed[0].Path)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestReplayJournalIntoPendingHonorsCommitMarkers verifies that crash
// recovery does not resurrect paths whose latest fsync was followed by a
// commit marker (the path was uploaded or unlinked before the crash), while
// still resurrecting paths fsync'd again after their last commit.
func TestReplayJournalIntoPendingHonorsCommitMarkers(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "replay.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()

	// /committed.txt: fsync then commit — must NOT resurrect.
	_ = j.Append(JournalEntry{Op: JournalFsync, Path: "/committed.txt", Length: 10, BaseRev: 5})
	_ = j.Append(JournalEntry{Op: JournalCommit, Path: "/committed.txt"})
	// /pending.txt: fsync, commit, then fsync again — MUST resurrect with
	// the latest entry's metadata.
	_ = j.Append(JournalEntry{Op: JournalFsync, Path: "/pending.txt", Length: 10, BaseRev: 3})
	_ = j.Append(JournalEntry{Op: JournalCommit, Path: "/pending.txt"})
	_ = j.Append(JournalEntry{Op: JournalFsync, Path: "/pending.txt", Length: 30, BaseRev: 4})
	// /new-spill.bin: ShadowSpill fsync, never committed — must resurrect
	// in spill mode so recovery streams instead of ReadAll.
	_ = j.Append(JournalEntry{Op: JournalFsync, Path: "/new-spill.bin", Length: 1 << 30, BaseRev: 0, ShadowSpill: true})
	// /unlinked.txt: fsync then unlink marker — must NOT resurrect.
	_ = j.Append(JournalEntry{Op: JournalFsync, Path: "/unlinked.txt", Length: 7, BaseRev: 2})
	_ = j.Append(JournalEntry{Op: JournalUnlink, Path: "/unlinked.txt"})
	_ = j.Fsync()

	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(j, idx); err != nil {
		t.Fatal(err)
	}

	if idx.HasPending("/committed.txt") {
		t.Error("committed path was resurrected")
	}
	if idx.HasPending("/unlinked.txt") {
		t.Error("unlinked path was resurrected")
	}

	meta, ok := idx.GetMeta("/pending.txt")
	if !ok {
		t.Fatal("re-fsync'd path was not resurrected")
	}
	if meta.Size != 30 || meta.BaseRev != 4 {
		t.Errorf("resurrected meta = size %d baseRev %d, want 30/4 (latest fsync)", meta.Size, meta.BaseRev)
	}
	if meta.Kind != PendingOverwrite {
		t.Errorf("resurrected kind = %v, want PendingOverwrite", meta.Kind)
	}

	spill, ok := idx.GetMeta("/new-spill.bin")
	if !ok {
		t.Fatal("ShadowSpill path was not resurrected")
	}
	if !spill.ShadowSpill {
		t.Error("resurrected entry lost ShadowSpill flag — recovery would ReadAll a huge file")
	}
	if spill.Kind != PendingNew {
		t.Errorf("resurrected kind = %v, want PendingNew (BaseRev 0)", spill.Kind)
	}
	if spill.Size != 1<<30 {
		t.Errorf("resurrected size = %d, want %d", spill.Size, int64(1<<30))
	}
}

// TestReplayJournalIntoPendingPrefersSurvivingMeta verifies that a .meta file
// that survived the crash stays authoritative over older WAL frames.
func TestReplayJournalIntoPendingPrefersSurvivingMeta(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "replay.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()

	_ = j.Append(JournalEntry{Op: JournalFsync, Path: "/a.txt", Length: 10, BaseRev: 1})
	_ = j.Fsync()

	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.PutWithBaseRev("/a.txt", 99, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(j, idx); err != nil {
		t.Fatal(err)
	}

	meta, ok := idx.GetMeta("/a.txt")
	if !ok {
		t.Fatal("pending entry missing")
	}
	if meta.Size != 99 || meta.BaseRev != 7 {
		t.Errorf("surviving meta was overwritten by WAL frame: size %d baseRev %d, want 99/7", meta.Size, meta.BaseRev)
	}
}

// TestJournalCompactAfterReplayKeepsSeqMonotonic simulates the mount-time
// replay+compact cycle and verifies appends after Compact stay monotonic and
// replayable.
func TestJournalCompactAfterReplayKeepsSeqMonotonic(t *testing.T) {
	dir := t.TempDir()
	jPath := filepath.Join(dir, "cycle.wal")

	j, err := NewJournal(jPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = j.Append(JournalEntry{Op: JournalFsync, Path: "/done.txt", Length: 4, BaseRev: 1})
	_ = j.Append(JournalEntry{Op: JournalCommit, Path: "/done.txt"})
	_ = j.Append(JournalEntry{Op: JournalFsync, Path: "/live.txt", Length: 8, BaseRev: 2})
	_ = j.Fsync()
	_ = j.Close()

	// "Remount": replay then compact, as mount.go does.
	j2, err := NewJournal(jPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j2.Close() }()
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(j2, idx); err != nil {
		t.Fatal(err)
	}
	if err := j2.Compact(); err != nil {
		t.Fatal(err)
	}
	if idx.HasPending("/done.txt") {
		t.Error("committed path resurrected")
	}
	if !idx.HasPending("/live.txt") {
		t.Error("uncommitted path not resurrected")
	}

	// Frames for the committed path must be gone; new appends must replay
	// with strictly increasing seq.
	_ = j2.Append(JournalEntry{Op: JournalFsync, Path: "/after.txt", Length: 2, BaseRev: 3})
	var replayed []JournalEntry
	if err := j2.Replay(func(e JournalEntry) { replayed = append(replayed, e) }); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 {
		t.Fatalf("after compact+append: replayed %d entries, want 2", len(replayed))
	}
	if replayed[0].Path != "/live.txt" || replayed[1].Path != "/after.txt" {
		t.Errorf("unexpected frames: %q, %q", replayed[0].Path, replayed[1].Path)
	}
	if replayed[1].Seq <= replayed[0].Seq {
		t.Errorf("seq not monotonic after compact: %d then %d", replayed[0].Seq, replayed[1].Seq)
	}
}

func TestJournalEmptyReplay(t *testing.T) {
	dir := t.TempDir()
	jPath := filepath.Join(dir, "empty.wal")

	j, err := NewJournal(jPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()

	var count int
	err = j.Replay(func(e JournalEntry) {
		count++
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("empty journal replayed %d entries", count)
	}
}
