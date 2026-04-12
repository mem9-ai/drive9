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
	j.Close()

	// Replay from a new journal instance.
	j2, err := NewJournal(jPath)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()

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
	j.Close()
}

func TestJournalEmptyReplay(t *testing.T) {
	dir := t.TempDir()
	jPath := filepath.Join(dir, "empty.wal")

	j, err := NewJournal(jPath)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

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
