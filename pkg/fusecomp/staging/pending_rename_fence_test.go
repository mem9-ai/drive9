// Crash-safety rows for the generation-fenced conflict-marker transfer used by
// writeback's rename: the transfer must keep metadata and payload paired
// throughout, so an interruption between the prepare and the shadow move
// leaves recoverable state rather than orphaned metadata.

package staging

import (
	"os"
	"path/filepath"
	"testing"
)

// newConflictIndex stages a conflicted pending entry at path and returns the
// index plus the entry's generation.
func newConflictIndex(t *testing.T, path, data string) (*PendingIndex, string, uint64) {
	t.Helper()
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, HashPath(path)+".shadow"), []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	gen, err := idx.Put(path, int64(len(data)), PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkConflict(path); err != nil {
		t.Fatal(err)
	}
	return idx, dir, gen
}

// TestPrepareRenameIfGenerationLeavesBothSidesRecoverable pins the crash
// window the transfer must survive: after the prepare and before the shadow
// move, BOTH .meta files are on disk — so a restart keeps whichever side the
// payload actually backs — while memory still answers from the old name.
func TestPrepareRenameIfGenerationLeavesBothSidesRecoverable(t *testing.T) {
	idx, dir, gen := newConflictIndex(t, "/a", "unlanded")

	prepared, ok, err := idx.PrepareRenameIfGeneration("/a", "/b", gen, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || prepared == nil {
		t.Fatal("prepare did not produce a transferable meta")
	}

	// "Crash" here: both metas must be durable on disk. Recovery prunes by
	// shadow presence, so each surviving .meta must carry the right path.
	for _, path := range []string{"/a", "/b"} {
		raw, rerr := os.ReadFile(filepath.Join(dir, HashPath(path)+".meta"))
		if rerr != nil {
			t.Fatalf("no durable meta for %s after prepare: %v", path, rerr)
		}
		if len(raw) == 0 {
			t.Fatalf("empty meta for %s after prepare", path)
		}
	}
	// Memory is still authoritative at the old name — the move has not been
	// committed, so lookups must not serve a phantom new name.
	if idx.HasPending("/b") {
		t.Fatal("prepare published the new name before the payload moved")
	}
	if meta, found := idx.GetMeta("/a"); !found || meta.Generation != gen {
		t.Fatalf("old-name entry = %+v found=%v, want the untouched generation %d", meta, found, gen)
	}
}

// TestAbortRenameIfGenerationRestoresOldNameAuthority pins the failed-move
// rollback: when the shadow move fails, the prepared meta is dropped and the
// old name keeps the marker (and is the only place recovery can find it).
func TestAbortRenameIfGenerationRestoresOldNameAuthority(t *testing.T) {
	idx, dir, gen := newConflictIndex(t, "/a", "unlanded")

	prepared, ok, err := idx.PrepareRenameIfGeneration("/a", "/b", gen, 0)
	if err != nil || !ok {
		t.Fatalf("prepare = %+v ok=%v err=%v", prepared, ok, err)
	}
	idx.AbortRenameIfGeneration("/a", "/b", prepared)

	if _, err := os.Stat(filepath.Join(dir, HashPath("/b")+".meta")); !os.IsNotExist(err) {
		t.Fatalf("prepared meta for the new name survived the abort: %v", err)
	}
	if meta, found := idx.GetMeta("/a"); !found || meta.Kind != PendingConflict || meta.Generation != gen {
		t.Fatalf("old-name entry after abort = %+v found=%v, want the conflict marker intact", meta, found)
	}
	if idx.HasPending("/b") {
		t.Fatal("aborted transfer left a live entry at the new name")
	}
}

// TestCommitRenameIfGenerationFencesBothEnds pins the fences: the commit
// refuses when the source generation moved on, and the prepare refuses when
// either endpoint no longer matches what the caller observed.
func TestCommitRenameIfGenerationFencesBothEnds(t *testing.T) {
	idx, _, gen := newConflictIndex(t, "/a", "unlanded")

	prepared, ok, err := idx.PrepareRenameIfGeneration("/a", "/b", gen, 0)
	if err != nil || !ok {
		t.Fatalf("prepare = %+v ok=%v err=%v", prepared, ok, err)
	}
	if idx.CommitRenameIfGeneration("/a", prepared, gen+1) {
		t.Fatal("commit accepted a source generation the caller never observed")
	}
	if !idx.CommitRenameIfGeneration("/a", prepared, gen) {
		t.Fatal("commit refused the generation the prepare was built from")
	}
	if idx.HasPending("/a") {
		t.Fatal("committed transfer left the old name behind")
	}
	if meta, found := idx.GetMeta("/b"); !found || meta.Kind != PendingConflict {
		t.Fatalf("new-name entry after commit = %+v found=%v, want the transferred conflict marker", meta, found)
	}

	// A stale prepare is refused outright: the old name no longer carries
	// the generation the caller captured.
	idx2, _, gen2 := newConflictIndex(t, "/c", "other")
	if _, ok, err := idx2.PrepareRenameIfGeneration("/c", "/d", gen2+1, 0); err != nil || ok {
		t.Fatalf("prepare with a stale source generation = ok=%v err=%v, want a refusal", ok, err)
	}
}
