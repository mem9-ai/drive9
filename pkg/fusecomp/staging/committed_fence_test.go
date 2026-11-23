package staging

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// newFenceTestIndex builds an index over a temp dir plus a ledger folder.
func newFenceTestIndex(t *testing.T) (*PendingIndex, string) {
	t.Helper()
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	return idx, dir
}

// TestCommittedFenceRetiresThePendingEntry pins the live-reader half of the
// fence: a landed image must stop being pending work for EVERY reader of the
// index, not only for the ones that know to filter a special kind.
func TestCommittedFenceRetiresThePendingEntry(t *testing.T) {
	idx, dir := newFenceTestIndex(t)
	const path = "/landed.txt"
	gen, err := idx.Put(path, 7, PendingNew)
	if err != nil {
		t.Fatal(err)
	}

	marked, err := idx.MarkCommittedIfGeneration(path, gen, 0)
	if err != nil || !marked {
		t.Fatalf("MarkCommittedIfGeneration = %v, %v; want true, nil", marked, err)
	}
	if _, ok := idx.GetMeta(path); ok {
		t.Fatal("GetMeta still returns the landed image as a pending entry")
	}
	if idx.HasPending(path) {
		t.Fatal("HasPending reports the landed image as pending work")
	}
	if n := idx.Count(); n != 0 {
		t.Fatalf("Count = %d, want 0 (a landed fence must not block ReexecRefusal)", n)
	}
	if paths := idx.ListUncommittedPaths(); len(paths) != 0 {
		t.Fatalf("ListUncommittedPaths = %v, want empty", paths)
	}
	if paths := idx.ListPendingPaths(); len(paths) != 0 {
		t.Fatalf("ListPendingPaths = %v, want empty", paths)
	}
	// The fence itself must be durable and rebuildable from disk.
	if _, err := os.Stat(filepath.Join(dir, HashPath(path)+".fence")); err != nil {
		t.Fatalf("fence file missing: %v", err)
	}
	recovered, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if _, ok := recovered.GetMeta(path); ok {
		t.Fatal("recovery resurrected the landed image as a pending entry")
	}
	if fence, ok := recovered.CommittedFence(path); !ok || fence.Generation != gen {
		t.Fatalf("recovered fence = %+v, %v; want generation %d", fence, ok, gen)
	}
}

// TestCommittedFenceMasksOnlyTheLandedGeneration pins the replay half: the
// fence drops the frames the landing superseded and keeps a NEWER publication,
// so fsync-acknowledged writes that happened after the landing survive.
func TestCommittedFenceMasksOnlyTheLandedGeneration(t *testing.T) {
	idx, dir := newFenceTestIndex(t)
	const path = "/replayed.txt"
	gen, err := idx.Put(path, 5, PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.MarkCommittedIfGeneration(path, gen, 0); err != nil {
		t.Fatal(err)
	}

	// An older frame (the landed image) must be masked.
	older := WriteBackMeta{Path: path, Size: 5, Kind: PendingNew, Generation: gen}
	rawOlder, err := json.Marshal(&older)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.publishRecoveredMeta(rawOlder); err != nil {
		t.Fatal(err)
	}
	if _, ok := idx.GetMeta(path); ok {
		t.Fatal("a fenced generation came back as a pending entry")
	}

	// A newer frame is a later acknowledged write: it must be recovered.
	newer := WriteBackMeta{Path: path, Size: 9, Kind: PendingNew, Generation: gen + 1}
	rawNewer, err := json.Marshal(&newer)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.publishRecoveredMeta(rawNewer); err != nil {
		t.Fatal(err)
	}
	meta, ok := idx.GetMeta(path)
	if !ok || meta.Generation != gen+1 {
		t.Fatalf("newer publication after the fence was dropped: %+v, %v", meta, ok)
	}
	_ = dir
}

// TestCommittedFenceSurvivesRestartUntilSettled pins the second-restart bug:
// once the pending entry is gone, a fence left on disk still suppresses the
// pre-commit frames, and settling appends a durable commit marker and removes
// the shadow on a COLD store (a real restart has no in-memory generations).
func TestCommittedFenceSurvivesRestartUntilSettled(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	shadows, err := NewShadowStore(filepath.Join(dir, "shadow"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shadows.Close() })
	idx.SetJournal(journal)

	const path = "/crash.txt"
	payload := []byte("landed image")
	if err := shadows.WriteFull(path, payload, 11); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadows.ActiveGeneration(path)
	gen, err := idx.Put(path, int64(len(payload)), PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.MarkCommittedIfGeneration(path, gen, shadowGen); err != nil {
		t.Fatal(err)
	}
	shadowFile := filepath.Join(dir, "shadow", HashPath(path)+".shadow")
	if _, err := os.Stat(shadowFile); err != nil {
		t.Fatalf("shadow missing before the restart: %v", err)
	}

	// Simulate a restart with a COLD shadow store: its in-memory generations and
	// recovered sizes are empty, exactly like a fresh mount.
	coldShadows, err := NewShadowStore(filepath.Join(dir, "shadow"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { coldShadows.Close() })
	restarted, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatal(err)
	}
	restarted.SetJournal(journal)
	restarted.SetShadowStore(coldShadows)
	if err := restarted.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if err := ReplayJournalIntoPending(journal, restarted, coldShadows); err != nil {
		t.Fatal(err)
	}
	if _, ok := restarted.GetMeta(path); ok {
		t.Fatal("replay resurrected the landed image as a pending entry")
	}
	if _, ok := restarted.CommittedFence(path); !ok {
		t.Fatal("the fence did not survive the restart")
	}
	if err := restarted.SettleCommittedFences(journal, coldShadows); err != nil {
		t.Fatal(err)
	}
	if _, ok := restarted.CommittedFence(path); ok {
		t.Fatal("the fence survived its own settle")
	}
	// The cold store must have removed the pre-crash shadow file, not skipped it
	// because its own generation counter started at zero.
	if _, err := os.Stat(shadowFile); !os.IsNotExist(err) {
		t.Fatalf("the landed image's shadow survived the settle: %v", err)
	}
	if restarted.HasPending(path) {
		t.Fatal("settle left the path pending")
	}
}

// TestSettleKeepsANewerWALWrite pins the tombstoning bug: the crash situation
// is a fence (G1) that survived on disk while a newer pending publication (G2)
// also reached the WAL (the fence unlink was not durable). Settling must not
// append a commit marker that lets compaction drop G2 — that write was
// acknowledged, so it has to survive a second restart.
func TestSettleKeepsANewerWALWrite(t *testing.T) {
	dir := t.TempDir()
	pendingDir := filepath.Join(dir, "pending")
	idx, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	shadows, err := NewShadowStore(filepath.Join(dir, "shadow"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shadows.Close() })
	idx.SetJournal(journal)

	const path = "/rewritten.txt"
	if err := shadows.WriteFull(path, []byte("landed"), 1); err != nil {
		t.Fatal(err)
	}
	gen, err := idx.Put(path, 6, PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.MarkCommittedIfGeneration(path, gen, shadows.ActiveGeneration(path)); err != nil {
		t.Fatal(err)
	}
	firstFence, ok := idx.CommittedFence(path)
	if !ok {
		t.Fatal("no fence was recorded")
	}

	// The operator wrote again: newer bytes in the shadow, then a newer pending
	// publication for the same path. Its Put clears the fence in memory and on
	// disk.
	newer := []byte("landed image")
	if err := shadows.WriteFull(path, newer, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.Put(path, int64(len(newer)), PendingOverwrite); err != nil {
		t.Fatal(err)
	}
	// Simulate the power loss that reverts the fence unlink: G1's fence is back
	// on disk next to G2's WAL frame.
	rawFence, err := json.Marshal(&firstFence)
	if err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(filepath.Join(pendingDir, HashPath(path)+".fence"), rawFence); err != nil {
		t.Fatal(err)
	}

	// Cold restart: the pipeline order mount uses (disk -> replay -> settle).
	restarted, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	restarted.SetJournal(journal)
	restarted.SetShadowStore(shadows)
	if err := restarted.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if err := ReplayJournalIntoPending(journal, restarted, shadows); err != nil {
		t.Fatal(err)
	}
	if meta, ok := restarted.GetMeta(path); !ok || meta.Generation <= firstFence.Generation {
		t.Fatalf("the newer write was not recovered before settle: %+v, %v", meta, ok)
	}
	if err := restarted.SettleCommittedFences(journal, shadows); err != nil {
		t.Fatal(err)
	}
	// A commit marker here would let compaction drop the newer frame.
	if err := journal.Compact(); err != nil {
		t.Fatal(err)
	}
	second, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	second.SetShadowStore(shadows)
	if err := second.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if err := ReplayJournalIntoPending(journal, second, shadows); err != nil {
		t.Fatal(err)
	}
	meta, ok := second.GetMeta(path)
	if !ok {
		t.Fatal("the newer write was lost across settle + compaction")
	}
	if meta.Size != int64(len(newer)) {
		t.Fatalf("recovered size = %d, want %d (the newer write)", meta.Size, len(newer))
	}
}

// TestRecoverFromDiskNeverRewindsGenerations pins the counter rule: a WAL-only
// entry (no .meta file) already raised the counter, so a later RecoverFromDisk
// pass must not lower it and a Put must not reuse a live generation.
func TestRecoverFromDiskNeverRewindsGenerations(t *testing.T) {
	dir := t.TempDir()
	pendingDir := filepath.Join(dir, "pending")
	idx, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	shadows, err := NewShadowStore(filepath.Join(dir, "shadow"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shadows.Close() })
	idx.SetJournal(journal)

	const path = "/wal-only.txt"
	payload := []byte("wal only")
	if err := shadows.WriteFull(path, payload, 1); err != nil {
		t.Fatal(err)
	}
	gen, err := idx.Put(path, int64(len(payload)), PendingNew)
	if err != nil {
		t.Fatal(err)
	}

	// Fresh index over the same directory: no .meta files exist (WAL mode).
	restarted, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	restarted.SetJournal(journal)
	restarted.SetShadowStore(shadows)
	if err := restarted.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if err := ReplayJournalIntoPending(journal, restarted, shadows); err != nil {
		t.Fatal(err)
	}
	// The facade's Start calls RecoverFromDisk again; it must stay idempotent.
	if err := restarted.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	next, err := restarted.Put(path, int64(len(payload)), PendingOverwrite)
	if err != nil {
		t.Fatal(err)
	}
	if next <= gen {
		t.Fatalf("generation after recovery = %d, want above the replayed %d", next, gen)
	}
}
