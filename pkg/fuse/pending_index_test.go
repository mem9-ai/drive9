package fuse

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPendingIndexPutGetRemove(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Put
	gen, err := idx.Put("/test/file.txt", 1024, PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if gen == 0 {
		t.Error("expected non-zero generation")
	}

	// GetMeta
	meta, ok := idx.GetMeta("/test/file.txt")
	if !ok {
		t.Fatal("expected pending entry")
	}
	if meta.Path != "/test/file.txt" {
		t.Errorf("path = %s, want /test/file.txt", meta.Path)
	}
	if meta.Size != 1024 {
		t.Errorf("size = %d, want 1024", meta.Size)
	}
	if meta.Kind != PendingNew {
		t.Errorf("kind = %d, want PendingNew", meta.Kind)
	}

	// HasPending
	if !idx.HasPending("/test/file.txt") {
		t.Error("expected HasPending to be true")
	}
	if idx.HasPending("/nonexistent") {
		t.Error("expected HasPending to be false for nonexistent path")
	}

	// Remove
	idx.Remove("/test/file.txt")
	_, ok = idx.GetMeta("/test/file.txt")
	if ok {
		t.Error("expected entry to be removed")
	}

	// .meta file should be removed from disk
	metaPath := filepath.Join(dir, hashPath("/test/file.txt")+".meta")
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Error("expected .meta file to be removed from disk")
	}
}

func TestPendingIndexRenamePending(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = idx.PutShadowSpillWithMode("/old/file.txt", 512, PendingOverwrite, 11, 0o755, true)
	if err != nil {
		t.Fatal(err)
	}

	ok := idx.RenamePending("/old/file.txt", "/new/file.txt")
	if !ok {
		t.Fatal("expected rename to succeed")
	}

	// Old path should be gone
	if idx.HasPending("/old/file.txt") {
		t.Error("expected old path to be removed")
	}

	// New path should exist with same size
	meta, ok := idx.GetMeta("/new/file.txt")
	if !ok {
		t.Fatal("expected new path to exist")
	}
	if meta.Size != 512 {
		t.Errorf("size = %d, want 512", meta.Size)
	}
	if !meta.ShadowSpill {
		t.Error("ShadowSpill = false, want true")
	}
	if meta.Kind != PendingOverwrite {
		t.Errorf("kind = %d, want PendingOverwrite", meta.Kind)
	}
	if meta.BaseRev != 11 {
		t.Errorf("baseRev = %d, want 11", meta.BaseRev)
	}
	if !meta.HasMode || meta.Mode != 0o755 {
		t.Errorf("mode = %o has=%t, want 0755 true", meta.Mode, meta.HasMode)
	}
}

func TestPendingIndexRecoverFromDisk(t *testing.T) {
	dir := t.TempDir()

	// Create an index and put some entries
	idx1, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = idx1.PutWithBaseRevAndMode("/recover/a.txt", 100, PendingNew, 0, 0o755, true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = idx1.Put("/recover/b.txt", 200, PendingOverwrite)
	if err != nil {
		t.Fatal(err)
	}

	// Create a new index and recover from disk
	idx2, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx2.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}

	meta, ok := idx2.GetMeta("/recover/a.txt")
	if !ok {
		t.Fatal("expected /recover/a.txt after recovery")
	}
	if meta.Size != 100 {
		t.Errorf("a.txt size = %d, want 100", meta.Size)
	}
	if !meta.HasMode || meta.Mode != 0o755 {
		t.Errorf("a.txt mode = %o has=%t, want 0755 true", meta.Mode, meta.HasMode)
	}

	meta, ok = idx2.GetMeta("/recover/b.txt")
	if !ok {
		t.Fatal("expected /recover/b.txt after recovery")
	}
	if meta.Size != 200 {
		t.Errorf("b.txt size = %d, want 200", meta.Size)
	}
}

func TestPendingIndexListPendingPaths(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Empty
	if paths := idx.ListPendingPaths(); paths != nil {
		t.Error("expected nil for empty index")
	}

	_, _ = idx.Put("/a.txt", 10, PendingNew)
	_, _ = idx.Put("/b.txt", 20, PendingNew)

	paths := idx.ListPendingPaths()
	if len(paths) != 2 {
		t.Errorf("expected 2 paths, got %d", len(paths))
	}
	if _, ok := paths["/a.txt"]; !ok {
		t.Error("expected /a.txt in paths")
	}
	if _, ok := paths["/b.txt"]; !ok {
		t.Error("expected /b.txt in paths")
	}
}

func TestPendingIndexListByPrefix(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = idx.Put("/dir/a.txt", 10, PendingNew)
	_, _ = idx.Put("/dir/b.txt", 20, PendingNew)
	_, _ = idx.Put("/other/c.txt", 30, PendingNew)

	results := idx.ListByPrefix("/dir/")
	if len(results) != 2 {
		t.Errorf("expected 2 results for /dir/ prefix, got %d", len(results))
	}
}

func TestPendingIndexCount(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	if idx.Count() != 0 {
		t.Errorf("expected count 0, got %d", idx.Count())
	}

	_, _ = idx.Put("/a.txt", 10, PendingNew)
	if idx.Count() != 1 {
		t.Errorf("expected count 1, got %d", idx.Count())
	}

	_, _ = idx.Put("/b.txt", 20, PendingNew)
	if idx.Count() != 2 {
		t.Errorf("expected count 2, got %d", idx.Count())
	}

	idx.Remove("/a.txt")
	if idx.Count() != 1 {
		t.Errorf("expected count 1 after remove, got %d", idx.Count())
	}
}

func TestPendingIndexRenameNonexistent(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}

	ok := idx.RenamePending("/nonexistent", "/new")
	if ok {
		t.Error("expected rename of nonexistent path to return false")
	}
}

// TestPendingIndexPrepareCommitRename verifies the crash-safe three-phase
// rename: after Prepare both .meta files are durable on disk (so a crash at
// any point between shadow rename and CommitRename leaves the data
// reachable), and Commit makes newPath authoritative.
func TestPendingIndexPrepareCommitRename(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.PutShadowSpillWithMode("/old.txt", 42, PendingNew, 0, 0o640, true); err != nil {
		t.Fatal(err)
	}

	newMeta, err := idx.PrepareRename("/old.txt", "/new.txt")
	if err != nil {
		t.Fatalf("PrepareRename: %v", err)
	}
	if newMeta == nil {
		t.Fatal("PrepareRename found no pending entry")
	}

	// Phase 1 (prepared): memory unchanged, BOTH metas durable on disk.
	if !idx.HasPending("/old.txt") {
		t.Error("old path must stay authoritative in memory until Commit")
	}
	if idx.HasPending("/new.txt") {
		t.Error("new path must not be live in memory before Commit")
	}
	crash, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := crash.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if !crash.HasPending("/old.txt") || !crash.HasPending("/new.txt") {
		t.Fatal("crash between Prepare and Commit must recover both metas (recovery prunes the one without a shadow)")
	}
	recovered, _ := crash.GetMeta("/new.txt")
	if recovered.Size != 42 || recovered.Kind != PendingNew || !recovered.ShadowSpill || !recovered.HasMode || recovered.Mode != 0o640 {
		t.Errorf("prepared meta lost fields: %+v", recovered)
	}

	// Phase 3 (commit): newPath authoritative, oldPath gone in memory + disk.
	idx.CommitRename("/old.txt", newMeta)
	if idx.HasPending("/old.txt") {
		t.Error("old path still pending after Commit")
	}
	if !idx.HasPending("/new.txt") {
		t.Error("new path not pending after Commit")
	}
	after, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := after.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if after.HasPending("/old.txt") {
		t.Error("old .meta not removed from disk after Commit")
	}
	if !after.HasPending("/new.txt") {
		t.Error("new .meta missing from disk after Commit")
	}
}

// TestPendingIndexAbortRename verifies that Abort removes only the prepared
// on-disk meta, and refuses to delete a meta that is live in memory.
func TestPendingIndexAbortRename(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.PutWithBaseRev("/old.txt", 10, PendingNew, 0); err != nil {
		t.Fatal(err)
	}

	if prepared, err := idx.PrepareRename("/old.txt", "/new.txt"); err != nil || prepared == nil {
		t.Fatalf("PrepareRename: meta=%v err=%v", prepared, err)
	}
	idx.AbortRename("/new.txt")

	recovered, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if recovered.HasPending("/new.txt") {
		t.Error("aborted .meta still on disk")
	}
	if !recovered.HasPending("/old.txt") {
		t.Error("old .meta lost by abort")
	}

	// Abort must not delete a live entry's meta.
	if _, err := idx.PutWithBaseRev("/live.txt", 5, PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	idx.AbortRename("/live.txt")
	recovered2, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered2.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if !recovered2.HasPending("/live.txt") {
		t.Error("AbortRename deleted a live entry's meta")
	}
}

// TestPendingIndexRemoveIfGenerationPutRaceConsistency races replacement Puts
// against stale-generation removals and asserts the crash-durability invariant
// at the end: recovery from disk must see exactly the live in-memory entries.
// Without per-path serialization, a stale RemoveIfGeneration can delete the
// replacement's fresh .meta after the replacement already passed the in-memory
// generation check (disk lost while memory looks correct).
func TestPendingIndexRemoveIfGenerationPutRaceConsistency(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	const p = "/race/file.txt"

	// Seed the stale generation the remover will target.
	staleGen, err := idx.Put(p, 1, PendingNew)
	if err != nil {
		t.Fatal(err)
	}

	const racers = 8
	const rounds = 100
	var wg sync.WaitGroup
	// Remover: repeatedly try to remove the ORIGINAL generation. Only the
	// first attempt can succeed; later ones must be no-ops because any
	// present entry is a fresher replacement generation.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			idx.RemoveIfGeneration(p, staleGen)
		}
	}()
	// Replacers: keep publishing newer generations.
	for r := 0; r < racers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				if _, err := idx.Put(p, int64(r*rounds+i+2), PendingNew); err != nil {
					t.Errorf("put: %v", err)
					return
				}
			}
		}(r)
	}
	wg.Wait()

	// Invariant: whatever is in memory must be exactly what is on disk.
	live, liveOK := idx.GetMeta(p)
	diskRaw, diskErr := os.ReadFile(filepath.Join(dir, hashPath(p)+".meta"))
	if liveOK {
		if diskErr != nil {
			t.Fatalf("live entry gen=%d present but .meta missing on disk: %v", live.Generation, diskErr)
		}
		var diskMeta WriteBackMeta
		if err := json.Unmarshal(diskRaw, &diskMeta); err != nil {
			t.Fatalf("decode .meta: %v", err)
		}
		if diskMeta.Generation != live.Generation {
			t.Fatalf("memory gen=%d but disk gen=%d — stale remove deleted replacement .meta", live.Generation, diskMeta.Generation)
		}
	}
	// Full recovery must reproduce the live view.
	idx2, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx2.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	_, recOK := idx2.GetMeta(p)
	if liveOK != recOK {
		t.Fatalf("recovery mismatch: live=%v recovered=%v", liveOK, recOK)
	}
}

// TestPendingIndexTwoPathLocksReverseOrder proves that a reverse-lexicographic
// acquire/release pair cannot split one path into two live mutexes: a waiter
// on the smaller path must hold the SAME lock instance that a later acquirer
// blocks on. Regression for the crossed lock/path pairing in
// releaseTwoPathLocks.
func TestPendingIndexTwoPathLocksReverseOrder(t *testing.T) {
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	pla, plb := idx.acquireTwoPathLocks("/z", "/a")

	// Register a waiter on "/a" (the lexicographically smaller path, held
	// via pla).
	waiterHeld := make(chan *pendingPathLock, 1)
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		pl := idx.acquirePathLock("/a")
		waiterHeld <- pl
		// Hold the lock briefly; a second acquirer must not enter meanwhile.
		time.Sleep(150 * time.Millisecond)
		idx.releasePathLock("/a", pl)
	}()
	// Wait until the waiter is queued on the /a lock.
	deadline := time.Now().Add(2 * time.Second)
	for {
		idx.mu.Lock()
		pl, ok := idx.pathLocks["/a"]
		waiters := 0
		if ok {
			waiters = pl.waiters
		}
		idx.mu.Unlock()
		if ok && waiters >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter never queued on /a path lock")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Release the pair in the original (unsorted) order. With the crossed
	// release the /a map entry would be deleted here, letting a second
	// acquirer create a fresh mutex and enter concurrently with the waiter.
	idx.releaseTwoPathLocks("/z", "/a", pla, plb)

	var waiterLock *pendingPathLock
	select {
	case waiterLock = <-waiterHeld:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not acquire /a lock after pair release")
	}

	// While the waiter holds /a, a new acquirer must block.
	secondAcquired := make(chan struct{})
	go func() {
		pl := idx.acquirePathLock("/a")
		close(secondAcquired)
		idx.releasePathLock("/a", pl)
	}()
	select {
	case <-secondAcquired:
		t.Fatal("second acquirer entered /a while the waiter still held it — path split into two locks")
	case <-time.After(50 * time.Millisecond):
		// Expected: still blocked.
	}
	<-waiterDone
	select {
	case <-secondAcquired:
	case <-time.After(2 * time.Second):
		t.Fatal("second acquirer never entered /a after waiter released")
	}
	_ = waiterLock
}
