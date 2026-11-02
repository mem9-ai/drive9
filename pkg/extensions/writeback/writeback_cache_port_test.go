package writeback

// WriteBackCache concurrency / generation / prefix rows, ported verbatim
// from v1 pkg/fuse/writeback_test.go (lines 4248-5130).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func TestWriteBackCache_ConcurrentPutAndRename(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Seed initial entry so RenamePending has something to work with.
	_ = cache.Put("/rename-src.txt", []byte("seed"), 4, staging.PendingNew)

	const goroutines = 4
	const iterations = 15
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("data-%d", i))
				_ = cache.Put("/rename-src.txt", data, int64(len(data)), staging.PendingNew)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				cache.RenamePending("/rename-src.txt", "/rename-dst.txt")
				// Rename back so the next iteration has a source.
				cache.RenamePending("/rename-dst.txt", "/rename-src.txt")
			}
		}()
	}
	wg.Wait()

	// After the storm, one of src or dst should hold valid data (or both absent).
	srcMeta, srcOK := cache.GetMeta("/rename-src.txt")
	dstMeta, dstOK := cache.GetMeta("/rename-dst.txt")
	if srcOK {
		data, ok := cache.Get("/rename-src.txt")
		if !ok {
			t.Fatal("src meta present but data absent")
		}
		if int64(len(data)) != srcMeta.Size {
			t.Fatalf("src data len %d != meta size %d", len(data), srcMeta.Size)
		}
	}
	if dstOK {
		data, ok := cache.Get("/rename-dst.txt")
		if !ok {
			t.Fatal("dst meta present but data absent")
		}
		if int64(len(data)) != dstMeta.Size {
			t.Fatalf("dst data len %d != meta size %d", len(data), dstMeta.Size)
		}
	}
}

// TestWriteBackCache_RemoveIfGeneration verifies the atomic generation-checked remove.
func TestWriteBackCache_RemoveIfGeneration(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	_ = cache.Put("/gen-rm.txt", []byte("v1"), 2, staging.PendingNew)
	meta1, _ := cache.GetMeta("/gen-rm.txt")
	gen1 := meta1.Generation

	// Overwrite with v2 → new generation.
	_ = cache.Put("/gen-rm.txt", []byte("v2"), 2, staging.PendingNew)
	meta2, _ := cache.GetMeta("/gen-rm.txt")
	gen2 := meta2.Generation

	if gen2 <= gen1 {
		t.Fatalf("gen2 %d should be > gen1 %d", gen2, gen1)
	}

	// RemoveIfGeneration with stale gen should NOT remove.
	if cache.RemoveIfGeneration("/gen-rm.txt", gen1) {
		t.Fatal("RemoveIfGeneration should return false for stale generation")
	}
	if _, ok := cache.GetMeta("/gen-rm.txt"); !ok {
		t.Fatal("entry should still exist after stale RemoveIfGeneration")
	}

	// RemoveIfGeneration with current gen should remove.
	if !cache.RemoveIfGeneration("/gen-rm.txt", gen2) {
		t.Fatal("RemoveIfGeneration should return true for matching generation")
	}
	if _, ok := cache.GetMeta("/gen-rm.txt"); ok {
		t.Fatal("entry should be removed after matching RemoveIfGeneration")
	}

	// RemoveIfGeneration on nonexistent path should return false.
	if cache.RemoveIfGeneration("/nonexistent.txt", 999) {
		t.Fatal("RemoveIfGeneration should return false for nonexistent path")
	}
}

// TestWriteBackCache_ConcurrentPutAndRemoveIfGeneration exercises the exact
// race that broke TestSamePathConsecutiveSaves: concurrent Put bumps generation
// while RemoveIfGeneration holds a stale captured gen.
func TestWriteBackCache_ConcurrentPutAndRemoveIfGeneration(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 4
	const iterations = 20

	// Track the highest generation successfully removed.
	var maxRemovedGen atomic.Uint64

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Writer goroutines: continuously Put new versions.
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("w%d-i%d", g, i))
				_ = cache.Put("/rig.txt", data, int64(len(data)), staging.PendingNew)
			}
		}()
	}

	// Remover goroutines: capture gen, then RemoveIfGeneration with stale gen.
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				meta, ok := cache.GetMeta("/rig.txt")
				if !ok {
					continue
				}
				capturedGen := meta.Generation
				// Yield to force interleaving with concurrent Puts.
				runtime.Gosched()
				if cache.RemoveIfGeneration("/rig.txt", capturedGen) {
					// Record the removed generation.
					for {
						old := maxRemovedGen.Load()
						if capturedGen <= old || maxRemovedGen.CompareAndSwap(old, capturedGen) {
							break
						}
					}
				}
			}
		}()
	}
	wg.Wait()

	// Invariant: if an entry still exists, its generation must be strictly
	// greater than the highest successfully removed generation (a stale
	// RemoveIfGeneration must not have deleted a newer entry).
	meta, ok := cache.GetMeta("/rig.txt")
	if ok {
		removed := maxRemovedGen.Load()
		if removed > 0 && meta.Generation <= removed {
			t.Fatalf("entry gen %d <= max removed gen %d (stale remove deleted newer entry)", meta.Generation, removed)
		}
		// Data must be consistent with meta.
		data, dataOK := cache.Get("/rig.txt")
		if !dataOK {
			t.Fatal("meta present but data absent")
		}
		if int64(len(data)) != meta.Size {
			t.Fatalf("data len %d != meta size %d", len(data), meta.Size)
		}
	}
}

// TestWriteBackCache_RenamePendingSamePath verifies that renaming a path to
// itself does not deadlock and is a no-op.
func TestWriteBackCache_RenamePendingSamePath(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	_ = cache.Put("/self.txt", []byte("data"), 4, staging.PendingNew)
	metaBefore, _ := cache.GetMeta("/self.txt")

	// This must not deadlock and should return false (no-op).
	result := cache.RenamePending("/self.txt", "/self.txt")
	if result {
		t.Fatal("RenamePending same path should return false")
	}

	// Entry should be unchanged.
	metaAfter, ok := cache.GetMeta("/self.txt")
	if !ok {
		t.Fatal("entry should still exist after same-path rename")
	}
	if metaAfter.Generation != metaBefore.Generation {
		t.Fatalf("generation changed from %d to %d after no-op rename", metaBefore.Generation, metaAfter.Generation)
	}
}

// TestWriteBackCache_GetMetaBlocksDuringInflightPut verifies that GetMeta
// serializes with the per-path lock so it blocks while a same-path Put is
// in progress (Phase 2 disk I/O). Without pathLock in GetMeta, metadata-only
// readers would see stale/missing state during the Put.
// Regression test for mornyx review comment.
func TestWriteBackCache_GetMetaBlocksDuringInflightPut(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	const path = "/inflight.txt"

	// Seed an initial entry so GetMeta has something to read.
	_ = cache.Put(path, []byte("v1"), 2, staging.PendingNew)
	metaV1, ok := cache.GetMeta(path)
	if !ok {
		t.Fatal("initial entry should exist")
	}

	// Simulate an in-flight Put by holding the path lock externally.
	pl := cache.AcquirePathLock(path)

	// GetMeta should block while the path lock is held.
	done := make(chan struct{})
	var metaResult *staging.WriteBackMeta
	var metaOK bool
	go func() {
		metaResult, metaOK = cache.GetMeta(path)
		close(done)
	}()

	// Give the goroutine time to start and block.
	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("GetMeta should block while pathLock is held, but it returned immediately")
	default:
		// Expected: GetMeta is blocked.
	}

	// Now do a Put under the path lock (simulating Phase 2 completion + Phase 3 publish).
	// We need to release the path lock first, do a real Put, then verify.
	// Actually, let's just release and verify GetMeta unblocks with the current state.
	cache.ReleasePathLock(path, pl)

	select {
	case <-done:
		// GetMeta unblocked.
	case <-time.After(5 * time.Second):
		t.Fatal("GetMeta did not unblock after pathLock release")
	}

	if !metaOK {
		t.Fatal("GetMeta should return true after pathLock release")
	}
	if metaResult.Generation != metaV1.Generation {
		t.Fatalf("GetMeta returned generation %d, want %d", metaResult.Generation, metaV1.Generation)
	}
}

// TestWriteBackCache_ListByPrefixWaitsInflightPut verifies that ListByPrefix
// waits for in-flight per-path operations under the prefix before scanning,
// so rmdir/rename mutation paths don't miss entries being written.
// Regression test for mornyx review comment about ListByPrefix visibility gap.
func TestWriteBackCache_ListByPrefixWaitsInflightPut(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	const prefix = "/mydir/"
	const childPath = "/mydir/child.txt"

	// Seed a child entry.
	_ = cache.Put(childPath, []byte("data"), 4, staging.PendingNew)

	// Simulate an in-flight Put on a second child by holding its pathLock.
	const inflightPath = "/mydir/inflight.txt"
	pl := cache.AcquirePathLock(inflightPath)

	// ListByPrefix should block waiting for the in-flight pathLock.
	done := make(chan []*staging.WriteBackMeta)
	go func() {
		result := cache.ListByPrefix(prefix)
		done <- result
	}()

	// Give the goroutine time to start and block.
	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("ListByPrefix should block while pathLock under prefix is held")
	default:
		// Expected: ListByPrefix is blocked.
	}

	// Simulate Phase 3 publish: add the entry to c.metas, then release pathLock.
	cache.PublishMeta(staging.WriteBackMeta{
		Path: inflightPath,
		Size: 5,
		Kind: staging.PendingNew,
	})
	cache.ReleasePathLock(inflightPath, pl)

	// ListByPrefix should now return both entries.
	select {
	case result := <-done:
		if len(result) != 2 {
			t.Fatalf("ListByPrefix returned %d entries, want 2", len(result))
		}
		paths := map[string]bool{}
		for _, m := range result {
			paths[m.Path] = true
		}
		if !paths[childPath] || !paths[inflightPath] {
			t.Fatalf("ListByPrefix missing expected paths: got %v", paths)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListByPrefix did not unblock after pathLock release")
	}
}

// TestLookupFindsPendingWriteBack verifies that Lookup returns metadata
// for files that only exist in the write-back cache (pending upload).
// v1 TestLookupFindsPendingWriteBack, adapted to the chain: a staged
// .dat/.meta image is visible to lookups with its pending size.
func TestLookupFindsPendingWriteBack(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := newTestClient(ts.URL)
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)

	if err := cache.Put("/pending.txt", []byte("local only"), 10, staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpLookup, Path: "/pending.txt"})
	if err != nil {
		t.Fatalf("lookup: %v — file should be visible from write-back cache", err)
	}
	if res.Attr == nil || res.Attr.Size != 10 {
		t.Fatalf("lookup attr = %+v, want size 10", res.Attr)
	}
}

// v1 TestOpenWritable_PreloadsFromWriteBackCache, adapted: an open-for-write
// preloads the staged image from the cache (no remote GET), and a
// subsequent commit appends onto those bytes.
func TestOpenWritable_PreloadsFromWriteBackCache(t *testing.T) {
	var getCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls.Add(1)
			_, _ = w.Write([]byte("stale"))
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := staging.NewWriteBackCache(dir)
	cl := newTestClient(ts.URL)
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)

	if err := cache.Put("/file.txt", []byte("fresh"), 5, staging.PendingOverwrite); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/file.txt", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	n, err := res.Handle.Read(context.Background(), 0, buf)
	if err != nil || string(buf[:n]) != "fresh" {
		t.Fatalf("read after open = %q err=%v, want cached fresh", buf[:n], err)
	}
	// v1 essential: reads must not serve the remote's stale bytes. (The
	// chain's create path may issue one metadata GET; content GETs of the
	// stale image are the regression this row guards.)
	if getCalls.Load() > 1 {
		t.Fatalf("remote GETs during open = %d, want <= 1", getCalls.Load())
	}
}

func TestUploader_WaitPath(t *testing.T) {
	uploadStarted := make(chan struct{})
	uploadRelease := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			close(uploadStarted)
			<-uploadRelease // block until test says continue
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	uploader := NewWriteBackUploader(c, cache, 1)

	_ = cache.Put("/inflight.txt", []byte("data"), 4, staging.PendingNew)
	uploader.Submit("/inflight.txt")

	// Wait for the upload to actually start.
	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}

	// WaitPath should block while the upload is in flight.
	waited := make(chan struct{})
	go func() {
		uploader.WaitPath("/inflight.txt")
		close(waited)
	}()

	// Verify WaitPath is actually blocking.
	select {
	case <-waited:
		t.Fatal("WaitPath returned before upload finished")
	case <-time.After(50 * time.Millisecond):
		// Good — still blocking.
	}

	// Release the upload.
	close(uploadRelease)

	// WaitPath should return now.
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitPath did not return after upload finished")
	}

	uploader.DrainAll()
}

// TestWriteBackCache_RenamePending verifies that RenamePending moves a cache
// entry from one path to another atomically.
func TestWriteBackCache_RenamePending(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	const snapshotID = "writeback-rename-snapshot-B"
	const parentSnapshotID = "writeback-rename-snapshot-A"
	ancestors := []string{parentSnapshotID, "writeback-rename-snapshot-root"}
	if _, _, err := cache.PutWithBaseRevAndModeAndLineageTimings(
		"/old.txt", []byte("data"), 4, staging.PendingNew, 9, 0o640, true,
		snapshotID, parentSnapshotID, true, ancestors...,
	); err != nil {
		t.Fatal(err)
	}

	if !cache.RenamePending("/old.txt", "/new.txt") {
		t.Fatal("RenamePending returned false, expected true")
	}

	// Old path should be gone.
	if _, ok := cache.Get("/old.txt"); ok {
		t.Fatal("old path should not exist after rename")
	}

	// New path should have the data.
	data, ok := cache.Get("/new.txt")
	if !ok {
		t.Fatal("new path should exist after rename")
	}
	if string(data) != "data" {
		t.Fatalf("data = %q, want %q", data, "data")
	}

	// Meta should reflect the new path.
	meta, ok := cache.GetMeta("/new.txt")
	if !ok {
		t.Fatal("meta should exist for new path")
	}
	if meta.Path != "/new.txt" {
		t.Fatalf("meta.Path = %q, want /new.txt", meta.Path)
	}
	if meta.BaseRev != 9 || !meta.HasMode || meta.Mode != 0o640 {
		t.Fatalf("meta base=%d mode=%o has=%t, want 9/0640/true", meta.BaseRev, meta.Mode, meta.HasMode)
	}
	if meta.SnapshotID != snapshotID || meta.ParentSnapshotID != parentSnapshotID || !meta.LineageTrusted() {
		t.Fatalf("lineage = %q/%q trusted=%t, want %q/%q true", meta.SnapshotID, meta.ParentSnapshotID, meta.LineageTrusted(), snapshotID, parentSnapshotID)
	}
	if !slices.Equal(meta.LiveAncestors(), ancestors) {
		t.Fatalf("liveAncestors = %v, want %v", meta.LiveAncestors(), ancestors)
	}
}

// TestWriteBackCache_Generation verifies that Put assigns monotonically
// increasing generation numbers.
func TestWriteBackCache_Generation(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	_ = cache.Put("/gen.txt", []byte("v1"), 2, staging.PendingNew)
	meta1, ok := cache.GetMeta("/gen.txt")
	if !ok {
		t.Fatal("expected meta after first Put")
	}

	_ = cache.Put("/gen.txt", []byte("v2"), 2, staging.PendingNew)
	meta2, ok := cache.GetMeta("/gen.txt")
	if !ok {
		t.Fatal("expected meta after second Put")
	}

	if meta2.Generation <= meta1.Generation {
		t.Fatalf("generation did not increase: %d <= %d", meta2.Generation, meta1.Generation)
	}
}

func TestWriteBackCache_ReopenPreservesGenerationCounter(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := cache.Put("/gen.txt", []byte("v1"), 2, staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	meta1, ok := cache.GetMeta("/gen.txt")
	if !ok {
		t.Fatal("expected meta after first Put")
	}

	cache2, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache2.Put("/gen.txt", []byte("v2"), 2, staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	meta2, ok := cache2.GetMeta("/gen.txt")
	if !ok {
		t.Fatal("expected meta after reopened Put")
	}
	if meta2.Generation <= meta1.Generation {
		t.Fatalf("generation did not increase after reopen: %d <= %d", meta2.Generation, meta1.Generation)
	}
}

// TestRename_PendingOverwrite_UsesSlowPath verifies that Rename falls back to
// the slow path (sync upload + remote rename) for pending-overwrite files,
// rather than using the local RenamePending fast path.
// v1 TestRename_PendingOverwrite_UsesSlowPath, adapted to the chain: a
// rename of a path with a staged pending-overwrite image must flush the
// cached image to the OLD path first, then remote-rename.
func TestRename_PendingOverwrite_UsesSlowPath(t *testing.T) {
	var ops []string
	var mu sync.Mutex
	record := func(op string) {
		mu.Lock()
		ops = append(ops, op)
		mu.Unlock()
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "10")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			_, _ = w.Write([]byte("serverdata"))
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			record("PUT:" + r.URL.Path)
			w.WriteHeader(http.StatusOK)
		case http.MethodPost:
			if r.URL.RawQuery == "rename" && r.Header.Get("X-Dat9-Rename-Source") == "/existing.txt" {
				record("RENAME")
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, _ := staging.NewWriteBackCache(dir)
	c := newTestClient(ts.URL)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: c, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(c, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)

	// Pending-overwrite: file existed on server, was edited locally.
	_ = cache.PutWithBaseRev("/existing.txt", []byte("edited data"), 11, staging.PendingOverwrite, 7)

	if _, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRename, Path: "/existing.txt", Path2: "/renamed.txt"}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if wb.uploader != nil {
		wb.uploader.DrainAll()
	}

	mu.Lock()
	defer mu.Unlock()
	foundPut := false
	foundRename := false
	for _, op := range ops {
		if op == "PUT:/v1/fs/existing.txt" {
			foundPut = true
		}
		if op == "RENAME" {
			foundRename = true
		}
	}
	if !foundPut {
		t.Fatalf("staging.PendingOverwrite should upload to old path first; ops = %v", ops)
	}
	if !foundRename {
		t.Fatalf("remote rename missing; ops = %v", ops)
	}
}
func TestWriteBackCache_PendingIndex(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Initially empty.
	if paths := cache.ListPendingPaths(); len(paths) != 0 {
		t.Fatalf("expected 0 pending paths, got %d", len(paths))
	}

	// Put adds to index.
	_ = cache.Put("/a.txt", []byte("a"), 1, staging.PendingNew)
	_ = cache.Put("/b.txt", []byte("b"), 1, staging.PendingNew)
	paths := cache.ListPendingPaths()
	if len(paths) != 2 {
		t.Fatalf("expected 2 pending paths, got %d", len(paths))
	}

	// Remove updates index.
	cache.Remove("/a.txt")
	paths = cache.ListPendingPaths()
	if len(paths) != 1 {
		t.Fatalf("expected 1 pending path after Remove, got %d", len(paths))
	}
	if _, ok := paths["/b.txt"]; !ok {
		t.Fatal("/b.txt should still be pending")
	}

	// RenamePending updates index.
	cache.RenamePending("/b.txt", "/c.txt")
	paths = cache.ListPendingPaths()
	if len(paths) != 1 {
		t.Fatalf("expected 1 pending path after Rename, got %d", len(paths))
	}
	if _, ok := paths["/c.txt"]; !ok {
		t.Fatal("/c.txt should be pending after rename")
	}
	if _, ok := paths["/b.txt"]; ok {
		t.Fatal("/b.txt should NOT be pending after rename")
	}

	// Reopen cache from disk — index should be rebuilt.
	cache2, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	paths2 := cache2.ListPendingPaths()
	if len(paths2) != 1 {
		t.Fatalf("reopened cache: expected 1 pending path, got %d", len(paths2))
	}
	if _, ok := paths2["/c.txt"]; !ok {
		t.Fatal("reopened cache: /c.txt should be pending")
	}
}

func TestWriteBackCacheListByPrefix(t *testing.T) {
	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}

	_ = cache.Put("/dir/a.txt", []byte("a"), 1, staging.PendingNew)
	_ = cache.Put("/dir/sub/b.txt", []byte("b"), 1, staging.PendingNew)
	_ = cache.Put("/dir-other/c.txt", []byte("c"), 1, staging.PendingNew)

	entries := cache.ListByPrefix("/dir/")
	if len(entries) != 2 {
		t.Fatalf("ListByPrefix len = %d, want 2", len(entries))
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		seen[entry.Path] = struct{}{}
	}
	if _, ok := seen["/dir/a.txt"]; !ok {
		t.Fatal("missing /dir/a.txt")
	}
	if _, ok := seen["/dir/sub/b.txt"]; !ok {
		t.Fatal("missing /dir/sub/b.txt")
	}
	if _, ok := seen["/dir-other/c.txt"]; ok {
		t.Fatal("/dir-other/c.txt should not match /dir/")
	}
}
