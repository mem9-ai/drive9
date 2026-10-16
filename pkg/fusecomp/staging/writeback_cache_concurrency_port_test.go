// v1 writeback_test.go cache-component rows: concurrent Put/Remove/
// RenamePending/RemoveIfGeneration races and the generation contract —
// direct ports against the staging.WriteBackCache (same API).
package staging

import (
	"fmt"
	"sync"
	"testing"
)

// v1 TestWriteBackCache_ConcurrentPutSamePath.
func TestPortCacheConcurrentPutSamePath(t *testing.T) {
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 8
	const iterations = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("g%d-i%d", g, i))
				_ = cache.Put("/concurrent.txt", data, int64(len(data)), PendingNew)
			}
		}()
	}
	wg.Wait()
	meta, ok := cache.GetMeta("/concurrent.txt")
	if !ok {
		t.Fatal("meta should exist after concurrent Puts")
	}
	data, ok := cache.Get("/concurrent.txt")
	if !ok {
		t.Fatal("data should exist after concurrent Puts")
	}
	if int64(len(data)) != meta.Size {
		t.Fatalf("data len %d != meta size %d", len(data), meta.Size)
	}
}

// v1 TestWriteBackCache_ConcurrentPutAndRemove.
func TestPortCacheConcurrentPutAndRemove(t *testing.T) {
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 4
	const iterations = 20
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("put-g%d-i%d", g, i))
				_ = cache.Put("/put-rm.txt", data, int64(len(data)), PendingNew)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				cache.Remove("/put-rm.txt")
			}
		}()
	}
	wg.Wait()
	meta, metaOK := cache.GetMeta("/put-rm.txt")
	data, dataOK := cache.Get("/put-rm.txt")
	if metaOK != dataOK {
		t.Fatalf("inconsistent: metaOK=%v dataOK=%v", metaOK, dataOK)
	}
	if metaOK && int64(len(data)) != meta.Size {
		t.Fatalf("data len %d != meta size %d", len(data), meta.Size)
	}
}

// v1 TestWriteBackCache_ConcurrentPutAndRename.
func TestPortCacheConcurrentPutAndRename(t *testing.T) {
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 4
	const iterations = 20
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("put-g%d-i%d", g, i))
				_ = cache.Put("/src.txt", data, int64(len(data)), PendingNew)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = cache.RenamePending("/src.txt", "/dst.txt")
			}
		}()
	}
	wg.Wait()
	// Either location may hold the entry; whichever does must be
	// meta/data consistent.
	for _, path := range []string{"/src.txt", "/dst.txt"} {
		meta, metaOK := cache.GetMeta(path)
		data, dataOK := cache.Get(path)
		if metaOK != dataOK {
			t.Fatalf("%s inconsistent: metaOK=%v dataOK=%v", path, metaOK, dataOK)
		}
		if metaOK && int64(len(data)) != meta.Size {
			t.Fatalf("%s data len %d != meta size %d", path, len(data), meta.Size)
		}
	}
}

// v1 TestWriteBackCache_RemoveIfGeneration.
func TestPortCacheRemoveIfGeneration(t *testing.T) {
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_ = cache.Put("/gen-rm.txt", []byte("v1"), 2, PendingNew)
	meta1, _ := cache.GetMeta("/gen-rm.txt")
	gen1 := meta1.Generation
	_ = cache.Put("/gen-rm.txt", []byte("v2"), 2, PendingNew)
	meta2, _ := cache.GetMeta("/gen-rm.txt")
	gen2 := meta2.Generation
	if gen2 <= gen1 {
		t.Fatalf("gen2 %d should be > gen1 %d", gen2, gen1)
	}
	if cache.RemoveIfGeneration("/gen-rm.txt", gen1) {
		t.Fatal("RemoveIfGeneration should return false for stale generation")
	}
	if _, ok := cache.GetMeta("/gen-rm.txt"); !ok {
		t.Fatal("entry should still exist after stale RemoveIfGeneration")
	}
	if !cache.RemoveIfGeneration("/gen-rm.txt", gen2) {
		t.Fatal("RemoveIfGeneration should return true for matching generation")
	}
	if _, ok := cache.GetMeta("/gen-rm.txt"); ok {
		t.Fatal("entry should be removed after matching RemoveIfGeneration")
	}
	if cache.RemoveIfGeneration("/nonexistent.txt", 999) {
		t.Fatal("RemoveIfGeneration should return false for nonexistent path")
	}
}

// v1 TestWriteBackCache_ConcurrentPutAndRemoveIfGeneration: a stale
// captured generation never removes a fresher Put.
func TestPortCacheConcurrentPutAndRemoveIfGeneration(t *testing.T) {
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 4
	const iterations = 20
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("v%d", i))
				_ = cache.Put("/race.txt", data, int64(len(data)), PendingNew)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if meta, ok := cache.GetMeta("/race.txt"); ok {
					// Stale-by-construction: another Put may bump the
					// generation between read and remove.
					cache.RemoveIfGeneration("/race.txt", meta.Generation)
				}
			}
		}()
	}
	wg.Wait()
	// Consistent end state: present (meta matches data) or absent.
	meta, metaOK := cache.GetMeta("/race.txt")
	data, dataOK := cache.Get("/race.txt")
	if metaOK != dataOK {
		t.Fatalf("inconsistent: metaOK=%v dataOK=%v", metaOK, dataOK)
	}
	if metaOK && int64(len(data)) != meta.Size {
		t.Fatalf("data len %d != meta size %d", len(data), meta.Size)
	}
}

// v1 TestWriteBackCache_RenamePending / Generation / ReopenPreserves-
// GenerationCounter: rename re-keys, generations are monotone, and the
// counter survives reopen.
func TestPortCacheRenameAndGeneration(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = cache.Put("/a.txt", []byte("one"), 3, PendingNew)
	cache.RenamePending("/a.txt", "/b.txt")
	if _, ok := cache.GetMeta("/a.txt"); ok {
		t.Fatal("source entry survived rename")
	}
	if meta, ok := cache.GetMeta("/b.txt"); !ok || meta.Size != 3 {
		t.Fatalf("dest entry = %v/%v", meta, ok)
	}
	gen1 := func() uint64 { m, _ := cache.GetMeta("/b.txt"); return m.Generation }()
	_ = cache.Put("/b.txt", []byte("two"), 3, PendingNew)
	gen2 := func() uint64 { m, _ := cache.GetMeta("/b.txt"); return m.Generation }()
	if gen2 <= gen1 {
		t.Fatalf("generation not monotone: %d then %d", gen1, gen2)
	}
	reopened, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = reopened.Put("/b.txt", []byte("three"), 5, PendingNew)
	gen3 := func() uint64 { m, _ := reopened.GetMeta("/b.txt"); return m.Generation }()
	if gen3 <= gen2 {
		t.Fatalf("generation counter did not survive reopen: %d then %d", gen2, gen3)
	}
}
