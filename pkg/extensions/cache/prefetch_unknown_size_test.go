package cache

import "testing"

// TestPrefetchInvalidateUnknownSizeRejectsStaleShortBlock reproduces the view
// reset that kept the open-time file size: the prefetcher clipped the last
// block to the old EOF and Get answered it, which the read path accepts as a
// legitimate end of file — a mid-file short read after the file grew.
func TestPrefetchInvalidateUnknownSizeRejectsStaleShortBlock(t *testing.T) {
	const openTimeSize = 100
	p := newTestPrefetcher("/grown.txt", openTimeSize)

	// A block of 40 bytes at offset 60 is exactly the last block of the
	// open-time view, and 40 bytes short of the request at offset 60.
	block := &prefetchBlock{offset: 60, data: make([]byte, 40), ready: make(chan struct{})}
	close(block.ready)
	p.cache[60] = block

	if _, ok := p.Get(60, 80); !ok {
		t.Fatal("precondition: the stale short block is served while the open-time size is known")
	}

	p.cache[60] = block // Get removes a served block
	p.InvalidateUnknownSize()
	p.mu.Lock()
	size := p.fileSize
	p.mu.Unlock()
	if size >= 0 {
		t.Fatalf("fileSize after InvalidateUnknownSize = %d, want unknown (< 0)", size)
	}
	if got := len(p.cache); got != 0 {
		t.Fatalf("cached blocks after invalidation = %d, want 0", got)
	}

	// Seed a short block again to prove the guard, not the empty cache, is
	// what rejects it while the size is unknown.
	block2 := &prefetchBlock{offset: 60, data: make([]byte, 40), ready: make(chan struct{})}
	close(block2.ready)
	p.cache[60] = block2
	if _, ok := p.Get(60, 80); ok {
		t.Fatal("a short block was served as EOF while the file size is unknown")
	}
}
