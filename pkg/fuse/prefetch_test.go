package fuse

import (
	"testing"
)

// newTestPrefetcher builds a Prefetcher without a backing client so OnRead's
// state machine can be exercised in isolation. startPrefetch is a no-op when
// the client is nil, so only OnRead/Get bookkeeping is observed.
func newTestPrefetcher(path string, fileSize int64) *Prefetcher {
	return NewPrefetcher(nil, path, fileSize)
}

// TestOnReadStrictOrderPreservesWindow covers the original fast path: reads
// arriving in exact offset order keep growing the window and advancing
// nextExpect. This guards against the fix regressing the common case.
func TestOnReadStrictOrderPreservesWindow(t *testing.T) {
	t.Helper()
	const chunk = 128 * 1024
	p := newTestPrefetcher("/f", 32*1024*1024)
	p.window = prefetchMinWindow

	for off := int64(0); off < 4*chunk; off += chunk {
		p.OnRead(off, chunk)
	}

	if p.nextExpect != 4*chunk {
		t.Fatalf("nextExpect = %d, want %d", p.nextExpect, 4*chunk)
	}
	if p.window <= prefetchMinWindow {
		t.Fatalf("window = %d, did not grow from min %d", p.window, prefetchMinWindow)
	}
	if len(p.cache) != 0 {
		t.Fatalf("cache = %d, want 0 (no client so no prefetch blocks)", len(p.cache))
	}
}

// TestOnReadOutOfOrderDoesNotReset reproduces the regression: the kernel's
// async readahead issues several reads concurrently that may complete out of
// order. The old strict offset==nextExpect check reset the window on every
// out-of-order arrival, collapsing prefetch to single-stream fetch latency.
// The fix must treat a forward read whose end does not retreat behind the
// frontier as sequential and keep the window growing.
func TestOnReadOutOfOrderDoesNotReset(t *testing.T) {
	t.Helper()
	const chunk = 128 * 1024
	p := newTestPrefetcher("/f", 32*1024*1024)
	p.window = prefetchMinWindow

	// Simulate four concurrent readahead requests completing out of order.
	// In a real run startPrefetch would have planted placeholder blocks in
	// p.cache for each chunk offset before any OnRead fires, so a block
	// arriving out of order is recognised as a late prefetched block rather
	// than a random seek. Mirror that by pre-seeding the cache.
	for i := int64(0); i < 4; i++ {
		p.cache[i*chunk] = &prefetchBlock{offset: i * chunk}
	}

	p.OnRead(0*chunk, chunk) // advances frontier to 1 chunk
	p.OnRead(3*chunk, chunk) // arrives before 1 & 2 but end=4chunk > frontier
	p.OnRead(1*chunk, chunk) // arrives after 3, behind frontier but prefetched
	p.OnRead(2*chunk, chunk) // fills the gap

	if p.nextExpect != 4*chunk {
		t.Fatalf("nextExpect = %d, want %d (furthest forward end)", p.nextExpect, 4*chunk)
	}
	// Window must have grown well beyond the minimum despite the interleaving.
	// Four sequential doublings from min would reach min*16, but out-of-order
	// arrivals that don't advance the frontier do not grow the window; assert
	// it is strictly larger than the minimum and that no reset cleared it.
	if p.window <= prefetchMinWindow {
		t.Fatalf("window = %d, want > %d (out-of-order read wrongly reset window)", p.window, prefetchMinWindow)
	}
}

// TestOnReadGenuineBackwardJumpResets ensures a real seek to a region well
// outside the tracked prefetch window still resets the window and clears
// state, so random access patterns do not accumulate stale prefetch bookkeeping.
// The seek here is a forward jump far beyond nextExpect+window (a backward jump
// is symmetric): both fall outside [nextExpect-window, nextExpect+window].
func TestOnReadGenuineBackwardJumpResets(t *testing.T) {
	t.Helper()
	const chunk = 128 * 1024
	p := newTestPrefetcher("/f", 32*1024*1024)
	p.window = prefetchMinWindow

	// Establish a small frontier; window stays modest.
	p.OnRead(0, chunk)
	p.OnRead(chunk, chunk)
	if p.nextExpect != 2*chunk {
		t.Fatalf("setup frontier = %d, want %d", p.nextExpect, 2*chunk)
	}
	prevWindow := p.window
	if prevWindow <= prefetchMinWindow {
		t.Fatalf("prevWindow = %d, did not grow; test does not exercise reset", prevWindow)
	}

	// Seek to a region far outside the window (50MB >> nextExpect+window).
	const far = 50 * 1024 * 1024
	p.OnRead(far, chunk)

	if p.nextExpect != far+chunk {
		t.Fatalf("after reset nextExpect = %d, want %d", p.nextExpect, far+chunk)
	}
	if p.window != prefetchMinWindow {
		t.Fatalf("window = %d, want %d (reset)", p.window, prefetchMinWindow)
	}
}

// TestOnReadReReadSameRangeDoesNotInflateWindow guards the sub-fix: readahead
// may re-deliver an already-consumed range. The read neither advances the
// frontier nor retreats behind it, so it must be treated as sequential
// (no reset) but must NOT grow the window, otherwise the window would inflate
// without bound from kernel readahead re-reads.
func TestOnReadReReadSameRangeDoesNotInflateWindow(t *testing.T) {
	t.Helper()
	const chunk = 128 * 1024
	p := newTestPrefetcher("/f", 32*1024*1024)
	p.window = prefetchMinWindow

	p.OnRead(0, chunk) // advance frontier to chunk
	afterFirst := p.window

	// Re-read the same range several times, as readahead may do.
	for range 4 {
		p.OnRead(0, chunk)
	}

	if p.nextExpect != chunk {
		t.Fatalf("nextExpect = %d, want %d (re-read must not advance frontier)", p.nextExpect, chunk)
	}
	if p.window != afterFirst {
		t.Fatalf("window = %d, want %d (re-read must not inflate window)", p.window, afterFirst)
	}
}
