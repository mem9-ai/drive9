package cache

// Ports of v1 pkg/fuse/prefetch_test.go (d39a6d1b3): the Prefetcher window
// state machine — sequential detection with out-of-order tolerance, window
// growth from 256KB to the 16MB cap, exact-offset Get hits, and
// invalidateWithSize resets — exercised with a nil client so only the
// bookkeeping is observed, plus the default parallel-fetch block size (1MiB).

import (
	"errors"
	"testing"
)

// newTestPrefetcher builds a Prefetcher without a backing client so OnRead's
// state machine can be exercised in isolation. startPrefetch is a no-op when
// the client is nil, so only OnRead/Get bookkeeping is observed.
func newTestPrefetcher(path string, fileSize int64) *Prefetcher {
	return NewPrefetcher(nil, path, fileSize)
}

// v1 TestOnReadStrictOrderPreservesWindow covers the original fast path:
// reads arriving in exact offset order keep growing the window and advancing
// nextExpect.
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

// v1 TestOnReadOutOfOrderDoesNotReset reproduces the regression the window
// check fixed: readahead requests completing out of order must be recognised
// as late prefetched blocks, not random seeks.
func TestOnReadOutOfOrderDoesNotReset(t *testing.T) {
	t.Helper()
	const chunk = 128 * 1024
	p := newTestPrefetcher("/f", 32*1024*1024)
	p.window = prefetchMinWindow

	// Mirror a real run: startPrefetch plants placeholder blocks for each
	// chunk offset before any OnRead fires, so a block arriving out of order
	// is recognised as a late prefetched block rather than a random seek.
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
	if p.window <= prefetchMinWindow {
		t.Fatalf("window = %d, want > %d (out-of-order read wrongly reset window)", p.window, prefetchMinWindow)
	}
}

// v1 TestOnReadGenuineBackwardJumpResets ensures a seek to a region well
// outside the tracked window resets the window and clears stale state.
func TestOnReadGenuineBackwardJumpResets(t *testing.T) {
	t.Helper()
	const chunk = 128 * 1024
	p := newTestPrefetcher("/f", 32*1024*1024)
	p.window = prefetchMinWindow

	p.OnRead(0, chunk)
	p.OnRead(chunk, chunk)
	if p.nextExpect != 2*chunk {
		t.Fatalf("setup frontier = %d, want %d", p.nextExpect, 2*chunk)
	}
	prevWindow := p.window
	if prevWindow <= prefetchMinWindow {
		t.Fatalf("prevWindow = %d, did not grow; test does not exercise reset", prevWindow)
	}

	const far = 50 * 1024 * 1024
	p.OnRead(far, chunk)

	if p.nextExpect != far+chunk {
		t.Fatalf("after reset nextExpect = %d, want %d", p.nextExpect, far+chunk)
	}
	if p.window != prefetchMinWindow {
		t.Fatalf("window = %d, want %d (reset)", p.window, prefetchMinWindow)
	}
}

// v1 TestOnReadReReadSameRangeDoesNotInflateWindow: a re-delivered
// already-consumed range is sequential (no reset) but must not grow the
// window, otherwise readahead re-reads inflate it without bound.
func TestOnReadReReadSameRangeDoesNotInflateWindow(t *testing.T) {
	t.Helper()
	const chunk = 128 * 1024
	p := newTestPrefetcher("/f", 32*1024*1024)
	p.window = prefetchMinWindow

	p.OnRead(0, chunk) // advance frontier to chunk
	afterFirst := p.window

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

// The window doubles on every frontier-advancing read and saturates at
// prefetchMaxWindow (16MB), starting from prefetchMinWindow (256KB).
func TestOnReadWindowGrows256KTo16MCap(t *testing.T) {
	t.Helper()
	const chunk = 128 * 1024
	p := newTestPrefetcher("/f", 64*1024*1024)
	if p.window != prefetchMinWindow {
		t.Fatalf("initial window = %d, want %d", p.window, prefetchMinWindow)
	}

	off := int64(0)
	for ; p.window < prefetchMaxWindow; off += chunk {
		before := p.window
		p.OnRead(off, chunk)
		if p.window != before*2 {
			t.Fatalf("window = %d after advancing read, want doubled %d", p.window, before*2)
		}
	}
	if p.window != prefetchMaxWindow {
		t.Fatalf("window = %d, want cap %d", p.window, prefetchMaxWindow)
	}

	// Further sequential reads keep it pinned at the cap.
	for i := 0; i < 8; i++ {
		p.OnRead(off, chunk)
		off += chunk
	}
	if p.window != prefetchMaxWindow {
		t.Fatalf("window = %d past cap, want %d", p.window, prefetchMaxWindow)
	}
}

// Get is exact-offset: a hit requires a block keyed at the requested offset;
// interior offsets of a stored block do not match.
func TestGetExactOffsetHitAndMiss(t *testing.T) {
	t.Helper()
	p := newTestPrefetcher("/f", 32*1024*1024)
	const offset = 4 * 128 * 1024
	ready := make(chan struct{})
	close(ready)
	p.cache[offset] = &prefetchBlock{offset: offset, data: []byte("hello world 128k block padding!!"), ready: ready}

	data, ok := p.Get(offset, 8)
	if !ok || string(data) != "hello wo" {
		t.Fatalf("exact-offset get = (%q, %v), want trimmed hit", data, ok)
	}
	if _, ok := p.cache[offset]; ok {
		t.Fatal("consumed block must be removed after a hit")
	}
	if _, ok := p.Get(offset, 8); ok {
		t.Fatal("second get at consumed offset must miss")
	}
	if _, ok := p.Get(offset+1, 8); ok {
		t.Fatal("interior offset must not hit (exact-offset matching)")
	}
}

// invalidateWithSize resets the sequential state and publishes a replacement
// file size; a negative size leaves the size unchanged.
func TestInvalidateWithSizeResetsStateAndSize(t *testing.T) {
	t.Helper()
	const chunk = 128 * 1024
	p := newTestPrefetcher("/f", 32*1024*1024)
	p.OnRead(0, chunk)
	p.OnRead(chunk, chunk)
	ready := make(chan struct{})
	close(ready)
	p.cache[4*chunk] = &prefetchBlock{offset: 4 * chunk, data: []byte("x"), ready: ready}
	p.inflight[8*chunk] = true
	p.readSize = chunk

	p.invalidateWithSize(12345)

	if p.nextExpect != 0 || p.window != prefetchMinWindow || p.readSize != 0 {
		t.Fatalf("state after invalidate = (next=%d window=%d readSize=%d), want zeros/min", p.nextExpect, p.window, p.readSize)
	}
	if len(p.cache) != 0 {
		t.Fatalf("cache after invalidate = %d, want empty", len(p.cache))
	}
	if p.target != nil {
		t.Fatal("read target must be dropped by invalidation")
	}
	if p.fileSize != 12345 {
		t.Fatalf("fileSize = %d, want 12345", p.fileSize)
	}

	// A negative size keeps the current size.
	p.OnRead(0, chunk)
	p.invalidateWithSize(-1)
	if p.fileSize != 12345 {
		t.Fatalf("fileSize after negative invalidate = %d, want 12345", p.fileSize)
	}
	// Invalidation replaces the context so cancelled waits stop early.
	select {
	case <-p.ctx.Done():
		t.Fatal("fresh context must not be pre-cancelled")
	default:
	}
}

// The default parallel fetch plan uses 1MiB blocks and 4-way concurrency;
// SetParallelRead resets invalid values back to those defaults, and block
// sizes align up to whole chunks.
func TestPrefetchDefaultParallelBlockAndAlignment(t *testing.T) {
	t.Helper()
	p := newTestPrefetcher("/f", 64*1024*1024)
	if p.fetchConcurrency != defaultParallelReadConcurrency {
		t.Fatalf("default concurrency = %d, want %d", p.fetchConcurrency, defaultParallelReadConcurrency)
	}
	if p.fetchBlockSize != 1<<20 {
		t.Fatalf("default fetchBlockSize = %d, want 1MiB", p.fetchBlockSize)
	}
	p.SetParallelRead(0, 0)
	if p.fetchConcurrency != defaultParallelReadConcurrency || p.fetchBlockSize != defaultParallelReadBlockSize {
		t.Fatalf("reset defaults = (%d, %d)", p.fetchConcurrency, p.fetchBlockSize)
	}

	if got := alignPrefetchFetchBlockSize(1<<20, 128*1024); got != 1<<20 {
		t.Fatalf("align(1MiB, 128KiB) = %d, want unchanged", got)
	}
	if got := alignPrefetchFetchBlockSize(100*1024, 128*1024); got != 128*1024 {
		t.Fatalf("align(100KiB, 128KiB) = %d, want lifted to chunk", got)
	}
	if got := alignPrefetchFetchBlockSize(200*1024, 128*1024); got != 256*1024 {
		t.Fatalf("align(200KiB, 128KiB) = %d, want rounded up to whole chunks", got)
	}
	if got := alignPrefetchFetchBlockSize(1<<20, 0); got != 1<<20 {
		t.Fatalf("align(1MiB, no chunk) = %d, want unchanged", got)
	}

	plan := prefetchFetchPlan(0, 4*1024*1024, 1<<20)
	if len(plan) != 4 {
		t.Fatalf("plan(4MiB, 1MiB) = %d fetches, want 4", len(plan))
	}
	for i, f := range plan {
		if f.offset != int64(i)*1<<20 || f.length != 1<<20 {
			t.Fatalf("plan[%d] = %+v, want aligned 1MiB fetch", i, f)
		}
	}
	tail := prefetchFetchPlan(3*1024*1024, 2*1024*1024, 1<<20)
	if len(tail) != 2 || tail[1].length != 1*1024*1024 {
		t.Fatalf("tail plan = %+v, want final fetch clamped to file end", tail)
	}
	if got := prefetchFetchPlan(0, 0, 1<<20); got != nil {
		t.Fatalf("empty plan = %+v, want nil", got)
	}
}

// A failed prefetch block is removed on Get and reported as a miss instead
// of surfacing an error to the read path.
func TestGetFailedBlockMissesAndIsDropped(t *testing.T) {
	t.Helper()
	p := newTestPrefetcher("/f", 32*1024*1024)
	ready := make(chan struct{})
	close(ready)
	block := &prefetchBlock{offset: 128 * 1024, ready: ready}
	block.err = errors.New("prefetch failed")
	p.cache[128*1024] = block

	if data, ok := p.Get(128*1024, 16); ok || data != nil {
		t.Fatalf("failed block get = (%q, %v), want miss", data, ok)
	}
	if _, still := p.cache[128*1024]; still {
		t.Fatal("failed block must be dropped after the miss")
	}
}
