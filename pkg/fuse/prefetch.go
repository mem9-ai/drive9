package fuse

import (
	"context"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const (
	prefetchMinWindow = 256 * 1024       // 256KB
	prefetchMaxWindow = 16 * 1024 * 1024 // 16MB
	prefetchMaxBlocks = 128              // max cached prefetch chunks
)

// prefetchBlock holds prefetched data for a byte range.
type prefetchBlock struct {
	offset int64
	data   []byte
	ready  chan struct{} // closed when data is available
	err    error
}

// Prefetcher detects sequential read patterns and prefetches upcoming data
// blocks in the background, reducing HTTP round-trips for large file reads.
//
// Design: a prefetch window is split into bounded range requests. Fetched
// ranges are then split into read-aligned chunks, each stored at its own
// offset key. Get() uses exact-offset matching — no sub-block scanning needed.
//
// Concurrency: Prefetcher is fully self-synchronized via p.mu.
// Callers do NOT need to hold any external lock.
// Lock ordering (if relevant): FileHandle.mu → Prefetcher.mu.
//
// Eviction: uses smallest-offset eviction, optimised for forward sequential
// reads (e.g. cat, cp). Reverse reads will thrash the cache — this is
// acceptable since the common case is forward streaming.
//
// Lifecycle: call Close() when the file handle is released to cancel
// inflight prefetches and prevent goroutine leaks.
type Prefetcher struct {
	mu               sync.Mutex
	nextExpect       int64 // next expected offset (for sequential detection)
	window           int64 // current prefetch window (adaptive)
	readSize         int   // observed FUSE read size (for chunk splitting)
	cache            map[int64]*prefetchBlock
	inflight         map[int64]bool
	client           *client.Client
	target           *client.ReadTarget
	fetchConcurrency int
	fetchBlockSize   int64
	path             atomic.Value
	fileSize         int64
	cancel           context.CancelFunc // cancels all inflight prefetch goroutines
	ctx              context.Context    // parent context for prefetch goroutines
	closed           bool
	debug            bool
	perf             *fusePerfCounters
}

// NewPrefetcher creates a Prefetcher for the given file.
func NewPrefetcher(c *client.Client, path string, fileSize int64, debug ...bool) *Prefetcher {
	ctx, cancel := context.WithCancel(context.Background())
	debugEnabled := false
	if len(debug) > 0 {
		debugEnabled = debug[0]
	}
	p := &Prefetcher{
		nextExpect:       0,
		window:           prefetchMinWindow,
		cache:            make(map[int64]*prefetchBlock),
		inflight:         make(map[int64]bool),
		client:           c,
		fetchConcurrency: defaultParallelReadConcurrency,
		fetchBlockSize:   defaultParallelReadBlockSize,
		fileSize:         fileSize,
		ctx:              ctx,
		cancel:           cancel,
		debug:            debugEnabled,
	}
	p.path.Store(path)
	return p
}

func (p *Prefetcher) SetParallelRead(concurrency int, blockSize int64) {
	if p == nil {
		return
	}
	if concurrency <= 0 {
		concurrency = defaultParallelReadConcurrency
	}
	if blockSize <= 0 {
		blockSize = defaultParallelReadBlockSize
	}
	p.mu.Lock()
	p.fetchConcurrency = concurrency
	p.fetchBlockSize = blockSize
	p.mu.Unlock()
}

func (p *Prefetcher) SetPerfCounters(perf *fusePerfCounters) {
	p.perf = perf
}

func (p *Prefetcher) SetReadTarget(target *client.ReadTarget) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.target = target
	p.mu.Unlock()
}

func (p *Prefetcher) SetPath(path string) {
	if p == nil {
		return
	}
	p.path.Store(path)
}

func (p *Prefetcher) pathString() string {
	if p == nil {
		return ""
	}
	if v := p.path.Load(); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (p *Prefetcher) debugf(format string, args ...any) {
	if p == nil || !p.debug {
		return
	}
	log.Printf("dat9 debug: prefetch "+format, args...)
}

// Get checks the prefetch cache for data at [offset, offset+size).
// Returns the data and true on hit, or nil and false on miss.
func (p *Prefetcher) Get(offset int64, size int) ([]byte, bool) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.debugf("get closed path=%s offset=%d size=%d", p.pathString(), offset, size)
		return nil, false
	}
	block, ok := p.cache[offset]
	p.mu.Unlock()

	if !ok {
		p.debugf("miss path=%s offset=%d size=%d", p.pathString(), offset, size)
		return nil, false
	}

	// Wait for the block to be ready (or context cancellation)
	waitStart := time.Now()
	select {
	case <-block.ready:
	case <-p.ctx.Done():
		p.debugf("wait canceled path=%s offset=%d size=%d wait=%s", p.pathString(), offset, size, time.Since(waitStart))
		return nil, false
	}
	wait := time.Since(waitStart)

	if block.err != nil {
		// Remove failed block — verify identity to avoid deleting a replacement
		p.mu.Lock()
		if p.cache[offset] == block {
			delete(p.cache, offset)
		}
		p.mu.Unlock()
		p.debugf("block error path=%s offset=%d size=%d wait=%s err=%v", p.pathString(), offset, size, wait, block.err)
		return nil, false
	}

	// Trim to requested size
	data := block.data
	if len(data) < size && offset+int64(len(data)) < p.fileSize {
		// A prefetched chunk smaller than the caller's request is only valid at
		// EOF. Returning it for an interior range exposes a short read to FUSE.
		p.mu.Lock()
		if p.cache[offset] == block {
			delete(p.cache, offset)
		}
		p.mu.Unlock()
		p.debugf("short block path=%s offset=%d req=%d got=%d file_size=%d wait=%s", p.pathString(), offset, size, len(data), p.fileSize, wait)
		return nil, false
	}
	if len(data) > size {
		data = data[:size]
	}

	// Clean up used block — verify identity to avoid deleting a replacement
	p.mu.Lock()
	if p.cache[offset] == block {
		delete(p.cache, offset)
	}
	p.mu.Unlock()

	p.debugf("hit path=%s offset=%d req=%d got=%d wait=%s", p.pathString(), offset, size, len(data), wait)
	return data, true
}

// OnRead should be called after each Read() to trigger prefetching.
// offset is the read offset, size is the bytes read.
func (p *Prefetcher) OnRead(offset int64, size int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	// Track observed read size for chunk splitting.
	if size > 0 {
		p.readSize = size
	}

	end := offset + int64(size)

	// A read is sequential if its offset stays within the region the
	// prefetcher is tracking: no further than window bytes behind the
	// consumed frontier (nextExpect) and no further than window bytes ahead of
	// it. The kernel issues several reads ahead of the consumer position and
	// they may complete in any order, so a strict offset==nextExpect match
	// misclassified those out-of-order completions as random and reset the
	// prefetch window on every interleaving, collapsing prefetch to
	// single-stream fetch latency. A read that jumps outside the tracked
	// region — forward beyond nextExpect+window or backward beyond
	// nextExpect-window — is a genuine random seek and resets.
	withinWindow := offset >= p.nextExpect-p.window && offset <= p.nextExpect+p.window
	if withinWindow {
		if end > p.nextExpect {
			// Forward read advancing the frontier — grow the window and prefetch.
			p.window *= 2
			if p.window < int64(size) {
				p.window = int64(size)
			}
			if p.window > prefetchMaxWindow {
				p.window = prefetchMaxWindow
			}
			p.nextExpect = end
		}
		// Trigger prefetch for the next region beyond the frontier if not
		// already cached or inflight. This also covers end==nextExpect (a read
		// landing exactly at the frontier, e.g. the first read or a readahead
		// re-read) and out-of-order completions behind the frontier that are
		// within the window — neither advances the frontier, neither resets.
		prefetchStart := p.nextExpect
		if prefetchStart < p.fileSize && !p.inflight[prefetchStart] && p.cache[prefetchStart] == nil {
			p.startPrefetch(prefetchStart, p.window)
		}
	} else {
		// Random read outside the tracked region — reset.
		if p.nextExpect != 0 || len(p.cache) > 0 || len(p.inflight) > 0 {
			p.debugf("random reset path=%s offset=%d size=%d next_expect=%d cache=%d inflight=%d", p.pathString(), offset, size, p.nextExpect, len(p.cache), len(p.inflight))
		}
		p.window = prefetchMinWindow
		// Clear stale cache
		for k := range p.cache {
			delete(p.cache, k)
		}
		for k := range p.inflight {
			delete(p.inflight, k)
		}
		p.nextExpect = end
	}
}

// Close cancels all inflight prefetch goroutines and clears the cache.
// Safe to call multiple times. After Close, Get and OnRead are no-ops.
func (p *Prefetcher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	cacheLen := len(p.cache)
	inflightLen := len(p.inflight)
	p.closed = true
	p.cancel()
	// Clear cache to release memory
	for k := range p.cache {
		delete(p.cache, k)
	}
	for k := range p.inflight {
		delete(p.inflight, k)
	}
	p.debugf("close path=%s cache=%d inflight=%d", p.pathString(), cacheLen, inflightLen)
}

// startPrefetch launches background fetches. Caller must hold p.mu.
// It fetches [offset, offset+length) as bounded parallel range requests,
// then splits results into read-aligned chunks stored at their own offset keys.
func (p *Prefetcher) startPrefetch(offset, length int64) {
	if p.client == nil {
		return // no client available (e.g., in tests)
	}
	// Cap at file end
	if offset+length > p.fileSize {
		length = p.fileSize - offset
	}
	if length <= 0 {
		return
	}

	chunkSize := int64(p.readSize)
	if chunkSize <= 0 {
		chunkSize = 128 * 1024 // default 128KB if not yet observed
	}
	if length < chunkSize && offset+length < p.fileSize {
		length = chunkSize
		if offset+length > p.fileSize {
			length = p.fileSize - offset
		}
	}

	// Calculate how many chunks this window will produce, capped to avoid
	// unbounded map growth when readSize is small (e.g. 4KB reads with 16MB window).
	nChunks := int((length + chunkSize - 1) / chunkSize)
	if nChunks > prefetchMaxBlocks {
		nChunks = prefetchMaxBlocks
		length = int64(nChunks) * chunkSize
	}

	// Evict oldest blocks if needed to make room.
	for len(p.cache)+nChunks > prefetchMaxBlocks {
		var minOff int64 = 1<<63 - 1
		for k := range p.cache {
			if k < minOff {
				minOff = k
			}
		}
		if minOff == 1<<63-1 {
			break
		}
		delete(p.cache, minOff)
		delete(p.inflight, minOff)
	}

	// Create placeholder blocks for each chunk so Get() can find them.
	// All chunks share a single ready channel — they become available together.
	ready := make(chan struct{})
	blocks := make([]*prefetchBlock, nChunks)
	for i := range nChunks {
		off := offset + int64(i)*chunkSize
		b := &prefetchBlock{
			offset: off,
			ready:  ready,
		}
		blocks[i] = b
		p.cache[off] = b
	}
	// Mark the fetch start as inflight (not each chunk — OnRead checks start offset).
	p.inflight[offset] = true
	fetchPath := p.pathString()
	target := p.target
	fetchConcurrency := p.fetchConcurrency
	fetchBlockSize := alignPrefetchFetchBlockSize(p.fetchBlockSize, chunkSize)
	p.debugf("start path=%s offset=%d length=%d chunk_size=%d chunks=%d window=%d", fetchPath, offset, length, chunkSize, nChunks, p.window)

	ctx := p.ctx
	go func() {
		defer func() {
			p.mu.Lock()
			delete(p.inflight, offset)
			p.mu.Unlock()
			close(ready)
		}()

		fetchCtx, fetchCancel := context.WithCancel(ctx)
		defer fetchCancel()
		fetches := prefetchFetchPlan(offset, length, fetchBlockSize)
		workerCount := minInt(len(fetches), fetchConcurrency)
		if workerCount <= 0 {
			workerCount = 1
		}
		jobs := make(chan prefetchFetch)
		go func() {
			defer close(jobs)
			for _, fetch := range fetches {
				select {
				case jobs <- fetch:
				case <-fetchCtx.Done():
					return
				}
			}
		}()

		var wg sync.WaitGroup
		for range workerCount {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-fetchCtx.Done():
						return
					case fetch, ok := <-jobs:
						if !ok {
							return
						}
						start := time.Now()
						data, err := p.fetchRange(fetchCtx, fetchPath, target, fetch.offset, fetch.length)
						if p.perf != nil {
							p.perf.recordRemoteOp(perfRemoteRead, err, time.Since(start), uint64(len(data)))
						}
						if err != nil {
							p.markPrefetchBlocks(blocks, fetch, chunkSize, nil, err)
							p.debugf("fetch error path=%s offset=%d length=%d dur=%s err=%v", fetchPath, fetch.offset, fetch.length, time.Since(start), err)
							fetchCancel()
							continue
						}
						p.markPrefetchBlocks(blocks, fetch, chunkSize, data, nil)
						p.debugf("fetch done path=%s offset=%d length=%d got=%d dur=%s", fetchPath, fetch.offset, fetch.length, len(data), time.Since(start))
					}
				}
			}()
		}
		wg.Wait()
	}()
}

type prefetchFetch struct {
	offset int64
	length int64
}

func alignPrefetchFetchBlockSize(blockSize, chunkSize int64) int64 {
	if chunkSize <= 0 {
		return blockSize
	}
	if blockSize <= 0 {
		return chunkSize
	}
	if blockSize < chunkSize {
		return chunkSize
	}
	if rem := blockSize % chunkSize; rem != 0 {
		blockSize += chunkSize - rem
	}
	return blockSize
}

func prefetchFetchPlan(offset, length, blockSize int64) []prefetchFetch {
	if length <= 0 {
		return nil
	}
	if blockSize <= 0 || blockSize > length {
		blockSize = length
	}
	fetches := make([]prefetchFetch, 0, int((length+blockSize-1)/blockSize))
	for fetchOffset := offset; fetchOffset < offset+length; fetchOffset += blockSize {
		fetchLength := blockSize
		if fetchOffset+fetchLength > offset+length {
			fetchLength = offset + length - fetchOffset
		}
		fetches = append(fetches, prefetchFetch{offset: fetchOffset, length: fetchLength})
	}
	return fetches
}

func (p *Prefetcher) fetchRange(ctx context.Context, fetchPath string, target *client.ReadTarget, offset, length int64) ([]byte, error) {
	var rc io.ReadCloser
	var err error
	if target != nil {
		rc, err = p.client.ReadObjectRange(ctx, target, offset, length)
		if client.IsPresignExpired(err) {
			p.SetReadTarget(nil)
			rc, err = p.client.ReadStreamRange(ctx, fetchPath, offset, length)
		}
	} else {
		rc, err = p.client.ReadStreamRange(ctx, fetchPath, offset, length)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

func (p *Prefetcher) markPrefetchBlocks(blocks []*prefetchBlock, fetch prefetchFetch, chunkSize int64, data []byte, err error) {
	fetchEnd := fetch.offset + fetch.length
	for _, b := range blocks {
		if b.offset < fetch.offset || b.offset >= fetchEnd {
			continue
		}
		if err != nil {
			b.err = err
			continue
		}
		start := b.offset - fetch.offset
		expected := chunkSize
		if p.fileSize > 0 && b.offset+expected > p.fileSize {
			expected = p.fileSize - b.offset
		}
		if expected <= 0 {
			b.err = io.ErrUnexpectedEOF
			continue
		}
		end := start + expected
		if start >= int64(len(data)) || end > int64(len(data)) {
			b.err = io.ErrUnexpectedEOF
			continue
		}
		chunk := make([]byte, end-start)
		copy(chunk, data[start:end])
		b.data = chunk
	}
}
