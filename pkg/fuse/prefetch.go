package fuse

import (
	"context"
	"io"
	"sync"

	"github.com/mem9-ai/dat9/pkg/client"
)

const (
	prefetchMinWindow = 256 * 1024       // 256KB
	prefetchMaxWindow = 16 * 1024 * 1024 // 16MB
	prefetchMaxBlocks = 4                // max cached prefetch blocks
)

// prefetchBlock holds prefetched data for a byte range [offset, offset+len(data)).
type prefetchBlock struct {
	offset int64
	data   []byte
	ready  chan struct{} // closed when data is available
	err    error
}

// Prefetcher detects sequential read patterns and prefetches upcoming data
// blocks in the background, reducing HTTP round-trips for large file reads.
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
	mu         sync.Mutex
	nextExpect int64 // next expected offset (for sequential detection)
	window     int64 // current prefetch window (adaptive)
	cache      map[int64]*prefetchBlock
	inflight   map[int64]bool
	client     *client.Client
	path       string
	fileSize   int64
	cancel     context.CancelFunc // cancels all inflight prefetch goroutines
	ctx        context.Context    // parent context for prefetch goroutines
	closed     bool
}

// NewPrefetcher creates a Prefetcher for the given file.
func NewPrefetcher(c *client.Client, path string, fileSize int64) *Prefetcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Prefetcher{
		nextExpect: 0,
		window:     prefetchMinWindow,
		cache:      make(map[int64]*prefetchBlock),
		inflight:   make(map[int64]bool),
		client:     c,
		path:       path,
		fileSize:   fileSize,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Get checks the prefetch cache for data at [offset, offset+size).
// A block is a hit if it contains the requested offset (sub-block reads).
// Returns the data and true on hit, or nil and false on miss.
func (p *Prefetcher) Get(offset int64, size int) ([]byte, bool) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, false
	}

	// Find a block that covers this offset.
	// Fast path: exact offset match (first read into a block).
	block, ok := p.cache[offset]
	if !ok {
		// Slow path: scan for a ready block whose data range covers offset.
		// Only check ready blocks — inflight blocks have unknown actual size.
		block = p.findReadyBlockLocked(offset)
		ok = block != nil
	}
	p.mu.Unlock()

	if !ok {
		return nil, false
	}

	// Wait for the block to be ready (or context cancellation).
	// For blocks found via exact match, they may still be inflight.
	select {
	case <-block.ready:
	case <-p.ctx.Done():
		return nil, false
	}

	if block.err != nil {
		// Remove failed block
		p.mu.Lock()
		if p.cache[block.offset] == block {
			delete(p.cache, block.offset)
		}
		p.mu.Unlock()
		return nil, false
	}

	// Calculate the sub-range within the block.
	blockEnd := block.offset + int64(len(block.data))
	if offset < block.offset || offset >= blockEnd {
		// Shouldn't happen, but be defensive.
		return nil, false
	}

	start := int(offset - block.offset)
	end := start + size
	if end > len(block.data) {
		end = len(block.data)
	}
	// Copy to decouple from the prefetch buffer.
	data := make([]byte, end-start)
	copy(data, block.data[start:end])

	// Evict the block once the read has consumed past its end.
	readEnd := offset + int64(size)
	if readEnd >= blockEnd {
		p.mu.Lock()
		if p.cache[block.offset] == block {
			delete(p.cache, block.offset)
		}
		p.mu.Unlock()
	}

	return data, true
}

// findReadyBlockLocked scans the cache for a ready (non-inflight) block
// whose data range covers the given offset. Returns nil if none found.
// Caller must hold p.mu.
func (p *Prefetcher) findReadyBlockLocked(offset int64) *prefetchBlock {
	for _, b := range p.cache {
		select {
		case <-b.ready:
			if b.err == nil && offset >= b.offset && offset < b.offset+int64(len(b.data)) {
				return b
			}
		default:
			// Block still inflight — skip.
		}
	}
	return nil
}

// OnRead should be called after each Read() to trigger prefetching.
// offset is the read offset, size is the bytes read.
func (p *Prefetcher) OnRead(offset int64, size int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	if offset == p.nextExpect {
		// Sequential read detected — grow window
		p.window *= 2
		if p.window > prefetchMaxWindow {
			p.window = prefetchMaxWindow
		}

		// Trigger prefetch for the region after the current read.
		prefetchStart := offset + int64(size)
		if prefetchStart < p.fileSize && !p.inflight[prefetchStart] && p.findReadyBlockLocked(prefetchStart) == nil {
			p.startPrefetch(prefetchStart, p.window)
		}
	} else {
		// Random read — reset
		p.window = prefetchMinWindow
		// Clear stale cache
		for k := range p.cache {
			delete(p.cache, k)
		}
		for k := range p.inflight {
			delete(p.inflight, k)
		}
	}

	p.nextExpect = offset + int64(size)
}

// Close cancels all inflight prefetch goroutines and clears the cache.
// Safe to call multiple times. After Close, Get and OnRead are no-ops.
func (p *Prefetcher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true
	p.cancel()
	// Clear cache to release memory
	for k := range p.cache {
		delete(p.cache, k)
	}
	for k := range p.inflight {
		delete(p.inflight, k)
	}
}

// startPrefetch launches a background fetch. Caller must hold p.mu.
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

	// Evict oldest blocks if at capacity
	for len(p.cache) >= prefetchMaxBlocks {
		// Find and remove the block with smallest offset
		var minOff int64 = 1<<63 - 1
		for k := range p.cache {
			if k < minOff {
				minOff = k
			}
		}
		delete(p.cache, minOff)
		delete(p.inflight, minOff)
	}

	block := &prefetchBlock{
		offset: offset,
		ready:  make(chan struct{}),
	}
	p.cache[offset] = block
	p.inflight[offset] = true

	ctx := p.ctx
	go func() {
		defer func() {
			p.mu.Lock()
			delete(p.inflight, offset)
			p.mu.Unlock()
			close(block.ready)
		}()

		rc, err := p.client.ReadStreamRange(ctx, p.path, offset, length)
		if err != nil {
			block.err = err
			return
		}
		defer func() { _ = rc.Close() }()

		data, err := io.ReadAll(rc)
		if err != nil {
			block.err = err
			return
		}
		block.data = data
	}()
}
