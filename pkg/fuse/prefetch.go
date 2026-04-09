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
// Concurrency: Prefetcher is fully self-synchronized via p.mu.
// Callers do NOT need to hold any external lock.
// Lock ordering (if relevant): FileHandle.mu → Prefetcher.mu.
//
// Eviction: uses smallest-offset eviction, optimised for forward sequential
// reads (e.g. cat, cp). Reverse reads will thrash the cache — this is
// acceptable since the common case is forward streaming.
type Prefetcher struct {
	mu         sync.Mutex
	nextExpect int64 // next expected offset (for sequential detection)
	window     int64 // current prefetch window (adaptive)
	cache      map[int64]*prefetchBlock
	inflight   map[int64]bool
	client     *client.Client
	path       string
	fileSize   int64
}

// NewPrefetcher creates a Prefetcher for the given file.
func NewPrefetcher(c *client.Client, path string, fileSize int64) *Prefetcher {
	return &Prefetcher{
		nextExpect: 0,
		window:     prefetchMinWindow,
		cache:      make(map[int64]*prefetchBlock),
		inflight:   make(map[int64]bool),
		client:     c,
		path:       path,
		fileSize:   fileSize,
	}
}

// Get checks the prefetch cache for data at [offset, offset+size).
// Returns the data and true on hit, or nil and false on miss.
func (p *Prefetcher) Get(offset int64, size int) ([]byte, bool) {
	p.mu.Lock()
	block, ok := p.cache[offset]
	p.mu.Unlock()

	if !ok {
		return nil, false
	}

	// Wait for the block to be ready
	<-block.ready
	if block.err != nil {
		// Remove failed block
		p.mu.Lock()
		delete(p.cache, offset)
		p.mu.Unlock()
		return nil, false
	}

	// Trim to requested size
	data := block.data
	if len(data) > size {
		data = data[:size]
	}

	// Clean up used block
	p.mu.Lock()
	delete(p.cache, offset)
	p.mu.Unlock()

	return data, true
}

// OnRead should be called after each Read() to trigger prefetching.
// offset is the read offset, size is the bytes read.
func (p *Prefetcher) OnRead(offset int64, size int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset == p.nextExpect {
		// Sequential read detected — grow window
		p.window *= 2
		if p.window > prefetchMaxWindow {
			p.window = prefetchMaxWindow
		}

		// Trigger prefetch for next region
		prefetchStart := offset + int64(size)
		if prefetchStart < p.fileSize && !p.inflight[prefetchStart] {
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

	go func() {
		defer close(block.ready)

		rc, err := p.client.ReadStreamRange(context.Background(), p.path, offset, length)
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

		p.mu.Lock()
		delete(p.inflight, offset)
		p.mu.Unlock()
	}()
}
