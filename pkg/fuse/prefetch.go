package fuse

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
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
// Design: a single HTTP request fetches a large window of data. The result
// is split into read-aligned chunks, each stored at its own offset key.
// Get() uses exact-offset matching — no sub-block scanning needed.
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
	readSize   int   // observed FUSE read size (for chunk splitting)
	cache      map[int64]*prefetchBlock
	inflight   map[int64]bool
	client     *client.Client
	path       string
	fileSize   int64
	cancel     context.CancelFunc // cancels all inflight prefetch goroutines
	ctx        context.Context    // parent context for prefetch goroutines
	closed     bool
	debug      bool
}

// NewPrefetcher creates a Prefetcher for the given file.
func NewPrefetcher(c *client.Client, path string, fileSize int64, debug ...bool) *Prefetcher {
	ctx, cancel := context.WithCancel(context.Background())
	debugEnabled := false
	if len(debug) > 0 {
		debugEnabled = debug[0]
	}
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
		debug:      debugEnabled,
	}
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
		p.debugf("get closed path=%s offset=%d size=%d", p.path, offset, size)
		return nil, false
	}
	block, ok := p.cache[offset]
	p.mu.Unlock()

	if !ok {
		p.debugf("miss path=%s offset=%d size=%d", p.path, offset, size)
		return nil, false
	}

	// Wait for the block to be ready (or context cancellation)
	waitStart := time.Now()
	select {
	case <-block.ready:
	case <-p.ctx.Done():
		p.debugf("wait canceled path=%s offset=%d size=%d wait=%s", p.path, offset, size, time.Since(waitStart))
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
		p.debugf("block error path=%s offset=%d size=%d wait=%s err=%v", p.path, offset, size, wait, block.err)
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
		p.debugf("short block path=%s offset=%d req=%d got=%d file_size=%d wait=%s", p.path, offset, size, len(data), p.fileSize, wait)
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

	p.debugf("hit path=%s offset=%d req=%d got=%d wait=%s", p.path, offset, size, len(data), wait)
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

	if offset == p.nextExpect {
		// Sequential read detected — grow window
		p.window *= 2
		if p.window < int64(size) {
			p.window = int64(size)
		}
		if p.window > prefetchMaxWindow {
			p.window = prefetchMaxWindow
		}

		// Trigger prefetch for next region if not already cached or inflight.
		prefetchStart := offset + int64(size)
		if prefetchStart < p.fileSize && !p.inflight[prefetchStart] && p.cache[prefetchStart] == nil {
			p.startPrefetch(prefetchStart, p.window)
		}
	} else {
		// Random read — reset
		if p.nextExpect != 0 || len(p.cache) > 0 || len(p.inflight) > 0 {
			p.debugf("random reset path=%s offset=%d size=%d next_expect=%d cache=%d inflight=%d", p.path, offset, size, p.nextExpect, len(p.cache), len(p.inflight))
		}
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
	p.debugf("close path=%s cache=%d inflight=%d", p.path, cacheLen, inflightLen)
}

// startPrefetch launches a background fetch. Caller must hold p.mu.
// It fetches [offset, offset+length) in one HTTP request, then splits
// the result into read-aligned chunks stored at their own offset keys.
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
	p.debugf("start path=%s offset=%d length=%d chunk_size=%d chunks=%d window=%d", p.path, offset, length, chunkSize, nChunks, p.window)

	ctx := p.ctx
	go func() {
		start := time.Now()
		defer func() {
			p.mu.Lock()
			delete(p.inflight, offset)
			p.mu.Unlock()
			close(ready)
		}()

		rc, err := p.client.ReadStreamRange(ctx, p.path, offset, length)
		if err != nil {
			for _, b := range blocks {
				b.err = err
			}
			p.debugf("fetch open error path=%s offset=%d length=%d dur=%s err=%v", p.path, offset, length, time.Since(start), err)
			return
		}
		defer func() { _ = rc.Close() }()

		data, err := io.ReadAll(rc)
		if err != nil {
			for _, b := range blocks {
				b.err = err
			}
			p.debugf("fetch read error path=%s offset=%d length=%d got=%d dur=%s err=%v", p.path, offset, length, len(data), time.Since(start), err)
			return
		}

		// Split data into chunks.
		for i, b := range blocks {
			start := int64(i) * chunkSize
			end := start + chunkSize
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			if start >= int64(len(data)) {
				b.err = io.ErrUnexpectedEOF
				continue
			}
			// Copy to give each chunk its own backing array.
			chunk := make([]byte, end-start)
			copy(chunk, data[start:end])
			b.data = chunk
		}
		p.debugf("fetch done path=%s offset=%d length=%d got=%d chunks=%d dur=%s", p.path, offset, length, len(data), nChunks, time.Since(start))
	}()
}
