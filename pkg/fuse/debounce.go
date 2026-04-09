package fuse

import (
	"sync"
	"time"
)

const defaultFlushDebounce = 2 * time.Second

// flushDebouncer coalesces rapid flush calls for the same path. When Schedule
// is called for a path that already has a pending timer, the previous timer is
// reset and the new uploadFn replaces the old one — only the latest snapshot
// will be uploaded.
type flushDebouncer struct {
	mu      sync.Mutex
	delay   time.Duration
	pending map[string]*pendingFlush
}

type pendingFlush struct {
	timer    *time.Timer
	uploadFn func()
}

func newFlushDebouncer(delay time.Duration) *flushDebouncer {
	if delay < 0 {
		delay = 0
	}
	return &flushDebouncer{
		delay:   delay,
		pending: make(map[string]*pendingFlush),
	}
}

// Schedule registers (or replaces) a deferred upload for path. If a previous
// timer exists for path, it is stopped and replaced. The uploadFn is called
// after delay elapses without another Schedule call for the same path.
func (d *flushDebouncer) Schedule(path string, uploadFn func()) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if pf, ok := d.pending[path]; ok {
		pf.timer.Stop()
		pf.uploadFn = uploadFn
		pf.timer.Reset(d.delay)
		return
	}

	pf := &pendingFlush{uploadFn: uploadFn}
	pf.timer = time.AfterFunc(d.delay, func() {
		d.mu.Lock()
		// Re-read the pending entry to get the latest uploadFn.
		current, ok := d.pending[path]
		if ok {
			delete(d.pending, path)
		}
		d.mu.Unlock()

		if ok && current.uploadFn != nil {
			current.uploadFn()
		}
	})
	d.pending[path] = pf
}

// Cancel stops and removes the pending debounce for path without executing
// the upload. Used when Release needs to do an immediate flush.
func (d *flushDebouncer) Cancel(path string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if pf, ok := d.pending[path]; ok {
		pf.timer.Stop()
		delete(d.pending, path)
	}
}

// FlushAll executes all pending uploads immediately and clears the pending map.
// Used during graceful shutdown.
func (d *flushDebouncer) FlushAll() {
	d.mu.Lock()
	// Snapshot and clear.
	pending := d.pending
	d.pending = make(map[string]*pendingFlush)
	d.mu.Unlock()

	for _, pf := range pending {
		pf.timer.Stop()
		if pf.uploadFn != nil {
			pf.uploadFn()
		}
	}
}
