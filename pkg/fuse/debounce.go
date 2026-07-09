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
	mu       sync.Mutex
	delay    time.Duration
	pending  map[string]*pendingFlush
	inflight map[string]*pendingFlush // callbacks currently executing uploadFn
}

type pendingFlush struct {
	timer    *time.Timer
	uploadFn func()
	done     chan struct{}
	once     sync.Once
}

// finish closes the done channel exactly once. It is safe to call from
// multiple goroutines (Cancel, Schedule, and the timer callback).
func (pf *pendingFlush) finish() {
	pf.once.Do(func() { close(pf.done) })
}

func newFlushDebouncer(delay time.Duration) *flushDebouncer {
	if delay < 0 {
		delay = 0
	}
	return &flushDebouncer{
		delay:    delay,
		pending:  make(map[string]*pendingFlush),
		inflight: make(map[string]*pendingFlush),
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
		pf.finish()
		delete(d.pending, path)
	}

	pf := &pendingFlush{uploadFn: uploadFn, done: make(chan struct{})}
	pf.timer = time.AfterFunc(d.delay, func() {
		d.mu.Lock()
		current, ok := d.pending[path]
		if ok {
			delete(d.pending, path)
			d.inflight[path] = current
		}
		d.mu.Unlock()

		if ok && current.uploadFn != nil {
			current.uploadFn()
		}

		d.mu.Lock()
		delete(d.inflight, path)
		d.mu.Unlock()
		current.finish()
	})
	d.pending[path] = pf
}

// Cancel stops and removes the pending debounce for path without executing
// the upload. If the timer callback is already running uploadFn, Cancel
// blocks until it finishes so callers can safely check server-side state
// (e.g. Unlink must see the pendingIndex cleanup performed by the callback).
//
// Cancel must NOT be called while holding the handle lock — the callback
// acquires the handle lock and would deadlock. Use CancelNoWait instead.
func (d *flushDebouncer) Cancel(path string) {
	d.mu.Lock()
	pf, ok := d.pending[path]
	if ok {
		pf.timer.Stop()
		pf.finish()
		delete(d.pending, path)
	}
	running, rok := d.inflight[path]
	d.mu.Unlock()

	if rok {
		<-running.done
	}
}

// CancelNoWait stops and removes the pending debounce for path without
// waiting for an already-running callback. Safe to call while holding the
// handle lock (e.g. Release, flushHandle). Callers that need to know the
// upload completed should use Cancel instead.
func (d *flushDebouncer) CancelNoWait(path string) {
	d.mu.Lock()
	pf, ok := d.pending[path]
	if ok {
		pf.timer.Stop()
		pf.finish()
		delete(d.pending, path)
	}
	d.mu.Unlock()
}

// FlushAll executes all pending uploads immediately and clears the pending map.
// Used during graceful shutdown.
func (d *flushDebouncer) FlushAll() {
	d.mu.Lock()
	pending := d.pending
	d.pending = make(map[string]*pendingFlush)
	d.mu.Unlock()

	for _, pf := range pending {
		pf.timer.Stop()
		if pf.uploadFn != nil {
			pf.uploadFn()
		}
		pf.finish()
	}

	// Wait for any callbacks that were already running when FlushAll was called.
	d.mu.Lock()
	inflight := d.inflight
	d.mu.Unlock()
	for _, pf := range inflight {
		<-pf.done
	}
}
