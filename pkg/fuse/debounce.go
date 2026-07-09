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
//
// Concurrency model: at most one callback per path is executing at any time
// (tracked in inflight). The timer callback captures the specific pendingFlush
// it was created for and verifies it is still the current pending entry (via
// pointer identity) before running; this prevents a stale timer from acting on
// a newer Schedule's entry or clobbering a newer inflight slot. Cancel and
// CancelNoWait are safe to call concurrently with the timer callback. Cancel
// blocks until an in-flight callback for the path finishes so callers (e.g.
// Unlink) can safely inspect server-side state afterwards.
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
//
// If a callback for path is already executing (inflight), the new entry is
// still recorded in pending so it fires after the in-flight callback finishes;
// the timer callback will not run a second concurrent callback for the same
// path — it waits for the existing inflight to drain first.
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
		d.runCallback(path, pf)
	})
	d.pending[path] = pf
}

// runCallback is the timer body. It verifies pf is still the current pending
// entry for path (via pointer identity), waits for any pre-existing in-flight
// callback to finish, then promotes pf to inflight and runs uploadFn.
func (d *flushDebouncer) runCallback(path string, pf *pendingFlush) {
	for {
		d.mu.Lock()
		// Verify pf is still the current pending entry. A newer Schedule may
		// have replaced it (and finished the old done channel); in that case
		// this timer is stale and must not run.
		current, ok := d.pending[path]
		if !ok || current != pf {
			d.mu.Unlock()
			return
		}
		// If a previous callback is still in-flight for this path, wait for it
		// to finish before promoting ourselves. This prevents two same-path
		// callbacks from running concurrently and clobbering the inflight slot.
		if running, rok := d.inflight[path]; rok {
			d.mu.Unlock()
			<-running.done
			continue // re-check pending in case a newer Schedule arrived
		}
		delete(d.pending, path)
		d.inflight[path] = pf
		d.mu.Unlock()

		if pf.uploadFn != nil {
			pf.uploadFn()
		}

		d.mu.Lock()
		// Only delete inflight if pf is still the one we installed. A newer
		// Schedule cannot have overwritten inflight (we hold the slot until
		// here), but guard defensively.
		if d.inflight[path] == pf {
			delete(d.inflight, path)
		}
		d.mu.Unlock()
		pf.finish()
		return
	}
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
// Used during graceful shutdown. It waits for any callbacks that were already
// in-flight when called to finish, avoiding a concurrent map iteration/write.
func (d *flushDebouncer) FlushAll() {
	d.mu.Lock()
	// Snapshot pending entries and clear the map under the lock.
	pending := make([]*pendingFlush, 0, len(d.pending))
	for _, pf := range d.pending {
		pending = append(pending, pf)
	}
	d.pending = make(map[string]*pendingFlush)
	// Snapshot inflight pointers under the lock so the range below does not
	// race with timer callbacks that delete from d.inflight concurrently.
	inflight := make([]*pendingFlush, 0, len(d.inflight))
	for _, pf := range d.inflight {
		inflight = append(inflight, pf)
	}
	d.mu.Unlock()

	for _, pf := range pending {
		pf.timer.Stop()
		if pf.uploadFn != nil {
			pf.uploadFn()
		}
		pf.finish()
	}

	for _, pf := range inflight {
		<-pf.done
	}
}