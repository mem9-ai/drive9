package fuse

import (
	"context"
	"sync"
)

// singleflightCall represents an in-progress or completed call.
type singleflightCall struct {
	done chan struct{} // closed when the call completes
	val  []byte
	err  error
}

// SingleFlight deduplicates concurrent calls for the same key.
// When multiple goroutines call Do with the same key concurrently,
// only one executes the function; the others wait and receive
// the same result. This prevents thundering-herd HTTP requests
// when multiple FUSE reads hit the same uncached file simultaneously.
type SingleFlight struct {
	mu    sync.Mutex
	calls map[string]*singleflightCall
}

// NewSingleFlight creates a new SingleFlight instance.
func NewSingleFlight() *SingleFlight {
	return &SingleFlight{
		calls: make(map[string]*singleflightCall),
	}
}

// Do executes fn once for the given key, deduplicating concurrent calls.
// If a call for key is already in progress, Do blocks until it completes
// or the context is cancelled. The returned data slice is shared among
// all callers for the same key — callers MUST NOT mutate it.
//
// The boolean return value is true if the caller was the one that
// executed fn (the "owner"), false if it piggybacked on another call.
//
// When a piggybacker's context is cancelled before the owner finishes,
// Do returns ctx.Err(). The owner call is NOT cancelled — it runs to
// completion so that other waiters (and the cache) still get the result.
func (sf *SingleFlight) Do(ctx context.Context, key string, fn func() ([]byte, error)) ([]byte, error, bool) {
	sf.mu.Lock()
	if c, ok := sf.calls[key]; ok {
		// Another goroutine is already fetching this key.
		sf.mu.Unlock()
		select {
		case <-c.done:
			return c.val, c.err, false
		case <-ctx.Done():
			return nil, ctx.Err(), false
		}
	}

	c := &singleflightCall{
		done: make(chan struct{}),
	}
	sf.calls[key] = c
	sf.mu.Unlock()

	// Execute the function.
	c.val, c.err = fn()

	// Remove from map and wake waiters.
	sf.mu.Lock()
	delete(sf.calls, key)
	sf.mu.Unlock()

	close(c.done)

	return c.val, c.err, true
}

// Inflight returns the number of keys currently being fetched.
// This is intended for testing and metrics only.
func (sf *SingleFlight) Inflight() int {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return len(sf.calls)
}
