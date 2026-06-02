package fuse

import "sync"

// singleflightResult holds the result of a singleflight call.
type singleflightResult struct {
	data []byte
	err  error
}

// singleflightCall represents an in-progress or completed call.
type singleflightCall struct {
	wg  sync.WaitGroup
	val []byte
	err error
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
// and returns the same result. The returned data slice is shared among
// all callers for the same key — callers MUST NOT mutate it.
//
// The boolean return value is true if the caller was the one that
// executed fn (the "owner"), false if it piggybacked on another call.
func (sf *SingleFlight) Do(key string, fn func() ([]byte, error)) ([]byte, error, bool) {
	sf.mu.Lock()
	if c, ok := sf.calls[key]; ok {
		// Another goroutine is already fetching this key.
		sf.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, false
	}

	c := &singleflightCall{}
	c.wg.Add(1)
	sf.calls[key] = c
	sf.mu.Unlock()

	// Execute the function.
	c.val, c.err = fn()

	// Remove from map and wake waiters.
	sf.mu.Lock()
	delete(sf.calls, key)
	sf.mu.Unlock()

	c.wg.Done()

	return c.val, c.err, true
}

// Inflight returns the number of keys currently being fetched.
// This is intended for testing and metrics only.
func (sf *SingleFlight) Inflight() int {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return len(sf.calls)
}
