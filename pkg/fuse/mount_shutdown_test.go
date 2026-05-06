package fuse

import (
	"sync"
	"testing"
)

func TestMountShutdownRunsOnce(t *testing.T) {
	var mu sync.Mutex
	stopCount := 0
	flushCount := 0
	shutdown := newMountShutdown(
		func() {
			mu.Lock()
			stopCount++
			mu.Unlock()
		},
		func() {
			mu.Lock()
			flushCount++
			mu.Unlock()
		},
	)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			shutdown()
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if stopCount != 1 {
		t.Fatalf("stopCount = %d, want 1", stopCount)
	}
	if flushCount != 1 {
		t.Fatalf("flushCount = %d, want 1", flushCount)
	}
}
