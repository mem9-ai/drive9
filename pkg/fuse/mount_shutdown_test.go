package fuse

import (
	"errors"
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

func TestCleanupMountStartFailureUnmountsAfterStoppingWatchersAndFlushing(t *testing.T) {
	var calls []string

	cleanupMountStartFailure(mountStartCleanup{
		reason:     "test",
		mountPoint: "/mnt/drive9",
		stopWatchers: func() {
			calls = append(calls, "stop")
		},
		flushAll: func() {
			calls = append(calls, "flush")
		},
		unmount: func() error {
			calls = append(calls, "unmount")
			return nil
		},
		forceUnmount: func(string) {
			calls = append(calls, "force")
		},
	})

	want := []string{"stop", "flush", "unmount"}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", calls, want)
		}
	}
}

func TestCleanupMountStartFailureForceUnmountsWhenServerUnmountFails(t *testing.T) {
	var forced []string

	cleanupMountStartFailure(mountStartCleanup{
		reason:     "test",
		mountPoint: "/mnt/drive9",
		unmount: func() error {
			return errors.New("transport endpoint is not connected")
		},
		forceUnmount: func(mountPoint string) {
			forced = append(forced, mountPoint)
		},
	})

	if len(forced) != 1 || forced[0] != "/mnt/drive9" {
		t.Fatalf("forced = %v, want [/mnt/drive9]", forced)
	}
}
