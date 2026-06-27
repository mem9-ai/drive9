package fuse

import (
	"errors"
	"fmt"
	"strings"
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
	var logs strings.Builder

	cleanupMountStartFailure(mountStartCleanup{
		reason:     "test",
		mountPoint: "/mnt/drive9",
		cause:      errors.New("fuse init failed"),
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
		logf: func(format string, args ...any) {
			logs.WriteString(fmt.Sprintf(format, args...))
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
	gotLogs := logs.String()
	for _, wantLog := range []string{
		"drive9: mount startup failed during test at /mnt/drive9: fuse init failed",
		"drive9: cleanup after test: unmounted /mnt/drive9",
	} {
		if !strings.Contains(gotLogs, wantLog) {
			t.Fatalf("logs = %q, want substring %q", gotLogs, wantLog)
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

func TestCleanupNewServerFailureStopsFlushesAndForceUnmountsInitFailure(t *testing.T) {
	var calls []string
	var logs strings.Builder

	cleanupNewServerFailure(
		"/mnt/drive9",
		errors.New("init: ENODEV"),
		func() {
			calls = append(calls, "stop")
		},
		func() {
			calls = append(calls, "flush")
		},
		func(mountPoint string) {
			calls = append(calls, "force:"+mountPoint)
		},
		func(format string, args ...any) {
			logs.WriteString(fmt.Sprintf(format, args...))
		},
	)

	want := []string{"stop", "flush", "force:/mnt/drive9"}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", calls, want)
		}
	}
	gotLogs := logs.String()
	for _, wantLog := range []string{
		"drive9: mount startup failed during fuse server initialization failure at /mnt/drive9: init: ENODEV",
		"drive9: cleanup after fuse server initialization failure: forcing unmount of /mnt/drive9 after partial init failure",
	} {
		if !strings.Contains(gotLogs, wantLog) {
			t.Fatalf("logs = %q, want substring %q", gotLogs, wantLog)
		}
	}
}

func TestCleanupNewServerFailureDoesNotForceUnmountNonInitFailure(t *testing.T) {
	var calls []string

	cleanupNewServerFailure(
		"/mnt/drive9",
		errors.New("fusermount: mountpoint is busy"),
		func() {
			calls = append(calls, "stop")
		},
		func() {
			calls = append(calls, "flush")
		},
		func(mountPoint string) {
			calls = append(calls, "force:"+mountPoint)
		},
		func(string, ...any) {},
	)

	want := []string{"stop", "flush"}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", calls, want)
		}
	}
}

func TestCleanupMountStartFailureSkipsForceUnmountForEmptyMountPoint(t *testing.T) {
	forced := false

	cleanupMountStartFailure(mountStartCleanup{
		reason:                    "test",
		cause:                     errors.New("init: ENODEV"),
		forceUnmount:              func(string) { forced = true },
		forceUnmountWithoutServer: true,
		logf:                      func(string, ...any) {},
	})

	if forced {
		t.Fatal("forceUnmount should not be called for an empty mountpoint")
	}
}

func TestShouldForceUnmountAfterNewServerErrorOnlyForInitFailure(t *testing.T) {
	if !shouldForceUnmountAfterNewServerError(errors.New("init: ENODEV")) {
		t.Fatal("init failure should force cleanup")
	}
	if shouldForceUnmountAfterNewServerError(errors.New("fusermount: mountpoint is busy")) {
		t.Fatal("pre-mount failure should not force cleanup")
	}
	if shouldForceUnmountAfterNewServerError(nil) {
		t.Fatal("nil error should not force cleanup")
	}
}
