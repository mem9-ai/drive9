package fuse

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestKernelNotificationsWaitsForCompleteWorkAndRemainsReusable(t *testing.T) {
	var n kernelNotifications
	for round := 0; round < 2; round++ {
		entered, release := make(chan struct{}), make(chan struct{})
		n.run(func() { close(entered); <-release })
		<-entered
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := n.wait(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		n.mu.Lock()
		active := n.active
		n.mu.Unlock()
		if active != 1 {
			t.Fatalf("cancellation discarded running work: %d", active)
		}
		close(release)
		if err := n.wait(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestKernelNotificationsConcurrentDrainAndEnqueue(t *testing.T) {
	var n kernelNotifications
	var producers sync.WaitGroup
	var completed atomic.Int32
	for i := 0; i < 8; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for j := 0; j < 100; j++ {
				n.run(func() { completed.Add(1) })
				if err := n.wait(t.Context()); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	producers.Wait()
	if err := n.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := completed.Load(); got != 800 {
		t.Fatalf("lost notifications: %d", got)
	}
}

type blockedResetNotifier struct {
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
	mu         sync.Mutex
	inodeCalls int
	entryCalls int
}

func (n *blockedResetNotifier) InodeNotify(_ uint64, _, _ int64) gofuse.Status {
	n.once.Do(func() { close(n.entered); <-n.release })
	n.mu.Lock()
	n.inodeCalls++
	n.mu.Unlock()
	return gofuse.OK
}
func (n *blockedResetNotifier) EntryNotify(_ uint64, _ string) gofuse.Status {
	n.mu.Lock()
	n.entryCalls++
	n.mu.Unlock()
	return gofuse.OK
}

func TestKernelNotificationsIncludeInvalidateAllReset(t *testing.T) {
	current := newXattrFS(t)
	current.inodes.Lookup("/a", false, 1, time.Time{})
	probe := &blockedResetNotifier{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(probe.release) }) }
	t.Cleanup(release)
	current.invalidateAllWithNotifier(probe)
	<-probe.entered
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := current.notifications.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// The real drain path must include a reset's detached notification job.
	resp := current.Drain(ctx)
	if resp.OK || len(resp.Phases) == 0 || resp.Phases[len(resp.Phases)-1].Name != "notifications" {
		t.Fatalf("drain ignored reset: %+v", resp)
	}
	release()
	if err := current.notifications.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if drained := current.Drain(t.Context()); !drained.OK {
		t.Fatalf("completed reset still pending: %+v", drained)
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if probe.inodeCalls != 2 || probe.entryCalls != 1 {
		t.Fatalf("changed reset semantics: inode=%d entry=%d", probe.inodeCalls, probe.entryCalls)
	}
}
