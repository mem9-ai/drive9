package fuse

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"

	"github.com/mem9-ai/drive9/pkg/extent"
)

// extentShutdownRecorder is an object store that records Shutdown calls.
type extentShutdownRecorder struct {
	object.ObjectStorage
	shutdowns atomic.Int64
}

func (s *extentShutdownRecorder) Shutdown() { s.shutdowns.Add(1) }

// TestExtentTeardownStopsAndClosesRuntime pins the teardown hook: it marks the
// fs torn down, stops the compaction loop, closes the runtime exactly once,
// clears the pointer, and refuses a later ensureExtentRuntime.
func TestExtentTeardownStopsAndClosesRuntime(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rec := &extentShutdownRecorder{ObjectStorage: inner}
	var stopped atomic.Bool
	fs := &Dat9FS{}
	fs.extentRT.Store(&extentRuntime{
		rt:   &extent.Runtime{Storage: rec},
		hold: newExtentMetaHold(),
		stop: func() { stopped.Store(true) },
	})

	fs.stopExtentRuntimeLoop()
	if !stopped.Load() {
		t.Fatal("compaction loop was not stopped")
	}
	if !fs.extentTornDown {
		t.Fatal("fs was not marked torn down")
	}

	fs.closeExtentRuntime()
	if fs.extentRT.Load() != nil {
		t.Fatal("extentRT still set after teardown")
	}
	if got := rec.shutdowns.Load(); got != 1 {
		t.Fatalf("runtime shutdowns = %d, want 1", got)
	}

	if err := fs.ensureExtentRuntime(); err == nil {
		t.Fatal("ensureExtentRuntime must refuse after teardown")
	}
}

// TestExtentMetaHoldDrains pins the teardown drain: drain waits for in-flight
// metadata RPCs instead of refusing them, so the session-close RPC teardown
// issues next is still admitted; close (after the session is gone) refuses.
func TestExtentMetaHoldDrains(t *testing.T) {
	h := newExtentMetaHold()
	if !h.enter() {
		t.Fatal("first enter must be admitted")
	}
	drained := make(chan bool, 1)
	go func() { drained <- h.drain(2 * time.Second) }()
	select {
	case <-drained:
		t.Fatal("drain returned with an RPC in flight")
	case <-time.After(50 * time.Millisecond):
	}
	if !h.enter() {
		t.Fatal("drain must keep admitting RPCs; teardown's session-close call needs the gate")
	}
	h.leave()
	h.leave()
	select {
	case ok := <-drained:
		if !ok {
			t.Fatal("drain must report drained after the RPCs finished")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not return after the RPCs finished")
	}
	if !h.enter() {
		t.Fatal("enter must stay admitted until close; the session is still open")
	}
	h.leave()
	h.close()
	if h.enter() {
		t.Fatal("enter must be refused after close")
	}
}

// TestCloseExtentRuntimeDrainsWithoutRefusing drives closeExtentRuntime: while a
// metadata RPC is in flight the drain waits, and it keeps admitting new RPCs so
// the runtime's own session-close call is not refused by the gate that protects
// the session. Only after the session is closed does it refuse further RPCs.
func TestCloseExtentRuntimeDrainsWithoutRefusing(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rec := &extentShutdownRecorder{ObjectStorage: inner}
	hold := newExtentMetaHold()
	hold.mu.Lock()
	hold.active = 1 // one in-flight metadata RPC
	hold.mu.Unlock()

	fs := &Dat9FS{}
	fs.extentRT.Store(&extentRuntime{rt: &extent.Runtime{Storage: rec}, hold: hold})

	done := make(chan struct{})
	go func() {
		fs.closeExtentRuntime()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("closeExtentRuntime returned while a metadata RPC was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	if !hold.enter() {
		t.Fatal("drain must keep admitting RPCs; the session-close call needs the gate")
	}
	hold.leave()

	// Finish the in-flight RPC; the drain can now complete.
	hold.mu.Lock()
	hold.active = 0
	hold.cond.Broadcast()
	hold.mu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("closeExtentRuntime did not finish after the RPC drained")
	}
	if hold.enter() {
		t.Fatal("enter must be refused after closeExtentRuntime")
	}
	if got := rec.shutdowns.Load(); got != 1 {
		t.Fatalf("storage shutdowns = %d, want 1", got)
	}
}

// TestEnsureExtentRuntimeWaiterWakesOnTeardown parks a single-flight waiter
// behind an in-progress build, then tears down: the teardown must broadcast the
// build condition so the waiter does not linger forever.
func TestEnsureExtentRuntimeWaiterWakesOnTeardown(t *testing.T) {
	fs := &Dat9FS{}
	fs.extentMu.Lock()
	fs.extentBuildCond = sync.NewCond(&fs.extentMu)
	fs.extentBuilding = true
	fs.extentBuildGen = 1
	fs.extentMu.Unlock()

	done := make(chan error, 1)
	go func() { done <- fs.ensureExtentRuntime() }()
	select {
	case err := <-done:
		t.Fatalf("waiter returned before teardown: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	fs.stopExtentRuntimeLoop()
	select {
	case err := <-done:
		if !errors.Is(err, errExtentTornDown) {
			t.Fatalf("waiter err = %v, want errExtentTornDown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not woken by teardown")
	}
}

// TestEnsureExtentRuntimeWaitersShareBuildError pins the single-flight outcome:
// every waiter parked on a failed build gets that same error, and a later caller
// starts a fresh build instead of inheriting the stale one.
func TestEnsureExtentRuntimeWaitersShareBuildError(t *testing.T) {
	boom := errors.New("build boom")
	fs := &Dat9FS{}
	fs.extentMu.Lock()
	fs.extentBuildCond = sync.NewCond(&fs.extentMu)
	fs.extentBuilding = true
	fs.extentBuildGen = 1
	fs.extentMu.Unlock()

	done := make(chan error, 1)
	go func() { done <- fs.ensureExtentRuntime() }()
	select {
	case err := <-done:
		t.Fatalf("waiter returned before the build finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Simulate the in-progress build failing, as ensureExtentRuntime's deferred
	// publisher would.
	fs.extentMu.Lock()
	fs.extentBuilding = false
	fs.extentBuildErr = boom
	fs.extentBuildCond.Broadcast()
	fs.extentMu.Unlock()

	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Fatalf("waiter err = %v, want %v", err, boom)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not observe the failed build")
	}

	// A fresh caller (generation moved on) attempts its own build. With no
	// client it fails at setup, not with the stale error, proving it did not
	// short-circuit on the previous failure.
	if err := fs.ensureExtentRuntime(); err == nil || errors.Is(err, boom) {
		t.Fatalf("fresh caller err = %v, want an unrelated build error", err)
	}
}

// TestExtentRuntimePointerRace drives a hot runtime reader concurrently with
// the teardown that clears the pointer. Under -race it pins the atomic-snapshot
// discipline: a bare field read/write here is a data race.
func TestExtentRuntimePointerRace(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rec := &extentShutdownRecorder{ObjectStorage: inner}
	fs := &Dat9FS{}
	fs.extentRT.Store(&extentRuntime{rt: &extent.Runtime{Storage: rec}, hold: newExtentMetaHold()})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			_ = fs.extentVFS()
		}
	}()
	fs.stopExtentRuntimeLoop()
	fs.closeExtentRuntime()
	wg.Wait()
	if fs.extentRT.Load() != nil {
		t.Fatal("extentRT still set after teardown")
	}
}
