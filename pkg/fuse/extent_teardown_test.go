package fuse

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
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

// TestExtentMetaHoldDrains pins the teardown gate: it waits for in-flight
// metadata RPCs, refuses new non-cleanup RPCs (a late handler), but admits the
// session-cleanup RPC teardown must run itself.
func TestExtentMetaHoldDrains(t *testing.T) {
	h := newExtentMetaHold()
	if !h.enter(jfsmeta.Drive9OpLookup) {
		t.Fatal("first enter must be admitted")
	}
	// A zero-timeout close closes the gate synchronously but must report that the
	// in-flight RPC was not drained yet.
	if h.closeAndWait(0) {
		t.Fatal("closeAndWait(0) must report a timeout while an RPC is in flight")
	}
	if h.enter(jfsmeta.Drive9OpLookup) {
		t.Fatal("a late non-cleanup RPC must be refused once the gate is closed")
	}
	if !h.enter(jfsmeta.Drive9OpCleanStaleSession) {
		t.Fatal("the session-cleanup RPC must be admitted through the closed gate")
	}
	h.leave() // the cleanup RPC

	// The original lookup is still in flight: a real closeAndWait returns true
	// only after it finishes.
	closed := make(chan bool, 1)
	go func() { closed <- h.closeAndWait(2 * time.Second) }()
	h.leave() // the pre-teardown RPC
	select {
	case ok := <-closed:
		if !ok {
			t.Fatal("closeAndWait must report drained after the RPC finished")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closeAndWait did not return after the RPC finished")
	}
	if h.enter(jfsmeta.Drive9OpLookup) {
		t.Fatal("a non-cleanup RPC must stay refused")
	}
	if !h.enter(jfsmeta.Drive9OpCleanStaleSession) {
		t.Fatal("the cleanup RPC must stay admitted")
	}
	h.leave()
}

// holdGatedMeta is a jfsmeta.Meta whose only live method, CloseSession, mirrors
// the transport's gate: it enters the hold before doing the cleanup and reports
// EIO when the gate refuses. Embedding the interface leaves every other method
// unimplemented, so a test that calls one panics loudly.
type holdGatedMeta struct {
	jfsmeta.Meta
	hold   *extentMetaHold
	closes atomic.Int64
}

func (m *holdGatedMeta) CloseSession() error {
	if m.hold != nil {
		if !m.hold.enter(jfsmeta.Drive9OpCleanStaleSession) {
			return syscall.EIO
		}
		defer m.hold.leave()
	}
	m.closes.Add(1)
	return nil
}

// TestCloseExtentRuntimeAttemptsSessionCleanup drives closeExtentRuntime and
// asserts its own session-cleanup RPC is attempted and admitted. With the old
// refuse-then-drain gate the hold was closed before CloseRuntime ran, so this
// call returned EIO and the session was never released.
func TestCloseExtentRuntimeAttemptsSessionCleanup(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rec := &extentShutdownRecorder{ObjectStorage: inner}
	hold := newExtentMetaHold()
	meta := &holdGatedMeta{hold: hold}

	fs := &Dat9FS{}
	fs.extentRT.Store(&extentRuntime{rt: &extent.Runtime{Meta: meta, Storage: rec}, hold: hold})
	fs.closeExtentRuntime()

	if got := meta.closes.Load(); got != 1 {
		t.Fatalf("CloseSession calls = %d, want 1 (the drain gate refused the cleanup RPC)", got)
	}
	if got := rec.shutdowns.Load(); got != 1 {
		t.Fatalf("storage shutdowns = %d, want 1", got)
	}
	if hold.enter(jfsmeta.Drive9OpLookup) {
		t.Fatal("a late non-cleanup RPC must be refused after closeExtentRuntime")
	}
}

// TestEnsureExtentRuntimeWaiterWakesOnTeardown parks a single-flight waiter
// behind an in-progress build, then tears down: the teardown must broadcast the
// build condition so the waiter does not linger forever.
func TestEnsureExtentRuntimeWaiterWakesOnTeardown(t *testing.T) {
	fs := &Dat9FS{}
	parked := make(chan struct{})
	fs.extentMu.Lock()
	fs.extentBuildCond = sync.NewCond(&fs.extentMu)
	fs.extentBuilding = true
	fs.extentBuildGen = 1
	fs.extentBuildWaitHook = func() { close(parked) }
	fs.extentMu.Unlock()

	done := make(chan error, 1)
	go func() { done <- fs.ensureExtentRuntime() }()
	<-parked // the waiter now holds extentMu and is about to park

	// stopExtentRuntimeLoop blocks on extentMu until the waiter's Wait releases
	// it, so the teardown broadcast can never race ahead of the park.
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
	parked := make(chan struct{})
	fs.extentMu.Lock()
	fs.extentBuildCond = sync.NewCond(&fs.extentMu)
	fs.extentBuilding = true
	fs.extentBuildGen = 1
	fs.extentBuildWaitHook = func() { close(parked) }
	fs.extentMu.Unlock()

	done := make(chan error, 1)
	go func() { done <- fs.ensureExtentRuntime() }()
	<-parked // the waiter now holds extentMu and is about to park

	// Simulate the in-progress build failing, as ensureExtentRuntime's deferred
	// publisher would. The waiter holds extentMu between the hook and Wait, so
	// this lock can only be acquired after the waiter has parked — no race.
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
